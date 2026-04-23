"""
SurgeonAgent Web Server

FastAPI application that exposes the SurgeonAgent via WebSocket for real-time
multi-turn conversations, plus a REST fallback and file download endpoint.

Run locally:  uvicorn server:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
from collections import defaultdict
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("surgeonagent")

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv(override=True)

logger.info("Starting SurgeonAgent server...")
logger.info(f"PORT={os.environ.get('PORT', 'not set')}")
logger.info(f"ANTHROPIC_API_KEY={'set' if os.environ.get('ANTHROPIC_API_KEY') else 'NOT SET'}")

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
logger.info("FastAPI imported OK")

try:
    from claude_agent_sdk import (
        tool,
        create_sdk_mcp_server,
        ClaudeSDKClient,
        ClaudeAgentOptions,
        AssistantMessage,
        ResultMessage,
        SystemMessage,
        TextBlock,
    )
    logger.info("claude_agent_sdk imported OK")
except Exception as e:
    logger.error(f"Failed to import claude_agent_sdk: {e}")
    raise

from research import (
    lookup_npi as _lookup_npi,
    lookup_csv as _lookup_csv,
    check_intuitive_davinci as _check_davinci,
    research_surgeon as _research_surgeon,
    lookup_surgery_wiki as _lookup_wiki,
    list_surgery_wiki_topics as _list_wiki_topics,
    find_best_surgeon as _find_best_surgeon,
    load_cache,
    save_cache,
)
from profile_generator import generate_profile
from summary_generator import generate_consultation_summary as _generate_summary
from email_sender import (
    send_consultation_summary as _send_email,
    validate_email as _validate_email,
)
from palantir_score import (
    build_patient_features as _build_patient_features,
    score_surgeons as _score_surgeons,
)
logger.info("All imports complete")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
SESSION_OUTPUT_BASE = os.path.join(BASE_DIR, "output")


# ---------------------------------------------------------------------------
# Rate limiter  (IP-based, in-memory)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Sliding-window rate limiter keyed by IP address.

    Limits:
      - CONNECTIONS_PER_HOUR:  max WebSocket sessions one IP can open per hour
      - MAX_CONCURRENT:        max simultaneous active sessions per IP
      - MESSAGES_PER_SESSION:  max user messages within one WebSocket session
      - HTTP_REQUESTS_PER_MIN: max HTTP requests (download, debug) per minute
    """

    CONNECTIONS_PER_HOUR = int(os.environ.get("RATE_LIMIT_CONNECTIONS", "10"))
    MAX_CONCURRENT = int(os.environ.get("RATE_LIMIT_CONCURRENT", "3"))
    MESSAGES_PER_SESSION = int(os.environ.get("RATE_LIMIT_MESSAGES", "60"))
    HTTP_REQUESTS_PER_MIN = int(os.environ.get("RATE_LIMIT_HTTP", "30"))
    EMAILS_PER_IP_HOUR = int(os.environ.get("RATE_LIMIT_EMAILS_IP", "3"))
    EMAILS_PER_ADDR_HOUR = int(os.environ.get("RATE_LIMIT_EMAILS_ADDR", "2"))
    EMAILS_PER_SESSION = int(os.environ.get("RATE_LIMIT_EMAILS_SESSION", "1"))

    def __init__(self):
        self._ws_timestamps: dict[str, list[float]] = defaultdict(list)
        self._active_sessions: dict[str, set[str]] = defaultdict(set)
        self._msg_counts: dict[str, int] = {}          # session_id -> count
        self._http_timestamps: dict[str, list[float]] = defaultdict(list)
        self._email_ip_timestamps: dict[str, list[float]] = defaultdict(list)
        self._email_addr_timestamps: dict[str, list[float]] = defaultdict(list)
        self._email_session_counts: dict[str, int] = {}

    # -- helpers --

    @staticmethod
    def _prune(timestamps: list[float], window_seconds: float) -> list[float]:
        cutoff = time.time() - window_seconds
        return [t for t in timestamps if t > cutoff]

    # -- WebSocket guards --

    def check_ws_connect(self, ip: str, session_id: str) -> str | None:
        """Return an error message if the connection should be rejected, else None."""
        # Prune old entries
        self._ws_timestamps[ip] = self._prune(self._ws_timestamps[ip], 3600)

        if len(self._ws_timestamps[ip]) >= self.CONNECTIONS_PER_HOUR:
            logger.warning(f"Rate limit: {ip} exceeded {self.CONNECTIONS_PER_HOUR} connections/hour")
            return "Rate limit exceeded. Please try again later."

        if len(self._active_sessions[ip]) >= self.MAX_CONCURRENT:
            logger.warning(f"Rate limit: {ip} has {len(self._active_sessions[ip])} concurrent sessions")
            return "Too many active sessions. Please close an existing session first."

        # Record connection
        self._ws_timestamps[ip].append(time.time())
        self._active_sessions[ip].add(session_id)
        self._msg_counts[session_id] = 0
        return None

    def check_ws_message(self, session_id: str) -> str | None:
        """Return an error if this session has sent too many messages, else None."""
        count = self._msg_counts.get(session_id, 0) + 1
        self._msg_counts[session_id] = count
        if count > self.MESSAGES_PER_SESSION:
            logger.warning(f"Rate limit: session {session_id[:8]}... exceeded {self.MESSAGES_PER_SESSION} messages")
            return "Message limit reached for this session. Please start a new session."
        return None

    def release_ws(self, ip: str, session_id: str):
        """Called when a WebSocket disconnects."""
        self._active_sessions[ip].discard(session_id)
        if not self._active_sessions[ip]:
            del self._active_sessions[ip]
        self._msg_counts.pop(session_id, None)
        self._email_session_counts.pop(session_id, None)

    # -- HTTP guards --

    def check_http(self, ip: str) -> bool:
        """Return True if the request is allowed, False if rate-limited."""
        self._http_timestamps[ip] = self._prune(self._http_timestamps[ip], 60)
        if len(self._http_timestamps[ip]) >= self.HTTP_REQUESTS_PER_MIN:
            logger.warning(f"Rate limit: {ip} exceeded {self.HTTP_REQUESTS_PER_MIN} HTTP requests/min")
            return False
        self._http_timestamps[ip].append(time.time())
        return True

    # -- Email guards --

    def check_email(self, ip: str, session_id: str, to_addr: str) -> str | None:
        """Return an error message if the email send should be rejected, else None."""
        to_key = to_addr.strip().lower()

        # IP-level hourly limit
        self._email_ip_timestamps[ip] = self._prune(self._email_ip_timestamps[ip], 3600)
        if len(self._email_ip_timestamps[ip]) >= self.EMAILS_PER_IP_HOUR:
            logger.warning(f"Email rate limit: {ip} exceeded {self.EMAILS_PER_IP_HOUR}/hour")
            return "Email send limit reached for your location. Please try again later."

        # Per-destination hourly limit (anti-harassment)
        self._email_addr_timestamps[to_key] = self._prune(self._email_addr_timestamps[to_key], 3600)
        if len(self._email_addr_timestamps[to_key]) >= self.EMAILS_PER_ADDR_HOUR:
            logger.warning(f"Email rate limit: destination {to_key} exceeded {self.EMAILS_PER_ADDR_HOUR}/hour")
            return "This email address has received too many messages recently. Please try again later."

        # Per-session limit
        count = self._email_session_counts.get(session_id, 0)
        if count >= self.EMAILS_PER_SESSION:
            logger.warning(f"Email rate limit: session {session_id[:8]}... exceeded {self.EMAILS_PER_SESSION}")
            return "You have already sent the consultation summary for this session."

        # Record
        now = time.time()
        self._email_ip_timestamps[ip].append(now)
        self._email_addr_timestamps[to_key].append(now)
        self._email_session_counts[session_id] = count + 1
        return None

    def release_email_session(self, session_id: str):
        """Called on WebSocket disconnect to clean up email session counter."""
        self._email_session_counts.pop(session_id, None)

    def cleanup(self):
        """Periodic housekeeping — remove stale entries."""
        now = time.time()
        stale_ips = [ip for ip, ts in self._ws_timestamps.items()
                     if not self._prune(ts, 3600)]
        for ip in stale_ips:
            del self._ws_timestamps[ip]
        stale_http = [ip for ip, ts in self._http_timestamps.items()
                      if not self._prune(ts, 60)]
        for ip in stale_http:
            del self._http_timestamps[ip]
        stale_email_ips = [ip for ip, ts in self._email_ip_timestamps.items()
                           if not self._prune(ts, 3600)]
        for ip in stale_email_ips:
            del self._email_ip_timestamps[ip]
        stale_email_addrs = [a for a, ts in self._email_addr_timestamps.items()
                             if not self._prune(ts, 3600)]
        for a in stale_email_addrs:
            del self._email_addr_timestamps[a]


rate_limiter = RateLimiter()

logger.info(
    f"Rate limits: {rate_limiter.CONNECTIONS_PER_HOUR} conn/hr, "
    f"{rate_limiter.MAX_CONCURRENT} concurrent, "
    f"{rate_limiter.MESSAGES_PER_SESSION} msg/session, "
    f"{rate_limiter.HTTP_REQUESTS_PER_MIN} HTTP/min"
)


def _get_client_ip(websocket_or_request) -> str:
    """Extract client IP, respecting X-Forwarded-For from Railway's proxy."""
    forwarded = None
    if hasattr(websocket_or_request, "headers"):
        forwarded = websocket_or_request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = getattr(websocket_or_request, "client", None)
    if client:
        return client.host
    return "unknown"


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

sessions: dict[str, dict] = {}
# Each session: {"client": ClaudeSDKClient, "output_dir": str, "last_active": float, "ip": str}

SESSION_TIMEOUT_SECONDS = 30 * 60  # 30 minutes


# ---------------------------------------------------------------------------
# Tool factories — creates per-session tools with isolated output directories
# ---------------------------------------------------------------------------

def create_tools(session_output_dir: str, session_id: str = "", client_ip: str = ""):
    """Create a set of MCP tools scoped to a specific session output directory."""

    @tool(
        "lookup_surgery_info",
        "Search the Surgery wiki knowledge base for information about a surgical "
        "procedure. Returns markdown content from the wiki if a matching article "
        "exists. Also returns a list of all available wiki topics.",
        {"surgery_name": str},
    )
    async def lookup_surgery_info_tool(args):
        name = args["surgery_name"]
        wiki_result = _lookup_wiki(name)
        topics = _list_wiki_topics()
        out = {
            "wiki_found": wiki_result["found"],
            "wiki_content": wiki_result["content"][:5000] if wiki_result["found"] else "",
            "wiki_source": wiki_result["source"],
            "available_topics": topics,
        }
        return {"content": [{"type": "text", "text": json.dumps(out, indent=2)}]}

    @tool(
        "find_best_surgeon",
        "Find the top-rated surgeons for a specific procedure in or near a given "
        "city. Searches NationalTop80Score.csv and ranks by Informed Score.",
        {"city": str, "procedure": str, "state": str, "top_n": int},
    )
    async def find_best_surgeon_tool(args):
        result = _find_best_surgeon(
            city=args["city"],
            procedure=args["procedure"],
            state=args.get("state", ""),
            top_n=args.get("top_n", 5),
        )
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    @tool(
        "get_patient_match_scores",
        "Get per-surgeon Patient Match Scores from the Palantir model for the "
        "top surgeons. Call this AFTER find_best_surgeon, ONCE, with the full "
        "list of top surgeons and the patient's collected health data. The "
        "tool handles all the API calls in parallel and returns each surgeon "
        "with an added 'match_score_display' field (e.g. '98% Match'). "
        "Pass 'surgeons_json' as a JSON array of surgeon objects (each must "
        "have an 'npi' field — the rest of the surgeon object is preserved "
        "and returned as-is).",
        {
            "surgeons_json": str,
            "procedure": str,
            "bmi": float,
            "diabetes_status": str,   # "none", "type 1", "type 2"
            "age": int,                # patient age in years, 0 if unknown
            "general_health": str,     # "Very Healthy" / "Healthy" / "One Chronic Condition" / "Multiple Chronic Conditions"
            "gender": str,             # "Male" / "Female" / "No Answer"
        },
    )
    async def get_match_scores_tool(args):
        try:
            surgeons = json.loads(args["surgeons_json"])
            if not isinstance(surgeons, list):
                return {"content": [{"type": "text", "text": "surgeons_json must be a JSON array"}]}
        except json.JSONDecodeError as e:
            return {"content": [{"type": "text", "text": f"Invalid surgeons_json: {e}"}]}

        age_val = args.get("age", 0)
        features = _build_patient_features(
            bmi=args.get("bmi") or None,
            diabetes_status=args.get("diabetes_status", "") or "",
            age=int(age_val) if age_val else None,
            general_health=args.get("general_health", "") or "",
            gender=args.get("gender", "") or "",
        )

        try:
            scored = await _score_surgeons(
                surgeons=surgeons,
                inf_proc_group=args.get("procedure", ""),
                patient_features=features,
            )
        except Exception as e:
            logger.exception("Palantir scoring failed")
            return {"content": [{"type": "text", "text": (
                f"Match scoring unavailable ({e}). Proceeding without match scores."
            )}]}

        summary = {
            "patient_features": features,
            "scored_surgeons": scored,
            "note": (
                "match_score is 0.0-1.0 or null; match_score_display is the "
                "user-facing percentage string (or empty if unavailable). If "
                "a score is null, the API call failed or the token is missing "
                "— continue gracefully without showing a score for that surgeon."
            ),
        }
        return {"content": [{"type": "text", "text": json.dumps(summary, indent=2)}]}

    @tool(
        "lookup_npi",
        "Look up a surgeon in the NPI Registry by their 10-digit NPI number.",
        {"npi": str},
    )
    async def lookup_npi_tool(args):
        npi = args["npi"]
        result = _lookup_npi(npi)
        if not result:
            return {"content": [{"type": "text", "text": f"No results found for NPI {npi}"}]}
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    @tool(
        "research_surgeon",
        "Perform comprehensive web research for a surgeon. Queries the NPI Registry "
        "and scrapes Healthgrades for bio, ratings, education, affiliations, awards, "
        "and photo. Results are cached locally.",
        {
            "npi": str, "first_name": str, "last_name": str,
            "city": str, "state": str, "specialty": str,
        },
    )
    async def research_surgeon_tool(args):
        npi = args["npi"]
        cached = load_cache(npi)
        if cached:
            return {"content": [{"type": "text", "text": (
                f"Using cached research for NPI {npi}.\n\n"
                + json.dumps(cached, indent=2)
            )}]}
        result = _research_surgeon(
            npi=npi,
            first_name=args.get("first_name", ""),
            last_name=args.get("last_name", ""),
            city=args.get("city", ""),
            state=args.get("state", ""),
            specialty=args.get("specialty", ""),
        )
        save_cache(npi, result)
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    @tool(
        "lookup_csv_performance",
        "Look up a surgeon's performance metrics from NationalTop80Score.csv by NPI.",
        {"npi": str},
    )
    async def lookup_csv_tool(args):
        npi = args["npi"]
        result = _lookup_csv(npi)
        if not result:
            return {"content": [{"type": "text", "text": f"NPI {npi} not found in NationalTop80Score.csv"}]}
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    @tool(
        "lookup_knowledge",
        "Search the Informed knowledge base for articles about surgical techniques, "
        "robotic-assisted surgery, da Vinci systems, minimally invasive surgery, and "
        "related clinical topics. Use this to enrich conversations with authoritative "
        "content about surgical approaches and their benefits.",
        {"query": str},
    )
    async def lookup_knowledge_tool(args):
        result = _search_knowledge(args["query"])
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    @tool(
        "check_davinci_listing",
        "Check if a surgeon is listed on the Intuitive da Vinci Physician Locator.",
        {"first_name": str, "last_name": str, "city": str, "state": str},
    )
    async def check_davinci_tool(args):
        result = _check_davinci(
            first_name=args["first_name"],
            last_name=args["last_name"],
            city=args.get("city", ""),
            state=args.get("state", ""),
        )
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}

    @tool(
        "generate_surgeon_profile",
        "Generate a formatted .docx surgeon profile document from structured data. "
        "Pass in the full profile data dict as a JSON string.",
        {"profile_data_json": str},
    )
    async def generate_profile_tool(args):
        try:
            profile_data = json.loads(args["profile_data_json"])
        except json.JSONDecodeError as e:
            return {"content": [{"type": "text", "text": f"Invalid JSON: {e}"}]}
        try:
            filepath = generate_profile(profile_data, output_dir=session_output_dir)
            filename = os.path.basename(filepath)
            return {"content": [{"type": "text", "text": (
                f"Profile generated successfully!\n"
                f"Saved to: {filepath}\n"
                f"Download filename: {filename}"
            )}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Error generating profile: {e}"}]}

    @tool(
        "generate_consultation_summary",
        "Generate a branded PDF summarizing this consultation. Includes: procedure "
        "overview, recommended surgeon details, top 5 surgeons comparison grid, and "
        "a 'Questions for Your Surgeon' worksheet. Pass all data as a JSON string.",
        {"summary_data_json": str},
    )
    async def generate_summary_tool(args):
        try:
            summary_data = json.loads(args["summary_data_json"])
        except json.JSONDecodeError as e:
            return {"content": [{"type": "text", "text": f"Invalid JSON: {e}"}]}
        try:
            filepath = _generate_summary(summary_data, output_dir=session_output_dir)
            filename = os.path.basename(filepath)
            return {"content": [{"type": "text", "text": (
                f"Consultation summary PDF generated!\n"
                f"Saved to: {filepath}\n"
                f"Download filename: {filename}"
            )}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Error generating summary: {e}"}]}

    @tool(
        "email_consultation_summary",
        "Email the consultation summary PDF to the user. Call this ONLY after "
        "the user has explicitly provided an email address and confirmed they "
        "want it sent. Required params: to_email (the recipient), pdf_filename "
        "(the filename returned by generate_consultation_summary), "
        "procedure_name (for the subject/body), surgeon_name (for the body).",
        {
            "to_email": str,
            "pdf_filename": str,
            "procedure_name": str,
            "surgeon_name": str,
        },
    )
    async def email_summary_tool(args):
        to_email = args.get("to_email", "").strip()
        pdf_filename = args.get("pdf_filename", "").strip()

        # Validate email format
        if not _validate_email(to_email):
            return {"content": [{"type": "text", "text": (
                f"The email address '{to_email}' doesn't look valid. "
                f"Please double-check and try again."
            )}]}

        # Rate limits (IP / per-address / per-session)
        reject = rate_limiter.check_email(client_ip, session_id, to_email)
        if reject:
            return {"content": [{"type": "text", "text": reject}]}

        # Locate the PDF
        pdf_path = os.path.join(session_output_dir, pdf_filename)
        if not os.path.isfile(pdf_path):
            # Fallback: find the most recent PDF in the session output dir
            pdfs = [f for f in os.listdir(session_output_dir) if f.endswith(".pdf")] \
                if os.path.isdir(session_output_dir) else []
            if pdfs:
                pdfs.sort(key=lambda f: os.path.getmtime(
                    os.path.join(session_output_dir, f)), reverse=True)
                pdf_path = os.path.join(session_output_dir, pdfs[0])
            else:
                return {"content": [{"type": "text", "text": (
                    "No consultation summary PDF found. Please generate the "
                    "summary first with generate_consultation_summary."
                )}]}

        result = _send_email(
            to_email=to_email,
            pdf_path=pdf_path,
            procedure_name=args.get("procedure_name", ""),
            surgeon_name=args.get("surgeon_name", ""),
        )
        if result["success"]:
            return {"content": [{"type": "text", "text": (
                f"Email sent successfully to {to_email}. "
                f"They should receive it shortly."
            )}]}
        return {"content": [{"type": "text", "text": (
            f"Email could not be sent: {result['message']}"
        )}]}

    return [
        lookup_surgery_info_tool,
        find_best_surgeon_tool,
        get_match_scores_tool,
        lookup_knowledge_tool,
        lookup_npi_tool,
        lookup_csv_tool,
        check_davinci_tool,
        research_surgeon_tool,
        generate_profile_tool,
        generate_summary_tool,
        email_summary_tool,
    ]


# ---------------------------------------------------------------------------
# Load business rules and knowledge base from markdown files
# ---------------------------------------------------------------------------

RULES_DIR = os.path.join(BASE_DIR, "rules")
KNOWLEDGE_DIR = os.path.join(BASE_DIR, "knowledge")


def _load_md_directory(directory: str) -> str:
    """Load all .md files from a directory and return concatenated content."""
    if not os.path.isdir(directory):
        return ""
    texts = []
    for fname in sorted(os.listdir(directory)):
        if fname.endswith(".md"):
            fpath = os.path.join(directory, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    texts.append(f.read().strip())
            except Exception:
                pass
    return "\n\n".join(texts)


def _list_knowledge_topics() -> list[str]:
    """List available knowledge base topics."""
    if not os.path.isdir(KNOWLEDGE_DIR):
        return []
    return [
        f.replace(".md", "").replace("-", " ").replace("_", " ").title()
        for f in sorted(os.listdir(KNOWLEDGE_DIR))
        if f.endswith(".md")
    ]


def _search_knowledge(query: str) -> dict:
    """Search the knowledge base for content matching a query.

    Returns {"found": bool, "results": [{"topic": str, "content": str}]}.
    """
    if not os.path.isdir(KNOWLEDGE_DIR):
        return {"found": False, "results": [], "available_topics": []}

    query_lower = query.lower().strip()
    query_words = query_lower.split()
    results = []

    for fname in sorted(os.listdir(KNOWLEDGE_DIR)):
        if not fname.endswith(".md"):
            continue
        fpath = os.path.join(KNOWLEDGE_DIR, fname)
        topic = fname.replace(".md", "").replace("-", " ").replace("_", " ").title()
        fname_lower = fname.lower().replace("-", " ").replace("_", " ").replace(".md", "")

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            continue

        # Score: filename match + content match
        score = 0
        if query_lower in fname_lower or fname_lower in query_lower:
            score += 10
        for w in query_words:
            if len(w) > 2 and w in fname_lower:
                score += 3
            if len(w) > 2 and w in content.lower():
                score += 1

        if score > 0:
            results.append({"topic": topic, "content": content, "_score": score})

    results.sort(key=lambda r: r["_score"], reverse=True)
    for r in results:
        del r["_score"]

    return {
        "found": len(results) > 0,
        "results": results[:3],  # Top 3 matches
        "available_topics": _list_knowledge_topics(),
    }


BUSINESS_RULES = _load_md_directory(RULES_DIR)
KNOWLEDGE_BASE = _load_md_directory(KNOWLEDGE_DIR)
logger.info(f"Loaded business rules: {len(BUSINESS_RULES)} chars from {RULES_DIR}")
logger.info(f"Loaded knowledge base: {len(KNOWLEDGE_BASE)} chars from {KNOWLEDGE_DIR} ({len(_list_knowledge_topics())} topics)")


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are SurgeonAgent, a friendly and professional assistant that helps \
patients find the best surgeons for their specific surgical needs.

## Your Conversational Flow

Follow these steps IN ORDER. Do NOT skip steps. Ask one question at a time \
and wait for the user's response before proceeding.

### Step 1: Identify the Surgery
- Ask the user: "What type of surgery have you been told you need?"
- IMPORTANT: Do NOT list or enumerate the available procedures in your \
greeting. The user interface automatically displays clickable procedure \
buttons below your message, so listing them in your text would be redundant \
and take up space. Keep your greeting concise — just ask the question.
- The user may click one of those buttons or type their own procedure. \
Either way, treat their response the same.
- Once they answer, confirm the name of the surgery.
- Use the `lookup_surgery_info` tool to check the Surgery wiki knowledge base.
  - If a wiki article is found, use it as the PRIMARY source for your summary.
  - If no wiki article is found, provide a summary from your own medical knowledge.
- Use `lookup_knowledge` to search for relevant articles about the surgical \
technique, especially robotic-assisted or minimally invasive approaches.
  - If the procedure CAN be performed with robotic assistance, proactively \
mention the robotic-assisted option and its benefits (smaller incisions, \
less pain, faster recovery, enhanced precision, 3D visualization).
  - Use the knowledge base content as your source — do not invent claims.
- Present a clear, patient-friendly summary of the surgery (2-3 paragraphs), \
including any robotic-assisted option and its advantages.
- Confirm with the user that you have the right procedure before continuing.

### Step 2: Health Screening Questions
Ask the following health questions ONE AT A TIME:

**Diabetes:**
- Ask: "Do you have diabetes?"
- If YES, follow up: "Is it Type 1 or Type 2 diabetes?"

**BMI:**
- Ask: "Do you know what your BMI is?"
- If YES, record the number they provide.
- If NO, ask: "What is your height and weight? I can calculate it for you."
  - Calculate BMI: BMI = (weight_lbs / height_inches^2) * 703
  - Categories: Under 18.5 Underweight, 18.5-24.9 Normal, 25.0-29.9 Overweight, 30.0+ Obese

**Age:**
- Ask: "What is your age?"
- Accept a numeric answer. If the user prefers not to say or gives a non-numeric answer, record age as unknown.
- Do NOT attach an OPTIONS marker — this is free-text numeric input.

**Gender:**
- Ask: "What is your gender?"
- At the END of that message (on its own line, nothing after) append this exact marker:
  `[[OPTIONS: Male | Female | No Answer]]`
- The UI will render those as clickable buttons. The user may click one or type their own response — accept any reasonable answer.

**General Health:**
- Ask: "How would you describe your general health?"
- At the END of that message (on its own line, nothing after) append this exact marker:
  `[[OPTIONS: Very Healthy | Healthy | One Chronic Condition | Multiple Chronic Conditions]]`
- The UI will render those as clickable buttons.

After collecting health info, briefly note how these factors may be relevant \
to their surgery. Keep this factual and reassuring — do NOT diagnose.

## OPTIONS Marker — General Rule
Whenever you want the UI to show a short list of clickable response buttons, \
end your message with a single line of the form:
  `[[OPTIONS: Option A | Option B | Option C]]`
- Options are separated by ` | ` (space-pipe-space).
- Only use this for short, structured-choice questions (gender, health status, \
yes/no, etc.). Do NOT use it for open-ended questions like city or surgeon name.
- The marker line itself is hidden from the user — they see only clickable chips.
- Keep each option label short (under ~30 chars).

### Step 3: Location
- Ask: "What city do you live in?"
- If they only provide a city, ask for their state.

### Step 3.5: Existing Surgeon Referral
Before searching for recommendations, ask:
"Have you already been referred to or directed to use a particular surgeon? \
If so, what is their name?"

- If the user provides a surgeon name:
  - Remember the name — you will look them up in Step 4 using \
`lookup_csv_performance` (by name, if NPI is unknown) so you can compare their \
performance metrics directly against the top-ranked surgeons in their area.
  - Reassure the user that you'll still show them the top surgeons in their \
region so they can make an informed comparison.
- If the user says no, proceed normally to Step 4.

### Step 4: Find the Best Surgeon
- Use the `find_best_surgeon` tool to search for the top surgeon.
- If the user named a referred surgeon in Step 3.5, ALSO look up that surgeon's \
metrics (using `lookup_csv_performance` or by searching `find_best_surgeon` \
results for a name match) and INCLUDE them in your comparison table even if \
they are not in the top 5. Clearly label their row as "Your Referred Surgeon" \
so the user can compare them side-by-side with the top-ranked options.
- **CALL `get_patient_match_scores` ONCE** with:
  - `surgeons_json`: the JSON-stringified list of the top surgeons (plus the \
referred surgeon if any). Each object MUST include the surgeon's `npi`.
  - `procedure`: the exact procedure string from Step 1 (e.g. "Cholecystectomy").
  - `bmi`: the BMI number collected in Step 2 (or 0 if unknown).
  - `diabetes_status`: "none", "type 1", or "type 2".
  - `age`: the patient's age as an integer (0 if unknown).
  - `general_health`: the exact value the user chose ("Very Healthy", \
"Healthy", "One Chronic Condition", "Multiple Chronic Conditions").
  - `gender`: the value the user chose ("Male", "Female", or "No Answer").
  This returns each surgeon enriched with a `match_score_display` string \
(e.g. "98% Match"). If a surgeon's score is empty/null, the service was \
unavailable — simply omit that surgeon's match score column entry in your \
response and continue gracefully. Do NOT mention the word "Palantir" or any \
backend service to the user.
- For the top recommended surgeons, use `check_davinci_listing` to check if \
they are listed as robotic-assisted surgeons.
- Present results in a table: name, Patient Match, Informed Score, cases, \
complication-free rate, cost, facility. The "Patient Match" column should \
show the `match_score_display` value (or blank if unavailable).
- If a surgeon is listed as performing robotic-assisted procedures, highlight \
this with a note about the advantages of robotic-assisted surgery (use content \
from `lookup_knowledge` about robotic surgery benefits).
- If a robotic-certified surgeon has a similar Informed Score (within 5 points) \
to the top-ranked surgeon, specifically call them out as a recommended \
alternative and explain the potential benefits of choosing a robotic surgeon.

**Final Recommendation Logic:**
1. Start from the surgeon with the highest Informed Score.
2. **Patient Match tie-breaker:** If another surgeon in the top list has an \
Informed Score within 5 points of the top-ranked surgeon AND a meaningfully \
higher Patient Match Score (at least 5 percentage points higher, e.g. 92% vs \
85%), recommend that surgeon instead. Explain clearly: "Dr. X has a slightly \
lower Informed Score than Dr. Y, but their Patient Match Score for your \
specific profile is notably higher — this model accounts for factors like \
your age, health status, and clinical history, so I'd recommend Dr. X as a \
better fit for you."
3. If multiple surgeons are still in contention (similar Informed Score AND \
similar Patient Match), prefer the one that is robotic-assisted certified \
when the procedure supports it.
4. After announcing your recommendation, ask if they want a full profile \
generated.

**When explaining the Patient Match Score to the user:** describe it as \
"a personalized match score that considers your specific health profile \
(age, BMI, diabetes status, general health) and how similar patients have \
done with this surgeon." Do NOT mention "Palantir" or any backend system.
- If Patient Match Scores are unavailable (empty column), fall back to \
ranking by Informed Score alone and do not reference match scores in your \
recommendation.

### Step 5: Generate Profile (if requested)
If the user wants a full profile, you MUST:
1. Call `research_surgeon` with the surgeon's NPI, first_name, last_name, city, state
2. Call `lookup_csv_performance` with the NPI
3. Call `check_davinci_listing` with first_name, last_name, city, state
4. Use WebSearch for additional enrichment (practice website, news, etc.)
5. Compile ALL gathered data into a single JSON object and pass it to \
`generate_surgeon_profile`

After the profile is generated, tell the user it's ready and that they'll \
find it in the "File Cabinet" panel on the left (mobile: floating folder icon \
in the lower-left). Do NOT paste a download link into the chat.

## CRITICAL: Profile Data JSON Schema
When calling `generate_surgeon_profile`, the JSON MUST include ALL of these keys. \
Do NOT omit any key — use an empty list [] or empty string "" if no data is available:

```json
{
  "npi": "1234567890",
  "full_name": "John Smith",
  "credential": "M.D.",
  "specialty": "General Surgery",
  "city": "Birmingham",
  "state": "AL",
  "address": "123 Main St",
  "zip": "35243",
  "phone": "(205) 555-1234",
  "practice_name": "Smith Surgical Associates",
  "practice_website": "https://example.com",
  "description": "Dr. Smith is a board-certified general surgeon...",
  "education": ["Medical School: University of Alabama", "Residency: UAB"],
  "board_certs": ["Board Certified, General Surgery"],
  "memberships": ["American College of Surgeons"],
  "affiliations": [{"name": "Hospital Name", "city": "City", "state": "ST"}],
  "ratings": [{"platform": "Healthgrades", "rating": "4.5 / 5.0", "notes": "20 reviews"}],
  "awards": ["Healthgrades Honor Roll"],
  "media": [],
  "procedures": [{"name": "Cholecystectomy", "informed_score": 93, "cases": 506}],
  "languages": ["English"],
  "source_urls": ["https://npiregistry.cms.hhs.gov/provider-view/1234567890"],
  "photo_path": "",
  "locations": [],
  "davinci_status": {"listed": true, "details": "...", "profile_url": "..."}
}
```

CRITICAL RULES for profile generation:
- The `procedures` list MUST come from `lookup_csv_performance` results
- The `davinci_status` MUST come from `check_davinci_listing` results
- The `education` list should include medical school from CSV + any from web research
- The `source_urls` MUST include all URLs used during research
- NEVER omit a key — include it with an empty value if no data was found
- ALWAYS merge data from ALL tool calls into one complete JSON before generating

### Step 6: Offer Consultation Summary
After the surgeon profile has been generated (or if the user declines a profile), \
ask the user:
"Would you like me to generate a Consultation Summary document? It will include \
a summary of the procedure we discussed, the surgeon recommendation, a comparison \
of the top surgeons, and a worksheet of questions to bring to your appointment."

If they say yes, call `generate_consultation_summary` with a JSON object containing:
```json
{
  "procedure_name": "Cholecystectomy (Gallbladder Removal)",
  "procedure_description": "A 2-3 sentence summary of the procedure...",
  "patient_city": "Dallas",
  "patient_state": "TX",
  "recommended_surgeon": {
    "name": "John Smith",
    "credential": "M.D.",
    "specialty": "General Surgery",
    "informed_score": 95,
    "cases": "506",
    "complication_free_rate": "98.2%",
    "avg_90_day_cost": "$12,450",
    "city": "Dallas",
    "state": "TX",
    "medical_school": "University Of Texas Southwestern",
    "facilities": ["Baylor University Medical Center"],
    "davinci_status": {"listed": true, "details": "..."}
  },
  "top_surgeons": [
    {"name": "...", "informed_score": 95, "cases": "506",
     "complication_free_rate": "98.2%", "avg_90_day_cost": "$12,450",
     "facilities": ["..."]}
  ]
}
```
IMPORTANT: The `top_surgeons` list should include ALL surgeons from Step 4 \
(up to 5), including the recommended surgeon. Use the data you already have \
from the `find_best_surgeon` results — do NOT re-query.

After generating the PDF, tell the user it's ready and that they will find \
it in the "File Cabinet" panel on the left side of the screen (on mobile, a \
floating folder icon appears in the lower-left). Do NOT paste a download link \
into the chat — the file cabinet handles downloads. Remember the exact \
`Download filename` returned by the tool — you will need it in Step 7.

### Step 7: Offer to Email the Summary
After the PDF is generated, ask:
"Would you like me to also email a copy to you? If so, what email address \
should I send it to?"

- If the user declines, thank them and end the conversation warmly.
- If they provide an email address:
  1. READ THE ADDRESS BACK to them exactly as you heard it and ask them to \
confirm: "Just to confirm — I'll send it to [address]. Is that correct?"
  2. Only after they confirm (yes / correct / that's right), call \
`email_consultation_summary` with:
     - `to_email`: the confirmed address
     - `pdf_filename`: the filename from Step 6 (the PDF you just generated)
     - `procedure_name`: the procedure discussed
     - `surgeon_name`: the recommended surgeon's name
  3. Report the result to the user:
     - On success: "Great — I've sent the summary to [address]. You should \
receive it shortly. Please check your spam folder if you don't see it."
     - On failure: apologize, state the reason briefly, and offer the download \
link as an alternative.
- NEVER send an email without explicit confirmation.
- If the user gives you a different address on confirmation, read back the \
new one and confirm again before sending.

## Important Rules
- Be warm, professional, and patient-centered.
- NEVER provide medical diagnoses or treatment recommendations.
- BRAND NEUTRALITY: In all responses shown to the user, use the generic term \
"robotic-assisted" (not "da Vinci", "Intuitive", or any other brand name). \
Only mention "da Vinci" or "Intuitive Surgical" if the user specifically asks \
about that system or company by name. Internally, tool results may reference \
Intuitive's Physician Locator — translate these to "robotic-assisted surgeon \
directory" or similar generic language when speaking to the user.
- Available procedures (shown as clickable chips in the UI — do NOT list these \
in your messages): Knee Replacement, Hip Replacement, Cholecystectomy, \
Hysterectomy (Benign/Malignant), Colon, Rectal, Prostatectomy, Lung Resection, \
Pancreatectomy, Splenectomy, Appendectomy, Gastric Bypass, Sleeve Gastrectomy, \
Lap Band Removal, Other Bariatrics, Abdominal Hernia, Inguinal Hernia, \
Hiatal Hernia, Other Thoracic.
"""

# Append business rules to system prompt if they exist
if BUSINESS_RULES:
    SYSTEM_PROMPT += f"""

## Business Rules (MANDATORY — always follow these)
The following rules are loaded from the rules/ directory and MUST be followed. \
They override any conflicting default behavior.

{BUSINESS_RULES}
"""


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="SurgeonAgent", version="1.0.0")

logger.info("FastAPI app created")


@app.on_event("startup")
async def on_startup():
    logger.info("SurgeonAgent server is UP and ready to accept connections")
    asyncio.create_task(_cleanup_loop())


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "active_sessions": len(sessions)}


@app.get("/debug")
async def debug():
    """Debug endpoint to check environment and readiness."""
    import shutil
    return {
        "status": "running",
        "port": os.environ.get("PORT", "not set"),
        "api_key_set": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "claude_cli_found": shutil.which("claude") is not None,
        "node_version": os.popen("node --version 2>&1").read().strip(),
        "python_version": sys.version,
        "static_dir_exists": os.path.isdir(STATIC_DIR),
        "static_files": os.listdir(STATIC_DIR) if os.path.isdir(STATIC_DIR) else [],
        "csv_exists": os.path.isfile(os.path.join(BASE_DIR, "SurgeonScores", "NationalTop80Score.csv")),
    }


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for multi-turn conversations."""
    await websocket.accept()

    # --- Rate limiting ---
    client_ip = _get_client_ip(websocket)
    reject_reason = rate_limiter.check_ws_connect(client_ip, session_id)
    if reject_reason:
        await websocket.send_json({"type": "error", "content": reject_reason})
        await websocket.close(code=1008, reason="Rate limited")
        return

    # Create per-session output directory
    session_output_dir = os.path.join(SESSION_OUTPUT_BASE, session_id)
    os.makedirs(session_output_dir, exist_ok=True)

    # Create session-scoped tools and MCP server
    tools = create_tools(session_output_dir, session_id=session_id, client_ip=client_ip)
    server = create_sdk_mcp_server("surgeon-tools", tools=tools)

    options = ClaudeAgentOptions(
        cwd=BASE_DIR,
        model="claude-sonnet-4-6",
        allowed_tools=[
            "Read", "Glob", "Grep", "Bash", "WebSearch", "WebFetch",
            "mcp__surgeon-tools__lookup_surgery_info",
            "mcp__surgeon-tools__find_best_surgeon",
            "mcp__surgeon-tools__get_patient_match_scores",
            "mcp__surgeon-tools__lookup_knowledge",
            "mcp__surgeon-tools__lookup_npi",
            "mcp__surgeon-tools__lookup_csv_performance",
            "mcp__surgeon-tools__check_davinci_listing",
            "mcp__surgeon-tools__research_surgeon",
            "mcp__surgeon-tools__generate_surgeon_profile",
            "mcp__surgeon-tools__generate_consultation_summary",
            "mcp__surgeon-tools__email_consultation_summary",
        ],
        permission_mode="acceptEdits",
        max_turns=50,
        system_prompt=SYSTEM_PROMPT,
        mcp_servers={"surgeon-tools": server},
    )

    client = None
    try:
        async with ClaudeSDKClient(options=options) as client:
            sessions[session_id] = {
                "client": client,
                "output_dir": session_output_dir,
                "last_active": time.time(),
                "ip": client_ip,
            }

            # Send initial greeting prompt
            initial_prompt = "Begin the patient consultation. Start by asking me what surgery I need."
            await client.query(initial_prompt)
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            await websocket.send_json({
                                "type": "text",
                                "content": block.text,
                            })
                elif isinstance(message, ResultMessage):
                    # Check if a .docx was generated in this turn
                    await _notify_downloads(websocket, session_id, session_output_dir)
                    await websocket.send_json({
                        "type": "done",
                        "stop_reason": message.stop_reason,
                    })

            # Conversation loop
            while True:
                raw = await websocket.receive_text()
                try:
                    data = json.loads(raw)
                    user_message = data.get("message", raw)
                except json.JSONDecodeError:
                    user_message = raw

                if not user_message.strip():
                    continue

                # --- Per-message rate limit ---
                msg_reject = rate_limiter.check_ws_message(session_id)
                if msg_reject:
                    await websocket.send_json({"type": "error", "content": msg_reject})
                    await websocket.send_json({"type": "done", "stop_reason": "rate_limited"})
                    continue

                sessions[session_id]["last_active"] = time.time()

                await client.query(user_message)
                async for message in client.receive_response():
                    if isinstance(message, AssistantMessage):
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                await websocket.send_json({
                                    "type": "text",
                                    "content": block.text,
                                })
                    elif isinstance(message, ResultMessage):
                        await _notify_downloads(websocket, session_id, session_output_dir)
                        await websocket.send_json({
                            "type": "done",
                            "stop_reason": message.stop_reason,
                        })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "content": str(e)})
        except Exception:
            pass
    finally:
        sessions.pop(session_id, None)
        rate_limiter.release_ws(client_ip, session_id)


async def _notify_downloads(websocket: WebSocket, session_id: str, output_dir: str):
    """Check output dir for new .docx / .pdf files and notify the client."""
    if not os.path.isdir(output_dir):
        return
    for fname in os.listdir(output_dir):
        if fname.endswith(".docx") or fname.endswith(".pdf"):
            await websocket.send_json({
                "type": "file_ready",
                "filename": fname,
                "download_url": f"/download/{session_id}/{fname}",
            })


@app.get("/download/{session_id}/{filename}")
async def download_file(session_id: str, filename: str, request: Request):
    """Serve generated .docx / .pdf files for download."""
    client_ip = _get_client_ip(request)
    if not rate_limiter.check_http(client_ip):
        return JSONResponse({"error": "Rate limit exceeded"}, status_code=429)

    # Sanitize to prevent directory traversal
    safe_filename = os.path.basename(filename)
    filepath = os.path.join(SESSION_OUTPUT_BASE, session_id, safe_filename)
    if not os.path.isfile(filepath):
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(
        filepath,
        filename=safe_filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


# ---------------------------------------------------------------------------
# Session cleanup background task
# ---------------------------------------------------------------------------

## Cleanup task is started in on_startup() above


async def _cleanup_loop():
    """Periodically remove idle sessions and their output files."""
    while True:
        await asyncio.sleep(300)  # Check every 5 minutes
        now = time.time()
        rate_limiter.cleanup()
        expired = [
            sid for sid, info in sessions.items()
            if now - info["last_active"] > SESSION_TIMEOUT_SECONDS
        ]
        for sid in expired:
            info = sessions.pop(sid, None)
            if info:
                # Release rate limiter slot
                ip = info.get("ip", "unknown")
                rate_limiter.release_ws(ip, sid)
                # Clean up output files
                out_dir = info.get("output_dir", "")
                if os.path.isdir(out_dir):
                    for f in os.listdir(out_dir):
                        try:
                            os.remove(os.path.join(out_dir, f))
                        except OSError:
                            pass
                    try:
                        os.rmdir(out_dir)
                    except OSError:
                        pass


# ---------------------------------------------------------------------------
# Serve static frontend
# ---------------------------------------------------------------------------

os.makedirs(STATIC_DIR, exist_ok=True)

# Mount static files LAST so API routes take precedence
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
