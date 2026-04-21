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

    def __init__(self):
        self._ws_timestamps: dict[str, list[float]] = defaultdict(list)
        self._active_sessions: dict[str, set[str]] = defaultdict(set)
        self._msg_counts: dict[str, int] = {}          # session_id -> count
        self._http_timestamps: dict[str, list[float]] = defaultdict(list)

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

    # -- HTTP guards --

    def check_http(self, ip: str) -> bool:
        """Return True if the request is allowed, False if rate-limited."""
        self._http_timestamps[ip] = self._prune(self._http_timestamps[ip], 60)
        if len(self._http_timestamps[ip]) >= self.HTTP_REQUESTS_PER_MIN:
            logger.warning(f"Rate limit: {ip} exceeded {self.HTTP_REQUESTS_PER_MIN} HTTP requests/min")
            return False
        self._http_timestamps[ip].append(time.time())
        return True

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

def create_tools(session_output_dir: str):
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

    return [
        lookup_surgery_info_tool,
        find_best_surgeon_tool,
        lookup_knowledge_tool,
        lookup_npi_tool,
        lookup_csv_tool,
        check_davinci_tool,
        research_surgeon_tool,
        generate_profile_tool,
        generate_summary_tool,
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

After collecting health info, briefly note how these factors may be relevant \
to their surgery. Keep this factual and reassuring — do NOT diagnose.

### Step 3: Location
- Ask: "What city do you live in?"
- If they only provide a city, ask for their state.

### Step 4: Find the Best Surgeon
- Use the `find_best_surgeon` tool to search for the top surgeon.
- For the top recommended surgeons, use `check_davinci_listing` to check if \
they are listed as robotic-assisted surgeons.
- Present results in a table: name, Informed Score, cases, complication-free \
rate, cost, facility.
- If a surgeon is listed as performing robotic-assisted procedures, highlight \
this with a note about the advantages of robotic-assisted surgery (use content \
from `lookup_knowledge` about robotic surgery benefits).
- If a robotic-certified surgeon has a similar Informed Score (within 5 points) \
to the top-ranked surgeon, specifically call them out as a recommended \
alternative and explain the potential benefits of choosing a robotic surgeon.
- Recommend the #1 surgeon and ask if they want a full profile generated.

### Step 5: Generate Profile (if requested)
If the user wants a full profile, you MUST:
1. Call `research_surgeon` with the surgeon's NPI, first_name, last_name, city, state
2. Call `lookup_csv_performance` with the NPI
3. Call `check_davinci_listing` with first_name, last_name, city, state
4. Use WebSearch for additional enrichment (practice website, news, etc.)
5. Compile ALL gathered data into a single JSON object and pass it to \
`generate_surgeon_profile`

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
    tools = create_tools(session_output_dir)
    server = create_sdk_mcp_server("surgeon-tools", tools=tools)

    options = ClaudeAgentOptions(
        cwd=BASE_DIR,
        model="claude-sonnet-4-6",
        allowed_tools=[
            "Read", "Glob", "Grep", "Bash", "WebSearch", "WebFetch",
            "mcp__surgeon-tools__lookup_surgery_info",
            "mcp__surgeon-tools__find_best_surgeon",
            "mcp__surgeon-tools__lookup_knowledge",
            "mcp__surgeon-tools__lookup_npi",
            "mcp__surgeon-tools__lookup_csv_performance",
            "mcp__surgeon-tools__check_davinci_listing",
            "mcp__surgeon-tools__research_surgeon",
            "mcp__surgeon-tools__generate_surgeon_profile",
            "mcp__surgeon-tools__generate_consultation_summary",
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
