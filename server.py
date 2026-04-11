"""
SurgeonAgent Web Server

FastAPI application that exposes the SurgeonAgent via WebSocket for real-time
multi-turn conversations, plus a REST fallback and file download endpoint.

Run locally:  uvicorn server:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv(override=True)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
SESSION_OUTPUT_BASE = os.path.join(BASE_DIR, "output")

# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

sessions: dict[str, dict] = {}
# Each session: {"client": ClaudeSDKClient, "output_dir": str, "last_active": float}

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

    return [
        lookup_surgery_info_tool,
        find_best_surgeon_tool,
        lookup_npi_tool,
        lookup_csv_tool,
        check_davinci_tool,
        research_surgeon_tool,
        generate_profile_tool,
    ]


# ---------------------------------------------------------------------------
# System prompt (same as agent.py)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are SurgeonAgent, a friendly and professional assistant that helps \
patients find the best surgeons for their specific surgical needs.

## Your Conversational Flow

Follow these steps IN ORDER. Do NOT skip steps. Ask one question at a time \
and wait for the user's response before proceeding.

### Step 1: Identify the Surgery
- Ask the user: "What type of surgery have you been told you need?"
- Once they answer, confirm the name of the surgery.
- Use the `lookup_surgery_info` tool to check the Surgery wiki knowledge base.
  - If a wiki article is found, use it as the PRIMARY source for your summary.
  - If no wiki article is found, provide a summary from your own medical knowledge.
- Present a clear, patient-friendly summary of the surgery (2-3 paragraphs).
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
- Present results in a table: name, Informed Score, cases, complication-free \
rate, cost, facility.
- Recommend the #1 surgeon and ask if they want a full profile generated.

### Step 5: Generate Profile (if requested)
Use research_surgeon, lookup_csv_performance, check_davinci_listing, and \
generate_surgeon_profile to create a .docx profile.

## Important Rules
- Be warm, professional, and patient-centered.
- NEVER provide medical diagnoses or treatment recommendations.
- Available procedures: Knee Replacement, Hip Replacement, Cholecystectomy, \
Hysterectomy (Benign/Malignant), Colon, Rectal, Prostatectomy, Lung Resection, \
Pancreatectomy, Splenectomy, Appendectomy, Gastric Bypass, Sleeve Gastrectomy, \
Lap Band Removal, Other Bariatrics, Abdominal Hernia, Inguinal Hernia, \
Hiatal Hernia, Other Thoracic.
"""


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="SurgeonAgent", version="1.0.0")


@app.get("/health")
async def health():
    """Health check endpoint for Azure App Service probes."""
    return {"status": "healthy", "active_sessions": len(sessions)}


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for multi-turn conversations."""
    await websocket.accept()

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
            "mcp__surgeon-tools__lookup_npi",
            "mcp__surgeon-tools__lookup_csv_performance",
            "mcp__surgeon-tools__check_davinci_listing",
            "mcp__surgeon-tools__research_surgeon",
            "mcp__surgeon-tools__generate_surgeon_profile",
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


async def _notify_downloads(websocket: WebSocket, session_id: str, output_dir: str):
    """Check output dir for new .docx files and notify the client."""
    if not os.path.isdir(output_dir):
        return
    for fname in os.listdir(output_dir):
        if fname.endswith(".docx"):
            await websocket.send_json({
                "type": "file_ready",
                "filename": fname,
                "download_url": f"/download/{session_id}/{fname}",
            })


@app.get("/download/{session_id}/{filename}")
async def download_file(session_id: str, filename: str):
    """Serve generated .docx files for download."""
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

@app.on_event("startup")
async def startup_cleanup_task():
    asyncio.create_task(_cleanup_loop())


async def _cleanup_loop():
    """Periodically remove idle sessions and their output files."""
    while True:
        await asyncio.sleep(300)  # Check every 5 minutes
        now = time.time()
        expired = [
            sid for sid, info in sessions.items()
            if now - info["last_active"] > SESSION_TIMEOUT_SECONDS
        ]
        for sid in expired:
            info = sessions.pop(sid, None)
            if info:
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
