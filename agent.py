"""
SurgeonAgent — An interactive agent that helps patients find top-rated surgeons.

Conversational flow:
  1. Ask what surgery the user needs; confirm and explain the procedure
  2. Ask health-screening questions (diabetes, BMI)
  3. Ask what city they live in
  4. Look up the best surgeon for that procedure in their city
  5. Optionally generate a full .docx surgeon profile

Custom MCP tools:
  - lookup_surgery_info: Search the Surgery wiki knowledge base
  - find_best_surgeon: Find top surgeons by city + procedure from CSV
  - lookup_knowledge: Search the Informed knowledge base (robotic surgery, MIS, etc.)
  - lookup_npi / research_surgeon / lookup_csv_performance / check_davinci_listing
  - generate_surgeon_profile: Create a formatted .docx from research data

Built-in tools:
  - WebSearch, WebFetch: For additional web research
  - Read, Glob, Grep: For reading local files
  - Bash: For general shell commands
"""

import json
import os
import sys
import anyio
from dotenv import load_dotenv

# Fix Windows console encoding for emoji/unicode output
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

load_dotenv(override=True)

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


# ---------------------------------------------------------------------------
# Custom MCP Tools — Patient-facing (new)
# ---------------------------------------------------------------------------

@tool(
    "lookup_surgery_info",
    "Search the Surgery wiki knowledge base for information about a surgical "
    "procedure. Returns markdown content from the wiki if a matching article "
    "exists. Also returns a list of all available wiki topics. Use this to "
    "provide the user with an evidence-based summary of their surgery.",
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
    "city. Searches the NationalTop80Score.csv and ranks by Informed Score. "
    "Returns up to top_n results with scores, case counts, complication rates, "
    "costs, and facility information. If no surgeons are found in the exact "
    "city, it searches state-wide and suggests nearby cities.",
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


# ---------------------------------------------------------------------------
# Knowledge base search
# ---------------------------------------------------------------------------

KNOWLEDGE_DIR = os.path.join(BASE_DIR, "knowledge")


def _search_knowledge(query: str) -> dict:
    """Search the knowledge/ directory for matching articles."""
    if not os.path.isdir(KNOWLEDGE_DIR):
        return {"found": False, "results": [], "available_topics": []}

    query_lower = query.lower().strip()
    query_words = query_lower.split()
    results = []
    topics = []

    for fname in sorted(os.listdir(KNOWLEDGE_DIR)):
        if not fname.endswith(".md"):
            continue
        topic = fname.replace(".md", "").replace("-", " ").replace("_", " ").title()
        topics.append(topic)
        fpath = os.path.join(KNOWLEDGE_DIR, fname)
        fname_lower = fname.lower().replace("-", " ").replace("_", " ").replace(".md", "")

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            continue

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
        "results": results[:3],
        "available_topics": topics,
    }


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


# ---------------------------------------------------------------------------
# Custom MCP Tools — Research & Profile (existing)
# ---------------------------------------------------------------------------

@tool(
    "lookup_npi",
    "Look up a surgeon in the NPI Registry by their 10-digit NPI number. "
    "Returns name, credentials, address, phone, and taxonomy/specialty info.",
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
    "and photo. Results are cached locally. Requires at minimum: npi, first_name, "
    "last_name. Optionally provide city, state, specialty for better matching.",
    {
        "npi": str,
        "first_name": str,
        "last_name": str,
        "city": str,
        "state": str,
        "specialty": str,
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
    "Look up a surgeon's performance metrics from the NationalTop80Score.csv file "
    "by NPI number. Returns procedures with Informed Scores, case counts, "
    "complication-free rates, 90-day costs, length of stay, state ranks, "
    "facility affiliations, demographics, and medical school.",
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
    "Check if a surgeon is listed on the Intuitive da Vinci Physician Locator "
    "(robotic surgery directory). Searches by name and location. Returns "
    "{listed: true/false, details: str, profile_url: str}.",
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
    "Pass in the full profile data dict (as JSON string) that was gathered from "
    "research. Returns the path to the generated .docx file.",
    {"profile_data_json": str},
)
async def generate_profile_tool(args):
    try:
        profile_data = json.loads(args["profile_data_json"])
    except json.JSONDecodeError as e:
        return {"content": [{"type": "text", "text": f"Invalid JSON: {e}"}]}
    try:
        filepath = generate_profile(profile_data)
        return {"content": [{"type": "text", "text": (
            f"Profile generated successfully!\nSaved to: {filepath}"
        )}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Error generating profile: {e}"}]}


# ---------------------------------------------------------------------------
# Agent setup
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
- Use the `lookup_surgery_info` tool to check the Surgery wiki knowledge base \
for information about this procedure.
  - If a wiki article is found, use it as the PRIMARY source for your summary.
  - If no wiki article is found, provide a summary from your own medical knowledge.
- Use `lookup_knowledge` to search for relevant articles about the surgical \
technique, especially robotic-assisted or minimally invasive approaches.
  - If the procedure CAN be performed with robotic assistance (da Vinci system), \
proactively mention the robotic-assisted option and its benefits (smaller \
incisions, less pain, faster recovery, enhanced precision, 3D visualization).
  - Use the knowledge base content as your source — do not invent claims.
- Present a clear, patient-friendly summary of the surgery (2-3 paragraphs), \
including any robotic-assisted option and its advantages.
- Confirm with the user that you have the right procedure before continuing.

### Step 2: Health Screening Questions
Ask the following health questions ONE AT A TIME:

**Diabetes:**
- Ask: "Do you have diabetes?"
- If YES, follow up: "Is it Type 1 or Type 2 diabetes?"
- Record their answer.

**BMI:**
- Ask: "Do you know what your BMI is?"
- If YES, record the number they provide.
- If NO, ask: "What is your height and weight? I can calculate it for you."
  - Calculate BMI using the formula: BMI = (weight_lbs / height_inches^2) * 703
  - Tell them their BMI and what category it falls in:
    - Under 18.5: Underweight
    - 18.5-24.9: Normal weight
    - 25.0-29.9: Overweight
    - 30.0+: Obese
- Record the BMI value.

After collecting health info, briefly note how these factors may be relevant \
to their surgery (e.g., diabetes management, BMI considerations for anesthesia \
or recovery). Keep this factual and reassuring — do NOT diagnose or give \
medical advice. Simply note that these are factors their surgical team will \
take into account.

### Step 3: Location
- Ask: "What city do you live in?"
- If they only provide a city name, ask for their state as well.
- Record city and state.

### Step 4: Find the Best Surgeon
- Use the `find_best_surgeon` tool to search for the top-rated surgeon in \
their city for their procedure.
- For the top recommended surgeons, use `check_davinci_listing` to check if \
they are listed as da Vinci robotic surgeons.
- Present the results clearly in a table format showing:
  - Surgeon name and credentials
  - Informed Score (explain this is a quality metric, higher = better)
  - Number of cases performed
  - Complication-free rate
  - Average 90-day cost
  - Hospital/facility
- If a surgeon is da Vinci-listed, highlight this with a note about the \
advantages of robotic-assisted surgery (use content from `lookup_knowledge` \
about robotic surgery benefits).
- If a robotic-certified surgeon has a similar Informed Score (within 5 points) \
to the top-ranked surgeon, specifically call them out as a recommended \
alternative and explain the potential benefits of choosing a robotic surgeon.
- If no surgeons are found in their exact city, the tool will search \
state-wide and suggest nearby cities — present these alternatives.
- Recommend the #1 ranked surgeon and ask if they'd like a full detailed \
profile generated for that surgeon (or a different one from the list).

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

## Important Rules
- Be warm, professional, and patient-centered in your tone.
- NEVER provide medical diagnoses or treatment recommendations.
- Frame health information as "factors your surgical team will consider."
- Always call `lookup_surgery_info` first when discussing a procedure — \
the wiki is the authoritative source when available.
- The Informed Score is a composite quality metric — explain it simply as \
"a quality score from 0-100 based on patient outcomes."
- When generating a full profile, ALWAYS call both `lookup_csv_performance` \
and `check_davinci_listing` — these are mandatory data sources.
- Available procedures in the database: Knee Replacement, Hip Replacement, \
Cholecystectomy, Hysterectomy (Benign/Malignant), Colon, Rectal, \
Prostatectomy, Lung Resection, Pancreatectomy, Splenectomy, Appendectomy, \
Gastric Bypass, Sleeve Gastrectomy, Lap Band Removal, Other Bariatrics, \
Abdominal Hernia, Inguinal Hernia, Hiatal Hernia, Other Thoracic.
"""

# Load business rules from rules/ directory
RULES_DIR = os.path.join(BASE_DIR, "rules")
if os.path.isdir(RULES_DIR):
    _rules_parts = []
    for _fname in sorted(os.listdir(RULES_DIR)):
        if _fname.endswith(".md"):
            with open(os.path.join(RULES_DIR, _fname), "r", encoding="utf-8") as _f:
                _rules_parts.append(_f.read().strip())
    if _rules_parts:
        SYSTEM_PROMPT += f"""

## Business Rules (MANDATORY — always follow these)
The following rules are loaded from the rules/ directory and MUST be followed. \
They override any conflicting default behavior.

{chr(10).join(_rules_parts)}
"""


async def main():
    # Build the MCP server with all tools
    server = create_sdk_mcp_server(
        "surgeon-tools",
        tools=[
            lookup_surgery_info_tool,
            find_best_surgeon_tool,
            lookup_knowledge_tool,
            lookup_npi_tool,
            lookup_csv_tool,
            check_davinci_tool,
            research_surgeon_tool,
            generate_profile_tool,
        ],
    )

    # Determine the prompt — default to starting the conversation
    if len(sys.argv) > 1:
        prompt = " ".join(sys.argv[1:])
    else:
        prompt = "Begin the patient consultation. Start by asking me what surgery I need."

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
        ],
        permission_mode="acceptEdits",
        max_turns=50,
        system_prompt=SYSTEM_PROMPT,
        mcp_servers={"surgeon-tools": server},
    )

    async with ClaudeSDKClient(options=options) as client:
        # Initial prompt to start the conversation
        await client.query(prompt)
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(block.text)
            elif isinstance(message, ResultMessage):
                print(f"\n{'='*60}")
                print(message.result)
                print(f"{'='*60}")
                print(f"[stop_reason: {message.stop_reason}]")
            elif isinstance(message, SystemMessage) and message.subtype == "init":
                session_id = message.data.get("session_id")
                print(f"[session: {session_id}]")

        # Conversation loop — keep going until the user is done
        while True:
            try:
                user_input = input("\nYou: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break

            if not user_input:
                continue
            if user_input.lower() in ("exit", "quit", "bye", "done"):
                print("Thank you for using SurgeonAgent. Good luck with your procedure!")
                break

            await client.query(user_input)
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            print(block.text)
                elif isinstance(message, ResultMessage):
                    print(f"\n{'='*60}")
                    print(message.result)
                    print(f"{'='*60}")


if __name__ == "__main__":
    anyio.run(main)
