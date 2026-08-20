"""
build_davinci_cache.py — batch scraper for the Intuitive da Vinci Physician Locator.

Runs state-by-state (50 + DC + PR), queries a curated set of seed cities per
state with a 100-mile radius, deduplicates results by NPI (or by
firstname+lastname+city+state as fallback), and writes a single
`davinci_cache.json` that the agent loads for fast lookups.

Also uploads the resulting cache to Supabase Storage (bucket configurable via
DAVINCI_BUCKET env var, defaults to "davinci-cache").

Usage:
    python build_davinci_cache.py                     # full run, all states
    python build_davinci_cache.py --states TX,CA      # only these states
    python build_davinci_cache.py --resume            # skip states with existing per-state files
    python build_davinci_cache.py --no-upload         # skip Supabase upload (write locally only)
    python build_davinci_cache.py --dry-run           # show what would be queried, don't hit API

Environment:
    SUPABASE_URL          — required for upload
    SUPABASE_SERVICE_KEY  — required for upload (service role bypasses RLS)
    DAVINCI_TABLE         — optional, defaults to "davinci_surgeons"
    NOMINATIM_USER_AGENT  — optional custom UA for geocoding

Prerequisite: run supabase_schema.sql in the Supabase SQL editor once to
create the davinci_surgeons table with the required indexes.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("build-davinci")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_CACHE_DIR = os.path.join(BASE_DIR, "davinci_cache_by_state")
FINAL_CACHE_PATH = os.path.join(BASE_DIR, "davinci_cache.json")
GEOCODE_CACHE_PATH = os.path.join(BASE_DIR, "davinci_cache_by_state", "_geocode_cache.json")

# ---------------------------------------------------------------------------
# Curated seed cities per state.
# Chosen for population + geographic spread — a 100-mile radius from each
# covers most of the state's populated regions.
# ---------------------------------------------------------------------------

SEED_CITIES: dict[str, list[str]] = {
    "AL": ["Birmingham", "Montgomery", "Mobile", "Huntsville", "Tuscaloosa"],
    "AK": ["Anchorage", "Fairbanks", "Juneau", "Wasilla", "Ketchikan"],
    "AZ": ["Phoenix", "Tucson", "Flagstaff", "Yuma", "Prescott"],
    "AR": ["Little Rock", "Fort Smith", "Fayetteville", "Jonesboro", "Texarkana"],
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento",
           "Fresno", "Redding", "Bakersfield", "Eureka"],
    "CO": ["Denver", "Colorado Springs", "Grand Junction", "Pueblo", "Durango"],
    "CT": ["Hartford", "Bridgeport", "New Haven", "Stamford"],
    "DE": ["Wilmington", "Dover", "Georgetown"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville",
           "Tallahassee", "Pensacola", "Fort Myers", "Key West"],
    "GA": ["Atlanta", "Savannah", "Macon", "Columbus", "Augusta", "Albany"],
    "HI": ["Honolulu", "Hilo", "Kahului", "Lihue"],
    "ID": ["Boise", "Idaho Falls", "Coeur d'Alene", "Twin Falls", "Pocatello"],
    "IL": ["Chicago", "Springfield", "Rockford", "Peoria", "Champaign", "Carbondale"],
    "IN": ["Indianapolis", "Fort Wayne", "Evansville", "South Bend", "Bloomington"],
    "IA": ["Des Moines", "Cedar Rapids", "Davenport", "Sioux City", "Waterloo"],
    "KS": ["Wichita", "Kansas City", "Topeka", "Manhattan", "Dodge City"],
    "KY": ["Louisville", "Lexington", "Bowling Green", "Owensboro", "Paducah"],
    "LA": ["New Orleans", "Baton Rouge", "Shreveport", "Lafayette", "Lake Charles", "Monroe"],
    "ME": ["Portland", "Bangor", "Augusta", "Presque Isle"],
    "MD": ["Baltimore", "Annapolis", "Rockville", "Cumberland", "Salisbury"],
    "MA": ["Boston", "Springfield", "Worcester", "Pittsfield"],
    "MI": ["Detroit", "Grand Rapids", "Lansing", "Traverse City", "Marquette"],
    "MN": ["Minneapolis", "Duluth", "Rochester", "St. Cloud", "Bemidji"],
    "MS": ["Jackson", "Gulfport", "Hattiesburg", "Tupelo", "Greenville"],
    "MO": ["Kansas City", "St. Louis", "Springfield", "Columbia", "Cape Girardeau"],
    "MT": ["Billings", "Missoula", "Great Falls", "Bozeman", "Kalispell", "Helena", "Miles City"],
    "NE": ["Omaha", "Lincoln", "Grand Island", "Scottsbluff", "Norfolk"],
    "NV": ["Las Vegas", "Reno", "Carson City", "Elko"],
    "NH": ["Manchester", "Concord", "Portsmouth", "Lebanon"],
    "NJ": ["Newark", "Jersey City", "Trenton", "Atlantic City"],
    "NM": ["Albuquerque", "Santa Fe", "Las Cruces", "Farmington", "Roswell"],
    "NY": ["New York", "Buffalo", "Rochester", "Syracuse", "Albany", "Plattsburgh"],
    "NC": ["Charlotte", "Raleigh", "Greensboro", "Asheville", "Wilmington", "Fayetteville"],
    "ND": ["Fargo", "Bismarck", "Grand Forks", "Minot", "Williston"],
    "OH": ["Columbus", "Cleveland", "Cincinnati", "Toledo", "Dayton", "Youngstown"],
    "OK": ["Oklahoma City", "Tulsa", "Lawton", "Enid", "McAlester"],
    "OR": ["Portland", "Eugene", "Bend", "Medford", "Pendleton"],
    "PA": ["Philadelphia", "Pittsburgh", "Harrisburg", "Erie", "Scranton", "State College"],
    "RI": ["Providence", "Newport"],
    "SC": ["Columbia", "Charleston", "Greenville", "Myrtle Beach", "Florence"],
    "SD": ["Sioux Falls", "Rapid City", "Pierre", "Aberdeen"],
    "TN": ["Nashville", "Memphis", "Knoxville", "Chattanooga", "Johnson City", "Jackson"],
    "TX": ["Houston", "Dallas", "San Antonio", "Austin", "Fort Worth",
           "El Paso", "Amarillo", "Lubbock", "Corpus Christi", "McAllen", "Odessa"],
    "UT": ["Salt Lake City", "Provo", "St. George", "Ogden", "Vernal"],
    "VT": ["Burlington", "Montpelier", "Rutland", "Brattleboro"],
    "VA": ["Richmond", "Virginia Beach", "Roanoke", "Charlottesville", "Fredericksburg", "Abingdon"],
    "WA": ["Seattle", "Spokane", "Tacoma", "Bellingham", "Yakima", "Walla Walla"],
    "WV": ["Charleston", "Huntington", "Morgantown", "Wheeling", "Beckley"],
    "WI": ["Milwaukee", "Madison", "Green Bay", "Eau Claire", "Wausau"],
    "WY": ["Cheyenne", "Casper", "Jackson", "Gillette", "Rock Springs"],
    "DC": ["Washington"],
    "PR": ["San Juan", "Ponce", "Mayaguez", "Aguadilla", "Arecibo"],
}

# ---------------------------------------------------------------------------
# HTTP config
# ---------------------------------------------------------------------------

INTUITIVE_API = "https://www.intuitive.com/api/provider-locator/search"
INTUITIVE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.intuitive.com",
    "Referer": "https://www.intuitive.com/en-us/physician-locator/search/search",
}

NOMINATIM_USER_AGENT = os.environ.get(
    "NOMINATIM_USER_AGENT", "SurgeonAgent-Cache/1.0 (build_davinci_cache.py)"
)

REQUEST_DELAY_SECONDS = 0.5   # be a good neighbor
MAX_RADIUS_MILES = 100
MAX_PAGES_PER_QUERY = 25       # 20 per page → up to 500 surgeons per seed city
API_MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Geocode cache (avoid re-hitting Nominatim)
# ---------------------------------------------------------------------------

_geocode_cache: dict[str, tuple[float, float]] = {}


def _load_geocode_cache():
    global _geocode_cache
    if os.path.exists(GEOCODE_CACHE_PATH):
        try:
            with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            _geocode_cache = {k: tuple(v) for k, v in raw.items()}
            logger.info(f"Loaded {len(_geocode_cache)} geocoded seed cities from cache")
        except Exception as e:
            logger.warning(f"Could not load geocode cache: {e}")


def _save_geocode_cache():
    os.makedirs(STATE_CACHE_DIR, exist_ok=True)
    try:
        with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({k: list(v) for k, v in _geocode_cache.items()}, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save geocode cache: {e}")


def geocode(city: str, state: str) -> Optional[tuple[float, float]]:
    """Return (lat, lng) for a city/state, cached across runs."""
    key = f"{city}|{state}"
    if key in _geocode_cache:
        return _geocode_cache[key]

    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{city}, {state}, USA", "format": "json", "limit": 1},
            headers={"User-Agent": NOMINATIM_USER_AGENT},
            timeout=15,
        )
        data = resp.json()
        if data:
            latlng = (float(data[0]["lat"]), float(data[0]["lon"]))
            _geocode_cache[key] = latlng
            time.sleep(1.0)  # Nominatim usage policy: max 1 req/sec
            return latlng
    except Exception as e:
        logger.warning(f"Geocode failed for {city}, {state}: {e}")
    return None


# ---------------------------------------------------------------------------
# Intuitive API pagination
# ---------------------------------------------------------------------------


def query_intuitive_page(location: str, lat: float, lng: float, page: int) -> Optional[dict]:
    """POST one page of results. Returns parsed JSON or None on failure."""
    payload = {
        "location": location,
        "search": "physician",
        "distance": MAX_RADIUS_MILES,
        "page": page,
        "pageSize": 20,
        "lat": lat,
        "long": lng,
    }
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                INTUITIVE_API, json=payload, headers=INTUITIVE_HEADERS, timeout=20
            )
            if resp.status_code == 429:
                wait = 5 * attempt
                logger.warning(f"429 rate limit; sleeping {wait}s (attempt {attempt}/{API_MAX_RETRIES})")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = 2 * attempt
                logger.warning(f"{resp.status_code} server error; sleeping {wait}s (attempt {attempt}/{API_MAX_RETRIES})")
                time.sleep(wait)
                continue
            if not resp.ok:
                logger.warning(f"HTTP {resp.status_code} for {location} page {page}: {resp.text[:200]}")
                return None
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"Request exception for {location} page {page} (attempt {attempt}): {e}")
            time.sleep(2 * attempt)
    return None


def scrape_seed_city(city: str, state: str) -> list[dict]:
    """Fetch every physician result within 100 miles of the given city."""
    latlng = geocode(city, state)
    if not latlng:
        logger.warning(f"  {city}, {state}: could not geocode, skipping")
        return []
    lat, lng = latlng
    location = f"{city}, {state}"
    all_results: list[dict] = []

    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        data = query_intuitive_page(location, lat, lng, page)
        if not data:
            break
        results = data.get("Results", [])
        if not results:
            break
        all_results.extend(results)

        total = data.get("TotalCount", 0)
        logger.info(f"  {city}, {state} page {page}: +{len(results)} (running total {len(all_results)}, TotalCount {total})")
        if page * 20 >= total:
            break
        time.sleep(REQUEST_DELAY_SECONDS)

    return all_results


# ---------------------------------------------------------------------------
# Normalization + dedup
# ---------------------------------------------------------------------------


def normalize_entry(raw_entry: dict) -> Optional[dict]:
    """Extract the useful fields from one API result row."""
    raw = raw_entry.get("Raw", {})
    firstname = (raw.get("Firstname") or "").strip()
    lastname = (raw.get("Lastname") or "").strip()
    if not lastname:
        return None

    npi = (raw.get("NPI") or raw.get("Npi") or "").strip()
    seo_url = raw.get("Seourl", "")
    profile_url = (
        f"https://www.intuitive.com/en-us/physician-locator/surgeon/{seo_url}"
        if seo_url else ""
    )

    hospitals_raw = raw.get("Hospitallist", []) or []
    hospitals = []
    for h in hospitals_raw:
        if isinstance(h, dict):
            n = h.get("name") or h.get("Name") or ""
            if n:
                hospitals.append(n)
        elif isinstance(h, str):
            hospitals.append(h)

    return {
        "npi": npi,
        "firstname": firstname,
        "lastname": lastname,
        "city": (raw.get("City") or "").strip(),
        "state": (raw.get("State") or "").strip(),
        "location": raw.get("Location", ""),
        "profile_url": profile_url,
        "procedure_count": raw.get("Surgeonlocatorprocedurecount", ""),
        "procedure_category": raw.get("Surgeonlocatorprocedurecountcategory", ""),
        "specialties": raw.get("Surgeonlocatorspecialities", []) or [],
        "procedures": raw.get("Surgeonlocatorprocedures", []) or [],
        "hospitals": hospitals,
    }


def dedupe_entries(entries: list[dict]) -> list[dict]:
    """Deduplicate by NPI, falling back to (firstname, lastname, city, state)."""
    seen_npis: set[str] = set()
    seen_names: set[tuple] = set()
    unique: list[dict] = []

    for e in entries:
        npi = e.get("npi", "")
        if npi:
            if npi in seen_npis:
                continue
            seen_npis.add(npi)
            unique.append(e)
        else:
            key = (
                e.get("firstname", "").lower(),
                e.get("lastname", "").lower(),
                e.get("city", "").lower(),
                e.get("state", "").upper(),
            )
            if key in seen_names:
                continue
            seen_names.add(key)
            unique.append(e)
    return unique


# ---------------------------------------------------------------------------
# Per-state driver
# ---------------------------------------------------------------------------


def state_output_path(state: str) -> str:
    return os.path.join(STATE_CACHE_DIR, f"{state}.json")


def scrape_state(state: str) -> dict:
    """Scrape one state; return {"state": ..., "entries": [...], "seed_count": N}."""
    cities = SEED_CITIES.get(state, [])
    if not cities:
        logger.warning(f"No seed cities defined for {state}, skipping")
        return {"state": state, "entries": [], "seed_count": 0}

    logger.info(f"[{state}] scraping {len(cities)} seed cities: {', '.join(cities)}")
    raw: list[dict] = []
    for city in cities:
        page_results = scrape_seed_city(city, state)
        for entry in page_results:
            normalized = normalize_entry(entry)
            if normalized:
                raw.append(normalized)
        # Save geocode cache incrementally
        _save_geocode_cache()

    unique = dedupe_entries(raw)
    logger.info(f"[{state}] collected {len(raw)} raw / {len(unique)} unique surgeons")
    return {"state": state, "entries": unique, "seed_count": len(cities)}


def save_state(state: str, payload: dict):
    os.makedirs(STATE_CACHE_DIR, exist_ok=True)
    with open(state_output_path(state), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ---------------------------------------------------------------------------
# Combine + upload
# ---------------------------------------------------------------------------


def combine_all_states(states: list[str]) -> dict:
    """Merge per-state cache files into a single cache with indices."""
    by_npi: dict[str, dict] = {}
    by_name_city: dict[str, str] = {}  # secondary index → npi (or synthetic id)
    orphan_no_npi: list[dict] = []
    states_covered: list[str] = []

    for state in states:
        path = state_output_path(state)
        if not os.path.exists(path):
            logger.warning(f"Skipping {state} in combine — no state file at {path}")
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        states_covered.append(state)
        for e in data.get("entries", []):
            npi = e.get("npi", "")
            if npi:
                by_npi[npi] = e
                key = f"{e.get('lastname','').lower()}|{e.get('firstname','').lower()}|{e.get('city','').lower()}|{e.get('state','').upper()}"
                by_name_city[key] = npi
            else:
                orphan_no_npi.append(e)
                key = f"{e.get('lastname','').lower()}|{e.get('firstname','').lower()}|{e.get('city','').lower()}|{e.get('state','').upper()}"
                # Store orphans under synthetic key so they're still queryable
                synthetic_id = f"noname:{key}"
                by_npi[synthetic_id] = e
                by_name_city[key] = synthetic_id

    return {
        "manifest": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_surgeons": len(by_npi),
            "surgeons_with_npi": sum(1 for k in by_npi if not k.startswith("noname:")),
            "surgeons_without_npi": len(orphan_no_npi),
            "states_covered": sorted(states_covered),
            "source": "Intuitive Physician Locator via www.intuitive.com/api/provider-locator/search",
        },
        "by_npi": by_npi,
        "by_name_city": by_name_city,
    }


UPSERT_BATCH_SIZE = 500


def _prepare_rows(combined: dict) -> list[dict]:
    """Flatten the combined cache into PostgREST-ready row dicts."""
    rows = []
    for key, entry in combined["by_npi"].items():
        rows.append({
            # Convert empty NPI to None so the generated dedup_key falls
            # back to the name+city composite.
            "npi": entry.get("npi") or None,
            "firstname": entry.get("firstname", ""),
            "lastname": entry.get("lastname", ""),
            "city": entry.get("city", ""),
            "state": entry.get("state", ""),
            "location": entry.get("location", ""),
            "profile_url": entry.get("profile_url", ""),
            "procedure_count": entry.get("procedure_count", ""),
            "procedure_category": entry.get("procedure_category", ""),
            "specialties": entry.get("specialties", []) or [],
            "procedures": entry.get("procedures", []) or [],
            "hospitals": entry.get("hospitals", []) or [],
        })
    return rows


def _upsert_batch(rows: list[dict], table_url: str, service_key: str) -> bool:
    """POST one batch of rows, upserting on the generated dedup_key."""
    headers = {
        "Authorization": f"Bearer {service_key}",
        "apikey": service_key,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    url = f"{table_url}?on_conflict=dedup_key"
    try:
        resp = requests.post(url, json=rows, headers=headers, timeout=60)
        if resp.ok:
            return True
        logger.error(
            f"Upsert failed: HTTP {resp.status_code} — {resp.text[:400]}"
        )
    except requests.RequestException as e:
        logger.error(f"Upsert exception: {e}")
    return False


def upload_to_supabase(combined: dict) -> bool:
    """Upsert the combined cache into the davinci_surgeons Supabase table."""
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    service_key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    table = os.environ.get("DAVINCI_TABLE", "davinci_surgeons").strip() or "davinci_surgeons"

    if not supabase_url or not service_key:
        logger.warning("SUPABASE_URL or SUPABASE_SERVICE_KEY missing — skipping upload")
        return False

    table_url = f"{supabase_url.rstrip('/')}/rest/v1/{table}"
    rows = _prepare_rows(combined)
    logger.info(f"Upserting {len(rows)} rows to '{table}' (batches of {UPSERT_BATCH_SIZE})")

    total_ok = 0
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        batch = rows[i:i + UPSERT_BATCH_SIZE]
        if not _upsert_batch(batch, table_url, service_key):
            logger.error(f"Aborting at batch {i // UPSERT_BATCH_SIZE + 1}")
            return False
        total_ok += len(batch)
        logger.info(f"  batch {i // UPSERT_BATCH_SIZE + 1}: {total_ok}/{len(rows)} rows")

    logger.info(f"Uploaded {total_ok} surgeons to table '{table}'")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", type=str, default="",
                        help="Comma-separated state codes to scrape (default: all)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip states that already have per-state cache files")
    parser.add_argument("--no-upload", action="store_true",
                        help="Skip Supabase Storage upload; write locally only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log what would be queried without hitting APIs")
    args = parser.parse_args()

    os.makedirs(STATE_CACHE_DIR, exist_ok=True)
    _load_geocode_cache()

    if args.states:
        target_states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
        unknown = [s for s in target_states if s not in SEED_CITIES]
        if unknown:
            logger.error(f"Unknown state codes: {unknown}")
            sys.exit(1)
    else:
        target_states = sorted(SEED_CITIES.keys())

    if args.dry_run:
        total_cities = sum(len(SEED_CITIES[s]) for s in target_states)
        logger.info(f"DRY RUN: would scrape {len(target_states)} states, {total_cities} seed cities")
        for s in target_states:
            logger.info(f"  {s}: {len(SEED_CITIES[s])} cities — {', '.join(SEED_CITIES[s])}")
        return

    completed = []
    failed = []
    started = time.time()

    for state in target_states:
        if args.resume and os.path.exists(state_output_path(state)):
            logger.info(f"[{state}] --resume: state file exists, skipping")
            completed.append(state)
            continue
        try:
            payload = scrape_state(state)
            save_state(state, payload)
            completed.append(state)
        except KeyboardInterrupt:
            logger.error("Interrupted by user")
            break
        except Exception as e:
            logger.exception(f"[{state}] failed: {e}")
            failed.append(state)

    _save_geocode_cache()

    if failed:
        logger.warning(f"States that failed: {failed}")
    if not completed:
        logger.error("No states completed; nothing to combine")
        sys.exit(1)

    logger.info(f"Combining {len(completed)} state files...")
    combined = combine_all_states(completed)
    with open(FINAL_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2)
    logger.info(
        f"Wrote {FINAL_CACHE_PATH}: "
        f"{combined['manifest']['total_surgeons']} surgeons across "
        f"{len(combined['manifest']['states_covered'])} states"
    )

    if not args.no_upload:
        upload_to_supabase(combined)

    elapsed = int(time.time() - started)
    logger.info(f"Done in {elapsed // 60}m {elapsed % 60}s")


if __name__ == "__main__":
    main()
