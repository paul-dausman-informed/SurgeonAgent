"""
Surgeon Research Module

Provides NPI Registry lookups and Healthgrades scraping for surgeon data.
Designed to be called by the agent or directly from CLI.
"""

import csv
import json
import os
import re
import time
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "research_cache")
PHOTO_DIR = os.path.join(CACHE_DIR, "photos")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _get(url, timeout=15):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        return None


def _format_phone(phone):
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


# ---------------------------------------------------------------------------
# NPI Registry API
# ---------------------------------------------------------------------------

def lookup_npi(npi: str) -> dict:
    """Query the NPI Registry for provider details."""
    url = f"https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1"
    resp = _get(url)
    if not resp:
        return {}

    data = resp.json()
    if data.get("result_count", 0) == 0:
        return {}

    result = data["results"][0]
    basic = result.get("basic", {})
    addresses = result.get("addresses", [])
    taxonomies = result.get("taxonomies", [])

    location = {}
    for addr in addresses:
        if addr.get("address_purpose") == "LOCATION":
            location = addr
            break
    if not location and addresses:
        location = addresses[0]

    return {
        "full_name": (
            f"{basic.get('first_name', '')} "
            f"{basic.get('middle_name', '')} "
            f"{basic.get('last_name', '')}"
        ).replace("  ", " ").strip(),
        "credential": basic.get("credential", ""),
        "sex": basic.get("sex", ""),
        "enumeration_date": basic.get("enumeration_date", ""),
        "address": location.get("address_1", ""),
        "city": location.get("city", ""),
        "state": location.get("state", ""),
        "zip": location.get("postal_code", ""),
        "phone": _format_phone(location.get("telephone_number", "")),
        "fax": location.get("fax_number", ""),
        "taxonomies": [
            {"code": t.get("code", ""), "desc": t.get("desc", ""), "primary": t.get("primary", False)}
            for t in taxonomies
        ],
    }


# ---------------------------------------------------------------------------
# Healthgrades
# ---------------------------------------------------------------------------

def find_healthgrades_url(first_name, last_name, city, state):
    """Search Healthgrades to find the physician's profile URL."""
    search_url = (
        f"https://www.healthgrades.com/usearch"
        f"?what={quote_plus(first_name + ' ' + last_name)}"
        f"&where={quote_plus(city + ', ' + state)}"
    )
    resp = _get(search_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    last_lower = last_name.lower().strip()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/physician/" in href and last_lower in href.lower():
            base = href.split("#")[0]
            if base.startswith("/"):
                return f"https://www.healthgrades.com{base}"
            return base

    return None


def scrape_healthgrades(url):
    """Scrape Healthgrades profile using JSON-LD structured data."""
    resp = _get(url)
    if not resp:
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    ld_blocks = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, dict):
                ld_blocks.append(ld)
        except (json.JSONDecodeError, TypeError):
            pass

    result = {
        "photo_url": "",
        "description": "",
        "rating": None,
        "review_count": None,
        "affiliations": [],
        "education": [],
        "awards": [],
        "locations": [],
        "reviews": [],
    }

    seen_affiliations = set()
    seen_education = set()

    for ld in ld_blocks:
        if "image" in ld and not result["photo_url"]:
            img = ld["image"]
            if isinstance(img, str) and "prov" in img:
                if img.startswith("//"):
                    img = "https:" + img
                result["photo_url"] = img.split("?")[0]

        desc = ld.get("description", "")
        if desc and len(desc) > len(result["description"]):
            result["description"] = desc

        if "aggregateRating" in ld:
            ar = ld["aggregateRating"]
            rv = ar.get("ratingValue")
            rc = ar.get("reviewCount")
            if rv and (result["rating"] is None or isinstance(rv, float)):
                result["rating"] = rv
                result["review_count"] = rc

        if "hospitalAffiliation" in ld:
            ha = ld["hospitalAffiliation"]
            if isinstance(ha, dict):
                name = ha.get("name", "")
                addr = ha.get("address", {})
                key = name.lower()
                if name and key not in seen_affiliations:
                    seen_affiliations.add(key)
                    result["affiliations"].append({
                        "name": name,
                        "city": addr.get("addressLocality", ""),
                        "state": addr.get("addressRegion", ""),
                    })

        if "alumni" in ld:
            al = ld["alumni"]
            if isinstance(al, dict):
                alumni_of = al.get("alumniOf", {})
                if isinstance(alumni_of, dict):
                    name = alumni_of.get("name", "")
                    if name and name.lower() not in seen_education:
                        seen_education.add(name.lower())
                        result["education"].append(name)

        if "award" in ld:
            aw = ld["award"]
            if isinstance(aw, list):
                result["awards"].extend(aw)
            elif isinstance(aw, str):
                result["awards"].append(aw)

        if "location" in ld:
            loc = ld["location"]
            if isinstance(loc, dict):
                addr = loc.get("address", {})
                result["locations"].append({
                    "street": addr.get("streetAddress", ""),
                    "city": addr.get("addressLocality", ""),
                    "state": addr.get("addressRegion", ""),
                    "zip": addr.get("postalCode", ""),
                })

        if "review" in ld:
            reviews = ld["review"]
            if isinstance(reviews, dict):
                reviews = [reviews]
            if isinstance(reviews, list):
                for rev in reviews[:5]:
                    if isinstance(rev, dict) and rev.get("reviewBody"):
                        result["reviews"].append({
                            "body": rev["reviewBody"][:200],
                            "rating": rev.get("reviewRating", {}).get("ratingValue"),
                            "date": rev.get("datePublished", ""),
                        })

    result["awards"] = list(dict.fromkeys(result["awards"]))
    return result


def download_photo(npi, photo_url):
    """Download a surgeon's photo and return the local file path, or None."""
    if not photo_url:
        return None

    os.makedirs(PHOTO_DIR, exist_ok=True)
    local_path = os.path.join(PHOTO_DIR, f"{npi}.jpg")

    if os.path.exists(local_path) and os.path.getsize(local_path) > 100:
        return local_path

    resp = _get(photo_url)
    if not resp or len(resp.content) < 100:
        return None

    header = resp.content[:4]
    if header[:2] != b'\xff\xd8' and header[:4] != b'\x89PNG':
        return None

    with open(local_path, "wb") as f:
        f.write(resp.content)
    return local_path


# ---------------------------------------------------------------------------
# Full research pipeline
# ---------------------------------------------------------------------------

def research_surgeon(npi, first_name, last_name, middle_name="", city="",
                     state="", specialty=""):
    """Perform comprehensive web research for a surgeon. Returns a dict."""
    data = {
        "npi": npi,
        "practice_name": "",
        "practice_website": "",
        "photo_path": "",
        "address": "",
        "city": city,
        "state": state,
        "zip": "",
        "phone": "",
        "description": "",
        "education": [],
        "board_certs": [],
        "memberships": [],
        "languages": ["English"],
        "affiliations": [],
        "ratings": [],
        "awards": [],
        "media": [],
        "source_urls": [],
        "locations": [],
    }

    # 1. NPI Registry
    npi_data = lookup_npi(npi)
    if npi_data:
        data["address"] = npi_data.get("address", "")
        data["city"] = npi_data.get("city", "") or city
        data["state"] = npi_data.get("state", "") or state
        data["zip"] = npi_data.get("zip", "")
        data["phone"] = npi_data.get("phone", "")
        data["source_urls"].append(
            f"https://npiregistry.cms.hhs.gov/provider-view/{npi}"
        )
        for tax in npi_data.get("taxonomies", []):
            desc = tax.get("desc", "")
            if desc:
                cert = f"Board Certified, {desc}"
                if cert not in data["board_certs"]:
                    data["board_certs"].append(cert)

    time.sleep(0.5)

    # 2. Healthgrades
    hg_url = find_healthgrades_url(first_name, last_name, city, state)
    if hg_url:
        data["source_urls"].append(hg_url)
        time.sleep(0.5)

        hg = scrape_healthgrades(hg_url)

        if hg.get("description"):
            data["description"] = hg["description"]

        if hg.get("rating"):
            rating_val = hg["rating"]
            if isinstance(rating_val, float):
                rating_val = round(rating_val, 1)
            data["ratings"].append({
                "platform": "Healthgrades",
                "rating": f"{rating_val} / 5.0",
                "notes": f"{hg.get('review_count', '')} reviews",
            })

        for aff in hg.get("affiliations", []):
            name = aff.get("name", "")
            existing_names = [a["name"].lower() for a in data["affiliations"]]
            if name and name.lower() not in existing_names:
                data["affiliations"].append(aff)

        for edu in hg.get("education", []):
            if edu and edu not in data["education"]:
                data["education"].append(edu)

        for award in hg.get("awards", []):
            if award and award not in data["awards"]:
                data["awards"].append(award)

        data["locations"] = hg.get("locations", [])

        desc = hg.get("description", "")
        practice_match = re.search(
            r"(?:practices? at|affiliated with|works? at)\s+([^.]+)",
            desc, re.IGNORECASE
        )
        if practice_match:
            data["practice_name"] = practice_match.group(1).strip()

        if hg.get("photo_url"):
            photo_path = download_photo(npi, hg["photo_url"])
            if photo_path:
                data["photo_path"] = photo_path

    # Reference URLs
    first_l = first_name.lower().strip()
    last_l = last_name.lower().strip()
    data["source_urls"].append(
        f"https://doctor.webmd.com/results?type=name&query={quote_plus(first_name + ' ' + last_name)}"
    )
    data["source_urls"].append(
        f"https://health.usnews.com/doctors?name={quote_plus(first_name + ' ' + last_name)}"
        f"&location={quote_plus(city + ', ' + state)}"
    )
    data["source_urls"].append(f"https://www.doximity.com/pub/{first_l}-{last_l}-md")

    return data


# ---------------------------------------------------------------------------
# Intuitive da Vinci Physician Locator
# ---------------------------------------------------------------------------

def _geocode_city_state(city: str, state: str):
    """Geocode a city/state to (lat, lng) using OpenStreetMap Nominatim."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{city}, {state}, USA",
        "format": "json",
        "limit": 1,
    }
    try:
        resp = requests.get(
            url, params=params,
            headers={"User-Agent": "SurgeonAgent/1.0"},
            timeout=10,
        )
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None, None


def check_intuitive_davinci(first_name: str, last_name: str, city: str, state: str) -> dict:
    """Check if a surgeon is listed on the Intuitive da Vinci Physician Locator.

    Queries the Intuitive provider-locator API by geocoding the surgeon's
    city/state and searching within a 100-mile radius.
    Returns {"listed": True/False, "details": str, "profile_url": str}.
    """
    result = {"listed": False, "details": "", "profile_url": ""}

    # Step 1: Geocode the city/state
    lat, lng = _geocode_city_state(city, state)
    if lat is None or lng is None:
        result["details"] = (
            "Could not geocode location for Intuitive Physician Locator search"
        )
        return result

    # Step 2: Query the Intuitive provider-locator API
    api_url = "https://www.intuitive.com/api/provider-locator/search"
    api_headers = {
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

    last_lower = last_name.lower().strip()
    first_lower = first_name.lower().strip()
    location_str = f"{city}, {state}".strip(", ")

    # Paginate through all results (20 per page)
    page = 1
    max_pages = 10
    while page <= max_pages:
        payload = {
            "location": location_str,
            "search": "physician",
            "distance": 100,
            "page": page,
            "pageSize": 20,
            "lat": lat,
            "long": lng,
        }
        try:
            resp = requests.post(
                api_url, json=payload, headers=api_headers, timeout=15
            )
            data = resp.json()
        except Exception:
            result["details"] = "Could not reach Intuitive Physician Locator API"
            return result

        if not isinstance(data, dict):
            break

        results_list = data.get("Results", [])
        if not results_list:
            break

        for entry in results_list:
            raw = entry.get("Raw", {})
            entry_last = raw.get("Lastname", "").lower().strip()
            entry_first = raw.get("Firstname", "").lower().strip()

            if entry_last == last_lower and entry_first == first_lower:
                result["listed"] = True

                # Extract details
                seo_url = raw.get("Seourl", "")
                if seo_url:
                    result["profile_url"] = (
                        f"https://www.intuitive.com/en-us/"
                        f"physician-locator/surgeon/{seo_url}"
                    )

                proc_count = raw.get("Surgeonlocatorprocedurecount", "")
                proc_category = raw.get(
                    "Surgeonlocatorprocedurecountcategory", ""
                )
                specialties = raw.get("Surgeonlocatorspecialities", [])
                procedures = raw.get("Surgeonlocatorprocedures", [])
                hospitals = raw.get("Hospitallist", [])
                location = raw.get("Location", "")

                details_parts = [
                    "Listed on Intuitive da Vinci Physician Locator"
                ]
                if proc_count:
                    details_parts.append(
                        f"da Vinci procedure count: {proc_count}"
                    )
                elif proc_category:
                    details_parts.append(
                        f"da Vinci procedures: {proc_category}"
                    )
                if specialties:
                    details_parts.append(
                        f"Specialties: {', '.join(specialties)}"
                    )
                if procedures:
                    details_parts.append(
                        f"Procedures: {', '.join(procedures[:5])}"
                        + (" ..." if len(procedures) > 5 else "")
                    )
                if hospitals and isinstance(hospitals, list):
                    hosp_names = []
                    for h in hospitals:
                        if isinstance(h, dict):
                            hosp_names.append(h.get("name", ""))
                        elif isinstance(h, str):
                            hosp_names.append(h)
                    if hosp_names:
                        details_parts.append(
                            f"Hospitals: {', '.join(hosp_names)}"
                        )
                if location:
                    details_parts.append(f"Location: {location}")

                result["details"] = "; ".join(details_parts)
                return result

        # Check if there are more pages
        total = data.get("TotalCount", 0)
        if page * 20 >= total:
            break
        page += 1

    if not result["listed"]:
        result["details"] = "Not found on Intuitive da Vinci Physician Locator"

    return result


# ---------------------------------------------------------------------------
# CSV Performance Data
# ---------------------------------------------------------------------------

CSV_PATH = os.path.join(BASE_DIR, "NationalTop80Score.csv")


def lookup_csv(npi: str) -> dict | None:
    """Look up a surgeon by NPI in the CSV and return structured performance data.

    Returns a dict with procedures, facilities, demographics, and metrics,
    or None if the NPI is not found.
    """
    if not os.path.exists(CSV_PATH):
        return None

    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["NPI"] == npi:
                rows.append(row)

    if not rows:
        return None

    first_row = rows[0]

    # Deduplicate procedures
    seen_procs = {}
    for row in rows:
        proc = row["Procedure"]
        if proc not in seen_procs:
            seen_procs[proc] = row
    procedures = []
    for proc_name, row in seen_procs.items():
        proc = {
            "name": proc_name,
            "informed_score": row.get("Informed Score", ""),
            "cases": row.get("Cases", ""),
            "state_rank": row.get("State Rank", ""),
            "complication_free_rate": "",
            "avg_90_day_cost": "",
            "length_of_stay": "",
        }
        try:
            proc["complication_free_rate"] = f"{float(row['prop_complication_free_surgeon']):.1%}"
        except (ValueError, KeyError):
            pass
        try:
            proc["avg_90_day_cost"] = f"${float(row['Raw 90 Day Cost']):,.0f}"
        except (ValueError, KeyError):
            pass
        try:
            proc["length_of_stay"] = f"{float(row['los_elective']):.2f} days"
        except (ValueError, KeyError):
            pass
        procedures.append(proc)

    # Deduplicate facilities
    seen_fac = set()
    facilities = []
    for row in rows:
        name = row["Facility Name"].strip().title()
        if name and name not in seen_fac:
            seen_fac.add(name)
            facilities.append({
                "name": name,
                "address": row.get("Address", "").strip().title(),
                "city": row.get("City", "").strip().title(),
                "state": row.get("State", "").strip(),
                "zip": row.get("Zip", "").strip(),
                "phone": row.get("Phone", "").strip(),
            })

    return {
        "npi": npi,
        "last_name": first_row.get("Last Name", "").strip().title(),
        "first_name": first_row.get("First Name", "").strip().title(),
        "middle": first_row.get("Middle", "").strip().title(),
        "specialty": first_row.get("Specialty", "").strip().title(),
        "gender": first_row.get("Gender", ""),
        "credential": first_row.get("Credential", "M.D."),
        "medical_school": first_row.get("Medical School", "").strip().title(),
        "procedures": procedures,
        "facilities": facilities,
    }


# ---------------------------------------------------------------------------
# Surgery Knowledge Base (wiki lookup)
# ---------------------------------------------------------------------------

WIKI_DIR = r"C:\Users\pauld\Knowledge\Surgery\wiki"


def lookup_surgery_wiki(surgery_name: str) -> dict:
    """Search the Surgery wiki knowledge base for information about a procedure.

    Looks for markdown files in the wiki directory whose filename or content
    matches the surgery name. Returns {"found": bool, "content": str, "source": str}.
    """
    result = {"found": False, "content": "", "source": ""}

    if not os.path.isdir(WIKI_DIR):
        return result

    surgery_lower = surgery_name.lower().strip()
    surgery_words = surgery_lower.split()

    best_match = None
    best_score = 0

    for fname in os.listdir(WIKI_DIR):
        if not fname.endswith(".md"):
            continue

        fpath = os.path.join(WIKI_DIR, fname)
        fname_lower = fname.lower().replace("-", " ").replace("_", " ").replace(".md", "")

        # Score by filename match
        score = 0
        if surgery_lower in fname_lower or fname_lower in surgery_lower:
            score = 10
        else:
            for word in surgery_words:
                if len(word) > 2 and word in fname_lower:
                    score += 2

        # If filename is promising, also check content
        if score > 0:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    content = f.read()
                # Boost score if surgery name appears in content
                if surgery_lower in content.lower():
                    score += 5
            except Exception:
                content = ""

            if score > best_score:
                best_score = score
                best_match = (fpath, fname, content)

    # If no filename match, do a content scan of all files
    if best_match is None:
        for fname in os.listdir(WIKI_DIR):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(WIKI_DIR, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    content = f.read()
                if surgery_lower in content.lower():
                    best_match = (fpath, fname, content)
                    break
            except Exception:
                continue

    if best_match:
        fpath, fname, content = best_match
        result["found"] = True
        result["content"] = content
        result["source"] = fpath

    return result


def list_surgery_wiki_topics() -> list[str]:
    """List all available topics in the Surgery wiki."""
    if not os.path.isdir(WIKI_DIR):
        return []
    return [
        f.replace(".md", "").replace("-", " ").replace("_", " ").title()
        for f in os.listdir(WIKI_DIR)
        if f.endswith(".md") and f.upper() != "INDEX.MD"
    ]


# ---------------------------------------------------------------------------
# Find Best Surgeon by City + Procedure  (with CBSA metro-area matching)
# ---------------------------------------------------------------------------

SCORES_CSV_PATH = os.path.join(BASE_DIR, "SurgeonScores", "NationalTop80Score.csv")
CBSA_LOOKUP_PATH = os.path.join(BASE_DIR, "cbsa_lookup.json")

# Load CBSA lookup once at import time.
# JSON structure: {"zips": {zip5: {code, title}}, "counties": {"name|ST": {code, title}}}
_CBSA_ZIPS: dict = {}      # zip5 -> {"code": "16974", "title": "Chicago-..."}
_CBSA_COUNTIES: dict = {}  # "tarrant county|TX" -> {"code": "19124", "title": "Dallas-..."}
if os.path.exists(CBSA_LOOKUP_PATH):
    try:
        with open(CBSA_LOOKUP_PATH, "r", encoding="utf-8") as _f:
            _cbsa_data = json.load(_f)
            _CBSA_ZIPS = _cbsa_data.get("zips", _cbsa_data)  # backwards compat
            _CBSA_COUNTIES = _cbsa_data.get("counties", {})
    except Exception:
        pass


def _get_cbsa_code(zip5: str) -> str:
    """Return the CBSA code for a 5-digit ZIP, or '' if not in a metro area."""
    entry = _CBSA_ZIPS.get(zip5[:5])
    return entry["code"] if entry else ""


def _get_cbsa_title(zip5: str) -> str:
    """Return the CBSA metro area name for a ZIP, or '' if unknown."""
    entry = _CBSA_ZIPS.get(zip5[:5])
    return entry["title"] if entry else ""


def _geocode_city_to_cbsa(city: str, state: str) -> tuple:
    """Geocode a city/state and find its CBSA via county name from Nominatim.

    Returns (cbsa_code, cbsa_title) or ('', '').
    Nominatim returns the county for city-level queries, which we then
    match against our county-to-CBSA lookup.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{city}, {state}, USA",
        "format": "json",
        "addressdetails": 1,
        "limit": 1,
    }
    try:
        resp = requests.get(
            url, params=params,
            headers={"User-Agent": "SurgeonAgent/1.0"},
            timeout=10,
        )
        data = resp.json()
        if data and "address" in data[0]:
            addr = data[0]["address"]

            # Try county name lookup (most reliable for city-level queries)
            county = addr.get("county", "")
            state_abbr = state.upper().strip()
            if county and state_abbr and _CBSA_COUNTIES:
                key = f"{county.lower()}|{state_abbr}"
                entry = _CBSA_COUNTIES.get(key)
                if entry:
                    return entry["code"], entry["title"]

            # Fallback: try postcode if available (specific address queries)
            postcode = addr.get("postcode", "")
            if postcode:
                zip5 = postcode.split("-")[0].split(":")[0].strip()[:5]
                code = _get_cbsa_code(zip5)
                if code:
                    return code, _get_cbsa_title(zip5)
    except Exception:
        pass
    return "", ""


def find_best_surgeon(city: str, procedure: str, state: str = "", top_n: int = 5) -> dict:
    """Find the top-scoring surgeons for a procedure near a given city.

    Search priority (CBSA-aware):
      1. Exact city match (surgeons physically in the named city)
      2. CBSA / metro-area match (same metropolitan statistical area)
      3. State-wide fallback (only if no CBSA results)

    Within each tier, surgeons are ranked by Informed Score (highest first).
    Returns {"results": [...], "procedure_matched": str, "city_matched": str,
             "search_scope": str}.
    """
    if not os.path.exists(SCORES_CSV_PATH):
        return {"results": [], "procedure_matched": "", "city_matched": "",
                "error": f"CSV not found at {SCORES_CSV_PATH}"}

    city_lower = city.lower().strip()
    state_upper = state.upper().strip()
    proc_lower = procedure.lower().strip()

    # ------------------------------------------------------------------
    # Read all rows and find matching procedure name (fuzzy)
    # ------------------------------------------------------------------
    all_rows = []
    procedure_names = set()
    with open(SCORES_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_rows.append(row)
            p = row.get("Procedure", "").strip()
            if p:
                procedure_names.add(p)

    # Match procedure name (best fuzzy match)
    matched_proc = None
    for p in sorted(procedure_names):
        if p.lower() == proc_lower:
            matched_proc = p
            break
    if not matched_proc:
        for p in sorted(procedure_names):
            if proc_lower in p.lower() or p.lower() in proc_lower:
                matched_proc = p
                break
    if not matched_proc:
        proc_words = proc_lower.split()
        best_p, best_score = None, 0
        for p in procedure_names:
            p_lower = p.lower()
            score = sum(1 for w in proc_words if len(w) > 2 and w in p_lower)
            if score > best_score:
                best_score = score
                best_p = p
        if best_score > 0:
            matched_proc = best_p

    if not matched_proc:
        return {
            "results": [],
            "procedure_matched": "",
            "city_matched": city,
            "available_procedures": sorted(procedure_names),
            "error": f"No matching procedure found for '{procedure}'",
        }

    # ------------------------------------------------------------------
    # Filter rows by procedure
    # ------------------------------------------------------------------
    proc_rows = [r for r in all_rows if r["Procedure"].strip() == matched_proc]

    # ------------------------------------------------------------------
    # Tier 1: Exact city match
    # ------------------------------------------------------------------
    exact_city_rows = []
    for row in proc_rows:
        row_city = row.get("City", "").strip().lower()
        row_state = row.get("State", "").strip().upper()
        if row_city == city_lower:
            if not state_upper or row_state == state_upper:
                exact_city_rows.append(row)

    # ------------------------------------------------------------------
    # Tier 2: CBSA / metro-area match
    # Identify the target CBSA by finding a ZIP in the CSV for the target
    # city, then pull all surgeons whose facility ZIP is in the same CBSA.
    # ------------------------------------------------------------------
    cbsa_rows = []
    target_cbsa_code = ""
    target_cbsa_title = ""
    search_scope = "exact_city"

    if _CBSA_ZIPS:
        # Strategy A: Find CBSA code from a CSV row in the target city
        for row in exact_city_rows:
            row_zip = row.get("Zip", "").strip()[:5]
            target_cbsa_code = _get_cbsa_code(row_zip)
            if target_cbsa_code:
                target_cbsa_title = _get_cbsa_title(row_zip)
                break

        # Strategy B: If the city isn't in the CSV (or has no CBSA),
        # geocode the city to find its county, then look up its CBSA.
        # This handles suburbs and cities not represented in the data.
        if not target_cbsa_code and state_upper:
            target_cbsa_code, target_cbsa_title = _geocode_city_to_cbsa(city, state_upper)

        # If we found a CBSA, gather all surgeons in that metro area
        if target_cbsa_code:
            for row in proc_rows:
                row_zip = row.get("Zip", "").strip()[:5]
                if _get_cbsa_code(row_zip) == target_cbsa_code:
                    cbsa_rows.append(row)

    # ------------------------------------------------------------------
    # Decide which pool to rank from
    # Priority: exact city first; if not enough, use full CBSA pool;
    # if still empty, fall back to state-wide.
    # ------------------------------------------------------------------
    if exact_city_rows:
        candidate_rows = exact_city_rows
        matched_city = city
        search_scope = "exact_city"
        # If we also have broader CBSA rows, note that for the response
        cbsa_extra_count = len(set(r["NPI"] for r in cbsa_rows)) - len(set(r["NPI"] for r in exact_city_rows)) if cbsa_rows else 0
    elif cbsa_rows:
        candidate_rows = cbsa_rows
        matched_city = f"{city} metro area ({target_cbsa_title})"
        search_scope = "cbsa"
        cbsa_extra_count = 0
    else:
        # Tier 3: State-wide fallback
        candidate_rows = []
        if state_upper:
            for row in proc_rows:
                row_state = row.get("State", "").strip().upper()
                if row_state == state_upper:
                    candidate_rows.append(row)
        if candidate_rows:
            matched_city = f"{state_upper} (state-wide, no results in {city} metro area)"
            search_scope = "state"
            cbsa_extra_count = 0
        else:
            available_cities = sorted(set(
                f"{r['City'].strip().title()}, {r['State'].strip()}"
                for r in proc_rows if r["City"].strip()
            ))
            return {
                "results": [],
                "procedure_matched": matched_proc,
                "city_matched": "",
                "search_scope": "none",
                "nearby_cities": available_cities[:20],
                "error": f"No surgeons found for '{matched_proc}' in {city}"
                         + (f", {state_upper}" if state_upper else ""),
            }

    # ------------------------------------------------------------------
    # Deduplicate by NPI — keep the row with the highest Informed Score
    # ------------------------------------------------------------------
    by_npi = {}
    for row in candidate_rows:
        npi = row["NPI"]
        try:
            score = int(row.get("Informed Score", "0"))
        except ValueError:
            score = 0
        if npi not in by_npi or score > by_npi[npi]["_score"]:
            by_npi[npi] = {**row, "_score": score}

    # Sort by Informed Score descending
    ranked = sorted(by_npi.values(), key=lambda r: r["_score"], reverse=True)

    # ------------------------------------------------------------------
    # Build result list
    # ------------------------------------------------------------------
    results = []
    for row in ranked[:top_n]:
        npi = row["NPI"]
        facilities = sorted(set(
            r["Facility Name"].strip().title()
            for r in candidate_rows if r["NPI"] == npi and r["Facility Name"].strip()
        ))
        try:
            comp_free = f"{float(row['prop_complication_free_surgeon']):.1%}"
        except (ValueError, KeyError):
            comp_free = ""
        try:
            cost = f"${float(row['Raw 90 Day Cost']):,.0f}"
        except (ValueError, KeyError):
            cost = ""
        try:
            los = f"{float(row['los_elective']):.2f} days"
        except (ValueError, KeyError):
            los = ""

        row_city = row.get("City", "").strip().title()
        row_state = row.get("State", "").strip()
        in_target_city = (row_city.lower() == city_lower)

        results.append({
            "npi": npi,
            "name": f"{row.get('First Name', '').strip().title()} "
                    f"{row.get('Middle', '').strip().title()} "
                    f"{row.get('Last Name', '').strip().title()}".replace("  ", " ").strip(),
            "credential": row.get("Credential", "").strip(),
            "specialty": row.get("Specialty", "").strip().title(),
            "informed_score": row["_score"],
            "cases": row.get("Cases", ""),
            "state_rank": row.get("State Rank", ""),
            "complication_free_rate": comp_free,
            "avg_90_day_cost": cost,
            "length_of_stay": los,
            "facilities": facilities,
            "city": row_city,
            "state": row_state,
            "medical_school": row.get("Medical School", "").strip().title(),
            "in_target_city": in_target_city,
        })

    response = {
        "results": results,
        "procedure_matched": matched_proc,
        "city_matched": matched_city,
        "search_scope": search_scope,
    }
    if target_cbsa_code:
        response["cbsa_code"] = target_cbsa_code
        response["cbsa_title"] = target_cbsa_title
    if search_scope == "exact_city" and cbsa_extra_count > 0:
        response["cbsa_additional_surgeons"] = cbsa_extra_count
        response["cbsa_title"] = target_cbsa_title

    return response


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def save_cache(npi, data):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{npi}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return path


def load_cache(npi):
    path = os.path.join(CACHE_DIR, f"{npi}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None
