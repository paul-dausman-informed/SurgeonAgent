"""
One-time build script: Generate cbsa_lookup.json

Downloads two public datasets and joins them:
  1. Census ZCTA-to-County  (ZIP -> County FIPS)
  2. NBER County-to-CBSA     (County FIPS -> CBSA code + title)

Result JSON has two top-level keys:
  "zips":     { "60134": {"code":"16974","title":"Chicago-..."}, ... }
  "counties": { "tarrant county|tx": {"code":"19124","title":"Dallas-..."}, ... }

Run once:  python build_cbsa_map.py
Output:    cbsa_lookup.json  (shipped with the Docker image)
"""

import csv
import io
import json
import os
import sys

import requests

ZCTA_COUNTY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
    "zcta520/tab20_zcta520_county20_natl.txt"
)
COUNTY_CBSA_URL = (
    "https://data.nber.org/cbsa-csa-fips-county-crosswalk/2023/"
    "cbsa2fipsxw_2023.csv"
)

# State FIPS -> state abbreviation
STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR", "78": "VI",
}

OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cbsa_lookup.json")


def main():
    print("Step 1/4: Downloading ZCTA -> County mapping from Census Bureau ...")
    resp = requests.get(ZCTA_COUNTY_URL, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content):,} bytes")

    # Parse pipe-delimited: GEOID_ZCTA5_20 (col 1) -> GEOID_COUNTY_20 (col 9)
    zip_to_county = {}   # zip5 -> county_fips (5-digit)
    zip_to_area = {}     # zip5 -> best area seen so far

    lines = resp.text.splitlines()
    header = lines[0].split("|")
    print(f"  Columns: {header[:4]} ... ({len(header)} total)")

    for line in lines[1:]:
        parts = line.split("|")
        if len(parts) < 17:
            continue
        zcta = parts[1].strip()
        county_fips = parts[9].strip()     # 5-digit state+county
        try:
            area = int(parts[16].strip())   # AREALAND_PART
        except ValueError:
            area = 0

        if zcta not in zip_to_county or area > zip_to_area.get(zcta, 0):
            zip_to_county[zcta] = county_fips
            zip_to_area[zcta] = area

    print(f"  Mapped {len(zip_to_county):,} ZCTAs to counties")

    # ------------------------------------------------------------------
    print("Step 2/4: Downloading County -> CBSA mapping from NBER ...")
    resp2 = requests.get(COUNTY_CBSA_URL, timeout=120)
    resp2.raise_for_status()
    print(f"  Downloaded {len(resp2.content):,} bytes")

    # Build county_fips -> (cbsa_code, cbsa_title)
    # Also build county_name+state -> CBSA for geocoding fallback
    county_to_cbsa = {}       # "48439" -> {"code": "19124", "title": "Dallas-..."}
    county_name_to_cbsa = {}  # "tarrant county|TX" -> {"code": "19124", ...}

    reader = csv.DictReader(io.StringIO(resp2.text))
    for row in reader:
        cbsa_code = row.get("cbsacode", "").strip()
        cbsa_title = row.get("cbsatitle", "").strip()
        state_fips = row.get("fipsstatecode", "").strip().zfill(2)
        county_fips_3 = row.get("fipscountycode", "").strip().zfill(3)
        county_name = row.get("countycountyequivalent", "").strip()
        state_name = row.get("statename", "").strip()

        if cbsa_code and state_fips and county_fips_3:
            full_fips = state_fips + county_fips_3
            cbsa_entry = {"code": cbsa_code, "title": cbsa_title}
            county_to_cbsa[full_fips] = cbsa_entry

            # Build name-based lookup: "tarrant county|TX"
            state_abbr = STATE_FIPS_TO_ABBR.get(state_fips, "")
            if county_name and state_abbr:
                key = f"{county_name.lower()}|{state_abbr}"
                county_name_to_cbsa[key] = cbsa_entry

    print(f"  Mapped {len(county_to_cbsa):,} county FIPS to CBSAs")
    print(f"  Mapped {len(county_name_to_cbsa):,} county names to CBSAs")

    # ------------------------------------------------------------------
    print("Step 3/4: Joining ZIP -> County -> CBSA ...")
    zip_lookup = {}
    matched = 0
    for zcta, county_fips in zip_to_county.items():
        if county_fips in county_to_cbsa:
            zip_lookup[zcta] = county_to_cbsa[county_fips]
            matched += 1

    print(f"  Result: {matched:,} ZIPs mapped to a CBSA")
    print(f"  ({len(zip_to_county) - matched:,} ZIPs are in non-CBSA rural areas)")

    # ------------------------------------------------------------------
    print("Step 4/4: Writing combined lookup file ...")
    combined = {
        "zips": zip_lookup,
        "counties": county_name_to_cbsa,
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(combined, f, separators=(",", ":"))

    size_mb = os.path.getsize(OUT_PATH) / (1024 * 1024)
    print(f"\nWrote {OUT_PATH}")
    print(f"  File size: {size_mb:.1f} MB")
    print(f"  ZIP entries: {len(zip_lookup):,}")
    print(f"  County entries: {len(county_name_to_cbsa):,}")
    print("Done!")


if __name__ == "__main__":
    main()
