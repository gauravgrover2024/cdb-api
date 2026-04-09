#!/usr/bin/env python3
"""
CarWale Variant Price Scraper
==============================
Scrapes ex-showroom prices for all variants from CarWale API.

Usage:
  python carwale_scraper.py               # Scrape all + save CSV
  python carwale_scraper.py --match       # Scrape + match to Excel
  python carwale_scraper.py --match-only  # Skip scrape, match existing CSV
"""

import requests
import time
import csv
import os
import sys
from difflib import SequenceMatcher

# ============================================================
# CONFIGURATION — Edit these as needed
# ============================================================
CITY_ID        = 10         # 10=Delhi | 20=Mumbai | 1=Bangalore | 3=Chennai
AREA_ID        = -1
PLATFORM_ID    = 43
DELAY          = 2.0        # seconds between API calls
OUTPUT_CSV     = "carwale_prices.csv"
EXCEL_INPUT    = "variants.xlsx"        # Your Excel file (for --match mode)
MATCHED_OUT    = "matched_prices.xlsx"  # Output after matching

# Set True to scrape only TARGET_MODELS below instead of full map
SCRAPE_SUBSET  = False

TARGET_MODELS  = {
    "maruti-suzuki": ["swift", "swift-2018-2021", "baleno"],
    "hyundai":       ["creta", "i20"],
}

# Column names in YOUR Excel file
EXCEL_COL_MAKE    = "Make"
EXCEL_COL_MODEL   = "Model"
EXCEL_COL_VARIANT = "Variant"

# Minimum fuzzy score (0.0 to 1.0) to accept a variant match
MATCH_THRESHOLD = 0.5

# API base
BASE_URL = "https://www.carwale.com/api/modelpagedata/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.carwale.com/",
    "Origin":          "https://www.carwale.com",
}
# ============================================================
# COMPLETE MAKE → MODEL MASKING NAME MAP
# ============================================================
MODELS_MAP = {
    "maruti-suzuki": [
        "alto-800", "alto-k10", "s-presso",
        "celerio", "celerio-2014-2021",
        "wagon-r", "wagon-r-2010-2019",
        "swift", "swift-2018-2021", "swift-2014-2018", "swift-2005-2011",
        "dzire", "dzire-2017-2020", "dzire-2012-2017",
        "baleno", "baleno-2015-2022",
        "ignis", "xl6", "xl6-2019-2022",
        "ertiga", "ertiga-2018-2022", "ertiga-2012-2018",
        "brezza", "vitara-brezza",
        "s-cross", "grand-vitara", "jimny", "fronx", "invicto",
        "eeco", "ciaz", "ciaz-2014-2017",
        "omni", "ritz", "a-star",
    ],
    "hyundai": [
        "santro", "santro-2018-2022",
        "i10", "grand-i10", "grand-i10-nios",
        "i20", "i20-n-line", "i20-active", "elite-i20",
        "aura", "verna", "verna-2017-2020", "verna-2015-2017",
        "exter", "venue", "venue-n-line",
        "creta", "creta-2015-2020",
        "alcazar", "tucson",
        "ioniq-5", "ioniq-6", "kona-electric",
        "xcent", "xcent-prime", "elantra",
    ],
    "tata": [
        "tiago", "tigor", "altroz", "punch", "nexon",
        "harrier", "safari", "hexa",
        "tiago-ev", "tigor-ev", "punch-ev", "nexon-ev",
        "harrier-ev", "curvv", "curvv-ev", "bolt",
        "zest", "manza", "nano", "sumo-gold",
    ],
    "mahindra": [
        "bolero", "bolero-neo", "bolero-neo-plus",
        "xuv300", "xuv400", "xuv700", "xuv-3xo",
        "scorpio-n", "scorpio-classic", "scorpio",
        "thar", "thar-roxx", "be-6", "xe-9",
        "marazzo", "alturas-g4", "kuv100", "xylo",
    ],
    "toyota": [
        "glanza", "rumion", "hyryder", "hycross",
        "innova-crysta", "innova-hycross",
        "fortuner", "hilux", "camry", "vellfire",
        "land-cruiser-300", "bz4x",
        "yaris", "etios", "etios-cross", "etios-liva",
        "corolla-altis",
    ],
    "kia": [
        "sonet", "seltos", "carens",
        "ev6", "ev9", "carnival", "syros",
    ],
    "honda": [
        "amaze", "city", "city-2014-2017", "city-2008-2013",
        "elevate", "wr-v", "jazz", "cr-v", "accord",
        "brio", "mobilio",
    ],
    "volkswagen": [
        "polo", "vento", "taigun", "virtus", "tiguan", "id-4",
    ],
    "skoda": [
        "slavia", "kushaq", "superb", "kodiaq",
        "kylaq", "octavia", "rapid",
    ],
    "mg": [
        "hector", "hector-plus", "gloster",
        "zs-ev", "astor", "comet-ev", "windsor-ev", "cloud-ev",
    ],
    "renault": [
        "kwid", "triber", "kiger", "duster",
    ],
    "nissan": [
        "magnite", "kicks", "terrano",
    ],
    "ford": [
        "figo", "aspire", "ecosport", "endeavour", "freestyle",
    ],
    "jeep": [
        "compass", "meridian", "wrangler", "grand-cherokee",
    ],
    "bmw": [
        "3-series", "5-series", "7-series",
        "x1", "x3", "x5", "x7",
        "i4", "ix", "2-series", "4-series", "m3", "m5",
    ],
    "mercedes-benz": [
        "a-class", "c-class", "e-class", "s-class",
        "gla", "glb", "glc", "gle", "gls",
        "eqb", "eqs", "eqe", "amg-gt",
    ],
    "audi": [
        "a4", "a6", "a8",
        "q3", "q5", "q7", "q8",
        "e-tron", "rs5",
    ],
    "citroen": [
        "c3", "c3-aircross", "basalt",
    ],
    "volvo": [
        "xc40", "xc60", "xc90", "s90", "ex30", "ex40",
    ],
    "land-rover": [
        "defender", "discovery", "discovery-sport",
        "range-rover-evoque", "range-rover-velar",
        "range-rover-sport", "range-rover",
    ],
    "lexus": [
        "es", "ls", "nx", "rx", "ux", "lx",
    ],
    "porsche": [
        "macan", "cayenne", "panamera", "taycan", "911",
    ],
    "jaguar": [
        "xe", "xf", "xj",
        "f-pace", "e-pace", "i-pace", "f-type",
    ],
    "vinfast": [
        "vf-e34", "vf-5", "vf-6", "vf-7", "vf-8", "vf-9",
    ],
    "byd": [
        "atto-3", "seal", "sealion-6",
    ],
    "isuzu": [
        "d-max", "mu-x",
    ],
    "rolls-royce": [
        "ghost", "cullinan", "phantom",
    ],
    "bentley": [
        "bentayga", "continental-gt", "flying-spur",
    ],
    "lamborghini": [
        "urus",
    ],
    "maserati": [
        "ghibli", "levante", "quattroporte",
    ],
}
# ============================================================
# API FUNCTIONS
# ============================================================

def fetch_model_data(make_masking, model_masking):
    """
    One API call → full model data including ALL variants + prices.
    Works for both active and discontinued models.
    """
    params = {
        "makeMaskingName":  make_masking,
        "modelMaskingName": model_masking,
        "cityId":           CITY_ID,
        "areaId":           AREA_ID,
        "showOfferUpfront": "false",
        "platformId":       PLATFORM_ID,
    }
    try:
        resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("pageValidation", {}).get("isValid"):
                return data
        return None
    except Exception as e:
        print(f"    ERROR: {e}")
        return None


def get_spec(specs_list, item_id):
    """
    Extract a spec value by itemId from specsSummary list.
    Common itemIds:
      12 = Mileage (ARAI kmpl)    14 = Displacement (cc)
      15 = Power (bhp)            17 = Torque (Nm)
      26 = Fuel Type              29 = Transmission
       9 = Seating Capacity       31 = Drivetrain
    """
    for s in specs_list:
        if s.get("itemId") == item_id:
            return s.get("value", "")
    return ""


def extract_variants(data, make_masking, model_masking):
    """
    Parse API response → list of variant dicts with prices.
    Each dict = one row in the output CSV.
    """
    rows = []
    if not data:
        return rows

    md         = data.get("modelDetails", {})
    make_name  = md.get("makeName", "")
    model_name = md.get("modelName", "")
    model_id   = md.get("modelId", "")

    for v in data.get("versions", []):
        price_ov = v.get("priceOverview", {})
        specs    = v.get("specsSummary", [])

        rows.append({
            "makeName":           make_name,
            "makeMaskingName":    make_masking,
            "modelName":          model_name,
            "modelMaskingName":   model_masking,
            "modelId":            model_id,
            "versionId":          v.get("versionId", ""),
            "variantName":        v.get("versionName", ""),
            "variantMaskingName": v.get("versionMaskingName", ""),
            "trimName":           v.get("trimName", ""),
            "exShowRoomPrice":    price_ov.get("exShowRoomPrice", 0),
            "formattedPrice":     price_ov.get("formattedPrice", ""),
            "fuelType":           get_spec(specs, 26),
            "transmission":       get_spec(specs, 29),
            "displacement_cc":    get_spec(specs, 14),
            "mileage_kmpl":       get_spec(specs, 12),
            "power_bhp":          get_spec(specs, 15),
            "torque_nm":          get_spec(specs, 17),
            "seating":            get_spec(specs,  9),
            "launchedOn":         v.get("launchedOn", ""),
            "discontinuedOn":     v.get("discontinuedOn", ""),
            "isDiscontinued":     "Yes" if v.get("discontinuedOn") else "No",
            "isSpecialEdition":   "Yes" if v.get("isSpecialVersion") else "No",
        })
    return rows


def scrape(models_map):
    """
    Main scrape loop.
    Hits API for every make/model combo, collects all variant rows.
    Returns list of dicts.
    """
    all_rows = []
    found    = 0
    skipped  = 0
    total    = sum(len(v) for v in models_map.values())
    done     = 0

    print(f"\n{'='*65}")
    print(f"  CarWale Price Scraper")
    print(f"  City ID  : {CITY_ID}  |  Delay : {DELAY}s per call")
    print(f"  Makes    : {len(models_map)}")
    print(f"  Models   : {total}")
    print(f"{'='*65}")

    for make, model_list in models_map.items():
        print(f"\n── {make.upper()}  ({len(model_list)} models)")

        for model in model_list:
            done += 1
            label = f"{make}/{model}"
            print(f"  [{done:>3}/{total}]  {label:<48}", end="", flush=True)

            data = fetch_model_data(make, model)

            if data:
                rows = extract_variants(data, make, model)
                if rows:
                    all_rows.extend(rows)
                    found += 1
                    print(f"✓  {len(rows)} variants")
                else:
                    skipped += 1
                    print("✗  0 variants (empty response)")
            else:
                skipped += 1
                print("✗  not found / invalid")

            time.sleep(DELAY)

    print(f"  ✅  Models scraped  : {found}")
    print(f"  ⏭   Models skipped  : {skipped}")
    print(f"  📦  Total variants  : {len(all_rows)}")
    print(f"{'='*65}\n")
    return all_rows
# ============================================================
# PART 4 — MATCHING FUNCTIONS
# ============================================================

def similarity(a, b):
    """Fuzzy string match ratio between 0.0 and 1.0."""
    return SequenceMatcher(
        None,
        str(a).lower().strip(),
        str(b).lower().strip()
    ).ratio()


def normalize(text):
    """Clean a string for better matching."""
    text = str(text).lower().strip()
    for char in ["(", ")", "[", "]", "-", "_"]:
        text = text.replace(char, " ")
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def find_best_match(excel_make, excel_model, excel_variant, scraped_rows):
    """
    Find the best matching CarWale variant for a given Excel row.
    Filters by make → model → scores by variant name similarity.
    """
    # Step 1 — filter by make
    make_filtered = [
        r for r in scraped_rows
        if normalize(excel_make) in normalize(r.get("makeName", ""))
        or normalize(r.get("makeName", "")) in normalize(excel_make)
    ]
    if not make_filtered:
        make_filtered = scraped_rows

    # Step 2 — filter by model
    model_filtered = [
        r for r in make_filtered
        if normalize(excel_model) in normalize(r.get("modelName", ""))
        or normalize(r.get("modelName", "")) in normalize(excel_model)
    ]
    if not model_filtered:
        model_filtered = make_filtered

    # Step 3 — score by variant name
    best_row   = None
    best_score = 0.0

    for row in model_filtered:
        score = similarity(
            normalize(excel_variant),
            normalize(row.get("variantName", ""))
        )
        if score > best_score:
            best_score = score
            best_row   = row

    if best_row and best_score >= MATCH_THRESHOLD:
        return best_row, round(best_score, 3)

    return None, 0.0


def match_excel_to_scraped(excel_file, scraped_rows, output_file):
    """
    Match every row in your Excel to a scraped CarWale variant.
    Saves result as a new Excel file with price columns added.
    """
    try:
        import pandas as pd
    except ImportError:
        print("❌  pandas not installed. Run: pip install pandas openpyxl")
        return

    print(f"\n📂  Loading Excel: {excel_file}")
    try:
        df = pd.read_excel(excel_file)
    except FileNotFoundError:
        print(f"❌  File not found: {excel_file}")
        return
    except Exception as e:
        print(f"❌  Could not read Excel: {e}")
        return

    print(f"    {len(df)} rows found")

    # Validate columns
    missing_cols = [
        col for col in [EXCEL_COL_MAKE, EXCEL_COL_MODEL, EXCEL_COL_VARIANT]
        if col not in df.columns
    ]
    if missing_cols:
        print(f"❌  Missing columns: {missing_cols}")
        print(f"    Available: {list(df.columns)}")
        return

    print(f"\n🔍  Matching {len(df)} rows...\n")

    matched   = 0
    unmatched = 0

    results = {
        "CW_Make":          [],
        "CW_Model":         [],
        "CW_Variant":       [],
        "CW_ExShowroom":    [],
        "CW_FormattedPrice":[],
        "CW_FuelType":      [],
        "CW_Transmission":  [],
        "CW_LaunchedOn":    [],
        "CW_DiscontinuedOn":[],
        "CW_MatchScore":    [],
        "CW_MatchStatus":   [],
    }

    for idx, row in df.iterrows():
        excel_make    = str(row.get(EXCEL_COL_MAKE,    ""))
        excel_model   = str(row.get(EXCEL_COL_MODEL,   ""))
        excel_variant = str(row.get(EXCEL_COL_VARIANT, ""))

        best_row, score = find_best_match(
            excel_make, excel_model, excel_variant, scraped_rows
        )

        if best_row:
            matched += 1
            results["CW_Make"].append(best_row.get("makeName", ""))
            results["CW_Model"].append(best_row.get("modelName", ""))
            results["CW_Variant"].append(best_row.get("variantName", ""))
            results["CW_ExShowroom"].append(best_row.get("exShowRoomPrice", ""))
            results["CW_FormattedPrice"].append(best_row.get("formattedPrice", ""))
            results["CW_FuelType"].append(best_row.get("fuelType", ""))
            results["CW_Transmission"].append(best_row.get("transmission", ""))
            results["CW_LaunchedOn"].append(best_row.get("launchedOn", ""))
            results["CW_DiscontinuedOn"].append(best_row.get("discontinuedOn", ""))
            results["CW_MatchScore"].append(score)
            results["CW_MatchStatus"].append("Matched")
        else:
            unmatched += 1
            for key in results:
                results[key].append("" if key != "CW_MatchScore" else 0.0)
            results["CW_MatchStatus"][-1] = "No Match"

        if (idx + 1) % 500 == 0:
            print(f"  Processed {idx + 1}/{len(df)} rows...")

    for col, values in results.items():
        df[col] = values

    try:
        df.to_excel(output_file, index=False)
        print(f"\n{'='*65}")
        print(f"  ✅  Matched    : {matched}")
        print(f"  ❌  Unmatched  : {unmatched}")
        print(f"  💾  Saved to   : {output_file}")
        print(f"{'='*65}\n")
    except Exception as e:
        print(f"❌  Could not save: {e}")


# ============================================================
# PART 5 — MAIN ENTRY POINT
# ============================================================

def main():
    args       = sys.argv[1:]
    models_map = TARGET_MODELS if SCRAPE_SUBSET else MODELS_MAP

    # --match-only: skip scrape, load CSV, match Excel
    if "--match-only" in args:
        print("\n🔁  Mode: Match Only")
        scraped_rows = load_csv(OUTPUT_CSV)
        if not scraped_rows:
            print(f"❌  No data in {OUTPUT_CSV}. Run without --match-only first.")
            sys.exit(1)
        match_excel_to_scraped(EXCEL_INPUT, scraped_rows, MATCHED_OUT)
        return

    # --match: scrape + save CSV + match Excel
    if "--match" in args:
        print("\n🔁  Mode: Scrape + Match")
        scraped_rows = scrape(models_map)
        if not scraped_rows:
            print("❌  Nothing scraped.")
            sys.exit(1)
        save_csv(scraped_rows, OUTPUT_CSV)
        match_excel_to_scraped(EXCEL_INPUT, scraped_rows, MATCHED_OUT)
        return

    # default: scrape + save CSV only
    print("\n🔁  Mode: Scrape Only")
    scraped_rows = scrape(models_map)
    if scraped_rows:
        save_csv(scraped_rows, OUTPUT_CSV)
    else:
        print("❌  Nothing scraped.")
        sys.exit(1)


if __name__ == "__main__":
    main()