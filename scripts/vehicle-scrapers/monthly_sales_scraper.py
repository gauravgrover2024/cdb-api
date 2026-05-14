# monthly_sales_scraper.py
# ============================================================
# CDrive / ACI Assist
# Monthly Popular Cars Scraper (V1)
#
# PURPOSE:
# Scrape model-wise monthly car sales rankings from:
# - Autopunditz
#
# OUTPUT:
# monthly_car_sales.json
#
# QUICK START:
# pip install requests beautifulsoup4 pandas lxml
#
# RUN:
# python3 monthly_sales_scraper.py
# ============================================================

import re
import json
import time
import requests
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime


try:
    PARSER = "lxml"
except:
    PARSER = "html.parser"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

OUTPUT_JSON = "monthly_car_sales.json"
OUTPUT_CSV = "monthly_car_sales.csv"

# ============================================================
# TARGET URLS
# ============================================================

TARGETS = [
    {
        "source": "autopunditz",
        "url": "https://www.autopunditz.com/car-sales-figures/"
    }
]

# ============================================================
# HELPERS
# ============================================================

def clean_number(value):
    if value is None:
        return None

    value = str(value)
    value = value.replace(",", "").strip()

    match = re.search(r"(\d+)", value)

    if not match:
        return None

    return int(match.group(1))


def normalize_model_name(name):
    if not name:
        return ""

    name = re.sub(r"\s+", " ", name).strip()

    replacements = {
        "Maruti Suzuki": "",
        "Hyundai": "",
        "Tata": "",
        "Mahindra": "",
        "Toyota": "",
        "Kia": "",
        "Honda": "",
        "Skoda": "",
        "Volkswagen": "",
    }

    for k, v in replacements.items():
        name = name.replace(k, v)

    return name.strip()


def infer_segment(model):
    model_lower = model.lower()

    suv_keywords = [
        "creta", "brezza", "venue", "scorpio", "xuv",
        "harrier", "safari", "thar", "seltos",
        "grand vitara", "taigun", "kushaq"
    ]

    sedan_keywords = [
        "city", "verna", "slavia", "virtus",
        "dzire", "amaze"
    ]

    hatch_keywords = [
        "swift", "baleno", "i20", "wagonr",
        "alto", "tiago"
    ]

    for k in suv_keywords:
        if k in model_lower:
            return "SUV"

    for k in sedan_keywords:
        if k in model_lower:
            return "Sedan"

    for k in hatch_keywords:
        if k in model_lower:
            return "Hatchback"

    return "Other"


# ============================================================
# SCRAPER
# ============================================================
def scrape_autopunditz():

    url = "https://www.autopunditz.com/car-sales-figures/"

    print(f"\nFetching: {url}")

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=30
    )

    response.raise_for_status()

    # DIRECTLY READ ALL HTML TABLES
    tables = pd.read_html(response.text)
    print(tables[0].head())

    print(f"Found {len(tables)} tables")

    all_rows = []

    for table_index, df in enumerate(tables):

        try:

            columns = [str(c).lower() for c in df.columns]

            # Skip junk tables
            if not any(
                "model" in c
                or "car" in c
                or "vehicle" in c
                for c in columns
            ):
                continue

            print(f"Processing table #{table_index + 1}")

            print(df.head())

            for _, row in df.iterrows():

                values = [str(v).strip() for v in row.values]

                if len(values) < 2:
                    continue

                model = values[0]

                if (
                    not model
                    or model.lower() in ["nan", "total", "model"]
                ):
                    continue

                model = normalize_model_name(model)

                sales_value = None

                # find first numeric value in row
                for v in values[1:]:

                    num = clean_number(v)

                    if num and num > 100:
                        sales_value = num
                        break

                if not sales_value:
                    continue

                item = {
                    "model": model,
                    "segment": infer_segment(model),
                    "retailSales": sales_value,
                    "source": "autopunditz",
                    "scrapedAt": datetime.utcnow().isoformat()
                }

                all_rows.append(item)

        except Exception as e:

            print(f"Skipping table #{table_index + 1}: {e}")

    return all_rows

# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("CDRIVE MONTHLY SALES SCRAPER")
    print("=" * 60)

    final_rows = []

    try:
        rows = scrape_autopunditz()
        final_rows.extend(rows)

    except Exception as e:
        print(f"ERROR scraping Autopunditz: {e}")

    # ========================================================
    # DEDUPLICATION
    # ========================================================

    deduped = {}

    for row in final_rows:

        key = row["model"].lower()

        existing = deduped.get(key)

        if not existing:
            deduped[key] = row
            continue

        # Keep highest sales value
        if row["retailSales"] > existing["retailSales"]:
            deduped[key] = row

    final_data = list(deduped.values())

    # ========================================================
    # SORT
    # ========================================================

    final_data.sort(
        key=lambda x: x["retailSales"],
        reverse=True
    )

    # ========================================================
    # ADD RANKS
    # ========================================================

    for idx, row in enumerate(final_data, start=1):
        row["rank"] = idx

    # ========================================================
    # SAVE JSON
    # ========================================================

    with open(OUTPUT_JSON, "w") as f:
        json.dump(final_data, f, indent=2)

    # ========================================================
    # SAVE CSV
    # ========================================================

    pd.DataFrame(final_data).to_csv(
        OUTPUT_CSV,
        index=False
    )

    # ========================================================
    # PRINT SUMMARY
    # ========================================================

    print("\n")
    print("=" * 60)
    print("TOP 20 CARS")
    print("=" * 60)

    for row in final_data[:20]:

        print(
            f"#{row['rank']:02d} | "
            f"{row['model']:<25} | "
            f"{row['segment']:<10} | "
            f"{row['retailSales']}"
        )

    print("\n")
    print("=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)

    print(f"Total models: {len(final_data)}")
    print(f"JSON saved: {OUTPUT_JSON}")
    print(f"CSV saved : {OUTPUT_CSV}")


if __name__ == "__main__":
    main()