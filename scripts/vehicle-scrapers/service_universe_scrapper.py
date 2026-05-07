#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
service_universe_scrapper.py
------------------------------------------------

Production-grade Cardekho service scraper
for CDrive / ACI Assist

Fixes:
✅ proper service cost extraction
✅ proper fuel parsing
✅ proper service schedule parsing
✅ proper service center extraction
✅ Next.js JSON support
✅ DOM targeted scraping
✅ noisy page-body elimination

Outputs:
- service_cost_universe.json
- service_schedule_universe.json
- service_center_universe.json
- ownership_cost_universe.json

------------------------------------------------

RUN:

python3 service_universe_scrapper.py --debug

python3 service_universe_scrapper.py \
  --brand Hyundai \
  --limit-models 5 \
  --debug

------------------------------------------------
"""

import argparse
import csv
import json
import random
import re
import time

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from ncr_universe_utils_v2 import (
    build_ncr_variant_universe,
    normalize_key,
    normalize_spaces,
)

# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://www.cardekho.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}

OUTPUT_SERVICE_COST = "service_cost_universe.json"
OUTPUT_SERVICE_SCHEDULE = "service_schedule_universe.json"
OUTPUT_SERVICE_CENTER = "service_center_universe.json"
OUTPUT_OWNERSHIP = "ownership_cost_universe.json"

# =========================================================
# DATACLASSES
# =========================================================

@dataclass
class ServiceCostRow:
    brand: str
    model: str
    fuel_type: str

    five_year_cost: int
    avg_yearly_cost: int

    source_url: str
    confidence: str

@dataclass
class ServiceScheduleRow:
    brand: str
    model: str
    fuel_type: str

    service_no: int
    km: int
    months: int

    service_type: str
    cost: int

    source: str

@dataclass
class ServiceCenterRow:
    brand: str
    city: str

    name: str
    address: str
    phone: str

    source_url: str
    confidence: str

@dataclass
class OwnershipCostRow:
    brand: str
    model: str
    fuel_type: str

    five_year_service_cost: int
    avg_yearly_service_cost: int
    estimated_monthly_maintenance: int

    source_url: str
    confidence: str

# =========================================================
# ARGS
# =========================================================

def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--brand", type=str, default="")
    parser.add_argument("--brands", type=str, default="")
    parser.add_argument("--model", type=str, default="")

    parser.add_argument("--limit-models", type=int, default=0)

    parser.add_argument("--debug", action="store_true")

    parser.add_argument(
        "--debug-dir",
        type=str,
        default="service_debug"
    )

    return parser.parse_args()

# =========================================================
# HELPERS
# =========================================================

def slugify(text):

    text = normalize_spaces(text or "").lower()

    text = text.replace("&", "and")

    text = re.sub(r"[^a-z0-9\s\-]", "", text)

    text = re.sub(r"\s+", "-", text)

    text = re.sub(r"-+", "-", text)

    return text.strip("-")

def money_to_int(text):

    if not text:
        return 0

    text = str(text)

    text = text.replace(",", "")

    lakh_match = re.search(
        r"([\d\.]+)\s*lakh",
        text,
        re.I,
    )

    if lakh_match:

        try:
            return int(float(lakh_match.group(1)) * 100000)
        except:
            pass

    num_match = re.search(
        r"(\d+)",
        text
    )

    if not num_match:
        return 0

    try:
        return int(num_match.group(1))
    except:
        return 0

def fetch_text(session, url):

    for attempt in range(3):

        try:

            resp = session.get(
                url,
                timeout=(10, 35),
                allow_redirects=True,
            )

            if resp.status_code == 200:
                return True, resp.text

        except Exception:
            pass

        time.sleep(
            (attempt + 1)
            + random.uniform(0.1, 0.4)
        )

    return False, ""

def extract_next_data_json(raw_html):

    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        raw_html,
        re.S,
    )

    if not m:
        return {}

    try:
        return json.loads(m.group(1))
    except:
        return {}

def recursive_find_keys(obj, target_keys, results=None):

    if results is None:
        results = []

    if isinstance(obj, dict):

        for k, v in obj.items():

            if str(k).lower() in target_keys:
                results.append(v)

            recursive_find_keys(
                v,
                target_keys,
                results,
            )

    elif isinstance(obj, list):

        for item in obj:

            recursive_find_keys(
                item,
                target_keys,
                results,
            )

    return results

# =========================================================
# SESSION
# =========================================================

def build_session():

    s = requests.Session()

    s.headers.update(HEADERS)

    return s

# =========================================================
# URLS
# =========================================================

def service_cost_url(
    brand_slug,
    model_slug,
):

    return (
        f"{BASE_URL}/"
        f"{brand_slug}/"
        f"{model_slug}/service-cost"
    )

def service_center_url(brand_slug):

    return (
        f"{BASE_URL}/"
        f"{brand_slug}/car-service-center.htm"
    )

# =========================================================
# SERVICE COST
# =========================================================

def scrape_service_cost_page(
    session,
    model_entry,
    debug_dir=None,
):

    brand = model_entry["brand_display"]
    model = model_entry["model_display"]

    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]

    fuel_type = (
        model_entry.get("fuel")
        or "Petrol"
    )

    url = service_cost_url(
        brand_slug,
        model_slug,
    )

    ok, raw_html = fetch_text(
        session,
        url,
    )

    if not ok:
        return [], [], [], "fetch_failed"

    if debug_dir:

        (
            debug_dir /
            f"{brand_slug}_{model_slug}.html"
        ).write_text(
            raw_html,
            encoding="utf-8",
        )

    soup = BeautifulSoup(raw_html, "html.parser")

    text = soup.get_text("\n", strip=True)

    text = normalize_spaces(text)

    next_json = extract_next_data_json(raw_html)

    # =====================================================
    # SERVICE COST
    # =====================================================

    five_year_cost = 0

    service_patterns = [

        r"service cost.*?₹\s*([\d,]+)",

        r"maintenance cost.*?₹\s*([\d,]+)",

        r"paid service.*?₹\s*([\d,]+)",

        r"total service cost.*?₹\s*([\d,]+)",

        r"cost for 5 years.*?₹\s*([\d,]+)",

        r"₹\s*([\d,]+)\s*for 5 years",
    ]

    for pat in service_patterns:

        m = re.search(
            pat,
            text,
            re.I | re.S,
        )

        if m:

            val = money_to_int(
                m.group(1)
            )

            if val > 5000:

                five_year_cost = val
                break

    # =====================================================
    # NEXT JSON FALLBACK
    # =====================================================

    if not five_year_cost:

        possible_keys = recursive_find_keys(
            next_json,
            {
                "servicecost",
                "maintenancecost",
                "service_cost",
                "ownershipcost",
            }
        )

        for item in possible_keys:

            val = money_to_int(
                str(item)
            )

            if val > 5000:
                five_year_cost = val
                break

    avg_yearly = int(
        round(five_year_cost / 5)
    ) if five_year_cost else 0

    monthly = int(
        round(avg_yearly / 12)
    ) if avg_yearly else 0

    # =====================================================
    # SERVICE TABLE
    # =====================================================

    schedule_rows = []

    tables = soup.find_all("table")

    service_no = 1

    for table in tables:

        rows = table.find_all("tr")

        if len(rows) < 2:
            continue

        headers = [
            normalize_spaces(
                x.get_text(" ", strip=True)
            ).lower()
            for x in rows[0].find_all(["th", "td"])
        ]

        header_blob = " ".join(headers)

        if not (
            "service" in header_blob
            or "cost" in header_blob
        ):
            continue

        for row in rows[1:]:

            cols = [
                normalize_spaces(
                    x.get_text(" ", strip=True)
                )
                for x in row.find_all(["th", "td"])
            ]

            if len(cols) < 2:
                continue

            row_blob = " ".join(cols)

            km = 0
            months = 0
            cost = 0

            km_match = re.search(
                r"(\d[\d,]*)\s*km",
                row_blob,
                re.I,
            )

            if km_match:
                km = money_to_int(
                    km_match.group(1)
                )

            month_match = re.search(
                r"(\d+)\s*month",
                row_blob,
                re.I,
            )

            if month_match:
                months = int(
                    month_match.group(1)
                )

            all_amounts = re.findall(
                r"(?:₹|Rs\.?)\s*[\d,]+",
                row_blob,
                re.I,
            )

            if all_amounts:

                parsed = [
                    money_to_int(x)
                    for x in all_amounts
                ]

                parsed = [
                    x for x in parsed
                    if x > 100
                ]

                if parsed:
                    cost = max(parsed)

            service_type = "Paid"

            if "free" in row_blob.lower():
                service_type = "Free"

            schedule_rows.append(

                ServiceScheduleRow(
                    brand=brand,
                    model=model,
                    fuel_type=fuel_type,

                    service_no=service_no,

                    km=km,
                    months=months,

                    service_type=service_type,
                    cost=cost,

                    source="cardekho",
                )
            )

            service_no += 1

    # =====================================================
    # FINAL ROWS
    # =====================================================

    cost_rows = [

        ServiceCostRow(
            brand=brand,
            model=model,
            fuel_type=fuel_type,

            five_year_cost=five_year_cost,
            avg_yearly_cost=avg_yearly,

            source_url=url,

            confidence=(
                "high"
                if five_year_cost
                else "low"
            ),
        )
    ]

    ownership_rows = [

        OwnershipCostRow(
            brand=brand,
            model=model,
            fuel_type=fuel_type,

            five_year_service_cost=five_year_cost,
            avg_yearly_service_cost=avg_yearly,
            estimated_monthly_maintenance=monthly,

            source_url=url,

            confidence=(
                "high"
                if five_year_cost
                else "low"
            ),
        )
    ]

    return (
        cost_rows,
        schedule_rows,
        ownership_rows,
        "success",
    )

# =========================================================
# SERVICE CENTERS
# =========================================================

def scrape_service_centers(
    session,
    brand_entry,
    debug_dir=None,
):

    brand = brand_entry["brand_display"]

    brand_slug = brand_entry["brand_slug"]

    url = service_center_url(
        brand_slug
    )

    ok, raw_html = fetch_text(
        session,
        url,
    )

    if not ok:
        return [], "fetch_failed"

    if debug_dir:

        (
            debug_dir /
            f"{brand_slug}_centers.html"
        ).write_text(
            raw_html,
            encoding="utf-8",
        )

    soup = BeautifulSoup(raw_html, "html.parser")

    rows = []

    seen = set()

    candidate_blocks = soup.find_all(
        [
            "li",
            "div",
            "section",
        ]
    )

    for block in candidate_blocks:

        txt = normalize_spaces(
            block.get_text(" ", strip=True)
        )

        if len(txt) < 40:
            continue

        if (
            "service center"
            not in txt.lower()
            and "service centre"
            not in txt.lower()
        ):
            continue

        if len(txt) > 600:
            continue

        phone_match = re.search(
            r"(\+91[\-\s]?)?[6-9]\d{9}",
            txt,
        )

        city_match = re.search(
            r"\b(delhi|gurgaon|noida|mumbai|pune|bangalore|hyderabad|chennai|kolkata|jaipur|lucknow)\b",
            txt,
            re.I,
        )

        lines = [
            normalize_spaces(x)
            for x in txt.split("\n")
            if normalize_spaces(x)
        ]

        name = lines[0] if lines else ""

        if len(name) > 120:
            continue

        if name.lower() in {
            "service center",
            "service centres",
        }:
            continue

        dedupe_key = normalize_key(name)

        if dedupe_key in seen:
            continue

        seen.add(dedupe_key)

        rows.append(

            ServiceCenterRow(
                brand=brand,

                city=(
                    city_match.group(1).title()
                    if city_match
                    else ""
                ),

                name=name,

                address=txt[:300],

                phone=(
                    phone_match.group(0)
                    if phone_match
                    else ""
                ),

                source_url=url,

                confidence="medium",
            )
        )

    return rows, "success"

# =========================================================
# CSV
# =========================================================

def write_csv(path, rows):

    if not rows:
        return

    keys = list(rows[0].keys())

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=keys,
        )

        writer.writeheader()

        writer.writerows(rows)

# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_args()

    debug_dir = None

    if args.debug:

        debug_dir = Path(
            args.debug_dir
        )

        debug_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    print(
        "\n[1/3] Building canonical NCR universe...\n"
    )

    raw_universe = build_ncr_variant_universe()

    normalized_universe = []

    for row in raw_universe:

        if isinstance(row, tuple):

            brand = row[0]
            model = row[1]

            normalized_universe.append({

                "brand_display": str(brand),
                "model_display": str(model),

                "brand_slug": slugify(brand),
                "model_slug": slugify(model),

                "fuel": (
                    row[3]
                    if len(row) > 3
                    else "Petrol"
                ),
            })

        elif isinstance(row, dict):

            brand = (
                row.get("brand_display")
                or row.get("brand")
                or row.get("make")
                or ""
            )

            model = (
                row.get("model_display")
                or row.get("model")
                or ""
            )

            normalized_universe.append({

                "brand_display": str(brand),
                "model_display": str(model),

                "brand_slug": (
                    row.get("brand_slug")
                    or slugify(brand)
                ),

                "model_slug": (
                    row.get("model_slug")
                    or slugify(model)
                ),

                "fuel": (
                    row.get("fuel")
                    or "Petrol"
                ),
            })

    # =====================================================
    # FILTERS
    # =====================================================

    filtered = []

    for row in normalized_universe:

        if args.brand:

            if (
                row["brand_display"].lower()
                != args.brand.lower()
            ):
                continue

        if args.model:

            if (
                normalize_key(
                    row["model_display"]
                )
                != normalize_key(args.model)
            ):
                continue

        filtered.append(row)

    # =====================================================
    # DEDUPE
    # =====================================================

    unique_models = {}

    for row in filtered:

        key = (
            normalize_key(
                row["brand_display"]
            ),
            normalize_key(
                row["model_display"]
            ),
        )

        if key not in unique_models:
            unique_models[key] = row

    models = list(
        unique_models.values()
    )

    if args.limit_models > 0:
        models = models[:args.limit_models]

    print(
        f"Models in scope: {len(models)}\n"
    )

    # =====================================================
    # SESSION
    # =====================================================

    session = build_session()

    # =====================================================
    # SCRAPE SERVICE COSTS
    # =====================================================

    print(
        "[2/3] Scraping Cardekho service costs...\n"
    )

    all_cost_rows = []
    all_schedule_rows = []
    all_ownership_rows = []

    for model_entry in tqdm(models):

        try:

            (
                cost_rows,
                schedule_rows,
                ownership_rows,
                reason,
            ) = scrape_service_cost_page(
                session,
                model_entry,
                debug_dir,
            )

            all_cost_rows.extend(cost_rows)

            all_schedule_rows.extend(
                schedule_rows
            )

            all_ownership_rows.extend(
                ownership_rows
            )

        except Exception as exc:

            print(
                f"ERROR "
                f"{model_entry['brand_display']} "
                f"{model_entry['model_display']} "
                f"{exc}"
            )

        time.sleep(
            random.uniform(0.2, 0.5)
        )

    # =====================================================
    # SERVICE CENTERS
    # =====================================================

    print(
        "\n[3/3] Scraping service centers...\n"
    )

    unique_brands = {}

    for row in models:

        k = normalize_key(
            row["brand_display"]
        )

        if k not in unique_brands:
            unique_brands[k] = row

    brand_rows = list(
        unique_brands.values()
    )

    all_service_centers = []

    for brand_entry in tqdm(brand_rows):

        try:

            rows, reason = scrape_service_centers(
                session,
                brand_entry,
                debug_dir,
            )

            all_service_centers.extend(rows)

        except Exception as exc:

            print(
                f"ERROR centers "
                f"{brand_entry['brand_display']} "
                f"{exc}"
            )

        time.sleep(
            random.uniform(0.2, 0.6)
        )

    # =====================================================
    # EXPORT
    # =====================================================

    cost_json = [
        asdict(x)
        for x in all_cost_rows
    ]

    schedule_json = [
        asdict(x)
        for x in all_schedule_rows
    ]

    center_json = [
        asdict(x)
        for x in all_service_centers
    ]

    ownership_json = [
        asdict(x)
        for x in all_ownership_rows
    ]

    Path(
        OUTPUT_SERVICE_COST
    ).write_text(
        json.dumps(
            cost_json,
            indent=2,
            ensure_ascii=False,
        )
    )

    Path(
        OUTPUT_SERVICE_SCHEDULE
    ).write_text(
        json.dumps(
            schedule_json,
            indent=2,
            ensure_ascii=False,
        )
    )

    Path(
        OUTPUT_SERVICE_CENTER
    ).write_text(
        json.dumps(
            center_json,
            indent=2,
            ensure_ascii=False,
        )
    )

    Path(
        OUTPUT_OWNERSHIP
    ).write_text(
        json.dumps(
            ownership_json,
            indent=2,
            ensure_ascii=False,
        )
    )

    write_csv(
        OUTPUT_SERVICE_COST.replace(
            ".json",
            ".csv",
        ),
        cost_json,
    )

    write_csv(
        OUTPUT_SERVICE_SCHEDULE.replace(
            ".json",
            ".csv",
        ),
        schedule_json,
    )

    write_csv(
        OUTPUT_SERVICE_CENTER.replace(
            ".json",
            ".csv",
        ),
        center_json,
    )

    write_csv(
        OUTPUT_OWNERSHIP.replace(
            ".json",
            ".csv",
        ),
        ownership_json,
    )

    print("\n====================================")
    print("SCRAPE COMPLETE")
    print("====================================\n")

    print(
        f"Service cost rows: "
        f"{len(all_cost_rows)}"
    )

    print(
        f"Service schedule rows: "
        f"{len(all_schedule_rows)}"
    )

    print(
        f"Service center rows: "
        f"{len(all_service_centers)}"
    )

    print(
        f"Ownership rows: "
        f"{len(all_ownership_rows)}"
    )

    print("\nDone.\n")

# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":
    main()