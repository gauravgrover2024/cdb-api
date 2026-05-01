"""
service_cost_scraper.py
------------------------
Scrapes service cost and AMC data from CarDekho and CarWale.

Data captured:
  - Scheduled service costs (1st, 2nd, 3rd service etc.)
  - Total 5-year / 3-year maintenance cost
  - Annual Maintenance Contract (AMC) packages
  - Labour cost, parts cost breakdown
  - Service intervals (km / months)

MongoDB collection: service_costs_collection
Schema per doc:
  {
    brand, model, variant?,
    service_schedule: [
        {service_no, km, months, cost_labour, cost_parts, cost_total}
    ],
    total_3yr_cost, total_5yr_cost,
    amc_packages: [{name, duration_years, km_limit, cost, inclusions}],
    source, last_updated
  }

Run monthly or quarterly via cron.
"""

import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Dict, List, Optional

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import service_costs_collection
from ncr_universe_utils_v2 import build_ncr_variant_universe, normalize_spaces

BASE_CD = "https://www.cardekho.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}
TODAY = date.today().isoformat()


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_text(session: requests.Session, url: str, retries: int = 4) -> Optional[str]:
    for i in range(retries):
        try:
            r = session.get(url, timeout=(10, 30))
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            pass
        time.sleep((2 ** i) + random.uniform(0.1, 0.3))
    return None


def to_int(x) -> Optional[int]:
    if x is None:
        return None
    txt = str(x).replace(",", "").replace("₹", "").strip()
    m = re.search(r"\d+", txt)
    if m:
        try:
            return int(m.group(0))
        except ValueError:
            pass
    return None


def parse_service_schedule_from_json(data: dict) -> List[Dict]:
    """
    Extract service schedule from CarDekho's embedded JSON.
    CarDekho returns an array of service objects with cost breakdown.
    """
    schedule = []

    # Look for service cost data in various locations
    service_data = (
        data.get("maintenanceCost")
        or data.get("serviceCost")
        or data.get("serviceSchedule")
        or data.get("maintenanceData")
        or {}
    )

    service_list = (
        service_data.get("serviceList")
        or service_data.get("scheduleList")
        or service_data.get("services")
        or []
    )

    # Also check top-level
    if not service_list:
        service_list = (
            data.get("serviceList")
            or data.get("services")
            or []
        )

    for item in service_list:
        if not isinstance(item, dict):
            continue

        entry = {
            "service_no": item.get("serviceNo") or item.get("serviceNumber") or item.get("no"),
            "km": to_int(item.get("km") or item.get("kms") or item.get("kilometre")),
            "months": to_int(item.get("months") or item.get("month")),
            "cost_labour": to_int(
                item.get("labourCost") or item.get("labour") or item.get("laborCost")
            ),
            "cost_parts": to_int(
                item.get("partsCost") or item.get("parts") or item.get("sparesCost")
            ),
            "cost_total": to_int(
                item.get("totalCost") or item.get("cost") or item.get("total")
            ),
            "service_type": normalize_spaces(
                item.get("serviceType") or item.get("type") or "Periodic"
            ),
        }

        # Derive total if missing
        if not entry["cost_total"] and entry["cost_labour"] and entry["cost_parts"]:
            entry["cost_total"] = entry["cost_labour"] + entry["cost_parts"]

        if entry["cost_total"] and entry["cost_total"] > 0:
            schedule.append(entry)

    return schedule


def parse_amc_from_json(data: dict) -> List[Dict]:
    """Extract AMC package data."""
    amc_list = []

    amc_data = (
        data.get("amcPackages")
        or data.get("amc")
        or data.get("maintenancePackages")
        or []
    )

    for item in amc_data:
        if not isinstance(item, dict):
            continue
        pkg = {
            "name": normalize_spaces(item.get("name") or item.get("packageName") or ""),
            "duration_years": to_int(item.get("durationYears") or item.get("years")),
            "km_limit": to_int(item.get("kmLimit") or item.get("kms")),
            "cost": to_int(item.get("cost") or item.get("price") or item.get("amount")),
            "inclusions": item.get("inclusions") or item.get("includes") or [],
        }
        if pkg["cost"] and pkg["cost"] > 0:
            amc_list.append(pkg)

    return amc_list


def compute_totals(schedule: List[Dict]) -> Dict:
    """Compute 3yr and 5yr estimated maintenance costs from schedule."""
    totals = {"total_3yr_cost": None, "total_5yr_cost": None}

    if not schedule:
        return totals

    # Estimate based on km milestones: 3yr ≈ 45,000 km, 5yr ≈ 75,000 km
    cost_3yr = sum(
        s["cost_total"] for s in schedule
        if s.get("km") and s["km"] <= 45000 and s.get("cost_total")
    )
    cost_5yr = sum(
        s["cost_total"] for s in schedule
        if s.get("km") and s["km"] <= 75000 and s.get("cost_total")
    )

    if cost_3yr > 0:
        totals["total_3yr_cost"] = cost_3yr
    if cost_5yr > 0:
        totals["total_5yr_cost"] = cost_5yr

    return totals


def extract_json_from_html(html: str) -> List[dict]:
    """Pull embedded JSON state blobs from page HTML."""
    blobs = []
    pattern = re.compile(
        r'(?:window\.__INITIAL_STATE__|__NEXT_DATA__|window\.__data__)\s*=\s*(\{.+?\});',
        re.DOTALL,
    )
    for match in pattern.finditer(html):
        try:
            blobs.append(json.loads(match.group(1)))
        except json.JSONDecodeError:
            pass

    # Also try <script type="application/json">
    script_pattern = re.compile(
        r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>',
        re.DOTALL | re.IGNORECASE,
    )
    for match in script_pattern.finditer(html):
        try:
            blobs.append(json.loads(match.group(1)))
        except json.JSONDecodeError:
            pass

    return blobs


def parse_service_from_table(html: str) -> List[Dict]:
    """
    Regex fallback: parse service cost HTML tables.
    Targets rows like: 5,000 km | 6 months | ₹2,500 | ₹1,800 | ₹4,300
    """
    schedule = []
    normalized = html.replace("\\/", "/")

    # Row pattern: km / months / cost columns
    row_pattern = re.compile(
        r'<tr[^>]*>(.*?)</tr>',
        re.DOTALL | re.IGNORECASE,
    )
    cell_pattern = re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', re.DOTALL | re.IGNORECASE)

    for row_match in row_pattern.finditer(normalized):
        row_html = row_match.group(1)
        cells = [
            normalize_spaces(re.sub(r"<[^>]+>", " ", cell.group(1)))
            for cell in cell_pattern.finditer(row_html)
        ]

        if len(cells) < 3:
            continue

        # Check if this looks like a service row (has km-like number)
        km_val = None
        for cell in cells:
            km_m = re.search(r"([\d,]+)\s*(?:km|kms|kilometre)", cell, re.IGNORECASE)
            if km_m:
                km_val = to_int(km_m.group(1))
                break

        if not km_val:
            continue

        # Extract costs from remaining cells
        costs = []
        for cell in cells:
            cost_m = re.search(r"(?:₹|rs\.?)?\s*([\d,]+)", cell)
            if cost_m:
                val = to_int(cost_m.group(1))
                if val and val > 100:
                    costs.append(val)

        if not costs:
            continue

        entry = {
            "km": km_val,
            "months": None,
            "cost_labour": costs[0] if len(costs) >= 2 else None,
            "cost_parts": costs[1] if len(costs) >= 2 else None,
            "cost_total": costs[-1] if costs else None,
            "service_type": "Periodic",
        }
        if entry["cost_total"] and entry["cost_total"] > 0:
            schedule.append(entry)

    return schedule


def scrape_model_service(model_entry: dict) -> dict:
    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]
    brand_display = model_entry["brand_display"]
    model_display = model_entry["model_display"]

    session = build_session()

    candidate_urls = [
        f"{BASE_CD}/{brand_slug}/{model_slug}/maintenance-cost",
        f"{BASE_CD}/{brand_slug}/{model_slug}/service-cost",
        f"{BASE_CD}/{brand_slug}-{model_slug}/maintenance-cost.htm",
        f"{BASE_CD}/{brand_slug}-{model_slug}-maintenance-cost.htm",
    ]

    schedule = []
    amc_packages = []
    source_url = None

    for url in candidate_urls:
        html = fetch_text(session, url)
        if not html or len(html) < 3000:
            continue

        source_url = url

        # Try JSON extraction first
        blobs = extract_json_from_html(html)
        for blob in blobs:
            if not schedule:
                schedule = parse_service_schedule_from_json(blob)
            if not amc_packages:
                amc_packages = parse_amc_from_json(blob)
            if schedule:
                break

        # Fallback to table parsing
        if not schedule:
            schedule = parse_service_from_table(html)

        if schedule:
            break

    totals = compute_totals(schedule)

    time.sleep(0.15 + random.uniform(0.05, 0.2))

    return {
        "brand": brand_display,
        "model": model_display,
        "brand_slug": brand_slug,
        "model_slug": model_slug,
        "service_schedule": schedule,
        "service_count": len(schedule),
        "amc_packages": amc_packages,
        **totals,
        "source_url": source_url,
        "source": "cardekho_service_v1",
        "last_updated": TODAY,
        "scrape_timestamp": datetime.now().isoformat(),
    }


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Service cost scraper")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit-models", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    workers = max(1, min(int(args.workers), 3))
    start = time.time()

    print("Building model universe...")
    universe = build_ncr_variant_universe(active_only=True)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

    if args.limit_models > 0:
        models = models[:args.limit_models]

    print(f"Models: {len(models)} | Workers: {workers} | Dry run: {args.dry_run}")

    all_docs = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_model_service, m): m for m in models}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Service cost", unit="model"):
            try:
                result = future.result()
                all_docs.append(result)
            except Exception as e:
                print(f"Error: {e}")

    found = sum(1 for d in all_docs if d["service_count"] > 0)
    print(f"\nModels with service data: {found}/{len(all_docs)}")

    if args.dry_run:
        sample = next((d for d in all_docs if d["service_count"] > 0), all_docs[0] if all_docs else {})
        print(json.dumps(sample, indent=2, default=str))
        return

    operations = []
    for doc in all_docs:
        operations.append(
            UpdateOne(
                {"brand": doc["brand"], "model": doc["model"]},
                {"$set": doc},
                upsert=True,
            )
        )

    if operations:
        result = service_costs_collection.bulk_write(operations, ordered=False)
        print(f"Upserted: {result.upserted_count} new, {result.modified_count} updated")

    print(f"Runtime: {round(time.time() - start, 2)}s")


if __name__ == "__main__":
    main()