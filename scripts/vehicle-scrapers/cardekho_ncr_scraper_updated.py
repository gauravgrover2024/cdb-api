import math
import re
import time
from datetime import date, datetime
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import prices_collection, price_history_collection

start_time = time.time()

BASE = "https://www.cardekho.com"
API = "https://www.cardekho.com/api/v3/model/modelprice"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TODAY = date.today().isoformat()
DISCONTINUE_GRACE_DAYS = 7

NCR_CITIES = {
    "price-in-new-delhi": "new-delhi",
    "price-in-gurgaon": "gurgaon",
    "price-in-noida": "noida",
    "car-price-in-new-delhi.htm": "new-delhi",
    "car-price-in-gurgaon.htm": "gurgaon",
    "car-price-in-noida.htm": "noida",
}

session = requests.Session()
session.headers.update(HEADERS)


def retry_get(url, retries=4):
    for i in range(retries):
        try:
            r = session.get(url, timeout=25)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(2 ** i)
    return None


def to_number(x):
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        try:
            xf = float(x)
            return 0.0 if math.isnan(xf) or math.isinf(xf) else xf
        except Exception:
            return 0.0
    if isinstance(x, str):
        txt = x.replace(",", "").replace("₹", "").strip()
        m = re.search(r"\d+(?:\.\d+)?", txt)
        if not m:
            return 0.0
        try:
            return float(m.group(0))
        except Exception:
            return 0.0
    return 0.0


def sanitize_for_mongo(value):
    if isinstance(value, dict):
        return {k: sanitize_for_mongo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_mongo(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def flatten_dict(d, parent_key=""):
    items = {}
    for k, v in (d or {}).items():
        new_key = f"{parent_key}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key + "_"))
        else:
            items[new_key] = v
    return items


def parse_full_price(v):
    others = v.get("others", {}) or {}
    optional = v.get("optionalAccessories", {}) or {}

    flat_others = flatten_dict(others, "other_")
    flat_optional = flatten_dict(optional, "optional_")
    flat_variant = flatten_dict(v, "")

    def pick_raw_numeric(*needles):
        for key, value in flat_variant.items():
            lk = key.lower()
            if any(n in lk for n in needles):
                num = to_number(value)
                if num > 0:
                    return num
        return 0.0

    ex_showroom = to_number(v.get("exShowRoom")) or pick_raw_numeric("exshowroom", "ex_showroom")
    rto = to_number(v.get("rto")) or to_number(v.get("roadTax")) or pick_raw_numeric("roadtax", "road_tax", "rto")
    insurance = to_number(v.get("insurance")) or pick_raw_numeric("insurance")

    or_without = to_number(v.get("ORPWithoutOptionAccessories"))
    optional_total = to_number(optional.get("totalAccessories"))
    on_road = to_number(v.get("onRoadPriceOfVariant"))
    tcs = to_number(flat_others.get("other_tcsCharges")) or pick_raw_numeric("tcs")
    raw_other_total = (
        to_number(flat_others.get("other_totalOtherCharges"))
        or to_number(others.get("totalOtherCharges"))
    )
    explicit_other = (
        to_number(flat_others.get("other_otherCharges"))
        or to_number(flat_others.get("other_handlingCharges"))
    )
    non_tcs_other = explicit_other
    if not non_tcs_other and raw_other_total:
        if tcs and raw_other_total > tcs and abs(raw_other_total - tcs) > 1:
            non_tcs_other = max(raw_other_total - tcs, 0)
        elif tcs and abs(raw_other_total - tcs) <= 1:
            non_tcs_other = 0
        elif tcs:
            non_tcs_other = raw_other_total
        elif not tcs:
            non_tcs_other = raw_other_total

    # Prefer API on-road if present; else derive.
    total_or = on_road if on_road > 0 else (or_without + optional_total)

    return {
        # Primary fields used by your DB/UI
        "ex_showroom": int(ex_showroom) if ex_showroom > 0 else None,
        "rto": int(rto) if rto > 0 else None,
        "insurance": int(insurance) if insurance > 0 else None,

        # Compatibility aliases
        "ex_showroom_price_cardekho": int(ex_showroom) if ex_showroom > 0 else None,
        "rto_amount_cardekho": int(rto) if rto > 0 else None,
        "insurance_amount_cardekho": int(insurance) if insurance > 0 else None,

        "variant_short": v.get("variantShortName") or "",
        **flat_others,
        **flat_optional,
        "tcs": int(tcs) if tcs > 0 else 0,
        "other_tcsCharges": int(tcs) if tcs > 0 else 0,
        "otherCharges": int(non_tcs_other) if non_tcs_other > 0 else 0,
        "other_totalOtherCharges": int(non_tcs_other) if non_tcs_other > 0 else 0,
        "orp_without_accessories": int(or_without) if or_without > 0 else 0,
        "optional_total": int(optional_total) if optional_total > 0 else 0,
        "optional_totalAccessories": int(optional_total) if optional_total > 0 else 0,
        "total_on_road_with_accessories": int(total_or) if total_or > 0 else 0,
        "on_road_price_cardekho": int(total_or) if total_or > 0 else 0,
        "onRoadPrice": int(total_or) if total_or > 0 else 0,
        "raw_price_json": str(v),
    }


def fetch_variants(api_url):
    current = api_url
    for _ in range(6):
        r = retry_get(current)
        if not r:
            return []

        data = r.json()

        redirect = data.get("data", {}).get("redirect")
        if redirect and redirect.get("redirectURL"):
            new_path = redirect["redirectURL"].lstrip("/")
            model_slug = current.split("modelSlug=")[1].split("&")[0]
            current = f"{API}?lang_code=en&regionId=0&otherinfo=all&modelSlug={model_slug}&url={new_path}"
            continue

        sections = data.get("data", {}).get("priceDetailSection", [])
        if not sections:
            return []

        variants = []
        for sec in sections:
            variants.extend(sec.get("variantDetailByFuel", {}).get("variantList", []))
        return variants

    return []


def snapshot_key(doc):
    brand = doc.get("brand") or doc.get("make")
    model = doc.get("model")
    variant = doc.get("variant")
    city = doc.get("city")
    if not all([brand, model, variant, city]):
        return None
    return (str(brand), str(model), str(variant), str(city))


def parse_last_seen_date(last_seen):
    if not last_seen:
        return None
    if isinstance(last_seen, datetime):
        return last_seen.date()
    if isinstance(last_seen, date):
        return last_seen
    if isinstance(last_seen, str):
        txt = last_seen.strip()
        try:
            return datetime.fromisoformat(txt.replace("Z", "+00:00")).date()
        except Exception:
            pass
        try:
            return datetime.strptime(txt[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


print("Fetching sitemap index...")
resp = retry_get(f"{BASE}/sitemap.xml")
if not resp:
    raise RuntimeError("Failed to fetch sitemap")

root = ET.fromstring(resp.text)
ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
sitemaps = [loc.text for loc in root.findall("ns:sitemap/ns:loc", ns)]
price_maps = [s for s in sitemaps if "car-model-price-sitemap" in s]

pages = []
print("Discovering NCR pages...")
for sm in price_maps:
    r = retry_get(sm)
    if not r:
        continue
    sm_root = ET.fromstring(r.text)

    for loc in sm_root.findall("ns:url/ns:loc", ns):
        url = loc.text
        path = url.replace(BASE, "").strip("/")
        parts = path.split("/")

        if len(parts) == 3 and parts[2] in NCR_CITIES:
            pages.append((parts[0], parts[1], parts[2], path))

        if len(parts) == 2 and parts[1] in NCR_CITIES:
            combined = parts[0]
            if "-" in combined:
                brand = combined.split("-")[0]
                model = "-".join(combined.split("-")[1:])
                pages.append((brand, model, parts[1], path))

pages = list(set(pages))
print(f"Total pages: {len(pages)}")

rows = []
for brand, model, city_slug, path in tqdm(pages, desc="Scraping", unit="page"):
    brand_title = brand.replace("-", " ").title()
    model_title = model.replace("-", " ").title()
    city = NCR_CITIES[city_slug]

    model_slug = f"/carmodels/{brand_title}/{model_title.replace(' ', '_')}"
    api_url = f"{API}?lang_code=en&regionId=0&otherinfo=all&modelSlug={model_slug}&url={path}"

    variants = fetch_variants(api_url)
    for v in variants:
        row = {
            "city": city,
            "brand": brand_title,
            "make": brand_title,
            "model": f"{brand_title} {model_title}",
            "variant": v.get("variantDisplayName"),
            "fuel_type": v.get("variantFuelType"),
            "LastSeenDate": TODAY,
            "scrape_timestamp": datetime.now().isoformat(),
            "is_discontinued": False,
            **parse_full_price(v),
        }
        rows.append(sanitize_for_mongo(row))

    time.sleep(0.05)

df_today = pd.DataFrame(rows)

print("Loading Mongo snapshot...")
existing_docs = list(prices_collection.find({}))
existing_map = {}
skipped_snapshot_docs = 0
for doc in existing_docs:
    key = snapshot_key(doc)
    if not key:
        skipped_snapshot_docs += 1
        continue
    existing_map[key] = doc

if skipped_snapshot_docs:
    print(f"Skipped {skipped_snapshot_docs} non-price snapshot docs")

today_keys = set()
operations = []
history_records = []

new_count = 0
price_updates = 0
unchanged = 0

for _, row in df_today.iterrows():
    row_dict = sanitize_for_mongo(row.to_dict())

    key = (row_dict["brand"], row_dict["model"], row_dict["variant"], row_dict["city"])
    today_keys.add(key)

    existing = existing_map.get(key)

    if not existing:
        new_count += 1
        history_records.append({
            "brand": row_dict["brand"],
            "model": row_dict["model"],
            "variant": row_dict["variant"],
            "city": row_dict["city"],
            "price": row_dict.get("on_road_price_cardekho", 0),
            "date": TODAY,
        })
    elif abs(float(existing.get("on_road_price_cardekho", 0) or 0) - float(row_dict.get("on_road_price_cardekho", 0) or 0)) > 1:
        price_updates += 1
        history_records.append({
            "brand": row_dict["brand"],
            "model": row_dict["model"],
            "variant": row_dict["variant"],
            "city": row_dict["city"],
            "price": row_dict.get("on_road_price_cardekho", 0),
            "date": TODAY,
        })
    else:
        unchanged += 1

    operations.append(
        UpdateOne(
            {"brand": row_dict["brand"], "model": row_dict["model"], "variant": row_dict["variant"], "city": row_dict["city"]},
            {"$set": row_dict},
            upsert=True,
        )
    )

print("Writing updates to Mongo...")
if operations:
    prices_collection.bulk_write(operations)

if history_records:
    price_history_collection.insert_many(history_records)

print("Checking discontinuations with grace period...")
existing_docs_full = list(prices_collection.find({}))
pending_discontinue = 0
discontinued_count = 0

for doc in existing_docs_full:
    key = snapshot_key(doc)
    if not key:
        continue
    if key in today_keys:
        continue
    if doc.get("is_discontinued"):
        continue

    last_seen_date = parse_last_seen_date(doc.get("LastSeenDate"))
    if not last_seen_date:
        continue

    days_missing = (date.today() - last_seen_date).days
    if days_missing >= DISCONTINUE_GRACE_DAYS:
        prices_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"is_discontinued": True, "discontinued_date": TODAY}},
        )
        discontinued_count += 1
    else:
        pending_discontinue += 1

print("\n===== RUN CHANGE SUMMARY =====")
print(f"New variants: {new_count}")
print(f"Price updates: {price_updates}")
print(f"Pending discontinue: {pending_discontinue}")
print(f"Marked discontinued: {discontinued_count}")
print(f"Unchanged: {unchanged}")
print(f"Total processed: {len(df_today)}")
print(f"History records inserted: {len(history_records)}")
print(f"\nRuntime: {round(time.time() - start_time, 2)} seconds")
