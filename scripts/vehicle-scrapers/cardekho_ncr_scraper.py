import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
from datetime import datetime, date
from tqdm import tqdm
from pymongo import UpdateOne
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


def retry_get(url, retries=3):
    for i in range(retries):
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                return r
        except:
            pass
        time.sleep(2 ** i)
    return None


def to_number(x):
    if x is None:
        return 0
    if isinstance(x, (int, float)):
        return x
    if isinstance(x, str):
        x = x.replace(",", "").replace("₹", "").strip()
        try:
            return float(x)
        except:
            return 0
    return 0


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

    or_without = to_number(v.get("ORPWithoutOptionAccessories"))
    optional_total = to_number(optional.get("totalAccessories"))

    total_or = or_without + optional_total

    return {
        "orp_without_accessories": or_without,
        "optional_total": optional_total,
        "total_on_road_with_accessories": total_or,
        "on_road_price_cardekho": to_number(v.get("onRoadPriceOfVariant")),
        **flat_others,
        **flat_optional,
        "raw_price_json": str(v)
    }


def fetch_variants(api_url):
    current = api_url
    for _ in range(5):
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

        return sections[0].get("variantDetailByFuel", {}).get("variantList", [])

    return []


print("Fetching sitemap index...")
resp = retry_get(f"{BASE}/sitemap.xml")
if not resp:
    raise Exception("Failed to fetch sitemap")

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
        rows.append({
            "city": city,
            "brand": brand_title,
            "model": f"{brand_title} {model_title}",
            "variant": v.get("variantDisplayName"),
            "fuel_type": v.get("variantFuelType"),
            "LastSeenDate": TODAY,
            "scrape_timestamp": datetime.now().isoformat(),
            "is_discontinued": False,
            **parse_full_price(v)
        })

    time.sleep(0.05)

df_today = pd.DataFrame(rows)


def snapshot_key(doc):
    brand = doc.get("brand") or doc.get("make")
    model = doc.get("model")
    variant = doc.get("variant")
    city = doc.get("city")

    # Ignore non-scraper rows living in the same collection, such as loan-form placeholders.
    if not all([brand, model, variant, city]):
        return None

    return (str(brand), str(model), str(variant), str(city))


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

    key = (row["brand"], row["model"], row["variant"], row["city"])
    today_keys.add(key)

    existing = existing_map.get(key)

    if not existing:
        new_count += 1
        history_records.append({
            "brand": row["brand"],
            "model": row["model"],
            "variant": row["variant"],
            "city": row["city"],
            "price": row["on_road_price_cardekho"],
            "date": TODAY
        })

    elif abs(existing.get("on_road_price_cardekho", 0) - row["on_road_price_cardekho"]) > 1:
        price_updates += 1
        history_records.append({
            "brand": row["brand"],
            "model": row["model"],
            "variant": row["variant"],
            "city": row["city"],
            "price": row["on_road_price_cardekho"],
            "date": TODAY
        })

    else:
        unchanged += 1

    operations.append(UpdateOne(
        {"brand": row["brand"], "model": row["model"], "variant": row["variant"], "city": row["city"]},
        {"$set": row.to_dict()},
        upsert=True
    ))

print("Writing updates to Mongo...")
prices_collection.bulk_write(operations)

if history_records:
    price_history_collection.insert_many(history_records)

print("Checking discontinuations with grace period...")

existing_docs_full = list(prices_collection.find({}))

pending_discontinue = 0
discontinued_count = 0

for doc in existing_docs_full:

    key = (doc.get("brand"), doc.get("model"), doc.get("variant"), doc.get("city"))

    if key in today_keys:
        continue

    last_seen = doc.get("LastSeenDate")

    if not last_seen:
        continue

    try:
        last_seen_date = datetime.fromisoformat(last_seen).date()
    except:
        continue

    days_missing = (date.today() - last_seen_date).days

    if days_missing >= DISCONTINUE_GRACE_DAYS:

        prices_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "is_discontinued": True,
                "discontinued_date": TODAY
            }}
        )

        discontinued_count += 1

    else:
        pending_discontinue += 1

print("\n===== RUN CHANGE SUMMARY =====")
print(f"🆕 New variants: {new_count}")
print(f"💰 Price updates: {price_updates}")
print(f"⏳ Pending discontinue: {pending_discontinue}")
print(f"🛑 Marked discontinued: {discontinued_count}")
print(f"✅ Unchanged: {unchanged}")
print(f"📦 Total processed: {len(df_today)}")
print(f"📈 History records inserted: {len(history_records)}")

print(f"\n⏱ Runtime: {round(time.time()-start_time,2)} seconds")