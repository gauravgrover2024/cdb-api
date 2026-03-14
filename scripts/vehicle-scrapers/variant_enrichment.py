import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
import random
from tqdm import tqdm
from urllib.parse import urlparse
from datetime import datetime, date
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed
from mongo_connection import features_collection

start_time = time.time()

BASE = "https://www.cardekho.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

INITIAL_WORKERS = 4
MIN_WORKERS = 2

SLEEP_BASE = 0.03
MAX_RETRIES = 3

TODAY = date.today().isoformat()

session = requests.Session()
session.headers.update(HEADERS)

FEATURE_NORMALIZATION = {
    "sun roof": "Sunroof",
    "usb ports": "USB Ports",
    "wireless charging": "Wireless Charging",
    "air conditioner": "Air Conditioning",
    "bluetooth connectivity": "Bluetooth Connectivity",
    "navigation": "Navigation System",
    "android auto": "Android Auto",
    "apple carplay": "Apple CarPlay",
}


def normalize(name):
    name_lower = name.lower()
    for k, v in FEATURE_NORMALIZATION.items():
        if k in name_lower:
            return v
    return name.strip()


def fetch(url):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=(10, 25))
            if r.status_code == 200:
                return r
        except Exception:
            pass

        time.sleep((2 ** attempt) + random.uniform(0.05, 0.2))

    return None


print("📡 Fetching sitemap index...")
resp = fetch(f"{BASE}/sitemap.xml")
root = ET.fromstring(resp.text)

ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
sitemaps = [loc.text for loc in root.findall("ns:sitemap/ns:loc", ns)]
price_maps = [s for s in sitemaps if "car-model-price-sitemap" in s]

models = set()

print("🔎 Discovering models...")
for sm in tqdm(price_maps):
    xml = fetch(sm)
    if not xml:
        continue

    sm_root = ET.fromstring(xml.text)

    for loc in sm_root.findall("ns:url/ns:loc", ns):
        url = loc.text
        path = urlparse(url).path.strip("/").split("/")

        if len(path) == 3 and path[2].startswith("price"):
            models.add((path[0], path[1]))

        if len(path) == 2 and path[1].startswith("car-price"):
            combined = path[0]
            if "-" in combined:
                brand = combined.split("-")[0]
                model = "-".join(combined.split("-")[1:])
                models.add((brand, model))

models_discovered = len(models)
print(f"✅ Models discovered: {models_discovered}")


def get_variants(brand, model):

    brand_title = brand.replace("-", " ").title()
    model_title = model.replace("-", " ").title()
    model_slug = f"/carmodels/{brand_title}/{model_title.replace(' ', '_')}"

    api = (
        f"{BASE}/api/v3/model/modelprice"
        f"?lang_code=en&regionId=0&otherinfo=all"
        f"&modelSlug={model_slug}"
        f"&url={brand}-{model}/car-price-in-new-delhi.htm"
    )

    current = api

    for _ in range(5):
        r = fetch(current)
        if not r:
            return {}

        data = r.json()

        redirect = data.get("data", {}).get("redirect")
        if redirect and redirect.get("redirectURL"):
            new_path = redirect["redirectURL"].lstrip("/")
            current = (
                f"{BASE}/api/v3/model/modelprice"
                f"?lang_code=en&regionId=0&otherinfo=all"
                f"&modelSlug={model_slug}&url={new_path}"
            )
            continue

        sections = data.get("data", {}).get("priceDetailSection", [])
        if not sections:
            return {}

        variants = {}
        for sec in sections:
            vd = sec.get("variantDetailByFuel", {})
            for v in vd.get("variantList", []):
                slug = v.get("variantSlug")
                name = v.get("variantDisplayName")
                if slug and name:
                    variants[slug] = name

        return variants

    return {}

def get_features(brand, model, variant_slug):

    current_url = (
        f"{BASE}/api/v3/model/pwamodelspecs"
        f"?business_unit=car&country_code=in&_format=json"
        f"&lang_code=en&regionId=0&otherinfo=all"
        f"&brandSlug={brand}&modelSlug={model}"
        f"&variantSlug={variant_slug}"
        f"&url={brand}/{model}/specs"
    )

    for _ in range(5):
        r = fetch(current_url)
        if not r:
            return []

        data = r.json()

        redirect = data.get("data", {}).get("redirect")
        if redirect and redirect.get("redirectURL"):
            new_path = redirect["redirectURL"].lstrip("/")
            current_url = (
                f"{BASE}/api/v3/model/pwamodelspecs"
                f"?business_unit=car&country_code=in&_format=json"
                f"&lang_code=en&regionId=0&otherinfo=all"
                f"&brandSlug={brand}&modelSlug={model}"
                f"&variantSlug={variant_slug}"
                f"&url={new_path}"
            )
            continue

        specs = data.get("data", {}).get("specs", {})

        sections = []
        for v in specs.values():
            if isinstance(v, list):
                sections.extend(v)

        return sections

    return []

existing_docs = list(features_collection.find({}))
existing_map = {
    (d["brand"], d["model"], d["variant"]): d.get("features", {})
    for d in existing_docs
}

workers = INITIAL_WORKERS
empty_feature_count = 0
variant_calls = 0

models_processed = 0
models_skipped = 0

new_variants = 0
feature_updates = 0
unchanged = 0

operations = []

for brand, model in tqdm(sorted(models), desc="🚗 Models"):

    variants = get_variants(brand, model)
    if not variants:
        models_skipped += 1
        continue

    models_processed += 1
    matrix = {}

    def worker_task(item):
        slug, vname = item
        sections = get_features(brand, model, slug)
        return slug, vname, sections

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker_task, item) for item in variants.items()]

        for future in as_completed(futures):
            slug, vname, sections = future.result()
            variant_calls += 1

            if not sections:
                empty_feature_count += 1
                continue

            for section in sections:
                category = section.get("title", "General")

                for item in section.get("items", []):
                    raw = item.get("text")
                    if not raw:
                        continue

                    feature = normalize(raw)
                    value = item.get("value") or ("Yes" if item.get("available") else "No")

                    key = f"{category} | {feature}"

                    matrix.setdefault(key, {})
                    matrix[key][vname] = value

    # throttle detection
    if variant_calls > 50:
        empty_ratio = empty_feature_count / variant_calls
        if empty_ratio > 0.25 and workers > MIN_WORKERS:
            workers -= 1
            print(f"⚠ Throttle suspected — reducing workers to {workers}")

    if not matrix:
        continue

    df = pd.DataFrame.from_dict(matrix, orient="index")

    for variant in df.columns:
        features = df[variant].dropna().to_dict()

        key = (brand.title(), model.title(), variant)
        existing = existing_map.get(key)

        if not existing:
            new_variants += 1
        elif existing != features:
            feature_updates += 1
        else:
            unchanged += 1

        operations.append(UpdateOne(
            {"brand": key[0], "model": key[1], "variant": key[2]},
            {"$set": {
                "features": features,
                "last_updated": TODAY,
                "scrape_timestamp": datetime.now().isoformat()
            }},
            upsert=True
        ))

    time.sleep(SLEEP_BASE + random.uniform(0.02, 0.08))

print("Writing to Mongo...")
if operations:
    features_collection.bulk_write(operations)

runtime = time.time() - start_time
throughput = variant_calls / runtime if runtime > 0 else 0

print("\n===== FEATURE RUN SUMMARY =====")
print(f"🚗 Models discovered: {models_discovered}")
print(f"✅ Models processed: {models_processed}")
print(f"⚠️ Models skipped: {models_skipped}")
print(f"🆕 New variants: {new_variants}")
print(f"🔄 Feature updates: {feature_updates}")
print(f"✅ Unchanged: {unchanged}")
print(f"🟡 Empty feature responses: {empty_feature_count}")
print(f"📦 Mongo writes: {len(operations)}")
print(f"⚙ Workers final: {workers}")
print(f"🚀 Throughput: {throughput:.2f} variants/sec")
print(f"\n⏱ Runtime: {runtime:.2f} seconds")