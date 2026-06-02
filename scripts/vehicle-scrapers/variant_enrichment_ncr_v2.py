#!/usr/bin/env python3
import argparse
import hashlib
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Dict, List, Optional

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import features_collection
from ncr_universe_utils_v2 import (
    build_ncr_variant_universe,
    normalize_key,
    normalize_spaces,
    normalize_variant_key,
    strip_variant_prefix,
    title_from_slug,
)

BASE = "https://www.cardekho.com"
API_MODEL_PRICE = f"{BASE}/api/v3/model/modelprice"
API_MODEL_SPECS = f"{BASE}/api/v3/model/pwamodelspecs"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TODAY = date.today().isoformat()
FEATURE_HASH_VERSION = "sha256-json-v1"

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



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NCR-driven feature enrichment v2")
    parser.add_argument("--workers", type=int, default=3, help="Max worker threads (hard capped at 3)")
    parser.add_argument("--limit-models", type=int, default=0, help="Optional model limit for test runs")
    parser.add_argument(
        "--limit-variants-per-model",
        type=int,
        default=0,
        help="Optional per-model variant limit for test runs",
    )
    parser.add_argument("--dry-run", action="store_true", help="No DB writes")
    parser.add_argument(
        "--include-discontinued",
        action="store_true",
        help="Use all variants from vehicles collection (default: active only)",
    )
    return parser.parse_args()



def clamp_workers(workers: int) -> int:
    return max(1, min(int(workers or 1), 3))



def normalize_feature_name(name: str) -> str:
    raw = normalize_spaces(name)
    low = raw.lower()
    for k, v in FEATURE_NORMALIZATION.items():
        if k in low:
            return v
    return raw



def stable_features_json(features: Dict) -> str:
    return json.dumps(
        features or {},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )



def compute_features_hash(features: Dict) -> str:
    return hashlib.sha256(stable_features_json(features).encode("utf-8")).hexdigest()



def identity_key(brand: str, model: str, variant: str) -> str:
    return f"{brand}||{model}||{variant}"




def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s



def fetch_json(session: requests.Session, url: str, params: Optional[Dict] = None, retries: int = 4) -> Optional[Dict]:
    for i in range(retries):
        try:
            resp = session.get(url, params=params, timeout=(10, 30))
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        time.sleep((2 ** i) + random.uniform(0.03, 0.12))
    return None



def fetch_model_variants(session: requests.Session, brand_slug: str, model_slug: str) -> List[Dict]:
    brand_title = title_from_slug(brand_slug)
    model_title = title_from_slug(model_slug)
    model_slug_path = f"/carmodels/{brand_title}/{model_title.replace(' ', '_')}"

    params = {
        "lang_code": "en",
        "regionId": "0",
        "otherinfo": "all",
        "modelSlug": model_slug_path,
        "url": f"{brand_slug}-{model_slug}/car-price-in-new-delhi.htm",
    }

    for _ in range(6):
        payload = fetch_json(session, API_MODEL_PRICE, params=params)
        if not payload:
            return []

        redirect = payload.get("data", {}).get("redirect") or {}
        redirect_url = redirect.get("redirectURL")
        if redirect_url:
            params["url"] = str(redirect_url).lstrip("/")
            continue

        sections = payload.get("data", {}).get("priceDetailSection", []) or []
        out = []
        for section in sections:
            for row in (section.get("variantDetailByFuel", {}) or {}).get("variantList", []) or []:
                slug = normalize_spaces(row.get("variantSlug"))
                name = normalize_spaces(row.get("variantDisplayName"))
                if slug and name:
                    out.append({"variant_slug": slug, "variant_name": name})
        return out

    return []



def resolve_variant_slug(
    target_variant: str,
    brand_display: str,
    model_display: str,
    available_variants: List[Dict],
) -> Optional[Dict]:
    if not available_variants:
        return None

    target_full = normalize_key(target_variant)
    target_clean = normalize_variant_key(target_variant, brand_display, model_display)

    enriched = []
    by_full = {}
    by_clean = {}

    for item in available_variants:
        name = item["variant_name"]
        full_key = normalize_key(name)
        clean_key = normalize_variant_key(name, brand_display, model_display)
        payload = {**item, "full_key": full_key, "clean_key": clean_key}
        enriched.append(payload)
        by_full.setdefault(full_key, payload)
        by_clean.setdefault(clean_key, payload)

    if target_clean in by_clean:
        return by_clean[target_clean]
    if target_full in by_full:
        return by_full[target_full]

    # Fuzzy fallback based on token overlap in cleaned keys.
    target_tokens = set(target_clean.split()) or set(target_full.split())
    best = None
    best_score = 0.0
    for item in enriched:
        candidate_tokens = set(item["clean_key"].split()) or set(item["full_key"].split())
        if not candidate_tokens or not target_tokens:
            continue
        inter = len(target_tokens & candidate_tokens)
        union = len(target_tokens | candidate_tokens)
        score = inter / union if union else 0.0
        if score > best_score:
            best = item
            best_score = score

    if best and best_score >= 0.55:
        return best

    return None



def fetch_variant_features(
    session: requests.Session,
    brand_slug: str,
    model_slug: str,
    variant_slug: str,
) -> tuple[bool, List[Dict]]:
    params = {
        "business_unit": "car",
        "country_code": "in",
        "_format": "json",
        "lang_code": "en",
        "regionId": "0",
        "otherinfo": "all",
        "brandSlug": brand_slug,
        "modelSlug": model_slug,
        "variantSlug": variant_slug,
        "url": f"{brand_slug}/{model_slug}/specs",
    }

    for _ in range(6):
        payload = fetch_json(session, API_MODEL_SPECS, params=params)
        if not payload:
            return (False, [])

        redirect = payload.get("data", {}).get("redirect") or {}
        redirect_url = redirect.get("redirectURL")
        if redirect_url:
            params["url"] = str(redirect_url).lstrip("/")
            continue

        specs = (payload.get("data", {}) or {}).get("specs", {}) or {}
        sections = []
        for value in specs.values():
            if isinstance(value, list):
                sections.extend(value)
        return (True, sections)

    return (False, [])



def sections_to_feature_map(sections: List[Dict]) -> Dict:
    matrix = {}
    for section in sections or []:
        category = normalize_spaces(section.get("title") or "General")
        for item in (section.get("items") or []):
            raw = normalize_spaces(item.get("text"))
            if not raw:
                continue
            feature = normalize_feature_name(raw)
            value = item.get("value")
            if value in (None, ""):
                value = "Yes" if item.get("available") else "No"
            key = f"{category} | {feature}"
            matrix[key] = value
    return matrix



def main() -> None:
    args = parse_args()
    workers = clamp_workers(args.workers)

    start = time.time()
    print("Building NCR variant universe from vehicles collection...")
    universe = build_ncr_variant_universe(active_only=not args.include_discontinued)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

    if args.limit_models and args.limit_models > 0:
        models = models[: args.limit_models]

    print(f"Models in scope: {len(models)} | workers: {workers} | dry_run: {args.dry_run}")

    session = build_session()
    operations: List[UpdateOne] = []
    run_started_at = datetime.now().isoformat()

    existing_hash_by_identity = {
        identity_key(
            normalize_spaces(doc.get("brand")),
            normalize_spaces(doc.get("model")),
            normalize_spaces(doc.get("variant")),
        ): doc.get("featuresHash")
        for doc in features_collection.find(
            {},
            {
                "brand": 1,
                "model": 1,
                "variant": 1,
                "featuresHash": 1,
            },
        )
    }

    models_processed = 0
    models_skipped = 0
    variants_targeted = 0
    variants_resolved = 0
    variants_unresolved = 0
    variants_empty_features = 0
    variants_feature_unchanged = 0
    variants_feature_changed_or_new = 0

    for model_entry in tqdm(models, desc="Feature enrich", unit="model"):
        brand_slug = model_entry["brand_slug"]
        model_slug = model_entry["model_slug"]
        brand_display = model_entry["brand_display"]
        model_display = model_entry["model_display"]
        target_variants = list(model_entry.get("variant_list") or [])

        if args.limit_variants_per_model and args.limit_variants_per_model > 0:
            target_variants = target_variants[: args.limit_variants_per_model]

        if not target_variants:
            models_skipped += 1
            continue

        variants_targeted += len(target_variants)

        available = fetch_model_variants(session, brand_slug, model_slug)
        if not available:
            models_skipped += 1
            variants_unresolved += len(target_variants)
            continue

        resolved = []
        for target_variant in target_variants:
            match = resolve_variant_slug(
                target_variant,
                brand_display,
                model_display,
                available,
            )
            if not match:
                variants_unresolved += 1
                continue
            resolved.append((target_variant, match["variant_slug"]))

        if not resolved:
            models_skipped += 1
            continue

        models_processed += 1

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    fetch_variant_features,
                    session,
                    brand_slug,
                    model_slug,
                    variant_slug,
                ): (variant_display, variant_slug)
                for variant_display, variant_slug in resolved
            }

            for future in as_completed(futures):
                variant_display, variant_slug = futures[future]
                try:
                    success, sections = future.result()
                except Exception:
                    success, sections = (False, [])

                if not success:
                    variants_empty_features += 1
                    continue

                features = sections_to_feature_map(sections)
                if not features:
                    variants_empty_features += 1

                variants_resolved += 1

                features_hash = compute_features_hash(features)
                identity = {
                    "brand": brand_display,
                    "model": model_display,
                    "variant": variant_display,
                }
                existing_hash = existing_hash_by_identity.get(
                    identity_key(brand_display, model_display, variant_display)
                )

                if existing_hash == features_hash:
                    variants_feature_unchanged += 1
                    operations.append(
                        UpdateOne(
                            identity,
                            {
                                "$set": {
                                    "scraperSeenAt": run_started_at,
                                    "lastSeenAt": TODAY,
                                    "featuresHash": features_hash,
                                    "featuresHashVersion": FEATURE_HASH_VERSION,
                                }
                            },
                            upsert=False,
                        )
                    )
                else:
                    variants_feature_changed_or_new += 1
                    doc = {
                        "brand": brand_display,
                        "model": model_display,
                        "variant": variant_display,
                        "brand_slug": brand_slug,
                        "model_slug": model_slug,
                        "variant_slug": variant_slug,
                        "features": features,
                        "featuresHash": features_hash,
                        "featuresHashVersion": FEATURE_HASH_VERSION,
                        "featureContentChangedAt": run_started_at,
                        "scraperSeenAt": run_started_at,
                        "lastSeenAt": TODAY,
                        "source": "ncr_variant_enrichment_v2",
                        "last_updated": TODAY,
                        "scrape_timestamp": run_started_at,
                    }

                    operations.append(
                        UpdateOne(
                            identity,
                            {
                                "$set": doc,
                                "$setOnInsert": {
                                    "createdAt": run_started_at,
                                },
                            },
                            upsert=True,
                        )
                    )

        time.sleep(0.03 + random.uniform(0.01, 0.05))

    if not args.dry_run and operations:
        features_collection.bulk_write(operations)

    runtime = time.time() - start

    print("\n===== NCR FEATURE ENRICHMENT V2 SUMMARY =====")
    print(f"Models in scope: {len(models)}")
    print(f"Models processed: {models_processed}")
    print(f"Models skipped: {models_skipped}")
    print(f"Variants targeted: {variants_targeted}")
    print(f"Variants resolved+written: {variants_resolved}")
    print(f"Variants unresolved: {variants_unresolved}")
    print(f"Variants with empty features: {variants_empty_features}")
    print(f"Feature docs unchanged: {variants_feature_unchanged}")
    print(f"Feature docs changed/new/unhashed: {variants_feature_changed_or_new}")
    print(f"Upserts prepared: {len(operations)}")
    print(f"Workers used: {workers}")
    print(f"Dry run: {args.dry_run}")
    print(f"Runtime: {runtime:.2f}s")


if __name__ == "__main__":
    main()
