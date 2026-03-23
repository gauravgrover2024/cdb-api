#!/usr/bin/env python3
import argparse
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import colors_collection
from ncr_universe_utils_v2 import build_ncr_variant_universe, normalize_key, normalize_spaces

BASE = "https://www.cardekho.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cardekho.com/",
    "Connection": "keep-alive",
}
TODAY = date.today().isoformat()

IMAGE_RE = re.compile(r"https?://[^\"'\s<>]+\.(?:jpg|jpeg|png)", re.IGNORECASE)
HEX_SUFFIX_RE = re.compile(r"(.+)_([0-9a-fA-F]{6})$")
RESOLUTION_RE = re.compile(r"/(\d{2,4})x(\d{2,4})/")

BAD_NAME_TOKENS = {
    "front",
    "rear",
    "side",
    "interior",
    "exterior",
    "gallery",
    "thumb",
    "thumbnail",
    "banner",
    "logo",
    "default",
    "car",
    "cars",
    "cardekho",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NCR-driven color enrichment v2")
    parser.add_argument("--workers", type=int, default=3, help="Max worker threads (hard capped at 3)")
    parser.add_argument("--limit-models", type=int, default=0, help="Optional model limit for test runs")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes")
    parser.add_argument(
        "--include-discontinued",
        action="store_true",
        help="Use all variants from vehicles collection (default: active only)",
    )
    return parser.parse_args()


def clamp_workers(workers: int) -> int:
    return max(1, min(int(workers or 1), 3))


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_text(session: requests.Session, url: str, retries: int = 4) -> Optional[str]:
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=(10, 30), allow_redirects=True)
            if resp.status_code == 200 and resp.text:
                return resp.text
        except Exception:
            pass
        time.sleep((2**attempt) + random.uniform(0.05, 0.2))
    return None


def clean_color_name(raw: str, brand_slug: str = "", model_slug: str = "") -> str:
    if not raw:
        return ""
    txt = normalize_spaces(str(raw).replace("-", " ").replace("_", " ")).strip()
    txt = re.sub(r"^\d+", "", txt).strip()
    txt_low = txt.lower()

    for noise in [brand_slug.replace("-", " "), model_slug.replace("-", " ")]:
        cleaned_noise = normalize_spaces(noise).lower()
        if cleaned_noise:
            txt_low = txt_low.replace(cleaned_noise, " ")

    tokens = [
        t for t in re.split(r"\s+", txt_low) if t and t not in BAD_NAME_TOKENS and not t.isdigit()
    ]
    if not tokens:
        return ""

    return normalize_spaces(" ".join(tokens)).title()


def resolution_score(url: str) -> int:
    if not url:
        return 0
    if "/large/" in url.lower():
        return 10**9
    match = RESOLUTION_RE.search(url)
    if match:
        return int(match.group(1)) * int(match.group(2))
    return 1


def parse_color_from_filename(filename: str, brand_slug: str, model_slug: str) -> List[Tuple[str, Optional[str]]]:
    base = filename.split("?")[0].rsplit(".", 1)[0]
    parsed: List[Tuple[str, Optional[str]]] = []

    for piece in base.split("-and-"):
        match = HEX_SUFFIX_RE.search(piece)
        if match:
            color_name = clean_color_name(match.group(1), brand_slug, model_slug)
            if color_name:
                parsed.append((color_name, match.group(2).lower()))

    if parsed:
        return parsed

    fallback_name = clean_color_name(base, brand_slug, model_slug)
    if fallback_name:
        return [(fallback_name, None)]

    return []


def extract_active_color_keys(html: str, brand_slug: str, model_slug: str) -> set:
    text = (html or "").replace("\\/", "/")
    active = set()

    patterns = [
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"(?:isActive|active|isSelected)"\s*:\s*true',
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"isDiscontinued"\s*:\s*false',
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"status"\s*:\s*"active"',
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"availability"\s*:\s*"available"',
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            color_name = clean_color_name(match.group(1), brand_slug, model_slug)
            if color_name:
                active.add(color_name.lower())

    html_patterns = [
        r'data-color-name="([^"]+)"[^>]{0,200}class="[^"]*active[^"]*"',
        r'data-colour-name="([^"]+)"[^>]{0,200}class="[^"]*active[^"]*"',
    ]

    for pattern in html_patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            color_name = clean_color_name(match.group(1), brand_slug, model_slug)
            if color_name:
                active.add(color_name.lower())

    return active


def extract_color_rows_from_html(
    html: str,
    brand_slug: str,
    model_slug: str,
    brand_display: str,
    model_display: str,
    source_page: str,
) -> List[Dict]:
    rows: List[Dict] = []
    normalized_html = (html or "").replace("\\/", "/").replace("&amp;", "&")

    found_urls = IMAGE_RE.findall(normalized_html)
    for raw_url in set(found_urls):
        url = unquote(raw_url.split("?")[0])
        url_lower = url.lower()

        if "cardekho" not in url_lower:
            continue

        likely_color_media = (
            "/color/" in url_lower
            or "/colors/" in url_lower
            or bool(HEX_SUFFIX_RE.search(url.rsplit(".", 1)[0]))
        )
        if not likely_color_media:
            continue

        filename = url.split("/")[-1]
        parsed = parse_color_from_filename(filename, brand_slug, model_slug)
        if not parsed:
            continue

        score = resolution_score(url)
        for color_name, hex_code in parsed:
            rows.append(
                {
                    "brand": brand_display,
                    "model": model_display,
                    "color_name": color_name,
                    "hex": hex_code,
                    "image_url": url,
                    "score": score,
                    "key": color_name.lower(),
                    "source_page": source_page,
                }
            )

    pair_re = re.compile(
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,220}?"(?:hexCode|colorCode|colourCode)"\s*:\s*"#?([0-9a-fA-F]{6})"',
        re.IGNORECASE,
    )
    for match in pair_re.finditer(normalized_html):
        color_name = clean_color_name(match.group(1), brand_slug, model_slug)
        if not color_name:
            continue
        rows.append(
            {
                "brand": brand_display,
                "model": model_display,
                "color_name": color_name,
                "hex": match.group(2).lower(),
                "image_url": None,
                "score": 0,
                "key": color_name.lower(),
                "source_page": source_page,
            }
        )

    return rows


def dedupe_best_rows(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[str, Dict] = {}
    for row in rows:
        key = row.get("key")
        if not key:
            continue
        current = grouped.get(key)
        candidate_rank = (
            1 if row.get("image_url") else 0,
            int(row.get("score") or 0),
        )
        if not current:
            grouped[key] = row
            continue
        current_rank = (
            1 if current.get("image_url") else 0,
            int(current.get("score") or 0),
        )
        if candidate_rank > current_rank:
            grouped[key] = row
    return list(grouped.values())


def candidate_color_pages(brand_slug: str, model_slug: str) -> List[str]:
    return [
        f"{BASE}/{brand_slug}/{model_slug}/colors",
        f"{BASE}/{brand_slug}/{model_slug}/colours",
        f"{BASE}/{brand_slug}/{model_slug}/colour",
        f"{BASE}/{brand_slug}-{model_slug}-colors.htm",
        f"{BASE}/{brand_slug}-{model_slug}-colour.htm",
    ]


def model_task(model_entry: Dict) -> Dict:
    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]
    brand_display = model_entry["brand_display"]
    model_display = model_entry["model_display"]

    session = build_session()
    all_rows: List[Dict] = []
    fetched_any = False
    inactive_dropped = 0

    for page in candidate_color_pages(brand_slug, model_slug):
        html = fetch_text(session, page)
        if not html:
            continue

        fetched_any = True
        active_keys = extract_active_color_keys(html, brand_slug, model_slug)
        rows = extract_color_rows_from_html(
            html,
            brand_slug,
            model_slug,
            brand_display,
            model_display,
            page,
        )

        if active_keys:
            before = len(rows)
            rows = [row for row in rows if row.get("key") in active_keys]
            inactive_dropped += max(0, before - len(rows))

        if rows:
            all_rows.extend(rows)
            break

    return {
        "brand_slug": brand_slug,
        "model_slug": model_slug,
        "brand_display": brand_display,
        "model_display": model_display,
        "fetched_any": fetched_any,
        "inactive_dropped": inactive_dropped,
        "rows": dedupe_best_rows(all_rows),
    }


def color_key(brand: str, model: str, color_name: str) -> Tuple[str, str, str]:
    return (
        normalize_key(brand),
        normalize_key(model),
        normalize_key(color_name),
    )


def main() -> None:
    args = parse_args()
    workers = clamp_workers(args.workers)

    start = time.time()
    print("Building NCR model universe from vehicles collection...")
    universe = build_ncr_variant_universe(active_only=not args.include_discontinued)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

    if args.limit_models and args.limit_models > 0:
        models = models[: args.limit_models]

    print(f"Models in scope: {len(models)} | workers: {workers} | dry_run: {args.dry_run}")

    models_processed = 0
    models_skipped = 0
    empty_pages = 0
    inactive_dropped = 0

    new_colors = 0
    updates = 0
    unchanged = 0

    operations: List[UpdateOne] = []

    existing_docs = list(
        colors_collection.find(
            {
                "brand": {"$exists": True, "$ne": ""},
                "model": {"$exists": True, "$ne": ""},
                "color_name": {"$exists": True, "$ne": ""},
            },
            {
                "_id": 1,
                "brand": 1,
                "model": 1,
                "color_name": 1,
                "hex": 1,
                "image_url": 1,
                "last_updated": 1,
            },
        )
    )

    existing_map: Dict[Tuple[str, str, str], Dict] = {}
    for doc in existing_docs:
        key = color_key(doc.get("brand"), doc.get("model"), doc.get("color_name"))
        if all(key):
            existing_map[key] = doc

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(model_task, model) for model in models]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Color enrich", unit="model"):
            result = future.result()
            rows = result["rows"]
            inactive_dropped += result["inactive_dropped"]

            if not result["fetched_any"]:
                models_skipped += 1
                continue

            if not rows:
                empty_pages += 1
                continue

            models_processed += 1

            for row in rows:
                doc = {
                    "brand": normalize_spaces(row["brand"]),
                    "model": normalize_spaces(row["model"]),
                    "color_name": normalize_spaces(row["color_name"]),
                    "hex": row.get("hex"),
                    "image_url": row.get("image_url"),
                    "source_page": row.get("source_page"),
                    "source": "ncr_color_enrichment_v2",
                    "last_updated": TODAY,
                    "scrape_timestamp": datetime.now().isoformat(),
                }

                key = color_key(doc["brand"], doc["model"], doc["color_name"])
                if not all(key):
                    continue

                existing = existing_map.get(key)

                if not existing:
                    new_colors += 1
                elif existing.get("hex") != doc["hex"] or existing.get("image_url") != doc["image_url"]:
                    updates += 1
                else:
                    unchanged += 1

                operations.append(
                    UpdateOne(
                        {
                            "brand": doc["brand"],
                            "model": doc["model"],
                            "color_name": doc["color_name"],
                        },
                        {"$set": doc},
                        upsert=True,
                    )
                )

    if not args.dry_run and operations:
        colors_collection.bulk_write(operations)

    runtime = time.time() - start

    print("\n===== NCR COLOR ENRICHMENT V2 SUMMARY =====")
    print(f"Models in scope: {len(models)}")
    print(f"Models processed: {models_processed}")
    print(f"Models skipped (fetch failed): {models_skipped}")
    print(f"Empty color pages: {empty_pages}")
    print(f"Inactive dropped by filter: {inactive_dropped}")
    print(f"New colors: {new_colors}")
    print(f"Updates: {updates}")
    print(f"Unchanged: {unchanged}")
    print(f"Upserts prepared: {len(operations)}")
    print(f"Workers used: {workers}")
    print(f"Dry run: {args.dry_run}")
    print(f"Runtime: {runtime:.2f}s")


if __name__ == "__main__":
    main()
