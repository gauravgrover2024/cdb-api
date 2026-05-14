#!/usr/bin/env python3
import argparse
import random
import re
import time
import subprocess
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

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
SCRIPT_DIR = Path(__file__).resolve().parent

IMAGE_RE = re.compile(
    r"https?://[^\"'\s<>]+\.(?:jpg|jpeg|png|webp|avif)(?:\?[^\"'\s<>]*)?",
    re.IGNORECASE,
)
HEX_SUFFIX_RE = re.compile(r"(.+)_([0-9a-fA-F]{6})$")
RESOLUTION_RE = re.compile(r"/(\d{2,4})x(\d{2,4})/")
MEDIA_SIZE_RE = re.compile(
    r"/images/(car-images|carexteriorimages)/(?:large|medium|\d{2,4}x\d{2,4})/",
    re.IGNORECASE,
)

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

MAKE_ALIASES = {
    "mercedes": "mercedes-benz",
    "mercedes-benz": "mercedes-benz",
    "maruti": "maruti-suzuki",
    "maruti-suzuki": "maruti-suzuki",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NCR-driven Cardekho color enrichment v3")
    parser.add_argument("--workers", type=int, default=3, help="Max worker threads (hard capped at 3)")
    parser.add_argument("--limit-models", type=int, default=0, help="Optional model limit for test runs")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes and no post-processing")
    parser.add_argument(
        "--include-discontinued",
        action="store_true",
        help="Use all variants from vehicles collection (default: active only)",
    )
    parser.add_argument(
        "--delete-scope-leaks",
        action="store_true",
        help="Delete old wrong-model color docs instead of marking them rejected",
    )
    parser.add_argument("--skip-cleanup", action="store_true", help="Skip old wrong-model cleanup pass")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip image normalization/R2 upload step")
    parser.add_argument("--skip-frame", action="store_true", help="Skip image frame computation step")
    parser.add_argument("--force-normalize", action="store_true", help="Pass --force to normalizer")
    parser.add_argument("--force-frame", action="store_true", help="Pass --force to frame computation")
    parser.add_argument(
        "--normalizer-script",
        default="bulk_normalize_car_images.py",
        help="Normalizer script filename/path",
    )
    parser.add_argument(
        "--frame-script",
        default="compute_car_image_frames.py",
        help="Frame script filename/path",
    )
    parser.add_argument(
        "--normalizer-extra-args",
        default="",
        help="Extra args for normalizer. Example: '--only-missing' if your script supports it",
    )
    parser.add_argument(
        "--frame-extra-args",
        default="",
        help="Extra args for image-frame script",
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
    txt = re.sub(r"^[^a-zA-Z]+", "", txt).strip()
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


def slug_tokens(value: str) -> List[str]:
    text = normalize_spaces(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return [token for token in text.split("-") if token]


def make_slug_variants(brand_slug: str) -> List[str]:
    token = normalize_spaces(brand_slug).lower().replace(" ", "-")
    canonical = MAKE_ALIASES.get(token, token)
    variants = {token, canonical}
    for k, v in MAKE_ALIASES.items():
        if v == canonical:
            variants.add(k)
    return [item for item in variants if item]


def normalize_slug(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        "-",
        normalize_spaces(str(value or "")).lower(),
    ).strip("-")


def compact_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_spaces(str(value or "")).lower())


def slug_exact_equal(left: str, right: str) -> bool:
    left_slug = normalize_slug(left)
    right_slug = normalize_slug(right)
    if not left_slug or not right_slug:
        return False
    if left_slug == right_slug:
        return True

    # Exact compact equivalence handles i20-n-line vs i20nline.
    # It still rejects fortuner vs fortuner-legender and thar vs thar-roxx.
    return compact_slug(left_slug) == compact_slug(right_slug)


def model_slug_variants(model_slug: str) -> List[str]:
    token = normalize_slug(model_slug)
    return [token] if token else []


def canonicalize_image_url(raw_url: str, preferred_size: str = "930x620") -> str:
    base = str(raw_url or "").split("?", 1)[0].strip()
    if not base:
        return ""

    if "cardekho.com" not in base.lower():
        return base

    return MEDIA_SIZE_RE.sub(
        lambda m: f"/images/{m.group(1)}/{preferred_size}/",
        base,
    )


def path_parts_from_url(url: str) -> List[str]:
    parsed = urlparse(url)
    return [
        normalize_slug(part)
        for part in unquote(parsed.path).split("/")
        if normalize_slug(part)
    ]


def extract_url_brand_model_segments(url: str, brand_slug: str) -> Tuple[str, str]:
    normalized = canonicalize_image_url(url)
    if not normalized:
        return "", ""

    parts = path_parts_from_url(normalized)
    make_variants = set(make_slug_variants(brand_slug))

    for index, part in enumerate(parts):
        if part in make_variants and index + 1 < len(parts):
            return part, parts[index + 1]

    return "", ""


def url_matches_scope(url: str, brand_slug: str, model_slug: str) -> bool:
    """
    Strict generic scope check.

    Do not use loose includes matching for model names.
    We compare the Cardekho URL model segment against the expected model slug.

    This is generic, not hardcoded, and separates examples like:
    - Thar vs Thar Roxx
    - Fortuner vs Fortuner Legender
    - Ertiga vs Ertiga Tour
    - Venue vs Venue N Line
    - Creta vs Creta N Line
    """
    normalized = canonicalize_image_url(url)
    if not normalized:
        return False

    url_brand_segment, url_model_segment = extract_url_brand_model_segments(normalized, brand_slug)

    if url_brand_segment and url_model_segment:
        return slug_exact_equal(url_model_segment, model_slug)

    expected_model = normalize_slug(model_slug)
    if not expected_model:
        return False

    # Fallback: exact path-part match only, never partial matching.
    return any(slug_exact_equal(part, expected_model) for part in path_parts_from_url(normalized))

def resolution_score(url: str) -> int:
    if not url:
        return 0
    normalized = canonicalize_image_url(url)
    if "/930x620/" in normalized:
        return 930 * 620
    if "/630x420/" in normalized:
        return 630 * 420
    if "/360x240/" in normalized:
        return 360 * 240
    if "/large/" in normalized.lower():
        return 500 * 320
    match = RESOLUTION_RE.search(normalized)
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
        url = canonicalize_image_url(unquote(raw_url))
        url_lower = url.lower()

        if "cardekho" not in url_lower:
            continue
        if not url_matches_scope(url, brand_slug, model_slug):
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

    # Second pass: if two color labels map to same hex for same model, keep the best media row.
    by_hex: Dict[str, Dict] = {}
    without_hex: List[Dict] = []
    for row in grouped.values():
        hex_code = str(row.get("hex") or "").strip().lower().lstrip("#")
        if not hex_code:
            without_hex.append(row)
            continue

        current = by_hex.get(hex_code)
        candidate_rank = (
            1 if row.get("image_url") else 0,
            int(row.get("score") or 0),
        )
        if not current:
            by_hex[hex_code] = row
            continue

        current_rank = (
            1 if current.get("image_url") else 0,
            int(current.get("score") or 0),
        )
        if candidate_rank >= current_rank:
            by_hex[hex_code] = row

    return [*without_hex, *by_hex.values()]


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


def parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value
    raw = str(value or "").strip()
    if not raw:
        return datetime.min

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        pass

    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue

    return datetime.min


def dedupe_existing_hex_duplicates(dry_run: bool = False) -> Tuple[int, int]:
    docs = list(
        colors_collection.find(
            {
                "brand": {"$exists": True, "$ne": ""},
                "model": {"$exists": True, "$ne": ""},
                "hex": {"$exists": True, "$type": "string", "$ne": ""},
            },
            {
                "_id": 1,
                "brand": 1,
                "model": 1,
                "hex": 1,
                "scrape_timestamp": 1,
                "updatedAt": 1,
                "last_updated": 1,
            },
        )
    )

    grouped: Dict[Tuple[str, str, str], List[Dict]] = {}
    for doc in docs:
        brand_key = normalize_key(doc.get("brand"))
        model_key = normalize_key(doc.get("model"))
        hex_key = str(doc.get("hex") or "").strip().lower().lstrip("#")
        if not brand_key or not model_key or not hex_key:
            continue
        grouped.setdefault((brand_key, model_key, hex_key), []).append(doc)

    duplicate_groups = [rows for rows in grouped.values() if len(rows) > 1]
    removed = 0

    for rows in duplicate_groups:
        keep = max(
            rows,
            key=lambda row: (
                parse_dt(row.get("scrape_timestamp")),
                parse_dt(row.get("updatedAt")),
                parse_dt(row.get("last_updated")),
                str(row.get("_id")),
            ),
        )
        delete_ids = [row.get("_id") for row in rows if row.get("_id") != keep.get("_id")]
        delete_ids = [item for item in delete_ids if item is not None]
        removed += len(delete_ids)
        if not dry_run and delete_ids:
            colors_collection.delete_many({"_id": {"$in": delete_ids}})

    return len(duplicate_groups), removed



def cleanup_existing_scope_leaks(dry_run: bool = False, delete_bad_docs: bool = False) -> Tuple[int, int]:
    docs = list(
        colors_collection.find(
            {
                "brand": {"$exists": True, "$ne": ""},
                "model": {"$exists": True, "$ne": ""},
                "scopeStatus": {"$ne": "rejected"},
                "$or": [
                    {"image_url": {"$exists": True, "$ne": ""}},
                    {"sourceImageUrl": {"$exists": True, "$ne": ""}},
                    {"normalizedImageUrl": {"$exists": True, "$ne": ""}},
                ],
            },
            {
                "_id": 1,
                "brand": 1,
                "model": 1,
                "color_name": 1,
                "image_url": 1,
                "sourceImageUrl": 1,
                "normalizedImageUrl": 1,
            },
        )
    )

    bad_ids = []

    for doc in docs:
        brand_slug = normalize_slug(doc.get("brand"))
        model_slug = normalize_slug(doc.get("model"))
        source_url = doc.get("image_url") or doc.get("sourceImageUrl") or ""

        # Only source Cardekho URLs have reliable brand/model path segments.
        # Do not judge using R2 normalized URL if source image is missing.
        if not source_url or "cardekho" not in source_url.lower():
            continue

        if not url_matches_scope(source_url, brand_slug, model_slug):
            bad_ids.append(doc["_id"])

    if not bad_ids:
        return 0, 0

    if dry_run:
        return len(bad_ids), 0

    if delete_bad_docs:
        result = colors_collection.delete_many({"_id": {"$in": bad_ids}})
        return len(bad_ids), result.deleted_count

    result = colors_collection.update_many(
        {"_id": {"$in": bad_ids}},
        {
            "$set": {
                "scopeStatus": "rejected",
                "scopeRejectReason": "model_scope_mismatch",
                "scopeRejectedAt": datetime.now().isoformat(),
                "scopeVersion": "exact-url-model-segment-v1",
            },
            "$unset": {
                "image_url": "",
                "sourceImageUrl": "",
                "normalizedImageUrl": "",
                "normalizedImagePngUrl": "",
                "cleanImageUrl": "",
                "stagedImageUrl": "",
                "imageFrame": "",
                "imageProcessingStatus": "",
                "imageProcessingMethod": "",
                "imageProcessingHash": "",
            },
        },
    )

    return len(bad_ids), result.modified_count


def split_extra_args(extra: str) -> List[str]:
    raw = str(extra or "").strip()
    if not raw:
        return []
    return raw.split()


def script_path(value: str) -> str:
    candidate = Path(value)
    if candidate.is_absolute():
        return str(candidate)
    return str(SCRIPT_DIR / value)


def run_pipeline_step(label: str, command: List[str], dry_run: bool = False) -> bool:
    print(f"\n===== {label} =====")
    print(" ".join(command))

    if dry_run:
        print("Dry run: skipped")
        return True

    result = subprocess.run(command, cwd=str(SCRIPT_DIR))

    if result.returncode != 0:
        print(f"{label} failed with exit code {result.returncode}")
        return False

    return True


def build_post_process_commands(args: argparse.Namespace, workers: int) -> Tuple[List[str], List[str]]:
    normalizer_command = [
        sys.executable,
        script_path(args.normalizer_script),
        "--write",
        *split_extra_args(args.normalizer_extra_args),
    ]

    if args.force_normalize:
        normalizer_command.append("--force")

    frame_command = [
        sys.executable,
        script_path(args.frame_script),
        "--write",
        "--workers",
        str(workers),
        *split_extra_args(args.frame_extra_args),
    ]

    if args.force_frame:
        frame_command.append("--force")

    return normalizer_command, frame_command

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
    image_changes = 0

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
                "scopeStatus": 1,
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
                image_url = row.get("image_url") or ""

                doc = {
                    "brand": normalize_spaces(row["brand"]),
                    "model": normalize_spaces(row["model"]),
                    "color_name": normalize_spaces(row["color_name"]),
                    "hex": row.get("hex"),
                    "image_url": image_url,
                    "sourceImageUrl": image_url,
                    "source_page": row.get("source_page"),
                    "source": "ncr_color_enrichment_v3",
                    "scopeStatus": "active",
                    "scopeVersion": "exact-url-model-segment-v1",
                    "last_updated": TODAY,
                    "scrape_timestamp": datetime.now().isoformat(),
                }

                key = color_key(doc["brand"], doc["model"], doc["color_name"])
                if not all(key):
                    continue

                existing = existing_map.get(key)
                image_changed = bool(existing and existing.get("image_url") != doc["image_url"])
                hex_changed = bool(existing and existing.get("hex") != doc["hex"])

                if not existing:
                    new_colors += 1
                elif hex_changed or image_changed or existing.get("scopeStatus") == "rejected":
                    updates += 1
                else:
                    unchanged += 1

                if image_changed:
                    image_changes += 1

                update_payload = {"$set": doc}

                if image_changed:
                    update_payload["$unset"] = {
                        "normalizedImageUrl": "",
                        "normalizedImagePngUrl": "",
                        "cleanImageUrl": "",
                        "stagedImageUrl": "",
                        "imageFrame": "",
                        "imageProcessingStatus": "",
                        "imageProcessingMethod": "",
                        "imageProcessingHash": "",
                        "imageProcessedAt": "",
                        "imageQualityWarnings": "",
                    }

                operations.append(
                    UpdateOne(
                        {
                            "brand": doc["brand"],
                            "model": doc["model"],
                            "color_name": doc["color_name"],
                        },
                        update_payload,
                        upsert=True,
                    )
                )

    if not args.dry_run and operations:
        colors_collection.bulk_write(operations, ordered=False)

    hex_duplicate_groups = 0
    hex_duplicates_removed = 0
    if operations or not args.dry_run:
        hex_duplicate_groups, hex_duplicates_removed = dedupe_existing_hex_duplicates(
            dry_run=args.dry_run
        )

    scope_leaks_found = 0
    scope_leaks_fixed = 0
    if not args.skip_cleanup:
        scope_leaks_found, scope_leaks_fixed = cleanup_existing_scope_leaks(
            dry_run=args.dry_run,
            delete_bad_docs=args.delete_scope_leaks,
        )

    post_process_ok = True
    normalizer_ran = False
    frame_ran = False

    normalizer_command, frame_command = build_post_process_commands(args, workers)

    if not args.skip_normalize:
        post_process_ok = run_pipeline_step(
            "NORMALIZE NEW / CHANGED COLOR IMAGES",
            normalizer_command,
            dry_run=args.dry_run,
        )
        normalizer_ran = post_process_ok and not args.dry_run

    if post_process_ok and not args.skip_frame:
        post_process_ok = run_pipeline_step(
            "COMPUTE IMAGE FRAMES",
            frame_command,
            dry_run=args.dry_run,
        )
        frame_ran = post_process_ok and not args.dry_run

    runtime = time.time() - start

    print("\n===== NCR COLOR ENRICHMENT V3 SUMMARY =====")
    print(f"Models in scope: {len(models)}")
    print(f"Models processed: {models_processed}")
    print(f"Models skipped (fetch failed): {models_skipped}")
    print(f"Empty color pages: {empty_pages}")
    print(f"Inactive dropped by filter: {inactive_dropped}")
    print(f"New colors: {new_colors}")
    print(f"Updates: {updates}")
    print(f"Unchanged: {unchanged}")
    print(f"Image URL changes: {image_changes}")
    print(f"Upserts prepared: {len(operations)}")
    print(f"Hex duplicate groups found: {hex_duplicate_groups}")
    print(f"Hex duplicates removed (latest timestamp retained): {hex_duplicates_removed}")
    print(f"Scope leaks found: {scope_leaks_found}")
    print(f"Scope leaks fixed: {scope_leaks_fixed}")
    print(f"Normalizer ran: {normalizer_ran}")
    print(f"Frame computation ran: {frame_ran}")
    print(f"Post-process OK: {post_process_ok}")
    print(f"Workers used: {workers}")
    print(f"Dry run: {args.dry_run}")
    print(f"Runtime: {runtime:.2f}s")


if __name__ == "__main__":
    main()
