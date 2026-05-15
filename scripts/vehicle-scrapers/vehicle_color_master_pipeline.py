# ============================================================
# vehicle_color_master_pipeline.py
# ============================================================
#
# FIRST RUN:
#   TEST ONLY FOR ONE MODEL
#
# Example:
#   python3 vehicle_color_master_pipeline.py --brand kia --model seltos
#
# AFTER VALIDATION:
#   remove filters and run full universe
#
# ============================================================

import os
import re
import io
import cv2
import json
import time
import hashlib
import argparse
import requests
import subprocess
import traceback
import numpy as np

from PIL import Image
from rembg import remove
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from pymongo import ReplaceOne
from datetime import datetime
from collections import defaultdict

from mongo_connection import (
    prices_collection,
    db,
)

# ============================================================
# CONFIG
# ============================================================

COLLECTION_NAME = "vehicle_colors_v2"

vehicle_colors_collection = db[COLLECTION_NAME]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TMP_DIR = os.path.join(BASE_DIR, "tmp")
NORMALIZED_DIR = os.path.join(BASE_DIR, "normalized")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(NORMALIZED_DIR, exist_ok=True)

R2_REMOTE = "r2:cdrive-car-images"
R2_BASE_URL = "https://cdn.acillp.com"

FUZZ_THRESHOLD = 88

PIPELINE_VERSION = 1

# ============================================================
# HELPERS
# ============================================================


def log(msg):
    print(f"[{datetime.utcnow().isoformat()}] {msg}")


def normalize_spaces(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def normalize_key(v):
    v = normalize_spaces(v).lower()
    v = v.replace("&", " and ")
    v = re.sub(r"[^a-z0-9]+", " ", v)
    return normalize_spaces(v)


def slugify(v):
    v = normalize_spaces(v).lower()
    v = v.replace("&", " and ")
    v = re.sub(r"[^a-z0-9]+", "-", v)
    v = re.sub(r"-{2,}", "-", v)
    return v.strip("-")


def sha1(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# ============================================================
# COLOR ALIASES
# ============================================================

CANONICAL_COLOR_ALIASES = {
    "gravity gray": "Gravity Grey",
    "gravitygrey": "Gravity Grey",
    "aurora black": "Aurora Black Pearl",
    "glacier white": "Glacier White Pearl",
    "ivory silver": "Ivory Silver Gloss",
    "pewterolive": "Pewter Olive",
}


def normalize_color_name(name):
    raw = normalize_spaces(name)

    if not raw:
        return ""

    key = normalize_key(raw).replace(" ", "")

    for alias, canonical in CANONICAL_COLOR_ALIASES.items():
        alias_key = normalize_key(alias).replace(" ", "")

        if key == alias_key:
            return canonical

    return " ".join(
        word.capitalize()
        for word in normalize_key(raw).split()
    )


# ============================================================
# BUILD ACTIVE UNIVERSE
# ============================================================

def build_active_universe(
    only_brand=None,
    only_model=None,
):
    query = {
        "$or": [
            {"is_discontinued": False},
            {"is_discontinued": {"$exists": False}},
        ]
    }

    projection = {
        "brand": 1,
        "model_normalized": 1,
        "colors_normalized": 1,
    }

    docs = list(prices_collection.find(query, projection))

    universe = {}

    for doc in docs:
        brand = normalize_spaces(doc.get("brand"))
        model = normalize_spaces(doc.get("model_normalized"))

        if not brand or not model:
            continue

        brand_slug = slugify(brand)
        model_slug = slugify(model)

        if only_brand and brand_slug != only_brand:
            continue

        if only_model and model_slug != only_model:
            continue

        key = (brand_slug, model_slug)

        if key not in universe:
            universe[key] = {
                "brand": brand,
                "model": model,
                "brand_slug": brand_slug,
                "model_slug": model_slug,
                "active_colors": set(),
            }

        for color in doc.get("colors_normalized", []):
            normalized = normalize_color_name(color)

            if normalized:
                universe[key]["active_colors"].add(normalized)

    return universe


# ============================================================
# FETCH PAGE
# ============================================================

def fetch_page(url):
    r = requests.get(
        url,
        headers=HEADERS,
        timeout=60,
    )

    r.raise_for_status()

    return r.text


# ============================================================
# PARSE NEXT DATA
# ============================================================

def extract_next_data(html):
    soup = BeautifulSoup(html, "html.parser")

    script = soup.find(
        "script",
        {"id": "__NEXT_DATA__"}
    )

    if not script:
        return {}

    try:
        return json.loads(script.string)
    except Exception:
        return {}


# ============================================================
# RECURSIVE IMAGE EXTRACTION
# ============================================================

def recursive_extract(obj, results=None):
    if results is None:
        results = []

    if isinstance(obj, dict):
        color_name = None
        image_url = None

        for k, v in obj.items():
            lk = str(k).lower()

            if isinstance(v, str):

                if (
                    "http" in v
                    and any(
                        x in v.lower()
                        for x in [
                            ".jpg",
                            ".jpeg",
                            ".png",
                            ".webp",
                        ]
                    )
                ):
                    image_url = v

                if (
                    "color" in lk
                    or lk in ["name", "title"]
                ):
                    color_name = v

        if image_url:
            results.append({
                "color": color_name,
                "image": image_url,
            })

        for v in obj.values():
            recursive_extract(v, results)

    elif isinstance(obj, list):
        for item in obj:
            recursive_extract(item, results)

    return results


# ============================================================
# SCRAPE CARDKHO COLORS
# ============================================================

def scrape_cardekho_gallery(
    brand_slug,
    model_slug,
):
    urls = [
        f"https://www.cardekho.com/{brand_slug}/{model_slug}/colors",
        f"https://www.cardekho.com/{brand_slug}/{model_slug}",
    ]

    final_results = []

    for url in urls:
        try:
            log(f"Fetching: {url}")

            html = fetch_page(url)

            # ========================================================
            # DEBUG SAVE
            # ========================================================

            debug_path = os.path.join(
                TMP_DIR,
                f"{brand_slug}-{model_slug}.html"
            )

            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(html)

            log(f"HTML SAVED: {debug_path}")

            # ========================================================
            # JSON-LD EXTRACTION
            # ========================================================

            json_ld_matches = re.findall(
                r'<script type="application/ld\\+json">(.*?)</script>',
                html,
                re.DOTALL,
            )

            for raw_json in json_ld_matches:

                try:
                    parsed = json.loads(raw_json)

                    if isinstance(parsed, list):
                        iterable = parsed
                    else:
                        iterable = [parsed]

                    for obj in iterable:

                        # ====================================================
                        # COLORS
                        # ====================================================

                        colors = (
                            obj.get("Color")
                            or obj.get("color")
                            or []
                        )

                        # ====================================================
                        # IMAGES
                        # ====================================================

                        image_list = obj.get("image")

                        extracted_images = []

                        if isinstance(image_list, list):

                            for img in image_list:

                                if isinstance(img, str):
                                    extracted_images.append(img)

                                elif isinstance(img, dict):

                                    url = img.get("url")

                                    if url:
                                        extracted_images.append(url)

                        elif isinstance(image_list, str):
                            extracted_images.append(image_list)

                        # ====================================================
                        # PRIMARY SAME-ANGLE IMAGE
                        # ====================================================

                        primary_image = None

                        for img in extracted_images:

                            img_lower = img.lower()

                            if (
                                "front-left-side" in img_lower
                                or "front-left-side-47" in img_lower
                            ):
                                primary_image = img
                                break

                        if not primary_image and extracted_images:
                            primary_image = extracted_images[0]

                        # ====================================================
                        # MAP COLORS
                        # ====================================================

                        if colors and primary_image:

                            for color in colors:

                                normalized_color = (
                                    normalize_color_name(color)
                                )

                                if not normalized_color:
                                    continue

                                final_results.append({
                                    "color": normalized_color,
                                    "image": primary_image,
                                })

                except Exception:
                    traceback.print_exc()

        except Exception:
            traceback.print_exc()

    # ============================================================
    # DEDUPE
    # ============================================================

    deduped = []

    seen = set()

    for item in final_results:

        color = normalize_color_name(
            item.get("color")
        )

        image = item.get("image")

        if not image:
            continue

        key = f"{color}::{image}"

        if key in seen:
            continue

        seen.add(key)

        deduped.append({
            "color": color,
            "image": image,
        })

    log(f"FINAL DEDUPED COLORS: {len(deduped)}")

    return deduped
# ============================================================
# JSON-LD PARSING
# ============================================================

json_ld_matches = re.findall(
    r'<script type=\"application/ld\\+json\">(.*?)</script>',
    html,
    re.DOTALL,
)

for raw_json in json_ld_matches:
    try:
        parsed = json.loads(raw_json)

        if isinstance(parsed, list):
            iterable = parsed
        else:
            iterable = [parsed]

        for obj in iterable:

            # ------------------------------------------------
            # Extract colors
            # ------------------------------------------------

            colors = obj.get("Color") or obj.get("color")

            # ------------------------------------------------
            # Extract images
            # ------------------------------------------------

            image_list = obj.get("image")

            extracted_images = []

            if isinstance(image_list, list):

                for img in image_list:

                    if isinstance(img, str):
                        extracted_images.append(img)

                    elif isinstance(img, dict):
                        url = img.get("url")

                        if url:
                            extracted_images.append(url)

            elif isinstance(image_list, str):
                extracted_images.append(image_list)

            # ------------------------------------------------
            # Map colors to images
            # ------------------------------------------------

            if colors and extracted_images:

                primary_image = extracted_images[0]

                for color in colors:

                    images.append({
                        "color": color,
                        "image": primary_image,
                    })

        except Exception as e:
            log(f"Failed: {e}")

    deduped = []

    seen = set()

    for item in final_results:
        color = normalize_color_name(item.get("color"))

        image = item.get("image")

        if not image:
            continue

        key = f"{color}::{image}"

        if key in seen:
            continue

        seen.add(key)

        deduped.append({
            "color": color,
            "image": image,
        })

    return deduped


# ============================================================
# MATCH COLORS
# ============================================================

def match_colors(
    active_colors,
    gallery,
):
    matched = []

    for active in active_colors:
        best = None
        best_score = 0

        active_key = normalize_key(active)

        for gallery_item in gallery:
            gallery_color = gallery_item.get("color")

            if not gallery_color:
                continue

            score = fuzz.token_sort_ratio(
                active_key,
                normalize_key(gallery_color),
            )

            if score > best_score:
                best_score = score
                best = gallery_item

        if best and best_score >= FUZZ_THRESHOLD:
            matched.append({
                "color": active,
                "image": best["image"],
                "score": best_score,
            })

    return matched


# ============================================================
# DOWNLOAD IMAGE
# ============================================================

def download_image(url, path):
    r = requests.get(
        url,
        headers=HEADERS,
        timeout=120,
    )

    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)


# ============================================================
# NORMALIZE IMAGE
# ============================================================

def normalize_car_image(
    input_path,
    output_path,
):
    with open(input_path, "rb") as f:
        input_bytes = f.read()

    output = remove(input_bytes)

    image = Image.open(
        io.BytesIO(output)
    ).convert("RGBA")

    arr = np.array(image)

    alpha = arr[:, :, 3]

    coords = cv2.findNonZero(alpha)

    if coords is None:
        raise Exception("No foreground found")

    x, y, w, h = cv2.boundingRect(coords)

    cropped = image.crop((x, y, x + w, y + h))

    canvas = Image.new(
        "RGBA",
        (1600, 1000),
        (255, 255, 255, 0),
    )

    target_width = 1250

    ratio = target_width / cropped.size[0]

    resized = cropped.resize(
        (
            int(cropped.size[0] * ratio),
            int(cropped.size[1] * ratio),
        ),
        Image.LANCZOS,
    )

    x_pos = (
        1600 - resized.size[0]
    ) // 2

    y_pos = (
        1000 - resized.size[1]
    ) // 2

    canvas.paste(
        resized,
        (x_pos, y_pos),
        resized,
    )

    canvas.save(
        output_path,
        "WEBP",
        quality=92,
    )


# ============================================================
# FRAME DETECTION
# ============================================================

def compute_frame_metadata(path):
    img = cv2.imread(
        path,
        cv2.IMREAD_UNCHANGED,
    )

    if img.shape[2] < 4:
        return {}

    alpha = img[:, :, 3]

    coords = cv2.findNonZero(alpha)

    if coords is None:
        return {}

    x, y, w, h = cv2.boundingRect(coords)

    return {
        "x": int(x),
        "y": int(y),
        "width": int(w),
        "height": int(h),
        "aspect_ratio": round(w / h, 4),
    }


# ============================================================
# R2 BATCH UPLOAD
# ============================================================

def batch_upload(files):
    if not files:
        return

    for file_path in files:
        subprocess.run([
            "rclone",
            "copy",
            file_path,
            R2_REMOTE,
        ])


# ============================================================
# PROCESS MODEL
# ============================================================

def process_model(payload):
    brand = payload["brand"]
    model = payload["model"]

    brand_slug = payload["brand_slug"]
    model_slug = payload["model_slug"]

    active_colors = payload["active_colors"]

    log("=" * 80)
    log(f"PROCESSING: {brand} {model}")
    log("=" * 80)

    gallery = scrape_cardekho_gallery(
        brand_slug,
        model_slug,
    )

    log(f"Gallery images found: {len(gallery)}")

    matched = match_colors(
        active_colors,
        gallery,
    )

    log(f"Matched colors: {len(matched)}")

    existing_doc = vehicle_colors_collection.find_one({
        "brand_slug": brand_slug,
        "model_slug": model_slug,
    })

    existing_hashes = {}

    if existing_doc:
        for color_obj in existing_doc.get("colors", []):
            existing_hashes[
                color_obj["name"]
            ] = color_obj.get("sourceHash")

    changed_files = []

    final_colors = []

    hero_image = None

    for item in matched:
        color = item["color"]
        source_image = item["image"]

        source_hash = sha1(source_image)

        if not hero_image:
            hero_image = source_image

        existing_hash = existing_hashes.get(color)

        filename = (
            f"{brand_slug}-"
            f"{model_slug}-"
            f"{slugify(color)}-"
            f"{source_hash[:10]}"
        )

        normalized_filename = f"{filename}.webp"

        normalized_local_path = os.path.join(
            NORMALIZED_DIR,
            normalized_filename,
        )

        normalized_r2_url = (
            f"{R2_BASE_URL}/{normalized_filename}"
        )

        if existing_hash != source_hash:
            log(f"UPDATED IMAGE: {color}")

            tmp_path = os.path.join(
                TMP_DIR,
                f"{filename}.jpg",
            )

            try:
                download_image(
                    source_image,
                    tmp_path,
                )

                normalize_car_image(
                    tmp_path,
                    normalized_local_path,
                )

                changed_files.append(
                    normalized_local_path
                )

            except Exception:
                traceback.print_exc()
                continue

        else:
            log(f"UNCHANGED: {color}")

        frame_meta = compute_frame_metadata(
            normalized_local_path
        )

        final_colors.append({
            "name": color,

            "sourceImageUrl": source_image,

            "normalizedImageUrl": normalized_r2_url,

            "sourceHash": source_hash,

            "frameMeta": frame_meta,
        })

    # ========================================================
    # VALIDATION
    # ========================================================

    if not final_colors:
        log("NO VALID COLORS")
        return

    # ========================================================
    # R2 UPLOAD
    # ========================================================

    log(f"Uploading changed files: {len(changed_files)}")

    batch_upload(changed_files)

    # ========================================================
    # FINAL DOCUMENT
    # ========================================================

    final_document = {
        "brand": brand,
        "model": model,

        "brand_slug": brand_slug,
        "model_slug": model_slug,

        "heroImage": hero_image,

        "colors": sorted(
            final_colors,
            key=lambda x: normalize_key(x["name"])
        ),

        "pipelineVersion": PIPELINE_VERSION,

        "updatedAt": datetime.utcnow(),
    }

    # ========================================================
    # FINAL ATOMIC REPLACE
    # ========================================================

    log("FINAL MONGO REPLACE")

    vehicle_colors_collection.replace_one(
        {
            "brand_slug": brand_slug,
            "model_slug": model_slug,
        },
        final_document,
        upsert=True,
    )

    log("DONE")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--brand",
        type=str,
        default=None,
    )

    parser.add_argument(
        "--model",
        type=str,
        default=None,
    )

    args = parser.parse_args()

    universe = build_active_universe(
        only_brand=args.brand,
        only_model=args.model,
    )

    log(f"Models discovered: {len(universe)}")

    for payload in universe.values():
        try:
            process_model(payload)

        except Exception:
            traceback.print_exc()

        time.sleep(1)

    log("FULL RUN COMPLETE")


if __name__ == "__main__":
    main()