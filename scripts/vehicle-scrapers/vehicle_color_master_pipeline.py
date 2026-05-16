# ============================================================
# vehicle_color_master_pipeline.py
# ============================================================
#
# One-file ACI vehicle color media pipeline.
#
# What this version does:
#   1. Uses prices_collection colors as the active source of truth.
#   2. Discovers Cardekho model colors from JSON-LD.
#   3. Avoids one-request-per-color as the primary strategy.
#   4. Fetches model page + colors page + ONE seed color page to build
#      the full Cardekho color image catalog for that model.
#   5. Matches active colors to catalog images by filename/color slug.
#   6. Extracts official Cardekho display / hero image separately.
#   7. Normalizes and frames the display / hero image as well.
#   8. Prefers higher quality 930x620 image URLs and strips ?tr
#      transformation params for cleaner source URLs.
#   9. Extracts hex codes from Cardekho image filenames.
#  10. Reuses unchanged Mongo assets without re-normalizing/re-uploading.
#  11. Normalizes/uploads only changed or missing images unless --force is used.
#  12. Performs one final atomic Mongo replace per model.
#  13. Does NOT write partial model results by default.
#
# Run one model:
#   python3 vehicle_color_master_pipeline.py --brand kia --model seltos
#
# Debug without upload/Mongo:
#   python3 vehicle_color_master_pipeline.py --brand kia --model seltos --skip-upload --skip-mongo --force
#
# Force regeneration:
#   python3 vehicle_color_master_pipeline.py --brand kia --model seltos --force
#
# Allow partial Mongo write only when you explicitly want it:
#   python3 vehicle_color_master_pipeline.py --brand kia --model seltos --allow-partial
#
# ============================================================

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse, urlunparse

import cv2
import numpy as np
import requests
from bs4 import BeautifulSoup
from PIL import Image
from rapidfuzz import fuzz
from rembg import remove

from normalize_car_image_rembg import process_single_image
from mongo_connection import db, prices_collection


# ============================================================
# CONFIG
# ============================================================

COLLECTION_NAME = "vehicle_colors_v2"
vehicle_colors_collection = db[COLLECTION_NAME]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.join(BASE_DIR, "tmp")
NORMALIZED_DIR = os.path.join(BASE_DIR, "normalized")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(NORMALIZED_DIR, exist_ok=True)

# Matches your earlier working rclone target.
R2_REMOTE = "r2:cdrive-car-images/media/car-images/normalized"

# Public R2 URL base currently being used instead of cdn.acillp.com.
R2_BASE_URL = (
    "https://pub-8504a10fc1c04f02ac8760cb90462ae3.r2.dev/"
    "media/car-images/normalized"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

FUZZ_THRESHOLD = 88
PIPELINE_VERSION = 9
REQUEST_SLEEP_SECONDS = 0.15

# Match the existing ACI image normalizer technology.
# This calls normalize_car_image_rembg.process_single_image() instead of
# doing a direct rembg.remove() inside this file.
NORMALIZER_MODE = "auto"
NORMALIZER_MODEL = "isnet-general-use"
NORMALIZER_MAX_WIDTH = 2200
NORMALIZER_CANVAS_RATIO = "16:9"
NORMALIZER_PREVIEW = False
NORMALIZER_KEEP_RAW = False
NORMALIZER_ALLOW_FALLBACK_CUTOUT = True


# ============================================================
# LOGGING / BASIC HELPERS
# ============================================================

def utc_now():
    return datetime.now(UTC)


def log(message):
    print(f"[{utc_now().isoformat()}] {message}")


def normalize_spaces(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_key(value):
    value = normalize_spaces(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_spaces(value)


def compact_key(value):
    return normalize_key(value).replace(" ", "")


def slugify(value):
    value = normalize_spaces(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def sha1(text):
    return hashlib.sha1(str(text or "").encode("utf-8")).hexdigest()


def smart_title(value):
    tokens = normalize_key(value).split()
    keep_upper = {
        "amg", "ev", "gt", "gts", "suv", "cs", "dt", "n", "xuv", "eqs",
        "gle", "glc", "gla", "gls", "cla", "cle", "xl6", "q5", "q7",
    }
    out = []
    for token in tokens:
        if token in keep_upper or re.search(r"[0-9]", token):
            out.append(token.upper())
        else:
            out.append(token.capitalize())
    return " ".join(out)


def strip_redundant_brand_from_model(model, brand):
    """
    Fix canonical universe rows where model_normalized already includes brand.

    Examples:
      Bmw + Bmw M440I -> M440I
      Citroen + Citroen C3 -> C3
      Tata + Tata Nexon -> Nexon
      Mercedes Benz + Mercedes Benz E Class -> E Class
      Mercedes + Mercedes Benz C Class -> C Class
    """
    model_key = normalize_key(model)
    brand_key = normalize_key(brand)

    aliases = {brand_key}

    if "mercedes" in brand_key:
        aliases.update({"mercedes", "mercedes benz", "mercedes-benz"})
    if brand_key == "land rover":
        aliases.update({"land rover", "land-rover"})
    if brand_key == "rolls royce":
        aliases.update({"rolls royce", "rolls-royce"})
    if brand_key == "aston martin":
        aliases.update({"aston martin", "aston-martin"})

    aliases = sorted({normalize_key(a) for a in aliases if a}, key=len, reverse=True)

    for alias in aliases:
        if model_key == alias:
            return normalize_spaces(model)
        if model_key.startswith(alias + " "):
            stripped = model_key[len(alias):].strip()
            return smart_title(stripped) if stripped else normalize_spaces(model)

    return normalize_spaces(model)


def save_debug_html(name, html):
    path = os.path.join(TMP_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def unique_list(items):
    seen = set()
    output = []
    for item in items:
        if not item:
            continue
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


# ============================================================
# COLOR NORMALIZATION / HEX EXTRACTION
# ============================================================

CANONICAL_COLOR_ALIASES = {
    "gravity gray": "Gravity Grey",
    "gravitygrey": "Gravity Grey",
    "gravity grey": "Gravity Grey",
    "aurora black": "Aurora Black Pearl",
    "aurora black pearl": "Aurora Black Pearl",
    "glacier white": "Glacier White Pearl",
    "glacier white pearl": "Glacier White Pearl",
    "ivory silver": "Ivory Silver Gloss",
    "ivory silver gloss": "Ivory Silver Gloss",
    "pewterolive": "Pewter Olive",
    "pewter olive": "Pewter Olive",
    "matte graphite": "Matte Graphite",
    "magma red": "Magma Red",
    "magma red with aurora black": "Magma Red With Aurora Black",
    "frost blue": "Frost Blue",
    "morning haze": "Morning Haze",
    "imperial blue": "Imperial Blue",
    "glacier white pearl with aurora black": "Glacier White Pearl With Aurora Black",
}

COLOR_SUFFIX_WORDS = [
    "pearl",
    "gloss",
    "metallic",
]

# Matching-only equivalents. These do NOT rename the final active color;
# they only help us find the correct Cardekho filename when OEM/site names differ.
COLOR_EQUIVALENCE_RULES = [
    ("abyss black", "titanium black"),
    ("titanium black", "abyss black"),
    ("abyss black matte", "titanium black matte"),
    ("titanium black matte", "abyss black matte"),
    ("black matte", "titanium black matte"),
    ("atlas white dual tone", "atlas white with titanium black"),
    ("atlas white with black roof", "atlas white with titanium black"),
    ("atlas white with abyss black", "atlas white with titanium black"),
    ("atlas white with titanium black", "atlas white dual tone"),
    ("shadow grey with black roof", "titan grey matte"),
    ("shadow grey with black roof", "titan grey"),
    ("shadow grey", "titan grey"),
    ("titan grey", "shadow grey"),
]


def equivalent_color_names(color_name):
    base = normalize_color_name(color_name)
    if not base:
        return []

    options = [base]
    key = normalize_key(base)

    for source, target in COLOR_EQUIVALENCE_RULES:
        source_key = normalize_key(source)
        target_key = normalize_key(target)

        if source_key in key:
            options.append(smart_title(key.replace(source_key, target_key)))

    return unique_list(options)


def normalize_color_name(name):
    raw = normalize_spaces(name)
    if not raw:
        return ""

    # Common source typos / OCR-ish variants seen in price/color feeds.
    fixes = {
        r"altas": "atlas",
        r"prisitne": "pristine",
        r"obsedian": "obsidian",
        r"metalic": "metallic",
        r"metallc": "metallic",
        r"poler": "polar",
        r"greay": "grey",
        r"gray": "grey",
        r"red": "red",
    }

    raw_fixed = raw
    for pattern, replacement in fixes.items():
        raw_fixed = re.sub(pattern, replacement, raw_fixed, flags=re.IGNORECASE)

    raw = normalize_spaces(raw_fixed)
    key = compact_key(raw)

    for alias, canonical in CANONICAL_COLOR_ALIASES.items():
        if key == compact_key(alias):
            return canonical

    words = normalize_key(raw).split()
    return " ".join(
        w.upper() if len(w) <= 3 and w.isalpha() else w.capitalize()
        for w in words
    )


def color_slug_variants(color_name):
    """
    Generate likely Cardekho filename/URL color slugs.

    Includes matching-only equivalent names such as:
      Abyss Black -> Titanium Black
      Atlas White Dual Tone -> Atlas White With Titanium Black
      Shadow Grey With Black Roof -> Titan Grey Matte
    """
    variants = []

    for normalized in equivalent_color_names(color_name):
        base = slugify(normalized)
        if base:
            variants.append(base)

        expanded = normalized
        expanded = re.sub(r"dt", "Dual Tone", expanded, flags=re.IGNORECASE)
        expanded = re.sub(r"d t", "Dual Tone", expanded, flags=re.IGNORECASE)
        expanded_slug = slugify(expanded)
        if expanded_slug:
            variants.append(expanded_slug)

        simplified = expanded
        simplified = re.sub(r"with aurora black", "", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"with titanium black", "", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"with abyss black", "", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"with black roof", "", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"black roof", "", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"dual tone", "", simplified, flags=re.IGNORECASE)

        for suffix in COLOR_SUFFIX_WORDS:
            simplified = re.sub(rf"{suffix}", "", simplified, flags=re.IGNORECASE)

        simplified_slug = slugify(simplified)
        if simplified_slug:
            variants.append(simplified_slug)

        for sep in ["-with-", "-and-"]:
            if sep in base:
                core = base.split(sep)[0]
                if core:
                    variants.append(core)

        if "-dual-tone" in expanded_slug:
            core = expanded_slug.replace("-dual-tone", "")
            if core:
                variants.append(core)

    more = []
    for item in variants:
        if "grey" in item:
            more.append(item.replace("grey", "gray"))
        if "gray" in item:
            more.append(item.replace("gray", "grey"))
    variants.extend(more)

    return unique_list(variants)


def color_identity_tokens(color_name):
    """
    Tokens that must identify the color in the image filename/path.
    Prevents unsafe matches like:
      Shadow Grey -> Maruti Splendid Silver
      Abyss Black -> Titanium Black Matte
    """
    stop = {
        "with", "and", "roof", "body", "dual", "tone", "dt", "d", "t",
        "metallic", "pearl", "gloss", "solid", "colour", "color", "paint",
        "edition", "limited", "king", "range", "body", "mono", "tune",
    }
    tokens = []
    for variant in color_slug_variants(color_name):
        for token in variant.split("-"):
            token = token.strip().lower()
            if len(token) < 3 or token in stop:
                continue
            tokens.append(token)

    # Preserve order but dedupe.
    return unique_list(tokens)


def is_dual_tone_name(color_name):
    key = normalize_key(color_name)
    dual_markers = [
        " dual tone",
        " dt",
        " with ",
        " roof",
        "black roof",
        "white roof",
        "dual",
    ]
    return any(marker in f" {key} " for marker in dual_markers)


def is_dual_tone_url(url):
    lower = str(url or "").lower()
    dual_markers = [
        "-with-",
        "-and-",
        "dual-tone",
        "dual-ton",
        "dual-tone",
        "black-roof",
        "white-roof",
        "roof_",
        "-roof",
    ]
    return any(marker in lower for marker in dual_markers)


def image_has_color_identity(url, color_name):
    lower = str(url or "").lower()
    tokens = color_identity_tokens(color_name)

    if not tokens:
        return False

    # Safety: do not map a single-tone active color to a dual-tone image,
    # and do not map a dual-tone active color to a single-tone image,
    # unless one of the matching-only equivalent names has that tone type.
    allowed_dual_states = {
        is_dual_tone_name(name)
        for name in equivalent_color_names(color_name)
    }
    url_is_dual = is_dual_tone_url(url)
    if allowed_dual_states and url_is_dual not in allowed_dual_states:
        return False

    # Exact slug match is strongest.
    for variant in color_slug_variants(color_name):
        if variant and variant in lower:
            return True

    # Require at least one strong identity token. Generic colors are allowed only
    # when they are the main identity, e.g. Black, White, Red.
    generic = {"black", "white", "red", "blue", "grey", "gray", "silver", "green", "orange", "yellow", "brown", "purple", "gold"}
    strong_tokens = [t for t in tokens if t not in generic]

    if strong_tokens:
        return any(t in lower for t in strong_tokens)

    return any(t in lower for t in tokens)


def extract_hex_codes_from_image_url(url):
    """
    Extract Cardekho filename hex codes.

    Examples:
      Frost-Blue_445b6b.jpg
        -> #445b6b

      Magma-Red_420107-and-Aurora-Black-Pearl_0a0a0a.jpg
        -> #420107, #0a0a0a
    """
    path = urlparse(str(url or "")).path
    filename = os.path.basename(path)

    hexes = re.findall(r"_([0-9a-fA-F]{6})(?=\.|-|_)", filename)
    hex_codes = [f"#{h.lower()}" for h in hexes]

    return {
        "hex": hex_codes[0] if hex_codes else None,
        "secondaryHex": hex_codes[1] if len(hex_codes) > 1 else None,
        "hexCodes": hex_codes,
        "isDualTone": len(hex_codes) > 1,
    }


# ============================================================
# ACTIVE UNIVERSE FROM PRICELIST
# ============================================================

def build_active_universe(only_brand=None, only_model=None):
    query = {
        "$or": [
            {"is_discontinued": False},
            {"is_discontinued": {"$exists": False}},
        ]
    }

    projection = {
        "brand": 1,
        "model": 1,
        "model_normalized": 1,
        "colors_normalized": 1,
    }

    docs = list(prices_collection.find(query, projection))
    universe = {}

    for doc in docs:
        brand = normalize_spaces(doc.get("brand"))
        raw_model = normalize_spaces(doc.get("model_normalized") or doc.get("model"))
        model = strip_redundant_brand_from_model(raw_model, brand)

        if not brand or not model:
            continue

        brand_slug = slugify(brand)
        model_slug = slugify(model)

        if only_brand and brand_slug != slugify(only_brand):
            continue
        if only_model and model_slug != slugify(only_model):
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

        for color in doc.get("colors_normalized") or []:
            normalized = normalize_color_name(color)
            if normalized:
                universe[key]["active_colors"].add(normalized)

    return universe


# ============================================================
# HTTP
# ============================================================

def fetch_page(url):
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


# ============================================================
# IMAGE URL EXTRACTION
# ============================================================

def clean_image_url(url, strip_transform=True):
    if not url:
        return ""

    url = unquote(str(url))
    url = url.replace("\\/", "/")
    url = url.replace("&amp;", "&")
    url = url.strip().strip('"').strip("'")

    if strip_transform:
        parsed = urlparse(url)
        # Keep path only. Cardekho ?tr=w-* is a transform, not the source identity.
        url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

    return url


def is_cardekho_image_url(url):
    lower = str(url or "").lower()
    if "stimg.cardekho.com" not in lower:
        return False
    return any(ext in lower for ext in [".jpg", ".jpeg", ".png", ".webp"])


def collect_images_from_json(value, out=None):
    if out is None:
        out = []

    if isinstance(value, dict):
        for child in value.values():
            collect_images_from_json(child, out)
    elif isinstance(value, list):
        for child in value:
            collect_images_from_json(child, out)
    elif isinstance(value, str):
        if is_cardekho_image_url(value):
            out.append(clean_image_url(value))

    return out


def extract_json_ld_objects(soup):
    objects = []
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})

    for script in scripts:
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue

        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        if isinstance(parsed, list):
            objects.extend([obj for obj in parsed if isinstance(obj, dict)])
        elif isinstance(parsed, dict):
            objects.append(parsed)

    return objects


def extract_colors_from_json_ld(soup):
    colors = []
    images = []

    for obj in extract_json_ld_objects(soup):
        raw_colors = obj.get("Color") or obj.get("color") or []
        if isinstance(raw_colors, str):
            raw_colors = [raw_colors]

        for color in raw_colors:
            normalized = normalize_color_name(color)
            if normalized:
                colors.append(normalized)

        images.extend(collect_images_from_json(obj.get("image")))

    return unique_list(colors), unique_list(images)


def extract_images_from_meta(soup):
    images = []

    selectors = [
        ("meta", {"property": "og:image"}),
        ("meta", {"name": "twitter:image"}),
        ("link", {"rel": "preload"}),
    ]

    for tag_name, attrs in selectors:
        for tag in soup.find_all(tag_name, attrs=attrs):
            candidate = tag.get("content") or tag.get("href")
            if is_cardekho_image_url(candidate):
                images.append(clean_image_url(candidate))

    return unique_list(images)


def extract_images_from_raw_html(html):
    patterns = [
        r'https://stimg\.cardekho\.com/[^\s"\'<>\\]+?\.(?:jpg|jpeg|png|webp)(?:\?[^\s"\'<>\\]*)?',
        r'https:\\/\\/stimg\.cardekho\.com\\/[^\s"\'<>]+?\.(?:jpg|jpeg|png|webp)(?:\?[^\s"\'<>]*)?',
    ]

    images = []
    for pattern in patterns:
        for match in re.findall(pattern, html, flags=re.IGNORECASE):
            url = clean_image_url(match)
            if is_cardekho_image_url(url):
                images.append(url)

    return unique_list(images)


def image_quality_score(url):
    lower = str(url or "").lower()
    score = 0

    # Prefer the color configurator image folder.
    if "/images/car-images/" in lower:
        score += 120
    if "/images/carexteriorimages/" in lower:
        score += 80

    # Prefer highest available dimensions found in Cardekho paths.
    if "/930x620/" in lower:
        score += 100
    elif "/630x420/" in lower:
        score += 60
    elif "/300x225/" in lower:
        score += 10
    elif "/200x133/" in lower:
        score -= 20

    # Same angle priority.
    if "front-left-side-47" in lower:
        score += 100
    elif "front-left-side" in lower:
        score += 90
    elif "front-right" in lower:
        score += 30
    elif "front-view" in lower:
        score += 20
    elif "side-view" in lower:
        score += 5

    bad_tokens = [
        "logo",
        "favicon",
        "sprite",
        "pwa/img",
        "emi",
        "placeholder",
        "user",
        "dealer",
        "icon",
        "default",
    ]
    if any(token in lower for token in bad_tokens):
        score -= 500

    return score


def image_color_score(url, color_name):
    lower = str(url or "").lower()
    score = image_quality_score(url)

    # Color images should come from Cardekho's color image folder, not
    # model hero/display exterior images.
    if "/images/car-images/" not in lower:
        score -= 500

    # Reject weak identity by score later, but penalize early too.
    if not image_has_color_identity(url, color_name):
        score -= 350

    # Match all likely color slugs against filename/path.
    for variant in color_slug_variants(color_name):
        if variant and variant in lower:
            score += 180

    # Token-level score helps if filename has hex suffixes.
    tokens = set()
    for variant in color_slug_variants(color_name):
        tokens.update([t for t in variant.split("-") if t])

    for token in tokens:
        if len(token) >= 3 and token in lower:
            score += 35

    # Dual-tone awareness.
    if "with aurora black" in normalize_key(color_name):
        if "aurora-black" in lower or "and-aurora-black" in lower:
            score += 140
    else:
        # Avoid accidentally choosing dual-tone for plain colors when a plain image exists.
        if "and-aurora-black" in lower:
            score -= 80

    return score


def image_display_score(url):
    """
    Pick the official model display / hero image separately from color images.

    Preferred:
      /images/carexteriorimages/930x620/.../front-left-side-47.jpg

    Avoid:
      /images/car-images/.../Color_hex.jpg because those are color-studio images.
    """
    lower = str(url or "").lower()
    score = 0

    if "/images/carexteriorimages/" in lower:
        score += 300
    if "/images/car-images/" in lower:
        score -= 100

    if "/930x620/" in lower:
        score += 100
    elif "/630x420/" in lower:
        score += 60
    elif "/300x225/" in lower:
        score += 10

    if "front-left-side-47" in lower:
        score += 150
    elif "front-left-side" in lower:
        score += 130
    elif "front-right" in lower:
        score += 40
    elif "front-view" in lower:
        score += 30
    elif "side-view" in lower:
        score += 5

    bad_tokens = [
        "logo",
        "favicon",
        "sprite",
        "pwa/img",
        "emi",
        "placeholder",
        "user",
        "dealer",
        "icon",
        "default",
        "interior",
    ]
    if any(token in lower for token in bad_tokens):
        score -= 500

    return score


def url_belongs_to_model(url, brand_slug, model_slug):
    path_key = normalize_key(urlparse(str(url or "")).path)
    brand_tokens = normalize_key(brand_slug).split()
    model_tokens = normalize_key(model_slug).split()

    if not brand_tokens or not model_tokens:
        return True

    # Current model image paths generally contain /Brand/Model/.
    brand_ok = all(token in path_key for token in brand_tokens)
    model_ok = all(token in path_key for token in model_tokens)

    return brand_ok and model_ok


def filter_images_for_model(images, brand_slug, model_slug):
    filtered = [
        img for img in unique_list(images)
        if url_belongs_to_model(img, brand_slug, model_slug)
    ]

    if not filtered:
        log(f"WARNING: no model-scoped images found for {brand_slug}/{model_slug}")

    return filtered


def pick_best_image_for_color(images, color_name, brand_slug=None, model_slug=None, min_score=300):
    candidates = unique_list([img for img in images if is_cardekho_image_url(img)])

    if brand_slug and model_slug:
        candidates = filter_images_for_model(candidates, brand_slug, model_slug)

    # Never use hero/exterior images as color-specific images.
    candidates = [img for img in candidates if "/images/car-images/" in str(img).lower()]

    # Must contain actual color identity tokens.
    candidates = [img for img in candidates if image_has_color_identity(img, color_name)]

    if not candidates:
        log(f"NO SAFE IMAGE CANDIDATE for {color_name}")
        return None

    ranked = sorted(
        candidates,
        key=lambda url: image_color_score(url, color_name),
        reverse=True,
    )

    best = ranked[0]
    best_score = image_color_score(best, color_name)
    log(f"BEST IMAGE for {color_name}: {best_score} | {best}")

    if best_score < min_score:
        log(f"REJECT IMAGE for {color_name}: score below {min_score}")
        return None

    return best


def pick_display_image(images):
    candidates = unique_list([img for img in images if is_cardekho_image_url(img)])
    if not candidates:
        return None

    ranked = sorted(
        candidates,
        key=image_display_score,
        reverse=True,
    )

    best = ranked[0]
    log(f"DISPLAY IMAGE: {image_display_score(best)} | {best}")
    return best


# ============================================================
# CARDEKHO SCRAPING / CATALOG MATCHING
# ============================================================

def fetch_and_extract_page(url, debug_name):
    log(f"Fetching: {url}")
    html = fetch_page(url)
    save_debug_html(debug_name, html)
    soup = BeautifulSoup(html, "html.parser")

    colors, jsonld_images = extract_colors_from_json_ld(soup)
    images = []
    images.extend(jsonld_images)
    images.extend(extract_images_from_meta(soup))
    images.extend(extract_images_from_raw_html(html))

    return colors, unique_list(images), html


def discover_cardekho_model_catalog(brand_slug, model_slug):
    """
    Scalable extraction:
      - Fetch model page and /colors page.
      - Discover colors.
      - Fetch only ONE seed color page to expose the full car-images catalog.
      - Match all colors to images from the catalog.
      - Separately return display candidates for model-level hero image.
    """
    discovered_colors = []
    catalog_images = []
    display_candidates = []

    base_urls = [
        (
            f"https://www.cardekho.com/{brand_slug}/{model_slug}/colors",
            f"{brand_slug}-{model_slug}-colors.html",
        ),
        (
            f"https://www.cardekho.com/{brand_slug}/{model_slug}",
            f"{brand_slug}-{model_slug}-model.html",
        ),
    ]

    for url, debug_name in base_urls:
        try:
            colors, images, _html = fetch_and_extract_page(url, debug_name)
            discovered_colors.extend(colors)
            catalog_images.extend(images)
            display_candidates.extend(images)
            log(f"Colors from {url}: {len(colors)}")
            log(f"Images from {url}: {len(images)}")
        except Exception:
            traceback.print_exc()

        time.sleep(REQUEST_SLEEP_SECONDS)

    discovered_colors = unique_list([normalize_color_name(c) for c in discovered_colors])

    # Seed color page. One color page usually contains the full model color catalog.
    seed_colors = discovered_colors[:]
    if not seed_colors:
        log("No discovered colors for seed page")
        return [], unique_list(catalog_images), unique_list(display_candidates)

    seed_color = seed_colors[0]
    seed_slug_candidates = color_slug_variants(seed_color)

    seed_done = False
    for seed_slug in seed_slug_candidates:
        if seed_done:
            break

        seed_url = f"https://www.cardekho.com/{brand_slug}/{model_slug}/{seed_slug}-color"
        try:
            _colors, images, _html = fetch_and_extract_page(
                seed_url,
                f"{brand_slug}-{model_slug}-seed-{seed_slug}.html",
            )
            catalog_images.extend(images)
            log(f"Seed color catalog images from {seed_url}: {len(images)}")
            seed_done = True
        except Exception:
            traceback.print_exc()

        time.sleep(REQUEST_SLEEP_SECONDS)

    return discovered_colors, unique_list(catalog_images), unique_list(display_candidates)


def scrape_cardekho_gallery(brand_slug, model_slug, active_colors=None):
    discovered_colors, catalog_images, display_candidates = discover_cardekho_model_catalog(
        brand_slug,
        model_slug,
    )

    display_image = pick_display_image(
        filter_images_for_model(display_candidates, brand_slug, model_slug)
    )

    log(f"DISCOVERED CARDEKHO COLORS: {len(discovered_colors)}")
    log(f"CATALOG IMAGES: {len(catalog_images)}")

    # Match both Cardekho-discovered colors and active pricelist colors.
    # This fixes cases where Cardekho's JSON-LD color list is incomplete but
    # the image catalog still contains filenames like Abyss-Black_0a0a0a.jpg.
    candidate_colors = unique_list([
        *[normalize_color_name(c) for c in discovered_colors],
        *[normalize_color_name(c) for c in (active_colors or [])],
    ])

    results = []
    for color in candidate_colors:
        best_image = pick_best_image_for_color(
            catalog_images,
            color,
            brand_slug=brand_slug,
            model_slug=model_slug,
        )
        if best_image:
            hex_meta = extract_hex_codes_from_image_url(best_image)
            results.append({
                "color": normalize_color_name(color),
                "image": best_image,
                "hex": hex_meta["hex"],
                "secondaryHex": hex_meta["secondaryHex"],
                "hexCodes": hex_meta["hexCodes"],
                "isDualTone": hex_meta["isDualTone"],
            })
        else:
            log(f"NO IMAGE FOUND FOR COLOR: {color}")

    deduped_by_color = {}
    for item in results:
        color = normalize_color_name(item.get("color"))
        image = clean_image_url(item.get("image"))
        if color and image and color not in deduped_by_color:
            deduped_by_color[color] = {
                **item,
                "color": color,
                "image": image,
            }

    # Rescue active colors that are known OEM/site aliases of an already
    # discovered Cardekho color. Example:
    #   active: Altas/Atlas White Dual Tone
    #   Cardekho: Atlas White With Titanium Black
    # This does not guess a new image; it only clones a high-confidence
    # equivalent result already found from the same model catalog.
    if active_colors:
        existing_items = list(deduped_by_color.values())
        for active_color in active_colors:
            active_norm = normalize_color_name(active_color)
            if not active_norm or active_norm in deduped_by_color:
                continue

            best_item = None
            best_score = 0
            for existing in existing_items:
                score = color_name_match_score(active_norm, existing.get("color"))
                if score > best_score:
                    best_score = score
                    best_item = existing

            if best_item and best_score >= 100:
                cloned = {
                    **best_item,
                    "color": active_norm,
                    "matchedAliasColor": best_item.get("color"),
                    "aliasMatchScore": best_score,
                }
                deduped_by_color[active_norm] = cloned
                log(
                    f"ALIAS RESCUE: {active_norm} -> "
                    f"{best_item.get('color')} | {best_item.get('image')}"
                )

    deduped = list(deduped_by_color.values())

    log(f"FINAL COLORS: {len(deduped)}")
    for item in deduped:
        log(
            f"SELECTED: {item['color']} | {sha1(item['image'])[:10]} | "
            f"hex={item.get('hex')} | secondaryHex={item.get('secondaryHex')} | {item['image']}"
        )

    return {
        "colors": deduped,
        "displayImageUrl": display_image,
    }


# ============================================================
# COLOR MATCHING AGAINST ACTIVE PRICELIST COLORS
# ============================================================

def color_name_match_score(active, gallery_color):
    active_norm = normalize_color_name(active)
    gallery_norm = normalize_color_name(gallery_color)

    active_key = normalize_key(active_norm)
    gallery_key = normalize_key(gallery_norm)

    active_dual_states = {
        is_dual_tone_name(name)
        for name in equivalent_color_names(active_norm)
    }
    gallery_dual_states = {
        is_dual_tone_name(name)
        for name in equivalent_color_names(gallery_norm)
    }

    if compact_key(active_norm) == compact_key(gallery_norm):
        return 100

    active_variants = set(color_slug_variants(active_norm))
    gallery_variants = set(color_slug_variants(gallery_norm))

    # DT vs Dual Tone, grey vs gray, and OEM rename aliases should win,
    # but only when both sides agree on possible single-tone/dual-tone state.
    if active_variants.intersection(gallery_variants) and active_dual_states.intersection(gallery_dual_states):
        return 100

    score = fuzz.token_sort_ratio(active_key, gallery_key)

    # Prevent plain colors from matching dual-tone names and vice versa.
    if active_dual_states.isdisjoint(gallery_dual_states):
        score = min(score, 84)

    return score


def match_colors(active_colors, gallery):
    matched = []

    for active in sorted(active_colors, key=normalize_key):
        best = None
        best_score = 0

        for item in gallery:
            gallery_color = item.get("color")
            if not gallery_color:
                continue

            score = color_name_match_score(active, gallery_color)

            if score > best_score:
                best_score = score
                best = item

        if best and best_score >= FUZZ_THRESHOLD:
            matched.append({
                "color": normalize_color_name(active),
                "image": clean_image_url(best["image"]),
                "score": best_score,
                "matchedGalleryColor": best.get("color"),
                "hex": best.get("hex"),
                "secondaryHex": best.get("secondaryHex"),
                "hexCodes": best.get("hexCodes") or [],
                "isDualTone": bool(best.get("isDualTone")),
            })
        else:
            log(f"NO COLOR MATCH: {active} | best_score={best_score}")

    return matched


# ============================================================
# DOWNLOAD / NORMALIZE / FRAME
# ============================================================

def download_image(url, output_path):
    response = requests.get(url, headers=HEADERS, timeout=120)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(response.content)


def normalize_car_image(input_path, output_path):
    with open(input_path, "rb") as f:
        input_bytes = f.read()

    output = remove(input_bytes)
    image = Image.open(io.BytesIO(output)).convert("RGBA")

    arr = np.array(image)
    alpha = arr[:, :, 3]
    coords = cv2.findNonZero(alpha)

    if coords is None:
        raise RuntimeError("No foreground found after background removal")

    x, y, w, h = cv2.boundingRect(coords)
    cropped = image.crop((x, y, x + w, y + h))

    canvas_w = 1600
    canvas_h = 1000
    target_w = 1250

    ratio = target_w / cropped.size[0]
    resized = cropped.resize(
        (int(cropped.size[0] * ratio), int(cropped.size[1] * ratio)),
        Image.LANCZOS,
    )

    canvas = Image.new("RGBA", (canvas_w, canvas_h), (255, 255, 255, 0))
    x_pos = (canvas_w - resized.size[0]) // 2
    y_pos = (canvas_h - resized.size[1]) // 2

    canvas.paste(resized, (x_pos, y_pos), resized)
    canvas.save(output_path, "WEBP", quality=92)


def compute_frame_metadata(path):
    img = cv2.imread(path, cv2.IMREAD_UNCHANGED)
    if img is None or len(img.shape) < 3 or img.shape[2] < 4:
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
        "aspect_ratio": round(w / h, 4) if h else None,
        "canvas_width": int(img.shape[1]),
        "canvas_height": int(img.shape[0]),
    }


def upload_to_r2(file_path, skip_upload=False):
    if skip_upload:
        log(f"SKIP UPLOAD: {file_path}")
        return True

    result = subprocess.run(
        [
            "rclone",
            "copy",
            file_path,
            R2_REMOTE,
            "--s3-no-check-bucket",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        log("R2 UPLOAD FAILED")
        if result.stdout:
            log(result.stdout)
        if result.stderr:
            log(result.stderr)
        return False

    return True


def local_path_from_public_url(public_url):
    if not public_url:
        return None
    filename = os.path.basename(urlparse(str(public_url)).path)
    if not filename:
        return None
    return os.path.join(NORMALIZED_DIR, filename)


def upload_normalizer_outputs(result, skip_upload=False):
    uploaded = []

    for key in ["stagedImageUrl", "cleanWebpUrl", "cleanPngUrl"]:
        public_url = result.get(key)
        local_path = local_path_from_public_url(public_url)

        if not local_path or not os.path.exists(local_path):
            continue

        upload_ok = upload_to_r2(local_path, skip_upload=skip_upload)
        if not upload_ok and not skip_upload:
            raise RuntimeError(f"Upload failed for {key}: {local_path}")

        uploaded.append({
            "key": key,
            "localPath": local_path,
            "url": public_url,
        })

    return uploaded


def process_media_asset(
    *,
    brand_slug,
    model_slug,
    asset_slug,
    source_image,
    existing_asset=None,
    force=False,
    skip_upload=False,
):
    """
    Shared media processing for both:
      - display / hero image
      - color images

    IMPORTANT:
      This now uses the same normalizer technology as your original bulk script:
        normalize_car_image_rembg.process_single_image()

    It stores both:
      - stagedImageUrl: studio/background image if produced
      - normalizedImageUrl: clean/cutout WebP if produced, else staged fallback
    """
    source_image = clean_image_url(source_image)
    if not source_image:
        return None

    existing_asset = existing_asset or {}
    source_hash = sha1(source_image)

    slug = f"{brand_slug}-{model_slug}-{slugify(asset_slug)}-{source_hash[:10]}"

    can_reuse_existing = (
        not force
        and existing_asset.get("sourceHash") == source_hash
        and (
            existing_asset.get("normalizedImageUrl")
            or existing_asset.get("stagedImageUrl")
        )
        and existing_asset.get("frameMeta") is not None
    )

    if can_reuse_existing:
        log(f"REUSE EXISTING ASSET: {asset_slug} | {source_hash[:10]}")
        return {
            "sourceImageUrl": source_image,
            "sourceHash": source_hash,
            "stagedImageUrl": existing_asset.get("stagedImageUrl"),
            "normalizedImageUrl": existing_asset.get("normalizedImageUrl") or existing_asset.get("stagedImageUrl"),
            "normalizedImagePngUrl": existing_asset.get("normalizedImagePngUrl"),
            "frameMeta": existing_asset.get("frameMeta") or {},
            "imageProcessingMethod": existing_asset.get("imageProcessingMethod"),
            "imageModeUsed": existing_asset.get("imageModeUsed"),
            "imageQualityWarnings": existing_asset.get("imageQualityWarnings") or [],
            "isStudioBackground": existing_asset.get("isStudioBackground"),
            "imageBackgroundRemoved": existing_asset.get("imageBackgroundRemoved"),
            "reused": True,
        }

    log(f"Processing with ACI normalizer: {asset_slug} | {source_hash[:10]} | {source_image}")

    result = process_single_image(
        input_source=source_image,
        slug=slug,
        out_dir=Path(NORMALIZED_DIR),
        public_url_prefix=R2_BASE_URL,
        mode=NORMALIZER_MODE,
        model=NORMALIZER_MODEL,
        max_width=NORMALIZER_MAX_WIDTH,
        canvas_ratio=NORMALIZER_CANVAS_RATIO,
        force=force,
        preview=NORMALIZER_PREVIEW,
        keep_raw=NORMALIZER_KEEP_RAW,
        allow_fallback_cutout=NORMALIZER_ALLOW_FALLBACK_CUTOUT,
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or f"Normalizer failed for {asset_slug}")

    staged_url = result.get("stagedImageUrl")
    clean_webp_url = result.get("cleanWebpUrl")
    clean_png_url = result.get("cleanPngUrl")

    # For the frontend, prefer transparent/clean WebP if generated; otherwise use staged.
    primary_url = clean_webp_url or staged_url
    primary_local_path = local_path_from_public_url(primary_url)

    if not primary_local_path or not os.path.exists(primary_local_path):
        raise RuntimeError(f"Normalizer did not create expected output for {asset_slug}: {primary_url}")

    frame_meta = compute_frame_metadata(primary_local_path)

    log(f"Uploading normalizer outputs to R2: {asset_slug}")
    uploaded = upload_normalizer_outputs(result, skip_upload=skip_upload)

    background_removed = bool(clean_webp_url or clean_png_url)

    return {
        "sourceImageUrl": source_image,
        "sourceHash": source_hash,
        "stagedImageUrl": staged_url,
        "normalizedImageUrl": primary_url,
        "normalizedImagePngUrl": clean_png_url,
        "frameMeta": frame_meta,
        "imageProcessingMethod": result.get("method"),
        "imageModeUsed": result.get("modeUsed"),
        "imageQualityWarnings": result.get("qualityWarnings") or [],
        "isStudioBackground": result.get("isStudioBackground"),
        "imageBackgroundRemoved": background_removed,
        "uploaded": uploaded,
        "reused": False,
    }


# ============================================================
# MODEL PROCESSING
# ============================================================

def process_model(payload, skip_upload=False, skip_mongo=False, force=False, allow_partial=False):
    if skip_upload and not skip_mongo:
        log(
            "ABORTING: --skip-upload was used without --skip-mongo. "
            "This would write URLs to Mongo without uploading files. "
            "Use --skip-upload --skip-mongo together for dry runs."
        )
        return None

    brand = payload["brand"]
    model = payload["model"]
    brand_slug = payload["brand_slug"]
    model_slug = payload["model_slug"]
    active_colors = payload["active_colors"]

    log("=" * 80)
    log(f"PROCESSING: {brand} {model}")
    log("=" * 80)
    log(f"ACTIVE PRICELIST COLORS: {len(active_colors)}")
    log(f"MONGO TARGET COLLECTION: {COLLECTION_NAME}")

    gallery_payload = scrape_cardekho_gallery(brand_slug, model_slug, active_colors=active_colors)
    gallery = gallery_payload.get("colors") or []
    display_image_url = gallery_payload.get("displayImageUrl")

    log(f"Gallery colors: {len(gallery)}")
    log(f"Display image: {display_image_url}")

    matched = match_colors(active_colors, gallery)
    matched_color_names = {normalize_key(item.get("color")) for item in matched}
    unmatched_active_colors = sorted(
        [color for color in active_colors if normalize_key(color) not in matched_color_names],
        key=normalize_key,
    )

    log(f"Matched colors: {len(matched)}")
    if unmatched_active_colors:
        log(f"UNMATCHED ACTIVE COLORS: {unmatched_active_colors}")

    if not matched:
        log("NO MATCHES")
        return None

    existing_doc = vehicle_colors_collection.find_one({
        "brand_slug": brand_slug,
        "model_slug": model_slug,
    }) or {}

    existing_display_asset = {
        "sourceHash": existing_doc.get("displaySourceHash"),
        "stagedImageUrl": existing_doc.get("displayStagedImageUrl"),
        "normalizedImageUrl": existing_doc.get("displayNormalizedImageUrl"),
        "normalizedImagePngUrl": existing_doc.get("displayNormalizedImagePngUrl"),
        "frameMeta": existing_doc.get("displayFrameMeta"),
        "imageProcessingMethod": existing_doc.get("displayImageProcessingMethod"),
        "imageModeUsed": existing_doc.get("displayImageModeUsed"),
        "imageQualityWarnings": existing_doc.get("displayImageQualityWarnings") or [],
        "isStudioBackground": existing_doc.get("displayIsStudioBackground"),
        "imageBackgroundRemoved": existing_doc.get("displayImageBackgroundRemoved"),
    }

    existing_by_color = {
        color_doc.get("name"): color_doc
        for color_doc in existing_doc.get("colors", [])
        if color_doc.get("name")
    }

    display_asset = None
    if display_image_url:
        try:
            display_asset = process_media_asset(
                brand_slug=brand_slug,
                model_slug=model_slug,
                asset_slug="display-hero",
                source_image=display_image_url,
                existing_asset=existing_display_asset,
                force=force,
                skip_upload=skip_upload,
            )
        except Exception:
            log("DISPLAY / HERO IMAGE PROCESSING FAILED")
            traceback.print_exc()

    final_colors = []
    default_color_image_url = None
    default_normalized_image_url = None
    failed_colors = []

    for item in matched:
        color = item["color"]
        source_image = clean_image_url(item["image"])
        source_hash = sha1(source_image)

        if not default_color_image_url:
            default_color_image_url = source_image

        existing_color = existing_by_color.get(color, {})
        existing_color_asset = {
            "sourceHash": existing_color.get("sourceHash"),
            "stagedImageUrl": existing_color.get("stagedImageUrl"),
            "normalizedImageUrl": existing_color.get("normalizedImageUrl"),
            "normalizedImagePngUrl": existing_color.get("normalizedImagePngUrl"),
            "frameMeta": existing_color.get("frameMeta"),
            "imageProcessingMethod": existing_color.get("imageProcessingMethod"),
            "imageModeUsed": existing_color.get("imageModeUsed"),
            "imageQualityWarnings": existing_color.get("imageQualityWarnings") or [],
            "isStudioBackground": existing_color.get("isStudioBackground"),
            "imageBackgroundRemoved": existing_color.get("imageBackgroundRemoved"),
        }

        hex_meta = {
            "hex": item.get("hex"),
            "secondaryHex": item.get("secondaryHex"),
            "hexCodes": item.get("hexCodes") or [],
            "isDualTone": bool(item.get("isDualTone")),
        }

        try:
            asset = process_media_asset(
                brand_slug=brand_slug,
                model_slug=model_slug,
                asset_slug=color,
                source_image=source_image,
                existing_asset=existing_color_asset,
                force=force,
                skip_upload=skip_upload,
            )

            if not asset:
                raise RuntimeError(f"No processed asset returned for {color}")

            if not default_normalized_image_url:
                default_normalized_image_url = asset.get("normalizedImageUrl")

            final_colors.append({
                "name": color,
                "sourceImageUrl": source_image,
                "stagedImageUrl": asset.get("stagedImageUrl"),
                "normalizedImageUrl": asset.get("normalizedImageUrl"),
                "normalizedImagePngUrl": asset.get("normalizedImagePngUrl"),
                "sourceHash": source_hash,
                "frameMeta": asset.get("frameMeta") or {},
                "imageProcessingMethod": asset.get("imageProcessingMethod"),
                "imageModeUsed": asset.get("imageModeUsed"),
                "imageQualityWarnings": asset.get("imageQualityWarnings") or [],
                "isStudioBackground": asset.get("isStudioBackground"),
                "imageBackgroundRemoved": asset.get("imageBackgroundRemoved"),
                "matchScore": item.get("score"),
                "matchedGalleryColor": item.get("matchedGalleryColor"),
                "hex": hex_meta["hex"],
                "secondaryHex": hex_meta["secondaryHex"],
                "hexCodes": hex_meta["hexCodes"],
                "isDualTone": hex_meta["isDualTone"],
                "updatedAt": utc_now(),
            })

        except Exception:
            failed_colors.append(color)
            traceback.print_exc()

    if failed_colors:
        log(f"FAILED COLORS: {failed_colors}")

    if not final_colors:
        log("NO VALID COLORS")
        return None

    if len(final_colors) < len(matched) and not allow_partial:
        log(
            "ABORTING MONGO WRITE: partial model result. "
            f"final_colors={len(final_colors)} matched={len(matched)}. "
            "Use --allow-partial only if you intentionally want a partial write."
        )
        return None

    display_staged_url = display_asset.get("stagedImageUrl") if display_asset else None
    display_normalized_url = display_asset.get("normalizedImageUrl") if display_asset else None
    display_normalized_png_url = display_asset.get("normalizedImagePngUrl") if display_asset else None
    display_frame_meta = display_asset.get("frameMeta") if display_asset else None
    display_source_hash = display_asset.get("sourceHash") if display_asset else None
    display_processing_method = display_asset.get("imageProcessingMethod") if display_asset else None
    display_mode_used = display_asset.get("imageModeUsed") if display_asset else None
    display_quality_warnings = display_asset.get("imageQualityWarnings") if display_asset else []
    display_is_studio_background = display_asset.get("isStudioBackground") if display_asset else None
    display_background_removed = display_asset.get("imageBackgroundRemoved") if display_asset else None

    # Use normalized hero first. If display normalization fails, fallback to first normalized color.
    hero_image_url = display_normalized_url or default_normalized_image_url or display_image_url or default_color_image_url

    final_document = {
        "brand": brand,
        "model": model,
        "brand_slug": brand_slug,
        "model_slug": model_slug,

        # Raw official Cardekho display / hero source.
        "displayImageUrl": display_image_url,
        "displaySourceHash": display_source_hash,

        # Normalized / R2 display hero asset.
        "displayStagedImageUrl": display_staged_url,
        "displayNormalizedImageUrl": display_normalized_url,
        "displayNormalizedImagePngUrl": display_normalized_png_url,
        "displayFrameMeta": display_frame_meta or {},
        "displayImageProcessingMethod": display_processing_method,
        "displayImageModeUsed": display_mode_used,
        "displayImageQualityWarnings": display_quality_warnings,
        "displayIsStudioBackground": display_is_studio_background,
        "displayImageBackgroundRemoved": display_background_removed,

        # Frontend hero aliases.
        "heroImageUrl": hero_image_url,
        "heroImage": hero_image_url,

        # First matched color fallback.
        "defaultColorImageUrl": default_color_image_url,
        "defaultNormalizedImageUrl": default_normalized_image_url,

        "colors": sorted(final_colors, key=lambda x: normalize_key(x["name"])),
        "activeColorCount": len(final_colors),
        "expectedActiveColorCount": len(active_colors),
        "matchedActiveColorCount": len(matched),
        "unmatchedActiveColors": unmatched_active_colors,
        "isColorCoverageComplete": len(unmatched_active_colors) == 0,
        "pipelineVersion": PIPELINE_VERSION,
        "source": "cardekho",
        "updatedAt": utc_now(),
    }

    if skip_mongo:
        log("SKIP MONGO REPLACE")
        return final_document

    log(f"FINAL MONGO REPLACE -> {COLLECTION_NAME} / {brand_slug}-{model_slug}")
    result = vehicle_colors_collection.replace_one(
        {
            "brand_slug": brand_slug,
            "model_slug": model_slug,
        },
        final_document,
        upsert=True,
    )

    log(
        "MONGO WRITE RESULT: "
        f"matched={result.matched_count}, "
        f"modified={result.modified_count}, "
        f"upserted_id={result.upserted_id}"
    )
    log("DONE")
    return final_document


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, default=None)
    parser.add_argument("--model", type=str, default=None)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-mongo", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")

    args = parser.parse_args()

    universe = build_active_universe(
        only_brand=args.brand,
        only_model=args.model,
    )

    log(f"Models discovered: {len(universe)}")

    if not universe:
        log("No models found. Check --brand/--model slugs and prices_collection data.")
        return

    for payload in universe.values():
        try:
            process_model(
                payload,
                skip_upload=args.skip_upload,
                skip_mongo=args.skip_mongo,
                force=args.force,
                allow_partial=args.allow_partial,
            )
        except Exception:
            traceback.print_exc()

        time.sleep(1)

    log("FULL RUN COMPLETE")


if __name__ == "__main__":
    main()
