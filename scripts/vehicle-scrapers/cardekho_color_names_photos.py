import random
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import colors_collection

start_time = time.time()

BASE_URLS = ["https://www.cardekho.com", "https://cardekho.com"]
WORKING_BASE = BASE_URLS[0]
BASE = "https://www.cardekho.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cardekho.com/",
    "Connection": "keep-alive",
}

SLEEP = 0.15
RETRY_SLEEP = 2
MAX_RETRIES = 4
STRICT_ACTIVE_ONLY = True

TODAY = date.today().isoformat()

session = requests.Session()
session.headers.update(HEADERS)

XML_NS = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
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


def fetch(url, retries=MAX_RETRIES):
    for i in range(retries):
        try:
            r = session.get(url, timeout=30, allow_redirects=True)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(RETRY_SLEEP * (i + 1))
    return None


def fetch_sitemap_index():
    sitemap_paths = ["/sitemap.xml", "/sitemap_index.xml"]
    attempts = []

    for base in BASE_URLS:
        for path in sitemap_paths:
            url = f"{base}{path}"
            resp = fetch(url, retries=3)
            if not resp:
                attempts.append(f"{url} -> no response")
                continue

            head = (resp.text or "")[:2000].lower()
            if "<sitemapindex" in head or "<urlset" in head:
                return resp, base

            attempts.append(f"{url} -> status 200 but non-xml payload")

    msg = "Failed to fetch Cardekho sitemap index. Attempts: " + " | ".join(attempts)
    raise RuntimeError(msg)


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def title_like_slug(slug):
    words = [w for w in str(slug or "").split("-") if w]
    return " ".join(w.upper() if len(w) <= 3 and w.isalpha() else w.title() for w in words)


def clean_color_name(raw, brand_slug="", model_slug=""):
    if not raw:
        return ""
    txt = normalize_whitespace(str(raw).replace("-", " ").replace("_", " ")).strip()
    txt = re.sub(r"^\d+", "", txt).strip()
    txt = re.sub(r"^[^a-zA-Z]+", "", txt).strip()
    txt_low = txt.lower()

    # Remove brand/model noise if present.
    for noise in [brand_slug.replace("-", " "), model_slug.replace("-", " ")]:
        noise = noise.strip().lower()
        if noise:
            txt_low = txt_low.replace(noise, " ")

    tokens = [t for t in re.split(r"\s+", txt_low) if t and t not in BAD_NAME_TOKENS and not t.isdigit()]
    if not tokens:
        return ""

    out = " ".join(tokens)
    out = normalize_whitespace(out)
    return out.title()


def resolution_score(url):
    normalized = canonicalize_image_url(url)
    if "/930x620/" in normalized:
        return 930 * 620
    if "/630x420/" in normalized:
        return 630 * 420
    if "/360x240/" in normalized:
        return 360 * 240
    if "/large/" in normalized.lower():
        return 500 * 320
    m = RESOLUTION_RE.search(normalized)
    if m:
        return int(m.group(1)) * int(m.group(2))
    return 1


def slug_tokens(value):
    txt = normalize_whitespace(value).lower()
    txt = txt.replace("&", " and ")
    txt = re.sub(r"[^a-z0-9]+", "-", txt).strip("-")
    return [token for token in txt.split("-") if token]


def make_slug_variants(brand_slug):
    token = normalize_whitespace(brand_slug).lower().replace(" ", "-")
    canonical = MAKE_ALIASES.get(token, token)
    variants = {token, canonical}
    for k, v in MAKE_ALIASES.items():
        if v == canonical:
            variants.add(k)
    return [item for item in variants if item]


def model_slug_variants(model_slug):
    token = normalize_whitespace(model_slug).lower().replace(" ", "-")
    if not token:
        return []
    return [token, token.replace("-", "")]


def canonicalize_image_url(raw_url, preferred_size="930x620"):
    base = str(raw_url or "").split("?", 1)[0].strip()
    if not base:
        return ""
    if "cardekho.com" not in base.lower():
        return base
    return MEDIA_SIZE_RE.sub(lambda m: f"/images/{m.group(1)}/{preferred_size}/", base)


def url_matches_scope(url, brand_slug, model_slug):
    normalized = canonicalize_image_url(url)
    if not normalized:
        return False

    path = unquote(urlparse(normalized).path).lower()
    normalized_path = re.sub(r"[^a-z0-9]+", "-", path)

    make_variants = make_slug_variants(brand_slug)
    model_variants = model_slug_variants(model_slug)
    if not make_variants or not model_variants:
        return False

    has_make = any(
        f"/{make}/" in path
        or f"-{make}-" in normalized_path
        or normalized_path.startswith(f"{make}-")
        for make in make_variants
    )
    has_model = any(
        f"/{model}/" in path
        or f"-{model}-" in normalized_path
        or normalized_path.endswith(f"-{model}")
        for model in model_variants
    )
    return has_make and has_model


def parse_color_from_filename(filename, brand_slug, model_slug):
    base = filename.split("?")[0].rsplit(".", 1)[0]
    out = []

    # Common Cardekho format: name_hex and chained with -and-
    for piece in base.split("-and-"):
        m = HEX_SUFFIX_RE.search(piece)
        if m:
            name = clean_color_name(m.group(1), brand_slug, model_slug)
            if name:
                out.append((name, m.group(2).lower()))

    if out:
        return out

    # Fallback: try generic tokenized name even when hex suffix is missing.
    generic_name = clean_color_name(base, brand_slug, model_slug)
    if generic_name:
        return [(generic_name, None)]

    return []


def extract_active_color_keys(html, brand_slug, model_slug):
    """
    Detect active/available colors from page payload and return normalized lowercase keys.
    If empty, caller should skip filtering.
    """
    text = (html or "").replace("\\/", "/")
    active = set()

    # JSON payload patterns where color has explicit active status.
    patterns = [
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"(?:isActive|active|isSelected)"\s*:\s*true',
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"isDiscontinued"\s*:\s*false',
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"status"\s*:\s*"active"',
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,280}?"availability"\s*:\s*"available"',
    ]

    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            name = clean_color_name(m.group(1), brand_slug, model_slug)
            if name:
                active.add(name.lower())

    # HTML/data-attribute fallback.
    html_patterns = [
        r'data-color-name="([^"]+)"[^>]{0,200}class="[^"]*active[^"]*"',
        r'data-colour-name="([^"]+)"[^>]{0,200}class="[^"]*active[^"]*"',
    ]
    for pat in html_patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            name = clean_color_name(m.group(1), brand_slug, model_slug)
            if name:
                active.add(name.lower())

    return active


def parse_model_pair_from_path(path):
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) >= 3 and parts[2] in {"colors", "colours", "colour"}:
        return parts[0], parts[1]

    if len(parts) >= 2:
        leaf = parts[1]
        # Pattern: /brand-model-colors.htm
        if leaf.endswith("-colors.htm") or leaf.endswith("-colour.htm") or leaf.endswith("-colours.htm"):
            combined = parts[0]
            if "-" in combined:
                brand = combined.split("-")[0]
                model = "-".join(combined.split("-")[1:])
                return brand, model
    return None


def discover_models_and_color_pages():
    """
    Primary finder intentionally mirrors the working NCR scraper logic:
    sitemap.xml -> car-model-price-sitemap -> parse URL paths.
    """
    global WORKING_BASE

    # NCR-style: primary host sitemap first.
    sitemap_resp = fetch(f"{BASE}/sitemap.xml", retries=3)

    # Fallbacks only if primary host fails.
    if not sitemap_resp:
        sitemap_resp, resolved_base = fetch_sitemap_index()
        WORKING_BASE = resolved_base
    else:
        WORKING_BASE = BASE

    root = ET.fromstring(sitemap_resp.text)
    sitemaps = [loc.text for loc in root.findall("ns:sitemap/ns:loc", XML_NS)]
    price_maps = [sm for sm in sitemaps if "car-model-price-sitemap" in sm]
    color_maps = [sm for sm in sitemaps if "color" in sm.lower() or "colour" in sm.lower()]

    models = set()
    model_color_pages = {}

    # NCR-scraper equivalent path parsing.
    for sm in price_maps:
        r = fetch(sm)
        if not r:
            continue

        sm_root = ET.fromstring(r.text)
        for loc in sm_root.findall("ns:url/ns:loc", XML_NS):
            url = loc.text
            path = url.replace(WORKING_BASE, "").replace(BASE, "").strip("/")
            parts = path.split("/")

            # pattern: brand/model/price-in-city
            if len(parts) == 3 and parts[2].startswith("price"):
                models.add((parts[0], parts[1]))

            # pattern: brand-model combined
            if len(parts) == 2 and parts[1].startswith("car-price"):
                combined = parts[0]
                if "-" in combined:
                    brand = combined.split("-")[0]
                    model = "-".join(combined.split("-")[1:])
                    models.add((brand, model))

    # Optional enrichment from any color sitemap URLs.
    for sm in color_maps:
        r = fetch(sm)
        if not r:
            continue
        sm_root = ET.fromstring(r.text)

        for loc in sm_root.findall("ns:url/ns:loc", XML_NS):
            url = loc.text
            pair = parse_model_pair_from_path(urlparse(url).path)
            if not pair:
                continue
            brand, model = pair
            models.add((brand, model))
            model_color_pages.setdefault((brand, model), set()).add(url)

    return models, model_color_pages


def candidate_color_pages(brand, model, discovered_pages):
    pages = list(discovered_pages.get((brand, model), set()))
    defaults = [
        f"{WORKING_BASE}/{brand}/{model}/colors",
        f"{WORKING_BASE}/{brand}/{model}/colours",
        f"{WORKING_BASE}/{brand}/{model}/colour",
    ]
    for u in defaults:
        if u not in pages:
            pages.append(u)
    return pages


def extract_color_rows_from_html(html, brand_slug, model_slug, source_page):
    rows = []

    normalized_html = html.replace("\\/", "/").replace("&amp;", "&")
    found_urls = IMAGE_RE.findall(normalized_html)

    # Use a set to reduce repeated URLs from same page scripts.
    for raw_url in set(found_urls):
        u = canonicalize_image_url(unquote(raw_url))
        ul = u.lower()

        if "cardekho" not in ul:
            continue
        if not url_matches_scope(u, brand_slug, model_slug):
            continue

        # Restrict to likely color media to avoid unrelated gallery noise.
        likely_color_media = (
            "/color/" in ul
            or "/colors/" in ul
            or bool(HEX_SUFFIX_RE.search(u.rsplit(".", 1)[0]))
        )
        if not likely_color_media:
            continue

        filename = u.split("/")[-1]
        parsed = parse_color_from_filename(filename, brand_slug, model_slug)
        if not parsed:
            continue

        score = resolution_score(u)
        for name, hexcode in parsed:
            rows.append(
                {
                    "brand": title_like_slug(brand_slug),
                    "model": title_like_slug(model_slug),
                    "color_name": name,
                    "hex": hexcode,
                    "image_url": u,
                    "score": score,
                    "key": name.lower(),
                    "source_page": source_page,
                }
            )

    # Optional fallback: name + hex pairs from embedded JSON even if no image matched.
    pair_re = re.compile(
        r'"(?:colorName|colourName)"\s*:\s*"([^\"]+)"[^{}]{0,220}?"(?:hexCode|colorCode|colourCode)"\s*:\s*"#?([0-9a-fA-F]{6})"',
        re.IGNORECASE,
    )
    for m in pair_re.finditer(normalized_html):
        name = clean_color_name(m.group(1), brand_slug, model_slug)
        if not name:
            continue
        rows.append(
            {
                "brand": title_like_slug(brand_slug),
                "model": title_like_slug(model_slug),
                "color_name": name,
                "hex": m.group(2).lower(),
                "image_url": None,
                "score": 0,
                "key": name.lower(),
                "source_page": source_page,
            }
        )

    return rows


print("Fetching model universe from sitemap...")
models, model_color_pages = discover_models_and_color_pages()
models = sorted(models)
print(f"Using base host: {WORKING_BASE}")
print(f"Models discovered: {len(models)}")

existing_docs = list(colors_collection.find({}))
existing_map = {
    (
        str(d.get("brand", "")).strip().lower(),
        str(d.get("model", "")).strip().lower(),
        str(d.get("color_name", "")).strip().lower(),
    ): d
    for d in existing_docs
    if d.get("brand") and d.get("model") and d.get("color_name")
}

models_processed = 0
models_skipped = 0
empty_pages = 0
inactive_dropped = 0

new_colors = 0
updates = 0
unchanged = 0

operations = []

for brand, model in tqdm(models, desc="Scraping colors", unit="model"):
    pages = candidate_color_pages(brand, model, model_color_pages)

    all_rows = []
    fetched_any = False

    for page in pages[:4]:
        r = fetch(page)
        if not r:
            continue
        fetched_any = True

        active_keys = extract_active_color_keys(r.text, brand, model)
        rows = extract_color_rows_from_html(r.text, brand, model, page)

        # Keep active/available colors only.
        if active_keys:
            before = len(rows)
            rows = [row for row in rows if row.get("key") in active_keys]
            inactive_dropped += max(0, before - len(rows))
        elif STRICT_ACTIVE_ONLY:
            # If page does not expose active metadata, skip this page in strict mode
            # to avoid ingesting discontinued colors.
            rows = []

        if rows:
            all_rows.extend(rows)
            # Prefer first successful page to avoid excessive duplicate merges.
            break

    if not fetched_any:
        models_skipped += 1
        continue

    if not all_rows:
        empty_pages += 1
        continue

    models_processed += 1

    df = pd.DataFrame(all_rows)

    best_rows = []
    for _, g in df.groupby("key", dropna=False):
        # Prefer rows with image_url and bigger resolution score.
        g2 = g.copy()
        g2["has_image"] = g2["image_url"].notna().astype(int)
        best_rows.append(g2.sort_values(["has_image", "score"], ascending=False).iloc[0])

    out = pd.DataFrame(best_rows)

    for _, row in out.iterrows():
        brand_name = normalize_whitespace(row["brand"])
        model_name = normalize_whitespace(row["model"])
        color_name = normalize_whitespace(row["color_name"])
        key = (brand_name.lower(), model_name.lower(), color_name.lower())
        existing = existing_map.get(key)

        doc = {
            "brand": brand_name,
            "model": model_name,
            "color_name": color_name,
            "hex": row["hex"],
            "image_url": row["image_url"],
            "source_page": row.get("source_page"),
            "last_updated": TODAY,
            "scrape_timestamp": datetime.now().isoformat(),
        }

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

    time.sleep(SLEEP + random.uniform(0, 0.05))

print("Writing to Mongo...")
if operations:
    colors_collection.bulk_write(operations)



runtime = time.time() - start_time

print("\n===== COLORS RUN SUMMARY =====")
print(f"Models discovered: {len(models)}")
print(f"Models processed: {models_processed}")
print(f"Models skipped (fetch failed): {models_skipped}")
print(f"Empty color pages: {empty_pages}")
print(f"Inactive dropped by filter: {inactive_dropped}")
print(f"New colors: {new_colors}")
print(f"Updates: {updates}")
print(f"Unchanged: {unchanged}")
print(f"Mongo writes: {len(operations)}")
print(f"Runtime: {runtime:.2f} sec")
