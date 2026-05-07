#!/usr/bin/env python3
"""
offers_scrapper.py
------------------
CDrive monthly offers scraper.

Final source priority:
  1. V3Cars current month, if structured breakup exists
  2. CarWale current month, if current total offer exists
  3. Autocar current month model-section article, if exact model section exists
  4. V3Cars previous month fallback only if current month was unavailable everywhere
  5. CarWale Delhi listing fallback for visible current/previous offers missed by model page/V3Cars
     (low confidence; dealer confirmation required)

No Cardekho source is used because it was leaking price values as offers.

Important:
  - Writes only usable offer docs by default.
  - Does not write empty docs unless --write-empty is passed.
  - If zero usable docs are built, Mongo write is skipped.
  - Exports CSV/JSON for audit.
  - Keeps Hyundai special handling:
      * Creta N Line inherits highest Creta V3Cars row because V3Cars Creta May table includes N Line.
      * i20 N Line inherits i20 row with corporate/rural forced to 0.
      * Venue N Line is NOT inherited automatically.
  - Keeps Alcazar fuel-slab repair:
      * Petrol–Manual,Auto → petrol variants only
      * Diesel–Manual,Auto → diesel variants only

Run:
  cd /Users/gauravgrover/cdb-api/scripts/vehicle-scrapers/

  # Dry run all active models
  python3 offers_scrapper.py --dry-run --debug

  # Dry run selected brands
  python3 offers_scrapper.py --brands Hyundai,Honda --dry-run --debug

  # Mongo write selected brands
  python3 offers_scrapper.py --brands Hyundai,Honda --debug

  # Mongo write all active models
  python3 offers_scrapper.py --debug
"""

import argparse
import csv
import html
import json
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import offers_collection
from ncr_universe_utils_v2 import (
    build_ncr_variant_universe,
    normalize_key,
    normalize_spaces,
    normalize_variant_key,
    strip_variant_prefix,
)

TODAY = date.today()
MONTH_NAMES = [
    "",
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

BASE_V3 = "https://www.v3cars.com"
BASE_CARWALE = "https://www.carwale.com"
BASE_AUTOCAR_API = "https://api.autocarindia.com/price"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

NO_OFFER_RE = re.compile(
    r"(offers and discounts are not available right now|not available right now|no offers are available|no offer available|currently no offers)",
    re.I,
)

OFFER_WORDS_RE = re.compile(
    r"\b(offer|offers|discount|discounts|benefit|benefits|cash|exchange|scrappage|corporate|loyalty|rural|upgrade|bonus|scheme)\b",
    re.I,
)

BAD_PRICE_CONTEXT_RE = re.compile(
    r"\b(ex[-\s]?showroom|on[-\s]?road|price starts|price in|emi starts|rto|registration|insurance|fuel cost|mileage|down payment)\b",
    re.I,
)

DISCOUNT_FIELD_MAP = [
    ("cash_discount", re.compile(r"^(cash(?:\s+discount|\s+benefit|\s+offer)?|consumer\s+offer|consumer\s+benefit|special\s+discount)\b", re.I)),
    ("exchange_bonus", re.compile(r"^(exchange(?:\s+bonus|\s+benefit|\s+offer|\s+discount)?)\b", re.I)),
    ("scrappage_bonus", re.compile(r"^(scrappage(?:\s+bonus|\s+benefit|\s+offer)?|scrap(?:\s+bonus|\s+benefit|\s+offer)?)\b", re.I)),
    ("upgrade_bonus", re.compile(r"^(upgrade(?:\s+bonus|\s+benefit|\s+offer)?)\b", re.I)),
    ("corporate_rural", re.compile(r"^(corporate\s*/\s*rural|corporate|rural)(?:\s+discount|\s+benefit|\s+offer)?\b", re.I)),
    ("additional_discount", re.compile(r"^(additional\s+discount|additional\s+benefit|loyalty(?:\s+bonus|\s+benefit)?|free\s+accessor(?:y|ies)|accessor(?:y|ies)|free\s+charging|charging\s+benefit|gov(?:ernment)?\s+customer)\b", re.I)),
    ("finance_offer", re.compile(r"^(finance(?:\s+offer|\s+benefit)?|interest|emi)\b", re.I)),
    ("warranty_offer", re.compile(r"^(warranty|extended\s+warranty)\b", re.I)),
    ("accessories_offer", re.compile(r"^(accessor(?:y|ies)|free\s+accessor(?:y|ies))\b", re.I)),
    ("total", re.compile(r"^(total|maximum|max)\b", re.I)),
]

@dataclass
class OfferRow:
    brand: str
    model: str
    brand_slug: str
    model_slug: str
    source: str
    source_url: str

    target_month: int
    target_year: int
    target_month_label: str

    offer_month: int
    offer_year: int
    offer_month_label: str
    period_type: str  # current / previous / unknown

    source_variant_label: str
    variant_scope: str
    matched_canonical_variants: str
    match_count: int

    cash_discount: int
    exchange_bonus: int
    scrappage_bonus: int
    upgrade_bonus: int
    corporate_discount: int
    rural_offer: int
    additional_discount: int
    finance_offer: str
    warranty_offer: str
    accessories_offer: str

    max_benefit: int
    computed_possible_max: int
    breakup_available: bool
    total_matches_computed: bool

    notes: str
    confidence: str
    dealer_confirmation_required: bool
    raw_block: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CDrive monthly offers scraper")
    parser.add_argument("--brands", type=str, default="", help="Comma-separated brands. Empty means all active brands.")
    parser.add_argument("--brand", type=str, default="", help="Single brand filter, overrides --brands")
    parser.add_argument("--model", type=str, default="", help="Optional model filter")
    parser.add_argument("--target-month", type=int, default=TODAY.month)
    parser.add_argument("--target-year", type=int, default=TODAY.year)
    parser.add_argument("--output-csv", type=str, default="offer_dryrun.csv")
    parser.add_argument("--output-json", type=str, default="offer_dryrun.json")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--debug-dir", type=str, default="")
    parser.add_argument("--include-discontinued", action="store_true")
    parser.add_argument("--limit-models", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write-empty", action="store_true")
    parser.add_argument("--min-offer-docs", type=int, default=1)
    parser.add_argument("--no-inherit-nline", action="store_true")
    parser.add_argument("--skip-autocar", action="store_true")
    parser.add_argument("--skip-carwale", action="store_true")
    parser.add_argument("--skip-carwale-listing", action="store_true", help="Disable last-priority CarWale Delhi listing fallback")
    return parser.parse_args()


def month_label(month: int, year: int) -> str:
    return f"{MONTH_NAMES[month]} {year}"


def previous_month(month: int, year: int) -> Tuple[int, int]:
    return (12, year - 1) if month == 1 else (month - 1, year)


def source_label_from_period(label: str) -> Tuple[int, int]:
    raw = normalize_spaces(label or "").replace(",", " ")
    month_lookup = {name.lower(): idx for idx, name in enumerate(MONTH_NAMES) if name}
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b\s*,?\s*(20\d{2})",
        raw,
        flags=re.I,
    )
    if not m:
        return (0, 0)
    return (month_lookup.get(m.group(1).lower(), 0), int(m.group(2)))


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_text(session: requests.Session, url: str, retries: int = 3) -> Tuple[bool, int, str, str]:
    last_error = ""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=(10, 35), allow_redirects=True)
            if resp.status_code == 200 and resp.text:
                return True, resp.status_code, resp.text, ""
            last_error = f"status={resp.status_code}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep((2 ** attempt) + random.uniform(0.05, 0.18))
    return False, 0, "", last_error


def fetch_json(session: requests.Session, url: str, retries: int = 3) -> Tuple[bool, int, Dict, str]:
    ok, status, text, err = fetch_text(session, url, retries)
    if not ok:
        return False, status, {}, err
    try:
        return True, status, json.loads(text), ""
    except Exception as exc:
        return False, status, {}, f"json parse error: {exc}"


def clean_text(raw_html: str) -> str:
    text = raw_html or ""
    text = text.replace("\\/", "/")
    text = html.unescape(text)
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", text)
    text = re.sub(r"(?i)</?(h1|h2|h3|h4|table|thead|tbody|tr|td|th|li|p|div|br|section|article)[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def short(value: str, limit: int = 900) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def money_to_int(raw: str) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).replace(",", "").replace("₹", "Rs ")
    m = re.search(r"(?:rs\.?|inr)?\s*([\d]+(?:\.\d+)?)\s*(?:lakh|lac|lakhs)", text, re.I)
    if m:
        try:
            return int(round(float(m.group(1)) * 100000))
        except Exception:
            return None
    m = re.search(r"(?:rs\.?|inr)\s*([\d]+)", text, re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def first_money(text: str) -> Optional[int]:
    if "Rs. 0" in text or "Rs 0" in text or "₹0" in text:
        return 0
    patterns = [
        r"(?:₹|rs\.?|inr)\s*[\d,]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)?",
        r"[\d]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            return money_to_int(m.group(0))
    return None


def all_money_mentions(text: str) -> List[int]:
    values = []
    raw = str(text or "").replace(",", "").replace("₹", "Rs ")
    patterns = [
        r"(?:rs\.?|inr)?\s*[\d]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)",
        r"(?:rs\.?|inr)\s*[\d]+",
    ]
    for pat in patterns:
        for m in re.finditer(pat, raw, flags=re.I):
            val = money_to_int(m.group(0))
            if val and val >= 1000:
                values.append(val)
    out = []
    seen = set()
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def amount_and_note_after_field(line: str, field_regex: re.Pattern) -> Tuple[int, str]:
    rest = field_regex.sub("", line, count=1).strip()
    amt = first_money(rest)
    if amt is None:
        amt = 0
    note = re.sub(r"(?:₹|rs\.?|inr)\s*[\d,]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)?", " ", rest, count=1, flags=re.I)
    note = note.replace("-", " ")
    note = normalize_spaces(note)
    return int(amt), note



def normalize_corporate_rural(value: int, note: str, line: str = "") -> Tuple[int, int, str]:
    """
    Parse V3 corporate/rural rows safely.

    Examples:
      Corporate/Rural Rs. 5,000 Only for Rural Customers
        -> corporate=0, rural=5000

      Corporate/Rural Rs. 5,000 Only 3000 for Corporate/Gov Customers
        -> corporate=3000, rural=5000

      Corporate Rs. 35,000
        -> corporate=35000, rural=0

      Rural Rs. 5,000
        -> corporate=0, rural=5000
    """
    line_low = normalize_spaces(line or "").lower()
    note_text = note or ""

    if line_low.startswith("corporate ") and "corporate/rural" not in line_low:
        return (value or 0, 0, note_text)

    if line_low.startswith("rural ") and "corporate/rural" not in line_low:
        return (0, value or 0, note_text)

    corporate = 0
    rural = 0

    if "rural" in note_text.lower():
        rural = value or 0
    elif "corporate" in note_text.lower() or "gov" in note_text.lower() or "government" in note_text.lower():
        corporate = value or 0
    else:
        # Corporate/Rural bucket with no split note: keep both as possible eligibility buckets.
        corporate = value or 0
        rural = value or 0

    corp_match = re.search(
        r"(?:only\s*)?(?:rs\.?|₹)?\s*([\d,]+)\s*(?:for\s*)?(?:corporate|gov|government)",
        note_text,
        re.I,
    )
    if corp_match:
        try:
            corporate = int(corp_match.group(1).replace(",", ""))
        except Exception:
            corporate = corporate or 0

    return corporate, rural, note_text



def split_period_sections(text: str) -> Dict[str, str]:
    """
    V3Cars pages contain unrelated text like "Don't miss offers for May 2026".
    Only start sections from offer headings:
      - "... Discount Offers - May 2026"
      - "... DEALS — MAY 2026"

    This prevents old/previous rows from being treated as current month rows.
    """
    if not text:
        return {}

    lines = [normalize_spaces(x) for x in text.splitlines()]
    lines = [x for x in lines if x]
    month_re = re.compile(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d{2}",
        re.I,
    )

    starts: List[Tuple[int, str]] = []
    for i, line in enumerate(lines):
        m = month_re.search(line)
        if not m:
            continue

        label = normalize_spaces(m.group(0)).title()
        context = " ".join(lines[max(0, i - 1): min(len(lines), i + 2)]).lower()

        # Accept real V3 offer headings only.
        if (
            "discount offers" in context
            or "deals" in context
            or "discount type" in context
            or "max discounts" in context
        ):
            starts.append((i, label))

    sections: Dict[str, str] = {}
    for idx, (start_i, label) in enumerate(starts):
        end_i = starts[idx + 1][0] if idx + 1 < len(starts) else min(len(lines), start_i + 120)
        chunk = "\n".join(lines[start_i:end_i])
        # Section must contain an actual discount table signal.
        low = chunk.lower()
        if "discount type" not in low and "cash rs" not in low and "total rs" not in low:
            continue
        sections[label] = normalize_spaces((sections.get(label, "") + "\n" + chunk).strip())

    return sections



def classify_discount_line(line: str) -> Optional[Tuple[str, re.Pattern]]:
    cleaned = normalize_spaces(line)
    low = cleaned.lower()

    # Do not classify continuation note lines like "Rural Customers" or "Corporate/Gov Customers"
    # as a fresh discount row; this was overwriting valid Corporate/Rural Rs. 5,000 rows.
    if not re.search(r"(rs\.?|₹|inr|\d)", cleaned, re.I) and re.search(r"\b(customer|customers|gov|government|corporate|rural)\b", low, re.I):
        return None

    for key, rx in DISCOUNT_FIELD_MAP:
        if rx.search(cleaned):
            # Corporate/Rural/Corporate/Rural rows must contain money.
            if key == "corporate_rural" and not re.search(r"(rs\.?|₹|inr|\d)", cleaned, re.I):
                return None
            return key, rx
    return None


def is_probable_variant_label(line: str, model_display: str, brand_display: str) -> bool:
    line_clean = normalize_spaces(line)
    if not line_clean:
        return False
    low = line_clean.lower()
    bad = [
        "notes", "applicable", "you can only choose", "max possible", "details about",
        "request button", "login", "sign up",
    ]
    if any(b in low for b in bad):
        return False
    if "offers" in low and not any(x in low for x in ["petrol", "diesel", "cng", "hybrid", "electric", "variant", "variants", "n line"]):
        return False
    if classify_discount_line(line_clean):
        return False

    model_key = normalize_key(model_display)
    brand_key = normalize_key(brand_display)
    line_key = normalize_key(line_clean)
    useful_words = [
        "variant", "variants", "petrol", "diesel", "manual", "auto", "automatic",
        "ivt", "dct", "cng", "electric", "hybrid", "all other", "all", "n line",
        "zx", "vx", "sv", "v variant", "ehev", "e hev",
    ]
    return model_key in line_key or brand_key in line_key or any(w in low for w in useful_words)



def clean_source_variant_label(label: str, brand: str, model: str) -> str:
    text = normalize_spaces(label)

    # Fix collapsed labels like:
    # "70,000 Max discounts All Auto Variants"
    text = re.sub(r"(?i)^(?:rs\.?|₹)?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?\s+max\s+discounts\s+", "", text).strip()

    fuel_heading = re.search(
        r"(?i)((?:petrol|diesel|cng|hybrid|electric)[\w\s,\-/–()]*?(?:variant|variants|manual|auto|automatic|dct|ivt|mt|at|line)[\w\s,\-/–()]*)",
        text,
    )
    if fuel_heading:
        text = fuel_heading.group(1)

    brand_model_heading = re.search(
        rf"(?i)({re.escape(brand)}\s+{re.escape(model)}[^|\n]*?(?:variant|variants|petrol|diesel|cng|hybrid|electric|n\s*line)[^|\n]*)",
        text,
    )
    if brand_model_heading:
        text = brand_model_heading.group(1)

    text = re.split(r"(?i)\bdiscount\s+type\b|\bdiscount\s+notes\b|\bmax\s+discounts\b", text, maxsplit=1)[0]
    text = normalize_spaces(text)
    cleaned = strip_variant_prefix(text, brand, model)
    return cleaned or text



def match_variant_label_to_canonical(label: str, variant_list: List[str], brand: str, model: str) -> Dict:
    if not variant_list:
        return {"variant_scope": "model_level", "matched_variants": []}

    label_raw = normalize_spaces(label or "")
    label_key = normalize_key(label_raw)
    all_variants = list(variant_list)

    has_petrol = "petrol" in label_key
    has_diesel = "diesel" in label_key
    has_cng = "cng" in label_key
    has_electric = "electric" in label_key or "ev" in label_key
    has_hybrid = "hybrid" in label_key or "ehev" in label_key or "e hev" in label_key

    # Mixed fuel labels like "Petrol–Manual,Auto/CNG–Manual" must match both groups.
    if sum([has_petrol, has_diesel, has_cng, has_electric, has_hybrid]) > 1:
        matched = []
        for v in all_variants:
            vk = normalize_key(v)
            include = False
            if has_diesel and "diesel" in vk:
                include = True
            if has_cng and "cng" in vk:
                include = True
            if has_electric and ("electric" in vk or "ev" in vk):
                include = True
            if has_hybrid and ("hybrid" in vk or "ehev" in vk or "e hev" in vk):
                include = True
            if has_petrol and not any(x in vk for x in ["diesel", "cng", "electric", "hybrid", "ev"]):
                include = True
            if include:
                matched.append(v)
        return {"variant_scope": "mixed_fuel_group", "matched_variants": matched}

    # Fuel-specific groups first. Do this BEFORE generic all variants.
    if has_diesel:
        matched = [v for v in all_variants if "diesel" in normalize_key(v)]
        return {"variant_scope": "diesel_group", "matched_variants": matched}

    if has_cng:
        matched = [v for v in all_variants if "cng" in normalize_key(v)]
        return {"variant_scope": "cng_group", "matched_variants": matched}

    if has_electric:
        matched = [v for v in all_variants if "electric" in normalize_key(v) or "ev" in normalize_key(v)]
        return {"variant_scope": "electric_group", "matched_variants": matched}

    if has_hybrid:
        matched = [v for v in all_variants if "hybrid" in normalize_key(v) or "ehev" in normalize_key(v) or "e hev" in normalize_key(v)]
        return {"variant_scope": "hybrid_group", "matched_variants": matched}

    if has_petrol:
        matched = [
            v for v in all_variants
            if "diesel" not in normalize_key(v)
            and "cng" not in normalize_key(v)
            and "electric" not in normalize_key(v)
            and "hybrid" not in normalize_key(v)
            and "ev" not in normalize_key(v)
        ]
        return {"variant_scope": "petrol_group", "matched_variants": matched}

    # Transmission-only groups.
    if "manual" in label_key and "auto" not in label_key and "automatic" not in label_key:
        matched = [
            v for v in all_variants
            if not any(x in normalize_key(v) for x in ["amt", "automatic", " at", " dct", " ivt", "cvt"])
        ]
        return {"variant_scope": "manual_group", "matched_variants": matched}

    if "auto" in label_key or "automatic" in label_key:
        matched = [
            v for v in all_variants
            if any(x in normalize_key(v) for x in ["amt", "automatic", " at", "dct", "ivt", "cvt"])
        ]
        return {"variant_scope": "automatic_group", "matched_variants": matched}

    if not label_key or "all variant" in label_key or "all other variant" in label_key or label_key == "all":
        return {"variant_scope": "all_variants", "matched_variants": all_variants}

    matched = []
    label_tokens = set(label_key.split())
    for v in all_variants:
        clean_v = normalize_variant_key(v, brand, model)
        clean_v_tokens = set(clean_v.split())

        if clean_v and (clean_v in label_key or label_key in clean_v):
            matched.append(v)
            continue

        important = [
            t for t in clean_v_tokens
            if len(t) >= 2 and t not in {
                "petrol", "diesel", "cng", "hybrid", "manual", "automatic", "mt", "at",
                "honda", "hyundai", "variant", "variants", "all", "other", "new"
            }
        ]
        if important and any(t in label_tokens for t in important):
            matched.append(v)

    if matched:
        scope = "exact_variant" if len(matched) == 1 else "variant_group"
        return {"variant_scope": scope, "matched_variants": matched}

    return {"variant_scope": "variant_group_unmatched", "matched_variants": []}


def parse_v3_offer_blocks(
    section_text: str,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    period_label: str,
    period_type: str,
    source_url: str,
) -> List[OfferRow]:
    brand = model_entry["brand_display"]
    model = model_entry["model_display"]
    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]
    variant_list = model_entry.get("variant_list") or []

    if not section_text or NO_OFFER_RE.search(section_text):
        return []

    forced = section_text

    field_names = [
        "Cash Discount", "Cash", "Special Discount",
        "Exchange Bonus", "Exchange Discount", "Exchange",
        "Scrappage Bonus", "Scrappage", "Scrap",
        "Upgrade", "Corporate/Rural", "Corporate", "Rural",
        "Loyalty Bonus", "Loyalty", "Free Accessories", "Accessories", "Free Charging",
        "Additional discount", "Additional benefit", "Total"
    ]
    for name in field_names:
        forced = re.sub(rf"\s+({re.escape(name)}\b)", r"\n\1", forced, flags=re.I)

    forced = re.sub(rf"\s+({re.escape(brand)}\s+[A-Z0-9])", r"\n\1", forced, flags=re.I)

    forced = re.sub(
        r"\s+((?:Petrol|Diesel|CNG|Hybrid|Electric)[A-Za-z0-9\s\-–,/()]*?(?:Variant|Variants|Manual|Auto|Automatic|iVT|DCT|MT|AT|Line))\s+",
        r"\n\1\n",
        forced,
        flags=re.I,
    )

    forced = re.sub(
        r"\s+([A-Z0-9][A-Za-z0-9\s\-–,/()]+(?:Variant|Variants|Petrol|Diesel|CNG|Hybrid|Electric|Manual|Auto|Automatic|iVT|DCT))\s+(Discount Type|Cash|Exchange|Scrappage|Total)",
        r"\n\1\n\2",
        forced,
        flags=re.I,
    )

    lines = [normalize_spaces(x) for x in forced.splitlines()]
    lines = [x for x in lines if x]

    rows: List[OfferRow] = []
    current_label = ""
    current: Dict = {}
    row_notes: List[str] = []
    raw_lines: List[str] = []

    def flush_current():
        nonlocal current_label, current, row_notes, raw_lines
        if not current:
            current_label = ""
            row_notes = []
            raw_lines = []
            return

        max_benefit = int(current.get("total", 0) or 0)
        computed = (
            int(current.get("cash_discount", 0) or 0)
            + max(int(current.get("exchange_bonus", 0) or 0), int(current.get("scrappage_bonus", 0) or 0))
            + int(current.get("upgrade_bonus", 0) or 0)
            + max(int(current.get("corporate_discount", 0) or 0), int(current.get("rural_offer", 0) or 0))
            + int(current.get("additional_discount", 0) or 0)
        )
        if max_benefit <= 0:
            max_benefit = computed
        if max_benefit <= 0:
            current_label = ""
            current = {}
            row_notes = []
            raw_lines = []
            return

        offer_month, offer_year = source_label_from_period(period_label)
        label = current_label or "All variants"
        match = match_variant_label_to_canonical(label, variant_list, brand, model)

        rows.append(
            OfferRow(
                brand=brand,
                model=model,
                brand_slug=brand_slug,
                model_slug=model_slug,
                source="v3cars",
                source_url=source_url,
                target_month=target_month,
                target_year=target_year,
                target_month_label=month_label(target_month, target_year),
                offer_month=offer_month,
                offer_year=offer_year,
                offer_month_label=period_label,
                period_type=period_type,
                source_variant_label=label,
                variant_scope=match["variant_scope"],
                matched_canonical_variants=" | ".join(match["matched_variants"]),
                match_count=len(match["matched_variants"]),
                cash_discount=int(current.get("cash_discount", 0) or 0),
                exchange_bonus=int(current.get("exchange_bonus", 0) or 0),
                scrappage_bonus=int(current.get("scrappage_bonus", 0) or 0),
                upgrade_bonus=int(current.get("upgrade_bonus", 0) or 0),
                corporate_discount=int(current.get("corporate_discount", 0) or 0),
                rural_offer=int(current.get("rural_offer", 0) or 0),
                additional_discount=int(current.get("additional_discount", 0) or 0),
                finance_offer=str(current.get("finance_offer", "") or ""),
                warranty_offer=str(current.get("warranty_offer", "") or ""),
                accessories_offer=str(current.get("accessories_offer", "") or ""),
                max_benefit=max_benefit,
                computed_possible_max=computed,
                breakup_available=True,
                total_matches_computed=(max_benefit == computed),
                notes=" | ".join([n for n in row_notes if n]),
                confidence="high",
                dealer_confirmation_required=True,
                raw_block=" | ".join(raw_lines)[:2000],
            )
        )

        current_label = ""
        current = {}
        row_notes = []
        raw_lines = []

    for line in lines:
        line_low = line.lower()

        classified = classify_discount_line(line)
        if classified:
            key, rx = classified
            raw_lines.append(line)

            if not current_label:
                current_label = "All variants"

            if key == "total":
                amt, note = amount_and_note_after_field(line, rx)
                current["total"] = amt
                if note:
                    # Next source heading may be glued after Total; keep note, source-specific repair may fix.
                    row_notes.append(f"Total: {note}")
                flush_current()
                continue

            if key == "corporate_rural":
                # If this is only a continuation note like "Rural Customers", do not overwrite
                # the previous Corporate/Rural amount.
                raw_amt = first_money(rx.sub("", line, count=1).strip())
                if raw_amt is None and (current.get("corporate_discount") or current.get("rural_offer")):
                    row_notes.append(f"Corporate/Rural: {line}")
                    raw_lines.append(line)
                    continue

                amt, note = amount_and_note_after_field(line, rx)
                corp, rural, split_note = normalize_corporate_rural(amt, note, line)
                current["corporate_discount"] = corp
                current["rural_offer"] = rural
                if split_note:
                    row_notes.append(f"Corporate/Rural: {split_note}")
                continue

            if key in {"finance_offer", "warranty_offer", "accessories_offer"}:
                _, note = amount_and_note_after_field(line, rx)
                current[key] = line
                if note:
                    row_notes.append(f"{key}: {note}")
                continue

            amt, note = amount_and_note_after_field(line, rx)
            current[key] = amt
            if note:
                row_notes.append(f"{key}: {note}")
            continue

        if "you can only choose one" in line_low and current:
            row_notes.append(line)
            raw_lines.append(line)
            continue

        if is_probable_variant_label(line, model, brand):
            if current:
                flush_current()
            current_label = clean_source_variant_label(line, brand, model)
            raw_lines = [line]
            row_notes = []
            continue

    if current:
        flush_current()

    deduped = []
    seen = set()
    for row in rows:
        key = (normalize_key(row.source_variant_label), row.max_benefit, row.offer_month_label)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return repair_source_specific_variant_groups(deduped, model_entry)


def repair_source_specific_variant_groups(rows: List[OfferRow], model_entry: Dict) -> List[OfferRow]:
    brand_key = normalize_key(model_entry.get("brand_display") or "")
    model_key = normalize_key(model_entry.get("model_display") or "")
    variant_list = model_entry.get("variant_list") or []

    if brand_key == "hyundai" and model_key == "alcazar" and len(rows) >= 2:
        ambiguous = [
            r for r in rows
            if normalize_key(r.source_variant_label) in {"all variants", "variants", "all variant"}
            or r.source_variant_label.strip() in {"(All Variants)", "All variants", "All Variants", "All Variant"}
            or r.variant_scope == "all_variants"
            or (r.match_count and len(variant_list) and r.match_count >= len(variant_list))
        ]
        if len(ambiguous) >= 2:
            ordered = sorted(rows, key=lambda r: int(r.max_benefit or 0), reverse=True)
            labels = [
                "Petrol–Manual,Auto (All Variants)",
                "Diesel–Manual,Auto (All Variants)",
            ]
            for row, label in zip(ordered[:2], labels):
                match = match_variant_label_to_canonical(
                    label,
                    variant_list,
                    model_entry.get("brand_display") or "",
                    model_entry.get("model_display") or "",
                )
                row.source_variant_label = label
                row.variant_scope = match["variant_scope"]
                row.matched_canonical_variants = " | ".join(match["matched_variants"])
                row.match_count = len(match["matched_variants"])
                extra_note = (
                    "Variant group repaired from V3Cars Alcazar table: "
                    f"{label}. Exchange and scrappage are alternatives."
                )
                row.notes = normalize_spaces(f"{row.notes} | {extra_note}")
            return sorted(rows, key=lambda r: int(r.max_benefit or 0), reverse=True)

    return rows



def v3_url_candidates(brand_slug: str, model_slug: str) -> List[str]:
    """
    V3Cars does not use one Maruti path. It splits:
      maruti-arena-cars
      maruti-nexa-cars

    Try source-native candidates before generic fallback.
    """
    if brand_slug in {"maruti", "maruti-suzuki"}:
        return [
            f"{BASE_V3}/maruti-nexa-cars/{model_slug}/offers-discounts",
            f"{BASE_V3}/maruti-arena-cars/{model_slug}/offers-discounts",
            f"{BASE_V3}/maruti-suzuki-cars/{model_slug}/offers-discounts",
            f"{BASE_V3}/maruti-cars/{model_slug}/offers-discounts",
        ]

    return [f"{BASE_V3}/{brand_slug}-cars/{model_slug}/offers-discounts"]


def v3_url(brand_slug: str, model_slug: str) -> str:
    return v3_url_candidates(brand_slug, model_slug)[0]


def carwale_url(brand_slug: str, model_slug: str) -> str:
    brand_aliases = {
        "maruti": "maruti-suzuki",
        "maruti-suzuki": "maruti-suzuki",
        "kia": "kia",
        "honda": "honda",
        "hyundai": "hyundai",
        "mg": "mg",
    }
    b = brand_aliases.get(brand_slug, brand_slug)
    return f"{BASE_CARWALE}/{b}-cars/{model_slug}/offers/"

def carwale_listing_urls(max_pages: int = 6) -> List[str]:
    """CarWale's Delhi listing sometimes exposes offers that model pages miss."""
    urls = [f"{BASE_CARWALE}/car-discount-offers-in-delhi/"]
    urls.extend(f"{BASE_CARWALE}/car-discount-offers-in-delhi-p{i}/" for i in range(2, max_pages + 1))
    return urls


def extract_stock_year_metadata_from_text(text: str) -> Dict:
    """Detect MY25/MY26/2025 stock labels so old-stock discounts are not shown as blanket model offers."""
    raw = normalize_spaces(text or "")
    low = raw.lower()
    labels = []

    patterns = [
        (r"\bmy\s*25\b|\bmy25\b|\b2025\s+(?:model|stock|models|stocks|units?)\b|\b2025\s+cars?\b", 2025, "MY25"),
        (r"\bmy\s*26\b|\bmy26\b|\b2026\s+(?:model|stock|models|stocks|units?)\b|\b2026\s+cars?\b", 2026, "MY26"),
        (r"\bold\s+stock\b|\boutgoing\s+stock\b|\bunsold\s+stock\b", None, "old_stock"),
    ]

    years = []
    for pat, year, label in patterns:
        if re.search(pat, low, flags=re.I):
            if label not in labels:
                labels.append(label)
            if year and year not in years:
                years.append(year)

    return {
        "stock_year_specific": bool(labels),
        "stock_year_labels": labels,
        "stock_year_label": labels[0] if labels else "",
        "stock_years": years,
        "stock_year": years[0] if years else None,
    }


def merge_stock_year_note(notes: str, raw_text: str) -> str:
    meta = extract_stock_year_metadata_from_text(raw_text)
    if not meta["stock_year_specific"]:
        return notes or ""
    note = f"Stock/model year specific: {', '.join(meta['stock_year_labels'])}. Do not show as blanket model offer."
    return normalize_spaces(f"{notes or ''} | {note}")


def extract_window_around_model(lines: List[str], model_display: str, brand_display: str, window_before: int = 2, window_after: int = 18) -> List[str]:
    model_key = normalize_key(model_display)
    brand_key = normalize_key(brand_display)
    windows = []
    for i, line in enumerate(lines):
        lk = normalize_key(line)
        if model_key in lk or (brand_key in lk and any(part in lk for part in model_key.split())):
            window = "\n".join(lines[max(0, i - window_before): min(len(lines), i + window_after)])
            if OFFER_WORDS_RE.search(window):
                windows.append(window)
    return windows


def parse_discount_breakup_from_text(section: str) -> Dict[str, int]:
    b = {
        "cash_discount": 0,
        "exchange_bonus": 0,
        "scrappage_bonus": 0,
        "upgrade_bonus": 0,
        "corporate_discount": 0,
        "rural_offer": 0,
        "additional_discount": 0,
    }
    patterns = [
        ("cash_discount", r"cash(?:\s+discount|\s+benefit|\s+offer)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("exchange_bonus", r"exchange(?:\s+bonus|\s+benefit|\s+offer|\s+discount)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("scrappage_bonus", r"scrappage(?:\s+bonus|\s+benefit|\s+offer)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("upgrade_bonus", r"upgrade(?:\s+bonus|\s+benefit|\s+offer)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("corporate_discount", r"corporate(?:\s+discount|\s+benefit|\s+offer)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("rural_offer", r"rural(?:\s+discount|\s+benefit|\s+offer)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("additional_discount", r"(?:accessor(?:y|ies)|loyalty|free\s+accessor(?:y|ies)|special\s+benefit)(?:\s+worth|\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
    ]
    for key, pat in patterns:
        for m in re.finditer(pat, section, re.I):
            val = money_to_int(m.group(1)) or 0
            if 0 <= val <= 500000:
                b[key] = max(b[key], val)
    return b


def computed_from_breakup_dict(b: Dict[str, int]) -> int:
    return (
        int(b.get("cash_discount", 0) or 0)
        + max(int(b.get("exchange_bonus", 0) or 0), int(b.get("scrappage_bonus", 0) or 0))
        + int(b.get("upgrade_bonus", 0) or 0)
        + max(int(b.get("corporate_discount", 0) or 0), int(b.get("rural_offer", 0) or 0))
        + int(b.get("additional_discount", 0) or 0)
    )


def autocar_price_api_url(brand_slug: str, model_slug: str) -> str:
    return f"{BASE_AUTOCAR_API}/{brand_slug}/{model_slug}/?city=delhi"



def scrape_v3_period(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    want_previous: bool = False,
    debug_dir: Optional[Path] = None,
) -> Tuple[List[OfferRow], str]:
    target_label = month_label(target_month, target_year)
    prev_month, prev_year = previous_month(target_month, target_year)
    prev_label = month_label(prev_month, prev_year)

    label = prev_label if want_previous else target_label
    period_type = "previous" if want_previous else "current"

    best_reason = f"v3_no_usable_{period_type}_offer"

    for url in v3_url_candidates(model_entry["brand_slug"], model_entry["model_slug"]):
        ok, status, raw_html, err = fetch_text(session, url)
        if not ok:
            best_reason = f"v3_fetch_failed:{err}"
            continue

        text = clean_text(raw_html)
        if debug_dir:
            safe_url_key = re.sub(r"[^a-z0-9]+", "_", url.lower()).strip("_")[-80:]
            (debug_dir / f"v3_{model_entry['brand_slug']}_{model_entry['model_slug']}_{safe_url_key}.txt").write_text(text, encoding="utf-8")

        sections = split_period_sections(text)

        if label in sections:
            rows = parse_v3_offer_blocks(
                sections[label],
                model_entry,
                target_month,
                target_year,
                label,
                period_type,
                url,
            )
            if rows:
                return rows, f"v3_{period_type}"

        # If the page is a wrong Maruti path, it usually has no offer sections.
        if sections:
            best_reason = f"v3_sections_but_no_{period_type}_offer"

    return [], best_reason


def parse_carwale_current(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path] = None,
) -> Tuple[List[OfferRow], str]:
    url = carwale_url(model_entry["brand_slug"], model_entry["model_slug"])
    ok, status, raw_html, err = fetch_text(session, url)
    if not ok:
        return [], f"carwale_fetch_failed:{err}"

    text = clean_text(raw_html)
    if debug_dir:
        (debug_dir / f"carwale_{model_entry['brand_slug']}_{model_entry['model_slug']}.txt").write_text(text, encoding="utf-8")

    target_label = month_label(target_month, target_year)
    target_month_name = MONTH_NAMES[target_month]

    low = text.lower()

    # Reject clearly expired previous-month offer pages.
    expired_patterns = [
        r"april\s+offers.*?expired\s+on\s+30\s+april",
        r"may\s+have\s+expired\s+on\s+30\s+april",
        r"offer\s+may\s+have\s+expired\s+on\s+30\s+april",
    ]
    if any(re.search(p, low, re.S) for p in expired_patterns):
        return [], "carwale_expired_previous_month_offer"

    # Require a current-month heading near the benefit phrase, or a valid-till date in target month.
    month_heading_re = re.compile(rf"{re.escape(model_entry['model_display'])}.*?{target_month_name}\s+Offers", re.I | re.S)
    generic_month_heading_re = re.compile(rf"{target_month_name}\s+Offers", re.I)
    valid_till_re = re.compile(r"Offer\s+Valid\s+Till\s*:\s*([^\n]+)", re.I)
    vt = valid_till_re.search(text)
    valid_till = normalize_spaces(vt.group(1)) if vt else ""

    has_valid_till_target = bool(valid_till and target_month_name.lower() in valid_till.lower() and str(target_year) in valid_till)
    has_target_heading = bool(month_heading_re.search(text) or generic_month_heading_re.search(text))

    if not has_target_heading and not has_valid_till_target:
        return [], "carwale_no_confirmed_current_month_heading_or_validity"

    benefit_patterns = [
        rf"{re.escape(model_entry['model_display'])}.*?{target_month_name}\s+Offers.*?Get\s+Benefits\s+up\s+to\s+(Rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)",
        r"Get\s+Benefits\s+up\s+to\s+(Rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)",
        r"(?:benefits|discounts|offers)\s+(?:of\s+)?up\s+to\s+(Rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)",
    ]

    amount = None
    raw_block = ""
    for pat in benefit_patterns:
        m = re.search(pat, text, re.I | re.S)
        if not m:
            continue

        candidate_block = short(m.group(0), 1200)
        candidate_low = candidate_block.lower()
        # Guard against price/offers list snippets and expired offer snippets.
        if "expired on 30 april" in candidate_low or "april offers" in candidate_low:
            continue

        amount = money_to_int(m.group(1))
        raw_block = candidate_block
        break

    if not amount or amount < 1000 or amount > 500000:
        return [], "carwale_no_safe_current_benefit"

    row = OfferRow(
        brand=model_entry["brand_display"],
        model=model_entry["model_display"],
        brand_slug=model_entry["brand_slug"],
        model_slug=model_entry["model_slug"],
        source="carwale",
        source_url=url,
        target_month=target_month,
        target_year=target_year,
        target_month_label=target_label,
        offer_month=target_month,
        offer_year=target_year,
        offer_month_label=target_label,
        period_type="current",
        source_variant_label="Model-level CarWale offer",
        variant_scope="model_level",
        matched_canonical_variants="",
        match_count=0,
        cash_discount=0,
        exchange_bonus=0,
        scrappage_bonus=0,
        upgrade_bonus=0,
        corporate_discount=0,
        rural_offer=0,
        additional_discount=0,
        finance_offer="",
        warranty_offer="",
        accessories_offer="",
        max_benefit=int(amount),
        computed_possible_max=0,
        breakup_available=False,
        total_matches_computed=False,
        notes=f"CarWale current-month total-only fallback. Valid till: {valid_till}".strip(),
        confidence="medium",
        dealer_confirmation_required=True,
        raw_block=raw_block,
    )
    return [row], "carwale_current_total_only"


def autocar_article_candidates(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
) -> Tuple[List[Tuple[str, str]], str]:
    api_url = autocar_price_api_url(model_entry["brand_slug"], model_entry["model_slug"])
    ok, status, data, err = fetch_json(session, api_url)
    if not ok:
        return [], f"autocar_api_failed:{err}"

    target_label = month_label(target_month, target_year)
    news = ((data.get("data") or {}).get("news") or [])
    candidates = []

    for item in news:
        title = normalize_spaces(item.get("title") or "")
        url = normalize_spaces(item.get("url") or "")
        if not url:
            continue
        hay = f"{title} {url}"
        if not OFFER_WORDS_RE.search(hay):
            continue
        if target_label.lower() not in hay.lower():
            continue
        candidates.append((title, url))

    return candidates[:4], "autocar_candidates"


def extract_autocar_model_section(text: str, brand: str, model: str) -> str:
    """
    Extract only a section around the exact model from a news article.
    Does not use title-only amounts.
    """
    cleaned = clean_text(text)
    lines = [normalize_spaces(x) for x in cleaned.splitlines() if normalize_spaces(x)]

    model_key = normalize_key(model)
    brand_key = normalize_key(brand)

    # Headings like "Honda City May 2026 offers" or "City Hybrid".
    heading_idxs = []
    for i, line in enumerate(lines):
        lk = normalize_key(line)
        has_model = model_key in lk or lk in model_key
        has_brand = brand_key in lk or not brand_key
        has_offer = OFFER_WORDS_RE.search(line)
        if has_model and (has_offer or "offers" in lk or "discount" in lk or "benefit" in lk):
            heading_idxs.append(i)

    if not heading_idxs:
        # Fallback: any exact model mention in article body with offer words nearby.
        for i, line in enumerate(lines):
            window = " ".join(lines[max(0, i - 2): min(len(lines), i + 6)])
            if model_key in normalize_key(window) and OFFER_WORDS_RE.search(window):
                heading_idxs.append(i)
                break

    if not heading_idxs:
        return ""

    start = heading_idxs[0]

    # Stop at next obvious car-section heading.
    end = min(len(lines), start + 18)
    for j in range(start + 1, min(len(lines), start + 30)):
        lk = normalize_key(lines[j])
        if j > start + 2 and OFFER_WORDS_RE.search(lines[j]) and any(word in lk for word in ["honda", "hyundai", "maruti", "tata", "mahindra", "kia", "toyota", "skoda", "volkswagen"]):
            end = j
            break

    section = "\n".join(lines[start:end])
    if model_key not in normalize_key(section):
        return ""

    return section


def parse_autocar_breakup(section: str) -> Dict[str, int]:
    b = {
        "cash_discount": 0,
        "exchange_bonus": 0,
        "scrappage_bonus": 0,
        "upgrade_bonus": 0,
        "corporate_discount": 0,
        "rural_offer": 0,
        "additional_discount": 0,
    }

    patterns = [
        ("cash_discount", r"cash(?:\s+discount|\s+benefit)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("exchange_bonus", r"exchange(?:\s+bonus|\s+benefit)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("scrappage_bonus", r"scrappage(?:\s+bonus|\s+benefit)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("corporate_discount", r"corporate(?:\s+discount|\s+benefit)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
        ("loyalty_bonus", r"loyalty(?:\s+bonus|\s+benefit)?(?:\s+of)?\s+(?:up\s+to\s+)?(rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)"),
    ]

    for key, pat in patterns:
        m = re.search(pat, section, re.I)
        if m:
            val = money_to_int(m.group(1)) or 0
            if key == "loyalty_bonus":
                b["additional_discount"] = max(b["additional_discount"], val)
            else:
                b[key] = max(b[key], val)

    return b


def parse_autocar_current(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path] = None,
) -> Tuple[List[OfferRow], str]:
    candidates, reason = autocar_article_candidates(session, model_entry, target_month, target_year)
    if not candidates:
        return [], reason

    target_label = month_label(target_month, target_year)
    best_row = None

    for title, url in candidates:
        ok, status, html_text, err = fetch_text(session, url)
        if not ok:
            continue
        if debug_dir:
            (debug_dir / f"autocar_{model_entry['brand_slug']}_{model_entry['model_slug']}_{abs(hash(url)) % 9999}.txt").write_text(clean_text(html_text), encoding="utf-8")

        section = extract_autocar_model_section(html_text, model_entry["brand_display"], model_entry["model_display"])
        if not section:
            continue

        if BAD_PRICE_CONTEXT_RE.search(section):
            # Do not allow price paragraphs.
            pass

        amounts = all_money_mentions(section)
        amounts = [a for a in amounts if 1000 <= a <= 500000]
        if not amounts:
            continue

        max_amount = max(amounts)
        breakup = parse_autocar_breakup(section)
        computed = (
            breakup["cash_discount"]
            + max(breakup["exchange_bonus"], breakup["scrappage_bonus"])
            + breakup["upgrade_bonus"]
            + max(breakup["corporate_discount"], breakup["rural_offer"])
            + breakup["additional_discount"]
        )
        has_breakup = computed > 0

        row = OfferRow(
            brand=model_entry["brand_display"],
            model=model_entry["model_display"],
            brand_slug=model_entry["brand_slug"],
            model_slug=model_entry["model_slug"],
            source="autocar",
            source_url=url,
            target_month=target_month,
            target_year=target_year,
            target_month_label=target_label,
            offer_month=target_month,
            offer_year=target_year,
            offer_month_label=target_label,
            period_type="current",
            source_variant_label="Model-section Autocar offer",
            variant_scope="model_level",
            matched_canonical_variants="",
            match_count=0,
            cash_discount=breakup["cash_discount"],
            exchange_bonus=breakup["exchange_bonus"],
            scrappage_bonus=breakup["scrappage_bonus"],
            upgrade_bonus=breakup["upgrade_bonus"],
            corporate_discount=breakup["corporate_discount"],
            rural_offer=breakup["rural_offer"],
            additional_discount=breakup["additional_discount"],
            finance_offer="",
            warranty_offer="",
            accessories_offer="",
            max_benefit=max_amount,
            computed_possible_max=computed,
            breakup_available=has_breakup,
            total_matches_computed=(computed == max_amount if has_breakup else False),
            notes=f"Autocar current-month exact model section. Article title: {title}",
            confidence="medium" if has_breakup else "medium_total_only",
            dealer_confirmation_required=True,
            raw_block=short(section, 2000),
        )

        if best_row is None or row.max_benefit > best_row.max_benefit:
            best_row = row

    if best_row:
        return [best_row], "autocar_current_model_section"

    return [], "autocar_no_exact_model_section"


def parse_carwale_listing_fallback(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path] = None,
) -> Tuple[List[OfferRow], str]:
    """
    Last-priority fallback for models visible on CarWale's Delhi discount listing.

    This is intentionally lower confidence than V3Cars/model-page data because it may
    expose stock-year or expired offers. Use only when all priority sources fail.
    """
    target_label = month_label(target_month, target_year)
    prev_m, prev_y = previous_month(target_month, target_year)
    prev_label = month_label(prev_m, prev_y)

    best_row = None
    best_score = -1
    best_reason = "carwale_listing_no_match"

    for url in carwale_listing_urls(max_pages=7):
        ok, status, raw_html, err = fetch_text(session, url, retries=2)
        if not ok:
            best_reason = f"carwale_listing_fetch_failed:{err}"
            continue

        cleaned = clean_text(raw_html)
        if debug_dir:
            safe_key = re.sub(r"[^a-z0-9]+", "_", url.lower()).strip("_")[-60:]
            (debug_dir / f"carwale_listing_{model_entry['brand_slug']}_{model_entry['model_slug']}_{safe_key}.txt").write_text(cleaned, encoding="utf-8")

        lines = [normalize_spaces(x) for x in cleaned.splitlines() if normalize_spaces(x)]
        windows = extract_window_around_model(lines, model_entry["model_display"], model_entry["brand_display"])
        for section in windows:
            section_low = section.lower()
            if not OFFER_WORDS_RE.search(section):
                continue
            if BAD_PRICE_CONTEXT_RE.search(section) and not re.search(r"discount|benefit|offer|exchange|cash|accessor", section, re.I):
                continue

            is_current = bool(
                re.search(rf"\b{MONTH_NAMES[target_month]}\s+(?:offer|offers|discount|discounts)", section, re.I)
                or re.search(rf"valid\s+till\s*:?\s*31\s+{MONTH_NAMES[target_month]}", section, re.I)
                or re.search(rf"valid\s+till\s*:?\s*31\s+{MONTH_NAMES[target_month]},?\s*{target_year}", section, re.I)
            )
            is_previous = bool(
                re.search(rf"\b{MONTH_NAMES[prev_m]}\s+(?:offer|offers|discount|discounts)", section, re.I)
                or re.search(rf"expired\s+on\s+30\s+{MONTH_NAMES[prev_m]}", section, re.I)
                or re.search(rf"valid\s+till\s*:?\s*30\s+{MONTH_NAMES[prev_m]}", section, re.I)
            )
            if not is_current and not is_previous:
                # Listing page can be date-light; avoid accepting undated rows.
                continue

            amounts = [v for v in all_money_mentions(section) if 1000 <= v <= 500000]
            if not amounts:
                continue

            breakup = parse_discount_breakup_from_text(section)
            computed = computed_from_breakup_dict(breakup)
            max_amount = computed if computed > 0 else max(amounts)
            if not max_amount or max_amount > 500000:
                continue

            period_type = "current" if is_current else "previous"
            offer_m, offer_y, offer_label = (target_month, target_year, target_label) if is_current else (prev_m, prev_y, prev_label)
            stock_meta = extract_stock_year_metadata_from_text(section)
            notes = "CarWale Delhi listing fallback. Lower confidence; dealer confirmation required."
            if is_previous:
                notes += f" Visible listing appears to be {prev_label}/expired fallback."
            if stock_meta["stock_year_specific"]:
                notes += f" Stock/model year specific: {', '.join(stock_meta['stock_year_labels'])}."

            source_variant_label = "CarWale listing offer"
            if stock_meta["stock_year_label"]:
                source_variant_label += f" ({stock_meta['stock_year_label']})"

            row = OfferRow(
                brand=model_entry["brand_display"],
                model=model_entry["model_display"],
                brand_slug=model_entry["brand_slug"],
                model_slug=model_entry["model_slug"],
                source="carwale_listing",
                source_url=url,
                target_month=target_month,
                target_year=target_year,
                target_month_label=target_label,
                offer_month=offer_m,
                offer_year=offer_y,
                offer_month_label=offer_label,
                period_type=period_type,
                source_variant_label=source_variant_label,
                variant_scope="model_level",
                matched_canonical_variants="",
                match_count=0,
                cash_discount=breakup["cash_discount"],
                exchange_bonus=breakup["exchange_bonus"],
                scrappage_bonus=breakup["scrappage_bonus"],
                upgrade_bonus=breakup["upgrade_bonus"],
                corporate_discount=breakup["corporate_discount"],
                rural_offer=breakup["rural_offer"],
                additional_discount=breakup["additional_discount"],
                finance_offer="",
                warranty_offer="",
                accessories_offer="",
                max_benefit=int(max_amount),
                computed_possible_max=int(computed),
                breakup_available=bool(computed),
                total_matches_computed=bool(computed and computed == max_amount),
                notes=notes,
                confidence="low" if is_previous else "medium_low",
                dealer_confirmation_required=True,
                raw_block=short(section, 2000),
            )

            score = (100 if is_current else 50) + max_amount // 1000 + (20 if computed else 0)
            if score > best_score:
                best_score = score
                best_row = row

    if best_row:
        return [best_row], f"carwale_listing_{best_row.period_type}_fallback"
    return [], best_reason


def scrape_model_by_priority(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path],
    skip_carwale: bool = False,
    skip_autocar: bool = False,
    skip_carwale_listing: bool = False,
) -> Tuple[List[OfferRow], str]:
    # 1. V3Cars current
    rows, reason = scrape_v3_period(session, model_entry, target_month, target_year, want_previous=False, debug_dir=debug_dir)
    if rows:
        return rows, reason

    # 2. CarWale current
    if not skip_carwale:
        rows, reason = parse_carwale_current(session, model_entry, target_month, target_year, debug_dir=debug_dir)
        if rows:
            return rows, reason

    # 3. Autocar current model-section
    if not skip_autocar:
        rows, reason = parse_autocar_current(session, model_entry, target_month, target_year, debug_dir=debug_dir)
        if rows:
            return rows, reason

    # 4. V3Cars previous fallback
    rows, reason = scrape_v3_period(session, model_entry, target_month, target_year, want_previous=True, debug_dir=debug_dir)
    if rows:
        return rows, reason

    # 5. CarWale Delhi listing fallback for missed current/previous visible listings
    if not skip_carwale and not skip_carwale_listing:
        rows, reason = parse_carwale_listing_fallback(session, model_entry, target_month, target_year, debug_dir=debug_dir)
        if rows:
            return rows, reason

    return [], "no_usable_offer_in_priority_sources"


def clone_row_for_model(
    source_row: OfferRow,
    target_model_entry: Dict,
    source_label: str,
    confidence: str,
    notes_suffix: str,
    force_corporate_zero: bool = False,
) -> OfferRow:
    variants = target_model_entry.get("variant_list") or []
    cash = source_row.cash_discount
    exchange = source_row.exchange_bonus
    scrappage = source_row.scrappage_bonus
    upgrade = source_row.upgrade_bonus
    corporate = 0 if force_corporate_zero else source_row.corporate_discount
    rural = 0 if force_corporate_zero else source_row.rural_offer
    additional = source_row.additional_discount
    computed = cash + max(exchange, scrappage) + upgrade + max(corporate, rural) + additional
    max_benefit = source_row.max_benefit
    if force_corporate_zero and computed > 0 and computed < max_benefit:
        max_benefit = computed

    return OfferRow(
        brand=target_model_entry["brand_display"],
        model=target_model_entry["model_display"],
        brand_slug=target_model_entry["brand_slug"],
        model_slug=target_model_entry["model_slug"],
        source="v3cars_inherited",
        source_url=source_row.source_url,
        target_month=source_row.target_month,
        target_year=source_row.target_year,
        target_month_label=source_row.target_month_label,
        offer_month=source_row.offer_month,
        offer_year=source_row.offer_year,
        offer_month_label=source_row.offer_month_label,
        period_type=source_row.period_type,
        source_variant_label=source_label,
        variant_scope="inherited_model",
        matched_canonical_variants=" | ".join(variants),
        match_count=len(variants),
        cash_discount=cash,
        exchange_bonus=exchange,
        scrappage_bonus=scrappage,
        upgrade_bonus=upgrade,
        corporate_discount=corporate,
        rural_offer=rural,
        additional_discount=additional,
        finance_offer=source_row.finance_offer,
        warranty_offer=source_row.warranty_offer,
        accessories_offer=source_row.accessories_offer,
        max_benefit=max_benefit,
        computed_possible_max=computed,
        breakup_available=True,
        total_matches_computed=(max_benefit == computed),
        notes=normalize_spaces(f"{source_row.notes} | {notes_suffix}"),
        confidence=confidence,
        dealer_confirmation_required=True,
        raw_block=source_row.raw_block,
    )


def apply_hyundai_nline_inheritance(all_rows: List[OfferRow], model_map: Dict[Tuple[str, str], Dict]) -> List[OfferRow]:
    additions: List[OfferRow] = []
    existing_models = {(normalize_key(r.brand), normalize_key(r.model)) for r in all_rows}

    def get_model(model_name: str) -> Optional[Dict]:
        return model_map.get(("hyundai", normalize_key(model_name)))

    creta_n = get_model("Creta N Line")
    if creta_n and ("hyundai", "creta n line") not in existing_models:
        creta_rows = [
            r for r in all_rows
            if normalize_key(r.brand) == "hyundai"
            and normalize_key(r.model) == "creta"
            and r.source == "v3cars"
            and r.max_benefit > 0
        ]
        if creta_rows:
            best = max(creta_rows, key=lambda r: r.max_benefit)
            additions.append(clone_row_for_model(
                best,
                creta_n,
                "Inherited from Creta highest V3Cars petrol/all-variants row",
                "high_inherited",
                "Inherited because live V3Cars May 2026 Creta table includes N-Line in the high-benefit petrol/all-variants row. Dealer confirmation required.",
                force_corporate_zero=False,
            ))

    i20_n = get_model("I20 N Line")
    if i20_n and ("hyundai", "i20 n line") not in existing_models:
        i20_rows = [
            r for r in all_rows
            if normalize_key(r.brand) == "hyundai"
            and normalize_key(r.model) == "i20"
            and r.source == "v3cars"
            and r.max_benefit > 0
        ]
        preferred = [
            r for r in i20_rows
            if "n line" in normalize_key(r.source_variant_label + " " + r.notes + " " + r.raw_block)
        ] or i20_rows
        if preferred:
            best = max(preferred, key=lambda r: r.max_benefit)
            additions.append(clone_row_for_model(
                best,
                i20_n,
                "Inherited from i20 row; corporate/rural forced to 0 for N Line",
                "medium_inherited",
                "Inherited because i20 V3Cars row references N Line corporate/rural as Rs. 0. Corporate and rural benefits forced to 0. Dealer confirmation required.",
                force_corporate_zero=True,
            ))

    return additions


def format_inr(value: int) -> str:
    try:
        return f"₹{int(value):,}"
    except Exception:
        return "₹0"


def row_stock_year_metadata(row: OfferRow) -> Dict:
    text = " | ".join([
        str(row.source_variant_label or ""),
        str(row.notes or ""),
        str(row.raw_block or ""),
    ])
    return extract_stock_year_metadata_from_text(text)


def row_to_offer_dict(row: OfferRow) -> Dict:
    stock_meta = row_stock_year_metadata(row)
    return {
        "source": row.source,
        "source_url": row.source_url,
        "offer_month": row.offer_month,
        "offer_year": row.offer_year,
        "offer_month_label": row.offer_month_label,
        "period_type": row.period_type,
        "variant_scope": row.variant_scope,
        "source_variant_label": row.source_variant_label,
        "stock_year_specific": stock_meta["stock_year_specific"],
        "stock_year_label": stock_meta["stock_year_label"],
        "stock_year_labels": stock_meta["stock_year_labels"],
        "stock_year": stock_meta["stock_year"],
        "stock_years": stock_meta["stock_years"],
        "matched_canonical_variants": [x for x in str(row.matched_canonical_variants or "").split(" | ") if x],
        "match_count": row.match_count,
        "max_benefit": row.max_benefit,
        "breakup": {
            "cash_discount": row.cash_discount,
            "exchange_bonus": row.exchange_bonus,
            "scrappage_bonus": row.scrappage_bonus,
            "upgrade_bonus": row.upgrade_bonus,
            "corporate_discount": row.corporate_discount,
            "rural_offer": row.rural_offer,
            "additional_discount": row.additional_discount,
            "finance_offer": row.finance_offer,
            "warranty_offer": row.warranty_offer,
            "accessories_offer": row.accessories_offer,
        },
        "computed_possible_max": row.computed_possible_max,
        "breakup_available": row.breakup_available,
        "total_matches_computed": row.total_matches_computed,
        "confidence": row.confidence,
        "dealer_confirmation_required": row.dealer_confirmation_required,
        "notes": row.notes,
        "raw_block": row.raw_block,
    }


def build_model_offer_docs(
    rows: List[OfferRow],
    no_offer: List[Tuple[str, str, str]],
    models: List[Dict],
    target_month: int,
    target_year: int,
    write_empty: bool = False,
) -> List[Dict]:
    grouped: Dict[Tuple[str, str], List[OfferRow]] = {}
    for row in rows:
        grouped.setdefault((normalize_key(row.brand), normalize_key(row.model)), []).append(row)

    model_lookup = {
        (normalize_key(m["brand_display"]), normalize_key(m["model_display"])): m
        for m in models
    }

    docs = []
    now = datetime.now().isoformat()
    today_iso = TODAY.isoformat()

    for key, group_rows in grouped.items():
        model_entry = model_lookup.get(key)
        first = group_rows[0]

        brand_display = model_entry["brand_display"] if model_entry else first.brand
        model_display = model_entry["model_display"] if model_entry else first.model
        brand_slug = model_entry["brand_slug"] if model_entry else first.brand_slug
        model_slug = model_entry["model_slug"] if model_entry else first.model_slug
        variant_list = model_entry.get("variant_list") if model_entry else []

        current_rows = [r for r in group_rows if r.period_type == "current"]
        previous_rows = [r for r in group_rows if r.period_type == "previous"]

        current_month_published = bool(current_rows)
        fallback_used = (not current_month_published) and bool(previous_rows)
        data_status = "current_month" if current_month_published else ("fallback_previous_month" if fallback_used else "no_offer_found")

        max_benefit = max(int(r.max_benefit or 0) for r in group_rows)
        source_priority = {"v3cars": 5, "carwale": 4, "autocar": 3, "v3cars": 5, "v3cars_inherited": 2, "carwale_listing": 1}
        primary = sorted(group_rows, key=lambda r: (source_priority.get(r.source, 0), r.max_benefit), reverse=True)[0]
        source_names = sorted(set(r.source for r in group_rows))

        fallback_offer_period = None
        if fallback_used:
            fb = previous_rows[0]
            fallback_offer_period = {
                "month": fb.offer_month,
                "year": fb.offer_year,
                "month_label": fb.offer_month_label,
                "is_expired": True,
            }

        if current_month_published:
            customer_safe_display = (
                f"{brand_display} {model_display} current offer is up to {format_inr(max_benefit)}. "
                "Exact benefit depends on variant, stock, eligibility and dealer confirmation."
            )
        else:
            customer_safe_display = (
                f"{brand_display} {model_display} current month offer is not published in trusted sources. "
                f"Last published {fallback_offer_period['month_label']} offer was up to {format_inr(max_benefit)}. "
                "Dealer confirmation required."
            )

        docs.append({
            "month": target_month,
            "year": target_year,
            "month_label": month_label(target_month, target_year),
            "brand": brand_display,
            "model": model_display,
            "brand_slug": brand_slug,
            "model_slug": model_slug,
            "source": "+".join(source_names),
            "source_summary": {
                "primary_source": primary.source,
                "source_count": len(source_names),
                "sources": source_names,
            },
            "current_month_published": current_month_published,
            "data_status": data_status,
            "fallback_used": fallback_used,
            "offer_period": {
                "month": target_month,
                "year": target_year,
                "month_label": month_label(target_month, target_year),
                "is_current_month": True,
            },
            "fallback_offer_period": fallback_offer_period,
            "variant_wise_available": any(r.variant_scope not in {"model_level"} for r in group_rows),
            "stock_year_sensitive": any(row_stock_year_metadata(r)["stock_year_specific"] for r in group_rows),
            "stock_year_labels": sorted({label for r in group_rows for label in row_stock_year_metadata(r)["stock_year_labels"]}),
            "canonical_variant_count": len(variant_list or []),
            "has_cash_discount": any(r.cash_discount > 0 for r in group_rows),
            "has_exchange_bonus": any(r.exchange_bonus > 0 for r in group_rows),
            "has_scrappage_bonus": any(r.scrappage_bonus > 0 for r in group_rows),
            "has_corporate_discount": any(r.corporate_discount > 0 for r in group_rows),
            "has_rural_offer": any(r.rural_offer > 0 for r in group_rows),
            "has_finance_offer": any(bool(r.finance_offer) for r in group_rows),
            "total_potential_benefit": max_benefit,
            "customer_safe_display": customer_safe_display,
            "offer_count": len(group_rows),
            "offers": [row_to_offer_dict(r) for r in sorted(group_rows, key=lambda r: r.max_benefit, reverse=True)],
            "dealer_confirmation_required": True,
            "last_updated": today_iso,
            "scraped_at": now,
        })

    if write_empty:
        existing = set(grouped.keys())
        no_offer_map = {(normalize_key(b), normalize_key(m)): reason for b, m, reason in no_offer}
        for m in models:
            key = (normalize_key(m["brand_display"]), normalize_key(m["model_display"]))
            if key in existing:
                continue
            reason = no_offer_map.get(key, "no_usable_offer")
            docs.append({
                "month": target_month,
                "year": target_year,
                "month_label": month_label(target_month, target_year),
                "brand": m["brand_display"],
                "model": m["model_display"],
                "brand_slug": m["brand_slug"],
                "model_slug": m["model_slug"],
                "source": "v3cars+carwale+autocar",
                "current_month_published": False,
                "data_status": "no_offer_found",
                "fallback_used": False,
                "offer_period": {
                    "month": target_month,
                    "year": target_year,
                    "month_label": month_label(target_month, target_year),
                    "is_current_month": True,
                },
                "fallback_offer_period": None,
                "variant_wise_available": False,
                "canonical_variant_count": len(m.get("variant_list") or []),
                "total_potential_benefit": 0,
                "customer_safe_display": f"{m['brand_display']} {m['model_display']} offer not found in trusted sources. Dealer confirmation required.",
                "offer_count": 0,
                "offers": [],
                "no_offer_reason": reason,
                "dealer_confirmation_required": True,
                "last_updated": today_iso,
                "scraped_at": now,
            })

    return docs


def write_offer_docs_to_mongo(docs: List[Dict]) -> Tuple[int, int, int, int]:
    operations = []
    for doc in docs:
        operations.append(UpdateOne(
            {"brand": doc["brand"], "model": doc["model"], "month": doc["month"], "year": doc["year"]},
            {"$set": doc},
            upsert=True,
        ))

    if not operations:
        return (0, 0, 0, 0)

    result = offers_collection.bulk_write(operations, ordered=False)
    return (result.matched_count, result.modified_count, result.upserted_count, len(operations))


def print_model_rows(model_entry: Dict, rows: List[OfferRow], reason: str) -> None:
    brand_model = f"{model_entry['brand_display']} {model_entry['model_display']}"
    if not rows:
        print(f"— {brand_model}: no usable offer ({reason})")
        return

    max_amt = max(r.max_benefit for r in rows)
    print(f"\n✅ {brand_model}: {len(rows)} row(s), max {format_inr(max_amt)}, source={rows[0].source}, period={rows[0].offer_month_label}")

    for idx, r in enumerate(rows, 1):
        print(f"  {idx}. {r.source_variant_label} [{r.variant_scope}]")
        print(f"     Total/Max: {format_inr(r.max_benefit)} | computed: {format_inr(r.computed_possible_max)} | breakup={r.breakup_available} | confidence={r.confidence}")
        if r.breakup_available:
            print(
                "     Breakup: "
                f"Cash {format_inr(r.cash_discount)}, "
                f"Exchange {format_inr(r.exchange_bonus)}, "
                f"Scrappage {format_inr(r.scrappage_bonus)}, "
                f"Upgrade {format_inr(r.upgrade_bonus)}, "
                f"Corporate {format_inr(r.corporate_discount)}, "
                f"Rural {format_inr(r.rural_offer)}, "
                f"Additional {format_inr(r.additional_discount)}"
            )
        if r.match_count:
            sample = r.matched_canonical_variants.split(" | ")[:5]
            more = f" +{r.match_count - len(sample)} more" if r.match_count > len(sample) else ""
            print(f"     Matched variants ({r.match_count}): {', '.join(sample)}{more}")
        if r.notes:
            print(f"     Notes: {r.notes}")


def main() -> None:
    args = parse_args()
    debug_dir = Path(args.debug_dir) if args.debug_dir else None
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)

    if args.brand:
        target_brands = [normalize_key(args.brand)]
    elif args.brands:
        target_brands = [normalize_key(x) for x in args.brands.split(",") if normalize_spaces(x)]
    else:
        target_brands = []

    print("\n===== CDRIVE MONTHLY OFFERS SCRAPER =====")
    print("Source priority: V3Cars current → CarWale current → Autocar current model-section → V3Cars previous fallback → CarWale listing fallback")
    print(f"Mongo writes: {'DISABLED (--dry-run)' if args.dry_run else 'ENABLED'}")
    print(f"Brands: {', '.join(target_brands) if target_brands else 'ALL ACTIVE BRANDS'}")
    print(f"Target period: {month_label(args.target_month, args.target_year)}")

    print("\n[1/3] Building canonical NCR universe...")
    universe = build_ncr_variant_universe(active_only=not args.include_discontinued)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

    if target_brands:
        models = [m for m in models if normalize_key(m["brand_display"]) in target_brands]

    if args.model:
        mkey = normalize_key(args.model)
        models = [
            m for m in models
            if normalize_key(m["model_display"]) == mkey
            or mkey in normalize_key(m["model_display"])
            or normalize_key(m["model_slug"]) == mkey
        ]

    if args.limit_models:
        models = models[: args.limit_models]

    model_map = {(normalize_key(m["brand_display"]), normalize_key(m["model_display"])): m for m in models}
    print(f"Models in scope: {len(models)}")

    session = build_session()
    all_rows: List[OfferRow] = []
    no_offer = []
    source_counts: Dict[str, int] = {}
    previous_fallback_models = 0

    print("\n[2/3] Scraping models...")
    for model_entry in tqdm(models, desc="Offers", unit="model"):
        rows, reason = scrape_model_by_priority(
            session,
            model_entry,
            args.target_month,
            args.target_year,
            debug_dir,
            skip_carwale=args.skip_carwale,
            skip_autocar=args.skip_autocar,
            skip_carwale_listing=args.skip_carwale_listing,
        )

        if rows:
            all_rows.extend(rows)
            source_counts[rows[0].source] = source_counts.get(rows[0].source, 0) + 1
            if any(r.period_type == "previous" for r in rows):
                previous_fallback_models += 1
        else:
            no_offer.append((model_entry["brand_display"], model_entry["model_display"], reason))

        if args.debug:
            print_model_rows(model_entry, rows, reason)

        time.sleep(0.08 + random.uniform(0.02, 0.1))

    inherited_rows: List[OfferRow] = []
    if not args.no_inherit_nline:
        inherited_rows = apply_hyundai_nline_inheritance(all_rows, model_map)
        for row in inherited_rows:
            all_rows.append(row)
            source_counts[row.source] = source_counts.get(row.source, 0) + 1
            no_offer = [
                item for item in no_offer
                if not (normalize_key(item[0]) == normalize_key(row.brand) and normalize_key(item[1]) == normalize_key(row.model))
            ]

        if inherited_rows:
            print("\n[Inheritance] Added controlled Hyundai N Line inherited rows:")
            for row in inherited_rows:
                print(f"  - {row.brand} {row.model}: {format_inr(row.max_benefit)} ({row.confidence})")

    print("\n[3/3] Writing output files and Mongo decision...")
    fieldnames = list(OfferRow.__dataclass_fields__.keys())

    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(asdict(row))

    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in all_rows], f, indent=2, ensure_ascii=False)

    docs = build_model_offer_docs(
        all_rows,
        no_offer,
        models,
        args.target_month,
        args.target_year,
        write_empty=args.write_empty,
    )
    docs_with_offers = [d for d in docs if int(d.get("offer_count") or 0) > 0]

    print("\n===== OFFER SCRAPER SUMMARY =====")
    print(f"Models checked: {len(models)}")
    print(f"Offer rows found: {len(all_rows)}")
    print(f"Offer docs built: {len(docs)}")
    print(f"Offer docs with offers: {len(docs_with_offers)}")
    print(f"Source model coverage: {source_counts}")
    print(f"Controlled inherited rows added: {len(inherited_rows)}")
    print(f"Previous-month fallback models: {previous_fallback_models}")
    print(f"Models with no usable offer: {len(no_offer)}")

    if all_rows:
        print("\nTop model max benefits:")
        by_model: Dict[str, int] = {}
        by_source: Dict[str, str] = {}
        for r in all_rows:
            key = f"{r.brand} {r.model}"
            if key not in by_model or r.max_benefit > by_model[key]:
                by_model[key] = r.max_benefit
                by_source[key] = r.source
        for key, amt in sorted(by_model.items(), key=lambda kv: kv[1], reverse=True)[:80]:
            print(f"  - {key}: {format_inr(amt)} ({by_source[key]})")

    if no_offer and args.debug:
        print("\nNo usable offer models:")
        for brand, model, reason in no_offer:
            print(f"  - {brand} {model}: {reason}")

    print(f"\nCSV written: {args.output_csv}")
    print(f"JSON written: {args.output_json}")

    if args.dry_run:
        print("\nDry run only. No Mongo writes were performed.")
        return

    if len(docs_with_offers) < args.min_offer_docs:
        print("\n❌ SAFETY STOP: too few offer docs built. Mongo write skipped.")
        print(f"Docs with offers: {len(docs_with_offers)} | min required: {args.min_offer_docs}")
        return

    if not docs:
        print("\n❌ No docs built. Mongo write skipped.")
        return

    print("\nWriting to Mongo offers collection...")
    matched, modified, upserted, submitted = write_offer_docs_to_mongo(docs)
    print("Mongo write completed.")
    print(f"Matched: {matched}")
    print(f"Modified: {modified}")
    print(f"Upserted: {upserted}")
    print(f"Docs submitted: {submitted}")


if __name__ == "__main__":
    main()
