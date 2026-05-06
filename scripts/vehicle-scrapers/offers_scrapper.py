#!/usr/bin/env python3
"""
offers_scrapper.py
---------------------------
Production monthly offer scraper for CDrive.

Scope:
  - Default brands: Hyundai + Honda
  - Writes to Mongo offers collection unless --dry-run is passed
  - V3Cars is primary because it gives breakup rows
  - CarWale is fallback only when V3Cars has no usable current/previous offer
  - Controlled Hyundai N Line inheritance:
      * Creta N Line inherits from highest valid V3Cars Creta row because live V3Cars May 2026 Creta table includes N-Line in that high-benefit row
      * i20 N Line inherits i20 offer with corporate/rural forced to 0
        because V3Cars i20 row note says corporate/rural is Rs. 0 for N Line
      * Venue N Line is NOT inherited automatically
  - Outputs full breakup rows to console + CSV + JSON

Run:
  cd /Users/gauravgrover/cdb-api/scripts/vehicle-scrapers/

  python3 offers_v3_carwale_dryrun.py --debug
  python3 offers_v3_carwale_dryrun.py --brands Hyundai,Honda --debug
  python3 offers_v3_carwale_dryrun.py --brand Hyundai --model i20 --debug --debug-dir ./offer_debug_i20

Output:
  offer_dryrun.csv
  offer_dryrun.json

Important:
  Uses safety guards: zero-offer runs skip Mongo writes.
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
from tqdm import tqdm
from pymongo import UpdateOne

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

NO_OFFER_RE = re.compile(
    r"(offers and discounts are not available right now|not available right now|no offers are available|no offer available|currently no offers)",
    re.I,
)

DISCOUNT_FIELD_MAP = [
    ("cash_discount", re.compile(r"^(cash(?:\s+discount|\s+benefit|\s+offer)?|consumer\s+offer|consumer\s+benefit)\b", re.I)),
    ("exchange_bonus", re.compile(r"^(exchange(?:\s+bonus|\s+benefit|\s+offer)?)\b", re.I)),
    ("scrappage_bonus", re.compile(r"^(scrappage(?:\s+bonus|\s+benefit|\s+offer)?|scrap(?:\s+bonus|\s+benefit|\s+offer)?)\b", re.I)),
    ("upgrade_bonus", re.compile(r"^(upgrade(?:\s+bonus|\s+benefit|\s+offer)?)\b", re.I)),
    ("corporate_rural", re.compile(r"^(corporate\s*/\s*rural|corporate|rural)(?:\s+discount|\s+benefit|\s+offer)?\b", re.I)),
    ("additional_discount", re.compile(r"^(additional\s+discount|additional\s+benefit|gov(?:ernment)?\s+customer)\b", re.I)),
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

    # Important: total is the source's max benefit. Do not blindly sum all rows.
    max_benefit: int
    computed_possible_max: int
    breakup_available: bool
    total_matches_computed: bool

    notes: str
    confidence: str
    dealer_confirmation_required: bool
    raw_block: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run offer scraper: V3Cars primary + CarWale fallback")
    parser.add_argument("--brands", type=str, default="Hyundai,Honda", help="Comma-separated brand filter. Default: Hyundai,Honda")
    parser.add_argument("--brand", type=str, default="", help="Optional single brand filter, overrides --brands")
    parser.add_argument("--model", type=str, default="", help="Optional model filter, e.g. i20, venue, city")
    parser.add_argument("--target-month", type=int, default=TODAY.month)
    parser.add_argument("--target-year", type=int, default=TODAY.year)
    parser.add_argument("--output-csv", type=str, default="offer_dryrun.csv")
    parser.add_argument("--output-json", type=str, default="offer_dryrun.json")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--debug-dir", type=str, default="")
    parser.add_argument("--include-discontinued", action="store_true")
    parser.add_argument("--limit-models", type=int, default=0)
    parser.add_argument("--no-inherit-nline", action="store_true", help="Disable controlled Hyundai N Line inheritance")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to Mongo")
    parser.add_argument("--write-empty", action="store_true", help="Write empty no-offer docs; normally keep disabled")
    parser.add_argument("--min-offer-docs", type=int, default=1, help="Safety guard: skip Mongo write if fewer than this many offer docs are built")
    return parser.parse_args()


def month_label(month: int, year: int) -> str:
    return f"{MONTH_NAMES[month]} {year}"


def previous_month(month: int, year: int) -> Tuple[int, int]:
    return (12, year - 1) if month == 1 else (month - 1, year)


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


def money_to_int(raw: str) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).replace(",", "").replace("₹", "Rs ")
    m = re.search(r"(?:rs\.?|inr)?\s*([\d]+(?:\.\d+)?)\s*(?:lakh|lac|lakhs)", text, re.I)
    if m:
        try:
            val = int(round(float(m.group(1)) * 100000))
            return val if val >= 0 else None
        except Exception:
            return None
    m = re.search(r"(?:rs\.?|inr)\s*([\d]+)", text, re.I)
    if m:
        try:
            val = int(m.group(1))
            return val if val >= 0 else None
        except Exception:
            return None
    return None


def first_money(text: str) -> Optional[int]:
    if "Rs. 0" in text or "Rs 0" in text or "₹0" in text:
        return 0
    money_patterns = [
        r"(?:₹|rs\.?|inr)\s*[\d,]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)?",
        r"[\d]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)",
    ]
    for pat in money_patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            return money_to_int(m.group(0))
    return None


def amount_and_note_after_field(line: str, field_regex: re.Pattern) -> Tuple[int, str]:
    rest = field_regex.sub("", line, count=1).strip()
    amt = first_money(rest)
    if amt is None:
        amt = 0
    note = re.sub(r"(?:₹|rs\.?|inr)\s*[\d,]+(?:\.\d+)?\s*(?:lakh|lac|lakhs)?", " ", rest, count=1, flags=re.I)
    note = note.replace("-", " ")
    note = normalize_spaces(note)
    return int(amt), note


def normalize_corporate_rural(value: int, note: str) -> Tuple[int, int, str]:
    """
    V3Cars sometimes uses one row:
      Corporate/Rural Rs. 5,000 Only 3000 for Corporate/Gov Customers
    We keep rural_offer = row amount and corporate_discount if note says lower corporate amount.
    """
    corporate = 0
    rural = value or 0
    note_text = note or ""
    corp_match = re.search(r"(?:only\s*)?(?:rs\.?|₹)?\s*([\d,]+)\s*(?:for\s*)?(?:corporate|gov|government)", note_text, re.I)
    if corp_match:
        try:
            corporate = int(corp_match.group(1).replace(",", ""))
        except Exception:
            corporate = 0
    else:
        corporate = value or 0
    return corporate, rural, note_text


def split_v3_period_sections(text: str) -> Dict[str, str]:
    if not text:
        return {}
    month_re = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d{2}"
    matches = list(re.finditer(month_re, text, flags=re.I))
    sections: Dict[str, str] = {}

    for i, m in enumerate(matches):
        label = normalize_spaces(m.group(0)).title()
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else min(len(text), start + 12000)
        chunk = text[start:end]
        sections[label] = normalize_spaces((sections.get(label, "") + "\n" + chunk).strip())

    return sections


def source_label_from_period(label: str) -> Tuple[int, int]:
    parts = label.split()
    if len(parts) != 2:
        return (0, 0)
    month = MONTH_NAMES.index(parts[0]) if parts[0] in MONTH_NAMES else 0
    try:
        year = int(parts[1])
    except Exception:
        year = 0
    return month, year


def classify_discount_line(line: str) -> Optional[Tuple[str, re.Pattern]]:
    cleaned = normalize_spaces(line)
    for key, rx in DISCOUNT_FIELD_MAP:
        if rx.search(cleaned):
            return key, rx
    return None


def is_probable_variant_label(line: str, model_display: str, brand_display: str) -> bool:
    line_clean = normalize_spaces(line)
    if not line_clean:
        return False

    # If a table header is attached to the variant label, test only the prefix.
    candidate = clean_source_variant_label(line_clean, brand_display, model_display)
    low = candidate.lower()
    if not candidate:
        return False

    bad = [
        "notes", "applicable", "you can only choose",
        "max possible", "offers", "deals", "details about", "request button", "login", "sign up",
    ]
    if any(b in low for b in bad):
        return False
    if classify_discount_line(candidate):
        return False

    model_key = normalize_key(model_display)
    brand_key = normalize_key(brand_display)
    line_key = normalize_key(candidate)
    useful_words = [
        "variant", "variants", "petrol", "diesel", "manual", "auto", "automatic",
        "ivt", "dct", "cng", "electric", "hybrid", "all other", "all"
    ]
    return model_key in line_key or brand_key in line_key or any(w in low for w in useful_words)


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
        "Cash Discount", "Cash", "Exchange Bonus", "Exchange", "Scrappage Bonus", "Scrappage",
        "Upgrade", "Corporate/Rural", "Corporate", "Rural", "Additional discount", "Total"
    ]
    for name in field_names:
        forced = re.sub(rf"\s+({re.escape(name)}\b)", r"\n\1", forced, flags=re.I)

    forced = re.sub(rf"\s+({re.escape(brand)}\s+[A-Z0-9])", r"\n\1", forced, flags=re.I)
    forced = re.sub(r"\s+([A-Z0-9][A-Za-z0-9\s\-–,/()]+(?:Variant|Variants|Petrol|Diesel|CNG|Hybrid|Electric|Manual|Auto|Automatic|iVT|DCT))\s+(Discount Type|Cash|Exchange|Scrappage|Total)", r"\n\1\n\2", forced, flags=re.I)

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
        match = match_variant_label_to_canonical(current_label or "All variants", variant_list, brand, model)

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

                source_variant_label=current_label or "All variants",
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

        # Classify first. Lines like "Exchange Rs. 20,000 You can only choose one"
        # must still populate exchange_bonus; the old parser skipped them entirely.
        classified = classify_discount_line(line)
        if classified:
            key, rx = classified
            raw_lines.append(line)

            if not current_label:
                current_label = "All variants"

            if key == "total":
                amt, note = amount_and_note_after_field(line, rx)
                current["total"] = amt

                # If next variant heading is glued into the total note, preserve it for the next block.
                next_label = extract_next_variant_label_from_total_note(note, brand, model)
                clean_note = note
                if next_label:
                    clean_note = re.sub(r"(?i)max\s+discounts.*$", "", clean_note).strip()

                if clean_note:
                    row_notes.append(f"Total: {clean_note}")
                flush_current()

                if next_label:
                    current_label = next_label
                    raw_lines = [next_label]
                    row_notes = []
                continue

            if key == "corporate_rural":
                amt, note = amount_and_note_after_field(line, rx)
                corp, rural, split_note = normalize_corporate_rural(amt, note)
                current["corporate_discount"] = corp
                current["rural_offer"] = rural
                if split_note:
                    row_notes.append(f"Corporate/Rural: {split_note}")
                continue

            if key in {"finance_offer", "warranty_offer", "accessories_offer"}:
                amt, note = amount_and_note_after_field(line, rx)
                current[key] = line
                if note:
                    row_notes.append(f"{key}: {note}")
                continue

            amt, note = amount_and_note_after_field(line, rx)
            current[key] = amt
            if note:
                row_notes.append(f"{key}: {note}")
            continue

        if "you can only choose one" in line_low:
            row_notes.append(line)
            raw_lines.append(line)
            continue

        if is_probable_variant_label(line, model, brand):
            if current:
                flush_current()
            current_label = clean_source_variant_label(line, brand, model)
            raw_lines = [current_label]
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

    return deduped


def clean_source_variant_label(label: str, brand: str, model: str) -> str:
    text = normalize_spaces(label)
    # V3Cars table text can collapse like:
    # "Petrol–Manual,Auto (All Variants) Discount Type Discount Notes (If applicable)"
    # Keep only the actual variant/fuel label before table headers/noise.
    text = re.split(r"(?i)\bdiscount\s+type\b|\bdiscount\s+notes\b|\bmax\s+discounts\b", text, maxsplit=1)[0]
    text = normalize_spaces(text)
    cleaned = strip_variant_prefix(text, brand, model)
    return cleaned or text


def extract_next_variant_label_from_total_note(note: str, brand: str, model: str) -> str:
    """
    V3Cars sometimes collapses the next variant heading into the Total line, e.g.
    "Total Rs. 75,000 Max discounts Diesel–Manual,Auto (All Variants) Discount Type...".
    This extracts "Diesel–Manual,Auto (All Variants)" so the next offer row is not saved as generic All variants.
    """
    raw = normalize_spaces(note or "")
    if not raw:
        return ""

    # Prefer text after "Max discounts" because that is where the next heading begins.
    m = re.search(r"(?i)max\s+discounts\s+(.+)$", raw)
    candidate = m.group(1) if m else raw
    candidate = clean_source_variant_label(candidate, brand, model)

    if not candidate:
        return ""

    key = normalize_key(candidate)
    useful = ["petrol", "diesel", "cng", "hybrid", "electric", "manual", "auto", "automatic", "ivt", "dct", "variant", "variants", "all"]
    if any(x in key for x in useful) or normalize_key(model) in key:
        return candidate
    return ""


def match_variant_label_to_canonical(label: str, variant_list: List[str], brand: str, model: str) -> Dict:
    if not variant_list:
        return {"variant_scope": "model_level", "matched_variants": []}

    label_raw = normalize_spaces(label or "")
    label_key = normalize_key(label_raw)
    all_variants = list(variant_list)

    def is_diesel_variant(v: str) -> bool:
        return "diesel" in normalize_key(v)

    def is_cng_variant(v: str) -> bool:
        return "cng" in normalize_key(v)

    def is_electric_variant(v: str) -> bool:
        vk = normalize_key(v)
        return "electric" in vk or " ev " in f" {vk} "

    def is_hybrid_variant(v: str) -> bool:
        return "hybrid" in normalize_key(v)

    def is_petrol_variant(v: str) -> bool:
        # In our canonical vehicle names petrol is often implicit, while diesel/CNG/electric/hybrid are explicit.
        return not (is_diesel_variant(v) or is_cng_variant(v) or is_electric_variant(v) or is_hybrid_variant(v))

    fuel_filtered: List[str] = []
    fuel_scope = ""

    if "diesel" in label_key:
        fuel_filtered = [v for v in all_variants if is_diesel_variant(v)]
        fuel_scope = "diesel_group"
    elif "cng" in label_key:
        fuel_filtered = [v for v in all_variants if is_cng_variant(v)]
        fuel_scope = "cng_group"
    elif "electric" in label_key or " ev " in f" {label_key} ":
        fuel_filtered = [v for v in all_variants if is_electric_variant(v)]
        fuel_scope = "electric_group"
    elif "hybrid" in label_key:
        fuel_filtered = [v for v in all_variants if is_hybrid_variant(v)]
        fuel_scope = "hybrid_group"
    elif "petrol" in label_key:
        fuel_filtered = [v for v in all_variants if is_petrol_variant(v)]
        fuel_scope = "petrol_group"

    # If the source says "Petrol/Diesel (All Variants)", do NOT return all variants;
    # return only the matching fuel group.
    if fuel_filtered and ("all variant" in label_key or "all other variant" in label_key or "variant" in label_key):
        return {"variant_scope": fuel_scope, "matched_variants": fuel_filtered}

    if not label_key or "all variant" in label_key or "all other variant" in label_key or label_key == "all":
        return {"variant_scope": "all_variants", "matched_variants": all_variants}

    matched = []
    for v in all_variants:
        clean_v = normalize_variant_key(v, brand, model)
        clean_v_tokens = set(clean_v.split())
        label_tokens = set(label_key.split())

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
            continue

    if matched:
        scope = "exact_variant" if len(matched) == 1 else "variant_group"
        return {"variant_scope": scope, "matched_variants": matched}

    if fuel_filtered:
        return {"variant_scope": fuel_scope, "matched_variants": fuel_filtered}

    return {"variant_scope": "variant_group_unmatched", "matched_variants": []}

def v3_url(brand_slug: str, model_slug: str) -> str:
    return f"{BASE_V3}/{brand_slug}-cars/{model_slug}/offers-discounts"


def carwale_url(brand_slug: str, model_slug: str) -> str:
    brand_aliases = {
        "maruti": "maruti-suzuki",
        "maruti-suzuki": "maruti-suzuki",
        "kia": "kia",
        "honda": "honda",
        "hyundai": "hyundai",
    }
    b = brand_aliases.get(brand_slug, brand_slug)
    return f"{BASE_CARWALE}/{b}-cars/{model_slug}/offers/"


def scrape_v3(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path] = None,
) -> Tuple[List[OfferRow], str]:
    url = v3_url(model_entry["brand_slug"], model_entry["model_slug"])
    ok, status, raw_html, err = fetch_text(session, url)
    if not ok:
        return [], f"V3 fetch failed: {err}"

    text = clean_text(raw_html)
    if debug_dir:
        (debug_dir / f"v3_{model_entry['brand_slug']}_{model_entry['model_slug']}.txt").write_text(text, encoding="utf-8")

    sections = split_v3_period_sections(text)
    target_label = month_label(target_month, target_year)
    prev_month, prev_year = previous_month(target_month, target_year)
    prev_label = month_label(prev_month, prev_year)

    if target_label in sections:
        rows = parse_v3_offer_blocks(
            sections[target_label],
            model_entry,
            target_month,
            target_year,
            target_label,
            "current",
            url,
        )
        if rows:
            return rows, "v3_current"

    if prev_label in sections:
        rows = parse_v3_offer_blocks(
            sections[prev_label],
            model_entry,
            target_month,
            target_year,
            prev_label,
            "previous",
            url,
        )
        if rows:
            return rows, "v3_previous"

    return [], "v3_no_usable_current_or_previous_offer"


def parse_carwale_fallback(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path] = None,
) -> Tuple[List[OfferRow], str]:
    url = carwale_url(model_entry["brand_slug"], model_entry["model_slug"])
    ok, status, raw_html, err = fetch_text(session, url)
    if not ok:
        return [], f"CarWale fetch failed: {err}"

    text = clean_text(raw_html)
    if debug_dir:
        (debug_dir / f"carwale_{model_entry['brand_slug']}_{model_entry['model_slug']}.txt").write_text(text, encoding="utf-8")

    target_label = month_label(target_month, target_year)
    target_month_name = MONTH_NAMES[target_month]

    benefit_re = re.compile(
        rf"{re.escape(model_entry['model_display'])}.*?{target_month_name}\s+Offers.*?Get\s+Benefits\s+up\s+to\s+(Rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)",
        re.I | re.S,
    )
    m = benefit_re.search(text)
    if not m:
        m = re.search(r"Get\s+Benefits\s+up\s+to\s+(Rs\.?\s*[\d,]+(?:\.\d+)?(?:\s*lakh)?)", text, re.I)

    if not m:
        return [], "carwale_no_benefit_phrase"

    amount = money_to_int(m.group(1))
    if not amount or amount < 1000:
        return [], "carwale_bad_amount"

    valid_till = ""
    vt = re.search(r"Offer\s+Valid\s+Till\s*:\s*([^\n]+)", text, re.I)
    if vt:
        valid_till = normalize_spaces(vt.group(1))

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

        notes=f"CarWale total-only fallback. Valid till: {valid_till}".strip(),
        confidence="medium",
        dealer_confirmation_required=True,
        raw_block=normalize_spaces(m.group(0))[:1000],
    )
    return [row], "carwale_current_total_only"


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
        # i20 N Line has corporate/rural 0; reduce max to the recalculated possible max.
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

    # Creta N Line:
    # Live V3Cars May 2026 Creta table includes N-Line in the high-benefit petrol/all-variants row.
    # The parser can sometimes attach "N Line" text to the wrong raw block, so do NOT pick the first
    # row whose raw_block contains N Line. Use the highest valid structured Creta V3Cars row.
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

    # i20 N Line: user-approved assumption: inherit i20 all-other/N Line referenced row, corporate/rural = 0.
    i20_n = get_model("I20 N Line")
    if i20_n and ("hyundai", "i20 n line") not in existing_models:
        i20_rows = [r for r in all_rows if normalize_key(r.brand) == "hyundai" and normalize_key(r.model) == "i20" and r.source == "v3cars"]
        # Prefer row mentioning N Line in notes/raw; otherwise highest i20 row.
        preferred = [
            r for r in i20_rows
            if "n line" in normalize_key(r.source_variant_label + " " + r.notes + " " + r.raw_block)
            and r.max_benefit > 0
        ]
        if not preferred:
            preferred = [r for r in i20_rows if r.max_benefit > 0]
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




def row_to_offer_dict(row: OfferRow) -> Dict:
    return {
        "source": row.source,
        "source_url": row.source_url,
        "offer_month": row.offer_month,
        "offer_year": row.offer_year,
        "offer_month_label": row.offer_month_label,
        "period_type": row.period_type,
        "variant_scope": row.variant_scope,
        "source_variant_label": row.source_variant_label,
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


def build_model_offer_docs(rows: List[OfferRow], no_offer: List[Tuple[str, str, str]], models: List[Dict], target_month: int, target_year: int, write_empty: bool = False) -> List[Dict]:
    grouped: Dict[Tuple[str, str], List[OfferRow]] = {}
    for row in rows:
        grouped.setdefault((normalize_key(row.brand), normalize_key(row.model)), []).append(row)

    model_lookup = {(normalize_key(m["brand_display"]), normalize_key(m["model_display"])): m for m in models}
    no_offer_map = {(normalize_key(b), normalize_key(m)): reason for b, m, reason in no_offer}
    docs: List[Dict] = []

    def make_doc(model_entry: Dict, group_rows: List[OfferRow]) -> Dict:
        brand_display = model_entry.get("brand_display") or group_rows[0].brand
        model_display = model_entry.get("model_display") or group_rows[0].model
        brand_slug = model_entry.get("brand_slug") or group_rows[0].brand_slug
        model_slug = model_entry.get("model_slug") or group_rows[0].model_slug
        variant_list = model_entry.get("variant_list") or []

        current_rows = [r for r in group_rows if r.period_type == "current"]
        previous_rows = [r for r in group_rows if r.period_type == "previous"]
        current_month_published = bool(current_rows)
        fallback_used = (not current_month_published) and bool(previous_rows)
        data_status = "current_month" if current_month_published else ("fallback_previous_month" if fallback_used else "no_offer_found")
        source_names = sorted(set(r.source for r in group_rows))
        source_priority = {"v3cars": 4, "v3cars_inherited": 3, "carwale": 2}
        primary = sorted(group_rows, key=lambda r: (source_priority.get(r.source, 0), r.max_benefit), reverse=True)[0]
        max_benefit = max(int(r.max_benefit or 0) for r in group_rows)
        fallback_offer_period = None
        if fallback_used:
            fb = previous_rows[0]
            fallback_offer_period = {"month": fb.offer_month, "year": fb.offer_year, "month_label": fb.offer_month_label, "is_expired": True}

        if current_month_published:
            customer_safe_display = f"{brand_display} {model_display} current offer is up to {format_inr(max_benefit)}. Exact benefit depends on variant, stock, eligibility and dealer confirmation."
        else:
            customer_safe_display = f"{brand_display} {model_display} current month offer is not published in trusted sources. Last published {fallback_offer_period['month_label']} offer was up to {format_inr(max_benefit)}. Dealer confirmation required."

        return {
            "month": target_month,
            "year": target_year,
            "month_label": month_label(target_month, target_year),
            "brand": brand_display,
            "model": model_display,
            "brand_slug": brand_slug,
            "model_slug": model_slug,
            "source": "+".join(source_names),
            "source_summary": {"primary_source": primary.source, "source_count": len(source_names), "sources": source_names},
            "current_month_published": current_month_published,
            "data_status": data_status,
            "fallback_used": fallback_used,
            "offer_period": {"month": target_month, "year": target_year, "month_label": month_label(target_month, target_year), "is_current_month": True},
            "fallback_offer_period": fallback_offer_period,
            "variant_wise_available": any(r.variant_scope != "model_level" for r in group_rows),
            "canonical_variant_count": len(variant_list),
            "has_cash_discount": any((r.cash_discount or 0) > 0 for r in group_rows),
            "has_exchange_bonus": any((r.exchange_bonus or 0) > 0 for r in group_rows),
            "has_scrappage_bonus": any((r.scrappage_bonus or 0) > 0 for r in group_rows),
            "has_corporate_discount": any((r.corporate_discount or 0) > 0 for r in group_rows),
            "has_rural_offer": any((r.rural_offer or 0) > 0 for r in group_rows),
            "has_finance_offer": any(bool(r.finance_offer) for r in group_rows),
            "total_potential_benefit": max_benefit,
            "customer_safe_display": customer_safe_display,
            "offer_count": len(group_rows),
            "offers": [row_to_offer_dict(r) for r in sorted(group_rows, key=lambda r: r.max_benefit, reverse=True)],
            "dealer_confirmation_required": True,
            "last_updated": TODAY.isoformat(),
            "scraped_at": datetime.now().isoformat(),
        }

    for key, group_rows in grouped.items():
        model_entry = model_lookup.get(key) or {"brand_display": group_rows[0].brand, "model_display": group_rows[0].model, "brand_slug": group_rows[0].brand_slug, "model_slug": group_rows[0].model_slug, "variant_list": []}
        docs.append(make_doc(model_entry, group_rows))

    if write_empty:
        for m in models:
            key = (normalize_key(m["brand_display"]), normalize_key(m["model_display"]))
            if key in grouped:
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
                "source": "v3cars+carwale",
                "current_month_published": False,
                "data_status": "no_offer_found",
                "fallback_used": False,
                "offer_period": {"month": target_month, "year": target_year, "month_label": month_label(target_month, target_year), "is_current_month": True},
                "fallback_offer_period": None,
                "variant_wise_available": False,
                "canonical_variant_count": len(m.get("variant_list") or []),
                "has_cash_discount": False,
                "has_exchange_bonus": False,
                "has_scrappage_bonus": False,
                "has_corporate_discount": False,
                "has_rural_offer": False,
                "has_finance_offer": False,
                "total_potential_benefit": 0,
                "customer_safe_display": f"{m['brand_display']} {m['model_display']} offer not found in trusted sources. Dealer confirmation required.",
                "offer_count": 0,
                "offers": [],
                "no_offer_reason": reason,
                "dealer_confirmation_required": True,
                "last_updated": TODAY.isoformat(),
                "scraped_at": datetime.now().isoformat(),
            })
    return docs


def write_offer_docs_to_mongo(docs: List[Dict]) -> Tuple[int, int, int, int]:
    operations = [UpdateOne({"brand": d["brand"], "model": d["model"], "month": d["month"], "year": d["year"]}, {"$set": d}, upsert=True) for d in docs]
    if not operations:
        return (0, 0, 0, 0)
    result = offers_collection.bulk_write(operations, ordered=False)
    return (result.matched_count, result.modified_count, result.upserted_count, len(operations))

def format_inr(value: int) -> str:
    try:
        return f"₹{int(value):,}"
    except Exception:
        return "₹0"


def print_model_rows(model_entry: Dict, rows: List[OfferRow], reason: str) -> None:
    brand_model = f"{model_entry['brand_display']} {model_entry['model_display']}"
    if not rows:
        print(f"— {brand_model}: no usable offer ({reason})")
        return

    max_amt = max(r.max_benefit for r in rows)
    source = rows[0].source
    period = rows[0].offer_month_label
    fallback = " fallback" if rows[0].period_type == "previous" else ""
    print(f"\n✅ {brand_model}: {len(rows)} row(s), max {format_inr(max_amt)}, {source}, {period}{fallback}")

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

    target_brands = [normalize_key(x) for x in (args.brand or args.brands).split(",") if normalize_spaces(x)]
    if args.brand:
        target_brands = [normalize_key(args.brand)]

    print("\n===== CDRIVE MONTHLY OFFERS SCRAPER =====")
    print("Source priority: V3Cars breakup primary → CarWale total-only fallback")
    print(f"Mongo writes: {'DISABLED (--dry-run)' if args.dry_run else 'ENABLED'}")
    print(f"Brands: {', '.join(target_brands)}")
    print(f"Target period: {month_label(args.target_month, args.target_year)}")

    print("\n[1/3] Building canonical NCR universe...")
    universe = build_ncr_variant_universe(active_only=not args.include_discontinued)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

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

    model_map = {
        (normalize_key(m["brand_display"]), normalize_key(m["model_display"])): m
        for m in models
    }

    print(f"Models in scope: {len(models)}")

    session = build_session()
    all_rows: List[OfferRow] = []
    no_offer = []
    v3_models = 0
    carwale_models = 0
    previous_fallback_models = 0

    print("\n[2/3] Scraping models...")
    per_model_rows: Dict[Tuple[str, str], List[OfferRow]] = {}

    for model_entry in tqdm(models, desc="Offers", unit="model"):
        rows, reason = scrape_v3(session, model_entry, args.target_month, args.target_year, debug_dir)
        if rows:
            v3_models += 1
            if any(r.period_type == "previous" for r in rows):
                previous_fallback_models += 1
        else:
            rows, reason = parse_carwale_fallback(session, model_entry, args.target_month, args.target_year, debug_dir)
            if rows:
                carwale_models += 1

        key = (normalize_key(model_entry["brand_display"]), normalize_key(model_entry["model_display"]))
        per_model_rows[key] = rows

        if rows:
            all_rows.extend(rows)
        else:
            no_offer.append((model_entry["brand_display"], model_entry["model_display"], reason))

        if args.debug:
            print_model_rows(model_entry, rows, reason)

        time.sleep(0.08 + random.uniform(0.02, 0.1))

    inherited_rows: List[OfferRow] = []
    if not args.no_inherit_nline:
        inherited_rows = apply_hyundai_nline_inheritance(all_rows, model_map)
        if inherited_rows:
            print("\n[Inheritance] Added controlled Hyundai N Line inherited rows:")
            for row in inherited_rows:
                all_rows.append(row)
                # Remove from no_offer if present.
                no_offer = [
                    item for item in no_offer
                    if not (normalize_key(item[0]) == normalize_key(row.brand) and normalize_key(item[1]) == normalize_key(row.model))
                ]
                print(f"  - {row.brand} {row.model}: {format_inr(row.max_benefit)} ({row.confidence})")
                if args.debug:
                    fake_entry = model_map.get((normalize_key(row.brand), normalize_key(row.model)), {
                        "brand_display": row.brand, "model_display": row.model
                    })
                    print_model_rows(fake_entry, [row], "inherited")

    print("\n[3/3] Writing output files and Mongo decision...")
    fieldnames = list(OfferRow.__dataclass_fields__.keys())

    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(asdict(row))

    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in all_rows], f, indent=2, ensure_ascii=False)

    docs = build_model_offer_docs(all_rows, no_offer, models, args.target_month, args.target_year, write_empty=args.write_empty)
    docs_with_offers = [d for d in docs if int(d.get("offer_count") or 0) > 0]

    print("\n===== OFFER SCRAPER SUMMARY =====")
    print(f"Models checked: {len(models)}")
    print(f"Offer rows found: {len(all_rows)}")
    print(f"Offer docs built: {len(docs)}")
    print(f"Offer docs with offers: {len(docs_with_offers)}")
    print(f"Models covered by V3Cars: {v3_models}")
    print(f"Models covered by CarWale fallback: {carwale_models}")
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
        for key, amt in sorted(by_model.items(), key=lambda kv: kv[1], reverse=True):
            print(f"  - {key}: {format_inr(amt)} ({by_source[key]})")

    if no_offer:
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

    print("\nWriting to Mongo offers collection...")
    matched, modified, upserted, submitted = write_offer_docs_to_mongo(docs)
    print("Mongo write completed.")
    print(f"Matched: {matched}")
    print(f"Modified: {modified}")
    print(f"Upserted: {upserted}")
    print(f"Docs submitted: {submitted}")


if __name__ == "__main__":
    main()
