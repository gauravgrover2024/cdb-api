#!/usr/bin/env python3
"""
offers_scraper.py
-----------------
Replacement monthly offer scraper for CDrive / ACI Assist.

Primary source:
  - V3Cars model offer pages because they expose month-wise offer breakup.

Validation source:
  - Autocar India discount/news articles discovered from Autocar news sitemap/RSS.

Important behaviour:
  - Uses existing NCR vehicle universe from vehicles collection.
  - Does NOT discover vehicles independently.
  - Scrapes variant/group-wise offers where available.
  - Stores offer period, breakup, source signals and confidence.
  - If current month is not published, falls back to latest previous month.
  - Previous month fallback is clearly marked and never saved as confirmed current offer.
  - Adds detailed progress and diagnostics so a bad run does not silently end with 0 data.

Run examples:
  python offers_scraper.py --brand Hyundai --model i20 --dry-run --debug
  python offers_scraper.py --workers 2 --limit-models 20 --dry-run --debug
  python offers_scraper.py --workers 2

Mongo collection:
  offers_collection = db["offers"]
"""

import argparse
import html
import json
import random
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import offers_collection
from ncr_universe_utils_v2 import (
    build_ncr_variant_universe,
    normalize_key,
    normalize_spaces,
    normalize_variant_key,
    slugify,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_V3 = "https://www.v3cars.com"
BASE_AUTOCAR = "https://www.autocarindia.com"
AUTOCAR_NEWS_SITEMAP = f"{BASE_AUTOCAR}/news-sitemap.xml"
AUTOCAR_RSS_NEWS = f"{BASE_AUTOCAR}/rss/news"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "keep-alive",
}

TODAY = date.today()
DEFAULT_MONTH = TODAY.month
DEFAULT_YEAR = TODAY.year

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
MONTH_NAMES = {v: k.title() for k, v in MONTHS.items()}

OFFER_TYPE_MAP = {
    "cash": "cash_discount",
    "cash discount": "cash_discount",
    "consumer": "cash_discount",
    "consumer offer": "cash_discount",
    "exchange": "exchange_bonus",
    "exchange bonus": "exchange_bonus",
    "scrappage": "scrappage_bonus",
    "scrappage bonus": "scrappage_bonus",
    "upgrade": "upgrade_bonus",
    "upgrade bonus": "upgrade_bonus",
    "corporate": "corporate_discount",
    "corporate/rural": "corporate_discount",
    "corporate rural": "corporate_discount",
    "rural": "rural_offer",
    "additional": "additional_discount",
    "additional discount": "additional_discount",
    "loyalty": "loyalty_bonus",
    "loyalty bonus": "loyalty_bonus",
    "finance": "finance_offer",
    "finance offer": "finance_offer",
    "accessories": "accessories_offer",
    "accessory": "accessories_offer",
    "warranty": "warranty_offer",
    "insurance": "insurance_offer",
    "amc": "amc_offer",
}

NUMERIC_BREAKUP_FIELDS = [
    "cash_discount",
    "exchange_bonus",
    "scrappage_bonus",
    "upgrade_bonus",
    "corporate_discount",
    "rural_offer",
    "additional_discount",
    "loyalty_bonus",
    "accessories_offer",
    "insurance_offer",
    "warranty_offer",
    "amc_offer",
]

SKIP_SCOPE_LINES = {
    "discount type discount notes (if applicable)",
    "discount type",
    "notes (if applicable)",
    "you can only choose one",
    "max discounts",
    "max possible discounts",
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FetchResult:
    ok: bool
    status_code: Optional[int]
    url: str
    text: str = ""
    error: str = ""


@dataclass
class AutocarArticle:
    title: str
    url: str
    text: str


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def clamp_workers(workers: int) -> int:
    return max(1, min(int(workers or 1), 3))


def month_label(month: int, year: int) -> str:
    return f"{MONTH_NAMES.get(month, str(month))} {year}"


def previous_month(month: int, year: int) -> Tuple[int, int]:
    if month <= 1:
        return 12, year - 1
    return month - 1, year


def period_sort_key(month: int, year: int) -> int:
    return year * 100 + month


def compact_url(url: str, max_len: int = 110) -> str:
    url = str(url or "")
    if len(url) <= max_len:
        return url
    return url[: max_len - 3] + "..."


def inr(value: Optional[int]) -> str:
    if value is None:
        return "-"
    try:
        return f"₹{int(value):,}"
    except Exception:
        return str(value)


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_text(session: requests.Session, url: str, retries: int = 4) -> FetchResult:
    last_error = ""
    status_code = None
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=(10, 30), allow_redirects=True)
            status_code = resp.status_code
            if resp.status_code == 200 and resp.text:
                return FetchResult(True, resp.status_code, resp.url or url, resp.text)
            last_error = f"HTTP {resp.status_code}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep((2**attempt) + random.uniform(0.08, 0.25))
    return FetchResult(False, status_code, url, "", last_error)


def html_to_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = raw_html.replace("\\/", "/")
    text = re.sub(r"<script\b[^>]*>.*?</script>", "\n", text, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", "\n", text, flags=re.I | re.S)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</(?:p|div|li|tr|td|th|h1|h2|h3|h4|h5|section|article)>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\u00a0", " ")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def text_lines(raw: str) -> List[str]:
    lines = []
    for line in (raw or "").splitlines():
        clean = normalize_spaces(line)
        if clean:
            lines.append(clean)
    return lines


def extract_amount(text: str) -> Optional[int]:
    """Extract INR amount. Handles Rs 65,000, ₹65,000 and Rs 1.05 lakh."""
    if not text:
        return None
    cleaned = str(text).replace("₹", "Rs ").replace(",", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Rs 1.05 lakh / INR 1.10 lakh / 1 lakh
    m = re.search(r"(?:rs\.?|inr)?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:lakh|lac)\b", cleaned, flags=re.I)
    if m:
        try:
            return int(round(float(m.group(1)) * 100000))
        except Exception:
            pass

    # Rs 65 000 or Rs. 65000
    m = re.search(r"(?:rs\.?|inr)\s*([0-9][0-9\s]{0,12})", cleaned, flags=re.I)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if digits:
            try:
                return int(digits)
            except Exception:
                pass

    # fallback for direct amount followed by off/discount/benefit
    m = re.search(r"\b([0-9]{4,7})\b\s*(?:off|discount|benefit|benefits|bonus)?", cleaned, flags=re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    return None


def clean_note_from_line(line: str) -> str:
    if not line:
        return ""
    # Remove first money expression but keep any secondary notes like Rs. 0 for N Line.
    note = re.sub(r"^(Cash Discount|Cash|Exchange Bonus|Exchange|Scrappage Bonus|Scrappage|Upgrade|Corporate/Rural|Corporate|Rural|Additional discount|Additional|Total)\s*", "", line, flags=re.I)
    note = re.sub(r"(?:Rs\.?|INR|₹)\s*[0-9,.]+\s*(?:lakh|lac)?\s*-?", "", note, count=1, flags=re.I)
    note = normalize_spaces(note.strip(" -"))
    return note


def classify_offer_line(line: str) -> Optional[Tuple[str, str]]:
    """Return (field_key, raw_label) if line starts with a known offer component."""
    if not line:
        return None
    low = normalize_key(line)

    candidates = [
        ("cash discount", "cash_discount"),
        ("cash", "cash_discount"),
        ("exchange bonus", "exchange_bonus"),
        ("exchange", "exchange_bonus"),
        ("scrappage bonus", "scrappage_bonus"),
        ("scrappage", "scrappage_bonus"),
        ("upgrade bonus", "upgrade_bonus"),
        ("upgrade", "upgrade_bonus"),
        ("corporate rural", "corporate_discount"),
        ("corporate", "corporate_discount"),
        ("rural", "rural_offer"),
        ("additional discount", "additional_discount"),
        ("additional", "additional_discount"),
        ("loyalty bonus", "loyalty_bonus"),
        ("loyalty", "loyalty_bonus"),
        ("finance offer", "finance_offer"),
        ("finance", "finance_offer"),
        ("accessories", "accessories_offer"),
        ("insurance", "insurance_offer"),
        ("warranty", "warranty_offer"),
        ("amc", "amc_offer"),
    ]
    for prefix, key in candidates:
        if low.startswith(prefix):
            return key, prefix
    return None


def is_total_line(line: str) -> bool:
    return bool(re.match(r"^\s*Total\b", line or "", flags=re.I))


def compute_breakup_total(breakup: Dict[str, Optional[int]]) -> int:
    """
    Compute realistic max total. Exchange and scrappage are usually alternatives,
    so use the higher of the two instead of blindly adding both.
    """
    cash = int(breakup.get("cash_discount") or 0)
    exchange = int(breakup.get("exchange_bonus") or 0)
    scrappage = int(breakup.get("scrappage_bonus") or 0)
    upgrade = int(breakup.get("upgrade_bonus") or 0)
    corporate = int(breakup.get("corporate_discount") or 0)
    rural = int(breakup.get("rural_offer") or 0)
    additional = int(breakup.get("additional_discount") or 0)
    loyalty = int(breakup.get("loyalty_bonus") or 0)
    accessories = int(breakup.get("accessories_offer") or 0)
    insurance = int(breakup.get("insurance_offer") or 0)
    warranty = int(breakup.get("warranty_offer") or 0)
    amc = int(breakup.get("amc_offer") or 0)

    # Corporate/rural often comes as one line. If both are separately present, take max.
    corp_or_rural = max(corporate, rural)
    exchange_or_scrappage = max(exchange, scrappage)

    return cash + exchange_or_scrappage + upgrade + corp_or_rural + additional + loyalty + accessories + insurance + warranty + amc


def normalize_scope_label(label: str) -> str:
    label = normalize_spaces(label)
    label = re.sub(r"^(HYUNDAI|MARUTI SUZUKI|MARUTI|TATA|MAHINDRA|KIA|HONDA|TOYOTA|SKODA|VOLKSWAGEN|RENAULT|NISSAN|MG|MORRIS GARAGES)\s+", "", label, flags=re.I)
    return label.strip(" -")


def infer_variant_scope(scope_label: str) -> str:
    low = normalize_key(scope_label)
    if not low:
        return "model_level"
    if "all variants" in low:
        return "all_variants"
    if "all other variants" in low or "other variants" in low:
        return "variant_group"
    if any(token in low for token in ["petrol", "diesel", "cng", "manual", "auto", "ivt", "dct", "amt", "mt"]):
        return "variant_group"
    if any(token in low for token in ["variant", "variants", "trim", "trims", "edition", "line"]):
        return "variant_group"
    return "model_level"


def meaningful_scope_tokens(scope_label: str, brand: str, model: str) -> List[str]:
    low = normalize_key(scope_label)
    for remove in [brand, model, f"{brand} {model}"]:
        rk = normalize_key(remove)
        if rk:
            low = normalize_spaces(low.replace(rk, " "))
    stop = {
        "all", "other", "variants", "variant", "petrol", "diesel", "cng", "manual", "auto",
        "automatic", "mt", "at", "ivt", "dct", "amt", "normal", "new", "old", "my25", "my26",
        "deals", "offers", "discount", "discounts", "edition", "model", "models",
    }
    return [token for token in low.split() if token and token not in stop and len(token) > 1]


def match_canonical_variants(scope_label: str, variant_list: List[str], brand: str, model: str) -> Tuple[str, List[str]]:
    """Best-effort group mapping back to canonical variants from vehicles collection."""
    if not variant_list:
        return "model_only", []

    scope = infer_variant_scope(scope_label)
    low = normalize_key(scope_label)

    if "all variants" in low:
        return "all_variants", variant_list

    tokens = meaningful_scope_tokens(scope_label, brand, model)
    if not tokens:
        return scope, []

    matched = []
    for variant in variant_list:
        vkey = normalize_variant_key(variant, brand, model)
        if all(token in vkey for token in tokens):
            matched.append(variant)
            continue
        # softer token matching for groups like Sportz / Asta / N Line
        hit_count = sum(1 for token in tokens if token in vkey)
        if hit_count >= max(1, min(2, len(tokens))):
            matched.append(variant)

    status = "exact_variant" if len(matched) == 1 else ("variant_group" if matched else scope)
    return status, matched[:60]


def save_debug(debug_dir: Optional[Path], name: str, text: str) -> None:
    if not debug_dir:
        return
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "-", name).strip("-")[:140]
        (debug_dir / safe_name).write_text(text or "", encoding="utf-8")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# V3Cars parser
# ---------------------------------------------------------------------------


def v3cars_offer_url(brand_slug: str, model_slug: str) -> str:
    return f"{BASE_V3}/{brand_slug}-cars/{model_slug}/offers-discounts"


def extract_month_year(line: str) -> Optional[Tuple[int, int]]:
    if not line:
        return None
    pattern = r"\b(" + "|".join(MONTHS.keys()) + r")\s+(20\d{2})\b"
    m = re.search(pattern, line, flags=re.I)
    if not m:
        return None
    return MONTHS[m.group(1).lower()], int(m.group(2))


def line_is_month_section_start(line: str) -> bool:
    if not line:
        return False
    low = line.lower()
    if not extract_month_year(line):
        return False
    return any(token in low for token in ["discount offers", "deals", "offers"])


def parse_v3cars_sections(page_text: str) -> List[Dict]:
    lines = text_lines(page_text)
    sections: List[Dict] = []
    current: Optional[Dict] = None

    for line in lines:
        if line_is_month_section_start(line):
            found = extract_month_year(line)
            if found:
                mth, yr = found
                # V3 often has two consecutive heading lines for the same month.
                if current and current["month"] == mth and current["year"] == yr and len(current["lines"]) <= 3:
                    current["lines"].append(line)
                    continue
                if current:
                    sections.append(current)
                current = {
                    "month": mth,
                    "year": yr,
                    "month_label": month_label(mth, yr),
                    "heading": line,
                    "lines": [line],
                }
                continue

        if current:
            current["lines"].append(line)

    if current:
        sections.append(current)

    # Deduplicate duplicate sections if page repeats content.
    deduped: List[Dict] = []
    seen = set()
    for section in sections:
        key = (section["month"], section["year"], section["heading"][:80])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(section)
    return deduped


def choose_v3_section(sections: List[Dict], target_month: int, target_year: int) -> Tuple[Optional[Dict], bool]:
    if not sections:
        return None, False

    target_key = period_sort_key(target_month, target_year)
    exact = [s for s in sections if s["month"] == target_month and s["year"] == target_year]
    if exact:
        return exact[0], False

    previous = [s for s in sections if period_sort_key(s["month"], s["year"]) < target_key]
    previous.sort(key=lambda s: period_sort_key(s["month"], s["year"]), reverse=True)
    return (previous[0], True) if previous else (None, False)


def is_scope_candidate(line: str) -> bool:
    if not line:
        return False
    low = normalize_key(line)
    if low in SKIP_SCOPE_LINES:
        return False
    if line_is_month_section_start(line):
        return False
    if classify_offer_line(line) or is_total_line(line):
        return False
    if len(line) <= 2:
        return False
    if re.search(r"\bRs\.?\s*[0-9]", line, flags=re.I):
        return False
    # A good scope usually has car/variant words or variant descriptors.
    if any(token in low for token in ["variant", "variants", "petrol", "diesel", "manual", "auto", "cng", "edition", "line", "mt", "ivt", "dct"]):
        return True
    # Or all caps model group lines.
    if line.upper() == line and len(line.split()) >= 2:
        return True
    return False


def blank_breakup() -> Dict[str, Optional[int]]:
    return {field: None for field in NUMERIC_BREAKUP_FIELDS}


def finalize_offer_row(
    row: Optional[Dict],
    brand: str,
    model: str,
    variant_list: List[str],
    source_url: str,
) -> Optional[Dict]:
    if not row:
        return None
    max_benefit = row.get("max_benefit")
    breakup = row.get("breakup") or blank_breakup()
    computed_total = compute_breakup_total(breakup)

    # If total is absent but breakup exists, use computed total.
    if not max_benefit and computed_total > 0:
        max_benefit = computed_total

    if not max_benefit or max_benefit <= 0:
        return None

    diff = abs(int(max_benefit or 0) - int(computed_total or 0))
    breakup_has_amount = any((breakup.get(k) or 0) > 0 for k in NUMERIC_BREAKUP_FIELDS)
    matches_total = bool(breakup_has_amount and computed_total > 0 and diff <= 1000)

    scope_label = normalize_scope_label(row.get("variant_group") or "All variants")
    match_status, matched_variants = match_canonical_variants(scope_label, variant_list, brand, model)

    return {
        "variant_scope": infer_variant_scope(scope_label),
        "variant_group": scope_label,
        "variant": matched_variants[0] if len(matched_variants) == 1 else None,
        "variant_slug": slugify(matched_variants[0]) if len(matched_variants) == 1 else None,
        "fuel": None,
        "transmission": None,
        "canonical_match_status": match_status,
        "matched_canonical_variants": matched_variants,
        "max_benefit": int(max_benefit),
        "breakup": breakup,
        "breakup_total": int(computed_total or 0),
        "breakup_matches_total": matches_total,
        "use_breakup_as_truth": matches_total,
        "source_signals": [
            {
                "source": "v3cars",
                "source_url": source_url,
                "max_benefit": int(max_benefit),
                "has_breakup": breakup_has_amount,
                "breakup_total": int(computed_total or 0),
                "breakup_matches_total": matches_total,
                "confidence": "high" if matches_total else "medium",
                "raw_scope": scope_label,
            }
        ],
        "confidence": "high" if matches_total else "medium",
        "dealer_confirmation_required": True,
        "note": "Breakup matched total benefit." if matches_total else "Total and breakup need dealer confirmation.",
        "raw_lines": row.get("raw_lines", [])[:30],
    }


def parse_v3cars_offer_rows(
    section: Dict,
    brand: str,
    model: str,
    variant_list: List[str],
    source_url: str,
) -> List[Dict]:
    rows: List[Dict] = []
    current: Optional[Dict] = None

    def flush_current() -> None:
        nonlocal current
        final = finalize_offer_row(current, brand, model, variant_list, source_url)
        if final:
            rows.append(final)
        current = None

    for line in section.get("lines") or []:
        if line_is_month_section_start(line):
            continue

        if is_scope_candidate(line):
            # Avoid making the generic "HYUNDAI i20 OFFERS" line a variant group.
            low = normalize_key(line)
            if any(token in low for token in ["offers", "deals"]) and extract_month_year(line):
                continue
            if current and (current.get("max_benefit") or any((current.get("breakup") or {}).values())):
                flush_current()
            current = {
                "variant_group": line,
                "breakup": blank_breakup(),
                "max_benefit": None,
                "raw_lines": [line],
            }
            continue

        if not current:
            # Some pages start directly with Cash/Exchange rows. Create model-level group.
            if classify_offer_line(line) or is_total_line(line):
                current = {
                    "variant_group": "All variants",
                    "breakup": blank_breakup(),
                    "max_benefit": None,
                    "raw_lines": [],
                }
            else:
                continue

        current.setdefault("raw_lines", []).append(line)

        if is_total_line(line):
            amount = extract_amount(line)
            if amount is not None:
                current["max_benefit"] = amount
            flush_current()
            continue

        classified = classify_offer_line(line)
        if classified:
            field, _label = classified
            amount = extract_amount(line)
            if amount is not None:
                current.setdefault("breakup", blank_breakup())[field] = amount
            note = clean_note_from_line(line)
            if note:
                current.setdefault("breakup_notes", {})[field] = note

    flush_current()

    # Deduplicate rows with same scope and max benefit.
    deduped: List[Dict] = []
    seen = set()
    for row in rows:
        key = (normalize_key(row.get("variant_group")), row.get("max_benefit"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def scrape_v3cars_model(
    session: requests.Session,
    model_entry: Dict,
    target_month: int,
    target_year: int,
    debug_dir: Optional[Path] = None,
) -> Dict:
    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]
    brand = model_entry["brand_display"]
    model = model_entry["model_display"]
    variant_list = list(model_entry.get("variant_list") or [])
    url = v3cars_offer_url(brand_slug, model_slug)

    fetched = fetch_text(session, url)
    out = {
        "source": "v3cars",
        "source_url": url,
        "fetch_ok": fetched.ok,
        "status_code": fetched.status_code,
        "fetch_error": fetched.error,
        "sections_found": 0,
        "selected_period": None,
        "fallback_used": False,
        "offers": [],
    }

    if not fetched.ok:
        return out

    page_text = html_to_text(fetched.text)
    save_debug(debug_dir, f"v3cars-{brand_slug}-{model_slug}.txt", page_text)

    sections = parse_v3cars_sections(page_text)
    out["sections_found"] = len(sections)
    if not sections:
        return out

    target_key = period_sort_key(target_month, target_year)
    exact_sections = [
        s for s in sections
        if s.get("month") == target_month and s.get("year") == target_year
    ]
    previous_sections = [
        s for s in sections
        if period_sort_key(int(s.get("month") or 0), int(s.get("year") or 0)) < target_key
    ]
    previous_sections.sort(
        key=lambda s: period_sort_key(int(s.get("month") or 0), int(s.get("year") or 0)),
        reverse=True,
    )

    # Important: V3Cars often publishes a current-month section that only says
    # "details are not available right now". In that case, do NOT stop at the
    # empty current month. Actively try the previous published month and mark it
    # as fallback_previous_month if it has real offer rows.
    candidate_sections = []
    for section in exact_sections + previous_sections:
        key = (section.get("month"), section.get("year"), section.get("heading"))
        if key not in {(s.get("month"), s.get("year"), s.get("heading")) for s in candidate_sections}:
            candidate_sections.append(section)

    tried_periods = []
    first_selected = candidate_sections[0] if candidate_sections else None
    selected = None
    fallback_used = False
    offers: List[Dict] = []

    for section in candidate_sections:
        section_offers = parse_v3cars_offer_rows(section, brand, model, variant_list, fetched.url or url)
        is_fallback = not (section.get("month") == target_month and section.get("year") == target_year)
        tried_periods.append({
            "month": section.get("month"),
            "year": section.get("year"),
            "month_label": section.get("month_label"),
            "rows_found": len(section_offers),
            "fallback_candidate": is_fallback,
            "heading": section.get("heading"),
            "preview": " | ".join((section.get("lines") or [])[:4])[:700],
        })
        if section_offers:
            selected = section
            fallback_used = is_fallback
            offers = section_offers
            break

    # If no offer rows are found anywhere, keep the current month as the
    # selected diagnostic period when available, but do not create an offer.
    if not selected:
        selected = first_selected
        fallback_used = False

    if not selected:
        return out

    out.update(
        {
            "source_url": fetched.url or url,
            "selected_period": {
                "month": selected["month"],
                "year": selected["year"],
                "month_label": selected["month_label"],
            },
            "fallback_used": fallback_used,
            "offers": offers,
            "tried_periods": tried_periods,
        }
    )
    return out


# ---------------------------------------------------------------------------
# Autocar validation
# ---------------------------------------------------------------------------


def article_candidate_title(title: str, target_month: int, target_year: int) -> bool:
    low = (title or "").lower()
    if not any(w in low for w in ["discount", "discounts", "offer", "offers", "benefit", "benefits"]):
        return False
    # Prefer current/previous month, but keep generic discount pages too.
    target_name = MONTH_NAMES[target_month].lower()
    pm, py = previous_month(target_month, target_year)
    prev_name = MONTH_NAMES[pm].lower()
    return (
        str(target_year) in low
        or str(py) in low
        or target_name in low
        or prev_name in low
        or "this month" in low
    )


def parse_autocar_news_sitemap(session: requests.Session, target_month: int, target_year: int, max_articles: int) -> List[Tuple[str, str]]:
    fetched = fetch_text(session, AUTOCAR_NEWS_SITEMAP, retries=3)
    if not fetched.ok:
        return []

    candidates: List[Tuple[str, str]] = []
    try:
        root = ET.fromstring(fetched.text)
        ns = {
            "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
            "news": "http://www.google.com/schemas/sitemap-news/0.9",
        }
        for url_node in root.findall("sm:url", ns):
            loc_node = url_node.find("sm:loc", ns)
            title_node = url_node.find("news:news/news:title", ns)
            loc = normalize_spaces(loc_node.text if loc_node is not None else "")
            title = normalize_spaces(title_node.text if title_node is not None else "")
            if loc and title and article_candidate_title(title, target_month, target_year):
                candidates.append((title, loc))
    except Exception:
        return []

    # Prioritize stronger discount titles.
    candidates.sort(key=lambda x: ("discount" not in x[0].lower(), "offer" not in x[0].lower(), x[0]))
    return candidates[:max_articles]


def parse_autocar_rss(session: requests.Session, target_month: int, target_year: int, max_articles: int) -> List[Tuple[str, str]]:
    fetched = fetch_text(session, AUTOCAR_RSS_NEWS, retries=3)
    if not fetched.ok:
        return []
    pairs: List[Tuple[str, str]] = []
    try:
        root = ET.fromstring(fetched.text)
        for item in root.findall(".//item"):
            title_node = item.find("title")
            link_node = item.find("link")
            title = normalize_spaces(title_node.text if title_node is not None else "")
            link = normalize_spaces(link_node.text if link_node is not None else "")
            if title and link and article_candidate_title(title, target_month, target_year):
                pairs.append((title, link))
    except Exception:
        return []
    return pairs[:max_articles]


def discover_autocar_articles(
    target_month: int,
    target_year: int,
    max_articles: int = 30,
    debug: bool = False,
) -> List[AutocarArticle]:
    session = build_session()
    pairs = parse_autocar_news_sitemap(session, target_month, target_year, max_articles=max_articles)
    if len(pairs) < 5:
        pairs.extend(parse_autocar_rss(session, target_month, target_year, max_articles=max_articles))

    # Deduplicate by URL.
    seen = set()
    deduped = []
    for title, url in pairs:
        if url in seen:
            continue
        seen.add(url)
        deduped.append((title, url))

    articles: List[AutocarArticle] = []
    for title, url in deduped[:max_articles]:
        fetched = fetch_text(session, url, retries=3)
        if not fetched.ok:
            if debug:
                tqdm.write(f"[Autocar] fetch failed {compact_url(url)}: {fetched.error or fetched.status_code}")
            continue
        txt = html_to_text(fetched.text)
        if len(txt) < 500:
            continue
        articles.append(AutocarArticle(title=title, url=fetched.url or url, text=txt))

    return articles


def extract_autocar_model_block(article_text: str, brand: str, model: str, target_month: int, target_year: int) -> str:
    lines = text_lines(article_text)
    model_key = normalize_key(model)
    brand_key = normalize_key(brand)
    target_month_name = MONTH_NAMES[target_month].lower()

    blocks = []
    for idx, line in enumerate(lines):
        low = normalize_key(line)
        raw_low = line.lower()
        is_heading_like = any(w in raw_low for w in ["discount", "discounts", "offer", "offers", "benefit", "benefits"])
        model_hit = model_key and model_key in low
        brand_model_hit = brand_key and model_key and f"{brand_key} {model_key}" in low
        month_hit = target_month_name in raw_low or str(target_year) in raw_low

        if is_heading_like and (model_hit or brand_model_hit) and month_hit:
            block = lines[idx : idx + 12]
            blocks.append("\n".join(block))
        elif is_heading_like and (model_hit or brand_model_hit):
            block = lines[idx : idx + 12]
            blocks.append("\n".join(block))

    # Fallback: find model mention near an "up to" amount.
    if not blocks:
        for idx, line in enumerate(lines):
            low = normalize_key(line)
            if model_key and model_key in low:
                nearby = lines[max(0, idx - 3) : idx + 10]
                joined = "\n".join(nearby)
                if re.search(r"up to\s+(?:Rs\.?|₹|INR)\s*[0-9]", joined, flags=re.I):
                    blocks.append(joined)
                    break

    return "\n---\n".join(blocks[:2])


def parse_autocar_signal_from_block(block: str, article: AutocarArticle) -> Optional[Dict]:
    if not block:
        return None

    # Prefer amount in "Up to Rs ... off" line.
    amount = None
    m = re.search(r"up to\s+(?:Rs\.?|₹|INR)?\s*([0-9.,]+\s*(?:lakh|lac)?)", block, flags=re.I)
    if m:
        amount = extract_amount("Rs " + m.group(1))
    if amount is None:
        amount = extract_amount(block)
    if not amount:
        return None

    breakup = blank_breakup()
    lower = block.lower()

    component_patterns = {
        "cash_discount": r"(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?\s+cash discount|cash discount[^.\n]{0,80}(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?",
        "exchange_bonus": r"exchange (?:bonus|benefit|offer)[^.\n]{0,80}(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?|(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?\s+exchange",
        "scrappage_bonus": r"scrappage (?:bonus|benefit|offer)[^.\n]{0,80}(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?|(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?\s+scrappage",
        "upgrade_bonus": r"upgrade (?:bonus|benefit|offer)[^.\n]{0,80}(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?|(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?\s+upgrade",
        "corporate_discount": r"corporate (?:discount|benefit|offer)[^.\n]{0,80}(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?|(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?\s+corporate",
        "additional_discount": r"additional (?:discount|offer|benefit)[^.\n]{0,80}(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?|(?:rs\.?|₹|inr)\s*[0-9,.]+\s*(?:lakh|lac)?\s+additional",
    }

    for field, pattern in component_patterns.items():
        found = re.search(pattern, lower, flags=re.I)
        if found:
            breakup[field] = extract_amount(found.group(0))

    return {
        "source": "autocar_india",
        "source_url": article.url,
        "article_title": article.title,
        "max_benefit": int(amount),
        "has_breakup": any((breakup.get(k) or 0) > 0 for k in NUMERIC_BREAKUP_FIELDS),
        "breakup": breakup,
        "confidence": "medium",
        "raw_text": normalize_spaces(block)[:900],
    }


def find_autocar_signal(
    articles: List[AutocarArticle],
    brand: str,
    model: str,
    target_month: int,
    target_year: int,
) -> Optional[Dict]:
    for article in articles:
        # Cheap skip before parsing block.
        hay = normalize_key(f"{article.title} {article.text[:3000]}")
        if normalize_key(model) not in hay:
            continue
        block = extract_autocar_model_block(article.text, brand, model, target_month, target_year)
        signal = parse_autocar_signal_from_block(block, article)
        if signal:
            return signal
    return None


# ---------------------------------------------------------------------------
# Merge / document builder
# ---------------------------------------------------------------------------


def build_customer_safe_display(doc: Dict) -> str:
    model_name = f"{doc.get('brand')} {doc.get('model')}".strip()
    amount = doc.get("total_potential_benefit") or 0
    selected = doc.get("fallback_offer_period") if doc.get("fallback_used") else doc.get("offer_period")
    selected_label = (selected or {}).get("month_label") or doc.get("month_label")

    if amount <= 0:
        return f"No public offer found for {model_name} for {doc.get('month_label')}. Dealer confirmation required."

    if doc.get("fallback_used"):
        return (
            f"{doc.get('month_label')} offer is not published yet. Last published {selected_label} offer for "
            f"{model_name} was up to {inr(amount)}, subject to variant, eligibility, stock and dealer confirmation."
        )

    return (
        f"{model_name} {selected_label} public offer is up to {inr(amount)}, subject to variant, "
        "eligibility, stock and dealer confirmation."
    )


def merge_autocar_into_offers(offers: List[Dict], signal: Optional[Dict]) -> List[Dict]:
    if not signal:
        return offers
    if not offers:
        return []

    signal_amount = int(signal.get("max_benefit") or 0)
    for offer in offers:
        offer.setdefault("source_signals", [])
        offer_amount = int(offer.get("max_benefit") or 0)
        # Attach Autocar to every close match, or to the highest row if one source gives model-level amount.
        if signal_amount and abs(offer_amount - signal_amount) <= 5000:
            offer["source_signals"].append(signal)
            offer["confidence"] = "high" if offer.get("breakup_matches_total") else "medium"
    return offers


def build_model_doc(
    model_entry: Dict,
    v3_result: Dict,
    autocar_signal: Optional[Dict],
    target_month: int,
    target_year: int,
) -> Dict:
    brand = model_entry["brand_display"]
    model = model_entry["model_display"]
    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]

    offers = list(v3_result.get("offers") or [])
    offers = merge_autocar_into_offers(offers, autocar_signal)

    # Autocar is intentionally validation-only. It must not create model-level
    # offer rows by itself because generic article amounts can otherwise get
    # applied to unrelated models. If V3Cars has no structured row, we leave the
    # model as not_found or fallback_previous_month only.

    selected_period = v3_result.get("selected_period") or None
    fallback_used = bool(v3_result.get("fallback_used"))
    current_month_published = bool(selected_period and not fallback_used and selected_period.get("month") == target_month and selected_period.get("year") == target_year)

    if not selected_period and autocar_signal:
        selected_period = {
            "month": target_month,
            "year": target_year,
            "month_label": month_label(target_month, target_year),
        }
        current_month_published = True
        fallback_used = False

    target_period = {
        "month": target_month,
        "year": target_year,
        "month_label": month_label(target_month, target_year),
        "valid_from": f"{target_year}-{target_month:02d}-01",
        "valid_till": None,
        "is_current_month": True,
    }

    fallback_period = None
    if fallback_used and selected_period:
        sm = int(selected_period["month"])
        sy = int(selected_period["year"])
        fallback_period = {
            "month": sm,
            "year": sy,
            "month_label": selected_period["month_label"],
            "valid_from": f"{sy}-{sm:02d}-01",
            "valid_till": None,
            "is_expired": True,
        }

    total_potential_benefit = max([int(o.get("max_benefit") or 0) for o in offers] or [0])

    all_breakups = [o.get("breakup") or {} for o in offers]
    has = lambda key: any((b.get(key) or 0) > 0 for b in all_breakups)

    source_signals_flat = []
    for offer in offers:
        for sig in offer.get("source_signals") or []:
            source_signals_flat.append(sig)

    source_names = sorted(set(sig.get("source") for sig in source_signals_flat if sig.get("source")))
    if v3_result.get("fetch_ok") and "v3cars" not in source_names:
        source_names.insert(0, "v3cars")

    doc = {
        "month": target_month,
        "year": target_year,
        "month_label": month_label(target_month, target_year),
        "brand": brand,
        "model": model,
        "brand_slug": brand_slug,
        "model_slug": model_slug,
        "source": "+".join(source_names) if source_names else "v3cars+autocar_india",
        "current_month_published": current_month_published,
        "data_status": "current_month" if current_month_published else ("fallback_previous_month" if fallback_used else "not_found"),
        "fallback_used": fallback_used,
        "offer_period": target_period,
        "fallback_offer_period": fallback_period,
        "has_cash_discount": has("cash_discount"),
        "has_exchange_bonus": has("exchange_bonus"),
        "has_scrappage_bonus": has("scrappage_bonus"),
        "has_corporate_discount": has("corporate_discount"),
        "has_finance_offer": has("finance_offer"),
        "has_accessories_offer": has("accessories_offer"),
        "variant_wise_available": any(o.get("variant_scope") in {"exact_variant", "variant_group", "all_variants"} for o in offers),
        "offer_count": len(offers),
        "offers": offers,
        "total_potential_benefit": total_potential_benefit,
        "source_summary": {
            "primary_source": "v3cars" if v3_result.get("offers") else None,
            "validation_sources": ["autocar_india"] if autocar_signal else [],
            "source_count": len(source_names),
            "conflicting_sources": [],
            "v3cars": {
                "url": v3_result.get("source_url"),
                "fetch_ok": v3_result.get("fetch_ok"),
                "status_code": v3_result.get("status_code"),
                "sections_found": v3_result.get("sections_found"),
                "selected_period": selected_period,
                "fallback_used": fallback_used,
                "tried_periods": v3_result.get("tried_periods"),
                "fetch_error": v3_result.get("fetch_error"),
            },
            "autocar_india": {
                "matched": bool(autocar_signal),
                "url": autocar_signal.get("source_url") if autocar_signal else None,
                "max_benefit": autocar_signal.get("max_benefit") if autocar_signal else None,
            },
        },
        "confidence": "high" if any(o.get("confidence") == "high" for o in offers) else ("medium" if offers else "low"),
        "dealer_confirmation_required": True,
        "scraped_at": datetime.now().isoformat(),
        "last_updated": TODAY.isoformat(),
    }
    doc["customer_safe_display"] = build_customer_safe_display(doc)
    return doc


# ---------------------------------------------------------------------------
# Main per-model task
# ---------------------------------------------------------------------------


def scrape_model_offers(
    model_entry: Dict,
    target_month: int,
    target_year: int,
    autocar_articles: List[AutocarArticle],
    debug_dir: Optional[Path] = None,
) -> Dict:
    session = build_session()
    brand = model_entry["brand_display"]
    model = model_entry["model_display"]

    v3_result = scrape_v3cars_model(session, model_entry, target_month, target_year, debug_dir=debug_dir)
    autocar_signal = find_autocar_signal(autocar_articles, brand, model, target_month, target_year)
    doc = build_model_doc(model_entry, v3_result, autocar_signal, target_month, target_year)

    return {
        "doc": doc,
        "progress": {
            "brand": brand,
            "model": model,
            "offer_count": doc.get("offer_count", 0),
            "max_benefit": doc.get("total_potential_benefit", 0),
            "data_status": doc.get("data_status"),
            "v3_fetch_ok": v3_result.get("fetch_ok"),
            "v3_status_code": v3_result.get("status_code"),
            "v3_sections_found": v3_result.get("sections_found"),
            "v3_selected_period": v3_result.get("selected_period"),
            "fallback_used": doc.get("fallback_used"),
            "autocar_matched": bool(autocar_signal),
            "v3_url": v3_result.get("source_url"),
            "error": v3_result.get("fetch_error"),
        },
    }


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NCR monthly car offers scraper using V3Cars + Autocar India")
    parser.add_argument("--workers", type=int, default=2, help="Max worker threads, hard-capped at 3")
    parser.add_argument("--limit-models", type=int, default=0, help="Limit models for test runs")
    parser.add_argument("--brand", type=str, default="", help="Filter by brand display/slug, e.g. Hyundai")
    parser.add_argument("--model", type=str, default="", help="Filter by model display/slug, e.g. i20")
    parser.add_argument("--month", type=int, default=DEFAULT_MONTH, help="Target month number, default=current month")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Target year, default=current year")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to Mongo")
    parser.add_argument("--include-discontinued", action="store_true", help="Include discontinued variants from vehicle universe")
    parser.add_argument("--debug", action="store_true", help="Print per-model progress lines")
    parser.add_argument("--debug-dir", type=str, default="", help="Optional folder to save fetched text pages")
    parser.add_argument("--progress-every", type=int, default=20, help="Print summary every N completed models")
    parser.add_argument("--max-autocar-articles", type=int, default=40, help="Max Autocar discount articles to fetch for validation")
    parser.add_argument("--write-empty", action="store_true", help="Also write empty/no-offer docs. Default writes only docs with offers.")
    parser.add_argument("--force-write-zero-run", action="store_true", help="Allow DB write even when 0 models have offers. Unsafe; for debugging only.")
    return parser.parse_args()


def filter_models(models: List[Dict], brand_filter: str, model_filter: str) -> List[Dict]:
    bq = normalize_key(brand_filter)
    mq = normalize_key(model_filter)
    out = []
    for item in models:
        brand_blob = normalize_key(f"{item.get('brand_display')} {item.get('brand_slug')}")
        model_blob = normalize_key(f"{item.get('model_display')} {item.get('model_slug')}")
        if bq and bq not in brand_blob:
            continue
        if mq and mq not in model_blob:
            continue
        out.append(item)
    return out


def print_progress_line(progress: Dict) -> None:
    brand = progress["brand"]
    model = progress["model"]
    offers = progress["offer_count"]
    amount = progress["max_benefit"]
    status = progress["data_status"]
    selected = progress.get("v3_selected_period") or {}
    selected_label = selected.get("month_label") or "no period"

    if offers > 0:
        icon = "✅" if status == "current_month" else "🟡"
        fallback = " fallback" if progress.get("fallback_used") else ""
        autocar = " + Autocar" if progress.get("autocar_matched") else ""
        tqdm.write(f"{icon} {brand} {model}: {offers} row(s), max {inr(amount)}, {selected_label}{fallback}{autocar}")
    else:
        fetch_state = "ok" if progress.get("v3_fetch_ok") else f"fail {progress.get('v3_status_code') or ''}"
        tqdm.write(
            f"❌ {brand} {model}: no offers | V3 {fetch_state}, sections={progress.get('v3_sections_found')}, "
            f"Autocar={progress.get('autocar_matched')} | {compact_url(progress.get('v3_url'))}"
        )


def main() -> None:
    args = parse_args()
    workers = clamp_workers(args.workers)
    debug_dir = Path(args.debug_dir).expanduser() if args.debug_dir else None
    start = time.time()

    print("\n===== CDRIVE MONTHLY OFFERS SCRAPER =====")
    print(f"Target period: {month_label(args.month, args.year)}")
    print(f"Sources: V3Cars primary + Autocar India validation")
    print(f"Workers: {workers} | dry_run: {args.dry_run} | write_empty: {args.write_empty}")

    print("\n[1/4] Building NCR vehicle universe from vehicles collection...")
    universe = build_ncr_variant_universe(active_only=not args.include_discontinued)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

    models = filter_models(models, args.brand, args.model)
    if args.limit_models and args.limit_models > 0:
        models = models[: args.limit_models]

    print(f"Models in scope: {len(models)}")
    if args.brand or args.model:
        print(f"Filters: brand={args.brand or '-'} | model={args.model or '-'}")

    if not models:
        print("No models found from universe. Check vehicles collection / filters.")
        return

    print("\n[2/4] Discovering Autocar India validation articles...")
    autocar_articles = discover_autocar_articles(
        args.month,
        args.year,
        max_articles=args.max_autocar_articles,
        debug=args.debug,
    )
    print(f"Autocar candidate articles fetched: {len(autocar_articles)}")
    if args.debug and autocar_articles:
        for article in autocar_articles[:8]:
            print(f"  - {article.title} | {compact_url(article.url)}")

    print("\n[3/4] Scraping model offer pages...")
    all_docs: List[Dict] = []
    progress_rows: List[Dict] = []
    errors: List[str] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                scrape_model_offers,
                model_entry,
                args.month,
                args.year,
                autocar_articles,
                debug_dir,
            ): model_entry
            for model_entry in models
        }

        completed = 0
        for future in tqdm(as_completed(futures), total=len(futures), desc="Offers", unit="model"):
            completed += 1
            model_entry = futures[future]
            try:
                result = future.result()
                doc = result["doc"]
                progress = result["progress"]
                all_docs.append(doc)
                progress_rows.append(progress)

                if args.debug or doc.get("offer_count", 0) > 0:
                    print_progress_line(progress)

            except Exception as exc:
                label = f"{model_entry.get('brand_display')} {model_entry.get('model_display')}"
                msg = f"{label}: {exc}"
                errors.append(msg)
                tqdm.write(f"💥 ERROR {msg}")

            if args.progress_every and completed % args.progress_every == 0:
                with_offers_so_far = sum(1 for d in all_docs if d.get("offer_count", 0) > 0)
                fallback_so_far = sum(1 for d in all_docs if d.get("fallback_used"))
                tqdm.write(
                    f"Progress checkpoint: {completed}/{len(models)} done | with offers {with_offers_so_far} | fallback {fallback_so_far} | errors {len(errors)}"
                )

    print("\n[4/4] Summary and Mongo write decision...")

    total = len(all_docs)
    with_offers = sum(1 for d in all_docs if d.get("offer_count", 0) > 0)
    current_count = sum(1 for d in all_docs if d.get("data_status") == "current_month")
    fallback_count = sum(1 for d in all_docs if d.get("fallback_used"))
    not_found = sum(1 for d in all_docs if d.get("offer_count", 0) == 0)
    autocar_matches = sum(1 for p in progress_rows if p.get("autocar_matched"))
    v3_fetch_failures = sum(1 for p in progress_rows if not p.get("v3_fetch_ok"))
    v3_no_sections = sum(1 for p in progress_rows if p.get("v3_fetch_ok") and not p.get("v3_sections_found"))

    print("\n===== RUN SUMMARY =====")
    print(f"Target period: {month_label(args.month, args.year)}")
    print(f"Models attempted: {len(models)}")
    print(f"Docs built: {total}")
    print(f"Models with offers: {with_offers}")
    print(f"Current-month published: {current_count}")
    print(f"Fallback previous-month used: {fallback_count}")
    print(f"No-offer docs: {not_found}")
    print(f"Autocar validation matches: {autocar_matches}")
    print(f"V3 fetch failures: {v3_fetch_failures}")
    print(f"V3 pages with no month sections: {v3_no_sections}")
    print(f"Errors: {len(errors)}")

    top_docs = sorted([d for d in all_docs if d.get("total_potential_benefit", 0) > 0], key=lambda d: d.get("total_potential_benefit", 0), reverse=True)[:10]
    if top_docs:
        print("\nTop offers found:")
        for d in top_docs:
            status = "fallback" if d.get("fallback_used") else "current"
            print(f"  - {d['brand']} {d['model']}: {inr(d.get('total_potential_benefit'))} | {status} | rows={d.get('offer_count')}")

    if not_found:
        print("\nFirst no-offer diagnostics:")
        for p in [p for p in progress_rows if p.get("offer_count", 0) == 0][:12]:
            print(
                f"  - {p['brand']} {p['model']} | V3 ok={p.get('v3_fetch_ok')} "
                f"status={p.get('v3_status_code')} sections={p.get('v3_sections_found')} "
                f"Autocar={p.get('autocar_matched')} | {compact_url(p.get('v3_url'))}"
            )

    if errors:
        print("\nFirst errors:")
        for err in errors[:10]:
            print(f"  - {err}")

    write_docs = all_docs if args.write_empty else [d for d in all_docs if d.get("offer_count", 0) > 0]

    # Safety guard: prevent wiping/overwriting every model as 0 offers on a bad website run.
    if with_offers == 0 and not args.force_write_zero_run:
        print("\n❌ ZERO OFFER RUN DETECTED. Mongo write skipped.")
        print("Reason: no model produced any offer rows. This usually means page structure changed, source blocked requests, or filters are wrong.")
        print("Try: python offers_scraper.py --brand Hyundai --model i20 --dry-run --debug --debug-dir ./offer_debug")
        return

    if args.dry_run:
        print("\nDRY RUN: Mongo write skipped.")
        sample = next((d for d in all_docs if d.get("offer_count", 0) > 0), all_docs[0] if all_docs else {})
        print("\nSample document:")
        print(json.dumps(sample, indent=2, default=str)[:12000])
        print(f"\nRuntime: {round(time.time() - start, 2)}s")
        return

    if not write_docs:
        print("No docs selected for write. Use --write-empty if you intentionally want to upsert empty offer docs.")
        return

    operations: List[UpdateOne] = []
    for doc in write_docs:
        operations.append(
            UpdateOne(
                {
                    "brand": doc["brand"],
                    "model": doc["model"],
                    "month": doc["month"],
                    "year": doc["year"],
                },
                {"$set": doc},
                upsert=True,
            )
        )

    result = offers_collection.bulk_write(operations, ordered=False)
    print("\nMongo write completed.")
    print(f"Matched: {result.matched_count}")
    print(f"Modified: {result.modified_count}")
    print(f"Upserted: {result.upserted_count}")
    print(f"Docs submitted: {len(operations)}")
    print(f"Runtime: {round(time.time() - start, 2)}s")


if __name__ == "__main__":
    main()
