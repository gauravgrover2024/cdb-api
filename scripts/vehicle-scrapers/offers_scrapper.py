"""
offers_scraper.py
------------------
Scrapes monthly car offers & discounts from CarDekho and CarWale.

Data captured per offer:
  - Cash discount
  - Exchange bonus
  - Corporate / loyalty discount
  - Finance offers (low interest, zero down payment)
  - Free accessories / AMC
  - Month/year of offer

Run this once at the start of each month via cron:
  0 6 1 * * python offers_scraper.py

MongoDB collection: offers_collection
Schema: {brand, model, variant?, city, month, year, offers: [...], source, scraped_at}
"""

import json
import random
import re
import time
from datetime import date, datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import offers_collection
from ncr_universe_utils_v2 import (
    build_ncr_variant_universe,
    normalize_spaces,
    NCR_CITIES,
)

BASE_CD = "https://www.cardekho.com"
BASE_CW = "https://www.carwale.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}
TODAY = date.today()
CURRENT_MONTH = TODAY.month
CURRENT_YEAR = TODAY.year


# ---------------------------------------------------------------------------
# Offer type patterns for text classification
# ---------------------------------------------------------------------------

OFFER_PATTERNS = {
    "cash_discount": [
        r"cash\s*(?:discount|back|off)", r"flat\s*(?:₹|rs\.?|inr)?\s*[\d,]+\s*(?:off|discount)",
        r"direct\s*discount",
    ],
    "exchange_bonus": [
        r"exchange\s*(?:bonus|offer|benefit|discount)",
        r"old\s*car\s*(?:bonus|exchange)", r"trade.?in",
    ],
    "corporate_discount": [
        r"corporate\s*(?:discount|offer|benefit)",
        r"employee\s*(?:discount|offer)", r"fleet\s*(?:discount|offer)",
    ],
    "loyalty_discount": [
        r"loyalty\s*(?:bonus|discount|offer)", r"existing\s*(?:owner|customer)",
        r"repurchase\s*(?:bonus|discount)",
    ],
    "finance_offer": [
        r"(?:zero|0)%?\s*(?:down\s*payment|dp)", r"low\s*(?:emi|interest)",
        r"finance\s*(?:offer|benefit|scheme)", r"interest\s*subvention",
    ],
    "accessories": [
        r"free\s*(?:accessories|accessory)", r"(?:worth|valued)\s*(?:₹|rs\.?)\s*[\d,]+\s*accessories",
    ],
    "insurance": [
        r"free\s*insurance", r"complimentary\s*insurance",
        r"insurance\s*(?:benefit|offer|free)",
    ],
    "extended_warranty": [
        r"free\s*(?:extended\s*)?warranty", r"complimentary\s*warranty",
    ],
    "amc": [
        r"free\s*(?:amc|annual\s*maintenance)", r"complimentary\s*(?:service|maintenance)",
    ],
}


def classify_offer_text(text: str) -> str:
    text_lower = text.lower()
    for offer_type, patterns in OFFER_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return offer_type
    return "other"


def extract_amount(text: str) -> Optional[int]:
    """Extract monetary amount from offer text. Returns int in INR or None."""
    text = text.replace(",", "")
    # Match ₹X lakh / X lakh
    lakh = re.search(r"(?:₹|rs\.?|inr)?\s*([\d.]+)\s*lakh", text, re.IGNORECASE)
    if lakh:
        try:
            return int(float(lakh.group(1)) * 100000)
        except ValueError:
            pass
    # Match ₹X,XXX or ₹XXXXX
    amount = re.search(r"(?:₹|rs\.?|inr)\s*([\d]+)", text, re.IGNORECASE)
    if amount:
        try:
            return int(amount.group(1))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# CarDekho offers scraping
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_text(session: requests.Session, url: str, retries: int = 4) -> Optional[str]:
    for i in range(retries):
        try:
            r = session.get(url, timeout=(10, 30))
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            pass
        time.sleep((2 ** i) + random.uniform(0.1, 0.3))
    return None


def parse_offers_from_html(html: str, brand: str, model: str) -> List[Dict]:
    """
    Parse offer cards from CarDekho offers HTML.
    CarDekho embeds offer data in JSON within <script> tags.
    """
    offers = []
    normalized = html.replace("\\/", "/")

    # Try to extract structured JSON offer data
    json_patterns = [
        r'"offers"\s*:\s*(\[.+?\])',
        r'"offerList"\s*:\s*(\[.+?\])',
        r'"dealerOffers"\s*:\s*(\[.+?\])',
        r'"monthlyOffers"\s*:\s*(\[.+?\])',
    ]

    raw_offer_lists = []
    for pattern in json_patterns:
        for match in re.finditer(pattern, normalized, re.DOTALL):
            try:
                data = json.loads(match.group(1))
                if isinstance(data, list):
                    raw_offer_lists.extend(data)
            except json.JSONDecodeError:
                pass

    for item in raw_offer_lists:
        if not isinstance(item, dict):
            continue
        title = normalize_spaces(
            item.get("title") or item.get("offerTitle") or item.get("name") or ""
        )
        description = normalize_spaces(
            item.get("description") or item.get("offerDescription") or item.get("text") or ""
        )
        full_text = f"{title} {description}".strip()
        if not full_text:
            continue

        offer_type = classify_offer_text(full_text)
        amount = extract_amount(full_text)

        offers.append({
            "title": title,
            "description": description,
            "offer_type": offer_type,
            "amount": amount,
            "raw": full_text[:500],
        })

    # Regex fallback: look for offer text blobs in HTML
    if not offers:
        text_pattern = re.compile(
            r'<(?:div|p|span|li)[^>]*class="[^"]*offer[^"]*"[^>]*>(.*?)</(?:div|p|span|li)>',
            re.DOTALL | re.IGNORECASE,
        )
        for match in text_pattern.finditer(normalized):
            text = re.sub(r"<[^>]+>", " ", match.group(1))
            text = normalize_spaces(text)
            if len(text) < 10:
                continue
            offer_type = classify_offer_text(text)
            amount = extract_amount(text)
            offers.append({
                "title": "",
                "description": text[:300],
                "offer_type": offer_type,
                "amount": amount,
                "raw": text[:500],
            })

    return offers


def scrape_cardekho_offers(
    session: requests.Session,
    brand_slug: str,
    model_slug: str,
    brand_display: str,
    model_display: str,
    city: str = "new-delhi",
) -> List[Dict]:
    urls = [
        f"{BASE_CD}/{brand_slug}/{model_slug}/offers",
        f"{BASE_CD}/{brand_slug}/{model_slug}/offers-in-{city}",
        f"{BASE_CD}/{brand_slug}-{model_slug}-offers.htm",
    ]
    for url in urls:
        html = fetch_text(session, url)
        if html and len(html) > 3000:
            return parse_offers_from_html(html, brand_display, model_display)
    return []


def scrape_carwale_offers(
    session: requests.Session,
    brand_slug: str,
    model_slug: str,
    brand_display: str,
    model_display: str,
) -> List[Dict]:
    # CarWale uses different slug format
    cw_brand = brand_slug.replace("-", "")
    cw_model = model_slug
    urls = [
        f"{BASE_CW}/{cw_brand}-cars/{cw_model}/offers/",
        f"{BASE_CW}/{brand_slug}-cars/{model_slug}/offers/",
    ]
    for url in urls:
        html = fetch_text(session, url)
        if html and len(html) > 3000:
            offers = parse_offers_from_html(html, brand_display, model_display)
            # Tag source
            for o in offers:
                o["source"] = "carwale"
            return offers
    return []


# ---------------------------------------------------------------------------
# Main task per model
# ---------------------------------------------------------------------------

def scrape_model_offers(model_entry: dict) -> dict:
    brand_slug = model_entry["brand_slug"]
    model_slug = model_entry["model_slug"]
    brand_display = model_entry["brand_display"]
    model_display = model_entry["model_display"]

    session = build_session()

    # Scrape CarDekho first, then CarWale
    cd_offers = scrape_cardekho_offers(
        session, brand_slug, model_slug, brand_display, model_display
    )
    cw_offers = scrape_carwale_offers(
        session, brand_slug, model_slug, brand_display, model_display
    )

    # Merge, deduplicate by offer_type+amount
    all_offers = []
    seen = set()
    for offer in cd_offers + cw_offers:
        key = (offer.get("offer_type"), offer.get("amount"))
        if key not in seen:
            seen.add(key)
            all_offers.append(offer)

    # Compute total benefit
    total_benefit = sum(
        o["amount"] for o in all_offers if o.get("amount") and o["amount"] > 0
    )

    time.sleep(0.2 + random.uniform(0.1, 0.3))

    return {
        "brand": brand_display,
        "model": model_display,
        "brand_slug": brand_slug,
        "model_slug": model_slug,
        "month": CURRENT_MONTH,
        "year": CURRENT_YEAR,
        "month_label": TODAY.strftime("%B %Y"),
        "offers": all_offers,
        "offer_count": len(all_offers),
        "total_potential_benefit": total_benefit,
        "has_cash_discount": any(o["offer_type"] == "cash_discount" for o in all_offers),
        "has_exchange_bonus": any(o["offer_type"] == "exchange_bonus" for o in all_offers),
        "has_finance_offer": any(o["offer_type"] == "finance_offer" for o in all_offers),
        "source": "cardekho+carwale",
        "scraped_at": datetime.now().isoformat(),
        "last_updated": TODAY.isoformat(),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Monthly offers scraper")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit-models", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    workers = max(1, min(int(args.workers), 3))
    start = time.time()

    print("Building model universe...")
    universe = build_ncr_variant_universe(active_only=True)
    models = sorted(universe.values(), key=lambda x: (x["brand_slug"], x["model_slug"]))

    if args.limit_models > 0:
        models = models[:args.limit_models]

    print(f"Models: {len(models)} | Workers: {workers} | Dry run: {args.dry_run}")

    all_docs = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_model_offers, m): m for m in models}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Offers", unit="model"):
            try:
                result = future.result()
                all_docs.append(result)
            except Exception as e:
                print(f"Error: {e}")

    models_with_offers = sum(1 for d in all_docs if d["offer_count"] > 0)
    print(f"\nModels with offers found: {models_with_offers}/{len(all_docs)}")

    if args.dry_run:
        sample = next((d for d in all_docs if d["offer_count"] > 0), all_docs[0] if all_docs else {})
        print(json.dumps(sample, indent=2, default=str))
        return

    if not all_docs:
        print("No data to write.")
        return

    operations = []
    for doc in all_docs:
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
    print(f"Upserted: {result.upserted_count} new, {result.modified_count} updated")
    print(f"Runtime: {round(time.time() - start, 2)}s")


if __name__ == "__main__":
    main()