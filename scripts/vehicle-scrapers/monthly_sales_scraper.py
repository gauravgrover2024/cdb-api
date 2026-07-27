#!/usr/bin/env python3

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import monthly_sales_collection

# ============================================================
# CONFIG
# ============================================================

URL = "https://www.v3cars.com/popular-cars"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

# ============================================================
# HELPERS
# ============================================================

MONTH_NUMBERS = {
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


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_model_name(model: str) -> str:

    model = normalize_spaces(model)

    # ========================================================
    # REMOVE MARUTI CHANNEL PREFIXES ONLY
    # ========================================================

    model = model.replace("Maruti Arena ", "Maruti ")
    model = model.replace("Maruti Nexa ", "Maruti ")

    # ========================================================
    # FIX KNOWN PARSING ISSUES
    # ========================================================

    replacements = {
        "Mahindra XUV 7XO": "Mahindra XUV700",
        "Mahindra Scorpio-N": "Mahindra Scorpio N",
    }

    return replacements.get(model, model)


def parse_number(value: str):

    match = re.search(r"([\d,]+)", str(value or ""))

    if not match:
        return None

    return int(
        match.group(1).replace(",", "")
    )


def month_period(month_label: str):

    month_num = MONTH_NUMBERS.get(
        str(month_label or "").strip().lower()
    )

    if not month_num:
        return None

    now = datetime.now(timezone.utc)
    year = now.year

    # Sales data usually lags the calendar month. If a future month label
    # appears around a year boundary, it belongs to the previous year.
    if month_num > now.month:
        year -= 1

    return f"{year}-{month_num:02d}"


def build_session():

    session = requests.Session()

    session.headers.update(HEADERS)

    return session


def fetch_page(session):

    response = session.get(
        URL,
        timeout=(10, 30)
    )

    response.raise_for_status()

    return response.text


# ============================================================
# CARD PARSER
# ============================================================

def find_sales_cards(soup):

    legacy_cards = soup.find_all(
        "section",
        class_="card"
    )

    if legacy_cards:
        return legacy_cards

    cards = []
    seen = set()

    for heading in soup.find_all("h3"):

        heading_classes = heading.get("class") or []

        if not {"text-lg", "font-medium"}.issubset(set(heading_classes)):
            continue

        card = heading.find_parent(
            "div",
            class_=lambda value: value and "cursor-pointer" in value
        )

        if not card:
            continue

        text = card.get_text(" ", strip=True)

        if not re.search(r"\b[A-Za-z]+\s+Sales:\s*[\d,]+", text):
            continue

        key = id(card)

        if key in seen:
            continue

        seen.add(key)
        cards.append(card)

    return cards


def parse_card(card, rank: int):

    text = card.get_text(" ", strip=True)

    # ========================================================
    # MODEL NAME
    # ========================================================

    model = None

    heading = card.find("h3")

    if heading:
        candidate = normalize_spaces(
            heading.get_text(" ", strip=True)
        )

        if candidate:
            model = candidate

    links = card.find_all("a")

    if not model:

        for link in links:

            candidate = normalize_spaces(
                link.get_text(" ", strip=True)
            )

            if (
                len(candidate) > 3
                and len(candidate) < 60
                and "price" not in candidate.lower()
            ):
                model = candidate
                break

    if not model:
        return None

    model = normalize_model_name(model)

    # ========================================================
    # SALES
    # ========================================================

    sales = None

    sales_match = re.search(
        r"\b([A-Za-z]+)\s+Sales:\s*([\d,]+)",
        text,
        flags=re.IGNORECASE
    )

    sales_month_label = None

    if sales_match:

        sales_month_label = sales_match.group(1)

        sales = parse_number(
            sales_match.group(2)
        )

    if not sales:
        return None

    period = month_period(sales_month_label)

    if not period:
        return None

    # ========================================================
    # PREVIOUS MONTH SALES
    # ========================================================

    previous_sales = None
    previous_month_label = None

    sales_matches = list(re.finditer(
        r"\b([A-Za-z]+)\s+Sales:\s*([\d,]+)",
        text,
        flags=re.IGNORECASE
    ))

    if len(sales_matches) > 1:

        previous_month_label = sales_matches[1].group(1)

        previous_sales = parse_number(
            sales_matches[1].group(2)
        )

    # ========================================================
    # PERCENT CHANGE
    # ========================================================

    percent_change = None

    pct_match = re.search(
        r"%\s*Change:\s*([\-0-9\.]+)",
        text,
        flags=re.IGNORECASE
    )

    if pct_match:

        try:
            percent_change = float(
                pct_match.group(1)
            )
        except Exception:
            pass

    # ========================================================
    # BODY STYLE
    # ========================================================

    body_style = None

    body_match = re.search(
        r"Body Style:\s+([A-Za-z]+)",
        text,
        flags=re.IGNORECASE
    )

    if body_match:

        body_style = normalize_spaces(
            body_match.group(1)
        )

    # ========================================================
    # SEGMENT
    # ========================================================

    segment = None

    segment_match = re.search(
        r"Segment:\s+([A-Za-z0-9\-]+)\s*-?\s*segment",
        text,
        flags=re.IGNORECASE
    )

    if segment_match:

        segment = normalize_spaces(
            segment_match.group(1)
        )

    # ========================================================
    # PRICE RANGE
    # ========================================================

    price_range = None

    price_match = re.search(
        r"₹\s*([\d\.]+)\s*-\s*₹\s*([\d\.]+)\s*L",
        text,
        flags=re.IGNORECASE
    )

    if price_match:

        price_range = normalize_spaces(
            f"{price_match.group(1)} - {price_match.group(2)} L"
        )

    rank_match = re.search(r"\b(\d+)\s+Rank\b", text, flags=re.IGNORECASE)

    if rank_match:
        rank = int(rank_match.group(1))

    # ========================================================
    # DOCUMENT
    # ========================================================

    return {
        "month": period,

        "salesMonthLabel": sales_month_label,

        "rank": rank,

        "model": model,

        "sales": sales,

        "previousMonthSales": previous_sales,

        "previousMonthLabel": previous_month_label,

        "percentChange": percent_change,

        "bodyStyle": body_style,

        "segment": segment,

        "priceRange": price_range,

        "source": "v3cars",

        "sourceUrl": URL,

        "scrapedAt": datetime.now(timezone.utc),
    }


# ============================================================
# MAIN
# ============================================================

def main():

    start = time.time()

    print("\n===== V3CARS MONTHLY SALES SCRAPER =====")

    session = build_session()

    html = fetch_page(session)

    print(f"HTML Length: {len(html)}")

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    cards = find_sales_cards(soup)

    print(f"Cards found: {len(cards)}")

    results = []

    # ========================================================
    # PARSE CARDS
    # ========================================================

    with ThreadPoolExecutor(max_workers=3) as executor:

        futures = []

        for idx, card in enumerate(cards, start=1):

            futures.append(
                executor.submit(
                    parse_card,
                    card,
                    idx
                )
            )

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Parsing cards"
        ):

            result = future.result()

            if result:
                results.append(result)

    # ========================================================
    # SORT
    # ========================================================

    results.sort(
        key=lambda x: x["sales"],
        reverse=True
    )

    # ========================================================
    # BULK WRITE
    # ========================================================

    operations = []

    for item in results:

        operations.append(
            UpdateOne(
                {
                    "month": item["month"],
                    "model": item["model"]
                },
                {
                    "$set": item
                },
                upsert=True
            )
        )

    if operations:

        monthly_sales_collection.bulk_write(
            operations,
            ordered=False
        )

    # ========================================================
    # PRINT RESULTS
    # ========================================================

    print("\n===== TOP CARS =====")

    for row in results[:25]:

        print(
            f"#{row['rank']:02d} | "
            f"{row['model']:<30} | "
            f"{row['sales']}"
        )

    runtime = time.time() - start

    # ========================================================
    # SUMMARY
    # ========================================================

    print("\n===== SUMMARY =====")

    print(f"Cars Parsed : {len(results)}")
    print(f"Mongo Writes: {len(operations)}")
    print(f"Runtime     : {runtime:.2f}s")

    print("\n===== SCRAPE COMPLETE =====")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()
