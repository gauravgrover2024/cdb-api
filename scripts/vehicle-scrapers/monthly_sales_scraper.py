#!/usr/bin/env python3

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from pymongo import UpdateOne
from tqdm import tqdm

from mongo_connection import monthly_sales_collection

# ============================================================
# CONFIG
# ============================================================

URL = "https://www.v3cars.com/popular-cars"

MONTH = "2026-04"

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
        "Maruti Victoris": "Maruti Grand Vitara",
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

def parse_card(card, rank: int):

    text = card.get_text(" ", strip=True)

    # ========================================================
    # MODEL NAME
    # ========================================================

    model = None

    links = card.find_all("a")

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
        r"April\s+Sales\s+([\d,]+)",
        text,
        flags=re.IGNORECASE
    )

    if sales_match:

        sales = parse_number(
            sales_match.group(1)
        )

    if not sales:
        return None

    # ========================================================
    # PREVIOUS MONTH SALES
    # ========================================================

    previous_sales = None

    previous_match = re.search(
        r"March\s+Sales\s+([\d,]+)",
        text,
        flags=re.IGNORECASE
    )

    if previous_match:

        previous_sales = parse_number(
            previous_match.group(1)
        )

    # ========================================================
    # PERCENT CHANGE
    # ========================================================

    percent_change = None

    pct_match = re.search(
        r"%\s*Change\s*([\-0-9\.]+)",
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
        r"Body Style\s+([A-Za-z]+)",
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
        r"Segment\s+([A-Za-z0-9\-]+)",
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
        r"₹\s*([\d\.\-\s]+lakh)",
        text,
        flags=re.IGNORECASE
    )

    if price_match:

        price_range = normalize_spaces(
            price_match.group(1)
        )

    # ========================================================
    # DOCUMENT
    # ========================================================

    return {
        "month": MONTH,

        "rank": rank,

        "model": model,

        "sales": sales,

        "previousMonthSales": previous_sales,

        "percentChange": percent_change,

        "bodyStyle": body_style,

        "segment": segment,

        "priceRange": price_range,

        "source": "v3cars",

        "sourceUrl": URL,

        "scrapedAt": datetime.utcnow(),
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

    cards = soup.find_all(
        "section",
        class_="card"
    )

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