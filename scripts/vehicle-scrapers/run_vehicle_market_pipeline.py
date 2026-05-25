#!/usr/bin/env python3
"""
Run the market-facing vehicle refresh jobs:

1. Monthly offers scrape -> offers
2. Monthly sales scrape -> monthly_car_sales
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent


def run_step(name: str, command: Iterable[str]) -> None:
    command = list(command)
    print("\n" + "=" * 80)
    print(f"STEP: {name}")
    print("CMD : " + " ".join(command))
    print("=" * 80)

    start = time.time()
    subprocess.run(command, cwd=SCRIPT_DIR, check=True)
    print(f"\nDONE: {name} in {time.time() - start:.2f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run vehicle offers and monthly sales pipeline.")

    parser.add_argument("--skip-offers", action="store_true", help="Skip offers scraper.")
    parser.add_argument("--skip-sales", action="store_true", help="Skip monthly sales scraper.")

    parser.add_argument("--brand", default="", help="Single brand filter for offers.")
    parser.add_argument("--brands", default="", help="Comma-separated brand filter for offers.")
    parser.add_argument("--model", default="", help="Optional model filter for offers.")
    parser.add_argument("--target-month", type=int, default=0, help="Offer target month number.")
    parser.add_argument("--target-year", type=int, default=0, help="Offer target year.")
    parser.add_argument("--offers-debug", action="store_true", help="Enable offers debug logging.")
    parser.add_argument("--offers-dry-run", action="store_true", help="Run offers without Mongo writes.")
    parser.add_argument("--offers-limit-models", type=int, default=0, help="Optional offers model limit.")
    parser.add_argument("--min-offer-docs", type=int, default=1, help="Safety minimum before offers Mongo write.")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.skip_offers:
        command = [sys.executable, "offers_scrapper.py"]
        if args.brand:
            command += ["--brand", args.brand]
        if args.brands:
            command += ["--brands", args.brands]
        if args.model:
            command += ["--model", args.model]
        if args.target_month:
            command += ["--target-month", str(args.target_month)]
        if args.target_year:
            command += ["--target-year", str(args.target_year)]
        if args.offers_debug:
            command.append("--debug")
        if args.offers_dry_run:
            command.append("--dry-run")
        if args.offers_limit_models:
            command += ["--limit-models", str(args.offers_limit_models)]
        command += ["--min-offer-docs", str(args.min_offer_docs)]

        run_step("Refresh offers", command)

    if not args.skip_sales:
        run_step(
            "Refresh monthly sales",
            [sys.executable, "monthly_sales_scraper.py"],
        )

    print("\nVehicle market pipeline complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
