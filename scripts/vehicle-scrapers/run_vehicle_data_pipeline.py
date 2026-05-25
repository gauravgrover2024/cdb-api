#!/usr/bin/env python3
"""
Run the core vehicle data refresh in the correct order:

1. Cardekho NCR price scrape -> vehicles + price_history
2. Variant feature enrichment -> vehicle_features
3. Color/media master pipeline -> vehicle_colors_v2

Color/media processing is changed-only by default. The master pipeline reuses
existing assets when the source image hash and frame metadata are already
present, so background removal and R2 uploads only run for new/changed assets.
Pass --force-colors only when you intentionally want to regenerate media.
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
    parser = argparse.ArgumentParser(description="Run vehicle prices, features, and color media pipeline.")

    parser.add_argument("--skip-prices", action="store_true", help="Skip Cardekho NCR price update.")
    parser.add_argument("--skip-variants", action="store_true", help="Skip variant feature enrichment.")
    parser.add_argument("--skip-colors", action="store_true", help="Skip color/media master pipeline.")

    parser.add_argument("--variant-workers", type=int, default=3, help="Workers for variant enrichment.")
    parser.add_argument("--variant-limit-models", type=int, default=0, help="Optional model limit for variant enrichment.")
    parser.add_argument(
        "--include-discontinued",
        action="store_true",
        help="Include discontinued variants in enrichment scope where supported.",
    )

    parser.add_argument("--brand", default="", help="Optional brand slug/name for color master pipeline.")
    parser.add_argument("--model", default="", help="Optional model slug/name for color master pipeline.")
    parser.add_argument("--force-colors", action="store_true", help="Regenerate color media even if source image is unchanged.")
    parser.add_argument("--allow-partial-colors", action="store_true", help="Allow partial color writes from color master pipeline.")
    parser.add_argument("--skip-color-upload", action="store_true", help="Skip R2 upload in color master pipeline.")
    parser.add_argument("--skip-color-mongo", action="store_true", help="Skip Mongo writes in color master pipeline.")

    parser.add_argument(
        "--dry-run-enrichment",
        action="store_true",
        help=(
            "Dry-run enrichment steps only. Prices are skipped because the price "
            "scraper does not currently expose a dry-run mode."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.skip_color_upload and not args.skip_color_mongo:
        raise SystemExit("--skip-color-upload must be used with --skip-color-mongo to avoid writing non-uploaded URLs.")

    if args.dry_run_enrichment and not args.skip_prices:
        print("Dry-run enrichment requested; skipping prices because the price scraper writes directly.")
        args.skip_prices = True

    if not args.skip_prices:
        run_step(
            "Update NCR prices",
            [sys.executable, "cardekho_ncr_scraper_updated.py"],
        )

    if not args.skip_variants:
        command = [
            sys.executable,
            "variant_enrichment_ncr_v2.py",
            "--workers",
            str(args.variant_workers),
        ]
        if args.variant_limit_models:
            command += ["--limit-models", str(args.variant_limit_models)]
        if args.include_discontinued:
            command.append("--include-discontinued")
        if args.dry_run_enrichment:
            command.append("--dry-run")

        run_step("Enrich variant features", command)

    if not args.skip_colors:
        command = [sys.executable, "vehicle_color_master_pipeline.py"]
        if args.brand:
            command += ["--brand", args.brand]
        if args.model:
            command += ["--model", args.model]
        if args.force_colors:
            command.append("--force")
        if args.allow_partial_colors:
            command.append("--allow-partial")
        if args.skip_color_upload or args.dry_run_enrichment:
            command.append("--skip-upload")
        if args.skip_color_mongo or args.dry_run_enrichment:
            command.append("--skip-mongo")

        run_step("Refresh color media master", command)

    print("\nVehicle data pipeline complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
