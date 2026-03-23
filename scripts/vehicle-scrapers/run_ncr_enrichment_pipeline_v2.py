#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run NCR scraper + v2 feature/color enrichment pipeline"
    )
    parser.add_argument("--workers", type=int, default=3, help="Max workers for enrichment steps (hard capped at 3)")
    parser.add_argument("--limit-models", type=int, default=0, help="Optional model limit for testing")
    parser.add_argument(
        "--limit-variants-per-model",
        type=int,
        default=0,
        help="Optional per-model variant limit for feature enrichment testing",
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry-run enrichment steps (no DB writes)")
    parser.add_argument(
        "--include-discontinued",
        action="store_true",
        help="Include discontinued variants from vehicles collection in enrichment scope",
    )
    parser.add_argument(
        "--skip-prices",
        action="store_true",
        help="Skip running cardekho_ncr_scraper_updated.py",
    )
    return parser.parse_args()


def clamp_workers(value: int) -> int:
    return max(1, min(int(value or 1), 3))


def run_step(step_name: str, cmd: list, cwd: Path) -> None:
    print(f"\n>>> {step_name}")
    print(" ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)


def main() -> None:
    args = parse_args()
    workers = clamp_workers(args.workers)
    base_dir = Path(__file__).resolve().parent

    try:
        if not args.skip_prices:
            if args.dry_run:
                print("[note] --dry-run set, skipping prices step because NCR price scraper has no dry-run mode.")
            else:
                run_step(
                    "NCR Price Scraper",
                    [sys.executable, "cardekho_ncr_scraper_updated.py"],
                    base_dir,
                )

        feature_cmd = [
            sys.executable,
            "variant_enrichment_ncr_v2.py",
            "--workers",
            str(workers),
        ]
        if args.limit_models > 0:
            feature_cmd += ["--limit-models", str(args.limit_models)]
        if args.limit_variants_per_model > 0:
            feature_cmd += ["--limit-variants-per-model", str(args.limit_variants_per_model)]
        if args.include_discontinued:
            feature_cmd.append("--include-discontinued")
        if args.dry_run:
            feature_cmd.append("--dry-run")

        run_step("NCR Feature Enrichment v2", feature_cmd, base_dir)

        color_cmd = [
            sys.executable,
            "color_enrichment_ncr_v2.py",
            "--workers",
            str(workers),
        ]
        if args.limit_models > 0:
            color_cmd += ["--limit-models", str(args.limit_models)]
        if args.include_discontinued:
            color_cmd.append("--include-discontinued")
        if args.dry_run:
            color_cmd.append("--dry-run")

        run_step("NCR Color Enrichment v2", color_cmd, base_dir)

        print("\nPipeline completed successfully.")
    except subprocess.CalledProcessError as exc:
        print(f"\nPipeline failed at step with exit code {exc.returncode}")
        sys.exit(exc.returncode)


if __name__ == "__main__":
    main()
