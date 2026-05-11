#!/usr/bin/env python3
"""Bulk normalize vehicle images and write normalized URL fields."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
NORMALIZER = SCRIPT_DIR / "normalize_car_image_rembg.py"


def sha1_text(value: str) -> str:
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def slugify(value: str) -> str:
    import re

    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "car-image"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_manifest(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"input manifest not found: {path}")

    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    if path.suffix.lower() in {".json", ".jsonl"}:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []

        if path.suffix.lower() == ".jsonl":
            rows = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
            return rows

        payload = json.loads(text)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            records = payload.get("records")
            if isinstance(records, list):
                return records
        raise ValueError("JSON manifest must be array or object with records array")

    raise ValueError("input manifest must be .csv, .json, or .jsonl")


def first_url(record: dict[str, Any]) -> str:
    keys = [
        "sourceImageUrl",
        "imageUrl",
        "image_url",
        "carImageUrl",
        "car_image_url",
        "originalImageUrl",
    ]

    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


@dataclass
class ImageRef:
    source_url: str
    source_key: str
    slug: str
    doc_id: Any
    collection: str
    path: str
    label: str


def build_slug(record: dict[str, Any], source_url: str) -> str:
    parts = [
        record.get("brand") or record.get("make"),
        record.get("model"),
        record.get("variant"),
        record.get("color") or record.get("color_name"),
    ]
    title = "-".join(str(part).strip() for part in parts if part)
    base = slugify(title) if title else "car-image"
    return f"{base}-{sha1_text(source_url)[:10]}"


def discover_image_refs(record: dict[str, Any], collection: str) -> list[ImageRef]:
    refs: list[ImageRef] = []
    doc_id = record.get("_id") or record.get("id")

    def add(url: str, path: str, label: str) -> None:
        if not url:
            return
        key = sha1_text(url)
        refs.append(
            ImageRef(
                source_url=url,
                source_key=key,
                slug=build_slug(record, url),
                doc_id=doc_id,
                collection=collection,
                path=path,
                label=label,
            )
        )

    top_url = first_url(record)
    add(top_url, "", "top-level")

    colors = record.get("colors")
    if isinstance(colors, list):
        for index, color in enumerate(colors):
            if not isinstance(color, dict):
                continue
            url = first_url(color)
            add(url, f"colors.{index}", f"color[{index}]")

    variants = record.get("variants")
    if isinstance(variants, list):
        for index, variant in enumerate(variants):
            if not isinstance(variant, dict):
                continue
            url = first_url(variant)
            add(url, f"variants.{index}", f"variant[{index}]")

    return refs


def call_normalizer(
    ref: ImageRef,
    out_dir: Path,
    public_url_prefix: str,
    model: str,
    preview: bool,
    keep_raw: bool,
    force: bool,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(NORMALIZER),
        "--input",
        ref.source_url,
        "--slug",
        ref.slug,
        "--out-dir",
        str(out_dir),
        "--public-url-prefix",
        public_url_prefix,
        "--model",
        model,
        "--json-only",
    ]

    if preview:
        command.append("--preview")
    if keep_raw:
        command.append("--keep-raw")
    if force:
        command.append("--force")

    process = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )

    raw = (process.stdout or process.stderr or "").strip()

    if process.returncode != 0:
        return {
            "ok": False,
            "error": raw or f"normalizer failed with code {process.returncode}",
        }

    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("normalizer output is not JSON object")
        return payload
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse normalizer output: {exc}",
            "raw": raw,
        }


def parse_query_json(query_json: str) -> dict[str, Any]:
    if not query_json:
        return {}
    payload = json.loads(query_json)
    if not isinstance(payload, dict):
        raise ValueError("query-json must be a JSON object")
    return payload


def load_db_records(args) -> list[dict[str, Any]]:
    import certifi
    from pymongo import MongoClient

    if not args.mongo_uri:
        raise ValueError("--mongo-uri is required when --input-manifest is not used")

    client = MongoClient(
        args.mongo_uri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
    )
    db = client[args.db_name]
    collection = db[args.collection]

    query = parse_query_json(args.query_json)
    cursor = collection.find(query)

    if args.limit and args.limit > 0:
        cursor = cursor.limit(args.limit)

    records = list(cursor)
    client.close()
    return records


def update_doc_image_fields(collection, doc_id, ref: ImageRef, normalize_result: dict[str, Any]) -> bool:
    clean_webp = normalize_result.get("cleanWebpUrl") or ""
    clean_png = normalize_result.get("cleanPngUrl") or ""
    if not clean_webp and not clean_png:
        return False

    normalized_url = clean_webp or clean_png

    if not ref.path:
        update = {
            "$set": {
                "sourceImageUrl": ref.source_url,
                "normalizedImageUrl": normalized_url,
                "normalizedImagePngUrl": clean_png,
                "imageBackgroundRemoved": True,
                "imageProcessingMethod": "rembg",
                "imageSourceKey": ref.source_key,
                "imageProcessedAt": datetime.now(timezone.utc),
            }
        }
        collection.update_one({"_id": doc_id}, update)
        return True

    if ref.path.startswith("colors."):
        index = int(ref.path.split(".")[1])
        doc = collection.find_one({"_id": doc_id}, {"colors": 1})
        colors = list(doc.get("colors") or []) if doc else []
        if index >= len(colors) or not isinstance(colors[index], dict):
            return False
        entry = dict(colors[index])
        entry["sourceImageUrl"] = ref.source_url
        entry["normalizedImageUrl"] = normalized_url
        entry["normalizedImagePngUrl"] = clean_png
        entry["imageBackgroundRemoved"] = True
        entry["imageProcessingMethod"] = "rembg"
        entry["imageSourceKey"] = ref.source_key
        entry["imageProcessedAt"] = datetime.now(timezone.utc)
        colors[index] = entry
        collection.update_one({"_id": doc_id}, {"$set": {"colors": colors}})
        return True

    if ref.path.startswith("variants."):
        index = int(ref.path.split(".")[1])
        doc = collection.find_one({"_id": doc_id}, {"variants": 1})
        variants = list(doc.get("variants") or []) if doc else []
        if index >= len(variants) or not isinstance(variants[index], dict):
            return False
        entry = dict(variants[index])
        entry["sourceImageUrl"] = ref.source_url
        entry["normalizedImageUrl"] = normalized_url
        entry["normalizedImagePngUrl"] = clean_png
        entry["imageBackgroundRemoved"] = True
        entry["imageProcessingMethod"] = "rembg"
        entry["imageSourceKey"] = ref.source_key
        entry["imageProcessedAt"] = datetime.now(timezone.utc)
        variants[index] = entry
        collection.update_one({"_id": doc_id}, {"$set": {"variants": variants}})
        return True

    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bulk normalize vehicle images")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", ""))
    parser.add_argument("--db-name", default=os.getenv("MONGO_DB_NAME", "cdrive"))
    parser.add_argument("--collection", default="vehicles")
    parser.add_argument("--query-json", default="{}")
    parser.add_argument("--input-manifest", default="")
    parser.add_argument("--out-dir", default="../../public/media/car-images/normalized")
    parser.add_argument("--public-url-prefix", default="/media/car-images/normalized")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--model", default="u2netp", choices=["u2netp", "u2net", "isnet-general-use"])
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--keep-raw", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    out_dir = (SCRIPT_DIR / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "normalized-car-images.manifest.jsonl"

    if args.input_manifest:
        records = read_manifest(Path(args.input_manifest).expanduser().resolve())
        collection_name = "manifest"
    else:
        records = load_db_records(args)
        collection_name = args.collection

    refs: list[ImageRef] = []
    for record in records:
        refs.extend(discover_image_refs(record, collection_name))

    by_key: dict[str, ImageRef] = {}
    for ref in refs:
        by_key.setdefault(ref.source_key, ref)

    unique_refs = list(by_key.values())
    if args.limit and args.limit > 0:
        unique_refs = unique_refs[: args.limit]

    total = len(unique_refs)
    processed = 0
    skipped = 0
    failed = 0
    rembg_count = 0
    fallback_count = 0

    collection = None
    client = None
    if not args.input_manifest and not args.dry_run:
        import certifi
        from pymongo import MongoClient

        client = MongoClient(
            args.mongo_uri,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=30000,
        )
        collection = client[args.db_name][args.collection]

    workers = max(1, min(int(args.workers or 1), 4))

    def run_one(index: int, ref: ImageRef):
        record = {
            "index": index,
            "total": total,
            "sourceKey": ref.source_key,
            "sourceUrl": ref.source_url,
            "slug": ref.slug,
            "collection": ref.collection,
            "docId": str(ref.doc_id),
            "path": ref.path,
            "label": ref.label,
            "processedAt": now_iso(),
        }

        if args.dry_run:
            record.update({"status": "dry_run"})
            return record

        if args.skip_existing and collection is not None and ref.doc_id is not None:
            doc = collection.find_one({"_id": ref.doc_id}, {"normalizedImageUrl": 1})
            if doc and doc.get("normalizedImageUrl") and not args.force:
                record.update({"status": "skipped", "reason": "db_has_normalized"})
                return record

        result = call_normalizer(
            ref=ref,
            out_dir=out_dir,
            public_url_prefix=args.public_url_prefix,
            model=args.model,
            preview=args.preview,
            keep_raw=args.keep_raw,
            force=args.force,
        )

        if not result.get("ok"):
            record.update({"status": "failed", "error": result.get("error", "unknown")})
            return record

        updated = False
        if collection is not None and ref.doc_id is not None:
            updated = update_doc_image_fields(collection, ref.doc_id, ref, result)

        record.update(
            {
                "status": "processed",
                "method": result.get("method"),
                "cleanWebpUrl": result.get("cleanWebpUrl"),
                "cleanPngUrl": result.get("cleanPngUrl"),
                "updatedDb": bool(updated),
                "skipped": bool(result.get("skipped")),
            }
        )
        return record

    with manifest_path.open("a", encoding="utf-8") as manifest_file:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(run_one, index + 1, ref): (index + 1, ref)
                for index, ref in enumerate(unique_refs)
            }

            for future in as_completed(futures):
                index, ref = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "index": index,
                        "total": total,
                        "sourceKey": ref.source_key,
                        "sourceUrl": ref.source_url,
                        "slug": ref.slug,
                        "status": "failed",
                        "error": str(exc),
                        "processedAt": now_iso(),
                    }

                manifest_file.write(json.dumps(result, ensure_ascii=False) + "\n")
                manifest_file.flush()

                status = result.get("status")
                if status in {"processed", "dry_run"}:
                    processed += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    failed += 1

                method = result.get("method", "")
                if method == "rembg":
                    rembg_count += 1
                if method == "white-bg-fallback":
                    fallback_count += 1

                reason = result.get("reason") or result.get("error") or method or status
                print(f"[{index}/{total}] {ref.slug} - {reason}")

    if client is not None:
        client.close()

    summary = {
        "total": total,
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "rembg": rembg_count,
        "fallback": fallback_count,
        "outputDir": str(out_dir),
        "manifestPath": str(manifest_path),
    }

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
