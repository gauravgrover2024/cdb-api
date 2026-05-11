#!/usr/bin/env python3
"""
Bulk ACI Assist car image normalizer with MongoDB write-back.

This script:
  1. Finds image URLs in Mongo documents.
  2. Calls normalize_car_image_rembg.process_single_image().
  3. Writes stagedImageUrl / normalizedImageUrl back to the same document path.
  4. Writes a JSONL manifest.
  5. Skips duplicates by SHA1 source URL.
  6. Safely cleans stale old cutout fields when new run uses stage-only mode.

Important:
  - original image_url / imageUrl is untouched
  - sourceImageUrl is set for compatibility
  - stagedImageUrl becomes primary
  - normalizedImageUrl / normalizedImagePngUrl are only kept when cutout exists
  - imageBackgroundRemoved becomes False in stage-only mode
"""

from __future__ import annotations

import argparse
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from normalize_car_image_rembg import process_single_image


SOURCE_IMAGE_KEYS = {
    "image",
    "imageUrl",
    "imageURL",
    "image_url",
    "sourceImageUrl",
    "originalImageUrl",
    "thumbnail",
    "thumbnailUrl",
    "thumb",
    "src",
    "url",
    "modelImage",
    "heroImage",
    "primaryImage",
    "mainImage",
    "exteriorImage",
    "carImage",
    "colorImage",
}

OUTPUT_IMAGE_KEYS = {
    "stagedImageUrl",
    "normalizedImageUrl",
    "normalizedImagePngUrl",
    "cleanImageUrl",
    "cleanWebpUrl",
    "cleanPngUrl",
    "previewUrl",
    "imageSourceKey",
    "imageProcessingMethod",
    "imageModeUsed",
    "imageProcessedAt",
    "imageQualityWarnings",
    "isStudioBackground",
    "imageBackgroundRemoved",
}

IMAGE_HOST_HINTS = [
    "stimg.cardekho.com",
    "imgd.cardekho.com",
    "imgct.cardekho.com",
    "imgd.aeplcdn.com",
    "images.carandbike.com",
    "static.autox.com",
    "images.91wheels.com",
    "img-zigwheels.com",
]


@dataclass
class ImageEntry:
    doc_id: Any
    doc: dict[str, Any]
    url: str
    source_key: str
    parent_path: list[Any]
    source_field: str
    parent_snapshot: dict[str, Any]
    existing_staged: Optional[str]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha1_text(value: str) -> str:
    import hashlib

    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def slugify(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "car-image"


def looks_like_remote_image_url(value: str) -> bool:
    if not isinstance(value, str):
        return False

    url = value.strip()
    if not url.startswith(("http://", "https://")):
        return False

    lower = url.lower()

    if any(host in lower for host in IMAGE_HOST_HINTS):
        return True

    base = lower.split("?")[0]
    return any(base.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".avif"])


def safe_json_loads(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return fallback


def mongo_path(path: list[Any], field: str) -> str:
    return ".".join(str(p) for p in [*path, field])


def iter_image_entries_in_node(
    node: Any,
    doc: dict[str, Any],
    doc_id: Any,
    path: Optional[list[Any]] = None,
) -> Iterable[ImageEntry]:
    path = path or []

    if isinstance(node, dict):
        for key, value in node.items():
            if key in OUTPUT_IMAGE_KEYS:
                continue

            if key in SOURCE_IMAGE_KEYS and isinstance(value, str) and looks_like_remote_image_url(value):
                existing_staged = node.get("stagedImageUrl") or node.get("normalizedImageUrl")

                yield ImageEntry(
                    doc_id=doc_id,
                    doc=doc,
                    url=value,
                    source_key=sha1_text(value),
                    parent_path=path,
                    source_field=key,
                    parent_snapshot=node,
                    existing_staged=existing_staged,
                )

            if isinstance(value, (dict, list)):
                yield from iter_image_entries_in_node(value, doc, doc_id, [*path, key])

    elif isinstance(node, list):
        for index, item in enumerate(node):
            if isinstance(item, (dict, list)):
                yield from iter_image_entries_in_node(item, doc, doc_id, [*path, index])


def build_slug(entry: ImageEntry) -> str:
    doc = entry.doc or {}
    parent = entry.parent_snapshot or {}

    parts = [
        doc.get("brand"),
        doc.get("make"),
        doc.get("model"),
        doc.get("modelName"),
        parent.get("brand"),
        parent.get("make"),
        parent.get("model"),
        parent.get("variant"),
        parent.get("variantName"),
        parent.get("name"),
        parent.get("color"),
        parent.get("color_name"),
        parent.get("colorName"),
        parent.get("title"),
        entry.source_key[:10],
    ]

    return slugify(" ".join(str(p) for p in parts if p))


def build_update_ops(entry: ImageEntry, result: dict[str, Any]) -> dict[str, Any]:
    parent_path = entry.parent_path
    source_url = entry.url

    set_ops: dict[str, Any] = {}
    unset_ops: dict[str, Any] = {}

    staged = result.get("stagedImageUrl")
    clean_webp = result.get("cleanWebpUrl")
    clean_png = result.get("cleanPngUrl")

    if staged:
        set_ops[mongo_path(parent_path, "stagedImageUrl")] = staged

    if clean_webp:
        set_ops[mongo_path(parent_path, "normalizedImageUrl")] = clean_webp
    else:
        unset_ops[mongo_path(parent_path, "normalizedImageUrl")] = ""

    if clean_png:
        set_ops[mongo_path(parent_path, "normalizedImagePngUrl")] = clean_png
    else:
        unset_ops[mongo_path(parent_path, "normalizedImagePngUrl")] = ""

    # Compatibility with the old write pattern.
    set_ops[mongo_path(parent_path, "sourceImageUrl")] = source_url

    set_ops[mongo_path(parent_path, "imageSourceKey")] = result.get("sourceKey") or entry.source_key
    set_ops[mongo_path(parent_path, "imageProcessingMethod")] = result.get("method")
    set_ops[mongo_path(parent_path, "imageModeUsed")] = result.get("modeUsed")
    set_ops[mongo_path(parent_path, "imageProcessedAt")] = datetime.now(timezone.utc)
    set_ops[mongo_path(parent_path, "imageQualityWarnings")] = result.get("qualityWarnings") or []
    set_ops[mongo_path(parent_path, "isStudioBackground")] = result.get("isStudioBackground")

    # Old field compatibility:
    # True only if we actually generated transparent/clean cutout files.
    background_removed = bool(clean_webp or clean_png)
    set_ops[mongo_path(parent_path, "imageBackgroundRemoved")] = background_removed

    update_ops: dict[str, Any] = {}

    if set_ops:
        update_ops["$set"] = set_ops

    if unset_ops:
        update_ops["$unset"] = unset_ops

    return update_ops


def write_manifest_line(manifest_path: Path, payload: dict[str, Any], lock: threading.Lock) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, default=str)

    with lock:
        with manifest_path.open("a", encoding="utf-8") as file:
            file.write(line + "\n")


def process_group(
    *,
    source_key: str,
    entries: list[ImageEntry],
    collection: Any,
    args: argparse.Namespace,
    manifest_path: Path,
    manifest_lock: threading.Lock,
    index: int,
    total: int,
) -> dict[str, Any]:
    first = entries[0]
    slug = build_slug(first)

    if args.skip_existing and not args.force and all(entry.existing_staged for entry in entries):
        payload = {
            "ok": True,
            "status": "skipped",
            "reason": "all_entries_already_have_staged_or_normalized_image",
            "sourceKey": source_key,
            "source": first.url,
            "slug": slug,
            "count": len(entries),
            "processedAt": now_iso(),
        }
        print(f"[{index}/{total}] skipped existing - {slug}")
        write_manifest_line(manifest_path, payload, manifest_lock)
        return payload

    if args.dry_run:
        payload = {
            "ok": True,
            "status": "dry_run",
            "sourceKey": source_key,
            "source": first.url,
            "slug": slug,
            "count": len(entries),
            "docIds": [str(entry.doc_id) for entry in entries],
            "plannedUpdates": [
                {
                    "docId": str(entry.doc_id),
                    "parentPath": ".".join(str(p) for p in entry.parent_path) or "<root>",
                    "sourceField": entry.source_field,
                }
                for entry in entries
            ],
            "processedAt": now_iso(),
        }
        print(f"[{index}/{total}] dry-run - {slug}")
        write_manifest_line(manifest_path, payload, manifest_lock)
        return payload

    result = process_single_image(
        input_source=first.url,
        slug=slug,
        out_dir=Path(args.out_dir),
        public_url_prefix=args.public_url_prefix,
        mode=args.mode,
        model=args.model,
        max_width=args.max_width,
        canvas_ratio=args.canvas_ratio,
        force=args.force,
        preview=args.preview,
        keep_raw=args.keep_raw,
        allow_fallback_cutout=args.allow_fallback_cutout,
    )

    if not result.get("ok"):
        payload = {
            "ok": False,
            "status": "failed",
            "sourceKey": source_key,
            "source": first.url,
            "slug": slug,
            "error": result.get("error"),
            "result": result,
            "processedAt": now_iso(),
        }
        print(f"[{index}/{total}] failed - {slug} - {result.get('error')}")
        write_manifest_line(manifest_path, payload, manifest_lock)
        return payload

    matched = 0
    modified = 0

    for entry in entries:
        update_ops = build_update_ops(entry, result)

        if update_ops:
            response = collection.update_one(
                {"_id": entry.doc_id},
                update_ops,
            )
            matched += int(response.matched_count or 0)
            modified += int(response.modified_count or 0)

    payload = {
        "ok": True,
        "status": "processed",
        "sourceKey": source_key,
        "source": first.url,
        "slug": slug,
        "count": len(entries),
        "matched": matched,
        "modified": modified,
        "modeUsed": result.get("modeUsed"),
        "method": result.get("method"),
        "stagedImageUrl": result.get("stagedImageUrl"),
        "normalizedImageUrl": result.get("cleanWebpUrl"),
        "processedAt": now_iso(),
    }

    print(f"[{index}/{total}] processed - {slug} - {result.get('method')} - updated {modified}")
    write_manifest_line(manifest_path, payload, manifest_lock)
    return payload


def build_mongo_client(mongo_uri: str):
    """
    Atlas TLS fix for Python/PyMongo on macOS/Python venvs.

    Uses certifi CA bundle instead of disabling certificate verification.
    """
    try:
        from pymongo import MongoClient
    except Exception as exc:
        raise SystemExit(
            "pymongo is required. Install with: pip install pymongo\n"
            f"Original error: {exc}"
        )

    try:
        import certifi
    except Exception as exc:
        raise SystemExit(
            "certifi is required for MongoDB TLS verification.\n"
            "Install with: pip install certifi\n"
            f"Original error: {exc}"
        )

    return MongoClient(
        mongo_uri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk normalize car images and write URLs back to MongoDB")

    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI"), help="MongoDB URI")
    parser.add_argument("--db-name", required=True, help="MongoDB database name")
    parser.add_argument("--collection", default="vehicles", help="MongoDB collection name")
    parser.add_argument("--query-json", default="{}", help="Mongo query JSON")
    parser.add_argument("--limit", type=int, default=0, help="Limit image URL entries, 0 means no limit")
    parser.add_argument("--dry-run", action="store_true", help="Discover planned updates but do not write files or DB")
    parser.add_argument("--force", action="store_true", help="Reprocess existing images")
    parser.add_argument("--skip-existing", action="store_true", help="Skip entries that already have staged/normalized image")
    parser.add_argument("--workers", type=int, default=1, help="Keep 1 by default; rembg is memory heavy")

    parser.add_argument("--out-dir", default="../../public/media/car-images/normalized", help="Output directory")
    parser.add_argument("--public-url-prefix", default="/media/car-images/normalized", help="Public URL prefix")
    parser.add_argument("--mode", default="auto", choices=["auto", "stage-only", "cutout"])
    parser.add_argument(
        "--model",
        default="isnet-general-use",
        choices=["birefnet-general", "birefnet-general-lite", "isnet-general-use", "u2net", "u2netp"],
    )
    parser.add_argument("--max-width", type=int, default=2200)
    parser.add_argument("--canvas-ratio", default="16:9")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--keep-raw", action="store_true")
    parser.add_argument("--allow-fallback-cutout", action="store_true")
    parser.add_argument("--manifest", default=None, help="JSONL manifest path")

    args = parser.parse_args()

    if not args.mongo_uri:
        raise SystemExit("Missing --mongo-uri or MONGO_URI environment variable")

    query = safe_json_loads(args.query_json, None)

    if not isinstance(query, dict):
        raise SystemExit("Invalid --query-json. It must be a JSON object.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = Path(args.manifest) if args.manifest else out_dir / "normalized-car-images.manifest.jsonl"
    manifest_lock = threading.Lock()

    client = build_mongo_client(args.mongo_uri)
    db = client[args.db_name]
    collection = db[args.collection]

    print(f"DB: {args.db_name}.{args.collection}")
    print(f"Query: {json.dumps(query, ensure_ascii=False)}")
    print(f"Dry run: {args.dry_run}")
    print(f"Force: {args.force}")
    print(f"Mode: {args.mode}")
    print(f"Model: {args.model}")
    print(f"Output: {out_dir}")
    print(f"Manifest: {manifest_path}")

    entries: list[ImageEntry] = []

    for doc in collection.find(query):
        doc_id = doc.get("_id")

        for entry in iter_image_entries_in_node(doc, doc, doc_id):
            entries.append(entry)

            if args.limit and len(entries) >= args.limit:
                break

        if args.limit and len(entries) >= args.limit:
            break

    if not entries:
        print("No image URLs found.")
        return

    grouped: dict[str, list[ImageEntry]] = {}

    for entry in entries:
        grouped.setdefault(entry.source_key, []).append(entry)

    groups = list(grouped.items())

    print(f"Image URL entries found: {len(entries)}")
    print(f"Unique source URLs: {len(groups)}")

    summary = {
        "totalGroups": len(groups),
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "dryRun": 0,
        "modifiedDocs": 0,
    }

    max_workers = max(1, int(args.workers or 1))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        total = len(groups)

        for index, (source_key, group_entries) in enumerate(groups, start=1):
            futures.append(
                executor.submit(
                    process_group,
                    source_key=source_key,
                    entries=group_entries,
                    collection=collection,
                    args=args,
                    manifest_path=manifest_path,
                    manifest_lock=manifest_lock,
                    index=index,
                    total=total,
                )
            )

        for future in as_completed(futures):
            result = future.result()

            status = result.get("status")

            if status == "processed":
                summary["processed"] += 1
                summary["modifiedDocs"] += int(result.get("modified") or 0)
            elif status == "skipped":
                summary["skipped"] += 1
            elif status == "dry_run":
                summary["dryRun"] += 1
            elif status == "failed":
                summary["failed"] += 1

    print("\n===== SUMMARY =====")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()