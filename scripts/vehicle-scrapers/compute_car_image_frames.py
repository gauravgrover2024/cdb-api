#!/usr/bin/env python3
"""
Compute smart image framing metadata for vehicle color images.

Dry run:
  python3 scripts/vehicle-scrapers/compute_car_image_frames.py --limit 25

Write all:
  python3 scripts/vehicle-scrapers/compute_car_image_frames.py --write --workers 6
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import numpy as np
import requests
from PIL import Image, ImageFile
from pymongo import MongoClient, UpdateOne
import certifi
from tqdm import tqdm

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

ImageFile.LOAD_TRUNCATED_IMAGES = True

FRAME_VERSION = "car-image-frame-v1"
IMAGE_FIELDS = [
    "normalizedImageUrl",
    "cleanImageUrl",
    "normalized_image_url",
    "clean_image_url",
    "normalizedImagePngUrl",
    "stagedImageUrl",
    "sourceImageUrl",
    "imageUrl",
    "image_url",
    "car_image_url",
    "colorImage",
    "color_image",
    "swatchImage",
    "url",
    "src",
]

STAGE_PRESETS = {
    "chatCard": {"targetWidth": 0.78, "targetHeight": 0.68, "bottomTarget": 0.80, "maxScale": 1.95, "minScale": 0.92},
    "priceSide": {"targetWidth": 0.90, "targetHeight": 0.76, "bottomTarget": 0.84, "maxScale": 2.08, "minScale": 0.92},
    "mobileHero": {"targetWidth": 0.86, "targetHeight": 0.72, "bottomTarget": 0.84, "maxScale": 2.00, "minScale": 0.92},
    "overviewHero": {"targetWidth": 0.88, "targetHeight": 0.74, "bottomTarget": 0.84, "maxScale": 2.04, "minScale": 0.92},
}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def pick_image_url(doc: Dict[str, Any]) -> Tuple[str, str]:
    for field in IMAGE_FIELDS:
        value = clean_text(doc.get(field))
        if value:
            return value, field
    return "", ""


def get_mongo_uri() -> str:
    return os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or os.getenv("MONGO_URL") or os.getenv("DATABASE_URL") or ""


def get_database_name(uri: str, explicit: str = "") -> Optional[str]:
    if explicit:
        return explicit
    parsed = urlparse(uri)
    db_name = (parsed.path or "").strip("/").split("/")[0]
    return db_name or None


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def local_path_from_url(url: str, repo_root: Path) -> Optional[Path]:
    if not url:
        return None

    if url.startswith("/media/"):
        candidate = repo_root / "public" / url.lstrip("/")
        return candidate if candidate.exists() else None

    parsed = urlparse(url)
    path = unquote(parsed.path or "")

    if "/media/" in path:
        rel = path.split("/media/", 1)[1]
        candidate = repo_root / "public" / "media" / rel
        return candidate if candidate.exists() else None

    if path.startswith("media/"):
        candidate = repo_root / "public" / path
        return candidate if candidate.exists() else None

    return None


def read_image_bytes(url: str, repo_root: Path, timeout: int = 25) -> Tuple[bytes, str]:
    local_path = local_path_from_url(url, repo_root)
    if local_path and local_path.exists():
        return local_path.read_bytes(), f"local:{local_path}"

    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"}:
        response = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "CDriveImageFrameBot/1.0 (+https://acillp.com)",
                "Accept": "image/avif,image/webp,image/png,image/jpeg,image/*,*/*",
            },
        )
        response.raise_for_status()
        return response.content, "remote"

    candidate = Path(url)
    if candidate.exists():
        return candidate.read_bytes(), f"local:{candidate}"

    raise FileNotFoundError(f"Image not found locally and not downloadable: {url}")


def load_image(image_bytes: bytes) -> Image.Image:
    return Image.open(io.BytesIO(image_bytes)).convert("RGBA")


def mask_from_alpha_or_background(image: Image.Image) -> Tuple[np.ndarray, str, float]:
    arr = np.array(image)
    rgb = arr[:, :, :3].astype(np.int16)
    alpha = arr[:, :, 3].astype(np.uint8)

    h, w = alpha.shape
    transparent_ratio = float(np.mean(alpha < 245))
    alpha_range = int(alpha.max()) - int(alpha.min())

    if transparent_ratio > 0.03 and alpha_range > 20:
        mask = alpha > 34
        return mask, "alpha-bounds-v1", min(1.0, transparent_ratio * 3.0)

    border = np.concatenate(
        [
            rgb[: max(2, h // 30), :, :].reshape(-1, 3),
            rgb[-max(2, h // 30) :, :, :].reshape(-1, 3),
            rgb[:, : max(2, w // 30), :].reshape(-1, 3),
            rgb[:, -max(2, w // 30) :, :].reshape(-1, 3),
        ],
        axis=0,
    )
    bg = np.median(border, axis=0)
    diff = np.sqrt(np.sum((rgb - bg) ** 2, axis=2))

    max_rgb = rgb.max(axis=2)
    min_rgb = rgb.min(axis=2)
    saturation = max_rgb - min_rgb
    luminance = 0.299 * rgb[:, :, 0] + 0.587 * rgb[:, :, 1] + 0.114 * rgb[:, :, 2]

    mask = (diff > 22) | ((saturation > 18) & (luminance < 245)) | (luminance < 225)
    mask[: max(1, h // 80), :] = False
    mask[-max(1, h // 80) :, :] = False
    mask[:, : max(1, w // 80)] = False
    mask[:, -max(1, w // 80) :] = False

    return mask, "background-diff-bounds-v1", 0.58


def robust_bounds(mask: np.ndarray) -> Optional[Dict[str, int]]:
    ys, xs = np.where(mask)
    if len(xs) < 25 or len(ys) < 25:
        return None

    x1 = int(np.floor(np.percentile(xs, 0.35)))
    x2 = int(np.ceil(np.percentile(xs, 99.65)))
    y1 = int(np.floor(np.percentile(ys, 0.35)))
    y2 = int(np.ceil(np.percentile(ys, 99.65)))

    if x2 <= x1 or y2 <= y1:
        return None

    return {"x": max(0, x1), "y": max(0, y1), "width": max(1, x2 - x1 + 1), "height": max(1, y2 - y1 + 1)}


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def compute_stage_frame(bounds: Dict[str, int], canvas: Dict[str, int], preset: Dict[str, float]) -> Dict[str, Any]:
    w = float(canvas["width"])
    h = float(canvas["height"])

    bx = bounds["x"] / w
    by = bounds["y"] / h
    bw = max(0.01, bounds["width"] / w)
    bh = max(0.01, bounds["height"] / h)

    center_x = bx + bw / 2.0
    bottom_y = by + bh

    desired_scale = min(preset["targetWidth"] / bw, preset["targetHeight"] / bh)
    max_by_width = 0.965 / bw
    max_by_height = 0.90 / bh

    scale = clamp(
        min(desired_scale, max_by_width, max_by_height, preset["maxScale"]),
        preset["minScale"],
        preset["maxScale"],
    )

    translate_x = (0.50 - center_x) * 100.0 / max(1.0, scale)
    translate_y = (preset["bottomTarget"] - bottom_y) * 100.0 / max(1.0, scale)

    return {
        "scale": round(scale, 4),
        "translateXPct": round(clamp(translate_x, -18.0, 18.0), 3),
        "translateYPct": round(clamp(translate_y, -16.0, 18.0), 3),
        "objectPosition": "center bottom",
        "transformOrigin": "center bottom",
    }


def compute_frame(doc: Dict[str, Any], repo_root: Path, timeout: int = 25) -> Dict[str, Any]:
    image_url, source_field = pick_image_url(doc)
    if not image_url:
        raise ValueError("No image URL field found")

    image_bytes, source_mode = read_image_bytes(image_url, repo_root, timeout=timeout)
    image = load_image(image_bytes)
    width, height = image.size

    if width < 20 or height < 20:
        raise ValueError(f"Image too small: {width}x{height}")

    mask, method, confidence_seed = mask_from_alpha_or_background(image)
    bounds = robust_bounds(mask)

    if not bounds:
        raise ValueError("Could not detect visible car bounds")

    coverage = (bounds["width"] * bounds["height"]) / float(width * height)
    mask_coverage = float(np.mean(mask))

    if coverage < 0.015:
        confidence = 0.35
    elif coverage > 0.92:
        confidence = 0.45
    else:
        confidence = 0.72

    confidence = clamp((confidence + confidence_seed) / 2.0, 0.20, 0.98)

    canvas = {"width": width, "height": height}
    stage_frames = {name: compute_stage_frame(bounds, canvas, preset) for name, preset in STAGE_PRESETS.items()}
    default_frame = stage_frames["priceSide"]

    normalized_bounds = {
        "x": round(bounds["x"] / width, 5),
        "y": round(bounds["y"] / height, 5),
        "width": round(bounds["width"] / width, 5),
        "height": round(bounds["height"] / height, 5),
    }

    return {
        "version": FRAME_VERSION,
        "method": method,
        "status": "computed",
        "sourceField": source_field,
        "sourceUrl": image_url,
        "sourceMode": source_mode,
        "canvas": canvas,
        "bounds": bounds,
        "normalizedBounds": normalized_bounds,
        "coverage": round(float(coverage), 5),
        "maskCoverage": round(float(mask_coverage), 5),
        "confidence": round(float(confidence), 4),
        "stageFrames": stage_frames,
        "cssVars": {
            "--car-frame-scale": str(default_frame["scale"]),
            "--car-frame-x": f'{default_frame["translateXPct"]}%',
            "--car-frame-y": f'{default_frame["translateYPct"]}%',
            "--car-frame-origin": default_frame["transformOrigin"],
        },
        "computedAt": now_utc(),
    }


def build_query(force: bool = False, brand: str = "", model: str = "") -> Dict[str, Any]:
    image_exists_or = [{field: {"$exists": True, "$ne": ""}} for field in IMAGE_FIELDS]
    query: Dict[str, Any] = {"$and": [{"$or": image_exists_or}]}

    if not force:
        query["$and"].append(
            {
                "$or": [
                    {"imageFrame": {"$exists": False}},
                    {"imageFrame.version": {"$ne": FRAME_VERSION}},
                    {"imageFrame.status": {"$ne": "computed"}},
                ]
            }
        )

    if brand:
        query["$and"].append({"brand": re.compile(re.escape(brand), re.I)})
    if model:
        query["$and"].append({"model": re.compile(re.escape(model), re.I)})

    return query


def process_one(doc: Dict[str, Any], repo_root: Path, timeout: int) -> Tuple[Any, Dict[str, Any], Optional[str]]:
    try:
        frame = compute_frame(doc, repo_root, timeout=timeout)
        return doc["_id"], frame, None
    except Exception as exc:
        error_frame = {
            "version": FRAME_VERSION,
            "method": "frame-compute",
            "status": "failed",
            "error": str(exc)[:500],
            "computedAt": now_utc(),
        }
        return doc["_id"], error_frame, str(exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute image framing metadata for vehicle_colors.")
    parser.add_argument("--write", action="store_true", help="Actually update MongoDB. Without this, dry-run only.")
    parser.add_argument("--force", action="store_true", help="Recompute docs that already have imageFrame.")
    parser.add_argument("--limit", type=int, default=0, help="Limit documents processed.")
    parser.add_argument("--workers", type=int, default=6, help="Parallel workers.")
    parser.add_argument("--batch-size", type=int, default=100, help="Mongo bulk update batch size.")
    parser.add_argument("--db", default="", help="Mongo database name override.")
    parser.add_argument("--collection", default="vehicle_colors_v2", help="Mongo collection name.")
    parser.add_argument("--brand", default="", help="Optional brand filter for testing.")
    parser.add_argument("--model", default="", help="Optional model filter for testing.")
    parser.add_argument("--timeout", type=int, default=25, help="HTTP image fetch timeout seconds.")
    args = parser.parse_args()

    repo_root = resolve_repo_root()
    env_path = repo_root / ".env"
    if load_dotenv and env_path.exists():
        load_dotenv(env_path)

    mongo_uri = get_mongo_uri()
    if not mongo_uri:
        raise SystemExit("Missing Mongo URI. Set MONGO_URI / MONGODB_URI / MONGO_URL / DATABASE_URL.")

    db_name = get_database_name(mongo_uri, args.db)
    if not db_name:
        raise SystemExit("Could not resolve database name from Mongo URI. Pass --db <name>.")

    client = MongoClient(
    mongo_uri,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=30000,
    )
    collection = client[db_name][args.collection]

    query = build_query(force=args.force, brand=args.brand, model=args.model)
    total = collection.count_documents(query)
    if args.limit:
        total = min(total, args.limit)

    print("===== CDRIVE CAR IMAGE FRAME COMPUTE =====")
    print(f"Repo root       : {repo_root}")
    print(f"Database        : {db_name}")
    print(f"Collection      : {args.collection}")
    print(f"Matching docs   : {total}")
    print(f"Write mode      : {args.write}")
    print(f"Force recompute : {args.force}")
    print(f"Workers         : {args.workers}")
    print("==========================================")

    projection = {field: 1 for field in IMAGE_FIELDS}
    projection.update({"brand": 1, "make": 1, "model": 1, "color_name": 1, "colorName": 1, "name": 1, "imageFrame": 1})

    cursor = collection.find(query, projection=projection).sort([("brand", 1), ("model", 1)])
    if args.limit:
        cursor = cursor.limit(args.limit)

    docs = list(cursor)
    processed = computed = failed = 0
    operations: List[UpdateOne] = []
    samples: List[Dict[str, Any]] = []

    def flush_ops() -> None:
        nonlocal operations
        if not operations:
            return
        if args.write:
            collection.bulk_write(operations, ordered=False)
        operations = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {executor.submit(process_one, doc, repo_root, args.timeout): doc for doc in docs}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Computing frames"):
            doc = futures[future]
            doc_id, frame, error = future.result()
            processed += 1

            if error:
                failed += 1
            else:
                computed += 1
                if len(samples) < 8:
                    samples.append(
                        {
                            "brand": doc.get("brand") or doc.get("make"),
                            "model": doc.get("model"),
                            "color": doc.get("color_name") or doc.get("colorName") or doc.get("name"),
                            "bounds": frame.get("bounds"),
                            "priceSide": frame.get("stageFrames", {}).get("priceSide"),
                            "confidence": frame.get("confidence"),
                        }
                    )

            operations.append(
                UpdateOne(
                    {"_id": doc_id},
                    {
                        "$set": {
                            "imageFrame": frame,
                            "imageFrameStatus": frame.get("status"),
                            "imageFrameUpdatedAt": now_utc(),
                        },
                        "$unset": {"imageFrameError": ""},
                    }
                    if not error
                    else {
                        "$set": {
                            "imageFrame": frame,
                            "imageFrameStatus": "failed",
                            "imageFrameError": error[:500],
                            "imageFrameUpdatedAt": now_utc(),
                        }
                    },
                )
            )

            if len(operations) >= max(1, args.batch_size):
                flush_ops()

    flush_ops()

    print("\n===== SUMMARY =====")
    print(f"Processed : {processed}")
    print(f"Computed  : {computed}")
    print(f"Failed    : {failed}")
    print(f"Write mode: {args.write}")

    if samples:
        print("\nSample computed frames:")
        for sample in samples:
            print(sample)

    if not args.write:
        print("\nDry run only. Re-run with --write to update MongoDB.")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
