#!/usr/bin/env python3
"""
ACI Assist car image normalizer
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("NUMBA_DISABLE_JIT", "1")

import numpy as np
import requests
from PIL import Image, ImageFilter, ImageDraw


def slugify(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "car-image"


def sha1_text(value: str) -> str:
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def fetch_image(source: str, timeout: int = 30):
    if source.startswith(("http://", "https://")):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept": "image/jpeg,image/png,image/webp,*/*;q=0.8",
            "Referer": "https://www.cardekho.com/",
        }
        response = requests.get(source, headers=headers, timeout=timeout)
        response.raise_for_status()
        raw = response.content
        return Image.open(io.BytesIO(raw)).convert("RGB"), raw, "url"

    raw = Path(source).read_bytes()
    return Image.open(io.BytesIO(raw)).convert("RGB"), raw, "file"


def trim_transparent(img: Image.Image, padding: int = 24) -> Image.Image:
    if img.mode != "RGBA":
        img = img.convert("RGBA")

    alpha = img.getchannel("A")
    bbox = alpha.getbbox()
    if not bbox:
        return img

    cropped = img.crop(bbox)
    canvas = Image.new(
        "RGBA",
        (cropped.width + padding * 2, cropped.height + padding * 2),
        (255, 255, 255, 0),
    )
    canvas.alpha_composite(cropped, (padding, padding))
    return canvas


def remove_bg_with_rembg(img: Image.Image, model: str = "u2netp") -> Image.Image:
    from rembg import new_session, remove

    session = new_session(model)
    result = remove(img.convert("RGB"), session=session)
    return result.convert("RGBA")


def remove_white_bg_fallback(img: Image.Image, max_width: int = 1800) -> Image.Image:
    import cv2

    img = img.convert("RGB")
    if img.width > max_width:
        h = int(img.height * max_width / img.width)
        img = img.resize((max_width, h), Image.Resampling.LANCZOS)

    arr = np.array(img)
    r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
    diff = np.max(arr, axis=2) - np.min(arr, axis=2)

    near_white = (r > 246) & (g > 246) & (b > 246) & (diff < 12)
    raw_fg = (~near_white).astype("uint8") * 255

    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (11, 11))
    raw_fg = cv2.morphologyEx(raw_fg, cv2.MORPH_CLOSE, kernel, iterations=2)
    raw_fg = cv2.dilate(raw_fg, kernel, iterations=1)

    contours, _ = cv2.findContours(raw_fg, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    mask = np.zeros(raw_fg.shape, "uint8")
    area = raw_fg.shape[0] * raw_fg.shape[1]

    for contour in contours:
        if cv2.contourArea(contour) > area * 0.002:
            cv2.drawContours(mask, [contour], -1, 255, thickness=cv2.FILLED)

    ys, xs = np.where(mask > 0)
    if len(xs) > 0:
        gc_mask = np.full(mask.shape, cv2.GC_BGD, dtype=np.uint8)
        gc_mask[mask > 0] = cv2.GC_PR_FGD

        x0, x1 = xs.min(), xs.max()
        y0, y1 = ys.min(), ys.max()
        pad = 20
        x0, y0 = max(0, x0 - pad), max(0, y0 - pad)
        x1, y1 = min(mask.shape[1] - 1, x1 + pad), min(mask.shape[0] - 1, y1 + pad)
        rect = (int(x0), int(y0), int(x1 - x0), int(y1 - y0))

        bgd = np.zeros((1, 65), np.float64)
        fgd = np.zeros((1, 65), np.float64)

        try:
            cv2.grabCut(arr, gc_mask, rect, bgd, fgd, 4, cv2.GC_INIT_WITH_MASK)
            mask = np.where(
                (gc_mask == cv2.GC_FGD) | (gc_mask == cv2.GC_PR_FGD),
                255,
                0,
            ).astype("uint8")
        except Exception:
            pass

    mask = cv2.morphologyEx(
        mask,
        cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3)),
    )
    mask = cv2.morphologyEx(
        mask,
        cv2.MORPH_CLOSE,
        cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (7, 7)),
    )

    alpha = Image.fromarray(mask).filter(ImageFilter.GaussianBlur(0.8))
    rgba = img.convert("RGBA")
    rgba.putalpha(alpha)
    return rgba


def make_aci_preview(clean_img: Image.Image, out_path: Path, width: int = 1400, height: int = 800) -> None:
    clean_img = clean_img.convert("RGBA")
    canvas = Image.new("RGBA", (width, height), (255, 255, 255, 255))

    top = np.array([255, 255, 255, 255], dtype=np.float32)
    bottom = np.array([232, 242, 255, 255], dtype=np.float32)
    grad = np.zeros((height, width, 4), dtype=np.uint8)

    for y in range(height):
        t = y / max(1, height - 1)
        grad[y, :, :] = (top * (1 - t) + bottom * t).astype(np.uint8)

    canvas = Image.alpha_composite(canvas, Image.fromarray(grad, "RGBA"))

    glow = Image.new("RGBA", (width, height), (255, 255, 255, 0))
    draw = ImageDraw.Draw(glow)
    draw.ellipse(
        (width * 0.22, height * 0.36, width * 0.78, height * 1.05),
        fill=(31, 111, 255, 38),
    )
    glow = glow.filter(ImageFilter.GaussianBlur(52))
    canvas = Image.alpha_composite(canvas, glow)

    max_car_w = int(width * 0.86)
    max_car_h = int(height * 0.55)
    ratio = min(max_car_w / clean_img.width, max_car_h / clean_img.height)
    resized = clean_img.resize(
        (int(clean_img.width * ratio), int(clean_img.height * ratio)),
        Image.Resampling.LANCZOS,
    )

    x = (width - resized.width) // 2
    y = int(height * 0.27)

    shadow = Image.new("RGBA", (width, height), (255, 255, 255, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.ellipse(
        (width * 0.20, y + resized.height * 0.72, width * 0.80, y + resized.height * 0.96),
        fill=(15, 23, 42, 72),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(28))
    canvas = Image.alpha_composite(canvas, shadow)

    canvas.alpha_composite(resized, (x, y))
    canvas.convert("RGB").save(out_path, quality=94)


def emit_json(payload: dict, exit_code: int = 0, json_only: bool = True) -> None:
    text = json.dumps(payload, ensure_ascii=False) if json_only else json.dumps(payload, indent=2, ensure_ascii=False)
    print(text)
    raise SystemExit(exit_code)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize car image with rembg and store transparent outputs")
    parser.add_argument("--input", required=True, help="Image URL or local file path")
    parser.add_argument("--slug", default=None, help="Output slug")
    parser.add_argument("--out-dir", default="./normalized-cars", help="Output directory")
    parser.add_argument("--public-url-prefix", default="/media/car-images/normalized", help="Public URL prefix")
    parser.add_argument("--model", default="u2netp", choices=["u2netp", "u2net", "isnet-general-use"])
    parser.add_argument("--max-width", type=int, default=1800)
    parser.add_argument("--fallback", action="store_true", help="Force white-bg fallback")
    parser.add_argument("--keep-raw", action="store_true", help="Persist downloaded raw image")
    parser.add_argument("--preview", action="store_true", help="Generate ACI preview JPG")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    parser.add_argument("--json-only", action="store_true", help="Emit compact machine-readable JSON")
    args = parser.parse_args()

    json_only = bool(args.json_only)

    try:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        source_key = sha1_text(args.input)
        slug = slugify(args.slug or f"car-{source_key[:12]}")

        png_path = out_dir / f"{slug}.clean.png"
        webp_path = out_dir / f"{slug}.clean.webp"
        meta_path = out_dir / f"{slug}.meta.json"

        public_prefix = args.public_url_prefix.rstrip("/")
        clean_png_url = f"{public_prefix}/{png_path.name}"
        clean_webp_url = f"{public_prefix}/{webp_path.name}"

        if webp_path.exists() and png_path.exists() and meta_path.exists() and not args.force:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            existing_meta["skipped"] = True
            existing_meta["reason"] = "already_processed"
            emit_json(existing_meta, json_only=json_only)

        raw_img, raw_bytes, source_type = fetch_image(args.input)

        raw_path = None
        if args.keep_raw:
            raw_path = out_dir / f"{slug}.raw.jpg"
            raw_path.write_bytes(raw_bytes)

        work_img = raw_img
        if work_img.width > args.max_width:
            h = int(work_img.height * args.max_width / work_img.width)
            work_img = work_img.resize((args.max_width, h), Image.Resampling.LANCZOS)

        method = "rembg"
        error_note = None

        try:
            if args.fallback:
                raise RuntimeError("Fallback forced by --fallback")
            clean = remove_bg_with_rembg(work_img, model=args.model)
        except Exception as exc:
            method = "white-bg-fallback"
            error_note = str(exc)
            clean = remove_white_bg_fallback(work_img, max_width=args.max_width)

        clean = trim_transparent(clean, padding=24)
        clean.save(png_path)
        clean.save(webp_path, quality=96, method=6)

        preview_path = None
        preview_url = None
        if args.preview:
            preview_path = out_dir / f"{slug}.aci-preview.jpg"
            make_aci_preview(clean, preview_path)
            preview_url = f"{public_prefix}/{preview_path.name}"

        meta = {
            "ok": True,
            "slug": slug,
            "source": args.input,
            "sourceType": source_type,
            "sourceKey": source_key,
            "method": method,
            "model": args.model if method == "rembg" else None,
            "errorNote": error_note,
            "rawPath": str(raw_path) if raw_path else None,
            "cleanPngPath": str(png_path),
            "cleanWebpPath": str(webp_path),
            "previewPath": str(preview_path) if preview_path else None,
            "cleanPngUrl": clean_png_url,
            "cleanWebpUrl": clean_webp_url,
            "previewUrl": preview_url,
            "width": clean.width,
            "height": clean.height,
            "processedAt": datetime.now(timezone.utc).isoformat(),
            "skipped": False,
        }

        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        emit_json(meta, json_only=json_only)

    except Exception as exc:
        emit_json(
            {
                "ok": False,
                "error": str(exc),
                "input": args.input,
                "processedAt": datetime.now(timezone.utc).isoformat(),
            },
            exit_code=1,
            json_only=json_only,
        )


if __name__ == "__main__":
    main()
