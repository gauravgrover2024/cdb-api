#!/usr/bin/env python3
"""
ACI Assist quality-first car image normalizer.

Primary output:
  <slug>.aci.webp

Why:
  For ACI Assist, a premium staged image often looks better than a damaged
  transparent cutout. Clean Cardekho/OEM studio images should usually be
  staged, not aggressively background-removed.

Examples:

  python normalize_car_image_rembg.py \
    --input "https://stimg.cardekho.com/images/car-images/..." \
    --slug hyundai-creta-atlas-white \
    --mode auto \
    --out-dir ../../public/media/car-images/normalized \
    --public-url-prefix /media/car-images/normalized \
    --preview \
    --force

  python normalize_car_image_rembg.py \
    --input "https://stimg.cardekho.com/images/car-images/..." \
    --slug test-cutout \
    --mode cutout \
    --model isnet-general-use \
    --out-dir ../../public/media/car-images/normalized \
    --public-url-prefix /media/car-images/normalized \
    --preview \
    --force
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

os.environ.setdefault("NUMBA_DISABLE_JIT", "1")

import numpy as np
import requests
from PIL import Image, ImageChops, ImageDraw, ImageFilter


MODEL_FALLBACK_ORDER = [
    "birefnet-general",
    "birefnet-general-lite",
    "isnet-general-use",
    "u2net",
    "u2netp",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "car-image"


def sha1_text(value: str) -> str:
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def fetch_image(source: str, timeout: int = 35) -> tuple[Image.Image, bytes, str]:
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


def resize_if_needed(img: Image.Image, max_width: int) -> Image.Image:
    if img.width <= max_width:
        return img
    h = int(img.height * max_width / img.width)
    return img.resize((max_width, h), Image.Resampling.LANCZOS)


def detect_studio_background(img: Image.Image) -> dict[str, Any]:
    rgb = img.convert("RGB")
    arr = np.asarray(rgb)
    h, w = arr.shape[:2]

    band = max(8, int(min(w, h) * 0.04))

    top = arr[:band, :, :]
    bottom = arr[h - band :, :, :]
    left = arr[:, :band, :]
    right = arr[:, w - band :, :]

    border = np.concatenate(
        [
            top.reshape(-1, 3),
            bottom.reshape(-1, 3),
            left.reshape(-1, 3),
            right.reshape(-1, 3),
        ],
        axis=0,
    )

    mx = border.max(axis=1)
    mn = border.min(axis=1)
    diff = mx - mn

    light_neutral = (mx >= 228) & (mn >= 212) & (diff <= 38)
    near_white = (mx >= 240) & (mn >= 232) & (diff <= 26)

    light_ratio = float(light_neutral.mean())
    near_white_ratio = float(near_white.mean())

    is_studio = light_ratio >= 0.74 or near_white_ratio >= 0.62

    return {
        "isStudio": bool(is_studio),
        "borderLightRatio": round(light_ratio, 4),
        "borderWhiteRatio": round(near_white_ratio, 4),
        "reason": "border mostly light neutral" if is_studio else "border not clean studio",
    }


def find_content_bbox_for_studio(img: Image.Image, padding_ratio: float = 0.08) -> tuple[int, int, int, int]:
    rgb = img.convert("RGB")
    arr = np.asarray(rgb)
    h, w = arr.shape[:2]

    mx = arr.max(axis=2)
    mn = arr.min(axis=2)
    diff = mx - mn

    background = (mx >= 238) & (mn >= 224) & (diff <= 34)
    foreground = ~background

    ys, xs = np.where(foreground)

    if len(xs) < max(1000, int(w * h * 0.01)):
        return (0, 0, w, h)

    x0, x1 = int(xs.min()), int(xs.max())
    y0, y1 = int(ys.min()), int(ys.max())

    pad_x = int(w * padding_ratio)
    pad_y = int(h * padding_ratio)

    x0 = max(0, x0 - pad_x)
    y0 = max(0, y0 - pad_y)
    x1 = min(w, x1 + pad_x)
    y1 = min(h, y1 + pad_y)

    if (x1 - x0) < w * 0.35 or (y1 - y0) < h * 0.35:
        return (0, 0, w, h)

    return (x0, y0, x1, y1)


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


def feather_alpha(img: Image.Image, radius: float = 0.35) -> Image.Image:
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    alpha = img.getchannel("A").filter(ImageFilter.GaussianBlur(radius))
    out = img.copy()
    out.putalpha(alpha)
    return out


def remove_bg_with_rembg_quality(img: Image.Image, preferred_model: str) -> tuple[Image.Image, str]:
    from rembg import new_session, remove

    ordered_models: list[str] = []
    for model in [preferred_model, *MODEL_FALLBACK_ORDER]:
        if model and model not in ordered_models:
            ordered_models.append(model)

    last_error: Optional[Exception] = None

    for model in ordered_models:
        try:
            session = new_session(model)
            result = remove(img.convert("RGB"), session=session)
            return result.convert("RGBA"), model
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"rembg failed for all models. Last error: {last_error}")


def remove_white_bg_fallback(img: Image.Image, max_width: int = 2200) -> Image.Image:
    import cv2

    img = img.convert("RGB")
    img = resize_if_needed(img, max_width)

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


def build_gradient_canvas(width: int, height: int) -> Image.Image:
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
        (width * 0.18, height * 0.28, width * 0.82, height * 1.02),
        fill=(37, 99, 235, 34),
    )
    glow = glow.filter(ImageFilter.GaussianBlur(56))

    return Image.alpha_composite(canvas, glow)


def add_ground_shadow(canvas: Image.Image, x: int, y: int, car_w: int, car_h: int) -> Image.Image:
    shadow = Image.new("RGBA", canvas.size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(shadow)

    cx0 = x + car_w * 0.08
    cx1 = x + car_w * 0.92
    cy0 = y + car_h * 0.72
    cy1 = y + car_h * 0.98

    draw.ellipse((cx0, cy0, cx1, cy1), fill=(15, 23, 42, 56))
    shadow = shadow.filter(ImageFilter.GaussianBlur(28))
    return Image.alpha_composite(canvas, shadow)


def parse_canvas_ratio(value: str) -> tuple[int, int]:
    value = str(value or "16:9").strip()
    if ":" not in value:
        return (1400, 800)
    left, right = value.split(":", 1)
    try:
        a = float(left)
        b = float(right)
        if a <= 0 or b <= 0:
            return (1400, 800)
        width = 1400
        height = int(width * b / a)
        return width, height
    except Exception:
        return (1400, 800)


def make_aci_staged_image(
    input_img: Image.Image,
    out_path: Path,
    width: int = 1400,
    height: int = 800,
    has_alpha: bool = False,
    studio_mode: bool = False,
) -> None:
    canvas = build_gradient_canvas(width, height)

    if has_alpha:
        car_img = trim_transparent(input_img.convert("RGBA"), padding=24)
    else:
        rgb = input_img.convert("RGB")
        if studio_mode:
            bbox = find_content_bbox_for_studio(rgb)
            car_img = rgb.crop(bbox)
        else:
            car_img = rgb

    max_car_w = int(width * 0.88)
    max_car_h = int(height * 0.62)

    ratio = min(max_car_w / car_img.width, max_car_h / car_img.height)
    resized = car_img.resize(
        (int(car_img.width * ratio), int(car_img.height * ratio)),
        Image.Resampling.LANCZOS,
    )

    x = (width - resized.width) // 2
    y = int(height * 0.22)

    canvas = add_ground_shadow(canvas, x, y, resized.width, resized.height)

    if has_alpha:
        canvas.alpha_composite(resized.convert("RGBA"), (x, y))
    else:
        region = canvas.crop((x, y, x + resized.width, y + resized.height)).convert("RGB")
        car_rgb = resized.convert("RGB")
        multiplied = ImageChops.multiply(region, car_rgb)
        blended = Image.blend(region, multiplied, 0.96)
        canvas.paste(blended.convert("RGBA"), (x, y))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(out_path, "WEBP", quality=96, method=6)


def make_preview_jpg(staged_webp_path: Path, preview_path: Path) -> None:
    img = Image.open(staged_webp_path).convert("RGB")
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(preview_path, quality=94)


def process_single_image(
    *,
    input_source: str,
    slug: Optional[str],
    out_dir: Path,
    public_url_prefix: str,
    mode: str = "auto",
    model: str = "isnet-general-use",
    max_width: int = 2200,
    canvas_ratio: str = "16:9",
    force: bool = False,
    preview: bool = False,
    keep_raw: bool = False,
    allow_fallback_cutout: bool = False,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    source_key = sha1_text(input_source)
    final_slug = slugify(slug or f"car-{source_key[:12]}")

    public_prefix = public_url_prefix.rstrip("/")

    staged_path = out_dir / f"{final_slug}.aci.webp"
    clean_png_path = out_dir / f"{final_slug}.clean.png"
    clean_webp_path = out_dir / f"{final_slug}.clean.webp"
    preview_path = out_dir / f"{final_slug}.preview.jpg"
    meta_path = out_dir / f"{final_slug}.meta.json"

    staged_url = f"{public_prefix}/{staged_path.name}"
    clean_png_url = f"{public_prefix}/{clean_png_path.name}"
    clean_webp_url = f"{public_prefix}/{clean_webp_path.name}"
    preview_url = f"{public_prefix}/{preview_path.name}"

    if staged_path.exists() and meta_path.exists() and not force:
        existing = json.loads(meta_path.read_text(encoding="utf-8"))
        existing["skipped"] = True
        existing["reason"] = "already_processed"
        return existing

    raw_img, raw_bytes, source_type = fetch_image(input_source)
    original_w, original_h = raw_img.width, raw_img.height

    raw_path = None
    if keep_raw:
        raw_path = out_dir / f"{final_slug}.raw.jpg"
        raw_path.write_bytes(raw_bytes)

    work_img = resize_if_needed(raw_img, max_width)
    studio_info = detect_studio_background(work_img)

    canvas_w, canvas_h = parse_canvas_ratio(canvas_ratio)

    quality_warnings: list[str] = []
    clean_png_url_out: Optional[str] = None
    clean_webp_url_out: Optional[str] = None
    clean_png_path_out: Optional[str] = None
    clean_webp_path_out: Optional[str] = None
    model_used: Optional[str] = None

    mode_used = mode
    method = ""

    should_stage_only = mode == "stage-only" or (mode == "auto" and studio_info["isStudio"])

    if should_stage_only:
        mode_used = "stage-only"
        method = "aci-stage-original"

        if studio_info["isStudio"]:
            quality_warnings.append("studio_background_detected_cutout_skipped")

        make_aci_staged_image(
            work_img,
            staged_path,
            width=canvas_w,
            height=canvas_h,
            has_alpha=False,
            studio_mode=True,
        )

    else:
        mode_used = "cutout"

        try:
            cutout, model_used = remove_bg_with_rembg_quality(work_img, model)
            cutout = trim_transparent(feather_alpha(cutout), padding=24)

            clean_png_path.parent.mkdir(parents=True, exist_ok=True)
            cutout.save(clean_png_path)
            cutout.save(clean_webp_path, "WEBP", quality=96, method=6, lossless=False)

            clean_png_url_out = clean_png_url
            clean_webp_url_out = clean_webp_url
            clean_png_path_out = str(clean_png_path)
            clean_webp_path_out = str(clean_webp_path)

            method = f"rembg-{model_used}"

            make_aci_staged_image(
                cutout,
                staged_path,
                width=canvas_w,
                height=canvas_h,
                has_alpha=True,
                studio_mode=False,
            )

        except Exception as exc:
            if allow_fallback_cutout:
                quality_warnings.append(f"rembg_failed_used_white_bg_fallback: {exc}")

                cutout = remove_white_bg_fallback(work_img, max_width=max_width)
                cutout = trim_transparent(feather_alpha(cutout), padding=24)

                clean_png_path.parent.mkdir(parents=True, exist_ok=True)
                cutout.save(clean_png_path)
                cutout.save(clean_webp_path, "WEBP", quality=96, method=6)

                clean_png_url_out = clean_png_url
                clean_webp_url_out = clean_webp_url
                clean_png_path_out = str(clean_png_path)
                clean_webp_path_out = str(clean_webp_path)

                model_used = None
                method = "white-bg-fallback-cutout"

                make_aci_staged_image(
                    cutout,
                    staged_path,
                    width=canvas_w,
                    height=canvas_h,
                    has_alpha=True,
                    studio_mode=False,
                )

            elif mode == "auto":
                quality_warnings.append(f"rembg_failed_staged_original_used: {exc}")
                mode_used = "stage-only"
                method = "aci-stage-original-after-rembg-failure"

                make_aci_staged_image(
                    work_img,
                    staged_path,
                    width=canvas_w,
                    height=canvas_h,
                    has_alpha=False,
                    studio_mode=True,
                )

            else:
                return {
                    "ok": False,
                    "error": str(exc),
                    "input": input_source,
                    "slug": final_slug,
                    "modeRequested": mode,
                    "processedAt": now_iso(),
                }

    preview_url_out = None
    preview_path_out = None

    if preview:
        make_preview_jpg(staged_path, preview_path)
        preview_url_out = preview_url
        preview_path_out = str(preview_path)

    meta = {
        "ok": True,
        "slug": final_slug,
        "source": input_source,
        "sourceType": source_type,
        "sourceKey": source_key,
        "modeRequested": mode,
        "modeUsed": mode_used,
        "method": method,
        "model": model_used,
        "isStudioBackground": bool(studio_info["isStudio"]),
        "borderWhiteRatio": studio_info["borderWhiteRatio"],
        "borderLightRatio": studio_info["borderLightRatio"],
        "studioReason": studio_info["reason"],
        "stagedImagePath": str(staged_path),
        "stagedImageUrl": staged_url,
        "cleanPngPath": clean_png_path_out,
        "cleanPngUrl": clean_png_url_out,
        "cleanWebpPath": clean_webp_path_out,
        "cleanWebpUrl": clean_webp_url_out,
        "previewPath": preview_path_out,
        "previewUrl": preview_url_out,
        "rawPath": str(raw_path) if raw_path else None,
        "originalWidth": original_w,
        "originalHeight": original_h,
        "outputWidth": canvas_w,
        "outputHeight": canvas_h,
        "qualityWarnings": quality_warnings,
        "processedAt": now_iso(),
        "skipped": False,
    }

    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return meta


def main() -> None:
    parser = argparse.ArgumentParser(description="Quality-first ACI car image normalizer")
    parser.add_argument("--input", required=True, help="Image URL or local file path")
    parser.add_argument("--slug", default=None, help="Output slug")
    parser.add_argument("--out-dir", default="./normalized-cars", help="Output directory")
    parser.add_argument("--public-url-prefix", default="/media/car-images/normalized", help="Public URL prefix")
    parser.add_argument("--mode", default="auto", choices=["auto", "stage-only", "cutout"])
    parser.add_argument(
        "--model",
        default="isnet-general-use",
        choices=["birefnet-general", "birefnet-general-lite", "isnet-general-use", "u2net", "u2netp"],
    )
    parser.add_argument("--max-width", type=int, default=2200)
    parser.add_argument("--canvas-ratio", default="16:9")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--keep-raw", action="store_true")
    parser.add_argument("--allow-fallback-cutout", action="store_true")
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    try:
        result = process_single_image(
            input_source=args.input,
            slug=args.slug,
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

        print(
            json.dumps(
                result,
                ensure_ascii=False if args.json_only else False,
                indent=None if args.json_only else 2,
                default=str,
            )
        )
        raise SystemExit(0 if result.get("ok") else 1)

    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": str(exc),
                    "processedAt": now_iso(),
                },
                ensure_ascii=False,
                indent=None if args.json_only else 2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()