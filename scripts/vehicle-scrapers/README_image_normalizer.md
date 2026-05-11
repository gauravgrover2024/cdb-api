# ACI Assist V2 Image Normalizer

## Install
```bash
cd /Users/gauravgrover/cdb-api/scripts/vehicle-scrapers
python3 -m venv image-bg-venv
source image-bg-venv/bin/activate
pip install -r requirements_image_normalizer.txt
```

## Single-image test
```bash
python normalize_car_image_rembg.py \
  --input "IMAGE_URL_HERE" \
  --slug test-car \
  --out-dir ../../public/media/car-images/normalized \
  --public-url-prefix /media/car-images/normalized \
  --preview \
  --json-only
```

## Bulk dry run
```bash
python bulk_normalize_car_images.py \
  --mongo-uri "$MONGO_URI" \
  --db-name YOUR_DB_NAME \
  --collection vehicles \
  --dry-run \
  --limit 20
```

## Bulk real run
```bash
python bulk_normalize_car_images.py \
  --mongo-uri "$MONGO_URI" \
  --db-name YOUR_DB_NAME \
  --collection vehicles \
  --out-dir ../../public/media/car-images/normalized \
  --public-url-prefix /media/car-images/normalized \
  --workers 1 \
  --model u2netp
```

## Notes
- First run downloads ONNX model files into `~/.u2net/`.
- Keep workers low on constrained RAM systems (`1` recommended).
- Raw images are not persisted unless `--keep-raw` is provided.
- Re-running is safe; processed artifacts can be skipped and every attempt is logged in `normalized-car-images.manifest.jsonl`.
- Default output path for normalized assets is:
  - `/Users/gauravgrover/cdb-api/public/media/car-images/normalized`
