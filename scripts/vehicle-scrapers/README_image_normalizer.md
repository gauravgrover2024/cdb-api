# ACI Assist V2 Vehicle Color Media

## Install
```bash
cd /Users/gauravgrover/cdb-api/scripts/vehicle-scrapers
python3 -m venv image-bg-venv
source image-bg-venv/bin/activate
pip install -r requirements_image_normalizer.txt
```

## Full color media pipeline
```bash
python vehicle_color_master_pipeline.py --brand kia --model seltos
```

By default, the master pipeline only normalizes/background-removes/uploads
new or changed assets. It reuses existing Mongo media when the source image
hash and frame metadata are already present.

Force regeneration only when intentionally rebuilding media:
```bash
python vehicle_color_master_pipeline.py --brand kia --model seltos --force
```

Debug without Mongo writes or R2 upload:
```bash
python vehicle_color_master_pipeline.py \
  --brand kia \
  --model seltos \
  --skip-upload \
  --skip-mongo
```

## Single-image helper test
```bash
python normalize_car_image_rembg.py \
  --input "IMAGE_URL_HERE" \
  --slug test-car \
  --out-dir ../../public/media/car-images/normalized \
  --public-url-prefix /media/car-images/normalized \
  --preview \
  --json-only
```

## Notes
- First run downloads ONNX model files into `~/.u2net/`.
- Keep forced regeneration narrow on constrained RAM systems.
- Raw images are not persisted unless `--keep-raw` is provided.
- Re-running the master pipeline is safe; unchanged assets are reused.
- Default working output path for normalized assets is:
  - `/Users/gauravgrover/cdb-api/scripts/vehicle-scrapers/normalized`
