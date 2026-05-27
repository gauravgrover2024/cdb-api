# ACI Data Refresh Pipeline

## Source Collections

- `vehicles`: main price, variant, fuel, city, lifecycle, and normalized vehicle rows.
- `vehicle_features`: variant feature source rows from the enrichment scraper.
- `vehicle_colors_v2`: color and media source rows. This is a source/main collection, not an ACI read model.
- `price_history`: historical price changes written by the price scraper.

## Derived Read Models

These collections are rebuilt after source data changes:

- `vehicle_feature_catalog_v2`
- `vehicle_variant_feature_matrix_v2`
- `aci_vehicle_model_summary`
- `aci_vehicle_price_rows`

`vehicle_colors_v2` must not be rebuilt or deleted by `aci:rebuild-read-models`. It is only read by ACI builders and covered by index/audit paths where relevant.

## Normal Refresh Command

Run the source scrape/import pipeline and then rebuild ACI read models:

```sh
npm run vehicle:data-pipeline:aci
```

This runs:

1. `python3 scripts/vehicle-scrapers/run_vehicle_data_pipeline.py --skip-aci-post-refresh`
2. `npm run aci:post-data-refresh`

The direct Python pipeline defaults to running `npm run aci:post-data-refresh` after prices, features, and colors complete:

```sh
python3 scripts/vehicle-scrapers/run_vehicle_data_pipeline.py
```

## Skip Post Refresh

Run only the source scrape/import work:

```sh
python3 scripts/vehicle-scrapers/run_vehicle_data_pipeline.py --skip-aci-post-refresh
```

or:

```sh
npm run vehicle:data-pipeline
```

## Post Refresh Order

`npm run aci:post-data-refresh` runs:

1. `npm run aci:rebuild-read-models`
2. `npm run aci:ensure-read-model-indexes`
3. `npm run aci:core-index-audit`
4. `npm run aci:candidates-smoke`

`npm run aci:rebuild-read-models` runs:

1. `npm run aci:feature-kb:build`
2. `npm run aci:read-models:build`

`npm run aci:ensure-read-model-indexes` runs:

1. `npm run indexes:vehicle`
2. `npm run aci:feature-kb:indexes`
3. `npm run aci:ensure-feature-matrix-indexes`

## Full Verification

Run the normal post-refresh checks plus understanding workers and the safety gate:

```sh
npm run aci:post-data-refresh:full
```

This intentionally is not part of the normal source scrape pipeline because it can be slower.

## After Manual DB Imports

After any manual import or direct update to `vehicles`, `vehicle_features`, or `vehicle_colors_v2`, run:

```sh
npm run vehicles:normalize-pricing -- --apply
npm run vehicles:normalize-dataset -- --apply
npm run aci:post-data-refresh
```

Do not delete source collections as part of ACI read-model refreshes. The read-model builders are idempotent and should only write their own target collections.
