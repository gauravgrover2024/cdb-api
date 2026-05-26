# ACI Assist Runtime Read Model Plan

## Why this exists

ACI Assist must feel blazing fast. The public runtime should not repeatedly query heavy scraper-shaped collections for common chat/canvas answers.

Current profiling showed repeated MongoDB time and COLLSCAN patterns in common flows like pricelist, colors, EMI, and multi-intent answers.

## Rule

Raw scraper/source collections remain the source history.
ACI Assist runtime collections are optimized read models for fast product responses.

## Phase 1 collections

### aci_vehicle_model_summary

One document per make + model + city.

Used for:
- hero card
- selected vehicle context
- quick model summary
- price range
- home cards
- quote prefill
- comparison header
- EMI header

Important: this includes the hero/display image from color/media data so price flow does not need to query full color gallery every time.

### aci_vehicle_price_rows

One document per make + model + variant + city.

Used for:
- pricelist
- EMI
- budget filters
- fuel/transmission filters
- variant advisor
- quotation variant prefill

## Future collections

### aci_vehicle_color_gallery

One document per make + model.

Used for full color studio and color inline cards.

### aci_vehicle_feature_lookup

One row per make + model + variant + featureKey.

Used for ultra-fast feature answers.

### aci_vehicle_offer_summary

One row per make + model + city + period.

Used for safe offer display and quotation guidance.

## Non-negotiable rules

- No fake data.
- No runtime dependency on broad regex scans for common queries.
- Use normalized keys: makeKey, modelKey, variantKey, citySlug.
- Use strict projections.
- Keep raw scrape payloads out of runtime card queries.
- Do not store giant all-in-one documents.
- Do not break current V2 until read models are verified.
