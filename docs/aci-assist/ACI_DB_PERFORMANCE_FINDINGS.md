# ACI Assist DB Performance Findings

## Current verdict

The database is usable for current V2, but not yet optimized as a world-class public product backend.

## Good

- Main vehicles collection has usable indexes for current model lookup paths.
- vehicle_colors, vehicle_colors_v2, vehicle_features, vehicle_master_records, and vehicles did not show high-risk warnings in the first audit.
- Lead/quotation/customer collections already have some useful indexes.

## Risks

- price_history model-only lookup caused COLLSCAN over 9,143 docs.
- vehicle_variant_feature_matrix_v2 model-only lookup caused COLLSCAN over 2,875 docs.
- vehicle_variant_feature_matrix_v2 documents are heavy; sample around 47 KB, mostly featuresByKey.
- vehicles documents carry raw_price_json, making common queries heavier than needed unless projections are strict.
- offers collection is small now, but model-only lookup caused COLLSCAN.
- Multiple overlapping vehicle/color/feature collections make the runtime source of truth unclear.

## Product rule

ACI Assist public runtime should not depend directly on heavy scraper-shaped documents for common user queries.

## Target direction

Build optimized read models:

- aci_vehicle_model_summary
- aci_vehicle_price_rows
- aci_vehicle_feature_matrix_light
- aci_vehicle_color_gallery
- aci_vehicle_offer_summary
- aci_leads
- aci_conversations

## Next audit

Profile actual ACI Assist tools, not only generic collection explains:

- vehicle_pricelist
- vehicle_feature_lookup
- vehicle_colors
- vehicle_compare
- vehicle_emi
- aci_lead_capture

Goal:
Identify whether latency comes from Gemini/planner, DB queries, data shaping, or response normalization.
