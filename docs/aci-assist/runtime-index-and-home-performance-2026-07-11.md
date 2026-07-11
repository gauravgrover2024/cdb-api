# ACI Assist Runtime Index and Home Performance Audit

Date: 2026-07-11
Database: Atlas `cdrive`

## Scope

The audit inspected 51 Atlas collections and ran detailed checks against 30 ACI-related collections. The five core read models were healthy before this change:

| Collection | Documents | Indexes before change |
| --- | ---: | ---: |
| `aci_vehicle_model_summary` | 695 | 9 |
| `aci_vehicle_price_rows` | 5,516 | 15 |
| `vehicle_feature_catalog_v2` | 397 | 6 |
| `vehicle_variant_feature_matrix_v2` | 3,003 | 45 |
| `vehicle_colors_v2` | 265 | 16 |

The broad audit's legacy text-field probes produced several `COLLSCAN` warnings that do not represent the current runtime. ACI Assist uses normalized keys such as `modelKey`, `variantKey`, `citySlug`, `fuelKey`, and `transmissionKey`. Those actual query shapes were checked separately with `explain("executionStats")`.

## Findings

1. Model, exact variant, feature catalog, feature explainer, comparison matrix, colour, and media lookups already had relevant indexes.
2. On-road budget discovery lacked compound indexes matching its city, body type, fuel, transmission, price-range, and sort shapes.
3. The popular-cars home feed rebuilt data from monthly sales, raw vehicle pricing, and colour media after a process cold start. A production cached response carried an original build time of about 1.8 seconds; a local live rebuild took 3.4 seconds.
4. The public Cloudflare R2 car images had no `Cache-Control` metadata, forcing unnecessary revalidation or downloads.
5. `vehicle_variant_feature_matrix_v2` is already heavily indexed. Adding more speculative indexes would increase write and storage cost without improving a demonstrated runtime query.

## Changes

- Added three compound indexes for actual on-road recommendation query shapes.
- Added an indexed `aci_home_popular_cars_v1` persistent snapshot, rebuilt by the post-data-refresh pipeline.
- Added browser, CDN, and Vercel CDN caching headers to popular cars and vehicle media responses.
- Applied `public, max-age=31536000, immutable` to all 2,863 referenced normalized R2 images and added the same metadata to future scraper uploads.
- Added idempotent index ensure, runtime explain audit, home snapshot build, and R2 metadata audit/write commands.

## Verification

Atlas query plans after index creation:

| Runtime shape | Plan | Documents examined | Keys examined | Atlas execution |
| --- | --- | ---: | ---: | ---: |
| City + on-road budget | `IXSCAN` | 25 | 25 | 1 ms |
| City + body type + on-road budget | `IXSCAN` | 25 | 25 | 1 ms |
| City + fuel + transmission + on-road budget | `IXSCAN` | 13 | 13 | 1 ms |
| Home snapshot by cache key | `EXPRESS_IXSCAN` | 1 | 1 | 0 ms |

Fresh local backend process against live Atlas:

| Endpoint | First request | Memory-cached request |
| --- | ---: | ---: |
| Popular cars home feed | 482 ms | 2 ms |
| Tata Punch media/colours | 249 ms | 2 ms |

The first-request figures include network travel to Atlas. Edge-cached requests should avoid both Node and Atlas after deployment. A sampled public R2 image now returns the immutable one-year cache policy, and a 25-object post-migration audit found all 25 current.

## Operational Notes

- Run `npm run aci:runtime-indexes` after schema or query-shape changes.
- Run `npm run aci:runtime-index-audit` to detect missing indexes or runtime collection scans.
- `npm run aci:post-data-refresh` now rebuilds the home snapshot automatically.
- Run `npm run aci:images:cache-control:audit` after image migrations; use the write command only when the audit reports missing metadata.
- Atlas/network geography and serverless cold starts still set the uncached latency floor. The persistent snapshot and CDN headers reduce their user-visible impact; they do not eliminate physical network latency.
