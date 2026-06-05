# ACI Context Manager V1 Final Closure

## Scope

ACI Context Manager v1 owns durable vehicle context for ACI Assist. It separates compact backend memory from display/runtime payloads and routes short model aliases plus spec-attribute questions without hardcoded automotive facts.

This file is currently ignored by the repo ignore rules; force-add it if you want it tracked:

```bash
git add -f docs/aci-assist/ACI_CONTEXT_MANAGER_AUDIT_V1.md
```

## Durable Context Contract

Durable `contextState` uses schema `aci_context_state_v1`. It may contain only compact canonical fields:

- `selectedVehicle`: `make`, `model`, `fullModel`, keys, variant keys, fuel/transmission keys, city, confidence, source.
- `activeComparison`: compact vehicle targets, fuel/transmission keys, requested features, confidence, source.
- `requested`: facts, features, topics, spec attributes, budget, city.
- `anchors`: primary vehicle and comparison targets.
- `provenance`: sources, warnings, isolation, updatedBy.

Forbidden durable payloads: images, galleries, colors, selected color, price ranges, ex-showroom/on-road prices, feature rows, display strings like `fuelText`/`transmissionText`, and runtime/canvas rows.

## Alias Registry

`aciVehicleAliasRegistry.service.js` centralizes interpretation-only aliases:

- `be 6e`, `be6e`, `mahindra be 6e`, `mahindra be6e` -> `Mahindra Be 6`
- `eqs`, `mercedes eqs`, `mercedes benz eqs` -> `Mercedes Benz Eqs`
- `ix`, `bmw ix` -> `Bmw Ix`

Each alias is accepted only after `aci_vehicle_model_summary` has a matching canonical row. The registry does not contain feature, price, spec, or variant facts.

## Spec Resolver

`aciVehicleSpecAttributeResolver.service.js` recognizes spec topics such as range, battery capacity, charging time, boot space, ground clearance, dimensions, mileage, tank capacity, and seating capacity.

Contract:

- intent: `vehicle_spec_attribute_answer`
- tool: `vehicle_spec_attribute_lookup`
- data: `anchorMake`, `anchorModel`, `anchorFullModel`, `attributeKey`, `attributeLabel`, `values`, `missingData`, `recordCount`, `sourceTransparency`

If exact values are not indexed, the resolver returns a model-anchored missing-data answer. It must not infer a value from feature presence.

## Routing

- Model plus spec topic -> `vehicle_spec_attribute_lookup`
- Model plus feature topic -> `vehicle_feature_lookup`
- Price/on-road -> price tool
- Comparison follow-up -> comparison route with preserved targets
- No grounded model/topic -> clarification

Validated examples:

- `be 6e sunroof` -> Mahindra Be 6, feature lookup
- `mahindra be6e sunroof` -> Mahindra Be 6, feature lookup
- `eqs range`, `mercedes eqs range` -> Mercedes Benz Eqs, spec lookup
- `ix range`, `bmw ix range` -> Bmw Ix, spec lookup

## Merge Authority

Context authority order is:

1. previous context
2. context manager
3. explicit user patch
4. tool/runtime canonical data

Every patch is compacted before becoming durable `contextState`. Rich runtime data may still appear in response widgets/canvas payloads, but not in `contextState`.

## Frontend Boundary

The frontend request builders now compact outbound `contextState` / `aciContextState` through the same durable shape. No UI redesign was made. Top-level display payloads remain app-local; durable state sent back to backend is compact.

## Permanent Gates

Fast safety includes `aci:context-manager:audit`. The summary exposes:

- `contextManagerExitCode`
- context manager duration
- context manager suite summary
- `slowSuites`

Fast safety fails if:

- context manager > 15000ms
- model alias feature queries > 60000ms
- embarrassment queries > 60000ms
- total fast safety > 90000ms

Public-chat checks live in `aci:context-manager:public-smoke` and require a backend already running on port 5050.

## Known Future Work

- Replace the audit-only lightweight candidate harness with a shared bounded candidate service if more pure routing suites need it.
- Expand indexed spec field mappings when new read-model fields are introduced.
- Continue moving frontend display-only context handling away from request payloads.
