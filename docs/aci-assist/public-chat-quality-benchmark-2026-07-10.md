# Public Chat Quality Benchmark

Date: 2026-07-10

## Method

The same two prompts were run against ACI Assist, public logged-out ChatGPT, and public logged-out Gemini Flash:

1. A complete Delhi family-SUV recommendation brief with an on-road cap, petrol automatic, 1,200 km/month, safety priority, six airbags and sunroof.
2. `What is ABS in a car, why does it matter, and what can it not do?`

This is a point-in-time behavioral check, not a permanent model ranking.

## Recommendation Result

ACI considered 23 models and retained 76 exact variants after the requested city, on-road budget, fuel, transmission, must-have feature, source and freshness gates. It gave one exact-variant pick, one different-brand alternative, exact indexed on-road prices and an explicit warning that independent crash-test applicability was not verified for the winning variant.

ChatGPT gave four useful, well-explained options and chose a Creta IVT. Gemini gave two polished options and chose a Kushaq AT. Both public systems were richer in immediate editorial prose, but relied on estimated prices and several market, comfort, service, mileage or safety claims that were not consistently exact-variant scoped. Gemini called one option the safest choice and used crash-rating applicability more confidently than ACI's current evidence permits.

Honest result:

- ACI wins strict filter enforcement, exact-variant proof, current Atlas traceability and uncertainty handling.
- ChatGPT and Gemini still win breadth of narrative and ownership commentary.
- ACI's verdict is now comparable in decisiveness, but it deliberately refuses to call a car the `safest` until exact tested-version applicability is trustworthy.

## ABS Result

All three systems correctly explained wheel-lock prevention, retained steering control and physical limits. ChatGPT and Gemini supplied a longer driving example and pedal-feedback description. ACI was shorter, source-backed and avoided Gemini's unsafe phrase that ABS plays a role in `preventing accidents`. The Codex-authored ABS record now also explains normal pedal pulsing, loose-surface stopping-distance trade-offs, firm braking and why tyres, ESC and crash evidence still matter.

## Current Gap

ACI should not be described as universally better than general-purpose models yet. It is stronger where exact vehicle truth and hard filters matter; it is approximately at parity for high-frequency feature education; and it remains behind on broad ownership/service/resale narrative because those claims are intentionally withheld until corresponding evidence collections are production-grade.

## Gates

- `npm run -s aci:decision:final-recommendation:smoke`
- `npm run -s aci:feature-explainer:coverage`
- `npm run -s aci:feature-explainer:smoke`
- `npm run -s aci:no-hardcoded-facts:audit`
- `npm run -s aci:safety:fast`
