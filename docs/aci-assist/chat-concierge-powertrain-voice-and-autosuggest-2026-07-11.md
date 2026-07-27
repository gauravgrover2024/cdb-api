# Chat-First Concierge: Powertrain, Voice and Autosuggest

Date: 2026-07-11

## Problems Confirmed

1. Model-level comparisons selected the first priced variant for each model independently. This allowed a petrol Creta to be compared with a diesel Thar without explanation.
2. Feature follow-ups after a comparison widened back to every model variant instead of keeping the exact variants shown.
3. Public answers still contained implementation language such as `DB-backed`, `indexed feature records`, `current structured data`, and `variants shown`.
4. The existing autocomplete endpoint was authenticated, model-only, ignored context, and added generic query templates even though the progress registry explicitly excludes them.

## Completed

- Preserve an explicitly selected anchor variant during contextual comparisons.
- Match the peer by fuel and transmission, then nearest on-road price, using live read-model rows.
- If a matching transmission is unavailable, keep fuel aligned and explain the transmission fallback.
- If a matching fuel is unavailable, explain the fallback before giving price differences and expose two structured choices: accept the fallback or choose another variant.
- Carry the resolved exact pair into ABS, sunroof, and other feature follow-ups.
- Replace implementation-facing answer wording across comparison, colour, specification, feature, variant-recovery, EMI-recovery, and score no-data paths.
- Add a language-registry guard that rejects implementation terms from rendered public templates.
- Add public GET and POST autosuggest endpoints backed by prewarmed vehicle and feature catalogs.
- Return ranked brand, model, variant, feature, and selected-car action suggestions without generic question templates.
- Cache the data-backed vehicle alias catalog with a TTL and warm it during ACI startup instead of reloading up to 3,000 model summaries during follow-up turns.
- Remove generic repeated-task language from convenience-feature explanations and refresh all 397 published Atlas explainers without Gemini.

## Verified Examples

- Creta-first `vs thar`: Hyundai Creta E petrol/manual is paired with Mahindra Thar LXT 4WD petrol/manual.
- Thar-first `vs creta`: Mahindra Thar AXT RWD Diesel is paired with Hyundai Creta E Diesel, both diesel/manual.
- `which has abs`: answers for the exact selected variants rather than all variants in each model range.
- Punch EV `vs thar`: explicitly says no electric Thar variant was found, identifies the fallback variant, and asks the user to confirm or switch it.
- Warm autosuggest queries complete in about 7-10 ms in the service audit and about 9-10 ms through the local public endpoint.
- The warmed 64-case context stress audit passes with a 1 ms median and 238 ms p95 planning time; cold end-to-end response latency is still reported separately and remains a deployment optimization target.

## Honest Remaining Chat-First Work

Backend and deployment work still open:

- Deployed multi-instance session/context soak testing.
- Safe response-streaming status and cancellation contract.
- Complete multi-intent response orchestration.
- Broader Hindi/Hinglish and adversarial answer-quality evaluation.
- Recently compared and shortlist persistence.
- Generalize the new comparison fallback choices into the shared trust/recovery contract.

Frontend work still open:

- Connect the public autosuggest endpoint to the chat bar.
- Render comparison fallback choices and other clarification cards.
- Add the visible context chip and clear/change action.
- Standardize embedded canvas behavior and build the shortlist/recently-compared tray.

Crash-test, service-network, resale and TCO evidence are intentionally deferred as requested. Absolute final recommendations must remain evidence-gated while those inputs are deferred.
