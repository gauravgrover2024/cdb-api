# ACI Answer Language Registry v1 Audit

Snapshot saved before edits with:

`git diff > /tmp/aci_before_answer_composer_language_registry_v1_$(date +%Y%m%d_%H%M%S).diff`

Scope: active backend source paths under `src/services`, `src/scripts`, and `package.json`. Deprecated `aiAgent/_deprecated_v1` and `_legacy_backup_before_v2` paths were scanned but not targeted for migration.

| Source file | Old wording | Proposed template key | Migrated | Reason if not migrated yet |
| --- | --- | --- | --- | --- |
| `src/services/aciCore/specs/aciResolvedTopicAnswerUx.service.js` | `I found {model}. You're asking about {attribute}. In the current vehicle data...` | `resolved_spec_value_summary` | yes | Central resolver UX now calls `renderAciTemplate`. |
| `src/services/aciCore/specs/aciResolvedTopicAnswerUx.service.js` | `I don't have the exact certified {attribute} value... so I won't guess.` | `resolved_spec_missing_summary` | yes | Central resolver UX now calls `renderAciTemplate`. |
| `src/services/aiAgent/aiAgent.answerComposer.js` | `I found {model}. You're asking about {attribute}...` | `resolved_spec_value_summary` | yes | Final composer uses registry for spec value answers. |
| `src/services/aiAgent/aiAgent.answerComposer.js` | `I don't have the exact certified {attribute} value...` | `resolved_spec_missing_summary` | yes | Final composer uses registry for spec missing answers. |
| `src/services/aiAgent/tools/newCars/vehicleFeatures.tool.js` | `{model} offers {feature} on {available} of {total} current variants...` | `resolved_feature_available_summary` | yes | Mixed available/unavailable feature summary now uses registry. |
| `src/services/aiAgent/aiAgent.answerComposer.js` | `I compared {a} and {b}. {priceLine} {differenceLine}` | `comparison_summary` | yes | Final comparison summary now uses registry. |
| `src/services/aiAgent/aiAgent.answerComposer.js` | `I compared {a} for {scope} on {features}...` | `comparison_summary` | yes | Feature comparison summary now uses registry while preserving structured scope. |
| `src/services/aiAgent/aiAgent.responseTools.js` | `I compared {compareLabel} with price and feature/spec differences.` | `comparison_summary` | partial | Final composer migrates resolved `vehicle_comparison`; the response tool fallback remains for risky pre-compose paths. |
| `src/services/aiAgent/aiAgent.answerComposer.js` | `I found {n} {model} variants in {city}...` | `pricelist_summary` | yes | Final composer uses registry for price-list answers. |
| `src/services/aiAgent/tools/newCars/vehiclePricelist.tool.js` | `I found {n} {model} variants in {city}...` | `pricelist_summary` | yes | V2 price widget summary now uses registry. |
| `src/services/aiAgent/aiAgent.answerComposer.js` | `For {vehicle} in {city}, the on-road price...` | `price_summary` | yes | Final composer uses registry for price summary answers. |
| `src/services/aiAgent/tools/newCars/vehiclePricelist.tool.js` | `I found the {title} for {city}.` | `price_summary` | yes | Exact price widget summary now uses registry. |
| `src/services/aiAgent/tools/newCars/vehiclePricelist.tool.js` | `I don't have live on-road pricing for {city} yet...` | `unsupported_city_price` | yes | Unsupported city tool response now uses registry. |
| `src/services/aciCore/integration/aciCoreLiveBridge.service.js` | `I don't have live on-road pricing for {city} yet...` | `unsupported_city_price` | yes | Fast unsupported-city bridge response now uses registry. |
| `src/services/aiAgent/aiAgent.responseTools.js` | `Can you clarify what you want to check?` | `clarification_known_model_missing_topic`, `clarification_known_topic_missing_model` | partial | Known model/topic cases use registry; explicit planner/runtime clarification questions still pass through. |
| `src/services/aiAgent/aiAgent.responseTools.js` | `This request needs data or functionality...` | `generic_no_data_but_can_help` | yes | Generic unavailable fallback now uses registry. |
| `src/services/aciCore/integration/aciCoreLiveBridge.service.js` | Comparison follow-up message expansion for `which one is better?` | `comparison_followup_context_ack` | no | This path expands routing text, not final answer text; forcing answer copy here would risk duplicate targets. Covered by registry audit and stress e2e checks. |
| `src/services/aiAgent/aiAgent.responseTools.js` | Verified offers / service center / finance scheme unavailability copy | `generic_no_data_but_can_help` | no | Deep unsupported-intent wording is intentionally specific and lower priority for v1; left to avoid broad behavior changes. |
| `src/services/aiAgent/aiAgent.contractNormalizer.js` | `I found stored feature data for...`, `I found {n} matching feature records.` | `generic_no_data_but_can_help`, feature summary keys | no | Contract normalizer fallback strings are defensive repair paths; migration could mask upstream contract issues. |
| `src/services/aiAgent/aiAgent.featurePayloadBuilder.js` | Feature explorer / discovery answer summaries | `resolved_feature_available_summary`, `next_action_prompts` | no | Larger feature discovery wording surface; v1 migrated the direct availability summary first. |
| `src/services/aiAgent/aiAgent.featureComparisonAnswer.js` | `I compared {names} on {n} features...` | `comparison_summary` | no | This builds multi-line structured comparison rows; final composer handles common comparison summary, but row-level wording remains structured output. |
| `src/services/aiAgent/aiAgent.responseSanitizer.js` | Sanitizer fallback copy for budget/features and multi-intent messages | `generic_no_data_but_can_help`, `next_action_prompts` | no | Sanitizer is a guardrail/repair layer; migrating it broadly in v1 risks changing existing passing behavior. |
| `src/services/aiAgent/aiAgent.newCarQuestionMap.js` | Example questions and UI labels involving price/offers/comparison | `next_action_prompts` | no | Mostly planner examples/config labels, not final buyer answer text. |
| `src/services/aiAgent/aiAgent.planSchema.js` | Planner guardrails and unavailable reasons | `generic_no_data_but_can_help` | no | Prompt/schema guardrails are not final buyer answer text. |
| `src/scripts/aci-audits/auditAciContextManagerStressV1.js` | Hardcoded expected/preview resolved topic wording | `resolved_spec_value_summary`, `resolved_spec_missing_summary` | partial | Buyer-friendly matcher now accepts registry variants; preview remains an internal audit approximation. |
| `src/scripts/aci-audits/smokeAciContextPublicChatV1.cjs` | Smoke expected forbidden wording | registry audit coverage | no | Test assertion strings are not customer-facing; left as regression checks. |
| `src/services/aciProgress/aciProgress.registry.cjs` | Progress module descriptions mention price/cities/features | none | no | Operational progress text, not ACI Assist buyer answer copy. |

Central v1 templates added:

- `resolved_feature_available_summary`
- `resolved_spec_value_summary`
- `resolved_spec_missing_summary`
- `comparison_summary`
- `price_summary`
- `pricelist_summary`
- `unsupported_city_price`
- `clarification_known_model_missing_topic`
- `clarification_known_topic_missing_model`
- `comparison_followup_context_ack`
- `generic_no_data_but_can_help`
- `next_action_prompts`

Validation added:

- `src/scripts/aci-audits/auditAciAnswerLanguageRegistryV1.js`
- `npm run aci:answer-language:audit`
- Included in `runAciSafetyGateSummary.js` fast suite.
