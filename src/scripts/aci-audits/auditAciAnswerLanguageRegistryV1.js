import {
  ACI_ANSWER_LANGUAGE_REGISTRY,
} from "../../services/aciCore/language/aciAnswerLanguageRegistry.js";
import {
  renderAciTemplate,
  selectAciLanguageVariant,
} from "../../services/aciCore/language/aciAnswerLanguageComposer.js";

const failures = [];

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const includesAll = (text = "", values = []) => {
  const lower = text.toLowerCase();
  return values.every((value) => lower.includes(String(value || "").toLowerCase()));
};

const formatList = (value, fallback = "") => {
  const items = asArray(value).map(clean).filter(Boolean);
  if (!items.length) return fallback;
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
};

const renderVariantTextForAudit = (text = "", input = {}) => {
  const missingCount = Number(input.missingCount ?? input.unavailableCount ?? 0);
  const normalized = {
    ...input,
    model: input.model || "this model",
    topic: input.topic || "this topic",
    city: input.city || input.requestedCity || "that city",
    values: formatList(input.values, input.value || "the available value"),
    supportedCities: formatList(input.supportedCities, "supported cities"),
    firstSupportedCity: asArray(input.supportedCities)[0] || "a supported city",
    actions: formatList(input.actions, "supported next steps"),
    vehicleA: input.vehicleA || asArray(input.vehicles)[0] || "the first car",
    vehicleB: input.vehicleB || asArray(input.vehicles)[1] || "the second car",
    availableCount: input.availableCount ?? 0,
    totalCount: input.totalCount ?? 0,
    missingCount,
    missingVariantWord: missingCount === 1 ? "variant" : "variants",
    variantCount: input.variantCount ?? input.totalVariants ?? 0,
    priceLine: input.priceLine || "price data is available.",
    differenceLine: input.differenceLine || "The available comparison table has the details.",
  };
  normalized.comparisonLabel =
    input.comparisonLabel ||
    (normalized.vehicleA && normalized.vehicleB
      ? `${normalized.vehicleA} vs ${normalized.vehicleB}`
      : "this comparison");

  return clean(
    String(text || "").replace(/{{\s*([a-zA-Z0-9_.-]+)\s*}}/g, (_, key) =>
      clean(normalized[key] ?? key),
    ),
  );
};

const SAMPLE_INPUTS = Object.freeze({
  resolved_feature_available_summary: {
    model: "Mahindra Be 6",
    topic: "panoramic sunroof",
    availableCount: 17,
    totalCount: 20,
    missingCount: 3,
  },
  resolved_spec_value_summary: {
    model: "Mercedes Benz Eqs",
    topic: "range",
    values: ["813 km", "857 km"],
  },
  resolved_spec_missing_summary: {
    model: "Mercedes-Benz EQS",
    topic: "driving range",
  },
  comparison_summary: {
    vehicleA: "Hyundai Creta E",
    vehicleB: "Kia Seltos HTE",
    priceLine: "The price table is available.",
    differenceLine: "Feature differences are available in the comparison data.",
  },
  price_summary: {
    model: "Hyundai Creta SX",
    city: "Noida",
    priceLine: "the on-road price is available in the card.",
  },
  pricelist_summary: {
    model: "Hyundai Creta",
    city: "Gurgaon",
    variantCount: 12,
  },
  unsupported_city_price: {
    city: "Mumbai",
    supportedCities: ["Delhi", "Noida", "Gurgaon"],
  },
  clarification_known_model_missing_topic: {
    model: "Hyundai Creta",
  },
  clarification_known_topic_missing_model: {
    topic: "sunroof",
  },
  comparison_followup_context_ack: {
    vehicleA: "Hyundai Creta",
    vehicleB: "Kia Seltos",
  },
  generic_no_data_but_can_help: {
    topic: "dealer inventory",
  },
  next_action_prompts: {
    topic: "price",
    actions: ["variants", "EMI", "quote"],
  },
});

const FORBIDDEN_RENDERED_PATTERNS = [
  /indexed spec value/i,
  /buy this/i,
  /clear winner/i,
  /best choice/i,
  /New Delhi price/i,
  /Delhi on-road/i,
];

for (const [templateKey, template] of Object.entries(ACI_ANSWER_LANGUAGE_REGISTRY)) {
  if (template.key !== templateKey) {
    failures.push(`${templateKey}: key field mismatch`);
  }

  if (!clean(template.purpose)) failures.push(`${templateKey}: missing purpose`);
  if (!clean(template.tone)) failures.push(`${templateKey}: missing tone`);
  if (!Array.isArray(template.requiredInputs)) failures.push(`${templateKey}: missing requiredInputs`);
  if (!Array.isArray(template.guardrails)) failures.push(`${templateKey}: missing guardrails`);

  const variants = asArray(template.variants);
  if (variants.length < 3) {
    failures.push(`${templateKey}: expected at least 3 variants, got ${variants.length}`);
  }

  const sample = SAMPLE_INPUTS[templateKey] || {};
  variants.forEach((variant, index) => {
    if (!clean(variant.id)) failures.push(`${templateKey}[${index}]: missing variant id`);
    if (!clean(variant.text)) failures.push(`${templateKey}[${index}]: missing variant text`);

    const rendered = renderVariantTextForAudit(variant.text, sample);

    if (/{{[^}]+}}/.test(rendered)) {
      failures.push(`${templateKey}[${variant.id}]: unresolved placeholder in "${rendered}"`);
    }

    const forbidden = FORBIDDEN_RENDERED_PATTERNS.find((pattern) => pattern.test(rendered));
    if (forbidden) {
      failures.push(`${templateKey}[${variant.id}]: forbidden wording ${forbidden} in "${rendered}"`);
    }
  });
}

const renderedSpec = renderAciTemplate(
  "resolved_spec_value_summary",
  SAMPLE_INPUTS.resolved_spec_value_summary,
  { seed: "spec-value-audit" },
).text;
if (!includesAll(renderedSpec, ["Mercedes Benz Eqs", "range"]) || !/813 km|857 km/i.test(renderedSpec)) {
  failures.push(`resolved_spec_value_summary: expected model/topic/value in "${renderedSpec}"`);
}

const renderedComparison = renderAciTemplate(
  "comparison_summary",
  SAMPLE_INPUTS.comparison_summary,
  { seed: "comparison-audit" },
).text;
if (!includesAll(renderedComparison, ["Hyundai Creta E", "Kia Seltos HTE"])) {
  failures.push(`comparison_summary: expected both vehicles in "${renderedComparison}"`);
}

const renderedMissing = renderAciTemplate(
  "resolved_spec_missing_summary",
  {
    ...SAMPLE_INPUTS.resolved_spec_missing_summary,
    values: ["813 km"],
  },
  { seed: "missing-audit" },
).text;
if (/813 km|857 km/i.test(renderedMissing)) {
  failures.push(`resolved_spec_missing_summary: invented or leaked value in "${renderedMissing}"`);
}

const renderedUnsupportedCity = renderAciTemplate(
  "unsupported_city_price",
  SAMPLE_INPUTS.unsupported_city_price,
  { seed: "unsupported-city-audit" },
).text;
if (!includesAll(renderedUnsupportedCity, ["Mumbai", "Delhi", "Noida", "Gurgaon"])) {
  failures.push(`unsupported_city_price: expected requested and supported cities in "${renderedUnsupportedCity}"`);
}

const variants = ACI_ANSWER_LANGUAGE_REGISTRY.resolved_spec_value_summary.variants;
const stableA = selectAciLanguageVariant({
  templateKey: "resolved_spec_value_summary",
  variants,
  seed: "same-seed",
});
const stableB = selectAciLanguageVariant({
  templateKey: "resolved_spec_value_summary",
  variants,
  seed: "same-seed",
});
if (stableA?.id !== stableB?.id) {
  failures.push("deterministic selection: same seed returned different variants");
}

let varied = false;
for (let index = 0; index < 20; index += 1) {
  const candidate = selectAciLanguageVariant({
    templateKey: "resolved_spec_value_summary",
    variants,
    seed: `different-seed-${index}`,
  });
  if (candidate?.id && candidate.id !== stableA?.id) {
    varied = true;
    break;
  }
}
if (!varied) {
  failures.push("deterministic selection: different seeds did not vary across checked sample");
}

const nonRepeated = selectAciLanguageVariant({
  templateKey: "resolved_spec_value_summary",
  variants,
  seed: "same-seed",
  previousVariantId: stableA?.id,
});
if (variants.length > 1 && nonRepeated?.id === stableA?.id) {
  failures.push("deterministic selection: previousVariantId was repeated despite alternatives");
}

if (failures.length) {
  console.error("ACI Answer Language Registry v1 audit failed:");
  failures.forEach((failure) => console.error(`- ${failure}`));
  process.exit(1);
}

console.log("ACI Answer Language Registry v1 audit passed.");
console.log(`Templates checked: ${Object.keys(ACI_ANSWER_LANGUAGE_REGISTRY).length}`);
