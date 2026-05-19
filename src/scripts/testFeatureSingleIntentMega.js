import dotenv from "dotenv";
import fs from "fs";
import mongoose from "mongoose";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const MAX_TESTS = Number(process.env.ACI_FEATURE_MEGA_MAX || 420);
const REQUESTED_WORKERS = Number(process.env.ACI_FEATURE_MEGA_WORKERS || 1);
const WORKERS =
  Number.isFinite(REQUESTED_WORKERS) && REQUESTED_WORKERS > 0
    ? Math.floor(REQUESTED_WORKERS)
    : 1;
const OUT_JSON = "/tmp/aci_feature_single_intent_mega.json";
const OUT_MD = "/tmp/aci_feature_single_intent_mega.md";

const IMPORTANT_MODELS = [
  { make: "Hyundai", model: "Creta" },
  { make: "Hyundai", model: "Verna" },
  { make: "Kia", model: "Seltos" },
  { make: "Mahindra", model: "Thar" },
  { make: "Honda", model: "Elevate" },
  { make: "Honda", model: "City" },
  { make: "Maruti", model: "Brezza" },
  { make: "Tata", model: "Nexon" },
  { make: "Tata", model: "Punch" },
  { make: "Mahindra", model: "Scorpio N" },
  { make: "Mahindra", model: "XUV700" },
  { make: "Toyota", model: "Hyryder" },
];

const IMPORTANT_FEATURES = [
  {
    canonical: "sunroof",
    phrases: [
      "sunroof",
      "single pane sunroof",
      "panoramic sunroof",
      "moonroof",
      "roof window",
      "sunrrof",
      "sonroof",
      "sunnroof",
    ],
  },
  {
    canonical: "ADAS",
    phrases: [
      "ADAS",
      "adas",
      "adaptive cruise",
      "lane assist",
      "forward collision warning",
      "driver assistance",
      "collision warning",
      "autonomous emergency braking",
    ],
  },
  {
    canonical: "6 airbags",
    phrases: ["6 airbags", "six airbags", "airbags", "curtain airbags", "side airbags"],
  },
  {
    canonical: "360 camera",
    phrases: ["360 camera", "360 degree camera", "surround camera", "parking camera"],
  },
  {
    canonical: "rear camera",
    phrases: ["rear camera", "reverse camera", "parking camera", "rear parking camera"],
  },
  {
    canonical: "ventilated seats",
    phrases: [
      "ventilated seats",
      "ventilated seat",
      "cooled seats",
      "seat ventilation",
      "ventillated seets",
      "ventilated seets",
    ],
  },
  {
    canonical: "wireless charger",
    phrases: ["wireless charger", "wireless charging", "phone charger", "wireless mobile charging"],
  },
  {
    canonical: "cruise control",
    phrases: ["cruise control", "adaptive cruise", "cruise"],
  },
  {
    canonical: "connected car",
    phrases: ["connected car", "connected features", "blue link", "connected tech"],
  },
  {
    canonical: "alloy wheels",
    phrases: ["alloy wheels", "alloys", "diamond cut alloys"],
  },
  {
    canonical: "LED headlamps",
    phrases: ["LED headlamps", "LED headlights", "projector headlamps", "headlights"],
  },
  {
    canonical: "rear AC vents",
    phrases: ["rear ac vents", "rear vents", "back ac vents"],
  },
  {
    canonical: "automatic climate control",
    phrases: ["automatic climate control", "climate control", "auto ac"],
  },
  {
    canonical: "hill hold",
    phrases: ["hill hold", "hill assist", "hill start assist"],
  },
  {
    canonical: "TPMS",
    phrases: ["TPMS", "tyre pressure monitor", "tire pressure monitoring"],
  },
  {
    canonical: "Bose speakers",
    phrases: ["bose speakers", "premium speakers", "branded audio", "sound system"],
  },
];

const TYPO_MODELS = {
  Verna: ["vrna", "vernaa", "vern a", "vrena", "vernaaa", "verma"],
  Creta: ["creta", "cretaa", "cretaaa", "cretta", "cretae", "cre ta"],
  Seltos: ["seltos", "selto", "seltosss", "sel tos", "sletos", "sletos"],
  Thar: ["thar", "tha r", "tharr", "tar", "thaar"],
  Elevate: ["elevate", "elevte", "elvate", "ele vate"],
  City: ["city", "cty", "honda cty"],
  Nexon: ["nexon", "nexn", "naxon"],
  Brezza: ["brezza", "breza", "breeza"],
};

const GENERIC_BAD_ANSWER_PHRASES = [
  "i found a result for you",
  "appears in the matching",
  "matching feature records",
  "please confirm the exact variant in the feature card",
  "backend",
  "records",
  "unable to determine",
  "not sure",
];

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const normalizeKey = (value = "") =>
  clean(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const uniqueByQuery = (tests = []) => {
  const seen = new Set();
  return tests.filter((test) => {
    const key = normalizeKey(test.q);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const includesAny = (value = "", phrases = []) => {
  const text = normalizeKey(value);
  return phrases.some((phrase) => text.includes(normalizeKey(phrase)));
};

const getWidget = (response = {}) =>
  response.widget || toArray(response.widgets)[0] || {};

const getRows = (response = {}, widget = {}) =>
  toArray(
    response.rows ||
      response.items ||
      response.features ||
      response.data?.rows ||
      response.data?.items ||
      response.data?.features ||
      widget.rows ||
      widget.items ||
      widget.features ||
      widget.featureList ||
      widget.matchedVariants ||
      widget.data?.rows ||
      widget.data?.features,
  );

const getFeatureGroups = (response = {}, widget = {}) =>
  toArray(
    response.featureGroups ||
      response.groups ||
      response.data?.featureGroups ||
      response.data?.groups ||
      widget.featureGroups ||
      widget.groups ||
      widget.data?.featureGroups ||
      widget.data?.groups,
  );

const getVariantOptions = (response = {}, widget = {}) =>
  toArray(
    widget.variantOptions ||
      widget.variants ||
      response.data?.variantOptions ||
      response.data?.variants,
  );

const getAnswer = (response = {}, widget = {}) =>
  clean(response.answer || response.text || response.message || widget.answer || widget.summary || "");


const isAllowedEmptyFeatureResult = (test = {}, answer = "") => {
  const kind = String(test.kind || "");
  const query = normalizeKey(test.q || test.query || "");
  const actualIntent = String(test.actualIntent || "");

  // Valid case:
  // User asks which variants miss/do not have a feature.
  // If every current variant has that feature, rows = 0 is the correct result.
  if (
    kind === "negative_feature_discovery" &&
    (query.includes("do not have") || query.includes("miss"))
  ) {
    return true;
  }

  return false;
};

const expectedFor = {
  modelFeatureAnswer: ["vehicle_feature_answer", "vehicle_feature_discovery"],
  variantFeatureAnswer: ["vehicle_feature_answer"],
  discovery: ["vehicle_feature_discovery"],
  explorer: ["vehicle_model_features_explorer"],
  category: ["vehicle_model_features_explorer", "vehicle_feature_discovery"],
  comparison: ["vehicle_feature_comparison", "comparison_canvas", "vehicle_model_features_explorer", "vehicle_feature_discovery"],
  recommendation: ["vehicle_recommendation", "vehicle_feature_discovery", "vehicle_model_features_explorer"],
};

const templates = {
  modelAvailability: [
    "{model} {feature}",
    "Does {model} have {feature}?",
    "Is {feature} available in {model}?",
    "Does {model} come with {feature}?",
    "Do we get {feature} in {model}?",
    "{model} me {feature} hai kya?",
    "{model} mein {feature} milta hai?",
    "{model} {feature} available?",
    "Check {feature} in {model}",
    "Tell me if {model} has {feature}",
    "Is {feature} standard in {model}?",
    "Is {feature} optional in {model}?",
    "Does the base {model} get {feature}?",
    "Does top model of {model} have {feature}?",
  ],

  variantAvailability: [
    "Does {model} {variant} have {feature}?",
    "{model} {variant} {feature}",
    "Is {feature} available in {model} {variant}?",
    "{model} {variant} me {feature} hai kya?",
    "Does {variant} variant of {model} get {feature}?",
    "Check {feature} in {model} {variant}",
  ],

  discovery: [
    "Which {model} variants have {feature}?",
    "Which variants of {model} get {feature}?",
    "{feature} available in which {model} variant?",
    "{model} {feature} variants",
    "Cheapest {model} variant with {feature}",
    "Lowest price {model} with {feature}",
    "Kis {model} variant me {feature} hai?",
    "{model} ke kis variant me {feature} milta hai?",
    "Show {model} variants with {feature}",
    "List {model} variants having {feature}",
  ],

  negativeDiscovery: [
    "Which {model} variants do not have {feature}?",
    "Which {model} variants miss {feature}?",
    "Which {model} variants don't get {feature}?",
    "{model} variants without {feature}",
    "Base variants of {model} without {feature}",
  ],

  valueCheck: [
    "Is {model} {feature} single pane or panoramic?",
    "What type of {feature} does {model} get?",
    "{model} {feature} type",
    "Is {feature} in {model} basic or premium?",
    "Does {model} have {feature} or only a lower version?",
  ],

  explorer: [
    "Show features of {model}",
    "{model} features",
    "Show all features of {model}",
    "Open feature explorer for {model}",
    "List all features of {model}",
    "Show full feature list of {model}",
    "Show features of {model} {variant}",
    "{model} {variant} features",
    "Show all features of {model} {variant}",
  ],

  category: [
    "Show safety features of {model}",
    "Show comfort features of {model}",
    "Show infotainment features of {model}",
    "Show exterior features of {model}",
    "Show engine features of {model}",
    "Show dimensions and capacity of {model}",
    "{model} safety features",
    "{model} comfort features",
    "{model} infotainment features",
    "{model} exterior features",
  ],

  buyingAdvisor: [
    "Best {model} variant for safety features",
    "Best {model} variant for family safety",
    "Which {model} variant gives most features for the money?",
    "Which {model} variant has best rear seat comfort?",
    "Which {model} variant has best night driving features?",
    "Which {model} variant should I buy for features?",
    "Is it worth upgrading in {model} for better features?",
  ],

  comparison: [
    "Compare features of {model} {variantA} and {variantB}",
    "Difference between {model} {variantA} and {variantB} features",
    "{model} {variantA} vs {variantB} features",
    "What extra features do I get in {model} {variantB} over {variantA}?",
    "Should I upgrade from {model} {variantA} to {variantB} for features?",
  ],

  crossModel: [
    "Which is better for features, {model} or {modelB}?",
    "Compare {model} and {modelB} features",
    "{model} vs {modelB} safety features",
    "{model} vs {modelB} sunroof",
    "{model} vs {modelB} ADAS",
  ],

  budgetFeature: [
    "Cars under 20 lakh with {feature}",
    "SUVs under 20 lakh with {feature}",
    "Automatic cars under 18 lakh with {feature}",
    "Safest cars under 15 lakh with {feature}",
    "Best cars with {feature} under 20 lakh",
  ],

  typoIntent: [
    "{typoModel} prc list",
    "{typoModel} pricelist",
    "{typoModel} price list",
    "{typoModel} feature",
    "{typoModel} featuers",
    "{typoModel} {typoFeature}",
    "{typoModel} me {typoFeature} hai kya",
    "which {typoModel} variants have {typoFeature}",
  ],

  context: [
    "does it have {feature}?",
    "which variants have it?",
    "show all safety features",
    "does base variant get it?",
    "show cheapest variant with it",
    "is it worth upgrading for it?",
  ],
};

const fill = (template, data) =>
  template.replace(/\{([^}]+)\}/g, (_, key) => clean(data[key] || ""));

const pickVariantsForModel = async (model) => {
  const db = mongoose.connection.db;
  const rows = await db
    .collection("vehicles")
    .find(
      {
        $or: [
          { model: new RegExp(`^${model}$`, "i") },
          { model_normalized: new RegExp(`^${model}$`, "i") },
        ],
      },
      {
        projection: {
          variant: 1,
          variant_short: 1,
          variant_normalized: 1,
          ex_showroom: 1,
          active: 1,
          is_active: 1,
          is_discontinued: 1,
        },
      },
    )
    .limit(80)
    .toArray();

  const variants = rows
    .filter((row) => row.is_discontinued !== true && row.active !== false && row.is_active !== false)
    .map((row) => clean(row.variant_short || row.variant_normalized || row.variant))
    .filter(Boolean);

  return [...new Set(variants)].slice(0, 8);
};

const buildTests = async () => {
  const tests = [];

  const modelVariants = {};

  for (const car of IMPORTANT_MODELS) {
    modelVariants[car.model] = await pickVariantsForModel(car.model);
    if (!modelVariants[car.model].length) {
      modelVariants[car.model] = ["Base", "Mid", "Top"];
    }
  }

  for (const car of IMPORTANT_MODELS) {
    const model = car.model;
    const variants = modelVariants[model];
    const variant = variants[Math.floor((variants.length - 1) / 2)] || variants[0];
    const variantA = variants[0] || variant;
    const variantB = variants[Math.min(variants.length - 1, Math.floor(variants.length / 2))] || variant;

    for (const featureEntry of IMPORTANT_FEATURES) {
      const feature = featureEntry.canonical;
      const phrase = featureEntry.phrases[0];
      const typoFeature = featureEntry.phrases.find((item) => item !== phrase) || phrase;

      for (const template of templates.modelAvailability.slice(0, 5)) {
        tests.push({
          tier: "core",
          kind: "model_feature_answer",
          q: fill(template, { model, feature: phrase }),
          model,
          feature,
          expectedIntents: expectedFor.modelFeatureAnswer,
        });
      }

      for (const template of templates.discovery.slice(0, 5)) {
        tests.push({
          tier: "core",
          kind: "feature_discovery",
          q: fill(template, { model, feature: phrase }),
          model,
          feature,
          expectedIntents: expectedFor.discovery,
          expectedCanvas: "feature_match_builder_canvas",
        });
      }

      for (const template of templates.variantAvailability.slice(0, 2)) {
        tests.push({
          tier: "core",
          kind: "variant_feature_answer",
          q: fill(template, { model, variant, feature: phrase }),
          model,
          variant,
          feature,
          expectedIntents: expectedFor.variantFeatureAnswer,
        });
      }

      if (["sunroof", "ADAS", "360 camera", "ventilated seats", "6 airbags"].includes(feature)) {
        for (const template of templates.negativeDiscovery.slice(0, 2)) {
          tests.push({
            tier: "important",
            kind: "negative_feature_discovery",
            q: fill(template, { model, feature: phrase }),
            model,
            feature,
            expectedIntents: expectedFor.discovery,
          });
        }

        for (const template of templates.valueCheck.slice(0, 2)) {
          tests.push({
            tier: "important",
            kind: "feature_value",
            q: fill(template, { model, feature: phrase }),
            model,
            feature,
            expectedIntents: expectedFor.modelFeatureAnswer,
          });
        }
      }

      const typoModels = TYPO_MODELS[model] || [model];
      for (const typoModel of typoModels.slice(0, 2)) {
        for (const template of templates.typoIntent.slice(5, 8)) {
          tests.push({
            tier: "core",
            kind: "typo_feature",
            q: fill(template, { typoModel, typoFeature }),
            model,
            feature,
            expectedIntents: template.includes("which")
              ? expectedFor.discovery
              : expectedFor.modelFeatureAnswer,
          });
        }
      }
    }

    for (const template of templates.explorer) {
      tests.push({
        tier: "core",
        kind: "feature_explorer",
        q: fill(template, { model, variant }),
        model,
        variant,
        expectedIntents: expectedFor.explorer,
        expectedCanvas: "features_explorer_canvas",
        minFeatureGroups: 1,
        minVariantOptions: 1,
      });
    }

    for (const template of templates.category) {
      tests.push({
        tier: "core",
        kind: "category_features",
        q: fill(template, { model }),
        model,
        expectedIntents: expectedFor.category,
      });
    }

    for (const template of templates.buyingAdvisor) {
      tests.push({
        tier: "important",
        kind: "buying_feature_advisor",
        q: fill(template, { model }),
        model,
        expectedIntents: expectedFor.recommendation,
      });
    }

    for (const template of templates.comparison) {
      tests.push({
        tier: "important",
        kind: "feature_comparison",
        q: fill(template, { model, variantA, variantB }),
        model,
        variantA,
        variantB,
        expectedIntents: expectedFor.comparison,
      });
    }

    const typoModels = TYPO_MODELS[model] || [model];
    for (const typoModel of typoModels.slice(0, 4)) {
      tests.push({
        tier: "core",
        kind: "typo_pricelist_control",
        q: `${typoModel} pricelist`,
        model,
        expectedIntents: ["vehicle_pricelist"],
        expectedCanvas: "pricelist_canvas",
      });

      tests.push({
        tier: "core",
        kind: "typo_feature_explorer",
        q: `${typoModel} featuers`,
        model,
        expectedIntents: expectedFor.explorer,
        expectedCanvas: "features_explorer_canvas",
      });
    }
  }

  for (let i = 0; i < IMPORTANT_MODELS.length - 1; i += 1) {
    const model = IMPORTANT_MODELS[i].model;
    const modelB = IMPORTANT_MODELS[i + 1].model;

    for (const template of templates.crossModel) {
      tests.push({
        tier: "advanced",
        kind: "cross_model_feature",
        q: fill(template, { model, modelB }),
        model,
        modelB,
        expectedIntents: expectedFor.comparison,
      });
    }
  }

  for (const featureEntry of IMPORTANT_FEATURES.slice(0, 8)) {
    for (const template of templates.budgetFeature) {
      tests.push({
        tier: "advanced",
        kind: "feature_budget_discovery",
        q: fill(template, { feature: featureEntry.phrases[0] }),
        feature: featureEntry.canonical,
        expectedIntents: expectedFor.recommendation,
      });
    }
  }

  for (const featureEntry of IMPORTANT_FEATURES.slice(0, 5)) {
    tests.push({
      tier: "core",
      kind: "context_followup",
      q: fill(templates.context[0], { feature: featureEntry.phrases[0] }),
      model: "Creta",
      feature: featureEntry.canonical,
      expectedIntents: expectedFor.modelFeatureAnswer,
      context: {
        selectedVehicle: { make: "Hyundai", brand: "Hyundai", model: "Creta" },
        anchorMake: "Hyundai",
        anchorModel: "Creta",
        anchorFeature: featureEntry.canonical,
        anchorCity: "new-delhi",
      },
    });

    tests.push({
      tier: "core",
      kind: "context_followup",
      q: "which variants have it?",
      model: "Creta",
      feature: featureEntry.canonical,
      expectedIntents: expectedFor.discovery,
      expectedCanvas: "feature_match_builder_canvas",
      context: {
        selectedVehicle: { make: "Hyundai", brand: "Hyundai", model: "Creta" },
        anchorMake: "Hyundai",
        anchorModel: "Creta",
        anchorFeature: featureEntry.canonical,
        anchorCity: "new-delhi",
      },
    });
  }

  return uniqueByQuery(tests).slice(0, MAX_TESTS);
};

const classify = (test, response) => {
  const widget = getWidget(response);
  const rows = getRows(response, widget);
  const featureGroups = getFeatureGroups(response, widget);
  const variantOptions = getVariantOptions(response, widget);

  const actualIntent = response.intent || widget.intent || "";
  const actualCanvas = response.canvasType || widget.canvasType || "";
  const actualInline = response.inlineType || widget.inlineType || "";
  const answer = getAnswer(response, widget);

  const failures = [];
  const warnings = [];

  if (test.expectedIntents?.length && !test.expectedIntents.includes(actualIntent)) {
    failures.push(`Intent mismatch: expected ${test.expectedIntents.join(" / ")}, got ${actualIntent || "blank"}`);
  }

  if (test.expectedCanvas && actualCanvas !== test.expectedCanvas) {
    failures.push(`Canvas mismatch: expected ${test.expectedCanvas}, got ${actualCanvas || "blank"}`);
  }

  if (test.expectedInline && actualInline !== test.expectedInline) {
    failures.push(`Inline mismatch: expected ${test.expectedInline}, got ${actualInline || "blank"}`);
  }

  if (test.minRows && rows.length < test.minRows) {
    failures.push(`Rows too low: expected >= ${test.minRows}, got ${rows.length}`);
  }

  if (test.minFeatureGroups && featureGroups.length < test.minFeatureGroups) {
    failures.push(`Feature groups too low: expected >= ${test.minFeatureGroups}, got ${featureGroups.length}`);
  }

  if (test.minVariantOptions && variantOptions.length < test.minVariantOptions) {
    failures.push(`Variant options too low: expected >= ${test.minVariantOptions}, got ${variantOptions.length}`);
  }

  if (!answer) warnings.push("No answer text.");
  if (includesAny(answer, GENERIC_BAD_ANSWER_PHRASES)) {
    warnings.push("Answer sounds internal/generic/repetitive.");
  }

  if (["model_feature_answer", "variant_feature_answer", "typo_feature"].includes(test.kind) && answer.length > 180) {
    warnings.push("Inline answer too long.");
  }

  if (
    actualIntent === "vehicle_feature_discovery" &&
    actualCanvas !== "feature_match_builder_canvas"
  ) {
    warnings.push("Feature discovery did not return feature_match_builder_canvas.");
  }

  if (
    actualIntent === "vehicle_model_features_explorer" &&
    actualCanvas !== "features_explorer_canvas"
  ) {
    warnings.push("Feature explorer did not return features_explorer_canvas.");
  }

  if (
    actualIntent?.includes("feature") &&
    rows.length === 0 &&
    featureGroups.length === 0
  ) {
    if (!isAllowedEmptyFeatureResult(test, answer)) {
      warnings.push("Feature intent returned no rows and no feature groups.");
    }
  }

  const sampleRows = rows.slice(0, 5).map((row) => ({
    variant: row.variant || row.variantName || row.label || row.name || "",
    feature: row.feature || row.matchedFeature || row.name || "",
    section: row.section || row.category || "",
    value: row.value || row.displayValue || "",
    available: row.available,
    price: row.exShowroomPrice || row.price || row.onRoadPrice || "",
  }));

  return {
    id: test.id,
    tier: test.tier,
    kind: test.kind,
    query: test.q,
    model: test.model || "",
    feature: test.feature || "",
    expectedIntents: test.expectedIntents,
    expectedCanvas: test.expectedCanvas || "",
    actualIntent,
    actualCanvas,
    actualInline,
    title: response.title || widget.title || "",
    answer,
    rowsCount: rows.length,
    featureGroupsCount: featureGroups.length,
    variantOptionsCount: variantOptions.length,
    activeStatusSource: widget.activeStatusSource || response.data?.activeStatusSource || "",
    currentPricelistMatched:
      widget.currentPricelistMatched ??
      response.data?.currentPricelistMatched ??
      null,
    modulesChecked: response.sourceTransparency?.modulesChecked || [],
    sampleRows,
    failures,
    warnings,
    pass: failures.length === 0,
  };
};

const runOne = async (test, index) => {
  const context = {
    city: "new-delhi",
    anchorCity: "new-delhi",
    ...(test.context || {}),
  };

  const response = await chatWithAgent({
    message: test.q,
    sessionId: `feature-mega-${index}`,
    context,
    debug: true,
    user: null,
  });

  return classify({ ...test, id: index + 1 }, response);
};

const makeMarkdown = (summary, results) => {
  const failures = results.filter((item) => !item.pass);
  const warnings = results.filter((item) => item.warnings?.length);

  return [
    "# ACI Assist V2 — Mega Feature Single-Intent Regression",
    "",
    `Total: **${summary.total}**`,
    `Passed: **${summary.passed}**`,
    `Failed: **${summary.failed}**`,
    `Warnings: **${summary.warningCases}**`,
    "",
    "## Summary by tier",
    "",
    "| Tier | Total | Passed | Failed | Warning cases |",
    "|---|---:|---:|---:|---:|",
    ...Object.entries(summary.byTier).map(
      ([tier, item]) =>
        `| ${tier} | ${item.total} | ${item.passed} | ${item.failed} | ${item.warningCases} |`,
    ),
    "",
    "## Summary by kind",
    "",
    "| Kind | Total | Passed | Failed | Warning cases |",
    "|---|---:|---:|---:|---:|",
    ...Object.entries(summary.byKind).map(
      ([kind, item]) =>
        `| ${kind} | ${item.total} | ${item.passed} | ${item.failed} | ${item.warningCases} |`,
    ),
    "",
    "## Failures",
    "",
    ...(failures.length
      ? failures.flatMap((item) => [
          `### ${item.id}. ${item.query}`,
          "",
          `- Tier: ${item.tier}`,
          `- Kind: ${item.kind}`,
          `- Expected: ${item.expectedIntents?.join(" / ") || ""} ${item.expectedCanvas ? `· ${item.expectedCanvas}` : ""}`,
          `- Actual: ${item.actualIntent || "blank"} / ${item.actualCanvas || item.actualInline || "no-card"}`,
          `- Rows: ${item.rowsCount}`,
          `- Groups: ${item.featureGroupsCount}`,
          `- Variants: ${item.variantOptionsCount}`,
          `- Failures: ${item.failures.join("; ")}`,
          `- Answer: ${item.answer || ""}`,
          `- Sample rows: \`${JSON.stringify(item.sampleRows).slice(0, 900)}\``,
          "",
        ])
      : ["No failures ✅", ""]),
    "",
    "## Copy / quality warnings",
    "",
    ...(warnings.length
      ? warnings.flatMap((item) => [
          `### ${item.id}. ${item.query}`,
          "",
          `- Actual: ${item.actualIntent || "blank"} / ${item.actualCanvas || item.actualInline || "no-card"}`,
          `- Warning: ${item.warnings.join("; ")}`,
          `- Answer: ${item.answer}`,
          "",
        ])
      : ["No warnings ✅", ""]),
    "",
    "## Full table",
    "",
    "| # | Tier | Kind | Query | Intent | Canvas/Inline | Rows | Groups | Variants | Pass | Warnings |",
    "|---:|---|---|---|---|---|---:|---:|---:|---|---:|",
    ...results.map(
      (item) =>
        `| ${item.id} | ${item.tier} | ${item.kind} | ${item.query.replaceAll("|", "\\|")} | ${item.actualIntent || ""} | ${item.actualCanvas || item.actualInline || ""} | ${item.rowsCount} | ${item.featureGroupsCount} | ${item.variantOptionsCount} | ${item.pass ? "✅" : "❌"} | ${item.warnings.length} |`,
    ),
    "",
  ].join("\n");
};

const main = async () => {
  if (mongoUri && mongoose.connection.readyState !== 1) {
    await mongoose.connect(mongoUri);
    console.log("MongoDB connected");
  }

  const tests = await buildTests();
  console.log(`Running ${tests.length} mega single-intent feature questions...`);

  const workerCount = tests.length
    ? Math.max(1, Math.min(WORKERS, tests.length))
    : 1;

  console.log(`Workers: ${workerCount}`);

  const results = Array(tests.length);
  let nextIndex = 0;

  const runIndexedTest = async (i, workerId) => {
    const test = tests[i];

    try {
      const result = await runOne(test, i);
      results[i] = result;

      const icon = result.pass ? "✅" : "❌";
      const warn = result.warnings.length ? " ⚠️" : "";
      console.log(
        `${icon}${warn} ${String(i + 1).padStart(3, "0")}/${tests.length} [w${workerId}] [${result.tier}] ${result.query} -> ${result.actualIntent || "blank"} / ${result.actualCanvas || result.actualInline || "no-card"} rows=${result.rowsCount} groups=${result.featureGroupsCount}`,
      );

      result.failures.forEach((failure) => console.log(`   - ${failure}`));
      result.warnings.forEach((warning) => console.log(`   ⚠ ${warning}`));
    } catch (error) {
      const failed = {
        id: i + 1,
        tier: test.tier,
        kind: test.kind,
        query: test.q,
        pass: false,
        actualIntent: "",
        actualCanvas: "",
        actualInline: "",
        rowsCount: 0,
        featureGroupsCount: 0,
        variantOptionsCount: 0,
        failures: [error?.stack || error?.message || String(error)],
        warnings: [],
        sampleRows: [],
      };

      results[i] = failed;
      console.log(`💥 ${String(i + 1).padStart(3, "0")}/${tests.length} [w${workerId}] ${test.q}`);
      console.log(failed.failures[0]);
    }
  };

  const runWorker = async (workerId) => {
    while (nextIndex < tests.length) {
      const i = nextIndex;
      nextIndex += 1;
      await runIndexedTest(i, workerId);
    }
  };

  await Promise.all(
    Array.from({ length: workerCount }, (_, index) => runWorker(index + 1)),
  );

  const summary = {
    total: results.length,
    passed: results.filter((item) => item.pass).length,
    failed: results.filter((item) => !item.pass).length,
    warningCases: results.filter((item) => item.warnings?.length).length,
    byTier: {},
    byKind: {},
  };

  for (const result of results) {
    summary.byTier[result.tier] ||= { total: 0, passed: 0, failed: 0, warningCases: 0 };
    summary.byTier[result.tier].total += 1;
    summary.byTier[result.tier].passed += result.pass ? 1 : 0;
    summary.byTier[result.tier].failed += result.pass ? 0 : 1;
    summary.byTier[result.tier].warningCases += result.warnings?.length ? 1 : 0;

    summary.byKind[result.kind] ||= { total: 0, passed: 0, failed: 0, warningCases: 0 };
    summary.byKind[result.kind].total += 1;
    summary.byKind[result.kind].passed += result.pass ? 1 : 0;
    summary.byKind[result.kind].failed += result.pass ? 0 : 1;
    summary.byKind[result.kind].warningCases += result.warnings?.length ? 1 : 0;
  }

  fs.writeFileSync(OUT_JSON, JSON.stringify({ summary, results }, null, 2));
  fs.writeFileSync(OUT_MD, makeMarkdown(summary, results));

  console.log("\n================ SUMMARY ================");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`\nJSON: ${OUT_JSON}`);
  console.log(`Markdown: ${OUT_MD}`);

  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
  process.exit(1);
});
