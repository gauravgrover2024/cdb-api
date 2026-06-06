#!/usr/bin/env node
require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

const PUBLIC_ENDPOINT =
  process.env.ACI_DEEP_AUDIT_PUBLIC_ENDPOINT ||
  "http://localhost:5050/api/ai-agent/public-chat";

const SUPPORTED_PRICE_CITIES = new Set(["new-delhi", "noida", "gurgaon"]);
const UNSUPPORTED_PRICE_CITIES = new Set(["mumbai", "bangalore", "bengaluru", "jaipur"]);
const JSON_START = "__ACI_DEEP_AUDIT_JSON_START__";
const JSON_END = "__ACI_DEEP_AUDIT_JSON_END__";

const GROUP_DEFS = [
  { key: "A", start: 1, end: 32, label: "A - Price / pricelist / on-road / city support" },
  { key: "B", start: 33, end: 53, label: "B - Feature availability" },
  { key: "C", start: 54, end: 64, label: "C - Colors" },
  { key: "D", start: 65, end: 82, label: "D - Specs / attributes" },
  { key: "E", start: 83, end: 100, label: "E - Comparison" },
  { key: "F", start: 101, end: 113, label: "F - Context switching / stale context isolation" },
  { key: "G", start: 114, end: 130, label: "G - Score insight single-car / value" },
  { key: "H", start: 131, end: 140, label: "H - Cross-model score diagnostic" },
  { key: "I", start: 141, end: 160, label: "I - Clarification and no-data honesty" },
  { key: "J", start: 161, end: 170, label: "J - Hindi / Hinglish buyer queries" },
  { key: "K", start: 171, end: 185, label: "K - Adversarial / typo / alias / ambiguity" },
];

const rx = (value) => new RegExp(String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
const asArray = (value) => (Array.isArray(value) ? value : []);
const text = (value) => String(value || "");
const lower = (value) => text(value).toLowerCase();
const slug = (value = "") =>
  text(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
const preview = (value, len = 360) => text(value).replace(/\s+/g, " ").trim().slice(0, len);

const selectedVehicle = ({ make = "", model = "", variant = "", city = "new-delhi", citySlug = city } = {}) => ({
  make,
  model,
  fullModel: [make, model].filter(Boolean).join(" "),
  variant,
  variantName: variant,
  fullVariant: [make, model, variant].filter(Boolean).join(" "),
  city,
  citySlug,
});

const activeComparison = (...vehicles) => ({
  vehicles: vehicles.map((vehicle) => ({
    make: vehicle.make,
    model: vehicle.model,
    fullModel: vehicle.fullModel || [vehicle.make, vehicle.model].filter(Boolean).join(" "),
    variant: vehicle.variant || "",
  })),
});

const V = {
  creta: selectedVehicle({ make: "Hyundai", model: "Creta" }),
  cretaDelhi: selectedVehicle({ make: "Hyundai", model: "Creta", city: "new-delhi" }),
  cretaNoida: selectedVehicle({ make: "Hyundai", model: "Creta", city: "noida" }),
  cretaSx: selectedVehicle({ make: "Hyundai", model: "Creta", variant: "SX" }),
  cretaSxo: selectedVehicle({ make: "Hyundai", model: "Creta", variant: "SX(O)" }),
  seltos: selectedVehicle({ make: "Kia", model: "Seltos" }),
  seltosHtx: selectedVehicle({ make: "Kia", model: "Seltos", variant: "HTX" }),
  venue: selectedVehicle({ make: "Hyundai", model: "Venue" }),
  verna: selectedVehicle({ make: "Hyundai", model: "Verna" }),
  baleno: selectedVehicle({ make: "Maruti", model: "Baleno" }),
  balenoAlpha: selectedVehicle({ make: "Maruti", model: "Baleno", variant: "Alpha" }),
  balenoSigma: selectedVehicle({ make: "Maruti", model: "Baleno", variant: "Sigma" }),
  i20Sportz: selectedVehicle({ make: "Hyundai", model: "I20", variant: "Sportz" }),
  scorpioN: selectedVehicle({ make: "Mahindra", model: "Scorpio N" }),
  scorpio: selectedVehicle({ make: "Mahindra", model: "Scorpio" }),
  eqs: selectedVehicle({ make: "Mercedes Benz", model: "Eqs" }),
  be6: selectedVehicle({ make: "Mahindra", model: "Be 6" }),
};

const C = {
  cretaSeltos: { activeComparison: activeComparison(V.creta, V.seltos) },
  scorpioNScorpio: { activeComparison: activeComparison(V.scorpioN, V.scorpio) },
};

const getTool = (body = {}) =>
  body.aciCoreBridge?.tool ||
  body.meta?.aciCoreBridge?.tool ||
  body.tool ||
  body.responseTool ||
  body.data?.tool ||
  "";

const getIntent = (body = {}) =>
  body.intent ||
  body.meta?.intent ||
  body.data?.intent ||
  "";

const getOperation = (body = {}) =>
  body.operation ||
  body.diagnosticType ||
  body.data?.operation ||
  body.data?.diagnosticType ||
  body.meta?.scoreInsightOperation ||
  body.meta?.aciCoreBridge?.operation ||
  body.aciCoreBridge?.operation ||
  "";

const getCanvasType = (body = {}) =>
  body.canvasType ||
  body.data?.canvasType ||
  body.widget?.canvasType ||
  body.output?.canvasType ||
  "";

const getRows = (body = {}) => {
  const direct =
    body.rows ||
    body.data?.rows ||
    body.modelSummaries ||
    body.data?.modelSummaries ||
    body.scoreComparison?.modelSummaries ||
    body.data?.scoreComparison?.modelSummaries ||
    body.variants ||
    body.data?.variants ||
    body.items ||
    body.data?.items ||
    body.records ||
    body.widget?.rows ||
    body.widget?.items ||
    [];
  return asArray(direct);
};

const countUniqueModels = (body = {}) => {
  if (Number.isFinite(Number(body.modelCount))) return Number(body.modelCount);
  if (Number.isFinite(Number(body.data?.modelCount))) return Number(body.data.modelCount);
  const rows = getRows(body);
  const names = new Set();
  rows.forEach((row = {}) => {
    const label = row.fullModel || row.modelName || row.model || row.displayName || row.label || row.modelKey || "";
    if (label) names.add(lower(label));
  });
  asArray(
    body.models ||
      body.modelSummaries ||
      body.data?.models ||
      body.data?.modelSummaries ||
      body.comparison?.models ||
      body.scoreComparison?.models ||
      body.scoreComparison?.modelSummaries ||
      body.data?.scoreComparison?.models ||
      body.data?.scoreComparison?.modelSummaries,
  ).forEach((model = {}) => {
    const label = model.fullModel || model.model || model.name || model.label || model.modelKey || "";
    if (label) names.add(lower(label));
  });
  return names.size;
};

const countModuleComparisons = (body = {}) =>
  Number.isFinite(Number(body.moduleComparisonCount))
    ? Number(body.moduleComparisonCount)
    : Number.isFinite(Number(body.data?.moduleComparisonCount))
      ? Number(body.data.moduleComparisonCount)
      :
  asArray(
    body.moduleComparisons ||
      body.data?.moduleComparisons ||
      body.scoreComparison?.moduleComparisons ||
      body.data?.scoreComparison?.moduleComparisons,
  ).length;

const summarizeResponse = (body = {}) => ({
  intent: getIntent(body),
  tool: getTool(body),
  operation: getOperation(body),
  canvasType: getCanvasType(body),
  title: body.title || body.data?.title || body.widget?.title || "",
  answerPreview: preview(body.answer || body.text || body.message || ""),
  rowsCount: getRows(body).length,
  modelCount: countUniqueModels(body),
  moduleComparisonCount: countModuleComparisons(body),
  contextIsolation:
    body.aciCoreBridge?.contextIsolation ||
    body.meta?.aciCoreBridge?.contextIsolation ||
    body.contextIsolation ||
    "",
});

const hasAny = (haystack, needles = []) => needles.some((needle) => new RegExp(needle, "i").test(haystack));
const hasAll = (haystack, needles = []) => needles.every((needle) => new RegExp(needle, "i").test(haystack));

const POSITIVE_RECOMMENDATION_PHRASES = [
  "must buy",
  "buy this",
  "clear winner",
  "recommended buy",
  "definitely buy",
];

const hasUnsafePositiveRecommendation = (value = "") => {
  const source = text(value);
  const normalized = lower(source);
  return POSITIVE_RECOMMENDATION_PHRASES.some((phrase) => {
    let index = normalized.indexOf(phrase);
    while (index >= 0) {
      const prefix = normalized.slice(Math.max(0, index - 32), index);
      if (!/\b(not|no|never|avoid|without|isn'?t|is not|not a)\b[\w\s,-]*$/i.test(prefix)) {
        return true;
      }
      index = normalized.indexOf(phrase, index + phrase.length);
    }
    return false;
  });
};

const genericScoreSubject =
  /\bScore insight\b|I found score insight data for/i;
const noConfirmedPriceRows = /could not find confirmed price rows|no confirmed price rows/i;

const priceLabelsFromRow = (row = {}) =>
  [
    row.exShowroomPriceLabel,
    row.onRoadPriceWithoutOptionalLabel,
    row.onRoadPriceLabel,
    row.priceLabel,
  ]
    .map((value) => text(value).trim())
    .filter(Boolean);

const db = () => mongoose.connection.db;

const collectionCount = async (collection, query) => {
  if (!db()) return 0;
  try {
    return await db().collection(collection).countDocuments(query);
  } catch {
    return 0;
  }
};

const priceEvidence = async ({ model, citySlug = "new-delhi" }) => ({
  kind: "price",
  model,
  citySlug,
  priceRows: await collectionCount("aci_vehicle_price_rows", {
    model: rx(model),
    citySlug,
  }),
});

const nestedColorCount = async ({ model }) => {
  if (!db()) return 0;
  try {
    const [result = {}] = await db().collection("vehicle_colors_v2")
      .aggregate([
        { $match: { model: rx(model) } },
        {
          $project: {
            colorCount: {
              $size: {
                $ifNull: ["$colors", []],
              },
            },
          },
        },
        {
          $group: {
            _id: null,
            total: { $sum: "$colorCount" },
          },
        },
      ])
      .toArray();
    return Number(result.total || 0) || 0;
  } catch {
    return 0;
  }
};

const colorEvidence = async ({ model }) => {
  const rowCount = await collectionCount("aci_vehicle_color_rows", {
    model: rx(model),
  });
  const nestedCount = await nestedColorCount({ model });

  return {
    kind: "colors",
    model,
    colorRows: rowCount + nestedCount,
    colorRowDocs: rowCount,
    nestedColorRows: nestedCount,
  };
};

const specEvidence = async ({ model, attribute }) => ({
  kind: "spec",
  model,
  attribute,
  specRows: await collectionCount("aci_vehicle_spec_rows", {
    model: rx(model),
    $or: [
      { attributeKey: rx(attribute) },
      { attributeLabel: rx(attribute) },
      { key: rx(attribute) },
      { label: rx(attribute) },
    ],
  }),
});

const scoreEvidence = async ({ model, variant = "" }) => ({
  kind: "score",
  model,
  variant,
  scoreRows: await collectionCount("aci_vehicle_variant_score_profile", {
    model: rx(model),
    ...(variant ? { variant: rx(variant) } : {}),
  }),
});

const directPriceAssert = ({ model, citySlug = "new-delhi", unsupportedCity = "" } = {}) => async ({ body, summary, dbEvidence, testCase }) => {
  const answer = text(body.answer);
  const blob = JSON.stringify(body || {});
  const rows = getRows(body);
  const caseMessage = text(testCase?.message);
  const isOnRoadQuery = /\bon\s*road\b|\bonroad\b/i.test(caseMessage);
  const isExShowroomQuery = /\bex\s*showroom\b|\bexshowroom\b/i.test(caseMessage);
  assert.notStrictEqual(summary.tool, "vehicle_compare", "direct price must not route to vehicle_compare");
  assert.notStrictEqual(summary.canvasType, "comparison_canvas", "direct price must not return comparison_canvas");
  assert(!/\bcompared\b/i.test(answer), "direct price answer must not use comparison language");
  assert(!/\bprice rows\b/i.test(answer), "buyer answer must not expose internal phrase 'price rows'");
  assert(!/\b1\s+[\w\s]+variants\b/i.test(answer), "single resolved variant answer must not use plural variant-count wording");
  assert(!/\. the on-road\b/.test(answer), "buyer answer must capitalize sentence after period before on-road");
  if (unsupportedCity) {
    assert(
      !/New Delhi price|Delhi on-road|for New Delhi with \d+ variants|₹/i.test(blob),
      `unsupported ${unsupportedCity} query must not silently show Delhi/New Delhi price evidence`,
    );
    return;
  }
  if (dbEvidence?.priceRows > 0) {
    assert(summary.rowsCount > 0, `DB has ${dbEvidence.priceRows} ${model} price rows, but response returned no rows`);
    assert(/₹|rs\.?|ex-showroom|on-road/i.test(answer), "buyer answer missing price evidence");
    if (summary.rowsCount > 0) {
      assert(/₹|rs\.?|ex-showroom price|on-road price/i.test(answer), "returned rows require buyer-facing price labels or rupee values");
    }
    if (isOnRoadQuery) {
      assert(/on-road|on road/i.test(answer), "on-road query must mention on-road price/range");
    }
    if (isExShowroomQuery) {
      assert(/ex-showroom|ex showroom/i.test(answer), "ex-showroom query must mention ex-showroom price/range");
    }
    if (rows.length === 1) {
      const labels = priceLabelsFromRow(rows[0]);
      assert(/₹|rs\.?/i.test(answer), "single-row variant price answer must include an actual rupee value");
      if (labels.length) {
        assert(
          labels.some((label) => rx(label).test(answer)),
          `single-row variant answer must include an actual returned price value: ${labels.join(", ")}`,
        );
      }
    }
    assert(!noConfirmedPriceRows.test(answer), "DB rows exist, but answer says no confirmed price rows");
  }
  if (model) assert(new RegExp(model.replace(/\s+/g, "\\s+"), "i").test(blob), `response must mention ${model}`);
  if (citySlug && SUPPORTED_PRICE_CITIES.has(citySlug)) {
    assert(!/pricing unavailable/i.test(answer), `supported city ${citySlug} must not be treated as unsupported`);
  }
};

const scoreDiagnosticAssert = ({ models = [], variants = [], crossModel = false } = {}) => ({ body, summary }) => {
  const answer = text(body.answer);
  const blob = JSON.stringify(body || {});
  assert(!genericScoreSubject.test(answer), "buyer answer leaked generic score-insight wording");
  assert(/diagnostic/i.test(answer), "score answer must be diagnostic-only");
  assert(!hasUnsafePositiveRecommendation(blob), "score answer leaked final recommendation wording");
  models.forEach((model) => assert(rx(model).test(blob), `score response must mention ${model}`));
  variants.forEach((variant) => assert(rx(variant).test(blob), `score response must mention ${variant}`));
  if (crossModel) {
    assert.strictEqual(summary.operation, "cross_model_score_diagnostic", `expected cross_model_score_diagnostic, got ${summary.operation}`);
    assert(summary.modelCount >= 2, "cross-model score response must include at least two models");
    assert(summary.moduleComparisonCount >= 5, "cross-model score response must include module comparison evidence");
  }
};

const c = (id, group, message, options = {}) => ({ id, group, message, ...options });

const priceCase = (n, message, model, citySlug = "new-delhi", options = {}) =>
  c(`A${n}-${slug(message)}`, "A - Price / pricelist / on-road / city support", message, {
    expectedTool: "vehicle_pricelist",
    expectedIntent: "vehicle_pricelist",
    forbiddenTools: ["vehicle_compare"],
    forbiddenCanvasTypes: ["comparison_canvas"],
    expectedModels: model ? [model] : [],
    dbEvidenceCheck: model ? () => priceEvidence({ model, citySlug }) : null,
    customAssert: directPriceAssert({ model, citySlug, unsupportedCity: UNSUPPORTED_PRICE_CITIES.has(citySlug) ? citySlug : "" }),
    ...options,
  });

const featureCase = (n, message, model, topic = "", options = {}) =>
  c(`B${n}-${slug(message)}`, "B - Feature availability", message, {
    expectedTool: "vehicle_feature_lookup",
    forbiddenTools: ["vehicle_compare"],
    expectedModels: model ? [model] : [],
    requiredAnswerAny: [topic || model || "available", "yes", "no", "not available", "feature"],
    forbiddenAnswerAny: ["^I understood\\b"],
    customAssert: ({ body }) => {
      const blob = JSON.stringify(body || {});
      const answer = text(body.answer || body.message || "");
      const title = text(body.title || body.data?.title || body.widget?.title || "");
      if (model) assert(rx(model).test(blob), `feature answer/data must mention ${model}`);
      if (topic) assert(rx(topic).test(blob), `feature answer/data must mention ${topic}`);
      assert(!/match\s+[“"]{1,2}\s*[”"]{1,2}\s+to a feature/i.test(answer), "feature-list query must not use blank feature-topic failure copy");
      assert(!/could not safely match\s+[“"]{1,2}\s*[”"]{1,2}/i.test(blob), "feature response must not contain blank feature-topic match text");
      if (/\bfeatures?\b/i.test(message) && !/\b(sunroof|airbags?|adas|camera|ventilated|rear\s+ac)\b/i.test(message)) {
        assert(!/could not safely match/i.test(answer), "feature-list query must return a summary or honest limitation, not feature-match failure copy");
        assert(/feature/i.test(answer), "feature-list query must mention feature-list context");
      }
      if (n === 48) {
        assert(!/seating capacity/i.test(`${title} ${answer}`), "ventilated-seat query must not answer seating capacity");
        assert(/ventilated/i.test(`${title} ${answer} ${blob}`), "ventilated-seat query must preserve ventilated-seat topic");
      }
    },
    ...options,
  });

const colorCase = (n, message, model, options = {}) =>
  c(`C${n}-${slug(message)}`, "C - Colors", message, {
    expectedTool: "vehicle_colors",
    forbiddenTools: ["vehicle_compare"],
    expectedModels: model ? [model] : [],
    dbEvidenceCheck: model ? () => colorEvidence({ model }) : null,
    customAssert: ({ body, summary, dbEvidence }) => {
      const blob = JSON.stringify(body || {});
      if (model) assert(rx(model).test(blob), `color answer/data must mention ${model}`);
      if (dbEvidence?.colorRows > 0) {
        assert(summary.rowsCount > 0 || /black|white|red|blue|grey|gray|silver|color|colour/i.test(blob), "color evidence missing");
      }
    },
    ...options,
  });

const specCase = (n, message, model, attribute, options = {}) =>
  c(`D${n}-${slug(message)}`, "D - Specs / attributes", message, {
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedModels: model ? [model] : [],
    requiredAnswerAny: [attribute, "km", "mileage", "boot", "ground", "power", "cc"],
    dbEvidenceCheck: model && attribute ? () => specEvidence({ model, attribute }) : null,
    customAssert: ({ body }) => {
      const blob = JSON.stringify(body || {});
      const answer = text(body.answer || body.message || "");
      const title = text(body.title || body.data?.title || body.widget?.title || "");
      if (model) assert(rx(model).test(blob), `spec answer/data must mention ${model}`);
      if (attribute) assert(rx(attribute).test(blob), `spec answer/data must mention ${attribute}`);
      assert(!/What would you like to check about the car\?/i.test(answer), "clear model+attribute spec query must not return generic clarification");
      if (n === 79) {
        assert(/Honda\s+City|City/i.test(`${title} ${answer} ${blob}`), "city power must resolve to Honda City model");
        assert(!/\b(citySlug|new-delhi|noida|gurgaon|location)\b/i.test(`${title} ${answer}`), "city power must not be treated as a city/location query");
      }
      if (/eqs/i.test(model || "")) {
        assert(/813\s*km|857\s*km/i.test(blob), "EQS range must include 813 km or 857 km when available");
        assert(!/not available|unavailable|don't have the exact/i.test(text(body.answer)), "EQS range must not be marked unavailable");
      }
    },
    ...options,
  });

const compareCase = (n, message, models, options = {}) =>
  c(`E${n}-${slug(message)}`, "E - Comparison", message, {
    expectedTool: "vehicle_compare",
    expectedCanvasType: "comparison_canvas",
    expectedModels: models,
    expectedModelCountMin: 2,
    customAssert: ({ body }) => {
      const blob = JSON.stringify(body || {});
      models.forEach((model) => assert(rx(model).test(blob), `comparison response must mention ${model}`));
      const title = text(body.title || body.data?.title || body.widget?.title || "");
      if (new Set(models.map((model) => lower(model))).size >= 2) {
        assert(!/\b([A-Za-z0-9 ]+)\s+vs\s+\1\b/i.test(title), "comparison must not compare the same car with itself");
      }
      assert(!/Creta vs Seltos vs Hyundai Creta vs Kia Seltos/i.test(blob), "comparison title duplicated models");
    },
    ...options,
  });

const scoreCase = (n, message, models, options = {}) =>
  c(`G${n}-${slug(message)}`, "G - Score insight single-car / value", message, {
    expectedTool: "vehicle_score_insight",
    expectedIntent: "vehicle_score_insight",
    forbiddenAnswerAny: ["Score insight", "I found score insight data for"],
    expectedModels: models,
    dbEvidenceCheck: models?.[0] ? () => scoreEvidence({ model: models[0], variant: options.expectedVariants?.[0] || "" }) : null,
    customAssert: scoreDiagnosticAssert({ models, variants: options.expectedVariants || [] }),
    ...options,
  });

const crossScoreCase = (n, message, models, options = {}) =>
  c(`H${n}-${slug(message)}`, "H - Cross-model score diagnostic", message, {
    expectedTool: "vehicle_score_insight",
    expectedIntent: "vehicle_score_insight",
    expectedOperation: "cross_model_score_diagnostic",
    expectedModelCountMin: 2,
    expectedModuleComparisonMin: 5,
    expectedModels: models,
    forbiddenAnswerAny: ["Score insight", "I found score insight data for"],
    customAssert: scoreDiagnosticAssert({ models, crossModel: true }),
    ...options,
  });

const honestyCase = (n, message, options = {}) =>
  c(`I${n}-${slug(message)}`, "I - Clarification and no-data honesty", message, {
    forbiddenAnswerAny: [
      "must buy",
      "buy this",
      "clear winner",
      "guaranteed",
      "confirmed offer",
      "confirmed discount",
    ],
    customAssert: ({ body }) => {
      const answer = text(body.answer || body.message);
      assert(answer.trim(), "honesty case must return a buyer-facing answer");
      assert(!hasUnsafePositiveRecommendation(answer), "honesty answer must avoid false certainty");
    },
    ...options,
  });

const caseNumberFromId = (id = "") => {
  const match = text(id).match(/^[A-Z](\d+)/i) || text(id).match(/(\d+)/);
  return match ? Number.parseInt(match[1], 10) : 0;
};

const groupDefForCaseNumber = (caseNumber = 0) =>
  GROUP_DEFS.find((def) => caseNumber >= def.start && caseNumber <= def.end) || null;

const normalizeCaseGroup = (testCase = {}) => {
  const caseNumber = caseNumberFromId(testCase.id);
  const def = groupDefForCaseNumber(caseNumber);
  if (!def) {
    return {
      ...testCase,
      groupKey: text(testCase.group || testCase.id).trim().slice(0, 1).toUpperCase(),
    };
  }

  return {
    ...testCase,
    id: text(testCase.id).replace(/^[A-Z]/i, def.key),
    group: def.label,
    groupKey: def.key,
  };
};

const rawCases = [
  priceCase(1, "creta price delhi", "Creta"),
  priceCase(2, "creta on road price delhi", "Creta"),
  priceCase(3, "creta ex showroom price delhi", "Creta"),
  priceCase(4, "creta sx on road price delhi", "Creta", "new-delhi", { expectedVariants: ["SX"] }),
  priceCase(5, "creta sx price", "Creta", "new-delhi", { expectedVariants: ["SX"] }),
  priceCase(6, "seltos price delhi", "Seltos"),
  priceCase(7, "seltos htx price delhi", "Seltos", "new-delhi", { expectedVariants: ["HTX"] }),
  priceCase(8, "venue price noida", "Venue", "noida"),
  priceCase(9, "venue automatic price noida", "Venue", "noida"),
  priceCase(10, "scorpio n price", "Scorpio N"),
  priceCase(11, "scorpio n price delhi", "Scorpio N"),
  priceCase(12, "scorpio n on road price delhi", "Scorpio N"),
  priceCase(13, "mahindra scorpio n price", "Scorpio N"),
  priceCase(14, "thar price delhi", "Thar"),
  priceCase(15, "thar roxx price delhi", "Thar Roxx"),
  priceCase(16, "i20 sportz price delhi", "I20", "new-delhi", { expectedVariants: ["Sportz"] }),
  priceCase(17, "baleno alpha price delhi", "Baleno", "new-delhi", { expectedVariants: ["Alpha"] }),
  priceCase(18, "fronx price delhi", "Fronx"),
  priceCase(19, "brezza price gurgaon", "Brezza", "gurgaon"),
  priceCase(20, "grand vitara price noida", "Grand Vitara", "noida"),
  priceCase(21, "creta price mumbai", "Creta", "mumbai", { expectedCanvasType: "unsupported_city_canvas" }),
  priceCase(22, "seltos price bangalore", "Seltos", "bangalore", { expectedCanvasType: "unsupported_city_canvas" }),
  priceCase(23, "scorpio n price mumbai", "Scorpio N", "mumbai", { expectedCanvasType: "unsupported_city_canvas" }),
  priceCase(24, "baleno price jaipur", "Baleno", "jaipur", { expectedCanvasType: "unsupported_city_canvas" }),
  priceCase(25, "same price in Mumbai", "Creta", "mumbai", { context: { selectedVehicle: V.cretaDelhi }, expectedCanvasType: "unsupported_city_canvas" }),
  priceCase(26, "change city to Noida", "Creta", "noida", { context: { selectedVehicle: V.cretaDelhi } }),
  priceCase(27, "now Gurgaon", "Seltos", "gurgaon", { context: { selectedVehicle: selectedVehicle({ make: "Kia", model: "Seltos", city: "noida" }) } }),
  priceCase(28, "price of this", "Creta", "new-delhi", { context: { selectedVehicle: V.cretaDelhi } }),
  priceCase(29, "price of this", "Scorpio N", "new-delhi", { context: { selectedVehicle: V.scorpioN } }),
  compareCase(30, "price difference", ["Creta", "Seltos"], { context: C.cretaSeltos }),
  priceCase(31, "now Creta price", "Creta", "new-delhi", { context: C.cretaSeltos }),
  priceCase(32, "scorpio n price", "Scorpio N", "new-delhi", { context: C.scorpioNScorpio }),

  featureCase(33, "creta sunroof", "Creta", "sunroof"),
  featureCase(34, "creta panoramic sunroof", "Creta", "panoramic"),
  featureCase(35, "seltos sunroof", "Seltos", "sunroof"),
  featureCase(36, "be 6e sunroof", "Be 6", "sunroof"),
  featureCase(37, "xuv700 airbags", "XUV700", "airbags"),
  featureCase(38, "seltos airbags", "Seltos", "airbags"),
  featureCase(39, "creta airbags", "Creta", "airbags"),
  featureCase(40, "venue rear camera", "Venue", "camera"),
  featureCase(41, "i20 sportz features", "I20", "features", { expectedVariants: ["Sportz"] }),
  featureCase(42, "baleno alpha features", "Baleno", "features", { expectedVariants: ["Alpha"] }),
  featureCase(43, "verna adas", "Verna", "ADAS"),
  featureCase(44, "city zx features", "City", "features", { expectedVariants: ["ZX"] }),
  featureCase(45, "elevate airbags", "Elevate", "airbags"),
  featureCase(46, "brezza sunroof", "Brezza", "sunroof"),
  featureCase(47, "fronx rear ac vents", "Fronx", "rear ac"),
  featureCase(48, "grand vitara ventilated seats", "Grand Vitara", "ventilated"),
  featureCase(49, "features in this", "Creta", "features", { context: { selectedVehicle: V.creta } }),
  featureCase(50, "does this have sunroof", "Seltos", "sunroof", { context: { selectedVehicle: V.seltos } }),
  featureCase(51, "does it have 6 airbags", "Creta", "airbags", { context: { selectedVehicle: V.creta } }),
  featureCase(52, "tell me safety features", "Scorpio N", "safety", { context: { selectedVehicle: V.scorpioN } }),
  featureCase(53, "what features do I lose", "Baleno", "features", { context: { selectedVehicle: V.balenoAlpha }, forbiddenTools: ["vehicle_compare"] }),

  colorCase(54, "seltos colors", "Seltos"),
  colorCase(55, "creta colors", "Creta"),
  colorCase(56, "venue colors", "Venue"),
  colorCase(57, "scorpio n colors", "Scorpio N"),
  colorCase(58, "baleno colors", "Baleno"),
  colorCase(59, "verna black color", "Verna", { requiredAnswerAny: ["black"] }),
  colorCase(60, "does creta have black", "Creta", { requiredAnswerAny: ["black"] }),
  colorCase(61, "colors of this", "Seltos", { context: { selectedVehicle: V.seltos } }),
  colorCase(62, "Seltos colors", "Seltos", { context: { selectedVehicle: V.verna } }),
  colorCase(63, "show me white color", "Creta", { context: { selectedVehicle: V.creta }, requiredAnswerAny: ["white"] }),
  colorCase(64, "dual tone colors in seltos", "Seltos", { requiredAnswerAny: ["dual", "tone", "color"] }),

  specCase(65, "eqs range", "Eqs", "range"),
  specCase(66, "mercedes eqs range", "Eqs", "range"),
  specCase(67, "be 6e range", "Be 6", "range"),
  specCase(68, "creta mileage", "Creta", "mileage"),
  specCase(69, "seltos mileage", "Seltos", "mileage"),
  specCase(70, "venue mileage", "Venue", "mileage"),
  specCase(71, "baleno mileage", "Baleno", "mileage"),
  specCase(72, "scorpio n mileage", "Scorpio N", "mileage"),
  specCase(73, "creta boot space", "Creta", "boot"),
  specCase(74, "seltos boot space", "Seltos", "boot"),
  specCase(75, "baleno boot space", "Baleno", "boot"),
  specCase(76, "creta ground clearance", "Creta", "ground"),
  specCase(77, "seltos engine cc", "Seltos", "cc"),
  specCase(78, "verna power", "Verna", "power"),
  specCase(79, "city power", "City", "power"),
  specCase(80, "range", "Eqs", "range", { context: { selectedVehicle: V.eqs } }),
  specCase(81, "mileage", "Baleno", "mileage", { context: { selectedVehicle: V.baleno } }),
  specCase(82, "boot space", "Creta", "boot", { context: { selectedVehicle: V.creta } }),

  compareCase(83, "creta vs seltos", ["Creta", "Seltos"]),
  compareCase(84, "creta vs venue", ["Creta", "Venue"]),
  compareCase(85, "baleno vs i20", ["Baleno", "I20"]),
  compareCase(86, "seltos vs scorpio n", ["Seltos", "Scorpio N"]),
  compareCase(87, "city vs verna", ["City", "Verna"]),
  compareCase(88, "hyryder vs grand vitara", ["Hyryder", "Grand Vitara"]),
  compareCase(89, "creta sx vs seltos htx", ["Creta", "Seltos"], { expectedVariants: ["SX", "HTX"] }),
  compareCase(90, "baleno alpha vs i20 asta", ["Baleno", "I20"], { expectedVariants: ["Alpha", "Asta"] }),
  compareCase(91, "compare creta and seltos petrol automatic variants", ["Creta", "Seltos"]),
  compareCase(92, "compare their petrol automatic variants", ["Creta", "Seltos"], { context: C.cretaSeltos }),
  compareCase(93, "which one is better", ["Creta", "Seltos"], { context: C.cretaSeltos }),
  compareCase(94, "which is safer", ["Creta", "Seltos"], { context: C.cretaSeltos }),
  compareCase(95, "which is cheaper", ["Creta", "Seltos"], { context: C.cretaSeltos }),
  compareCase(96, "show price difference", ["Creta", "Seltos"], { context: C.cretaSeltos }),
  compareCase(97, "now compare Baleno vs i20", ["Baleno", "I20"], { context: C.cretaSeltos }),
  compareCase(98, "compare this with Seltos", ["Creta", "Seltos"], { context: { selectedVehicle: V.creta } }),
  compareCase(99, "compare this with Venue", ["Creta", "Venue"], { context: { selectedVehicle: V.creta } }),
  compareCase(100, "compare Scorpio N and Scorpio price", ["Scorpio N", "Scorpio"]),

  priceCase(101, "now Creta price", "Creta", "new-delhi", { group: "F - Context switching / stale context isolation", context: C.cretaSeltos }),
  colorCase(102, "Seltos colors", "Seltos", { group: "F - Context switching / stale context isolation", context: C.cretaSeltos }),
  colorCase(103, "Seltos colors", "Seltos", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.verna } }),
  featureCase(104, "i20 sportz features", "I20", "features", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.baleno } }),
  priceCase(105, "same in Mumbai", "Creta", "mumbai", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.cretaDelhi }, expectedCanvasType: "unsupported_city_canvas" }),
  priceCase(106, "same in Noida", "Creta", "noida", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.cretaDelhi } }),
  featureCase(107, "Creta airbags", "Creta", "airbags", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.seltos } }),
  specCase(108, "range", "Eqs", "range", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.eqs } }),
  priceCase(109, "price", "Scorpio N", "new-delhi", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.scorpioN } }),
  priceCase(110, "scorpio n price", "Scorpio N", "new-delhi", { group: "F - Context switching / stale context isolation", context: C.scorpioNScorpio }),
  featureCase(111, "features in this", "Creta", "features", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.creta, ...C.cretaSeltos } }),
  honestyCase(112, "which one is better?", { group: "F - Context switching / stale context isolation", context: { selectedVehicle: V.creta }, forbiddenTools: ["vehicle_compare"] }),
  compareCase(113, "which one?", ["Creta", "Seltos"], { group: "F - Context switching / stale context isolation", context: C.cretaSeltos }),

  scoreCase(114, "how good is baleno petrol manual overall", ["Baleno"]),
  scoreCase(115, "is baleno good value", ["Baleno"], { expectedOperation: ["same_family_value_insights", "model_score_insights"] }),
  scoreCase(116, "is baleno alpha good value", ["Baleno"], { expectedOperation: "variant_score_insight", expectedVariants: ["Alpha"] }),
  scoreCase(117, "is creta good value", ["Creta"], { expectedOperation: ["same_family_value_insights", "model_score_insights"] }),
  scoreCase(118, "is creta sx good value", ["Creta"], { expectedOperation: "variant_score_insight", expectedVariants: ["SX"] }),
  scoreCase(119, "is seltos htx good value", ["Seltos"], { expectedOperation: "variant_score_insight", expectedVariants: ["HTX"] }),
  scoreCase(120, "is venue good for city", ["Venue"]),
  scoreCase(121, "is i20 sportz good", ["I20"], { expectedVariants: ["Sportz"] }),
  scoreCase(122, "how good is scorpio n overall", ["Scorpio N"]),
  scoreCase(123, "baleno alpha score", ["Baleno"], { expectedVariants: ["Alpha"] }),
  scoreCase(124, "creta sx score", ["Creta"], { expectedVariants: ["SX"] }),
  scoreCase(125, "venue diesel automatic score", ["Venue"]),
  scoreCase(126, "best value variant in baleno", ["Baleno"]),
  scoreCase(127, "top value in creta petrol manual", ["Creta"]),
  scoreCase(128, "score of this", ["Baleno"], { context: { selectedVehicle: V.balenoAlpha }, expectedVariants: ["Alpha"] }),
  scoreCase(129, "is this good value", ["Baleno"], { context: { selectedVehicle: V.balenoAlpha }, expectedOperation: "variant_score_insight", expectedVariants: ["Alpha"] }),
  scoreCase(130, "is this good value", ["Baleno"], { context: { selectedVehicle: V.baleno }, expectedOperation: ["same_family_value_insights", "model_score_insights"] }),

  crossScoreCase(131, "Creta vs Venue diagnostic score comparison", ["Creta", "Venue"]),
  crossScoreCase(132, "Creta vs Seltos diagnostic score comparison", ["Creta", "Seltos"]),
  crossScoreCase(133, "Baleno vs i20 diagnostic score comparison", ["Baleno", "I20"]),
  crossScoreCase(134, "Which scores better between Creta and Venue", ["Creta", "Venue"]),
  crossScoreCase(135, "Compare Creta and Venue scores", ["Creta", "Venue"]),
  crossScoreCase(136, "Compare Baleno and i20 value scores", ["Baleno", "I20"]),
  crossScoreCase(137, "Which has better safety score Creta or Venue", ["Creta", "Venue"]),
  crossScoreCase(138, "Which has better city score Baleno or i20", ["Baleno", "I20"]),
  crossScoreCase(139, "Which has better mileage score Creta or Seltos", ["Creta", "Seltos"]),
  crossScoreCase(140, "diagnostic score comparison of this and Seltos", ["Creta", "Seltos"], { context: { selectedVehicle: V.creta } }),

  honestyCase(141, "price"),
  honestyCase(142, "sunroof"),
  honestyCase(143, "mileage"),
  honestyCase(144, "colors"),
  honestyCase(145, "which one is better"),
  honestyCase(146, "compare these"),
  honestyCase(147, "best car"),
  honestyCase(148, "recommend me a car"),
  honestyCase(149, "car under 20 lakh automatic", {
    expectedTool: "vehicle_recommend",
    forbiddenTools: ["clarification", "vehicle_score_insight", "vehicle_pricelist"],
    expectedRowsMin: 1,
    expectedModelCountMin: 1,
    forbiddenAnswerAny: ["What would you like to check about the car", "Need one detail"],
  }),
  honestyCase(150, "electric suv under 25 lakh", {
    expectedTool: "vehicle_recommend",
    forbiddenTools: ["clarification", "vehicle_score_insight", "vehicle_pricelist"],
    expectedRowsMin: 1,
    expectedModelCountMin: 1,
    requiredAnswerAny: ["electric", "EV"],
    forbiddenAnswerAny: ["What would you like to check about the car", "Need one detail"],
    customAssert: ({ body }) => {
      const blob = JSON.stringify(body || {});
      assert(/electric|\bev\b/i.test(blob), "electric SUV query must preserve electric/EV scope in answer/data");
      assert(/suv/i.test(blob), "electric SUV query must preserve SUV scope in answer/data");
    },
  }),
  honestyCase(151, "cars with sunroof under 15 lakh"),
  honestyCase(152, "safest car under 20 lakh"),
  honestyCase(153, "family car under 15 lakh", {
    expectedTool: "vehicle_recommend",
    forbiddenTools: ["clarification", "vehicle_score_insight", "vehicle_pricelist"],
    expectedRowsMin: 1,
    expectedModelCountMin: 1,
    forbiddenAnswerAny: ["What would you like to check about the car", "Need one detail"],
  }),
  honestyCase(154, "I drive 100 km daily, what fuel should I choose", {
    expectedTool: "vehicle_explainer",
    forbiddenTools: ["vehicle_score_insight", "vehicle_pricelist"],
    requiredAnswerAny: ["fuel", "petrol", "diesel", "cng", "ev", "daily", "running"],
    forbiddenAnswerAny: ["What would you like to check about the car", "Need one detail", "value diagnostics"],
  }),
  honestyCase(155, "should I buy petrol or diesel", {
    expectedTool: "vehicle_explainer",
    forbiddenTools: ["vehicle_score_insight", "vehicle_pricelist"],
    requiredAnswerAny: ["petrol", "diesel", "running", "usage", "fuel"],
    forbiddenAnswerAny: ["Alto Tour", "value diagnostics"],
  }),
  honestyCase(156, "is diesel worth it now", {
    expectedTool: "vehicle_explainer",
    forbiddenTools: ["vehicle_score_insight", "vehicle_pricelist"],
    requiredAnswerAny: ["diesel", "running", "usage", "worth", "fuel"],
    forbiddenAnswerAny: ["Gurkha", "value diagnostics"],
  }),
  honestyCase(157, "should I wait for discount"),
  honestyCase(158, "are there offers on Creta"),
  honestyCase(159, "service cost of Creta", {
    expectedTool: "vehicle_explainer",
    expectedIntent: "unavailable",
    forbiddenTools: ["vehicle_pricelist", "vehicle_score_insight"],
    forbiddenCanvasTypes: ["pricelist_canvas", "price_breakup_canvas"],
    expectedModels: ["Creta"],
    requiredAnswerAll: ["Creta", "service"],
    requiredAnswerAny: ["not available", "do not have", "yet"],

    forbiddenAnswerAny: ["Creta Electric"],  }),
  honestyCase(160, "insurance price for Creta", {
    expectedTool: "vehicle_explainer",
    expectedIntent: "unavailable",
    forbiddenTools: ["vehicle_pricelist", "vehicle_score_insight"],
    forbiddenCanvasTypes: ["pricelist_canvas", "price_breakup_canvas"],
    expectedModels: ["Creta"],
    requiredAnswerAll: ["Creta", "insurance"],
    requiredAnswerAny: ["not available", "do not have", "yet"],

    forbiddenAnswerAny: ["Creta Electric"],  }),

  priceCase(161, "creta ka price delhi", "Creta"),
  featureCase(162, "seltos me sunroof hai kya", "Seltos", "sunroof"),
  scoreCase(163, "baleno alpha value for money hai kya", ["Baleno"], { expectedOperation: "variant_score_insight", expectedVariants: ["Alpha"] }),
  priceCase(164, "scorpio n ka on road price", "Scorpio N"),
  compareCase(165, "creta aur seltos me kaunsi better hai", ["Creta", "Seltos"]),
  featureCase(166, "isme airbags kitne hain", "Creta", "airbags", { context: { selectedVehicle: V.creta } }),
  specCase(167, "iska mileage kya hai", "Baleno", "mileage", { context: { selectedVehicle: V.baleno } }),
  priceCase(168, "same noida me batao", "Creta", "noida", { context: { selectedVehicle: V.cretaDelhi } }),
  priceCase(169, "mumbai me price batao", "Creta", "mumbai", { context: { selectedVehicle: V.cretaDelhi }, expectedCanvasType: "unsupported_city_canvas" }),
  scoreCase(170, "top model worth hai kya", ["Baleno"], { context: { selectedVehicle: V.balenoAlpha } }),

  honestyCase(171, "scorpio price", { expectedModels: ["Scorpio"], forbiddenAnswerAny: ["Scorpio N price"] }),
  priceCase(172, "scorpio n price", "Scorpio N"),
  honestyCase(173, "scorpion price", { forbiddenAnswerAny: ["Mahindra Scorpio N price list", "Mahindra Scorpio price list"] }),
  featureCase(174, "be6 sunroof", "Be 6", "sunroof"),
  featureCase(175, "be 6e sunroof", "Be 6", "sunroof"),
  specCase(176, "mahindra be 6 range", "Be 6", "range"),
  specCase(177, "merc eqs range", "Eqs", "range"),
  specCase(178, "merc benz eqs range", "Eqs", "range"),
  priceCase(179, "hundai creta price", "Creta"),
  honestyCase(180, "seltos htxx price", { expectedModels: ["Seltos"], forbiddenAnswerAny: ["HTX price list"] }),
  scoreCase(181, "baleno alfa good value", ["Baleno"], { expectedVariants: ["Alpha"] }),
  priceCase(182, "hyundai ceta price", "Creta"),
  colorCase(183, "kia selto colors", "Seltos"),
  compareCase(184, "grand vitara vs hyryder", ["Grand Vitara", "Hyryder"]),
  compareCase(185, "hyryder vs grand vitara price difference", ["Hyryder", "Grand Vitara"]),
];

const cases = rawCases.map(normalizeCaseGroup);

const runBridgeCase = async (runAciCoreLiveBridge, testCase) =>
  runAciCoreLiveBridge({
    message: testCase.message,
    context: testCase.context || {},
    user: null,
    session: {},
    meta: { source: "auditAciBuyerAnswerDeepV1", caseId: testCase.id },
  });

const runPublicCase = async (testCase) => {
  const response = await fetch(PUBLIC_ENDPOINT, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      message: testCase.message,
      context: testCase.context || {},
      meta: { source: "auditAciBuyerAnswerDeepV1", caseId: testCase.id },
    }),
  });
  const body = await response.json().catch(() => ({}));
  return {
    ...body,
    httpStatus: response.status,
    httpOk: response.ok,
  };
};

const assertCase = async (testCase, body, dbEvidence, precomputedSummary = null) => {
  const summary = precomputedSummary || summarizeResponse(body);
  const answer = text(body.answer || body.text || body.message);
  const blob = JSON.stringify(body || {});

  if (testCase.expectedTool) {
    assert.strictEqual(summary.tool, testCase.expectedTool, `expected tool ${testCase.expectedTool}, got ${summary.tool}`);
  }
  asArray(testCase.forbiddenTools).forEach((tool) => {
    assert.notStrictEqual(summary.tool, tool, `forbidden tool ${tool} was returned`);
  });

  if (testCase.expectedIntent) {
    assert.strictEqual(summary.intent, testCase.expectedIntent, `expected intent ${testCase.expectedIntent}, got ${summary.intent}`);
  }

  if (testCase.expectedOperation) {
    const expected = asArray(testCase.expectedOperation).length ? testCase.expectedOperation : [testCase.expectedOperation];
    assert(expected.includes(summary.operation), `expected operation ${expected.join(" or ")}, got ${summary.operation}`);
  }

  if (testCase.expectedCanvasType) {
    assert.strictEqual(
      summary.canvasType,
      testCase.expectedCanvasType,
      `expected canvas ${testCase.expectedCanvasType}, got ${summary.canvasType}`,
    );
  }
  asArray(testCase.forbiddenCanvasTypes).forEach((canvasType) => {
    assert.notStrictEqual(summary.canvasType, canvasType, `forbidden canvas ${canvasType} was returned`);
  });

  if (testCase.requiredAnswerAny?.length) {
    assert(hasAny(answer, testCase.requiredAnswerAny), `answer missing any of: ${testCase.requiredAnswerAny.join(", ")}`);
  }
  if (testCase.requiredAnswerAll?.length) {
    assert(hasAll(answer, testCase.requiredAnswerAll), `answer missing required terms: ${testCase.requiredAnswerAll.join(", ")}`);
  }
  asArray(testCase.forbiddenAnswerAny).forEach((term) => {
    assert(!new RegExp(term, "i").test(answer), `answer contains forbidden text: ${term}`);
  });

  asArray(testCase.expectedModels).forEach((model) => {
    assert(rx(model).test(blob), `response/data missing expected model: ${model}`);
  });
  const expectedModelsForScorpioCheck = asArray(testCase.expectedModels);
  const expectsScorpioN = expectedModelsForScorpioCheck.some((model) => /scorpio\s*n/i.test(model));
  const alsoExpectsBaseScorpio = expectedModelsForScorpioCheck.some((model) => /^\s*scorpio\s*$/i.test(model));

  if (expectsScorpioN && !alsoExpectsBaseScorpio) {
    assert(!/\bScorpio\s+S\b/i.test(blob), "Scorpio N case must not resolve to Scorpio S");
  }
  asArray(testCase.expectedVariants).forEach((variant) => {
    assert(rx(variant).test(blob), `response/data missing expected variant: ${variant}`);
  });

  if (Number.isFinite(testCase.expectedRowsMin)) {
    assert(summary.rowsCount >= testCase.expectedRowsMin, `expected at least ${testCase.expectedRowsMin} rows, got ${summary.rowsCount}`);
  }
  if (Number.isFinite(testCase.expectedModelCountMin)) {
    assert(summary.modelCount >= testCase.expectedModelCountMin, `expected at least ${testCase.expectedModelCountMin} models, got ${summary.modelCount}`);
  }
  if (Number.isFinite(testCase.expectedModuleComparisonMin)) {
    assert(
      summary.moduleComparisonCount >= testCase.expectedModuleComparisonMin,
      `expected at least ${testCase.expectedModuleComparisonMin} module comparisons, got ${summary.moduleComparisonCount}`,
    );
  }

  if (typeof testCase.customAssert === "function") {
    await testCase.customAssert({ body, summary, dbEvidence, testCase });
  }

  return summary;
};

const envInt = (name, fallback = 0) => {
  const value = Number.parseInt(process.env[name] || "", 10);
  return Number.isFinite(value) && value >= 0 ? value : fallback;
};

const clampInt = (value, min, max) => Math.max(min, Math.min(max, value));

const isDebug = () => process.env.ACI_DEEP_AUDIT_DEBUG === "1";

const parseCaseIds = (value = "") =>
  text(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

const groupKey = (value = "") => text(value).trim().slice(0, 1).toUpperCase();

const normalizeRequestedGroup = (value = "") => {
  const normalized = text(value).trim().toUpperCase();
  const explicit = normalized.match(/^GROUP\s+([A-K])$/i);
  if (explicit) return explicit[1].toUpperCase();
  if (/^[A-K]$/.test(normalized)) return normalized;
  return normalized;
};

const caseMatchesGroup = (testCase, requestedGroup = "") => {
  const expected = normalizeRequestedGroup(requestedGroup);
  if (!expected) return true;
  const canonical = text(testCase.groupKey || groupKey(testCase.group || testCase.id)).toUpperCase();
  return canonical === expected || (!testCase.groupKey && text(testCase.id).toUpperCase().startsWith(expected));
};

const caseMatchesIds = (testCase, requestedIds = []) => {
  if (!requestedIds.length) return true;
  const id = text(testCase.id).toLowerCase();
  return requestedIds.some((requested) => {
    const requestedId = text(requested).toLowerCase();
    return id === requestedId || id.startsWith(`${requestedId}-`);
  });
};

const formatFailure = (failures = []) =>
  asArray(failures)
    .map((item) => text(item).replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .join(" | ");

const dbEvidenceCount = (dbEvidence = {}) => {
  if (!dbEvidence || typeof dbEvidence !== "object") return 0;
  return [
    "priceRows",
    "colorRows",
    "specRows",
    "scoreRows",
    "rows",
    "count",
  ].reduce((total, key) => total + (Number(dbEvidence[key] || 0) || 0), 0);
};

const isNoDataAnswer = ({ body = {}, summary = {} } = {}) => {
  const answer = text(body.answer || body.text || body.message || summary.answerPreview);
  return /\b(no data|not available|unavailable|could not find|couldn't find|do not have|don't have|not found|pricing unavailable|unsupported|need more detail|need one more detail)\b/i.test(answer);
};

const noDataEntryFor = ({ testCase = {}, result = {}, dbEvidence = {}, body = null } = {}) => {
  const response = result.response || summarizeResponse(body || {});
  return {
    id: result.id || testCase.id,
    group: result.group || testCase.group,
    groupKey: result.groupKey || testCase.groupKey,
    message: result.message || testCase.message,
    pass: Boolean(result.pass),
    dbEvidence: dbEvidence || result.dbEvidence || null,
    tool: response.tool || "",
    intent: response.intent || "",
    title: response.title || "",
    answerPreview: response.answerPreview || "",
    noDataSuspicion: dbEvidenceCount(dbEvidence || result.dbEvidence) > 0
      ? "false_negative_possible"
      : "no_db_evidence",
  };
};

const HARD_FAILURE_PATTERNS = [
  /expected tool|forbidden tool|expected intent|expected operation|expected canvas|forbidden canvas/i,
  /missing expected model|response\/data missing expected model|must mention .*model|wrong model/i,
  /missing expected variant|wrong variant|must mention .*variant/i,
  /supported city|unsupported .*silently|wrong city|Delhi fallback/i,
  /comparison canvas|direct price must not route|stale comparison|comparison language/i,
  /returned no rows|DB has .* rows|rows exist|false unsupported|no confirmed/i,
  /same car|itself/i,
  /Scorpio N.*Scorpio S|Scorpio S.*Scorpio N/i,
  /ventilated.*seating capacity|seating capacity.*ventilated/i,
  /clarification despite|must not be treated as unsupported|must not return comparison/i,
  /EQS range|marked unavailable/i,
  /color evidence missing|price evidence missing|spec answer\/data must mention/i,
];

const SOFT_FAILURE_PATTERNS = [
  /internal phrase|price rows|weak wording|buyer answer|generic score|disclaimer|capitalize|plural variant-count|unsafe recommendation wording/i,
  /answer missing any|answer missing required terms/i,
];

const classifyFailures = (failures = []) => {
  const joined = formatFailure(failures);
  if (!joined) return { severity: "pass", hardFailures: [], softFailures: [], auditWarnings: [] };

  const failureList = asArray(failures).map((failure) => text(failure));
  const hardFailures = failureList.filter((failure) =>
    HARD_FAILURE_PATTERNS.some((pattern) => pattern.test(failure)),
  );
  const softFailures = failureList.filter((failure) =>
    !hardFailures.includes(failure) &&
    SOFT_FAILURE_PATTERNS.some((pattern) => pattern.test(failure)),
  );
  const auditWarnings = failureList.filter(
    (failure) => !hardFailures.includes(failure) && !softFailures.includes(failure),
  );

  if (hardFailures.length) {
    return { severity: "hard", hardFailures, softFailures, auditWarnings };
  }
  if (softFailures.length) {
    return { severity: "soft", hardFailures, softFailures, auditWarnings };
  }
  return { severity: "warning", hardFailures, softFailures, auditWarnings };
};

class CaseTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = "CaseTimeoutError";
  }
}

const withTimeout = (promise, timeoutMs, label) => {
  if (!timeoutMs) return promise;

  let timer = null;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new CaseTimeoutError(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    if (typeof timer.unref === "function") timer.unref();
  });

  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
};

const runOneCase = async ({
  runAciCoreLiveBridge,
  publicMode,
  testCase,
  caseTimeoutMs,
} = {}) => {
  let dbEvidence = null;
  let body = null;
  let summary = null;

  const execute = async () => {
    if (typeof testCase.dbEvidenceCheck === "function") {
      dbEvidence = await testCase.dbEvidenceCheck(testCase);
    }

    body = publicMode
      ? await runPublicCase(testCase)
      : await runBridgeCase(runAciCoreLiveBridge, testCase);
    summary = summarizeResponse(body);
    await assertCase(testCase, body, dbEvidence, summary);
    return summary;
  };

  try {
    const responseSummary = await withTimeout(
      execute(),
      caseTimeoutMs,
      `case id=${testCase.id}`,
    );
    return {
      pass: true,
      failures: [],
      dbEvidence,
      response: responseSummary,
    };
  } catch (error) {
    return {
      pass: false,
      failures: [error?.message || String(error)],
      dbEvidence,
      response: summary,
      timedOut: error instanceof CaseTimeoutError,
    };
  }
};

async function main() {
  const publicMode = process.env.ACI_DEEP_AUDIT_PUBLIC === "1";
  const caseTimeoutMs = envInt("ACI_DEEP_AUDIT_CASE_TIMEOUT_MS", 30000);
  const totalTimeoutMs = envInt("ACI_DEEP_AUDIT_TOTAL_TIMEOUT_MS", 0);
  const requestedGroup = text(process.env.ACI_DEEP_AUDIT_GROUP || "").trim();
  const requestedCaseIds = parseCaseIds(process.env.ACI_DEEP_AUDIT_CASE_IDS || "");
  const from = envInt("ACI_DEEP_AUDIT_FROM", 0);
  const limit = envInt("ACI_DEEP_AUDIT_LIMIT", 0);
  const workers = clampInt(envInt("ACI_DEEP_AUDIT_WORKERS", 1), 1, 10);
  const startedAllAt = Date.now();
  const totalDeadline = totalTimeoutMs ? startedAllAt + totalTimeoutMs : 0;

  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") {
    throw new Error("connectDB export not found");
  }
  await connectDB();

  let runAciCoreLiveBridge = null;
  if (!publicMode) {
    const mod = await import("../../services/aciCore/integration/aciCoreLiveBridge.service.js");
    runAciCoreLiveBridge = mod.runAciCoreLiveBridge || mod.default;
    if (typeof runAciCoreLiveBridge !== "function") {
      throw new Error("runAciCoreLiveBridge export not found");
    }
  }

  const resultSlots = [];
  const filteredCases = cases
    .map((testCase, index) => ({ ...testCase, originalIndex: index }))
    .filter((testCase) => caseMatchesGroup(testCase, requestedGroup))
    .filter((testCase) => caseMatchesIds(testCase, requestedCaseIds));
  const selectedCases = filteredCases.slice(from, limit ? from + limit : undefined);
  const auditWarnings = [];

  console.error(
    `[deep-audit] start mode=${publicMode ? "public" : "bridge"} totalCases=${cases.length} selected=${selectedCases.length} workers=${workers} group=${requestedGroup || "ALL"} caseIds=${requestedCaseIds.join(",") || "ALL"} from=${from} limit=${limit || "ALL"} caseTimeoutMs=${caseTimeoutMs} totalTimeoutMs=${totalTimeoutMs || "NONE"}`,
  );
  if (isDebug()) {
    console.error(`[deep-audit][debug] publicEndpoint=${PUBLIC_ENDPOINT}`);
    console.error(`[deep-audit] selectedIds=${selectedCases.map((testCase) => testCase.id).join(",")}`);
  }
  if (requestedGroup && !selectedCases.length) {
    auditWarnings.push(`No cases selected for group ${requestedGroup}`);
  }

  let nextCaseIndex = 0;
  let totalTimeoutLogged = false;

  const runSelectedCase = async (index, testCase) => {
    const startedAt = Date.now();
    console.error(
      `[deep-audit] ${index + 1}/${selectedCases.length} group=${testCase.groupKey || groupKey(testCase.group)} id=${testCase.id} message=${JSON.stringify(testCase.message)}`,
    );
    if (isDebug()) {
      console.error(`[deep-audit][debug] context=${JSON.stringify(testCase.context || {})}`);
    }

    const caseResult = await runOneCase({
      runAciCoreLiveBridge,
      publicMode,
      testCase,
      caseTimeoutMs,
    });
    const durationMs = Date.now() - startedAt;
    const classified = classifyFailures(caseResult.failures);
    const result = {
      id: testCase.id,
      group: testCase.group,
      groupKey: testCase.groupKey || groupKey(testCase.group),
      message: testCase.message,
      pass: caseResult.pass,
      severity: classified.severity,
      failures: caseResult.failures,
      hardFailures: classified.hardFailures,
      softFailures: classified.softFailures,
      auditWarnings: classified.auditWarnings,
      durationMs,
      dbEvidence: caseResult.dbEvidence,
      response: caseResult.response,
      timedOut: Boolean(caseResult.timedOut),
    };

    resultSlots[index] = result;

    if (result.pass) {
      console.error(
        `[pass] id=${result.id} durationMs=${durationMs} tool=${result.response?.tool || ""}`,
      );
    } else {
      console.error(
        `[fail] id=${result.id} durationMs=${durationMs} failures=${formatFailure(result.failures)}`,
      );
    }
  };

  const runWorker = async () => {
    while (nextCaseIndex < selectedCases.length) {
      if (totalDeadline && Date.now() >= totalDeadline) {
        if (!totalTimeoutLogged) {
          totalTimeoutLogged = true;
          console.error(
            `[deep-audit] total-timeout reached before case ${nextCaseIndex + 1}/${selectedCases.length}; stopping early`,
          );
        }
        break;
      }

      const index = nextCaseIndex;
      nextCaseIndex += 1;
      await runSelectedCase(index, selectedCases[index]);
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(workers, selectedCases.length || 1) }, () => runWorker()),
  );

  const results = resultSlots.filter(Boolean);

  const failed = results.filter((item) => !item.pass);
  const hardFailures = results.filter((item) => item.hardFailures?.length);
  const softFailures = results.filter((item) => item.softFailures?.length);
  const resultAuditWarnings = results.filter((item) => item.auditWarnings?.length);
  const noDataAnswers = results
    .filter((item) => isNoDataAnswer({ summary: item.response || {} }))
    .map((item) => noDataEntryFor({ result: item }));
  const byGroup = results.reduce((acc, item) => {
    const key = item.groupKey || groupKey(item.group);
    acc[key] ||= { group: item.group, total: 0, passed: 0, failed: 0, hard: 0, soft: 0, warnings: 0 };
    acc[key].total += 1;
    if (item.pass) acc[key].passed += 1;
    else acc[key].failed += 1;
    if (item.hardFailures?.length) acc[key].hard += 1;
    if (item.softFailures?.length) acc[key].soft += 1;
    if (item.auditWarnings?.length) acc[key].warnings += 1;
    return acc;
  }, {});

  const summary = {
    suite: "ACI Buyer Answer Deep Audit v1",
    mode: publicMode ? "public" : "bridge",
    ok: failed.length === 0,
    totalCases: cases.length,
    selected: selectedCases.length,
    workers,
    executed: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    hardFailureCount: hardFailures.length,
    softFailureCount: softFailures.length,
    auditWarningCount: auditWarnings.length + resultAuditWarnings.length,
    noDataAnswerCount: noDataAnswers.length,
    hardFailures,
    softFailures,
    noDataAnswers,
    auditWarnings: [
      ...auditWarnings,
      ...resultAuditWarnings.map((item) => ({
        id: item.id,
        group: item.group,
        groupKey: item.groupKey,
        message: item.message,
        warnings: item.auditWarnings,
      })),
    ],
    filters: {
      group: requestedGroup || null,
      caseIds: requestedCaseIds,
      from,
      limit: limit || null,
    },
    timeouts: {
      caseTimeoutMs,
      totalTimeoutMs: totalTimeoutMs || null,
      totalTimedOut: Boolean(totalDeadline && Date.now() >= totalDeadline && results.length < selectedCases.length),
    },
    durationMs: Date.now() - startedAllAt,
    byGroup,
    results,
  };

  console.log(JSON_START);
  console.log(JSON.stringify(summary, null, 2));
  console.log(JSON_END);

  return failed.length === 0 && results.length === selectedCases.length;
}

let finalExitCode = 0;

main()
  .then((ok) => {
    finalExitCode = ok ? 0 : 1;
  })
  .catch((error) => {
    console.error(error);
    finalExitCode = 1;
  })
  .finally(async () => {
    if (mongoose.connection?.readyState) {
      await mongoose.disconnect();
    }
    process.exit(finalExitCode);
  });
