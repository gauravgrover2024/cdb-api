import {
  retrieveAciDbCandidates,
} from "../candidates/aciDbCandidateRetriever.js";
import { parseHybridMeaningFrame } from "../understanding/hybridMeaningFrame.parser.js";
import { runAciUnderstandingEngine } from "../understanding/aciUnderstandingEngine.js";
import mongoose from "mongoose";

import { buildLegacyPlanFromAciMeaningFrame } from "./aciCoreToLegacyPlan.adapter.js";
import { executeAciPlannerPlan } from "../../aiAgent/aiAgent.executor.js";
import { normalizeAciFinalResponse } from "../../aiAgent/aiAgent.contractNormalizer.js";
import { composeAciAnswer } from "../../aiAgent/aiAgent.answerComposer.js";
import { runVehiclePricelistNewCarsTool } from "../../aiAgent/tools/newCars/vehiclePricelist.tool.js";
import { buildVehiclePricelistResponse } from "../../aiAgent/aiAgent.responseTools.js";
import {
  buildAciLanguageSeed,
  renderAciTemplate,
} from "../language/aciAnswerLanguageComposer.js";
import {
  applyContextIsolationRules,
  buildContextPatchFromState,
  getContextForToolPlan,
  hydrateContextFromCandidates,
  mergeContextPatches,
} from "../context/aciContextManager.service.js";

const truthy = (value = "") =>
  ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const uniqueKeys = (items = []) =>
  [...new Set(items.map((item) => cleanText(item)).filter(Boolean))];

const getSnapshotKeys = (items = []) =>
  uniqueKeys(
    asArray(items).map((item) =>
      item?.canonicalKey ||
      item?.displayName ||
      item?.rawText ||
      item?.key ||
      "",
    ),
  );

const hasContextReference = (message = "") =>
  /\b(this|that|it|its|one|same|current|selected|previous|earlier|above)\b/i.test(message);

const hasComparisonLanguage = (message = "") =>
  /\b(vs|v\/s|versus|compare|comparison|compared|better|better than|difference between|which one|which should|choose|pick|recommend|verdict)\b/i.test(message);

const hasActiveComparisonFollowUp = ({ message = "", context = {} } = {}) => {
  const vehicles =
    context?.activeComparison?.vehicles ||
    context?.selectedComparisonSet?.vehicles ||
    context?.contextState?.activeComparison?.vehicles ||
    context?.aciContextState?.activeComparison?.vehicles ||
    [];

  if (!Array.isArray(vehicles) || vehicles.length < 2) return false;

  return /\b(which one|which is better|better|safer|safety|their|change city|which should i|should i buy|choose|pick|recommend|verdict|final choice)\b/i.test(
    message,
  );
};

const expandActiveComparisonFollowUpMessage = ({ message = "", context = {} } = {}) => {
  if (!hasActiveComparisonFollowUp({ message, context })) return message;

  const activeComparison =
    context?.activeComparison ||
    context?.selectedComparisonSet ||
    context?.contextState?.activeComparison ||
    context?.aciContextState?.activeComparison ||
    {};

  const vehicles =
    activeComparison?.vehicles ||
    context?.selectedComparisonSet?.vehicles ||
    context?.contextState?.activeComparison?.vehicles ||
    context?.aciContextState?.activeComparison?.vehicles ||
    [];

  const labels = vehicles
    .map((vehicle = {}) =>
      cleanText(
        [
          vehicle.fullModel || [vehicle.make, vehicle.model].filter(Boolean).join(" "),
          vehicle.variant || vehicle.variantName,
        ]
          .filter(Boolean)
          .join(" "),
      ),
    )
    .filter(Boolean);

  if (labels.length < 2) return message;

  const fuelFilter = cleanText(
    activeComparison.fuelFilter ||
      activeComparison.fuelType ||
      activeComparison.fuel ||
      "",
  );

  const featureText = asArray(activeComparison.features)
    .map((feature) =>
      cleanText(
        typeof feature === "string"
          ? feature
          : feature?.displayName || feature?.feature || feature?.featureKey || feature?.key || "",
      )
        .replace(/_/g, " "),
    )
    .filter(Boolean)
    .join(" ");

  const scopeText = [
    labels.join(" vs "),
    fuelFilter ? `${fuelFilter} variants` : "",
    featureText ? `based on ${featureText}` : "",
  ]
    .filter(Boolean)
    .join(" ");

  return `${message} ${scopeText}`;
};

const hasBroadVehicleLanguage = (message = "") =>
  /\b(cars?|vehicles?|models?|options?|suvs?|sedans?|hatchbacks?|mpvs?|muvs?)\b/i.test(message);

const stripVehicleContextForTurn = ({ context = {}, clearComparison = false } = {}) => {
  const isolated = { ...(context || {}) };
  const preservedSelectedVehicle = {};
  const selectedVehicle = context?.selectedVehicle || {};

  if (selectedVehicle.city) preservedSelectedVehicle.city = selectedVehicle.city;
  if (selectedVehicle.citySlug) preservedSelectedVehicle.citySlug = selectedVehicle.citySlug;

  delete isolated.anchorMake;
  delete isolated.anchorBrand;
  delete isolated.anchorModel;
  delete isolated.anchorFullModel;
  delete isolated.anchorVariant;
  delete isolated.model;
  delete isolated.variant;
  delete isolated.vehicle;

  if (Object.keys(preservedSelectedVehicle).length) {
    isolated.selectedVehicle = preservedSelectedVehicle;
  } else {
    delete isolated.selectedVehicle;
  }

  if (clearComparison) {
    delete isolated.selectedComparisonSet;
    delete isolated.comparisonTargets;
  }

  return isolated;
};

const isolateAciCoreBridgeContext = ({
  message = "",
  context = {},
  candidateSnapshot = {},
} = {}) => {
  const makes = getSnapshotKeys(candidateSnapshot?.vehicles?.makes);
  const models = getSnapshotKeys(candidateSnapshot?.vehicles?.models);
  const variants = getSnapshotKeys(candidateSnapshot?.vehicles?.variants);
  const features = getSnapshotKeys(candidateSnapshot?.taxonomy?.features);
  const bodyTypes = getSnapshotKeys(candidateSnapshot?.taxonomy?.bodyTypes);
  const fuelTypes = getSnapshotKeys(candidateSnapshot?.taxonomy?.fuelTypes);
  const transmissions = getSnapshotKeys(candidateSnapshot?.taxonomy?.transmissions);
  const budgets = asArray(candidateSnapshot?.commerce?.budgets);

  const explicitTargetCount = Math.max(models.length, variants.length);
  const contextReference = hasContextReference(message);
  const comparisonLanguage = hasComparisonLanguage(message);
  const explicitComparison = (models.length >= 2 || explicitTargetCount >= 2) &&
    (comparisonLanguage || models.length >= 2);
  const hasDiscoveryFilters =
    makes.length > 0 ||
    features.length > 0 ||
    bodyTypes.length > 0 ||
    fuelTypes.length > 0 ||
    transmissions.length > 0 ||
    budgets.length > 0;
  const broadDiscovery =
    models.length === 0 &&
    variants.length === 0 &&
    hasDiscoveryFilters &&
    !contextReference &&
    (hasBroadVehicleLanguage(message) || makes.length > 0);
  const explicitVehicleSwitch =
    explicitTargetCount > 0 &&
    !contextReference;

  if (explicitComparison) {
    return {
      context: stripVehicleContextForTurn({ context, clearComparison: true }),
      isolation: "explicit_comparison_targets",
    };
  }

  if (broadDiscovery) {
    return {
      context: stripVehicleContextForTurn({ context, clearComparison: true }),
      isolation: "broad_discovery_without_model",
    };
  }

  if (explicitVehicleSwitch) {
    return {
      context: stripVehicleContextForTurn({ context, clearComparison: true }),
      isolation: "explicit_vehicle_switch",
    };
  }

  return {
    context,
    isolation: "preserve_context",
  };
};

const isAciCoreLiveBridgeEnabled = () =>
  process.env.ACI_CORE_LIVE_BRIDGE_ENABLED === undefined
    ? true
    : truthy(process.env.ACI_CORE_LIVE_BRIDGE_ENABLED);

const shouldUseAciCoreLiveBridge = ({ message = "" } = {}) => {
  if (!isAciCoreLiveBridgeEnabled()) return false;

  const text = String(message || "").trim();
  if (!text) return false;

  return true;
};


const SUPPORTED_PRICE_CITY_LABELS_FOR_FAST_PATH = ["New Delhi", "Noida", "Gurgaon"];

const UNSUPPORTED_PRICE_CITY_ALIASES_FOR_FAST_PATH = [
  ["mumbai", "Mumbai"],
  ["bombay", "Mumbai"],
  ["bangalore", "Bangalore"],
  ["bengaluru", "Bangalore"],
  ["pune", "Pune"],
  ["chennai", "Chennai"],
  ["hyderabad", "Hyderabad"],
  ["kolkata", "Kolkata"],
  ["ahmedabad", "Ahmedabad"],
];

const hasAciPriceIntentForFastUnsupportedCity = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  if (!normalized) return false;

  if (/\b(compare|vs|versus)\b/.test(normalized)) return false;

  return /\b(on road|on-road|onroad|price|pricing|pricelist|price list|breakup|quotation|quote)\b/.test(normalized);
};

const findUnsupportedPriceCityForFastPath = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  return UNSUPPORTED_PRICE_CITY_ALIASES_FOR_FAST_PATH.find(([alias]) =>
    new RegExp(`(^|\\b)${alias}(\\b|$)`, "i").test(normalized),
  );
};

const buildUnsupportedCityFastPathResponse = ({
  message = "",
  effectiveMessage = "",
  requestedCity = "",
  durationMs = 0,
} = {}) => {
  const unsupportedCity = {
    requestedCity,
    supportedCities: SUPPORTED_PRICE_CITY_LABELS_FOR_FAST_PATH,
    reason: "pricing_city_not_supported",
    canRetryWithSupportedCity: true,
  };

  return {
    intent: "vehicle_pricelist",
    displayMode: "canvas",
    canvasType: "unsupported_city_canvas",
    inlineType: null,
    title: `Pricing unavailable in ${requestedCity}`,
    answer: renderAciTemplate(
      "unsupported_city_price",
      {
        city: requestedCity,
        supportedCities: SUPPORTED_PRICE_CITY_LABELS_FOR_FAST_PATH,
      },
      {
        seed: buildAciLanguageSeed("unsupported_city_price", requestedCity, message, effectiveMessage),
      },
    ).text,
    matched: 0,
    count: 0,
    rows: [],
    items: [],
    data: {
      rows: [],
      items: [],
      unsupportedCity,
      supportedCities: SUPPORTED_PRICE_CITY_LABELS_FOR_FAST_PATH,
      canvasType: "unsupported_city_canvas",
    },
    unsupportedCity,
    supportedCities: SUPPORTED_PRICE_CITY_LABELS_FOR_FAST_PATH,
    sourceTransparency: {
      modulesChecked: ["aci_vehicle_price_rows"],
      matched: 0,
      dataSource: "unsupported_city_fast_path",
      recordCount: 0,
    },
    aciCoreBridge: {
      enabled: true,
      durationMs,
      selectedParser: "",
      usedGemini: false,
      primaryTask: "on_road_estimate",
      tool: "vehicle_pricelist",
      planMode: "single_tool",
      contextIsolation: "unsupported_city_fast_path",
      originalMessage: message,
      effectiveMessage: effectiveMessage || message,
    },
    contextPatch: {
      anchorCity: requestedCity,
    },
  };
};

const maybeReturnUnsupportedCityFastPath = ({
  message = "",
  effectiveMessage = "",
  startedAt = 0,
} = {}) => {
  if (!hasAciPriceIntentForFastUnsupportedCity(effectiveMessage || message)) {
    return null;
  }

  const match = findUnsupportedPriceCityForFastPath(effectiveMessage || message);
  if (!match) return null;

  const [, requestedCity] = match;

  return buildUnsupportedCityFastPathResponse({
    message,
    effectiveMessage,
    requestedCity,
    durationMs: startedAt ? Date.now() - startedAt : 0,
  });
};

const hasAciPriceIntent = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  if (!normalized) return false;
  if (hasComparisonLanguage(normalized)) return false;
  return /\b(on road|on-road|onroad|price|pricing|pricelist|price list|breakup|quotation|quote)\b/.test(normalized);
};

const isBroadDiscoveryOrComparison = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  if (/\b(compare|vs|versus)\b/.test(normalized)) return true;
  if (/\b(under|below|above|over|between|budget|sunroof|abs|adas|alloy|airbags?|with|having|must have)\b/i.test(normalized)) {
    return true;
  }
  return false;
};

const hasSupportedCityMention = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  return /\b(delhi|new delhi|noida|gurgaon|gurugram)\b/i.test(normalized);
};

const getCitySlugFromMessage = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  if (normalized.includes("new delhi") || normalized.includes("delhi")) return "new-delhi";
  if (normalized.includes("noida")) return "noida";
  if (normalized.includes("gurgaon") || normalized.includes("gurugram")) return "gurgaon";
  return "";
};

const getSupportedCitySlugFromContext = (context = {}) => {
  const selectedVehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    {};
  const city = cleanText(
    selectedVehicle.citySlug ||
      selectedVehicle.city ||
      context?.anchorCity ||
      context?.city ||
      "",
  );
  const normalized = normalizeFastPathSlug(city);
  if (["new-delhi", "noida", "gurgaon"].includes(normalized)) return normalized;
  if (normalized === "delhi") return "new-delhi";
  if (normalized === "gurugram") return "gurgaon";
  return "";
};


const normalizeFastPathText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeFastPathSlug = (value = "") =>
  normalizeFastPathText(value).replace(/\s+/g, "-").replace(/^gurugram$/, "gurgaon");

const escapeFastPathRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const FAST_PRICE_ENTITY_STOP_WORDS = new Set([
  "show", "tell", "me", "please", "what", "is", "the", "for", "of", "in",
  "on", "road", "onroad", "price", "pricing", "pricelist", "list",
  "breakup", "break", "up", "ex", "showroom", "delhi", "new", "noida",
  "gurgaon", "gurugram", "mumbai", "quote", "quotation",
]);

const extractVehiclePhraseForSupportedPriceFastPath = (message = "") => {
  const normalized = normalizeFastPathText(message)
    .replace(/\bnew delhi\b/g, " ")
    .replace(/\b(delhi|noida|gurgaon|gurugram)\b/g, " ")
    .replace(/\bon road\b/g, " ")
    .replace(/\bonroad\b/g, " ")
    .replace(/\bprice list\b/g, " ")
    .replace(/\b(pricelist|price|pricing|breakup|quotation|quote)\b/g, " ")
    .replace(/\b(show|tell|me|please|what|is|the|for|of|in)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return normalized;
};

const getFastPathDb = () =>
  mongoose.connection?.readyState === 1 && mongoose.connection?.db
    ? mongoose.connection.db
    : null;

const scoreFastPathModelSummary = ({ row = {}, vehiclePhrase = "", tokens = [] } = {}) => {
  const makeText = normalizeFastPathText(row.make || row.brand || "");
  const modelText = normalizeFastPathText(row.model || "");
  const fullText = normalizeFastPathText(
    [row.make, row.model, row.fullModel, row.displayName, row.modelKey]
      .filter(Boolean)
      .join(" "),
  );
  const modelTokens = new Set(modelText.split(/\s+/).filter(Boolean));
  const fullTokens = new Set(fullText.split(/\s+/).filter(Boolean));

  let score = 0;

  for (const token of tokens) {
    if (modelTokens.has(token)) score += 10;
    else if (fullTokens.has(token)) score += 4;
    else if (fullText.includes(token)) score += 1;
  }

  if (modelText && vehiclePhrase === modelText) score += 40;
  if (modelText && vehiclePhrase.startsWith(`${modelText} `)) score += 30;
  if (modelText && vehiclePhrase.includes(modelText)) score += 18;
  if (makeText && vehiclePhrase.includes(makeText)) score += 4;

  // Penalise model summaries that add extra model words not mentioned by the user
  // e.g. prefer "Creta" over "Creta Electric" for "Creta SX".
  for (const modelToken of modelTokens) {
    if (modelToken && !tokens.includes(modelToken)) score -= 7;
  }

  return score;
};

const resolveSupportedExactPriceVehicleFromMessage = async ({
  message = "",
  citySlug = "",
} = {}) => {
  const db = getFastPathDb();
  if (!db || !message || !citySlug) return null;

  const vehiclePhrase = extractVehiclePhraseForSupportedPriceFastPath(message);
  const tokens = vehiclePhrase
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length >= 2 && !FAST_PRICE_ENTITY_STOP_WORDS.has(token));

  if (!tokens.length) return null;

  const regexes = tokens
    .slice(0, 8)
    .map((token) => new RegExp(`(^|\\b)${escapeFastPathRegex(token)}(\\b|$)`, "i"));

  const modelKeys = tokens.map((token) => token.replace(/\s+/g, "-")).filter(Boolean);

  const rows = await db
    .collection("aci_vehicle_model_summary")
    .find(
      {
        citySlug,
        $or: [
          { model: { $in: regexes } },
          { fullModel: { $in: regexes } },
          { displayName: { $in: regexes } },
          { make: { $in: regexes } },
          { modelKey: { $in: modelKeys } },
        ],
      },
      {
        projection: {
          make: 1,
          makeKey: 1,
          model: 1,
          modelKey: 1,
          fullModel: 1,
          displayName: 1,
          citySlug: 1,
        },
      },
    )
    .limit(40)
    .toArray();

  const best = rows
    .map((row) => ({
      row,
      score: scoreFastPathModelSummary({ row, vehiclePhrase, tokens }),
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)[0];

  if (!best?.row) return null;

  const row = best.row;
  const makeTokens = normalizeFastPathText(row.make || "").split(/\s+/).filter(Boolean);
  const modelTokens = normalizeFastPathText(row.model || row.displayName || row.fullModel || "")
    .split(/\s+/)
    .filter(Boolean);

  const removeTokens = new Set([...makeTokens, ...modelTokens]);
  const variant = tokens.filter((token) => !removeTokens.has(token)).join(" ").trim();

  return {
    make: row.make || "",
    model: row.model || row.displayName || row.fullModel || "",
    modelKey: row.modelKey || "",
    variant,
    citySlug,
    vehiclePhrase,
    score: best.score,
  };
};

const getFastPathRows = (result = {}) =>
  result.rows ||
  result.data?.rows ||
  result.widget?.rows ||
  result.records ||
  result.variants ||
  [];

const validateSupportedExactPriceFastPathResult = ({
  result = {},
  resolved = {},
  citySlug = "",
} = {}) => {
  const rows = getFastPathRows(result);
  if (!rows.length) return false;

  const canvasType = result.canvasType || result.widget?.canvasType || result.data?.canvasType || "";
  if (!["price_breakup_canvas", "pricelist_canvas"].includes(canvasType)) return false;

  const expectedModel = normalizeFastPathText(resolved.model || "");
  const expectedVariant = normalizeFastPathText(resolved.variant || "");
  const expectedCitySlug = normalizeFastPathSlug(citySlug);

  const firstRow = rows[0] || {};
  const rowModel = normalizeFastPathText(firstRow.model || firstRow.fullModel || firstRow.displayName || "");
  const rowCitySlug = normalizeFastPathSlug(
    firstRow.citySlug ||
      firstRow.city ||
      result.requested?.city ||
      result.citySlug ||
      result.city ||
      "",
  );

  const modelMatches =
    expectedModel &&
    rowModel &&
    (rowModel === expectedModel ||
      rowModel.includes(expectedModel) ||
      expectedModel.includes(rowModel));

  if (!modelMatches) return false;

  if (expectedCitySlug && rowCitySlug && rowCitySlug !== expectedCitySlug) {
    return false;
  }

  if (expectedVariant) {
    const expectedVariantTokens = expectedVariant.split(/\s+/).filter(Boolean);
    const variantMatches = rows.some((row) => {
      const rowVariant = normalizeFastPathText(row.variant || row.variantKey || row.fullVariant || "");
      return expectedVariantTokens.every((token) => rowVariant.includes(token));
    });

    if (!variantMatches) return false;
  }

  return true;
};

const maybeReturnSupportedExactPriceFastPath = async ({
  message = "",
  effectiveMessage = "",
  context = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  const text = effectiveMessage || message;
  if (!hasAciPriceIntent(text)) return null;
  if (isBroadDiscoveryOrComparison(text)) return null;

  const citySlug = hasSupportedCityMention(text)
    ? getCitySlugFromMessage(text)
    : getSupportedCitySlugFromContext(context) || "new-delhi";
  if (!citySlug) return null;

  const resolved = await resolveSupportedExactPriceVehicleFromMessage({
    message: text,
    citySlug,
  });

  if (!resolved?.model) return null;

  const contextReference = hasContextReference(text);
  const isolation = "supported_exact_price_fast_path";
  const isolatedContext = contextReference
    ? context
    : stripVehicleContextForTurn({ context, clearComparison: true });

  const toolPlan = {
    tool: "vehicle_pricelist",
    input: {
      message: text,
      query: text,
      make: resolved.make,
      model: resolved.model,
      variant: resolved.variant,
      city: citySlug,
      limit: resolved.variant ? 24 : 240,
    },
    entities: {
      make: resolved.make,
      model: resolved.model,
      variant: resolved.variant,
    },
    filters: {
      city: citySlug,
      variant: resolved.variant,
    },
    limit: resolved.variant ? 24 : 240,
  };

  const toolResult = await runVehiclePricelistNewCarsTool({
    userMessage: text,
    message: text,
    query: text,
    make: resolved.make,
    model: resolved.model,
    variant: resolved.variant,
    city: citySlug,
    context: isolatedContext,
    toolPlan,
  });

  if (!validateSupportedExactPriceFastPathResult({ result: toolResult, resolved, citySlug })) {
    return null;
  }

  const formatted = buildVehiclePricelistResponse({
    toolPlan,
    runtimeData: toolResult,
    context: isolatedContext,
  });

  const normalized = await normalizeAciFinalResponse(formatted, {
    message: text,
    context: isolatedContext,
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "on_road_estimate",
    tool: "vehicle_pricelist",
    planMode: "single_tool",
    contextIsolation: isolation,
    originalMessage: originalMessage || message,
    effectiveMessage: text,
  };

  return composeAciAnswer({
    ...normalized,
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: bridge,
    },
  });
};



const ensureDiagnosticOnlyAnswerNote = (answer = "") => {
  const text = cleanText(answer);
  if (!text) return "This is diagnostic-only, not a final recommendation.";
  if (/\bdiagnostic-only\b/i.test(text)) return text;
  return `${text} This is diagnostic-only, not a final recommendation.`;
};

const detectCrossModelScoreDiagnosticRequest = ({ message = "", candidateSnapshot = {} } = {}) => {
  const raw = String(message || "");
  const normalized = cleanText(raw);

  const hasComparison =
    hasComparisonLanguage(raw) ||
    /\b(vs|v\/s|versus|compare|comparison|against)\b/i.test(raw);

  const hasScoreLanguage =
    /\b(score|scores|scoring|overall|diagnostic|which\s+scores?\s+better|scores?\s+better|better\s+overall)\b/i.test(raw);

  if (!hasComparison || !hasScoreLanguage) return null;

  const modelKeys = asArray(candidateSnapshot?.vehicles?.models)
    .map((item = {}) =>
      item?.metadata?.raw?.shortModelKey ||
      item?.metadata?.raw?.modelKey ||
      item?.metadata?.raw?.rawModel ||
      item?.metadata?.model ||
      item?.rawText ||
      item?.canonicalKey ||
      item?.displayName ||
      ""
    )
    .map((key) => normalizeFastPathSlug(key))
    .filter(Boolean);

  const uniqueModelKeys = [...new Set(modelKeys)].slice(0, 2);
  if (uniqueModelKeys.length < 2) return null;

  const fuelKey =
    /\bcng\b/i.test(raw)
      ? "cng"
      : /\bdiesel\b/i.test(raw)
        ? "diesel"
        : /\belectric|ev\b/i.test(raw)
          ? "electric"
          : /\bhybrid\b/i.test(raw)
            ? "hybrid"
            : /\bpetrol\b/i.test(raw)
              ? "petrol"
              : "";

  const transmissionKey =
    /\bmanual|mt\b/i.test(raw)
      ? "manual"
      : /\bautomatic|auto|amt|cvt|dct|imt|iv?t\b/i.test(raw)
        ? "automatic"
        : "";

  return {
    operation: "cross_model_score_diagnostic",
    targets: uniqueModelKeys.map((modelKey) => ({
      modelKey,
      ...(fuelKey ? { fuelKey } : {}),
      ...(transmissionKey ? { transmissionKey } : {}),
    })),
    models: uniqueModelKeys,
    comparisonModels: uniqueModelKeys,
    ...(fuelKey ? { fuelKey } : {}),
    ...(transmissionKey ? { transmissionKey } : {}),
    routingReason: "cross_model_score_diagnostic_request",
    normalizedMessage: normalized,
  };
};

const applyCrossModelScoreDiagnosticPlanOverride = ({ plan = {}, override = null } = {}) => {
  if (!override) return plan;

  const baseTool = plan.tools?.[0] || {};

  const patchedTool = {
    ...baseTool,
    tool: "vehicle_score_insight",
    operation: override.operation,
    targets: override.targets,
    models: override.models,
    comparisonModels: override.comparisonModels,
    ...(override.fuelKey ? { fuelKey: override.fuelKey } : {}),
    ...(override.transmissionKey ? { transmissionKey: override.transmissionKey } : {}),
    input: {
      ...(baseTool.input || {}),
      ...override,
    },
    args: {
      ...(baseTool.args || {}),
      ...override,
    },
    params: {
      ...(baseTool.params || {}),
      ...override,
    },
    filters: {
      ...(baseTool.filters || {}),
      ...(override.fuelKey ? { fuelKey: override.fuelKey } : {}),
      ...(override.transmissionKey ? { transmissionKey: override.transmissionKey } : {}),
    },
    entities: {
      ...(baseTool.entities || {}),
      models: override.models,
      comparisonModels: override.comparisonModels,
      targets: override.targets,
      operation: override.operation,
    },
  };

  return {
    ...plan,
    intent: "vehicle_score_insight",
    conversationMode: "diagnostic",
    mode: plan.mode || "single_tool",
    tools: [patchedTool],
    output: {
      ...(plan.output || {}),
      canvasType: "score_insight_canvas",
      inlineType: "score_insight_summary",
    },
    meta: {
      ...(plan.meta || {}),
      crossModelScoreDiagnosticOverride: true,
      crossModelScoreDiagnosticTargets: override.targets,
    },
  };
};




const isExplicitDirectPriceLookupRequest = (message = "", primaryTask = "") => {
  const raw = String(message || "");
  const normalized = cleanText(raw);
  const task = cleanText(primaryTask);

  const hasPriceIntent =
    task === "price_lookup" ||
    /\b(price|on[-\s]?road|ex[-\s]?showroom|cost|kitna|rate|pricing)\b/i.test(raw);

  if (!hasPriceIntent) return false;

  // Keep comparison only when the user explicitly asks for a difference/comparison.
  const asksComparisonPrice =
    /\b(price\s+difference|difference|diff|compare|comparison|vs|versus|between|cheaper|costlier|expensive)\b/i.test(raw);

  if (asksComparisonPrice) return false;

  return normalized.length > 0;
};

const buildDirectPriceLookupOverride = ({ message = "", meaningFrame = {}, contextState = {} } = {}) => {
  if (!isExplicitDirectPriceLookupRequest(message, meaningFrame?.primaryTask)) return null;

  const vehicle =
    contextState?.selectedVehicle ||
    contextState?.anchors?.primaryVehicle ||
    {};

  const model =
    vehicle.model ||
    vehicle.fullModel ||
    vehicle.shortModelKey ||
    vehicle.modelKey ||
    "";

  if (!model) return null;

  return {
    tool: "vehicle_pricelist",
    intent: "vehicle_pricelist",
    routingReason: "direct_price_lookup_overrides_comparison_context",
    model,
    make: vehicle.make || vehicle.brand || "",
    fullModel: vehicle.fullModel || vehicle.model || model,
    variant: vehicle.variant || vehicle.variantName || "",
    city: vehicle.citySlug || vehicle.city || "new-delhi",
  };
};

const applyDirectPriceLookupOverride = ({ plan = {}, override = null } = {}) => {
  if (!override) return plan;

  const baseTool = plan.tools?.[0] || {};

  return {
    ...plan,
    intent: "vehicle_pricelist",
    conversationMode: "pricing",
    mode: plan.mode || "single_tool",
    tools: [
      {
        ...baseTool,
        tool: "vehicle_pricelist",
        input: {
          ...(baseTool.input || {}),
          model: override.model,
          make: override.make,
          fullModel: override.fullModel,
          variant: override.variant,
          city: override.city,
        },
        args: {
          ...(baseTool.args || {}),
          model: override.model,
          make: override.make,
          fullModel: override.fullModel,
          variant: override.variant,
          city: override.city,
        },
        params: {
          ...(baseTool.params || {}),
          model: override.model,
          make: override.make,
          fullModel: override.fullModel,
          variant: override.variant,
          city: override.city,
        },
        entities: {
          ...(baseTool.entities || {}),
          primaryModel: override.model,
          primaryMake: override.make,
          primaryVariant: override.variant,
        },
        filters: {
          ...(baseTool.filters || {}),
          city: override.city,
        },
      },
    ],
    output: {
      ...(plan.output || {}),
      canvasType: override.variant ? "price_breakup_canvas" : "pricelist_canvas",
      inlineType: "",
    },
    meta: {
      ...(plan.meta || {}),
      directPriceLookupOverride: true,
      directPriceLookupRoutingReason: override.routingReason,
    },
  };
};




const isExplicitScoreValueLookupRequest = (message = "", primaryTask = "") => {
  const raw = String(message || "");
  const task = cleanText(primaryTask);

  const hasScoreTask =
    task === "score_insight" ||
    task === "vehicle_score_insight" ||
    /\b(score|good|value|worth|value\s+for\s+money|good\s+value)\b/i.test(raw);

  if (!hasScoreTask) return false;

  const asksValue =
    /\b(value|worth|value\s+for\s+money|good\s+value)\b/i.test(raw);

  if (!asksValue) return false;

  const asksComparison =
    /\b(vs|v\/s|versus|compare|comparison|between|against|which\s+one|better)\b/i.test(raw);

  return !asksComparison;
};

const normalizeVariantIdentityText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const getScoreVariantCandidateFromSnapshot = (candidateSnapshot = {}) => {
  const variants = asArray(candidateSnapshot?.vehicles?.variants);
  const best = variants
    .map((item = {}) => {
      const raw = item.metadata?.raw || {};
      return {
        make: item.metadata?.make || raw.make || "",
        model: item.metadata?.model || raw.model || "",
        fullModel: raw.fullModel || [raw.make, raw.model].filter(Boolean).join(" "),
        variant: item.metadata?.variant || raw.variant || item.rawText || item.displayName || "",
        variantName: item.metadata?.variant || raw.variant || item.rawText || "",
        variantKey: raw.variantKey || item.canonicalKey || "",
        fullVariant: item.metadata?.fullVariant || raw.fullVariant || item.displayName || "",
        confidence: Number(item.confidence || raw.score || 0),
      };
    })
    .filter((item) => item.variant)
    .sort((left, right) => right.confidence - left.confidence)[0];

  return best || {};
};

const buildScoreValueLookupOverride = ({
  message = "",
  meaningFrame = {},
  contextState = {},
  context = {},
  candidateSnapshot = {},
} = {}) => {
  if (!isExplicitScoreValueLookupRequest(message, meaningFrame?.primaryTask)) return null;

  const contextVehicle = context?.selectedVehicle || {};
  const variantCandidate = getScoreVariantCandidateFromSnapshot(candidateSnapshot);
  const stateVehicle =
    contextState?.selectedVehicle ||
    contextState?.anchors?.primaryVehicle ||
    {};
  const vehicle = {
    ...variantCandidate,
    ...contextVehicle,
    ...stateVehicle,
    variant:
      stateVehicle.variant ||
      stateVehicle.variantName ||
      contextVehicle.variant ||
      contextVehicle.variantName ||
      variantCandidate.variant ||
      variantCandidate.variantName ||
      "",
    variantName:
      stateVehicle.variantName ||
      stateVehicle.variant ||
      contextVehicle.variantName ||
      contextVehicle.variant ||
      variantCandidate.variantName ||
      variantCandidate.variant ||
      "",
    variantKey:
      stateVehicle.variantKey ||
      contextVehicle.variantKey ||
      variantCandidate.variantKey ||
      "",
  };

  const model =
    vehicle.shortModelKey ||
    vehicle.modelKey ||
    vehicle.model ||
    vehicle.fullModel ||
    "";

  if (!model) return null;

  const variant =
    vehicle.variant ||
    vehicle.variantName ||
    "";

  const raw = String(message || "");
  const rawVariantToken = normalizeVariantIdentityText(variant);
  const messageContainsVariant =
    rawVariantToken &&
    normalizeVariantIdentityText(raw).includes(rawVariantToken);

  const hasExplicitVariant = Boolean(variant && messageContainsVariant);

  return {
    tool: "vehicle_score_insight",
    intent: "vehicle_score_insight",
    operation: hasExplicitVariant ? "variant_score_insight" : "same_family_value_insights",
    routingReason: hasExplicitVariant
      ? "variant_value_score_lookup"
      : "model_family_value_score_lookup",
    modelKey: vehicle.shortModelKey || vehicle.modelKey || vehicle.model || model,
    makeKey: vehicle.makeKey || vehicle.make || vehicle.brand || "",
    fullModel: vehicle.fullModel || vehicle.model || model,
    variantKey: hasExplicitVariant ? (vehicle.variantKey || variant) : "",
    variantName: hasExplicitVariant ? variant : "",
    fuelKey: vehicle.fuelKey || vehicle.fuelType || "",
    transmissionKey: vehicle.transmissionKey || vehicle.transmission || "",
  };
};

const applyScoreValueLookupOverride = ({ plan = {}, override = null } = {}) => {
  if (!override) return plan;

  const baseTool = plan.tools?.[0] || {};

  const payload = {
    operation: override.operation,
    modelKey: override.modelKey,
    makeKey: override.makeKey,
    fullModel: override.fullModel,
    ...(override.variantKey ? { variantKey: override.variantKey } : {}),
    ...(override.variantName ? { variant: override.variantName, variantName: override.variantName } : {}),
    ...(override.fuelKey ? { fuelKey: override.fuelKey } : {}),
    ...(override.transmissionKey ? { transmissionKey: override.transmissionKey } : {}),
  };

  return {
    ...plan,
    intent: "vehicle_score_insight",
    conversationMode: "diagnostic",
    mode: plan.mode || "single_tool",
    tools: [
      {
        ...baseTool,
        tool: "vehicle_score_insight",
        operation: override.operation,
        input: {
          ...(baseTool.input || {}),
          ...payload,
        },
        args: {
          ...(baseTool.args || {}),
          ...payload,
        },
        params: {
          ...(baseTool.params || {}),
          ...payload,
        },
        entities: {
          ...(baseTool.entities || {}),
          ...payload,
          primaryModel: override.modelKey,
          primaryMake: override.makeKey,
          primaryVariant: override.variantName || "",
        },
        filters: {
          ...(baseTool.filters || {}),
          ...(override.fuelKey ? { fuelKey: override.fuelKey } : {}),
          ...(override.transmissionKey ? { transmissionKey: override.transmissionKey } : {}),
        },
      },
    ],
    output: {
      ...(plan.output || {}),
      canvasType: "score_insight_canvas",
      inlineType: "score_insight_summary",
    },
    meta: {
      ...(plan.meta || {}),
      scoreValueLookupOverride: true,
      scoreValueLookupRoutingReason: override.routingReason,
    },
  };
};



export const runAciCoreLiveBridge = async ({
  message = "",
  context = {},
  user = null,
  session = null,
  meta = {},
} = {}) => {
  const startedAt = Date.now();
  const originalMessage = message;
  message = expandActiveComparisonFollowUpMessage({
    message,
    context,
  });
  const effectiveMessage = message;
  const unsupportedCityFastPath = maybeReturnUnsupportedCityFastPath({
    message,
    effectiveMessage,
    startedAt: typeof startedAt !== "undefined" ? startedAt : Date.now(),
  });

  if (unsupportedCityFastPath) {
    return unsupportedCityFastPath;
  }

  const supportedExactPriceFastPath = await maybeReturnSupportedExactPriceFastPath({
    message,
    effectiveMessage,
    context,
    originalMessage,
    startedAt,
  });

  if (supportedExactPriceFastPath) {
    return supportedExactPriceFastPath;
  }

  const rawMessage = String(message || "");
  const normalizedMessage = cleanText(rawMessage);
  const candidateSnapshot = await retrieveAciDbCandidates({
    rawMessage,
    normalizedMessage,
    activeContext: context,
  });

  const hydratedContext = await hydrateContextFromCandidates({
    message,
    candidateSnapshot,
    activeContext: context,
  });

  const managedCandidateSnapshot = hydratedContext.candidateSnapshot || candidateSnapshot;
  const {
    contextState,
    isolation,
  } = applyContextIsolationRules({
    message,
    contextState: hydratedContext.contextState,
    candidateSnapshot: managedCandidateSnapshot,
  });
  const isolatedContext = getContextForToolPlan(contextState);

  const understanding = await runAciUnderstandingEngine({
    message,
    activeContext: isolatedContext,
    candidateSnapshot: managedCandidateSnapshot,
    parser: parseHybridMeaningFrame,
  });

  const basePlan = buildLegacyPlanFromAciMeaningFrame({
    meaningFrame: understanding.meaningFrame,
    message,
    context: isolatedContext,
  });

  const crossModelScoreDiagnosticOverride = detectCrossModelScoreDiagnosticRequest({
    message,
    candidateSnapshot: managedCandidateSnapshot,
  });

  const directPriceLookupOverride = buildDirectPriceLookupOverride({
    message,
    meaningFrame: understanding.meaningFrame,
    contextState,
  });

  const scoreValueLookupOverride =
    !directPriceLookupOverride && !crossModelScoreDiagnosticOverride
      ? buildScoreValueLookupOverride({
          message,
          meaningFrame: understanding.meaningFrame,
          contextState,
          context,
          candidateSnapshot: managedCandidateSnapshot,
        })
      : null;

  const plan = applyDirectPriceLookupOverride({
    plan: applyScoreValueLookupOverride({
      plan: applyCrossModelScoreDiagnosticPlanOverride({
        plan: basePlan,
        override: crossModelScoreDiagnosticOverride,
      }),
      override: scoreValueLookupOverride,
    }),
    override: directPriceLookupOverride,
  });

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context: isolatedContext,
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context: isolatedContext,
  });

  const responseForCompose = crossModelScoreDiagnosticOverride
    ? {
        ...normalized,
        answer: ensureDiagnosticOnlyAnswerNote(normalized.answer),
        operation: "cross_model_score_diagnostic",
        data: {
          ...(normalized.data || {}),
          operation: "cross_model_score_diagnostic",
        },
      }
    : normalized;

  const managedContextPatch = buildContextPatchFromState(contextState);
  const bridgeTool = plan.tools?.[0]?.tool || "";
  const bridgePrimaryTask =
    bridgeTool === "vehicle_recommend" && isolation === "broad_discovery_without_model"
      ? "vehicle_discovery"
      : bridgeTool === "vehicle_score_insight"
        ? "score_insight"
        : bridgeTool === "vehicle_similar"
          ? "similar_cars"
          : understanding.meaningFrame?.primaryTask || "";

  const bridge = {
    enabled: true,
    durationMs: Date.now() - startedAt,
    selectedParser: understanding.selectedParser || "",
    usedGemini: Boolean(understanding.usedGemini),
    primaryTask: bridgePrimaryTask,
    tool: bridgeTool,
    planMode: plan.mode || "",
    contextIsolation: isolation,
    originalMessage,
    effectiveMessage,
    ...(crossModelScoreDiagnosticOverride
      ? {
          operation: "cross_model_score_diagnostic",
          routingReason: crossModelScoreDiagnosticOverride.routingReason,
          crossModelScoreDiagnosticTargets: crossModelScoreDiagnosticOverride.targets,
        }
      : {}),
    ...(directPriceLookupOverride
      ? {
          routingReason: directPriceLookupOverride.routingReason,
          directTaskOverride: "vehicle_pricelist",
        }
      : {}),
    ...(scoreValueLookupOverride
      ? {
          operation: scoreValueLookupOverride.operation,
          routingReason: scoreValueLookupOverride.routingReason,
          directTaskOverride: "vehicle_score_insight",
        }
      : {}),
  };

  const scoreInsightGuardrail =
    bridgeTool === "vehicle_score_insight"
      ? {
          canUseForFinalRecommendation: false,
          finalRecommendationEnabled: false,
          reason:
            "These are diagnostic module scores only. Final recommendation needs buyer-context weighting, similar-cars graph, upgrade ladder, service/resale evidence and recommendation policy.",
        }
      : null;

  return composeAciAnswer({
    ...responseForCompose,
    contextPatch: mergeContextPatches({
      previousPatch: context,
      managerPatch: managedContextPatch,
      toolPatch: responseForCompose.contextPatch || {},
    }),
    ...(scoreInsightGuardrail ? { usageGuardrail: scoreInsightGuardrail } : {}),
    aciCoreBridge: bridge,
    meta: {
      ...(responseForCompose.meta || {}),
      ...(scoreInsightGuardrail ? { scoreInsightGuardrail } : {}),
      aciCoreBridge: bridge,
    },
  });
};

export {
  isAciCoreLiveBridgeEnabled,
  shouldUseAciCoreLiveBridge,
};
