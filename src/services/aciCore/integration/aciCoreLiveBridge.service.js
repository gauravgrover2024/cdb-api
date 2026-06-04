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
    [];

  if (!Array.isArray(vehicles) || vehicles.length < 2) return false;

  return /\b(which one|which is better|better|which should i|should i buy|choose|pick|recommend|verdict|final choice)\b/i.test(
    message,
  );
};

const expandActiveComparisonFollowUpMessage = ({ message = "", context = {} } = {}) => {
  if (!hasActiveComparisonFollowUp({ message, context })) return message;

  const activeComparison =
    context?.activeComparison ||
    context?.selectedComparisonSet ||
    {};

  const vehicles =
    activeComparison?.vehicles ||
    context?.selectedComparisonSet?.vehicles ||
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
  truthy(process.env.ACI_CORE_LIVE_BRIDGE_ENABLED);

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
    answer: `I don't have live on-road pricing for ${requestedCity} yet. Pricing is currently available for Delhi, Noida, and Gurgaon.`,
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
  if (!hasSupportedCityMention(text)) return null;

  const citySlug = getCitySlugFromMessage(text);
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
  const {
    context: isolatedContext,
    isolation,
  } = isolateAciCoreBridgeContext({
    message,
    context,
    candidateSnapshot,
  });

  const understanding = await runAciUnderstandingEngine({
    message,
    activeContext: isolatedContext,
    candidateSnapshot,
    parser: parseHybridMeaningFrame,
  });

  const plan = buildLegacyPlanFromAciMeaningFrame({
    meaningFrame: understanding.meaningFrame,
    message,
    context: isolatedContext,
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
  const bridgeTool = plan.tools?.[0]?.tool || "";
  const bridgePrimaryTask =
    bridgeTool === "vehicle_recommend" && isolation === "broad_discovery_without_model"
      ? "vehicle_discovery"
      : bridgeTool === "vehicle_score_insight"
        ? "score_insight"
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
    ...normalized,
    ...(scoreInsightGuardrail ? { usageGuardrail: scoreInsightGuardrail } : {}),
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      ...(scoreInsightGuardrail ? { scoreInsightGuardrail } : {}),
      aciCoreBridge: bridge,
    },
  });
};

export {
  isAciCoreLiveBridgeEnabled,
  shouldUseAciCoreLiveBridge,
};
