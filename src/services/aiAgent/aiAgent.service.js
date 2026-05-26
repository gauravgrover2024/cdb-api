import * as PlannerModule from "./aiAgent.planner.js";
import { normalizeAciFinalResponse } from "./aiAgent.contractNormalizer.js";

import { runVehicleFeaturesTool } from "./tools/newCars/vehicleFeatures.tool.js";
import { resolveVehicleModelFromText } from "./aiAgent.vehicleModelResolver.js";
import mongoose from "mongoose";
import { runVehiclePricelistNewCarsTool } from "./tools/newCars/vehiclePricelist.tool.js";
import {
  makeUnavailablePlan,
  sanitizePlannerPlan,
  validatePlannerPlan,
} from "./aiAgent.planSchema.js";

import {
  executeAciPlannerPlan,
  EXECUTOR_VERSION,
} from "./aiAgent.executor.js";

/**
 * ACI Assist V2 Service
 *
 * This service intentionally does NOT use:
 *
 * Flow:
 * user message
 * -> aiAgent.planner.js
 * -> aiAgent.executor.js
 * -> aiAgent.responseTools.js
 * -> aiAgent.responseSanitizer.js
 * -> stable frontend contract
 */

/* -------------------------------------------------------------------------- */
/*  Constants                                                                 */
/* -------------------------------------------------------------------------- */

export const ACI_ASSIST_SERVICE_VERSION = "aci-assist-v2-service";

export const DEFAULT_CONTEXT = Object.freeze({
  selectedVehicle: {},
  selectedComparisonSet: {},
  userPreferences: {},
  leadContext: {},
});

const PLANNER_FUNCTION_CANDIDATES = [
  "planAciAssistMessage",
  "createAciPlannerPlan",
  "createPlannerPlan",
  "buildAciPlannerPlan",
  "buildPlannerPlan",
  "generateAciPlannerPlan",
  "generatePlannerPlan",
  "runAciPlanner",
  "runPlanner",
  "planMessage",
  "planner",
  "default",
];

/* -------------------------------------------------------------------------- */
/*  Generic Helpers                                                           */
/* -------------------------------------------------------------------------- */

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const isPlainObject = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  !(value instanceof Date);

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const hasPlannerTools = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  Array.isArray(value.tools) &&
  value.tools.length > 0;

const safeObject = (value) => (isPlainObject(value) ? value : {});

const mergeContext = (...contexts) => {
  const output = {
    ...DEFAULT_CONTEXT,
  };

  for (const context of contexts) {
    if (!isPlainObject(context)) continue;

    Object.assign(output, context);

    if (context.selectedVehicle) {
      output.selectedVehicle = {
        ...(output.selectedVehicle || {}),
        ...context.selectedVehicle,
      };
    }

    if (context.selectedComparisonSet) {
      output.selectedComparisonSet = {
        ...(output.selectedComparisonSet || {}),
        ...context.selectedComparisonSet,
      };
    }

    if (context.userPreferences) {
      output.userPreferences = {
        ...(output.userPreferences || {}),
        ...context.userPreferences,
      };
    }

    if (context.leadContext) {
      output.leadContext = {
        ...(output.leadContext || {}),
        ...context.leadContext,
      };
    }
  }

  return output;
};

export const normalizeChatInput = (...args) => {
  const firstArg = args[0];
  const secondArg = args[1];

  if (isPlainObject(firstArg)) {
    const message = cleanText(
      firstArg.message ||
        firstArg.query ||
        firstArg.userMessage ||
        firstArg.prompt ||
        firstArg.text ||
        "",
    );

    const context = mergeContext(
      firstArg.context,
      firstArg.conversationContext,
      firstArg.state,
      firstArg.session,
      secondArg,
    );

    return {
      message,
      context,
      history: asArray(firstArg.history || firstArg.messages),
      options: safeObject(firstArg.options),
      rawInput: firstArg,
    };
  }

  return {
    message: cleanText(firstArg || ""),
    context: mergeContext(secondArg),
    history: [],
    options: {},
    rawInput: {
      message: cleanText(firstArg || ""),
      context: safeObject(secondArg),
    },
  };
};

const extractPlannerPlan = (plannerResult) => {
  if (hasPlannerTools(plannerResult)) return plannerResult;

  const candidates = [
    plannerResult?.plan,
    plannerResult?.plannerPlan,
    plannerResult?.aiPlan,
    plannerResult?.aciPlan,
    plannerResult?.data?.plan,
    plannerResult?.result?.plan,
    plannerResult?.planner?.plan,
    plannerResult?.validation?.plan,
  ];

  for (const candidate of candidates) {
    if (hasPlannerTools(candidate)) return candidate;
  }

  return null;
};

const getPlannerFunction = () => {
  for (const name of PLANNER_FUNCTION_CANDIDATES) {
    const candidate = PlannerModule[name];

    if (typeof candidate === "function") {
      return {
        name,
        fn: candidate,
      };
    }
  }

  return {
    name: "",
    fn: null,
  };
};

const tryPlannerCall = async ({
  plannerFn,
  callShape,
  message,
  context,
  history,
  options,
}) => {
  if (callShape === "object") {
    return plannerFn({
      message,
      userMessage: message,
      query: message,
      prompt: message,
      context,
      conversationContext: context,
      history,
      messages: history,
      options,
    });
  }

  if (callShape === "positional") {
    return plannerFn(message, context, {
      history,
      options,
    });
  }

  if (callShape === "message-only") {
    return plannerFn(message);
  }

  return null;
};

export const createUnavailableFallbackPlan = ({
  message = "",
  reason = "unsupported_request",
  details = "",
} = {}) =>
  makeUnavailablePlan({
    reason,
    message:
      details ||
      `ACI Assist could not create a valid planner plan for: ${message}`,
    confidence: 0,
  });

export const createPlannerPlanForMessage = async ({
  message = "",
  context = {},
  history = [],
  options = {},
} = {}) => {
  const { name, fn } = getPlannerFunction();

  if (!fn) {
    return {
      plan: createUnavailableFallbackPlan({
        message,
        details:
          "No planner function export was found in aiAgent.planner.js. Expected one of: " +
          PLANNER_FUNCTION_CANDIDATES.join(", "),
      }),
      plannerMeta: {
        plannerExport: "",
        fallbackUsed: true,
        error: "planner_function_not_found",
      },
    };
  }

  const callShapes = ["object", "positional", "message-only"];
  const errors = [];

  for (const callShape of callShapes) {
    try {
      const plannerResult = await tryPlannerCall({
        plannerFn: fn,
        callShape,
        message,
        context,
        history,
        options,
      });

      const extractedPlan = extractPlannerPlan(plannerResult);

      if (extractedPlan) {
        const sanitizedPlan = sanitizePlannerPlan(extractedPlan, {
          message,
        });

        const validation = validatePlannerPlan(sanitizedPlan, {
          message,
        });

        return {
          plan: validation.plan || sanitizedPlan,
          rawPlannerResult: plannerResult,
          plannerMeta: {
            plannerExport: name,
            callShape,
            fallbackUsed: false,
            validation: {
              valid: validation.valid,
              errors: validation.errors || [],
              warnings: validation.warnings || [],
            },
          },
        };
      }

      errors.push({
        callShape,
        error: "planner_returned_no_tools",
      });
    } catch (error) {
      errors.push({
        callShape,
        error: error?.message || String(error),
      });
    }
  }

  return {
    plan: createUnavailableFallbackPlan({
      message,
      details:
        "Planner function was found but did not return a valid plan with tools[].",
    }),
    plannerMeta: {
      plannerExport: name,
      fallbackUsed: true,
      error: "planner_invalid_output",
      attempts: errors,
    },
  };
};

/* -------------------------------------------------------------------------- */
/*  Main V2 Chat Function                                                     */
/* -------------------------------------------------------------------------- */


const ACI_EARLY_FEATURE_MODELS = [
  "creta", "verna", "thar", "seltos", "sonet", "venue", "exter",
  "alcazar", "city", "elevate", "nexon", "harrier", "safari",
  "punch", "scorpio", "xuv700", "xuv 700", "xuv7xo", "xuv 7xo",
  "xuv300", "xuv 300", "xuv3xo", "xuv 3xo", "slavia", "virtus",
  "taigun", "kushaq", "brezza", "fronx", "swift", "dzire",
  "baleno", "fortuner", "innova",
];

const ACI_EARLY_FEATURE_ALIASES = [
  {
    feature: "ARAI Mileage",
    pattern: /\b(mileage|fuel\s*efficiency|average|kitna\s*deti|kitna\s*deti\s*hai|kmpl|kpl|arai\s*mileage)\b/i,
  },
  { feature: "Integrated 2DIN Audio", pattern: /\b(music\s*system|audio\s*system|sound\s*system|stereo|car\s*stereo|speaker\s*system)\b/i },
  { feature: "Speakers", pattern: /\b(speakers?|bose\s*speakers?|premium\s*speakers?)\b/i },
  { feature: "Touchscreen", pattern: /\b(infotainment\s*system|infotainment|touch\s*screen|touchscreen|music\s*display|display\s*audio)\b/i },
  { feature: "Android Auto", pattern: /\b(android\s*auto|android\s*connect|phone\s*projection)\b/i },
  { feature: "Apple CarPlay", pattern: /\b(apple\s*car\s*play|apple\s*carplay|carplay|iphone\s*carplay)\b/i },
  { feature: "Sunroof", pattern: /\b(sunroof|panoramic\s*sunroof|single\s*pane\s*sunroof)\b/i },
  { feature: "ADAS", pattern: /\b(adas|advanced\s*driver|driver\s*assist)\b/i },
  { feature: "6 Airbags", pattern: /\b(6\s*airbags?|six\s*airbags?|airbags?)\b/i },
  { feature: "Rear Camera", pattern: /\b(rear\s*camera|reverse\s*camera|parking\s*camera|rear\s*view\s*camera)\b/i },
  { feature: "360 Camera", pattern: /\b(360\s*camera|360\s*degree\s*camera|360\s*view\s*camera)\b/i },
  { feature: "Ventilated Seats", pattern: /\b(ventilated\s*seats?|seat\s*ventilation)\b/i },
  { feature: "Wireless Charging", pattern: /\b(wireless\s*charger|wireless\s*charging|phone\s*charging)\b/i },
  { feature: "Cruise Control", pattern: /\b(cruise\s*control|adaptive\s*cruise)\b/i },
  { feature: "Alloy Wheels", pattern: /\b(alloy\s*wheels?|alloys?)\b/i },
  { feature: "Rear AC Vents", pattern: /\b(rear\s*ac\s*vents?|rear\s*vents?|rear\s*blower)\b/i },
  { feature: "TPMS", pattern: /\b(tpms|tyre\s*pressure|tire\s*pressure)\b/i },

  {
    feature: "LED Headlamps",
    pattern: /\b(led\s*headlamps?|led\s*headlights?|headlamps?|headlights?|projector\s*headlamps?)\b/i,
  },
  {
    feature: "Automatic Climate Control",
    pattern: /\b(automatic\s*climate\s*control|climate\s*control|auto\s*ac|automatic\s*ac)\b/i,
  },
  {
    feature: "Hill Hold",
    pattern: /\b(hill\s*hold|hill\s*assist|hill\s*start\s*assist)\b/i,
  },

  {
    feature: "ABS",
    pattern: /\b(abs|ags|anti\s*lock\s*braking|anti-lock\s*braking|anti\s*lock\s*braking\s*system|anti-lock\s*braking\s*system|braking\s*system)\b/i,
  },
];

const toAciTitleCaseModel = (model = "") => {
  const normalized = String(model || "").trim().replace(/-/g, " ").toLowerCase();
  if (["xuv700", "xuv 700", "xuv7xo", "xuv 7xo"].includes(normalized)) return "XUV 7XO";
  if (["xuv300", "xuv 300", "xuv3xo", "xuv 3xo"].includes(normalized)) return "XUV 3XO";
  if (normalized === "thar roxx") return "Thar Roxx";
  if (normalized === "scorpio n") return "Scorpio N";
  if (normalized === "i20" || normalized === "i 20") return "i20";
  return normalized
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
};

const detectAciEarlyFeatureRequest = (message = "") => {
  const raw = String(message || "").trim();
  if (!raw) return null;

  const model = ACI_EARLY_FEATURE_MODELS.find((name) => {
    const safe = name.replace(/\s+/g, "\\s*");
    return new RegExp(`\\b${safe}\\b`, "i").test(raw);
  });

  if (!model) return null;

  const alias = ACI_EARLY_FEATURE_ALIASES.find((entry) => entry.pattern.test(raw));
  if (!alias?.feature) return null;

  const isDiscovery =
    /\b(which|show|find|list)\b.*\b(variants?|cars?)\b/i.test(raw) ||
    /\bavailable\b.*\b(which|variant|variants)\b/i.test(raw) ||
    /\b(cheapest|most affordable|lowest price|without|miss|missing|do not have|dont have|don't have)\b/i.test(raw);

  return {
    model: toAciTitleCaseModel(model),
    feature: alias.feature,
    intent: isDiscovery ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    canvasType: isDiscovery ? "feature_match_builder_canvas" : "",
  };
};


const extractAciEarlyComparisonVariants = ({ message = "", model = "" } = {}) => {
  const raw = String(message || "").trim();
  if (!raw || !model) return [];

  const modelPattern = /xuv\s*(?:700|7xo)/i.test(model.toLowerCase())
    ? /xuv\s*(?:700|7xo)/i
    : /xuv\s*(?:300|3xo)/i.test(model.toLowerCase())
      ? /xuv\s*(?:300|3xo)/i
    : new RegExp(`\\b${model.replace(/\\s+/g, "\\\\s*")}\\b`, "i");

  const modelMatch = raw.match(modelPattern);
  if (!modelMatch) return [];

  let tail = raw.slice((modelMatch.index || 0) + modelMatch[0].length);

  tail = tail
    .replace(/\b(difference|different|between|in|of|features?|feature|compare|comparison|variant|variants|what|extra|do|i|get|show|tell|me|please)\b/gi, " ")
    .replace(/[?,.]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!tail) return [];

  const parts = tail
    .split(/\s+(?:and|vs|versus|v\/s|against|over|to|with)\s+/i)
    .map((part) =>
      part
        .replace(/\b(features?|variant|variants|difference|compare|comparison)\b/gi, " ")
        .replace(/\s+/g, " ")
        .trim(),
    )
    .filter(Boolean)
    .slice(0, 3);

  return parts.length >= 2 ? parts : [];
};

const detectAciEarlyVariantComparisonRequest = (message = "") => {
  const raw = String(message || "").trim();
  if (!raw) return null;

  const hasComparisonLanguage =
    /\b(difference|different|compare|comparison|vs|versus|v\/s|extra\s+features?|upgrade)\b/i.test(raw);

  if (!hasComparisonLanguage) return null;

  const model = ACI_EARLY_FEATURE_MODELS.find((name) => {
    const safe = name.replace(/\s+/g, "\\s*");
    return new RegExp(`\\b${safe}\\b`, "i").test(raw);
  });

  if (!model) return null;

  const variants = extractAciEarlyComparisonVariants({
    message: raw,
    model,
  });

  if (variants.length < 2) return null;

  return {
    model: toAciTitleCaseModel(model),
    variants,
    feature: "",
    intent: "vehicle_feature_comparison",
    canvasType: "comparison_canvas",
  };
};



const getAciAgentMongoDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};


const normalizeAciContextText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const stripAciContextMake = (value = "", make = "") => {
  const raw = String(value || "").trim();
  const brand = String(make || "").trim();

  if (!raw || !brand) return raw;

  const rawNorm = normalizeAciContextText(raw);
  const brandNorm = normalizeAciContextText(brand);

  if (rawNorm.startsWith(`${brandNorm} `)) {
    const brandWordCount = brandNorm.split(" ").filter(Boolean).length;
    return raw.split(/\s+/).slice(brandWordCount).join(" ").trim() || raw;
  }

  return raw;
};

const titleAciContextName = (value = "") =>
  String(value || "")
    .trim()
    .split(/\s+/)
    .map((word) => {
      if (/^ivt$/i.test(word)) return "iVT";
      if (/^(dct|amt|at|mt|cvt)$/i.test(word)) return word.toUpperCase();
      if (/^sx$/i.test(word)) return "SX";
      if (/^htx$/i.test(word)) return "HTX";
      if (/^abs$/i.test(word)) return "ABS";
      if (/^[A-Z0-9()]+$/.test(word)) return word;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");


const isAciGenericModelEntityMatch = (entity = {}) => {
  const matched = normalizeAciContextText(entity?.matchedText || "");

  if (!matched) return false;

  const genericMatches = new Set([
    "best",
    "better",
    "top",
    "highest",
    "maximum",
    "max",
    "most",
    "mileage",
    "average",
    "fuel",
    "efficiency",
    "kmpl",
    "kpl",
    "variant",
    "variants",
    "feature",
    "features",
    "worth",
    "upgrade",
    "buy",
    "family",
    "rear",
    "seat",
    "night",
    "driving",
  ]);

  return genericMatches.has(matched);
};

const hasAciComparisonLanguage = (message = "") =>
  /\b(difference|different|compare|comparison|vs|versus|v\/s|extra\s+features?|upgrade)\b/i.test(
    String(message || ""),
  );

const isAciLikelyVariantTokenModelMatch = ({
  message = "",
  textEntity = null,
  contextEntity = null,
} = {}) => {
  if (!textEntity || !contextEntity) return false;
  if (!hasAciComparisonLanguage(message)) return false;

  const matched = normalizeAciContextText(textEntity.matchedText || "");
  if (!matched) return false;

  const weakVariantTokens = new Set([
    "e",
    "ex",
    "s",
    "sx",
    "vx",
    "zx",
    "v",
    "z",
    "ht",
    "htk",
    "htx",
    "gtx",
    "ax",
    "lx",
    "mx",
  ]);

  return matched.length <= 3 && weakVariantTokens.has(matched);
};

const chooseAciDynamicModelEntity = ({
  textEntity = null,
  contextEntity = null,
  message = "",
} = {}) => {
  if (!textEntity) return contextEntity;
  if (!contextEntity) return textEntity;

  if (isAciLikelyVariantTokenModelMatch({ message, textEntity, contextEntity })) {
    return contextEntity;
  }

  if (isAciGenericModelEntityMatch(textEntity)) {
    return contextEntity;
  }

  return textEntity;
};


const buildAciContextModelEntity = ({ context = {}, selectedEntity = null } = {}) => {
  const selectedVehicle =
    context?.selectedVehicle ||
    selectedEntity?.selectedVehicle ||
    selectedEntity?.vehicle ||
    selectedEntity ||
    {};

  const make =
    context?.anchorMake ||
    context?.make ||
    selectedVehicle?.make ||
    selectedVehicle?.brand ||
    "";

  const rawModel =
    context?.anchorModel ||
    context?.model ||
    selectedVehicle?.model ||
    selectedVehicle?.name ||
    "";

  const rawFullModel =
    context?.anchorFullModel ||
    selectedVehicle?.fullModel ||
    selectedVehicle?.fullName ||
    (make && rawModel ? `${make} ${stripAciContextMake(rawModel, make)}` : rawModel);

  const model = titleAciContextName(stripAciContextMake(rawModel, make));
  const fullModel = titleAciContextName(rawFullModel);
  const brand = titleAciContextName(make);

  if (!model) return null;

  return {
    brand,
    model,
    fullModel: fullModel || (brand ? `${brand} ${model}` : model),
    matchedText: "",
    confidence: 1,
    method: "context_anchor",
    fromContext: true,
  };
};


const resolveAciDynamicModelEntity = async (message = "") => {
  try {
    const db = getAciAgentMongoDb();
    if (!db) return null;

    return await resolveVehicleModelFromText({
      db,
      message,
    });
  } catch {
    return null;
  }
};

const detectAciKnownModelEntityFromMessage = (message = "") => {
  const raw = String(message || "").trim();
  if (!raw) return null;

  const matched = ACI_EARLY_FEATURE_MODELS.find((name) => {
    const safe = name.replace(/\s+/g, "\\s*");
    return new RegExp(`\\b${safe}\\b`, "i").test(raw);
  });

  if (!matched) return null;

  // Avoid treating generic location wording as Honda City unless it is clearly a car request.
  if (
    normalizeAciContextText(matched) === "city" &&
    !/\bhonda\s+city\b/i.test(raw) &&
    !/\b(city)\b.*\b(price|pricelist|emi|colors?|colours?|features?|sunroof|compare|vs)\b/i.test(raw) &&
    !/\b(price|pricelist|emi|colors?|colours?|features?|sunroof|compare|vs)\b.*\b(city)\b/i.test(raw)
  ) {
    return null;
  }

  const model = toAciTitleCaseModel(matched);

  return {
    brand: "",
    make: "",
    model,
    fullModel: model,
    matchedText: matched,
    confidence: 0.98,
    method: "known_model_message_fallback",
    fromMessage: true,
  };
};

const hydrateAciExplicitModelEntityFromReadModel = async (entity = {}) => {
  if (!entity?.model) return entity;

  try {
    const db = getAciAgentMongoDb();
    if (!db) return entity;

    const modelText = cleanText(entity.model || "");
    const fullText = cleanText(entity.fullModel || "");
    const brandText = cleanText(entity.make || entity.brand || "");
    const modelKey = normalizeAciContextText(modelText).replace(/\s+/g, "-");

    if (!modelKey && !modelText) return entity;

    const escapedModel = modelText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const escapedFull = fullText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    const or = [
      modelKey ? { modelKey } : null,
      modelText ? { model: new RegExp(`^\\s*${escapedModel}\\s*$`, "i") } : null,
      modelText ? { displayName: new RegExp(`\\b${escapedModel}\\b`, "i") } : null,
      modelText ? { fullModel: new RegExp(`\\b${escapedModel}\\b`, "i") } : null,
      fullText ? { displayName: new RegExp(`^\\s*${escapedFull}\\s*$`, "i") } : null,
      fullText ? { fullModel: new RegExp(`^\\s*${escapedFull}\\s*$`, "i") } : null,
    ].filter(Boolean);

    const candidates = await db.collection("aci_vehicle_model_summary")
      .find(
        {
          citySlug: "new-delhi",
          $or: or,
        },
        {
          projection: {
            make: 1,
            makeKey: 1,
            model: 1,
            modelKey: 1,
            fullModel: 1,
            displayName: 1,
            variantCount: 1,
          },
        },
      )
      .limit(20)
      .toArray();

    if (!candidates.length) return entity;

    const normalizedWanted = normalizeAciContextText(modelText);
    const normalizedBrand = normalizeAciContextText(brandText);

    const scoreCandidate = (row = {}) => {
      const rowModel = normalizeAciContextText(row.model || "");
      const rowFull = normalizeAciContextText(row.fullModel || row.displayName || "");
      const rowMake = normalizeAciContextText(row.make || "");
      let score = 0;

      if (rowModel === normalizedWanted) score += 100;
      if (rowFull === normalizedWanted) score += 90;
      if (rowFull.endsWith(` ${normalizedWanted}`)) score += 75;
      if (rowFull.includes(normalizedWanted)) score += 50;
      if (normalizedBrand && rowMake === normalizedBrand) score += 25;
      if (Number(row.variantCount || 0) > 0) score += 5;

      return score;
    };

    const summary = [...candidates].sort((a, b) => scoreCandidate(b) - scoreCandidate(a))[0];

    if (!summary?.model) return entity;

    const make = cleanText(summary.make || "");
    const model = cleanText(summary.model || entity.model || "");
    const fullModel = cleanText(
      summary.fullModel ||
        summary.displayName ||
        (make && model ? `${make} ${model}` : model),
    );

    return {
      ...entity,
      make,
      brand: make,
      model,
      fullModel,
      makeKey: summary.makeKey || "",
      modelKey: summary.modelKey || modelKey,
      fromReadModelSummary: true,
    };
  } catch {
    return entity;
  }
};

const resolveAciExplicitMessageModelEntity = async (message = "") => {
  const resolved = await resolveAciDynamicModelEntity(message);

  if (resolved?.model && !isAciGenericModelEntityMatch(resolved)) {
    return hydrateAciExplicitModelEntityFromReadModel({
      ...resolved,
      fromMessage: true,
    });
  }

  const knownFallback = detectAciKnownModelEntityFromMessage(message);
  return hydrateAciExplicitModelEntityFromReadModel(knownFallback);
};

const ACI_DYNAMIC_CONNECTED_FEATURE_ALIAS = {
  feature: "Connected Features",
  pattern: /\b(connected\s*car|connected\s*features|connected\s*tech|connected\s*services|bluelink|blue\s*link)\b/i,
};

const getAciDynamicFeatureAlias = (message = "") => {
  const raw = String(message || "");

  const aliases = [
    ...(Array.isArray(ACI_EARLY_FEATURE_ALIASES) ? ACI_EARLY_FEATURE_ALIASES : []),
    ACI_DYNAMIC_CONNECTED_FEATURE_ALIAS,
  ].filter((entry) => entry?.feature && entry?.pattern);

  return aliases.find((entry) => entry.pattern.test(raw)) || null;
};


const buildAciDynamicFeatureCleanUserMessage = ({
  raw = "",
  model = "",
  modelEntity = null,
  feature = "",
  alias = null,
} = {}) => {
  let tail = String(raw || "").trim();
  const xuvAliasMatch = tail.match(/\bxuv\s*(?:700|7xo|300|3xo)\b/i);

  const modelCandidates = [
    xuvAliasMatch?.[0],
    modelEntity?.matchedText,
    modelEntity?.fullModel,
    modelEntity?.model,
    model,
  ]
    .filter(Boolean)
    .sort((a, b) => String(b).length - String(a).length);

  for (const candidate of modelCandidates) {
    const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    tail = tail.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
  }

  if (alias?.pattern) {
    tail = tail.replace(alias.pattern, " ");
  }

  tail = tail
    .replace(/\b(does|do|is|are|has|have|having|come|comes|with|get|gets|got|available|check|tell|show|please|which|what|who|where|best|better|top|highest|maximum|max|most|gives|give|for|about|should|would|could|can|it|this|that)\b/gi, " ")
    .replace(/\b(me|mein|mai|hai|kya|in|of|the|a|an|variant|variants|car|cars|feature|features|mileage|petrol)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  // The feature is already passed explicitly in toolPlan.entities.feature.
  // Do NOT append it to userMessage, otherwise the feature word can be misread as a variant.
  //
  // "Does seltos has abs in htx ivt" -> "Seltos htx ivt"
  // "Does seltos has abs"            -> "Seltos"
  if (tail) {
    return `${model} ${tail}`.replace(/\s+/g, " ").trim();
  }

  return `${model}`.replace(/\s+/g, " ").trim();
};


const extractAciDynamicComparisonVariants = ({ message = "", modelEntity = null } = {}) => {
  const raw = String(message || "").trim();
  if (!raw || !modelEntity?.model) return [];

  let tail = raw;

  const mention = String(modelEntity.matchedText || "").trim();
  if (mention) {
    const idx = raw.toLowerCase().indexOf(mention.toLowerCase());
    if (idx >= 0) {
      tail = raw.slice(idx + mention.length);
    }
  } else {
    const modelWords = [
      modelEntity.fullModel,
      modelEntity.model,
    ].filter(Boolean);

    for (const candidate of modelWords) {
      const idx = raw.toLowerCase().indexOf(String(candidate).toLowerCase());
      if (idx >= 0) {
        tail = raw.slice(idx + String(candidate).length);
        break;
      }
    }
  }

  tail = tail
    .replace(/\b(difference|different|between|in|of|features?|feature|compare|comparison|variant|variants|what|extra|do|i|get|show|tell|me|please)\b/gi, " ")
    .replace(/[?,.]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!tail) return [];

  return tail
    .split(/\s+(?:and|vs|versus|v\/s|against|over|to|with)\s+/i)
    .map((part) =>
      part
        .replace(/\b(features?|variant|variants|difference|compare|comparison)\b/gi, " ")
        .replace(/\s+/g, " ")
        .trim(),
    )
    .filter(Boolean)
    .slice(0, 3);
};


const ACI_DYNAMIC_FEATURE_CATEGORY_MAP = [
  {
    key: "safety",
    label: "Safety",
    pattern: /\b(safety|airbags?|abs|esc|hill\s*hold|tpms|camera|parking\s*sensors?|adas)\b/i,
  },
  {
    key: "comfort",
    label: "Comfort",
    pattern: /\b(comfort|convenience|seat|seats|ventilated|climate|ac|cruise|armrest|keyless|push\s*button)\b/i,
  },
  {
    key: "infotainment",
    label: "Infotainment",
    pattern: /\b(infotainment|music|audio|speakers?|touchscreen|android\s*auto|apple\s*carplay|carplay|bluetooth|radio)\b/i,
  },
  {
    key: "exterior",
    label: "Exterior",
    pattern: /\b(exterior|sunroof|headlamps?|tail\s*lamps?|drl|alloy|wheels?|tyres?|roof\s*rails?)\b/i,
  },
  {
    key: "interior",
    label: "Interior",
    pattern: /\b(interior|dashboard|upholstery|cabin|cluster|steering|ambient)\b/i,
  },
  {
    key: "engine",
    label: "Engine",
    pattern: /\b(engine|power|torque|fuel|transmission|gearbox|mileage|performance)\b/i,
  },
  {
    key: "dimensions",
    label: "Dimensions",
    pattern: /\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase|seating|fuel\s*tank)\b/i,
  },
  {
    key: "connected",
    label: "Connected Car",
    pattern: /\b(connected|bluelink|blue\s*link|connected\s*car|connected\s*features|connected\s*services)\b/i,
  },
  {
    key: "adas",
    label: "ADAS",
    pattern: /\b(adas|driver\s*assist|lane\s*keep|blind\s*spot|adaptive\s*cruise|collision)\b/i,
  },
];

const detectAciDynamicFeatureCategory = (message = "") => {
  const raw = String(message || "");
  return ACI_DYNAMIC_FEATURE_CATEGORY_MAP.find((entry) =>
    entry.pattern.test(raw),
  ) || null;
};

const hasAciTestDriveIntent = (message = "") =>
  /\b(test\s*drive|book\s*test|schedule\s*test|trial\s*drive|drive\s+the\s+car)\b/i.test(
    String(message || ""),
  );


const detectAciEarlyDynamicRoutedRequest = ({ message = "", modelEntity = null } = {}) => {
  const raw = String(message || "").trim();
  if (!raw || !modelEntity?.model) return null;
  if (hasAciTestDriveIntent(raw)) return null;

  const xuvAliasMatch = raw.match(/\bxuv\s*(?:700|7xo|300|3xo)\b/i);
  let model = toAciTitleCaseModel(modelEntity.model);
  let brand = modelEntity.brand || "";
  let fullModel = modelEntity.fullModel
    ? `${brand ? `${brand} ` : ""}${model}`.trim()
    : "";

  if (xuvAliasMatch) {
    brand = brand || "Mahindra";
    if (/7|700/i.test(xuvAliasMatch[0])) {
      model = "XUV 7XO";
      fullModel = "Mahindra XUV 7XO";
    } else {
      model = "XUV 3XO";
      fullModel = "Mahindra XUV 3XO";
    }
  }

  const categoryMatch = detectAciDynamicFeatureCategory(raw);

  const isCategoryFeatureExplorerRequest =
    /\b(featuers|features|feature\s*list)\b/i.test(raw) &&
    categoryMatch?.key &&
    categoryMatch.key !== "connected";

  if (isCategoryFeatureExplorerRequest) {
    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: "",
      category: categoryMatch.key,
      categoryLabel: categoryMatch.label || categoryMatch.key,
      cleanUserMessage: `${model} ${categoryMatch.label || categoryMatch.key} features`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
    };
  }


  if (/\b(price\s*list|pricelist|price|on\s*road|on-road)\b/i.test(raw)) {
    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: "",
      cleanUserMessage: `${model} price`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
    };
  }

  const hasComparisonLanguage =
    /\b(difference|different|compare|comparison|vs|versus|v\/s|extra\s+features?|upgrade)\b/i.test(raw);

  if (hasComparisonLanguage) {
    const variants = extractAciDynamicComparisonVariants({
      message: raw,
      modelEntity,
    });

    if (variants.length >= 2) {
      return {
        model,
        make: brand,
        brand,
        fullModel,
        variants,
        feature: "",
        cleanUserMessage: `${model} ${variants.join(" vs ")}`,
        intent: "vehicle_feature_comparison",
        canvasType: "comparison_canvas",
      };
    }
  }

  const alias = getAciDynamicFeatureAlias(raw);

  if (alias?.feature) {
    const isConnectedFeaturesPhrase =
      alias.feature === "Connected Features" &&
      /\bconnected\s*features\b/i.test(raw);

    const isDiscovery =
      /\b(which|show|find|list)\b.*\b(variants?|cars?)\b/i.test(raw) ||
      /\bavailable\b.*\b(which|variant|variants)\b/i.test(raw) ||
      /\bvariants?\b/i.test(raw) ||
      isConnectedFeaturesPhrase ||
      /\b(best|highest|maximum|max|most)\b.*\b(mileage|fuel\s*efficiency|average|kmpl|kpl)\b/i.test(raw) ||
      /\b(cheapest|most affordable|lowest price|without|miss|missing|do not have|dont have|don't have)\b/i.test(raw);

    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: alias.feature,
      cleanUserMessage: isDiscovery
        ? raw
        : buildAciDynamicFeatureCleanUserMessage({
            raw,
            model,
            modelEntity,
            feature: alias.feature,
            alias,
          }),
      intent: isDiscovery ? "vehicle_feature_discovery" : "vehicle_feature_answer",
      canvasType: isDiscovery ? "feature_match_builder_canvas" : "",
    };
  }

  const hasFeatureExplorerLanguage =
    /\b(show|open|list|full|all)\b.*\b(featuers|features|feature\s*list)\b/i.test(raw) ||
    /\b(featuers|features)\b$/i.test(raw) ||
    /\b(safety|comfort|infotainment|entertainment|exterior|engine|dimensions?|capacity)\s+features\b/i.test(raw) ||
    /\b(show|open|list|tell|check)\b.*\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase)\b/i.test(raw) ||
    /\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase)\b.*\b(of|for|in)\b/i.test(raw) ||
    /\b(best|most|money|worth|upgrade|buy)\b.*\b(features|safety|comfort)\b/i.test(raw);

  if (hasFeatureExplorerLanguage) {
    const categoryKey = categoryMatch?.key || "";
    const categoryLabel = categoryMatch?.label || "";

    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: "",
      category: categoryKey,
      categoryLabel,
      cleanUserMessage: categoryLabel
        ? `${model} ${categoryLabel} features`
        : /\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase)\b/i.test(raw)
          ? `${model} dimensions and capacity`
          : `${model} features`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
    };
  }

  const explicitModelMention = cleanText(xuvAliasMatch?.[0] || modelEntity.matchedText || "");
  if (explicitModelMention) {
    let residual = raw;
    [
      modelEntity.matchedText,
      xuvAliasMatch?.[0],
      fullModel,
      modelEntity.model,
      model,
    ]
      .filter(Boolean)
      .sort((a, b) => String(b).length - String(a).length)
      .forEach((candidate) => {
        const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        residual = residual.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
      });

    residual = residual
      .replace(/\b(show|open|tell|me|about|overview|details?|car|model|variant|new)\b/gi, " ")
      .replace(/[?.,]/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    const hasSpecificIntent =
      /\b(price|pricelist|on\s*road|on-road|emi|compare|comparison|vs|versus|features?|colors?|colours?|sunroof|abs|mileage|airbags?|quotation|offer)\b/i.test(
        raw,
      );

    if (!hasSpecificIntent && residual.split(/\s+/).filter(Boolean).length <= 3) {
      return {
        model,
        make: brand,
        brand,
        fullModel,
        variant: formatAciInlineVariantName(residual),
        feature: "",
        cleanUserMessage: `${model} overview`,
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
      };
    }
  }

  return null;
};



const formatAciInlineVariantName = (value = "") =>
  String(value || "")
    .trim()
    .split(/\s+/)
    .map((word) => {
      if (/^ivt$/i.test(word)) return "iVT";
      if (/^(dct|amt|at|mt|cvt)$/i.test(word)) return word.toUpperCase();
      if (/^sx$/i.test(word)) return "SX";
      if (/^htx$/i.test(word)) return "HTX";
      if (/^abs$/i.test(word)) return "ABS";
      if (/^[A-Z0-9()]+$/.test(word)) return word;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");


const toAciInlineArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const getAciInlineWidget = (response = {}) =>
  response.widget || (Array.isArray(response.widgets) ? response.widgets[0] : null) || {};

const getAciInlineRows = (response = {}) => {
  const widget = getAciInlineWidget(response);

  return toAciInlineArray(
    response.rows ||
      response.items ||
      response.data?.rows ||
      response.data?.items ||
      response.data?.matchedVariants ||
      response.widget?.rows ||
      response.widget?.items ||
      response.widget?.matchedVariants ||
      widget.rows ||
      widget.items ||
      widget.matchedVariants ||
      widget.data?.rows ||
      widget.data?.items,
  );
};

const getAciRowVariantName = (row = {}) =>
  row.variant ||
  row.variantName ||
  row.displayVariant ||
  row.name ||
  row.title ||
  row.label ||
  "";

const getAciRowValue = (row = {}) =>
  row.value ??
  row.displayValue ??
  row.featureValue ??
  row.specValue ??
  row.formattedValue ??
  row.rawValue ??
  row.text ??
  "";

const normalizeAciInlineValue = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .replace(/\baRAI\b/g, "ARAI")
    .trim();

const uniqueAciInlineValues = (items = []) => {
  const seen = new Set();
  const out = [];

  for (const item of items) {
    const key = String(item || "").toLowerCase().trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
};

const isAciMileageFeature = (value = "") =>
  /\b(arai\s*mileage|mileage|fuel\s*efficiency|kmpl|kpl|average)\b/i.test(
    String(value || ""),
  );

const extractAciVariantFromCleanMessage = ({ cleanUserMessage = "", model = "" } = {}) => {
  let text = String(cleanUserMessage || "").trim();
  const modelText = String(model || "").trim();

  if (!text || !modelText) return "";

  const safeModel = modelText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  text = text.replace(new RegExp(`\\b${safeModel}\\b`, "ig"), " ");

  text = text
    .replace(/\b(arai\s*mileage|mileage|fuel\s*efficiency|kmpl|kpl|average)\b/gi, " ")
    .replace(/\b(features?|variant|variants|show|tell|check|does|do|has|have|is|are|it|which|what|who|where|best|better|top|highest|maximum|max|most|gives|give|for|worth|buy|should|would|could|can)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return text ? formatAciInlineVariantName(text) : "";
};

const extractAciNumericMileage = (value = "") => {
  const text = String(value || "");
  const match = text.match(/(\d+(?:\.\d+)?)/);
  return match ? Number(match[1]) : null;
};

const buildAciMileageDirectAnswer = (
  response = {},
  { detected = {}, cleanUserMessage = "", message = "" } = {},
) => {
  const featureName =
    detected.feature ||
    response.meta?.detectedFeature ||
    response.detectedFeature ||
    response.feature ||
    response.data?.feature ||
    response.widget?.feature ||
    "";

  if (!isAciMileageFeature(featureName)) return "";

  const rows = getAciInlineRows(response).filter((row) => row?.available !== false);

  if (!rows.length) return "";

  const values = uniqueAciInlineValues(
    rows
      .map((row) => normalizeAciInlineValue(getAciRowValue(row)))
      .filter((value) => value && !/not available|false|no$/i.test(value)),
  );

  if (!values.length) return "";

  const model =
    detected.model ||
    response.meta?.detectedModel ||
    response.model ||
    response.data?.model ||
    response.widget?.model ||
    "this car";

  const askedVariant =
    extractAciVariantFromCleanMessage({
      cleanUserMessage: cleanUserMessage || message,
      model,
    }) ||
    (rows.length === 1 ? formatAciInlineVariantName(getAciRowVariantName(rows[0])) : "");

  const subject = [model, askedVariant].filter(Boolean).join(" ");

  const isBestMileageQuery =
    /\b(best|highest|maximum|max|most)\b.*\b(mileage|fuel\s*efficiency|average|kmpl|kpl)\b/i.test(
      String(message || cleanUserMessage || ""),
    );

  if (isBestMileageQuery) {
    const numericPairs = rows
      .map((row) => ({
        variant: formatAciInlineVariantName(getAciRowVariantName(row)),
        value: normalizeAciInlineValue(getAciRowValue(row)),
        numeric: extractAciNumericMileage(getAciRowValue(row)),
      }))
      .filter((item) => Number.isFinite(item.numeric))
      .sort((a, b) => b.numeric - a.numeric);

    if (numericPairs.length) {
      const best = numericPairs[0];
      const topVariants = uniqueAciInlineValues(
        numericPairs
          .filter((item) => item.numeric === best.numeric)
          .map((item) => item.variant)
          .filter(Boolean),
      ).slice(0, 4);

      const variantCopy = topVariants.length
        ? ` Top variant${topVariants.length > 1 ? "s" : ""}: ${topVariants.join(", ")}.`
        : "";

      return `The best claimed mileage in ${model} is ${best.value}.${variantCopy}`;
    }
  }

  if (rows.length === 1 || values.length === 1) {
    return `${subject} mileage is ${values[0]}.`;
  }

  const numericValues = values
    .map(extractAciNumericMileage)
    .filter((value) => Number.isFinite(value));

  if (numericValues.length >= 2) {
    const min = Math.min(...numericValues);
    const max = Math.max(...numericValues);

    if (min !== max) {
      const unit = values.find((value) => /\bkmpl\b|\bkpl\b/i.test(value))?.match(/\b(kmpl|kpl)\b/i)?.[1] || "kmpl";
      return `${subject} mileage ranges from ${min} to ${max} ${unit}, depending on the exact transmission/variant.`;
    }
  }

  return `${subject} mileage varies by variant — ${values.slice(0, 3).join(", ")}${values.length > 3 ? " and more" : ""}.`;
};


const polishAciEarlyFeatureResponseCopy = (response = {}, options = {}) => {
  if (!response || typeof response !== "object") return response;

  const directMileageAnswer = buildAciMileageDirectAnswer(response, options);
  let answer = directMileageAnswer || String(response.answer || "");

  answer = answer
    .replace(/\baRAI Mileage\b/g, "ARAI mileage")
    .replace(/\barai mileage\b/gi, "ARAI mileage")
    .replace(/anti-lock\s+Braking\s+System\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)")
    .replace(/anti-lock\s+braking\s+system\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)")
    .replace(/\bHTX IVT\b/g, "HTX iVT")
    .replace(/\bSX IVT\b/g, "SX iVT")
    .replace(/\bSingle Pane sunroof\b/g, "single-pane sunroof")
    .replace(/\bPanoramic sunroof\b/g, "panoramic sunroof");

  const singleVariantMatch = answer.match(
    /Good news\s*—\s*all\s+1\s+current\s+(.+?)\s+variants\s+get\s+(.+?)\./i,
  );

  if (singleVariantMatch) {
    const variantName = formatAciInlineVariantName(singleVariantMatch[1]);
    const featureName = singleVariantMatch[2]
      .replace(/\baRAI Mileage\b/g, "ARAI mileage")
      .replace(/\barai mileage\b/gi, "ARAI mileage")
      .replace(/anti-lock\s+Braking\s+System\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)")
      .replace(/anti-lock\s+braking\s+system\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)");

    answer = `Yes — ${variantName} gets ${featureName}.`;
  }

  response.answer = answer;

  if (response.data && typeof response.data === "object") {
    response.data.answer = answer;
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget.answer = answer;
  }

  return response;
};




const pickAciContextValue = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const text = String(value).trim();
    if (text) return value;
  }
  return "";
};


const extractAciScopedVariantFromCleanMessage = ({
  cleanUserMessage = "",
  model = "",
  fullModel = "",
  make = "",
} = {}) => {
  let text = String(cleanUserMessage || "").trim();

  [
    fullModel,
    model,
    make,
  ]
    .filter(Boolean)
    .sort((a, b) => String(b).length - String(a).length)
    .forEach((candidate) => {
      const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      text = text.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
    });

  text = text
    .replace(/\b(abs|ags|anti\s*lock\s*braking|anti-lock\s*braking|sunroof|mileage|arai\s*mileage|features?|feature|price|pricelist|overview|details?|on\s*road|on-road)\b/gi, " ")
    .replace(/\b(does|do|is|are|has|have|having|with|get|gets|got|which|what|best|highest|maximum|max|most|variant|variants|car|cars|it|this|that|current|selected|new|old)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return text ? formatAciInlineVariantName(text) : "";
};

const shouldCarryAciCurrentVariant = ({ message = "", dynamicModelEntity = null, explicitVariant = "", context = {} } = {}) => {
  if (explicitVariant) return false;
  if (!dynamicModelEntity?.fromContext) return false;
  if (!context?.anchorVariant && !context?.selectedVehicle?.variant && !context?.selectedVehicle?.variantName) return false;

  // Carry variant only for true pronoun/current-variant questions.
  // Do not carry it for model-level questions like "Does Seltos have ABS?"
  return /\b(it|this|that|current one|selected one|this variant|current variant|selected variant)\b/i.test(
    String(message || ""),
  );
};

const buildAciFeatureAuthorityContextPatch = ({
  context = {},
  detected = {},
  dynamicModelEntity = null,
  cleanUserMessage = "",
  message = "",
} = {}) => {
  const make =
    detected.make ||
    detected.brand ||
    dynamicModelEntity?.brand ||
    context?.anchorMake ||
    context?.selectedVehicle?.make ||
    context?.selectedVehicle?.brand ||
    "";

  const model =
    detected.model ||
    dynamicModelEntity?.model ||
    context?.anchorModel ||
    context?.selectedVehicle?.model ||
    "";

  const fullModel =
    detected.fullModel ||
    dynamicModelEntity?.fullModel ||
    context?.anchorFullModel ||
    context?.selectedVehicle?.fullModel ||
    (make && model ? `${make} ${model}` : model);

  const explicitVariant = extractAciScopedVariantFromCleanMessage({
    cleanUserMessage,
    model,
    fullModel,
    make,
  });

  const carriedVariant = shouldCarryAciCurrentVariant({
    message,
    dynamicModelEntity,
    explicitVariant,
    context,
  })
    ? pickAciContextValue(
        context?.anchorVariant,
        context?.selectedVehicle?.variant,
        context?.selectedVehicle?.variantName,
      )
    : "";

  const isComparisonIntent =
    detected?.intent === "vehicle_feature_comparison" ||
    detected?.canvasType === "comparison_canvas";

  const nextVariant = isComparisonIntent
    ? ""
    : explicitVariant || carriedVariant || "";
  const contextVehicle = context?.selectedVehicle || {};
  const contextVehicleMatchesModel =
    normalizeAciContextText(contextVehicle.model || "") ===
    normalizeAciContextText(model || "");

  return {
    selectedVehicle: {
      ...(contextVehicleMatchesModel ? contextVehicle : {}),
      make,
      brand: make,
      model,
      fullModel,
      variant: nextVariant,
      variantName: nextVariant,
      city: pickAciContextValue(context?.anchorCity, context?.city, context?.selectedVehicle?.city, "new-delhi"),
      citySlug: pickAciContextValue(context?.anchorCity, context?.citySlug, context?.selectedVehicle?.citySlug, "new-delhi"),
    },
    anchorMake: make,
    anchorModel: model,
    anchorFullModel: fullModel,
    anchorVariant: nextVariant,
    anchorCity: pickAciContextValue(context?.anchorCity, context?.city, context?.selectedVehicle?.citySlug, "new-delhi"),
    selectedColor: null,
    ...(isComparisonIntent
      ? {
          selectedComparisonSet: {
            model,
            variants: Array.isArray(detected?.variants) ? detected.variants : [],
          },
        }
      : {}),
  };
};

const applyAciFeatureAuthorityContextPatch = (response = {}, patch = {}) => {
  if (!response || typeof response !== "object") return response;

  const mergeAuthorityPatch = (existingPatch = {}) => {
    const existingVehicle =
      existingPatch.selectedVehicle ||
      response.vehicle ||
      response.widget?.vehicle ||
      {};
    const patchVehicle = patch.selectedVehicle || {};
    const existingModel = normalizeAciContextText(existingVehicle.model || "");
    const patchModel = normalizeAciContextText(
      patchVehicle.model || patch.anchorModel || "",
    );
    const canPreserveExistingVehicle =
      existingVehicle &&
      (!existingModel || !patchModel || existingModel === patchModel);
    const selectedVehicle = {
      ...(canPreserveExistingVehicle ? existingVehicle : {}),
      ...patchVehicle,
    };

    if (canPreserveExistingVehicle) {
      selectedVehicle.imageUrl =
        patchVehicle.imageUrl || existingVehicle.imageUrl || "";
      selectedVehicle.normalizedImageUrl =
        patchVehicle.normalizedImageUrl ||
        existingVehicle.normalizedImageUrl ||
        existingVehicle.imageUrl ||
        "";
      selectedVehicle.imageFrame =
        patchVehicle.imageFrame || existingVehicle.imageFrame || null;
      selectedVehicle.displayFrameMeta =
        patchVehicle.displayFrameMeta ||
        existingVehicle.displayFrameMeta ||
        selectedVehicle.imageFrame ||
        null;
    }

    return {
      ...existingPatch,
      ...patch,
      selectedVehicle,
    };
  };

  response.contextPatch = {
    ...mergeAuthorityPatch(response.contextPatch || {}),
  };

  response.context = {
    ...mergeAuthorityPatch(response.context || {}),
  };

  if (response.data && typeof response.data === "object") {
    response.data = {
      ...response.data,
      contextPatch: mergeAuthorityPatch(response.data.contextPatch || {}),
    };
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget = {
      ...response.widget,
      contextPatch: mergeAuthorityPatch(response.widget.contextPatch || {}),
    };
  }

  return response;
};



const ACI_EARLY_GATE_INTERNAL_PATTERN =
  /\b(loan|closure|lan|case\s*id|customer|cust\s*id|policy|payout|rc|challan|cdrive|internal|file\s*no|agreement|collection|overdue|repo|noc|insurance\s*expiry)\b/i;

const ACI_EARLY_GATE_LEAD_PATTERN =
  /\b(best\s*price|quotation|quote|final\s*price|deal|discount|offer|offers|callback|call\s*back|book|booking|finance|exchange|insurance)\b/i;

const countAciEarlyGateIntentFamilies = (message = "") => {
  const raw = String(message || "");
  const checks = [
    /\b(price|pricelist|on\s*road|on-road|ex\s*showroom|ex-showroom)\b/i,
    /\b(compare|comparison|vs|versus|v\/s|with)\b/i,
    /\b(emi|loan|down\s*payment|tenure|interest)\b/i,
    /\b(offer|offers|discount|deal|best\s*price|quotation|quote)\b/i,
    /\b(color|colors|colour|colours|black|white|red|blue|grey|gray)\b/i,
    /\b(feature|features|sunroof|adas|airbags?|abs|mileage|camera|ventilated)\b/i,
  ];

  return checks.reduce((count, pattern) => count + (pattern.test(raw) ? 1 : 0), 0);
};

const shouldSkipAciEarlyFeatureGate = (message = "") => {
  const raw = String(message || "").trim();
  if (!raw) return true;

  // Internal office queries must never be interpreted as new-car model names.
  // Example: "Loan closure 7077" was being misread as Mahindra Logan.
  if (ACI_EARLY_GATE_INTERNAL_PATTERN.test(raw)) return true;

  // Quote/best-price/offer flows need planner + lead tools, not a quick price card.
  if (ACI_EARLY_GATE_LEAD_PATTERN.test(raw)) return true;

  // Multi-intent needs executor secondaryResponses. Early gate can only return one card.
  if (countAciEarlyGateIntentFamilies(raw) >= 2) return true;

  return false;
};



const isAciComparisonMessage = (message = "") =>
  /\b(compare|comparison|vs|versus)\b/i.test(String(message || ""));

const applyAciExplicitMessageModelContextOverride = ({
  message = "",
  context = {},
  dynamicModelEntity = null,
} = {}) => {
  if (!context || typeof context !== "object") return context;
  if (!dynamicModelEntity?.model) return context;

  // Do not hijack "Compare with City" type follow-ups.
  // In comparison follow-ups, the mentioned model can be the rival, not the selected car.
  if (isAciComparisonMessage(message)) return context;

  const nextModel = cleanText(dynamicModelEntity.model);
  const nextMake = cleanText(dynamicModelEntity.brand || dynamicModelEntity.make || "");
  const nextFullModel = cleanText(
    dynamicModelEntity.fullModel ||
      (nextMake && nextModel ? `${nextMake} ${nextModel}` : nextModel),
  );

  if (!nextModel) return context;

  const selectedVehicle = context.selectedVehicle || {};
  const currentModel = cleanText(
    context.anchorModel ||
      selectedVehicle.model ||
      context.model ||
      "",
  );

  const sameModel =
    normalizeAciContextText(currentModel) === normalizeAciContextText(nextModel);

  if (sameModel) return context;

  // Latest explicit user message must beat stale frontend/backend context.
  context.selectedVehicle = {
    ...selectedVehicle,

    // Explicit model switch must not carry stale make/brand from previous car.
    // Example: Verna context + "Does Thar have sunroof?" must not become Hyundai Thar.
    make: nextMake || "",
    brand: nextMake || "",
    model: nextModel,
    displayName: nextFullModel || nextModel,
    fullModel: nextFullModel || nextModel,
    variant: "",
    selectedVariant: "",
    variantName: "",
  };

  context.anchorMake = nextMake || "";
  context.anchorModel = nextModel;
  context.anchorFullModel = nextFullModel || nextModel;
  context.anchorVariant = "";
  context.model = nextModel;

  return context;
};


const maybeRunAciEarlyFeatureGate = async ({
  message = "",
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  if (shouldSkipAciEarlyFeatureGate(message)) {
    return null;
  }

  const dynamicModelEntityFromText = await resolveAciExplicitMessageModelEntity(message);

  applyAciExplicitMessageModelContextOverride({
    message,
    context,
    dynamicModelEntity: dynamicModelEntityFromText,
  });
  const dynamicModelEntityFromContext = buildAciContextModelEntity({
    context,
    selectedEntity,
  });

  const dynamicModelEntity = chooseAciDynamicModelEntity({
    textEntity: dynamicModelEntityFromText,
    contextEntity: dynamicModelEntityFromContext,
    message,
  });

  if (hasAciTestDriveIntent(message)) {
    const model = dynamicModelEntity?.model || context?.anchorModel || "";
    const brand = dynamicModelEntity?.brand || context?.anchorMake || "";
    const selectedVehicle = {
      ...(context?.selectedVehicle || {}),
      make: brand || context?.selectedVehicle?.make || "",
      brand: brand || context?.selectedVehicle?.brand || "",
      model: model || context?.selectedVehicle?.model || "",
      displayName:
        dynamicModelEntity?.fullModel ||
        context?.selectedVehicle?.displayName ||
        model ||
        "",
      variant: "",
      variantName: "",
      selectedVariant: "",
    };

    return {
      intent: "unavailable",
      tool: "unavailable",
      displayMode: "inline",
      inlineType: "unavailable_notice",
      canvasType: "",
      answer:
        "Test drives are not available from ACI Assist right now. I can still help with price, variants, features, colours, EMI, quotation, and comparisons.",
      actions: model
        ? [
            {
              id: "test-drive-fallback-quotation",
              label: "Get quotation",
              type: "lead",
              query: `Get quotation for ${model}`,
              intent: "aci_new_car_quotation",
              canvasType: "aci_quotation_canvas",
              leadType: "quotation",
            },
            {
              id: "test-drive-fallback-price",
              label: "See price",
              type: "ask",
              query: `${model} price`,
              intent: "vehicle_pricelist",
              canvasType: "pricelist_canvas",
            },
          ]
        : [],
      leadingQuestions: [],
      conversationSuggestions: [],
      contextPatch: {
        anchorMake: selectedVehicle.make || "",
        anchorModel: selectedVehicle.model || "",
        anchorFullModel: selectedVehicle.displayName || selectedVehicle.model || "",
        anchorVariant: "",
        selectedVehicle,
      },
      meta: {
        earlyFeatureGate: true,
        testDriveUnavailable: true,
      },
    };
  }

  const detected =
    detectAciEarlyDynamicRoutedRequest({
      message,
      modelEntity: dynamicModelEntity,
    }) ||
    (typeof detectAciEarlyPricelistTypoRequest === "function"
      ? detectAciEarlyPricelistTypoRequest(message)
      : null) ||
    (typeof detectAciEarlyFeatureExplorerRequest === "function"
      ? detectAciEarlyFeatureExplorerRequest(message)
      : null) ||
    (typeof detectAciEarlyVariantComparisonRequest === "function"
      ? detectAciEarlyVariantComparisonRequest(message)
      : null) ||
    (typeof detectAciEarlyFeatureRequest === "function"
      ? detectAciEarlyFeatureRequest(message)
      : null);

  if (!detected) return null;

  const toolRunner =
    detected.intent === "vehicle_overview"
      ? runVehiclePricelistNewCarsTool
      : detected.intent === "vehicle_pricelist" ||
    detected.canvasType === "pricelist_canvas"
      ? runVehiclePricelistNewCarsTool
      : runVehicleFeaturesTool;

  const cleanUserMessage = detected.cleanUserMessage || message;

  const toolPlan = {
    tool: detected.intent,
    intent: detected.intent,
    toolIntent: detected.intent,
    canvasType: detected.canvasType,
    entities: {
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
    input: {
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
    filters: {
      ...(filters || {}),
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
  };

  const preToolAuthorityContextPatch = buildAciFeatureAuthorityContextPatch({
    context,
    detected,
    dynamicModelEntity,
    cleanUserMessage,
    message,
  });

  const scopedAnchorVariant = String(
    preToolAuthorityContextPatch.anchorVariant || "",
  );
  const scopedSelectedVehicle = {
    ...(preToolAuthorityContextPatch.selectedVehicle || {}),
    variant: scopedAnchorVariant,
    variantName: scopedAnchorVariant,
  };

  const scopedSelectedEntity =
    selectedEntity && typeof selectedEntity === "object"
      ? {
          ...selectedEntity,
          selectedVehicle: {
            ...(selectedEntity.selectedVehicle || selectedEntity.vehicle || {}),
            ...scopedSelectedVehicle,
          },
          vehicle: {
            ...(selectedEntity.vehicle || selectedEntity.selectedVehicle || {}),
            ...scopedSelectedVehicle,
          },
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
        }
      : {
          selectedVehicle: scopedSelectedVehicle,
          vehicle: scopedSelectedVehicle,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
        };

  const scopedFeatureContext = {
    ...(context || {}),
    ...preToolAuthorityContextPatch,
    selectedEntity: scopedSelectedEntity,
    anchorVariant: scopedAnchorVariant,
    selectedVehicle: scopedSelectedVehicle,
  };

  const scopedFeatureFilters =
    filters && typeof filters === "object"
      ? {
          ...filters,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
        }
      : {
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
        };

  const scopedDetected =
    detected && typeof detected === "object"
      ? {
          ...detected,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          entities: {
            ...(detected.entities || {}),
            variant: scopedAnchorVariant,
            variantName: scopedAnchorVariant,
            selectedVariant: scopedAnchorVariant,
            selectedVariantKey: scopedAnchorVariant,
            requestedVariant: scopedAnchorVariant,
          },
        }
      : detected;

  const scopedToolPlan =
    toolPlan && typeof toolPlan === "object"
      ? {
          ...toolPlan,
          variant: scopedAnchorVariant,
	          variantName: scopedAnchorVariant,
	          selectedVariant: scopedAnchorVariant,
	          selectedVariantKey: scopedAnchorVariant,
	          requestedVariant: scopedAnchorVariant,
	          entities: {
	            ...(toolPlan.entities || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	          input: {
	            ...(toolPlan.input || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	          filters: {
	            ...(toolPlan.filters || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	        }
	      : toolPlan;

  let response = await toolRunner({
    detected: scopedDetected,
    filters: scopedFeatureFilters,
    context: scopedFeatureContext,
    toolPlan: scopedToolPlan,
    selectedEntity: scopedSelectedEntity,
    userMessage: cleanUserMessage,
  });

  let overviewAuthorityContextPatch = null;

  if (detected.intent === "vehicle_overview") {
    const overviewVehicle =
      response.vehicle ||
      response.widget?.vehicle ||
      response.contextPatch?.selectedVehicle ||
      preToolAuthorityContextPatch.selectedVehicle ||
      {};
    const overviewContextPatch = {
      ...preToolAuthorityContextPatch,
      ...(response.contextPatch || {}),
      selectedVehicle: {
        ...(overviewVehicle || {}),
        variant: detected.variant || "",
        variantName: detected.variant || "",
        selectedVariant: detected.variant || "",
      },
      anchorMake:
        overviewVehicle.make ||
        overviewVehicle.brand ||
        response.contextPatch?.anchorMake ||
        preToolAuthorityContextPatch.anchorMake ||
        "",
      anchorModel:
        overviewVehicle.model ||
        response.contextPatch?.anchorModel ||
        preToolAuthorityContextPatch.anchorModel ||
        detected.model ||
        "",
      anchorFullModel:
        overviewVehicle.fullModel ||
        overviewVehicle.displayName ||
        response.contextPatch?.anchorFullModel ||
        preToolAuthorityContextPatch.anchorFullModel ||
        detected.fullModel ||
        "",
      anchorVariant: detected.variant || "",
    };

    response = {
      ...response,
      tool: "vehicle_overview",
      intent: "vehicle_overview",
      canvasType: "car_overview_canvas",
      answer: `Opened ${overviewVehicle.displayName || detected.model} overview.`,
      vehicle: overviewContextPatch.selectedVehicle,
      contextPatch: overviewContextPatch,
      widget: {
        ...(response.widget || {}),
        type: "vehicle_overview",
        tool: "vehicle_overview",
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
        title: `${overviewVehicle.displayName || detected.model} overview`,
        answer: `Opened ${overviewVehicle.displayName || detected.model} overview.`,
        vehicle: overviewContextPatch.selectedVehicle,
        rows: response.rows || response.widget?.rows || [],
        items: response.items || response.widget?.items || response.rows || [],
        contextPatch: overviewContextPatch,
      },
    };

    overviewAuthorityContextPatch = overviewContextPatch;
  }

  polishAciEarlyFeatureResponseCopy(response, { detected, cleanUserMessage, message });

  const authorityContextPatch =
    overviewAuthorityContextPatch || preToolAuthorityContextPatch;

  applyAciFeatureAuthorityContextPatch(response, authorityContextPatch);



  return {
    ...response,
    meta: {
      ...(response?.meta || {}),
      earlyFeatureGate: true,
      detectedModel: detected.model,
      detectedFullModel: detected.fullModel || "",
      detectedFeature: detected.feature,
      detectedCategory: detected.category || "",
      detectedCategoryLabel: detected.categoryLabel || "",
      modelMatchedText: dynamicModelEntity?.matchedText || "",
      modelCorrectionConfidence: dynamicModelEntity?.confidence || null,
    },
  };
};


const chatWithAgentCore = async (...args) => {
  const __aciEarlyAgentArgs = Array.isArray(args) ? (args[0] || {}) : {};

  const earlyFeatureResponse = await maybeRunAciEarlyFeatureGate({
    message: __aciEarlyAgentArgs.message,
    context: __aciEarlyAgentArgs.context,
    selectedEntity: __aciEarlyAgentArgs.selectedEntity,
    filters: __aciEarlyAgentArgs.filters,
  });

  if (earlyFeatureResponse) {
    return earlyFeatureResponse;
  }


  const startedAt = Date.now();

  const {
    message,
    context,
    history,
    options,
    rawInput,
  } = normalizeChatInput(...args);

  const explicitMessageModelEntityForContext =
    await resolveAciExplicitMessageModelEntity(message);

  applyAciExplicitMessageModelContextOverride({
    message,
    context,
    dynamicModelEntity: explicitMessageModelEntityForContext,
  });


  if (!message) {
    const plan = createUnavailableFallbackPlan({
      message,
      reason: "insufficient_information",
      details: "No user message was provided.",
    });

    const response = await executeAciPlannerPlan({
      plan,
      userMessage: "",
      context,
    });

    return {
      ...response,
      service: {
        version: ACI_ASSIST_SERVICE_VERSION,
        executorVersion: EXECUTOR_VERSION,
        durationMs: Date.now() - startedAt,
        plannerFallbackUsed: true,
      },
    };
  }

  const {
    plan,
    plannerMeta,
    rawPlannerResult,
  } = await createPlannerPlanForMessage({
    message,
    context,
    history,
    options,
  });

  const response = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context,
    runtimeHints: {
      rawPlannerResult,
      rawInput,
    },
  });

  return {
    ...response,
    service: {
      version: ACI_ASSIST_SERVICE_VERSION,
      executorVersion: EXECUTOR_VERSION,
      durationMs: Date.now() - startedAt,
      planner: plannerMeta,
      oldSystemUsed: false,
    },
  };
};

const getNormalizerInputs = (args = []) => {
  const rawInput = args[0] || {};

  const message =
    typeof rawInput === "string"
      ? rawInput
      : rawInput.message || rawInput.query || rawInput.text || rawInput.userMessage || rawInput.prompt || "";

  const context =
    typeof rawInput === "object"
      ? rawInput.context ||
        rawInput.conversationContext ||
        rawInput.sessionContext ||
        rawInput.state ||
        rawInput.session ||
        {}
      : args[1] || {};

  return {
    message,
    context,
  };
};


const pickAciFirstObject = (...values) => {
  for (const value of values) {
    if (isPlainObject(value)) return value;
  }
  return {};
};

const pickAciFirstArray = (...values) => {
  for (const value of values) {
    if (Array.isArray(value)) return value.filter(Boolean);
  }
  return [];
};

const normalizeAciServiceResponseContract = (
  response = {},
  { startedAt = Date.now() } = {},
) => {
  if (!response || typeof response !== "object") return response;

  const widget =
    response.widget ||
    (Array.isArray(response.widgets) ? response.widgets.find(Boolean) : null) ||
    null;

  const canvasType = response.canvasType || widget?.canvasType || "";
  const inlineType = response.inlineType || widget?.inlineType || "";

  const displayMode =
    response.displayMode ||
    (canvasType && inlineType
      ? "both"
      : canvasType
        ? "canvas"
        : inlineType
          ? "inline"
          : "inline");

  const actions = pickAciFirstArray(
    response.actions,
    widget?.actions,
    response.data?.actions,
  );

  const leadingQuestions = pickAciFirstArray(
    response.leadingQuestions,
    response.conversationSuggestions,
    widget?.leadingQuestions,
    widget?.conversationSuggestions,
    response.data?.leadingQuestions,
    response.data?.conversationSuggestions,
  );

  const rows = pickAciFirstArray(
    response.rows,
    response.items,
    response.data?.rows,
    response.data?.items,
    widget?.rows,
    widget?.items,
  );

  const data = {
    ...pickAciFirstObject(response.data),
  };

  if (!Object.keys(data).length) {
    data.title = response.title || widget?.title || "";
    data.answer = response.answer || widget?.answer || "";
    data.intent = response.intent || widget?.intent || "";
    data.canvasType;
    data.inlineType;
    data.displayMode = displayMode;
    data.vehicle =
      response.vehicle ||
      widget?.vehicle ||
      response.contextPatch?.selectedVehicle ||
      {};
    data.rows = rows;
    data.items = rows;
    data.contextPatch = response.contextPatch || {};
  }

  const secondaryResponses = Array.isArray(response.secondaryResponses)
    ? response.secondaryResponses
    : [];

  const runtimeResultsMeta = Array.isArray(response.runtimeResultsMeta)
    ? response.runtimeResultsMeta
    : [];

  return {
    ...response,
    displayMode,
    canvasType,
    inlineType,
    actions,
    leadingQuestions,
    secondaryResponses,
    runtimeResultsMeta,
    data,
    service: {
      ...(response.service || {}),
      version:
        response.service?.version ||
        ACI_ASSIST_SERVICE_VERSION,
      executorVersion:
        response.service?.executorVersion ||
        EXECUTOR_VERSION,
      durationMs:
        response.service?.durationMs ??
        Math.max(0, Date.now() - startedAt),
      oldSystemUsed: false,
    },
  };
};



const repairAciResponseContextFromActiveContext = ({
  response = {},
  context = {},
} = {}) => {
  if (!response || typeof response !== "object") return response;
  if (!context || typeof context !== "object") return response;

  const contextVehicle = context.selectedVehicle || {};
  const activeModel = cleanText(
    context.anchorModel ||
      contextVehicle.model ||
      context.model ||
      "",
  );

  const activeMake = cleanText(
    context.anchorMake ||
      contextVehicle.make ||
      contextVehicle.brand ||
      "",
  );

  if (!activeModel) return response;

  const patch = response.contextPatch || {};
  const patchVehicle = patch.selectedVehicle || {};

  const patchModel = cleanText(
    patch.anchorModel ||
      patchVehicle.model ||
      response.vehicle?.model ||
      response.data?.model ||
      "",
  );

  // Only repair when response is for the same active model.
  // Never force active context into a different returned car.
  if (
    patchModel &&
    normalizeAciContextText(patchModel) !== normalizeAciContextText(activeModel)
  ) {
    return response;
  }

  const activeFullModel = cleanText(
    context.anchorFullModel ||
      contextVehicle.fullModel ||
      contextVehicle.displayName ||
      (activeMake && activeModel ? `${activeMake} ${activeModel}` : activeModel),
  );

  const repairedVehicle = {
    ...patchVehicle,
    make: cleanText(patchVehicle.make || patchVehicle.brand || "") || activeMake,
    brand: cleanText(patchVehicle.brand || patchVehicle.make || "") || activeMake,
    model: patchVehicle.model || activeModel,
    fullModel: patchVehicle.fullModel || activeFullModel,
    displayName: patchVehicle.displayName || activeFullModel,
  };

  response.contextPatch = {
    ...patch,
    selectedVehicle: repairedVehicle,
    anchorMake: cleanText(patch.anchorMake || "") || activeMake,
    anchorModel: patch.anchorModel || activeModel,
    anchorFullModel: patch.anchorFullModel || activeFullModel,
    anchorCity:
      patch.anchorCity ||
      context.anchorCity ||
      contextVehicle.citySlug ||
      contextVehicle.city ||
      "new-delhi",
  };

  if (response.data && typeof response.data === "object") {
    response.data.contextPatch = {
      ...(response.data.contextPatch || {}),
      ...response.contextPatch,
    };
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget.contextPatch = {
      ...(response.widget.contextPatch || {}),
      ...response.contextPatch,
    };
  }

  return response;
};


export const chatWithAgent = async (...args) => {
  const startedAt = Date.now();
  const { message, context } = getNormalizerInputs(args);
  const response = await chatWithAgentCore(...args);

  repairAciResponseContextFromActiveContext({
    response,
    context,
  });

  const normalized = await normalizeAciFinalResponse(response, {
    message,
    context,
  });

  repairAciResponseContextFromActiveContext({
    response: normalized,
    context,
  });

  return normalizeAciServiceResponseContract(normalized, {
    startedAt,
  });
};

export const chatWithAciAssist = chatWithAgent;
export const runAciAssist = chatWithAgent;
export const askAciAssist = chatWithAgent;


export default {
  chatWithAgent,
  chatWithAciAssist,
  runAciAssist,
  askAciAssist,
  createPlannerPlanForMessage,
};
