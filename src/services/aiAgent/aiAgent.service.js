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
  "punch", "scorpio", "xuv700", "xuv 700", "slavia", "virtus",
  "taigun", "kushaq", "brezza", "fronx", "swift", "dzire",
  "baleno", "fortuner", "innova",
];

const ACI_EARLY_FEATURE_ALIASES = [
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
];

const toAciTitleCaseModel = (model = "") => {
  const normalized = String(model || "").trim().toLowerCase();
  if (normalized === "xuv700" || normalized === "xuv 700") return "XUV700";
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
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

  const modelPattern = model.toLowerCase() === "xuv700" || model.toLowerCase() === "xuv 700"
    ? /xuv\s*700/i
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


const detectAciEarlyDynamicRoutedRequest = ({ message = "", modelEntity = null } = {}) => {
  const raw = String(message || "").trim();
  if (!raw || !modelEntity?.model) return null;

  const model = modelEntity.model;
  const categoryMatch = detectAciDynamicFeatureCategory(raw);

  const isCategoryFeatureExplorerRequest =
    /\b(featuers|features|feature\s*list)\b/i.test(raw) &&
    categoryMatch?.key &&
    categoryMatch.key !== "connected";

  if (isCategoryFeatureExplorerRequest) {
    return {
      model,
      make: modelEntity.brand || "",
      brand: modelEntity.brand || "",
      fullModel: modelEntity.fullModel || "",
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
      make: modelEntity.brand || "",
      brand: modelEntity.brand || "",
      fullModel: modelEntity.fullModel || "",
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
        make: modelEntity.brand || "",
        brand: modelEntity.brand || "",
        fullModel: modelEntity.fullModel || "",
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
      /\b(cheapest|most affordable|lowest price|without|miss|missing|do not have|dont have|don't have)\b/i.test(raw);

    return {
      model,
      make: modelEntity.brand || "",
      brand: modelEntity.brand || "",
      fullModel: modelEntity.fullModel || "",
      feature: alias.feature,
      cleanUserMessage: raw,
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
      make: modelEntity.brand || "",
      brand: modelEntity.brand || "",
      fullModel: modelEntity.fullModel || "",
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

  return null;
};


const maybeRunAciEarlyFeatureGate = async ({
  message = "",
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  const dynamicModelEntity = await resolveAciDynamicModelEntity(message);

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
    detected.intent === "vehicle_pricelist" ||
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
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
  };

  const response = await toolRunner({
    toolPlan,
    context: {
      ...(context || {}),
      selectedEntity,
      anchorMake: detected.make || dynamicModelEntity?.brand || context?.anchorMake || "",
      anchorModel: detected.model,
      anchorFullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      anchorFeatureCategory: detected.category || "",
      anchorFeatureCategoryLabel: detected.categoryLabel || "",
      anchorVariant: "",
      anchorCity: context?.anchorCity || context?.city || "new-delhi",
      selectedVehicle: {
        ...(context?.selectedVehicle || {}),
        make: detected.make || dynamicModelEntity?.brand || "",
        model: detected.model,
        fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
        variant: "",
        city: context?.anchorCity || context?.city || "new-delhi",
      },
    },
    userMessage: cleanUserMessage,
  });

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

export const chatWithAgent = async (...args) => {
  const { message, context } = getNormalizerInputs(args);
  const response = await chatWithAgentCore(...args);

  return await normalizeAciFinalResponse(response, {
    message,
    context,
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
