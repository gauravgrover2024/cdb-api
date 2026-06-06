import {
  retrieveAciDbCandidates,
} from "../candidates/aciDbCandidateRetriever.js";
import { parseHybridMeaningFrame } from "../understanding/hybridMeaningFrame.parser.js";
import { runAciUnderstandingEngine } from "../understanding/aciUnderstandingEngine.js";
import { normalizeAciBuyerLanguage } from "../understanding/aciLanguageNormalization.service.js";
import mongoose from "mongoose";
import resolveVehicleAlias from "../context/aciVehicleAliasRegistry.service.js";

import { buildLegacyPlanFromAciMeaningFrame } from "./aciCoreToLegacyPlan.adapter.js";
import { executeAciPlannerPlan } from "../../aiAgent/aiAgent.executor.js";
import { normalizeAciFinalResponse } from "../../aiAgent/aiAgent.contractNormalizer.js";
import { composeAciAnswer } from "../../aiAgent/aiAgent.answerComposer.js";
import { runVehiclePricelistNewCarsTool } from "../../aiAgent/tools/newCars/vehiclePricelist.tool.js";
import {
  buildVehiclePricelistResponse,
} from "../../aiAgent/aiAgent.responseTools.js";
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
  /\b(vs|v\/s|versus|compare|comparison|compared|better|better than|difference between|price difference|show price difference|which is cheaper|cheaper|costlier|more expensive|which one|which should|choose|pick|recommend|verdict)\b/i.test(message);

const isDirectNonComparisonTask = (message = "") =>
  /\b(colors?|colours?|sunroof|airbags?|features?|mileage|range|boot space|ground clearance|engine cc|power|price|on road|on-road|ex showroom|ex-showroom)\b/i.test(message) &&
  !/\b(price difference|show price difference|which is cheaper|compare|vs|v\/s|versus|difference between)\b/i.test(message);

const hasActiveComparisonFollowUp = ({ message = "", context = {} } = {}) => {
  const vehicles =
    context?.activeComparison?.vehicles ||
    context?.selectedComparisonSet?.vehicles ||
    context?.contextState?.activeComparison?.vehicles ||
    context?.aciContextState?.activeComparison?.vehicles ||
    [];

  if (!Array.isArray(vehicles) || vehicles.length < 2) return false;

  if (isDirectNonComparisonTask(message)) return false;

  return /\b(which one|which is better|better|safer|safety|their|price difference|show price difference|which is cheaper|cheaper|costlier|expensive|which should i|should i buy|choose|pick|recommend|verdict|final choice)\b/i.test(
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

const getActiveComparisonVehiclesFromContext = (context = {}) =>
  asArray(
    context?.activeComparison?.vehicles ||
      context?.selectedComparisonSet?.vehicles ||
      context?.contextState?.activeComparison?.vehicles ||
      context?.aciContextState?.activeComparison?.vehicles ||
      [],
  );

const getComparisonCityFromContext = (context = {}) =>
  cleanText(
    context?.activeComparison?.city ||
      context?.activeComparison?.citySlug ||
      context?.contextState?.activeComparison?.city ||
      context?.contextState?.activeComparison?.citySlug ||
      context?.selectedVehicle?.citySlug ||
      context?.selectedVehicle?.city ||
      context?.anchorCity ||
      "new-delhi",
  );

const vehicleLabelForComparison = (vehicle = {}) =>
  cleanText(
    vehicle.fullModel ||
      [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" ") ||
      vehicle.model ||
      "",
  );

const getSelectedVehicleFromContext = (context = {}) =>
  context?.selectedVehicle ||
  context?.contextState?.selectedVehicle ||
  context?.aciContextState?.selectedVehicle ||
  context?.anchors?.primaryVehicle ||
  {};

const selectedVehicleLabel = (vehicle = {}) =>
  cleanText(
    vehicle.fullModel ||
      [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" ") ||
      vehicle.model ||
      "",
  );

const detectDirectFactLookup = (message = "") => {
  if (/\bairbags?\b/i.test(message)) {
    return {
      tool: "vehicle_feature_lookup",
      intent: "vehicle_feature_answer",
      feature: "airbags",
      output: {
        inlineType: "feature_answer_card",
      },
    };
  }

  if (/\bventilated\s+seats?\b/i.test(message)) {
    return {
      tool: "vehicle_feature_lookup",
      intent: "vehicle_feature_answer",
      feature: "ventilated seats",
      output: {
        inlineType: "feature_answer_card",
      },
    };
  }

  if (/\bsunroof\b/i.test(message)) {
    return {
      tool: "vehicle_feature_lookup",
      intent: "vehicle_feature_answer",
      feature: "sunroof",
      output: {
        inlineType: "feature_answer_card",
      },
    };
  }

  if (/\bmileage\b/i.test(message)) {
    return {
      tool: "vehicle_spec_attribute_lookup",
      intent: "vehicle_spec_attribute_answer",
      attributeKey: "mileage",
      attributeLabel: "mileage",
      output: {
        inlineType: "spec_attribute_answer_card",
      },
    };
  }

  if (/\bboot\s+space\b/i.test(message)) {
    return {
      tool: "vehicle_spec_attribute_lookup",
      intent: "vehicle_spec_attribute_answer",
      attributeKey: "boot_space",
      attributeLabel: "boot space",
      output: {
        inlineType: "spec_attribute_answer_card",
      },
    };
  }

  return null;
};

const detectBatch2FeatureLookup = (message = "") => {
  if (/\bventilated\s+seats?\b/i.test(message)) return { feature: "ventilated seats" };
  if (/\b6\s*airbags?|six\s+airbags?\b/i.test(message)) return { feature: "6 airbags" };
  if (/\bairbags?\b/i.test(message)) return { feature: "airbags" };
  if (/\bsunroof\b/i.test(message)) return { feature: "sunroof" };
  if (/\brear\s+(?:camera|parking\s+camera|view\s+camera)\b/i.test(message)) return { feature: "rear camera" };
  if (/\brear\s+ac\s+vents?\b/i.test(message)) return { feature: "rear ac vents" };
  if (/\badas\b/i.test(message)) return { feature: "ADAS" };
  if (/\bsafety\s+features?\b/i.test(message)) return { feature: "", category: "safety", summary: true };
  if (/\bfeatures?\b/i.test(message)) return { feature: "", category: "", summary: true };
  return null;
};

const detectBatch2SpecLookup = (message = "") => {
  if (/\bengine\s+(?:cc|capacity|displacement)\b|\bdisplacement\b/i.test(message)) {
    return { attributeKey: "engine_displacement", attributeLabel: "engine cc" };
  }
  if (/\bpower\b|\bbhp\b|\bmax\s+power\b/i.test(message)) {
    return { attributeKey: "power", attributeLabel: "power" };
  }
  if (/\btorque\b/i.test(message)) {
    return { attributeKey: "torque", attributeLabel: "torque" };
  }
  if (/\bmileage\b|\bfuel\s+efficiency\b/i.test(message)) {
    return { attributeKey: "mileage", attributeLabel: "mileage" };
  }
  if (/\bboot\s+space\b/i.test(message)) {
    return { attributeKey: "boot_space", attributeLabel: "boot space" };
  }
  if (/\bground\s+clearance\b/i.test(message)) {
    return { attributeKey: "ground_clearance", attributeLabel: "ground clearance" };
  }
  return null;
};

const getBatch2VehicleFromContextOrCandidates = async ({ contextState = {}, context = {}, candidateSnapshot = {} } = {}) => {
  const stateVehicle =
    contextState?.selectedVehicle ||
    contextState?.anchors?.primaryVehicle ||
    context?.selectedVehicle ||
    {};
  const variantCandidate = getScoreVariantCandidateFromSnapshot(candidateSnapshot);
  const modelCandidate = getScoreModelCandidateFromSnapshot(candidateSnapshot);
  const hasExplicitVariantModel = Boolean(variantCandidate.model || variantCandidate.fullModel);
  const explicitVehicle = (hasExplicitVariantModel && variantCandidate.variant) || modelCandidate.model || modelCandidate.fullModel
    ? {
        ...modelCandidate,
        ...(hasExplicitVariantModel ? variantCandidate : {}),
      }
    : {};

  const aliasVehicle =
    (await resolveVehicleAlias({ message: candidateSnapshot?.rawMessage || "" }).catch(() => null)) || {};

  return {
    ...stateVehicle,
    ...aliasVehicle,
    ...explicitVehicle,
    make: explicitVehicle.make || aliasVehicle.make || stateVehicle.make || stateVehicle.brand || "",
    model: explicitVehicle.model || aliasVehicle.model || stateVehicle.model || "",
    fullModel:
      explicitVehicle.fullModel ||
      aliasVehicle.fullModel ||
      stateVehicle.fullModel ||
      [
        explicitVehicle.make || aliasVehicle.make || stateVehicle.make || stateVehicle.brand,
        explicitVehicle.model || aliasVehicle.model || stateVehicle.model,
      ]
        .filter(Boolean)
        .join(" "),
    variant: explicitVehicle.variant || explicitVehicle.variantName || stateVehicle.variant || stateVehicle.variantName || "",
    variantName: explicitVehicle.variantName || explicitVehicle.variant || stateVehicle.variantName || stateVehicle.variant || "",
    variantKey: explicitVehicle.variantKey || stateVehicle.variantKey || "",
  };
};

const maybeReturnDeterministicFeatureSpecFastPath = async ({
  message = "",
  context = {},
  contextState = {},
  candidateSnapshot = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  if (hasComparisonLanguage(message)) return null;

  const featureLookup = detectBatch2FeatureLookup(message);
  const specLookup = featureLookup ? null : detectBatch2SpecLookup(message);
  if (!featureLookup && !specLookup) return null;

  const vehicle = await getBatch2VehicleFromContextOrCandidates({ contextState, context, candidateSnapshot });
  const model = cleanText(vehicle.model || selectedVehicleLabel(vehicle));
  const fullModel = selectedVehicleLabel(vehicle) || model;
  if (!model) return null;

  const variant = vehicle.variant || vehicle.variantName || "";
  const city = vehicle.citySlug || vehicle.city || context.anchorCity || "new-delhi";
  const tool = featureLookup ? "vehicle_feature_lookup" : "vehicle_spec_attribute_lookup";
  const intent = featureLookup ? "vehicle_feature_answer" : "vehicle_spec_attribute_answer";
  const feature = featureLookup?.feature || "";
  const category = featureLookup?.category || "";
  const featureSummary = Boolean(featureLookup?.summary);
  const attributeKey = specLookup?.attributeKey || "";
  const attributeLabel = specLookup?.attributeLabel || "";

  const payload = {
    ...(featureSummary ? { intent: "vehicle_model_features_explorer" } : {}),
    message,
    query: message,
    model,
    fullModel,
    make: vehicle.make || vehicle.brand || "",
    variant,
    variantName: variant,
    city,
    ...(feature ? { feature, features: [feature], topic: feature } : {}),
    ...(category ? { category, categoryKey: category } : {}),
    ...(attributeKey ? { attributeKey, attributeLabel, topic: attributeLabel } : {}),
  };

  const toolPlan = {
    tool,
    ...(featureSummary ? { intent: "vehicle_model_features_explorer" } : {}),
    input: payload,
    args: payload,
    params: payload,
    entities: {
      ...payload,
      primaryModel: model,
      primaryMake: payload.make,
      primaryVariant: variant,
    },
    filters: {
      model,
      city,
      ...(variant ? { variant } : {}),
      ...(feature ? { feature, mustHaveFeatures: [feature] } : {}),
      ...(category ? { category, categoryKey: category } : {}),
      ...(attributeKey ? { attributeKey, attributeLabel } : {}),
    },
    output: {
      canvasType: null,
      inlineType: featureLookup ? "feature_answer_card" : "spec_attribute_answer_card",
    },
  };

  const plan = {
    intent,
    mode: "single_tool",
    conversationMode: "direct_answer",
    tools: [toolPlan],
  };

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context: getContextForToolPlan(contextState),
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context: getContextForToolPlan(contextState),
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: featureLookup ? "feature_lookup" : "spec_attribute_lookup",
    tool,
    planMode: "single_tool",
    contextIsolation: "deterministic_feature_spec_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    routingReason: featureLookup ? "deterministic_feature_lookup" : "deterministic_spec_attribute_lookup",
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

const maybeReturnContextDirectFactFastPath = async ({
  message = "",
  context = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  const lookup = detectDirectFactLookup(message);
  if (!lookup) return null;
  if (hasComparisonLanguage(message)) return null;

  const vehicle = getSelectedVehicleFromContext(context);
  const model = selectedVehicleLabel(vehicle);
  if (!model) return null;

  const city = vehicle.citySlug || vehicle.city || context.anchorCity || "new-delhi";
  const toolPlan = {
    tool: lookup.tool,
    input: {
      message,
      query: message,
      model,
      ...(lookup.feature ? { feature: lookup.feature, features: [lookup.feature] } : {}),
      ...(lookup.attributeKey ? { attributeKey: lookup.attributeKey, attributeLabel: lookup.attributeLabel } : {}),
    },
    entities: {
      model,
      primaryModel: model,
      ...(lookup.feature ? { feature: lookup.feature, features: [lookup.feature], topic: lookup.feature } : {}),
      ...(lookup.attributeKey ? { attributeKey: lookup.attributeKey, attributeLabel: lookup.attributeLabel } : {}),
    },
    filters: {
      model,
      city,
      ...(lookup.feature ? { mustHaveFeatures: [lookup.feature] } : {}),
      ...(lookup.attributeKey ? { attributeKey: lookup.attributeKey, attributeLabel: lookup.attributeLabel } : {}),
    },
    output: {
      canvasType: null,
      inlineType: lookup.output.inlineType,
    },
  };

  const plan = {
    intent: lookup.intent,
    mode: "single_tool",
    conversationMode: "direct_answer",
    tools: [toolPlan],
  };

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context,
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context,
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: lookup.tool,
    tool: lookup.tool,
    planMode: "single_tool",
    contextIsolation: "context_direct_fact_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    routingReason: "context_direct_fact_follow_up",
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

const maybeReturnActiveComparisonFollowUpFastPath = async ({
  message = "",
  context = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  if (!hasActiveComparisonFollowUp({ message, context })) return null;

  const vehicles = getActiveComparisonVehiclesFromContext(context);
  if (vehicles.length < 2) return null;

  const targets = vehicles.slice(0, 2).map((vehicle = {}) => ({
    make: vehicle.make || vehicle.brand || "",
    brand: vehicle.brand || vehicle.make || "",
    model: vehicle.model || "",
    fullModel: vehicleLabelForComparison(vehicle),
    variant: vehicle.variant || vehicle.variantName || "",
    variantName: vehicle.variantName || vehicle.variant || "",
    city: vehicle.city || vehicle.citySlug || getComparisonCityFromContext(context),
  }));

  if (targets.filter((target) => target.model || target.fullModel).length < 2) return null;

  const city = getComparisonCityFromContext(context);
  const toolPlan = {
    tool: "vehicle_compare",
    input: {
      message,
      query: message,
    },
    entities: {
      comparisonVehicles: pruneSubstringComparisonTargets({ vehicles: targets, message }),
      models: targets.map((target) => target.fullModel || target.model).filter(Boolean),
    },
    filters: {
      city,
    },
    output: {
      canvasType: "comparison_canvas",
      inlineType: "",
    },
    resolution: {
      comparisonLevel: "model",
      selectedComparisonVehicles: pruneSubstringComparisonTargets({ vehicles: targets, message }),
    },
    contextPatch: {
      activeComparison: {
        vehicles: targets,
        city,
      },
      selectedComparisonSet: {
        vehicles: targets,
      },
    },
  };

  const plan = {
    intent: "vehicle_comparison",
    mode: "single_tool",
    tools: [toolPlan],
    output: {
      canvasType: "comparison_canvas",
    },
  };

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context,
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context,
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "vehicle_comparison",
    tool: "vehicle_compare",
    planMode: "single_tool",
    contextIsolation: "active_comparison_follow_up_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    routingReason: "active_comparison_follow_up",
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


const parseBudgetRupeesFromBuyerMessage = (message = "") => {
  const source = String(message || "").toLowerCase();

  const lakhMatch = source.match(/\b(?:under|below|upto|up to|within|budget(?:\s+of)?|less than)?\s*(\d+(?:\.\d+)?)\s*(?:lakh|lakhs|lac|lacs|l)\b/i);
  if (lakhMatch) return Math.round(Number(lakhMatch[1]) * 100000);

  const croreMatch = source.match(/\b(?:under|below|upto|up to|within|budget(?:\s+of)?|less than)?\s*(\d+(?:\.\d+)?)\s*(?:crore|crores|cr)\b/i);
  if (croreMatch) return Math.round(Number(croreMatch[1]) * 10000000);

  return 0;
};

const detectBatch4BroadDiscoveryRequest = (message = "") => {
  const normalized = String(message || "").toLowerCase();
  const budgetMax = parseBudgetRupeesFromBuyerMessage(message);
  const hasBudget = budgetMax > 0;
  const hasBroadCar = /\b(cars?|vehicles?|models?|options?|suvs?|sedans?|hatchbacks?|mpvs?|muvs?)\b/i.test(normalized);
  const wantsFamily = /\bfamily\b|\bpractical\b|\bspacious\b|\bspace\b/i.test(normalized);
  const wantsElectric = /\belectric\b|\bev\b/i.test(normalized);
  const wantsSuv = /\bsuvs?\b/i.test(normalized);
  const wantsAutomatic = /\bautomatic\b|\bauto\b|\bat\b|\bdct\b|\bcvt\b|\bamt\b/i.test(normalized);

  if (!hasBudget || (!hasBroadCar && !wantsFamily && !wantsElectric && !wantsSuv && !wantsAutomatic)) {
    return null;
  }

  if (/\b(price|on road|on-road|insurance|service cost|service|offer|discount)\b/i.test(normalized)) {
    return null;
  }

  return {
    budgetMax,
    bodyType: wantsSuv ? "suv" : "",
    fuelType: wantsElectric ? "electric" : "",
    transmission: wantsAutomatic ? "automatic" : "",
    buyerUseCase: wantsFamily ? "family" : "",
    reason: wantsFamily
      ? "family_budget_discovery"
      : wantsElectric || wantsSuv || wantsAutomatic
        ? "filtered_budget_discovery"
        : "budget_discovery",
  };
};

const detectBatch4FuelAdviceRequest = (message = "") => {
  const normalized = String(message || "").toLowerCase();

  if (/\bservice\b|\binsurance\b|\boffers?\b|\bdiscount\b|\bprice\b|\bon road\b|\bon-road\b/i.test(normalized)) {
    return null;
  }

  if (
    /\bpetrol\s+or\s+diesel\b|\bdiesel\s+or\s+petrol\b|\bfuel\s+should\s+i\s+choose\b|\bwhat\s+fuel\b|\bwhich\s+fuel\b|\bdiesel\s+worth\b|\b\d+\s*km\s+daily\b|\bdaily\s+running\b/i.test(normalized)
  ) {
    return {
      reason: "fuel_choice_guidance",
    };
  }

  return null;
};

const detectBatch4PendingModuleRequest = (message = "") => {
  const normalized = String(message || "").toLowerCase();

  if (/\b(wait\s+for\s+discount|discount|offers?|deal|benefit)\b/i.test(normalized)) {
    return {
      unavailableReason: "offers_discount_data_not_available",
      topic: "discount or offer",
    };
  }

  if (/\bservice\s+(cost|costs|price|prices|estimate|maintenance)\b|\bmaintenance\s+cost\b/i.test(normalized)) {
    return {
      unavailableReason: "service_cost_not_available",
      topic: "service cost",
    };
  }

  if (/\binsurance\b.*\b(price|prices|cost|premium|quote|estimate)\b|\b(price|cost|premium|quote|estimate)\b.*\binsurance\b/i.test(normalized)) {
    return {
      unavailableReason: "insurance_price_not_available",
      topic: "insurance price",
    };
  }

  return null;
};


const findVehicleLabelFromMessage = async (message = "") => {
  const aliasVehicle = (await resolveVehicleAlias({ message }).catch(() => null)) || {};
  const aliasLabel = cleanText(
    aliasVehicle.fullModel ||
      [aliasVehicle.make || aliasVehicle.brand, aliasVehicle.model].filter(Boolean).join(" ") ||
      aliasVehicle.model ||
      "",
  );
  if (aliasLabel) return aliasLabel;

  const db = getFastPathDb();
  if (!db) return "";

  const normalized = normalizeFastPathText(message);
  if (!normalized) return "";

  const rows = await db.collection("aci_vehicle_model_summary")
    .find(
      {},
      {
        projection: {
          make: 1,
          brand: 1,
          model: 1,
          fullModel: 1,
          modelKey: 1,
        },
      },
    )
    .limit(3000)
    .toArray()
    .catch(() => []);

  const matches = rows
    .map((row = {}) => {
      const label = cleanText(row.fullModel || [row.make || row.brand, row.model].filter(Boolean).join(" ") || row.model || "");
      const modelText = normalizeFastPathText(row.model || row.modelKey || label);
      const fullText = normalizeFastPathText(label);
      const hit = modelText && new RegExp(`(^|\\\\s)${escapeFastPathRegex(modelText)}($|\\\\s)`, "i").test(normalized)
        ? modelText.length
        : fullText && new RegExp(`(^|\\\\s)${escapeFastPathRegex(fullText)}($|\\\\s)`, "i").test(normalized)
          ? fullText.length
          : 0;
      return { row, label, hit };
    })
    .filter((item) => item.hit > 0)
    .sort((a, b) => b.hit - a.hit);

  return matches[0]?.label || "";
};



const buildBatch4VehicleTokenRegexes = (message = "") => {
  const stopWords = new Set([
    "are", "there", "offer", "offers", "discount", "discounts", "deal", "deals",
    "service", "cost", "costs", "insurance", "price", "premium", "quote",
    "should", "wait", "for", "of", "on", "the", "a", "an", "in", "me", "my",
    "what", "which", "is", "it", "now", "live", "verified",
  ]);

  const tokens = normalizeFastPathText(message)
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length >= 3 && !stopWords.has(token))
    .slice(0, 6);

  return tokens.map((token) => new RegExp(`(^|\\s|-)${escapeFastPathRegex(token)}($|\\s|-)`, "i"));
};

const resolveBatch4VehicleLabel = async ({ message = "", context = {} } = {}) => {
  const contextVehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    {};

  const contextLabel = cleanText(
    contextVehicle.fullModel ||
      [contextVehicle.make || contextVehicle.brand, contextVehicle.model].filter(Boolean).join(" ") ||
      contextVehicle.model ||
      "",
  );
  if (contextLabel) return contextLabel;

  const regexes = buildBatch4VehicleTokenRegexes(message);
  if (!regexes.length) return "";

  const db = getFastPathDb();
  if (!db) return "";

  const query = {
    $or: [
      { model: { $in: regexes } },
      { fullModel: { $in: regexes } },
      { displayName: { $in: regexes } },
      { modelKey: { $in: regexes } },
      { shortModelKey: { $in: regexes } },
    ],
  };

  const projection = {
    make: 1,
    brand: 1,
    model: 1,
    fullModel: 1,
    displayName: 1,
    modelKey: 1,
    shortModelKey: 1,
  };

  const collections = [
    "aci_vehicle_model_summary",
    "aci_vehicle_price_rows",
    "vehicle_variant_feature_matrix_v2",
  ];

  const matches = [];

  for (const name of collections) {
    const exists = await db.listCollections({ name }).hasNext().catch(() => false);
    if (!exists) continue;

    const rows = await db.collection(name)
      .find(query, { projection })
      .limit(20)
      .toArray()
      .catch(() => []);

    for (const row of rows) {
      const label = cleanText(
        row.fullModel ||
          row.displayName ||
          [row.make || row.brand, row.model].filter(Boolean).join(" ") ||
          row.model ||
          "",
      );
      if (!label) continue;

      const labelKey = normalizeFastPathText(label);
      const modelKey = normalizeFastPathText(row.model || row.modelKey || row.shortModelKey || "");
      const messageKey = normalizeFastPathText(message);

      const messageTokens = new Set(messageKey.split(" ").filter(Boolean));
      const rowModelKey = normalizeFastPathText(row.model || "");
      const rowCanonicalKey = normalizeFastPathText(row.modelKey || row.shortModelKey || "");
      const messageRequestsElectric = /(^|\\s)(electric|ev)($|\\s)/i.test(messageKey);
      const rowLooksElectric = /(^|\\s)(electric|ev)($|\\s)/i.test(`${labelKey} ${rowModelKey} ${rowCanonicalKey}`);

      const exactModelTokenHit =
        (rowModelKey && messageTokens.has(rowModelKey)) ||
        (rowCanonicalKey && messageTokens.has(rowCanonicalKey));

      const fullPhraseHit =
        labelKey && new RegExp(`(^|\\s)${escapeFastPathRegex(labelKey)}($|\\s)`, "i").test(messageKey);

      const partialModelHit =
        rowModelKey && new RegExp(`(^|\\s)${escapeFastPathRegex(rowModelKey)}($|\\s)`, "i").test(messageKey);

      const electricPenalty = !messageRequestsElectric && rowLooksElectric ? 5000 : 0;

      const score =
        (exactModelTokenHit ? 10000 : 0) +
        (fullPhraseHit ? 3000 + labelKey.length : 0) +
        (partialModelHit ? 1000 + rowModelKey.length : 0) +
        normalizeFastPathText(row.model || "").length -
        electricPenalty;

      matches.push({ label, score });
    }

    if (matches.length) break;
  }

  matches.sort((left, right) => right.score - left.score || right.label.length - left.label.length);
  return matches[0]?.label || "";
};

const buildBatch4ExplainerFastPathResponse = async ({
  message = "",
  originalMessage = "",
  effectiveMessage = "",
  context = {},
  startedAt = 0,
  kind = "",
  unavailableReason = "",
  topic = "",
} = {}) => {
  const vehicleLabel =
    kind === "fuel_advice"
      ? ""
      : await resolveBatch4VehicleLabel({
          message: effectiveMessage || message,
          context,
        });

  const title =
    kind === "fuel_advice"
      ? "Fuel choice guidance"
      : "Not available yet";

  const answer =
    kind === "fuel_advice"
      ? "For fuel choice, use your running pattern first: petrol usually suits lower running and simpler city use, diesel starts making sense only with high monthly highway/running needs, CNG can work for very high daily use if boot-space compromise is acceptable, and EV depends on charging access. For 100 km daily running, compare monthly fuel cost, registration rules, maintenance, resale, and the exact car shortlist before deciding."
      : topic === "insurance price"
        ? `I do not have verified live insurance premium data${vehicleLabel ? ` for ${vehicleLabel}` : ""} yet. Insurance depends on IDV, insurer, add-ons, NCB, registration city, and coverage type, so I should not show a made-up figure.`
        : topic === "discount or offer"
          ? `I do not have verified live discount or offer data${vehicleLabel ? ` for ${vehicleLabel}` : ""} yet. Offers change by city, dealer stock, variant, and month, so I should not invent a discount.`
          : `I do not have verified service-cost data${vehicleLabel ? ` for ${vehicleLabel}` : ""} yet. I can explain ownership cost generally, but I should not invent exact service figures.`;

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "vehicle_explainer",
    tool: "vehicle_explainer",
    planMode: "single_tool",
    contextIsolation: kind === "fuel_advice" ? "batch4_fuel_advice_fast_path" : "batch4_pending_module_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: effectiveMessage || message,
    routingReason: kind === "fuel_advice" ? "fuel_advice_request" : unavailableReason || "pending_module_request",
  };

  return {
    intent: kind === "fuel_advice" ? "vehicle_explainer" : "unavailable",
    displayMode: "inline",
    canvasType: "",
    inlineType: kind === "fuel_advice" ? "explainer_card" : "unavailable_notice",
    title,
    answer,
    matched: 0,
    count: 0,
    rows: [],
    items: [],
    data: {
      title,
      answer,
      rows: [],
      items: [],
      ...(vehicleLabel ? { model: vehicleLabel, fullModel: vehicleLabel } : {}),
      ...(unavailableReason ? { unavailableReason } : {}),
    },
    sourceTransparency: {
      modulesChecked: ["vehicle_explainer"],
      matched: 0,
      dataSource: kind === "fuel_advice" ? "deterministic_fuel_advice" : "pending_module_guardrail",
      recordCount: 0,
    },
    aciCoreBridge: bridge,
    meta: {
      aciCoreBridge: bridge,
    },
  };
};


const maybeReturnBatch4BareClarificationFastPath = ({
  message = "",
  context = {},
  originalMessage = "",
  effectiveMessage = "",
  startedAt = 0,
} = {}) => {
  const normalized = normalizeFastPathText(effectiveMessage || message);

  if (!/^(price|sunroof|mileage|colors|colours|which one is better|compare these|best car|recommend me a car)$/.test(normalized)) {
    return null;
  }

  const selectedVehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    {};

  if (normalized === "price" && (selectedVehicle.model || selectedVehicle.fullModel || selectedVehicle.modelKey)) {
    return null;
  }

  const answer =
    normalized === "price"
      ? "Which car and city should I check the price for?"
      : "Which car should I check this for?";

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "clarification",
    tool: "clarification",
    planMode: "clarification",
    contextIsolation: "bare_query_clarification_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: effectiveMessage || message,
    routingReason: "bare_ambiguous_query",
  };

  return {
    intent: "clarification",
    tool: "clarification",
    displayMode: "inline",
    canvasType: "",
    inlineType: "clarification",
    title: "Need one detail",
    answer,
    rows: [],
    items: [],
    matched: 0,
    count: 0,
    data: {
      title: "Need one detail",
      answer,
      rows: [],
      items: [],
    },
    aciCoreBridge: bridge,
    meta: {
      aciCoreBridge: bridge,
    },
  };
};


const maybeReturnBatch4BroadDiscoveryFastPath = async ({
  message = "",
  context = {},
  contextState = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  effectiveMessage = "",
  startedAt = 0,
} = {}) => {
  const discovery = detectBatch4BroadDiscoveryRequest(effectiveMessage || message);
  if (!discovery) return null;

  const evRequested = discovery.fuelType === "electric";
  const fuelKey = evRequested ? "ev" : discovery.fuelType;

  const filters = {
    budgetMax: discovery.budgetMax,
    maxBudget: discovery.budgetMax,
    maxPrice: discovery.budgetMax,
    maxExShowroomPrice: discovery.budgetMax,
    budgetMaxLakh: Math.round(discovery.budgetMax / 100000),
    ...(discovery.transmission ? {
      transmission: discovery.transmission,
      transmissionType: discovery.transmission,
      transmissionKey: discovery.transmission,
    } : {}),
    ...(fuelKey ? {
      fuelType: evRequested ? "electric" : fuelKey,
      fuel: evRequested ? "electric" : fuelKey,
      fuelKey,
      requestedFuelType: discovery.fuelType,
      requestedFuelLabel: evRequested ? "Electric" : discovery.fuelType,
    } : {}),
    ...(discovery.bodyType ? {
      bodyType: discovery.bodyType,
      bodyStyle: discovery.bodyType,
      bodyTypeKey: discovery.bodyType,
    } : {}),
    ...(discovery.buyerUseCase ? {
      buyerUseCase: discovery.buyerUseCase,
      useCase: discovery.buyerUseCase,
      buyerIntent: discovery.buyerUseCase,
    } : {}),
  };

  const toolPlan = {
    tool: "vehicle_recommend",
    input: {
      message,
      query: message,
      ...filters,
    },
    args: {
      message,
      query: message,
      ...filters,
    },
    params: {
      message,
      query: message,
      ...filters,
    },
    entities: {
      ...filters,
    },
    filters,
    output: {
      canvasType: "recommendation_results_canvas",
      inlineType: "recommendation_summary",
    },
  };

  const plan = {
    intent: "vehicle_recommendation",
    mode: "single_tool",
    conversationMode: "direct_answer",
    tools: [toolPlan],
    output: {
      canvasType: "recommendation_results_canvas",
    },
  };

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context: getContextForToolPlan(contextState || {}),
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context: getContextForToolPlan(contextState || {}),
  });

  if (evRequested) {
    const currentTitle = cleanText(normalized.title || normalized.data?.title || "Best SUV cars under budget");
    const electricTitle = /electric|\bev\b/i.test(currentTitle)
      ? currentTitle
      : currentTitle.replace(/^Best\s+/i, "Best Electric ");

    normalized.title = electricTitle;

    const currentAnswer = cleanText(normalized.answer || "");
    normalized.answer = /electric|\bev\b/i.test(currentAnswer)
      ? currentAnswer
      : /^I found\s+(\d+)\s+/i.test(currentAnswer)
        ? currentAnswer.replace(/^I found\s+(\d+)\s+/i, "I found $1 electric ")
        : `For electric SUV under budget: ${currentAnswer}`;

    if (normalized.data && typeof normalized.data === "object") {
      normalized.data.title = normalized.title;
      normalized.data.answer = normalized.answer;
      normalized.data.requestedFuelType = "electric";
      normalized.data.requestedFuelKey = "ev";
    }
  }

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "vehicle_recommendation",
    tool: "vehicle_recommend",
    planMode: "single_tool",
    contextIsolation: "broad_discovery_without_model",
    originalMessage: originalMessage || message,
    effectiveMessage: effectiveMessage || message,
    routingReason: discovery.reason,
  };

  const composed = composeAciAnswer({
    ...normalized,
    intent: normalized.intent || "vehicle_recommendation",
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: bridge,
    },
  });

  if (evRequested) {
    const title = cleanText(composed.title || composed.data?.title || normalized.title || "");
    const answer = cleanText(composed.answer || normalized.answer || "");

    if (title && !/electric|\\bev\\b/i.test(title)) {
      composed.title = title.replace(/^Best\\s+/i, "Best Electric ");
    }

    if (answer && !/electric|\bev\b/i.test(answer)) {
      const withElectricScope = answer
        .replace(/suv models/i, "electric SUV models")
        .replace(/suv cars/i, "electric SUV cars")
        .replace(/models under/i, "electric SUV models under");

      composed.answer = /electric|\bev\b/i.test(withElectricScope)
        ? withElectricScope
        : `For electric SUV under budget: ${answer}`;
    }

    if (composed.answer) {
      composed.answer = String(composed.answer)
        .replace(/electric\s+suv\s+electric\s+suv\s+models/ig, "electric SUV models")
        .replace(/electric\s+suv\s+electric\s+suv\s+cars/ig, "electric SUV cars");
    }

    if (composed.data && typeof composed.data === "object") {
      composed.data.title = composed.title || title;
      composed.data.answer = composed.answer || answer;
      composed.data.requestedFuelType = "electric";
      composed.data.requestedFuelKey = "ev";
      composed.data.requestedBodyType = "suv";
    }
  }

  return composed;
};

const maybeReturnBatch4ExplainerFastPath = async ({
  message = "",
  context = {},
  originalMessage = "",
  effectiveMessage = "",
  startedAt = 0,
} = {}) => {
  const pending = detectBatch4PendingModuleRequest(effectiveMessage || message);
  if (pending) {
    return await buildBatch4ExplainerFastPathResponse({
      message,
      originalMessage,
      effectiveMessage,
      context,
      startedAt,
      kind: "pending_module",
      unavailableReason: pending.unavailableReason,
      topic: pending.topic,
    });
  }

  const fuelAdvice = detectBatch4FuelAdviceRequest(effectiveMessage || message);
  if (fuelAdvice) {
    return await buildBatch4ExplainerFastPathResponse({
      message,
      originalMessage,
      effectiveMessage,
      context,
      startedAt,
      kind: "fuel_advice",
    });
  }

  return null;
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


const normalizeComparisonModelText = (value = "") =>
  normalizeFastPathText(value).replace(/-/g, " ").replace(/\s+/g, " ").trim();

const getComparisonModelLabel = (vehicle = {}) =>
  cleanText(
    vehicle.fullModel ||
      [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" ") ||
      vehicle.displayName ||
      vehicle.model ||
      "",
  );

const getComparisonModelKeyParts = (vehicle = {}) => {
  const label = normalizeComparisonModelText(getComparisonModelLabel(vehicle));
  const model = normalizeComparisonModelText(vehicle.model || "");
  const modelKey = normalizeComparisonModelText(vehicle.modelKey || "");
  const shortModelKey = normalizeComparisonModelText(vehicle.shortModelKey || "");
  return [...new Set([label, model, modelKey, shortModelKey].filter(Boolean))];
};

const getComparisonPruningKeyParts = (vehicle = {}) => {
  const parts = getComparisonModelKeyParts(vehicle);
  const looseBaseParts = [];

  parts.forEach((part) => {
    const tokens = part.split(/\s+/).filter(Boolean);
    const suffix = tokens[tokens.length - 1] || "";
    if (tokens.length >= 2 && suffix.length <= 2) {
      looseBaseParts.push(tokens.slice(0, -1).join(" "));
      looseBaseParts.push(tokens[tokens.length - 2]);
    }
  });

  return [...new Set([...parts, ...looseBaseParts].filter(Boolean))];
};

const findComparisonPhraseSpans = ({ messageTokens = [], phrase = "" } = {}) => {
  const phraseTokens = normalizeComparisonModelText(phrase).split(/\s+/).filter(Boolean);
  if (!messageTokens.length || !phraseTokens.length || phraseTokens.length > messageTokens.length) return [];

  const spans = [];
  for (let index = 0; index <= messageTokens.length - phraseTokens.length; index += 1) {
    const matches = phraseTokens.every((token, offset) => messageTokens[index + offset] === token);
    if (matches) {
      spans.push({
        start: index,
        end: index + phraseTokens.length - 1,
        length: phraseTokens.length,
      });
    }
  }
  return spans;
};

const isSpanCoveredByLongerSpan = ({ span, longerSpans = [] } = {}) =>
  longerSpans.some((other) =>
    other.length > span.length &&
    other.start <= span.start &&
    other.end >= span.end,
  );

const hasExplicitUncoveredComparisonMention = ({ messageTokens = [], candidateParts = [], longerExplicitSpans = [] } = {}) =>
  candidateParts.some((part) =>
    findComparisonPhraseSpans({ messageTokens, phrase: part }).some((span) =>
      !isSpanCoveredByLongerSpan({ span, longerSpans: longerExplicitSpans }),
    ),
  );

const pruneSubstringComparisonTargets = ({ vehicles = [], message = "" } = {}) => {
  const list = asArray(vehicles).filter(Boolean);
  if (list.length <= 2) return list;

  const messageTokens = normalizeComparisonModelText(message).split(/\s+/).filter(Boolean);
  if (!messageTokens.length) return list;

  const partsByIndex = list.map((vehicle) => getComparisonModelKeyParts(vehicle));
  const pruningPartsByIndex = list.map((vehicle) => getComparisonPruningKeyParts(vehicle));

  const allExplicitSpans = [];
  pruningPartsByIndex.forEach((parts, index) => {
    parts.forEach((part) => {
      findComparisonPhraseSpans({ messageTokens, phrase: part }).forEach((span) => {
        allExplicitSpans.push({ ...span, index, part });
      });
    });
  });

  return list.filter((candidate, candidateIndex) => {
    const candidateParts = pruningPartsByIndex[candidateIndex] || [];
    if (!candidateParts.length) return true;

    const candidateHasUncoveredExplicitMention = hasExplicitUncoveredComparisonMention({
      messageTokens,
      candidateParts,
      longerExplicitSpans: allExplicitSpans,
    });

    // Keep if the user explicitly mentions this model outside a longer model phrase.
    // Example: "Scorpio N and Scorpio" keeps both.
    if (candidateHasUncoveredExplicitMention) return true;

    const isSubstringOfLongerCandidate = list.some((other, otherIndex) => {
      if (otherIndex === candidateIndex) return false;

      const otherParts = pruningPartsByIndex[otherIndex] || [];
      const otherHasExplicitMention = hasExplicitUncoveredComparisonMention({
        messageTokens,
        candidateParts: otherParts,
        longerExplicitSpans: [],
      }) || otherParts.some((part) => findComparisonPhraseSpans({ messageTokens, phrase: part }).length > 0);

      if (!otherHasExplicitMention) return false;

      return candidateParts.some((candidatePart) =>
        otherParts.some((otherPart) =>
          otherPart !== candidatePart &&
          otherPart.length > candidatePart.length &&
          new RegExp(`(^|\\s)${escapeFastPathRegex(candidatePart)}($|\\s)`, "i").test(otherPart),
        ),
      );
    });

    return !isSubstringOfLongerCandidate;
  });
};

const comparisonVehicleLabelFromTarget = (vehicle = {}) =>
  getComparisonModelLabel(vehicle) || cleanText(vehicle.model || vehicle.fullModel || "");

const comparisonModelEntryFromTarget = (vehicle = {}) => {
  const label = comparisonVehicleLabelFromTarget(vehicle);
  return {
    ...(vehicle.make || vehicle.brand ? { make: vehicle.make || vehicle.brand } : {}),
    ...(vehicle.brand || vehicle.make ? { brand: vehicle.brand || vehicle.make } : {}),
    model: vehicle.model || label,
    ...(label ? { fullModel: label } : {}),
    ...(vehicle.modelKey ? { modelKey: vehicle.modelKey } : {}),
    ...(vehicle.shortModelKey ? { shortModelKey: vehicle.shortModelKey } : {}),
  };
};

const comparisonTargetFromModelValue = (value) => {
  if (value && typeof value === "object") {
    const label = comparisonVehicleLabelFromTarget(value);
    return label ? { ...value, fullModel: value.fullModel || label } : value;
  }

  const label = cleanText(value || "");
  return label ? { model: label, fullModel: label } : null;
};

const comparisonTargetRichScore = (vehicle = {}) =>
  (vehicle.make || vehicle.brand ? 4 : 0) +
  (vehicle.fullModel ? 2 : 0) +
  (vehicle.modelKey ? 1 : 0) +
  (vehicle.shortModelKey ? 1 : 0);

const comparisonTargetLookupKeys = (vehicle = {}) =>
  [
    comparisonVehicleLabelFromTarget(vehicle),
    vehicle.model,
    vehicle.fullModel,
    vehicle.modelKey,
    vehicle.shortModelKey,
  ]
    .map(normalizeComparisonModelText)
    .filter(Boolean);

const collectComparisonTargetSources = ({ toolPlan = {}, rootContextPatch = {} } = {}) => {
  const sources = [];
  const addSource = (items, name) => {
    const targets = asArray(items).map(comparisonTargetFromModelValue).filter(Boolean);
    if (!targets.length) return;
    sources.push({
      name,
      targets,
      richScore: targets.reduce((sum, target) => sum + comparisonTargetRichScore(target), 0),
    });
  };

  addSource(toolPlan.entities?.comparisonVehicles, "entities.comparisonVehicles");
  addSource(toolPlan.resolution?.selectedComparisonVehicles, "resolution.selectedComparisonVehicles");
  addSource(toolPlan.contextPatch?.selectedComparisonSet?.vehicles, "contextPatch.selectedComparisonSet.vehicles");
  addSource(toolPlan.contextPatch?.activeComparison?.vehicles, "contextPatch.activeComparison.vehicles");
  addSource(toolPlan.contextPatch?.contextState?.activeComparison?.vehicles, "contextPatch.contextState.activeComparison.vehicles");
  addSource(toolPlan.contextPatch?.contextState?.anchors?.comparisonTargets, "contextPatch.contextState.anchors.comparisonTargets");
  addSource(toolPlan.contextPatch?.aciContextState?.activeComparison?.vehicles, "contextPatch.aciContextState.activeComparison.vehicles");
  addSource(toolPlan.contextPatch?.aciContextState?.anchors?.comparisonTargets, "contextPatch.aciContextState.anchors.comparisonTargets");
  addSource(rootContextPatch?.activeComparison?.vehicles, "root.contextPatch.activeComparison.vehicles");
  addSource(rootContextPatch?.selectedComparisonSet?.vehicles, "root.contextPatch.selectedComparisonSet.vehicles");
  addSource(rootContextPatch?.contextState?.activeComparison?.vehicles, "root.contextPatch.contextState.activeComparison.vehicles");
  addSource(rootContextPatch?.contextState?.anchors?.comparisonTargets, "root.contextPatch.contextState.anchors.comparisonTargets");
  addSource(rootContextPatch?.aciContextState?.activeComparison?.vehicles, "root.contextPatch.aciContextState.activeComparison.vehicles");
  addSource(rootContextPatch?.aciContextState?.anchors?.comparisonTargets, "root.contextPatch.aciContextState.anchors.comparisonTargets");
  addSource(toolPlan.resolution?.selectedModels, "resolution.selectedModels");
  addSource(toolPlan.filters?.models, "filters.models");
  addSource(toolPlan.entities?.models, "entities.models");
  addSource(toolPlan.entities?.comparisonModels, "entities.comparisonModels");
  addSource(rootContextPatch?.selectedComparisonSet?.models, "root.contextPatch.selectedComparisonSet.models");

  return sources;
};

const chooseComparisonTargetSource = (sources = []) =>
  [...sources].sort((left, right) =>
    right.targets.length - left.targets.length ||
    right.richScore - left.richScore ||
    left.name.localeCompare(right.name),
  )[0] || null;

const buildComparisonTargetLookup = (sources = []) => {
  const lookup = new Map();

  sources
    .flatMap((source) => source.targets)
    .sort((left, right) => comparisonTargetRichScore(right) - comparisonTargetRichScore(left))
    .forEach((target) => {
      comparisonTargetLookupKeys(target).forEach((key) => {
        if (!lookup.has(key)) lookup.set(key, target);
      });
    });

  return lookup;
};

const enrichComparisonTarget = ({ target = {}, lookup = new Map() } = {}) => {
  const match = comparisonTargetLookupKeys(target)
    .map((key) => lookup.get(key))
    .find(Boolean);

  return match ? { ...match } : target;
};

const patchComparisonContextTargets = ({ contextPatch = null, prunedVehicles = [], prunedModels = [] } = {}) => {
  if (!contextPatch || typeof contextPatch !== "object") return contextPatch;

  const patchNestedState = (state = null) => {
    if (!state || typeof state !== "object") return state;
    return {
      ...state,
      ...(state.activeComparison
        ? {
            activeComparison: {
              ...(state.activeComparison || {}),
              vehicles: prunedVehicles,
            },
          }
        : {}),
      ...(state.anchors
        ? {
            anchors: {
              ...(state.anchors || {}),
              comparisonTargets: prunedVehicles,
            },
          }
        : {}),
    };
  };

  return {
    ...contextPatch,
    ...(contextPatch.activeComparison
      ? {
          activeComparison: {
            ...(contextPatch.activeComparison || {}),
            vehicles: prunedVehicles,
          },
        }
      : {}),
    ...(contextPatch.selectedComparisonSet
      ? {
          selectedComparisonSet: {
            ...(contextPatch.selectedComparisonSet || {}),
            vehicles: prunedVehicles,
            models: prunedModels,
          },
        }
      : {}),
    ...(contextPatch.contextState
      ? {
          contextState: patchNestedState(contextPatch.contextState),
        }
      : {}),
    ...(contextPatch.aciContextState
      ? {
          aciContextState: patchNestedState(contextPatch.aciContextState),
        }
      : {}),
  };
};

const sanitizeComparisonTargetsInPlan = ({ plan = {}, message = "" } = {}) => {
  if (!plan || typeof plan !== "object") return plan;

  let rootPrunedVehicles = null;
  let rootPrunedModels = null;

  const nextPlan = {
    ...plan,
    tools: asArray(plan.tools).map((toolPlan = {}) => {
      if (toolPlan.tool !== "vehicle_compare") return toolPlan;

      const targetSources = collectComparisonTargetSources({
        toolPlan,
        rootContextPatch: plan.contextPatch || {},
      });
      const selectedSource = chooseComparisonTargetSource(targetSources);
      const existingVehicles = selectedSource?.targets || [];
      const targetLookup = buildComparisonTargetLookup(targetSources);

      const prunedVehicles = pruneSubstringComparisonTargets({ vehicles: existingVehicles, message })
        .map((target) => enrichComparisonTarget({ target, lookup: targetLookup }));

      if (!existingVehicles.length || prunedVehicles.length === existingVehicles.length) {
        return toolPlan;
      }

      const prunedModels = prunedVehicles.map(comparisonVehicleLabelFromTarget).filter(Boolean);
      const prunedSelectedModels = prunedVehicles.map(comparisonModelEntryFromTarget);
      rootPrunedVehicles = prunedVehicles;
      rootPrunedModels = prunedModels;

      return {
        ...toolPlan,
        entities: {
          ...(toolPlan.entities || {}),
          comparisonVehicles: prunedVehicles,
          models: prunedModels,
          comparisonModels: prunedModels,
        },
        filters: {
          ...(toolPlan.filters || {}),
          models: prunedModels,
        },
        resolution: {
          ...(toolPlan.resolution || {}),
          selectedModels: prunedSelectedModels,
          selectedComparisonVehicles: prunedVehicles,
          comparisonLevel: toolPlan.resolution?.comparisonLevel || "model",
        },
        contextPatch: patchComparisonContextTargets({
          contextPatch: toolPlan.contextPatch || {},
          prunedVehicles,
          prunedModels,
        }),
      };
    }),
  };

  if (!rootPrunedVehicles) return nextPlan;

  return {
    ...nextPlan,
    contextPatch: patchComparisonContextTargets({
      contextPatch: nextPlan.contextPatch || {},
      prunedVehicles: rootPrunedVehicles,
      prunedModels: rootPrunedModels || [],
    }),
  };
};

const sanitizeComparisonContextFromPlan = ({ context = {}, plan = {} } = {}) => {
  const comparisonTool = asArray(plan.tools).find((toolPlan = {}) => toolPlan.tool === "vehicle_compare");
  if (!comparisonTool) return context;

  const targetSources = collectComparisonTargetSources({
    toolPlan: comparisonTool,
    rootContextPatch: plan.contextPatch || {},
  });
  const selectedSource = chooseComparisonTargetSource(targetSources);
  const targets = selectedSource?.targets || [];
  if (targets.length < 2) return context;

  const targetLookup = buildComparisonTargetLookup(targetSources);
  const prunedVehicles = targets.map((target) => enrichComparisonTarget({ target, lookup: targetLookup }));
  const prunedModels = prunedVehicles.map(comparisonVehicleLabelFromTarget).filter(Boolean);

  return patchComparisonContextTargets({
    contextPatch: context || {},
    prunedVehicles,
    prunedModels,
  });
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
  context = {},
  durationMs = 0,
} = {}) => {
  const selectedVehicle = context?.selectedVehicle || context?.contextState?.selectedVehicle || context?.aciContextState?.selectedVehicle || {};
  const vehicleLabel = cleanText(
    selectedVehicle.fullModel ||
      [selectedVehicle.make || selectedVehicle.brand, selectedVehicle.model].filter(Boolean).join(" ") ||
      selectedVehicle.model ||
      "",
  );
  const unsupportedCity = {
    requestedCity,
    ...(vehicleLabel ? { model: vehicleLabel } : {}),
    supportedCities: SUPPORTED_PRICE_CITY_LABELS_FOR_FAST_PATH,
    reason: "pricing_city_not_supported",
    canRetryWithSupportedCity: true,
  };

  return {
    intent: "vehicle_pricelist",
    displayMode: "canvas",
    canvasType: "unsupported_city_canvas",
    inlineType: null,
    title: vehicleLabel
      ? `${vehicleLabel} pricing unavailable in ${requestedCity}`
      : `Pricing unavailable in ${requestedCity}`,
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
  context = {},
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
    context,
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
  "gurgaon", "gurugram", "mumbai", "quote", "quotation", "a", "an",
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
  const modelKeyText = normalizeFastPathText(row.modelKey || row.shortModelKey || "");
  const fullText = normalizeFastPathText(
    [row.make, row.model, row.fullModel, row.displayName, row.modelKey, row.shortModelKey]
      .filter(Boolean)
      .join(" "),
  );
  const modelTokens = new Set(modelText.split(/\s+/).filter(Boolean));
  const fullTokens = new Set(fullText.split(/\s+/).filter(Boolean));

  const tokenNgrams = new Set();
  for (let size = 1; size <= Math.min(5, tokens.length); size += 1) {
    for (let index = 0; index <= tokens.length - size; index += 1) {
      tokenNgrams.add(tokens.slice(index, index + size).join(" "));
    }
  }

  let score = 0;

  for (const token of tokens) {
    if (modelTokens.has(token)) score += 10;
    else if (fullTokens.has(token)) score += 4;
    else if (fullText.includes(token)) score += 1;
  }

  if (modelText && tokenNgrams.has(modelText)) score += 120;
  if (modelKeyText && tokenNgrams.has(modelKeyText)) score += 140;
  if (modelText && vehiclePhrase === modelText) score += 80;
  if (modelKeyText && vehiclePhrase === modelKeyText) score += 90;
  if (modelText && vehiclePhrase.includes(modelText)) score += 40;
  if (modelKeyText && vehiclePhrase.includes(modelKeyText)) score += 45;
  if (makeText && vehiclePhrase.includes(makeText)) score += 4;

  // Penalise model summaries that add extra model words not mentioned by the user
  // e.g. prefer "Creta" over "Creta Electric" for "Creta SX".
  for (const modelToken of modelTokens) {
    if (modelToken && !tokens.includes(modelToken)) score -= 20;
  }

  // If the user typed a longer exact model n-gram, penalise shorter partial models.
  const longestTypedModelNgramLength = Math.max(...[...tokenNgrams].map((value) => value.split(/\s+/).length), 0);
  const modelTokenLength = modelText.split(/\s+/).filter(Boolean).length;
  if (modelTokenLength < longestTypedModelNgramLength && modelText && vehiclePhrase.includes(modelText)) {
    const longerMatchingNgramExists = [...tokenNgrams].some((ngram) => ngram.length > modelText.length && ngram.includes(modelText));
    if (longerMatchingNgramExists) score -= 35;
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
    .filter((token) => token.length >= 1 && !FAST_PRICE_ENTITY_STOP_WORDS.has(token));

  if (!tokens.length) return null;

  const regexes = tokens
    .slice(0, 8)
    .map((token) => new RegExp(`(^|\\b)${escapeFastPathRegex(token)}(\\b|$)`, "i"));

  const modelKeySet = new Set(tokens.map((token) => token.replace(/\s+/g, "-")).filter(Boolean));
  for (let size = 2; size <= Math.min(4, tokens.length); size += 1) {
    for (let index = 0; index <= tokens.length - size; index += 1) {
      modelKeySet.add(tokens.slice(index, index + size).join("-"));
    }
  }
  const modelKeys = [...modelKeySet].filter(Boolean);

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
    (
      rowModel === expectedModel ||
      rowModel.includes(expectedModel)
    );

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

  let resolved = await resolveSupportedExactPriceVehicleFromMessage({
    message: text,
    citySlug,
  });

  if (!resolved?.model) {
    const selectedVehicle =
      context?.selectedVehicle ||
      context?.contextState?.selectedVehicle ||
      context?.aciContextState?.selectedVehicle ||
      {};

    const hasSelectedVehicle =
      selectedVehicle?.model ||
      selectedVehicle?.fullModel ||
      selectedVehicle?.modelKey;

    const isBareOrContextualPrice =
      /^price$/i.test(normalizeFastPathText(text)) ||
      hasContextReference(text);

    if (hasSelectedVehicle && isBareOrContextualPrice) {
      resolved = {
        make: selectedVehicle.make || selectedVehicle.brand || "",
        model: selectedVehicle.model || selectedVehicle.fullModel || "",
        modelKey: selectedVehicle.modelKey || "",
        variant: selectedVehicle.variant || selectedVehicle.variantName || "",
        citySlug:
          normalizeFastPathSlug(selectedVehicle.citySlug || selectedVehicle.city || citySlug) ||
          citySlug,
        vehiclePhrase: selectedVehicle.fullModel || selectedVehicle.model || "",
        score: 100000,
      };
    }
  }

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
    /\b(vs|v\/s|versus|compare|comparison|against|between| or | and )\b/i.test(raw);

  const hasScoreLanguage =
    /\b(score|scores|scoring|overall|diagnostic|which\s+scores?\s+better|scores?\s+better|better\s+overall)\b/i.test(raw);

  if (!hasComparison || !hasScoreLanguage) return null;

  const isSpuriousScoreModuleModelKey = (modelKey = "") => {
    if (modelKey !== "city") return false;
    if (!/\bcity\s+score\b|\bcity\s+use\b|\bfor\s+city\b/i.test(raw)) return false;
    return !/\bhonda\s+city\b|\bcity\s+(?:vs|v\/s|versus|and|or)\b|\b(?:vs|v\/s|versus|and|or)\s+city\b/i.test(raw);
  };

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
    .filter((key) => key && !isSpuriousScoreModuleModelKey(key));

  const contextVehicle = getSelectedVehicleFromContext(
    candidateSnapshot?.activeContext ||
      candidateSnapshot?.context ||
      {},
  );
  const contextModelKey = normalizeFastPathSlug(
    contextVehicle.shortModelKey ||
      contextVehicle.modelKey ||
      contextVehicle.model ||
      contextVehicle.fullModel ||
      "",
  );
  const uniqueModelKeys = [
    ...new Set([
      ...(hasContextReference(raw) && contextModelKey ? [contextModelKey] : []),
      ...modelKeys,
    ]),
  ].slice(0, 2);
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

const isDirectScoreInsightRequest = (message = "") =>
  /\b(score|scores|how good|overall|good|value|worth|value for money|good value)\b/i.test(message) &&
  !/\b(vs|v\/s|versus|compare|comparison|between|against|which one|better between|which\s+has\s+better|scores?\s+better| or | and )\b/i.test(message);

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

const getScoreModelCandidateFromSnapshot = (candidateSnapshot = {}) => {
  const models = asArray(candidateSnapshot?.vehicles?.models);
  const best = models
    .map((item = {}) => {
      const raw = item.metadata?.raw || {};
      return {
        make: item.metadata?.make || raw.make || raw.brand || "",
        model: item.metadata?.model || raw.rawModel || raw.model || item.displayName || "",
        fullModel: raw.fullModel || raw.displayName || item.displayName || [raw.make || raw.brand, raw.model || raw.rawModel].filter(Boolean).join(" "),
        modelKey: raw.modelKey || item.canonicalKey || "",
        shortModelKey: raw.shortModelKey || "",
        confidence: Number(item.confidence || raw.confidence || 0),
      };
    })
    .filter((item) => item.model || item.fullModel)
    .sort((left, right) => right.confidence - left.confidence)[0];

  return best || {};
};

const maybeReturnDirectScoreInsightFastPath = async ({
  message = "",
  context = {},
  contextState = {},
  candidateSnapshot = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  if (!isDirectScoreInsightRequest(message)) return null;

  const variantCandidate = getScoreVariantCandidateFromSnapshot(candidateSnapshot);
  const explicitScorpioN = /\bscorpio\s*n\b/i.test(message);
  const modelCandidate = explicitScorpioN
    ? {
        make: "Mahindra",
        model: "Scorpio N",
        fullModel: "Mahindra Scorpio N",
        modelKey: "mahindra-scorpio-n",
        shortModelKey: "scorpio-n",
        confidence: 1,
      }
    : getScoreModelCandidateFromSnapshot(candidateSnapshot);
  const stateVehicle =
    contextState?.selectedVehicle ||
    contextState?.anchors?.primaryVehicle ||
    context?.selectedVehicle ||
    {};
  const rawVariantToken = normalizeVariantIdentityText(variantCandidate.variant || variantCandidate.variantName);
  const normalizedMessage = normalizeVariantIdentityText(message);
  const hasExplicitVariant =
    !explicitScorpioN &&
    rawVariantToken &&
    rawVariantToken.length >= 2 &&
    new RegExp(`(^|\\s)${rawVariantToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\s|$)`).test(normalizedMessage);
  const hasContextVariant =
    !explicitScorpioN &&
    /\b(this|it|its|same|current|selected)\b/i.test(message) &&
    Boolean(stateVehicle.variantKey || stateVehicle.variant || stateVehicle.variantName);
  const candidateVehicle = hasExplicitVariant
    ? {
        ...modelCandidate,
        ...variantCandidate,
      }
    : modelCandidate;
  const vehicle = {
    ...stateVehicle,
    ...candidateVehicle,
    variant:
      explicitScorpioN
        ? "" :
      (hasExplicitVariant ? (variantCandidate.variant || variantCandidate.variantName) : "") ||
      stateVehicle.variant ||
      stateVehicle.variantName ||
      "",
    variantName:
      explicitScorpioN
        ? "" :
      (hasExplicitVariant ? (variantCandidate.variantName || variantCandidate.variant) : "") ||
      stateVehicle.variantName ||
      stateVehicle.variant ||
      "",
  };

  const modelKey =
    vehicle.shortModelKey ||
    vehicle.modelKey ||
    vehicle.model ||
    vehicle.fullModel ||
    "";
  if (!modelKey) return null;

  const asksValue = /\b(value|worth|value for money|good value)\b/i.test(message);
  const operation = hasExplicitVariant || hasContextVariant
    ? "variant_score_insight"
    : asksValue
      ? "same_family_value_insights"
      : "model_score_insights";

  const payload = {
    operation,
    modelKey,
    makeKey: vehicle.makeKey || vehicle.make || vehicle.brand || "",
    fullModel: vehicle.fullModel || vehicle.model || modelKey,
    ...(hasExplicitVariant || hasContextVariant ? {
      variantKey: vehicle.variantKey || vehicle.variant || vehicle.variantName || "",
      variant: vehicle.variant || vehicle.variantName || "",
      variantName: vehicle.variantName || vehicle.variant || "",
    } : {}),
  };

  const toolPlan = {
    tool: "vehicle_score_insight",
    operation,
    input: {
      message,
      query: message,
      ...payload,
    },
    args: payload,
    params: payload,
    entities: {
      ...payload,
      primaryModel: modelKey,
      primaryMake: payload.makeKey,
      primaryVariant: payload.variantName || "",
    },
    filters: {},
    output: {
      canvasType: "score_insight_canvas",
      inlineType: "score_insight_summary",
    },
  };

  const plan = {
    intent: "vehicle_score_insight",
    mode: "single_tool",
    conversationMode: "diagnostic",
    tools: [toolPlan],
    output: {
      canvasType: "score_insight_canvas",
      inlineType: "score_insight_summary",
    },
  };

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context: getContextForToolPlan(contextState),
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context: getContextForToolPlan(contextState),
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "score_insight",
    tool: "vehicle_score_insight",
    planMode: "single_tool",
    contextIsolation: "direct_score_insight_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    operation,
    routingReason: "direct_score_insight_fast_path",
  };

  return composeAciAnswer({
    ...normalized,
    answer: ensureDiagnosticOnlyAnswerNote(normalized.answer),
    operation,
    data: {
      ...(normalized.data || {}),
      operation,
    },
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: bridge,
    },
  });
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
  const languageNormalization = normalizeAciBuyerLanguage({
    message,
    context,
  });
  message = languageNormalization.normalizedMessage || message;
  message = expandActiveComparisonFollowUpMessage({
    message,
    context,
  });
  const effectiveMessage = message;
  const contextDirectFactFastPath = await maybeReturnContextDirectFactFastPath({
    message,
    context,
    user,
    session,
    meta,
    originalMessage,
    startedAt,
  });

  if (contextDirectFactFastPath) {
    return contextDirectFactFastPath;
  }

  const activeComparisonFollowUpFastPath = await maybeReturnActiveComparisonFollowUpFastPath({
    message,
    context,
    user,
    session,
    meta,
    originalMessage,
    startedAt,
  });

  if (activeComparisonFollowUpFastPath) {
    return activeComparisonFollowUpFastPath;
  }

  const unsupportedCityFastPath = maybeReturnUnsupportedCityFastPath({
    message,
    effectiveMessage,
    context,
    startedAt: typeof startedAt !== "undefined" ? startedAt : Date.now(),
  });

  if (unsupportedCityFastPath) {
    return unsupportedCityFastPath;
  }

  const batch4BareClarificationFastPath = maybeReturnBatch4BareClarificationFastPath({
    message,
    context,
    originalMessage,
    effectiveMessage,
    startedAt,
  });

  if (batch4BareClarificationFastPath) {
    return batch4BareClarificationFastPath;
  }

  const batch4ExplainerFastPath = await maybeReturnBatch4ExplainerFastPath({
    message,
    context,
    originalMessage,
    effectiveMessage,
    startedAt,
  });

  if (batch4ExplainerFastPath) {
    return batch4ExplainerFastPath;
  }

  const batch4BroadDiscoveryFastPath = await maybeReturnBatch4BroadDiscoveryFastPath({
    message,
    context,
    contextState: context?.contextState || context?.aciContextState || {},
    user,
    session,
    meta,
    originalMessage,
    effectiveMessage,
    startedAt,
  });

  if (batch4BroadDiscoveryFastPath) {
    return batch4BroadDiscoveryFastPath;
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

  const deterministicFeatureSpecFastPath = await maybeReturnDeterministicFeatureSpecFastPath({
    message,
    context,
    contextState,
    candidateSnapshot: {
      ...managedCandidateSnapshot,
      rawMessage: message,
    },
    user,
    session,
    meta,
    originalMessage,
    startedAt,
  });

  if (deterministicFeatureSpecFastPath) {
    return deterministicFeatureSpecFastPath;
  }

  const crossModelScoreDiagnosticFastPath = detectCrossModelScoreDiagnosticRequest({
    message,
    candidateSnapshot: {
      ...managedCandidateSnapshot,
      activeContext: isolatedContext,
    },
  });

  if (crossModelScoreDiagnosticFastPath) {
    const crossPlan = applyCrossModelScoreDiagnosticPlanOverride({
      plan: {
        intent: "vehicle_score_insight",
        mode: "single_tool",
        conversationMode: "diagnostic",
        tools: [
          {
            tool: "vehicle_score_insight",
            output: {
              canvasType: "score_insight_canvas",
              inlineType: "score_insight_summary",
            },
          },
        ],
      },
      override: crossModelScoreDiagnosticFastPath,
    });

    const executed = await executeAciPlannerPlan({
      plan: crossPlan,
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

    const bridge = {
      enabled: true,
      durationMs: Date.now() - startedAt,
      selectedParser: "",
      usedGemini: false,
      primaryTask: "score_insight",
      tool: "vehicle_score_insight",
      planMode: "single_tool",
      contextIsolation: "cross_model_score_diagnostic_fast_path",
      originalMessage,
      effectiveMessage: message,
      operation: "cross_model_score_diagnostic",
      routingReason: crossModelScoreDiagnosticFastPath.routingReason,
      crossModelScoreDiagnosticTargets: crossModelScoreDiagnosticFastPath.targets,
    };

    return composeAciAnswer({
      ...normalized,
      answer: ensureDiagnosticOnlyAnswerNote(normalized.answer),
      operation: "cross_model_score_diagnostic",
      data: {
        ...(normalized.data || {}),
        operation: "cross_model_score_diagnostic",
      },
      aciCoreBridge: bridge,
      meta: {
        ...(normalized.meta || {}),
        aciCoreBridge: bridge,
      },
    });
  }

  const directScoreInsightFastPath = await maybeReturnDirectScoreInsightFastPath({
    message,
    context,
    contextState,
    candidateSnapshot: managedCandidateSnapshot,
    user,
    session,
    meta,
    originalMessage,
    startedAt,
  });

  if (directScoreInsightFastPath) {
    return directScoreInsightFastPath;
  }

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

  const rawPlan = applyDirectPriceLookupOverride({
    plan: applyScoreValueLookupOverride({
      plan: applyCrossModelScoreDiagnosticPlanOverride({
        plan: basePlan,
        override: crossModelScoreDiagnosticOverride,
      }),
      override: scoreValueLookupOverride,
    }),
    override: directPriceLookupOverride,
  });

  const plan = sanitizeComparisonTargetsInPlan({
    plan: rawPlan,
    message,
  });
  const executionContext = sanitizeComparisonContextFromPlan({
    context: isolatedContext,
    plan,
  });

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context: executionContext,
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context: executionContext,
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
    ...(languageNormalization.changed
      ? {
          normalizedMessage: message,
          languageNormalizationRules: languageNormalization.rules,
        }
      : {}),
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
      ...(languageNormalization.changed ? { languageNormalization } : {}),
    },
  });
};

export {
  isAciCoreLiveBridgeEnabled,
  shouldUseAciCoreLiveBridge,
};
