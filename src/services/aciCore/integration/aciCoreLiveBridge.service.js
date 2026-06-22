import { createRequire } from "module";
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
import {
  renderAciLanguageText,
} from "../language/aciAnswerLanguageComposer.js";
import { maybeRunAciFeatureComparisonAnswer } from "../../aiAgent/aiAgent.featureComparisonAnswer.js";
import { runVehiclePricelistNewCarsTool } from "../../aiAgent/tools/newCars/vehiclePricelist.tool.js";
import { runVehicleFeaturesTool } from "../../aiAgent/tools/newCars/vehicleFeatures.tool.js";
import { runVehicleColorsTool } from "../../aiAgent/tools/newCars/vehicleColors.tool.js";
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
import {
  resolveModelScopedVariantFromMessage,
} from "../variants/modelScopedVariantResolver.service.js";


const require = createRequire(import.meta.url);

const {
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  DECISION_MODULES,
  ALLOWED_ANSWER_TYPES,
  CLAIM_TYPES,
} = require("../decisionPolicy/aciDecisionPolicy.constants.cjs");

const {
  createBaseDecisionPolicy,
  createBaseEvidence,
  createBaseProvenance,
  createBaseTrace,
} = require("../decisionPolicy/aciDecisionOutput.contract.cjs");

const {
  applyDecisionPolicyWithModuleProfile,
} = require("../decisionPolicy/aciDecisionModulePolicyProfiles.service.cjs");

const {
  applyDecisionDegradedMode,
} = require("../decisionPolicy/aciDecisionDegradedMode.service.cjs");

const {
  evaluateDecisionProvenance,
} = require("../decisionPolicy/aciDecisionProvenance.service.cjs");

const {
  buildFinalRecommendationEligibilityRuntime,
} = require("../decisionPolicy/aciFinalRecommendationEligibility.service.cjs");

const {
  getModelScoreInsights,
} = require("../scoreProfiles/aciVariantScoreInsight.service.cjs");

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

const keyify = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

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

const comparisonVehicleDedupeKey = (vehicle = {}) => {
  const make = cleanText(vehicle.make || vehicle.brand || "");
  let model = cleanText(vehicle.model || vehicle.rawModel || vehicle.fullModel || vehicle.displayName || "");
  const variant = cleanText(vehicle.variant || vehicle.variantName || vehicle.selectedVariant || "");

  if (make && model.toLowerCase().startsWith(`${make} `.toLowerCase())) {
    model = cleanText(model.slice(make.length));
  }

  return [make, model, variant]
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
};

const dedupeComparisonVehicles = (vehicles = []) => {
  const seen = new Set();

  return asArray(vehicles).filter((vehicle) => {
    if (!vehicle || typeof vehicle !== "object") return false;

    const key =
      comparisonVehicleDedupeKey(vehicle) ||
      cleanText(vehicle.fullModel || vehicle.displayName || vehicle.model || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();

    if (!key) return true;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const getActiveComparisonVehiclesFromContext = (context = {}) =>
  dedupeComparisonVehicles(
    asArray(
      context?.activeComparison?.vehicles ||
        context?.selectedComparisonSet?.vehicles ||
        context?.contextState?.activeComparison?.vehicles ||
        context?.aciContextState?.activeComparison?.vehicles ||
        [],
    ),
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

const MODEL_FEATURE_FAST_PATH_CACHE_TTL_MS = 5 * 60 * 1000;
let modelFeatureSummaryCache = {
  loadedAt: 0,
  rows: [],
};

const normalizeModelFeatureText = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactModelFeatureText = (value = "") =>
  normalizeModelFeatureText(value).replace(/\s+/g, "");

const getModelFeatureSummaryRows = async () => {
  const now = Date.now();
  if (
    modelFeatureSummaryCache.rows.length &&
    now - modelFeatureSummaryCache.loadedAt < MODEL_FEATURE_FAST_PATH_CACHE_TTL_MS
  ) {
    return modelFeatureSummaryCache.rows;
  }

  const collection = mongoose.connection?.db?.collection("aci_vehicle_model_summary");
  if (!collection) return [];

  const rows = await collection
    .find(
      {},
      {
        projection: {
          make: 1,
          brand: 1,
          model: 1,
          fullModel: 1,
          displayName: 1,
          modelKey: 1,
          brandKey: 1,
          makeKey: 1,
          brandModelKey: 1,
          lifecycleStatus: 1,
          active: 1,
          isActive: 1,
        },
      },
    )
    .toArray()
    .catch(() => []);

  modelFeatureSummaryCache = {
    loadedAt: now,
    rows: rows.filter((row = {}) => row.model || row.fullModel || row.displayName || row.modelKey),
  };

  return modelFeatureSummaryCache.rows;
};

const getModelFeatureAliases = (row = {}) =>
  uniqueKeys([
    row.fullModel,
    row.displayName,
    [row.make || row.brand, row.model].filter(Boolean).join(" "),
    [row.brand, row.model].filter(Boolean).join(" "),
    [row.make, row.model].filter(Boolean).join(" "),
    row.model,
    row.brandModelKey,
    row.modelKey,
  ]).filter((alias) => normalizeModelFeatureText(alias).length >= 3);

const containsModelAlias = (normalizedMessage = "", alias = "") => {
  const normalizedAlias = normalizeModelFeatureText(alias);
  if (!normalizedAlias) return false;

  const paddedMessage = ` ${normalizedMessage} `;
  const paddedAlias = ` ${normalizedAlias} `;
  if (paddedMessage.includes(paddedAlias)) return true;

  const compactMessage = compactModelFeatureText(normalizedMessage);
  const compactAlias = compactModelFeatureText(normalizedAlias);
  return compactAlias.length >= 4 && compactMessage.includes(compactAlias);
};

const resolveStandaloneModelMentionFromSummary = async (message = "") => {
  const normalizedMessage = normalizeModelFeatureText(message);
  if (!normalizedMessage) return null;

  const rows = await getModelFeatureSummaryRows();
  const matches = [];

  for (const row of rows) {
    const aliases = getModelFeatureAliases(row);

    for (const alias of aliases) {
      if (!containsModelAlias(normalizedMessage, alias)) continue;

      matches.push({
        row,
        alias,
        aliasLength: normalizeModelFeatureText(alias).length,
      });
    }
  }

  matches.sort((a, b) => b.aliasLength - a.aliasLength);
  const best = matches[0];
  if (!best?.row) return null;

  const row = best.row;
  return {
    make: row.make || row.brand || "",
    brand: row.brand || row.make || "",
    model: row.model || row.displayName || row.fullModel || row.modelKey || "",
    fullModel:
      row.fullModel ||
      row.displayName ||
      [row.make || row.brand, row.model].filter(Boolean).join(" ") ||
      row.model ||
      "",
    modelKey: row.modelKey || "",
    brandKey: row.brandKey || row.makeKey || "",
    matchedAlias: best.alias,
  };
};

const stripModelFeatureIntentWords = (value = "") =>
  normalizeModelFeatureText(value)
    .replace(
      /\b(which|what|show|tell|list|all|current|new|car|cars|model|models|variant|variants|option|options|available|availability|have|has|get|gets|offer|offers|come|comes|with|having|does|do|is|are|in|the|a|an)\b/g,
      " ",
    )
    .replace(
      /\b(me|mein|mai|main|ke|ka|ki|kaunse|konse|kaunsi|konsi|kis|wale|waale|wali|hai|hain|kya|milta|milti|milte|batao|dikhao)\b/g,
      " ",
    )
    .replace(/\s+/g, " ")
    .trim();

const hasLikelyVariantResidueForModelFeatureFastPath = ({
  message = "",
  modelAlias = "",
  feature = "",
} = {}) => {
  let residue = normalizeModelFeatureText(message);

  for (const value of [
    modelAlias,
    feature,
    feature.replace(/^6\s+/, "six "),
    feature.replace(/^six\s+/, "6 "),
    "sunroof",
    "airbags",
    "6 airbags",
    "six airbags",
    "adas",
    "rear camera",
    "ventilated seats",
  ]) {
    const normalized = normalizeModelFeatureText(value);
    if (!normalized) continue;
    residue = (` ${residue} `).replace(new RegExp(` ${normalized.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} `, "g"), " ");
  }

  residue = stripModelFeatureIntentWords(residue);

  if (!residue) return false;

  // Short trim-like leftovers usually mean user asked for a specific variant:
  // "Creta SX sunroof", "Seltos HTX sunroof", "Punch Adventure S sunroof".
  return /(^| )(sx|sxo|sx o|htx|hte|htk|gtx|x line|zxi|vxi|lxi|alpha|delta|zeta|sigma|adventure|accomplished|pure|creative|fearless|smart|savvy)( |$)/i.test(residue) ||
    residue.split(" ").filter(Boolean).length <= 3;
};

const maybeReturnStandaloneModelFeatureFastPath = async ({
  message = "",
  context = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  if (hasComparisonLanguage(message)) return null;
  if (hasContextReference(message) && !/\b(mein|mai|main|me|ke|ka|ki|hai|kya|milta|milti|milte)\b/i.test(message)) return null;

  const featureLookup = detectBatch2FeatureLookup(message);
  if (!featureLookup?.feature) return null;
  if (featureLookup.summary || featureLookup.category) return null;

  const resolvedVehicle = await resolveStandaloneModelMentionFromSummary(message);
  if (!resolvedVehicle?.model) return null;

  if (
    hasLikelyVariantResidueForModelFeatureFastPath({
      message,
      modelAlias: resolvedVehicle.matchedAlias || resolvedVehicle.fullModel || resolvedVehicle.model,
      feature: featureLookup.feature,
    })
  ) {
    return null;
  }

  const asksForVariants =
    /\bvariants?\b/i.test(message) ||
    /\b(which|show|list)\b.*\bvariants?\b/i.test(message) ||
    /\b(kaunse|konse|kaunsi|konsi|kis)\b.*\b(variants?|wale|waale|wali)\b/i.test(message) ||
    /\b(variants?|wale|waale|wali)\b.*\b(kaunse|konse|kaunsi|konsi|kis)\b/i.test(message);

  const city =
    context?.selectedVehicle?.citySlug ||
    context?.selectedVehicle?.city ||
    context?.anchorCity ||
    "new-delhi";

  const toolContext = {
    ...context,
    anchorMake: resolvedVehicle.make,
    anchorModel: resolvedVehicle.model,
    anchorFullModel: resolvedVehicle.fullModel,
    anchorCity: city,
    selectedVehicle: {
      ...(context?.selectedVehicle || {}),
      make: resolvedVehicle.make,
      brand: resolvedVehicle.brand,
      model: resolvedVehicle.model,
      fullModel: resolvedVehicle.fullModel,
      modelKey: resolvedVehicle.modelKey,
      city,
      citySlug: city,
    },
  };

  const toolMessage = asksForVariants
    ? `which ${resolvedVehicle.model} variants have ${featureLookup.feature}`
    : message;

  const toolPlan = {
    tool: "vehicle_feature_lookup",
    intent: asksForVariants ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    toolIntent: asksForVariants ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    input: {
      message: toolMessage,
      query: toolMessage,
      originalMessage: message,
      model: resolvedVehicle.model,
      fullModel: resolvedVehicle.fullModel,
      make: resolvedVehicle.make,
      feature: featureLookup.feature,
      features: [featureLookup.feature],
      topic: featureLookup.feature,
      intent: asksForVariants ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    },
    args: {
      message: toolMessage,
      query: toolMessage,
      originalMessage: message,
      model: resolvedVehicle.model,
      fullModel: resolvedVehicle.fullModel,
      make: resolvedVehicle.make,
      feature: featureLookup.feature,
      features: [featureLookup.feature],
      topic: featureLookup.feature,
      intent: asksForVariants ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    },
    params: {
      message: toolMessage,
      query: toolMessage,
      originalMessage: message,
      model: resolvedVehicle.model,
      fullModel: resolvedVehicle.fullModel,
      make: resolvedVehicle.make,
      feature: featureLookup.feature,
      features: [featureLookup.feature],
      topic: featureLookup.feature,
      intent: asksForVariants ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    },
    entities: {
      model: resolvedVehicle.model,
      fullModel: resolvedVehicle.fullModel,
      primaryModel: resolvedVehicle.model,
      primaryMake: resolvedVehicle.make,
      feature: featureLookup.feature,
      features: [featureLookup.feature],
      topic: featureLookup.feature,
    },
    filters: {
      model: resolvedVehicle.model,
      city,
      feature: featureLookup.feature,
      mustHaveFeatures: [featureLookup.feature],
    },
    output: {
      canvasType: asksForVariants ? "feature_match_builder_canvas" : null,
      inlineType: asksForVariants ? "feature_match_summary" : "feature_answer_card",
    },
  };

  const plan = {
    intent: asksForVariants ? "vehicle_feature_discovery" : "vehicle_feature_answer",
    mode: "single_tool",
    conversationMode: "direct_answer",
    tools: [toolPlan],
    output: {
      canvasType: asksForVariants ? "feature_match_builder_canvas" : null,
    },
  };

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: toolMessage,
    context: {
      ...toolContext,
      originalUserMessage: message,
    },
    user,
    session,
    meta: {
      ...meta,
      originalUserMessage: message,
    },
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message: toolMessage,
    context: {
      ...toolContext,
      originalUserMessage: message,
    },
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: asksForVariants ? "feature_discovery" : "feature_lookup",
    tool: "vehicle_feature_lookup",
    planMode: "single_tool",
    contextIsolation: "standalone_model_feature_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    routingReason: asksForVariants
      ? "standalone_model_feature_discovery"
      : "standalone_model_feature_lookup",
  };

  const composed = composeAciAnswer({
    ...normalized,
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: bridge,
    },
  });

  return attachDecisionRuntimeEnvelope(composed, { bridge, context });
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


const hasStandaloneModelFeatureComparisonIntent = (message = "") => {
  const text = cleanText(message).toLowerCase();
  if (!text) return false;

  const hasExplicitComparison =
    /\bvs\b|\bv\/s\b|\bversus\b|\bcompare\b|\bcomparison\b|\bdifference\b|\bdifferent\b/i.test(text);

  if (!hasExplicitComparison) return false;

  const featureLookup = detectBatch2FeatureLookup(message);
  if (!featureLookup || !featureLookup.feature || featureLookup.summary) return false;

  return true;
};

const maybeReturnStandaloneModelFeatureComparisonFastPath = async ({
  message = "",
  context = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  if (!hasStandaloneModelFeatureComparisonIntent(message)) return null;

  const featureLookup = detectBatch2FeatureLookup(message);
  if (!featureLookup?.feature) return null;

  const result = await maybeRunAciFeatureComparisonAnswer({
    message,
    toolPlan: {
      tool: "vehicle_feature_comparison",
      intent: "vehicle_feature_comparison",
      toolIntent: "vehicle_feature_comparison",
      input: {
        message,
        query: message,
        feature: featureLookup.feature,
        features: [featureLookup.feature],
      },
      entities: {
        feature: featureLookup.feature,
        features: [featureLookup.feature],
      },
      filters: {
        mustHaveFeatures: [featureLookup.feature],
      },
      output: {
        canvasType: "feature_comparison_canvas",
        inlineType: "feature_comparison_summary",
      },
    },
    context,
  });

  if (!result) return null;

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "vehicle_feature_comparison",
    tool: "vehicle_feature_comparison",
    planMode: "single_tool",
    contextIsolation: "standalone_model_feature_comparison_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    routingReason: "standalone_model_feature_comparison",
  };

  return composeAciAnswer({
    ...result,
    dataStatus: result.dataStatus || "available",
    sourceCollections: result.sourceCollections || [
      "vehicle_feature_catalog_v2",
      "vehicle_variant_feature_matrix_v2",
    ],
    aciCoreBridge: bridge,
    meta: {
      ...(result.meta || {}),
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
      selectedComparisonVehicles: dedupeComparisonVehicles(pruneSubstringComparisonTargets({ vehicles: targets, message })),
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

  const composed = composeAciAnswer({
    ...normalized,
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: bridge,
    },
  });

  return attachDecisionRuntimeEnvelope(composed, { bridge, context });
};

const normalizeExplicitComparisonText = (value = "") =>
  cleanText(value)
    .replace(/[?!.]+$/g, "")
    .replace(/\b(price\s+difference|show\s+price\s+difference|difference|comparison|compare|which\s+is\s+cheaper|cheaper|costlier|more\s+expensive)\b/gi, " ")
    .replace(/\b(on\s*road|on-road|ex\s*showroom|ex-showroom|price|prices)\b/gi, " ")
    .replace(/\b(in|for|at)\s+(new\s+delhi|delhi|noida|gurgaon|gurugram)\b.*$/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

const detectExplicitComparisonCity = (message = "", context = {}) => {
  const text = cleanText(message).toLowerCase();

  if (/\bnoida\b/i.test(text)) return "noida";
  if (/\b(gurgaon|gurugram)\b/i.test(text)) return "gurgaon";
  if (/\b(new\s+delhi|delhi)\b/i.test(text)) return "new-delhi";

  return getComparisonCityFromContext(context);
};

const hasExplicitUnsupportedComparisonCity = (message = "") => {
  const text = cleanText(message).toLowerCase();

  if (/\b(new\s+delhi|delhi|noida|gurgaon|gurugram)\b/i.test(text)) return false;

  return /\b(mumbai|bombay|bangalore|bengaluru|jaipur|pune|chennai|hyderabad|kolkata|ahmedabad|lucknow|faridabad|ghaziabad|chandigarh|surat|indore|bhopal|patna|kochi|cochin)\b/i.test(text);
};

const hasScoreDiagnosticComparisonLanguage = (message = "") =>
  hasComparisonLanguage(message) &&
  /\b(diagnostic\s+score|score\s+diagnostic|scores?|scoring|value\s+scores?|value\s+score|safety\s+scores?|overall\s+scores?)\b/i.test(message);

const parseExplicitComparisonTargetsFromMessage = (message = "") => {
  const raw = cleanText(message);
  if (!raw) return [];

  const featureLookup = detectBatch2FeatureLookup(message);
  if (featureLookup?.feature) return [];

  const patterns = [
    /^(.+?)\s+(?:vs|v\/s|versus)\s+(.+?)$/i,
    /^compare\s+(.+?)\s+(?:and|with|to|against)\s+(.+?)$/i,
    /^(?:show\s+)?(?:price\s+)?difference\s+between\s+(.+?)\s+and\s+(.+?)$/i,
  ];

  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (!match) continue;

    const left = normalizeExplicitComparisonText(match[1]);
    const right = normalizeExplicitComparisonText(match[2]);

    if (!left || !right) continue;
    if (left.toLowerCase() === right.toLowerCase()) continue;

    return [left, right];
  }

  return [];
};

const maybeReturnExplicitComparisonFastPath = async ({
  message = "",
  context = {},
  user = null,
  session = null,
  meta = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  if (!hasComparisonLanguage(message)) return null;
  if (isDirectNonComparisonTask(message)) return null;
  if (hasScoreDiagnosticComparisonLanguage(message)) return null;
  if (hasExplicitUnsupportedComparisonCity(message)) return null;

  const parsedTargets = parseExplicitComparisonTargetsFromMessage(message);
  if (parsedTargets.length < 2) return null;

  const city = detectExplicitComparisonCity(message, context);
  const targets = parsedTargets.slice(0, 2).map((label) => ({
    model: label,
    fullModel: label,
    city,
  }));

  const comparisonVehicles = dedupeComparisonVehicles(
    pruneSubstringComparisonTargets({ vehicles: targets, message }),
  );

  if (comparisonVehicles.length < 2) return null;

  const toolPlan = {
    tool: "vehicle_compare",
    input: {
      message,
      query: message,
    },
    entities: {
      comparisonVehicles,
      models: comparisonVehicles.map((target) => target.fullModel || target.model).filter(Boolean),
      comparisonModels: comparisonVehicles.map((target) => target.fullModel || target.model).filter(Boolean),
    },
    filters: {
      city,
      models: comparisonVehicles.map((target) => target.fullModel || target.model).filter(Boolean),
    },
    output: {
      canvasType: "comparison_canvas",
      inlineType: "",
    },
    resolution: {
      comparisonLevel: "model",
      variantSelectionMode: "not_required",
      selectedVariants: [],
      selectedModels: comparisonVehicles.map((target) => ({
        model: target.model || target.fullModel,
      })),
      selectedComparisonVehicles: comparisonVehicles,
    },
    contextPatch: {
      activeComparison: {
        vehicles: comparisonVehicles,
        city,
        citySlug: city,
      },
      selectedComparisonSet: {
        vehicles: comparisonVehicles,
      },
    },
  };

  const plan = {
    intent: "vehicle_comparison",
    mode: "single_tool",
    conversationMode: "comparison",
    tools: [toolPlan],
    output: {
      canvasType: "comparison_canvas",
    },
    contextPatch: toolPlan.contextPatch,
  };

  const executionContext = {
    ...(context || {}),
    anchorCity: city,
    selectedVehicle: {
      ...((context || {}).selectedVehicle || {}),
      city,
      citySlug: city,
    },
    activeComparison: {
      vehicles: comparisonVehicles,
      city,
      citySlug: city,
    },
    selectedComparisonSet: {
      vehicles: comparisonVehicles,
    },
  };

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

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "",
    usedGemini: false,
    primaryTask: "vehicle_comparison",
    tool: "vehicle_compare",
    planMode: "single_tool",
    contextIsolation: "explicit_comparison_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: message,
    routingReason: "explicit_comparison_fast_path",
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
  const wantsTurbo = /\bturbo(?:\s*charged|charged)?\b|\bturbo\s+charger\b/i.test(normalized);

  if (!hasBudget || (!hasBroadCar && !wantsFamily && !wantsElectric && !wantsSuv && !wantsAutomatic && !wantsTurbo)) {
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
    mustHaveFeatures: wantsTurbo ? ["turbo charger"] : [],
    reason: wantsFamily
      ? "family_budget_discovery"
      : wantsElectric || wantsSuv || wantsAutomatic || wantsTurbo
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

  const hasOfferOrDiscountIntent = /\b(wait\s+for\s+discount|discount|offers?|deal|benefit)\b/i.test(normalized);
  const hasPriceIntent = /\b(price|pricing|pricelist|price list|on[-\s]?road|ex[-\s]?showroom)\b/i.test(normalized);
  const hasComparisonIntent = /\b(compare|comparison|vs|v\/s|versus|difference|cheaper|costlier|expensive)\b/i.test(normalized);
  const hasEmiIntent = /\b(emi|loan|finance|tenure|down\s*payment)\b/i.test(normalized);

  // Do not let the pending offers fast path swallow useful multi-intent answers.
  // Price/comparison/EMI must still be answered, with offer honesty as a secondary note.
  if (hasOfferOrDiscountIntent && [hasPriceIntent, hasComparisonIntent, hasEmiIntent].filter(Boolean).length >= 1) {
    return null;
  }

  if (hasOfferOrDiscountIntent) {
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
    ...(Array.isArray(discovery.mustHaveFeatures) && discovery.mustHaveFeatures.length ? {
      mustHaveFeatures: discovery.mustHaveFeatures,
      compareFeatures: discovery.mustHaveFeatures,
      feature: discovery.mustHaveFeatures[0],
      ranking: "feature_match",
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
      canvasType: filters.mustHaveFeatures?.length
        ? "feature_match_builder_canvas"
        : "recommendation_results_canvas",
      inlineType: filters.mustHaveFeatures?.length
        ? "feature_match_summary"
        : "recommendation_summary",
    },
  };

  const plan = {
    intent: "vehicle_recommendation",
    mode: "single_tool",
    conversationMode: "direct_answer",
    tools: [toolPlan],
    output: {
      canvasType: filters.mustHaveFeatures?.length
        ? "feature_match_builder_canvas"
        : "recommendation_results_canvas",
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

  return attachDecisionRuntimeEnvelope(composed, { bridge, context: getContextForToolPlan(contextState || {}) });
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

      const prunedVehicles = dedupeComparisonVehicles(
        pruneSubstringComparisonTargets({ vehicles: existingVehicles, message })
          .map((target) => enrichComparisonTarget({ target, lookup: targetLookup })),
      );

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
  const prunedVehicles = dedupeComparisonVehicles(
    targets.map((target) => enrichComparisonTarget({ target, lookup: targetLookup })),
  );
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

const isHeavyNewCarMultiIntentForLegacyPlanner = (message = "") => {
  const text = String(message || "").toLowerCase();
  if (!text.trim()) return false;

  const hasPriceIntent = /\b(price|pricing|pricelist|price list|on[-\s]?road|ex[-\s]?showroom)\b/i.test(text);
  const hasComparisonIntent = /\b(compare|comparison|vs|v\/s|versus|difference|cheaper|costlier|expensive)\b/i.test(text);
  const hasEmiIntent = /\b(emi|loan|finance|tenure|down\s*payment)\b/i.test(text);
  const hasOfferIntent = /\b(offer|offers|discount|deal|benefit|quotation|quote)\b/i.test(text);

  return [hasPriceIntent, hasComparisonIntent, hasEmiIntent, hasOfferIntent].filter(Boolean).length >= 3;
};

const shouldUseAciCoreLiveBridge = ({ message = "" } = {}) => {
  if (!isAciCoreLiveBridgeEnabled()) return false;

  const text = String(message || "").trim();
  if (!text) return false;

  // Heavy buyer queries need the legacy V2 multi-tool composer for secondary cards.
  // Live bridge single-tool fast paths must not collapse price + compare + EMI + offers.
  if (isHeavyNewCarMultiIntentForLegacyPlanner(text)) return false;

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
  ["jaipur", "Jaipur"],
  ["chandigarh", "Chandigarh"],
  ["faridabad", "Faridabad"],
  ["ghaziabad", "Ghaziabad"],
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

const normalizeUnsupportedCityVehicleContext = ({
  vehicle = {},
  requestedCity = "",
  source = "",
} = {}) => {
  const make = cleanText(vehicle.make || vehicle.brand || vehicle.metadata?.make || vehicle.metadata?.brand || "");
  const model = cleanText(vehicle.model || vehicle.rawModel || vehicle.metadata?.model || vehicle.metadata?.rawModel || "");
  const fullModel = cleanText(
    vehicle.fullModel ||
      vehicle.displayName ||
      vehicle.metadata?.fullModel ||
      vehicle.metadata?.displayName ||
      [make, model].filter(Boolean).join(" "),
  );
  const variant = cleanText(vehicle.variant || vehicle.variantName || vehicle.selectedVariant || "");

  if (!model && !fullModel) return {};

  return {
    ...vehicle,
    make,
    brand: make || vehicle.brand || vehicle.make || "",
    model: model || fullModel,
    fullModel: fullModel || [make, model].filter(Boolean).join(" "),
    displayName: fullModel || [make, model].filter(Boolean).join(" "),
    modelKey: vehicle.modelKey || vehicle.shortModelKey || vehicle.canonicalKey || "",
    shortModelKey: vehicle.shortModelKey || "",
    variant,
    variantName: cleanText(vehicle.variantName || vehicle.variant || vehicle.selectedVariant || ""),
    selectedVariant: cleanText(vehicle.selectedVariant || vehicle.variantName || vehicle.variant || ""),
    city: requestedCity,
    citySlug: normalizeFastPathSlug(requestedCity),
    unsupportedCity: requestedCity,
    confidence: Number(vehicle.confidence || 0.85),
    source: source || vehicle.source || "unsupported_city_vehicle_context",
  };
};

const resolveUnsupportedCityVehicleContext = async ({
  message = "",
  requestedCity = "",
  context = {},
} = {}) => {
  const contextVehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    {};

  const normalizedContextVehicle = normalizeUnsupportedCityVehicleContext({
    vehicle: contextVehicle,
    requestedCity,
    source: contextVehicle.source || "active_context",
  });

  if (normalizedContextVehicle.model || normalizedContextVehicle.fullModel) {
    return normalizedContextVehicle;
  }

  const priceVehicle = await resolveSupportedExactPriceVehicleFromMessage({
    message,
    citySlug: "new-delhi",
  }).catch(() => null);

  if (priceVehicle?.model || priceVehicle?.vehiclePhrase) {
    const cleanMake = cleanText(priceVehicle.make || "");
    const cleanModel = cleanText(priceVehicle.model || priceVehicle.vehiclePhrase || "");
    const cleanFullModel = [cleanMake, cleanModel].filter(Boolean).join(" ") || cleanModel;

    return normalizeUnsupportedCityVehicleContext({
      vehicle: {
        make: cleanMake,
        brand: cleanMake,
        model: cleanModel,
        fullModel: cleanFullModel,
        displayName: cleanFullModel,
        modelKey: priceVehicle.modelKey || "",
        shortModelKey: priceVehicle.shortModelKey || "",
        variant: priceVehicle.variant || "",
        variantName: priceVehicle.variant || "",
        selectedVariant: priceVehicle.variant || "",
        confidence: priceVehicle.score ? 0.9 : 0.85,
      },
      requestedCity,
      source: "unsupported_city_price_resolver",
    });
  }

  const aliasVehicle = await resolveVehicleAlias({ message }).catch(() => null);
  return normalizeUnsupportedCityVehicleContext({
    vehicle: aliasVehicle || {},
    requestedCity,
    source: "unsupported_city_alias_resolver",
  });
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
      anchorMake: selectedVehicle.make || selectedVehicle.brand || "",
      anchorModel: selectedVehicle.model || selectedVehicle.fullModel || "",
      anchorVariant: selectedVehicle.variant || selectedVehicle.variantName || selectedVehicle.selectedVariant || "",
      selectedVehicle:
        selectedVehicle.model || selectedVehicle.fullModel
          ? selectedVehicle
          : {},
    },
  };
};

const maybeReturnUnsupportedCityFastPath = async ({
  message = "",
  effectiveMessage = "",
  context = {},
  startedAt = 0,
} = {}) => {
  const text = effectiveMessage || message;
  if (!hasAciPriceIntentForFastUnsupportedCity(text)) {
    return null;
  }

  const explicitMatch = findUnsupportedPriceCityForFastPath(text);
  const contextMatch =
    !explicitMatch &&
    !hasSupportedCityMention(text) &&
    isContextualPriceFastPathFollowUp(text)
      ? findUnsupportedPriceCityForContext(context)
      : null;

  const match = explicitMatch || contextMatch;
  if (!match) return null;

  const [, requestedCity] = match;

  const selectedVehicle = await resolveUnsupportedCityVehicleContext({
    message: text,
    requestedCity,
    context,
  });

  const enrichedContext =
    selectedVehicle?.model || selectedVehicle?.fullModel
      ? {
          ...context,
          selectedVehicle,
          contextState: {
            ...(context?.contextState || {}),
            selectedVehicle,
          },
          aciContextState: {
            ...(context?.aciContextState || {}),
            selectedVehicle,
          },
        }
      : context;

  return buildUnsupportedCityFastPathResponse({
    message,
    effectiveMessage,
    requestedCity,
    context: enrichedContext,
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

const getFastPathCityCandidatesFromContext = (context = {}) => {
  const selectedVehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    {};

  const contextState = context?.contextState || {};
  const aciContextState = context?.aciContextState || {};
  const anchors = contextState?.anchors || aciContextState?.anchors || {};

  return [
    context?.anchorCity,
    context?.citySlug,
    context?.city,
    anchors?.anchorCity,
    anchors?.citySlug,
    anchors?.city,
    contextState?.anchorCity,
    contextState?.citySlug,
    contextState?.city,
    aciContextState?.anchorCity,
    aciContextState?.citySlug,
    aciContextState?.city,
    selectedVehicle.citySlug,
    selectedVehicle.city,
  ]
    .map((value) => cleanText(value))
    .filter(Boolean);
};

const normalizeSupportedFastPathCitySlug = (value = "") => {
  const normalized = normalizeFastPathSlug(value);
  if (["new-delhi", "noida", "gurgaon"].includes(normalized)) return normalized;
  if (normalized === "delhi") return "new-delhi";
  if (normalized === "gurugram") return "gurgaon";
  return "";
};

const getSupportedCitySlugFromContext = (context = {}) => {
  for (const city of getFastPathCityCandidatesFromContext(context)) {
    const normalized = normalizeSupportedFastPathCitySlug(city);
    if (normalized) return normalized;
  }
  return "";
};

const findUnsupportedPriceCityForContext = (context = {}) => {
  for (const city of getFastPathCityCandidatesFromContext(context)) {
    const match = findUnsupportedPriceCityForFastPath(city);
    if (match) return match;
  }
  return null;
};

const isContextualPriceFastPathFollowUp = (message = "") => {
  const normalized = normalizeFastPathText(message);
  return (
    /^price$/.test(normalized) ||
    /\b(price there|there price|same price|same in|price in that city|price in this city)\b/i.test(String(message || "")) ||
    hasContextReference(message)
  );
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

const hasParentheticalVariantRequest = (message = "") =>
  /\b[A-Za-z0-9]+\s*\([^)]+\)/.test(String(message || ""));

const buildExactUnavailableVariantFastPathResponse = ({
  message = "",
  effectiveMessage = "",
  originalMessage = "",
  resolved = {},
  resolution = {},
  citySlug = "new-delhi",
  startedAt = 0,
} = {}) => {
  const requestedVariant = resolution.requestedVariantText || "that exact variant";
  const fullModel = [resolved.make, resolved.model].filter(Boolean).join(" ") || resolved.model || "";
  const answer = renderAciLanguageText("decision_exact_variant_unavailable_recovery", { modelLabel: fullModel || "this model", requestedVariant }, { seed: buildAciLanguageSeed("decision_exact_variant_unavailable_recovery", fullModel, requestedVariant, message) }) || `I found ${fullModel || "this model"}, but ${requestedVariant} does not match an exact current variant in the DB-backed catalog. I can continue with listed variants, model-level price, and features.`;
  const selectedVehicle = {
    make: resolved.make || "",
    brand: resolved.make || "",
    model: resolved.model || "",
    fullModel,
    // Keep the model context, but do not store the invalid requested variant
    // as the active selected variant. Otherwise follow-up price/features keep
    // filtering by a non-existent trim.
    variant: "",
    variantName: "",
    selectedVariant: "",
    variantKey: "",
    variantResolutionStatus: "exact_unavailable",
    unresolvedVariant: requestedVariant,
    city: citySlug,
    citySlug,
  };
  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "deterministic",
    usedGemini: false,
    primaryTask: "clarification",
    tool: "clarification",
    planMode: "clarification",
    contextIsolation: "exact_variant_unavailable_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: effectiveMessage || message,
    routingReason: "exact_variant_unavailable",
  };

  return {
    intent: "clarification",
    tool: "clarification",
    mode: "clarification",
    conversationMode: "clarification",
    displayMode: "inline",
    canvasType: "",
    inlineType: "clarification_card",
    title: "Choose a listed variant",
    answer,
    clarification: answer,
    rows: [],
    items: [],
    matched: 0,
    count: 0,
    selectedVehicle,
    contextPatch: {
      anchorMake: selectedVehicle.make,
      anchorModel: selectedVehicle.model,
      anchorVariant: "",
      anchorCity: citySlug,
      selectedVehicle,
      lastUnresolvedVariant: requestedVariant,
      variantResolution: {
        status: "exact_unavailable",
        requestedVariant,
      },
    },
    data: {
      title: "Need one detail",
      answer,
      rows: [],
      items: [],
      selectedVehicle,
    },
    aciCoreBridge: bridge,
    meta: {
      aciCoreBridge: bridge,
      variantResolution: {
        status: "exact_unavailable",
        requestedVariant,
        candidates: resolution.candidates || [],
      },
    },
  };
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

    const isSupportedCityPriceFollowUp =
      hasSupportedCityMention(text) &&
      /\b(price|pricing|pricelist|price list|on road|on-road|onroad|breakup)\b/i.test(text);

    const isExplicitModelLevelPriceFollowUp =
      /\b(show|open|give|get|list)\b.*\b(model\s+)?(price|prices|pricing|price\s+list|pricelist)\b/i.test(text) ||
      /\bmodel\s+(price|prices|pricing|price\s+list|pricelist)\b/i.test(text) ||
      /\b(all\s+variants?|variant\s+prices?|full\s+price\s+list)\b/i.test(text);

    const isBareOrContextualPrice =
      /^price$/i.test(normalizeFastPathText(text)) ||
      hasContextReference(text) ||
      isSupportedCityPriceFollowUp ||
      isExplicitModelLevelPriceFollowUp;

    if (hasSelectedVehicle && isBareOrContextualPrice) {
      const contextVariantIsInvalid =
        selectedVehicle.variantResolutionStatus === "exact_unavailable" ||
        selectedVehicle.variantKey === selectedVehicle.unresolvedVariant;

      const selectedVariant =
        isExplicitModelLevelPriceFollowUp || contextVariantIsInvalid
          ? ""
          : selectedVehicle.variant || selectedVehicle.variantName || selectedVehicle.selectedVariant || "";

      resolved = {
        make: selectedVehicle.make || selectedVehicle.brand || "",
        model: selectedVehicle.model || selectedVehicle.fullModel || "",
        modelKey: selectedVehicle.modelKey || "",
        variant: selectedVariant,
        citySlug:
          normalizeFastPathSlug(selectedVehicle.citySlug || selectedVehicle.city || citySlug) ||
          citySlug,
        vehiclePhrase: selectedVehicle.fullModel || selectedVehicle.model || "",
        score: 100000,
      };
    }
  }

  if (!resolved?.model) return null;

  if (hasParentheticalVariantRequest(text)) {
    const variantResolution = await resolveModelScopedVariantFromMessage({
      message: text,
      make: resolved.make,
      model: resolved.model,
      fullModel: [resolved.make, resolved.model].filter(Boolean).join(" "),
      modelKey: resolved.modelKey,
      citySlug,
    });

    if (variantResolution?.status === "exact_unavailable") {
      return buildExactUnavailableVariantFastPathResponse({
        message,
        effectiveMessage: text,
        originalMessage,
        resolved,
        resolution: variantResolution,
        citySlug,
        startedAt,
      });
    }
  }

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

  const composed = composeAciAnswer({
    ...normalized,
    aciCoreBridge: bridge,
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: bridge,
    },
  });

  return attachDecisionRuntimeEnvelope(composed, { bridge, context });
};


const hasVehicleColorLookupIntent = (message = "") => {
  const raw = String(message || "");
  if (!raw.trim()) return false;
  if (hasComparisonLanguage(raw)) return false;

  const hasColorWord =
    /\b(colou?rs?|paint|shade|shades|colour\s+options?|color\s+options?)\b/i.test(raw) ||
    /\b(black|white|red|blue|grey|gray|silver|pearl|matte|dual\s*tone|dual-tone)\b/i.test(raw);

  if (!hasColorWord) return false;

  if (/\b(price|pricing|on[-\s]?road|ex[-\s]?showroom|emi|loan|finance|quote|quotation|offer|discount|under\s+\d+|budget)\b/i.test(raw)) {
    return false;
  }

  return true;
};

const maybeReturnVehicleColorsFastPath = async ({
  message = "",
  effectiveMessage = "",
  context = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  const text = effectiveMessage || message;
  if (!hasVehicleColorLookupIntent(text)) return null;

  const citySlug = getSupportedCitySlugFromContext(context) || "new-delhi";
  let resolved = await resolveSupportedExactPriceVehicleFromMessage({
    message: text,
    citySlug,
  });

  if (!resolved?.model) {
    const contextVehicle =
      context?.selectedVehicle ||
      context?.contextState?.selectedVehicle ||
      context?.aciContextState?.selectedVehicle ||
      {};

    if (contextVehicle.model || contextVehicle.fullModel || contextVehicle.modelKey) {
      resolved = {
        make: contextVehicle.make || contextVehicle.brand || "",
        model: contextVehicle.model || contextVehicle.fullModel || "",
        modelKey: contextVehicle.modelKey || "",
        citySlug: normalizeFastPathSlug(contextVehicle.citySlug || contextVehicle.city || citySlug) || citySlug,
      };
    }
  }

  if (!resolved?.model) return null;

  const toolPlan = {
    tool: "vehicle_colors",
    input: {
      message: text,
      query: text,
      make: resolved.make,
      model: resolved.model,
      city: citySlug,
    },
    entities: {
      make: resolved.make,
      model: resolved.model,
    },
    filters: {
      city: citySlug,
    },
  };

  const toolResult = await runVehicleColorsTool({
    userMessage: text,
    message: text,
    query: text,
    make: resolved.make,
    model: resolved.model,
    city: citySlug,
    context,
    toolPlan,
  });

  if (!toolResult || toolResult.intent !== "vehicle_colors") return null;

  const normalized = await normalizeAciFinalResponse(toolResult, {
    message: text,
    context,
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "deterministic",
    usedGemini: false,
    primaryTask: "color_lookup",
    tool: "vehicle_colors",
    planMode: "single_tool",
    contextIsolation: "vehicle_colors_fast_path",
    originalMessage: originalMessage || message,
    effectiveMessage: text,
    routingReason: "explicit_color_lookup",
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

const hasSingleFeatureAnswerIntent = (message = "") => {
  const raw = String(message || "");
  if (hasComparisonLanguage(raw)) return false;
  if (/\b(and|plus|also|as well as)\b|[,/]/i.test(raw)) return false;

  // Broad feature discovery should stay on the normal discovery path and must
  // not become an exact single-variant answer just because a feature word exists.
  if (
    /\bwhich\b.*\bvariants?\b/i.test(raw) ||
    /\bvariants?\b.*\b(have|has|get|gets|with|available)\b/i.test(raw) ||
    /\b(cheapest|most affordable|lowest price|without|do not have|does not have|missing|miss)\b/i.test(raw)
  ) {
    return false;
  }

  return /\b(sunroof|adas|airbags?|six\s+airbags|6\s+airbags|camera|tpms|wireless\s+charging|cruise\s+control|ventilated\s+seats?)\b/i.test(raw);
};

const maybeReturnExactSingleFeatureFastPath = async ({
  message = "",
  effectiveMessage = "",
  context = {},
  originalMessage = "",
  startedAt = 0,
} = {}) => {
  const text = effectiveMessage || message;
  if (!hasSingleFeatureAnswerIntent(text)) return null;

  const citySlug = getSupportedCitySlugFromContext(context) || "new-delhi";
  const resolved = await resolveSupportedExactPriceVehicleFromMessage({
    message: text,
    citySlug,
  });

  if (!resolved?.model) return null;

  const variantResolution = await resolveModelScopedVariantFromMessage({
    message: text,
    make: resolved.make,
    model: resolved.model,
    fullModel: [resolved.make, resolved.model].filter(Boolean).join(" "),
    modelKey: resolved.modelKey,
    citySlug,
  });

  if (variantResolution?.status !== "exact" || !variantResolution.selected?.variant) {
    return null;
  }

  const variant = variantResolution.selected.variant;
  const toolPlan = {
    tool: "vehicle_feature_lookup",
    input: {
      message: text,
      query: text,
      make: resolved.make,
      model: resolved.model,
      variant,
      city: citySlug,
    },
    entities: {
      make: resolved.make,
      model: resolved.model,
      variant,
      primaryVariant: variant,
    },
    filters: {
      city: citySlug,
      variant,
    },
    resolution: {
      variantSelectionMode: "exact",
      selectedVariants: [
        {
          variant,
          variantName: variant,
          selectedVariant: variant,
          variantKey: variantResolution.selected.variantKey || "",
        },
      ],
    },
  };

  const toolResult = await runVehicleFeaturesTool({
    userMessage: text,
    message: text,
    query: text,
    context,
    toolPlan,
  });

  const normalized = await normalizeAciFinalResponse(toolResult, {
    message: text,
    context,
  });

  const bridge = {
    enabled: true,
    durationMs: startedAt ? Date.now() - startedAt : 0,
    selectedParser: "deterministic",
    usedGemini: false,
    primaryTask: "feature_answer",
    tool: "vehicle_feature_lookup",
    planMode: "single_tool",
    contextIsolation: "exact_single_feature_fast_path",
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



const DECISION_BUYER_CONTEXT_LABELS = Object.freeze({
  city: "city",
  budgetOrPriceCeiling: "budget ceiling",
  bodyPreferenceOrPrimaryUseCase: "body type or primary use case",
  familySizeOrOccupancyUse: "family size or occupancy use",
  fuelPreferenceOrMonthlyRunning: "fuel preference or monthly running",
  transmissionPreference: "transmission preference",
  safetyPriority: "safety priority",
  featurePriority: "feature priorities",
  shortlistedModelsOrDiscoveryScope: "shortlisted cars or discovery scope",
});

const getDecisionMissingInputLabels = (items = []) =>
  (Array.isArray(items) ? items : [])
    .map((item) => DECISION_BUYER_CONTEXT_LABELS[item] || String(item || "").replace(/([A-Z])/g, " $1").toLowerCase().trim())
    .filter(Boolean);

const DECISION_SAFE_NEXT_CAPABILITIES = Object.freeze([
  "price",
  "features",
  "safety",
  "running cost",
  "variant value",
  "similar alternatives",
]);

const BUYER_GUIDANCE_TEMPLATE_BY_MODE = Object.freeze({
  practical_first_view: "decision_buyer_guidance_practical_first_view",
  conditional_guidance: "decision_buyer_guidance_conditional",
  sharpened_recommendation: "decision_buyer_guidance_sharpened_recommendation",
});

const formatGuidanceList = (items = []) => {
  const list = (Array.isArray(items) ? items : [items])
    .map((item) => cleanText(item))
    .filter(Boolean);
  if (!list.length) return "";
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  return `${list.slice(0, -1).join(", ")}, and ${list[list.length - 1]}`;
};

const optionalGuidanceLine = (value = "") => cleanText(value) || " ";

const buildBuyerGuidanceVehicleFactsLine = (facts = {}) => {
  const clauses = [];
  const bodyType = cleanText(facts.bodyType);
  const seatingCapacity = cleanText(facts.seatingCapacity);
  const fuelTypes = formatGuidanceList(facts.fuelTypes || []);
  const transmissions = formatGuidanceList(facts.transmissions || []);
  const priceBand = cleanText(facts.priceBand);
  const safetyFeatures = formatGuidanceList(facts.safetyFeatures || []);
  const featureHighlights = formatGuidanceList(facts.featureHighlights || []);
  const ownershipSignals = formatGuidanceList(facts.ownershipSignals || []);
  const similarAlternatives = formatGuidanceList(facts.similarAlternatives || []);

  if (bodyType) clauses.push(`body type ${bodyType}`);
  if (seatingCapacity) clauses.push(`${seatingCapacity}-seater`);
  if (fuelTypes) clauses.push(`available fuels ${fuelTypes}`);
  if (transmissions) clauses.push(`available transmissions ${transmissions}`);
  if (priceBand) clauses.push(`price band ${priceBand}`);
  if (safetyFeatures) clauses.push(`known safety features ${safetyFeatures}`);
  if (featureHighlights) clauses.push(`known feature highlights ${featureHighlights}`);
  if (ownershipSignals) clauses.push(`ownership signals ${ownershipSignals}`);
  if (similarAlternatives) clauses.push(`similar alternatives ${similarAlternatives}`);

  return clauses.join("; ");
};

const buildBuyerGuidanceContextLine = (buyerContext = {}) => {
  const entries = [
    ["city", buyerContext.city],
    ["budget", buyerContext.budgetOrPriceCeiling],
    ["use case", buyerContext.bodyPreferenceOrPrimaryUseCase],
    ["occupancy", buyerContext.familySizeOrOccupancyUse],
    ["running/fuel", buyerContext.fuelPreferenceOrMonthlyRunning],
    ["transmission", buyerContext.transmissionPreference],
    ["safety priority", buyerContext.safetyPriority],
    ["feature priority", formatGuidanceList(buyerContext.featurePriority || [])],
    ["shortlist/scope", buyerContext.shortlistedModelsOrDiscoveryScope],
  ]
    .map(([label, value]) => [label, cleanText(value)])
    .filter(([, value]) => value)
    .map(([label, value]) => `${label}: ${value}`);

  return entries.join("; ");
};


const extractMakeLabelFromBuyerMessage = (message = "") => {
  const raw = cleanText(message).toLowerCase();
  const makes = [
    ["maruti suzuki", "Maruti Suzuki"],
    ["maruti", "Maruti"],
    ["tata", "Tata"],
    ["hyundai", "Hyundai"],
    ["mahindra", "Mahindra"],
    ["honda", "Honda"],
    ["toyota", "Toyota"],
    ["kia", "Kia"],
    ["skoda", "Skoda"],
    ["volkswagen", "Volkswagen"],
    ["mg", "MG"],
    ["renault", "Renault"],
    ["nissan", "Nissan"],
    ["citroen", "Citroen"],
  ];
  const hit = makes.find(([key]) => new RegExp(`\\b${key.replace(/\\s+/g, "\\s+")}\\b`, "i").test(raw));
  return hit ? hit[1] : "";
};

const stripComparisonSideNoise = (value = "") =>
  cleanText(value)
    .replace(/^[,:;\-\s]+/g, "")
    .replace(/[,:;\-\s]+$/g, "")
    .replace(/^(?:which\s+one\s+should\s+i\s+(?:finally\s+|ultimately\s+)?(?:buy|choose|pick|go\s+for)|which\s+should\s+i\s+(?:finally\s+|ultimately\s+)?(?:buy|choose|pick|go\s+for)|should\s+i\s+(?:buy|choose|pick|go\s+for|purchase)|i\s+should\s+(?:buy|choose|pick|go\s+for|purchase)|(?:finally|ultimately)\s+(?:buy|choose|pick|go\s+for)|choose|pick|buy|go\s+for)[,:;\-\s]+/i, "")
    .replace(/\b(?:which\s+is\s+better|which\s+one\s+is\s+better|which\s+should\s+i\s+choose|which\s+should\s+i\s+pick|for\s+family|for\s+me|overall|finally|ultimately)\b.*$/i, "")
    .trim();

const extractComparisonSubjectLabelFromMessage = (message = "") => {
  const raw = cleanText(message).replace(/[?!.]+$/g, "");
  if (!raw) return "";

  const scopedText = raw.includes(":") ? raw.split(":").pop() : raw;

  const betweenMatch = scopedText.match(/\bbetween\s+(.+?)\s+and\s+(.+?)(?:\s+(?:which|for|overall)\b.*)?$/i);
  const parts = betweenMatch
    ? [betweenMatch[1], betweenMatch[2]]
    : scopedText.split(/\s+(?:vs|v\/s|versus|against|or)\s+/i);

  if (parts.length < 2) return "";

  const left = stripComparisonSideNoise(parts[0]);
  const right = stripComparisonSideNoise(parts[1]);

  if (!left || !right) return "";
  if (/^(?:this|that|one|car|variant|model)$/i.test(left) || /^(?:this|that|one|car|variant|model)$/i.test(right)) return "";

  return formatBuyerGuidanceDisplayLabel(`${left} vs ${right}`);
};

const buildBuyerGuidanceSubjectLabel = ({ facts = {}, evidencePack = {}, response = {}, bridge = {} } = {}) => {
  const subject = evidencePack.subject || {};
  const scope = cleanText(evidencePack.scope || "");
  const comparisonTargets = Array.isArray(subject.comparisonTargets) ? subject.comparisonTargets : [];
  const messageComparisonLabel = extractComparisonSubjectLabelFromMessage(
    bridge.effectiveMessage ||
      bridge.originalMessage ||
      response.effectiveMessage ||
      response.originalMessage ||
      response.message ||
      response.query ||
      "",
  );
  const labelOf = (vehicle = {}) =>
    cleanText(vehicle.label || vehicle.fullModel || [vehicle.make, vehicle.model, vehicle.variant].filter(Boolean).join(" "));

  if (scope === "comparison_scope" && comparisonTargets.length < 2 && messageComparisonLabel) return messageComparisonLabel;

  if (scope === "comparison_scope" && comparisonTargets.length >= 2) {
    const messageText = cleanText(
      bridge.effectiveMessage ||
        bridge.originalMessage ||
        response.effectiveMessage ||
        response.originalMessage ||
        response.message ||
        response.query ||
        "",
    ).toLowerCase();

    const targetIndex = (vehicle = {}) => {
      const candidates = [
        vehicle.label,
        vehicle.fullModel,
        [vehicle.make, vehicle.model, vehicle.variant].filter(Boolean).join(" "),
        vehicle.model,
        vehicle.variant,
        vehicle.make,
      ]
        .map((value) => cleanText(value))
        .filter(Boolean);

      let best = Number.MAX_SAFE_INTEGER;
      for (const candidate of candidates) {
        const index = messageText.indexOf(candidate.toLowerCase());
        if (index >= 0 && index < best) best = index;
      }
      return best;
    };

    const orderedTargets = messageText
      ? [...comparisonTargets].sort((a, b) => targetIndex(a) - targetIndex(b))
      : comparisonTargets;

    return orderedTargets.map(labelOf).filter(Boolean).join(" vs ");
  }

  if (scope === "upgrade_scope") {
    const base = labelOf(subject.upgradeBase || {});
    const target = labelOf(subject.upgradeTarget || {});
    if (base && target) return `${base} to ${target}`;
  }

  if (scope === "make_scope") return cleanText(subject.make || extractMakeLabelFromBuyerMessage(bridge.effectiveMessage || bridge.originalMessage || "") || facts.make || facts.brand || "this make");
  if (scope === "discovery_scope" && messageComparisonLabel) return messageComparisonLabel;
  if (scope === "discovery_scope") return cleanText(subject.discoveryLabel || response.title || "your car search");
  if (scope === "variant_scope" && (facts.variant || subject.variant)) {
    return cleanText(
      [facts.make || facts.brand || subject.make, facts.model || subject.model, facts.variant || subject.variant].filter(Boolean).join(" "),
    );
  }

  return cleanText(
    facts.fullModel ||
      [facts.make || facts.brand || subject.make, facts.model || subject.model, facts.variant || subject.variant].filter(Boolean).join(" ") ||
      response.anchorFullModel ||
      response.anchorModel ||
      (scope === "upgrade_scope" ? "this upgrade" : scope === "comparison_scope" ? "this comparison" : "this choice"),
  );
};

const buyerSafeScoreSignalText = (key = "", value = {}) => {
  const band = cleanText(value?.band || value?.status || value?.label || "").toLowerCase().replace(/_/g, " ");
  const strong = /\b(strong|good|high)\b/.test(band);
  const weak = /\b(weak|very weak|poor|low)\b/.test(band);
  const average = /\b(average|moderate|ok)\b/.test(band);

  if (key === "features") return strong ? "feature evidence looks positive" : weak ? "feature evidence needs comparison" : average ? "feature evidence looks adequate" : "";
  if (key === "value") return strong ? "value evidence looks positive versus nearby variants" : weak ? "value evidence needs nearby-variant comparison" : average ? "value evidence needs nearby-variant comparison" : "";
  if (key === "runningCost") return strong ? "running-cost evidence looks positive" : weak ? "running-cost evidence is not the main reason to choose it" : average ? "running-cost evidence looks acceptable" : "";
  if (key === "safety") return strong ? "safety evidence looks positive, but verify source applicability" : "safety evidence needs verified-source review";
  if (key === "familyPracticality") return strong ? "family-practicality evidence looks positive" : weak ? "family-practicality evidence needs use-case review" : average ? "family-practicality evidence looks acceptable" : "";
  if (key === "comfort") return strong ? "comfort evidence looks positive" : weak ? "comfort evidence needs comparison" : average ? "comfort evidence looks acceptable" : "";
  if (key === "regretRisk") return strong || weak || average ? "regret-risk evidence needs use-case review" : "";

  return "";
};

const formatScoreSignalLine = (scoreSignals = {}) => {
  return formatGuidanceList(
    Object.entries(scoreSignals || {})
      .map(([key, value]) => buyerSafeScoreSignalText(key, value))
      .filter(Boolean)
  );
};


const buildBuyerGuidanceEvidenceValues = (evidencePack = {}) => {
  const scoreLine = formatScoreSignalLine(evidencePack.scoreSignals || {});
  return {
    strengths: formatGuidanceList([
      ...(Array.isArray(evidencePack.strengths) ? evidencePack.strengths : []),
      scoreLine,
    ].filter(Boolean)),
    watchouts: formatGuidanceList(evidencePack.watchouts || []),
    fit: formatGuidanceList(evidencePack.fitSignals || []),
    alternatives: formatGuidanceList(evidencePack.alternativeSignals || []),
    upgrade: formatGuidanceList(evidencePack.upgradeSignals || []),
  };
};

const BUYER_GUIDANCE_UPPER_TOKENS = new Set([
  "ABS",
  "ADAS",
  "AMT",
  "AT",
  "CNG",
  "CVT",
  "DCT",
  "EV",
  "EBD",
  "IVT",
  "MT",
  "SUV",
  "MPV",
  "MUV",
]);

const titleCaseBuyerGuidanceLabelSide = (value = "") => {
  return cleanText(value)
    .split(/\s+/)
    .map((token) => {
      const cleanToken = cleanText(token);
      if (!cleanToken) return "";

      const upper = cleanToken.toUpperCase();
      const lower = cleanToken.toLowerCase();

      if (BUYER_GUIDANCE_UPPER_TOKENS.has(upper)) return upper;
      if (/^\d/.test(cleanToken)) return cleanToken;
      if (/^[A-Z0-9-]{2,}$/.test(cleanToken)) return cleanToken;
      if (/^(and|or|with|for|to|vs|versus)$/i.test(cleanToken)) return lower;

      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .filter(Boolean)
    .join(" ");
};

const formatBuyerGuidanceDisplayLabel = (value = "", { preserveSentenceCase = false } = {}) => {
  const text = cleanText(value);
  if (!text) return "";
  if (preserveSentenceCase) return text;

  return text
    .split(/\s+to\s+/i)
    .map((part) =>
      part
        .split(/\s+vs\s+/i)
        .map(titleCaseBuyerGuidanceLabelSide)
        .join(" vs ")
    )
    .join(" to ");
};

const buildBuyerGuidanceOpeningLine = ({ scope = "", model = "" } = {}) => {
  if (scope === "make_scope") {
    return `For ${model}, I would keep this at brand level first: the exact model, budget, use case, and any ownership or resale evidence matter more than the badge alone.`;
  }
  if (scope === "variant_scope") {
    return `For ${model}, I would not treat this as a final yes/no yet; I would first check features, price gap, and regret-risk evidence.`;
  }
  if (scope === "comparison_scope") {
    return `For ${model}, treat this as a trade-off check, not a single winner yet.`;
  }
  if (scope === "upgrade_scope") {
    return `For ${model}, treat this as an upgrade-value call: the added benefits need to justify the extra spend for your use case.`;
  }
  if (scope === "discovery_scope" && /\s+vs\s+/i.test(model)) {
    return `For ${model}, treat this as a trade-off check, not a single winner yet.`;
  }
  if (scope === "discovery_scope") {
    return `For ${model}, I can keep this as provisional discovery guidance around budget, use case, and shortlist quality.`;
  }
  return `For ${model}, I would not treat this as a final yes/no yet.`;
};

const buildBuyerGuidanceUsefulViewLine = ({ factsLine = "", buyerContextLine = "", scope = "" } = {}) => {
  if (factsLine) {
    return `What I can use now: ${factsLine}.`;
  }

  if (scope === "make_scope") {
    return "The next step is to pin down the model, budget, and use case, because make-level guidance should stay broad.";
  }

  if (scope === "discovery_scope") {
    return "The next step is to compare shortlisted options on safety, features, value, running cost, and family practicality once those signals are available.";
  }

  if (buyerContextLine) {
    return "";
  }

  return "";
};

const buildBuyerGuidanceLineInput = ({ model = "", facts = {}, guidance = {}, evidencePack = {} } = {}) => {
  const scope = cleanText(evidencePack.scope || guidance.scope || "");
  const displayModel = formatBuyerGuidanceDisplayLabel(model, { preserveSentenceCase: scope === "discovery_scope" });
  const factsLine = buildBuyerGuidanceVehicleFactsLine(facts);
  const buyerContextLine = buildBuyerGuidanceContextLine(guidance.explicitBuyerContext || {});
  const evidence = buildBuyerGuidanceEvidenceValues(evidencePack);
  const assumptions = formatGuidanceList(guidance.softAssumptions || []);
  const sentenceFragment = (value = "") => cleanText(value).replace(/[.?!]+$/g, "");
  const softQuestion =
    scope === "make_scope"
      ? `Which ${displayModel || model} model are you considering?`
      : scope === "discovery_scope" && /\s+vs\s+/i.test(displayModel || model)
        ? "What matters more here: safety, features, mileage/running cost, or family comfort?"
        : cleanText(guidance.softQuestion) || "Is your use mostly city, highway, or mixed?";

  return {
    model: displayModel || model,
    openingLine: optionalGuidanceLine(buildBuyerGuidanceOpeningLine({ scope, model: displayModel || model })),
    usefulViewLine: optionalGuidanceLine(buildBuyerGuidanceUsefulViewLine({ factsLine, buyerContextLine, scope })),
    strengthLine: optionalGuidanceLine(evidence.strengths ? `What looks good: ${evidence.strengths}.` : ""),
    watchoutLine: optionalGuidanceLine(evidence.watchouts ? `What to check: ${evidence.watchouts}.` : ""),
    fitLine: optionalGuidanceLine(evidence.fit ? `This fits better when: ${evidence.fit}.` : ""),
    alternativeLine: optionalGuidanceLine(evidence.alternatives ? `Compare alternatives if: ${evidence.alternatives}.` : ""),
    upgradeLine: optionalGuidanceLine(evidence.upgrade ? `For the upgrade: ${sentenceFragment(evidence.upgrade)}.` : ""),
    assumptionLine: "",
    softQuestion: optionalGuidanceLine(softQuestion ? `Best next question: ${softQuestion}` : ""),
  };
};



const scoreSignalToBuyerLine = (key = "", signal = {}) => {
  const band = cleanText(signal?.band || signal?.status || signal?.label || "").toLowerCase().replace(/_/g, " ");
  const strong = /\b(strong|good|high)\b/.test(band);
  const weak = /\b(weak|very weak|poor|low)\b/.test(band);
  const average = /\b(average|moderate|ok)\b/.test(band);

  if (key === "features") return strong ? "feature equipment looks strong" : weak ? "feature equipment may be limited" : average ? "feature equipment looks adequate" : "";
  if (key === "value") return strong ? "value looks promising versus nearby variants" : weak ? "value may be a concern versus nearby variants" : average ? "value looks acceptable, but compare nearby variants" : "";
  if (key === "runningCost") return strong ? "running-cost signal looks strong" : weak ? "running-cost signal may not be the main reason to choose it" : average ? "running-cost signal looks acceptable" : "";
  if (key === "safety") return strong ? "safety evidence looks positive, but verify crash/source applicability" : "safety evidence needs a careful check before a family or highway decision";
  if (key === "familyPracticality") return strong ? "family practicality signal looks strong" : weak ? "family practicality may not be its strongest area" : average ? "family practicality looks acceptable for normal use" : "";
  if (key === "comfort") return strong ? "comfort signal looks positive" : weak ? "comfort may not be the strongest reason to choose it" : average ? "comfort looks acceptable" : "";
  if (key === "regretRisk") return weak || /\blow\b/.test(band) ? "regret risk does not look high, but depends on use case" : average ? "regret risk looks moderate, so compare nearby variants" : "";

  return "";
};

const buildBuyerGuidanceEvidenceLines = (evidencePack = {}) => {
  const scoreSignals = evidencePack.scoreSignals || {};
  const scoreLines = Object.entries(scoreSignals)
    .map(([key, signal]) => scoreSignalToBuyerLine(key, signal))
    .filter(Boolean);

  return {
    strengthsLine: formatGuidanceList([
      ...(Array.isArray(evidencePack.strengths) ? evidencePack.strengths : []),
      ...scoreLines,
    ]),
    watchoutsLine: formatGuidanceList(Array.isArray(evidencePack.watchouts) ? evidencePack.watchouts : []),
    fitLine: formatGuidanceList(Array.isArray(evidencePack.fitSignals) ? evidencePack.fitSignals : []),
    alternativeLine: formatGuidanceList(Array.isArray(evidencePack.alternativeSignals) ? evidencePack.alternativeSignals : []),
    upgradeLine: formatGuidanceList(Array.isArray(evidencePack.upgradeSignals) ? evidencePack.upgradeSignals : []),
  };
};

const isUnsafeBuyerGuidanceEvidenceLine = (line = "") =>
  /\b(taxonomy-driven|global-percentile|normalization|safetyScore|performance score v2|score snapshot|score profile|same-model value score|ground-clearance normalization|score excludes|not yet scored|diagnostic-only module scoring|power-to-weight unavailable|data missing|unavailable)\b/i.test(cleanText(line));

const buyerSafeEvidenceLine = (line = "") => {
  const text = cleanText(line);
  if (!text) return "";

  const numericScore = "\\d+(?:\\.\\d+)?\\s*[(][^)]+[)]";
  const replacements = [
    [new RegExp("\\bfeatures?\\s+score\\s+" + numericScore, "i"), "feature evidence looks positive"],
    [new RegExp("\\bvalue\\s+score\\s+" + numericScore, "i"), "value evidence needs nearby-variant comparison"],
    [new RegExp("\\brunning\\s*cost\\s+score\\s+" + numericScore, "i"), "running-cost evidence looks positive"],
    [new RegExp("\\bmileage\\s+score\\s+(?:is\\s+)?(?:not\\s+available|not\\s+fully\\s+scored|unavailable)", "i"), "mileage evidence is incomplete"],
    [new RegExp("\\bsafety\\s+score\\s+" + numericScore, "i"), "safety evidence needs verified-source review"],
    [new RegExp("\\bfamily\\s*practicality\\s+score\\s+" + numericScore, "i"), "family-practicality evidence needs use-case review"],
    [new RegExp("\\bcomfort\\s+score\\s+" + numericScore, "i"), "comfort evidence needs comparison"],
    [new RegExp("\\bregret\\s*risk\\s+score\\s+" + numericScore, "i"), "regret-risk evidence needs use-case review"],
  ];

  for (const [pattern, safeText] of replacements) {
    if (pattern.test(text)) return safeText;
  }

  if (/Feature-rich for its scoring context/i.test(text)) return "feature evidence looks positive";
  if (/Strong city-use suitability/i.test(text)) return "city-use evidence looks positive";
  if (/Strong mileage\/running-cost signal/i.test(text)) return "running-cost evidence looks positive";
  if (/Same-model value score is weak/i.test(text)) return "value evidence needs nearby-variant comparison";
  if (/Premium comfort score is limited/i.test(text)) return "comfort evidence needs comparison";
  if (/Safety\/crash applicability needs verified-source caution/i.test(text)) return "safety evidence needs verified-source review";

  if (isUnsafeBuyerGuidanceEvidenceLine(text)) return "";
  return text;
};


const uniqueBuyerSafeGuidanceList = (items = [], limit = 3) => {
  const seen = new Set();
  const out = [];
  for (const item of Array.isArray(items) ? items : [items]) {
    const safe = buyerSafeEvidenceLine(item);
    if (!safe) continue;
    const key = safe.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(safe);
    if (out.length >= limit) break;
  }
  return out;
};

const sanitizeBuyerGuidanceEvidenceLines = (lines = {}) => {
  const sanitizeField = (value = "", limit = 3) =>
    formatGuidanceList(uniqueBuyerSafeGuidanceList(String(value || "").split(/\s*,\s*|\s+and\s+|;\s*/i), limit));

  return {
    strengthsLine: sanitizeField(lines.strengthsLine, 3),
    watchoutsLine: sanitizeField(lines.watchoutsLine, 2),
    fitLine: sanitizeField(lines.fitLine, 1),
    alternativeLine: sanitizeField(lines.alternativeLine, 1),
    upgradeLine: sanitizeField(lines.upgradeLine, 1),
  };
};


const sanitizeRenderedBuyerGuidanceAnswer = (answer = "") => {
  let text = cleanText(answer);
  if (!text) return "";

  const rawScore = "\\d+(?:\\.\\d+)?\\s*[(][^)]+[)]";

  const replacements = [
    [new RegExp("\\bfeatures?\\s+score\\s+" + rawScore, "gi"), "feature evidence looks positive"],
    [new RegExp("\\bvalue\\s+score\\s+" + rawScore, "gi"), "value evidence needs nearby-variant comparison"],
    [new RegExp("\\brunning\\s*cost\\s+score\\s+" + rawScore, "gi"), "running-cost evidence looks positive"],
    [new RegExp("\\bmileage\\s+score\\s+(?:is\\s+)?(?:not\\s+available|not\\s+fully\\s+scored|unavailable)\\.?", "gi"), "mileage evidence is incomplete"],
    [new RegExp("\\bsafety\\s+score\\s+" + rawScore, "gi"), "safety evidence needs verified-source review"],
    [new RegExp("\\bfamily\\s*practicality\\s+score\\s+" + rawScore, "gi"), "family-practicality evidence needs use-case review"],
    [new RegExp("\\bcomfort\\s+score\\s+" + rawScore, "gi"), "comfort evidence needs comparison"],
    [new RegExp("\\bregret\\s*risk\\s+score\\s+" + rawScore, "gi"), "regret-risk evidence needs use-case review"],

    [/Feature-rich for its scoring context/gi, "feature equipment looks positive"],
    [/Strong city-use suitability/gi, "city-use suitability looks positive"],
    [/Strong mileage\/running-cost signal/gi, "running-cost evidence looks favourable"],
    [/Strong same-model value signal/gi, "value looks positive versus nearby variants"],
    [/Same-model value score is weak/gi, "value evidence needs nearby-variant comparison"],
    [/Premium comfort score is limited/gi, "comfort evidence needs comparison"],
    [/Safety\/crash applicability needs verified-source caution/gi, "safety evidence needs verified-source review"],
    [/mileage evidence is incomplete/gi, "mileage evidence is incomplete"],
    [/safety evidence needs verified-source caution/gi, "safety evidence needs verified-source review"],
    [/value evidence needs comparison with nearby variants/gi, "value evidence needs nearby-variant comparison"],
    [/comfort evidence is not the strongest reason to choose it/gi, "comfort evidence needs comparison"],

    [/Evidence-backed positives:/gi, "What looks good:"],
    [/Watch out for:/gi, "What to check:"],
    [/One useful next question:/gi, "Best next question:"],
  ];

  for (const [pattern, safeText] of replacements) {
    text = text.replace(pattern, safeText);
  }

  const unsafeTechnicalPattern = /\b(?:Feature score is|Safety-critical equipment|Ground-clearance|ground clearance|Highway score v2|Boot space data missing|CNG tank placement|NVH|tyre quality|braking feel|highway-assist features|taxonomy-driven|global-percentile|normalization|safetyScore|performance score v2|score snapshot|score profile|score excludes|not yet scored|diagnostic-only module scoring|power-to-weight unavailable|data missing or reduced|unavailable; practicality)\b/i;

  const canonicalEvidenceItem = (item = "", section = "") => {
    let value = cleanText(item)
      .replace(/^[,.;:\s]+|[,.;:\s]+$/g, "")
      .replace(/^and\s+/i, "")
      .trim();

    if (!value) return "";
    if (unsafeTechnicalPattern.test(value)) return "";
    if (value.length < 8) return "";

    const lower = value.toLowerCase();

    if (/feature|equipment/.test(lower)) {
      return section === "watchout" ? "" : "feature equipment";
    }

    if (/city-use|city use|city suitability/.test(lower)) {
      return section === "watchout" ? "" : "city-use suitability";
    }

    if (/running-cost|running cost/.test(lower)) {
      return section === "watchout" ? "" : "running-cost evidence";
    }

    if (/mileage/.test(lower)) {
      if (/incomplete|not available|not fully/.test(lower)) return "treat mileage evidence as incomplete";
      return section === "watchout" ? "" : "running-cost evidence";
    }

    if (/value|nearby variant|nearby-variant/.test(lower)) {
      return section === "positive"
        ? "value evidence"
        : "compare value against nearby variants";
    }

    if (/safety|crash|verified-source|verified source/.test(lower)) {
      return section === "positive" ? "" : "verify safety evidence for the exact variant";
    }

    if (/comfort|nvh|ride/.test(lower)) {
      return section === "positive" ? "" : "compare comfort against alternatives";
    }

    if (/family-practicality|family practicality/.test(lower)) {
      return section === "positive" ? "family-practicality evidence" : "check family practicality against your use case";
    }

    if (/regret-risk|regret risk/.test(lower)) {
      return section === "positive" ? "" : "check regret-risk against your actual use case";
    }

    if (/amt|cvt|ivt|torque converter|manual|automatic/.test(lower)) {
      return "test the AMT feel in traffic";
    }

    return value
      .replace(/\bFeature score is\b.*$/i, "")
      .replace(/\bGround-clearance\b.*$/i, "")
      .replace(/\bHighway score v2\b.*$/i, "")
      .replace(/\bBoot space data missing\b.*$/i, "")
      .replace(/\s{2,}/g, " ")
      .trim();
  };

  const joinItems = (items = [], limit = 3) => {
    const seen = new Set();
    const cleanItems = [];

    for (const item of items.filter(Boolean)) {
      const key = item.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      cleanItems.push(item);
      if (cleanItems.length >= limit) break;
    }

    if (cleanItems.length === 0) return "";
    if (cleanItems.length === 1) return cleanItems[0];
    if (cleanItems.length === 2) return cleanItems.join(" and ");
    return cleanItems.slice(0, -1).join(", ") + ", and " + cleanItems[cleanItems.length - 1];
  };

  const joinPositiveItems = (items = [], limit = 3) => {
    const seen = new Set();
    const cleanItems = [];

    for (const item of items.filter(Boolean)) {
      const key = item.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      cleanItems.push(item);
      if (cleanItems.length >= limit) break;
    }

    if (cleanItems.length === 0) return "";
    if (cleanItems.length === 1) return cleanItems[0] + " looks positive";
    return joinItems(cleanItems, limit) + " look positive";
  };

  const cleanListSection = (source = "", label = "", section = "") => {
    const markers = "(?:What looks good:|What to check:|This fits better when:|Compare alternatives if:|For the upgrade:|Assumption:|Best next question:|$)";
    const pattern = new RegExp("\\b" + label + ":\\s*([\\s\\S]*?)(?=\\s*" + markers + ")", "gi");

    return source.replace(pattern, (match, body) => {
      const rawItems = cleanText(body)
        .replace(/\band\s*$/i, "")
        .split(/\s*,\s*|\s+and\s+/i)
        .map((item) => canonicalEvidenceItem(item, section))
        .filter(Boolean);

      const joined = section === "positive"
        ? joinPositiveItems(rawItems, 3)
        : joinItems(rawItems, 3);

      return joined ? label + ": " + joined + ". " : "";
    });
  };

  text = text
    .replace(/\s*Buyer context captured:[^.]*\./gi, "")
    .replace(/\s*I can keep this provisional for now\./gi, "")
    .replace(/\s*I would keep this provisional until the exact use case, budget, and priority are clearer\./gi, "")
    .replace(/\s*I would keep this modest until your use case and priorities are clearer\./gi, "")
    .replace(/\s*Assumption:[^.]*\./gi, "");

  text = cleanListSection(text, "What looks good", "positive");
  text = cleanListSection(text, "What to check", "watchout");
  text = cleanListSection(text, "This fits better when", "positive");
  text = cleanListSection(text, "Compare alternatives if", "watchout");

  text = text.replace(
    /\s*(?:,?\s*(?:and\s+)?)?(?:Feature score is|Safety-critical equipment|Ground-clearance|ground clearance|Highway score v2|Boot space data missing|CNG tank placement|NVH|tyre quality|braking feel|highway-assist features|taxonomy-driven|global-percentile|normalization|safetyScore|performance score v2|score snapshot|score profile|score excludes|not yet scored|diagnostic-only module scoring|power-to-weight unavailable|data missing or reduced|unavailable; practicality)[^.]*[.;,]?/gi,
    ""
  );

  text = text
    .replace(/\b(?:openingLine|usefulViewLine|strengthLine|watchoutLine|fitLine|alternativeLine|upgradeLine|assumptionLine|softQuestion)\b/gi, "")
    .replace(/\{\{\s*(?:openingLine|usefulViewLine|strengthLine|watchoutLine|fitLine|alternativeLine|upgradeLine|assumptionLine|softQuestion)\s*\}\}/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  text = text
    .replace(/:\s*[.,]\s*/g, ": ")
    .replace(/,\s*,+/g, ", ")
    .replace(/,\s*(?:and\s*)?(?=\.|$)/g, "")
    .replace(/,\s*(?:and\s*)?(?=\s*(?:Best next question:|For the upgrade:))/g, ". ")
    .replace(/\b(?:What looks good|What to check|This fits better when|Compare alternatives if):\s*(?:\.|,|and\s*)*(?=\s*(?:Best next question:|For the upgrade:|$))/gi, "")
    .replace(/\band\s*\./gi, ".")
    .replace(/\s+\.\s*/g, ". ")
    .replace(/\.{2,}/g, ".")
    .replace(/\s{2,}/g, " ")
    .trim();

  return text;
};

const buildBuyerGuidanceAnswer = ({ eligibility = {}, bridge = {}, response = {} } = {}) => {
  const guidance = eligibility.buyerGuidanceContext || eligibility.buyerDecisionInput?.buyerGuidanceContext || {};
  const mode = cleanText(guidance.guidanceMode);
  const templateKey = BUYER_GUIDANCE_TEMPLATE_BY_MODE[mode];
  const facts = guidance.selectedVehicleFacts || {};
  const evidencePack = guidance.decisionEvidencePack || {};
  const model = buildBuyerGuidanceSubjectLabel({ facts, evidencePack, response, bridge });
  const buyerSafeEvidenceLines = sanitizeBuyerGuidanceEvidenceLines(buildBuyerGuidanceEvidenceLines(evidencePack));

  if (!templateKey || !model || model === "this choice") return "";

  return sanitizeRenderedBuyerGuidanceAnswer(renderAciLanguageText(
    templateKey,
    buildBuyerGuidanceLineInput({ model, facts, guidance, evidencePack }),
    {
      seed: [
        templateKey,
        bridge.effectiveMessage,
        bridge.originalMessage,
        model,
        mode,
        JSON.stringify(facts),
      ].filter(Boolean).join("|"),
    },
  ));
};

const isWeakGenericClarificationAnswer = (response = {}) => {
  const answer = cleanText(response.answer || response.data?.answer || "");
  return (
    response.intent === "clarification" ||
    response.tool === "clarification" ||
    /what would you like to check about the car\??/i.test(answer) ||
    /which car should i check this for\??/i.test(answer)
  );
};

const buildFinalRecommendationBlockedAnswer = ({ eligibility = {}, response = {}, bridge = {} } = {}) => {
  const buyerGuidanceAnswer = buildBuyerGuidanceAnswer({ eligibility, response, bridge });
  if (buyerGuidanceAnswer) return buyerGuidanceAnswer;

  const missingInputs = getDecisionMissingInputLabels(eligibility.missingMandatoryInputs);
  const templateKey = isWeakGenericClarificationAnswer(response)
    ? "decision_final_blocked_missing_context"
    : "decision_final_blocked_partial_results";

  return renderAciLanguageText(
    templateKey,
    {
      missingInputs,
      nextCapabilities: DECISION_SAFE_NEXT_CAPABILITIES,
    },
    {
      seed: [
        templateKey,
        bridge.effectiveMessage,
        bridge.originalMessage,
        response.intent,
        response.tool,
        missingInputs.join("|"),
      ].filter(Boolean).join("|"),
    },
  );
};

const DIAGNOSTIC_ONLY_NOTE_SENTENCE_PATTERN =
  /\s*(?:This score view is diagnostic-only and should not be treated as a final recommendation\.|Use this as directional scoring, not as a final purchase verdict\.|This is diagnostic-only, not a final recommendation\.|Treat this as diagnostic-only guidance, not a final recommendation\.)/gi;

const normalizeDiagnosticOnlyNotes = (answer = "") => {
  const text = cleanText(answer);
  if (!text) return "";
  const notes = text.match(DIAGNOSTIC_ONLY_NOTE_SENTENCE_PATTERN) || [];
  if (notes.length <= 1) return text;
  const base = cleanText(text.replace(DIAGNOSTIC_ONLY_NOTE_SENTENCE_PATTERN, " "));
  const note = cleanText(notes[0]);
  return cleanText(`${base} ${note}`);
};

const applyFinalRecommendationBlockedAnswer = (response = {}, { eligibility = {}, bridge = {} } = {}) => {
  if (eligibility?.requestedFinalRecommendation !== true) return response;

  const blockedAnswer = buildFinalRecommendationBlockedAnswer({ eligibility, bridge });
  const existingAnswer = normalizeDiagnosticOnlyNotes(String(
    response.answer ||
    response.clarification ||
    response.data?.answer ||
    response.data?.clarification ||
    ""
  ));

  const shouldPreserveExisting =
    existingAnswer &&
    existingAnswer !== blockedAnswer &&
    !isWeakGenericClarificationAnswer(response) &&
    !existingAnswer.toLowerCase().includes(blockedAnswer.toLowerCase());

  const guidanceMode = eligibility.provisionalGuidanceMode || eligibility.buyerGuidanceContext?.guidanceMode || "";
  const effectiveShouldPreserveExisting = guidanceMode ? false : shouldPreserveExisting;
  const answer = effectiveShouldPreserveExisting
    ? `${blockedAnswer}\n\n${existingAnswer}`
    : blockedAnswer;
  const decisionScope = eligibility.buyerGuidanceContext?.decisionEvidencePack?.scope || "";
  const guidanceTitle = guidanceMode ? "Practical guidance" : "Need one detail";

  const finalBlockedUx = {
    status: guidanceMode ? "provisional_buyer_guidance" : "final_recommendation_blocked",
    requestedFinalRecommendation: true,
    finalRecommendationEnabled: false,
    canUseForFinalRecommendation: false,
    provisionalGuidanceMode: guidanceMode,
    decisionScope,
    allowedAnswerType: eligibility.allowedAnswerType || "",
    safeAnswerTypesNow: [
      "diagnostic_only",
      "clarification_required",
      "recovery_required",
      "fact_only",
    ],
  };

  return {
    ...response,
    title: response.title === "Need one detail" || guidanceMode ? guidanceTitle : response.title,
    answer,
    clarification: response.clarification || answer,
    finalBlockedUx,
    data: {
      ...(response.data || {}),
      title: response.data?.title === "Need one detail" || guidanceMode ? guidanceTitle : response.data?.title,
      answer,
      clarification: response.data?.clarification || answer,
      finalBlockedUx,
    },
    meta: {
      ...(response.meta || {}),
      finalBlockedUx,
    },
  };
};

const getDecisionDiagnosticOnlyNote = (seed = "") =>
  renderAciLanguageText("decision_diagnostic_only_note", {}, { seed }) ||
  "Diagnostic-only output.";

const collapseDuplicateDiagnosticOnlyNotes = (answer = "") => {
  const text = cleanText(answer);
  if (!text) return "";

  const diagnosticOnlyNotePattern = /\s*(?:This score view is diagnostic-only and should not be treated as a final recommendation\.|Use this as directional module-score diagnostics, not as a final purchase verdict\.|Use this as directional scoring, not as a final purchase verdict\.|This is diagnostic-only, not a final recommendation\.|Treat this as diagnostic-only guidance, not a final recommendation\.|This is a diagnostic signal only; it is not a final purchase recommendation\.|Use this as directional context\.)/gi;
  let seen = false;

  return cleanText(text.replace(diagnosticOnlyNotePattern, (match) => {
    if (seen) return "";
    seen = true;
    return ` ${cleanText(match)}`;
  }));
};

const ensureDiagnosticOnlyAnswerNote = (answer = "") => {
  const text = collapseDuplicateDiagnosticOnlyNotes(answer);
  const note = getDecisionDiagnosticOnlyNote(text);
  if (!text) return note;
  if (/\bdiagnostic-only\b|\bnot\s+(?:a\s+)?final recommendation\b|\bshould not be treated as a final recommendation\b|\bdirectional module-score diagnostics\b|\bdirectional context\b/i.test(text)) return collapseDuplicateDiagnosticOnlyNotes(text);
  return `${text} ${note}`;
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
  const rawOrderText = String(raw || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const getScoreDiagnosticModelMentionPosition = (modelKey = "", fallbackIndex = 0) => {
    const normalizedModelKey = normalizeFastPathSlug(modelKey);
    const aliases = [
      normalizedModelKey,
      normalizedModelKey.replace(/-/g, " "),
      normalizedModelKey.replace(/_/g, " "),
    ]
      .map((value) =>
        String(value || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, " ")
          .replace(/\s+/g, " ")
          .trim()
      )
      .filter(Boolean);

    const positions = aliases
      .map((alias) => rawOrderText.indexOf(alias))
      .filter((position) => position >= 0);

    return positions.length ? Math.min(...positions) : Number.MAX_SAFE_INTEGER;
  };

  const uniqueModelKeys = [
    ...new Set([
      ...(hasContextReference(raw) && contextModelKey ? [contextModelKey] : []),
      ...modelKeys,
    ]),
  ]
    .map((modelKey, index) => ({
      modelKey,
      index,
      position: getScoreDiagnosticModelMentionPosition(modelKey, index),
    }))
    .sort((left, right) => {
      if (left.position !== right.position) return left.position - right.position;
      return left.index - right.index;
    })
    .map((item) => item.modelKey)
    .slice(0, 2);

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
  if (
    meaningFrame?.clarification?.reason === "exact_variant_unavailable" ||
    meaningFrame?.trace?.variantResolution?.status === "exact_unavailable"
  ) {
    return null;
  }

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

const getScoreModelCandidateFromSnapshot = (candidateSnapshot = {}, message = "") => {
  const normalizedMessage = normalizeVariantIdentityText(message);
  const isUseCaseOnlyModelToken = (field = "") => {
    // Prevent "city" use-case phrases from resolving to Honda City when another
    // model is explicitly mentioned, e.g. "Best Baleno for city driving?".
    if (field !== "city") return false;
    return /(^|\s)city\s+(driving|drive|use|usage|commute|commuting|traffic)($|\s)/.test(normalizedMessage);
  };

  const scoreExplicitModelMatch = (candidate = {}) => {
    const fields = [
      candidate.fullModel,
      candidate.model,
      candidate.modelKey,
      candidate.shortModelKey,
    ]
      .map((value) => normalizeVariantIdentityText(value))
      .filter(Boolean);

    let score = Number(candidate.confidence || 0);
    let explicitMention = false;

    for (const field of fields) {
      const escaped = field.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const tokenCount = field.split(/\s+/).filter(Boolean).length;

      if (field && new RegExp(`(^|\\s)${escaped}(\\s|$)`).test(normalizedMessage)) {
        if (tokenCount === 1 && isUseCaseOnlyModelToken(field)) {
          continue;
        }

        explicitMention = true;
        score += 5000 + tokenCount * 100;
        continue;
      }

      const tokens = field.split(/\s+/).filter(Boolean);
      if (
        tokens.length > 1 &&
        tokens.every((token) =>
          new RegExp(`(^|\\s)${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\s|$)`).test(normalizedMessage)
        )
      ) {
        explicitMention = true;
        score += 2500 + tokens.length * 80;
      }
    }

    return { score, explicitMention };
  };

  const models = asArray(candidateSnapshot?.vehicles?.models)
    .map((item = {}) => {
      const raw = item.metadata?.raw || {};
      const candidate = {
        make: item.metadata?.make || raw.make || raw.brand || "",
        model: item.metadata?.model || raw.rawModel || raw.model || item.displayName || "",
        fullModel: raw.fullModel || raw.displayName || item.displayName || [raw.make || raw.brand, raw.model || raw.rawModel].filter(Boolean).join(" "),
        modelKey: raw.modelKey || item.canonicalKey || "",
        shortModelKey: raw.shortModelKey || "",
        confidence: Number(item.confidence || raw.confidence || 0),
      };
      const scored = scoreExplicitModelMatch(candidate);

      return {
        ...candidate,
        score: scored.score,
        explicitMention: scored.explicitMention,
      };
    })
    .filter((item) => item.model || item.fullModel);

  const eligibleModels = models.some((item) => item.explicitMention)
    ? models.filter((item) => item.explicitMention)
    : models;

  const best = eligibleModels
    .sort((left, right) => right.score - left.score || right.confidence - left.confidence)[0];

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
  const modelCandidate = getScoreModelCandidateFromSnapshot(candidateSnapshot, message);
  const stateVehicle =
    contextState?.selectedVehicle ||
    contextState?.anchors?.primaryVehicle ||
    context?.selectedVehicle ||
    {};
  const rawVariantToken = normalizeVariantIdentityText(variantCandidate.variant || variantCandidate.variantName);
  const normalizedMessage = normalizeVariantIdentityText(message);
  const hasExplicitVariant =
    rawVariantToken &&
    rawVariantToken.length >= 2 &&
    new RegExp(`(^|\\s)${rawVariantToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\s|$)`).test(normalizedMessage);
  const hasContextVariant =
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
      (hasExplicitVariant ? (variantCandidate.variant || variantCandidate.variantName) : "") ||
      stateVehicle.variant ||
      stateVehicle.variantName ||
      "",
    variantName:
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

  const hasSelectedVehicleContext = Boolean(
    stateVehicle.modelKey ||
    stateVehicle.model ||
    stateVehicle.fullModel ||
    stateVehicle.shortModelKey
  );

  if (!modelCandidate.explicitMention && !hasSelectedVehicleContext) {
    return null;
  }

  const asksValue = /\b(value|worth|value for money|good value)\b/i.test(message);
  const asksUpgrade =
    /\b(worth over|worth upgrading|upgrade|gain from|what do i gain|pay extra|extra over|over|vs|versus)\b/i.test(message);

  const operation = asksUpgrade && (hasExplicitVariant || hasContextVariant)
    ? "variant_upgrade_insight"
    : hasExplicitVariant || hasContextVariant
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

  const composed = composeAciAnswer({
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

  return attachDecisionRuntimeEnvelope(composed, { bridge, context: getContextForToolPlan(contextState) });
};

const listFrom = (value) => (Array.isArray(value) ? value : []);

const getDecisionRuntimeModule = ({ bridge = {}, response = {} } = {}) => {
  const tool = bridge.tool || response.tool || response.executorTool || "";
  const primaryTask = bridge.primaryTask || "";

  if (tool === "vehicle_score_insight" || primaryTask === "score_insight") {
    return DECISION_MODULES.SCORE_INSIGHT;
  }

  if (tool === "vehicle_similar" || primaryTask === "similar_cars") {
    return DECISION_MODULES.SIMILAR_CARS;
  }

  return "";
};

const getDecisionRuntimeSourceClass = (moduleName = "") => {
  if (moduleName === DECISION_MODULES.SCORE_INSIGHT) {
    return SOURCE_CLASSES.INTERNAL_SCORE_PROFILE;
  }

  if (moduleName === DECISION_MODULES.SIMILAR_CARS) {
    return SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE;
  }

  return SOURCE_CLASSES.MIXED;
};

const getDecisionRuntimeRows = (response = {}) => {
  const candidates = [
    response.rows,
    response.variants,
    response.items,
    response.similarModels,
    response.models,
    response.modelSummaries,
    response.moduleComparisons,
    response.data?.rows,
    response.data?.variants,
    response.data?.items,
    response.data?.similarModels,
    response.data?.models,
    response.data?.modelSummaries,
    response.data?.moduleComparisons,
  ];

  return candidates.find((value) => Array.isArray(value) && value.length > 0) || [];
};

const getDecisionRuntimeSourceCollections = (response = {}) => {
  const collections = [
    ...listFrom(response.sourceCollections),
    ...listFrom(response.data?.sourceCollections),
    ...listFrom(response.trace?.sourceCollections),
    ...listFrom(response.trace?.collectionsUsed),
    ...listFrom(response.sourceTransparency?.modulesChecked),
    ...listFrom(response.data?.sourceTransparency?.modulesChecked),
  ].filter(Boolean);

  return [...new Set(collections)];
};

const buildDecisionRuntimeEnvelope = ({ response = {}, bridge = {} } = {}) => {
  const moduleName = getDecisionRuntimeModule({ bridge, response });
  if (!moduleName) return null;

  const rows = getDecisionRuntimeRows(response);
  const sourceCollections = getDecisionRuntimeSourceCollections(response);
  const dataStatus = response.dataStatus || response.data?.dataStatus || response.meta?.dataStatus || "";
  const hasUsefulRows =
    rows.length > 0 ||
    Number(response.count || response.matched || response.modelCount || response.rowsCount || 0) > 0;

  const sourceClass = getDecisionRuntimeSourceClass(moduleName);
  const buildVersion =
    response.meta?.buildVersion ||
    response.meta?.graphVersion ||
    response.meta?.source ||
    response.data?.buildVersion ||
    response.data?.graphVersion ||
    "aci_decision_runtime_envelope_v1";

  const evidenceStatus =
    response.evidence?.evidenceStatus ||
    response.data?.evidence?.evidenceStatus ||
    (dataStatus === "available" || hasUsefulRows ? EVIDENCE_STATUS.PARTIAL : EVIDENCE_STATUS.MISSING);

  const evidenceConfidence =
    response.evidence?.confidence ||
    response.data?.evidence?.confidence ||
    CONFIDENCE_LEVELS.MEDIUM;

  const evidence = createBaseEvidence({
    ...(response.evidence || {}),
    ...(response.data?.evidence || {}),
    evidenceStatus,
    confidence: evidenceConfidence,
    usableEvidenceCount: Number(
      response.evidence?.usableEvidenceCount ??
      response.data?.evidence?.usableEvidenceCount ??
      (hasUsefulRows ? 1 : 0)
    ),
    requiredEvidenceCount: Number(
      response.evidence?.requiredEvidenceCount ??
      response.data?.evidence?.requiredEvidenceCount ??
      1
    ),
    sourceTransparency: response.sourceTransparency || response.data?.sourceTransparency || [],
  });

  const provenanceBase = createBaseProvenance({
    ...(response.provenance || {}),
    ...(response.data?.provenance || {}),
    buildVersion,
    builtAt: response.provenance?.builtAt || response.data?.provenance?.builtAt || new Date().toISOString(),
    sourceClass,
    needsRebuild: Boolean(response.provenance?.needsRebuild || response.data?.provenance?.needsRebuild),
  });

  const provenanceStatus = evaluateDecisionProvenance(provenanceBase, {
    maxStalenessDays: 30,
  });

  const provenance = {
    ...provenanceBase,
    status: provenanceStatus.status,
    ok: provenanceStatus.ok,
    sourceClassValid: provenanceStatus.sourceClassValid,
    stalenessDays: provenanceStatus.stalenessDays,
    staleByThreshold: provenanceStatus.staleByThreshold,
    issues: provenanceStatus.issues,
  };

  const trace = createBaseTrace({
    ...(response.trace || {}),
    toolRoute: bridge.tool || response.tool || "",
    collectionsUsed: sourceCollections,
    matchedRows: Number(
      response.trace?.matchedRows ??
      response.trace?.rowCount ??
      response.matched ??
      response.count ??
      rows.length ??
      0
    ),
    candidateCount: Number(
      response.trace?.candidateCount ??
      response.trace?.rowCount ??
      response.matched ??
      response.count ??
      rows.length ??
      0
    ),
  });

  const baseDecisionPolicy = createBaseDecisionPolicy({
    ...(response.decisionPolicy || {}),
    evidenceStatus,
    confidence: evidenceConfidence,
    canUseForFinalRecommendation: false,
    allowedAnswerType: hasUsefulRows
      ? ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY
      : ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED,
    claimType: CLAIM_TYPES.DIAGNOSTIC,
  });

  const profiled = applyDecisionPolicyWithModuleProfile({
    module: moduleName,
    intent: response.intent || "",
    decisionPolicy: baseDecisionPolicy,
    evidence,
    provenance,
    rows,
    trace,
    requestedFinalRecommendation: false,
  });

  const degraded = applyDecisionDegradedMode({
    ...profiled,
    module: moduleName,
    intent: response.intent || "",
    evidence,
    provenance,
    rows,
    trace,
    diagnostics: hasUsefulRows ? [{ module: moduleName, status: "diagnostic_runtime_output" }] : [],
  }, {
    provenance: {
      maxStalenessDays: 30,
    },
  });

  return {
    module: moduleName,
    decisionPolicy: {
      ...profiled.decisionPolicy,
      degradedMode: degraded.degradedMode || profiled.decisionPolicy.degradedMode || null,
      evidenceStatus,
      confidence: evidenceConfidence,
    },
    evidence,
    provenance,
    trace,
    degradedMode: degraded.degradedMode || null,
    sourceCollections,
  };
};

const FINAL_RECOMMENDATION_REQUEST_PATTERNS = Object.freeze([
  /\bshould\s+i\s+(buy|choose|pick|go\s+for|purchase|stretch|upgrade)\b/i,
  /\bwhich\s+(one|car|variant|model)\s+should\s+i\s+(?:finally\s+|ultimately\s+)?(buy|choose|pick|go\s+for)\b/i,
  /\bdecide\s+for\s+me\b/i,
  /\bfinal\s+(answer|recommendation|verdict|call|decision)\b/i,
  /\bbest\s+(car|option|choice|variant|model)\s+(for\s+me|to\s+buy|under|within)\b/i,
  /\brecommend\s+(me|one|a\s+car|the\s+best)\b/i,
  /\bworth\s+buying\b/i,
  /\bworth\s+(?:the\s+)?extra\b/i,
  /\bworth\s+upgrading\b/i,
]);

const isFinalChoiceRequestText = (message = "") =>
  FINAL_RECOMMENDATION_REQUEST_PATTERNS.some((pattern) => pattern.test(message));

const normalizeEvidenceSignal = (module = {}, scoreField = "score") => {
  const score = module?.[scoreField] ?? module?.score;
  return {
    score: score !== undefined && score !== null && score !== "" ? score : "",
    band: cleanText(module?.band || module?.status || module?.label),
    confidence: cleanText(module?.confidence),
    source: cleanText(module?.scoreType || module?.key),
  };
};

const buildScoreSignalsFromInsight = (insight = {}) => {
  const modules = insight?.modules || {};
  return {
    ...(modules.safety ? { safety: normalizeEvidenceSignal(modules.safety) } : {}),
    ...(modules.features ? { features: normalizeEvidenceSignal(modules.features) } : {}),
    ...(modules.value ? { value: normalizeEvidenceSignal(modules.value) } : {}),
    ...(modules.mileageRunningCost ? { runningCost: normalizeEvidenceSignal(modules.mileageRunningCost) } : {}),
    ...(modules.practicality ? { familyPracticality: normalizeEvidenceSignal(modules.practicality) } : {}),
    ...(modules.premiumComfort ? { comfort: normalizeEvidenceSignal(modules.premiumComfort) } : {}),
    ...(modules.regretRisk ? { regretRisk: normalizeEvidenceSignal(modules.regretRisk) } : {}),
  };
};

const getVehicleFromContextForGuidance = ({ context = {}, response = {}, candidateSnapshot = {} } = {}) => {
  const selected =
    context?.selectedVehicle ||
    context?.anchors?.primaryVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.contextState?.anchors?.primaryVehicle ||
    response?.data?.provenance?.selectedVehicle ||
    response?.provenance?.selectedVehicle ||
    {};
  const modelCandidate = asArray(candidateSnapshot?.vehicles?.models)[0] || {};
  const variantCandidate = asArray(candidateSnapshot?.vehicles?.variants)[0] || {};
  const makeCandidate = asArray(candidateSnapshot?.vehicles?.makes)[0] || {};
  const meta = selected?.metadata || selected?.raw || {};
  const variantMeta = variantCandidate?.metadata || variantCandidate?.raw || {};
  const modelMeta = modelCandidate?.metadata || modelCandidate?.raw || {};
  return {
    make: cleanText(selected.make || selected.brand || meta.make || meta.brand || modelMeta.make || modelMeta.brand || makeCandidate.displayName || makeCandidate.rawText),
    model: cleanText(selected.model || meta.model || modelMeta.model || modelCandidate.displayName || modelCandidate.rawText),
    variant: cleanText(selected.variant || selected.variantName || meta.variant || meta.variantName || variantMeta.variant || variantMeta.variantName || variantCandidate.displayName || variantCandidate.rawText),
  };
};

const buildDecisionEvidencePackInputFromScorePayload = ({ response = {}, scope = "", subject = {} } = {}) => {
  const data = response.data || {};
  const topLevel = response || {};
  const singleInsight = data.modules ? data : topLevel.modules ? topLevel : null;
  const variants = asArray(data.variants || topLevel.variants || data.rows || topLevel.rows);
  const representative = singleInsight || variants[0] || {};
  const sources = uniqueKeys([
    ...(asArray(data.sourceCollections || topLevel.sourceCollections)),
    representative.buildVersion,
    data.provenance?.buildVersion,
    topLevel.provenance?.buildVersion,
  ]);
  const strengths = [
    ...asArray(representative.strengths),
  ];
  const watchouts = asArray(representative.watchouts);
  const upgradeSignals =
    scope === "upgrade_scope"
      ? ["I can judge this properly once the price gap and feature gains are available."]
      : [];

  if (!singleInsight && !variants.length && !sources.length) return {};

  return {
    scope,
    subject,
    ...(singleInsight ? { variantScoreProfile: singleInsight } : {}),
    ...(variants.length ? { modelScoreProfile: { variants, count: variants.length } } : {}),
    scoreSignals: singleInsight ? buildScoreSignalsFromInsight(singleInsight) : {},
    strengths: uniqueKeys(strengths),
    watchouts: uniqueKeys(watchouts),
    upgradeSignals,
    evidenceSources: sources.length ? sources : ["score_profile_read_model"],
  };
};

const buildDecisionEvidencePackInputFromReadModel = async ({ context = {}, response = {}, bridge = {}, scope = "", subject = {} } = {}) => {
  if (!mongoose.connection?.db) return {};
  if (scope === "make_scope" || scope === "discovery_scope") return {};

  const vehicle = getVehicleFromContextForGuidance({
    context,
    response,
    candidateSnapshot: context?.candidateSnapshot || {},
  });
  const modelKey = keyify(subject.model || vehicle.model);
  const makeKey = keyify(subject.make || vehicle.make);
  if (!modelKey) return {};

  try {
    const modelInsights = await getModelScoreInsights({
      db: mongoose.connection.db,
      makeKey,
      modelKey,
      limit: 80,
    });
    const variants = asArray(modelInsights?.variants);
    if (!variants.length) return {};

    const requestedVariantKey = keyify(subject.variant || vehicle.variant);
    const selectedInsight = requestedVariantKey
      ? variants.find((variant) => keyify(variant.variantKey || variant.variantFullName).includes(requestedVariantKey))
      : null;
    const representative = selectedInsight || (scope === "variant_scope" ? variants[0] : null);
    const sources = uniqueKeys([
      "score_profile_read_model",
      representative?.buildVersion,
      ...variants.map((variant) => variant.buildVersion),
    ]);
    const strengths = [
      ...(representative ? asArray(representative.strengths) : []),
    ];
    const watchouts = representative ? asArray(representative.watchouts) : [];
    const upgradeSignals =
      scope === "upgrade_scope"
        ? ["I can judge this properly once the price gap and feature gains are available."]
        : [];

    return {
      scope,
      subject,
      ...(representative ? { variantScoreProfile: representative } : {}),
      modelScoreProfile: {
        variants,
        count: variants.length,
      },
      scoreSignals: representative ? buildScoreSignalsFromInsight(representative) : {},
      strengths: uniqueKeys(strengths),
      watchouts: uniqueKeys(watchouts),
      upgradeSignals,
      evidenceSources: sources,
    };
  } catch {
    return {};
  }
};

const buildHydratedFinalChoiceResponse = async (response = {}, { bridge = {}, context = {} } = {}) => {
  const message = bridge.effectiveMessage || bridge.originalMessage || "";
  if (!isFinalChoiceRequestText(message)) return response;

  const preliminaryEligibility = buildFinalRecommendationEligibilityRuntime({
    message,
    bridge,
    response,
    context,
  });
  const guidance = preliminaryEligibility?.buyerGuidanceContext || {};
  const scope = guidance.decisionEvidencePack?.scope || guidance.scope || "";
  const subject = guidance.decisionEvidencePack?.subject || {};
  const existing = response.decisionEvidencePack || response.data?.decisionEvidencePack || {};
  const payloadEvidence = buildDecisionEvidencePackInputFromScorePayload({ response, scope, subject });
  const readModelEvidence = await buildDecisionEvidencePackInputFromReadModel({ context, response, bridge, scope, subject });
  const decisionEvidencePack = {
    ...existing,
    ...payloadEvidence,
    ...readModelEvidence,
    subject: {
      ...(existing.subject || {}),
      ...(payloadEvidence.subject || {}),
      ...(readModelEvidence.subject || {}),
      ...subject,
    },
    scope: readModelEvidence.scope || payloadEvidence.scope || existing.scope || scope,
    strengths: uniqueKeys([
      ...asArray(existing.strengths),
      ...asArray(payloadEvidence.strengths),
      ...asArray(readModelEvidence.strengths),
    ]),
    watchouts: uniqueKeys([
      ...asArray(existing.watchouts),
      ...asArray(payloadEvidence.watchouts),
      ...asArray(readModelEvidence.watchouts),
    ]),
    upgradeSignals: uniqueKeys([
      ...asArray(existing.upgradeSignals),
      ...asArray(payloadEvidence.upgradeSignals),
      ...asArray(readModelEvidence.upgradeSignals),
    ]),
    evidenceSources: uniqueKeys([
      ...asArray(existing.evidenceSources),
      ...asArray(payloadEvidence.evidenceSources),
      ...asArray(readModelEvidence.evidenceSources),
    ]),
    scoreSignals: {
      ...(existing.scoreSignals || {}),
      ...(readModelEvidence.scoreSignals || {}),
      ...(payloadEvidence.scoreSignals || {}),
    },
  };

  if (!Object.keys(decisionEvidencePack).length) return response;

  return {
    ...response,
    decisionEvidencePack,
    data: {
      ...(response.data || {}),
      decisionEvidencePack,
    },
  };
};


const isMeaningfulBuyerContextValue = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean).length > 0;
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  if (value && typeof value === "object") {
    return Object.values(value).some((item) => isMeaningfulBuyerContextValue(item));
  }
  const text = cleanText(value);
  return Boolean(text) && text !== "0";
};

const compactBuyerContextForEligibility = (buyerContext = {}) => {
  if (!buyerContext || typeof buyerContext !== "object" || Array.isArray(buyerContext)) return {};
  const output = {};
  for (const [key, value] of Object.entries(buyerContext)) {
    if (isMeaningfulBuyerContextValue(value)) output[key] = value;
  }
  return output;
};

const mergeBuyerContextForEligibility = (...contexts) => {
  const merged = {};
  for (const context of contexts) {
    const compact = compactBuyerContextForEligibility(context);
    for (const [key, value] of Object.entries(compact)) {
      if (key === "featurePriority") {
        const values = [
          ...(Array.isArray(merged.featurePriority) ? merged.featurePriority : []),
          ...(Array.isArray(value) ? value : [value]),
        ].map((item) => cleanText(item)).filter(Boolean);
        merged.featurePriority = [...new Set(values)];
        continue;
      }
      merged[key] = value;
    }
  }
  return merged;
};

const getBuyerContextFromEligibilitySource = (source = {}) => {
  if (!source || typeof source !== "object") return {};
  return mergeBuyerContextForEligibility(
    source.buyerContext,
    source.buyerIntent,
    source.contextState?.buyerContext,
    source.contextState?.buyerIntent,
    source.aciContextState?.buyerContext,
    source.aciContextState?.buyerIntent,
    source.data?.buyerContext,
    source.data?.buyerIntent,
    source.data?.contextState?.buyerContext,
    source.data?.aciContextState?.buyerContext,
    source.meta?.buyerContext,
    source.meta?.buyerIntent,
    source.contextPatch?.buyerContext,
    source.contextPatch?.buyerIntent,
    source.contextPatch?.contextState?.buyerContext,
    source.contextPatch?.aciContextState?.buyerContext
  );
};


const isUnsupportedOrBlockedRecommendationResponse = (response = {}, bridge = {}) => {
  const dataStatus = String(response.dataStatus || response.data?.dataStatus || response.meta?.dataStatus || "").toLowerCase();
  const degradedMode = String(
    response.degradedMode ||
      response.data?.degradedMode ||
      response.meta?.degradedMode ||
      response.decisionPolicy?.degradedMode ||
      "",
  ).toLowerCase();

  const answer = String(response.answer || response.data?.answer || "").toLowerCase();
  const cityStatus = String(
    response.cityStatus ||
      response.data?.cityStatus ||
      response.meta?.cityStatus ||
      bridge.cityStatus ||
      "",
  ).toLowerCase();

  return (
    dataStatus === "unsupported_city" ||
    dataStatus === "no_data" ||
    dataStatus === "blocked" ||
    degradedMode === "unsupported_city" ||
    degradedMode.includes("unsupported_city") ||
    cityStatus === "unsupported" ||
    answer.includes("not supported") ||
    answer.includes("unsupported city") ||
    answer.includes("we currently support")
  );
};

const isRecommendationCandidateResolverEligible = (response = {}, bridge = {}) => {
  if (isUnsupportedOrBlockedRecommendationResponse(response, bridge)) return false;

  const tool = bridge.tool || response.tool || response.meta?.responseTool || response.data?.provenance?.tool || "";
  const intent = response.intent || response.data?.intent || "";
  const canvasType = response.canvasType || response.data?.canvasType || "";
  const dataStatus = String(response.dataStatus || response.data?.dataStatus || response.meta?.dataStatus || "").toLowerCase();

  const looksLikeRecommendation =
    tool === "vehicle_recommend" ||
    tool === "vehicle_recommendation" ||
    intent === "vehicle_recommendation" ||
    canvasType === "recommendation_results_canvas";

  if (!looksLikeRecommendation) return false;
  if (dataStatus && dataStatus !== "available") return false;

  const rows = getDecisionRuntimeRows(response);
  return rows.length > 0;
};

const unionList = (...lists) => [...new Set(lists.flatMap((list) => Array.isArray(list) ? list : []).filter(Boolean))];

const attachRecommendationCandidateResolverEvidence = async (response = {}, { bridge = {}, context = {} } = {}) => {
  if (!isRecommendationCandidateResolverEligible(response, bridge)) return response;

  const rows = getDecisionRuntimeRows(response);
  if (!rows.length) return response;

  const { buildRecommendationCandidateResolver } = await import("../candidates/aciRecommendationCandidateResolver.service.js");

  const buyerContext =
    response.buyerContext ||
    response.data?.buyerContext ||
    response.contextPatch?.buyerContext ||
    response.contextPatch?.contextState?.buyerContext ||
    context.buyerContext ||
    context.contextState?.buyerContext ||
    {};

  const resolved = await buildRecommendationCandidateResolver({
    rows,
    buyerContext,
    bridge,
    limit: rows.length,
  });

  if (!resolved?.ok || !Array.isArray(resolved.rows) || !resolved.rows.length) return response;

  const sourceCollections = unionList(
    response.sourceCollections,
    response.data?.sourceCollections,
    response.meta?.sourceCollections,
    response.sourceTransparency?.modulesChecked,
    resolved.sourceCollections,
  );

  const sourceTransparency = {
    ...(response.sourceTransparency || {}),
    modulesChecked: sourceCollections,
    matched: response.sourceTransparency?.matched ?? response.matched ?? resolved.totalCandidates,
    dataSource: response.sourceTransparency?.dataSource || "aci_vehicle_read_models",
    candidateResolver: resolved.version,
  };

  const contextPatch = {
    ...(response.contextPatch || {}),
    recommendationCandidateResolver: {
      version: resolved.version,
      evidenceStatus: resolved.evidenceStatus,
      enrichedCount: resolved.enrichedCount,
      totalCandidates: resolved.totalCandidates,
      finalRecommendationEnabled: false,
    },
  };

  return {
    ...response,
    rows: resolved.rows,
    items: resolved.rows,
    modelGroups: Array.isArray(response.modelGroups) ? resolved.rows : response.modelGroups,
    previewModelGroups: Array.isArray(response.previewModelGroups) ? resolved.rows : response.previewModelGroups,
    sourceCollections,
    sourceTransparency,
    contextPatch,
    evidence: {
      ...(response.evidence || {}),
      evidenceStatus: resolved.evidenceStatus,
      confidence: response.evidence?.confidence || "medium",
      usableEvidenceCount: resolved.enrichedCount,
      requiredEvidenceCount: Math.max(1, resolved.totalCandidates || resolved.rows.length),
      sourceTransparency,
    },
    data: {
      ...(response.data || {}),
      rows: resolved.rows,
      items: resolved.rows,
      modelGroups: Array.isArray(response.data?.modelGroups) ? resolved.rows : response.data?.modelGroups,
      previewModelGroups: Array.isArray(response.data?.previewModelGroups) ? resolved.rows : response.data?.previewModelGroups,
      sourceCollections,
      sourceTransparency,
      recommendationCandidateResolver: resolved,
      evidence: {
        ...(response.data?.evidence || {}),
        evidenceStatus: resolved.evidenceStatus,
        confidence: response.data?.evidence?.confidence || "medium",
        usableEvidenceCount: resolved.enrichedCount,
        requiredEvidenceCount: Math.max(1, resolved.totalCandidates || resolved.rows.length),
        sourceTransparency,
      },
    },
    meta: {
      ...(response.meta || {}),
      sourceCollections,
      recommendationCandidateResolver: {
        version: resolved.version,
        evidenceStatus: resolved.evidenceStatus,
        enrichedCount: resolved.enrichedCount,
        totalCandidates: resolved.totalCandidates,
        scoreProfileCount: resolved.scoreProfileCount,
        featureSummaryCount: resolved.featureSummaryCount,
        finalRecommendationEnabled: false,
      },
    },
    trace: {
      ...(response.trace || {}),
      recommendationCandidateResolver: {
        version: resolved.version,
        evidenceStatus: resolved.evidenceStatus,
        enrichedCount: resolved.enrichedCount,
        totalCandidates: resolved.totalCandidates,
      },
    },
  };
};


const buildEligibilityContextWithExtractedBuyerContext = async ({
  message = "",
  context = {},
  response = {},
} = {}) => {
  const { applyBuyerContextToContextState } = await import("../context/aciBuyerContextExtractor.service.js");

  const baseState =
    context.contextState ||
    context.aciContextState ||
    response.contextPatch?.contextState ||
    response.contextPatch?.aciContextState ||
    response.data?.contextState ||
    response.data?.aciContextState ||
    {};

  const extractedState = applyBuyerContextToContextState({
    message,
    contextState: baseState,
  });

  const buyerContext = mergeBuyerContextForEligibility(
    getBuyerContextFromEligibilitySource(context),
    getBuyerContextFromEligibilitySource(response),
    extractedState.buyerContext
  );

  const buyerGuidanceContext = {
    ...(baseState.buyerGuidanceContext || {}),
    ...(extractedState.buyerGuidanceContext || {}),
  };

  const contextState = {
    ...baseState,
    ...extractedState,
    buyerContext,
    buyerGuidanceContext,
  };

  const enrichedContextPatch = {
    ...(response.contextPatch || {}),
    buyerContext,
    buyerGuidanceContext,
    contextState: {
      ...(response.contextPatch?.contextState || {}),
      ...contextState,
    },
    aciContextState: {
      ...(response.contextPatch?.aciContextState || {}),
      ...contextState,
    },
  };

  const enrichedResponse = {
    ...response,
    buyerContext,
    buyerGuidanceContext,
    contextState,
    aciContextState: contextState,
    contextPatch: enrichedContextPatch,
    data: {
      ...(response.data || {}),
      buyerContext,
      buyerGuidanceContext,
      contextState,
      aciContextState: contextState,
    },
    meta: {
      ...(response.meta || {}),
      buyerContext,
      buyerGuidanceContext,
    },
  };

  const enrichedContext = {
    ...(context || {}),
    buyerContext,
    buyerGuidanceContext,
    contextState,
    aciContextState: contextState,
  };

  return {
    context: enrichedContext,
    response: enrichedResponse,
  };
};


const attachDecisionRuntimeEnvelope = async (response = {}, { bridge = {}, context = {} } = {}) => {
  const messageForEligibility = bridge.effectiveMessage || bridge.originalMessage || "";
  const rawResponseForEligibility = await buildHydratedFinalChoiceResponse(response, { bridge, context });
  const enrichedEligibilityInput = await buildEligibilityContextWithExtractedBuyerContext({
    message: messageForEligibility,
    context,
    response: rawResponseForEligibility,
  });
  const responseForEligibility = await attachRecommendationCandidateResolverEvidence(
    enrichedEligibilityInput.response,
    {
      bridge,
      context: enrichedEligibilityInput.context,
    },
  );
  const eligibilityContext = enrichedEligibilityInput.context;
  const envelope = buildDecisionRuntimeEnvelope({ response: responseForEligibility, bridge });
  const finalRecommendationEligibility = buildFinalRecommendationEligibilityRuntime({
    message: messageForEligibility,
    bridge,
    response: responseForEligibility,
    context: eligibilityContext,
    decisionPolicy: envelope?.decisionPolicy || responseForEligibility.decisionPolicy || responseForEligibility.data?.decisionPolicy || responseForEligibility.meta?.decisionPolicy || {},
    evidence: envelope?.evidence || responseForEligibility.evidence || responseForEligibility.data?.evidence || {},
  });
  const shouldAttachFinalRecommendationEligibility =
    finalRecommendationEligibility.requestedFinalRecommendation === true;

  if (!envelope) {
    if (!shouldAttachFinalRecommendationEligibility) return responseForEligibility;

    const responseWithFinalBlockedAnswer = applyFinalRecommendationBlockedAnswer(responseForEligibility, {
      eligibility: finalRecommendationEligibility,
      bridge,
    });

    return {
      ...responseWithFinalBlockedAnswer,
      finalRecommendationEligibility,
      data: {
        ...(responseWithFinalBlockedAnswer.data || {}),
        finalRecommendationEligibility,
      },
      meta: {
        ...(responseWithFinalBlockedAnswer.meta || {}),
        finalRecommendationEligibility,
      },
    };
  }

  const decisionPolicy = envelope.decisionPolicy;
  const responseWithFinalBlockedAnswer = shouldAttachFinalRecommendationEligibility
    ? applyFinalRecommendationBlockedAnswer(responseForEligibility, {
        eligibility: finalRecommendationEligibility,
        bridge,
      })
    : responseForEligibility;

  const enrichedBuyerContext = responseForEligibility.buyerContext || responseForEligibility.data?.buyerContext || {};
  const enrichedBuyerGuidanceContext = responseForEligibility.buyerGuidanceContext || responseForEligibility.data?.buyerGuidanceContext || {};
  const enrichedContextPatch = responseForEligibility.contextPatch || response.contextPatch || {};

  const data = {
    ...(responseWithFinalBlockedAnswer.data || {}),
    decisionPolicy,
    evidence: envelope.evidence,
    provenance: envelope.provenance,
    degradedMode: envelope.degradedMode,
    canUseForFinalRecommendation: false,
    ...(shouldAttachFinalRecommendationEligibility ? { finalRecommendationEligibility } : {}),
  };

  return {
    ...responseWithFinalBlockedAnswer,
    buyerContext: enrichedBuyerContext,
    buyerGuidanceContext: enrichedBuyerGuidanceContext,
    contextPatch: enrichedContextPatch,
    module: responseWithFinalBlockedAnswer.module || envelope.module,
    decisionPolicy,
    evidence: envelope.evidence,
    provenance: envelope.provenance,
    degradedMode: envelope.degradedMode,
    ...(shouldAttachFinalRecommendationEligibility ? { finalRecommendationEligibility } : {}),
    sourceCollections: envelope.sourceCollections.length
      ? envelope.sourceCollections
      : responseWithFinalBlockedAnswer.sourceCollections,
    trace: {
      ...(responseWithFinalBlockedAnswer.trace || {}),
      ...envelope.trace,
    },
    data: {
      ...data,
      buyerContext: enrichedBuyerContext,
      buyerGuidanceContext: enrichedBuyerGuidanceContext,
    },
    meta: {
      ...(responseWithFinalBlockedAnswer.meta || {}),
      buyerContext: enrichedBuyerContext,
      buyerGuidanceContext: enrichedBuyerGuidanceContext,
      decisionPolicy,
      evidenceStatus: envelope.evidence.evidenceStatus,
      evidenceConfidence: envelope.evidence.confidence,
      provenance: envelope.provenance,
      degradedMode: envelope.degradedMode,
      ...(shouldAttachFinalRecommendationEligibility ? { finalRecommendationEligibility } : {}),
    },
  };
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

const buildExplicitScoreModelPlanOverride = ({
  message = "",
  plan = {},
  candidateSnapshot = {},
} = {}) => {
  const firstTool = plan.tools?.[0] || {};
  const plannedTool = firstTool.tool || "";
  const plannedOperation = firstTool.operation || firstTool.input?.operation || firstTool.args?.operation || "";

  if (plannedTool !== "vehicle_score_insight" && plan.intent !== "vehicle_score_insight") {
    return null;
  }

  // Keep genuine variant-upgrade answers intact.
  if (plannedOperation === "variant_upgrade_insight") {
    return null;
  }

  const modelCandidate = getScoreModelCandidateFromSnapshot(candidateSnapshot, message);
  if (!modelCandidate?.explicitMention) return null;

  const variantCandidate = getScoreVariantCandidateFromSnapshot(candidateSnapshot);
  const rawVariantToken = normalizeVariantIdentityText(
    variantCandidate.variant || variantCandidate.variantName
  );
  const normalizedMessage = normalizeVariantIdentityText(message);
  const hasExplicitVariant =
    rawVariantToken &&
    rawVariantToken.length >= 2 &&
    new RegExp(`(^|\\s)${rawVariantToken.replace(/[.*+?^${}()|[\\]\\]/g, "\\$&")}(\\s|$)`).test(normalizedMessage);

  // If the user explicitly names a variant, let variant score logic handle it.
  if (hasExplicitVariant) return null;

  const modelKey =
    modelCandidate.shortModelKey ||
    modelCandidate.modelKey ||
    modelCandidate.model ||
    modelCandidate.fullModel ||
    "";
  if (!modelKey) return null;

  return {
    tool: "vehicle_score_insight",
    intent: "vehicle_score_insight",
    operation: "model_score_insights",
    routingReason: "explicit_score_model_plan_override",
    modelKey,
    makeKey: modelCandidate.makeKey || modelCandidate.make || modelCandidate.brand || "",
    fullModel: modelCandidate.fullModel || modelCandidate.model || modelKey,
  };
};

const applyExplicitScoreModelPlanOverride = ({ plan = {}, override = null, message = "" } = {}) => {
  if (!override) return plan;

  const baseTool = plan.tools?.[0] || {};
  const payload = {
    operation: override.operation,
    modelKey: override.modelKey,
    makeKey: override.makeKey,
    fullModel: override.fullModel,
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
          message,
          query: message,
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
          primaryVariant: "",
        },
        filters: {
          ...(baseTool.filters || {}),
        },
        output: {
          ...(baseTool.output || {}),
          canvasType: "score_insight_canvas",
          inlineType: "score_insight_summary",
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
      explicitScoreModelPlanOverride: true,
      explicitScoreModelRoutingReason: override.routingReason,
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

  const standaloneModelFeatureComparisonFastPath = await maybeReturnStandaloneModelFeatureComparisonFastPath({
    message,
    context,
    originalMessage,
    startedAt,
  });

  if (standaloneModelFeatureComparisonFastPath) {
    return standaloneModelFeatureComparisonFastPath;
  }

  const unsupportedCityFastPath = await maybeReturnUnsupportedCityFastPath({
    message,
    effectiveMessage,
    context,
    startedAt: typeof startedAt !== "undefined" ? startedAt : Date.now(),
  });

  if (unsupportedCityFastPath) {
    return unsupportedCityFastPath;
  }

  const explicitComparisonFastPath = await maybeReturnExplicitComparisonFastPath({
    message,
    context,
    user,
    session,
    meta,
    originalMessage,
    startedAt,
  });

  if (explicitComparisonFastPath) {
    return explicitComparisonFastPath;
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

  const vehicleColorsFastPath = await maybeReturnVehicleColorsFastPath({
    message,
    effectiveMessage,
    context,
    originalMessage,
    startedAt,
  });

  if (vehicleColorsFastPath) {
    return vehicleColorsFastPath;
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

  const exactSingleFeatureFastPath = await maybeReturnExactSingleFeatureFastPath({
    message,
    effectiveMessage,
    context,
    originalMessage,
    startedAt,
  });

  if (exactSingleFeatureFastPath) {
    return exactSingleFeatureFastPath;
  }

  const standaloneModelFeatureFastPath = await maybeReturnStandaloneModelFeatureFastPath({
    message,
    context,
    user,
    session,
    meta,
    originalMessage,
    startedAt,
  });

  if (standaloneModelFeatureFastPath) {
    return standaloneModelFeatureFastPath;
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

    const composed = composeAciAnswer({
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

    return attachDecisionRuntimeEnvelope(composed, {
      bridge,
      context: {
        ...isolatedContext,
        candidateSnapshot: managedCandidateSnapshot,
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

  const scoreExplicitModelPlanOverride = buildExplicitScoreModelPlanOverride({
    message,
    plan: basePlan,
    candidateSnapshot: managedCandidateSnapshot,
  });

  const rawPlan = applyDirectPriceLookupOverride({
    plan: applyExplicitScoreModelPlanOverride({
      plan: applyScoreValueLookupOverride({
        plan: applyCrossModelScoreDiagnosticPlanOverride({
          plan: basePlan,
          override: crossModelScoreDiagnosticOverride,
        }),
        override: scoreValueLookupOverride,
      }),
      override: scoreExplicitModelPlanOverride,
      message,
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
            renderAciLanguageText("decision_score_guardrail_reason", {}, {
              seed: "live_bridge_score_guardrail",
            }),
        }
      : null;

  const composed = composeAciAnswer({
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

  return attachDecisionRuntimeEnvelope(composed, {
    bridge,
    context: {
      ...executionContext,
      candidateSnapshot: managedCandidateSnapshot,
    },
  });
};

export {
  isAciCoreLiveBridgeEnabled,
  shouldUseAciCoreLiveBridge,
};
