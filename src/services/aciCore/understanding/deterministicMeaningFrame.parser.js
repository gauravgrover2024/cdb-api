'use strict';

/**
 * Deterministic ACI meaning-frame parser.
 *
 * Purpose:
 * candidate snapshot + raw message → meaning frame
 *
 * This parser is the fast baseline path.
 *
 * Rules:
 * - No Gemini call.
 * - No automotive factual data.
 * - No hardcoded model/variant/price/feature/color availability.
 * - Only uses candidate snapshot, schema constants, and generic structural logic.
 */

import {
  createEmptyMeaningFrame,
  createEmptyVehicleAnchor,
  ACI_MESSAGE_TYPES,
  ACI_DOMAINS,
  ACI_TASKS,
  ACI_CONTEXT_ACTIONS,
  ACI_RESULT_GRANULARITY,
  assertMeaningFrameShape,
} from './aciMeaningFrame.schema.js';

import {
  PARSER_TYPES,
  createParserResult,
  assertParserResultShape,
} from './aciParserResult.schema.js';

const PARSER_VERSION = '0.1.0';

const unique = (items = []) =>
  [...new Set((Array.isArray(items) ? items : []).filter(Boolean))];

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [];
  return [value];
};

const cleanText = (value = '') =>
  String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const getCandidateKey = (candidate = {}) =>
  candidate.canonicalKey || candidate.displayName || candidate.rawText || null;

const getCandidateDisplayName = (candidate = {}) =>
  candidate.displayName || candidate.rawText || candidate.canonicalKey || null;

const candidateKeys = (items = []) =>
  unique(asArray(items).map(getCandidateKey));

const candidateDisplayNames = (items = []) =>
  unique(asArray(items).map(getCandidateDisplayName));

const validTask = (task) =>
  Object.values(ACI_TASKS).includes(task) ? task : null;

const createConfidence = ({
  taskConfidence = 0.5,
  entityConfidence = 0.5,
  toolReadiness = 0.5,
} = {}) => {
  const clamp = (value) => Math.max(0, Math.min(1, Number(value) || 0));

  return {
    overall: clamp((taskConfidence + entityConfidence + toolReadiness) / 3),
    entityResolution: clamp(entityConfidence),
    taskUnderstanding: clamp(taskConfidence),
    toolReadiness: clamp(toolReadiness),
  };
};

const getBudget = (budgets = []) => {
  const output = {
    min: null,
    max: null,
    basis: null,
    currency: 'INR',
  };

  for (const candidate of budgets || []) {
    const amount = Number(candidate?.metadata?.amount);
    if (!Number.isFinite(amount)) continue;

    const relation = candidate?.metadata?.relation;
    if (relation === 'max') output.max = amount;
    if (relation === 'min') output.min = amount;

    if (candidate?.metadata?.currency) {
      output.currency = candidate.metadata.currency;
    }
  }

  return output;
};

const choosePrimaryTask = ({ candidateTasks = [], modelCount = 0, featureCount = 0, hasBudget = false } = {}) => {
  const taskSet = new Set(candidateTasks.map(validTask).filter(Boolean));

  if (taskSet.has(ACI_TASKS.VEHICLE_COMPARISON) && modelCount >= 2) {
    return ACI_TASKS.VEHICLE_COMPARISON;
  }

  if (taskSet.has(ACI_TASKS.VEHICLE_DISCOVERY) && (hasBudget || featureCount > 0 || modelCount === 0)) {
    return ACI_TASKS.VEHICLE_DISCOVERY;
  }

  const priority = [
    ACI_TASKS.ON_ROAD_ESTIMATE,
    ACI_TASKS.PRICE_BREAKDOWN,
    ACI_TASKS.EMI_CALCULATION,
    ACI_TASKS.COLOR_LOOKUP,
    ACI_TASKS.PRICE_LOOKUP,
    ACI_TASKS.OFFER_LOOKUP,
    ACI_TASKS.FEATURE_FILTER,
    ACI_TASKS.FEATURE_DISCOVERY,
    ACI_TASKS.FEATURE_ANSWER,
    ACI_TASKS.RECOMMENDATION,
    ACI_TASKS.SIMILAR_VEHICLES,
    ACI_TASKS.SAFEST_VEHICLES,
    ACI_TASKS.VALUE_VARIANT,
    ACI_TASKS.VARIANT_DELTA,
    ACI_TASKS.QUOTATION,
    ACI_TASKS.LEAD_CAPTURE,
  ];

  return priority.find((task) => taskSet.has(task)) ||
    candidateTasks.map(validTask).find(Boolean) ||
    ACI_TASKS.CLARIFICATION;
};

const inferRequestedFacts = ({ primaryTask, secondaryTasks = [], featureKeys = [], hasBudget = false } = {}) => {
  const tasks = new Set([primaryTask, ...secondaryTasks].filter(Boolean));

  return {
    price:
      hasBudget ||
      tasks.has(ACI_TASKS.PRICE_LOOKUP) ||
      tasks.has(ACI_TASKS.ON_ROAD_ESTIMATE) ||
      tasks.has(ACI_TASKS.PRICE_BREAKDOWN),
    onRoad: tasks.has(ACI_TASKS.ON_ROAD_ESTIMATE),
    emi: tasks.has(ACI_TASKS.EMI_CALCULATION),
    colors: tasks.has(ACI_TASKS.COLOR_LOOKUP),
    features:
      featureKeys.length > 0 ||
      tasks.has(ACI_TASKS.FEATURE_ANSWER) ||
      tasks.has(ACI_TASKS.FEATURE_DISCOVERY) ||
      tasks.has(ACI_TASKS.FEATURE_FILTER),
    safety: tasks.has(ACI_TASKS.SAFETY_RATING) || tasks.has(ACI_TASKS.SAFEST_VEHICLES),
    offers: tasks.has(ACI_TASKS.OFFER_LOOKUP),
    comparison:
      tasks.has(ACI_TASKS.VEHICLE_COMPARISON) ||
      tasks.has(ACI_TASKS.VARIANT_COMPARISON) ||
      tasks.has(ACI_TASKS.MODEL_YEAR_COMPARISON),
    recommendation:
      tasks.has(ACI_TASKS.RECOMMENDATION) ||
      tasks.has(ACI_TASKS.SIMILAR_VEHICLES) ||
      tasks.has(ACI_TASKS.RIVALS_ALTERNATIVES) ||
      tasks.has(ACI_TASKS.SAFEST_VEHICLES) ||
      tasks.has(ACI_TASKS.VALUE_VARIANT),
    quotation: tasks.has(ACI_TASKS.QUOTATION),
    lead: tasks.has(ACI_TASKS.LEAD_CAPTURE),
    insurance: tasks.has(ACI_TASKS.INSURANCE_QUOTE),
    finance: tasks.has(ACI_TASKS.FINANCE_ELIGIBILITY),
    exchange: tasks.has(ACI_TASKS.EXCHANGE_VALUATION),
    challan: tasks.has(ACI_TASKS.CHALLAN_LOOKUP),
    rc: tasks.has(ACI_TASKS.RC_LOOKUP),
    service: tasks.has(ACI_TASKS.SERVICE_COST),
    tco: tasks.has(ACI_TASKS.TCO_ESTIMATE),
    content: tasks.has(ACI_TASKS.CONTENT_EXPLAINER),
  };
};

const modelMatchesVariant = (modelName = '', variantName = '') => {
  const modelClean = cleanText(modelName);
  const variantClean = cleanText(variantName);
  return Boolean(modelClean && variantClean && variantClean.includes(modelClean));
};

const findVariantForModel = ({ modelName = '', variants = [] } = {}) =>
  variants.find((variantName) => modelMatchesVariant(modelName, variantName)) || null;

const buildComparisonTargets = ({ modelNames = [], variantNames = [], fuelTypes = [] } = {}) =>
  modelNames.map((modelName) => {
    const variantName = findVariantForModel({ modelName, variants: variantNames });

    return {
      ...createEmptyVehicleAnchor(),
      model: modelName,
      fullModel: modelName,
      variant: variantName,
      fullVariant: variantName,
      fuel: fuelTypes.length === 1 ? fuelTypes[0] : null,
      confidence: variantName ? 0.9 : 0.8,
      source: 'candidate_snapshot',
    };
  });

const buildPrimaryVehicle = ({ modelNames = [], variantNames = [], fuelTypes = [] } = {}) => {
  const modelName = modelNames[0] || null;
  const variantName = modelName
    ? findVariantForModel({ modelName, variants: variantNames })
    : variantNames[0] || null;

  return {
    ...createEmptyVehicleAnchor(),
    model: modelName,
    fullModel: modelName,
    variant: variantName,
    fullVariant: variantName,
    fuel: fuelTypes.length === 1 ? fuelTypes[0] : null,
    confidence: modelName || variantName ? 0.85 : null,
    source: modelName || variantName ? 'candidate_snapshot' : null,
  };
};

async function parseDeterministicMeaningFrame({
  rawMessage = '',
  normalizedMessage = '',
  activeContext = null,
  candidateSnapshot = null,
} = {}) {
  const startedAt = Date.now();

  const makes = candidateDisplayNames(candidateSnapshot?.vehicles?.makes || []);
  const models = candidateDisplayNames(candidateSnapshot?.vehicles?.models || []);
  const variants = candidateDisplayNames(candidateSnapshot?.vehicles?.variants || []);
  const colors = candidateDisplayNames(candidateSnapshot?.vehicles?.colors || []);

  const features = candidateKeys(candidateSnapshot?.taxonomy?.features || []);
  const bodyTypes = candidateKeys(candidateSnapshot?.taxonomy?.bodyTypes || []);
  const fuelTypes = candidateKeys(candidateSnapshot?.taxonomy?.fuelTypes || []);
  const transmissions = candidateKeys(candidateSnapshot?.taxonomy?.transmissions || []);

  const budgets = candidateSnapshot?.commerce?.budgets || [];
  const budget = getBudget(budgets);

  const candidateTasks = candidateKeys(candidateSnapshot?.language?.tasks || [])
    .filter((task) => Object.values(ACI_TASKS).includes(task));

  const primaryTask = choosePrimaryTask({
    candidateTasks,
    modelCount: models.length,
    featureCount: features.length,
    hasBudget: Boolean(budget.min || budget.max),
  });

  const secondaryTasks = unique(candidateTasks.filter((task) => task !== primaryTask));

  const isBroadDiscovery = (
    primaryTask === ACI_TASKS.VEHICLE_DISCOVERY ||
    (
      models.length === 0 &&
      (
        makes.length > 0 ||
        bodyTypes.length > 0 ||
        fuelTypes.length > 0 ||
        features.length > 0 ||
        Boolean(budget.min || budget.max)
      )
    )
  );

  const isComparison = (
    primaryTask === ACI_TASKS.VEHICLE_COMPARISON ||
    primaryTask === ACI_TASKS.VARIANT_COMPARISON ||
    models.length >= 2
  );

  const frame = createEmptyMeaningFrame({
    messageType: ACI_MESSAGE_TYPES.AUTOMOTIVE_QUERY,
    domains: [ACI_DOMAINS.NEW_CAR],
    primaryTask,
    secondaryTasks,
    rawMessage,
    normalizedMessage,

    anchors: {
      ...createEmptyMeaningFrame().anchors,
      primaryVehicle: !isComparison
        ? buildPrimaryVehicle({ modelNames: models, variantNames: variants, fuelTypes })
        : createEmptyVehicleAnchor(),
      comparisonTargets: isComparison
        ? buildComparisonTargets({ modelNames: models, variantNames: variants, fuelTypes })
        : [],
      customer: null,
      location: null,
      channel: null,
    },

    filters: {
      ...createEmptyMeaningFrame().filters,
      makes,
      models,
      variants,
      bodyTypes,
      fuelTypes,
      transmissions,
      budget,
      features,
      colors,
      safety: [],
      usage: [],
      ownership: [],
    },

    requestedFacts: inferRequestedFacts({
      primaryTask,
      secondaryTasks,
      featureKeys: features,
      hasBudget: Boolean(budget.min || budget.max),
    }),

    constraints: {
      ...createEmptyMeaningFrame().constraints,
      mustHaveFeatures: features,
      mustHaveFuelTypes: fuelTypes,
      mustHaveTransmissions: transmissions,
      maxBudget: budget.max,
      minBudget: budget.min,
    },

    discovery: {
      ...createEmptyMeaningFrame().discovery,
      isBroadDiscovery,
      resultGranularity: isComparison
        ? ACI_RESULT_GRANULARITY.VEHICLE_TARGETS
        : isBroadDiscovery
          ? ACI_RESULT_GRANULARITY.MODEL_AND_VARIANT
          : variants.length
            ? ACI_RESULT_GRANULARITY.VARIANT
            : ACI_RESULT_GRANULARITY.MODEL_AND_VARIANT,
    },

    context: {
      ...createEmptyMeaningFrame().context,
      action: models.length || makes.length || variants.length
        ? ACI_CONTEXT_ACTIONS.SWITCH_TO_EXPLICIT_ENTITY
        : ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT,
      usesPreviousVehicle: Boolean(activeContext && !models.length && !variants.length),
      explicitVehicleMentioned: Boolean(models.length || variants.length || makes.length),
      explicitVariantMentioned: Boolean(variants.length),
      explicitCityMentioned: false,
      ambiguity: [],
    },

    routing: {
      ...createEmptyMeaningFrame().routing,
      requiredCapabilities: primaryTask ? [primaryTask] : [],
      requiredProviders: [],
      preferredCanvasType: null,
      toolPlanHint: unique([primaryTask, ...secondaryTasks].filter(Boolean)),
    },

    clarification: {
      ...createEmptyMeaningFrame().clarification,
      needed: primaryTask === ACI_TASKS.CLARIFICATION,
      reason: primaryTask === ACI_TASKS.CLARIFICATION ? 'insufficient_grounded_candidates' : null,
      question: primaryTask === ACI_TASKS.CLARIFICATION
        ? 'What would you like to check about the car?'
        : null,
      options: [],
    },

    confidence: createConfidence({
      taskConfidence: candidateTasks.length ? 0.85 : 0.45,
      entityConfidence: models.length || makes.length || variants.length || features.length || budget.min || budget.max ? 0.85 : 0.45,
      toolReadiness: primaryTask === ACI_TASKS.CLARIFICATION ? 0.35 : 0.8,
    }),

    trace: {
      parser: 'deterministicMeaningFrameParser',
      parserVersion: PARSER_VERSION,
      createdAt: new Date().toISOString(),
      candidateCounts: candidateSnapshot?.trace?.counts || null,
      activeContextPresent: Boolean(activeContext),
    },
  });

  assertMeaningFrameShape(frame);

  const result = createParserResult({
    parserType: PARSER_TYPES.DETERMINISTIC_BASELINE,
    parserVersion: PARSER_VERSION,
    meaningFrame: frame,
    rawParserOutput: null,
    warnings: [],
    errors: [],
    trace: {
      latencyMs: Date.now() - startedAt,
      model: null,
      promptVersion: null,
      activeContextPresent: Boolean(activeContext),
      deterministic: true,
    },
  });

  assertParserResultShape(result);
  return result;
}

export {
  PARSER_VERSION,
  parseDeterministicMeaningFrame,
};

export default parseDeterministicMeaningFrame;
