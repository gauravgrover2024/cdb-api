'use strict';

/**
 * Deterministic repair for Gemini meaning frames.
 *
 * Allowed:
 * - fill missing structural defaults
 * - normalize arrays/booleans
 * - copy raw/normalized message
 * - use DB-backed candidate keys for canonical filters
 *
 * Not allowed:
 * - invent car facts
 * - invent prices/features/availability
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


const tokensFrom = (value = '') =>
  cleanText(value)
    .split(' ')
    .map((token) => token.trim())
    .filter(Boolean);

const getMentionedCandidateDisplayNames = ({
  message = '',
  candidates = [],
  modelNames = [],
  makeNames = [],
  max = 6,
} = {}) => {
  const messageClean = cleanText(message);
  const messageTokens = new Set(tokensFrom(message));

  const excludedTokens = new Set([
    ...modelNames.flatMap(tokensFrom),
    ...makeNames.flatMap(tokensFrom),
  ]);

  const matches = [];

  for (const candidate of candidates || []) {
    const displayName = candidate.displayName || candidate.rawText || '';
    const rawTokens = tokensFrom(displayName);

    const residualTokens = rawTokens
      .filter((token) => token.length >= 1)
      .filter((token) => !excludedTokens.has(token));

    if (!residualTokens.length) continue;

    const significantTokens = residualTokens.filter((token) => token.length >= 2);
    const residualPhrase = residualTokens.join(' ');
    const significantPhrase = significantTokens.join(' ');

    const hasFullDisplayPhrase = messageClean.includes(cleanText(displayName));
    const hasResidualPhrase = Boolean(residualPhrase) && messageClean.includes(residualPhrase);
    const hasSignificantPhrase = Boolean(significantPhrase) && messageClean.includes(significantPhrase);
    const allSignificantPresent =
      significantTokens.length >= 2 &&
      significantTokens.every((token) => messageTokens.has(token));

    const singleResidualExactMatch =
      residualTokens.length === 1 &&
      significantTokens.length === 1 &&
      messageTokens.has(significantTokens[0]);

    if (
      !hasFullDisplayPhrase &&
      !hasResidualPhrase &&
      !hasSignificantPhrase &&
      !allSignificantPresent &&
      !singleResidualExactMatch
    ) {
      continue;
    }

    matches.push({
      displayName,
      tokens: significantTokens.length ? significantTokens : residualTokens,
      tokenCount: residualTokens.length,
      matchedBy: hasFullDisplayPhrase
        ? 'full_display_phrase'
        : hasResidualPhrase
          ? 'residual_phrase'
          : hasSignificantPhrase
            ? 'significant_phrase'
            : allSignificantPresent
              ? 'all_significant_tokens'
              : 'single_residual_token',
    });
  }

  const pruned = matches.filter((candidate, index) => {
    return !matches.some((other, otherIndex) => {
      if (index === otherIndex) return false;
      if (other.tokens.length <= candidate.tokens.length) return false;
      return candidate.tokens.every((token) => other.tokens.includes(token));
    });
  });

  return unique(
    pruned
      .sort((a, b) =>
        (b.tokenCount - a.tokenCount) ||
        (b.tokens.length - a.tokens.length) ||
        a.displayName.localeCompare(b.displayName),
      )
      .map((item) => item.displayName),
  ).slice(0, max);
};

const findGroundedCandidateDisplayName = ({ value = '', candidates = [] } = {}) => {
  const valueClean = cleanText(value);
  if (!valueClean) return null;

  const exact = candidates.find((candidate) => cleanText(candidate) === valueClean);
  if (exact) return exact;

  const valueTokens = tokensFrom(valueClean);
  if (!valueTokens.length) return null;

  const scored = [];

  for (const candidate of candidates || []) {
    const candidateClean = cleanText(candidate);
    const candidateTokens = tokensFrom(candidateClean);

    if (!candidateClean || !candidateTokens.length) continue;

    const allValueTokensPresent = valueTokens.every((token) => candidateTokens.includes(token));
    const candidateEndsWithValue = candidateClean.endsWith(` ${valueClean}`);
    const candidateIncludesValuePhrase = candidateClean.includes(valueClean);
    const valueIncludesCandidatePhrase = valueClean.includes(candidateClean);

    if (
      !allValueTokensPresent &&
      !candidateEndsWithValue &&
      !candidateIncludesValuePhrase &&
      !valueIncludesCandidatePhrase
    ) {
      continue;
    }

    scored.push({
      candidate,
      score:
        (candidateEndsWithValue ? 4 : 0) +
        (candidateIncludesValuePhrase ? 3 : 0) +
        (valueIncludesCandidatePhrase ? 3 : 0) +
        (allValueTokensPresent ? 2 : 0) -
        Math.max(0, candidateTokens.length - valueTokens.length) * 0.1,
    });
  }

  scored.sort((a, b) => b.score - a.score || a.candidate.localeCompare(b.candidate));
  return scored[0]?.candidate || null;
};

const groundDisplayNameList = ({ values = [], candidates = [] } = {}) =>
  unique(
    asArray(values).map((value) =>
      findGroundedCandidateDisplayName({ value, candidates }) || value,
    ),
  );

const cleanConfidence = (value, fallback = null) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.max(0, Math.min(1, num));
};

const validValue = (value, allowed, fallback) =>
  Object.values(allowed).includes(value) ? value : fallback;

const mergeObject = (base = {}, input = {}) => ({
  ...base,
  ...(input && typeof input === 'object' ? input : {}),
});

const normalizeVehicleAnchor = (anchor = {}) => ({
  ...createEmptyVehicleAnchor(),
  ...(anchor && typeof anchor === 'object' ? anchor : {}),
  confidence: cleanConfidence(anchor?.confidence, null),
});

const candidateKeys = (items = []) =>
  unique(items.map((item) => item.canonicalKey).filter(Boolean));

const candidateDisplayNames = (items = []) =>
  unique(items.map((item) => item.displayName).filter(Boolean));

function repairGeminiMeaningFrame({
  frame = {},
  rawMessage = '',
  normalizedMessage = '',
  candidateSnapshot = null,
  parserName = 'geminiMeaningFrameParser',
  parserVersion = '0.1.0',
} = {}) {
  const base = createEmptyMeaningFrame();
  const input = frame && typeof frame === 'object' ? frame : {};

  const repaired = {
    ...base,
    ...input,
    schemaVersion: 'aci.meaningFrame.v1',
    rawMessage,
    normalizedMessage,
    messageType: validValue(
      input.messageType,
      ACI_MESSAGE_TYPES,
      ACI_MESSAGE_TYPES.AUTOMOTIVE_QUERY,
    ),
    domains: unique(asArray(input.domains))
      .filter((domain) => Object.values(ACI_DOMAINS).includes(domain)),
    primaryTask: validValue(
      input.primaryTask,
      ACI_TASKS,
      ACI_TASKS.CLARIFICATION,
    ),
    secondaryTasks: unique(asArray(input.secondaryTasks))
      .filter((task) => Object.values(ACI_TASKS).includes(task)),
  };

  if (!repaired.domains.length) {
    repaired.domains = [ACI_DOMAINS.NEW_CAR];
  }

  repaired.anchors = {
    ...base.anchors,
    ...mergeObject({}, input.anchors),
    primaryVehicle: normalizeVehicleAnchor(input.anchors?.primaryVehicle),
    comparisonTargets: asArray(input.anchors?.comparisonTargets).map(normalizeVehicleAnchor),
  };

  repaired.filters = {
    ...base.filters,
    ...mergeObject({}, input.filters),
    makes: unique(asArray(input.filters?.makes)),
    models: unique(asArray(input.filters?.models)),
    variants: unique(asArray(input.filters?.variants)),
    bodyTypes: unique(asArray(input.filters?.bodyTypes)),
    fuelTypes: unique(asArray(input.filters?.fuelTypes)),
    transmissions: unique(asArray(input.filters?.transmissions)),
    features: unique(asArray(input.filters?.features)),
    colors: unique(asArray(input.filters?.colors)),
    safety: unique(asArray(input.filters?.safety)),
    usage: unique(asArray(input.filters?.usage)),
    ownership: unique(asArray(input.filters?.ownership)),
    budget: {
      ...base.filters.budget,
      ...(input.filters?.budget || {}),
      currency: input.filters?.budget?.currency || 'INR',
    },
  };

  repaired.requestedFacts = {
    ...base.requestedFacts,
    ...(input.requestedFacts || {}),
  };

  for (const key of Object.keys(repaired.requestedFacts)) {
    repaired.requestedFacts[key] = Boolean(repaired.requestedFacts[key]);
  }

  repaired.constraints = {
    ...base.constraints,
    ...(input.constraints || {}),
    mustHaveFeatures: unique(asArray(input.constraints?.mustHaveFeatures)),
    niceToHaveFeatures: unique(asArray(input.constraints?.niceToHaveFeatures)),
    excludeFeatures: unique(asArray(input.constraints?.excludeFeatures)),
    mustHaveFuelTypes: unique(asArray(input.constraints?.mustHaveFuelTypes)),
    mustHaveTransmissions: unique(asArray(input.constraints?.mustHaveTransmissions)),
  };

  repaired.discovery = {
    ...base.discovery,
    ...(input.discovery || {}),
    isBroadDiscovery: Boolean(input.discovery?.isBroadDiscovery),
    resultGranularity: validValue(
      input.discovery?.resultGranularity,
      ACI_RESULT_GRANULARITY,
      ACI_RESULT_GRANULARITY.MODEL_AND_VARIANT,
    ),
  };

  repaired.context = {
    ...base.context,
    ...(input.context || {}),
    action: validValue(
      input.context?.action,
      ACI_CONTEXT_ACTIONS,
      ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT,
    ),
    usesPreviousVehicle: Boolean(input.context?.usesPreviousVehicle),
    explicitVehicleMentioned: Boolean(input.context?.explicitVehicleMentioned),
    explicitVariantMentioned: Boolean(input.context?.explicitVariantMentioned),
    explicitCityMentioned: Boolean(input.context?.explicitCityMentioned),
    ambiguity: unique(asArray(input.context?.ambiguity)),
  };

  repaired.routing = {
    ...base.routing,
    ...(input.routing || {}),
    requiredCapabilities: unique(asArray(input.routing?.requiredCapabilities)),
    requiredProviders: unique(asArray(input.routing?.requiredProviders)),
    toolPlanHint: unique(asArray(input.routing?.toolPlanHint)),
  };

  repaired.clarification = {
    ...base.clarification,
    ...(input.clarification || {}),
    needed: Boolean(input.clarification?.needed),
    options: unique(asArray(input.clarification?.options)),
  };

  repaired.confidence = {
    ...base.confidence,
    ...(input.confidence || {}),
    overall: cleanConfidence(input.confidence?.overall, 0.5),
    entityResolution: cleanConfidence(input.confidence?.entityResolution, 0.5),
    taskUnderstanding: cleanConfidence(input.confidence?.taskUnderstanding, 0.5),
    toolReadiness: cleanConfidence(input.confidence?.toolReadiness, 0.5),
  };

  repaired.safety = {
    ...base.safety,
    ...(input.safety || {}),
    shouldRefuse: Boolean(input.safety?.shouldRefuse),
    requiresConsent: Boolean(input.safety?.requiresConsent),
  };

  repaired.trace = {
    ...base.trace,
    ...(input.trace || {}),
    parser: parserName,
    parserVersion,
    createdAt: new Date().toISOString(),
  };

  const candidateFeatureKeys = candidateKeys(candidateSnapshot?.taxonomy?.features || []);
  const candidateFuelKeys = candidateKeys(candidateSnapshot?.taxonomy?.fuelTypes || []);
  const candidateTransmissionKeys = candidateKeys(candidateSnapshot?.taxonomy?.transmissions || []);
  const candidateModelNames = candidateDisplayNames(candidateSnapshot?.vehicles?.models || []);
  const candidateMakeNames = candidateDisplayNames(candidateSnapshot?.vehicles?.makes || []);
  const candidateVariantItems = candidateSnapshot?.vehicles?.variants || [];
  const candidateVariantNames = candidateDisplayNames(candidateVariantItems);
  const mentionedCandidateVariantNames = getMentionedCandidateDisplayNames({
    message: normalizedMessage || rawMessage,
    candidates: candidateVariantItems,
    modelNames: candidateModelNames,
    makeNames: candidateMakeNames,
  });
  const candidateColorNames = candidateDisplayNames(candidateSnapshot?.vehicles?.colors || []);
  const candidateTaskKeys = candidateKeys(candidateSnapshot?.language?.tasks || [])
    .filter((task) => Object.values(ACI_TASKS).includes(task));

  if (candidateMakeNames.length) {
    repaired.filters.makes = groundDisplayNameList({
      values: repaired.filters.makes,
      candidates: candidateMakeNames,
    });
  }

  if (candidateModelNames.length) {
    repaired.filters.models = groundDisplayNameList({
      values: repaired.filters.models,
      candidates: candidateModelNames,
    });

    const groundedPrimaryModel = findGroundedCandidateDisplayName({
      value: repaired.anchors.primaryVehicle.fullModel || repaired.anchors.primaryVehicle.model,
      candidates: candidateModelNames,
    });

    if (groundedPrimaryModel) {
      repaired.anchors.primaryVehicle.model = groundedPrimaryModel;
      repaired.anchors.primaryVehicle.fullModel = groundedPrimaryModel;
      repaired.anchors.primaryVehicle.source = repaired.anchors.primaryVehicle.source || 'candidate_snapshot';
      repaired.anchors.primaryVehicle.confidence = repaired.anchors.primaryVehicle.confidence ?? 0.75;
    }

    repaired.anchors.comparisonTargets = asArray(repaired.anchors.comparisonTargets).map((target) => {
      const groundedModel = findGroundedCandidateDisplayName({
        value: target.fullModel || target.model,
        candidates: candidateModelNames,
      });

      if (!groundedModel) return target;

      return {
        ...target,
        model: groundedModel,
        fullModel: groundedModel,
        source: target.source || 'candidate_snapshot',
        confidence: target.confidence ?? 0.75,
      };
    });
  }

  // Keep canonical filters grounded to candidate snapshot where available.
  // Candidate snapshot is DB-backed / language-operator-backed, so using it here is allowed.
  if (candidateFeatureKeys.length) {
    const groundedInputFeatures = repaired.filters.features
      .filter((item) => candidateFeatureKeys.includes(item));

    repaired.filters.features = unique([
      ...groundedInputFeatures,
      ...candidateFeatureKeys,
    ]);

    repaired.requestedFacts.features = true;
  }

  if (candidateFuelKeys.length && !repaired.filters.fuelTypes.length) {
    repaired.filters.fuelTypes = candidateFuelKeys;
  }

  if (candidateTransmissionKeys.length && !repaired.filters.transmissions.length) {
    repaired.filters.transmissions = candidateTransmissionKeys;
  }

  if (candidateMakeNames.length && !repaired.filters.makes.length) {
    repaired.filters.makes = candidateMakeNames;
  }

  if (candidateModelNames.length && !repaired.filters.models.length) {
    repaired.filters.models = candidateModelNames;
  }

  if (mentionedCandidateVariantNames.length && !repaired.filters.variants.length) {
    repaired.filters.variants = mentionedCandidateVariantNames;
  } else if (candidateVariantNames.length <= 3 && !repaired.filters.variants.length) {
    repaired.filters.variants = candidateVariantNames;
  }

  if (candidateColorNames.length && !repaired.filters.colors.length) {
    repaired.filters.colors = candidateColorNames;
  }

  if (candidateTaskKeys.length) {
    const currentTaskIsWeak = (
      !repaired.primaryTask ||
      repaired.primaryTask === ACI_TASKS.CLARIFICATION ||
      repaired.primaryTask === ACI_TASKS.UNSUPPORTED
    );

    if (currentTaskIsWeak) {
      const priority = [
        ACI_TASKS.VEHICLE_COMPARISON,
        ACI_TASKS.VEHICLE_DISCOVERY,
        ACI_TASKS.ON_ROAD_ESTIMATE,
        ACI_TASKS.PRICE_LOOKUP,
        ACI_TASKS.EMI_CALCULATION,
        ACI_TASKS.COLOR_LOOKUP,
        ACI_TASKS.FEATURE_FILTER,
        ACI_TASKS.FEATURE_ANSWER,
      ];

      repaired.primaryTask =
        priority.find((task) => candidateTaskKeys.includes(task)) ||
        candidateTaskKeys[0] ||
        repaired.primaryTask;
    }

    repaired.secondaryTasks = unique([
      ...repaired.secondaryTasks,
      ...candidateTaskKeys.filter((task) => task !== repaired.primaryTask),
    ]);
  }

  if (repaired.primaryTask === ACI_TASKS.VEHICLE_COMPARISON) {
    repaired.requestedFacts.comparison = true;
  }

  if (repaired.primaryTask === ACI_TASKS.VEHICLE_DISCOVERY) {
    repaired.discovery.isBroadDiscovery = true;
  }

  if (repaired.primaryTask === ACI_TASKS.ON_ROAD_ESTIMATE) {
    repaired.requestedFacts.onRoad = true;
    repaired.requestedFacts.price = true;
  }

  if (repaired.primaryTask === ACI_TASKS.PRICE_LOOKUP) {
    repaired.requestedFacts.price = true;
  }

  if (repaired.primaryTask === ACI_TASKS.EMI_CALCULATION) {
    repaired.requestedFacts.emi = true;
  }

  if (repaired.primaryTask === ACI_TASKS.COLOR_LOOKUP) {
    repaired.requestedFacts.colors = true;
  }

  if (repaired.filters.models.length >= 2 && !repaired.anchors.comparisonTargets.length) {
    repaired.anchors.comparisonTargets = repaired.filters.models.map((model) => {
      const matchingVariant = mentionedCandidateVariantNames.find((variantName) =>
        cleanText(variantName).includes(cleanText(model)),
      );

      return {
        ...createEmptyVehicleAnchor(),
        model,
        fullModel: model,
        variant: matchingVariant || null,
        fullVariant: matchingVariant || null,
        confidence: 0.75,
        source: 'candidate_snapshot',
      };
    });
  }

  if (repaired.filters.models.length === 1 && !repaired.anchors.primaryVehicle.model) {
    repaired.anchors.primaryVehicle = {
      ...repaired.anchors.primaryVehicle,
      model: repaired.filters.models[0],
      fullModel: repaired.filters.models[0],
      confidence: 0.75,
      source: 'candidate_snapshot',
    };
  }

  const budgetCandidate = candidateSnapshot?.commerce?.budgets?.[0];
  if (budgetCandidate?.metadata?.amount && !repaired.filters.budget.max && budgetCandidate.metadata.relation === 'max') {
    repaired.filters.budget.max = budgetCandidate.metadata.amount;
    repaired.filters.budget.currency = budgetCandidate.metadata.currency || 'INR';
  }

  assertMeaningFrameShape(repaired);

  return repaired;
}

export {
  repairGeminiMeaningFrame,
};

export default repairGeminiMeaningFrame;
