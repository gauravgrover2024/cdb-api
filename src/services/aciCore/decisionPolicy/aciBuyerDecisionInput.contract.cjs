const {
  MANDATORY_FINAL_RECOMMENDATION_INPUTS,
} = require('./aciDecisionPolicy.constants.cjs');
const {
  inferBuyerSignalsFromMessage,
} = require('../context/aciBuyerContextSignals.service.cjs');

const CONTRACT_VERSION = 'aci_buyer_decision_input_contract_v1';

const asObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) ? value : {};

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const textOf = (value) => String(value ?? '').trim();

const valuePresent = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean).length > 0;
  if (value && typeof value === 'object') return Object.keys(value).length > 0;
  return Boolean(textOf(value));
};

const normalizeList = (value) => {
  if (Array.isArray(value)) return value.map(textOf).filter(Boolean);
  if (valuePresent(value)) return [textOf(value)].filter(Boolean);
  return [];
};

const firstPresent = (...candidates) => {
  for (const candidate of candidates) {
    if (valuePresent(candidate.value)) {
      return {
        value: candidate.value,
        source: candidate.source,
      };
    }
  }
  return { value: '', source: '' };
};

const unique = (items = []) => {
  const seen = new Set();
  const out = [];
  for (const item of items.map(textOf).filter(Boolean)) {
    const key = item.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      out.push(item);
    }
  }
  return out;
};

const firstValue = (...values) => {
  for (const value of values) {
    if (valuePresent(value)) return value;
  }
  return '';
};

const normalizeFactList = (...values) => {
  for (const value of values) {
    const items = normalizeList(value);
    if (items.length) return unique(items);
  }
  return [];
};

const normalizeAlternativeList = (...values) => {
  for (const value of values) {
    const source = Array.isArray(value) ? value : valuePresent(value) ? [value] : [];
    const items = source
      .map((item) => {
        if (item && typeof item === 'object') {
          return textOf(item.model || item.fullModel || item.label || item.name || item.title);
        }
        return textOf(item);
      })
      .filter(Boolean);
    if (items.length) return unique(items);
  }
  return [];
};

const normalizeBuyerInputValue = (value) => {
  if (Array.isArray(value)) {
    const alternatives = normalizeAlternativeList(value);
    return alternatives.length ? alternatives : normalizeList(value);
  }
  if (value && typeof value === 'object') {
    const object = asObject(value);
    const label = textOf(firstValue(
      object.label,
      object.name,
      object.title,
      object.fullModel,
      [object.make || object.brand, object.model, object.variant || object.variantName].filter(Boolean).join(' ')
    ));
    if (label) return label;
    if (valuePresent(object.max) || valuePresent(object.min)) {
      const currency = textOf(object.currency || 'INR');
      return [
        valuePresent(object.min) ? `min ${currency} ${object.min}` : '',
        valuePresent(object.max) ? `max ${currency} ${object.max}` : '',
      ].filter(Boolean).join(', ');
    }
    return '';
  }
  return value;
};

const hasInput = (inputStatus = {}, key = '') => Boolean(inputStatus[key]?.present);

const getInputValue = (inputStatus = {}, key = '') =>
  hasInput(inputStatus, key) ? normalizeBuyerInputValue(inputStatus[key].value) : '';

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === 'object') return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== '';
    })
  );

const getNestedDecisionContext = ({ context = {}, response = {} } = {}) => {
  const ctx = asObject(context);
  const data = asObject(response.data);
  const meta = asObject(response.meta);

  const contextState = asObject(
    ctx.contextState ||
      ctx.aciContextState ||
      response.contextState ||
      response.aciContextState ||
      data.contextState ||
      data.aciContextState
  );

  const buyerContext = asObject(
    ctx.buyerContext ||
      ctx.buyerIntent ||
      contextState.buyerContext ||
      contextState.buyerIntent ||
      data.buyerContext ||
      data.buyerIntent ||
      meta.buyerContext ||
      meta.buyerIntent
  );

  const selectedVehicle = asObject(
    ctx.selectedVehicle ||
      contextState.selectedVehicle ||
      contextState.anchors?.primaryVehicle ||
      data.selectedVehicle ||
      data.vehicle ||
      response.selectedVehicle
  );

  const selectedVehicleContext = asObject(
    ctx.selectedVehicleContext ||
      ctx.selectedVehicleFacts ||
      ctx.vehicleFacts ||
      contextState.selectedVehicleContext ||
      contextState.selectedVehicleFacts ||
      contextState.buyerGuidanceContext?.selectedVehicleFacts ||
      data.selectedVehicleContext ||
      data.selectedVehicleFacts ||
      data.vehicleFacts ||
      response.selectedVehicleContext ||
      response.selectedVehicleFacts
  );

  const filters = asObject(ctx.filters || contextState.filters || data.filters || response.filters);
  const entities = asObject(ctx.entities || data.entities || response.entities);
  const priorities = asObject(buyerContext.priorities || ctx.priorities || data.priorities || response.priorities);
  const candidateSnapshot = asObject(
    ctx.candidateSnapshot ||
      contextState.candidateSnapshot ||
      data.candidateSnapshot ||
      response.candidateSnapshot ||
      meta.candidateSnapshot
  );

  const shortlistedModels = asArray(
    buyerContext.shortlistedModels ||
      buyerContext.shortlist ||
      buyerContext.models ||
      ctx.shortlistedModels ||
      ctx.shortlist ||
      data.shortlistedModels ||
      data.models ||
      response.shortlistedModels ||
      response.models
  );

  const comparisonTargets = asArray(
    contextState.activeComparison?.vehicles ||
      contextState.anchors?.comparisonTargets ||
      contextState.comparison?.targets ||
      ctx.comparisonTargets ||
      ctx.comparisonTargetsEvidence ||
      ctx.decisionEvidencePack?.subject?.comparisonTargets ||
      data.comparisonTargets ||
      data.decisionEvidencePack?.subject?.comparisonTargets ||
      response.comparisonTargets
  );

  const upgradeBase = asObject(
    ctx.upgradeBase ||
      ctx.upgradeLadder?.base ||
      ctx.decisionEvidencePack?.subject?.upgradeBase ||
      data.upgradeBase ||
      data.upgradeLadder?.base ||
      data.decisionEvidencePack?.subject?.upgradeBase ||
      response.upgradeBase
  );

  const upgradeTarget = asObject(
    ctx.upgradeTarget ||
      ctx.upgradeLadder?.target ||
      ctx.decisionEvidencePack?.subject?.upgradeTarget ||
      data.upgradeTarget ||
      data.upgradeLadder?.target ||
      data.decisionEvidencePack?.subject?.upgradeTarget ||
      response.upgradeTarget
  );

  const decisionEvidencePackInput = asObject(
    ctx.decisionEvidencePack ||
      ctx.buyerGuidanceEvidence ||
      ctx.decisionEvidence ||
      contextState.buyerGuidanceContext?.decisionEvidencePack ||
      data.decisionEvidencePack ||
      data.buyerGuidanceEvidence ||
      data.decisionEvidence ||
      response.decisionEvidencePack ||
      response.buyerGuidanceEvidence ||
      response.decisionEvidence
  );

  return {
    ctx,
    data,
    meta,
    contextState,
    buyerContext,
    selectedVehicle,
    selectedVehicleContext,
    filters,
    entities,
    candidateSnapshot,
    priorities,
    shortlistedModels,
    comparisonTargets,
    upgradeBase,
    upgradeTarget,
    decisionEvidencePackInput,
  };
};

const candidateMetadata = (candidate = {}) => asObject(candidate.metadata);
const candidateRaw = (candidate = {}) =>
  asObject(candidate.raw || candidateMetadata(candidate).raw || candidateMetadata(candidate).vehicle || candidate.vehicle);

const normalizeCandidateVehicle = (candidate = {}, kind = '') => {
  const raw = candidateRaw(candidate);
  const meta = candidateMetadata(candidate);
  const make = textOf(firstValue(candidate.make, candidate.brand, meta.make, meta.brand, raw.make, raw.brand));
  const model = textOf(firstValue(candidate.model, meta.model, raw.model, raw.rawModel, kind === 'model' ? candidate.displayName : ''));
  const variant = textOf(firstValue(candidate.variant, candidate.variantName, meta.variant, meta.variantName, raw.variant, raw.variantName, kind === 'variant' ? candidate.displayName : ''));
  const fullModel = textOf(firstValue(candidate.fullModel, meta.fullModel, raw.fullModel, [make, model].filter(Boolean).join(' ')));
  const label = textOf(firstValue(candidate.label, candidate.name, candidate.displayName, raw.label, [make, model, variant].filter(Boolean).join(' ')));

  if (kind === 'make') {
    return compactObject({
      make: make || textOf(firstValue(candidate.displayName, candidate.rawText, candidate.canonicalKey)),
      label,
    });
  }

  return compactObject({
    make,
    model,
    variant,
    fullModel,
    label,
  });
};

const getCandidateVehicles = (candidateSnapshot = {}, kind = 'model') => {
  const vehicles = asObject(candidateSnapshot.vehicles);
  const items =
    kind === 'variant'
      ? vehicles.variants
      : kind === 'make'
        ? vehicles.makes
        : vehicles.models;
  return asArray(items)
    .map((candidate) => normalizeCandidateVehicle(candidate, kind))
    .filter((item) => Object.keys(item).length);
};

const labelFromMessageChunk = (chunk = '') =>
  textOf(chunk)
    .replace(/^[,.;:\-\s]+|[,.;:\-\s?]+$/g, '')
    .replace(/\b(?:please|pls)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim();

const inferUpgradeSubjectFromMessage = ({ message = '', selectedVehicleFacts = {} } = {}) => {
  const match = textOf(message).match(/\bfrom\s+(.+?)\s+to\s+(.+?)(?:[?.!,]|$)/i);
  if (!match) return {};

  const baseLabel = labelFromMessageChunk(match[1]);
  let targetLabel = labelFromMessageChunk(match[2]);
  if (!baseLabel || !targetLabel) return {};

  const baseTokens = baseLabel.split(/\s+/).filter(Boolean);
  const targetTokens = targetLabel.split(/\s+/).filter(Boolean);
  const knownModel = textOf(selectedVehicleFacts.model);
  if (targetTokens.length === 1 && baseTokens.length > 1 && !knownModel) {
    targetLabel = `${baseTokens.slice(0, -1).join(' ')} ${targetLabel}`.trim();
  } else if (targetTokens.length === 1 && knownModel && !new RegExp(`\\b${knownModel.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i').test(targetLabel)) {
    targetLabel = `${knownModel} ${targetLabel}`.trim();
  }

  return {
    upgradeBase: compactObject({ label: baseLabel }),
    upgradeTarget: compactObject({ label: targetLabel }),
  };
};


function isExplicitMakeOnlyFinalChoiceMessage(message = '', { subject = {}, selectedVehicleFacts = {} } = {}) {
  const raw = textOf(message).toLowerCase();
  if (!/\bshould\s+i\s+(buy|choose|pick|go\s+for|purchase)\b|\bworth\s+buying\b/.test(raw)) return false;
  if (/\b\w+\s+(?:vs|versus|or)\s+\w+\b/.test(raw)) return false;
  if (/\bstretch\b|\bupgrade\b|\bfrom\b.+\bto\b/.test(raw)) return false;

  const resolvedMake = textOf(firstValue(subject.make, selectedVehicleFacts.make, selectedVehicleFacts.brand));
  const resolvedModel = textOf(firstValue(subject.model, selectedVehicleFacts.model, selectedVehicleFacts.fullModel));
  const resolvedVariant = textOf(firstValue(subject.variant, selectedVehicleFacts.variant));

  return Boolean(resolvedMake && !resolvedModel && !resolvedVariant);
}

function buildSelectedVehicleFacts({
  selectedVehicle = {},
  selectedVehicleContext = {},
  data = {},
  entities = {},
  candidateSnapshot = {},
} = {}) {
  const vehicle = asObject(selectedVehicle);
  const facts = asObject(selectedVehicleContext);
  const entityObject = asObject(entities);
  const makeCandidate = getCandidateVehicles(candidateSnapshot, 'make')[0] || {};
  const variantCandidate = getCandidateVehicles(candidateSnapshot, 'variant')[0] || {};
  const modelCandidate = getCandidateVehicles(candidateSnapshot, 'model')[0] || variantCandidate || {};
  const entityModel = firstValue(entityObject.model, entityObject.primaryModel, asArray(entityObject.models)[0]);
  const entityVariant = firstValue(entityObject.variant, entityObject.primaryVariant, asArray(entityObject.variants)[0]);
  const entityMake = firstValue(entityObject.make, entityObject.brand, asArray(entityObject.makes)[0]);

  return compactObject({
    brand: textOf(firstValue(facts.brand, facts.make, vehicle.brand, vehicle.make, entityObject.brand, entityMake, modelCandidate.make, makeCandidate.make)),
    make: textOf(firstValue(facts.make, facts.brand, vehicle.make, vehicle.brand, entityMake, modelCandidate.make, makeCandidate.make)),
    model: textOf(firstValue(facts.model, vehicle.model, entityModel, modelCandidate.model)),
    variant: textOf(firstValue(facts.variant, facts.variantName, vehicle.variant, vehicle.variantName, entityVariant, variantCandidate.variant)),
    fullModel: textOf(firstValue(facts.fullModel, vehicle.fullModel, modelCandidate.fullModel, [facts.brand || facts.make || vehicle.brand || vehicle.make || entityMake || modelCandidate.make, facts.model || vehicle.model || entityModel || modelCandidate.model].filter(Boolean).join(' '))),
    bodyType: textOf(firstValue(facts.bodyType, facts.bodyStyle, vehicle.bodyType, vehicle.bodyStyle, data.bodyType)),
    seatingCapacity: firstValue(facts.seatingCapacity, facts.seats, vehicle.seatingCapacity, vehicle.seats),
    fuelTypes: normalizeFactList(facts.fuelTypes, facts.availableFuels, vehicle.fuelTypes, vehicle.availableFuels, vehicle.fuelType, data.fuelTypes),
    transmissions: normalizeFactList(facts.transmissions, facts.availableTransmissions, vehicle.transmissions, vehicle.availableTransmissions, vehicle.transmission, data.transmissions),
    priceBand: textOf(firstValue(facts.priceBand, facts.priceRange, facts.price, vehicle.priceBand, data.priceBand)),
    safetyFeatures: normalizeFactList(facts.safetyFeatures, facts.knownSafetyFeatures, vehicle.safetyFeatures, data.safetyFeatures),
    featureHighlights: normalizeFactList(facts.featureHighlights, facts.knownFeatureHighlights, vehicle.featureHighlights, data.featureHighlights),
    ownershipSignals: normalizeFactList(facts.ownershipSignals, facts.serviceResaleOwnershipSignals, facts.ownershipHighlights, vehicle.ownershipSignals, data.ownershipSignals),
    similarAlternatives: normalizeAlternativeList(facts.similarAlternatives, facts.alternatives, vehicle.similarAlternatives, data.similarAlternatives),
    source: textOf(firstValue(facts.source, vehicle.source, data.source)),
  });
}

function buildExplicitBuyerContext(inputStatus = {}) {
  return compactObject({
    city: getInputValue(inputStatus, 'city'),
    budgetOrPriceCeiling: getInputValue(inputStatus, 'budgetOrPriceCeiling'),
    bodyPreferenceOrPrimaryUseCase: getInputValue(inputStatus, 'bodyPreferenceOrPrimaryUseCase'),
    familySizeOrOccupancyUse: getInputValue(inputStatus, 'familySizeOrOccupancyUse'),
    fuelPreferenceOrMonthlyRunning: getInputValue(inputStatus, 'fuelPreferenceOrMonthlyRunning'),
    transmissionPreference: getInputValue(inputStatus, 'transmissionPreference'),
    safetyPriority: getInputValue(inputStatus, 'safetyPriority'),
    featurePriority: getInputValue(inputStatus, 'featurePriority'),
    shortlistedModelsOrDiscoveryScope: getInputValue(inputStatus, 'shortlistedModelsOrDiscoveryScope'),
  });
}

function buildSoftAssumptions({ selectedVehicleFacts = {}, inputStatus = {} } = {}) {
  const assumptions = [];
  const seating = Number(selectedVehicleFacts.seatingCapacity || 0);

  if (!hasInput(inputStatus, 'familySizeOrOccupancyUse') && Number.isFinite(seating) && seating > 0) {
    const lower = seating >= 5 ? Math.max(2, seating - 1) : seating;
    assumptions.push(`assuming normal ${lower}-${seating} person use`);
  }

  if (!hasInput(inputStatus, 'fuelPreferenceOrMonthlyRunning')) {
    assumptions.push('assuming monthly running is not very high');
  }

  return unique(assumptions);
}

function selectGuidanceMode({ selectedVehicleFacts = {}, inputStatus = {}, scope = '' } = {}) {
  const hasSelectedVehicle = Boolean(selectedVehicleFacts.model || selectedVehicleFacts.fullModel);
  const hasDecisionScope = scope && scope !== 'unknown_scope';
  if (!hasSelectedVehicle && !hasDecisionScope) return '';

  const hasCityOrBasicContext =
    hasInput(inputStatus, 'city') ||
    hasInput(inputStatus, 'bodyPreferenceOrPrimaryUseCase') ||
    hasInput(inputStatus, 'familySizeOrOccupancyUse');
  const hasUseCase = hasInput(inputStatus, 'bodyPreferenceOrPrimaryUseCase');
  const hasRunning = hasInput(inputStatus, 'fuelPreferenceOrMonthlyRunning');
  const hasPriority = hasInput(inputStatus, 'safetyPriority') || hasInput(inputStatus, 'featurePriority');

  if (hasUseCase && (hasRunning || hasPriority)) return 'sharpened_recommendation';
  if (hasCityOrBasicContext) return 'practical_first_view';
  return 'conditional_guidance';
}

const normalizeSubjectVehicle = (value = {}) => {
  const vehicle = asObject(value);
  if (!Object.keys(vehicle).length) return {};
  return compactObject({
    make: textOf(firstValue(vehicle.make, vehicle.brand)),
    model: textOf(vehicle.model),
    variant: textOf(firstValue(vehicle.variant, vehicle.variantName, vehicle.selectedVariant)),
    fullModel: textOf(vehicle.fullModel),
    label: textOf(firstValue(vehicle.label, vehicle.name, vehicle.fullModel, [vehicle.make || vehicle.brand, vehicle.model, vehicle.variant || vehicle.variantName].filter(Boolean).join(' '))),
  });
};

const normalizeSubjectTargets = (items = []) =>
  asArray(items)
    .map((item) => {
      if (item && typeof item === 'object') {
        return compactObject({
          make: textOf(firstValue(item.make, item.brand)),
          model: textOf(item.model),
          variant: textOf(firstValue(item.variant, item.variantName, item.selectedVariant)),
          fullModel: textOf(item.fullModel),
          label: textOf(firstValue(item.label, item.name, item.fullModel, [item.make || item.brand, item.model, item.variant || item.variantName].filter(Boolean).join(' '))),
        });
      }
      return compactObject({ label: textOf(item) });
    })
    .filter((item) => Object.keys(item).length);

function buildDecisionSubject({
  selectedVehicleFacts = {},
  selectedVehicle = {},
  decisionEvidencePackInput = {},
  comparisonTargets = [],
  upgradeBase = {},
  upgradeTarget = {},
  entities = {},
  candidateSnapshot = {},
  message = '',
} = {}) {
  const inputSubject = asObject(decisionEvidencePackInput.subject);
  const candidateMakes = getCandidateVehicles(candidateSnapshot, 'make');
  const candidateModels = getCandidateVehicles(candidateSnapshot, 'model');
  const candidateVariants = getCandidateVehicles(candidateSnapshot, 'variant');
  const entityObject = asObject(entities);
  const inferredUpgrade = inferUpgradeSubjectFromMessage({ message, selectedVehicleFacts });
  const baseSubject = compactObject({
    make: textOf(firstValue(inputSubject.make, selectedVehicleFacts.make, selectedVehicleFacts.brand, selectedVehicle.make, selectedVehicle.brand, entityObject.make, entityObject.brand, candidateModels[0]?.make, candidateMakes[0]?.make)),
    model: textOf(firstValue(inputSubject.model, selectedVehicleFacts.model, selectedVehicle.model, entityObject.model, entityObject.primaryModel, asArray(entityObject.models)[0], candidateModels[0]?.model, candidateVariants[0]?.model)),
    variant: textOf(firstValue(inputSubject.variant, selectedVehicleFacts.variant, selectedVehicle.variant, entityObject.variant, entityObject.primaryVariant, asArray(entityObject.variants)[0], candidateVariants[0]?.variant)),
    discoveryLabel: textOf(inputSubject.discoveryLabel),
    comparisonTargets: normalizeSubjectTargets(firstValue(inputSubject.comparisonTargets, comparisonTargets, candidateModels.length >= 2 ? candidateModels : [])),
    upgradeBase: normalizeSubjectVehicle(firstValue(inputSubject.upgradeBase, upgradeBase, inferredUpgrade.upgradeBase)),
    upgradeTarget: normalizeSubjectVehicle(firstValue(inputSubject.upgradeTarget, upgradeTarget, inferredUpgrade.upgradeTarget)),
  });

  return baseSubject;
}

function detectBuyerGuidanceScope({
  message = '',
  subject = {},
  selectedVehicleFacts = {},
  inputStatus = {},
  decisionEvidencePackInput = {},
  response = {},
} = {
}) {
  if (isExplicitMakeOnlyFinalChoiceMessage(message, { subject, selectedVehicleFacts })) return 'make_scope';
  const explicitScope = textOf(decisionEvidencePackInput.scope || decisionEvidencePackInput.decisionScope);

  const raw = textOf(message).toLowerCase();
  const comparisonTargets = asArray(subject.comparisonTargets);
  const hasUpgradeSubject = Object.keys(asObject(subject.upgradeBase)).length || Object.keys(asObject(subject.upgradeTarget)).length;
  const hasUpgradeIntent = /\b(stretch|upgrade|worth\s+(?:the\s+)?extra|extra\s+(?:money|cost|price))\b/.test(raw);
  const hasComparisonIntent = /\b(vs|v\/s|versus)\b/.test(raw) || /\bshould\s+i\s+buy\b.+\bor\b.+\?/i.test(message);

  if (explicitScope) {
    if (explicitScope === 'upgrade_scope' && !(hasUpgradeSubject || hasUpgradeIntent)) {
      // Ignore stale upstream upgrade labels unless the current question is actually about an upgrade.
    } else if (explicitScope === 'comparison_scope' && !(comparisonTargets.length >= 2 || hasComparisonIntent)) {
      // Ignore stale upstream comparison labels unless the current question is actually comparative.
    } else {
      return explicitScope;
    }
  }

  if (hasUpgradeSubject || hasUpgradeIntent) {
    return 'upgrade_scope';
  }
  if (comparisonTargets.length >= 2 || hasComparisonIntent) {
    return 'comparison_scope';
  }
  if (subject.variant || selectedVehicleFacts.variant) return 'variant_scope';
  if (subject.model || selectedVehicleFacts.model) return 'model_scope';
  if ((subject.make || selectedVehicleFacts.make || selectedVehicleFacts.brand) && !subject.model && !selectedVehicleFacts.model) {
    return 'make_scope';
  }
  if (
    hasInput(inputStatus, 'budgetOrPriceCeiling') ||
    hasInput(inputStatus, 'bodyPreferenceOrPrimaryUseCase') ||
    response.module === 'recommendation' ||
    /\b(best|recommend|cars?|options?)\b.+\b(under|within|budget|family|city|highway|suv|sedan|hatchback)\b/i.test(message)
  ) {
    return 'discovery_scope';
  }
  return 'unknown_scope';
}

const SCORE_SIGNAL_KEYS = Object.freeze({
  safety: ['safety', 'safetyScore'],
  features: ['features', 'featureScore'],
  value: ['value', 'valueScore'],
  runningCost: ['runningCost', 'mileageRunningCost', 'mileageRunningCostScore', 'runningCostScore', 'mileageScore'],
  familyPracticality: ['familyPracticality', 'practicality', 'practicalityScore', 'familyScore'],
  comfort: ['comfort', 'premiumComfort', 'premiumComfortScore', 'comfortScore'],
  regretRisk: ['regretRisk', 'regretRiskScore'],
});

const normalizeSignalValue = (value) => {
  if (!valuePresent(value)) return null;
  if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') return value;
  const object = asObject(value);
  if (!Object.keys(object).length) return null;
  return compactObject({
    score: firstValue(object.score, object.riskScore, object.value),
    band: textOf(firstValue(object.band, object.tier, object.level, object.status)),
    confidence: textOf(object.confidence),
    label: textOf(object.label),
    reasons: normalizeList(object.reasons),
    caveats: normalizeList(object.caveats),
    source: textOf(object.source),
  });
};

const readFirstPath = (source = {}, paths = []) => {
  for (const path of paths) {
    const parts = String(path).split('.');
    let current = source;
    for (const part of parts) {
      if (!current || typeof current !== 'object' || !Object.prototype.hasOwnProperty.call(current, part)) {
        current = undefined;
        break;
      }
      current = current[part];
    }
    if (valuePresent(current)) return current;
  }
  return '';
};

function buildScoreSignals(evidenceInput = {}) {
  const input = asObject(evidenceInput);
  const direct = asObject(input.scoreSignals);
  const scoreProfile = asObject(input.variantScoreProfile || input.scoreProfile || input.scoreInsight);
  const modules = asObject(scoreProfile.modules);
  const signals = {};

  for (const [key, aliases] of Object.entries(SCORE_SIGNAL_KEYS)) {
    const candidates = [
      readFirstPath(direct, aliases),
      ...aliases.map((alias) => modules[alias]).filter(valuePresent),
      ...aliases.map((alias) => scoreProfile[alias]).filter(valuePresent),
      ...aliases.map((alias) => input[alias]).filter(valuePresent),
    ];
    const normalized = normalizeSignalValue(firstValue(...candidates));
    if (normalized !== null) signals[key] = normalized;
  }

  return signals;
}

const normalizeEvidenceList = (...values) => {
  for (const value of values) {
    const list = normalizeList(value);
    if (list.length) return unique(list);
  }
  return [];
};

function buildDecisionEvidencePack({
  scope = '',
  subject = {},
  decisionEvidencePackInput = {},
  selectedVehicleFacts = {},
} = {}) {
const input = asObject(decisionEvidencePackInput);
  const pack = compactObject({
    scope,
    subject,
    scoreSignals: buildScoreSignals(input),
    strengths: normalizeEvidenceList(input.strengths, input.scoreInsight?.strengths, input.variantScoreProfile?.strengths),
    watchouts: normalizeEvidenceList(input.watchouts, input.scoreInsight?.watchouts, input.variantScoreProfile?.watchouts),
    fitSignals: normalizeEvidenceList(input.fitSignals, input.buyerFitSignals, input.fit),
    alternativeSignals: normalizeEvidenceList(input.alternativeSignals, input.similarModelGraph?.signals, input.similarAlternatives, selectedVehicleFacts.similarAlternatives),
    upgradeSignals: normalizeEvidenceList(input.upgradeSignals, input.upgradeLadder?.signals, input.upgradeLadder?.reasons),
    missingEvidence: normalizeEvidenceList(input.missingEvidence, input.dataQuality?.missingEvidence),
    evidenceSources: normalizeEvidenceList(input.evidenceSources, input.sources, input.variantScoreProfile?.buildVersion, input.scoreInsight?.buildVersion),
  });

  return {
    scope,
    subject,
    scoreSignals: asObject(pack.scoreSignals),
    strengths: asArray(pack.strengths),
    watchouts: asArray(pack.watchouts),
    fitSignals: asArray(pack.fitSignals),
    alternativeSignals: asArray(pack.alternativeSignals),
    upgradeSignals: asArray(pack.upgradeSignals),
    missingEvidence: asArray(pack.missingEvidence),
    evidenceSources: asArray(pack.evidenceSources),
  };
}

function buildBuyerGuidanceContext({
  inputStatus = {},
  selectedVehicle = {},
  selectedVehicleContext = {},
  buyerContext = {},
  contextState = {},
  data = {},
  entities = {},
  candidateSnapshot = {},
  message = '',
  comparisonTargets = [],
  upgradeBase = {},
  upgradeTarget = {},
  decisionEvidencePackInput = {},
  response = {},
} = {}) {
  const selectedVehicleFacts = buildSelectedVehicleFacts({
    selectedVehicle,
    selectedVehicleContext,
    data,
    entities,
    candidateSnapshot,
  });
  const subject = buildDecisionSubject({
    selectedVehicleFacts,
    selectedVehicle,
    decisionEvidencePackInput,
    comparisonTargets,
    upgradeBase,
    upgradeTarget,
    entities,
    candidateSnapshot,
    message,
  });
  const scope = detectBuyerGuidanceScope({
    message,
    subject,
    selectedVehicleFacts,
    inputStatus,
    decisionEvidencePackInput,
    response,
  });
  const decisionEvidencePack = buildDecisionEvidencePack({
    scope,
    subject,
    decisionEvidencePackInput,
    selectedVehicleFacts,
  });
  const guidanceMode = selectGuidanceMode({ selectedVehicleFacts, inputStatus, scope });
  const existingGuidance = asObject(contextState.buyerGuidanceContext);
  const inferredContext = compactObject({
    ...asObject(existingGuidance.inferredContext),
    ...asObject(buyerContext.inferredBuyerContext),
    ...inferBuyerSignalsFromMessage(message),
  });
  const softAssumptions = buildSoftAssumptions({ selectedVehicleFacts, inputStatus });
  const needsUseCase = !hasInput(inputStatus, 'bodyPreferenceOrPrimaryUseCase');

  return compactObject({
    version: 'aci_buyer_guidance_context_v1',
    guidanceMode,
    allowedGuidanceModes: [
      'practical_first_view',
      'conditional_guidance',
      'sharpened_recommendation',
    ],
    finalPurchaseVerdictEnabled: false,
    scope,
    selectedVehicleFacts,
    decisionEvidencePack,
    explicitBuyerContext: buildExplicitBuyerContext(inputStatus),
    inferredContext,
    softAssumptions,
    softQuestion: needsUseCase ? 'Is your use mostly city, highway, or mixed?' : '',
    knownCapabilities: compactObject({
      hasCng: asArray(selectedVehicleFacts.fuelTypes).some((fuel) => /\bcng\b/i.test(fuel)),
      hasPetrol: asArray(selectedVehicleFacts.fuelTypes).some((fuel) => /\bpetrol\b/i.test(fuel)),
      hasAutomatic: asArray(selectedVehicleFacts.transmissions).some((transmission) => /\bautomatic|auto|amt|cvt|dct|ivt|at\b/i.test(transmission)),
      hasManual: asArray(selectedVehicleFacts.transmissions).some((transmission) => /\bmanual|mt\b/i.test(transmission)),
      hasNamedAlternatives: asArray(selectedVehicleFacts.similarAlternatives).length > 0,
    }),
  });
}

function buildInputStatus(key, candidate, { required = true } = {}) {
  const present = valuePresent(candidate.value);
  return {
    key,
    required,
    present,
    value: present ? candidate.value : '',
    source: present ? candidate.source : '',
  };
}

function buildBuyerDecisionInputContract({ context = {}, response = {}, message = '' } = {}) {
  const {
    ctx,
    data,
    contextState,
    buyerContext,
    selectedVehicle,
    selectedVehicleContext,
    filters,
    entities,
    candidateSnapshot,
    priorities,
    shortlistedModels,
    comparisonTargets,
    upgradeBase,
    upgradeTarget,
    decisionEvidencePackInput,
  } = getNestedDecisionContext({ context, response });

  const candidates = {
    city: firstPresent(
      { value: buyerContext.city, source: 'buyerContext.city' },
      { value: buyerContext.citySlug, source: 'buyerContext.citySlug' },
      { value: ctx.city, source: 'context.city' },
      { value: ctx.citySlug, source: 'context.citySlug' },
      { value: filters.city, source: 'filters.city' },
      { value: filters.citySlug, source: 'filters.citySlug' },
      { value: selectedVehicle.city, source: 'selectedVehicle.city' },
      { value: selectedVehicle.citySlug, source: 'selectedVehicle.citySlug' },
      { value: data.city, source: 'data.city' },
      { value: data.citySlug, source: 'data.citySlug' }
    ),

    budgetOrPriceCeiling: firstPresent(
      { value: buyerContext.budgetOrPriceCeiling, source: 'buyerContext.budgetOrPriceCeiling' },
      { value: buyerContext.budget, source: 'buyerContext.budget' },
      { value: buyerContext.budgetRange, source: 'buyerContext.budgetRange' },
      { value: buyerContext.maxBudget, source: 'buyerContext.maxBudget' },
      { value: buyerContext.budgetMax, source: 'buyerContext.budgetMax' },
      { value: buyerContext.priceCeiling, source: 'buyerContext.priceCeiling' },
      { value: ctx.budget, source: 'context.budget' },
      { value: ctx.maxBudget, source: 'context.maxBudget' },
      { value: filters.budgetMax, source: 'filters.budgetMax' },
      { value: filters.maxBudget, source: 'filters.maxBudget' },
      { value: filters.maxPrice, source: 'filters.maxPrice' },
      { value: filters.priceCeiling, source: 'filters.priceCeiling' },
      { value: data.budget, source: 'data.budget' },
      { value: data.maxBudget, source: 'data.maxBudget' }
    ),

    bodyPreferenceOrPrimaryUseCase: firstPresent(
      { value: buyerContext.bodyPreferenceOrPrimaryUseCase, source: 'buyerContext.bodyPreferenceOrPrimaryUseCase' },
      { value: buyerContext.bodyType, source: 'buyerContext.bodyType' },
      { value: buyerContext.bodyPreference, source: 'buyerContext.bodyPreference' },
      { value: buyerContext.primaryUseCase, source: 'buyerContext.primaryUseCase' },
      { value: buyerContext.useCase, source: 'buyerContext.useCase' },
      { value: ctx.bodyType, source: 'context.bodyType' },
      { value: ctx.primaryUseCase, source: 'context.primaryUseCase' },
      { value: filters.bodyType, source: 'filters.bodyType' },
      { value: data.bodyType, source: 'data.bodyType' },
      { value: data.primaryUseCase, source: 'data.primaryUseCase' }
    ),

    familySizeOrOccupancyUse: firstPresent(
      { value: buyerContext.familySizeOrOccupancyUse, source: 'buyerContext.familySizeOrOccupancyUse' },
      { value: buyerContext.familySize, source: 'buyerContext.familySize' },
      { value: buyerContext.occupancy, source: 'buyerContext.occupancy' },
      { value: buyerContext.occupancyUse, source: 'buyerContext.occupancyUse' },
      { value: buyerContext.seatingNeed, source: 'buyerContext.seatingNeed' },
      { value: ctx.familySize, source: 'context.familySize' },
      { value: ctx.occupancy, source: 'context.occupancy' },
      { value: data.familySize, source: 'data.familySize' },
      { value: data.occupancy, source: 'data.occupancy' }
    ),

    fuelPreferenceOrMonthlyRunning: firstPresent(
      { value: buyerContext.fuelPreferenceOrMonthlyRunning, source: 'buyerContext.fuelPreferenceOrMonthlyRunning' },
      { value: buyerContext.fuelPreference, source: 'buyerContext.fuelPreference' },
      { value: buyerContext.fuel, source: 'buyerContext.fuel' },
      { value: buyerContext.fuelType, source: 'buyerContext.fuelType' },
      { value: buyerContext.monthlyRunning, source: 'buyerContext.monthlyRunning' },
      { value: buyerContext.running, source: 'buyerContext.running' },
      { value: buyerContext.runningPattern, source: 'buyerContext.runningPattern' },
      { value: ctx.fuel, source: 'context.fuel' },
      { value: ctx.fuelType, source: 'context.fuelType' },
      { value: filters.fuel, source: 'filters.fuel' },
      { value: filters.fuelType, source: 'filters.fuelType' },
      { value: data.fuel, source: 'data.fuel' },
      { value: data.fuelType, source: 'data.fuelType' }
    ),

    transmissionPreference: firstPresent(
      { value: buyerContext.transmissionPreference, source: 'buyerContext.transmissionPreference' },
      { value: buyerContext.transmission, source: 'buyerContext.transmission' },
      { value: buyerContext.transmissionType, source: 'buyerContext.transmissionType' },
      { value: ctx.transmission, source: 'context.transmission' },
      { value: filters.transmission, source: 'filters.transmission' },
      { value: data.transmission, source: 'data.transmission' }
    ),

    safetyPriority: firstPresent(
      { value: buyerContext.safetyPriority, source: 'buyerContext.safetyPriority' },
      { value: priorities.safety, source: 'priorities.safety' },
      { value: ctx.safetyPriority, source: 'context.safetyPriority' },
      { value: data.safetyPriority, source: 'data.safetyPriority' }
    ),

    featurePriority: firstPresent(
      { value: buyerContext.featurePriority, source: 'buyerContext.featurePriority' },
      { value: buyerContext.priorityFeatures, source: 'buyerContext.priorityFeatures' },
      { value: buyerContext.mustHaveFeatures, source: 'buyerContext.mustHaveFeatures' },
      { value: priorities.features, source: 'priorities.features' },
      { value: ctx.featurePriority, source: 'context.featurePriority' },
      { value: data.featurePriority, source: 'data.featurePriority' },
      { value: data.mustHaveFeatures, source: 'data.mustHaveFeatures' }
    ),

    shortlistedModelsOrDiscoveryScope: firstPresent(
      { value: shortlistedModels, source: 'shortlistedModels' },
      { value: comparisonTargets, source: 'comparisonTargets' },
      { value: buyerContext.shortlistedModelsOrDiscoveryScope, source: 'buyerContext.shortlistedModelsOrDiscoveryScope' },
      { value: buyerContext.discoveryScope, source: 'buyerContext.discoveryScope' },
      { value: selectedVehicle.model, source: 'selectedVehicle.model' },
      { value: selectedVehicle.modelKey, source: 'selectedVehicle.modelKey' },
      { value: selectedVehicle.fullModel, source: 'selectedVehicle.fullModel' },
      { value: data.discoveryScope, source: 'data.discoveryScope' },
      { value: filters.bodyType, source: 'filters.bodyType' }
    ),
  };

  const inputStatus = {};
  for (const key of MANDATORY_FINAL_RECOMMENDATION_INPUTS) {
    inputStatus[key] = buildInputStatus(key, candidates[key] || { value: '', source: '' });
  }

  const presentInputs = Object.entries(inputStatus)
    .filter(([, status]) => status.present)
    .map(([key]) => key);

  const missingMandatoryInputs = MANDATORY_FINAL_RECOMMENDATION_INPUTS.filter(
    (key) => !inputStatus[key]?.present
  );

  const completionRatio =
    MANDATORY_FINAL_RECOMMENDATION_INPUTS.length > 0
      ? Number((presentInputs.length / MANDATORY_FINAL_RECOMMENDATION_INPUTS.length).toFixed(2))
      : 1;
  const buyerGuidanceContext = buildBuyerGuidanceContext({
    inputStatus,
    selectedVehicle,
    selectedVehicleContext,
    buyerContext,
    contextState,
    data,
    entities,
    candidateSnapshot,
    message,
    comparisonTargets,
    upgradeBase,
    upgradeTarget,
    decisionEvidencePackInput,
    response,
  });

  return {
    version: CONTRACT_VERSION,
    inputStatus,
    presentInputs,
    missingMandatoryInputs,
    completionRatio,
    readyForFinalRecommendationPolicyEval: missingMandatoryInputs.length === 0,
    readinessStatus:
      missingMandatoryInputs.length === 0
        ? 'buyer_context_complete'
        : 'buyer_context_incomplete',
    normalizedBuyerInputs: Object.fromEntries(
      Object.entries(inputStatus).map(([key, status]) => [key, status.value])
    ),
    buyerGuidanceContext,
    shortlist: normalizeList(shortlistedModels.length ? shortlistedModels : comparisonTargets),
  };
}

module.exports = {
  CONTRACT_VERSION,
  buildBuyerGuidanceContext,
  buildBuyerDecisionInputContract,
};
