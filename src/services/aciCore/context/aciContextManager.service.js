'use strict';

import {
  assertAciContextStateShape,
  createEmptyAciContextState,
  createEmptySelectedVehicleState,
  isAciContextState,
} from './aciContextState.contract.js';
import { resolveVehicleAlias } from './aciVehicleAliasRegistry.service.js';
import { applyBuyerContextToContextState } from './aciBuyerContextExtractor.service.js';

const CONTEXT_MANAGER_VERSION = 'aci-context-manager-v1.0.0';

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const normalizeText = (value = '') =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const keyify = (value = '') => normalizeText(value);

const normalizeFuelKey = (value = '') => {
  const key = keyify(value);
  if (!key) return '';
  if (/\bcng\b/.test(key)) return 'cng';
  if (/\bdiesel\b/.test(key)) return 'diesel';
  if (/\bpetrol\b/.test(key)) return 'petrol';
  if (/\belectric\b|\bev\b/.test(key)) return 'electric';
  if (/\bhybrid\b/.test(key)) return 'hybrid';
  return key;
};

const normalizeTransmissionKey = (value = '') => {
  const key = keyify(value);
  if (!key) return '';
  if (/\bmanual\b|\bmt\b/.test(key)) return 'manual';
  if (/\bautomatic\b|\bauto\b|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bimt\b/.test(key)) return 'automatic';
  return key;
};

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [];
  return [value];
};

const unique = (items = []) => [...new Set(asArray(items).map(cleanText).filter(Boolean))];

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && cleanText(value) !== '') || '';

const clampConfidence = (value, fallback = 0) => {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(0, Math.min(1, number));
};

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === 'object') return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== '';
    }),
  );

const isPlainObject = (value) =>
  Boolean(value && typeof value === 'object' && !Array.isArray(value));

const SELECTED_VEHICLE_CONTEXT_KEYS = [
  'make',
  'model',
  'fullModel',
  'makeKey',
  'modelKey',
  'shortModelKey',
  'variant',
  'variantKey',
  'fuelType',
  'fuelKey',
  'transmission',
  'transmissionKey',
  'city',
  'citySlug',
  'confidence',
  'source',
];

const REQUESTED_CONTEXT_KEYS = [
  'facts',
  'features',
  'topics',
  'specAttributes',
  'topic',
  'budget',
  'city',
  'citySlug',
];

const compactVehicleContext = (vehicle = {}) => {
  if (!isPlainObject(vehicle)) return createEmptySelectedVehicleState();
  const compact = {};
  for (const key of SELECTED_VEHICLE_CONTEXT_KEYS) {
    const value = vehicle[key];
    if (value === undefined || value === null || value === '') continue;
    compact[key] = value;
  }

  return createEmptySelectedVehicleState({
    ...compact,
    make: cleanText(firstMeaningful(compact.make, vehicle.brand)),
    model: cleanText(compact.model),
    fullModel: cleanText(firstMeaningful(compact.fullModel, [compact.make || vehicle.brand, compact.model].filter(Boolean).join(' '))),
    makeKey: keyify(firstMeaningful(compact.makeKey, compact.make, vehicle.brand)),
    modelKey: keyify(firstMeaningful(compact.modelKey, compact.fullModel, compact.model)),
    shortModelKey: keyify(firstMeaningful(compact.shortModelKey, compact.model)),
    variant: cleanText(firstMeaningful(compact.variant, vehicle.variantName, vehicle.selectedVariant)),
    variantKey: keyify(firstMeaningful(compact.variantKey, compact.variant, vehicle.variantName, vehicle.selectedVariant)),
    fuelType: cleanText(firstMeaningful(compact.fuelType, vehicle.fuel)),
    fuelKey: normalizeFuelKey(firstMeaningful(compact.fuelType, vehicle.fuel, compact.fuelKey)),
    transmission: cleanText(compact.transmission),
    transmissionKey: normalizeTransmissionKey(firstMeaningful(compact.transmission, compact.transmissionKey)),
    city: cleanText(compact.city),
    citySlug: keyify(firstMeaningful(compact.citySlug, compact.city)).replace(/\s+/g, '-'),
    confidence: clampConfidence(compact.confidence, 0),
    source: cleanText(compact.source),
  });
};

const compactRequestedContext = (requested = {}) => {
  if (!isPlainObject(requested)) return {};
  const compact = {};
  for (const key of REQUESTED_CONTEXT_KEYS) {
    const value = requested[key];
    if (value === undefined || value === null || value === '') continue;
    compact[key] = value;
  }
  return compactObject(compact);
};

const compactActiveComparisonContext = (comparison = {}) => {
  if (!isPlainObject(comparison)) return {};
  const vehicles = asArray(comparison.vehicles).map(compactVehicleContext).filter((vehicle) => vehicle.model);
  return compactObject({
    vehicles,
    fuelKey: keyify(firstMeaningful(comparison.fuelKey, comparison.fuelFilter)),
    transmissionKey: keyify(comparison.transmissionKey),
    city: cleanText(comparison.city),
    citySlug: keyify(firstMeaningful(comparison.citySlug, comparison.city)).replace(/\s+/g, '-'),
    features: unique(asArray(comparison.features)),
    confidence: clampConfidence(comparison.confidence, vehicles.length >= 2 ? 0.8 : 0),
    source: cleanText(comparison.source),
  });
};

function compactAciContextState(contextState = {}) {
  const state = isAciContextState(contextState) ? contextState : getPreviousState(contextState);
  const selectedVehicle = compactVehicleContext(state.selectedVehicle || {});
  const activeComparison = compactActiveComparisonContext(state.activeComparison || {});

  return createEmptyAciContextState({
    selectedVehicle,
    buyerContext: state.buyerContext || state.buyerIntent || {},
    activeComparison,
    requested: compactRequestedContext(state.requested || {}),
    anchors: {
      primaryVehicle: selectedVehicle.model ? selectedVehicle : {},
      comparisonTargets: asArray(
        state.anchors?.comparisonTargets?.length
          ? state.anchors.comparisonTargets
          : activeComparison.vehicles,
      ).map(compactVehicleContext).filter((vehicle) => vehicle.model),
    },
    confidence: {
      entityConfidence: clampConfidence(state.confidence?.entityConfidence, 0),
      modelConfidence: clampConfidence(state.confidence?.modelConfidence, 0),
      variantConfidence: clampConfidence(state.confidence?.variantConfidence, 0),
      contextConfidence: clampConfidence(state.confidence?.contextConfidence, 0),
      resolutionSource: cleanText(state.confidence?.resolutionSource),
    },
    provenance: {
      sources: unique(asArray(state.provenance?.sources)),
      warnings: unique(asArray(state.provenance?.warnings)),
      isolation: cleanText(state.provenance?.isolation),
      updatedBy: cleanText(state.provenance?.updatedBy || CONTEXT_MANAGER_VERSION),
      intent: cleanText(state.provenance?.intent),
    },
  });
}

const stripDisplayPayloadFromContext = (contextState = {}) => compactAciContextState(contextState);

const mergeSelectedVehicle = (...vehicles) => {
  const output = {};
  const POWERTRAIN_KEYS = new Set(['fuelType', 'fuelKey', 'fuel', 'transmission', 'transmissionKey']);

  for (const [index, vehicle] of vehicles.entries()) {
    if (!isPlainObject(vehicle)) continue;
    const incomingVariant = firstMeaningful(vehicle.variant, vehicle.variantName, vehicle.selectedVariant);
    const currentVehicleKey = vehicleIdentityKey(output);
    const incomingVehicleKey = vehicleIdentityKey(vehicle);
    if (currentVehicleKey && incomingVehicleKey && currentVehicleKey !== incomingVehicleKey) {
      for (const key of ['variant', 'variantName', 'selectedVariant', 'variantKey', 'fuelType', 'fuelKey', 'fuel', 'transmission', 'transmissionKey']) {
        delete output[key];
      }
    }

    for (const [key, value] of Object.entries(vehicle)) {
      if (value === undefined || value === null || value === '') continue;
      if (POWERTRAIN_KEYS.has(key) && !incomingVariant && index >= 3) continue;
      output[key] = value;
    }
  }

  return Object.keys(output).length ? output : null;
};

const mergeActiveComparison = (managerComparison = {}, toolComparison = {}) => {
  const managerVehicles = asArray(managerComparison?.vehicles);
  const toolVehicles = asArray(toolComparison?.vehicles);

  if (!managerVehicles.length && !toolVehicles.length) return null;
  if (managerVehicles.length >= 2) {
    return compactActiveComparisonContext({
      ...toolComparison,
      ...managerComparison,
      vehicles: managerVehicles,
      fuelFilter: managerComparison.fuelFilter || managerComparison.fuelKey || '',
      fuelKey: managerComparison.fuelKey || '',
      transmissionKey: managerComparison.transmissionKey || '',
    });
  }

  return compactActiveComparisonContext({
    ...managerComparison,
    ...toolComparison,
    vehicles: toolVehicles,
  });
};

function mergeContextPatches({
  previousPatch = {},
  managerPatch = {},
  userPatch = {},
  toolPatch = {},
} = {}) {
  const selectedVehicle = mergeSelectedVehicle(
    previousPatch.selectedVehicle,
    managerPatch.selectedVehicle,
    userPatch.selectedVehicle,
    toolPatch.selectedVehicle,
  );
  const managerVehicle = managerPatch.selectedVehicle || {};
  const canonicalSelectedVehicle = selectedVehicle
    ? compactVehicleContext({
        ...selectedVehicle,
        make: managerVehicle.make || selectedVehicle.make,
        brand: managerVehicle.brand || managerVehicle.make || selectedVehicle.brand,
        model: managerVehicle.model || selectedVehicle.model,
        fullModel: managerVehicle.fullModel || selectedVehicle.fullModel,
        makeKey: managerVehicle.makeKey || selectedVehicle.makeKey,
        modelKey: managerVehicle.modelKey || selectedVehicle.modelKey,
        shortModelKey: managerVehicle.shortModelKey || selectedVehicle.shortModelKey,
      })
    : null;
  const merged = {
    ...(previousPatch || {}),
    ...(managerPatch || {}),
    ...(userPatch || {}),
    ...(toolPatch || {}),
    ...(canonicalSelectedVehicle ? { selectedVehicle: canonicalSelectedVehicle } : {}),
    anchorMake: managerPatch.anchorMake || toolPatch.anchorMake || userPatch.anchorMake || previousPatch.anchorMake || '',
    anchorBrand: managerPatch.anchorBrand || toolPatch.anchorBrand || userPatch.anchorBrand || previousPatch.anchorBrand || '',
    anchorModel: managerPatch.anchorModel || toolPatch.anchorModel || userPatch.anchorModel || previousPatch.anchorModel || '',
    anchorFullModel: managerPatch.anchorFullModel || toolPatch.anchorFullModel || userPatch.anchorFullModel || previousPatch.anchorFullModel || '',
  };
  const activeComparison = mergeActiveComparison(managerPatch.activeComparison, toolPatch.activeComparison);
  if (activeComparison) merged.activeComparison = activeComparison;

  const managerState = managerPatch.contextState || managerPatch.aciContextState || null;
  if (managerState) {
    const durableState = compactAciContextState({
      ...managerState,
      selectedVehicle: {
        ...(managerState.selectedVehicle || {}),
        ...(canonicalSelectedVehicle?.model ? canonicalSelectedVehicle : {}),
      },
      anchors: {
        ...(managerState.anchors || {}),
        primaryVehicle: canonicalSelectedVehicle?.model
          ? {
              ...(managerState.anchors?.primaryVehicle || {}),
              ...canonicalSelectedVehicle,
            }
          : managerState.anchors?.primaryVehicle || {},
      },
    });
    merged.contextState = durableState;
    merged.aciContextState = durableState;
  } else if (canonicalSelectedVehicle?.model || activeComparison) {
    const durableState = compactAciContextState({
      selectedVehicle: canonicalSelectedVehicle?.model ? canonicalSelectedVehicle : {},
      activeComparison: activeComparison || {},
      provenance: {
        sources: ['merge_context_patches'],
        updatedBy: CONTEXT_MANAGER_VERSION,
      },
    });
    merged.contextState = durableState;
    merged.aciContextState = durableState;
  }

  return compactObject(merged);
}

const titleCaseLoose = (value = '') =>
  cleanText(value)
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');

const getCandidateMetadata = (candidate = {}) => candidate?.metadata || {};
const getCandidateRaw = (candidate = {}) => getCandidateMetadata(candidate).raw || {};

const getCandidateMake = (candidate = {}) =>
  firstMeaningful(
    getCandidateMetadata(candidate).make,
    getCandidateRaw(candidate).make,
    getCandidateRaw(candidate).brand,
    candidate.make,
    candidate.brand,
  );

const getCandidateModel = (candidate = {}) =>
  firstMeaningful(
    getCandidateMetadata(candidate).model,
    getCandidateRaw(candidate).rawModel,
    getCandidateRaw(candidate).model,
    candidate.model,
  );

const getCandidateFullModel = (candidate = {}) =>
  firstMeaningful(
    getCandidateMetadata(candidate).fullModel,
    getCandidateRaw(candidate).fullModel,
    getCandidateRaw(candidate).displayName,
    candidate.fullModel,
    candidate.displayName,
    [getCandidateMake(candidate), getCandidateModel(candidate)].filter(Boolean).join(' '),
  );


const getFeatureKeys = (candidateSnapshot = {}) =>
  unique(
    asArray(candidateSnapshot?.taxonomy?.features).map((feature = {}) =>
      firstMeaningful(feature.canonicalKey, feature.displayName, feature.rawText),
    ),
  );

const getBudget = (candidateSnapshot = {}) => {
  const budget = {};

  for (const candidate of asArray(candidateSnapshot?.commerce?.budgets)) {
    const amount = Number(candidate?.metadata?.amount);
    if (!Number.isFinite(amount) || amount <= 0) continue;

    if (candidate?.metadata?.relation === 'min') budget.min = amount;
    if (candidate?.metadata?.relation === 'max') budget.max = amount;
    budget.currency = candidate?.metadata?.currency || budget.currency || 'INR';
  }

  return budget;
};

const getFirstCandidateKey = (items = []) =>
  firstMeaningful(...asArray(items).map((item = {}) => item.canonicalKey || item.displayName || item.rawText));

const getFirstCandidateText = (items = []) =>
  firstMeaningful(...asArray(items).map((item = {}) => item.displayName || item.rawText || item.canonicalKey));

const uniqueTextValues = (items = []) =>
  unique(asArray(items).map((item) => cleanText(item)).filter(Boolean));

const isVariantCandidate = (candidate = {}) => {
  const raw = getCandidateRaw(candidate);
  return raw.type === 'variant' ||
    candidate.type === 'variant' ||
    Boolean(raw.variant || raw.variantKey || candidate.variant || candidate.variantKey);
};

const getDurablePowertrainValue = ({
  explicitValue = '',
  candidateValues = [],
  variantResolved = false,
} = {}) => {
  const explicit = cleanText(explicitValue);
  if (explicit) return explicit;

  const values = uniqueTextValues(candidateValues);
  if (variantResolved && values[0]) return values[0];
  if (values.length === 1) return values[0];

  return '';
};

const vehicleIdentityKey = (vehicle = {}) =>
  keyify(
    firstMeaningful(
      vehicle.modelKey,
      vehicle.shortModelKey,
      [vehicle.make, vehicle.model].filter(Boolean).join(' '),
      vehicle.fullModel,
      vehicle.model,
    ),
  );

function normalizeVehicleAnchor(anchor = {}) {
  const make = cleanText(firstMeaningful(anchor.make, anchor.brand));
  const model = cleanText(anchor.model);
  const fullModel = cleanText(firstMeaningful(anchor.fullModel, [make, model].filter(Boolean).join(' ')));
  const makeKey = keyify(firstMeaningful(anchor.makeKey, make));
  const modelKey = keyify(firstMeaningful(anchor.modelKey, fullModel, model));
  const shortModelKey = keyify(firstMeaningful(anchor.shortModelKey, model));
  const variant = cleanText(firstMeaningful(anchor.variant, anchor.variantName, anchor.selectedVariant));
  const variantKey = keyify(firstMeaningful(anchor.variantKey, variant));
  const fuelType = cleanText(firstMeaningful(anchor.fuelType, anchor.fuel));
  const transmission = cleanText(anchor.transmission);
  const city = cleanText(anchor.city);
  const citySlug = keyify(firstMeaningful(anchor.citySlug, city)).replace(/\s+/g, '-');

  return compactVehicleContext({
    make,
    model,
    fullModel,
    makeKey,
    modelKey,
    shortModelKey,
    variant,
    variantKey,
    fuelType,
    fuelKey: normalizeFuelKey(firstMeaningful(anchor.fuelKey, fuelType)),
    transmission,
    transmissionKey: normalizeTransmissionKey(firstMeaningful(anchor.transmissionKey, transmission)),
    city,
    citySlug,
    confidence: clampConfidence(anchor.confidence, 0),
    source: cleanText(anchor.source),
  });
}

function buildResolvedVehicleAnchor({
  candidate = {},
  fallback = {},
  explicitFuelType = '',
  explicitTransmission = '',
} = {}) {
  const raw = getCandidateRaw(candidate);
  const make = firstMeaningful(getCandidateMake(candidate), fallback.make, fallback.brand);
  const model = firstMeaningful(getCandidateModel(candidate), fallback.model);
  const fullModel = firstMeaningful(getCandidateFullModel(candidate), fallback.fullModel);
  const variantResolved = isVariantCandidate(candidate);
  const confidence = clampConfidence(
    firstMeaningful(raw.confidence, candidate.confidence, fallback.confidence),
    model ? 0.8 : 0,
  );

  return normalizeVehicleAnchor({
    make,
    model,
    fullModel,
    makeKey: firstMeaningful(raw.makeKey, raw.brandKey, candidate.makeKey, make),
    modelKey: firstMeaningful(raw.modelKey, candidate.modelKey, candidate.canonicalKey, fullModel),
    shortModelKey: firstMeaningful(raw.shortModelKey, candidate.shortModelKey, model),
    variant: firstMeaningful(raw.variant, candidate.variant, fallback.variant),
    variantKey: firstMeaningful(raw.variantKey, candidate.variantKey, fallback.variantKey),
    fuelType: getDurablePowertrainValue({
      explicitValue: explicitFuelType,
      candidateValues: [
        ...asArray(raw.fuelTypes),
        ...asArray(candidate.fuelTypes),
        raw.fuelType,
        candidate.fuelType,
      ],
      variantResolved,
    }),
    transmission: getDurablePowertrainValue({
      explicitValue: explicitTransmission,
      candidateValues: [
        ...asArray(raw.transmissions),
        ...asArray(candidate.transmissions),
        raw.transmission,
        candidate.transmission,
      ],
      variantResolved,
    }),
    city: fallback.city,
    citySlug: fallback.citySlug,
    confidence,
    source: candidate?.source ? 'candidate_snapshot' : fallback.source || 'fallback',
  });
}

const makeModelCandidateFromAnchor = (anchor = {}) => ({
  rawText: anchor.model,
  canonicalKey: anchor.modelKey || keyify(anchor.fullModel),
  displayName: anchor.fullModel || [anchor.make, anchor.model].filter(Boolean).join(' '),
  type: 'model',
  source: 'alias',
  confidence: anchor.confidence || 0.88,
  metadata: {
    make: anchor.make,
    model: anchor.model,
    fullModel: anchor.fullModel,
    raw: {
      type: 'model',
      brand: anchor.make,
      model: anchor.model,
      rawModel: anchor.model,
      displayName: anchor.fullModel,
      modelKey: anchor.modelKey,
      shortModelKey: anchor.shortModelKey,
      matchedAlias: anchor.aliasRuleId || 'alias_registry',
      confidence: anchor.confidence || 0.88,
    },
  },
});

const enrichCandidateSnapshotWithAnchor = ({ candidateSnapshot = {}, anchor = null } = {}) => {
  if (!anchor?.model) return candidateSnapshot;

  const models = asArray(candidateSnapshot?.vehicles?.models);
  const anchorKey = vehicleIdentityKey(anchor);
  const hasModel = models.some((candidate) =>
    vehicleIdentityKey(buildResolvedVehicleAnchor({ candidate })) === anchorKey,
  );

  if (hasModel) return candidateSnapshot;

  return {
    ...(candidateSnapshot || {}),
    vehicles: {
      ...(candidateSnapshot?.vehicles || {}),
      models: [
        makeModelCandidateFromAnchor(anchor),
        ...models,
      ],
    },
    trace: {
      ...(candidateSnapshot?.trace || {}),
      contextManagerEnriched: true,
      contextManagerAlias: anchor.source === 'alias',
    },
  };
};

function shouldPreserveCity({ previousContext = {}, message = '' } = {}) {
  const text = normalizeText(message);
  if (/\b(delhi|new delhi|noida|gurgaon|gurugram)\b/.test(text)) return false;
  return Boolean(previousContext?.selectedVehicle?.city || previousContext?.anchorCity || previousContext?.city);
}

function shouldPreserveComparison({ message = '', previousContext = {}, candidateSnapshot = {} } = {}) {
  const previousVehicles = asArray(
    previousContext?.activeComparison?.vehicles ||
      previousContext?.selectedComparisonSet?.vehicles ||
      previousContext?.contextState?.activeComparison?.vehicles ||
      previousContext?.aciContextState?.activeComparison?.vehicles,
  );

  if (previousVehicles.length < 2) return false;

  const explicitModels = asArray(candidateSnapshot?.vehicles?.models);
  if (explicitModels.length >= 2) return true;

  if (/\b(colors?|colours?|sunroof|airbags?|features?|mileage|range|boot space|ground clearance|engine cc|power|price|on road|on-road|ex showroom|ex-showroom)\b/i.test(message) &&
    !/\b(price difference|show price difference|which is cheaper|compare|vs|v\/s|versus|difference between)\b/i.test(message)) {
    return false;
  }

  return /\b(which one|which is better|better|safer|safety|their|price difference|show price difference|which is cheaper|cheaper|costlier|expensive|choose|pick|recommend|verdict|final choice)\b/i.test(message);
}

function shouldClearStaleVehicleContext({ message = '', contextState = {}, candidateSnapshot = {} } = {}) {
  const selected = contextState?.selectedVehicle || {};
  if (!selected.model) return false;

  const explicitModels = asArray(candidateSnapshot?.vehicles?.models);
  if (!explicitModels.length) return false;

  const explicitAnchor = buildResolvedVehicleAnchor({ candidate: explicitModels[0] });
  if (!explicitAnchor.model) return false;

  if (/\b(this|that|it|its|same|current|selected|previous|earlier)\b/i.test(message)) return false;

  return vehicleIdentityKey(explicitAnchor) !== vehicleIdentityKey(selected);
}

const getPreviousState = (activeContext = {}) => {
  if (isAciContextState(activeContext)) return activeContext;
  if (isAciContextState(activeContext?.contextState)) return activeContext.contextState;
  if (isAciContextState(activeContext?.aciContextState)) return activeContext.aciContextState;
  return createEmptyAciContextState({
    selectedVehicle: activeContext?.selectedVehicle || {},
    activeComparison:
      activeContext?.activeComparison ||
      activeContext?.selectedComparisonSet ||
      {},
    provenance: {
      sources: ['active_context'],
      updatedBy: CONTEXT_MANAGER_VERSION,
    },
  });
};

function mergeAciContext({ previousContext = {}, resolvedContext = {}, message = '', intent = '' } = {}) {
  const previousState = getPreviousState(previousContext);
  const previousVehicle = previousState.selectedVehicle || {};
  const resolvedVehicle = resolvedContext.selectedVehicle || {};
  const nextVehicle = resolvedVehicle.model
    ? resolvedVehicle
    : previousVehicle.model && /\b(this|that|it|its|same|current|selected|previous|earlier)\b/i.test(message)
      ? previousVehicle
      : createEmptySelectedVehicleState();

  const preservedCity =
    !nextVehicle.city && shouldPreserveCity({ previousContext, message })
      ? firstMeaningful(previousVehicle.city, previousContext?.selectedVehicle?.city, previousContext?.anchorCity, previousContext?.city)
      : '';
  const preservedCitySlug =
    !nextVehicle.citySlug && shouldPreserveCity({ previousContext, message })
      ? firstMeaningful(previousVehicle.citySlug, previousContext?.selectedVehicle?.citySlug, previousContext?.anchorCity, previousContext?.city)
      : '';

  const selectedVehicle = {
    ...nextVehicle,
    city: nextVehicle.city || preservedCity,
    citySlug: nextVehicle.citySlug || preservedCitySlug,
  };

  const contextConfidence = clampConfidence(
    Math.max(
      resolvedContext.confidence?.contextConfidence || 0,
      selectedVehicle.confidence || 0,
      resolvedContext.activeComparison?.confidence || 0,
    ),
    0,
  );

  return createEmptyAciContextState({
    selectedVehicle,
    activeComparison: resolvedContext.activeComparison || previousState.activeComparison || {},
    requested: resolvedContext.requested || {},
    anchors: {
      primaryVehicle: selectedVehicle.model ? selectedVehicle : {},
      comparisonTargets: asArray(resolvedContext.activeComparison?.vehicles),
    },
    confidence: {
      entityConfidence: selectedVehicle.model || selectedVehicle.make ? selectedVehicle.confidence || contextConfidence : 0,
      modelConfidence: selectedVehicle.model ? selectedVehicle.confidence || contextConfidence : 0,
      variantConfidence: selectedVehicle.variant ? selectedVehicle.confidence || contextConfidence : 0,
      contextConfidence,
      resolutionSource:
        selectedVehicle.source ||
        resolvedContext.confidence?.resolutionSource ||
        (selectedVehicle.model ? 'active_context' : ''),
    },
    provenance: {
      sources: unique([
        ...asArray(previousState.provenance?.sources),
        ...asArray(resolvedContext.provenance?.sources),
      ]),
      warnings: unique([
        ...asArray(previousState.provenance?.warnings),
        ...asArray(resolvedContext.provenance?.warnings),
      ]),
      isolation: resolvedContext.provenance?.isolation || previousState.provenance?.isolation || '',
      updatedBy: CONTEXT_MANAGER_VERSION,
      intent,
    },
  });
}

async function hydrateContextFromCandidates({
  message = '',
  candidateSnapshot = {},
  activeContext = {},
} = {}) {
  const modelCandidates = asArray(candidateSnapshot?.vehicles?.models);
  const variantCandidates = asArray(candidateSnapshot?.vehicles?.variants);
  const primaryCandidate = modelCandidates[0] || variantCandidates[0] || null;
  const explicitFuelType = getFirstCandidateText(candidateSnapshot?.taxonomy?.fuelTypes);
  const explicitTransmission = getFirstCandidateText(candidateSnapshot?.taxonomy?.transmissions);
  const previousSelectedVehicle = getPreviousState(activeContext).selectedVehicle || {};
  const features = getFeatureKeys(candidateSnapshot);
  const taskHints = unique(asArray(candidateSnapshot?.language?.tasks).map((task = {}) =>
    firstMeaningful(task.canonicalKey, task.displayName, task.rawText),
  ));
  const hasTopicOrTaskFollowUp =
    features.length > 0 ||
    taskHints.length > 0 ||
    explicitFuelType ||
    explicitTransmission ||
    /\b(price|on road|onroad|emi|colors?|colours?|range|sunroof|features?|battery|charging|boot space|ground clearance)\b/i.test(message);
  const aliasAnchor = primaryCandidate
    ? null
    : normalizeVehicleAnchor(await resolveVehicleAlias({ message, candidateSnapshot }) || {});
  const selectedVehicle = aliasAnchor?.model
    ? aliasAnchor
    : primaryCandidate
      ? buildResolvedVehicleAnchor({
          candidate: primaryCandidate,
          fallback: previousSelectedVehicle,
          explicitFuelType,
          explicitTransmission,
        })
      : previousSelectedVehicle.model && hasTopicOrTaskFollowUp
        ? normalizeVehicleAnchor({
            ...previousSelectedVehicle,
            fuelType: explicitFuelType || previousSelectedVehicle.fuelType,
            transmission: explicitTransmission || previousSelectedVehicle.transmission,
            source: 'active_context',
          })
        : createEmptySelectedVehicleState();
  const fuelKey = getFirstCandidateKey(candidateSnapshot?.taxonomy?.fuelTypes);
  const transmissionKey = getFirstCandidateKey(candidateSnapshot?.taxonomy?.transmissions);
  const comparisonVehicles = modelCandidates.length >= 2
    ? modelCandidates.map((candidate) => buildResolvedVehicleAnchor({
        candidate,
        explicitFuelType,
        explicitTransmission,
      }))
    : shouldPreserveComparison({ message, previousContext: activeContext, candidateSnapshot })
      ? asArray(
          activeContext?.activeComparison?.vehicles ||
          activeContext?.selectedComparisonSet?.vehicles ||
          activeContext?.contextState?.activeComparison?.vehicles ||
          activeContext?.aciContextState?.activeComparison?.vehicles,
        ).map(normalizeVehicleAnchor)
      : [];

  const resolvedState = createEmptyAciContextState({
    selectedVehicle,
    activeComparison: {
      vehicles: comparisonVehicles,
      fuelKey,
      transmissionKey,
      features,
      confidence: comparisonVehicles.length >= 2 ? 0.85 : 0,
      source: comparisonVehicles.length >= 2 ? 'candidate_snapshot' : '',
    },
    requested: {
      facts: {},
      features,
      topic: features[0] || '',
      budget: getBudget(candidateSnapshot),
      city: '',
      citySlug: '',
    },
    anchors: {
      primaryVehicle: selectedVehicle.model ? selectedVehicle : {},
      comparisonTargets: comparisonVehicles,
    },
    confidence: {
      entityConfidence: selectedVehicle.model || features.length ? 0.85 : 0.35,
      modelConfidence: selectedVehicle.model ? selectedVehicle.confidence || 0.85 : 0,
      variantConfidence: selectedVehicle.variant ? selectedVehicle.confidence || 0.75 : 0,
      contextConfidence: selectedVehicle.model || comparisonVehicles.length ? selectedVehicle.confidence || 0.85 : 0.35,
      resolutionSource: selectedVehicle.source || (features.length ? 'candidate_snapshot' : ''),
    },
    provenance: {
      sources: unique([
        modelCandidates.length || variantCandidates.length || features.length ? 'candidate_snapshot' : '',
        aliasAnchor?.model ? 'alias_registry' : '',
      ]),
      warnings: aliasAnchor?.model ? ['Resolved vehicle alias through DB-confirmed alias registry.'] : [],
      updatedBy: CONTEXT_MANAGER_VERSION,
    },
  });

  let merged = mergeAciContext({
    previousContext: activeContext,
    resolvedContext: resolvedState,
    message,
  });

  const enrichedSnapshot = enrichCandidateSnapshotWithAnchor({
    candidateSnapshot,
    anchor: aliasAnchor?.model ? aliasAnchor : null,
  });

  merged = applyBuyerContextToContextState({ message, contextState: merged });

  assertAciContextStateShape(merged);

  return {
    contextState: merged,
    candidateSnapshot: enrichedSnapshot,
    aliasAnchor: aliasAnchor?.model ? aliasAnchor : null,
  };
}

const hasContextReference = (message = '') =>
  /\b(this|that|it|its|one|same|current|selected|previous|earlier|above)\b/i.test(message);

const hasComparisonLanguage = (message = '') =>
  /\b(vs|v\/s|versus|compare|comparison|compared|better|better than|difference between|price difference|show price difference|which is cheaper|cheaper|costlier|more expensive|which one|which should|choose|pick|recommend|verdict)\b/i.test(message);

const hasBroadVehicleLanguage = (message = '') =>
  /\b(cars?|vehicles?|models?|options?|suvs?|sedans?|hatchbacks?|mpvs?|muvs?)\b/i.test(message);

function applyContextIsolationRules({ message = '', contextState = {}, candidateSnapshot = {}, meaningFrame = {} } = {}) {
  const state = isAciContextState(contextState) ? contextState : getPreviousState(contextState);
  const models = asArray(candidateSnapshot?.vehicles?.models);
  const variants = asArray(candidateSnapshot?.vehicles?.variants);
  const makes = asArray(candidateSnapshot?.vehicles?.makes);
  const features = asArray(candidateSnapshot?.taxonomy?.features);
  const bodyTypes = asArray(candidateSnapshot?.taxonomy?.bodyTypes);
  const fuelTypes = asArray(candidateSnapshot?.taxonomy?.fuelTypes);
  const transmissions = asArray(candidateSnapshot?.taxonomy?.transmissions);
  const budgets = asArray(candidateSnapshot?.commerce?.budgets);
  const explicitTargetCount = Math.max(models.length, variants.length);
  const explicitComparison = (models.length >= 2 || explicitTargetCount >= 2) &&
    (hasComparisonLanguage(message) || models.length >= 2);
  const broadDiscovery =
    models.length === 0 &&
    variants.length === 0 &&
    (makes.length || features.length || bodyTypes.length || fuelTypes.length || transmissions.length || budgets.length) &&
    !hasContextReference(message) &&
    (hasBroadVehicleLanguage(message) || makes.length > 0);
  const explicitVehicleSwitch = explicitTargetCount > 0 && !hasContextReference(message);

  if (explicitComparison) {
    return {
      contextState: createEmptyAciContextState({
        ...state,
        selectedVehicle: createEmptySelectedVehicleState({
          city: state.selectedVehicle?.city || '',
          citySlug: state.selectedVehicle?.citySlug || '',
        }),
        provenance: {
          ...(state.provenance || {}),
          isolation: 'explicit_comparison_targets',
          updatedBy: CONTEXT_MANAGER_VERSION,
        },
      }),
      isolation: 'explicit_comparison_targets',
    };
  }

  if (broadDiscovery) {
    return {
      contextState: createEmptyAciContextState({
        ...state,
        selectedVehicle: createEmptySelectedVehicleState({
          city: state.selectedVehicle?.city || '',
          citySlug: state.selectedVehicle?.citySlug || '',
        }),
        activeComparison: {},
        provenance: {
          ...(state.provenance || {}),
          isolation: 'broad_discovery_without_model',
          updatedBy: CONTEXT_MANAGER_VERSION,
        },
      }),
      isolation: 'broad_discovery_without_model',
    };
  }

  if (explicitVehicleSwitch || shouldClearStaleVehicleContext({ message, contextState: state, candidateSnapshot })) {
    const switchedVehicle = {
      ...(state.selectedVehicle || {}),
      variant: '',
      variantKey: '',
    };

    return {
      contextState: createEmptyAciContextState({
        ...state,
        selectedVehicle: switchedVehicle,
        anchors: {
          ...(state.anchors || {}),
          primaryVehicle: switchedVehicle.model ? switchedVehicle : {},
        },
        activeComparison: {},
        provenance: {
          ...(state.provenance || {}),
          isolation: 'explicit_vehicle_switch',
          updatedBy: CONTEXT_MANAGER_VERSION,
        },
      }),
      isolation: 'explicit_vehicle_switch',
    };
  }

  return {
    contextState: createEmptyAciContextState({
      ...state,
      provenance: {
        ...(state.provenance || {}),
        isolation: meaningFrame?.context?.action || 'preserve_context',
        updatedBy: CONTEXT_MANAGER_VERSION,
      },
    }),
    isolation: 'preserve_context',
  };
}

function buildContextPatchFromState(contextState = {}) {
  const state = compactAciContextState(contextState);
  const vehicle = state.selectedVehicle || {};
  const comparison = state.activeComparison || {};
  const hasVehicle = Boolean(vehicle.model || vehicle.make || vehicle.city);
  const comparisonVehicles = asArray(comparison.vehicles).map((item) => compactVehicleContext(item));

  if (comparisonVehicles.length >= 2) {
    return compactObject({
      contextState: state,
    buyerContext: state.buyerContext || {},
      aciContextState: state,
      anchorCity: vehicle.citySlug || vehicle.city,
      activeComparison: compactObject({
        type: 'vehicle_compare',
        vehicles: comparisonVehicles.map((item) => compactObject({
          make: item.make,
          brand: item.make,
          model: item.model,
          fullModel: item.fullModel,
          variant: item.variant,
          variantName: item.variant,
          fuel: item.fuelType,
          transmission: item.transmission,
          city: item.city || vehicle.city,
        })),
        fuelFilter: comparison.fuelKey,
        fuelKey: comparison.fuelKey,
        transmissionKey: comparison.transmissionKey,
        features: comparison.features,
        city: vehicle.city || vehicle.citySlug,
      }),
      selectedComparisonSet: {
        vehicles: comparisonVehicles.map((item) => compactObject({
          make: item.make,
          brand: item.make,
          model: item.model,
          fullModel: item.fullModel,
          variant: item.variant,
          variantName: item.variant,
        })),
      },
      conversationMode: 'comparison',
    });
  }

  return compactObject({
      contextState: state,
      aciContextState: state,
    anchorBrand: vehicle.make,
    anchorMake: vehicle.make,
    anchorModel: vehicle.model,
    anchorFullModel: vehicle.fullModel,
    anchorVariant: vehicle.variant,
    anchorCity: vehicle.citySlug || vehicle.city,
    selectedVehicle: hasVehicle
      ? compactObject({
          make: vehicle.make,
          brand: vehicle.make,
          model: vehicle.model,
          fullModel: vehicle.fullModel,
          makeKey: vehicle.makeKey,
          modelKey: vehicle.modelKey,
          shortModelKey: vehicle.shortModelKey,
          variant: vehicle.variant,
          variantName: vehicle.variant,
          selectedVariant: vehicle.variant,
          variantKey: vehicle.variantKey,
          fuelType: vehicle.fuelType,
          fuelKey: vehicle.fuelKey,
          transmission: vehicle.transmission,
          transmissionKey: vehicle.transmissionKey,
          city: vehicle.city,
          citySlug: vehicle.citySlug,
        })
      : null,
    requested: state.requested,
    contextConfidence: state.confidence?.contextConfidence,
    resolutionSource: state.confidence?.resolutionSource,
  });
}

function getContextForToolPlan(contextState = {}) {
  const patch = buildContextPatchFromState(contextState);
  const vehicle = patch.selectedVehicle || {};

  return compactObject({
    ...patch,
    selectedVehicle: vehicle,
    anchorMake: patch.anchorMake || vehicle.make,
    anchorBrand: patch.anchorBrand || vehicle.make,
    anchorModel: patch.anchorModel || vehicle.model,
    anchorFullModel: patch.anchorFullModel || vehicle.fullModel,
    anchorVariant: patch.anchorVariant || vehicle.variant,
    anchorCity: patch.anchorCity || vehicle.citySlug || vehicle.city,
  });
}

export {
  CONTEXT_MANAGER_VERSION,
  applyContextIsolationRules,
  buildContextPatchFromState,
  buildResolvedVehicleAnchor,
  compactAciContextState,
  compactVehicleContext,
  createEmptyAciContextState,
  getContextForToolPlan,
  hydrateContextFromCandidates,
  mergeAciContext,
  mergeContextPatches,
  normalizeVehicleAnchor,
  shouldClearStaleVehicleContext,
  shouldPreserveCity,
  shouldPreserveComparison,
  stripDisplayPayloadFromContext,
  titleCaseLoose,
};

export default {
  applyContextIsolationRules,
  buildContextPatchFromState,
  buildResolvedVehicleAnchor,
  compactAciContextState,
  compactVehicleContext,
  createEmptyAciContextState,
  getContextForToolPlan,
  hydrateContextFromCandidates,
  mergeAciContext,
  mergeContextPatches,
  normalizeVehicleAnchor,
  shouldClearStaleVehicleContext,
  shouldPreserveCity,
  shouldPreserveComparison,
  stripDisplayPayloadFromContext,
};
