const {
  MANDATORY_FINAL_RECOMMENDATION_INPUTS,
} = require('./aciDecisionPolicy.constants.cjs');

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

  const filters = asObject(ctx.filters || contextState.filters || data.filters || response.filters);
  const entities = asObject(ctx.entities || data.entities || response.entities);
  const priorities = asObject(buyerContext.priorities || ctx.priorities || data.priorities || response.priorities);

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
      data.comparisonTargets ||
      response.comparisonTargets
  );

  return {
    ctx,
    data,
    meta,
    contextState,
    buyerContext,
    selectedVehicle,
    filters,
    entities,
    priorities,
    shortlistedModels,
    comparisonTargets,
  };
};

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

function buildBuyerDecisionInputContract({ context = {}, response = {} } = {}) {
  const {
    ctx,
    data,
    contextState,
    buyerContext,
    selectedVehicle,
    filters,
    entities,
    priorities,
    shortlistedModels,
    comparisonTargets,
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
    shortlist: normalizeList(shortlistedModels.length ? shortlistedModels : comparisonTargets),
  };
}

module.exports = {
  CONTRACT_VERSION,
  buildBuyerDecisionInputContract,
};
