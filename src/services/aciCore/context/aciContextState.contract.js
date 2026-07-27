'use strict';

const ACI_CONTEXT_STATE_SCHEMA_VERSION = 'aci_context_state_v1';

const createEmptySelectedVehicleState = (overrides = {}) => ({
  make: '',
  model: '',
  fullModel: '',
  makeKey: '',
  modelKey: '',
  shortModelKey: '',
  variant: '',
  variantKey: '',
  fuelType: '',
  fuelKey: '',
  transmission: '',
  transmissionKey: '',
  bodyType: '',
  seatingCapacity: '',
  fuelTypes: [],
  transmissions: [],
  priceBand: '',
  safetyFeatures: [],
  featureHighlights: [],
  ownershipSignals: [],
  similarAlternatives: [],
  city: '',
  citySlug: '',
  confidence: 0,
  source: '',
  ...overrides,
});

const createEmptyActiveComparisonState = (overrides = {}) => ({
  vehicles: [],
  fuelKey: '',
  transmissionKey: '',
  city: '',
  citySlug: '',
  features: [],
  confidence: 0,
  source: '',
  ...overrides,
});

const createEmptyRequestedState = (overrides = {}) => ({
  facts: {},
  features: [],
  topic: '',
  budget: {},
  city: '',
  citySlug: '',
  ...overrides,
});

const createEmptyBuyerContextState = (overrides = {}) => ({
  city: '',
  citySlug: '',
  budgetOrPriceCeiling: '',
  maxBudget: 0,
  bodyPreferenceOrPrimaryUseCase: '',
  primaryUseCase: '',
  familySizeOrOccupancyUse: '',
  fuelPreferenceOrMonthlyRunning: '',
  fuelPreference: '',
  monthlyRunning: '',
  transmissionPreference: '',
  safetyPriority: '',
  featurePriority: [],
  shortlistedModelsOrDiscoveryScope: '',
  source: '',
  confidence: 0,
  extractedAt: '',
  ...overrides,
});

function createEmptyAciContextState(overrides = {}) {
  return {
    schemaVersion: ACI_CONTEXT_STATE_SCHEMA_VERSION,
    selectedVehicle: createEmptySelectedVehicleState(overrides.selectedVehicle || {}),
    activeComparison: createEmptyActiveComparisonState(overrides.activeComparison || {}),
    requested: createEmptyRequestedState(overrides.requested || {}),
    buyerContext: createEmptyBuyerContextState(overrides.buyerContext || overrides.buyerIntent || {}),
    contextLedger: overrides.contextLedger || {},
    buyerGuidanceContext: {
      guidanceMode: '',
      finalPurchaseVerdictEnabled: false,
      selectedVehicleFacts: {},
      explicitBuyerContext: {},
      inferredContext: {},
      softAssumptions: [],
      softQuestion: '',
      ...(overrides.buyerGuidanceContext || {}),
    },
    anchors: {
      primaryVehicle: {},
      comparisonTargets: [],
      ...(overrides.anchors || {}),
    },
    confidence: {
      entityConfidence: 0,
      modelConfidence: 0,
      variantConfidence: 0,
      contextConfidence: 0,
      resolutionSource: '',
      ...(overrides.confidence || {}),
    },
    provenance: {
      sources: [],
      warnings: [],
      isolation: '',
      updatedBy: '',
      ...(overrides.provenance || {}),
    },
  };
}

const isAciContextState = (value = {}) =>
  Boolean(value && typeof value === 'object' && value.schemaVersion === ACI_CONTEXT_STATE_SCHEMA_VERSION);

function assertAciContextStateShape(state = {}) {
  if (!state || typeof state !== 'object') {
    throw new Error('ACI context state must be an object');
  }

  if (state.schemaVersion !== ACI_CONTEXT_STATE_SCHEMA_VERSION) {
    throw new Error(`Unsupported ACI context state schemaVersion: ${state.schemaVersion}`);
  }

  if (!state.selectedVehicle || typeof state.selectedVehicle !== 'object') {
    throw new Error('ACI context state missing selectedVehicle object');
  }

  if (!state.activeComparison || typeof state.activeComparison !== 'object') {
    throw new Error('ACI context state missing activeComparison object');
  }

  if (!state.requested || typeof state.requested !== 'object') {
    throw new Error('ACI context state missing requested object');
  }

  if (!state.buyerContext || typeof state.buyerContext !== 'object') {
    throw new Error('ACI context state missing buyerContext object');
  }

  if (!state.contextLedger || typeof state.contextLedger !== 'object') {
    throw new Error('ACI context state missing contextLedger object');
  }

  if (!state.buyerGuidanceContext || typeof state.buyerGuidanceContext !== 'object') {
    throw new Error('ACI context state missing buyerGuidanceContext object');
  }

  if (!state.provenance || typeof state.provenance !== 'object') {
    throw new Error('ACI context state missing provenance object');
  }

  return true;
}

export {
  ACI_CONTEXT_STATE_SCHEMA_VERSION,
  assertAciContextStateShape,
  createEmptyAciContextState,
  createEmptyBuyerContextState,
  createEmptyActiveComparisonState,
  createEmptyRequestedState,
  createEmptySelectedVehicleState,
  isAciContextState,
};

export default createEmptyAciContextState;
