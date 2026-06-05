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

function createEmptyAciContextState(overrides = {}) {
  return {
    schemaVersion: ACI_CONTEXT_STATE_SCHEMA_VERSION,
    selectedVehicle: createEmptySelectedVehicleState(overrides.selectedVehicle || {}),
    activeComparison: createEmptyActiveComparisonState(overrides.activeComparison || {}),
    requested: createEmptyRequestedState(overrides.requested || {}),
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

  if (!state.provenance || typeof state.provenance !== 'object') {
    throw new Error('ACI context state missing provenance object');
  }

  return true;
}

export {
  ACI_CONTEXT_STATE_SCHEMA_VERSION,
  assertAciContextStateShape,
  createEmptyAciContextState,
  createEmptyActiveComparisonState,
  createEmptyRequestedState,
  createEmptySelectedVehicleState,
  isAciContextState,
};

export default createEmptyAciContextState;
