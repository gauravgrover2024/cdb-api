'use strict';

/**
 * ACI Capability Registry Contract
 *
 * Capabilities are product/business abilities that the ACI Intelligence Core can route to.
 * New modules must be added as capabilities, not by modifying the core brain flow.
 *
 * This file must not contain automotive facts.
 * It only defines capability contracts and registration helpers.
 */

const CAPABILITY_STATUS = Object.freeze({
  ACTIVE: 'active',
  EXPERIMENTAL: 'experimental',
  DISABLED: 'disabled',
});

const CAPABILITY_SOURCE_TYPES = Object.freeze({
  DB: 'db',
  THIRD_PARTY_PROVIDER: 'third_party_provider',
  DERIVED_INTELLIGENCE: 'derived_intelligence',
  HYBRID: 'hybrid',
});

const capabilityRegistry = new Map();

function validateCapabilityDefinition(capability) {
  if (!capability || typeof capability !== 'object') {
    throw new Error('ACI capability definition must be an object');
  }

  if (!capability.capabilityId || typeof capability.capabilityId !== 'string') {
    throw new Error('ACI capability missing capabilityId');
  }

  if (!capability.domain || typeof capability.domain !== 'string') {
    throw new Error(`ACI capability ${capability.capabilityId} missing domain`);
  }

  if (!Array.isArray(capability.supportedTasks)) {
    throw new Error(`ACI capability ${capability.capabilityId} supportedTasks must be an array`);
  }

  if (!Array.isArray(capability.requiredEntities)) {
    throw new Error(`ACI capability ${capability.capabilityId} requiredEntities must be an array`);
  }

  if (!Array.isArray(capability.optionalEntities)) {
    throw new Error(`ACI capability ${capability.capabilityId} optionalEntities must be an array`);
  }

  if (!Array.isArray(capability.tools)) {
    throw new Error(`ACI capability ${capability.capabilityId} tools must be an array`);
  }

  if (!capability.sourceType || !Object.values(CAPABILITY_SOURCE_TYPES).includes(capability.sourceType)) {
    throw new Error(`ACI capability ${capability.capabilityId} has invalid sourceType`);
  }

  if (!capability.status || !Object.values(CAPABILITY_STATUS).includes(capability.status)) {
    throw new Error(`ACI capability ${capability.capabilityId} has invalid status`);
  }

  return true;
}

function registerCapability(capability) {
  validateCapabilityDefinition(capability);

  if (capabilityRegistry.has(capability.capabilityId)) {
    throw new Error(`ACI capability already registered: ${capability.capabilityId}`);
  }

  capabilityRegistry.set(capability.capabilityId, Object.freeze({
    version: '1.0.0',
    requiresConsent: false,
    requiredProviders: [],
    canvasTypes: [],
    confidencePolicy: null,
    timeoutMs: null,
    ...capability,
  }));

  return capabilityRegistry.get(capability.capabilityId);
}

function getCapability(capabilityId) {
  return capabilityRegistry.get(capabilityId) || null;
}

function listCapabilities(options = {}) {
  const { includeDisabled = false, domain = null } = options;

  return Array.from(capabilityRegistry.values()).filter((capability) => {
    if (!includeDisabled && capability.status === CAPABILITY_STATUS.DISABLED) return false;
    if (domain && capability.domain !== domain) return false;
    return true;
  });
}

function clearCapabilityRegistryForTests() {
  capabilityRegistry.clear();
}

export {
  CAPABILITY_STATUS,
  CAPABILITY_SOURCE_TYPES,
  registerCapability,
  getCapability,
  listCapabilities,
  validateCapabilityDefinition,
  clearCapabilityRegistryForTests,
};
