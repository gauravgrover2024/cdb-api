'use strict';

/**
 * ACI Provider Registry Contract
 *
 * Providers are external systems/APIs used by capabilities:
 * challan, RC, insurance, finance, service history, exchange valuation, CRM, etc.
 *
 * The core brain must never call third-party APIs directly.
 * It should call tools/capabilities, which use provider adapters behind this contract.
 */

const PROVIDER_STATUS = Object.freeze({
  ACTIVE: 'active',
  EXPERIMENTAL: 'experimental',
  DISABLED: 'disabled',
});

const PROVIDER_AUTH_TYPES = Object.freeze({
  NONE: 'none',
  API_KEY: 'api_key',
  OAUTH: 'oauth',
  BASIC: 'basic',
  CUSTOM: 'custom',
});

const providerRegistry = new Map();

function validateProviderDefinition(provider) {
  if (!provider || typeof provider !== 'object') {
    throw new Error('ACI provider definition must be an object');
  }

  if (!provider.providerId || typeof provider.providerId !== 'string') {
    throw new Error('ACI provider missing providerId');
  }

  if (!provider.domain || typeof provider.domain !== 'string') {
    throw new Error(`ACI provider ${provider.providerId} missing domain`);
  }

  if (!provider.status || !Object.values(PROVIDER_STATUS).includes(provider.status)) {
    throw new Error(`ACI provider ${provider.providerId} has invalid status`);
  }

  if (!provider.authType || !Object.values(PROVIDER_AUTH_TYPES).includes(provider.authType)) {
    throw new Error(`ACI provider ${provider.providerId} has invalid authType`);
  }

  if (!Array.isArray(provider.supportedOperations)) {
    throw new Error(`ACI provider ${provider.providerId} supportedOperations must be an array`);
  }

  if (typeof provider.requiresConsent !== 'boolean') {
    throw new Error(`ACI provider ${provider.providerId} requiresConsent must be boolean`);
  }

  return true;
}

function registerProvider(provider) {
  validateProviderDefinition(provider);

  if (providerRegistry.has(provider.providerId)) {
    throw new Error(`ACI provider already registered: ${provider.providerId}`);
  }

  providerRegistry.set(provider.providerId, Object.freeze({
    version: '1.0.0',
    timeoutMs: 8000,
    retryPolicy: {
      retries: 1,
      retryOnTimeout: false,
    },
    privacyPolicy: {
      storesRawResponse: false,
      storesCustomerPII: false,
      redactsSensitiveFields: true,
    },
    confidencePolicy: null,
    ...provider,
  }));

  return providerRegistry.get(provider.providerId);
}

function getProvider(providerId) {
  return providerRegistry.get(providerId) || null;
}

function listProviders(options = {}) {
  const { includeDisabled = false, domain = null } = options;

  return Array.from(providerRegistry.values()).filter((provider) => {
    if (!includeDisabled && provider.status === PROVIDER_STATUS.DISABLED) return false;
    if (domain && provider.domain !== domain) return false;
    return true;
  });
}

function normalizeProviderResult(result = {}) {
  return {
    providerId: result.providerId || null,
    operation: result.operation || null,
    status: result.status || 'unknown',
    data: result.data || null,
    confidence: result.confidence || null,
    source: result.source || 'third_party_provider',
    requiresConsent: Boolean(result.requiresConsent),
    latencyMs: typeof result.latencyMs === 'number' ? result.latencyMs : null,
    errors: Array.isArray(result.errors) ? result.errors : [],
    fetchedAt: result.fetchedAt || new Date().toISOString(),
    metadata: result.metadata || {},
  };
}

function clearProviderRegistryForTests() {
  providerRegistry.clear();
}

export {
  PROVIDER_STATUS,
  PROVIDER_AUTH_TYPES,
  registerProvider,
  getProvider,
  listProviders,
  validateProviderDefinition,
  normalizeProviderResult,
  clearProviderRegistryForTests,
};
