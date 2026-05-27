'use strict';

/**
 * ACI Canonical Feature Taxonomy Contract
 *
 * Purpose:
 * Normalize raw brand/model/variant feature names into comparable canonical capabilities.
 *
 * Example idea, not hardcoded data:
 * Raw names like "ABS", "Anti-lock Braking System", "Anti Lock Brakes"
 * should map to one canonical feature key.
 *
 * Important:
 * - This file defines structure only.
 * - Do not hardcode actual feature truth/availability here.
 * - Do not use this file as a factual source for which car has which feature.
 * - Raw feature data must still come from DB/source collections.
 */

const FEATURE_TAXONOMY_SCHEMA_VERSION = 'aci.canonicalFeature.v1';

const FEATURE_IMPORTANCE_LEVELS = Object.freeze({
  CRITICAL: 'critical',
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
  UNKNOWN: 'unknown',
});

const FEATURE_VALUE_TYPES = Object.freeze({
  BOOLEAN: 'boolean',
  NUMBER: 'number',
  TEXT: 'text',
  ENUM: 'enum',
  LIST: 'list',
  RANGE: 'range',
  UNKNOWN: 'unknown',
});

const FEATURE_MAPPING_STATUS = Object.freeze({
  CANONICAL: 'canonical',
  ALIAS: 'alias',
  RELATED: 'related',
  SPLIT_REQUIRED: 'split_required',
  REVIEW_REQUIRED: 'review_required',
  REJECTED: 'rejected',
});

function normalizeFeatureKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
}

function createCanonicalFeature(overrides = {}) {
  const canonicalKey = overrides.canonicalKey
    ? normalizeFeatureKey(overrides.canonicalKey)
    : null;

  return {
    schemaVersion: FEATURE_TAXONOMY_SCHEMA_VERSION,
    canonicalKey,
    displayName: overrides.displayName || null,
    category: overrides.category || null,
    subCategory: overrides.subCategory || null,
    description: overrides.description || null,

    aliases: Array.isArray(overrides.aliases) ? overrides.aliases : [],
    relatedCanonicalKeys: Array.isArray(overrides.relatedCanonicalKeys)
      ? overrides.relatedCanonicalKeys.map(normalizeFeatureKey)
      : [],

    comparableWith: Array.isArray(overrides.comparableWith)
      ? overrides.comparableWith.map(normalizeFeatureKey)
      : [],

    valueType: overrides.valueType || FEATURE_VALUE_TYPES.BOOLEAN,
    allowedValues: Array.isArray(overrides.allowedValues) ? overrides.allowedValues : [],

    importance: overrides.importance || FEATURE_IMPORTANCE_LEVELS.UNKNOWN,
    weights: {
      safety: null,
      comfort: null,
      convenience: null,
      technology: null,
      performance: null,
      ownership: null,
      resale: null,
      ...overrides.weights,
    },

    flags: {
      isSafetyCritical: false,
      isRegulatory: false,
      isLuxury: false,
      isConvenience: false,
      isPerformance: false,
      isOwnershipCostRelevant: false,
      ...overrides.flags,
    },

    metadata: {
      source: overrides.source || null,
      confidence: overrides.confidence || null,
      reviewedBy: overrides.reviewedBy || null,
      reviewedAt: overrides.reviewedAt || null,
      notes: overrides.notes || null,
      ...overrides.metadata,
    },
  };
}

function createRawFeatureMapping(overrides = {}) {
  return {
    schemaVersion: FEATURE_TAXONOMY_SCHEMA_VERSION,
    rawFeatureName: overrides.rawFeatureName || '',
    normalizedRawFeatureName: normalizeFeatureKey(overrides.rawFeatureName || ''),
    canonicalKeys: Array.isArray(overrides.canonicalKeys)
      ? overrides.canonicalKeys.map(normalizeFeatureKey)
      : [],
    mappingStatus: overrides.mappingStatus || FEATURE_MAPPING_STATUS.REVIEW_REQUIRED,
    mappingConfidence: typeof overrides.mappingConfidence === 'number'
      ? overrides.mappingConfidence
      : null,
    sourceCollection: overrides.sourceCollection || null,
    sourceDocumentId: overrides.sourceDocumentId || null,
    sourceField: overrides.sourceField || null,
    notes: overrides.notes || null,
    createdAt: overrides.createdAt || new Date().toISOString(),
  };
}

function validateCanonicalFeature(feature) {
  if (!feature || typeof feature !== 'object') {
    throw new Error('Canonical feature must be an object');
  }

  if (feature.schemaVersion !== FEATURE_TAXONOMY_SCHEMA_VERSION) {
    throw new Error(`Unsupported canonical feature schemaVersion: ${feature.schemaVersion}`);
  }

  if (!feature.canonicalKey || typeof feature.canonicalKey !== 'string') {
    throw new Error('Canonical feature missing canonicalKey');
  }

  if (feature.canonicalKey !== normalizeFeatureKey(feature.canonicalKey)) {
    throw new Error(`Canonical feature key is not normalized: ${feature.canonicalKey}`);
  }

  if (!feature.displayName || typeof feature.displayName !== 'string') {
    throw new Error(`Canonical feature ${feature.canonicalKey} missing displayName`);
  }

  if (!feature.category || typeof feature.category !== 'string') {
    throw new Error(`Canonical feature ${feature.canonicalKey} missing category`);
  }

  if (!Object.values(FEATURE_VALUE_TYPES).includes(feature.valueType)) {
    throw new Error(`Canonical feature ${feature.canonicalKey} has invalid valueType`);
  }

  if (!Object.values(FEATURE_IMPORTANCE_LEVELS).includes(feature.importance)) {
    throw new Error(`Canonical feature ${feature.canonicalKey} has invalid importance`);
  }

  return true;
}

function validateRawFeatureMapping(mapping) {
  if (!mapping || typeof mapping !== 'object') {
    throw new Error('Raw feature mapping must be an object');
  }

  if (mapping.schemaVersion !== FEATURE_TAXONOMY_SCHEMA_VERSION) {
    throw new Error(`Unsupported raw feature mapping schemaVersion: ${mapping.schemaVersion}`);
  }

  if (!mapping.rawFeatureName || typeof mapping.rawFeatureName !== 'string') {
    throw new Error('Raw feature mapping missing rawFeatureName');
  }

  if (!Array.isArray(mapping.canonicalKeys)) {
    throw new Error(`Raw feature mapping ${mapping.rawFeatureName} canonicalKeys must be an array`);
  }

  if (!Object.values(FEATURE_MAPPING_STATUS).includes(mapping.mappingStatus)) {
    throw new Error(`Raw feature mapping ${mapping.rawFeatureName} has invalid mappingStatus`);
  }

  return true;
}

export {
  FEATURE_TAXONOMY_SCHEMA_VERSION,
  FEATURE_IMPORTANCE_LEVELS,
  FEATURE_VALUE_TYPES,
  FEATURE_MAPPING_STATUS,
  normalizeFeatureKey,
  createCanonicalFeature,
  createRawFeatureMapping,
  validateCanonicalFeature,
  validateRawFeatureMapping,
};
