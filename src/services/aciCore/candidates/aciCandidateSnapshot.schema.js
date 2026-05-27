'use strict';

/**
 * ACI Candidate Snapshot Contract
 *
 * Candidate retrieval happens before semantic parsing.
 * It should provide DB-backed possible entities/features/filters/providers to the parser.
 *
 * This file defines structure only.
 * It must not contain factual automotive availability.
 */

const CANDIDATE_SNAPSHOT_SCHEMA_VERSION = 'aci.candidateSnapshot.v1';

const CANDIDATE_SOURCE_TYPES = Object.freeze({
  DB: 'db',
  CONTEXT: 'context',
  USER_TEXT: 'user_text',
  PROVIDER: 'provider',
  STATIC_LANGUAGE_OPERATOR: 'static_language_operator',
  UNKNOWN: 'unknown',
});

function createCandidateItem(overrides = {}) {
  return {
    rawText: overrides.rawText || null,
    canonicalKey: overrides.canonicalKey || null,
    displayName: overrides.displayName || null,
    type: overrides.type || null,
    source: overrides.source || CANDIDATE_SOURCE_TYPES.UNKNOWN,
    confidence: typeof overrides.confidence === 'number' ? overrides.confidence : null,
    metadata: overrides.metadata || {},
  };
}

function createEmptyCandidateSnapshot(overrides = {}) {
  return {
    schemaVersion: CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
    rawMessage: overrides.rawMessage || '',
    normalizedMessage: overrides.normalizedMessage || '',
    activeContext: overrides.activeContext || null,

    vehicles: {
      makes: Array.isArray(overrides?.vehicles?.makes) ? overrides.vehicles.makes : [],
      models: Array.isArray(overrides?.vehicles?.models) ? overrides.vehicles.models : [],
      variants: Array.isArray(overrides?.vehicles?.variants) ? overrides.vehicles.variants : [],
      colors: Array.isArray(overrides?.vehicles?.colors) ? overrides.vehicles.colors : [],
    },

    taxonomy: {
      features: Array.isArray(overrides?.taxonomy?.features) ? overrides.taxonomy.features : [],
      featureAliases: Array.isArray(overrides?.taxonomy?.featureAliases) ? overrides.taxonomy.featureAliases : [],
      bodyTypes: Array.isArray(overrides?.taxonomy?.bodyTypes) ? overrides.taxonomy.bodyTypes : [],
      fuelTypes: Array.isArray(overrides?.taxonomy?.fuelTypes) ? overrides.taxonomy.fuelTypes : [],
      transmissions: Array.isArray(overrides?.taxonomy?.transmissions) ? overrides.taxonomy.transmissions : [],
    },

    commerce: {
      budgets: Array.isArray(overrides?.commerce?.budgets) ? overrides.commerce.budgets : [],
      cities: Array.isArray(overrides?.commerce?.cities) ? overrides.commerce.cities : [],
      finance: Array.isArray(overrides?.commerce?.finance) ? overrides.commerce.finance : [],
      ownership: Array.isArray(overrides?.commerce?.ownership) ? overrides.commerce.ownership : [],
    },

    language: {
      tasks: Array.isArray(overrides?.language?.tasks) ? overrides.language.tasks : [],
      operators: Array.isArray(overrides?.language?.operators) ? overrides.language.operators : [],
      ambiguity: Array.isArray(overrides?.language?.ambiguity) ? overrides.language.ambiguity : [],
    },

    providers: {
      requiredProviderHints: Array.isArray(overrides?.providers?.requiredProviderHints)
        ? overrides.providers.requiredProviderHints
        : [],
    },

    trace: {
      candidateRetriever: overrides?.trace?.candidateRetriever || null,
      candidateRetrieverVersion: overrides?.trace?.candidateRetrieverVersion || null,
      createdAt: overrides?.trace?.createdAt || new Date().toISOString(),
    },
  };
}

function assertCandidateSnapshotShape(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    throw new Error('ACI candidate snapshot must be an object');
  }

  if (snapshot.schemaVersion !== CANDIDATE_SNAPSHOT_SCHEMA_VERSION) {
    throw new Error(`Unsupported candidate snapshot schemaVersion: ${snapshot.schemaVersion}`);
  }

  if (!snapshot.vehicles || typeof snapshot.vehicles !== 'object') {
    throw new Error('ACI candidate snapshot missing vehicles');
  }

  if (!snapshot.taxonomy || typeof snapshot.taxonomy !== 'object') {
    throw new Error('ACI candidate snapshot missing taxonomy');
  }

  if (!snapshot.language || typeof snapshot.language !== 'object') {
    throw new Error('ACI candidate snapshot missing language');
  }

  return true;
}

export {
  CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
  CANDIDATE_SOURCE_TYPES,
  createCandidateItem,
  createEmptyCandidateSnapshot,
  assertCandidateSnapshotShape,
};
