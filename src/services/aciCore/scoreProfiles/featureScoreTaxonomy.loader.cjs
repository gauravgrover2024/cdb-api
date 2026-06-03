const fs = require('fs');
const path = require('path');

const DEFAULT_TAXONOMY_PATH = path.join(__dirname, 'config', 'featureScoreTaxonomy.v1.json');

let cachedTaxonomy = null;

const normKey = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const normalizeFeatureDefinition = (feature) => {
  if (!feature || typeof feature !== 'object') {
    throw new Error('Invalid feature taxonomy entry.');
  }

  if (!feature.key) throw new Error('Feature taxonomy entry missing key.');
  if (!feature.category) throw new Error(`Feature taxonomy entry ${feature.key} missing category.`);
  if (!Number.isFinite(Number(feature.weight))) {
    throw new Error(`Feature taxonomy entry ${feature.key} has invalid weight.`);
  }

  return {
    ...feature,
    key: String(feature.key),
    category: String(feature.category),
    weight: Number(feature.weight),
    aliases: Array.isArray(feature.aliases) ? feature.aliases.filter(Boolean).map(String) : [],
    sourceKeys: Array.isArray(feature.sourceKeys)
      ? [...new Set(feature.sourceKeys.map(normKey).filter(Boolean))]
      : [],
  };
};

const validateTaxonomy = ({ features, layerWeights, taxonomyPath }) => {
  const featureKeys = new Set();
  const aliasOwners = new Map();

  for (const feature of features) {
    if (featureKeys.has(feature.key)) {
      throw new Error(`Duplicate taxonomy feature key: ${feature.key}`);
    }

    featureKeys.add(feature.key);

    if (!Object.prototype.hasOwnProperty.call(layerWeights, feature.category)) {
      throw new Error(
        `Feature ${feature.key} uses category ${feature.category}, but layerWeights has no such category.`
      );
    }

    if (!feature.aliases.length && !feature.sourceKeys.length) {
      throw new Error(`Feature ${feature.key} has neither aliases nor sourceKeys.`);
    }

    for (const alias of feature.aliases || []) {
      const normalizedAlias = normKey(alias);
      if (!normalizedAlias) continue;

      const existingOwner = aliasOwners.get(normalizedAlias);
      if (existingOwner && existingOwner !== feature.key) {
        throw new Error(
          `Alias collision in ${taxonomyPath}: "${alias}" normalizes to "${normalizedAlias}" and is used by both ${existingOwner} and ${feature.key}`
        );
      }

      aliasOwners.set(normalizedAlias, feature.key);
    }
  }

  const layerWeightTotal = Object.values(layerWeights).reduce(
    (sum, value) => sum + Number(value || 0),
    0
  );

  if (Math.abs(layerWeightTotal - 1) > 0.001) {
    throw new Error(`Layer weights must total 1. Current total=${layerWeightTotal}`);
  }
};

const loadFeatureScoreTaxonomy = (taxonomyPath = DEFAULT_TAXONOMY_PATH) => {
  if (cachedTaxonomy && cachedTaxonomy.__path === taxonomyPath) {
    return cachedTaxonomy;
  }

  const raw = JSON.parse(fs.readFileSync(taxonomyPath, 'utf8'));
  const layerWeights = raw.layerWeights || {};

  const features = (raw.featureDefinitions || [])
    .filter((feature) => feature.scoreEligible !== false)
    .map(normalizeFeatureDefinition);

  if (!features.length) {
    throw new Error(`No score-eligible feature definitions found in ${taxonomyPath}`);
  }

  validateTaxonomy({ features, layerWeights, taxonomyPath });

  const taxonomy = {
    __path: taxonomyPath,
    sourcePath: path.relative(process.cwd(), taxonomyPath),
    taxonomyVersion: raw.taxonomyVersion || path.basename(taxonomyPath),
    formulaCompatibility: raw.formulaCompatibility || null,
    status: raw.status || null,
    note: raw.note || null,
    sourceKeyDiscovery: raw.sourceKeyDiscovery || null,
    layerWeights,
    features,
  };

  cachedTaxonomy = taxonomy;
  return taxonomy;
};

module.exports = {
  DEFAULT_TAXONOMY_PATH,
  loadFeatureScoreTaxonomy,
};
