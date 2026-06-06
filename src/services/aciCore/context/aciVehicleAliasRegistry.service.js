'use strict';

import mongoose from 'mongoose';

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

const compactAlphaNumKey = (value = '') =>
  normalizeText(value)
    .replace(/([a-z])\s+([0-9])/g, '$1$2')
    .replace(/([0-9])\s+([a-z])/g, '$1$2');

const hasAlphaNumSignal = (value = '') => {
  const compact = compactAlphaNumKey(value);
  return /[a-z]/.test(compact) && /[0-9]/.test(compact);
};

const compactAliasContains = (message = '', alias = '') => {
  if (!message || !alias || !hasAlphaNumSignal(alias)) return false;

  if (aliasContains(message, alias)) return true;

  const compactAlias = compactAlphaNumKey(alias);
  if (!compactAlias || compactAlias.length < 3) return false;

  const tokens = normalizeText(message).split(' ').filter(Boolean);

  for (let start = 0; start < tokens.length; start += 1) {
    for (let size = 1; size <= 4 && start + size <= tokens.length; size += 1) {
      const compactWindow = compactAlphaNumKey(tokens.slice(start, start + size).join(' '));
      if (compactWindow === compactAlias) return true;
    }
  }

  return false;
};

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && cleanText(value) !== '') || '';

const uniqueTextValues = (items = []) =>
  [...new Set((Array.isArray(items) ? items : [items]).map(cleanText).filter(Boolean))];

const escapeRegex = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const aliasContains = (message = '', alias = '') =>
  new RegExp(`(^|\\s)${escapeRegex(normalizeText(alias)).replace(/\\ /g, '\\s+')}($|\\s)`, 'i').test(
    normalizeText(message),
  );

const getDb = () =>
  mongoose.connection?.readyState === 1 && mongoose.connection?.db
    ? mongoose.connection.db
    : null;

const ALIAS_RULES = [
  {
    id: 'mahindra-be-6e-to-be-6',
    aliases: ['be 6e', 'be6e', 'mahindra be 6e', 'mahindra be6e'],
    make: 'Mahindra',
    model: 'Be 6',
    candidateKeys: ['mahindra be 6', 'be 6', 'be6'],
    confidence: 0.88,
  },
  {
    id: 'mercedes-eqs-short-alias',
    aliases: ['eqs', 'mercedes eqs', 'mercedes benz eqs'],
    make: 'Mercedes Benz',
    model: 'Eqs',
    candidateKeys: ['mercedes benz eqs', 'mercedes eqs', 'eqs'],
    confidence: 0.82,
  },
  {
    id: 'bmw-ix-short-alias',
    aliases: ['ix', 'bmw ix'],
    make: 'Bmw',
    model: 'Ix',
    candidateKeys: ['bmw ix', 'ix'],
    confidence: 0.82,
  },
];

const findAliasRule = (message = '') =>
  ALIAS_RULES.find((rule) => rule.aliases.some((alias) => aliasContains(message, alias))) || null;

const buildRowQuery = (rule = {}) => {
  const makeRegex = new RegExp(`^${escapeRegex(rule.make)}$`, 'i');
  const modelRegex = new RegExp(`^${escapeRegex(rule.model)}$`, 'i');
  const fullModelRegex = new RegExp(`^${escapeRegex(`${rule.make} ${rule.model}`)}$`, 'i');
  const keyRegexes = uniqueTextValues(rule.candidateKeys).map((key) => new RegExp(`^${escapeRegex(keyify(key))}$`, 'i'));

  return {
    $or: [
      { make: makeRegex, model: modelRegex },
      { brand: makeRegex, model: modelRegex },
      { fullModel: fullModelRegex },
      { modelKey: { $in: keyRegexes } },
      { shortModelKey: { $in: keyRegexes } },
    ],
  };
};

const compactAliasAnchor = ({ row = {}, rule = {} } = {}) => {
  const make = firstMeaningful(row.make, row.brand, rule.make);
  const model = firstMeaningful(row.model, rule.model);
  const fuelTypes = uniqueTextValues(row.fuelTypes);
  const transmissions = uniqueTextValues(row.transmissions);

  if (!model) return null;

  return {
    make,
    model,
    fullModel: firstMeaningful(row.fullModel, [make, model].filter(Boolean).join(' ')),
    makeKey: keyify(firstMeaningful(row.makeKey, make)),
    modelKey: keyify(firstMeaningful(row.modelKey, [make, model].filter(Boolean).join(' '))),
    shortModelKey: keyify(firstMeaningful(row.shortModelKey, model)),
    variant: '',
    variantKey: '',
    fuelType: fuelTypes.length === 1 ? fuelTypes[0] : '',
    fuelKey: fuelTypes.length === 1 ? keyify(fuelTypes[0]) : '',
    transmission: transmissions.length === 1 ? transmissions[0] : '',
    transmissionKey: transmissions.length === 1 ? keyify(transmissions[0]) : '',
    city: '',
    citySlug: '',
    confidence: rule.confidence || 0.8,
    source: 'alias_registry',
    aliasRuleId: rule.id,
  };
};



const buildDbBackedAliasCandidates = (row = {}) => {
  const make = firstMeaningful(row.make, row.brand);
  const model = firstMeaningful(row.model);
  return uniqueTextValues([
    row.fullModel,
    [make, model].filter(Boolean).join(' '),
    model,
    row.modelKey,
    row.shortModelKey,
  ]);
};

const COMPACT_ALIAS_COLLECTIONS = [
  {
    name: 'aci_vehicle_model_summary',
    limit: 3000,
    source: 'model_summary',
  },
  {
    name: 'vehicle_variant_feature_matrix_v2',
    limit: 8000,
    source: 'feature_matrix',
  },
  {
    name: 'aci_vehicle_price_rows',
    limit: 8000,
    source: 'price_rows',
  },
  {
    name: 'vehicles',
    limit: 8000,
    source: 'vehicles',
  },
];

const compactAliasProjection = {
  make: 1,
  brand: 1,
  model: 1,
  fullModel: 1,
  displayName: 1,
  makeKey: 1,
  modelKey: 1,
  shortModelKey: 1,
  fuelTypes: 1,
  transmissions: 1,
};

const collectionExists = async (db, name) => {
  try {
    return Boolean(await db.listCollections({ name }).hasNext());
  } catch {
    return false;
  }
};


const compactAliasMessageCandidates = (message = '') => {
  const tokens = normalizeText(message).split(' ').filter(Boolean);
  const candidates = new Set();

  for (let start = 0; start < tokens.length; start += 1) {
    for (let size = 1; size <= 4 && start + size <= tokens.length; size += 1) {
      const phrase = tokens.slice(start, start + size).join(' ');
      const compact = compactAlphaNumKey(phrase);

      if (!compact || compact.length < 3 || !hasAlphaNumSignal(compact)) continue;

      candidates.add(compact);
      candidates.add(phrase);
    }
  }

  return [...candidates]
    .map(cleanText)
    .filter(Boolean)
    .sort((left, right) => compactAlphaNumKey(right).length - compactAlphaNumKey(left).length)
    .slice(0, 12);
};

const looseAlphaNumRegex = (value = '') => {
  const compact = compactAlphaNumKey(value);
  if (!compact || compact.length < 3 || !hasAlphaNumSignal(compact)) return null;

  const parts = compact.match(/[a-z]+|[0-9]+/g) || [compact];
  const core = parts.map(escapeRegex).join('[\\s-]*');

  return new RegExp(`(^|[^a-z0-9])${core}([^a-z0-9]|$)`, 'i');
};

const buildCompactAliasDbQuery = (message = '') => {
  const regexes = compactAliasMessageCandidates(message)
    .map(looseAlphaNumRegex)
    .filter(Boolean);

  if (!regexes.length) return null;

  const fields = [
    'make',
    'brand',
    'model',
    'fullModel',
    'displayName',
    'modelKey',
    'shortModelKey',
  ];

  return {
    $or: fields.map((field) => ({ [field]: { $in: regexes } })),
  };
};


async function resolveGenericCompactVehicleAlias({ message = '', db = null } = {}) {
  if (!db || !hasAlphaNumSignal(message)) return null;

  const query = buildCompactAliasDbQuery(message);
  if (!query) return null;

  const matches = [];

  for (const collection of COMPACT_ALIAS_COLLECTIONS) {
    if (!(await collectionExists(db, collection.name))) continue;

    const rows = await db.collection(collection.name)
      .find(query, { projection: compactAliasProjection })
      .limit(Math.min(collection.limit || 250, 250))
      .toArray();

    for (const row of rows) {
      const aliases = buildDbBackedAliasCandidates(row);
      for (const alias of aliases) {
        if (!hasAlphaNumSignal(alias)) continue;
        if (!compactAliasContains(message, alias)) continue;

        matches.push({
          row,
          alias,
          compactAlias: compactAlphaNumKey(alias),
          source: collection.source,
        });
      }
    }
  }

  matches.sort((left, right) => {
    if (right.compactAlias.length !== left.compactAlias.length) {
      return right.compactAlias.length - left.compactAlias.length;
    }

    const sourceRank = {
      model_summary: 4,
      feature_matrix: 3,
      price_rows: 2,
      vehicles: 1,
    };

    const leftRank = sourceRank[left.source] || 0;
    const rightRank = sourceRank[right.source] || 0;
    if (rightRank !== leftRank) return rightRank - leftRank;

    return String(right.alias || '').length - String(left.alias || '').length;
  });

  const best = matches[0];
  if (!best?.row) return null;

  return compactAliasAnchor({
    row: best.row,
    rule: {
      id: `db-backed-compact-alphanum-alias:${best.source || 'unknown'}`,
      confidence: best.source === 'model_summary' ? 0.86 : 0.82,
    },
  });
}


async function resolveVehicleAlias({ message = '' } = {}) {
  const db = getDb();
  if (!db) return null;

  const rule = findAliasRule(message);

  if (rule) {
    const row = await db.collection('aci_vehicle_model_summary').findOne(
      buildRowQuery(rule),
      {
        projection: {
          make: 1,
          brand: 1,
          model: 1,
          fullModel: 1,
          makeKey: 1,
          modelKey: 1,
          shortModelKey: 1,
          fuelTypes: 1,
          transmissions: 1,
        },
      },
    );

    if (row) return compactAliasAnchor({ row, rule });
  }

  return resolveGenericCompactVehicleAlias({ message, db });
}

export {
  ALIAS_RULES,
  resolveVehicleAlias,
};

export default resolveVehicleAlias;
