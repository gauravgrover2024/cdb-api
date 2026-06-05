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

async function resolveVehicleAlias({ message = '' } = {}) {
  const rule = findAliasRule(message);
  if (!rule) return null;

  const db = getDb();
  if (!db) return null;

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

  return row ? compactAliasAnchor({ row, rule }) : null;
}

export {
  ALIAS_RULES,
  resolveVehicleAlias,
};

export default resolveVehicleAlias;
