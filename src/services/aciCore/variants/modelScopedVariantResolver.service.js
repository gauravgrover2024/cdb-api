'use strict';

import mongoose from 'mongoose';

const DEFAULT_CITY_SLUG = process.env.ACI_VARIANT_RESOLVER_CITY_SLUG || 'new-delhi';
const CACHE_TTL_MS = Number(process.env.ACI_VARIANT_RESOLVER_CACHE_TTL_MS || 5 * 60 * 1000);
const PRICE_ROWS_COLLECTION = 'aci_vehicle_price_rows';

const REQUEST_NOISE_TERMS = [
  'does',
  'do',
  'is',
  'are',
  'have',
  'has',
  'get',
  'gets',
  'give',
  'tell',
  'show',
  'list',
  'with',
  'without',
  'and',
  'or',
  'available',
  'come',
  'comes',
  'in',
  'for',
  'of',
  'the',
  'this',
  'that',
  'it',
  'its',
  'price',
  'prices',
  'pricing',
  'on',
  'road',
  'onroad',
  'ex',
  'showroom',
  'breakup',
  'breakdown',
  'feature',
  'features',
  'sunroof',
  'adas',
  'airbag',
  'airbags',
  'six',
  '6',
  'camera',
  'tpms',
  'wireless',
  'cruise',
  'control',
  'ventilated',
  'seat',
  'seats',
  'rear',
  'parking',
  'safety',
  'colour',
  'color',
  'colours',
  'colors',
  'emi',
  'finance',
  'loan',
  'tenure',
  'year',
  'years',
  'month',
  'months',
  'lakh',
  'lakhs',
  'lac',
  'lacs',
  'down',
  'payment',
  'dp',
  'quote',
  'quotation',
  'best',
  'now',
  'current',
  'new',
  'latest',
  'black',
  'white',
  'red',
  'blue',
  'grey',
  'gray',
  'silver',
  'green',
  'brown',
  'orange',
  'yellow',
  'dual',
  'tone',
  'monotone',
  'single',
];

const GENERIC_FILTER_TOKENS = new Set([
  'automatic',
  'auto',
  'manual',
  'petrol',
  'diesel',
  'cng',
  'electric',
  'ev',
  'hybrid',
  'amt',
  'dct',
  'ivt',
  'cvt',
  'at',
  'mt',
]);

let variantCatalogCache = {
  builtAt: 0,
  byScope: new Map(),
};

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const normalizeSearchText = (value = '') =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/([a-z0-9])\s*\(([^)]+)\)/gi, '$1 $2 ')
    .replace(/[’']/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const slugKey = (value = '') => normalizeSearchText(value).replace(/\s+/g, '-');

const tokensFrom = (value = '') =>
  normalizeSearchText(value)
    .split(' ')
    .map((token) => token.trim())
    .filter(Boolean);

const uniqueBy = (items = [], keyFn) => {
  const seen = new Set();
  const output = [];

  for (const item of items) {
    const key = keyFn(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }

  return output;
};

const hasTokenSequence = (haystack = [], needle = []) => {
  if (!haystack.length || !needle.length || needle.length > haystack.length) return false;

  for (let index = 0; index <= haystack.length - needle.length; index += 1) {
    const matches = needle.every((token, offset) => haystack[index + offset] === token);
    if (matches) return true;
  }

  return false;
};

const findTokenSequenceStart = (haystack = [], needle = []) => {
  if (!haystack.length || !needle.length || needle.length > haystack.length) return -1;

  for (let index = 0; index <= haystack.length - needle.length; index += 1) {
    const matches = needle.every((token, offset) => haystack[index + offset] === token);
    if (matches) return index;
  }

  return -1;
};

const countCoveredTokens = (source = [], target = []) => {
  const sourceSet = new Set(source);
  return target.filter((token) => sourceSet.has(token)).length;
};

const stripMakeModelTokens = ({ tokens = [], make = '', model = '', fullModel = '' } = {}) => {
  const vehicleTokens = new Set(
    [
      ...tokensFrom(make),
      ...tokensFrom(model),
      ...tokensFrom(fullModel),
    ].filter(Boolean),
  );

  return tokens.filter((token) => !vehicleTokens.has(token));
};

const getNoiseTokens = ({ citySlug = '' } = {}) =>
  new Set([
    ...REQUEST_NOISE_TERMS,
    ...tokensFrom(citySlug),
  ].filter(Boolean));

const extractExplicitVariantTokens = ({
  message = '',
  make = '',
  model = '',
  fullModel = '',
  citySlug = '',
} = {}) => {
  const noise = getNoiseTokens({ citySlug });
  const messageTokens = stripMakeModelTokens({
    tokens: tokensFrom(message),
    make,
    model,
    fullModel,
  });

  return messageTokens.filter((token) => !noise.has(token));
};

const residualTokensOnlyBeforeModel = ({
  messageTokens = [],
  residualTokens = [],
  model = '',
  fullModel = '',
} = {}) => {
  if (!messageTokens.length || !residualTokens.length) return false;

  const modelTokenOptions = [
    tokensFrom(model),
    tokensFrom(fullModel),
  ].filter((tokens) => tokens.length);

  const starts = modelTokenOptions
    .map((tokens) => ({
      start: findTokenSequenceStart(messageTokens, tokens),
      length: tokens.length,
    }))
    .filter((match) => match.start >= 0)
    .sort((left, right) => left.start - right.start || right.length - left.length);

  const modelMatch = starts[0] || null;
  if (!modelMatch || modelMatch.start <= 0) return false;

  const modelEnd = modelMatch.start + modelMatch.length - 1;

  return residualTokens.every((token) => {
    const indexes = messageTokens
      .map((messageToken, index) => (messageToken === token ? index : -1))
      .filter((index) => index >= 0);

    return indexes.length > 0 && indexes.every((index) => index < modelMatch.start || index <= modelEnd);
  });
};

const extractRequestedVariantLabel = ({ message = '', residualTokens = [] } = {}) => {
  const parenthetical = String(message || '').match(/\b[A-Za-z0-9]+(?:\s*)\([^)]+\)/);
  if (parenthetical?.[0]) return cleanText(parenthetical[0]);

  return residualTokens.join(' ').toUpperCase();
};

const hasParentheticalVariantSyntax = (message = '') =>
  /\b[A-Za-z0-9]+(?:\s*)\([^)]+\)/.test(String(message || ''));

const normalizeVariantDoc = (doc = {}) => {
  const make = doc.make || doc.brand || '';
  const model = doc.model || doc.fullModel || '';
  const fullModel = doc.fullModel || [make, doc.model].filter(Boolean).join(' ');
  const variant = doc.variant || doc.fullVariant || '';
  const fullVariant = doc.fullVariant || [make, doc.model, variant].filter(Boolean).join(' ');
  const variantKey = doc.variantKey || slugKey(variant);
  const modelKey = doc.modelKey || slugKey(fullModel || model);
  const variantTokens = stripMakeModelTokens({
    tokens: tokensFrom(variant),
    make,
    model,
    fullModel,
  });
  const fullVariantTokens = stripMakeModelTokens({
    tokens: tokensFrom(fullVariant),
    make,
    model,
    fullModel,
  });

  return {
    make,
    model,
    fullModel,
    variant,
    fullVariant,
    modelKey,
    variantKey,
    displayName: fullVariant,
    citySlug: doc.citySlug || null,
    variantTokens: variantTokens.length ? variantTokens : fullVariantTokens,
    normalizedVariant: normalizeSearchText(variant),
    normalizedFullVariant: normalizeSearchText(fullVariant),
  };
};

const getDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};

const loadModelScopedVariants = async ({
  make = '',
  model = '',
  modelKey = '',
  fullModel = '',
  citySlug = DEFAULT_CITY_SLUG,
} = {}) => {
  const db = getDb();
  if (!db) return [];

  const scopeKey = [
    slugKey(make),
    slugKey(modelKey || model || fullModel),
    slugKey(citySlug || DEFAULT_CITY_SLUG),
  ].join('|');
  const now = Date.now();

  if (
    variantCatalogCache.byScope.has(scopeKey) &&
    now - variantCatalogCache.builtAt < CACHE_TTL_MS
  ) {
    return variantCatalogCache.byScope.get(scopeKey);
  }

  const modelKeys = [
    modelKey,
    slugKey(model),
    slugKey(fullModel),
  ].filter(Boolean);
  const makeKey = slugKey(make);
  const modelText = cleanText(model || fullModel);
  const fullModelText = cleanText(fullModel || [make, model].filter(Boolean).join(' '));

  const baseOr = [
    ...modelKeys.map((key) => ({ modelKey: key })),
    ...(modelText ? [{ model: new RegExp(`^${modelText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }] : []),
    ...(fullModelText ? [{ fullModel: new RegExp(`^${fullModelText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }] : []),
  ];

  const baseQuery = {
    ...(baseOr.length ? { $or: baseOr } : {}),
    ...(makeKey ? { makeKey } : {}),
  };

  const projection = {
    _id: 0,
    make: 1,
    brand: 1,
    model: 1,
    fullModel: 1,
    modelKey: 1,
    makeKey: 1,
    variant: 1,
    fullVariant: 1,
    variantKey: 1,
    citySlug: 1,
  };

  let docs = await db
    .collection(PRICE_ROWS_COLLECTION)
    .find({ ...baseQuery, citySlug: citySlug || DEFAULT_CITY_SLUG })
    .project(projection)
    .limit(300)
    .toArray();

  if (!docs.length) {
    docs = await db
      .collection(PRICE_ROWS_COLLECTION)
      .find(baseQuery)
      .project(projection)
      .limit(300)
      .toArray();
  }

  const variants = uniqueBy(
    docs.map(normalizeVariantDoc).filter((item) => item.modelKey && item.variantKey && item.variantTokens.length),
    (item) => `${item.modelKey}:${item.variantKey}`,
  );

  variantCatalogCache = {
    builtAt: now,
    byScope: new Map(variantCatalogCache.byScope).set(scopeKey, variants),
  };

  return variants;
};

const scoreVariantCandidate = ({
  candidate = {},
  messageTokens = [],
  residualTokens = [],
} = {}) => {
  const variantTokens = candidate.variantTokens || tokensFrom(candidate.variant);
  if (!variantTokens.length) return null;

  const residualPhrase = residualTokens.join(' ');
  const variantPhrase = variantTokens.join(' ');
  const exactResidual = Boolean(residualPhrase && residualPhrase === variantPhrase);
  const exactShortPhrase = hasTokenSequence(messageTokens, variantTokens);
  const exactFullPhrase = hasTokenSequence(messageTokens, tokensFrom(candidate.fullVariant));
  const contiguousResidual = hasTokenSequence(residualTokens, variantTokens);
  const covered = countCoveredTokens(residualTokens.length ? residualTokens : messageTokens, variantTokens);
  const coverageRatio = covered / variantTokens.length;
  const residualCoverageRatio = residualTokens.length
    ? countCoveredTokens(variantTokens, residualTokens) / residualTokens.length
    : 0;

  if (!exactResidual && !exactShortPhrase && !exactFullPhrase && coverageRatio < 1) {
    return null;
  }

  let score = 0;

  if (exactResidual) score += 120;
  if (exactFullPhrase) score += 110;
  if (exactShortPhrase) score += 95;
  if (contiguousResidual) score += 28;
  score += coverageRatio * 42;
  score += residualCoverageRatio * 22;
  score += Math.min(12, variantTokens.length * 2);

  if (residualTokens.length && !exactResidual) {
    const extraTokens = residualTokens.filter((token) => !variantTokens.includes(token));
    score -= extraTokens.length * 30;
  }

  return {
    ...candidate,
    score,
    matchedBy: exactResidual
      ? 'exact_residual_phrase'
      : exactFullPhrase
        ? 'exact_full_variant_phrase'
        : exactShortPhrase
          ? 'exact_short_variant_phrase'
          : contiguousResidual
            ? 'contiguous_residual_phrase'
            : 'token_coverage',
    coverageRatio,
    residualCoverageRatio,
  };
};

async function resolveModelScopedVariantFromMessage({
  message = '',
  make = '',
  model = '',
  fullModel = '',
  modelKey = '',
  citySlug = DEFAULT_CITY_SLUG,
} = {}) {
  const cleanMessage = cleanText(message);
  const cleanModel = cleanText(model || fullModel);

  if (!cleanMessage || !cleanModel) {
    return {
      status: 'no_model_or_message',
      selected: null,
      candidates: [],
    };
  }

  const variants = await loadModelScopedVariants({
    make,
    model,
    fullModel,
    modelKey,
    citySlug,
  });

  if (!variants.length) {
    return {
      status: 'no_catalog',
      selected: null,
      candidates: [],
    };
  }

  const messageTokens = tokensFrom(cleanMessage);
  const residualTokens = extractExplicitVariantTokens({
    message: cleanMessage,
    make,
    model,
    fullModel,
    citySlug,
  });

  if (!residualTokens.length) {
    return {
      status: 'no_explicit_variant',
      selected: null,
      candidates: [],
      residualTokens,
    };
  }

  const scored = variants
    .map((candidate) => scoreVariantCandidate({
      candidate,
      messageTokens,
      residualTokens,
    }))
    .filter(Boolean)
    .sort((left, right) =>
      (right.score - left.score) ||
      (right.variantTokens.length - left.variantTokens.length) ||
      left.variant.localeCompare(right.variant),
    );

  const top = scored[0] || null;
  const second = scored[1] || null;
  const requestedVariantText = extractRequestedVariantLabel({
    message: cleanMessage,
    residualTokens,
  });

  const residualPhrase = residualTokens.join(' ');
  const hasExactResidualVariant = variants.some((candidate) =>
    candidate.variantTokens.join(' ') === residualPhrase ||
    normalizeSearchText(candidate.variant) === residualPhrase,
  );
  const parentheticalRequest = hasParentheticalVariantSyntax(cleanMessage);

  if (parentheticalRequest && !hasExactResidualVariant) {
    return {
      status: 'exact_unavailable',
      reason: 'parenthetical_variant_not_found',
      requestedVariantText,
      selected: null,
      candidates: scored.slice(0, 6),
      residualTokens,
    };
  }

  if (!top || top.score < 120) {
    const hasGenericFilterToken = residualTokens.some((token) => GENERIC_FILTER_TOKENS.has(token));
    const onlyBeforeModel = residualTokensOnlyBeforeModel({
      messageTokens,
      residualTokens,
      model: cleanModel,
      fullModel,
    });

    return {
      status: top || hasGenericFilterToken || onlyBeforeModel ? 'below_threshold' : 'exact_unavailable',
      reason: top ? 'highest_score_below_threshold' : 'no_candidate_scored',
      requestedVariantText,
      selected: null,
      candidates: scored.slice(0, 6),
      residualTokens,
    };
  }

  if (second && top.score - second.score <= 8 && top.variantKey !== second.variantKey) {
    return {
      status: 'ambiguous',
      reason: 'close_scored_candidates',
      requestedVariantText,
      selected: null,
      candidates: scored.slice(0, 6),
      residualTokens,
    };
  }

  return {
    status: 'exact',
    selected: top,
    candidates: scored.slice(0, 6),
    residualTokens,
    requestedVariantText: top.variant,
  };
}

function clearModelScopedVariantResolverCache() {
  variantCatalogCache = {
    builtAt: 0,
    byScope: new Map(),
  };
}

export {
  clearModelScopedVariantResolverCache,
  loadModelScopedVariants,
  normalizeSearchText,
  resolveModelScopedVariantFromMessage,
  slugKey,
};

export default resolveModelScopedVariantFromMessage;
