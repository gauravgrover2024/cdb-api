'use strict';

import {
  ACI_NEXT_ACTION_PROMPTS,
  getAciLanguageTemplate,
} from './aciAnswerLanguageRegistry.js';

const cleanText = (value = '') =>
  String(value ?? '')
    .replace(/\s+/g, ' ')
    .trim();

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [];
  return [value];
};

const hashSeed = (value = '') => {
  const text = cleanText(value);
  if (!text) return 0;

  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const stringifySeedPart = (value) => {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

export const buildAciLanguageSeed = (...parts) =>
  parts.map(stringifySeedPart).filter(Boolean).join('|');

export const selectAciLanguageVariant = ({
  templateKey = '',
  variants = [],
  seed = '',
  previousVariantId = '',
} = {}) => {
  const candidates = asArray(variants).filter((variant) => cleanText(variant?.text));
  if (!candidates.length) return null;

  const seedText = cleanText(seed);
  const hash = seedText ? hashSeed(`${templateKey}|${seedText}`) : 0;
  let selected = candidates[hash % candidates.length] || candidates[0];

  if (
    previousVariantId &&
    candidates.length > 1 &&
    cleanText(selected.id) === cleanText(previousVariantId)
  ) {
    const currentIndex = candidates.findIndex((variant) => variant.id === selected.id);
    selected = candidates[(currentIndex + 1) % candidates.length] || candidates[0];
  }

  return selected;
};

const formatList = (value, fallback = '') => {
  const items = asArray(value).map(cleanText).filter(Boolean);
  if (!items.length) return fallback;
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
};

const formatPlaceholderValue = (value, placeholder = '') => {
  if (Array.isArray(value)) return formatList(value, cleanText(placeholder));
  if (value && typeof value === 'object') {
    return cleanText(value.label || value.text || value.value || value.name || JSON.stringify(value));
  }
  return cleanText(value || placeholder);
};

const buildInputWithDerivedValues = (input = {}) => {
  const normalized = { ...(input || {}) };

  const model = cleanText(normalized.model || normalized.fullModel || normalized.vehicle || '');
  const topic = cleanText(normalized.topic || normalized.feature || normalized.spec || normalized.attribute || '');
  const city = cleanText(normalized.city || normalized.requestedCity || '');
  const supportedCities = formatList(normalized.supportedCities, 'supported cities');
  const values = formatList(normalized.values, cleanText(normalized.value || 'the available value'));
  const actions = formatList(normalized.actions, 'supported next steps');
  const vehicleA = cleanText(normalized.vehicleA || asArray(normalized.vehicles)[0] || '');
  const vehicleB = cleanText(normalized.vehicleB || asArray(normalized.vehicles)[1] || '');
  const comparisonLabel = cleanText(
    normalized.comparisonLabel ||
      (vehicleA && vehicleB ? `${vehicleA} vs ${vehicleB}` : ''),
  );
  const firstSupportedCity = asArray(normalized.supportedCities).map(cleanText).filter(Boolean)[0] || '';
  const missingCount = Number(normalized.missingCount ?? normalized.unavailableCount ?? 0);

  return {
    ...normalized,
    model: model || 'this model',
    topic: topic || 'this topic',
    city: city || 'that city',
    values,
    value: cleanText(normalized.value || values || 'the available value'),
    supportedCities,
    firstSupportedCity: firstSupportedCity || 'a supported city',
    actions,
    vehicleA: vehicleA || 'the first car',
    vehicleB: vehicleB || 'the second car',
    comparisonLabel: comparisonLabel || 'this comparison',
    availableCount: cleanText(normalized.availableCount ?? 0),
    totalCount: cleanText(normalized.totalCount ?? 0),
    missingCount: cleanText(missingCount),
    missingVariantWord: missingCount === 1 ? 'variant' : 'variants',
    variantCount: cleanText(normalized.variantCount ?? normalized.totalVariants ?? 0),
    priceLine: cleanText(normalized.priceLine || 'price data is available.'),
    differenceLine: cleanText(normalized.differenceLine || 'The available comparison table has the details.'),
  };
};

export const renderAciTemplate = (templateKey = '', input = {}, options = {}) => {
  const template = getAciLanguageTemplate(templateKey);
  const variants = asArray(template?.variants);
  const selected = selectAciLanguageVariant({
    templateKey,
    variants,
    seed: options.seed,
    previousVariantId: options.previousVariantId,
  });

  if (!template || !selected) {
    return {
      text: '',
      templateKey,
      variantId: '',
      missingTemplate: true,
    };
  }

  const normalizedInput = buildInputWithDerivedValues(input);
  const text = cleanText(
    selected.text.replace(/{{\s*([a-zA-Z0-9_.-]+)\s*}}/g, (_, key) =>
      formatPlaceholderValue(normalizedInput[key], key),
    ),
  );

  return {
    text,
    templateKey,
    variantId: selected.id || '',
    tone: template.tone || '',
  };
};

const interpolateAction = (value = '', input = {}) =>
  cleanText(value).replace(/{{\s*([a-zA-Z0-9_.-]+)\s*}}/g, (_, key) =>
    formatPlaceholderValue(input[key], key),
  );

export const getAciNextActionPrompts = (topic = '', input = {}) => {
  const key = cleanText(topic).toLowerCase() || 'comparison';
  const templates = ACI_NEXT_ACTION_PROMPTS[key] || ACI_NEXT_ACTION_PROMPTS.comparison || [];
  const normalizedInput = buildInputWithDerivedValues(input);

  return templates.map((item) => ({
    ...item,
    label: interpolateAction(item.label, normalizedInput),
    query: interpolateAction(item.query, normalizedInput),
  }));
};

export {
  ACI_NEXT_ACTION_PROMPTS,
};
