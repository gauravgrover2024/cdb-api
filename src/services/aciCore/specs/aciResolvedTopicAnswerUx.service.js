'use strict';

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === 'object') return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== '';
    }),
  );

const makeAction = ({ id = '', label = '', query = '', intent = '' } = {}) =>
  compactObject({
    id,
    label,
    query,
    intent,
    priority: 80,
  });

const specNextActions = ({ modelLabel = '', attributeLabel = '' } = {}) => {
  const model = cleanText(modelLabel) || 'this car';
  const attribute = cleanText(attributeLabel) || 'specs';

  return [
    makeAction({
      id: 'compare-variants',
      label: 'Compare variants',
      query: `Compare ${model} variants`,
      intent: 'vehicle_compare',
    }),
    makeAction({
      id: 'battery-charging-info',
      label: /range|battery|charging/i.test(attribute) ? 'Battery and charging' : 'Related specs',
      query: /range|battery|charging/i.test(attribute)
        ? `${model} battery and charging`
        : `${model} ${attribute}`,
      intent: 'vehicle_spec_attribute_lookup',
    }),
    makeAction({
      id: 'check-price',
      label: 'Check on-road price',
      query: `${model} on road price`,
      intent: 'vehicle_pricelist',
    }),
    makeAction({
      id: 'compare-alternative',
      label: 'Compare with rival',
      query: `Compare ${model} with a rival`,
      intent: 'vehicle_compare',
    }),
  ].slice(0, 4);
};

function composeResolvedSpecAnswer({
  modelLabel = '',
  attributeLabel = '',
  values = [],
  missingData = false,
} = {}) {
  const model = cleanText(modelLabel) || 'this model';
  const attribute = cleanText(attributeLabel) || 'requested specification';
  const formattedValues = values
    .map((item) => {
      const value = cleanText(item?.value || item);
      if (!value) return '';
      const variant = cleanText(item?.variant || '');
      return variant ? `${variant}: ${value}` : value;
    })
    .filter(Boolean);

  const uniqueValues = [...new Set(formattedValues)];
  const valueText = uniqueValues.slice(0, 6).join('; ');
  const extraCount = Math.max(0, uniqueValues.length - 6);

  const answer = valueText && !missingData
    ? `I found ${model}. You're asking about ${attribute}. In the current vehicle data, ${attribute} is listed as ${valueText}${extraCount ? `; plus ${extraCount} more variant value(s)` : ''}. Please verify the exact variant/source before final booking.`
    : `I found ${model}. You're asking about ${attribute}. I don't have the exact certified ${attribute} value in the current spec data yet, so I won't guess.`;

  return {
    answer,
    nextActions: specNextActions({ modelLabel: model, attributeLabel: attribute }),
  };
}

const hasBuyerFriendlyResolvedTopicAnswer = (answer = '') => {
  const text = cleanText(answer).toLowerCase();
  return (
    /\bi found\b/.test(text) &&
    /\byou('|’)re asking about\b/.test(text) &&
    (
      /\bwon't guess\b/.test(text) ||
      /\bcurrent vehicle data\b/.test(text) ||
      /\bcurrent vehicle data\b/.test(text) ||
      /\bcurrent vehicle data\b/.test(text) ||
      /\bcurrent spec data\b/.test(text)
    )
  );
};

export {
  composeResolvedSpecAnswer,
  hasBuyerFriendlyResolvedTopicAnswer,
  specNextActions,
};

export default composeResolvedSpecAnswer;
