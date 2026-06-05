'use strict';

import {
  buildAciLanguageSeed,
  getAciNextActionPrompts,
  renderAciTemplate,
} from '../language/aciAnswerLanguageComposer.js';

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

  return getAciNextActionPrompts('spec', {
    model,
    topic: attribute,
  })
    .map((item) =>
      makeAction({
        id: item.id,
        label: /range|battery|charging/i.test(attribute) && item.id === 'spec-battery'
          ? 'Battery and charging'
          : item.label,
        query: /range|battery|charging/i.test(attribute) && item.id === 'spec-battery'
          ? `${model} battery and charging`
          : item.query,
        intent: item.intent,
      }),
    )
    .slice(0, 4);
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
  const valuesForTemplate = extraCount
    ? [`${valueText}; plus ${extraCount} more variant value(s)`]
    : uniqueValues.slice(0, 6);
  const templateKey = valueText && !missingData
    ? 'resolved_spec_value_summary'
    : 'resolved_spec_missing_summary';

  const answer = renderAciTemplate(
    templateKey,
    {
      model,
      topic: attribute,
      values: valuesForTemplate,
    },
    {
      seed: buildAciLanguageSeed(templateKey, model, attribute, valuesForTemplate),
    },
  ).text;

  return {
    answer,
    nextActions: specNextActions({ modelLabel: model, attributeLabel: attribute }),
  };
}

const hasBuyerFriendlyResolvedTopicAnswer = (answer = '') => {
  const text = cleanText(answer).toLowerCase();
  return (
    (
      /\bi found\b/.test(text) ||
      /\bcurrent data lists\b/.test(text) ||
      /\bis shown as\b/.test(text) ||
      /\bis clear\b/.test(text)
    ) &&
    (
      /\bwon't guess\b/.test(text) ||
      /\bwill not guess\b/.test(text) ||
      /\bwill not make one up\b/.test(text) ||
      /\bcurrent data\b/.test(text) ||
      /\bcurrent structured data\b/.test(text)
    )
  );
};

export {
  composeResolvedSpecAnswer,
  hasBuyerFriendlyResolvedTopicAnswer,
  specNextActions,
};

export default composeResolvedSpecAnswer;
