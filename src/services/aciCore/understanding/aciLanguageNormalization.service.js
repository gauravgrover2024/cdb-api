'use strict';

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const normalizeKey = (value = '') =>
  cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && cleanText(value) !== '') || '';

const contextVehicleLabel = (context = {}) => {
  const vehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    context?.anchors?.primaryVehicle ||
    {};

  return cleanText(
    firstMeaningful(
      vehicle.variant || vehicle.variantName
        ? [
            vehicle.fullModel || [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(' '),
            vehicle.variant || vehicle.variantName,
          ].filter(Boolean).join(' ')
        : '',
      vehicle.fullModel,
      [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(' '),
      vehicle.model,
    ),
  );
};

const cityFromPhrase = (message = '') => {
  const key = normalizeKey(message);
  if (/\bnoida\b/.test(key)) return 'Noida';
  if (/\bgurgaon\b|\bgurugram\b/.test(key)) return 'Gurgaon';
  if (/\bnew delhi\b|\bdelhi\b/.test(key)) return 'Delhi';
  if (/\bmumbai\b|\bbombay\b/.test(key)) return 'Mumbai';
  if (/\bbangalore\b|\bbengaluru\b/.test(key)) return 'Bangalore';
  if (/\bjaipur\b/.test(key)) return 'Jaipur';
  return '';
};

const replacePhrase = (message = '', pattern, replacement = '') =>
  cleanText(String(message || '').replace(pattern, replacement));

function normalizeAciBuyerLanguage({
  message = '',
  context = {},
} = {}) {
  const originalMessage = cleanText(message);
  const key = normalizeKey(originalMessage);
  const vehicle = contextVehicleLabel(context);
  let normalizedMessage = originalMessage;
  const rules = [];

  if (!originalMessage) {
    return {
      originalMessage,
      normalizedMessage,
      changed: false,
      rules,
    };
  }

  if (/\bka price\b/i.test(normalizedMessage)) {
    normalizedMessage = replacePhrase(normalizedMessage, /\bka\s+price\b/gi, 'price');
    rules.push('hinglish_ka_price');
  }

  if (/\bme sunroof hai kya\b/i.test(normalizedMessage)) {
    normalizedMessage = replacePhrase(normalizedMessage, /\bme\s+sunroof\s+hai\s+kya\b/gi, 'sunroof');
    rules.push('hinglish_sunroof_question');
  }

  if (/\bvalue for money hai kya\b/i.test(normalizedMessage)) {
    normalizedMessage = replacePhrase(normalizedMessage, /\bvalue\s+for\s+money\s+hai\s+kya\b/gi, 'good value');
    rules.push('hinglish_value_for_money');
  }

  if (/\bka on road price\b/i.test(normalizedMessage)) {
    normalizedMessage = replacePhrase(normalizedMessage, /\bka\s+on\s+road\s+price\b/gi, 'on-road price');
    rules.push('hinglish_ka_on_road_price');
  }

  if (/\bon road price\b/i.test(normalizedMessage)) {
    normalizedMessage = replacePhrase(normalizedMessage, /\bon\s+road\s+price\b/gi, 'on-road price');
  }

  if (/\bsame noida me batao\b/i.test(normalizedMessage)) {
    normalizedMessage = vehicle ? `${vehicle} price in Noida` : 'same in Noida';
    rules.push('hinglish_same_noida');
  }

  if (/\bmumbai me price batao\b/i.test(normalizedMessage)) {
    normalizedMessage = vehicle ? `${vehicle} price in Mumbai` : 'price in Mumbai';
    rules.push('hinglish_mumbai_price');
  }

  const city = cityFromPhrase(normalizedMessage);
  if (
    vehicle &&
    city &&
    /\b(change city to|same in|now)\b/i.test(normalizedMessage) &&
    !/\b(compare|vs|v\/s|versus)\b/i.test(normalizedMessage)
  ) {
    normalizedMessage = `${vehicle} price in ${city}`;
    rules.push('context_city_follow_up');
  }

  const compareThisMatch = normalizedMessage.match(/\bcompare\s+this\s+with\s+(.+)$/i);
  if (vehicle && compareThisMatch?.[1]) {
    normalizedMessage = `${vehicle} vs ${cleanText(compareThisMatch[1])}`;
    rules.push('context_compare_this_with_target');
  }

  if (vehicle && /\bisme airbags kitne hain\b/i.test(key)) {
    normalizedMessage = `${vehicle} airbags`;
    rules.push('hinglish_this_airbags_with_context');
  }

  if (vehicle && /\biska mileage kya hai\b/i.test(key)) {
    normalizedMessage = `${vehicle} mileage`;
    rules.push('hinglish_this_mileage_with_context');
  }

  if (vehicle && /\btop model worth hai kya\b/i.test(key)) {
    normalizedMessage = `${vehicle} top model worth`;
    rules.push('hinglish_top_model_worth_with_context');
  }

  normalizedMessage = cleanText(normalizedMessage);

  return {
    originalMessage,
    normalizedMessage,
    changed: normalizedMessage !== originalMessage,
    rules,
  };
}

export {
  normalizeAciBuyerLanguage,
};

export default normalizeAciBuyerLanguage;
