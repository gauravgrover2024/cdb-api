'use strict';

const TONE = 'clear, practical, premium, not salesy';

const action = ({ id = '', label = '', query = '', intent = '' } = {}) => ({
  id,
  label,
  query,
  intent,
});

export const ACI_NEXT_ACTION_PROMPTS = Object.freeze({
  price: [
    action({ id: 'price-variants', label: 'Compare variants', query: 'Compare {{model}} variants', intent: 'vehicle_compare' }),
    action({ id: 'price-emi', label: 'Check EMI', query: '{{model}} EMI', intent: 'vehicle_emi' }),
    action({ id: 'price-offers', label: 'Check offers', query: '{{model}} offers', intent: 'vehicle_offers' }),
    action({ id: 'price-quote', label: 'Get quote', query: 'Get quote for {{model}}', intent: 'aci_new_car_quotation' }),
  ],
  feature: [
    action({ id: 'feature-without', label: 'Variants with/without', query: '{{model}} variants with {{topic}}', intent: 'vehicle_feature_lookup' }),
    action({ id: 'feature-compare', label: 'Compare variants', query: 'Compare {{model}} variants on {{topic}}', intent: 'vehicle_compare' }),
    action({ id: 'feature-price', label: 'Check price', query: '{{model}} price', intent: 'vehicle_pricelist' }),
  ],
  spec: [
    action({ id: 'spec-variants', label: 'Compare variants', query: 'Compare {{model}} variants', intent: 'vehicle_compare' }),
    action({ id: 'spec-compare', label: 'Compare with rival', query: 'Compare {{model}} with a rival', intent: 'vehicle_compare' }),
    action({ id: 'spec-price', label: 'Check price', query: '{{model}} price', intent: 'vehicle_pricelist' }),
    action({ id: 'spec-battery', label: 'Battery and charging', query: '{{model}} battery and charging', intent: 'vehicle_spec_attribute_lookup' }),
  ],
  comparison: [
    action({ id: 'comparison-value', label: 'Value view', query: '{{comparisonLabel}} value comparison', intent: 'vehicle_compare' }),
    action({ id: 'comparison-features', label: 'Feature differences', query: '{{comparisonLabel}} feature comparison', intent: 'vehicle_compare' }),
    action({ id: 'comparison-safety', label: 'Safety view', query: '{{comparisonLabel}} safety comparison', intent: 'vehicle_compare' }),
    action({ id: 'comparison-family', label: 'Family use', query: '{{comparisonLabel}} family use comparison', intent: 'vehicle_compare' }),
  ],
  unsupported_city: [
    action({ id: 'unsupported-supported-city', label: 'Use supported city', query: '{{model}} price in {{firstSupportedCity}}', intent: 'vehicle_pricelist' }),
    action({ id: 'unsupported-request-data', label: 'Request city data', query: 'Notify me when {{city}} pricing is available', intent: 'aci_lead_capture' }),
    action({ id: 'unsupported-compare-specs', label: 'Compare without price', query: 'Compare {{model}} specs', intent: 'vehicle_compare' }),
  ],
});

export const ACI_ANSWER_LANGUAGE_REGISTRY = Object.freeze({
  resolved_feature_available_summary: {
    key: 'resolved_feature_available_summary',
    purpose: 'Summarize whether a known model offers a requested feature across current variants.',
    requiredInputs: ['model', 'topic', 'availableCount', 'totalCount', 'missingCount'],
    tone: TONE,
    guardrails: ['Interpolate counts only from structured feature rows.', 'Do not infer feature value beyond the provided rows.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'feature_available_a', text: '{{model}} offers {{topic}} on {{availableCount}} of {{totalCount}} current variants. {{missingCount}} {{missingVariantWord}} skip it.' },
      { id: 'feature_available_b', text: 'For {{model}}, {{topic}} is present on {{availableCount}}/{{totalCount}} current variants. {{missingCount}} {{missingVariantWord}} do not list it.' },
      { id: 'feature_available_c', text: 'I found {{topic}} on {{availableCount}} of {{totalCount}} current {{model}} variants. {{missingCount}} {{missingVariantWord}} are shown without it.' },
      { id: 'feature_available_d', text: '{{model}} has {{topic}} in {{availableCount}} current variants out of {{totalCount}} checked. {{missingCount}} {{missingVariantWord}} skip it.' },
    ],
  },
  resolved_spec_value_summary: {
    key: 'resolved_spec_value_summary',
    purpose: 'Answer a known model and known spec/topic when structured values exist.',
    requiredInputs: ['model', 'topic', 'values'],
    tone: TONE,
    guardrails: ['Use only values supplied by the resolver.', 'Do not add verification or provenance claims unless passed as input.'],
    forbiddenWording: ['indexed spec value', 'clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'spec_value_a', text: 'I found {{model}}. For {{topic}}, current data lists {{values}}.' },
      { id: 'spec_value_b', text: '{{model}} is the car I found. The listed {{topic}} value is {{values}}.' },
      { id: 'spec_value_c', text: 'For {{model}}, {{topic}} is shown as {{values}} in the current data.' },
      { id: 'spec_value_d', text: 'On {{model}}, the available {{topic}} entry reads {{values}}.' },
    ],
  },
  resolved_spec_missing_summary: {
    key: 'resolved_spec_missing_summary',
    purpose: 'Answer a known model and topic when exact structured spec data is missing.',
    requiredInputs: ['model', 'topic'],
    tone: TONE,
    guardrails: ['Be honest about missing exact data.', 'Do not invent a value.'],
    forbiddenWording: ['indexed spec value', 'not available', 'clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'spec_missing_a', text: 'I found {{model}}. You are asking about {{topic}}, but I do not have the exact certified value in current data yet, so I will not guess.' },
      { id: 'spec_missing_b', text: '{{model}} is clear; the {{topic}} figure is not present in the current structured data yet, so I will keep that answer open.' },
      { id: 'spec_missing_c', text: 'I found {{model}} for this query. The exact {{topic}} value is missing from the current data, so I will not make one up.' },
      { id: 'spec_missing_d', text: 'For {{model}}, I do not have a confirmed {{topic}} value in current data yet. I can still help compare variants or related specs.' },
    ],
  },
  comparison_summary: {
    key: 'comparison_summary',
    purpose: 'Summarize a resolved two-vehicle comparison without claiming a final winner.',
    requiredInputs: ['vehicleA', 'vehicleB', 'priceLine', 'differenceLine'],
    tone: TONE,
    guardrails: ['Avoid final recommendation unless structured decision mode supports it.', 'Include both compared vehicles.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'comparison_a', text: 'I compared {{vehicleA}} and {{vehicleB}}. {{priceLine}} {{differenceLine}}' },
      { id: 'comparison_b', text: 'Here is the {{vehicleA}} vs {{vehicleB}} comparison. {{priceLine}} {{differenceLine}}' },
      { id: 'comparison_c', text: '{{vehicleA}} and {{vehicleB}} are compared on the available price and feature/spec data. {{priceLine}} {{differenceLine}}' },
      { id: 'comparison_d', text: 'For {{vehicleA}} against {{vehicleB}}, I found the comparison data. {{priceLine}} {{differenceLine}}' },
    ],
  },
  price_summary: {
    key: 'price_summary',
    purpose: 'Summarize one model/variant price answer for a structured supported city result.',
    requiredInputs: ['model', 'city', 'priceLine'],
    tone: TONE,
    guardrails: ['Use only structured price labels.', 'Do not silently change cities.'],
    forbiddenWording: ['hidden Delhi fallback', 'clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'price_a', text: 'For {{model}} in {{city}}, {{priceLine}}' },
      { id: 'price_b', text: '{{model}} pricing for {{city}} is available. {{priceLine}}' },
      { id: 'price_c', text: 'I found the {{city}} price for {{model}}. {{priceLine}}' },
      { id: 'price_d', text: 'In {{city}}, {{model}} is listed with this pricing: {{priceLine}}' },
    ],
  },
  pricelist_summary: {
    key: 'pricelist_summary',
    purpose: 'Summarize a model-level price list result.',
    requiredInputs: ['model', 'city', 'variantCount'],
    tone: TONE,
    guardrails: ['Use structured row count and city only.', 'Do not assume a default city when the tool marks a requested city unsupported.'],
    forbiddenWording: ['hidden Delhi fallback', 'clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'pricelist_a', text: 'I found {{variantCount}} {{model}} variants in {{city}}. Default on-road prices exclude optional add-ons; optional add-on totals are available in each variant breakup.' },
      { id: 'pricelist_b', text: 'For {{model}} in {{city}}, {{variantCount}} current variants have price rows. Optional add-ons stay separate from the default on-road figure.' },
      { id: 'pricelist_c', text: '{{city}} pricing is available for {{variantCount}} {{model}} variants. The default on-road view keeps optional accessories separate.' },
      { id: 'pricelist_d', text: 'I found the {{model}} price list for {{city}} with {{variantCount}} variants. Optional add-ons are shown separately where available.' },
    ],
  },
  unsupported_city_price: {
    key: 'unsupported_city_price',
    purpose: 'Explain that requested city pricing is unsupported without falling back to another city.',
    requiredInputs: ['city', 'supportedCities'],
    tone: TONE,
    guardrails: ['Mention the unsupported city honestly.', 'Do not show Delhi/New Delhi as if it were the requested city.'],
    forbiddenWording: ['New Delhi price', 'Delhi on-road', 'clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'unsupported_city_a', text: 'I do not have live on-road pricing for {{city}} yet. Pricing is currently available for {{supportedCities}}.' },
      { id: 'unsupported_city_b', text: '{{city}} pricing is not supported in current data yet. I can show prices for {{supportedCities}}, or compare specs without city pricing.' },
      { id: 'unsupported_city_c', text: 'I cannot quote {{city}} on-road prices from current data yet. Supported pricing cities are {{supportedCities}}.' },
      { id: 'unsupported_city_d', text: 'For {{city}}, live price rows are not available yet. The supported city price data currently covers {{supportedCities}}.' },
    ],
  },
  clarification_known_model_missing_topic: {
    key: 'clarification_known_model_missing_topic',
    purpose: 'Ask for a task/topic when the model is known but the user has not asked a resolvable question.',
    requiredInputs: ['model'],
    tone: TONE,
    guardrails: ['Acknowledge the model.', 'Ask for one concise next detail.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'clarify_model_a', text: 'I found {{model}}. What would you like to check: price, variants, features, range/specs, EMI, or a comparison?' },
      { id: 'clarify_model_b', text: '{{model}} is clear. Tell me the next angle: price, features, specs, variants, EMI, or comparison.' },
      { id: 'clarify_model_c', text: 'I have {{model}}. Which detail should I pull up: price, variant list, feature availability, specs, EMI, or compare it?' },
      { id: 'clarify_model_d', text: '{{model}} is selected. Share the topic you want next, such as price, features, specs, EMI, or comparison.' },
    ],
  },
  clarification_known_topic_missing_model: {
    key: 'clarification_known_topic_missing_model',
    purpose: 'Ask for a model when the topic is known but the car is missing.',
    requiredInputs: ['topic'],
    tone: TONE,
    guardrails: ['Acknowledge the topic.', 'Ask for a car/model.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'clarify_topic_a', text: '{{topic}} is the topic. Which car or model should I check?' },
      { id: 'clarify_topic_b', text: 'I can check {{topic}} once you tell me the model.' },
      { id: 'clarify_topic_c', text: 'Tell me the car name and I will check {{topic}} for it.' },
      { id: 'clarify_topic_d', text: 'Which model should I use for the {{topic}} answer?' },
    ],
  },
  comparison_followup_context_ack: {
    key: 'comparison_followup_context_ack',
    purpose: 'Acknowledge a comparison follow-up without inventing a final recommendation.',
    requiredInputs: ['vehicleA', 'vehicleB'],
    tone: TONE,
    guardrails: ['Continue the comparison context.', 'Do not declare a final winner without priorities.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'comparison_followup_a', text: 'I am continuing the {{vehicleA}} vs {{vehicleB}} comparison. Share your priority, such as value, features, safety, mileage, or family use, and I can narrow it responsibly.' },
      { id: 'comparison_followup_b', text: '{{vehicleA}} and {{vehicleB}} are still the comparison set. I can help choose once you tell me what matters most.' },
      { id: 'comparison_followup_c', text: 'Continuing with {{vehicleA}} vs {{vehicleB}}. I will avoid a final pick until your priority is clear.' },
      { id: 'comparison_followup_d', text: 'For this {{vehicleA}} vs {{vehicleB}} follow-up, I can compare the angle you care about next: value, features, safety, running cost, or family use.' },
    ],
  },
  generic_no_data_but_can_help: {
    key: 'generic_no_data_but_can_help',
    purpose: 'Fallback when current data cannot answer exactly but useful next steps remain.',
    requiredInputs: ['topic'],
    tone: TONE,
    guardrails: ['Be honest about the data gap.', 'Offer supported alternatives.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this', 'indexed spec value'],
    variants: [
      { id: 'no_data_a', text: 'I do not have enough current data to answer {{topic}} exactly yet. I can still help with supported prices, features, specs, EMI, or comparisons.' },
      { id: 'no_data_b', text: 'That exact {{topic}} data is not in the current structured set yet. I can help with related price, feature, spec, EMI, or comparison checks.' },
      { id: 'no_data_c', text: 'I cannot confirm {{topic}} from current data yet, so I will not guess. I can still look up nearby supported car details.' },
      { id: 'no_data_d', text: '{{topic}} needs data I do not have yet. I can continue with supported model, price, feature, spec, EMI, or comparison questions.' },
    ],
  },
  next_action_prompts: {
    key: 'next_action_prompts',
    purpose: 'Render concise next-action prompt copy by topic.',
    requiredInputs: ['topic', 'actions'],
    tone: TONE,
    guardrails: ['Keep actions short.', 'Do not add facts or recommendations.'],
    forbiddenWording: ['clear winner', 'best choice', 'buy this'],
    variants: [
      { id: 'next_actions_a', text: 'Useful next steps for {{topic}}: {{actions}}.' },
      { id: 'next_actions_b', text: 'You can continue with {{actions}} for {{topic}}.' },
      { id: 'next_actions_c', text: 'Next, I can help with {{actions}} around {{topic}}.' },
      { id: 'next_actions_d', text: 'For {{topic}}, good follow-ups are {{actions}}.' },
    ],
  },
});

export const getAciLanguageTemplate = (templateKey = '') =>
  ACI_ANSWER_LANGUAGE_REGISTRY[String(templateKey || '')] || null;

export default ACI_ANSWER_LANGUAGE_REGISTRY;
