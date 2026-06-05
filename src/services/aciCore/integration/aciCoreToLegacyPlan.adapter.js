'use strict';

/**
 * ACI Core → legacy planner-plan adapter.
 *
 * Non-live bridge:
 * ACI Core meaningFrame
 * → existing aiAgent planner-plan contract
 * → existing aiAgent executor/tools/normalizer path.
 *
 * This file must not contain automotive factual data. It only maps abstract
 * task/result intent to the existing planner tool contract.
 */

const ADAPTER_VERSION = 'aci-core-legacy-plan-adapter-v0.1.0';

import {
  buildContextPatchFromState,
} from '../context/aciContextManager.service.js';
import {
  resolveSpecAttributeFromText,
} from '../specs/aciVehicleSpecAttributeResolver.service.js';

const DEFAULT_CITY = 'new-delhi';

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && cleanText(value) !== '') || '';

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [];
  return [value];
};

const unique = (items = []) => [...new Set(items.filter(Boolean).map(cleanText).filter(Boolean))];

const firstArrayValue = (value = []) => asArray(value).find(Boolean) || '';

const getConfidence = (meaningFrame = {}) => {
  const confidence = Number(meaningFrame.confidence?.overall);
  return Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0.75;
};

const getPrimaryVehicle = (meaningFrame = {}) => meaningFrame.anchors?.primaryVehicle || {};

const getContextState = (context = {}) =>
  context?.contextState || context?.aciContextState || context || {};

const getContextSelectedVehicle = (context = {}) => {
  const state = getContextState(context);
  return context?.selectedVehicle || state?.selectedVehicle || {};
};

const getComparisonTargets = (meaningFrame = {}) =>
  Array.isArray(meaningFrame.anchors?.comparisonTargets)
    ? meaningFrame.anchors.comparisonTargets.filter(Boolean)
    : [];

const getModels = (meaningFrame = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  const targets = getComparisonTargets(meaningFrame);

  return unique([
    ...(meaningFrame.filters?.models || []),
    firstMeaningful(primaryVehicle.model, primaryVehicle.fullModel),
    ...targets.map((target) => firstMeaningful(target.model, target.fullModel)),
  ]);
};

const getVariants = (meaningFrame = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  const targets = getComparisonTargets(meaningFrame);

  return unique([
    ...(meaningFrame.filters?.variants || []),
    primaryVehicle.fullVariant,
    primaryVehicle.variant,
    ...targets.flatMap((target) => [target.fullVariant, target.variant]),
  ]);
};

const getPrimaryModel = (meaningFrame = {}, context = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  const contextVehicle = getContextSelectedVehicle(context);
  return firstMeaningful(
    primaryVehicle.model,
    primaryVehicle.fullModel,
    firstArrayValue(meaningFrame.filters?.models),
    firstArrayValue(getModels(meaningFrame)),
    contextVehicle.model,
    contextVehicle.fullModel,
  );
};

const getPrimaryVariant = (meaningFrame = {}, context = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  const contextVehicle = getContextSelectedVehicle(context);
  return firstMeaningful(
    primaryVehicle.fullVariant,
    primaryVehicle.variant,
    firstArrayValue(meaningFrame.filters?.variants),
    firstArrayValue(getVariants(meaningFrame)),
    contextVehicle.fullVariant,
    contextVehicle.variant,
  );
};

const getCity = (meaningFrame = {}, context = {}) =>
  firstMeaningful(
    getPrimaryVehicle(meaningFrame).city,
    meaningFrame.filters?.city,
    getContextSelectedVehicle(context).city,
    getContextState(context).anchorCity,
    context?.selectedVehicle?.city,
    context?.anchorCity,
    context?.city,
    DEFAULT_CITY,
  );

const getMake = (meaningFrame = {}, context = {}) =>
  firstMeaningful(
    firstArrayValue(meaningFrame.filters?.makes),
    getPrimaryVehicle(meaningFrame).make,
    getContextSelectedVehicle(context).make,
    getContextSelectedVehicle(context).brand,
  );

const getFuelType = (meaningFrame = {}) =>
  firstArrayValue(meaningFrame.filters?.fuelTypes);

const getTransmission = (meaningFrame = {}) =>
  firstArrayValue(meaningFrame.filters?.transmissions);

const getBodyType = (meaningFrame = {}) =>
  firstArrayValue(meaningFrame.filters?.bodyTypes);

const getComparisonVehicles = (meaningFrame = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  const targets = getComparisonTargets(meaningFrame);

  const vehicles = [
    primaryVehicle,
    ...targets,
  ]
    .filter(Boolean)
    .map((vehicle = {}) => compactObject({
      make: vehicle.make,
      brand: vehicle.make,
      model: firstMeaningful(vehicle.model, vehicle.fullModel),
      fullModel: firstMeaningful(vehicle.fullModel, [vehicle.make, vehicle.model].filter(Boolean).join(' '), vehicle.model),
      variant: firstMeaningful(vehicle.fullVariant, vehicle.variant),
      variantName: firstMeaningful(vehicle.fullVariant, vehicle.variant),
      fuel: vehicle.fuel,
      transmission: vehicle.transmission,
      city: vehicle.city,
    }))
    .filter((vehicle) => vehicle.model);

  const seen = new Set();
  return vehicles.filter((vehicle) => {
    const key = cleanText(`${vehicle.model}|${vehicle.variant}`).toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const getFeatures = (meaningFrame = {}) =>
  unique([
    ...(meaningFrame.filters?.features || []),
    ...(meaningFrame.filters?.safety || []),
  ]);

const hasTurboFeature = (meaningFrame = {}) =>
  getFeatures(meaningFrame).some((feature) =>
    /\bturbo\b|\bturbo\s*charger\b|\bturbocharged\b/.test(
      cleanText(String(feature || '').replace(/[_-]+/g, ' ')).toLowerCase(),
    ),
  );

const hasSimilarVehicleIntent = (message = '') =>
  /\b(similar cars?|cars? similar to|similar to|alternatives?|alternative to|cars? like|competitors?|rivals?|cheaper alternatives?|premium alternatives?)\b/i.test(
    message || '',
  );

const primaryTaskForTool = (tool = '', fallback = '') => {
  const normalizedTool = cleanText(tool);
  const normalizedFallback = cleanText(fallback);

  const map = {
    vehicle_feature_lookup: 'feature_answer',
    vehicle_spec_attribute_lookup: 'feature_answer',
    vehicle_pricelist: 'price_lookup',
    vehicle_colors: 'color_lookup',
    vehicle_emi: 'emi_calculation',
    vehicle_compare: 'vehicle_comparison',
    vehicle_recommend: 'vehicle_recommendation',
    vehicle_similar: 'similar_vehicles',
    clarification: 'clarification',
  };

  const mapped = map[normalizedTool] || '';
  if (!mapped) return normalizedFallback || normalizedTool || 'clarification';

  // Critical: if a real executable tool is selected after contextual routing,
  // do not leave the semantic task as clarification.
  if (normalizedTool !== 'clarification' && normalizedFallback === 'clarification') {
    return mapped;
  }

  return normalizedFallback || mapped;
};

const hasActiveComparisonContext = (context = {}) => {
  const state = getContextState(context);
  const comparisonCandidates = [
    state?.activeComparison,
    context?.activeComparison,
    context?.selectedComparisonSet,
  ];

  return comparisonCandidates.some((comparison = {}) =>
    asArray(comparison?.vehicles).filter((vehicle = {}) =>
      cleanText(vehicle.model || vehicle.fullModel || vehicle.modelKey || vehicle.shortModelKey),
    ).length >= 2,
  );
};

const isComparisonFollowUpText = (message = '') =>
  /\b(which one|which is better|which should|better|choose|pick|recommend|verdict|from the comparison|comparison)\b/i.test(
    message || '',
  );

const isContextualFeatureLookupRequest = ({
  rawText = '',
  requestedFacts = {},
  hasFeatureFilters = false,
  task = '',
} = {}) => {
  if (
    requestedFacts.price ||
    requestedFacts.onRoad ||
    requestedFacts.emi ||
    requestedFacts.colors ||
    requestedFacts.comparison
  ) {
    return false;
  }

  return Boolean(
    requestedFacts.features ||
      hasFeatureFilters ||
      task.includes('feature') ||
      /\b(features?|feature list|sunroof|airbags?|adas|abs|cruise|camera|parking camera|ventilated|ventilation|safety features?)\b/i.test(
        rawText || '',
      ),
  );
};

const isUnsupportedCitySamePriceFollowUp = (message = '') =>
  /\bsame\b/i.test(message || '') &&
  /\bin\b/i.test(message || '') &&
  /\b(mumbai|bombay|bangalore|bengaluru|pune|chennai|hyderabad|kolkata|ahmedabad)\b/i.test(
    message || '',
  );

const getSpecAttribute = (meaningFrame = {}, rawMessage = '') =>
  resolveSpecAttributeFromText({
    message: rawMessage,
    features: [
      ...(meaningFrame.filters?.features || []),
      ...(meaningFrame.filters?.safety || []),
      meaningFrame.requestedFacts?.topic,
    ],
  });

const getBudgetMax = (meaningFrame = {}) => {
  const max = Number(meaningFrame.filters?.budget?.max);
  return Number.isFinite(max) && max > 0 ? max : undefined;
};

const getBudgetMin = (meaningFrame = {}) => {
  const min = Number(meaningFrame.filters?.budget?.min);
  return Number.isFinite(min) && min > 0 ? min : undefined;
};

const inferTool = (meaningFrame = {}, rawMessage = '', context = {}) => {
  const task = cleanText(meaningFrame.primaryTask).toLowerCase();
  const requestedFacts = meaningFrame.requestedFacts || {};
  const hasComparisonTargets = getComparisonTargets(meaningFrame).length >= 2;
  const hasTwoOrMoreModels = getModels(meaningFrame).length >= 2;
  const hasFeatureFilters = getFeatures(meaningFrame).length > 0;
  const isBroadDiscovery = Boolean(meaningFrame.discovery?.isBroadDiscovery);
  const specAttribute = getSpecAttribute(meaningFrame, rawMessage);
  const contextVehicle = getContextSelectedVehicle(context);
  const hasVehicleContext = Boolean(
    getPrimaryModel(meaningFrame, context) ||
      getPrimaryVehicle(meaningFrame)?.model ||
      getPrimaryVehicle(meaningFrame)?.fullModel ||
      contextVehicle.model ||
      contextVehicle.fullModel ||
      contextVehicle.modelKey ||
      contextVehicle.shortModelKey ||
      getModels(meaningFrame).length,
  );
  const rawText = cleanText(rawMessage).toLowerCase();
  const hasBroadLanguage = hasSimilarVehicleIntent(rawMessage) || /\b(cars?|vehicles?|models?|options?|suvs?|sedans?|hatchbacks?|mpvs?|muvs?)\b/i.test(rawMessage || '');
  const hasKnownComparisonContext = hasComparisonTargets || hasTwoOrMoreModels || hasActiveComparisonContext(context);

  if (meaningFrame.safety?.shouldRefuse || task === 'unsupported') {
    return 'unavailable';
  }

  if (
    !hasVehicleContext &&
    !hasKnownComparisonContext &&
    !hasBroadLanguage &&
    (
      requestedFacts.price ||
      requestedFacts.onRoad ||
      requestedFacts.emi ||
      requestedFacts.colors ||
      requestedFacts.features ||
      hasFeatureFilters ||
      specAttribute ||
      /\b(price|on road|onroad|emi|colors?|colours?|sunroof|range|automatic|which one|compare these)\b/i.test(rawText)
    )
  ) {
    return 'clarification';
  }

  if (
    (hasSimilarVehicleIntent(rawMessage) ||
      task === 'similar_vehicles' ||
      task === 'rivals_alternatives') &&
    !requestedFacts.price &&
    !requestedFacts.onRoad &&
    !requestedFacts.colors
  ) {
    return 'vehicle_similar';
  }

  if (
    isComparisonFollowUpText(rawMessage) &&
    !hasKnownComparisonContext &&
    !hasComparisonTargets &&
    !hasTwoOrMoreModels
  ) {
    return 'clarification';
  }

  if (hasVehicleContext && isUnsupportedCitySamePriceFollowUp(rawMessage)) {
    return 'vehicle_pricelist';
  }

  if (specAttribute && hasVehicleContext && !requestedFacts.price && !requestedFacts.onRoad) {
    return 'vehicle_spec_attribute_lookup';
  }

  if (
    hasVehicleContext &&
    isContextualFeatureLookupRequest({
      rawText,
      requestedFacts,
      hasFeatureFilters,
      task,
    })
  ) {
    return 'vehicle_feature_lookup';
  }

  const preClarificationScoreIntentText = [
    rawMessage,
    task,
    meaningFrame.primaryTask,
    ...(Array.isArray(meaningFrame.secondaryTasks) ? meaningFrame.secondaryTasks : []),
  ].join(' ').toLowerCase();

  const preClarificationRequestedFactsText = JSON.stringify(requestedFacts || {}).toLowerCase();

  const hasPreClarificationScoreInsightIntent =
    requestedFacts.score ||
    requestedFacts.scores ||
    requestedFacts.value ||
    requestedFacts.regret ||
    requestedFacts.strengths ||
    requestedFacts.weaknesses ||
    /\b(score|scores|rating|ratings|value|worth|worth over|regret|strong|weak|strength|weakness|pros|cons|good value|bad value|gain|gain from|upgrade|worth upgrading|pay extra|extra over|overall|good family|family car|family use|city driving|city use|daily use|most sensible|should i consider)\b/i.test(preClarificationScoreIntentText) ||
    /\b(score|scores|rating|ratings|value|regret|strength|weakness|gain|upgrade|pay extra|extra over)\b/i.test(preClarificationRequestedFactsText);

  const hasPreClarificationVehicleContext =
    Boolean(
      meaningFrame.anchors?.primaryVehicle?.modelKey ||
      meaningFrame.anchors?.primaryVehicle?.model ||
      meaningFrame.anchors?.primaryVehicle?.variantKey ||
      meaningFrame.anchors?.vehicle?.modelKey ||
      meaningFrame.anchors?.vehicle?.model ||
      meaningFrame.anchors?.vehicle?.variantKey ||
      contextVehicle.modelKey ||
      contextVehicle.shortModelKey ||
      contextVehicle.model ||
      contextVehicle.fullModel ||
      contextVehicle.variantKey ||
      asArray(meaningFrame.anchors?.comparisonTargets).length ||
      asArray(meaningFrame.filters?.models).length ||
      asArray(meaningFrame.filters?.variants).length ||
      /\b(alpha|zeta|delta|sigma|smart|sx|s\(o\)|htx|gtx|zx|vx|vxi|zxi|amt|ivt|dct|cvt|automatic|manual|petrol|diesel|cng|ev)\b/i.test(rawMessage || '')
    );

  const hasPreClarificationUpgradePairIntent =
    /\bwhat\s+do\s+i\s+gain\s+from\b/i.test(rawMessage || '') ||
    /\bfrom\s+.+\s+to\s+.+\b/i.test(rawMessage || '') ||
    /\bworth\s+over\b/i.test(rawMessage || '') ||
    /\bpay\s+extra\b/i.test(rawMessage || '') ||
    /\bshould\s+i\s+buy\b.+\bor\b.+/i.test(rawMessage || '');

  const hasPreClarificationModelSummaryIntent =
    /\b(overall|how good|good family car|family car|family use|city driving|city use|daily use|best .* city|most sensible|should i consider|which variant should i consider|model summary)\b/i.test(rawMessage || '') ||
    (/\bgood\b/i.test(rawMessage || '') && /\b(overall|family)\b/i.test(rawMessage || ''));

  if (
    (
      hasPreClarificationModelSummaryIntent ||
      ((hasPreClarificationScoreInsightIntent || hasPreClarificationUpgradePairIntent) &&
        hasPreClarificationVehicleContext)
    ) &&
    !requestedFacts.price &&
    !requestedFacts.onRoad &&
    !requestedFacts.emi &&
    !requestedFacts.colors
  ) {
    return 'vehicle_score_insight';
  }

  if (meaningFrame.clarification?.needed || task === 'clarification') {
    return 'clarification';
  }

  if (requestedFacts.quotation || requestedFacts.lead || task.includes('lead') || task.includes('quotation')) {
    return 'aci_lead_capture';
  }

  if (requestedFacts.emi || task.includes('emi') || task.includes('finance')) {
    return 'vehicle_emi';
  }

  if (requestedFacts.colors || task.includes('color')) {
    return 'vehicle_colors';
  }

  const scoreIntentText = [
    task,
    meaningFrame.primaryTask,
    ...(Array.isArray(meaningFrame.secondaryTasks) ? meaningFrame.secondaryTasks : []),
  ].join(' ').toLowerCase();

  const requestedFactsText = JSON.stringify(requestedFacts || {}).toLowerCase();

  const hasScoreInsightIntent =
    requestedFacts.score ||
    requestedFacts.scores ||
    requestedFacts.value ||
    requestedFacts.regret ||
    requestedFacts.strengths ||
    requestedFacts.weaknesses ||
    /\b(score|scores|rating|ratings|value|worth|worth over|regret|strong|weak|strength|weakness|pros|cons|good value|bad value|gain|gain from|upgrade|worth upgrading|pay extra|extra over|overall|good family|family car|family use|city driving|city use|daily use|most sensible|should i consider)\b/i.test(scoreIntentText) ||
    /\b(score|scores|rating|ratings|value|regret|strength|weakness|gain|upgrade|pay extra|extra over)\b/i.test(requestedFactsText);

  if (
    hasScoreInsightIntent &&
    !requestedFacts.price &&
    !requestedFacts.onRoad &&
    !requestedFacts.emi &&
    !requestedFacts.colors
  ) {
    return 'vehicle_score_insight';
  }


  if (isBroadDiscovery && !hasComparisonTargets && !hasTwoOrMoreModels) {
    return 'vehicle_recommend';
  }

  if (
    (requestedFacts.comparison || hasComparisonTargets || hasTwoOrMoreModels || task.includes('comparison') || task.includes('compare')) &&
    (requestedFacts.features || hasFeatureFilters)
  ) {
    return 'vehicle_feature_comparison';
  }

  if (requestedFacts.comparison || hasComparisonTargets || task.includes('comparison') || task.includes('compare')) {
    return 'vehicle_compare';
  }

  if (isBroadDiscovery || requestedFacts.recommendation || task.includes('recommend') || task.includes('discovery')) {
    return 'vehicle_recommend';
  }

  if (requestedFacts.features || getFeatures(meaningFrame).length || task.includes('feature')) {
    return 'vehicle_feature_lookup';
  }

  if (requestedFacts.price || requestedFacts.onRoad || task.includes('price') || task.includes('on_road')) {
    return 'vehicle_pricelist';
  }

  return 'vehicle_explainer';
};

const inferConversationMode = (tool = '', meaningFrame = {}) => {
  if (tool === 'clarification') return 'clarification';
  if (tool === 'unavailable') return 'unavailable';
  if (tool === 'vehicle_feature_comparison') return 'comparison';
  if (tool === 'vehicle_compare') return 'comparison';
  if (tool === 'vehicle_similar') return 'evaluation';
  if (tool === 'vehicle_score_insight') return 'decision_intelligence';
  if (tool === 'vehicle_recommend') return 'recommendation';
  if (tool === 'vehicle_emi') return 'calculation';
  if (tool === 'vehicle_spec_attribute_lookup') return 'direct_answer';
  if (tool === 'aci_lead_capture') return 'lead_capture';
  if (tool === 'vehicle_explainer') return 'education';
  if (meaningFrame.discovery?.isBroadDiscovery) return 'recommendation';
  return 'direct_answer';
};

const inferOutput = (tool = '', meaningFrame = {}) => {
  if (tool === 'vehicle_pricelist') {
    const wantsPriceBreakup =
      meaningFrame.requestedFacts?.onRoad ||
      meaningFrame.primaryTask === 'price_breakdown' ||
      asArray(meaningFrame.secondaryTasks).includes('price_breakdown');

    return {
      canvasType: wantsPriceBreakup ? 'price_breakup_canvas' : 'pricelist_canvas',
      inlineType: null,
      groupBy: 'variant',
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_colors') {
    return {
      canvasType: 'color_studio_canvas',
      inlineType: null,
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_feature_comparison') {
    return {
      canvasType: 'feature_comparison_canvas',
      inlineType: 'feature_comparison_summary',
      groupBy: 'feature',
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_spec_attribute_lookup') {
    return {
      canvasType: null,
      inlineType: 'spec_attribute_answer_card',
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_feature_discovery') {
    return {
      canvasType: 'feature_match_builder_canvas',
      inlineType: null,
      groupBy: 'model',
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_compare') {
    return {
      canvasType: 'comparison_canvas',
      inlineType: null,
      groupBy: 'variant',
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_recommend') {
    return {
      canvasType: 'recommendation_results_canvas',
      inlineType: null,
      groupBy: 'model',
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_similar') {
    return {
      canvasType: 'similar_cars_canvas',
      inlineType: 'similar_cars_summary',
      groupBy: 'model',
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_score_insight') {
    return {
      canvasType: 'score_insight_canvas',
      inlineType: 'score_insight_summary',
      groupBy: 'variant',
      preferredWidgetType: null,
    };
  }


  if (tool === 'vehicle_emi') {
    return {
      canvasType: 'emi_calculator_canvas',
      inlineType: null,
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  if (tool === 'aci_lead_capture') {
    return {
      canvasType: 'aci_quotation_canvas',
      inlineType: null,
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  if (tool === 'unavailable') {
    return {
      canvasType: 'unavailable_notice_canvas',
      inlineType: 'unavailable_notice',
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  if (tool === 'clarification') {
    return {
      canvasType: null,
      inlineType: 'clarification_card',
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  if (tool === 'vehicle_explainer') {
    return {
      canvasType: 'explainer_canvas',
      inlineType: 'explainer_card',
      groupBy: null,
      preferredWidgetType: null,
    };
  }

  return {
    canvasType: null,
    inlineType: tool === 'vehicle_feature_lookup' ? 'feature_answer_card' : null,
    groupBy: null,
    preferredWidgetType: null,
  };
};

const inferRanking = (tool = '', meaningFrame = {}) => {
  if (tool === 'vehicle_recommend' || tool === 'vehicle_similar') {
    if (getFeatures(meaningFrame).length) return 'feature_match';
    if (tool === 'vehicle_similar') return 'similarity';
    if (getBudgetMax(meaningFrame)) return 'value';
    return 'balanced';
  }

  return null;
};

const isBroadFeatureDiscoveryWithoutModel = (meaningFrame = {}) =>
  Boolean(
    meaningFrame.discovery?.isBroadDiscovery &&
      getFeatures(meaningFrame).length &&
      !getPrimaryModel(meaningFrame),
  );

const buildEntities = (meaningFrame = {}, context = {}) => {
  const models = getModels(meaningFrame);
  const variants = getVariants(meaningFrame);
  const features = getFeatures(meaningFrame);
  const specAttribute = getSpecAttribute(meaningFrame);
  const make = getMake(meaningFrame, context);
  const city = getCity(meaningFrame, context);
  const clearVariantAnchors = isBroadFeatureDiscoveryWithoutModel(meaningFrame);
  const primaryModel = getPrimaryModel(meaningFrame, context);
  const primaryVariant = getPrimaryVariant(meaningFrame, context);

  return {
    brand: make,
    make,
    model: primaryModel,
    models: models.length ? models : asArray(primaryModel),
    variant: clearVariantAnchors ? '' : primaryVariant,
    variants: clearVariantAnchors ? [] : variants.length ? variants : asArray(primaryVariant),
    primaryModel,
    primaryVariant: clearVariantAnchors ? '' : primaryVariant,
    comparisonModels: getComparisonVehicles(meaningFrame)
      .map((target) => firstMeaningful(target.fullModel, target.model))
      .filter(Boolean),
    comparisonVariants: getComparisonVehicles(meaningFrame)
      .map((target) => firstMeaningful(target.variantName, target.variant))
      .filter(Boolean),
    comparisonVehicles: getComparisonVehicles(meaningFrame),
    city,
    fuelType: getFuelType(meaningFrame),
    transmission: getTransmission(meaningFrame),
    bodyType: getBodyType(meaningFrame),
    feature: firstArrayValue(features),
    features,
    attributeKey: specAttribute?.key,
    attributeLabel: specAttribute?.label,
    topic: firstArrayValue(features),
    topics: features,
  };
};

const buildFilters = (meaningFrame = {}, context = {}) => {
  const models = getModels(meaningFrame);
  const variants = getVariants(meaningFrame);
  const features = getFeatures(meaningFrame);
  const specAttribute = getSpecAttribute(meaningFrame);
  const make = getMake(meaningFrame, context);
  const city = getCity(meaningFrame, context);
  const clearVariantAnchors = isBroadFeatureDiscoveryWithoutModel(meaningFrame);
  const primaryModel = getPrimaryModel(meaningFrame, context);
  const primaryVariant = getPrimaryVariant(meaningFrame, context);

  return {
    brand: make,
    make,
    model: primaryModel,
    models: models.length ? models : asArray(primaryModel),
    variant: clearVariantAnchors ? '' : primaryVariant,
    variants: clearVariantAnchors ? [] : variants.length ? variants : asArray(primaryVariant),
    city,
    budgetMin: getBudgetMin(meaningFrame),
    budgetMax: getBudgetMax(meaningFrame),
    priceBasis:
      meaningFrame.requestedFacts?.onRoad ||
      meaningFrame.primaryTask === 'price_breakdown' ||
      asArray(meaningFrame.secondaryTasks).includes('price_breakdown')
        ? 'on_road'
        : undefined,
    bodyType: getBodyType(meaningFrame),
    fuelType: getFuelType(meaningFrame),
    transmission: getTransmission(meaningFrame),
    activeOnly: true,
    includeDiscontinued: false,
    mustHaveFeatures: features,
    attributeKey: specAttribute?.key,
    attributeLabel: specAttribute?.label,
    compareFeatures: meaningFrame.requestedFacts?.comparison ? features : [],
    color: firstArrayValue(meaningFrame.filters?.colors),
  };
};

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === 'object') return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== '';
    }),
  );

const compactTool = (toolPlan = {}) => ({
  ...toolPlan,
  entities: compactObject(toolPlan.entities || {}),
  filters: compactObject(toolPlan.filters || {}),
  output: compactObject(toolPlan.output || {}),
  resolution: compactObject(toolPlan.resolution || {}),
});

const buildContextPatch = (meaningFrame = {}, context = {}) => {
  const make = getMake(meaningFrame, context);
  const model = getPrimaryModel(meaningFrame, context);
  const variant = getPrimaryVariant(meaningFrame, context);
  const city = getCity(meaningFrame, context);
  const tool = inferTool(meaningFrame, '', context);
  const clearVariantAnchors = isBroadFeatureDiscoveryWithoutModel(meaningFrame);
  const safeVariant = clearVariantAnchors ? '' : variant;
  const comparisonVehicles = getComparisonVehicles(meaningFrame).map((vehicle) =>
    compactObject({
      ...vehicle,
      city: vehicle.city || city,
    }),
  );
  const isComparison =
    ["vehicle_compare", "vehicle_feature_comparison"].includes(tool) &&
    comparisonVehicles.length >= 2;

  if (isComparison) {
    return compactObject({
      anchorCity: city,
      activeComparison: {
        type: tool,
        vehicles: comparisonVehicles,
        fuelFilter: getFuelType(meaningFrame),
        features: getFeatures(meaningFrame),
        city,
      },
      selectedComparisonSet: {
        vehicles: comparisonVehicles,
      },
      conversationMode: inferConversationMode(tool, meaningFrame),
    });
  }

  return compactObject({
    anchorBrand: make,
    anchorMake: make,
    anchorModel: model,
    anchorVariant: safeVariant,
    anchorCity: city,
    selectedVehicle: compactObject({
      make,
      brand: make,
      model,
      variant: safeVariant,
      city,
    }),
    conversationMode: inferConversationMode(tool, meaningFrame),
  });
};

function buildLegacyPlanFromAciMeaningFrame({
  meaningFrame = {},
  context = {},
  message = '',
} = {}) {
  const task = cleanText(meaningFrame.primaryTask).toLowerCase();
  const tool = inferTool(meaningFrame, message, context);
  const effectivePrimaryTask = primaryTaskForTool(tool, task);
  const conversationMode = inferConversationMode(tool, meaningFrame);
  const confidence = getConfidence(meaningFrame);
  const output = inferOutput(tool, meaningFrame);

  const toolPlan = compactTool({
    tool,
    entities: buildEntities(meaningFrame, context),
    filters: buildFilters(meaningFrame, context),
    ranking: inferRanking(tool, meaningFrame),
    output,
    resolution: {
      comparisonLevel: tool === 'vehicle_compare' ? 'variant' : null,
      variantSelectionMode: isBroadFeatureDiscoveryWithoutModel(meaningFrame)
        ? 'not_required'
        : getPrimaryVariant(meaningFrame, context) ? 'exact' : 'not_required',
      selectedModels: (getModels(meaningFrame).length
        ? getModels(meaningFrame)
        : asArray(getPrimaryModel(meaningFrame, context)))
        .map((model) => ({ model })),
      selectedVariants: isBroadFeatureDiscoveryWithoutModel(meaningFrame)
        ? []
        : (getVariants(meaningFrame).length
          ? getVariants(meaningFrame)
          : asArray(getPrimaryVariant(meaningFrame, context)))
          .map((variant) => ({ variant })),
      selectedComparisonVehicles: getComparisonVehicles(meaningFrame),
      changeAllowed: true,
      note: 'Generated from ACI Core meaning frame.',
    },
  });

  return {
    mode: tool === 'clarification' ? 'clarification' : tool === 'unavailable' ? 'unavailable' : 'single_tool',
    domain: 'new_car',
    conversationMode,
    customerStage: 'exploration',
    confidence,
    tools: [toolPlan],
    ambiguity: {
      level: meaningFrame.clarification?.needed ? 'ask_user' : 'none',
      type: meaningFrame.clarification?.needed ? 'model' : 'none',
      message: meaningFrame.clarification?.question || '',
      options: meaningFrame.clarification?.options || [],
      selectedDefault: null,
    },
    clarification: meaningFrame.clarification?.question || '',
    nextSteps: [],
    contextPatch: context?.contextState || context?.aciContextState
      ? buildContextPatchFromState(context.contextState || context.aciContextState)
      : buildContextPatch(meaningFrame, context),
    meta: {
      adapter: ADAPTER_VERSION,
      source: 'aci_core_meaning_frame',
      primaryTask: effectivePrimaryTask,
      routeReason: effectivePrimaryTask,
      originalPrimaryTask: meaningFrame.primaryTask || '',
      message,
      confidence: meaningFrame.confidence || {},
      discovery: meaningFrame.discovery || {},
      requestedFacts: meaningFrame.requestedFacts || {},
      safety: meaningFrame.safety || {},
    },
  };
}

export {
  ADAPTER_VERSION,
  buildLegacyPlanFromAciMeaningFrame,
  inferTool,
};

export default buildLegacyPlanFromAciMeaningFrame;
