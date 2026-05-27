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

const getComparisonTargets = (meaningFrame = {}) =>
  Array.isArray(meaningFrame.anchors?.comparisonTargets)
    ? meaningFrame.anchors.comparisonTargets.filter(Boolean)
    : [];

const getModels = (meaningFrame = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  const targets = getComparisonTargets(meaningFrame);

  return unique([
    ...(meaningFrame.filters?.models || []),
    primaryVehicle.fullModel,
    primaryVehicle.model,
    ...targets.flatMap((target) => [target.fullModel, target.model]),
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

const getPrimaryModel = (meaningFrame = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  return firstMeaningful(
    primaryVehicle.fullModel,
    primaryVehicle.model,
    firstArrayValue(meaningFrame.filters?.models),
    firstArrayValue(getModels(meaningFrame)),
  );
};

const getPrimaryVariant = (meaningFrame = {}) => {
  const primaryVehicle = getPrimaryVehicle(meaningFrame);
  return firstMeaningful(
    primaryVehicle.fullVariant,
    primaryVehicle.variant,
    firstArrayValue(meaningFrame.filters?.variants),
    firstArrayValue(getVariants(meaningFrame)),
  );
};

const getCity = (meaningFrame = {}, context = {}) =>
  firstMeaningful(
    getPrimaryVehicle(meaningFrame).city,
    meaningFrame.filters?.city,
    context?.selectedVehicle?.city,
    context?.anchorCity,
    context?.city,
    DEFAULT_CITY,
  );

const getMake = (meaningFrame = {}) =>
  firstMeaningful(
    firstArrayValue(meaningFrame.filters?.makes),
    getPrimaryVehicle(meaningFrame).make,
  );

const getFuelType = (meaningFrame = {}) =>
  firstArrayValue(meaningFrame.filters?.fuelTypes);

const getTransmission = (meaningFrame = {}) =>
  firstArrayValue(meaningFrame.filters?.transmissions);

const getBodyType = (meaningFrame = {}) =>
  firstArrayValue(meaningFrame.filters?.bodyTypes);

const getFeatures = (meaningFrame = {}) =>
  unique([
    ...(meaningFrame.filters?.features || []),
    ...(meaningFrame.filters?.safety || []),
  ]);

const getBudgetMax = (meaningFrame = {}) => {
  const max = Number(meaningFrame.filters?.budget?.max);
  return Number.isFinite(max) && max > 0 ? max : undefined;
};

const getBudgetMin = (meaningFrame = {}) => {
  const min = Number(meaningFrame.filters?.budget?.min);
  return Number.isFinite(min) && min > 0 ? min : undefined;
};

const inferTool = (meaningFrame = {}) => {
  const task = cleanText(meaningFrame.primaryTask).toLowerCase();
  const requestedFacts = meaningFrame.requestedFacts || {};
  const hasComparisonTargets = getComparisonTargets(meaningFrame).length >= 2;
  const hasTwoOrMoreModels = getModels(meaningFrame).length >= 2;
  const hasFeatureFilters = getFeatures(meaningFrame).length > 0;
  const isBroadDiscovery = Boolean(meaningFrame.discovery?.isBroadDiscovery);

  if (meaningFrame.safety?.shouldRefuse || task === 'unsupported') {
    return 'unavailable';
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
  if (tool === 'vehicle_recommend') return 'recommendation';
  if (tool === 'vehicle_emi') return 'calculation';
  if (tool === 'aci_lead_capture') return 'lead_capture';
  if (tool === 'vehicle_explainer') return 'education';
  if (meaningFrame.discovery?.isBroadDiscovery) return 'recommendation';
  return 'direct_answer';
};

const inferOutput = (tool = '', meaningFrame = {}) => {
  if (tool === 'vehicle_pricelist') {
    return {
      canvasType: meaningFrame.requestedFacts?.onRoad ? 'price_breakup_canvas' : 'pricelist_canvas',
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

  if (tool === 'vehicle_feature_comparison') {
    return {
      canvasType: 'feature_comparison_canvas',
      inlineType: 'feature_comparison_summary',
      groupBy: 'feature',
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
  if (tool === 'vehicle_recommend') {
    if (getFeatures(meaningFrame).length) return 'feature_match';
    if (getBudgetMax(meaningFrame)) return 'value';
    return 'balanced';
  }

  return null;
};

const buildEntities = (meaningFrame = {}, context = {}) => {
  const models = getModels(meaningFrame);
  const variants = getVariants(meaningFrame);
  const features = getFeatures(meaningFrame);
  const make = getMake(meaningFrame);
  const city = getCity(meaningFrame, context);

  return {
    brand: make,
    make,
    model: getPrimaryModel(meaningFrame),
    models,
    variant: getPrimaryVariant(meaningFrame),
    variants,
    primaryModel: getPrimaryModel(meaningFrame),
    primaryVariant: getPrimaryVariant(meaningFrame),
    comparisonModels: getComparisonTargets(meaningFrame)
      .map((target) => firstMeaningful(target.fullModel, target.model))
      .filter(Boolean),
    comparisonVariants: getComparisonTargets(meaningFrame)
      .map((target) => firstMeaningful(target.fullVariant, target.variant))
      .filter(Boolean),
    city,
    fuelType: getFuelType(meaningFrame),
    transmission: getTransmission(meaningFrame),
    bodyType: getBodyType(meaningFrame),
    feature: firstArrayValue(features),
    features,
    topic: firstArrayValue(features),
    topics: features,
  };
};

const buildFilters = (meaningFrame = {}, context = {}) => {
  const models = getModels(meaningFrame);
  const variants = getVariants(meaningFrame);
  const features = getFeatures(meaningFrame);
  const make = getMake(meaningFrame);
  const city = getCity(meaningFrame, context);

  return {
    brand: make,
    make,
    model: getPrimaryModel(meaningFrame),
    models,
    variant: getPrimaryVariant(meaningFrame),
    variants,
    city,
    budgetMin: getBudgetMin(meaningFrame),
    budgetMax: getBudgetMax(meaningFrame),
    priceBasis: meaningFrame.requestedFacts?.onRoad ? 'on_road' : undefined,
    bodyType: getBodyType(meaningFrame),
    fuelType: getFuelType(meaningFrame),
    transmission: getTransmission(meaningFrame),
    activeOnly: true,
    includeDiscontinued: false,
    mustHaveFeatures: features,
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
  const make = getMake(meaningFrame);
  const model = getPrimaryModel(meaningFrame);
  const variant = getPrimaryVariant(meaningFrame);
  const city = getCity(meaningFrame, context);
  const comparisonTargets = getComparisonTargets(meaningFrame);

  return compactObject({
    anchorBrand: make,
    anchorMake: make,
    anchorModel: model,
    anchorVariant: variant,
    anchorCity: city,
    selectedVehicle: compactObject({
      make,
      brand: make,
      model,
      variant,
      city,
    }),
    selectedComparisonSet: comparisonTargets.length
      ? {
          vehicles: comparisonTargets.map((target) => compactObject({
            make: target.make,
            brand: target.make,
            model: firstMeaningful(target.fullModel, target.model),
            variant: firstMeaningful(target.fullVariant, target.variant),
            fuel: target.fuel,
            transmission: target.transmission,
            city: target.city || city,
          })),
        }
      : {},
    conversationMode: inferConversationMode(inferTool(meaningFrame), meaningFrame),
  });
};

function buildLegacyPlanFromAciMeaningFrame({
  meaningFrame = {},
  context = {},
  message = '',
} = {}) {
  const tool = inferTool(meaningFrame);
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
      variantSelectionMode: getPrimaryVariant(meaningFrame) ? 'exact' : 'not_required',
      selectedModels: getModels(meaningFrame).map((model) => ({ model })),
      selectedVariants: getVariants(meaningFrame).map((variant) => ({ variant })),
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
    contextPatch: buildContextPatch(meaningFrame, context),
    meta: {
      adapter: ADAPTER_VERSION,
      source: 'aci_core_meaning_frame',
      primaryTask: meaningFrame.primaryTask || '',
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
