'use strict';

/**
 * ACI Meaning Frame Contract
 *
 * This is the permanent semantic contract for ACI Intelligence Core.
 * Gemini/parsers should produce this shape.
 * Validators, planners, tools, composers and future modules should consume this shape.
 *
 * Important:
 * - This file must not contain automotive facts.
 * - This file must not hardcode models, variants, prices, colors, features, or availability.
 * - It defines structure only.
 */

const ACI_MESSAGE_TYPES = Object.freeze({
  AUTOMOTIVE_QUERY: 'automotive_query',
  GENERAL_CHAT: 'general_chat',
  UNSUPPORTED: 'unsupported',
  CLARIFICATION_RESPONSE: 'clarification_response',
});

const ACI_DOMAINS = Object.freeze({
  NEW_CAR: 'new_car',
  USED_CAR: 'used_car',
  FINANCE: 'finance',
  INSURANCE: 'insurance',
  OWNERSHIP: 'ownership',
  SERVICE: 'service',
  CHALLAN: 'challan',
  RC: 'rc',
  CRM: 'crm',
  CONTENT: 'content',
  GENERAL: 'general',
});

const ACI_TASKS = Object.freeze({
  VEHICLE_DISCOVERY: 'vehicle_discovery',
  VEHICLE_OVERVIEW: 'vehicle_overview',
  PRICE_LOOKUP: 'price_lookup',
  ON_ROAD_ESTIMATE: 'on_road_estimate',
  EMI_CALCULATION: 'emi_calculation',
  COLOR_LOOKUP: 'color_lookup',
  FEATURE_ANSWER: 'feature_answer',
  FEATURE_DISCOVERY: 'feature_discovery',
  FEATURE_FILTER: 'feature_filter',
  VEHICLE_COMPARISON: 'vehicle_comparison',
  VARIANT_COMPARISON: 'variant_comparison',
  RECOMMENDATION: 'recommendation',
  SIMILAR_VEHICLES: 'similar_vehicles',
  SAFEST_VEHICLES: 'safest_vehicles',
  VALUE_VARIANT: 'value_variant',
  VARIANT_DELTA: 'variant_delta',
  QUOTATION: 'quotation',
  LEAD_CAPTURE: 'lead_capture',
  OFFER_LOOKUP: 'offer_lookup',
  PRICE_BREAKDOWN: 'price_breakdown',
  WAITING_PERIOD: 'waiting_period',
  UPCOMING_LAUNCHES: 'upcoming_launches',
  MODEL_YEAR_COMPARISON: 'model_year_comparison',
  RESALE_VALUE: 'resale_value',
  RUNNING_COST: 'running_cost',
  RELIABILITY_OWNERSHIP: 'reliability_ownership',
  SAFETY_RATING: 'safety_rating',
  RIVALS_ALTERNATIVES: 'rivals_alternatives',
  DEALER_LOCATOR: 'dealer_locator',
  ACCESSORY_LOOKUP: 'accessory_lookup',
  FUEL_TYPE_ADVICE: 'fuel_type_advice',
  WARRANTY_LOOKUP: 'warranty_lookup',
  MULTI_CITY_PRICE: 'multi_city_price',
  NEW_VS_USED: 'new_vs_used',
  INSURANCE_QUOTE: 'insurance_quote',
  FINANCE_ELIGIBILITY: 'finance_eligibility',
  EXCHANGE_VALUATION: 'exchange_valuation',
  CHALLAN_LOOKUP: 'challan_lookup',
  RC_LOOKUP: 'rc_lookup',
  SERVICE_COST: 'service_cost',
  TCO_ESTIMATE: 'tco_estimate',
  CONTENT_EXPLAINER: 'content_explainer',
  CLARIFICATION: 'clarification',
  UNSUPPORTED: 'unsupported',
});

const ACI_CONTEXT_ACTIONS = Object.freeze({
  USE_EXISTING_CONTEXT: 'use_existing_context',
  SWITCH_TO_EXPLICIT_ENTITY: 'switch_to_explicit_entity',
  REFINE_EXISTING_CONTEXT: 'refine_existing_context',
  CLEAR_CONTEXT: 'clear_context',
  ASK_CLARIFICATION: 'ask_clarification',
});

const ACI_RESULT_GRANULARITY = Object.freeze({
  MODEL: 'model',
  VARIANT: 'variant',
  MODEL_AND_VARIANT: 'model_and_variant',
  VEHICLE_TARGETS: 'vehicle_targets',
  SUMMARY_ONLY: 'summary_only',
});

function createEmptyVehicleAnchor(overrides = {}) {
  return {
    make: null,
    model: null,
    variant: null,
    fullModel: null,
    fullVariant: null,
    bodyType: null,
    fuel: null,
    transmission: null,
    city: null,
    confidence: null,
    source: null,
    ...overrides,
  };
}

function createEmptyMeaningFrame(overrides = {}) {
  return {
    schemaVersion: 'aci.meaningFrame.v1',
    messageType: ACI_MESSAGE_TYPES.AUTOMOTIVE_QUERY,
    domains: [],
    primaryTask: null,
    secondaryTasks: [],
    rawMessage: '',
    normalizedMessage: '',

    anchors: {
      primaryVehicle: createEmptyVehicleAnchor(),
      comparisonTargets: [],
      customer: null,
      location: null,
      channel: null,
    },

    filters: {
      makes: [],
      models: [],
      variants: [],
      bodyTypes: [],
      fuelTypes: [],
      transmissions: [],
      budget: {
        min: null,
        max: null,
        basis: null, // ex_showroom | on_road | emi | unknown
        currency: 'INR',
      },
      features: [],
      colors: [],
      safety: [],
      usage: [],
      ownership: [],
    },

    requestedFacts: {
      price: false,
      onRoad: false,
      emi: false,
      colors: false,
      features: false,
      safety: false,
      offers: false,
      comparison: false,
      recommendation: false,
      quotation: false,
      lead: false,
      insurance: false,
      finance: false,
      exchange: false,
      challan: false,
      rc: false,
      service: false,
      tco: false,
      content: false,
    },

    constraints: {
      mustHaveFeatures: [],
      niceToHaveFeatures: [],
      excludeFeatures: [],
      mustHaveFuelTypes: [],
      mustHaveTransmissions: [],
      maxBudget: null,
      minBudget: null,
      city: null,
      buyerUsage: null,
      timeline: null,
    },

    discovery: {
      isBroadDiscovery: false,
      resultGranularity: ACI_RESULT_GRANULARITY.MODEL_AND_VARIANT,
      sortBy: null,
      limit: null,
    },

    context: {
      action: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT,
      usesPreviousVehicle: false,
      explicitVehicleMentioned: false,
      explicitVariantMentioned: false,
      explicitCityMentioned: false,
      ambiguity: [],
    },

    routing: {
      requiredCapabilities: [],
      requiredProviders: [],
      preferredCanvasType: null,
      toolPlanHint: [],
    },

    clarification: {
      needed: false,
      reason: null,
      question: null,
      options: [],
    },

    confidence: {
      overall: null,
      entityResolution: null,
      taskUnderstanding: null,
      toolReadiness: null,
    },

    safety: {
      shouldRefuse: false,
      refusalReason: null,
      unsupportedReason: null,
      requiresConsent: false,
      consentReason: null,
    },

    trace: {
      parser: null,
      parserVersion: null,
      createdAt: new Date().toISOString(),
    },

    ...overrides,
  };
}

function assertMeaningFrameShape(frame) {
  if (!frame || typeof frame !== 'object') {
    throw new Error('ACI meaning frame must be an object');
  }

  if (frame.schemaVersion !== 'aci.meaningFrame.v1') {
    throw new Error(`Unsupported ACI meaning frame schemaVersion: ${frame.schemaVersion}`);
  }

  if (!frame.messageType) {
    throw new Error('ACI meaning frame missing messageType');
  }

  if (!Object.values(ACI_MESSAGE_TYPES).includes(frame.messageType)) {
    throw new Error(`Invalid ACI meaning frame messageType: ${frame.messageType}`);
  }

  if (frame.primaryTask && !Object.values(ACI_TASKS).includes(frame.primaryTask)) {
    throw new Error(`Invalid ACI meaning frame primaryTask: ${frame.primaryTask}`);
  }

  if (!frame.anchors || typeof frame.anchors !== 'object') {
    throw new Error('ACI meaning frame missing anchors');
  }

  if (!frame.filters || typeof frame.filters !== 'object') {
    throw new Error('ACI meaning frame missing filters');
  }

  if (!frame.requestedFacts || typeof frame.requestedFacts !== 'object') {
    throw new Error('ACI meaning frame missing requestedFacts');
  }

  return true;
}

export {
  ACI_MESSAGE_TYPES,
  ACI_DOMAINS,
  ACI_TASKS,
  ACI_CONTEXT_ACTIONS,
  ACI_RESULT_GRANULARITY,
  createEmptyVehicleAnchor,
  createEmptyMeaningFrame,
  assertMeaningFrameShape,
};
