import { customer360, customerDataQualityReport, customerLookup, vehicle360 } from "./aiAgent.customerTools.js";
import { latestInsurance, insuranceExpiryReport } from "./aiAgent.insuranceTools.js";
import { loanStatusOrClosure } from "./aiAgent.loanTools.js";
import {
  activeLoanExpiredInsuranceReport,
  deliveryOrderReport,
  loanDisbursalReport,
  loanDisbursedReport,
  loanBusinessReport,
  loanInsuranceMissingReport,
  loanInvoiceMissingReport,
  loanMissingRegistrationReport,
  loanPendingApprovalReport,
  missingRegistrationReport,
  operationsDigest,
  paymentPendingReport,
  payoutEnteredReport,
  payoutMissingReport,
  usedCarRcPendingReport,
} from "./aiAgent.reportTools.js";
import { unavailableWidget } from "./aiAgent.responseBuilders.js";
import {
  priceHistoryReport,
  similarCars,
  latestCatalogueUpdates,
  vehicleColorSearch,
  vehicleColors,
  vehicleComparison,
  vehicleEmiBudgetSearch,
  vehicleEmiCalculator,
  vehicleFeatureAvailability,
  vehicleFeatureDiscovery,
  vehicleFeatures,
  vehiclePriceBreakup,
  vehiclePriceHistory,
  vehiclePricelist,
  vehicleRecommendationSearch,
  vehicleOffersUnavailable,
  vehicleServiceDataUnavailable,
  vehicleFinanceFaqUnavailable,
  buildLeadPayloadFromParsed,
  vehicleVariantDifference,
  vehicleVariantRecommendation,
} from "./aiAgent.vehicleTools.js";
import { getFieldMapForIntent } from "./aiAgent.fieldMaps.js";
import { INTENT_DEFINITIONS } from "./aiAgent.intentRouter.js";
import {
  getNewCarQuestionConfig,
  NEW_CAR_INTENTS,
  resolveToolIntentForQuestionIntent,
} from "./aiAgent.newCarQuestionMap.js";

const placeholderRun = (intent) => async () => ({
  widgets: [
    unavailableWidget(
      "Tool not implemented yet",
      `The deterministic route for ${intent} exists, but its database tool has not been implemented in this phase.`,
      [],
    ),
  ],
});

const newCarUnavailableRun = (intent) => async () => {
  const config = getNewCarQuestionConfig(intent);
  const actions = (config?.defaultActions || []).map((item) => ({
    label: item.label,
    type: item.type,
    query: item.query,
    canvasType: item.canvasType,
    leadType: item.leadType,
    route: item.route,
  }));
  return {
    widgets: [
      unavailableWidget(
        config?.title || "Information currently unavailable",
        `The route for ${intent} is enabled, but this dataset is not available in the current environment.`,
        config?.dataSources || [],
      ),
    ],
    actions,
    leadingQuestions: config?.leadingQuestions || [],
    followUpSuggestions: (config?.defaultActions || [])
      .map((item) => item.query || item.label)
      .filter(Boolean)
      .slice(0, 6),
  };
};

const conversionLeadRun =
  (intent, leadType) =>
  async (parsed = {}, access = null, trace = []) => {
    if (
      leadType === "finance_callback" &&
      /\bbank\b.*\b(best|offer|scheme|rate)\b/i.test(parsed?.lower || "")
    ) {
      return vehicleFinanceFaqUnavailable(parsed, access, trace);
    }
    void access;
    const model =
      parsed?.entities?.model || parsed?.entities?.models?.[0] || "";
    const variant = parsed?.entities?.variant || "";
    const city = parsed?.entities?.city || "new-delhi";
    const modelLabel = [model, variant].filter(Boolean).join(" ");
    const leadMeta = await buildLeadPayloadFromParsed(parsed, trace, leadType);

    const titleByLeadType = {
      quotation: "ACI quotation request",
      test_drive: "Test drive request",
      callback: "Callback request",
      finance_callback: "Finance callback request",
    };

    const messageByLeadType = {
      quotation: modelLabel
        ? `I can prepare an ACI quotation for ${modelLabel}.`
        : "I can prepare an ACI quotation once you choose the model and variant.",
      test_drive: modelLabel
        ? `I can help book a test drive for ${modelLabel}.`
        : "I can help book a test drive once you choose the model.",
      callback: modelLabel
        ? `I can arrange a callback for ${modelLabel}.`
        : "I can arrange a callback from an advisor.",
      finance_callback: modelLabel
        ? `I can arrange a finance callback for ${modelLabel}.`
        : "I can arrange a finance callback from an advisor.",
    };

    return {
      widgets: [
        {
          type: "lead_capture",
          title: titleByLeadType[leadType] || "Lead request",
          data: {
            leadType,
            intent,
            model,
            variant,
            city,
            modelLabel,
            leadPayload: leadMeta.leadPayload,
            leadCaptureRequired: leadMeta.leadCaptureRequired,
            fieldsRequired: leadMeta.fieldsRequired,
            message: messageByLeadType[leadType] || "I can help you proceed.",
            requiredFields: leadMeta.fieldsRequired?.length
              ? leadMeta.fieldsRequired
              : ["customerName", "mobile"],
            optionalFields: [
              "city",
              "preferredTime",
              "exchangeCar",
              "financeRequired",
            ],
          },
          canvasType: "aci_quotation_canvas",
        },
      ],
      actions: [
        {
          label: "Continue",
          type: "lead",
          leadType,
          intent,
          canvasType: "aci_quotation_canvas",
          query: modelLabel
            ? `Continue ${leadType} for ${modelLabel}`
            : "Continue",
          contextPatch: {
            leadType,
            leadPayload: leadMeta.leadPayload,
            leadCaptureRequired: leadMeta.leadCaptureRequired,
            fieldsRequired: leadMeta.fieldsRequired,
          },
        },
      ],
      followUpSuggestions: [
        modelLabel ? `Calculate EMI for ${modelLabel}` : "Calculate EMI",
        modelLabel
          ? `Show price breakup for ${modelLabel}`
          : "Show price breakup",
        modelLabel ? `Book test drive for ${modelLabel}` : "Book test drive",
      ],
    };
  };

const handlerByIntent = {
  latest_insurance: latestInsurance,
  insurance_expiry_report: insuranceExpiryReport,
  loan_status: loanStatusOrClosure,
  loan_closure: loanStatusOrClosure,
  loan_closure_pos: loanStatusOrClosure,
  loan_disbursal_report: loanDisbursalReport,
  loan_pending_approval_report: loanPendingApprovalReport,
  loan_disbursed_report: loanDisbursedReport,
  loan_business_report: loanBusinessReport,
  loan_missing_registration_report: loanMissingRegistrationReport,
  loan_invoice_missing_report: loanInvoiceMissingReport,
  loan_insurance_missing_report_basic: loanInsuranceMissingReport,
  missing_registration_report: missingRegistrationReport,
  payout_missing_report: payoutMissingReport,
  payout_entered_report: payoutEnteredReport,
  payment_pending_report: paymentPendingReport,
  delivery_order_report: deliveryOrderReport,
  vehicle_pricelist: vehiclePricelist,
  vehicle_city_change: vehiclePricelist,
  vehicle_price_breakup: vehiclePriceBreakup,
  vehicle_colors: vehicleColors,
  vehicle_color_search: vehicleColorSearch,
  vehicle_features: vehicleFeatures,
  vehicle_feature_answer: vehicleFeatureAvailability,
  vehicle_feature_discovery: vehicleFeatureDiscovery,
  similar_cars: similarCars,
  vehicle_comparison: vehicleComparison,
  vehicle_budget_search: vehicleRecommendationSearch,
  vehicle_use_case_recommendation: vehicleRecommendationSearch,
  vehicle_body_type_search: vehicleRecommendationSearch,
  vehicle_fuel_transmission_search: vehicleRecommendationSearch,
  vehicle_dimension_space_search: vehicleRecommendationSearch,
  vehicle_performance_mileage_search: vehicleRecommendationSearch,
  vehicle_safety_expert: vehicleRecommendationSearch,
  vehicle_emi_calculator: vehicleEmiCalculator,
  vehicle_emi_budget_search: vehicleEmiBudgetSearch,
  vehicle_price_history: vehiclePriceHistory,
  vehicle_launch_status: latestCatalogueUpdates,
  vehicle_best_variant_recommendation: vehicleVariantRecommendation,
  vehicle_variant_difference: vehicleVariantDifference,
  aci_new_car_quotation: conversionLeadRun(
    "aci_new_car_quotation",
    "quotation",
  ),
  vehicle_test_drive_request: conversionLeadRun(
    "vehicle_test_drive_request",
    "test_drive",
  ),
  vehicle_callback_request: conversionLeadRun(
    "vehicle_callback_request",
    "callback",
  ),
  new_car_loan_enquiry: conversionLeadRun(
    "new_car_loan_enquiry",
    "finance_callback",
  ),
  price_history_report: priceHistoryReport,
  active_loan_expired_insurance_report: activeLoanExpiredInsuranceReport,
  operations_digest: operationsDigest,
  data_quality_workbench: missingRegistrationReport,
  finance_intelligence: payoutMissingReport,
  customer_lookup: customerLookup,
  customer_data_quality_report: customerDataQualityReport,
  customer_360: customer360,
  vehicle_360: vehicle360,
  vehicle_registration_search: vehicle360,
  used_car_rc_pending_report: usedCarRcPendingReport,
  inspection_report: usedCarRcPendingReport,
  background_check_report: usedCarRcPendingReport,
  rc_lookup: usedCarRcPendingReport,
  challan_report: usedCarRcPendingReport,
};

const canonicalIntentHandlers = {
  vehicle_recommendation_discovery: vehicleRecommendationSearch,
  vehicle_budget_search: vehicleRecommendationSearch,
  vehicle_body_type_search: vehicleRecommendationSearch,
  vehicle_use_case_search: vehicleRecommendationSearch,
  vehicle_brand_search: vehicleRecommendationSearch,
  vehicle_model_overview: vehiclePricelist,
  vehicle_model_ambiguity: vehiclePricelist,
  vehicle_variant_ambiguity: vehiclePricelist,
  vehicle_pricelist: vehiclePricelist,
  vehicle_city_price: vehiclePricelist,
  vehicle_variant_price: vehiclePricelist,
  vehicle_price_breakup: vehiclePriceBreakup,
  vehicle_feature_answer: vehicleFeatureAvailability,
  vehicle_spec_lookup: vehicleFeatureAvailability,
  vehicle_model_features_explorer: vehicleFeatures,
  vehicle_feature_discovery: vehicleFeatureDiscovery,
  vehicle_must_have_feature_builder: vehicleRecommendationSearch,
  vehicle_safety_search: vehicleRecommendationSearch,
  vehicle_safety_answer: vehicleFeatureAvailability,
  vehicle_safety_comparison: vehicleComparison,
  vehicle_mileage_search: vehicleRecommendationSearch,
  vehicle_running_cost: vehicleRecommendationSearch,
  vehicle_fuel_decision_advisor: vehicleRecommendationSearch,
  vehicle_colors: vehicleColors,
  vehicle_color_gallery: vehicleColorSearch,
  vehicle_comparison: vehicleComparison,
  vehicle_model_comparison: vehicleComparison,
  vehicle_variant_comparison: vehicleVariantDifference,
  vehicle_similar_cars: similarCars,
  vehicle_alternative_search: similarCars,
  vehicle_variant_recommendation: vehicleVariantRecommendation,
  vehicle_variant_upgrade_value: vehicleVariantDifference,
  vehicle_variant_difference: vehicleVariantDifference,
  vehicle_emi_calculator: vehicleEmiCalculator,
  vehicle_emi_options: vehicleEmiBudgetSearch,
  vehicle_monthly_budget_planner: vehicleEmiBudgetSearch,
  new_car_finance_faq: vehicleFinanceFaqUnavailable,
  new_car_loan_enquiry: conversionLeadRun(
    "new_car_loan_enquiry",
    "finance_callback",
  ),
  vehicle_offers: vehicleOffersUnavailable,
  vehicle_offer_lookup: vehicleOffersUnavailable,
  aci_new_car_quotation: conversionLeadRun(
    "aci_new_car_quotation",
    "quotation",
  ),
  vehicle_test_drive_request: conversionLeadRun(
    "vehicle_test_drive_request",
    "test_drive",
  ),
  vehicle_callback_request: conversionLeadRun(
    "vehicle_callback_request",
    "callback",
  ),
  vehicle_availability: vehicleServiceDataUnavailable,
  vehicle_waiting_period: vehicleServiceDataUnavailable,
  new_car_service_center_search: vehicleServiceDataUnavailable,
  new_car_ownership_guide: vehicleServiceDataUnavailable,
  new_car_service_cost: vehicleServiceDataUnavailable,
  new_car_warranty: vehicleServiceDataUnavailable,
  vehicle_tco_analysis: vehicleRecommendationSearch,
  vehicle_resale_value_analysis: vehicleRecommendationSearch,
  vehicle_lifestyle_fit_score: vehicleRecommendationSearch,
  vehicle_senior_friendly_recommendation: vehicleRecommendationSearch,
  vehicle_space_practicality_advisor: vehicleRecommendationSearch,
  vehicle_performance_advisor: vehicleRecommendationSearch,
  vehicle_spec_ranking: vehicleRecommendationSearch,
  vehicle_bad_roads_advisor: vehicleRecommendationSearch,
  new_car_unavailable_or_out_of_scope: null,
};

const definitionByIntent = new Map(INTENT_DEFINITIONS.map((definition) => [definition.intent, definition]));

const buildTool = (intent) => {
  const canonicalConfig = getNewCarQuestionConfig(intent);
  const resolvedIntent = canonicalConfig
    ? resolveToolIntentForQuestionIntent(intent)
    : intent;
  const definition = definitionByIntent.get(intent) || {};
  const collections =
    definition.collections ||
    canonicalConfig?.dataSources ||
    getFieldMapForIntent(intent).map((item) => item.key);
  const directHandler =
    handlerByIntent[intent] ||
    canonicalIntentHandlers[intent] ||
    handlerByIntent[resolvedIntent] ||
    canonicalIntentHandlers[resolvedIntent];
  const implemented = Boolean(directHandler);
  return {
    intent,
    priority: definition.priority || 999,
    triggerKeywords: definition.patterns || [],
    collectionsUsed: collections,
    requiredEntities: definition.requiredEntities || [],
    optionalEntities: definition.optionalEntities || [],
    widgetType:
      definition.widgetType ||
      canonicalConfig?.canvasType ||
      canonicalConfig?.inlineType ||
      "records_table",
    failureMessage: definition.failureMessage || "No matching records found.",
    canHandle: ({ routed } = {}) => routed?.intent === intent,
    extractEntities: ({ routed, parsed } = {}) => ({ ...(parsed?.entities || {}), ...(routed?.entities || {}) }),
    validateEntities: (entities = {}) => {
      const missing = (definition.requiredEntities || []).filter((key) => {
        const value = entities[key];
        if (Array.isArray(value)) return value.length === 0;
        return value === undefined || value === null || value === "";
      });
      return { ok: missing.length === 0, missing };
    },
    implemented,
    run:
      directHandler ||
      (canonicalConfig ? newCarUnavailableRun(intent) : placeholderRun(intent)),
  };
};

export const AI_AGENT_TOOL_REGISTRY = Object.fromEntries(
  [
    ...new Set([
      ...INTENT_DEFINITIONS.map((definition) => definition.intent),
      ...Object.keys(handlerByIntent),
      ...NEW_CAR_INTENTS,
    ]),
  ].map((intent) => [intent, buildTool(intent)]),
);

export const getToolForIntent = (intent) => AI_AGENT_TOOL_REGISTRY[intent] || null;

export const listAiAgentTools = () =>
  Object.values(AI_AGENT_TOOL_REGISTRY)
    .sort((a, b) => a.priority - b.priority)
    .map((tool) => ({
      intent: tool.intent,
      priority: tool.priority,
      collectionsUsed: tool.collectionsUsed,
      requiredEntities: tool.requiredEntities,
      optionalEntities: tool.optionalEntities,
      widgetType: tool.widgetType,
      implemented: tool.implemented,
    }));
