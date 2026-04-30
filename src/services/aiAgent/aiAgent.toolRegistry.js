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
  vehicleColors,
  vehicleComparison,
  vehicleFeatureAvailability,
  vehicleFeatures,
  vehiclePriceBreakup,
  vehiclePricelist,
} from "./aiAgent.vehicleTools.js";
import { getFieldMapForIntent } from "./aiAgent.fieldMaps.js";
import { INTENT_DEFINITIONS } from "./aiAgent.intentRouter.js";

const placeholderRun = (intent) => async () => ({
  widgets: [
    unavailableWidget(
      "Tool not implemented yet",
      `The deterministic route for ${intent} exists, but its database tool has not been implemented in this phase.`,
      [],
    ),
  ],
});

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
  vehicle_features: vehicleFeatures,
  vehicle_feature_answer: vehicleFeatureAvailability,
  similar_cars: similarCars,
  vehicle_comparison: vehicleComparison,
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

const definitionByIntent = new Map(INTENT_DEFINITIONS.map((definition) => [definition.intent, definition]));

const buildTool = (intent) => {
  const definition = definitionByIntent.get(intent) || {};
  const collections = definition.collections || getFieldMapForIntent(intent).map((item) => item.key);
  const implemented = Boolean(handlerByIntent[intent]);
  return {
    intent,
    priority: definition.priority || 999,
    triggerKeywords: definition.patterns || [],
    collectionsUsed: collections,
    requiredEntities: definition.requiredEntities || [],
    optionalEntities: definition.optionalEntities || [],
    widgetType: definition.widgetType || "records_table",
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
    run: handlerByIntent[intent] || placeholderRun(intent),
  };
};

export const AI_AGENT_TOOL_REGISTRY = Object.fromEntries(
  [
    ...new Set([
      ...INTENT_DEFINITIONS.map((definition) => definition.intent),
      ...Object.keys(handlerByIntent),
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
