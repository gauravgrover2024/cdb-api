import { parseAgentMessage } from "./aiAgent.intentParser.js";
import { buildAccessContext } from "./aiAgent.accessControl.js";
import { assembleResponse, buildFilters } from "./aiAgent.tools.js";
import { latestInsurance } from "./aiAgent.insuranceTools.js";
import { loanStatusOrClosure } from "./aiAgent.loanTools.js";
import {
  activeLoanExpiredInsuranceReport,
  missingRegistrationReport,
  operationsDigest,
  payoutMissingReport,
  usedCarRcPendingReport,
} from "./aiAgent.reportTools.js";
import {
  priceHistoryReport,
  similarCars,
  vehicleFeatureAvailability,
  vehicleComparison,
  vehiclePricelist,
} from "./aiAgent.vehicleTools.js";
import { customer360, vehicle360 } from "./aiAgent.customerTools.js";
import { unavailableWidget } from "./aiAgent.renderPayloads.js";

const handlerByIntent = {
  latest_insurance: latestInsurance,
  loan_status: loanStatusOrClosure,
  loan_closure: loanStatusOrClosure,
  missing_registration_report: missingRegistrationReport,
  payout_missing_report: payoutMissingReport,
  vehicle_pricelist: vehiclePricelist,
  vehicle_feature_availability: vehicleFeatureAvailability,
  similar_cars: similarCars,
  vehicle_comparison: vehicleComparison,
  price_history_report: priceHistoryReport,
  active_loan_expired_insurance_report: activeLoanExpiredInsuranceReport,
  operations_digest: operationsDigest,
  data_quality_workbench: missingRegistrationReport,
  finance_intelligence: payoutMissingReport,
  customer_360: customer360,
  vehicle_360: vehicle360,
  used_car_rc_pending_report: usedCarRcPendingReport,
};

const fallbackHandler = async (parsed, access, trace) => {
  if (parsed.entities.customerName) return customer360({ ...parsed, intent: "customer_360" }, access, trace);
  if (parsed.entities.registrationNumber || parsed.entities.last4 || parsed.entities.model) {
    return vehicle360({ ...parsed, intent: "vehicle_360" }, access, trace);
  }
  return {
    widgets: [
      unavailableWidget(
        "I need a more specific question",
        "Ask for a customer, vehicle, insurance, loan, pricelist, comparison, or report and I will query live records.",
        ["ACI Assist"],
      ),
    ],
    followUpSuggestions: [
      "How many cars are without registration number?",
      "Cases with payout missing",
      "Verna pricelist",
      "Customer 360 Rahul Diwan",
    ],
  };
};

const intentLabel = (intent) =>
  String(intent || "answer")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());

const buildAssistantMessage = (parsed, result) => {
  if (result.ambiguity) return result.ambiguity.message;
  if (!result.widgets?.length) return "I checked live records but did not find a matching result.";
  const primary = result.widgets[0];
  if (primary.type === "unavailable_notice") return primary.data?.message || "That data is unavailable.";
  if (primary.summary?.total !== undefined) return `I found ${primary.summary.total} matching records.`;
  if (primary.data?.total !== undefined) return `I found ${primary.data.total} matching records.`;
  return `Here is the ${intentLabel(parsed.intent)} result from live CDrive records.`;
};

export const chatWithAgent = async ({
  message,
  sessionId,
  context = {},
  selectedEntity = null,
  filters = {},
  user,
} = {}) => {
  const parsed = parseAgentMessage(message, context, selectedEntity, filters);
  const access = buildAccessContext(user);
  const trace = [];
  const handler = handlerByIntent[parsed.intent] || fallbackHandler;
  const result = await handler(parsed, access, trace);
  const queryPlan =
    (parsed.wantsDebug || context?.debug || filters?.debug) && access.canDebug
      ? {
          sessionId,
          detectedIntent: parsed.intent,
          extractedEntities: parsed.entities,
          filters,
          modulesScanned: trace.map((item) => item.module),
          toolsUsed: [handler.name || "fallbackHandler"],
          recordsFound: trace.reduce((sum, item) => sum + (Number(item.matched) || 0), 0),
          confidence: parsed.confidence,
          accessRestrictionsApplied: access.restrictions,
        }
      : undefined;

  return assembleResponse({
    parsed,
    assistantMessage: buildAssistantMessage(parsed, result),
    resultType: result.ambiguity ? "ambiguity" : result.widgets?.[0]?.type || "answer",
    widgets: result.widgets || [],
    modulesChecked: trace,
    filtersApplied: buildFilters(parsed).map((chip) => `${chip.label}: ${chip.value}`),
    followUpSuggestions: result.followUpSuggestions || [],
    ambiguity: result.ambiguity,
    access,
    queryPlan,
    filters: buildFilters(parsed, result.moduleName),
  });
};
