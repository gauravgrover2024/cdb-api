import InsuranceCase from "../../models/InsuranceCase.js";
import {
  action,
  disabledAction,
  unavailableWidget,
  widget,
} from "./aiAgent.renderPayloads.js";
import {
  firstMeaningful,
  firstNumber,
  formatDateValue,
  getRegistration,
  getVehicleName,
  latestDate,
} from "./aiAgent.normalizers.js";
import {
  buildEntityQuery,
  canSearchByEntity,
  entityOption,
  getInsuranceRoute,
  LIMIT,
  makeAmbiguity,
  pushModuleTrace,
  rowBase,
  safeId,
} from "./aiAgent.tools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";

const insuranceQuery = (entities) =>
  buildEntityQuery({
    entities,
    customerFields: ["customerName", "companyName", "mobile"],
    registrationFields: ["registrationNumber"],
    vehicleFields: ["vehicleMake", "vehicleModel", "vehicleVariant"],
  });

const insuranceSortDate = (item) =>
  latestDate(
    item.newOdExpiryDate,
    item.newTpExpiryDate,
    item.newPolicyStartDate,
    item.newIssueDate,
    item.updatedAt,
    item.createdAt,
  )?.getTime() || 0;

export const findInsuranceCases = async (parsed, access, trace, limit = LIMIT) => {
  if (!access.canAccess("insurance")) {
    noteRestriction(access, "Insurance", "No insurance access");
    return [];
  }
  const query = insuranceQuery(parsed.entities);
  if (!Object.keys(query).length) return [];
  const records = await InsuranceCase.find(query).sort({ updatedAt: -1 }).limit(limit).lean();
  pushModuleTrace(trace, "Insurance", records.length);
  return records;
};

export const latestInsurance = async (parsed, access, trace) => {
  if (!access.canAccess("insurance")) {
    noteRestriction(access, "Insurance", "No insurance access");
    return {
      widgets: [
        unavailableWidget("Insurance unavailable", "You do not have access to insurance records.", [
          "Insurance",
        ]),
      ],
      followUpSuggestions: [],
    };
  }
  if (!canSearchByEntity(parsed.entities)) {
    return {
      widgets: [
        unavailableWidget(
          "Need a customer or vehicle",
          "Share a customer name, registration number, last 4 digits, or vehicle model to find the latest insurance.",
          ["Insurance"],
        ),
      ],
      followUpSuggestions: [],
    };
  }
  const records = await findInsuranceCases(parsed, access, trace, 20);
  if (!records.length) {
    return {
      widgets: [
        unavailableWidget("No insurance case found", "No matching insurance record was found in live data.", [
          "Insurance",
        ]),
      ],
      followUpSuggestions: ["Check loan status", "Show customer 360", "Show vehicle 360"],
    };
  }
  const uniqueRegistrations = new Set(records.map((item) => getRegistration(item)).filter(Boolean));
  if (!parsed.selectedEntity && records.length > 1 && uniqueRegistrations.size > 1 && parsed.entities.last4) {
    return {
      ambiguity: makeAmbiguity(records.map((item) => entityOption(item, "Insurance", "insurance_case"))),
      widgets: [],
      followUpSuggestions: [],
    };
  }
  const item = [...records].sort((a, b) => insuranceSortDate(b) - insuranceSortDate(a))[0];
  const premium = firstNumber(item.newTotalPremium, item.totalPremium, item.premium);
  const paymentStatus = item.paymentHistory?.length
    ? "Payment history available"
    : firstMeaningful(item.paymentStatus, item.customerPaymentStatus, item.inhousePaymentStatus);
  return {
    widgets: [
      widget("insurance_case_card", "Latest insurance", {
        data: {
          id: safeId(item),
          caseId: item.caseId,
          customer: firstMeaningful(item.customerName, item.companyName),
          vehicle: getVehicleName(item),
          registrationNumber: getRegistration(item),
          policyNumber: item.newPolicyNumber,
          insurer: item.newInsuranceCompany,
          policyType: item.newPolicyType,
          premium,
          startDate: formatDateValue(item.newPolicyStartDate),
          expiryDate: formatDateValue(firstMeaningful(item.newOdExpiryDate, item.newTpExpiryDate)),
          status: item.status,
          paymentStatus,
          source: firstMeaningful(item.policyJourneyClassification, item.usedCarFlowType),
        },
        actions: [
          action("open_record", "Open Full Case", { route: getInsuranceRoute(item) }),
          access.canEdit
            ? action("edit_record", "Edit", { route: getInsuranceRoute(item) })
            : disabledAction("edit_record", "Edit", "You do not have edit access"),
          disabledAction("download_if_supported", "Download Policy", "No policy download route found"),
          disabledAction("send_whatsapp_if_supported", "Send WhatsApp", "Messaging is not enabled from ACI Assist"),
        ],
      }),
    ],
    followUpSuggestions: ["Show customer 360", "Show vehicle 360", "Open policy", "Check loan status"],
  };
};

export const insuranceRows = (records) =>
  records.map((item) => ({
    ...rowBase(item),
    caseId: item.caseId,
    policyNumber: item.newPolicyNumber,
    insurer: item.newInsuranceCompany,
    route: getInsuranceRoute(item),
  }));
