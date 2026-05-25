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
  findLean,
  getInsuranceRoute,
  LIMIT,
  makeAmbiguity,
  pushModuleTrace,
  rowBase,
  objectIdOrNull,
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
  const selectedId =
    parsed.selectedEntity?.entityType === "insurance_case"
      ? objectIdOrNull(parsed.selectedEntity?.id)
      : null;

  if (selectedId) {
    const record = await InsuranceCase.findOne({ _id: selectedId })
      .maxTimeMS(2500)
      .lean();

    pushModuleTrace(trace, "Insurance", record ? 1 : 0, {
      selectedEntity: true,
    });

    return record ? [record] : [];
  }
  const query = insuranceQuery(parsed.entities);
  if (!Object.keys(query).length) return [];
  const records = await findLean(InsuranceCase, query, { sort: { updatedAt: -1 }, limit });
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

export const insuranceExpiryReport = async (parsed, access, trace) => {
  if (!access.canAccess("insurance")) {
    noteRestriction(access, "Insurance", "No insurance access");
    return {
      widgets: [unavailableWidget("Insurance report unavailable", "You do not have access to insurance records.", ["Insurance"])],
      followUpSuggestions: [],
    };
  }
  const now = new Date();
  const sevenDays = new Date(now);
  sevenDays.setDate(sevenDays.getDate() + 7);
  const lower = parsed.lower || "";
  const query = /expired/.test(lower)
    ? {
        $or: [
          { newOdExpiryDate: { $lt: now } },
          { newTpExpiryDate: { $lt: now } },
          { status: /expired/i },
        ],
      }
    : {
        $or: [
          { newOdExpiryDate: { $gte: now, $lte: sevenDays } },
          { newTpExpiryDate: { $gte: now, $lte: sevenDays } },
        ],
      };
  const records = await findLean(InsuranceCase, query, { sort: { newOdExpiryDate: 1, newTpExpiryDate: 1, updatedAt: -1 }, limit: LIMIT });
  pushModuleTrace(trace, "Insurance", records.length);
  const rows = insuranceRows(records).map((row) => ({
    ...row,
    expiry: firstMeaningful(row.expiryDate, row.newOdExpiryDate, row.newTpExpiryDate),
  }));
  return {
    widgets: [
      widget("count_summary", /expired/.test(lower) ? "Expired policies" : "Policies expiring soon", {
        summary: { total: rows.length, modules: [{ module: "Insurance", total: rows.length }] },
      }),
      widget("records_table", /expired/.test(lower) ? "Expired insurance cases" : "Insurance expiring in 7 days", {
        summary: { total: rows.length },
        rows,
        actions: [
          action("open_dashboard_with_filter", "Open Insurance Dashboard", {
            route: "/insurance",
            query: /expired/.test(lower) ? { expired: "true" } : { expiringThisWeek: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Show latest insurance", "Show customer 360", "Check loan status"],
  };
};

export const insuranceRows = (records) =>
  records.map((item) => ({
    ...rowBase(item),
    caseId: item.caseId,
    policyNumber: item.newPolicyNumber,
    insurer: item.newInsuranceCompany,
    expiryDate: formatDateValue(firstMeaningful(item.newOdExpiryDate, item.newTpExpiryDate)),
    route: getInsuranceRoute(item),
  }));
