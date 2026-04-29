import Loan from "../../models/Loan.js";
import InsuranceCase from "../../models/InsuranceCase.js";
import VehicleRecord from "../../models/VehicleRecord.js";
import UsedCarLead from "../../models/UsedCarLead.js";
import Receivable from "../../models/Receivable.js";
import {
  action,
  unavailableWidget,
  widget,
} from "./aiAgent.renderPayloads.js";
import { buildMissingValueQuery, firstMeaningful, firstNumber } from "./aiAgent.normalizers.js";
import { LIMIT, pushModuleTrace } from "./aiAgent.tools.js";
import { loanRows } from "./aiAgent.loanTools.js";
import { insuranceRows } from "./aiAgent.insuranceTools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";

const missingRegistrationQueryForAll = (fields) => ({
  $and: fields.map((field) => buildMissingValueQuery([field])),
});

export const missingRegistrationReport = async (parsed, access, trace) => {
  const sections = [];
  let total = 0;

  if (access.canAccess("loans")) {
    const loans = await Loan.find(missingRegistrationQueryForAll(["registrationNumber", "vehicleRegNo", "rc_redg_no"]))
      .sort({ updatedAt: -1 })
      .limit(LIMIT)
      .lean();
    const count = await Loan.countDocuments(missingRegistrationQueryForAll(["registrationNumber", "vehicleRegNo", "rc_redg_no"]));
    pushModuleTrace(trace, "Loans", count, { returned: loans.length });
    total += count;
    sections.push({
      module: "Loans",
      total: count,
      rows: loanRows(loans),
      actions: [
        action("open_dashboard_with_filter", "View in Loan Dashboard", {
          route: "/loans",
          query: { missingRegistration: "true" },
        }),
      ],
    });
  } else noteRestriction(access, "Loans", "No loan access");

  if (access.canAccess("insurance")) {
    const query = buildMissingValueQuery(["registrationNumber"]);
    const insurance = await InsuranceCase.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
    const count = await InsuranceCase.countDocuments(query);
    pushModuleTrace(trace, "Insurance", count, { returned: insurance.length });
    total += count;
    sections.push({
      module: "Insurance",
      total: count,
      rows: insuranceRows(insurance),
      actions: [
        action("open_dashboard_with_filter", "View in Insurance Dashboard", {
          route: "/insurance",
          query: { missingRegistration: "true" },
        }),
      ],
    });
  } else noteRestriction(access, "Insurance", "No insurance access");

  if (access.canAccess("vehicles")) {
    const query = buildMissingValueQuery(["registrationNumber"]);
    const vehicles = await VehicleRecord.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
    const count = await VehicleRecord.countDocuments(query);
    pushModuleTrace(trace, "Vehicle Records", count, { returned: vehicles.length });
    total += count;
    sections.push({
      module: "Vehicle Records",
      total: count,
      rows: vehicles.map((item) => ({
        id: String(item._id),
        customer: item.customerName,
        vehicle: [item.make, item.model, item.variant].filter(Boolean).join(" "),
        registrationNumber: item.registrationNumber,
        status: item.status,
        updatedAt: item.updatedAt,
      })),
    });
  }

  if (access.canAccess("usedCars")) {
    const query = buildMissingValueQuery(["vehicle.regNo"]);
    const leads = await UsedCarLead.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
    const count = await UsedCarLead.countDocuments(query);
    pushModuleTrace(trace, "Used Cars", count, { returned: leads.length });
    total += count;
    sections.push({
      module: "Used Cars",
      total: count,
      rows: leads.map((item) => ({
        id: String(item._id),
        customer: item?.seller?.name,
        vehicle: [item?.vehicle?.make, item?.vehicle?.model, item?.vehicle?.variant].filter(Boolean).join(" "),
        registrationNumber: item?.vehicle?.regNo,
        status: firstMeaningful(item?.workflow?.status, item.status),
        updatedAt: item.updatedAt,
        route: `/used-cars/leads/${item._id}`,
      })),
    });
  }

  return {
    widgets: [
      widget("count_summary", "Missing registration count", {
        summary: { total, modules: sections.map(({ module, total }) => ({ module, total })) },
      }),
      widget("chart_summary", "Module-wise missing registration", {
        charts: [{ type: "bar", label: "Missing registration", data: sections.map(({ module, total }) => ({ label: module, value: total })) }],
      }),
      widget("missing_registration_report", "Missing registration report", {
        data: { total, sections },
        rows: sections.flatMap((section) => section.rows.map((row) => ({ module: section.module, ...row }))),
        actions: sections.flatMap((section) => section.actions || []),
      }),
    ],
    followUpSuggestions: [
      "Show only loan cases",
      "Show only insurance cases",
      "Export report",
      "Open Loan Dashboard with filter",
      "Show records older than 30 days",
    ],
  };
};

export const payoutMissingReport = async (parsed, access, trace) => {
  const sections = [];
  let total = 0;
  if (access.canAccess("loans")) {
    const query = {
      $and: [
        { $or: [{ payoutApplicable: true }, { payoutApplicable: "Yes" }, { payoutApplicable: "yes" }] },
        {
          $or: [
            buildMissingValueQuery(["payout_percentage"]),
            buildMissingValueQuery(["prefile_sourcePayoutPercentage"]),
            { payout_amount: { $in: [null, "", 0] } },
            { payoutAmount: { $in: [null, "", 0] } },
          ],
        },
      ],
    };
    const loans = await Loan.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
    const count = await Loan.countDocuments(query);
    total += count;
    pushModuleTrace(trace, "Loans", count, { returned: loans.length });
    sections.push({
      module: "Loans",
      total: count,
      rows: loanRows(loans),
      actions: [
        action("open_dashboard_with_filter", "View in Loan Dashboard", {
          route: "/loans",
          query: { payoutMissing: "true" },
        }),
      ],
    });
  } else noteRestriction(access, "Loans", "No loan access");

  if (access.canAccess("insurance")) {
    const query = {
      $and: [
        { $or: [{ payoutApplicable: true }, { payoutApplicable: "Yes" }, { payoutApplicable: "yes" }] },
        buildMissingValueQuery(["payoutPercent"]),
      ],
    };
    const cases = await InsuranceCase.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
    const count = await InsuranceCase.countDocuments(query);
    total += count;
    pushModuleTrace(trace, "Insurance", count, { returned: cases.length });
    sections.push({
      module: "Insurance",
      total: count,
      rows: insuranceRows(cases),
      actions: [
        action("open_dashboard_with_filter", "View in Insurance Dashboard", {
          route: "/insurance",
          query: { payoutMissing: "true" },
        }),
      ],
    });
  }

  if (access.canAccess("payouts")) {
    const query = {
      $or: [
        buildMissingValueQuery(["payout_status"]),
        { payout_status: /pending|missing|not received/i },
        { payout_amount: { $in: [null, 0] } },
      ],
    };
    const receivables = await Receivable.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
    const count = await Receivable.countDocuments(query);
    total += count;
    pushModuleTrace(trace, "Receivables", count, { returned: receivables.length });
    sections.push({
      module: "Receivables",
      total: count,
      rows: receivables.map((item) => ({
        id: String(item._id),
        customer: item.customerName,
        reference: firstMeaningful(item.loanId, item.insuranceCaseId, item.payoutId),
        status: item.payout_status,
        amount: access.canViewFinance ? firstNumber(item.net_payout_amount, item.payout_amount) : undefined,
        updatedAt: item.updatedAt,
      })),
      actions: [
        action("open_dashboard_with_filter", "View Payout Dashboard", {
          route: "/payouts/receivables",
          query: { payoutMissing: "true" },
        }),
      ],
    });
  } else noteRestriction(access, "Payouts", "No payout access");

  const statusBreakdown = sections.flatMap((section) =>
    section.rows.map((row) => ({ module: section.module, status: row.status || "Unknown" })),
  );
  return {
    widgets: [
      widget("count_summary", "Payout missing count", {
        summary: { total, modules: sections.map(({ module, total }) => ({ module, total })) },
      }),
      widget("chart_summary", "Payout missing breakdown", {
        charts: [{ type: "bar", label: "Module-wise payout missing", data: sections.map(({ module, total }) => ({ label: module, value: total })) }],
      }),
      widget("payout_missing_report", "Payout missing report", {
        summary: { total, statusBreakdown },
        data: { sections },
        rows: sections.flatMap((section) => section.rows.map((row) => ({ module: section.module, ...row }))),
        actions: sections.flatMap((section) => section.actions || []),
      }),
    ],
    followUpSuggestions: ["Show only loan cases", "Open Loan Dashboard with filter", "Show records older than 30 days"],
  };
};

export const usedCarRcPendingReport = async (parsed, access, trace) => {
  if (!access.canAccess("usedCars")) {
    noteRestriction(access, "Used Cars", "No used-car access");
    return { widgets: [unavailableWidget("Used-car data unavailable", "You do not have access to used-car records.", ["Used Cars"])] };
  }
  const query = {
    $or: [
      buildMissingValueQuery(["vehicle.regNo"]),
      { "backgroundCheck.formValues.rcStatus": /pending|missing|not done/i },
      { "backgroundCheck.formValues.challanPending": /pending|yes|true/i },
    ],
  };
  const rows = await UsedCarLead.find(query).sort({ updatedAt: -1 }).limit(LIMIT).lean();
  pushModuleTrace(trace, "Used Cars", rows.length);
  return {
    widgets: [
      widget("records_table", "Used car RC/check pending", {
        rows: rows.map((item) => ({
          id: String(item._id),
          customer: item?.seller?.name,
          vehicle: [item?.vehicle?.make, item?.vehicle?.model, item?.vehicle?.variant].filter(Boolean).join(" "),
          registrationNumber: item?.vehicle?.regNo,
          status: firstMeaningful(item?.workflow?.status, item.status),
          updatedAt: item.updatedAt,
          route: `/used-cars/leads/${item._id}`,
        })),
      }),
    ],
    followUpSuggestions: ["Show vehicle 360", "Show records older than 30 days"],
  };
};

export const activeLoanExpiredInsuranceReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans") || !access.canAccess("insurance")) {
    if (!access.canAccess("loans")) noteRestriction(access, "Loans", "No loan access");
    if (!access.canAccess("insurance")) noteRestriction(access, "Insurance", "No insurance access");
    return {
      widgets: [
        unavailableWidget(
          "Report unavailable",
          "This report requires access to both loans and insurance records.",
          ["Loans", "Insurance"],
        ),
      ],
    };
  }

  const activeLoanQuery = {
    $or: [
      { loanStatus: /active|disbursed|running|approved/i },
      { status: /active|disbursed|running|approved/i },
      { currentStage: /active|disbursed|running|approved/i },
    ],
  };
  const loans = await Loan.find(activeLoanQuery).sort({ updatedAt: -1 }).limit(300).lean();
  pushModuleTrace(trace, "Loans", loans.length);

  const expiredQuery = {
    $or: [
      { newOdExpiryDate: { $lt: new Date() } },
      { newTpExpiryDate: { $lt: new Date() } },
      { status: /expired/i },
    ],
  };
  const insurance = await InsuranceCase.find(expiredQuery).sort({ updatedAt: -1 }).limit(500).lean();
  pushModuleTrace(trace, "Insurance", insurance.length);

  const insuranceByReg = new Map();
  const insuranceByCustomer = new Map();
  for (const item of insurance) {
    const reg = String(item.registrationNumber || "").replace(/\W/g, "").toUpperCase();
    if (reg) insuranceByReg.set(reg, item);
    const customer = String(item.customerName || "").toLowerCase().trim();
    if (customer) insuranceByCustomer.set(customer, item);
  }

  const rows = loans
    .map((loan) => {
      const reg = String(firstMeaningful(loan.registrationNumber, loan.vehicleRegNo, loan.rc_redg_no)).replace(/\W/g, "").toUpperCase();
      const customer = String(loan.customerName || "").toLowerCase().trim();
      const policy = (reg && insuranceByReg.get(reg)) || (customer && insuranceByCustomer.get(customer));
      if (!policy) return null;
      return {
        customer: loan.customerName,
        vehicle: [loan.vehicleMake, loan.vehicleModel, loan.vehicleVariant].filter(Boolean).join(" "),
        registrationNumber: firstMeaningful(loan.registrationNumber, loan.vehicleRegNo, loan.rc_redg_no),
        loanStatus: firstMeaningful(loan.loanStatus, loan.status, loan.currentStage),
        insuranceStatus: policy.status,
        insuranceExpiry: firstMeaningful(policy.newOdExpiryDate, policy.newTpExpiryDate),
        loanRoute: `/loans/edit/${loan._id}`,
        insuranceRoute: `/insurance/edit/${policy.caseId || policy._id}`,
      };
    })
    .filter(Boolean)
    .slice(0, LIMIT);

  return {
    widgets: [
      widget("records_table", "Active loans with expired insurance", {
        summary: { total: rows.length },
        rows,
        actions: [
          action("open_dashboard_with_filter", "Open Loan Dashboard", {
            route: "/loans",
            query: { loanActive: "true", insuranceExpired: "true" },
          }),
          action("open_dashboard_with_filter", "Open Insurance Dashboard", {
            route: "/insurance",
            query: { expired: "true", loanActive: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Show customer 360", "Show vehicle 360", "Show records older than 30 days"],
  };
};
