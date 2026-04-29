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
import { findAndCount, findLean, LIMIT, pushModuleTrace } from "./aiAgent.tools.js";
import { loanRows } from "./aiAgent.loanTools.js";
import { insuranceRows } from "./aiAgent.insuranceTools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";

const missingRegistrationQueryForAll = (fields) => ({
  $and: fields.map((field) => buildMissingValueQuery([field])),
});

export const missingRegistrationReport = async (parsed, access, trace) => {
  const sectionTasks = [];

  if (access.canAccess("loans")) {
    sectionTasks.push(async () => {
      const { rows: loans, count, approximate, error } = await findAndCount(
        Loan,
        missingRegistrationQueryForAll(["registrationNumber", "vehicleRegNo", "rc_redg_no"]),
        { sort: { updatedAt: -1 }, limit: LIMIT },
      );
      pushModuleTrace(trace, "Loans", count, { returned: loans.length, approximate });
      return {
        module: "Loans",
        total: count,
        rows: loanRows(loans),
        actions: [
          action("open_dashboard_with_filter", "View in Loan Dashboard", {
            route: "/loans",
            query: { missingRegistration: "true" },
          }),
        ],
        approximate,
        error,
      };
    });
  } else noteRestriction(access, "Loans", "No loan access");

  if (access.canAccess("insurance")) {
    sectionTasks.push(async () => {
      const query = buildMissingValueQuery(["registrationNumber"]);
      const { rows: insurance, count, approximate, error } = await findAndCount(InsuranceCase, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Insurance", count, { returned: insurance.length, approximate });
      return {
        module: "Insurance",
        total: count,
        rows: insuranceRows(insurance),
        actions: [
          action("open_dashboard_with_filter", "View in Insurance Dashboard", {
            route: "/insurance",
            query: { missingRegistration: "true" },
          }),
        ],
        approximate,
        error,
      };
    });
  } else noteRestriction(access, "Insurance", "No insurance access");

  if (access.canAccess("vehicles")) {
    sectionTasks.push(async () => {
      const query = buildMissingValueQuery(["registrationNumber"]);
      const { rows: vehicles, count, approximate, error } = await findAndCount(VehicleRecord, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Vehicle Records", count, { returned: vehicles.length, approximate });
      return {
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
        approximate,
        error,
      };
    });
  }

  if (access.canAccess("usedCars")) {
    sectionTasks.push(async () => {
      const query = buildMissingValueQuery(["vehicle.regNo"]);
      const { rows: leads, count, approximate, error } = await findAndCount(UsedCarLead, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Used Cars", count, { returned: leads.length, approximate });
      return {
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
        approximate,
        error,
      };
    });
  }

  const sections = (await Promise.all(sectionTasks.map((task) => task()))).filter(Boolean);
  const total = sections.reduce((sum, section) => sum + (Number(section.total) || 0), 0);

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
  const sectionTasks = [];
  if (access.canAccess("loans")) {
    sectionTasks.push(async () => {
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
      const { rows: loans, count, approximate, error } = await findAndCount(Loan, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Loans", count, { returned: loans.length, approximate });
      return {
        module: "Loans",
        total: count,
        rows: loanRows(loans),
        actions: [
          action("open_dashboard_with_filter", "View in Loan Dashboard", {
            route: "/loans",
            query: { payoutMissing: "true" },
          }),
        ],
        approximate,
        error,
      };
    });
  } else noteRestriction(access, "Loans", "No loan access");

  if (access.canAccess("insurance")) {
    sectionTasks.push(async () => {
      const query = {
        $and: [
          { $or: [{ payoutApplicable: true }, { payoutApplicable: "Yes" }, { payoutApplicable: "yes" }] },
          buildMissingValueQuery(["payoutPercent"]),
        ],
      };
      const { rows: cases, count, approximate, error } = await findAndCount(InsuranceCase, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Insurance", count, { returned: cases.length, approximate });
      return {
        module: "Insurance",
        total: count,
        rows: insuranceRows(cases),
        actions: [
          action("open_dashboard_with_filter", "View in Insurance Dashboard", {
            route: "/insurance",
            query: { payoutMissing: "true" },
          }),
        ],
        approximate,
        error,
      };
    });
  }

  if (access.canAccess("payouts")) {
    sectionTasks.push(async () => {
      const query = {
        $or: [
          buildMissingValueQuery(["payout_status"]),
          { payout_status: /pending|missing|not received/i },
          { payout_amount: { $in: [null, 0] } },
        ],
      };
      const { rows: receivables, count, approximate, error } = await findAndCount(Receivable, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Receivables", count, { returned: receivables.length, approximate });
      return {
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
        approximate,
        error,
      };
    });
  } else noteRestriction(access, "Payouts", "No payout access");

  const sections = (await Promise.all(sectionTasks.map((task) => task()))).filter(Boolean);
  const total = sections.reduce((sum, section) => sum + (Number(section.total) || 0), 0);
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
  const rows = await findLean(UsedCarLead, query, { sort: { updatedAt: -1 }, limit: LIMIT });
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

  const expiredQuery = {
    $or: [
      { newOdExpiryDate: { $lt: new Date() } },
      { newTpExpiryDate: { $lt: new Date() } },
      { status: /expired/i },
    ],
  };
  const [loans, insurance] = await Promise.all([
    findLean(Loan, activeLoanQuery, { sort: { updatedAt: -1 }, limit: 150 }),
    findLean(InsuranceCase, expiredQuery, { sort: { updatedAt: -1 }, limit: 250 }),
  ]);
  pushModuleTrace(trace, "Loans", loans.length, { capped: loans.length >= 150 });
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

export const operationsDigest = async (parsed, access, trace) => {
  const now = new Date();
  const nextWeek = new Date(now);
  nextWeek.setDate(nextWeek.getDate() + 7);

  const tasks = [];
  if (access.canAccess("insurance")) {
    tasks.push(
      findLean(
        InsuranceCase,
        {
          $or: [
            { newOdExpiryDate: { $gte: now.toISOString(), $lte: nextWeek.toISOString() } },
            { newTpExpiryDate: { $gte: now.toISOString(), $lte: nextWeek.toISOString() } },
          ],
        },
        { sort: { newOdExpiryDate: 1, updatedAt: -1 }, limit: 12 },
      ).then((rows) => {
        pushModuleTrace(trace, "Insurance expiring", rows.length);
        return {
          module: "Insurance",
          issue: "Policies expiring in 7 days",
          count: rows.length,
          rows: insuranceRows(rows).map((row) => ({ ...row, issue: "Expiring soon" })),
          action: action("open_dashboard_with_filter", "Open Insurance", {
            route: "/insurance",
            query: { expiringThisWeek: "true" },
          }),
        };
      }),
    );
  }

  if (access.canAccess("loans")) {
    tasks.push(
      findLean(
        Loan,
        {
          $and: [
            { $or: [{ loanStatus: /approved/i }, { status: /approved/i }, { currentStage: /approved/i }] },
            {
              $or: [
                { disburse_amount: { $in: [null, "", 0] } },
                { approval_loanAmountDisbursed: { $in: [null, "", 0] } },
                { disbursement_date: { $in: [null, ""] } },
              ],
            },
          ],
        },
        { sort: { updatedAt: -1 }, limit: 12 },
      ).then((rows) => {
        pushModuleTrace(trace, "Approved not disbursed", rows.length);
        return {
          module: "Loans",
          issue: "Approved but not disbursed",
          count: rows.length,
          rows: loanRows(rows).map((row) => ({ ...row, issue: "Approved not disbursed" })),
          action: action("open_dashboard_with_filter", "Open Loans", {
            route: "/loans",
            query: { approvedNotDisbursed: "true" },
          }),
        };
      }),
    );
  }

  if (access.canAccess("usedCars")) {
    tasks.push(
      findLean(
        UsedCarLead,
        {
          $or: [
            buildMissingValueQuery(["vehicle.regNo"]),
            { "backgroundCheck.status": /pending|open/i },
            { "backgroundCheck.formValues.rcStatus": /pending|missing|not done/i },
          ],
        },
        { sort: { updatedAt: -1 }, limit: 12 },
      ).then((rows) => {
        pushModuleTrace(trace, "Used-car checks", rows.length);
        return {
          module: "Used Cars",
          issue: "RC/background check pending",
          count: rows.length,
          rows: rows.map((item) => ({
            id: String(item._id),
            customer: item?.seller?.name,
            vehicle: [item?.vehicle?.make, item?.vehicle?.model, item?.vehicle?.variant].filter(Boolean).join(" "),
            registrationNumber: item?.vehicle?.regNo,
            status: firstMeaningful(item?.backgroundCheck?.status, item?.workflow?.status, item.status),
            updatedAt: item.updatedAt,
            issue: "RC/background check pending",
            route: `/used-cars/leads/${item._id}`,
          })),
          action: action("open_dashboard_with_filter", "Open Used Cars", {
            route: "/used-cars/background-check",
            query: { rcPending: "true" },
          }),
        };
      }),
    );
  }

  const sections = await Promise.all(tasks);
  const rows = sections.flatMap((section) => section.rows.map((row) => ({ module: section.module, ...row })));
  const total = sections.reduce((sum, section) => sum + section.count, 0);

  return {
    widgets: [
      widget("count_summary", "Operations attention", {
        summary: { total, modules: sections.map(({ module, issue, count }) => ({ module, issue, total: count })) },
      }),
      widget("module_breakdown", "Attention by module", {
        modules: sections.map(({ module, issue, count }) => ({ module, label: issue, count })),
      }),
      widget("records_table", "Records needing attention", {
        rows,
        actions: sections.map((section) => section.action),
      }),
    ],
    followUpSuggestions: [
      "Show only insurance cases",
      "Show approved but not disbursed cases",
      "Cases with payout missing",
      "How many cars are without registration number?",
    ],
  };
};
