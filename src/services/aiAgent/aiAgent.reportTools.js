import Loan from "../../models/Loan.js";
import InsuranceCase from "../../models/InsuranceCase.js";
import VehicleRecord from "../../models/VehicleRecord.js";
import UsedCarLead from "../../models/UsedCarLead.js";
import Receivable from "../../models/Receivable.js";
import Payment from "../../models/Payment.js";
import DeliveryOrder from "../../models/DeliveryOrder.js";
import { action, unavailableWidget, widget } from "./aiAgent.renderPayloads.js";
import {
  buildMissingValueQuery,
  firstMeaningful,
  firstNumber,
  formatDateValue,
  isMissingValue,
  latestDate,
} from "./aiAgent.normalizers.js";
import {
  findAndCount,
  findLean,
  LIMIT,
  pushModuleTrace,
} from "./aiAgent.tools.js";
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
      const {
        rows: loans,
        count,
        approximate,
        error,
      } = await findAndCount(
        Loan,
        missingRegistrationQueryForAll([
          "registrationNumber",
          "vehicleRegNo",
          "rc_redg_no",
        ]),
        { sort: { updatedAt: -1 }, limit: LIMIT },
      );
      pushModuleTrace(trace, "Loans", count, {
        returned: loans.length,
        approximate,
      });
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
      const {
        rows: insurance,
        count,
        approximate,
        error,
      } = await findAndCount(InsuranceCase, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Insurance", count, {
        returned: insurance.length,
        approximate,
      });
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
      const {
        rows: vehicles,
        count,
        approximate,
        error,
      } = await findAndCount(VehicleRecord, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Vehicle Records", count, {
        returned: vehicles.length,
        approximate,
      });
      return {
        module: "Vehicle Records",
        total: count,
        rows: vehicles.map((item) => ({
          id: String(item._id),
          customer: item.customerName,
          vehicle: [item.make, item.model, item.variant]
            .filter(Boolean)
            .join(" "),
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
      const {
        rows: leads,
        count,
        approximate,
        error,
      } = await findAndCount(UsedCarLead, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Used Cars", count, {
        returned: leads.length,
        approximate,
      });
      return {
        module: "Used Cars",
        total: count,
        rows: leads.map((item) => ({
          id: String(item._id),
          customer: item?.seller?.name,
          vehicle: [
            item?.vehicle?.make,
            item?.vehicle?.model,
            item?.vehicle?.variant,
          ]
            .filter(Boolean)
            .join(" "),
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

  const sections = (
    await Promise.all(sectionTasks.map((task) => task()))
  ).filter(Boolean);
  const total = sections.reduce(
    (sum, section) => sum + (Number(section.total) || 0),
    0,
  );

  return {
    widgets: [
      widget("count_summary", "Missing registration count", {
        summary: {
          total,
          modules: sections.map(({ module, total }) => ({ module, total })),
        },
      }),
      widget("chart_summary", "Module-wise missing registration", {
        charts: [
          {
            type: "bar",
            label: "Missing registration",
            data: sections.map(({ module, total }) => ({
              label: module,
              value: total,
            })),
          },
        ],
      }),
      widget("missing_registration_report", "Missing registration report", {
        data: { total, sections },
        rows: sections.flatMap((section) =>
          section.rows.map((row) => ({ module: section.module, ...row })),
        ),
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
          {
            $or: [
              { payoutApplicable: true },
              { payoutApplicable: "Yes" },
              { payoutApplicable: "yes" },
            ],
          },
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
      const {
        rows: loans,
        count,
        approximate,
        error,
      } = await findAndCount(Loan, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Loans", count, {
        returned: loans.length,
        approximate,
      });
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
          {
            $or: [
              { payoutApplicable: true },
              { payoutApplicable: "Yes" },
              { payoutApplicable: "yes" },
            ],
          },
          buildMissingValueQuery(["payoutPercent"]),
        ],
      };
      const {
        rows: cases,
        count,
        approximate,
        error,
      } = await findAndCount(InsuranceCase, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Insurance", count, {
        returned: cases.length,
        approximate,
      });
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
      const {
        rows: receivables,
        count,
        approximate,
        error,
      } = await findAndCount(Receivable, query, {
        sort: { updatedAt: -1 },
        limit: LIMIT,
      });
      pushModuleTrace(trace, "Receivables", count, {
        returned: receivables.length,
        approximate,
      });
      return {
        module: "Receivables",
        total: count,
        rows: receivables.map((item) => ({
          id: String(item._id),
          customer: item.customerName,
          reference: firstMeaningful(
            item.loanId,
            item.insuranceCaseId,
            item.payoutId,
          ),
          status: item.payout_status,
          amount: access.canViewFinance
            ? firstNumber(item.net_payout_amount, item.payout_amount)
            : undefined,
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

  const sections = (
    await Promise.all(sectionTasks.map((task) => task()))
  ).filter(Boolean);
  const total = sections.reduce(
    (sum, section) => sum + (Number(section.total) || 0),
    0,
  );
  const statusBreakdown = sections.flatMap((section) =>
    section.rows.map((row) => ({
      module: section.module,
      status: row.status || "Unknown",
    })),
  );
  return {
    widgets: [
      widget("count_summary", "Payout missing count", {
        summary: {
          total,
          modules: sections.map(({ module, total }) => ({ module, total })),
        },
      }),
      widget("chart_summary", "Payout missing breakdown", {
        charts: [
          {
            type: "bar",
            label: "Module-wise payout missing",
            data: sections.map(({ module, total }) => ({
              label: module,
              value: total,
            })),
          },
        ],
      }),
      widget("payout_missing_report", "Payout missing report", {
        summary: { total, statusBreakdown },
        data: { sections },
        rows: sections.flatMap((section) =>
          section.rows.map((row) => ({ module: section.module, ...row })),
        ),
        actions: sections.flatMap((section) => section.actions || []),
      }),
    ],
    followUpSuggestions: [
      "Show only loan cases",
      "Open Loan Dashboard with filter",
      "Show records older than 30 days",
    ],
  };
};

const approvedLoanQuery = {
  $or: [
    { loanStatus: /approved/i },
    { status: /approved/i },
    { currentStage: /approved/i },
    { approvalStatus: /approved/i },
    { approval_status: /approved/i },
    { "approval_banksData.status": /approved/i },
  ],
};

const disbursedMissingQuery = {
  $or: [
    {
      $and: [
        buildMissingValueQuery(["disbursedDate"]),
        buildMissingValueQuery(["disbursementDate"]),
        buildMissingValueQuery(["disbursement_date"]),
        buildMissingValueQuery(["approval_disbursedDate"]),
      ],
    },
    { disbursementStatus: /pending|not disbursed|awaiting/i },
    { disburse_status: /pending|not disbursed|awaiting/i },
  ],
};

const hasAnyDate = (...values) =>
  values.some((value) => !isMissingValue(value));
const hasAnyAmount = (...values) =>
  values.some((value) => firstNumber(value) > 0);

const approvedBankRows = (loan = {}) =>
  Array.isArray(loan.approval_banksData)
    ? loan.approval_banksData.filter((bank) =>
        /approved/i.test(String(bank?.status || bank?.approval_status || "")),
      )
    : [];

const bankRowHasDisbursedAmount = (bank = {}) =>
  hasAnyAmount(
    bank.disbursedAmount,
    bank.loanAmountDisbursed,
    bank.amountDisbursed,
    bank.disburse_amount,
  );

const bankRowHasDisbursedDate = (bank = {}) =>
  hasAnyDate(
    bank.disbursedDate,
    bank.disbursementDate,
    bank.disbursement_date,
    bank.approval_disbursedDate,
  );

const loanDisbursalBucket = (loan = {}) => {
  const hasDate = hasAnyDate(
    loan.disbursedDate,
    loan.disbursementDate,
    loan.disbursement_date,
    loan.approval_disbursedDate,
    loan.disburse_date,
  );
  const hasAmount = hasAnyAmount(
    loan.approval_loanAmountDisbursed,
    loan.postfile_loanAmountDisbursed,
    loan.disburse_amount,
    loan.disburseAmount,
  );
  const approvedRows = approvedBankRows(loan);
  if (
    approvedRows.some(
      (bank) =>
        bankRowHasDisbursedAmount(bank) && !bankRowHasDisbursedDate(bank),
    )
  ) {
    return "approved_bank_row_disbursed_amount_but_date_missing";
  }
  if (hasAmount && !hasDate) return "approved_amount_entered_but_date_missing";
  if (
    /approval/i.test(
      String(
        firstMeaningful(loan.currentStage, loan.status, loan.approval_status),
      ),
    )
  ) {
    return "approval_stage_records_if_status_indicates_approved_or_approval_stage";
  }
  return "approved_no_disbursal_amount_no_date";
};

const loanReportRow = (loan, access) => ({
  ...loanRows([loan])[0],
  currentStage: loan.currentStage,
  approvalStatus: firstMeaningful(
    loan.approval_status,
    loan.approvalStatus,
    loan.status,
  ),
  approvalBank: firstMeaningful(
    loan.approval_bankName,
    loan.postfile_bankName,
    loan.disburse_bankName,
  ),
  approvedAmount: access.canViewFinance
    ? firstNumber(
        loan.approval_loanAmountApproved,
        loan.postfile_loanAmountApproved,
      )
    : undefined,
  disbursedAmount: access.canViewFinance
    ? firstNumber(
        loan.approval_loanAmountDisbursed,
        loan.postfile_loanAmountDisbursed,
        loan.disburse_amount,
      )
    : undefined,
  disbursedDate: formatDateValue(
    firstMeaningful(
      loan.disbursedDate,
      loan.disbursementDate,
      loan.disbursement_date,
      loan.approval_disbursedDate,
      loan.disburse_date,
    ),
  ),
  updatedAt: formatDateValue(loan.updatedAt),
});

export const loanDisbursalReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const approvedLoans = await findLean(Loan, approvedLoanQuery, {
    sort: { updatedAt: -1 },
    limit: LIMIT * 40,
  });
  const incompleteLoans = approvedLoans.filter((loan) => {
    const topLevelDatesMissing = [
      loan.disbursedDate,
      loan.disbursementDate,
      loan.disbursement_date,
      loan.approval_disbursedDate,
      loan.disburse_date,
    ].every(isMissingValue);
    const pendingStatus = /pending|not disbursed|awaiting/i.test(
      String(firstMeaningful(loan.disbursementStatus, loan.disburse_status)),
    );
    return topLevelDatesMissing || pendingStatus;
  });
  const count = incompleteLoans.length;
  const shownLoans = incompleteLoans.slice(0, LIMIT);
  pushModuleTrace(trace, "Loans", count, {
    returned: shownLoans.length,
    scannedApproved: approvedLoans.length,
  });
  const rows = shownLoans.map((loan) => ({
    ...loanReportRow(loan, access),
    issue: loanDisbursalBucket(loan),
  }));
  const bucketCounts = rows.reduce((acc, row) => {
    acc[row.issue] = (acc[row.issue] || 0) + 1;
    return acc;
  }, {});
  return {
    widgets: [
      widget("count_summary", "Approved but not disbursed", {
        total: count,
        count,
        shown: rows.length,
        hasMore: count > rows.length,
        data: {
          total: count,
          shown: rows.length,
          hasMore: count > rows.length,
        },
        summary: {
          total: count,
          shown: rows.length,
          hasMore: count > rows.length,
          modules: [{ module: "Loans", total: count }],
        },
      }),
      widget("loan_disbursal_report", "Approved but not disbursed cases", {
        total: count,
        count,
        shown: rows.length,
        hasMore: count > rows.length,
        data: {
          total: count,
          shown: rows.length,
          hasMore: count > rows.length,
        },
        summary: {
          total: count,
          shown: rows.length,
          hasMore: count > rows.length,
          approximate: false,
        },
        buckets: Object.entries(bucketCounts).map(([label, total]) => ({
          label,
          total,
        })),
        rows,
        records: rows,
        actions: [
          action("open_dashboard_with_filter", "Open Loan Dashboard", {
            route: "/loans",
            query: { approvedNotDisbursed: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: [
      "Pending approval cases",
      "Disbursed cases this month",
      "Approved loans without DO",
    ],
  };
};

export const loanPendingApprovalReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const query = {
    $or: [
      { loanStatus: /pending approval|approval pending|not approved/i },
      { status: /pending approval|approval pending|not approved/i },
      { currentStage: /approval/i },
      { approvalStatus: /pending|not approved/i },
    ],
  };
  const {
    rows: loans,
    count,
    approximate,
    error,
  } = await findAndCount(Loan, query, {
    sort: { updatedAt: -1 },
    limit: LIMIT,
  });
  pushModuleTrace(trace, "Loans", count, {
    returned: loans.length,
    approximate,
  });
  return {
    widgets: [
      widget("count_summary", "Pending approval cases", {
        summary: { total: count, modules: [{ module: "Loans", total: count }] },
      }),
      widget("records_table", "Pending approval loan cases", {
        summary: { total: count, approximate, error },
        rows: loanRows(loans),
        actions: [
          action("open_dashboard_with_filter", "Open Loan Dashboard", {
            route: "/loans",
            query: { pendingApproval: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: [
      "Approved but not disbursed cases",
      "Disbursed cases this month",
    ],
  };
};

const businessDateRange = (parsed) => {
  const range = parsed.dateRange || parsed.entities?.dateRange;
  if (range?.start && range?.end)
    return {
      key: range.key || "custom",
      start: new Date(range.start),
      end: new Date(range.end),
    };
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  return { key: "this_month", start, end: now };
};

const rangeQueryForFields = (fields, range) => ({
  $or: fields.map((field) => ({
    [field]: { $gte: range.start, $lte: range.end },
  })),
});

const parseBusinessDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const isWithinRange = (value, range) => {
  const date = parseBusinessDate(value);
  return Boolean(date && date >= range.start && date <= range.end);
};

const isCashLoan = (loan = {}) => {
  if (loan.isCashCase === true) return true;
  const financed = String(
    firstMeaningful(loan.isFinanced, loan.isFinanceRequired),
  )
    .trim()
    .toLowerCase();
  if (["no", "false"].includes(financed)) return true;
  return /cash/i.test(
    String(
      firstMeaningful(
        loan.loanType,
        loan.typeOfLoan,
        loan.caseType,
        loan.loan_type,
      ),
    ),
  );
};

const cashLoanQuery = {
  $or: [
    { isCashCase: true },
    { isFinanced: /^no$/i },
    { isFinanceRequired: /^no$/i },
    { loanType: /cash/i },
    { typeOfLoan: /cash/i },
    { caseType: /cash/i },
    { loan_type: /cash/i },
  ],
};

const disbursedLoanDate = (loan = {}) =>
  firstMeaningful(
    loan.disbursedDate,
    loan.disbursementDate,
    loan.disbursement_date,
    loan.approval_disbursedDate,
    loan.disburse_date,
  );

const loanDisbursedAmount = (loan = {}) => {
  const bankRows = Array.isArray(loan.approval_banksData)
    ? loan.approval_banksData
    : [];
  const bankDisbursed = firstNumber(
    ...bankRows.map(
      (bank) =>
        bank.disbursedAmount ||
        bank.loanAmountDisbursed ||
        bank.amountDisbursed,
    ),
  );
  return firstNumber(
    loan.postfile_loanAmountDisbursed,
    loan.approval_loanAmountDisbursed,
    bankDisbursed,
    loan.approval_loanAmountApproved,
  );
};

const cashDeliveryDate = (loan = {}, deliveryOrder = {}) =>
  firstMeaningful(
    loan.delivery_date,
    loan.deliveryDate,
    loan.delivery_done_at,
    loan.vehicleDeliveryDate,
    deliveryOrder.do_date,
    deliveryOrder.doDate,
    deliveryOrder.do_bookingDate,
  );

const cashBookValue = (loan = {}, deliveryOrder = {}) =>
  firstNumber(
    loan.exShowroomPrice,
    loan.exShowroom,
    deliveryOrder.do_customer_exShowroomPrice,
    deliveryOrder.do_exShowroomPrice,
    deliveryOrder.exShowroomPrice,
  );

const acceptedQuotePremium = (insurance = {}) => {
  const quotes = Array.isArray(insurance.quotes) ? insurance.quotes : [];
  const accepted = quotes.find(
    (quote) =>
      quote.isAccepted ||
      String(quote.id) === String(insurance.acceptedQuoteId),
  );
  return firstNumber(
    accepted?.totalPremium,
    ...quotes.map((quote) => quote.totalPremium),
  );
};

const insuranceBusinessDate = (item = {}) =>
  firstMeaningful(
    item.newIssueDate,
    item.newPolicyStartDate,
    item.policyPurchaseDate,
    item.updatedAt,
  );

const isRenewalInsurance = (item = {}) =>
  item.isRenewal === true ||
  /renewed|renewal/i.test(
    String(
      firstMeaningful(
        item.renewalFollowUpStatus,
        item.source,
        item.sourceOrigin,
        item.usedCarFlowType,
        item.newRemarks,
        item.renewalFollowUpNotes,
      ),
    ),
  );

const isIssuedInsurance = (item = {}) =>
  Boolean(firstMeaningful(item.newPolicyNumber)) ||
  /issued|completed|submitted/i.test(String(item.status)) ||
  Boolean(
    item.acceptedQuoteId &&
    firstMeaningful(item.newIssueDate, item.newPolicyStartDate),
  );

const insurancePremiumAmount = (item = {}) =>
  firstNumber(
    item.newTotalPremium,
    acceptedQuotePremium(item),
    item.customerPaymentExpected,
  );

const businessLoanRow = (loan, access, type, date, amount) => ({
  ...loanReportRow(loan, access),
  businessType: type,
  businessDate: formatDateValue(date),
  amount: access.canViewFinance ? amount : undefined,
  isCashCase: isCashLoan(loan),
});

const insuranceBusinessRow = (item, access) => ({
  id: String(item._id || item.id || ""),
  caseId: item.caseId,
  customer: firstMeaningful(
    item.customerName,
    item.companyName,
    item.contactPersonName,
  ),
  vehicle: [item.vehicleMake, item.vehicleModel, item.vehicleVariant]
    .filter(Boolean)
    .join(" "),
  registrationNumber: item.registrationNumber,
  policyNumber: item.newPolicyNumber,
  status: item.status,
  businessType: isRenewalInsurance(item)
    ? "Insurance renewal"
    : "New insurance",
  businessDate: formatDateValue(insuranceBusinessDate(item)),
  amount: access.canViewFinance ? insurancePremiumAmount(item) : undefined,
  route: `/insurance/edit/${item.caseId || item._id}`,
});

export const loanBusinessReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const range = businessDateRange(parsed);
  const wantsCashOnly = /cash car/.test(parsed.lower);
  const disbursedQuery = {
    $and: [
      { $nor: [cashLoanQuery] },
      rangeQueryForFields(
        [
          "disbursedDate",
          "disbursementDate",
          "disbursement_date",
          "approval_disbursedDate",
          "disburse_date",
        ],
        range,
      ),
    ],
  };
  const {
    rows: disbursedLoans,
    count: disbursedCount,
    approximate: disbursedApprox,
    error: disbursedError,
  } = wantsCashOnly
    ? { rows: [], count: 0, approximate: false, error: undefined }
    : await findAndCount(Loan, disbursedQuery, {
        sort: { disbursement_date: -1, disbursedDate: -1, updatedAt: -1 },
        limit: LIMIT,
      });

  const directCashRows = await findLean(
    Loan,
    {
      $and: [
        cashLoanQuery,
        rangeQueryForFields(
          [
            "delivery_date",
            "deliveryDate",
            "delivery_done_at",
            "vehicleDeliveryDate",
          ],
          range,
        ),
      ],
    },
    { sort: { delivery_date: -1, updatedAt: -1 }, limit: LIMIT * 4 },
  );
  const deliveryOrders = await findLean(
    DeliveryOrder,
    rangeQueryForFields(["do_date", "doDate", "do_bookingDate"], range),
    { sort: { do_date: -1, doDate: -1 }, limit: LIMIT * 4 },
  );
  const deliveryByLoanId = new Map(
    deliveryOrders.map((item) => [
      String(firstMeaningful(item.loanId, item.do_loanId)),
      item,
    ]),
  );
  const fallbackLoanIds = [...deliveryByLoanId.keys()].filter(Boolean);
  const fallbackCashRows = fallbackLoanIds.length
    ? await findLean(
        Loan,
        { $and: [cashLoanQuery, { loanId: { $in: fallbackLoanIds } }] },
        { sort: { updatedAt: -1 }, limit: LIMIT * 4 },
      )
    : [];
  const cashRows = [...directCashRows, ...fallbackCashRows].filter(
    (loan, index, arr) => {
      const key = String(loan._id || loan.loanId || index);
      return (
        arr.findIndex((item) => String(item._id || item.loanId) === key) ===
        index
      );
    },
  );
  const cashRowsInRange = cashRows.filter((loan) =>
    isWithinRange(
      cashDeliveryDate(loan, deliveryByLoanId.get(String(loan.loanId))),
      range,
    ),
  );

  let insuranceRowsForBusiness = [];
  if (!wantsCashOnly && access.canAccess("insurance")) {
    const candidates = await findLean(
      InsuranceCase,
      {
        $or: [
          { newPolicyNumber: { $nin: [null, ""] } },
          { status: /issued|submitted|completed/i },
          { renewalFollowUpStatus: /renewed/i },
          { isRenewal: true },
        ],
      },
      { sort: { updatedAt: -1 }, limit: LIMIT * 8 },
    );
    insuranceRowsForBusiness = candidates.filter(
      (item) =>
        isIssuedInsurance(item) &&
        isWithinRange(insuranceBusinessDate(item), range),
    );
    pushModuleTrace(trace, "Insurance", insuranceRowsForBusiness.length, {
      returned: Math.min(insuranceRowsForBusiness.length, LIMIT),
      dateRange: range.key,
    });
  } else if (!wantsCashOnly)
    noteRestriction(access, "Insurance", "No insurance access");

  pushModuleTrace(trace, "Loans", disbursedCount + cashRowsInRange.length, {
    disbursedCount,
    cashCarCount: cashRowsInRange.length,
    returnedDisbursed: disbursedLoans.length,
    returnedCash: Math.min(cashRowsInRange.length, LIMIT),
    includesCashCars: true,
  });

  const loanDisbursedRecords = disbursedLoans
    .slice(0, LIMIT)
    .map((loan) =>
      businessLoanRow(
        loan,
        access,
        "Loan disbursed",
        disbursedLoanDate(loan),
        loanDisbursedAmount(loan),
      ),
    );
  const cashCarRecords = cashRowsInRange
    .slice(0, LIMIT)
    .map((loan) =>
      businessLoanRow(
        loan,
        access,
        "Cash car delivered",
        cashDeliveryDate(loan, deliveryByLoanId.get(String(loan.loanId))),
        cashBookValue(loan, deliveryByLoanId.get(String(loan.loanId))),
      ),
    );
  const insuranceRecords = insuranceRowsForBusiness
    .slice(0, LIMIT)
    .map((item) => insuranceBusinessRow(item, access));
  const insuranceRenewedCount =
    insuranceRowsForBusiness.filter(isRenewalInsurance).length;
  const insuranceIssuedCount =
    insuranceRowsForBusiness.length - insuranceRenewedCount;
  const loanDisbursedAmountTotal = disbursedLoans.reduce(
    (sum, loan) => sum + loanDisbursedAmount(loan),
    0,
  );
  const cashCarBookValue = cashRowsInRange.reduce(
    (sum, loan) =>
      sum + cashBookValue(loan, deliveryByLoanId.get(String(loan.loanId))),
    0,
  );
  const insurancePremiumAmountTotal = insuranceRowsForBusiness.reduce(
    (sum, item) => sum + insurancePremiumAmount(item),
    0,
  );
  const totalBusinessAmount =
    loanDisbursedAmountTotal + cashCarBookValue + insurancePremiumAmountTotal;
  const sections = wantsCashOnly
    ? [
        {
          key: "cash_cars_delivered_this_month",
          label: "Cash cars delivered this month",
          total: cashRowsInRange.length,
          amount: cashCarBookValue,
        },
      ]
    : [
        {
          key: "loan_disbursed_this_month",
          label: "Loan disbursed business",
          total: disbursedCount,
          amount: loanDisbursedAmountTotal,
        },
        {
          key: "cash_cars_delivered_this_month",
          label: "Cash car business",
          total: cashRowsInRange.length,
          amount: cashCarBookValue,
        },
        {
          key: "insurance_issued_or_renewed_this_month",
          label: "Insurance business",
          total: insuranceRowsForBusiness.length,
          amount: insurancePremiumAmountTotal,
        },
      ];
  const recordsBySection = {
    loan_disbursed_this_month: wantsCashOnly ? [] : loanDisbursedRecords,
    cash_cars_delivered_this_month: cashCarRecords,
    insurance_issued_or_renewed_this_month: wantsCashOnly
      ? []
      : insuranceRecords,
  };
  const records = wantsCashOnly
    ? cashCarRecords
    : [...loanDisbursedRecords, ...cashCarRecords, ...insuranceRecords];
  return {
    widgets: [
      widget("loan_business_report", "Loan business report", {
        total: wantsCashOnly
          ? cashRowsInRange.length
          : disbursedCount +
            cashRowsInRange.length +
            insuranceRowsForBusiness.length,
        amount: access.canViewFinance ? totalBusinessAmount : undefined,
        businessSubtype: wantsCashOnly ? "cash_cars" : "all_business",
        dateRange: {
          key: range.key,
          start: range.start.toISOString(),
          end: range.end.toISOString(),
        },
        summary: {
          totalBusinessAmount: access.canViewFinance
            ? totalBusinessAmount
            : undefined,
          loanDisbursedAmount: access.canViewFinance
            ? loanDisbursedAmountTotal
            : undefined,
          cashCarBookValue: access.canViewFinance
            ? cashCarBookValue
            : undefined,
          insurancePremiumAmount: access.canViewFinance
            ? insurancePremiumAmountTotal
            : undefined,
          loanDisbursedCount: wantsCashOnly ? 0 : disbursedCount,
          cashCarCount: cashRowsInRange.length,
          insuranceIssuedCount: wantsCashOnly ? 0 : insuranceIssuedCount,
          insuranceRenewedCount: wantsCashOnly ? 0 : insuranceRenewedCount,
          totalCases: wantsCashOnly
            ? cashRowsInRange.length
            : disbursedCount +
              cashRowsInRange.length +
              insuranceRowsForBusiness.length,
          approximate: disbursedApprox,
          error: disbursedError,
          cashCarLogic: [
            "isCashCase === true",
            "isFinanced === No",
            "loanType/typeOfLoan/caseType contains Cash",
          ],
          cashCarDateLogic: [
            "delivery_date",
            "deliveryDate",
            "delivery_done_at",
            "vehicleDeliveryDate",
            "deliveryOrders.do_date fallback",
          ],
        },
        sections,
        recordsBySection,
        moduleTables: sections.map((section) => ({
          title: section.label,
          total: section.total,
          amount: section.amount,
          rows: recordsBySection[section.key] || [],
        })),
        rows: records,
        records,
      }),
    ],
    followUpSuggestions: [
      "Cash car business this month",
      "Pending approval cases",
      "Approved but not disbursed cases",
    ],
  };
};

export const loanMissingRegistrationReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const { rows, count, approximate, error } = await findAndCount(
    Loan,
    buildMissingValueQuery([
      "rc_redg_no",
      "registrationNumber",
      "vehicleRegNo",
    ]),
    { sort: { updatedAt: -1 }, limit: LIMIT },
  );
  pushModuleTrace(trace, "Loans", count, {
    returned: rows.length,
    approximate,
  });
  return {
    widgets: [
      widget("records_table", "Loans missing registration", {
        summary: { total: count, approximate, error },
        rows: rows.map((loan) => loanReportRow(loan, access)),
      }),
    ],
  };
};

export const loanInvoiceMissingReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const { rows, count, approximate, error } = await findAndCount(
    Loan,
    buildMissingValueQuery(["invoice_number", "invoice_received_date"]),
    { sort: { updatedAt: -1 }, limit: LIMIT },
  );
  pushModuleTrace(trace, "Loans", count, {
    returned: rows.length,
    approximate,
  });
  return {
    widgets: [
      widget("records_table", "Loans missing invoice", {
        summary: { total: count, approximate, error },
        rows: rows.map((loan) => loanReportRow(loan, access)),
      }),
    ],
  };
};

export const loanInsuranceMissingReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const { rows, count, approximate, error } = await findAndCount(
    Loan,
    buildMissingValueQuery([
      "insurance_company_name",
      "insurance_policy_number",
    ]),
    { sort: { updatedAt: -1 }, limit: LIMIT },
  );
  pushModuleTrace(trace, "Loans", count, {
    returned: rows.length,
    approximate,
  });
  return {
    widgets: [
      widget("records_table", "Loans missing insurance details", {
        summary: { total: count, approximate, error },
        rows: rows.map((loan) => loanReportRow(loan, access)),
      }),
    ],
  };
};

export const loanDisbursedReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [
        unavailableWidget(
          "Loan report unavailable",
          "You do not have access to loan records.",
          ["Loans"],
        ),
      ],
    };
  }
  const range = parsed.dateRange || parsed.entities?.dateRange;

  const dateFilter =
    range?.start && range?.end
      ? rangeQueryForFields(
          [
            "disbursedDate",
            "disbursementDate",
            "disbursement_date",
            "approval_disbursedDate",
            "disburse_date",
          ],
          {
            start: new Date(range.start),
            end: new Date(range.end),
          },
        )
      : {};

  const query = {
    ...dateFilter,
    $or: [
      { loanStatus: /disbursed/i },
      { status: /disbursed/i },
      { currentStage: /disbursed/i },
      { disbursementStatus: /disbursed/i },
      { disburse_amount: { $gt: 0 } },
    ],
  };
  const {
    rows: loans,
    count,
    approximate,
    error,
  } = await findAndCount(Loan, query, {
    sort: { updatedAt: -1 },
    limit: LIMIT,
  });
  pushModuleTrace(trace, "Loans", count, {
    returned: loans.length,
    approximate,
  });
  return {
    widgets: [
      widget("count_summary", "Disbursed loan cases", {
        summary: { total: count, modules: [{ module: "Loans", total: count }] },
      }),
      widget("records_table", "Disbursed loans", {
        summary: { total: count, approximate, error },
        rows: loanRows(loans),
        actions: [
          action("open_dashboard_with_filter", "Open Loan Dashboard", {
            route: "/loans",
            query: { disbursed: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: [
      "Approved but not disbursed cases",
      "Pending approval cases",
    ],
  };
};

export const payoutEnteredReport = async (parsed, access, trace) => {
  if (!access.canAccess("payouts") && !access.canAccess("loans")) {
    noteRestriction(access, "Payouts", "No payout access");
    return {
      widgets: [
        unavailableWidget(
          "Payout report unavailable",
          "You do not have access to payout records.",
          ["Payouts"],
        ),
      ],
    };
  }
  const sections = [];
  if (access.canAccess("loans")) {
    const query = {
      $or: [
        { payout_amount: { $gt: 0 } },
        { payoutAmount: { $gt: 0 } },
        { prefile_sourcePayoutPercentage: { $nin: [null, "", 0] } },
        { payout_percentage: { $nin: [null, "", 0] } },
      ],
    };
    const { rows, count, approximate } = await findAndCount(Loan, query, {
      sort: { updatedAt: -1 },
      limit: LIMIT,
    });
    pushModuleTrace(trace, "Loans", count, {
      returned: rows.length,
      approximate,
    });
    sections.push({ module: "Loans", total: count, rows: loanRows(rows) });
  }
  if (access.canAccess("payouts")) {
    const query = {
      $or: [
        { payout_amount: { $gt: 0 } },
        { net_payout_amount: { $gt: 0 } },
        { payout_status: /entered|received|paid|done/i },
      ],
    };
    const { rows, count, approximate } = await findAndCount(Receivable, query, {
      sort: { updatedAt: -1 },
      limit: LIMIT,
    });
    pushModuleTrace(trace, "Receivables", count, {
      returned: rows.length,
      approximate,
    });
    sections.push({
      module: "Receivables",
      total: count,
      rows: rows.map((item) => ({
        id: String(item._id),
        customer: item.customerName,
        reference: firstMeaningful(
          item.loanId,
          item.insuranceCaseId,
          item.payoutId,
        ),
        status: item.payout_status,
        amount: access.canViewFinance
          ? firstNumber(item.net_payout_amount, item.payout_amount)
          : undefined,
        updatedAt: item.updatedAt,
      })),
    });
  }
  const total = sections.reduce(
    (sum, section) => sum + (Number(section.total) || 0),
    0,
  );
  return {
    widgets: [
      widget("count_summary", "Payout entered count", {
        summary: {
          total,
          modules: sections.map(({ module, total }) => ({ module, total })),
        },
      }),
      widget("records_table", "Cases with payout entered", {
        summary: { total },
        rows: sections.flatMap((section) =>
          section.rows.map((row) => ({ module: section.module, ...row })),
        ),
        actions: [
          action("open_dashboard_with_filter", "Open Payout Dashboard", {
            route: "/payouts/receivables",
            query: { payoutEntered: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: [
      "Cases with payout missing",
      "Open Payout Dashboard",
      "Show finance intelligence",
    ],
  };
};

export const paymentPendingReport = async (parsed, access, trace) => {
  if (!access.canAccess("payments")) {
    noteRestriction(access, "Payments", "No payment access");
    return {
      widgets: [
        unavailableWidget(
          "Payment report unavailable",
          "You do not have access to payment records.",
          ["Payments"],
        ),
      ],
    };
  }
  const query = {
    $or: [
      { status: /pending|partial|due|unpaid/i },
      { paymentStatus: /pending|partial|due|unpaid/i },
      { balance: { $gt: 0 } },
      { balanceAmount: { $gt: 0 } },
      { pendingAmount: { $gt: 0 } },
    ],
  };
  const { rows, count, approximate, error } = await findAndCount(
    Payment,
    query,
    { sort: { updatedAt: -1 }, limit: LIMIT },
  );
  pushModuleTrace(trace, "Payments", count, {
    returned: rows.length,
    approximate,
  });
  return {
    widgets: [
      widget("count_summary", "Payment pending count", {
        summary: {
          total: count,
          modules: [{ module: "Payments", total: count }],
        },
      }),
      widget("records_table", "Payment pending cases", {
        summary: { total: count, approximate, error },
        rows: rows.map((item) => ({
          id: String(item._id),
          customer: firstMeaningful(item.customerName, item.name),
          vehicle: firstMeaningful(item.vehicle, item.vehicleName),
          paymentType: firstMeaningful(item.paymentType, item.type),
          expectedAmount: access.canViewFinance
            ? firstNumber(item.expectedAmount, item.totalAmount)
            : undefined,
          receivedAmount: access.canViewFinance
            ? firstNumber(item.receivedAmount, item.paidAmount)
            : undefined,
          balance: access.canViewFinance
            ? firstNumber(item.balance, item.balanceAmount, item.pendingAmount)
            : undefined,
          status: firstMeaningful(item.status, item.paymentStatus),
          updatedAt: item.updatedAt,
        })),
      }),
    ],
    followUpSuggestions: [
      "Cases with payout missing",
      "Show customer 360",
      "Open payment records",
    ],
  };
};

export const deliveryOrderReport = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Delivery Orders", "No loan/DO access");
    return {
      widgets: [
        unavailableWidget(
          "Delivery order report unavailable",
          "You do not have access to delivery order records.",
          ["Delivery Orders"],
        ),
      ],
    };
  }
  const query = /missing|pending|without|no do|approved/.test(parsed.lower)
    ? {
        $or: [
          { status: /pending|draft|missing/i },
          { doNumber: { $in: [null, ""] } },
          { do_refNo: { $in: [null, ""] } },
        ],
      }
    : {};
  const { rows, count, approximate, error } = await findAndCount(
    DeliveryOrder,
    query,
    { sort: { updatedAt: -1 }, limit: LIMIT },
  );
  pushModuleTrace(trace, "Delivery Orders", count, {
    returned: rows.length,
    approximate,
  });
  return {
    widgets: [
      widget("records_table", "Delivery order records", {
        summary: { total: count, approximate, error },
        rows: rows.map((item) => ({
          id: String(item._id),
          customer: firstMeaningful(item.do_customerName, item.customerName),
          vehicle: [
            firstMeaningful(item.do_vehicleMake, item.vehicleMake),
            firstMeaningful(item.do_vehicleModel, item.vehicleModel),
            firstMeaningful(item.do_vehicleVariant, item.vehicleVariant),
          ]
            .filter(Boolean)
            .join(" "),
          dealer: firstMeaningful(item.do_dealerName, item.dealerName),
          doNumber: firstMeaningful(item.doNumber, item.do_refNo),
          status: item.status,
          doDate: firstMeaningful(item.doDate, item.do_date),
          updatedAt: item.updatedAt,
        })),
        actions: [
          action("open_dashboard_with_filter", "Open Delivery Orders", {
            route: "/loans",
            query: { deliveryOrder: "true" },
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Approved loans without DO", "Open Loan Dashboard"],
  };
};

export const usedCarRcPendingReport = async (parsed, access, trace) => {
  if (!access.canAccess("usedCars")) {
    noteRestriction(access, "Used Cars", "No used-car access");
    return {
      widgets: [
        unavailableWidget(
          "Used-car data unavailable",
          "You do not have access to used-car records.",
          ["Used Cars"],
        ),
      ],
    };
  }
  const query = {
    $or: [
      buildMissingValueQuery(["vehicle.regNo"]),
      { "backgroundCheck.formValues.rcStatus": /pending|missing|not done/i },
      { "backgroundCheck.formValues.challanPending": /pending|yes|true/i },
    ],
  };
  const rows = await findLean(UsedCarLead, query, {
    sort: { updatedAt: -1 },
    limit: LIMIT,
  });
  pushModuleTrace(trace, "Used Cars", rows.length);
  return {
    widgets: [
      widget("records_table", "Used car RC/check pending", {
        rows: rows.map((item) => ({
          id: String(item._id),
          customer: item?.seller?.name,
          vehicle: [
            item?.vehicle?.make,
            item?.vehicle?.model,
            item?.vehicle?.variant,
          ]
            .filter(Boolean)
            .join(" "),
          registrationNumber: item?.vehicle?.regNo,
          status: firstMeaningful(item?.workflow?.status, item.status),
          updatedAt: item.updatedAt,
          route: `/used-cars/leads/${item._id}`,
        })),
      }),
    ],
    followUpSuggestions: [
      "Show vehicle 360",
      "Show records older than 30 days",
    ],
  };
};

export const activeLoanExpiredInsuranceReport = async (
  parsed,
  access,
  trace,
) => {
  if (!access.canAccess("loans") || !access.canAccess("insurance")) {
    if (!access.canAccess("loans"))
      noteRestriction(access, "Loans", "No loan access");
    if (!access.canAccess("insurance"))
      noteRestriction(access, "Insurance", "No insurance access");
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
    findLean(InsuranceCase, expiredQuery, {
      sort: { updatedAt: -1 },
      limit: 250,
    }),
  ]);
  pushModuleTrace(trace, "Loans", loans.length, {
    capped: loans.length >= 150,
  });
  pushModuleTrace(trace, "Insurance", insurance.length);

  const insuranceByReg = new Map();
  const insuranceByCustomer = new Map();
  for (const item of insurance) {
    const reg = String(item.registrationNumber || "")
      .replace(/\W/g, "")
      .toUpperCase();
    if (reg) insuranceByReg.set(reg, item);
    const customer = String(item.customerName || "")
      .toLowerCase()
      .trim();
    if (customer) insuranceByCustomer.set(customer, item);
  }

  const rows = loans
    .map((loan) => {
      const reg = String(
        firstMeaningful(
          loan.registrationNumber,
          loan.vehicleRegNo,
          loan.rc_redg_no,
        ),
      )
        .replace(/\W/g, "")
        .toUpperCase();
      const customer = String(loan.customerName || "")
        .toLowerCase()
        .trim();
      const policy =
        (reg && insuranceByReg.get(reg)) ||
        (customer && insuranceByCustomer.get(customer));
      if (!policy) return null;
      return {
        customer: loan.customerName,
        vehicle: [loan.vehicleMake, loan.vehicleModel, loan.vehicleVariant]
          .filter(Boolean)
          .join(" "),
        registrationNumber: firstMeaningful(
          loan.registrationNumber,
          loan.vehicleRegNo,
          loan.rc_redg_no,
        ),
        loanStatus: firstMeaningful(
          loan.loanStatus,
          loan.status,
          loan.currentStage,
        ),
        insuranceStatus: policy.status,
        insuranceExpiry: firstMeaningful(
          policy.newOdExpiryDate,
          policy.newTpExpiryDate,
        ),
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
    followUpSuggestions: [
      "Show customer 360",
      "Show vehicle 360",
      "Show records older than 30 days",
    ],
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
            {
              newOdExpiryDate: {
                $gte: now.toISOString(),
                $lte: nextWeek.toISOString(),
              },
            },
            {
              newTpExpiryDate: {
                $gte: now.toISOString(),
                $lte: nextWeek.toISOString(),
              },
            },
          ],
        },
        { sort: { newOdExpiryDate: 1, updatedAt: -1 }, limit: 12 },
      ).then((rows) => {
        pushModuleTrace(trace, "Insurance expiring", rows.length);
        return {
          module: "Insurance",
          issue: "Policies expiring in 7 days",
          count: rows.length,
          rows: insuranceRows(rows).map((row) => ({
            ...row,
            issue: "Expiring soon",
          })),
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
            {
              $or: [
                { loanStatus: /approved/i },
                { status: /approved/i },
                { currentStage: /approved/i },
              ],
            },
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
          rows: loanRows(rows).map((row) => ({
            ...row,
            issue: "Approved not disbursed",
          })),
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
            {
              "backgroundCheck.formValues.rcStatus":
                /pending|missing|not done/i,
            },
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
            vehicle: [
              item?.vehicle?.make,
              item?.vehicle?.model,
              item?.vehicle?.variant,
            ]
              .filter(Boolean)
              .join(" "),
            registrationNumber: item?.vehicle?.regNo,
            status: firstMeaningful(
              item?.backgroundCheck?.status,
              item?.workflow?.status,
              item.status,
            ),
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
  const rows = sections.flatMap((section) =>
    section.rows.map((row) => ({ module: section.module, ...row })),
  );
  const total = sections.reduce((sum, section) => sum + section.count, 0);

  return {
    widgets: [
      widget("count_summary", "Operations attention", {
        summary: {
          total,
          modules: sections.map(({ module, issue, count }) => ({
            module,
            issue,
            total: count,
          })),
        },
      }),
      widget("module_breakdown", "Attention by module", {
        modules: sections.map(({ module, issue, count }) => ({
          module,
          label: issue,
          count,
        })),
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
