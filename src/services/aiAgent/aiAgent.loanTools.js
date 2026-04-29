import Loan from "../../models/Loan.js";
import Payment from "../../models/Payment.js";
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
  getLoanRoute,
  getPaymentRoute,
  LIMIT,
  makeAmbiguity,
  pushModuleTrace,
  rowBase,
  safeId,
} from "./aiAgent.tools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";
import {
  calculateLivePrincipalOutstanding,
  generateRepaymentSchedule,
  getStoredPrincipalOutstanding,
} from "./aiAgent.loanCalc.js";

const loanQuery = (entities) =>
  buildEntityQuery({
    entities,
    customerFields: ["customerName", "primaryMobile", "customerMobile"],
    registrationFields: ["registrationNumber", "vehicleRegNo", "rc_redg_no"],
    vehicleFields: ["vehicleMake", "vehicleModel", "vehicleVariant"],
  });

export const findLoans = async (parsed, access, trace, limit = LIMIT) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return [];
  }
  const query = loanQuery(parsed.entities);
  if (!Object.keys(query).length) return [];
  const records = await Loan.find(query).sort({ updatedAt: -1 }).limit(limit).lean();
  pushModuleTrace(trace, "Loans", records.length);
  return records;
};

const loanCardData = (loan, access) => {
  const finance = access.canViewFinance;
  const restrictedFields = finance ? [] : ["loanAmount", "disbursedAmount", "emi", "relatedPayments"];
  return {
    id: safeId(loan),
    loanId: loan.loanId,
    customer: loan.customerName,
    vehicle: getVehicleName(loan),
    registrationNumber: getRegistration(loan),
    loanBank: firstMeaningful(loan.postfile_bankName, loan.approval_bankName, loan.disburse_bankName),
    loanAmount: finance ? firstNumber(loan.postfile_loanAmountApproved, loan.approval_loanAmountApproved) : undefined,
    disbursedAmount: finance
      ? firstNumber(loan.postfile_loanAmountDisbursed, loan.approval_loanAmountDisbursed, loan.disburse_amount)
      : undefined,
    emi: finance ? firstNumber(loan.postfile_emiAmount, loan.emiAmount) : undefined,
    tenure: firstMeaningful(loan.postfile_tenureMonths, loan.approval_tenureMonths),
    status: firstMeaningful(loan.loanStatus, loan.status, loan.currentStage),
    restrictedFields,
  };
};

const chooseLoan = (records) =>
  [...records].sort(
    (a, b) =>
      (latestDate(b.updatedAt, b.createdAt)?.getTime() || 0) -
      (latestDate(a.updatedAt, a.createdAt)?.getTime() || 0),
  )[0];

const closureBreakdown = (loan, access) => {
  const disbursedAmount = firstNumber(
    loan.postfile_disbursedLoan,
    loan.postfile_loanAmountDisbursed,
    loan.approval_loanAmountDisbursed,
    loan.disburse_amount,
  );
  const interestRate = firstNumber(loan.postfile_roi, loan.approval_roi, loan.roi);
  const tenureMonths = firstNumber(loan.postfile_tenureMonths, loan.approval_tenureMonths, loan.tenureMonths);
  const firstEmiDate = firstMeaningful(
    loan.postfile_firstEmiDate,
    loan.firstEmiDate,
    loan.disbursementDate,
    loan.approval_disbursedDate,
  );
  const stored = getStoredPrincipalOutstanding(loan);
  const derived =
    !stored && disbursedAmount && tenureMonths
      ? calculateLivePrincipalOutstanding(disbursedAmount, interestRate, tenureMonths, firstEmiDate)
      : null;
  const outstandingPrincipal = stored || derived?.outstanding || 0;
  const schedule =
    disbursedAmount && tenureMonths
      ? generateRepaymentSchedule(disbursedAmount, interestRate, tenureMonths, firstEmiDate)
      : [];
  const paidMonths = Math.min(derived?.monthsElapsed || 0, schedule.length);
  const lastPaidRow = paidMonths > 0 ? schedule[paidMonths - 1] : null;
  const today = new Date();
  const lastDate = lastPaidRow?.date ? new Date(lastPaidRow.date) : null;
  const diffMs = lastDate ? today.setHours(0, 0, 0, 0) - lastDate.setHours(0, 0, 0, 0) : 0;
  const daysAfterLastPaidEmi = Number.isNaN(diffMs) || diffMs <= 0 ? 0 : Math.floor(diffMs / 86400000);
  const perDayInterest = outstandingPrincipal ? (outstandingPrincipal * interestRate) / 100 / 365 : 0;
  const missingFields = [];
  if (!outstandingPrincipal) missingFields.push("principal outstanding");
  if (!disbursedAmount) missingFields.push("disbursed amount");
  if (!tenureMonths) missingFields.push("tenure");
  if (!interestRate) missingFields.push("interest rate");

  return {
    approxClosure: access.canViewFinance && outstandingPrincipal ? outstandingPrincipal : undefined,
    calculationBreakdown: access.canViewFinance
      ? {
          source: stored ? "Stored Live POS/current outstanding field" : "Live POS schedule fallback",
          outstandingPrincipal,
          perDayInterest,
          daysAfterLastPaidEmi,
          daysInterest: perDayInterest * daysAfterLastPaidEmi,
          preClosureCharge: 0,
          totalClosure: outstandingPrincipal + perDayInterest * daysAfterLastPaidEmi,
          note: "Pre-closure charge percentage is user-entered in the existing Live POS UI; backend returns a zero-charge estimate unless that value is supplied later.",
        }
      : undefined,
    missingFields,
  };
};

export const loanStatusOrClosure = async (parsed, access, trace) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return {
      widgets: [unavailableWidget("Loan unavailable", "You do not have access to loan records.", ["Loans"])],
      followUpSuggestions: [],
    };
  }
  if (!canSearchByEntity(parsed.entities)) {
    return {
      widgets: [
        unavailableWidget(
          "Need a customer or vehicle",
          "Share a customer name, registration number, last 4 digits, or model to find the loan.",
          ["Loans"],
        ),
      ],
      followUpSuggestions: [],
    };
  }
  const records = await findLoans(parsed, access, trace, 20);
  if (!records.length) {
    return {
      widgets: [unavailableWidget("No loan found", "No matching loan record was found in live data.", ["Loans"])],
      followUpSuggestions: ["Show customer 360", "Show vehicle 360"],
    };
  }
  const uniqueRegistrations = new Set(records.map((item) => getRegistration(item)).filter(Boolean));
  if (!parsed.selectedEntity && records.length > 1 && uniqueRegistrations.size > 1 && parsed.entities.last4) {
    return {
      ambiguity: makeAmbiguity(records.map((item) => entityOption(item, "Loans", "loan"))),
      widgets: [],
      followUpSuggestions: [],
    };
  }
  const loan = chooseLoan(records);
  const payment = await Payment.findOne({ loanId: loan.loanId }).lean();
  pushModuleTrace(trace, "Payments", payment ? 1 : 0);
  const data = loanCardData(loan, access);
  const actions = [
    action("open_record", "Open Loan", { route: getLoanRoute(loan) }),
    access.canEdit
      ? action("edit_record", "Edit", { route: getLoanRoute(loan) })
      : disabledAction("edit_record", "Edit", "You do not have edit access"),
    action("open_dashboard_with_filter", "Open Payment Records", { route: getPaymentRoute(loan) }),
  ];
  if (parsed.intent === "loan_closure") {
    const closure = closureBreakdown(loan, access);
    return {
      widgets: [
        widget("loan_closure_card", "Loan closure estimate", {
          data: {
            ...data,
            ...closure,
            relatedPayments: access.canViewFinance ? payment : undefined,
          },
          restrictedFields: data.restrictedFields,
          notices: closure.missingFields.length
            ? [`Exact closure could not be fully calculated because ${closure.missingFields.join(", ")} is missing.`]
            : ["Final settlement remains subject to lender confirmation."],
          actions: [
            ...actions,
            action("open_live_pos", "Open Live POS", { route: getLoanRoute(loan) }),
          ],
        }),
      ],
      followUpSuggestions: ["Open Live POS", "Show payment records", "Show customer 360", "Show vehicle 360"],
    };
  }
  return {
    widgets: [
      widget("loan_case_card", "Loan status", {
        data: {
          ...data,
          relatedPayments: access.canViewFinance ? payment : undefined,
          lastUpdated: formatDateValue(loan.updatedAt),
        },
        restrictedFields: data.restrictedFields,
        actions,
      }),
    ],
    followUpSuggestions: ["Approx loan closure", "Show payment records", "Show customer 360", "Show vehicle 360"],
  };
};

export const loanRows = (records) =>
  records.map((item) => ({
    ...rowBase(item),
    loanId: item.loanId,
    bank: firstMeaningful(item.postfile_bankName, item.approval_bankName, item.disburse_bankName),
    route: getLoanRoute(item),
  }));
