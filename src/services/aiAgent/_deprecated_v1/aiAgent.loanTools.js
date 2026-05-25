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
  findLean,
  getLoanRoute,
  getPaymentRoute,
  LIMIT,
  makeAmbiguity,
  objectIdOrNull,
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

const loanQuery = (entities) => {
  if (entities.loanId) return { loanId: entities.loanId };
  if (entities.mobile) return { $or: [{ primaryMobile: entities.mobile }, { customerMobile: entities.mobile }, { mobileNo: entities.mobile }] };
  return buildEntityQuery({
    entities,
    customerFields: ["customerName", "primaryMobile", "customerMobile"],
    registrationFields: ["registrationNumber", "vehicleRegNo", "rc_redg_no"],
    vehicleFields: ["vehicleMake", "vehicleModel", "vehicleVariant"],
  });
};

export const findLoans = async (parsed, access, trace, limit = LIMIT) => {
  if (!access.canAccess("loans")) {
    noteRestriction(access, "Loans", "No loan access");
    return [];
  }
  const selectedId = parsed.selectedEntity?.entityType === "loan" ? objectIdOrNull(parsed.selectedEntity?.id) : null;
  if (selectedId) {
    const record = await Loan.findOne({ _id: selectedId }).maxTimeMS(2500).lean();
    pushModuleTrace(trace, "Loans", record ? 1 : 0, { selectedEntity: true });
    return record ? [record] : [];
  }
  const query = loanQuery(parsed.entities);
  if (!Object.keys(query).length) return [];
  const records = await findLean(Loan, query, { sort: { updatedAt: -1 }, limit });
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

const chooseSelectedLoan = (records, parsed) => {
  const selectedId = parsed.selectedEntity?.id;
  if (selectedId) {
    const exact = records.find((record) => safeId(record) === String(selectedId));
    if (exact) return exact;
  }
  const selectedRegistration = parsed.selectedEntity?.registrationNumber || parsed.selectedEntity?.context?.registrationNumber;
  if (selectedRegistration) {
    const normalized = String(selectedRegistration).replace(/[^A-Z0-9]/gi, "").toUpperCase();
    const exact = records.find((record) => String(getRegistration(record)).replace(/[^A-Z0-9]/gi, "").toUpperCase() === normalized);
    if (exact) return exact;
  }
  return chooseLoan(records);
};

const closureBreakdown = (loan, access) => {
  const disbursedAmount = firstNumber(
    loan.postfile_loanAmountDisbursed,
    loan.postfile_loanAmountApproved,
    loan.approval_loanAmountDisbursed,
    loan.approval_loanAmountApproved,
  );
  const interestRate = firstNumber(loan.postfile_roi, loan.approval_roi, loan.roi);
  const tenureMonths = firstNumber(loan.postfile_tenureMonths, loan.approval_tenureMonths, loan.tenureMonths);
  const firstEmiDate = firstMeaningful(
    loan.postfile_firstEmiDate,
    loan.disbursement_date,
    loan.disbursementDate,
    loan.disbursedDate,
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
  const todayStart = new Date(today);
  todayStart.setHours(0, 0, 0, 0);

  const lastDateStart = lastDate ? new Date(lastDate) : null;
  if (lastDateStart) lastDateStart.setHours(0, 0, 0, 0);

  const diffMs = lastDateStart
    ? todayStart.getTime() - lastDateStart.getTime()
    : 0;
  const daysAfterLastPaidEmi = Number.isNaN(diffMs) || diffMs <= 0 ? 0 : Math.floor(diffMs / 86400000);
  const perDayInterest = outstandingPrincipal ? (outstandingPrincipal * interestRate) / 100 / 365 : 0;
  const missingFields = [];
  if (!outstandingPrincipal) missingFields.push("principal outstanding");
  if (!disbursedAmount) missingFields.push("disbursed amount");
  if (!tenureMonths) missingFields.push("tenure");
  if (!interestRate) missingFields.push("interest rate");

  return {
    principal: disbursedAmount,
    roi: interestRate,
    tenureMonths,
    firstEmiDate: formatDateValue(firstEmiDate),
    emi: derived?.emi || (schedule[0]?.emi ?? 0),
    monthsElapsed: derived?.monthsElapsed || 0,
    monthsRemaining: derived?.monthsRemaining || tenureMonths,
    approxClosure: access.canViewFinance && outstandingPrincipal ? outstandingPrincipal : undefined,
    calculationBreakdown: access.canViewFinance
      ? {
          source: stored ? "Stored Live POS/current outstanding field" : "Live POS schedule fallback",
          outstandingPrincipal,
          emi: derived?.emi || (schedule[0]?.emi ?? 0),
          monthsElapsed: derived?.monthsElapsed || 0,
          monthsRemaining: derived?.monthsRemaining || tenureMonths,
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
  const loan = chooseSelectedLoan(records, parsed);
  const payment = access.canViewFinance
    ? await Payment.findOne({ loanId: loan.loanId }).maxTimeMS(2500).lean()
    : null;
  pushModuleTrace(trace, "Payments", payment ? 1 : 0);
  const data = loanCardData(loan, access);
  const actions = [
    action("open_record", "Open Loan", { route: getLoanRoute(loan) }),
    access.canEdit
      ? action("edit_record", "Edit", { route: getLoanRoute(loan) })
      : disabledAction("edit_record", "Edit", "You do not have edit access"),
    action("open_dashboard_with_filter", "Open Payment Records", { route: getPaymentRoute(loan) }),
  ];
  if (parsed.intent === "loan_closure" || parsed.intent === "loan_closure_pos") {
    const closure = closureBreakdown(loan, access);
    return {
      widgets: [
        widget("loan_closure_card", "Loan closure estimate", {
          data: {
            ...data,
            ...closure,
            calculationDate: new Date().toISOString(),
            relatedPayments: access.canViewFinance ? payment : undefined,
          },
          restrictedFields: data.restrictedFields,
          notices: closure.missingFields.length
            ? [`Exact closure could not be fully calculated because ${closure.missingFields.join(", ")} is missing.`]
            : ["Outstanding is calculated assuming all EMIs were paid on time. Actual bank foreclosure may vary."],
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
