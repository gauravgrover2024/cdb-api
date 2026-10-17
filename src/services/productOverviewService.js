/**
 * Business Overview data for the Home Loan and Insurance tabs.
 *
 * The car-loan overview lives in loanController (getLoanAnalyticsOverview).
 * These two products return one shared shape so the Home page renders every
 * tab with the same widgets:
 *
 *   {
 *     timeframe: { range, start, end },
 *     kpis:      [{ key, title, value, format, footer, tooltip, drill }],
 *     trend:     [{ bucket, label, created, completed, amount }],
 *     trendLabels: { created, completed },
 *     breakdowns:[{ key, title, rows: [{ key, label, count, amount }] }],
 *     attention: [{ key, title, value, hint, drill }],
 *   }
 *
 * Drill-downs (`drill: { widget, key? }`) are resolved by the matching
 * *Drilldown function with the same filters, so a number and the list
 * behind it always agree.
 */

import HomeLoan from "../models/HomeLoan.js";
import InsuranceCase from "../models/InsuranceCase.js";

const DRILL_LIMIT_MAX = 2000;

// Same presets as the car-loan overview (loanController.parseAnalyticsRange).
export function parseRange(query = {}) {
  const now = new Date();
  const end = new Date(query.to || now);
  if (Number.isNaN(end.getTime())) end.setTime(now.getTime());
  end.setHours(23, 59, 59, 999);

  const range = String(query.range || "mtd").toLowerCase();
  let start;
  if (range === "all") {
    start = new Date(0);
  } else if (range === "1m") {
    start = new Date(end);
    start.setMonth(start.getMonth() - 1);
  } else if (range === "3m") {
    start = new Date(end);
    start.setMonth(start.getMonth() - 3);
  } else if (range === "1y") {
    start = new Date(end);
    start.setFullYear(start.getFullYear() - 1);
  } else if (range === "custom" && query.from) {
    start = new Date(query.from);
  } else {
    start = new Date(end.getFullYear(), end.getMonth(), 1);
  }
  if (Number.isNaN(start?.getTime?.())) start = new Date(end.getFullYear(), end.getMonth(), 1);
  start.setHours(0, 0, 0, 0);
  return { range, start, end };
}

// ─── Small helpers ────────────────────────────────────────────────────────────

const num = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const n = Number(String(value ?? "").replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
};
const firstPositive = (...values) => values.map(num).find((n) => n > 0) || 0;
const text = (value) => String(value ?? "").trim();
const lower = (value) => text(value).toLowerCase();
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const monthBucket = (date) => {
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
};
const monthLabel = (bucket) => {
  const [y, m] = bucket.split("-");
  return `${MONTHS[Number(m) - 1]} ${y}`;
};
const parseDate = (value) => {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
};

// Groups rows into [{ key, label, count, amount }] sorted by count.
function groupRows(rows, keyOf, amountOf = () => 0, { limit = 12, order } = {}) {
  const map = new Map();
  for (const row of rows) {
    const label = keyOf(row);
    if (!label) continue;
    const key = lower(label);
    const entry = map.get(key) || { key, label, count: 0, amount: 0 };
    entry.count += 1;
    entry.amount += amountOf(row);
    map.set(key, entry);
  }
  let out = [...map.values()];
  if (order) {
    out = order.map((o) => map.get(o.key) || { key: o.key, label: o.label, count: 0, amount: 0 })
      .map((row, i) => ({ ...row, label: order[i].label }));
  } else {
    out.sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  }
  return limit ? out.slice(0, limit) : out;
}

function buildTrend(rows, createdAtOf, completedAtOf, amountOf) {
  const map = new Map();
  const touch = (bucket) => {
    if (!map.has(bucket)) map.set(bucket, { bucket, label: monthLabel(bucket), created: 0, completed: 0, amount: 0 });
    return map.get(bucket);
  };
  for (const row of rows) {
    const created = monthBucket(createdAtOf(row));
    if (created) touch(created).created += 1;
    const completedAt = completedAtOf(row);
    // Same rule as the car-loan trend: completions are counted for the files
    // created in the period, bucketed by created month.
    if (created && completedAt) {
      const entry = touch(created);
      entry.completed += 1;
      entry.amount += amountOf(row);
    }
  }
  return [...map.values()].sort((a, b) => a.bucket.localeCompare(b.bucket));
}

const clampLimit = (value) => Math.min(DRILL_LIMIT_MAX, Math.max(1, Number(value) || 500));

// ─── Home Loan ────────────────────────────────────────────────────────────────

const HOME_STAGES = [
  { key: "profile", label: "Customer profile" },
  { key: "prefile", label: "Pre-file" },
  { key: "approval", label: "Approval" },
  { key: "postfile", label: "Post-file" },
  { key: "payout", label: "Payout" },
];

const HOME_PROJECTION = {
  applicationNumber: 1,
  loanId: 1,
  customerName: 1,
  "applicant.primaryMobile": 1,
  typeOfLoan: 1,
  status: 1,
  currentStep: 1,
  isBulk: 1,
  "lead.source": 1,
  "lead.recordSource": 1,
  approval: 1,
  "disbursement.date": 1,
  "disbursement.amount": 1,
  "pricing.netLoan": 1,
  unsecuredLoanAmount: 1,
  "formExtras.propertyType": 1,
  "formExtras.financeExpectation": 1,
  "formExtras.isFinanced": 1,
  "formExtras.currentStage": 1,
  createdAt: 1,
  updatedAt: 1,
};

const bankStatus = (bank) => lower(bank?.status);
const homeBanks = (loan) => (Array.isArray(loan?.approval?.banks) ? loan.approval.banks : []);

function homeDisbursedBank(loan) {
  return homeBanks(loan).find((b) => bankStatus(b) === "disbursed") || null;
}
function homeApprovedBank(loan) {
  return homeBanks(loan).find((b) => ["approved", "accepted", "sanctioned"].includes(bankStatus(b))) || null;
}

export function homeLoanStatus(loan) {
  if (
    lower(loan?.status) === "disbursed" ||
    lower(loan?.approval?.status) === "disbursed" ||
    loan?.approval?.disbursedDate ||
    loan?.disbursement?.date ||
    homeDisbursedBank(loan)
  ) {
    return "disbursed";
  }
  const status = lower(loan?.status);
  if (status === "rejected") return "rejected";
  if (status === "closed") return "completed";
  if (
    status === "approved" ||
    /approv|sanction/.test(lower(loan?.approval?.status)) ||
    homeApprovedBank(loan)
  ) {
    return "approved";
  }
  if (homeBanks(loan).length && homeBanks(loan).every((b) => bankStatus(b) === "rejected")) {
    return "rejected";
  }
  return "pending";
}

export function homeLoanValue(loan) {
  const disbursedBank = homeDisbursedBank(loan);
  const approvedBank = homeApprovedBank(loan);
  return firstPositive(
    loan?.approval?.loanAmountDisbursed,
    loan?.disbursement?.amount,
    disbursedBank?.loanAmount,
    loan?.approval?.loanAmountApproved,
    approvedBank?.loanAmount,
    loan?.unsecuredLoanAmount,
    loan?.pricing?.netLoan,
    loan?.formExtras?.financeExpectation,
  );
}

const homeBankName = (loan) =>
  text(homeDisbursedBank(loan)?.bankName) ||
  text(loan?.approval?.bankName) ||
  text(homeApprovedBank(loan)?.bankName) ||
  text(homeBanks(loan)[0]?.bankName);

const homeStage = (loan) => lower(loan?.currentStep) || lower(loan?.formExtras?.currentStage) || "profile";
const homeSource = (loan) => text(loan?.lead?.source) || text(loan?.lead?.recordSource) || "Direct";
const homeDisbursedAt = (loan) =>
  homeLoanStatus(loan) === "disbursed"
    ? parseDate(loan?.approval?.disbursedDate) ||
      parseDate(loan?.disbursement?.date) ||
      parseDate(homeDisbursedBank(loan)?.disbursedDate || homeDisbursedBank(loan)?.disbursalDate) ||
      parseDate(loan?.updatedAt) ||
      parseDate(loan?.createdAt)
    : null;

async function loadHomeLoans(range) {
  return HomeLoan.find(
    { deletedAt: null, createdAt: { $gte: range.start, $lte: range.end } },
    HOME_PROJECTION,
  )
    .sort({ updatedAt: -1 })
    .lean();
}

// Filter behind every Home Loan number, keyed by `widget` (+ optional key/bucket).
function homeDrillFilter({ widget, key, bucket } = {}) {
  const k = lower(key);
  switch (widget) {
    case "disbursed":
      return (l) => homeLoanStatus(l) === "disbursed";
    case "approved_pending":
      return (l) => homeLoanStatus(l) === "approved";
    case "pending":
      return (l) => homeLoanStatus(l) === "pending";
    case "split_files":
      return (l) => Boolean(l.isBulk);
    case "no_bank":
      return (l) => homeStage(l) !== "profile" && homeStage(l) !== "prefile" && homeBanks(l).length === 0;
    case "trend_created":
      return (l) => monthBucket(l.createdAt) === bucket;
    case "trend_completed":
      return (l) => monthBucket(l.createdAt) === bucket && homeLoanStatus(l) === "disbursed";
    case "stage":
      return (l) => homeStage(l) === k;
    case "status":
      return (l) => homeLoanStatus(l) === k;
    case "bank":
      return (l) => lower(homeBankName(l)) === k;
    case "loan_type":
      return (l) => lower(l.typeOfLoan) === k;
    case "property_type":
      return (l) => lower(l.formExtras?.propertyType) === k;
    case "source":
      return (l) => lower(homeSource(l)) === k;
    default:
      return () => true;
  }
}

export async function getHomeLoanOverview(query = {}) {
  const range = parseRange(query);
  const loans = await loadHomeLoans(range);

  const disbursed = loans.filter((l) => homeLoanStatus(l) === "disbursed");
  const approvedPending = loans.filter((l) => homeLoanStatus(l) === "approved");
  const sum = (rows) => rows.reduce((s, l) => s + homeLoanValue(l), 0);
  const total = loans.length;

  return {
    timeframe: range,
    kpis: [
      { key: "files", title: "Files created", value: total, format: "count",
        footer: `${loans.filter((l) => l.isBulk).length} split from bulk entries`, drill: { widget: "all" } },
      { key: "value", title: "Loan value", value: sum(loans), format: "inr",
        tooltip: "Disbursed amount if disbursed, otherwise approved, requested or expected amount",
        footer: "Across all files in the period" },
      { key: "disbursed", title: "Disbursed", value: sum(disbursed), format: "inr",
        footer: `${disbursed.length} files`, drill: { widget: "disbursed" } },
      { key: "conversion", title: "Disbursal conversion", value: total ? Math.round((disbursed.length / total) * 1000) / 10 : 0,
        format: "percent", tooltip: "Disbursed files ÷ all files", footer: `${disbursed.length} of ${total} files` },
      { key: "approved_pending", title: "Approved, not disbursed", value: approvedPending.length, format: "count",
        footer: `₹${Math.round(sum(approvedPending)).toLocaleString("en-IN")}`, drill: { widget: "approved_pending" } },
      { key: "pending", title: "In progress", value: loans.filter((l) => homeLoanStatus(l) === "pending").length,
        format: "count", footer: "Not yet approved or rejected", drill: { widget: "pending" } },
    ],
    trend: buildTrend(loans, (l) => l.createdAt, homeDisbursedAt, homeLoanValue),
    trendLabels: { created: "Files created", completed: "Disbursed" },
    breakdowns: [
      { key: "stage", title: "Pipeline by stage",
        rows: groupRows(loans, (l) => homeStage(l), () => 0, { limit: 0, order: HOME_STAGES }) },
      { key: "status", title: "Status", rows: groupRows(loans, homeLoanStatus, homeLoanValue, { limit: 0 }) },
      { key: "bank", title: "Banks", rows: groupRows(loans, homeBankName, homeLoanValue) },
      { key: "loan_type", title: "Loan types", rows: groupRows(loans, (l) => text(l.typeOfLoan), homeLoanValue) },
      { key: "property_type", title: "Property types", rows: groupRows(loans, (l) => text(l.formExtras?.propertyType), homeLoanValue) },
      { key: "source", title: "Sources", rows: groupRows(loans, homeSource, homeLoanValue) },
    ],
    attention: [
      { key: "approved_pending", title: "Approved, not disbursed", value: approvedPending.length,
        hint: "Approved by a bank but not yet disbursed", drill: { widget: "approved_pending" } },
      { key: "no_bank", title: "At approval with no bank", value: loans.filter(homeDrillFilter({ widget: "no_bank" })).length,
        hint: "Past pre-file but no bank has been added for comparison", drill: { widget: "no_bank" } },
      { key: "split_files", title: "Split loan files", value: loans.filter((l) => l.isBulk).length,
        hint: "Files created by splitting a multiple-loan entry", drill: { widget: "split_files" } },
    ],
  };
}

export async function getHomeLoanDrilldown(query = {}) {
  const range = parseRange(query);
  const loans = (await loadHomeLoans(range)).filter(homeDrillFilter(query));
  const limit = clampLimit(query.limit);
  return {
    total: loans.length,
    data: loans.slice(0, limit).map((l) => ({
      _id: l._id,
      id: l.applicationNumber || l.loanId,
      customerName: l.customerName,
      mobile: l.applicant?.primaryMobile,
      type: l.typeOfLoan,
      stage: HOME_STAGES.find((s) => s.key === homeStage(l))?.label || homeStage(l),
      status: homeLoanStatus(l),
      bank: homeBankName(l),
      value: homeLoanValue(l),
      createdAt: l.createdAt,
    })),
  };
}

// ─── Insurance ────────────────────────────────────────────────────────────────

const INSURANCE_STATUSES = [
  { key: "draft", label: "Draft" },
  { key: "submitted", label: "Submitted" },
  { key: "issued", label: "Issued" },
  { key: "cancelled", label: "Cancelled" },
];

const INSURANCE_PROJECTION = {
  caseId: 1,
  status: 1,
  customerName: 1,
  companyName: 1,
  mobile: 1,
  vehicleType: 1,
  policyCategory: 1,
  source: 1,
  sourceName: 1,
  showroomName: 1,
  isRenewal: 1,
  vehicleMake: 1,
  vehicleModel: 1,
  registrationNumber: 1,
  newInsuranceCompany: 1,
  newPolicyType: 1,
  newPolicyNumber: 1,
  newTotalPremium: 1,
  newIssueDate: 1,
  newOdExpiryDate: 1,
  newTpExpiryDate: 1,
  acceptedQuoteId: 1,
  "quotes.id": 1,
  "quotes.insuranceCompany": 1,
  "quotes.totalPremium": 1,
  "quotes.isAccepted": 1,
  customerPaymentExpected: 1,
  customerPaymentReceived: 1,
  createdAt: 1,
  updatedAt: 1,
};

const acceptedQuote = (row) => {
  const quotes = Array.isArray(row?.quotes) ? row.quotes : [];
  return (
    quotes.find((q) => q?.isAccepted) ||
    (row?.acceptedQuoteId != null
      ? quotes.find((q) => String(q?.id) === String(row.acceptedQuoteId))
      : null) ||
    null
  );
};

// Issued = marked issued, or a new policy number has been recorded.
export function insuranceStatus(row) {
  const status = lower(row?.status);
  if (status === "cancelled") return "cancelled";
  if (status === "issued" || text(row?.newPolicyNumber)) return "issued";
  if (status === "submitted") return "submitted";
  return "draft";
}

export const insurancePremium = (row) =>
  firstPositive(row?.newTotalPremium, acceptedQuote(row)?.totalPremium);

const insurerName = (row) => text(row?.newInsuranceCompany) || text(acceptedQuote(row)?.insuranceCompany);
const insuranceSource = (row) => text(row?.source) || "Direct";
const insuranceKind = (row) => (row?.isRenewal ? "Renewal" : text(row?.vehicleType) || "New Car");
const insuranceIssuedAt = (row) =>
  insuranceStatus(row) === "issued"
    ? parseDate(row?.newIssueDate) || parseDate(row?.updatedAt) || parseDate(row?.createdAt)
    : null;
const paymentDue = (row) => num(row?.customerPaymentExpected) - num(row?.customerPaymentReceived);

async function loadInsuranceCases(range) {
  return InsuranceCase.find(
    { createdAt: { $gte: range.start, $lte: range.end } },
    INSURANCE_PROJECTION,
  )
    .sort({ updatedAt: -1 })
    .lean();
}

function insuranceDrillFilter({ widget, key, bucket } = {}) {
  const k = lower(key);
  switch (widget) {
    case "issued":
      return (r) => insuranceStatus(r) === "issued";
    case "open":
      return (r) => ["draft", "submitted"].includes(insuranceStatus(r));
    case "renewals":
      return (r) => Boolean(r.isRenewal);
    case "payment_due":
      return (r) => insuranceStatus(r) === "issued" && paymentDue(r) > 0;
    case "issued_no_policy_no":
      return (r) => lower(r.status) === "issued" && !text(r.newPolicyNumber);
    case "no_quote":
      return (r) => insuranceStatus(r) !== "cancelled" && !acceptedQuote(r) && !insurancePremium(r);
    case "trend_created":
      return (r) => monthBucket(r.createdAt) === bucket;
    case "trend_completed":
      return (r) => monthBucket(r.createdAt) === bucket && insuranceStatus(r) === "issued";
    case "status":
      return (r) => insuranceStatus(r) === k;
    case "insurer":
      return (r) => lower(insurerName(r)) === k;
    case "case_type":
      return (r) => lower(insuranceKind(r)) === k;
    case "policy_type":
      return (r) => lower(r.newPolicyType) === k;
    case "source":
      return (r) => lower(insuranceSource(r)) === k;
    default:
      return () => true;
  }
}

export async function getInsuranceOverview(query = {}) {
  const range = parseRange(query);
  const cases = await loadInsuranceCases(range);

  const issued = cases.filter((r) => insuranceStatus(r) === "issued");
  const open = cases.filter((r) => ["draft", "submitted"].includes(insuranceStatus(r)));
  const sumPremium = (rows) => rows.reduce((s, r) => s + insurancePremium(r), 0);
  const due = issued.filter((r) => paymentDue(r) > 0);
  const total = cases.length;

  return {
    timeframe: range,
    kpis: [
      { key: "cases", title: "Cases created", value: total, format: "count",
        footer: `${cases.filter((r) => r.isRenewal).length} renewals`, drill: { widget: "all" } },
      { key: "premium", title: "Premium value", value: sumPremium(cases), format: "inr",
        tooltip: "Policy premium if issued, otherwise the accepted quote", footer: "Across all cases in the period" },
      { key: "issued", title: "Policies issued", value: issued.length, format: "count",
        footer: `₹${Math.round(sumPremium(issued)).toLocaleString("en-IN")} premium`, drill: { widget: "issued" } },
      { key: "conversion", title: "Issue conversion", value: total ? Math.round((issued.length / total) * 1000) / 10 : 0,
        format: "percent", tooltip: "Issued policies ÷ all cases", footer: `${issued.length} of ${total} cases` },
      { key: "open", title: "Open cases", value: open.length, format: "count",
        footer: "Draft or submitted", drill: { widget: "open" } },
      { key: "payment_due", title: "Customer payment due", value: due.reduce((s, r) => s + paymentDue(r), 0),
        format: "inr", footer: `${due.length} issued policies`, drill: { widget: "payment_due" } },
    ],
    trend: buildTrend(cases, (r) => r.createdAt, insuranceIssuedAt, insurancePremium),
    trendLabels: { created: "Cases created", completed: "Issued" },
    breakdowns: [
      { key: "status", title: "Status",
        rows: groupRows(cases, insuranceStatus, insurancePremium, { limit: 0, order: INSURANCE_STATUSES }) },
      { key: "case_type", title: "Case types", rows: groupRows(cases, insuranceKind, insurancePremium) },
      { key: "insurer", title: "Insurers", rows: groupRows(cases, insurerName, insurancePremium) },
      { key: "policy_type", title: "Policy types", rows: groupRows(cases, (r) => text(r.newPolicyType), insurancePremium) },
      { key: "source", title: "Sources", rows: groupRows(cases, insuranceSource, insurancePremium) },
    ],
    attention: [
      { key: "payment_due", title: "Customer payment due", value: due.length,
        hint: "Issued policies where the customer has not paid in full", drill: { widget: "payment_due" } },
      { key: "issued_no_policy_no", title: "Issued without policy no.",
        value: cases.filter(insuranceDrillFilter({ widget: "issued_no_policy_no" })).length,
        hint: "Marked issued but no new policy number recorded", drill: { widget: "issued_no_policy_no" } },
      { key: "no_quote", title: "No accepted quote",
        value: cases.filter(insuranceDrillFilter({ widget: "no_quote" })).length,
        hint: "Open cases without an accepted quote or premium", drill: { widget: "no_quote" } },
    ],
  };
}

export async function getInsuranceDrilldown(query = {}) {
  const range = parseRange(query);
  const cases = (await loadInsuranceCases(range)).filter(insuranceDrillFilter(query));
  const limit = clampLimit(query.limit);
  return {
    total: cases.length,
    data: cases.slice(0, limit).map((r) => ({
      _id: r._id,
      id: r.caseId,
      customerName: r.customerName || r.companyName,
      mobile: r.mobile,
      type: insuranceKind(r),
      stage: r.newPolicyType || "",
      status: insuranceStatus(r),
      bank: insurerName(r),
      vehicle: [r.vehicleMake, r.vehicleModel].filter(Boolean).join(" "),
      registrationNumber: r.registrationNumber,
      value: insurancePremium(r),
      createdAt: r.createdAt,
    })),
  };
}
