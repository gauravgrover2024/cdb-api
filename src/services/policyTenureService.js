/**
 * Policy Tenure Service
 *
 * Generic multi-year policy tenure handling (1+1, 1+3, 3+3, 5+5, ...).
 * Owns: parsing "<N>yr OD + <M>yr TP" duration strings into structured
 * tenure, tracking which policy year is currently active, deciding whether
 * an OD renewal should fire, and resolving the IDV for a given policy year.
 *
 * Deliberately independent of premium collection and payout calculation —
 * those live in payoutEngine.js and the existing payment-history flow.
 */

const DURATION_PATTERN = /(\d+)\s*yr\s*OD\s*\+\s*(\d+)\s*yr\s*TP/i;
const SINGLE_YEAR_PATTERN = /(\d+)\s*yr/i;

const toYear = (value, fallback = 1) => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.round(n) : fallback;
};

/**
 * Parse a free-text duration string (e.g. "3yr OD + 3yr TP") into tenure
 * years. Falls back to a single "<N>yr" match, then to 1+1.
 */
export const parseTenureFromDuration = (durationStr = "") => {
  const str = String(durationStr || "");
  const match = str.match(DURATION_PATTERN);
  if (match) {
    return { odTenureYears: toYear(match[1]), tpTenureYears: toYear(match[2]) };
  }
  const single = str.match(SINGLE_YEAR_PATTERN);
  const years = single ? toYear(single[1]) : 1;
  return { odTenureYears: years, tpTenureYears: years };
};

export const buildProductType = (odTenureYears, tpTenureYears) =>
  `${toYear(odTenureYears)}+${toYear(tpTenureYears)}`;

export const isMultiYearOd = (odTenureYears) => toYear(odTenureYears) > 1;

/**
 * Build a policyTenure sub-document from a duration string + start date.
 * Preserves currentPolicyYear from the existing tenure if provided.
 */
export const buildPolicyTenure = ({
  durationStr,
  policyStartDate,
  existingTenure = {},
} = {}) => {
  const { odTenureYears, tpTenureYears } = parseTenureFromDuration(durationStr);
  return {
    productType: buildProductType(odTenureYears, tpTenureYears),
    odTenureYears,
    tpTenureYears,
    policyStartDate: policyStartDate
      ? new Date(policyStartDate)
      : existingTenure.policyStartDate || null,
    currentPolicyYear: toYear(existingTenure.currentPolicyYear, 1),
    isMultiYearOd: isMultiYearOd(odTenureYears),
  };
};

/**
 * Which policy year (1-indexed) is active as of a given date, clamped to
 * [1, odTenureYears]. Used to select the active IDV and to gate renewal.
 */
export const computeCurrentPolicyYear = (
  policyStartDate,
  odTenureYears = 1,
  asOfDate = new Date(),
) => {
  const years = toYear(odTenureYears, 1);
  if (!policyStartDate) return 1;
  const start = new Date(policyStartDate);
  const asOf = new Date(asOfDate);
  if (Number.isNaN(start.getTime()) || Number.isNaN(asOf.getTime())) return 1;

  const MS_PER_YEAR = 365.25 * 24 * 60 * 60 * 1000;
  const elapsedYears = Math.floor((asOf.getTime() - start.getTime()) / MS_PER_YEAR);
  const year = Math.max(elapsedYears, 0) + 1;
  return Math.min(Math.max(year, 1), Math.max(years, 1));
};

/**
 * OD renewal should NOT trigger while still inside a multi-year OD tenure.
 * Once the tenure has fully elapsed, normal annual renewal resumes.
 *
 * Deliberately does NOT reuse computeCurrentPolicyYear() here — that helper
 * clamps to [1, odTenureYears] for IDV-lookup purposes, which would make
 * "elapsed > odTenureYears" impossible to detect.
 */
export const shouldTriggerOdRenewal = (policyTenure = {}, asOfDate = new Date()) => {
  const odTenureYears = toYear(policyTenure.odTenureYears, 1);
  if (odTenureYears <= 1) return true; // normal annual flow, no change
  if (!policyTenure.policyStartDate) return true; // nothing to gate against yet

  const start = new Date(policyTenure.policyStartDate);
  const asOf = new Date(asOfDate);
  if (Number.isNaN(start.getTime()) || Number.isNaN(asOf.getTime())) return true;

  const MS_PER_YEAR = 365.25 * 24 * 60 * 60 * 1000;
  const elapsedYears = Math.floor((asOf.getTime() - start.getTime()) / MS_PER_YEAR);
  return elapsedYears >= odTenureYears;
};

/** Look up the IDV schedule entry for a given policy year. */
export const getIdvForPolicyYear = (yearlyIdvSchedule = [], policyYear = 1) => {
  const list = Array.isArray(yearlyIdvSchedule) ? yearlyIdvSchedule : [];
  return (
    list.find((row) => toYear(row.policyYear) === toYear(policyYear)) ||
    list[0] ||
    null
  );
};

/**
 * Resolve the IDV that should be used for a claim: the active policy year
 * as of the claim date, and its corresponding IDV schedule entry.
 */
export const resolveClaimIdv = (insuranceCase = {}, claimDate = new Date()) => {
  const tenure = insuranceCase.policyTenure || {};
  const policyYear = computeCurrentPolicyYear(
    tenure.policyStartDate,
    tenure.odTenureYears,
    claimDate,
  );
  return {
    policyYear,
    idv: getIdvForPolicyYear(insuranceCase.yearlyIdvSchedule, policyYear),
  };
};

/**
 * Default per-year IDV schedule seed: replicates year 1's IDV across every
 * year with no assumed depreciation rate — purely a starting point the user
 * fills in per year in the UI.
 */
export const buildDefaultYearlyIdvSchedule = ({
  odTenureYears = 1,
  vehicleIdv = 0,
  cngIdv = 0,
  accessoriesIdv = 0,
} = {}) => {
  const years = toYear(odTenureYears, 1);
  const vIdv = Number(vehicleIdv) || 0;
  const cIdv = Number(cngIdv) || 0;
  const aIdv = Number(accessoriesIdv) || 0;
  return Array.from({ length: years }, (_, idx) => ({
    policyYear: idx + 1,
    vehicleIdv: vIdv,
    cngIdv: cIdv,
    accessoriesIdv: aIdv,
    totalIdv: vIdv + cIdv + aIdv,
  }));
};
