/**
 * Payout Engine
 *
 * Configurable payout calculation for multi-year policies, switchable
 * between two modes without touching policy/tenure/premium logic:
 *
 *  - "yearly":  the total payout percentage is split evenly across the
 *               tenure years (auto-calculated from however many years the
 *               policy runs — e.g. a 3-year policy pays 1/3 of the total %
 *               each year, a 2-year policy pays 1/2 each year).
 *  - "lumpsum": the full total payout percentage is paid once, at issuance.
 *
 * Independent of premium collection (handled by the existing paymentHistory
 * flow) and independent of renewal logic (policyTenureService.js).
 */

export const PAYOUT_MODES = { YEARLY: "yearly", LUMPSUM: "lumpsum" };

const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

const toPositiveYears = (value) => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.round(n) : 1;
};

/**
 * Build the payout schedule for a policy.
 *
 * @param {object} params
 * @param {"yearly"|"lumpsum"} params.mode
 * @param {number} params.tenureYears - years to divide the payout across (yearly mode)
 * @param {number} params.totalPayoutPercentage - the full entitlement, e.g. 10 (%)
 * @param {number} params.baseAmount - premium/OD amount the percentage applies to
 * @param {Date|string} [params.policyStartDate] - anchors due dates
 * @returns {object} payoutSchedule sub-document shape
 */
export const generatePayoutSchedule = ({
  mode = PAYOUT_MODES.LUMPSUM,
  tenureYears = 1,
  totalPayoutPercentage = 0,
  yearlyPercentages = null,
  baseAmount = 0,
  policyStartDate = null,
} = {}) => {
  const years = toPositiveYears(tenureYears);
  const totalPct = Number(totalPayoutPercentage) || 0;
  const base = Number(baseAmount) || 0;
  const start = policyStartDate ? new Date(policyStartDate) : new Date();
  const normalizedMode = mode === PAYOUT_MODES.YEARLY ? PAYOUT_MODES.YEARLY : PAYOUT_MODES.LUMPSUM;

  if (normalizedMode === PAYOUT_MODES.YEARLY) {
    const configuredPercentages = Array.isArray(yearlyPercentages)
      ? yearlyPercentages
          .slice(0, years)
          .map((value) => round2(Math.max(0, Number(value) || 0)))
      : [];
    const hasConfiguredPercentages = configuredPercentages.length === years;
    const perYearPct = round2(totalPct / years);
    const percentages = hasConfiguredPercentages
      ? configuredPercentages
      : Array.from({ length: years }, () => perYearPct);
    const resolvedTotalPct = hasConfiguredPercentages
      ? round2(percentages.reduce((sum, value) => sum + value, 0))
      : totalPct;
    const entries = Array.from({ length: years }, (_, idx) => {
      const dueDate = new Date(start);
      dueDate.setFullYear(dueDate.getFullYear() + idx);
      const percentage = percentages[idx];
      return {
        policyYear: idx + 1,
        percentage,
        baseAmount: base,
        amount: round2((base * percentage) / 100),
        status: "Pending",
        dueDate,
        paidDate: null,
      };
    });
    return {
      mode: PAYOUT_MODES.YEARLY,
      tenureYears: years,
      totalPayoutPercentage: resolvedTotalPct,
      baseAmount: base,
      entries,
    };
  }

  return {
    mode: PAYOUT_MODES.LUMPSUM,
    tenureYears: years,
    totalPayoutPercentage: totalPct,
    baseAmount: base,
    entries: [
      {
        policyYear: 0, // 0 = upfront, paid at issuance
        percentage: totalPct,
        baseAmount: base,
        amount: round2((base * totalPct) / 100),
        status: "Pending",
        dueDate: start,
        paidDate: null,
      },
    ],
  };
};

export const getPayoutEntryForYear = (payoutSchedule = {}, policyYear) =>
  (payoutSchedule.entries || []).find(
    (entry) => Number(entry.policyYear) === Number(policyYear),
  ) || null;

export const markPayoutEntryStatus = (
  payoutSchedule = {},
  policyYear,
  status,
  paidDate = new Date(),
) => {
  const entries = (payoutSchedule.entries || []).map((entry) => {
    if (Number(entry.policyYear) !== Number(policyYear)) return entry;
    return {
      ...entry,
      status,
      paidDate: status === "Paid" ? new Date(paidDate) : entry.paidDate,
    };
  });
  return { ...payoutSchedule, entries };
};

export const sumPayoutSchedule = (payoutSchedule = {}) =>
  (payoutSchedule.entries || []).reduce(
    (sum, entry) => sum + (Number(entry.amount) || 0),
    0,
  );
