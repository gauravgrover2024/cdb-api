import Receivable from "../models/Receivable.js";

const safeArray = (value) => (Array.isArray(value) ? value : []);

const normalizeDirection = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const normalizeType = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const inferReceivableKind = (row = {}) => {
  const explicitKind = String(row?.receivableKind || "")
    .trim()
    .toLowerCase();
  if (explicitKind) return explicitKind;

  const payoutType = normalizeType(row?.payout_type);
  if (payoutType.includes("commission")) return "commission";
  if (payoutType.includes("insurance")) return "insurance";
  return "loan";
};

const isLikelyReceivableFromLegacyRow = (row = {}) => {
  const direction = normalizeDirection(row?.payout_direction);
  if (direction) return direction === "receivable";

  const payoutId = String(row?.payoutId || row?.id || "")
    .trim()
    .toUpperCase();
  if (payoutId.startsWith("PR-")) return true;
  if (payoutId.startsWith("PP-")) return false;

  const payoutType = normalizeType(row?.payout_type);
  if (["bank", "insurance", "commission"].includes(payoutType)) return true;

  return true;
};

const parsePercent = (value) => {
  const cleaned = String(value ?? "")
    .replace("%", "")
    .trim();
  const number = Number(cleaned);
  return Number.isFinite(number) ? number : 0;
};

const firstValidDate = (...values) => {
  for (const value of values) {
    if (!value) continue;
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) return date.toISOString();
  }
  return null;
};

const getOrDerivePayoutId = (row = {}, fallbackPrefix = "PR-AUTO") => {
  const existing = String(row?.payoutId || row?.id || "")
    .trim()
    .toUpperCase();
  if (existing) return existing;
  return `${fallbackPrefix}-${Date.now()}`;
};

const buildDerivedDisbursedBankReceivable = (loan = {}, existingRows = []) => {
  const hasBankReceivable = safeArray(existingRows).some((row) => {
    const type = normalizeType(row?.payout_type);
    const direction = normalizeDirection(row?.payout_direction || "receivable");
    return type === "bank" && direction === "receivable";
  });
  if (hasBankReceivable) return null;

  const disbursedBank = safeArray(loan?.approval_banksData).find((bank) => {
    const status = String(bank?.status || "")
      .trim()
      .toLowerCase();
    return status === "disbursed";
  });
  if (!disbursedBank) return null;

  const payoutPercent = parsePercent(disbursedBank?.payoutPercent);
  if (!(payoutPercent > 0)) return null;

  const disbursedAmount = Number(
    disbursedBank?.disbursedAmount || disbursedBank?.loanAmount || 0,
  );
  if (!(Number.isFinite(disbursedAmount) && disbursedAmount > 0)) return null;

  const payoutAmount = Number(
    ((disbursedAmount * payoutPercent) / 100).toFixed(2),
  );
  if (!(payoutAmount > 0)) return null;

  const tdsPercentage = 5;
  const tdsAmount = Number(((payoutAmount * tdsPercentage) / 100).toFixed(2));
  const loanIdToken = String(loan?.loanId || loan?._id || "")
    .trim()
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
  const payoutId = `PR-BANK-${loanIdToken}`;
  const createdAt = firstValidDate(
    disbursedBank?.disbursedDate,
    loan?.disbursement_date,
    loan?.approval_disbursedDate,
    loan?.updatedAt,
    loan?.createdAt,
  );

  return {
    id: payoutId,
    payoutId,
    payout_createdAt: createdAt,
    created_date: createdAt,
    payout_applicable: "Yes",
    payout_type: "Bank",
    payout_party_name:
      disbursedBank?.bankName ||
      loan?.disburse_bankName ||
      loan?.approval_bankName ||
      "Bank",
    payout_percentage: String(disbursedBank?.payoutPercent || payoutPercent),
    payout_amount: payoutAmount,
    payout_direction: "Receivable",
    tds_applicable: "Yes",
    tds_percentage: tdsPercentage,
    tds_amount: tdsAmount,
    net_payout_amount: Number((payoutAmount - tdsAmount).toFixed(2)),
    payout_status: "Expected",
    payout_expected_date: null,
    payout_received_date: null,
    payment_history: [],
    activity_log: [],
    payout_remarks: "Auto-generated from disbursed bank payoutPercent.",
    meta_source: "loan_disbursed_bank_payout_percent",
  };
};

export const collectReceivableRowsFromLoan = (loan = {}) => {
  const strictKeys = ["loan_receivables", "loanReceivables", "receivables"];
  const legacyKeys = ["loan_payouts"];
  const collected = [];

  strictKeys.forEach((key) => {
    safeArray(loan?.[key]).forEach((row) => {
      if (!row || typeof row !== "object") return;
      collected.push({ ...row, __sourceArrayKey: key });
    });
  });

  legacyKeys.forEach((key) => {
    safeArray(loan?.[key]).forEach((row) => {
      if (!row || typeof row !== "object") return;
      if (!isLikelyReceivableFromLegacyRow(row)) return;
      collected.push({ ...row, __sourceArrayKey: key });
    });
  });

  const derivedBankRow = buildDerivedDisbursedBankReceivable(loan, collected);
  if (derivedBankRow) {
    collected.push({ ...derivedBankRow, __sourceArrayKey: "loan_receivables" });
  }

  const dedup = new Map();
  for (const row of collected) {
    const payoutId = getOrDerivePayoutId(row);
    if (!payoutId) continue;
    if (!dedup.has(payoutId)) dedup.set(payoutId, { ...row, payoutId });
  }
  return Array.from(dedup.values());
};

const toReceivableDocPayload = (loan, row) => {
  const payoutId = getOrDerivePayoutId(row);
  const paymentHistory = safeArray(row?.payment_history);
  const activityLog = safeArray(row?.activity_log);
  const sourceArrayKey = String(row?.__sourceArrayKey || "loan_receivables");

  return {
    receivableKind: inferReceivableKind(row),
    sourceModule: "loan",
    loanId: String(loan?.loanId || "").trim(),
    loanMongoId: loan?._id || null,
    customerName: String(loan?.customerName || "").trim(),
    payoutId,
    sourceArrayKey,
    payload: { ...row, payoutId, id: row?.id || payoutId },

    payout_type: String(row?.payout_type || "").trim(),
    payout_party_name: String(row?.payout_party_name || "").trim(),
    payout_direction: String(row?.payout_direction || "").trim(),
    payout_status: String(row?.payout_status || "").trim(),
    payout_percentage: String(row?.payout_percentage || "").trim(),
    payout_amount: Number(row?.payout_amount || 0) || 0,
    net_payout_amount: Number(row?.net_payout_amount || 0) || 0,
    tds_amount: Number(row?.tds_amount || 0) || 0,
    tds_percentage: Number(row?.tds_percentage || 0) || 0,
    payout_received_date: row?.payout_received_date || null,
    created_date: row?.created_date || null,
    payout_createdAt: row?.payout_createdAt || null,
    payment_history: paymentHistory,
    activity_log: activityLog,
    meta_source: String(row?.meta_source || "").trim(),
  };
};

export const upsertReceivablesFromLoan = async (loanDoc = {}) => {
  const loanId = String(loanDoc?.loanId || "").trim();
  if (!loanId) return { loanId: "", upserted: 0, removed: 0 };

  const rows = collectReceivableRowsFromLoan(loanDoc);
  const payloads = rows.map((row) => toReceivableDocPayload(loanDoc, row));
  const payoutIds = payloads.map((row) => row.payoutId).filter(Boolean);

  if (payloads.length) {
    await Receivable.bulkWrite(
      payloads.map((row) => ({
        updateOne: {
          filter: { loanId, payoutId: row.payoutId },
          update: { $set: row },
          upsert: true,
        },
      })),
      { ordered: false },
    );
  }

  const removeResult = await Receivable.deleteMany({
    loanId,
    receivableKind: "loan",
    sourceModule: "loan",
    ...(payoutIds.length ? { payoutId: { $nin: payoutIds } } : {}),
  });

  return {
    loanId,
    upserted: payloads.length,
    removed: Number(removeResult?.deletedCount || 0),
  };
};
