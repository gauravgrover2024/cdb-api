import Payment from "../models/Payment.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Loan from "../models/Loan.js";
import Receivable from "../models/Receivable.js";

export const PAYMENTS_AUTO_COMMISSION_META =
  "payments_negative_balance_commission_auto";

const AUTO_COMMISSION_SOURCE_ARRAY_KEY = "payments_auto_commission";
const COLLECTIONS_AUTO_PAYMENT_KEY_PREFIX = "collections_commission_receivable:";

const asInt = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.trunc(n);
};

const firstValidIsoDate = (...values) => {
  for (const value of values) {
    if (!value) continue;
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) return date.toISOString();
  }
  return null;
};

const sumPaymentHistory = (history = []) =>
  (Array.isArray(history) ? history : []).reduce(
    (sum, item) => sum + asInt(item?.amount || 0),
    0,
  );

const normalizeSourceTag = (entry = {}) =>
  String(entry?.source || entry?.sourceModule || "")
    .trim()
    .toLowerCase();

const buildPaymentsCommissionHistory = (rows = []) => {
  const list = (Array.isArray(rows) ? rows : [])
    .filter((row) => row?.paymentType === "Commission")
    .filter((row) => asInt(row?.paymentAmount || 0) > 0)
    .filter((row) => {
      const autoKey = String(row?._autoKey || "").trim();
      // Do not loop back auto rows mirrored from Collections.
      return !autoKey.startsWith(COLLECTIONS_AUTO_PAYMENT_KEY_PREFIX);
    })
    .map((row, index) => {
      const amount = asInt(row?.paymentAmount || 0);
      const paymentDate = firstValidIsoDate(row?.paymentDate, new Date().toISOString());
      const rowId = String(row?.id || "").trim();
      return {
        amount,
        date: paymentDate,
        timestamp: paymentDate,
        remarks:
          String(row?.remarks || "").trim() || "Recorded in Payments showroom ledger",
        source: "payments",
        sourceModule: "payments",
        payment_row_id: rowId || `payments_commission_${index + 1}`,
      };
    });

  // Stable de-duplication for repeated saves.
  const seen = new Set();
  return list.filter((entry) => {
    const key = `${String(entry?.payment_row_id || "").trim().toLowerCase()}|${asInt(entry?.amount || 0)}|${String(entry?.date || "").trim()}`;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const mergeCommissionHistory = ({ existingHistory = [], paymentsHistory = [] }) => {
  const keptNonPayments = (Array.isArray(existingHistory) ? existingHistory : []).filter(
    (entry) => normalizeSourceTag(entry) !== "payments",
  );
  const merged = [...keptNonPayments, ...(Array.isArray(paymentsHistory) ? paymentsHistory : [])];
  const withSortKey = merged.map((entry, index) => ({
    entry,
    index,
    sortTs: firstValidIsoDate(entry?.date, entry?.timestamp),
  }));
  withSortKey.sort((a, b) => {
    const aTs = a.sortTs ? new Date(a.sortTs).getTime() : 0;
    const bTs = b.sortTs ? new Date(b.sortTs).getTime() : 0;
    if (aTs !== bTs) return aTs - bTs;
    return a.index - b.index;
  });
  return withSortKey.map((item) => item.entry);
};

const deriveStatusFromHistory = ({ expectedAmount = 0, paymentHistory = [] }) => {
  const received = sumPaymentHistory(paymentHistory);
  if (received <= 0) return "Expected";
  if (received >= expectedAmount) return "Received";
  return "Partial";
};

const getCrossAdjustmentNet = (rows = []) =>
  (Array.isArray(rows) ? rows : [])
    .filter((row) => row?.paymentType === "Cross Adjustment")
    .reduce((sum, row) => {
      const amount = asInt(row?.paymentAmount || 0);
      if (!amount) return sum;
      return sum + (row?.adjustmentDirection === "incoming" ? amount : -amount);
    }, 0);

const computeNegativeBalance = ({ doDoc = {}, paymentDoc = {} }) => {
  const entryTotals =
    paymentDoc?.entryTotals && typeof paymentDoc.entryTotals === "object"
      ? paymentDoc.entryTotals
      : {};
  const crossAdjustmentNet = getCrossAdjustmentNet(paymentDoc?.showroomRows || []);

  const showroomNetOnRoadVehicleCost = asInt(doDoc?.do_netOnRoadVehicleCost || 0);
  const customerNetOnRoadVehicleCost = asInt(
    doDoc?.do_customer_netOnRoadVehicleCost || 0,
  );
  const exchangeValue = asInt(doDoc?.do_exchangeVehiclePrice || 0);

  const netOnRoadVehicleCost =
    showroomNetOnRoadVehicleCost > 0
      ? showroomNetOnRoadVehicleCost + exchangeValue
      : customerNetOnRoadVehicleCost;

  const insuranceAdj = asInt(entryTotals?.paymentAdjustmentInsuranceApplied || 0);
  const exchangeAdj = asInt(entryTotals?.paymentAdjustmentExchangeApplied || 0);
  const baseNetPayableToShowroom = Math.max(
    0,
    netOnRoadVehicleCost - insuranceAdj - exchangeAdj,
  );
  const netPayableToShowroom = baseNetPayableToShowroom + asInt(crossAdjustmentNet);

  const totalPaidToShowroom =
    asInt(entryTotals?.paymentAmountLoan || 0) +
    asInt(entryTotals?.paymentAmountAutocredits || 0) +
    asInt(entryTotals?.paymentAmountCustomer || 0);

  const balancePayment = netPayableToShowroom - totalPaidToShowroom;
  return {
    balancePayment,
    receivableAmount: balancePayment < 0 ? Math.abs(balancePayment) : 0,
  };
};

export const buildAutoCommissionPayoutId = (loanId) =>
  `PR-COMM-${String(loanId || "")
    .trim()
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase()}`;

const resolvePartyName = ({ doDoc = {}, loanDoc = {} }) =>
  String(
    doDoc?.do_dealerName ||
      doDoc?.dealerName ||
      loanDoc?.showroomDealerName ||
      loanDoc?.delivery_dealerName ||
      loanDoc?.dealerName ||
      "Showroom",
  ).trim();

export const syncPaymentsCommissionReceivableForLoan = async ({
  loanId,
  paymentDoc: paymentDocInput = null,
  deliveryOrderDoc: doDocInput = null,
  loanDoc: loanDocInput = null,
} = {}) => {
  const canonicalLoanId = String(loanId || "").trim();
  if (!canonicalLoanId) {
    return { loanId: "", action: "skipped", reason: "missing-loan-id" };
  }

  const [paymentDoc, doDoc, loanDoc] = await Promise.all([
    paymentDocInput ||
      Payment.findOne({ loanId: canonicalLoanId })
        .select("loanId entryTotals showroomRows")
        .lean(),
    doDocInput ||
      DeliveryOrder.findOne({
        $or: [{ loanId: canonicalLoanId }, { do_loanId: canonicalLoanId }],
      })
        .sort({ updatedAt: -1, _id: -1 })
        .select(
          [
            "loanId",
            "do_loanId",
            "do_dealerName",
            "dealerName",
            "do_netOnRoadVehicleCost",
            "do_customer_netOnRoadVehicleCost",
            "do_exchangeVehiclePrice",
          ].join(" "),
        )
        .lean(),
    loanDocInput ||
      Loan.findOne({ loanId: canonicalLoanId })
        .select(
          [
            "loanId",
            "customerName",
            "delivery_date",
            "deliveryDate",
            "vehicleDeliveryDate",
            "approval_disbursedDate",
            "disbursement_date",
            "updatedAt",
            "createdAt",
            "showroomDealerName",
            "delivery_dealerName",
            "dealerName",
          ].join(" "),
        )
        .lean(),
  ]);

  const payoutId = buildAutoCommissionPayoutId(canonicalLoanId);
  const existingDoc = await Receivable.findOne({
    loanId: canonicalLoanId,
    payoutId,
  }).lean();

  if (!paymentDoc || !doDoc) {
    if (existingDoc) {
      await Receivable.deleteOne({ _id: existingDoc._id });
      return {
        loanId: canonicalLoanId,
        payoutId,
        action: "deleted",
        receivableAmount: 0,
        reason: "missing-payment-or-do",
      };
    }
    return {
      loanId: canonicalLoanId,
      payoutId,
      action: "noop",
      receivableAmount: 0,
      reason: "missing-payment-or-do",
    };
  }

  const { receivableAmount, balancePayment } = computeNegativeBalance({
    doDoc,
    paymentDoc,
  });

  if (!(receivableAmount > 0)) {
    if (existingDoc) {
      await Receivable.deleteOne({ _id: existingDoc._id });
      return {
        loanId: canonicalLoanId,
        payoutId,
        action: "deleted",
        receivableAmount: 0,
        balancePayment,
      };
    }
    return {
      loanId: canonicalLoanId,
      payoutId,
      action: "noop",
      receivableAmount: 0,
      balancePayment,
    };
  }

  const createdDate =
    existingDoc?.created_date ||
    existingDoc?.payout_createdAt ||
    firstValidIsoDate(
      loanDoc?.delivery_date,
      loanDoc?.deliveryDate,
      loanDoc?.vehicleDeliveryDate,
      loanDoc?.approval_disbursedDate,
      loanDoc?.disbursement_date,
      loanDoc?.updatedAt,
      loanDoc?.createdAt,
      new Date().toISOString(),
    );

  const paymentHistory = Array.isArray(existingDoc?.payment_history)
    ? existingDoc.payment_history
    : [];
  const paymentsHistory = buildPaymentsCommissionHistory(paymentDoc?.showroomRows || []);
  const mergedPaymentHistory = mergeCommissionHistory({
    existingHistory: paymentHistory,
    paymentsHistory,
  });
  const activityLog = Array.isArray(existingDoc?.activity_log)
    ? existingDoc.activity_log
    : [];
  const nextStatus = deriveStatusFromHistory({
    expectedAmount: receivableAmount,
    paymentHistory: mergedPaymentHistory,
  });
  const partyName = resolvePartyName({ doDoc, loanDoc });
  const latestReceiptDate = firstValidIsoDate(
    ...mergedPaymentHistory
      .map((entry) => firstValidIsoDate(entry?.date, entry?.timestamp))
      .filter(Boolean),
  );
  const nextPayoutReceivedDate =
    nextStatus === "Received"
      ? existingDoc?.payout_received_date || latestReceiptDate || new Date().toISOString()
      : null;

  const payload = {
    ...(existingDoc?.payload && typeof existingDoc.payload === "object"
      ? existingDoc.payload
      : {}),
    id: payoutId,
    payoutId,
    payout_applicable: "Yes",
    payout_type: "Commission",
    payout_party_name: partyName,
    payout_percentage: "",
    payout_amount: receivableAmount,
    payout_direction: "Receivable",
    tds_applicable: "No",
    tds_percentage: 0,
    tds_amount: 0,
    net_payout_amount: receivableAmount,
    payout_status: nextStatus,
    payout_received_date: nextPayoutReceivedDate,
    created_date: createdDate,
    payout_createdAt: createdDate,
    payout_remarks:
      "Auto-synced from showroom balance payment in payments ledger.",
    meta_source: PAYMENTS_AUTO_COMMISSION_META,
  };

  const nextDoc = {
    receivableKind: "commission",
    sourceModule: "payments",
    loanId: canonicalLoanId,
    loanMongoId: loanDoc?._id || existingDoc?.loanMongoId || null,
    customerName:
      String(loanDoc?.customerName || "").trim() ||
      String(existingDoc?.customerName || "").trim(),
    payoutId,
    sourceArrayKey:
      String(existingDoc?.sourceArrayKey || "").trim() ||
      AUTO_COMMISSION_SOURCE_ARRAY_KEY,
    payload,
    payout_type: "Commission",
    payout_party_name: partyName,
    payout_direction: "Receivable",
    payout_status: nextStatus,
    payout_percentage: "",
    payout_amount: receivableAmount,
    net_payout_amount: receivableAmount,
    tds_amount: 0,
    tds_percentage: 0,
    payout_received_date: nextPayoutReceivedDate,
    created_date: createdDate,
    payout_createdAt: createdDate,
    payment_history: mergedPaymentHistory,
    activity_log: activityLog,
    meta_source: PAYMENTS_AUTO_COMMISSION_META,
  };

  const amountChanged = asInt(existingDoc?.payout_amount || 0) !== receivableAmount;
  const historyChanged =
    JSON.stringify(paymentHistory || []) !== JSON.stringify(mergedPaymentHistory || []);
  if (amountChanged || historyChanged) {
    const historyTotal = sumPaymentHistory(mergedPaymentHistory);
    nextDoc.activity_log = [
      ...activityLog,
      {
        timestamp: new Date().toISOString(),
        action: "Auto synced from Payments",
        details: amountChanged
          ? `Balance payment updated to ${balancePayment}; commission receivable recalculated to ${receivableAmount}.`
          : `Commission receipt history synced from Payments (${historyTotal} received).`,
      },
    ];
  }

  const saved = await Receivable.findOneAndUpdate(
    { loanId: canonicalLoanId, payoutId },
    { $set: nextDoc },
    {
      returnDocument: "after",
      upsert: true,
      setDefaultsOnInsert: true,
      runValidators: false,
    },
  ).lean();

  return {
    loanId: canonicalLoanId,
    payoutId,
    action: existingDoc ? "updated" : "created",
    receivableAmount,
    balancePayment,
    savedId: saved?._id ? String(saved._id) : "",
  };
};

export const syncPaymentsCommissionReceivablesForLoanIds = async (loanIds = []) => {
  const canonicalIds = Array.from(
    new Set(
      (Array.isArray(loanIds) ? loanIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean),
    ),
  );
  if (!canonicalIds.length) {
    return { processed: 0, created: 0, updated: 0, deleted: 0, noop: 0 };
  }

  const summary = { processed: 0, created: 0, updated: 0, deleted: 0, noop: 0 };
  for (const loanId of canonicalIds) {
    // eslint-disable-next-line no-await-in-loop
    const result = await syncPaymentsCommissionReceivableForLoan({ loanId });
    summary.processed += 1;
    if (result?.action === "created") summary.created += 1;
    else if (result?.action === "updated") summary.updated += 1;
    else if (result?.action === "deleted") summary.deleted += 1;
    else summary.noop += 1;
  }
  return summary;
};
