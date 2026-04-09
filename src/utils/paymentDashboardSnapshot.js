/**
 * Pure snapshot math for the payments dashboard — mirrors
 * ShowroomPaymentHeader + AutocreditsPaymentHeader + PaymentForm showroomData
 * so numbers match the payment workspace screens.
 */

const norm = (s) =>
  String(s || "")
    .trim()
    .toLowerCase();

export const asInt = (val) => {
  const n = Number(val);
  if (!Number.isFinite(n)) return 0;
  return Math.trunc(n);
};

const isMeaningfulShowroomRow = (row = {}) =>
  Boolean(
    String(row?.paymentType || "").trim() ||
      String(row?.paymentAmount || "").trim() ||
      String(row?.paymentMode || "").trim() ||
      String(row?.paymentMadeBy || "").trim() ||
      String(row?.remarks || "").trim(),
  );

const isMeaningfulAutocreditsRow = (row = {}) => {
  if (!row || typeof row !== "object") return false;
  const amount = asInt(row?.receiptAmount || 0);
  if (row?._auto && amount <= 0) return false;
  return Boolean(
    amount > 0 ||
      (Array.isArray(row?.receiptTypes) && row.receiptTypes.length > 0) ||
      String(row?.insurancePaymentMadeBy || "").trim() ||
      String(row?.receiptMode || "").trim() ||
      row?.receiptDate ||
      String(row?.transactionDetails || "").trim() ||
      String(row?.bankName || "").trim() ||
      String(row?.remarks || "").trim(),
  );
};

const isInsuranceCustomerAdjustment = (row = {}) => {
  const types = Array.isArray(row?.receiptTypes) ? row.receiptTypes : [];
  return types.includes("Insurance") && norm(row?.insurancePaymentMadeBy) === "customer";
};

export const computeShowroomEntryTotals = (rows = []) => {
  let paymentAmountLoan = 0;
  let paymentAmountAutocredits = 0;
  let paymentAmountCustomer = 0;
  let paymentAdjustmentExchangeApplied = 0;
  let paymentAdjustmentInsuranceApplied = 0;
  let paymentAmountMarginMoney = 0;
  let paymentCommissionReceived = 0;

  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const amount = asInt(row?.paymentAmount || 0);

    if (row?.paymentType === "Loan") {
      paymentAmountLoan += amount;
      return;
    }

    if (
      row?.paymentType === "Exchange Vehicle" &&
      row?.paymentMode === "Adjustment"
    ) {
      paymentAdjustmentExchangeApplied += amount;
      return;
    }

    if (row?.paymentType === "Insurance" && row?.paymentMode === "Adjustment") {
      paymentAdjustmentInsuranceApplied += amount;
      return;
    }

    if (row?.paymentType === "Margin Money") {
      paymentAmountMarginMoney += amount;
    }

    if (row?.paymentType === "Commission") {
      paymentCommissionReceived += amount;
      return;
    }

    if (row?.paymentMadeBy === "Autocredits") paymentAmountAutocredits += amount;
    if (row?.paymentMadeBy === "Customer") paymentAmountCustomer += amount;
  });

  return {
    paymentAmountLoan,
    paymentAmountAutocredits,
    paymentAmountCustomer,
    paymentAdjustmentExchangeApplied,
    paymentAdjustmentInsuranceApplied,
    paymentAmountMarginMoney,
    paymentCommissionReceived,
  };
};

export const computeAutocreditsEntryTotals = (
  rows = [],
  {
    insuranceReceivable = 0,
    marginReceivable = 0,
    exchangeReceivable = 0,
  } = {},
) => {
  const breakup = {
    Insurance: 0,
    "Margin Money": 0,
    "Exchange Vehicle": 0,
    Commission: 0,
  };

  const insuranceTarget = asInt(insuranceReceivable);
  const marginTarget = asInt(marginReceivable);

  let receiptAmountTotal = 0;
  let insuranceAdjustmentTotal = 0;

  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const amount = asInt(row?.receiptAmount || 0);
    if (!amount) return;

    const types = Array.isArray(row?.receiptTypes) ? row.receiptTypes : [];
    if (isInsuranceCustomerAdjustment(row)) {
      insuranceAdjustmentTotal += amount;
      return;
    }

    receiptAmountTotal += amount;
    if (!types.length) return;

    let remaining = amount;

    if (types.length === 1 && types.includes("Commission")) {
      breakup.Commission += remaining;
      return;
    }

    if (types.includes("Insurance")) {
      const onlyInsurance = types.length === 1;
      const current = asInt(breakup.Insurance);
      const cap =
        onlyInsurance || insuranceTarget <= 0
          ? remaining
          : Math.max(0, insuranceTarget - current);
      const insuranceAlloc = Math.min(remaining, cap || remaining);
      breakup.Insurance += insuranceAlloc;
      remaining -= insuranceAlloc;
    }

    if (remaining > 0 && types.includes("Margin Money")) {
      const current = asInt(breakup["Margin Money"]);
      const cap =
        marginTarget > 0 ? Math.max(0, marginTarget - current) : remaining;
      const marginAlloc = Math.min(remaining, cap || remaining);
      breakup["Margin Money"] += marginAlloc;
      remaining -= marginAlloc;
    }

    if (remaining > 0 && types.includes("Exchange Vehicle")) {
      breakup["Exchange Vehicle"] += remaining;
      remaining = 0;
    }

    if (remaining > 0 && types.includes("Commission")) {
      breakup.Commission += remaining;
    }
  });

  return {
    receiptAmountTotal,
    receiptBreakup: breakup,
    insuranceAdjustmentTotal,
    exchangeReceivable: asInt(exchangeReceivable || 0),
  };
};

export const computeCrossAdjustmentNet = (showroomRows = []) =>
  (Array.isArray(showroomRows) ? showroomRows : [])
    .filter((r) => r?.paymentType === "Cross Adjustment")
    .reduce((sum, r) => {
      const amt = asInt(r?.paymentAmount);
      if (!amt) return sum;
      return sum + (r.adjustmentDirection === "incoming" ? amt : -amt);
    }, 0);

/**
 * @param {object} doRec - delivery order record
 * @param {object} loan - loan record (may be {})
 * @param {object} entryTotals - payment.entryTotals
 * @param {number} crossAdjustmentNet - signed sum of cross adjustments
 */
export const buildShowroomData = (doRec = {}, loan = {}, entryTotals = {}, crossAdjustmentNet = 0) => {
  const financed = norm(loan?.isFinanced) === "yes";

  const customerInsuranceAmount = asInt(doRec?.do_customer_insuranceCost || 0);
  const actualInsurancePremium = asInt(doRec?.do_customer_actualInsurancePremium || 0);

  const exchangeValue = asInt(doRec?.do_exchangeVehiclePrice || 0);
  const exchangePurchasedBy = String(doRec?.do_exchangePurchasedBy || "");
  const customerNetOnRoadVehicleCost = asInt(doRec?.do_customer_netOnRoadVehicleCost || 0);
  const showroomNetOnRoadVehicleCost = asInt(doRec?.do_netOnRoadVehicleCost || 0);
  const netOnRoadVehicleCost =
    showroomNetOnRoadVehicleCost > 0
      ? showroomNetOnRoadVehicleCost + exchangeValue
      : customerNetOnRoadVehicleCost;

  const insuranceAdjustmentFromLedger = asInt(entryTotals?.paymentAdjustmentInsuranceApplied || 0);
  const exchangeAdjustmentFromLedger = asInt(entryTotals?.paymentAdjustmentExchangeApplied || 0);
  const totalPaidToShowroom =
    asInt(entryTotals?.paymentAmountLoan || 0) +
    asInt(entryTotals?.paymentAmountAutocredits || 0) +
    asInt(entryTotals?.paymentAmountCustomer || 0);
  const netPayableToShowroomForBalance =
    Math.max(
      0,
      netOnRoadVehicleCost -
        insuranceAdjustmentFromLedger -
        exchangeAdjustmentFromLedger,
    ) + asInt(crossAdjustmentNet || 0);
  const receivableFromShowroom = Math.max(
    0,
    totalPaidToShowroom - netPayableToShowroomForBalance,
  );

  const autocreditsExchangeDeduction =
    norm(exchangePurchasedBy) === "autocredits" ? exchangeValue : 0;

  const insuranceBy = String(
    doRec?.do_customer_insuranceBy || doRec?.do_insuranceBy || "",
  );
  const insuranceByNorm = norm(insuranceBy);
  const isAutocreditsInsurance = insuranceByNorm.includes("autocredits");

  const customerNetWithoutInsurance =
    customerNetOnRoadVehicleCost - customerInsuranceAmount;
  const marginPartFromOnRoadDelta = isAutocreditsInsurance
    ? customerNetWithoutInsurance - showroomNetOnRoadVehicleCost
    : customerNetOnRoadVehicleCost - showroomNetOnRoadVehicleCost;
  const marginPartFromInsuranceSpread = isAutocreditsInsurance
    ? customerInsuranceAmount - actualInsurancePremium
    : 0;
  const autocreditsMargin =
    marginPartFromOnRoadDelta + marginPartFromInsuranceSpread;

  const autocreditsMarginBreakup = {
    mode: isAutocreditsInsurance ? "autocredits_insurance" : "standard",
    insuranceBy,
    showroomNetOnRoadVehicleCost,
    customerNetOnRoadVehicleCost,
    customerInsuranceAmount,
    actualInsurancePremium,
    customerNetWithoutInsurance,
    marginPartFromOnRoadDelta,
    marginPartFromInsuranceSpread,
    autocreditsMargin,
  };

  const autocreditsInsuranceReceivable = isAutocreditsInsurance
    ? actualInsurancePremium
    : 0;

  const doMarginMoney = asInt(doRec?.do_marginMoneyPaid || 0);

  return {
    netOnRoadVehicleCost,
    customerNetOnRoadVehicleCost,
    showroomNetOnRoadVehicleCost,
    autocreditsMargin,
    autocreditsMarginBreakup,
    autocreditsExchangeDeduction,
    autocreditsInsurancePremiumReceivable: autocreditsInsuranceReceivable,
    autocreditsInsuranceReceivable: autocreditsInsuranceReceivable,
    autocreditsReceivableFromShowroom: receivableFromShowroom,
    doMarginMoney,
    exchangeValue,
    insuranceBy,
    isFinanced: financed,
  };
};

export const computeShowroomSummary = (data = {}, entryTotals = {}, crossAdjustmentNet = 0) => {
  const netOnRoad = asInt(data?.netOnRoadVehicleCost);
  const loanPay = asInt(entryTotals?.paymentAmountLoan);
  const autoPay = asInt(entryTotals?.paymentAmountAutocredits);
  const custPay = asInt(entryTotals?.paymentAmountCustomer);
  const exAdjApplied = asInt(entryTotals?.paymentAdjustmentExchangeApplied);
  const insAdjApplied = asInt(entryTotals?.paymentAdjustmentInsuranceApplied);
  const commissionReceived = asInt(entryTotals?.paymentCommissionReceived);
  const crossAdjNet = asInt(crossAdjustmentNet);

  const baseNetPayableToShowroom = Math.max(0, netOnRoad - insAdjApplied - exAdjApplied);
  const netPayableToShowroom = baseNetPayableToShowroom + crossAdjNet;
  const totalPaidToShowroom = loanPay + autoPay + custPay;
  const balancePayment = netPayableToShowroom - totalPaidToShowroom;
  const closingBalance = balancePayment + commissionReceived;
  const canVerify = closingBalance === 0;

  const exchangeValue = asInt(data?.exchangeValue);
  const hasExchange = exchangeValue > 0;
  const doMarginMoney = asInt(data?.doMarginMoney);
  const paidMarginMoney = asInt(entryTotals?.paymentAmountMarginMoney);
  const marginDiff = doMarginMoney - paidMarginMoney;
  const marginMatched = doMarginMoney > 0 && marginDiff === 0;
  const marginProgress =
    doMarginMoney > 0
      ? Math.min(100, Math.max(0, (paidMarginMoney / doMarginMoney) * 100))
      : 0;

  const settlementRatio =
    netPayableToShowroom > 0
      ? Math.min(
          100,
          Math.max(0, (totalPaidToShowroom / netPayableToShowroom) * 100),
        )
      : 100;

  return {
    netOnRoad,
    insAdjApplied,
    exAdjApplied,
    netPayableToShowroom,
    autoPay,
    custPay,
    loanPay,
    totalPaidToShowroom,
    balancePayment,
    commissionReceived,
    crossAdjNet,
    closingBalance,
    canVerify,
    exchangeValue,
    hasExchange,
    doMarginMoney,
    paidMarginMoney,
    marginDiff,
    marginMatched,
    marginProgress,
    settlementRatio,
  };
};

export const computeAutocreditsSummary = (
  data = {},
  showroomTotals = {},
  autocreditsTotals = {},
) => {
  const showroomNet = asInt(data?.showroomNetOnRoadVehicleCost || 0);
  const customerNet = asInt(data?.customerNetOnRoadVehicleCost || 0);
  const autocreditsMargin = Number.isFinite(Number(data?.autocreditsMargin))
    ? asInt(data.autocreditsMargin)
    : customerNet - showroomNet;

  const showroomAutoPaid = asInt(showroomTotals?.paymentAmountAutocredits || 0);
  const insuranceReceivable = asInt(
    data?.autocreditsInsurancePremiumReceivable ??
      data?.autocreditsInsuranceReceivable ??
      0,
  );
  const exchangeAdjustment = asInt(data?.autocreditsExchangeDeduction || 0);
  const insuranceAdjustment = asInt(autocreditsTotals?.insuranceAdjustmentTotal || 0);
  const receivableFromShowroom = asInt(data?.autocreditsReceivableFromShowroom || 0);

  const netReceivable =
    autocreditsMargin +
    showroomAutoPaid +
    insuranceReceivable -
    exchangeAdjustment -
    insuranceAdjustment -
    receivableFromShowroom;
  const receiptTotal = asInt(autocreditsTotals?.receiptAmountTotal || 0);
  const closingBalance = netReceivable - receiptTotal;
  const breakup = autocreditsTotals?.receiptBreakup || {};

  const settlementRatio =
    netReceivable > 0
      ? Math.min(100, Math.max(0, (receiptTotal / netReceivable) * 100))
      : 100;

  const canVerify = closingBalance === 0;

  return {
    autocreditsMargin,
    showroomAutoPaid,
    insuranceReceivable,
    exchangeAdjustment,
    insuranceAdjustment,
    receivableFromShowroom,
    netReceivable,
    receiptTotal,
    closingBalance,
    breakup,
    settlementRatio,
    canVerify,
  };
};

export const normalizePaymentForSnapshot = (doRec = {}, loan = {}, payment = {}) => {
  const showroomRows = (Array.isArray(payment?.showroomRows) ? payment.showroomRows : []).filter(
    isMeaningfulShowroomRow,
  );
  const autocreditsRows = (
    Array.isArray(payment?.autocreditsRows) ? payment.autocreditsRows : []
  ).filter(isMeaningfulAutocreditsRow);

  const entryTotalsFromRows = computeShowroomEntryTotals(showroomRows);
  const crossAdjustmentNet = computeCrossAdjustmentNet(showroomRows);
  const showroomData = buildShowroomData(
    doRec,
    loan,
    entryTotalsFromRows,
    crossAdjustmentNet,
  );

  const marginReceivable =
    asInt(showroomData?.autocreditsMargin || 0) +
    asInt(entryTotalsFromRows?.paymentAmountAutocredits || 0) -
    asInt(showroomData?.autocreditsExchangeDeduction || 0);

  const autocreditsTotalsFromRows = computeAutocreditsEntryTotals(autocreditsRows, {
    insuranceReceivable: showroomData?.autocreditsInsurancePremiumReceivable || 0,
    marginReceivable,
    exchangeReceivable: showroomData?.autocreditsExchangeDeduction || 0,
  });

  return {
    ...payment,
    showroomRows,
    autocreditsRows,
    entryTotals: entryTotalsFromRows,
    autocreditsTotals: autocreditsTotalsFromRows,
  };
};

export const buildPaymentCaseSnapshot = (doRec = {}, loan = {}, payment = {}) => {
  const normalizedPayment = normalizePaymentForSnapshot(doRec, loan, payment);
  const entryTotals = normalizedPayment?.entryTotals || {};
  const autocreditsTotals = normalizedPayment?.autocreditsTotals || {};
  const showroomRows = Array.isArray(normalizedPayment?.showroomRows)
    ? normalizedPayment.showroomRows
    : [];
  const crossAdjustmentNet = computeCrossAdjustmentNet(showroomRows);
  const crossAdjustmentRows = showroomRows.filter((r) => r?.paymentType === "Cross Adjustment");

  const showroomData = buildShowroomData(doRec, loan, entryTotals, crossAdjustmentNet);
  const showroomSummary = computeShowroomSummary(showroomData, entryTotals, crossAdjustmentNet);
  const autocreditsSummary = computeAutocreditsSummary(
    showroomData,
    entryTotals,
    autocreditsTotals,
  );

  return {
    showroomData,
    showroomSummary,
    autocreditsSummary,
    marginBreakup: showroomData.autocreditsMarginBreakup || {},
    crossAdjustmentNet,
    crossAdjustmentRows,
    normalizedPayment,
  };
};
