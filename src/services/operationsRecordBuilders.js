const LEGACY_CUTOFF = new Date("2026-02-01T00:00:00.000Z");

const hasOwn = (obj = {}, key) =>
  Object.prototype.hasOwnProperty.call(obj || {}, key);

const firstMeaningfulText = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const text = String(value).trim();
    if (!text) continue;
    if (
      ["n/a", "na", "null", "undefined", "-", "--", "not set"].includes(
        text.toLowerCase(),
      )
    ) {
      continue;
    }
    return text;
  }
  return "";
};

const firstNumber = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null || value === "") continue;
    const normalized =
      typeof value === "string" ? value.replace(/,/g, "").trim() : value;
    const parsed = Number(normalized);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
};

const asInt = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.trunc(n);
};

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

const sanitizeAutocreditsRows = (rows = []) =>
  (Array.isArray(rows) ? rows : []).filter(isMeaningfulAutocreditsRow);

const pickDisbursedBankName = (loan = {}) => {
  const banks = Array.isArray(loan?.approval_banksData)
    ? loan.approval_banksData
    : [];
  const disbursedBank = banks.find(
    (row) =>
      String(row?.status || "").trim().toLowerCase() === "disbursed" &&
      firstMeaningfulText(row?.bankName),
  );
  const approvedBank = banks.find(
    (row) =>
      String(row?.status || "").trim().toLowerCase() === "approved" &&
      firstMeaningfulText(row?.bankName),
  );

  return firstMeaningfulText(
    loan?.disburse_bankName,
    disbursedBank?.bankName,
    loan?.postfile_bankName,
    loan?.approval_bankName,
    approvedBank?.bankName,
  );
};

const parseValidDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const resolveField = (payload = {}, payloadKeys = [], fallbackValue = undefined) => {
  for (const key of payloadKeys) {
    if (hasOwn(payload, key)) return payload[key];
  }
  return fallbackValue;
};

const normalizeLoanType = (loan = {}) => {
  const raw =
    loan?.typeOfLoan ||
    loan?.loanType ||
    loan?.caseType ||
    loan?.vehicleType ||
    "";
  return String(raw)
    .trim()
    .toLowerCase()
    .replace(/[-_\s]+/g, " ");
};

const isNewCarLoan = (loan = {}) => {
  const normalized = normalizeLoanType(loan);
  if (!normalized) return false;

  if (
    normalized.includes("used") ||
    normalized.includes("refinance") ||
    normalized.includes("cash in")
  ) {
    return false;
  }

  return (
    normalized === "new" ||
    normalized.includes("new car") ||
    normalized.includes("newcar")
  );
};

const parseBusinessDate = (loan = {}) => {
  const candidates = [
    loan?.leadDate,
    loan?.latestBusinessDate,
    loan?.delivery_date,
    loan?.deliveryDate,
    loan?.do_date,
    loan?.doDate,
    loan?.invoice_date,
    loan?.invoiceDate,
    loan?.approval_disbursedDate,
    loan?.disbursement_date,
    loan?.disbursementDate,
    loan?.disbursedDate,
    loan?.disburseDate,
    loan?.postfile_disbursementDate,
  ];

  for (const candidate of candidates) {
    const date = parseValidDate(candidate);
    if (date) return date;
  }

  return null;
};

const isLegacyNewCar = (loan = {}) => {
  if (!isNewCarLoan(loan)) return false;
  const businessDate = parseBusinessDate(loan);
  if (!businessDate) return false;
  return businessDate < LEGACY_CUTOFF;
};

const generateDORef = (seedDate = new Date()) => {
  const year = new Date(seedDate).getFullYear() || new Date().getFullYear();
  const random = Math.floor(Math.random() * 999999)
    .toString()
    .padStart(6, "0");
  return `DO-${year}-${random}`;
};

const buildDeliveryOrderSnapshot = (payload = {}, loan = {}, loanId = "") => {
  const resolvedLoanId = String(
    resolveField(payload, ["loanId", "do_loanId"], loanId || loan?.loanId || ""),
  ).trim();
  const doDate =
    parseValidDate(
      resolveField(payload, ["do_date", "doDate", "date"], undefined),
    ) ||
    parseValidDate(loan?.do_date) ||
    parseValidDate(loan?.doDate) ||
    parseValidDate(loan?.leadDate) ||
    new Date();

  const doRefNo =
    resolveField(payload, ["do_refNo", "doNumber"], undefined) ??
    (firstMeaningfulText(loan?.do_number, loan?.do_refNo, loan?.doNumber) ||
      generateDORef(doDate));

  const customerNameFallback = firstMeaningfulText(
    loan?.customerName,
    loan?.applicant_name,
    loan?.applicantName,
    loan?.companyName,
  );
  const primaryMobileFallback = firstMeaningfulText(
    loan?.primaryMobile,
    loan?.mobile,
    loan?.phone,
    loan?.phoneNumber,
  );
  const dealerNameFallback = firstMeaningfulText(
    loan?.showroomDealerName,
    loan?.delivery_dealerName,
    loan?.dealerName,
    loan?.showroomName,
  );
  const dealerAddressFallback = firstMeaningfulText(
    loan?.showroomDealerAddress,
    loan?.delivery_dealerAddress,
    loan?.dealerAddress,
    loan?.showroomAddress,
  );
  const dealerMobileFallback = firstMeaningfulText(
    loan?.delivery_dealerContactNumber,
    loan?.dealerMobile,
    loan?.dealerContactNumber,
  );
  const dealerContactPersonFallback = firstMeaningfulText(
    loan?.delivery_dealerContactPerson,
    loan?.dealerContactPerson,
    loan?.showroomContactPerson,
  );
  const dealerCityFallback = firstMeaningfulText(
    loan?.delivery_dealerCity,
    loan?.dealerCity,
    loan?.showroomCity,
  );
  const dealerPincodeFallback = firstMeaningfulText(
    loan?.delivery_dealerPincode,
    loan?.dealerPincode,
    loan?.showroomPincode,
  );

  const customerName = resolveField(
    payload,
    ["customerName", "do_customerName"],
    customerNameFallback,
  );
  const primaryMobile = resolveField(
    payload,
    ["primaryMobile", "do_primaryMobile"],
    primaryMobileFallback,
  );
  const residenceAddress = resolveField(
    payload,
    ["residenceAddress", "do_residenceAddress"],
    firstMeaningfulText(
      loan?.residenceAddress,
      loan?.currentAddress,
      loan?.address,
      loan?.permanentAddress,
    ),
  );
  const pincode = resolveField(
    payload,
    ["pincode", "do_pincode"],
    firstMeaningfulText(
      loan?.pincode,
      loan?.currentPincode,
      loan?.permanentPincode,
    ),
  );
  const city = resolveField(
    payload,
    ["city", "do_city"],
    firstMeaningfulText(loan?.city, loan?.currentCity, loan?.permanentCity),
  );
  const recordSource = resolveField(
    payload,
    ["recordSource", "do_recordSource"],
    firstMeaningfulText(
      loan?.recordSource,
      loan?.source,
      loan?.sourcingChannel,
      loan?.sourceType,
    ),
  );
  const sourceName = resolveField(
    payload,
    ["sourceName", "do_sourceName"],
    firstMeaningfulText(
      loan?.sourceName,
      loan?.showroomDealerName,
      loan?.showroomName,
      loan?.dealerName,
      loan?.channelName,
    ),
  );
  const dealerName = resolveField(
    payload,
    ["dealerName", "do_dealerName"],
    dealerNameFallback,
  );
  const dealerAddress = resolveField(
    payload,
    ["dealerAddress", "do_dealerAddress"],
    dealerAddressFallback,
  );
  const dealerMobile = resolveField(
    payload,
    ["dealerMobile", "do_dealerMobile"],
    dealerMobileFallback,
  );
  const dealerContactPerson = resolveField(
    payload,
    ["dealerContactPerson", "do_dealerContactPerson"],
    dealerContactPersonFallback,
  );
  const dealerCity = resolveField(
    payload,
    ["dealerCity", "do_dealerCity"],
    dealerCityFallback,
  );
  const dealerPincode = resolveField(
    payload,
    ["dealerPincode", "do_dealerPincode"],
    dealerPincodeFallback,
  );
  const vehicleMake = resolveField(
    payload,
    ["vehicleMake", "do_vehicleMake"],
    firstMeaningfulText(loan?.vehicleMake, loan?.make),
  );
  const vehicleModel = resolveField(
    payload,
    ["vehicleModel", "do_vehicleModel"],
    firstMeaningfulText(loan?.vehicleModel, loan?.model),
  );
  const vehicleVariant = resolveField(
    payload,
    ["vehicleVariant", "do_vehicleVariant"],
    firstMeaningfulText(loan?.vehicleVariant, loan?.variant),
  );
  const vehicleColor = resolveField(
    payload,
    ["vehicleColor", "do_vehicleColor", "do_colour"],
    firstMeaningfulText(loan?.vehicleColor, loan?.colour, loan?.color),
  );
  const hypothecationBank = firstMeaningfulText(
    // Always prefer the disbursed-bank source from Loan to avoid stale UI payloads.
    pickDisbursedBankName(loan),
    resolveField(payload, ["do_hypothecation", "hypothecationBank"], undefined),
  );

  return {
    ...payload,
    loanId: resolvedLoanId,
    do_loanId: resolvedLoanId,
    do_refNo: doRefNo,
    doNumber:
      resolveField(payload, ["doNumber"], undefined) ?? String(doRefNo || "").trim(),
    do_date: doDate,
    doDate:
      resolveField(payload, ["doDate"], undefined) ??
      parseValidDate(loan?.doDate) ??
      doDate,
    do_bookingDate:
      parseValidDate(resolveField(payload, ["do_bookingDate", "doBookingDate"], undefined)) ||
      parseValidDate(loan?.do_bookingDate) ||
      undefined,
    customerName,
    do_customerName: resolveField(payload, ["do_customerName"], customerName),
    primaryMobile,
    do_primaryMobile: resolveField(payload, ["do_primaryMobile"], primaryMobile),
    residenceAddress,
    do_residenceAddress: resolveField(
      payload,
      ["do_residenceAddress"],
      residenceAddress,
    ),
    pincode,
    do_pincode: resolveField(payload, ["do_pincode"], pincode),
    city,
    do_city: resolveField(payload, ["do_city"], city),
    recordSource,
    do_recordSource: resolveField(payload, ["do_recordSource"], recordSource),
    sourceName,
    do_sourceName: resolveField(payload, ["do_sourceName"], sourceName),
    dealerName,
    do_dealerName: resolveField(payload, ["do_dealerName"], dealerName),
    dealerAddress,
    do_dealerAddress: resolveField(payload, ["do_dealerAddress"], dealerAddress),
    dealerMobile,
    do_dealerMobile: resolveField(payload, ["do_dealerMobile"], dealerMobile),
    dealerContactPerson,
    do_dealerContactPerson: resolveField(
      payload,
      ["do_dealerContactPerson"],
      dealerContactPerson,
    ),
    dealerCity,
    do_dealerCity: resolveField(payload, ["do_dealerCity"], dealerCity),
    dealerPincode,
    do_dealerPincode: resolveField(
      payload,
      ["do_dealerPincode"],
      dealerPincode,
    ),
    vehicleMake,
    do_vehicleMake: resolveField(payload, ["do_vehicleMake"], vehicleMake),
    vehicleModel,
    do_vehicleModel: resolveField(payload, ["do_vehicleModel"], vehicleModel),
    vehicleVariant,
    do_vehicleVariant: resolveField(payload, ["do_vehicleVariant"], vehicleVariant),
    vehicleColor,
    do_vehicleColor: resolveField(payload, ["do_vehicleColor"], vehicleColor),
    do_colour: resolveField(payload, ["do_colour"], vehicleColor),
    do_exShowroomPrice: resolveField(
      payload,
      ["do_exShowroomPrice"],
      firstNumber(
        loan?.exShowroomPrice,
        loan?.ex_showroom,
        loan?.exShowroom,
        loan?.vehiclePricing?.exShowroom,
        loan?.pricing?.exShowroom,
      ),
    ),
    do_insuranceCost: resolveField(
      payload,
      ["do_insuranceCost"],
      firstNumber(
        loan?.insuranceCost,
        loan?.insurance,
        loan?.insurance_amount_cardekho,
      ),
    ),
    do_roadTax: resolveField(
      payload,
      ["do_roadTax"],
      firstNumber(loan?.roadTax, loan?.rto, loan?.rto_amount_cardekho),
    ),
    do_tcs: resolveField(
      payload,
      ["do_tcs"],
      firstNumber(loan?.tcs, loan?.other_tcsCharges),
    ),
    do_accessoriesAmount: resolveField(
      payload,
      ["do_accessoriesAmount"],
      firstNumber(loan?.accessoriesAmount, loan?.optional_accessoriesCharges),
    ),
    do_extendedWarranty: resolveField(
      payload,
      ["do_extendedWarranty"],
      firstNumber(
        loan?.extendedWarranty,
        loan?.optional_extendedWarrantyCharges,
      ),
    ),
    do_processingFees: resolveField(
      payload,
      ["do_processingFees"],
      firstNumber(
        loan?.postfile_processingFees,
        loan?.approval_processingFees,
        loan?.processingFees,
        loan?.postFile?.processingFees,
        loan?.postfile?.processingFees,
      ),
    ),
    do_loanAmount: resolveField(
      payload,
      ["do_loanAmount"],
      firstNumber(
        loan?.postfile_loanAmountDisbursed,
        loan?.postfile_netLoanAmount,
        loan?.loanAmount,
      ),
    ),
    do_customer_insuranceCost: resolveField(
      payload,
      ["do_customer_insuranceCost"],
      firstNumber(
        loan?.insuranceCost,
        loan?.insurance,
        loan?.insurance_amount_cardekho,
      ),
    ),
    do_customer_actualInsurancePremium: resolveField(
      payload,
      ["do_customer_actualInsurancePremium"],
      firstNumber(loan?.insurance_premium),
    ),
    do_customer_insuranceBy: resolveField(
      payload,
      ["do_customer_insuranceBy"],
      firstMeaningfulText(loan?.insurance_by, loan?.insuranceBy),
    ),
    do_customer_insuranceCompanyName: resolveField(
      payload,
      ["do_customer_insuranceCompanyName"],
      firstMeaningfulText(loan?.insurance_company_name),
    ),
    do_customer_insurancePolicyNumber: resolveField(
      payload,
      ["do_customer_insurancePolicyNumber"],
      firstMeaningfulText(loan?.insurance_policy_number),
    ),
    do_customer_insurancePolicyStartDate: resolveField(
      payload,
      ["do_customer_insurancePolicyStartDate"],
      parseValidDate(loan?.insurance_policy_start_date),
    ),
    do_customer_insurancePolicyDurationOD: resolveField(
      payload,
      ["do_customer_insurancePolicyDurationOD"],
      firstMeaningfulText(loan?.insurance_policy_duration_od),
    ),
    do_customer_insurancePolicyEndDateOD: resolveField(
      payload,
      ["do_customer_insurancePolicyEndDateOD"],
      parseValidDate(loan?.insurance_policy_end_date_od),
    ),
    do_hypothecation: hypothecationBank,
  };
};

const buildPaymentSkeleton = (loanId, payload = {}, loan = {}) => ({
  loanId,
  showroomRows: Array.isArray(payload?.showroomRows) ? payload.showroomRows : [],
  entryTotals:
    payload?.entryTotals && typeof payload.entryTotals === "object"
      ? payload.entryTotals
      : {},
  isVerified: Boolean(payload?.isVerified),
  autocreditsRows: sanitizeAutocreditsRows(payload?.autocreditsRows),
  autocreditsTotals:
    payload?.autocreditsTotals && typeof payload.autocreditsTotals === "object"
      ? payload.autocreditsTotals
      : {},
  isAutocreditsVerified: Boolean(payload?.isAutocreditsVerified),
  showroomName: firstMeaningfulText(
    payload?.showroomName,
    loan?.showroomDealerName,
    loan?.delivery_dealerName,
    loan?.dealerName,
  ),
  customerName: firstMeaningfulText(
    payload?.customerName,
    loan?.customerName,
    loan?.applicant_name,
    loan?.applicantName,
    loan?.companyName,
  ),
  primaryMobile: firstMeaningfulText(
    payload?.primaryMobile,
    loan?.primaryMobile,
    loan?.mobile,
    loan?.phone,
    loan?.phoneNumber,
  ),
  vehicleMake: firstMeaningfulText(
    payload?.vehicleMake,
    loan?.vehicleMake,
    loan?.make,
  ),
  vehicleModel: firstMeaningfulText(
    payload?.vehicleModel,
    loan?.vehicleModel,
    loan?.model,
  ),
  vehicleVariant: firstMeaningfulText(
    payload?.vehicleVariant,
    loan?.vehicleVariant,
    loan?.variant,
  ),
  do_refNo: firstMeaningfulText(
    payload?.do_refNo,
    payload?.doNumber,
    loan?.do_refNo,
    loan?.do_number,
    loan?.doNumber,
  ),
  doNumber: firstMeaningfulText(
    payload?.doNumber,
    payload?.do_refNo,
    loan?.do_number,
    loan?.do_refNo,
    loan?.doNumber,
  ),
  channelName: firstMeaningfulText(
    payload?.channelName,
    loan?.channelName,
    loan?.sourceName,
  ),
});

export {
  LEGACY_CUTOFF,
  buildDeliveryOrderSnapshot,
  buildPaymentSkeleton,
  generateDORef,
  isLegacyNewCar,
  isNewCarLoan,
  normalizeLoanType,
  parseBusinessDate,
  parseValidDate,
};
