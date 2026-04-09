import asyncHandler from 'express-async-handler';
import Payment from '../models/Payment.js';
import Loan from '../models/Loan.js';
import Counter from '../models/Counter.js';
import DeliveryOrder from '../models/DeliveryOrder.js';
import {
  buildDeliveryOrderSnapshot,
  buildPaymentSkeleton,
  isNewCarLoan,
  parseBusinessDate,
} from '../services/operationsRecordBuilders.js';
import { syncPaymentsCommissionReceivableForLoan } from '../services/paymentsCommissionReceivableService.js';
import { buildPaymentCaseSnapshot } from '../utils/paymentDashboardSnapshot.js';

const LOAN_ID_PREFIX = 'LN';

const escapeRegex = (value = '') =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const parseCsvIds = (value = '') =>
  String(value || '')
    .split(',')
    .map((item) => String(item || '').trim())
    .filter(Boolean);

const asInt = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.trunc(n);
};

const safeText = (value) =>
  value === undefined || value === null ? '' : String(value);

const normalizeLoanId = (value = '') => String(value || '').trim();

const isMeaningfulAutocreditsRow = (row = {}) => {
  if (!row || typeof row !== 'object') return false;
  const amount = asInt(row?.receiptAmount || 0);
  if (row?._auto && amount <= 0) return false;
  return Boolean(
    amount > 0 ||
      (Array.isArray(row?.receiptTypes) && row.receiptTypes.length > 0) ||
      String(row?.insurancePaymentMadeBy || '').trim() ||
      String(row?.receiptMode || '').trim() ||
      row?.receiptDate ||
      String(row?.transactionDetails || '').trim() ||
      String(row?.bankName || '').trim() ||
      String(row?.remarks || '').trim(),
  );
};

const sanitizeAutocreditsRows = (rows = []) =>
  (Array.isArray(rows) ? rows : []).filter(isMeaningfulAutocreditsRow);

const sanitizePaymentForResponse = (payment = {}) => {
  if (!payment || typeof payment !== 'object') return payment;
  const next = { ...payment };
  if (Array.isArray(next.autocreditsRows)) {
    next.autocreditsRows = sanitizeAutocreditsRows(next.autocreditsRows);
  }
  return next;
};

const isManualShowroomRow = (row = {}) =>
  !row?._auto &&
  Boolean(
    asInt(row?.paymentAmount) > 0 ||
      String(row?.paymentType || '').trim() ||
      String(row?.paymentMadeBy || '').trim() ||
      String(row?.paymentMode || '').trim(),
  );

const isManualAcRow = (row = {}) =>
  !row?._auto &&
  Boolean(
    asInt(row?.receiptAmount) > 0 ||
      (Array.isArray(row?.receiptTypes) && row.receiptTypes.length > 0) ||
      String(row?.receiptMode || '').trim(),
  );

const dedupeDOByLoanId = (rows = []) => {
  const byLoanId = new Map();
  (rows || []).forEach((row) => {
    const key = normalizeLoanId(row?.loanId || row?.do_loanId);
    if (!key) return;
    if (!byLoanId.has(key)) {
      byLoanId.set(key, row);
      return;
    }
    const prev = byLoanId.get(key);
    const prevTs =
      new Date(prev?.updatedAt || prev?.createdAt || 0).getTime() || 0;
    const nextTs =
      new Date(row?.updatedAt || row?.createdAt || 0).getTime() || 0;
    if (nextTs >= prevTs) byLoanId.set(key, row);
  });
  return Array.from(byLoanId.values());
};

const buildPaymentsDashboardRow = (doRec = {}, loan = {}, paymentRaw = {}) => {
  const loanId = normalizeLoanId(doRec?.loanId || doRec?.do_loanId || loan?.loanId);
  const payment = sanitizePaymentForResponse(paymentRaw) || {};

  const customerName =
    safeText(doRec?.do_customerName) ||
    safeText(doRec?.customerName) ||
    safeText(loan?.customerName) ||
    'Unknown';
  const primaryMobile =
    safeText(doRec?.do_primaryMobile) ||
    safeText(doRec?.primaryMobile) ||
    safeText(loan?.primaryMobile) ||
    '';

  const vehicle = [
    safeText(doRec?.do_vehicleMake || doRec?.vehicleMake || loan?.vehicleMake),
    safeText(doRec?.do_vehicleModel || doRec?.vehicleModel || loan?.vehicleModel),
    safeText(doRec?.do_vehicleVariant || doRec?.vehicleVariant || loan?.vehicleVariant),
  ]
    .filter(Boolean)
    .join(' ');

  const dealerName =
    safeText(doRec?.do_dealerName) ||
    safeText(doRec?.dealerName) ||
    safeText(loan?.showroomDealerName) ||
    safeText(loan?.delivery_dealerName) ||
    'Showroom not set';

  const doRef = safeText(doRec?.do_refNo) || safeText(doRec?.doNumber) || '';
  const netDO = asInt(doRec?.do_netDOAmount);
  const snapshot = buildPaymentCaseSnapshot(doRec, loan, payment);
  const ss = snapshot?.showroomSummary || {};
  const ac = snapshot?.autocreditsSummary || {};
  const showroomRows = Array.isArray(payment?.showroomRows)
    ? payment.showroomRows
    : [];
  const acRows = Array.isArray(payment?.autocreditsRows)
    ? payment.autocreditsRows
    : [];
  const manualShowroomCount = showroomRows.filter(isManualShowroomRow).length;
  const manualAcCount = acRows.filter(isManualAcRow).length;
  const hasManualActivity = manualShowroomCount > 0 || manualAcCount > 0;

  const showroomSettled = Boolean(payment?.isVerified) && Boolean(ss?.canVerify);
  const autocreditsSettled =
    Boolean(payment?.isAutocreditsVerified) && Boolean(ac?.canVerify);

  let overallStatus = 'DRAFT';
  if (showroomSettled && autocreditsSettled) overallStatus = 'SETTLED';
  else if (showroomSettled || autocreditsSettled) overallStatus = 'PARTIAL';
  else if (
    hasManualActivity &&
    (asInt(ss?.totalPaidToShowroom) > 0 || asInt(ac?.receiptTotal) > 0)
  ) {
    overallStatus = 'IN_PROGRESS';
  }

  const isFinanced =
    safeText(loan?.isFinanced || doRec?.isFinanced).trim().toLowerCase() === 'yes';

  const outstandingShowroom = Math.max(0, asInt(ss?.closingBalance));

  const lastActivityTs = Math.max(
    new Date(payment?.updatedAt || 0).getTime(),
    new Date(doRec?.updatedAt || 0).getTime(),
    new Date(doRec?.do_date || doRec?.doDate || 0).getTime(),
    0,
  );

  return {
    key: loanId,
    loanId,
    doRef,
    customerName,
    primaryMobile,
    dealerName,
    vehicle,
    netDO,
    paidShowroom: asInt(ss?.totalPaidToShowroom),
    balanceToShowroom: asInt(ss?.closingBalance),
    outstandingShowroom,
    receivedAutocredits: asInt(ac?.receiptTotal),
    showroomSettled,
    autocreditsSettled,
    overallStatus,
    hasPayment: Boolean(payment?._id || payment?.loanId),
    payment,
    loan,
    doRec,
    isFinanced,
    hasManualActivity,
    manualShowroomCount,
    manualAcCount,
    snapshot,
    lastActivityTs,
  };
};

const matchesPaymentsDashboardFilters = (row = {}, filters = {}) => {
  const statusFilter = String(filters?.status || 'all').trim().toLowerCase();
  const typeFilter = String(filters?.type || 'all').trim().toLowerCase();

  if (typeFilter === 'financed' && !row.isFinanced) return false;
  if (typeFilter === 'cash' && row.isFinanced) return false;

  if (statusFilter === 'nofile' && row.hasPayment) return false;

  if (statusFilter === 'progress') {
    if (!row.hasManualActivity) return false;
    if (row.overallStatus !== 'IN_PROGRESS' && row.overallStatus !== 'PARTIAL') {
      return false;
    }
  }

  if (statusFilter === 'acpending') {
    if (!row.hasManualActivity) return false;
    const ac = row.snapshot?.autocreditsSummary || {};
    const closingPending = asInt(ac?.closingBalance) > 0;
    const verificationPending =
      Boolean(ac?.canVerify) && !row?.payment?.isAutocreditsVerified;
    if (!closingPending && !verificationPending) return false;
  }

  if (statusFilter === 'settled' && row.overallStatus !== 'SETTLED') {
    return false;
  }

  if (statusFilter === 'verify') {
    if (!row.hasManualActivity) return false;
    const ss = row.snapshot?.showroomSummary || {};
    const ac = row.snapshot?.autocreditsSummary || {};
    const payment = row.payment || {};
    const needsVerify =
      (ss?.canVerify && !payment?.isVerified) ||
      (ac?.canVerify && !payment?.isAutocreditsVerified);
    if (!needsVerify) return false;
  }

  return true;
};

const buildPaymentsDashboardDoQuery = (query = {}) => {
  const {
    search = '',
    dealerName = '',
    loanIds = '',
    doStatus = '',
  } = query || {};

  const andFilters = [];
  const safeLoanIds = parseCsvIds(loanIds);
  if (safeLoanIds.length) {
    andFilters.push({
      $or: [{ loanId: { $in: safeLoanIds } }, { do_loanId: { $in: safeLoanIds } }],
    });
  }

  const safeStatus = String(doStatus || '').trim();
  if (safeStatus) {
    andFilters.push({ status: safeStatus });
  }

  const safeDealer = String(dealerName || '').trim();
  if (safeDealer) {
    andFilters.push({
      dealerName: new RegExp(escapeRegex(safeDealer), 'i'),
    });
  }

  const safeSearch = String(search || '').trim();
  if (safeSearch) {
    const re = new RegExp(escapeRegex(safeSearch), 'i');
    andFilters.push({
      $or: [
        { loanId: re },
        { do_loanId: re },
        { customerName: re },
        { do_customerName: re },
        { primaryMobile: re },
        { do_primaryMobile: re },
        { dealerName: re },
        { do_dealerName: re },
        { vehicleMake: re },
        { do_vehicleMake: re },
        { vehicleModel: re },
        { do_vehicleModel: re },
        { vehicleVariant: re },
        { do_vehicleVariant: re },
        { do_refNo: re },
        { doNumber: re },
      ],
    });
  }

  if (!andFilters.length) return {};
  if (andFilters.length === 1) return andFilters[0];
  return { $and: andFilters };
};

const reserveNextLoanId = async () => {
  const year = new Date().getFullYear();
  const key = `loan_id_sequence_${year}`;
  const regex = new RegExp(`^${LOAN_ID_PREFIX}-${year}-\\d+$`, 'i');

  const bumped = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    { returnDocument: 'after', lean: true },
  );
  if (bumped?.value)
    return `${LOAN_ID_PREFIX}-${year}-${String(bumped.value).padStart(4, '0')}`;

  const lastLoan = await Loan.findOne({ loanId: { $regex: regex } })
    .sort({ loanId: -1 })
    .select('loanId')
    .lean();
  const lastSeq = lastLoan?.loanId ? Number(lastLoan.loanId.split('-')[2]) || 0 : 0;

  try {
    await Counter.create({ key, value: lastSeq });
  } catch (e) {
    if (e?.code !== 11000) throw e;
  }

  const bumped2 = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true, lean: true },
  );
  return `${LOAN_ID_PREFIX}-${year}-${String(bumped2?.value || 1).padStart(4, '0')}`;
};

const isLegacyCase = (loan = {}) => {
  const businessDate = parseBusinessDate(loan);
  if (!businessDate) return false;
  return businessDate < new Date('2026-02-01T00:00:00.000Z');
};

const createDirectPayment = asyncHandler(async (req, res) => {
  const {
    customerName,
    primaryMobile,
    vehicleMake,
    vehicleModel,
    vehicleVariant,
    isFinanced,
    typeOfLoan,
    dealerName,
    dealerAddress,
    vehicleColor,
    createdBy,
  } = req.body;

  if (!customerName || !primaryMobile) {
    res.status(400);
    throw new Error('customerName and primaryMobile are required');
  }

  const loanType = typeOfLoan || 'New Car';
  const financed = isFinanced === false || isFinanced === 'No' ? 'No' : 'Yes';

  const loanId = await reserveNextLoanId();

  const loan = await Loan.create({
    loanId,
    customerName,
    primaryMobile,
    vehicleMake: vehicleMake || '',
    vehicleModel: vehicleModel || '',
    vehicleVariant: vehicleVariant || '',
    typeOfLoan: loanType,
    loanType,
    isFinanced: financed,
    currentStage: 'profile',
    status: 'Pending',
    createdBy: createdBy || undefined,
  });

  const doRecord = await DeliveryOrder.findOneAndUpdate(
    { loanId },
    {
      $set: {
        ...buildDeliveryOrderSnapshot(
          {
            dealerName: dealerName || '',
            do_dealerName: dealerName || '',
            dealerAddress: dealerAddress || '',
            do_dealerAddress: dealerAddress || '',
            customerName,
            do_customerName: customerName,
            primaryMobile,
            do_primaryMobile: primaryMobile,
            vehicleMake: vehicleMake || '',
            do_vehicleMake: vehicleMake || '',
            vehicleModel: vehicleModel || '',
            do_vehicleModel: vehicleModel || '',
            vehicleVariant: vehicleVariant || '',
            do_vehicleVariant: vehicleVariant || '',
            vehicleColor: vehicleColor || '',
            do_vehicleColor: vehicleColor || '',
            do_colour: vehicleColor || '',
          },
          loan,
          loanId,
        ),
        createdBy: createdBy || undefined,
      },
    },
    { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true },
  );

  const payment = await Payment.findOneAndUpdate(
    { loanId },
    {
      $setOnInsert: {
        ...buildPaymentSkeleton(loanId, req.body, loan),
        createdBy: createdBy || undefined,
      },
    },
    { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true },
  );

  await Loan.findOneAndUpdate(
    { loanId },
    {
      $set: {
        do_number: doRecord?.do_refNo || doRecord?.doNumber || '',
        do_date: doRecord?.do_date || doRecord?.doDate || new Date(),
      },
    },
  );

  return res.status(201).json({
    success: true,
    loanId,
    data: payment,
    loan: { loanId, customerName, primaryMobile },
    message: `Loan file ${loanId} created with Payment and DO`,
  });
});

const getPayments = asyncHandler(async (req, res) => {
  const {
    search = '',
    showroomName = '',
    channelName = '',
    loanIds = '',
    skip = 0,
    page = 1,
    limit = 1000,
    noCount = '',
  } = req.query;

  const safeLimit = Math.min(Math.max(Number(limit) || 1000, 1), 5000);
  const safePage = Math.max(Number(page) || 1, 1);
  const safeSkip = Number.isFinite(Number(skip)) && Number(skip) >= 0
    ? Number(skip)
    : (safePage - 1) * safeLimit;
  const skipCount = new Set(['1', 'true', 'yes']).has(
    String(noCount || '').trim().toLowerCase(),
  );

  const andFilters = [];
  const safeLoanIds = parseCsvIds(loanIds);
  if (safeLoanIds.length) {
    andFilters.push({ loanId: { $in: safeLoanIds } });
  }
  const safeSearch = String(search || '').trim();
  if (safeSearch) {
    const re = new RegExp(escapeRegex(safeSearch), 'i');
    andFilters.push({
      $or: [
        { loanId: re },
        { customerName: re },
        { primaryMobile: re },
        { vehicleMake: re },
        { vehicleModel: re },
        { vehicleVariant: re },
        { showroomName: re },
        { channelName: re },
        { do_refNo: re },
        { doNumber: re },
      ],
    });
  }
  if (String(showroomName || '').trim()) {
    andFilters.push({
      showroomName: new RegExp(escapeRegex(String(showroomName).trim()), 'i'),
    });
  }
  if (String(channelName || '').trim()) {
    andFilters.push({
      channelName: new RegExp(escapeRegex(String(channelName).trim()), 'i'),
    });
  }

  const query =
    andFilters.length === 0
      ? {}
      : andFilters.length === 1
        ? andFilters[0]
        : { $and: andFilters };

  const dataPromise = Payment.find(query)
    .sort({ updatedAt: -1, _id: -1 })
    .skip(safeSkip)
    .limit(safeLimit)
    .lean();
  const totalPromise = skipCount
    ? Promise.resolve(null)
    : Payment.countDocuments(query);

  const [payments, countedTotal] = await Promise.all([dataPromise, totalPromise]);
  const total = skipCount
    ? safeSkip + payments.length + (payments.length === safeLimit ? 1 : 0)
    : Number(countedTotal || 0);

  res.json({
    success: true,
    data: (payments || []).map(sanitizePaymentForResponse),
    total,
    page: Math.floor(safeSkip / safeLimit) + 1,
    limit: safeLimit,
    skip: safeSkip,
    hasMore: skipCount
      ? payments.length === safeLimit
      : safeSkip + payments.length < total,
  });
});

const getPaymentsDashboardSnapshot = asyncHandler(async (req, res) => {
  const {
    search = '',
    status = 'all',
    type = 'all',
    dealerName = '',
    loanIds = '',
    skip = 0,
    page = 1,
    limit = 5000,
  } = req.query;

  const safeLimit = Math.min(Math.max(Number(limit) || 5000, 1), 10000);
  const safePage = Math.max(Number(page) || 1, 1);
  const safeSkip = Number.isFinite(Number(skip)) && Number(skip) >= 0
    ? Number(skip)
    : (safePage - 1) * safeLimit;

  const doQuery = buildPaymentsDashboardDoQuery({
    search,
    dealerName,
    loanIds,
  });

  const doDocsRaw = await DeliveryOrder.find(doQuery)
    .sort({ updatedAt: -1, _id: -1 })
    .lean();
  const doDocs = dedupeDOByLoanId(doDocsRaw);
  const loanIdsForJoin = Array.from(
    new Set(
      doDocs
        .map((row) => normalizeLoanId(row?.loanId || row?.do_loanId))
        .filter(Boolean),
    ),
  );

  const [loanDocs, paymentDocs] = await Promise.all([
    loanIdsForJoin.length
      ? Loan.find({ loanId: { $in: loanIdsForJoin } })
          .select(
            [
              '_id',
              'loanId',
              'customerName',
              'primaryMobile',
              'vehicleMake',
              'vehicleModel',
              'vehicleVariant',
              'showroomDealerName',
              'delivery_dealerName',
              'isFinanced',
              'typeOfLoan',
              'loanType',
              'createdAt',
              'updatedAt',
            ].join(' '),
          )
          .lean()
      : Promise.resolve([]),
    loanIdsForJoin.length
      ? Payment.find({ loanId: { $in: loanIdsForJoin } }).lean()
      : Promise.resolve([]),
  ]);

  const loanMap = new Map(
    loanDocs.map((loan) => [normalizeLoanId(loan?.loanId), loan]),
  );
  const paymentMap = new Map(
    paymentDocs.map((payment) => [normalizeLoanId(payment?.loanId), payment]),
  );

  const filteredRows = doDocs
    .map((doRec) => {
      const loanIdKey = normalizeLoanId(doRec?.loanId || doRec?.do_loanId);
      const loan = loanMap.get(loanIdKey) || { loanId: loanIdKey };
      const payment = paymentMap.get(loanIdKey) || {};
      return buildPaymentsDashboardRow(doRec, loan, payment);
    })
    .filter((row) => matchesPaymentsDashboardFilters(row, { status, type }))
    .sort((a, b) => (b.lastActivityTs || 0) - (a.lastActivityTs || 0));

  const pageRows = filteredRows.slice(safeSkip, safeSkip + safeLimit);
  const total = filteredRows.length;

  res.json({
    success: true,
    data: pageRows,
    total,
    page: Math.floor(safeSkip / safeLimit) + 1,
    limit: safeLimit,
    skip: safeSkip,
    hasMore: safeSkip + pageRows.length < total,
    meta: {
      search: String(search || '').trim(),
      status: String(status || 'all').trim().toLowerCase(),
      type: String(type || 'all').trim().toLowerCase(),
      joinedCases: filteredRows.length,
      rawDoCount: doDocs.length,
    },
  });
});

const getPaymentsByLoanId = asyncHandler(async (req, res) => {
  const { loanId } = req.params;

  const paymentRecord = await Payment.findOne({ loanId }).lean();

  if (paymentRecord) {
    res.json({ success: true, data: sanitizePaymentForResponse(paymentRecord) });
  } else {
    res.json({ success: true, data: null });
  }
});

const savePayment = asyncHandler(async (req, res) => {
  const { loanId } = req.params;
  if (Array.isArray(req.body?.autocreditsRows)) {
    req.body.autocreditsRows = sanitizeAutocreditsRows(req.body.autocreditsRows);
  }

  let paymentRecord = await Payment.findOne({ loanId });

  if (!paymentRecord) {
    const loan = await Loan.findOne({ loanId });
    if (loan && !isNewCarLoan(loan)) {
      res.status(400);
      throw new Error('Payment is only allowed for New Car loans (financed and cash)');
    }
    if (loan && isLegacyCase(loan)) {
      return res.status(200).json({
        success: true,
        data: null,
        skipped: true,
        message: 'Payment creation is paused for cases delivered/disbursed before 1 Feb 2026.',
      });
    }
  }

  if (paymentRecord) {
    Object.assign(paymentRecord, req.body);
    const updated = await paymentRecord.save();
    try {
      await syncPaymentsCommissionReceivableForLoan({ loanId });
    } catch (syncError) {
      console.error('Payments commission receivable sync failed (update):', syncError);
    }
    return res.json({ success: true, data: sanitizePaymentForResponse(updated?.toObject ? updated.toObject() : updated) });
  }

  const created = await Payment.create({
    ...req.body,
    loanId,
  });
  try {
    await syncPaymentsCommissionReceivableForLoan({ loanId });
  } catch (syncError) {
    console.error('Payments commission receivable sync failed (create):', syncError);
  }

  return res
    .status(201)
    .json({ success: true, data: sanitizePaymentForResponse(created?.toObject ? created.toObject() : created) });
});

export {
  createDirectPayment,
  getPayments,
  getPaymentsDashboardSnapshot,
  getPaymentsByLoanId,
  savePayment,
};
