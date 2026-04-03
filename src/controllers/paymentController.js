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
import { syncPaymentsCommissionReceivableForLoan } from "../services/paymentsCommissionReceivableService.js";

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

// ── Helpers ──────────────────────────────────────────────────────────────────

// ── Helpers ──────────────────────────────────────────────────────────────────

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

  try { await Counter.create({ key, value: lastSeq }); } catch (e) { if (e?.code !== 11000) throw e; }

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

// ── Controllers ───────────────────────────────────────────────────────────────

// @desc    Create Payment directly — auto-creates loan file + DO skeleton
// @route   POST /api/payments
// @access  Public
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

// @desc    Get all payments
// @route   GET /api/payments
// @access  Public
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

// @desc    Get payment sheet by Loan ID
// @route   GET /api/payments/:loanId
// @access  Public
const getPaymentsByLoanId = asyncHandler(async (req, res) => {
  const { loanId } = req.params;

  const paymentRecord = await Payment.findOne({ loanId }).lean();

  if (paymentRecord) {
    res.json({ success: true, data: sanitizePaymentForResponse(paymentRecord) });
  } else {
    res.json({ success: true, data: null });
  }
});

// @desc    Save (Create/Update) Payment Sheet
// @route   POST/PUT /api/payments/:loanId
// @access  Public
const savePayment = asyncHandler(async (req, res) => {
  const { loanId } = req.params;
  if (Array.isArray(req.body?.autocreditsRows)) {
    req.body.autocreditsRows = sanitizeAutocreditsRows(req.body.autocreditsRows);
  }

  let paymentRecord = await Payment.findOne({ loanId });

  if (!paymentRecord) {
    const loan = await Loan.findOne({ loanId });
    // Point 5: only New Car loans (financed + cash) allowed
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
      console.error("Payments commission receivable sync failed (update):", syncError);
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
    console.error("Payments commission receivable sync failed (create):", syncError);
  }

  return res
    .status(201)
    .json({ success: true, data: sanitizePaymentForResponse(created?.toObject ? created.toObject() : created) });
});

export { createDirectPayment, getPayments, getPaymentsByLoanId, savePayment };
