import asyncHandler from 'express-async-handler';
import Payment from '../models/Payment.js';
import Loan from '../models/Loan.js';
import Counter from '../models/Counter.js';
import DeliveryOrder from '../models/DeliveryOrder.js';

const LEGACY_CUTOFF = new Date('2026-02-01T00:00:00.000Z');
const LOAN_ID_PREFIX = 'LN';

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

const parseBusinessDate = (loan = {}) => {
  const candidates = [
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
    if (!candidate) continue;
    const date = new Date(candidate);
    if (!Number.isNaN(date.getTime())) return date;
  }

  return null;
};

const isLegacyCase = (loan = {}) => {
  const businessDate = parseBusinessDate(loan);
  if (!businessDate) return false;
  return businessDate < LEGACY_CUTOFF;
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

  await DeliveryOrder.create({
    loanId,
    do_loanId: loanId,
    dealerName: dealerName || '',
    dealerAddress: dealerAddress || '',
    vehicleModel: vehicleModel || '',
    vehicleColor: vehicleColor || '',
    createdBy: createdBy || undefined,
  });

  const payment = await Payment.create({
    loanId,
    showroomRows: [],
    entryTotals: {},
    isVerified: false,
    autocreditsRows: [],
    autocreditsTotals: {},
    isAutocreditsVerified: false,
    createdBy: createdBy || undefined,
  });

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
  const payments = await Payment.find({}).sort({ updatedAt: -1, createdAt: -1 });

  res.json({
    success: true,
    data: payments,
  });
});

// @desc    Get payment sheet by Loan ID
// @route   GET /api/payments/:loanId
// @access  Public
const getPaymentsByLoanId = asyncHandler(async (req, res) => {
  const { loanId } = req.params;

  const paymentRecord = await Payment.findOne({ loanId });

  if (paymentRecord) {
    res.json({ success: true, data: paymentRecord });
  } else {
    res.json({ success: true, data: null });
  }
});

// @desc    Save (Create/Update) Payment Sheet
// @route   POST/PUT /api/payments/:loanId
// @access  Public
const savePayment = asyncHandler(async (req, res) => {
  const { loanId } = req.params;

  let paymentRecord = await Payment.findOne({ loanId });

  if (!paymentRecord) {
    const loan = await Loan.findOne({ loanId });
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
    return res.json({ success: true, data: updated });
  }

  const created = await Payment.create({
    ...req.body,
    loanId,
  });

  return res.status(201).json({ success: true, data: created });
});

export { createDirectPayment, getPayments, getPaymentsByLoanId, savePayment };
