import asyncHandler from 'express-async-handler';
import DeliveryOrder from '../models/DeliveryOrder.js';
import Loan from '../models/Loan.js';
import Counter from '../models/Counter.js';
import Payment from '../models/Payment.js';

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

const normalizeLoanType = (loan = {}) => {
  const raw =
    loan?.typeOfLoan ||
    loan?.loanType ||
    loan?.caseType ||
    loan?.vehicleType ||
    '';
  return String(raw).trim().toLowerCase().replace(/[-_\s]+/g, ' ');
};

const isNewCarLoan = (loan = {}) => {
  const normalized = normalizeLoanType(loan);
  if (!normalized) return false;

  if (
    normalized.includes('used') ||
    normalized.includes('refinance') ||
    normalized.includes('cash in')
  ) {
    return false;
  }

  return (
    normalized === 'new' ||
    normalized.includes('new car') ||
    normalized.includes('newcar')
  );
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

const isLegacyNewCar = (loan = {}) => {
  if (!isNewCarLoan(loan)) return false;
  const businessDate = parseBusinessDate(loan);
  if (!businessDate) return false;
  return businessDate < LEGACY_CUTOFF;
};

// ── Controllers ───────────────────────────────────────────────────────────────

// @desc    Create DO directly — auto-creates loan file + payment skeleton
// @route   POST /api/do
// @access  Public
const createDirectDO = asyncHandler(async (req, res) => {
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

  const doRecord = await DeliveryOrder.create({
    loanId,
    do_loanId: loanId,
    dealerName: dealerName || '',
    dealerAddress: dealerAddress || '',
    vehicleModel: vehicleModel || '',
    vehicleColor: vehicleColor || '',
    createdBy: createdBy || undefined,
  });

  await Payment.create({
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
    data: doRecord,
    loan: { loanId, customerName, primaryMobile },
    message: `Loan file ${loanId} created with DO and Payment`,
  });
});

// @desc    Get all DOs
// @route   GET /api/do
// @access  Public
const getDeliveryOrders = asyncHandler(async (req, res) => {
  const dos = await DeliveryOrder.find({}).sort({ createdAt: -1 });

  res.json({
    success: true,
    data: dos,
  });
});

// @desc    Get DO by LoanId
// @route   GET /api/do/:loanId
// @access  Public
const getDeliveryOrderByLoanId = asyncHandler(async (req, res) => {
  const doRecord = await DeliveryOrder.findOne({ loanId: req.params.loanId });

  if (doRecord) {
    res.json({ success: true, data: doRecord });
  } else {
    res.json({ success: true, data: null });
  }
});

// @desc    Create or Update DO
// @route   POST/PUT /api/do/:loanId
// @access  Public
const saveDeliveryOrder = asyncHandler(async (req, res) => {
  const { loanId } = req.params;

  const loan = await Loan.findOne({ loanId });
  if (!loan) {
    res.status(404);
    throw new Error('Loan not found');
  }

  if (!isNewCarLoan(loan)) {
    res.status(400);
    throw new Error('Delivery Order is only allowed for New Car loans');
  }

  let doRecord = await DeliveryOrder.findOne({ loanId });

  if (!doRecord && isLegacyNewCar(loan)) {
    return res.status(200).json({
      success: true,
      data: null,
      skipped: true,
      message: 'DO creation is paused for New Car loans delivered/disbursed before 1 Feb 2026.',
    });
  }

  if (doRecord) {
    Object.assign(doRecord, req.body);
    const updated = await doRecord.save();
    return res.json({ success: true, data: updated });
  }

  const created = await DeliveryOrder.create({
    ...req.body,
    loanId,
  });

  return res.status(201).json({ success: true, data: created });
});

export { createDirectDO, getDeliveryOrders, getDeliveryOrderByLoanId, saveDeliveryOrder };
