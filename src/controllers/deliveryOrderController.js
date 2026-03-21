import asyncHandler from 'express-async-handler';
import DeliveryOrder from '../models/DeliveryOrder.js';
import Loan from '../models/Loan.js';

const LEGACY_CUTOFF = new Date('2026-02-01T00:00:00.000Z');

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
    // If not found, return null data rather than 404 to avoid frontend errors if it just checks existence
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

  // Business rule: do not auto/create DO for New Car loans delivered/disbursed before 1 Feb 2026.
  if (!doRecord && isLegacyNewCar(loan)) {
    return res.status(200).json({
      success: true,
      data: null,
      skipped: true,
      message:
        'DO creation is paused for New Car loans delivered/disbursed before 1 Feb 2026.',
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

export { getDeliveryOrders, getDeliveryOrderByLoanId, saveDeliveryOrder };
