import asyncHandler from 'express-async-handler';
import DeliveryOrder from '../models/DeliveryOrder.js';
import Loan from '../models/Loan.js';

const normalizeLoanType = (loan = {}) => {
  const raw = loan?.vehicleType || loan?.loanType || loan?.typeOfLoan || '';
  return String(raw).trim().toLowerCase().replace(/[-_\s]+/g, ' ');
};

const isNewCarLoan = (loan = {}) => {
  const normalized = normalizeLoanType(loan);
  return (
    normalized === 'new car' ||
    normalized === 'newcar' ||
    normalized === 'new car loan' ||
    normalized === 'new'
  );
};

const parseDisbursementDate = (loan = {}) => {
  const candidates = [
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
  const disbursementDate = parseDisbursementDate(loan);
  if (!disbursementDate) return false;
  return disbursementDate.getFullYear() <= 2025;
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

  // Business rule: do not auto/create DO for New Car loans disbursed in or before 2025.
  if (!doRecord && isLegacyNewCar(loan)) {
    return res.status(200).json({
      success: true,
      data: null,
      skipped: true,
      message:
        'DO creation is paused for New Car loans disbursed in 2025 or earlier.',
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
