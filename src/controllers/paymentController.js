import asyncHandler from 'express-async-handler';
import Payment from '../models/Payment.js';
import Loan from '../models/Loan.js';

const LEGACY_CUTOFF = new Date('2026-02-01T00:00:00.000Z');

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

  // Business rule: do not auto/create payment for cases delivered/disbursed before 1 Feb 2026.
  if (!paymentRecord) {
    const loan = await Loan.findOne({ loanId });
    if (loan && isLegacyCase(loan)) {
      return res.status(200).json({
        success: true,
        data: null,
        skipped: true,
        message:
          'Payment creation is paused for cases delivered/disbursed before 1 Feb 2026.',
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

export { getPayments, getPaymentsByLoanId, savePayment };
