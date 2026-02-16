import asyncHandler from 'express-async-handler';
import Payment from '../models/Payment.js';
import Loan from '../models/Loan.js';

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
  
  // Find the single payment sheet for this loan
  const paymentRecord = await Payment.findOne({ loanId });

  if (paymentRecord) {
    res.json({ success: true, data: paymentRecord });
  } else {
    // Return null data so frontend knows to init empty
    res.json({ success: true, data: null });
  }
});

// @desc    Save (Create/Update) Payment Sheet
// @route   PUT /api/payments/:loanId
// @access  Public
const savePayment = asyncHandler(async (req, res) => {
  const { loanId } = req.params;
  
  let paymentRecord = await Payment.findOne({ loanId });

  if (paymentRecord) {
    // Update existing
    Object.assign(paymentRecord, req.body);
    const updated = await paymentRecord.save();
    res.json({ success: true, data: updated });
  } else {
    // Create new
    const created = await Payment.create({
      ...req.body,
      loanId,
    });
    res.status(201).json({ success: true, data: created });
  }
});

export { getPayments, getPaymentsByLoanId, savePayment };
