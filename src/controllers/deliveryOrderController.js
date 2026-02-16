import asyncHandler from 'express-async-handler';
import DeliveryOrder from '../models/DeliveryOrder.js';
import Payment from '../models/Payment.js';
import Loan from '../models/Loan.js';

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
// @route   POST /api/do/:loanId
// @access  Public
const saveDeliveryOrder = asyncHandler(async (req, res) => {
  const { loanId } = req.params;

  const loan = await Loan.findOne({ loanId });
  if (!loan) {
    res.status(404);
    throw new Error('Loan not found');
  }

  const rawType = loan.vehicleType || loan.loanType || loan.typeOfLoan || "";
  const normalized = String(rawType).trim().toUpperCase().replace(/[-_\s]+/g, " ");
  const isNewCar = normalized === "NEW CAR" || normalized === "NEWCAR" || normalized === "NEW CAR LOAN" || normalized === "NEW";
  if (!isNewCar) {
    res.status(400);
    throw new Error('Delivery Order is only allowed for New Car loans');
  }
  
  let doRecord = await DeliveryOrder.findOne({ loanId });

  if (doRecord) {
    // Update
    Object.assign(doRecord, req.body);
    const updated = await doRecord.save();

    await Payment.findOneAndUpdate(
      { loanId },
      { $setOnInsert: { loanId, do_loanId: loanId } },
      { upsert: true, new: true }
    );

    res.json({ success: true, data: updated });
  } else {
    // Create
    const created = await DeliveryOrder.create({
      ...req.body,
      loanId
    });

    await Payment.findOneAndUpdate(
      { loanId },
      { $setOnInsert: { loanId, do_loanId: loanId } },
      { upsert: true, new: true }
    );

    res.status(201).json({ success: true, data: created });
  }
});

export { getDeliveryOrders, getDeliveryOrderByLoanId, saveDeliveryOrder };
