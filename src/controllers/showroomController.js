import asyncHandler from 'express-async-handler';
import Showroom from '../models/Showroom.js';

// @desc    Get all showrooms with search and filtering
// @route   GET /api/showrooms
// @access  Public
const getShowrooms = asyncHandler(async (req, res) => {
  const { q, city, status } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : 50;
  const skip = Number(req.query.skip) || 0;

  let query = {};

  // Text search across name, mobile, city
  if (q) {
    query.$or = [
      { name: new RegExp(q, 'i') },
      { businessName: new RegExp(q, 'i') },
      { mobile: new RegExp(q, 'i') },
      { contactPerson: new RegExp(q, 'i') },
      { showroomId: new RegExp(q, 'i') },
    ];
  }

  if (city) query.city = new RegExp(city, 'i');
  if (status) query.status = status;

  const count = await Showroom.countDocuments(query);
  const showrooms = await Showroom.find(query)
    .sort({ createdAt: -1 })
    .limit(pageSize)
    .skip(skip);

  res.json({
    success: true,
    count,
    data: showrooms,
  });
});

// @desc    Get showroom by ID
// @route   GET /api/showrooms/:id
// @access  Public
const getShowroomById = asyncHandler(async (req, res) => {
  const showroom = await Showroom.findById(req.params.id);
  
  if (showroom) {
    res.json({ success: true, data: showroom });
  } else {
    res.status(404);
    throw new Error('Showroom not found');
  }
});

// @desc    Search showrooms by name or mobile (for auto-complete)
// @route   GET /api/showrooms/search
// @access  Public
const searchShowrooms = asyncHandler(async (req, res) => {
  const { term } = req.query;
  
  if (!term) {
    res.status(400);
    throw new Error('Search term is required');
  }

  const showrooms = await Showroom.find({
    $or: [
      { name: new RegExp(term, 'i') },
      { mobile: new RegExp(term, 'i') },
      { showroomId: new RegExp(term, 'i') },
    ],
    status: 'Active',
  })
    .limit(10)
    .select('showroomId name mobile contactPerson city address commissionRate outstandingCommission');

  res.json({
    success: true,
    data: showrooms,
  });
});

// @desc    Create new showroom
// @route   POST /api/showrooms
// @access  Private
const createShowroom = asyncHandler(async (req, res) => {
  const { name, mobile, address, city } = req.body;

  if (!name || !mobile || !address || !city) {
    res.status(400);
    throw new Error('Please provide name, mobile, address, and city');
  }

  // Check if showroom with same mobile already exists
  const showroomExists = await Showroom.findOne({ mobile });
  if (showroomExists) {
    res.status(400);
    throw new Error('Showroom with this mobile number already exists');
  }

  const showroom = await Showroom.create({
    ...req.body,
    createdBy: req.user?._id,
  });

  if (showroom) {
    res.status(201).json({ success: true, data: showroom });
  } else {
    res.status(400);
    throw new Error('Invalid showroom data');
  }
});

// @desc    Update showroom
// @route   PUT /api/showrooms/:id
// @access  Private
const updateShowroom = asyncHandler(async (req, res) => {
  const showroom = await Showroom.findById(req.params.id);

  if (showroom) {
    Object.keys(req.body).forEach((key) => {
      showroom[key] = req.body[key];
    });
    
    showroom.lastModifiedBy = req.user?._id;

    const updatedShowroom = await showroom.save();
    res.json({ success: true, data: updatedShowroom });
  } else {
    res.status(404);
    throw new Error('Showroom not found');
  }
});

// @desc    Delete showroom
// @route   DELETE /api/showrooms/:id
// @access  Private
const deleteShowroom = asyncHandler(async (req, res) => {
  const showroom = await Showroom.findById(req.params.id);

  if (showroom) {
    // Soft delete by setting status to Inactive
    showroom.status = 'Inactive';
    await showroom.save();
    
    res.json({ success: true, message: 'Showroom deactivated successfully' });
  } else {
    res.status(404);
    throw new Error('Showroom not found');
  }
});

// @desc    Add payment/commission entry to showroom
// @route   POST /api/showrooms/:id/payments
// @access  Private
const addShowroomPayment = asyncHandler(async (req, res) => {
  const showroom = await Showroom.findById(req.params.id);

  if (!showroom) {
    res.status(404);
    throw new Error('Showroom not found');
  }

  const { amount, excessAmount, adjustedAmount, loanId, paymentMode, remarks } = req.body;

  const paymentEntry = {
    date: new Date(),
    amount: amount || 0,
    excessAmount: excessAmount || 0,
    adjustedAmount: adjustedAmount || 0,
    loanId,
    paymentMode,
    remarks,
  };

  showroom.paymentHistory.push(paymentEntry);
  
  // Update commission totals
  if (excessAmount > 0) {
    showroom.totalCommissionReceivable += excessAmount;
  }
  if (adjustedAmount > 0) {
    showroom.totalCommissionPaid += adjustedAmount;
  }

  const updatedShowroom = await showroom.save();
  res.json({ success: true, data: updatedShowroom });
});

// @desc    Get showroom statistics
// @route   GET /api/showrooms/:id/stats
// @access  Public
const getShowroomStats = asyncHandler(async (req, res) => {
  const showroom = await Showroom.findById(req.params.id);

  if (!showroom) {
    res.status(404);
    throw new Error('Showroom not found');
  }

  const stats = {
    totalLoansProcessed: showroom.totalLoansProcessed,
    totalBusinessVolume: showroom.totalBusinessVolume,
    totalCommissionReceivable: showroom.totalCommissionReceivable,
    totalCommissionPaid: showroom.totalCommissionPaid,
    outstandingCommission: showroom.outstandingCommission,
    paymentHistoryCount: showroom.paymentHistory.length,
    status: showroom.status,
  };

  res.json({ success: true, data: stats });
});

export {
  getShowrooms,
  getShowroomById,
  searchShowrooms,
  createShowroom,
  updateShowroom,
  deleteShowroom,
  addShowroomPayment,
  getShowroomStats,
};
