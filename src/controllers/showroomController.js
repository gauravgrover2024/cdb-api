import asyncHandler from 'express-async-handler';
import Showroom from '../models/Showroom.js';

const cleanText = (value) => String(value ?? '').replace(/\s+/g, ' ').trim();

const canonicalBrandKey = (value) => {
  const raw = cleanText(value).toLowerCase().replace(/[_-]+/g, ' ');
  if (!raw) return '';
  const compact = raw.replace(/[^a-z0-9]/g, '');

  if (['maruti', 'marutisuzuki', 'suzuki', 'msil'].includes(compact)) return 'maruti';
  if (['mercedes', 'mercedesbenz', 'benz', 'mercedesbenzcars'].includes(compact)) return 'mercedes-benz';
  if (['bmw', 'bmwindia'].includes(compact)) return 'bmw';
  if (['landrover', 'jaguarlandrover', 'jlr'].includes(compact)) return 'land-rover';
  if (['volkswagen', 'vw'].includes(compact)) return 'volkswagen';
  if (['mahindra', 'mahindramahindra'].includes(compact)) return 'mahindra';
  if (['mg', 'morrisgarages', 'morrisgarage'].includes(compact)) return 'mg';
  if (['tata', 'tatamotors'].includes(compact)) return 'tata';
  if (['hyundai'].includes(compact)) return 'hyundai';
  if (['kia'].includes(compact)) return 'kia';
  if (['honda'].includes(compact)) return 'honda';
  if (['toyota', 'toyotakirloskar'].includes(compact)) return 'toyota';
  if (['renault'].includes(compact)) return 'renault';
  if (['nissan'].includes(compact)) return 'nissan';
  if (['skoda'].includes(compact)) return 'skoda';
  if (['audi'].includes(compact)) return 'audi';
  if (['jeep'].includes(compact)) return 'jeep';
  if (['isuzu'].includes(compact)) return 'isuzu';
  if (['citroen'].includes(compact)) return 'citroen';
  if (['byd'].includes(compact)) return 'byd';
  if (['force', 'forcemotors'].includes(compact)) return 'force';
  if (['jaguar'].includes(compact)) return 'jaguar';
  if (['astonmartin'].includes(compact)) return 'aston-martin';
  if (['bentley'].includes(compact)) return 'bentley';

  return raw;
};

const safeRegex = (value) => {
  const escaped = String(value ?? '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(escaped, 'i');
};

// @desc    Get all showrooms with search and filtering
// @route   GET /api/showrooms
// @access  Public
const getShowrooms = asyncHandler(async (req, res) => {
  const { q, city, status } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : 50;
  const skip = Number(req.query.skip) || 0;

  const query = {};

  if (q) {
    const re = safeRegex(q);
    query.$or = [
      { name: re },
      { businessName: re },
      { mobile: re },
      { contactPerson: re },
      { showroomId: re },
    ];
  }

  if (city) query.city = safeRegex(city);
  if (status) query.status = status;

  const count = await Showroom.countDocuments(query);
  const showrooms = await Showroom.find(query)
    .sort({ createdAt: -1 })
    .limit(pageSize)
    .skip(skip)
    .lean();

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
  const term = cleanText(req.query.term || '');
  const brand = cleanText(req.query.brand || '');
  const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 5000);

  const query = { status: 'Active' };

  const brandKey = canonicalBrandKey(brand);
  if (brandKey) {
    // Indexed array exact-match filter (fast)
    query.brandKeys = brandKey;
  }

  if (term) {
    const termKey = canonicalBrandKey(term);
    const looksLikeSameBrandSearch = brandKey && termKey && termKey === brandKey;

    if (!looksLikeSameBrandSearch) {
      const re = safeRegex(term);
      // Strict showroom-name search only (contiguous typed sequence).
      query.name = re;
    }
  }

  const showrooms = await Showroom.find(query)
    .sort({ name: 1, city: 1 })
    .limit(limit)
    .select('showroomId name businessName mobile contactPerson city address brands brandKeys commissionRate outstandingCommission')
    .lean();

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

  const showroomExists = await Showroom.findOne({ mobile });
  if (showroomExists) {
    res.status(400);
    throw new Error('Showroom with this mobile number already exists');
  }

  const brandKeys = Array.isArray(req.body?.brands)
    ? [...new Set(req.body.brands.map((b) => canonicalBrandKey(b)).filter(Boolean))]
    : [];

  const showroom = await Showroom.create({
    ...req.body,
    brandKeys,
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

    if (Array.isArray(req.body?.brands)) {
      showroom.brandKeys = [...new Set(req.body.brands.map((b) => canonicalBrandKey(b)).filter(Boolean))];
    }

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
