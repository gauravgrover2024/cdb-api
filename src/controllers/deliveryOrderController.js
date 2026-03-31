import asyncHandler from 'express-async-handler';
import DeliveryOrder from '../models/DeliveryOrder.js';
import Loan from '../models/Loan.js';
import Counter from '../models/Counter.js';
import Payment from '../models/Payment.js';

const LEGACY_CUTOFF = new Date('2026-02-01T00:00:00.000Z');
const LOAN_ID_PREFIX = 'LN';

const escapeRegex = (value = '') =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

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
  const {
    search = '',
    status = '',
    dealerName = '',
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
  const safeStatus = String(status || '').trim();
  if (safeStatus) andFilters.push({ status: safeStatus });
  const safeDealer = String(dealerName || '').trim();
  if (safeDealer) {
    andFilters.push({
      dealerName: new RegExp(escapeRegex(safeDealer), 'i'),
    });
  }
  const safeSearch = String(search || '').trim();
  if (safeSearch) {
    const re = new RegExp(escapeRegex(safeSearch), 'i');
    andFilters.push({
      $or: [
        { loanId: re },
        { do_loanId: re },
        { dealerName: re },
        { vehicleModel: re },
        { doNumber: re },
        { do_refNo: re },
      ],
    });
  }

  const query =
    andFilters.length === 0
      ? {}
      : andFilters.length === 1
        ? andFilters[0]
        : { $and: andFilters };

  const dataPromise = DeliveryOrder.find(query)
    .sort({ updatedAt: -1, _id: -1 })
    .skip(safeSkip)
    .limit(safeLimit)
    .lean();
  const totalPromise = skipCount
    ? Promise.resolve(null)
    : DeliveryOrder.countDocuments(query);

  const [dos, countedTotal] = await Promise.all([dataPromise, totalPromise]);
  const total = skipCount
    ? safeSkip + dos.length + (dos.length === safeLimit ? 1 : 0)
    : Number(countedTotal || 0);

  res.json({
    success: true,
    data: dos,
    total,
    page: Math.floor(safeSkip / safeLimit) + 1,
    limit: safeLimit,
    skip: safeSkip,
    hasMore: skipCount
      ? dos.length === safeLimit
      : safeSkip + dos.length < total,
  });
});

// @desc    Get DO by LoanId
// @route   GET /api/do/:loanId
// @access  Public
const getDeliveryOrderByLoanId = asyncHandler(async (req, res) => {
  const loanId = String(req.params.loanId || "").trim();
  const doRecord = await DeliveryOrder.findOne({
    $or: [{ loanId }, { do_loanId: loanId }],
  })
    .sort({ updatedAt: -1, _id: -1 })
    .lean();

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

  let doRecord = await DeliveryOrder.findOne({
    $or: [{ loanId }, { do_loanId: loanId }],
  }).sort({ updatedAt: -1, _id: -1 });

  if (!doRecord && isLegacyNewCar(loan)) {
    return res.status(200).json({
      success: true,
      data: null,
      skipped: true,
      message: 'DO creation is paused for New Car loans delivered/disbursed before 1 Feb 2026.',
    });
  }

  if (doRecord) {
    Object.assign(doRecord, req.body, {
      loanId,
      do_loanId: loanId,
    });
    const updated = await doRecord.save();
    return res.json({ success: true, data: updated });
  }

  const created = await DeliveryOrder.create({
    ...req.body,
    loanId,
    do_loanId: loanId,
  });

  return res.status(201).json({ success: true, data: created });
});

export { createDirectDO, getDeliveryOrders, getDeliveryOrderByLoanId, saveDeliveryOrder };
