import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import Counter from "../models/Counter.js";
import Customer from "../models/Customer.js";
import InsuranceCase from "../models/InsuranceCase.js";

const INSURANCE_COUNTER_PREFIX = "insurance_case_id_sequence_";
const INSURANCE_ID_PREFIX = "INS";

const safeString = (value) =>
  value === undefined || value === null ? "" : String(value);

const toObjectIdOrNull = (value) => {
  const v = safeString(value).trim();
  if (!v) return null;
  return mongoose.Types.ObjectId.isValid(v)
    ? new mongoose.Types.ObjectId(v)
    : null;
};

const buildCustomerSnapshot = (customer) => {
  if (!customer) return {};
  return {
    customerName: safeString(customer.customerName).trim(),
    primaryMobile: safeString(customer.primaryMobile).trim(),
    email: safeString(customer.email || customer.emailAddress).trim(),
    panNumber: safeString(customer.panNumber).trim(),
    residenceAddress: safeString(customer.residenceAddress).trim(),
    pincode: safeString(customer.pincode).trim(),
    city: safeString(customer.city).trim(),
  };
};

const getNextInsuranceCaseId = async () => {
  const year = new Date().getFullYear();
  const key = `${INSURANCE_COUNTER_PREFIX}${year}`;
  const next = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    { upsert: true, new: true },
  );
  const seq = Number(next?.value || 0);
  return `${INSURANCE_ID_PREFIX}-${year}-${String(seq).padStart(4, "0")}`;
};

// @desc    Get insurance cases (basic list)
// @route   GET /api/insurance
// @access  Public
export const getInsuranceCases = asyncHandler(async (req, res) => {
  const limit = Math.min(200, Math.max(1, Number(req.query.limit || 50)));
  const skip = Math.max(0, Number(req.query.skip || 0));

  const count = await InsuranceCase.countDocuments({});
  const rows = await InsuranceCase.find({})
    .sort({ updatedAt: -1 })
    .limit(limit)
    .skip(skip);

  res.json({ success: true, count, data: rows });
});

// @desc    Get insurance case by id (supports _id or caseId)
// @route   GET /api/insurance/:id
// @access  Public
export const getInsuranceCaseById = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const byObjectId = mongoose.Types.ObjectId.isValid(raw)
    ? await InsuranceCase.findById(raw)
    : null;
  const doc = byObjectId || (await InsuranceCase.findOne({ caseId: raw }));

  if (!doc) {
    res.status(404);
    throw new Error("Insurance case not found");
  }

  res.json({ success: true, data: doc });
});

// @desc    Create insurance case
// @route   POST /api/insurance
// @access  Public
export const createInsuranceCase = asyncHandler(async (req, res) => {
  const payload = req.body || {};

  const caseId = await getNextInsuranceCaseId();
  const customerId = toObjectIdOrNull(payload.customerId);

  let customerSnapshot = payload.customerSnapshot || {};
  if (customerId) {
    const customer = await Customer.findById(customerId);
    if (customer) customerSnapshot = buildCustomerSnapshot(customer);
  }

  const doc = await InsuranceCase.create({
    ...payload,
    caseId,
    customerId: customerId || undefined,
    customerSnapshot,
    status: payload.status || "draft",
    currentStep: Number(payload.currentStep || 1),
  });

  res.status(201).json({ success: true, data: doc });
});

// @desc    Update insurance case (full replace/merge style)
// @route   PUT /api/insurance/:id
// @access  Public
export const updateInsuranceCase = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const payload = req.body || {};

  const doc =
    (mongoose.Types.ObjectId.isValid(raw)
      ? await InsuranceCase.findById(raw)
      : null) || (await InsuranceCase.findOne({ caseId: raw }));

  if (!doc) {
    res.status(404);
    throw new Error("Insurance case not found");
  }

  const customerId =
    toObjectIdOrNull(payload.customerId) || doc.customerId || null;
  let customerSnapshot = payload.customerSnapshot || doc.customerSnapshot || {};
  if (
    customerId &&
    (!payload.customerSnapshot ||
      Object.keys(payload.customerSnapshot || {}).length === 0)
  ) {
    const customer = await Customer.findById(customerId);
    if (customer) customerSnapshot = buildCustomerSnapshot(customer);
  }

  Object.assign(doc, payload, {
    customerId: customerId || undefined,
    customerSnapshot,
    currentStep: Number(payload.currentStep || doc.currentStep || 1),
    status: safeString(payload.status || doc.status || "draft"),
  });

  const saved = await doc.save();
  res.json({ success: true, data: saved });
});

// @desc    Delete insurance case
// @route   DELETE /api/insurance/:id
// @access  Public
export const deleteInsuranceCase = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();

  const doc =
    (mongoose.Types.ObjectId.isValid(raw)
      ? await InsuranceCase.findById(raw)
      : null) || (await InsuranceCase.findOne({ caseId: raw }));

  if (!doc) {
    console.warn(`[Insurance Delete] Case not found: ${raw}`);
    res.status(404);
    throw new Error("Insurance case not found");
  }

  await InsuranceCase.deleteOne({ _id: doc._id });
  console.log(
    `[Insurance Delete] Successfully deleted case: ${doc.caseId} (ID: ${doc._id})`,
  );

  res.json({
    success: true,
    message: "Insurance case deleted successfully",
    data: { id: doc._id, caseId: doc.caseId },
  });
});
