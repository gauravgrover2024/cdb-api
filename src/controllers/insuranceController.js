import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import Counter from "../models/Counter.js";
import Customer from "../models/Customer.js";
import InsuranceCase from "../models/InsuranceCase.js";
import InsurancePayoutRate from "../models/InsurancePayoutRate.js";
import Receivable from "../models/Receivable.js";
import VehicleFeature from "../models/VehicleFeature.js";
import VehicleRecord from "../models/VehicleRecord.js";
import { upsertChannelPartner } from "../services/channelPartnerUpsert.js";

const INSURANCE_COUNTER_PREFIX = "insurance_case_id_sequence_";
const INSURANCE_ID_PREFIX = "INS";
const INSURANCE_TEMP_REG_COUNTER_KEY = "insurance_temp_registration_sequence";
const DEFAULT_INSURANCE_PAYOUT_PERCENTAGE = 10;

const syncChannelPartnerOnInsurancePayload = async (payload = {}) => {
  try {
    const channel = await upsertChannelPartner(payload);
    if (channel?.channelId) {
      payload.channelDealerNo = channel.channelId;
    }
  } catch (err) {
    console.warn("[Insurance] Channel partner upsert skipped:", err?.message);
  }
  return payload;
};

const safeString = (value) =>
  value === undefined || value === null ? "" : String(value);

const toObjectIdOrNull = (value) => {
  const v = safeString(value).trim();
  if (!v) return null;
  return mongoose.Types.ObjectId.isValid(v)
    ? new mongoose.Types.ObjectId(v)
    : null;
};

const resolveCustomerObjectId = async (value) => {
  const raw = safeString(value).trim();
  if (!raw) return null;

  const asObjectId = toObjectIdOrNull(raw);
  if (asObjectId) {
    const byId = await Customer.findById(asObjectId).select("_id").lean();
    if (byId?._id) return byId._id;
  }

  const byCustomId = await Customer.findOne({ customerId: raw })
    .select("_id")
    .lean();
  return byCustomId?._id || null;
};

const hasOwn = (obj, key) =>
  Object.prototype.hasOwnProperty.call(obj || {}, key);

const stripImmutableInsuranceFields = (payload = {}) => {
  const cleaned = { ...(payload || {}) };
  delete cleaned._id;
  delete cleaned.__v;
  delete cleaned.id;
  delete cleaned.createdAt;
  delete cleaned.updatedAt;
  return cleaned;
};

const sanitizePaymentHistoryRow = (row = {}) => {
  if (!row || typeof row !== "object") return null;
  const normalized = { ...row };
  const rawId = safeString(row._id).trim();
  if (rawId && mongoose.Types.ObjectId.isValid(rawId)) {
    normalized._id = new mongoose.Types.ObjectId(rawId);
  } else {
    delete normalized._id;
  }

  normalized.clientEntryId = safeString(
    row.clientEntryId || row.client_entry_id || row.id || rawId,
  ).trim();
  normalized.idempotencyKey = safeString(
    row.idempotencyKey || row.idempotency_key,
  ).trim();

  if (row.date === null || safeString(row.date).trim() === "") {
    normalized.date = null;
  } else {
    normalized.date = toDateOrNull(row.date);
  }

  if (hasOwn(row, "entry_type") && !hasOwn(row, "entryType")) {
    normalized.entryType = row.entry_type;
  }
  if (hasOwn(row, "payment_type") && !hasOwn(row, "paymentType")) {
    normalized.paymentType = row.payment_type;
  }
  if (hasOwn(row, "payment_mode") && !hasOwn(row, "paymentMode")) {
    normalized.paymentMode = row.payment_mode;
  }
  if (hasOwn(row, "transaction_ref") && !hasOwn(row, "transactionRef")) {
    normalized.transactionRef = row.transaction_ref;
  }

  delete normalized.key;
  delete normalized.id;
  delete normalized.amountColor;
  delete normalized.amountDirection;
  delete normalized.amountPrefix;
  delete normalized.typeLabel;
  delete normalized.entry_type;
  delete normalized.payment_type;
  delete normalized.payment_mode;
  delete normalized.transaction_ref;
  return normalized;
};

const normalizePaymentHistoryPayload = (input) =>
  (Array.isArray(input) ? input : [])
    .map((row) => sanitizePaymentHistoryRow(row))
    .filter(Boolean);

const buildCustomerSnapshot = (customer) => {
  if (!customer) return {};
  return {
    customerName: safeString(customer.customerName).trim(),
    companyName: safeString(customer.companyName).trim(),
    contactPersonName: safeString(customer.contactPersonName).trim(),
    primaryMobile: safeString(customer.primaryMobile).trim(),
    email: safeString(customer.email || customer.emailAddress).trim(),
    panNumber: safeString(customer.panNumber).trim(),
    residenceAddress: safeString(customer.residenceAddress).trim(),
    pincode: safeString(customer.pincode).trim(),
    city: safeString(customer.city).trim(),
  };
};

const normalizeMobile10 = (value) => {
  const digits = safeString(value).replace(/\D/g, "");
  if (!digits) return "";
  return digits.length >= 10 ? digits.slice(-10) : digits;
};

const syncCustomerFromInsurancePayload = async (customerId, payload = {}) => {
  if (!customerId) return null;
  const customer = await Customer.findById(customerId);
  if (!customer) return null;

  const nextCustomerName = safeString(payload.customerName).trim();
  const nextCompanyName = safeString(payload.companyName).trim();
  const nextContactPersonName = safeString(payload.contactPersonName).trim();
  const nextMobile = normalizeMobile10(payload.mobile);
  const nextAltMobile = normalizeMobile10(payload.alternatePhone);
  const nextEmail = safeString(payload.email).trim();
  const nextGender = safeString(payload.gender).trim();
  const nextPan = safeString(payload.panNumber).trim();
  const nextAadhaar = safeString(
    payload.aadhaarNumber || payload.aadharNumber,
  ).trim();
  const nextGst = safeString(payload.gstNumber).trim();
  const nextResidenceAddress = safeString(payload.residenceAddress).trim();
  const nextCity = safeString(payload.city).trim();
  const nextPincode = safeString(payload.pincode)
    .replace(/\D/g, "")
    .slice(0, 6);
  const nextNomineeName = safeString(payload.nomineeName).trim();
  const nextNomineeRelation = safeString(
    payload.nomineeRelationship || payload.nomineeRelation,
  ).trim();
  const nextNomineeDob = toDateOrNull(payload.nomineeDob);
  const nextReferenceName = safeString(payload.referenceName).trim();
  const nextReferencePhone = normalizeMobile10(payload.referencePhone);

  let hasChanges = false;
  const setIfPresent = (field, value) => {
    if (value === undefined || value === null || value === "") return;
    if (safeString(customer[field]) === safeString(value)) return;
    customer[field] = value;
    hasChanges = true;
  };

  setIfPresent("customerName", nextCustomerName);
  setIfPresent("companyName", nextCompanyName);
  setIfPresent("contactPersonName", nextContactPersonName);
  setIfPresent("primaryMobile", nextMobile);
  if (nextAltMobile) {
    const existingExtraMobiles = Array.isArray(customer.extraMobiles)
      ? customer.extraMobiles
      : [];
    if (!existingExtraMobiles.includes(nextAltMobile)) {
      customer.extraMobiles = [nextAltMobile, ...existingExtraMobiles].slice(
        0,
        3,
      );
      hasChanges = true;
    }
  }
  setIfPresent("email", nextEmail);
  setIfPresent("emailAddress", nextEmail);
  setIfPresent("gender", nextGender);
  setIfPresent("panNumber", nextPan);
  setIfPresent("aadharNumber", nextAadhaar);
  setIfPresent("aadhaarNumber", nextAadhaar);
  setIfPresent("gstNumber", nextGst);
  setIfPresent("residenceAddress", nextResidenceAddress);
  setIfPresent("city", nextCity);
  setIfPresent("pincode", nextPincode);
  setIfPresent("nomineeName", nextNomineeName);
  setIfPresent("nomineeRelation", nextNomineeRelation);
  if (nextNomineeDob) {
    const existingDob = customer.nomineeDob
      ? new Date(customer.nomineeDob).toISOString()
      : "";
    const nextDob = nextNomineeDob.toISOString();
    if (existingDob !== nextDob) {
      customer.nomineeDob = nextNomineeDob;
      hasChanges = true;
    }
  }
  setIfPresent("reference1_name", nextReferenceName);
  setIfPresent("reference1_mobile", nextReferencePhone);

  if (hasChanges) {
    await customer.save();
  }
  return customer;
};

const normalizeStep1Payload = (payload = {}, options = {}) => {
  const { applyDefaults = true } = options || {};
  const hasSource =
    hasOwn(payload, "source") ||
    hasOwn(payload, "sourceOrigin") ||
    hasOwn(payload, "recordSource");
  const sourceNormalized = safeString(
    payload.source ||
      payload.sourceOrigin ||
      payload.recordSource ||
      (applyDefaults ? "Direct" : ""),
  ).trim();
  const hasPayout =
    hasOwn(payload, "payoutPercent") || hasOwn(payload, "payoutPercentage");
  const payoutPercentRaw = Number(
    payload.payoutPercent ??
      payload.payoutPercentage ??
      (applyDefaults ? 0 : undefined),
  );
  const payoutPercent = Number.isFinite(payoutPercentRaw)
    ? payoutPercentRaw
    : applyDefaults
      ? 0
      : undefined;

  const normalized = { ...payload };

  if (
    hasOwn(payload, "policyCategory") ||
    hasOwn(payload, "policyTypeSelector") ||
    applyDefaults
  ) {
    normalized.policyCategory = safeString(
      payload.policyCategory ||
        payload.policyTypeSelector ||
        (applyDefaults ? "Insurance Policy" : ""),
    ).trim();
    normalized.policyTypeSelector = safeString(
      payload.policyTypeSelector ||
        payload.policyCategory ||
        (applyDefaults ? "Insurance Policy" : ""),
    ).trim();
  }
  if (hasSource || applyDefaults) {
    normalized.source = sourceNormalized || "Direct";
    normalized.sourceOrigin = sourceNormalized || "Direct";
  }
  if (hasOwn(payload, "usedCarFlowType") || applyDefaults) {
    normalized.usedCarFlowType =
      safeString(payload.usedCarFlowType || (applyDefaults ? "Renewal" : ""))
        .trim() || (applyDefaults ? "Renewal" : "");
  }
  if (hasOwn(payload, "policyJourneyClassification") || applyDefaults) {
    normalized.policyJourneyClassification = safeString(
      payload.policyJourneyClassification || "",
    ).trim();
  }
  if (hasPayout || applyDefaults) {
    normalized.payoutPercent = Number.isFinite(payoutPercent) ? payoutPercent : 0;
    normalized.payoutPercentage = normalized.payoutPercent;
  }

  if (hasOwn(payload, "employeeUserId")) {
    normalized.assignedTo = safeString(payload.employeeUserId).trim();
  } else if (hasOwn(payload, "assignedTo")) {
    normalized.assignedTo = safeString(payload.assignedTo).trim();
  }

  return normalized;
};

const normalizeInsuranceStatus = (value, fallback = "draft") => {
  const normalized = safeString(value || fallback).trim().toLowerCase();
  return ["draft", "submitted", "issued", "cancelled"].includes(normalized)
    ? normalized
    : fallback;
};

const normalizeInsuranceCurrentStep = (value, fallback = 1) => {
  const numeric = Number(value);
  const base = Number.isFinite(numeric) ? numeric : Number(fallback || 1);
  // Legacy step 5 (Premium Breakup) has been retired; route to step 6.
  if (base === 5) return 6;
  return Math.max(1, Math.round(base));
};

const isInsuranceCaseReadyForSubmit = (payload = {}) => {
  const errors = [];
  const isCompany = safeString(payload.buyerType).trim() === "Company";
  const mobile = safeString(payload.mobile).trim();
  if (!mobile || !/^\d{10}$/.test(mobile)) errors.push("Valid mobile is required");
  if (isCompany) {
    if (!safeString(payload.companyName).trim())
      errors.push("Company name is required");
    if (!safeString(payload.contactPersonName).trim())
      errors.push("Contact person name is required");
  } else if (!safeString(payload.customerName).trim()) {
    errors.push("Customer name is required");
  }
  if (!safeString(payload.employeeName).trim())
    errors.push("Assigned employee is required");
  if (!safeString(payload.policyCategory).trim())
    errors.push("Policy category is required");
  if (!safeString(payload.registrationNumber).trim())
    errors.push("Registration number is required");
  if (!safeString(payload.vehicleMake).trim() || !safeString(payload.vehicleModel).trim()) {
    errors.push("Vehicle make/model is required");
  }
  if (!safeString(payload.manufactureMonth).trim() || !safeString(payload.manufactureYear).trim()) {
    errors.push("Vehicle manufacture month/year is required");
  }
  const usedFlow = safeString(payload.usedCarFlowType).trim().toLowerCase();
  const needsPreviousPolicy =
    safeString(payload.vehicleType).trim().toLowerCase() === "used car" &&
    (usedFlow.includes("renew") || usedFlow.includes("rollover") || usedFlow.includes("expired"));
  if (needsPreviousPolicy) {
    if (!safeString(payload.previousInsuranceCompany).trim())
      errors.push("Previous insurance company is required");
    if (!safeString(payload.previousPolicyNumber).trim())
      errors.push("Previous policy number is required");
    if (!safeString(payload.previousPolicyType).trim())
      errors.push("Previous policy type is required");
    if (!safeString(payload.claimTakenLastYear).trim())
      errors.push("Claim taken last year is required");
  }
  if (!safeString(payload.acceptedQuoteId).trim())
    errors.push("Accepted quote is required");
  if (!safeString(payload.newInsuranceCompany).trim())
    errors.push("New insurance company is required");
  if (!safeString(payload.newPolicyType).trim())
    errors.push("New policy type is required");
  if (!safeString(payload.newPolicyNumber).trim())
    errors.push("New policy number is required");
  if (!safeString(payload.newIssueDate).trim())
    errors.push("New policy issue date is required");
  if (!safeString(payload.newPolicyStartDate).trim())
    errors.push("New policy start date is required");
  if (!safeString(payload.residenceAddress).trim() || !safeString(payload.city).trim() || !safeString(payload.pincode).trim()) {
    errors.push("Residence address, city and pincode are required");
  }
  if (!safeString(payload.nomineeName).trim() || !safeString(payload.nomineeRelationship).trim()) {
    errors.push("Nominee name and relationship are required");
  }
  const referencePhoneRaw = safeString(payload.referencePhone).replace(/\D/g, "");
  const referencePhone =
    referencePhoneRaw.length >= 10
      ? referencePhoneRaw.slice(-10)
      : referencePhoneRaw;
  if (referencePhoneRaw && referencePhone.length !== 10) {
    errors.push("Reference phone must be a valid 10-digit number when provided");
  }
  return { ok: errors.length === 0, errors };
};

const getRenewalExpiryDate = (doc = {}) =>
  safeString(
    doc.newOdExpiryDate ||
      doc.previousOdExpiryDate ||
      doc.newTpExpiryDate ||
      doc.policyExpiry ||
      "",
  ).trim();

const isPendingRenewalWithinWindow = (doc = {}, futureDays = 30, pastDays = 45) => {
  const raw = getRenewalExpiryDate(doc);
  if (!raw) return false;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return false;
  const startOfToday = new Date();
  startOfToday.setHours(0, 0, 0, 0);
  const expiry = new Date(parsed);
  expiry.setHours(0, 0, 0, 0);
  const days = Math.round((expiry.getTime() - startOfToday.getTime()) / 86400000);
  return days <= futureDays && days >= -pastDays;
};

const RENEWAL_LEAD_STATUSES = [
  "new",
  "follow up",
  "quotes shared",
  "payment pending",
  "closed",
];

const RENEWAL_OUTCOMES = [
  "NONE",
  "ALREADY_RENEWED",
  "CAR_SOLD",
  "CAR_EXPIRED",
  "POLICY_FROM_ELSEWHERE",
];

const normalizeRenewalLeadStatus = (value) => {
  const raw = safeString(value || "New").trim().toLowerCase();
  if (!raw) return "New";
  const mapped =
    raw === "followup"
      ? "follow up"
      : raw === "quotesshared"
        ? "quotes shared"
        : raw === "paymentpending"
          ? "payment pending"
          : raw;
  if (!RENEWAL_LEAD_STATUSES.includes(mapped)) return "New";
  return mapped
    .split(" ")
    .map((chunk) => chunk.charAt(0).toUpperCase() + chunk.slice(1))
    .join(" ");
};

const normalizeRenewalOutcome = (value) => {
  const raw = safeString(value).trim().toUpperCase();
  return RENEWAL_OUTCOMES.includes(raw) ? raw : "NONE";
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

const normalizeRegNumber = (value) =>
  safeString(value)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

const normalizeIdentityValue = (value) =>
  safeString(value)
    .toUpperCase()
    .replace(/\s+/g, "")
    .trim();

const isTempRegistration = (value) =>
  /^TEMP_REDG_/i.test(safeString(value).trim());

const escapeRegex = (value) =>
  safeString(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const normalizeKeyToken = (value) =>
  safeString(value)
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

const normalizeMakeToken = (value) => {
  const token = normalizeKeyToken(value);
  if (!token) return "";
  if (token === "marutisuzuki" || token === "marutisuzukiindia") return "maruti";
  if (token === "bmwindia" || token === "bayerischemotorenwerke") return "bmw";
  return token;
};

const extractCubicCapacity = (value) => {
  const raw = safeString(value).trim();
  if (!raw) return null;
  const match = raw.match(/(\d{2,5})/);
  if (!match) return null;
  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed : null;
};

const resolveCubicCapacityFromVehicleFeatures = async ({
  make = "",
  model = "",
  variant = "",
} = {}) => {
  const brand = safeString(make).trim();
  const modelName = safeString(model).trim();
  const variantName = safeString(variant).trim();
  if (!brand || !modelName || !variantName) return null;

  const quickMatch = await VehicleFeature.findOne({
    brand: { $in: [brand] },
    model: { $in: [modelName] },
    variant: { $in: [variantName] },
  })
    .collation({ locale: "en", strength: 2 })
    .lean();

  const doc =
    quickMatch ||
    (await VehicleFeature.findOne({
      brand: new RegExp(`^${brand.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}$`, "i"),
      model: new RegExp(`^${modelName.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}$`, "i"),
      variant: new RegExp(`^${variantName.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}$`, "i"),
    }).lean());
  let targetDoc = doc;
  if (!targetDoc) {
    const brandRegex = new RegExp(escapeRegex(brand), "i");
    const modelRegex = new RegExp(escapeRegex(modelName), "i");
    const variantRegex = new RegExp(escapeRegex(variantName), "i");
    const pool = await VehicleFeature.find({
      brand: brandRegex,
      model: modelRegex,
      variant: variantRegex,
    })
      .limit(30)
      .lean();
    const targetVariantToken = normalizeKeyToken(variantName);
    targetDoc =
      pool.find((row) => {
        const docVariantToken = normalizeKeyToken(row?.variant);
        return (
          docVariantToken === targetVariantToken ||
          docVariantToken.includes(targetVariantToken) ||
          targetVariantToken.includes(docVariantToken)
        );
      }) ||
      pool[0] ||
      null;
  }

  if (!targetDoc?.features || typeof targetDoc.features !== "object") return null;

  const exactKeyValue = targetDoc.features["Engine & Transmission | Displacement"];
  if (exactKeyValue !== undefined && exactKeyValue !== null) {
    return extractCubicCapacity(exactKeyValue);
  }

  for (const [fullKey, value] of Object.entries(targetDoc.features)) {
    const key = safeString(fullKey).toLowerCase();
    if (!key.includes("displacement")) continue;
    const parsed = extractCubicCapacity(value);
    if (parsed != null) return parsed;
  }
  return null;
};

const toDateOrNull = (value) => {
  const raw = safeString(value).trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const upsertVehicleRecordFromInsuranceCase = async (doc) => {
  if (!doc) return null;
  const registrationNumber = safeString(doc.registrationNumber).trim();
  const registrationNumberNormalized = normalizeRegNumber(registrationNumber);
  const engineNumber = safeString(doc.engineNumber).trim();
  const chassisNumber = safeString(doc.chassisNumber).trim();
  const make = safeString(doc.vehicleMake).trim();
  const model = safeString(doc.vehicleModel).trim();
  const variant = safeString(doc.vehicleVariant).trim();

  const cubicCapacityParsed = extractCubicCapacity(doc.cubicCapacity);
  const updateDoc = {
    customerId: doc.customerId || undefined,
    registrationNumber: registrationNumber || registrationNumberNormalized,
    registrationNumberNormalized: registrationNumberNormalized || undefined,
    registrationNumberLast4: registrationNumberNormalized
      ? registrationNumberNormalized.slice(-4)
      : undefined,
    make,
    model,
    variant,
    cubicCapacityCc: Number.isFinite(cubicCapacityParsed)
      ? cubicCapacityParsed
      : undefined,
    engineNumber,
    chassisNumber,
    manufactureMonth: safeString(doc.manufactureMonth).trim(),
    yearOfManufacture: safeString(doc.manufactureYear).trim(),
    registrationDate: toDateOrNull(doc.dateOfReg),
    regAuthority: safeString(doc.regAuthority).trim(),
    registrationCity: safeString(doc.city || doc.registrationCity).trim(),
    hypothecation: safeString(doc.hypothecation).trim(),
    fuelType: safeString(doc.fuelType).trim(),
    typesOfVehicle: safeString(doc.typesOfVehicle).trim(),
    batteryNumber: safeString(doc.batteryNumber).trim(),
    chargerNumber: safeString(doc.chargerNumber).trim(),
    customerName: safeString(doc.customerName || doc.companyName).trim(),
    primaryMobile: safeString(doc.mobile).trim(),
    lastSyncedAt: new Date(),
  };

  Object.keys(updateDoc).forEach((key) => {
    if (updateDoc[key] === undefined) delete updateDoc[key];
  });

  const hasCoreIdentity =
    registrationNumberNormalized || chassisNumber || engineNumber || make || model;
  if (!hasCoreIdentity) return null;

  const query = registrationNumberNormalized
    ? { registrationNumberNormalized }
    : chassisNumber
      ? { chassisNumber }
      : engineNumber
        ? { engineNumber }
        : { make, model, variant };

  return await VehicleRecord.findOneAndUpdate(
    query,
    { $set: updateDoc },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  );
};

// @desc    Generate next temporary registration number for new car insurance
// @route   POST /api/insurance/temp-registration/next
// @access  Public
export const getNextTempRegistration = asyncHandler(async (_req, res) => {
  const counter = await Counter.findOneAndUpdate(
    { key: INSURANCE_TEMP_REG_COUNTER_KEY },
    { $inc: { value: 1 } },
    { upsert: true, new: true },
  );
  const seq = Number(counter?.value || 0);
  const registrationNumber = `TEMP_REDG_${String(seq).padStart(4, "0")}`;
  res.json({
    success: true,
    data: {
      registrationNumber,
      sequence: seq,
    },
  });
});

// @desc    Resolve cubic capacity from vehicle_features and store to vehicle_master_records
// @route   POST /api/insurance/vehicle-cubic-capacity/resolve
// @access  Public
export const resolveVehicleCubicCapacity = asyncHandler(async (req, res) => {
  const make = safeString(req.body?.make).trim();
  const model = safeString(req.body?.model).trim();
  const variant = safeString(req.body?.variant).trim();
  const registrationNumber = safeString(req.body?.registrationNumber).trim();

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error("make, model and variant are required");
  }

  const cubicCapacity = await resolveCubicCapacityFromVehicleFeatures({
    make,
    model,
    variant,
  });

  const registrationNumberNormalized = normalizeRegNumber(registrationNumber);
  let vehicleRecord = null;

  if (registrationNumberNormalized) {
    const updateDoc = {
      registrationNumber: registrationNumber || registrationNumberNormalized,
      registrationNumberNormalized,
      registrationNumberLast4: registrationNumberNormalized.slice(-4),
      make,
      model,
      variant,
      lastSyncedAt: new Date(),
    };
    if (Number.isFinite(cubicCapacity) && cubicCapacity > 0) {
      updateDoc.cubicCapacityCc = cubicCapacity;
    }
    vehicleRecord = await VehicleRecord.findOneAndUpdate(
      { registrationNumberNormalized },
      { $set: updateDoc },
      { upsert: true, new: true, setDefaultsOnInsert: true },
    );
  }

  res.json({
    success: true,
    data: {
      make,
      model,
      variant,
      cubicCapacity: Number.isFinite(cubicCapacity) ? cubicCapacity : null,
      registrationNumber: registrationNumber || null,
      registrationNumberNormalized: registrationNumberNormalized || null,
      vehicleRecordId: vehicleRecord?._id || null,
    },
  });
});

// @desc    Find potential historical vehicle match from vehicle_master_records
// @route   POST /api/insurance/vehicle-match/potential
// @access  Public
export const findPotentialVehicleMatch = asyncHandler(async (req, res) => {
  const make = safeString(req.body?.make).trim();
  const model = safeString(req.body?.model).trim();
  const variant = safeString(req.body?.variant).trim();
  const manufactureMonth = safeString(req.body?.manufactureMonth).trim();
  const manufactureYear = safeString(req.body?.manufactureYear).trim();
  const engineNumber = normalizeIdentityValue(req.body?.engineNumber);
  const chassisNumber = normalizeIdentityValue(req.body?.chassisNumber);
  const currentRegistrationNumber = safeString(
    req.body?.currentRegistrationNumber,
  ).trim();
  const currentRegNormalized = normalizeRegNumber(currentRegistrationNumber);

  if (!make || !model || !variant || (!engineNumber && !chassisNumber)) {
    return res.json({ success: true, data: [] });
  }

  const identityOr = [];
  if (engineNumber) {
    identityOr.push({
      engineNumber: new RegExp(`^${escapeRegex(engineNumber)}$`, "i"),
    });
  }
  if (chassisNumber) {
    identityOr.push({
      chassisNumber: new RegExp(`^${escapeRegex(chassisNumber)}$`, "i"),
    });
  }

  const rows = await VehicleRecord.find({
    $or: identityOr,
  })
    .sort({ updatedAt: -1, createdAt: -1 })
    .limit(30)
    .lean();

  const makeToken = normalizeMakeToken(make);
  const modelToken = normalizeKeyToken(model);
  const variantToken = normalizeKeyToken(variant);

  const scored = rows
    .map((row) => {
      const reg = safeString(row?.registrationNumber).trim();
      const regNorm = normalizeRegNumber(row?.registrationNumberNormalized || reg);
      if (!regNorm) return null;
      if (currentRegNormalized && regNorm === currentRegNormalized) return null;

      const rowEngine = normalizeIdentityValue(row?.engineNumber);
      const rowChassis = normalizeIdentityValue(row?.chassisNumber);
      const rowMakeToken = normalizeMakeToken(row?.make);
      const rowModelToken = normalizeKeyToken(row?.model);
      const rowVariantToken = normalizeKeyToken(row?.variant);
      let score = 0;

      const engineMatch = Boolean(engineNumber && rowEngine && rowEngine === engineNumber);
      const chassisMatch = Boolean(
        chassisNumber &&
          rowChassis &&
          rowChassis === chassisNumber,
      );

      if (engineMatch && chassisMatch) score += 320;
      else if (engineMatch || chassisMatch) score += 220;

      const strictTokenMatch =
        rowMakeToken &&
        rowModelToken &&
        rowVariantToken &&
        rowMakeToken === makeToken &&
        rowModelToken === modelToken &&
        rowVariantToken === variantToken;
      const fuzzyTokenMatch =
        rowMakeToken &&
        rowModelToken &&
        rowVariantToken &&
        (rowMakeToken.includes(makeToken) || makeToken.includes(rowMakeToken)) &&
        (rowModelToken.includes(modelToken) || modelToken.includes(rowModelToken)) &&
        (rowVariantToken.includes(variantToken) || variantToken.includes(rowVariantToken));
      if (strictTokenMatch) {
        score += 120;
      } else if (fuzzyTokenMatch) {
        score += 90;
      }
      if (
        manufactureMonth &&
        safeString(row?.manufactureMonth).trim().toLowerCase() ===
          manufactureMonth.toLowerCase()
      ) {
        score += 40;
      }
      if (
        manufactureYear &&
        safeString(row?.yearOfManufacture).trim().toLowerCase() ===
          manufactureYear.toLowerCase()
      ) {
        score += 40;
      }
      if (!isTempRegistration(reg)) score += 20;

      return {
        _id: row?._id,
        registrationNumber: reg,
        registrationNumberNormalized: regNorm,
        make: safeString(row?.make).trim(),
        model: safeString(row?.model).trim(),
        variant: safeString(row?.variant).trim(),
        manufactureMonth: safeString(row?.manufactureMonth).trim(),
        manufactureYear: safeString(row?.yearOfManufacture).trim(),
        engineNumber: safeString(row?.engineNumber).trim(),
        chassisNumber: safeString(row?.chassisNumber).trim(),
        customerName: safeString(row?.customerName).trim(),
        primaryMobile: safeString(row?.primaryMobile).trim(),
        cubicCapacityCc: row?.cubicCapacityCc ?? null,
        score,
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);

  const best = scored[0] || null;
  res.json({
    success: true,
    data: scored.slice(0, 5),
    bestMatch: best,
  });
});

// @desc    Merge temp registration history into final registration
// @route   POST /api/insurance/vehicle-match/merge
// @access  Public
export const mergeVehicleMatch = asyncHandler(async (req, res) => {
  const insuranceCaseId = safeString(req.body?.insuranceCaseId).trim();
  const matchedVehicleRecordId = safeString(req.body?.matchedVehicleRecordId).trim();
  const overwriteHistoricalRecords = Boolean(req.body?.overwriteHistoricalRecords);
  const currentRegistrationNumber = safeString(
    req.body?.currentRegistrationNumber,
  ).trim();

  if (!matchedVehicleRecordId || !mongoose.Types.ObjectId.isValid(matchedVehicleRecordId)) {
    res.status(400);
    throw new Error("matchedVehicleRecordId is required");
  }

  const matchedVehicle = await VehicleRecord.findById(matchedVehicleRecordId);
  if (!matchedVehicle) {
    res.status(404);
    throw new Error("Matched vehicle record not found");
  }

  let insuranceCaseDoc = null;
  if (insuranceCaseId) {
    insuranceCaseDoc =
      (mongoose.Types.ObjectId.isValid(insuranceCaseId)
        ? await InsuranceCase.findById(insuranceCaseId)
        : null) ||
      (await InsuranceCase.findOne({ caseId: insuranceCaseId }));
  }

  const caseReg = safeString(insuranceCaseDoc?.registrationNumber).trim();
  const matchedReg = safeString(matchedVehicle.registrationNumber).trim();
  const candidateRegs = [currentRegistrationNumber, caseReg, matchedReg]
    .map((v) => safeString(v).trim())
    .filter(Boolean);

  const canonicalRegistration =
    candidateRegs.find((reg) => !isTempRegistration(reg)) || candidateRegs[0] || "";
  const canonicalRegNormalized = normalizeRegNumber(canonicalRegistration);
  if (!canonicalRegNormalized) {
    res.status(400);
    throw new Error("Unable to determine canonical registration number");
  }

  const tempRegs = [
    ...new Set(
      candidateRegs.filter((reg) => {
        const normalized = normalizeRegNumber(reg);
        return normalized && normalized !== canonicalRegNormalized && isTempRegistration(reg);
      }),
    ),
  ];
  const tempNorms = tempRegs.map(normalizeRegNumber).filter(Boolean);

  const canonicalRecord =
    (await VehicleRecord.findOne({
      registrationNumberNormalized: canonicalRegNormalized,
    })) || null;

  const tempRecords = tempNorms.length
    ? await VehicleRecord.find({
        registrationNumberNormalized: { $in: tempNorms },
      })
    : [];

  const baseData = {
    make:
      safeString(req.body?.make).trim() ||
      safeString(matchedVehicle.make).trim() ||
      safeString(canonicalRecord?.make).trim(),
    model:
      safeString(req.body?.model).trim() ||
      safeString(matchedVehicle.model).trim() ||
      safeString(canonicalRecord?.model).trim(),
    variant:
      safeString(req.body?.variant).trim() ||
      safeString(matchedVehicle.variant).trim() ||
      safeString(canonicalRecord?.variant).trim(),
    engineNumber:
      safeString(req.body?.engineNumber).trim() ||
      safeString(matchedVehicle.engineNumber).trim() ||
      safeString(canonicalRecord?.engineNumber).trim(),
    chassisNumber:
      safeString(req.body?.chassisNumber).trim() ||
      safeString(matchedVehicle.chassisNumber).trim() ||
      safeString(canonicalRecord?.chassisNumber).trim(),
    manufactureMonth:
      safeString(req.body?.manufactureMonth).trim() ||
      safeString(matchedVehicle.manufactureMonth).trim() ||
      safeString(canonicalRecord?.manufactureMonth).trim(),
    yearOfManufacture:
      safeString(req.body?.manufactureYear).trim() ||
      safeString(matchedVehicle.yearOfManufacture).trim() ||
      safeString(canonicalRecord?.yearOfManufacture).trim(),
    hypothecation:
      safeString(req.body?.hypothecation).trim() ||
      safeString(matchedVehicle.hypothecation).trim() ||
      safeString(canonicalRecord?.hypothecation).trim(),
    registrationDate:
      toDateOrNull(req.body?.dateOfReg) ||
      toDateOrNull(matchedVehicle.registrationDate) ||
      toDateOrNull(canonicalRecord?.registrationDate),
    regAuthority:
      safeString(req.body?.regAuthority).trim() ||
      safeString(matchedVehicle.regAuthority).trim() ||
      safeString(canonicalRecord?.regAuthority).trim(),
    fuelType:
      safeString(req.body?.fuelType).trim() ||
      safeString(matchedVehicle.fuelType).trim() ||
      safeString(canonicalRecord?.fuelType).trim(),
    typesOfVehicle:
      safeString(req.body?.typesOfVehicle).trim() ||
      safeString(matchedVehicle.typesOfVehicle).trim() ||
      safeString(canonicalRecord?.typesOfVehicle).trim(),
    batteryNumber:
      safeString(req.body?.batteryNumber).trim() ||
      safeString(matchedVehicle.batteryNumber).trim() ||
      safeString(canonicalRecord?.batteryNumber).trim(),
    chargerNumber:
      safeString(req.body?.chargerNumber).trim() ||
      safeString(matchedVehicle.chargerNumber).trim() ||
      safeString(canonicalRecord?.chargerNumber).trim(),
    customerName:
      safeString(req.body?.customerName).trim() ||
      safeString(insuranceCaseDoc?.customerName).trim() ||
      safeString(matchedVehicle.customerName).trim() ||
      safeString(canonicalRecord?.customerName).trim(),
    primaryMobile:
      safeString(req.body?.primaryMobile).trim() ||
      safeString(insuranceCaseDoc?.mobile).trim() ||
      safeString(matchedVehicle.primaryMobile).trim() ||
      safeString(canonicalRecord?.primaryMobile).trim(),
    cubicCapacityCc:
      Number(req.body?.cubicCapacityCc) ||
      Number(matchedVehicle.cubicCapacityCc) ||
      Number(canonicalRecord?.cubicCapacityCc) ||
      undefined,
  };

  const mergedVehicleRecord = await VehicleRecord.findOneAndUpdate(
    { registrationNumberNormalized: canonicalRegNormalized },
    {
      $set: {
        registrationNumber: canonicalRegistration,
        registrationNumberNormalized: canonicalRegNormalized,
        registrationNumberLast4: canonicalRegNormalized.slice(-4),
        ...baseData,
        lastSyncedAt: new Date(),
      },
    },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  );

  const rowsToRemove = tempRecords
    .map((row) => String(row?._id || ""))
    .filter((id) => id && id !== String(mergedVehicleRecord?._id || ""));
  if (rowsToRemove.length) {
    await VehicleRecord.deleteMany({ _id: { $in: rowsToRemove } });
  }

  const mergePatch = {
    registrationNumber: canonicalRegistration,
    registrationAllotted: "Yes",
    vehicleMake: baseData.make,
    vehicleModel: baseData.model,
    vehicleVariant: baseData.variant,
    engineNumber: baseData.engineNumber,
    chassisNumber: baseData.chassisNumber,
    manufactureMonth: baseData.manufactureMonth,
    manufactureYear: baseData.yearOfManufacture,
    regAuthority: baseData.regAuthority,
    fuelType: baseData.fuelType,
    typesOfVehicle: baseData.typesOfVehicle,
    batteryNumber: baseData.batteryNumber,
    chargerNumber: baseData.chargerNumber,
    hypothecation: baseData.hypothecation || "Not applicable",
    customerName: baseData.customerName,
    mobile: baseData.primaryMobile,
  };
  if (baseData.registrationDate) {
    mergePatch.dateOfReg = baseData.registrationDate.toISOString();
  }
  if (Number.isFinite(baseData.cubicCapacityCc) && baseData.cubicCapacityCc > 0) {
    mergePatch.cubicCapacity = String(Math.round(baseData.cubicCapacityCc));
  }
  Object.keys(mergePatch).forEach((key) => {
    const value = mergePatch[key];
    if (value === undefined || value === null || safeString(value).trim() === "") {
      delete mergePatch[key];
    }
  });

  for (const tempReg of tempRegs) {
    await InsuranceCase.updateMany(
      { registrationNumber: new RegExp(`^${escapeRegex(tempReg)}$`, "i") },
      { $set: mergePatch },
    );
  }

  if (overwriteHistoricalRecords) {
    const historyOr = [];
    const regCandidates = [...new Set([canonicalRegistration, ...tempRegs])];
    regCandidates.forEach((reg) => {
      if (!safeString(reg).trim()) return;
      historyOr.push({
        registrationNumber: new RegExp(`^${escapeRegex(reg)}$`, "i"),
      });
    });
    if (baseData.engineNumber) {
      historyOr.push({
        engineNumber: new RegExp(`^${escapeRegex(baseData.engineNumber)}$`, "i"),
      });
    }
    if (baseData.chassisNumber) {
      historyOr.push({
        chassisNumber: new RegExp(`^${escapeRegex(baseData.chassisNumber)}$`, "i"),
      });
    }
    if (historyOr.length) {
      await InsuranceCase.updateMany({ $or: historyOr }, { $set: mergePatch });
    }
  }

  let updatedCase = null;
  if (insuranceCaseDoc) {
    insuranceCaseDoc.registrationNumber = canonicalRegistration;
    insuranceCaseDoc.registrationAllotted = "Yes";
    const applyMergeField = (docKey, incoming) => {
      const value = safeString(incoming).trim();
      if (!value) return;
      if (overwriteHistoricalRecords || !safeString(insuranceCaseDoc?.[docKey]).trim()) {
        insuranceCaseDoc[docKey] = incoming;
      }
    };
    applyMergeField("vehicleMake", baseData.make);
    applyMergeField("vehicleModel", baseData.model);
    applyMergeField("vehicleVariant", baseData.variant);
    applyMergeField("engineNumber", baseData.engineNumber);
    applyMergeField("chassisNumber", baseData.chassisNumber);
    applyMergeField("regAuthority", baseData.regAuthority);
    applyMergeField("fuelType", baseData.fuelType);
    applyMergeField("typesOfVehicle", baseData.typesOfVehicle);
    applyMergeField("batteryNumber", baseData.batteryNumber);
    applyMergeField("chargerNumber", baseData.chargerNumber);
    applyMergeField("hypothecation", baseData.hypothecation || "Not applicable");
    if (baseData.registrationDate && (overwriteHistoricalRecords || !insuranceCaseDoc.dateOfReg)) {
      insuranceCaseDoc.dateOfReg = baseData.registrationDate.toISOString();
    }
    if (
      Number.isFinite(baseData.cubicCapacityCc) &&
      baseData.cubicCapacityCc > 0 &&
      (overwriteHistoricalRecords || !safeString(insuranceCaseDoc.cubicCapacity).trim())
    ) {
      insuranceCaseDoc.cubicCapacity = String(Math.round(baseData.cubicCapacityCc));
    }
    updatedCase = await insuranceCaseDoc.save();
  }

  res.json({
    success: true,
    data: {
      canonicalRegistration,
      mergedVehicleRecord,
      tempRegistrationsMerged: tempRegs,
      insuranceCaseId: updatedCase?._id || insuranceCaseDoc?._id || null,
    },
  });
});

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

// @desc    Get pending renewal insurance cases
// @route   GET /api/insurance/renewals/cases
// @access  Public
export const getInsuranceRenewalCases = asyncHandler(async (req, res) => {
  const includeAssignedOnly = String(req.query.assignedOnly || "").trim() === "1";
  const assignedToId = safeString(req.query.assignedToId).trim();
  const futureDays = Math.max(1, Math.min(365, Number(req.query.futureDays || 365)));
  const pastDays = Math.max(1, Math.min(365, Number(req.query.pastDays || 365)));
  const odAmountRange = safeString(req.query.odAmountRange).trim().toLowerCase();
  const role = safeString(req.user?.role).trim().toLowerCase();
  const meId = safeString(req.user?._id).trim();
  const isAdminLike = [
    "admin",
    "superadmin",
    "team_lead",
    "insurance_team_lead",
  ].includes(role);
  const view = safeString(req.query.view || "renewal").trim().toLowerCase();

  const baseRows = await InsuranceCase.find({})
    .sort({ updatedAt: -1, createdAt: -1 })
    .lean();

  let rows = baseRows.filter((doc) =>
    isPendingRenewalWithinWindow(doc, futureDays, pastDays),
  );

  if (includeAssignedOnly && assignedToId) {
    rows = rows.filter(
      (doc) => safeString(doc?.renewalAssignedToId).trim() === assignedToId,
    );
  }
  // Do not hard-scope non-admin users to assigned-only by default.
  // Assignment scoping should happen only when explicitly requested via query params.
  const mappedRows = rows.map((doc) => ({
    ...doc,
      renewalOutcome: normalizeRenewalOutcome(doc?.renewalOutcome),
  }));

  const getForRenewalTab = (list) =>
    list.filter((doc) => {
      if (String(doc?.policyCategory || "").trim().toLowerCase() === "extended warranty") return false;
      const outcome = normalizeRenewalOutcome(doc?.renewalOutcome);
      if (outcome === "CAR_SOLD" || outcome === "CAR_EXPIRED") return false;
      if (outcome === "ALREADY_RENEWED") return false;
      if (doc?.renewedToCaseId) return false;
      return true;
    });

  const getForRenewedTab = (list) =>
    list.filter(
      (doc) =>
        normalizeRenewalOutcome(doc?.renewalOutcome) === "ALREADY_RENEWED" ||
        Boolean(doc?.renewedToCaseId),
    );

  const getForExternalTab = (list) =>
    list.filter(
      (doc) => normalizeRenewalOutcome(doc?.renewalOutcome) === "POLICY_FROM_ELSEWHERE",
    );

  if (view === "renewed") {
    rows = getForRenewedTab(mappedRows);
  } else if (view === "external") {
    rows = getForExternalTab(mappedRows);
  } else {
    rows = getForRenewalTab(mappedRows);
  }
  if (odAmountRange) {
    rows = rows.filter((doc) => {
      const value = Number(doc?.previousOwnDamageAmount || doc?.odAmount || 0);
      if (!Number.isFinite(value) || value < 0) return false;
      if (odAmountRange === "lt10k") return value < 10000;
      if (odAmountRange === "10k-20k") return value >= 10000 && value <= 20000;
      if (odAmountRange === "gt20k") return value > 20000;
      return true;
    });
  }

  res.json({
    success: true,
    count: rows.length,
    data: rows,
  });
});

// @desc    Bulk assign renewal cases
// @route   POST /api/insurance/renewals/assign
// @access  Public
export const assignInsuranceRenewalCases = asyncHandler(async (req, res) => {
  const caseIds = Array.isArray(req.body?.caseIds) ? req.body.caseIds : [];
  const assigneeId = safeString(req.body?.assigneeId).trim();
  const assigneeName = safeString(req.body?.assigneeName).trim();
  const assignedBy = safeString(req.body?.assignedBy).trim();

  const validCaseObjectIds = caseIds
    .map((id) => safeString(id).trim())
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  if (!validCaseObjectIds.length) {
    res.status(400);
    throw new Error("caseIds are required");
  }
  if (!assigneeId) {
    res.status(400);
    throw new Error("assigneeId is required");
  }

  const update = {
    renewalAssignedToId: assigneeId,
    renewalAssignedToName: assigneeName,
    renewalAssignedAt: new Date(),
    renewalAssignedBy: assignedBy || "System",
  };
  const timelineEntry = {
    at: new Date().toISOString(),
    by: assignedBy || safeString(req.user?.name).trim() || "System",
    event: "ASSIGNED",
    status: "New",
    comment: `Assigned to ${assigneeName || assigneeId}`,
  };

  const result = await InsuranceCase.updateMany(
    { _id: { $in: validCaseObjectIds } },
    { $set: update, $push: { renewalTimeline: timelineEntry } },
  );

  res.json({
    success: true,
    matchedCount: Number(result?.matchedCount || 0),
    modifiedCount: Number(result?.modifiedCount || 0),
  });
});

// @desc    Update renewal lead details for one case
// @route   PATCH /api/insurance/renewals/:id/lead
// @access  Public
export const updateInsuranceRenewalLead = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const doc =
    (mongoose.Types.ObjectId.isValid(raw)
      ? await InsuranceCase.findById(raw)
      : null) || (await InsuranceCase.findOne({ caseId: raw }));

  if (!doc) {
    res.status(404);
    throw new Error("Insurance case not found");
  }
  const role = safeString(req.user?.role).trim().toLowerCase();
  const meId = safeString(req.user?._id).trim();
  const isAdminLike = [
    "admin",
    "superadmin",
    "team_lead",
    "insurance_team_lead",
  ].includes(role);
  if (!isAdminLike && safeString(doc.renewalAssignedToId).trim() !== meId) {
    res.status(403);
    throw new Error("You can update only your assigned renewal cases");
  }

  let renewalLeadStatus = normalizeRenewalLeadStatus(
    req.body?.renewalLeadStatus || "New",
  );
  const renewalFollowUpDate = safeString(req.body?.renewalFollowUpDate).trim();
  const renewalComment = safeString(req.body?.renewalComment).trim();
  const renewalClosedReason = safeString(req.body?.renewalClosedReason).trim();
  const action = safeString(req.body?.action).trim().toUpperCase();
  const requestedOutcome = normalizeRenewalOutcome(req.body?.renewalOutcome);
  const updatedBy =
    safeString(req.body?.updatedBy).trim() ||
    safeString(req.user?.name).trim() ||
    "User";
  if (action === "SHARE_QUOTES") renewalLeadStatus = "Quotes Shared";
  if (action === "MARK_PAYMENT_PENDING") renewalLeadStatus = "Payment Pending";
  if (action === "ALREADY_RENEWED") renewalLeadStatus = "Closed";
  if (action === "CAR_SOLD") renewalLeadStatus = "Closed";
  if (action === "CAR_EXPIRED") renewalLeadStatus = "Closed";
  if (action === "POLICY_FROM_ELSEWHERE") renewalLeadStatus = "Closed";

  let renewalOutcome = requestedOutcome;
  if (action === "ALREADY_RENEWED") renewalOutcome = "ALREADY_RENEWED";
  if (action === "CAR_SOLD") renewalOutcome = "CAR_SOLD";
  if (action === "CAR_EXPIRED") renewalOutcome = "CAR_EXPIRED";
  if (action === "POLICY_FROM_ELSEWHERE") renewalOutcome = "POLICY_FROM_ELSEWHERE";
  const hasAutoClosedReason = [
    "ALREADY_RENEWED",
    "CAR_SOLD",
    "CAR_EXPIRED",
    "POLICY_FROM_ELSEWHERE",
  ].includes(action);
  if (
    renewalLeadStatus.toLowerCase() === "closed" &&
    !renewalClosedReason &&
    !hasAutoClosedReason
  ) {
    res.status(400);
    throw new Error("Closed reason is required when lead status is Closed");
  }

  doc.renewalLeadStatus = renewalLeadStatus || "New";
  doc.renewalOutcome = renewalOutcome || "NONE";
  doc.renewalFollowUpDate = renewalFollowUpDate || "";
  doc.renewalComment = renewalComment || "";
  if (renewalLeadStatus.toLowerCase() === "closed") {
    doc.renewalClosedReason = renewalClosedReason || "";
  }
  if (
    action === "ALREADY_RENEWED" &&
    !doc.renewalClosedReason
  ) {
    doc.renewalClosedReason = "Already Renewed";
  }
  if (action === "CAR_SOLD" && !doc.renewalClosedReason) {
    doc.renewalClosedReason = "Car Sold";
  }
  if (action === "CAR_EXPIRED" && !doc.renewalClosedReason) {
    doc.renewalClosedReason = "Car Expired";
  }
  if (action === "POLICY_FROM_ELSEWHERE" && !doc.renewalClosedReason) {
    doc.renewalClosedReason = "Policy from Elsewhere";
  }
  doc.renewalLastContactedAt = new Date();
  doc.renewalNextFollowUpDate =
    renewalFollowUpDate && !Number.isNaN(new Date(renewalFollowUpDate).getTime())
      ? new Date(renewalFollowUpDate)
      : undefined;

  const existingTimeline = Array.isArray(doc.renewalTimeline)
    ? doc.renewalTimeline
    : [];
  doc.renewalTimeline = [
    ...existingTimeline,
    {
      at: new Date().toISOString(),
      by: updatedBy,
      event: action || "STATUS_UPDATE",
      status: doc.renewalLeadStatus,
      outcome: doc.renewalOutcome || "NONE",
      followUpDate: doc.renewalFollowUpDate,
      comment: doc.renewalComment,
      closedReason: doc.renewalClosedReason || "",
    },
  ];

  await doc.save();
  res.json({ success: true, data: doc });
});

// @desc    Renewal dashboard summary
// @route   GET /api/insurance/renewals/summary
// @access  Private
export const getInsuranceRenewalSummary = asyncHandler(async (req, res) => {
  const role = safeString(req.user?.role).trim().toLowerCase();
  const meId = safeString(req.user?._id).trim();
  const isAdminLike = [
    "admin",
    "superadmin",
    "team_lead",
    "insurance_team_lead",
  ].includes(role);
  const rows = await InsuranceCase.find({})
    .sort({ updatedAt: -1 })
    .lean();
  let pendingRows = rows.filter((doc) => isPendingRenewalWithinWindow(doc, 365, 365));
  pendingRows = pendingRows.map((doc) => ({
    ...doc,
    renewalOutcome: normalizeRenewalOutcome(doc?.renewalOutcome),
  }));
  const scopedRows = pendingRows;
  const renewalRows = scopedRows.filter((doc) => {
    if (String(doc?.policyCategory || "").trim().toLowerCase() === "extended warranty") return false;
    const outcome = normalizeRenewalOutcome(doc?.renewalOutcome);
    if (outcome === "CAR_SOLD" || outcome === "CAR_EXPIRED") return false;
    if (outcome === "ALREADY_RENEWED") return false;
    if (doc?.renewedToCaseId) return false;
    return true;
  });
  const renewedRows = scopedRows.filter(
    (doc) =>
      normalizeRenewalOutcome(doc?.renewalOutcome) === "ALREADY_RENEWED" ||
      Boolean(doc?.renewedToCaseId),
  );
  const externalRows = scopedRows.filter(
    (doc) => normalizeRenewalOutcome(doc?.renewalOutcome) === "POLICY_FROM_ELSEWHERE",
  );
  const activeCases = scopedRows.filter(
    (row) => normalizeRenewalLeadStatus(row?.renewalLeadStatus) !== "Closed",
  ).length;
  const policiesPending = scopedRows.filter((row) => !safeString(row?.newPolicyNumber).trim()).length;
  const paymentPending = scopedRows.filter(
    (row) => normalizeRenewalLeadStatus(row?.renewalLeadStatus) === "Payment Pending",
  ).length;
  res.json({
    success: true,
    data: {
      activeCases,
      policiesPending,
      paymentPending,
      pendingRenewals: renewalRows.length,
      renewed: renewedRows.length,
      external: externalRows.length,
      nonAssigned: renewalRows.filter((row) => !safeString(row?.renewalAssignedToId).trim()).length,
      assignedToMe: meId
        ? renewalRows.filter((row) => safeString(row?.renewalAssignedToId).trim() === meId).length
        : 0,
    },
  });
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
  const payload = stripImmutableInsuranceFields(
    normalizeStep1Payload(req.body || {}, { applyDefaults: true }),
  );
  if (hasOwn(payload, "paymentHistory") || hasOwn(payload, "payment_history")) {
    const normalizedPaymentHistory = normalizePaymentHistoryPayload(
      hasOwn(payload, "paymentHistory")
        ? payload.paymentHistory
        : payload.payment_history,
    );
    payload.paymentHistory = normalizedPaymentHistory;
    delete payload.payment_history;
  }

  const caseId = await getNextInsuranceCaseId();
  const customerIdRaw = safeString(payload.customerId).trim();
  const customerId = await resolveCustomerObjectId(customerIdRaw);
  if (customerIdRaw && !customerId) {
    res.status(400);
    throw new Error("Invalid customerId");
  }

  let customerSnapshot = payload.customerSnapshot || {};
  if (customerId) {
    const syncedCustomer = await syncCustomerFromInsurancePayload(
      customerId,
      payload,
    );
    if (syncedCustomer) customerSnapshot = buildCustomerSnapshot(syncedCustomer);
  }

  const normalizedStatus = normalizeInsuranceStatus(payload.status, "draft");
  if (normalizedStatus === "submitted") {
    const submitValidation = isInsuranceCaseReadyForSubmit(payload);
    if (!submitValidation.ok) {
      res.status(400);
      throw new Error(submitValidation.errors.join(" | "));
    }
  }

  await syncChannelPartnerOnInsurancePayload(payload);

  const doc = await InsuranceCase.create({
    ...payload,
    caseId,
    customerId: customerId || undefined,
    customerSnapshot,
    status: normalizedStatus,
    currentStep: normalizeInsuranceCurrentStep(payload.currentStep, 1),
  });

  if (doc?.renewedFromCaseId) {
    await InsuranceCase.findByIdAndUpdate(doc.renewedFromCaseId, {
      $set: { renewedToCaseId: doc._id },
    });
  }

  await upsertVehicleRecordFromInsuranceCase(doc);

  res.status(201).json({ success: true, data: doc });
});

// @desc    Update insurance case (full replace/merge style)
// @route   PUT /api/insurance/:id
// @access  Public
export const updateInsuranceCase = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const payload = stripImmutableInsuranceFields(
    normalizeStep1Payload(req.body || {}, { applyDefaults: false }),
  );
  if (hasOwn(payload, "paymentHistory") || hasOwn(payload, "payment_history")) {
    const normalizedPaymentHistory = normalizePaymentHistoryPayload(
      hasOwn(payload, "paymentHistory")
        ? payload.paymentHistory
        : payload.payment_history,
    );
    payload.paymentHistory = normalizedPaymentHistory;
    delete payload.payment_history;
  }

  const existingDoc =
    (mongoose.Types.ObjectId.isValid(raw)
      ? await InsuranceCase.findById(raw)
      : null) || (await InsuranceCase.findOne({ caseId: raw }));

  if (!existingDoc) {
    res.status(404);
    throw new Error("Insurance case not found");
  }

  const customerIdRaw = safeString(payload.customerId).trim();
  const parsedCustomerId = await resolveCustomerObjectId(customerIdRaw);
  if (customerIdRaw && !parsedCustomerId) {
    res.status(400);
    throw new Error("Invalid customerId");
  }
  const customerId = parsedCustomerId || existingDoc.customerId || null;
  let customerSnapshot =
    existingDoc.customerSnapshot || {};
  if (payload.customerSnapshot && typeof payload.customerSnapshot === "object") {
    customerSnapshot = {
      ...customerSnapshot,
      ...payload.customerSnapshot,
    };
  }
  if (customerId) {
    const syncedCustomer = await syncCustomerFromInsurancePayload(customerId, {
      ...existingDoc.toObject(),
      ...payload,
    });
    if (syncedCustomer) {
      customerSnapshot = {
        ...customerSnapshot,
        ...buildCustomerSnapshot(syncedCustomer),
      };
    }
  }

  const normalizedStatus = normalizeInsuranceStatus(
    payload.status,
    normalizeInsuranceStatus(existingDoc.status, "draft"),
  );
  const updatePatch = {
    ...payload,
    customerId: customerId || undefined,
    customerSnapshot,
    assignedTo:
      safeString(payload.assignedTo || payload.employeeUserId).trim() ||
      safeString(existingDoc.assignedTo).trim() ||
      "",
    currentStep: normalizeInsuranceCurrentStep(
      payload.currentStep,
      existingDoc.currentStep || 1,
    ),
    status: normalizedStatus,
  };
  if (normalizedStatus === "submitted") {
    const submitValidation = isInsuranceCaseReadyForSubmit({
      ...existingDoc.toObject(),
      ...updatePatch,
    });
    if (!submitValidation.ok) {
      res.status(400);
      throw new Error(submitValidation.errors.join(" | "));
    }
  }

  const mergedForChannel = {
    ...existingDoc.toObject(),
    ...updatePatch,
  };
  await syncChannelPartnerOnInsurancePayload(mergedForChannel);
  if (mergedForChannel.channelDealerNo) {
    updatePatch.channelDealerNo = mergedForChannel.channelDealerNo;
  }

  const saved = await InsuranceCase.findByIdAndUpdate(
    existingDoc._id,
    { $set: updatePatch },
    {
      new: true,
      runValidators: true,
      context: "query",
    },
  );

  if (!saved) {
    res.status(404);
    throw new Error("Insurance case not found");
  }

  await upsertVehicleRecordFromInsuranceCase(saved);
  if (saved?.renewedFromCaseId) {
    await InsuranceCase.findByIdAndUpdate(saved.renewedFromCaseId, {
      $set: { renewedToCaseId: saved._id },
    });
  }
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
    res.status(404);
    throw new Error("Insurance case not found");
  }

  await InsuranceCase.deleteOne({ _id: doc._id });

  res.json({
    success: true,
    message: "Insurance case deleted successfully",
    data: { id: doc._id, caseId: doc.caseId },
  });
});

// @desc    Append a payment ledger entry (idempotent by idempotency key)
// @route   POST /api/insurance/:id/payments
// @access  Public
export const appendInsurancePayment = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const doc =
    (mongoose.Types.ObjectId.isValid(raw)
      ? await InsuranceCase.findById(raw)
      : null) || (await InsuranceCase.findOne({ caseId: raw }));

  if (!doc) {
    res.status(404);
    throw new Error("Insurance case not found");
  }

  const idempotencyKey = safeString(
    req.headers["idempotency-key"] ||
      req.body?.idempotencyKey ||
      req.body?.idempotency_key,
  ).trim();

  const existingByKey = idempotencyKey
    ? (Array.isArray(doc.paymentHistory) ? doc.paymentHistory : []).find(
        (row) =>
          safeString(row?.idempotencyKey).trim() &&
          safeString(row?.idempotencyKey).trim() === idempotencyKey,
      )
    : null;

  if (existingByKey) {
    return res.status(200).json({
      success: true,
      duplicate: true,
      payment: existingByKey,
      paymentHistory: doc.paymentHistory || [],
    });
  }

  const entry = sanitizePaymentHistoryRow({
    ...(req.body || {}),
    ...(idempotencyKey ? { idempotencyKey } : {}),
  });
  const amount = Number(entry?.amount || 0);
  if (!entry || !Number.isFinite(amount) || amount <= 0) {
    res.status(400);
    throw new Error("Valid payment amount is required");
  }

  const updateQuery = { _id: doc._id };
  if (idempotencyKey) {
    updateQuery["paymentHistory.idempotencyKey"] = { $ne: idempotencyKey };
  }

  let saved = await InsuranceCase.findOneAndUpdate(
    updateQuery,
    {
      $push: { paymentHistory: entry },
      $set: { updatedAt: new Date() },
    },
    { new: true },
  );

  if (!saved) {
    const latestDoc = await InsuranceCase.findById(doc._id);
    if (!latestDoc) {
      res.status(404);
      throw new Error("Insurance case not found");
    }
    if (idempotencyKey) {
      const duplicateRow = (Array.isArray(latestDoc.paymentHistory)
        ? latestDoc.paymentHistory
        : []
      ).find(
        (row) =>
          safeString(row?.idempotencyKey).trim() &&
          safeString(row?.idempotencyKey).trim() === idempotencyKey,
      );
      if (duplicateRow) {
        return res.status(200).json({
          success: true,
          duplicate: true,
          payment: duplicateRow,
          paymentHistory: latestDoc.paymentHistory || [],
        });
      }
    }
    res.status(409);
    throw new Error("Payment could not be appended due to concurrent update");
  }
  const latest = Array.isArray(saved.paymentHistory)
    ? saved.paymentHistory[saved.paymentHistory.length - 1]
    : null;

  return res.status(201).json({
    success: true,
    payment: latest,
    paymentHistory: saved.paymentHistory || [],
  });
});

// @desc    Sync insurance customer payment to receivables module
// @route   POST /api/insurance/:id/sync-receivable
// @access  Public
export const syncInsuranceReceivable = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const doc =
    (mongoose.Types.ObjectId.isValid(raw)
      ? await InsuranceCase.findById(raw)
      : null) || (await InsuranceCase.findOne({ caseId: raw }));

  if (!doc) {
    res.status(404);
    throw new Error("Insurance case not found");
  }

  const expectedAmount = Number(
    doc.customerPaymentExpected ?? doc.customer_payment_expected ?? 0
  );
  if (expectedAmount <= 0) {
    res.status(400);
    throw new Error("No customer payment expected for this case");
  }

  const payoutId = `INS-RCV-${doc.caseId}`;
  
  const receivedAmount = Number(
    doc.customerPaymentReceived ?? doc.customer_payment_received ?? 0
  );
  const pendingAmount = Math.max(0, expectedAmount - receivedAmount);
  
  let status = "Expected";
  if (receivedAmount >= expectedAmount && receivedAmount > 0) {
    status = "Received";
  } else if (receivedAmount > 0) {
    status = "Partial";
  }

  const receivablePayload = {
    receivableKind: "insurance",
    sourceModule: "Insurance",
    loanId: "",
    insuranceCaseId: doc.caseId,
    insuranceCaseMongoId: doc._id,
    customerName: doc.customerName || doc.customerSnapshot?.customerName || "",
    payoutId,
    sourceArrayKey: "insurance_receivable",
    payout_type: "Insurance Premium",
    payout_party_name: doc.customerName || "Customer",
    payout_direction: "Receivable",
    payout_status: status,
    payout_amount: expectedAmount,
    net_payout_amount: expectedAmount,
    tds_amount: 0,
    tds_percentage: 0,
    payout_received_date: receivedAmount >= expectedAmount ? new Date() : null,
    created_date: doc.createdAt || new Date(),
    payment_history: (Array.isArray(doc.paymentHistory)
      ? doc.paymentHistory
      : Array.isArray(doc.payment_history)
        ? doc.payment_history
        : []
    )
      .filter((p) => (p.paymentType ?? p.payment_type) === "customer")
      .map((p) => ({
        amount: p.amount,
        date: p.date,
        mode: p.paymentMode ?? p.payment_mode,
        remarks: p.remarks,
        transactionRef: p.transactionRef ?? p.transaction_ref,
      })),
    activity_log: [],
    meta_source: "Insurance Module",
    payload: {
      caseId: doc.caseId,
      insuranceCompany: doc.newInsuranceCompany,
      policyNumber: doc.newPolicyNumber,
      registrationNumber: doc.registrationNumber,
      vehicleMake: doc.vehicleMake,
      vehicleModel: doc.vehicleModel,
    },
  };

  const existing = await Receivable.findOne({
    insuranceCaseId: doc.caseId,
    payoutId,
  });

  let receivable;
  if (existing) {
    Object.assign(existing, receivablePayload);
    receivable = await existing.save();
  } else {
    receivable = await Receivable.create(receivablePayload);
  }

  res.json({
    success: true,
    message: "Insurance receivable synced successfully",
    data: receivable,
  });
});

// @desc    Get payout rate by company and date (latest effective)
// @route   GET /api/insurance/payout-rates
// @access  Public
export const getInsurancePayoutRate = asyncHandler(async (req, res) => {
  const companyName = safeString(req.query.companyName).trim();
  const onDateRaw = safeString(req.query.onDate).trim();
  const onDate = onDateRaw ? new Date(onDateRaw) : new Date();

  if (!companyName) {
    res.status(400);
    throw new Error("companyName query param is required");
  }

  const isValidDate = !Number.isNaN(onDate.getTime());
  const effectiveDate = isValidDate ? onDate : new Date();

  const row = await InsurancePayoutRate.findOne({
    companyName,
    active: true,
    effectiveFrom: { $lte: effectiveDate },
  })
    .sort({ effectiveFrom: -1, createdAt: -1 })
    .lean();

  const payoutPercentage = Number(
    row?.payoutPercentage ?? DEFAULT_INSURANCE_PAYOUT_PERCENTAGE,
  );

  res.json({
    success: true,
    data: {
      companyName,
      payoutPercentage,
      source: row ? "db" : "default",
      effectiveFrom: row?.effectiveFrom || null,
      rateId: row?._id || null,
    },
  });
});

// @desc    Upsert payout rate (company/date specific)
// @route   POST /api/insurance/payout-rates
// @access  Public
export const upsertInsurancePayoutRate = asyncHandler(async (req, res) => {
  const companyName = safeString(req.body?.companyName).trim();
  const notes = safeString(req.body?.notes).trim();
  const active = req.body?.active !== false;
  const payoutPercentage = Number(req.body?.payoutPercentage);
  const effectiveFromRaw = safeString(req.body?.effectiveFrom).trim();
  const effectiveFrom = effectiveFromRaw ? new Date(effectiveFromRaw) : new Date();

  if (!companyName) {
    res.status(400);
    throw new Error("companyName is required");
  }
  if (!Number.isFinite(payoutPercentage) || payoutPercentage < 0 || payoutPercentage > 100) {
    res.status(400);
    throw new Error("payoutPercentage must be between 0 and 100");
  }
  if (Number.isNaN(effectiveFrom.getTime())) {
    res.status(400);
    throw new Error("effectiveFrom is invalid");
  }

  const row = await InsurancePayoutRate.findOneAndUpdate(
    {
      companyName,
      effectiveFrom,
    },
    {
      $set: {
        payoutPercentage,
        active,
        notes,
      },
    },
    {
      new: true,
      upsert: true,
      setDefaultsOnInsert: true,
    },
  );

  res.status(201).json({ success: true, data: row });
});
