import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import Counter from "../models/Counter.js";
import Customer from "../models/Customer.js";
import InsuranceCase from "../models/InsuranceCase.js";
import InsurancePayoutRate from "../models/InsurancePayoutRate.js";
import Receivable from "../models/Receivable.js";
import VehicleFeature from "../models/VehicleFeature.js";
import VehicleRecord from "../models/VehicleRecord.js";

const INSURANCE_COUNTER_PREFIX = "insurance_case_id_sequence_";
const INSURANCE_ID_PREFIX = "INS";
const INSURANCE_TEMP_REG_COUNTER_KEY = "insurance_temp_registration_sequence";
const DEFAULT_INSURANCE_PAYOUT_PERCENTAGE = 10;

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

const normalizeStep1Payload = (payload = {}) => {
  const sourceNormalized = safeString(
    payload.source || payload.sourceOrigin || payload.recordSource || "Direct",
  ).trim();
  const payoutPercentRaw = Number(
    payload.payoutPercent ?? payload.payoutPercentage ?? 0,
  );
  const payoutPercent = Number.isFinite(payoutPercentRaw)
    ? payoutPercentRaw
    : 0;

  return {
    ...payload,
    policyCategory: safeString(
      payload.policyCategory || payload.policyTypeSelector || "Insurance Policy",
    ).trim(),
    policyTypeSelector: safeString(
      payload.policyTypeSelector || payload.policyCategory || "Insurance Policy",
    ).trim(),
    source: sourceNormalized || "Direct",
    sourceOrigin: sourceNormalized || "Direct",
    payoutPercent,
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
  if (!registrationNumberNormalized) return null;

  const cubicCapacityParsed = extractCubicCapacity(doc.cubicCapacity);
  const updateDoc = {
    registrationNumber: registrationNumber || registrationNumberNormalized,
    registrationNumberNormalized,
    registrationNumberLast4: registrationNumberNormalized.slice(-4),
    make: safeString(doc.vehicleMake).trim(),
    model: safeString(doc.vehicleModel).trim(),
    variant: safeString(doc.vehicleVariant).trim(),
    cubicCapacityCc: Number.isFinite(cubicCapacityParsed)
      ? cubicCapacityParsed
      : undefined,
    engineNumber: safeString(doc.engineNumber).trim(),
    chassisNumber: safeString(doc.chassisNumber).trim(),
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

  return await VehicleRecord.findOneAndUpdate(
    { registrationNumberNormalized },
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
  const payload = normalizeStep1Payload(req.body || {});

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

  await upsertVehicleRecordFromInsuranceCase(doc);

  res.status(201).json({ success: true, data: doc });
});

// @desc    Update insurance case (full replace/merge style)
// @route   PUT /api/insurance/:id
// @access  Public
export const updateInsuranceCase = asyncHandler(async (req, res) => {
  const raw = safeString(req.params.id).trim();
  const payload = normalizeStep1Payload(req.body || {});

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
  await upsertVehicleRecordFromInsuranceCase(saved);
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
