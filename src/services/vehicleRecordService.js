import Vehicle from "../models/Vehicle.js";
import VehicleFeature from "../models/VehicleFeature.js";
import VehicleRecord from "../models/VehicleRecord.js";

const pickFirst = (...values) =>
  values.find((value) => value !== undefined && value !== null && String(value).trim() !== "");

const cleanText = (value) => String(value || "").trim();

const normalizeRegNo = (value) =>
  String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

const normalizeLoose = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeMake = (value) => {
  const normalized = normalizeLoose(value);
  const aliases = {
    mercedes: "mercedes benz",
    "mercedes-benz": "mercedes benz",
    "mercedes benz": "mercedes benz",
    benz: "mercedes benz",
    maruti: "maruti suzuki",
    "maruti suzuki": "maruti suzuki",
  };
  return aliases[normalized] || normalized;
};

const parseNumeric = (value) => {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const parsed = Number(String(value).replace(/[^\d.]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
};

const parseDateValue = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const parseCcFromText = (text) => {
  const source = String(text || "").replace(/,/g, " ");
  const ccMatch = source.match(/(\d{2,5}(?:\.\d+)?)\s*cc/i);
  if (ccMatch) {
    const n = Number(ccMatch[1]);
    return Number.isFinite(n) ? Math.round(n) : null;
  }
  const plain = source.match(/\b(\d{3,5})\b/);
  if (plain) {
    const n = Number(plain[1]);
    if (Number.isFinite(n)) return Math.round(n);
  }
  return null;
};

const extractCcFromFeaturesObject = (features) => {
  if (!features || typeof features !== "object") return null;
  const candidates = [];
  Object.entries(features).forEach(([key, value]) => {
    const keyNorm = normalizeLoose(key);
    if (
      keyNorm.includes("displacement") ||
      keyNorm.includes("engine displacement") ||
      keyNorm.includes("cubic capacity")
    ) {
      candidates.push(value);
    }
  });

  for (const value of candidates) {
    const parsed = parseCcFromText(value);
    if (parsed) return parsed;
  }
  return null;
};

const buildLookupCandidates = ({ make, model, variant }) => {
  const mk = cleanText(make);
  const md = cleanText(model);
  const vr = cleanText(variant);

  const makeNorm = normalizeMake(mk);
  const makeCandidates = [...new Set([mk, makeNorm, makeNorm.replace(/\s+/g, "-")].filter(Boolean))];
  const modelCandidates = [...new Set([md, `${mk} ${md}`.trim()].filter(Boolean))];
  const variantCandidates = [
    ...new Set([
      vr,
      `${mk} ${vr}`.trim(),
      `${md} ${vr}`.trim(),
      `${mk} ${md} ${vr}`.trim(),
    ].filter(Boolean)),
  ];

  return { makeCandidates, modelCandidates, variantCandidates };
};

const resolveCcFromVehicleDatabases = async ({ make, model, variant }) => {
  if (!make || !model || !variant) return null;
  const { makeCandidates, modelCandidates, variantCandidates } = buildLookupCandidates({
    make,
    model,
    variant,
  });

  const makeRegexes = makeCandidates.map((value) => new RegExp(`^${value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i"));
  const modelRegexes = modelCandidates.map((value) => new RegExp(`^${value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i"));
  const variantRegexes = variantCandidates.map((value) => new RegExp(`^${value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i"));

  const featureDoc = await VehicleFeature.findOne({
    $and: [
      { $or: [{ brand: { $in: makeCandidates } }, { brand: { $in: makeRegexes } }] },
      { $or: [{ model: { $in: modelCandidates } }, { model: { $in: modelRegexes } }] },
      { $or: [{ variant: { $in: variantCandidates } }, { variant: { $in: variantRegexes } }] },
    ],
  }).lean();

  const ccFromFeatures = extractCcFromFeaturesObject(featureDoc?.features);
  if (ccFromFeatures) return ccFromFeatures;

  const vehicleDoc = await Vehicle.findOne({
    $and: [
      {
        $or: [
          { make: { $in: makeCandidates } },
          { brand: { $in: makeCandidates } },
          { make: { $in: makeRegexes } },
          { brand: { $in: makeRegexes } },
        ],
      },
      { $or: [{ model: { $in: modelCandidates } }, { model: { $in: modelRegexes } }] },
      { $or: [{ variant: { $in: variantCandidates } }, { variant: { $in: variantRegexes } }] },
    ],
  }).lean();

  const ccFromVehicle =
    parseNumeric(vehicleDoc?.cubicCapacityCc) ||
    parseCcFromText(vehicleDoc?.engineDisplacement) ||
    extractCcFromFeaturesObject(vehicleDoc?.features);

  return ccFromVehicle || null;
};

const buildVehicleRecordPayload = async (loanDoc) => {
  if (!loanDoc) return null;

  const registrationNumber = cleanText(
    pickFirst(
      loanDoc.registrationNumber,
      loanDoc.rc_redg_no,
      loanDoc.vehicleRegNo,
      loanDoc.vehicleRegdNumber,
    ),
  );
  const make = cleanText(pickFirst(loanDoc.vehicleMake, loanDoc.make));
  const model = cleanText(pickFirst(loanDoc.vehicleModel, loanDoc.model));
  const variant = cleanText(pickFirst(loanDoc.vehicleVariant, loanDoc.variant));

  const cubicCapacitySeed =
    parseNumeric(loanDoc.cubicCapacityCc) ||
    parseNumeric(loanDoc.cubicCapacity) ||
    parseCcFromText(loanDoc.engineDisplacement);
  const cubicCapacityCc =
    cubicCapacitySeed || (await resolveCcFromVehicleDatabases({ make, model, variant }));

  const rawRegistrationDate = pickFirst(
    loanDoc.rc_redg_date,
    loanDoc.registrationDate,
    loanDoc.regdDate,
  );

  const payload = {
    loanId: cleanText(loanDoc.loanId),
    customerId: loanDoc.customerId || undefined,
    customerName: cleanText(loanDoc.customerName),
    primaryMobile: cleanText(loanDoc.primaryMobile),

    registrationNumber,
    registrationNumberNormalized: normalizeRegNo(registrationNumber),
    make,
    model,
    variant,
    cubicCapacityCc: cubicCapacityCc || undefined,
    engineNumber: cleanText(
      pickFirst(
        loanDoc.engineNumber,
        loanDoc.rc_engine_no,
        loanDoc.vehicleEngineNo,
      ),
    ),
    chassisNumber: cleanText(
      pickFirst(
        loanDoc.chassisNumber,
        loanDoc.rc_chassis_no,
        loanDoc.vehicleChassisNo,
      ),
    ),
    manufactureMonth: cleanText(
      pickFirst(
        loanDoc.manufactureMonth,
        loanDoc.manufacturingMonth,
        loanDoc.mfgMonth,
      ),
    ),
    yearOfManufacture: cleanText(
      pickFirst(
        loanDoc.yearOfManufacture,
        loanDoc.manufacturingYear,
        loanDoc.yearOfReg,
      ),
    ),
    registrationDate: parseDateValue(rawRegistrationDate) || undefined,
    hypothecation: cleanText(
      pickFirst(loanDoc.hypothecationBank, loanDoc.hypothecation),
    ),
    registrationCity: cleanText(
      pickFirst(loanDoc.registrationCity, loanDoc.postfile_regd_city, loanDoc.city),
    ),

    sourceLoanType: cleanText(pickFirst(loanDoc.typeOfLoan, loanDoc.loanType)),
    sourceCaseType: cleanText(loanDoc.caseType),
    sourceLoanUpdatedAt: parseDateValue(loanDoc.updatedAt) || undefined,
    lastSyncedAt: new Date(),
  };

  // Only persist if at least one primary vehicle identifier is present.
  const hasCoreIdentity =
    payload.registrationNumberNormalized ||
    payload.make ||
    payload.model ||
    payload.variant ||
    payload.engineNumber ||
    payload.chassisNumber;
  if (!hasCoreIdentity) return null;
  return payload;
};

export const upsertVehicleRecordFromLoan = async (loanDoc) => {
  const payload = await buildVehicleRecordPayload(loanDoc);
  if (!payload) return null;

  const matchers = [];
  if (payload.loanId) matchers.push({ loanId: payload.loanId });
  if (payload.registrationNumberNormalized) {
    matchers.push({ registrationNumberNormalized: payload.registrationNumberNormalized });
  }

  let existing = null;
  if (matchers.length) {
    existing = await VehicleRecord.findOne({ $or: matchers });
  }

  if (existing) {
    Object.assign(existing, payload);
    return await existing.save();
  }

  return await VehicleRecord.create(payload);
};
