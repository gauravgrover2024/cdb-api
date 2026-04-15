import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import Counter from "../models/Counter.js";
import UsedCarLead from "../models/UsedCarLead.js";

const USED_CAR_LEAD_COUNTER_PREFIX = "used_car_lead_sequence_";
const USED_CAR_LEAD_ID_PREFIX = "UCL";

const safeString = (value) =>
  value === undefined || value === null ? "" : String(value);

const escapeRegex = (value = "") =>
  safeString(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const normalizeText = (value) =>
  safeString(value)
    .trim()
    .replace(/\s+/g, " ");

const normalizeHeaderKey = (value) =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const normalizeMoney = (value) => {
  const n = Number(String(value ?? "").replace(/[^\d.]/g, "") || 0);
  return Number.isFinite(n) ? n : 0;
};

const normalizeNumber = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeBoolean = (value) => {
  if (value === true || value === false) return value;
  const text = normalizeText(value).toLowerCase();
  if (!text) return null;
  if (["yes", "true", "1", "y"].includes(text)) return true;
  if (["no", "false", "0", "n"].includes(text)) return false;
  return null;
};

const normalizeInsuranceCategory = (value) => {
  const text = normalizeText(value).toLowerCase();
  if (!text) return "";
  if (text.includes("zero")) return "Zero-Dep";
  if (text.includes("third")) return "Third Party";
  if (text.includes("expired")) return "Expired";
  if (text.includes("comprehensive") || text.includes("valid")) return "Comprehensive";
  return normalizeText(value);
};

const normalizeStatus = (value) => {
  const status = normalizeText(value);
  if (!status) return "New";
  if (status === "Attempted") return "Not Answered";
  return status;
};

const parseDate = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;

  const text = normalizeText(value);
  if (!text) return null;

  const ddmmyyyy = text.match(/^(\d{2})-(\d{2})-(\d{4})$/);
  if (ddmmyyyy) {
    const parsed = new Date(`${ddmmyyyy[3]}-${ddmmyyyy[2]}-${ddmmyyyy[1]}`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const firstPresent = (...values) =>
  values.find((value) => {
    if (typeof value === "number") return Number.isFinite(value);
    return Boolean(normalizeText(value));
  });

const pickMapped = (mapped, aliases) =>
  firstPresent(...aliases.map((alias) => mapped[normalizeHeaderKey(alias)]));

const buildSourceLeadKey = ({ source, c2bLeadId, sourceLeadId }) => {
  const src = normalizeText(source).toLowerCase();
  const external = normalizeText(sourceLeadId || c2bLeadId).toLowerCase();
  if (!src || !external) return "";
  return `${src}|${external}`;
};

const hasPath = (obj, path) => {
  if (!obj || !path) return false;
  const parts = String(path).split(".");
  let cursor = obj;
  for (let i = 0; i < parts.length; i += 1) {
    const part = parts[i];
    if (!cursor || !Object.prototype.hasOwnProperty.call(cursor, part)) {
      return false;
    }
    cursor = cursor[part];
  }
  return true;
};

const hasAnyPath = (obj, paths = []) => paths.some((path) => hasPath(obj, path));

const buildDedupeFingerprint = (payload = {}) => {
  const mobile = normalizeText(payload?.seller?.mobile || payload.mobile)
    .replace(/\D/g, "")
    .slice(-10);
  const regNo = normalizeText(payload?.vehicle?.regNo || payload.regNo).toLowerCase();
  const make = normalizeText(payload?.vehicle?.make || payload.make).toLowerCase();
  const model = normalizeText(payload?.vehicle?.model || payload.model).toLowerCase();
  const variant = normalizeText(payload?.vehicle?.variant || payload.variant).toLowerCase();
  return [mobile, regNo, make, model, variant].filter(Boolean).join("|");
};

const calculateProcurementScore = (lead = {}) => {
  let score = 20;
  const mileage = Number(lead?.vehicle?.mileage || 0);
  const owner = normalizeText(lead?.vehicle?.ownership).toLowerCase();
  const insurance = normalizeText(lead?.vehicle?.insuranceCategory).toLowerCase();
  const hypothecation = lead?.vehicle?.hypothecation === true;
  const accident = lead?.vehicle?.accidentPaintHistory === true;
  const price = Number(lead?.pricing?.updatedExpectedPrice || lead?.pricing?.expectedPrice || 0);
  const modelKey = `${normalizeText(lead?.vehicle?.make)} ${normalizeText(lead?.vehicle?.model)}`.toLowerCase();

  if (mileage > 0 && mileage <= 30000) score += 18;
  else if (mileage <= 60000) score += 12;
  else if (mileage <= 90000) score += 6;

  if (owner.includes("1")) score += 14;
  else if (owner.includes("2")) score += 8;
  else if (owner) score += 4;

  if (insurance.includes("zero")) score += 10;
  else if (insurance.includes("comprehensive")) score += 8;
  else if (insurance.includes("third")) score += 4;

  if (!hypothecation) score += 10;
  if (!accident) score += 8;
  if (price > 0 && price <= 1500000) score += 6;
  if (["swift", "baleno", "wagon r", "city", "creta", "i20", "grand i10", "innova crysta", "nexon", "brezza"].some((item) => modelKey.includes(item))) score += 8;
  if (normalizeText(lead?.vehicle?.variant)) score += 4;
  if (normalizeText(lead?.vehicle?.regNo)) score += 2;

  return Math.max(0, Math.min(100, Math.round(score)));
};

const getNextInternalLeadId = async () => {
  const year = new Date().getFullYear();
  const key = `${USED_CAR_LEAD_COUNTER_PREFIX}${year}`;
  const next = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    { upsert: true, new: true },
  );
  return `${USED_CAR_LEAD_ID_PREFIX}-${year}-${String(next?.value || 1).padStart(4, "0")}`;
};

const reserveInternalLeadIds = async (count = 0) => {
  const safeCount = Math.max(0, Number(count) || 0);
  if (!safeCount) return [];

  const year = new Date().getFullYear();
  const key = `${USED_CAR_LEAD_COUNTER_PREFIX}${year}`;
  const next = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: safeCount } },
    { upsert: true, new: true },
  );

  const lastValue = Number(next?.value || safeCount);
  const firstValue = lastValue - safeCount + 1;

  return Array.from({ length: safeCount }, (_, index) =>
    `${USED_CAR_LEAD_ID_PREFIX}-${year}-${String(firstValue + index).padStart(4, "0")}`,
  );
};

const buildActivity = (payload = {}) => ({
  activityId: normalizeText(payload.activityId) || `ACT-${Date.now()}`,
  type: normalizeText(payload.type) || "note",
  title: normalizeText(payload.title) || "Activity",
  detail: normalizeText(payload.detail),
  at: parseDate(payload.at) || new Date(),
  actorId:
    payload.actorId && mongoose.Types.ObjectId.isValid(payload.actorId)
      ? new mongoose.Types.ObjectId(payload.actorId)
      : undefined,
  actorName: normalizeText(payload.actorName),
  meta: payload.meta && typeof payload.meta === "object" ? payload.meta : {},
});

const buildCallLog = (payload = {}) => ({
  logId: normalizeText(payload.logId) || `CALL-${Date.now()}`,
  at: parseDate(payload.at) || new Date(),
  status: normalizeText(payload.status),
  outcome: normalizeText(payload.outcome),
  notes: normalizeText(payload.notes),
  durationSeconds: Number(payload.durationSeconds || 0) || 0,
  nextFollowUpAt: parseDate(payload.nextFollowUpAt),
  createdBy:
    payload.createdBy && mongoose.Types.ObjectId.isValid(payload.createdBy)
      ? new mongoose.Types.ObjectId(payload.createdBy)
      : undefined,
  createdByName: normalizeText(payload.createdByName),
});

const buildFollowUp = (payload = {}) => ({
  followUpId: normalizeText(payload.followUpId) || `FU-${Date.now()}`,
  dueAt: parseDate(payload.dueAt),
  status: normalizeText(payload.status) || "Scheduled",
  notes: normalizeText(payload.notes),
  createdAt: parseDate(payload.createdAt) || new Date(),
  completedAt: parseDate(payload.completedAt),
  createdBy:
    payload.createdBy && mongoose.Types.ObjectId.isValid(payload.createdBy)
      ? new mongoose.Types.ObjectId(payload.createdBy)
      : undefined,
  createdByName: normalizeText(payload.createdByName),
});

const normalizeFileAsset = (payload = {}, index = 0) => ({
  uid: normalizeText(payload.uid) || `file-${Date.now()}-${index}`,
  name: normalizeText(payload.name) || `Photo ${index + 1}`,
  url: normalizeText(firstPresent(payload.url, payload.secure_url, payload.preview)),
  thumbUrl: normalizeText(firstPresent(payload.thumbUrl, payload.preview, payload.url, payload.secure_url)),
  preview: normalizeText(firstPresent(payload.preview, payload.thumbUrl, payload.url, payload.secure_url)),
  evidenceTag: normalizeText(payload.evidenceTag),
  customTagName: normalizeText(payload.customTagName),
  publicId: normalizeText(firstPresent(payload.publicId, payload.public_id)),
  format: normalizeText(payload.format),
  size: Number(payload.size || 0) || 0,
  uploadedAt: parseDate(payload.uploadedAt) || new Date(),
  source: normalizeText(payload.source) || "r2",
});

const normalizeFileArray = (files = []) =>
  (Array.isArray(files) ? files : [])
    .map((file, index) => normalizeFileAsset(file, index))
    .filter((file) => file.url || file.publicId);

const normalizeMapObject = (value = {}) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};

const normalizeInspectionItemMap = (items = {}) =>
  Object.fromEntries(
    Object.entries(normalizeMapObject(items)).map(([key, rawValue]) => {
      const value = normalizeMapObject(rawValue);
      return [
        key,
        {
          status: Array.isArray(value.status)
            ? value.status.map((entry) => normalizeText(entry)).filter(Boolean)
            : normalizeText(value.status)
              ? [normalizeText(value.status)]
              : [],
          severity: normalizeText(value.severity),
          photos: normalizeFileArray(value.photos),
          treadDepth:
            value.treadDepth === "" || value.treadDepth === undefined || value.treadDepth === null
              ? null
              : Number(value.treadDepth) || null,
          tyreBrand: normalizeText(value.tyreBrand),
        },
      ];
    }),
  );

const normalizePhotoBuckets = (photoBuckets = {}) =>
  Object.fromEntries(
    Object.entries(normalizeMapObject(photoBuckets)).map(([key, files]) => [
      key,
      normalizeFileArray(files),
    ]),
  );

const normalizeLeadVerification = (value = {}) =>
  Object.fromEntries(
    Object.entries(normalizeMapObject(value)).map(([key, checked]) => [
      key,
      Boolean(checked),
    ]),
  );

const normalizeRefurbSection = (value = {}) => {
  const section = normalizeMapObject(value);
  return {
    status: normalizeText(section.status) || "OK",
    cost: Number(section.cost || 0) || 0,
    cap: Number(section.cap || 0) || 0,
    issueCount: Number(section.issueCount || 0) || 0,
    notes: Array.isArray(section.notes)
      ? section.notes.map((entry) => normalizeText(entry)).filter(Boolean)
      : [],
    noGoReasons: Array.isArray(section.noGoReasons)
      ? section.noGoReasons.map((entry) => normalizeText(entry)).filter(Boolean)
      : [],
  };
};

const normalizeRefurbSummary = (value = {}) => {
  const summary = normalizeMapObject(value);
  const sections = normalizeMapObject(summary.sections);
  return {
    noGo: Boolean(summary.noGo),
    noGoReasons: Array.isArray(summary.noGoReasons)
      ? summary.noGoReasons.map((entry) => normalizeText(entry)).filter(Boolean)
      : [],
    totalCost: Number(summary.totalCost || 0) || 0,
    suggestedBuyPrice: Number(summary.suggestedBuyPrice || 0) || 0,
    insuranceValidAndComprehensive: Boolean(summary.insuranceValidAndComprehensive),
    sections: {
      exterior: normalizeRefurbSection(sections.exterior),
      exteriorFitment: normalizeRefurbSection(sections.exteriorFitment),
      wheelsTyres: normalizeRefurbSection(sections.wheelsTyres),
      engineMechanical: normalizeRefurbSection(sections.engineMechanical),
      interiorElectrical: normalizeRefurbSection(sections.interiorElectrical),
      safety: normalizeRefurbSection(sections.safety),
      roadTest: normalizeRefurbSection(sections.roadTest),
      acSystem: normalizeRefurbSection(sections.acSystem),
    },
  };
};

const normalizeInspectionReport = (value = {}) => {
  const report = normalizeMapObject(value);
  return {
    reportVersion: normalizeText(firstPresent(report.reportVersion, report.version)),
    generatedAt: parseDate(firstPresent(report.generatedAt, report.updatedAt)),
    customerName: normalizeText(report.customerName),
    inspectionLocation: normalizeText(report.inspectionLocation),
    registrationNumber: normalizeText(report.registrationNumber).toUpperCase(),
    insuranceType: normalizeText(report.insuranceType),
    insuranceExpiry: parseDate(report.insuranceExpiry),
    makeConfirmation: normalizeText(report.makeConfirmation),
    modelConfirmation: normalizeText(report.modelConfirmation),
    variantConfirmation: normalizeText(report.variantConfirmation),
    leadVerification: normalizeLeadVerification(report.leadVerification),
    photoBuckets: normalizePhotoBuckets(report.photoBuckets),
    bulkEvidence: normalizeFileArray(report.bulkEvidence),
    evidenceTags: Array.isArray(report.evidenceTags)
      ? report.evidenceTags.map((entry) => normalizeText(entry)).filter(Boolean)
      : [],
    items: normalizeInspectionItemMap(report.items),
    airbagCount: normalizeText(report.airbagCount),
    powerWindowCount: normalizeText(report.powerWindowCount),
    seatMaterial: normalizeText(report.seatMaterial),
    estimatedRefurbCost: Number(report.estimatedRefurbCost || 0) || 0,
    evaluatorPrice: Number(report.evaluatorPrice || 0) || 0,
    suggestedBuyPrice: Number(report.suggestedBuyPrice || 0) || 0,
    negotiationNotes: normalizeText(report.negotiationNotes),
    overallRemarks: normalizeText(report.overallRemarks),
    noGoReasons: Array.isArray(report.noGoReasons)
      ? report.noGoReasons.map((entry) => normalizeText(entry)).filter(Boolean)
      : [],
    refurb: normalizeRefurbSummary(report.refurb),
  };
};

const normalizeLeadPayload = (payload = {}) => {
  const mapped = Object.fromEntries(
    Object.entries(payload || {}).map(([key, value]) => [normalizeHeaderKey(key), value]),
  );

  const source = normalizeText(firstPresent(payload.source, pickMapped(mapped, ["Source"])));
  const c2bLeadId = normalizeText(
    firstPresent(
      payload?.externalRefs?.c2bLeadId,
      payload.c2bLeadId,
      payload.leadId,
      pickMapped(mapped, ["C2B Lead Id", "Lead Id"]),
    ),
  );

  const seller = {
    name: normalizeText(firstPresent(payload?.seller?.name, payload.name, pickMapped(mapped, ["Name"]))),
    mobile: normalizeText(firstPresent(payload?.seller?.mobile, payload.mobile, pickMapped(mapped, ["Mobile"]))),
    alternateMobile: normalizeText(firstPresent(payload?.seller?.alternateMobile, payload.alternateMobile)),
    email: normalizeText(firstPresent(payload?.seller?.email, payload.email, pickMapped(mapped, ["Email"]))),
    address: normalizeText(firstPresent(payload?.seller?.address, payload.address)),
    area: normalizeText(firstPresent(payload?.seller?.area, payload.area, pickMapped(mapped, ["Area"]))),
    pincode: normalizeText(firstPresent(payload?.seller?.pincode, payload.pincode, pickMapped(mapped, ["Pincode"]))),
    pincodeCity: normalizeText(firstPresent(payload?.seller?.pincodeCity, payload.pincodeCity, pickMapped(mapped, ["Pincode City"]))),
    city: normalizeText(firstPresent(payload?.seller?.city, payload.city, pickMapped(mapped, ["City"]))),
    state: normalizeText(firstPresent(payload?.seller?.state, payload.state)),
  };

  const insuranceValue = normalizeText(firstPresent(payload?.vehicle?.insurance, payload.insurance, pickMapped(mapped, ["Insurance"])));

  const vehicle = {
    make: normalizeText(firstPresent(payload?.vehicle?.make, payload.make, pickMapped(mapped, ["Make"]))),
    model: normalizeText(firstPresent(payload?.vehicle?.model, payload.model, pickMapped(mapped, ["Model"]))),
    variant: normalizeText(firstPresent(payload?.vehicle?.variant, payload.variant, pickMapped(mapped, ["Version", "Variant"]))),
    mfgYear: normalizeText(firstPresent(payload?.vehicle?.mfgYear, payload.mfgYear, pickMapped(mapped, ["Mfg Year"]))),
    mfgMonth: normalizeText(firstPresent(payload?.vehicle?.mfgMonth, payload.mfgMonth, pickMapped(mapped, ["Mfg Month"]))),
    color: normalizeText(firstPresent(payload?.vehicle?.color, payload.color, pickMapped(mapped, ["Color"]))),
    mileage: normalizeNumber(firstPresent(payload?.vehicle?.mileage, payload.mileage, payload.milage, pickMapped(mapped, ["Mileage", "Milage"]))),
    fuel: normalizeText(firstPresent(payload?.vehicle?.fuel, payload.fuel, pickMapped(mapped, ["Fuel"]))),
    regNo: normalizeText(firstPresent(payload?.vehicle?.regNo, payload.regNo, pickMapped(mapped, ["Regno", "Reg No", "Registration Number"]))).toUpperCase(),
    ownership: normalizeText(firstPresent(payload?.vehicle?.ownership, payload.ownership, pickMapped(mapped, ["Owner", "Ownership"]))),
    insurance: insuranceValue,
    insuranceCategory: normalizeInsuranceCategory(firstPresent(payload?.vehicle?.insuranceCategory, payload.insuranceCategory, insuranceValue)),
    insuranceExpiry: parseDate(firstPresent(payload?.vehicle?.insuranceExpiry, payload.insuranceExpiry)),
    hypothecation: normalizeBoolean(firstPresent(payload?.vehicle?.hypothecation, payload.hypothecation)),
    bankName: normalizeText(firstPresent(payload?.vehicle?.bankName, payload.bankName)),
    accidentPaintHistory: normalizeBoolean(firstPresent(payload?.vehicle?.accidentPaintHistory, payload.accidentPaintHistory)),
    accidentPaintNotes: normalizeText(firstPresent(payload?.vehicle?.accidentPaintNotes, payload.accidentPaintNotes)),
  };

  const pricing = {
    expectedPrice: normalizeMoney(firstPresent(payload?.pricing?.expectedPrice, payload.expectedPrice, pickMapped(mapped, ["Expected Price"]))),
    updatedExpectedPrice: (() => {
      const value = firstPresent(payload?.pricing?.updatedExpectedPrice, payload.updatedExpectedPrice);
      if (value === undefined || value === null || value === "") return null;
      const amount = normalizeMoney(value);
      return amount > 0 ? amount : null;
    })(),
    procurementScore: Number(payload?.pricing?.procurementScore || payload.procurementScore || 0) || 0,
    scoreUpdatedAt: parseDate(payload?.pricing?.scoreUpdatedAt || payload.scoreUpdatedAt),
  };

  const status = normalizeStatus(firstPresent(payload?.workflow?.status, payload.status, pickMapped(mapped, ["Status"])));
  const closureReason = normalizeText(firstPresent(payload?.workflow?.closureReason, payload.closureReason));
  const isClosed = Boolean(payload?.workflow?.isClosed || payload.isClosed || status === "Closed" || closureReason);

  const workflow = {
    currentStage: normalizeText(firstPresent(payload?.workflow?.currentStage, payload.currentStage)) || (isClosed ? "closed" : status === "Inspection Scheduled" ? "inspection" : "lead-intake"),
    pipelineStage: normalizeText(firstPresent(payload?.workflow?.pipelineStage, payload.pipelineStage)) || (status === "Inspection Scheduled" ? "Inspection Queue" : "Lead Intake"),
    status,
    isClosed,
    closureReason,
    closureNotes: normalizeText(firstPresent(payload?.workflow?.closureNotes, payload.closureNotes)),
    closedAt: parseDate(firstPresent(payload?.workflow?.closedAt, payload.closedAt)),
    notes: normalizeText(firstPresent(payload?.workflow?.notes, payload.notes, pickMapped(mapped, ["Note against status"]))),
  };

  const assignment = {
    assignedTo: normalizeText(firstPresent(payload?.assignment?.assignedTo, payload.assignedTo, pickMapped(mapped, ["Executive Name"]))),
    assignedAt: parseDate(firstPresent(payload?.assignment?.assignedAt, payload.assignedAt)),
    assignmentRule: normalizeText(firstPresent(payload?.assignment?.assignmentRule, payload.assignmentRule)),
    assignmentNotes: normalizeText(firstPresent(payload?.assignment?.assignmentNotes, payload.assignmentNotes)),
  };

  const scheduling = {
    nextFollowUpAt: parseDate(firstPresent(payload?.scheduling?.nextFollowUpAt, payload.nextFollowUp, payload.nextFollowUpAt)),
    inspectionScheduledAt: parseDate(firstPresent(payload?.scheduling?.inspectionScheduledAt, payload.inspectionScheduledAt)),
    inspectionExecutiveName: normalizeText(firstPresent(payload?.scheduling?.inspectionExecutiveName, payload.inspectionExecutiveName)),
    inspectionExecutiveMobile: normalizeText(firstPresent(payload?.scheduling?.inspectionExecutiveMobile, payload.inspectionExecutiveMobile)),
  };

  const externalRefs = {
    c2bLeadId,
    ctiListingId: normalizeText(firstPresent(payload?.externalRefs?.ctiListingId, payload.ctiListingId, pickMapped(mapped, ["CTI Listing Id"]))),
    cwListingId: normalizeText(firstPresent(payload?.externalRefs?.cwListingId, payload.cwListingId, pickMapped(mapped, ["CW Listing Id"]))),
    pgClQleadId: normalizeText(firstPresent(payload?.externalRefs?.pgClQleadId, payload.pgClQleadId, pickMapped(mapped, ["PG Cl Qlead Id"]))),
    dealerId: normalizeText(firstPresent(payload?.externalRefs?.dealerId, payload.dealerId, pickMapped(mapped, ["Dealer Id"]))),
    sourceLeadId: normalizeText(firstPresent(payload?.externalRefs?.sourceLeadId, payload.sourceLeadId, c2bLeadId)),
    sourceLeadKey: "",
  };
  externalRefs.sourceLeadKey = buildSourceLeadKey({
    source,
    c2bLeadId: externalRefs.c2bLeadId,
    sourceLeadId: externalRefs.sourceLeadId,
  }) || undefined;

  const importMeta = {
    recordSource: normalizeText(firstPresent(payload?.importMeta?.recordSource, payload.recordSource)) || (payload.rawRow || Object.keys(mapped).length ? "excel-import" : "manual"),
    importedAt: parseDate(firstPresent(payload?.importMeta?.importedAt, payload.importedAt)),
    importBatchId: normalizeText(firstPresent(payload?.importMeta?.importBatchId, payload.importBatchId)),
    importFileName: normalizeText(firstPresent(payload?.importMeta?.importFileName, payload.importFileName)),
    importedBy:
      payload?.importMeta?.importedBy && mongoose.Types.ObjectId.isValid(payload.importMeta.importedBy)
        ? new mongoose.Types.ObjectId(payload.importMeta.importedBy)
        : undefined,
    importedByName: normalizeText(firstPresent(payload?.importMeta?.importedByName, payload.importedByName)),
    rawRow: payload?.importMeta?.rawRow || payload.rawRow || {},
  };

  const rawInspection = normalizeMapObject(payload?.inspection);
  const inspectionReport = normalizeInspectionReport(
    rawInspection.report || payload.report || {},
  );

  const inspection = {
    inspectionId: normalizeText(firstPresent(payload?.inspection?.inspectionId, payload.inspectionId)),
    executiveName: normalizeText(firstPresent(payload?.inspection?.executiveName, payload.inspectionExecutiveName)),
    executiveMobile: normalizeText(firstPresent(payload?.inspection?.executiveMobile, payload.inspectionExecutiveMobile)),
    conducted: normalizeBoolean(firstPresent(payload?.inspection?.conducted, payload.inspectionConducted)),
    verdict: normalizeText(firstPresent(payload?.inspection?.verdict, payload.inspectionVerdict)),
    noGoReason: normalizeText(firstPresent(payload?.inspection?.noGoReason, payload.noGoReason)),
    noGoReasons: Array.isArray(firstPresent(payload?.inspection?.noGoReasons, inspectionReport.noGoReasons))
      ? firstPresent(payload?.inspection?.noGoReasons, inspectionReport.noGoReasons)
          .map((entry) => normalizeText(entry))
          .filter(Boolean)
      : [],
    remarks: normalizeText(firstPresent(payload?.inspection?.remarks, payload.inspectionRemarks)),
    startedAt: parseDate(firstPresent(payload?.inspection?.startedAt, payload.inspectionStartedAt)),
    submittedAt: parseDate(firstPresent(payload?.inspection?.submittedAt, payload.inspectionSubmittedAt)),
    inspectedAt: parseDate(firstPresent(payload?.inspection?.inspectedAt, payload.inspectedAt)),
    lastOutcome: normalizeText(firstPresent(payload?.inspection?.lastOutcome, payload.inspectionLastOutcome)),
    rescheduledAt: parseDate(firstPresent(payload?.inspection?.rescheduledAt, payload.rescheduledAt)),
    rescheduleExecutiveName: normalizeText(firstPresent(payload?.inspection?.rescheduleExecutiveName, payload.rescheduleExecutiveName)),
    rescheduleExecutiveMobile: normalizeText(firstPresent(payload?.inspection?.rescheduleExecutiveMobile, payload.rescheduleExecutiveMobile)),
    reportVersion: normalizeText(firstPresent(payload?.inspection?.reportVersion, inspectionReport.reportVersion)),
    report: inspectionReport,
  };

  const activities = Array.isArray(payload.activities)
    ? payload.activities.map(buildActivity)
    : [];
  const callLogs = Array.isArray(payload.callLogs)
    ? payload.callLogs.map(buildCallLog)
    : [];
  const followUps = Array.isArray(payload.followUps)
    ? payload.followUps.map(buildFollowUp)
    : [];

  const normalized = {
    leadDate: parseDate(firstPresent(payload.leadDate, pickMapped(mapped, ["Added Date"])) ) || new Date(),
    source,
    statusDate: parseDate(firstPresent(payload.statusDate, pickMapped(mapped, ["Status Date"]))),
    statusUpdatedDate: parseDate(firstPresent(payload.statusUpdatedDate, pickMapped(mapped, ["Status Updated Date"]))),
    subStatus: normalizeText(firstPresent(payload.subStatus, pickMapped(mapped, ["Sub Status"]))),
    sourceStatus: normalizeText(firstPresent(payload.sourceStatus, pickMapped(mapped, ["Status"]))),
    executiveName: normalizeText(firstPresent(payload.executiveName, pickMapped(mapped, ["Executive Name"]))),
    externalRefs,
    seller,
    vehicle,
    pricing,
    workflow,
    assignment,
    scheduling,
    importMeta,
    latestCallSummary: normalizeText(firstPresent(payload.latestCallSummary, payload.callSummary)),
    latestDisposition: normalizeText(firstPresent(payload.latestDisposition, payload.disposition, status)),
    callLogs,
    followUps,
    activities,
    inspection,
    stageData: payload.stageData && typeof payload.stageData === "object" ? payload.stageData : {},
  };

  normalized.dedupeFingerprint = buildDedupeFingerprint(normalized);
  normalized.pricing.procurementScore = normalized.pricing.procurementScore || calculateProcurementScore(normalized);
  normalized.pricing.scoreUpdatedAt = normalized.pricing.scoreUpdatedAt || new Date();

  return normalized;
};

const appendTimelineItems = (doc, payload = {}) => {
  if (payload.activity) doc.activities.unshift(buildActivity(payload.activity));
  if (payload.callLog) doc.callLogs.unshift(buildCallLog(payload.callLog));
  if (payload.followUpEntry) doc.followUps.unshift(buildFollowUp(payload.followUpEntry));
};

const applyLeadUpdate = (doc, payload = {}) => {
  const normalized = normalizeLeadPayload(payload);

  if (hasAnyPath(payload, ["leadDate"])) {
    doc.leadDate = normalized.leadDate || doc.leadDate;
  }
  if (hasAnyPath(payload, ["source"])) {
    doc.source = normalized.source || doc.source;
  }
  if (hasAnyPath(payload, ["statusDate"])) {
    doc.statusDate = normalized.statusDate ?? doc.statusDate;
  }
  if (hasAnyPath(payload, ["statusUpdatedDate"])) {
    doc.statusUpdatedDate = normalized.statusUpdatedDate ?? doc.statusUpdatedDate;
  }
  if (hasAnyPath(payload, ["subStatus"])) {
    doc.subStatus = normalized.subStatus;
  }
  if (hasAnyPath(payload, ["sourceStatus", "status"])) {
    doc.sourceStatus = normalized.sourceStatus;
  }
  if (hasAnyPath(payload, ["executiveName"])) {
    doc.executiveName = normalized.executiveName || doc.executiveName;
  }

  if (
    hasAnyPath(payload, [
      "externalRefs",
      "ctiListingId",
      "cwListingId",
      "pgClQleadId",
      "dealerId",
      "c2bLeadId",
      "leadId",
      "sourceLeadId",
      "source",
    ])
  ) {
    doc.externalRefs = {
      ...doc.externalRefs?.toObject?.(),
      ...normalized.externalRefs,
    };
  }

  if (
    hasAnyPath(payload, [
      "seller",
      "name",
      "mobile",
      "alternateMobile",
      "email",
      "address",
      "area",
      "pincode",
      "pincodeCity",
      "city",
      "state",
    ])
  ) {
    doc.seller = { ...doc.seller?.toObject?.(), ...normalized.seller };
  }

  if (
    hasAnyPath(payload, [
      "vehicle",
      "make",
      "model",
      "variant",
      "mfgYear",
      "mfgMonth",
      "color",
      "mileage",
      "milage",
      "fuel",
      "regNo",
      "ownership",
      "insurance",
      "insuranceCategory",
      "insuranceExpiry",
      "hypothecation",
      "bankName",
      "accidentPaintHistory",
      "accidentPaintNotes",
    ])
  ) {
    doc.vehicle = { ...doc.vehicle?.toObject?.(), ...normalized.vehicle };
  }

  if (
    hasAnyPath(payload, [
      "pricing",
      "expectedPrice",
      "updatedExpectedPrice",
      "procurementScore",
      "scoreUpdatedAt",
    ])
  ) {
    doc.pricing = { ...doc.pricing?.toObject?.(), ...normalized.pricing };
  }

  if (
    hasAnyPath(payload, [
      "workflow",
      "status",
      "currentStage",
      "pipelineStage",
      "isClosed",
      "closureReason",
      "closureNotes",
      "closedAt",
      "notes",
    ])
  ) {
    doc.workflow = { ...doc.workflow?.toObject?.(), ...normalized.workflow };
  }

  if (
    hasAnyPath(payload, [
      "assignment",
      "assignedTo",
      "assignedAt",
      "assignmentRule",
      "assignmentNotes",
    ])
  ) {
    doc.assignment = {
      ...doc.assignment?.toObject?.(),
      ...normalized.assignment,
    };
  }

  if (
    hasAnyPath(payload, [
      "scheduling",
      "nextFollowUp",
      "nextFollowUpAt",
      "inspectionScheduledAt",
      "inspectionExecutiveName",
      "inspectionExecutiveMobile",
    ])
  ) {
    doc.scheduling = {
      ...doc.scheduling?.toObject?.(),
      ...normalized.scheduling,
    };
  }

  if (
    hasAnyPath(payload, [
      "importMeta",
      "recordSource",
      "importedAt",
      "importBatchId",
      "importFileName",
      "importedByName",
      "rawRow",
    ])
  ) {
    doc.importMeta = {
      ...doc.importMeta?.toObject?.(),
      ...normalized.importMeta,
    };
  }

  if (hasAnyPath(payload, ["latestCallSummary", "callSummary"])) {
    doc.latestCallSummary = normalized.latestCallSummary || doc.latestCallSummary;
  }
  if (hasAnyPath(payload, ["latestDisposition", "disposition", "status"])) {
    doc.latestDisposition = normalized.latestDisposition || doc.latestDisposition;
  }

  if (
    hasAnyPath(payload, [
      "inspection",
      "inspectionId",
      "inspectionExecutiveName",
      "inspectionExecutiveMobile",
      "inspectionConducted",
      "inspectionVerdict",
      "noGoReason",
      "inspectionRemarks",
      "inspectionStartedAt",
      "inspectionSubmittedAt",
      "inspection.report",
    ])
  ) {
    doc.inspection = {
      ...doc.inspection?.toObject?.(),
      ...normalized.inspection,
    };
  }

  if (hasAnyPath(payload, ["stageData"])) {
    doc.stageData = { ...(doc.stageData || {}), ...(normalized.stageData || {}) };
  }

  if (normalized.dedupeFingerprint) {
    doc.dedupeFingerprint = normalized.dedupeFingerprint;
  }

  if (Array.isArray(payload.activities)) doc.activities = normalized.activities;
  if (Array.isArray(payload.callLogs)) doc.callLogs = normalized.callLogs;
  if (Array.isArray(payload.followUps)) doc.followUps = normalized.followUps;

  appendTimelineItems(doc, payload);

  if (doc.workflow?.status === "Closed") {
    doc.workflow.isClosed = true;
    doc.workflow.currentStage = "closed";
    doc.workflow.closedAt = doc.workflow.closedAt || new Date();
  }
};

const resolveLead = async (raw = "") => {
  const value = normalizeText(raw);
  if (!value) return null;
  const byObjectId = mongoose.Types.ObjectId.isValid(value)
    ? await UsedCarLead.findById(value)
    : null;
  return byObjectId || (await UsedCarLead.findOne({ internalLeadId: value }));
};

export const listUsedCarLeads = asyncHandler(async (req, res) => {
  const limit = Math.min(5000, Math.max(1, Number(req.query.limit || 250)));
  const skip = Math.max(0, Number(req.query.skip || 0));

  const filter = {};
  const q = normalizeText(req.query.q);
  const status = normalizeText(req.query.status);
  const currentStage = normalizeText(req.query.currentStage);
  const assignedTo = normalizeText(req.query.assignedTo);
  const source = normalizeText(req.query.source);
  const make = normalizeText(req.query.make);
  const fuel = normalizeText(req.query.fuel);
  const includeClosed = normalizeText(req.query.includeClosed).toLowerCase() === "true";
  const leadDateFrom = parseDate(req.query.leadDateFrom);
  const leadDateTo = parseDate(req.query.leadDateTo);

  if (!includeClosed) filter["workflow.isClosed"] = { $ne: true };
  if (status) filter["workflow.status"] = status;
  if (currentStage) filter["workflow.currentStage"] = currentStage;
  if (assignedTo) filter["assignment.assignedTo"] = assignedTo;
  if (source) filter.source = source;
  if (make) filter["vehicle.make"] = make;
  if (fuel) filter["vehicle.fuel"] = fuel;
  if (leadDateFrom || leadDateTo) {
    filter.leadDate = {};
    if (leadDateFrom) filter.leadDate.$gte = leadDateFrom;
    if (leadDateTo) filter.leadDate.$lte = leadDateTo;
  }
  if (q) {
    filter.$or = [
      { internalLeadId: new RegExp(escapeRegex(q), "i") },
      { "externalRefs.c2bLeadId": new RegExp(escapeRegex(q), "i") },
      { "seller.name": new RegExp(escapeRegex(q), "i") },
      { "seller.mobile": new RegExp(escapeRegex(q), "i") },
      { "vehicle.make": new RegExp(escapeRegex(q), "i") },
      { "vehicle.model": new RegExp(escapeRegex(q), "i") },
      { "vehicle.variant": new RegExp(escapeRegex(q), "i") },
      { "vehicle.regNo": new RegExp(escapeRegex(q), "i") },
      { source: new RegExp(escapeRegex(q), "i") },
    ];
  }

  const count = await UsedCarLead.countDocuments(filter);
  const rows = await UsedCarLead.find(filter)
    .sort({ updatedAt: -1, _id: -1 })
    .skip(skip)
    .limit(limit)
    .lean();

  res.json({ success: true, count, data: rows });
});

export const getUsedCarLeadById = asyncHandler(async (req, res) => {
  const doc = await resolveLead(req.params.id);
  if (!doc) {
    res.status(404);
    throw new Error("Used car lead not found");
  }
  res.json({ success: true, data: doc });
});

export const createUsedCarLead = asyncHandler(async (req, res) => {
  const payload = normalizeLeadPayload(req.body || {});
  if (!payload.seller?.name || !payload.seller?.mobile) {
    res.status(400);
    throw new Error("Seller name and mobile are required");
  }

  const sourceLeadKey = payload.externalRefs?.sourceLeadKey;
  const dedupeFingerprint = payload.dedupeFingerprint;

  const existing =
    (sourceLeadKey
      ? await UsedCarLead.findOne({ "externalRefs.sourceLeadKey": sourceLeadKey })
      : null) ||
    (dedupeFingerprint
      ? await UsedCarLead.findOne({ dedupeFingerprint })
      : null);

  if (existing) {
    res.status(409);
    throw new Error(`Lead already exists as ${existing.internalLeadId}`);
  }

  const internalLeadId = await getNextInternalLeadId();
  const doc = await UsedCarLead.create({
    ...payload,
    internalLeadId,
  });

  res.status(201).json({ success: true, data: doc });
});

export const importUsedCarLeads = asyncHandler(async (req, res) => {
  const leads = Array.isArray(req.body?.leads) ? req.body.leads : [];
  const importFileName = normalizeText(req.body?.importFileName);
  const importBatchId = normalizeText(req.body?.importBatchId) || `BATCH-${Date.now()}`;
  const importedByName = normalizeText(req.body?.importedByName);

  if (!leads.length) {
    res.status(400);
    throw new Error("No leads supplied for import");
  }

  const normalizedRows = leads
    .map((row) =>
      normalizeLeadPayload({
        ...row,
        importMeta: {
          ...(row.importMeta || {}),
          importFileName,
          importBatchId,
          importedByName,
          importedAt: new Date(),
          recordSource: "excel-import",
          rawRow: row.rawRow || row,
        },
      }),
    )
    .filter((row) => row.seller?.name && row.seller?.mobile);

  const sourceKeys = normalizedRows
    .map((row) => row.externalRefs?.sourceLeadKey)
    .filter(Boolean);
  const fingerprints = normalizedRows
    .map((row) => row.dedupeFingerprint)
    .filter(Boolean);

  const existingDocs = await UsedCarLead.find({
    $or: [
      ...(sourceKeys.length
        ? [{ "externalRefs.sourceLeadKey": { $in: sourceKeys } }]
        : []),
      ...(fingerprints.length
        ? [{ dedupeFingerprint: { $in: fingerprints } }]
        : []),
    ],
  }).select("internalLeadId externalRefs.sourceLeadKey dedupeFingerprint seller.name seller.mobile");

  const existingSourceKeys = new Set(
    existingDocs.map((doc) => safeString(doc.externalRefs?.sourceLeadKey)).filter(Boolean),
  );
  const existingFingerprints = new Set(
    existingDocs.map((doc) => safeString(doc.dedupeFingerprint)).filter(Boolean),
  );

  const candidates = [];
  const skipped = [];
  const stagedSourceKeys = new Set();
  const stagedFingerprints = new Set();

  for (const row of normalizedRows) {
    const sourceLeadKey = safeString(row.externalRefs?.sourceLeadKey);
    const fingerprint = safeString(row.dedupeFingerprint);
    const duplicate =
      (sourceLeadKey && (existingSourceKeys.has(sourceLeadKey) || stagedSourceKeys.has(sourceLeadKey))) ||
      (fingerprint && (existingFingerprints.has(fingerprint) || stagedFingerprints.has(fingerprint)));

    if (duplicate) {
      skipped.push({
        sellerName: row.seller?.name,
        mobile: row.seller?.mobile,
        sourceLeadKey,
        dedupeFingerprint: fingerprint,
      });
      continue;
    }

    candidates.push(row);
    if (sourceLeadKey) stagedSourceKeys.add(sourceLeadKey);
    if (fingerprint) stagedFingerprints.add(fingerprint);
  }

  const reservedIds = await reserveInternalLeadIds(candidates.length);
  const toInsert = candidates.map((row, index) => ({
    ...row,
    internalLeadId: reservedIds[index],
  }));

  const created = toInsert.length
    ? await UsedCarLead.insertMany(toInsert, { ordered: false })
    : [];

  res.status(201).json({
    success: true,
    summary: {
      received: leads.length,
      normalized: normalizedRows.length,
      imported: created.length,
      skipped: skipped.length,
      importBatchId,
    },
    data: created,
    skipped,
  });
});

export const updateUsedCarLead = asyncHandler(async (req, res) => {
  const doc = await resolveLead(req.params.id);
  if (!doc) {
    res.status(404);
    throw new Error("Used car lead not found");
  }

  applyLeadUpdate(doc, req.body || {});
  await doc.save();

  res.json({ success: true, data: doc });
});

export const patchUsedCarLeadWorkflow = asyncHandler(async (req, res) => {
  const doc = await resolveLead(req.params.id);
  if (!doc) {
    res.status(404);
    throw new Error("Used car lead not found");
  }

  applyLeadUpdate(doc, {
    workflow: req.body?.workflow || {},
    assignment: req.body?.assignment || {},
    scheduling: req.body?.scheduling || {},
    activity: req.body?.activity,
    followUpEntry: req.body?.followUpEntry,
    callLog: req.body?.callLog,
  });

  await doc.save();
  res.json({ success: true, data: doc });
});

export const bulkAssignUsedCarLeads = asyncHandler(async (req, res) => {
  const from = parseDate(req.body?.from);
  const to = parseDate(req.body?.to);
  const oddAssignee = normalizeText(req.body?.oddAssignee);
  const evenAssignee = normalizeText(req.body?.evenAssignee);
  const onlyUnassigned = Boolean(req.body?.onlyUnassigned);

  if (!from || !to || !oddAssignee || !evenAssignee) {
    res.status(400);
    throw new Error("from, to, oddAssignee, and evenAssignee are required");
  }

  const filter = {
    leadDate: { $gte: from, $lte: to },
    "workflow.isClosed": { $ne: true },
  };
  if (onlyUnassigned) {
    filter.$or = [
      { "assignment.assignedTo": { $exists: false } },
      { "assignment.assignedTo": "" },
    ];
  }

  const leads = await UsedCarLead.find(filter);
  let updated = 0;

  for (const lead of leads) {
    const day = new Date(lead.leadDate || Date.now()).getDate();
    const assignee = day % 2 === 0 ? evenAssignee : oddAssignee;
    lead.assignment = {
      ...lead.assignment?.toObject?.(),
      assignedTo: assignee,
      assignedAt: new Date(),
      assignmentRule: "odd-even-date",
    };
    lead.activities.unshift(
      buildActivity({
        type: "assignment",
        title: "Lead assigned",
        detail: `${assignee} via odd/even date rule`,
      }),
    );
    await lead.save();
    updated += 1;
  }

  res.json({ success: true, updated });
});

export const clearUsedCarLeads = asyncHandler(async (req, res) => {
  const all = normalizeText(req.body?.all).toLowerCase() === "true" || req.body?.all === true;
  const leadIds = Array.isArray(req.body?.leadIds)
    ? req.body.leadIds.map((value) => normalizeText(value)).filter(Boolean)
    : [];

  if (!all && !leadIds.length) {
    res.status(400);
    throw new Error("Set all=true or provide leadIds to clear leads");
  }

  const objectIds = leadIds
    .filter((value) => mongoose.Types.ObjectId.isValid(value))
    .map((value) => new mongoose.Types.ObjectId(value));

  const filter = all
    ? {}
    : {
        $or: [
          ...(objectIds.length ? [{ _id: { $in: objectIds } }] : []),
          { internalLeadId: { $in: leadIds } },
        ],
      };

  const result = await UsedCarLead.deleteMany(filter);
  res.json({ success: true, deletedCount: result.deletedCount || 0 });
});

export const deleteUsedCarLead = asyncHandler(async (req, res) => {
  const doc = await resolveLead(req.params.id);
  if (!doc) {
    res.status(404);
    throw new Error("Used car lead not found");
  }

  await doc.deleteOne();
  res.json({ success: true, message: "Used car lead deleted" });
});
