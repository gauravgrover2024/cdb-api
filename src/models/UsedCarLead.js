import mongoose from "mongoose";

const activitySchema = new mongoose.Schema(
  {
    activityId: { type: String, trim: true },
    type: { type: String, trim: true, default: "note" },
    title: { type: String, trim: true, default: "Activity" },
    detail: { type: String, trim: true, default: "" },
    at: { type: Date, default: Date.now },
    actorId: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    actorName: { type: String, trim: true, default: "" },
    meta: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { _id: false },
);

const callLogSchema = new mongoose.Schema(
  {
    logId: { type: String, trim: true },
    at: { type: Date, default: Date.now },
    status: { type: String, trim: true, default: "" },
    outcome: { type: String, trim: true, default: "" },
    notes: { type: String, trim: true, default: "" },
    durationSeconds: { type: Number, default: 0 },
    nextFollowUpAt: { type: Date, default: null },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    createdByName: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const followUpSchema = new mongoose.Schema(
  {
    followUpId: { type: String, trim: true },
    dueAt: { type: Date, default: null },
    status: { type: String, trim: true, default: "Scheduled" },
    notes: { type: String, trim: true, default: "" },
    createdAt: { type: Date, default: Date.now },
    completedAt: { type: Date, default: null },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    createdByName: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const externalRefsSchema = new mongoose.Schema(
  {
    c2bLeadId: { type: String, trim: true, default: "" },
    ctiListingId: { type: String, trim: true, default: "" },
    cwListingId: { type: String, trim: true, default: "" },
    pgClQleadId: { type: String, trim: true, default: "" },
    dealerId: { type: String, trim: true, default: "" },
    sourceLeadId: { type: String, trim: true, default: "" },
    sourceLeadKey: { type: String, trim: true },
  },
  { _id: false },
);

const sellerSchema = new mongoose.Schema(
  {
    name: { type: String, trim: true, required: true },
    mobile: { type: String, trim: true, required: true },
    alternateMobile: { type: String, trim: true, default: "" },
    email: { type: String, trim: true, lowercase: true, default: "" },
    address: { type: String, trim: true, default: "" },
    area: { type: String, trim: true, default: "" },
    pincode: { type: String, trim: true, default: "" },
    pincodeCity: { type: String, trim: true, default: "" },
    city: { type: String, trim: true, default: "" },
    state: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const vehicleSchema = new mongoose.Schema(
  {
    make: { type: String, trim: true, default: "" },
    model: { type: String, trim: true, default: "" },
    variant: { type: String, trim: true, default: "" },
    mfgYear: { type: String, trim: true, default: "" },
    mfgMonth: { type: String, trim: true, default: "" },
    color: { type: String, trim: true, default: "" },
    mileage: { type: Number, default: null },
    fuel: { type: String, trim: true, default: "" },
    regNo: { type: String, trim: true, uppercase: true, default: "" },
    ownership: { type: String, trim: true, default: "" },
    insurance: { type: String, trim: true, default: "" },
    insuranceCategory: { type: String, trim: true, default: "" },
    insuranceExpiry: { type: Date, default: null },
    hypothecation: { type: Boolean, default: null },
    bankName: { type: String, trim: true, default: "" },
    accidentPaintHistory: { type: Boolean, default: null },
    accidentPaintNotes: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const pricingSchema = new mongoose.Schema(
  {
    expectedPrice: { type: Number, default: 0 },
    updatedExpectedPrice: { type: Number, default: null },
    procurementScore: { type: Number, default: 0 },
    scoreUpdatedAt: { type: Date, default: null },
  },
  { _id: false },
);

const workflowSchema = new mongoose.Schema(
  {
    currentStage: {
      type: String,
      enum: [
        "lead-intake",
        "inspection",
        "background-check",
        "negotiation",
        "documentation",
        "procurement",
        "stock",
        "closed",
      ],
      default: "lead-intake",
      index: true,
    },
    pipelineStage: { type: String, trim: true, default: "Lead Intake" },
    status: {
      type: String,
      enum: [
        "New",
        "Not Answered",
        "Connected",
        "Callback Scheduled",
        "Qualified",
        "Inspection Scheduled",
        "Inspection Passed",
        "Inspection Done",
        "Background Check",
        "Negotiation",
        "Documentation",
        "Procurement",
        "Stock",
        "Closed",
      ],
      default: "New",
      index: true,
    },
    isClosed: { type: Boolean, default: false, index: true },
    closureReason: { type: String, trim: true, default: "" },
    closureNotes: { type: String, trim: true, default: "" },
    closedAt: { type: Date, default: null },
    notes: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const assignmentSchema = new mongoose.Schema(
  {
    assignedTo: { type: String, trim: true, default: "" },
    assignedAt: { type: Date, default: null },
    assignmentRule: { type: String, trim: true, default: "" },
    assignmentNotes: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const schedulingSchema = new mongoose.Schema(
  {
    nextFollowUpAt: { type: Date, default: null, index: true },
    inspectionScheduledAt: { type: Date, default: null, index: true },
    inspectionExecutiveName: { type: String, trim: true, default: "" },
    inspectionExecutiveMobile: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const importMetaSchema = new mongoose.Schema(
  {
    recordSource: {
      type: String,
      enum: ["manual", "excel-import", "api-import"],
      default: "manual",
    },
    importedAt: { type: Date, default: null },
    importBatchId: { type: String, trim: true, default: "" },
    importFileName: { type: String, trim: true, default: "" },
    importedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    importedByName: { type: String, trim: true, default: "" },
    rawRow: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { _id: false },
);

const inspectionFileSchema = new mongoose.Schema(
  {
    uid: { type: String, trim: true, default: "" },
    name: { type: String, trim: true, default: "" },
    url: { type: String, trim: true, default: "" },
    thumbUrl: { type: String, trim: true, default: "" },
    preview: { type: String, trim: true, default: "" },
    evidenceTag: { type: String, trim: true, default: "" },
    customTagName: { type: String, trim: true, default: "" },
    publicId: { type: String, trim: true, default: "" },
    format: { type: String, trim: true, default: "" },
    size: { type: Number, default: 0 },
    uploadedAt: { type: Date, default: Date.now },
    source: { type: String, trim: true, default: "r2" },
  },
  { _id: false },
);

const inspectionItemSchema = new mongoose.Schema(
  {
    status: { type: [String], default: [] },
    severity: { type: String, trim: true, default: "" },
    photos: { type: [inspectionFileSchema], default: [] },
    treadDepth: { type: Number, default: null },
    tyreBrand: { type: String, trim: true, default: "" },
  },
  { _id: false },
);

const refurbSectionSchema = new mongoose.Schema(
  {
    status: { type: String, trim: true, default: "OK" },
    cost: { type: Number, default: 0 },
    cap: { type: Number, default: 0 },
    issueCount: { type: Number, default: 0 },
    notes: { type: [String], default: [] },
    noGoReasons: { type: [String], default: [] },
  },
  { _id: false },
);

const refurbSummarySchema = new mongoose.Schema(
  {
    noGo: { type: Boolean, default: false },
    noGoReasons: { type: [String], default: [] },
    totalCost: { type: Number, default: 0 },
    suggestedBuyPrice: { type: Number, default: 0 },
    insuranceValidAndComprehensive: { type: Boolean, default: false },
    sections: {
      exterior: { type: refurbSectionSchema, default: () => ({}) },
      exteriorFitment: { type: refurbSectionSchema, default: () => ({}) },
      wheelsTyres: { type: refurbSectionSchema, default: () => ({}) },
      engineMechanical: { type: refurbSectionSchema, default: () => ({}) },
      interiorElectrical: { type: refurbSectionSchema, default: () => ({}) },
      safety: { type: refurbSectionSchema, default: () => ({}) },
      roadTest: { type: refurbSectionSchema, default: () => ({}) },
      acSystem: { type: refurbSectionSchema, default: () => ({}) },
    },
  },
  { _id: false },
);

const inspectionReportSchema = new mongoose.Schema(
  {
    reportVersion: { type: String, trim: true, default: "" },
    generatedAt: { type: Date, default: null },
    customerName: { type: String, trim: true, default: "" },
    inspectionLocation: { type: String, trim: true, default: "" },
    registrationNumber: { type: String, trim: true, uppercase: true, default: "" },
    insuranceType: { type: String, trim: true, default: "" },
    insuranceExpiry: { type: Date, default: null },
    makeConfirmation: { type: String, trim: true, default: "" },
    modelConfirmation: { type: String, trim: true, default: "" },
    variantConfirmation: { type: String, trim: true, default: "" },
    leadVerification: { type: Map, of: Boolean, default: {} },
    photoBuckets: { type: Map, of: [inspectionFileSchema], default: {} },
    bulkEvidence: { type: [inspectionFileSchema], default: [] },
    evidenceTags: { type: [String], default: [] },
    items: { type: Map, of: inspectionItemSchema, default: {} },
    airbagCount: { type: String, trim: true, default: "" },
    powerWindowCount: { type: String, trim: true, default: "" },
    seatMaterial: { type: String, trim: true, default: "" },
    estimatedRefurbCost: { type: Number, default: 0 },
    evaluatorPrice: { type: Number, default: 0 },
    suggestedBuyPrice: { type: Number, default: 0 },
    negotiationNotes: { type: String, trim: true, default: "" },
    overallRemarks: { type: String, trim: true, default: "" },
    noGoReasons: { type: [String], default: [] },
    refurb: { type: refurbSummarySchema, default: () => ({}) },
  },
  { _id: false },
);

const inspectionSchema = new mongoose.Schema(
  {
    inspectionId: { type: String, trim: true, default: "" },
    executiveName: { type: String, trim: true, default: "" },
    executiveMobile: { type: String, trim: true, default: "" },
    conducted: { type: Boolean, default: null },
    verdict: { type: String, trim: true, default: "" },
    noGoReason: { type: String, trim: true, default: "" },
    noGoReasons: { type: [String], default: [] },
    remarks: { type: String, trim: true, default: "" },
    startedAt: { type: Date, default: null },
    submittedAt: { type: Date, default: null },
    inspectedAt: { type: Date, default: null },
    lastOutcome: { type: String, trim: true, default: "" },
    rescheduledAt: { type: Date, default: null },
    rescheduleExecutiveName: { type: String, trim: true, default: "" },
    rescheduleExecutiveMobile: { type: String, trim: true, default: "" },
    reportVersion: { type: String, trim: true, default: "" },
    report: { type: inspectionReportSchema, default: () => ({}) },
  },
  { _id: false },
);

const usedCarLeadSchema = new mongoose.Schema(
  {
    internalLeadId: { type: String, required: true, unique: true, index: true },
    dedupeFingerprint: { type: String, trim: true, default: "", index: true },
    leadDate: { type: Date, default: Date.now, index: true },
    source: { type: String, trim: true, default: "", index: true },
    statusDate: { type: Date, default: null },
    statusUpdatedDate: { type: Date, default: null },
    subStatus: { type: String, trim: true, default: "" },
    sourceStatus: { type: String, trim: true, default: "" },
    executiveName: { type: String, trim: true, default: "" },

    externalRefs: { type: externalRefsSchema, default: () => ({}) },
    seller: { type: sellerSchema, required: true },
    vehicle: { type: vehicleSchema, default: () => ({}) },
    pricing: { type: pricingSchema, default: () => ({}) },
    workflow: { type: workflowSchema, default: () => ({}) },
    assignment: { type: assignmentSchema, default: () => ({}) },
    scheduling: { type: schedulingSchema, default: () => ({}) },
    importMeta: { type: importMetaSchema, default: () => ({}) },

    latestCallSummary: { type: String, trim: true, default: "" },
    latestDisposition: { type: String, trim: true, default: "" },

    callLogs: { type: [callLogSchema], default: [] },
    followUps: { type: [followUpSchema], default: [] },
    activities: { type: [activitySchema], default: [] },

    inspection: { type: inspectionSchema, default: () => ({}) },
    stageData: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  {
    timestamps: true,
    strict: true,
  },
);

usedCarLeadSchema.index({ "externalRefs.sourceLeadKey": 1 }, { unique: true, sparse: true });
usedCarLeadSchema.index({ source: 1, leadDate: -1 });
usedCarLeadSchema.index({ "seller.mobile": 1, leadDate: -1 });
usedCarLeadSchema.index({ "vehicle.regNo": 1, leadDate: -1 });
usedCarLeadSchema.index({ "assignment.assignedTo": 1, updatedAt: -1 });
usedCarLeadSchema.index({ "workflow.currentStage": 1, "workflow.status": 1, updatedAt: -1 });
usedCarLeadSchema.index({ "workflow.isClosed": 1, updatedAt: -1 });
usedCarLeadSchema.index({ "scheduling.nextFollowUpAt": 1, updatedAt: -1 });
usedCarLeadSchema.index({ "inspection.inspectionId": 1 }, { sparse: true });
usedCarLeadSchema.index({ "inspection.verdict": 1, updatedAt: -1 });
usedCarLeadSchema.index({ "inspection.inspectedAt": -1 });
usedCarLeadSchema.index(
  {
    internalLeadId: "text",
    "externalRefs.c2bLeadId": "text",
    "seller.name": "text",
    "seller.mobile": "text",
    "seller.city": "text",
    "vehicle.make": "text",
    "vehicle.model": "text",
    "vehicle.variant": "text",
    "vehicle.regNo": "text",
    source: "text",
  },
  {
    weights: {
      internalLeadId: 10,
      "externalRefs.c2bLeadId": 8,
      "seller.mobile": 8,
      "vehicle.regNo": 8,
      "seller.name": 6,
      "vehicle.make": 4,
      "vehicle.model": 4,
      "vehicle.variant": 3,
      source: 2,
    },
    name: "used_car_lead_search_index",
  },
);

const UsedCarLead = mongoose.model("UsedCarLead", usedCarLeadSchema);

export default UsedCarLead;
