import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import VehicleRecord from "../models/VehicleRecord.js";
import { buildVehicleRecordPayload } from "../services/vehicleRecordService.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");
const SAMPLE_LIMIT = 25;

const toEpoch = (value) => {
  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) return 0;
  return parsed.getTime();
};

const pickProjection = {
  loanId: 1,
  customerId: 1,
  customerName: 1,
  primaryMobile: 1,
  registrationNumber: 1,
  rc_redg_no: 1,
  vehicleRegNo: 1,
  vehicleRegdNumber: 1,
  vehicleMake: 1,
  make: 1,
  vehicleModel: 1,
  model: 1,
  vehicleVariant: 1,
  variant: 1,
  cubicCapacityCc: 1,
  cubicCapacity: 1,
  engineDisplacement: 1,
  engineNumber: 1,
  rc_engine_no: 1,
  vehicleEngineNo: 1,
  chassisNumber: 1,
  rc_chassis_no: 1,
  vehicleChassisNo: 1,
  manufactureMonth: 1,
  manufacturingMonth: 1,
  mfgMonth: 1,
  yearOfManufacture: 1,
  manufacturingYear: 1,
  yearOfReg: 1,
  rc_redg_date: 1,
  registrationDate: 1,
  regdDate: 1,
  hypothecationBank: 1,
  hypothecation: 1,
  registrationCity: 1,
  postfile_regd_city: 1,
  city: 1,
  typeOfLoan: 1,
  loanType: 1,
  caseType: 1,
  updatedAt: 1,
  createdAt: 1,
};

const run = async () => {
  await connectDB();

  const loans = await Loan.find({}, pickProjection).lean();
  const existingRecords = await VehicleRecord.find(
    {},
    { loanId: 1, registrationNumberNormalized: 1, registrationNumber: 1 },
  ).lean();

  const existingByReg = new Map();
  const existingByLoanId = new Map();
  for (const record of existingRecords) {
    const reg = String(record.registrationNumberNormalized || "").trim();
    if (reg) existingByReg.set(reg, record._id);
    const loanId = String(record.loanId || "").trim();
    if (loanId) existingByLoanId.set(loanId, record._id);
  }

  const latestPayloadByReg = new Map();
  const noRegPayloads = [];

  for (const loan of loans) {
    const payload = await buildVehicleRecordPayload(loan);
    if (!payload) continue;

    const reg = String(payload.registrationNumberNormalized || "").trim();
    if (reg) {
      const prev = latestPayloadByReg.get(reg);
      const prevTs = toEpoch(prev?.sourceLoanUpdatedAt || prev?.lastSyncedAt);
      const nextTs = toEpoch(payload.sourceLoanUpdatedAt || payload.lastSyncedAt);
      if (!prev || nextTs >= prevTs) latestPayloadByReg.set(reg, payload);
      continue;
    }

    noRegPayloads.push(payload);
  }

  let plannedRegInserts = 0;
  let plannedNoRegInserts = 0;
  let plannedLoanIdConflict = 0;
  const samplePlan = [];
  const inserts = [];

  for (const [reg, payload] of latestPayloadByReg.entries()) {
    if (existingByReg.has(reg)) continue;
    const doc = { ...payload };
    let droppedLoanId = false;
    const loanId = String(doc.loanId || "").trim();
    if (loanId && existingByLoanId.has(loanId)) {
      delete doc.loanId;
      droppedLoanId = true;
      plannedLoanIdConflict += 1;
    }
    inserts.push(doc);
    plannedRegInserts += 1;
    if (samplePlan.length < SAMPLE_LIMIT) {
      samplePlan.push({
        type: "missing-reg",
        reg,
        loanId: loanId || "",
        insertedWithLoanId: !droppedLoanId && Boolean(loanId),
      });
    }
  }

  for (const payload of noRegPayloads) {
    const loanId = String(payload.loanId || "").trim();
    if (!loanId) continue;
    if (existingByLoanId.has(loanId)) continue;
    inserts.push({ ...payload });
    plannedNoRegInserts += 1;
    if (samplePlan.length < SAMPLE_LIMIT) {
      samplePlan.push({
        type: "missing-loanId-no-reg",
        reg: "",
        loanId,
        insertedWithLoanId: true,
      });
    }
  }

  console.log("Vehicle master missing-record backfill");
  console.log("--------------------------------------");
  console.log("Mode:", APPLY ? "APPLY (INSERT)" : "DRY RUN");
  console.log("Loans scanned:", loans.length);
  console.log("Existing vehicle records:", existingRecords.length);
  console.log("Planned inserts (missing reg):", plannedRegInserts);
  console.log("Planned inserts (no-reg by loanId):", plannedNoRegInserts);
  console.log("Planned loanId conflicts (insert without loanId):", plannedLoanIdConflict);
  console.log("Total planned inserts:", inserts.length);

  if (samplePlan.length) {
    console.log("Sample planned inserts:");
    samplePlan.forEach((row) => {
      console.log(
        `- [${row.type}] reg=${row.reg || "-"} loanId=${row.loanId || "-"} keepLoanId=${row.insertedWithLoanId ? "yes" : "no"}`,
      );
    });
  }

  if (!APPLY) {
    console.log('\nDry-run complete. Re-run with "--apply" to insert missing records.');
    return;
  }

  let insertedCount = 0;
  let failedCount = 0;
  const failSamples = [];

  for (const doc of inserts) {
    try {
      await VehicleRecord.create(doc);
      insertedCount += 1;
    } catch (error) {
      failedCount += 1;
      if (failSamples.length < SAMPLE_LIMIT) {
        failSamples.push({
          loanId: String(doc.loanId || "").trim(),
          reg: String(doc.registrationNumberNormalized || "").trim(),
          error: error?.message || String(error),
        });
      }
    }
  }

  console.log("\nInsert run complete.");
  console.log("Inserted:", insertedCount);
  console.log("Failed:", failedCount);
  if (failSamples.length) {
    console.log("Sample failures:");
    failSamples.forEach((row) => {
      console.log(
        `- reg=${row.reg || "-"} loanId=${row.loanId || "-"} error=${row.error}`,
      );
    });
  }
};

run()
  .catch((error) => {
    console.error(
      "backfillMissingVehicleMasterRecords failed:",
      error?.message || error,
    );
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await mongoose.connection.close();
    } catch {
      // noop
    }
  });

