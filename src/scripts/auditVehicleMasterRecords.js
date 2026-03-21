import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import VehicleRecord from "../models/VehicleRecord.js";

dotenv.config();

const normalizeReg = (value) =>
  String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

const firstValue = (...values) =>
  values.find(
    (value) =>
      value !== undefined && value !== null && String(value).trim() !== "",
  );

const summarizeLoanVehicleIdentity = (loan = {}) => {
  const reg = normalizeReg(
    firstValue(
      loan.registrationNumber,
      loan.rc_redg_no,
      loan.vehicleRegNo,
      loan.vehicleRegdNumber,
    ),
  );

  const make = String(loan.vehicleMake || "").trim();
  const model = String(loan.vehicleModel || "").trim();
  const variant = String(loan.vehicleVariant || "").trim();
  const engine = String(firstValue(loan.engineNumber, loan.rc_engine_no) || "").trim();
  const chassis = String(firstValue(loan.chassisNumber, loan.rc_chassis_no) || "").trim();

  const hasCoreIdentity = Boolean(
    reg || make || model || variant || engine || chassis,
  );

  return { reg, make, model, variant, engine, chassis, hasCoreIdentity };
};

const runAudit = async () => {
  await connectDB();

  const loans = await Loan.find(
    {},
    {
      loanId: 1,
      customerName: 1,
      vehicleMake: 1,
      vehicleModel: 1,
      vehicleVariant: 1,
      registrationNumber: 1,
      rc_redg_no: 1,
      vehicleRegNo: 1,
      vehicleRegdNumber: 1,
      engineNumber: 1,
      rc_engine_no: 1,
      chassisNumber: 1,
      rc_chassis_no: 1,
      updatedAt: 1,
      createdAt: 1,
    },
  ).lean();

  const records = await VehicleRecord.find(
    {},
    {
      loanId: 1,
      registrationNumberNormalized: 1,
      registrationNumber: 1,
      make: 1,
      model: 1,
      variant: 1,
      updatedAt: 1,
      createdAt: 1,
    },
  ).lean();

  const recordByLoanId = new Map();
  const recordByReg = new Map();

  for (const record of records) {
    const loanId = String(record.loanId || "").trim();
    if (loanId) recordByLoanId.set(loanId, record);

    const reg = normalizeReg(
      firstValue(record.registrationNumberNormalized, record.registrationNumber),
    );
    if (reg) recordByReg.set(reg, record);
  }

  let loansWithLoanId = 0;
  let loansWithRegNo = 0;
  let loansWithCoreVehicleIdentity = 0;
  let matchedByLoanId = 0;
  let matchedByRegNo = 0;
  let missingWithoutLoanOrRegMatch = 0;
  let missingNoCoreVehicleIdentity = 0;
  let missingWithoutRegNo = 0;

  const regToLoanIds = new Map();
  const sampleMissing = [];
  const sampleNoCore = [];

  for (const loan of loans) {
    const loanId = String(loan.loanId || "").trim();
    if (loanId) loansWithLoanId += 1;

    const identity = summarizeLoanVehicleIdentity(loan);
    if (identity.hasCoreIdentity) loansWithCoreVehicleIdentity += 1;
    if (identity.reg) {
      loansWithRegNo += 1;
      if (!regToLoanIds.has(identity.reg)) regToLoanIds.set(identity.reg, []);
      regToLoanIds.get(identity.reg).push(loanId || "(no-loanId)");
    }

    const hasLoanMatch = loanId ? recordByLoanId.has(loanId) : false;
    const hasRegMatch = identity.reg ? recordByReg.has(identity.reg) : false;

    if (hasLoanMatch) matchedByLoanId += 1;
    if (hasRegMatch) matchedByRegNo += 1;

    if (!hasLoanMatch && !hasRegMatch) {
      if (!identity.hasCoreIdentity) {
        missingNoCoreVehicleIdentity += 1;
        if (sampleNoCore.length < 20) {
          sampleNoCore.push({
            loanId,
            customerName: String(loan.customerName || "").trim(),
            createdAt: loan.createdAt,
          });
        }
        continue;
      }

      missingWithoutLoanOrRegMatch += 1;
      if (!identity.reg) missingWithoutRegNo += 1;
      if (sampleMissing.length < 40) {
        sampleMissing.push({
          loanId,
          customerName: String(loan.customerName || "").trim(),
          reg: identity.reg,
          make: identity.make,
          model: identity.model,
          variant: identity.variant,
          hasEngine: Boolean(identity.engine),
          hasChassis: Boolean(identity.chassis),
          updatedAt: loan.updatedAt,
        });
      }
    }
  }

  let multiLoanSameRegCount = 0;
  let multiLoanSameRegLoans = 0;
  const sampleSameRegMultipleLoans = [];
  for (const [reg, loanIds] of regToLoanIds.entries()) {
    if (loanIds.length > 1) {
      multiLoanSameRegCount += 1;
      multiLoanSameRegLoans += loanIds.length;
      if (sampleSameRegMultipleLoans.length < 20) {
        sampleSameRegMultipleLoans.push({
          reg,
          totalLoans: loanIds.length,
          loanIds: loanIds.slice(0, 8),
        });
      }
    }
  }

  const summary = {
    loansTotal: loans.length,
    vehicleMasterRecordsTotal: records.length,
    loansWithLoanId,
    loansWithCoreVehicleIdentity,
    loansWithRegNo,
    matchedByLoanId,
    matchedByRegNo,
    missingWithoutLoanOrRegMatch,
    missingNoCoreVehicleIdentity,
    missingWithoutRegNo,
    distinctLoanRegCount: regToLoanIds.size,
    multiLoanSameRegCount,
    multiLoanSameRegLoans,
    sampleMissing,
    sampleNoCore,
    sampleSameRegMultipleLoans,
  };

  const loanDistinctLoanIds = await Loan.distinct("loanId", {
    loanId: { $exists: true, $ne: null },
  });
  const recordDistinctLoanIds = await VehicleRecord.distinct("loanId", {
    loanId: { $exists: true, $ne: null },
  });
  const duplicateLoanIdsInLoans = await Loan.aggregate([
    { $match: { loanId: { $exists: true, $ne: null } } },
    { $group: { _id: "$loanId", count: { $sum: 1 } } },
    { $match: { count: { $gt: 1 } } },
    { $sort: { count: -1, _id: 1 } },
    { $limit: 30 },
  ]);

  summary.loanDistinctLoanIdCount = loanDistinctLoanIds.length;
  summary.recordDistinctLoanIdCount = recordDistinctLoanIds.length;
  summary.duplicateLoanIdRowsInLoans = duplicateLoanIdsInLoans.length;
  summary.sampleDuplicateLoanIdsInLoans = duplicateLoanIdsInLoans;

  console.log(JSON.stringify(summary, null, 2));
};

runAudit()
  .catch((error) => {
    console.error("auditVehicleMasterRecords failed:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await mongoose.connection.close();
    } catch {
      // noop
    }
  });
