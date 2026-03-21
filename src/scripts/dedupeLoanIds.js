import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import Counter from "../models/Counter.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";
import VehicleRecord from "../models/VehicleRecord.js";

dotenv.config();

const APPLY_MODE = process.argv.includes("--apply");
const LOAN_ID_PREFIX = "LN";
const LOAN_ID_COUNTER_PREFIX = "loan_id_sequence";

const parseLoanIdParts = (value) => {
  const match = String(value || "")
    .trim()
    .match(/^([A-Z]+)-(\d{4})-(\d+)$/i);
  if (!match) return null;
  return {
    prefix: String(match[1] || "").toUpperCase(),
    year: Number(match[2]),
    sequence: Number(match[3]),
  };
};

const buildLoanId = (year, sequence) =>
  `${LOAN_ID_PREFIX}-${year}-${String(sequence).padStart(4, "0")}`;

const getLoanCounterKey = (year) => `${LOAN_ID_COUNTER_PREFIX}_${year}`;

const toEpoch = (value) => {
  if (!value) return Number.MAX_SAFE_INTEGER;
  const date = value instanceof Date ? value : new Date(value);
  const epoch = Number(date.getTime());
  return Number.isFinite(epoch) ? epoch : Number.MAX_SAFE_INTEGER;
};

const sortDuplicateDocs = (docs = []) =>
  [...docs].sort((a, b) => {
    const createdDiff = toEpoch(a?.createdAt) - toEpoch(b?.createdAt);
    if (createdDiff !== 0) return createdDiff;
    const updatedDiff = toEpoch(a?.updatedAt) - toEpoch(b?.updatedAt);
    if (updatedDiff !== 0) return updatedDiff;
    return String(a?._id || "").localeCompare(String(b?._id || ""));
  });

const getMaxLoanSequenceFromLoans = async (year) => {
  const regex = new RegExp(`^${LOAN_ID_PREFIX}-${year}-\\d+$`, "i");
  const cursor = Loan.find({ loanId: { $regex: regex } })
    .select("loanId")
    .lean()
    .cursor();

  let max = 0;
  for await (const doc of cursor) {
    const parsed = parseLoanIdParts(doc?.loanId);
    if (!parsed || parsed.year !== Number(year)) continue;
    if (parsed.sequence > max) max = parsed.sequence;
  }
  return max;
};

const reserveNextLoanIdForYear = async (yearInput) => {
  const year = Number(yearInput) || new Date().getFullYear();
  const key = getLoanCounterKey(year);

  const bumpedExisting = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    { returnDocument: "after", lean: true },
  );
  if (bumpedExisting?.value) return buildLoanId(year, bumpedExisting.value);

  const maxFromLoans = await getMaxLoanSequenceFromLoans(year);
  const seed = Math.max(maxFromLoans, 0);

  try {
    await Counter.create({ key, value: seed });
  } catch (error) {
    if (error?.code !== 11000) throw error;
  }

  const bumped = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    {
      returnDocument: "after",
      upsert: true,
      setDefaultsOnInsert: true,
      lean: true,
    },
  );

  return buildLoanId(year, bumped?.value || 1);
};

const countDistinctLinkedLoanIds = async (Model, loanIds = []) => {
  if (!loanIds.length) return 0;
  const result = await Model.aggregate([
    { $match: { loanId: { $in: loanIds } } },
    { $group: { _id: "$loanId" } },
    { $count: "count" },
  ]);
  return Number(result?.[0]?.count || 0);
};

const run = async () => {
  try {
    await connectDB();
    console.log(
      `Starting loanId de-duplication in ${APPLY_MODE ? "APPLY" : "DRY-RUN"} mode...`,
    );

    const duplicateGroups = await Loan.aggregate([
      { $match: { loanId: { $type: "string", $ne: "" } } },
      {
        $group: {
          _id: "$loanId",
          count: { $sum: 1 },
          docs: {
            $push: {
              _id: "$_id",
              createdAt: "$createdAt",
              updatedAt: "$updatedAt",
              customerName: "$customerName",
              primaryMobile: "$primaryMobile",
              panNumber: "$panNumber",
              registrationNumber: "$registrationNumber",
            },
          },
        },
      },
      { $match: { count: { $gt: 1 } } },
      { $sort: { count: -1, _id: 1 } },
    ]).allowDiskUse(true);

    if (!duplicateGroups.length) {
      console.log("No duplicate loanId rows found.");
      return;
    }

    const duplicateRows = duplicateGroups.reduce((acc, row) => acc + row.count, 0);
    const rowsToRelabel = duplicateRows - duplicateGroups.length;
    console.log(`Duplicate loanIds: ${duplicateGroups.length}`);
    console.log(`Rows participating in duplicates: ${duplicateRows}`);
    console.log(`Rows that will be assigned new loanIds: ${rowsToRelabel}`);

    const sampleIds = duplicateGroups
      .slice(0, 10)
      .map((g) => `${g._id} (x${g.count})`)
      .join(", ");
    console.log(`Sample duplicate ids: ${sampleIds}`);

    const duplicateLoanIds = duplicateGroups
      .map((group) => String(group._id || "").trim())
      .filter(Boolean);

    const [deliveryOrderGroups, paymentGroups, vehicleRecordGroups] =
      await Promise.all([
        countDistinctLinkedLoanIds(DeliveryOrder, duplicateLoanIds),
        countDistinctLinkedLoanIds(Payment, duplicateLoanIds),
        countDistinctLinkedLoanIds(VehicleRecord, duplicateLoanIds),
      ]);

    console.log(
      `Duplicate groups with linked records -> DO: ${deliveryOrderGroups}, Payment: ${paymentGroups}, VehicleRecord: ${vehicleRecordGroups}`,
    );

    if (!APPLY_MODE) {
      console.log("Dry run complete. Re-run with --apply to perform updates.");
      return;
    }

    let relabeledRows = 0;
    const mappingSample = [];

    for (const group of duplicateGroups) {
      const oldLoanId = String(group._id || "").trim();
      if (!oldLoanId) continue;

      const docs = sortDuplicateDocs(group.docs || []);
      const duplicates = docs.slice(1); // Keep the oldest as canonical.
      const parsed = parseLoanIdParts(oldLoanId);
      const targetYear = parsed?.year || new Date().getFullYear();

      for (const doc of duplicates) {
        const newLoanId = await reserveNextLoanIdForYear(targetYear);
        const result = await Loan.updateOne(
          { _id: doc._id, loanId: oldLoanId },
          {
            $set: {
              loanId: newLoanId,
              loan_number: newLoanId,
            },
          },
        );

        if (result.modifiedCount === 1) {
          relabeledRows += 1;
          if (mappingSample.length < 25) {
            mappingSample.push({
              _id: String(doc._id),
              from: oldLoanId,
              to: newLoanId,
            });
          }
        } else {
          console.warn(
            `Skipped row ${String(doc._id)} for ${oldLoanId} (it may have already changed).`,
          );
        }
      }
    }

    console.log(`Relabeled duplicate rows: ${relabeledRows}`);
    if (mappingSample.length > 0) {
      console.log("Sample remap:");
      mappingSample.forEach((row) =>
        console.log(`- ${row._id}: ${row.from} -> ${row.to}`),
      );
    }
    console.log("Done. Next step: run node src/scripts/ensureLoanIndexes.js");
  } catch (error) {
    console.error("LoanId de-duplication failed:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
