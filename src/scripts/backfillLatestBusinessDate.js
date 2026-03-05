import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";

dotenv.config();

const inferIsCashCase = (doc) => {
  const isFinancedText = String(doc?.isFinanced ?? doc?.isFinanceRequired ?? "")
    .trim()
    .toLowerCase();
  if (isFinancedText === "no" || isFinancedText === "false") return true;
  if (isFinancedText === "yes" || isFinancedText === "true") return false;
  const loanTypeText = String(
    doc?.typeOfLoan || doc?.loanType || doc?.caseType || doc?.loan_type || "",
  )
    .trim()
    .toLowerCase();
  return loanTypeText.includes("cash");
};

const pickLatestBusinessDate = (doc, isCashCase) => {
  if (isCashCase) {
    return (
      doc?.delivery_date ||
      doc?.deliveryDate ||
      doc?.delivery_done_at ||
      doc?.vehicleDeliveryDate ||
      null
    );
  }
  return (
    doc?.disbursement_date ||
    doc?.approval_disbursedDate ||
    doc?.disbursedDate ||
    null
  );
};

const run = async () => {
  await connectDB();
  const cursor = Loan.find(
    {},
    [
      "_id",
      "isFinanced",
      "isFinanceRequired",
      "typeOfLoan",
      "loanType",
      "caseType",
      "loan_type",
      "delivery_date",
      "deliveryDate",
      "delivery_done_at",
      "vehicleDeliveryDate",
      "disbursement_date",
      "approval_disbursedDate",
      "disbursedDate",
      "isCashCase",
      "latestBusinessDate",
    ].join(" "),
  )
    .lean()
    .cursor();

  let processed = 0;
  let updated = 0;
  let ops = [];
  const batchSize = 1000;

  for await (const row of cursor) {
    processed += 1;
    const isCashCase = inferIsCashCase(row);
    const latestBusinessDate = pickLatestBusinessDate(row, isCashCase) || null;
    const currentLatestTs = row?.latestBusinessDate
      ? new Date(row.latestBusinessDate).getTime()
      : 0;
    const nextLatestTs = latestBusinessDate
      ? new Date(latestBusinessDate).getTime()
      : 0;

    if (row?.isCashCase !== isCashCase || currentLatestTs !== nextLatestTs) {
      ops.push({
        updateOne: {
          filter: { _id: row._id },
          update: { $set: { isCashCase, latestBusinessDate } },
        },
      });
      updated += 1;
    }

    if (ops.length >= batchSize) {
      await Loan.bulkWrite(ops, { ordered: false });
      ops = [];
      console.log(`Processed ${processed} loans, updated ${updated}`);
    }
  }

  if (ops.length) {
    await Loan.bulkWrite(ops, { ordered: false });
  }

  console.log(`Backfill complete. Processed ${processed} loans, updated ${updated}`);
  await mongoose.connection.close();
};

run().catch(async (err) => {
  console.error("Backfill failed:", err);
  await mongoose.connection.close();
  process.exit(1);
});
