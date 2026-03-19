import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";

dotenv.config();

const LEGACY_CUTOFF = new Date("2026-02-01T00:00:00.000Z");
const APPLY_CHANGES = process.argv.includes("--apply");
const SAMPLE_LIMIT = 20;
const BATCH_SIZE = 1000;

const BUSINESS_DATE_FIELDS = [
  "latestBusinessDate",
  "delivery_date",
  "deliveryDate",
  "do_date",
  "doDate",
  "invoice_date",
  "invoiceDate",
  "disbursement_date",
  "approval_disbursedDate",
  "disburse_date",
  "disbursementDate",
  "disbursedDate",
  "postfile_disbursementDate",
];

const parseValidDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const chunkArray = (arr, size) => {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
};

const countByLoanIds = async (Model, loanIds) => {
  let total = 0;
  for (const chunk of chunkArray(loanIds, BATCH_SIZE)) {
    total += await Model.countDocuments({ loanId: { $in: chunk } });
  }
  return total;
};

const deleteByLoanIds = async (Model, loanIds) => {
  let deleted = 0;
  for (const chunk of chunkArray(loanIds, BATCH_SIZE)) {
    const result = await Model.deleteMany({ loanId: { $in: chunk } });
    deleted += Number(result?.deletedCount || 0);
  }
  return deleted;
};

const detectLegacyEvent = (loan = {}) => {
  for (const field of BUSINESS_DATE_FIELDS) {
    const parsed = parseValidDate(loan[field]);
    if (parsed && parsed < LEGACY_CUTOFF) {
      return { matched: true, field, date: parsed };
    }
  }
  return { matched: false };
};

const run = async () => {
  await connectDB();

  const projection = { loanId: 1 };
  for (const field of BUSINESS_DATE_FIELDS) projection[field] = 1;

  let scanned = 0;
  let matched = 0;
  const matchedLoanIds = [];
  const samples = [];

  const cursor = Loan.find(
    { loanId: { $exists: true, $ne: null } },
    projection,
  ).lean().cursor();

  for await (const loan of cursor) {
    scanned += 1;
    const legacyEvent = detectLegacyEvent(loan);
    if (!legacyEvent.matched) continue;

    const loanId = String(loan.loanId || "").trim();
    if (!loanId) continue;

    matched += 1;
    matchedLoanIds.push(loanId);

    if (samples.length < SAMPLE_LIMIT) {
      samples.push({
        loanId,
        matchedField: legacyEvent.field,
        matchedDate: legacyEvent.date.toISOString().slice(0, 10),
      });
    }
  }

  const uniqueLoanIds = [...new Set(matchedLoanIds)];

  const existingDOCount = uniqueLoanIds.length
    ? await countByLoanIds(DeliveryOrder, uniqueLoanIds)
    : 0;
  const existingPaymentCount = uniqueLoanIds.length
    ? await countByLoanIds(Payment, uniqueLoanIds)
    : 0;

  console.log("Legacy linked-record cleanup");
  console.log("--------------------------------");
  console.log("Cutoff (exclusive):", LEGACY_CUTOFF.toISOString());
  console.log("Mode:", APPLY_CHANGES ? "APPLY (DELETE)" : "DRY RUN");
  console.log("Loans scanned:", scanned);
  console.log("Loans matched:", matched);
  console.log("Unique loanIds matched:", uniqueLoanIds.length);
  console.log("DeliveryOrder rows currently present:", existingDOCount);
  console.log("Payment rows currently present:", existingPaymentCount);

  if (samples.length > 0) {
    console.log("Sample matched loans:");
    for (const sample of samples) {
      console.log(
        `- ${sample.loanId} (${sample.matchedField}: ${sample.matchedDate})`,
      );
    }
  }

  if (!APPLY_CHANGES) {
    console.log(
      '\nDry-run complete. Re-run with "--apply" to delete matching DeliveryOrder and Payment records.',
    );
    return;
  }

  const deletedDOCount = uniqueLoanIds.length
    ? await deleteByLoanIds(DeliveryOrder, uniqueLoanIds)
    : 0;
  const deletedPaymentCount = uniqueLoanIds.length
    ? await deleteByLoanIds(Payment, uniqueLoanIds)
    : 0;

  console.log("\nDelete completed.");
  console.log("DeliveryOrder rows deleted:", deletedDOCount);
  console.log("Payment rows deleted:", deletedPaymentCount);
};

run()
  .catch((error) => {
    console.error("cleanupLegacyDoPayments failed:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await mongoose.connection.close();
    } catch (_) {
      // noop
    }
  });
