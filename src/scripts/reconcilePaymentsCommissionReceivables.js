import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Payment from "../models/Payment.js";
import {
  syncPaymentsCommissionReceivableForLoan,
} from "../services/paymentsCommissionReceivableService.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");
const loanIdArgIndex = process.argv.findIndex((arg) => arg === "--loanId");
const loanIdArg =
  loanIdArgIndex >= 0 ? String(process.argv[loanIdArgIndex + 1] || "").trim() : "";

const main = async () => {
  await connectDB();

  if (!APPLY) {
    console.log(
      'Dry run only. Re-run with "--apply" to reconcile payments commission receivables.',
    );
    process.exit(0);
  }

  const loanIds = loanIdArg
    ? [loanIdArg]
    : (
        await Payment.find({ loanId: { $exists: true, $ne: "" } })
          .select("loanId")
          .lean()
      ).map((row) => String(row?.loanId || "").trim()).filter(Boolean);

  const uniqLoanIds = Array.from(new Set(loanIds));
  const summary = { processed: 0, created: 0, updated: 0, deleted: 0, noop: 0 };

  for (const loanId of uniqLoanIds) {
    // eslint-disable-next-line no-await-in-loop
    const result = await syncPaymentsCommissionReceivableForLoan({ loanId });
    summary.processed += 1;
    if (result?.action === "created") summary.created += 1;
    else if (result?.action === "updated") summary.updated += 1;
    else if (result?.action === "deleted") summary.deleted += 1;
    else summary.noop += 1;
  }

  console.log(JSON.stringify(summary, null, 2));
  process.exit(0);
};

main().catch((error) => {
  console.error("reconcilePaymentsCommissionReceivables failed:", error);
  process.exit(1);
});

