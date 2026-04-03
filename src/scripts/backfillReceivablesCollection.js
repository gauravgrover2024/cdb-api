import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import Receivable from "../models/Receivable.js";
import { upsertReceivablesFromLoan } from "../services/receivableSyncService.js";

dotenv.config();

const run = async () => {
  let processed = 0;
  let syncedLoans = 0;
  let totalUpserted = 0;
  let totalRemoved = 0;
  try {
    await connectDB();
    await Receivable.createIndexes();

    const cursor = Loan.find(
      {
        $or: [
          { "loan_receivables.0": { $exists: true } },
          { "loanReceivables.0": { $exists: true } },
          { "receivables.0": { $exists: true } },
          { "loan_payouts.0": { $exists: true } },
          {
            approval_banksData: {
              $elemMatch: {
                status: { $regex: /^disbursed$/i },
                payoutPercent: { $nin: [null, "", "0", 0, "0.0", "0.00"] },
              },
            },
          },
        ],
      },
      {
        _id: 1,
        loanId: 1,
        customerName: 1,
        loan_receivables: 1,
        loanReceivables: 1,
        receivables: 1,
        loan_payouts: 1,
        approval_banksData: 1,
        disburse_bankName: 1,
        approval_bankName: 1,
        disbursement_date: 1,
        approval_disbursedDate: 1,
        updatedAt: 1,
        createdAt: 1,
      },
    )
      .lean()
      .cursor();

    for await (const loan of cursor) {
      processed += 1;
      const result = await upsertReceivablesFromLoan(loan);
      syncedLoans += 1;
      totalUpserted += Number(result?.upserted || 0);
      totalRemoved += Number(result?.removed || 0);
      if (processed % 200 === 0) {
        console.log(
          `Processed ${processed} loans | synced=${syncedLoans} | upserted=${totalUpserted} | removed=${totalRemoved}`,
        );
      }
    }

    console.log("Receivables backfill completed.");
    console.log(
      JSON.stringify(
        { processed, syncedLoans, totalUpserted, totalRemoved },
        null,
        2,
      ),
    );
  } catch (error) {
    console.error("Receivables backfill failed:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
