import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import Payment from "../models/Payment.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");

const normalizeLoanId = (value = "") => String(value || "").trim();

const parseValidDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const pickLoanDisbursementDate = (loan = {}) => {
  const candidates = [
    loan?.disburse_date,
    loan?.disbursement_date,
    loan?.disbursementDate,
    loan?.disbursedDate,
    loan?.disburseDate,
    loan?.approval_disbursedDate,
    loan?.postfile_disbursementDate,
  ];
  for (const candidate of candidates) {
    const parsed = parseValidDate(candidate);
    if (parsed) return parsed;
  }
  return null;
};

const sameInstant = (left, right) => {
  const l = parseValidDate(left);
  const r = parseValidDate(right);
  if (!l && !r) return true;
  if (!l || !r) return false;
  return l.getTime() === r.getTime();
};

const main = async () => {
  await connectDB();

  const payments = await Payment.find({})
    .select("_id loanId showroomRows disbursementDetails")
    .lean();
  const loanIds = Array.from(
    new Set(payments.map((row) => normalizeLoanId(row?.loanId)).filter(Boolean)),
  );

  const loans = await Loan.find({ loanId: { $in: loanIds } })
    .select(
      "loanId disburse_date disbursement_date disbursementDate disbursedDate disburseDate approval_disbursedDate postfile_disbursementDate",
    )
    .lean();
  const loanDateMap = new Map(
    loans.map((loan) => [
      normalizeLoanId(loan?.loanId),
      pickLoanDisbursementDate(loan),
    ]),
  );

  const bulk = [];
  let docsToUpdate = 0;
  let loanRowsToUpdate = 0;
  let disbDetailsToUpdate = 0;

  for (const payment of payments) {
    const loanId = normalizeLoanId(payment?.loanId);
    if (!loanId) continue;
    const disbDate = loanDateMap.get(loanId);
    if (!disbDate) continue;

    const showroomRows = Array.isArray(payment?.showroomRows)
      ? payment.showroomRows
      : [];
    let rowsChangedInDoc = 0;
    const nextRows = showroomRows.map((entry) => {
      if (String(entry?.paymentType || "").trim().toLowerCase() !== "loan") {
        return entry;
      }
      if (sameInstant(entry?.paymentDate, disbDate)) return entry;
      rowsChangedInDoc += 1;
      return { ...entry, paymentDate: disbDate.toISOString() };
    });

    const patch = {};
    let changed = false;

    if (rowsChangedInDoc > 0) {
      changed = true;
      loanRowsToUpdate += rowsChangedInDoc;
      patch.showroomRows = nextRows;
    }

    if (
      payment?.disbursementDetails &&
      typeof payment.disbursementDetails === "object" &&
      !sameInstant(payment?.disbursementDetails?.date, disbDate)
    ) {
      changed = true;
      disbDetailsToUpdate += 1;
      patch["disbursementDetails.date"] = disbDate;
    }

    if (!changed) continue;

    docsToUpdate += 1;
    bulk.push({
      updateOne: {
        filter: { _id: payment._id },
        update: { $set: patch },
      },
    });
  }

  console.log(
    JSON.stringify(
      {
        apply: APPLY,
        paymentsChecked: payments.length,
        loansMatched: loans.length,
        paymentsToUpdate: docsToUpdate,
        loanRowsToUpdate,
        disbursementDetailsToUpdate: disbDetailsToUpdate,
      },
      null,
      2,
    ),
  );

  if (!APPLY) {
    console.log(
      '\nDry run complete. Re-run with "--apply" to update payment loan-entry dates.',
    );
    process.exit(0);
  }

  let modified = 0;
  if (bulk.length) {
    const result = await Payment.bulkWrite(bulk, { ordered: false });
    modified =
      Number(result?.modifiedCount || 0) + Number(result?.upsertedCount || 0);
  }

  console.log(
    JSON.stringify(
      {
        paymentDocsModified: modified,
        loanRowsUpdated: loanRowsToUpdate,
        disbursementDetailsUpdated: disbDetailsToUpdate,
      },
      null,
      2,
    ),
  );

  process.exit(0);
};

main().catch((error) => {
  console.error("fixPaymentLoanEntryDatesFromDisbursement failed:", error);
  process.exit(1);
});

