import dotenv from "dotenv";
import connectDB from "../config/db.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");

const asText = (value) => String(value ?? "").trim();
const asInt = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.trunc(parsed);
};

const hasDOReference = (doRecord = {}) =>
  Boolean(
    asText(doRecord?._id) ||
      asText(doRecord?.do_refNo) ||
      asText(doRecord?.doRefNo) ||
      asText(doRecord?.do_date) ||
      asText(doRecord?.doDate),
  );

const getAutoLoanTargetAmount = (doRecord = {}) => {
  const loanAmount = asInt(doRecord?.do_loanAmount);
  const processingFees = asInt(doRecord?.do_processingFees);
  const financeDeduction = asInt(doRecord?.do_financeDeduction);

  // Business rule:
  // If Processing Fees exists in DO calculation, Loan entry = Loan Amount - Processing Fees.
  if (loanAmount > 0 && processingFees > 0) {
    return Math.max(0, loanAmount - processingFees);
  }

  // Otherwise keep existing DO flow.
  if (financeDeduction > 0) return financeDeduction;
  if (loanAmount > 0) return loanAmount;
  return null;
};

const main = async () => {
  await connectDB();

  const [payments, doRecords] = await Promise.all([
    Payment.find({}).select("_id loanId showroomRows").lean(),
    DeliveryOrder.find({})
      .select(
        "_id loanId do_loanId do_refNo doRefNo do_date doDate do_loanAmount do_processingFees do_financeDeduction",
      )
      .lean(),
  ]);

  const doMap = new Map();
  for (const row of doRecords) {
    const k1 = asText(row?.loanId);
    const k2 = asText(row?.do_loanId);
    if (k1) doMap.set(k1, row);
    if (k2) doMap.set(k2, row);
  }

  let paymentsChecked = 0;
  let paymentsToUpdate = 0;
  let loanRowsUpdated = 0;
  let autoLoanRowsRemoved = 0;
  const bulk = [];

  for (const payment of payments) {
    paymentsChecked += 1;
    const loanId = asText(payment?.loanId);
    if (!loanId) continue;

    const doRecord = doMap.get(loanId) || null;
    const hasDO = hasDOReference(doRecord || {});
    const targetAmount = getAutoLoanTargetAmount(doRecord || {});

    const rows = Array.isArray(payment?.showroomRows) ? payment.showroomRows : [];
    if (!rows.length) continue;

    let changedInDoc = 0;
    const nextRows = rows.filter((row) => {
      const isAutoLoan =
        row?._auto === true && asText(row?.paymentType).toLowerCase() === "loan";
      if (!isAutoLoan) return true;

      // Enforce: auto loan row only after DO creation.
      if (!hasDO) {
        changedInDoc += 1;
        autoLoanRowsRemoved += 1;
        return false;
      }

      if (targetAmount === null) return true;
      const currentAmount = asInt(row?.paymentAmount);
      if (currentAmount !== targetAmount) {
        changedInDoc += 1;
        loanRowsUpdated += 1;
        return true;
      }
      return true;
    }).map((row) => {
      const isAutoLoan =
        row?._auto === true && asText(row?.paymentType).toLowerCase() === "loan";
      if (!isAutoLoan || targetAmount === null || !hasDO) return row;
      const currentAmount = asInt(row?.paymentAmount);
      if (currentAmount === targetAmount) return row;
      return { ...row, paymentAmount: String(targetAmount) };
    });

    if (changedInDoc === 0) continue;
    paymentsToUpdate += 1;
    bulk.push({
      updateOne: {
        filter: { _id: payment._id },
        update: { $set: { showroomRows: nextRows } },
      },
    });
  }

  console.log(
    JSON.stringify(
      {
        apply: APPLY,
        paymentsChecked,
        paymentsToUpdate,
        loanRowsUpdated,
        autoLoanRowsRemoved,
      },
      null,
      2,
    ),
  );

  if (!APPLY) {
    console.log(
      '\nDry run complete. Re-run with "--apply" to update auto loan entries.',
    );
    process.exit(0);
  }

  let paymentDocsModified = 0;
  if (bulk.length) {
    const result = await Payment.bulkWrite(bulk, { ordered: false });
    paymentDocsModified =
      Number(result?.modifiedCount || 0) + Number(result?.upsertedCount || 0);
  }

  console.log(
    JSON.stringify(
      {
        paymentDocsModified,
        loanRowsUpdated,
        autoLoanRowsRemoved,
      },
      null,
      2,
    ),
  );

  process.exit(0);
};

main().catch((error) => {
  console.error("fixAutoLoanEntriesFromDO failed:", error);
  process.exit(1);
});

