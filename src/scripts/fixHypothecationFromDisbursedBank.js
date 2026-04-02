import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");

const asText = (value) => String(value ?? "").trim();

const firstMeaningfulText = (...values) => {
  for (const value of values) {
    const text = asText(value);
    if (!text) continue;
    if (
      ["n/a", "na", "null", "undefined", "-", "--", "not set"].includes(
        text.toLowerCase(),
      )
    ) {
      continue;
    }
    return text;
  }
  return "";
};

const pickDisbursedBankName = (loan = {}) => {
  const banks = Array.isArray(loan?.approval_banksData)
    ? loan.approval_banksData
    : [];

  const disbursedBank = banks.find(
    (row) =>
      asText(row?.status).toLowerCase() === "disbursed" &&
      firstMeaningfulText(row?.bankName),
  );
  const approvedBank = banks.find(
    (row) =>
      asText(row?.status).toLowerCase() === "approved" &&
      firstMeaningfulText(row?.bankName),
  );

  return firstMeaningfulText(
    loan?.disburse_bankName,
    disbursedBank?.bankName,
    loan?.postfile_bankName,
    loan?.approval_bankName,
    approvedBank?.bankName,
  );
};

const main = async () => {
  await connectDB();

  const [doDocs, paymentDocs] = await Promise.all([
    DeliveryOrder.find({})
      .select("_id loanId do_loanId do_hypothecation")
      .lean(),
    Payment.find({})
      .select(
        "_id loanId showroomRows disbursementDetails bankName hypothecationBank",
      )
      .lean(),
  ]);

  const loanIdSet = new Set();
  for (const row of doDocs) {
    const loanId = asText(row?.loanId || row?.do_loanId);
    if (loanId) loanIdSet.add(loanId);
  }
  for (const row of paymentDocs) {
    const loanId = asText(row?.loanId);
    if (loanId) loanIdSet.add(loanId);
  }

  const loanIds = Array.from(loanIdSet);
  const loans = await Loan.find({ loanId: { $in: loanIds } })
    .select(
      "loanId disburse_bankName postfile_bankName approval_bankName approval_banksData",
    )
    .lean();

  const loanBankMap = new Map(
    loans.map((loan) => [asText(loan?.loanId), pickDisbursedBankName(loan)]),
  );

  const doBulk = [];
  let doPlanned = 0;
  for (const row of doDocs) {
    const loanId = asText(row?.loanId || row?.do_loanId);
    const expected = loanBankMap.get(loanId) || "";
    const current = asText(row?.do_hypothecation);
    if (current === expected) continue;
    doPlanned += 1;
    doBulk.push({
      updateOne: {
        filter: { _id: row._id },
        update: { $set: { do_hypothecation: expected } },
      },
    });
  }

  const paymentBulk = [];
  let paymentPlanned = 0;
  let paymentLoanRowsTouched = 0;
  for (const row of paymentDocs) {
    const loanId = asText(row?.loanId);
    const expected = loanBankMap.get(loanId) || "";
    if (!loanId) continue;

    const patch = {};
    let changed = false;

    const showroomRows = Array.isArray(row?.showroomRows) ? row.showroomRows : [];
    let loanRowsChangedInDoc = 0;
    const nextRows = showroomRows.map((entry) => {
      if (asText(entry?.paymentType).toLowerCase() !== "loan") return entry;
      const currentBank = asText(entry?.bankName);
      if (currentBank === expected) return entry;
      loanRowsChangedInDoc += 1;
      return { ...entry, bankName: expected };
    });
    if (loanRowsChangedInDoc > 0) {
      changed = true;
      paymentLoanRowsTouched += loanRowsChangedInDoc;
      patch.showroomRows = nextRows;
    }

    if (asText(row?.hypothecationBank) !== expected) {
      changed = true;
      patch.hypothecationBank = expected;
    }

    if (asText(row?.bankName) !== expected) {
      changed = true;
      patch.bankName = expected;
    }

    if (row?.disbursementDetails && typeof row.disbursementDetails === "object") {
      const currentDisbBank = asText(row.disbursementDetails?.bankName);
      if (currentDisbBank !== expected) {
        changed = true;
        patch["disbursementDetails.bankName"] = expected;
      }
    }

    if (!changed) continue;
    paymentPlanned += 1;
    paymentBulk.push({
      updateOne: {
        filter: { _id: row._id },
        update: { $set: patch },
      },
    });
  }

  const summary = {
    apply: APPLY,
    loanIdsChecked: loanIds.length,
    loansMatched: loans.length,
    deliveryOrdersChecked: doDocs.length,
    deliveryOrdersToUpdate: doPlanned,
    paymentsChecked: paymentDocs.length,
    paymentsToUpdate: paymentPlanned,
    paymentLoanRowsToUpdate: paymentLoanRowsTouched,
  };
  console.log(JSON.stringify(summary, null, 2));

  if (!APPLY) {
    console.log(
      '\nDry run complete. Re-run with "--apply" to update existing DeliveryOrder and Payment entries.',
    );
    process.exit(0);
  }

  let doModified = 0;
  let paymentModified = 0;
  if (doBulk.length) {
    const result = await DeliveryOrder.bulkWrite(doBulk, { ordered: false });
    doModified =
      Number(result?.modifiedCount || 0) + Number(result?.upsertedCount || 0);
  }
  if (paymentBulk.length) {
    const result = await Payment.bulkWrite(paymentBulk, { ordered: false });
    paymentModified =
      Number(result?.modifiedCount || 0) + Number(result?.upsertedCount || 0);
  }

  console.log(
    JSON.stringify(
      {
        doModified,
        paymentModified,
        paymentLoanRowsUpdated: paymentLoanRowsTouched,
      },
      null,
      2,
    ),
  );

  process.exit(0);
};

main().catch((error) => {
  console.error("fixHypothecationFromDisbursedBank failed:", error);
  process.exit(1);
});

