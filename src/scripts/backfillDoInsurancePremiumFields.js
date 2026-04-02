import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import { buildDeliveryOrderSnapshot } from "../services/operationsRecordBuilders.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");
const TARGET_LOAN_ID = "LN-2026-4639";
const TARGET_PREMIUM = 60935;

const TRACKED_FIELDS = [
  "do_customer_insuranceCost",
  "do_customer_actualInsurancePremium",
  "do_customer_insuranceBy",
  "do_customer_insuranceCompanyName",
  "do_customer_insurancePolicyNumber",
  "do_customer_insurancePolicyStartDate",
  "do_customer_insurancePolicyDurationOD",
  "do_customer_insurancePolicyEndDateOD",
];

const normalizeComparable = (value) => {
  if (value instanceof Date) return value.toISOString();
  if (value === undefined) return "__undefined__";
  if (value === null) return "__null__";
  return JSON.stringify(value);
};

const main = async () => {
  await connectDB();

  const report = {
    apply: APPLY,
    targetLoanId: TARGET_LOAN_ID,
    targetPremium: TARGET_PREMIUM,
    targetLoanUpdated: 0,
    deliveryOrdersScanned: 0,
    deliveryOrdersWithLoan: 0,
    deliveryOrdersToUpdate: 0,
    deliveryOrdersUpdated: 0,
    sampleUpdates: [],
    skippedMissingLoan: 0,
  };

  if (APPLY) {
    const premiumSet = await Loan.updateOne(
      { loanId: TARGET_LOAN_ID },
      { $set: { insurance_premium: TARGET_PREMIUM } },
    );
    report.targetLoanUpdated = premiumSet?.modifiedCount || 0;
  }

  const doRows = await DeliveryOrder.find({})
    .select(["_id", "loanId", "do_loanId", ...TRACKED_FIELDS].join(" "))
    .lean();
  report.deliveryOrdersScanned = doRows.length;

  const loanIds = Array.from(
    new Set(
      doRows
        .map((row) => String(row?.loanId || row?.do_loanId || "").trim())
        .filter(Boolean),
    ),
  );

  const loans = await Loan.find({ loanId: { $in: loanIds } })
    .select(
      [
        "loanId",
        "insurance_by",
        "insuranceBy",
        "insurance_company_name",
        "insurance_policy_number",
        "insurance_premium",
        "insurance_policy_start_date",
        "insurance_policy_duration_od",
        "insurance_policy_end_date_od",
        "insuranceCost",
        "insurance",
        "insurance_amount_cardekho",
      ].join(" "),
    )
    .lean();

  const loanMap = new Map(
    loans.map((loan) => [String(loan?.loanId || "").trim(), loan]),
  );

  const ops = [];
  for (const row of doRows) {
    const loanId = String(row?.loanId || row?.do_loanId || "").trim();
    if (!loanId) continue;
    const loan = loanMap.get(loanId);
    if (!loan) {
      report.skippedMissingLoan += 1;
      continue;
    }
    report.deliveryOrdersWithLoan += 1;

    const snapshot = buildDeliveryOrderSnapshot(row, loan, loanId);
    const patch = {};

    for (const field of TRACKED_FIELDS) {
      const nextValue = snapshot?.[field];
      const currentValue = row?.[field];
      if (normalizeComparable(nextValue) !== normalizeComparable(currentValue)) {
        patch[field] = nextValue;
      }
    }

    if (!Object.keys(patch).length) continue;
    report.deliveryOrdersToUpdate += 1;
    if (report.sampleUpdates.length < 8) {
      report.sampleUpdates.push({
        loanId,
        doId: String(row?._id || ""),
        patch,
      });
    }

    if (APPLY) {
      ops.push({
        updateOne: {
          filter: { _id: row._id },
          update: { $set: patch },
        },
      });
    }
  }

  if (APPLY && ops.length) {
    const result = await DeliveryOrder.bulkWrite(ops, { ordered: false });
    report.deliveryOrdersUpdated = result?.modifiedCount || 0;
  }

  console.log(JSON.stringify(report, null, 2));
  process.exit(0);
};

main().catch((error) => {
  console.error("backfillDoInsurancePremiumFields failed:", error);
  process.exit(1);
});

