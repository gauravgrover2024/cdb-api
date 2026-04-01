import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import Payment from "../models/Payment.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import { buildPaymentSkeleton } from "../services/operationsRecordBuilders.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");

const normalizeLoanId = (value = "") => String(value || "").trim();

const dedupeByLoanId = (rows = []) => {
  const map = new Map();
  for (const row of rows) {
    const loanId = normalizeLoanId(row?.loanId || row?.do_loanId);
    if (!loanId) continue;
    const prev = map.get(loanId);
    if (!prev) {
      map.set(loanId, row);
      continue;
    }
    const prevTs =
      new Date(prev?.updatedAt || prev?.createdAt || 0).getTime() || 0;
    const nextTs = new Date(row?.updatedAt || row?.createdAt || 0).getTime() || 0;
    if (nextTs >= prevTs) map.set(loanId, row);
  }
  return Array.from(map.entries()).map(([loanId, row]) => ({ loanId, row }));
};

const main = async () => {
  await connectDB();

  const allDOs = await DeliveryOrder.find({})
    .select(
      [
        "loanId",
        "do_loanId",
        "do_refNo",
        "doNumber",
        "dealerName",
        "do_dealerName",
        "customerName",
        "do_customerName",
        "primaryMobile",
        "do_primaryMobile",
        "vehicleMake",
        "do_vehicleMake",
        "vehicleModel",
        "do_vehicleModel",
        "vehicleVariant",
        "do_vehicleVariant",
        "recordSource",
        "do_recordSource",
        "sourceName",
        "do_sourceName",
        "createdBy",
        "updatedAt",
        "createdAt",
      ].join(" "),
    )
    .lean();

  const latestDOs = dedupeByLoanId(allDOs);
  const loanIds = latestDOs.map((item) => item.loanId);

  const loans = await Loan.find({ loanId: { $in: loanIds } })
    .select(
      [
        "loanId",
        "customerName",
        "primaryMobile",
        "vehicleMake",
        "vehicleModel",
        "vehicleVariant",
        "showroomDealerName",
        "delivery_dealerName",
        "dealerName",
        "sourceName",
        "recordSource",
        "createdBy",
        "do_number",
        "do_refNo",
        "doNumber",
      ].join(" "),
    )
    .lean();
  const loanMap = new Map(
    loans.map((loan) => [normalizeLoanId(loan?.loanId), loan]),
  );

  const existingPayments = await Payment.countDocuments({});
  const payloadPreview = latestDOs.slice(0, 5).map(({ loanId, row }) => ({
    loanId,
    do_refNo: row?.do_refNo || row?.doNumber || "",
    customerName: row?.do_customerName || row?.customerName || "",
    dealerName: row?.do_dealerName || row?.dealerName || "",
    hasLoan: loanMap.has(loanId),
  }));

  console.log(
    JSON.stringify(
      {
        apply: APPLY,
        doRows: allDOs.length,
        uniqueLoanIdsFromDO: latestDOs.length,
        loansMatched: loans.length,
        existingPayments,
        sample: payloadPreview,
      },
      null,
      2,
    ),
  );

  if (!APPLY) {
    console.log(
      '\nDry-run complete. Re-run with "--apply" to delete all payments and recreate from current DO records.',
    );
    process.exit(0);
  }

  const deleteResult = await Payment.deleteMany({});

  const docs = latestDOs.map(({ loanId, row }) => {
    const loan = loanMap.get(loanId) || {};
    const payload = {
      showroomName: row?.do_dealerName || row?.dealerName || "",
      channelName: row?.do_sourceName || row?.sourceName || "",
      customerName: row?.do_customerName || row?.customerName || "",
      primaryMobile: row?.do_primaryMobile || row?.primaryMobile || "",
      vehicleMake: row?.do_vehicleMake || row?.vehicleMake || "",
      vehicleModel: row?.do_vehicleModel || row?.vehicleModel || "",
      vehicleVariant: row?.do_vehicleVariant || row?.vehicleVariant || "",
      do_refNo: row?.do_refNo || row?.doNumber || "",
      doNumber: row?.doNumber || row?.do_refNo || "",
    };
    return {
      ...buildPaymentSkeleton(loanId, payload, loan),
      createdBy: row?.createdBy || loan?.createdBy || undefined,
    };
  });

  if (docs.length) {
    await Payment.insertMany(docs, { ordered: false });
  }

  const finalCount = await Payment.countDocuments({});
  console.log(
    JSON.stringify(
      {
        deletedPayments: deleteResult?.deletedCount || 0,
        recreatedPayments: docs.length,
        finalCount,
      },
      null,
      2,
    ),
  );

  process.exit(0);
};

main().catch((error) => {
  console.error("rebuildPaymentsFromDOs failed:", error);
  process.exit(1);
});

