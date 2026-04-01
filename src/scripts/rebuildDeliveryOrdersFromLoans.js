import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";
import {
  buildDeliveryOrderSnapshot,
  buildPaymentSkeleton,
  isNewCarLoan,
} from "../services/operationsRecordBuilders.js";

dotenv.config();

const cutoff = new Date("2026-02-01T00:00:00.000Z");
const APPLY = process.argv.includes("--apply");

const main = async () => {
  await connectDB();

  const candidateLoans = await Loan.find({
    leadDate: { $gte: cutoff },
  })
    .select(
      [
        "loanId",
        "leadDate",
        "typeOfLoan",
        "loanType",
        "customerName",
        "primaryMobile",
        "vehicleMake",
        "vehicleModel",
        "vehicleVariant",
        "vehicleColor",
        "showroomDealerName",
        "showroomDealerAddress",
        "delivery_dealerName",
        "delivery_dealerAddress",
        "delivery_dealerContactNumber",
        "delivery_dealerContactPerson",
        "delivery_dealerCity",
        "delivery_dealerPincode",
        "residenceAddress",
        "currentAddress",
        "pincode",
        "city",
        "recordSource",
        "sourceName",
        "exShowroomPrice",
        "ex_showroom",
        "insuranceCost",
        "insurance",
        "roadTax",
        "rto",
        "tcs",
        "other_tcsCharges",
        "optional_accessoriesCharges",
        "optional_extendedWarrantyCharges",
        "postfile_processingFees",
        "processingFees",
        "postfile_loanAmountDisbursed",
        "postfile_netLoanAmount",
        "loanAmount",
        "do_number",
        "do_date",
        "createdBy",
      ].join(" "),
    )
    .lean();

  const eligibleLoans = candidateLoans.filter((loan) => isNewCarLoan(loan) && loan?.loanId);
  const loanIds = eligibleLoans.map((loan) => String(loan.loanId).trim()).filter(Boolean);

  const existingDOCount = await DeliveryOrder.countDocuments({
    $or: [{ loanId: { $in: loanIds } }, { do_loanId: { $in: loanIds } }],
  });
  const existingPaymentCount = await Payment.countDocuments({
    loanId: { $in: loanIds },
  });

  console.log(
    JSON.stringify(
      {
        apply: APPLY,
        cutoff: cutoff.toISOString(),
        eligibleLoans: eligibleLoans.length,
        existingDOsForEligibleLoans: existingDOCount,
        existingPaymentsForEligibleLoans: existingPaymentCount,
        sample: eligibleLoans.slice(0, 5).map((loan) => ({
          loanId: loan.loanId,
          customerName: loan.customerName,
          leadDate: loan.leadDate,
          loanType: loan.typeOfLoan || loan.loanType,
        })),
      },
      null,
      2,
    ),
  );

  if (!APPLY) {
    console.log(
      '\nDry-run complete. Re-run with "--apply" to delete existing eligible DOs and rebuild them from Loans.',
    );
    process.exit(0);
  }

  const deleteResult = await DeliveryOrder.deleteMany({
    $or: [{ loanId: { $in: loanIds } }, { do_loanId: { $in: loanIds } }],
  });

  let rebuilt = 0;
  let paymentEnsured = 0;
  let loanUpdated = 0;

  for (const loan of eligibleLoans) {
    const snapshot = buildDeliveryOrderSnapshot({}, loan, loan.loanId);
    const created = await DeliveryOrder.findOneAndUpdate(
      { loanId: loan.loanId },
      {
        $set: {
          ...snapshot,
          createdBy: loan?.createdBy || undefined,
        },
      },
      {
        upsert: true,
        returnDocument: "after",
        setDefaultsOnInsert: true,
      },
    );
    rebuilt += 1;

    const paymentUpsert = await Payment.findOneAndUpdate(
      { loanId: loan.loanId },
      {
        $setOnInsert: {
          ...buildPaymentSkeleton(loan.loanId, {}, loan),
          createdBy: loan?.createdBy || undefined,
        },
      },
      {
        upsert: true,
        returnDocument: "after",
        setDefaultsOnInsert: true,
        rawResult: true,
      },
    );
    if (paymentUpsert) paymentEnsured += 1;

    await Loan.updateOne(
      { loanId: loan.loanId },
      {
        $set: {
          do_number: created?.do_refNo || created?.doNumber || "",
          do_date: created?.do_date || created?.doDate || new Date(),
        },
      },
    );
    loanUpdated += 1;
  }

  const finalDOCount = await DeliveryOrder.countDocuments({
    $or: [{ loanId: { $in: loanIds } }, { do_loanId: { $in: loanIds } }],
  });
  const finalPaymentCount = await Payment.countDocuments({
    loanId: { $in: loanIds },
  });

  console.log(
    JSON.stringify(
      {
        deletedExistingDOs: deleteResult?.deletedCount || 0,
        rebuiltDOs: rebuilt,
        paymentEnsured,
        loanUpdated,
        finalDOCount,
        finalPaymentCount,
      },
      null,
      2,
    ),
  );

  process.exit(0);
};

main().catch((error) => {
  console.error("rebuildDeliveryOrdersFromLoans failed:", error);
  process.exit(1);
});
