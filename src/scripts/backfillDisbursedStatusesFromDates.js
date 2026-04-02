import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";

dotenv.config();

const toLower = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const asDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const pickDisbursementDate = (loan) =>
  asDate(
    loan?.disburse_date ||
      loan?.approval_disbursedDate ||
      loan?.disbursement_date ||
      loan?.disbursementDate ||
      loan?.disbursedDate ||
      loan?.disburseDate ||
      loan?.postfile_disbursementDate ||
      null,
  );

const pickDisbursedBankName = (loan) =>
  String(
    loan?.disburse_bankName ||
      loan?.postfile_bankName ||
      loan?.approval_bankName ||
      "",
  ).trim();

const buildStatusEvent = (status, dateObj) => ({
  status,
  date: dateObj.toISOString(),
  changedAt: dateObj.toISOString(),
});

const normalizeBankHistory = (bank, disbDate) => {
  const history = Array.isArray(bank?.statusHistory) ? [...bank.statusHistory] : [];
  const hasApproved = history.some(
    (item) => toLower(item?.status) === "approved",
  );
  const hasDisbursed = history.some(
    (item) => toLower(item?.status) === "disbursed",
  );

  if (!hasApproved) {
    history.push(buildStatusEvent("Approved", disbDate));
  }
  if (!hasDisbursed) {
    history.push(buildStatusEvent("Disbursed", disbDate));
  }

  return history;
};

const chooseBankIndex = (banks, disbursedBankName) => {
  if (!banks.length) return -1;
  if (!disbursedBankName) return 0;
  const wanted = toLower(disbursedBankName);
  const exact = banks.findIndex((bank) => toLower(bank?.bankName) === wanted);
  if (exact >= 0) return exact;
  return 0;
};

const shouldSkipCashCase = (loan) => {
  const financed = toLower(loan?.isFinanced);
  if (financed === "no" || financed === "false") return true;
  const loanType = toLower(loan?.typeOfLoan || loan?.loanType || loan?.caseType);
  return loanType.includes("cash");
};

const run = async () => {
  const apply = process.argv.includes("--apply");

  try {
    await connectDB();

    const candidates = await Loan.find({
      $and: [
        {
          $or: [
            { disbursement_date: { $exists: true, $ne: null } },
            { approval_disbursedDate: { $exists: true, $ne: null } },
            { disburse_date: { $exists: true, $ne: null } },
            { disbursementDate: { $exists: true, $ne: null } },
            { disbursedDate: { $exists: true, $ne: null } },
            { disburseDate: { $exists: true, $ne: null } },
            { postfile_disbursementDate: { $exists: true, $ne: null } },
          ],
        },
        { $nor: [{ "approval_banksData.status": /disburs/i }] },
      ],
    })
      .select(
        [
          "_id",
          "loanId",
          "isFinanced",
          "typeOfLoan",
          "loanType",
          "caseType",
          "status",
          "currentStage",
          "approval_status",
          "approval_bankName",
          "postfile_bankName",
          "disburse_bankName",
          "disburse_status",
          "disbursementStatus",
          "disbursement_status",
          "disburse_date",
          "approval_disbursedDate",
          "disbursement_date",
          "disbursementDate",
          "disbursedDate",
          "disburseDate",
          "postfile_disbursementDate",
          "approval_banksData",
        ].join(" "),
      )
      .lean();

    const ops = [];
    const skipped = [];
    const touchedLoanIds = [];

    for (const loan of candidates) {
      if (shouldSkipCashCase(loan)) {
        skipped.push({ loanId: loan.loanId, reason: "cash-case" });
        continue;
      }

      const disbDate = pickDisbursementDate(loan);
      if (!disbDate) {
        skipped.push({ loanId: loan.loanId, reason: "missing-valid-disbursement-date" });
        continue;
      }

      const banks = Array.isArray(loan?.approval_banksData)
        ? [...loan.approval_banksData]
        : [];

      if (!banks.length) {
        const fallbackBankName = pickDisbursedBankName(loan) || "Unknown";
        banks.push({
          id: 1,
          bankName: fallbackBankName,
          status: "Disbursed",
          disbursedDate: disbDate.toISOString(),
          statusHistory: [
            buildStatusEvent("Approved", disbDate),
            buildStatusEvent("Disbursed", disbDate),
          ],
        });
      } else {
        const index = chooseBankIndex(banks, pickDisbursedBankName(loan));
        if (index >= 0) {
          const target = { ...banks[index] };
          target.status = "Disbursed";
          target.disbursedDate =
            target.disbursedDate || target.disbursalDate || disbDate.toISOString();
          target.statusHistory = normalizeBankHistory(target, disbDate);
          banks[index] = target;
        }
      }

      const currentStage = toLower(loan?.currentStage);
      const nextStage =
        currentStage === "profile" ||
        currentStage === "prefile" ||
        currentStage === "approval"
          ? "postfile"
          : loan?.currentStage;

      const disbursedBankName = pickDisbursedBankName(loan) || banks[0]?.bankName || "";

      ops.push({
        updateOne: {
          filter: { _id: loan._id },
          update: {
            $set: {
              approval_banksData: banks,
              approval_status: "Disbursed",
              disburse_status: "Disbursed",
              disbursementStatus: "Disbursed",
              disbursement_status: "Disbursed",
              disburse_bankName: disbursedBankName,
              disburse_date: loan?.disburse_date || disbDate,
              approval_disbursedDate: loan?.approval_disbursedDate || disbDate,
              disbursement_date: loan?.disbursement_date || disbDate,
              disbursementDate: loan?.disbursementDate || disbDate,
              disbursedDate: loan?.disbursedDate || disbDate,
              disburseDate: loan?.disburseDate || disbDate,
              currentStage: nextStage,
            },
          },
        },
      });
      touchedLoanIds.push(loan.loanId);
    }

    console.log(`Found ${candidates.length} candidate loans.`);
    console.log(
      `Prepared ${ops.length} updates. Skipped ${skipped.length} loans.`,
    );
    if (touchedLoanIds.length) {
      console.log("Loans to update:", touchedLoanIds.join(", "));
    }
    if (skipped.length) {
      console.log("Skipped:", JSON.stringify(skipped, null, 2));
    }

    if (!apply) {
      console.log("Dry run complete. Re-run with --apply to persist updates.");
      return;
    }

    if (ops.length) {
      const result = await Loan.bulkWrite(ops, { ordered: false });
      console.log("Bulk write result:", {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      });
    } else {
      console.log("No updates required.");
    }
  } catch (error) {
    console.error("Backfill failed:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
