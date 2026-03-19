import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const duplicateSummary = await Loan.aggregate([
      { $match: { loanId: { $exists: true, $ne: null } } },
      { $group: { _id: "$loanId", count: { $sum: 1 } } },
      { $match: { count: { $gt: 1 } } },
      {
        $group: {
          _id: null,
          duplicateLoanIds: { $sum: 1 },
          duplicateRows: { $sum: "$count" },
          maxCopies: { $max: "$count" },
        },
      },
    ]);

    const duplicateStats = duplicateSummary[0] || {
      duplicateLoanIds: 0,
      duplicateRows: 0,
      maxCopies: 0,
    };

    if (duplicateStats.duplicateLoanIds > 0) {
      console.error("Cannot enforce unique loanId index: duplicates still exist.");
      console.error(
        `duplicate loanIds=${duplicateStats.duplicateLoanIds}, duplicate rows=${duplicateStats.duplicateRows}, max copies for a loanId=${duplicateStats.maxCopies}`,
      );
      console.error("Run dedupe script first: node src/scripts/dedupeLoanIds.js --apply");
      process.exitCode = 1;
      return;
    }

    const indexesBefore = await Loan.collection.indexes();
    const existingLoanIdIndex = indexesBefore.find((idx) => idx.name === "loanId_1");
    if (existingLoanIdIndex && existingLoanIdIndex.unique !== true) {
      console.log("Dropping non-unique index loanId_1 before creating unique index...");
      await Loan.collection.dropIndex("loanId_1");
    }

    const result = await Loan.createIndexes();
    const indexes = await Loan.collection.indexes();
    const enforced = indexes.find((idx) => idx.name === "loanId_1");

    console.log("Loan indexes ensured.");
    console.log("createIndexes result:", result);
    console.log("loanId_1 index unique:", enforced?.unique === true);
    console.log(
      "active indexes:",
      indexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure Loan indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
