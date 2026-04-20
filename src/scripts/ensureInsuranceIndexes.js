import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import InsuranceCase from "../models/InsuranceCase.js";
import InsurancePayoutRate from "../models/InsurancePayoutRate.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();

    const [insuranceCaseResult, payoutRateResult] = await Promise.all([
      InsuranceCase.createIndexes(),
      InsurancePayoutRate.createIndexes(),
    ]);

    const [insuranceCaseIndexes, payoutRateIndexes] = await Promise.all([
      InsuranceCase.collection.indexes(),
      InsurancePayoutRate.collection.indexes(),
    ]);

    console.log("Insurance indexes ensured.");
    console.log("insuranceCase createIndexes result:", insuranceCaseResult);
    console.log(
      "insuranceCase active indexes:",
      insuranceCaseIndexes.map((idx) => idx.name),
    );
    console.log("insurancePayoutRate createIndexes result:", payoutRateResult);
    console.log(
      "insurancePayoutRate active indexes:",
      payoutRateIndexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure insurance indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
