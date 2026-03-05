import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const result = await Loan.createIndexes();
    const indexes = await Loan.collection.indexes();

    console.log("Loan indexes ensured.");
    console.log("createIndexes result:", result);
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
