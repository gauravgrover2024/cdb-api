import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import UsedCarLead from "../models/UsedCarLead.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const result = await UsedCarLead.createIndexes();
    const indexes = await UsedCarLead.collection.indexes();

    console.log("Used car lead indexes ensured.");
    console.log("createIndexes result:", result);
    console.log(
      "active indexes:",
      indexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure used car lead indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
