import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const result = await Vehicle.createIndexes();
    const indexes = await Vehicle.collection.indexes();

    console.log("Vehicle indexes ensured.");
    console.log("createIndexes result:", result);
    console.log(
      "active indexes:",
      indexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure Vehicle indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
