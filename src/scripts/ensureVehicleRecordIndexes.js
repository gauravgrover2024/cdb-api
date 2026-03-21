import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import VehicleRecord from "../models/VehicleRecord.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const created = [];
    created.push(
      await VehicleRecord.collection.createIndex({ registrationNumberNormalized: 1 }),
    );
    created.push(
      await VehicleRecord.collection.createIndex({ registrationNumberLast4: 1 }),
    );
    created.push(
      await VehicleRecord.collection.createIndex({ make: 1, model: 1, variant: 1 }),
    );
    const indexes = await VehicleRecord.collection.indexes();

    console.log("Vehicle record indexes ensured.");
    console.log("created/verified indexes:", created);
    console.log(
      "active indexes:",
      indexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure Vehicle record indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
