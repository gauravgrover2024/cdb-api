import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";

dotenv.config();

const clearVehicles = async () => {
  try {
    await connectDB();
    console.log("🧹 Clearing vehicles collection...");

    const res = await Vehicle.deleteMany({});
    console.log(`✅ Deleted ${res.deletedCount ?? 0} vehicle(s).`);

    await mongoose.connection.close();
    process.exit(0);
  } catch (error) {
    console.error("❌ Failed to clear vehicles:", error);
    try {
      await mongoose.connection.close();
    } catch (_) {}
    process.exit(1);
  }
};

clearVehicles();

