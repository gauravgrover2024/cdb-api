import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import { reconcileManualVehicleTerms } from "../services/vehicleSuggestionTermService.js";

dotenv.config();

// Standalone entry point for the same reconciliation that runs automatically
// after a vehicle-prices scraper job. Useful for cron or a manual re-run.
const main = async () => {
  await connectDB();
  const result = await reconcileManualVehicleTerms();
  console.log(JSON.stringify(result, null, 2));
  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error("reconcileVehicleSuggestionTerms failed:", error);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
