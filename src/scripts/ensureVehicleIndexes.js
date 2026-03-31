import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";
import VehicleFeature from "../models/VehicleFeature.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const vehicleResult = await Vehicle.createIndexes();
    const vehicleIndexes = await Vehicle.collection.indexes();
    const featureResult = await VehicleFeature.createIndexes();
    const featureIndexes = await VehicleFeature.collection.indexes();
    const vehicleColorsCollection = mongoose.connection.db.collection("vehicle_colors");
    await vehicleColorsCollection.createIndex({ brand: 1, model: 1, variant: 1 });
    await vehicleColorsCollection.createIndex({ brand: 1, model: 1 });
    await vehicleColorsCollection.createIndex({ brand: 1, model: 1, color_hex: 1, scrape_timestamp: -1 });
    const vehicleColorsIndexes = await vehicleColorsCollection.indexes();

    console.log("Vehicle indexes ensured.");
    console.log("vehicle createIndexes result:", vehicleResult);
    console.log(
      "vehicle active indexes:",
      vehicleIndexes.map((idx) => idx.name),
    );
    console.log("VehicleFeature indexes ensured.");
    console.log("vehicleFeature createIndexes result:", featureResult);
    console.log(
      "vehicleFeature active indexes:",
      featureIndexes.map((idx) => idx.name),
    );
    console.log(
      "vehicle_colors active indexes:",
      vehicleColorsIndexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure Vehicle indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
