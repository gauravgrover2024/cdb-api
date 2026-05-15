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
    const vehicleColorsCollection = mongoose.connection.db.collection("vehicle_colors_v2");
    await vehicleColorsCollection.createIndex({ brand_slug: 1, model_slug: 1 }, { name: "uniq_brand_model_color_media", unique: true });
    await vehicleColorsCollection.createIndex({ brand: 1, model: 1, variant: 1 }, { name: "vehicle_colors_v2_brand_model_variant" });
    await vehicleColorsCollection.createIndex({ brand: 1, model: 1 }, { name: "vehicle_colors_v2_brand_model" });
    await vehicleColorsCollection.createIndex({ brand: 1, model: 1, color_hex: 1, scrape_timestamp: -1 });
    await vehicleColorsCollection.createIndex({ "colors.name": 1, brand: 1, model: 1 }, { name: "vehicle_colors_v2_nested_color_name" });
    await vehicleColorsCollection.createIndex(
      { brand: 1, model: 1, scopeStatus: 1, color_name: 1, updatedAt: -1 },
      { name: "vehicle_colors_brand_model_scope_color_updated" },
    );
    await vehicleColorsCollection.createIndex(
      { make: 1, model: 1, scopeStatus: 1, color_name: 1, updatedAt: -1 },
      { name: "vehicle_colors_make_model_scope_color_updated" },
    );
    await vehicleColorsCollection.createIndex(
      { model: 1, scopeStatus: 1, color_name: 1, updatedAt: -1 },
      { name: "vehicle_colors_model_scope_color_updated" },
    );
    await vehicleColorsCollection.createIndex(
      { brand: 1, model: 1, activeColorCount: -1, updatedAt: -1 },
      { name: "vehicle_colors_v2_brand_model_active_updated" },
    );
    await vehicleColorsCollection.createIndex(
      { brand_slug: 1, model_slug: 1, activeColorCount: -1, updatedAt: -1 },
      { name: "vehicle_colors_v2_slug_active_updated" },
    );
    await Vehicle.collection.createIndex(
      { brand_normalized: 1, model_normalized: 1, city: 1, is_discontinued: 1, ex_showroom: 1 },
      { name: "vehicle_popular_price_city_exact" },
    );
    await Vehicle.collection.createIndex(
      { brand_normalized: 1, model_normalized: 1, is_discontinued: 1, ex_showroom: 1 },
      { name: "vehicle_popular_price_exact" },
    );
    const monthlySalesCollection = mongoose.connection.db.collection("monthly_car_sales");
    await monthlySalesCollection.createIndex(
      { source: 1, month: -1, rank: 1 },
      { name: "monthly_sales_source_month_rank" },
    );
    const vehicleColorsIndexes = await vehicleColorsCollection.indexes();
    const monthlySalesIndexes = await monthlySalesCollection.indexes();

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
      "vehicle_colors_v2 active indexes:",
      vehicleColorsIndexes.map((idx) => idx.name),
    );
    console.log(
      "monthly_car_sales active indexes:",
      monthlySalesIndexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure Vehicle indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
