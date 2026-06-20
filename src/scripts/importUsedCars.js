import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import csvParser from "csv-parser";
import dotenv from "dotenv";
import connectDB from "../config/db.js";
import UsedCar from "../models/UsedCar.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const cleanText = (value) => {
  const text = String(value ?? "").trim();
  if (!text || text.toLowerCase() === "nan" || text.toLowerCase() === "null") return "";
  return text;
};

const coerceNumber = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const num = Number(value);
  return isNaN(num) ? null : num;
};

const coerceBoolean = (value) => {
  if (value === undefined || value === null) return false;
  const str = String(value).trim().toLowerCase();
  return str === "true" || str === "1" || str === "yes";
};

async function importUsedCars() {
  try {
    await connectDB();
    console.log("📦 Connected to MongoDB");

    const filePath = path.join(__dirname, "../../carwale_vehicle_year_rows.csv");
    console.log("📄 Reading CSV file:", filePath);

    if (!fs.existsSync(filePath)) {
      console.error(`❌ CSV File not found at path: ${filePath}`);
      process.exit(1);
    }

    let count = 0;
    let ops = [];
    const BATCH_SIZE = 1000;

    const stream = fs.createReadStream(filePath)
      .pipe(csvParser());

    console.log("🚀 Starting parsing and streaming...");

    for await (const row of stream) {
      const make = cleanText(row.make);
      const model = cleanText(row.model);
      const variant = cleanText(row.variant);
      const year = coerceNumber(row.year);

      // Make, model, variant, and year are required identity fields
      if (!make || !model || !variant || !year) {
        continue;
      }

      const parsedDoc = {
        make,
        model,
        variant,
        year,
        fuel_type: cleanText(row.fuel_type),
        transmission: cleanText(row.transmission),
        cc: coerceNumber(row.cc),
        mileage: cleanText(row.mileage),
        seating_capacity: cleanText(row.seating_capacity),
        body_type: cleanText(row.body_type),
        is_active: coerceBoolean(row.is_active),
        is_discontinued: coerceBoolean(row.is_discontinued),
        start_year: coerceNumber(row.start_year),
        end_year: coerceNumber(row.end_year),
        model_generation: cleanText(row.model_generation),
        carwale_make_slug: cleanText(row.carwale_make_slug),
        carwale_model_slug: cleanText(row.carwale_model_slug),
        carwale_version_id: coerceNumber(row.carwale_version_id),
        ex_showroom_price: coerceNumber(row.ex_showroom_price),
      };

      ops.push({
        updateOne: {
          filter: { make, model, variant, year },
          update: { $set: parsedDoc },
          upsert: true,
        }
      });

      if (ops.length >= BATCH_SIZE) {
        const result = await UsedCar.bulkWrite(ops, { ordered: false });
        count += ops.length;
        console.log(`✅ Processed ${count} records (Upserted: ${result.upsertedCount}, Modified: ${result.modifiedCount})`);
        ops = [];
      }
    }

    // Insert remaining operations
    if (ops.length > 0) {
      const result = await UsedCar.bulkWrite(ops, { ordered: false });
      count += ops.length;
      console.log(`✅ Processed final batch. Total records processed: ${count} (Upserted: ${result.upsertedCount}, Modified: ${result.modifiedCount})`);
    }

    console.log("🎉 Seeding completed successfully!");
    process.exit(0);
  } catch (error) {
    console.error("❌ Seeding failed:", error);
    process.exit(1);
  }
}

importUsedCars();
