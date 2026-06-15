import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import csv from 'csv-parser';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import Vehicle from '../models/Vehicle.js';
import { vehicleNormalizationFields } from '../utils/vehicleDatasetNormalizer.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function syncVehicles() {
  await mongoose.connect(process.env.MONGO_URI);
  console.log('📦 Connected to MongoDB');

  const filePath = path.join(__dirname, '../../carwale_vehicle_year_rows.csv');
  console.log(`📄 Reading CSV file: ${filePath}`);

  const results = [];
  
  await new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve())
      .on('error', (err) => reject(err));
  });

  console.log(`📊 Found ${results.length} rows in CSV file`);

  let bulkOps = [];
  let processed = 0;

  for (let i = 0; i < results.length; i++) {
    const row = results[i];
    
    // Basic validation
    if (!row.make || !row.model || !row.variant) {
      continue;
    }

    const make = row.make.trim();
    const model = row.model.trim();
    const variant = row.variant.trim();
    const fuel = (row.fuel_type || 'N/A').trim();
    const city = ''; 

    const payload = {
      make,
      model,
      variant,
      fuel,
      city,
      ex_showroom: row.ex_showroom_price ? Number(row.ex_showroom_price) : 0,
      exShowroom: row.ex_showroom_price ? Number(row.ex_showroom_price) : 0,
      fuel_type: fuel,
      status: row.is_active === 'True' ? 'Active' : 'Inactive',
      is_discontinued: row.is_discontinued === 'True',
      scrape_timestamp: new Date().toISOString(),
      raw_price_json: {
        carwale_make_slug: row.carwale_make_slug,
        carwale_model_slug: row.carwale_model_slug,
        carwale_version_id: row.carwale_version_id,
        carwale_version_slug: row.carwale_version_slug,
        carwale_model_url: row.carwale_model_url,
        range_key: row.range_key,
        source: row.source,
        confidence: row.confidence
      },
      features: {
        transmission: row.transmission,
        cc: row.cc,
        mileage: row.mileage,
        seating_capacity: row.seating_capacity,
        body_type: row.body_type,
        year: row.year,
        start_year: row.start_year,
        end_year: row.end_year,
        model_generation: row.model_generation
      }
    };

    const normalizedPayload = {
      ...payload,
      ...vehicleNormalizationFields(payload)
    };

    // We use the fields that make up the unique index to avoid E11000 duplicate key error.
    // The previous error was on brand_1_model_1_variant_1_city_1
    const filter = {
      brand: normalizedPayload.brand || null,
      model: normalizedPayload.model,
      variant: normalizedPayload.variant,
      city: normalizedPayload.city || ""
    };

    bulkOps.push({
      updateOne: {
        filter,
        update: { $set: normalizedPayload },
        upsert: true
      }
    });

    processed++;

    // Execute in batches of 1000
    if (bulkOps.length === 1000) {
      await Vehicle.bulkWrite(bulkOps, { ordered: false });
      bulkOps = [];
      console.log(`✅ Processed ${processed} rows...`);
    }
  }

  // Execute remaining
  if (bulkOps.length > 0) {
    await Vehicle.bulkWrite(bulkOps, { ordered: false });
    console.log(`✅ Processed ${processed} rows...`);
  }

  console.log('\\n📊 Synchronization Completed!');
  await mongoose.disconnect();
  console.log('🔌 Disconnected from MongoDB');
}

syncVehicles().catch((err) => {
  console.error(err);
  process.exit(1);
});
