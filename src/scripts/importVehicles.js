import XLSX from 'xlsx';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import connectDB from '../config/db.js';
import Vehicle from '../models/Vehicle.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/** Coerce to number for price fields */
const num = (val) => (val === '' || val == null ? 0 : Number(val));

/** Map one Excel row to vehicle doc; matches DB unique key: make, model, variant, fuel, city */
function rowToVehicle(row) {
  const make = (row.Make ?? row.make ?? row.Brand ?? row.brand ?? '').toString().trim();
  const model = (row.Model ?? row.model ?? row.Name ?? row.name ?? '').toString().trim();
  const variant = (row.Variant ?? row.variant ?? row.Version ?? row.version ?? 'Standard').toString().trim();
  const fuel = (row.Fuel ?? row['Fuel Type'] ?? row.FuelType ?? 'N/A').toString().trim() || 'N/A';
  const city = (row.City ?? row.city ?? 'Delhi').toString().trim() || 'Delhi';
  const onRoadPrice = num(row.OnRoadPrice ?? row['On-Road Price']);

  return {
    make,
    model,
    variant,
    fuel,
    city,
    exShowroom: num(row.ExShowroom ?? row['Ex-Showroom Price'] ?? row.ExShowroomPrice ?? row.Price),
    rto: num(row.RTO ?? row.rto),
    insurance: num(row.Insurance ?? row.insurance),
    otherCharges: num(row.OtherCharges ?? row['Other Charges']),
    onRoadPrice,
    on_road_price_cardekho: onRoadPrice,
    total_on_road_with_accessories: onRoadPrice,
    status: 'Active',
    isDiscontinued: false,
    createdFrom: 'EXCEL_IMPORT',
    importedAt: new Date(),
    // optional fields (schema is strict: false)
    fuelType: row.Fuel ?? row['Fuel Type'] ?? 'N/A',
    transmission: row.Transmission ?? row.transmission ?? 'N/A',
    bodyType: row['Body Type'] ?? row.BodyType ?? 'N/A',
    seatingCapacity: row['Seating Capacity'] ?? row.SeatingCapacity ?? null,
    engineCapacity: row['Engine Capacity'] ?? row.EngineCapacity ?? row.Engine ?? null,
    maxPower: row['Max Power'] ?? row.MaxPower ?? row.Power ?? 'N/A',
    maxTorque: row['Max Torque'] ?? row.MaxTorque ?? row.Torque ?? 'N/A',
    mileage: row.Mileage ?? row.mileage ?? 'N/A',
    length: row.Length ?? row.length ?? null,
    width: row.Width ?? row.width ?? null,
    height: row.Height ?? row.height ?? null,
    wheelbase: row.Wheelbase ?? row.wheelbase ?? null,
    launchYear: row['Launch Year'] ?? row.LaunchYear ?? row.Year ?? null,
  };
}

const importVehicles = async () => {
  try {
    await connectDB();
    console.log('📦 Connected to MongoDB');

    const filePath = path.join(__dirname, '../../cardekho_ncr_prices.xlsx');
    console.log('📄 Reading Excel file:', filePath);

    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);
    console.log(`📊 Found ${data.length} rows in Excel file`);

    if (data.length === 0) {
      console.log('❌ No data found in Excel file');
      process.exit(1);
    }

    console.log('📋 Column names:', Object.keys(data[0]));

    // Build set of existing keys (make|model|variant|fuel|city) from DB
    const existing = await Vehicle.find({}).select('make model variant fuel city').lean();
    const existingKeys = new Set(
      existing.map((v) => [v.make, v.model, v.variant, v.fuel || '', v.city || ''].join('|'))
    );
    console.log(`🗄️  Database has ${existingKeys.size} vehicle(s). Will seed only rows not in DB.`);

    let imported = 0;
    let skipped = 0;
    let errors = 0;

    for (const row of data) {
      try {
        const v = rowToVehicle(row);
        // Require basic identity only
        if (!v.make || !v.model || !v.variant) {
          skipped++;
          continue;
        }
        const key = [v.make, v.model, v.variant, v.fuel || '', v.city || ''].join('|');
        if (existingKeys.has(key)) {
          skipped++;
          continue;
        }

        await Vehicle.create(v);
        existingKeys.add(key);
        imported++;
        if (imported % 100 === 0) {
          console.log(`✅ Imported ${imported} vehicles...`);
        }
      } catch (error) {
        errors++;
        console.error(`❌ Error importing row:`, error.message);
      }
    }

    console.log('\n📊 Import Summary:');
    console.log(`✅ New vehicles seeded: ${imported}`);
    console.log(`⏭️  Skipped (already in DB or invalid): ${skipped}`);
    console.log(`❌ Errors: ${errors}`);
    console.log('\n🎉 Vehicle import completed!');

    process.exit(0);
  } catch (error) {
    console.error('❌ Error importing vehicles:', error);
    process.exit(1);
  }
};

importVehicles();
