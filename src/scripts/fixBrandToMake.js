import dotenv from "dotenv";
import connectDB from "../src/config/db.js";
import Vehicle from "../src/models/Vehicle.js";

dotenv.config();

async function fixBrandField() {
  await connectDB();

  const result = await Vehicle.updateMany(
    { brand: { $exists: true, $ne: null } },
    [{ $set: { make: "$brand" } }, { $unset: "brand" }],
  );

  console.log(`✅ Fixed ${result.modifiedCount} documents: brand → make`);
  process.exit(0);
}

fixBrandField().catch(console.error);
