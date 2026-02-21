import dotenv from "dotenv";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";
import VehicleFeature from "../models/VehicleFeature.js";

// copy of normalizeModelForJoin from controller
const normalizeModelForJoin = (brand, model) => {
  if (!model) return "";
  const b = (brand || "").trim().toLowerCase();
  const m = String(model).trim().toLowerCase();
  const prefix = b + " ";
  return m.startsWith(prefix) ? m.slice(prefix.length) : m;
};

dotenv.config();

const run = async () => {
  await connectDB();

  const featureDocs = await VehicleFeature.find({});
  const vehicles = await Vehicle.find({ city: "New-Delhi" });

  const vehicleKeys = new Set(
    vehicles.map((v) => {
      const brand = (v.brand || v.make || "").trim().toLowerCase();
      const model = normalizeModelForJoin(brand, v.model);
      const variant = (v.variant || "").trim().toLowerCase();
      return `${brand}|${model}|${variant}`;
    }),
  );

  let matched = 0;
  let missing = 0;

  featureDocs.forEach((f) => {
    const brand = (f.brand || "").trim().toLowerCase();
    const model = (f.model || "").trim().toLowerCase();
    const variant = (f.variant || "").trim().toLowerCase();
    const key = `${brand}|${model}|${variant}`;
    if (vehicleKeys.has(key)) matched += 1;
    else missing += 1;
  });

  console.log("Total feature docs:", featureDocs.length);
  console.log("New-Delhi vehicle docs:", vehicles.length);
  console.log("Features with matching New-Delhi vehicle:", matched);
  console.log("Features without New-Delhi vehicle:", missing);

  process.exit(0);
};

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
