import mongoose from "mongoose";
import dotenv from "dotenv";
import Vehicle from "./src/models/Vehicle.js";

dotenv.config();

async function main() {
  await mongoose.connect(process.env.MONGO_URI);
  const count = await Vehicle.countDocuments();
  console.log("Total vehicles in DB:", count);
  const sample = await Vehicle.findOne();
  console.log("Sample vehicle:", sample);
  const sampleDelhi = await Vehicle.findOne({ city: "Delhi" });
  console.log("Sample Delhi vehicle:", sampleDelhi?.variant);
  const sampleEmpty = await Vehicle.findOne({ city: "" });
  console.log("Sample Empty city vehicle:", sampleEmpty?.variant);
  
  await mongoose.disconnect();
}
main().catch(console.error);
