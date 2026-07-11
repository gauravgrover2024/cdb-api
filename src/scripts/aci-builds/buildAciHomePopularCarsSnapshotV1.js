#!/usr/bin/env node

import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

process.env.ACI_DISABLE_VEHICLE_CACHE_AUTOWARM = "1";

const cityArg = process.argv.find((arg) => arg.startsWith("--city="));
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const city = cityArg ? cityArg.slice("--city=".length) : "new-delhi";
const limit = Math.min(
  Math.max(Number(limitArg?.slice("--limit=".length) || 25), 1),
  25,
);

await connectDB();

try {
  const { refreshPopularCarsSnapshot } = await import(
    "../../controllers/vehicleController.js"
  );
  const startedAt = Date.now();
  const payload = await refreshPopularCarsSnapshot({ city, limit });
  console.log(
    JSON.stringify(
      {
        suite: "ACI home popular-cars snapshot build v1",
        ok: true,
        city,
        limit,
        count: payload.count,
        source: payload.source,
        month: payload.month,
        durationMs: Date.now() - startedAt,
      },
      null,
      2,
    ),
  );
} finally {
  await mongoose.disconnect();
}
