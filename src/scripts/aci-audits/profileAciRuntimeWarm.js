import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import {
  prewarmAciAssistRuntime,
  getAciAssistRuntimePrewarmState,
} from "../../services/aiAgent/aiAgent.runtimePrewarm.js";

await connectDB();

const prewarm = await prewarmAciAssistRuntime({ force: true });

console.log("[ACI warm profile] prewarm:", JSON.stringify({
  status: prewarm.status,
  durationMs: prewarm.durationMs,
  error: prewarm.error,
  results: prewarm.results,
}, null, 2));

console.log("[ACI warm profile] state:", JSON.stringify(
  getAciAssistRuntimePrewarmState(),
  null,
  2,
));

// Import after prewarm so the existing profiler runs in the same Node process
// with resolver/entity/hint caches already hot.
await import("./profileAciRuntime.js");

// profileAciRuntime.js may manage its own disconnect.
// This guard keeps the wrapper safe either way.
if (mongoose.connection.readyState === 1) {
  await mongoose.disconnect();
}
