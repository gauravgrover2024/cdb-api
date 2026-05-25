import "dotenv/config";
import mongoose from "mongoose";

import {
  prewarmAciAssistRuntime,
  getAciAssistRuntimePrewarmState,
} from "../../services/aiAgent/aiAgent.runtimePrewarm.js";

const uri = process.env.MONGO_URI;

if (!uri) {
  throw new Error("MONGO_URI is not configured.");
}

// Important:
// Connect with monitorCommands before prewarm so the existing profiler can attach
// command listeners to a monitoring-enabled Mongo client in the same process.
mongoose.set("strictQuery", false);

if (mongoose.connection.readyState !== 1) {
  await mongoose.connect(uri, {
    monitorCommands: true,
    serverSelectionTimeoutMS: 10000,
  });
}

const prewarm = await prewarmAciAssistRuntime({ force: true });

console.log(
  "[ACI warm profile] prewarm:",
  JSON.stringify(
    {
      status: prewarm.status,
      durationMs: prewarm.durationMs,
      error: prewarm.error,
      results: prewarm.results,
    },
    null,
    2,
  ),
);

console.log(
  "[ACI warm profile] state:",
  JSON.stringify(getAciAssistRuntimePrewarmState(), null, 2),
);

// Import after prewarm so profileAciRuntime.js runs in the same Node process
// with resolver/entity/hint caches already hot.
//
// Do NOT disconnect here.
// profileAciRuntime.js owns the final mongoose.disconnect() after it finishes.
await import("./profileAciRuntime.js");
