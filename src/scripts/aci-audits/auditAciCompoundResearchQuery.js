import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";

const message =
  "Compare Hyundai Creta and Kia Seltos on sunroof, ABS, 6 airbags, ADAS and 360 camera, and show their colour options and price lists in Noida";

const main = async () => {
  await connectDB();
  const response = await chatWithAgent({
    message,
    context: {},
    conversationId: "audit-compound-research-query",
    userId: "audit",
  });

  const failures = [];
  const features = response.data?.features || response.features || [];
  const secondary = response.secondaryResponses || [];
  const secondaryIntents = secondary.map((item) => item.intent);
  const actions = response.actions || [];
  const answer = String(response.answer || "").toLowerCase();

  if (response.intent !== "vehicle_feature_comparison") {
    failures.push(`Expected vehicle_feature_comparison, got ${response.intent || "empty"}`);
  }
  if (features.length !== 5) failures.push(`Expected 5 features, got ${features.length}`);
  for (const feature of ["sunroof", "anti-lock", "6 airbags", "adas", "360 camera"]) {
    if (!answer.includes(feature)) failures.push(`Answer omitted ${feature}`);
  }
  if (secondaryIntents.filter((intent) => intent === "vehicle_colors").length !== 2) {
    failures.push("Expected two colour results");
  }
  if (secondaryIntents.filter((intent) => intent === "vehicle_pricelist").length !== 2) {
    failures.push("Expected two price-list results");
  }
  for (const label of [
    "open hyundai creta colours",
    "open kia seltos colours",
    "open hyundai creta price list",
    "open kia seltos price list",
  ]) {
    if (!actions.some((action) => String(action.label || "").toLowerCase() === label)) {
      failures.push(`Missing related action: ${label}`);
    }
  }
  if (actions.some((action) => action.type === "lead")) {
    failures.push("Research-stage compound answer exposed a lead action too early");
  }
  if (response.journeyGuidance?.stage !== "evaluation") {
    failures.push(`Expected evaluation journey stage, got ${response.journeyGuidance?.stage || "empty"}`);
  }
  if (response.journeyGuidance?.leadMode !== "hidden") {
    failures.push(`Expected hidden lead mode, got ${response.journeyGuidance?.leadMode || "empty"}`);
  }

  const result = {
    suite: "ACI compound research query audit",
    pass: failures.length === 0,
    failures,
    summary: {
      intent: response.intent,
      title: response.title,
      answer: response.answer,
      featureCount: features.length,
      secondaryIntents,
      actionLabels: actions.map((action) => action.label),
      journeyGuidance: response.journeyGuidance,
    },
  };

  console.log(JSON.stringify(result, null, 2));
  if (failures.length) process.exitCode = 1;
};

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
