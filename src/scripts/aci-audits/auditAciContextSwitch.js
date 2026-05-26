import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";

await connectDB();

const baseContext = {
  selectedVehicle: {
    make: "Hyundai",
    brand: "Hyundai",
    model: "Verna",
    variant: "",
    city: "new-delhi",
  },
  anchorMake: "Hyundai",
  anchorModel: "Verna",
  anchorVariant: "",
  anchorCity: "new-delhi",
};

const tests = [
  {
    id: "switch-price-creta-from-verna-context",
    message: "Show Creta pricelist",
    context: baseContext,
  },
  {
    id: "switch-colors-seltos-from-verna-context",
    message: "Show Seltos colors",
    context: baseContext,
  },
  {
    id: "switch-emi-city-from-verna-context",
    message: "EMI for Honda City",
    context: baseContext,
  },
  {
    id: "switch-feature-thar-from-verna-context",
    message: "Does Thar have sunroof?",
    context: baseContext,
  },
  {
    id: "no-context-creta",
    message: "Show Creta pricelist",
    context: {},
  },
];

for (const test of tests) {
  const start = Date.now();

  const response = await chatWithAgent({
    message: test.message,
    context: test.context,
    user: { id: "aci-context-audit", role: "admin" },
  });

  const selected = response?.contextPatch?.selectedVehicle || {};
  const meta = response?.runtimeResultsMeta || [];

  console.log("\n==============================");
  console.log(test.id);
  console.log("==============================");
  console.log("message:", test.message);
  console.log("durationMs:", Date.now() - start);
  console.log("intent:", response?.intent);
  console.log("title:", response?.title);
  console.log("answer:", response?.answer);
  console.log("anchor:", {
    anchorMake: response?.contextPatch?.anchorMake,
    anchorModel: response?.contextPatch?.anchorModel,
    anchorVariant: response?.contextPatch?.anchorVariant,
    anchorCity: response?.contextPatch?.anchorCity,
  });
  console.log("selectedVehicle:", {
    make: selected.make || selected.brand || "",
    model: selected.model || "",
    variant: selected.variant || selected.selectedVariant || "",
    city: selected.city || selected.citySlug || "",
  });
  console.log("topLevelSource:", {
    source: response?.source || "",
    dataSource: response?.dataSource || "",
    modulesChecked: response?.modulesChecked || [],
    sourceTransparency: response?.sourceTransparency || {},
  });

  console.log("runtimeMeta:", meta.map((item) => ({
    tool: item.tool,
    matched: item.matched,
    source: item.source,
    dataSource: item.dataSource,
    modulesChecked: item.modulesChecked || [],
    error: item.error,
  })));
}

await mongoose.disconnect();
