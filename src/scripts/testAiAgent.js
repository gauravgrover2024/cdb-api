import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import { routeAiAgentIntent } from "../services/aiAgent/aiAgent.intentRouter.js";
import { getToolForIntent } from "../services/aiAgent/aiAgent.toolRegistry.js";

dotenv.config();

const tests = [
  {
    query: "Verna pricelist",
    expectedIntent: "vehicle_pricelist",
    expectedWidget: "vehicle_pricelist",
    forbiddenIntent: "customer_360",
  },
  {
    query: "Show colors of Verna",
    expectedIntent: "vehicle_colors",
    expectedWidget: "vehicle_colors",
    forbiddenIntent: "vehicle_pricelist",
  },
  {
    query: "Does Verna SX have sunroof?",
    expectedIntent: "vehicle_feature_answer",
    expectedWidget: "variant_feature_availability",
    forbiddenIntent: "customer_360",
  },
  {
    query: "Approved but not disbursed cases",
    expectedIntent: "loan_disbursal_report",
    forbiddenIntent: "customer_360",
  },
  {
    query: "Latest insurance of Rahul 4577",
    expectedIntent: "latest_insurance",
    forbiddenIntent: "customer_360",
  },
  {
    query: "Customer 360 Rahul",
    expectedIntent: "customer_360",
  },
  {
    query: "Random unclear query",
    forbiddenIntent: "customer_360",
  },
];

const adminUser = { _id: "000000000000000000000000", role: "admin", name: "AI Agent Test" };

const resultLine = ({ query, route, tool, response, test }) => {
  const widgetType = response.widgets?.[0]?.type || "";
  const matchedCount =
    response.widgets?.[0]?.summary?.total ??
    response.widgets?.[0]?.data?.total ??
    response.widgets?.[0]?.rows?.length ??
    0;
  const failures = [];
  if (test.expectedIntent && route.intent !== test.expectedIntent) {
    failures.push(`intent expected ${test.expectedIntent}, got ${route.intent}`);
  }
  if (test.forbiddenIntent && route.intent === test.forbiddenIntent) {
    failures.push(`forbidden intent ${test.forbiddenIntent}`);
  }
  if (test.expectedWidget && widgetType !== test.expectedWidget) {
    failures.push(`widget expected ${test.expectedWidget}, got ${widgetType || "none"}`);
  }

  return {
    query,
    detectedIntent: route.intent,
    entities: route.entities,
    selectedTool: tool?.intent || "generic_search",
    collectionsModulesUsed: tool?.collectionsUsed || route.collections || [],
    widgetType,
    matchedCount,
    pass: failures.length === 0,
    failureReason: failures.join("; "),
  };
};

const run = async () => {
  await connectDB();
  let failed = 0;
  for (const test of tests) {
    const route = routeAiAgentIntent({ message: test.query });
    const tool = getToolForIntent(route.intent);
    const response = await chatWithAgent({ message: test.query, user: adminUser, debug: true });
    const line = resultLine({ query: test.query, route, tool, response, test });
    if (!line.pass) failed += 1;
    console.log(JSON.stringify(line, null, 2));
  }
  await mongoose.connection.close();
  if (failed) {
    console.error(`ACI Assist harness failed ${failed} test(s).`);
    process.exit(1);
  }
  console.log("ACI Assist harness passed.");
};

run().catch(async (error) => {
  console.error("ACI Assist harness crashed:", error);
  await mongoose.connection.close().catch(() => {});
  process.exit(1);
});
