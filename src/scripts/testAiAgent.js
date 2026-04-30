import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import { AI_AGENT_FIELD_MAPS } from "../services/aiAgent/aiAgent.fieldMaps.js";
import { routeAiAgentIntent } from "../services/aiAgent/aiAgent.intentRouter.js";
import { getToolForIntent } from "../services/aiAgent/aiAgent.toolRegistry.js";

dotenv.config();

const adminUser = { _id: "000000000000000000000000", role: "admin", name: "AI Agent Test" };

const baseTests = [
  { query: "Verna pricelist", expectedIntent: "vehicle_pricelist", expectedWidgets: ["vehicle_pricelist"], forbiddenIntent: "customer_360", expectedPhysicalCollections: ["vehicles"] },
  { query: "Show colors of Verna", expectedIntent: "vehicle_colors", expectedWidgets: ["vehicle_colors"], forbiddenIntent: "vehicle_pricelist", expectedPhysicalCollections: ["vehicle_colors"] },
  { query: "Does Verna SX have sunroof?", expectedIntent: "vehicle_feature_answer", expectedWidgets: ["vehicle_feature_answer"], forbiddenIntent: "customer_360", expectedPhysicalCollections: ["vehicle_features"] },
  { query: "Approved but not disbursed cases", expectedIntent: "loan_disbursal_report", requiredWidget: "loan_disbursal_report", forbiddenIntent: "customer_360", requireRecords: true, expectedPhysicalCollections: ["loans"] },
  { query: "Latest insurance of Rahul 4577", expectedIntent: "latest_insurance", forbiddenIntent: "customer_360", expectedPhysicalCollections: ["insurancecases"] },
  { query: "Customer 360 Rahul", expectedIntent: "customer_360", allowedWidgets: ["customer_360", "ambiguity", "unavailable_notice"] },
  { query: "Random unclear query", expectedIntent: "generic_search", expectedWidgets: ["unavailable_notice"], forbiddenIntent: "customer_360" },
  { query: "Find Vinod Kumar Jha", expectedIntent: "customer_lookup", forbiddenIntent: "customer_360", allowedWidgets: ["customer_card", "records_table", "unavailable_notice"], expectedPhysicalCollections: ["customers"] },
  { query: "Customer 360 Vinod Kumar Jha", expectedIntent: "customer_360", allowedWidgets: ["customer_360", "ambiguity", "unavailable_notice"], expectedPhysicalCollections: ["customers"] },
  { query: "Customers with KYC pending", expectedIntent: "customer_data_quality_report", expectedWidgets: ["customer_data_quality_report"], expectedPhysicalCollections: ["customers"] },
  { query: "Loan status of LN-2026-0001", expectedIntent: "loan_status", allowedWidgets: ["loan_case_card", "unavailable_notice"], expectedPhysicalCollections: ["loans"] },
  { query: "Pending approval cases", expectedIntent: "loan_pending_approval_report", expectedWidgets: ["count_summary"], expectedPhysicalCollections: ["loans"] },
  { query: "Total business this month", expectedIntent: "loan_business_report", expectedWidgets: ["loan_business_report"], expectedPhysicalCollections: ["loans"], assert: ({ response }) => response.widgets?.[0]?.summary?.cashCarLogic?.length },
  { query: "Cash car business this month", expectedIntent: "loan_business_report", expectedWidgets: ["loan_business_report"], expectedPhysicalCollections: ["loans"], assert: ({ response }) => response.widgets?.[0]?.summary?.cashCarLogic?.length },
  { query: "Loan closure 7077", expectedIntent: "loan_closure_pos", allowedWidgets: ["loan_closure_card", "ambiguity", "unavailable_notice"], expectedPhysicalCollections: ["loans"] },
  { query: "Vehicle 360 6300", expectedIntent: "vehicle_360", allowedWidgets: ["vehicle_360", "ambiguity", "unavailable_notice"], expectedPhysicalCollections: ["vehicle_master_records"] },
  { query: "DL4CAZ6300", expectedIntent: "vehicle_registration_search", allowedWidgets: ["vehicle_360", "ambiguity", "unavailable_notice"], expectedPhysicalCollections: ["vehicle_master_records"] },
  { query: "Seltos HTE price breakup", expectedIntent: "vehicle_price_breakup", allowedWidgets: ["vehicle_price_breakup", "vehicle_pricelist", "unavailable_notice"], expectedPhysicalCollections: ["vehicles"] },
  { query: "Show features of Hyundai Verna HX8 iVT", expectedIntent: "vehicle_features", allowedWidgets: ["vehicle_features", "unavailable_notice"], forbiddenIntent: "customer_360", expectedPhysicalCollections: ["vehicle_features"] },
  { query: "Show colors", context: { model: "verna" }, expectedIntent: "vehicle_colors", expectedWidgets: ["vehicle_colors"], expectedPhysicalCollections: ["vehicle_colors"] },
];

const physicalCollectionsFor = (tool) =>
  (tool?.collectionsUsed || []).map((key) => AI_AGENT_FIELD_MAPS[key]?.collectionName || key);

const widgetTypesFor = (response) => {
  if (response.ambiguity) return ["ambiguity", ...(response.widgets || []).map((widget) => widget.type).filter(Boolean)];
  return (response.widgets || []).map((widget) => widget.type).filter(Boolean);
};

const matchedCountFor = (response) =>
  (response.widgets || []).reduce((sum, widget) => {
    const count = widget?.summary?.total ?? widget?.data?.total ?? widget?.rows?.length ?? widget?.records?.length ?? 0;
    return sum + (Number(count) || 0);
  }, response.ambiguity?.options?.length || 0);

const hasRecords = (response) =>
  (response.widgets || []).some((widget) => (widget.rows?.length || widget.records?.length || 0) > 0);

const buildLine = ({ test, route, tool, response }) => {
  const widgetTypes = widgetTypesFor(response);
  const physicalCollections = physicalCollectionsFor(tool);
  const failures = [];

  if (test.expectedIntent && route.intent !== test.expectedIntent) failures.push(`intent expected ${test.expectedIntent}, got ${route.intent}`);
  if (test.forbiddenIntent && route.intent === test.forbiddenIntent) failures.push(`forbidden intent ${test.forbiddenIntent}`);
  if (route.structured && widgetTypes.length === 0) failures.push("structured intent returned blank widgetType");
  if (test.expectedWidgets?.length && !test.expectedWidgets.some((widget) => widgetTypes.includes(widget))) {
    failures.push(`expected widget ${test.expectedWidgets.join(" or ")}, got ${widgetTypes.join(", ") || "none"}`);
  }
  if (test.allowedWidgets?.length && !test.allowedWidgets.some((widget) => widgetTypes.includes(widget))) {
    failures.push(`allowed widgets ${test.allowedWidgets.join(", ")}, got ${widgetTypes.join(", ") || "none"}`);
  }
  if (test.requiredWidget && !widgetTypes.includes(test.requiredWidget)) failures.push(`missing required widget ${test.requiredWidget}`);
  if (test.requireRecords && !hasRecords(response)) failures.push("expected actual records, got count-only response");
  if (test.expectedPhysicalCollections?.length && !test.expectedPhysicalCollections.some((collection) => physicalCollections.includes(collection))) {
    failures.push(`expected physical collection ${test.expectedPhysicalCollections.join(" or ")}, got ${physicalCollections.join(", ") || "none"}`);
  }
  if (test.assert && !test.assert({ response, route, tool })) failures.push("custom assertion failed");

  return {
    query: test.query,
    intent: route.intent,
    entities: route.entities,
    selectedTool: tool?.intent || "generic_search",
    logicalAdapterUsed: tool?.collectionsUsed || route.collections || [],
    physicalCollectionUsed: physicalCollections,
    widgetType: widgetTypes[0] || "",
    widgetTypes,
    matchedCount: matchedCountFor(response),
    pass: failures.length === 0,
    failureReason: failures.join("; "),
  };
};

const selectedEntityTest = async () => {
  const loan = await Loan.findOne({
    $or: [
      { rc_redg_no: /7077$/i },
      { registrationNumber: /7077$/i },
      { vehicleRegNo: /7077$/i },
    ],
  }).lean();
  if (!loan) return null;
  return {
    query: "Loan closure 7077",
    expectedIntent: "loan_closure_pos",
    allowedWidgets: ["loan_closure_card", "unavailable_notice"],
    expectedPhysicalCollections: ["loans"],
    selectedEntity: {
      id: String(loan._id),
      entityType: "loan",
      customerName: loan.customerName,
      registrationNumber: loan.rc_redg_no || loan.registrationNumber || loan.vehicleRegNo,
      context: { loanId: loan.loanId },
    },
    assert: ({ response }) => {
      const data = response.widgets?.[0]?.data || {};
      return !data.id || data.id === String(loan._id);
    },
  };
};

const run = async () => {
  await connectDB();
  const dynamicTest = await selectedEntityTest();
  const tests = dynamicTest ? [...baseTests, dynamicTest] : baseTests;
  let failed = 0;
  for (const test of tests) {
    const route = routeAiAgentIntent({
      message: test.query,
      context: test.context || {},
      selectedEntity: test.selectedEntity,
    });
    const tool = getToolForIntent(route.intent);
    const response = await chatWithAgent({
      message: test.query,
      context: test.context || {},
      selectedEntity: test.selectedEntity,
      user: adminUser,
      debug: true,
    });
    const line = buildLine({ test, route, tool, response });
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
