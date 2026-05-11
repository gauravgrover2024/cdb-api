import { runVehiclePricelistTool } from "./vehiclePricelist.tool.js";
import {
  runQuotationLeadTool,
  runVehicleColorsTool,
  runVehicleCompareTool,
  runVehicleEmiTool,
  runVehicleFeaturesTool,
  runVehicleOffersTool,
  runVehicleOverviewTool,
  runVehicleOwnershipCostTool,
  runVehiclePricelistNewCarsTool,
  runVehicleRecommendationTool,
  runVehicleSafetyRankingTool,
  runVehicleSimilarTool,
  runVehicleVariantAdvisorTool,
} from "./newCars/index.js";

/**
 * ACI Assist V2 tool registry.
 *
 * This is NOT the old aiAgent.toolRegistry.js.
 * This registry maps planner tool names to new V2 data-only tool files.
 */

export const ACI_V2_TOOL_RUNNERS = {
  vehicle_pricelist: runVehiclePricelistTool,
  vehicle_overview: runVehicleOverviewTool,
  vehicle_colors: runVehicleColorsTool,
  vehicle_similar: runVehicleSimilarTool,
  vehicle_safety_ranking: runVehicleSafetyRankingTool,
  vehicle_recommendation: runVehicleRecommendationTool,
  vehicle_variant_advisor: runVehicleVariantAdvisorTool,
  vehicle_compare: runVehicleCompareTool,
  vehicle_emi: runVehicleEmiTool,
  vehicle_features: runVehicleFeaturesTool,
  vehicle_ownership_cost: runVehicleOwnershipCostTool,
  vehicle_offers: runVehicleOffersTool,
  quotation_lead: runQuotationLeadTool,

  // Planner/runtime compatibility aliases.
  vehicle_feature_lookup: runVehicleFeaturesTool,
  vehicle_recommend: runVehicleRecommendationTool,
  aci_lead_capture: runQuotationLeadTool,
  vehicle_pricelist_v2: runVehiclePricelistNewCarsTool,
};

export const getAciV2ToolRunner = (tool = "") =>
  ACI_V2_TOOL_RUNNERS[tool] || null;

export const runAciV2Tool = async ({
  toolPlan = {},
  plan = {},
  context = {},
  userMessage = "",
  runtimeHints = {},
  index = 0,
} = {}) => {
  const runner = getAciV2ToolRunner(toolPlan.tool);

  if (!runner) {
    return {
      tool: toolPlan.tool || "unknown",
      rows: [],
      matched: 0,
      count: 0,
      modulesChecked: [`missing_v2_tool:${toolPlan.tool || "unknown"}`],
      dataSource: "missing_v2_tool",
      missingTool: true,
    };
  }

  return runner({
    toolPlan,
    plan,
    context,
    userMessage,
    runtimeHints,
    index,
  });
};

export default ACI_V2_TOOL_RUNNERS;
