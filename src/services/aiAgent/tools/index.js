import { runVehiclePricelistTool } from "./vehiclePricelist.tool.js";

/**
 * ACI Assist V2 tool registry.
 *
 * This is NOT the old aiAgent.toolRegistry.js.
 * This registry maps planner tool names to new V2 data-only tool files.
 */

export const ACI_V2_TOOL_RUNNERS = {
  vehicle_pricelist: runVehiclePricelistTool,
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
