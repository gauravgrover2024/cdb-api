import { runVehiclePricelistTool as runCurrentPricelistTool } from "../vehiclePricelist.tool.js";

export const runVehiclePricelistNewCarsTool = async (args = {}) =>
  runCurrentPricelistTool(args);

export default runVehiclePricelistNewCarsTool;
