import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleSafetyRankingTool = createNewCarsToolStub({
  toolName: "vehicle_safety_ranking",
  canvasType: NEW_CAR_CANVAS_TYPES.SAFETY,
});

export default runVehicleSafetyRankingTool;
