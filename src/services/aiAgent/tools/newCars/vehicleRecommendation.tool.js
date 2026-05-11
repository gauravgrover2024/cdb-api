import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleRecommendationTool = createNewCarsToolStub({
  toolName: "vehicle_recommendation",
  canvasType: NEW_CAR_CANVAS_TYPES.RECOMMENDATION,
});

export default runVehicleRecommendationTool;
