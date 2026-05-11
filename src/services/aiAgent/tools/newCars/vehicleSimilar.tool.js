import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleSimilarTool = createNewCarsToolStub({
  toolName: "vehicle_similar",
  canvasType: NEW_CAR_CANVAS_TYPES.SIMILAR,
});

export default runVehicleSimilarTool;
