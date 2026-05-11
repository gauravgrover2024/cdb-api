import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleCompareTool = createNewCarsToolStub({
  toolName: "vehicle_compare",
  canvasType: NEW_CAR_CANVAS_TYPES.COMPARISON,
});

export default runVehicleCompareTool;
