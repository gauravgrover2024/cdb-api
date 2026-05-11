import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleEmiTool = createNewCarsToolStub({
  toolName: "vehicle_emi",
  canvasType: NEW_CAR_CANVAS_TYPES.EMI,
});

export default runVehicleEmiTool;
