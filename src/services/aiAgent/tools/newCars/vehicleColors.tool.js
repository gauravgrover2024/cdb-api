import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleColorsTool = createNewCarsToolStub({
  toolName: "vehicle_colors",
  canvasType: NEW_CAR_CANVAS_TYPES.COLORS,
  dataKey: "colors",
});

export default runVehicleColorsTool;
