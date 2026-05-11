import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleOverviewTool = createNewCarsToolStub({
  toolName: "vehicle_overview",
  canvasType: NEW_CAR_CANVAS_TYPES.OVERVIEW,
});

export default runVehicleOverviewTool;
