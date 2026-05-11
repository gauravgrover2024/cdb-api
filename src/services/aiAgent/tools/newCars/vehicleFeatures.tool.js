import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleFeaturesTool = createNewCarsToolStub({
  toolName: "vehicle_features",
  canvasType: NEW_CAR_CANVAS_TYPES.FEATURES,
});

export default runVehicleFeaturesTool;
