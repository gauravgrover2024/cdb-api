import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleVariantAdvisorTool = createNewCarsToolStub({
  toolName: "vehicle_variant_advisor",
  canvasType: NEW_CAR_CANVAS_TYPES.VARIANT_ADVISOR,
});

export default runVehicleVariantAdvisorTool;
