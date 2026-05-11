import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runVehicleOffersTool = createNewCarsToolStub({
  toolName: "vehicle_offers",
  canvasType: NEW_CAR_CANVAS_TYPES.OFFERS,
});

export default runVehicleOffersTool;
