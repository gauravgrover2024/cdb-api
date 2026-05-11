import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

export const runQuotationLeadTool = createNewCarsToolStub({
  toolName: "quotation_lead",
  canvasType: NEW_CAR_CANVAS_TYPES.QUOTATION,
});

export default runQuotationLeadTool;
