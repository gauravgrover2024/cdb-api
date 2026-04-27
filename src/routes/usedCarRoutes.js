import express from "express";
import {
  bulkAssignUsedCarLeads,
  clearUsedCarLeads,
  createUsedCarLead,
  deleteUsedCarLead,
  downloadUsedCarInspectionReportPdf,
  getUsedCarLeadById,
  importUsedCarLeads,
  listBackgroundCheckLeads,
  listNegotiationLeads,
  listUsedCarLeads,
  patchUsedCarLeadWorkflow,
  saveBackgroundCheck,
  saveNegotiation,
  updateUsedCarLead,
} from "../controllers/usedCarLeadController.js";

const router = express.Router();

router.get("/leads", listUsedCarLeads);
router.get("/background-check/leads", listBackgroundCheckLeads);
router.get("/negotiation/leads", listNegotiationLeads);
router.post("/leads", createUsedCarLead);
router.post("/leads/import", importUsedCarLeads);
router.post("/leads/clear", clearUsedCarLeads);
router.post("/leads/assign", bulkAssignUsedCarLeads);
router.get("/leads/:id", getUsedCarLeadById);
router.get("/leads/:id/inspection/report.pdf", downloadUsedCarInspectionReportPdf);
router.put("/leads/:id", updateUsedCarLead);
router.put("/leads/:id/background-check", saveBackgroundCheck);
router.put("/leads/:id/negotiation", saveNegotiation);
router.patch("/leads/:id/workflow", patchUsedCarLeadWorkflow);
router.delete("/leads/:id", deleteUsedCarLead);

export default router;
