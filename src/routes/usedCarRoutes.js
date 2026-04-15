import express from "express";
import {
  bulkAssignUsedCarLeads,
  clearUsedCarLeads,
  createUsedCarLead,
  deleteUsedCarLead,
  getUsedCarLeadById,
  importUsedCarLeads,
  listUsedCarLeads,
  patchUsedCarLeadWorkflow,
  updateUsedCarLead,
} from "../controllers/usedCarLeadController.js";

const router = express.Router();

router.get("/leads", listUsedCarLeads);
router.post("/leads", createUsedCarLead);
router.post("/leads/import", importUsedCarLeads);
router.post("/leads/clear", clearUsedCarLeads);
router.post("/leads/assign", bulkAssignUsedCarLeads);
router.get("/leads/:id", getUsedCarLeadById);
router.put("/leads/:id", updateUsedCarLead);
router.patch("/leads/:id/workflow", patchUsedCarLeadWorkflow);
router.delete("/leads/:id", deleteUsedCarLead);

export default router;
