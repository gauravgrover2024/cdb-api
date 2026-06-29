import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import * as ctrl from "../controllers/homeLoanController.js";

const router = express.Router();

// All routes require authentication
router.use(protect);

// ── Dashboard & Stats ────────────────────────────────────────────────────────
router.get("/dashboard/stats", ctrl.getDashboardStats);

// ── Collections / Receivables ────────────────────────────────────────────────
router.get("/collections/receivables", ctrl.getCollectionsReceivables);
router.post("/collections/receivables/upsert", ctrl.upsertCollectionReceivable);
router.patch("/collections/receivables/:id", ctrl.updateCollectionReceivable);
router.delete("/collections/receivables/:id", ctrl.deleteCollectionReceivable);

// ── Analytics ─────────────────────────────────────────────────────────────────
router.get("/analytics/overview", ctrl.getAnalyticsOverview);
router.get("/analytics/drilldown", ctrl.getAnalyticsDrilldown);
router.post("/analytics/custom-widget", ctrl.createCustomWidget);
router.post("/analytics/custom-report", ctrl.createCustomReport);

// ── Counters ──────────────────────────────────────────────────────────────────
router.get("/counters/rc-inv/next", ctrl.getNextRcInvNumber);

// ── Application CRUD (keep :id routes below static routes) ────────────────────
router.get("/", ctrl.listLoans);
router.post("/", ctrl.createLoan);

router.get("/:id", ctrl.getLoan);
router.put("/:id", ctrl.updateLoan);
router.delete("/:id", ctrl.deleteLoan);

// ── Per-loan sub-resources ────────────────────────────────────────────────────
router.post("/:id/disburse", ctrl.disburseLoan);
router.get("/:id/banks", ctrl.getBanks);
router.put("/:id/banks", ctrl.saveBanks);

export default router;
