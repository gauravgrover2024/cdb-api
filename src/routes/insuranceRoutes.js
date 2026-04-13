import express from "express";
import {
  createInsuranceCase,
  getInsuranceCaseById,
  getInsuranceCases,
  updateInsuranceCase,
  deleteInsuranceCase,
  syncInsuranceReceivable,
} from "../controllers/insuranceController.js";

const router = express.Router();

router.route("/").get(getInsuranceCases).post(createInsuranceCase);

router
  .route("/:id")
  .get(getInsuranceCaseById)
  .put(updateInsuranceCase)
  .delete(deleteInsuranceCase);

router.route("/:id/sync-receivable").post(syncInsuranceReceivable);

export default router;
