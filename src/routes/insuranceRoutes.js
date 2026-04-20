import express from "express";
import {
  createInsuranceCase,
  getInsuranceCaseById,
  getInsuranceCases,
  updateInsuranceCase,
  deleteInsuranceCase,
  syncInsuranceReceivable,
  getInsurancePayoutRate,
  upsertInsurancePayoutRate,
} from "../controllers/insuranceController.js";

const router = express.Router();

router.route("/").get(getInsuranceCases).post(createInsuranceCase);
router
  .route("/payout-rates")
  .get(getInsurancePayoutRate)
  .post(upsertInsurancePayoutRate);

router
  .route("/:id")
  .get(getInsuranceCaseById)
  .put(updateInsuranceCase)
  .delete(deleteInsuranceCase);

router.route("/:id/sync-receivable").post(syncInsuranceReceivable);

export default router;
