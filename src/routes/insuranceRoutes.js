import express from "express";
import {
  createInsuranceCase,
  getInsuranceCaseById,
  getInsuranceCases,
  updateInsuranceCase,
  deleteInsuranceCase,
  appendInsurancePayment,
  getNextTempRegistration,
  resolveVehicleCubicCapacity,
  findPotentialVehicleMatch,
  mergeVehicleMatch,
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
router.route("/temp-registration/next").post(getNextTempRegistration);
router
  .route("/vehicle-cubic-capacity/resolve")
  .post(resolveVehicleCubicCapacity);
router.route("/vehicle-match/potential").post(findPotentialVehicleMatch);
router.route("/vehicle-match/merge").post(mergeVehicleMatch);

router
  .route("/:id")
  .get(getInsuranceCaseById)
  .put(updateInsuranceCase)
  .delete(deleteInsuranceCase);
router.route("/:id/payments").post(appendInsurancePayment);

router.route("/:id/sync-receivable").post(syncInsuranceReceivable);

export default router;
