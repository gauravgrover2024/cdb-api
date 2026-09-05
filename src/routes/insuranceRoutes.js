import express from "express";
import {
  createInsuranceCase,
  getInsuranceCaseById,
  getInsuranceCases,
  updateInsuranceCase,
  deleteInsuranceCase,
  appendInsurancePayment,
  getNextTempRegistration,
  reserveInsuranceCaseIdHandler,
  releaseInsuranceCaseIdHandler,
  resolveVehicleCubicCapacity,
  findPotentialVehicleMatch,
  mergeVehicleMatch,
  syncInsuranceReceivable,
  getInsurancePayoutRate,
  upsertInsurancePayoutRate,
  getInsuranceRenewalCases,
  getInsuranceRenewalSummary,
  assignInsuranceRenewalCases,
  updateInsuranceRenewalLead,
  getInsurancePolicyYearInfo,
  generateInsurancePayoutScheduleForCase,
  updateInsurancePayoutEntryStatus,
} from "../controllers/insuranceController.js";
import { protect, staff } from "../middleware/authMiddleware.js";

const router = express.Router();
const RENEWAL_MANAGER_ROLES = [
  "admin",
  "superadmin",
  "team_lead",
  "insurance_team_lead",
];
const renewalManagerOnly = (req, res, next) => {
  const role = String(req.user?.role || "").toLowerCase();
  if (RENEWAL_MANAGER_ROLES.includes(role)) return next();
  res.status(403);
  throw new Error("Only admin/team lead can assign renewal cases");
};

router.route("/").get(getInsuranceCases).post(createInsuranceCase);
router
  .route("/payout-rates")
  .get(getInsurancePayoutRate)
  .post(upsertInsurancePayoutRate);
router.route("/temp-registration/next").post(getNextTempRegistration);
router.route("/case-id/reserve").post(reserveInsuranceCaseIdHandler);
router.route("/case-id/release").post(releaseInsuranceCaseIdHandler);
router
  .route("/vehicle-cubic-capacity/resolve")
  .post(resolveVehicleCubicCapacity);
router.route("/vehicle-match/potential").post(findPotentialVehicleMatch);
router.route("/vehicle-match/merge").post(mergeVehicleMatch);
router.route("/renewals/cases").get(protect, staff, getInsuranceRenewalCases);
router.route("/renewals/summary").get(protect, staff, getInsuranceRenewalSummary);
router
  .route("/renewals/assign")
  .post(protect, staff, renewalManagerOnly, assignInsuranceRenewalCases);
router.route("/renewals/:id/lead").patch(protect, staff, updateInsuranceRenewalLead);

router
  .route("/:id")
  .get(getInsuranceCaseById)
  .put(updateInsuranceCase)
  .delete(deleteInsuranceCase);
router.route("/:id/payments").post(appendInsurancePayment);

router.route("/:id/sync-receivable").post(syncInsuranceReceivable);

router.route("/:id/policy-year").get(getInsurancePolicyYearInfo);
router
  .route("/:id/payout-schedule")
  .post(generateInsurancePayoutScheduleForCase);
router
  .route("/:id/payout-schedule/:policyYear")
  .patch(updateInsurancePayoutEntryStatus);

export default router;
