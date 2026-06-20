import express from "express";
import {
  getUsedCars,
  getUsedCarById,
  createUsedCar,
  updateUsedCar,
  deleteUsedCar,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  getUsedCarByDetails,
} from "../controllers/usedCarController.js";

const router = express.Router();

router.route("/")
  .get(getUsedCars)
  .post(createUsedCar);

router.get("/makes", getUniqueMakes);
router.get("/models", getUniqueModels);
router.get("/variants", getUniqueVariants);
router.get("/by-details", getUsedCarByDetails);

router.route("/:id")
  .get(getUsedCarById)
  .put(updateUsedCar)
  .delete(deleteUsedCar);

export default router;

