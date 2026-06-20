import express from "express";
import {
  getUsedCars,
  getUsedCarById,
  createUsedCar,
  updateUsedCar,
  deleteUsedCar,
} from "../controllers/usedCarController.js";

const router = express.Router();

router.route("/")
  .get(getUsedCars)
  .post(createUsedCar);

router.route("/:id")
  .get(getUsedCarById)
  .put(updateUsedCar)
  .delete(deleteUsedCar);

export default router;
