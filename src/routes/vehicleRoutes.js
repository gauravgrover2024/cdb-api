import express from 'express';
const router = express.Router();
import {
  getVehicles,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
  bulkUploadVehicles,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  getVehicleByDetails,
  getVehicleMedia,
} from '../controllers/vehicleController.js';

router.route('/')
  .get(getVehicles)
  .post(createVehicle);

router.route('/bulk')
  .post(bulkUploadVehicles);

// Distinct values routes (must come before /:id)
router.route('/distinct/makes')
  .get(getUniqueMakes);

router.route('/distinct/models')
  .get(getUniqueModels);

router.route('/distinct/variants')
  .get(getUniqueVariants);

router.route('/by-details')
  .get(getVehicleByDetails);

router.route('/media')
  .get(getVehicleMedia);

router.route('/:id')
  .get(getVehicleById)
  .put(updateVehicle)
  .delete(deleteVehicle);

export default router;
