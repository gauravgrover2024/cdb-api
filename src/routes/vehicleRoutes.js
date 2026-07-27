import express from 'express';
const router = express.Router();
import {
  getVehicles,
  searchVehicleRecords,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
  deleteVehicleRecord,
  bulkUploadVehicles,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  addSuggestionTerm,
  getVariantOptionsByModel,
  getVehicleByDetails,
  getVehicleMedia,
  getPopularCars,
  getSimilarModels,
} from '../controllers/vehicleController.js';
import {
  getScraperCatalog,
  getScraperSummary,
  getScraperRuns,
  getScraperRunById,
  runScraper,
} from "../controllers/vehicleScraperController.js";

router.route('/')
  .get(getVehicles)
  .post(createVehicle);

router.get('/records/search', searchVehicleRecords);
router.delete('/records/:id', deleteVehicleRecord);

router.route('/bulk')
  .post(bulkUploadVehicles);

// Distinct values routes (must come before /:id)
router.route('/distinct/makes')
  .get(getUniqueMakes);

router.route('/distinct/models')
  .get(getUniqueModels);

router.route('/distinct/variants')
  .get(getUniqueVariants);

router.route('/distinct/variants-with-price')
  .get(getVariantOptionsByModel);

router.route('/suggestions/terms')
  .post(addSuggestionTerm);

router.route('/by-details')
  .get(getVehicleByDetails);

router.route('/media')
  .get(getVehicleMedia);

router.route('/popular-cars')
  .get(getPopularCars);

router.route('/similar-models')
  .get(getSimilarModels);

router.get("/scraper/catalog", getScraperCatalog);
router.get("/scraper/summary", getScraperSummary);
router.get("/scraper/runs", getScraperRuns);
router.get("/scraper/runs/:runId", getScraperRunById);
router.post("/scraper/run", runScraper);

router.route('/:id')
  .get(getVehicleById)
  .put(updateVehicle)
  .delete(deleteVehicle);

export default router;
