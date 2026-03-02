// src/routes/featuresRoutes.js
import express from "express";
const router = express.Router();

import {
  getFeatureDetails,
  getFeatureVariants,
  getFeatureVariantById,
  getVariantsWithPriceAndFeatures,
  getFeaturesBySelection,
} from "../controllers/featuresController.js";

router.get("/details", getFeatureDetails);
router.get("/variants", getFeatureVariants);
router.get("/variants-with-price", getVariantsWithPriceAndFeatures);
router.get("/by-selection", getFeaturesBySelection);
router.get("/variant/:id", getFeatureVariantById);

export default router;
