import express from "express";
import {
  assistSearch,
  globalSearch,
} from "../controllers/globalSearchController.js";

const router = express.Router();

router.get("/", globalSearch);
router.get("/assist", assistSearch);

export default router;
