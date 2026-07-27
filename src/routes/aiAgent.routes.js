import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import {
  autocompleteAiAgent,
  autocompleteAiAgentPublic,
  chatWithAiAgent,
  chatWithAiAgentPublic,
  getAiAgentPricingCitiesPublic,
  logAiSuggestionInteraction,
} from "../controllers/aiAgent.controller.js";

const router = express.Router();

router.post("/chat", protect, chatWithAiAgent);
router.post("/public-chat", chatWithAiAgentPublic);
router.get("/autocomplete", protect, autocompleteAiAgent);
router.get("/public-autocomplete", autocompleteAiAgentPublic);
router.post("/public-autocomplete", autocompleteAiAgentPublic);
router.get("/public-pricing-cities", getAiAgentPricingCitiesPublic);
router.post("/suggestion-interaction", protect, logAiSuggestionInteraction);

export default router;
