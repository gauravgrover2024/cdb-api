import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import {
  autocompleteAiAgent,
  chatWithAiAgent,
  chatWithAiAgentPublic,
  logAiSuggestionInteraction,
} from "../controllers/aiAgent.controller.js";

const router = express.Router();

router.post("/chat", protect, chatWithAiAgent);
router.post("/public-chat", chatWithAiAgentPublic);
router.get("/autocomplete", protect, autocompleteAiAgent);
router.post("/suggestion-interaction", protect, logAiSuggestionInteraction);

export default router;
