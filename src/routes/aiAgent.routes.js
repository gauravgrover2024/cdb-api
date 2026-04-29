import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import { chatWithAiAgent } from "../controllers/aiAgent.controller.js";

const router = express.Router();

router.post("/chat", protect, chatWithAiAgent);

export default router;
