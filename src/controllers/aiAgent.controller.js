import asyncHandler from "express-async-handler";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

export const chatWithAiAgent = asyncHandler(async (req, res) => {
  const { message, sessionId, context, selectedEntity, filters, debug } = req.body || {};
  if (!message || typeof message !== "string") {
    res.status(400);
    throw new Error("message is required");
  }

  const response = await chatWithAgent({
    message,
    sessionId,
    context,
    selectedEntity,
    filters,
    debug,
    user: req.user,
  });

  res.json(response);
});
