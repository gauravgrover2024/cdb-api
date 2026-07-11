import asyncHandler from "express-async-handler";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import { logInteraction } from "../services/aiAgent/aiAgent.learningEngine.js";
import { getAiAutocompleteSuggestions } from "../services/aiAgent/aiAgent.autocomplete.js";

export const chatWithAiAgent = asyncHandler(async (req, res) => {
  const { message, sessionId, context, selectedEntity, filters, debug } =
    req.body || {};

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

export const chatWithAiAgentPublic = asyncHandler(async (req, res) => {
  const { message, sessionId, context, selectedEntity, filters, debug } =
    req.body || {};

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
    user: null,
  });

  res.json(response);
});

export const logAiSuggestionInteraction = asyncHandler(async (req, res) => {
  const { suggestionId, intent, actionTaken = true } = req.body || {};

  if (!suggestionId || !intent) {
    res.status(400);
    throw new Error("suggestionId and intent are required");
  }

  const result = await logInteraction({
    userId: String(req.user?._id || req.user?.id || ""),
    intent,
    suggestionId,
    actionTaken: true,
    countImpression: false,
  });

  res.json({
    success: true,
    result,
  });
});

export const autocompleteAiAgent = asyncHandler(async (req, res) => {
  const { q = "", limit = 8, make = "", model = "", variant = "" } =
    req.query || {};
  const context =
    req.body?.context ||
    (model
      ? {
          selectedVehicle: {
            make,
            brand: make,
            model,
            variant,
          },
        }
      : {});

  const response = await getAiAutocompleteSuggestions({
    q,
    context,
    limit: Number(limit) || 8,
  });

  res.set(
    "Cache-Control",
    "public, max-age=60, s-maxage=300, stale-while-revalidate=3600",
  );
  res.json(response);
});

export const autocompleteAiAgentPublic = asyncHandler(async (req, res) => {
  const payload = req.method === "POST" ? req.body || {} : req.query || {};
  const {
    q = "",
    limit = 8,
    context: suppliedContext = {},
    make = "",
    model = "",
    variant = "",
  } = payload;
  const context =
    suppliedContext && typeof suppliedContext === "object" &&
    Object.keys(suppliedContext).length
      ? suppliedContext
      : model
        ? {
            selectedVehicle: {
              make,
              brand: make,
              model,
              variant,
            },
          }
        : {};

  const response = await getAiAutocompleteSuggestions({
    q,
    context,
    limit: Number(limit) || 8,
  });

  if (req.method === "GET") {
    res.set(
      "Cache-Control",
      "public, max-age=60, s-maxage=300, stale-while-revalidate=3600",
    );
  } else {
    res.set("Cache-Control", "no-store");
  }
  res.json(response);
});
