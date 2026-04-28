import asyncHandler from "express-async-handler";
import { runGlobalSearch } from "../services/globalSearch/globalSearchService.js";
import { runAssistSearch } from "../services/globalSearch/assistSearchService.js";

const toSafeInt = (value, fallback, min, max) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
};

export const globalSearch = asyncHandler(async (req, res) => {
  const query = String(req.query.q || req.query.query || "").trim();
  const limit = toSafeInt(req.query.limit, 40, 1, 100);
  const perEntityLimit = toSafeInt(req.query.perEntityLimit, 8, 1, 20);

  const payload = await runGlobalSearch({ query, limit, perEntityLimit });

  res.json({
    success: true,
    data: payload,
  });
});

export const assistSearch = asyncHandler(async (req, res) => {
  const query = String(req.query.q || req.query.query || "").trim();
  const limit = toSafeInt(req.query.limit, 40, 1, 100);
  const perEntityLimit = toSafeInt(req.query.perEntityLimit, 8, 1, 20);

  const payload = await runAssistSearch({ query, limit, perEntityLimit });

  res.json({
    success: true,
    data: payload,
  });
});
