import asyncHandler from "express-async-handler";
import {
  getVehicleScraperCatalog,
  getVehicleScraperRuns,
  getVehicleScraperRun,
  getVehicleScraperSummary,
  startVehicleScraperRun,
} from "../services/vehicleScraperService.js";

export const getScraperCatalog = asyncHandler(async (_req, res) => {
  const catalog = getVehicleScraperCatalog();
  res.json({ success: true, data: catalog });
});

export const getScraperSummary = asyncHandler(async (_req, res) => {
  const summary = await getVehicleScraperSummary();
  res.json({ success: true, data: summary });
});

export const getScraperRuns = asyncHandler(async (_req, res) => {
  const runs = getVehicleScraperRuns();
  res.json({ success: true, data: runs });
});

export const getScraperRunById = asyncHandler(async (req, res) => {
  const includeLogs = String(req.query.includeLogs || "").toLowerCase() === "true";
  const run = getVehicleScraperRun(req.params.runId, { includeLogs });
  if (!run) {
    res.status(404);
    throw new Error("Script run not found");
  }
  res.json({ success: true, data: run });
});

export const runScraper = asyncHandler(async (req, res) => {
  const scriptKey = String(req.body?.scriptKey || "").trim();
  if (!scriptKey) {
    res.status(400);
    throw new Error("scriptKey is required");
  }
  try {
    const run = await startVehicleScraperRun(scriptKey);
    res.status(202).json({
      success: true,
      message: `Started ${run.label}`,
      data: run,
    });
  } catch (error) {
    if (error?.statusCode) {
      res.status(error.statusCode);
    } else {
      res.status(500);
    }
    throw new Error(error?.message || "Unable to start scraper script");
  }
});

