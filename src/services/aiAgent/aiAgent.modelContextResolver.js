import mongoose from "mongoose";
import {
  isAciGenericModelEntityMatch,
  normalizeAciContextText,
} from "./aiAgent.contextPriority.js";
import { resolveVehicleModelFromText } from "./aiAgent.vehicleModelResolver.js";

const DEFAULT_MODEL_CONTEXT_CITY =
  String(process.env.ACI_MODEL_CONTEXT_CITY_SLUG || "new-delhi").trim() ||
  "new-delhi";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const getAciAgentMongoDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};

export const resolveAciDynamicModelEntity = async (message = "") => {
  try {
    const db = getAciAgentMongoDb();
    if (!db) return null;

    return await resolveVehicleModelFromText({
      db,
      message,
    });
  } catch {
    return null;
  }
};

export const hydrateAciExplicitModelEntityFromReadModel = async (entity = {}) => {
  if (!entity?.model) return entity;

  try {
    const db = getAciAgentMongoDb();
    if (!db) return entity;

    const modelText = cleanText(entity.model || "");
    const fullText = cleanText(entity.fullModel || entity.displayName || "");
    const brandText = cleanText(entity.make || entity.brand || "");
    const modelKey = normalizeAciContextText(modelText).replace(/\s+/g, "-");

    if (!modelKey && !modelText) return entity;

    const escapedModel = modelText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const escapedFull = fullText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    const or = [
      modelKey ? { modelKey } : null,
      modelText ? { model: new RegExp(`^\\s*${escapedModel}\\s*$`, "i") } : null,
      modelText ? { displayName: new RegExp(`\\b${escapedModel}\\b`, "i") } : null,
      modelText ? { fullModel: new RegExp(`\\b${escapedModel}\\b`, "i") } : null,
      fullText ? { displayName: new RegExp(`^\\s*${escapedFull}\\s*$`, "i") } : null,
      fullText ? { fullModel: new RegExp(`^\\s*${escapedFull}\\s*$`, "i") } : null,
    ].filter(Boolean);

    const candidates = await db.collection("aci_vehicle_model_summary")
      .find(
        {
          citySlug: DEFAULT_MODEL_CONTEXT_CITY,
          $or: or,
        },
        {
          projection: {
            make: 1,
            makeKey: 1,
            model: 1,
            modelKey: 1,
            fullModel: 1,
            displayName: 1,
            variantCount: 1,
          },
        },
      )
      .limit(20)
      .toArray();

    if (!candidates.length) return entity;

    const normalizedWanted = normalizeAciContextText(modelText);
    const normalizedBrand = normalizeAciContextText(brandText);

    const scoreCandidate = (row = {}) => {
      const rowModel = normalizeAciContextText(row.model || "");
      const rowFull = normalizeAciContextText(row.fullModel || row.displayName || "");
      const rowMake = normalizeAciContextText(row.make || "");
      let score = 0;

      if (rowModel === normalizedWanted) score += 100;
      if (rowFull === normalizedWanted) score += 90;
      if (rowFull.endsWith(` ${normalizedWanted}`)) score += 75;
      if (rowFull.includes(normalizedWanted)) score += 50;
      if (normalizedBrand && rowMake === normalizedBrand) score += 25;
      if (Number(row.variantCount || 0) > 0) score += 5;

      return score;
    };

    const summary = [...candidates].sort((a, b) => scoreCandidate(b) - scoreCandidate(a))[0];

    if (!summary?.model) return entity;

    const make = cleanText(summary.make || "");
    const model = cleanText(summary.model || entity.model || "");
    const fullModel = cleanText(
      summary.fullModel ||
        summary.displayName ||
        (make && model ? `${make} ${model}` : model),
    );

    return {
      ...entity,
      make,
      brand: make,
      model,
      fullModel,
      displayName: fullModel,
      makeKey: summary.makeKey || "",
      modelKey: summary.modelKey || modelKey,
      fromReadModelSummary: true,
    };
  } catch {
    return entity;
  }
};

export const resolveAciExplicitMessageModelEntity = async (message = "") => {
  const resolved = await resolveAciDynamicModelEntity(message);

  if (resolved?.model && !isAciGenericModelEntityMatch(resolved)) {
    return hydrateAciExplicitModelEntityFromReadModel({
      ...resolved,
      fromMessage: true,
    });
  }

  // No static model fallback here.
  // Explicit model truth must come from resolver/read-model data only.
  return null;
};
