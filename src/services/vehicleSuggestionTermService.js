import Vehicle from "../models/Vehicle.js";
import VehicleSuggestionTerm from "../models/VehicleSuggestionTerm.js";
import { normalizeVehicleDatasetRow, lowerKey } from "../utils/vehicleDatasetNormalizer.js";
import {
  buildVehicleQuery,
  invalidateDistinctCache,
} from "../controllers/vehicleController.js";

const VEHICLE_MATCH_PROJECTION = { make: 1, brand: 1, model: 1, variant: 1 };

const computeTermKeys = ({ level, make, model, variant }) => {
  const normalized = normalizeVehicleDatasetRow({ brand: make, model, variant });
  const makeKey = lowerKey(normalized.brand_normalized);
  const modelKey = lowerKey(normalized.model_normalized);
  const variantKey = lowerKey(normalized.variant_normalized);

  if (level === "make") {
    return {
      display: normalized.brand_normalized,
      canonicalKey: makeKey,
      scopeKey: "",
      makeDisplay: normalized.brand_normalized,
    };
  }
  if (level === "model") {
    return {
      display: normalized.model_normalized,
      canonicalKey: modelKey,
      scopeKey: makeKey,
      makeDisplay: normalized.brand_normalized,
      modelDisplay: normalized.model_normalized,
    };
  }
  return {
    display: normalized.variant_normalized,
    canonicalKey: variantKey,
    scopeKey: `${makeKey}|${modelKey}`,
    makeDisplay: normalized.brand_normalized,
    modelDisplay: normalized.model_normalized,
  };
};

const scrapedValueFor = (level, vehicleDoc) => {
  if (level === "make") return vehicleDoc.make || vehicleDoc.brand;
  if (level === "model") return vehicleDoc.model;
  return vehicleDoc.variant;
};

const findScrapedMatch = ({ level, make, model, variant }) =>
  Vehicle.findOne(
    buildVehicleQuery({
      make,
      model: level !== "make" ? model : undefined,
      variant: level === "variant" ? variant : undefined,
    }),
  )
    .select(VEHICLE_MATCH_PROJECTION)
    .lean();

// "Type & Save": add a manual term if (and only if) it isn't already covered
// by scraped data. Returns the scraped value directly instead of creating a
// duplicate when a match already exists.
export const createSuggestionTerm = async ({
  level,
  make,
  model,
  variant,
  city,
  createdBy,
} = {}) => {
  if (!["make", "model", "variant"].includes(level)) {
    const error = new Error("level must be one of make, model, variant");
    error.statusCode = 400;
    throw error;
  }

  const makeInput = String(make || "").trim();
  const modelInput = String(model || "").trim();
  const variantInput = String(variant || "").trim();

  if (!makeInput) {
    const error = new Error("Make is required");
    error.statusCode = 400;
    throw error;
  }
  if (level !== "make" && !modelInput) {
    const error = new Error("Model is required");
    error.statusCode = 400;
    throw error;
  }
  if (level === "variant" && !variantInput) {
    const error = new Error("Variant is required");
    error.statusCode = 400;
    throw error;
  }

  const scrapedMatch = await findScrapedMatch({
    level,
    make: makeInput,
    model: modelInput,
    variant: variantInput,
  });
  if (scrapedMatch) {
    return {
      matchedExisting: true,
      isCustom: false,
      value: scrapedValueFor(level, scrapedMatch),
    };
  }

  const keys = computeTermKeys({
    level,
    make: makeInput,
    model: modelInput,
    variant: variantInput,
  });

  const term = await VehicleSuggestionTerm.findOneAndUpdate(
    { level, scopeKey: keys.scopeKey, canonicalKey: keys.canonicalKey },
    {
      $setOnInsert: {
        level,
        make: keys.makeDisplay,
        model: level !== "make" ? keys.modelDisplay : undefined,
        variant: level === "variant" ? keys.display : undefined,
        canonicalKey: keys.canonicalKey,
        scopeKey: keys.scopeKey,
        city: city || undefined,
        createdBy: createdBy || undefined,
        status: "active",
        source: "manual",
      },
    },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  );

  invalidateDistinctCache();

  return {
    matchedExisting: false,
    isCustom: true,
    value: keys.display,
    term,
  };
};

// Active manual terms for a given scope, to be unioned into the
// getUniqueMakes/getUniqueModels/getUniqueVariants autosuggest responses.
export const getActiveManualTerms = async ({ level, make, model } = {}) => {
  const keys = computeTermKeys({ level, make, model, variant: undefined });
  const terms = await VehicleSuggestionTerm.find({
    level,
    scopeKey: keys.scopeKey,
    status: "active",
  })
    .select({ make: 1, model: 1, variant: 1 })
    .lean();

  return terms.map((term) => ({
    value: level === "make" ? term.make : level === "model" ? term.model : term.variant,
    isCustom: true,
  }));
};

// Run after every scraper sync: any manual term now covered by scraped data
// is marked merged so it silently drops out of the autosuggest union above.
// Nothing is deleted, so anything that already referenced the manual value
// keeps working.
export const reconcileManualVehicleTerms = async () => {
  const activeTerms = await VehicleSuggestionTerm.find({ status: "active" }).lean();
  const ops = [];

  for (const term of activeTerms) {
    const match = await findScrapedMatch({
      level: term.level,
      make: term.make,
      model: term.model,
      variant: term.variant,
    });
    if (!match) continue;

    ops.push({
      updateOne: {
        filter: { _id: term._id },
        update: {
          $set: {
            status: "merged",
            mergedIntoValue: scrapedValueFor(term.level, match),
            mergedAt: new Date(),
          },
        },
      },
    });
  }

  if (ops.length) {
    await VehicleSuggestionTerm.bulkWrite(ops, { ordered: false });
    invalidateDistinctCache();
  }

  return { checked: activeTerms.length, merged: ops.length };
};
