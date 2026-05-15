import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";
import {
  buildVehicleNormalizationUpdate,
  normalizeColorName,
  normalizeVehicleDatasetRow,
  vehicleIdentityKey,
  vehicleModelKey,
} from "../utils/vehicleDatasetNormalizer.js";

dotenv.config();

const sameArray = (a = [], b = []) => {
  const left = Array.isArray(a) ? a : [];
  const right = Array.isArray(b) ? b : [];
  if (left.length !== right.length) return false;
  return left.every((item, index) => item === right[index]);
};

const fieldChanged = (doc, key, value) => {
  if (Array.isArray(value)) return !sameArray(doc[key], value);
  return String(doc[key] ?? "") !== String(value ?? "");
};

const buildColorMaps = async () => {
  const rows = await mongoose.connection.db
    .collection("vehicle_colors_v2")
    .find(
      {},
      {
        projection: {
          brand: 1,
          make: 1,
          model: 1,
          variant: 1,
          color_name: 1,
          colorName: 1,
          name: 1,
        },
      },
    )
    .toArray();

  const byModel = new Map();
  const byVariant = new Map();

  for (const row of rows) {
    const color = normalizeColorName(row.color_name || row.colorName || row.name);
    if (!color) continue;

    const normalized = normalizeVehicleDatasetRow({
      brand: row.brand || row.make,
      make: row.make || row.brand,
      model: row.model,
      variant: row.variant,
    });
    if (!normalized.brand_normalized || !normalized.model_normalized) continue;

    const targetMap = normalized.variant_normalized ? byVariant : byModel;
    const key = normalized.variant_normalized
      ? vehicleIdentityKey({
          brand: normalized.brand_normalized,
          model: normalized.model_normalized,
          variant: normalized.variant_normalized,
        })
      : vehicleModelKey({
          brand: normalized.brand_normalized,
          model: normalized.model_normalized,
        });

    if (!targetMap.has(key)) targetMap.set(key, new Set());
    targetMap.get(key).add(color);
  }

  return { byModel, byVariant, sourceRows: rows.length };
};

const colorsForVehicle = (normalized, colorMaps) => {
  const variantKey = vehicleIdentityKey({
    brand: normalized.brand_normalized,
    model: normalized.model_normalized,
    variant: normalized.variant_normalized,
  });
  const modelKey = vehicleModelKey({
    brand: normalized.brand_normalized,
    model: normalized.model_normalized,
  });

  const variantColors = colorMaps.byVariant.get(variantKey);
  const modelColors = colorMaps.byModel.get(modelKey);
  const colors = variantColors?.size ? variantColors : modelColors;
  return colors?.size ? [...colors].sort((a, b) => a.localeCompare(b)) : [];
};

const main = async () => {
  const apply = process.argv.includes("--apply");
  const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
  const limit = limitArg ? Number(limitArg.split("=")[1]) : 0;

  await connectDB();
  const colorMaps = await buildColorMaps();
  const query = {};
  const cursor = Vehicle.find(query)
    .select({
      make: 1,
      brand: 1,
      model: 1,
      variant: 1,
      fuel: 1,
      fuel_type: 1,
      fuelType: 1,
      transmission: 1,
      transmission_type: 1,
      gearbox: 1,
      brand_normalized: 1,
      model_normalized: 1,
      variant_normalized: 1,
      search_text: 1,
      colors_normalized: 1,
    })
    .sort({ brand: 1, make: 1, model: 1, variant: 1 })
    .lean();

  if (limit > 0) cursor.limit(limit);
  const vehicles = await cursor;

  const operations = [];
  const sampleOutput = [];
  let checked = 0;
  let needingUpdate = 0;
  let withColors = 0;

  for (const doc of vehicles) {
    checked += 1;
    const normalizedBase = normalizeVehicleDatasetRow(doc);
    const colors = colorsForVehicle(normalizedBase, colorMaps);
    const normalized = normalizeVehicleDatasetRow(doc, { colors });
    if (normalized.colors_normalized?.length) withColors += 1;

    if (sampleOutput.length < 10) {
      sampleOutput.push({
        brand: normalized.brand,
        model: normalized.model,
        variant: normalized.variant,
        brand_normalized: normalized.brand_normalized,
        model_normalized: normalized.model_normalized,
        variant_normalized: normalized.variant_normalized,
        search_text: normalized.search_text,
        colors_normalized: normalized.colors_normalized || undefined,
      });
    }

    const update = buildVehicleNormalizationUpdate(doc, { colors });
    const setChanged = Object.entries(update.$set).some(([key, value]) =>
      fieldChanged(doc, key, value),
    );
    const shouldUnsetColors =
      update.$unset?.colors_normalized &&
      doc.colors_normalized !== undefined;
    const colorsChanged =
      update.$set.colors_normalized &&
      fieldChanged(doc, "colors_normalized", update.$set.colors_normalized);

    if (!setChanged && !shouldUnsetColors && !colorsChanged) continue;

    needingUpdate += 1;
    operations.push({
      updateOne: {
        filter: { _id: doc._id },
        update,
      },
    });
  }

  console.log(
    JSON.stringify(
      {
        apply,
        checked,
        needingUpdate,
        colorSourceRows: colorMaps.sourceRows,
        vehiclesWithColors: withColors,
        sampleOutput,
      },
      null,
      2,
    ),
  );

  if (apply && operations.length) {
    const result = await Vehicle.bulkWrite(operations, { ordered: false });
    console.log(
      JSON.stringify(
        {
          matched: result.matchedCount,
          modified: result.modifiedCount,
        },
        null,
        2,
      ),
    );
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error("normalizeVehicleDataset failed:", error);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
