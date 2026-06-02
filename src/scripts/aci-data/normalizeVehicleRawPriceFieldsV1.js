#!/usr/bin/env node

import "dotenv/config";
import mongoose from "mongoose";

const args = new Set(process.argv.slice(2));
const SHOULD_WRITE = args.has("--write");
const FORCE = args.has("--force");

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const SOURCE_COLLECTION = "vehicles";
const NORMALIZE_VERSION = "raw-price-normalize-v1";

const cleanText = (value = "") =>
  String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();

const first = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return value;
    }
  }
  return "";
};

const getObjectishField = (value, key = "") => {
  if (!value || !key) return "";

  if (typeof value === "object" && !Array.isArray(value)) {
    return value[key] ?? "";
  }

  const raw = String(value || "");
  if (!raw) return "";

  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const quotedPattern = new RegExp(
    `['"]${escaped}['"]\\s*:\\s*['"]([^'"]*)['"]`,
    "i",
  );

  const quoted = raw.match(quotedPattern);
  if (quoted?.[1]) return quoted[1];

  const numericPattern = new RegExp(
    `['"]${escaped}['"]\\s*:\\s*([0-9][0-9,\\.]*)(?:\\s|,|})`,
    "i",
  );

  const numeric = raw.match(numericPattern);
  if (numeric?.[1]) return numeric[1];

  return "";
};

const toNumber = (value) => {
  if (typeof value === "number" && Number.isFinite(value)) return Math.round(value);

  const raw = String(value || "").replace(/[₹,\s]/g, "").trim();
  if (!raw) return null;

  const lower = raw.toLowerCase();
  const num = Number(lower.replace(/l|lac|lakh|cr|crore/g, ""));

  if (!Number.isFinite(num)) return null;

  if (/cr|crore/.test(lower)) return Math.round(num * 10000000);
  if (/l|lac|lakh/.test(lower)) return Math.round(num * 100000);

  return Math.round(num);
};

const buildPatch = (doc = {}) => {
  const rawPrice = doc.raw_price_json;
  if (!rawPrice) return null;

  const variantShortName = cleanText(
    first(
      doc.variantShortName,
      getObjectishField(rawPrice, "variantShortName"),
    ),
  );

  const variantDisplayId = cleanText(
    first(
      doc.variantDisplayId,
      getObjectishField(rawPrice, "variantDisplayId"),
    ),
  );

  const variantDisplayName = cleanText(
    first(
      doc.variantDisplayName,
      getObjectishField(rawPrice, "variantDisplayName"),
      getObjectishField(rawPrice, "variantId"),
    ),
  );

  const cityName = cleanText(
    first(
      doc.cityName,
      getObjectishField(rawPrice, "variantCity"),
      getObjectishField(rawPrice, "cityName"),
    ),
  );

  const exShowroomPriceNumeric = toNumber(
    first(
      doc.exShowroomPriceNumeric,
      doc.exShowroomPrice,
      doc.ex_showroom_price,
      doc.price,
      doc.priceNumeric,
      doc.exShowroom,
      getObjectishField(rawPrice, "exShowRoom"),
      getObjectishField(rawPrice, "exShowroom"),
      getObjectishField(rawPrice, "exShowroomPrice"),
      getObjectishField(rawPrice, "threeDigitExShowRoomPrice"),
    ),
  );

  const onRoadPriceNumeric = toNumber(
    first(
      doc.onRoadPriceNumeric,
      doc.onRoadPrice,
      doc.on_road_price,
      doc.startingOnRoadPrice,
      getObjectishField(rawPrice, "onRoadPriceOfVariant"),
      getObjectishField(rawPrice, "ORPWithoutOptionAccessories"),
      getObjectishField(rawPrice, "onRoadPriceInIndianFormat"),
      getObjectishField(rawPrice, "threeDigitOnROadPrice"),
    ),
  );

  const fuelType = cleanText(
    first(
      doc.fuelType,
      doc.fuel,
      getObjectishField(rawPrice, "variantFuelType"),
      getObjectishField(rawPrice, "fuel"),
    ),
  );

  const transmissionType = cleanText(
    first(
      doc.transmissionType,
      doc.transmission,
      getObjectishField(rawPrice, "transmission"),
      getObjectishField(rawPrice, "transmissionType"),
    ),
  );

  const bodyType = cleanText(
    first(
      doc.bodyType,
      getObjectishField(rawPrice, "bodyType"),
    ),
  );

  const patch = {
    rawPriceNormalizedAt: new Date(),
    rawPriceNormalizeVersion: NORMALIZE_VERSION,
  };

  if (variantShortName) patch.variantShortName = variantShortName;
  if (variantDisplayId) patch.variantDisplayId = variantDisplayId;
  if (variantDisplayName) patch.variantDisplayName = variantDisplayName;
  if (cityName) patch.cityName = cityName;
  if (Number.isFinite(exShowroomPriceNumeric)) patch.exShowroomPriceNumeric = exShowroomPriceNumeric;
  if (Number.isFinite(onRoadPriceNumeric)) patch.onRoadPriceNumeric = onRoadPriceNumeric;
  if (fuelType) patch.fuelType = fuelType;
  if (transmissionType) patch.transmissionType = transmissionType;
  if (bodyType) patch.bodyType = bodyType;

  return patch;
};

const main = async () => {
  if (!mongoUri) throw new Error("Mongo URI missing. Check .env.");

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const col = db.collection(SOURCE_COLLECTION);

  const filter = {
    raw_price_json: { $exists: true, $nin: [null, ""] },
    ...(FORCE
      ? {}
      : {
          $or: [
            { rawPriceNormalizeVersion: { $ne: NORMALIZE_VERSION } },
            { exShowroomPriceNumeric: { $in: [null, ""] } },
            { variantShortName: { $in: [null, ""] } },
            { cityName: { $in: [null, ""] } },
          ],
        }),
  };

  console.log(`[mode] ${SHOULD_WRITE ? "WRITE" : "DRY_RUN"}, force=${FORCE}`);
  console.log(`[source] ${SOURCE_COLLECTION}`);

  const cursor = col.find(filter, {
    projection: {
      _id: 1,
      make: 1,
      brand: 1,
      model: 1,
      variant: 1,
      city: 1,
      cityName: 1,
      citySlug: 1,
      variantShortName: 1,
      variantDisplayId: 1,
      variantDisplayName: 1,
      exShowroomPriceNumeric: 1,
      exShowroomPrice: 1,
      ex_showroom_price: 1,
      price: 1,
      priceNumeric: 1,
      exShowroom: 1,
      onRoadPriceNumeric: 1,
      onRoadPrice: 1,
      on_road_price: 1,
      startingOnRoadPrice: 1,
      fuel: 1,
      fuelType: 1,
      transmission: 1,
      transmissionType: 1,
      bodyType: 1,
      raw_price_json: 1,
      rawPriceNormalizeVersion: 1,
    },
  }).batchSize(500);

  let scanned = 0;
  let updateCandidates = 0;
  let modified = 0;
  const samples = [];
  const bulk = [];

  for await (const doc of cursor) {
    scanned += 1;

    const patch = buildPatch(doc);
    if (!patch) continue;

    updateCandidates += 1;

    if (samples.length < 8) {
      samples.push({
        _id: String(doc._id),
        before: {
          variantShortName: doc.variantShortName,
          cityName: doc.cityName,
          exShowroomPriceNumeric: doc.exShowroomPriceNumeric,
          onRoadPriceNumeric: doc.onRoadPriceNumeric,
          fuelType: doc.fuelType,
          transmissionType: doc.transmissionType,
          bodyType: doc.bodyType,
        },
        after: patch,
      });
    }

    if (SHOULD_WRITE) {
      bulk.push({
        updateOne: {
          filter: { _id: doc._id },
          update: { $set: patch },
        },
      });

      if (bulk.length >= 500) {
        const result = await col.bulkWrite(bulk.splice(0), { ordered: false });
        modified += result.modifiedCount || 0;
        console.log(`[write] scanned=${scanned}, modified=${modified}`);
      }
    }
  }

  if (SHOULD_WRITE && bulk.length) {
    const result = await col.bulkWrite(bulk, { ordered: false });
    modified += result.modifiedCount || 0;
  }

  console.log(JSON.stringify({
    mode: SHOULD_WRITE ? "WRITE" : "DRY_RUN",
    scanned,
    updateCandidates,
    modified,
    samples,
  }, null, 2));

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
