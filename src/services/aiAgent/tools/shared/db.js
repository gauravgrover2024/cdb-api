import mongoose from "mongoose";

/**
 * Shared Mongo helpers for ACI Assist V2 tools.
 * This file only handles collection discovery and safe reads.
 */

export const VEHICLE_COLLECTION_CANDIDATES = [
  "vehicles",
  "vehicle_prices",
  "vehicleprices",
  "vehicle_variants",
  "vehiclevariants",
  "new_car_variants",
  "newcarvariants",
  "car_variants",
  "carvariants",
  "features",
  "vehicle_features",
  "vehiclefeatures",
  "cars",
  "prices",
];

export const COLOR_COLLECTION_CANDIDATES = [
  "vehicle_colors_v2",
  "vehiclecolors",
  "colors",
  "car_colors",
  "carcolors",
  "vehicles",
  "features",
  "vehicle_features",
  "vehiclefeatures",
  "vehicle_variants",
  "vehiclevariants",
  "cars",
];

export const FEATURE_COLLECTION_CANDIDATES = [
  "vehicle_features",
  "vehiclefeatures",
  "features",
  "car_features",
  "carfeatures",
  "vehicles",
  "vehicle_variants",
  "vehiclevariants",
  "cars",
];

export const PRICE_HISTORY_COLLECTION_CANDIDATES = [
  "vehicle_price_history",
  "vehiclepricehistory",
  "price_history",
  "pricehistory",
  "vehicle_prices",
  "vehicleprices",
  "prices",
];

export const getMongooseDb = async () => {
  if (mongoose.connection?.readyState === 1 && mongoose.connection?.db) {
    return mongoose.connection.db;
  }

  return null;
};

export const listDbCollections = async (db) => {
  if (!db) return [];

  try {
    return await db.listCollections().toArray();
  } catch {
    return [];
  }
};

export const normalizeCollectionName = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .trim();

export const findCollectionName = async (db, candidates = []) => {
  const collections = await listDbCollections(db);
  const names = collections.map((item) => item.name).filter(Boolean);

  if (!names.length) return "";

  const candidateKeys = candidates.map(normalizeCollectionName);

  const exact = names.find((name) =>
    candidateKeys.includes(normalizeCollectionName(name)),
  );

  if (exact) return exact;

  const contains = names.find((name) => {
    const nameKey = normalizeCollectionName(name);

    return candidateKeys.some((candidate) => nameKey.includes(candidate));
  });

  return contains || "";
};

export const getCollection = async (candidates = []) => {
  const db = await getMongooseDb();

  if (!db) {
    return {
      db: null,
      collection: null,
      collectionName: "",
      reason: "mongoose_not_connected",
    };
  }

  const collectionName = await findCollectionName(db, candidates);

  if (!collectionName) {
    return {
      db,
      collection: null,
      collectionName: "",
      reason: "collection_not_found",
    };
  }

  return {
    db,
    collection: db.collection(collectionName),
    collectionName,
    reason: "",
  };
};

export const safeFind = async (
  collection,
  query = {},
  {
    limit = 80,
    projection = {},
    sort = {},
  } = {},
) => {
  if (!collection) return [];

  try {
    return await collection
      .find(query, { projection })
      .sort(sort)
      .limit(limit)
      .toArray();
  } catch {
    try {
      return await collection.find({}).limit(limit).toArray();
    } catch {
      return [];
    }
  }
};
