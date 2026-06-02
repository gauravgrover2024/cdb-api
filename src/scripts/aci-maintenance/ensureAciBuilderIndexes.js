import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const sameKey = (a = {}, b = {}) => JSON.stringify(a) === JSON.stringify(b);

const ensure = async (db, collectionName, keys, options = {}) => {
  const collection = db.collection(collectionName);
  const indexes = await collection.indexes().catch((error) => {
    if (error?.code === 26 || error?.codeName === "NamespaceNotFound") return [];
    throw error;
  });

  const existingSameKey = indexes.find((index) => sameKey(index.key, keys));
  if (existingSameKey) {
    console.log(`exists ${collectionName}.${existingSameKey.name}`, keys);
    return { collectionName, name: existingSameKey.name, status: "exists" };
  }

  const name =
    options.name ||
    Object.entries(keys)
      .map(([key, direction]) => `${key.replace(/[^a-zA-Z0-9]+/g, "_")}_${direction}`)
      .join("_");

  try {
    await collection.createIndex(keys, {
      background: true,
      ...options,
      name,
    });
    console.log(`created ${collectionName}.${name}`, keys);
    return { collectionName, name, status: "created" };
  } catch (error) {
    if (error?.codeName === "IndexOptionsConflict" || error?.codeName === "IndexKeySpecsConflict") {
      console.warn(`skipped conflicting index ${collectionName}.${name}: ${error.message}`);
      return { collectionName, name, status: "conflict", message: error.message };
    }
    throw error;
  }
};

await connectDB();
const db = mongoose.connection.db;

const existingCollections = new Set(
  (await db.listCollections({}, { nameOnly: true }).toArray()).map((entry) => entry.name),
);

const specs = [
  // Source vehicle read-model and feature-KB scans / lookup repairs.
  ["vehicles", { citySlug: 1, make: 1, model: 1, variant: 1 }, { name: "aci_builder_vehicle_city_make_model_variant" }],
  ["vehicles", { city: 1, brand: 1, model: 1, variant: 1 }, { name: "aci_builder_vehicle_city_brand_model_variant" }],
  ["vehicles", { model_normalized: 1, variant_normalized: 1, city: 1 }, { name: "aci_builder_vehicle_normalized_variant_city" }],
  ["vehicles", { brand_normalized: 1, model_normalized: 1, variant_normalized: 1, city: 1 }, { name: "aci_builder_vehicle_brand_model_variant_city" }],
  ["vehicles", { is_discontinued: 1, active: 1, is_active: 1, model: 1 }, { name: "aci_builder_vehicle_active_model" }],
  ["vehicles", { is_discontinued: 1, discontinued_date: 1, citySlug: 1, brand: 1, model: 1, variant: 1 }, { name: "aci_builder_vehicle_lifecycle_city_slug_variant" }],
  ["vehicles", { is_discontinued: 1, discontinued_date: 1, city: 1, brand: 1, model: 1, variant: 1 }, { name: "aci_builder_vehicle_lifecycle_city_variant" }],
  ["vehicles", { source: 1, scrape_timestamp: -1 }, { name: "aci_builder_vehicle_source_scrape_timestamp" }],

  // Raw feature source lookups used by read-model transmission extraction, feature KB, repairs and raw P0 audits.
  ["vehicle_features", { modelKey: 1, variantKey: 1, activePricelistMatched: 1 }, { name: "aci_builder_vf_model_variant_active" }],
  ["vehicle_features", { makeKey: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_vf_make_model_variant" }],
  ["vehicle_features", { brandKey: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_vf_brand_model_variant" }],
  ["vehicle_features", { model_slug: 1, variant_slug: 1 }, { name: "aci_builder_vf_slug_variant" }],
  ["vehicle_features", { brand: 1, model: 1, variant: 1 }, { name: "aci_builder_vf_brand_model_variant_text" }],
  ["vehicle_features", { make: 1, model: 1, variant: 1 }, { name: "aci_builder_vf_make_model_variant_text" }],
  ["vehicle_features", { featuresHash: 1, brand: 1, model: 1, variant: 1 }, { name: "aci_builder_vf_hash_identity" }],
  // Optional raw collection indexes are created only when the collection exists.
  // Do not index the huge `features` object itself; it bloats storage and does not
  // help the builders that must read the object for normalization.

  // Color hero lookup.
  ["vehicle_colors_v2", { brand_slug: 1, model_slug: 1, activeColorCount: -1, updatedAt: -1 }, { name: "aci_builder_color_slug_active_updated" }],
  ["vehicle_colors_v2", { brand: 1, model: 1, activeColorCount: -1, updatedAt: -1 }, { name: "aci_builder_color_brand_model_active_updated" }],

  // Derived price/model read models.
  ["aci_vehicle_price_rows", { makeKey: 1, modelKey: 1, variantKey: 1, citySlug: 1 }, { name: "aci_builder_price_identity" }],
  ["aci_vehicle_price_rows", { makeKey: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_price_variant_lookup" }],
  ["aci_vehicle_price_rows", { modelKey: 1, variantKey: 1, fuelKey: 1, transmissionKey: 1 }, { name: "aci_builder_price_model_variant_powertrain" }],
  ["aci_vehicle_price_rows", { citySlug: 1, exShowroomPrice: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_price_city_budget_variant" }],
  ["aci_vehicle_price_rows", { make: 1, model: 1, variant: 1 }, { name: "aci_builder_price_text_variant" }],
  ["aci_vehicle_price_rows", { sourceVehicleId: 1 }, { name: "aci_builder_price_source_vehicle_id" }],
  ["aci_vehicle_price_rows", { makeKey: 1, modelKey: 1, variantKey: 1, citySlug: 1, sourceVehicleId: 1 }, { name: "aci_builder_price_identity_source_vehicle" }],
  ["aci_vehicle_model_summary", { makeKey: 1, modelKey: 1, citySlug: 1 }, { name: "aci_builder_model_summary_identity" }],
  ["aci_vehicle_model_summary", { modelKey: 1, citySlug: 1, minExShowroomPrice: 1 }, { name: "aci_builder_model_summary_model_city_price" }],

  // Feature matrix builder, feature answers, crash extraction and evidence resolution.
  ["vehicle_feature_catalog_v2", { canonicalKey: 1 }, { name: "aci_builder_feature_catalog_canonical" }],
  ["vehicle_feature_catalog_v2", { aliases: 1 }, { name: "aci_builder_feature_catalog_aliases" }],
  ["vehicle_variant_feature_matrix_v2", { modelKey: 1, variantKey: 1, activePricelistMatched: 1 }, { name: "aci_builder_matrix_model_variant_active" }],
  ["vehicle_variant_feature_matrix_v2", { makeKey: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_matrix_make_model_variant" }],
  ["vehicle_variant_feature_matrix_v2", { brandKey: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_matrix_brand_model_variant" }],
  ["vehicle_variant_feature_matrix_v2", { activePricelistMatched: 1, modelKey: 1, priceMin: 1, variantKey: 1 }, { name: "aci_builder_matrix_active_model_price_variant" }],
  ["vehicle_variant_feature_matrix_v2", { "featuresByKey.global_ncap_safety_rating": 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_matrix_global_ncap_docs", sparse: true }],
  ["vehicle_variant_feature_matrix_v2", { "featuresByKey.bharat_ncap_safety_rating": 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_matrix_bharat_ncap_docs", sparse: true }],
  ["vehicle_variant_feature_matrix_v2", { "featuresByKey.gearbox": 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_matrix_gearbox_docs", sparse: true }],

  // Decision profile family, coverage and patching.
  ["aci_vehicle_variant_decision_profile", { variantProfileKey: 1 }, { name: "aci_builder_decision_variant_key", unique: true }],
  ["aci_vehicle_variant_decision_profile", { makeKey: 1, modelKey: 1, fuelTransmissionFamilyKey: 1, priceRank: 1 }, { name: "aci_builder_decision_model_family_rank" }],
  ["aci_vehicle_variant_decision_profile", { modelKey: 1, fuelKey: 1, transmissionKey: 1, referenceExShowroomPrice: 1 }, { name: "aci_builder_decision_model_powertrain_price" }],
  ["aci_vehicle_variant_decision_profile", { "dataQuality.hasFeatureMatrix": 1, makeKey: 1, modelKey: 1 }, { name: "aci_builder_decision_feature_quality" }],
  ["aci_vehicle_variant_decision_profile", { lifecycleStatus: 1, dataStatus: 1, makeKey: 1, modelKey: 1 }, { name: "aci_builder_decision_lifecycle_status_model" }],
  ["aci_vehicle_variant_decision_profile", { "dataQuality.hasFeatureMatrix": 1, lifecycleStatus: 1, makeKey: 1, modelKey: 1 }, { name: "aci_builder_decision_feature_quality_lifecycle" }],
  ["aci_vehicle_variant_decision_profile", { fuelTransmissionFamilyKey: 1 }, { name: "aci_builder_decision_fuel_transmission_family" }],

  // City overlay, ladder, crash and gaps.
  ["aci_vehicle_variant_city_price_profile", { cityPriceProfileKey: 1 }, { name: "aci_builder_city_price_key", unique: true }],
  ["aci_vehicle_variant_city_price_profile", { variantProfileKey: 1, citySlug: 1 }, { name: "aci_builder_city_price_variant_city", unique: true }],
  ["aci_vehicle_variant_city_price_profile", { citySlug: 1, exShowroomPrice: 1, modelKey: 1 }, { name: "aci_builder_city_price_city_model_budget" }],
  ["aci_vehicle_variant_upgrade_ladder", { ladderKey: 1 }, { name: "aci_builder_ladder_key", unique: true }],
  ["aci_vehicle_variant_upgrade_ladder", { variantProfileKey: 1 }, { name: "aci_builder_ladder_variant" }],
  ["aci_vehicle_variant_upgrade_ladder", { groupKey: 1, priceRank: 1 }, { name: "aci_builder_ladder_group_price_rank" }],
  ["aci_vehicle_crash_safety_profile", { crashSafetyProfileKey: 1 }, { name: "aci_builder_crash_key", unique: true }],
  ["aci_vehicle_crash_safety_profile", { hasCrashRating: 1, variantProfileKey: 1 }, { name: "aci_builder_crash_rating_variant" }],
  ["aci_variant_data_gap_queue", { gapKey: 1 }, { name: "aci_builder_gap_key", unique: true }],
  ["aci_variant_data_gap_queue", { status: 1, priority: 1, gapType: 1, makeKey: 1, modelKey: 1 }, { name: "aci_builder_gap_work_queue" }],
  ["aci_variant_data_gap_queue", { status: 1, priority: 1, lifecycleStatus: 1, gapType: 1 }, { name: "aci_builder_gap_lifecycle_work_queue" }],
  ["aci_variant_data_gap_queue", { variantProfileKey: 1, gapType: 1 }, { name: "aci_builder_gap_variant_type" }],
  ["aci_variant_external_evidence", { evidenceKey: 1 }, { name: "aci_builder_evidence_key", unique: true }],
  ["aci_variant_external_evidence", { priority: 1, status: 1, evidenceType: 1, makeKey: 1, modelKey: 1, variantKey: 1 }, { name: "aci_builder_evidence_seed_queue" }],
  ["aci_variant_external_evidence", { evidenceType: 1, status: 1, reviewStatus: 1, variantProfileKey: 1 }, { name: "aci_builder_evidence_patch_queue" }],
];

const results = [];
for (const [collectionName, keys, options] of specs) {
  if (!existingCollections.has(collectionName)) {
    console.log(`missing ${collectionName}; skipped`, keys);
    results.push({ collectionName, name: options.name, status: "missing_collection" });
    continue;
  }
  results.push(await ensure(db, collectionName, keys, options));
}

  await ensure(db, "aci_vehicle_feature_snapshot_v1", {
    sourceFeatureDocId: 1,
  }, {
    name: "aci_feature_snapshot_source_doc",
    unique: true,
  });

  await ensure(db, "aci_vehicle_feature_snapshot_v1", {
    modelKey: 1,
    variantKey: 1,
  }, {
    name: "aci_feature_snapshot_model_variant",
  });

  await ensure(db, "aci_vehicle_feature_snapshot_v1", {
    sourceFeaturesHash: 1,
    modelKey: 1,
    variantKey: 1,
  }, {
    name: "aci_feature_snapshot_hash_variant",
  });

  await ensure(db, "aci_vehicle_feature_snapshot_v1", {
    activeForFeatureExplorer: 1,
    modelKey: 1,
    variantKey: 1,
  }, {
    name: "aci_feature_snapshot_active_model_variant",
  });


const summary = results.reduce((acc, result) => {
  acc[result.status] = (acc[result.status] || 0) + 1;
  return acc;
}, {});

console.log(JSON.stringify({ ok: true, indexes: results.length, summary }, null, 2));


await mongoose.disconnect();
