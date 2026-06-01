#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const {
  buildVariantDecisionProfileFromSources,
  makeVariantLookupKey,
  makeVariantLooseLookupKey,
} = require('../../services/aciCore/decisionProfiles/aciVariantDecisionProfile.builder.cjs');

const {
  getFirst,
  normalizeFuelKey,
  normalizeTransmissionKey,
} = require('../../services/aciCore/decisionProfiles/aciDecisionProfileKeys.cjs');

const PRICE_COLLECTION = process.env.ACI_PRICE_ROWS_COLLECTION || 'aci_vehicle_price_rows';
const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const MODEL_SUMMARY_COLLECTION = process.env.ACI_MODEL_SUMMARY_COLLECTION || 'aci_vehicle_model_summary';
const TARGET_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const args = process.argv.slice(2);
const write = args.includes('--write');
const reset = args.includes('--reset');

const getArgNumber = (name, fallback) => {
  const hit = args.find((arg) => arg.startsWith(`--${name}=`));
  if (!hit) return fallback;
  const value = Number(hit.split('=').slice(1).join('='));
  return Number.isFinite(value) ? value : fallback;
};

const limit = getArgNumber('limit', 0);

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const normalizeHyphenKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const uniq = (arr) => [...new Set(arr.filter(Boolean))];

const addMapArray = (map, key, value) => {
  if (!key) return;
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(value);
};

const pickBestFeatureDoc = (docs) => {
  if (!docs || !docs.length) return null;
  return (
    docs.find((doc) => doc.activePricelistMatched === true) ||
    docs.find((doc) => doc.discontinuedPricelistMatched !== true) ||
    docs[0]
  );
};

const inferTransmission = (row) => {
  const fuel = getFirst(row, ['fuel', 'fuelType', 'fuel_type']);
  const fuelKey = normalizeFuelKey(fuel);

  const rawTransmission = getFirst(row, ['transmission', 'transmissionType']);
  const gearbox = getFirst(row, ['gearbox']);
  const variant = getFirst(row, ['variant', 'variantName', 'variantLabel', 'trim', 'version']);
  const variantKey = getFirst(row, ['variantKey']);

  let transmissionKey = normalizeTransmissionKey(rawTransmission || gearbox);
  const text = [variant, variantKey, rawTransmission, gearbox].filter(Boolean).join(' ').toLowerCase();

  if (!transmissionKey && fuelKey === 'electric') transmissionKey = 'automatic';
  if (!transmissionKey && /\b(dct|cvt|ivt|amt|automatic|auto)\b/i.test(text)) transmissionKey = 'automatic';
  if (!transmissionKey && /(^|[-_\s])at($|[-_\s])/i.test(text)) transmissionKey = 'automatic';
  if (!transmissionKey && /\b(mt|manual)\b/i.test(text)) transmissionKey = 'manual';

  const transmission =
    rawTransmission ||
    (transmissionKey === 'automatic' ? 'Automatic' : transmissionKey === 'manual' ? 'Manual' : '');

  return {
    transmission,
    transmissionKey,
    fuelKey,
    inferred: !rawTransmission && Boolean(transmissionKey),
  };
};

const normalizePriceRowForProfile = (row) => {
  const inferred = inferTransmission(row);

  return {
    ...row,
    fuelKey: row.fuelKey || inferred.fuelKey,
    transmission: row.transmission || inferred.transmission,
    transmissionKey: row.transmissionKey || inferred.transmissionKey,
  };
};

const preferPriceRow = (candidate, current) => {
  if (!current) return true;

  const cityRank = (row) => {
    const city = String(row.citySlug || row.city || '').toLowerCase();
    if (city.includes('new-delhi') || city === 'delhi') return 1;
    if (city.includes('noida')) return 2;
    if (city.includes('gurgaon') || city.includes('gurugram')) return 3;
    return 9;
  };

  const candidateRank = cityRank(candidate);
  const currentRank = cityRank(current);

  if (candidateRank !== currentRank) return candidateRank < currentRank;

  const candidatePrice = Number(candidate.exShowroomPrice || candidate.price || 0);
  const currentPrice = Number(current.exShowroomPrice || current.price || 0);

  if (candidatePrice && currentPrice && candidatePrice !== currentPrice) {
    return candidatePrice < currentPrice;
  }

  return false;
};

const FEATURE_KEYS_TO_PROJECT = [
  'sunroof',
  'panoramic_sunroof',
  'panorama_sunroof',
  'adas_package',
  'adas',
  'advanced_driver_assistance_systems',

  'six_airbags',
  'anti_lock_braking_system_abs',
  'abs',
  'electronic_brakeforce_distribution_ebd',
  'ebd',
  'electronic_stability_control_esc',
  'esc',
  'esp',
  'brake_assist',
  'traction_control',
  'tyre_pressure_monitoring_system_tpms',
  'tpms',
  'hill_assist',
  'hill_hold',
  'hill_descent_control',
  'isofix_child_seat_mounts',
  'isofix',

  'rear_camera',
  'camera_360',
  '360_degree_camera',
  'surround_view_camera',
  'front_parking_sensors',
  'front_parking_sensor',
  'rear_parking_sensors',
  'rear_parking_sensor',
  'parking_sensors',

  'ventilated_seats',
  'powered_driver_seat',
  'electric_driver_seat',
  'driver_electric_adjustable_seat',
  'powered_passenger_seat',
  'electric_passenger_seat',
  'passenger_electric_adjustable_seat',
  'leatherette_seats',
  'leatherette_upholstery',
  'upholstery',
  'automatic_climate_control',
  'rear_ac_vents',

  'cruise_control',
  'wireless_charging',
  'wireless_phone_charging',
  'touchscreen',
  'android_auto',
  'apple_carplay',
  'connected_car_features',
  'connected_car',
  'digital_cluster',
  'led_headlamps',
  'led_headlights',
  'alloy_wheels',
  'paddle_shifters',
  'drive_modes',
  'drive_mode',

  'max_power',
  'power',
  'max_torque',
  'torque',
  'kerb_weight',
  'arai_mileage',
  'petrol_mileage_arai',
  'diesel_mileage_arai',
  'cng_mileage_arai',
  'battery_capacity',
  'range',
  'claimed_range',

  'engine_type',
  'displacement',
  'engine_displacement',
  'number_of_cylinders',
  'turbo_charger',
  'super_charger',
  'drive_type',
  'fuel_tank_capacity',
  'petrol_fuel_tank_capacity',
  'diesel_fuel_tank_capacity',
  'length',
  'width',
  'height',
  'wheel_base',
  'ground_clearance_unladen',
  'ground_clearance',
  'seating_capacity',
  'boot_space',
];

const buildFeatureProjection = () => {
  const projection = {
    _id: 1,
    make: 1,
    makeKey: 1,
    brand: 1,
    brandKey: 1,
    model: 1,
    modelKey: 1,
    variant: 1,
    variantKey: 1,
    variantName: 1,
    activePricelistMatched: 1,
    discontinuedPricelistMatched: 1,
  };

  for (const key of FEATURE_KEYS_TO_PROJECT) {
    projection[`featuresByKey.${key}`] = 1;
  }

  return projection;
};

const buildFeatureIndexes = async (featureCollection) => {
  const index = new Map();
  const docs = [];
  const projection = buildFeatureProjection();

  const total = await featureCollection.estimatedDocumentCount();
  console.log(`[step] Feature matrix count=${total}; loading projected fields only...`);

  let loaded = 0;
  const cursor = featureCollection.find({}, { projection }).batchSize(250);

  for await (const doc of cursor) {
    docs.push(doc);
    loaded += 1;

    if (loaded % 250 === 0) {
      console.log(`[step] Loaded feature docs ${loaded}/${total}`);
    }

    const makeKeys = uniq([
      doc.makeKey,
      doc.brandKey,
      normalizeKey(doc.make),
      normalizeKey(doc.brand),
    ]);

    const modelKeys = uniq([
      doc.modelKey,
      normalizeKey(doc.modelKey),
      normalizeKey(doc.model),
      normalizeHyphenKey(doc.modelKey),
      normalizeHyphenKey(doc.model),
    ]);

    const variantKeys = uniq([
      doc.variantKey,
      normalizeKey(doc.variantKey),
      normalizeKey(doc.variant),
      normalizeKey(doc.variantName),
      normalizeHyphenKey(doc.variantKey),
      normalizeHyphenKey(doc.variant),
      normalizeHyphenKey(doc.variantName),
    ]);

    for (const modelKey of modelKeys) {
      for (const variantKey of variantKeys) {
        addMapArray(index, `model:${modelKey}__${variantKey}`, doc);
      }
    }

    for (const makeKey of makeKeys) {
      for (const modelKey of modelKeys) {
        for (const variantKey of variantKeys) {
          addMapArray(index, `make:${makeKey}__${modelKey}__${variantKey}`, doc);
        }
      }
    }
  }

  console.log(`[step] Feature docs loaded=${loaded}/${total}`);

  return { docs, index };
};

const buildFeatureLookupKeys = (row) => {
  const make = getFirst(row, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']);
  const makeKey = getFirst(row, ['makeKey', 'brandKey']) || normalizeKey(make);

  const model = getFirst(row, ['model', 'modelName', 'fullModel', 'nameplate']);
  const modelKey = getFirst(row, ['modelKey']) || normalizeKey(model);

  const variant = getFirst(row, ['variant', 'variantName', 'variantLabel', 'trim', 'version']);
  const variantKey = getFirst(row, ['variantKey']) || normalizeKey(variant);

  const variantWithoutDualTone = String(variantKey || variant || '')
    .replace(/dual[-_ ]tone/gi, '')
    .replace(/[-_ ]dt$/i, '')
    .trim();

  const makeKeys = uniq([makeKey, normalizeKey(makeKey), normalizeKey(make)]);
  const modelKeys = uniq([
    modelKey,
    normalizeKey(modelKey),
    normalizeKey(model),
    normalizeHyphenKey(modelKey),
    normalizeHyphenKey(model),
  ]);

  const variantKeys = uniq([
    variantKey,
    normalizeKey(variantKey),
    normalizeKey(variant),
    normalizeHyphenKey(variantKey),
    normalizeHyphenKey(variant),
    normalizeKey(variantWithoutDualTone),
    normalizeHyphenKey(variantWithoutDualTone),
  ]);

  const keys = [];

  for (const makeKeyCandidate of makeKeys) {
    for (const modelKeyCandidate of modelKeys) {
      for (const variantKeyCandidate of variantKeys) {
        keys.push(`make:${makeKeyCandidate}__${modelKeyCandidate}__${variantKeyCandidate}`);
      }
    }
  }

  for (const modelKeyCandidate of modelKeys) {
    for (const variantKeyCandidate of variantKeys) {
      keys.push(`model:${modelKeyCandidate}__${variantKeyCandidate}`);
    }
  }

  return uniq(keys);
};

const findFeatureDocFromMemory = (row, featureIndex) => {
  const lookupKeys = buildFeatureLookupKeys(row);

  for (const key of lookupKeys) {
    const docs = featureIndex.get(key);
    const picked = pickBestFeatureDoc(docs);
    if (picked) {
      return {
        doc: picked,
        matchKey: key,
      };
    }
  }

  return {
    doc: null,
    matchKey: null,
  };
};

const buildBrandModelKey = (doc) => {
  const make = getFirst(doc, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']);
  const makeKey = getFirst(doc, ['makeKey', 'brandKey']) || normalizeKey(make);
  const model = getFirst(doc, ['model', 'modelName', 'fullModel', 'nameplate']);
  const modelKey = getFirst(doc, ['modelKey']) || normalizeKey(model);
  return makeKey && modelKey ? `${makeKey}_${modelKey}` : null;
};

const buildModelSummaryIndexes = async (modelSummaryCollection) => {
  const byBrandModel = new Map();
  const byModel = new Map();
  const docs = await modelSummaryCollection.find({}).toArray();

  for (const doc of docs) {
    const brandModelKey = buildBrandModelKey(doc);
    const modelKey = getFirst(doc, ['modelKey']) || normalizeKey(getFirst(doc, ['model', 'modelName', 'fullModel']));

    if (brandModelKey) byBrandModel.set(brandModelKey, doc);
    if (modelKey) {
      byModel.set(modelKey, doc);
      byModel.set(normalizeKey(modelKey), doc);
      byModel.set(normalizeHyphenKey(modelKey), doc);
    }
  }

  return {
    docs,
    byBrandModel,
    byModel,
  };
};

const findModelSummary = (row, modelIndexes) => {
  const brandModelKey = buildBrandModelKey(row);
  if (brandModelKey && modelIndexes.byBrandModel.has(brandModelKey)) {
    return modelIndexes.byBrandModel.get(brandModelKey);
  }

  const model = getFirst(row, ['model', 'modelName', 'fullModel', 'nameplate']);
  const modelKey = getFirst(row, ['modelKey']) || normalizeKey(model);

  return (
    modelIndexes.byModel.get(modelKey) ||
    modelIndexes.byModel.get(normalizeKey(modelKey)) ||
    modelIndexes.byModel.get(normalizeHyphenKey(modelKey)) ||
    null
  );
};

const flushBulk = async (collection, bulk) => {
  if (!bulk.length) return { upserted: 0, modified: 0 };
  const result = await collection.bulkWrite(bulk, { ordered: false });
  return {
    upserted: result.upsertedCount || 0,
    modified: result.modifiedCount || 0,
  };
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI. Set MONGODB_URI or MONGO_URI.');
    process.exit(1);
  }

  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;

  const priceRows = db.collection(PRICE_COLLECTION);
  const featureRows = db.collection(FEATURE_COLLECTION);
  const modelSummaries = db.collection(MODEL_SUMMARY_COLLECTION);
  const target = db.collection(TARGET_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, reset=${reset}, limit=${limit || 'all'}`);

  if (write && reset) {
    await target.deleteMany({});
    console.log(`[reset] cleared ${TARGET_COLLECTION}`);
  }

  console.log('[step] Loading feature matrix into memory...');
  const featureIndexes = await buildFeatureIndexes(featureRows);
  console.log(`[step] Feature docs loaded=${featureIndexes.docs.length}, indexKeys=${featureIndexes.index.size}`);

  console.log('[step] Loading model summaries into memory...');
  const modelIndexes = await buildModelSummaryIndexes(modelSummaries);
  console.log(`[step] Model summaries loaded=${modelIndexes.docs.length}`);

  console.log('[step] Grouping price rows into unique global variants...');
  const grouped = new Map();
  let scannedPriceRows = 0;
  let skippedNoKey = 0;

  let cursor = priceRows.find({}).batchSize(1000);
  if (limit > 0) cursor = cursor.limit(limit);

  for await (const rawRow of cursor) {
    scannedPriceRows += 1;

    const row = normalizePriceRowForProfile(rawRow);
    const key = makeVariantLookupKey(row) || makeVariantLooseLookupKey(row);

    if (!key) {
      skippedNoKey += 1;
      continue;
    }

    const current = grouped.get(key);
    if (!current || preferPriceRow(row, current)) {
      grouped.set(key, row);
    }
  }

  console.log(`[step] scannedPriceRows=${scannedPriceRows}, uniqueVariants=${grouped.size}, skippedNoKey=${skippedNoKey}`);

  console.log('[step] Building profiles...');
  const docs = [];
  let featureHits = 0;
  let featureMisses = 0;
  let modelSummaryHits = 0;
  let modelSummaryMisses = 0;
  const featureMissSamples = [];

  for (const row of grouped.values()) {
    const featureMatch = findFeatureDocFromMemory(row, featureIndexes.index);
    const modelSummary = findModelSummary(row, modelIndexes);

    if (featureMatch.doc) featureHits += 1;
    else {
      featureMisses += 1;
      if (featureMissSamples.length < 40) {
        featureMissSamples.push({
          make: row.make,
          model: row.model,
          variant: row.variant,
          makeKey: row.makeKey,
          modelKey: row.modelKey,
          variantKey: row.variantKey,
          fuel: row.fuel,
          transmission: row.transmission,
        });
      }
    }

    if (modelSummary) modelSummaryHits += 1;
    else modelSummaryMisses += 1;

    const profile = buildVariantDecisionProfileFromSources({
      priceRow: row,
      featureDoc: featureMatch.doc,
      modelSummary,
    });

    profile.fastBuild = {
      builtAt: new Date(),
      builder: 'buildVariantDecisionProfilesFastV2',
      featureMatchKey: featureMatch.matchKey,
      featureDocId: featureMatch.doc ? String(featureMatch.doc._id) : null,
    };

    docs.push(profile);
  }

  const summary = docs.reduce(
    (acc, doc) => {
      acc.high += doc.dataQuality.confidenceTier === 'high' ? 1 : 0;
      acc.medium += doc.dataQuality.confidenceTier === 'medium' ? 1 : 0;
      acc.low += doc.dataQuality.confidenceTier === 'low' ? 1 : 0;
      acc.hasFeatureMatrix += doc.dataQuality.hasFeatureMatrix ? 1 : 0;
      acc.hasPerformanceData += doc.dataQuality.hasPerformanceData ? 1 : 0;
      acc.hasMileageData += doc.dataQuality.hasMileageData ? 1 : 0;
      acc.hasDimensionsData += doc.dataQuality.hasDimensionsData ? 1 : 0;
      return acc;
    },
    {
      high: 0,
      medium: 0,
      low: 0,
      hasFeatureMatrix: 0,
      hasPerformanceData: 0,
      hasMileageData: 0,
      hasDimensionsData: 0,
    }
  );

  const duplicateKeys = docs.reduce((acc, doc) => {
    acc[doc.variantProfileKey] = (acc[doc.variantProfileKey] || 0) + 1;
    return acc;
  }, {});

  const duplicateKeyCount = Object.values(duplicateKeys).filter((count) => count > 1).length;

  let writeResult = null;

  if (write) {
    await target.createIndex({ variantProfileKey: 1 }, { unique: true });

    let upserted = 0;
    let modified = 0;
    let bulk = [];

    for (const doc of docs) {
      const { createdAt, ...setDoc } = doc;

      bulk.push({
        updateOne: {
          filter: { variantProfileKey: doc.variantProfileKey },
          update: {
            $set: setDoc,
            $setOnInsert: { createdAt: createdAt || new Date() },
          },
          upsert: true,
        },
      });

      if (bulk.length >= 500) {
        const result = await flushBulk(target, bulk);
        upserted += result.upserted;
        modified += result.modified;
        bulk = [];
      }
    }

    const result = await flushBulk(target, bulk);
    upserted += result.upserted;
    modified += result.modified;

    writeResult = { upserted, modified };
  }

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    scannedPriceRows,
    uniqueVariants: grouped.size,
    builtProfiles: docs.length,
    skippedNoKey,
    featureHits,
    featureMisses,
    modelSummaryHits,
    modelSummaryMisses,
    duplicateKeyCount,
    summary,
    featureMissSamples,
    writeResult,
    sampleProfiles: docs.slice(0, 5).map((doc) => ({
      variantProfileKey: doc.variantProfileKey,
      variantFullName: doc.variantFullName,
      fuelTransmissionFamilyKey: doc.fuelTransmissionFamilyKey,
      hasFeatureMatrix: doc.dataQuality.hasFeatureMatrix,
      confidenceTier: doc.dataQuality.confidenceTier,
      referenceExShowroomPrice: doc.referenceExShowroomPrice,
    })),
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
