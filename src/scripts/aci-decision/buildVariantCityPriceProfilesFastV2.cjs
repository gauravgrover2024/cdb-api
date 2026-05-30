#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const {
  makeVariantLookupKey,
  makeVariantLooseLookupKey,
} = require('../../services/aciCore/decisionProfiles/aciVariantDecisionProfile.builder.cjs');

const {
  getFirst,
  toNumber,
  normalizeFuelKey,
  normalizeTransmissionKey,
} = require('../../services/aciCore/decisionProfiles/aciDecisionProfileKeys.cjs');

const PRICE_COLLECTION = process.env.ACI_PRICE_ROWS_COLLECTION || 'aci_vehicle_price_rows';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const TARGET_COLLECTION = process.env.ACI_VARIANT_CITY_PRICE_PROFILE_COLLECTION || 'aci_vehicle_variant_city_price_profile';

const args = process.argv.slice(2);
const write = args.includes('--write');
const reset = args.includes('--reset');

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const slugText = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const normalizeLooseForMatch = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const formatINR = (value) => {
  const number = toNumber(value);
  if (!number) return null;
  return `₹${Math.round(number).toLocaleString('en-IN')}`;
};

const canonicalCity = (row) => {
  const raw = getFirst(row, ['citySlug', 'city', 'cityName', 'location.city']);
  const normalized = slugText(raw);

  if (['new-delhi', 'delhi', 'new-delhi-delhi', 'delhi-ncr'].includes(normalized)) {
    return { citySlug: 'new-delhi', city: 'New Delhi', supported: true };
  }

  if (normalized === 'noida') {
    return { citySlug: 'noida', city: 'Noida', supported: true };
  }

  if (['gurgaon', 'gurugram'].includes(normalized)) {
    return { citySlug: 'gurgaon', city: 'Gurgaon', supported: true };
  }

  return {
    citySlug: normalized || null,
    city: raw || null,
    supported: false,
  };
};

const inferTransmission = (row) => {
  const fuelKey = normalizeFuelKey(getFirst(row, ['fuel', 'fuelType', 'fuel_type']));
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

  return { transmission, transmissionKey, fuelKey };
};

const buildLooseIndexKey = ({ looseKey, fuelKey, transmissionKey }) =>
  [looseKey, fuelKey || 'unknown_fuel', transmissionKey || 'unknown_transmission'].join('__');

const buildLooseFuelIndexKey = ({ looseKey, fuelKey }) =>
  [looseKey, fuelKey || 'unknown_fuel'].join('__');

const addMapArray = (map, key, value) => {
  if (!key) return;
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(value);
};

const onlyOne = (items) => (Array.isArray(items) && items.length === 1 ? items[0] : null);

const PROFILE_PROJECTION = {
  _id: 0,
  variantProfileKey: 1,
  lookupKeys: 1,
  make: 1,
  makeKey: 1,
  model: 1,
  modelKey: 1,
  fullModel: 1,
  variant: 1,
  variantKey: 1,
  variantFullName: 1,
  fuel: 1,
  fuelKey: 1,
  transmission: 1,
  transmissionKey: 1,
  fuelTransmissionFamilyKey: 1,
};

const loadProfileIndexes = async (profilesCollection) => {
  const exact = new Map();
  const looseFuelTransmission = new Map();
  const looseFuel = new Map();
  const normalizedLooseFuelTransmission = new Map();
  const normalizedLooseFuel = new Map();

  const total = await profilesCollection.estimatedDocumentCount();
  console.log(`[load] Loading projected variant profiles ${total}...`);

  let loaded = 0;
  const cursor = profilesCollection.find({}, { projection: PROFILE_PROJECTION }).batchSize(500);

  for await (const profile of cursor) {
    loaded += 1;

    if (loaded % 500 === 0) {
      console.log(`[load] Loaded profiles ${loaded}/${total}`);
    }

    if (profile.variantProfileKey) exact.set(profile.variantProfileKey, profile);
    if (profile.lookupKeys?.exact) exact.set(profile.lookupKeys.exact, profile);

    const looseKeys = [
      profile.lookupKeys?.loose,
      `${profile.makeKey}_${profile.modelKey}__${profile.variantKey}`,
    ].filter(Boolean);

    for (const looseKey of looseKeys) {
      addMapArray(
        looseFuelTransmission,
        buildLooseIndexKey({
          looseKey,
          fuelKey: profile.fuelKey,
          transmissionKey: profile.transmissionKey,
        }),
        profile
      );

      addMapArray(
        looseFuel,
        buildLooseFuelIndexKey({
          looseKey,
          fuelKey: profile.fuelKey,
        }),
        profile
      );

      const normalizedLoose = normalizeLooseForMatch(looseKey);

      addMapArray(
        normalizedLooseFuelTransmission,
        buildLooseIndexKey({
          looseKey: normalizedLoose,
          fuelKey: profile.fuelKey,
          transmissionKey: profile.transmissionKey,
        }),
        profile
      );

      addMapArray(
        normalizedLooseFuel,
        buildLooseFuelIndexKey({
          looseKey: normalizedLoose,
          fuelKey: profile.fuelKey,
        }),
        profile
      );
    }
  }

  console.log(`[load] Variant profiles loaded=${loaded}/${total}`);

  return {
    exact,
    looseFuelTransmission,
    looseFuel,
    normalizedLooseFuelTransmission,
    normalizedLooseFuel,
  };
};

const resolveProfile = (row, indexes) => {
  const inferred = inferTransmission(row);
  const normalizedRow = {
    ...row,
    fuelKey: row.fuelKey || inferred.fuelKey,
    transmission: row.transmission || inferred.transmission,
    transmissionKey: row.transmissionKey || inferred.transmissionKey,
  };

  const exactKey = makeVariantLookupKey(normalizedRow);
  if (exactKey && indexes.exact.has(exactKey)) {
    return {
      profile: indexes.exact.get(exactKey),
      matchType: 'exact',
      attempted: { exactKey },
    };
  }

  const looseKey = makeVariantLooseLookupKey(normalizedRow);
  const fuelKey = normalizeFuelKey(getFirst(normalizedRow, ['fuel', 'fuelType', 'fuel_type']));
  const transmissionKey = inferred.transmissionKey;
  const normalizedLoose = normalizeLooseForMatch(looseKey);

  const attempts = {
    exactKey,
    looseKey,
    normalizedLoose,
    fuelKey,
    transmissionKey,
  };

  const byLooseFuelTransmission =
    onlyOne(indexes.looseFuelTransmission.get(buildLooseIndexKey({ looseKey, fuelKey, transmissionKey }))) ||
    onlyOne(
      indexes.normalizedLooseFuelTransmission.get(
        buildLooseIndexKey({ looseKey: normalizedLoose, fuelKey, transmissionKey })
      )
    );

  if (byLooseFuelTransmission) {
    return { profile: byLooseFuelTransmission, matchType: 'loose_fuel_transmission', attempted: attempts };
  }

  const byLooseFuel =
    onlyOne(indexes.looseFuel.get(buildLooseFuelIndexKey({ looseKey, fuelKey }))) ||
    onlyOne(indexes.normalizedLooseFuel.get(buildLooseFuelIndexKey({ looseKey: normalizedLoose, fuelKey })));

  if (byLooseFuel) {
    return { profile: byLooseFuel, matchType: 'loose_fuel_single_candidate', attempted: attempts };
  }

  return { profile: null, matchType: 'unmatched', attempted: attempts };
};

const buildBreakup = (row) => {
  const fields = {
    exShowroomPrice: ['exShowroomPrice', 'ex_showroom_price', 'price', 'priceValue'],
    roadTax: ['roadTax', 'rto', 'registrationCharges', 'rtoCharges'],
    insurance: ['insurance', 'insuranceCharges', 'comprehensiveInsurance'],
    tcs: ['tcs'],
    fastag: ['fastag', 'fastTag'],
    handlingCharges: ['handlingCharges', 'logisticsCharges'],
    hypothecationCharges: ['hypothecationCharges'],
    otherCharges: ['otherCharges'],
    optionalAccessories: ['optionalAccessories', 'accessories'],
    extendedWarranty: ['extendedWarranty'],
  };

  const breakup = {};

  for (const [key, paths] of Object.entries(fields)) {
    const value = toNumber(getFirst(row, paths));
    if (value !== null) {
      breakup[key] = { amount: value, label: formatINR(value) };
    }
  }

  return breakup;
};

const buildCityPriceDoc = ({ row, profile, cityInfo, matchType }) => {
  const exShowroomPrice = toNumber(getFirst(row, ['exShowroomPrice', 'ex_showroom_price', 'price', 'priceValue']));
  const onRoadPrice = toNumber(getFirst(row, ['onRoadPrice', 'on_road_price', 'finalOnRoadPrice', 'totalOnRoadPrice']));

  const onRoadPriceWithoutOptional = toNumber(
    getFirst(row, [
      'onRoadPriceWithoutOptional',
      'onRoadWithoutOptional',
      'onRoadPriceExcludingOptional',
      'onRoadPrice',
      'finalOnRoadPrice',
      'totalOnRoadPrice',
    ])
  );

  const onRoadPriceWithOptional = toNumber(
    getFirst(row, ['onRoadPriceWithOptional', 'onRoadWithOptional', 'onRoadPriceIncludingOptional'])
  );

  const now = new Date();
  const priceUpdatedAt = getFirst(row, ['priceUpdatedAt', 'updatedAt', 'modifiedAt', 'createdAt']) || null;
  const cityPriceProfileKey = `${profile.variantProfileKey}__${cityInfo.citySlug}`;

  return {
    cityPriceProfileKey,
    variantProfileKey: profile.variantProfileKey,

    make: profile.make,
    makeKey: profile.makeKey,
    model: profile.model,
    modelKey: profile.modelKey,
    fullModel: profile.fullModel,
    variant: profile.variant,
    variantKey: profile.variantKey,
    variantFullName: profile.variantFullName,
    fuel: profile.fuel,
    fuelKey: profile.fuelKey,
    transmission: profile.transmission,
    transmissionKey: profile.transmissionKey,
    fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,

    city: cityInfo.city,
    citySlug: cityInfo.citySlug,
    supportedCity: true,

    exShowroomPrice,
    exShowroomPriceLabel: formatINR(exShowroomPrice),
    onRoadPrice,
    onRoadPriceLabel: formatINR(onRoadPrice),
    onRoadPriceWithoutOptional,
    onRoadPriceWithoutOptionalLabel: formatINR(onRoadPriceWithoutOptional),
    onRoadPriceWithOptional,
    onRoadPriceWithOptionalLabel: formatINR(onRoadPriceWithOptional),

    priceBreakup: buildBreakup(row),
    optionalAddons: getFirst(row, ['optionalAddons', 'optionalAccessoriesList', 'addons']) || [],

    availabilityStatus: getFirst(row, ['availabilityStatus', 'status']) || 'available',
    priceUpdatedAt,
    sourcePriceRowId: row._id ? String(row._id) : null,
    sourceCollection: PRICE_COLLECTION,
    matchType,
    sourceVersion: 'aci_variant_city_price_profile_fast_v2_2026_05_31',
    createdAt: now,
    updatedAt: now,
  };
};

const preferCityPriceDoc = (candidate, current) => {
  if (!current) return true;

  const candidateOnRoad = toNumber(candidate.onRoadPrice);
  const currentOnRoad = toNumber(current.onRoadPrice);

  if (candidateOnRoad && !currentOnRoad) return true;

  const candidateEx = toNumber(candidate.exShowroomPrice);
  const currentEx = toNumber(current.exShowroomPrice);

  if (candidateEx && !currentEx) return true;

  return false;
};

const hasIndexWithKeys = (indexes, keys) => {
  const wanted = JSON.stringify(keys);
  return indexes.some((idx) => JSON.stringify(idx.key) === wanted);
};

const safeCreateIndex = async (collection, keys, options = {}) => {
  let indexes = [];

  try {
    indexes = await collection.indexes();
  } catch (error) {
    // NamespaceNotFound is normal after --reset when the target collection has not been created yet.
    if (error?.code !== 26 && error?.codeName !== 'NamespaceNotFound') {
      throw error;
    }
    indexes = [];
  }

  if (hasIndexWithKeys(indexes, keys)) return;

  const name =
    options.name ||
    Object.entries(keys)
      .map(([k, v]) => `${k}_${v}`)
      .join('_');

  try {
    await collection.createIndex(keys, { ...options, name });
  } catch (error) {
    // If another index with same key got created between listIndexes and createIndex, do not fail.
    if (error?.code === 85 || error?.code === 86) {
      const freshIndexes = await collection.indexes().catch(() => []);
      if (hasIndexWithKeys(freshIndexes, keys)) return;
    }
    throw error;
  }
};

const flushBulk = async (collection, bulk) => {
  if (!bulk.length) return { upserted: 0, modified: 0 };
  const result = await collection.bulkWrite(bulk, { ordered: false });
  return { upserted: result.upsertedCount || 0, modified: result.modifiedCount || 0 };
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI.');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const prices = db.collection(PRICE_COLLECTION);
  const profiles = db.collection(PROFILE_COLLECTION);
  const target = db.collection(TARGET_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, reset=${reset}`);
  console.log(`[source] prices=${PRICE_COLLECTION}, profiles=${PROFILE_COLLECTION}`);
  console.log(`[target] ${TARGET_COLLECTION}`);

  if (write && reset) {
    await target.deleteMany({});
    console.log(`[reset] Cleared ${TARGET_COLLECTION}`);
  }

  const indexes = await loadProfileIndexes(profiles);

  let scanned = 0;
  let supportedCityRows = 0;
  let unsupportedCityRows = 0;
  let matched = 0;
  let unmatched = 0;

  const docsByKey = new Map();
  const unmatchedSamples = [];
  const unsupportedCitySamples = [];
  const byCity = {};
  const byMatchType = {};

  const cursor = prices.find({}).batchSize(1000);

  for await (const row of cursor) {
    scanned += 1;

    if (scanned % 1000 === 0) {
      console.log(`[scan] price rows scanned=${scanned}, matched=${matched}, unmatched=${unmatched}`);
    }

    const cityInfo = canonicalCity(row);

    if (!cityInfo.supported) {
      unsupportedCityRows += 1;
      if (unsupportedCitySamples.length < 20) {
        unsupportedCitySamples.push({
          city: cityInfo.city,
          citySlug: cityInfo.citySlug,
          make: row.make,
          model: row.model,
          variant: row.variant,
        });
      }
      continue;
    }

    supportedCityRows += 1;
    byCity[cityInfo.citySlug] = (byCity[cityInfo.citySlug] || 0) + 1;

    const resolved = resolveProfile(row, indexes);

    if (!resolved.profile) {
      unmatched += 1;
      if (unmatchedSamples.length < 40) {
        unmatchedSamples.push({
          make: row.make,
          model: row.model,
          variant: row.variant,
          makeKey: row.makeKey,
          modelKey: row.modelKey,
          variantKey: row.variantKey,
          fuel: row.fuel,
          transmission: row.transmission,
          citySlug: cityInfo.citySlug,
          attempted: resolved.attempted,
        });
      }
      continue;
    }

    matched += 1;
    byMatchType[resolved.matchType] = (byMatchType[resolved.matchType] || 0) + 1;

    const doc = buildCityPriceDoc({
      row,
      profile: resolved.profile,
      cityInfo,
      matchType: resolved.matchType,
    });

    const current = docsByKey.get(doc.cityPriceProfileKey);
    if (preferCityPriceDoc(doc, current)) {
      docsByKey.set(doc.cityPriceProfileKey, doc);
    }
  }

  const docs = [...docsByKey.values()];

  const duplicateKeyCount = matched - docs.length;
  let writeResult = null;

  if (write) {
    await safeCreateIndex(target, { cityPriceProfileKey: 1 }, { unique: true, name: 'city_price_profile_key_unique' });
    await safeCreateIndex(target, { variantProfileKey: 1, citySlug: 1 }, { unique: true, name: 'variant_city_unique' });
    await safeCreateIndex(target, { citySlug: 1, exShowroomPrice: 1 }, { name: 'city_price_exshowroom_idx' });
    await safeCreateIndex(target, { makeKey: 1, modelKey: 1, citySlug: 1 }, { name: 'city_price_make_model_city_idx' });
    await safeCreateIndex(target, { fuelKey: 1, transmissionKey: 1, citySlug: 1 }, { name: 'city_price_fuel_transmission_city_idx' });

    let upserted = 0;
    let modified = 0;
    let bulk = [];

    for (const doc of docs) {
      const { createdAt, ...setDoc } = doc;

      bulk.push({
        updateOne: {
          filter: { cityPriceProfileKey: doc.cityPriceProfileKey },
          update: {
            $set: setDoc,
            $setOnInsert: { createdAt },
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
    scanned,
    supportedCityRows,
    unsupportedCityRows,
    matched,
    unmatched,
    builtDocs: docs.length,
    duplicateKeyCount,
    byCity,
    byMatchType,
    unmatchedSamples,
    unsupportedCitySamples,
    writeResult,
    sampleDocs: docs.slice(0, 10).map((doc) => ({
      cityPriceProfileKey: doc.cityPriceProfileKey,
      variantProfileKey: doc.variantProfileKey,
      variantFullName: doc.variantFullName,
      citySlug: doc.citySlug,
      exShowroomPrice: doc.exShowroomPrice,
      onRoadPrice: doc.onRoadPrice,
      matchType: doc.matchType,
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
