#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const {
  isInactiveDecisionProfile,
} = require('../../services/aciCore/lifecycle/aciVehicleLifecycle.cjs');

const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const CITY_PRICE_COLLECTION = process.env.ACI_VARIANT_CITY_PRICE_PROFILE_COLLECTION || 'aci_vehicle_variant_city_price_profile';
const LADDER_COLLECTION = process.env.ACI_VARIANT_UPGRADE_LADDER_COLLECTION || 'aci_vehicle_variant_upgrade_ladder';

const args = process.argv.slice(2);
const write = args.includes('--write');
const reset = args.includes('--reset');

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const SUPPORTED_CITIES = ['new-delhi', 'gurgaon', 'noida'];

const toNumber = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(String(value).replace(/[^\d.-]/g, ''));
  return Number.isFinite(num) ? num : null;
};

const formatINR = (value) => {
  const num = toNumber(value);
  if (num === null) return null;
  return `₹${Math.round(num).toLocaleString('en-IN')}`;
};

const compact = (obj) => {
  const out = {};
  for (const [key, value] of Object.entries(obj || {})) {
    if (value !== undefined) out[key] = value;
  }
  return out;
};

const trueFeatureKeys = (profile) => {
  const flags = profile.featureFlags || {};
  return Object.keys(flags)
    .filter((key) => flags[key] === true)
    .sort();
};

const knownFeatureKeys = (profile) => {
  const flags = profile.featureFlags || {};
  return Object.keys(flags)
    .filter((key) => flags[key] === true || flags[key] === false)
    .sort();
};

const hasDualToneSignal = (profile) => {
  const text = [
    profile.variant,
    profile.variantKey,
    profile.variantFullName,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return (
    profile.isDualToneOnly === true ||
    /\bdual\s*tone\b/i.test(text) ||
    /(^|[-_\s])dt($|[-_\s])/i.test(text)
  );
};

const hasCosmeticOrEditionSignal = (profile) => {
  const text = [
    profile.variant,
    profile.variantKey,
    profile.variantFullName,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return (
    profile.isCosmeticOnly === true ||
    profile.isDarkEdition === true ||
    profile.isAdventureEdition === true ||
    profile.isSpecialEdition === true ||
    /\bdark\b/i.test(text) ||
    /\badventure\b/i.test(text) ||
    /\bspecial\s*edition\b/i.test(text) ||
    /\bcelebration\s*edition\b/i.test(text) ||
    /\bnight\s*series\b/i.test(text)
  );
};

const shouldSkipAsDefaultUpgradeTarget = (profile) =>
  profile.shouldSkipInUpgradeLadder === true ||
  hasDualToneSignal(profile) ||
  hasCosmeticOrEditionSignal(profile);

const getGainedLostFeatures = (fromProfile, toProfile) => {
  const fromFlags = fromProfile.featureFlags || {};
  const toFlags = toProfile.featureFlags || {};

  const allKeys = [...new Set([...Object.keys(fromFlags), ...Object.keys(toFlags)])].sort();

  const gained = [];
  const lost = [];
  const newlyKnown = [];

  for (const key of allKeys) {
    const fromValue = fromFlags[key];
    const toValue = toFlags[key];

    if (fromValue !== true && toValue === true) gained.push(key);
    if (fromValue === true && toValue === false) lost.push(key);
    if ((fromValue === null || fromValue === undefined) && (toValue === true || toValue === false)) {
      newlyKnown.push(key);
    }
  }

  return { gained, lost, newlyKnown };
};

const roleFromRank = (priceRank, totalInGroup) => {
  if (!priceRank || !totalInGroup) return 'unknown';
  if (totalInGroup === 1) return 'single_variant';

  const normalized = (priceRank - 1) / Math.max(1, totalInGroup - 1);

  if (normalized <= 0.2) return 'entry';
  if (normalized >= 0.8) return 'top';
  return 'mid';
};

const safeCreateIndex = async (collection, keys, options = {}) => {
  let indexes = [];
  try {
    indexes = await collection.indexes();
  } catch (error) {
    if (error?.code !== 26 && error?.codeName !== 'NamespaceNotFound') throw error;
  }

  const wanted = JSON.stringify(keys);
  if (indexes.some((idx) => JSON.stringify(idx.key) === wanted)) return;

  const name =
    options.name ||
    Object.entries(keys)
      .map(([key, value]) => `${key}_${value}`)
      .join('_');

  try {
    await collection.createIndex(keys, { ...options, name });
  } catch (error) {
    if (error?.code === 85 || error?.code === 86) {
      const fresh = await collection.indexes().catch(() => []);
      if (fresh.some((idx) => JSON.stringify(idx.key) === wanted)) return;
    }
    throw error;
  }
};

const loadProfiles = async (collection) => {
  const projection = {
    _id: 0,
    variantProfileKey: 1,
    make: 1,
    makeKey: 1,
    model: 1,
    modelKey: 1,
    fullModel: 1,
    variant: 1,
    variantKey: 1,
    variantFullName: 1,
    brandModelKey: 1,
    fuel: 1,
    fuelKey: 1,
    transmission: 1,
    transmissionKey: 1,
    fuelTransmissionFamilyKey: 1,
    referenceExShowroomPrice: 1,
    referenceOnRoadPrice: 1,
    referencePriceCitySlug: 1,
    featureFlags: 1,
    dataQuality: 1,
    isCosmeticOnly: 1,
    isDualToneOnly: 1,
    isSpecialEdition: 1,
    shouldSkipInUpgradeLadder: 1,
    lifecycleStatus: 1,
    dataStatus: 1,
  };

  const total = await collection.estimatedDocumentCount();
  console.log(`[load] Loading profiles ${total}...`);

  const docs = [];
  const cursor = collection.find({}, { projection }).batchSize(500);

  for await (const doc of cursor) {
    if (isInactiveDecisionProfile(doc)) continue;

    docs.push(doc);
    if (docs.length % 500 === 0) {
      console.log(`[load] Loaded profiles ${docs.length}/${total}`);
    }
  }

  return docs;
};

const loadCityPrices = async (collection) => {
  const projection = {
    _id: 0,
    variantProfileKey: 1,
    citySlug: 1,
    exShowroomPrice: 1,
    onRoadPrice: 1,
    onRoadPriceWithoutOptional: 1,
  };

  const cityPrices = new Map();
  const cursor = collection.find({}, { projection }).batchSize(1000);

  for await (const doc of cursor) {
    const key = `${doc.variantProfileKey}__${doc.citySlug}`;
    cityPrices.set(key, doc);
  }

  return cityPrices;
};

const getCityPrice = (cityPrices, variantProfileKey, citySlug) =>
  cityPrices.get(`${variantProfileKey}__${citySlug}`) || null;

const buildCityDeltas = ({ cityPrices, fromProfile, toProfile }) => {
  const deltas = {};

  for (const citySlug of SUPPORTED_CITIES) {
    const fromPrice = getCityPrice(cityPrices, fromProfile.variantProfileKey, citySlug);
    const toPrice = getCityPrice(cityPrices, toProfile.variantProfileKey, citySlug);

    const fromOnRoad = toNumber(fromPrice?.onRoadPrice);
    const toOnRoad = toNumber(toPrice?.onRoadPrice);
    const fromEx = toNumber(fromPrice?.exShowroomPrice);
    const toEx = toNumber(toPrice?.exShowroomPrice);

    deltas[citySlug] = {
      fromOnRoadPrice: fromOnRoad,
      toOnRoadPrice: toOnRoad,
      onRoadDelta: fromOnRoad !== null && toOnRoad !== null ? toOnRoad - fromOnRoad : null,
      onRoadDeltaLabel: fromOnRoad !== null && toOnRoad !== null ? formatINR(toOnRoad - fromOnRoad) : null,
      fromExShowroomPrice: fromEx,
      toExShowroomPrice: toEx,
      exShowroomDelta: fromEx !== null && toEx !== null ? toEx - fromEx : null,
      exShowroomDeltaLabel: fromEx !== null && toEx !== null ? formatINR(toEx - fromEx) : null,
    };
  }

  return deltas;
};

const groupProfiles = (profiles) => {
  const groups = new Map();

  for (const profile of profiles) {
    const groupKey = [
      profile.brandModelKey || `${profile.makeKey}_${profile.modelKey}`,
      profile.fuelTransmissionFamilyKey || `${profile.fuelKey || 'unknown_fuel'}_${profile.transmissionKey || 'unknown_transmission'}`,
    ].join('__');

    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey).push(profile);
  }

  return groups;
};

const buildGroup = ({ groupKey, groupProfiles, cityPrices }) => {
  const sortedByPrice = [...groupProfiles].sort((a, b) => {
    const priceA = toNumber(a.referenceExShowroomPrice) ?? Number.MAX_SAFE_INTEGER;
    const priceB = toNumber(b.referenceExShowroomPrice) ?? Number.MAX_SAFE_INTEGER;

    if (priceA !== priceB) return priceA - priceB;

    const featureCountA = trueFeatureKeys(a).length;
    const featureCountB = trueFeatureKeys(b).length;
    return featureCountA - featureCountB;
  });

  const sortedByEquipment = [...groupProfiles].sort((a, b) => {
    const featureCountA = trueFeatureKeys(a).length;
    const featureCountB = trueFeatureKeys(b).length;

    if (featureCountA !== featureCountB) return featureCountA - featureCountB;

    const priceA = toNumber(a.referenceExShowroomPrice) ?? Number.MAX_SAFE_INTEGER;
    const priceB = toNumber(b.referenceExShowroomPrice) ?? Number.MAX_SAFE_INTEGER;
    return priceA - priceB;
  });

  const equipmentRankByKey = new Map();
  sortedByEquipment.forEach((profile, index) => {
    equipmentRankByKey.set(profile.variantProfileKey, index + 1);
  });

  const totalInGroup = sortedByPrice.length;

  const profileUpdates = [];
  const ladderDocs = [];

  const meaningfulUpgradeCandidates = sortedByPrice.filter((profile) => !profile.shouldSkipInUpgradeLadder);

  for (let index = 0; index < sortedByPrice.length; index += 1) {
    const profile = sortedByPrice[index];

    const priceRank = index + 1;
    const equipmentRank = equipmentRankByKey.get(profile.variantProfileKey) || null;
    const featureKeys = trueFeatureKeys(profile);
    const knownKeys = knownFeatureKeys(profile);

    const normalizedVariantRank =
      totalInGroup > 1 ? Number(((priceRank - 1) / (totalInGroup - 1)).toFixed(4)) : 0;

    const structuralRole = roleFromRank(priceRank, totalInGroup);

    const higherPricedCandidates = meaningfulUpgradeCandidates.filter((candidate) => {
      if (candidate.variantProfileKey === profile.variantProfileKey) return false;
      if (shouldSkipAsDefaultUpgradeTarget(candidate)) return false;

      const candidatePrice = toNumber(candidate.referenceExShowroomPrice);
      const currentPrice = toNumber(profile.referenceExShowroomPrice);
      if (candidatePrice === null || currentPrice === null) return false;

      return candidatePrice > currentPrice;
    });

    const nextPricedVariant = higherPricedCandidates[0] || null;

    const nextMeaningfulUpgrade =
      higherPricedCandidates.find((candidate) => {
        const diff = getGainedLostFeatures(profile, candidate);
        const currentHasFeatureData = profile.dataQuality?.hasFeatureMatrix === true;
        const candidateHasFeatureData = candidate.dataQuality?.hasFeatureMatrix === true;

        // Only call it a clean meaningful upgrade when both sides have feature evidence
        // and the higher-priced candidate does not have confirmed feature losses.
        if (!currentHasFeatureData || !candidateHasFeatureData) return false;
        if (diff.lost.length > 0) return false;
        if (diff.gained.length === 0) return false;

        return true;
      }) || null;

    const nextUpgrade = nextMeaningfulUpgrade || nextPricedVariant;

    const pricedVariantDiff = nextPricedVariant ? getGainedLostFeatures(profile, nextPricedVariant) : null;
    const meaningfulUpgradeDiff = nextMeaningfulUpgrade ? getGainedLostFeatures(profile, nextMeaningfulUpgrade) : null;
    const gainedLost = nextUpgrade ? getGainedLostFeatures(profile, nextUpgrade) : null;

    const edgeQuality = (() => {
      if (!nextPricedVariant) return 'no_higher_variant';
      if (nextMeaningfulUpgrade && nextMeaningfulUpgrade.variantProfileKey === nextPricedVariant.variantProfileKey) return 'clean';
      if (pricedVariantDiff?.lost?.length) return 'caveated_next_priced_variant_has_losses';
      if (!profile.dataQuality?.hasFeatureMatrix || !nextPricedVariant.dataQuality?.hasFeatureMatrix) return 'needs_feature_evidence';
      if (!pricedVariantDiff?.gained?.length) return 'needs_review_no_clear_gain';
      return 'caveated';
    })();

    const globalExDelta =
      nextUpgrade &&
      toNumber(nextUpgrade.referenceExShowroomPrice) !== null &&
      toNumber(profile.referenceExShowroomPrice) !== null
        ? toNumber(nextUpgrade.referenceExShowroomPrice) - toNumber(profile.referenceExShowroomPrice)
        : null;

    const globalOnRoadDelta =
      nextUpgrade &&
      toNumber(nextUpgrade.referenceOnRoadPrice) !== null &&
      toNumber(profile.referenceOnRoadPrice) !== null
        ? toNumber(nextUpgrade.referenceOnRoadPrice) - toNumber(profile.referenceOnRoadPrice)
        : null;

    const ladderDoc = {
      ladderKey: profile.variantProfileKey,
      groupKey,

      variantProfileKey: profile.variantProfileKey,
      variantFullName: profile.variantFullName,
      make: profile.make,
      makeKey: profile.makeKey,
      model: profile.model,
      modelKey: profile.modelKey,
      brandModelKey: profile.brandModelKey,
      fuel: profile.fuel,
      fuelKey: profile.fuelKey,
      transmission: profile.transmission,
      transmissionKey: profile.transmissionKey,
      fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,

      totalInGroup,
      priceRank,
      equipmentRank,
      normalizedVariantRank,
      structuralRole,

      featureCount: featureKeys.length,
      knownFeatureCount: knownKeys.length,
      isDualToneUpgradeCandidate: hasDualToneSignal(profile),
      isCosmeticOrEditionUpgradeCandidate: hasCosmeticOrEditionSignal(profile),
      skipInUpgradeLadder: Boolean(shouldSkipAsDefaultUpgradeTarget(profile)),

      nextPricedVariantProfileKey: nextPricedVariant?.variantProfileKey || null,
      nextPricedVariantFullName: nextPricedVariant?.variantFullName || null,
      nextPricedVariantLostFeatureKeys: pricedVariantDiff?.lost || [],
      nextPricedVariantGainedFeatureKeys: pricedVariantDiff?.gained || [],

      nextMeaningfulUpgradeVariantProfileKey: nextMeaningfulUpgrade?.variantProfileKey || null,
      nextMeaningfulUpgradeVariantFullName: nextMeaningfulUpgrade?.variantFullName || null,
      nextMeaningfulUpgradeGainedFeatureKeys: meaningfulUpgradeDiff?.gained || [],

      nextUpgradeVariantProfileKey: nextUpgrade?.variantProfileKey || null,
      nextUpgradeVariantFullName: nextUpgrade?.variantFullName || null,
      nextUpgradePriceRank: nextUpgrade ? sortedByPrice.findIndex((p) => p.variantProfileKey === nextUpgrade.variantProfileKey) + 1 : null,
      nextUpgradeEquipmentRank: nextUpgrade ? equipmentRankByKey.get(nextUpgrade.variantProfileKey) || null : null,
      upgradeEdgeQuality: edgeQuality,
      upgradeEdgeNeedsReview: ['caveated_next_priced_variant_has_losses', 'needs_feature_evidence', 'needs_review_no_clear_gain', 'caveated'].includes(edgeQuality),

      globalExShowroomDelta: globalExDelta,
      globalExShowroomDeltaLabel: formatINR(globalExDelta),
      globalOnRoadDelta: globalOnRoadDelta,
      globalOnRoadDeltaLabel: formatINR(globalOnRoadDelta),
      cityDeltas: nextUpgrade ? buildCityDeltas({ cityPrices, fromProfile: profile, toProfile: nextUpgrade }) : {},

      gainedFeatureKeys: gainedLost?.gained || [],
      lostFeatureKeys: gainedLost?.lost || [],
      newlyKnownFeatureKeys: gainedLost?.newlyKnown || [],

      evidence: {
        method: 'derived_from_price_order_and_feature_flags',
        noCarJudgementHardcoded: true,
        priceSource: 'aci_vehicle_variant_city_price_profile/reference profile price',
        featureSource: 'aci_vehicle_variant_decision_profile.featureFlags',
        upgradeEdgeRule: 'nextPricedVariant is structural; nextMeaningfulUpgrade requires feature evidence, gained features, and no confirmed feature losses',
        upgradeTargetRule: 'same model + same fuel + same transmission group; excludes dual-tone/cosmetic/special-edition targets by default',
      },

      sourceVersion: 'aci_variant_upgrade_ladder_v1_2026_05_31',
      updatedAt: new Date(),
      createdAt: new Date(),
    };

    ladderDocs.push(ladderDoc);

    profileUpdates.push({
      variantProfileKey: profile.variantProfileKey,
      set: compact({
        priceRank,
        equipmentRank,
        normalizedVariantRank,
        isBaseVariant: structuralRole === 'entry' || structuralRole === 'single_variant',
        isMidVariant: structuralRole === 'mid',
        isTopVariant: structuralRole === 'top' || structuralRole === 'single_variant',
        'variantRole.role': structuralRole,
        'variantRole.roleEvidence': {
          method: 'derived_from_price_rank_within_same_model_fuel_transmission_group',
          groupKey,
          priceRank,
          totalInGroup,
          noCarJudgementHardcoded: true,
        },
        updatedAt: new Date(),
      }),
    });
  }

  return { profileUpdates, ladderDocs };
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
    console.error('Missing Mongo URI.');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const profilesCol = db.collection(PROFILE_COLLECTION);
  const cityPricesCol = db.collection(CITY_PRICE_COLLECTION);
  const ladderCol = db.collection(LADDER_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, reset=${reset}`);
  console.log(`[source] profiles=${PROFILE_COLLECTION}, cityPrices=${CITY_PRICE_COLLECTION}`);
  console.log(`[target] ladder=${LADDER_COLLECTION}`);

  const profiles = await loadProfiles(profilesCol);
  const cityPrices = await loadCityPrices(cityPricesCol);

  const groups = groupProfiles(profiles);

  console.log(`[build] profiles=${profiles.length}, cityPrices=${cityPrices.size}, groups=${groups.size}`);

  const allProfileUpdates = [];
  const allLadderDocs = [];
  const groupSummaries = [];

  for (const [groupKey, groupProfiles] of groups.entries()) {
    const { profileUpdates, ladderDocs } = buildGroup({ groupKey, groupProfiles, cityPrices });

    allProfileUpdates.push(...profileUpdates);
    allLadderDocs.push(...ladderDocs);

    if (groupSummaries.length < 40) {
      groupSummaries.push({
        groupKey,
        count: groupProfiles.length,
        first: ladderDocs[0]?.variantFullName || null,
        last: ladderDocs[ladderDocs.length - 1]?.variantFullName || null,
      });
    }
  }

  const noNextUpgrade = allLadderDocs.filter((doc) => !doc.nextUpgradeVariantProfileKey).length;
  const withNextUpgrade = allLadderDocs.length - noNextUpgrade;

  const duplicateLadderKeys = allLadderDocs.reduce((acc, doc) => {
    acc[doc.ladderKey] = (acc[doc.ladderKey] || 0) + 1;
    return acc;
  }, {});

  const duplicateLadderKeyCount = Object.values(duplicateLadderKeys).filter((count) => count > 1).length;

  const blockedDefaultUpgradeTargets = allLadderDocs.filter((doc) => {
    if (!doc.nextUpgradeVariantFullName) return false;

    const targetLikeProfile = {
      variant: doc.nextUpgradeVariantFullName,
      variantFullName: doc.nextUpgradeVariantFullName,
      variantKey: doc.nextUpgradeVariantProfileKey || '',
    };

    return hasDualToneSignal(targetLikeProfile) || hasCosmeticOrEditionSignal(targetLikeProfile);
  });

  const blockedMeaningfulUpgradeTargets = allLadderDocs.filter((doc) => {
    if (!doc.nextMeaningfulUpgradeVariantFullName) return false;

    const targetLikeProfile = {
      variant: doc.nextMeaningfulUpgradeVariantFullName,
      variantFullName: doc.nextMeaningfulUpgradeVariantFullName,
      variantKey: doc.nextMeaningfulUpgradeVariantProfileKey || '',
    };

    return hasDualToneSignal(targetLikeProfile) || hasCosmeticOrEditionSignal(targetLikeProfile);
  });

  let writeResult = null;

  if (write) {
    if (reset) {
      await ladderCol.deleteMany({});
      console.log(`[reset] cleared ${LADDER_COLLECTION}`);
    }

    await safeCreateIndex(ladderCol, { ladderKey: 1 }, { unique: true, name: 'upgrade_ladder_key_unique' });
    await safeCreateIndex(ladderCol, { groupKey: 1, priceRank: 1 }, { name: 'upgrade_group_price_rank_idx' });
    await safeCreateIndex(ladderCol, { variantProfileKey: 1 }, { name: 'upgrade_variant_profile_idx' });
    await safeCreateIndex(ladderCol, { brandModelKey: 1, fuelTransmissionFamilyKey: 1 }, { name: 'upgrade_brand_model_fuel_transmission_idx' });

    let profileModified = 0;
    let profileBulk = [];

    for (const update of allProfileUpdates) {
      profileBulk.push({
        updateOne: {
          filter: { variantProfileKey: update.variantProfileKey },
          update: { $set: update.set },
        },
      });

      if (profileBulk.length >= 500) {
        const result = await flushBulk(profilesCol, profileBulk);
        profileModified += result.modified;
        profileBulk = [];
      }
    }

    const profileResult = await flushBulk(profilesCol, profileBulk);
    profileModified += profileResult.modified;

    let ladderUpserted = 0;
    let ladderModified = 0;
    let ladderBulk = [];

    for (const doc of allLadderDocs) {
      const { createdAt, ...setDoc } = doc;

      ladderBulk.push({
        updateOne: {
          filter: { ladderKey: doc.ladderKey },
          update: {
            $set: setDoc,
            $setOnInsert: { createdAt },
          },
          upsert: true,
        },
      });

      if (ladderBulk.length >= 500) {
        const result = await flushBulk(ladderCol, ladderBulk);
        ladderUpserted += result.upserted;
        ladderModified += result.modified;
        ladderBulk = [];
      }
    }

    const ladderResult = await flushBulk(ladderCol, ladderBulk);
    ladderUpserted += ladderResult.upserted;
    ladderModified += ladderResult.modified;

    writeResult = {
      profileModified,
      ladderUpserted,
      ladderModified,
    };
  }

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    profiles: profiles.length,
    cityPrices: cityPrices.size,
    groups: groups.size,
    profileUpdates: allProfileUpdates.length,
    ladderDocs: allLadderDocs.length,
    withNextUpgrade,
    noNextUpgrade,
    duplicateLadderKeyCount,
    blockedDefaultUpgradeTargetCount: blockedDefaultUpgradeTargets.length,
    blockedDefaultUpgradeTargetSamples: blockedDefaultUpgradeTargets.slice(0, 20).map((doc) => ({
      variantFullName: doc.variantFullName,
      groupKey: doc.groupKey,
      nextUpgradeVariantFullName: doc.nextUpgradeVariantFullName,
      upgradeEdgeQuality: doc.upgradeEdgeQuality,
    })),
    blockedMeaningfulUpgradeTargetCount: blockedMeaningfulUpgradeTargets.length,
    blockedMeaningfulUpgradeTargetSamples: blockedMeaningfulUpgradeTargets.slice(0, 20).map((doc) => ({
      variantFullName: doc.variantFullName,
      groupKey: doc.groupKey,
      nextMeaningfulUpgradeVariantFullName: doc.nextMeaningfulUpgradeVariantFullName,
      upgradeEdgeQuality: doc.upgradeEdgeQuality,
    })),
    groupSummaries,
    samples: allLadderDocs.slice(0, 12).map((doc) => ({
      ladderKey: doc.ladderKey,
      variantFullName: doc.variantFullName,
      groupKey: doc.groupKey,
      priceRank: doc.priceRank,
      equipmentRank: doc.equipmentRank,
      structuralRole: doc.structuralRole,
      nextPricedVariantFullName: doc.nextPricedVariantFullName,
      nextMeaningfulUpgradeVariantFullName: doc.nextMeaningfulUpgradeVariantFullName,
      nextUpgradeVariantFullName: doc.nextUpgradeVariantFullName,
      upgradeEdgeQuality: doc.upgradeEdgeQuality,
      upgradeEdgeNeedsReview: doc.upgradeEdgeNeedsReview,
      isDualToneUpgradeCandidate: doc.isDualToneUpgradeCandidate,
      isCosmeticOrEditionUpgradeCandidate: doc.isCosmeticOrEditionUpgradeCandidate,
      skipInUpgradeLadder: doc.skipInUpgradeLadder,
      globalExShowroomDelta: doc.globalExShowroomDelta,
      gainedFeatureKeys: doc.gainedFeatureKeys.slice(0, 10),
      lostFeatureKeys: doc.lostFeatureKeys.slice(0, 10),
    })),
    writeResult,
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
