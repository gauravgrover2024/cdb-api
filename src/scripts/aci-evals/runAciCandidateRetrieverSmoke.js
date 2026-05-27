import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

import {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
} from "../../services/aciCore/candidates/aciDbCandidateRetriever.js";

const cases = [
  {
    id: "punch-cng-sunroof-abs-price",
    message: "Punch CNG sunroof ABS price",
    expectations: {
      minModels: 1,
      minFeatures: 2,
      minFuelTypes: 1,
      minTasks: 1,
      forbiddenVariantNameParts: ['bajaj', 'alto tour', 'prime hb', 'prime sd', 'xpres'],
      expectedFeatureKeys: ['sunroof', 'anti_lock_braking_system'],
      forbiddenFeatureKeys: ['voice_assisted_sunroof'],
    },
  },
  {
    id: "broad-hyundai-sunroof-budget",
    message: "Hyundai cars with sunroof under 20 lakh",
    expectations: {
      minMakes: 1,
      minFeatures: 1,
      minBudgets: 1,
      minTasks: 1,
      expectedMakeParts: ['hyundai'],
      expectedFeatureKeys: ['sunroof'],
      forbiddenFeatureKeys: ['voice_assisted_sunroof'],
    },
  },
  {
    id: "variant-comparison",
    message: "Verna HX8 iVT vs City ZX CVT",
    expectations: {
      minModels: 2,
      minVariants: 2,
      minTasks: 1,
      expectedVariantNameParts: ['verna hx8 ivt', 'city zx cvt'],
      expectedTaskKeys: ['vehicle_comparison'],
    },
  },
  {
    id: "extreme-multi-intent",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
    expectations: {
      minModels: 2,
      minFeatures: 3,
      minFuelTypes: 1,
      maxFeatures: 4,
      expectedFeatureKeys: ['sunroof', 'anti_lock_braking_system', 'adas_package'],
      forbiddenFeatureKeys: [
        'voice_assisted_sunroof',
        'lane_keep_assist',
        'forward_collision_warning',
        'blind_spot_monitor',
        'autonomous_parking',
        'traffic_sign_recognition',
        'rear_cross_traffic_collision_avoidance_assist',
        'leading_vehicle_departure_alert',
        'lane_departure_prevention_assist',
        'speed_assist_system',
      ],
      expectedTaskKeys: ['vehicle_comparison'],
      forbiddenVariantNameParts: ['bajaj', 'alto tour', 'prime hb', 'prime sd', 'xpres'],
      expectedFeatureKeys: ['sunroof', 'anti_lock_braking_system'],
      forbiddenFeatureKeys: ['voice_assisted_sunroof'],
    },
  },
];

const getCount = (snapshot, key) => {
  switch (key) {
    case "minMakes": return snapshot.vehicles.makes.length;
    case "minModels": return snapshot.vehicles.models.length;
    case "minVariants": return snapshot.vehicles.variants.length;
    case "minFeatures": return snapshot.taxonomy.features.length;
    case "minFuelTypes": return snapshot.taxonomy.fuelTypes.length;
    case "minTransmissions": return snapshot.taxonomy.transmissions.length;
    case "minBudgets": return snapshot.commerce.budgets.length;
    case "minTasks": return snapshot.language.tasks.length;
    default: return 0;
  }
};

const main = async () => {
  await connectDB();
  clearAciCandidateRetrieverCaches();

  const results = [];
  const failures = [];

  for (const item of cases) {
    const startedAt = Date.now();

    const snapshot = await retrieveAciDbCandidates({
      rawMessage: item.message,
    });

    const caseFailures = [];

    for (const [key, expectedMin] of Object.entries(item.expectations)) {
      if (!key.startsWith('min')) continue;
      const actual = getCount(snapshot, key);
      if (actual < expectedMin) {
        caseFailures.push(`${key} expected >= ${expectedMin}, got ${actual}`);
      }
    }

    const makeText = snapshot.vehicles.makes.map((candidate) => candidate.displayName || '').join(' ').toLowerCase();
    for (const expectedPart of item.expectations.expectedMakeParts || []) {
      if (!makeText.includes(String(expectedPart).toLowerCase())) {
        caseFailures.push(`expected make containing "${expectedPart}", got "${makeText}"`);
      }
    }

    const variantText = snapshot.vehicles.variants.map((candidate) => candidate.displayName || '').join(' ').toLowerCase();
    for (const forbiddenPart of item.expectations.forbiddenVariantNameParts || []) {
      if (variantText.includes(String(forbiddenPart).toLowerCase())) {
        caseFailures.push(`forbidden variant candidate part present: "${forbiddenPart}"`);
      }
    }

    for (const expectedPart of item.expectations.expectedVariantNameParts || []) {
      if (!variantText.includes(String(expectedPart).toLowerCase())) {
        caseFailures.push(`expected variant containing "${expectedPart}", got "${variantText}"`);
      }
    }

    const featureKeys = snapshot.taxonomy.features.map((candidate) => candidate.canonicalKey);
    for (const expectedFeature of item.expectations.expectedFeatureKeys || []) {
      if (!featureKeys.includes(expectedFeature)) {
        caseFailures.push(`expected feature "${expectedFeature}", got ${JSON.stringify(featureKeys)}`);
      }
    }

    for (const forbiddenFeature of item.expectations.forbiddenFeatureKeys || []) {
      if (featureKeys.includes(forbiddenFeature)) {
        caseFailures.push(`forbidden feature candidate present: "${forbiddenFeature}"`);
      }
    }

    if (typeof item.expectations.maxFeatures === 'number' && featureKeys.length > item.expectations.maxFeatures) {
      caseFailures.push(`features expected <= ${item.expectations.maxFeatures}, got ${featureKeys.length}: ${JSON.stringify(featureKeys)}`);
    }

    const taskKeys = snapshot.language.tasks.map((candidate) => candidate.canonicalKey);
    for (const expectedTask of item.expectations.expectedTaskKeys || []) {
      if (!taskKeys.includes(expectedTask)) {
        caseFailures.push(`expected task "${expectedTask}", got ${JSON.stringify(taskKeys)}`);
      }
    }

    const summary = {
      id: item.id,
      message: item.message,
      pass: caseFailures.length === 0,
      durationMs: Date.now() - startedAt,
      failures: caseFailures,
      counts: snapshot.trace.counts,
      makes: snapshot.vehicles.makes.map((candidate) => candidate.displayName).slice(0, 5),
      models: snapshot.vehicles.models.map((candidate) => candidate.displayName).slice(0, 5),
      variants: snapshot.vehicles.variants.map((candidate) => candidate.displayName).slice(0, 5),
      features: snapshot.taxonomy.features.map((candidate) => candidate.canonicalKey).slice(0, 10),
      fuelTypes: snapshot.taxonomy.fuelTypes.map((candidate) => candidate.canonicalKey),
      budgets: snapshot.commerce.budgets.map((candidate) => candidate.metadata),
      tasks: snapshot.language.tasks.map((candidate) => candidate.canonicalKey),
    };

    results.push(summary);

    if (caseFailures.length) {
      failures.push({
        id: item.id,
        failures: caseFailures,
        summary,
      });
    }
  }

  console.log(JSON.stringify({
    suite: "ACI DB candidate retriever smoke",
    ok: failures.length === 0,
    total: cases.length,
    passed: cases.length - failures.length,
    failed: failures.length,
    failedIds: failures.map((item) => item.id),
    failures,
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failures.length) {
    process.exit(1);
  }
};

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI DB candidate retriever smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
