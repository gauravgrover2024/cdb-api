#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const WRITE = process.argv.includes('--write');

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const now = new Date();

const entries = [
  {
    variantFullName: 'Maruti Eeco Tour V 5 Seater STD',
    gapType: 'dimensions_missing',
    resolutionStatus: 'manual_verified_complete',
    sourceName: 'maruti_official',
    sourceUrl: 'https://www.marutisuzuki.com/tour/eeco',
    sourceNote: 'Official Maruti Suzuki Tour V spec page. Ground clearance not published there, so not set.',
    set: {
      'practicalityBasis.lengthMm': 3675,
      'practicalityBasis.widthMm': 1475,
      'practicalityBasis.heightMm': 1825,
      'practicalityBasis.wheelbaseMm': 2350,
      'practicalityBasis.seatingCapacity': 5,
      'dataQuality.hasDimensionsData': true,
      'dataQuality.dimensionsCompletenessStatus': 'manual_verified_complete',
    },
    closeGapStatus: 'resolved',
  },
  {
    variantFullName: 'Mahindra Bolero Pik Up Bolero Pik-Up 1.3 T CBC MS',
    gapType: 'dimensions_missing',
    resolutionStatus: 'manual_verified_complete',
    sourceName: 'trucksbuses_carandbike',
    sourceUrl: 'https://www.trucksbuses.com/scv/pickups-and-mini-trucks/mahindra-bolero-pik-up-extra-strong-1-3t/specifications',
    sourceNote: 'Exact/nearest 1.3T commercial pickup spec source. carandbike exact CBC 1.3T BS6 MS confirms 200 mm ground clearance and 2-seater.',
    set: {
      'practicalityBasis.lengthMm': 5219,
      'practicalityBasis.widthMm': 1700,
      'practicalityBasis.heightMm': 1865,
      'practicalityBasis.wheelbaseMm': 3264,
      'practicalityBasis.groundClearanceMm': 200,
      'practicalityBasis.seatingCapacity': 2,
      'dataQuality.hasDimensionsData': true,
      'dataQuality.dimensionsCompletenessStatus': 'manual_verified_complete',
    },
    closeGapStatus: 'resolved',
  },
  {
    variantFullName: 'Blinq Ryde Family',
    gapType: 'dimensions_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'cardekho_zigwheels',
    sourceUrl: 'https://www.cardekho.com/blinq/ryde/specs',
    sourceNote: 'Trusted pages publish seating capacity, doors and range, but not length/width/height/wheelbase.',
    set: {
      'practicalityBasis.seatingCapacity': 5,
      'practicalityBasis.doors': 5,
      'dataQuality.hasDimensionsData': false,
      'dataQuality.dimensionsCompletenessStatus': 'known_source_limitation_length_width_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
  {
    variantFullName: 'Blinq Ryde Fleets',
    gapType: 'dimensions_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'cardekho_zigwheels',
    sourceUrl: 'https://www.cardekho.com/blinq/ryde/specs',
    sourceNote: 'Trusted pages publish seating capacity, doors and range, but not length/width/height/wheelbase.',
    set: {
      'practicalityBasis.seatingCapacity': 5,
      'practicalityBasis.doors': 5,
      'dataQuality.hasDimensionsData': false,
      'dataQuality.dimensionsCompletenessStatus': 'known_source_limitation_length_width_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
  {
    variantFullName: 'Blinq Ryde Family',
    gapType: 'performance_specs_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'cardekho_zigwheels',
    sourceUrl: 'https://www.cardekho.com/blinq/ryde/specs',
    sourceNote: 'Trusted pages publish electric range and transmission, but not motor power or torque.',
    set: {
      'mileageBasis.evClaimedRangeKm': 250,
      'dataQuality.hasPerformanceData': false,
      'dataQuality.performanceCompletenessStatus': 'known_source_limitation_power_torque_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
  {
    variantFullName: 'Blinq Ryde Fleets',
    gapType: 'performance_specs_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'cardekho_zigwheels',
    sourceUrl: 'https://www.cardekho.com/blinq/ryde/specs',
    sourceNote: 'Trusted pages publish electric range and transmission, but not motor power or torque.',
    set: {
      'mileageBasis.evClaimedRangeKm': 250,
      'dataQuality.hasPerformanceData': false,
      'dataQuality.performanceCompletenessStatus': 'known_source_limitation_power_torque_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
  {
    variantFullName: 'Tesla Model Y Premium RWD',
    gapType: 'performance_specs_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'tesla_official_carwale',
    sourceUrl: 'https://www.tesla.com/en_in/modely',
    sourceNote: 'Tesla official India page publishes range, acceleration and drive; CarWale publishes 235 bhp for Premium RWD, but torque is not published.',
    set: {
      'performanceBasis.powerBhp': 235,
      'dataQuality.hasPerformanceData': false,
      'dataQuality.performanceCompletenessStatus': 'known_source_limitation_torque_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
  {
    variantFullName: 'Tesla Model Y L Premium AWD',
    gapType: 'performance_specs_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'tesla_official_carwale',
    sourceUrl: 'https://www.tesla.com/en_in/modely',
    sourceNote: 'Tesla official India page publishes range, acceleration and AWD drive, but not motor power or torque.',
    set: {
      'dataQuality.hasPerformanceData': false,
      'dataQuality.performanceCompletenessStatus': 'known_source_limitation_power_torque_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
  {
    variantFullName: 'Tesla Model Y Long Range Premium RWD',
    gapType: 'performance_specs_missing',
    resolutionStatus: 'known_source_limitation',
    sourceName: 'tesla_official_carwale',
    sourceUrl: 'https://www.carwale.com/tesla-cars/model-y/',
    sourceNote: 'Trusted sources publish range/top speed/acceleration for Model Y trims, but not reliable variant-level torque.',
    set: {
      'dataQuality.hasPerformanceData': false,
      'dataQuality.performanceCompletenessStatus': 'known_source_limitation_power_torque_not_published',
    },
    closeGapStatus: 'known_source_limitation',
  },
];

const setNested = (target, path, value) => {
  target[path] = value;
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const profiles = db.collection(PROFILE_COLLECTION);
  const gaps = db.collection(GAP_COLLECTION);

  const preview = [];
  const profileOps = [];
  const gapOps = [];
  const errors = [];

  for (const entry of entries) {
    const gap = await gaps.findOne(
      {
        status: 'open',
        gapType: entry.gapType,
        variantFullName: entry.variantFullName,
      },
      {
        projection: {
          _id: 1,
          variantProfileKey: 1,
          variantFullName: 1,
          gapType: 1,
          priority: 1,
        },
      },
    );

    if (!gap) {
      errors.push({
        variantFullName: entry.variantFullName,
        gapType: entry.gapType,
        error: 'open_gap_not_found',
      });
      continue;
    }

    const profile = await profiles.findOne(
      { variantProfileKey: gap.variantProfileKey },
      { projection: { _id: 0, variantProfileKey: 1, variantFullName: 1 } },
    );

    if (!profile) {
      errors.push({
        variantFullName: entry.variantFullName,
        gapType: entry.gapType,
        variantProfileKey: gap.variantProfileKey,
        error: 'profile_not_found',
      });
      continue;
    }

    const setDoc = {
      updatedAt: now,
      'dataQuality.remainingSmallSpecCleanupApplied': true,
      'dataQuality.remainingSmallSpecCleanupUpdatedAt': now,
    };

    for (const [path, value] of Object.entries(entry.set || {})) {
      setNested(setDoc, path, value);
    }

    const evidenceRoot =
      entry.gapType === 'dimensions_missing'
        ? 'manualEvidence.remainingSmallSpecCleanup.dimensions'
        : 'manualEvidence.remainingSmallSpecCleanup.performance';

    setDoc[evidenceRoot] = {
      resolutionStatus: entry.resolutionStatus,
      sourceName: entry.sourceName,
      sourceUrl: entry.sourceUrl,
      sourceNote: entry.sourceNote,
      appliedBy: 'completeRemainingSmallSpecGapsV1',
      appliedAt: now,
      fields: Object.keys(entry.set || {}),
    };

    profileOps.push({
      updateOne: {
        filter: { variantProfileKey: gap.variantProfileKey },
        update: { $set: setDoc },
      },
    });

    gapOps.push({
      updateOne: {
        filter: { _id: gap._id },
        update: {
          $set: {
            status: entry.closeGapStatus,
            resolutionStatus: entry.resolutionStatus,
            resolvedBy: 'completeRemainingSmallSpecGapsV1',
            resolvedAt: now,
            sourceName: entry.sourceName,
            sourceUrl: entry.sourceUrl,
            sourceNote: entry.sourceNote,
            updatedAt: now,
          },
        },
      },
    });

    preview.push({
      variantFullName: entry.variantFullName,
      variantProfileKey: gap.variantProfileKey,
      gapType: entry.gapType,
      closeGapStatus: entry.closeGapStatus,
      resolutionStatus: entry.resolutionStatus,
      set: entry.set,
      sourceName: entry.sourceName,
    });
  }

  let profileWriteResult = null;
  let gapWriteResult = null;

  if (WRITE) {
    if (profileOps.length) {
      const result = await profiles.bulkWrite(profileOps, { ordered: false });
      profileWriteResult = {
        matched: result.matchedCount || 0,
        modified: result.modifiedCount || 0,
        upserted: result.upsertedCount || 0,
      };
    }

    if (gapOps.length) {
      const result = await gaps.bulkWrite(gapOps, { ordered: false });
      gapWriteResult = {
        matched: result.matchedCount || 0,
        modified: result.modifiedCount || 0,
        upserted: result.upsertedCount || 0,
      };
    }
  }

  console.log(JSON.stringify({
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    entries: entries.length,
    previewRows: preview.length,
    errors,
    profileOps: profileOps.length,
    gapOps: gapOps.length,
    profileWriteResult,
    gapWriteResult,
    preview,
  }, null, 2));

  await mongoose.disconnect();

  if (errors.length) process.exit(2);
})().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
