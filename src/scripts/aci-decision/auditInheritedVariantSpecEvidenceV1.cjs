#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const mongoose = require('mongoose');

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const FEATURE_COLLECTION =
  process.env.ACI_SOURCE_FEATURE_COLLECTION || 'vehicle_features';

const OUT = '/tmp/aci_inherited_spec_evidence_dryrun.json';

const norm = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const keyNorm = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const num = (value) => {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const text = String(value).replace(/,/g, '');
  const hit = text.match(/-?\d+(?:\.\d+)?/);
  return hit ? Number(hit[0]) : null;
};

const hasNum = (value) => value !== null && value !== undefined && Number.isFinite(Number(value));

const nearlyEqual = (a, b, tolerance = 0.01) => {
  if (!hasNum(a) || !hasNum(b)) return false;
  return Math.abs(Number(a) - Number(b)) <= tolerance;
};

const isPresent = (value) => value !== null && value !== undefined && value !== '';

const getFeature = (doc, labelCandidates) => {
  const features = doc?.features || {};
  const entries = Object.entries(features);

  for (const label of labelCandidates) {
    const wanted = norm(label);

    const exact = entries.find(([key]) => norm(key).endsWith(wanted));
    if (exact) return exact[1];

    const contains = entries.find(([key]) => norm(key).includes(wanted));
    if (contains) return contains[1];
  }

  return null;
};

const extractDims = (doc) => ({
  lengthMm: num(getFeature(doc, ['Dimensions & Capacity | Length', 'Length'])),
  widthMm: num(getFeature(doc, ['Dimensions & Capacity | Width', 'Width'])),
  heightMm: num(getFeature(doc, ['Dimensions & Capacity | Height', 'Height'])),
  wheelbaseMm: num(getFeature(doc, ['Dimensions & Capacity | Wheel Base', 'Wheel Base', 'Wheelbase'])),
  groundClearanceMm: num(getFeature(doc, [
    'Dimensions & Capacity | Reported Ground Clearance (Unladen)',
    'Reported Ground Clearance',
    'Ground Clearance'
  ])),
  seatingCapacity: num(getFeature(doc, ['Dimensions & Capacity | Seating Capacity', 'Seating Capacity'])),
  doors: num(getFeature(doc, ['Dimensions & Capacity | No. of Doors', 'No. of Doors'])),
  bootSpaceLitres: num(getFeature(doc, ['Dimensions & Capacity | Boot Space', 'Boot Space'])),
});

const extractPerformance = (doc) => ({
  powerBhp: num(getFeature(doc, ['Engine & Transmission | Max Power', 'Max Power', 'Motor Power', 'Power'])),
  torqueNm: num(getFeature(doc, ['Engine & Transmission | Max Torque', 'Max Torque', 'Motor Torque', 'Torque'])),
  kerbWeightKg: num(getFeature(doc, ['Kerb Weight', 'Kerb Weight Kg'])),
  zeroToHundredClaimedSec: num(getFeature(doc, ['Suspension, Steering & Brakes | 0-100kmph', '0-100kmph', 'Acceleration'])),
  topSpeedKmph: num(getFeature(doc, ['Fuel & Performance | Top Speed', 'Top Speed'])),
  engineType: getFeature(doc, ['Engine & Transmission | Engine Type', 'Engine Type']),
  fuelType: getFeature(doc, ['Fuel & Performance | Fuel Type', 'Fuel Type']),
  transmissionType: getFeature(doc, ['Engine & Transmission | Transmission Type', 'Transmission Type']),
  drivetrain: getFeature(doc, ['Engine & Transmission | Drive Type', 'Drive Type']),
  batteryCapacityRaw: getFeature(doc, ['Battery Capacity', 'Battery Type']),
  rangeKm: num(getFeature(doc, ['Range', 'Claimed Range'])),
});

const latestSortValue = (doc) =>
  new Date(doc?.scraperSeenAt || doc?.featureContentChangedAt || doc?.scrape_timestamp || doc?.last_updated || doc?.updatedAt || 0).getTime();

const pickLatest = (docs) =>
  [...docs].sort((a, b) => latestSortValue(b) - latestSortValue(a))[0] || null;

const uniqueNums = (values) =>
  [...new Set(values.filter(hasNum).map(Number))];

const profileDimensionFields = (profile = {}) => {
  const p = profile.practicalityBasis || {};
  return {
    seatingCapacity: p.seatingCapacity ?? null,
    lengthMm: p.lengthMm ?? null,
    widthMm: p.widthMm ?? null,
    heightMm: p.heightMm ?? null,
    wheelbaseMm: p.wheelbaseMm ?? null,
    groundClearanceMm: p.groundClearanceMm ?? null,
    doors: p.doors ?? null,
    bootSpaceLitres: p.bootSpaceLitres ?? null,
  };
};

const profilePerformanceFields = (profile = {}) => {
  const p = profile.performanceBasis || {};
  return {
    powerBhp: p.powerBhp ?? null,
    torqueNm: p.torqueNm ?? null,
    kerbWeightKg: p.kerbWeightKg ?? null,
    zeroToHundredClaimedSec: p.zeroToHundredClaimedSec ?? null,
    topSpeedKmph: p.topSpeedKmph ?? null,
    engineType: p.engineType ?? null,
    fuelType: profile.fuel ?? profile.fuelKey ?? null,
    transmissionType: profile.transmission ?? profile.transmissionKey ?? null,
    drivetrain: p.drivetrain ?? profile.drivetrain ?? null,
    batteryCapacityRaw: profile.batteryCapacityKwh ?? null,
    rangeKm: profile.claimedRangeKm ?? null,
  };
};

const dimensionGapRequiredFields = ['seatingCapacity', 'lengthMm', 'widthMm'];
const performanceGapRequiredFields = ['powerBhp', 'torqueNm'];

const getMissingFields = (base, fields) => fields.filter((field) => !hasNum(base[field]));

const valueForField = (obj, field) => {
  const value = obj?.[field];
  return hasNum(value) ? Number(value) : null;
};

const isSameVariantDoc = ({ doc, gap, profile }) => {
  const docVariant = keyNorm(doc?.variant);
  const variantNames = [
    gap.variantFullName,
    profile?.variantFullName,
    profile?.variant,
  ].map(keyNorm).filter(Boolean);

  const variantKeys = [
    gap.variantKey,
    profile?.variantKey,
  ].map(keyNorm).filter(Boolean);

  return (
    variantNames.some((name) => docVariant === name || docVariant.includes(name) || name.includes(docVariant)) ||
    variantKeys.some((key) => docVariant.includes(key))
  );
};

const dimensionDonorCompatibility = ({ targetDims, donorDims }) => {
  if (targetDims.lengthMm && donorDims.lengthMm && !nearlyEqual(targetDims.lengthMm, donorDims.lengthMm)) {
    return { compatible: false, reason: `length_mismatch:${targetDims.lengthMm}_vs_${donorDims.lengthMm}` };
  }

  if (targetDims.wheelbaseMm && donorDims.wheelbaseMm && !nearlyEqual(targetDims.wheelbaseMm, donorDims.wheelbaseMm)) {
    return { compatible: false, reason: `wheelbase_mismatch:${targetDims.wheelbaseMm}_vs_${donorDims.wheelbaseMm}` };
  }

  if (targetDims.seatingCapacity && donorDims.seatingCapacity && !nearlyEqual(targetDims.seatingCapacity, donorDims.seatingCapacity)) {
    return { compatible: false, reason: `seating_mismatch:${targetDims.seatingCapacity}_vs_${donorDims.seatingCapacity}` };
  }

  if (
    targetDims.groundClearanceMm &&
    donorDims.groundClearanceMm &&
    !nearlyEqual(targetDims.groundClearanceMm, donorDims.groundClearanceMm)
  ) {
    return { compatible: false, reason: `ground_clearance_mismatch:${targetDims.groundClearanceMm}_vs_${donorDims.groundClearanceMm}` };
  }

  const strongAnchors = [];

  if (targetDims.lengthMm && donorDims.lengthMm && nearlyEqual(targetDims.lengthMm, donorDims.lengthMm)) {
    strongAnchors.push('same_length');
  }

  if (targetDims.wheelbaseMm && donorDims.wheelbaseMm && nearlyEqual(targetDims.wheelbaseMm, donorDims.wheelbaseMm)) {
    strongAnchors.push('same_wheelbase');
  }

  if (
    targetDims.groundClearanceMm &&
    donorDims.groundClearanceMm &&
    nearlyEqual(targetDims.groundClearanceMm, donorDims.groundClearanceMm) &&
    targetDims.seatingCapacity &&
    donorDims.seatingCapacity &&
    nearlyEqual(targetDims.seatingCapacity, donorDims.seatingCapacity)
  ) {
    strongAnchors.push('same_ground_clearance_and_seating');
  }

  if (!strongAnchors.length) {
    return { compatible: false, reason: 'insufficient_dimension_anchor_not_using_seating_alone' };
  }

  return {
    compatible: true,
    reason: strongAnchors.join('+'),
  };
};

const compatiblePerformanceDonor = ({ targetProfile, targetPerf, donorPerf }) => {
  const targetFuel = keyNorm(targetProfile.fuelKey || targetProfile.fuel);
  const targetTransmission = keyNorm(targetProfile.transmissionKey || targetProfile.transmission);
  const donorFuel = keyNorm(donorPerf.fuelType || '');
  const donorTransmission = keyNorm(donorPerf.transmissionType || '');

  if (donorFuel && targetFuel && !donorFuel.includes(targetFuel)) {
    return { compatible: false, reason: `fuel_mismatch:${targetFuel}_vs_${donorFuel}` };
  }

  if (donorTransmission && targetTransmission && !donorTransmission.includes(targetTransmission)) {
    return { compatible: false, reason: `transmission_mismatch:${targetTransmission}_vs_${donorTransmission}` };
  }

  if (targetPerf.powerBhp && donorPerf.powerBhp && !nearlyEqual(targetPerf.powerBhp, donorPerf.powerBhp)) {
    return { compatible: false, reason: `power_anchor_mismatch:${targetPerf.powerBhp}_vs_${donorPerf.powerBhp}` };
  }

  if (targetPerf.torqueNm && donorPerf.torqueNm && !nearlyEqual(targetPerf.torqueNm, donorPerf.torqueNm)) {
    return { compatible: false, reason: `torque_anchor_mismatch:${targetPerf.torqueNm}_vs_${donorPerf.torqueNm}` };
  }

  const isEv =
    targetFuel === 'electric' ||
    donorFuel === 'electric' ||
    /electric|ev/i.test(String(targetPerf.engineType || donorPerf.engineType || ''));

  const reasons = [];

  if (targetPerf.powerBhp && donorPerf.powerBhp && nearlyEqual(targetPerf.powerBhp, donorPerf.powerBhp)) {
    reasons.push('same_power_anchor');
  }

  if (targetPerf.torqueNm && donorPerf.torqueNm && nearlyEqual(targetPerf.torqueNm, donorPerf.torqueNm)) {
    reasons.push('same_torque_anchor');
  }

  if (isEv) {
    const targetBattery = norm(targetPerf.batteryCapacityRaw);
    const donorBattery = norm(donorPerf.batteryCapacityRaw);

    if (targetBattery && donorBattery && targetBattery === donorBattery) {
      reasons.push('same_battery_family');
    }

    if (targetPerf.rangeKm && donorPerf.rangeKm && nearlyEqual(targetPerf.rangeKm, donorPerf.rangeKm)) {
      reasons.push('same_range_anchor');
    }

    const drivetrainTarget = norm(targetPerf.drivetrain);
    const drivetrainDonor = norm(donorPerf.drivetrain);

    if (drivetrainTarget && drivetrainDonor && drivetrainTarget === drivetrainDonor) {
      reasons.push('same_drivetrain');
    }

    if (!reasons.length) {
      return { compatible: false, reason: 'insufficient_ev_powertrain_anchor' };
    }

    return { compatible: true, reason: reasons.join('+') };
  }

  const targetEngine = norm(targetPerf.engineType);
  const donorEngine = norm(donorPerf.engineType);

  if (targetEngine && donorEngine && targetEngine === donorEngine) {
    reasons.push('same_engine_type');
  }

  if (targetFuel && donorFuel) reasons.push('same_fuel');
  if (targetTransmission && donorTransmission) reasons.push('same_transmission');

  if (targetEngine && donorEngine && targetEngine !== donorEngine && !targetPerf.powerBhp && !targetPerf.torqueNm) {
    return { compatible: false, reason: `engine_mismatch:${targetEngine}_vs_${donorEngine}` };
  }

  if (!reasons.includes('same_engine_type') && !targetPerf.powerBhp && !targetPerf.torqueNm) {
    // Non-EV fallback is allowed only when all donor values later collapse to one value.
    // Keep donor candidate but flag weaker reason.
    reasons.push('same_fuel_transmission_weak_anchor');
  }

  return {
    compatible: true,
    reason: reasons.join('+') || 'same_model_powertrain_candidate',
  };
};

const fieldPatchFromDirectSource = ({ field, directValues }) => {
  const value = valueForField(directValues, field);
  if (!hasNum(value)) return null;

  return {
    field,
    action: 'would_patch_direct_source',
    value,
    classification: 'direct_source_available',
  };
};

const fieldPatchFromDonors = ({ field, donorValues }) => {
  const unique = uniqueNums(donorValues);

  if (unique.length === 1) {
    return {
      field,
      action: 'would_patch_inherited',
      value: unique[0],
      classification: 'inherited_sibling_consistent',
    };
  }

  if (unique.length > 1) {
    return {
      field,
      action: 'blocked',
      values: unique,
      classification: 'blocked_mixed_sibling_values',
    };
  }

  return {
    field,
    action: 'no_patch',
    classification: 'source_missing_or_no_compatible_donor',
  };
};

const computeFieldPatches = ({ fields, currentValues, directValues, donorRows, donorValueGetter }) => {
  return fields.map((field) => {
    if (hasNum(currentValues[field])) {
      return {
        field,
        action: 'already_present',
        value: Number(currentValues[field]),
        classification: 'already_present',
      };
    }

    const directPatch = fieldPatchFromDirectSource({ field, directValues });
    if (directPatch) return directPatch;

    return fieldPatchFromDonors({
      field,
      donorValues: donorRows.map((row) => donorValueGetter(row, field)),
    });
  });
};

const applyPatchPreview = ({ base, patches }) => {
  const out = { ...base };

  for (const patch of patches) {
    if (
      ['would_patch_direct_source', 'would_patch_inherited', 'already_present'].includes(patch.action) &&
      hasNum(patch.value)
    ) {
      out[patch.field] = Number(patch.value);
    }
  }

  return out;
};

const requiredResolved = ({ values, requiredFields }) =>
  requiredFields.every((field) => hasNum(values[field]));

const classifyFinal = ({ gapType, beforeValues, afterValues, patches }) => {
  const requiredFields =
    gapType === 'dimensions_missing'
      ? dimensionGapRequiredFields
      : performanceGapRequiredFields;

  const wasResolved = requiredResolved({ values: beforeValues, requiredFields });
  const isResolved = requiredResolved({ values: afterValues, requiredFields });

  const patchCount = patches.filter((p) => ['would_patch_direct_source', 'would_patch_inherited'].includes(p.action)).length;
  const blockedCount = patches.filter((p) => p.action === 'blocked').length;

  if (wasResolved) return 'already_resolved_no_gap_expected';
  if (isResolved && patchCount > 0) return `would_resolve_${gapType}`;
  if (blockedCount > 0) return `blocked_${gapType}_mixed_candidate_values`;
  if (patchCount > 0) return `partial_patch_but_${gapType}_still_unresolved`;
  return `known_source_limitation_${gapType}_not_resolved`;
};

(async () => {
  const uri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!uri) throw new Error('Missing Mongo URI');

  await mongoose.connect(uri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const gaps = db.collection(GAP_COLLECTION);
  const profiles = db.collection(PROFILE_COLLECTION);
  const features = db.collection(FEATURE_COLLECTION);

  const targetGaps = await gaps.find(
    {
      status: 'open',
      gapType: { $in: ['dimensions_missing', 'performance_specs_missing'] },
    },
    {
      projection: {
        _id: 0,
        gapType: 1,
        priority: 1,
        variantProfileKey: 1,
        variantFullName: 1,
        makeKey: 1,
        modelKey: 1,
        variantKey: 1,
        fuelKey: 1,
        transmissionKey: 1,
        evidence: 1,
      },
    },
  ).sort({ gapType: 1, variantFullName: 1 }).toArray();

  const rows = [];

  for (const gap of targetGaps) {
    const profile = await profiles.findOne(
      { variantProfileKey: gap.variantProfileKey },
      {
        projection: {
          _id: 0,
          variantProfileKey: 1,
          variantFullName: 1,
          make: 1,
          makeKey: 1,
          model: 1,
          modelKey: 1,
          variant: 1,
          variantKey: 1,
          fuel: 1,
          fuelKey: 1,
          transmission: 1,
          transmissionKey: 1,
          practicalityBasis: 1,
          performanceBasis: 1,
          dataQuality: 1,
        },
      },
    );

    const strictModelRegex = new RegExp(`^${String(gap.modelKey || '').replace(/-/g, '[\\s-]*')}$`, 'i');

    let modelDocs = await features.find(
      { model: strictModelRegex },
      {
        projection: {
          _id: 1,
          brand: 1,
          make: 1,
          model: 1,
          variant: 1,
          features: 1,
          last_updated: 1,
          scrape_timestamp: 1,
          scraperSeenAt: 1,
          featureContentChangedAt: 1,
          updatedAt: 1,
        },
      },
    ).toArray();

    if (!modelDocs.length) {
      modelDocs = await features.find(
        { model: new RegExp(String(gap.modelKey || '').replace(/-/g, '.*'), 'i') },
        {
          projection: {
            _id: 1,
            brand: 1,
            make: 1,
            model: 1,
            variant: 1,
            features: 1,
            last_updated: 1,
            scrape_timestamp: 1,
            scraperSeenAt: 1,
            featureContentChangedAt: 1,
            updatedAt: 1,
          },
        },
      ).toArray();
    }

    const targetDocs = modelDocs.filter((doc) => isSameVariantDoc({ doc, gap, profile }));
    const exactDoc = pickLatest(targetDocs);

    const directDims = exactDoc ? extractDims(exactDoc) : {};
    const directPerf = exactDoc ? extractPerformance(exactDoc) : {};

    const currentDims = profileDimensionFields(profile);
    const currentPerf = profilePerformanceFields(profile);

    let result;

    if (gap.gapType === 'dimensions_missing') {
      const targetDimsForCompatibility = {
        ...currentDims,
        ...Object.fromEntries(Object.entries(directDims).filter(([, value]) => hasNum(value))),
        ...(gap.evidence || {}),
      };

      const donorRows = [];
      const excludedRows = [];

      for (const doc of modelDocs) {
        if (isSameVariantDoc({ doc, gap, profile })) continue;

        const dims = extractDims(doc);
        const compatibility = dimensionDonorCompatibility({
          targetDims: targetDimsForCompatibility,
          donorDims: dims,
        });

        const row = {
          sourceId: String(doc._id),
          variant: doc.variant,
          dims,
          compatibility,
        };

        if (compatibility.compatible) donorRows.push(row);
        else excludedRows.push(row);
      }

      const fieldsToReview = ['seatingCapacity', 'lengthMm', 'widthMm', 'heightMm', 'wheelbaseMm', 'groundClearanceMm', 'doors'];
      const patches = computeFieldPatches({
        fields: fieldsToReview,
        currentValues: currentDims,
        directValues: directDims,
        donorRows,
        donorValueGetter: (row, field) => row.dims[field],
      });

      const afterValues = applyPatchPreview({ base: currentDims, patches });

      result = {
        finalClassification: classifyFinal({
          gapType: gap.gapType,
          beforeValues: currentDims,
          afterValues,
          patches,
        }),
        requiredFields: dimensionGapRequiredFields,
        beforeValues: currentDims,
        directSourceValues: directDims,
        afterValues,
        wouldPatch: patches,
        compatibleDonors: donorRows.slice(0, 12),
        excludedDonors: excludedRows.slice(0, 12),
      };
    } else {
      const targetPerfForCompatibility = {
        ...currentPerf,
        ...Object.fromEntries(Object.entries(directPerf).filter(([, value]) => isPresent(value))),
        ...(gap.evidence || {}),
      };

      const donorRows = [];
      const excludedRows = [];

      for (const doc of modelDocs) {
        if (isSameVariantDoc({ doc, gap, profile })) continue;

        const perf = extractPerformance(doc);
        const compatibility = compatiblePerformanceDonor({
          targetProfile: profile || gap,
          targetPerf: targetPerfForCompatibility,
          donorPerf: perf,
        });

        const row = {
          sourceId: String(doc._id),
          variant: doc.variant,
          perf,
          compatibility,
        };

        if (compatibility.compatible) donorRows.push(row);
        else excludedRows.push(row);
      }

      const fieldsToReview = ['powerBhp', 'torqueNm'];
      const patches = computeFieldPatches({
        fields: fieldsToReview,
        currentValues: currentPerf,
        directValues: directPerf,
        donorRows,
        donorValueGetter: (row, field) => row.perf[field],
      });

      const afterValues = applyPatchPreview({ base: currentPerf, patches });

      result = {
        finalClassification: classifyFinal({
          gapType: gap.gapType,
          beforeValues: currentPerf,
          afterValues,
          patches,
        }),
        requiredFields: performanceGapRequiredFields,
        beforeValues: currentPerf,
        directSourceValues: directPerf,
        afterValues,
        wouldPatch: patches,
        compatibleDonors: donorRows.slice(0, 12),
        excludedDonors: excludedRows.slice(0, 12),
      };
    }

    rows.push({
      gap,
      profileSnapshot: {
        variantProfileKey: profile?.variantProfileKey,
        variantFullName: profile?.variantFullName,
        practicalityBasis: profile?.practicalityBasis,
        performanceBasis: profile?.performanceBasis,
        dataQuality: profile?.dataQuality,
      },
      exactDoc: exactDoc
        ? {
          sourceId: String(exactDoc._id),
          variant: exactDoc.variant,
          last_updated: exactDoc.last_updated,
          scraperSeenAt: exactDoc.scraperSeenAt,
          featureContentChangedAt: exactDoc.featureContentChangedAt,
          dims: extractDims(exactDoc),
          perf: extractPerformance(exactDoc),
        }
        : null,
      result,
    });
  }

  const summary = {
    totalRows: rows.length,
    byGapType: rows.reduce((acc, row) => {
      acc[row.gap.gapType] = (acc[row.gap.gapType] || 0) + 1;
      return acc;
    }, {}),
    byClassification: rows.reduce((acc, row) => {
      const key = `${row.gap.gapType}__${row.result.finalClassification}`;
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
    rowsThatWouldResolveGap: rows.filter((row) => row.result.finalClassification.startsWith('would_resolve_')).length,
    directSourcePatchFields: rows.reduce((sum, row) =>
      sum + row.result.wouldPatch.filter((x) => x.action === 'would_patch_direct_source').length, 0),
    inheritedPatchFields: rows.reduce((sum, row) =>
      sum + row.result.wouldPatch.filter((x) => x.action === 'would_patch_inherited').length, 0),
    blockedFields: rows.reduce((sum, row) =>
      sum + row.result.wouldPatch.filter((x) => x.action === 'blocked').length, 0),
    noPatchFields: rows.reduce((sum, row) =>
      sum + row.result.wouldPatch.filter((x) => x.action === 'no_patch').length, 0),
  };

  const payload = {
    checkedAt: new Date().toISOString(),
    summary,
    rows,
  };

  fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));

  console.log(JSON.stringify({
    summary,
    sampleRows: rows.map((row) => ({
      variantFullName: row.gap.variantFullName,
      gapType: row.gap.gapType,
      classification: row.result.finalClassification,
      exactDoc: row.exactDoc
        ? {
          variant: row.exactDoc.variant,
          dims: row.exactDoc.dims,
          perf: row.exactDoc.perf,
        }
        : null,
      requiredFields: row.result.requiredFields,
      beforeValues: row.result.beforeValues,
      directSourceValues: row.result.directSourceValues,
      afterValues: row.result.afterValues,
      wouldPatch: row.result.wouldPatch,
      compatibleDonors: row.result.compatibleDonors.map((d) => ({
        variant: d.variant,
        dims: d.dims,
        perf: d.perf,
        reason: d.compatibility.reason,
      })).slice(0, 5),
      excludedDonors: row.result.excludedDonors.map((d) => ({
        variant: d.variant,
        dims: d.dims,
        perf: d.perf,
        reason: d.compatibility.reason,
      })).slice(0, 5),
    })).slice(0, 26),
    fullReportPath: OUT,
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
