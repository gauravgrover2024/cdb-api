#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');

const args = process.argv.slice(2);
const write = args.includes('--write');
const allowOverwrite = args.includes('--allow-overwrite');

const fileArgIndex = args.indexOf('--file');
const filePath =
  fileArgIndex >= 0
    ? args[fileArgIndex + 1]
    : 'src/scripts/aci-data/manual/variant_spec_manual_review_26.csv';

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const numberFields = new Set([
  'manualPowerBhp',
  'manualTorqueNm',
  'manualLengthMm',
  'manualWidthMm',
  'manualHeightMm',
  'manualWheelbaseMm',
  'manualGroundClearanceMm',
  'manualSeatingCapacity',
  'manualDoors',
]);

const dimensionManualFields = [
  ['manualLengthMm', 'practicalityBasis.lengthMm', 'lengthMm'],
  ['manualWidthMm', 'practicalityBasis.widthMm', 'widthMm'],
  ['manualHeightMm', 'practicalityBasis.heightMm', 'heightMm'],
  ['manualWheelbaseMm', 'practicalityBasis.wheelbaseMm', 'wheelbaseMm'],
  ['manualGroundClearanceMm', 'practicalityBasis.groundClearanceMm', 'groundClearanceMm'],
  ['manualSeatingCapacity', 'practicalityBasis.seatingCapacity', 'seatingCapacity'],
  ['manualDoors', 'practicalityBasis.doors', 'doors'],
];

const performanceManualFields = [
  ['manualPowerBhp', 'performanceBasis.powerBhp', 'powerBhp'],
  ['manualTorqueNm', 'performanceBasis.torqueNm', 'torqueNm'],
];

const parseCsv = (text) => {
  const lines = text.replace(/\r/g, '').split('\n').filter((line) => line.trim() !== '');
  const headerLineIndex = lines.findIndex((line) => line.startsWith('gapType,priority,'));

  if (headerLineIndex < 0) {
    throw new Error('Could not find CSV header line starting with gapType,priority');
  }

  const parseLine = (line) => {
    const out = [];
    let cur = '';
    let quoted = false;

    for (let i = 0; i < line.length; i += 1) {
      const ch = line[i];

      if (ch === '"' && quoted && line[i + 1] === '"') {
        cur += '"';
        i += 1;
        continue;
      }

      if (ch === '"') {
        quoted = !quoted;
        continue;
      }

      if (ch === ',' && !quoted) {
        out.push(cur);
        cur = '';
        continue;
      }

      cur += ch;
    }

    out.push(cur);
    return out;
  };

  const headers = parseLine(lines[headerLineIndex]).map((h) => h.trim());

  return lines.slice(headerLineIndex + 1).map((line, idx) => {
    const values = parseLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = values[i] ?? '';
    });
    row.__rowNumber = idx + 1;
    return row;
  });
};

const toNumberOrNull = (value) => {
  if (value === null || value === undefined || String(value).trim() === '') return null;
  const n = Number(String(value).replace(/,/g, '').trim());
  return Number.isFinite(n) ? n : null;
};

const getPath = (obj, dotted) =>
  dotted.split('.').reduce((acc, key) => (acc ? acc[key] : undefined), obj);

const setPath = (obj, dotted, value) => {
  const parts = dotted.split('.');
  let cur = obj;

  for (let i = 0; i < parts.length - 1; i += 1) {
    if (!cur[parts[i]]) cur[parts[i]] = {};
    cur = cur[parts[i]];
  }

  cur[parts[parts.length - 1]] = value;
};

const hasManualValue = (row) =>
  [...numberFields].some((field) => String(row[field] || '').trim() !== '');

const hasRequiredSource = (row) =>
  String(row.sourceName || '').trim() !== '' && String(row.sourceUrl || '').trim() !== '';

const relevantFieldsForGap = (gapType) => {
  if (gapType === 'dimensions_missing') return dimensionManualFields;
  if (gapType === 'performance_specs_missing') return performanceManualFields;
  return [];
};

const clone = (value) => JSON.parse(JSON.stringify(value || {}));

const recomputeCompleteness = (profile) => {
  const practical = profile.practicalityBasis || {};
  const perf = profile.performanceBasis || {};

  return {
    hasDimensionsData: Boolean(
      practical.seatingCapacity &&
      practical.lengthMm &&
      practical.widthMm
    ),
    hasPerformanceData: Boolean(
      perf.powerBhp &&
      perf.torqueNm
    ),
  };
};

(async () => {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Manual evidence CSV not found: ${filePath}`);
  }

  const rows = parseCsv(fs.readFileSync(filePath, 'utf8'));
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const profiles = db.collection(PROFILE_COLLECTION);
  const gaps = db.collection(GAP_COLLECTION);

  const now = new Date();
  const updates = [];
  const preview = [];
  const errors = [];

  for (const row of rows) {
    if (!hasManualValue(row)) continue;

    if (!hasRequiredSource(row)) {
      errors.push({
        row: row.__rowNumber,
        variantFullName: row.variantFullName,
        error: 'manual_values_need_sourceName_and_sourceUrl',
      });
      continue;
    }

    const profile = await profiles.findOne(
      { variantProfileKey: row.variantProfileKey },
      {
        projection: {
          _id: 0,
          variantProfileKey: 1,
          variantFullName: 1,
          practicalityBasis: 1,
          performanceBasis: 1,
          dataQuality: 1,
          manualEvidence: 1,
        },
      },
    );

    if (!profile) {
      errors.push({
        row: row.__rowNumber,
        variantProfileKey: row.variantProfileKey,
        error: 'profile_not_found',
      });
      continue;
    }

    const setDoc = {};
    const patches = [];
    const overwritten = [];
    const beforeProfile = clone(profile);
    const afterProfile = clone(profile);

    for (const [manualField, targetPath, shortField] of relevantFieldsForGap(row.gapType)) {
      const manualValue = toNumberOrNull(row[manualField]);
      if (manualValue === null) continue;

      const currentValue = getPath(profile, targetPath);
      const currentNumber = Number(currentValue);
      const hasCurrent = Number.isFinite(currentNumber);

      if (hasCurrent && Math.abs(currentNumber - manualValue) > 0.01 && !allowOverwrite) {
        errors.push({
          row: row.__rowNumber,
          variantFullName: row.variantFullName,
          field: shortField,
          currentValue: currentNumber,
          manualValue,
          error: 'existing_value_conflict_use_allow_overwrite',
        });
        continue;
      }

      if (hasCurrent && Math.abs(currentNumber - manualValue) > 0.01) {
        overwritten.push({
          field: shortField,
          oldValue: currentNumber,
          newValue: manualValue,
        });
      }

      setDoc[targetPath] = manualValue;
      setPath(afterProfile, targetPath, manualValue);

      setDoc[`manualEvidence.specEvidence.${shortField}`] = {
        value: manualValue,
        previousValue: hasCurrent ? currentNumber : null,
        overwritten: hasCurrent && Math.abs(currentNumber - manualValue) > 0.01,
        sourceName: String(row.sourceName || '').trim(),
        sourceUrl: String(row.sourceUrl || '').trim(),
        sourceNote: String(row.sourceNote || '').trim(),
        evidenceStatus: String(row.evidenceStatus || '').trim() || 'manual_verified',
        appliedBy: 'manual_variant_spec_evidence_v1',
        appliedAt: now,
      };

      patches.push({
        field: shortField,
        targetPath,
        value: manualValue,
        previousValue: hasCurrent ? currentNumber : null,
      });
    }

    const completeness = recomputeCompleteness(afterProfile);

    if (row.gapType === 'dimensions_missing') {
      setDoc['dataQuality.hasDimensionsData'] = completeness.hasDimensionsData;
      setDoc['dataQuality.dimensionsCompletenessStatus'] = completeness.hasDimensionsData
        ? 'manual_verified_complete'
        : 'manual_verified_partial';
      setDoc['dataQuality.dimensionsManualEvidenceUpdatedAt'] = now;
    }

    if (row.gapType === 'performance_specs_missing') {
      setDoc['dataQuality.hasPerformanceData'] = completeness.hasPerformanceData;
      setDoc['dataQuality.performanceCompletenessStatus'] = completeness.hasPerformanceData
        ? 'manual_verified_complete'
        : 'manual_verified_partial';
      setDoc['dataQuality.performanceManualEvidenceUpdatedAt'] = now;

      const power = setDoc['performanceBasis.powerBhp'] ?? afterProfile.performanceBasis?.powerBhp;
      const torque = setDoc['performanceBasis.torqueNm'] ?? afterProfile.performanceBasis?.torqueNm;
      const kerb = afterProfile.performanceBasis?.kerbWeightKg;

      if (power && kerb) {
        setDoc['performanceBasis.powerToWeight'] = Number((Number(power) / (Number(kerb) / 1000)).toFixed(2));
      }

      if (torque && kerb) {
        setDoc['performanceBasis.torqueToWeight'] = Number((Number(torque) / (Number(kerb) / 1000)).toFixed(2));
      }
    }

    setDoc['dataQuality.manualSpecEvidenceApplied'] = true;
    setDoc['dataQuality.manualSpecEvidenceUpdatedAt'] = now;
    setDoc.updatedAt = now;

    preview.push({
      row: row.__rowNumber,
      variantProfileKey: row.variantProfileKey,
      variantFullName: row.variantFullName,
      gapType: row.gapType,
      patches,
      overwritten,
      resultingDataQuality: {
        hasDimensionsData: setDoc['dataQuality.hasDimensionsData'],
        hasPerformanceData: setDoc['dataQuality.hasPerformanceData'],
        dimensionsCompletenessStatus: setDoc['dataQuality.dimensionsCompletenessStatus'],
        performanceCompletenessStatus: setDoc['dataQuality.performanceCompletenessStatus'],
      },
    });

    if (patches.length) {
      updates.push({
        updateOne: {
          filter: { variantProfileKey: row.variantProfileKey },
          update: { $set: setDoc },
        },
      });
    }
  }

  if (errors.length) {
    console.log(JSON.stringify({
      mode: write ? 'WRITE_BLOCKED' : 'DRY_RUN_BLOCKED',
      errors,
      preview,
    }, null, 2));
    await mongoose.disconnect();
    process.exit(2);
  }

  let writeResult = null;

  if (write && updates.length) {
    const result = await profiles.bulkWrite(updates, { ordered: false });
    writeResult = {
      matched: result.matchedCount || 0,
      modified: result.modifiedCount || 0,
      upserted: result.upsertedCount || 0,
    };

    // Do not delete/reset gaps here. Gap queue is rebuilt by its own builder after this script.
    await gaps.updateMany(
      {
        variantProfileKey: { $in: preview.map((p) => p.variantProfileKey) },
        gapType: { $in: ['dimensions_missing', 'performance_specs_missing'] },
      },
      {
        $set: {
          manualEvidenceAppliedAt: now,
          updatedAt: now,
        },
      },
    );
  }

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    allowOverwrite,
    file: path.resolve(filePath),
    rowsRead: rows.length,
    rowsWithManualValues: preview.length,
    updateOps: updates.length,
    patchFields: preview.reduce((sum, r) => sum + r.patches.length, 0),
    overwrittenFields: preview.reduce((sum, r) => sum + r.overwritten.length, 0),
    writeResult,
    preview,
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
