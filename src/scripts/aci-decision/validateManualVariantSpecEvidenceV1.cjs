#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');

const args = process.argv.slice(2);
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

  // Numbers export has a title line before actual headers.
  const headerLineIndex = lines.findIndex((line) => line.startsWith('gapType,priority,'));
  if (headerLineIndex < 0) throw new Error('Could not find CSV header line starting with gapType,priority');

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

const hasManualValue = (row) =>
  [...numberFields].some((field) => String(row[field] || '').trim() !== '');

const hasRequiredSource = (row) =>
  String(row.sourceName || '').trim() !== '' && String(row.sourceUrl || '').trim() !== '';

const relevantFieldsForGap = (gapType) => {
  if (gapType === 'dimensions_missing') return dimensionManualFields;
  if (gapType === 'performance_specs_missing') return performanceManualFields;
  return [];
};

const requiredResolvedAfterPatch = ({ gapType, after }) => {
  if (gapType === 'dimensions_missing') {
    return Boolean(after.practicalityBasis?.seatingCapacity && after.practicalityBasis?.lengthMm && after.practicalityBasis?.widthMm);
  }

  if (gapType === 'performance_specs_missing') {
    return Boolean(after.performanceBasis?.powerBhp && after.performanceBasis?.torqueNm);
  }

  return false;
};

const clone = (value) => JSON.parse(JSON.stringify(value || {}));

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

  const results = [];
  const errors = [];
  const warnings = [];

  for (const row of rows) {
    const manualFilled = hasManualValue(row);

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
        },
      },
    );

    const gap = await gaps.findOne(
      {
        variantProfileKey: row.variantProfileKey,
        gapType: row.gapType,
        status: 'open',
      },
      { projection: { _id: 0, gapType: 1, priority: 1, evidence: 1 } },
    );

    if (!profile) {
      errors.push({ row: row.__rowNumber, variantProfileKey: row.variantProfileKey, error: 'profile_not_found' });
      continue;
    }

    if (!gap) {
      warnings.push({ row: row.__rowNumber, variantProfileKey: row.variantProfileKey, warning: 'open_gap_not_found_may_already_be_resolved' });
    }

    if (manualFilled && !hasRequiredSource(row)) {
      errors.push({
        row: row.__rowNumber,
        variantFullName: row.variantFullName,
        error: 'manual_values_need_sourceName_and_sourceUrl',
      });
    }

    for (const field of numberFields) {
      const value = String(row[field] || '').trim();
      if (!value) continue;
      if (toNumberOrNull(value) === null) {
        errors.push({
          row: row.__rowNumber,
          variantFullName: row.variantFullName,
          field,
          value,
          error: 'manual_numeric_value_invalid',
        });
      }
    }

    const before = {
      practicalityBasis: clone(profile.practicalityBasis),
      performanceBasis: clone(profile.performanceBasis),
    };

    const after = clone(before);
    const patches = [];
    const conflicts = [];

    for (const [manualField, targetPath, shortField] of relevantFieldsForGap(row.gapType)) {
      const manualValue = toNumberOrNull(row[manualField]);
      if (manualValue === null) continue;

      const currentValue = getPath(before, targetPath);

      if (currentValue !== null && currentValue !== undefined && currentValue !== '') {
        const currentNumber = Number(currentValue);

        if (Number.isFinite(currentNumber) && Math.abs(currentNumber - manualValue) > 0.01) {
          conflicts.push({
            field: shortField,
            currentValue: currentNumber,
            manualValue,
            action: 'will_not_overwrite_existing_value',
          });
        }

        continue;
      }

      const [root, key] = targetPath.split('.');
      if (!after[root]) after[root] = {};
      after[root][key] = manualValue;

      patches.push({
        field: shortField,
        targetPath,
        value: manualValue,
        sourceName: row.sourceName,
        sourceUrl: row.sourceUrl,
      });
    }

    results.push({
      row: row.__rowNumber,
      variantProfileKey: row.variantProfileKey,
      variantFullName: row.variantFullName,
      gapType: row.gapType,
      manualFilled,
      sourceName: row.sourceName,
      sourceUrl: row.sourceUrl,
      patches,
      conflicts,
      resolvesGapAfterPatch: requiredResolvedAfterPatch({ gapType: row.gapType, after }),
      before,
      after,
    });
  }

  const summary = {
    file: path.resolve(filePath),
    rows: rows.length,
    rowsWithManualValues: results.filter((r) => r.manualFilled).length,
    blankRows: results.filter((r) => !r.manualFilled).length,
    patchRows: results.filter((r) => r.patches.length > 0).length,
    patchFields: results.reduce((sum, r) => sum + r.patches.length, 0),
    rowsResolvingGapAfterPatch: results.filter((r) => r.resolvesGapAfterPatch).length,
    conflictRows: results.filter((r) => r.conflicts.length > 0).length,
    errors: errors.length,
    warnings: warnings.length,
  };

  console.log(JSON.stringify({
    summary,
    errors,
    warnings,
    patchPreview: results
      .filter((r) => r.manualFilled || r.patches.length || r.conflicts.length)
      .map((r) => ({
        row: r.row,
        variantFullName: r.variantFullName,
        gapType: r.gapType,
        patches: r.patches,
        conflicts: r.conflicts,
        resolvesGapAfterPatch: r.resolvesGapAfterPatch,
      })),
    blankRows: results
      .filter((r) => !r.manualFilled)
      .map((r) => ({
        row: r.row,
        variantFullName: r.variantFullName,
        gapType: r.gapType,
      })),
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
