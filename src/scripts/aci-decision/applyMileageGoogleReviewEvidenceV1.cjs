#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');

const WRITE = process.argv.includes('--write');

const fileArgIndex = process.argv.indexOf('--file');
const FILE_PATH =
  fileArgIndex >= 0
    ? process.argv[fileArgIndex + 1]
    : 'src/scripts/aci-data/manual/mileage_google_review_grouped_engine_family.csv';

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const now = new Date();

const parseCsv = (text) => {
  const lines = text.replace(/\r/g, '').split('\n').filter((line) => line.trim() !== '');

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

  const parsed = lines.map(parseLine);
  const headerIndex = parsed.findIndex((row) => row[0] === 'make' && row[1] === 'model');

  if (headerIndex < 0) {
    throw new Error('Could not find CSV header row starting make,model');
  }

  const headers = parsed[headerIndex].map((h) => String(h || '').trim());

  return parsed.slice(headerIndex + 1).map((values, idx) => {
    const row = {};
    headers.forEach((h, i) => {
      row[h] = String(values[i] ?? '').trim();
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

const looksLikeUrl = (value) => /^https?:\/\//i.test(String(value || '').trim());

const domainFromUrl = (url) => {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch (_) {
    return '';
  }
};

const splitKeys = (value) =>
  String(value || '')
    .split('|')
    .map((x) => x.trim())
    .filter(Boolean);

const isFilledRow = (row) => toNumberOrNull(row.evidenceValue) !== null;

const getSourceInfo = (row) => {
  const sourceUrl =
    looksLikeUrl(row.sourceUrl)
      ? row.sourceUrl
      : looksLikeUrl(row.evidenceBasis)
        ? row.evidenceBasis
        : '';

  const sourceName =
    row.sourceName ||
    (sourceUrl ? domainFromUrl(sourceUrl) : '') ||
    'manual_google_review';

  const evidenceBasis =
    row.evidenceBasis && !looksLikeUrl(row.evidenceBasis)
      ? row.evidenceBasis
      : 'manual_google_review_claimed_mileage';

  return { sourceUrl, sourceName, evidenceBasis };
};

const mileageFieldSet = ({ fuel, value }) => {
  const fuelKey = String(fuel || '').trim().toLowerCase();

  const set = {
    'mileageBasis.primaryMileageValue': value,
    'mileageBasis.primaryMileageBasis': 'manual_google_review_claimed_mileage',
  };

  if (fuelKey === 'cng') {
    set['mileageBasis.cngMileageKmPerKg'] = value;
    set['mileageBasis.primaryMileageUnit'] = 'km/kg';
  } else {
    set['mileageBasis.claimedMileageKmpl'] = value;
    set['mileageBasis.primaryMileageUnit'] = 'kmpl';
  }

  return set;
};

(async () => {
  if (!fs.existsSync(FILE_PATH)) {
    throw new Error(`CSV not found: ${FILE_PATH}`);
  }

  const rows = parseCsv(fs.readFileSync(FILE_PATH, 'utf8'));
  const filledRows = rows.filter(isFilledRow);

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const profiles = db.collection(PROFILE_COLLECTION);
  const gaps = db.collection(GAP_COLLECTION);

  const openMileageGaps = await gaps.find(
    { status: 'open', gapType: 'mileage_specs_missing' },
    {
      projection: {
        _id: 1,
        variantProfileKey: 1,
        variantFullName: 1,
        fuelKey: 1,
        transmissionKey: 1,
      },
    },
  ).toArray();

  const openKeys = new Set(openMileageGaps.map((g) => g.variantProfileKey));
  const acceptedKeys = new Set();

  const profileOps = [];
  const gapOps = [];
  const acceptedPreview = [];

  for (const row of filledRows) {
    const value = toNumberOrNull(row.evidenceValue);
    if (value === null) continue;

    const variantProfileKeys = splitKeys(row.variantProfileKeys).filter((key) => openKeys.has(key));
    if (!variantProfileKeys.length) continue;

    variantProfileKeys.forEach((key) => acceptedKeys.add(key));

    const { sourceUrl, sourceName, evidenceBasis } = getSourceInfo(row);

    const setBase = {
      updatedAt: now,
      'dataQuality.hasMileageData': true,
      'dataQuality.mileageCompletenessStatus': 'manual_google_review_complete',
      'dataQuality.mileageEvidenceReviewed': true,
      'dataQuality.mileageEvidenceReviewedAt': now,
      'manualEvidence.mileageEvidence': {
        value,
        fuel: row.fuel || '',
        transmission: row.transmission || '',
        transmissionSubtype: row.transmissionSubtype || '',
        engineFamily: row.engineFamily || '',
        evidenceBasis,
        sourceName,
        sourceUrl,
        sourceNote:
          row.note ||
          'Applied from user-reviewed mileage evidence group. Stored as claimed/manual review mileage, not official ARAI unless source explicitly supports it.',
        appliedBy: 'applyMileageGoogleReviewEvidenceV1',
        appliedAt: now,
      },
    };

    const mileageSet = mileageFieldSet({ fuel: row.fuel, value });

    profileOps.push({
      updateMany: {
        filter: { variantProfileKey: { $in: variantProfileKeys } },
        update: { $set: { ...setBase, ...mileageSet } },
      },
    });

    gapOps.push({
      updateMany: {
        filter: {
          status: 'open',
          gapType: 'mileage_specs_missing',
          variantProfileKey: { $in: variantProfileKeys },
        },
        update: {
          $set: {
            status: 'resolved',
            resolutionStatus: 'manual_google_review_complete',
            resolvedBy: 'applyMileageGoogleReviewEvidenceV1',
            resolvedAt: now,
            sourceName,
            sourceUrl,
            updatedAt: now,
          },
        },
      },
    });

    acceptedPreview.push({
      row: row.__rowNumber,
      make: row.make,
      model: row.model,
      fuel: row.fuel,
      transmissionSubtype: row.transmissionSubtype,
      engineFamily: row.engineFamily,
      value,
      sourceName,
      sourceUrl,
      variantCount: variantProfileKeys.length,
      sampleKeys: variantProfileKeys.slice(0, 5),
    });
  }

  const limitationKeys = openMileageGaps
    .map((g) => g.variantProfileKey)
    .filter((key) => !acceptedKeys.has(key));

  if (limitationKeys.length) {
    profileOps.push({
      updateMany: {
        filter: { variantProfileKey: { $in: limitationKeys } },
        update: {
          $set: {
            updatedAt: now,
            'dataQuality.hasMileageData': false,
            'dataQuality.mileageCompletenessStatus':
              'known_source_limitation_no_reliable_arai_mileage_source',
            'dataQuality.mileageEvidenceReviewed': true,
            'dataQuality.mileageEvidenceReviewedAt': now,
            'manualEvidence.mileageEvidence': {
              evidenceStatus: 'known_source_limitation_no_reliable_arai_mileage_source',
              sourceType: 'manual_google_review',
              sourceName: 'manual_google_review',
              sourceNote:
                'No reliable ARAI/certified claimed mileage accepted in this pass. Highway/city/user-reported mileage intentionally not used.',
              appliedBy: 'applyMileageGoogleReviewEvidenceV1',
              appliedAt: now,
            },
          },
        },
      },
    });

    gapOps.push({
      updateMany: {
        filter: {
          status: 'open',
          gapType: 'mileage_specs_missing',
          variantProfileKey: { $in: limitationKeys },
        },
        update: {
          $set: {
            status: 'known_source_limitation',
            resolutionStatus: 'known_source_limitation_no_reliable_arai_mileage_source',
            resolvedBy: 'applyMileageGoogleReviewEvidenceV1',
            resolvedAt: now,
            updatedAt: now,
          },
        },
      },
    });
  }

  let profileWriteResult = null;
  let gapWriteResult = null;

  if (WRITE) {
    if (profileOps.length) {
      const r = await profiles.bulkWrite(profileOps, { ordered: false });
      profileWriteResult = {
        matched: r.matchedCount || 0,
        modified: r.modifiedCount || 0,
        upserted: r.upsertedCount || 0,
      };
    }

    if (gapOps.length) {
      const r = await gaps.bulkWrite(gapOps, { ordered: false });
      gapWriteResult = {
        matched: r.matchedCount || 0,
        modified: r.modifiedCount || 0,
        upserted: r.upsertedCount || 0,
      };
    }
  }

  console.log(JSON.stringify({
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    file: path.resolve(FILE_PATH),
    csvRows: rows.length,
    filledRows: filledRows.length,
    openMileageGapsBefore: openMileageGaps.length,
    acceptedGroupRows: acceptedPreview.length,
    acceptedVariantKeys: acceptedKeys.size,
    limitationVariantKeys: limitationKeys.length,
    profileOps: profileOps.length,
    gapOps: gapOps.length,
    profileWriteResult,
    gapWriteResult,
    acceptedPreview: acceptedPreview.slice(0, 80),
    limitationSampleKeys: limitationKeys.slice(0, 20),
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
