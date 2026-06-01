#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const fs = require('fs');
const mongoose = require('mongoose');

const EVIDENCE_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';

const args = process.argv.slice(2);
const write = args.includes('--write');
const logPath = args.find((arg) => arg.startsWith('--log='))?.slice('--log='.length) || '/tmp/aci_raw_variant_features_audit_v1.log';

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const parseNumber = (value) => {
  const match = String(value ?? '').replace(/,/g, '').match(/-?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
};

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const normalizeTransmission = (value) => {
  const v = String(value ?? '').trim().toLowerCase();
  if (!v) return { transmission: null, transmissionKey: null };
  if (/automatic|dct|dsg|cvt|ivt|amt|at\b/.test(v)) return { transmission: 'Automatic', transmissionKey: 'automatic' };
  if (/manual|mt\b/.test(v)) return { transmission: 'Manual', transmissionKey: 'manual' };
  return { transmission: String(value).trim(), transmissionKey: normalizeKey(value) || null };
};

const readJsonFromLog = (path) => {
  const text = fs.readFileSync(path, 'utf8');
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start < 0 || end < start) throw new Error(`Could not find JSON object in ${path}`);
  return JSON.parse(text.slice(start, end + 1));
};

const pickBestCandidate = (candidates = []) => {
  if (!candidates.length) return null;

  return candidates.slice().sort((a, b) => {
    const aDate = Date.parse(a.scrape_timestamp || a.last_updated || '') || 0;
    const bDate = Date.parse(b.scrape_timestamp || b.last_updated || '') || 0;
    return bDate - aDate;
  })[0];
};

const normalizedFieldsFromCandidate = (candidate) => {
  const extracted = candidate.extracted || {};
  const transmission = normalizeTransmission(extracted.transmission);

  return {
    sourceCollection: candidate.collectionName || null,
    sourceFeatureDocId: candidate.id || null,
    sourceBrand: candidate.brand || null,
    sourceModel: candidate.model || null,
    sourceVariant: candidate.variant || null,
    exactVariantMatch: candidate.exactVariantMatch === true,
    sameModelBrand: candidate.sameModelBrand === true,
    lastUpdated: candidate.last_updated || null,
    scrapeTimestamp: candidate.scrape_timestamp || null,

    rawTransmission: extracted.transmission ?? null,
    transmission: transmission.transmission,
    transmissionKey: transmission.transmissionKey,
    gearbox: extracted.gearbox ?? null,

    engineCc: parseNumber(extracted.engineCc),
    powerRaw: extracted.power ?? null,
    torqueRaw: extracted.torque ?? null,
    powerBhp: parseNumber(extracted.power),
    torqueNm: parseNumber(extracted.torque),

    fuelType: extracted.fuelType ?? null,
    araiMileage: parseNumber(extracted.araiMileage),
    fuelTankCapacity: parseNumber(extracted.fuelTankCapacity),

    lengthMm: parseNumber(extracted.length),
    widthMm: parseNumber(extracted.width),
    heightMm: parseNumber(extracted.height),
    bootSpaceLitres: parseNumber(extracted.bootSpace),
    seatingCapacity: parseNumber(extracted.seatingCapacity),
    wheelbaseMm: parseNumber(extracted.wheelBase),
    groundClearanceMm: parseNumber(extracted.groundClearance),

    featureCount: extracted.featureCount ?? null,
    rawCandidateCount: null,
  };
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  const audit = readJsonFromLog(logPath);
  const resolvableItems = (audit.items || [])
    .map((item) => ({
      item,
      candidate: pickBestCandidate(item.candidates || []),
    }))
    .filter(({ candidate }) => Boolean(candidate));

  const updates = resolvableItems.map(({ item, candidate }) => {
    const normalizedFields = normalizedFieldsFromCandidate(candidate);
    normalizedFields.rawCandidateCount = item.candidatesFound || (item.candidates || []).length || 1;

    return {
      evidenceKey: item.evidenceKey,
      variantFullName: item.variantFullName,
      evidenceType: item.evidenceType,
      normalizedFields,
      status: 'internal_raw_source_ready',
      reviewStatus: 'auto_raw_vehicle_features_exact_match',
      confidence: normalizedFields.transmission ? 'high' : 'medium',
      sourceName: normalizedFields.sourceCollection,
      sourceType: 'internal_raw_db',
      notes: 'Resolved from prior raw vehicle_features / variant_features audit log using exact variant and model/brand match. Ready for controlled profile patcher.',
    };
  });

  const summary = {
    mode: write ? 'WRITE' : 'DRY_RUN',
    logPath,
    auditSeeds: audit.seeds,
    resolvableItems: updates.length,
    unresolvedFromAudit: (audit.items || []).length - updates.length,
    byEvidenceType: updates.reduce((acc, row) => {
      acc[row.evidenceType] = (acc[row.evidenceType] || 0) + 1;
      return acc;
    }, {}),
    samples: updates.slice(0, 20),
  };

  let writeResult = null;

  if (write) {
    await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
    const db = mongoose.connection.db;
    const evidenceCol = db.collection(EVIDENCE_COLLECTION);

    const bulk = updates.map((row) => ({
      updateOne: {
        filter: { evidenceKey: row.evidenceKey },
        update: {
          $set: {
            status: row.status,
            reviewStatus: row.reviewStatus,
            confidence: row.confidence,
            sourceName: row.sourceName,
            sourceType: row.sourceType,
            sourceFetchedAt: new Date(),
            normalizedFields: row.normalizedFields,
            notes: row.notes,
            updatedAt: new Date(),
          },
        },
      },
    }));

    if (bulk.length) {
      const result = await evidenceCol.bulkWrite(bulk, { ordered: false });
      writeResult = {
        matched: result.matchedCount || 0,
        modified: result.modifiedCount || 0,
      };
    } else {
      writeResult = { matched: 0, modified: 0 };
    }

    await mongoose.disconnect();
  }

  console.log(JSON.stringify({
    ...summary,
    writeResult,
  }, null, 2));
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
