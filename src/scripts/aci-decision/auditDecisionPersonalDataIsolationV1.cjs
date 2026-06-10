#!/usr/bin/env node

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');

const DECISION_COLLECTIONS = [
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile',
  process.env.ACI_VARIANT_CITY_PRICE_PROFILE_COLLECTION || 'aci_vehicle_variant_city_price_profile',
  process.env.ACI_VARIANT_UPGRADE_LADDER_COLLECTION || 'aci_vehicle_variant_upgrade_ladder',
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile',
  process.env.ACI_FEATURE_SCORE_MATRIX_PROJECTION_COLLECTION || 'aci_feature_score_matrix_projection_v1',
];

const SCAN_PATHS = [
  'src/services/aciCore/decisionPolicy',
  'src/services/aciCore/decisionProfiles',
  'src/services/aciCore/scoreProfiles',
  'src/scripts/aci-decision',
];

const CURRENT_FILE = path.normalize(path.resolve(__filename));

const FORBIDDEN_FIELD_KEYS = [
  'userId',
  'user_id',
  'accountId',
  'customerId',
  'customer_id',
  'phone',
  'phoneNumber',
  'mobile',
  'mobileNumber',
  'whatsapp',
  'whatsappNumber',
  'email',
  'emailAddress',
  'ip',
  'ipAddress',
  'sessionId',
  'session_id',
  'deviceId',
  'leadId',
  'crmLeadId',
  'consent',
  'consentId',
  'consentStatus',
  'dealerSharingConsent',
  'pincode',
  'pinCode',
  'address',
  'fullAddress',
  'latitude',
  'longitude',
  'geoLocation',
  'userLocation',
  'conversationId',
  'chatId',
  'messageId',
  'buyerMemory',
  'personalData',
];

const CODE_PATTERNS = [
  { name: 'phone_or_mobile', regex: /\b(phoneNumber|mobileNumber|whatsappNumber|phone|mobile|whatsapp)\b/i },
  { name: 'email', regex: /\b(email|emailAddress)\b/i },
  { name: 'lead_or_crm', regex: /\b(leadId|crmLeadId|CRM|dealerSharingConsent)\b/ },
  { name: 'session_or_user_identity', regex: /\b(userId|sessionId|deviceId|ipAddress|conversationId|chatId)\b/ },
  { name: 'address_or_precise_location', regex: /\b(pincode|pinCode|address|fullAddress|latitude|longitude|geoLocation|userLocation)\b/ },
  { name: 'consent_or_personal_data', regex: /\b(consentId|consentStatus|personalData|buyerMemory)\b/ },
];

const SAFE_CODE_MATCHES = [
  /auditDecisionPersonalDataIsolationV1\.cjs/,
];

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const listFiles = (dir, out = []) => {
  if (!fs.existsSync(dir)) return out;

  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      listFiles(fullPath, out);
    } else if (/\.(cjs|js|json)$/.test(entry.name)) {
      out.push(fullPath);
    }
  }

  return out;
};

const scanCode = () => {
  const matches = [];

  const files = SCAN_PATHS.flatMap((scanPath) => listFiles(scanPath))
    .map((file) => path.normalize(path.resolve(file)))
    .filter((file) => file !== CURRENT_FILE);

  for (const file of files) {
    const relFile = path.relative(process.cwd(), file);

    if (SAFE_CODE_MATCHES.some((safe) => safe.test(relFile))) continue;

    const text = fs.readFileSync(file, 'utf8');
    const lines = text.split(/\r?\n/);

    lines.forEach((line, index) => {
      for (const pattern of CODE_PATTERNS) {
        if (pattern.regex.test(line)) {
          matches.push({
            file: relFile,
            line: index + 1,
            pattern: pattern.name,
            text: line.trim().slice(0, 220),
          });
        }
      }
    });
  }

  return {
    scannedFileCount: files.length,
    matches,
  };
};

async function scanCollection(db, collectionName) {
  const exists = await db.listCollections({ name: collectionName }).hasNext();

  if (!exists) {
    return {
      collection: collectionName,
      exists: false,
      total: 0,
      offendingDocCount: 0,
      offendingFields: [],
      samples: [],
    };
  }

  const col = db.collection(collectionName);
  const total = await col.countDocuments();

  const offendingFields = [];
  const samples = [];

  for (const field of FORBIDDEN_FIELD_KEYS) {
    const count = await col.countDocuments({ [field]: { $exists: true } });

    if (count > 0) {
      offendingFields.push({ field, count });

      const fieldSamples = await col
        .find(
          { [field]: { $exists: true } },
          {
            projection: {
              _id: 1,
              variantProfileKey: 1,
              cityPriceProfileKey: 1,
              ladderKey: 1,
              modelKey: 1,
              variantKey: 1,
              [field]: 1,
            },
          }
        )
        .limit(5)
        .toArray();

      samples.push({
        field,
        samples: fieldSamples.map((doc) => ({
          ...doc,
          _id: String(doc._id),
        })),
      });
    }
  }

  const offendingDocCount =
    offendingFields.length === 0
      ? 0
      : await col.countDocuments({
          $or: FORBIDDEN_FIELD_KEYS.map((field) => ({ [field]: { $exists: true } })),
        });

  return {
    collection: collectionName,
    exists: true,
    total,
    checkedMode: 'fast_top_level_field_existence',
    offendingDocCount,
    offendingFields,
    samples,
  };
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const collectionResults = [];
  for (const collectionName of DECISION_COLLECTIONS) {
    collectionResults.push(await scanCollection(db, collectionName));
  }

  const codeScan = scanCode();

  const dbOffendingCollections = collectionResults.filter((result) => result.offendingDocCount > 0);
  const codeMatches = codeScan.matches;

  const failures = [];

  if (dbOffendingCollections.length > 0) {
    failures.push(
      `personal-data-like fields found in decision collections: ${JSON.stringify(
        dbOffendingCollections.map((result) => ({
          collection: result.collection,
          offendingDocCount: result.offendingDocCount,
          offendingFields: result.offendingFields,
        }))
      )}`
    );
  }

  if (codeMatches.length > 0) {
    failures.push(
      `personal-data-like code references found inside decision/score paths: ${JSON.stringify(
        codeMatches.slice(0, 30)
      )}`
    );
  }

  const summary = {
    suite: 'ACI Decision Personal Data Isolation Audit v1',
    ok: failures.length === 0,
    dpdpRule:
      'Decision/read-model collections must remain non-personal. Lead, CRM, WhatsApp, buyer memory, consent, and dealer-sharing data must stay in DPDP-governed modules.',
    collections: collectionResults,
    codeScan,
    failures,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) process.exit(1);
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
