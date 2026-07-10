#!/usr/bin/env node
"use strict";

require("dotenv").config();

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const mongoose = require("mongoose");

const COLLECTION = "aci_feature_explainers_v1";
const CATALOG_COLLECTION = "vehicle_feature_catalog_v2";
const SOURCE_PATH = path.resolve(
  __dirname,
  "../aci-data/manual/aciFeatureExplainersV1.json",
);

const shouldWrite = process.argv.includes("--write");

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const clean = (value = "") => String(value ?? "").replace(/\s+/g, " ").trim();
const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const loadSeed = () => JSON.parse(fs.readFileSync(SOURCE_PATH, "utf8"));

const validateSeed = (seed = {}) => {
  assert(clean(seed.schemaVersion), "schemaVersion is required");
  assert(clean(seed.contentVersion), "contentVersion is required");
  assert(asArray(seed.entries).length > 0, "at least one explainer entry is required");

  const keys = new Set();
  for (const entry of seed.entries) {
    const key = clean(entry.canonicalKey);
    assert(key, "canonicalKey is required");
    assert(!keys.has(key), `duplicate canonicalKey: ${key}`);
    keys.add(key);
    assert(entry.status === "published" || entry.status === "draft", `${key}: invalid status`);
    assert(clean(entry.buyerSummary), `${key}: buyerSummary is required`);
    assert(clean(entry.howItWorks), `${key}: howItWorks is required`);
    assert(clean(entry.whenItMattersSummary), `${key}: whenItMattersSummary is required`);
    assert(clean(entry.limitationsSummary), `${key}: limitationsSummary is required`);
    assert(asArray(entry.sourceRefs).length > 0, `${key}: sourceRefs are required`);
    for (const source of entry.sourceRefs) {
      assert(/^https:\/\//i.test(clean(source.url)), `${key}: source URL must use HTTPS`);
      assert(clean(source.title), `${key}: source title is required`);
    }
  }
};

async function main() {
  const seed = loadSeed();
  validateSeed(seed);

  const uri = mongoUri();
  assert(uri, "Mongo URI is required to validate feature explainers against the catalog");
  await mongoose.connect(uri);

  try {
    const db = mongoose.connection.db;
    const keys = seed.entries.map((entry) => entry.canonicalKey);
    const catalogRows = await db.collection(CATALOG_COLLECTION)
      .find(
        { canonicalKey: { $in: keys } },
        {
          projection: {
            _id: 0,
            canonicalKey: 1,
            displayName: 1,
            groupKey: 1,
            groupLabel: 1,
            aliases: 1,
          },
        },
      )
      .toArray();
    const catalogByKey = new Map(catalogRows.map((row) => [row.canonicalKey, row]));
    const missingCatalogKeys = keys.filter((key) => !catalogByKey.has(key));
    assert.strictEqual(
      missingCatalogKeys.length,
      0,
      `explainer keys missing from ${CATALOG_COLLECTION}: ${missingCatalogKeys.join(", ")}`,
    );

    const now = new Date();
    const docs = seed.entries.map((entry) => {
      const catalog = catalogByKey.get(entry.canonicalKey);
      return {
        ...entry,
        schemaVersion: seed.schemaVersion,
        contentVersion: seed.contentVersion,
        displayName: catalog.displayName,
        groupKey: catalog.groupKey,
        groupLabel: catalog.groupLabel,
        aliases: [...new Set([
          ...asArray(catalog.aliases),
          ...asArray(entry.aliases),
        ].map(clean).filter(Boolean))],
        sourceCatalogCollection: CATALOG_COLLECTION,
        sourceSeedPath: path.relative(process.cwd(), SOURCE_PATH),
        updatedAt: now,
      };
    });

    if (shouldWrite) {
      const collection = db.collection(COLLECTION);
      await collection.bulkWrite(
        docs.map((doc) => ({
          updateOne: {
            filter: { canonicalKey: doc.canonicalKey },
            update: {
              $set: doc,
              $setOnInsert: { createdAt: now },
            },
            upsert: true,
          },
        })),
        { ordered: true },
      );
      await collection.createIndex({ canonicalKey: 1 }, { unique: true });
      await collection.createIndex({ aliases: 1 });
      await collection.createIndex({ status: 1, groupKey: 1 });
    }

    console.log(JSON.stringify({
      suite: "ACI Feature Explainer build v1",
      ok: true,
      mode: shouldWrite ? "write" : "dry_run",
      source: path.relative(process.cwd(), SOURCE_PATH),
      collection: COLLECTION,
      catalogCollection: CATALOG_COLLECTION,
      entries: docs.length,
      canonicalKeys: docs.map((doc) => doc.canonicalKey),
      published: docs.filter((doc) => doc.status === "published").length,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Feature Explainer build v1",
    ok: false,
    error: error.message,
  }, null, 2));
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
