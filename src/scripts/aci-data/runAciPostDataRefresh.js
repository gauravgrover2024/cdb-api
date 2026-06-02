#!/usr/bin/env node

import { spawn } from "node:child_process";
import process from "node:process";
import dotenv from "dotenv";
import { MongoClient } from "mongodb";

dotenv.config();

const args = new Set(process.argv.slice(2));

const write = args.has("--write");
const dryRun = args.has("--dry-run") || !write;
const resetDerived = args.has("--reset-derived");
const skipDecision = args.has("--skip-decision");
const skipCrash = args.has("--skip-crash");
const skipGapQueue = args.has("--skip-gap-queue");
const skipProgress = args.has("--skip-progress");
const decisionOnly = args.has("--decision-only");

if (write && args.has("--dry-run")) {
  console.error("Use either --write or --dry-run, not both.");
  process.exit(1);
}

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const SOURCE_COLLECTIONS = [
  "vehicles",
  "vehicle_features",
  "vehicle_colors_v2",
  "variant_features",
];

const DERIVED_COLLECTIONS = [
  "aci_vehicle_price_rows",
  "aci_vehicle_model_summary",
  "vehicle_feature_catalog_v2",
  "vehicle_variant_feature_matrix_v2",
  "aci_vehicle_variant_decision_profile",
  "aci_vehicle_variant_city_price_profile",
  "aci_vehicle_variant_upgrade_ladder",
  "aci_variant_data_gap_queue",
  "aci_vehicle_crash_safety_profile",
  "aci_variant_external_evidence",
];

const DUPLICATE_CHECKS = [
  {
    collection: "aci_vehicle_price_rows",
    keys: ["makeKey", "modelKey", "variantKey", "citySlug"],
  },
  {
    collection: "aci_vehicle_model_summary",
    keys: ["makeKey", "modelKey", "citySlug"],
  },
  {
    collection: "vehicle_feature_catalog_v2",
    keys: ["canonicalKey"],
  },
  {
    collection: "vehicle_variant_feature_matrix_v2",
    keys: ["modelKey", "variantKey"],
  },
  {
    collection: "aci_vehicle_variant_decision_profile",
    keys: ["variantProfileKey"],
  },
  {
    collection: "aci_vehicle_variant_city_price_profile",
    keys: ["cityPriceProfileKey"],
  },
  {
    collection: "aci_vehicle_variant_city_price_profile",
    keys: ["variantProfileKey", "citySlug"],
  },
  {
    collection: "aci_vehicle_variant_upgrade_ladder",
    keys: ["ladderKey"],
  },
  {
    collection: "aci_variant_data_gap_queue",
    keys: ["gapKey"],
  },
  {
    collection: "aci_vehicle_crash_safety_profile",
    keys: ["crashSafetyProfileKey"],
  },
  {
    collection: "aci_variant_external_evidence",
    keys: ["evidenceKey"],
  },
];

const resetArg = () => (resetDerived ? ["--reset"] : []);
const writeArg = () => (write ? ["--write"] : []);

const command = (label, commandName, commandArgs, options = {}) => ({
  label,
  commandName,
  commandArgs,
  options,
});

const baseEnv = {
  ...process.env,
  FORCE_COLOR: process.env.FORCE_COLOR || "1",
};

const readModelCommand = () =>
  command(
    "A. Build ACI price rows and model summaries",
    "node",
    ["src/scripts/aci-read-models/buildAciVehicleReadModels.js"],
    {
      env: {
        ...baseEnv,
        ACI_READ_MODEL_DRY_RUN: write ? "false" : "true",
      },
    },
  );

const normalizeVehicleRawPriceCommand = () =>
  command(
    "A0. Normalize vehicle raw price fields",
    "node",
    [
      "src/scripts/aci-data/normalizeVehicleRawPriceFieldsV1.js",
      ...writeArg(),
    ],
  );

const steps = [];

if (write) {
  steps.push(
    command("Prepare builder indexes", "node", [
      "src/scripts/aci-maintenance/ensureAciBuilderIndexes.js",
    ]),
  );
}

if (!decisionOnly) {
  steps.push(normalizeVehicleRawPriceCommand());
  steps.push(readModelCommand());

  steps.push(
    command(
      "B. Build feature catalog and variant feature matrix",
      "node",
      [
        "src/scripts/buildVehicleFeatureKnowledgeBaseV2.js",
        ...writeArg(),
        ...(write ? ["--replace"] : []),
      ],
    ),
  );

  if (write) {
    steps.push(
      command("C. Ensure vehicle/color/source indexes", "npm", [
        "run",
        "indexes:vehicle",
      ]),
    );
    steps.push(
      command("C. Ensure feature KB indexes", "node", [
        "src/scripts/ensureFeatureKbV2Indexes.js",
      ]),
    );
    steps.push(
      command("C. Ensure feature matrix indexes", "node", [
        "src/scripts/aci-maintenance/ensureAciFeatureMatrixIndexes.js",
      ]),
    );
    steps.push(
      command("C. Ensure decision/read-model indexes", "node", [
        "src/scripts/aci-maintenance/createAciRecommendationIndexes.js",
      ]),
    );
  } else {
    steps.push(
      command("C. Audit core read-model/index health", "node", [
        "src/scripts/aci-audits/auditAciCoreReadModelsAndIndexes.js",
      ]),
    );
  }
}

if (!skipDecision) {
  steps.push(
    command("D. Build variant decision profiles", "node", [
      "src/scripts/aci-decision/buildVariantDecisionProfilesFastV2.js",
      ...writeArg(),
      ...resetArg(),
    ]),
  );

  steps.push(
    command("E. Build supported-city price overlay", "node", [
      "src/scripts/aci-decision/buildVariantCityPriceProfilesFastV2.js",
      ...writeArg(),
      ...resetArg(),
    ]),
  );

  if (!skipCrash) {
    steps.push(
      command("F. Build crash safety profiles", "node", [
        "src/scripts/aci-decision/buildCrashSafetyProfilesFromFeatureMatrixV1.js",
        ...writeArg(),
        ...resetArg(),
      ]),
    );

    steps.push(
      command("F1. Build inherited model-level crash safety profiles", "node", [
        "src/scripts/aci-decision/buildInheritedModelLevelCrashSafetyProfilesV1.cjs",
        ...writeArg(),
      ]),
    );

    steps.push(
      command("G. Patch decision profiles from crash safety", "node", [
        "src/scripts/aci-decision/patchVariantDecisionProfilesFromCrashSafetyV1.js",
        ...writeArg(),
      ]),
    );
  }

  steps.push(
    command("H. Build ranks and upgrade ladder", "node", [
      "src/scripts/aci-decision/buildVariantRanksAndUpgradeLadderV1.js",
      ...writeArg(),
      ...resetArg(),
    ]),
  );

  if (!skipGapQueue) {
    steps.push(
      command("I. Build data gap queue", "node", [
        "src/scripts/aci-decision/buildVariantDataGapQueueV1.js",
        ...writeArg(),
        ...resetArg(),
      ]),
    );

    steps.push(
      command("I. Seed controlled evidence queue", "node", [
        "src/scripts/aci-decision/buildVariantExternalEvidenceSeedV1.js",
        ...writeArg(),
      ]),
    );

    steps.push(
      command("I. Resolve P0 evidence from internal derived sources", "node", [
        "src/scripts/aci-decision/resolveP0EvidenceInternalSourcesV1.js",
        ...writeArg(),
      ]),
    );

    steps.push(
      command("I. Audit P0 evidence against raw variant features", "node", [
        "src/scripts/aci-decision/auditP0RawVariantFeaturesEvidenceV1.js",
      ]),
    );

    steps.push(
      command("I. Patch profiles from reviewed internal feature evidence", "node", [
        "src/scripts/aci-decision/patchVariantDecisionProfilesFromFeatureEvidenceV1.js",
        ...writeArg(),
      ]),
    );

    steps.push(
      command("H/I. Rebuild ranks after evidence patch", "node", [
        "src/scripts/aci-decision/buildVariantRanksAndUpgradeLadderV1.js",
        ...writeArg(),
        ...resetArg(),
      ]),
    );

    steps.push(
      command("I. Rebuild final data gap queue", "node", [
        "src/scripts/aci-decision/buildVariantDataGapQueueV1.js",
        ...writeArg(),
        ...resetArg(),
      ]),
    );
  }
}

if (!decisionOnly) {
  steps.push(
    command("Verify core read models and indexes", "node", [
      "src/scripts/aci-audits/auditAciCoreReadModelsAndIndexes.js",
    ]),
  );
  steps.push(
    command("Verify candidate retriever smoke", "node", [
      "src/scripts/aci-evals/runAciCandidateRetrieverSmoke.js",
    ]),
  );
}

if (!skipProgress && !dryRun) {
  steps.push(
    command("J. Verify ACI progress registry snapshot", "node", [
      "-e",
      [
        "const { getAciProgressSnapshot } = require('./src/services/aciProgress/aciProgress.service.cjs');",
        "const s = getAciProgressSnapshot();",
        "console.log(JSON.stringify({ modules: s.modules.length, source: s.meta.source, reportsFound: s.meta.reportsFound, latestReports: s.meta.latestReports }, null, 2));",
      ].join(" "),
    ]),
  );
}

const nowIso = () => new Date().toISOString();

const formatDuration = (ms) => `${(ms / 1000).toFixed(2)}s`;

const runStep = (step) =>
  new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const rendered = [step.commandName, ...step.commandArgs].join(" ");

    console.log("\n" + "=".repeat(80));
    console.log(`STEP: ${step.label}`);
    console.log(`CMD : ${rendered}`);
    console.log("=".repeat(80));

    const child = spawn(step.commandName, step.commandArgs, {
      cwd: process.cwd(),
      env: step.options.env || baseEnv,
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("close", (code, signal) => {
      const durationMs = Date.now() - startedAt;
      if (code === 0) {
        console.log(`DONE: ${step.label} in ${formatDuration(durationMs)}`);
        resolve({
          label: step.label,
          command: rendered,
          durationMs,
          exitCode: code,
        });
        return;
      }

      reject(
        new Error(
          `${step.label} failed with code ${code ?? "null"} signal ${signal ?? "none"}`,
        ),
      );
    });
  });

const getDb = async () => {
  if (!mongoUri) return { client: null, db: null };
  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 60000 });
  await client.connect();
  return { client, db: client.db() };
};

const collectionExists = async (db, collectionName) =>
  db.listCollections({ name: collectionName }, { nameOnly: true }).hasNext();

const countCollection = async (db, collectionName) => {
  if (!(await collectionExists(db, collectionName))) {
    return { exists: false, count: 0 };
  }

  const count = await db.collection(collectionName).estimatedDocumentCount();
  return { exists: true, count };
};

const countSnapshot = async (db, collectionNames) => {
  if (!db) return {};
  const entries = [];
  for (const name of collectionNames) {
    const result = await countCollection(db, name);
    entries.push([name, result]);
  }
  return Object.fromEntries(entries);
};

const countDuplicates = async (db, { collection, keys }) => {
  if (!(await collectionExists(db, collection))) {
    return { collection, keys, exists: false, duplicateGroups: 0, samples: [] };
  }

  const id = Object.fromEntries(keys.map((key) => [key, `$${key}`]));
  const rows = await db
    .collection(collection)
    .aggregate([
      { $group: { _id: id, count: { $sum: 1 } } },
      { $match: { count: { $gt: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 },
    ])
    .toArray();

  return {
    collection,
    keys,
    exists: true,
    duplicateGroups: rows.length,
    samples: rows,
  };
};

const gapSummary = async (db) => {
  const collection = "aci_variant_data_gap_queue";
  if (!(await collectionExists(db, collection))) {
    return { exists: false };
  }

  const col = db.collection(collection);
  const [byStatus, byPriority, openP0ByType, unresolvedP0Samples] = await Promise.all([
    col
      .aggregate([{ $group: { _id: "$status", count: { $sum: 1 } } }, { $sort: { count: -1 } }])
      .toArray(),
    col
      .aggregate([{ $group: { _id: "$priority", count: { $sum: 1 } } }, { $sort: { _id: 1 } }])
      .toArray(),
    col
      .aggregate([
        { $match: { status: "open", priority: "P0" } },
        { $group: { _id: "$gapType", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ])
      .toArray(),
    col
      .find(
        { status: "open", priority: "P0" },
        {
          projection: {
            _id: 0,
            gapKey: 1,
            variantFullName: 1,
            gapType: 1,
            evidence: 1,
          },
        },
      )
      .limit(20)
      .toArray(),
  ]);

  return {
    exists: true,
    byStatus,
    byPriority,
    openP0ByType,
    unresolvedP0Samples,
  };
};

const printSnapshot = (title, snapshot) => {
  console.log("\n" + title);
  console.table(
    Object.entries(snapshot).map(([collection, value]) => ({
      collection,
      exists: value.exists,
      count: value.count,
    })),
  );
};

async function main() {
  const startedAt = Date.now();

  console.log("=".repeat(80));
  console.log("ACI POST DATA REFRESH ORCHESTRATOR");
  console.log("=".repeat(80));
  console.log(
    JSON.stringify(
      {
        mode: write ? "WRITE" : "DRY_RUN",
        startedAt: nowIso(),
        resetDerived,
        skipDecision,
        skipCrash,
        skipGapQueue,
        skipProgress,
        decisionOnly,
        sourceCollections: SOURCE_COLLECTIONS,
        derivedCollections: DERIVED_COLLECTIONS,
      },
      null,
      2,
    ),
  );

  const { client, db } = await getDb();
  if (!db) {
    console.warn("Mongo URI not found. Continuing without count/duplicate audit.");
  }

  let beforeSources = {};
  let beforeDerived = {};
  let beforeGapSummary = {};

  try {
    beforeSources = await countSnapshot(db, SOURCE_COLLECTIONS);
    beforeDerived = await countSnapshot(db, DERIVED_COLLECTIONS);
    beforeGapSummary = db ? await gapSummary(db) : {};
  } finally {
    if (client) await client.close();
  }

  if (db || Object.keys(beforeSources).length) {
    printSnapshot("Source collections before refresh", beforeSources);
    printSnapshot("Derived collections before refresh", beforeDerived);
    console.log("\nGap queue before refresh");
    console.log(JSON.stringify(beforeGapSummary, null, 2));
  }

  const results = [];
  for (const step of steps) {
    results.push(await runStep(step));
  }

  const post = await getDb();
  let afterSources = {};
  let afterDerived = {};
  let afterGapSummary = {};
  let duplicateChecks = [];

  try {
    afterSources = await countSnapshot(post.db, SOURCE_COLLECTIONS);
    afterDerived = await countSnapshot(post.db, DERIVED_COLLECTIONS);
    afterGapSummary = post.db ? await gapSummary(post.db) : {};
    if (post.db) {
      for (const check of DUPLICATE_CHECKS) {
        duplicateChecks.push(await countDuplicates(post.db, check));
      }
    }
  } finally {
    if (post.client) await post.client.close();
  }

  printSnapshot("Source collections after refresh", afterSources);
  printSnapshot("Derived collections after refresh", afterDerived);

  console.log("\nDuplicate key checks");
  console.log(JSON.stringify(duplicateChecks, null, 2));

  console.log("\nGap queue after refresh");
  console.log(JSON.stringify(afterGapSummary, null, 2));

  console.log("\nRefresh step summary");
  console.table(
    results.map((result) => ({
      step: result.label,
      seconds: Number((result.durationMs / 1000).toFixed(2)),
      exitCode: result.exitCode,
    })),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: write ? "WRITE" : "DRY_RUN",
        startedAt: new Date(startedAt).toISOString(),
        finishedAt: nowIso(),
        durationMs: Date.now() - startedAt,
        steps: results.length,
        resetDerived,
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
