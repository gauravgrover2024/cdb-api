#!/usr/bin/env node

require("dotenv").config();

const PUBLIC_ENDPOINT =
  process.env.ACI_TRACE_AUDIT_PUBLIC_ENDPOINT ||
  process.env.ACI_DEEP_AUDIT_PUBLIC_ENDPOINT ||
  "http://localhost:5050/api/ai-agent/public-chat";

const STRICT = String(process.env.ACI_TRACE_AUDIT_STRICT || "0") === "1";

const selectedVehicle = ({ make = "", model = "", variant = "", city = "new-delhi", citySlug = city } = {}) => ({
  make,
  model,
  fullModel: [make, model].filter(Boolean).join(" "),
  variant,
  variantName: variant,
  fullVariant: [make, model, variant].filter(Boolean).join(" "),
  city,
  citySlug,
});

const activeComparison = (...vehicles) => ({
  vehicles: vehicles.map((vehicle) => ({
    make: vehicle.make,
    model: vehicle.model,
    fullModel: vehicle.fullModel || [vehicle.make, vehicle.model].filter(Boolean).join(" "),
    variant: vehicle.variant || "",
  })),
});

const V = {
  creta: selectedVehicle({ make: "Hyundai", model: "Creta" }),
  cretaDelhi: selectedVehicle({ make: "Hyundai", model: "Creta", city: "new-delhi" }),
  seltos: selectedVehicle({ make: "Kia", model: "Seltos" }),
  baleno: selectedVehicle({ make: "Maruti", model: "Baleno" }),
};

const C = {
  cretaSeltos: { activeComparison: activeComparison(V.creta, V.seltos) },
};

const CASES = [
  {
    id: "trace_price_pricelist",
    kind: "price",
    message: "Creta price Delhi",
    requiredCollectionsAny: ["aci_vehicle_price_rows"],
  },
  {
    id: "trace_unsupported_city_price",
    kind: "unsupported_city",
    message: "seltos price bangalore",
    requiredCollectionsAny: ["aci_vehicle_price_rows"],
    allowMatchedZero: true,
  },
  {
    id: "trace_feature_lookup",
    kind: "feature",
    message: "Creta airbags",
    requiredCollectionsAny: ["vehicle_variant_feature_matrix_v2", "vehicle_feature_catalog_v2"],
  },
  {
    id: "trace_feature_context",
    kind: "feature",
    message: "features in this",
    context: { selectedVehicle: V.creta },
    requiredCollectionsAny: ["vehicle_variant_feature_matrix_v2", "vehicle_feature_catalog_v2"],
  },
  {
    id: "trace_colors",
    kind: "colors",
    message: "Seltos colors",
    requiredCollectionsAny: ["vehicle_colors_v2"],
  },
  {
    id: "trace_spec",
    kind: "spec",
    message: "baleno mileage",
    requiredCollectionsAny: ["aci_vehicle_model_summary", "vehicle_variant_feature_matrix_v2"],
  },
  {
    id: "trace_comparison",
    kind: "comparison",
    message: "price difference",
    context: C.cretaSeltos,
    requiredCollectionsAny: ["aci_vehicle_price_rows", "vehicle_variant_feature_matrix_v2"],
  },
];

const asArray = (value) => (Array.isArray(value) ? value : []);
const text = (value) => String(value || "");
const unique = (values = []) => [...new Set(values.map(text).filter(Boolean))];

function unwrap(payload) {
  // /api/ai-agent/public-chat returns the normalized ACI response at top level.
  // The top-level response may also contain a nested `data` payload, but that
  // nested object can intentionally omit contract fields such as
  // sourceTransparency/runtimeResultsMeta/contextPatch. Do not unwrap to
  // payload.data unless this is clearly a wrapper-style response.
  if (payload?.response && typeof payload.response === "object") return payload.response;
  if (payload?.data?.response && typeof payload.data.response === "object") return payload.data.response;

  const looksLikeAciTopLevel =
    payload &&
    typeof payload === "object" &&
    (
      payload.intent ||
      payload.canvasType ||
      payload.sourceTransparency ||
      payload.runtimeResultsMeta ||
      payload.contextPatch ||
      payload.aciCoreBridge ||
      payload.service ||
      payload.widget ||
      payload.widgets ||
      payload.rows ||
      payload.records ||
      payload.variants
    );

  if (looksLikeAciTopLevel) return payload;

  return payload?.data || payload?.result || payload;
}

function collectCollections(output = {}) {
  const collections = [];

  const add = (value) => {
    if (!value) return;
    if (Array.isArray(value)) value.forEach(add);
    else collections.push(String(value));
  };

  add(output.modulesChecked);
  add(output.source);
  add(output.dataSource);
  add(output.sourceTransparency?.modulesChecked);
  add(output.sourceTransparency?.source);
  add(output.sourceTransparency?.dataSource);
  add(output.meta?.sourceTransparency?.modulesChecked);
  add(output.meta?.sourceTransparency?.source);
  add(output.meta?.sourceTransparency?.dataSource);
  add(output.data?.sourceTransparency?.modulesChecked);
  add(output.data?.sourceTransparency?.source);
  add(output.data?.sourceTransparency?.dataSource);

  for (const item of asArray(output.runtimeResultsMeta || output.meta?.runtimeResultsMeta || output.data?.runtimeResultsMeta)) {
    add(item.modulesChecked);
    add(item.source);
    add(item.dataSource);
  }

  for (const row of asArray(output.rows || output.data?.rows || output.items || output.data?.items)) {
    add(row.sourceCollection);
    add(row.source);
    add(row.dataSource);
    add(row.collection);
  }

  return unique(collections)
    .map((value) => value.replace(/:not_connected$/i, ""))
    .filter(Boolean);
}

function getRecordCount(output = {}) {
  const candidates = [
    output.recordCount,
    output.sourceTransparency?.recordCount,
    output.sourceTransparency?.matched,
    output.matched,
    output.count,
    output.totalVariants,
    output.data?.recordCount,
    output.data?.sourceTransparency?.recordCount,
    output.data?.sourceTransparency?.matched,
    output.data?.matched,
    output.data?.count,
    output.data?.totalVariants,
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }

  for (const key of ["rows", "records", "variants", "items", "colors", "values", "models"]) {
    if (Array.isArray(output[key])) return output[key].length;
    if (Array.isArray(output.data?.[key])) return output.data[key].length;
  }

  return null;
}

function getTraceObjects(output = {}) {
  return {
    sourceTransparency:
      output.sourceTransparency ||
      output.meta?.sourceTransparency ||
      output.data?.sourceTransparency ||
      null,
    runtimeResultsMeta:
      output.runtimeResultsMeta ||
      output.meta?.runtimeResultsMeta ||
      output.data?.runtimeResultsMeta ||
      null,
    trace:
      output.trace ||
      output.meta?.trace ||
      output.data?.trace ||
      null,
    evidence:
      output.evidence ||
      output.dbEvidence ||
      output.meta?.dbEvidence ||
      output.data?.dbEvidence ||
      null,
  };
}

function classify(testCase, output = {}) {
  const collections = collectCollections(output);
  const recordCount = getRecordCount(output);
  const traceObjects = getTraceObjects(output);

  const hasRequiredCollection = asArray(testCase.requiredCollectionsAny).some((needle) =>
    collections.some((value) => value === needle || value.includes(needle)),
  );

  const hasAnyTraceObject = Boolean(
    traceObjects.sourceTransparency ||
    traceObjects.runtimeResultsMeta ||
    traceObjects.trace ||
    traceObjects.evidence,
  );

  const hasRecordSignal =
    testCase.allowMatchedZero ||
    (Number.isFinite(Number(recordCount)) && Number(recordCount) > 0);

  const missing = [];
  if (!hasRequiredCollection) missing.push("required_collection_signal");
  if (!hasAnyTraceObject) missing.push("trace_or_source_transparency_object");
  if (!hasRecordSignal) missing.push("record_count_or_match_signal");

  return {
    id: testCase.id,
    kind: testCase.kind,
    message: testCase.message,
    pass: missing.length === 0,
    missing,
    observed: {
      collections,
      recordCount,
      hasSourceTransparency: Boolean(traceObjects.sourceTransparency),
      hasRuntimeResultsMeta: Boolean(traceObjects.runtimeResultsMeta),
      hasTrace: Boolean(traceObjects.trace),
      hasEvidence: Boolean(traceObjects.evidence),
      topLevelKeys: Object.keys(output || {}),
      dataKeys: Object.keys(output.data || {}),
      title: output.title || output.data?.title || "",
      canvasType: output.canvasType || output.data?.canvasType || "",
      answerPreview: text(output.answer || output.message || "").replace(/\s+/g, " ").slice(0, 220),
    },
  };
}

async function callCase(testCase) {
  const res = await fetch(PUBLIC_ENDPOINT, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      message: testCase.message,
      context: testCase.context || {},
    }),
  });

  const raw = await res.text();
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return {
      id: testCase.id,
      kind: testCase.kind,
      message: testCase.message,
      pass: false,
      missing: ["invalid_json_response"],
      observed: { status: res.status, rawPreview: raw.slice(0, 400) },
    };
  }

  const output = unwrap(payload);
  return classify(testCase, output);
}

async function main() {
  const results = [];

  for (const testCase of CASES) {
    results.push(await callCase(testCase));
  }

  const failed = results.filter((row) => !row.pass);

  const summary = {
    suite: "ACI Factual Trace Metadata Audit v1",
    ok: STRICT ? failed.length === 0 : true,
    strict: STRICT,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((row) => row.id),
    endpoint: PUBLIC_ENDPOINT,
    results,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (STRICT && failed.length) {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
