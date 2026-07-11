#!/usr/bin/env node

import "dotenv/config";
import {
  CopyObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const WRITE = process.argv.includes("--write");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const LIMIT = Math.max(0, Number(limitArg?.slice("--limit=".length) || 0));
const CONCURRENCY = Math.max(1, Math.min(Number(process.env.ACI_R2_METADATA_WORKERS || 6), 12));
const CACHE_CONTROL = "public, max-age=31536000, immutable";
const BUCKET = process.env.R2_CAR_IMAGES_BUCKET || process.env.R2_BUCKET;
const PUBLIC_BASE = String(
  process.env.R2_PUBLIC_BASE_URL ||
    "https://pub-8504a10fc1c04f02ac8760cb90462ae3.r2.dev",
).replace(/\/+$/, "");

const required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"];
const missing = required.filter((name) => !process.env[name]);
if (!BUCKET) missing.push("R2_CAR_IMAGES_BUCKET or R2_BUCKET");
if (missing.length) throw new Error(`Missing R2 environment variables: ${missing.join(", ")}`);

const client = new S3Client({
  region: "auto",
  endpoint: process.env.R2_ENDPOINT || `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const collectUrls = (value, out) => {
  if (typeof value === "string") {
    try {
      const url = new URL(value);
      const isKnownR2Host =
        url.origin === PUBLIC_BASE || url.hostname.endsWith(".r2.dev");
      if (
        url.protocol === "https:" &&
        isKnownR2Host &&
        url.pathname.startsWith("/media/car-images/normalized/")
      ) {
        out.add(value);
      }
    } catch {
      // Stored non-URL strings are unrelated to R2 object metadata.
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectUrls(item, out));
    return;
  }
  if (value && typeof value === "object") {
    Object.values(value).forEach((item) => collectUrls(item, out));
  }
};

const keyFromUrl = (url) => decodeURIComponent(new URL(url).pathname.replace(/^\/+/, ""));
const encodeCopySource = (bucket, key) =>
  `${encodeURIComponent(bucket)}/${key.split("/").map(encodeURIComponent).join("/")}`;

const updateKey = async (key) => {
  const head = await client.send(new HeadObjectCommand({
    Bucket: BUCKET,
    Key: key,
  }));
  if (String(head.CacheControl || "").toLowerCase() === CACHE_CONTROL) {
    return { key, status: "current" };
  }
  if (!WRITE) return { key, status: "would_update", previous: head.CacheControl || "" };

  await client.send(new CopyObjectCommand({
    Bucket: BUCKET,
    Key: key,
    CopySource: encodeCopySource(BUCKET, key),
    MetadataDirective: "REPLACE",
    Metadata: head.Metadata || {},
    CacheControl: CACHE_CONTROL,
    ContentType: head.ContentType,
    ContentDisposition: head.ContentDisposition,
    ContentEncoding: head.ContentEncoding,
    ContentLanguage: head.ContentLanguage,
  }));
  return { key, status: "updated", previous: head.CacheControl || "" };
};

await connectDB();

try {
  const docs = await mongoose.connection.db
    .collection("vehicle_colors_v2")
    .find({})
    .project({ _id: 0 })
    .toArray();
  const urls = new Set();
  docs.forEach((doc) => collectUrls(doc, urls));
  const keys = [...urls].map(keyFromUrl).slice(0, LIMIT || undefined);
  const results = [];

  for (let offset = 0; offset < keys.length; offset += CONCURRENCY) {
    const batch = keys.slice(offset, offset + CONCURRENCY);
    const settled = await Promise.allSettled(batch.map(updateKey));
    settled.forEach((result, index) => {
      if (result.status === "fulfilled") results.push(result.value);
      else results.push({ key: batch[index], status: "failed", error: result.reason?.message || String(result.reason) });
    });
  }

  const counts = results.reduce((summary, item) => {
    summary[item.status] = (summary[item.status] || 0) + 1;
    return summary;
  }, {});
  console.log(JSON.stringify({
    suite: "ACI image immutable cache-control migration v1",
    ok: !counts.failed,
    mode: WRITE ? "write" : "dry_run",
    cacheControl: CACHE_CONTROL,
    discoveredUrls: urls.size,
    processed: results.length,
    counts,
    failures: results.filter((item) => item.status === "failed").slice(0, 20),
  }, null, 2));
  if (counts.failed) process.exitCode = 1;
} finally {
  await mongoose.disconnect();
}
