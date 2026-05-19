import dotenv from "dotenv";
import mongoose from "mongoose";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const COLLECTION = "vehicle_features";

const walk = (obj, prefix = "", out = {}) => {
  if (!obj || typeof obj !== "object") return out;

  if (Array.isArray(obj)) {
    const key = `${prefix}[]`;
    out[key] ||= { count: 0, examples: [] };
    out[key].count += 1;

    if (obj.length && out[key].examples.length < 3) {
      out[key].examples.push(JSON.stringify(obj[0]).slice(0, 500));
    }

    obj.slice(0, 3).forEach((item) => walk(item, key, out));
    return out;
  }

  Object.entries(obj).forEach(([key, value]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    const type = Array.isArray(value) ? "array" : typeof value;

    out[path] ||= {
      count: 0,
      types: {},
      examples: [],
    };

    out[path].count += 1;
    out[path].types[type] = (out[path].types[type] || 0) + 1;

    if (
      value !== null &&
      value !== undefined &&
      typeof value !== "object" &&
      out[path].examples.length < 5
    ) {
      out[path].examples.push(String(value).slice(0, 200));
    }

    if (value && typeof value === "object") {
      walk(value, path, out);
    }
  });

  return out;
};

const main = async () => {
  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;
  const c = db.collection(COLLECTION);

  const count = await c.countDocuments();
  console.log("Collection:", COLLECTION);
  console.log("Count:", count);

  const samples = await c.find({}).limit(20).toArray();

  console.log("\n================ SAMPLE DOCS ================");
  samples.slice(0, 5).forEach((doc, index) => {
    console.log(`\n--- DOC ${index + 1} ---`);
    console.log(JSON.stringify(doc, null, 2).slice(0, 5000));
  });

  const paths = {};

  samples.forEach((doc) => walk(doc, "", paths));

  const sorted = Object.entries(paths)
    .sort((a, b) => b[1].count - a[1].count)
    .map(([path, meta]) => ({
      path,
      count: meta.count,
      types: meta.types,
      examples: meta.examples,
    }));

  console.log("\n================ TOP PATHS ================");
  sorted.slice(0, 250).forEach((item) => {
    console.log(JSON.stringify(item));
  });

  console.log("\n================ LIKELY FEATURE ARRAYS ================");
  sorted
    .filter((item) =>
      item.path.includes("[]") ||
      /feature|spec|section|category|comfort|safety|exterior|interior|engine/i.test(item.path)
    )
    .slice(0, 150)
    .forEach((item) => console.log(JSON.stringify(item)));

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
