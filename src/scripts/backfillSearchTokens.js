import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Customer from "../models/Customer.js";
import Showroom from "../models/Showroom.js";
import Vehicle from "../models/Vehicle.js";
import { buildSearchTokens } from "../utils/searchTokens.js";
import { normalizeVehicleDatasetRow } from "../utils/vehicleDatasetNormalizer.js";

dotenv.config();

const sameArray = (left = [], right = []) => {
  const a = Array.isArray(left) ? left : [];
  const b = Array.isArray(right) ? right : [];
  return a.length === b.length && a.every((value, index) => value === b[index]);
};

const flushOps = async (Model, ops, label) => {
  if (!ops.length) return { matched: 0, modified: 0 };
  const result = await Model.bulkWrite(ops.splice(0), { ordered: false });
  console.log(
    `${label}: matched ${result.matchedCount || 0}, modified ${result.modifiedCount || 0}`,
  );
  return result;
};

const backfillModel = async ({ Model, label, projection, buildTokens }) => {
  const cursor = Model.find({}, projection).lean().cursor();
  const ops = [];
  let checked = 0;
  let changed = 0;

  for await (const doc of cursor) {
    checked += 1;
    const searchTokens = buildTokens(doc);
    if (sameArray(doc.searchTokens, searchTokens)) continue;

    changed += 1;
    ops.push({
      updateOne: {
        filter: { _id: doc._id },
        update: { $set: { searchTokens } },
      },
    });

    if (ops.length >= 500) {
      await flushOps(Model, ops, label);
    }
  }

  await flushOps(Model, ops, label);
  console.log(`${label}: checked ${checked}, needing update ${changed}`);
};

const run = async () => {
  await connectDB();

  await backfillModel({
    Model: Showroom,
    label: "showrooms",
    projection: {
      showroomId: 1,
      name: 1,
      businessName: 1,
      contactPerson: 1,
      mobile: 1,
      alternatePhone: 1,
      city: 1,
      state: 1,
      status: 1,
      brands: 1,
      brandKeys: 1,
      searchTokens: 1,
    },
    buildTokens: (doc) =>
      buildSearchTokens([
        doc.showroomId,
        doc.name,
        doc.businessName,
        doc.contactPerson,
        doc.mobile,
        doc.alternatePhone,
        doc.city,
        doc.state,
        doc.status,
        doc.brands,
        doc.brandKeys,
      ]),
  });

  await backfillModel({
    Model: Customer,
    label: "customers",
    projection: {
      customerId: 1,
      customerName: 1,
      companyName: 1,
      contactPersonName: 1,
      primaryMobile: 1,
      extraMobiles: 1,
      whatsappNumber: 1,
      panNumber: 1,
      aadharNumber: 1,
      aadhaarNumber: 1,
      gstNumber: 1,
      city: 1,
      state: 1,
      companyCity: 1,
      registrationCity: 1,
      searchTokens: 1,
    },
    buildTokens: (doc) =>
      buildSearchTokens([
        doc.customerId,
        doc.customerName,
        doc.companyName,
        doc.contactPersonName,
        doc.primaryMobile,
        doc.extraMobiles,
        doc.whatsappNumber,
        doc.panNumber,
        doc.aadharNumber,
        doc.aadhaarNumber,
        doc.gstNumber,
        doc.city,
        doc.state,
        doc.companyCity,
        doc.registrationCity,
      ]),
  });

  await backfillModel({
    Model: Vehicle,
    label: "vehicles",
    projection: {
      make: 1,
      brand: 1,
      model: 1,
      variant: 1,
      fuel: 1,
      fuel_type: 1,
      city: 1,
      brand_normalized: 1,
      model_normalized: 1,
      variant_normalized: 1,
      search_text: 1,
      searchTokens: 1,
    },
    buildTokens: (doc) => normalizeVehicleDatasetRow(doc).searchTokens,
  });

  await Promise.all([
    Showroom.collection.createIndex({ searchTokens: 1 }, { name: "searchTokens_1" }),
    Customer.collection.createIndex({ searchTokens: 1 }, { name: "searchTokens_1" }),
    Vehicle.collection.createIndex({ searchTokens: 1 }, { name: "searchTokens_1" }),
  ]);
  console.log("search token indexes ensured");
};

run()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (mongoose.connection.readyState !== 0) {
      await mongoose.connection.close();
    }
  });
