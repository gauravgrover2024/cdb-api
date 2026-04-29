import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";

dotenv.config();

const parseAmount = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string") {
    const parsed = Number(value.replace(/[^0-9.-]/g, ""));
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
};

const firstPositiveAmount = (...values) => {
  for (const value of values) {
    const amount = parseAmount(value);
    if (amount > 0) return amount;
  }
  return 0;
};

const nearlyEqual = (a, b) => Math.abs(parseAmount(a) - parseAmount(b)) <= 1;

const normalizePricing = (doc) => {
  const tcs = firstPositiveAmount(doc.tcs, doc.other_tcsCharges);
  const optionalTotal = firstPositiveAmount(
    doc.optional_total,
    doc.optional_totalAccessories,
    doc.optional_accessoriesCharges,
  );
  const orpWithoutAccessories = firstPositiveAmount(doc.orp_without_accessories);
  const totalOnRoad = firstPositiveAmount(
    doc.total_on_road_with_accessories,
    doc.on_road_price_cardekho,
    doc.onRoadPrice,
    orpWithoutAccessories && optionalTotal
      ? orpWithoutAccessories + optionalTotal
      : 0,
  );
  const rawOtherTotal = firstPositiveAmount(doc.other_totalOtherCharges, doc.otherCharges);
  const explicitOther = firstPositiveAmount(
    doc.handlingCharges,
    doc.other_otherCharges,
    doc.other_handlingCharges,
  );
  const otherCharges =
    explicitOther ||
    (rawOtherTotal && tcs
      ? rawOtherTotal > tcs && !nearlyEqual(rawOtherTotal, tcs)
        ? Math.max(rawOtherTotal - tcs, 0)
        : nearlyEqual(rawOtherTotal, tcs)
          ? 0
          : rawOtherTotal
      : rawOtherTotal || 0);
  const onRoadWithoutAccessories =
    orpWithoutAccessories ||
    (totalOnRoad && optionalTotal && totalOnRoad >= optionalTotal
      ? totalOnRoad - optionalTotal
      : totalOnRoad);

  return {
    tcs,
    optionalTotal,
    otherCharges,
    orpWithoutAccessories: onRoadWithoutAccessories,
    totalOnRoad,
  };
};

const isDifferent = (current, next) => {
  if (typeof current === "number" || typeof next === "number") {
    return !nearlyEqual(current, next);
  }
  return current !== next;
};

const main = async () => {
  const apply = process.argv.includes("--apply");
  await connectDB();

  const docs = await Vehicle.find({})
    .select({
      tcs: 1,
      other_tcsCharges: 1,
      optional_total: 1,
      optional_totalAccessories: 1,
      optional_accessoriesCharges: 1,
      orp_without_accessories: 1,
      total_on_road_with_accessories: 1,
      on_road_price_cardekho: 1,
      onRoadPrice: 1,
      otherCharges: 1,
      other_totalOtherCharges: 1,
      handlingCharges: 1,
      other_otherCharges: 1,
      other_handlingCharges: 1,
    })
    .lean();

  const operations = [];
  let checked = 0;
  let changed = 0;

  for (const doc of docs) {
    checked += 1;
    const pricing = normalizePricing(doc);
    if (!pricing.totalOnRoad) continue;

    const next = {
      onRoadPrice: pricing.totalOnRoad,
      on_road_price_cardekho: pricing.totalOnRoad,
      total_on_road_with_accessories: pricing.totalOnRoad,
      orp_without_accessories: pricing.orpWithoutAccessories,
      tcs: pricing.tcs,
      other_tcsCharges: pricing.tcs,
      optional_total: pricing.optionalTotal,
      optional_totalAccessories: pricing.optionalTotal,
      otherCharges: pricing.otherCharges,
      other_totalOtherCharges: pricing.otherCharges,
    };

    const needsUpdate = Object.entries(next).some(([key, value]) =>
      isDifferent(doc[key], value),
    );
    if (!needsUpdate) continue;

    changed += 1;
    operations.push({
      updateOne: {
        filter: { _id: doc._id },
        update: { $set: next },
      },
    });
  }

  console.log(
    JSON.stringify(
      {
        checked,
        needingUpdate: changed,
        apply,
      },
      null,
      2,
    ),
  );

  if (apply && operations.length) {
    const result = await Vehicle.bulkWrite(operations, { ordered: false });
    console.log(
      JSON.stringify(
        {
          matched: result.matchedCount,
          modified: result.modifiedCount,
        },
        null,
        2,
      ),
    );
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error("normalizeVehiclePricing failed:", error);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
