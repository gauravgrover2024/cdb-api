import Vehicle from "../../models/Vehicle.js";
import VehicleFeature from "../../models/VehicleFeature.js";
import {
  action,
  unavailableWidget,
  widget,
} from "./aiAgent.renderPayloads.js";
import {
  firstMeaningful,
  formatDateValue,
  pickVehiclePrice,
} from "./aiAgent.normalizers.js";
import { LIMIT, pushModuleTrace, safeId } from "./aiAgent.tools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";

const vehicleModelQuery = (parsed) => {
  const model = parsed.entities.model || parsed.entities.models?.[0];
  const make = parsed.entities.make;
  const and = [];
  if (model) and.push({ model: { $regex: model, $options: "i" } });
  if (make) and.push({ make: { $regex: make, $options: "i" } });
  if (parsed.entities.variant) and.push({ variant: { $regex: parsed.entities.variant, $options: "i" } });
  return and.length ? { $and: and } : {};
};

const vehicleRow = (item) => ({
  id: safeId(item),
  make: item.make,
  model: item.model,
  variant: item.variant,
  fuel: firstMeaningful(item.fuel, item.fuel_type),
  transmission: firstMeaningful(item.transmission, item.transmission_type),
  exShowroomPrice: item.exShowroom || item.ex_showroom,
  onRoadPrice: firstMeaningful(item.onRoadPrice, item.on_road_price_cardekho, item.total_on_road_with_accessories),
  year: firstMeaningful(item.year, item.activeYear),
  status: firstMeaningful(item.status, item.is_discontinued ? "Discontinued" : "Active"),
  lastUpdated: formatDateValue(firstMeaningful(item.LastPriceChangeDate, item.updatedAt, item.scrape_timestamp)),
});

export const vehiclePricelist = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const query = vehicleModelQuery(parsed);
  if (!Object.keys(query).length) {
    return {
      widgets: [
        unavailableWidget(
          "Need a model",
          "Share a model such as Verna, City, or Slavia to fetch catalog data.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const rows = await Vehicle.find(query).sort({ make: 1, model: 1, variant: 1 }).limit(100).lean();
  pushModuleTrace(trace, "Vehicles", rows.length);
  if (!rows.length) {
    return { widgets: [unavailableWidget("No pricelist found", "No matching vehicle catalog records were found.", ["Vehicles"])] };
  }
  const model = parsed.entities.model || rows[0]?.model;
  const featureDocs = await VehicleFeature.find({ model: { $regex: model, $options: "i" } }).limit(20).lean();
  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);
  const wantsColors = /colors|colours/.test(parsed.lower);
  const wantsSunroof = /sunroof/.test(parsed.lower);
  const notices = [];
  if (wantsColors) notices.push("Dedicated color data was not found in the catalog fields scanned.");
  if (wantsSunroof) {
    const featureText = JSON.stringify(featureDocs).toLowerCase();
    notices.push(featureText.includes("sunroof") ? "Sunroof appears in feature data for matching variants." : "Sunroof was not found in the available feature data.");
  }
  return {
    widgets: [
      widget("vehicle_pricelist", `${model} pricelist`, {
        data: { make: rows[0]?.make, model, total: rows.length, features: featureDocs },
        columns: ["Make", "Model", "Variant", "Fuel", "Transmission", "Ex-showroom", "On-road", "Year", "Status", "Last updated"],
        rows: rows.map(vehicleRow),
        notices,
        actions: [
          action("open_pricelist_prefilled", "Open full pricelist", {
            route: "/vehicles/price-list",
            query: { make: rows[0]?.make, model },
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Show similar cars", "Compare with City and Slavia", "Show colors", "Show top variants", "Open full pricelist"],
  };
};

export const similarCars = async (parsed, access, trace) => {
  const baseResult = await vehiclePricelist(parsed, access, trace);
  const baseRows = baseResult.widgets?.[0]?.rows || [];
  if (!baseRows.length) return baseResult;
  const prices = baseRows.map((row) => Number(row.onRoadPrice || row.exShowroomPrice || 0)).filter(Boolean);
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const anchorModel = parsed.entities.model;
  const query = {
    model: { $not: new RegExp(anchorModel, "i") },
    $or: [
      { onRoadPrice: { $gte: min * 0.85, $lte: max * 1.15 } },
      { exShowroom: { $gte: min * 0.85, $lte: max * 1.15 } },
      { ex_showroom: { $gte: min * 0.85, $lte: max * 1.15 } },
    ],
  };
  const vehicles = await Vehicle.find(query).limit(200).lean();
  pushModuleTrace(trace, "Vehicles similar", vehicles.length);
  const grouped = new Map();
  for (const item of vehicles) {
    const key = `${item.make || ""}:${item.model || ""}`.toLowerCase();
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  }
  const rows = [...grouped.values()].slice(0, 12).map((items) => {
    const prices = items.map(pickVehiclePrice).filter(Boolean);
    return {
      make: items[0].make,
      model: items[0].model,
      segment: firstMeaningful(items[0].segment, items[0].bodyType, items[0].body_type),
      priceRange: prices.length ? { min: Math.min(...prices), max: Math.max(...prices) } : null,
      fuelOptions: [...new Set(items.map((item) => firstMeaningful(item.fuel, item.fuel_type)).filter(Boolean))],
      transmissionOptions: [...new Set(items.map((item) => item.transmission).filter(Boolean))],
      matchingReason: "Similar catalog price band",
    };
  });
  return {
    widgets: [
      widget("similar_cars", `Similar cars to ${anchorModel}`, {
        rows,
        data: { anchorModel, priceBand: { min, max } },
        actions: rows.map((row) =>
          action("open_pricelist_prefilled", `Open ${row.model}`, {
            route: "/vehicles/price-list",
            query: { make: row.make, model: row.model },
          }),
        ),
      }),
    ],
    followUpSuggestions: ["Compare with City and Slavia", "View variants", "Open full pricelist"],
  };
};

export const vehicleComparison = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const models = parsed.entities.models || [];
  if (models.length < 2) {
    return { widgets: [unavailableWidget("Need models to compare", "Ask with two or more models, for example: compare Verna City Slavia.", ["Vehicles"])] };
  }
  const rows = [];
  for (const model of models) {
    const docs = await Vehicle.find({ model: { $regex: model, $options: "i" } }).limit(100).lean();
    pushModuleTrace(trace, `Vehicles ${model}`, docs.length);
    const prices = docs.map(pickVehiclePrice).filter(Boolean);
    rows.push({
      make: docs[0]?.make,
      model: docs[0]?.model || model,
      startingPrice: prices.length ? Math.min(...prices) : null,
      topPrice: prices.length ? Math.max(...prices) : null,
      variantCount: docs.length,
      fuelOptions: [...new Set(docs.map((item) => firstMeaningful(item.fuel, item.fuel_type)).filter(Boolean))],
      transmissionOptions: [...new Set(docs.map((item) => item.transmission).filter(Boolean))],
      lastUpdated: formatDateValue(docs[0]?.updatedAt),
      actions: [
        action("open_pricelist_prefilled", "Open Pricelist", {
          route: "/vehicles/price-list",
          query: { make: docs[0]?.make, model: docs[0]?.model || model },
        }),
      ],
    });
  }
  return {
    widgets: [widget("vehicle_comparison", "Vehicle comparison", { rows, data: { models } })],
    followUpSuggestions: ["Show similar cars", "Show top variants", "Open full pricelist"],
  };
};

export const priceHistoryReport = async (parsed, access, trace) => {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const query = { createdAt: { $gte: start, $lte: now } };
  if (parsed.entities.model) query.model = { $regex: parsed.entities.model, $options: "i" };
  const rows = await Vehicle.find(query).sort({ createdAt: -1 }).limit(LIMIT).lean();
  pushModuleTrace(trace, "Vehicles", rows.length);
  return {
    widgets: [
      widget("price_history_report", "Variants added this month", {
        summary: { count: rows.length, periodStart: start.toISOString(), periodEnd: now.toISOString() },
        rows: rows.map(vehicleRow),
        notices: ["Price history is not stored as a dedicated history table. Showing records inferred from createdAt/updatedAt."],
        actions: [
          action("open_pricelist_prefilled", "Open pricelist", {
            route: "/vehicles/price-list",
            query: parsed.entities.model ? { model: parsed.entities.model } : {},
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Open full pricelist", "Show top variants", "Compare with City and Slavia"],
  };
};
