import asyncHandler from 'express-async-handler';
import mongoose from 'mongoose';
import Vehicle from '../models/Vehicle.js';

const normalizeText = (value) => String(value || '').trim().toLowerCase();

const escapeRegex = (value) => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const trimLeading = (value, prefix) => {
  const source = String(value || '').trim();
  const leader = String(prefix || '').trim();
  if (!source || !leader) return source;
  const escaped = escapeRegex(leader);
  return source.replace(new RegExp(`^${escaped}\\s*`, 'i'), '').trim();
};

const normalizeVehicleRecord = (doc) => {
  const raw = doc?.toObject ? doc.toObject() : { ...(doc || {}) };
  const make = String(raw.make || raw.brand || '').trim();
  const rawModel = String(raw.model || '').trim();
  const rawVariant = String(raw.variant || '').trim();
  const model = trimLeading(rawModel, make) || rawModel;
  const variant =
    trimLeading(rawVariant, `${make} ${rawModel}`.trim()) ||
    trimLeading(rawVariant, rawModel) ||
    trimLeading(rawVariant, `${make} ${model}`.trim()) ||
    trimLeading(rawVariant, make) ||
    rawVariant;

  return {
    ...raw,
    make,
    brand: String(raw.brand || make).trim(),
    model,
    rawModel,
    variant,
    rawVariant,
    fuel: raw.fuel || raw.fuel_type || '',
    fuel_type: raw.fuel_type || raw.fuel || '',
    exShowroom: Number(raw.exShowroom ?? raw.ex_showroom ?? 0),
    onRoadPrice: Number(raw.onRoadPrice ?? raw.on_road_price_cardekho ?? raw.total_on_road_with_accessories ?? 0),
    insurance: Number(raw.insurance ?? 0),
    rto: Number(raw.rto ?? 0),
    otherCharges: Number(raw.otherCharges ?? raw.other_totalOtherCharges ?? 0),
  };
};

const matchesExact = (actual, expected) => {
  if (!expected) return true;
  return normalizeText(actual) === normalizeText(expected);
};

const matchesVehicleFilters = (vehicle, filters = {}) => {
  const normalized = normalizeVehicleRecord(vehicle);
  if (!matchesExact(normalized.make, filters.make)) return false;
  if (!matchesExact(normalized.model, filters.model)) return false;
  if (!matchesExact(normalized.variant, filters.variant)) return false;
  if (!matchesExact(normalized.city, filters.city)) return false;
  if (!matchesExact(normalized.fuel, filters.fuel)) return false;
  return true;
};

const buildVehicleQuery = ({ q, city, fuel }) => {
  const query = {};

  if (q) {
    query.$or = [
      { make: new RegExp(q, 'i') },
      { brand: new RegExp(q, 'i') },
      { model: new RegExp(q, 'i') },
      { variant: new RegExp(q, 'i') },
    ];
  }

  if (city) query.city = new RegExp(`^${escapeRegex(String(city).trim())}$`, 'i');
  if (fuel) {
    const fuelRegex = new RegExp(`^${escapeRegex(String(fuel).trim())}$`, 'i');
    query.$and = [...(query.$and || []), { $or: [{ fuel: fuelRegex }, { fuel_type: fuelRegex }] }];
  }

  return query;
};

const sortVehicleRows = (rows) =>
  [...rows].sort((a, b) => {
    const aa = normalizeVehicleRecord(a);
    const bb = normalizeVehicleRecord(b);
    return [aa.make, aa.model, aa.variant].join('|').localeCompare([bb.make, bb.model, bb.variant].join('|'));
  });

const buildMakeRegex = (make) => new RegExp(`^${escapeRegex(String(make || '').trim())}$`, 'i');

const findDocsForMake = async (make) =>
  Vehicle.find({ $or: [{ make: buildMakeRegex(make) }, { brand: buildMakeRegex(make) }] }).lean();

const getVehicles = asyncHandler(async (req, res) => {
  const { q, make, model, city, fuel } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : null;
  const skip = Number(req.query.skip) || 0;

  const baseQuery = buildVehicleQuery({ q, city, fuel });
  const docs = await Vehicle.find(baseQuery).lean();

  const filtered = sortVehicleRows(
    docs.filter((doc) => matchesVehicleFilters(doc, { make, model, city, fuel })),
  ).map(normalizeVehicleRecord);

  const count = filtered.length;
  const data = pageSize ? filtered.slice(skip, skip + pageSize) : filtered.slice(skip);

  res.json({ success: true, count, data });
});

const getVehicleById = asyncHandler(async (req, res) => {
  if (!req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    res.status(400);
    throw new Error('Invalid vehicle ID format');
  }

  const vehicle = await Vehicle.findById(req.params.id);
  if (vehicle) {
    res.json({ success: true, data: normalizeVehicleRecord(vehicle) });
  } else {
    res.status(404);
    throw new Error('Vehicle not found');
  }
});

const createVehicle = asyncHandler(async (req, res) => {
  const make = String(req.body.make || req.body.brand || '').trim();
  const model = String(req.body.model || '').trim();
  const variant = String(req.body.variant || '').trim();
  const fuel = req.body.fuel || req.body.fuel_type;
  const city = req.body.city;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error('Please include Make, Model, and Variant');
  }

  const payload = { ...req.body, make, brand: req.body.brand || make, model, variant };
  const existingDocs = await Vehicle.find(buildVehicleQuery({ city, fuel }));
  const duplicate = existingDocs.find((doc) => matchesVehicleFilters(doc, { make, model, variant, city, fuel }));
  if (duplicate) {
    res.status(400);
    throw new Error('Vehicle variant already exists for this city/fuel combination');
  }

  const vehicle = await Vehicle.create(payload);
  res.status(201).json({ success: true, data: normalizeVehicleRecord(vehicle) });
});

const updateVehicle = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);

  if (vehicle) {
    const nextMake = String(req.body.make || req.body.brand || vehicle.make || vehicle.brand || '').trim();
    Object.assign(vehicle, { ...req.body, make: nextMake, brand: req.body.brand || vehicle.brand || nextMake });
    const updatedVehicle = await vehicle.save();
    res.json({ success: true, data: normalizeVehicleRecord(updatedVehicle) });
  } else {
    res.status(404);
    throw new Error('Vehicle not found');
  }
});

const deleteVehicle = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);
  if (vehicle) {
    await vehicle.deleteOne();
    res.json({ success: true, message: 'Vehicle removed' });
  } else {
    res.status(404);
    throw new Error('Vehicle not found');
  }
});

const bulkUploadVehicles = asyncHandler(async (req, res) => {
  const vehiclesData = req.body;

  if (!Array.isArray(vehiclesData)) {
    res.status(400);
    throw new Error('Expected an array of vehicle objects');
  }

  const results = { inserted: 0, updated: 0, errors: [] };

  for (const item of vehiclesData) {
    try {
      const make = String(item.make || item.brand || '').trim();
      const model = String(item.model || '').trim();
      const variant = String(item.variant || '').trim();
      const fuel = item.fuel || item.fuel_type;
      const city = item.city;
      if (!make || !model || !variant) continue;

      const payload = { ...item, make, brand: item.brand || make, model, variant };
      const existingDocs = await Vehicle.find(buildVehicleQuery({ city, fuel }));
      const duplicate = existingDocs.find((doc) => matchesVehicleFilters(doc, { make, model, variant, city, fuel }));

      if (duplicate) {
        await Vehicle.findByIdAndUpdate(duplicate._id, payload, { new: true });
        results.updated++;
      } else {
        await Vehicle.create(payload);
        results.inserted++;
      }
    } catch (error) {
      results.errors.push({ item, error: error.message });
    }
  }

  res.json({ success: true, data: results });
});

const getUniqueMakes = asyncHandler(async (_req, res) => {
  const [makeValues, brandValues] = await Promise.all([
    Vehicle.distinct('make', { make: { $exists: true, $ne: null } }),
    Vehicle.distinct('brand', { brand: { $exists: true, $ne: null } }),
  ]);

  const makes = [...new Set([...makeValues, ...brandValues].map((value) => String(value || '').trim()).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b));

  res.json({ success: true, data: makes });
});

const getUniqueModels = asyncHandler(async (req, res) => {
  const { make } = req.query;

  if (!make) {
    res.status(400);
    throw new Error('Make parameter is required');
  }

  const docs = await findDocsForMake(make);
  const models = [...new Set(
    docs.map((doc) => normalizeVehicleRecord(doc).model).filter(Boolean),
  )].sort((a, b) => a.localeCompare(b));

  res.json({ success: true, data: models });
});

const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model } = req.query;

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and Model parameters are required');
  }

  const docs = await findDocsForMake(make);
  const variants = [...new Set(
    docs
      .map((doc) => normalizeVehicleRecord(doc))
      .filter((doc) => matchesExact(doc.model, model))
      .map((doc) => doc.variant)
      .filter(Boolean),
  )].sort((a, b) => a.localeCompare(b));

  res.json({ success: true, data: variants });
});



const getVariantOptionsByModel = asyncHandler(async (req, res) => {
  const { make, model, city } = req.query;

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and model are required');
  }

  const docs = await findDocsForMake(make);
  const variants = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .filter((doc) => matchesExact(doc.model, model))
    .sort((a, b) => {
      if (city) {
        const aCity = matchesExact(a.city, city) ? 1 : 0;
        const bCity = matchesExact(b.city, city) ? 1 : 0;
        if (aCity !== bCity) return bCity - aCity;
      }
      return (a.onRoadPrice || 0) - (b.onRoadPrice || 0);
    });

  const byVariant = new Map();
  variants.forEach((doc) => {
    const key = normalizeText(doc.variant);
    if (!key || byVariant.has(key)) return;
    byVariant.set(key, {
      _id: doc._id,
      id: doc._id,
      make: doc.make,
      model: doc.model,
      variant: doc.variant,
      city: doc.city,
      fuel: doc.fuel,
      exShowroom: doc.exShowroom,
      onRoadPrice: doc.onRoadPrice || Number(doc.total_on_road_with_accessories || doc.on_road_price_cardekho || 0),
      insurance: doc.insurance,
      rto: doc.rto,
      tcs: Number(doc.tcs || doc.other_tcsCharges || doc.otherCharges || 0),
      ...doc,
    });
  });

  res.json({ success: true, data: Array.from(byVariant.values()) });
});

const getVehicleByDetails = asyncHandler(async (req, res) => {
  const { make, model, variant, fuel, city } = req.query;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error('Make, model and variant are required');
  }

  const docs = await findDocsForMake(make);
  const match = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .find((doc) =>
      matchesExact(doc.make, make) &&
      matchesExact(doc.model, model) &&
      matchesExact(doc.variant, variant) &&
      matchesExact(doc.city, city) &&
      matchesExact(doc.fuel, fuel),
    ) ||
    docs
      .map((doc) => normalizeVehicleRecord(doc))
      .find((doc) =>
        matchesExact(doc.make, make) &&
        matchesExact(doc.model, model) &&
        matchesExact(doc.variant, variant),
      );

  if (!match) {
    res.status(404);
    throw new Error('Vehicle not found');
  }

  res.json({ success: true, data: match });
});

const getVehicleMedia = asyncHandler(async (req, res) => {
  const { make, model, variant } = req.query;

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and model are required');
  }

  const collection = mongoose.connection.db.collection('vehicle_colors');
  const docs = await collection.find({ brand: buildMakeRegex(make) }).toArray();

  const rows = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .filter((doc) => matchesExact(doc.make, make) && matchesExact(doc.model, model) && matchesExact(doc.variant, variant))
    .sort((a, b) => String(a.color_name || '').localeCompare(String(b.color_name || '')));

  const fallbackRows = rows.length
    ? rows
    : docs
        .map((doc) => normalizeVehicleRecord(doc))
        .filter((doc) => matchesExact(doc.make, make) && matchesExact(doc.model, model))
        .sort((a, b) => String(a.color_name || '').localeCompare(String(b.color_name || '')));

  res.json({ success: true, data: fallbackRows });
});

export {
  getVehicles,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
  bulkUploadVehicles,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  getVariantOptionsByModel,
  getVehicleByDetails,
  getVehicleMedia,
};
