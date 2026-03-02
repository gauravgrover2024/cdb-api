import asyncHandler from 'express-async-handler';
import mongoose from 'mongoose';
import Vehicle from '../models/Vehicle.js';

const normalizeText = (value) => String(value || '').trim().toLowerCase();

const trimLeading = (value, prefix) => {
  const source = String(value || '').trim();
  const leader = String(prefix || '').trim();
  if (!source || !leader) return source;
  const escaped = leader.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return source.replace(new RegExp(`^${escaped}\\s*`, 'i'), '').trim();
};

const normalizeVehicleRecord = (doc) => {
  const raw = doc?.toObject ? doc.toObject() : { ...(doc || {}) };
  const make = String(raw.make || raw.brand || '').trim();
  const rawModel = String(raw.model || '').trim();
  const rawVariant = String(raw.variant || '').trim();
  const model = trimLeading(rawModel, make) || rawModel;
  const variant =
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
    onRoadPrice: Number(raw.onRoadPrice ?? raw.on_road_price_cardekho ?? 0),
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

  if (city) query.city = new RegExp(`^${String(city).trim()}$`, 'i');
  if (fuel) {
    query.$or = [...(query.$or || []), { fuel: new RegExp(`^${String(fuel).trim()}$`, 'i') }, { fuel_type: new RegExp(`^${String(fuel).trim()}$`, 'i') }];
  }

  return query;
};

const sortVehicleRows = (rows) =>
  [...rows].sort((a, b) => {
    const aa = normalizeVehicleRecord(a);
    const bb = normalizeVehicleRecord(b);
    return [aa.make, aa.model, aa.variant].join('|').localeCompare([bb.make, bb.model, bb.variant].join('|'));
  });

// @desc    Get all vehicles with search and filtering
// @route   GET /api/vehicles
// @access  Public
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

// @desc    Get vehicle by ID
// @route   GET /api/vehicles/:id
// @access  Public
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

// @desc    Create a vehicle
// @route   POST /api/vehicles
// @access  Public
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

// @desc    Update a vehicle
// @route   PUT /api/vehicles/:id
// @access  Public
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

// @desc    Delete a vehicle
// @route   DELETE /api/vehicles/:id
// @access  Public
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

// @desc    Bulk create/update vehicles
// @route   POST /api/vehicles/bulk
// @access  Public
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

// @desc    Get unique makes from vehicle database
// @route   GET /api/vehicles/distinct/makes
// @access  Public
const getUniqueMakes = asyncHandler(async (req, res) => {
  const docs = await Vehicle.find({}).lean();
  const makes = [...new Set(docs.map((doc) => normalizeVehicleRecord(doc).make).filter(Boolean))].sort();
  res.json({ success: true, data: makes });
});

// @desc    Get unique models for a specific make
// @route   GET /api/vehicles/distinct/models?make=Toyota
// @access  Public
const getUniqueModels = asyncHandler(async (req, res) => {
  const { make } = req.query;

  if (!make) {
    res.status(400);
    throw new Error('Make parameter is required');
  }

  const docs = await Vehicle.find({}).lean();
  const models = [...new Set(
    docs
      .filter((doc) => matchesVehicleFilters(doc, { make }))
      .map((doc) => normalizeVehicleRecord(doc).model)
      .filter(Boolean),
  )].sort();

  res.json({ success: true, data: models });
});

// @desc    Get unique variants for a specific make and model
// @route   GET /api/vehicles/distinct/variants?make=Toyota&model=Corolla
// @access  Public
const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model } = req.query;

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and Model parameters are required');
  }

  const docs = await Vehicle.find({}).lean();
  const variants = [...new Set(
    docs
      .filter((doc) => matchesVehicleFilters(doc, { make, model }))
      .map((doc) => normalizeVehicleRecord(doc).variant)
      .filter(Boolean),
  )].sort();

  res.json({ success: true, data: variants });
});

// @desc    Get vehicle details by make, model, variant
// @route   GET /api/vehicles/by-details?make=Toyota&model=Corolla&variant=1.8E
// @access  Public
const getVehicleByDetails = asyncHandler(async (req, res) => {
  const { make, model, variant, fuel, city } = req.query;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error('Make, Model, and Variant parameters are required');
  }

  const docs = await Vehicle.find(buildVehicleQuery({ city, fuel })).lean();
  const vehicle = docs.find((doc) => matchesVehicleFilters(doc, { make, model, variant, city, fuel }));

  if (vehicle) {
    res.json({ success: true, data: normalizeVehicleRecord(vehicle) });
  } else {
    res.json({ success: false, data: null, message: 'Vehicle not found' });
  }
});

// @desc    Get vehicle media from vehicle_colors collection
// @route   GET /api/vehicles/media?make=Audi&model=Q8&variant=Quattro
// @access  Public
const getVehicleMedia = asyncHandler(async (req, res) => {
  const { make, model, variant } = req.query;

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and Model parameters are required');
  }

  const collection = mongoose.connection.db.collection('vehicle_colors');
  const rows = await collection.find({
    brand: new RegExp(`^${String(make).trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i'),
  }).toArray();

  const normalizedRows = rows.filter((row) => {
    const rowMake = normalizeText(row.brand || row.make);
    const rowModel = normalizeText(trimLeading(row.model || '', row.brand || row.make || ''));
    const rowVariant = normalizeText(
      trimLeading(row.variant || '', row.model || '') || trimLeading(row.variant || '', `${row.brand || row.make || ''} ${trimLeading(row.model || '', row.brand || row.make || '')}`.trim()),
    );

    if (rowMake !== normalizeText(make)) return false;
    if (rowModel !== normalizeText(model)) return false;
    if (!variant) return true;
    return rowVariant === normalizeText(variant);
  }).map((row) => ({
    ...row,
    make: row.make || row.brand,
    brand: row.brand || row.make,
    model: trimLeading(row.model || '', row.brand || row.make || '') || row.model,
    variant:
      trimLeading(row.variant || '', row.model || '') ||
      trimLeading(row.variant || '', `${row.brand || row.make || ''} ${trimLeading(row.model || '', row.brand || row.make || '')}`.trim()) ||
      row.variant,
  }));

  res.json({ success: true, count: normalizedRows.length, data: normalizedRows });
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
  getVehicleByDetails,
  getVehicleMedia,
};
