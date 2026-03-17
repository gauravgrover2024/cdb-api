import asyncHandler from 'express-async-handler';
import mongoose from 'mongoose';
import Vehicle from '../models/Vehicle.js';

const VEHICLE_LIST_PROJECTION = {
  make: 1,
  brand: 1,
  model: 1,
  variant: 1,
  fuel: 1,
  fuel_type: 1,
  city: 1,
  exShowroom: 1,
  ex_showroom: 1,
  rto: 1,
  insurance: 1,
  otherCharges: 1,
  other_totalOtherCharges: 1,
  onRoadPrice: 1,
  on_road_price_cardekho: 1,
  total_on_road_with_accessories: 1,
  other_tcsCharges: 1,
  status: 1,
  is_discontinued: 1,
  isDiscontinued: 1,
  image_url: 1,
  imageUrl: 1,
  color_name: 1,
  color_hex: 1,
  hex: 1,
  createdAt: 1,
  updatedAt: 1,
};

const toVehicleListItem = (doc) => {
  const normalized = normalizeVehicleRecord(doc);
  const tcs = Number(
    normalized.tcs ?? normalized.other_tcsCharges ?? normalized.otherCharges ?? 0,
  );
  const rto = Number(normalized.rto ?? normalized.roadTax ?? 0);

  return {
    _id: normalized._id,
    make: normalized.make,
    brand: normalized.brand,
    model: normalized.model,
    variant: normalized.variant,
    city: normalized.city,
    fuel: normalized.fuel,
    fuel_type: normalized.fuel_type,
    exShowroom: normalized.exShowroom,
    ex_showroom: Number(normalized.ex_showroom ?? normalized.exShowroom ?? 0),
    rto,
    roadTax: rto,
    insurance: normalized.insurance,
    otherCharges: normalized.otherCharges,
    tcs,
    other_tcsCharges: tcs,
    onRoadPrice: normalized.onRoadPrice,
    on_road_price_cardekho: Number(normalized.on_road_price_cardekho ?? normalized.onRoadPrice ?? 0),
    total_on_road_with_accessories: Number(
      normalized.total_on_road_with_accessories ?? normalized.onRoadPrice ?? 0,
    ),
    status: normalized.status,
    is_discontinued: Boolean(normalized.is_discontinued ?? normalized.isDiscontinued),
    isDiscontinued: Boolean(normalized.isDiscontinued ?? normalized.is_discontinued),
    image_url: normalized.image_url || normalized.imageUrl || '',
    imageUrl: normalized.imageUrl || normalized.image_url || '',
    color_name: normalized.color_name || '',
    color_hex: normalized.color_hex || normalized.hex || '',
    hex: normalized.hex || normalized.color_hex || '',
    createdAt: normalized.createdAt,
    updatedAt: normalized.updatedAt,
  };
};

const normalizeText = (value) => String(value || '').trim().toLowerCase();

const canonicalizeMake = (value) => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[-_]+/g, ' ')
    .replace(/\s+/g, ' ');
  const aliases = {
    mercedes: 'mercedes benz',
    'mercedes benz': 'mercedes benz',
    benz: 'mercedes benz',
    maruti: 'maruti suzuki',
    'maruti suzuki': 'maruti suzuki',
  };
  return aliases[normalized] || normalized;
};

const escapeRegex = (value) => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const trimLeading = (value, prefix) => {
  const source = String(value || '').trim();
  const leader = String(prefix || '').trim();
  if (!source || !leader) return source;
  const escaped = escapeRegex(leader);
  return source.replace(new RegExp(`^${escaped}\\s*`, 'i'), '').trim();
};

const toCityToken = (value) => String(value || '').trim().toLowerCase().replace(/\s+/g, '-');

const buildCityCandidates = (city) => {
  const token = toCityToken(city);
  if (!token) return [];

  const aliases = {
    delhi: ['new-delhi'],
    'new-delhi': ['delhi'],
    gurugram: ['gurgaon'],
    gurgaon: ['gurugram'],
  };

  return [...new Set([token, ...(aliases[token] || [])])];
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
  return (
    canonicalizeMake(actual) === canonicalizeMake(expected) ||
    normalizeText(actual).replace(/[-_]+/g, ' ').replace(/\s+/g, ' ') ===
      normalizeText(expected).replace(/[-_]+/g, ' ').replace(/\s+/g, ' ')
  );
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

const buildMakeMatch = (make) => {
  const value = String(make || '').trim();
  const normalized = canonicalizeMake(value);
  const candidates = [
    ...new Set(
      [value, normalized, normalized.replace(/ /g, '-'), normalized.replace(/ /g, '')].filter(Boolean),
    ),
  ];
  return { $or: [{ make: { $in: candidates } }, { brand: { $in: candidates } }] };
};

const buildModelCandidates = (make, model) => {
  const makeValue = String(make || '').trim();
  const modelValue = String(model || '').trim();
  return [...new Set([modelValue, `${makeValue} ${modelValue}`.trim()].filter(Boolean))];
};

const buildVariantCandidates = (make, model, variant) => {
  const makeValue = String(make || '').trim();
  const modelValue = String(model || '').trim();
  const variantValue = String(variant || '').trim();
  return [
    ...new Set(
      [
        variantValue,
        `${makeValue} ${variantValue}`.trim(),
        `${modelValue} ${variantValue}`.trim(),
        `${makeValue} ${modelValue} ${variantValue}`.trim(),
      ].filter(Boolean),
    ),
  ];
};

const mergeAndCondition = (query, condition) => {
  if (!condition || typeof condition !== 'object' || !Object.keys(condition).length) return;
  query.$and = [...(query.$and || []), condition];
};

const buildVehicleQuery = ({ q, make, model, variant, city, fuel }) => {
  const query = {};

  if (q) {
    mergeAndCondition(query, {
      $or: [
        { make: new RegExp(q, 'i') },
        { brand: new RegExp(q, 'i') },
        { model: new RegExp(q, 'i') },
        { variant: new RegExp(q, 'i') },
      ],
    });
  }

  if (make) mergeAndCondition(query, buildMakeMatch(make));
  if (model) query.model = { $in: buildModelCandidates(make, model) };
  if (variant) query.variant = { $in: buildVariantCandidates(make, model, variant) };

  if (city) {
    const cityCandidates = buildCityCandidates(city);
    if (cityCandidates.length === 1) query.city = cityCandidates[0];
    else if (cityCandidates.length > 1) query.city = { $in: cityCandidates };
  }

  if (fuel) {
    const fuelRegex = new RegExp(`^${escapeRegex(String(fuel).trim())}$`, 'i');
    mergeAndCondition(query, {
      $or: [{ fuel: fuelRegex }, { fuel_type: fuelRegex }],
    });
  }

  return query;
};

const buildMakeRegex = (make) => new RegExp(`^${escapeRegex(String(make || '').trim())}$`, 'i');

const parseBoolean = (value) => {
  const raw = String(value ?? '').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes';
};

const ACTIVE_VARIANT_FILTER = {
  $nor: [
    { is_discontinued: true },
    { is_discontinued: 1 },
    { is_discontinued: 'true' },
    { is_discontinued: 'True' },
    { isDiscontinued: true },
    { isDiscontinued: 1 },
    { isDiscontinued: 'true' },
    { isDiscontinued: 'True' },
  ],
};

const getVehicles = asyncHandler(async (req, res) => {
  const { q, make, model, variant, city, fuel } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : null;
  const skip = Number(req.query.skip) || 0;
  const includeFullPayload =
    String(req.query.full || '').toLowerCase() === 'true' || String(req.query.full || '') === '1';

  const query = buildVehicleQuery({ q, make, model, variant, city, fuel });
  const cursor = Vehicle.find(query).sort({ make: 1, model: 1, variant: 1 });

  if (!includeFullPayload) cursor.select(VEHICLE_LIST_PROJECTION);
  if (skip > 0) cursor.skip(skip);
  if (pageSize) cursor.limit(pageSize);

  const shouldCountSeparately = Boolean(pageSize || skip > 0);
  const [docs, count] = shouldCountSeparately
    ? await Promise.all([cursor.lean(), Vehicle.countDocuments(query)])
    : [await cursor.lean(), null];

  const data = includeFullPayload
    ? docs.map(normalizeVehicleRecord)
    : docs.map(toVehicleListItem);
  res.json({ success: true, count: count ?? data.length, data });
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
  const existingDocs = await Vehicle.find(buildVehicleQuery({ make, model, variant, city, fuel }))
    .select({ make: 1, brand: 1, model: 1, variant: 1, fuel: 1, fuel_type: 1, city: 1 })
    .lean();
  const duplicate = existingDocs.find((doc) =>
    matchesVehicleFilters(doc, { make, model, variant, city, fuel }),
  );
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
    Object.assign(vehicle, {
      ...req.body,
      make: nextMake,
      brand: req.body.brand || vehicle.brand || nextMake,
    });
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
      const existingDocs = await Vehicle.find(buildVehicleQuery({ make, model, variant, city, fuel }))
        .select({ make: 1, brand: 1, model: 1, variant: 1, fuel: 1, fuel_type: 1, city: 1 })
        .lean();
      const duplicate = existingDocs.find((doc) =>
        matchesVehicleFilters(doc, { make, model, variant, city, fuel }),
      );

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

const getUniqueMakes = asyncHandler(async (req, res) => {
  const { city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);
  const cityQuery = city ? buildVehicleQuery({ city }) : {};
  const baseQuery = includeDiscontinued
    ? cityQuery
    : { ...cityQuery, ...ACTIVE_VARIANT_FILTER };

  const [makeValues, brandValues] = await Promise.all([
    Vehicle.distinct('make', { ...baseQuery, make: { $exists: true, $ne: null } }),
    Vehicle.distinct('brand', { ...baseQuery, brand: { $exists: true, $ne: null } }),
  ]);

  const makes = [
    ...new Set(
      [...makeValues, ...brandValues]
        .map((value) => String(value || '').trim())
        .filter(Boolean),
    ),
  ].sort((a, b) => a.localeCompare(b));

  res.json({ success: true, data: makes });
});

const getUniqueModels = asyncHandler(async (req, res) => {
  const { make, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make) {
    res.status(400);
    throw new Error('Make parameter is required');
  }

  const query = buildVehicleQuery({ make, city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);
  const docs = await Vehicle.find(query).select({ make: 1, brand: 1, model: 1 }).lean();
  const models = [...new Set(docs.map((doc) => normalizeVehicleRecord(doc).model).filter(Boolean))].sort(
    (a, b) => a.localeCompare(b),
  );

  res.json({ success: true, data: models });
});

const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and Model parameters are required');
  }

  const query = buildVehicleQuery({ make, model, city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);

  const docs = await Vehicle.find(query)
    .select({ make: 1, brand: 1, model: 1, variant: 1 })
    .lean();
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
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and model are required');
  }

  const baseQuery = buildVehicleQuery({ make, model });
  const cityQuery = city ? buildVehicleQuery({ make, model, city }) : null;
  if (!includeDiscontinued) {
    mergeAndCondition(baseQuery, ACTIVE_VARIANT_FILTER);
    if (cityQuery) mergeAndCondition(cityQuery, ACTIVE_VARIANT_FILTER);
  }

  const cityDocs = cityQuery ? await Vehicle.find(cityQuery).select(VEHICLE_LIST_PROJECTION).lean() : [];
  const docs = cityDocs.length
    ? cityDocs
    : await Vehicle.find(baseQuery).select(VEHICLE_LIST_PROJECTION).lean();

  const variants = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .sort((a, b) => (a.onRoadPrice || 0) - (b.onRoadPrice || 0));

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
      onRoadPrice:
        doc.onRoadPrice || Number(doc.total_on_road_with_accessories || doc.on_road_price_cardekho || 0),
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

  const baseQuery = buildVehicleQuery({ make, model, variant, fuel });
  const cityQuery = city ? buildVehicleQuery({ make, model, variant, fuel, city }) : null;

  const docsWithCity = cityQuery
    ? await Vehicle.find(cityQuery).select(VEHICLE_LIST_PROJECTION).lean()
    : [];
  const docs = docsWithCity.length
    ? docsWithCity
    : await Vehicle.find(baseQuery).select(VEHICLE_LIST_PROJECTION).lean();

  const match = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .find(
      (doc) =>
        matchesExact(doc.make, make) &&
        matchesExact(doc.model, model) &&
        matchesExact(doc.variant, variant) &&
        matchesExact(doc.fuel, fuel),
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
    .filter(
      (doc) =>
        matchesExact(doc.make, make) &&
        matchesExact(doc.model, model) &&
        matchesExact(doc.variant, variant),
    )
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
