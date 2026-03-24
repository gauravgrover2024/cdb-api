import asyncHandler from 'express-async-handler';
import mongoose from 'mongoose';
import Vehicle from '../models/Vehicle.js';
import VehicleRecord from '../models/VehicleRecord.js';

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
  other_totalOtherChargesInRsFormat: 1,
  other_tcsCharges: 1,
  other_otherCharges: 1,
  other_mcdCharges: 1,
  other_numberPlateCharges: 1,
  other_smartCardcharges: 1,
  other_list: 1,
  optional_totalAccessories: 1,
  optional_totalAccessoriesInRs: 1,
  optional_total: 1,
  optional_accessoriesCharges: 1,
  optional_extendedWarrantyCharges: 1,
  optional_zeroDepInsuranceCharges: 1,
  optional_amcCharges: 1,
  optional_miscellaneouscharges: 1,
  optional_list: 1,
  orp_without_accessories: 1,
  ex_showroom_price_cardekho: 1,
  insurance_amount_cardekho: 1,
  rto_amount_cardekho: 1,
  onRoadPrice: 1,
  on_road_price_cardekho: 1,
  total_on_road_with_accessories: 1,
  LastSeenDate: 1,
  LastPriceChangeDate: 1,
  IsDiscontinued: 1,
  status: 1,
  is_discontinued: 1,
  isDiscontinued: 1,
  discontinued_date: 1,
  discontinuedDate: 1,
  image_url: 1,
  imageUrl: 1,
  color_name: 1,
  color_hex: 1,
  hex: 1,
  createdAt: 1,
  updatedAt: 1,
};

const parseAmount = (value) => {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (typeof value === 'string') {
    const cleaned = value.replace(/[^0-9.-]/g, '');
    if (!cleaned) return 0;
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
};

const toVehicleListItem = (doc) => {
  const normalized = normalizeVehicleRecord(doc);
  const { rawVariant, rawModel, ...normalizedWithoutRaw } = normalized;
  const discontinued = isVehicleDiscontinued(normalized);
  const tcs = parseAmount(
    normalized.tcs ?? normalized.other_tcsCharges ?? normalized.otherCharges ?? 0,
  );
  const rto = parseAmount(normalized.rto ?? normalized.roadTax ?? 0);

  return {
    ...normalizedWithoutRaw,
    _id: normalized._id,
    make: normalized.make,
    brand: normalized.brand,
    model: normalized.model,
    variant: normalized.variant,
    city: normalized.city,
    fuel: normalized.fuel,
    fuel_type: normalized.fuel_type,
    exShowroom: normalized.exShowroom,
    ex_showroom: parseAmount(
      normalized.ex_showroom ?? normalized.exShowroom ?? 0,
    ),
    rto,
    roadTax: rto,
    insurance: normalized.insurance,
    otherCharges: normalized.otherCharges,
    tcs,
    other_tcsCharges: tcs,
    onRoadPrice: normalized.onRoadPrice,
    on_road_price_cardekho: parseAmount(
      normalized.on_road_price_cardekho ?? normalized.onRoadPrice ?? 0,
    ),
    total_on_road_with_accessories: parseAmount(
      normalized.total_on_road_with_accessories ?? normalized.onRoadPrice ?? 0,
    ),
    status: normalized.status,
    is_discontinued: discontinued,
    isDiscontinued: discontinued,
    discontinued_date: normalized.discontinued_date ?? null,
    discontinuedDate: normalized.discontinuedDate ?? null,
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
const normalizeRegNo = (value) =>
  String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');

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

const slugTokens = (value) =>
  String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .split('-')
    .filter(Boolean);

const mediaUrlMatchesMakeModel = (url, make, model) => {
  const raw = String(url || '').trim().toLowerCase();
  if (!raw) return false;

  const makeParts = slugTokens(make);
  const modelParts = slugTokens(model);
  if (!makeParts.length || !modelParts.length) return false;

  const normalized = raw.replace(/[^a-z0-9]+/g, '-');
  const hasMake = makeParts.some(
    (part) =>
      normalized.includes(`-${part}-`) ||
      normalized.endsWith(`-${part}`) ||
      normalized.startsWith(`${part}-`),
  );
  const hasModel = modelParts.some(
    (part) =>
      normalized.includes(`-${part}-`) ||
      normalized.endsWith(`-${part}`) ||
      normalized.startsWith(`${part}-`),
  );

  return hasMake && hasModel;
};

const normalizeHex = (value) => String(value || '').trim().replace(/^#/, '').toLowerCase();

const parseTimestampValue = (value) => {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
};

const rowLatestTimestamp = (row) =>
  Math.max(
    parseTimestampValue(row?.scrape_timestamp),
    parseTimestampValue(row?.updatedAt),
    parseTimestampValue(row?.last_updated),
  );

const dedupeMediaRowsByHexLatest = (rows = []) => {
  const byHex = new Map();
  const withoutHex = [];

  rows.forEach((row) => {
    const hex = normalizeHex(row?.hex || row?.color_hex || row?.colour_hex || '');
    if (!hex) {
      withoutHex.push(row);
      return;
    }

    const existing = byHex.get(hex);
    if (!existing) {
      byHex.set(hex, row);
      return;
    }

    const existingTs = rowLatestTimestamp(existing);
    const candidateTs = rowLatestTimestamp(row);
    if (candidateTs >= existingTs) {
      byHex.set(hex, row);
    }
  });

  return [...withoutHex, ...byHex.values()].sort((a, b) =>
    String(a?.color_name || '').localeCompare(String(b?.color_name || '')),
  );
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
    exShowroom: parseAmount(raw.exShowroom ?? raw.ex_showroom ?? 0),
    onRoadPrice: parseAmount(
      raw.onRoadPrice ??
        raw.on_road_price_cardekho ??
        raw.total_on_road_with_accessories ??
        0,
    ),
    insurance: parseAmount(raw.insurance ?? 0),
    rto: parseAmount(raw.rto ?? raw.rto_amount_cardekho ?? 0),
    otherCharges: parseAmount(
      raw.otherCharges ??
        raw.other_totalOtherCharges ??
        raw.other_tcsCharges ??
        0,
    ),
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

const hasDiscontinuedDate = (value) => {
  if (value === undefined || value === null) return false;
  const raw = String(value).trim();
  if (!raw) return false;
  return raw.toLowerCase() !== 'null';
};

const isVehicleDiscontinued = (vehicle) =>
  parseBoolean(vehicle?.is_discontinued ?? vehicle?.isDiscontinued) ||
  hasDiscontinuedDate(vehicle?.discontinued_date ?? vehicle?.discontinuedDate);

const ACTIVE_VARIANT_FILTER = {
  $and: [
    {
      // Keep this branch cast-safe for the schema-typed boolean field.
      $or: [
        { is_discontinued: { $exists: false } },
        { is_discontinued: false },
        { is_discontinued: 0 },
        { is_discontinued: null },
      ],
    },
    {
      $nor: [
        { isDiscontinued: true },
        { isDiscontinued: 1 },
        { isDiscontinued: 'true' },
        { isDiscontinued: 'True' },
        { discontinued_date: { $exists: true, $nin: [null, '', 'null', 'NULL'] } },
        { discontinuedDate: { $exists: true, $nin: [null, '', 'null', 'NULL'] } },
      ],
    },
  ],
};

const DISTINCT_CACHE_TTL_MS = 5 * 60 * 1000;
const DISTINCT_CACHE = new Map();

const VEHICLE_LIST_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes cache
const VEHICLE_LIST_CACHE = new Map();

const getCacheKey = (prefix, params = {}) =>
  JSON.stringify({
    prefix,
    ...params,
  });

const readCache = (cacheMap, ttl, prefix, params = {}) => {
  const key = getCacheKey(prefix, params);
  const entry = cacheMap.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > ttl) {
    cacheMap.delete(key);
    return null;
  }
  return entry.data;
};

const writeCache = (cacheMap, prefix, params = {}, data = []) => {
  const key = getCacheKey(prefix, params);
  cacheMap.set(key, { ts: Date.now(), data });
};

const readDistinctCache = (scope, params = {}) => {
  return readCache(DISTINCT_CACHE, DISTINCT_CACHE_TTL_MS, scope, params);
};

const writeDistinctCache = (scope, params = {}, data = []) => {
  writeCache(DISTINCT_CACHE, scope, params, data);
};

const getVehicles = asyncHandler(async (req, res) => {
  const { q, make, model, variant, city, fuel } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : null;
  const skip = Number(req.query.skip) || 0;
  const includeFullPayload =
    String(req.query.full || '').toLowerCase() === 'true' || String(req.query.full || '') === '1';

  const cacheParams = { q, make, model, variant, city, fuel, pageSize, skip, includeFullPayload };
  const cached = readCache(VEHICLE_LIST_CACHE, VEHICLE_LIST_CACHE_TTL_MS, 'list', cacheParams);
  if (cached) {
    return res.json({ ...cached, fromCache: true });
  }

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
    
  const response = { success: true, count: count ?? data.length, data };
  writeCache(VEHICLE_LIST_CACHE, 'list', cacheParams, response);

  res.json(response);
});

const searchVehicleRecords = asyncHandler(async (req, res) => {
  const rawQ = String(req.query.q || req.query.search || '').trim();
  const q = normalizeRegNo(rawQ);
  const isFourDigitSuffixSearch = /^\d{4}$/.test(q);
  const requestedLimit = Number(req.query.limit);
  const defaultLimit = isFourDigitSuffixSearch ? 5000 : 20;
  const limit = Math.min(
    Math.max(Number.isFinite(requestedLimit) ? requestedLimit : defaultLimit, 1),
    10000,
  );

  if (q.length < 2) {
    return res.json({ success: true, count: 0, data: [] });
  }

  const escaped = escapeRegex(q);
  const suffix = q.slice(-4);
  const clauses = isFourDigitSuffixSearch
    ? [
        { registrationNumberLast4: suffix },
        { registrationNumberNormalized: new RegExp(`${escaped}$`, 'i') },
      ]
    : [
        { registrationNumberNormalized: new RegExp(`^${escaped}`, 'i') },
        { registrationNumberNormalized: new RegExp(escaped, 'i') },
      ];

  const fetchLimit = isFourDigitSuffixSearch ? limit : Math.max(limit * 4, 40);

  const rows = await VehicleRecord.find({ $or: clauses })
    .select({
      registrationNumber: 1,
      registrationNumberNormalized: 1,
      registrationNumberLast4: 1,
      make: 1,
      model: 1,
      variant: 1,
      yearOfManufacture: 1,
      manufactureMonth: 1,
      engineNumber: 1,
      chassisNumber: 1,
      registrationDate: 1,
      registrationCity: 1,
      hypothecation: 1,
      cubicCapacityCc: 1,
      updatedAt: 1,
      createdAt: 1,
    })
    .sort({ updatedAt: -1, createdAt: -1 })
    .limit(fetchLimit)
    .lean();

  const scored = rows
    .map((row) => {
      const normalized = normalizeRegNo(
        row?.registrationNumberNormalized || row?.registrationNumber,
      );
      if (!normalized) return null;

      let score = 0;
      if (normalized === q) score += 150;
      if (normalized.startsWith(q)) score += 110;
      if (normalized.includes(q)) score += 50;
      if (isFourDigitSuffixSearch && row?.registrationNumberLast4 === suffix) score += 220;
      if (isFourDigitSuffixSearch && normalized.endsWith(suffix)) score += 170;
      if (!isFourDigitSuffixSearch && suffix.length === 4 && row?.registrationNumberLast4 === suffix) score += 80;
      if (!isFourDigitSuffixSearch && suffix.length === 4 && normalized.endsWith(suffix)) score += 40;
      if (!score) return null;

      return {
        ...row,
        registrationNumber:
          String(row?.registrationNumber || '').trim() || normalized,
        registrationNumberNormalized: normalized,
        score,
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const aTs = Date.parse(a.updatedAt || a.createdAt || '') || 0;
      const bTs = Date.parse(b.updatedAt || b.createdAt || '') || 0;
      return bTs - aTs;
    });

  const deduped = [];
  const seen = new Set();
  for (const row of scored) {
    const key = row.registrationNumberNormalized;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push({
      _id: row._id,
      registrationNumber: row.registrationNumber,
      registrationNumberNormalized: row.registrationNumberNormalized,
      make: row.make || '',
      model: row.model || '',
      variant: row.variant || '',
      yearOfManufacture: row.yearOfManufacture || '',
      manufactureMonth: row.manufactureMonth || '',
      engineNumber: row.engineNumber || '',
      chassisNumber: row.chassisNumber || '',
      registrationDate: row.registrationDate || null,
      registrationCity: row.registrationCity || '',
      hypothecation: row.hypothecation || '',
      cubicCapacityCc: row.cubicCapacityCc,
    });
    if (deduped.length >= limit) break;
  }

  res.json({ success: true, count: deduped.length, data: deduped });
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
  const cached = readDistinctCache('makes', { city, includeDiscontinued });
  if (cached) {
    return res.json({ success: true, data: cached, cached: true });
  }
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
  writeDistinctCache('makes', { city, includeDiscontinued }, makes);

  res.json({ success: true, data: makes });
});

const getUniqueModels = asyncHandler(async (req, res) => {
  const { make, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make) {
    res.status(400);
    throw new Error('Make parameter is required');
  }
  const cached = readDistinctCache('models', { make, city, includeDiscontinued });
  if (cached) {
    return res.json({ success: true, data: cached, cached: true });
  }

  const query = buildVehicleQuery({ make, city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);
  const rawModels = await Vehicle.distinct('model', {
    ...query,
    model: { $exists: true, $ne: null },
  });
  const models = [
    ...new Set(
      rawModels
        .map((value) => String(value || '').trim())
        .map((value) => trimLeading(value, make) || value)
        .filter(Boolean),
    ),
  ].sort((a, b) => a.localeCompare(b));
  writeDistinctCache('models', { make, city, includeDiscontinued }, models);

  res.json({ success: true, data: models });
});

const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and Model parameters are required');
  }
  const cached = readDistinctCache('variants', {
    make,
    model,
    city,
    includeDiscontinued,
  });
  if (cached) {
    return res.json({ success: true, data: cached, cached: true });
  }

  const query = buildVehicleQuery({ make, model, city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);
  const rawVariants = await Vehicle.distinct('variant', {
    ...query,
    variant: { $exists: true, $ne: null },
  });
  const variants = [
    ...new Set(
      rawVariants
        .map((value) => String(value || '').trim())
        .map((value) =>
          trimLeading(value, `${make} ${model}`.trim()) ||
          trimLeading(value, model) ||
          trimLeading(value, make) ||
          value,
        )
        .filter(Boolean),
    ),
  ].sort((a, b) => a.localeCompare(b));
  writeDistinctCache('variants', { make, model, city, includeDiscontinued }, variants);

  res.json({ success: true, data: variants });
});

const getVariantOptionsByModel = asyncHandler(async (req, res) => {
  const { make, model, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make || !model) {
    res.status(400);
    throw new Error('Make and model are required');
  }

  const cacheParams = { make, model, city, includeDiscontinued };
  const cached = readCache(VEHICLE_LIST_CACHE, VEHICLE_LIST_CACHE_TTL_MS, 'variants-options', cacheParams);
  if (cached) {
    return res.json({ ...cached, fromCache: true });
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
        doc.onRoadPrice ||
        parseAmount(
          doc.total_on_road_with_accessories || doc.on_road_price_cardekho || 0,
        ),
      insurance: doc.insurance,
      rto: doc.rto,
      tcs: parseAmount(doc.tcs || doc.other_tcsCharges || doc.otherCharges || 0),
      ...doc,
    });
  });

  const response = { success: true, data: Array.from(byVariant.values()) };
  writeCache(VEHICLE_LIST_CACHE, 'variants-options', cacheParams, response);

  res.json(response);
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
    .filter((doc) => {
      const imageUrl = doc.image_url || doc.imageUrl || '';
      if (!imageUrl) return true;
      return mediaUrlMatchesMakeModel(imageUrl, make, model);
    })
    .sort((a, b) => String(a.color_name || '').localeCompare(String(b.color_name || '')));

  const fallbackRows = rows.length
    ? rows
    : docs
        .map((doc) => normalizeVehicleRecord(doc))
        .filter((doc) => matchesExact(doc.make, make) && matchesExact(doc.model, model))
        .filter((doc) => {
          const imageUrl = doc.image_url || doc.imageUrl || '';
          if (!imageUrl) return true;
          return mediaUrlMatchesMakeModel(imageUrl, make, model);
        })
        .sort((a, b) => String(a.color_name || '').localeCompare(String(b.color_name || '')));

  res.json({ success: true, data: dedupeMediaRowsByHexLatest(fallbackRows) });
});

export {
  getVehicles,
  searchVehicleRecords,
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
