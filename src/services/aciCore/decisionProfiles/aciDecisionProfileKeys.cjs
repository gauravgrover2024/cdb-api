const normalizeText = (value) => String(value ?? '').trim();

const slugKey = (value) =>
  normalizeText(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const getPath = (obj, path) => {
  if (!obj || !path) return undefined;
  return String(path)
    .split('.')
    .reduce((acc, key) => (acc && Object.prototype.hasOwnProperty.call(acc, key) ? acc[key] : undefined), obj);
};

const getFirst = (obj, paths) => {
  for (const path of paths) {
    const value = getPath(obj, path);
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return undefined;
};

const toNumber = (value) => {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const cleaned = String(value).replace(/,/g, '').match(/-?\d+(\.\d+)?/);
  return cleaned ? Number(cleaned[0]) : null;
};

const normalizeFuelKey = (value) => {
  const text = slugKey(value);
  if (!text) return null;
  if (text.includes('petrol')) return 'petrol';
  if (text.includes('diesel')) return 'diesel';
  if (text.includes('cng')) return 'cng';
  if (text.includes('electric') || text === 'ev') return 'electric';
  if (text.includes('hybrid')) return 'hybrid';
  return text;
};

const normalizeTransmissionKey = (value) => {
  const text = slugKey(value);
  if (!text) return null;
  if (text.includes('manual') || text === 'mt') return 'manual';
  if (
    text.includes('automatic') ||
    text === 'at' ||
    text.includes('cvt') ||
    text.includes('dct') ||
    text.includes('amt') ||
    text.includes('ivt') ||
    text.includes('torque_converter')
  ) {
    return 'automatic';
  }
  return text;
};

const buildFuelTransmissionFamilyKey = ({ fuel, fuelKey, transmission, transmissionKey, gearbox }) => {
  const normalizedFuel = fuelKey || normalizeFuelKey(fuel);
  const normalizedTransmission = transmissionKey || normalizeTransmissionKey(transmission || gearbox);
  if (!normalizedFuel && !normalizedTransmission) return null;
  return `${normalizedFuel || 'unknown_fuel'}_${normalizedTransmission || 'unknown_transmission'}`;
};

const makeBrandModelKey = ({ make, makeKey, model, modelKey }) => {
  const mk = makeKey || slugKey(make);
  const md = modelKey || slugKey(model);
  return mk && md ? `${mk}_${md}` : null;
};

const makeVariantLookupKey = (record) => {
  const make = getFirst(record, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']);
  const makeKey = getFirst(record, ['makeKey', 'brandKey']);
  const model = getFirst(record, ['model', 'modelName', 'fullModel', 'nameplate']);
  const modelKey = getFirst(record, ['modelKey']);
  const variant = getFirst(record, ['variant', 'variantName', 'variantLabel', 'trim', 'version']);
  const variantKey = getFirst(record, ['variantKey']);

  const fuel = getFirst(record, ['fuel', 'fuelType', 'fuel_type']);
  const fuelKey = getFirst(record, ['fuelKey']);
  const transmission = getFirst(record, ['transmission', 'transmissionType', 'gearbox']);
  const transmissionKey = getFirst(record, ['transmissionKey']);

  const brandModelKey = makeBrandModelKey({ make, makeKey, model, modelKey });
  const normalizedVariantKey = variantKey || slugKey(variant);
  const fuelTransmissionFamilyKey = buildFuelTransmissionFamilyKey({
    fuel,
    fuelKey,
    transmission,
    transmissionKey,
  });

  if (!brandModelKey || !normalizedVariantKey) return null;
  return `${brandModelKey}__${normalizedVariantKey}__${fuelTransmissionFamilyKey || 'unknown_powertrain'}`;
};

const makeVariantLooseLookupKey = (record) => {
  const make = getFirst(record, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']);
  const makeKey = getFirst(record, ['makeKey', 'brandKey']);
  const model = getFirst(record, ['model', 'modelName', 'fullModel', 'nameplate']);
  const modelKey = getFirst(record, ['modelKey']);
  const variant = getFirst(record, ['variant', 'variantName', 'variantLabel', 'trim', 'version']);
  const variantKey = getFirst(record, ['variantKey']);

  const brandModelKey = makeBrandModelKey({ make, makeKey, model, modelKey });
  const normalizedVariantKey = variantKey || slugKey(variant);

  if (!brandModelKey || !normalizedVariantKey) return null;
  return `${brandModelKey}__${normalizedVariantKey}`;
};

module.exports = {
  normalizeText,
  slugKey,
  getFirst,
  toNumber,
  normalizeFuelKey,
  normalizeTransmissionKey,
  buildFuelTransmissionFamilyKey,
  makeBrandModelKey,
  makeVariantLookupKey,
  makeVariantLooseLookupKey,
};
