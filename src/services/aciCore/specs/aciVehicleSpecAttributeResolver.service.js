'use strict';

import mongoose from 'mongoose';

import {
  compactVehicleContext,
  mergeContextPatches,
} from '../context/aciContextManager.service.js';
import {
  composeResolvedSpecAnswer,
} from './aciResolvedTopicAnswerUx.service.js';

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const normalizeText = (value = '') =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const keyify = (value = '') => normalizeText(value);
const slugify = (value = '') => keyify(value).replace(/\s+/g, '-');
const escapeRegex = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [];
  return [value];
};

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && cleanText(value) !== '') || '';

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === 'object') return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== '';
    }),
  );

const SPEC_ATTRIBUTE_DEFINITIONS = [
  {
    key: 'range',
    label: 'range',
    aliases: ['range', 'driving range', 'claimed range', 'arai range'],
    fields: ['range', 'drivingRange', 'claimedRange', 'araiRange', 'evRange', 'batteryRange'],
  },
  {
    key: 'battery_capacity',
    label: 'battery capacity',
    aliases: ['battery', 'battery capacity', 'battery pack', 'battery size'],
    fields: ['batteryCapacity', 'battery', 'batteryPack', 'batterySize'],
  },
  {
    key: 'charging_time',
    label: 'charging time',
    aliases: ['charging', 'charging time', 'fast charging', 'charge time'],
    fields: ['chargingTime', 'fastChargingTime', 'dcChargingTime', 'acChargingTime'],
  },
  {
    key: 'boot_space',
    label: 'boot space',
    aliases: ['boot space', 'boot capacity', 'luggage space'],
    fields: ['bootSpace', 'bootCapacity', 'luggageSpace'],
  },
  {
    key: 'ground_clearance',
    label: 'ground clearance',
    aliases: ['ground clearance'],
    fields: ['groundClearance'],
  },
  {
    key: 'dimensions',
    label: 'dimensions',
    aliases: ['dimensions', 'length width height', 'size'],
    fields: ['dimensions'],
  },
  {
    key: 'length',
    label: 'length',
    aliases: ['length'],
    fields: ['length'],
  },
  {
    key: 'width',
    label: 'width',
    aliases: ['width'],
    fields: ['width'],
  },
  {
    key: 'height',
    label: 'height',
    aliases: ['height'],
    fields: ['height'],
  },
  {
    key: 'wheelbase',
    label: 'wheelbase',
    aliases: ['wheelbase'],
    fields: ['wheelbase'],
  },
  {
    key: 'mileage',
    label: 'mileage',
    aliases: ['mileage', 'fuel efficiency'],
    fields: ['mileage', 'fuelEfficiency', 'araiMileage'],
  },
  {
    key: 'engine_displacement',
    label: 'engine cc',
    aliases: ['engine cc', 'engine capacity', 'displacement', 'engine displacement', 'cc'],
    fields: ['engineDisplacement', 'displacement', 'engineCapacity', 'engineCc', 'cc'],
  },
  {
    key: 'power',
    label: 'power',
    aliases: ['power', 'bhp', 'max power', 'maximum power'],
    fields: ['power', 'maxPower', 'maximumPower', 'bhp'],
  },
  {
    key: 'torque',
    label: 'torque',
    aliases: ['torque', 'max torque', 'maximum torque'],
    fields: ['torque', 'maxTorque', 'maximumTorque'],
  },
  {
    key: 'tank_capacity',
    label: 'tank capacity',
    aliases: ['tank capacity', 'fuel tank', 'fuel tank capacity'],
    fields: ['tankCapacity', 'fuelTankCapacity'],
  },
  {
    key: 'seating_capacity',
    label: 'seating capacity',
    aliases: ['seating capacity', 'seats', 'seat capacity'],
    fields: ['seatingCapacity', 'seats'],
  },
];

const SPEC_ATTRIBUTE_BY_KEY = Object.fromEntries(
  SPEC_ATTRIBUTE_DEFINITIONS.map((definition) => [definition.key, definition]),
);

const hasAlias = (text = '', alias = '') =>
  new RegExp(`(^|\\s)${normalizeText(alias).replace(/\s+/g, '\\s+')}($|\\s)`, 'i').test(normalizeText(text));

function resolveSpecAttributeFromText({ message = '', features = [], topics = [] } = {}) {
  const text = [
    message,
    ...asArray(features),
    ...asArray(topics),
  ].join(' ');

  return SPEC_ATTRIBUTE_DEFINITIONS.find((definition) =>
    definition.aliases.some((alias) => hasAlias(text, alias)),
  ) || null;
}

const isSpecAttributeTopic = (value = '') =>
  Boolean(resolveSpecAttributeFromText({ message: value }));

const getDb = () =>
  mongoose.connection?.readyState === 1 && mongoose.connection?.db
    ? mongoose.connection.db
    : null;

const getNestedValue = (object = {}, path = '') => {
  const direct = object?.[path];
  if (direct !== undefined && direct !== null && direct !== '') return direct;

  return path.split('.').reduce((current, part) => {
    if (current === undefined || current === null) return undefined;
    return current[part];
  }, object);
};

const collectAttributeValues = ({ row = {}, definition = {} } = {}) => {
  const candidates = [];
  for (const field of definition.fields || []) {
    candidates.push([field, getNestedValue(row, field)]);
    candidates.push([`specs.${field}`, getNestedValue(row, `specs.${field}`)]);
    candidates.push([`specifications.${field}`, getNestedValue(row, `specifications.${field}`)]);
    candidates.push([`attributes.${field}`, getNestedValue(row, `attributes.${field}`)]);
  }

  return candidates
    .flatMap(([field, value]) => {
      if (Array.isArray(value)) return value.map((item) => [field, item]);
      return [[field, value]];
    })
    .map(([field, value]) => {
      if (value === undefined || value === null || value === '') return null;
      if (typeof value === 'object') {
        const displayValue = firstMeaningful(value.value, value.displayValue, value.text, value.label);
        const unit = firstMeaningful(value.unit, value.units);
        return displayValue
          ? compactObject({
              attributeKey: definition.key,
              attributeLabel: definition.label,
              field,
              value: unit ? `${displayValue} ${unit}` : String(displayValue),
              rawValue: value,
            })
          : null;
      }
      return compactObject({
        attributeKey: definition.key,
        attributeLabel: definition.label,
        field,
        value: String(value),
      });
    })
    .filter(Boolean);
};

const buildVehicleQuery = (anchor = {}) => {
  const make = firstMeaningful(anchor.make, anchor.brand);
  const model = anchor.model;
  const fullModel = firstMeaningful(anchor.fullModel, [make, model].filter(Boolean).join(' '));

  const ors = [];
  if (anchor.modelKey) ors.push({ modelKey: new RegExp(`^${anchor.modelKey}$`, 'i') });
  if (anchor.shortModelKey) ors.push({ shortModelKey: new RegExp(`^${anchor.shortModelKey}$`, 'i') });
  if (fullModel) ors.push({ fullModel: new RegExp(`^${escapeRegex(fullModel)}$`, 'i') });
  if (make && model) {
    ors.push({
      $and: [
        { $or: [{ make: new RegExp(`^${escapeRegex(make)}$`, 'i') }, { brand: new RegExp(`^${escapeRegex(make)}$`, 'i') }] },
        { model: new RegExp(`^${escapeRegex(model)}$`, 'i') },
      ],
    });
  }

  return ors.length ? { $or: ors } : null;
};

const buildFeatureMatrixVehicleQuery = (anchor = {}) => {
  const make = firstMeaningful(anchor.make, anchor.brand);
  const model = anchor.model;
  const fullModel = firstMeaningful(anchor.fullModel, [make, model].filter(Boolean).join(' '));

  const rawCandidates = [
    anchor.shortModelKey,
    anchor.modelKey,
    model,
    fullModel,
  ];

  const modelKeyCandidates = rawCandidates
    .flatMap((value) => {
      const key = slugify(value);
      if (!key) return [];

      const makeSlug = slugify(make);
      const modelSlug = slugify(model);
      const fullModelSlug = slugify(fullModel);

      const out = [key];

      if (makeSlug && key.startsWith(`${makeSlug}-`)) {
        out.push(key.slice(`${makeSlug}-`.length));
      }

      if (fullModelSlug && key === fullModelSlug && modelSlug) {
        out.push(modelSlug);
      }

      return out;
    })
    .filter(Boolean);

  const uniqueModelKeys = [...new Set(modelKeyCandidates)];

  const ors = [];

  for (const key of uniqueModelKeys) {
    ors.push({ modelKey: new RegExp(`^${escapeRegex(key)}$`, 'i') });
    ors.push({ brandModelKey: new RegExp(`^${escapeRegex(key)}$`, 'i') });
  }

  if (make && model) {
    ors.push({
      $and: [
        {
          $or: [
            { make: new RegExp(`^${escapeRegex(make)}$`, 'i') },
            { brand: new RegExp(`^${escapeRegex(make)}$`, 'i') },
          ],
        },
        { model: new RegExp(`^${escapeRegex(model)}$`, 'i') },
      ],
    });
  }

  return ors.length ? { $or: ors } : null;
};

const valueFromFeatureCell = (cell = {}) => {
  if (!cell || typeof cell !== 'object') return '';
  if (cell.available === false || cell.availabilityStatus === 'not_available') return '';
  return firstMeaningful(cell.value, cell.displayValue, cell.text, cell.label);
};

const collectMatrixAttributeValues = ({ row = {}, definition = {} } = {}) => {
  const cells = [
    ['featuresByKey', row?.featuresByKey?.[definition.key]],
    ['decisionSignals.featuresByKey', row?.decisionSignals?.featuresByKey?.[definition.key]],
  ];

  return cells
    .map(([field, cell]) => {
      const value = valueFromFeatureCell(cell);
      if (!value) return null;

      return compactObject({
        attributeKey: definition.key,
        attributeLabel: definition.label,
        field: `${field}.${definition.key}`,
        value: String(value),
        variant: firstMeaningful(row.variantFull, row.variant, row.variantName, row.variantKey),
        variantKey: row.variantKey,
        modelKey: row.modelKey,
        make: firstMeaningful(row.make, row.brand),
        model: row.model,
        source: 'vehicle_variant_feature_matrix_v2',
        rawValue: cell,
      });
    })
    .filter(Boolean);
};

const dedupeAttributeValues = (values = []) => {
  const seen = new Set();

  return values.filter((item = {}) => {
    const key = [
      item.attributeKey || '',
      item.variant || '',
      item.value || '',
    ].join('|').toLowerCase();

    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

async function lookupVehicleSpecAttribute({
  attribute,
  anchor = {},
} = {}) {
  const definition = typeof attribute === 'string'
    ? SPEC_ATTRIBUTE_BY_KEY[attribute]
    : attribute;
  const db = getDb();
  const query = buildVehicleQuery(anchor);

  if (!definition || !db || !query) {
    return {
      values: [],
      missingData: true,
      recordCount: 0,
      modulesChecked: ['aci_vehicle_model_summary'],
    };
  }

  const projection = Object.fromEntries(
    [
      'make',
      'brand',
      'model',
      'fullModel',
      'modelKey',
      'shortModelKey',
      'specs',
      'specifications',
      'attributes',
      ...definition.fields,
    ].map((field) => [field, 1]),
  );

  const modelSummaryRows = await db.collection('aci_vehicle_model_summary').find(query, { projection }).limit(10).toArray();
  const modelSummaryValues = modelSummaryRows.flatMap((row) => collectAttributeValues({ row, definition }));

  const matrixQuery = buildFeatureMatrixVehicleQuery(anchor);
  const matrixProjection = {
    make: 1,
    brand: 1,
    model: 1,
    modelKey: 1,
    brandModelKey: 1,
    variant: 1,
    variantFull: 1,
    variantName: 1,
    variantKey: 1,
    [`featuresByKey.${definition.key}`]: 1,
    [`decisionSignals.featuresByKey.${definition.key}`]: 1,
  };

  const matrixRows = matrixQuery
    ? await db.collection('vehicle_variant_feature_matrix_v2').find(matrixQuery, { projection: matrixProjection }).limit(30).toArray()
    : [];

  const matrixValues = matrixRows.flatMap((row) => collectMatrixAttributeValues({ row, definition }));
  const values = dedupeAttributeValues([...modelSummaryValues, ...matrixValues]);

  return {
    values,
    missingData: values.length === 0,
    recordCount: modelSummaryRows.length + matrixRows.length,
    modulesChecked: ['aci_vehicle_model_summary', 'vehicle_variant_feature_matrix_v2'],
  };
}

function getAnchorFromToolPlan({ toolPlan = {}, context = {} } = {}) {
  const entityVehicle = {
    make: firstMeaningful(toolPlan.entities?.make, toolPlan.entities?.brand, toolPlan.filters?.make),
    model: firstMeaningful(toolPlan.entities?.model, toolPlan.entities?.primaryModel, toolPlan.filters?.model),
    fullModel: firstMeaningful(toolPlan.entities?.fullModel, toolPlan.filters?.fullModel),
    variant: firstMeaningful(toolPlan.entities?.variant, toolPlan.filters?.variant),
    city: firstMeaningful(toolPlan.entities?.city, toolPlan.filters?.city),
    source: 'tool_plan',
    confidence: 0.82,
  };

  const contextVehicle =
    context?.selectedVehicle ||
    context?.contextState?.selectedVehicle ||
    context?.aciContextState?.selectedVehicle ||
    {};

  return compactVehicleContext({
    ...contextVehicle,
    ...entityVehicle,
    makeKey: firstMeaningful(contextVehicle.makeKey, keyify(entityVehicle.make)),
    modelKey: firstMeaningful(contextVehicle.modelKey, keyify(entityVehicle.fullModel || [entityVehicle.make, entityVehicle.model].filter(Boolean).join(' '))),
    shortModelKey: firstMeaningful(contextVehicle.shortModelKey, keyify(entityVehicle.model)),
  });
}

async function runVehicleSpecAttributeLookup({
  toolPlan = {},
  userMessage = '',
  context = {},
} = {}) {
  const attribute =
    SPEC_ATTRIBUTE_BY_KEY[toolPlan.filters?.attributeKey] ||
    SPEC_ATTRIBUTE_BY_KEY[toolPlan.entities?.attributeKey] ||
    resolveSpecAttributeFromText({
      message: userMessage,
      features: [
        toolPlan.entities?.topic,
        toolPlan.filters?.topic,
        ...asArray(toolPlan.filters?.mustHaveFeatures),
      ],
    });
  const anchor = getAnchorFromToolPlan({ toolPlan, context });
  const lookup = await lookupVehicleSpecAttribute({ attribute, anchor });
  const label = attribute?.label || 'requested specification';
  const modelLabel = anchor.fullModel || [anchor.make, anchor.model].filter(Boolean).join(' ') || 'this model';
  const ux = composeResolvedSpecAnswer({
    modelLabel,
    attributeLabel: label,
    values: lookup.values,
    missingData: lookup.missingData,
  });

  return {
    intent: 'vehicle_spec_attribute_answer',
    tool: 'vehicle_spec_attribute_lookup',
    matched: lookup.values.length,
    count: lookup.values.length,
    modulesChecked: lookup.modulesChecked,
    dataSource: lookup.modulesChecked.join('+'),
    displayMode: 'inline',
    inlineType: 'spec_attribute_answer_card',
    canvasType: '',
    title: `${modelLabel} ${label}`,
    answer: ux.answer,
    actions: ux.nextActions,
    leadingQuestions: ux.nextActions,
    conversationSuggestions: ux.nextActions,
    followUpSuggestions: ux.nextActions.map((item) => item.query).filter(Boolean),
    data: {
      anchorMake: anchor.make,
      anchorModel: anchor.model,
      anchorFullModel: modelLabel,
      attributeKey: attribute?.key || '',
      attributeLabel: label,
      values: lookup.values,
      missingData: lookup.missingData,
      recordCount: lookup.recordCount,
      nextActions: ux.nextActions,
      sourceTransparency: {
        modulesChecked: lookup.modulesChecked,
        recordCount: lookup.recordCount,
        matched: lookup.values.length,
        note: lookup.missingData
          ? 'Model row found without an exact current vehicle-data value.'
          : 'Exact spec value read from current vehicle data.',
      },
    },
    sourceTransparency: {
      modulesChecked: lookup.modulesChecked,
      recordCount: lookup.recordCount,
      matched: lookup.values.length,
      missingData: lookup.missingData,
    },
    contextPatch: mergeContextPatches({
      managerPatch: context?.contextState || context?.aciContextState
        ? { contextState: context.contextState || context.aciContextState }
        : {},
      toolPatch: {
        selectedVehicle: anchor,
      },
    }),
  };
}

export {
  SPEC_ATTRIBUTE_DEFINITIONS,
  isSpecAttributeTopic,
  lookupVehicleSpecAttribute,
  resolveSpecAttributeFromText,
  runVehicleSpecAttributeLookup,
};

export default runVehicleSpecAttributeLookup;
