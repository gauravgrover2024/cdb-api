import buyerContextSignals from './aciBuyerContextSignals.service.cjs';

const { inferBuyerSignalsFromMessage } = buyerContextSignals;

const cleanText = (value = '') => String(value || '').trim();

const asObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) ? value : {};

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const unique = (items = []) => {
  const seen = new Set();
  const out = [];
  for (const item of items.map(cleanText).filter(Boolean)) {
    const key = item.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      out.push(item);
    }
  }
  return out;
};

const hasValue = (value) => {
  if (Array.isArray(value)) return value.length > 0;
  if (value && typeof value === 'object') return Object.keys(value).length > 0;
  return Boolean(cleanText(value));
};

const normalizeCitySlug = (city = '') => {
  const raw = cleanText(city).toLowerCase();
  if (!raw) return '';
  if (/\bdelhi\b|\bnew\s*delhi\b/.test(raw)) return 'new-delhi';
  if (/\bnoida\b/.test(raw)) return 'noida';
  if (/\bgurgaon\b|\bgurugram\b/.test(raw)) return 'gurgaon';
  return raw.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
};

const amountToRupees = (num, unit = '') => {
  const value = Number(num);
  if (!Number.isFinite(value) || value <= 0) return 0;
  const normalizedUnit = cleanText(unit).toLowerCase();
  if (/\b(crore|crores|cr)\b/.test(normalizedUnit)) return Math.round(value * 10000000);
  if (/\b(lakh|lakhs|lac|lacs|l)\b/.test(normalizedUnit)) return Math.round(value * 100000);
  if (/\b(k|thousand)\b/.test(normalizedUnit)) return Math.round(value * 1000);
  if (value <= 300) return Math.round(value * 100000);
  return Math.round(value);
};

const extractBudget = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  const match =
    raw.match(/\b(?:under|below|upto|up to|within|less than|budget(?:\s+of)?|around|near)\s*(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|k|thousand)?\b/i) ||
    raw.match(/\b(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l)\s*(?:budget|range)?\b/i);
  if (!match) return 0;
  return amountToRupees(match[1], match[2] || 'lakh');
};

const extractCity = (text = '') => {
  const raw = cleanText(text);
  if (/\bnew\s*delhi\b|\bdelhi\b/i.test(raw)) return 'Delhi';
  if (/\bnoida\b/i.test(raw)) return 'Noida';
  if (/\bgurgaon\b|\bgurugram\b/i.test(raw)) return 'Gurgaon';
  return '';
};

const extractUseCase = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  const signals = [];
  if (/\bfamily\b|\bparents?\b|\bkids?\b|\bpractical\b|\bspacious\b|\brear seat\b/.test(raw)) signals.push('family use');
  if (/\bcity\s+(?:use|drive|driving|commute|commuting)|\btraffic\b|\bdaily\s+use\b/.test(raw)) signals.push('city use');
  if (/\bhighway\b|\blong\s+(?:drive|trip|route)|\btouring\b/.test(raw)) signals.push('highway use');
  if (/\bbad\s+roads?\b|\brough\s+roads?\b|\bground clearance\b/.test(raw)) signals.push('bad-road use');
  if (/\bchauffeur\b|\brear\s+comfort\b|\bdriver driven\b/.test(raw)) signals.push('chauffeur/rear-seat use');
  if (/\benthusiast\b|\bperformance\b|\bpowerful\b|\bfast\b/.test(raw)) signals.push('performance use');
  return unique(signals).join(', ');
};

const extractFamily = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  const familyCount =
    raw.match(/\bfamily\s+of\s+(\d+)\b/i) ||
    raw.match(/\b(\d+)\s*(?:people|members|seater need|seats?)\b/i);
  if (familyCount) return `${familyCount[1]} occupants`;
  if (/\bfamily\b|\bparents?\b|\bkids?\b|\brear seat\b/.test(raw)) return 'family occupancy';
  return '';
};

const extractRunning = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  const daily =
    raw.match(/\b(\d+(?:\.\d+)?)\s*km\s*(?:daily|per day|a day)\b/i) ||
    raw.match(/\bdaily\s*(?:running|drive|travel)?\s*(?:of|is|=)?\s*(\d+(?:\.\d+)?)\s*km\b/i);
  if (daily) return `${daily[1]} km daily`;

  const monthly =
    raw.match(/\b(\d+(?:\.\d+)?)\s*km\s*(?:monthly|per month|a month)\b/i) ||
    raw.match(/\bmonthly\s*(?:running|drive|travel)?\s*(?:of|is|=)?\s*(\d+(?:\.\d+)?)\s*km\b/i);
  if (monthly) return `${monthly[1]} km monthly`;

  if (/\bhigh\s+running\b|\bheavy\s+running\b|\blong\s+running\b/.test(raw)) return 'high running';
  if (/\blow\s+running\b|\bless\s+running\b|\boccasional\s+use\b/.test(raw)) return 'low running';
  return '';
};

const extractFuel = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  const fuels = [];
  if (/\bcng\b/.test(raw)) fuels.push('CNG');
  if (/\bpetrol\b/.test(raw)) fuels.push('petrol');
  if (/\bdiesel\b/.test(raw)) fuels.push('diesel');
  if (/\belectric\b|\bev\b/.test(raw)) fuels.push('electric');
  if (/\bhybrid\b/.test(raw)) fuels.push('hybrid');
  return unique(fuels).join(', ');
};

const extractTransmission = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  if (/\bautomatic\b|\bauto\b|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bat\b/.test(raw)) return 'automatic';
  if (/\bmanual\b|\bmt\b/.test(raw)) return 'manual';
  return '';
};

const extractSafetyPriority = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  if (/\bsafest\b|\bsafety\s+(?:is\s+)?(?:important|priority|must)|\bhigh\s+safety\b|\bncap\b|\bcrash\s+test\b|\b5\s*star\b/.test(raw)) {
    return 'high';
  }
  if (/\bsafety\b|\bairbags?\b/.test(raw)) return 'medium';
  return '';
};

const extractFeaturePriority = (text = '') => {
  const raw = cleanText(text).toLowerCase();
  const features = [];
  if (/\bpanoramic\s+sunroof\b/.test(raw)) features.push('panoramic sunroof');
  else if (/\bsunroof\b/.test(raw)) features.push('sunroof');
  if (/\b6\s*airbags?\b|\bsix\s+airbags?\b/.test(raw)) features.push('6 airbags');
  else if (/\bairbags?\b/.test(raw)) features.push('airbags');
  if (/\badas\b/.test(raw)) features.push('ADAS');
  if (/\b360\s*(?:degree)?\s*camera\b|\b360\s*camera\b/.test(raw)) features.push('360 camera');
  if (/\brear\s+camera\b|\breverse\s+camera\b/.test(raw)) features.push('rear camera');
  if (/\bventilated\s+seats?\b/.test(raw)) features.push('ventilated seats');
  if (/\bcruise\s+control\b/.test(raw)) features.push('cruise control');
  if (/\bwireless\s+charging\b/.test(raw)) features.push('wireless charging');
  return unique(features);
};

const extractDiscoveryScope = ({ text = '', useCase = '', fuel = '', transmission = '', budget = 0 } = {}) => {
  const raw = cleanText(text).toLowerCase();
  const scope = [];
  if (/\bsuvs?\b/.test(raw)) scope.push('SUV');
  if (/\bsedans?\b/.test(raw)) scope.push('sedan');
  if (/\bhatchbacks?\b/.test(raw)) scope.push('hatchback');
  if (useCase) scope.push(useCase);
  if (fuel) scope.push(fuel);
  if (transmission) scope.push(transmission);
  if (budget) scope.push(`under ${budget}`);
  return unique(scope).join(', ');
};

function extractBuyerContextFromMessage({ message = '', previousBuyerContext = {} } = {}) {
  const text = cleanText(message);
  const previous = asObject(previousBuyerContext);
  if (!text) {
    return {
      buyerContextPatch: {},
      detectedInputs: [],
      confidence: 0,
    };
  }

  const city = extractCity(text);
  const budget = extractBudget(text);
  const useCase = extractUseCase(text);
  const family = extractFamily(text);
  const running = extractRunning(text);
  const fuel = extractFuel(text);
  const transmission = extractTransmission(text);
  const safety = extractSafetyPriority(text);
  const features = extractFeaturePriority(text);
  const scope = extractDiscoveryScope({ text, useCase, fuel, transmission, budget });
  const inferredBuyerContext = inferBuyerSignalsFromMessage(text);

  const patch = {
    ...(city ? { city, citySlug: normalizeCitySlug(city) } : {}),
    ...(budget ? { budgetOrPriceCeiling: budget, maxBudget: budget } : {}),
    ...(useCase ? { bodyPreferenceOrPrimaryUseCase: useCase, primaryUseCase: useCase } : {}),
    ...(family ? { familySizeOrOccupancyUse: family } : {}),
    ...(running || fuel ? {
      fuelPreferenceOrMonthlyRunning: unique([fuel, running]).join(', '),
      ...(fuel ? { fuelPreference: fuel } : {}),
      ...(running ? { monthlyRunning: running } : {}),
    } : {}),
    ...(transmission ? { transmissionPreference: transmission } : {}),
    ...(safety ? { safetyPriority: safety } : {}),
    ...(features.length ? { featurePriority: features } : {}),
    ...(scope ? { shortlistedModelsOrDiscoveryScope: scope } : {}),
    ...(asArray(inferredBuyerContext.signals).length ? { inferredBuyerContext } : {}),
  };

  const detectedInputs = Object.keys(patch).filter((key) => !['citySlug', 'maxBudget', 'primaryUseCase', 'fuelPreference', 'monthlyRunning'].includes(key));

  if (detectedInputs.length) {
    patch.source = 'buyer_context_extractor_v1';
    patch.confidence = Math.min(0.95, Number((0.45 + detectedInputs.length * 0.06).toFixed(2)));
    patch.extractedAt = new Date().toISOString();
  }

  return {
    buyerContextPatch: patch,
    detectedInputs,
    confidence: patch.confidence || 0,
    previousBuyerContext: previous,
  };
}

function mergeBuyerContext(previousBuyerContext = {}, buyerContextPatch = {}) {
  const previous = asObject(previousBuyerContext);
  const patch = asObject(buyerContextPatch);
  const merged = { ...previous };

  for (const [key, value] of Object.entries(patch)) {
    if (key === 'featurePriority') {
      merged.featurePriority = unique([
        ...asArray(previous.featurePriority),
        ...asArray(value),
      ]);
      continue;
    }
    if (hasValue(value)) merged[key] = value;
  }

  return merged;
}

function applyBuyerContextToContextState({ message = '', contextState = {} } = {}) {
  const state = asObject(contextState);
  const previousBuyerContext = asObject(state.buyerContext || state.buyerIntent);
  const extraction = extractBuyerContextFromMessage({ message, previousBuyerContext });

  if (!Object.keys(extraction.buyerContextPatch).length) {
    return {
      ...state,
      buyerContext: previousBuyerContext,
    };
  }

  const buyerContext = mergeBuyerContext(previousBuyerContext, extraction.buyerContextPatch);

  return {
    ...state,
    buyerContext,
    buyerGuidanceContext: {
      ...(state.buyerGuidanceContext || {}),
      inferredContext: {
        ...(state.buyerGuidanceContext?.inferredContext || {}),
        ...(buyerContext.inferredBuyerContext || {}),
      },
    },
    provenance: {
      ...(state.provenance || {}),
      sources: unique([...(state.provenance?.sources || []), 'buyer_context_extractor_v1']),
      updatedBy: 'buyer_context_extractor_v1',
    },
  };
}

export {
  applyBuyerContextToContextState,
  extractBuyerContextFromMessage,
  inferBuyerSignalsFromMessage,
  mergeBuyerContext,
  normalizeCitySlug,
};

export default extractBuyerContextFromMessage;
