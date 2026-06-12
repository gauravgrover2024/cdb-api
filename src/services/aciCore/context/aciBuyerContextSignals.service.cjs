'use strict';

const cleanText = (value = '') => String(value || '').trim();

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === 'object') return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== '';
    })
  );

function inferBuyerSignalsFromMessage(message = '') {
  const raw = cleanText(message).toLowerCase();
  const inferred = {};
  const signals = [];

  if (/\bshould\s+i\s+(buy|choose|pick|go\s+for|purchase)\b|\bworth\s+buying\b|\bgo\s+ahead\s+with\b/.test(raw)) {
    inferred.finalChoiceIntent = true;
    signals.push('final-choice intent');
  }

  if (/\bfamily\b|\bparents?\b|\bkids?\b/.test(raw) && /\bhighway\b|\blong\s+(?:drive|trip|route)\b/.test(raw)) {
    inferred.safetySensitiveUse = true;
    signals.push('family highway use');
  }

  if (/\bautomatic\b|\bauto\b|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bat\b/.test(raw) && /\bcity\b|\btraffic\b|\bcommute\b/.test(raw)) {
    inferred.cityAutomaticPreference = true;
    signals.push('city automatic preference');
  }

  if (/\bsafest\b|\bsafety\b|\bncap\b|\bcrash\s+test\b|\bairbags?\b/.test(raw)) {
    inferred.safetySensitive = true;
    signals.push('safety-sensitive query');
  }

  if (/\bmileage\b|\brunning\s+cost\b|\bservice\b|\bmaintenance\b|\bresale\b|\bfuel\s+cost\b/.test(raw)) {
    inferred.runningCostSensitive = true;
    signals.push('running-cost-sensitive query');
  }

  if (/\bfamily\b|\bparents?\b|\bkids?\b|\brear\s+seat\b/.test(raw)) {
    inferred.familyPracticalitySensitive = true;
    signals.push('family/practicality-sensitive query');
  }

  if (/\bstretch\b|\bupgrade\b|\bworth\s+(?:the\s+)?extra\b|\bextra\s+(?:money|cost|price)\b|\bfrom\b.+\bto\b/.test(raw)) {
    inferred.upgradeIntent = true;
    signals.push('upgrade intent');
  }

  return compactObject({
    ...inferred,
    signals,
  });
}

module.exports = {
  inferBuyerSignalsFromMessage,
};
