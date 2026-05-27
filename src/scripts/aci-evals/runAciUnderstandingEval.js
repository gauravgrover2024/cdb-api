import {
  ACI_UNDERSTANDING_CORPUS_V1,
} from './corpus/aciUnderstandingCorpus.v1.js';

import {
  ACI_UNDERSTANDING_CORPUS_EXTENDED_V1,
} from './corpus/aciUnderstandingCorpus.extendedV1.js';

const ACI_UNDERSTANDING_CORPUS = [
  ...ACI_UNDERSTANDING_CORPUS_V1,
  ...ACI_UNDERSTANDING_CORPUS_EXTENDED_V1,
];

/**
 * Temporary V1 runner.
 *
 * This validates the corpus structure only.
 * Next slice will plug the real ACI Understanding Engine into this runner.
 */

const REQUIRED_BUCKETS = [
  'direct',
  'broad_discovery',
  'messy_no_comma',
  'natural_language',
  'hinglish',
  'typos',
  'context_followup',
  'context_switch',
  'context_refine',
  'context_ambiguous',
  'comparison',
  'future_modules',
  'hallucination_traps',
  'ambiguous',
  'off_topic',
  'onroad_price',
  'price_breakdown',
  'offers_discounts',
  'waiting_period',
  'upcoming_launches',
  'model_year_comparison',
  'resale_value',
  'running_cost',
  'reliability_ownership',
  'ncap_safety_rating',
  'rivals_alternatives',
  'dealer_locator',
  'accessories',
  'variant_navigation',
  'color_specific',
  'fuel_type_advice',
  'finance_deep',
  'ownership_cost_total',
  'tco_vs_tco',
  'multi_city_price',
  'exchange_trade_in',
  'new_vs_used',
  'hinglish_extended',
  'warranty_service',
  'context_refine_extended',
  'typos_extended',
  'hallucination_traps_extended',
  'off_topic_boundary',
  'multi_intent_extreme',
];

const failures = [];
const ids = new Set();
const buckets = new Set();

for (const item of ACI_UNDERSTANDING_CORPUS) {
  if (!item || typeof item !== 'object') {
    failures.push({ id: null, issue: 'item_not_object' });
    continue;
  }

  if (!item.id) failures.push({ id: item.id || null, issue: 'missing_id' });
  if (!item.bucket) failures.push({ id: item.id || null, issue: 'missing_bucket' });
  if (!item.message) failures.push({ id: item.id || null, issue: 'missing_message' });
  if (!item.expected || typeof item.expected !== 'object') {
    failures.push({ id: item.id || null, issue: 'missing_expected' });
  }

  if (item.id) {
    if (ids.has(item.id)) failures.push({ id: item.id, issue: 'duplicate_id' });
    ids.add(item.id);
  }

  if (item.bucket) buckets.add(item.bucket);

  if (item.expected && !item.expected.primaryTask) {
    failures.push({ id: item.id, issue: 'missing_expected_primaryTask' });
  }
}

for (const bucket of REQUIRED_BUCKETS) {
  if (!buckets.has(bucket)) {
    failures.push({ id: null, issue: `missing_required_bucket:${bucket}` });
  }
}

const bucketSummary = ACI_UNDERSTANDING_CORPUS.reduce((acc, item) => {
  acc[item.bucket] = (acc[item.bucket] || 0) + 1;
  return acc;
}, {});

console.log(JSON.stringify({
  suite: 'ACI Understanding Corpus structural eval',
  ok: failures.length === 0,
  total: ACI_UNDERSTANDING_CORPUS.length,
  baseTotal: ACI_UNDERSTANDING_CORPUS_V1.length,
  extendedTotal: ACI_UNDERSTANDING_CORPUS_EXTENDED_V1.length,
  uniqueIds: ids.size,
  bucketCount: buckets.size,
  summary: {
    version: 'aciUnderstandingCorpus.combinedV1',
    total: ACI_UNDERSTANDING_CORPUS.length,
    buckets: bucketSummary,
  },
  failures,
}, null, 2));

if (failures.length > 0) {
  process.exit(1);
}
