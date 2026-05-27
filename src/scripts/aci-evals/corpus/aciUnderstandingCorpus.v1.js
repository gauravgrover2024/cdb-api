import {
  ACI_CONTEXT_ACTIONS,
  ACI_DOMAINS,
  ACI_RESULT_GRANULARITY,
  ACI_TASKS,
} from '../../../services/aciCore/understanding/aciMeaningFrame.schema.js';

/**
 * ACI Understanding Corpus V1
 *
 * Purpose:
 * Stress-test meaning-frame understanding before changing live routing.
 *
 * This corpus intentionally includes:
 * - direct queries
 * - broad discovery
 * - no-comma messy queries
 * - Hinglish
 * - typos
 * - follow-ups
 * - context switches
 * - multi-feature / multi-filter
 * - variant-vs-variant comparisons
 * - future capability placeholders
 *
 * Important:
 * - Expected values are semantic expectations, not factual car answers.
 * - No automotive truth is hardcoded here.
 * - Facts must later be validated through DB/tools.
 */

const DEFAULT_CITY = 'Delhi';

const ctx = {
  none: null,
  vernaSxIvt: {
    selectedVehicle: {
      make: 'Hyundai',
      model: 'Verna',
      variant: 'SX IVT',
    },
    city: DEFAULT_CITY,
    lastTask: ACI_TASKS.PRICE_LOOKUP,
  },
  creta: {
    selectedVehicle: {
      make: 'Hyundai',
      model: 'Creta',
      variant: null,
    },
    city: DEFAULT_CITY,
    lastTask: ACI_TASKS.VEHICLE_OVERVIEW,
  },
  broadSunroof: {
    lastDiscovery: {
      task: ACI_TASKS.VEHICLE_DISCOVERY,
      filters: {
        features: ['sunroof'],
      },
    },
    city: DEFAULT_CITY,
  },
};

const q = ({
  id,
  bucket,
  message,
  activeContext = ctx.none,
  expected,
  notes = '',
}) => ({
  id,
  bucket,
  message,
  activeContext,
  expected: {
    domains: [ACI_DOMAINS.NEW_CAR],
    contextAction: ACI_CONTEXT_ACTIONS.SWITCH_TO_EXPLICIT_ENTITY,
    resultGranularity: ACI_RESULT_GRANULARITY.MODEL_AND_VARIANT,
    shouldAskClarification: false,
    mustNotHallucinateFacts: true,
    ...expected,
  },
  notes,
});

export const ACI_UNDERSTANDING_CORPUS_V1 = [
  // 1–10: Simple direct questions
  q({ id: 'direct-001', bucket: 'direct', message: 'Creta price', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Creta' }, requestedFacts: ['price'] } }),
  q({ id: 'direct-002', bucket: 'direct', message: 'Verna colors', expected: { primaryTask: ACI_TASKS.COLOR_LOOKUP, anchors: { model: 'Verna' }, requestedFacts: ['colors'] } }),
  q({ id: 'direct-003', bucket: 'direct', message: 'Punch variants', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, anchors: { model: 'Punch' }, requestedFacts: ['variants'] } }),
  q({ id: 'direct-004', bucket: 'direct', message: 'Seltos features', expected: { primaryTask: ACI_TASKS.FEATURE_DISCOVERY, anchors: { model: 'Seltos' }, requestedFacts: ['features'] } }),
  q({ id: 'direct-005', bucket: 'direct', message: 'Thar mileage', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Thar' }, requestedFeatures: ['mileage'] } }),
  q({ id: 'direct-006', bucket: 'direct', message: 'EQS range', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'EQS' }, requestedFeatures: ['range'] } }),
  q({ id: 'direct-007', bucket: 'direct', message: 'Nexon price Delhi', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Nexon', city: DEFAULT_CITY }, requestedFacts: ['price'] } }),
  q({ id: 'direct-008', bucket: 'direct', message: 'City ZX CVT price', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'City', variant: 'ZX CVT' }, requestedFacts: ['price'] } }),
  q({ id: 'direct-009', bucket: 'direct', message: 'Creta SX(O) IVT features', expected: { primaryTask: ACI_TASKS.FEATURE_DISCOVERY, anchors: { model: 'Creta', variant: 'SX(O) IVT' }, requestedFacts: ['features'] } }),
  q({ id: 'direct-010', bucket: 'direct', message: 'Black Verna', expected: { primaryTask: ACI_TASKS.COLOR_LOOKUP, anchors: { model: 'Verna' }, filters: { colors: ['black'] } } }),

  // 11–24: Broad discovery/listing
  q({ id: 'broad-001', bucket: 'broad_discovery', message: 'Hyundai cars', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Hyundai'] }, discovery: { isBroadDiscovery: true, resultGranularity: ACI_RESULT_GRANULARITY.MODEL } } }),
  q({ id: 'broad-002', bucket: 'broad_discovery', message: 'cars under 20 lakhs', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { budget: { max: 2000000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-003', bucket: 'broad_discovery', message: 'cars with sunroof', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['sunroof'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-004', bucket: 'broad_discovery', message: 'automatic SUVs under 20L', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { transmissions: ['automatic'], bodyTypes: ['SUV'], budget: { max: 2000000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-005', bucket: 'broad_discovery', message: 'CNG cars with ABS', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { fuelTypes: ['CNG'], features: ['ABS'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-006', bucket: 'broad_discovery', message: 'sedans with sunroof', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { bodyTypes: ['sedan'], features: ['sunroof'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-007', bucket: 'broad_discovery', message: 'electric cars under 25 lakh', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { fuelTypes: ['electric'], budget: { max: 2500000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-008', bucket: 'broad_discovery', message: 'diesel automatic SUVs', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { fuelTypes: ['diesel'], transmissions: ['automatic'], bodyTypes: ['SUV'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-009', bucket: 'broad_discovery', message: 'cars with 6 airbags under 15 lakh', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['6 airbags'], budget: { max: 1500000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-010', bucket: 'broad_discovery', message: 'Tata cars', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Tata'] }, discovery: { isBroadDiscovery: true, resultGranularity: ACI_RESULT_GRANULARITY.MODEL } } }),
  q({ id: 'broad-011', bucket: 'broad_discovery', message: 'cars with ADAS', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['ADAS'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-012', bucket: 'broad_discovery', message: 'petrol automatic cars', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { fuelTypes: ['petrol'], transmissions: ['automatic'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-013', bucket: 'broad_discovery', message: 'safe cars under 12 lakh', expected: { primaryTask: ACI_TASKS.SAFEST_VEHICLES, filters: { budget: { max: 1200000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'broad-014', bucket: 'broad_discovery', message: 'best family cars under 20 lakh', expected: { primaryTask: ACI_TASKS.RECOMMENDATION, filters: { budget: { max: 2000000 }, usage: ['family'] }, discovery: { isBroadDiscovery: true } } }),

  // 25–38: No-comma messy queries
  q({ id: 'messy-001', bucket: 'messy_no_comma', message: 'Punch CNG sunroof ABS price', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS'] }, requestedFacts: ['price', 'features'], mustNotSetVariantFromFeatureWords: true } }),
  q({ id: 'messy-002', bucket: 'messy_no_comma', message: 'Creta automatic ADAS under 20 lakh', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, anchors: { model: 'Creta' }, filters: { transmissions: ['automatic'], features: ['ADAS'], budget: { max: 2000000 } } } }),
  q({ id: 'messy-003', bucket: 'messy_no_comma', message: 'Verna black SX IVT price Delhi', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Verna', variant: 'SX IVT', city: DEFAULT_CITY }, filters: { colors: ['black'] }, requestedFacts: ['price'] } }),
  q({ id: 'messy-004', bucket: 'messy_no_comma', message: 'Nexon EV range price Delhi', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Nexon EV', city: DEFAULT_CITY }, requestedFeatures: ['range'], requestedFacts: ['price', 'features'] } }),
  q({ id: 'messy-005', bucket: 'messy_no_comma', message: 'Seltos diesel automatic sunroof', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Seltos' }, filters: { fuelTypes: ['diesel'], transmissions: ['automatic'], features: ['sunroof'] } } }),
  q({ id: 'messy-006', bucket: 'messy_no_comma', message: 'Creta sunroof automatic petrol price', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Creta' }, filters: { fuelTypes: ['petrol'], transmissions: ['automatic'], features: ['sunroof'] }, requestedFacts: ['price'] } }),
  q({ id: 'messy-007', bucket: 'messy_no_comma', message: 'City ZX CVT EMI 2 lakh down', expected: { primaryTask: ACI_TASKS.EMI_CALCULATION, anchors: { model: 'City', variant: 'ZX CVT' }, finance: { downPayment: 200000 } } }),
  q({ id: 'messy-008', bucket: 'messy_no_comma', message: 'Thar 4x4 diesel price features', expected: { primaryTask: ACI_TASKS.FEATURE_DISCOVERY, anchors: { model: 'Thar' }, filters: { fuelTypes: ['diesel'], features: ['4x4'] }, requestedFacts: ['price', 'features'] } }),
  q({ id: 'messy-009', bucket: 'messy_no_comma', message: 'i20 automatic sunroof Delhi', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'i20', city: DEFAULT_CITY }, filters: { transmissions: ['automatic'], features: ['sunroof'] } } }),
  q({ id: 'messy-010', bucket: 'messy_no_comma', message: 'Sonet HTX DCT price emi', expected: { primaryTask: ACI_TASKS.EMI_CALCULATION, anchors: { model: 'Sonet', variant: 'HTX DCT' }, requestedFacts: ['price', 'emi'] } }),
  q({ id: 'messy-011', bucket: 'messy_no_comma', message: 'Kia cars sunroof under 15 lakh', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Kia'], features: ['sunroof'], budget: { max: 1500000 } } } }),
  q({ id: 'messy-012', bucket: 'messy_no_comma', message: 'Honda sedan automatic price', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Honda'], bodyTypes: ['sedan'], transmissions: ['automatic'] }, requestedFacts: ['price'] } }),
  q({ id: 'messy-013', bucket: 'messy_no_comma', message: 'XUV700 ADAS airbags price', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'XUV700' }, filters: { features: ['ADAS', 'airbags'] }, requestedFacts: ['price', 'features'] } }),
  q({ id: 'messy-014', bucket: 'messy_no_comma', message: 'Grand Vitara hybrid automatic under 20 lakh', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, anchors: { model: 'Grand Vitara' }, filters: { fuelTypes: ['hybrid'], transmissions: ['automatic'], budget: { max: 2000000 } } } }),

  // 39–49: Natural language
  q({ id: 'natural-001', bucket: 'natural_language', message: 'Which Punch CNG variants have sunroof and ABS?', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS'] }, mustNotSetVariantFromFeatureWords: true } }),
  q({ id: 'natural-002', bucket: 'natural_language', message: 'Does Creta automatic under 20 lakh get ADAS?', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Creta' }, filters: { transmissions: ['automatic'], budget: { max: 2000000 }, features: ['ADAS'] } } }),
  q({ id: 'natural-003', bucket: 'natural_language', message: 'Show me the cheapest Seltos with sunroof', expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, anchors: { model: 'Seltos' }, filters: { features: ['sunroof'] }, sortBy: 'cheapest' } }),
  q({ id: 'natural-004', bucket: 'natural_language', message: 'Which Verna variant should I buy?', expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, anchors: { model: 'Verna' }, requestedFacts: ['recommendation'] } }),
  q({ id: 'natural-005', bucket: 'natural_language', message: 'Is Nexon better than Punch?', expected: { primaryTask: ACI_TASKS.VEHICLE_COMPARISON, comparisonTargets: [{ model: 'Nexon' }, { model: 'Punch' }] } }),
  q({ id: 'natural-006', bucket: 'natural_language', message: 'What do I lose if I choose Creta S(O) instead of SX?', expected: { primaryTask: ACI_TASKS.VARIANT_DELTA, anchors: { model: 'Creta' }, comparisonTargets: [{ variant: 'S(O)' }, { variant: 'SX' }] } }),
  q({ id: 'natural-007', bucket: 'natural_language', message: 'Find me the cheapest car with ADAS and 6 airbags', expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, filters: { features: ['ADAS', '6 airbags'] }, sortBy: 'cheapest' } }),
  q({ id: 'natural-008', bucket: 'natural_language', message: 'What is the best SUV for city driving under 18 lakh?', expected: { primaryTask: ACI_TASKS.RECOMMENDATION, filters: { bodyTypes: ['SUV'], budget: { max: 1800000 }, usage: ['city driving'] } } }),
  q({ id: 'natural-009', bucket: 'natural_language', message: 'Can you give me a quote for Verna SX IVT?', expected: { primaryTask: ACI_TASKS.QUOTATION, anchors: { model: 'Verna', variant: 'SX IVT' }, requestedFacts: ['quotation', 'lead'] } }),
  q({ id: 'natural-010', bucket: 'natural_language', message: 'What similar cars should I check if I like Creta?', expected: { primaryTask: ACI_TASKS.SIMILAR_VEHICLES, anchors: { model: 'Creta' } } }),
  q({ id: 'natural-011', bucket: 'natural_language', message: 'Which is the safest car under 15 lakh?', expected: { primaryTask: ACI_TASKS.SAFEST_VEHICLES, filters: { budget: { max: 1500000 } } } }),

  // 50–58: Hinglish
  q({ id: 'hinglish-001', bucket: 'hinglish', message: 'Punch cng mein sunroof aur abs hai?', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS'] } } }),
  q({ id: 'hinglish-002', bucket: 'hinglish', message: 'Creta automatic 20 lakh ke andar ADAS ke saath', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Creta' }, filters: { transmissions: ['automatic'], budget: { max: 2000000 }, features: ['ADAS'] } } }),
  q({ id: 'hinglish-003', bucket: 'hinglish', message: 'Verna ka emi batao 2 lakh down payment', expected: { primaryTask: ACI_TASKS.EMI_CALCULATION, anchors: { model: 'Verna' }, finance: { downPayment: 200000 } } }),
  q({ id: 'hinglish-004', bucket: 'hinglish', message: 'Black color mein Verna ka best price', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Verna' }, filters: { colors: ['black'] }, requestedFacts: ['price', 'offer'] } }),
  q({ id: 'hinglish-005', bucket: 'hinglish', message: 'Family ke liye best car under 15 lakh', expected: { primaryTask: ACI_TASKS.RECOMMENDATION, filters: { budget: { max: 1500000 }, usage: ['family'] } } }),
  q({ id: 'hinglish-006', bucket: 'hinglish', message: 'Seltos aur Creta compare karo', expected: { primaryTask: ACI_TASKS.VEHICLE_COMPARISON, comparisonTargets: [{ model: 'Seltos' }, { model: 'Creta' }] } }),
  q({ id: 'hinglish-007', bucket: 'hinglish', message: 'Creta ka sabse value for money variant kaunsa hai?', expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, anchors: { model: 'Creta' } } }),
  q({ id: 'hinglish-008', bucket: 'hinglish', message: 'Mujhe sunroof wali Hyundai cars dikhao', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Hyundai'], features: ['sunroof'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'hinglish-009', bucket: 'hinglish', message: 'Kya City ZX CVT Verna SX IVT se better hai?', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'City', variant: 'ZX CVT' }, { model: 'Verna', variant: 'SX IVT' }] } }),

  // 59–67: Typos and broken language
  q({ id: 'typo-001', bucket: 'typos', message: 'vern aprice', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Verna' }, requestedFacts: ['price'], typoToleranceExpected: true } }),
  q({ id: 'typo-002', bucket: 'typos', message: 'creta adass', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Creta' }, requestedFeatures: ['ADAS'], typoToleranceExpected: true } }),
  q({ id: 'typo-003', bucket: 'typos', message: 'puch cng snroof abs', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS'] }, typoToleranceExpected: true } }),
  q({ id: 'typo-004', bucket: 'typos', message: 'nexn ev rnage', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Nexon EV' }, requestedFeatures: ['range'], typoToleranceExpected: true } }),
  q({ id: 'typo-005', bucket: 'typos', message: 'selsot diesel atomatic', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, anchors: { model: 'Seltos' }, filters: { fuelTypes: ['diesel'], transmissions: ['automatic'] }, typoToleranceExpected: true } }),
  q({ id: 'typo-006', bucket: 'typos', message: 'hyndai cars under 20 lac', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Hyundai'], budget: { max: 2000000 } }, typoToleranceExpected: true } }),
  q({ id: 'typo-007', bucket: 'typos', message: 'varana sx ivt vs city zx cvt', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Verna', variant: 'SX IVT' }, { model: 'City', variant: 'ZX CVT' }], typoToleranceExpected: true } }),
  q({ id: 'typo-008', bucket: 'typos', message: 'cars wit sunrof', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['sunroof'] }, typoToleranceExpected: true } }),
  q({ id: 'typo-009', bucket: 'typos', message: 'safst cars under 15 lakh', expected: { primaryTask: ACI_TASKS.SAFEST_VEHICLES, filters: { budget: { max: 1500000 } }, typoToleranceExpected: true } }),

  // 68–77: Follow-up and context handling
  q({ id: 'context-001', bucket: 'context_followup', message: 'EMI with 2 lakh down payment', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.EMI_CALCULATION, anchors: { model: 'Verna', variant: 'SX IVT' }, contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT, finance: { downPayment: 200000 } } }),
  q({ id: 'context-002', bucket: 'context_followup', message: 'Does it have sunroof?', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Verna', variant: 'SX IVT' }, requestedFeatures: ['sunroof'], contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),
  q({ id: 'context-003', bucket: 'context_followup', message: 'What colors are available?', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.COLOR_LOOKUP, anchors: { model: 'Verna', variant: 'SX IVT' }, contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),
  q({ id: 'context-004', bucket: 'context_followup', message: 'Compare it with City ZX CVT', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Verna', variant: 'SX IVT' }, { model: 'City', variant: 'ZX CVT' }], contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),
  q({ id: 'context-005', bucket: 'context_switch', message: 'What about Punch CNG sunroof?', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'], features: ['sunroof'] }, contextAction: ACI_CONTEXT_ACTIONS.SWITCH_TO_EXPLICIT_ENTITY, mustNotUseOldContextVehicle: true } }),
  q({ id: 'context-006', bucket: 'context_switch', message: 'Show Seltos colors', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.COLOR_LOOKUP, anchors: { model: 'Seltos' }, contextAction: ACI_CONTEXT_ACTIONS.SWITCH_TO_EXPLICIT_ENTITY, mustNotUseOldContextVehicle: true } }),
  q({ id: 'context-007', bucket: 'context_refine', message: 'only Hyundai', activeContext: ctx.broadSunroof, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { makes: ['Hyundai'], features: ['sunroof'] }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'context-008', bucket: 'context_refine', message: 'under 20 lakh also', activeContext: ctx.broadSunroof, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['sunroof'], budget: { max: 2000000 } }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'context-009', bucket: 'context_ambiguous', message: 'price', expected: { primaryTask: ACI_TASKS.CLARIFICATION, shouldAskClarification: true, clarificationReason: 'missing_vehicle_context' } }),
  q({ id: 'context-010', bucket: 'context_ambiguous', message: 'price', activeContext: ctx.creta, expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Creta' }, contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),

  // 78–88: Comparisons and variant-level intelligence
  q({ id: 'compare-001', bucket: 'comparison', message: 'Creta vs Seltos', expected: { primaryTask: ACI_TASKS.VEHICLE_COMPARISON, comparisonTargets: [{ model: 'Creta' }, { model: 'Seltos' }] } }),
  q({ id: 'compare-002', bucket: 'comparison', message: 'Verna SX IVT vs City ZX CVT', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Verna', variant: 'SX IVT' }, { model: 'City', variant: 'ZX CVT' }] } }),
  q({ id: 'compare-003', bucket: 'comparison', message: 'Creta S(O) IVT vs Seltos HTX IVT', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Creta', variant: 'S(O) IVT' }, { model: 'Seltos', variant: 'HTX IVT' }] } }),
  q({ id: 'compare-004', bucket: 'comparison', message: 'Compare Punch and Nexon on sunroof, ABS and airbags', expected: { primaryTask: ACI_TASKS.VEHICLE_COMPARISON, comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }], requestedFeatures: ['sunroof', 'ABS', 'airbags'] } }),
  q({ id: 'compare-005', bucket: 'comparison', message: 'Which is better value Verna SX IVT or City VX CVT?', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Verna', variant: 'SX IVT' }, { model: 'City', variant: 'VX CVT' }], requestedFacts: ['value'] } }),
  q({ id: 'compare-006', bucket: 'comparison', message: 'Compare this variant with Nexon top model', activeContext: ctx.vernaSxIvt, expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Verna', variant: 'SX IVT' }, { model: 'Nexon', variantDescriptor: 'top model' }], contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),
  q({ id: 'compare-007', bucket: 'comparison', message: 'What do I gain from Creta SX compared to S(O)?', expected: { primaryTask: ACI_TASKS.VARIANT_DELTA, anchors: { model: 'Creta' }, comparisonTargets: [{ variant: 'SX' }, { variant: 'S(O)' }] } }),
  q({ id: 'compare-008', bucket: 'comparison', message: 'Seltos HTX vs Creta SX on ADAS and sunroof', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, comparisonTargets: [{ model: 'Seltos', variant: 'HTX' }, { model: 'Creta', variant: 'SX' }], requestedFeatures: ['ADAS', 'sunroof'] } }),
  q({ id: 'compare-009', bucket: 'comparison', message: 'similar cars to Honda City', expected: { primaryTask: ACI_TASKS.SIMILAR_VEHICLES, anchors: { model: 'City' } } }),
  q({ id: 'compare-010', bucket: 'comparison', message: 'cheapest variant of Seltos with ventilated seats', expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, anchors: { model: 'Seltos' }, filters: { features: ['ventilated seats'] }, sortBy: 'cheapest' } }),
  q({ id: 'compare-011', bucket: 'comparison', message: 'which Creta variant is not worth buying?', expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, anchors: { model: 'Creta' }, requestedFacts: ['value', 'avoidance'] } }),

  // 89–94: Future modules / provider-ready understanding
  q({ id: 'future-001', bucket: 'future_modules', message: 'check challan for my car', expected: { domains: [ACI_DOMAINS.CHALLAN], primaryTask: ACI_TASKS.CHALLAN_LOOKUP, safety: { requiresConsent: true }, requiredProviders: ['challan'] } }),
  q({ id: 'future-002', bucket: 'future_modules', message: 'check RC details for DL01AB1234', expected: { domains: [ACI_DOMAINS.RC], primaryTask: ACI_TASKS.RC_LOOKUP, safety: { requiresConsent: true }, requiredProviders: ['rc'] } }),
  q({ id: 'future-003', bucket: 'future_modules', message: 'insurance quote for Verna SX IVT', expected: { domains: [ACI_DOMAINS.INSURANCE], primaryTask: ACI_TASKS.INSURANCE_QUOTE, anchors: { model: 'Verna', variant: 'SX IVT' }, safety: { requiresConsent: true } } }),
  q({ id: 'future-004', bucket: 'future_modules', message: 'service cost of Creta diesel', expected: { domains: [ACI_DOMAINS.SERVICE], primaryTask: ACI_TASKS.SERVICE_COST, anchors: { model: 'Creta' }, filters: { fuelTypes: ['diesel'] } } }),
  q({ id: 'future-005', bucket: 'future_modules', message: 'total ownership cost of Seltos automatic for 5 years', expected: { domains: [ACI_DOMAINS.OWNERSHIP], primaryTask: ACI_TASKS.TCO_ESTIMATE, anchors: { model: 'Seltos' }, filters: { transmissions: ['automatic'] }, ownership: { years: 5 } } }),
  q({ id: 'future-006', bucket: 'future_modules', message: 'sell my old car and buy Creta', expected: { domains: [ACI_DOMAINS.USED_CAR, ACI_DOMAINS.NEW_CAR], primaryTask: ACI_TASKS.EXCHANGE_VALUATION, anchors: { model: 'Creta' }, requestedFacts: ['exchange', 'lead'] } }),

  // 95–100: Hallucination traps / unsupported / ambiguity
  q({ id: 'trap-001', bucket: 'hallucination_traps', message: 'Does Alto have ADAS?', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Alto' }, requestedFeatures: ['ADAS'], mustUseDbValidation: true } }),
  q({ id: 'trap-002', bucket: 'hallucination_traps', message: 'Does WagonR have panoramic sunroof?', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'WagonR' }, requestedFeatures: ['panoramic sunroof'], mustUseDbValidation: true } }),
  q({ id: 'trap-003', bucket: 'hallucination_traps', message: 'Show Ferrari price in Delhi', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Ferrari', city: DEFAULT_CITY }, mustUseDbValidation: true, shouldNotInventUnavailableCatalogData: true } }),
  q({ id: 'trap-004', bucket: 'hallucination_traps', message: 'Does Punch have diesel automatic?', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Punch' }, filters: { fuelTypes: ['diesel'], transmissions: ['automatic'] }, mustUseDbValidation: true } }),
  q({ id: 'trap-005', bucket: 'ambiguous', message: 'range', expected: { primaryTask: ACI_TASKS.CLARIFICATION, shouldAskClarification: true, clarificationReason: 'ambiguous_without_vehicle_context' } }),
  q({ id: 'trap-006', bucket: 'off_topic', message: 'book me a hotel', expected: { domains: [ACI_DOMAINS.GENERAL], primaryTask: ACI_TASKS.UNSUPPORTED, shouldAskClarification: false, unsupportedReason: 'outside_automotive_commerce_scope' } }),
];

export const getUnderstandingCorpusSummary = () => {
  const buckets = ACI_UNDERSTANDING_CORPUS_V1.reduce((acc, item) => {
    acc[item.bucket] = (acc[item.bucket] || 0) + 1;
    return acc;
  }, {});

  return {
    version: 'aciUnderstandingCorpus.v1',
    total: ACI_UNDERSTANDING_CORPUS_V1.length,
    buckets,
  };
};

export default ACI_UNDERSTANDING_CORPUS_V1;
