import {
  ACI_CONTEXT_ACTIONS,
  ACI_DOMAINS,
  ACI_TASKS,
} from '../../../services/aciCore/understanding/aciMeaningFrame.schema.js';

/**
 * ACI Understanding Corpus Extended V1
 *
 * Adds wider buyer, ownership, provider, and boundary questions.
 * Excludes test-drive by product decision.
 */

const DEFAULT_CITY = 'Delhi';

const ctx = {
  cretaDiscovery: {
    lastDiscovery: {
      task: ACI_TASKS.VEHICLE_DISCOVERY,
      filters: { model: 'Creta' },
    },
    city: DEFAULT_CITY,
  },
  sunroofDiscovery: {
    lastDiscovery: {
      task: ACI_TASKS.VEHICLE_DISCOVERY,
      filters: { features: ['sunroof'] },
    },
    city: DEFAULT_CITY,
  },
  creta: {
    selectedVehicle: { make: 'Hyundai', model: 'Creta', variant: null },
    city: DEFAULT_CITY,
  },
};

const q = ({
  id,
  bucket,
  message,
  activeContext = null,
  expected,
  notes = '',
}) => ({
  id,
  bucket,
  message,
  activeContext,
  expected: {
    domains: [ACI_DOMAINS.NEW_CAR],
    shouldAskClarification: false,
    mustNotHallucinateFacts: true,
    ...expected,
  },
  notes,
});

export const ACI_UNDERSTANDING_CORPUS_EXTENDED_V1 = [
  // on-road price
  q({ id: 'onroad-001', bucket: 'onroad_price', message: 'Creta SX IVT on-road price Delhi', expected: { primaryTask: ACI_TASKS.ON_ROAD_ESTIMATE, anchors: { model: 'Creta', variant: 'SX IVT', city: DEFAULT_CITY }, requestedFacts: ['onRoad', 'price'] } }),
  q({ id: 'onroad-002', bucket: 'onroad_price', message: 'Verna on-road price', expected: { primaryTask: ACI_TASKS.ON_ROAD_ESTIMATE, anchors: { model: 'Verna' }, requestedFacts: ['onRoad'] } }),
  q({ id: 'onroad-003', bucket: 'onroad_price', message: 'Nexon EV total on-road price in Delhi', expected: { primaryTask: ACI_TASKS.ON_ROAD_ESTIMATE, anchors: { model: 'Nexon EV', city: DEFAULT_CITY }, requestedFacts: ['onRoad', 'priceBreakdown'] } }),
  q({ id: 'onroad-004', bucket: 'onroad_price', message: 'Kitne ka padega Creta Delhi mein on-road?', expected: { primaryTask: ACI_TASKS.ON_ROAD_ESTIMATE, anchors: { model: 'Creta', city: DEFAULT_CITY }, requestedFacts: ['onRoad'] } }),
  q({ id: 'onroad-005', bucket: 'onroad_price', message: 'Punch CNG on-road kitna hoga?', expected: { primaryTask: ACI_TASKS.ON_ROAD_ESTIMATE, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'] }, requestedFacts: ['onRoad'] } }),

  // price breakdown
  q({ id: 'breakdown-001', bucket: 'price_breakdown', message: 'What is ex-showroom price of Verna SX IVT?', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Verna', variant: 'SX IVT' }, requestedFacts: ['exShowroomPrice'] } }),
  q({ id: 'breakdown-002', bucket: 'price_breakdown', message: 'Road tax on Creta in Delhi', expected: { primaryTask: ACI_TASKS.PRICE_BREAKDOWN, anchors: { model: 'Creta', city: DEFAULT_CITY }, requestedFacts: ['roadTax'] } }),
  q({ id: 'breakdown-003', bucket: 'price_breakdown', message: 'Insurance cost for Nexon EV', expected: { primaryTask: ACI_TASKS.PRICE_BREAKDOWN, anchors: { model: 'Nexon EV' }, requestedFacts: ['insuranceCost'] } }),
  q({ id: 'breakdown-004', bucket: 'price_breakdown', message: 'Registration charges for Seltos in Delhi', expected: { primaryTask: ACI_TASKS.PRICE_BREAKDOWN, anchors: { model: 'Seltos', city: DEFAULT_CITY }, requestedFacts: ['registrationCharges'] } }),
  q({ id: 'breakdown-005', bucket: 'price_breakdown', message: 'TCS on EQS', expected: { primaryTask: ACI_TASKS.PRICE_BREAKDOWN, anchors: { model: 'EQS' }, requestedFacts: ['tcs'] } }),
  q({ id: 'breakdown-006', bucket: 'price_breakdown', message: 'What is the total cost breakdown for Thar diesel?', expected: { primaryTask: ACI_TASKS.PRICE_BREAKDOWN, anchors: { model: 'Thar' }, filters: { fuelTypes: ['diesel'] }, requestedFacts: ['priceBreakdown'] } }),

  // offers
  q({ id: 'offers-001', bucket: 'offers_discounts', message: 'Current offers on Creta', expected: { primaryTask: ACI_TASKS.OFFER_LOOKUP, anchors: { model: 'Creta' }, requestedFacts: ['offers'] } }),
  q({ id: 'offers-002', bucket: 'offers_discounts', message: 'Cash discount on Verna this month', expected: { primaryTask: ACI_TASKS.OFFER_LOOKUP, anchors: { model: 'Verna' }, requestedFacts: ['cashDiscount'], temporal: 'this_month' } }),
  q({ id: 'offers-003', bucket: 'offers_discounts', message: 'Any exchange bonus on Seltos?', expected: { primaryTask: ACI_TASKS.OFFER_LOOKUP, anchors: { model: 'Seltos' }, requestedFacts: ['exchangeBonus'] } }),
  q({ id: 'offers-004', bucket: 'offers_discounts', message: 'Corporate discount on i20', expected: { primaryTask: ACI_TASKS.OFFER_LOOKUP, anchors: { model: 'i20' }, requestedFacts: ['corporateDiscount'] } }),
  q({ id: 'offers-005', bucket: 'offers_discounts', message: 'Hyundai festival offers', expected: { primaryTask: ACI_TASKS.OFFER_LOOKUP, filters: { makes: ['Hyundai'] }, requestedFacts: ['festivalOffers'] } }),
  q({ id: 'offers-006', bucket: 'offers_discounts', message: 'Kia Sonet discount is kya hai?', expected: { primaryTask: ACI_TASKS.OFFER_LOOKUP, anchors: { make: 'Kia', model: 'Sonet' }, requestedFacts: ['discounts'] } }),

  // waiting period
  q({ id: 'waiting-001', bucket: 'waiting_period', message: 'Creta waiting period in Delhi', expected: { primaryTask: ACI_TASKS.WAITING_PERIOD, anchors: { model: 'Creta', city: DEFAULT_CITY }, requestedFacts: ['waitingPeriod'] } }),
  q({ id: 'waiting-002', bucket: 'waiting_period', message: 'Verna SX IVT delivery time', expected: { primaryTask: ACI_TASKS.WAITING_PERIOD, anchors: { model: 'Verna', variant: 'SX IVT' }, requestedFacts: ['deliveryTime'] } }),
  q({ id: 'waiting-003', bucket: 'waiting_period', message: 'How long is the wait for Thar diesel 4x4?', expected: { primaryTask: ACI_TASKS.WAITING_PERIOD, anchors: { model: 'Thar' }, filters: { fuelTypes: ['diesel'], features: ['4x4'] } } }),
  q({ id: 'waiting-004', bucket: 'waiting_period', message: 'Punch CNG kitne din mein milegi?', expected: { primaryTask: ACI_TASKS.WAITING_PERIOD, anchors: { model: 'Punch' }, filters: { fuelTypes: ['CNG'] } } }),
  q({ id: 'waiting-005', bucket: 'waiting_period', message: 'Immediate delivery cars under 15 lakh', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { budget: { max: 1500000 }, availability: ['immediate_delivery'] }, requestedFacts: ['availability'] } }),
  q({ id: 'waiting-006', bucket: 'waiting_period', message: 'Which SUVs have no waiting period?', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { bodyTypes: ['SUV'], availability: ['no_waiting_period'] }, requestedFacts: ['availability'] } }),

  // upcoming launches / model year
  q({ id: 'launch-001', bucket: 'upcoming_launches', message: 'When is the new Creta coming?', expected: { primaryTask: ACI_TASKS.UPCOMING_LAUNCHES, anchors: { model: 'Creta' }, requestedFacts: ['launchTimeline'] } }),
  q({ id: 'launch-002', bucket: 'upcoming_launches', message: 'Upcoming Hyundai cars 2025', expected: { primaryTask: ACI_TASKS.UPCOMING_LAUNCHES, filters: { makes: ['Hyundai'] }, requestedFacts: ['upcomingCars'] } }),
  q({ id: 'launch-003', bucket: 'upcoming_launches', message: 'New cars launching this month in India', expected: { primaryTask: ACI_TASKS.UPCOMING_LAUNCHES, requestedFacts: ['launches'], temporal: 'this_month' } }),
  q({ id: 'launch-004', bucket: 'upcoming_launches', message: 'Expected price of upcoming Seltos facelift', expected: { primaryTask: ACI_TASKS.UPCOMING_LAUNCHES, anchors: { model: 'Seltos' }, requestedFacts: ['expectedPrice'], mustUseDbValidation: true } }),
  q({ id: 'modelyear-001', bucket: 'model_year_comparison', message: '2024 vs 2025 Creta difference', expected: { primaryTask: ACI_TASKS.MODEL_YEAR_COMPARISON, anchors: { model: 'Creta' }, comparisonTargets: [{ year: 2024 }, { year: 2025 }] } }),
  q({ id: 'modelyear-002', bucket: 'model_year_comparison', message: 'What changed in new Verna compared to old?', expected: { primaryTask: ACI_TASKS.MODEL_YEAR_COMPARISON, anchors: { model: 'Verna' }, requestedFacts: ['changes'] } }),
  q({ id: 'modelyear-003', bucket: 'model_year_comparison', message: 'Facelift changes in i20', expected: { primaryTask: ACI_TASKS.MODEL_YEAR_COMPARISON, anchors: { model: 'i20' }, requestedFacts: ['faceliftChanges'] } }),

  // resale / running / ownership
  q({ id: 'resale-001', bucket: 'resale_value', message: 'Creta resale value after 5 years', expected: { primaryTask: ACI_TASKS.RESALE_VALUE, anchors: { model: 'Creta' }, ownership: { years: 5 } } }),
  q({ id: 'resale-002', bucket: 'resale_value', message: 'Which car has best resale value under 15 lakh?', expected: { primaryTask: ACI_TASKS.RESALE_VALUE, filters: { budget: { max: 1500000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'resale-003', bucket: 'resale_value', message: 'Verna vs City resale comparison', expected: { primaryTask: ACI_TASKS.RESALE_VALUE, comparisonTargets: [{ model: 'Verna' }, { model: 'City' }] } }),
  q({ id: 'running-001', bucket: 'running_cost', message: 'Creta petrol vs diesel running cost', expected: { primaryTask: ACI_TASKS.RUNNING_COST, anchors: { model: 'Creta' }, comparisonTargets: [{ fuel: 'petrol' }, { fuel: 'diesel' }] } }),
  q({ id: 'running-002', bucket: 'running_cost', message: 'Nexon EV monthly running cost in Delhi', expected: { primaryTask: ACI_TASKS.RUNNING_COST, anchors: { model: 'Nexon EV', city: DEFAULT_CITY }, requestedFacts: ['monthlyRunningCost'] } }),
  q({ id: 'running-003', bucket: 'running_cost', message: 'Is electric car cheaper to run than petrol?', expected: { primaryTask: ACI_TASKS.RUNNING_COST, comparisonTargets: [{ fuel: 'electric' }, { fuel: 'petrol' }] } }),
  q({ id: 'ownership-001', bucket: 'ownership_cost_total', message: 'Total cost of owning Creta petrol for 5 years', expected: { domains: [ACI_DOMAINS.OWNERSHIP], primaryTask: ACI_TASKS.TCO_ESTIMATE, anchors: { model: 'Creta' }, filters: { fuelTypes: ['petrol'] }, ownership: { years: 5 } } }),
  q({ id: 'ownership-002', bucket: 'ownership_cost_total', message: 'Which is cheaper to own, Verna or City over 3 years?', expected: { domains: [ACI_DOMAINS.OWNERSHIP], primaryTask: ACI_TASKS.TCO_ESTIMATE, comparisonTargets: [{ model: 'Verna' }, { model: 'City' }], ownership: { years: 3 } } }),
  q({ id: 'tcovs-001', bucket: 'tco_vs_tco', message: 'Compare total ownership cost of Nexon EV and Creta petrol', expected: { domains: [ACI_DOMAINS.OWNERSHIP], primaryTask: ACI_TASKS.TCO_ESTIMATE, comparisonTargets: [{ model: 'Nexon EV' }, { model: 'Creta', fuel: 'petrol' }] } }),

  // reliability / safety / rivals
  q({ id: 'reliability-001', bucket: 'reliability_ownership', message: 'Is Verna reliable?', expected: { primaryTask: ACI_TASKS.RELIABILITY_OWNERSHIP, anchors: { model: 'Verna' }, requestedFacts: ['reliability'] } }),
  q({ id: 'reliability-002', bucket: 'reliability_ownership', message: 'Common problems in Creta', expected: { primaryTask: ACI_TASKS.RELIABILITY_OWNERSHIP, anchors: { model: 'Creta' }, requestedFacts: ['commonProblems'] } }),
  q({ id: 'reliability-003', bucket: 'reliability_ownership', message: 'Which is the most low-maintenance car under 12 lakh?', expected: { primaryTask: ACI_TASKS.RELIABILITY_OWNERSHIP, filters: { budget: { max: 1200000 } }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'safetyrating-001', bucket: 'ncap_safety_rating', message: 'Nexon NCAP rating', expected: { primaryTask: ACI_TASKS.SAFETY_RATING, anchors: { model: 'Nexon' }, requestedFacts: ['ncapRating'] } }),
  q({ id: 'safetyrating-002', bucket: 'ncap_safety_rating', message: '5-star NCAP cars under 15 lakh', expected: { primaryTask: ACI_TASKS.SAFETY_RATING, filters: { budget: { max: 1500000 }, safetyRatings: ['5-star'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'safetyrating-003', bucket: 'ncap_safety_rating', message: 'Bharat NCAP rated cars', expected: { primaryTask: ACI_TASKS.SAFETY_RATING, filters: { ratingAgency: ['Bharat NCAP'] }, discovery: { isBroadDiscovery: true } } }),
  q({ id: 'rivals-001', bucket: 'rivals_alternatives', message: 'Creta rivals', expected: { primaryTask: ACI_TASKS.RIVALS_ALTERNATIVES, anchors: { model: 'Creta' } } }),
  q({ id: 'rivals-002', bucket: 'rivals_alternatives', message: 'Cars similar to Verna', expected: { primaryTask: ACI_TASKS.SIMILAR_VEHICLES, anchors: { model: 'Verna' } } }),
  q({ id: 'rivals-003', bucket: 'rivals_alternatives', message: 'Punch ke jaisi aur kaunsi car hai?', expected: { primaryTask: ACI_TASKS.SIMILAR_VEHICLES, anchors: { model: 'Punch' } } }),

  // dealer / accessories / variant navigation / color specific
  q({ id: 'dealer-001', bucket: 'dealer_locator', message: 'Hyundai dealer in Delhi', expected: { primaryTask: ACI_TASKS.DEALER_LOCATOR, filters: { makes: ['Hyundai'], city: DEFAULT_CITY }, requestedFacts: ['dealerLocation'], requiredProviders: ['dealerData'] } }),
  q({ id: 'dealer-002', bucket: 'dealer_locator', message: 'Nearest Kia showroom to me', expected: { primaryTask: ACI_TASKS.DEALER_LOCATOR, filters: { makes: ['Kia'] }, requestedFacts: ['nearestDealer'], requiresLocation: true } }),
  q({ id: 'dealer-003', bucket: 'dealer_locator', message: 'Where to buy Seltos in Noida?', expected: { primaryTask: ACI_TASKS.DEALER_LOCATOR, anchors: { model: 'Seltos', city: 'Noida' }, requestedFacts: ['dealerLocation'] } }),
  q({ id: 'accessory-001', bucket: 'accessories', message: 'What accessories come with Creta SX(O)?', expected: { primaryTask: ACI_TASKS.ACCESSORY_LOOKUP, anchors: { model: 'Creta', variant: 'SX(O)' }, requestedFacts: ['accessories'] } }),
  q({ id: 'accessory-002', bucket: 'accessories', message: 'Can I add sunroof to Punch?', expected: { primaryTask: ACI_TASKS.ACCESSORY_LOOKUP, anchors: { model: 'Punch' }, requestedFeatures: ['sunroof'], requestedFacts: ['accessoryFeasibility'], mustUseDbValidation: true } }),
  q({ id: 'accessory-003', bucket: 'accessories', message: 'Aftermarket vs OEM accessories for i20', expected: { primaryTask: ACI_TASKS.ACCESSORY_LOOKUP, anchors: { model: 'i20' }, comparisonTargets: [{ accessoryType: 'aftermarket' }, { accessoryType: 'OEM' }] } }),
  q({ id: 'variantnav-001', bucket: 'variant_navigation', message: 'Top model Creta price', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Creta', variantDescriptor: 'top model' }, requestedFacts: ['price'] } }),
  q({ id: 'variantnav-002', bucket: 'variant_navigation', message: 'Base model Verna price', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Verna', variantDescriptor: 'base model' }, requestedFacts: ['price'] } }),
  q({ id: 'variantnav-003', bucket: 'variant_navigation', message: 'Which Punch variants have automatic?', expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, anchors: { model: 'Punch' }, filters: { transmissions: ['automatic'] }, discovery: { resultGranularity: 'variant' } } }),
  q({ id: 'variantnav-004', bucket: 'variant_navigation', message: 'Which Verna variant is just below SX(O)?', expected: { primaryTask: ACI_TASKS.VARIANT_DELTA, anchors: { model: 'Verna', variant: 'SX(O)' }, requestedFacts: ['adjacentVariantBelow'] } }),
  q({ id: 'variantnav-005', bucket: 'variant_navigation', message: 'Difference between Creta SX and SX(O)', expected: { primaryTask: ACI_TASKS.VARIANT_DELTA, anchors: { model: 'Creta' }, comparisonTargets: [{ variant: 'SX' }, { variant: 'SX(O)' }] } }),
  q({ id: 'color-specific-001', bucket: 'color_specific', message: 'Is Fiery Red available in Verna SX IVT?', expected: { primaryTask: ACI_TASKS.COLOR_LOOKUP, anchors: { model: 'Verna', variant: 'SX IVT' }, filters: { colors: ['Fiery Red'] } } }),
  q({ id: 'color-specific-002', bucket: 'color_specific', message: 'What is the price of Creta in Atlas White?', expected: { primaryTask: ACI_TASKS.PRICE_LOOKUP, anchors: { model: 'Creta' }, filters: { colors: ['Atlas White'] }, requestedFacts: ['price'] } }),
  q({ id: 'color-specific-003', bucket: 'color_specific', message: 'Dual tone color options in Seltos', expected: { primaryTask: ACI_TASKS.COLOR_LOOKUP, anchors: { model: 'Seltos' }, filters: { colors: ['dual tone'] } } }),

  // fuel/finance/multi-city/exchange/new-used/warranty
  q({ id: 'fueladvice-001', bucket: 'fuel_type_advice', message: 'Should I buy petrol or diesel Creta?', expected: { primaryTask: ACI_TASKS.FUEL_TYPE_ADVICE, anchors: { model: 'Creta' }, comparisonTargets: [{ fuel: 'petrol' }, { fuel: 'diesel' }] } }),
  q({ id: 'fueladvice-002', bucket: 'fuel_type_advice', message: 'Diesel ya petrol Verna for 1500 km monthly?', expected: { primaryTask: ACI_TASKS.FUEL_TYPE_ADVICE, anchors: { model: 'Verna' }, usage: { monthlyKm: 1500 }, comparisonTargets: [{ fuel: 'diesel' }, { fuel: 'petrol' }] } }),
  q({ id: 'fueladvice-003', bucket: 'fuel_type_advice', message: 'CNG vs electric which is better for budget buyers?', expected: { primaryTask: ACI_TASKS.FUEL_TYPE_ADVICE, comparisonTargets: [{ fuel: 'CNG' }, { fuel: 'electric' }], usage: ['budget_buyer'] } }),
  q({ id: 'finance-deep-001', bucket: 'finance_deep', message: 'Creta SX IVT EMI for 5 years at 8.5%', expected: { primaryTask: ACI_TASKS.EMI_CALCULATION, anchors: { model: 'Creta', variant: 'SX IVT' }, finance: { tenureYears: 5, interestRate: 8.5 } } }),
  q({ id: 'finance-deep-002', bucket: 'finance_deep', message: 'What is the minimum down payment for Verna?', expected: { primaryTask: ACI_TASKS.FINANCE_ELIGIBILITY, anchors: { model: 'Verna' }, requestedFacts: ['minimumDownPayment'] } }),
  q({ id: 'finance-deep-003', bucket: 'finance_deep', message: 'HDFC vs SBI car loan interest rate', expected: { domains: [ACI_DOMAINS.FINANCE], primaryTask: ACI_TASKS.FINANCE_ELIGIBILITY, comparisonTargets: [{ lender: 'HDFC' }, { lender: 'SBI' }], requestedFacts: ['interestRate'] } }),
  q({ id: 'multi-city-001', bucket: 'multi_city_price', message: 'Creta price difference between Delhi and Mumbai', expected: { primaryTask: ACI_TASKS.MULTI_CITY_PRICE, anchors: { model: 'Creta' }, comparisonTargets: [{ city: 'Delhi' }, { city: 'Mumbai' }] } }),
  q({ id: 'multi-city-002', bucket: 'multi_city_price', message: 'Why is car price different in different cities?', expected: { primaryTask: ACI_TASKS.CONTENT_EXPLAINER, requestedFacts: ['cityPriceDifferenceExplanation'] } }),
  q({ id: 'exchange-001', bucket: 'exchange_trade_in', message: 'I have a WagonR 2019, what exchange value will I get?', expected: { domains: [ACI_DOMAINS.USED_CAR], primaryTask: ACI_TASKS.EXCHANGE_VALUATION, tradeInVehicle: { model: 'WagonR', year: 2019 }, safety: { requiresConsent: true } } }),
  q({ id: 'exchange-002', bucket: 'exchange_trade_in', message: 'Exchange my Swift for Creta', expected: { domains: [ACI_DOMAINS.USED_CAR, ACI_DOMAINS.NEW_CAR], primaryTask: ACI_TASKS.EXCHANGE_VALUATION, anchors: { model: 'Creta' }, tradeInVehicle: { model: 'Swift' } } }),
  q({ id: 'newused-001', bucket: 'new_vs_used', message: 'Should I buy new Creta or used one?', expected: { domains: [ACI_DOMAINS.NEW_CAR, ACI_DOMAINS.USED_CAR], primaryTask: ACI_TASKS.NEW_VS_USED, anchors: { model: 'Creta' } } }),
  q({ id: 'newused-002', bucket: 'new_vs_used', message: 'Used Seltos vs new Sonet which is better?', expected: { domains: [ACI_DOMAINS.NEW_CAR, ACI_DOMAINS.USED_CAR], primaryTask: ACI_TASKS.NEW_VS_USED, comparisonTargets: [{ model: 'Seltos', condition: 'used' }, { model: 'Sonet', condition: 'new' }] } }),
  q({ id: 'warranty-001', bucket: 'warranty_service', message: 'What is the warranty on Verna?', expected: { primaryTask: ACI_TASKS.WARRANTY_LOOKUP, anchors: { model: 'Verna' }, requestedFacts: ['warranty'] } }),
  q({ id: 'warranty-002', bucket: 'warranty_service', message: 'Extended warranty available for Creta?', expected: { primaryTask: ACI_TASKS.WARRANTY_LOOKUP, anchors: { model: 'Creta' }, requestedFacts: ['extendedWarranty'] } }),
  q({ id: 'warranty-003', bucket: 'warranty_service', message: 'Kia service interval for Seltos', expected: { domains: [ACI_DOMAINS.SERVICE], primaryTask: ACI_TASKS.SERVICE_COST, anchors: { make: 'Kia', model: 'Seltos' }, requestedFacts: ['serviceInterval'] } }),

  // extended Hinglish/refinements/typos/traps/boundaries
  q({ id: 'hinglish-ext-001', bucket: 'hinglish_extended', message: 'Yeh car ka highway mileage kitna hai?', activeContext: ctx.creta, expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Creta' }, requestedFeatures: ['highway mileage'], contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),
  q({ id: 'hinglish-ext-002', bucket: 'hinglish_extended', message: 'Kaun sa variant lena chahiye 12 lakh mein?', expected: { primaryTask: ACI_TASKS.RECOMMENDATION, filters: { budget: { max: 1200000 } } } }),
  q({ id: 'hinglish-ext-003', bucket: 'hinglish_extended', message: 'Same but diesel wali dikhao', activeContext: ctx.cretaDiscovery, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { fuelTypes: ['diesel'] }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'hinglish-ext-004', bucket: 'hinglish_extended', message: 'Petrol wala nahi chahiye', activeContext: ctx.cretaDiscovery, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, constraints: { excludeFuelTypes: ['petrol'] }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'hinglish-ext-005', bucket: 'hinglish_extended', message: 'Thoda sasta option hai kya isme?', activeContext: ctx.creta, expected: { primaryTask: ACI_TASKS.VALUE_VARIANT, anchors: { model: 'Creta' }, sortBy: 'cheaper', contextAction: ACI_CONTEXT_ACTIONS.USE_EXISTING_CONTEXT } }),
  q({ id: 'context-refine-ext-001', bucket: 'context_refine_extended', message: 'same but diesel', activeContext: ctx.cretaDiscovery, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { fuelTypes: ['diesel'] }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'context-refine-ext-002', bucket: 'context_refine_extended', message: 'with sunroof only', activeContext: ctx.cretaDiscovery, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['sunroof'] }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'context-refine-ext-003', bucket: 'context_refine_extended', message: 'under 15 lakh only', activeContext: ctx.sunroofDiscovery, expected: { primaryTask: ACI_TASKS.VEHICLE_DISCOVERY, filters: { features: ['sunroof'], budget: { max: 1500000 } }, contextAction: ACI_CONTEXT_ACTIONS.REFINE_EXISTING_CONTEXT } }),
  q({ id: 'typos-ext-001', bucket: 'typos_extended', message: 'crta sx ivt onrod price', expected: { primaryTask: ACI_TASKS.ON_ROAD_ESTIMATE, anchors: { model: 'Creta', variant: 'SX IVT' }, typoToleranceExpected: true } }),
  q({ id: 'typos-ext-002', bucket: 'typos_extended', message: 'seltos deisel atomatic sunrof', expected: { primaryTask: ACI_TASKS.FEATURE_FILTER, anchors: { model: 'Seltos' }, filters: { fuelTypes: ['diesel'], transmissions: ['automatic'], features: ['sunroof'] }, typoToleranceExpected: true } }),
  q({ id: 'typos-ext-003', bucket: 'typos_extended', message: 'verna sx ivt waitting period', expected: { primaryTask: ACI_TASKS.WAITING_PERIOD, anchors: { model: 'Verna', variant: 'SX IVT' }, typoToleranceExpected: true } }),
  q({ id: 'trap-ext-001', bucket: 'hallucination_traps_extended', message: 'Does Creta have a V6 engine?', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'Creta' }, requestedFeatures: ['V6 engine'], mustUseDbValidation: true } }),
  q({ id: 'trap-ext-002', bucket: 'hallucination_traps_extended', message: 'What is the price of upcoming Creta EV?', expected: { primaryTask: ACI_TASKS.UPCOMING_LAUNCHES, anchors: { model: 'Creta EV' }, requestedFacts: ['expectedPrice'], mustUseDbValidation: true, shouldNotInventUnavailableCatalogData: true } }),
  q({ id: 'trap-ext-003', bucket: 'hallucination_traps_extended', message: 'Does City have AWD?', expected: { primaryTask: ACI_TASKS.FEATURE_ANSWER, anchors: { model: 'City' }, requestedFeatures: ['AWD'], mustUseDbValidation: true } }),
  q({ id: 'trap-ext-004', bucket: 'hallucination_traps_extended', message: 'Nexon ZX+ vs Nexon Creative+', expected: { primaryTask: ACI_TASKS.VARIANT_COMPARISON, anchors: { model: 'Nexon' }, comparisonTargets: [{ variant: 'ZX+' }, { variant: 'Creative+' }], mustUseDbValidation: true } }),
  q({ id: 'boundary-001', bucket: 'off_topic_boundary', message: 'Show me used cars', expected: { domains: [ACI_DOMAINS.USED_CAR], primaryTask: ACI_TASKS.NEW_VS_USED, futureCapability: true } }),
  q({ id: 'boundary-002', bucket: 'off_topic_boundary', message: 'Book me an Uber', expected: { domains: [ACI_DOMAINS.GENERAL], primaryTask: ACI_TASKS.UNSUPPORTED, unsupportedReason: 'outside_automotive_commerce_scope' } }),
  q({ id: 'boundary-003', bucket: 'off_topic_boundary', message: 'What is petrol price today in Delhi?', expected: { domains: [ACI_DOMAINS.GENERAL], primaryTask: ACI_TASKS.UNSUPPORTED, unsupportedReason: 'adjacent_but_unsupported_live_commodity_price' } }),
  q({ id: 'boundary-004', bucket: 'off_topic_boundary', message: 'Apply for driving license', expected: { domains: [ACI_DOMAINS.GENERAL], primaryTask: ACI_TASKS.UNSUPPORTED, unsupportedReason: 'outside_current_aci_scope' } }),
  // extreme multi-vehicle / multi-filter / multi-feature compressed queries
  q({
    id: 'extreme-multi-001',
    bucket: 'multi_intent_extreme',
    message: 'Punch CNG and Nexon CNG sunroof ABS ADAS',
    expected: {
      primaryTask: ACI_TASKS.VEHICLE_COMPARISON,
      comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }],
      filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS', 'ADAS'] },
      requestedFacts: ['features'],
      semanticGroup: 'punch_nexon_cng_sunroof_abs_adas',
      mustNotSetVariantFromFeatureWords: true,
      mustUseDbValidation: true,
    },
  }),
  q({
    id: 'extreme-multi-002',
    bucket: 'multi_intent_extreme',
    message: 'Punch and Nexon CNG sunroof ABS ADAS',
    expected: {
      primaryTask: ACI_TASKS.VEHICLE_COMPARISON,
      comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }],
      filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS', 'ADAS'] },
      requestedFacts: ['features'],
      semanticGroup: 'punch_nexon_cng_sunroof_abs_adas',
      mustHaveSameMeaningAs: 'extreme-multi-001',
      mustNotSetVariantFromFeatureWords: true,
      mustUseDbValidation: true,
    },
  }),
  q({
    id: 'extreme-multi-003',
    bucket: 'multi_intent_extreme',
    message: 'Compare Punch CNG and Nexon CNG on sunroof ABS ADAS and price',
    expected: {
      primaryTask: ACI_TASKS.VEHICLE_COMPARISON,
      comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }],
      filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS', 'ADAS'] },
      requestedFacts: ['features', 'price', 'comparison'],
      semanticGroup: 'punch_nexon_cng_sunroof_abs_adas_price',
      mustNotSetVariantFromFeatureWords: true,
      mustUseDbValidation: true,
    },
  }),
  q({
    id: 'extreme-multi-004',
    bucket: 'multi_intent_extreme',
    message: 'Between Punch CNG and Nexon CNG which has sunroof ABS ADAS and lower EMI',
    expected: {
      primaryTask: ACI_TASKS.VEHICLE_COMPARISON,
      comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }],
      filters: { fuelTypes: ['CNG'], features: ['sunroof', 'ABS', 'ADAS'] },
      requestedFacts: ['features', 'emi', 'value'],
      semanticGroup: 'punch_nexon_cng_sunroof_abs_adas_emi',
      mustNotSetVariantFromFeatureWords: true,
      mustUseDbValidation: true,
    },
  }),
  q({
    id: 'extreme-multi-005',
    bucket: 'multi_intent_extreme',
    message: 'Show CNG variants of Punch and Nexon with sunroof ABS ADAS under 15 lakh',
    expected: {
      primaryTask: ACI_TASKS.FEATURE_FILTER,
      comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }],
      filters: {
        fuelTypes: ['CNG'],
        features: ['sunroof', 'ABS', 'ADAS'],
        budget: { max: 1500000 },
      },
      requestedFacts: ['features', 'price'],
      semanticGroup: 'punch_nexon_cng_sunroof_abs_adas_budget',
      mustNotSetVariantFromFeatureWords: true,
      mustUseDbValidation: true,
    },
  }),
  q({
    id: 'extreme-multi-006',
    bucket: 'multi_intent_extreme',
    message: 'Punch Nexon Sonet CNG sunroof ABS ADAS automatic under 15 lakh',
    expected: {
      primaryTask: ACI_TASKS.FEATURE_FILTER,
      comparisonTargets: [{ model: 'Punch' }, { model: 'Nexon' }, { model: 'Sonet' }],
      filters: {
        fuelTypes: ['CNG'],
        transmissions: ['automatic'],
        features: ['sunroof', 'ABS', 'ADAS'],
        budget: { max: 1500000 },
      },
      requestedFacts: ['features', 'price'],
      semanticGroup: 'three_car_cng_feature_budget_filter',
      mustNotSetVariantFromFeatureWords: true,
      mustUseDbValidation: true,
    },
  }),
];

export const getUnderstandingCorpusExtendedSummary = () => {
  const buckets = ACI_UNDERSTANDING_CORPUS_EXTENDED_V1.reduce((acc, item) => {
    acc[item.bucket] = (acc[item.bucket] || 0) + 1;
    return acc;
  }, {});

  return {
    version: 'aciUnderstandingCorpus.extendedV1',
    total: ACI_UNDERSTANDING_CORPUS_EXTENDED_V1.length,
    buckets,
  };
};

export default ACI_UNDERSTANDING_CORPUS_EXTENDED_V1;
