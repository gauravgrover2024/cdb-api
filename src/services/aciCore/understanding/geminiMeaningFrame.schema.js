'use strict';

/**
 * Simplified Zod schema for Gemini meaning-frame structured output.
 *
 * Gemini structured output can reject deeply constrained schemas with many enums.
 * So this schema stays intentionally light.
 *
 * Full validation/normalization happens in geminiMeaningFrame.repair.js
 * and aciMeaningFrame.schema.js.
 */

import { z } from 'zod';

const NullableString = z.string().nullable().optional();
const NullableNumber = z.number().nullable().optional();
const NullableBoolean = z.boolean().nullable().optional();

const VehicleAnchorSchema = z.object({
  make: NullableString,
  model: NullableString,
  variant: NullableString,
  fullModel: NullableString,
  fullVariant: NullableString,
  bodyType: NullableString,
  fuel: NullableString,
  transmission: NullableString,
  city: NullableString,
  confidence: NullableNumber,
  source: NullableString,
}).partial();

const GeminiMeaningFrameSchema = z.object({
  schemaVersion: z.string().optional(),
  messageType: z.string().optional(),
  domains: z.array(z.string()).optional(),
  primaryTask: z.string().nullable().optional(),
  secondaryTasks: z.array(z.string()).optional(),
  rawMessage: z.string().optional(),
  normalizedMessage: z.string().optional(),

  anchors: z.object({
    primaryVehicle: VehicleAnchorSchema.optional(),
    comparisonTargets: z.array(VehicleAnchorSchema).optional(),
    customer: z.any().nullable().optional(),
    location: z.any().nullable().optional(),
    channel: z.any().nullable().optional(),
  }).partial().optional(),

  filters: z.object({
    makes: z.array(z.string()).optional(),
    models: z.array(z.string()).optional(),
    variants: z.array(z.string()).optional(),
    bodyTypes: z.array(z.string()).optional(),
    fuelTypes: z.array(z.string()).optional(),
    transmissions: z.array(z.string()).optional(),
    budget: z.object({
      min: NullableNumber,
      max: NullableNumber,
      basis: NullableString,
      currency: z.string().optional(),
    }).partial().optional(),
    features: z.array(z.string()).optional(),
    colors: z.array(z.string()).optional(),
    safety: z.array(z.string()).optional(),
    usage: z.array(z.string()).optional(),
    ownership: z.array(z.string()).optional(),
  }).partial().optional(),

  requestedFacts: z.object({
    price: NullableBoolean,
    onRoad: NullableBoolean,
    emi: NullableBoolean,
    colors: NullableBoolean,
    features: NullableBoolean,
    safety: NullableBoolean,
    offers: NullableBoolean,
    comparison: NullableBoolean,
    recommendation: NullableBoolean,
    quotation: NullableBoolean,
    lead: NullableBoolean,
    insurance: NullableBoolean,
    finance: NullableBoolean,
    exchange: NullableBoolean,
    challan: NullableBoolean,
    rc: NullableBoolean,
    service: NullableBoolean,
    tco: NullableBoolean,
    content: NullableBoolean,
  }).partial().optional(),

  constraints: z.object({
    mustHaveFeatures: z.array(z.string()).optional(),
    niceToHaveFeatures: z.array(z.string()).optional(),
    excludeFeatures: z.array(z.string()).optional(),
    mustHaveFuelTypes: z.array(z.string()).optional(),
    mustHaveTransmissions: z.array(z.string()).optional(),
    maxBudget: NullableNumber,
    minBudget: NullableNumber,
    city: NullableString,
    buyerUsage: z.any().nullable().optional(),
    timeline: NullableString,
  }).partial().optional(),

  discovery: z.object({
    isBroadDiscovery: z.boolean().optional(),
    resultGranularity: z.string().optional(),
    sortBy: NullableString,
    limit: NullableNumber,
  }).partial().optional(),

  context: z.object({
    action: z.string().optional(),
    usesPreviousVehicle: z.boolean().optional(),
    explicitVehicleMentioned: z.boolean().optional(),
    explicitVariantMentioned: z.boolean().optional(),
    explicitCityMentioned: z.boolean().optional(),
    ambiguity: z.array(z.string()).optional(),
  }).partial().optional(),

  routing: z.object({
    requiredCapabilities: z.array(z.string()).optional(),
    requiredProviders: z.array(z.string()).optional(),
    preferredCanvasType: NullableString,
    toolPlanHint: z.array(z.string()).optional(),
  }).partial().optional(),

  clarification: z.object({
    needed: z.boolean().optional(),
    reason: NullableString,
    question: NullableString,
    options: z.array(z.string()).optional(),
  }).partial().optional(),

  confidence: z.object({
    overall: NullableNumber,
    entityResolution: NullableNumber,
    taskUnderstanding: NullableNumber,
    toolReadiness: NullableNumber,
  }).partial().optional(),

  safety: z.object({
    shouldRefuse: z.boolean().optional(),
    refusalReason: NullableString,
    unsupportedReason: NullableString,
    requiresConsent: z.boolean().optional(),
    consentReason: NullableString,
  }).partial().optional(),
});

export {
  GeminiMeaningFrameSchema,
};

export default GeminiMeaningFrameSchema;
