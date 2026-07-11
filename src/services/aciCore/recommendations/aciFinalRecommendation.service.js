import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose from "mongoose";
import { normalizeCitySlug } from "../context/aciBuyerContextExtractor.service.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const POLICY = JSON.parse(
  fs.readFileSync(path.join(__dirname, "config/finalRecommendationPolicy.v1.json"), "utf8"),
);

const SCORE_COLLECTION = "aci_vehicle_variant_score_profile";
const DECISION_COLLECTION = "aci_vehicle_variant_decision_profile";
const FINAL_RECOMMENDATION_VERSION = "aci_final_recommendation_v1_2026_07_10";

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const asObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};
const text = (value = "") => String(value ?? "").replace(/\s+/g, " ").trim();
const lower = (value = "") => text(value).toLowerCase();
const key = (value = "") =>
  lower(value)
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
const unique = (items = []) => [...new Set(items.map(text).filter(Boolean))];
const clamp = (value, min = 0, max = 100) => Math.min(max, Math.max(min, Number(value) || 0));
const ageDays = (value) => {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) return null;
  return Math.max(0, Math.floor((Date.now() - date.getTime()) / 86400000));
};
const maximumAgeDays = (values = []) => {
  const ages = values.map(Number).filter((value) => Number.isFinite(value) && value >= 0);
  return ages.length ? Math.max(...ages) : null;
};

const EVIDENCE_CACHE_TTL_MS = Math.max(
  60_000,
  Number(process.env.ACI_FINAL_RECOMMENDATION_CACHE_TTL_MS || 10 * 60 * 1000),
);
let evidenceCache = {
  expiresAt: 0,
  promise: null,
  scoreByModel: new Map(),
  decisionByModel: new Map(),
  vehicleByModel: new Map(),
  counts: {},
};

const groupByModel = (rows = []) => {
  const grouped = new Map();
  for (const row of rows) {
    const modelKey = key(row.modelKey || row.model_normalized);
    if (!modelKey) continue;
    if (!grouped.has(modelKey)) grouped.set(modelKey, []);
    grouped.get(modelKey).push(row);
  }
  return grouped;
};

const loadFinalRecommendationEvidence = async ({ db = mongoose.connection?.db, force = false } = {}) => {
  if (!db) throw new Error("MongoDB connection is required for final recommendation evidence.");
  if (!force && evidenceCache.expiresAt > Date.now() && evidenceCache.counts.scoreProfiles) {
    return evidenceCache;
  }
  if (!force && evidenceCache.promise) return evidenceCache.promise;

  evidenceCache.promise = (async () => {
    const [scoreProfiles, decisionProfiles, vehicleRows] = await Promise.all([
      db.collection(SCORE_COLLECTION).find({}).project({
        _id: 0,
        scoreProfileKey: 1,
        variantProfileKey: 1,
        variantFullName: 1,
        makeKey: 1,
        modelKey: 1,
        variantKey: 1,
        fuelKey: 1,
        transmissionKey: 1,
        builtAt: 1,
        safetyScore: 1,
        featureScore: 1,
        performanceScore: 1,
        mileageRunningCostScore: 1,
        practicalityScore: 1,
        cityUseScore: 1,
        highwayUseScore: 1,
        premiumComfortScore: 1,
        regretRisk: 1,
      }).toArray(),
      db.collection(DECISION_COLLECTION).find({}).project({
        _id: 0,
        variantProfileKey: 1,
        modelKey: 1,
        variantKey: 1,
        fuelKey: 1,
        transmissionKey: 1,
        builtAt: 1,
        lifecycleStatus: 1,
        dataQuality: 1,
        safetyBasis: 1,
      }).toArray(),
      db.collection("vehicles").find({}).project({
        _id: 0,
        model_normalized: 1,
        source: 1,
        url: 1,
        sourceUrl: 1,
        cardekhoId: 1,
        ex_showroom_price_cardekho: 1,
        LastSeenDate: 1,
        updatedAt: 1,
        is_discontinued: 1,
      }).toArray(),
    ]);

    const vehicleByModel = new Map();
    for (const item of vehicleRows) {
      const modelKey = key(item.model_normalized);
      if (!modelKey) continue;
      const current = vehicleByModel.get(modelKey) || {
        activeVehicleCount: 0,
        sourceSignalCount: 0,
        latestLastSeenAt: null,
        latestUpdatedAt: null,
      };
      if (item.is_discontinued !== true) current.activeVehicleCount += 1;
      if (item.source || item.url || item.sourceUrl || item.cardekhoId || item.ex_showroom_price_cardekho) {
        current.sourceSignalCount += 1;
      }
      const lastSeen = item.LastSeenDate ? new Date(item.LastSeenDate) : null;
      const updatedAt = item.updatedAt ? new Date(item.updatedAt) : null;
      if (lastSeen && !Number.isNaN(lastSeen.getTime()) && (!current.latestLastSeenAt || lastSeen > current.latestLastSeenAt)) {
        current.latestLastSeenAt = lastSeen;
      }
      if (updatedAt && !Number.isNaN(updatedAt.getTime()) && (!current.latestUpdatedAt || updatedAt > current.latestUpdatedAt)) {
        current.latestUpdatedAt = updatedAt;
      }
      vehicleByModel.set(modelKey, current);
    }

    evidenceCache = {
      expiresAt: Date.now() + EVIDENCE_CACHE_TTL_MS,
      promise: null,
      scoreByModel: groupByModel(scoreProfiles),
      decisionByModel: groupByModel(decisionProfiles),
      vehicleByModel,
      counts: {
        scoreProfiles: scoreProfiles.length,
        decisionProfiles: decisionProfiles.length,
        vehicleRows: vehicleRows.length,
        vehicleModels: vehicleByModel.size,
      },
    };
    return evidenceCache;
  })().catch((error) => {
    evidenceCache.promise = null;
    throw error;
  });

  return evidenceCache.promise;
};

const money = (value) => {
  const amount = Number(value || 0);
  if (!amount) return "";
  const lakh = amount / 100000;
  return `₹${lakh.toFixed(lakh >= 10 ? 2 : 1).replace(/\.0+$/, "")} lakh`;
};

const requestedFinalVerdict = ({ buyerContext = {}, bridge = {}, response = {} } = {}) => {
  if (buyerContext?.inferredBuyerContext?.finalChoiceIntent === true) return true;
  const source = [
    bridge.originalMessage,
    bridge.effectiveMessage,
    response.originalMessage,
    response.effectiveMessage,
    response.query,
  ].map(text).filter(Boolean).join(" ");
  return /\b(recommend\s+(?:me|one|a\s+car|the\s+best)|best\s+(?:car|option|choice|variant|model)\s+(?:for\s+me|to\s+buy|under|within)|which\s+(?:car|one|model|variant)\s+should\s+i|should\s+i\s+(?:buy|choose|pick)|final\s+(?:recommendation|verdict|answer|decision)|decide\s+for\s+me)\b/i.test(source);
};

const valuePresent = (value) => {
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  return Boolean(text(value));
};

const missingBuyerInputs = (buyerContext = {}) => {
  const checks = {
    city: buyerContext.city || buyerContext.citySlug,
    budgetOrPriceCeiling:
      buyerContext.budgetOrPriceCeiling || buyerContext.maxBudget || buyerContext.budget,
    bodyPreferenceOrPrimaryUseCase:
      buyerContext.bodyPreferenceOrPrimaryUseCase || buyerContext.primaryUseCase,
    familySizeOrOccupancyUse:
      buyerContext.familySizeOrOccupancyUse || buyerContext.familySize || buyerContext.occupancy,
    fuelPreferenceOrMonthlyRunning:
      buyerContext.fuelPreferenceOrMonthlyRunning || buyerContext.fuelPreference || buyerContext.monthlyRunning,
    transmissionPreference:
      buyerContext.transmissionPreference || buyerContext.transmission,
    safetyPriority: buyerContext.safetyPriority,
    featurePriority: buyerContext.featurePriority || buyerContext.mustHaveFeatures,
    shortlistedModelsOrDiscoveryScope:
      buyerContext.shortlistedModelsOrDiscoveryScope || buyerContext.discoveryScope || buyerContext.shortlistedModels,
  };
  return Object.entries(checks).filter(([, value]) => !valuePresent(value)).map(([name]) => name);
};

const contains = (value, pattern) => pattern.test(lower(value));

const buildWeights = (buyerContext = {}) => {
  const weights = { ...POLICY.baseWeights };
  const fullContext = JSON.stringify(buyerContext);
  const adjustments = [];
  if (contains(buyerContext.safetyPriority, /high|top|critical|must|important/) || buyerContext.inferredBuyerContext?.safetySensitive) {
    adjustments.push("highSafety");
  }
  if (contains(fullContext, /family|occupant|parents|kids|children/)) adjustments.push("familyUse");
  if (contains(fullContext, /city|traffic|urban|commute/)) adjustments.push("cityUse");
  if (contains(fullContext, /highway|touring|long drive/)) adjustments.push("highwayUse");
  if (contains(fullContext, /monthly|daily|\bkm\b|high running/)) adjustments.push("meaningfulRunning");
  if (valuePresent(buyerContext.featurePriority) || valuePresent(buyerContext.mustHaveFeatures)) adjustments.push("featurePriority");
  if (contains(buyerContext.transmissionPreference, /automatic|auto|amt|cvt|dct|ivt|at/)) adjustments.push("automaticPreference");

  for (const name of adjustments) {
    for (const [dimension, delta] of Object.entries(POLICY.priorityAdjustments[name] || {})) {
      weights[dimension] = Math.max(0, Number(weights[dimension] || 0) + Number(delta || 0));
    }
  }

  const total = Object.values(weights).reduce((sum, value) => sum + Number(value || 0), 0) || 1;
  return Object.fromEntries(
    Object.entries(weights).map(([dimension, value]) => [dimension, Number((value / total).toFixed(5))]),
  );
};

const priceFor = (variant = {}, priceBasis = "ex_showroom") =>
  Number(priceBasis === "on_road" ? variant.onRoadPrice : variant.exShowroomPrice) || 0;

const automaticTransmission = (value = "") =>
  /\b(automatic|auto|amt|cvt|dct|ivt|at|dsg)\b/i.test(text(value).replace(/[-_]+/g, " "));

const transmissionMatches = (actual = "", requested = "") => {
  if (!text(requested)) return true;
  if (automaticTransmission(requested)) return automaticTransmission(actual);
  if (/\b(manual|mt)\b/i.test(text(requested).replace(/[-_]+/g, " "))) {
    return !automaticTransmission(actual) && /\b(manual|mt)\b/i.test(text(actual).replace(/[-_]+/g, " "));
  }
  return key(actual) === key(requested);
};

const fuelMatches = (actual = "", requested = "") => {
  const actualKey = key(actual);
  const requestedKey = key(requested);
  if (!requestedKey || /^(any|open|no-preference)$/.test(requestedKey)) return true;
  if (["ev", "electric"].includes(requestedKey)) return ["ev", "electric"].includes(actualKey);
  return actualKey === requestedKey;
};

const profileMatchesVariant = (profile = {}, variant = {}) => {
  if (key(profile.modelKey) !== key(variant.modelKey || variant.model)) return false;
  if (key(profile.variantKey) !== key(variant.variantKey || variant.variant)) return false;
  const requestedFuel = key(variant.fuelType || variant.fuel);
  if (requestedFuel && key(profile.fuelKey) !== requestedFuel) return false;
  return transmissionMatches(profile.transmissionKey, variant.transmission);
};

const decisionMatchesVariant = (profile = {}, variant = {}) =>
  key(profile.modelKey) === key(variant.modelKey || variant.model) &&
  key(profile.variantKey) === key(variant.variantKey || variant.variant) &&
  (!variant.fuelType || key(profile.fuelKey) === key(variant.fuelType)) &&
  transmissionMatches(profile.transmissionKey, variant.transmission);

const moduleValue = (profile = {}, pathName = "") => clamp(profile[pathName]?.score);

const hasCoreScoreEvidence = (profile = {}) =>
  ["safetyScore", "featureScore", "practicalityScore", "cityUseScore", "mileageRunningCostScore"]
    .every((name) => {
      const module = asObject(profile[name]);
      return module.score !== null && module.score !== undefined &&
        Number.isFinite(Number(module.score)) && Boolean(text(module.confidence));
    });

const hasVerifiedCrashApplicability = ({ profile = {}, decision = {} } = {}) => {
  const safetyBasis = asObject(decision.safetyBasis);
  const scope = lower(safetyBasis.crashRatingApplicabilityScope);
  const reviewStatus = lower(
    safetyBasis.crashRatingReviewStatus || profile.safetyScore?.evidence?.crashRatingReviewStatus,
  );
  const ratingStatus = lower(safetyBasis.crashRatingStatus || profile.safetyScore?.status);
  const hasSource = Boolean(
    safetyBasis.crashRatingSourceProfileKey || profile.safetyScore?.evidence?.crashRatingSource,
  );
  const hasUnsafeStatus = /needs|unknown|blocked|not[_\s-]*publicly|internal[_\s-]*(?:feature|model)/.test(
    `${reviewStatus} ${ratingStatus}`,
  );
  return (
    asArray(POLICY.gates.verifiedCrashApplicabilityScopes).map(lower).includes(scope) &&
    safetyBasis.crashRatingNeedsOfficialVerification === false &&
    hasSource &&
    !hasUnsafeStatus
  );
};

const evidenceQualityScore = ({ row = {}, decision = {}, directEvidence = {} } = {}) => {
  const market = lower(row.candidateMarketConfidence?.confidenceBand);
  const active = lower(row.candidateActiveMarketEligibility?.activeMarketConfidenceBand);
  const provenance = lower(row.candidateSourceProvenance?.band);
  const decisionConfidence = lower(decision.dataQuality?.confidenceTier);
  const directReady = Number(directEvidence.activeVehicleCount || 0) > 0 && Number(directEvidence.sourceSignalCount || 0) > 0;
  const values = [
    market === "strong" ? 100 : market === "good" ? 82 : directReady ? 90 : 40,
    active === "strong" ? 100 : active === "good" ? 82 : directReady ? 90 : 40,
    provenance === "strong" ? 100 : provenance === "good" ? 82 : directReady ? 88 : 35,
    decisionConfidence === "high" ? 100 : decisionConfidence === "medium" ? 75 : 35,
  ];
  return values.reduce((sum, value) => sum + value, 0) / values.length;
};

const featureRequirementSatisfied = ({ variant = {}, requiredFeatureKeys = [] } = {}) => {
  if (!requiredFeatureKeys.length) return true;
  const matched = new Set(asArray(variant.matchedFeatureKeys).map(key));
  return requiredFeatureKeys.every((featureKey) => matched.has(key(featureKey)));
};

const rowPassesEvidenceGate = (row = {}) => {
  const gates = POLICY.gates;
  const hasCandidateEvidence = Boolean(
    row.candidateMarketConfidence ||
    row.candidateActiveMarketEligibility ||
    row.candidateSourceProvenance,
  );
  if (!hasCandidateEvidence) return true;
  const marketBand = lower(row.candidateMarketConfidence?.confidenceBand);
  const activeBand = lower(row.candidateActiveMarketEligibility?.activeMarketConfidenceBand);
  const provenanceBand = lower(row.candidateSourceProvenance?.band);
  const age = Number(row.candidateMarketConfidence?.evidence?.stalenessDays);
  return (
    gates.allowedMarketBands.includes(marketBand) &&
    gates.allowedActiveMarketBands.includes(activeBand) &&
    gates.allowedProvenanceBands.includes(provenanceBand) &&
    Number.isFinite(age) &&
    age <= gates.maximumEvidenceAgeDays
  );
};

const scoreVariant = ({ variant, profile, decision, row, directEvidence, weights, budget, priceBasis, highSafety }) => {
  const price = priceFor(variant, priceBasis);
  const headroom = budget > 0 ? clamp(((budget - price) / budget) * 100, 0, 45) : 0;
  const dimensions = {
    safety: moduleValue(profile, "safetyScore"),
    practicality: moduleValue(profile, "practicalityScore"),
    cityUse: moduleValue(profile, "cityUseScore"),
    highwayUse: moduleValue(profile, "highwayUseScore"),
    runningCost: moduleValue(profile, "mileageRunningCostScore"),
    features: moduleValue(profile, "featureScore"),
    comfort: moduleValue(profile, "premiumComfortScore"),
    performance: moduleValue(profile, "performanceScore"),
    affordability: clamp(55 + headroom),
    regretResistance: clamp(100 - Number(profile.regretRisk?.riskScore || 50)),
    evidenceQuality: evidenceQualityScore({ row, decision, directEvidence }),
  };

  let score = Object.entries(weights).reduce(
    (sum, [dimension, weight]) => sum + dimensions[dimension] * Number(weight || 0),
    0,
  );
  const penalties = [];
  const crashApplicabilityVerified = hasVerifiedCrashApplicability({ profile, decision });
  if (highSafety && !crashApplicabilityVerified) {
    score -= POLICY.penalties.missingExactCrashApplicabilityForHighSafety;
    penalties.push("independent crash-rating applicability is not verified for this exact variant");
  }
  if (lower(decision.dataQuality?.confidenceTier) === "medium") {
    score -= POLICY.penalties.mediumDecisionConfidence;
    penalties.push("decision-profile confidence is medium");
  }
  const scoreConfidenceMissing = [
    profile.safetyScore,
    profile.featureScore,
    profile.practicalityScore,
    profile.cityUseScore,
    profile.mileageRunningCostScore,
  ].some((module) => !module?.confidence);
  if (scoreConfidenceMissing) {
    score -= POLICY.penalties.missingScoreConfidence;
    penalties.push("one or more core module confidence labels are missing");
  }

  return {
    score: Number(score.toFixed(2)),
    dimensions,
    penalties,
    crashApplicabilityVerified,
    directEvidence,
    price,
  };
};

const DIMENSION_LABELS = {
  safety: "safety equipment and evidence",
  practicality: "family practicality",
  cityUse: "city usability",
  highwayUse: "highway suitability",
  runningCost: "running-cost fit",
  features: "useful equipment",
  comfort: "comfort features",
  performance: "performance",
  affordability: "budget headroom",
  regretResistance: "lower trade-off risk",
  evidenceQuality: "evidence quality",
};

const strongestDimensions = (candidate = {}, weights = {}, limit = 3) =>
  Object.entries(candidate.dimensions || {})
    .map(([dimension, value]) => ({
      dimension,
      value,
      contribution: value * Number(weights[dimension] || 0),
    }))
    .sort((left, right) => right.contribution - left.contribution)
    .slice(0, limit)
    .map((item) => DIMENSION_LABELS[item.dimension] || item.dimension);

const weakestDimension = (candidate = {}, weights = {}, excludedLabels = []) => {
  const excluded = new Set(excludedLabels);
  return (
    Object.entries(candidate.dimensions || {})
      .filter(([dimension]) => Number(weights[dimension] || 0) >= 0.04)
      .sort((left, right) => left[1] - right[1])
      .map(([dimension]) => DIMENSION_LABELS[dimension] || dimension)
      .find((label) => !excluded.has(label)) || "overall fit"
  );
};

const relativeStrengths = (candidate = {}, comparison = {}, limit = 2) =>
  Object.entries(candidate.dimensions || {})
    .map(([dimension, value]) => ({
      dimension,
      delta: Number(value || 0) - Number(comparison.dimensions?.[dimension] || 0),
      weight: Number(comparison.weights?.[dimension] || 0),
    }))
    .filter((item) => item.delta >= 2)
    .sort((left, right) => right.delta - left.delta)
    .slice(0, limit)
    .map((item) => DIMENSION_LABELS[item.dimension] || item.dimension);

const publicCandidate = (candidate = {}, weights = {}) => {
  const strongestFitReasons = strongestDimensions(candidate, weights);
  return {
    rank: candidate.rank,
    make: candidate.variant.make,
    model: candidate.variant.model,
    fullModel: candidate.variant.fullModel,
    modelKey: candidate.variant.modelKey,
    variant: candidate.variant.variant,
    variantKey: candidate.variant.variantKey,
    fuelType: candidate.variant.fuelType,
    transmission: candidate.variant.transmission,
    city: candidate.variant.city,
    citySlug: candidate.variant.citySlug,
    exShowroomPrice: candidate.variant.exShowroomPrice,
    exShowroomPriceLabel: candidate.variant.exShowroomPriceLabel,
    onRoadPrice: candidate.variant.onRoadPrice,
    onRoadPriceLabel: candidate.variant.onRoadPriceLabel,
    priceBasis: candidate.priceBasis,
    evaluatedPrice: candidate.price,
    evaluatedPriceLabel: money(candidate.price),
    fitScore: candidate.score,
    strongestFitReasons,
    mainTradeoff: weakestDimension(candidate, weights, strongestFitReasons),
    crashApplicabilityVerified: candidate.crashApplicabilityVerified,
    evidenceFreshnessDays: maximumAgeDays(Object.values(candidate.sourceFreshnessDays || {})),
    sourceProvenanceBand: candidate.row.candidateSourceProvenance?.band || "direct_db_current",
  };
};

const buildAnswer = ({ winner, runnerUp, buyerContext, response, weights, eligibleVariantCount }) => {
  const winnerLabel = `${text(winner.variant.fullModel)} ${text(winner.variant.variant)}`.trim();
  const runnerLabel = runnerUp
    ? `${text(runnerUp.variant.fullModel)} ${text(runnerUp.variant.variant)}`.trim()
    : "";
  const priceBasis = winner.priceBasis === "on_road" ? "on-road" : "ex-showroom";
  const city = text(buyerContext.city || winner.variant.city || "your city");
  const features = asArray(response.budgetDiscovery?.featureResolution?.resolvedFeatures)
    .map((item) => item.displayName || item.featureKey)
    .filter(Boolean);
  const reasons = strongestDimensions(winner, weights);
  const featureSentence = features.length
    ? `The indexed variant data confirms ${features.join(" and ")}.`
    : "";
  const runnerSentence = runnerUp
    ? (() => {
        const advantages = relativeStrengths(runnerUp, { ...winner, weights });
        const tradeoffs = relativeStrengths(winner, { ...runnerUp, weights });
        const advantage = advantages.length
          ? advantages.join(" and ")
          : runnerUp.price < winner.price
            ? `${money(winner.price - runnerUp.price)} more budget headroom`
            : strongestDimensions(runnerUp, weights, 1)[0];
        const tradeoff = tradeoffs.length ? tradeoffs.join(" and ") : weakestDimension(runnerUp, weights);
        return `The closest alternative is ${runnerLabel} at about ${money(runnerUp.price)} ${priceBasis}; choose it instead if you prefer ${advantage}, while accepting weaker ${tradeoff}.`;
      })()
    : "No second model cleared every evidence and requirement gate, so I would not invent a runner-up.";
  const safetyCaveat = winner.crashApplicabilityVerified
    ? "Its exact-variant crash evidence is present in the indexed profile, but you should still confirm the latest tested-version applicability before booking."
    : "One important caveat: this safety judgement uses indexed safety equipment and score evidence; independent crash-test applicability is not verified for this exact variant, so confirm the latest rating and dealer quote before booking.";
  const verdictLine = runnerUp && winner.score - runnerUp.score < 2
    ? `My pick is the ${winnerLabel}, but it is a close call.`
    : `My pick is the ${winnerLabel}.`;

  return [
    verdictLine,
    `At about ${money(winner.price)} ${priceBasis} in ${city}, it stays inside your stated cap.`,
    featureSentence,
    `For your brief, the main factors in its fit are ${reasons.join(", ")}. I checked ${eligibleVariantCount} exact variants that met your filters before making the call.`,
    runnerSentence,
    safetyCaveat,
  ].filter(Boolean).join(" ");
};

async function buildAciFinalRecommendation({ response = {}, rows = [], buyerContext = {}, bridge = {} } = {}) {
  const requested = requestedFinalVerdict({ buyerContext, bridge, response });
  const missingInputs = missingBuyerInputs(buyerContext);
  const base = {
    version: FINAL_RECOMMENDATION_VERSION,
    policyVersion: POLICY.version,
    requested,
    finalRecommendationEnabled: false,
    canUseForFinalRecommendation: false,
    status: requested ? "blocked" : "not_requested",
    missingInputs,
    blockedReasons: [],
  };
  if (!requested) return base;
  if (bridge.tool !== "vehicle_recommend" && bridge.primaryTask !== "vehicle_recommendation") {
    return { ...base, blockedReasons: ["module_not_final_recommendation_eligible"] };
  }
  if (missingInputs.length) {
    return { ...base, blockedReasons: ["buyer_context_incomplete"] };
  }

  const db = mongoose.connection?.db;
  if (!db) return { ...base, blockedReasons: ["recommendation_database_unavailable"] };

  const inputRows = asArray(rows).filter(rowPassesEvidenceGate);
  const modelKeys = unique(inputRows.map((row) => key(row.modelKey || row.model)));
  if (!modelKeys.length) return { ...base, blockedReasons: ["no_fresh_source_grounded_candidates"] };
  let cachedEvidence;
  try {
    cachedEvidence = await loadFinalRecommendationEvidence({ db });
  } catch {
    return { ...base, blockedReasons: ["recommendation_evidence_unavailable"] };
  }
  const scoreProfiles = modelKeys.flatMap((modelKey) => cachedEvidence.scoreByModel.get(modelKey) || []);
  const decisionProfiles = modelKeys.flatMap((modelKey) => cachedEvidence.decisionByModel.get(modelKey) || []);
  const directEvidenceByModel = cachedEvidence.vehicleByModel;

  const budget = Number(
    buyerContext.budgetOrPriceCeiling || buyerContext.maxBudget || response.filters?.budgetMax || 0,
  );
  const priceBasis = response.budgetDiscovery?.budgetBasis === "on_road" || response.filters?.priceBasis === "on_road"
    ? "on_road"
    : "ex_showroom";
  const requiredFeatureKeys = asArray(response.budgetDiscovery?.featureResolution?.featureKeys);
  const weights = buildWeights(buyerContext);
  const requestedCitySlug = normalizeCitySlug(buyerContext.citySlug || buyerContext.city);
  const requestedFuel = buyerContext.fuelPreference || buyerContext.fuelType || buyerContext.fuel || "";
  const requestedTransmission = buyerContext.transmissionPreference || buyerContext.transmission || "";
  const highSafety =
    contains(buyerContext.safetyPriority, /high|top|critical|must|important/) ||
    buyerContext.inferredBuyerContext?.safetySensitive === true;
  const evaluated = [];

  for (const row of inputRows) {
    for (const variant of asArray(row.qualifyingVariants)) {
      if (requestedCitySlug && normalizeCitySlug(variant.citySlug || variant.city) !== requestedCitySlug) continue;
      if (!fuelMatches(variant.fuelType || variant.fuel, requestedFuel)) continue;
      if (!transmissionMatches(variant.transmission, requestedTransmission)) continue;
      const price = priceFor(variant, priceBasis);
      if (!price || (budget > 0 && price > budget)) continue;
      if (!featureRequirementSatisfied({ variant, requiredFeatureKeys })) continue;
      const profile = scoreProfiles.find((item) => profileMatchesVariant(item, variant));
      const decision = decisionProfiles.find((item) => decisionMatchesVariant(item, variant));
      if (!profile || !decision) continue;
      if (!hasCoreScoreEvidence(profile)) continue;
      const directEvidence = directEvidenceByModel.get(key(variant.modelKey || variant.model)) || {};
      const directFreshnessDays = ageDays(directEvidence.latestLastSeenAt || directEvidence.latestUpdatedAt);
      const priceFreshnessDays = ageDays(variant.updatedAt || variant.builtAt);
      const scoreFreshnessDays = ageDays(profile.builtAt);
      const decisionFreshnessDays = ageDays(decision.builtAt);
      const sourceFreshnessDays = {
        rawVehicle: directFreshnessDays,
        priceAndFeatureJoin: priceFreshnessDays,
        scoreProfile: scoreFreshnessDays,
        decisionProfile: decisionFreshnessDays,
      };
      if (Number(directEvidence.activeVehicleCount || 0) <= 0) continue;
      if (Number(directEvidence.sourceSignalCount || 0) <= 0) continue;
      if (Object.values(sourceFreshnessDays).some((days) =>
        days === null || days > POLICY.gates.maximumEvidenceAgeDays
      )) continue;
      if (!POLICY.gates.allowedDecisionConfidence.includes(lower(decision.dataQuality?.confidenceTier))) continue;
      if (decision.dataQuality?.needsReview === true || decision.lifecycleStatus === "discontinued") continue;
      const scored = scoreVariant({
        variant,
        profile,
        decision,
        row,
        directEvidence,
        weights,
        budget,
        priceBasis,
        highSafety,
      });
      evaluated.push({ ...scored, variant, profile, decision, row, priceBasis, sourceFreshnessDays });
    }
  }

  evaluated.sort((left, right) => right.score - left.score || left.price - right.price);
  const bestByModel = [];
  const seenModels = new Set();
  for (const candidate of evaluated) {
    const modelKey = key(candidate.variant.modelKey || candidate.variant.model);
    if (!modelKey || seenModels.has(modelKey)) continue;
    seenModels.add(modelKey);
    bestByModel.push(candidate);
  }

  if (bestByModel.length < POLICY.gates.minimumEligibleModels) {
    return {
      ...base,
      blockedReasons: ["insufficient_exact_variant_evidence"],
      evidence: {
        inputModelCount: inputRows.length,
        evaluatedVariantCount: evaluated.length,
        eligibleModelCount: bestByModel.length,
      },
    };
  }

  bestByModel.forEach((candidate, index) => {
    candidate.rank = index + 1;
  });
  const winner = bestByModel[0];
  const runnerUp =
    bestByModel.find((candidate, index) =>
      index > 0 && key(candidate.variant.make) !== key(winner.variant.make),
    ) || bestByModel[1];
  const orderedForBuyer = [
    winner,
    runnerUp,
    ...bestByModel.filter((candidate) => candidate !== winner && candidate !== runnerUp),
  ].filter(Boolean);
  const publicRows = orderedForBuyer
    .slice(0, 8)
    .map((candidate, index) => publicCandidate({ ...candidate, rank: index + 1 }, weights));
  const answer = buildAnswer({
    winner,
    runnerUp,
    buyerContext,
    response,
    weights,
    eligibleVariantCount: evaluated.length,
  });

  return {
    ...base,
    status: "final_ready",
    finalRecommendationEnabled: true,
    canUseForFinalRecommendation: true,
    blockedReasons: [],
    title: `My pick: ${winner.variant.fullModel} ${winner.variant.variant}`.trim(),
    answer,
    rows: publicRows,
    winner: publicRows[0],
    runnerUp: publicRows[1],
    alternatives: publicRows.slice(1),
    weights,
    evidence: {
      evidenceStatus: "complete",
      confidence: "high",
      inputModelCount: inputRows.length,
      evaluatedVariantCount: evaluated.length,
      eligibleModelCount: bestByModel.length,
      requiredFeatureKeys,
      priceBasis,
      budget,
      sourceCollections: [
        "aci_vehicle_price_rows",
        "vehicle_variant_feature_matrix_v2",
        SCORE_COLLECTION,
        DECISION_COLLECTION,
        "vehicles",
      ],
    },
    guardrail:
      "Final verdict is a buyer-context fit judgement over fresh indexed evidence, not a guarantee of dealer stock, transaction price, or crash-test applicability.",
  };
}

const prewarmAciFinalRecommendationEvidence = async ({ force = false } = {}) => {
  const startedAt = Date.now();
  const cache = await loadFinalRecommendationEvidence({ force });
  return {
    ok: true,
    durationMs: Date.now() - startedAt,
    expiresAt: cache.expiresAt,
    counts: cache.counts,
  };
};

export {
  FINAL_RECOMMENDATION_VERSION,
  POLICY as FINAL_RECOMMENDATION_POLICY,
  buildAciFinalRecommendation,
  prewarmAciFinalRecommendationEvidence,
};

export default buildAciFinalRecommendation;
