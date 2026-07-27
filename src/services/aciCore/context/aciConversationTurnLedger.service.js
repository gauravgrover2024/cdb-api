const MAX_LEDGER_TURNS = 16;
const LEDGER_VERSION = "aci_context_turn_ledger_v1";
const TRACE_VERSION = "aci_context_trace_v1";

const textOf = (value = "") => String(value || "").replace(/\s+/g, " ").trim();
const lower = (value = "") => textOf(value).toLowerCase();
const keyify = (value = "") =>
  lower(value)
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const asArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value];
};

const compactObject = (obj = {}) =>
  Object.fromEntries(
    Object.entries(obj || {}).filter(([, value]) => {
      if (value === undefined || value === null) return false;
      if (typeof value === "string" && !value.trim()) return false;
      if (Array.isArray(value) && !value.length) return false;
      if (
        typeof value === "object" &&
        !Array.isArray(value) &&
        !Object.keys(value).length
      ) {
        return false;
      }
      return true;
    }),
  );

const uniqueVehicles = (vehicles = []) => {
  const seen = new Set();
  const output = [];

  for (const vehicle of asArray(vehicles).map(compactVehicle)) {
    const key = vehicleIdentityKey(vehicle);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(vehicle);
  }

  return output;
};

const modelText = (vehicle = {}) => {
  const make = textOf(vehicle.make || vehicle.brand);
  let model = textOf(
    vehicle.model ||
      vehicle.shortModelKey ||
      vehicle.modelKey ||
      vehicle.fullModel ||
      vehicle.displayName,
  );

  if (make && keyify(model).startsWith(`${keyify(make)} `)) {
    model = model.slice(make.length).trim();
  }

  return model;
};

export const vehicleLabel = (vehicle = {}) => {
  const make = textOf(vehicle.make || vehicle.brand);
  const model = modelText(vehicle);
  const variant = textOf(
    vehicle.variant || vehicle.variantName || vehicle.selectedVariant,
  );

  return [make, model, variant].filter(Boolean).join(" ").trim();
};

export const vehicleIdentityKey = (vehicle = {}) => {
  const makeKey = keyify(
    vehicle.makeKey || vehicle.make || vehicle.brand,
  );
  let modelKey = keyify(
    vehicle.shortModelKey ||
      vehicle.modelKey ||
      vehicle.model ||
      vehicle.fullModel ||
      vehicle.displayName,
  );
  const variantKey = keyify(
    vehicle.variantKey ||
      vehicle.variant ||
      vehicle.variantName ||
      vehicle.selectedVariant,
  );

  if (makeKey && modelKey.startsWith(`${makeKey} `)) {
    modelKey = modelKey.slice(makeKey.length).trim();
  }

  return [makeKey, modelKey, variantKey].filter(Boolean).join("|");
};

const vehicleModelIdentityKey = (vehicle = {}) => {
  const makeKey = keyify(vehicle.makeKey || vehicle.make || vehicle.brand);
  let modelKey = keyify(
    vehicle.shortModelKey ||
      vehicle.modelKey ||
      vehicle.model ||
      vehicle.fullModel ||
      vehicle.displayName,
  );

  if (makeKey && modelKey.startsWith(`${makeKey} `)) {
    modelKey = modelKey.slice(makeKey.length).trim();
  }

  return [makeKey, modelKey].filter(Boolean).join("|");
};

export const hasVehicleIdentity = (vehicle = {}) =>
  Boolean(
    vehicle &&
      (vehicle.model ||
        vehicle.fullModel ||
        vehicle.modelKey ||
        vehicle.shortModelKey),
  );

const compactVehicle = (vehicle = {}) => {
  if (!hasVehicleIdentity(vehicle)) return {};

  const make = textOf(vehicle.make || vehicle.brand);
  const model = modelText(vehicle);
  const variant = textOf(
    vehicle.variant || vehicle.variantName || vehicle.selectedVariant,
  );
  const fullModel = textOf(
    vehicle.fullModel ||
      vehicle.displayName ||
      [make, model].filter(Boolean).join(" "),
  );

  return compactObject({
    id: vehicle.id,
    make,
    brand: textOf(vehicle.brand || vehicle.make),
    model,
    fullModel,
    displayName: textOf(vehicle.displayName),
    makeKey: keyify(vehicle.makeKey || make),
    modelKey: keyify(vehicle.modelKey || fullModel || model),
    shortModelKey: keyify(vehicle.shortModelKey || model),
    variant,
    variantName: variant,
    variantKey: variant ? keyify(vehicle.variantKey || variant) : "",
    fuelType: textOf(vehicle.fuelType || vehicle.fuelText),
    transmission: textOf(vehicle.transmission || vehicle.transmissionText),
    city: textOf(vehicle.city),
    citySlug: textOf(vehicle.citySlug),
    source: textOf(vehicle.source || "context_turn_ledger"),
    confidence: Number(vehicle.confidence) || 0.9,
  });
};

const inferTopicFromMessage = (message = "", intent = "", tool = "") => {
  const text = lower(`${message} ${intent} ${tool}`);

  if (/\b(vs|versus|compare|comparison|against)\b/.test(text)) {
    return "comparison";
  }
  if (/\b(price|on.?road|ex.?showroom|cost|rate|pricelist|price list)\b/.test(text)) {
    return "price";
  }
  if (
    /\b(sunroof|abs|airbag|airbags|adas|feature|features|safety|camera|esc|tpms|isofix)\b/.test(
      text,
    )
  ) {
    return "feature";
  }
  if (/\b(colou?rs?|image|photo|look|visual|studio)\b/.test(text)) {
    return "color";
  }
  if (
    /\b(mileage|range|engine|power|torque|boot|ground clearance|dimension|spec)\b/.test(
      text,
    )
  ) {
    return "spec";
  }
  if (/\b(emi|loan|finance|down payment)\b/.test(text)) return "finance";

  return "general";
};

const isExplicitComparisonMessage = (message = "") =>
  /\b(vs|v\/s|versus|compare|comparison|against|difference between)\b/i.test(
    message,
  );

const isComparisonFollowUp = (message = "", previousContext = {}) => {
  const previousComparison = comparisonVehiclesFromContext(previousContext);
  if (previousComparison.length < 2) return false;

  return /\b(which|which one|which is|which has|between them|both|them|their|compare|comparison|better|safer|cheaper|costlier|difference|versus|vs|v\/s)\b/i.test(
    message,
  );
};

const relativeComparisonRequested = (message = "") =>
  /\b(last|previous|earlier|above|one before)\b/i.test(message) &&
  /\b(vs|v\/s|versus|compare|comparison|against|with)\b/i.test(message);

const rightSideText = (message = "") => {
  const parts = String(message || "").split(
    /\b(?:vs|v\/s|versus|against|with)\b/i,
  );
  return lower(parts.length > 1 ? parts[parts.length - 1] : "");
};

const phraseInMessage = (message = "", phrase = "") => {
  const normalizedMessage = ` ${keyify(message)} `;
  const normalizedPhrase = keyify(phrase);
  if (!normalizedPhrase) return false;
  return normalizedMessage.includes(` ${normalizedPhrase} `);
};

const vehicleMentionedInMessage = (vehicle = {}, message = "") => {
  if (!hasVehicleIdentity(vehicle)) return false;

  const make = textOf(vehicle.make || vehicle.brand);
  const model = modelText(vehicle);
  const fullModel = textOf(
    vehicle.fullModel || vehicle.displayName || [make, model].filter(Boolean).join(" "),
  );
  const candidates = [
    fullModel,
    [make, model].filter(Boolean).join(" "),
    model,
    vehicle.shortModelKey,
    vehicle.modelKey,
  ]
    .map(textOf)
    .filter((value) => keyify(value).length >= 3)
    .sort((left, right) => right.length - left.length);

  return candidates.some((candidate) => phraseInMessage(message, candidate));
};

const comparisonVehiclesFromContext = (context = {}) =>
  uniqueVehicles(
    context.selectedComparisonSet?.vehicles ||
      context.activeComparison?.vehicles ||
      context.contextState?.activeComparison?.vehicles ||
      context.aciContextState?.activeComparison?.vehicles ||
      [],
  );

const primaryVehicleFromSources = ({
  contextPatch = {},
  response = {},
  resolvedContext = {},
  candidateSnapshot = {},
} = {}) =>
  compactVehicle(
    contextPatch.selectedVehicle ||
      contextPatch.contextState?.selectedVehicle ||
      contextPatch.aciContextState?.selectedVehicle ||
      response.selectedVehicle ||
      resolvedContext.selectedVehicle ||
      candidateSnapshot.selectedVehicle ||
      {},
  );

const comparisonVehiclesFromSources = ({
  contextPatch = {},
  response = {},
} = {}) =>
  uniqueVehicles(
    contextPatch.selectedComparisonSet?.vehicles ||
      contextPatch.activeComparison?.vehicles ||
      contextPatch.contextState?.activeComparison?.vehicles ||
      contextPatch.aciContextState?.activeComparison?.vehicles ||
      response.selectedComparisonSet?.vehicles ||
      response.activeComparison?.vehicles ||
      [],
  );

const explicitCandidateVehicles = (candidateSnapshot = {}) =>
  uniqueVehicles([
    ...asArray(candidateSnapshot?.vehicles?.models).map(
      (candidate = {}) => candidate.metadata?.raw || candidate.metadata || candidate,
    ),
    ...asArray(candidateSnapshot?.vehicles?.variants).map(
      (candidate = {}) => candidate.metadata?.raw || candidate.metadata || candidate,
    ),
  ]);

const explicitVehicleEvidence = ({
  message = "",
  primaryVehicle = {},
  candidateSnapshot = {},
} = {}) => {
  const candidates = explicitCandidateVehicles(candidateSnapshot);
  const primaryKey = vehicleIdentityKey(primaryVehicle);
  const candidateMatch = candidates.some(
    (vehicle) => vehicleIdentityKey(vehicle) === primaryKey,
  );
  const messageMatch = vehicleMentionedInMessage(primaryVehicle, message);

  if (candidateMatch) {
    return {
      explicit: true,
      source: "candidate_snapshot",
      confidence: 0.98,
      reason: "One DB-resolved vehicle candidate matches the selected subject.",
      resolvedBy: "aci_db_candidate_retriever",
    };
  }

  if (messageMatch) {
    return {
      explicit: true,
      source: "message_vehicle_mention",
      confidence: 0.94,
      reason: "The selected vehicle is explicitly named in the current message.",
      resolvedBy: "deterministic_vehicle_mention",
    };
  }

  return {
    explicit: false,
    source: "active_context",
    confidence: 0.78,
    reason: "The selected subject was carried from active context.",
    resolvedBy: "aci_context_manager",
  };
};

const vehicleMatchesRightSide = (vehicle = {}, right = "") => {
  if (!right || !hasVehicleIdentity(vehicle)) return false;

  const rightTokens = keyify(right).split(" ").filter(Boolean);
  const vehicleTokens = new Set(
    keyify(
      [
        vehicleLabel(vehicle),
        vehicle.model,
        vehicle.fullModel,
        vehicle.modelKey,
        vehicle.shortModelKey,
      ].join(" "),
    )
      .split(" ")
      .filter(Boolean),
  );

  return rightTokens.length > 0 && rightTokens.every((token) => vehicleTokens.has(token));
};

export function getContextLedger(previousContext = {}) {
  return (
    previousContext.contextLedger ||
    previousContext.contextState?.contextLedger ||
    previousContext.aciContextState?.contextLedger ||
    {}
  );
}

export function buildAciTurnEvent({
  message = "",
  intent = "",
  tool = "",
  response = {},
  contextPatch = {},
  resolvedContext = {},
  candidateSnapshot = {},
  previousContext = {},
} = {}) {
  const primaryVehicle = primaryVehicleFromSources({
    contextPatch,
    response,
    resolvedContext,
    candidateSnapshot,
  });
  const comparisonVehicles = comparisonVehiclesFromSources({
    contextPatch,
    response,
  });
  const comparisonTurn =
    comparisonVehicles.length >= 2 ||
    isExplicitComparisonMessage(message) ||
    isComparisonFollowUp(message, previousContext) ||
    /comparison/i.test(`${intent} ${tool}`);
  const explicitEvidence = explicitVehicleEvidence({
    message,
    primaryVehicle,
    candidateSnapshot,
  });
  const topic = inferTopicFromMessage(message, intent, tool);

  return compactObject({
    turnId: `turn_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    message: textOf(message),
    intent: textOf(intent),
    tool: textOf(tool),
    primaryVehicle,
    comparisonVehicles,
    explicitSingleVehicleTurn: Boolean(
      hasVehicleIdentity(primaryVehicle) &&
        explicitEvidence.explicit &&
        !comparisonTurn,
    ),
    comparisonTurn: Boolean(comparisonTurn),
    topic,
    source: explicitEvidence.source,
    confidence: explicitEvidence.confidence,
    reason: explicitEvidence.reason,
    resolvedBy: explicitEvidence.resolvedBy,
    createdAt: new Date().toISOString(),
  });
}

export function appendAciTurnLedger(previousContext = {}, turnEvent = {}) {
  const previousLedger = getContextLedger(previousContext);
  const turns = Array.isArray(previousLedger.turns)
    ? previousLedger.turns.filter(Boolean)
    : [];
  const nextTurns = [...turns, turnEvent].filter(Boolean).slice(-MAX_LEDGER_TURNS);
  const explicitTurns = nextTurns.filter(
    (turn) =>
      turn.explicitSingleVehicleTurn &&
      hasVehicleIdentity(turn.primaryVehicle),
  );
  const comparisonTurns = nextTurns.filter(
    (turn) =>
      turn.comparisonTurn &&
      asArray(turn.comparisonVehicles).length >= 2,
  );
  const lastFactSubjectByTopic = {
    ...(previousLedger.lastFactSubjectByTopic || {}),
  };

  if (
    turnEvent.topic &&
    turnEvent.topic !== "comparison" &&
    hasVehicleIdentity(turnEvent.primaryVehicle) &&
    !turnEvent.comparisonTurn
  ) {
    lastFactSubjectByTopic[turnEvent.topic] = compactObject({
      vehicle: compactVehicle(turnEvent.primaryVehicle),
      source: turnEvent.source,
      confidence: turnEvent.confidence,
      reason: turnEvent.reason,
      resolvedBy: turnEvent.resolvedBy,
      turnId: turnEvent.turnId,
      updatedAt: turnEvent.createdAt,
    });
  }

  return {
    version: LEDGER_VERSION,
    maxTurns: MAX_LEDGER_TURNS,
    turns: nextTurns,
    lastTurn: nextTurns[nextTurns.length - 1] || {},
    lastExplicitSingleVehicle:
      explicitTurns[explicitTurns.length - 1]?.primaryVehicle || {},
    previousExplicitSingleVehicle:
      explicitTurns[explicitTurns.length - 2]?.primaryVehicle || {},
    lastComparisonVehicles:
      comparisonTurns[comparisonTurns.length - 1]?.comparisonVehicles ||
      previousLedger.lastComparisonVehicles ||
      [],
    activeTopic: turnEvent.topic || previousLedger.activeTopic || "general",
    lastFactSubjectByTopic,
    source: turnEvent.source || "context_turn_ledger",
    confidence: Number(turnEvent.confidence) || 0,
    reason: turnEvent.reason || "Turn appended to deterministic context ledger.",
    resolvedBy: turnEvent.resolvedBy || "aci_context_turn_ledger",
    updatedAt: new Date().toISOString(),
  };
}

export function findExplicitRightVehicleFromPatch({
  message = "",
  contextPatch = {},
  response = {},
} = {}) {
  const right = rightSideText(message);
  const comparisonVehicles = comparisonVehiclesFromSources({
    contextPatch,
    response,
  });
  const selected = primaryVehicleFromSources({ contextPatch, response });
  const candidates = uniqueVehicles([...comparisonVehicles, selected]);

  return (
    candidates.find((vehicle) => vehicleMatchesRightSide(vehicle, right)) ||
    comparisonVehicles[comparisonVehicles.length - 1] ||
    selected ||
    {}
  );
}

export function resolveRelativeComparisonFromLedger({
  message = "",
  previousContext = {},
  explicitRightVehicle = {},
} = {}) {
  if (!relativeComparisonRequested(message)) return null;

  const ledger = getContextLedger(previousContext);
  const lastExplicit = compactVehicle(
    ledger.lastExplicitSingleVehicle || {},
  );
  const previousExplicit = compactVehicle(
    ledger.previousExplicitSingleVehicle || {},
  );
  const activeVehicle = compactVehicle(
    previousContext.selectedVehicle ||
      previousContext.contextState?.selectedVehicle ||
      previousContext.aciContextState?.selectedVehicle ||
      {},
  );
  const left = hasVehicleIdentity(lastExplicit)
    ? lastExplicit
    : hasVehicleIdentity(previousExplicit)
      ? previousExplicit
      : activeVehicle;
  const right = compactVehicle(explicitRightVehicle || {});

  if (!hasVehicleIdentity(left) || !hasVehicleIdentity(right)) return null;

  const leftKey = vehicleIdentityKey(left);
  const rightKey = vehicleIdentityKey(right);
  if (leftKey && rightKey && leftKey === rightKey) return null;

  return {
    vehicles: [left, right],
    models: [vehicleLabel(left), vehicleLabel(right)].filter(Boolean),
    source: "context_turn_ledger_relative_reference",
    confidence: 0.97,
    reason:
      "Resolved the relative comparison subject from the immediately preceding explicit single-vehicle turn.",
    resolvedBy: "aci_context_turn_ledger",
  };
}

export function shouldClearComparisonForExplicitSingleVehicle({
  message = "",
  selectedVehicle = {},
  comparisonVehicles = [],
  previousContext = {},
} = {}) {
  const hasVehicle = hasVehicleIdentity(selectedVehicle);
  const comparison =
    isExplicitComparisonMessage(message) ||
    isComparisonFollowUp(message, previousContext) ||
    asArray(comparisonVehicles).length >= 2 &&
      /\b(which|between them|both|them|their)\b/i.test(message);
  const singleVehicleFactIntent =
    /\b(price|on.?road|ex.?showroom|sunroof|abs|airbag|airbags|adas|feature|features|colou?rs?|mileage|range|engine|power|torque|boot|ground clearance|safety|emi|finance)\b/i.test(
      message,
    );

  return Boolean(hasVehicle && singleVehicleFactIntent && !comparison);
}

const preserveActiveComparisonForFollowUp = ({
  patch = {},
  previousContext = {},
  message = "",
} = {}) => {
  if (!isComparisonFollowUp(message, previousContext)) return patch;

  const existing = comparisonVehiclesFromSources({ contextPatch: patch });
  if (existing.length >= 2) return patch;

  const previousVehicles = comparisonVehiclesFromContext(previousContext);
  if (previousVehicles.length < 2) return patch;

  const activeComparison = {
    ...(previousContext.activeComparison ||
      previousContext.contextState?.activeComparison ||
      previousContext.aciContextState?.activeComparison ||
      {}),
    vehicles: previousVehicles,
    source: "context_turn_ledger_comparison_follow_up",
    confidence: 0.94,
  };

  return {
    ...patch,
    activeComparison,
    selectedComparisonSet: {
      ...(previousContext.selectedComparisonSet || {}),
      vehicles: previousVehicles,
      models: previousVehicles.map(vehicleLabel),
      source: "context_turn_ledger_comparison_follow_up",
      confidence: 0.94,
    },
  };
};

const applyComparisonToNestedStates = (patch = {}, activeComparison = {}) => ({
  ...patch,
  contextState: {
    ...(patch.contextState || {}),
    activeComparison,
    anchors: {
      ...(patch.contextState?.anchors || {}),
      comparisonTargets: activeComparison.vehicles || [],
    },
  },
  aciContextState: {
    ...(patch.aciContextState || {}),
    activeComparison,
    anchors: {
      ...(patch.aciContextState?.anchors || {}),
      comparisonTargets: activeComparison.vehicles || [],
    },
  },
});

const stabilizeComparisonFollowUpSelectedVehicle = ({
  patch = {},
  previousContext = {},
  message = "",
  response = {},
  resolvedContext = {},
  candidateSnapshot = {},
} = {}) => {
  if (!isComparisonFollowUp(message, previousContext)) return patch;

  const selectedVehicle = primaryVehicleFromSources({
    contextPatch: patch,
    response,
    resolvedContext,
    candidateSnapshot,
  });
  const selectedModelKey = vehicleModelIdentityKey(selectedVehicle);
  const selectedShortModelKey = keyify(
    selectedVehicle.shortModelKey || modelText(selectedVehicle),
  );
  if (!selectedModelKey && !selectedShortModelKey) return patch;

  const stableVehicle = comparisonVehiclesFromContext(previousContext).find(
    (vehicle) =>
      vehicleModelIdentityKey(vehicle) === selectedModelKey ||
      keyify(vehicle.shortModelKey || modelText(vehicle)) === selectedShortModelKey,
  );
  if (!stableVehicle) return patch;

  const contextState = patch.contextState || {};
  const aciContextState = patch.aciContextState || {};

  return {
    ...patch,
    selectedVehicle: stableVehicle,
    contextState: {
      ...contextState,
      selectedVehicle: stableVehicle,
      anchors: {
        ...(contextState.anchors || {}),
        primaryVehicle: stableVehicle,
      },
    },
    aciContextState: {
      ...aciContextState,
      selectedVehicle: stableVehicle,
      anchors: {
        ...(aciContextState.anchors || {}),
        primaryVehicle: stableVehicle,
      },
    },
  };
};

export function enrichAciContextPatchWithTurnLedger({
  previousContext = {},
  contextPatch = {},
  message = "",
  intent = "",
  tool = "",
  response = {},
  resolvedContext = {},
  candidateSnapshot = {},
} = {}) {
  let patch = preserveActiveComparisonForFollowUp({
    patch: { ...(contextPatch || {}) },
    previousContext,
    message,
  });
  patch = stabilizeComparisonFollowUpSelectedVehicle({
    patch,
    previousContext,
    message,
    response,
    resolvedContext,
    candidateSnapshot,
  });
  const selectedVehicle = primaryVehicleFromSources({
    contextPatch: patch,
    response,
    resolvedContext,
    candidateSnapshot,
  });
  const comparisonVehicles = comparisonVehiclesFromSources({
    contextPatch: patch,
    response,
  });
  const clearComparison = shouldClearComparisonForExplicitSingleVehicle({
    message,
    selectedVehicle,
    comparisonVehicles,
    previousContext,
  });

  if (clearComparison) {
    patch.activeComparison = {};
    patch.selectedComparisonSet = null;
    patch = applyComparisonToNestedStates(patch, {});
  }

  const explicitRightVehicle = findExplicitRightVehicleFromPatch({
    message,
    contextPatch: patch,
    response,
  });
  const relativeComparison = resolveRelativeComparisonFromLedger({
    message,
    previousContext,
    explicitRightVehicle,
  });

  if (relativeComparison?.vehicles?.length >= 2) {
    const relativeSubject = relativeComparison.vehicles[0];
    const activeComparison = {
      ...(patch.activeComparison || {}),
      vehicles: relativeComparison.vehicles,
      models: relativeComparison.models,
      source: relativeComparison.source,
      confidence: relativeComparison.confidence,
      reason: relativeComparison.reason,
      resolvedBy: relativeComparison.resolvedBy,
    };
    patch.activeComparison = activeComparison;
    patch.selectedComparisonSet = {
      ...(patch.selectedComparisonSet || {}),
      vehicles: relativeComparison.vehicles,
      models: relativeComparison.models,
      source: relativeComparison.source,
      confidence: relativeComparison.confidence,
      reason: relativeComparison.reason,
      resolvedBy: relativeComparison.resolvedBy,
    };
    patch.selectedVehicle = relativeSubject;
    patch.contextState = {
      ...(patch.contextState || {}),
      selectedVehicle: relativeSubject,
      anchors: {
        ...(patch.contextState?.anchors || {}),
        primaryVehicle: relativeSubject,
      },
    };
    patch.aciContextState = {
      ...(patch.aciContextState || {}),
      selectedVehicle: relativeSubject,
      anchors: {
        ...(patch.aciContextState?.anchors || {}),
        primaryVehicle: relativeSubject,
      },
    };
    patch = applyComparisonToNestedStates(patch, activeComparison);
  } else if (patch.activeComparison?.vehicles?.length >= 2) {
    patch = applyComparisonToNestedStates(patch, patch.activeComparison);
  }

  const turnEvent = buildAciTurnEvent({
    message,
    intent,
    tool,
    response,
    contextPatch: patch,
    resolvedContext,
    candidateSnapshot,
    previousContext,
  });
  const contextLedger = appendAciTurnLedger(previousContext, turnEvent);
  const contextTrace = compactObject({
    version: TRACE_VERSION,
    turnId: turnEvent.turnId,
    topic: turnEvent.topic,
    explicitSingleVehicleTurn: turnEvent.explicitSingleVehicleTurn,
    comparisonTurn: turnEvent.comparisonTurn,
    comparisonCleared: clearComparison,
    relativeReferenceResolved: Boolean(relativeComparison),
    source: relativeComparison?.source || turnEvent.source,
    confidence: relativeComparison?.confidence || turnEvent.confidence,
    reason: relativeComparison?.reason || turnEvent.reason,
    resolvedBy: relativeComparison?.resolvedBy || turnEvent.resolvedBy,
    selectedVehicle: compactVehicle(selectedVehicle),
    comparisonVehicles:
      relativeComparison?.vehicles ||
      comparisonVehiclesFromSources({ contextPatch: patch, response }),
  });

  patch.contextLedger = contextLedger;
  patch.contextTrace = contextTrace;
  patch.contextState = {
    ...(patch.contextState || {}),
    contextLedger,
    provenance: {
      ...(patch.contextState?.provenance || {}),
      contextTrace,
    },
  };
  patch.aciContextState = {
    ...(patch.aciContextState || {}),
    contextLedger,
    provenance: {
      ...(patch.aciContextState?.provenance || {}),
      contextTrace,
    },
  };

  return patch;
}

export default {
  appendAciTurnLedger,
  buildAciTurnEvent,
  enrichAciContextPatchWithTurnLedger,
  findExplicitRightVehicleFromPatch,
  getContextLedger,
  hasVehicleIdentity,
  resolveRelativeComparisonFromLedger,
  shouldClearComparisonForExplicitSingleVehicle,
  vehicleIdentityKey,
  vehicleLabel,
};
