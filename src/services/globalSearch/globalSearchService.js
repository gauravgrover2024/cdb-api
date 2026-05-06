import { SEARCH_ADAPTERS } from "./adapters.js";
import {
  buildLooseRegPattern,
  digitsOnly,
  escapeRegex,
  normalizeAlphaNum,
  normalizeRegistration,
  normalizeText,
  parseGlobalSearchQuery,
} from "./queryParser.js";

const getByPath = (obj, path) => {
  if (!obj || !path) return "";
  const parts = String(path).split(".");
  let cursor = obj;
  for (const part of parts) {
    if (cursor === undefined || cursor === null) return "";
    cursor = cursor[part];
  }
  if (cursor === undefined || cursor === null) return "";
  if (Array.isArray(cursor)) return cursor.join(" ");
  if (typeof cursor === "object") return JSON.stringify(cursor);
  return String(cursor);
};

const getValues = (doc, paths = []) =>
  paths
    .map((path) => getByPath(doc, path))
    .filter(Boolean)
    .map((value) => String(value));

const addRegexConditions = (conditions, fields = [], pattern, options = "i") => {
  if (!pattern || !fields.length) return;
  let regex;
  try {
    regex = new RegExp(pattern, options);
  } catch {
    return;
  }
  fields.forEach((field) => {
    conditions.push({ [field]: { $regex: regex } });
  });
};

const addExactConditions = (conditions, fields = [], value) => {
  if (!value || !fields.length) return;
  fields.forEach((field) => {
    conditions.push({ [field]: value });
  });
};

const buildAdapterFilter = (adapter, parsed) => {
  const fields = adapter.fields || {};
  const clauses = [];

  const idFields = [...(fields.id || []), ...(fields.reference || [])];
  const textFields = [
    ...(fields.name || []),
    ...(fields.vehicle || []),
    ...(fields.text || []),
    ...(fields.status || []),
  ];

  if (parsed.rawQuery && parsed.rawQuery.length >= 3) {
    addExactConditions(clauses, idFields, parsed.rawQuery);
  }

  if (parsed.compactQuery && parsed.compactQuery.length >= 3) {
    addExactConditions(clauses, idFields, parsed.compactQuery);
  }

  const registrationCandidates = Array.isArray(parsed.registrationCandidates)
    ? parsed.registrationCandidates
    : [];
  registrationCandidates.forEach((candidate) => {
    if (!candidate || candidate.length < 4) return;
    addExactConditions(clauses, fields.registration || [], candidate);
    const regPattern = buildLooseRegPattern(candidate);
    if (regPattern) {
      addRegexConditions(clauses, fields.registration || [], regPattern);
    }
  });

  if (parsed.possiblePhoneDigits && parsed.possiblePhoneDigits.length >= 7) {
    addExactConditions(clauses, fields.phone || [], parsed.possiblePhoneDigits);
  }

  if (parsed.compactQuery && parsed.compactQuery.length >= 2) {
    addRegexConditions(clauses, idFields, escapeRegex(parsed.compactQuery));
    addRegexConditions(clauses, fields.registration || [], escapeRegex(parsed.compactQuery));
  }

  if (parsed.normalizedQuery && parsed.normalizedQuery.length >= 2) {
    addRegexConditions(clauses, textFields, escapeRegex(parsed.normalizedQuery));
    addRegexConditions(clauses, fields.email || [], escapeRegex(parsed.normalizedQuery));
  }

  if (parsed.vehicleLast4) {
    addRegexConditions(clauses, fields.registration || [], `${escapeRegex(parsed.vehicleLast4)}$`);
  }

  if (parsed.possiblePhoneDigits) {
    addRegexConditions(clauses, fields.phone || [], escapeRegex(parsed.possiblePhoneDigits));
  }

  parsed.tokens.slice(0, 7).forEach((token) => {
    if (!token || token.length < 2) return;
    if (/^\d+$/.test(token)) {
      addRegexConditions(clauses, [...idFields, ...(fields.registration || []), ...(fields.phone || [])], escapeRegex(token));
      return;
    }
    addRegexConditions(clauses, textFields, escapeRegex(token));
  });

  if (!clauses.length) return null;
  return { $or: clauses.slice(0, 100) };
};

const daysDiff = (dateValue) => {
  const date = new Date(dateValue);
  if (Number.isNaN(date.getTime())) return null;
  return Math.floor((Date.now() - date.getTime()) / (24 * 60 * 60 * 1000));
};

const scoreDocument = (adapter, doc, parsed) => {
  const fields = adapter.fields || {};
  const matched = new Set();
  let score = 0;

  const idValues = getValues(doc, fields.id).map((v) => normalizeAlphaNum(v));
  const regValues = getValues(doc, fields.registration).map((v) => normalizeRegistration(v));
  const nameValues = getValues(doc, fields.name).map((v) => normalizeText(v));
  const phoneValues = getValues(doc, fields.phone).map((v) => digitsOnly(v));
  const emailValues = getValues(doc, fields.email).map((v) => normalizeText(v));
  const vehicleValues = getValues(doc, fields.vehicle).map((v) => normalizeText(v));
  const refValues = getValues(doc, fields.reference).map((v) => normalizeText(v));
  const statusValues = getValues(doc, fields.status).map((v) => normalizeText(v));

  if (parsed.compactQuery) {
    if (idValues.some((v) => v && v === parsed.compactQuery)) {
      score += 130;
      matched.add("ID");
    } else if (idValues.some((v) => v && v.includes(parsed.compactQuery))) {
      score += 85;
      matched.add("ID");
    }
  }

  const registrationCandidates = Array.isArray(parsed.registrationCandidates)
    ? parsed.registrationCandidates
    : [];
  if (registrationCandidates.length) {
    const exactReg = registrationCandidates.some((candidate) =>
      regValues.some((v) => v && v === candidate),
    );
    if (exactReg) {
      score += 125;
      matched.add("Registration");
    } else {
      const partialReg = registrationCandidates.some((candidate) =>
        regValues.some((v) => v && v.includes(candidate)),
      );
      if (partialReg) {
        score += 82;
        matched.add("Registration");
      }
    }
  }

  if (parsed.vehicleLast4 && regValues.some((v) => v.endsWith(parsed.vehicleLast4))) {
    score += 96;
    matched.add("Vehicle Last 4");
  }

  if (parsed.possiblePhoneDigits) {
    if (phoneValues.some((v) => v === parsed.possiblePhoneDigits || v.endsWith(parsed.possiblePhoneDigits))) {
      score += 90;
      matched.add("Mobile");
    } else if (phoneValues.some((v) => v.includes(parsed.possiblePhoneDigits))) {
      score += 62;
      matched.add("Mobile");
    }
  }

  if (parsed.normalizedQuery) {
    if (nameValues.some((v) => v === parsed.normalizedQuery)) {
      score += 90;
      matched.add("Customer");
    } else if (nameValues.some((v) => v.includes(parsed.normalizedQuery))) {
      score += 65;
      matched.add("Customer");
    }

    if (emailValues.some((v) => v.includes(parsed.normalizedQuery))) {
      score += 52;
      matched.add("Email");
    }
  }

  parsed.tokens.forEach((token) => {
    if (!token || token.length < 2) return;
    if (nameValues.some((v) => v.includes(token))) {
      score += 16;
      matched.add("Customer");
    }
    if (vehicleValues.some((v) => v.includes(token))) {
      score += 14;
      matched.add("Vehicle");
    }
    if (refValues.some((v) => v.includes(token))) {
      score += 10;
      matched.add("Reference");
    }
    if (statusValues.some((v) => v.includes(token))) {
      score += 12;
      matched.add("Status");
    }
  });

  if (parsed.vehicleLast4 && matched.has("Vehicle Last 4") && matched.has("Customer")) {
    score += 32;
  }

  const adapterIntents = adapter.intents || [];
  const sharedIntents = parsed.intents.filter((intent) => adapterIntents.includes(intent));
  if (sharedIntents.length) {
    score += Math.min(60, sharedIntents.length * 22);
  }

  if (parsed.wantsLatest) {
    const datePath = fields.dateField;
    const ageDays = daysDiff(getByPath(doc, datePath));
    if (ageDays !== null) {
      score += Math.max(0, 30 - Math.floor(ageDays / 7));
      matched.add("Latest");
    }
  }

  return {
    score,
    matchedFields: Array.from(matched).slice(0, 5),
  };
};

const runAdapterSearch = async (adapter, parsed, perEntityLimit) => {
  const filter = buildAdapterFilter(adapter, parsed);
  if (!filter) return [];

  const limit = Math.max(1, Math.min(20, Number(perEntityLimit || adapter.limit || 8)));
  let docs = [];
  try {
    docs = await adapter.model
      .find(filter, adapter.projection || {})
      .sort({ updatedAt: -1, createdAt: -1, _id: -1 })
      .limit(limit)
      .lean();
  } catch (error) {
    console.warn(
      `[GlobalSearch] adapter "${adapter.key}" failed: ${error?.message || "unknown error"}`,
    );
    return [];
  }

  return docs
    .map((doc) => {
      const scoreMeta = scoreDocument(adapter, doc, parsed);
      const mapped = adapter.result(doc);
      return {
        id: `${adapter.key}:${String(doc._id)}`,
        entity: adapter.key,
        entityLabel: adapter.label,
        recordMongoId: String(doc._id),
        recordId: mapped.recordId || String(doc._id),
        title: mapped.title || adapter.label,
        subtitle: mapped.subtitle || "",
        status: mapped.status || "",
        badge: mapped.badge || "",
        route: mapped.route || "",
        updatedAt: mapped.updatedAt || doc.updatedAt || doc.createdAt || null,
        matchedFields: scoreMeta.matchedFields,
        score: scoreMeta.score,
      };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);
};

export const runGlobalSearch = async ({ query, limit = 40, perEntityLimit = 8 }) => {
  const parsed = parseGlobalSearchQuery(query);

  if (!parsed.rawQuery || parsed.rawQuery.length < 2) {
    return {
      query: parsed,
      total: 0,
      results: [],
      groups: [],
    };
  }

  const adapterResults = await Promise.all(
    SEARCH_ADAPTERS.map((adapter) => runAdapterSearch(adapter, parsed, perEntityLimit)),
  );

  const flat = adapterResults
    .flat()
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const aTime = a.updatedAt ? new Date(a.updatedAt).getTime() : 0;
      const bTime = b.updatedAt ? new Date(b.updatedAt).getTime() : 0;
      return bTime - aTime;
    })
    .slice(0, Math.max(1, Math.min(100, Number(limit || 40))));

  const groupedMap = new Map();
  flat.forEach((item) => {
    if (!groupedMap.has(item.entity)) {
      groupedMap.set(item.entity, {
        entity: item.entity,
        label: item.entityLabel,
        count: 0,
        results: [],
      });
    }
    const group = groupedMap.get(item.entity);
    group.count += 1;
    group.results.push(item);
  });

  const groups = Array.from(groupedMap.values()).sort((a, b) => b.count - a.count);

  return {
    query: parsed,
    total: flat.length,
    results: flat,
    groups,
  };
};
