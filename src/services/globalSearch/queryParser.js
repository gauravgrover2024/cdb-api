const STOP_WORDS = new Set([
  "of",
  "the",
  "a",
  "an",
  "show",
  "open",
  "find",
  "search",
  "for",
  "record",
  "records",
  "details",
  "detail",
  "please",
  "ka",
  "ki",
  "ke",
  "ko",
  "mein",
  "me",
  "hai",
  "aur",
  "and",
  "latest",
  "new",
  "current",
  "approx",
  "approximately",
  "showing",
  "with",
  "in",
  "on",
  "to",
  "from",
]);

const INTENT_KEYWORDS = {
  insurance: [
    "insurance",
    "policy",
    "od",
    "tp",
    "zerodep",
    "zero",
    "comprehensive",
    "renewal",
    "rollover",
    "expired",
  ],
  loan: [
    "loan",
    "emi",
    "closure",
    "closed",
    "closing",
    "foreclose",
    "foreclosure",
    "disburse",
    "sanction",
    "approval",
  ],
  payment: [
    "payment",
    "paid",
    "receipt",
    "receivable",
    "payable",
    "outstanding",
    "pending",
    "subvention",
  ],
  challan: [
    "challan",
    "traffic",
    "vahan",
    "peshi",
    "penalty",
    "tax",
    "blacklist",
    "noc",
  ],
  rc: ["rc", "registration", "reg", "rto", "engine", "chassis"],
  document: [
    "document",
    "documents",
    "doc",
    "rcbook",
    "invoice",
    "aadhaar",
    "aadhar",
    "pan",
    "gst",
    "policycopy",
  ],
  inspection: ["inspection", "inspect", "evaluator", "nogo", "refurb"],
  vehicle: [
    "vehicle",
    "car",
    "bike",
    "variant",
    "model",
    "make",
    "color",
    "pricelist",
    "ex showroom",
    "ex-showroom",
    "on road",
    "on-road",
    "quotation",
    "quote",
    "test drive",
    "colors",
    "colours",
    "features",
    "compare",
  ],
  latest: ["latest", "new", "recent", "current"],
};

export const normalizeText = (value) =>
  String(value ?? "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ");

export const normalizeAlphaNum = (value) =>
  String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

export const normalizeRegistration = (value) =>
  String(value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

export const digitsOnly = (value) => String(value ?? "").replace(/\D/g, "");

export const escapeRegex = (value = "") =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Build a registration regex that tolerates separators/spaces.
 * Example:
 *   DL08CAX4577 -> D[^A-Z0-9]*L[^A-Z0-9]*0...*7
 */
export const buildLooseRegPattern = (value = "") => {
  const normalized = normalizeRegistration(value);
  if (!normalized) return "";
  const chars = normalized.split("").map((c) => escapeRegex(c));
  return chars.join("[^A-Za-z0-9]*");
};

const isRegistrationLike = (value = "") => {
  const token = normalizeRegistration(value);
  if (!token || token.length < 6 || token.length > 14) return false;
  const letters = (token.match(/[A-Z]/g) || []).length;
  const digits = (token.match(/\d/g) || []).length;
  return letters >= 2 && digits >= 2;
};

const extractVehicleLast4 = (query = "", tokens = []) => {
  const regCompact = normalizeRegistration(query);
  const match = regCompact.match(/([A-Z]{1,3}\d{1,2}[A-Z]{0,3}\d{4})$/);
  if (match) return match[1].slice(-4);

  const directFour = tokens.find((token) => /^\d{4}$/.test(token));
  if (directFour) return directFour;

  return "";
};

const inferIntents = (tokens = [], normalizedQuery = "") => {
  const intents = new Set();
  const compact = normalizeAlphaNum(normalizedQuery);
  for (const [intent, keywords] of Object.entries(INTENT_KEYWORDS)) {
    const matched = keywords.some((keyword) => {
      const normalizedKeyword = normalizeText(keyword);
      const compactKeyword = normalizeAlphaNum(keyword);
      const tokenMatch = tokens.includes(normalizedKeyword);
      if (tokenMatch) return true;

      const wordBoundaryMatch = new RegExp(
        `(^|\\s)${escapeRegex(normalizedKeyword)}(\\s|$)`,
        "i",
      ).test(normalizedQuery);
      if (wordBoundaryMatch) return true;

      // Compact-match only for meaningful keywords (avoid tiny noise like "tp")
      if (compactKeyword.length >= 3) {
        return compact.includes(compactKeyword);
      }
      return false;
    });
    if (matched) intents.add(intent);
  }
  return Array.from(intents);
};

export const parseGlobalSearchQuery = (query = "") => {
  const rawQuery = String(query ?? "").trim();
  const normalizedQuery = normalizeText(rawQuery);
  const compactQuery = normalizeAlphaNum(rawQuery);
  const registrationQuery = normalizeRegistration(rawQuery);

  const tokensRaw = normalizedQuery
    .split(" ")
    .map((token) => token.trim())
    .filter(Boolean);

  const tokens = tokensRaw.filter((token) => !STOP_WORDS.has(token));
  const numberTokens = tokensRaw.filter((token) => /\d/.test(token));
  const alphaTokens = tokensRaw.filter((token) => /[a-z]/.test(token));

  const phoneToken = numberTokens.find(
    (token) => digitsOnly(token).length >= 7,
  );
  const vehicleLast4 = extractVehicleLast4(rawQuery, tokensRaw);
  const intents = inferIntents(tokensRaw, normalizedQuery);
  const registrationCandidates = new Set();

  tokensRaw.forEach((token) => {
    if (isRegistrationLike(token)) {
      registrationCandidates.add(normalizeRegistration(token));
    }
  });

  // Catch split registration styles like "DL 8C AX 4577".
  for (let i = 0; i < tokensRaw.length; i += 1) {
    for (let len = 2; len <= 4; len += 1) {
      const slice = tokensRaw.slice(i, i + len);
      if (!slice.length) continue;
      const merged = normalizeRegistration(slice.join(""));
      if (isRegistrationLike(merged)) {
        registrationCandidates.add(merged);
      }
    }
  }

  // Only keep compact full-query registration if it actually looks like one.
  if (isRegistrationLike(registrationQuery)) {
    registrationCandidates.add(registrationQuery);
  }

  return {
    rawQuery,
    normalizedQuery,
    compactQuery,
    registrationQuery,
    tokens,
    tokensRaw,
    numberTokens,
    alphaTokens,
    vehicleLast4,
    registrationCandidates: Array.from(registrationCandidates),
    possiblePhoneDigits: phoneToken ? digitsOnly(phoneToken) : "",
    wantsLatest: intents.includes("latest"),
    intents,
  };
};
