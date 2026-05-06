export const MISSING_TEXT_VALUES = new Set([
  "",
  "na",
  "n/a",
  "not available",
  "not captured",
  "pending",
  "unknown",
  "-",
  "--",
  "null",
  "undefined",
]);

export const normalizeText = (value) =>
  String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();

export const escapeRegex = (value) =>
  String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const makeRegex = (value) => {
  const clean = normalizeText(value);
  return clean ? new RegExp(escapeRegex(clean), "i") : null;
};

export const normalizeRegistration = (value) =>
  String(value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

export const normalizeVehicleNumber = normalizeRegistration;

export const digitsOnly = (value) => String(value ?? "").replace(/\D/g, "");

export const registrationSuffix = (value) => {
  const digits = digitsOnly(value);
  return digits.length >= 4 ? digits.slice(-4) : "";
};

export const extractVehicleLast4 = registrationSuffix;

export const normalizeName = (value) =>
  normalizeText(value)
    .replace(/[^a-zA-Z0-9\s.'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const normalizeCitySlug = (value) => {
  const clean = normalizeText(value)
    .toLowerCase()
    .replace(/_/g, "-")
    .replace(/\s+/g, " ")
    .trim();

  if (!clean) return "";

  const compact = clean.replace(/[^a-z0-9]/g, "");

  if (["newdelhi", "delhi", "ncr", "delhincr"].includes(compact)) {
    return "new-delhi";
  }

  if (["gurgaon", "gurugram"].includes(compact)) {
    return "gurgaon";
  }

  if (["bangalore", "bengaluru"].includes(compact)) {
    return "bengaluru";
  }

  return clean.replace(/\s+/g, "-");
};

export const isMissingValue = (value) => {
  if (value === null || value === undefined) return true;
  const text = normalizeText(value).toLowerCase();
  if (MISSING_TEXT_VALUES.has(text)) return true;
  return false;
};

export const formatInr = (value) => {
  const amount = firstNumber(value);
  if (!amount) return "";

  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 0,
  }).format(amount);
};

export const parseBooleanLike = (value) => {
  if (value === true || value === false) return value;

  const text = normalizeText(value).toLowerCase();

  if (
    ["yes", "y", "true", "1", "available", "present", "included"].includes(text)
  ) {
    return true;
  }

  if (
    [
      "no",
      "n",
      "false",
      "0",
      "not available",
      "unavailable",
      "absent",
    ].includes(text)
  ) {
    return false;
  }

  return null;
};

export const firstMeaningful = (...values) =>
  values.find((value) => !isMissingValue(value)) ?? "";

export const firstNumber = (...values) => {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;
    const parsed =
      typeof value === "number"
        ? value
        : Number(String(value).replace(/[^0-9.-]/g, ""));
    if (Number.isFinite(parsed) && parsed !== 0) return parsed;
  }
  return 0;
};

export const asArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
};

export const compactObject = (obj) =>
  Object.fromEntries(
    Object.entries(obj || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      return value !== undefined && value !== null && value !== "";
    }),
  );

export const parseDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

export const latestDate = (...values) => {
  const dates = values.map(parseDate).filter(Boolean);
  if (!dates.length) return null;
  return new Date(Math.max(...dates.map((date) => date.getTime())));
};

export const toNumber = (value) =>
  Number(String(value ?? "").replace(/[^0-9.-]/g, "")) || 0;

export const parseMoneyNumber = toNumber;

export const parseDateRange = (lowerValue) => {
  const lower = normalizeText(lowerValue).toLowerCase();
  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  if (lower.includes("today")) {
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
    return { key: "today", start: start.toISOString(), end: end.toISOString() };
  }
  if (lower.includes("yesterday")) {
    start.setDate(start.getDate() - 1);
    end.setDate(end.getDate() - 1);
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
    return { key: "yesterday", start: start.toISOString(), end: end.toISOString() };
  }
  if (lower.includes("this week")) {
    const day = start.getDay() || 7;
    start.setDate(start.getDate() - day + 1);
    start.setHours(0, 0, 0, 0);
    return { key: "this_week", start: start.toISOString(), end: end.toISOString() };
  }
  if (lower.includes("this month")) {
    start.setDate(1);
    start.setHours(0, 0, 0, 0);
    return { key: "this_month", start: start.toISOString(), end: end.toISOString() };
  }
  if (lower.includes("last month")) {
    start.setMonth(start.getMonth() - 1, 1);
    start.setHours(0, 0, 0, 0);
    end.setDate(0);
    end.setHours(23, 59, 59, 999);
    return { key: "last_month", start: start.toISOString(), end: end.toISOString() };
  }
  if (lower.includes("last 30 days")) {
    start.setDate(start.getDate() - 30);
    start.setHours(0, 0, 0, 0);
    return { key: "last_30_days", start: start.toISOString(), end: end.toISOString() };
  }
  return null;
};

export const getVehicleName = (doc = {}) =>
  normalizeText(
    [
      firstMeaningful(doc.vehicleMake, doc.make, doc.brand),
      firstMeaningful(doc.vehicleModel, doc.model),
      firstMeaningful(doc.vehicleVariant, doc.variant),
    ]
      .filter(Boolean)
      .join(" "),
  );

export const getRegistration = (doc = {}) =>
  firstMeaningful(
    doc.registrationNumber,
    doc.vehicleRegNo,
    doc.rc_redg_no,
    doc.regNo,
    doc?.vehicle?.regNo,
    doc.do_exchangeRegdNumber,
  );

export const pickVehiclePrice = (doc = {}) =>
  firstNumber(
    doc.onRoadPrice,
    doc.on_road_price_cardekho,
    doc.total_on_road_with_accessories,
    doc.exShowroom,
    doc.ex_showroom,
    doc.price,
  );

export const buildMissingValueQuery = (fields) => ({
  $or: fields.flatMap((field) => [
    { [field]: { $exists: false } },
    { [field]: null },
    { [field]: "" },
    { [field]: { $regex: /^(na|n\/a|not available|not captured|pending|unknown|-|--)$/i } },
  ]),
});

export const registrationConditions = (fields, registrationNumber, last4) => {
  const clauses = [];
  const normalized = normalizeRegistration(registrationNumber);
  const suffix = last4 || registrationSuffix(registrationNumber);
  if (normalized) {
    clauses.push(
      ...fields.map((field) => ({
        $expr: {
          $regexMatch: {
            input: {
              $replaceAll: {
                input: {
                  $replaceAll: {
                    input: { $toUpper: { $ifNull: [`$${field}`, ""] } },
                    find: " ",
                    replacement: "",
                  },
                },
                find: "-",
                replacement: "",
              },
            },
            regex: escapeRegex(normalized),
          },
        },
      })),
    );
  }
  if (suffix) {
    clauses.push(
      ...fields.map((field) => ({
        [field]: { $regex: `${escapeRegex(suffix)}$`, $options: "i" },
      })),
    );
  }
  return clauses;
};

export const formatDateValue = (value) => {
  const date = parseDate(value);
  return date ? date.toISOString() : null;
};
