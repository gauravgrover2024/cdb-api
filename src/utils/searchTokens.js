export const normalizeSearchValue = (value) =>
  String(value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const normalizeSearchCompact = (value) =>
  normalizeSearchValue(value).replace(/[^a-z0-9]/g, "");

export const escapeSearchRegex = (value) =>
  String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const addToken = (tokens, value, { minLength, maxLength }) => {
  const token = String(value ?? "").trim();
  if (token.length < minLength || token.length > maxLength) return;
  tokens.add(token);
};

export const buildSearchTokens = (values = [], options = {}) => {
  const minLength = options.minLength ?? 2;
  const maxLength = options.maxLength ?? 32;
  const maxTokens = options.maxTokens ?? 48;
  const tokens = new Set();

  values.flat().forEach((value) => {
    if (value === undefined || value === null) return;
    const normalized = normalizeSearchValue(value);
    if (!normalized) return;

    normalized.split(" ").forEach((part) => {
      addToken(tokens, part, { minLength, maxLength });
    });

    addToken(tokens, normalizeSearchCompact(normalized), {
      minLength,
      maxLength,
    });
  });

  return Array.from(tokens).slice(0, maxTokens);
};

export const buildSearchTokenFilter = (
  query,
  field = "searchTokens",
  options = {},
) => {
  const minLength = options.minLength ?? 2;
  const maxTokens = options.maxQueryTokens ?? 6;
  const normalized = normalizeSearchValue(query);
  const compact = normalizeSearchCompact(query);
  const queryTokens = normalized
    .split(" ")
    .map((token) => token.trim())
    .filter(
      (token, index, all) =>
        token.length >= minLength && all.indexOf(token) === index,
    )
    .slice(0, maxTokens);

  const compactToken =
    compact.length >= minLength && !queryTokens.includes(compact)
      ? compact
      : "";

  if (!queryTokens.length && !compactToken) return null;

  const tokenClauses = queryTokens.map((token) => ({
    [field]: new RegExp(`^${escapeSearchRegex(token)}`),
  }));

  const compactClause = compactToken
    ? { [field]: new RegExp(`^${escapeSearchRegex(compactToken)}`) }
    : null;

  if (!compactClause) {
    return tokenClauses.length === 1 ? tokenClauses[0] : { $and: tokenClauses };
  }

  if (!tokenClauses.length) return compactClause;

  const tokenFilter =
    tokenClauses.length === 1 ? tokenClauses[0] : { $and: tokenClauses };
  return { $or: [tokenFilter, compactClause] };
};

export const mergeMongoFilters = (...filters) => {
  const clauses = filters.filter(
    (filter) =>
      filter && typeof filter === "object" && Object.keys(filter).length,
  );
  if (!clauses.length) return {};
  if (clauses.length === 1) return clauses[0];
  return { $and: clauses };
};
