import mongoose from "mongoose";
import {
  filterChip,
  sourceTransparency,
} from "./aiAgent.renderPayloads.js";
import {
  firstMeaningful,
  formatDateValue,
  getRegistration,
  getVehicleName,
  makeRegex,
  normalizeText,
  registrationConditions,
} from "./aiAgent.normalizers.js";

export const LIMIT = 50;
export const QUERY_TIMEOUT_MS = 3500;

export const safeId = (doc) => String(doc?._id || doc?.id || "");

export const objectIdOrNull = (value) =>
  mongoose.Types.ObjectId.isValid(value) ? new mongoose.Types.ObjectId(value) : null;

export const pushModuleTrace = (trace, module, matched = 0, extra = {}) => {
  trace.push({ module, matched, ...extra });
};

export const findLean = (Model, query, options = {}) => {
  let builder = Model.find(query);
  if (options.select) builder = builder.select(options.select);
  if (options.sort) builder = builder.sort(options.sort);
  if (options.limit) builder = builder.limit(options.limit);
  return builder.maxTimeMS(options.maxTimeMS || QUERY_TIMEOUT_MS).lean();
};

export const countDocumentsSafe = async (Model, query, options = {}) => {
  try {
    return {
      count: await Model.countDocuments(query).maxTimeMS(options.maxTimeMS || QUERY_TIMEOUT_MS),
      approximate: false,
    };
  } catch (error) {
    return {
      count: options.fallbackCount || 0,
      approximate: true,
      error: error.message,
    };
  }
};

export const findAndCount = async (Model, query, options = {}) => {
  const [rowsResult, countResult] = await Promise.allSettled([
    findLean(Model, query, options),
    countDocumentsSafe(Model, query, options),
  ]);
  const rows = rowsResult.status === "fulfilled" ? rowsResult.value : [];
  const countPayload =
    countResult.status === "fulfilled"
      ? countResult.value
      : { count: rows.length, approximate: true, error: countResult.reason?.message };
  return {
    rows,
    count: countPayload.count || rows.length,
    approximate: countPayload.approximate,
    error: rowsResult.status === "rejected" ? rowsResult.reason?.message : countPayload.error,
  };
};

export const buildTextClauses = (fields, value) => {
  const regex = makeRegex(value);
  return regex ? fields.map((field) => ({ [field]: regex })) : [];
};

export const buildEntityQuery = ({
  customerFields = [],
  registrationFields = [],
  vehicleFields = [],
  entities = {},
} = {}) => {
  const and = [];
  const customer = buildTextClauses(customerFields, entities.customerName);
  if (customer.length) and.push({ $or: customer });

  const registration = registrationConditions(
    registrationFields,
    entities.registrationNumber,
    entities.last4,
  );
  if (registration.length) and.push({ $or: registration });

  const vehicleNeedles = [entities.make, entities.model, entities.variant].filter(Boolean);
  for (const needle of vehicleNeedles) {
    const clauses = buildTextClauses(vehicleFields, needle);
    if (clauses.length) and.push({ $or: clauses });
  }

  return and.length ? { $and: and } : {};
};

export const canSearchByEntity = (entities = {}) =>
  Boolean(entities.customerName || entities.registrationNumber || entities.last4 || entities.model);

export const getLoanRoute = (loan) => `/loans/edit/${safeId(loan)}`;
export const getInsuranceRoute = (insuranceCase) =>
  `/insurance/edit/${insuranceCase?.caseId || safeId(insuranceCase)}`;
export const getCustomerRoute = (customer) => `/customers/edit/${safeId(customer)}`;
export const getPaymentRoute = (loan) => `/payments/${loan?.loanId || safeId(loan)}`;
export const getUsedCarRoute = (lead) => `/used-cars/leads/${safeId(lead)}`;

export const rowBase = (doc = {}) => ({
  id: safeId(doc),
  customer: firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name),
  vehicle: getVehicleName(doc),
  registrationNumber: getRegistration(doc),
  status: firstMeaningful(doc.status, doc.loanStatus, doc.currentStage, doc?.workflow?.status),
  createdAt: formatDateValue(doc.createdAt),
  updatedAt: formatDateValue(doc.updatedAt),
});

export const makeAmbiguity = (options) => {
  if (!options?.length) return null;
  return {
    message: "I found multiple possible matches. Which one do you mean?",
    options: options.slice(0, 8).map((option) => ({
      id: option.id,
      entityType: option.entityType,
      displayName: option.displayName,
      customerName: option.customerName,
      vehicle: option.vehicle,
      registrationNumber: option.registrationNumber,
      module: option.module,
      status: option.status,
      lastActivityDate: option.lastActivityDate,
      context: option.context,
    })),
  };
};

export const buildFilters = (parsed, moduleName = "") =>
  [
    filterChip("intent", "Intent", parsed.intent.replace(/_/g, " ")),
    filterChip("customer", "Customer", parsed.entities.customerName),
    filterChip("make", "Make", parsed.entities.make),
    filterChip("model", "Model", parsed.entities.model),
    filterChip("variant", "Variant", parsed.entities.variant),
    filterChip("last4", "Vehicle Last 4", parsed.entities.last4),
    filterChip("module", "Module", moduleName),
    ...parsed.statusTerms.map((status) => filterChip(`status_${status}`, "Status", status)),
  ].filter(Boolean);

export const assembleResponse = ({
  parsed,
  assistantMessage,
  resultType = "answer",
  widgets = [],
  modulesChecked = [],
  filtersApplied = [],
  followUpSuggestions = [],
  ambiguity,
  access,
  queryPlan,
  filters,
}) => {
  const response = {
    assistantMessage,
    intent: parsed.intent,
    entities: parsed.entities,
    confidence: parsed.confidence,
    filters: filters || buildFilters(parsed),
    resultType,
    widgets,
    sourceTransparency: sourceTransparency({
      modulesChecked,
      filtersApplied,
      accessRestrictions: access?.restrictions || [],
    }),
    followUpSuggestions,
  };
  if (ambiguity) response.ambiguity = ambiguity;
  if (queryPlan) response.queryPlan = queryPlan;
  return response;
};

export const latestActivity = (doc) =>
  formatDateValue(
    firstMeaningful(doc?.updatedAt, doc?.createdAt, doc?.newIssueDate, doc?.newPolicyStartDate),
  );

export const entityOption = (doc, module, entityType, context = {}) => ({
  id: safeId(doc),
  entityType,
  displayName: normalizeText(
    [firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name), getVehicleName(doc), getRegistration(doc)]
      .filter(Boolean)
      .join(" - "),
  ),
  customerName: firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name),
  vehicle: getVehicleName(doc),
  registrationNumber: getRegistration(doc),
  module,
  status: firstMeaningful(doc.status, doc.loanStatus, doc.currentStage, doc?.workflow?.status),
  lastActivityDate: latestActivity(doc),
  context: {
    customerName: firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name),
    registrationNumber: getRegistration(doc),
    last4: getRegistration(doc).replace(/\D/g, "").slice(-4),
    ...context,
  },
});
