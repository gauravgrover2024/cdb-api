import mongoose from "mongoose";

const TOOL_NAME = "vehicle_finance_knowledge";
const COLLECTION_NAME =
  process.env.ACI_FINANCE_KNOWLEDGE_COLLECTION || "aci_finance_knowledge_v1";
const CACHE_TTL_MS = Math.max(
  60_000,
  Number(process.env.ACI_FINANCE_KNOWLEDGE_CACHE_TTL_MS || 15 * 60_000),
);

let cache = { expiresAt: 0, records: [] };

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const unique = (values = []) => [...new Set(values.filter(Boolean))];

export const detectFinanceKnowledgeTopics = (message = "") => {
  const text = cleanText(message).toLowerCase();
  const topics = [];

  if (/\b(document|documents|paperwork|papers|kyc|proofs?)\b/.test(text)) {
    topics.push("documents");
  }
  if (/\b(eligible|eligibility|qualify|qualification|approval|approved)\b/.test(text)) {
    topics.push("eligibility");
  }
  if (/\b(can i (?:get|qualify for).*loan|get a car loan|will i get.*loan|loan possible)\b/.test(text)) {
    topics.push("eligibility");
  }
  if (/\b(cibil|credit score|credit history|bureau score)\b/.test(text)) {
    topics.push("credit_score");
  }
  if (/\b(down payment|funding|finance percentage|loan percentage|90% loan|100% loan)\b/.test(text)) {
    topics.push("down_payment");
  }
  if (/\b(interest|rate of interest|roi|tenure|loan period)\b/.test(text)) {
    topics.push("interest_tenure");
  }
  if (/\b(processing fee|loan charges?|finance charges?|processing charges?|foreclos|prepay|part payment)\b/.test(text)) {
    topics.push("fees_preclosure");
  }
  if (/\b(apply|application|process|how to get|steps)\b/.test(text)) {
    topics.push("application_process");
  }

  return unique(topics.length ? topics : ["overview"]);
};

const detectApplicantType = (message = "") => {
  const text = cleanText(message).toLowerCase();
  if (/\b(self[ -]?employed|business owner|proprietor|partner|company owner)\b/.test(text)) {
    return "self_employed";
  }
  if (/\b(salaried|salary|employee|job)\b/.test(text)) return "salaried";
  return "general";
};

const readPublishedRecords = async () => {
  if (cache.expiresAt > Date.now() && cache.records.length) return cache.records;

  const db = mongoose.connection?.db;
  if (!db) return [];

  const records = await db
    .collection(COLLECTION_NAME)
    .find(
      { status: "published", active: { $ne: false } },
      {
        projection: {
          _id: 0,
          key: 1,
          topic: 1,
          applicantType: 1,
          priority: 1,
          title: 1,
          summary: 1,
          checklist: 1,
          caveats: 1,
          nextQuestion: 1,
          sourceLinks: 1,
          reviewedAt: 1,
          version: 1,
        },
      },
    )
    .sort({ priority: 1, key: 1 })
    .toArray();

  cache = { expiresAt: Date.now() + CACHE_TTL_MS, records };
  return records;
};

const selectRecords = ({ records = [], topics = [], applicantType = "general" }) => {
  const exact = records.filter(
    (record) =>
      topics.includes(record.topic) &&
      ["general", applicantType].includes(record.applicantType || "general"),
  );
  if (exact.length) return exact;

  return records.filter(
    (record) =>
      record.topic === "overview" &&
      (record.applicantType || "general") === "general",
  );
};

export const runVehicleFinanceKnowledgeTool = async ({
  toolPlan = {},
  userMessage = "",
} = {}) => {
  const requestedTopics = unique([
    ...(Array.isArray(toolPlan.entities?.topics) ? toolPlan.entities.topics : []),
    ...detectFinanceKnowledgeTopics(userMessage),
  ]);
  const applicantType = detectApplicantType(userMessage);
  const records = selectRecords({
    records: await readPublishedRecords(),
    topics: requestedTopics,
    applicantType,
  });
  const sourceLinks = unique(
    records.flatMap((record) =>
      (Array.isArray(record.sourceLinks) ? record.sourceLinks : [])
        .map((source) => source?.url)
        .filter(Boolean),
    ),
  );

  return {
    tool: TOOL_NAME,
    topics: requestedTopics,
    applicantType,
    records,
    count: records.length,
    matched: records.length,
    modulesChecked: [COLLECTION_NAME],
    dataSource: records.length ? COLLECTION_NAME : "finance_knowledge_unavailable",
    sourceTransparency: {
      modulesChecked: [COLLECTION_NAME],
      matched: records.length,
      dataSource: records.length ? COLLECTION_NAME : "finance_knowledge_unavailable",
      sourceLinks,
      reviewedAt: records
        .map((record) => record.reviewedAt)
        .filter(Boolean)
        .sort()
        .at(-1),
    },
    meta: {
      status: records.length ? "ready" : "no_published_guidance",
      collection: COLLECTION_NAME,
      liveLenderOfferUsed: false,
    },
  };
};

export const clearVehicleFinanceKnowledgeCache = () => {
  cache = { expiresAt: 0, records: [] };
};

export default runVehicleFinanceKnowledgeTool;
