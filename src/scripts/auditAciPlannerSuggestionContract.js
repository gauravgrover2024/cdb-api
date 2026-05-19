import dotenv from "dotenv";
import mongoose from "mongoose";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const queries = [
  "Creta King",
  "Show features of Creta King",
  "Does Creta King have ADAS?",
  "Does Creta King have sunroof?",
  "Which Creta variants have sunroof?",
  "Cheapest Creta variant with ADAS",
  "Compare Creta E vs EX Diesel features",
  "Verna SX sunroof",
  "Show features of Verna",
];

const suggestionKeys = [
  "leadingQuestions",
  "conversationSuggestions",
  "suggestedQuestions",
  "nextQuestions",
  "followUps",
  "followUpQuestions",
  "quickReplies",
  "quickActions",
  "recommendedActions",
  "actionPills",
  "chips",
  "suggestions",
];

const pickSuggestionFields = (obj = {}) => {
  const found = {};

  for (const key of suggestionKeys) {
    if (obj?.[key] !== undefined) found[key] = obj[key];
    if (obj?.data?.[key] !== undefined) found[`data.${key}`] = obj.data[key];
    if (obj?.widget?.[key] !== undefined) found[`widget.${key}`] = obj.widget[key];
    if (obj?.canvas?.[key] !== undefined) found[`canvas.${key}`] = obj.canvas[key];
    if (obj?.inlineCard?.[key] !== undefined) found[`inlineCard.${key}`] = obj.inlineCard[key];
  }

  return found;
};

const compactResponse = (response = {}) => ({
  intent: response.intent,
  displayMode: response.displayMode,
  canvasType: response.canvasType || response.canvas?.type || response.widget?.type,
  inlineType: response.inlineType || response.inlineCard?.type,
  title: response.title,
  answer: response.answer,
  suggestionFields: pickSuggestionFields(response),
  topLevelKeys: Object.keys(response).sort(),
  dataKeys: response.data ? Object.keys(response.data).sort() : [],
  canvasKeys: response.canvas ? Object.keys(response.canvas).sort() : [],
  widgetKeys: response.widget ? Object.keys(response.widget).sort() : [],
  inlineCardKeys: response.inlineCard ? Object.keys(response.inlineCard).sort() : [],
});

const main = async () => {
  await mongoose.connect(mongoUri);

  console.log("============================================================");
  console.log("ACI PLANNER / LIVE CHAT SUGGESTION CONTRACT AUDIT");
  console.log("============================================================");

  for (const query of queries) {
    console.log(`\n================ QUERY: ${query} ================`);

    const response = await chatWithAgent({
      message: query,
      sessionId: `suggestion-contract-${Date.now()}`,
      context: {
        source: "suggestion_contract_audit",
      },
      debug: true,
      user: null,
    });

    console.dir(compactResponse(response), { depth: 10 });
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
