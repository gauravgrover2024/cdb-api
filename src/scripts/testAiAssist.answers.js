import "dotenv/config";

import * as dbModule from "../config/db.js";
import * as aiAgentService from "../services/aiAgent/aiAgent.service.js";

const connect =
  dbModule.connectDB ||
  dbModule.connectDatabase ||
  dbModule.connectMongoDB ||
  dbModule.default;

if (typeof connect !== "function") {
  console.error("Could not find DB connect function in src/config/db.js");
  console.error("Available db exports:", Object.keys(dbModule));
  process.exit(1);
}

const chatWithAgent =
  aiAgentService.chatWithAgent ||
  aiAgentService.handleAiAgentChat ||
  aiAgentService.default;

if (typeof chatWithAgent !== "function") {
  console.error("Could not find chat function in src/services/aiAgent/aiAgent.service.js");
  console.error("Available aiAgent.service exports:", Object.keys(aiAgentService));
  process.exit(1);
}

const adminUser = {
  _id: "answer-smoke-test",
  id: "answer-smoke-test",
  role: "admin",
  permissions: ["*"],
  name: "Answer Smoke Test",
};

const sharedContext = {
  selectedVehicle: {
    brand: "Hyundai",
    model: "Verna",
    variant: "SX IVT",
    city: "new-delhi",
  },
  anchorBrand: "Hyundai",
  anchorModel: "Verna",
  anchorVariant: "SX IVT",
  anchorCity: "new-delhi",
};

const tests = [
  "Best automatic SUV under 20 lakh with sunroof and 6 airbags",
  "Cars with 6 airbags under 15 lakh",
  "CNG or petrol which is better for daily 50 km running?",
  "Best price for black Verna SX automatic",
  "Show Verna price in Delhi, compare with City, tell EMI for 5 years and check offers",
  "Loan closure 7077",
  "Can I get the car in my company name?",
  "Does Verna SX have sunroof?",
  "Compare with City",
  "black available?",
];

const callAgent = async (query) => {
  const attempts = [
    // Most likely object-style signature.
    () =>
      chatWithAgent({
        message: query,
        query,
        context: sharedContext,
        selectedEntity: null,
        filters: {},
        user: adminUser,
        currentUser: adminUser,
      }),

    // Some services use user as second arg.
    () =>
      chatWithAgent(
        {
          message: query,
          query,
          context: sharedContext,
          selectedEntity: null,
          filters: {},
        },
        adminUser,
      ),

    // Some services use positional message/context/user.
    () => chatWithAgent(query, sharedContext, adminUser),
  ];

  let lastError = null;

  for (const attempt of attempts) {
    try {
      const response = await attempt();
      if (response) return response;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("chatWithAgent returned empty response");
};

await connect();

for (const query of tests) {
  try {
    const response = await callAgent(query);

    const normalized = {
      query,
      intent: response.intent || response.plan?.tools?.[0]?.tool || response.tool || "",
      plannerMode: response.plannerMode || response.meta?.plannerMode || response.sourceTransparency?.plannerMode || "",
      displayMode: response.displayMode || "",
      canvasType: response.canvasType || response.output?.canvasType || "",
      inlineType: response.inlineType || response.output?.inlineType || "",
      answer:
        response.answer ||
        response.message ||
        response.text ||
        response.response ||
        response.assistantMessage ||
        "",
      actions: response.actions || response.nextSteps || response.followUps || [],
      leadingQuestions:
        response.leadingQuestions ||
        response.suggestedQuestions ||
        response.followUpQuestions ||
        [],
      conversationSuggestions:
        response.conversationSuggestions ||
        response.suggestions ||
        [],
      modulesChecked: response.sourceTransparency?.modulesChecked || [],
      widgets: (response.widgets || response.canvases || response.cards || []).map((w) => ({
        type: w.type || w.canvasType || w.inlineType || "",
        title: w.title || w.heading || "",
        recordCount: Array.isArray(w.records)
          ? w.records.length
          : Array.isArray(w.items)
            ? w.items.length
            : Array.isArray(w.rows)
              ? w.rows.length
              : undefined,
      })),
      rawKeys: Object.keys(response || {}),
    };

    console.log(JSON.stringify(normalized, null, 2));
  } catch (error) {
    console.log(
      JSON.stringify(
        {
          query,
          error: error?.message || String(error),
          stack: error?.stack?.split("\n").slice(0, 6),
        },
        null,
        2,
      ),
    );
  }
}
