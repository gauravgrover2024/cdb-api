import dotenv from "dotenv";
import mongoose from "mongoose";
import fs from "node:fs";
import { askAciAssist } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const getWidget = (response = {}) =>
  response.widget || toArray(response.widgets)[0] || null;

const getRowsCount = (response = {}) => {
  const widget = getWidget(response) || {};
  return (
    toArray(response.rows).length ||
    toArray(response.items).length ||
    toArray(response.features).length ||
    toArray(response.data?.rows).length ||
    toArray(response.data?.features).length ||
    toArray(widget.rows).length ||
    toArray(widget.items).length ||
    toArray(widget.features).length ||
    toArray(widget.colors).length
  );
};

const extractQueriesFromBackendSuite = () => {
  const file = "src/scripts/testAiAgentBackend.js";
  const text = fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";

  const queries = [];
  const regex = /query:\s*["'`]([^"'`]+)["'`]/g;
  let match;

  while ((match = regex.exec(text))) {
    const query = match[1];
    if (
      /\b(price|pricelist|on-road|on road|ex-showroom|variant price|colors?|colou?rs?|feature|features|specs|sunroof|adas|airbags?|camera|ventilated|tpms|isofix)\b/i.test(
        query,
      )
    ) {
      queries.push(query);
    }
  }

  return [...new Set(queries)];
};

const fallbackQueries = [
  "Verna pricelist",
  "Verna price in Mumbai",
  "Verna SX price",
  "Show colors of Verna",
  "Show features of Verna",
  "Does Verna SX have sunroof?",
  "Which Verna variants have sunroof?",
  "Does Elevate ZX have ADAS?",
  "Show colors of Elevate",
  "Elevate pricelist",
];

const main = async () => {
  if (mongoUri && mongoose.connection.readyState !== 1) {
    await mongoose.connect(mongoUri);
    console.log("MongoDB connected");
  }

  const queries = extractQueriesFromBackendSuite();
  const finalQueries = queries.length ? queries : fallbackQueries;

  console.log(`Running focused price/color/features subset: ${finalQueries.length} queries`);

  const results = [];

  for (const query of finalQueries) {
    const started = Date.now();

    try {
      const response = await askAciAssist({
        message: query,
        context: {
          city: "new-delhi",
          anchorCity: "new-delhi",
        },
        user: {
          _id: "focused-regression",
          name: "Focused Regression",
        },
      });

      const widget = getWidget(response);
      const modulesChecked =
        response.sourceTransparency?.modulesChecked ||
        response.runtimeResultsMeta?.[0]?.modulesChecked ||
        [];

      const row = {
        query,
        pass:
          Boolean(response.intent) &&
          Boolean(response.contextSnapshot) &&
          Boolean(response.runtimeResultsMeta?.length) &&
          (
            !response.canvasType ||
            Boolean(widget)
          ),
        intent: response.intent || "",
        displayMode: response.displayMode || "",
        canvasType: response.canvasType || widget?.canvasType || "",
        inlineType: response.inlineType || "",
        hasWidget: Boolean(widget),
        hasContextSnapshot: Boolean(response.contextSnapshot),
        runtimeMetaCount: response.runtimeResultsMeta?.length || 0,
        modulesChecked,
        rows: getRowsCount(response),
        durationMs: Date.now() - started,
      };

      results.push(row);
      console.log(JSON.stringify(row));
    } catch (error) {
      const row = {
        query,
        pass: false,
        error: error.message,
        durationMs: Date.now() - started,
      };
      results.push(row);
      console.log(JSON.stringify(row));
    }
  }

  const failed = results.filter((item) => !item.pass);
  console.log(
    JSON.stringify(
      {
        total: results.length,
        passed: results.length - failed.length,
        failed: failed.length,
        failedQueries: failed.map((item) => item.query),
      },
      null,
      2,
    ),
  );

  if (mongoose.connection.readyState === 1) await mongoose.disconnect();

  if (failed.length) process.exitCode = 1;
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
