import "dotenv/config";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import {
  ACI_CHANNELS,
  validateAciResponseContract,
} from "../services/aiAgent/contracts/aciV2ResponseContract.js";
import connectDB from "../config/db.js";

const TESTS = [
  {
    id: "contract-price-public",
    channel: ACI_CHANNELS.PUBLIC_WEB,
    message: "Verna pricelist",
    context: {},
  },
  {
    id: "contract-feature-public",
    channel: ACI_CHANNELS.PUBLIC_WEB,
    message: "Does Verna SX have sunroof?",
    context: {},
  },
  {
    id: "contract-emi-public",
    channel: ACI_CHANNELS.PUBLIC_WEB,
    message: "EMI for Verna SX IVT with 2 lakh down payment",
    context: {},
  },
  {
    id: "contract-multi-public",
    channel: ACI_CHANNELS.PUBLIC_WEB,
    message: "Show Verna price in Delhi, compare with City, tell EMI for 5 years and check offers",
    context: {},
  },
  {
    id: "contract-quotation-public",
    channel: ACI_CHANNELS.PUBLIC_WEB,
    message: "Best price for black Verna SX automatic",
    context: {},
  },
  {
    id: "contract-context-public",
    channel: ACI_CHANNELS.PUBLIC_WEB,
    message: "Compare with City",
    context: {
      selectedVehicle: {
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      anchorCity: "new-delhi",
    },
  },
  {
    id: "contract-internal-private",
    channel: ACI_CHANNELS.INTERNAL_WEB,
    message: "Loan closure 7077",
    context: {},
  },
];

const summarize = (response = {}) => ({
  intent: response.intent,
  mode: response.mode,
  displayMode: response.displayMode,
  canvasType: response.canvasType,
  inlineType: response.inlineType,
  title: response.title,
  answer: response.answer,
  actionsCount: Array.isArray(response.actions) ? response.actions.length : -1,
  leadingQuestionsCount: Array.isArray(response.leadingQuestions)
    ? response.leadingQuestions.length
    : -1,
  secondaryCount: Array.isArray(response.secondaryResponses)
    ? response.secondaryResponses.length
    : -1,
  runtimeMetaCount: Array.isArray(response.runtimeResultsMeta)
    ? response.runtimeResultsMeta.length
    : -1,
  hasData: Boolean(response.data && typeof response.data === "object"),
  hasService: Boolean(response.service && typeof response.service === "object"),
  oldSystemUsed: response.oldSystemUsed || response.service?.oldSystemUsed || false,
});

const run = async () => {
  await connectDB();

  const results = [];

  for (const test of TESTS) {
    const startedAt = Date.now();

    let response;
    let validation;

    try {
      response = await chatWithAgent({
        message: test.message,
        context: test.context,
        user: test.channel === ACI_CHANNELS.INTERNAL_WEB
          ? { id: "contract-test-user", role: "admin" }
          : null,
      });

      validation = validateAciResponseContract(response, {
        channel: test.channel,
        requireData: true,
        requireService: true,
      });
    } catch (error) {
      validation = {
        valid: false,
        errors: [error?.message || "Unknown error"],
        warnings: [],
        toolNames: [],
      };
    }

    const result = {
      id: test.id,
      message: test.message,
      channel: test.channel,
      pass: validation.valid,
      durationMs: Date.now() - startedAt,
      errors: validation.errors,
      warnings: validation.warnings,
      toolNames: validation.toolNames,
      summary: summarize(response),
    };

    results.push(result);
    console.log(JSON.stringify(result, null, 2));
  }

  const failed = results.filter((item) => !item.pass);

  const suite = {
    suite: "ACI Assist V2 official contract foundation",
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
  };

  console.log(JSON.stringify(suite, null, 2));

  if (failed.length) {
    process.exitCode = 1;
  }
};

run()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(() => {
    setTimeout(() => process.exit(process.exitCode || 0), 250);
  });
