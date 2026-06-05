require("dotenv/config");

const BASE_URL = process.env.ACI_PUBLIC_CHAT_BASE_URL || "http://localhost:5050";
const ENDPOINT = `${BASE_URL.replace(/\/$/, "")}/api/ai-agent/public-chat`;
const CASE_TIMEOUT_MS = 20000;

const cases = [
  {
    id: "be-6e-sunroof-public",
    message: "be 6e sunroof",
    expectedText: "sunroof",
  },
  {
    id: "eqs-range-public",
    message: "eqs range",
    expectedTool: "vehicle_spec_attribute_lookup",
    answerMustIncludeAny: ["813 km", "857 km"],
    answerMustNotInclude: ["not available", "functionality that is not available"],
  },
  {
    id: "comparison-follow-up-public",
    message: "which one is better?",
    context: {
      activeComparison: {
        vehicles: [
          { make: "Hyundai", model: "Creta", fullModel: "Hyundai Creta" },
          { make: "Kia", model: "Seltos", fullModel: "Kia Seltos" },
        ],
      },
    },
    expectedText: "creta",
  },
];

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const postCase = async (testCase = {}) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CASE_TIMEOUT_MS);
  const startedAt = Date.now();

  try {
    const response = await fetch(ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: testCase.message,
        context: testCase.context || {},
      }),
      signal: controller.signal,
    });

    const body = await response.json().catch(() => ({}));
    const failures = [];
    const responseText = JSON.stringify(body);

    if (!response.ok) {
      failures.push(`HTTP ${response.status}`);
    }

    if (testCase.expectedText && !clean(responseText).includes(clean(testCase.expectedText))) {
      failures.push(`Expected response to mention ${testCase.expectedText}`);
    }

    if (testCase.expectedTool) {
      const tool = body?.aciCoreBridge?.tool || body?.data?.aciCoreBridge?.tool || body?.tool || "";
      if (tool !== testCase.expectedTool) {
        failures.push(`Expected tool ${testCase.expectedTool}, got ${tool}`);
      }
    }

    const answerText = String(body?.answer || body?.data?.answer || "");
    if (Array.isArray(testCase.answerMustIncludeAny) && testCase.answerMustIncludeAny.length > 0) {
      const hasAnyRequiredText = testCase.answerMustIncludeAny.some((needle) =>
        answerText.includes(String(needle || "")),
      );

      if (!hasAnyRequiredText) {
        failures.push(`Expected answer to include one of: ${testCase.answerMustIncludeAny.join(", ")}`);
      }
    }

    if (Array.isArray(testCase.answerMustNotInclude) && testCase.answerMustNotInclude.length > 0) {
      const loweredAnswer = answerText.toLowerCase();
      const forbiddenText = testCase.answerMustNotInclude.find((needle) =>
        loweredAnswer.includes(String(needle || "").toLowerCase()),
      );

      if (forbiddenText) {
        failures.push(`Answer included forbidden text: ${forbiddenText}`);
      }
    }

    return {
      id: testCase.id,
      pass: failures.length === 0,
      durationMs: Date.now() - startedAt,
      failures,
      summary: {
        status: response.status,
        intent: body?.intent || body?.data?.intent || "",
        tool: body?.aciCoreBridge?.tool || body?.data?.aciCoreBridge?.tool || body?.tool || "",
        answer: body?.answer || body?.data?.answer || "",
      },
    };
  } catch (error) {
    return {
      id: testCase.id,
      pass: false,
      durationMs: Date.now() - startedAt,
      failures: [error?.name === "AbortError" ? `Exceeded ${CASE_TIMEOUT_MS}ms` : error?.message || String(error)],
      summary: {},
    };
  } finally {
    clearTimeout(timeout);
  }
};

const main = async () => {
  const startedAt = Date.now();
  const results = [];

  for (const testCase of cases) {
    results.push(await postCase(testCase));
  }

  const failed = results.filter((item) => !item.pass);
  const summary = {
    suite: "ACI Context Manager public chat smoke",
    endpoint: ENDPOINT,
    requiresRunningBackend: true,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    durationMs: Date.now() - startedAt,
    caseTimeoutMs: CASE_TIMEOUT_MS,
    results,
  };

  console.log(JSON.stringify(summary, null, 2));
  if (failed.length) process.exit(1);
};

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exit(1);
});
