#!/usr/bin/env node

const ENDPOINT =
  process.env.ACI_PUBLIC_CHAT_URL || "http://localhost:5050/api/ai-agent/public-chat";

const ask = async (message) => {
  const response = await fetch(ENDPOINT, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message, context: { anchorCity: "noida" } }),
  });
  if (!response.ok) throw new Error(`${message}: HTTP ${response.status}`);
  return response.json();
};

const toolNames = (response = {}) =>
  (response.planner?.tools || []).map((item) => item.tool).filter(Boolean);

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

const main = async () => {
  const documents = await ask("What documents are needed for a car loan?");
  assert(documents.intent === "new_car_finance_faq", "Documents query used the wrong intent");
  assert(toolNames(documents).includes("vehicle_finance_knowledge"), "Documents query missed finance knowledge");
  assert(!toolNames(documents).includes("vehicle_emi"), "Documents query incorrectly opened EMI");
  assert((documents.data?.checklist || []).length >= 4, "Documents checklist is incomplete");

  const cibil = await ask("Can I get a car loan with 700 CIBIL and what documents do I need?");
  assert(cibil.intent === "new_car_finance_faq", "CIBIL query used the wrong intent");
  assert(/not.*guarantee|does not approve|rather than a guarantee/i.test(cibil.answer), "CIBIL answer lacks approval guardrail");
  assert(cibil.data?.approvalGuaranteed === false, "CIBIL answer does not expose approval guardrail");

  const oneCar = await ask(
    "Show Hyundai Creta price, EMI and tell me loan eligibility and documents",
  );
  const oneCarTools = toolNames(oneCar);
  for (const expected of ["vehicle_pricelist", "vehicle_emi", "vehicle_finance_knowledge"]) {
    assert(oneCarTools.includes(expected), `One-car compound query missed ${expected}`);
  }

  const twoCar = await ask(
    "Compare Hyundai Creta and Kia Seltos prices, EMI, loan eligibility and documents",
  );
  const twoCarTools = toolNames(twoCar);
  assert(twoCarTools.includes("vehicle_compare"), "Two-car compound query missed comparison");
  assert(twoCarTools.filter((tool) => tool === "vehicle_pricelist").length === 2, "Two-car query missed a price list");
  assert(twoCarTools.filter((tool) => tool === "vehicle_emi").length === 2, "Two-car query missed an EMI result");
  assert(twoCarTools.filter((tool) => tool === "vehicle_finance_knowledge").length === 1, "Finance guidance must run once per turn");

  const onRoadCharges = await ask("Explain the on-road charges for Hyundai Creta in Noida");
  assert(
    !toolNames(onRoadCharges).includes("vehicle_finance_knowledge"),
    "On-road charges were incorrectly treated as a loan-finance question",
  );

  const serialized = JSON.stringify([documents, cibil, oneCar, twoCar]);
  assert(!/guaranteed approval|pre-approved for|live bank offer/i.test(serialized), "Finance answers fabricated approval or a live offer");

  console.log(
    JSON.stringify(
      {
        suite: "ACI Finance Knowledge v1",
        ok: true,
        checks: 15,
        collection: documents.sourceTransparency?.dataSource,
        oneCarTools,
        twoCarTools,
      },
      null,
      2,
    ),
  );
};

main().catch((error) => {
  console.error(JSON.stringify({ suite: "ACI Finance Knowledge v1", ok: false, error: error.message }, null, 2));
  process.exitCode = 1;
});
