import { applyAciCustomerJourneyGuidance } from "../../services/aiAgent/aiAgent.customerJourney.js";

const vehicle = {
  make: "Kia",
  model: "Seltos",
  fullModel: "Kia Seltos",
};

const turns = [
  { message: "Does Kia Seltos have a sunroof?", intent: "vehicle_feature_answer" },
  { message: "Show Kia Seltos colours", intent: "vehicle_colors" },
  { message: "Show Kia Seltos price list", intent: "vehicle_pricelist" },
  { message: "Which Kia Seltos variant should I choose?", intent: "vehicle_variant_recommendation", variant: "HTX" },
  { message: "What will the EMI be?", intent: "vehicle_emi" },
];

let context = { selectedVehicle: vehicle };
const snapshots = [];
const failures = [];

for (const turn of turns) {
  const response = applyAciCustomerJourneyGuidance({
    message: turn.message,
    context,
    response: {
      intent: turn.intent,
      answer: "Verified answer",
      actions: [
        {
          id: "generic-quote",
          label: "Get quote",
          type: "lead",
          query: "Get quotation for Kia Seltos",
          intent: "aci_new_car_quotation",
        },
      ],
      leadingQuestions: [],
      contextPatch: {
        selectedVehicle: { ...vehicle, variant: turn.variant || context.selectedVehicle?.variant || "" },
      },
      data: {},
    },
  });

  snapshots.push({
    message: turn.message,
    stage: response.journeyGuidance.stage,
    score: response.journeyGuidance.readinessScore,
    leadMode: response.journeyGuidance.leadMode,
    next: response.journeyGuidance.nextBestQuestion,
    visibleLeadCount: response.actions.filter((action) => action.type === "lead").length,
  });

  context = {
    ...context,
    ...response.contextPatch,
    selectedVehicle: response.contextPatch.selectedVehicle,
  };
}

if (snapshots[0].leadMode !== "hidden" || snapshots[0].visibleLeadCount !== 0) {
  failures.push("First research answer must not show a lead action");
}
if (!snapshots.every((item, index) => index === 0 || item.score >= snapshots[index - 1].score)) {
  failures.push("Readiness score regressed between turns");
}
if (!snapshots.some((item) => ["soft", "ready"].includes(item.leadMode))) {
  failures.push("Journey never progressed toward an optional enquiry");
}
if (snapshots.some((item) => item.visibleLeadCount > 1)) {
  failures.push("A turn exposed more than one enquiry action");
}
if (!snapshots.every((item) => item.next)) {
  failures.push("One or more turns lacked a next-best question");
}

console.log(
  JSON.stringify(
    {
      suite: "ACI customer journey guidance audit",
      pass: failures.length === 0,
      failures,
      snapshots,
    },
    null,
    2,
  ),
);

if (failures.length) process.exitCode = 1;
