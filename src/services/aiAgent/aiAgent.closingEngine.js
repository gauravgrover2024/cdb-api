export const generateClosingActions = (context = {}) => {
  const stage = context.stage || "";
  const model = context.anchorModel || context.model || "";
  const variant = context.anchorVariant || context.variant || "";
  const modelLabel = `${model}${variant ? ` ${variant}` : ""}`.trim();

  if (stage !== "closing") return [];

  return [
    {
      id: "close-lock-deal",
      title: "Lock best deal now",
      subtitle: modelLabel
        ? `Secure your ACI quotation for ${modelLabel}`
        : "Secure your ACI quotation now",
      kind: "lead",
      type: "lead",
      intent: "aci_new_car_quotation",
      query: modelLabel ? `Get quotation for ${modelLabel}` : "Get quotation",
      entities: {
        ...(model ? { model } : {}),
        ...(variant ? { variant } : {}),
        ...(context.city ? { city: context.city } : {}),
      },
      contextPatch: {
        leadType: "quotation",
        urgencySignals: [
          "limited offers may change",
          "on-road prices can increase",
          "early booking improves delivery priority",
        ],
      },
      canvasType: "aci_quotation_canvas",
      priority: 100,
      icon: "file-text",
      tone: "primary",
      leadType: "quotation",
    },
    {
      id: "close-test-drive",
      title: "Book test drive",
      subtitle: modelLabel
        ? `Confirm final fit for ${modelLabel}`
        : "Confirm fit before booking",
      kind: "lead",
      type: "lead",
      intent: "vehicle_test_drive_request",
      query: modelLabel
        ? `Book test drive for ${modelLabel}`
        : "Book test drive",
      entities: {
        ...(model ? { model } : {}),
        ...(variant ? { variant } : {}),
        ...(context.city ? { city: context.city } : {}),
      },
      contextPatch: {
        leadType: "test_drive",
        urgencySignals: ["limited slots this week"],
      },
      priority: 95,
      icon: "car",
      tone: "primary",
      leadType: "test_drive",
    },
  ];
};
