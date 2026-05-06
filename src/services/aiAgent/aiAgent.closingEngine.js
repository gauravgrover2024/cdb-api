export const generateClosingActions = (context = {}) => {
  const isRecent = (entry) => {
    if (!entry) return false;
    if (typeof entry === "boolean") return entry;
    if (typeof entry !== "object") return false;
    return Boolean(entry.value) && Date.now() - Number(entry.ts || 0) < 10 * 60 * 1000;
  };

  const stage = context.stage || "";
  const model = context.anchorModel || context.model || "";
  const variant = context.anchorVariant || context.variant || "";
  const modelLabel = `${model}${variant ? ` ${variant}` : ""}`.trim();

  const buyingSignals = Array.isArray(context.buyingSignals)
    ? context.buyingSignals
    : [];
  const history = context.history || {};
  const historyViewedPrice = isRecent(history.viewedPrice);
  const historyCompared = isRecent(history.compared);
  const historyCheckedEmi = isRecent(history.checkedEmi);

  const shouldClose =
    stage === "closing" ||
    buyingSignals.includes("ready_to_buy") ||
    (historyViewedPrice && historyCompared && historyCheckedEmi);

  if (!shouldClose) return [];

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
