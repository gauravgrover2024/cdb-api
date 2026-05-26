import { runVehicleFeaturesTool } from "./tools/newCars/vehicleFeatures.tool.js";
import { runVehiclePricelistNewCarsTool } from "./tools/newCars/vehiclePricelist.tool.js";

const getAciEarlyGateToolRunner = (detected = {}) =>
  detected.intent === "vehicle_overview"
    ? runVehiclePricelistNewCarsTool
    : detected.intent === "vehicle_pricelist" ||
        detected.canvasType === "pricelist_canvas"
      ? runVehiclePricelistNewCarsTool
      : runVehicleFeaturesTool;

export const runAciEarlyGateTool = async ({
  detected = {},
  scopedDetected = {},
  scopedFeatureFilters = {},
  scopedFeatureContext = {},
  scopedToolPlan = {},
  scopedSelectedEntity = {},
  cleanUserMessage = "",
} = {}) => {
  const toolRunner = getAciEarlyGateToolRunner(detected);

  return toolRunner({
    detected: scopedDetected,
    filters: scopedFeatureFilters,
    context: scopedFeatureContext,
    toolPlan: scopedToolPlan,
    selectedEntity: scopedSelectedEntity,
    userMessage: cleanUserMessage,
  });
};

export const normalizeAciEarlyGateOverviewResponse = ({
  response = {},
  detected = {},
  preToolAuthorityContextPatch = {},
} = {}) => {
  if (detected.intent !== "vehicle_overview") {
    return {
      response,
      overviewAuthorityContextPatch: null,
    };
  }

  const overviewVehicle =
    response.vehicle ||
    response.widget?.vehicle ||
    response.contextPatch?.selectedVehicle ||
    preToolAuthorityContextPatch.selectedVehicle ||
    {};

  const overviewContextPatch = {
    ...preToolAuthorityContextPatch,
    ...(response.contextPatch || {}),
    selectedVehicle: {
      ...(overviewVehicle || {}),
      variant: detected.variant || "",
      variantName: detected.variant || "",
      selectedVariant: detected.variant || "",
    },
    anchorMake:
      overviewVehicle.make ||
      overviewVehicle.brand ||
      response.contextPatch?.anchorMake ||
      preToolAuthorityContextPatch.anchorMake ||
      "",
    anchorModel:
      overviewVehicle.model ||
      response.contextPatch?.anchorModel ||
      preToolAuthorityContextPatch.anchorModel ||
      detected.model ||
      "",
    anchorFullModel:
      overviewVehicle.fullModel ||
      overviewVehicle.displayName ||
      response.contextPatch?.anchorFullModel ||
      preToolAuthorityContextPatch.anchorFullModel ||
      detected.fullModel ||
      "",
    anchorVariant: detected.variant || "",
  };

  const answer = `Opened ${overviewVehicle.displayName || detected.model} overview.`;

  return {
    response: {
      ...response,
      tool: "vehicle_overview",
      intent: "vehicle_overview",
      canvasType: "car_overview_canvas",
      answer,
      vehicle: overviewContextPatch.selectedVehicle,
      contextPatch: overviewContextPatch,
      widget: {
        ...(response.widget || {}),
        type: "vehicle_overview",
        tool: "vehicle_overview",
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
        title: `${overviewVehicle.displayName || detected.model} overview`,
        answer,
        vehicle: overviewContextPatch.selectedVehicle,
        rows: response.rows || response.widget?.rows || [],
        items: response.items || response.widget?.items || response.rows || [],
        contextPatch: overviewContextPatch,
      },
    },
    overviewAuthorityContextPatch: overviewContextPatch,
  };
};
