export const runVehicleRecommendationTool = async (args = {}) => {
  const { runtimeVehicleRecommend } = await import("../../aiAgent.executor.js");
  const result = await runtimeVehicleRecommend(args);

  return {
    ...result,
    tool: "vehicle_recommendation",
    modulesChecked: [
      ...new Set([
        ...(Array.isArray(result.modulesChecked) ? result.modulesChecked : []),
        "vehicle_recommendation",
      ]),
    ],
    meta: {
      ...(result.meta || {}),
      status: result.rows?.length ? "ready" : "no_matches",
      implementation: "runtime_vehicle_recommend",
    },
  };
};

export default runVehicleRecommendationTool;
