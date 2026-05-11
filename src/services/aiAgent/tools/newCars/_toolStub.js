import resolveVehicle from "./shared/resolveVehicle.js";

export const createNewCarsToolStub = ({
  toolName = "new_car_tool",
  canvasType = "",
  dataKey = "rows",
} = {}) => {
  return async ({ toolPlan = {}, context = {} } = {}) => {
    const vehicle = resolveVehicle({ toolPlan, context });

    return {
      tool: toolName,
      count: 0,
      matched: 0,
      rows: [],
      [dataKey]: [],
      vehicle,
      canvasType,
      modulesChecked: [toolName, "stub"],
      dataSource: "tool_stub",
      meta: {
        status: "scaffolded",
      },
    };
  };
};

export default createNewCarsToolStub;
