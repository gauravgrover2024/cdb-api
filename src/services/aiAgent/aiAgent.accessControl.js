const STAFF_MODULES = new Set([
  "customers",
  "loans",
  "insurance",
  "vehicles",
  "usedCars",
  "payments",
]);

const ADMIN_MODULES = new Set([
  ...STAFF_MODULES,
  "deliveryOrders",
  "payouts",
  "finance",
]);

export const buildAccessContext = (user = {}) => {
  const role = String(user?.role || "user").toLowerCase();
  const isAdmin = role === "admin" || role === "superadmin";
  const isStaff = isAdmin || role === "staff";
  const modules = isAdmin ? ADMIN_MODULES : isStaff ? STAFF_MODULES : new Set(["vehicles"]);

  return {
    role,
    userId: String(user?._id || ""),
    canDebug: isAdmin,
    canEdit: isStaff,
    canViewFinance: isAdmin,
    canAccess: (moduleName) => modules.has(moduleName),
    modules,
    restrictions: [],
  };
};

export const noteRestriction = (access, moduleName, reason = "No access") => {
  access.restrictions.push({ module: moduleName, reason });
};
