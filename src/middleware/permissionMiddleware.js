import RolePermission from "../models/RolePermission.js";

const isSuperadmin = (user) => String(user?.role || "").toLowerCase() === "superadmin";

export const getPermissionDocument = async (user) => {
  if (!user || isSuperadmin(user)) return { all: true };
  return (await RolePermission.findOne({ role: user.role }).select("permissions").lean())?.permissions || {};
};

export const canPermission = (permissions, module, section, action = "view", field) => {
  if (permissions?.all) return true;
  const modulePermissions = permissions?.[module];
  if (!modulePermissions) return true;
  let sectionPermissions = modulePermissions?.[section];
  // Resolve the field to the section that actually owns it (callers may pass a different section).
  if (field && !sectionPermissions?.fields?.[field]) {
    const matchingSection = Object.values(modulePermissions).find((item) => item?.fields?.[field]);
    if (matchingSection) sectionPermissions = matchingSection;
    else if (!sectionPermissions) return true;
  }
  if (!sectionPermissions) return false;
  const fieldPermission = field ? sectionPermissions.fields?.[field] : null;
  if (fieldPermission && typeof fieldPermission[action] === "boolean") return fieldPermission[action];
  return Boolean(sectionPermissions[action]);
};

export const requirePermission = (module, section, action = "view", field) => async (req, res, next) => {
  const permissions = await getPermissionDocument(req.user);
  if (canPermission(permissions, module, section, action, field)) return next();
  return res.status(403).json({ success: false, message: "You do not have permission for this action" });
};

export const filterPermissionFields = (value, permissions, module, section, fieldMap = {}) => {
  if (!value || typeof value !== "object" || permissions?.all) return value;
  const result = Array.isArray(value) ? [...value] : { ...value };
  Object.entries(fieldMap).forEach(([sourceKey, permissionField]) => {
    if (!canPermission(permissions, module, section, "view", permissionField)) {
      if (Array.isArray(result)) result.forEach((item) => { if (item && typeof item === "object") delete item[sourceKey]; });
      else delete result[sourceKey];
    }
  });
  return result;
};
