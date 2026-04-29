import { compactObject } from "./aiAgent.normalizers.js";

export const action = (type, label, options = {}) =>
  compactObject({ type, label, ...options });

export const disabledAction = (type, label, reason = "Action not available yet") =>
  action(type, label, { disabled: true, reason });

export const widget = (type, title, payload = {}) =>
  compactObject({ type, title, ...payload });

export const unavailableWidget = (title, message, checked = []) =>
  widget("unavailable_notice", title, {
    data: { message, checked },
    notices: [message],
  });

export const textNoticeWidget = (title, message) =>
  widget("text_notice", title, { data: { message }, notices: [message] });

export const filterChip = (key, label, value) =>
  value ? { key, label, value: String(value) } : null;

export const sourceTransparency = ({
  modulesChecked = [],
  filtersApplied = [],
  accessRestrictions = [],
  mode = "live",
} = {}) => ({
  modulesChecked,
  filtersApplied,
  refreshedAt: new Date().toISOString(),
  mode,
  accessRestrictions,
});
