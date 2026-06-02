const parseLifecycleDate = (value) => {
  if (!value) return null;

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  if (typeof value === 'number') {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  const raw = String(value).trim();
  if (!raw) return null;

  const isoDate = new Date(raw);
  if (!Number.isNaN(isoDate.getTime())) return isoDate;

  const dmy = raw.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/);
  if (dmy) {
    const [, dd, mm, yyyy] = dmy;
    const date = new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    if (
      date.getFullYear() === Number(yyyy) &&
      date.getMonth() === Number(mm) - 1 &&
      date.getDate() === Number(dd)
    ) {
      return date;
    }
  }

  const ymd = raw.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (ymd) {
    const [, yyyy, mm, dd] = ymd;
    const date = new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    if (
      date.getFullYear() === Number(yyyy) &&
      date.getMonth() === Number(mm) - 1 &&
      date.getDate() === Number(dd)
    ) {
      return date;
    }
  }

  return null;
};

const isInactiveVehicle = (row = {}, now = new Date()) => {
  if (row.is_discontinued === true) return true;
  if (row.isDiscontinued === true) return true;
  if (row.active === false) return true;
  if (row.is_active === false) return true;

  const discontinuedDate = parseLifecycleDate(
    row.discontinued_date || row.discontinuedDate || row.discontinuedAt,
  );

  return Boolean(discontinuedDate && discontinuedDate.getTime() <= now.getTime());
};

const isInactiveDecisionProfile = (profile = {}) => {
  const lifecycleStatus = String(profile.lifecycleStatus || '').trim().toLowerCase();
  const dataStatus = String(profile.dataStatus || '').trim().toLowerCase();

  return (
    lifecycleStatus === 'stale_or_removed_from_active_pricelist' ||
    lifecycleStatus === 'discontinued' ||
    dataStatus === 'inactive_stale' ||
    dataStatus === 'discontinued'
  );
};

module.exports = {
  parseLifecycleDate,
  isInactiveVehicle,
  isInactiveDecisionProfile,
};
