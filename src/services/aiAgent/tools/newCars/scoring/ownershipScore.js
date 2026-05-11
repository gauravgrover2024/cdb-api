export const ownershipScore = ({ value = 0, max = 100 } = {}) => {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(Number(max || 100), numeric));
};

export default ownershipScore;
