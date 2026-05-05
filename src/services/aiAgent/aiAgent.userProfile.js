export const updateUserProfile = (context = {}, parsed = {}) => ({
  preferredBudget:
    parsed?.entities?.budgetMax ||
    parsed?.entities?.priceMax ||
    context?.profile?.preferredBudget ||
    null,
  preferredBodyType:
    parsed?.entities?.bodyType ||
    context?.profile?.preferredBodyType ||
    null,
  preferredFuel:
    parsed?.entities?.fuelType ||
    parsed?.entities?.fuel ||
    context?.profile?.preferredFuel ||
    null,
});
