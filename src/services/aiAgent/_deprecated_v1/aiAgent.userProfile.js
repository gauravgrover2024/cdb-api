import UserProfile from "../../models/UserProfile.js";

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
  preferredTransmission:
    parsed?.entities?.transmission ||
    context?.profile?.preferredTransmission ||
    null,
  buyingPriority:
    parsed?.entities?.priority ||
    context?.profile?.buyingPriority ||
    null,
  intentAffinity: context?.profile?.intentAffinity || {},
});

export const updateIntentAffinity = async (userId, intent) => {
  if (!userId || !intent) return null;

  await UserProfile.updateOne(
    { userId: String(userId) },
    {
      $inc: { [`intentAffinity.${intent}`]: 1 },
      $set: { lastActive: new Date() },
    },
    { upsert: true },
  );

  return true;
};
