import SuggestionPerformance from "../../models/SuggestionPerformance.js";
import UserSuggestionBehavior from "../../models/UserSuggestionBehavior.js";

export const logInteraction = async ({
  userId,
  intent,
  suggestionId,
  actionTaken,
  countImpression = true,
}) => {
  if (!intent || !suggestionId) return null;

  const global = await SuggestionPerformance.findOneAndUpdate(
    { intent, suggestionId },
    {
      $inc: {
        ...(countImpression ? { impressions: 1 } : {}),
        ...(actionTaken ? { clicks: 1 } : {}),
      },
      $set: { updatedAt: new Date() },
    },
    {
      upsert: true,
      returnDocument: "after",
      setDefaultsOnInsert: true,
    },
  );

  global.globalCtr =
    Number(global.clicks || 0) / Number(global.impressions || 1);
  await global.save();

  if (!userId) {
    return { globalCtr: global.globalCtr, userCtr: 0 };
  }

  const user = await UserSuggestionBehavior.findOneAndUpdate(
    { userId: String(userId), intent, suggestionId },
    {
      $inc: {
        ...(countImpression ? { impressions: 1 } : {}),
        ...(actionTaken ? { clicks: 1 } : {}),
      },
      $set: { lastUsed: new Date() },
    },
    {
      upsert: true,
      returnDocument: "after",
      setDefaultsOnInsert: true,
    },
  );

  user.userCtr = Number(user.clicks || 0) / Number(user.impressions || 1);
  await user.save();

  return {
    globalCtr: global.globalCtr,
    userCtr: user.userCtr,
  };
};

export const getSuggestionScore = async (userId, intent, suggestionId) => {
  if (!intent || !suggestionId) return 0;

  const global = await SuggestionPerformance.findOne({
    intent,
    suggestionId,
  }).lean();

  const user = userId
    ? await UserSuggestionBehavior.findOne({
        userId: String(userId),
        intent,
        suggestionId,
      }).lean()
    : null;

  const globalScore = global?.globalCtr || 0;
  const userScore = user?.userCtr || 0;

  return globalScore * 0.4 + userScore * 0.6;
};

export const getIntentScore = async ({ userId, intent, userType } = {}) => {
  void userId;
  void userType;

  if (!intent) return 0;

  // Placeholder for future intent-level learning.
  // Keep it conservative so it never overrules deterministic intent logic too aggressively.
  return 0;
};
