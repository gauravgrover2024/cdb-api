import { getAutocompleteEntityMatches } from "./aiAgent.vehicleEntityIndex.js";

export const getAiAutocompleteSuggestions = async ({
  q = "",
  context = {},
  limit = 8,
} = {}) => {
  const query = String(q || "").trim();

  if (query.length < 2) {
    return {
      query,
      suggestions: [],
    };
  }

  const suggestions = await getAutocompleteEntityMatches({
    query,
    context,
    limit,
  });

  return {
    query,
    suggestions,
  };
};

export default getAiAutocompleteSuggestions;
