import { getAutocompleteEntityMatches } from "./aiAgent.vehicleEntityIndex.js";
import { loadAciFeatureRequestCatalog } from "./aiAgent.featureRequestParser.js";

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
      meta: {
        source: "aci_prewarmed_entity_and_feature_catalogs",
        count: 0,
        queryMs: 0,
        reason: "minimum_two_characters",
      },
    };
  }

  const startedAt = Date.now();
  const featureCatalog = await loadAciFeatureRequestCatalog();
  const suggestions = await getAutocompleteEntityMatches({
    query,
    context,
    limit,
    featureCatalog,
  });

  return {
    query,
    suggestions,
    meta: {
      source: "aci_prewarmed_entity_and_feature_catalogs",
      count: suggestions.length,
      queryMs: Date.now() - startedAt,
    },
  };
};

export default getAiAutocompleteSuggestions;
