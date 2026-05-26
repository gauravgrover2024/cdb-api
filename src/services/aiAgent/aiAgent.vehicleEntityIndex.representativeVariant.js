import { normalizeSearchKey } from "./aiAgent.planSchema.js";
import { containsAlias } from "./aiAgent.vehicleEntityIndex.matchers.js";

export const buildRepresentativeVariantFromIndex = ({
  index = {},
  model = "",
  brand = "",
  preferredTransmission = "",
  preferredFuel = "",
  targetPrice = null,
  selectedVariant = "",
} = {}) => {
  const shortModelKey = normalizeSearchKey(model);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);

  const candidates = (index.variants || []).filter((variant) => {
    if (!variant.active) return false;

    return (
      variant.shortModelKey === shortModelKey ||
      variant.modelKey === modelKey ||
      normalizeSearchKey(variant.model) === shortModelKey
    );
  });

  if (!candidates.length) {
    return {
      model,
      variantStrategy: "representative_default",
    };
  }

  if (selectedVariant) {
    const selectedKey = normalizeSearchKey(selectedVariant);
    const exact = candidates.find(
      (variant) =>
        variant.shortVariantKey === selectedKey ||
        containsAlias(normalizeSearchKey(variant.variant), selectedKey),
    );

    if (exact) return exact;
  }

  const scored = candidates.map((variant) => {
    let score = 0;

    const transmissionKey = normalizeSearchKey(variant.transmission);
    const fuelKey = normalizeSearchKey(variant.fuelType);

    if (
      preferredTransmission &&
      transmissionKey.includes(normalizeSearchKey(preferredTransmission))
    ) {
      score += 40;
    }

    if (preferredFuel && fuelKey.includes(normalizeSearchKey(preferredFuel))) {
      score += 25;
    }

    if (targetPrice && variant.price) {
      const distance = Math.abs(Number(variant.price) - Number(targetPrice));
      score += Math.max(0, 30 - distance / 50000);
    }

    if (
      /automatic|ivt|cvt|dct|amt|at/i.test(
        `${variant.variant} ${variant.transmission}`,
      )
    ) {
      score += 10;
    }

    if (
      /sx|zx|zxi|htx|gtx|alpha|creative|accomplished|top/i.test(variant.variant)
    ) {
      score += 8;
    }

    if (variant.price) score += 4;

    return {
      ...variant,
      representativeScore: score,
    };
  });

  scored.sort((a, b) => b.representativeScore - a.representativeScore);

  return scored[0];
};
