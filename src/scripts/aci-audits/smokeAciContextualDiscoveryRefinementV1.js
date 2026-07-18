import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import { prewarmAciCoreRuntime } from "../../services/aciCore/aciCore.prewarm.js";
import { runAciCoreLiveBridge } from "../../services/aciCore/integration/aciCoreLiveBridge.service.js";
import { compactAciClientResponse } from "../../services/aiAgent/aiAgent.clientResponse.js";

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const text = (value) => String(value || "").trim();
const lower = (value) => text(value).toLowerCase();

const mergeContextPatch = (context = {}, response = {}) => ({
  ...context,
  ...(response.contextPatch || {}),
});

const responseFilters = (response = {}) =>
  response.data?.filters || response.widget?.filters || response.filters || {};

const responseRows = (response = {}) => {
  const groups =
    response.data?.modelGroups ||
    response.modelGroups ||
    response.widget?.modelGroups ||
    [];
  return asArray(groups).length
    ? asArray(groups)
    : asArray(response.data?.rows || response.rows || response.items);
};

const responsePreviewRows = (response = {}) =>
  asArray(
    response.data?.previewModelGroups ||
      response.previewModelGroups ||
      response.widget?.previewModelGroups ||
      response.data?.rows ||
      response.rows,
  );

const rowPrice = (row = {}) => Number(
  row.startsFromPrice || row.bestUnderBudgetPrice || row.exShowroomPrice || 0,
);

const productName = (row = {}) =>
  [
    row.make,
    row.brand,
    row.model,
    row.fullModel,
    row.displayName,
    row.variant,
    ...asArray(row.qualifyingVariants).flatMap((variant) => [
      variant.variant,
      variant.variantName,
    ]),
  ]
    .filter(Boolean)
    .join(" ");

const commercialPattern =
  /\b(?:taxi|cab|fleet|commercial|cargo|goods?|pickup|pick[\s-]*up|tour|xpres(?:[\s-]*t)?|super\s+carry|dost|intra)\b/i;

const failures = [];

try {
  await connectDB();
  await prewarmAciCoreRuntime({ mode: "light", background: false });

  let context = {};
  const initial = await runAciCoreLiveBridge({
    message: "cars under 20 lakhs",
    context,
  });
  context = mergeContextPatch(context, initial);

  const storedBudget = Number(
    context.contextState?.buyerContext?.maxBudget ||
      context.aciContextState?.buyerContext?.maxBudget ||
      0,
  );
  if (storedBudget !== 2000000) {
    failures.push(
      `initial shortlist did not store ₹20L budget, got ${storedBudget}`,
    );
  }
  const initialFilters = responseFilters(initial);
  if (Number(initialFilters.budgetTarget || 0) !== 2000000) {
    failures.push(`initial shortlist lost the stated ₹20L target, got ${initialFilters.budgetTarget || 0}`);
  }
  if (Number(initialFilters.previewBudgetMin || 0) !== 1400000) {
    failures.push(`initial shortlist did not apply the ₹14L preview floor, got ${initialFilters.previewBudgetMin || 0}`);
  }
  if (Number(initialFilters.budgetMax || 0) !== 2200000) {
    failures.push(`initial shortlist did not apply the ₹22L result ceiling, got ${initialFilters.budgetMax || 0}`);
  }
  const outOfBandPreview = responsePreviewRows(initial).filter((row) => {
    const price = rowPrice(row);
    return price < 1400000 || price > 2200000;
  });
  if (outOfBandPreview.length) {
    failures.push(`chat preview leaked ${outOfBandPreview.length} cars outside ₹14L–₹22L`);
  }
  const initialPreviewPrices = responsePreviewRows(initial).map(rowPrice);
  if (initialPreviewPrices.some((price, index) => index > 0 && price > initialPreviewPrices[index - 1])) {
    failures.push("chat preview is not ranked from high to low");
  }
  const nonPetrolPreview = responsePreviewRows(initial).filter((row) =>
    !asArray(row.fuelTypes).some((fuel) => /petrol/i.test(String(fuel))) &&
    !asArray(row.qualifyingVariants).some((variant) => /petrol/i.test(String(variant.fuelType || variant.fuel))),
  );
  if (nonPetrolPreview.length) {
    failures.push(`default preview contains ${nonPetrolPreview.length} non-petrol-only models`);
  }
  const compactInitial = compactAciClientResponse(initial);
  const compactFuelOptions = asArray(compactInitial.widget?.modelGroups)
    .flatMap((row) => asArray(row.fuelOptions));
  const availableFuelTypes = new Set(
    compactFuelOptions.map((option) => lower(option.fuelType || option.fuel)),
  );
  for (const expectedFuel of ["petrol", "diesel", "cng", "electric"]) {
    if (!availableFuelTypes.has(expectedFuel)) {
      failures.push(`client shortlist fuel filter lost ${expectedFuel}`);
    }
  }
  const outOfBandFuelOptions = compactFuelOptions.filter((option) => {
    const price = Number(option.startsFromPrice || option.bestUnderBudgetPrice || 0);
    return price < 1400000 || price > 2200000;
  });
  if (outOfBandFuelOptions.length) {
    failures.push(`client fuel filter exposed ${outOfBandFuelOptions.length} options outside ₹14L–₹22L`);
  }
  const initialCommercialRows = responseRows(initial)
    .map((row) => productName(row))
    .filter((name) => commercialPattern.test(name));
  if (initialCommercialRows.length) {
    failures.push(
      `commercial products leaked into initial shortlist: ${initialCommercialRows.join(", ")}`,
    );
  }

  const automatic = await runAciCoreLiveBridge({
    message: "automatic",
    context,
  });
  const automaticFilters = responseFilters(automatic);
  const automaticRows = responseRows(automatic);
  const isolation =
    automatic.aciCoreBridge?.contextIsolation ||
    automatic.meta?.aciCoreBridge?.contextIsolation ||
    "";

  if (Number(automaticFilters.budgetMax || 0) !== 2200000) {
    failures.push(
      `automatic follow-up lost the ₹22L result ceiling, got ${automaticFilters.budgetMax || 0}`,
    );
  }
  if (Number(automaticFilters.budgetTarget || 0) !== 2000000) {
    failures.push(`automatic follow-up lost the ₹20L target, got ${automaticFilters.budgetTarget || 0}`);
  }
  if (lower(automaticFilters.transmission) !== "automatic") {
    failures.push(
      `automatic follow-up did not apply transmission filter, got ${automaticFilters.transmission || "empty"}`,
    );
  }
  if (isolation !== "contextual_discovery_refinement") {
    failures.push(
      `automatic follow-up used ${isolation || "no"} context isolation`,
    );
  }
  if (!automaticRows.length) {
    failures.push("automatic follow-up returned no passenger-car model groups");
  }

  const nonAutomaticVariants = automaticRows.flatMap((row) =>
    asArray(row.qualifyingVariants)
      .filter(
        (variant) =>
          !/automatic|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bdsg\b|\bat\b/i.test(
            [variant.transmission, variant.transmissionKey, variant.variant]
              .filter(Boolean)
              .join(" "),
          ),
      )
      .map(
        (variant) =>
          `${row.fullModel || row.model}: ${variant.variant || "unknown variant"}`,
      ),
  );
  if (nonAutomaticVariants.length) {
    failures.push(
      `non-automatic variants leaked into automatic refinement: ${nonAutomaticVariants.slice(0, 5).join(", ")}`,
    );
  }

  const commercialRows = automaticRows
    .map((row) => productName(row))
    .filter((name) => commercialPattern.test(name));
  if (commercialRows.length) {
    failures.push(
      `commercial products leaked into shortlist: ${commercialRows.join(", ")}`,
    );
  }

  const automaticContext = mergeContextPatch(context, automatic);
  const diesel = await runAciCoreLiveBridge({
    message: "diesel",
    context: automaticContext,
  });
  const dieselFilters = responseFilters(diesel);
  if (Number(dieselFilters.budgetMax || 0) !== 2200000) {
    failures.push(
      `diesel refinement lost the ₹22L result ceiling, got ${dieselFilters.budgetMax || 0}`,
    );
  }
  if (lower(dieselFilters.transmission) !== "automatic") {
    failures.push(
      `diesel refinement lost automatic filter, got ${dieselFilters.transmission || "empty"}`,
    );
  }
  if (lower(dieselFilters.fuelType) !== "diesel") {
    failures.push(
      `diesel refinement did not apply fuel filter, got ${dieselFilters.fuelType || "empty"}`,
    );
  }

  const modelSpecific = await runAciCoreLiveBridge({
    message: "Creta automatic",
    context,
  });
  const modelIsolation =
    modelSpecific.aciCoreBridge?.contextIsolation ||
    modelSpecific.meta?.aciCoreBridge?.contextIsolation ||
    "";
  if (modelIsolation === "contextual_discovery_refinement") {
    failures.push(
      "model-specific request was incorrectly expanded as a broad refinement",
    );
  }

  const family = await runAciCoreLiveBridge({
    message: "family car under 20 lakhs",
    context: {},
  });
  const familyFilters = responseFilters(family);
  const familyRows = responsePreviewRows(family);
  if (lower(familyFilters.buyerUseCase) !== "family") {
    failures.push(`family request lost its use case, got ${familyFilters.buyerUseCase || "empty"}`);
  }
  const nonFamilyBodies = familyRows.filter((row) =>
    !/suv|sport util|mpv|muv|sedan|hatch/i.test([row.bodyType, row.bodyTypeKey, row.segment].filter(Boolean).join(" ")),
  );
  if (!familyRows.length || nonFamilyBodies.length) {
    failures.push(`family recommendation eligibility failed for ${nonFamilyBodies.length || "all"} preview rows`);
  }

  console.log(
    JSON.stringify(
      {
        suite: "ACI contextual discovery refinement smoke",
        ok: failures.length === 0,
        failures,
        initial: {
          canvasType: initial.canvasType,
          storedBudget,
          rows: responseRows(initial).length,
        },
        automatic: {
          canvasType: automatic.canvasType,
          isolation,
          filters: automaticFilters,
          rows: automaticRows.length,
          models: automaticRows
            .slice(0, 8)
            .map((row) => row.fullModel || row.displayName || row.model),
        },
        dieselAfterAutomatic: {
          filters: dieselFilters,
          rows: responseRows(diesel).length,
        },
        modelSpecific: {
          canvasType: modelSpecific.canvasType,
          isolation: modelIsolation,
          intent: modelSpecific.intent,
        },
        family: {
          filters: familyFilters,
          rows: familyRows.length,
          models: familyRows.slice(0, 8).map((row) => row.fullModel || row.displayName || row.model),
        },
      },
      null,
      2,
    ),
  );
} finally {
  await mongoose.disconnect().catch(() => {});
}

if (failures.length) process.exitCode = 1;
