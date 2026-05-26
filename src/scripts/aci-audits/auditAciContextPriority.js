import {
  applyAciExplicitMessageModelContextOverride,
  buildAciContextModelEntity,
  chooseAciDynamicModelEntity,
  repairAciResponseContextFromActiveContext,
} from "../../services/aiAgent/aiAgent.contextPriority.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const clone = (value) => JSON.parse(JSON.stringify(value));

const cases = [];

const addCase = (id, run) => {
  cases.push({ id, run });
};

const expectEqual = (failures, label, actual, expected) => {
  if (clean(actual) !== clean(expected)) {
    failures.push(`${label}: expected "${expected}", got "${actual}"`);
  }
};

const expectBlank = (failures, label, actual) => {
  if (String(actual || "").trim()) {
    failures.push(`${label}: expected blank, got "${actual}"`);
  }
};

addCase("explicit-model-switch-clears-stale-make-and-variant", async () => {
  const context = {
    anchorMake: "Hyundai",
    anchorModel: "Verna",
    anchorVariant: "SX IVT",
    selectedVehicle: {
      make: "Hyundai",
      brand: "Hyundai",
      model: "Verna",
      variant: "SX IVT",
      selectedVariant: "SX IVT",
      variantName: "SX IVT",
    },
  };

  applyAciExplicitMessageModelContextOverride({
    message: "Does Thar have sunroof?",
    context,
    dynamicModelEntity: {
      make: "Mahindra",
      brand: "Mahindra",
      model: "Thar",
      fullModel: "Mahindra Thar",
    },
  });

  const failures = [];
  expectEqual(failures, "anchorMake", context.anchorMake, "Mahindra");
  expectEqual(failures, "anchorModel", context.anchorModel, "Thar");
  expectEqual(failures, "selectedVehicle.make", context.selectedVehicle.make, "Mahindra");
  expectEqual(failures, "selectedVehicle.model", context.selectedVehicle.model, "Thar");
  expectBlank(failures, "anchorVariant", context.anchorVariant);
  expectBlank(failures, "selectedVehicle.variant", context.selectedVehicle.variant);
  expectBlank(failures, "selectedVehicle.selectedVariant", context.selectedVehicle.selectedVariant);

  return failures;
});

addCase("comparison-followup-does-not-hijack-selected-car", async () => {
  const context = {
    anchorMake: "Hyundai",
    anchorModel: "Verna",
    anchorVariant: "SX IVT",
    selectedVehicle: {
      make: "Hyundai",
      brand: "Hyundai",
      model: "Verna",
      variant: "SX IVT",
    },
  };

  applyAciExplicitMessageModelContextOverride({
    message: "Compare with City",
    context,
    dynamicModelEntity: {
      make: "Honda",
      brand: "Honda",
      model: "City",
      fullModel: "Honda City",
    },
  });

  const failures = [];
  expectEqual(failures, "anchorMake", context.anchorMake, "Hyundai");
  expectEqual(failures, "anchorModel", context.anchorModel, "Verna");
  expectEqual(failures, "anchorVariant", context.anchorVariant, "SX IVT");
  expectEqual(failures, "selectedVehicle.model", context.selectedVehicle.model, "Verna");

  return failures;
});

addCase("choose-text-entity-over-context-for-explicit-switch", async () => {
  const chosen = chooseAciDynamicModelEntity({
    message: "Show Creta pricelist",
    textEntity: {
      brand: "Hyundai",
      model: "Creta",
      fullModel: "Hyundai Creta",
      matchedText: "Creta",
    },
    contextEntity: {
      brand: "Hyundai",
      model: "Verna",
      fullModel: "Hyundai Verna",
      fromContext: true,
    },
  });

  const failures = [];
  expectEqual(failures, "chosen.model", chosen?.model, "Creta");
  expectEqual(failures, "chosen.brand", chosen?.brand, "Hyundai");
  return failures;
});

addCase("choose-context-entity-for-weak-variant-token-in-comparison", async () => {
  const chosen = chooseAciDynamicModelEntity({
    message: "Compare SX with SX IVT",
    textEntity: {
      brand: "",
      model: "SX",
      fullModel: "SX",
      matchedText: "SX",
    },
    contextEntity: {
      brand: "Hyundai",
      model: "Verna",
      fullModel: "Hyundai Verna",
      fromContext: true,
    },
  });

  const failures = [];
  expectEqual(failures, "chosen.model", chosen?.model, "Verna");
  expectEqual(failures, "chosen.brand", chosen?.brand, "Hyundai");
  return failures;
});

addCase("build-context-model-entity-strips-make-from-model", async () => {
  const entity = buildAciContextModelEntity({
    context: {
      anchorMake: "Hyundai",
      anchorModel: "Hyundai Verna",
      anchorFullModel: "Hyundai Verna",
      selectedVehicle: {
        make: "Hyundai",
        model: "Hyundai Verna",
      },
    },
  });

  const failures = [];
  expectEqual(failures, "entity.brand", entity?.brand, "Hyundai");
  expectEqual(failures, "entity.model", entity?.model, "Verna");
  expectEqual(failures, "entity.fullModel", entity?.fullModel, "Hyundai Verna");
  expectEqual(failures, "entity.method", entity?.method, "context_anchor");
  return failures;
});

addCase("repair-context-hydrates-missing-make-fullmodel-for-same-model", async () => {
  const response = {
    contextPatch: {
      anchorModel: "Verna",
      selectedVehicle: {
        model: "Verna",
      },
    },
  };

  const context = {
    anchorModel: "Verna",
    anchorCity: "new-delhi",
    selectedVehicle: {
      model: "Verna",
      city: "new-delhi",
    },
  };

  await repairAciResponseContextFromActiveContext({
    response,
    context,
    hydrateModelEntity: async () => ({
      make: "Hyundai",
      brand: "Hyundai",
      model: "Verna",
      fullModel: "Hyundai Verna",
      displayName: "Hyundai Verna",
    }),
  });

  const failures = [];
  expectEqual(failures, "anchorMake", response.contextPatch.anchorMake, "Hyundai");
  expectEqual(failures, "anchorModel", response.contextPatch.anchorModel, "Verna");
  expectEqual(failures, "anchorFullModel", response.contextPatch.anchorFullModel, "Hyundai Verna");
  expectEqual(failures, "selectedVehicle.make", response.contextPatch.selectedVehicle.make, "Hyundai");
  expectEqual(failures, "selectedVehicle.fullModel", response.contextPatch.selectedVehicle.fullModel, "Hyundai Verna");
  return failures;
});

addCase("repair-context-does-not-force-active-context-into-different-response-model", async () => {
  const response = {
    contextPatch: {
      anchorMake: "Kia",
      anchorModel: "Seltos",
      selectedVehicle: {
        make: "Kia",
        model: "Seltos",
      },
    },
  };

  const before = clone(response);

  const context = {
    anchorMake: "Hyundai",
    anchorModel: "Verna",
    selectedVehicle: {
      make: "Hyundai",
      model: "Verna",
    },
  };

  await repairAciResponseContextFromActiveContext({
    response,
    context,
    hydrateModelEntity: async () => ({
      make: "Hyundai",
      model: "Verna",
      fullModel: "Hyundai Verna",
    }),
  });

  const failures = [];
  expectEqual(failures, "anchorMake", response.contextPatch.anchorMake, before.contextPatch.anchorMake);
  expectEqual(failures, "anchorModel", response.contextPatch.anchorModel, before.contextPatch.anchorModel);
  expectEqual(failures, "selectedVehicle.make", response.contextPatch.selectedVehicle.make, before.contextPatch.selectedVehicle.make);
  expectEqual(failures, "selectedVehicle.model", response.contextPatch.selectedVehicle.model, before.contextPatch.selectedVehicle.model);
  return failures;
});

const main = async () => {
  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();
    let failures = [];

    try {
      failures = await testCase.run();
    } catch (err) {
      failures = [err?.stack || err?.message || String(err)];
    }

    results.push({
      id: testCase.id,
      pass: failures.length === 0,
      durationMs: Date.now() - startedAt,
      failures,
    });
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI context priority audit",
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
  }, null, 2));

  if (failed.length) process.exit(1);
};

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
