import dotenv from "dotenv";
import mongoose from "mongoose";
import {
  buildVehicleFeatureDataIndex,
  searchDbFeatureRows,
} from "../services/aiAgent/aiAgent.featureDataAdapter.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const PROBES = [
  { model: "Creta", feature: "sunroof" },
  { model: "Creta", feature: "rear camera" },
  { model: "Creta", feature: "reverse camera" },
  { model: "Creta", feature: "ventilated seats" },
  { model: "Creta", feature: "wireless charging" },
  { model: "Creta", feature: "alloys" },
  { model: "Creta", feature: "led headlights" },
  { model: "Verna", feature: "sunroof" },
  { model: "Verna", feature: "adas" },
  { model: "Seltos", feature: "ventilated seats" },
  { model: "Seltos", feature: "bose speakers" },
  { model: "Thar", feature: "sunroof" },
  { model: "Thar", feature: "rear camera" },
];

const main = async () => {
  await mongoose.connect(mongoUri);

  const index = await buildVehicleFeatureDataIndex({ force: true });

  console.log("FEATURE DATA INDEX STATS");
  console.log(JSON.stringify(index.stats, null, 2));

  console.log("\nSECTIONS");
  console.log(index.sections.slice(0, 30));

  console.log("\nFEATURE NAME SAMPLE");
  console.log(
    index.featureNames.slice(0, 40).map((item) => ({
      featureName: item.featureName,
      aliases: item.aliases.slice(0, 8),
      count: item.count,
      availableCount: item.availableCount,
    })),
  );

  console.log("\nPROBES");

  for (const probe of PROBES) {
    const result = searchDbFeatureRows({
      index,
      model: probe.model,
      featurePhrase: probe.feature,
      includeUnavailable: true,
    });

    console.log(
      JSON.stringify({
        probe,
        modelCoverageRows: result.modelCoverageRows,
        resolvedFeature: result.resolvedFeature
          ? {
              featureName: result.resolvedFeature.featureName,
              matchedAlias: result.resolvedFeature.matchedAlias,
              score: result.resolvedFeature.score,
              sections: result.resolvedFeature.sections,
            }
          : null,
        totalRows: result.rows.length,
        availableRows: result.availableRows.length,
        unavailableRows: result.unavailableRows.length,
        sampleAvailable: result.availableRows.slice(0, 5).map((row) => ({
          variant: row.variant,
          featureName: row.featureName,
          section: row.section,
          value: row.displayValue,
        })),
        sampleUnavailable: result.unavailableRows.slice(0, 3).map((row) => ({
          variant: row.variant,
          featureName: row.featureName,
          section: row.section,
          value: row.displayValue,
        })),
      }),
    );
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
