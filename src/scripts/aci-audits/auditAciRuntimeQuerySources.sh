#!/usr/bin/env bash
set -euo pipefail

OUT="aci_runtime_query_source_audit_$(date +%Y%m%d_%H%M%S).md"

{
  echo "# ACI Runtime Query Source Audit"
  echo
  echo "Generated: $(date)"
  echo

  echo "## listCollections / collection existence checks"
  grep -RInE "listCollections|collectionExists|db\\.collection|mongoose\\.connection\\.db|connection\\.db" \
    src/services/aiAgent src/models src/controllers src/routes 2>/dev/null || true

  echo
  echo "## Broad model OR query patterns"
  grep -RInE "modelName|model_name|model_normalized|modelNormalized|cityName|city_name|citySlug|city_slug|brandName|\\$or|\\$and" \
    src/services/aiAgent src/models 2>/dev/null || true

  echo
  echo "## Vehicle/features/colors query functions"
  grep -RInE "Vehicle\\.|VehicleFeature|vehicle_features|vehicle_colors|vehicle_colors_v2|vehicle_variant_feature_matrix_v2|price_history|find\\(|aggregate\\(|limit\\(1200\\)|limit\\(240\\)|limit\\(50\\)" \
    src/services/aiAgent src/models 2>/dev/null || true

  echo
  echo "## Entity resolver / hint / catalog warmup paths"
  grep -RInE "refreshVehicleHints|vehicleHints|entityIndex|resolveAciDynamicModelEntity|resolve.*Model|semanticCompiler|modelResolver|catalog|hints|warm" \
    src/services/aiAgent src/server.js 2>/dev/null || true

  echo
  echo "## Tool entry points"
  grep -RInE "runVehicle|vehicle_pricelist|vehicle_colors|vehicle_feature|vehicle_emi|vehicle_compare|executeAciPlannerPlan|runtimeResultsMeta" \
    src/services/aiAgent 2>/dev/null || true

} > "$OUT"

echo "✅ Wrote $OUT"
echo "$OUT"
