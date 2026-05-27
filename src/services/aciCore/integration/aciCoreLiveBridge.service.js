import {
  retrieveAciDbCandidates,
} from "../candidates/aciDbCandidateRetriever.js";
import { parseHybridMeaningFrame } from "../understanding/hybridMeaningFrame.parser.js";
import { runAciUnderstandingEngine } from "../understanding/aciUnderstandingEngine.js";
import { buildLegacyPlanFromAciMeaningFrame } from "./aciCoreToLegacyPlan.adapter.js";
import { executeAciPlannerPlan } from "../../aiAgent/aiAgent.executor.js";
import { normalizeAciFinalResponse } from "../../aiAgent/aiAgent.contractNormalizer.js";

const truthy = (value = "") =>
  ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());

const isAciCoreLiveBridgeEnabled = () =>
  truthy(process.env.ACI_CORE_LIVE_BRIDGE_ENABLED);

const shouldUseAciCoreLiveBridge = ({ message = "" } = {}) => {
  if (!isAciCoreLiveBridgeEnabled()) return false;

  const text = String(message || "").trim();
  if (!text) return false;

  return true;
};

export const runAciCoreLiveBridge = async ({
  message = "",
  context = {},
  user = null,
  session = null,
  meta = {},
} = {}) => {
  const startedAt = Date.now();

  const understanding = await runAciUnderstandingEngine({
    message,
    context,
    candidateRetriever: retrieveAciDbCandidates,
    parser: parseHybridMeaningFrame,
  });

  const plan = buildLegacyPlanFromAciMeaningFrame({
    meaningFrame: understanding.meaningFrame,
    message,
    context,
  });

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context,
    user,
    session,
    meta,
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message,
    context,
  });

  return {
    ...normalized,
    aciCoreBridge: {
      enabled: true,
      durationMs: Date.now() - startedAt,
      selectedParser: understanding.selectedParser || "",
      usedGemini: Boolean(understanding.usedGemini),
      primaryTask: understanding.meaningFrame?.primaryTask || "",
      tool: plan.tools?.[0]?.tool || "",
      planMode: plan.mode || "",
    },
    meta: {
      ...(normalized.meta || {}),
      aciCoreBridge: {
        enabled: true,
        durationMs: Date.now() - startedAt,
        selectedParser: understanding.selectedParser || "",
        usedGemini: Boolean(understanding.usedGemini),
        primaryTask: understanding.meaningFrame?.primaryTask || "",
        tool: plan.tools?.[0]?.tool || "",
        planMode: plan.mode || "",
      },
    },
  };
};

export {
  isAciCoreLiveBridgeEnabled,
  shouldUseAciCoreLiveBridge,
};
