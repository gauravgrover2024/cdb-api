import InsuranceCase from "../../models/InsuranceCase.js";
import Loan from "../../models/Loan.js";
import Payment from "../../models/Payment.js";
import Quotation from "../../models/Quotation.js";
import Receivable from "../../models/Receivable.js";
import Vehicle from "../../models/Vehicle.js";
import VehicleFeature from "../../models/VehicleFeature.js";
import VehicleRecord from "../../models/VehicleRecord.js";
import { runGlobalSearch } from "./globalSearchService.js";
import { normalizeText, parseGlobalSearchQuery } from "./queryParser.js";

const FEATURE_KEYWORDS = [
  "sunroof",
  "adas",
  "airbag",
  "airbags",
  "camera",
  "360",
  "ventilated",
  "cruise",
  "tpms",
  "alloy",
  "wireless",
  "carplay",
  "android auto",
];

const CAPS = {
  answers: 8,
  similarCars: 5,
};

const LOCAL_TIMEZONE = "Asia/Kolkata";

const formatCurrency = (value) =>
  new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 0,
  }).format(Number(value || 0));

const formatDateTime = (value) => {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return new Intl.DateTimeFormat("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
    timeZone: LOCAL_TIMEZONE,
  }).format(d);
};

const monthBoundaries = () => {
  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
  const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);
  return { now, monthStart, monthEnd };
};

const hasAny = (source, terms = []) => terms.some((term) => source.includes(term));

const pickVehiclePrice = (doc = {}) => {
  const candidates = [
    doc.total_on_road_with_accessories,
    doc.on_road_price_cardekho,
    doc.onRoadPrice,
    doc.orp_without_accessories,
    doc.ex_showroom,
  ];
  const value = candidates.find((v) => Number.isFinite(Number(v)) && Number(v) > 0);
  return Number(value || 0);
};

const parseMonthYearMention = (queryText) => {
  const text = normalizeText(queryText);
  const yearMatch = text.match(/\b(20\d{2})\b/);
  const monthMap = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11,
  };
  let monthIndex = null;
  Object.entries(monthMap).forEach(([name, index]) => {
    if (monthIndex !== null) return;
    if (text.includes(name)) monthIndex = index;
  });
  if (!yearMatch || monthIndex === null) return null;
  const year = Number(yearMatch[1]);
  if (!Number.isFinite(year)) return null;
  const start = new Date(year, monthIndex, 1, 0, 0, 0, 0);
  const end = new Date(year, monthIndex + 1, 0, 23, 59, 59, 999);
  return { start, end };
};

const buildOpsAnswer = (id, title, value, details, tone = "info", route = "") => ({
  id,
  title,
  value,
  details,
  tone,
  route,
});

const buildVehicleTokenRegex = (tokens = []) => {
  const meaningful = tokens.filter((token) => token.length >= 3).slice(0, 3);
  if (!meaningful.length) return null;
  return meaningful.map((token) => new RegExp(token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i"));
};

const getVehicleRegexTokens = (parsed = {}) => {
  const noise = new Set([
    "latest",
    "insurance",
    "loan",
    "closure",
    "payment",
    "pending",
    "receivable",
    "payout",
    "compare",
    "cars",
    "car",
    "price",
    "updated",
    "update",
    "last",
    "month",
    "this",
    "show",
    "of",
    "and",
  ]);
  return (parsed.tokensRaw || []).filter((token) => token && !noise.has(token) && token.length >= 3);
};

const getFeatureNeedle = (queryText = "") => {
  const q = normalizeText(queryText);
  return FEATURE_KEYWORDS.find((needle) => q.includes(needle)) || "";
};

const runVehicleFeatureInsight = async (queryText, parsed) => {
  const needle = getFeatureNeedle(queryText);
  if (!needle) return null;

  const regexes = buildVehicleTokenRegex(parsed.tokensRaw || []);
  const match = {};
  if (regexes?.length) {
    match.$or = regexes.flatMap((regex) => [{ brand: regex }, { model: regex }, { variant: regex }]);
  }

  const docs = await VehicleFeature.find(match, {
    brand: 1,
    model: 1,
    variant: 1,
    features: 1,
    updatedAt: 1,
  })
    .sort({ updatedAt: -1 })
    .limit(20)
    .lean();

  const hits = [];
  docs.forEach((doc) => {
    const flat = JSON.stringify(doc.features || {}).toLowerCase();
    if (flat.includes(needle.toLowerCase())) {
      hits.push(doc);
    }
  });

  if (!hits.length) {
    return buildOpsAnswer(
      "feature-check",
      `Feature check: ${needle}`,
      "No strong match found",
      "I scanned available feature records but couldn't find a confident match for this feature in the queried variants.",
      "warning",
      "/loans/features",
    );
  }

  const variants = hits
    .slice(0, 4)
    .map((doc) => `${doc.brand || ""} ${doc.model || ""} ${doc.variant || ""}`.trim())
    .filter(Boolean);
  return buildOpsAnswer(
    "feature-check",
    `Feature check: ${needle}`,
    `${hits.length} variant${hits.length > 1 ? "s" : ""} matched`,
    `Top matches: ${variants.join(", ")}`,
    "success",
    "/loans/features",
  );
};

const runSimilarCarsInsight = async (queryText, parsed) => {
  const lower = normalizeText(queryText);
  const trigger = hasAny(lower, ["similar", "compare", "alternative", "options"]);
  if (!trigger) return null;

  const regexes = buildVehicleTokenRegex(parsed.tokensRaw || []);
  if (!regexes?.length) return null;

  const anchor = await Vehicle.findOne(
    {
      $or: regexes.flatMap((regex) => [{ make: regex }, { model: regex }, { variant: regex }]),
      is_discontinued: { $ne: true },
    },
    { make: 1, model: 1, variant: 1, city: 1, updatedAt: 1, onRoadPrice: 1, on_road_price_cardekho: 1, total_on_road_with_accessories: 1 },
  )
    .sort({ updatedAt: -1 })
    .lean();

  if (!anchor) return null;

  const anchorPrice = pickVehiclePrice(anchor);
  const range = anchorPrice > 0 ? Math.max(60000, anchorPrice * 0.15) : 0;
  const minPrice = anchorPrice > 0 ? anchorPrice - range : 0;
  const maxPrice = anchorPrice > 0 ? anchorPrice + range : Number.MAX_SAFE_INTEGER;

  const similar = await Vehicle.find(
    {
      _id: { $ne: anchor._id },
      is_discontinued: { $ne: true },
      ...(anchor.city ? { city: anchor.city } : {}),
      $or: [
        { total_on_road_with_accessories: { $gte: minPrice, $lte: maxPrice } },
        { on_road_price_cardekho: { $gte: minPrice, $lte: maxPrice } },
        { onRoadPrice: { $gte: minPrice, $lte: maxPrice } },
      ],
    },
    { make: 1, model: 1, variant: 1, city: 1, onRoadPrice: 1, on_road_price_cardekho: 1, total_on_road_with_accessories: 1 },
  )
    .sort({ updatedAt: -1 })
    .limit(CAPS.similarCars)
    .lean();

  if (!similar.length) return null;

  const details = similar
    .map((car) => `${car.make} ${car.model} ${car.variant} (${formatCurrency(pickVehiclePrice(car))})`)
    .join(" | ");
  return buildOpsAnswer(
    "similar-cars",
    "Similar cars",
    `${similar.length} suggestion${similar.length > 1 ? "s" : ""}`,
    details,
    "success",
    "/vehicles/price-list",
  );
};

const runOperationalInsights = async (queryText, parsed) => {
  const answers = [];
  const lower = normalizeText(queryText);
  const { monthStart, monthEnd } = monthBoundaries();
  const vehicleRegexes = buildVehicleTokenRegex(getVehicleRegexTokens(parsed));

  if (hasAny(lower, ["latest insurance", "current insurance", "policy"])) {
    const latestInsuranceMatch = {};
    if (vehicleRegexes?.length) {
      latestInsuranceMatch.$or = vehicleRegexes.flatMap((regex) => [
        { customerName: regex },
        { companyName: regex },
        { registrationNumber: regex },
        { vehicleMake: regex },
        { vehicleModel: regex },
        { vehicleVariant: regex },
      ]);
    }
    if (parsed.vehicleLast4) {
      latestInsuranceMatch.registrationNumber = new RegExp(`${parsed.vehicleLast4}$`, "i");
    }
    const latestCase = await InsuranceCase.findOne(
      latestInsuranceMatch,
      {
        caseId: 1,
        customerName: 1,
        companyName: 1,
        registrationNumber: 1,
        vehicleMake: 1,
        vehicleModel: 1,
        vehicleVariant: 1,
        status: 1,
        updatedAt: 1,
      },
    )
      .sort({ updatedAt: -1, createdAt: -1 })
      .lean();
    if (latestCase) {
      answers.push(
        buildOpsAnswer(
          "latest-insurance-case",
          "Latest insurance record",
          latestCase.caseId || "Insurance Case",
          `${latestCase.companyName || latestCase.customerName || "Case"} • ${[
            latestCase.vehicleMake,
            latestCase.vehicleModel,
            latestCase.vehicleVariant,
          ]
            .filter(Boolean)
            .join(" ")} • ${latestCase.registrationNumber || "Reg: —"} • Updated ${formatDateTime(latestCase.updatedAt)}`,
          "success",
          latestCase.caseId ? `/insurance/edit/${encodeURIComponent(String(latestCase.caseId))}` : "/insurance",
        ),
      );
    }
  }

  if (hasAny(lower, ["disbursed", "disbursal"])) {
    const disbursedCount = await Loan.countDocuments({
      $and: [
        {
          $or: [
            { status: /disbursed/i },
            { loanStatus: /disbursed/i },
            { currentStage: /disbursed/i },
            { disbursementStatus: /disbursed/i },
            { disburse_status: /disbursed/i },
            { disburse_amount: { $gt: 0 } },
            { approval_loanAmountDisbursed: { $gt: 0 } },
            { postfile_loanAmountDisbursed: { $gt: 0 } },
          ],
        },
        {
          $or: [
            { disbursedDate: { $gte: monthStart, $lte: monthEnd } },
            { disbursementDate: { $gte: monthStart, $lte: monthEnd } },
            { disbursement_date: { $gte: monthStart, $lte: monthEnd } },
            { approval_disbursedDate: { $gte: monthStart, $lte: monthEnd } },
            { disburse_date: { $gte: monthStart, $lte: monthEnd } },
          ],
        },
      ],
    });
    answers.push(
      buildOpsAnswer(
        "disbursed-month",
        "Loan disbursals (this month)",
        `${disbursedCount}`,
        `Updated between ${formatDateTime(monthStart)} and ${formatDateTime(monthEnd)}.`,
        disbursedCount > 0 ? "success" : "info",
        "/loans",
      ),
    );
  }

  if (hasAny(lower, ["insurance renewals", "renewals", "renewal"])) {
    const renewalsThisMonth = await InsuranceCase.countDocuments({
      $or: [{ usedCarFlowType: /renewal/i }, { policyJourneyClassification: /renewal/i }],
      updatedAt: { $gte: monthStart, $lte: monthEnd },
    });
    answers.push(
      buildOpsAnswer(
        "insurance-renewals-month",
        "Insurance renewals (this month)",
        `${renewalsThisMonth}`,
        `Based on renewal-classified insurance cases touched this month.`,
        renewalsThisMonth > 0 ? "success" : "info",
        "/insurance",
      ),
    );
  }

  if (hasAny(lower, ["payout", "receivable", "expected payout"])) {
    const payoutAgg = await Receivable.aggregate([
      {
        $match: {
          updatedAt: { $gte: monthStart, $lte: monthEnd },
          payout_amount: { $gt: 0 },
        },
      },
      {
        $group: {
          _id: null,
          totalExpectedPayout: { $sum: "$payout_amount" },
          totalCases: { $sum: 1 },
        },
      },
    ]);
    const payout = payoutAgg?.[0] || { totalExpectedPayout: 0, totalCases: 0 };
    answers.push(
      buildOpsAnswer(
        "expected-payout-month",
        "Expected payout (this month)",
        formatCurrency(payout.totalExpectedPayout || 0),
        `${payout.totalCases || 0} receivable case(s) updated this month.`,
        payout.totalExpectedPayout > 0 ? "success" : "info",
        "/payouts/receivables",
      ),
    );
  }

  if (hasAny(lower, ["payout has not been entered", "payout pending", "without payout"])) {
    const missingPayoutCount = await Loan.countDocuments({
      payoutApplicable: /yes/i,
      $or: [
        { prefile_sourcePayoutPercentage: { $exists: false } },
        { prefile_sourcePayoutPercentage: null },
        { prefile_sourcePayoutPercentage: 0 },
      ],
    });
    answers.push(
      buildOpsAnswer(
        "payout-not-entered",
        "Cases with payout missing",
        `${missingPayoutCount}`,
        "Payout applicable = Yes, but payout percentage is blank or zero.",
        missingPayoutCount > 0 ? "warning" : "success",
        "/loans",
      ),
    );
  }

  if (hasAny(lower, ["without registration", "without registeration", "no registration"])) {
    const withoutReg = await VehicleRecord.countDocuments({
      $or: [
        { registrationNumber: { $exists: false } },
        { registrationNumber: null },
        { registrationNumber: "" },
      ],
    });
    answers.push(
      buildOpsAnswer(
        "vehicles-without-reg",
        "Vehicles missing registration",
        `${withoutReg}`,
        "Pulled from vehicle master records synced from loans.",
        withoutReg > 0 ? "warning" : "success",
        "/vehicles/manage",
      ),
    );
  }

  if (hasAny(lower, ["price revision", "price updated", "price revised"])) {
    const customMonth = parseMonthYearMention(lower);
    const windowStart = customMonth?.start || monthStart;
    const windowEnd = customMonth?.end || monthEnd;

    const revisions = await Vehicle.countDocuments({
      updatedAt: { $gte: windowStart, $lte: windowEnd },
    });
    answers.push(
      buildOpsAnswer(
        "price-revision-window",
        "Vehicle price revisions",
        `${revisions}`,
        `${customMonth ? "Selected month" : "This month"} window: ${formatDateTime(windowStart)} to ${formatDateTime(windowEnd)}.`,
        revisions > 0 ? "success" : "info",
        "/vehicles/price-list",
      ),
    );
  }

  if (hasAny(lower, ["price updated last", "updated last", "latest price"])) {
    if (vehicleRegexes?.length) {
      const latestVehicle = await Vehicle.findOne(
        {
          $or: vehicleRegexes.flatMap((regex) => [
            { make: regex },
            { model: regex },
            { variant: regex },
          ]),
        },
        {
          make: 1,
          model: 1,
          variant: 1,
          city: 1,
          updatedAt: 1,
          onRoadPrice: 1,
          on_road_price_cardekho: 1,
          total_on_road_with_accessories: 1,
        },
      )
        .sort({ updatedAt: -1 })
        .lean();
      if (latestVehicle) {
        answers.push(
          buildOpsAnswer(
            "latest-price-update",
            "Latest price update",
            `${latestVehicle.make || ""} ${latestVehicle.model || ""} ${latestVehicle.variant || ""}`.trim(),
            `Last updated ${formatDateTime(latestVehicle.updatedAt)} • ${formatCurrency(
              pickVehiclePrice(latestVehicle),
            )}`,
            "info",
            "/vehicles/price-list",
          ),
        );
      }
    }
  }

  if (hasAny(lower, ["discontinued"])) {
    const customMonth = parseMonthYearMention(lower);
    const match = { is_discontinued: true };
    if (customMonth) {
      match.updatedAt = { $gte: customMonth.start, $lte: customMonth.end };
    }
    const discontinuedCount = await Vehicle.countDocuments(match);
    answers.push(
      buildOpsAnswer(
        "discontinued-cars",
        "Discontinued cars",
        `${discontinuedCount}`,
        customMonth
          ? `Filtered for ${formatDateTime(customMonth.start)} to ${formatDateTime(customMonth.end)}.`
          : "Across all tracked records.",
        discontinuedCount > 0 ? "warning" : "success",
        "/vehicles/price-list",
      ),
    );
  }

  if (hasAny(lower, ["receivable", "receivables", "outstanding company", "particular company"])) {
    if (vehicleRegexes?.length) {
      const byCompany = await Receivable.aggregate([
        {
          $match: {
            $or: vehicleRegexes.map((regex) => ({ payout_party_name: regex })),
          },
        },
        {
          $group: {
            _id: "$payout_party_name",
            amount: { $sum: "$payout_amount" },
            count: { $sum: 1 },
          },
        },
        { $sort: { amount: -1 } },
        { $limit: 1 },
      ]);
      if (byCompany?.length) {
        const top = byCompany[0];
        answers.push(
          buildOpsAnswer(
            "receivable-company",
            "Receivables by company",
            `${top._id || "Unknown"} • ${formatCurrency(top.amount || 0)}`,
            `${top.count || 0} receivable row(s) matched your query.`,
            top.amount > 0 ? "warning" : "info",
            "/payouts/receivables",
          ),
        );
      }
    }
  }

  const featureInsight = await runVehicleFeatureInsight(queryText, parsed);
  if (featureInsight) answers.push(featureInsight);

  const similarCarsInsight = await runSimilarCarsInsight(queryText, parsed);
  if (similarCarsInsight) answers.push(similarCarsInsight);

  if (hasAny(lower, ["quotation", "quote"])) {
    const regexes = buildVehicleTokenRegex(parsed.tokensRaw || []);
    const quoteMatch = regexes?.length
      ? {
          $or: regexes.flatMap((regex) => [
            { vehicleBrand: regex },
            { vehicleModel: regex },
            { vehicleVariant: regex },
            { customerName: regex },
            { customerMobile: regex },
            { city: regex },
          ]),
        }
      : {};
    const quoteCount = await Quotation.countDocuments(quoteMatch);
    answers.push(
      buildOpsAnswer(
        "quotations-matched",
        "Quotation records",
        `${quoteCount}`,
        regexes?.length ? "Matched against customer/vehicle tokens in your query." : "Total available quotations.",
        quoteCount > 0 ? "success" : "info",
        "/loans/quotations",
      ),
    );
  }

  if (hasAny(lower, ["price list", "pricelist", "emi", "features"])) {
    const adapterText = [];
    if (hasAny(lower, ["price list", "pricelist"])) adapterText.push("Price list");
    if (hasAny(lower, ["emi"])) adapterText.push("EMI calculator");
    if (hasAny(lower, ["features"])) adapterText.push("Features catalog");
    answers.push(
      buildOpsAnswer(
        "ops-tools-hint",
        "Tools quick route",
        adapterText.join(" • "),
        "You can open the matching operational tool directly from this result.",
        "info",
        hasAny(lower, ["emi"])
          ? "/loans/emi-calculator"
          : hasAny(lower, ["features"])
            ? "/loans/features"
            : "/vehicles/price-list",
      ),
    );
  }

  if (hasAny(lower, ["compare", "comparison"]) && hasAny(lower, ["car", "cars"])) {
    if (vehicleRegexes?.length) {
      const compared = await Vehicle.find(
        {
          $or: vehicleRegexes.flatMap((regex) => [{ make: regex }, { model: regex }, { variant: regex }]),
          is_discontinued: { $ne: true },
        },
        { make: 1, model: 1, variant: 1, onRoadPrice: 1, on_road_price_cardekho: 1, total_on_road_with_accessories: 1 },
      )
        .sort({ updatedAt: -1 })
        .limit(3)
        .lean();
      if (compared.length) {
        const details = compared
          .map((v) => `${[v.make, v.model, v.variant].filter(Boolean).join(" ")} (${formatCurrency(pickVehiclePrice(v))})`)
          .join(" vs ");
        answers.push(
          buildOpsAnswer(
            "car-comparison",
            "Quick car comparison",
            `${compared.length} car${compared.length > 1 ? "s" : ""}`,
            details,
            "info",
            "/vehicles/price-list",
          ),
        );
      }
    }
  }

  if (hasAny(lower, ["who updated"]) && hasAny(lower, ["loan"])) {
    const regexes = buildVehicleTokenRegex(parsed.tokensRaw || []);
    const loanMatch = regexes?.length
      ? {
          $or: regexes.flatMap((regex) => [
            { customerName: regex },
            { registrationNumber: regex },
            { vehicleMake: regex },
            { vehicleModel: regex },
            { vehicleVariant: regex },
          ]),
        }
      : {};
    const loan = await Loan.findOne(loanMatch, {
      loanId: 1,
      customerName: 1,
      updatedAt: 1,
      updatedBy: 1,
      status: 1,
    })
      .sort({ updatedAt: -1 })
      .lean();
    if (loan) {
      answers.push(
        buildOpsAnswer(
          "loan-updated-by",
          "Latest loan update",
          loan.customerName || loan.loanId || "Loan",
          `Last updated on ${formatDateTime(loan.updatedAt)}. Updated-by user id: ${loan.updatedBy || "not captured"}.`,
          "info",
          loan._id ? `/loans/edit/${loan._id}` : "/loans",
        ),
      );
    }
  }

  if (hasAny(lower, ["payment pending"]) || (parsed.intents.includes("payment") && parsed.intents.includes("latest"))) {
    const pendingPayments = await Payment.countDocuments({
      $or: [
        { isVerified: { $ne: true } },
        { isAutocreditsVerified: { $ne: true } },
      ],
      updatedAt: { $gte: monthStart, $lte: monthEnd },
    });
    answers.push(
      buildOpsAnswer(
        "payment-pending-month",
        "Payment pending sheets (this month)",
        `${pendingPayments}`,
        "Pending status inferred from verification flags.",
        pendingPayments > 0 ? "warning" : "success",
        "/payments",
      ),
    );
  }

  return answers.slice(0, CAPS.answers);
};

export const runAssistSearch = async ({ query, limit = 40, perEntityLimit = 8 }) => {
  const parsed = parseGlobalSearchQuery(query);
  const base = await runGlobalSearch({ query, limit, perEntityLimit });
  let answers = await runOperationalInsights(query, parsed);
  if (!answers.length && Array.isArray(base.results) && base.results.length) {
    const top = base.results[0];
    answers = [
      buildOpsAnswer(
        "best-match",
        "Best matching record",
        top.title || top.recordId || "Result",
        `${top.entityLabel || top.entity} • ${top.subtitle || top.recordId}`,
        "info",
        top.route || "",
      ),
    ];
  }

  return {
    ...base,
    answers,
    capabilities: [
      "Search by name, mobile, vehicle, policy, loan, reg number or last 4 digits.",
      "Find latest insurance / loan / payment records with intent-aware ranking.",
      "Check disbursals, renewal counts, payout gaps and registration data health.",
      "Fetch similar cars, feature hints and price revision/discontinued snapshots.",
      "Jump directly to module pages with deep links from results.",
    ],
  };
};
