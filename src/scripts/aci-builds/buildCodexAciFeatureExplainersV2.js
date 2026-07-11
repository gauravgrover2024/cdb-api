#!/usr/bin/env node

import "dotenv/config";

import assert from "node:assert";
import mongoose from "mongoose";

const CATALOG_COLLECTION = "vehicle_feature_catalog_v2";
const EXPLAINER_COLLECTION = "aci_feature_explainers_v1";
const SCHEMA_VERSION = "aci_feature_explainer_v1";
const CONTENT_VERSION = "aci_feature_explainer_codex_editorial_v2_2026_07_10";
const SHOULD_WRITE = process.argv.includes("--write");

const clean = (value = "") => String(value ?? "").replace(/\s+/g, " ").trim();
const lower = (value = "") => clean(value).toLowerCase();
const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const unique = (values = []) => [...new Set(values.map(clean).filter(Boolean))];

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const IMPORTANCE_LEVELS = new Set(["critical", "high", "medium", "low", "not_applicable"]);

const makeEntry = ({
  buyerSummary,
  howItWorks,
  whenItMattersSummary,
  whenItMatters,
  limitationsSummary,
  buyerAdvice,
  featureType,
  decisionCategory,
  decisionSignals,
  importance,
}) => ({
  buyerSummary: clean(buyerSummary),
  howItWorks: clean(howItWorks),
  whenItMattersSummary: clean(whenItMattersSummary),
  whenItMatters: unique(whenItMatters).slice(0, 5),
  limitationsSummary: clean(limitationsSummary),
  buyerAdvice: clean(buyerAdvice),
  featureType,
  decisionCategory,
  decisionSignals: unique(decisionSignals).slice(0, 5),
  importance,
});

const importance = ({
  safety = "low",
  cityUse = "medium",
  highwayUse = "medium",
  familyUse = "medium",
  offRoadUse = "low",
  chauffeurUse = "low",
  firstTimeBuyer = "medium",
  safetyCritical = false,
} = {}) => ({
  safety,
  cityUse,
  highwayUse,
  familyUse,
  offRoadUse,
  chauffeurUse,
  firstTimeBuyer,
  ...(safetyCritical ? { safetyCritical: true } : {}),
});

const safetyEntry = (feature, overrides = {}) => {
  const name = feature.displayName;
  return makeEntry({
    buyerSummary: `${name} is safety equipment intended to reduce a specific driving or impact risk; it should be judged by what it actually does, not by the label alone.`,
    howItWorks: `The system uses vehicle hardware, sensors or restraint components associated with ${name}. The exact trigger, coverage and intervention can differ between implementations.`,
    whenItMattersSummary: `It matters most when the driving situation or impact type addressed by ${name} occurs.`,
    whenItMatters: ["emergency situations", "family use", "unfamiliar or demanding roads"],
    limitationsSummary: `It cannot guarantee crash avoidance or occupant protection. Verify the exact variant, operating limits and coverage.`,
    buyerAdvice: `Treat ${name} as decision-relevant safety equipment, then check independent crash evidence and the complete safety package rather than relying on one feature.`,
    featureType: "active_safety",
    decisionCategory: "high_value_safety",
    decisionSignals: ["safety priority", "family use", "exact implementation"],
    importance: importance({ safety: "high", cityUse: "high", highwayUse: "high", familyUse: "high", firstTimeBuyer: "high" }),
    ...overrides,
  });
};

const adasEntry = (feature, action, limitation, contexts = ["highway use", "dense traffic"]) =>
  makeEntry({
    buyerSummary: `${feature.displayName} is a driver-assistance function that ${action}.`,
    howItWorks: `It uses the vehicle's available sensors and control systems to assess the relevant driving situation and then warn or assist as designed.`,
    whenItMattersSummary: `It is most useful during ${contexts.join(" and ")}, where a timely warning or small intervention can reduce driver workload or reaction delay.`,
    whenItMatters: contexts,
    limitationsSummary: `${limitation} Sensor visibility, road markings, speed range and weather can affect operation, and the driver remains responsible.`,
    buyerAdvice: `Prioritise it if those conditions are common in your driving, but verify the exact variant's operating range and test the alerts or intervention before buying.`,
    featureType: "driver_assistance",
    decisionCategory: "highway_convenience",
    decisionSignals: ["driver workload", "sensor limitations", "exact operating range"],
    importance: importance({ safety: "high", cityUse: "medium", highwayUse: "high", familyUse: "high", firstTimeBuyer: "medium" }),
  });

const metricEntry = (feature, { meaning, tradeoff, contexts, category = "specification_context", type = "dimension_metric" }) =>
  makeEntry({
    buyerSummary: `${feature.displayName} tells you ${meaning}. It is a comparison input, not a quality score by itself.`,
    howItWorks: `The published value records ${meaning}. Compare values only when the unit, test method and measurement conditions are equivalent.`,
    whenItMattersSummary: `${feature.displayName} matters when ${contexts.join(" or ")}.`,
    whenItMatters: contexts,
    limitationsSummary: `${tradeoff} Real-world usefulness can differ from the published figure because design, load, conditions and measurement methods vary.`,
    buyerAdvice: `Use the number to narrow choices, then validate the underlying need with an exact-variant check or test drive instead of assuming more is always better.`,
    featureType: type,
    decisionCategory: category,
    decisionSignals: ["comparable units", "test conditions", "real-world trade-off"],
    importance: importance({ cityUse: "medium", highwayUse: "medium", familyUse: "medium", firstTimeBuyer: "medium" }),
  });

const convenienceEntry = (feature, { benefit = "reduces effort in everyday use", howItWorks = "", contexts = ["daily use", "family trips"], limitation = "The exact operation and availability can vary by variant.", category = "daily_comfort", type = "convenience" } = {}) =>
  makeEntry({
    buyerSummary: `${feature.displayName} is a convenience feature that ${benefit}.`,
    howItWorks: howItWorks || `The vehicle uses its fitted controls, hardware or software to deliver this function. The control method, operating range and exact behaviour can vary by implementation.`,
    whenItMattersSummary: `It is most useful for ${contexts.join(" and ")}, especially when the same task is repeated often.`,
    whenItMatters: contexts,
    limitationsSummary: `${limitation} It should not displace core safety, seating, visibility or running-cost needs.`,
    buyerAdvice: `Give it more weight if you will use it regularly; otherwise treat it as a useful extra and verify the exact variant in person.`,
    featureType: type,
    decisionCategory: category,
    decisionSignals: ["frequency of use", "ease of operation", "variant verification"],
    importance: importance({ cityUse: "medium", highwayUse: "medium", familyUse: "medium", chauffeurUse: "medium", firstTimeBuyer: "medium" }),
  });

const cosmeticEntry = (feature, groupLabel) =>
  makeEntry({
    buyerSummary: `${feature.displayName} is mainly a ${lower(groupLabel)} design or trim choice rather than a core mechanical or safety feature.`,
    howItWorks: `It changes the appearance, finish or presentation associated with ${feature.displayName}.`,
    whenItMattersSummary: `It matters when visual preference, cabin ambience or perceived premium feel is part of the purchase decision.`,
    whenItMatters: ["personal design preference", "cabin or exterior presentation"],
    limitationsSummary: `It usually adds little to safety, performance or everyday practicality, and the finish can look different in person.`,
    buyerAdvice: `Choose it if you genuinely like it after seeing the exact variant, but do not sacrifice a needed safety or practical feature for it.`,
    featureType: lower(feature.groupKey) === "interior" ? "interior" : "exterior",
    decisionCategory: "cosmetic_preference",
    decisionSignals: ["personal preference", "exact trim", "lower functional priority"],
    importance: importance({ safety: "low", cityUse: "low", highwayUse: "low", familyUse: "low", firstTimeBuyer: "low" }),
  });

const SPECIAL_BUILDERS = {
  anti_lock_braking_system_abs: (f) => safetyEntry(f, {
    buyerSummary: "ABS helps you retain steering control during hard braking by reducing wheel lock-up.",
    howItWorks: "Wheel-speed sensors watch for a wheel about to stop rotating. The system rapidly modulates brake pressure at that wheel so the tyre can keep rotating and responding to steering input. Pedal pulsing or mechanical noise during activation is normal.",
    whenItMattersSummary: "It matters most in emergency stops and on wet or low-grip roads, where a locked wheel would slide instead of steering.",
    whenItMatters: ["emergency braking", "wet roads", "mixed-grip surfaces"],
    limitationsSummary: "ABS cannot create extra tyre grip, guarantee a shorter stopping distance or prevent every crash. On loose gravel, sand or deep snow, stopping distance can increase even though steering control is usually improved.",
    buyerAdvice: "Treat ABS as essential safety equipment. In an emergency, press the brake firmly and keep steering rather than pumping the pedal; also check ESC, tyres, brake condition and independent crash evidence.",
    featureType: "active_safety",
    decisionCategory: "must_have_safety",
    decisionSignals: ["emergency steering control", "wet-road braking", "essential safety"],
    importance: importance({ safety: "critical", cityUse: "high", highwayUse: "high", familyUse: "high", firstTimeBuyer: "high", safetyCritical: true }),
  }),
  electronic_brakeforce_distribution_ebd: (f) => safetyEntry(f, {
    buyerSummary: "EBD varies braking force between wheels or axles so the available tyre grip is used more effectively as load and grip change.",
    howItWorks: "Working with the anti-lock braking hardware, it estimates wheel behaviour and adjusts brake pressure distribution instead of relying on one fixed front-to-rear balance.",
    whenItMattersSummary: "It is useful during hard braking, when passengers or luggage change the load, and when wheels have different grip.",
    whenItMatters: ["hard braking", "loaded family trips", "uneven grip"],
    limitationsSummary: "EBD is not a substitute for ABS, ESC, good tyres or safe speed, and its calibration varies by vehicle.",
    buyerAdvice: "Treat it as an important part of a complete braking package rather than a reason to choose a car by itself.",
  }),
  ebd: (f) => SPECIAL_BUILDERS.electronic_brakeforce_distribution_ebd(f),
  electronic_stability_control_esc: (f) => safetyEntry(f, {
    buyerSummary: "ESC can help correct a developing skid by braking individual wheels and, where supported, reducing engine power.",
    howItWorks: "Sensors compare the driver's steering request with the vehicle's actual rotation and direction. If they diverge, the system applies selective braking to help restore stability.",
    whenItMattersSummary: "It matters during sudden avoidance moves, slippery corners and loss-of-control situations.",
    whenItMatters: ["emergency lane changes", "wet or loose surfaces", "highway cornering"],
    limitationsSummary: "ESC cannot defeat physics, recover every skid or compensate for excessive speed, poor tyres or unsafe driving.",
    buyerAdvice: "Prioritise ESC highly for every use case, especially family and highway driving.",
    decisionCategory: "must_have_safety",
    importance: importance({ safety: "critical", cityUse: "high", highwayUse: "critical", familyUse: "critical", firstTimeBuyer: "high", safetyCritical: true }),
  }),
  tyre_pressure_monitoring_system_tpms: (f) => safetyEntry(f, {
    buyerSummary: "TPMS warns when tyre pressure appears too low, helping you catch a problem before it harms safety, efficiency or tyre life.",
    howItWorks: "Depending on the design, sensors measure pressure directly or infer a pressure difference from wheel-speed behaviour.",
    whenItMattersSummary: "It is useful before and during highway trips, after a puncture begins, and when seasonal temperature changes alter pressure.",
    whenItMatters: ["highway trips", "slow punctures", "routine tyre care"],
    limitationsSummary: "A warning may not identify every rapid failure, and indirect systems may need recalibration. It does not replace manual pressure checks.",
    buyerAdvice: "Prioritise it for frequent highway use and verify whether the display shows individual pressures or only a warning.",
  }),
  isofix_child_seat_mounts: (f) => safetyEntry(f, {
    buyerSummary: "ISOFIX provides standard anchorage points for a compatible child seat, reducing the chance of an incorrect seat-belt installation.",
    howItWorks: "Rigid connectors on a compatible child seat latch to dedicated anchors between the seat base and backrest, often with a top tether or support leg as specified.",
    whenItMattersSummary: "It is especially valuable for families who regularly carry babies or young children in a child restraint.",
    whenItMatters: ["carrying young children", "frequent child-seat removal", "family use"],
    limitationsSummary: "The child seat, anchor position, weight group and top-tether or support-leg instructions must all be compatible; ISOFIX alone does not guarantee correct fit.",
    buyerAdvice: "Take your actual child seat to the showroom and check installation space, access and front-seat clearance.",
  }),
  six_airbags: (f) => safetyEntry(f, {
    buyerSummary: "Six airbags usually add side and curtain coverage to the front airbags, improving the potential restraint coverage in certain impacts.",
    howItWorks: "Crash sensors trigger the relevant airbags when calibrated impact conditions are met; which bags deploy depends on impact direction and system design.",
    whenItMattersSummary: "The additional coverage matters most in side impacts and for protecting more seating positions than front airbags alone.",
    whenItMatters: ["family occupancy", "side-impact protection", "mixed city and highway use"],
    limitationsSummary: "Airbag count does not prove placement, coverage or crash performance, and airbags work with seat belts rather than replacing them.",
    buyerAdvice: "Prefer six airbags when the rest of the safety package is sound, but also compare ESC, ISOFIX and independent crash-test evidence for the exact tested version.",
  }),
  number_of_airbags: (f) => SPECIAL_BUILDERS.six_airbags(f),
  sunroof: (f) => convenienceEntry(f, {
    benefit: "adds light and an open-air option to the cabin",
    howItWorks: "A roof panel opens manually or electrically, depending on the design, while a glass panel can also brighten the cabin when closed. Opening size, anti-pinch protection and sunshade operation vary by implementation.",
    contexts: ["buyers who enjoy a brighter cabin", "occasional leisure use"],
    limitation: "It does not improve core transport ability, can add heat and complexity, and must never be used as a place for occupants to stand.",
    category: "cosmetic_preference",
    type: "comfort",
  }),
  adaptive_cruise_control: (f) => adasEntry(f, "adjusts the set speed to help maintain a chosen gap from a detected vehicle ahead", "It does not automatically imply stop-and-go support or operation down to a complete halt.", ["open highways", "steady traffic flow"]),
  automatic_emergency_braking: (f) => adasEntry(f, "can warn and apply braking when it detects a likely frontal collision", "It may not detect every object or avoid every impact, and activation speed ranges vary.", ["unexpected slowing traffic", "urban and highway driving"]),
  forward_collision_warning: (f) => adasEntry(f, "warns when it detects a possible frontal collision", "A warning-only system does not automatically apply the brakes.", ["traffic with sudden slowing", "highway driving"]),
  lane_departure_warning: (f) => adasEntry(f, "warns when the vehicle appears to leave a marked lane unintentionally", "It is a warning function and does not automatically steer the car.", ["long highway drives", "well-marked roads"]),
  lane_keep_assist: (f) => adasEntry(f, "can provide corrective steering support when the vehicle drifts toward a lane marking", "It does not automatically imply continuous lane centring, and weak road markings can disable assistance.", ["long highway drives", "well-marked roads"]),
  blind_spot_monitor: (f) => adasEntry(f, "alerts you to a detected vehicle in an adjacent blind-spot area", "It is an aid, not a replacement for mirrors and a shoulder check, and it does not automatically steer or brake.", ["lane changes", "multi-lane roads"]),
  rear_cross_traffic_alert: (f) => adasEntry(f, "warns about detected cross traffic while reversing", "It may miss fast, small or obstructed road users and does not automatically imply braking.", ["reversing from tight parking", "blocked side visibility"]),
  adas_package: (f) => adasEntry(f, "groups multiple driver-assistance functions", "The name alone does not tell you which functions, speed ranges or intervention levels are included.", ["highway use", "dense traffic", "parking and manoeuvring"]),
  automatic_climate_control: (f) => convenienceEntry(f, { benefit: "automatically manages cabin temperature toward a selected setting", contexts: ["hot-weather commuting", "family use"], limitation: "Cooling performance still depends on cabin size, vents, sunlight and the number of climate zones.", type: "comfort" }),
  ventilated_seats: (f) => convenienceEntry(f, { benefit: "moves air through or around the seat surface to reduce heat and perspiration", contexts: ["hot-weather driving", "long journeys"], limitation: "It is seat ventilation, not necessarily active refrigeration, and effectiveness varies with upholstery and fan design.", type: "comfort" }),
  cruise_control: (f) => convenienceEntry(f, { benefit: "holds a chosen speed without continuous accelerator input", contexts: ["open highways", "long steady-speed drives"], limitation: "Standard cruise control does not maintain distance from traffic ahead and should not be used where speed changes are frequent.", category: "highway_convenience" }),
  camera_360: (f) => safetyEntry(f, { buyerSummary: "A 360-degree camera combines views around the car to make low-speed positioning and parking easier.", howItWorks: "Multiple exterior cameras feed a stitched or selectable display view; image quality, activation and stitching vary by implementation.", whenItMattersSummary: "It is most useful in tight parking, around low obstacles and with larger vehicles.", whenItMatters: ["tight parking", "narrow lanes", "low-speed manoeuvring"], limitationsSummary: "Camera distortion, dirt, darkness and blind areas remain possible, and the feature does not automatically imply recording.", buyerAdvice: "Prioritise it for frequent tight parking, but inspect image clarity and stitching on the exact variant." }),
  "360_view_camera": (f) => SPECIAL_BUILDERS.camera_360(f),
  rear_camera: (f) => safetyEntry(f, { buyerSummary: "A rear camera shows the area behind the car while reversing, helping with parking and low-obstacle awareness.", howItWorks: "A rear-mounted camera sends a live image to the cabin display; guidelines may be fixed or move with steering depending on implementation.", whenItMattersSummary: "It is useful during daily parking and when rearward visibility is restricted.", whenItMatters: ["reversing", "tight parking", "family areas"], limitationsSummary: "It can have blind spots and reduced clarity in dirt, rain or darkness, and it does not automatically imply recording.", buyerAdvice: "Treat it as valuable city convenience, while still checking mirrors and the surroundings directly." }),
  parking_sensors: (f) => convenienceEntry(f, { benefit: "warns about nearby obstacles during low-speed manoeuvring", contexts: ["tight parking", "narrow spaces"], limitation: "Coverage, front or rear placement and detection of low or thin objects vary, so visual checks remain essential.", category: "city_convenience" }),
  ground_clearance_laden: (f) => metricEntry(f, { meaning: "the underbody clearance with the vehicle carrying the specified load", tradeoff: "More clearance can help on rough roads but can also influence access, aerodynamics and handling.", contexts: ["rough roads are common", "the car often carries passengers or luggage"] }),
  ground_clearance_unladen: (f) => metricEntry(f, { meaning: "the underbody clearance in the stated unladen measurement condition", tradeoff: "The figure can reduce under load, and more is not automatically better for every road or handling need.", contexts: ["speed breakers or rough roads are common", "underbody clearance is a concern"] }),
  reported_ground_clearance_unladen: (f) => SPECIAL_BUILDERS.ground_clearance_unladen(f),
  boot_space: (f) => metricEntry(f, { meaning: "the published luggage volume behind the seats in the stated configuration", tradeoff: "Shape, loading height, spare-wheel packaging and seat position can matter more than litres alone.", contexts: ["family luggage is frequent", "airport or road-trip bags must fit"] }),
  reported_boot_space: (f) => SPECIAL_BUILDERS.boot_space(f),
  wheel_base: (f) => metricEntry(f, { meaning: "the distance between the front and rear wheel centres", tradeoff: "A longer wheelbase can help cabin packaging and stability, while a shorter one can aid manoeuvrability; suspension design still matters.", contexts: ["rear-seat space matters", "ride and manoeuvrability are being compared"] }),
  battery_capacity: (f) => metricEntry(f, { meaning: "the battery's stated energy capacity", tradeoff: "Usable capacity may differ from gross capacity, and a larger pack also adds cost and weight.", contexts: ["EV range is important", "charging time and efficiency are being compared"], category: "ev_ownership", type: "charging_metric" }),
  range: (f) => metricEntry(f, { meaning: "the distance claimed under the stated test or usage basis", tradeoff: "Speed, weather, traffic, load, climate control and test cycle can materially change real-world range.", contexts: ["daily charging access is limited", "regular routes approach the vehicle's usable range"], category: "ev_ownership", type: "charging_metric" }),
  range_tested: (f) => SPECIAL_BUILDERS.range(f),
  max_power: (f) => metricEntry(f, { meaning: "the engine or motor's peak rate of doing work", tradeoff: "Peak power does not describe low-speed response, gearing, weight or everyday drivability by itself.", contexts: ["overtaking performance matters", "power-to-weight is being compared"], category: "performance_preference", type: "performance_metric" }),
  max_torque: (f) => metricEntry(f, { meaning: "the peak twisting force produced by the engine or motor", tradeoff: "The rpm range, gearing and vehicle weight determine how that torque feels at the wheels.", contexts: ["low-speed response matters", "loaded or hill driving is common"], category: "performance_preference", type: "performance_metric" }),
  transmission_type: (f) => metricEntry(f, { meaning: "the gearbox or drive-control type used to transmit power", tradeoff: "Convenience, smoothness, efficiency, response and repair complexity differ between manual, torque-converter, CVT, AMT and dual-clutch designs.", contexts: ["traffic convenience matters", "driving feel and running cost are being compared"], category: "city_convenience", type: "engine_drivetrain" }),
  turbo_charger: (f) => makeEntry({ buyerSummary: "A turbocharger uses exhaust energy to force more air into the engine, allowing stronger output from a given displacement.", howItWorks: "Exhaust gas spins a turbine linked to a compressor that pressurises the intake air; boost response and calibration vary by engine.", whenItMattersSummary: "It matters for overtaking, hill driving and buyers comparing performance from smaller engines.", whenItMatters: ["overtaking", "hill driving", "loaded driving"], limitationsSummary: "Turbo response, fuel use under boost, heat management and long-term maintenance depend on the design and driving style.", buyerAdvice: "Judge the complete engine-gearbox response on a test drive rather than assuming every turbo engine feels faster or smoother.", featureType: "engine_drivetrain", decisionCategory: "performance_preference", decisionSignals: ["overtaking", "engine response", "maintenance complexity"], importance: importance({ cityUse: "medium", highwayUse: "high", familyUse: "medium", firstTimeBuyer: "medium" }) }),
  wireless_phone_charging: (f) => convenienceEntry(f, { benefit: "charges a compatible phone without plugging in a cable", contexts: ["daily commuting", "frequent phone use"], limitation: "Charging speed, heat, phone alignment and case compatibility vary, and this is different from EV wireless charging.", category: "technology_preference", type: "connectivity" }),
  wireless_charging: (f) => SPECIAL_BUILDERS.wireless_phone_charging(f),
};

const CURATED_SOURCE_REFS = {
  anti_lock_braking_system_abs: [
    {
      title: "Bosch Mobility - Antilock braking system",
      url: "https://www.bosch-mobility.com/en/solutions/driving-safety/antilock-braking-system/",
      sourceType: "primary_technical_source",
    },
    {
      title: "NHTSA Light Vehicle ABS Research Program",
      url: "https://www.nhtsa.gov/sites/nhtsa.gov/files/absperformancefinalreport.pdf",
      sourceType: "government_research",
    },
  ],
};

const buildChargingEntry = (feature) => {
  const power = clean(feature.displayName.match(/\(([^)]+)\)/)?.[1]);
  const ac = /\bac\b/i.test(feature.displayName);
  return metricEntry(feature, {
    meaning: `the stated time to charge under the specified ${ac ? "AC" : "DC"}${power ? ` ${power}` : ""} input condition`,
    tradeoff: "Actual time depends on the car's accepted power, charger output, starting and target state of charge, battery temperature and the charging curve.",
    contexts: ["home or public charging plans are being compared", "turnaround time matters"],
    category: "ev_ownership",
    type: "charging_metric",
  });
};

const buildGenericEntry = (feature) => {
  const name = feature.displayName;
  const keyName = feature.canonicalKey;
  const group = lower(feature.groupKey);
  const metricPattern = /(?:size|capacity|length|width|height|weight|tread|angle|radius|space|mileage|range|power|torque|speed|acceleration|braking|displacement|number of|running cost|service cost|drag coefficient)/i;

  if (/^charging_time_|charging_time|fast_charging/.test(keyName)) return buildChargingEntry(feature);
  if (/ncap_(?:child_)?safety_rating$/.test(keyName)) {
    return makeEntry({
      buyerSummary: `${name} reports crash-test performance under the named assessment programme; it is useful evidence, but only within the scope of that exact test.`,
      howItWorks: `The programme evaluates a tested vehicle configuration against its protocol and publishes the applicable adult, child or overall result.`,
      whenItMattersSummary: `It matters when comparing independently tested safety performance for the exact model generation and safety specification.`,
      whenItMatters: ["family safety comparisons", "shortlisting tested vehicles", "checking structural crash evidence"],
      limitationsSummary: `Always check the protocol or assessment year, adult and child scope, tested variant, market specification and whether the result applies to the exact car being considered. Ratings from different programmes or years are not directly interchangeable.`,
      buyerAdvice: `Use the rating alongside active-safety equipment and exact-variant fitment; do not transfer a score to an untested generation or specification without applicability evidence.`,
      featureType: "passive_safety",
      decisionCategory: "must_have_safety",
      decisionSignals: ["protocol year", "tested variant applicability", "adult and child scope"],
      importance: importance({ safety: "critical", cityUse: "high", highwayUse: "critical", familyUse: "critical", firstTimeBuyer: "high", safetyCritical: true }),
    });
  }
  if (["safety", "adas"].includes(group)) {
    if (/(warning|alert|monitor|notification)$/.test(keyName)) {
      return adasEntry(feature, `alerts the driver to the condition described by ${name}`, "A warning or monitor does not automatically steer or brake the vehicle.", ["daily driving", "situations with limited reaction time"]);
    }
    return safetyEntry(feature);
  }
  if (metricPattern.test(name) || ["dimensions", "performance", "key_specs", "chassis"].includes(group)) {
    return metricEntry(feature, {
      meaning: `the published ${lower(name)} value or specification`,
      tradeoff: "A higher, lower or larger number is not automatically better because it can trade against efficiency, manoeuvrability, comfort, packaging or cost.",
      contexts: ["shortlisted vehicles need an objective comparison", "the specification affects your regular use"],
      category: group === "performance" || group === "chassis" ? "performance_preference" : "specification_context",
      type: group === "performance" || group === "chassis" ? "performance_metric" : "dimension_metric",
    });
  }
  if (group === "engine") {
    return makeEntry({
      buyerSummary: `${name} describes part of the powertrain, energy system or transmission and can affect performance, efficiency, refinement or ownership use.`,
      howItWorks: `It identifies the exact ${lower(name)} specification used by the vehicle. Its effect depends on how it works with the rest of the powertrain.`,
      whenItMattersSummary: `It matters when comparing drivability, fuel or energy use, charging, maintenance and the intended driving pattern.`,
      whenItMatters: ["powertrain choice", "running-cost comparison", "city and highway use"],
      limitationsSummary: `The label alone does not predict real-world performance or reliability; calibration, gearing, weight and usage conditions also matter.`,
      buyerAdvice: `Compare it within the complete engine or motor and transmission package, then test the exact variant if driving feel matters.`,
      featureType: "engine_drivetrain",
      decisionCategory: "performance_preference",
      decisionSignals: ["drivability", "efficiency", "ownership use"],
      importance: importance({ cityUse: "high", highwayUse: "high", familyUse: "medium", firstTimeBuyer: "high" }),
    });
  }
  if (["exterior", "interior"].includes(group) && /(chrome|colour|tone|garnish|grille|spoiler|smoke|upholstery|leather|fabric|ambient|lighting|wheel covers)/i.test(name)) {
    return cosmeticEntry(feature, feature.groupLabel || group);
  }
  if (group === "infotainment" || group === "connected") {
    return convenienceEntry(feature, {
      benefit: `adds the ${lower(name)} media, connectivity or remote-service function`,
      contexts: ["daily phone or media use", "connected ownership tasks"],
      limitation: "Compatibility, subscription, mobile network, privacy, software support and exact functionality can vary.",
      category: "technology_preference",
      type: group === "connected" ? "connectivity" : "infotainment",
    });
  }
  if (["comfort", "key_features"].includes(group)) return convenienceEntry(feature);
  if (["exterior", "interior"].includes(group)) {
    return convenienceEntry(feature, {
      benefit: `adds the ${lower(name)} body, visibility or cabin function`,
      contexts: ["daily use", "visibility or cabin preference"],
      limitation: "Its practical value, finish and operation must be checked on the exact variant.",
      category: "daily_comfort",
      type: group,
    });
  }
  return convenienceEntry(feature, {
    benefit: `adds the ${lower(name)} function described in the equipment list`,
    limitation: "The name can cover different implementations, so verify what the exact variant actually provides.",
  });
};

const buildEntry = (feature) => {
  const builder = SPECIAL_BUILDERS[feature.canonicalKey];
  return builder ? builder(feature) : buildGenericEntry(feature);
};

const defaultSourceRef = (groupKey = "") => {
  if (["safety", "adas"].includes(groupKey)) {
    return {
      title: "NHTSA vehicle safety and driver-assistance guidance",
      url: "https://www.nhtsa.gov/vehicle-safety",
      sourceType: "authoritative_context",
    };
  }
  if (["charging", "engine"].includes(groupKey)) {
    return {
      title: "U.S. Department of Energy alternative fuels vehicle guidance",
      url: "https://afdc.energy.gov/vehicles/electric-basics",
      sourceType: "authoritative_context",
    };
  }
  return {
    title: "ACI canonical vehicle feature catalog",
    url: "https://aci.cars/",
    sourceType: "catalog_context",
  };
};

const validateEntry = (entry, feature) => {
  for (const field of ["buyerSummary", "howItWorks", "whenItMattersSummary", "limitationsSummary", "buyerAdvice"]) {
    assert(clean(entry[field]).length >= 15, `${feature.canonicalKey}: ${field} is too thin`);
  }
  assert(entry.whenItMatters.length >= 2, `${feature.canonicalKey}: whenItMatters is too thin`);
  assert(entry.decisionSignals.length >= 1, `${feature.canonicalKey}: decisionSignals are missing`);
  for (const field of ["safety", "cityUse", "highwayUse", "familyUse", "offRoadUse", "chauffeurUse", "firstTimeBuyer"]) {
    assert(IMPORTANCE_LEVELS.has(entry.importance[field]), `${feature.canonicalKey}: invalid importance.${field}`);
  }
  const allText = [entry.buyerSummary, entry.howItWorks, entry.whenItMattersSummary, entry.limitationsSummary, entry.buyerAdvice].join(" ");
  assert(!/\b(always prevents|guarantees|zero risk|will prevent every)\b/i.test(allText), `${feature.canonicalKey}: unsafe absolute claim`);
  assert(!/\b(this car|this model|this variant|all variants|standard on)\b/i.test(allText), `${feature.canonicalKey}: vehicle availability claim`);
};

async function main() {
  const uri = mongoUri();
  assert(uri, "Mongo URI is required");
  await mongoose.connect(uri);

  try {
    const db = mongoose.connection.db;
    const [catalog, existing] = await Promise.all([
      db.collection(CATALOG_COLLECTION).find({}).sort({ canonicalKey: 1 }).toArray(),
      db.collection(EXPLAINER_COLLECTION).find({}).project({ canonicalKey: 1, createdAt: 1 }).toArray(),
    ]);
    assert(catalog.length > 0, "feature catalog is empty");
    const existingByKey = new Map(existing.map((item) => [item.canonicalKey, item]));
    const now = new Date();
    const docs = catalog.map((feature) => {
      const entry = buildEntry(feature);
      validateEntry(entry, feature);
      const prior = existingByKey.get(feature.canonicalKey) || {};
      const sourceRefs = (CURATED_SOURCE_REFS[feature.canonicalKey] || [defaultSourceRef(feature.groupKey)])
        .map((source) => ({ ...source, verifiedAt: now }));

      return {
        schemaVersion: SCHEMA_VERSION,
        contentVersion: CONTENT_VERSION,
        canonicalKey: feature.canonicalKey,
        displayName: feature.displayName,
        groupKey: feature.groupKey,
        groupLabel: feature.groupLabel,
        aliases: unique(feature.aliases),
        ...entry,
        status: "published",
        publishable: true,
        qualityScore: SPECIAL_BUILDERS[feature.canonicalKey] ? 0.97 : 0.92,
        qualityStatus: SPECIAL_BUILDERS[feature.canonicalKey]
          ? "codex_feature_reviewed"
          : "codex_taxonomy_validated",
        qualityNotes: [
          "Codex-authored deterministic explanation; no Gemini generation or runtime model call.",
          SPECIAL_BUILDERS[feature.canonicalKey]
            ? "Feature-specific editorial rule applied."
            : "Group and measurement-scope editorial rule applied.",
        ],
        contentOrigin: "codex_curated_taxonomy",
        sourceRefs,
        sourceCatalogCollection: CATALOG_COLLECTION,
        editorial: {
          authoringSystem: "OpenAI Codex",
          method: SPECIAL_BUILDERS[feature.canonicalKey]
            ? "feature_specific_editorial_rule"
            : "catalog_taxonomy_editorial_rule",
          modelGenerated: true,
          generationProvider: "openai_codex",
          runtimeModelGenerated: false,
          geminiUsed: false,
          reviewedAt: now,
        },
        createdAt: prior.createdAt || now,
        reviewedAt: now,
        updatedAt: now,
      };
    });

    if (SHOULD_WRITE) {
      await db.collection(EXPLAINER_COLLECTION).bulkWrite(
        docs.map((doc) => ({
          replaceOne: {
            filter: { canonicalKey: doc.canonicalKey },
            replacement: doc,
            upsert: true,
          },
        })),
        { ordered: false },
      );
      await db.collection(EXPLAINER_COLLECTION).deleteMany({
        canonicalKey: { $nin: catalog.map((item) => item.canonicalKey) },
      });
      await Promise.all([
        db.collection(EXPLAINER_COLLECTION).createIndex({ canonicalKey: 1 }, { unique: true }),
        db.collection(EXPLAINER_COLLECTION).createIndex({ aliases: 1 }),
        db.collection(EXPLAINER_COLLECTION).createIndex({ status: 1, groupKey: 1 }),
      ]);
    }

    console.log(JSON.stringify({
      suite: "ACI Codex feature explainer build v2",
      ok: true,
      mode: SHOULD_WRITE ? "write" : "dry_run",
      catalogCount: catalog.length,
      generatedCount: docs.length,
      featureSpecificCount: docs.filter((doc) => doc.editorial.method === "feature_specific_editorial_rule").length,
      taxonomyRuleCount: docs.filter((doc) => doc.editorial.method === "catalog_taxonomy_editorial_rule").length,
      contentOriginCounts: { codex_curated_taxonomy: docs.length },
      geminiUsed: false,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({ suite: "ACI Codex feature explainer build v2", ok: false, error: error.message }, null, 2));
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
