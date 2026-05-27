const ACI_PROGRESS_MODULES = [
  {
    id: "intelligence-core",
    title: "ACI Intelligence Core",
    group: "Backend Brain",
    priority: "P0",
    owner: "Backend",
    status: "partial",
    summary: "Permanent meaning-frame brain that turns customer language into validated DB-backed tool plans.",
    whatWillWork: "Candidate retrieval, deterministic parser, Gemini fallback, module registry, context graph, tool execution, answer composer, trace logs, eval compatibility.",
    currentState: "Hybrid meaning-frame router, deterministic parser, optional Gemini fallback, DB-backed candidate retrieval and prewarm are implemented as a checkpoint. Old and new paths still need careful consolidation.",
    pending: "Finalize meaning-frame schema, complete module/capability registry, integrate into live chat path, replace fragile old early-gate behavior, add complete trace/audit contract.",
    items: [
      { key: "candidate_retrieval", name: "DB-backed candidate retrieval", status: "ready" },
      { key: "deterministic_parser", name: "Deterministic parser for clear queries", status: "ready" },
      { key: "gemini_fallback", name: "Gemini fallback parser", status: "partial" },
      { key: "hybrid_router", name: "Hybrid router", status: "mostly_ready" },
      { key: "meaning_frame_contract", name: "Stable meaning-frame contract", status: "partial" },
      { key: "capability_registry", name: "Capability/module registry", status: "planned" },
      { key: "answer_composer", name: "Answer composer", status: "pending" },
      { key: "trace_contract", name: "Trace/debug contract", status: "partial" }
    ]
  },
  {
    id: "chat-concierge",
    title: "Chat-First AI Concierge",
    group: "User Experience",
    priority: "P0",
    owner: "Frontend + Backend",
    status: "partial",
    summary: "Main customer experience where users ask natural questions and receive answers plus embedded canvases/cards.",
    whatWillWork: "Natural-language chat, context persistence, follow-ups, explicit car switching, multi-intent cards, clarification only when needed, safe response streaming.",
    currentState: "Chat-first direction is locked. Context handling has improved significantly. Final live integration and answer quality still need work.",
    pending: "Wire new ACI Core into the live chat path, improve premium answer language, add streaming status, and standardize embedded canvas behavior.",
    items: [
      { key: "chat_first_direction", name: "Chat-first direction", status: "ready" },
      { key: "context_persistence", name: "Context persistence", status: "mostly_ready" },
      { key: "explicit_context_switching", name: "Explicit context switching", status: "mostly_ready" },
      { key: "multi_intent_responses", name: "Multi-intent responses", status: "partial" },
      { key: "safe_streaming", name: "Safe first response streaming", status: "planned" },
      { key: "clarification_policy", name: "Clarifying question policy", status: "partial" },
      { key: "answer_variations", name: "Premium answer variations", status: "partial" }
    ]
  },
  {
    id: "vehicle-discovery",
    title: "Vehicle Discovery",
    group: "Core Car Research",
    priority: "P0",
    owner: "Backend + Frontend",
    status: "partial",
    summary: "Broad discovery for cars by budget, body type, brand, fuel, transmission and features.",
    whatWillWork: "Cars under budget, Hyundai cars, automatic SUVs under 20L, CNG cars with ABS, cars with sunroof/ADAS/airbags, ranked recommendations.",
    currentState: "Broad discovery is supported in the new understanding path, but ranking and final canvas output need strengthening.",
    pending: "Formal discovery tool, ranking engine, result explanation, shortlist support, and polished recommendation canvas.",
    items: [
      { key: "budget_discovery", name: "Budget discovery", status: "partial" },
      { key: "brand_discovery", name: "Brand discovery", status: "partial" },
      { key: "body_type_discovery", name: "Body-type discovery", status: "partial" },
      { key: "fuel_transmission_filters", name: "Fuel/transmission filters", status: "partial" },
      { key: "feature_discovery", name: "Feature-based discovery", status: "mostly_ready" },
      { key: "messy_queries", name: "No-comma messy queries", status: "mostly_ready" },
      { key: "ranking_logic", name: "Ranking and why-this-car logic", status: "pending" }
    ]
  },
  {
    id: "pricing",
    title: "Price List, Ex-showroom & On-road Price",
    group: "Commerce Core",
    priority: "P0",
    owner: "Backend + Frontend",
    status: "mostly_ready",
    summary: "DB-backed price answers, price list table, city context, price breakups and quote conversion.",
    whatWillWork: "Price list, ex-showroom, on-road, variant price cards, price comparison, city selection, quote CTA.",
    currentState: "Vehicle price read models exist and price list screen is functional. On-road and quote-grade accuracy still need stronger validation.",
    pending: "Finalize on-road calculations, price breakup, price answer cards, city expansion, and quote conversion from price intent.",
    items: [
      { key: "price_read_model", name: "Ex-showroom price read model", status: "ready" },
      { key: "price_list_screen", name: "Price list screen", status: "mostly_ready" },
      { key: "price_filters", name: "Fuel/transmission/budget filters", status: "partial" },
      { key: "city_selector", name: "City selector", status: "partial" },
      { key: "on_road_price", name: "On-road price", status: "partial" },
      { key: "price_breakup", name: "Price breakup", status: "partial" },
      { key: "quote_conversion", name: "Quote conversion", status: "planned" },
      { key: "historical_pricing", name: "Historical discontinued pricing", status: "planned" }
    ]
  },
  {
    id: "emi-finance",
    title: "EMI & Finance",
    group: "Commerce Core",
    priority: "P0",
    owner: "Backend + Frontend",
    status: "partial",
    summary: "EMI estimates using selected car context, down payment, tenure and interest assumptions.",
    whatWillWork: "EMI for selected variant, down payment parsing, tenure/rate assumptions, loan amount, total payable, EMI comparison, finance lead capture.",
    currentState: "Context-aware EMI has improved. Full finance workflow is not ready.",
    pending: "Finalize assumptions, disclaimer language, finance canvas, affordability advisor, finance lead capture and partner workflow.",
    items: [
      { key: "basic_emi", name: "Basic EMI calculation", status: "partial" },
      { key: "context_emi", name: "Context-aware EMI", status: "mostly_ready" },
      { key: "down_payment_parsing", name: "Down payment parsing", status: "partial" },
      { key: "tenure_rate_handling", name: "Tenure/rate handling", status: "partial" },
      { key: "finance_lead_capture", name: "Finance lead capture", status: "planned" },
      { key: "bank_partner_offers", name: "Bank/NBFC partner offers", status: "pending" },
      { key: "affordability_advisor", name: "Affordability advisor", status: "pending" }
    ]
  },
  {
    id: "feature-answers",
    title: "Feature Answers & Feature Explorer",
    group: "Core Car Research",
    priority: "P0",
    owner: "Backend + Frontend",
    status: "mostly_ready",
    summary: "DB-backed single and multi-feature answers with variant-level feature matrix support.",
    whatWillWork: "Does X have sunroof, which variants have ADAS, CNG with ABS, multi-feature answers, feature explorer, feature discovery.",
    currentState: "Single-feature, multi-feature, variant-scoped multi-feature and two-car feature comparison are strongly improved.",
    pending: "Improve feature explanation, missing-data messaging, UI cards, canonical taxonomy quality and feature importance advice.",
    items: [
      { key: "single_feature_answer", name: "Single-feature answer", status: "ready" },
      { key: "same_car_multi_feature", name: "Same-car multi-feature answer", status: "ready" },
      { key: "variant_multi_feature", name: "Variant-scoped multi-feature answer", status: "ready" },
      { key: "two_car_feature_comparison", name: "Two-car feature comparison", status: "ready" },
      { key: "db_feature_catalog", name: "DB-backed feature catalog", status: "mostly_ready" },
      { key: "feature_explorer_ui", name: "Feature explorer UI", status: "mostly_ready" },
      { key: "feature_explanation", name: "Feature explanation/advice", status: "pending" }
    ]
  },
  {
    id: "comparison",
    title: "Model & Variant Comparison",
    group: "Core Car Research",
    priority: "P0",
    owner: "Backend + Frontend",
    status: "partial",
    summary: "Model-vs-model and variant-vs-variant comparison across price, features, safety, EMI and value.",
    whatWillWork: "Verna vs City, Verna SX IVT vs City ZX CVT, Creta S(O) IVT vs Seltos HTX IVT, feature-specific comparisons.",
    currentState: "Feature-specific comparison is better now. Full value comparison and exact variant-vs-variant comparison still need work.",
    pending: "Formal comparison tool, variant anchor separation, value conclusion, safety/EMI comparison and polished comparison canvas.",
    items: [
      { key: "model_comparison", name: "Model-vs-model comparison", status: "partial" },
      { key: "variant_comparison", name: "Variant-vs-variant comparison", status: "partial" },
      { key: "feature_specific_comparison", name: "Feature-specific comparison", status: "mostly_ready" },
      { key: "price_comparison", name: "Price comparison", status: "partial" },
      { key: "emi_comparison", name: "EMI comparison", status: "planned" },
      { key: "value_conclusion", name: "Value conclusion", status: "pending" }
    ]
  },
  {
    id: "colors-images",
    title: "Colors, Color Studio & Vehicle Images",
    group: "Visual Experience",
    priority: "P0",
    owner: "Frontend + Data",
    status: "partial",
    summary: "Premium color browsing with accurate swatches, normalized images, staged frames and smooth transitions.",
    whatWillWork: "Show colors, color studio, swatches, car image by color, image framing, inline color answers, quote for selected color.",
    currentState: "Color Studio is functional and image pipeline exists. Coverage, animation, frame usage and CDN delivery need final polishing.",
    pending: "Use frame metadata everywhere, prevent image jumping, complete image coverage, finalize R2/CDN delivery and inline color answer card.",
    items: [
      { key: "color_list", name: "Color list", status: "mostly_ready" },
      { key: "color_studio", name: "Color Studio", status: "mostly_ready" },
      { key: "swatches", name: "Swatches/hex handling", status: "partial" },
      { key: "car_image_per_color", name: "Car image per color", status: "partial" },
      { key: "frame_metadata", name: "Frame metadata", status: "partial" },
      { key: "inline_color_answer", name: "Inline color answer", status: "partial" },
      { key: "cdn_r2_delivery", name: "CDN/R2 delivery", status: "planned" }
    ]
  },
  {
    id: "recommendations",
    title: "Recommendations & Car Advisor",
    group: "Advisory Layer",
    priority: "P0",
    owner: "Backend + Frontend",
    status: "partial",
    summary: "Need-based and budget-based car recommendations with explainable ranking.",
    whatWillWork: "Best family car, automatic SUV under 20L, safest car, best value variant, cheapest car with required features.",
    currentState: "Basic recommendation/discovery exists, but advisor-grade scoring is not ready.",
    pending: "Build scoring engine, explainability, value-for-money logic, use-case profiles, safety weighting and advisor canvas.",
    items: [
      { key: "budget_recommendations", name: "Budget recommendations", status: "partial" },
      { key: "feature_recommendations", name: "Feature-based recommendations", status: "partial" },
      { key: "need_recommendations", name: "Need-based recommendations", status: "pending" },
      { key: "vfm_scoring", name: "VFM scoring", status: "pending" },
      { key: "safe_car_suggestions", name: "Safest-car suggestions", status: "pending" },
      { key: "recommendation_explanation", name: "Recommendation explanation", status: "pending" }
    ]
  },
  {
    id: "quotation-leads",
    title: "Quotation, Best Price & Lead Capture",
    group: "Conversion Engine",
    priority: "P0",
    owner: "Backend + Frontend + CRM",
    status: "planned",
    summary: "Convert serious buying intent into structured leads, quotations and CRM handoff.",
    whatWillWork: "Get best quote, selected car/variant/color/city, customer details, timeline, finance/exchange interest, lead seriousness score, CRM sync.",
    currentState: "Quotation screen and CTA direction exist conceptually. Business-grade lead engine is not ready.",
    pending: "Build lead schema, lead scoring, customer capture, CRM sync, quote summary, dedupe, transcript storage and sales handoff.",
    items: [
      { key: "quote_cta", name: "Quote CTA", status: "partial" },
      { key: "quotation_canvas", name: "Quotation canvas", status: "partial" },
      { key: "customer_capture", name: "Customer details capture", status: "planned" },
      { key: "lead_scoring", name: "Lead seriousness scoring", status: "pending" },
      { key: "crm_sync", name: "CRM sync", status: "pending" },
      { key: "quote_pdf_share", name: "Quote PDF/share", status: "pending" }
    ]
  },
  {
    id: "offers",
    title: "Offers & Schemes",
    group: "Commerce Core",
    priority: "P1",
    owner: "Data + Backend",
    status: "planned",
    summary: "Current month cash/exchange/corporate benefits with source and validity confidence.",
    whatWillWork: "Model offers, offer breakup, validity month, source comparison, best deal CTA, quote conversion.",
    currentState: "Scraper research exists across sources. Production integration is pending.",
    pending: "Reliable offer ingestion, confidence scoring, current-month validation, UI cards and dealer-specific workflow later.",
    items: [
      { key: "offer_scraper_research", name: "Scraper research", status: "partial" },
      { key: "offer_breakup", name: "Offer breakup", status: "partial" },
      { key: "current_month_validation", name: "Current month validation", status: "planned" },
      { key: "variant_city_offers", name: "Variant/city-specific offers", status: "pending" },
      { key: "offer_to_lead", name: "Offer-to-lead CTA", status: "pending" }
    ]
  },
  {
    id: "whatsapp",
    title: "WhatsApp AI Concierge",
    group: "Channels",
    priority: "P1",
    owner: "Backend + CRM",
    status: "planned",
    summary: "Direct Meta WhatsApp Cloud API integration using the same ACI Core brain and CRM lead capture.",
    whatWillWork: "Customer messages on WhatsApp, AI replies, mobile number becomes qualified lead, quote/EMI reminders, human handoff, deep links to web canvases.",
    currentState: "Strategy is locked: direct Meta Cloud API first, no BSP initially. Not built yet.",
    pending: "Webhook routes, templates, opt-in handling, CRM sync, handoff, audit logs and deep-link canvas flow.",
    items: [
      { key: "direct_meta_strategy", name: "Direct Meta Cloud API strategy", status: "ready" },
      { key: "express_webhook", name: "Express webhook", status: "pending" },
      { key: "same_core_reuse", name: "Same ACI Core reuse", status: "planned" },
      { key: "mobile_lead_capture", name: "Mobile-number lead capture", status: "planned" },
      { key: "templates_reminders", name: "Templates/reminders", status: "pending" },
      { key: "human_handoff", name: "Human handoff", status: "pending" }
    ]
  },
  {
    id: "insurance-exchange",
    title: "Insurance, Exchange & Ownership Modules",
    group: "Future Business Modules",
    priority: "P2",
    owner: "Backend + Partners",
    status: "deferred",
    summary: "Future modules for insurance quotes, exchange, used-car valuation, RC/challan, service cost and ownership journey.",
    whatWillWork: "Insurance quote, add-on advice, exchange valuation, sell your car, RC/challan via legal providers, service/TCO and ownership reminders.",
    currentState: "Valuable future scope, but not immediate launch core.",
    pending: "Provider adapter architecture, consent/privacy, partner APIs, valuation data and ownership workflows.",
    items: [
      { key: "insurance_quote", name: "Insurance quote", status: "pending" },
      { key: "exchange_sell_car", name: "Exchange/sell car", status: "pending" },
      { key: "historical_idv_pricing", name: "Historical pricing for IDV", status: "planned" },
      { key: "rc_challan_adapter", name: "RC/challan provider adapter", status: "deferred" },
      { key: "service_tco", name: "Service cost/TCO", status: "planned" }
    ]
  },
  {
    id: "frontend-canvas",
    title: "Frontend Canvas System",
    group: "User Experience",
    priority: "P0",
    owner: "Frontend",
    status: "partial",
    summary: "Premium chat-first UI with embedded canvases, full detail pages on tap and consistent mobile/laptop experience.",
    whatWillWork: "Home, chat stream, overview, price list, color studio, feature explorer, EMI, comparison, recommendation, quotation, offers and inline cards.",
    currentState: "Many V2 screens exist and some are functional. Files need cleanup and consistent design-system extraction.",
    pending: "Break large files into components, remove CSS duplication, standardize card styles, polish mobile/laptop layouts and prevent mock data.",
    items: [
      { key: "home_screen", name: "Home screen", status: "partial" },
      { key: "price_screen", name: "Price list screen", status: "mostly_ready" },
      { key: "color_studio_screen", name: "Color Studio", status: "mostly_ready" },
      { key: "feature_explorer_screen", name: "Feature Explorer", status: "mostly_ready" },
      { key: "comparison_screen", name: "Comparison screen", status: "partial" },
      { key: "quotation_screen", name: "Quotation screen", status: "partial" },
      { key: "componentization", name: "Componentization", status: "pending" }
    ]
  },
  {
    id: "testing-evals",
    title: "Testing, Evals & Regression Gates",
    group: "Quality System",
    priority: "P0",
    owner: "Backend + QA",
    status: "mostly_ready",
    summary: "Layered testing system for safety, understanding, stress queries, answer quality, speed and regressions.",
    whatWillWork: "Fast safety gate, understanding eval, stress corpus, full 1000–1500 query eval, UI smoke, speed profiler and CI-friendly reports.",
    currentState: "Strong foundation exists: safety, resolver audits, context audits, multi-feature audits, answer-quality smoke, router smoke and workerized understanding eval.",
    pending: "Build brutal 100-query corpus first, then scale to 1000–1500; add UI automation, load testing and production monitoring.",
    items: [
      { key: "foundation_safety", name: "Foundation safety tests", status: "ready" },
      { key: "context_switch_audit", name: "Context switch audit", status: "ready" },
      { key: "multi_feature_audits", name: "Multi-feature audits", status: "ready" },
      { key: "understanding_workers", name: "Understanding workers", status: "mostly_ready" },
      { key: "answer_quality_smoke", name: "Answer quality smoke", status: "mostly_ready" },
      { key: "stress_100", name: "100-query stress corpus", status: "pending" },
      { key: "full_eval_1500", name: "1000–1500 full eval", status: "pending" }
    ]
  },
  {
    id: "performance-scale",
    title: "Performance, Prewarm & Scale",
    group: "Infrastructure",
    priority: "P0",
    owner: "Backend + DevOps",
    status: "mostly_ready",
    summary: "Fast candidate retrieval, prewarmed catalogs, read models, proper indexes and scalable response path.",
    whatWillWork: "Sub-second common answers, non-blocking prewarm, index audits, lean projections, caching, workerized evals and load-test readiness.",
    currentState: "Candidate prewarm and server startup prewarm are implemented. Warm path is fast. Production load testing still pending.",
    pending: "Add remaining index scripts, request-level caching where safe, load testing, rate limits, monitoring and scale drills.",
    items: [
      { key: "price_read_models", name: "Price read models", status: "ready" },
      { key: "candidate_prewarm", name: "Candidate prewarm", status: "ready" },
      { key: "startup_prewarm", name: "Server startup prewarm", status: "ready" },
      { key: "warm_path_speed", name: "Warm path speed", status: "mostly_ready" },
      { key: "index_audits", name: "Index audits", status: "partial" },
      { key: "load_testing", name: "Load testing", status: "pending" },
      { key: "production_monitoring", name: "Production monitoring", status: "pending" }
    ]
  },
  {
    id: "security-legal-seo",
    title: "Security, Privacy, Legal & SEO",
    group: "Production Readiness",
    priority: "P0",
    owner: "Full Stack",
    status: "pending",
    summary: "Launch blockers including privacy, legal pages, rate limiting, abuse protection, secure PII handling, sitemap and robots.",
    whatWillWork: "Privacy Policy, Terms, About, Contact, SEO metadata, sitemap, robots, rate limits, prompt-injection guards, audit logs and consent handling.",
    currentState: "Mostly pending. This is a serious public-launch blocker.",
    pending: "Implement legal pages, security middleware, PII policy, logging boundaries, prompt guards, sitemap/robots and production monitoring.",
    items: [
      { key: "privacy_policy", name: "Privacy Policy", status: "pending" },
      { key: "terms_pages", name: "Terms/legal pages", status: "pending" },
      { key: "seo_metadata", name: "SEO metadata", status: "pending" },
      { key: "sitemap_robots", name: "Sitemap/robots", status: "pending" },
      { key: "rate_limiting", name: "Rate limiting", status: "pending" },
      { key: "pii_handling", name: "PII handling", status: "pending" },
      { key: "prompt_guards", name: "Prompt-injection guardrails", status: "partial" }
    ]
  }
];

module.exports = {
  ACI_PROGRESS_MODULES
};
