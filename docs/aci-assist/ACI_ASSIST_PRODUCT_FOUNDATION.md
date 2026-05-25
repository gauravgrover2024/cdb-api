# ACI Assist Product Foundation

## Locked product ambition

ACI Assist is not a demo chatbot. It is a new-car intelligence and conversion platform.

The long-term ambition is to build a product strong enough that major Indian automotive/tech players would take it seriously for partnership, investment, or acquisition.

## Locked philosophy

- Chat-first
- Canvas-powered
- Conversion-focused
- Blazing fast
- Secure and privacy-aware
- CRM-ready
- WhatsApp-ready
- SEO-ready
- Testing-agent validated
- Real database facts only
- No fake prices, variants, offers, features, mileage, colors, ratings, or cars
- Mobile and laptop UI only; no awkward half-responsive layout
- Premium, restrained, white/glassmorphism UI direction
- Answer first, then ask one useful next question

## Locked architecture direction

- Backend: existing Node/Express/MongoDB remains source of truth
- AI orchestration: Mastra self-hosted after foundation cleanup
- Model: Gemini 2.5 Flash only
- Frontend: custom React ACI Assist chat/canvas system
- Tools: deterministic MongoDB-backed tools
- Streaming: safe early status language before final data/card loads
- Testing: staged testing agent, starting with 150 core queries and expanding to 1500 launch tests

## Current status

Current V2 is a working base, not yet final launch architecture.

Strengths:
- V2 route consolidation completed
- Old frontend layer removed
- Old backend V1 files archived
- V2 service and executor smoke tests passing
- Core response contract exists
- Real DB-backed tool flows exist

Known risks:
- Very large frontend files
- Very large backend files
- Mixed orchestration layers
- Some modular tools are still stubs
- No real streaming lifecycle yet
- Public route security/rate limiting needs hardening
- CRM is draft-level, not production-level
- WhatsApp integration is planned but not implemented
- SEO/legal/product pages are incomplete
- Performance needs product-grade treatment

## Product foundation cleanup goals

Before Mastra runtime wiring, we must complete:

1. Define one backend response contract.
2. Define one frontend parser for that contract.
3. Decide authoritative tool paths.
4. Remove or isolate stubs from active production paths.
5. Add public/private/channel permission rules.
6. Add basic request/response schema validation.
7. Add route-level security and rate-limit plan.
8. Add first testing-agent structure.
9. Create frontend modularization plan without changing UI randomly.
10. Create streaming event contract.
11. Create CRM/lead seriousness contract.
12. Create WhatsApp channel contract for later reuse.

## Non-negotiable rules

- Do not add fake data.
- Do not redesign UI without approval.
- Do not add framework complexity without removing equal or greater confusion.
- Do not break /portal or /aci-assist.
- Do not break existing V2 contract without frontend migration.
- Do not allow AI to invent factual car data.
- Do not expose internal CDrive data through public/WhatsApp routes.
- Do not optimize for demo speed at the cost of product quality.
