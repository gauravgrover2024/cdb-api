# ACI Assist Customer Query Corpus v1

Purpose: protect ACI Assist against real buyer questions, embarrassing routing mistakes, stale context, wrong prices, weak comparisons, and vague answers.

Status: Draft v1. Not all cases are automated yet.

## A. Budget Discovery

1. cars under 20 lakhs
2. best cars under 20 lakhs
3. SUVs under 20 lakhs
4. automatic cars under 20 lakhs
5. best automatic SUVs under 20 lakhs
6. safest cars under 15 lakhs
7. family cars under 12 lakhs
8. CNG cars under 10 lakhs
9. automatic hatchbacks under 12 lakhs
10. sedans under 18 lakhs

Expected:
- intent should be vehicle_recommendation / budget discovery
- canvas should be recommendation_results_canvas or budget explorer canvas
- no over-budget vehicles
- answer should state total count + preview count
- no activeComparison
- model groups should be returned

## B. Price and On-road Breakup

11. Creta SX on-road price Delhi
12. Creta SX on-road breakup Delhi
13. Verna HX8 iVT on-road price Delhi
14. City ZX CVT on-road price Delhi
15. Nexon Smart CNG on-road price Delhi

Expected:
- exact variant should resolve
- no fallback to base variant
- breakup query should return price_breakup_canvas
- price should be from read-model rows
- answer should mention exact variant and city

## C. Feature Discovery

16. Hyundai cars with sunroof under 20 lakh
17. cars with ADAS under 25 lakhs
18. CNG cars with sunroof
19. SUVs with 6 airbags under 15 lakh
20. automatic cars with ventilated seats under 25 lakh

Expected:
- feature must be DB-backed
- fuel/body/transmission must be filters, not fake feature rows
- grouped by model
- feature starts-from variant should be visible
- no random unrelated cars

## D. Feature Comparison

21. Punch and Nexon CNG sunroof ABS ADAS
22. Creta and Seltos sunroof ADAS ventilated seats
23. Verna and City ADAS sunroof ventilated seats
24. Nexon and Brezza CNG sunroof 6 airbags

Expected:
- activeComparison should include both vehicles
- selectedVehicle should not collapse to first car
- CNG should be fuelFilter, not feature row
- rows should contain only requested feature comparisons
- follow-up should preserve context

## E. Variant Comparison

25. Verna HX8 iVT vs City ZX CVT
26. Creta S(O) IVT vs Seltos HTX IVT
27. Nexon Fearless Plus S vs Brezza ZXI Plus AT
28. Punch Adventure S CNG vs Nexon Smart Plus S CNG

Expected:
- exact variants should resolve
- comparisonSummary should include price difference
- featureDifferences should be present where matrix exists
- selectedVehicle should be null
- activeComparison should include exactly compared vehicles

## F. Follow-up and Context

29. Punch and Nexon CNG sunroof ABS ADAS -> Which one is better?
30. Verna HX8 iVT vs City ZX CVT -> Which one gives more features?

Expected:
- follow-up should reuse activeComparison
- should not ask generic clarification
- should not switch to stale previous car
- answer should stay scoped to compared vehicles

## G. Ambiguity, Alias, No-result, and City Coverage

31. Tata Nexon vs Nexon EV
32. Swift vs Baleno
33. turbocharged SUVs under 8 lakhs
34. Creta SX on-road price Gurgaon
35. Verna HX8 iVT on-road price Noida
36. City ZX CVT on-road price Delhi
37. Creta SX on-road price Mumbai
38. cars under 20 lakhs in Gurgaon
39. automatic SUVs under 20 lakhs in Noida
40. cheapest CNG cars in Delhi

Expected:
- Same-name ambiguity should not collapse Nexon and Nexon EV incorrectly.
- Short aliases like Swift and Baleno should resolve without requiring make prefix.
- No-result queries should respond honestly and suggest nearby alternatives, not return wrong cars.
- Delhi, Noida, and Gurgaon should resolve as supported price cities.
- Unsupported cities such as Mumbai should not fabricate on-road prices; response should say pricing is currently available for Delhi, Noida, and Gurgaon.
- Budget filters must remain strict in supported cities.
- City context should not leak into later unrelated queries unless user asks a follow-up.
