# FAVE deficit thresholds — LOCKED decision (provenance-graded, citation-verified)

**Status:** citations verified against primary sources 2026-06-29. This file
SUPERSEDES the earlier paste-relayed table whose citations were wrong (see
"Correction log" at the bottom).

## Definition

**Unit: metres per category, mode-agnostic.** Deficit is computed in minutes by
converting distance → time via the existing `config.js` per-mode speed model:

```
threshold_min(mode) = distance_m / speed(mode)
deficit_c           = max(0, minutes_to_nearest_c − threshold_min)
binding constraint  = argmax_c deficit_c   (all deficits 0 → "adequately served")
```

Metres-not-minutes is deliberate: because both `minutes_to_nearest_c` and
`threshold_min` divide by the same mode speed, the speed **factors out** of the
cross-category comparison. So **which service binds** and **is this area
deficient** are *mode-invariant facts*; only the deficit's magnitude rescales
with mode. Claim this invariance in the paper — it neutralises the "driving
washes out the signal" objection.

**Implementation note that PRESERVES the invariance:** compute
`minutes_to_nearest_c` as `nearest-facility network distance ÷ mode reference
speed`, NOT the full gravity/transit travel time. Feeding real
access+wait+in-vehicle+egress time into the threshold test breaks the factoring
and makes "1000 m ÷ transit speed" ill-defined. Keep the deficit test on
distance-to-nearest; let the full network/gravity/transit model drive the access
value that feeds the Gini oracle (enforces separation-of-concerns at the formula
level).

## Threshold table

| Category | Threshold (m) | Provenance grade | Source |
|---|---|---|---|
| **Grocery** (livsmedelsbutik) | 1000 | ★★★ national indicator | Trafikanalys LTI — pop. share within 1000 m road-network |
| **Primary school** (grundskola) | 1000 | ★★★ national indicator | Trafikanalys LTI |
| **Healthcare centre** (vårdcentral) | 1000 | ★★★ national indicator | Trafikanalys LTI |
| **High school** (gymnasium) | 6000 | ★★★ national **law** | SFS 1991:1110 — statutory elevresor (student-travel) threshold |
| **Pharmacy** (apotek) | 2000 | ★★ national sectoral | Tillväxtverket R0478 (2024) commercial-service distance bands |
| **Kindergarten** (förskola) | 1500 | ★ municipal proxy | no national norm; set below lower-school (youngest children) |
| **Hospital** (sjukhus) | 5000 | ½ literature default | no national distance norm; inherently drive/transit-bound |

**Coverage: 4 of 7 have genuine national anchors** (grocery, primary school,
healthcare via the Trafikanalys 1000 m index; gymnasium via statute). förskola =
municipal proxy; pharmacy = national but sectoral/distance-band; hospital =
literature default.

## Hard constraints for downstream use

1. **Deficit signature is raw physical distance/time — NO need-weighting /
   socioWeight.** Keeps the EBM non-circular and the binding-constraint a
   physical fact. Need enters ONLY through the Gini oracle's weighting.
2. **Correct label in the manuscript** (do NOT write "Swedish standards" as a
   blanket): *"national Swedish access indicators where they exist (grocery,
   school, healthcare — Trafikanalys; gymnasium — statute); sectoral/literature
   defaults elsewhere; distance-expressed; stable under ±500 m sensitivity."*
3. **Sensitivity is mandatory:** perturb each threshold ±500 m, show the
   binding-constraint classification is stable. It does the most work on the
   weaker rows (förskola, pharmacy, hospital) — that is the point.
4. **Thresholds drive** the commensurable description, the composition-informed
   policy's choice rule, and the EBM target. They **do NOT** touch the Gini
   oracle that scores outcomes. State this separation explicitly.
5. **Do NOT adopt Trafikanalys's 2/5/10 km per-mode bands as thresholds.**
   Per-mode distance thresholds destroy the mode-invariance in constraint #0.
   Use ONE distance per category and convert via speed. The 2/5/10 km bands may
   be cited only as the crow-fly equivalent / a sensitivity variant.

## Why metres = 1000 m matches the harness exactly

The baked routing matrices are **network metres** (per CLAUDE.md → "OSRM and
routing"). The Trafikanalys indicator is also defined on the **road network**
("max 1000 meter i vägnätet"), so 1000 m is the *exactly-matching* standard for
the three LTI services — not a crow-fly approximation. Use the network-distance
rows from the baked matrices directly against the 1000 m cut.

Expected pattern: with a tight 1000 m core threshold, deficits appear in many
urban areas (not only the sparse rural tail). Flag this as the *expected* result,
or a reviewer reads low deficit-prevalence as a weak finding.

## The four-agency minefield (get attribution exactly right)

Four near-identical Swedish agency names — confusing the wrong one is the one
own-goal you can't take back:

- **Trafikanalys** → the 1000 m Local Accessibility Index (LTI) + the 2/5/10 km
  mode bands. ← THE anchor for grocery / school / healthcare.
- **Tillväxtverket** → grocery + pharmacy commercial-service bands (R0478 2024).
- **Trafikverket** → the 2012:193 urban "enkla tillgänglighetsmått" report. NOT
  used here.
- **Tillväxtanalys** → NOT a source. (This was the misattribution the
  citation-check caught — never write it.)

Cite the headline indicator as **Trafikanalys (developer)** reported via
**Sveriges miljömål / Boverket (reporting venue under "God bebyggd miljö")** —
NOT "Boverket's indicator," which mis-credits the developer.

## Correction log (what changed from the earlier relayed table)

| Category | Old (wrong) | Corrected | Reason |
|---|---|---|---|
| Grocery | 2000 m, "Tillväxtverket 2 km" | **1000 m, Trafikanalys LTI** | wrong agency; 2 km was the LTI *walking crow-fly band*, not a Tillväxtverket grocery standard |
| Primary school | 2000 m, "Stockholm municipal" | **1000 m, Trafikanalys LTI (national)** | a national anchor exists; Stockholm 2 km is skolskjuts placement, a different construct |
| Healthcare centre | 3000 m, "no norm / literature default" | **1000 m, Trafikanalys LTI (national)** | **was flat wrong** — vårdcentral IS one of the three LTI services |
| High school | 4000 m, "literature default" | **6000 m, SFS 1991:1110 (law)** | a statutory national figure exists |
| Pharmacy | 2000 m, "car norm walk-adapted" | 2000 m, **Tillväxtverket R0478** | number kept; attribution clarified (sectoral commercial-service, not LTI) |
| Kindergarten | 1500 m | 1500 m | unchanged — genuinely no national norm |
| Hospital | 5000 m | 5000 m | unchanged — genuinely no national norm |

## Sources

- Sveriges miljömål — *Tillgång till service och grönska* (God bebyggd miljö
  indicator; Trafikanalys 1000 m road-network index covering grundskolor,
  livsmedelsbutiker, vårdcentraler):
  https://www.sverigesmiljomal.se/miljomalen/god-bebyggd-miljo/tillgang-till-service-och-gronska/
- Trafikanalys PM 2021:6 — *Fördjupad måluppföljning – utveckling av
  tillgänglighetsmått* (LTI methodology; 2/5/10 km mode bands):
  https://www.trafa.se/globalassets/pm/2021/pm-2021_6-fordjupad-maluppfoljning--utveckling-av-tillganglighetsmatt-till-arlig-maluppfoljning.pdf
- SFS 1991:1110 — *Lag om kommunernas skyldighet att svara för vissa elevresor*
  (gymnasium 6 km student-travel threshold).
- Tillväxtverket Rapport 0478 (2024) — *Tillgänglighet till grundläggande
  kommersiell service* (grocery + pharmacy distance/time bands):
  https://tillvaxtverket.se/download/18.5eafc64b18ee72dfa42537a/1713437212174/Tillga%CC%88nglighet_grundla%CC%88ggande_kommersiell_service_2024__%20(TG).pdf
