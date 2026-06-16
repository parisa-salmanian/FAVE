# Plan: add SCB demographic data to FAVE to get meaningful clusters

Handoff doc. Goal: enrich FAVE's multivariate views (DR/UMAP, PCP, EBM,
contrastive) with SCB demographic/socioeconomic data so they reveal structure
the accessibility map alone cannot — ideally real, interpretable clusters
(neighbourhood types) and supply-vs-need fairness patterns.

---

## 0. What we already learned (READ FIRST — don't repeat these dead ends)

Measured this session, offline, from the baked routing matrices + gravity model
(scripts in `tools/analysis/`):

1. **FAVE's accessibility features are intrinsically ~1-D.** Across all 7 cities
   PCA PC1 = **70–90%** of variance; the 6 service scores correlate **0.5–0.87**
   (everything tracks "distance from centre"). Consequence:
   - **UMAP/PCA show a smear, not clusters.** This is the DATA, not the algorithm.
     UMAP/t-SNE *can* draw clusters here but they'd be **artifacts**.
   - EBM/contrastive on a selection just say "everything higher/lower".
   - PCP lines run roughly parallel (no trade-offs).
2. **The map + sidebar already shows per-district per-category profiles**, so
   "same overall score, different per-service reason" is NOT a combination-only
   finding — it's readable by clicking districts.
3. **Income is the one decorrelated axis we found.** In Växjö,
   `corr(accessibility, income) = -0.51` (richer districts = lower access:
   affluent peripheral/car-dependent vs poorer central/well-served). This gives a
   real **supply-vs-need** story (double-disadvantage quadrant), BUT:
   - At building level income is **district-blocky** (only ~17 distinct values),
     and "double disadvantage" is a **continuous corner**, not a cluster.
     Silhouette did NOT improve with income (−0.121 → −0.128). So income helps the
     *narrative*, not UMAP *clustering*.
4. **Therefore:** for clusters you need BOTH (a) multiple *independent* axes AND
   (b) genuine *multi-modality* (discrete neighbourhood TYPES). Accessibility +
   income alone is a continuous 2-D cloud. **Demographic *composition* is the data
   type that has both** — neighbourhoods come in discrete types.

### Answers to the two open questions
- **"Can more demographic vars help even at 17 RegSO districts?"** Yes — for
  *typing* districts (PCP across ~12 axes + hierarchical/k-means into 3–4 named
  types). **Not via UMAP** (17 points is too few for UMAP regardless of features).
  Right views at district level: **PCP + EBM/contrastive + small-N clustering**.
- **"Do I have to test only on Växjö?"** No. SCB publishes RegSO/DeSO stats for
  ALL municipalities. Bake all 7 cities. Pooling districts across cities also
  raises N (could even make UMAP viable across-city).

### Unit choice (important)
- **RegSO (~17/city)** = meaningful named districts, but too few for UMAP →
  use PCP + clustering, not UMAP.
- **Meso/H3 hexagons (hundreds)** = enough units for UMAP; already in the app.
- **Building/micro (~50k)** = most units, but demographics attached are blocky
  unless sourced at DeSO. DeSO is finer but "means nothing to a planner" — so
  keep RegSO as the *meaningful* label and use hex/building only to get unit count.

---

## 1. Acquire SCB data (external API — tools/ only, never runtime)

SCB is open data, free, no key. PxWeb API: `https://api.scb.se/OV0104/v1/doku/`
(Statistikdatabasen). The existing income file
(`frontend/assets/data/gender-district-income-vaxjo.json`) is already PxWeb JSON
(table HE0110, "Inkomststruktur nettoinkomst ... efter region/RegSO") — copy that
pattern.

Pull these per **RegSO** (and optionally DeSO) for each city's municipality.
Find exact table IDs in Statistikdatabasen under the RegSO/DeSO product
("Statistik baserad på regionala statistikområden"):

| Variable group | What to pull | Why it helps clustering |
|---|---|---|
| Age structure | population by age band (0–15, 16–64, 65+) per RegSO | student vs family vs ageing types |
| Income | mean/median income (have it) + quintiles if available | supply-vs-need axis (−0.51) |
| Education | % with tertiary education | strong neighbourhood-type signal |
| Employment | employment / unemployment rate | deprivation signal |
| Housing tenure | % rental vs owner-occupied; public-housing share | estate vs villa types |
| Background | born in Sweden vs abroad / foreign background % | main Swedish segregation marker |
| Household type | % single / families-with-children / living alone | lifecycle types |
| Density | population / area (derive from RegSO polygon area) | urban-core vs suburb |
| Car ownership | cars per capita per RegSO (if available) | feeds mobility-equity too |

Write a bake script `tools/bake_scb_demographics.py` (mirror
`tools/bake_city_data.py` conventions: a CITIES dict, `--city`, `--force`).
Output one file per city, e.g. `frontend/assets/data/demographics_<city>.json`,
keyed by RegSO code (`regsokod` in the regso geojson), each value = a flat dict of
the variables above. Keep raw counts AND derived rates.

> HARD RULE (CLAUDE.md): the API call lives only in the bake script. The running
> app reads the baked JSON, never SCB.

---

## 2. Disaggregate district → hexagon → building

The app already does the population version: `buildVaxjoBuildingPopulationMap`
in `frontend/assets/js/models/fairness.js` (~L1240–1268) implements paper Eq. 5
`p̂_i = P_d · a_i / Σ_k a_k` (residential buildings, by floor area). Reuse it:

- **COUNT variables** (population, # elderly, # tertiary-educated): distribute to
  residential buildings by floor area (Eq. 5), filtered by usage type. Then a
  building's rate = its share / its area.
- **RATE/MEAN variables** (income, % rental, % foreign background): you cannot
  split a mean by floor area — **assign the district value as-is** (blocky). This
  is the honest limit of RegSO-resolution data; note it.
- **Hexagons**: aggregate the building-level values up to H3 (the app's meso layer
  already exists) — area- or population-weighted.

Generalise `buildVaxjoBuildingPopulationMap` beyond Växjö (it's currently
Växjö-only, gated in `initVaxjoDemandWeights`).

---

## 3. Wire demographics into the multivariate views

Files to touch (classical-script load order — see CLAUDE.md "Frontend code organization"):
- `frontend/assets/js/views/drView.js`
  - `collectDRData` (~L281): add demographic columns to the feature matrix + `metrics`.
  - `DR_FEATURE_CONFIG` (~L1180): add demographic features for contrastive.
  - `computeFeatureDifferences` (~L1261): already generic — picks up new metrics.
- `frontend/assets/js/controllers/drFeatureMode.js`: extend the policy/legacy
  feature-set toggle to offer "accessibility / +demographics / demographics-only".
- PCP panel: add demographic axes.
- EBM (`api/ebm_service.py`) needs no change — it ranks whatever columns it's sent.

Add a **feature-set selector** so the user can compare:
accessibility-only vs accessibility+demographics vs demographics-only.

---

## 4. Test methodology (decide honestly whether clusters appear)

Reuse `tools/analysis/` scripts as templates (city_pipeline.py, vaxjo_equity_*.py).

**Decorrelation gate (run FIRST):** z-score all features, run PCA.
- If PC1 still > ~60% and 2–3 PCs cover everything → still low-D → **clusters
  will NOT appear; stop and use PCP + bivariate views**, don't force UMAP.
- If PC1 < ~50% with several meaningful PCs → real multi-D → clustering justified.

**At RegSO (17/city, or pooled across cities):**
- PCP across all axes; hierarchical clustering → 3–4 named neighbourhood types;
  colour the map by type. (No UMAP at N=17.)

**At hex/building (many units):**
- UMAP + **HDBSCAN**; report **silhouette** and cluster **stability**; reject
  clusters that aren't interpretable types (likely artifacts / area boundaries).
- Remember blocky demographics → clusters may just trace RegSO borders; check.

**Validate any cluster:** is it (a) interpretable as a neighbourhood type, (b)
stable across seeds/params, (c) supported by EBM/contrastive with a clear signed
signature? If not, it's an artifact.

---

## 5. Multi-city
Bake SCB demographics for all 7 cities (vaxjo, malmo, goteborg, stockholm, kalmar,
norrkoping, uppsala). Test per-city AND pooled (pooling raises N and may reveal
cross-city neighbourhood types — a stronger result than any single small city).

---

## 6. Reusable assets from this session
- `tools/analysis/vaxjo_features.py` — gravity fairness scores from routing matrices.
- `tools/analysis/city_pipeline.py` — per-city features + PCA + geography-vs-profile decoupling test.
- `tools/analysis/vaxjo_equity_*.py` — income join, quadrants, building-level cluster test.
- Baked routing matrices: `frontend/assets/data/cities/*/routing/` (on branch `baked-routing-and-analysis-2026-06`).

## 7. Honest expectation
Adding demographics is the **#1 upgrade** and the most likely source of real
clusters (typology is genuinely multi-modal). But if the decorrelation gate shows
the combined data is still ~1-D, accept it: lean on **PCP + a bivariate
access×need map + EBM/contrastive as explanation of a selection**, and demote
UMAP. Don't manufacture clusters that aren't there.
