# SCB + accessibility clustering — Växjö RegSO findings

Reproduce (100% offline, no APIs):
```bash
.venv/bin/python tools/analysis/scb_parse.py   --city vaxjo   # -> demographics_vaxjo.json
.venv/bin/python tools/analysis/scb_cluster.py --fig ~/Downloads/vaxjo_scb_clusters.png
```
Unit: RegSO, restricted to the **16 districts the app shows** (`shown_on_map`,
matching frontend `EXCLUDED_DISTRICTS`). Demographics = SCB AA/AA0003 (2023/2024).
Accessibility = per-building gravity fairness aggregated to district means.

## Data correctness (verified)
- **Slice selection verified**: the `TOT` / `tot20-64`, both-sexes totals sit
  between the Swedish-born and foreign-born subgroups (population-weighted), as a
  true total must. e.g. Araby employed SE 59.5 / **TOT 57.3** / F 41.7.
- **Internal consistency** across all 12 variables: Araby = lowest employment
  (57%), highest unemployment (39%), lowest managerial (0.8%), highest
  income-support; affluent villa belt (Östra Lugnet, Sandsbro, Öjaby, Hov-Norr)
  = 88–91% employed, 11–13% managerial, top income; Universitetsområde = 76%
  students, 40% employed, lowest income, 97% higher-ed eligible (textbook campus).
- **[NEEDS DATA] foreign-background / origin share is NOT extractable** from the
  baked tables. `IntGr3RegSOKONS` is a within-group cross-tab (age/education
  *inside* each origin group; the `TOTp` row is 100 by definition). The main
  Swedish segregation marker therefore needs a counts table (population by RegSO ×
  background) added to `tools/bake_scb_demographics.py`.
- **Stale**: `FolkmangdRegSO` is population year **2009** ("not updated") — not
  used; no current population/density variable is available from the baked set.
- **Missing values** (SCB suppresses small areas): dropped 2 high-missing fields
  (`income_support_only_pct`, `eligible_upper_secondary_pct`) from clustering;
  the 10 kept demographic features are complete across all 16 shown districts.

## Result: are there real clusters?
**Partly — honest answer: a 2-D gradient + one genuine outlier, not crisp types.**

PCA decorrelation gate (z-scored):
| feature set | PC1 | PC1–2 | PCs for 90% |
|---|---|---|---|
| accessibility-only | 71% | 84% | 3 |
| demographics-only | **68%** | 94% | 2 |
| **combined** | **54%** | 73% | 4 |

- **New finding**: demographics *alone* are also ~1-D (a single
  deprivation↔affluence axis — income, employment, managerial, education all
  co-vary). So demographics by themselves do **not** add a dimension.
- **What does**: combining accessibility + demographics drops PC1 to 54% because
  the two gradients are *anti-correlated* — `corr(overall access, median income) =
  −0.60`. Affluent peripheral villa areas are car-dependent (low walk access);
  poorer central areas are well-served. This **supply-vs-need** decoupling is the
  real source of 2-D structure.

Clustering (Ward, combined): silhouette peaks at **k=3 = +0.26** (weak; k=2 and
k=4 nearly identical → no strong natural k). The 3 groups:
1. **Affluent, low-access periphery** (7): Hov-Norr, Hovshaga, Högstorp, Sandsbro,
   Teleborg, Öjaby-Räppe, Östra Lugnet — high managerial, low grocery/pharmacy access.
2. **Central, well-served, mixed-SES** (8): Gamla norr, Kungsmad, Söder-Bäckaslöv,
   Teleborg centrum, Universitetsområde, Västra mark, Växjö centrum, Växjö öster.
3. **Araby-Dalbo-Nydala alone** (1): +3σ on unemployment/income-support — a real
   distinct deprivation type, not a smear corner.

## Honest verdict
The combined access+demographic data is genuinely **2-D, not 1-D** (the key win
over accessibility-only), and the supply-vs-need quadrant is a real, interpretable
story. But the clustering is **weak (silhouette 0.26)**: present it as a
**continuous access×SES plane with two corners + Araby as a labelled outlier
type**, not as a clean k=3/k=4 neighbourhood typology. Universitetsområde is a
second genuine outlier (campus) worth labelling separately even though k=3 folds
it into the central group. This matches the plan's "honest expectation" (§7):
lean on PCP + the bivariate access×need map + EBM/contrastive on a selection;
don't manufacture clusters.
