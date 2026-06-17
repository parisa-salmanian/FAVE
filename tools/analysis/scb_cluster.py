"""
RegSO-level clustering test for Växjö: do accessibility + SCB demographics form
meaningful neighbourhood-type clusters?  (Plan: SCB_EQUITY_CLUSTERING_PLAN.md §4)

100% OFFLINE. Inputs (all local):
  - tools/analysis/vaxjo_features.npz       (per-building gravity fairness scores)
  - frontend/assets/data/demographics_vaxjo.json   (per-RegSO SCB, from scb_parse.py)
  - frontend/assets/data/Vaxjo_regso.geojson       (district polygons)

Restricts to the 16 districts the app actually shows (shown_on_map == true), so
the result matches what the user sees on the map.

Method (honest, small-N):
  1. aggregate building fairness -> district means (point in polygon)
  2. PCA decorrelation gate on access-only / demographics-only / combined
  3. Ward hierarchical clustering on combined z-scores; silhouette k=2..6
  4. print cluster signatures (mean z per feature) and membership
Verdict printed at the end.

Run: .venv/bin/python tools/analysis/scb_cluster.py
"""
import os, json, argparse, numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.path import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
from scipy.cluster.hierarchy import linkage, fcluster

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
CATS = ["grocery", "hospital", "healthcare_center", "pharmacy", "kindergarten", "school_primary"]

# demographic features with full coverage across the 16 shown districts
DEMO_FEATS = [
    "employed_pct", "unemployed_pct", "longterm_unemployed_pct", "managerial_pct",
    "avg_disp_income_pba", "median_disp_income_pba", "income_support_share_pct",
    "unemployment_benefit_share_pct", "eligible_higher_edu_pct", "students_pct",
]


def district_polys(geo):
    polys = {}
    for f in geo["features"]:
        code = f["properties"]["regsokod"]
        g = f["geometry"]
        rings = [g["coordinates"][0]] if g["type"] == "Polygon" else [r[0] for r in g["coordinates"]]
        polys[code] = [Path(np.array(r)) for r in rings]
    return polys


def make_figure(Cz, feat_names, names, A, D, out):
    from scipy.cluster.hierarchy import dendrogram
    lab = fcluster(linkage(Cz, method="ward"), t=3, criterion="maxclust")
    colors = {1: "#1f77b4", 2: "#2ca02c", 3: "#d62728"}
    fig, ax = plt.subplots(1, 3, figsize=(20, 6))

    # (a) dendrogram
    dendrogram(linkage(Cz, method="ward"), labels=names, ax=ax[0],
               leaf_rotation=90, color_threshold=0)
    ax[0].set_title("Ward dendrogram (accessibility + SCB demographics, z-scored)")

    # (b) supply-vs-need quadrant, coloured by cluster
    ov = A[:, 0]
    inc = D[:, DEMO_FEATS.index("median_disp_income_pba")]
    ax[1].scatter(ov, inc, c=[colors[l] for l in lab], s=120, edgecolor="k")
    for i, nm in enumerate(names):
        ax[1].annotate(nm[:14], (ov[i], inc[i]), fontsize=7, xytext=(3, 3),
                       textcoords="offset points")
    ax[1].axvline(np.median(ov), ls="--", c="gray")
    ax[1].axhline(np.median(inc), ls="--", c="gray")
    ax[1].set_xlabel("overall accessibility fairness")
    ax[1].set_ylabel("median disposable income (PBA)")
    ax[1].set_title(f"supply vs need  (corr={np.corrcoef(ov, inc)[0,1]:+.2f})")

    # (c) PCP across all features, coloured by cluster
    Xn = (Cz - Cz.min(0)) / (np.ptp(Cz, axis=0) + 1e-9)
    xs = np.arange(len(feat_names))
    for i in range(len(names)):
        ax[2].plot(xs, Xn[i], color=colors[lab[i]], alpha=0.6, lw=1.2)
    ax[2].set_xticks(xs)
    ax[2].set_xticklabels(feat_names, rotation=90, fontsize=7)
    ax[2].set_title("PCP (blue=affluent low-access · green=central served · red=Araby)")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    print(f"\nsaved figure: {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fig", help="path to save the 3-panel figure (PNG)")
    args = ap.parse_args()
    feat = np.load(os.path.join(ROOT, "tools/analysis/vaxjo_features.npz"))
    coords = feat["coords"]
    overall = feat["overall"]
    catscore = {c: feat[f"score_{c}"] for c in CATS}

    demo = json.load(open(os.path.join(ROOT, "frontend/assets/data/demographics_vaxjo.json")))
    geo = json.load(open(os.path.join(ROOT, "frontend/assets/data/Vaxjo_regso.geojson")))
    polys = district_polys(geo)

    shown = [code for code, r in demo.items() if r.get("shown_on_map")]
    rows = []
    for code in shown:
        paths = polys.get(code)
        if not paths:
            continue
        mask = np.fromiter((any(p.contains_point(c) for p in paths) for c in coords),
                           dtype=bool, count=len(coords))
        n = int(mask.sum())
        if n < 30:
            print(f"  skip {demo[code]['regsonamn']}: only {n} buildings")
            continue
        acc = {"overall": float(overall[mask].mean())}
        for c in CATS:
            acc[c] = float(catscore[c][mask].mean())
        d = demo[code]
        if any(d.get(f) is None for f in DEMO_FEATS):
            miss = [f for f in DEMO_FEATS if d.get(f) is None]
            print(f"  WARN {d['regsonamn']}: missing {miss} -> dropped from clustering")
            continue
        rows.append({"code": code, "name": d["regsonamn"], "n": n, "acc": acc,
                     "demo": {f: float(d[f]) for f in DEMO_FEATS}})

    names = [r["name"] for r in rows]
    N = len(rows)
    print(f"\nclustering on {N} districts (buildings {min(r['n'] for r in rows)}–{max(r['n'] for r in rows)})\n")

    acc_feats = ["overall"] + CATS
    A = np.array([[r["acc"][f] for f in acc_feats] for r in rows])
    D = np.array([[r["demo"][f] for f in DEMO_FEATS] for r in rows])
    Az = StandardScaler().fit_transform(A)
    Dz = StandardScaler().fit_transform(D)
    Cz = np.column_stack([Az, Dz])
    feat_names = acc_feats + DEMO_FEATS

    # ---------- 1. PCA decorrelation gate ----------
    def pca_gate(X, label):
        p = PCA().fit(X)
        ev = p.explained_variance_ratio_
        cum = np.cumsum(ev)
        k90 = int(np.argmax(cum >= 0.90) + 1)
        print(f"  {label:22s} dim={X.shape[1]:2d}  PC1={ev[0]*100:4.0f}%  "
              f"PC1-2={cum[1]*100:4.0f}%  PCs>=90%var: {k90}")
        return ev
    print("PCA decorrelation gate (z-scored):")
    pca_gate(Az, "accessibility-only")
    pca_gate(Dz, "demographics-only")
    ev = pca_gate(Cz, "combined")
    pc1 = ev[0] * 100
    print()

    # ---------- 2. correlation: accessibility vs need ----------
    ov = A[:, 0]
    inc = D[:, DEMO_FEATS.index("median_disp_income_pba")]
    une = D[:, DEMO_FEATS.index("unemployed_pct")]
    print(f"corr(overall access, median income)     = {np.corrcoef(ov, inc)[0,1]:+.2f}")
    print(f"corr(overall access, unemployment)      = {np.corrcoef(ov, une)[0,1]:+.2f}\n")

    # ---------- 3. silhouette across k ----------
    print("Ward hierarchical clustering — silhouette by k (combined):")
    best = None
    for k in range(2, 7):
        lab = AgglomerativeClustering(n_clusters=k, linkage="ward").fit_predict(Cz)
        s = silhouette_score(Cz, lab)
        flag = ""
        if best is None or s > best[1]:
            best = (k, s)
        print(f"  k={k}: silhouette={s:+.3f}")
    print(f"  -> best k = {best[0]} (silhouette {best[1]:+.3f})\n")

    # ---------- 4. cluster signatures at chosen k ----------
    for k in sorted({3, best[0]}):
        Z = linkage(Cz, method="ward")
        lab = fcluster(Z, t=k, criterion="maxclust")
        print(f"=== k={k} clusters ===")
        for cl in sorted(set(lab)):
            idx = np.where(lab == cl)[0]
            sig = Cz[idx].mean(0)
            top = sorted(range(len(feat_names)), key=lambda j: -abs(sig[j]))[:4]
            desc = ", ".join(f"{feat_names[j]} {sig[j]:+.1f}σ" for j in top)
            print(f"  cluster {cl} (n={len(idx)}): {desc}")
            print(f"     {', '.join(names[i] for i in idx)}")
        print()

    # ---------- verdict ----------
    print("=" * 70)
    if pc1 > 60:
        print(f"VERDICT: combined data is still ~1-D (PC1={pc1:.0f}%). The deprivation")
        print("gradient dominates; 'clusters' are cuts along one axis, not distinct types.")
    elif pc1 < 50 and best[1] > 0.25:
        print(f"VERDICT: real multi-D structure (PC1={pc1:.0f}%) and clusters separate")
        print(f"(silhouette {best[1]:+.2f}). Neighbourhood types are defensible.")
    else:
        print(f"VERDICT: borderline (PC1={pc1:.0f}%, best silhouette {best[1]:+.2f}). Some")
        print("structure beyond 1-D, but clusters are weak — present as a gradient + a")
        print("few outlier types (e.g. campus), not as clean k-means typology.")

    if args.fig:
        make_figure(Cz, feat_names, names, A, D, args.fig)


if __name__ == "__main__":
    main()
