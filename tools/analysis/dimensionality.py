"""
Try ALL candidate feature blocks for Vaxjo at BUILDING level and measure which
combination gives genuinely high-dimensional structure (so DR/UMAP is meaningful).

100% OFFLINE. Inputs (all local, already baked):
  - routing/{walking,cycling,driving}/<cat>.json   (per-building sorted distances)
  - lantmateriat-...buildings...geojson            (height_m, andamal1, geometry)
  - Vaxjo_regso.geojson + demographics_vaxjo.json  (district need / income)

Feature blocks (each is one *candidate dimension family*):
  ACCESS     walking gravity access, 6 case-study categories  (the distance-from-centre axis)
  MODAL      car-dependency: walk/drive & cycle/drive access ratios (modal disadvantage)
  FORM       built form: height, log footprint area, local density, %multi-family
  DIVERSITY  service mix: Shannon entropy of per-category access + #categories <1km
  DEMO       district need index + median income (the socioeconomic axis, blocky)

Evaluation (the honest test of "is it really N-D"):
  - per block: PCA PC1%, dims for 90% var, participation ratio (effective #dims)
  - independence: |corr(block PC1, ACCESS PC1)| and |corr(block PC1, need)|
  - forward selection: greedily add the block that most raises the participation
    ratio of the combined z-scored matrix -> reports the best combination.

Run (first run builds a cache ~30s):
  .venv/bin/python tools/analysis/dimensionality.py
"""
import os, json, math, numpy as np
from scipy.spatial import cKDTree
from matplotlib.path import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
DATA = os.path.join(ROOT, "frontend/assets/data")
VDIR = os.path.join(DATA, "cities/vaxjo")
CACHE = os.path.join(os.path.dirname(__file__), "dimensionality_cache.npz")

CATS = ["grocery", "hospital", "healthcare_center", "pharmacy", "kindergarten", "school_primary"]
KAPPA = {"grocery":1.2,"hospital":0.6,"pharmacy":1.0,"healthcare_center":0.8,
         "kindergarten":1.1,"school_primary":0.9}
RHO   = {"grocery":1.0,"hospital":1.4,"pharmacy":1.2,"healthcare_center":1.2,
         "kindergarten":1.3,"school_primary":1.2}


def gravity_access(mode):
    """per-building IF-City access per category from baked sorted distances (top-3)."""
    out = {}
    coords = None
    for c in CATS:
        d = json.load(open(os.path.join(VDIR, f"routing/{mode}/{c}.json")))
        if coords is None:
            coords = np.array([[float(x) for x in k.split(",")] for k in d["key"]])
        m = d["m"]; kappa = KAPPA[c]; rho = RHO[c]
        acc = np.empty(len(m))
        for i, dist in enumerate(m):
            top = dist[:3]
            acc[i] = sum(rho * math.exp(-kappa * (v / 1000.0)) for v in top) if top else 0.0
        out[c] = acc
    return coords, out


def build_form(coords):
    """match each routing building to nearest Lantmateriet footprint for height/area/type."""
    fp = json.load(open(os.path.join(DATA, "lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson")))
    cent, hgt, area, multi = [], [], [], []
    for f in fp["features"]:
        g = f.get("geometry") or {}
        cs = g.get("coordinates")
        if not cs:
            continue
        # first ring (Polygon or MultiPolygon)
        ring = cs[0][0] if g["type"] == "MultiPolygon" else cs[0]
        a = np.array(ring)
        if a.ndim != 2 or len(a) < 3:
            continue
        cent.append(a.mean(0))
        p = f.get("properties") or {}
        hgt.append(float(p.get("height_m") or p.get("_mean") or 0.0))
        # shoelace area in deg^2 (relative size is all we need after z-scoring)
        x, y = a[:, 0], a[:, 1]
        area.append(abs(np.dot(x, np.roll(y, 1)) - np.dot(y, np.roll(x, 1))) / 2.0)
        multi.append(1.0 if "Flerfamiljshus" in str(p.get("andamal1", "")) else 0.0)
    cent = np.array(cent)
    tree = cKDTree(cent)
    _, idx = tree.query(coords, k=1)
    H = np.array(hgt)[idx]; A = np.log1p(np.array(area)[idx] * 1e8); M = np.array(multi)[idx]
    # local building density: neighbours within ~200 m (deg approx at 56.8N)
    deg = 200.0 / 111320.0
    bt = cKDTree(coords)
    dens = np.array([len(bt.query_ball_point(c, deg)) for c in coords], dtype=float)
    return H, A, np.log1p(dens), M


def demo_blocks(coords):
    """building -> district needZ + median income (district-blocky)."""
    dem = json.load(open(os.path.join(DATA, "demographics_vaxjo.json")))
    geo = json.load(open(os.path.join(DATA, "Vaxjo_regso.geojson")))
    # need index over shown districts
    shown = [c for c in dem if dem[c].get("shown_on_map")]
    flds = [("median_disp_income_pba", -1), ("unemployed_pct", 1),
            ("income_support_share_pct", 1), ("eligible_higher_edu_pct", -1)]
    stat = {}
    for f, _ in flds:
        v = np.array([float(dem[c][f]) for c in shown if dem[c].get(f) is not None])
        stat[f] = (v.mean(), v.std() or 1)
    need = {}
    for c in shown:
        s = u = 0
        for f, sg in flds:
            x = dem[c].get(f)
            if x is None: continue
            mu, sd = stat[f]; s += sg * (float(x) - mu) / sd; u += 1
        if u: need[c] = s / u
    nv = np.array(list(need.values())); mu, sd = nv.mean(), nv.std() or 1
    need = {c: (v - mu) / sd for c, v in need.items()}

    needZ = np.full(len(coords), np.nan); inc = np.full(len(coords), np.nan)
    for f in geo["features"]:
        code = f["properties"]["regsokod"]
        if code not in need: continue
        g = f["geometry"]
        rings = [g["coordinates"][0]] if g["type"] == "Polygon" else [r[0] for r in g["coordinates"]]
        inside = np.zeros(len(coords), bool)
        for r in rings:
            inside |= Path(np.array(r)).contains_points(coords)
        needZ[inside] = need[code]
        inc[inside] = float(dem[code]["median_disp_income_pba"])
    # impute missing (rural / unmatched) with median
    for arr in (needZ, inc):
        arr[~np.isfinite(arr)] = np.nanmedian(arr)
    return needZ, inc


def participation_ratio(X):
    """effective number of dimensions = (sum eig)^2 / sum(eig^2)."""
    ev = PCA().fit(X).explained_variance_
    return (ev.sum() ** 2) / (np.sum(ev ** 2))


def pca_summary(X):
    p = PCA().fit(X); ev = p.explained_variance_ratio_; cum = np.cumsum(ev)
    return ev[0] * 100, int(np.argmax(cum >= 0.90) + 1), participation_ratio(X)


def main():
    if os.path.exists(CACHE):
        z = np.load(CACHE, allow_pickle=True)
        coords = z["coords"]; blocks = z["blocks"].item()
        print(f"[cache] loaded {len(coords)} buildings")
    else:
        print("[build] walking access ..."); coords, w = gravity_access("walking")
        print("[build] cycling access ..."); _, cyc = gravity_access("cycling")
        print("[build] driving access ..."); _, drv = gravity_access("driving")
        print("[build] built form ...");     H, A, D, M = build_form(coords)
        print("[build] demographics ...");   needZ, inc = demo_blocks(coords)

        w_overall = np.sum([w[c] for c in CATS], axis=0)
        d_overall = np.sum([drv[c] for c in CATS], axis=0)
        c_overall = np.sum([cyc[c] for c in CATS], axis=0)
        eps = 1e-6
        # service diversity: Shannon entropy of per-cat walking access + #cats reachable
        P = np.array([w[c] for c in CATS]).T  # (N,6)
        Pn = P / (P.sum(1, keepdims=True) + eps)
        entropy = -np.sum(Pn * np.log(Pn + eps), axis=1)
        reachable = np.sum(P > 0.05, axis=1).astype(float)

        blocks = {
            # ACCESS block (6 correlated cols -> ~1 effective dim)
            **{f"ACCESS:{c}": w[c] for c in CATS},
            # MODAL block: car-dependency ratios (independent of level)
            "MODAL:walk_drive": np.log((w_overall + eps) / (d_overall + eps)),
            "MODAL:cycle_drive": np.log((c_overall + eps) / (d_overall + eps)),
            # FORM block
            "FORM:height": H, "FORM:logarea": A, "FORM:logdensity": D, "FORM:multifamily": M,
            # DIVERSITY block
            "DIVERSITY:entropy": entropy, "DIVERSITY:reachable": reachable,
            # DEMO block
            "DEMO:needZ": needZ, "DEMO:income": inc,
        }
        np.savez(CACHE, coords=coords, blocks=np.array(blocks, dtype=object))
        print(f"[cache] saved -> {CACHE}")

    BLOCK_NAMES = ["ACCESS", "MODAL", "FORM", "DIVERSITY", "DEMO"]
    def cols(b): return [k for k in blocks if k.split(":")[0] == b]
    def mat(names):
        keys = [k for k in blocks for b in names if k.split(":")[0] == b]
        keys = [k for k in blocks if k.split(":")[0] in names]
        return StandardScaler().fit_transform(np.column_stack([blocks[k] for k in keys])), keys

    N = len(blocks[next(iter(blocks))])
    print(f"\nbuildings: {N}\n")

    # ---- per-block dimensionality ----
    print("Per-block PCA (is the block itself multi-D?):")
    print(f"  {'block':10s} {'#cols':>5s} {'PC1%':>6s} {'PCs90':>6s} {'effDim(PR)':>11s}")
    accX, _ = mat(["ACCESS"])
    acc_pc1 = PCA(n_components=1).fit_transform(StandardScaler().fit_transform(accX)).ravel()
    needv = blocks["DEMO:needZ"]
    for b in BLOCK_NAMES:
        X, _ = mat([b])
        pc1, k90, pr = pca_summary(X)
        print(f"  {b:10s} {X.shape[1]:5d} {pc1:6.0f} {k90:6d} {pr:11.2f}")

    # ---- independence of each block from ACCESS and from need ----
    print("\nIndependence (|corr| of block's 1st PC with ACCESS axis / with need):")
    print(f"  {'block':10s} {'vs ACCESS':>10s} {'vs need':>9s}")
    for b in BLOCK_NAMES:
        X, _ = mat([b])
        bpc1 = PCA(n_components=1).fit_transform(X).ravel()
        ca = abs(np.corrcoef(bpc1, acc_pc1)[0, 1])
        cn = abs(np.corrcoef(bpc1, needv)[0, 1])
        flag = "  <- independent" if (ca < 0.3 and cn < 0.3) else ""
        print(f"  {b:10s} {ca:10.2f} {cn:9.2f}{flag}")

    # ---- combined + forward selection by effective dimensionality ----
    print("\nForward selection (add the block that most raises effective #dims):")
    chosen = ["ACCESS"]; remaining = [b for b in BLOCK_NAMES if b != "ACCESS"]
    X, _ = mat(chosen); pc1, k90, pr = pca_summary(X)
    print(f"  start ACCESS                         effDim={pr:.2f}  PC1={pc1:.0f}%  PCs90={k90}")
    while remaining:
        best = None
        for b in remaining:
            X, _ = mat(chosen + [b]); pr = participation_ratio(X)
            if best is None or pr > best[1]: best = (b, pr)
        chosen.append(best[0]); remaining.remove(best[0])
        X, _ = mat(chosen); pc1, k90, pr = pca_summary(X)
        print(f"  + {best[0]:10s} -> {'+'.join(chosen):28s} effDim={pr:.2f}  PC1={pc1:.0f}%  PCs90={k90}")

    # ---- full matrix verdict ----
    X, keys = mat(BLOCK_NAMES)
    pc1, k90, pr = pca_summary(X)
    print(f"\nALL blocks: {X.shape[1]} features, PC1={pc1:.0f}%, {k90} PCs for 90% var, "
          f"effective dims = {pr:.2f}")
    print("Reading: effDim ~1-2 = DR still a smear; ~3+ with PC1<45% = DR genuinely meaningful.")


if __name__ == "__main__":
    main()
