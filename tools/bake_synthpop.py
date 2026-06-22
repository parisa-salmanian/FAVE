#!/usr/bin/env python
"""
bake_synthpop.py — bake EpiCity SYNTHETIC per-building data onto the runtime
(Lantmäteriet `byggnad`) building footprints.

Why this exists
---------------
The runtime renders Lantmäteriet `byggnad` buildings, but the EpiCity synthetic
population lives on a *different* (OSM-derived) building set in
`cities/<key>/epicity/city.json` — no shared id. So we spatially transfer the
synthetic attributes onto the byggnad footprints once, at bake time, and emit a
compact file the runtime joins by building centroid (exact, turf.centroid-style
key) — mirroring lib/access2sfca.js / lib/routingMatrix.js coordinate snapping.

Synthetic fields transferred per building:
  pop      synthetic residents  (population total is PRESERVED: each OSM
           building's pop is given to its single nearest byggnad footprint)
  levels   building storeys     (nearest OSM building, descriptive)
  area_m2  footprint area       (nearest OSM building, descriptive)
  zone     EpiCity land-use zone code (nearest OSM building, descriptive)

Output: frontend/assets/data/cities/<key>/synthpop/buildings.json
  { meta:{...}, k:["lon,lat",...], pop:[...], lv:[...], ar:[...], zn:[...] }
  (parallel arrays keyed by byggnad centroid; only footprints with a match.)

Usage:  python tools/bake_synthpop.py [city ...]   (default: all 7)

Bake-time only — honours FAVE's "no external APIs at runtime" rule.
"""
import hashlib
import json
import math
import os
import statistics
import sys
from collections import defaultdict

import numpy as np
from scipy.spatial import cKDTree

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "frontend", "assets", "data")

# Mirror frontend/assets/js/lib/cityPaths.js BUILDING_URLS.
BUILDING_FILE = {
    "vaxjo": "lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson",
    "malmo": "byggnad_malmo.geojson",
    "goteborg": "byggnad_goteborg.geojson",
    "norrkoping": "byggnad_norrkoping.geojson",
    "stockholm": "byggnad_stockholm.geojson",
    "uppsala": "byggnad_uppsala.geojson",
    "kalmar": "byggnad_kalmar.geojson",
}
ALL_CITIES = list(BUILDING_FILE.keys())
SNAP_M = 40.0          # OSM building <-> byggnad footprint match tolerance (m)

# --- Synthetic income model ---------------------------------------------------
# SCB only publishes a single DESO mean income, so a per-building income must be
# SYNTHESIZED. We draw a deterministic, mean-PRESERVING lognormal spread around
# the building's DESO mean (a synthetic-population assumption, NOT measured), so
# the DESO pop-weighted mean is reproduced exactly while individual buildings
# vary. Per-building needZ is then recomputed from that synthetic income using
# the same weight the DESO need index uses (tools/bake_demographics.py).
INCOME_SYNTH_SIGMA = 0.30      # within-DESO log-income spread (modeling assumption)
NEED_W_INCOME = 0.30           # WEIGHTS["income"] in bake_demographics.py


def _det_normal(key):
    """Deterministic standard-normal in [-inf,inf] from a string key (Box-Muller
    over two MD5-derived uniforms) — reproducible, no RNG state."""
    h = hashlib.md5(key.encode("utf-8")).digest()
    u1 = int.from_bytes(h[:8], "big") / 2 ** 64
    u2 = int.from_bytes(h[8:16], "big") / 2 ** 64
    u1 = min(max(u1, 1e-12), 1 - 1e-12)
    return math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)


def _load_deso_props(city):
    """DESO code -> baked socioeconomic props, plus the cross-DESO income stdev
    (matches the pstdev z-scoring in bake_demographics.py)."""
    f = os.path.join(DATA, "cities", city, "demographics", "deso.geojson")
    if not os.path.exists(f):
        return {}, 0.0
    fc = json.load(open(f, encoding="utf-8"))
    props = {}
    incomes = []
    for ft in fc.get("features", []):
        p = ft.get("properties") or {}
        code = p.get("deso")
        if code is None:
            continue
        props[code] = p
        if p.get("income") is not None:
            incomes.append(float(p["income"]))
    sigma = statistics.pstdev(incomes) if len(incomes) > 1 else 0.0
    return props, sigma


def turf_centroid(geom):
    """Mean of EVERY coordinate in the geometry, incl. the closing point of each
    ring — byte-for-byte the same as turf.centroid used at runtime, so the
    6-dp key matches exactly."""
    sx = sy = 0.0
    n = 0
    t = geom.get("type")
    coords = geom.get("coordinates")
    if t == "Polygon":
        polys = [coords]
    elif t == "MultiPolygon":
        polys = coords
    else:
        return None
    for poly in polys:
        for ring in poly:
            for pt in ring:
                sx += pt[0]
                sy += pt[1]
                n += 1
    if not n:
        return None
    return (sx / n, sy / n)


def to_meters(lon, lat, lat0):
    """Local equirectangular projection to metres for KD-tree distances."""
    x = lon * 111320.0 * math.cos(math.radians(lat0))
    y = lat * 110540.0
    return x, y


def bake_city(city):
    bfile = BUILDING_FILE.get(city)
    if not bfile:
        print(f"[{city}] no building file mapping — skip"); return False
    bpath = os.path.join(DATA, bfile)
    cpath = os.path.join(DATA, "cities", city, "epicity", "city.json")
    if not os.path.exists(bpath):
        print(f"[{city}] missing byggnad file {bfile} — skip"); return False
    if not os.path.exists(cpath):
        print(f"[{city}] missing epicity/city.json — skip"); return False

    # --- per-DESO socioeconomics (for synthetic income / needZ) ---
    deso_props, sigma_income = _load_deso_props(city)

    # --- synthetic (OSM) buildings ---
    syn = json.load(open(cpath, encoding="utf-8")).get("buildings", [])
    osm = [b for b in syn if b.get("lat") is not None and b.get("lon") is not None]
    if not osm:
        print(f"[{city}] city.json has no usable buildings — skip"); return False
    lat0 = sum(b["lat"] for b in osm) / len(osm)
    # bbox of synthetic buildings (+~500 m margin) to restrict byggnad scan
    lons = [b["lon"] for b in osm]; lats = [b["lat"] for b in osm]
    mlon = 0.5 / (111.320 * math.cos(math.radians(lat0)))
    mlat = 0.5 / 110.540
    bb = (min(lons) - mlon, min(lats) - mlat, max(lons) + mlon, max(lats) + mlat)

    # --- byggnad footprints (centroids), restricted to bbox ---
    bjson = json.load(open(bpath, encoding="utf-8"))
    bcent = []   # (lon, lat)
    for f in bjson.get("features", []):
        c = turf_centroid(f.get("geometry") or {})
        if not c:
            continue
        if not (bb[0] <= c[0] <= bb[2] and bb[1] <= c[1] <= bb[3]):
            continue
        bcent.append(c)
    if not bcent:
        print(f"[{city}] no byggnad footprints in bbox — skip"); return False

    bxy = np.array([to_meters(lon, lat, lat0) for lon, lat in bcent])
    tree = cKDTree(bxy)

    n_b = len(bcent)
    pop = np.zeros(n_b)
    # descriptive attrs (from nearest OSM building, may be overwritten by closer)
    lv = np.full(n_b, np.nan)
    ar = np.full(n_b, np.nan)
    zn = np.full(n_b, np.nan)
    deso_of = [None] * n_b               # DESO code from the closest OSM building
    best_d = np.full(n_b, np.inf)

    assigned_pop = 0.0
    matched_osm = 0
    for b in osm:
        qx, qy = to_meters(b["lon"], b["lat"], lat0)
        d, j = tree.query([qx, qy], k=1)
        if d > SNAP_M:
            continue
        matched_osm += 1
        p = float(b.get("pop") or 0)
        pop[j] += p                        # population: 1 OSM -> 1 byggnad (total preserved)
        assigned_pop += p
        if d < best_d[j]:                  # attrs: take the closest OSM building
            best_d[j] = d
            lv[j] = b.get("levels") if b.get("levels") is not None else np.nan
            ar[j] = b.get("area_m2") if b.get("area_m2") is not None else np.nan
            zn[j] = b.get("zone") if b.get("zone") is not None else np.nan
            deso_of[j] = b.get("deso")

    # keep only footprints that received something
    keep = [i for i in range(n_b) if pop[i] > 0 or not math.isnan(lv[i])]
    k = [f"{bcent[i][0]:.6f},{bcent[i][1]:.6f}" for i in keep]

    # --- synthesize per-building income + needZ (residents-bearing footprints) ---
    # Group the kept footprints by DESO, draw mean-preserving lognormal income
    # multipliers, rescale so each DESO's pop-weighted mean reproduces the SCB
    # mean, then recompute needZ from the building's income deviation.
    inc = [None] * len(keep)
    nz = [None] * len(keep)
    de = [deso_of[i] for i in keep]
    pos_of = {i: p for p, i in enumerate(keep)}
    groups = defaultdict(list)
    for i in keep:
        if pop[i] > 0 and deso_of[i] is not None:
            groups[deso_of[i]].append(i)
    n_income = 0
    for code, idxs in groups.items():
        dp = deso_props.get(code)
        if not dp or dp.get("income") is None:
            continue
        mean_inc = float(dp["income"])
        need_deso = dp.get("needZ")
        mults = {i: math.exp(INCOME_SYNTH_SIGMA * _det_normal(k[pos_of[i]])) for i in idxs}
        wsum = sum(pop[i] for i in idxs)
        mbar = (sum(mults[i] * pop[i] for i in idxs) / wsum) if wsum > 0 else 0.0
        if mbar <= 0:
            continue
        for i in idxs:
            inc_i = mean_inc * mults[i] / mbar          # mean-preserving
            p = pos_of[i]
            inc[p] = round(inc_i, 1)
            if need_deso is not None and sigma_income > 0:
                # lower income -> higher need (same 0.30 income weight as the DESO index)
                nz[p] = round(float(need_deso) + NEED_W_INCOME * (mean_inc - inc_i) / sigma_income, 4)
            n_income += 1
    out = {
        "meta": {
            "city": city,
            "snap_m": SNAP_M,
            "osm_buildings": len(osm),
            "osm_matched": matched_osm,
            "synth_pop_total": round(sum(float(b.get("pop") or 0) for b in osm), 1),
            "synth_pop_assigned": round(assigned_pop, 1),
            "byggnad_with_data": len(keep),
            "byggnad_with_income": n_income,
            "income_synth_sigma": INCOME_SYNTH_SIGMA,
            "sigma_income_deso": round(sigma_income, 2),
            "source": "epicity/city.json synthetic population, NN-transferred to byggnad footprints; "
                      "income/needZ synthesized (mean-preserving lognormal around DESO mean)",
        },
        "k": k,
        "pop": [round(float(pop[i]), 2) for i in keep],
        "lv": [None if math.isnan(lv[i]) else int(lv[i]) for i in keep],
        "ar": [None if math.isnan(ar[i]) else round(float(ar[i]), 1) for i in keep],
        "zn": [None if math.isnan(zn[i]) else int(zn[i]) for i in keep],
        "inc": inc,        # synthetic per-building income (kSEK, DESO-mean-preserving)
        "nz": nz,          # synthetic per-building needZ (recomputed from income)
        "de": de,          # DESO code (for cross-referencing the rest of the SCB fields)
    }
    outdir = os.path.join(DATA, "cities", city, "synthpop")
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "buildings.json")
    with open(outpath, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, separators=(",", ":"))
    sz = os.path.getsize(outpath) / 1e6
    print(f"[{city}] OK  byggnad_with_data={len(keep)}  with_income={n_income}  "
          f"osm_matched={matched_osm}/{len(osm)}  "
          f"pop_assigned={assigned_pop:.0f}/{out['meta']['synth_pop_total']:.0f}  "
          f"file={sz:.2f} MB")
    return True


def main():
    cities = [c for c in sys.argv[1:]] or ALL_CITIES
    ok = 0
    for c in cities:
        try:
            if bake_city(c):
                ok += 1
        except Exception as e:
            print(f"[{c}] FAILED: {e}")
    print(f"\nDone: {ok}/{len(cities)} cities baked.")


if __name__ == "__main__":
    main()
