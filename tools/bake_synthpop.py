#!/usr/bin/env python
"""
bake_synthpop.py — bake a SYNTHETIC per-building residential population directly
onto the runtime (Lantmäteriet `byggnad`) building footprints, EpiCity-style.

Why this exists / what changed
------------------------------
The runtime renders Lantmäteriet `byggnad` footprints. The OLD version of this
script transferred EpiCity's pre-computed per-building population from
`cities/<key>/epicity/city.json` onto the byggnad set by nearest-neighbour. That
had two problems: (1) `city.json` is an OSM-derived building set that is NOT in
the repo, so the bake could not be reproduced; and (2) any byggnad footprint more
than SNAP_M from an OSM building got NO population, stranding ~20% of residents
(Växjö: only 78,506 of 98,879 assigned).

This version GENERATES the synthetic population the way EpiCity does — a
dasymetric (areal-interpolation) model — using only data we have in the repo:

  1. Each DESO zone carries a real SCB resident total (`pop` in
     demographics/deso.geojson). That is the control total we must reproduce.
  2. Each byggnad footprint is classified residential / accessory / other from
     its Lantmäteriet purpose (`andamal1`), and given an EpiCity land-use zone
     (RES_LO / RES_MED / RES_HI) from its typology + storeys.
  3. A capacity weight = floor_area / zone_density (m² of floor space per
     resident) is computed per residential building — exactly EpiCity's occupancy
     model (osm.py `_DENSITY_M2` / `estimate_population`). Apartment blocks
     (RES_HI, 14 m²/person) therefore hold ~2.5× the residents per m² of detached
     houses (RES_LO, 35 m²/person).
  4. Within each DESO, the SCB resident total is distributed across that DESO's
     residential buildings in proportion to the capacity weight, with
     largest-remainder integer rounding so the per-DESO sum is reproduced EXACTLY
     (mirrors EpiCity osm.py largest-remainder allocation).

This gives COMPLETE residential coverage and exact DESO control totals, with no
dependency on city.json. Output schema is unchanged, so the runtime
(lib/synthpop.js) consumes it with no changes.

Per-building fields:
  pop      synthetic residents (DESO total, density-weighted, exact per DESO)
  levels   storeys (from height_m where available, else typology default)
  area_m2  footprint area (local-projection shoelace, m²)
  zone     EpiCity land-use zone (1=RES_LO, 2=RES_MED, 3=RES_HI)
  inc      synthetic per-building income (kSEK, DESO-mean-preserving lognormal)
  nz       synthetic per-building needZ (recomputed from synthetic income)
  de       DESO code (join key for the rest of the SCB fields: age, gender, …,
           which are only resolved at DESO level by SCB — see deso.geojson)

Output: frontend/assets/data/cities/<key>/synthpop/buildings.json
  { meta:{...}, k:["lon,lat",...], pop:[...], lv:[...], ar:[...], zn:[...],
    inc:[...], nz:[...], de:[...] }   (parallel arrays keyed by byggnad centroid;
  one entry per residential footprint inside a DESO.)

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

from shapely.geometry import shape, Point
from shapely.strtree import STRtree

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

# --- EpiCity land-use zones + occupancy density (epicity_engine/osm.py) --------
RES_LO, RES_MED, RES_HI = 1, 2, 3
# m² of floor area per resident, by zone (osm.py `_DENSITY_M2`).
DENSITY_M2 = {RES_LO: 35.0, RES_MED: 22.0, RES_HI: 14.0}
METERS_PER_STOREY = 3.0        # height_m -> storeys (Swedish floor-to-floor ~3 m)
MAX_STOREYS = 40

# Default storeys per typology where no height is available (6 of 7 byggnad files
# carry no height field — only Lantmäteriet's vaxjo extract has height_m).
DEFAULT_STOREYS = {RES_LO: 2, RES_MED: 3, RES_HI: 4}

# --- Synthetic income model (unchanged from the NN-transfer version) -----------
# SCB only publishes a single DESO mean income, so a per-building income must be
# SYNTHESIZED. We draw a deterministic, mean-PRESERVING lognormal spread around
# the building's DESO mean (a synthetic-population assumption, NOT measured), so
# the DESO pop-weighted mean is reproduced exactly while individual buildings
# vary. Per-building needZ is then recomputed from that synthetic income using
# the same weight the DESO need index uses (tools/bake_demographics.py).
INCOME_SYNTH_SIGMA = 0.30      # within-DESO log-income spread (modeling assumption)
NEED_W_INCOME = 0.30           # WEIGHTS["income"] in bake_demographics.py

# --- Synthetic per-building demographics ---------------------------------------
# SCB publishes age/gender/education shares only per DESO, so the 7 demographic
# axes would otherwise carry one identical value for every building in a zone
# (≤53 distinct lines in the PCP). We SYNTHESIZE a per-building value the way a
# real population is drawn: each building's share is a sample around its DESO
# share with binomial-style sampling spread (std = sqrt(p(1-p)/N)), so bigger
# buildings sit closer to the DESO mean and small ones vary more — then rescaled
# (pop-weighted) so the DESO mean is reproduced EXACTLY. This is a synthetic-
# population assumption (NOT measured), parallel to the income synthesis above.
# (deso prop, output key, is_percent)
SYNTH_DEMO_FIELDS = [
    ("child_frac",     "ch",  False),
    ("elder_frac",     "el",  False),
    ("male_frac",      "ml",  False),
    ("higher_ed",      "he",  True),
    ("neet",           "nt",  True),
    ("income_support", "iss", True),
]


def _det_normal(key):
    """Deterministic standard-normal from a string key (Box-Muller over two
    MD5-derived uniforms) — reproducible, no RNG state."""
    h = hashlib.md5(key.encode("utf-8")).digest()
    u1 = int.from_bytes(h[:8], "big") / 2 ** 64
    u2 = int.from_bytes(h[8:16], "big") / 2 ** 64
    u1 = min(max(u1, 1e-12), 1 - 1e-12)
    return math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)


def _load_deso(city):
    """Return (props_by_code, income_sigma, shapely_polys, codes_parallel).

    props_by_code: DESO code -> baked socioeconomic props (incl. real `pop`).
    income_sigma : cross-DESO income stdev (matches pstdev z-scoring in
                   bake_demographics.py) for the synthetic-need recompute.
    polys / codes: parallel lists for point-in-polygon DESO assignment.
    """
    f = os.path.join(DATA, "cities", city, "demographics", "deso.geojson")
    if not os.path.exists(f):
        return {}, 0.0, [], []
    fc = json.load(open(f, encoding="utf-8"))
    props, incomes, polys, codes = {}, [], [], []
    for ft in fc.get("features", []):
        p = ft.get("properties") or {}
        code = p.get("deso")
        if code is None:
            continue
        props[code] = p
        if p.get("income") is not None:
            incomes.append(float(p["income"]))
        geom = ft.get("geometry")
        if geom:
            try:
                polys.append(shape(geom)); codes.append(code)
            except Exception:
                pass
    sigma = statistics.pstdev(incomes) if len(incomes) > 1 else 0.0
    return props, sigma, polys, codes


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
                sx += pt[0]; sy += pt[1]; n += 1
    if not n:
        return None
    return (sx / n, sy / n)


def _ring_area_m2(ring, lat0):
    """Shoelace area (m²) of one ring under a local equirectangular projection."""
    kx = 111320.0 * math.cos(math.radians(lat0))
    ky = 110540.0
    s = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i][0] * kx, ring[i][1] * ky
        x2, y2 = ring[i + 1][0] * kx, ring[i + 1][1] * ky
        s += x1 * y2 - x2 * y1
    return abs(s) * 0.5


def footprint_area_m2(geom, lat0):
    """Footprint area in m² (outer rings minus holes), local-projection shoelace."""
    t = geom.get("type")
    coords = geom.get("coordinates")
    polys = [coords] if t == "Polygon" else (coords if t == "MultiPolygon" else [])
    area = 0.0
    for poly in polys:
        if not poly:
            continue
        area += _ring_area_m2(poly[0], lat0)                 # outer ring
        for hole in poly[1:]:
            area -= _ring_area_m2(hole, lat0)                # subtract holes
    return max(0.0, area)


def _height_m(props):
    """Building height in metres if present (only Lantmäteriet vaxjo extract),
    else 0."""
    for key in ("height_m", "height", "Hojd", "building:height"):
        v = props.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return 0.0


def classify_residential(props, height_m):
    """Map a byggnad footprint to (zone, storeys) or (None, 0) if not residential.

    Residential typologies come from Lantmäteriet `andamal1` (= "Bostad;…").
    Accessory buildings (Komplementbyggnad — garages/sheds) and all
    non-residential purposes carry no residents. Storeys come from height_m where
    available, otherwise a per-typology default."""
    a = str(props.get("andamal1") or "").lower()
    if "bostad" not in a:
        return None, 0

    # storeys: height where we have it, else fill in once the zone is known
    storeys_from_h = (max(1, min(MAX_STOREYS, round(height_m / METERS_PER_STOREY)))
                      if height_m > 0 else 0)

    if "småhus" in a or "smahus" in a:
        # detached / terraced / row houses + the "flera lägenheter" small-house —
        # low-rise regardless of measured height.
        zone = RES_MED if ("flera lägenheter" in a or "flera lagenheter" in a) else RES_LO
    elif "flerfamilj" in a:
        # apartment block — RES_HI once it is genuinely tall, else mid-rise.
        zone = RES_HI if storeys_from_h >= 4 else RES_MED
    else:
        # generic "Bostad;" — fall back to EpiCity's level rule.
        zone = (RES_HI if storeys_from_h >= 4
                else RES_MED if storeys_from_h >= 2 else RES_LO)

    storeys = storeys_from_h or DEFAULT_STOREYS[zone]
    return zone, storeys


def _sigmoid(x):
    if x >= 0:
        z = math.exp(-x); return 1.0 / (1.0 + z)
    z = math.exp(x); return z / (1.0 + z)


def _synth_shares(base, residents, keys, salt, is_percent):
    """Synthesize a per-building share around a DESO `base` value.

    LOGIT-normal sampling: each building's share = sigmoid(logit(base) + σ·z),
    with σ = the binomial logit-scale standard error √(1/(N·p·(1-p))) — so the
    spread is tied to how many residents the building has (a 2-person house can
    plausibly be 0–80% children; a 200-resident block sits tight on the DESO
    mean). Unlike an additive Gaussian, the sigmoid keeps every value strictly
    INSIDE (0,1) — NOTHING piles up on the 0/1 boundary, so the PCP axis shows a
    smooth per-building cloud instead of a fat line on the floor. A single
    logit-space SHIFT (bisection) makes the pop-weighted mean reproduce the DESO
    value exactly. Returns a list aligned with `residents`/`keys` (×100 if
    `is_percent`); Nones if `base` is missing."""
    n = len(residents)
    if base is None:
        return [None] * n
    p = float(base) / 100.0 if is_percent else float(base)
    p = min(max(p, 1e-4), 1.0 - 1e-4)
    L = math.log(p / (1.0 - p))
    base_logit = []
    for key, res in zip(keys, residents):
        sigma = min(math.sqrt(1.0 / (max(1, res) * p * (1.0 - p))), 2.5)
        base_logit.append(L + sigma * _det_normal(key + "#" + salt))
    wsum = sum(residents)
    # mean-preserving logit shift: find δ so Σ wᵢ·sigmoid(logitᵢ+δ) / Σwᵢ == p
    delta = 0.0
    if wsum > 0:
        def wmean(d):
            return sum(_sigmoid(base_logit[i] + d) * residents[i]
                       for i in range(n)) / wsum
        lo, hi = -8.0, 8.0
        for _ in range(50):
            mid = (lo + hi) / 2.0
            if wmean(mid) < p:
                lo = mid
            else:
                hi = mid
        delta = (lo + hi) / 2.0
    vals = [_sigmoid(base_logit[i] + delta) for i in range(n)]
    return [round(v * 100.0, 2) if is_percent else round(v, 4) for v in vals]


def _largest_remainder(total, weights):
    """Distribute integer `total` across buckets in proportion to `weights`,
    rounding by largest remainder so the parts sum to `total` exactly."""
    n = len(weights)
    wsum = sum(weights)
    if total <= 0 or wsum <= 0 or n == 0:
        return [0] * n
    raw = [total * w / wsum for w in weights]
    base = [int(math.floor(r)) for r in raw]
    rem = total - sum(base)
    if rem > 0:
        order = sorted(range(n), key=lambda i: raw[i] - base[i], reverse=True)
        for i in order[:rem]:
            base[i] += 1
    return base


def bake_city(city):
    bfile = BUILDING_FILE.get(city)
    bpath = os.path.join(DATA, bfile) if bfile else None
    if not bpath or not os.path.exists(bpath):
        print(f"[{city}] missing byggnad file — skip"); return False

    deso_props, sigma_income, polys, codes = _load_deso(city)
    if not polys:
        print(f"[{city}] no demographics/deso.geojson DESO polygons — skip"); return False
    tree = STRtree(polys)

    feats = json.load(open(bpath, encoding="utf-8")).get("features", [])
    if not feats:
        print(f"[{city}] byggnad file has no features — skip"); return False
    lat0 = 0.0
    # quick lat0 estimate from a sample of centroids for the local projection
    sample = []
    for f in feats[: min(2000, len(feats))]:
        c = turf_centroid(f.get("geometry") or {})
        if c:
            sample.append(c[1])
    lat0 = sum(sample) / len(sample) if sample else 60.0

    # --- per-building residential records ---
    # rec: dict(key, area, zone, storeys, weight); grouped by DESO code
    by_deso = defaultdict(list)
    n_res = n_outside = 0
    for f in feats:
        geom = f.get("geometry") or {}
        props = f.get("properties") or {}
        zone, storeys = classify_residential(props, _height_m(props))
        if zone is None:
            continue                                   # accessory / non-residential
        c = turf_centroid(geom)
        if not c:
            continue
        # assign to DESO (point-in-polygon)
        pt = Point(c[0], c[1])
        code = None
        for idx in tree.query(pt):
            if polys[idx].contains(pt):
                code = codes[idx]; break
        if code is None:
            n_outside += 1
            continue                                   # outside every DESO — no control total
        area = footprint_area_m2(geom, lat0)
        if area <= 0:
            continue
        weight = (area * storeys) / DENSITY_M2[zone]    # EpiCity occupancy capacity
        by_deso[code].append({
            "key": f"{c[0]:.6f},{c[1]:.6f}",
            "area": area, "zone": zone, "storeys": storeys, "weight": weight,
        })
        n_res += 1

    # --- distribute each DESO's SCB resident total by capacity weight ---
    k, pop, lv, ar, zn, inc, nz, de = [], [], [], [], [], [], [], []
    # synthetic per-building demographic shares (parallel to k)
    dch, dele, dml, dhe, dnt, diss, ddp = [], [], [], [], [], [], []
    assigned_total = 0
    control_total = 0          # sum over DESOs that HAVE residential buildings
    n_income = 0
    # full DESO universe (incl. rural DESOs with no footprints in this byggnad
    # extract) — for transparency on what fraction of the kommun is placeable.
    deso_universe_pop = sum(int(round(float((p.get("pop") or 0))))
                            for p in deso_props.values())
    covered_desos = set(by_deso.keys())
    for code, recs in by_deso.items():
        dp = deso_props.get(code) or {}
        total = int(round(float(dp.get("pop") or 0)))
        control_total += total
        weights = [r["weight"] for r in recs]
        residents = _largest_remainder(total, weights)

        # synthetic income: mean-preserving lognormal around DESO mean income,
        # rescaled so the pop-weighted DESO mean is reproduced exactly.
        mean_inc = dp.get("income")
        need_deso = dp.get("needZ")
        mults = [math.exp(INCOME_SYNTH_SIGMA * _det_normal(r["key"])) for r in recs]
        wpop = sum(residents)
        mbar = (sum(m * p for m, p in zip(mults, residents)) / wpop) if wpop > 0 else 0.0

        # synthetic per-building demographic shares (one list per attribute)
        keys = [r["key"] for r in recs]
        syn = {ok: _synth_shares(dp.get(prop), residents, keys, ok, is_pct)
               for prop, ok, is_pct in SYNTH_DEMO_FIELDS}
        # dependency derived from the building's OWN synthetic child+elder shares
        dep_syn = []
        for ci, ei in zip(syn["ch"], syn["el"]):
            if ci is None or ei is None:
                dep_syn.append(None); continue
            s = min(ci + ei, 0.97)
            dep_syn.append(round(s / max(1e-3, 1.0 - s), 4))

        for i, (r, p, m) in enumerate(zip(recs, residents, mults)):
            k.append(r["key"]); pop.append(p)
            lv.append(int(r["storeys"])); ar.append(round(r["area"], 1))
            zn.append(int(r["zone"])); de.append(code)
            dch.append(syn["ch"][i]); dele.append(syn["el"][i]); dml.append(syn["ml"][i])
            dhe.append(syn["he"][i]); dnt.append(syn["nt"][i]); diss.append(syn["iss"][i])
            ddp.append(dep_syn[i])
            assigned_total += p
            if mean_inc is not None and mbar > 0:
                inc_i = float(mean_inc) * m / mbar          # mean-preserving
                inc.append(round(inc_i, 1))
                if need_deso is not None and sigma_income > 0:
                    nz.append(round(float(need_deso)
                                    + NEED_W_INCOME * (float(mean_inc) - inc_i) / sigma_income, 4))
                else:
                    nz.append(None)
                n_income += 1
            else:
                inc.append(None); nz.append(None)

    out = {
        "meta": {
            "city": city,
            "model": "dasymetric synthetic population (EpiCity occupancy density, "
                     "DESO control totals, largest-remainder allocation)",
            "residential_buildings": n_res,
            "buildings_outside_deso": n_outside,
            "byggnad_with_data": len(k),
            "byggnad_with_income": n_income,
            "control_pop_total": control_total,
            "assigned_pop_total": assigned_total,
            "deso_universe_pop": deso_universe_pop,
            "desos_total": len(deso_props),
            "desos_covered": len(covered_desos),
            "desos_uncovered": len(deso_props) - len(covered_desos),
            "uncovered_pop": deso_universe_pop - control_total,
            "density_m2_per_person": {"RES_LO": DENSITY_M2[RES_LO],
                                      "RES_MED": DENSITY_M2[RES_MED],
                                      "RES_HI": DENSITY_M2[RES_HI]},
            "meters_per_storey": METERS_PER_STOREY,
            "income_synth_sigma": INCOME_SYNTH_SIGMA,
            "sigma_income_deso": round(sigma_income, 2),
            "source": "byggnad footprints (Lantmäteriet andamal1/height) + SCB DESO "
                      "control totals; population dasymetrically allocated by EpiCity "
                      "floor-area/density; income/needZ + 7 demographic shares "
                      "synthesized per building (mean-preserving around DESO mean)",
            "synth_demo_fields": "ch=child_frac el=elder_frac dp=dependency "
                      "he=higher_ed% nt=neet% iss=income_support% ml=male_frac "
                      "(sampling spread around DESO share, pop-weighted mean preserved)",
        },
        "k": k, "pop": pop, "lv": lv, "ar": ar, "zn": zn,
        "inc": inc, "nz": nz, "de": de,
        # synthetic per-building demographic shares (parallel to k)
        "ch": dch, "el": dele, "dp": ddp, "he": dhe, "nt": dnt, "iss": diss, "ml": dml,
    }
    outdir = os.path.join(DATA, "cities", city, "synthpop")
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "buildings.json")
    with open(outpath, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, separators=(",", ":"))
    sz = os.path.getsize(outpath) / 1e6
    print(f"[{city}] OK  residential={len(k)}  with_income={n_income}  "
          f"pop_assigned={assigned_total}/{control_total} (exact per DESO)  "
          f"outside_deso={n_outside}  file={sz:.2f} MB")
    return True


def main():
    cities = [c for c in sys.argv[1:]] or ALL_CITIES
    ok = 0
    for c in cities:
        try:
            if bake_city(c):
                ok += 1
        except Exception as e:
            import traceback
            print(f"[{c}] FAILED: {e}")
            traceback.print_exc()
    print(f"\nDone: {ok}/{len(cities)} cities baked.")


if __name__ == "__main__":
    main()
