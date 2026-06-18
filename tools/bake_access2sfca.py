#!/usr/bin/env python
"""
bake_access2sfca.py — network-accurate Enhanced 2-Step Floating Catchment Area
(E2SFCA) accessibility, baked per city/mode for the FAVE app.

This is the Phase-2 *companion* metric to the headline gravity model. Where the
gravity model answers "how close are opportunities to me", 2SFCA answers "how
much supply is actually available to me once I account for everyone else
competing for it" (supply-to-demand crowding). Both are now NETWORK-accurate —
no straight-line. Distances come from a real OSMnx street/path graph.

Pipeline per (city, mode):
  1. Load buildings; estimate residential population (demand P_i) by distributing
     each DESO's population across its residential buildings by floor area —
     mirrors frontend/assets/js/models/epicityDemographics.js so demand matches
     the runtime model.
  2. Load POIs per category (supply locations j); capacity S_j from tags else 1.
  3. Build an OSMnx graph for the mode (walk/bike/drive), snap buildings + POIs
     to nearest graph nodes.
  4. Single-source Dijkstra from each POI (POIs are few) → network distance to
     every building inside the category catchment.
  5. Two-step E2SFCA with a Gaussian distance-decay W(d)=exp(-d^2/(2 sigma^2)):
       step 1  R_j = S_j / sum_i P_i W(d_ij)        (supply-to-demand ratio)
       step 2  A_i = sum_j R_j W(d_ij)              (building accessibility)

Output: frontend/assets/data/cities/<key>/access2sfca/<mode>.json
  { city, mode, baked_at, n, key:["lon,lat",...],
    cats:{ <cat>:{ catchment_m, sigma_m,
                   pois:[{id,lon,lat,cap,demand,R}, ...],
                   a:[[bldgIdx, A_i], ...] } },   # sparse: only reachable buildings
    params:{...} }
The `key` order matches `a`'s bldgIdx so runtime joins by coordinate (snap).

Honors FAVE's "no external APIs at runtime" rule — this is a bake tool; only it
talks to Overpass (via OSMnx).

Usage:
  python tools/bake_access2sfca.py vaxjo
  python tools/bake_access2sfca.py vaxjo --modes walking
  python tools/bake_access2sfca.py vaxjo --modes walking,cycling,driving --cats grocery,pharmacy
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import networkx as nx
import numpy as np
import osmnx as ox

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "frontend" / "assets" / "data"
CITIES = DATA / "cities"

# Building source per city (mirrors lib/cityPaths.js BUILDING_URL_BY_CITY_KEY).
BUILDING_FILE = {
    "vaxjo": "lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson",
    "malmo": "byggnad_malmo.geojson",
    "goteborg": "byggnad_goteborg.geojson",
    "norrkoping": "byggnad_norrkoping.geojson",
    "stockholm": "byggnad_stockholm.geojson",
    "uppsala": "byggnad_uppsala.geojson",
    "kalmar": "byggnad_kalmar.geojson",
}

MODE_NETWORK = {"walking": "walk", "cycling": "bike", "driving": "drive"}

# Per-category catchment (metres) for WALKING; scaled per mode below. These set
# how far a service "reaches": local groceries/kindergartens short, regional
# hospitals/universities long. sigma = catchment/3 (Gaussian truncated at ~3sigma).
CATCHMENT_WALK_M = {
    "grocery": 1200, "pharmacy": 1500, "healthcare_center": 2500, "dentistry": 2500,
    "kindergarten": 1200, "school_primary": 1500, "school_high": 2500,
    "hospital": 6000, "university": 5000, "veterinary": 4000,
}
MODE_CATCHMENT_FACTOR = {"walking": 1.0, "cycling": 3.0, "driving": 8.0}

CATS_ALL = list(CATCHMENT_WALK_M.keys())
SWEREF99 = 3006  # metric CRS for area / accurate centroids in Sweden


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def sig(x: float, n: int = 4) -> float:
    """Round to n significant figures (keeps JSON compact across 1e-7..1 ranges)."""
    if not x or not math.isfinite(x):
        return 0.0
    d = n - 1 - math.floor(math.log10(abs(x)))
    return round(x, d)


# ---- demand model (mirror of epicityDemographics.js) ----------------------

def _is_residential(props: dict) -> bool:
    a = str(props.get("andamal1") or "").lower()
    if a:
        return a.startswith("bostad")
    cat = str(props.get("category") or props.get("building") or props.get("objekttyp") or "").lower()
    return ("bostad" in cat or cat in ("residential", "house", "apartments", "detached"))


def _floors(props: dict) -> int:
    # Mirror _epiFloorAreaM2: read the same fields the runtime reads.
    height = 0.0
    for k in ("height", "Hojd", "building:height"):
        v = props.get(k)
        if v not in (None, ""):
            try:
                height = float(v); break
            except (TypeError, ValueError):
                pass
    levels = 0
    for k in ("building:levels", "floors"):
        v = props.get(k)
        if v not in (None, ""):
            try:
                levels = float(v); break
            except (TypeError, ValueError):
                pass
    if levels > 0:
        floors = int(levels)
    elif height > 0:
        floors = max(1, round(height / 3))
    else:
        floors = 1
    return min(floors, 40)


# ---- IO -------------------------------------------------------------------

def load_meta(city: str) -> dict:
    return json.loads((CITIES / city / "meta.json").read_text(encoding="utf-8"))


def load_pois(city: str, cat: str):
    """Return list of dicts {id, lon, lat, cap} for a category (Point POIs)."""
    fp = CITIES / city / "pois" / f"{cat}.geojson"
    if not fp.exists():
        return []
    fc = json.loads(fp.read_text(encoding="utf-8"))
    out = []
    for ft in fc.get("features", []):
        g = ft.get("geometry") or {}
        if g.get("type") != "Point":
            cs = np.array(g.get("coordinates") or []).reshape(-1, 2)
            if cs.size == 0:
                continue
            lon, lat = float(cs[:, 0].mean()), float(cs[:, 1].mean())
        else:
            lon, lat = float(g["coordinates"][0]), float(g["coordinates"][1])
        props = ft.get("properties") or {}
        tags = props.get("tags") or {}
        cap = 1.0
        for k in ("beds", "capacity", "capacity:persons"):
            v = tags.get(k)
            if v not in (None, ""):
                try:
                    cap = float(str(v).split(";")[0]); break
                except (TypeError, ValueError):
                    pass
        out.append({"id": props.get("id"), "lon": lon, "lat": lat, "cap": cap})
    return out


def build_demand(city: str):
    """Per-building (lon, lat, residential population P_i). Returns (lons, lats, P)."""
    import geopandas as gpd  # lazy import

    bpath = DATA / BUILDING_FILE[city]
    print(f"  [demand] reading buildings {bpath.name} ...", flush=True)
    gdf = gpd.read_file(bpath)
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    gdf = gdf.to_crs(4326)

    gm = gdf.to_crs(SWEREF99)
    area_m2 = gm.geometry.area.to_numpy()
    cent_m = gm.geometry.centroid
    cent = gpd.GeoSeries(cent_m, crs=SWEREF99).to_crs(4326)
    lons = cent.x.to_numpy()
    lats = cent.y.to_numpy()

    props = gdf.drop(columns=gdf.geometry.name).to_dict("records")
    residential = np.array([_is_residential(p) for p in props], dtype=bool)
    floors = np.array([_floors(p) for p in props], dtype=float)
    floor_area = np.where(residential, area_m2 * floors, 0.0)

    # Assign each building centroid to a DESO, distribute DESO pop by floor area.
    deso_path = CITIES / city / "demographics" / "deso.geojson"
    P = np.zeros(len(gdf))
    if deso_path.exists():
        deso = gpd.read_file(deso_path).to_crs(4326)[["deso", "pop", "geometry"]]
        pts = gpd.GeoDataFrame(
            {"i": np.arange(len(gdf))},
            geometry=gpd.points_from_xy(lons, lats), crs=4326,
        )
        joined = gpd.sjoin(pts, deso, predicate="within", how="left")
        # joined may duplicate rows if a point lands in overlapping polys; keep first.
        joined = joined[~joined.index.duplicated(keep="first")].sort_index()
        bdeso = joined["deso"].to_numpy()
        bpop = joined["pop"].to_numpy()
        # total residential floor area per DESO
        from collections import defaultdict
        tot = defaultdict(float)
        for i in range(len(gdf)):
            if residential[i] and isinstance(bdeso[i], str):
                tot[bdeso[i]] += floor_area[i]
        for i in range(len(gdf)):
            if residential[i] and isinstance(bdeso[i], str):
                t = tot[bdeso[i]]
                if t > 0 and bpop[i] and bpop[i] > 0:
                    P[i] = (floor_area[i] / t) * float(bpop[i])
    else:
        print("  [demand] WARNING: no deso.geojson — demand all zero", flush=True)

    print(f"  [demand] {len(gdf)} buildings, {int(residential.sum())} residential, "
          f"pop sum {P.sum():.0f}", flush=True)
    return lons, lats, P


# ---- E2SFCA core ----------------------------------------------------------

def gaussian_w(dist_m: np.ndarray, sigma_m: float) -> np.ndarray:
    return np.exp(-(dist_m * dist_m) / (2.0 * sigma_m * sigma_m))


def bake_mode(city: str, mode: str, cats: list[str], lons, lats, P, meta) -> dict:
    nt = MODE_NETWORK[mode]
    s, w, n, e = meta["bbox"]  # [minlat, minlon, maxlat, maxlon] = S, W, N, E
    print(f"  [graph] building OSMnx '{nt}' graph for bbox N{n} S{s} E{e} W{w} ...", flush=True)
    t0 = time.time()
    # osmnx 1.9.3 accepts bbox=(north, south, east, west) (positional north/south/
    # east/west are deprecated for v2). Pass the tuple to stay forward-compatible.
    G = ox.graph_from_bbox(bbox=(n, s, e, w),
                           network_type=nt, simplify=True, retain_all=False)
    G = ox.convert.to_undirected(G) if hasattr(ox, "convert") else G.to_undirected()
    print(f"  [graph] {G.number_of_nodes()} nodes, {G.number_of_edges()} edges "
          f"({time.time()-t0:.1f}s)", flush=True)

    # Snap all buildings to nearest nodes (vectorised) with snap offset.
    b_nodes, b_snap = ox.distance.nearest_nodes(
        G, X=lons.tolist(), Y=lats.tolist(), return_dist=True)
    b_nodes = np.asarray(b_nodes)
    b_snap = np.asarray(b_snap, dtype=float)

    # node -> list of building indices (with their snap offset)
    node_to_bldg: dict = {}
    for i, nd in enumerate(b_nodes):
        node_to_bldg.setdefault(nd, []).append(i)

    factor = MODE_CATCHMENT_FACTOR[mode]
    out_cats = {}

    for cat in cats:
        pois = load_pois(city, cat)
        if not pois:
            continue
        catch = CATCHMENT_WALK_M.get(cat, 1500) * factor
        sigma = catch / 3.0
        p_lon = [p["lon"] for p in pois]
        p_lat = [p["lat"] for p in pois]
        p_nodes, p_snap = ox.distance.nearest_nodes(
            G, X=p_lon, Y=p_lat, return_dist=True)
        p_nodes = np.atleast_1d(np.asarray(p_nodes))
        p_snap = np.atleast_1d(np.asarray(p_snap, dtype=float))

        # For each POI: Dijkstra (cutoff), gather reachable buildings + net dist.
        # pair_d[j] = dict bldgIdx -> network distance (keep the shortest if a
        # building snaps near several reachable nodes — it won't, one node each).
        poi_pairs = []  # list per poi of (bidx_array, dist_array)
        for j, pnode in enumerate(p_nodes):
            lengths = nx.single_source_dijkstra_path_length(
                G, pnode, cutoff=catch, weight="length")
            bidx_list = []
            dist_list = []
            offs = p_snap[j]
            for nd, L in lengths.items():
                blist = node_to_bldg.get(nd)
                if not blist:
                    continue
                for bi in blist:
                    d = offs + L + b_snap[bi]
                    if d <= catch:
                        bidx_list.append(bi)
                        dist_list.append(d)
            poi_pairs.append((np.asarray(bidx_list, dtype=np.int64),
                              np.asarray(dist_list, dtype=float)))

        # Step 1: supply-to-demand ratio R_j.
        R = np.zeros(len(pois))
        for j, (bidx, dist) in enumerate(poi_pairs):
            if bidx.size == 0:
                continue
            wj = gaussian_w(dist, sigma)
            demand = float(np.sum(P[bidx] * wj))
            R[j] = (pois[j]["cap"] / demand) if demand > 0 else 0.0

        # Step 2: building accessibility A_i = sum_j R_j W(d_ij).
        A = np.zeros(len(lons))
        for j, (bidx, dist) in enumerate(poi_pairs):
            if bidx.size == 0 or R[j] == 0:
                continue
            A[bidx] += R[j] * gaussian_w(dist, sigma)

        # store demand per poi too (for inspector)
        poi_out = []
        for j, p in enumerate(pois):
            bidx, dist = poi_pairs[j]
            demand = float(np.sum(P[bidx] * gaussian_w(dist, sigma))) if bidx.size else 0.0
            poi_out.append({
                "id": p["id"], "lon": round(p["lon"], 6), "lat": round(p["lat"], 6),
                "cap": p["cap"], "demand": round(demand, 1), "R": sig(float(R[j]), 4),
            })

        nz = np.nonzero(A > 0)[0]
        a_sparse = [[int(i), sig(float(A[i]), 4)] for i in nz]
        out_cats[cat] = {
            "catchment_m": round(catch, 1), "sigma_m": round(sigma, 1),
            "n_pois": len(pois), "n_reachable": int(nz.size),
            "pois": poi_out, "a": a_sparse,
        }
        print(f"    [{cat}] {len(pois)} pois, catch {catch:.0f}m, "
              f"{nz.size}/{len(lons)} buildings reachable", flush=True)

    return {
        "city": city, "mode": mode, "baked_at": _now_iso(), "n": len(lons),
        "cats": out_cats,
        "params": {
            "metric": "E2SFCA",
            "decay": "gaussian W=exp(-d^2/(2 sigma^2)), sigma=catchment/3",
            "network": f"OSMnx {ox.__version__} ({nt}, undirected)",
            "demand": "DESO pop distributed to residential buildings by floor area",
            "capacity_note": "S_j from POI beds/capacity tags else 1 (mostly uniform)",
            "catchment_factor": factor,
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("city")
    ap.add_argument("--modes", default="walking,cycling,driving")
    ap.add_argument("--cats", default=",".join(CATS_ALL))
    args = ap.parse_args()

    city = args.city
    if city not in BUILDING_FILE:
        sys.exit(f"unknown city '{city}' (known: {', '.join(BUILDING_FILE)})")
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    cats = [c.strip() for c in args.cats.split(",") if c.strip()]

    meta = load_meta(city)
    lons, lats, P = build_demand(city)

    outdir = CITIES / city / "access2sfca"
    outdir.mkdir(parents=True, exist_ok=True)

    # Shared building-coordinate index (join key for runtime, like routing/index.json).
    # The `a` sparse arrays in each mode file reference these positions by bldgIdx.
    keys = [f"{round(float(lons[i]),6)},{round(float(lats[i]),6)}" for i in range(len(lons))]
    idx = {
        "city": city, "baked_at": _now_iso(), "n": len(keys),
        "key_format": "lon,lat", "key_decimals": 6, "key": keys,
        "modes": modes, "cats": cats,
    }
    (outdir / "index.json").write_text(json.dumps(idx, separators=(",", ":")), encoding="utf-8")

    for mode in modes:
        if mode not in MODE_NETWORK:
            print(f"skip unknown mode '{mode}'", flush=True); continue
        print(f"\n=== {city} / {mode} ===", flush=True)
        t0 = time.time()
        result = bake_mode(city, mode, cats, lons, lats, P, meta)
        fp = outdir / f"{mode}.json"
        fp.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
        kb = fp.stat().st_size / 1024
        print(f"  -> wrote {fp.relative_to(ROOT)} ({kb:.0f} KB, {time.time()-t0:.1f}s)", flush=True)


if __name__ == "__main__":
    main()
