"""Bake public-transport access matrices for FAVE using a LOCAL r5py router.

Runtime contract (same as bake_routing.py): the FAVE app must never call a
routing API during normal use. This script precomputes, for every building, the
nearest POIs of each category by real door-to-door PUBLIC-TRANSPORT travel time,
and writes the results to disk under frontend/assets/data/. The running app then
reads those files — no Java, no r5py, no internet at request time.

Why a separate baker: OSRM (tools/bake_routing.py) routes over the road network
and has no concept of timetables. Transit is time-dependent — it needs GTFS
schedules and a transit-aware router. We use r5py (a Python wrapper around
Conveyal R5), which produces a building x POI travel-time matrix from a GTFS feed
plus the OSM street network (for the walk access/egress legs).

Scenario: a single representative WEEKDAY MORNING PEAK. The matrix is computed for
one date + departure window (default 08:00, 60-min window, median travel time).
That is the scenario the "Public transport" mode in the UI represents. Document
the date/window you baked — a transit fairness map is only meaningful relative to
when it represents (summer vs winter, peak vs off-peak all differ).

Prerequisites:
  - Java 11+ on PATH (r5py launches the R5 JVM).
  - pip install r5py geopandas shapely (into the project venv).
  - A GTFS zip that COVERS the --date you bake (e.g. Trafiklab "GTFS Sverige 3").
    Default path: tools/transit/gtfs-sweden.zip
  - The OSM extract already used for OSRM: tools/osrm/data/sweden-latest.osm.pbf

Output layout (per city, matching bake_routing.py so the frontend reads it the
same way via ensureBakedRouting / bakedRoutingNeighbours):
    frontend/assets/data/cities/<key>/routing/transit/<category>.json
        { mode:"transit", cat, k, baked_at, scenario, key[], m[], s[], v[] }

where `key[i]` is the building centroid "lon,lat" (6 decimals, turf.centroid
semantics), `s[i]` is the door-to-door transit time in SECONDS (what the transit
fairness decay uses — ifCityTransitTimeForMode in fairness.js), `m[i]` is the
straight-line distance in metres (informational only for transit), and `v[i]` is
the POI opportunity weight. Neighbours are ranked nearest-first BY TIME. Buildings
with no POI reachable within --max-minutes are omitted; the frontend falls back to
walking accessibility for those.

Usage:
    .venv/bin/python tools/bake_transit.py --date 2024-11-12                 # all cities
    .venv/bin/python tools/bake_transit.py --date 2024-11-12 --cities vaxjo  # one city
    .venv/bin/python tools/bake_transit.py --date 2024-11-12 --cities vaxjo --limit 2000
    .venv/bin/python tools/bake_transit.py --date 2024-11-12 --force         # overwrite
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Reuse the geometry / loader / category contract from the OSRM baker so the two
# pipelines stay in sync (same centroid keys, same POI categories, same schema).
from bake_routing import (  # noqa: E402
    BUILDING_FILES,
    CITIES_ROOT,
    POI_CATEGORIES,
    REPO_ROOT,
    TOP_K,
    _now,
    load_building_centroids,
    load_pois,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

MODE = "transit"

# Inputs shared with the OSRM bake (OSM) and the new transit feed (GTFS).
OSM_PBF = REPO_ROOT / "tools" / "osrm" / "data" / "sweden-latest.osm.pbf"
GTFS_ZIP = REPO_ROOT / "tools" / "transit" / "gtfs-sweden.zip"
# R5 rejects GTFS route_type values outside [0, 1500]. The Trafiklab feed uses
# 1501 (Communal Taxi Service), which aborts the whole load, so we bake a
# sanitised copy once (cached) with such values clamped to the nearest accepted
# code. R5_MAX_ROUTE_TYPE / R5_FALLBACK_ROUTE_TYPE control the remap.
GTFS_R5_ZIP = REPO_ROOT / "tools" / "transit" / "gtfs-sweden-r5.zip"
R5_MAX_ROUTE_TYPE = 1500
# R5 parses but refuses Taxi types (1500/1501), so map them to 3 = Bus. Communal
# taxi is bus-like demand-responsive transit and Bus is unambiguously supported.
R5_FALLBACK_ROUTE_TYPE = 3
# Per-city OSM crops live here. R5 is built for regional extracts, so we crop
# the 769 MB national pbf down to each city's bbox (from meta.json) before
# building the transit network — orders of magnitude less RAM and time.
OSM_CROP_DIR = REPO_ROOT / "tools" / "transit" / "osm_crops"
CROP_MARGIN_DEG = 0.05  # pad the bbox so walk/transit legs near the edge resolve

# Defaults for the representative weekday-morning-peak scenario.
DEFAULT_DEPARTURE = "08:00"      # local time of the departure window start
DEFAULT_WINDOW_MIN = 60          # range query over 08:00-09:00, median is used
DEFAULT_MAX_MINUTES = 90         # POIs unreachable within this are dropped
DEFAULT_MAX_WALK_MINUTES = 20    # cap on each walk access/egress leg
WALK_SPEED_KMH = 4.8
ORIGIN_CHUNK = 2000              # buildings per matrix call (bounds memory)
# r5py defaults the JVM heap to 80% of RAM, which starves Python/pandas/OS on
# big cities and sends the machine into memory-compression/swap (~150x slower).
# Cap it so the rest of the pipeline has headroom; override with --max-memory.
DEFAULT_MAX_MEMORY = "8G"


def log(msg: str) -> None:
    print(msg, flush=True)


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Straight-line metres. Stored as m[] for transit (informational only)."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# ---------------------------------------------------------- gtfs sanitising --

def prepare_gtfs(force: bool) -> Path:
    """Return a GTFS zip R5 can load, remapping out-of-range route_type values.

    R5 only accepts route_type in [0, R5_MAX_ROUTE_TYPE]; the Trafiklab feed has
    1501 (communal taxi), which otherwise aborts the load. We rewrite only
    routes.txt and copy every other file verbatim. Cached at GTFS_R5_ZIP.
    """
    if not GTFS_ZIP.exists():
        raise RuntimeError(
            f"GTFS feed missing: {GTFS_ZIP}\n"
            "Download a static GTFS zip that covers --date from Trafiklab "
            "and place it there. See tools/transit/RUNBOOK.md."
        )
    if GTFS_R5_ZIP.exists() and not force:
        return GTFS_R5_ZIP

    log(f"  sanitising GTFS route_type (>{R5_MAX_ROUTE_TYPE} -> "
        f"{R5_FALLBACK_ROUTE_TYPE}) …")
    remapped = 0
    with zipfile.ZipFile(GTFS_ZIP) as zin, \
            zipfile.ZipFile(GTFS_R5_ZIP, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            raw = zin.read(name)
            if name.lower() != "routes.txt":
                zout.writestr(name, raw)
                continue
            text = raw.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            fields = reader.fieldnames or []
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=fields)
            writer.writeheader()
            for row in reader:
                rt = row.get("route_type", "")
                if rt.isdigit() and int(rt) > R5_MAX_ROUTE_TYPE:
                    row["route_type"] = str(R5_FALLBACK_ROUTE_TYPE)
                    remapped += 1
                writer.writerow(row)
            zout.writestr(name, buf.getvalue())
    log(f"  GTFS sanitised -> {GTFS_R5_ZIP.relative_to(REPO_ROOT)} "
        f"({remapped} routes remapped)")
    return GTFS_R5_ZIP


# ------------------------------------------------------------- osm cropping --

def coords_bounds(coords: list[tuple[float, float]]) -> tuple[float, float, float, float]:
    """(min_lon, min_lat, max_lon, max_lat) over building centroids."""
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return min(lons), min(lats), max(lons), max(lats)


def crop_osm_for_city(city_key: str, bounds: tuple[float, float, float, float],
                      force: bool) -> Path:
    """Crop the national pbf to the BUILDING extent (+ margin) via `osmium extract`.

    Must cover every building we route, or R5 can't snap them and they silently
    fall back to walking. The Nominatim city bbox in meta.json is far too small
    for these region-wide building files (e.g. Norrköping spans ~2°), so we crop
    to the actual centroid bounds instead. Cached at osm_crops/<key>.osm.pbf.
    """
    OSM_CROP_DIR.mkdir(parents=True, exist_ok=True)
    out = OSM_CROP_DIR / f"{city_key}.osm.pbf"
    if out.exists() and not force:
        return out
    min_lon, min_lat, max_lon, max_lat = bounds
    m = CROP_MARGIN_DEG
    # osmium wants -b LEFT,BOTTOM,RIGHT,TOP = min_lon,min_lat,max_lon,max_lat
    bbox_arg = f"{min_lon - m},{min_lat - m},{max_lon + m},{max_lat + m}"
    log(f"  cropping OSM to {city_key} building extent {bbox_arg} …")
    subprocess.run(
        ["osmium", "extract", "-b", bbox_arg, "--overwrite",
         "-o", str(out), str(OSM_PBF)],
        check=True,
    )
    log(f"  cropped -> {out.relative_to(REPO_ROOT)} ({out.stat().st_size // 1024} KB)")
    return out


# ------------------------------------------------------------------- r5py ----
# Imported lazily inside build_network so `--help` and arg validation work
# without Java / r5py installed.

def build_network(city_key: str, bounds: tuple[float, float, float, float],
                  force_crop: bool):
    import r5py  # noqa: F401  (import surfaces a clear error if missing)

    if not OSM_PBF.exists():
        raise RuntimeError(f"OSM extract missing: {OSM_PBF} (run tools/osrm/setup_osrm.sh)")
    gtfs = prepare_gtfs(force_crop)
    osm = crop_osm_for_city(city_key, bounds, force_crop)
    log(f"  building transit network from {osm.name} + {gtfs.name} …")
    return r5py.TransportNetwork(str(osm), [str(gtfs)])


def origins_gdf(coords: list[tuple[float, float]], keys: list[str]):
    import geopandas as gpd
    from shapely.geometry import Point

    return gpd.GeoDataFrame(
        {"id": keys, "geometry": [Point(lon, lat) for lon, lat in coords]},
        crs="EPSG:4326",
    )


def destinations_gdf(xy: list[tuple[float, float]]):
    import geopandas as gpd
    from shapely.geometry import Point

    return gpd.GeoDataFrame(
        {"id": list(range(len(xy))),
         "geometry": [Point(lon, lat) for lon, lat in xy]},
        crs="EPSG:4326",
    )


def travel_time_matrix(network, origins, destinations, departure: datetime,
                       window_min: int, max_minutes: int, max_walk_minutes: int):
    """Return the r5py travel-time DataFrame (from_id, to_id, travel_time minutes).

    In r5py 1.0 `TravelTimeMatrix` IS the result (a pandas DataFrame subclass),
    computed on construction — there is no separate compute step. Median (50th
    percentile, the default) over the departure window, so an infrequent line
    shows up as a long realistic wait rather than a free transfer.
    """
    import r5py

    return r5py.TravelTimeMatrix(
        network,
        origins=origins,
        destinations=destinations,
        departure=departure,
        departure_time_window=timedelta(minutes=window_min),
        transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
        max_time=timedelta(minutes=max_minutes),
        max_time_walking=timedelta(minutes=max_walk_minutes),
        speed_walking=WALK_SPEED_KMH,
    )


# ------------------------------------------------------------------ baking ---

def _write_cat(out_dir, cat, scenario, key, m, s, v):
    (out_dir / f"{cat}.json").write_text(json.dumps(
        {"mode": MODE, "cat": cat, "k": TOP_K, "baked_at": _now(),
         "scenario": scenario, "key": key, "m": m, "s": s, "v": v},
        separators=(",", ":")), encoding="utf-8")


def bake_city(city_key: str, departure: datetime, args) -> None:
    import numpy as np

    log(f"[{city_key}] baking transit ({departure.isoformat()}, "
        f"{args.window}-min window) …")
    coords, keys = load_building_centroids(city_key)
    if args.limit:
        coords, keys = coords[: args.limit], keys[: args.limit]
        log(f"  --limit: using first {len(coords)} buildings")

    # Crop OSM to the actual building extent (not the tiny Nominatim bbox) so
    # every building can be routed; otherwise out-of-crop buildings silently
    # fall back to walking.
    network = build_network(city_key, coords_bounds(coords), args.force)
    key_xy = {k: c for k, c in zip(keys, coords)}
    out_dir = CITIES_ROOT / city_key / "routing" / MODE
    out_dir.mkdir(parents=True, exist_ok=True)
    scenario = {
        "date": args.date, "departure": args.departure,
        "window_min": args.window, "max_minutes": args.max_minutes,
        "max_walk_minutes": args.max_walk, "router": "r5py",
    }

    # Decide which categories still need routing; pool every POI into ONE
    # destination set so R5 propagates from each origin only once (the costly
    # part), not once per category.
    counts: dict[str, int] = {}
    todo: list[str] = []
    dcat: list[str] = []
    dxy: list[tuple[float, float]] = []
    dv: list[float] = []
    for cat in POI_CATEGORIES:
        if (out_dir / f"{cat}.json").exists() and not args.force:
            try:
                counts[cat] = len(json.loads((out_dir / f"{cat}.json").read_text())["key"])
            except Exception:
                counts[cat] = 0
            log(f"    [transit/{cat}] cached, skipping")
            continue
        pois = load_pois(city_key, cat)
        if not pois:
            log(f"    [transit/{cat}] no POIs; writing empty")
            _write_cat(out_dir, cat, scenario, [], [], [], [])
            counts[cat] = 0
            continue
        todo.append(cat)
        for lon, lat, v in pois:
            dcat.append(cat)
            dxy.append((lon, lat))
            dv.append(v)

    if todo:
        destinations = destinations_gdf(dxy)
        dcat_arr = np.array(dcat, dtype=object)
        # cat -> {from_id: [(travel_time_min, global_dest_idx), ...] top-K by time}
        best: dict[str, dict[str, list[tuple[float, int]]]] = {c: {} for c in todo}

        t0 = time.time()
        for c0 in range(0, len(coords), ORIGIN_CHUNK):
            sub_coords = coords[c0:c0 + ORIGIN_CHUNK]
            sub_keys = keys[c0:c0 + ORIGIN_CHUNK]
            df = travel_time_matrix(
                network, origins_gdf(sub_coords, sub_keys), destinations,
                departure, args.window, args.max_minutes, args.max_walk)
            df = df.dropna(subset=["travel_time"])
            if not df.empty:
                df = df.assign(cat=dcat_arr[df["to_id"].to_numpy()])
                for cat in todo:
                    sub = df[df["cat"] == cat]
                    if sub.empty:
                        continue
                    # K nearest POIs of this category per building, by time
                    sub = sub.sort_values("travel_time").groupby("from_id").head(TOP_K)
                    for from_id, g in sub.groupby("from_id", sort=False):
                        best[cat][from_id] = list(
                            zip(g["travel_time"].astype(float),
                                g["to_id"].astype(int)))
            log(f"    routed {min(c0 + ORIGIN_CHUNK, len(coords))}/{len(coords)} "
                f"buildings ({time.time() - t0:.0f}s)")

        for cat in todo:
            ok_key, ok_m, ok_s, ok_v = [], [], [], []
            for from_id, lst in best[cat].items():
                blon, blat = key_xy[from_id]
                ok_key.append(from_id)
                ok_m.append([round(haversine_m(blon, blat, *dxy[gi]), 1) for _, gi in lst])
                ok_s.append([round(t * 60.0, 1) for t, _ in lst])
                ok_v.append([round(dv[gi], 3) for _, gi in lst])
            _write_cat(out_dir, cat, scenario, ok_key, ok_m, ok_s, ok_v)
            counts[cat] = len(ok_key)
            log(f"    [transit/{cat}] {len(ok_key)}/{len(keys)} buildings baked")

    idx_path = CITIES_ROOT / city_key / "routing" / "transit_index.json"
    idx_path.write_text(json.dumps(
        {"city": city_key, "mode": MODE, "baked_at": _now(),
         "scenario": scenario, "building_count": len(keys),
         "categories": counts}, indent=2), encoding="utf-8")
    log(f"[{city_key}] done -> {idx_path.relative_to(REPO_ROOT)}")


def parse_departure(date_str: str, time_str: str) -> datetime:
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    # r5py interprets naive datetimes in the network's local timezone, which is
    # what we want for a "08:00 local" departure.
    return dt


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", required=True,
                   help="YYYY-MM-DD weekday covered by the GTFS feed (e.g. a Tuesday)")
    p.add_argument("--departure", default=DEFAULT_DEPARTURE,
                   help=f"HH:MM local departure window start (default {DEFAULT_DEPARTURE})")
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_MIN,
                   help=f"departure window minutes (default {DEFAULT_WINDOW_MIN})")
    p.add_argument("--max-minutes", type=int, default=DEFAULT_MAX_MINUTES,
                   dest="max_minutes", help="drop POIs slower than this")
    p.add_argument("--max-walk", type=int, default=DEFAULT_MAX_WALK_MINUTES,
                   help="cap on each walk access/egress leg (minutes)")
    p.add_argument("--cities", nargs="+", default=None,
                   help="city keys (default: all). " + ", ".join(BUILDING_FILES))
    p.add_argument("--force", action="store_true", help="overwrite existing files")
    p.add_argument("--limit", type=int, default=None, help="cap building count (smoke test)")
    p.add_argument("--max-memory", default=DEFAULT_MAX_MEMORY, dest="max_memory",
                   help=f"JVM heap cap, e.g. 8G or 50%% (default {DEFAULT_MAX_MEMORY})")
    args = p.parse_args()

    # r5py reads --max-memory from sys.argv via configargparse.parse_known_args
    # (it ignores our other flags). Inject our cap if the user didn't pass one,
    # so the JVM doesn't grab 80% of RAM and starve the rest of the pipeline.
    if not any(a in sys.argv for a in ("--max-memory", "-m")):
        sys.argv += ["--max-memory", args.max_memory]

    cities = args.cities or list(BUILDING_FILES)
    for c in cities:
        if c not in BUILDING_FILES:
            log(f"unknown city: {c}")
            return 2

    try:
        departure = parse_departure(args.date, args.departure)
    except ValueError as exc:
        log(f"bad --date/--departure: {exc}")
        return 2

    for city_key in cities:
        try:
            bake_city(city_key, departure, args)
        except Exception as exc:
            log(f"[{city_key}] FAILED: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
