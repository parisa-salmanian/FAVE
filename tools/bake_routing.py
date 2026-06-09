"""Bake road-distance access matrices for FAVE using a LOCAL OSRM instance.

Runtime contract: the FAVE app must never call a routing API during normal
use. This script precomputes, for every building, the nearest POI of each
category by real road network distance/time, for each travel mode, and writes
the results to disk under frontend/assets/data/. The running app then reads
those files — no Docker, no OSRM, no internet at request time.

Prerequisite: the three local OSRM servers from tools/osrm/ must be running
(see tools/osrm/RUNBOOK.md). This script only talks to them on localhost.

Output layout (per city, under frontend/assets/data/cities/<key>/):
    routing/<mode>/<category>.json   { mode, cat, baked_at, key[], m[], s[] }
    routing/index.json               summary + key contract

where mode is one of walking|cycling|driving, `key[i]` is the building centroid
key "lon,lat" (6 decimals, turf.centroid semantics), `m[i]` is the nearest-POI
road distance in metres and `s[i]` is the road travel time in seconds. Buildings
with no reachable POI are omitted; the frontend falls back to local straight-line
distance for those.

Usage:
    .venv/bin/python tools/bake_routing.py                       # all cities, all modes
    .venv/bin/python tools/bake_routing.py --cities vaxjo        # one city
    .venv/bin/python tools/bake_routing.py --modes walking       # one mode
    .venv/bin/python tools/bake_routing.py --cities vaxjo --limit 2000   # quick smoke test
    .venv/bin/python tools/bake_routing.py --force               # overwrite existing
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "frontend" / "assets" / "data"
CITIES_ROOT = DATA_ROOT / "cities"

# Mirrors BUILDING_URL_BY_CITY_KEY in frontend/assets/js/lib/cityPaths.js.
# Keep in sync when that table changes.
BUILDING_FILES: dict[str, str] = {
    "vaxjo": "lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson",
    "malmo": "byggnad_malmo.geojson",
    "goteborg": "byggnad_goteborg.geojson",
    "norrkoping": "byggnad_norrkoping.geojson",
    "stockholm": "byggnad_stockholm.geojson",
    "uppsala": "byggnad_uppsala.geojson",
    "kalmar": "byggnad_kalmar.geojson",
}

POI_CATEGORIES = [
    "grocery", "hospital", "pharmacy", "dentistry", "healthcare_center",
    "veterinary", "university", "kindergarten", "school_primary", "school_high",
]

# Travel mode -> (OSRM base url, OSRM profile name in the URL path).
# The profile name in the URL is cosmetic for a single-profile server, but we
# keep it meaningful. Ports match tools/osrm/docker-compose.yml.
MODE_OSRM: dict[str, dict[str, str]] = {
    "walking": {"url": "http://127.0.0.1:5001", "profile": "foot"},
    "cycling": {"url": "http://127.0.0.1:5002", "profile": "bike"},
    "driving": {"url": "http://127.0.0.1:5003", "profile": "car"},
}

# Keep total coordinates per /table request URL-safe. Sources(batch) + dests.
COORDS_PER_REQUEST = 900
DEST_CHUNK = 100            # POIs per request chunk
KEY_DECIMALS = 6           # must match the frontend centroid key rounding
TABLE_TIMEOUT = (10, 120)
# The IF-City accessibility model sums over the 3 nearest POIs (road distance +
# opportunity weight). We bake exactly those 3; the seconds array isn't used by
# the model, so it's omitted to keep files small and frontend load fast.
TOP_K = 3


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------- geometry ---

def _iter_positions(coords: Any) -> Iterable[tuple[float, float]]:
    """Yield every [lon, lat] position in an arbitrarily nested coords array.

    Matches turf's coordEach: every vertex of the geometry, holes and the
    closing duplicate included, so the mean equals turf.centroid exactly.
    """
    if (
        isinstance(coords, (list, tuple))
        and len(coords) >= 2
        and isinstance(coords[0], (int, float))
        and isinstance(coords[1], (int, float))
    ):
        yield (float(coords[0]), float(coords[1]))
        return
    if isinstance(coords, (list, tuple)):
        for c in coords:
            yield from _iter_positions(c)


def centroid_lonlat(geometry: dict[str, Any]) -> tuple[float, float] | None:
    """turf.centroid: arithmetic mean of all vertices. Returns (lon, lat)."""
    sx = sy = 0.0
    n = 0
    for lon, lat in _iter_positions(geometry.get("coordinates")):
        sx += lon
        sy += lat
        n += 1
    if n == 0:
        return None
    return (sx / n, sy / n)


def centroid_key(lon: float, lat: float) -> str:
    return f"{lon:.{KEY_DECIMALS}f},{lat:.{KEY_DECIMALS}f}"


# ------------------------------------------------------------------- loaders -

def load_building_centroids(city_key: str) -> tuple[list[tuple[float, float]], list[str]]:
    """Return (coords, keys) for every building, deduped by centroid key.

    Deduping by key means buildings that share a centroid cell are routed once;
    the frontend looks up by the same key so they resolve to the same value.
    """
    fname = BUILDING_FILES.get(city_key)
    if not fname:
        raise RuntimeError(f"no building file mapped for city '{city_key}'")
    path = DATA_ROOT / fname
    if not path.exists():
        raise RuntimeError(f"building file missing: {path}")

    log(f"  loading buildings {path.name} …")
    fc = json.loads(path.read_text(encoding="utf-8"))
    coords: list[tuple[float, float]] = []
    keys: list[str] = []
    seen: set[str] = set()
    for feat in fc.get("features", []):
        geom = feat.get("geometry") or {}
        c = centroid_lonlat(geom)
        if not c:
            continue
        k = centroid_key(c[0], c[1])
        if k in seen:
            continue
        seen.add(k)
        coords.append(c)
        keys.append(k)
    log(f"  {len(coords)} unique building centroids")
    return coords, keys


def opportunity_weight(category: str, props: dict[str, Any]) -> float:
    """Mirror ifCityOpportunityWeightFromPOI in fairness.js.

    Almost always 1; only hospitals with a `beds` tag or POIs with a numeric
    `capacity` tag differ. Baking it here avoids coupling the frontend to POI
    array indices.
    """
    tags = (props or {}).get("tags") or {}
    try:
        beds = float(tags.get("beds"))
    except (TypeError, ValueError):
        beds = float("nan")
    try:
        capacity = float(tags.get("capacity"))
    except (TypeError, ValueError):
        capacity = float("nan")
    if category == "hospital" and beds == beds and beds > 0:
        return max(1.0, min(10.0, beds / 100.0))
    if capacity == capacity and capacity > 0:
        return max(1.0, min(10.0, capacity / 100.0))
    return 1.0


def load_pois(city_key: str, category: str) -> list[tuple[float, float, float]]:
    """Return [(lon, lat, opportunity_weight), ...] for a category."""
    path = CITIES_ROOT / city_key / "pois" / f"{category}.geojson"
    if not path.exists():
        return []
    fc = json.loads(path.read_text(encoding="utf-8"))
    out: list[tuple[float, float, float]] = []
    for feat in fc.get("features", []):
        g = feat.get("geometry") or {}
        v = opportunity_weight(category, feat.get("properties") or {})
        if g.get("type") != "Point":
            c = centroid_lonlat(g)
            if c:
                out.append((c[0], c[1], v))
            continue
        coord = g.get("coordinates")
        if isinstance(coord, list) and len(coord) >= 2:
            out.append((float(coord[0]), float(coord[1]), v))
    return out


# --------------------------------------------------------------------- OSRM --

def osrm_table(
    base_url: str,
    profile: str,
    sources: list[tuple[float, float]],
    destinations: list[tuple[float, float]],
) -> tuple[list[list[float | None]], list[list[float | None]]]:
    """Call OSRM /table for sources x destinations.

    Returns (durations_seconds, distances_metres), each a sources x dests
    matrix with None where unreachable.
    """
    all_coords = sources + destinations
    coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in all_coords)
    src_idx = ";".join(str(i) for i in range(len(sources)))
    dst_idx = ";".join(str(i + len(sources)) for i in range(len(destinations)))
    url = f"{base_url}/table/v1/{profile}/{coord_str}"
    params = {
        "sources": src_idx,
        "destinations": dst_idx,
        "annotations": "duration,distance",
    }
    # The emulated OSRM server occasionally drops a connection under load
    # (RemoteDisconnected / read timeout). Retry transient network errors with
    # backoff so a long multi-hour bake survives blips instead of aborting.
    last_err = None
    for attempt in range(8):
        try:
            r = requests.get(url, params=params, timeout=TABLE_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if data.get("code") != "Ok":
                raise RuntimeError(f"OSRM table error: {data.get('code')} {data.get('message')}")
            return data.get("durations") or [], data.get("distances") or []
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_err = e
            time.sleep(min(30, 3 * (attempt + 1)))
    raise RuntimeError(f"OSRM table failed after retries: {last_err}")


def nearest_k_by_road(
    base_url: str,
    profile: str,
    buildings: list[tuple[float, float]],
    pois: list[tuple[float, float, float]],
    k: int = TOP_K,
) -> list[list[tuple[float, float, float]]]:
    """For every building, the k nearest POIs by road distance.

    Returns, aligned to `buildings`, a list of up to k (metres, seconds, weight)
    tuples sorted by ascending road distance. Chunks buildings and POIs to stay
    within the per-request coordinate budget, merging the k-best across chunks.
    """
    n = len(buildings)
    best: list[list[tuple[float, float, float]]] = [[] for _ in range(n)]
    if not pois:
        return best

    poi_xy = [(lon, lat) for lon, lat, _ in pois]
    poi_v = [v for _, _, v in pois]

    dest_chunk = min(DEST_CHUNK, max(1, COORDS_PER_REQUEST - 1))
    batch = max(1, COORDS_PER_REQUEST - dest_chunk)

    for b0 in range(0, n, batch):
        src = buildings[b0:b0 + batch]
        for d0 in range(0, len(poi_xy), dest_chunk):
            dst = poi_xy[d0:d0 + dest_chunk]
            durations, distances = osrm_table(base_url, profile, src, dst)
            for i in range(len(src)):
                drow = durations[i] if i < len(durations) else []
                mrow = distances[i] if i < len(distances) else []
                cand = best[b0 + i]
                for j in range(len(mrow)):
                    m = mrow[j]
                    s = drow[j] if j < len(drow) else None
                    if m is None or s is None:
                        continue
                    cand.append((m, s, poi_v[d0 + j]))
                # keep only the k nearest by distance so memory stays bounded
                cand.sort(key=lambda t: t[0])
                best[b0 + i] = cand[:k]
    return best


# -------------------------------------------------------------------- baking -

def bake_city_mode(
    city_key: str,
    mode: str,
    coords: list[tuple[float, float]],
    keys: list[str],
    force: bool,
) -> dict[str, int]:
    cfg = MODE_OSRM[mode]
    base_url, profile = cfg["url"], cfg["profile"]
    out_dir = CITIES_ROOT / city_key / "routing" / mode
    out_dir.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    for cat in POI_CATEGORIES:
        out_path = out_dir / f"{cat}.json"
        if out_path.exists() and not force:
            try:
                counts[cat] = len(json.loads(out_path.read_text())["key"])
            except Exception:
                counts[cat] = 0
            log(f"    [{mode}/{cat}] cached, skipping")
            continue

        pois = load_pois(city_key, cat)
        if not pois:
            log(f"    [{mode}/{cat}] no POIs baked for this city; writing empty")
            out_path.write_text(json.dumps(
                {"mode": mode, "cat": cat, "k": TOP_K, "baked_at": _now(),
                 "key": [], "m": [], "v": []},
                separators=(",", ":"),
            ), encoding="utf-8")
            counts[cat] = 0
            continue

        t0 = time.time()
        per_building = nearest_k_by_road(base_url, profile, coords, pois)
        ok_key, ok_m, ok_v = [], [], []
        for k, neighbours in zip(keys, per_building):
            if not neighbours:
                continue  # unreachable: frontend falls back to local haversine
            ok_key.append(k)
            # metres (int) + opportunity weight; seconds omitted (model uses metres)
            ok_m.append([round(m) for m, _, _ in neighbours])
            ok_v.append([round(v, 3) for _, _, v in neighbours])
        out_path.write_text(json.dumps(
            {"mode": mode, "cat": cat, "k": TOP_K, "baked_at": _now(),
             "key": ok_key, "m": ok_m, "v": ok_v},
            separators=(",", ":"),
        ), encoding="utf-8")
        counts[cat] = len(ok_key)
        log(f"    [{mode}/{cat}] {len(ok_key)}/{len(keys)} buildings, "
            f"{len(pois)} POIs, {time.time() - t0:.1f}s")
    return counts


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_osrm(mode: str) -> bool:
    cfg = MODE_OSRM[mode]
    try:
        # A trivial 2-coordinate table just to confirm the server answers Ok.
        # OSRM rejects single-coordinate tables, so we must pass two points.
        r = requests.get(
            f"{cfg['url']}/table/v1/{cfg['profile']}/14.80,56.88;14.81,56.89",
            params={"sources": "0", "destinations": "1"},
            timeout=(5, 15),
        )
        r.raise_for_status()
        return r.json().get("code") == "Ok"
    except Exception as exc:
        log(f"  OSRM for '{mode}' not reachable at {cfg['url']}: {exc}")
        return False


def bake_city(city_key: str, modes: list[str], force: bool, limit: int | None) -> None:
    log(f"[{city_key}] baking routing for modes: {', '.join(modes)}")
    coords, keys = load_building_centroids(city_key)
    if limit:
        coords, keys = coords[:limit], keys[:limit]
        log(f"  --limit: using first {len(coords)} buildings")

    summary: dict[str, Any] = {
        "city": city_key, "baked_at": _now(),
        "key_format": "lon,lat", "key_decimals": KEY_DECIMALS,
        "building_count": len(keys), "modes": {},
    }
    for mode in modes:
        if not check_osrm(mode):
            log(f"  skipping mode '{mode}' (server down)")
            continue
        counts = bake_city_mode(city_key, mode, coords, keys, force)
        summary["modes"][mode] = counts

    idx_path = CITIES_ROOT / city_key / "routing" / "index.json"
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    idx_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log(f"[{city_key}] done -> {idx_path.relative_to(REPO_ROOT)}")


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cities", nargs="+", default=None,
                   help="city keys (default: all). " + ", ".join(BUILDING_FILES))
    p.add_argument("--modes", nargs="+", default=None,
                   choices=list(MODE_OSRM), help="travel modes (default: all)")
    p.add_argument("--force", action="store_true", help="overwrite existing files")
    p.add_argument("--limit", type=int, default=None,
                   help="cap building count (smoke test)")
    args = p.parse_args()

    cities = args.cities or list(BUILDING_FILES)
    for c in cities:
        if c not in BUILDING_FILES:
            log(f"unknown city: {c}")
            return 2
    modes = args.modes or list(MODE_OSRM)

    for city_key in cities:
        try:
            bake_city(city_key, modes, args.force, args.limit)
        except Exception as exc:
            log(f"[{city_key}] FAILED: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
