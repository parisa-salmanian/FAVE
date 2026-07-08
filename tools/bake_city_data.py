"""Pre-bake POI data for the cities FAVE supports.

Runtime contract: the FAVE app must never call an external API during normal
use. Everything the frontend needs has to live on disk under
`frontend/assets/data/`. This script is the one place that hits external APIs
(Nominatim for the city bbox, Overpass for POIs) — it runs once per city
during development.

Usage
-----

    python tools/bake_city_data.py                       # bake all 7 cities
    python tools/bake_city_data.py --cities vaxjo malmo  # bake a subset
    python tools/bake_city_data.py --cities vaxjo --force  # overwrite
    python tools/bake_city_data.py --add stockholm "Stockholm, Sweden"  # new city

Output layout (per city, under frontend/assets/data/cities/<key>/)
------------------------------------------------------------------
- meta.json            City key, display name, bbox, center, baked_at, counts.
- pois/<category>.geojson   One FeatureCollection per POI category. Categories
                            and Overpass selectors mirror POI_QUERIES in
                            frontend/assets/js/main.js.
- pois/index.json      Map of category → feature count for quick frontend reads.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = REPO_ROOT / "frontend" / "assets" / "data" / "cities"

USER_AGENT = {
    "User-Agent": "fave-baker/1.0 (https://github.com/parisa-salmanian/FAVE)"
}

NOMINATIM_ENDPOINTS = [
    "https://nominatim.openstreetmap.org/search",
    "https://nominatim.openstreetmap.fr/search",
]
NOMINATIM_TIMEOUT = (20, 40)

OVERPASS_ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
]
OVERPASS_TIMEOUT = (10, 180)
OVERPASS_BACKOFF_S = [0.5, 1.5, 3.0]
OVERPASS_INTER_QUERY_DELAY_S = 1.5

CITIES: dict[str, str] = {
    "vaxjo": "Växjö, Sweden",
    "malmo": "Malmö, Sweden",
    "goteborg": "Göteborg, Sweden",
    "stockholm": "Stockholm, Sweden",
    "kalmar": "Kalmar, Sweden",
    "norrkoping": "Norrköping, Sweden",
    "uppsala": "Uppsala, Sweden",
}

POI_QUERIES: dict[str, list[str]] = {
    "grocery": [
        'nwr["shop"="supermarket"]',
        'nwr["shop"="convenience"]',
        'nwr["shop"="greengrocer"]',
        'nwr["amenity"="marketplace"]',
    ],
    "hospital": [
        'nwr["amenity"="hospital"]',
        'nwr["healthcare"="hospital"]',
    ],
    "pharmacy": ['nwr["amenity"="pharmacy"]'],
    "dentistry": ['nwr["amenity"="dentist"]'],
    "healthcare_center": [
        'nwr["amenity"="clinic"]',
        'nwr["healthcare"~"clinic|centre|doctor",i]',
    ],
    "veterinary": ['nwr["amenity"="veterinary"]'],
    "university": ['nwr["amenity"="university"]'],
    "kindergarten": [
        'nwr["amenity"="kindergarten"]',
        'nwr["amenity"="childcare"]',
    ],
    # Both school buckets pull the SAME `amenity=school` set; the
    # primary/gymnasium split is done in filter_school_subtype() using the
    # _is_swedish_gymnasium() heuristic (ported from EpiCity osm.py). Swedish OSM
    # tags school levels very inconsistently, so the old approach — keep a school
    # in `school_primary` only if explicitly tagged primary — dropped the many
    # untagged grundskolor entirely (e.g. Göteborg kept 1 of ~343). Defaulting an
    # untagged `amenity=school` to grundskola (primary) and promoting only the
    # heuristic gymnasium matches recovers them and de-inflates school_high.
    "school_primary": ['nwr["amenity"="school"]'],
    "school_high": ['nwr["amenity"="school"]'],
}


def geocode_city(query: str) -> dict[str, Any]:
    """Return {'lat','lon','bbox':[s,w,n,e],'raw'} via Nominatim with mirror fallback."""
    params = {"q": query, "format": "json", "limit": 1, "addressdetails": 1}
    last_err: Exception | None = None
    for url in NOMINATIM_ENDPOINTS:
        try:
            r = requests.get(url, params=params, headers=USER_AGENT, timeout=NOMINATIM_TIMEOUT)
            r.raise_for_status()
            arr = r.json()
            if arr:
                hit = arr[0]
                bb = [float(x) for x in hit["boundingbox"]]
                return {
                    "lat": float(hit["lat"]),
                    "lon": float(hit["lon"]),
                    "bbox": [bb[0], bb[2], bb[1], bb[3]],
                    "raw": hit,
                }
        except Exception as exc:
            last_err = exc
            time.sleep(1.0)
    raise RuntimeError(f"Nominatim failed for {query!r}: {last_err}")


def overpass_query(body: str) -> dict[str, Any]:
    """POST raw Overpass-QL with mirror fallback + per-mirror backoff."""
    payload = body.encode("utf-8")
    last_err: Any = None
    for base_url in OVERPASS_ENDPOINTS:
        for backoff in OVERPASS_BACKOFF_S:
            try:
                r = requests.post(base_url, data=payload, headers=USER_AGENT, timeout=OVERPASS_TIMEOUT)
                if r.status_code >= 500 or r.status_code == 429:
                    last_err = f"{r.status_code} from {base_url}"
                    time.sleep(backoff)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_err = e
                time.sleep(backoff)
    raise RuntimeError(f"All Overpass mirrors failed: {last_err}")


def overpass_to_pois(opj: dict[str, Any], category: str) -> list[dict[str, Any]]:
    """Convert Overpass elements to POI Point features, mirroring fetchPOIs in main.js."""
    feats: list[dict[str, Any]] = []
    for el in opj.get("elements", []):
        if el.get("type") == "node":
            lon, lat = el.get("lon"), el.get("lat")
        elif "center" in el:
            lon, lat = el["center"].get("lon"), el["center"].get("lat")
        else:
            continue
        if not (isinstance(lon, (int, float)) and isinstance(lat, (int, float))):
            continue
        tags = el.get("tags") or {}
        name = tags.get("name") or tags.get("name:sv") or tags.get("name:en") \
            or tags.get("brand") or tags.get("brand:sv") or tags.get("brand:en") or ""
        feats.append({
            "type": "Feature",
            "properties": {
                "id": el.get("id"),
                "osm_type": el.get("type"),
                "name": name,
                "category": category,
                "tags": tags,
            },
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })
    return feats


def dedupe_pois(features: list[dict[str, Any]], category: str) -> list[dict[str, Any]]:
    """Mirror the dedupe in fetchPOIs: collapse same OSM object, then collapse co-located near-name dupes."""
    seen_object: set[str] = set()
    by_place: dict[str, dict[str, Any]] = {}
    for feat in features:
        coords = feat.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        if not (isinstance(lon, (int, float)) and isinstance(lat, (int, float))):
            continue

        props = feat.get("properties", {})
        tags = props.get("tags", {}) or {}
        osm_type = str(props.get("osm_type") or "").strip().lower()
        osm_id = props.get("id")

        if osm_type and isinstance(osm_id, int):
            obj_key = f"{osm_type}:{osm_id}"
            if obj_key in seen_object:
                continue
            seen_object.add(obj_key)

        name_key = str(props.get("name") or "").strip().lower()
        kind_key = str(tags.get("amenity") or tags.get("healthcare") or category or "").strip().lower()
        coord_key = f"{lon:.5f},{lat:.5f}"
        place_key = f"{coord_key}:{name_key or kind_key}"
        by_place.setdefault(place_key, feat)

    return list(by_place.values())


def _is_swedish_gymnasium(tags: dict[str, Any]) -> bool:
    """Detect a Swedish `gymnasium` (upper secondary, ages 16-19) vs. a regular
    `grundskola`. Ported from EpiCity `osm.py:_is_swedish_gymnasium` (copy — the
    EpiCity reference under epicity_engine/ is never modified). OSM tags Swedish
    school levels very inconsistently, so several signals are combined:
      • `isced:level` contains "3" (ISCED 3 = upper secondary; authoritative but
        rarely tagged);
      • `school:type` / `school:level` contains gymnasium / upper_secondary /
        secondary;
      • the name contains "gymnasi" (catches "Gymnasium" / "Gymnasiet" / …);
      • a small list of historic gymnasiums whose names predate the word;
      • `grades` includes "gy" or year labels 10/11/12.
    NB `building=gymnasium` is deliberately NOT a signal — that OSM tag means a
    sports hall and would create false positives.
    """
    isced = (tags.get("isced:level", "") or "").strip()
    if "3" in isced.split(";"):
        return True
    school_type = (tags.get("school:type", "") or "").lower()
    if any(kw in school_type for kw in ("gymnasium", "upper_secondary", "secondary")):
        return True
    school_level = (tags.get("school:level", "") or "").lower()
    if any(kw in school_level for kw in ("gymnasium", "upper_secondary", "secondary")):
        return True
    name = (tags.get("name:sv") or tags.get("name") or "").lower()
    if "gymnasi" in name:
        return True
    _GYM_NAMES = ("katedralskol", "teknikum", "kungsmadskol")
    if any(kw in name for kw in _GYM_NAMES):
        return True
    grades = (tags.get("grades") or tags.get("grade") or "").lower()
    if "gy" in grades or "10" in grades or "11" in grades or "12" in grades:
        return True
    return False


def filter_school_subtype(features: list[dict[str, Any]], category: str) -> list[dict[str, Any]]:
    """Split the shared `amenity=school` set into grundskola (school_primary,
    the default) vs. gymnasium (school_high, the heuristic matches). Every school
    lands in exactly one bucket — none are dropped, unlike the old level-tag
    post-filter. See _is_swedish_gymnasium / the POI_QUERIES note."""
    if category not in {"school_primary", "school_high"}:
        return features
    want_high = (category == "school_high")
    return [
        f for f in features
        if _is_swedish_gymnasium(f.get("properties", {}).get("tags", {}) or {}) == want_high
    ]


FORBIDDEN_ZONE_SELECTORS: list[str] = [
    'way["natural"="water"]',
    'relation["natural"="water"]',
    'way["waterway"="riverbank"]',
    'way["waterway"="dock"]',
    'way["landuse"="reservoir"]',
    'relation["landuse"="reservoir"]',
    'way["natural"="wetland"]',
    'relation["natural"="wetland"]',
]


def build_overpass_body(bbox: list[float], selectors: list[str]) -> str:
    south, west, north, east = bbox
    sel_block = "\n      ".join(f"{q}({south},{west},{north},{east});" for q in selectors)
    return (
        f"[out:json][timeout:120];\n"
        f"(\n      {sel_block}\n);\n"
        f"out center tags;"
    )


def build_overpass_body_geom(bbox: list[float], selectors: list[str]) -> str:
    """Request full inline geometry via 'out geom' — avoids manual node/way assembly."""
    south, west, north, east = bbox
    sel_block = "\n      ".join(f"{q}({south},{west},{north},{east});" for q in selectors)
    return (
        f"[out:json][timeout:180];\n"
        f"(\n      {sel_block}\n);\n"
        f"out geom;"
    )


def _coords_approx_equal(a: tuple[float, float], b: tuple[float, float], tol: float = 1e-7) -> bool:
    return abs(a[0] - b[0]) < tol and abs(a[1] - b[1]) < tol


def _assemble_rings(segments: list[list[tuple[float, float]]]) -> list[list[tuple[float, float]]]:
    """Chain open way segments into closed rings (handles split multipolygon outer members)."""
    segs = [list(s) for s in segments if len(s) >= 2]
    rings: list[list[tuple[float, float]]] = []
    while segs:
        ring = segs.pop(0)
        changed = True
        while changed and not _coords_approx_equal(ring[0], ring[-1]):
            changed = False
            for i, seg in enumerate(segs):
                if _coords_approx_equal(ring[-1], seg[0]):
                    ring.extend(seg[1:])
                    segs.pop(i)
                    changed = True
                    break
                if _coords_approx_equal(ring[-1], seg[-1]):
                    ring.extend(reversed(seg[:-1]))
                    segs.pop(i)
                    changed = True
                    break
        if len(ring) >= 4:
            if not _coords_approx_equal(ring[0], ring[-1]):
                ring.append(ring[0])
            rings.append(ring)
    return rings


def overpass_to_polygons(opj: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert Overpass 'out geom' response to Polygon features.

    'out geom' embeds coordinates directly in each element:
    - way: el["geometry"] = [{"lat": .., "lon": ..}, ...]
    - relation: el["members"][i]["geometry"] = [{"lat": .., "lon": ..}, ...]
    """
    features: list[dict[str, Any]] = []
    seen: set[str] = set()  # deduplicate by osm_type:osm_id

    for el in opj.get("elements", []):
        key = f"{el['type']}:{el['id']}"
        if key in seen:
            continue

        if el["type"] == "way":
            raw = el.get("geometry") or []
            coords = [(pt["lon"], pt["lat"]) for pt in raw if "lon" in pt and "lat" in pt]
            if len(coords) < 4:
                continue
            if not _coords_approx_equal(coords[0], coords[-1]):
                coords.append(coords[0])
            seen.add(key)
            features.append({
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Polygon", "coordinates": [coords]},
            })

        elif el["type"] == "relation":
            outer_segs: list[list[tuple[float, float]]] = []
            for member in el.get("members", []):
                if member.get("role") != "outer" or member.get("type") != "way":
                    continue
                raw = member.get("geometry") or []
                coords = [(pt["lon"], pt["lat"]) for pt in raw if "lon" in pt and "lat" in pt]
                if coords:
                    outer_segs.append(coords)
            for ring in _assemble_rings(outer_segs):
                seen.add(key)
                features.append({
                    "type": "Feature",
                    "properties": {},
                    "geometry": {"type": "Polygon", "coordinates": [ring]},
                })

    return features


def bake_city(key: str, query_string: str, force: bool, log, cats: "set[str] | None" = None) -> None:
    out_dir = CACHE_ROOT / key
    pois_dir = out_dir / "pois"
    pois_dir.mkdir(parents=True, exist_ok=True)

    meta_path = out_dir / "meta.json"
    if meta_path.exists() and not force:
        log(f"[{key}] meta.json exists, refreshing only missing POI categories (use --force to redo)")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    else:
        log(f"[{key}] geocoding via Nominatim ({query_string!r})")
        geo = geocode_city(query_string)
        meta = {
            "key": key,
            "display_name": query_string.split(",")[0].strip(),
            "query": query_string,
            "lat": geo["lat"],
            "lon": geo["lon"],
            "bbox": geo["bbox"],
            "baked_at": datetime.now(timezone.utc).isoformat(),
            "counts": {},
        }

    bbox = meta["bbox"]
    counts = meta.get("counts", {})

    for category, selectors in POI_QUERIES.items():
        if cats is not None and category not in cats:
            continue
        out_path = pois_dir / f"{category}.geojson"
        if out_path.exists() and not force:
            try:
                cached = json.loads(out_path.read_text(encoding="utf-8"))
                counts[category] = len(cached.get("features", []))
            except Exception:
                pass
            log(f"[{key}] {category}: cached ({out_path.stat().st_size // 1024} KB), skipping")
            continue

        body = build_overpass_body(bbox, selectors)
        log(f"[{key}] querying Overpass for {category}…")
        opj = overpass_query(body)
        feats = overpass_to_pois(opj, category)
        feats = dedupe_pois(feats, category)
        feats = filter_school_subtype(feats, category)

        fc = {"type": "FeatureCollection", "features": feats}
        out_path.write_text(
            json.dumps(fc, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        counts[category] = len(feats)
        log(f"[{key}] {category}: {len(feats)} POIs → {out_path.relative_to(REPO_ROOT)}")
        time.sleep(OVERPASS_INTER_QUERY_DELAY_S)

    # Forbidden zones: water bodies, wetlands (polygon geometry required).
    # Skipped entirely when a --cats subset is in effect (those runs only touch
    # the named POI categories, not the shared zone layer).
    forbidden_path = out_dir / "forbidden_zones.geojson"
    if cats is not None:
        pass
    elif not forbidden_path.exists() or force:
        log(f"[{key}] querying Overpass for forbidden zones (water/wetlands)…")
        time.sleep(OVERPASS_INTER_QUERY_DELAY_S)
        body = build_overpass_body_geom(bbox, FORBIDDEN_ZONE_SELECTORS)
        try:
            opj = overpass_query(body)
            feats = overpass_to_polygons(opj)
            fz_fc = {"type": "FeatureCollection", "features": feats}
            forbidden_path.write_text(
                json.dumps(fz_fc, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            log(f"[{key}] forbidden_zones: {len(feats)} polygons → {forbidden_path.relative_to(REPO_ROOT)}")
        except Exception as exc:
            log(f"[{key}] WARNING: forbidden_zones fetch failed ({exc}); skipping")
    else:
        try:
            fz_cached = json.loads(forbidden_path.read_text(encoding="utf-8"))
            log(f"[{key}] forbidden_zones: cached ({len(fz_cached.get('features', []))} polygons), skipping")
        except Exception:
            pass

    meta["counts"] = counts
    meta["baked_at"] = datetime.now(timezone.utc).isoformat()
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (pois_dir / "index.json").write_text(
        json.dumps({"categories": sorted(POI_QUERIES.keys()), "counts": counts}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"[{key}] done ({sum(counts.values())} POIs total)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--cities", nargs="+", default=None,
        help="Subset of city keys to bake (default: all). Keys: " + ", ".join(CITIES),
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch even if cached files exist.",
    )
    parser.add_argument(
        "--add", nargs=2, metavar=("KEY", "QUERY"),
        help="Bake a brand-new city not in CITIES, e.g. --add lund 'Lund, Sweden'. "
             "Edit CITIES in this file to make the addition permanent.",
    )
    parser.add_argument(
        "--cats", nargs="+", default=None,
        help="Subset of POI categories to (re)bake (default: all). "
             "Other categories' cached files are left untouched. "
             "Keys: " + ", ".join(POI_QUERIES),
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List supported city keys and exit.",
    )
    args = parser.parse_args()

    if args.list:
        for key, q in CITIES.items():
            print(f"{key:12s} {q}")
        return 0

    targets: list[tuple[str, str]] = []
    if args.add:
        targets.append((args.add[0], args.add[1]))
    elif args.cities:
        for key in args.cities:
            if key not in CITIES:
                print(f"unknown city key: {key}", file=sys.stderr)
                return 2
            targets.append((key, CITIES[key]))
    else:
        targets = list(CITIES.items())

    def log(msg: str) -> None:
        print(msg, flush=True)

    cats = set(args.cats) if args.cats else None
    if cats:
        unknown = cats - set(POI_QUERIES)
        if unknown:
            print(f"unknown POI categories: {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2

    log(f"baking {len(targets)} city/cities into {CACHE_ROOT}"
        + (f" (categories: {', '.join(sorted(cats))})" if cats else ""))
    for key, query in targets:
        try:
            bake_city(key, query, args.force, log, cats=cats)
        except Exception as exc:
            log(f"[{key}] FAILED: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
