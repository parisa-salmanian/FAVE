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
    "school_primary": ['nwr["amenity"="school"]'],
    "school_high": [
        'nwr["amenity"="school"]',
        'nwr["amenity"="college"]',
        'nwr["school:level"~"upper|secondary|gymnas|high",i]',
        'nwr["education:level"~"upper|secondary|gymnas|high",i]',
        'nwr["isced:level"~"3",i]',
    ],
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


def filter_school_subtype(features: list[dict[str, Any]], category: str) -> list[dict[str, Any]]:
    """For school_primary and school_high, mirror the post-filter in fetchPOIs."""
    if category not in {"school_primary", "school_high"}:
        return features

    def parse_isced_digits(val: Any) -> list[int]:
        return [int(c) for c in str(val or "") if c.isdigit()]

    def text_for(tags: dict[str, Any]) -> str:
        return " ".join(
            str(tags.get(k) or "").lower()
            for k in ("school:level", "education:level", "level")
        )

    def is_primary(tags: dict[str, Any]) -> bool:
        t = text_for(tags)
        if any(kw in t for kw in ("grund", "primary", "lower", "elementary")):
            return True
        nums = parse_isced_digits(tags.get("isced:level"))
        return 1 in nums or 2 in nums

    def is_high(tags: dict[str, Any]) -> bool:
        t = text_for(tags)
        if any(kw in t for kw in ("gymnas", "high", "upper", "secondary")):
            return True
        return 3 in parse_isced_digits(tags.get("isced:level"))

    pred = is_primary if category == "school_primary" else is_high
    filtered = [f for f in features if pred(f.get("properties", {}).get("tags", {}) or {})]
    # If the post-filter discards everything, keep the unfiltered set so the
    # frontend at least has data — same fallback behavior as fetchPOIs.
    return filtered if filtered else features


def build_overpass_body(bbox: list[float], selectors: list[str]) -> str:
    south, west, north, east = bbox
    sel_block = "\n      ".join(f"{q}({south},{west},{north},{east});" for q in selectors)
    return (
        f"[out:json][timeout:120];\n"
        f"(\n      {sel_block}\n);\n"
        f"out center tags;"
    )


def bake_city(key: str, query_string: str, force: bool, log) -> None:
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

    log(f"baking {len(targets)} city/cities into {CACHE_ROOT}")
    for key, query in targets:
        try:
            bake_city(key, query, args.force, log)
        except Exception as exc:
            log(f"[{key}] FAILED: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
