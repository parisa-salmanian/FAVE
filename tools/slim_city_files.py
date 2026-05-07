"""Strip unused properties from city building GeoJSON files.

Each Lantmäteriet building feature ships with 20 properties; FAVE actually uses
about 10 of them. The rest (collection metadata, version info, positional
uncertainty, etc.) inflate file size and JSON-parse time without being read
anywhere in the JS. This script overwrites each `byggnad_*.geojson` (and the
Lantmäteriet aggregate) in place with only the whitelisted properties, and
re-serialises with compact separators.

A `.bak` copy of each file is created on first run so you can revert.

Run:
    python tools/slim_city_files.py            # slim all known building files
    python tools/slim_city_files.py --dry-run  # just report what would change
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "frontend" / "assets" / "data"

# Properties that the FAVE JS code actually reads off building features.
# Everything else gets stripped on serialise.
KEEP = frozenset({
    # Stable IDs
    "objektidentitet", "id", "@id", "osm_id", "byggnadsid",
    # Lantmäteriet building classification
    "objekttyp", "objektnamn",
    "byggnadsnamn1", "byggnadsnamn2", "byggnadsnamn3", "namn",
    "andamal1", "andamal2", "andamal3", "andamal4", "andamal5",
    # OSM-style classification (some derived files use these)
    "name", "building", "amenity", "shop", "office", "healthcare",
    "leisure", "tourism", "public_transport", "railway", "aeroway",
    "category", "category_label",
    # School-tag classifiers (used by lib/poi.js)
    "school:level", "education:level", "level", "isced:level",
    # Heights / extrusion
    "height", "height_m", "Hojd", "building:height", "building:levels",
    "floors", "_mean",
    # Year of construction
    "built_year", "start_date", "construction:year",
    # Sentinel-1 change-detection (used by DR view)
    "change_score", "is_change",
    # District / demographics keys (used by some derived files)
    "regso", "deso", "resident_group", "population_group",
    # OSM tags container (used by POI features, kept for safety)
    "tags",
    # Source tag (used by some popups)
    "src", "source", "osm_type", "brand", "operator", "operator:type",
    # Address (popup)
    "addr:housenumber", "addr:street", "addr:city", "addr:postcode",
    # Extras kept for PASSTHRU compatibility in popups
    "wikidata", "wikipedia", "ref",
})

# Files we know are city building data and want to slim.
TARGETS = [
    "byggnad_goteborg.geojson",
    "byggnad_kalmar.geojson",
    "byggnad_malmo.geojson",
    "byggnad_norrkoping.geojson",
    "byggnad_stockholm.geojson",
    "byggnad_uppsala.geojson",
    "lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson",
    "bldg_web.geojson",
]


def slim_file(path: Path, dry_run: bool) -> dict:
    size_before = path.stat().st_size
    t0 = time.perf_counter()
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    t_read = time.perf_counter() - t0

    feats = data.get("features", [])
    prop_keys_before = set()
    prop_keys_kept = set()
    bytes_dropped = 0
    for feat in feats:
        props = feat.get("properties") or {}
        prop_keys_before.update(props.keys())
        slim_props = {}
        for k, v in props.items():
            if k in KEEP and v is not None and v != "":
                slim_props[k] = v
                prop_keys_kept.add(k)
            else:
                bytes_dropped += len(str(v)) if v is not None else 0
        feat["properties"] = slim_props

    if dry_run:
        return {
            "name": path.name,
            "before_mb": size_before / 1024 / 1024,
            "feats": len(feats),
            "before_keys": sorted(prop_keys_before),
            "kept_keys": sorted(prop_keys_kept),
            "stripped_keys": sorted(prop_keys_before - prop_keys_kept),
            "dry": True,
        }

    bak = path.with_suffix(path.suffix + ".bak")
    if not bak.exists():
        path.replace(bak)  # rename current to .bak
    else:
        # bak already exists from a prior run — leave it alone, just overwrite path
        pass
    # write slimmed JSON in compact form
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    size_after = path.stat().st_size
    return {
        "name": path.name,
        "before_mb": size_before / 1024 / 1024,
        "after_mb": size_after / 1024 / 1024,
        "feats": len(feats),
        "saved_pct": 100 * (1 - size_after / size_before) if size_before else 0,
        "stripped_keys": sorted(prop_keys_before - prop_keys_kept),
        "kept_keys": sorted(prop_keys_kept),
        "read_s": round(t_read, 2),
        "dry": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report only — do not modify files")
    parser.add_argument("--files", nargs="+", default=None, help="Specific filenames to process (default: all known)")
    args = parser.parse_args()

    targets = args.files if args.files else TARGETS
    total_before = 0
    total_after = 0

    for name in targets:
        path = DATA_DIR / name
        if not path.exists():
            print(f"  SKIP {name}: not found")
            continue
        try:
            result = slim_file(path, args.dry_run)
        except Exception as exc:
            print(f"  FAIL {name}: {exc}")
            continue

        if result["dry"]:
            print(
                f"{name}: {result['before_mb']:.1f} MB · {result['feats']} feats · "
                f"strip {len(result['stripped_keys'])}/"
                f"{len(result['kept_keys'])+len(result['stripped_keys'])} props"
            )
            print(f"  keep:  {result['kept_keys']}")
            print(f"  strip: {result['stripped_keys']}")
        else:
            total_before += result["before_mb"]
            total_after += result["after_mb"]
            print(
                f"{name}: {result['before_mb']:.1f} → {result['after_mb']:.1f} MB "
                f"(-{result['saved_pct']:.1f}%, {result['feats']} feats, "
                f"read {result['read_s']}s)"
            )
            print(f"  stripped: {result['stripped_keys']}")

    if not args.dry_run and total_before:
        print(f"\nTOTAL: {total_before:.1f} → {total_after:.1f} MB "
              f"(-{100 * (1 - total_after / total_before):.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
