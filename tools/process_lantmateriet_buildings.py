#!/usr/bin/env python3
"""
Convert Lantmäteriet kommun GeoPackages into FAVE-shape building geojsons.

After running tools/fetch_lantmateriet_buildings.py to grab one or more
`lantmateriet/byggnader/byggnad_kn<code>.gpkg` files, this script:

  1. Reads each GeoPackage (EPSG:3006 / SWEREF 99 TM).
  2. Reprojects geometry to EPSG:4326 (lon/lat — what the frontend uses).
  3. Drops attribute columns FAVE doesn't read (mirrors slim_city_files.py).
  4. Writes a compact GeoJSON FeatureCollection to
     frontend/assets/data/byggnad_<city>.geojson when the kommun matches
     a known FAVE city key, else byggnad_kn<code>.geojson.

Examples
--------
    # Process every .gpkg currently in lantmateriet/byggnader/
    python tools/process_lantmateriet_buildings.py

    # Just one or two
    python tools/process_lantmateriet_buildings.py --city vaxjo --city malmo
    python tools/process_lantmateriet_buildings.py --kommun 0180

    # Don't overwrite existing outputs
    python tools/process_lantmateriet_buildings.py --skip-existing

Requires geopandas (already a transitive dep of osmnx in requirements.txt).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GPKG_DIR = REPO_ROOT / "lantmateriet" / "byggnader"
OUT_DIR = REPO_ROOT / "frontend" / "assets" / "data"

# Reverse of the alias map in fetch_lantmateriet_buildings.py.
KOMMUN_TO_CITY: dict[str, str] = {
    "0180": "stockholm",
    "0380": "uppsala",
    "0581": "norrkoping",
    "0780": "vaxjo",
    "0880": "kalmar",
    "1280": "malmo",
    "1480": "goteborg",
}

# Which Lantmäteriet attribute columns FAVE actually reads. Mirrors the
# KEEP set in slim_city_files.py but drops OSM-only fields. Anything not
# in here is dropped on serialise to keep the geojson small.
KEEP_ATTRS = frozenset({
    # Stable id
    "objektidentitet",
    # Building classification (Lantmäteriet)
    "objekttyp", "objektnamn", "objekttypnr",
    "byggnadsnamn1", "byggnadsnamn2", "byggnadsnamn3",
    "andamal1", "andamal2", "andamal3", "andamal4", "andamal5",
    "husnummer", "huvudbyggnad", "insamlingslage",
    # Heights
    "height", "height_m", "Hojd", "hojd", "HOJD",
    "byggnad_hojd", "_mean",
    # Year of construction (if present)
    "nybyggnadsar", "byggnadsar",
    # Free-form name (used by popup as fallback)
    "namn",
})


def detect_geopackages(args) -> list[Path]:
    if args.city or args.kommun:
        codes: set[str] = set()
        for city in args.city or []:
            c = city.lower()
            for code, key in KOMMUN_TO_CITY.items():
                if key == c:
                    codes.add(code)
                    break
            else:
                print(f"ERROR: unknown city '{city}'. Known: {', '.join(KOMMUN_TO_CITY.values())}", file=sys.stderr)
                sys.exit(2)
        for raw in args.kommun or []:
            digits = re.sub(r"\D", "", raw)
            if digits:
                codes.add(digits.zfill(4))
        return [GPKG_DIR / f"byggnad_kn{c}.gpkg" for c in sorted(codes)]
    return sorted(GPKG_DIR.glob("byggnad_kn*.gpkg"))


def output_path_for(gpkg_path: Path) -> Path:
    m = re.match(r"byggnad_kn(\d+)\.gpkg$", gpkg_path.name, re.IGNORECASE)
    if not m:
        return OUT_DIR / gpkg_path.with_suffix(".geojson").name
    code = m.group(1).zfill(4)
    city = KOMMUN_TO_CITY.get(code)
    name = f"byggnad_{city}.geojson" if city else f"byggnad_kn{code}.geojson"
    return OUT_DIR / name


def process_gpkg(gpkg_path: Path, out_path: Path) -> dict:
    """Read .gpkg → reproject → slim attrs → write compact geojson."""
    # Lazy import so --help works without geopandas installed.
    import geopandas as gpd  # type: ignore

    gdf = gpd.read_file(gpkg_path)
    feats_in = len(gdf)
    cols_in = list(gdf.columns)

    # Reproject if needed.
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Normalize height column: Lantmäteriet GPKGs use 'hojd' (or 'Hojd'/'HOJD');
    # rename to 'height_m' so the frontend can find it without guessing variants.
    for src in ("hojd", "Hojd", "HOJD", "byggnad_hojd"):
        if src in gdf.columns and "height_m" not in gdf.columns:
            gdf = gdf.rename(columns={src: "height_m"})
            break

    # Slim to KEEP_ATTRS + geometry.
    keep_cols = [c for c in gdf.columns if c == gdf.geometry.name or c in KEEP_ATTRS]
    dropped_cols = sorted(set(cols_in) - set(keep_cols))
    gdf = gdf[keep_cols]

    # Write compact GeoJSON. Use json.dump for tighter control than
    # geopandas's default (which pretty-prints by default in some versions).
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.loads(gdf.to_json())
    # Drop any remaining None / "" values to shrink further.
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        feat["properties"] = {k: v for k, v in props.items() if v not in (None, "")}
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = out_path.stat().st_size / 1024 / 1024
    return {
        "gpkg": gpkg_path.name,
        "out": out_path.name,
        "feats": feats_in,
        "out_mb": size_mb,
        "dropped_cols": dropped_cols,
        "kept_cols": sorted(c for c in keep_cols if c != gdf.geometry.name),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--city", action="append", help=f"FAVE city key. Known: {', '.join(KOMMUN_TO_CITY.values())}")
    ap.add_argument("--kommun", action="append", help="Raw 4-digit kommun code. Repeatable.")
    ap.add_argument("--skip-existing", action="store_true", help="Skip kommuner whose output already exists.")
    args = ap.parse_args()

    try:
        import geopandas  # noqa: F401
    except ImportError:
        print("ERROR: geopandas is required. Install via:\n    pip install geopandas", file=sys.stderr)
        return 1

    gpkgs = detect_geopackages(args)
    if not gpkgs:
        print(f"ERROR: no GeoPackages found under {GPKG_DIR}.", file=sys.stderr)
        print("Run tools/fetch_lantmateriet_buildings.py first to download data.", file=sys.stderr)
        return 2

    processed = skipped = failed = 0
    for gpkg in gpkgs:
        if not gpkg.exists():
            print(f"  miss {gpkg.name}: not downloaded yet")
            failed += 1
            continue
        out = output_path_for(gpkg)
        if args.skip_existing and out.exists():
            print(f"  skip {gpkg.name} -> {out.name} (already exists)")
            skipped += 1
            continue
        try:
            print(f"  read {gpkg.name}")
            result = process_gpkg(gpkg, out)
            print(
                f"  -> {result['out']}  {result['feats']} feats, "
                f"{result['out_mb']:.1f} MB, "
                f"dropped {len(result['dropped_cols'])} cols"
            )
            processed += 1
        except Exception as exc:
            print(f"  FAIL {gpkg.name}: {exc}", file=sys.stderr)
            failed += 1

    print(f"\nDone. {processed} processed, {skipped} skipped, {failed} failed.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
