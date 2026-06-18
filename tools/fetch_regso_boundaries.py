#!/usr/bin/env python3
"""Fetch RegSO (Regionala statistikomraden) boundaries per municipality from
SCB's open geodata WFS and save one GeoJSON per city.

External API (SCB GeoServer WFS) — DEV/BAKE time only, never at runtime
(CLAUDE.md hard rule). Feeds tools/bake_demographics.py, which spatial-joins
each DESO to its RegSO to attach SCB RegSO-level socioeconomic signals.

Source: https://geodata.scb.se/geoserver/stat/wfs  (layer stat:RegSO_2025,
open data, CC0). We pull only the kommuns FAVE supports (CQL_FILTER on
kommunkod), reproject SWEREF99 TM (EPSG:3006) -> WGS84, and write
  frontend/assets/data/<City>_regso.geojson
(same schema/name as the pre-existing Vaxjo_regso.geojson, props: regsokod,
kommunkod, regsonamn).

Usage:
  python tools/fetch_regso_boundaries.py                # all supported cities
  python tools/fetch_regso_boundaries.py --cities malmo goteborg
"""
from __future__ import annotations
import argparse, io, json, urllib.request
from pathlib import Path

DATA_ROOT = Path(__file__).resolve().parents[1] / "frontend" / "assets" / "data"
WFS = "https://geodata.scb.se/geoserver/stat/wfs"
LAYER = "stat:RegSO_2025"

# city key -> (Capitalized file stem, SCB kommun code). Vaxjo already has a file.
CITIES = {
    "vaxjo": ("Vaxjo", "0780"), "malmo": ("Malmo", "1280"),
    "goteborg": ("Goteborg", "1480"), "stockholm": ("Stockholm", "0180"),
    "kalmar": ("Kalmar", "0880"), "norrkoping": ("Norrkoping", "0581"),
    "uppsala": ("Uppsala", "0380"),
}


def fetch_kommun(kommun: str):
    """GeoDataFrame of RegSO polygons for one kommun, reprojected to WGS84."""
    import geopandas as gpd
    url = (f"{WFS}?service=WFS&version=2.0.0&request=GetFeature"
           f"&typeNames={LAYER}&outputFormat=application/json"
           f"&CQL_FILTER=kommunkod%3D%27{kommun}%27")
    with urllib.request.urlopen(url, timeout=120) as r:
        raw = r.read()
    gdf = gpd.read_file(io.BytesIO(raw))
    if gdf.empty:
        return gdf
    # WFS declares EPSG:3006; force it then reproject to lon/lat.
    gdf = gdf.set_crs(3006, allow_override=True).to_crs(4326)
    keep = [c for c in ("regsokod", "regsonamn", "kommunkod", "lanskod") if c in gdf.columns]
    return gdf[keep + ["geometry"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cities", nargs="*", default=list(CITIES))
    args = ap.parse_args()
    for key in args.cities:
        if key not in CITIES:
            print(f"[{key}] unknown city, skip"); continue
        stem, kommun = CITIES[key]
        print(f"[{key}] fetching RegSO_2025 for kommun {kommun} ...", flush=True)
        try:
            gdf = fetch_kommun(kommun)
        except Exception as e:
            print(f"  [{key}] FAILED: {type(e).__name__}: {e}"); continue
        if gdf.empty:
            print(f"  [{key}] no features for kommun {kommun}"); continue
        out = DATA_ROOT / f"{stem}_regso.geojson"
        out.write_text(gdf.to_json(), encoding="utf-8")
        print(f"  [{key}] -> {out.name}: {len(gdf)} RegSO areas "
              f"({out.stat().st_size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
