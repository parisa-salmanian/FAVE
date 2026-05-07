#!/usr/bin/env python3
"""
Download building data (Byggnader) from Lantmäteriet's open STAC vector
catalog, one GeoPackage per kommun.

The collection is published under CC-BY-4.0 and currently requires no
authentication — the public Lantmäteriet account agreement is enough.
Each STAC item is one Swedish kommun and ships a single ZIP with a
GeoPackage inside (EPSG:3006 / SWEREF 99 TM).

Examples
--------
    # List every available kommun and its size
    python tools/fetch_lantmateriet_buildings.py --list

    # Pull buildings for a known FAVE city
    python tools/fetch_lantmateriet_buildings.py --city vaxjo

    # Pull several at once
    python tools/fetch_lantmateriet_buildings.py --city malmo --city stockholm

    # Pull by raw 4-digit kommun code (leading zeros optional)
    python tools/fetch_lantmateriet_buildings.py --kommun 0180 --kommun 1480

    # Pull every kommun in Sweden (≈ 290 files, ~3-4 GB unzipped)
    python tools/fetch_lantmateriet_buildings.py --all

The script is idempotent: ZIPs already in lantmateriet/zip/ are skipped
unless --force is passed; GeoPackages are extracted on-the-fly to
lantmateriet/byggnader/byggnad_kn<code>.gpkg.

Override the STAC root with the LM_STAC_BASE env var if needed (mirrors,
testing).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "lantmateriet"
ZIP_DIR = OUT_DIR / "zip"
GPKG_DIR = OUT_DIR / "byggnader"

STAC_BASE = os.environ.get("LM_STAC_BASE", "https://api.lantmateriet.se/stac-vektor/v1")
COLLECTION = "byggnader"
USER_AGENT = "FAVE/lantmateriet-fetcher (https://github.com/claudiodgl/FAVE)"

# FAVE city keys → 4-digit SCB kommun code. Add to this map as more
# cities are supported. The codes are stable Statistics Sweden ids.
CITY_TO_KOMMUN: dict[str, str] = {
    "stockholm":  "0180",
    "uppsala":    "0380",
    "norrkoping": "0581",
    "vaxjo":      "0780",
    "kalmar":     "0880",
    "malmo":      "1280",
    "goteborg":   "1480",
}


def http_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req) as resp:  # noqa: S310 — public STAC catalog, https only
        return json.loads(resp.read().decode("utf-8"))


def http_download(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as resp:  # noqa: S310
        total = int(resp.headers.get("Content-Length") or 0)
        done = 0
        chunk = 1024 * 256
        with tmp.open("wb") as out:
            while True:
                buf = resp.read(chunk)
                if not buf:
                    break
                out.write(buf)
                done += len(buf)
                if total:
                    pct = done * 100 // total
                    sys.stdout.write(
                        f"\r    {dst.name}: {done / 1024 / 1024:6.1f} / "
                        f"{total / 1024 / 1024:6.1f} MB ({pct:3d}%)"
                    )
                    sys.stdout.flush()
        sys.stdout.write("\n")
    tmp.replace(dst)


def iter_items() -> Iterator[dict]:
    """Yield every STAC item in the byggnader collection, paginated."""
    url = f"{STAC_BASE}/collections/{COLLECTION}/items?limit=100"
    while url:
        page = http_json(url)
        yield from page.get("features", [])
        next_link = next((l["href"] for l in page.get("links", []) if l.get("rel") == "next"), None)
        url = next_link


def kommun_code_of(item: dict) -> str:
    """Pull the 4-digit kommun code out of a STAC item, normalized to a string."""
    raw = item.get("id") or item.get("properties", {}).get("kommunkod") or ""
    digits = re.sub(r"\D", "", str(raw))
    if not digits:
        raise ValueError(f"could not extract kommun code from item: {raw!r}")
    return digits.zfill(4)


def first_zip_asset(item: dict) -> tuple[str, str] | None:
    """Return (asset_key, href) for the first ZIP asset on an item."""
    for key, asset in (item.get("assets") or {}).items():
        href = asset.get("href")
        if href and href.lower().endswith(".zip"):
            return key, href
    return None


def extract_gpkg(zip_path: Path, dst: Path) -> bool:
    """Extract the first .gpkg file out of zip_path to dst. Idempotent."""
    if dst.exists() and dst.stat().st_size > 0:
        return False
    with zipfile.ZipFile(zip_path) as zf:
        gpkg_member = next((n for n in zf.namelist() if n.lower().endswith(".gpkg")), None)
        if not gpkg_member:
            print(f"    WARN: no .gpkg inside {zip_path.name}", file=sys.stderr)
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        with zf.open(gpkg_member) as src, dst.open("wb") as out:
            while True:
                buf = src.read(1024 * 1024)
                if not buf:
                    break
                out.write(buf)
    return True


def cmd_list() -> int:
    """Print one row per kommun: code, FAVE city alias (if any), zipped size, name."""
    code_to_city = {v: k for k, v in CITY_TO_KOMMUN.items()}
    rows = []
    for item in iter_items():
        code = kommun_code_of(item)
        zip_asset = first_zip_asset(item) or (None, None)
        href = zip_asset[1]
        size_mb = None
        # STAC sometimes carries file:size in the asset dict.
        for asset in (item.get("assets") or {}).values():
            if asset.get("href") == href:
                fsize = asset.get("file:size") or asset.get("file:size_bytes")
                if isinstance(fsize, (int, float)):
                    size_mb = round(fsize / 1024 / 1024, 1)
                break
        # Item title or kommun name from properties
        props = item.get("properties") or {}
        name = (
            props.get("kommunnamn")
            or props.get("name")
            or item.get("title")
            or item.get("id")
        )
        rows.append((code, code_to_city.get(code, ""), size_mb, str(name)))
    rows.sort(key=lambda r: r[0])
    print(f"{'kn':<6} {'fave':<11} {'mb':>6}  name")
    print("-" * 70)
    for code, alias, size_mb, name in rows:
        size = f"{size_mb:6.1f}" if size_mb is not None else "     ?"
        print(f"{code:<6} {alias:<11} {size}  {name}")
    print(f"\n{len(rows)} kommuner")
    return 0


def select_codes(args) -> list[str]:
    codes: set[str] = set()
    for city in args.city or []:
        c = city.lower()
        if c not in CITY_TO_KOMMUN:
            print(f"ERROR: unknown city '{city}'. Known: {', '.join(CITY_TO_KOMMUN)}", file=sys.stderr)
            sys.exit(2)
        codes.add(CITY_TO_KOMMUN[c])
    for raw in args.kommun or []:
        digits = re.sub(r"\D", "", raw)
        if not digits:
            print(f"ERROR: invalid kommun code '{raw}'", file=sys.stderr)
            sys.exit(2)
        codes.add(digits.zfill(4))
    return sorted(codes)


def cmd_fetch(args) -> int:
    if args.all:
        wanted: set[str] | None = None  # download everything
    else:
        codes = select_codes(args)
        if not codes:
            print("ERROR: pass at least one --city / --kommun, or --all.", file=sys.stderr)
            return 2
        wanted = set(codes)
        print(f"Downloading {len(wanted)} kommun(er): {', '.join(sorted(wanted))}")

    ZIP_DIR.mkdir(parents=True, exist_ok=True)
    GPKG_DIR.mkdir(parents=True, exist_ok=True)

    fetched = skipped = missing = 0
    for item in iter_items():
        code = kommun_code_of(item)
        if wanted is not None and code not in wanted:
            continue
        zip_asset = first_zip_asset(item)
        if not zip_asset:
            print(f"  skip kn{code}: no .zip asset", file=sys.stderr)
            missing += 1
            continue
        _, href = zip_asset
        zip_path = ZIP_DIR / Path(urllib.parse.urlparse(href).path).name
        gpkg_path = GPKG_DIR / f"byggnad_kn{code}.gpkg"

        if zip_path.exists() and zip_path.stat().st_size > 0 and not args.force:
            print(f"  ok   kn{code}  {zip_path.name} (cached)")
            skipped += 1
        else:
            print(f"  get  kn{code}  <-  {href}")
            try:
                http_download(href, zip_path)
                fetched += 1
            except urllib.error.HTTPError as exc:
                print(f"    HTTP {exc.code}: {exc.reason}", file=sys.stderr)
                continue
            except urllib.error.URLError as exc:
                print(f"    network error: {exc.reason}", file=sys.stderr)
                continue

        if not args.no_extract:
            try:
                extract_gpkg(zip_path, gpkg_path)
            except zipfile.BadZipFile:
                print(f"    WARN: {zip_path.name} is not a valid zip; will retry on next run.", file=sys.stderr)
                zip_path.unlink(missing_ok=True)

    print(f"\nDone. {fetched} downloaded, {skipped} cached, {missing} skipped.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true", help="List every kommun in the collection and exit.")
    ap.add_argument("--all", action="store_true", help="Download every kommun in Sweden (~290 files).")
    ap.add_argument("--city", action="append", help=f"FAVE city key. Known: {', '.join(CITY_TO_KOMMUN)}")
    ap.add_argument("--kommun", action="append", help="Raw 4-digit kommun code (leading zeros optional). Repeatable.")
    ap.add_argument("--force", action="store_true", help="Re-download ZIPs even if they exist.")
    ap.add_argument("--no-extract", action="store_true", help="Skip GeoPackage extraction (just keep the ZIP).")
    args = ap.parse_args()

    if args.list:
        return cmd_list()
    return cmd_fetch(args)


if __name__ == "__main__":
    sys.exit(main())
