#!/usr/bin/env python3
"""
Download building data (Byggnader) from Lantmäteriet's STAC vector
catalog, one GeoPackage per kommun.

Catalog browsing (listing kommuner, reading metadata) is open. The actual
ZIP downloads from dl1.lantmateriet.se require **HTTP Basic Auth** with
the username/password issued to your Lantmäteriet account. Pass them via
env vars:

    export LM_USERNAME=...   # set once per shell, or put in .env
    export LM_PASSWORD=...

The script also reads `.lantmateriet_credentials` and a top-level `.env`
file (KEY=VALUE per line) if present, so you don't have to keep
re-exporting. Credentials are NEVER printed to stdout/stderr or written
to log files.

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
import base64
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


def _load_credentials_file(path: Path) -> None:
    """Parse a KEY=VALUE file and merge into os.environ if not already set."""
    if not path.is_file():
        return
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k, v)
    except OSError:
        pass


# Try the obvious local stash spots so the user doesn't have to keep
# re-exporting LM_USERNAME / LM_PASSWORD. We never print or log values.
_load_credentials_file(REPO_ROOT / ".env")
_load_credentials_file(REPO_ROOT / ".lantmateriet_credentials")
_load_credentials_file(Path.home() / ".lantmateriet_credentials")


def _basic_auth_header() -> dict[str, str]:
    user = os.environ.get("LM_USERNAME")
    pw = os.environ.get("LM_PASSWORD")
    if not user or not pw:
        return {}
    token = base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def http_json(url: str) -> dict:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    headers.update(_basic_auth_header())  # harmless on the open catalog
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:  # noqa: S310 — https STAC catalog
        return json.loads(resp.read().decode("utf-8"))


def http_download(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    headers = {"User-Agent": USER_AGENT}
    headers.update(_basic_auth_header())
    req = urllib.request.Request(url, headers=headers)
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


# Each STAC item's title is "Byggnader för <Kommun> kommun" — pull the
# readable name out of there so we don't need a separate mapping file.
_TITLE_RE = re.compile(r"Byggnader\s+f[öo]r\s+(.+?)\s+kommun", re.IGNORECASE)


def kommun_name_of(item: dict) -> str:
    title = (item.get("properties") or {}).get("title") or item.get("title") or ""
    m = _TITLE_RE.search(str(title))
    if m:
        return m.group(1).strip()
    desc = (item.get("properties") or {}).get("description") or item.get("description") or ""
    m = _TITLE_RE.search(str(desc))
    return m.group(1).strip() if m else ""


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


def cmd_list(name_filter: str | None = None) -> int:
    """Print one row per kommun: code, FAVE city alias (if any), zipped size, name."""
    code_to_city = {v: k for k, v in CITY_TO_KOMMUN.items()}
    rows = []
    for item in iter_items():
        code = kommun_code_of(item)
        name = kommun_name_of(item) or item.get("id", "")
        if name_filter and name_filter.lower() not in name.lower():
            continue
        zip_asset = first_zip_asset(item) or (None, None)
        href = zip_asset[1]
        size_mb = None
        for asset in (item.get("assets") or {}).values():
            if asset.get("href") == href:
                fsize = asset.get("file:size") or asset.get("file:size_bytes")
                if isinstance(fsize, (int, float)):
                    size_mb = round(fsize / 1024 / 1024, 1)
                break
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
    if args.kommun_name:
        # Resolve names to codes by walking the catalog and matching titles.
        name_filter = [n.lower() for n in args.kommun_name]
        for item in iter_items():
            kname = kommun_name_of(item).lower()
            if kname and any(needle in kname for needle in name_filter):
                codes.add(kommun_code_of(item))
    return sorted(codes)


def cmd_fetch(args) -> int:
    if not _basic_auth_header():
        print(
            "ERROR: download endpoints require Basic Auth. Set LM_USERNAME and "
            "LM_PASSWORD env vars (or write them to .env / .lantmateriet_credentials).",
            file=sys.stderr,
        )
        return 2

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
    ap.add_argument("--kommun-name", action="append", help="Case-insensitive substring match on the kommun name (e.g. 'Lund'). Repeatable.")
    ap.add_argument("--force", action="store_true", help="Re-download ZIPs even if they exist.")
    ap.add_argument("--no-extract", action="store_true", help="Skip GeoPackage extraction (just keep the ZIP).")
    args = ap.parse_args()

    if args.list:
        # When --list is combined with a name filter, narrow the catalog readout.
        return cmd_list(name_filter=args.kommun_name[0] if args.kommun_name else None)
    return cmd_fetch(args)


if __name__ == "__main__":
    sys.exit(main())
