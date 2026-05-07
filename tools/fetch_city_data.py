#!/usr/bin/env python3
"""
Download FAVE runtime data files (building geojsons, etc.) that are too
large to ship inside git. Reads tools/data_manifest.json and pulls only
the files needed for the cities you ask for.

Examples:
    python tools/fetch_city_data.py --city vaxjo
    python tools/fetch_city_data.py --city malmo --city stockholm
    python tools/fetch_city_data.py --all
    python tools/fetch_city_data.py --list

Override the host:
    FAVE_DATA_BASE_URL=https://my-host.example/fave python tools/fetch_city_data.py --city vaxjo

The script is idempotent: it skips files that already exist locally with
the expected size (or any non-empty file if no size is in the manifest).
Re-run any time to top up new files.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "tools" / "data_manifest.json"


def load_manifest() -> dict:
    with MANIFEST_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def base_url(manifest: dict) -> str:
    return os.environ.get("FAVE_DATA_BASE_URL") or manifest["base_url"]


def needs_download(local_path: Path, expected_mb: int | None) -> bool:
    if not local_path.exists():
        return True
    if local_path.stat().st_size == 0:
        return True
    if expected_mb is not None and expected_mb > 0:
        # Treat anything under half the expected size as a partial/aborted
        # download. We'd rather re-fetch than ship a truncated file.
        if local_path.stat().st_size < (expected_mb * 1024 * 1024) // 2:
            return True
    return False


def download(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    try:
        with urllib.request.urlopen(url) as resp:  # noqa: S310 (intentional http fetch)
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
                            f"\r    {dst.name}: {done / 1024 / 1024:6.1f} MB / "
                            f"{total / 1024 / 1024:6.1f} MB  ({pct:3d}%)"
                        )
                        sys.stdout.flush()
            sys.stdout.write("\n")
        tmp.replace(dst)
    except Exception:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
        raise


def fetch_entries(entries, base: str, label: str) -> tuple[int, int]:
    fetched = skipped = 0
    if not entries:
        return fetched, skipped
    print(f"\n[{label}]")
    for entry in entries:
        local = REPO_ROOT / entry["local"]
        remote = entry["remote"]
        size_mb = entry.get("size_mb")
        if not needs_download(local, size_mb):
            print(f"  ok   {entry['local']}")
            skipped += 1
            continue
        url = f"{base}/{remote}"
        print(f"  get  {entry['local']}  <-  {url}")
        try:
            download(url, local)
            fetched += 1
        except urllib.error.HTTPError as exc:
            print(f"    HTTP {exc.code}: {exc.reason}", file=sys.stderr)
        except urllib.error.URLError as exc:
            print(f"    network error: {exc.reason}", file=sys.stderr)
    return fetched, skipped


def main() -> int:
    manifest = load_manifest()
    cities = manifest.get("cities", {})

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--city", action="append", default=[], help="City key (vaxjo/malmo/goteborg/stockholm/norrkoping/uppsala/kalmar). Repeat for multiple.")
    ap.add_argument("--all", action="store_true", help="Download data for every city.")
    ap.add_argument("--list", action="store_true", help="Print the manifest summary and exit.")
    ap.add_argument("--no-shared", action="store_true", help="Skip the shared (Sweden-wide) files.")
    args = ap.parse_args()

    if args.list:
        print(f"base_url = {base_url(manifest)}")
        print("\nshared:")
        for entry in manifest.get("shared", []):
            print(f"  - {entry['local']}  ({entry.get('size_mb', '?')} MB)")
        print("\ncities:")
        for key, meta in cities.items():
            files = meta.get("files", [])
            total_mb = sum(f.get("size_mb", 0) for f in files)
            print(f"  - {key} ({meta.get('label', key)}): {len(files)} files, ~{total_mb} MB")
        return 0

    if args.all:
        wanted = list(cities.keys())
    else:
        wanted = args.city
    if not wanted:
        ap.print_help()
        print("\nERROR: pick at least one --city, or pass --all.", file=sys.stderr)
        return 2

    unknown = [c for c in wanted if c not in cities]
    if unknown:
        print(f"ERROR: unknown city key(s): {', '.join(unknown)}", file=sys.stderr)
        print(f"Known: {', '.join(cities.keys())}", file=sys.stderr)
        return 2

    base = base_url(manifest)
    print(f"Source: {base}")

    total_fetched = total_skipped = 0
    if not args.no_shared:
        f, s = fetch_entries(manifest.get("shared", []), base, "shared")
        total_fetched += f
        total_skipped += s

    for key in wanted:
        meta = cities[key]
        f, s = fetch_entries(meta.get("files", []), base, f"{key} ({meta.get('label', key)})")
        total_fetched += f
        total_skipped += s

    print(f"\nDone. {total_fetched} downloaded, {total_skipped} already present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
