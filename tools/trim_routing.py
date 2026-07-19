"""Shrink baked routing files for faster frontend loading — no OSRM re-run.

The IF-City model uses only the 3 nearest POIs' road distance (metres) and the
opportunity weight v; it never reads the seconds array. The baker stored top-5
plus a seconds array, which made the files ~2.5x larger than needed (Göteborg's
walking set was ~299 MB). This rewrites every routing/<mode>/<cat>.json in place
to keep only top-3 m (rounded to int) and top-3 v, dropping s. Same scores,
much smaller download + JSON.parse + Map build.

Usage:
    .venv/bin/python tools/trim_routing.py            # all cities
    .venv/bin/python tools/trim_routing.py --cities goteborg stockholm
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CITIES_ROOT = REPO_ROOT / "frontend" / "assets" / "data" / "cities"
KEEP_K = 3


def trim_file(path: Path) -> tuple[int, int]:
    before = path.stat().st_size
    d = json.loads(path.read_text(encoding="utf-8"))
    keys = d.get("key", [])
    m = d.get("m", [])
    v = d.get("v", [])
    out = {
        "mode": d.get("mode"),
        "cat": d.get("cat"),
        "k": KEEP_K,
        "key": keys,
        # top-3 metres as ints; top-3 weights (usually 1)
        "m": [[round(x) for x in (row or [])[:KEEP_K]] for row in m],
        "v": [[round(x, 3) for x in (row or [])[:KEEP_K]] for row in v],
    }
    path.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    return before, path.stat().st_size


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cities", nargs="+", default=None)
    args = p.parse_args()

    cities = args.cities or [d.name for d in CITIES_ROOT.iterdir() if d.is_dir()]
    tot_before = tot_after = 0
    for city in sorted(cities):
        rdir = CITIES_ROOT / city / "routing"
        if not rdir.exists():
            continue
        cb = ca = 0
        for f in rdir.rglob("*.json"):
            if f.name == "index.json":
                continue
            try:
                b, a = trim_file(f)
                cb += b
                ca += a
            except Exception as e:
                print(f"  skip {f}: {e}")
        if cb:
            print(f"{city:12s} {cb/1e6:7.1f} MB -> {ca/1e6:7.1f} MB "
                  f"({100*(1-ca/cb):.0f}% smaller)")
            tot_before += cb
            tot_after += ca
    if tot_before:
        print(f"{'TOTAL':12s} {tot_before/1e6:7.1f} MB -> {tot_after/1e6:7.1f} MB "
              f"({100*(1-tot_after/tot_before):.0f}% smaller)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
