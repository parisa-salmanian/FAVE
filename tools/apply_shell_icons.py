#!/usr/bin/env python3
"""Replace entries in the ICONS table of frontend/assets/js/views/shell.js
with artwork converted from the PDF icon set.

The shell's svg() helper wraps every icon in
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
which suits line icons. The PDF set is solid glyphs, so each emitted path
overrides that with fill="currentColor" stroke="none", and uses fill-rule
"evenodd" so light knockout shapes (bus windows, car glass, wheel spokes)
stay as real holes instead of flooding solid.

Usage:
    python tools/apply_shell_icons.py --pdf-dir ~/Downloads [--pad 3.0] [--dry-run]
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SHELL_JS = HERE.parent / "frontend" / "assets" / "js" / "views" / "shell.js"

# ICONS key in shell.js  ->  source PDF stem
MAPPING = {
    "district": "macro",     # rail · Macro (districts)
    "hexagons": "meso",      # rail · Meso (hexagons)
    "building": "micro",     # rail · Micro (buildings)
    "walk": "walking",       # travel mode · Walk
    "bike": "cycling",       # travel mode · Cycle
    "car": "driving",        # travel mode · Car
    "bus": "transit",        # travel mode · Transit
}


def load_converter():
    spec = importlib.util.spec_from_file_location("p2s", HERE / "pdf_icon_to_svg.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_entries(pdf_dir: Path, pad: float, size: float):
    conv = load_converter()
    out = {}
    for key, stem in MAPPING.items():
        pdf = pdf_dir / f"{stem}.pdf"
        if not pdf.exists():
            raise FileNotFoundError(pdf)
        r = conv.convert(pdf, size, pad)
        # One path, evenodd: overlapping light shapes become holes.
        out[key] = (
            f'<path fill="currentColor" stroke="none" fill-rule="evenodd" '
            f'd="{r["combined_d"]}"/>'
        )
    return out


def patch(entries: dict, dry_run: bool = False):
    src = SHELL_JS.read_text(encoding="utf-8")
    original = src
    report = []

    for key, markup in entries.items():
        # Match one whole `key: '...',` line inside the ICONS table.
        pattern = re.compile(
            r"^(?P<indent>[ \t]*)" + re.escape(key) + r":\s*'(?P<old>(?:[^'\\]|\\.)*)',[ \t]*$",
            re.M,
        )
        m = pattern.search(src)
        if not m:
            report.append((key, "NOT FOUND", 0, 0))
            continue
        if "'" in markup:
            raise ValueError(f"{key}: generated markup contains a quote")
        replacement = f"{m.group('indent')}{key}: '{markup}',"
        src = src[: m.start()] + replacement + src[m.end():]
        report.append((key, "replaced", len(m.group("old")), len(markup)))

    if not dry_run and src != original:
        SHELL_JS.write_text(src, encoding="utf-8", newline="\n")
    return report, src != original


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-dir", type=Path, default=Path.home() / "Downloads")
    ap.add_argument("--pad", type=float, default=3.0,
                    help="padding inside the 24-unit viewBox (higher = smaller glyph)")
    ap.add_argument("--size", type=float, default=24.0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    entries = build_entries(args.pdf_dir.expanduser(), args.pad, args.size)
    report, changed = patch(entries, args.dry_run)

    for key, status, old_len, new_len in report:
        print(f"  {key:9s} {status:10s} {old_len:5d} -> {new_len:5d} chars")
    print(("dry run, nothing written" if args.dry_run
           else ("shell.js updated" if changed else "no change")))
    return 0 if all(r[1] == "replaced" for r in report) else 1


if __name__ == "__main__":
    raise SystemExit(main())
