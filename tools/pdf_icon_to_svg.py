#!/usr/bin/env python3
"""Convert a simple vector PDF icon into SVG path data for the FAVE shell.

The rail/travel icons are authored as one-page PDFs containing nothing but
filled paths (no fonts, no images, no shading). That is a small enough subset
of the PDF imaging model to translate directly, which keeps the icons true
vectors instead of round-tripping them through a bitmap.

Output is normalised into a square viewBox (24x24 by default) so the result
drops straight into the ICONS table in frontend/assets/js/views/shell.js.

Usage:
    python tools/pdf_icon_to_svg.py ICON.pdf [more.pdf ...] [--size 24]
        [--pad 1.5] [--json]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zlib
from pathlib import Path

# --------------------------------------------------------------------------
# Minimal PDF object access
# --------------------------------------------------------------------------

OBJ_RE = re.compile(rb"(\d+)\s+(\d+)\s+obj\b(.*?)\bendobj", re.S)


def load_objects(data: bytes) -> dict[int, bytes]:
    """Map object number -> raw body (dictionary plus optional stream)."""
    return {int(m.group(1)): m.group(3) for m in OBJ_RE.finditer(data)}


def stream_bytes(body: bytes) -> bytes | None:
    """Return the decoded stream payload of an object body, if it has one."""
    m = re.search(rb"stream\r?\n", body)
    if not m:
        return None
    start = m.end()
    end = body.find(b"endstream", start)
    if end == -1:
        return None
    raw = body[start:end]
    # Trailing EOL before `endstream` is not part of the data.
    raw = raw.rstrip(b"\r\n")
    if b"/FlateDecode" in body:
        try:
            return zlib.decompress(raw)
        except zlib.error:
            try:
                return zlib.decompressobj().decompress(raw)
            except zlib.error:
                return None
    return raw


def content_stream(data: bytes) -> bytes:
    """Find the page's content stream (never the ICC profile or metadata)."""
    objects = load_objects(data)

    page_body = next(
        (b for b in objects.values() if re.search(rb"/Type\s*/Page\b", b)), None
    )
    if page_body is not None:
        m = re.search(rb"/Contents\s+(\d+)\s+\d+\s+R", page_body)
        if m:
            payload = stream_bytes(objects.get(int(m.group(1)), b""))
            if payload:
                return payload

    # Fallback: the decoded stream that actually looks like page content.
    best = b""
    for body in objects.values():
        payload = stream_bytes(body)
        if not payload or b"acspMSFT" in payload[:64]:   # ICC profile
            continue
        if re.search(rb"(?m)^\s*[-\d.]+\s+[-\d.]+\s+m\s*$", payload) and len(payload) > len(best):
            best = payload
    if not best:
        raise ValueError("no page content stream found")
    return best


def media_box(data: bytes) -> tuple[float, float, float, float] | None:
    m = re.search(rb"/MediaBox\s*\[([^\]]+)\]", data)
    if not m:
        return None
    v = [float(x) for x in m.group(1).split()]
    return (v[0], v[1], v[2], v[3]) if len(v) == 4 else None


# --------------------------------------------------------------------------
# Content stream interpretation
# --------------------------------------------------------------------------

TOKEN_RE = re.compile(rb"/[^\s/\[\]<>()]+|<<|>>|\[|\]|[^\s/\[\]<>()]+")

PAINT_OPS = {"f", "F", "f*", "B", "B*", "b", "b*", "S", "s", "n"}


def mat_mul(m1, m2):
    """m1 then m2 (PDF order: new CTM = m1 x CTM)."""
    a1, b1, c1, d1, e1, f1 = m1
    a2, b2, c2, d2, e2, f2 = m2
    return (
        a1 * a2 + b1 * c2,
        a1 * b2 + b1 * d2,
        c1 * a2 + d1 * c2,
        c1 * b2 + d1 * d2,
        e1 * a2 + f1 * c2 + e2,
        e1 * b2 + f1 * d2 + f2,
    )


def apply(m, x, y):
    a, b, c, d, e, f = m
    return (a * x + c * y + e, b * x + d * y + f)


class PathBuilder:
    """Accumulates transformed subpaths, grouped by the fill colour used.

    Icons drawn with light "knockout" shapes on top of dark ones need those
    groups kept apart, otherwise the highlights get flooded with the icon
    colour and the artwork reads as a solid blob.
    """

    def __init__(self) -> None:
        self.groups: list[dict] = []      # [{'fill': (r,g,b)|None, 'segments': [...]}]
        self.evenodd = False

    @property
    def segments(self) -> list[tuple]:
        return [s for g in self.groups for s in g["segments"]]

    @property
    def fills(self) -> set:
        return {g["fill"] for g in self.groups if g["fill"] is not None}

    def paint(self, segments, fill):
        if segments:
            self.groups.append({"fill": fill, "segments": list(segments)})


def parse_content(content: bytes) -> PathBuilder:
    tokens = [t.decode("latin-1") for t in TOKEN_RE.findall(content)]
    out = PathBuilder()

    ctm = (1.0, 0.0, 0.0, 1.0, 0.0, 0.0)
    stack: list[tuple] = []
    operands: list = []
    cur = None          # current point in user space (untransformed)
    start = None
    cur_fill = None
    pending: list[tuple] = []

    def num(i):
        return float(operands[i])

    for tok in tokens:
        # operand?
        try:
            float(tok)
            operands.append(tok)
            continue
        except ValueError:
            pass
        if tok.startswith("/") or tok in ("[", "]", "<<", ">>"):
            operands.append(tok)
            continue

        op = tok
        try:
            if op == "q":
                stack.append(ctm)
            elif op == "Q":
                if stack:
                    ctm = stack.pop()
            elif op == "cm" and len(operands) >= 6:
                ctm = mat_mul(tuple(float(x) for x in operands[-6:]), ctm)
            elif op == "m" and len(operands) >= 2:
                cur = (num(-2), num(-1))
                start = cur
                pending.append(("M", apply(ctm, *cur)))
            elif op == "l" and len(operands) >= 2:
                cur = (num(-2), num(-1))
                pending.append(("L", apply(ctm, *cur)))
            elif op == "c" and len(operands) >= 6:
                p1 = (num(-6), num(-5))
                p2 = (num(-4), num(-3))
                p3 = (num(-2), num(-1))
                pending.append(("C", apply(ctm, *p1), apply(ctm, *p2), apply(ctm, *p3)))
                cur = p3
            elif op == "v" and len(operands) >= 4 and cur:
                p2 = (num(-4), num(-3))
                p3 = (num(-2), num(-1))
                pending.append(("C", apply(ctm, *cur), apply(ctm, *p2), apply(ctm, *p3)))
                cur = p3
            elif op == "y" and len(operands) >= 4:
                p1 = (num(-4), num(-3))
                p3 = (num(-2), num(-1))
                pending.append(("C", apply(ctm, *p1), apply(ctm, *p3), apply(ctm, *p3)))
                cur = p3
            elif op == "h":
                if pending and pending[-1][0] != "Z":
                    pending.append(("Z",))
                    if start:
                        cur = start
            elif op == "re" and len(operands) >= 4:
                x, y, w, h = (num(-4), num(-3), num(-2), num(-1))
                pending.append(("M", apply(ctm, x, y)))
                pending.append(("L", apply(ctm, x + w, y)))
                pending.append(("L", apply(ctm, x + w, y + h)))
                pending.append(("L", apply(ctm, x, y + h)))
                pending.append(("Z",))
                cur = (x, y)
                start = cur
            elif op in ("scn", "sc", "rg", "g", "k"):
                vals = []
                for o in operands:
                    try:
                        vals.append(float(o))
                    except ValueError:
                        pass
                if vals:
                    fill = tuple(round(v, 4) for v in vals)
                    if len(fill) == 1:              # DeviceGray
                        fill = fill * 3
                    if len(fill) == 4:              # CMYK -> rough RGB
                        c, m_, y_, k_ = fill
                        fill = (round((1 - c) * (1 - k_), 4),
                                round((1 - m_) * (1 - k_), 4),
                                round((1 - y_) * (1 - k_), 4))
                    cur_fill = fill
            elif op in PAINT_OPS:
                if op in ("f*", "B*", "b*"):
                    out.evenodd = True
                if op != "n":                       # 'n' = no paint (clip only)
                    out.paint(pending, cur_fill)
                pending = []
                cur = start = None
        finally:
            if op not in ("[", "]"):
                operands = []

    # Some producers omit the final paint operator.
    out.paint(pending, cur_fill)
    return out


# --------------------------------------------------------------------------
# Normalisation + emission
# --------------------------------------------------------------------------

def bezier_points(p0, p1, p2, p3, steps=16):
    for i in range(steps + 1):
        t = i / steps
        u = 1 - t
        yield (
            u * u * u * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t * t * t * p3[0],
            u * u * u * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t * t * t * p3[1],
        )


def bbox(segments):
    xs, ys, cur = [], [], (0.0, 0.0)
    for seg in segments:
        kind = seg[0]
        if kind in ("M", "L"):
            cur = seg[1]
            xs.append(cur[0]); ys.append(cur[1])
        elif kind == "C":
            for pt in bezier_points(cur, seg[1], seg[2], seg[3]):
                xs.append(pt[0]); ys.append(pt[1])
            cur = seg[3]
    if not xs:
        raise ValueError("empty path")
    return min(xs), min(ys), max(xs), max(ys)


def luminance(fill):
    if not fill:
        return 0.0
    r, g, b = fill[:3]
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def merge_adjacent(groups):
    """Collapse consecutive groups painted in the same colour."""
    out = []
    for g in groups:
        if out and out[-1]["fill"] == g["fill"]:
            out[-1]["segments"].extend(g["segments"])
        else:
            out.append({"fill": g["fill"], "segments": list(g["segments"])})
    return out


def emit_groups(groups, size=24.0, pad=1.5, precision=3):
    """Normalise every group through one shared transform into a square box."""
    all_segments = [s for g in groups for s in g["segments"]]
    x0, y0, x1, y1 = bbox(all_segments)
    w, h = max(x1 - x0, 1e-9), max(y1 - y0, 1e-9)
    inner = size - 2 * pad
    s = inner / max(w, h)
    ox = pad + (inner - w * s) / 2
    oy = pad + (inner - h * s) / 2

    def tp(p):
        # PDF's y-axis points up, SVG's points down.
        return ((p[0] - x0) * s + ox, (y1 - p[1]) * s + oy)

    def fmt(v):
        t = f"{v:.{precision}f}".rstrip("0").rstrip(".")
        return t if t not in ("-0", "") else "0"

    def build(segments):
        parts, last = [], None
        for seg in segments:
            kind = seg[0]
            if kind == "Z":
                if parts and not parts[-1].endswith("Z"):
                    parts.append("Z")
                continue
            pts = [tp(p) for p in seg[1:]]
            if kind == "M":
                parts.append(f"M{fmt(pts[0][0])} {fmt(pts[0][1])}")
            elif kind == "L":
                if last and abs(pts[0][1] - last[1]) < 10 ** -precision:
                    parts.append(f"H{fmt(pts[0][0])}")
                elif last and abs(pts[0][0] - last[0]) < 10 ** -precision:
                    parts.append(f"V{fmt(pts[0][1])}")
                else:
                    parts.append(f"L{fmt(pts[0][0])} {fmt(pts[0][1])}")
            elif kind == "C":
                parts.append("C" + " ".join(f"{fmt(p[0])} {fmt(p[1])}" for p in pts))
            last = pts[-1]
        return "".join(parts)

    emitted = []
    for g in groups:
        d = build(g["segments"])
        if d:
            emitted.append({
                "fill": g["fill"],
                "luminance": round(luminance(g["fill"]), 3),
                "d": d,
            })
    return emitted, (x0, y0, x1, y1)


def convert(path: Path, size: float, pad: float):
    data = path.read_bytes()
    built = parse_content(content_stream(data))
    if not built.segments:
        raise ValueError("no drawable paths")
    groups = merge_adjacent(built.groups)
    paths, box = emit_groups(groups, size=size, pad=pad)

    dark = [p for p in paths if p["luminance"] < 0.5]
    light = [p for p in paths if p["luminance"] >= 0.5]
    return {
        "name": path.stem,
        "paths": paths,
        "combined_d": "".join(p["d"] for p in paths),
        "dark_d": "".join(p["d"] for p in dark),
        "light_d": "".join(p["d"] for p in light),
        "fill_rule": "evenodd" if built.evenodd else None,
        "source_bbox": [round(v, 3) for v in box],
        "media_box": media_box(data),
        "source_fills": sorted(built.fills),
        "segments": len(built.segments),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pdfs", nargs="+", type=Path)
    ap.add_argument("--size", type=float, default=24.0, help="target viewBox (default 24)")
    ap.add_argument("--pad", type=float, default=1.5, help="padding inside the viewBox")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a report")
    args = ap.parse_args(argv)

    results = []
    for p in args.pdfs:
        try:
            results.append(convert(p, args.size, args.pad))
        except Exception as exc:                      # noqa: BLE001
            print(f"!! {p.name}: {exc}", file=sys.stderr)

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    for r in results:
        print(f"--- {r['name']} ---")
        print(f"  segments={r['segments']}  groups={len(r['paths'])}  "
              f"rule={r['fill_rule'] or 'nonzero'}  bbox={r['source_bbox']}")
        for p in r["paths"]:
            print(f"    fill={p['fill']} lum={p['luminance']} d={len(p['d'])}B")
        print(f"  combined d length={len(r['combined_d'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
