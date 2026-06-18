#!/usr/bin/env python3
"""Bake a per-DESO socioeconomic layer for FAVE's demographic fairness weighting.

Source (EpiCity data now under frontend/assets/data/cities/<key>/epicity/):
  - deso.json        (areas: deso code, polygon [lon,lat], total pop, age {child,adult,elder})
  - deso_income.json ({deso: mean disposable income, kSEK})
  - deso_gender.json ({deso: {male_frac, female_frac}})

Output:
  frontend/assets/data/cities/<key>/demographics/deso.geojson
    FeatureCollection of DESO polygons with properties:
      deso, pop, income, male_frac, child_frac, elder_frac, dependency, needZ
    needZ = composite deprivation z-score (higher = more need / more vulnerable),
            combining low income (economy) and high age-dependency. Used at runtime
            to up-weight under-served high-need areas in the fairness/Gini model.

Usage: python tools/bake_demographics.py [--cities vaxjo ...]
"""
from __future__ import annotations
import argparse, json, math, statistics
from pathlib import Path

CITIES = ["vaxjo", "malmo", "goteborg", "stockholm", "kalmar", "norrkoping", "uppsala"]
DATA_ROOT = Path(__file__).resolve().parents[1] / "frontend" / "assets" / "data" / "cities"

# Composite need weights (sum = 1). Income is the dominant, well-established
# deprivation proxy; age-dependency captures care-demand vulnerability.
W_INCOME = 0.65
W_DEPENDENCY = 0.35


def zscores(values):
    vals = [v for v in values if v is not None and math.isfinite(v)]
    if len(vals) < 2:
        return {i: 0.0 for i in range(len(values))}
    mean = statistics.fmean(vals)
    sd = statistics.pstdev(vals)
    if sd == 0:
        return {i: 0.0 for i in range(len(values))}
    return {i: ((v - mean) / sd if (v is not None and math.isfinite(v)) else 0.0)
            for i, v in enumerate(values)}


def build_city(key: str):
    cdir = DATA_ROOT / key / "epicity"
    deso_p = cdir / "deso.json"
    inc_p = cdir / "deso_income.json"
    gen_p = cdir / "deso_gender.json"
    if not deso_p.exists():
        print(f"  [{key}] SKIP — no deso.json"); return None
    deso = json.loads(deso_p.read_text(encoding="utf-8"))
    income = json.loads(inc_p.read_text(encoding="utf-8")) if inc_p.exists() else {}
    gender = json.loads(gen_p.read_text(encoding="utf-8")) if gen_p.exists() else {}
    areas = deso.get("areas", [])
    if not areas:
        print(f"  [{key}] SKIP — no areas"); return None

    rows = []
    for a in areas:
        code = a.get("deso")
        pop = float(a.get("total") or 0)
        age = a.get("age") or {}
        child = float(age.get("child") or 0)
        elder = float(age.get("elder") or 0)
        adult = float(age.get("adult") or 0)
        denom = child + adult + elder
        child_frac = child / denom if denom else 0.0
        elder_frac = elder / denom if denom else 0.0
        # dependency = dependents (child+elder) per working-age adult; classic ratio
        dependency = (child + elder) / adult if adult > 0 else (1.0 if denom else 0.0)
        inc = income.get(code)
        rows.append({
            "code": code, "pop": pop, "income": (float(inc) if inc is not None else None),
            "child_frac": child_frac, "elder_frac": elder_frac, "dependency": dependency,
            "male_frac": (gender.get(code) or {}).get("male_frac"),
            "gtype": a.get("gtype", "Polygon"), "polygon": a.get("polygon"),
        })

    # Composite need z-score: low income -> high need (negate), high dependency -> high need.
    z_neg_income = zscores([(-r["income"] if r["income"] is not None else None) for r in rows])
    z_dependency = zscores([r["dependency"] for r in rows])
    for i, r in enumerate(rows):
        r["needZ"] = round(W_INCOME * z_neg_income[i] + W_DEPENDENCY * z_dependency[i], 4)

    feats = []
    for r in rows:
        if not r["polygon"]:
            continue
        feats.append({
            "type": "Feature",
            "geometry": {"type": r["gtype"], "coordinates": r["polygon"]},
            "properties": {
                "deso": r["code"], "pop": r["pop"],
                "income": r["income"], "male_frac": r["male_frac"],
                "child_frac": round(r["child_frac"], 4), "elder_frac": round(r["elder_frac"], 4),
                "dependency": round(r["dependency"], 4), "needZ": r["needZ"],
            },
        })
    return {
        "type": "FeatureCollection",
        "meta": {
            "city": key, "deso_count": len(feats),
            "need_weights": {"income": W_INCOME, "dependency": W_DEPENDENCY},
            "income_units": "kSEK mean disposable", "source": "EpiCity SCB DeSO",
        },
        "features": feats,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cities", nargs="*", default=CITIES)
    args = ap.parse_args()
    for key in args.cities:
        print(f"[{key}] baking demographics...")
        fc = build_city(key)
        if not fc:
            continue
        out_dir = DATA_ROOT / key / "demographics"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_p = out_dir / "deso.geojson"
        out_p.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
        nz = [f["properties"]["needZ"] for f in fc["features"]]
        inc = [f["properties"]["income"] for f in fc["features"] if f["properties"]["income"] is not None]
        print(f"  [{key}] -> {out_p.name}: {len(fc['features'])} DESOs, "
              f"needZ [{min(nz):.2f},{max(nz):.2f}], "
              f"income [{min(inc):.0f},{max(inc):.0f}] kSEK" if inc else
              f"  [{key}] -> {len(fc['features'])} DESOs (no income)")


if __name__ == "__main__":
    main()
