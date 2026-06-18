#!/usr/bin/env python3
"""Bake a per-DESO socioeconomic layer for FAVE's demographic fairness weighting.

Phase 3 (2026-06-18): the need index is now a MULTI-DOMAIN deprivation score
(IMD-style) instead of just income + age-dependency. It folds in three SCB
RegSO-level signals (already baked by tools/bake_scb_demographics.py into
cities/<key>/scb/) on top of the EpiCity per-DESO economy/demography signals.

Sources
  EpiCity (cities/<key>/epicity/, per DESO — finest grain):
    - deso.json        (deso code, polygon [lon,lat], total pop, age{child,adult,elder})
    - deso_income.json ({deso: mean disposable income, kSEK})
    - deso_gender.json ({deso: {male_frac, female_frac}})
  SCB RegSO tables (cities/<key>/scb/, attached to each DESO by spatial join,
  since DESO nests inside RegSO):
    - IntGr8RegSOKon2   higher-education eligibility %  (low  -> more need)
    - IntGr8RegSOKON1N  NEET: % 20-64 neither employed nor studying (high -> need)
    - IntGr4RegSOKon    income-support share of net income %  (high -> need)

needZ = composite deprivation z-score (higher = more need / more vulnerable):
    0.30*z(-income) + 0.15*z(dependency)
  + 0.20*z(-higherEd) + 0.20*z(NEET) + 0.15*z(incomeSupport)
The runtime (models/epicityDemographics.js) reads `needZ` unchanged, so this
enrichment requires no frontend change. `needZ_v1` (the old income+dependency
score) is kept alongside for transparency/calibration.

Usage: python tools/bake_demographics.py [--cities vaxjo ...]
"""
from __future__ import annotations
import argparse, json, math, statistics
from pathlib import Path

CITIES = ["vaxjo", "malmo", "goteborg", "stockholm", "kalmar", "norrkoping", "uppsala"]
DATA_ROOT = Path(__file__).resolve().parents[1] / "frontend" / "assets" / "data"
CITIES_ROOT = DATA_ROOT / "cities"

# city key -> SCB municipality (kommun) code (for filtering all-Sweden RegSO geom).
KOMMUN = {"vaxjo": "0780", "malmo": "1280", "goteborg": "1480", "stockholm": "0180",
          "kalmar": "0880", "norrkoping": "0581", "uppsala": "0380"}

# Composite need weights (sum = 1). Income is the dominant, well-established
# deprivation proxy; education / employment-exclusion / welfare-reliance add the
# socioeconomic domains; age-dependency captures care-demand vulnerability.
WEIGHTS = {"income": 0.30, "dependency": 0.15, "education": 0.20,
           "neet": 0.20, "income_support": 0.15}

# SCB signal extraction specs: (table id, measure content-code, dimension filter,
# invert?). `invert` means "higher raw value = LESS need" (so we negate before z).
SCB_SIGNALS = {
    "education":      ("IntGr8RegSOKon2",  "000004VD", {"Bakgrund": "1+2"}, True),
    "neet":           ("IntGr8RegSOKON1N", "000004WA", {"Kon": "1+2", "Bakgrund": "tot20-64"}, False),
    "income_support": ("IntGr4RegSOKon",   "000004WZ", {"Kon": "1+2", "Bakgrund": "tot20-64"}, False),
}


def zscores(values):
    """z-score a list; None / non-finite -> 0.0 contribution."""
    vals = [v for v in values if v is not None and math.isfinite(v)]
    if len(vals) < 2:
        return [0.0] * len(values)
    mean = statistics.fmean(vals)
    sd = statistics.pstdev(vals)
    if sd == 0:
        return [0.0] * len(values)
    return [((v - mean) / sd if (v is not None and math.isfinite(v)) else 0.0) for v in values]


def _num(s):
    """Parse an SCB cell ('..' / '' -> None)."""
    if s in (None, "", "..", ".", "...", ":"):
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def load_scb_signal(city: str, table_id: str, measure: str, filt: dict, invert: bool):
    """Return {regso_code: value} for one SCB measure, filtered to the chosen
    sex/background/age dimension. Region codes are normalised (drop _RegSO2025)."""
    p = CITIES_ROOT / city / "scb" / f"{table_id}.json"
    if not p.exists():
        return None
    tab = json.loads(p.read_text(encoding="utf-8"))
    cols = tab.get("columns", [])
    data = tab.get("data", [])
    if not data:
        return None
    n_meas = len(data[0].get("values", []))
    dim_codes = [c["code"] for c in cols[:-n_meas]] if n_meas else [c["code"] for c in cols]
    meas_codes = [c["code"] for c in cols[-n_meas:]] if n_meas else []
    if measure not in meas_codes:
        return None
    mi = meas_codes.index(measure)
    out = {}
    for row in data:
        dims = dict(zip(dim_codes, row.get("key", [])))
        if any(dims.get(k) != v for k, v in filt.items()):
            continue
        region = (dims.get("Region") or "").split("_")[0]
        if not region:
            continue
        val = _num(row.get("values", [None] * (mi + 1))[mi])
        if val is None:
            continue
        out[region] = (-val if invert else val)
    return out


def deso_to_regso(city: str, deso_rows):
    """Map each DESO to its containing RegSO (centroid-in-polygon spatial join).
    Returns {deso_code: regso_code}. Needs RegSO geometry: a per-city
    <City>_regso.geojson, else all_sweden_regso.geojson filtered by kommun."""
    import geopandas as gpd
    from shapely.geometry import shape, Point

    # Resolve <city>_regso.geojson case-insensitively (Windows preserves whatever
    # case the file was first created with; other filesystems are case-sensitive).
    target = f"{city.lower()}_regso.geojson"
    per_city = next((p for p in DATA_ROOT.glob("*_regso.geojson")
                     if p.name.lower() == target), None)
    allswe = DATA_ROOT / "all_sweden_regso.geojson"
    if per_city is not None:
        regso = gpd.read_file(per_city)
    elif allswe.exists():
        regso = gpd.read_file(allswe)
        regso = regso[regso["kommunkod"].astype(str).str.zfill(4) == KOMMUN[city]]
    else:
        return None
    if regso.crs is None:
        regso.set_crs(4326, inplace=True)
    regso = regso.to_crs(4326)[["regsokod", "geometry"]]

    pts, codes = [], []
    for r in deso_rows:
        poly = r.get("polygon")
        if not poly:
            continue
        try:
            geom = shape({"type": r.get("gtype", "Polygon"), "coordinates": poly})
            pts.append(geom.representative_point())
            codes.append(r["code"])
        except Exception:
            continue
    if not pts:
        return None
    dpts = gpd.GeoDataFrame({"deso": codes}, geometry=pts, crs=4326)
    joined = gpd.sjoin(dpts, regso, predicate="within", how="left")
    joined = joined[~joined.index.duplicated(keep="first")]
    return {row["deso"]: row["regsokod"] for _, row in joined.iterrows()
            if isinstance(row.get("regsokod"), str)}


def build_city(key: str):
    cdir = CITIES_ROOT / key / "epicity"
    deso_p = cdir / "deso.json"
    if not deso_p.exists():
        print(f"  [{key}] SKIP — no deso.json"); return None
    deso = json.loads(deso_p.read_text(encoding="utf-8"))
    income = json.loads((cdir / "deso_income.json").read_text(encoding="utf-8")) if (cdir / "deso_income.json").exists() else {}
    gender = json.loads((cdir / "deso_gender.json").read_text(encoding="utf-8")) if (cdir / "deso_gender.json").exists() else {}
    areas = deso.get("areas", [])
    if not areas:
        print(f"  [{key}] SKIP — no areas"); return None

    rows = []
    for a in areas:
        code = a.get("deso")
        age = a.get("age") or {}
        child = float(age.get("child") or 0); elder = float(age.get("elder") or 0); adult = float(age.get("adult") or 0)
        denom = child + adult + elder
        inc = income.get(code)
        rows.append({
            "code": code, "pop": float(a.get("total") or 0),
            "income": (float(inc) if inc is not None else None),
            "child_frac": (child / denom if denom else 0.0),
            "elder_frac": (elder / denom if denom else 0.0),
            "dependency": ((child + elder) / adult if adult > 0 else (1.0 if denom else 0.0)),
            "male_frac": (gender.get(code) or {}).get("male_frac"),
            "gtype": a.get("gtype", "Polygon"), "polygon": a.get("polygon"),
        })

    # --- attach SCB RegSO signals via DESO->RegSO spatial join ---
    d2r = deso_to_regso(key, rows)
    signals = {}
    enriched_domains = []
    if d2r:
        for dom, (tid, meas, filt, inv) in SCB_SIGNALS.items():
            sig = load_scb_signal(key, tid, meas, filt, inv)
            if sig:
                signals[dom] = sig
                enriched_domains.append(dom)
        for r in rows:
            r["regso"] = d2r.get(r["code"])
            for dom in SCB_SIGNALS:
                raw = signals.get(dom, {}).get(r["regso"]) if r.get("regso") else None
                # store DISPLAY value (un-inverted) for overlays; z uses the stored (possibly negated) signal
                r[dom] = raw  # raw is already negated when invert=True
    else:
        print(f"  [{key}] note: no RegSO geometry — falling back to income+dependency only")
    n_join = sum(1 for r in rows if r.get("regso")) if d2r else 0

    # --- composite need z-score across available domains (missing -> 0 contribution) ---
    z = {
        "income": zscores([(-r["income"] if r["income"] is not None else None) for r in rows]),
        "dependency": zscores([r["dependency"] for r in rows]),
    }
    for dom in SCB_SIGNALS:
        z[dom] = zscores([r.get(dom) for r in rows]) if dom in signals else [0.0] * len(rows)

    for i, r in enumerate(rows):
        r["needZ_v1"] = round(0.65 * z["income"][i] + 0.35 * z["dependency"][i], 4)
        r["needZ"] = round(sum(WEIGHTS[d] * z[d][i] for d in WEIGHTS), 4)

    feats = []
    for r in rows:
        if not r["polygon"]:
            continue
        props = {
            "deso": r["code"], "pop": r["pop"], "income": r["income"], "male_frac": r["male_frac"],
            "child_frac": round(r["child_frac"], 4), "elder_frac": round(r["elder_frac"], 4),
            "dependency": round(r["dependency"], 4),
            "needZ": r["needZ"], "needZ_v1": r["needZ_v1"],
        }
        if d2r:
            props["regso"] = r.get("regso")
            # un-invert education for human-readable display (higher_ed% as published)
            props["higher_ed"] = (round(-r["education"], 1) if r.get("education") is not None else None)
            props["neet"] = (round(r["neet"], 1) if r.get("neet") is not None else None)
            props["income_support"] = (round(r["income_support"], 1) if r.get("income_support") is not None else None)
        feats.append({"type": "Feature",
                      "geometry": {"type": r["gtype"], "coordinates": r["polygon"]},
                      "properties": props})
    return {
        "type": "FeatureCollection",
        "meta": {
            "city": key, "deso_count": len(feats),
            "need_weights": WEIGHTS, "income_units": "kSEK mean disposable",
            "source": "EpiCity SCB DeSO + SCB RegSO (AA/AA0003)",
            "domains": ["income", "dependency"] + enriched_domains,
            "regso_join": f"{n_join}/{len(rows)} DESOs matched a RegSO" if d2r else "none",
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
        out_dir = CITIES_ROOT / key / "demographics"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_p = out_dir / "deso.geojson"
        out_p.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
        nz = [f["properties"]["needZ"] for f in fc["features"]]
        print(f"  [{key}] -> {out_p.name}: {len(fc['features'])} DESOs | "
              f"domains={fc['meta']['domains']} | join={fc['meta']['regso_join']} | "
              f"needZ [{min(nz):.2f},{max(nz):.2f}]")


if __name__ == "__main__":
    main()
