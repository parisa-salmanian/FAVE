#!/usr/bin/env python3
"""Bake an EXPANDED per-DESO SCB profile for FAVE's DR "All Data" feature.

Motivation
  The DR "All Data" UMAP feature (drFeatureMode.js FS_ALL_DATA) embeds the full
  socio-economic profile, not just access. Until now that profile was the ~9
  summary fields in demographics/deso.geojson. This bake unlocks the FULL rich
  SCB RegSO tables that were copied from EpiCity into cities/<key>/scb/ — income,
  labour market, transfers, education, students/NEET, SFI, origin composition —
  plus the whole age spectrum from <city>_age_gender.json.

Design (why standalone, not an edit to bake_demographics.py)
  bake_demographics.py depends on the EpiCity per-DESO source (epicity/deso.json,
  deso_income.json, deso_gender.json) which is NO LONGER on disk, so deso.geojson
  cannot be re-baked. This script therefore READS the already-baked
  demographics/deso.geojson (never overwrites it) for DESO polygons + the parent
  `regso` code, and JOINS the RegSO tables onto each DESO by that code. The rich
  fields are RegSO-resolution (91 regions for Malmo) — coarser than DESO (209) —
  but real. Output is a NEW file: demographics/deso_full.json.

Slices (honouring the "all vars + origin split + both age representations" choice)
  Every content measure is emitted at up to 5 slices where the SCB table carries
  them: total (both sexes, all origins), male, female, Sweden-born (SE),
  foreign-born (F). Age is emitted as BOTH the 17 five-year bands AND 6 grouped
  life-stages, plus the male share. All values are RAW; standardisation happens at
  runtime over whatever units are embedded.

Output: cities/<key>/demographics/deso_full.json
  { meta, fields: [{key,label,group,slice,table,code}], byDeso: {deso: {key: val}} }

Usage: python tools/bake_scb_full.py [--cities malmo ...]
"""
from __future__ import annotations
import argparse, json
from pathlib import Path

CITIES = ["vaxjo", "malmo", "goteborg", "stockholm", "kalmar", "norrkoping", "uppsala"]
DATA_ROOT = Path(__file__).resolve().parents[1] / "frontend" / "assets" / "data"
CITIES_ROOT = DATA_ROOT / "cities"

# --- SCB RegSO table extraction spec ------------------------------------------
# Per table: which background column, and the (Kon, Bakgrund) codes for each slice
# we want. Slices whose codes aren't present (or carry no data) simply yield None.
# `measures` is auto-enumerated from the table's content columns (their SCB text
# becomes the field label), so "add a measure" = nothing (all are taken).
SLICES_KON = [  # standard tables: both-sexes total, sex split, origin split
    ("tot", {"Kon": "1+2"}, "total"),
    ("m",   {"Kon": "1"},   "men"),
    ("f",   {"Kon": "2"},   "women"),
    ("se",  {"Kon": "1+2"}, "Sweden-born"),
    ("fb",  {"Kon": "1+2"}, "foreign-born"),
]
# origin (Bakgrund) code appended per slice, per table's own coding
def kon_bg_slices(bg_col, tot, se, fb):
    out = []
    for name, konf, lbl in SLICES_KON:
        f = dict(konf)
        if name in ("tot", "m", "f"):
            f[bg_col] = tot
        elif name == "se":
            if se is None:
                continue
            f[bg_col] = se
        elif name == "fb":
            if fb is None:
                continue
            f[bg_col] = fb
        out.append((name, f, lbl))
    return out

TABLES = [
    dict(id="IntGr5RegSOKon",  group="income",
         slices=kon_bg_slices("Bakgrund", "TOT", "SE", "F")),
    dict(id="IntGr1RegSOBASN", group="labour",
         slices=kon_bg_slices("BakgrVar", "TOT", "SE", "F")),
    dict(id="IntGr4RegSOKon",  group="transfers",
         slices=kon_bg_slices("Bakgrund", "TOT", "SE", "F")),
    dict(id="IntGr8RegSOKON1N", group="students_neet",
         slices=kon_bg_slices("Bakgrund", "TOT", "SE", "F")),
    # NB IntGr3RegSOKONS is intentionally excluded: its measures are an age×origin
    # cross-tab (each cell = "% of an origin group in an age band"), whose TOTp row
    # is a normalisation artifact (all 100.0). Origin is instead represented by the
    # SE/F (Sweden-born / foreign-born) slices of every measure below.
    dict(id="IntGr8RegSOKon2", group="education",   # no Kon; total code is 1+2
         slices=[("tot", {"Bakgrund": "1+2"}, "total")]),
    dict(id="IntGr8RegSOKON3", group="sfi",         # no Kon / no background dim
         slices=[("tot", {}, "total")]),
]

# Human-friendlier short labels for the (long, sometimes truncated) SCB texts.
LABEL_OVERRIDE = {
    "000004X3": "avg disposable income", "000004X4": "median disposable income",
    "000004X5": "income support %", "000004X6": "no income support %",
    "000004X7": "foothold allowance %", "000004X2": "persons %",
    "000008AW": "employed %", "000008B0": "self-employed %",
    "000008B1": "registered unemployed %", "000008B2": "openly unemployed %",
    "000008B3": "in labour programme %", "000008B4": "long-term unemployed %",
    "000008B5": "employed 1yr ago %", "000008B6": "still self-employed 1yr %",
    "000008AX": "managerial %", "000008AY": "post-sec skilled occ %",
    "000008AZ": "post-sec qual 3-4 %",
    "000004WR": "sick pay etc %inc", "000004WT": "sick pay %inc",
    "000004WX": "sickness/activity allowance %inc", "000004WY": "unemployment benefit %inc",
    "000004WZ": "income support %inc", "000004X0": "introductory allowance %inc",
    "000004X1": "foothold allowance %inc",
    "000004W7": "students %", "000004W8": "NEET 16-19 %", "000004W9": "NEET 20-25 %",
    "000004WA": "NEET 20-64 %",
    "000004UY": "Sweden-born %", "000004V3": "foreign background %", "000004UX": "foreign-born %",
    "000004V0": "born Nordics %", "000004V1": "born EU/EFTA %", "000004V2": "born rest-of-world %",
    "000004VC": "eligible upper-sec %", "000004VD": "eligible higher-ed %",
    "000004W5": "approved SFI %", "000004W6": "SFI residence yrs",
}
SLICE_SUFFIX = {"tot": "", "m": " (men)", "f": " (women)",
                "se": " (Sweden-born)", "fb": " (foreign-born)"}

# --- age spectrum spec --------------------------------------------------------
AGE_BANDS = [  # (scb code, field suffix)
    ("-4", "0_4"), ("5-9", "5_9"), ("10-14", "10_14"), ("15-19", "15_19"),
    ("20-24", "20_24"), ("25-29", "25_29"), ("30-34", "30_34"), ("35-39", "35_39"),
    ("40-44", "40_44"), ("45-49", "45_49"), ("50-54", "50_54"), ("55-59", "55_59"),
    ("60-64", "60_64"), ("65-69", "65_69"), ("70-74", "70_74"), ("75-79", "75_79"),
    ("80-", "80p"),
]
LIFE_STAGES = [  # (field suffix, [scb codes], label)
    ("child_0_14", ["-4", "5-9", "10-14"], "child 0-14 %"),
    ("youth_15_24", ["15-19", "20-24"], "youth 15-24 %"),
    ("young_adult_25_44", ["25-29", "30-34", "35-39", "40-44"], "young adult 25-44 %"),
    ("mid_45_64", ["45-49", "50-54", "55-59", "60-64"], "mid 45-64 %"),
    ("senior_65_79", ["65-69", "70-74", "75-79"], "senior 65-79 %"),
    ("old_80p", ["80-"], "80+ %"),
]


def _num(s):
    if s in (None, "", "..", ".", "...", ":", "-"):
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def load_table(city, tid):
    p = CITIES_ROOT / city / "scb" / f"{tid}.json"
    if not p.exists():
        return None
    tab = json.loads(p.read_text(encoding="utf-8"))
    cols = [c["code"] for c in tab.get("columns", [])]
    texts = {c["code"]: c.get("text", c["code"]) for c in tab.get("columns", [])}
    data = tab.get("data", [])
    if not data:
        return None
    nmeas = len(data[0].get("values", []))
    dim_codes = cols[:-nmeas]
    meas_codes = cols[-nmeas:]
    return dict(dim_codes=dim_codes, meas_codes=meas_codes, texts=texts, data=data)


def extract_table(city, spec, out_fields, out_by_regso):
    """Populate out_by_regso[regso][field_key]=value and append manifest rows."""
    tab = load_table(city, spec["id"])
    if not tab:
        return
    dim_codes, meas_codes, texts, data = (
        tab["dim_codes"], tab["meas_codes"], tab["texts"], tab["data"])
    # register manifest fields (once per measure×slice) — done lazily below
    seen = set()
    for row in data:
        dims = dict(zip(dim_codes, row.get("key", [])))
        region = (dims.get("Region") or "").split("_")[0]
        if not region:
            continue
        vals = row.get("values", [])
        for sl_name, filt, sl_lbl in spec["slices"]:
            if any(dims.get(k) != v for k, v in filt.items()):
                continue
            for mi, mcode in enumerate(meas_codes):
                base = LABEL_OVERRIDE.get(mcode) or texts.get(mcode, mcode)
                key = f"{spec['group']}_{mcode}_{sl_name}"
                if key not in seen:
                    seen.add(key)
                    label = base + SLICE_SUFFIX.get(sl_name, f" ({sl_lbl})")
                    out_fields.append(dict(key=key, label=label, group=spec["group"],
                                           slice=sl_name, table=spec["id"], code=mcode))
                v = _num(vals[mi]) if mi < len(vals) else None
                if v is not None:
                    out_by_regso.setdefault(region, {})[key] = v


def resolve_age_file(city):
    for cand in (f"{city}_age_gender.json", f"{city}_age_gender_population.json",
                 f"{city.capitalize()}_age_gender.json"):
        p = DATA_ROOT / cand
        if p.exists():
            return p
    return None


# Cities whose age spectrum lives in a nested-dict file (regso -> age -> year ->
# {male,female,total}) instead of the PxWeb columns/data layout. Vaxjo has no
# <key>_age_gender.json; gender-age-population.json holds its RegSO age pyramid.
NESTED_AGE_FILE = {"vaxjo": "gender-age-population.json"}


def _age_reg_pxweb(p):
    """region -> {(age, kon): pop} from a PxWeb columns/data age table."""
    tab = json.loads(p.read_text(encoding="utf-8"))
    cols = [c["code"] for c in tab.get("columns", [])]
    data = tab.get("data", [])
    if not data:
        return None
    dim_codes = cols[:-1]  # dims = Region, Alder, Kon, Tid ; 1 measure = pop count
    reg = {}
    for row in data:
        dims = dict(zip(dim_codes, row.get("key", [])))
        region = (dims.get("Region") or "").split("_")[0]
        age = dims.get("Alder"); kon = dims.get("Kon")
        v = _num((row.get("values") or [None])[0])
        if not region or age is None or v is None:
            continue
        reg.setdefault(region, {})[(age, kon)] = v
    return reg


def _age_reg_nested(p):
    """region -> {(age, kon): pop} from a nested regso->age->year->{male,female,
    total} dict, taking the most recent year per band."""
    d = json.loads(p.read_text(encoding="utf-8"))
    reg = {}
    for region, bands in d.items():
        if not isinstance(bands, dict):
            continue
        rr = reg.setdefault(region.split("_")[0], {})
        for age, years in bands.items():
            if not isinstance(years, dict) or not years:
                continue
            yr = max(years, key=lambda y: int(y) if str(y).isdigit() else -1)
            cell = years[yr]
            if not isinstance(cell, dict):
                continue
            if cell.get("total") is not None:
                rr[(age, "1+2")] = float(cell["total"])
            if cell.get("male") is not None:
                rr[(age, "1")] = float(cell["male"])
    return reg


def extract_age(city, out_fields, out_by_regso):
    p = resolve_age_file(city)
    reg = _age_reg_pxweb(p) if p else None
    if not reg and city in NESTED_AGE_FILE:      # Vaxjo-style fallback
        np = DATA_ROOT / NESTED_AGE_FILE[city]
        if np.exists():
            reg = _age_reg_nested(np)
    if not reg:
        return False

    # register manifest fields once
    for _, suf in AGE_BANDS:
        out_fields.append(dict(key=f"age_{suf}", label=f"age {suf.replace('_','-')} %",
                               group="age", slice="tot", table="age_gender", code=suf))
    for suf, _codes, lbl in LIFE_STAGES:
        out_fields.append(dict(key=f"age_{suf}", label=lbl, group="age_stage",
                               slice="tot", table="age_gender", code=suf))
    out_fields.append(dict(key="male_share", label="male share %", group="sex",
                           slice="tot", table="age_gender", code="male"))

    for region, cells in reg.items():
        tot = cells.get(("totalt", "1+2"))
        if not tot:  # fall back to summing bands
            tot = sum(cells.get((a, "1+2"), 0) for a, _ in AGE_BANDS) or None
        if not tot:
            continue
        d = out_by_regso.setdefault(region, {})
        for acode, suf in AGE_BANDS:
            band = cells.get((acode, "1+2"))
            if band is not None:
                d[f"age_{suf}"] = round(100.0 * band / tot, 3)
        for suf, codes, _lbl in LIFE_STAGES:
            s = sum(cells.get((a, "1+2"), 0) for a in codes)
            d[f"age_{suf}"] = round(100.0 * s / tot, 3)
        male = cells.get(("totalt", "1"))
        if male is not None:
            d["male_share"] = round(100.0 * male / tot, 3)
    return True


def build_city(key):
    deso_p = CITIES_ROOT / key / "demographics" / "deso.geojson"
    if not deso_p.exists():
        print(f"  [{key}] SKIP — no demographics/deso.geojson"); return None
    fc = json.loads(deso_p.read_text(encoding="utf-8"))
    feats = fc.get("features", [])

    fields = []
    by_regso = {}
    for spec in TABLES:
        extract_table(key, spec, fields, by_regso)
    age_ok = extract_age(key, fields, by_regso)

    # join RegSO values onto each DESO
    by_deso = {}
    n_join = 0
    for f in feats:
        pr = f.get("properties", {})
        deso = pr.get("deso")
        regso = pr.get("regso")
        if not deso:
            continue
        vals = dict(by_regso.get(regso, {})) if regso else {}
        if vals:
            n_join += 1
        by_deso[deso] = vals

    # Prune fields that carry no signal for THIS city: zero coverage (never
    # populated) or zero variance (identical across every DESO). Both are dead
    # weight in a UMAP embedding / EBM, so the per-city manifest only lists live
    # fields. The runtime reads this manifest, so field sets may differ per city.
    kept = []
    dropped = 0
    for fld in fields:
        vs = [by_deso[dz][fld["key"]] for dz in by_deso if fld["key"] in by_deso[dz]]
        if len(vs) == 0 or (max(vs) - min(vs)) == 0.0:
            dropped += 1
            for dz in by_deso:
                by_deso[dz].pop(fld["key"], None)
            continue
        kept.append(fld)
    fields = kept

    return {
        "meta": {
            "city": key, "n_deso": len(by_deso), "n_regso": len(by_regso),
            "n_fields": len(fields), "fields_dropped": dropped, "deso_with_data": n_join,
            "age_source": bool(age_ok),
            "source": "SCB RegSO tables (AA/AA0003) + age_gender, joined to DESO by regso",
            "note": "RegSO-resolution fields attached to nested DESO; RAW values, "
                    "standardise at runtime. Never overwrites deso.geojson.",
        },
        "fields": fields,
        "byDeso": by_deso,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cities", nargs="*", default=CITIES)
    args = ap.parse_args()
    for key in args.cities:
        print(f"[{key}] baking expanded SCB profile...")
        out = build_city(key)
        if not out:
            continue
        out_p = CITIES_ROOT / key / "demographics" / "deso_full.json"
        out_p.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        m = out["meta"]
        print(f"  [{key}] -> deso_full.json: {m['n_fields']} fields, "
              f"{m['n_deso']} DESO ({m['deso_with_data']} w/ data), "
              f"{m['n_regso']} RegSO, age={m['age_source']}")


if __name__ == "__main__":
    main()
