"""
Parse the baked SCB RegSO tables (frontend/assets/data/cities/<city>/scb/*.json)
into ONE clean per-district demographics file:
    frontend/assets/data/demographics_<city>.json   (keyed by regsokod)

100% OFFLINE. Reads only the already-baked SCB JSON + the regso geojson.
No network. (SCB API lives only in tools/bake_scb_demographics.py.)

For every variable we take the *total* slice:
  Kon = '1+2' (both sexes), and the table's all-persons background code.
Missing SCB values ('..') become null. We keep the raw % / index values as SCB
publishes them (income is in "number of price base amounts", PBA).

Run:
    .venv/bin/python tools/analysis/scb_parse.py --city vaxjo
"""
import os, json, argparse

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")

# regsokod suffix differs per table ('0780R001' vs '0780R001_RegSO2025') -> first 8 chars
def reg8(code): return code[:8]

# (table, sex_code, background_code, value_column_code, output_field)
# background_code = the "all persons" slice for that table (verified by spot-check).
SPEC = [
    # --- Labour market (IntGr1, BakgrVar total = 'TOT') ---
    ("IntGr1RegSOBASN", "1+2", "TOT", "000008AW", "employed_pct"),
    ("IntGr1RegSOBASN", "1+2", "TOT", "000008B1", "unemployed_pct"),
    ("IntGr1RegSOBASN", "1+2", "TOT", "000008B4", "longterm_unemployed_pct"),
    ("IntGr1RegSOBASN", "1+2", "TOT", "000008AX", "managerial_pct"),
    # --- Origin / background ---
    # [NEEDS DATA] IntGr3RegSOKONS is a WITHIN-GROUP cross-tab: columns are origin
    # groups (Born-in-Sweden / Foreign-background / Foreign-born) and each cell is the
    # age/education breakdown *inside* that group (the 'TOTp' row is 100 by definition).
    # It does NOT give the district's share of foreign-background population. That
    # variable (the main Swedish segregation marker) is therefore NOT extractable from
    # the baked tables and is intentionally omitted. To add it, bake a counts table
    # (population by RegSO x background) in tools/bake_scb_demographics.py.
    # --- Income (IntGr5, Bakgrund total = 'tot20-64') ---
    ("IntGr5RegSOKon", "1+2", "tot20-64", "000004X3", "avg_disp_income_pba"),
    ("IntGr5RegSOKon", "1+2", "tot20-64", "000004X4", "median_disp_income_pba"),
    ("IntGr5RegSOKon", "1+2", "tot20-64", "000004X5", "income_support_only_pct"),
    # --- Transfers / deprivation (IntGr4, Bakgrund total = 'tot20-64') ---
    ("IntGr4RegSOKon", "1+2", "tot20-64", "000004WZ", "income_support_share_pct"),
    ("IntGr4RegSOKon", "1+2", "tot20-64", "000004WY", "unemployment_benefit_share_pct"),
    # --- Education (IntGr8Kon2; dim labelled 'Bakgrund' is actually sex -> '1+2') ---
    ("IntGr8RegSOKon2", None, "1+2", "000004VD", "eligible_higher_edu_pct"),
    ("IntGr8RegSOKon2", None, "1+2", "000004VC", "eligible_upper_secondary_pct"),
    # --- Students (IntGr8KON1N, Bakgrund total = 'tot20-64') ---
    ("IntGr8RegSOKON1N", "1+2", "tot20-64", "000004W7", "students_pct"),
]


def load_table(scbdir, tid):
    j = json.load(open(os.path.join(scbdir, tid + ".json")))
    cols = j["columns"]
    code_idx = {c["code"]: i for i, c in enumerate(cols)}
    dim_pos = {c["code"]: i for i, c in enumerate(cols) if c["type"] == "d"}
    # value columns map: code -> position in row["values"]
    valcols = [c["code"] for c in cols if c["type"] == "c"]
    valpos = {code: i for i, code in enumerate(valcols)}
    return j["data"], dim_pos, valpos


# Rural RegSO the app drops from the map (frontend districts.js EXCLUDED_DISTRICTS).
# We keep them in the JSON but flag them so the clustering uses the same 16 the user sees.
EXCLUDED_NAMES = {
    "stadsnära landsbygd", "växjö landsbygd", "gemla", "ingelstad",
    "braås", "rottne", "lammhult",
}


def num(s):
    if s in ("..", ".", "", None):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="vaxjo")
    args = ap.parse_args()
    city = args.city

    scbdir = os.path.join(ROOT, "frontend/assets/data/cities", city, "scb")
    geo = json.load(open(os.path.join(ROOT, f"frontend/assets/data/{city.capitalize()}_regso.geojson")))
    names = {f["properties"]["regsokod"]: f["properties"]["regsonamn"] for f in geo["features"]}
    districts = sorted(names)

    # cache loaded tables
    cache = {}
    out = {code: {"regsokod": code, "regsonamn": names[code],
                  "shown_on_map": names[code].strip().lower() not in EXCLUDED_NAMES}
           for code in districts}

    for tid, kon, bg, vcode, field in SPEC:
        if tid not in cache:
            cache[tid] = load_table(scbdir, tid)
        data, dim_pos, valpos = cache[tid]
        # which key positions hold Region, Kon, background
        rp = dim_pos["Region"]
        kp = dim_pos.get("Kon")
        # background dim is whichever non-Region, non-Kon 'd' column exists
        bgp = None
        for code, pos in dim_pos.items():
            if code not in ("Region", "Kon"):
                bgp = pos
        vp = valpos[vcode]
        for row in data:
            key = row["key"]
            rc = reg8(key[rp])
            if rc not in out:
                continue
            if kon is not None and kp is not None and key[kp] != kon:
                continue
            if bg is not None and bgp is not None and key[bgp] != bg:
                continue
            out[rc][field] = num(row["values"][vp])

    dest = os.path.join(ROOT, f"frontend/assets/data/demographics_{city}.json")
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # ---- verification dump ----
    fields = [s[4] for s in SPEC]
    shown = [c for c in districts if out[c]["shown_on_map"]]
    print(f"[scb-parse] {city}: {len(districts)} districts ({len(shown)} shown on map) -> {dest}\n")
    print("missing per field:")
    for fld in fields:
        miss = sum(1 for c in districts if out[c].get(fld) is None)
        print(f"  {fld:32s} {miss:2d}/{len(districts)} missing")
    short = {"employed_pct": "emp%", "unemployed_pct": "unemp%",
             "longterm_unemployed_pct": "ltunemp", "managerial_pct": "mgr%",
             "avg_disp_income_pba": "avginc", "median_disp_income_pba": "medinc",
             "income_support_only_pct": "incOnly", "income_support_share_pct": "incShr",
             "unemployment_benefit_share_pct": "unempBen",
             "eligible_higher_edu_pct": "hiEdu%", "eligible_upper_secondary_pct": "upsec%",
             "students_pct": "stud%"}
    print("\nper-district values  ([x] = excluded from map):")
    print("      " + f"{'name':20s} " + " ".join(f"{short[f]:>5s}" for f in fields))
    for c in districts:
        r = out[c]
        vals = []
        for fld in fields:
            v = r.get(fld)
            vals.append("  --" if v is None else f"{v:5.1f}")
        tag = "   " if r["shown_on_map"] else "[x]"
        print(f"  {tag} {r['regsonamn'][:20]:20s} " + " ".join(vals))


if __name__ == "__main__":
    main()
