"""
Bake ALL RegSO-level SCB statistics for a city into the app data folder.

External API (SCB PxWeb v1) — runs at DEV/BAKE time only, never at runtime
(CLAUDE.md hard rule). Source: SCB's RegSO/DeSO statistics product AA/AA0003
(subfolders: B labour market, E demography, F incomes, G transfers, H education).
For each table whose id contains "RegSO", it downloads the latest year for the
city's RegSO areas, all measures/breakdowns.

Usage:
    .venv/bin/python tools/bake_scb_demographics.py --city vaxjo
    .venv/bin/python tools/bake_scb_demographics.py --city vaxjo --force

Output: frontend/assets/data/cities/<city>/scb/<tableId>.json   (+ index.json)
The JSON keeps SCB's native px format (columns/data) — same shape as the existing
gender-district-income-vaxjo.json, so downstream parsing is consistent.
"""
import os, sys, json, time, argparse, urllib.request, urllib.error

BASE = "https://api.scb.se/OV0104/v1/doris/en/ssd"
ROOT = os.path.join(os.path.dirname(__file__), "..")
START_PATH = "AA/AA0003"          # SCB RegSO/DeSO statistics product
TABLE_MATCH = "RegSO"             # only tables broken down by RegSO
SKIP_FOLDERS = {"AA0003X"}        # "Old tables, not updated"
SLEEP = 1.2                       # rate limit: PxWeb v1 ~10 calls / 10 s

# city key -> SCB municipality (kommun) code. RegSO codes look like <kommun>R###.
CITIES = {
    "vaxjo": "0780", "malmo": "1280", "goteborg": "1480", "stockholm": "0180",
    "kalmar": "0880", "norrkoping": "0581", "uppsala": "0380",
}

def _get(path):
    url = f"{BASE}/{path}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def _post(path, body):
    url = f"{BASE}/{path}"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8")

def crawl_tables(path):
    """Recurse the folder tree from `path`, return [(folder_path, table_id, title)]."""
    out = []
    for node in _get(path):
        nid = node["id"]
        if node["type"] == "l":           # sub-folder
            if nid in SKIP_FOLDERS: continue
            time.sleep(SLEEP)
            out += crawl_tables(f"{path}/{nid}")
        elif node["type"] == "t" and TABLE_MATCH in nid:
            out.append((path, nid, node.get("text", "")))
    return out

def build_query(meta, region_prefix):
    region_vals, region_code, year = [], None, None
    query = []
    for v in meta["variables"]:
        code = v["code"]
        if code == "Region":
            region_code = code
            region_vals = [x for x in v["values"] if x.startswith(region_prefix)]
            query.append({"code": code, "selection": {"filter": "item", "values": region_vals}})
        elif v.get("time"):
            year = v["values"][-1]         # latest year
            query.append({"code": code, "selection": {"filter": "item", "values": [year]}})
        else:                              # all measures / breakdowns
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
    return query, region_vals, year

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True, choices=sorted(CITIES))
    ap.add_argument("--force", action="store_true", help="re-download cached tables")
    args = ap.parse_args()

    kommun = CITIES[args.city]
    region_prefix = f"{kommun}R"
    outdir = os.path.join(ROOT, "frontend/assets/data/cities", args.city, "scb")
    os.makedirs(outdir, exist_ok=True)

    print(f"[scb] discovering RegSO tables under {START_PATH} ...")
    tables = crawl_tables(START_PATH)
    print(f"[scb] found {len(tables)} RegSO tables; downloading for {args.city} (kommun {kommun})")

    index = []
    for i, (folder, tid, title) in enumerate(tables, 1):
        dest = os.path.join(outdir, f"{tid}.json")
        if os.path.exists(dest) and not args.force:
            print(f"  ({i}/{len(tables)}) {tid}: cached, skip")
            continue
        try:
            time.sleep(SLEEP)
            meta = _get(f"{folder}/{tid}")
            query, region_vals, year = build_query(meta, region_prefix)
            if not region_vals:
                print(f"  ({i}/{len(tables)}) {tid}: no {region_prefix}* regions, skip")
                continue
            body = {"query": query, "response": {"format": "json"}}
            time.sleep(SLEEP)
            text = _post(f"{folder}/{tid}", body)
            with open(dest, "w", encoding="utf-8") as f:
                f.write(text)
            n_cells = len(json.loads(text).get("data", []))
            index.append({"id": tid, "title": title, "folder": folder,
                          "year": year, "regions": len(region_vals), "cells": n_cells})
            print(f"  ({i}/{len(tables)}) {tid}: {n_cells} cells, {len(region_vals)} regions, year {year}  ✓")
        except urllib.error.HTTPError as e:
            print(f"  ({i}/{len(tables)}) {tid}: HTTP {e.code} ({e.reason}) — skipped")
        except Exception as e:
            print(f"  ({i}/{len(tables)}) {tid}: {type(e).__name__}: {e} — skipped")

    with open(os.path.join(outdir, "index.json"), "w", encoding="utf-8") as f:
        json.dump({"city": args.city, "kommun": kommun, "source": f"SCB {START_PATH}",
                   "tables": index}, f, indent=2, ensure_ascii=False)
    print(f"[scb] done — {len(index)} tables written to {outdir}")

if __name__ == "__main__":
    main()
