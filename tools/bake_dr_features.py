"""
Bake per-building "rich" DR features for Vaxjo into the app data folder, so the
DR view's UMAP/PCA can use ~6 effective dimensions (not just accessibility).

OFFLINE. Reads tools/analysis/dimensionality_cache.npz (built by
tools/analysis/dimensionality.py from the baked routing matrices + footprints +
demographics) and writes:
    frontend/assets/data/dr_features_<city>.json

Keyed by building centroid "lon,lat" rounded to 5 decimals (~1.1 m). The routing
keys are building centroids to ~0.04 m, and the app rounds turf.centroid the same
way, so the runtime join is reliable; the few misses are mean-imputed in DR.

Exported per building (the winning combo ACCESS+FORM+MODAL+DEMO; ACCESS already
lives in the app via fairness scores, so only the extra blocks are baked):
  modal_walk_drive, modal_cycle_drive            (car-dependency / modal gap)
  form_height, form_logarea, form_logdensity, form_multifamily   (built form)
  demo_need, demo_income                         (socioeconomic, district-blocky)

Run:
  .venv/bin/python tools/analysis/dimensionality.py   # builds the cache first
  .venv/bin/python tools/bake_dr_features.py --city vaxjo
"""
import os, json, argparse, numpy as np

ROOT = os.path.join(os.path.dirname(__file__), "..")
CACHE = os.path.join(ROOT, "tools/analysis/dimensionality_cache.npz")

# cache block name -> output field name
FIELD_MAP = {
    "MODAL:walk_drive":   "modal_walk_drive",
    "MODAL:cycle_drive":  "modal_cycle_drive",
    "FORM:height":        "form_height",
    "FORM:logarea":       "form_logarea",
    "FORM:logdensity":    "form_logdensity",
    "FORM:multifamily":   "form_multifamily",
    "DEMO:needZ":         "demo_need",
    "DEMO:income":        "demo_income",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="vaxjo")
    args = ap.parse_args()

    if not os.path.exists(CACHE):
        raise SystemExit("cache missing — run tools/analysis/dimensionality.py first")
    z = np.load(CACHE, allow_pickle=True)
    coords = z["coords"]
    blocks = z["blocks"].item()

    missing = [b for b in FIELD_MAP if b not in blocks]
    if missing:
        raise SystemExit(f"cache lacks blocks {missing} — rebuild dimensionality cache")

    out = {}
    for i in range(len(coords)):
        lon, lat = coords[i]
        key = f"{lon:.5f},{lat:.5f}"
        rec = {}
        for b, field in FIELD_MAP.items():
            v = blocks[b][i]
            rec[field] = None if not np.isfinite(v) else round(float(v), 4)
        out[key] = rec

    dest = os.path.join(ROOT, f"frontend/assets/data/dr_features_{args.city}.json")
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    sz = os.path.getsize(dest) / 1e6
    print(f"[dr-features] {args.city}: {len(out)} buildings -> {dest}  ({sz:.1f} MB)")
    # quick stats
    for b, field in FIELD_MAP.items():
        v = blocks[b][np.isfinite(blocks[b])]
        print(f"  {field:20s} min={v.min():.2f} med={np.median(v):.2f} max={v.max():.2f}")


if __name__ == "__main__":
    main()
