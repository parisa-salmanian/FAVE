# Local road-routing for FAVE (offline, accurate)

This sets up **real road-network distances** for fairness while keeping the
running app fully offline. You run OSRM locally **once** to *bake* per-building
access distances into `frontend/assets/data/`. After baking, the app reads the
baked files — it never talks to OSRM, Docker, or the internet at runtime.

Travel modes baked: **walking, cycling, driving**. (Public transport is a
separate future step via GTFS.)

## What you need

- **Docker Desktop**, installed and running. (Used only for baking, never at
  app runtime.)
- ~2 GB download (Sweden OSM extract) + several GB of working files.
- The Python venv with `requests` (the same one used by `bake_city_data.py`).

## One-time steps

```bash
# 1) Prepare the three routing profiles (downloads Sweden extract, preprocesses).
#    ~10-40 min depending on machine. Run from the repo root.
cd tools/osrm
chmod +x setup_osrm.sh
./setup_osrm.sh                 # all three; or: ./setup_osrm.sh foot

# 2) Start the three local OSRM servers (ports 5001/5002/5003).
docker compose up -d
docker compose ps              # all three should be "running"

# 3) Bake the road-distance access files for every city.
#    Start with a smoke test on one small city, then do the rest.
cd ../..
.venv/bin/python tools/bake_routing.py --cities vaxjo --limit 2000   # quick check
.venv/bin/python tools/bake_routing.py                               # all cities, all modes

# 4) Stop the servers when baking is done — the app does not need them.
cd tools/osrm && docker compose down
```

## Tips / scaling

- Big cities (Stockholm ~150k buildings, Göteborg, Malmö) take the longest.
  Bake one city at a time with `--cities <key>` if you want to checkpoint.
- Re-bake a single mode or city with `--modes walking` / `--cities malmo`.
- `--force` overwrites existing baked files (use after changing POIs or
  profiles); without it, existing `<mode>/<cat>.json` files are skipped.
- The bake loads each building GeoJSON fully into memory; Stockholm (115 MB)
  needs a few GB of RAM. Close other heavy apps if needed.

## Output

```
frontend/assets/data/cities/<key>/routing/
  index.json                     summary + key contract
  walking/<category>.json        { key[], m[]=metres, s[]=seconds } per building
  cycling/<category>.json
  driving/<category>.json
```

`key[i]` is the building centroid `"lon,lat"` rounded to 6 decimals — computed
the same way (turf.centroid) in the baker and the frontend, so lookups line up.

## Verifying it's offline

After baking, run the app normally (backend + frontend, no Docker). Open the
browser Network tab and compute fairness: there must be **no requests** to
`router.project-osrm.org`, `overpass-api.de`, or `nominatim.*`. Distances now
come from the baked `routing/` files.
