# Public-transport routing for FAVE (offline, GTFS + r5py)

This bakes **public-transport travel times** into `frontend/assets/data/` so the
"Public transport" mode in the UI works fully offline — exactly like the OSRM
walking/cycling/driving bake (see `tools/osrm/RUNBOOK.md`). You run a transit
router locally **once** to bake per-building access times; after that the app
reads the baked files and never talks to a router, Java, or the internet.

Unlike OSRM, public transport needs **timetables**, so this pipeline uses GTFS
schedules + a transit-aware router (r5py / Conveyal R5) instead of OSRM.

## Scenario baked

A single **representative weekday morning peak**: one date, departure window
08:00–09:00, median travel time. That is what the "Public transport" mode
represents. Summer vs winter and peak vs off-peak all differ — re-bake with a
different `--date` / `--departure` for another scenario.

## Prerequisites (macOS, what actually works here)

The project's main `.venv` is **Apple CommandLineTools Python**, which is signed
with the hardened runtime and **cannot host an embedded JVM** — jpype crashes it
with SIGBUS while the JVM maps executable code pages. So the transit bake uses a
**separate venv built on Homebrew Python** (ad-hoc signed, JIT allowed). This is
already set up; to recreate from scratch:

```bash
brew install openjdk@21 osmium-tool python@3.12

# dedicated transit venv on Homebrew Python (NOT the project .venv)
/opt/homebrew/opt/python@3.12/bin/python3.12 -m venv .venv-transit
.venv-transit/bin/pip install r5py geopandas shapely requests
# if `simplification` fails to build, force its wheel: pip install simplification==0.7.12
```

Every transit command must run with `JAVA_HOME` pointing at the JDK:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
```

## Data (already downloaded)

`tools/transit/gtfs-sweden.zip` (≈68 MB) is **GTFS Sverige 2**, the nationwide
Swedish feed covering all cities. It was fetched **keyless** from the Mobility
Database public mirror (no Trafiklab account needed):

```bash
curl -sSL "https://storage.googleapis.com/storage/v1/b/mdb-latest/o/se-trafiklab-gtfs-sverige-2-gtfs-2661.zip?alt=media" \
  -o tools/transit/gtfs-sweden.zip
```

> The canonical source is **Trafiklab** ("GTFS Sverige 3", needs a free API key).
> The Mobility Database mirror is used here because it needs no account. A static
> feed is valid for a limited window — this one covers **2026-05-30 → 2027-05-31**,
> so pick a weekday `--date` inside that range.

The OSM street network reuses the OSRM extract
(`tools/osrm/data/sweden-latest.osm.pbf`, 769 MB) for walk legs.

## How the baker handles two real-world snags

`tools/bake_transit.py` does these automatically (cached, one-time):

1. **GTFS route_type fix** — R5 rejects the feed's `1501` (communal taxi) codes.
   The baker writes a sanitised `gtfs-sweden-r5.zip` remapping them to `3` (bus).
2. **OSM cropping** — R5 is built for regional extracts, so the baker crops the
   769 MB national pbf via `osmium extract`, cached under
   `tools/transit/osm_crops/<key>.osm.pbf`. The crop is taken from the **actual
   building-centroid extent** (+0.05° margin), NOT the Nominatim `meta.json`
   bbox: those bboxes are ~0.32° while the building files span whole regions
   (Norrköping ~2°), so cropping to the meta bbox silently dropped most buildings
   (they couldn't be routed and fell back to walking). Crop to the building
   extent so every building R5 routes is inside the network.

## Run

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21

# smoke test first (first 150 buildings of one city)
.venv-transit/bin/python3 tools/bake_transit.py --date 2026-06-09 --cities vaxjo --limit 150

# one city, full
.venv-transit/bin/python3 tools/bake_transit.py --date 2026-06-09 --cities vaxjo

# all cities (long — see timing below)
.venv-transit/bin/python3 tools/bake_transit.py --date 2026-06-09

# re-bake / overwrite (also regenerates the OSM crop + sanitised GTFS)
.venv-transit/bin/python3 tools/bake_transit.py --date 2026-06-09 --cities malmo --force
```

Flags: `--departure 08:00`, `--window 60` (min), `--max-minutes 90` (drop POIs
slower than this), `--max-walk 20` (cap each walk leg).

**Timing.** All POI categories are routed in ONE matrix per origin chunk, so cost
scales with building count, ~150 buildings ≈ 16 s on an M-series Mac. Växjö
(~50 k buildings) ≈ 1.5 h; Stockholm (~150 k) ≈ several hours. Bake big cities
one at a time / overnight, like the OSRM bake. Existing `<cat>.json` files are
skipped unless `--force`, so an interrupted run resumes per city.

## Output

```
frontend/assets/data/cities/<key>/routing/
  transit/<category>.json     { key[], m[]=straight-line metres, s[]=transit seconds, v[] }
  transit_index.json          summary + the scenario you baked
```

`key[i]` is the building centroid `"lon,lat"` (6 decimals, turf.centroid) — the
same key the OSRM bake uses, so the frontend reads transit via the same
`ensureBakedRouting` / `bakedRoutingNeighbours` path. For transit, neighbours are
ranked **by time** and the fairness model decays on `s[]` (seconds) via
`ifCityTransitTimeForMode` in `fairness.js`. Buildings with no POI reachable
within `--max-minutes` are omitted → the frontend falls back to walking.

## Verifying it's offline

Run the app normally (backend + frontend, no Java), pick "Public transport", and
compute fairness. The Network tab must show **no** requests to any router — the
times come from the baked `routing/transit/` files.
```
