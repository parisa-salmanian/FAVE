# CLAUDE.md

Working notes for Claude (and human contributors) on the FAVE codebase. Keep
this file accurate — it is the single source of truth for conventions that
are not obvious from the code alone.

## What FAVE is

Visual analytics system for urban accessibility fairness across Swedish
cities. FastAPI backend (`api/`), vanilla-JS frontend (`frontend/`), local
LLM via Ollama. See `README.md` for the user-facing description.

## Hard rule: no external APIs at runtime

The running app must never call external APIs. Everything the frontend or
backend needs at runtime has to live in `frontend/assets/data/`. External
APIs (Nominatim, Overpass, OSRM) are only allowed inside the offline bake
scripts under `tools/`, which run during development.

If you find yourself adding a `fetch()` call to an external host in code that
runs at request time, stop. Either bake the data first or use the local
fallback (Euclidean distances, in-memory POI cache, etc.). The only
"external" service the running app may talk to is the local Ollama daemon at
`http://127.0.0.1:11434` — it ships with the dev environment.

## Supported cities

The frontend offers a fixed set of cities. The canonical list lives in two
places that must stay in sync:

- `tools/bake_city_data.py` → `CITIES` dict (key → Nominatim query string)
- `frontend/assets/js/main.js` → `LOCAL_CITY_NAMES`, `BUILDING_URL_BY_CITY_KEY`,
  `DISTRICT_URL_BY_CITY_KEY`

Currently: `vaxjo, malmo, goteborg, stockholm, kalmar, norrkoping, uppsala`.

Free-text city input is intentionally not in the UI. Adding a new city is a
**code change + bake step** (next section).

## Adding a new city (dev-only workflow)

When the user asks "please add city X", do this end-to-end:

1. **Building data**: drop a building GeoJSON for the city into
   `frontend/assets/data/byggnad_<key>.geojson`. Source is up to you
   (Lantmäteriet, OSM, Overpass) — must be `FeatureCollection` with polygon
   geometries and one feature per building. Match the schema of the existing
   `byggnad_*.geojson` files (look at `byggnad_malmo.geojson` for reference).

2. **District boundaries**: drop a regions/DESO file into
   `frontend/assets/data/<key>_regso.geojson` (or whatever boundary set fits
   the city's country). Polygons with at least `Deso` / `Regso` / `name`
   property.

3. **Demographics**: drop `<key>_age_gender.json` into
   `frontend/assets/data/`. Same shape as `malmo_age_gender.json`.

4. **Bake POIs**: run

   ```bash
   .venv/Scripts/python.exe tools/bake_city_data.py --add <key> "City Name, Country"
   ```

   That writes `frontend/assets/data/cities/<key>/{meta.json,pois/*.geojson}`.
   POIs come from Overpass; the bbox comes from Nominatim. Both are external
   API calls — they must happen here, not at runtime.

5. **Wire the city into the frontend**: edit `frontend/assets/js/main.js`:
   - Add the key to `LOCAL_CITY_NAMES` (display name)
   - Add the building file path to `BUILDING_URL_BY_CITY_KEY`
   - Add the district file path to `DISTRICT_URL_BY_CITY_KEY`
   - Add the demographics path to `GENDER_AGE_POP_URL_BY_CITY_KEY` if applicable

6. **Make the bake permanent**: edit `tools/bake_city_data.py` and add the
   key + Nominatim query to the `CITIES` dict, so the next full bake
   includes it.

7. **Verify**: restart backend + frontend, pick the new city in the UI, run a
   fairness compute, confirm no network requests fire (check the browser's
   Network tab — no overpass-api.de, no nominatim.openstreetmap.org, no
   router.project-osrm.org).

## Re-baking existing cities

```bash
.venv/Scripts/python.exe tools/bake_city_data.py            # all cities, skip cached
.venv/Scripts/python.exe tools/bake_city_data.py --force    # all cities, refetch
.venv/Scripts/python.exe tools/bake_city_data.py --cities vaxjo --force
```

Re-bake when:
- The Overpass tagging conventions for a category change.
- You add a new POI category to `POI_QUERIES`.
- The city's bbox is wrong (use Nominatim manually to verify).

## OSRM and routing

OSRM is **not** called at runtime. Road-distance fairness is served from
**baked matrices**, computed once during development with a local OSRM
instance and stored under `frontend/assets/data/cities/<key>/routing/`:

- `tools/osrm/` — Docker setup for three local OSRM servers (walking/cycling/
  driving) + `RUNBOOK.md`. Dev-only; the app never talks to these.
- `tools/bake_routing.py` — for each building, bakes the 3 nearest POIs by
  real road distance per category per mode into
  `routing/<mode>/<cat>.json` (`{ key[], m[], s[], v[] }`, keyed by the
  building centroid `"lon,lat"` at 6 decimals — turf.centroid semantics,
  matched in JS).
- Frontend: `fairness.js` `ifCityAccessibilityForBuilding` uses the baked
  road distances when present (`ensureBakedRouting` / `bakedRoutingNeighbours`),
  and **falls back to local haversine** when a building has no baked entry or
  when interactive what-if POIs are active. Never a live network call.

Re-bake after changing POIs or OSRM profiles (`--force`). The old live OSRM
path in `api/fairness_routes.py` / `interaction.js` is legacy and gated off
(`USE_TRAVEL_TIME=false`).

### Public transport (transit mode)

OSRM can't route transit (no timetables), so the `transit` mode has its **own**
offline bake using GTFS + r5py (Conveyal R5):

- Data: a static GTFS feed (Trafiklab "GTFS Sverige 3", covers all Swedish
  cities) at `tools/transit/gtfs-sweden.zip`, plus the OSM extract already used
  for OSRM (walk access/egress legs). Dev-only, never at runtime.
- `tools/bake_transit.py` — bakes the nearest POIs **by door-to-door transit
  time** into `routing/transit/<cat>.json`, **same `{key,m,s,v}` schema** as the
  OSRM bake so the frontend reads it via the same `ensureBakedRouting` path. For
  transit, `s[]` is the meaningful field (seconds) and `m[]` is informational
  straight-line metres. See `tools/transit/RUNBOOK.md`.
- Scenario: a single representative **weekday morning peak** (one `--date`,
  08:00 window, median time). Transit is time-dependent — that one scenario is
  what the UI's "Public transport" mode means. Re-bake with a different date for
  another scenario.
- Frontend: transit decays on **time** (`s[]`) via `ifCityTransitTimeForMode`,
  not distance — see the `isTransit` branch in `ifCityAccessibilityForBuilding`
  (`fairness.js`). Buildings with no reachable POI fall back to walking. Mode is
  wired through `normalizeTravelMode` + the `transit` entries in the
  `TRAVEL_SPEED_KMH` / `IF_CITY_MODE_*` tables in `lib/config.js`.

## Population / demand weighting

The IF-City model weights each building's accessibility by estimated
population (demand). `initVaxjoDemandWeights` (historical name) runs for **any**
city wired into `GENDER_AGE_POP_URL_BY_CITY_KEY` (cityPaths.js) — it
distributes district REGSO population across residential buildings by floor
area. Add a city's `*_age_gender*.json` to that map to enable population
weighting for it; otherwise demand weight defaults to 1 (distance-only).

## Local dev (running the app)

Three services, three terminals (or background processes):

```bash
# Backend
.venv/Scripts/python.exe -m uvicorn api.server:app --host 127.0.0.1 --port 8001 --reload

# Frontend (from repo root)
cd frontend && python -m http.server 5500

# LLM daemon (auto-starts on Windows install; otherwise:)
ollama serve
```

App: http://127.0.0.1:5500/index.html. Health: http://127.0.0.1:8001/health.

## Layout

```
api/                           FastAPI backend
  server.py                    Routes + job pipeline (uses Overpass — see "Hard rule" above; this path is being phased out)
  fairness_routes.py           Distance/search endpoints (Euclidean is the runtime path)
  llm_routes.py                Ollama-only LLM proxy
  ebm_service.py               Explainable Boosting Machine training endpoint
api_data/                      Runtime artifacts (job outputs, static dirs). Not source.
frontend/
  index.html                   Single-page app shell (1330 lines — split candidate)
  assets/
    js/                        Frontend code (main.js is being broken up — see below)
    data/                      All bundled data. Nothing fetched from a remote host belongs anywhere else.
    data/cities/<key>/         Per-city baked artifacts (meta + POIs)
tools/
  bake_city_data.py            One script that hits external APIs. Run it during dev only.
```

## Frontend code organization

The original 16k-line `main.js` has been split into ~30 classical-script
modules under `frontend/assets/js/`:

```
lib/        config.js cityPaths.js poi.js visuals.js poiAtlas.js
            helpers.js drEncoders.js state.js utils.js
models/     changeHistory.js cityLoader.js districts.js fairness.js
            history.js loaders.js summarize.js
views/      drSpinner.js drView.js equity.js layers.js overlays.js
            poiMarkers.js popupView.js sidePanel.js
            whatIfMockBuildings.js yearHighlight.js
controllers/ apiClient.js drFeatureMode.js interaction.js
            llm.js mapLasso.js
main.js     5-line stub; load order is enforced in index.html
```

When you edit, **put new code in the appropriate module**, not in main.js.

**Load order matters.** The files are classical scripts (not ES modules), so
top-level `let`/`const` declarations are shared across `<script>` tags in
the same Realm. The order in `index.html` is: lib → models → views (with
drView near the end of the views block) → controllers → main.js → post-main
wrappers (drFeatureMode, history, llm, equity). If you add a new file, slot
it where its dependencies are already declared.

**State lives in `lib/state.js`.** That includes the `DOMContentLoaded` boot
handler — it ended up there because it was inside the original "Global
state" section. Long term this should move to a thin `app.js` entry point.

**Dead code:** `controllers/apiClient.js` and `models/loaders.js`'s
`loadCityOSM` are leftovers from the OSM-API source mode that was removed.
Don't extend them; if road-distance routing is ever needed, the right
architecture is to bake matrices via a `tools/` script (see "OSRM and
routing" above).

## Pinned versions / Python

The backend venv uses Python **3.10** (not 3.14) because pinned deps
(`numpy==2.0.2`, `osmnx==1.9.3`) lack 3.14 wheels. Recreate with
`py -3.10 -m venv .venv` if needed.
