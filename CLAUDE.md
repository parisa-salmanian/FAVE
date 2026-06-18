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

OSRM is **not** called at runtime. The fairness backend
(`api/fairness_routes.py`) currently still has the OSRM code path, but the
production behavior should always fall through to Euclidean. If you bring
back road-distance fairness, the right architecture is to bake distance
matrices into `frontend/assets/data/cities/<key>/routing/` during city
addition (using a local OSRM instance), not to reintroduce live calls.

**Baked routing matrices ARE now consumed (added 2026-06-18).** The
`cities/<key>/routing/<mode>/<cat>.json` files (per building: `m` = network
metres to nearest 15 POIs, `v` = opportunity values) feed accessibility via
`frontend/assets/js/lib/routingMatrix.js`. `models/fairness.js` uses real
network distance (`ifCityNetworkDistanceForMode`, detour factor dropped) for
walk/cycle/drive instead of haversine. Buildings resolve to a matrix row by
exact `"lon,lat"` key (6 dp) with a nearest-key snap fallback (≤45 m) — the
pre-existing bake used a different centroid so exact match is ~81%; the snap
lifts coverage to ~100%. Categories with active what-if edits keep the
haversine model (added/removed POIs aren't in the static matrices). `transit`
mode uses its own network (see above); `USE_TRAVEL_TIME` still gates live OSRM
only.

## Public transport ("transit") accessibility mode

`transit` is a travel mode alongside walking/cycling/driving in the fairness
"Mode" dropdown. Unlike the others (which use the speed/factor gravity model in
`config.js`), transit uses a **baked stop network**, honoring the no-runtime-API
rule:

- Source data: EpiCity transit data under
  `frontend/assets/data/cities/<key>/epicity/` (`transport_infra.json` stops,
  `transport_schedule.json` lines/trips). Ported from EpiCity (see
  `epicity_engine/`).
- Bake: `tools/bake_transit_network.py` → `cities/<key>/transit/network.json`
  (stops + stop→stop in-vehicle time matrix + per-stop boarding wait). It adds
  walking-transfer edges between stops within 150 m (EpiCity stops aren't
  deduped by location, so hubs must be linked to enable transfers). Re-bake all
  7 cities with `python tools/bake_transit_network.py`.
- Runtime: `frontend/assets/js/lib/transit.js` loads the network and
  `models/fairness.js` computes transit travel time =
  access walk → nearest stop + boarding wait + matrix in-vehicle time + egress
  walk → POI. Unreachable pairs / cities without a baked network fall back to
  the per-mode speed model. To register a mode, see the maps in `config.js`,
  the `#fairnessTravelMode` `<select>` in `index.html`, `normalizeTravelMode()`
  in `fairness.js`, and `DEFAULT_SPEED_MPS` in `api/fairness_routes.py`.

## 2SFCA supply-to-demand accessibility (companion metric)

A **network-accurate Enhanced 2-Step Floating Catchment Area (E2SFCA)** layer,
added 2026-06-18 as a *companion* to the headline gravity model. The gravity
model answers "how close are opportunities"; 2SFCA answers "how much supply is
actually available to me once everyone else competing for it is accounted for"
(supply-to-demand crowding). Both are network-accurate — no straight-line.

- Bake: `tools/bake_access2sfca.py` (uses OSMnx 1.9.3 — bake-time only, honoring
  the no-runtime-API rule). Per city/mode it builds a real OSMnx graph
  (walk/bike/drive), snaps POIs + building centroids to nodes, runs single-source
  Dijkstra from each POI (POIs are few), and computes two-step E2SFCA with a
  Gaussian decay `W(d)=exp(-d²/(2σ²))`, `σ=catchment/3`. Demand `P_i` mirrors
  `epicityDemographics.js`: each DESO's population distributed across its
  residential buildings (`andamal1`=`Bostad…`) by floor area. Capacity `S_j`
  from POI `beds`/`capacity` tags else 1 (mostly uniform → relative 2SFCA).
  Output `cities/<key>/access2sfca/{index.json,<mode>.json}`: `index.json` holds
  the shared building-coordinate key array; each `<mode>.json` has per category a
  sparse `a:[[bldgIdx,A_i],...]` plus per-POI crowding `R_j`. Re-bake one city
  with `python tools/bake_access2sfca.py <city>` (big metros are slow — driving
  graphs are large). Catchments per category scale by mode (walk×1, cycle×3,
  drive×8).
- Runtime: `frontend/assets/js/lib/access2sfca.js` loads index + mode file,
  joins buildings by `"lon,lat"` coordinate snap (≤45 m — runtime building set is
  filtered to ~45.7k vs the bake's ~50.3k, so featIdx can't be used). Raw A
  values scale inversely with each category's total demand, so they're NOT
  comparable across categories; the consumer normalises **per category**
  (log1p + robust p10..p95 clamp) to 0..1 provision, then combines by mean for an
  overall score. `views/inspector.js` `setBuildingSelection` attaches it async
  (mode follows `#fairnessTravelMode`, transit falls back to walking) and renders
  "Supply provision (2SFCA · <mode>)" + a per-category breakdown. It does NOT feed
  the Gini/headline number — purely an inspector companion (priority-zone use is
  a future step).

## Demographic fairness weighting (EpiCity SCB data)

Per-DESO socioeconomics shape the fairness/Gini model so under-served, high-need
areas read as more unfair (closer to how planners assess equity):

- Bake: `tools/bake_demographics.py` → `cities/<key>/demographics/deso.geojson`
  (DESO polygons + pop, income, child_frac, elder_frac, dependency, and a
  composite `needZ` = 0.65·z(−income) + 0.35·z(dependency)). Source is EpiCity
  SCB data under `cities/<key>/epicity/` (`deso.json`, `deso_income.json`,
  `deso_gender.json`). Baked for all 7 cities.
- Runtime: `frontend/assets/js/models/epicityDemographics.js` loads it, maps each
  building to its DESO (point-in-polygon, bbox-accelerated), and exposes
  `epiBuildingNeedMap` (needZ) + `epiBuildingPopMap`. `models/fairness.js` applies
  `socioWeight = needWeightFromZ(needZ, EPI_SOCIO_STRENGTH)` (default 0.5, on by
  default) in the benefit product. Effect on Växjö walking: Gini 0.061 → 0.130.
- NOT yet used for demand: distributing per-DESO population over ALL building
  types over-weights large non-residential buildings (0.5–5x, distorts Gini).
  Demand still uses the residential-based map; wiring EpiCity's per-building
  *synthetic* (residential) population from `city.json` is the proper next step.

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
