# EpiCity code & data, vendored into FAVE

This directory holds the **EpiCity** source (synthetic-population generation,
public-transport model, epidemic engines, SCB/OSM data tooling) copied verbatim
from `C:\Users\clliab\vis_projects\EpiCity` on 2026-06-18, so FAVE can grow the
same capabilities (synthetic population, public transport, "and other stuff").

> ⚠️ This is **vendored, not yet integrated.** None of it is wired into FAVE's
> `api/` app — FAVE runs exactly as before. Integration is the next step.

## What was brought over

### Code (here, `epicity_engine/`)
- All 17 EpiCity top-level Python modules (flat layout, EpiCity's original
  absolute imports preserved — keep this dir on `sys.path` to import them).
- `static_reference/` — EpiCity's 40 frontend JS files (THREE.js based) for
  reference only. Relevant ones: `transport.js`, `transport-pop.js`,
  `agents-layer.js`, `infection-flow.js`, `engine-mode.js`, `popup-manager.js`.
- `requirements_epicity.txt`, `pyproject_epicity.toml` — EpiCity's dep manifests.

### Data (under `frontend/assets/data/`)
- Per-city EpiCity data → `frontend/assets/data/cities/<key>/epicity/` for the
  7 FAVE cities (vaxjo, malmo, goteborg, stockholm, kalmar, norrkoping, uppsala).
- National/shared → `frontend/assets/data/epicity/_national/` and
  `frontend/assets/data/epicity/operator_brands.json`.

## Key facts (from a read of both codebases)

- **Synthetic population is computed, not stored.** Generated at runtime by
  `engine_abm.py` (`_Population`, vectorised NumPy per-individual arrays) and
  `engine.py` (per-building synthetic households/capacity). Inputs: each city's
  `city.json` (buildings + per-building `population`) plus DESO demographics
  (`deso*.json`, `households*.json`, `country_of_birth*.json`).
- **Public transport** lives in `transport.py` (`TripDispatcher`, synthetic line
  & schedule builders). Inputs: `transport_infra.json`, `transport_schedule.json`,
  `bus_routes_raw.json`, `wide_rail_raw.json`, `operators.json`.
- Both subsystems are **coupled to EpiCity's SEIR/ABM disease model** — e.g.
  `transport.py` applies "mass-transfer SEIR events"; the synthetic population is
  SEIR-state arrays. Reusing them in FAVE (an accessibility-fairness app) means
  decoupling the population/transport generation from the epidemic dynamics.
- **Frontend is incompatible as-is:** EpiCity uses THREE.js (3D, game-world
  `world_x/world_z` coords); FAVE uses MapLibre + Deck.gl (2D, lat/lon). The
  viz must be re-implemented for FAVE, not copied.

## Import status (under FAVE's `.venv`, 2026-06-18)

All modules import EXCEPT `main.py` (EpiCity's FastAPI entry; fails only because
it mounts a `static/` dir at import — not needed here). The reusable cores
import fine: `engine`, `engine_abm`, `transport`, `osm`, `demography`,
`climate`, `metapop`, `scb`, `abm_paths`, `spatial`, etc.

FAVE's existing deps cover EpiCity's needs (numpy, fastapi, pydantic, shapely,
pandas). EpiCity adds no exotic deps (no networkx/scipy/geopandas required).

## Integration TODO (next step — needs a scope decision)

1. Decide depth: (a) **vendored module** callable from FAVE, (b) **adapt
   synth-pop + transport only** to FAVE's fairness purpose (no epidemic model),
   or (c) **full epidemic ABM** inside FAVE.
2. Decouple `_Population` / `TripDispatcher` from SEIR state if going with (b).
3. Bridge data formats: EpiCity `city.json`/`world_x,z` ↔ FAVE lat/lon GeoJSON.
4. Add FAVE backend endpoints (e.g. `/api/population`, `/api/transport`).
5. Re-implement viz as Deck.gl layers/views in `frontend/assets/js/views/`.
6. Careful column-level dedup of overlapping demographics (EpiCity `deso_gender`/
   `deso_income` vs FAVE `*_age_gender.json`, `demographics_vaxjo.json`).
