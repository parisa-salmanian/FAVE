"""
main.py — FastAPI application wiring the SEIR engine to HTTP.

Multi-city pool: a lazily-populated, LRU-bounded dict of EpidemicEngine
instances keyed by city_id. Each warm engine retains its own day, cells,
history, interventions, weather and patient-zero state — so leaving a
city via /api/leave_city and returning later restores its exact state.

Global model parameters (β, σ, γ, μ, waning_days, stochastic, seed) live
in `_global_params` and are pushed into every new engine on creation.
This means tweaking sliders in one city carries over to the next city
the user enters, but per-city state (interventions, infection levels)
stays isolated.

Run:
    uvicorn main:app --reload --port 8000
"""
import json
import os
import random
from collections import OrderedDict

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import osm
import transport
import abm_paths
from engine import EpidemicEngine, VALID_INTERVENTION_IDS
from engine_abm import (
    AgentEngine,
    VALID_POPULATION_SCALES as _VALID_POP_SCALES,
    VALID_TIME_RESOLUTIONS  as _VALID_TIME_RES,
)
from metapop import metapop
from references import get_references
from presets import (
    get_presets, get_preset, default_preset, VALID_PRESET_IDS,
)
import demography
import city_stats
from game import (
    GameState, DIFFICULTY_PROFILES, VALID_DIFFICULTIES, GAME_DURATION_DAYS,
)
from spatial import (
    VALID_TYPES as SPATIAL_VALID_TYPES,
    PER_DAY_COSTS as SPATIAL_PER_DAY_COSTS,
)

app = FastAPI(title="EpiCity")

# ── Multi-city engine pool ────────────────────────────────────────────────────

# LRU pool of warm engines, keyed by city_id. Most-recently-used at the end
# (OrderedDict.popitem(last=False) yields the LRU). Bounded by MAX_WARM_ENGINES
# so memory stays predictable as the user explores more cities.
engines: "OrderedDict[str, EpidemicEngine]" = OrderedDict()
_layout_caches: dict[str, dict] = {}
_city_stats: dict = {}     # {city_id: {population, building_count, landmark_count}} + sentinel None: True once the whole stats file has been loaded
_transport_caches: dict[str, dict] = {}    # city_id → {infra, schedule}
_active_city_id: str | None = None
MAX_WARM_ENGINES = 6

# Global simulation parameters — preserved across cities. Mutated by
# /api/params and /api/simulation_mode (write-through). Each new engine
# is seeded with these via `_apply_globals()`. Defaults come from the
# default pathogen preset (COVID-19, Li 2020 / Verity 2020).
_default_pathogen = default_preset()["params"]
_global_params: dict = {
    "beta":        _default_pathogen["beta"],
    "sigma":       _default_pathogen["sigma"],
    "gamma":       _default_pathogen["gamma"],
    "mu":          _default_pathogen["mu"],
    "waning_days": _default_pathogen["waning_days"],
    "stochastic":  True,
    "seed":        random.randint(0, 1_000_000),
    # Pinned starting season for the next city entry. None = random (each
    # reset rolls a fresh DoY). Set to one of "winter"/"spring"/"summer"/
    # "autumn" via /api/start_season — applied to every engine on creation
    # and to the active engine on toggle (with a reset).
    "start_season": None,
}

VALID_SEASONS = {"random", "winter", "spring", "summer", "autumn"}

# Global mode selection — chosen on the macro view, applied when a city is
# entered. Sandbox is the legacy free-customise mode; game flips on
# scoring + auto-seed + hidden parameter sliders.
_game_config: dict = {
    "mode":       "sandbox",       # "sandbox" or "game"
    "difficulty": "normal",        # "easy" | "normal" | "hard"
}

# Global engine selection — also chosen on the macro view, before city
# entry. "compartmental" is the legacy building-level SEIR; "abm" is the
# per-agent activity-based engine (Phase 1a stub; Phase 2 replaces the
# stepper). Switching engine kinds invalidates every warm engine in the
# pool because the underlying class is different — see post_engine_mode.
VALID_ENGINE_KINDS: set[str] = {"compartmental", "abm"}
_engine_config: dict = {
    "engine":            "abm",
    "population_scale":  1,    # ABM only — see engine_abm.VALID_POPULATION_SCALES
    "time_resolution_h": 1.0,  # ABM only — see engine_abm.VALID_TIME_RESOLUTIONS
}


def _is_game_mode() -> bool:
    return _game_config["mode"] == "game"


def _apply_game_to_engine(eng: EpidemicEngine) -> None:
    """
    Push the current `_game_config` into `eng`. In game mode, also override
    base SEIR β/μ from the difficulty profile and instantiate a fresh
    GameState. The caller is expected to reset the engine afterwards so the
    new seed policy + base params take effect.
    """
    if _is_game_mode():
        prof   = DIFFICULTY_PROFILES[_game_config["difficulty"]]
        gstate = GameState.for_difficulty(_game_config["difficulty"])
        eng.set_game_mode(
            mode="game",
            difficulty=_game_config["difficulty"],
            game_state=gstate,
            auto_seed_n=prof["patient_zero_n"],
        )
        # Override base β + μ from the difficulty profile. Other params
        # (σ, γ, waning) come from the active pathogen preset.
        eng.update_params(beta=prof["beta"], mu=prof["mu"])
    else:
        eng.set_game_mode(mode="sandbox", difficulty="normal", game_state=None)


def _apply_globals(eng: EpidemicEngine) -> None:
    """Push the current global params into a freshly-created engine."""
    eng.update_params(
        beta=_global_params["beta"],
        sigma=_global_params["sigma"],
        gamma=_global_params["gamma"],
        mu=_global_params["mu"],
        waning_days=_global_params["waning_days"],
    )
    eng.set_simulation_mode(
        stochastic=_global_params["stochastic"],
        seed=_global_params["seed"],
    )
    eng.set_start_season(_global_params.get("start_season"))


def _new_engine_for(city_id: str) -> EpidemicEngine:
    """
    Factory: build the right engine class for `city_id` given the current
    `_engine_config` selection. AgentEngine is a subclass of EpidemicEngine,
    so the return type is unchanged from the caller's perspective.
    """
    if _engine_config["engine"] == "abm":
        return AgentEngine(
            city_id=city_id,
            population_scale=_engine_config["population_scale"],
            time_resolution_h=_engine_config["time_resolution_h"],
        )
    return EpidemicEngine(city_id=city_id)


def _get_or_create_engine(city_id: str) -> EpidemicEngine:
    """
    Lazily build the engine for `city_id`, applying global params on first
    creation. Marks the city as MRU and evicts the LRU when the pool grows
    past MAX_WARM_ENGINES. Raises 404 if the city is unknown or marked as
    not yet `available` in osm.CITIES.

    Note: this does NOT seed the outbreak — `post_select_city` is the
    single owner of `_apply_game_to_engine + eng.reset()` so we do
    exactly one patient-zero pick per /api/select_city call. Picking
    multiple times here used to generate a different random seed every
    pass, which the frontend saw as the camera jumping between
    candidates during city load.
    """
    if city_id in engines:
        engines.move_to_end(city_id)
        return engines[city_id]

    if city_id not in osm.CITIES:
        raise HTTPException(404, f"Unknown city: {city_id!r}")
    if not osm.CITIES[city_id].get("available", False):
        raise HTTPException(404, f"City {city_id!r} is not yet available.")

    eng = _new_engine_for(city_id)
    _apply_globals(eng)

    # Attach a transport dispatcher if a schedule has been built for this
    # city. Missing files are non-fatal — the engine simply runs without
    # transport coupling, identical to pre-feature behaviour.
    _attach_transport(eng, city_id)

    engines[city_id] = eng
    if len(engines) > MAX_WARM_ENGINES:
        evict_id, _evicted = engines.popitem(last=False)
        _layout_caches.pop(evict_id, None)
        _transport_caches.pop(evict_id, None)
    return eng


def _load_brands_config() -> dict:
    """
    Read the editable brand config from ``data/operator_brands.json``.
    Reloads from disk every call so edits take effect immediately.
    """
    brands_path = os.path.join(os.path.dirname(__file__), "data",
                               "operator_brands.json")
    if not os.path.exists(brands_path):
        return {}
    try:
        with open(brands_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _load_branded_operators(city_id: str) -> list[dict]:
    """
    Load raw ``operators.json`` for *city_id*, merge with any pinned
    operators from ``city_overrides``, and enrich each entry with brand
    name + colour from ``data/operator_brands.json``.

    The brands file is re-read every call so edits (colours, overrides)
    take effect on the next city switch — no re-download needed.
    """
    config = _load_brands_config()

    # Build brand lookup: osm_name → {brand, colour, modes}
    brands: dict[str, dict] = {}
    for brand_name, info in config.get("brands", {}).items():
        for osm_name in info.get("osm_names", [brand_name]):
            brands[osm_name] = {
                "brand":  brand_name,
                "colour": info.get("colour"),
                "modes":  info.get("modes", []),
            }

    # Load raw OSM-fetched operators
    ops_path = os.path.join(osm._city_dir(city_id), "operators.json")
    raw: list[dict] = []
    try:
        if os.path.exists(ops_path):
            with open(ops_path, encoding="utf-8") as f:
                raw = json.load(f).get("operators", [])
    except Exception:
        pass

    # Merge city_overrides — pinned operators that survive bad Overpass
    # fetches.  Override entries are added if not already present in the
    # raw data; if present, the higher route_count wins.
    seen: dict[str, dict] = {}
    for op in raw:
        seen[op["name"]] = op

    for override in config.get("city_overrides", {}).get(city_id, []):
        name = override["name"]
        if name in seen:
            # Keep the higher route_count
            if override.get("route_count", 0) > seen[name].get("route_count", 0):
                seen[name]["route_count"] = override["route_count"]
            # Merge modes
            for m in override.get("modes", []):
                if m not in seen[name].get("modes", []):
                    seen[name].setdefault("modes", []).append(m)
        else:
            seen[name] = {
                "name":        name,
                "modes":       override.get("modes", []),
                "colour_osm":  None,
                "route_count": override.get("route_count", 0),
                "source":      "override",
            }

    # Enrich with brand/colour
    enriched = []
    for op in seen.values():
        name = op.get("name", "")
        info = brands.get(name, {})
        enriched.append({
            "name":        name,
            "brand":       info.get("brand", name),
            "modes":       op.get("modes", []),
            "colour":      info.get("colour") or op.get("colour_osm") or op.get("colour"),
            "route_count": op.get("route_count", 0),
            "source":      op.get("source", "osm"),
        })
    enriched.sort(key=lambda o: (-o["route_count"], o["name"]))
    return enriched


def _attach_transport(eng: EpidemicEngine, city_id: str) -> None:
    """
    Build and attach a TripDispatcher for ``city_id``, if both
    transport_infra.json and transport_schedule.json exist on disk. Caches
    the raw bundle for the /api/transport endpoint to serve.
    """
    infra = None
    try:
        infra_path = osm._city_paths(city_id)["transport_infra"]
        if os.path.exists(infra_path):
            with open(infra_path, encoding="utf-8") as f:
                import json as _json
                infra = _json.load(f)
    except Exception as e:
        print(f"[transport] failed to load infra for {city_id}: {e}")
        infra = None
    schedule = transport.load_schedule(city_id)
    if not infra or not schedule:
        eng.attach_dispatcher(None)
        return
    focused_kommun = osm.CITIES.get(city_id, {}).get("kommunkod")
    dispatcher = transport.TripDispatcher(
        infra=infra,
        schedule=schedule,
        buildings=eng._buildings,
        focused_kommun=focused_kommun,
    )
    eng.attach_dispatcher(dispatcher)
    # Load operator metadata — raw OSM data enriched with brand colours from
    # the editable config at data/operator_brands.json.  Colours can be
    # changed there and picked up on next city switch (no re-download).
    operators = _load_branded_operators(city_id)
    _transport_caches[city_id] = {
        "infra": infra, "schedule": schedule, "operators": operators,
    }
    print(f"[transport] attached dispatcher for {city_id}: "
          f"{len(infra.get('stops', []))} stops, "
          f"{len(schedule.get('trips', []))} trips, "
          f"{len(operators)} operators")


def _active() -> EpidemicEngine:
    """Return the active city's engine, or 409 if no city is selected."""
    if _active_city_id is None or _active_city_id not in engines:
        raise HTTPException(
            409,
            "No active city. POST /api/select_city/{city_id} first.",
        )
    engines.move_to_end(_active_city_id)
    return engines[_active_city_id]


def _focused_kommun() -> str | None:
    """Kommunkod of the currently-active city, or None. Used by metapop."""
    if _active_city_id is None:
        return None
    meta = osm.CITIES.get(_active_city_id) or {}
    return meta.get("kommunkod")


def _sync_focused_into_metapop(eng: EpidemicEngine) -> None:
    """
    Push the focused engine's aggregate SEIR totals into the metapop
    shadow node for this city's kommun, so other coarse nodes see an
    up-to-date view of the focused city in tomorrow's FOI computation.
    """
    if not metapop.is_available():
        return
    sn = eng.state()
    metapop.sync_focused_totals(sn["S"], sn["E"], sn["I"], sn["R"], sn["D"])


# ── Request bodies ────────────────────────────────────────────────────────────


class ParamsBody(BaseModel):
    beta:        float | None = None
    sigma:       float | None = None
    gamma:       float | None = None
    mu:          float | None = None
    waning_days: int   | None = None


class RestoreBody(BaseModel):
    day:                  int
    cells:                list[dict]
    history:              list[dict]
    active_interventions: list[str] = []


class SimModeBody(BaseModel):
    stochastic:          bool | None = None
    random_patient_zero: bool | None = None
    seed:                int  | None = None


class ManualPZBody(BaseModel):
    # idx == None clears the manual patient-zero pin and reverts to
    # random/deterministic placement on the next reset.
    idx: int | None = None


# ── City catalogue + lifecycle ────────────────────────────────────────────────


@app.get("/api/cities")
def get_cities():
    """
    List every city in the catalogue (available + stubs).

    Filtered by `osm.VISIBLE_COUNTRIES` so countries that aren't currently
    exposed to the UI stay out of the macro view entirely. The hidden
    cities remain fully functional via direct engine/API calls — this is
    just a front-door filter.
    """
    return [
        {
            "id":         city_id,
            "name":       meta["name"],
            "country":    meta["country"],
            "center_lat": meta["center_lat"],
            "center_lon": meta["center_lon"],
            "available":  meta.get("available", False),
        }
        for city_id, meta in osm.CITIES.items()
        if osm.is_visible(city_id)
    ]


@app.get("/api/cities/summary")
def get_cities_summary():
    """
    Macro-view payload: every city plus its static stats and a `status`
    block for warm engines. Status is `null` for cold (never-visited)
    cities. Used to drive macro marker sizing, hover cards, and the
    Day-N badges.

    Static fields (population, building_count) come from osm.CITIES when
    present; otherwise read from city.json (saved by osm.load_city during
    the download pipeline). This means newly-added cities show correct
    stats without hand-editing osm.CITIES.
    """
    out = []
    for city_id, meta in osm.CITIES.items():
        # Skip cities hidden by the VISIBLE_COUNTRIES filter (Brazil today).
        # Keeps the macro-view marker list and the JSON payload size small
        # while leaving the underlying engine + data files intact.
        if not osm.is_visible(city_id):
            continue
        status = None
        eng = engines.get(city_id)
        if eng is not None:
            sn = eng.state()
            status = {
                "day":   sn["day"],
                "S":     sn["S"], "E": sn["E"], "I": sn["I"],
                "R":     sn["R"], "D": sn["D"],
                "total": sn["S"] + sn["E"] + sn["I"] + sn["R"] + sn["D"],
            }
        # Population, building_count, landmark_count come from the
        # pre-baked data/_national/city_stats.json — written by
        # tools/download_city.py at the end of each city build (and
        # backfilled for already-downloaded cities by
        # tools/backfill_city_stats.py). This avoids reading and
        # parsing every city's 20–50 MB city.json on first macro load
        # (~700 MB total, made the macro view take 30–60 s to first
        # paint). CITIES[population] / [building_count] stay as
        # fallbacks for never-downloaded cities.
        #
        # The whole stats file is loaded once on first hit and cached
        # in-process via _city_stats[None] (sentinel for "all loaded").
        if None not in _city_stats:
            _city_stats[None] = True
            for cid, st in city_stats.load_all().items():
                _city_stats[cid] = st
        if city_id not in _city_stats:
            # Stats file is missing this city (it was downloaded before
            # the cache existed). Fall back to a one-time city.json read
            # and persist the result so the next process starts fast.
            stats = city_stats.compute_from_city_json(city_id) or {}
            _city_stats[city_id] = stats
            if stats:
                try:
                    city_stats.write_one(city_id, stats)
                except OSError:
                    pass
        cs = _city_stats[city_id]

        pop = cs.get("population", meta.get("population"))
        bld = cs.get("building_count", meta.get("building_count"))
        lmk = cs.get("landmark_count", 0)

        out.append({
            "id":             city_id,
            "name":           meta["name"],
            "country":        meta["country"],
            "center_lat":     meta["center_lat"],
            "center_lon":     meta["center_lon"],
            "available":      meta.get("available", False),
            "population":     pop,
            "building_count": bld,
            "landmark_count": lmk,
            "status":         status,
        })
    return out


@app.post("/api/select_city/{city_id}")
def post_select_city(city_id: str):
    """Make `city_id` the active city, lazily creating its engine."""
    global _active_city_id
    eng = _get_or_create_engine(city_id)   # may raise 404
    _active_city_id = city_id
    # If the user toggled mode/difficulty since this engine was last used
    # (e.g. came back from the macro view), re-apply and reset so the
    # game state and seed policy reflect the latest selection.
    _apply_game_to_engine(eng)
    eng.reset()
    # Lazy-load the metapopulation layer on first city selection.
    # If the nationwide data hasn't been fetched yet this is a no-op and
    # the focused engine runs single-city just like before.
    metapop.ensure_loaded()
    metapop.set_focused(_focused_kommun())
    _sync_focused_into_metapop(eng)
    return eng.state()


@app.post("/api/leave_city")
def post_leave_city():
    """
    Drop the active-city pointer (no engine state is destroyed). The
    frontend pauses the sim before calling this; the next request that
    needs an engine will 409 until the user picks a new city.
    """
    global _active_city_id
    _active_city_id = None
    metapop.set_focused(None)
    return {}


# ── Active-engine endpoints ───────────────────────────────────────────────────


@app.get("/api/city")
def get_city():
    """Static city layout for the active city. Cached per-city."""
    eng = _active()
    if _active_city_id not in _layout_caches:
        _layout_caches[_active_city_id] = eng.city_layout()
    return _layout_caches[_active_city_id]


@app.get("/api/transport")
def get_transport():
    """
    Transport infrastructure (stops + routes) and the day's synthetic schedule
    (lines + trips) for the active city. Built offline by transport.py and
    cached on first city activation. Returns ``{infra: {...}, schedule: {...}}``
    or ``{infra: null, schedule: null}`` if no transport data has been built
    for this city (the frontend treats that as "feature unavailable here").
    """
    _ = _active()  # 409 if no active city
    bundle = _transport_caches.get(_active_city_id)
    if bundle is None:
        return {"infra": None, "schedule": None}
    return bundle


_neighborhood_caches: dict[str, list] = {}


@app.get("/api/neighborhoods")
def get_neighborhoods():
    """
    DeSO/RegSO polygon boundaries with human-readable names and demographic
    summaries for the active city.  Cached per-city.  Each entry includes
    population, age mix, income, gender, and household stats aggregated
    from the engine's per-building synthetic data.
    """
    eng = _active()
    if _active_city_id in _neighborhood_caches:
        return _neighborhood_caches[_active_city_id]

    import scb
    try:
        bundle = scb.load_deso(_active_city_id)
        name_map = scb.load_regso_names(_active_city_id)
    except Exception:
        return []

    # Aggregate per-building data by DeSO for the summary popup
    from collections import defaultdict
    deso_stats: dict[str, dict] = defaultdict(lambda: {
        "pop": 0, "res_buildings": 0,
        "child": 0, "adult": 0, "elder": 0,
        "male": 0, "female": 0,
        "income_sum": 0.0, "income_pop": 0,
        "hh_count": 0,
        # Country-of-birth weighted totals (one bucket per origin group).
        "origin_sum": [0.0] * len(demography.ORIGIN_GROUPS),
    })
    _RES = frozenset({1, 2, 3})  # RES_LO, RES_MED, RES_HI
    for b in eng._buildings:
        d = b.deso
        if not d:
            continue
        st = deso_stats[d]
        if b.zone in _RES:
            st["pop"] += b.pop
            st["res_buildings"] += 1
            st["child"] += b.S.get("child", 0) + b.E.get("child", 0) + b.I.get("child", 0) + b.R.get("child", 0) + b.D.get("child", 0)
            st["adult"] += b.S.get("adult", 0) + b.E.get("adult", 0) + b.I.get("adult", 0) + b.R.get("adult", 0) + b.D.get("adult", 0)
            st["elder"] += b.S.get("elder", 0) + b.E.get("elder", 0) + b.I.get("elder", 0) + b.R.get("elder", 0) + b.D.get("elder", 0)
            if b.gender:
                st["male"] += b.gender.get("male", 0)
                st["female"] += b.gender.get("female", 0)
            elif b.gender_frac:
                st["male"] += round(b.pop * b.gender_frac.get("male_frac", 0.5))
                st["female"] += b.pop - round(b.pop * b.gender_frac.get("male_frac", 0.5))
            if b.income is not None:
                st["income_sum"] += b.income * b.pop
                st["income_pop"] += b.pop
            if b.households is not None:
                st["hh_count"] += b.households
            # Population-weighted origin aggregation.
            if b.origin and b.pop > 0:
                for i, f in enumerate(b.origin):
                    st["origin_sum"][i] += b.pop * f

    # Group DeSO areas by their RegSO name so each named neighbourhood
    # appears as a single entry (multiple DeSOs can share a RegSO name).
    areas = bundle.get("areas", [])
    by_name: dict[str, dict] = {}
    for a in areas:
        deso_id = a.get("deso", "")
        name = name_map.get(deso_id, deso_id)
        gtype = a.get("gtype", "Polygon")
        coords = a.get("polygon", [])
        if not coords:
            continue
        st = deso_stats.get(deso_id, {})
        if name not in by_name:
            by_name[name] = {
                "name": name, "polygons": [], "gtypes": [],
                "pop": 0, "buildings": 0,
                "child": 0, "adult": 0, "elder": 0,
                "male": 0, "female": 0,
                "income_sum": 0.0, "income_pop": 0, "hh_count": 0,
                "origin_sum": [0.0] * len(demography.ORIGIN_GROUPS),
            }
        grp = by_name[name]
        grp["polygons"].append(coords)
        grp["gtypes"].append(gtype)
        grp["pop"]       += st.get("pop", 0)
        grp["buildings"] += st.get("res_buildings", 0)
        grp["child"]     += st.get("child", 0)
        grp["adult"]     += st.get("adult", 0)
        grp["elder"]     += st.get("elder", 0)
        grp["male"]      += st.get("male", 0)
        grp["female"]    += st.get("female", 0)
        grp["income_sum"]+= st.get("income_sum", 0.0)
        grp["income_pop"]+= st.get("income_pop", 0)
        grp["hh_count"]  += st.get("hh_count", 0)
        for i, v in enumerate(st.get("origin_sum") or [0] * len(grp["origin_sum"])):
            grp["origin_sum"][i] += v

    result = []
    for name, grp in by_name.items():
        # Merge polygons: collect all rings into a single MultiPolygon
        all_rings = []
        for coords, gtype in zip(grp["polygons"], grp["gtypes"]):
            if gtype == "MultiPolygon":
                all_rings.extend(coords)   # each element is already a polygon
            else:
                all_rings.append(coords)   # single polygon = list of rings
        if len(all_rings) == 1:
            gtype = "Polygon"
            polygon = all_rings[0]
        else:
            gtype = "MultiPolygon"
            polygon = all_rings
        avg_income = (round(grp["income_sum"] / grp["income_pop"], 1)
                      if grp["income_pop"] else None)
        # Normalize origin to fractions summing to 1.0 (or omit if no origins).
        os_sum = sum(grp["origin_sum"])
        origin_frac = (
            [round(v / os_sum, 4) for v in grp["origin_sum"]]
            if os_sum > 0 else None
        )
        result.append({
            "name":     name,
            "gtype":    gtype,
            "polygon":  polygon,
            "pop":      grp["pop"],
            "buildings": grp["buildings"],
            "age": {
                "child": round(grp["child"]),
                "adult": round(grp["adult"]),
                "elder": round(grp["elder"]),
            },
            "male":     grp["male"],
            "female":   grp["female"],
            "avg_income": avg_income,
            "households": grp["hh_count"],
            "origin":     origin_frac,    # 6-vector or null
        })
    _neighborhood_caches[_active_city_id] = result
    return result


@app.get("/api/state")
def get_state():
    """
    Full engine state plus the active engine-mode config (so the frontend
    can show the user which model is running without a second round-trip).
    """
    state = _active().state()
    state["engine_mode"] = {
        "engine":            _engine_config["engine"],
        "population_scale":  _engine_config["population_scale"],
        "time_resolution_h": _engine_config["time_resolution_h"],
    }
    return state


@app.get("/api/agent_positions")
def get_agent_positions(
    states: str = "E,I,R,D",
    max_agents: int = 8000,
    include_home_work: int = 0,
    include_path: int = 0,
):
    """
    Per-agent positions for the live "agents-as-points" map overlay.

    Only available under the ABM engine — returns an empty payload for
    the compartmental engine, which has no per-agent representation.

    `states` is a comma-separated list of state codes to include; default
    excludes S (susceptibles) because that's ~95 % of the population and
    crushes the JSON payload. `max_agents` caps the response with a
    deterministic random sample so cities like Stockholm don't push
    half a megabyte over the wire.

    `include_home_work=1` also returns each agent's home and work
    building world-coords so the frontend can interpolate a "walk"
    between locations during sub-day animation. When 0 the agent's
    current_location is the only position returned (cheaper).

    Payload (compact, parallel int arrays):
      {
        "n":     <int>,
        "day":   <int>,
        "hour":  <float>,
        "x":     [<world_x>, …],          # current location centroid
        "z":     [<world_z>, …],
        "state": [<0..4>, …],             # SEIR state code per agent
        "age":   [<0..2>, …],             # 0 child, 1 adult, 2 elder
        ?"hx":   [...], ?"hz": [...],     # if include_home_work=1
        ?"wx":   [...], ?"wz": [...],
      }
    """
    eng = _active()
    pop = getattr(eng, "_population", None)
    if pop is None:
        return {"engine": _engine_config["engine"], "n": 0, "agents_available": False}

    import numpy as _np
    # Parse the requested state filter ('S' → 0, 'E' → 1, …).
    name_to_int = {"S": 0, "E": 1, "I": 2, "R": 3, "D": 4}
    wanted = set()
    for tok in (states or "").split(","):
        t = tok.strip().upper()
        if t in name_to_int:
            wanted.add(name_to_int[t])
    if not wanted:
        wanted = {1, 2, 3, 4}

    state_arr = pop.state
    mask = _np.isin(state_arr, list(wanted))
    idxs = _np.where(mask)[0]
    if idxs.size > max_agents:
        # Deterministic sample so the viz doesn't flicker between frames.
        sample_rng = _np.random.default_rng(int(idxs.size))
        idxs = _np.sort(sample_rng.choice(idxs, size=max_agents, replace=False))

    # current_location → world coords lookup. Map each building idx to its
    # cell once; agents at commute-pool idxs (above max_b_idx) fall back to
    # their home_building centroid so they still show on the map.
    b_by_idx = {b.idx: b for b in eng._buildings}
    commute_base = getattr(pop, "_commute_base_idx", None)

    def _xy(loc_int: int, home_idx: int) -> tuple[float, float]:
        if commute_base is not None and loc_int >= commute_base:
            # Commute pool — no real world coord; cluster at home for now
            # so the user can SEE the agent's daily oscillation in v1.
            loc_int = int(home_idx)
        b = b_by_idx.get(int(loc_int))
        if b is None:
            return 0.0, 0.0
        return float(b.world_x), float(b.world_z)

    n = int(idxs.size)
    cur_loc = pop.current_location[idxs]
    home_b  = pop.home_building[idxs]
    xs = [0.0] * n
    zs = [0.0] * n
    for i, (li, hi) in enumerate(zip(cur_loc.tolist(), home_b.tolist())):
        xs[i], zs[i] = _xy(li, hi)
    payload = {
        "engine":          _engine_config["engine"],
        "agents_available": True,
        "n":               n,
        "total_n":         int(pop.n),
        "day":             int(eng._day),
        "hour":            float(getattr(eng, "_last_hour", 9.0)),
        "x":               xs,
        "z":               zs,
        "state":           pop.state[idxs].astype(int).tolist(),
        "age":             pop.age_group[idxs].astype(int).tolist(),
    }
    if include_home_work:
        wb = pop.work_building[idxs]
        hxs = [0.0] * n
        hzs = [0.0] * n
        wxs = [0.0] * n
        wzs = [0.0] * n
        for i, (hi, wi) in enumerate(zip(home_b.tolist(), wb.tolist())):
            hb = b_by_idx.get(int(hi))
            if hb is not None:
                hxs[i] = float(hb.world_x); hzs[i] = float(hb.world_z)
            if int(wi) >= 0:
                wbi = b_by_idx.get(int(wi))
                if wbi is not None:
                    wxs[i] = float(wbi.world_x); wzs[i] = float(wbi.world_z)
            else:
                wxs[i] = hxs[i]; wzs[i] = hzs[i]
        payload["hx"] = hxs; payload["hz"] = hzs
        payload["wx"] = wxs; payload["wz"] = wzs

    if include_path and include_home_work:
        # Street-snapped commute paths — 5 waypoints per agent that bend
        # the home→work line through nearest road points so the agent
        # cloud looks like people walking on streets in agents-layer.js.
        # The RoadGrid is cached per city; per-pair LRU avoids recomputing
        # for agents that share the same home + workplace.
        roads = getattr(eng, "_roads", None) or []
        if roads:
            grid = abm_paths.get_or_build_grid(_active_city_id or "", roads)
            home_b_list = home_b.tolist()
            work_b_list = wb.tolist()
            # Parallel arrays: shape (n, 5) flattened, since every agent
            # has the same number of waypoints by construction.
            px = [[0.0] * 5 for _ in range(n)]
            pz = [[0.0] * 5 for _ in range(n)]
            for i in range(n):
                hi = int(home_b_list[i])
                wi_ = int(work_b_list[i])
                # No workplace → no commute → degenerate path that pins
                # to home, so frontend interpolation collapses to a point.
                if wi_ < 0:
                    px[i] = [hxs[i]] * 5
                    pz[i] = [hzs[i]] * 5
                    continue
                path = grid.commute_path(
                    home_idx=hi, work_idx=wi_,
                    home_xz=(hxs[i], hzs[i]),
                    work_xz=(wxs[i], wzs[i]),
                )
                # Always pad / truncate to exactly 5 waypoints so the
                # frontend can lerp with a fixed-stride layout.
                if len(path) < 5:
                    last = path[-1]
                    path = list(path) + [last] * (5 - len(path))
                elif len(path) > 5:
                    # Down-sample evenly — preserves both endpoints.
                    path = [path[round(j * (len(path) - 1) / 4)] for j in range(5)]
                for j, (vx, vz) in enumerate(path):
                    px[i][j] = float(vx)
                    pz[i][j] = float(vz)
            payload["path_x"] = px
            payload["path_z"] = pz
            payload["path_len"] = 5

    return payload


@app.get("/api/abm_edges")
def get_abm_edges(day_from: int = 0, limit: int = 5000):
    """
    Real per-agent transmission edges recorded by the ABM engine.
    Each edge: {susceptible, infector, location, day, hour}.
    Returns [] under the compartmental engine (no per-agent tracking).

    `day_from` filters to edges on or after that day; `limit` caps the
    response so the JSON payload stays small even after a long run.
    The infection-flow visualization (static/infection-flow.js) will
    consume this in Phase 3 to render true transmission chains instead
    of the current spatial-nearest-neighbour approximation.
    """
    eng = _active()
    edges_fn = getattr(eng, "edges_since", None)
    if edges_fn is None:
        return {"engine": _engine_config["engine"], "edges": [], "truncated": False}
    edges = edges_fn(day_from)
    truncated = len(edges) > limit
    return {
        "engine":    _engine_config["engine"],
        "edges":     edges[-limit:] if truncated else edges,
        "truncated": truncated,
    }


@app.get("/api/metapop/state")
def get_metapop_state():
    """
    Read-only snapshot of every coarse SEIR node in the metapopulation
    layer. Shape: ``{day, focused, nodes: {kommunkod: {...}}}``. Empty
    ``nodes`` dict means the nationwide SCB data has not been fetched yet
    (run ``python tools/download_city.py --national``).

    This endpoint is intentionally cheap: it does not step anything, does
    not require an active city, and returns < 20 KB for all 290 kommuner.
    """
    metapop.ensure_loaded()
    return metapop.snapshot()


@app.post("/api/metapop/reset")
def post_metapop_reset():
    """Reset every coarse node to fully susceptible. Does not touch the focused engine."""
    metapop.reset_all()
    # Re-sync the focused city so its shadow node stays accurate.
    if _active_city_id is not None and _active_city_id in engines:
        _sync_focused_into_metapop(engines[_active_city_id])
    return metapop.snapshot()


@app.post("/api/step")
def post_step(n: int = 1, flow_hour: float = 9.0):
    eng = _active()
    # Metapop coupling: compute today's external FOI from the coarse layer
    # *before* stepping the focused engine. When the metapop layer is
    # disabled (no nationwide data) this returns 0.0 and behaviour is
    # identical to single-city mode.
    ext_foi = 0.0
    kommun = _focused_kommun()
    if kommun and metapop.is_available():
        ext_foi = metapop.external_foi_for(kommun)

    eng.step(max(1, min(n, 60)), flow_hour=flow_hour, external_foi=ext_foi)

    # Push focused totals back into the shadow coarse node, then advance
    # every other coarse node by one day.
    if metapop.is_available():
        _sync_focused_into_metapop(eng)
        metapop.step_coarse_nodes()

    return eng.state()


@app.post("/api/reset")
def post_reset():
    """
    Hard reset of the **active** city only. Other warm engines are
    untouched. Mirrors the "fresh defaults" the user expects from the
    Reset button (clear manual patient zero, force Random PZ on).
    """
    eng = _active()
    eng.set_manual_patient_zero(None)
    eng.set_simulation_mode(random_patient_zero=True)
    eng.reset()
    _layout_caches.pop(_active_city_id, None)  # buildings don't change but be safe
    return eng.state()


@app.post("/api/intervention/{iv_id}")
def post_intervention(iv_id: str):
    if iv_id not in VALID_INTERVENTION_IDS:
        raise HTTPException(404, f"Unknown intervention: {iv_id!r}")
    eng = _active()
    active = eng.toggle_intervention(iv_id)
    return {
        "id":                   iv_id,
        "active":               active,
        "active_interventions": eng.active_interventions,
    }


@app.get("/api/params")
def get_params():
    return _active().state()["params"]


@app.post("/api/params")
def post_params(body: ParamsBody):
    """
    Update SEIR parameters. Writes through to `_global_params` so the
    change persists when the user switches to a different city.
    """
    if _is_game_mode():
        raise HTTPException(403, "Parameters are read-only in game mode.")
    eng = _active()
    eng.update_params(
        beta=body.beta, sigma=body.sigma,
        gamma=body.gamma, mu=body.mu, waning_days=body.waning_days,
    )
    # Write-through: only update keys that were actually provided.
    for key, value in (
        ("beta", body.beta), ("sigma", body.sigma), ("gamma", body.gamma),
        ("mu", body.mu),     ("waning_days", body.waning_days),
    ):
        if value is not None:
            _global_params[key] = value
    return eng.state()["params"]


@app.post("/api/simulation_mode")
def post_simulation_mode(body: SimModeBody):
    """
    Configure deterministic vs stochastic dynamics, the patient-zero
    policy, and the RNG seed. `stochastic` and `seed` are global (write
    through to `_global_params`); `random_patient_zero` is per-city
    (each engine remembers its own placement preference).
    """
    if _is_game_mode():
        raise HTTPException(403, "Simulation mode is locked in game mode.")
    eng = _active()
    eng.set_simulation_mode(
        stochastic=body.stochastic,
        random_patient_zero=body.random_patient_zero,
        seed=body.seed,
    )
    eng.reset()
    _layout_caches.pop(_active_city_id, None)
    if body.stochastic is not None:
        _global_params["stochastic"] = body.stochastic
    if body.seed is not None:
        _global_params["seed"] = body.seed
    return eng.state()


@app.post("/api/manual_patient_zero")
def post_manual_patient_zero(body: ManualPZBody):
    """
    Pin (or clear) the patient-zero building for the active city only.
    Triggers a reset so the new seed location is reflected in the
    returned state immediately.
    """
    if _is_game_mode():
        raise HTTPException(403, "Patient-zero pin is disabled in game mode.")
    eng = _active()
    eng.set_manual_patient_zero(body.idx)
    eng.reset()
    _layout_caches.pop(_active_city_id, None)
    return eng.state()


@app.post("/api/restore")
def post_restore(body: RestoreBody):
    eng = _active()
    eng.restore(body.day, body.cells, body.history, body.active_interventions)
    return eng.state()


# ── Pathogen presets & references ────────────────────────────────────────────

@app.get("/api/about")
def get_about():
    """References + pathogen presets + origin-group metadata for the About modal."""
    return {
        "references":     get_references(),
        "presets":        get_presets(),
        "origin_groups":  demography.ORIGIN_GROUPS,
    }


@app.get("/api/presets")
def list_presets():
    """List pathogen scenario presets."""
    return get_presets()


@app.post("/api/preset/{pid}")
def apply_preset(pid: str):
    """Apply a scenario preset's parameters to the active engine + globals."""
    if _is_game_mode():
        raise HTTPException(403, "Pathogen presets are locked in game mode.")
    if pid not in VALID_PRESET_IDS:
        raise HTTPException(status_code=404, detail=f"Unknown preset: {pid!r}")
    preset = get_preset(pid)
    pp = preset["params"]
    eng = _active()
    eng.update_params(
        beta=pp.get("beta"), sigma=pp.get("sigma"),
        gamma=pp.get("gamma"), mu=pp.get("mu"),
        waning_days=pp.get("waning_days"),
    )
    # Write through to globals so the preset persists across city switches.
    for key in ("beta", "sigma", "gamma", "mu", "waning_days"):
        if key in pp:
            _global_params[key] = pp[key]
    return {"applied": pid, "params": eng.state()["params"], "preset": preset}


def _neighborhood_polygon(name: str) -> list | None:
    """
    Return the full geometry of the RegSO neighborhood as a list of rings
    in [lon, lat] pairs. Single-polygon neighborhoods return one ring;
    multi-polygon neighborhoods (most RegSO groupings cover more than
    one DeSO area) return one ring per piece, so the red dashed border
    traces every piece — not just the first one.
    Returns None if the neighborhood data hasn't been loaded yet or the
    name doesn't match.
    """
    if _active_city_id is None:
        return None
    cache = _neighborhood_caches.get(_active_city_id)
    if not cache:
        try:
            cache = get_neighborhoods()
        except HTTPException:
            return None
    rings: list = []
    for entry in cache or []:
        if entry.get("name") != name:
            continue
        gtype = entry.get("gtype")
        poly  = entry.get("polygon")
        if gtype == "MultiPolygon" and poly:
            # `poly` is a list of polygons; each polygon is a list of
            # rings (outer + holes). Pick the outer ring of each piece.
            for piece in poly:
                if piece:
                    rings.append(piece[0] if isinstance(piece[0][0], list) else piece)
        elif gtype == "Polygon" and poly:
            # `poly` is the outer ring (list of [lon,lat] pairs).
            rings.append(poly)
    return rings or None


# ── Spatial interventions ───────────────────────────────────────────────────


class SpatialBody(BaseModel):
    type:          str            # "quarantine_zone" | "targeted_vaccine" | "mobile_clinic" | "surge_testing"
    # One of these selection inputs is required (the right one for the
    # intervention type — frontend picks). The endpoint resolves to a list
    # of building indices on the engine side.
    neighborhood:  str | None     = None    # RegSO name — for quarantine_zone / targeted_vaccine
    polygon:       list[list[float]] | None = None   # for lasso quarantine; [[lon, lat], ...]
    center_lonlat: list[float] | None = None         # for mobile_clinic / radius selection
    radius_m:      float | None       = None         # for mobile_clinic
    building_idx:  int | None         = None         # for surge_testing
    duration_days: int | None         = None         # explicit override


@app.get("/api/spatial")
def get_spatial():
    """Active spatial interventions for the current city."""
    eng = _active()
    return {
        "items":     eng.spatial.snapshot(eng.state()["day"]),
        "tools":     sorted(SPATIAL_VALID_TYPES),
        "costs":     SPATIAL_PER_DAY_COSTS,
    }


@app.post("/api/spatial")
def post_spatial(body: SpatialBody):
    """
    Create a spatial intervention. The required selection input depends
    on `type`:

        quarantine_zone     → deso=… OR polygon=[[lon,lat], …]
        targeted_vaccine    → deso=…
        mobile_clinic       → center_lonlat=[lon,lat], radius_m=… (≤ 2000)
        surge_testing       → building_idx=…

    Returns the created item snapshot.
    """
    if body.type not in SPATIAL_VALID_TYPES:
        raise HTTPException(400, f"Unknown spatial type: {body.type!r}")
    eng = _active()
    today = eng.state()["day"]

    targets: list[int]
    label: str
    label_key: str = ""
    label_vars: dict = {}
    duration = body.duration_days if body.duration_days is not None else -1
    polygon: list | None = None
    center: list | None = None

    if body.type == "quarantine_zone":
        if body.polygon and len(body.polygon) >= 3:
            targets = eng.select_buildings_in_polygon(body.polygon)
            label   = f"Lasso zone · {len(targets)} buildings"
            label_key  = "sp_lbl_lasso"
            label_vars = {"n": len(targets)}
            polygon = [list(body.polygon)]
        elif body.neighborhood:
            targets = eng.select_buildings_in_neighborhood(body.neighborhood)
            label   = f"Quarantine · {body.neighborhood}"
            label_key  = "sp_lbl_qua_nbh"
            label_vars = {"name": body.neighborhood}
            polygon = _neighborhood_polygon(body.neighborhood)
        else:
            raise HTTPException(400, "quarantine_zone needs polygon or neighborhood")

    elif body.type == "targeted_vaccine":
        if not body.neighborhood:
            raise HTTPException(400, "targeted_vaccine needs neighborhood")
        targets = eng.select_buildings_in_neighborhood(body.neighborhood)
        label   = f"Vaccine drive · {body.neighborhood}"
        label_key  = "sp_lbl_vac_nbh"
        label_vars = {"name": body.neighborhood}
        polygon = _neighborhood_polygon(body.neighborhood)

    elif body.type == "mobile_clinic":
        if not body.center_lonlat or len(body.center_lonlat) != 2:
            raise HTTPException(400, "mobile_clinic needs center_lonlat")
        radius = float(body.radius_m or 500.0)
        radius = max(100.0, min(2000.0, radius))
        targets = eng.select_buildings_in_radius(
            body.center_lonlat[0], body.center_lonlat[1], radius)
        label   = f"Mobile clinic · {int(radius)} m"
        label_key  = "sp_lbl_clinic"
        label_vars = {"radius": int(radius)}
        center  = [float(body.center_lonlat[0]), float(body.center_lonlat[1])]
        if duration < 0:
            duration = 21    # default 3-week deployment

    elif body.type == "surge_testing":
        if body.building_idx is not None:
            idx = int(body.building_idx)
        elif body.center_lonlat and len(body.center_lonlat) == 2:
            idx = eng.nearest_building(body.center_lonlat[0], body.center_lonlat[1])
        else:
            raise HTTPException(400, "surge_testing needs building_idx or center_lonlat")
        if idx < 0:
            raise HTTPException(400, "no building found near that point")
        targets = eng.select_building_and_neighbours(idx, n=8)
        label   = f"Surge testing · cluster of {len(targets)}"
        label_key  = "sp_lbl_surge"
        label_vars = {"n": len(targets)}
        # Keep the click point so the 🔬 map marker stays anchored — the
        # frontend optimistic insert had a center, but the canonical
        # item used to land without one and the marker would blink out.
        if body.center_lonlat and len(body.center_lonlat) == 2:
            center = [float(body.center_lonlat[0]), float(body.center_lonlat[1])]
        if duration < 0:
            duration = 30
    else:
        raise HTTPException(400, f"Unsupported type: {body.type!r}")

    if not targets:
        raise HTTPException(400, "Selection produced no buildings")

    pop = eng.population_of(targets)
    item = eng.spatial.add(
        type=body.type,
        target_buildings=targets,
        label=label,
        label_key=label_key,
        label_vars=label_vars,
        population=pop,
        created_day=today,
        duration_days=duration,
        polygon=polygon,
        center_lonlat=center,
    )
    # In game mode, charge the trust on creation immediately so the user
    # feels the cost.
    if _is_game_mode() and eng._game_state is not None:
        cost = SPATIAL_PER_DAY_COSTS.get(body.type, {})
        eng._game_state.trust = max(0.0, min(100.0,
            eng._game_state.trust + cost.get("trust_on_create", 0.0)))

    return item.snapshot(today)


@app.delete("/api/spatial/{item_id}")
def delete_spatial(item_id: str):
    """Remove an active spatial intervention by id."""
    eng = _active()
    if not eng.spatial.remove(item_id):
        raise HTTPException(404, f"Unknown spatial intervention: {item_id!r}")
    return {"removed": item_id}


# ── Game-mode endpoints ──────────────────────────────────────────────────────


class GameModeBody(BaseModel):
    mode:       str | None = None    # "sandbox" or "game"
    difficulty: str | None = None    # "easy" | "normal" | "hard"


class EngineModeBody(BaseModel):
    engine:            str   | None = None  # "compartmental" or "abm"
    population_scale:  int   | None = None  # one of engine_abm.VALID_POPULATION_SCALES
    time_resolution_h: float | None = None  # one of engine_abm.VALID_TIME_RESOLUTIONS


class StartSeasonBody(BaseModel):
    season: str | None = None     # "random" | "winter" | "spring" | "summer" | "autumn"


@app.get("/api/start_season")
def get_start_season():
    """Read the current pinned starting season (or 'random')."""
    return {
        "season":  _global_params.get("start_season") or "random",
        "options": sorted(VALID_SEASONS),
    }


@app.post("/api/start_season")
def post_start_season(body: StartSeasonBody):
    """
    Pin (or randomize) the season at which day 0 begins. Applied to the
    active engine immediately (with a reset) and stored in `_global_params`
    so every subsequent city entry uses it.
    """
    s = (body.season or "random").lower()
    if s not in VALID_SEASONS:
        raise HTTPException(400, f"Invalid season: {body.season!r}")
    _global_params["start_season"] = None if s == "random" else s
    if _active_city_id is not None and _active_city_id in engines:
        eng = engines[_active_city_id]
        eng.set_start_season(_global_params["start_season"])
        eng.reset()
        _layout_caches.pop(_active_city_id, None)
    return get_start_season()


@app.get("/api/game_mode")
def get_game_mode():
    """
    Read the global mode + difficulty selection. Also returns the
    difficulty profile metadata so the macro view can show the user
    what each level changes (β, IFR, starting budget, etc.).
    """
    return {
        "mode":          _game_config["mode"],
        "difficulty":    _game_config["difficulty"],
        "duration_days": GAME_DURATION_DAYS,
        "difficulties":  DIFFICULTY_PROFILES,
    }


@app.post("/api/game_mode")
def post_game_mode(body: GameModeBody):
    """
    Set the global mode + difficulty. The selection is applied to the
    active city immediately (with a reset) and to every other city the
    next time it is entered. The macro picker calls this *before*
    selecting a city — typical flow.
    """
    if body.mode is not None:
        if body.mode not in ("sandbox", "game"):
            raise HTTPException(400, f"Invalid mode: {body.mode!r}")
        _game_config["mode"] = body.mode
    if body.difficulty is not None:
        if body.difficulty not in VALID_DIFFICULTIES:
            raise HTTPException(400, f"Invalid difficulty: {body.difficulty!r}")
        _game_config["difficulty"] = body.difficulty

    # Re-apply to the active engine if any. New cities pick this up via
    # _apply_game_to_engine() on first creation.
    if _active_city_id is not None and _active_city_id in engines:
        eng = engines[_active_city_id]
        _apply_game_to_engine(eng)
        eng.reset()
        _layout_caches.pop(_active_city_id, None)

    return get_game_mode()


@app.get("/api/engine_mode")
def get_engine_mode():
    """
    Read the global engine selection and the allowed values for the
    ABM-specific knobs (population_scale, time_resolution_h). The macro
    picker uses this to render the UI without hard-coding the lists.
    """
    return {
        "engine":                  _engine_config["engine"],
        "population_scale":        _engine_config["population_scale"],
        "time_resolution_h":       _engine_config["time_resolution_h"],
        "valid_engines":           sorted(VALID_ENGINE_KINDS),
        "valid_population_scales": list(_VALID_POP_SCALES),
        "valid_time_resolutions":  list(_VALID_TIME_RES),
    }


@app.post("/api/engine_mode")
def post_engine_mode(body: EngineModeBody):
    """
    Update the global engine selection. Switching engine kinds wipes the
    warm engine pool and the active-city pointer (different class), so
    the user is dropped back on the macro view to re-pick a city.
    Adjusting scale/resolution while staying on the same engine leaves
    warm engines untouched; the new values apply on the next
    /api/select_city call.
    """
    global _active_city_id
    kind_changed = False

    if body.engine is not None:
        if body.engine not in VALID_ENGINE_KINDS:
            raise HTTPException(400, f"Invalid engine: {body.engine!r}")
        if body.engine != _engine_config["engine"]:
            kind_changed = True
            _engine_config["engine"] = body.engine

    if body.population_scale is not None:
        if body.population_scale not in _VALID_POP_SCALES:
            raise HTTPException(
                400,
                f"Invalid population_scale: {body.population_scale!r} "
                f"(allowed: {list(_VALID_POP_SCALES)})",
            )
        _engine_config["population_scale"] = body.population_scale

    if body.time_resolution_h is not None:
        if body.time_resolution_h not in _VALID_TIME_RES:
            raise HTTPException(
                400,
                f"Invalid time_resolution_h: {body.time_resolution_h!r} "
                f"(allowed: {list(_VALID_TIME_RES)})",
            )
        _engine_config["time_resolution_h"] = body.time_resolution_h

    if kind_changed:
        # Different engine class → existing warm engines are no longer
        # valid. Drop the pool and force the user back through
        # /api/select_city, which rebuilds via _new_engine_for().
        engines.clear()
        _active_city_id = None
        metapop.set_focused(None)

    return get_engine_mode()


@app.get("/api/game_state")
def get_game_state():
    """
    HUD payload for the resource bars. Returns ``null`` in sandbox mode
    so the frontend can hide the HUD without a special case.
    """
    if not _is_game_mode():
        return None
    eng = _active()
    return eng.state().get("game")


# Static files — must be mounted LAST so /api/* routes take priority
app.mount("/", StaticFiles(directory="static", html=True), name="static")
