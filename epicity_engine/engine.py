"""
engine.py — SEIR epidemic engine on real-world OSM building data.

No I/O, no FastAPI dependencies.
City geometry comes from osm.py; neighbours are precomputed at load time.
"""
from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import osm
import climate as climate_mod
import demography
from presets import default_preset


class Zone(IntEnum):
    ROAD       = 0
    RES_LO     = 1
    RES_MED    = 2
    RES_HI     = 3
    COMMERCIAL = 4
    INDUSTRIAL = 5
    HOSPITAL   = 6
    PARK       = 7
    SCHOOL     = 8
    PHARMACY   = 9
    UNIVERSITY = 10
    GROCERY    = 11
    DENTAL     = 12
    VETERINARY = 13
    CHURCH     = 14
    CASTLE     = 15
    MUSEUM     = 16
    THEATER    = 17
    STADIUM    = 18
    WATER      = 19
    FORESTRY   = 20
    NIGHTCLUB  = 21
    PLAYGROUND = 22
    CEMETERY   = 23
    KINDERGARTEN = 24   # Förskola — ages 1-5
    HIGH_SCHOOL  = 25   # Gymnasium — ages 16-19 (SCHOOL=8 is grundskola)
    LIBRARY      = 26
    RESTAURANT        = 27
    SPORTS_CENTRE     = 28
    HOTEL             = 29
    COMMUNITY_CENTRE  = 30
    MALL              = 31
    PARKING           = 32
    MANOR             = 33
    HISTORIC_LANDMARK = 34
    POLICE            = 35
    FIRE_STATION      = 36


# ── Age stratification ───────────────────────────────────────────────────────

AGE_GROUPS: tuple[str, str, str] = ("child", "adult", "elder")

# Who-meets-whom contact matrix (rows = susceptible group, cols = infectious).
# Values roughly follow POLYMOD: children over-mix with children (school),
# adults mostly with adults, elders mostly with adults/other elders.
# Adult↔adult is normalised to 1.0 so base β stays comparable with the
# pre-stratified calibration.
_CONTACT_M: dict[str, dict[str, float]] = {
    "child": {"child": 1.6, "adult": 0.9, "elder": 0.3},
    "adult": {"child": 0.5, "adult": 1.0, "elder": 0.4},
    "elder": {"child": 0.2, "adult": 0.5, "elder": 0.8},
}

# Age-dependent case fatality multiplier (applied on top of base mu).
_MU_MULT: dict[str, float] = {"child": 0.1, "adult": 1.0, "elder": 6.0}

# Fallback age mix if a building was built before the SCB attach pass.
_DEFAULT_AGE_FRAC: dict[str, float] = {
    "child": 0.18, "adult": 0.62, "elder": 0.20,
}


# ── Indoor / outdoor ventilation factor ──────────────────────────────────────
# Empirical evidence from SARS-CoV-2, influenza, and other respiratory
# pathogens consistently shows ~5–20× higher per-contact transmission
# in enclosed indoor spaces than outdoors (Bulfone et al. 2021;
# Nishiura et al. 2020 — Tokyo cluster review found ~19× indoor risk).
# We split the per-zone β multiplier into TWO factors so the
# indoor/outdoor distinction is explicit and tunable:
#
#   _BETA_MULT[z] = _CONTACT_DENSITY[z] × _INDOOR_FACTOR[z]
#
# _CONTACT_DENSITY captures how densely people pack and mix at a zone
# type (a stadium is denser than a corner shop, a kindergarten denser
# than a library). _INDOOR_FACTOR captures the ventilation discount:
# 1.0 for enclosed buildings (offices, shops, schools, homes),
# ~0.35 for semi-open venues (stadia, churches with large doors),
# ~0.10 for fully outdoor zones (parks, cemeteries, playgrounds,
# parking lots, forests, open water). The split lets future
# interventions (e.g. open-window mandates, outdoor classes) tweak
# the indoor factor without re-tuning every zone's contact density.
_INDOOR_FACTOR: dict[Zone, float] = {
    # Fully outdoor — ventilation dominates, very low per-contact risk.
    Zone.PARK:       0.10,
    Zone.FORESTRY:   0.10,
    Zone.WATER:      0.10,
    Zone.CEMETERY:   0.10,
    Zone.PLAYGROUND: 0.10,
    Zone.PARKING:    0.10,
    # Semi-outdoor / partially covered.
    Zone.STADIUM:    0.40,   # most Swedish stadia are open-air or open-sided
}

def indoor_factor(zone: Zone) -> float:
    """Indoor/outdoor ventilation multiplier for a zone — 1.0 means
    fully enclosed (no discount), 0.10 means open-air (~10× lower
    per-contact transmission). See _INDOOR_FACTOR for the table."""
    return _INDOOR_FACTOR.get(zone, 1.0)


# Underlying "how densely do people mix here" multiplier — the
# pre-ventilation contact density. _BETA_MULT below is derived from
# this × the indoor/outdoor factor so we can never silently regress
# the implicit split. Outdoor zones look comparatively high here
# (PLAYGROUND = 20, CEMETERY = 9) because once you cancel the
# outdoor discount the underlying mixing IS intense (kids cluster,
# funerals gather around the grave). The outdoor factor then knocks
# the realised transmission back down.
_CONTACT_DENSITY: dict[Zone, float] = {
    Zone.ROAD: 0.0, Zone.RES_LO: 0.9, Zone.RES_MED: 1.1, Zone.RES_HI: 1.4,
    Zone.COMMERCIAL: 2.4, Zone.INDUSTRIAL: 1.3, Zone.HOSPITAL: 0.5,
    Zone.PARK: 3.0, Zone.SCHOOL: 1.8,
    Zone.PHARMACY: 1.5, Zone.UNIVERSITY: 2.0, Zone.GROCERY: 2.0,
    Zone.DENTAL: 0.8, Zone.VETERINARY: 0.6,
    Zone.CHURCH: 2.2,
    Zone.CASTLE: 1.8,
    Zone.MUSEUM:  1.7,
    Zone.THEATER: 2.8,    # dense audience, long sessions → high mixing
    Zone.STADIUM: 8.0,    # biggest gatherings in the simulation (× 0.4 outdoor = 3.2)
    Zone.WATER:    2.0,   # weak — recreational only, rarely populated
    Zone.FORESTRY: 1.5,   # very dispersed
    Zone.NIGHTCLUB: 3.0,  # densest enclosed crowd in the simulation
    Zone.PLAYGROUND: 20.0,# kids mix tightly — but outdoor factor cuts to 2.0
    Zone.CEMETERY:  9.0,  # gathered around the grave — outdoor cuts to 0.9
    # Swedish school tiers — SCHOOL (grundskola) stays at 1.8 above.
    Zone.KINDERGARTEN: 2.2, # förskola: toddlers mix tightly indoors
    Zone.HIGH_SCHOOL:  1.6, # gymnasium: adolescents indoors
    Zone.LIBRARY:      1.3, # quiet but dense reading rooms, long dwell time
    # Hospitality / commerce / recreation corrections
    Zone.RESTAURANT:        2.2, # indoor dining, long dwell, close contact
    Zone.SPORTS_CENTRE:     2.0, # aerosol-heavy gyms + pools + ice rinks
    Zone.HOTEL:             1.2, # overnight guests, spread out
    Zone.COMMUNITY_CENTRE:  1.7, # indoor group events
    Zone.MALL:              1.9, # huge enclosed foot traffic
    Zone.PARKING:           5.0, # transient — outdoor factor cuts to 0.5
    Zone.MANOR:             0.7, # tourist visits to estate houses
    Zone.HISTORIC_LANDMARK: 0.8, # museum-like throughput at tourist sites
    Zone.POLICE:            0.6, # office with public counter
    Zone.FIRE_STATION:      0.4, # closed crew environment
}

# Effective per-zone β multiplier the rest of the engine consumes.
# Computed once at import time from contact density × indoor factor
# so any future tweak to either side flows through automatically.
_BETA_MULT: dict[Zone, float] = {
    z: _CONTACT_DENSITY.get(z, 0.0) * indoor_factor(z) for z in Zone
}

_ATTRACT: dict[Zone, float] = {
    Zone.ROAD: 0.0, Zone.RES_LO: 0.5, Zone.RES_MED: 0.7, Zone.RES_HI: 0.8,
    Zone.COMMERCIAL: 3.0, Zone.INDUSTRIAL: 1.0, Zone.HOSPITAL: 0.3,
    Zone.PARK: 1.2, Zone.SCHOOL: 1.5,
    Zone.PHARMACY: 1.5, Zone.UNIVERSITY: 2.5, Zone.GROCERY: 2.5,
    Zone.DENTAL: 0.8, Zone.VETERINARY: 0.5,
    Zone.CHURCH: 1.0,
    Zone.CASTLE: 1.2,
    Zone.MUSEUM:  1.4,
    Zone.THEATER: 1.8,
    Zone.STADIUM: 2.2,
    Zone.WATER:    0.6,  # lakes — modest recreational draw
    Zone.FORESTRY: 0.4,  # forest — modest hiking / forestry worker draw
    Zone.NIGHTCLUB:  2.5, # strong evening draw from adults
    Zone.PLAYGROUND: 2.2, # strong draw for child cohort
    Zone.CEMETERY:   0.9, # modest memorial visits + funerals
    Zone.KINDERGARTEN: 1.6, # small catchment — families walk their kids
    Zone.HIGH_SCHOOL:  1.8, # teens commute farther than kindergarteners
    Zone.LIBRARY:      1.7, # modest draw, all age groups
    # Hospitality / commerce / recreation corrections
    Zone.RESTAURANT:        2.5, # big evening draw
    Zone.SPORTS_CENTRE:     1.8, # regular members + drop-ins
    Zone.HOTEL:             1.5, # tourist + business traveller inflow
    Zone.COMMUNITY_CENTRE:  1.5, # modest local draw
    Zone.MALL:              2.8, # strongest shopping destination
    Zone.PARKING:           0.5, # weak attractor — commuter hub only
    Zone.MANOR:             0.8, # tourist visits
    Zone.HISTORIC_LANDMARK: 1.0, # tourist draw, smaller than museums
    Zone.POLICE:            0.3, # low public draw
    Zone.FIRE_STATION:      0.2, # emergency crew only
}

# ── Synthetic-capacity table for non-residential patient-zero picks ───────
# When the user pinpoints an outbreak in a school / hospital / mall the
# engine still needs a non-zero S compartment to flip into E. We give it
# a per-zone default occupancy clamped by a sqrt of the building footprint
# so a tiny pharmacy doesn't get the same crowd as a stadium. Forest /
# water are intentionally excluded — infections in lakes are nonsense.
_PZ_DEFAULT_CAPACITY: dict["Zone", int] = {}

def _build_capacity_table() -> dict["Zone", int]:
    Z = Zone
    return {
        Z.HOSPITAL: 80, Z.PHARMACY: 12, Z.DENTAL: 8, Z.VETERINARY: 6,
        Z.POLICE: 25, Z.FIRE_STATION: 18,
        Z.KINDERGARTEN: 40, Z.SCHOOL: 220, Z.HIGH_SCHOOL: 300,
        Z.UNIVERSITY: 500, Z.LIBRARY: 35,
        Z.GROCERY: 35, Z.PARK: 25, Z.RESTAURANT: 40,
        Z.COMMUNITY_CENTRE: 50, Z.MALL: 180, Z.HOTEL: 80,
        Z.NIGHTCLUB: 70, Z.PLAYGROUND: 18, Z.SPORTS_CENTRE: 90,
        Z.CHURCH: 50, Z.CEMETERY: 8,
        Z.CASTLE: 25, Z.MUSEUM: 60, Z.THEATER: 100, Z.STADIUM: 250,
        Z.MANOR: 12, Z.HISTORIC_LANDMARK: 20,
        Z.PARKING: 10,
        Z.COMMERCIAL: 60, Z.INDUSTRIAL: 40,
    }

def _synthetic_capacity(b) -> int:
    """Per-building capacity for the patient-zero seeding path. Returns 0
    when the cell is one we don't want to host an outbreak in (water,
    forest, road)."""
    global _PZ_DEFAULT_CAPACITY
    if not _PZ_DEFAULT_CAPACITY:
        _PZ_DEFAULT_CAPACITY = _build_capacity_table()
    z = b.zone
    if z in (Zone.ROAD, Zone.WATER, Zone.FORESTRY):
        return 0
    base = _PZ_DEFAULT_CAPACITY.get(z, 30)
    # Mild footprint scaling: a 4000 m² stadium gets more than a 200 m²
    # corner shop. Square root keeps the spread reasonable.
    area = max(20.0, float(getattr(b, "area_m2", 100) or 100))
    scaled = int(round(base + (area ** 0.5) * 1.5))
    return max(10, min(1500, scaled))


INTERVENTIONS_META: list[dict[str, str]] = [
    {"id": "lockdown",    "label": "Lockdown",         "icon": "🔒",
     "desc": "Restrict movement between zones"},
    {"id": "masks",       "label": "Mask Mandate",      "icon": "😷",
     "desc": "Reduce droplet transmission ~50%"},
    {"id": "distancing",  "label": "Social Distancing", "icon": "↔️",
     "desc": "Reduce contacts and movement"},
    {"id": "vaccination", "label": "Vaccination Drive", "icon": "💉",
     "desc": "S → R at 1.5% per day"},
    {"id": "schools",     "label": "Close Schools",     "icon": "🏫",
     "desc": "Schools shut — transmission drops 80%"},
    {"id": "testing",     "label": "Test & Isolate",    "icon": "🔬",
     "desc": "Identify and isolate infectious faster"},
    # ── Transport-specific ──
    {"id": "transit_masks",     "label": "Transit Masks",     "icon": "🚌",
     "desc": "Masks on public transport — halve in-vehicle transmission"},
    {"id": "suspend_flights",   "label": "Suspend Flights",   "icon": "✈️",
     "desc": "Ground all flights — stop airborne intercity spread"},
    {"id": "transit_screening", "label": "Station Screening", "icon": "🌡️",
     "desc": "Screen passengers at stops — catch ~40% infectious"},
    # ── Soft / spatial / surge tools (added with game mode) ──
    {"id": "info_campaign",    "label": "Public Information", "icon": "📣",
     "desc": "Awareness campaign — small β reduction, slow trust gain"},
    {"id": "hospital_surge",   "label": "Hospital Surge",     "icon": "🏥",
     "desc": "Temporary +40% hospital capacity — mitigates overflow deaths"},
    {"id": "targeted_lockdown","label": "Targeted Lockdown",  "icon": "🚧",
     "desc": "Hot-zone restriction — β -35% at lower social/economic cost"},
    {"id": "stop_transport",  "label": "Stop Public Transport","icon": "🛑",
     "desc": "Halt every bus, tram, train, ferry and flight — no in-vehicle spread"},
]

VALID_INTERVENTION_IDS: frozenset[str] = frozenset(
    iv["id"] for iv in INTERVENTIONS_META
)

HOSPITAL_CAPACITY = 0.05   # fraction of city population

# Spatial-kernel parameters (metres)
_NEIGH_RADIUS  = 400.0    # maximum interaction distance
_NEIGH_SIGMA   = 150.0    # Gaussian std-dev (metres)


@dataclass
class Cell:
    idx:     int
    world_x: float
    world_z: float
    lon:     float
    lat:     float
    zone:    Zone
    pop:     int
    levels:  int
    area_m2: float
    polygon: list   # [[world_x, world_z], ...] — no closing node
    deso:    str | None   # SCB DeSO area id (None if outside any polygon)
    # Initial age fractions ({child, adult, elder}) — used by _seed_seir and restore
    age_frac: dict[str, float]
    # One SEIR vector per age group: {"child": .., "adult": .., "elder": ..}
    S: dict[str, float]
    E: dict[str, float]
    I: dict[str, float]
    R: dict[str, float]
    D: dict[str, float]
    # Manual-override metadata (populated from data/<city>/overrides.json via
    # osm._apply_overrides). None for the vast majority of buildings — only
    # set for landmarks (e.g. Växjö Cathedral). Preserved through city_layout()
    # so the frontend can swap in a custom 3D asset and show a proper label.
    osm_id:         int | None = None
    name:           str | None = None
    custom_builder: str | None = None
    # Natural-feature metadata (only set for FORESTRY cells). `tree_species`
    # is one of {"spruce","pine","birch","oak","mixed"} and selects which
    # species-specific 3D asset the frontend scatters across the polygon.
    # `tree_count` is the estimated stem count derived from area + species
    # spacing in osm._forestry_tree_count().
    tree_species:   str | None = None
    tree_count:     int | None = None
    water_type:     str | None = None
    # Interior hole rings — only set for cemetery polygons that were
    # clipped around enclosed buildings (churches, chapels) by
    # osm._clip_cemetery_polygons. Each hole is a flat list of
    # [[x, z], ...] vertices in world metres, same coordinate system
    # as `polygon`. Frontend (view.js _makeExtrudeGeo) pushes each one
    # into THREE.Shape.holes so the extruded mesh has physical cut-
    # outs around the buildings inside, leaving them visible and
    # clickable regardless of how much the cemetery height grows
    # under colour/height modes.
    polygon_holes:  list | None = None
    # Mean net income (SEK thousands) of the DeSO this building sits
    # in. Populated by osm._attach_demographics via scb.load_deso_income.
    # None when SCB returned no data for the DeSO. Drives the "Income"
    # color + height viz modes.
    income:         float | None = None
    # Human-readable neighborhood name (from SCB RegSO lookup).
    neighborhood:   str | None = None
    # Gender distribution (male/female fractions) per building.
    gender_frac:    dict | None = None
    # Absolute male/female counts per building (synthetic).
    gender:         dict | None = None
    # Average household size for this building (synthetic, zone-biased).
    household_avg_size: float | None = None
    # Estimated number of households in this building (synthetic).
    households:     int | None = None
    # Household size breakdown for this building (synthetic).
    household_sizes: dict | None = None
    # Country-of-birth distribution (6 groups: SE, Nordic, EU, MENA, SSA,
    # Asia/Other) — sums to 1.0. Derived per-building from zone defaults
    # rescaled to the city-wide foreign_frac (SCB BE0101E1).
    origin:            list[float] | None = None

    # ── Aggregate accessors (keep older call sites working) ───────────────────
    @property
    def S_tot(self) -> float: return self.S["child"] + self.S["adult"] + self.S["elder"]
    @property
    def E_tot(self) -> float: return self.E["child"] + self.E["adult"] + self.E["elder"]
    @property
    def I_tot(self) -> float: return self.I["child"] + self.I["adult"] + self.I["elder"]
    @property
    def R_tot(self) -> float: return self.R["child"] + self.R["adult"] + self.R["elder"]
    @property
    def D_tot(self) -> float: return self.D["child"] + self.D["adult"] + self.D["elder"]

    @property
    def N(self) -> float:
        return self.S_tot + self.E_tot + self.I_tot + self.R_tot


@dataclass
class SEIRParams:
    beta:              float = 0.55
    sigma:             float = 0.25
    gamma:             float = 1.0 / 14
    mu:                float = 0.015
    mu_overloaded:     float = 0.04
    hospital_capacity: float = HOSPITAL_CAPACITY
    local_frac:        float = 0.55
    school_factor:     float = 1.0
    waning_rate:       float = 1.0 / 13


class EpidemicEngine:
    """Manages OSM city buildings and SEIR state.  Not thread-safe."""

    def __init__(self, city_id: str = "vaxjo") -> None:
        self._city_id:   str              = city_id
        self._buildings: list[Cell]       = []
        self._roads:     list[dict]       = []
        self._border:    list             = []   # municipality border rings
        # _neighbors[i] = list of (j, weight) for building j within radius
        self._neighbors: list[list[tuple[int, float]]] = []
        self._active:    set[str]         = set()
        self._history:   deque            = deque(maxlen=1000)
        self._day:       int              = 0
        self._overloaded: bool            = False
        self._base:      SEIRParams       = SEIRParams()
        # Transport dispatcher: optional, attached by main.py after city
        # selection if a transport_schedule.json exists for the city. The
        # dispatcher mass-transfers passengers between buildings (intra-city)
        # or metapop coarse nodes (inter-city) once per sub-step.
        # ``_last_minute`` tracks the most recent minute-of-day the
        # dispatcher processed, so we can iterate the full minute range
        # covered by each sub-step instead of dropping ~95% of trips when
        # n=24 (full day in one /api/step call).
        self._dispatcher: Any | None      = None
        self._last_minute: int            = 0
        # Climate snapshot (set on every step) — exposed in state().
        self._last_hour:    float         = 9.0
        # Day-of-year offset added to the day counter when feeding the climate
        # model. Picked randomly on each reset so simulations don't always
        # start in early January (winter). 0..364.
        self._day_of_year_offset: int     = random.randint(0, 364)
        # Peak prevalence tracking
        self._peak_I_frac:  float         = 0.0
        self._peak_I_day:   int           = 0
        # Initial total population (frozen at seed) — for cumulative attack rate
        self._N0:           float         = 0.0
        # City-wide foreign-born fraction (set in _load_city from SCB).
        self._foreign_frac: float | None  = None
        # Simulation mode — set via set_simulation_mode() before reset()
        # Defaults: stochastic dynamics + randomised patient zero so each
        # fresh run explores a different outbreak trajectory.
        self._stochastic:          bool  = True
        self._random_patient_zero: bool  = True
        self._rng_seed:            int   = random.randint(0, 1_000_000)
        self._rng: random.Random         = random.Random(self._rng_seed)
        self._patient_zero_indices: list[int] = []
        # When set to a building index, _seed_seir() seeds the outbreak
        # there instead of using random/deterministic logic. None disables it.
        self._manual_patient_zero_idx: int | None = None
        # Pinned starting season — None means "random each reset" (legacy
        # behaviour). When set to "winter"/"spring"/"summer"/"autumn", the
        # day-of-year offset is fixed to that season's midpoint so the
        # outbreak always begins in the chosen month.
        self._start_season: str | None = None
        # Game mode state — None in sandbox. Held by engine so it ticks in
        # lockstep with the SEIR step and survives city revisits.
        self._mode:          str               = "sandbox"   # or "game"
        self._difficulty:    str               = "normal"
        self._game_state:    Any | None        = None
        # Number of buildings to auto-seed in game mode (set by main on entry).
        self._auto_seed_n:   int               = 1
        # Spatial-intervention registry (quarantine zones, mobile clinics,
        # targeted vaccination, surge testing). Empty by default — every
        # query is a fast set lookup so no overhead when nothing's set.
        from spatial import SpatialRegistry as _SR
        self._spatial = _SR()
        self.reset()

    # ── Simulation-mode configuration ─────────────────────────────────────────

    def set_simulation_mode(self, *, stochastic: bool | None = None,
                            random_patient_zero: bool | None = None,
                            seed: int | None = None) -> None:
        """
        Configure stochastic/deterministic behaviour and patient-zero policy.
        Takes effect on the next reset() (the caller is expected to reset
        immediately after so the change is reflected in the seed state).
        """
        if stochastic is not None:
            self._stochastic = bool(stochastic)
        if random_patient_zero is not None:
            self._random_patient_zero = bool(random_patient_zero)
        if seed is not None:
            self._rng_seed = int(seed)
        # Re-seed the RNG so repeat runs with the same seed are reproducible
        self._rng = random.Random(self._rng_seed)

    # Mid-season day-of-year for each pinnable season (Northern hemisphere,
    # tuned for Sweden — outbreaks land in the heart of the season).
    _SEASON_MIDPOINTS = {
        "winter": 15,    # mid-January
        "spring": 105,   # mid-April
        "summer": 197,   # mid-July
        "autumn": 288,   # mid-October
    }

    def _chosen_season_offset(self) -> int:
        """Resolve the current `_start_season` pin to a DoY offset."""
        if self._start_season in self._SEASON_MIDPOINTS:
            return self._SEASON_MIDPOINTS[self._start_season]
        return random.randint(0, 364)

    def set_start_season(self, season: str | None) -> None:
        """
        Pin the day-0 season. Pass None or "random" to revert to the
        random-each-reset behaviour. Caller should reset() afterwards.
        """
        if season in (None, "", "random"):
            self._start_season = None
        elif season in self._SEASON_MIDPOINTS:
            self._start_season = season
        else:
            self._start_season = None

    def set_game_mode(self, mode: str, difficulty: str = "normal",
                      game_state: Any | None = None,
                      auto_seed_n: int = 1) -> None:
        """
        Switch between sandbox/game scoring modes. Pass ``game_state=None``
        to leave scoring off — the SEIR simulation is unchanged. Caller
        is responsible for resetting the engine afterwards if the seed
        policy or base params should take effect.
        """
        self._mode        = "game" if mode == "game" else "sandbox"
        self._difficulty  = difficulty
        self._game_state  = game_state if self._mode == "game" else None
        self._auto_seed_n = max(1, int(auto_seed_n))

    def set_manual_patient_zero(self, idx: int | None) -> None:
        """
        Pin the next reset's outbreak seed to a specific building index, or
        pass None to clear and revert to random/deterministic placement.
        Caller is expected to reset() afterwards so the change takes effect.
        """
        if idx is None:
            self._manual_patient_zero_idx = None
        else:
            i = int(idx)
            if 0 <= i < len(self._buildings):
                self._manual_patient_zero_idx = i
            else:
                self._manual_patient_zero_idx = None

    def _perturb(self, mean: float, cap: float) -> float:
        """
        Langevin/Gaussian noise around a deterministic mean flow.
        Variance scales with the mean (Poisson-like); result is clamped
        to [0, cap] so it remains a valid compartment flow.
        """
        if mean <= 0.0:
            return 0.0
        noisy = mean + self._rng.gauss(0.0, math.sqrt(mean))
        if noisy < 0.0:
            return 0.0
        if noisy > cap:
            return cap
        return noisy

    # ── City loading ──────────────────────────────────────────────────────────

    def _load_city(self) -> None:
        data        = osm.load_city(self._city_id)
        raw_b       = data["buildings"]
        self._roads = data["roads"]
        self._border = data.get("border", [])
        # Lightweight nature POIs (nature reserves, cemeteries, …) — pass
        # through unchanged; they don't participate in SEIR and need no
        # engine-side state.
        self._nature_pois = data.get("nature_pois", [])
        # Swedish flag pole easter egg positions (3D-only, no SEIR).
        self._flag_poles = data.get("flag_poles", [])
        self._easter_eggs = data.get("easter_eggs", [])
        # LNU (Linnaeus University) flag poles — Växjö + Kalmar only.
        self._lnu_flag_poles = data.get("lnu_flag_poles", [])

        self._buildings = []
        for b in raw_b:
            frac = b.get("age_frac") or _DEFAULT_AGE_FRAC
            pop  = float(b["pop"])
            S0   = {g: pop * frac.get(g, 0.0) for g in AGE_GROUPS}
            zero = lambda: {g: 0.0 for g in AGE_GROUPS}
            zone     = Zone(b["zone"])
            levels   = max(1, int(b.get("levels") or 1))
            area_m2  = float(b.get("area_m2") or 0.0)
            self._buildings.append(Cell(
                idx=b["idx"], world_x=b["world_x"], world_z=b["world_z"],
                lon=b["lon"], lat=b["lat"],
                zone=zone, pop=b["pop"],
                levels=levels, area_m2=area_m2,
                polygon=b.get("polygon", []),
                deso=b.get("deso"),
                age_frac={g: frac.get(g, 0.0) for g in AGE_GROUPS},
                S=S0, E=zero(), I=zero(), R=zero(), D=zero(),
                osm_id=b.get("osm_id"),
                name=b.get("name"),
                custom_builder=b.get("custom_builder"),
                tree_species=b.get("tree_species"),
                tree_count=b.get("tree_count"),
                water_type=b.get("water_type"),
                polygon_holes=b.get("polygon_holes"),
                income=b.get("income"),
                neighborhood=b.get("neighborhood"),
                gender_frac=b.get("gender_frac"),
                gender=b.get("gender"),
                household_avg_size=b.get("household_avg_size"),
                households=b.get("households"),
                household_sizes=b.get("household_sizes"),
            ))
        # Enrich buildings with SCB data that may not be in the cached
        # city.json (neighborhood names, gender fractions, household size).
        # This is a lightweight stamp pass — no geometry or population
        # changes, just metadata for the tooltip.
        self._enrich_scb_metadata()

        self._build_neighbors()
        print(f"[engine] {len(self._buildings)} buildings, "
              f"{len(self._roads)} roads loaded for {self._city_id}.")

    def _enrich_scb_metadata(self) -> None:
        """
        Stamp neighborhood names, gender fractions, and household size
        onto buildings using their DeSO code. Works even on older cached
        city.json files that predate these fields — the SCB lookup data
        is fetched/cached independently.
        """
        country = osm._country_of(self._city_id)
        if country != "SE":
            return
        try:
            import scb
            # Neighborhood names (RegSO lookup).
            name_map = scb.load_regso_names(self._city_id)
            if name_map:
                for b in self._buildings:
                    if not b.neighborhood and b.deso and b.deso in name_map:
                        b.neighborhood = name_map[b.deso]
            # Gender distribution per DeSO.
            gender_map = scb.load_deso_gender(self._city_id)
            if gender_map:
                for b in self._buildings:
                    if not b.gender_frac and b.deso and b.deso in gender_map:
                        b.gender_frac = gender_map[b.deso]
            # Household average size (kommun-level, residential only).
            hh = scb.load_households(self._city_id)
            avg_sz = hh.get("avg_household_size") if hh else None
            if avg_sz:
                _RES = frozenset({Zone.RES_LO, Zone.RES_MED, Zone.RES_HI})
                for b in self._buildings:
                    if b.household_avg_size is None and b.zone in _RES:
                        b.household_avg_size = avg_sz
            # Country of birth (city-wide native/foreign split from SCB).
            try:
                cob = scb.load_country_of_birth(self._city_id)
                self._foreign_frac = cob.get("foreign_frac") if cob else None
            except Exception:
                self._foreign_frac = None
            self._attach_origins()
        except Exception as e:
            print(f"[engine] SCB enrichment failed for {self._city_id}: {e}")
            self._attach_origins()

    def _attach_origins(self) -> None:
        """
        Assign a 6-group country-of-birth distribution to every building.
        Deterministic per-building (idx-derived jitter) so the same city
        renders identically across reloads. The non-Swedish columns are
        rescaled so the city total matches the SCB foreign_frac.
        """
        ff = self._foreign_frac
        for b in self._buildings:
            # Stable jitter from the building index — Mulberry32-style splat.
            j = ((b.idx * 0x6D2B79F5) & 0xFFFFFFFF) / 0x1_0000_0000
            b.origin = demography.origin_for(int(b.zone), j, ff)

    def _build_neighbors(self) -> None:
        """
        Precompute Gaussian-weighted neighbour lists for each building.
        Weight = exp(-d² / 2σ²) × attractiveness, for d ≤ NEIGH_RADIUS.

        Uses a spatial grid hash (cell size = NEIGH_RADIUS) so complexity is
        O(N × avg_per_cell²) rather than O(N²).
        """
        sig2    = 2.0 * _NEIGH_SIGMA * _NEIGH_SIGMA
        radius2 = _NEIGH_RADIUS * _NEIGH_RADIUS
        cell_sz = _NEIGH_RADIUS  # one cell covers the full search radius

        # Build grid hash: (cx, cz) → list of Cell objects
        grid: dict[tuple[int, int], list] = {}
        for b in self._buildings:
            key = (int(b.world_x // cell_sz), int(b.world_z // cell_sz))
            grid.setdefault(key, []).append(b)

        nbrs: list[list[tuple[int, float]]] = [[] for _ in self._buildings]
        for bi in self._buildings:
            cx = int(bi.world_x // cell_sz)
            cz = int(bi.world_z // cell_sz)
            # Examine 3 × 3 surrounding grid cells
            for dcx in (-1, 0, 1):
                for dcz in (-1, 0, 1):
                    for bj in grid.get((cx + dcx, cz + dcz), []):
                        if bj.idx == bi.idx:
                            continue
                        dx = bi.world_x - bj.world_x
                        dz = bi.world_z - bj.world_z
                        d2 = dx * dx + dz * dz
                        if d2 > radius2:
                            continue
                        w = math.exp(-d2 / sig2) * _ATTRACT[bj.zone]
                        if w > 1e-6:
                            nbrs[bi.idx].append((bj.idx, w))
        self._neighbors = nbrs

    # ── SEIR seeding ──────────────────────────────────────────────────────────

    def _seed_seir(self) -> None:
        for b in self._buildings:
            pop = float(b.pop)
            b.S = {g: pop * b.age_frac[g] for g in AGE_GROUPS}
            b.E = {g: 0.0 for g in AGE_GROUPS}
            b.I = {g: 0.0 for g in AGE_GROUPS}
            b.R = {g: 0.0 for g in AGE_GROUPS}
            b.D = {g: 0.0 for g in AGE_GROUPS}

        # Re-seed RNG so every reset with the same seed is reproducible
        # for stochastic SEIR dynamics.
        self._rng = random.Random(self._rng_seed)

        # Seed 5 exposed adults in one of three ways, in priority order:
        #   1. A user-picked building (manual_patient_zero_idx), or
        #   2. A uniformly-random residential building (random mode), or
        #   3. The most central residential building (deterministic mode).
        #
        # Candidate pool is *any cell with pop > 0* — under the current
        # rules only residential zones carry population (non-residential
        # zones like COMMERCIAL/SCHOOL/FOREST all have pop=0). If we
        # picked from COMMERCIAL as the old code did, the seed would
        # land on an empty cell, take = min(5, 0) = 0, and the epidemic
        # would never start.
        #
        # Random mode uses the module-level `random` (truly non-deterministic
        # across resets), NOT self._rng — otherwise re-seeding above with the
        # same _rng_seed would always pick the exact same "random" building.
        populated = [b for b in self._buildings if b.pop > 0]

        # ── Game-mode multi-seed in crowded residential buildings ────────
        # When the engine is in game mode and no manual pin is set, ignore
        # the random/deterministic policy and seed the N most-crowded
        # *residential* buildings. Restricting to RES_LO / RES_MED /
        # RES_HI gives the outbreak a household origin (a real cluster
        # of people who live there), instead of a mall or station that
        # only has a synthetic capacity. The user can still manually pin
        # patient zero on any building via the picker.
        _RES_ZONES = (Zone.RES_LO, Zone.RES_MED, Zone.RES_HI)
        if (self._mode == "game"
                and self._manual_patient_zero_idx is None):
            scored: list[tuple[float, "Cell"]] = []
            for b in self._buildings:
                if b.zone not in _RES_ZONES:
                    continue
                if b.pop <= 0:
                    continue
                scored.append((float(b.pop), b))
            scored.sort(key=lambda t: t[0], reverse=True)
            chosen = [b for _, b in scored[: max(1, self._auto_seed_n)]]
            if chosen:
                seeded_idx: list[int] = []
                for s in chosen:
                    if s.pop <= 0:
                        cap = _synthetic_capacity(s)
                        if cap <= 0:
                            continue
                        s.pop = int(cap)
                        s.S["adult"] = float(cap)
                        s.S["child"] = 0.0
                        s.S["elder"] = 0.0
                    take = min(5.0, s.S["adult"])
                    s.E["adult"] += take
                    s.S["adult"]  = max(0.0, s.S["adult"] - take)
                    seeded_idx.append(s.idx)
                self._patient_zero_indices = seeded_idx
                self._day = 0
                self._day_of_year_offset = self._chosen_season_offset()
                self._peak_I_frac = 0.0
                self._peak_I_day  = 0
                self._N0          = sum(b.N for b in self._buildings)
                self._history.clear()
                self._history.append(self._snapshot())
                return

        if (self._manual_patient_zero_idx is not None
                and 0 <= self._manual_patient_zero_idx < len(self._buildings)):
            seed = self._buildings[self._manual_patient_zero_idx]
            # If the user pinned patient zero onto an unpopulated cell
            # (a school, hospital, mall, nightclub, …), give that cell a
            # synthetic capacity derived from zone + footprint so the
            # outbreak can actually take hold there. Forests / water are
            # excluded — an infection in a lake is meaningless.
            if seed.pop <= 0:
                cap = _synthetic_capacity(seed)
                if cap > 0:
                    seed.pop = int(cap)
                    # Initialise the SEIR adult bucket so the seed line
                    # below has S adults to flip into E.
                    seed.S["adult"] = float(cap)
                    seed.S["child"] = 0.0
                    seed.S["elder"] = 0.0
                elif populated:
                    seed = min(
                        populated,
                        key=lambda b: ((b.world_x - seed.world_x) ** 2
                                     + (b.world_z - seed.world_z) ** 2),
                    )
        elif not populated:
            seed = self._buildings[0]
        elif self._random_patient_zero:
            # Residential-only random pick, biased toward central + dense.
            # Outbreaks should originate in a household, not a cemetery
            # or a church that happens to sit central. Restricting to
            # RES_LO/MED/HI guarantees a real population is present and
            # makes the seed location physically plausible. The user
            # can still pin a non-residential building manually.
            #
            # Bias: sort by world_x²+world_z² (central first), then
            # weight the draw by building population so a dense apartment
            # block is more likely than a single-family house.
            elig = [b for b in populated if b.zone in _RES_ZONES]
            pool = elig if elig else populated   # fall back if no residential exists
            pool.sort(key=lambda b: b.world_x ** 2 + b.world_z ** 2)
            cutoff = max(1, len(pool) // 3)
            central = pool[:cutoff]
            weights = [max(1, int(b.pop)) for b in central]
            seed = random.choices(central, weights=weights, k=1)[0]
        else:
            seed = min(populated,
                       key=lambda b: b.world_x ** 2 + b.world_z ** 2)
        take = min(5.0, seed.S["adult"])
        seed.E["adult"] += take
        seed.S["adult"]  = max(0.0, seed.S["adult"] - take)

        # Remember which buildings hosted the initial outbreak so the
        # frontend can mark them as "patient zero" pins. List for future
        # multi-seed support; currently always exactly one entry.
        self._patient_zero_indices: list[int] = [seed.idx]

        self._day = 0
        # Re-roll the seasonal start offset so each fresh run lands in a
        # different season — UNLESS the user pinned a specific season via
        # set_start_season(), in which case lock day 0 to that season's
        # midpoint so the climate model produces the right weather.
        self._day_of_year_offset = self._chosen_season_offset()
        self._peak_I_frac = 0.0
        self._peak_I_day  = 0
        self._N0          = sum(b.N for b in self._buildings)
        self._history.clear()
        self._history.append(self._snapshot())

    # ── Parameter computation ─────────────────────────────────────────────────

    def _effective_params(self) -> SEIRParams:
        p  = SEIRParams(
            beta=self._base.beta, sigma=self._base.sigma,
            gamma=self._base.gamma, mu=self._base.mu,
            waning_rate=self._base.waning_rate,
        )
        iv = self._active
        if "lockdown" in iv:
            p.local_frac = min(0.97, p.local_frac + 0.40)
            p.beta      *= 0.72
        if "masks" in iv:
            p.beta *= 0.50
        if "distancing" in iv:
            p.beta       *= 0.68
            p.local_frac  = min(0.92, p.local_frac + 0.15)
        if "schools" in iv:
            p.beta         *= 0.88
            p.school_factor = 0.20
        if "testing" in iv:
            p.gamma *= 1.4
            p.beta  *= 0.75
        # Game-mode (also exposed in sandbox) extras
        if "info_campaign" in iv:
            p.beta *= 0.90
        if "targeted_lockdown" in iv:
            # Stacks multiplicatively with full lockdown if both are on.
            p.beta      *= 0.65
            p.local_frac = min(0.95, p.local_frac + 0.20)
        if "hospital_surge" in iv:
            # Effective hospital capacity raised; engine.step() reads
            # p.hospital_capacity to decide overload.
            p.hospital_capacity *= 1.40
        if "stop_transport" in iv:
            # No public transit running → no in-vehicle spread, plus a
            # mild β reduction citywide (people travel less even by foot
            # / car when public transit is gone).
            p.beta *= 0.85
        return p

    # ── Force of infection ────────────────────────────────────────────────────

    def _foi(self, i: int, p: SEIRParams) -> dict[str, float]:
        """Return {group: λ_g} — per-group force of infection for building i."""
        bi = self._buildings[i]
        N  = bi.N
        if N == 0:
            return {g: 0.0 for g in AGE_GROUPS}

        sf       = p.school_factor if bi.zone == Zone.SCHOOL else 1.0
        beta_eff = p.beta * _BETA_MULT[bi.zone] * sf

        # Spatial quarantine zone — buildings inside get β crushed and
        # the cross-boundary neighbour edges below are skipped, so the
        # zone is effectively isolated from the rest of the city.
        from spatial import QUARANTINE_BETA_MULT as _QBM
        q_set = self._spatial.quarantine_set()
        in_q  = i in q_set
        if in_q:
            beta_eff *= _QBM

        # Household-adjusted local_frac: larger average household size
        # concentrates contacts within the building (higher local_frac);
        # smaller households spread contacts more to the neighbourhood.
        # Calibrated around 2.2 persons/household (Swedish average).
        # The adjustment is ±0.05 per unit deviation, clamped.
        lf = p.local_frac
        if bi.household_avg_size is not None:
            lf += 0.05 * (bi.household_avg_size - 2.2)
            lf = max(0.10, min(0.97, lf))

        # Local infectious prevalence by age group (within this building)
        local_Ig = {g: bi.I[g] / N for g in AGE_GROUPS}

        # Neighbour-weighted infectious prevalence by age group
        w_tot = 0.0
        w_Ig  = {g: 0.0 for g in AGE_GROUPS}
        for j, w in self._neighbors[i]:
            # Quarantine boundary: if exactly one of (i, j) is inside a
            # quarantine zone, skip this edge entirely — that's the
            # "sealed perimeter" effect. Edges fully inside or fully
            # outside the zone behave normally.
            if (j in q_set) != in_q:
                continue
            bj = self._buildings[j]
            Nj = bj.N
            if Nj == 0 or bj.I_tot < 0.001:
                w_tot += w  # count weight even for healthy neighbours
                continue
            nb_sf = p.school_factor if bj.zone == Zone.SCHOOL else 1.0
            coeff = w * _BETA_MULT[bj.zone] * nb_sf
            for g in AGE_GROUPS:
                w_Ig[g] += coeff * (bj.I[g] / Nj)
            w_tot += w
        neigh_Ig = {g: (w_Ig[g] / w_tot if w_tot else 0.0) for g in AGE_GROUPS}

        # λ_g = β_eff × Σ_g' C[g][g'] × (lf·I_local_g' + (1-lf)·I_neigh_g')
        out: dict[str, float] = {}
        for g in AGE_GROUPS:
            row = _CONTACT_M[g]
            mix = 0.0
            for gp in AGE_GROUPS:
                I_eff = lf * local_Ig[gp] + (1.0 - lf) * neigh_Ig[gp]
                mix  += row[gp] * I_eff
            out[g] = beta_eff * mix
        return out

    # ── Commuting multiplier ──────────────────────────────────────────────────

    @staticmethod
    def _commute_mult(hour: float) -> tuple[float, float]:
        if 6 <= hour < 9 or 17 <= hour < 20:
            return 1.25, -0.15
        elif 9 <= hour < 17:
            return 0.95, -0.05
        else:
            return 0.70, +0.15

    def _weekday(self) -> int:
        """0 = Monday … 6 = Sunday. The simulation always starts on
        Monday (day 0) so the working week aligns naturally with the
        first sim day."""
        return self._day % 7

    def _is_weekend(self) -> bool:
        return self._weekday() >= 5

    @staticmethod
    def _weekend_mult(hour: float) -> tuple[float, float, float]:
        """Saturday + Sunday: no commuter rush, no school, more time at
        home / out for leisure. Returns (beta_scale, lfrac_delta, school_factor).
          - beta dimmed during the would-be rush windows (no commuting)
          - local_frac up everywhere (more household mixing)
          - schools effectively closed
        """
        if 9 <= hour < 18:
            return 0.85, +0.05, 0.05      # midday — leisure / shops
        elif 18 <= hour < 23:
            return 1.05, -0.05, 0.05      # evenings — restaurants / nightlife
        else:
            return 0.65, +0.20, 0.05      # nights — at home

    # ── Simulation step ───────────────────────────────────────────────────────

    def step(self, n: int = 1, flow_hour: float = 9.0,
             external_foi: float = 0.0) -> None:
        """
        Advance the simulation ``n`` sub-steps within a single day.

        ``external_foi`` is an additive per-day force-of-infection contribution
        injected from outside the focused city — specifically, the metapopulation
        wrapper uses it to seed infections imported from other kommuner via the
        commuter flow matrix. It is applied uniformly to every building (a
        scalar, not per-group) and is *added* to the intra-city FOI computed
        by ``_foi()``. When zero, behaviour is bit-identical to single-city
        runs, so existing calibrations do not drift.
        """
        hours_per_step = 24.0 / max(1, n)
        for i in range(n):
            h = (flow_hour + i * hours_per_step) % 24
            self._step_one(h, external_foi=external_foi)

    def _step_one(self, flow_hour: float = 9.0,
                  external_foi: float = 0.0) -> None:
        p              = self._effective_params()
        if self._is_weekend():
            # Weekend mixing: stronger household mixing, weak commute,
            # schools effectively closed regardless of the policy slider.
            beta_scale, lfrac_delta, weekend_school = self._weekend_mult(flow_hour)
            p.school_factor = min(p.school_factor, weekend_school)
        else:
            beta_scale, lfrac_delta = self._commute_mult(flow_hour)
        p.beta         *= beta_scale
        p.local_frac    = max(0.10, min(0.97, p.local_frac + lfrac_delta))

        # Climate modulation (Lowen 2014; Wang 2021): cold/dry → β ↑.
        # Day-of-year is shifted by a per-run random offset so the season at
        # day 0 varies (winter / spring / summer / autumn) — otherwise every
        # outbreak starts in early-January cold.
        doy          = (self._day + self._day_of_year_offset) % 365
        clim         = climate_mod.compute(doy, flow_hour)
        p.beta      *= clim.beta_mult
        self._last_hour = flow_hour

        total_N = sum(b.N          for b in self._buildings)
        total_I = sum(b.I_tot      for b in self._buildings)
        self._overloaded = (total_I / total_N > p.hospital_capacity) if total_N else False
        mu_eff = p.mu_overloaded if self._overloaded else p.mu
        # Track peak prevalence over the whole simulation
        if total_N > 0:
            i_frac = total_I / total_N
            if i_frac > self._peak_I_frac:
                self._peak_I_frac = i_frac
                self._peak_I_day  = self._day

        stochastic = self._stochastic
        # Vaccination is gated by the difficulty's rollout date in game
        # mode. In sandbox it's always available the moment it's toggled.
        _vax_unlocked = (self._game_state is None
                         or self._game_state.vaccine_unlocked(self._day))
        # Spatial-intervention lookups — read once, used inside the inner
        # loops below. All return empty sets when nothing is active.
        from spatial import (
            VACC_BOOST_TARGET_MULT as _VBT,
            VACC_BOOST_BACKGROUND   as _VBB,
            SURGE_GAMMA_MULT        as _SGM,
        )
        _q_set     = self._spatial.quarantine_set()
        _vacc_set  = self._spatial.vacc_boost_set()
        _vacc_any  = bool(_vacc_set)
        _clinic_set = self._spatial.clinic_set()
        _surge_set  = self._spatial.surge_set()
        # Fast path: if no infection exists at all, skip the entire SEIR
        # loop. Only vaccination and waning need processing.
        total_E = sum(b.E_tot      for b in self._buildings)
        if total_I < 0.001 and total_E < 0.001 and external_foi <= 0.0:
            if "vaccination" in self._active and _vax_unlocked:
                for i, b in enumerate(self._buildings):
                    if _vacc_any:
                        vm = _VBT if i in _vacc_set else _VBB
                    else:
                        vm = 1.0
                    for g in ("elder", "adult", "child"):
                        Sg = b.S[g]
                        if Sg > 0:
                            rate = 0.015 * vm * (2.0 if g == "elder" else 1.0)
                            vax  = min(rate * Sg, Sg)
                            b.S[g] = max(0.0, Sg - vax)
                            b.R[g] += vax
            # Still need waning (processed below) and dispatcher
        else:
            # Build a set of buildings (and their neighbours) that are near
            # infection. Much faster than per-building any() checks.
            # Include buildings with E (need E→I) or I (need FOI spread).
            # Neighbours of I-buildings need FOI computation too.
            _infected_zone: set[int] = set()
            for b in self._buildings:
                if b.E_tot > 0.001:
                    _infected_zone.add(b.idx)
                if b.I_tot > 0.001:
                    _infected_zone.add(b.idx)
                    for j, _ in self._neighbors[b.idx]:
                        _infected_zone.add(j)

            do_vax = "vaccination" in self._active and _vax_unlocked
            for i, b in enumerate(self._buildings):
                if b.N == 0:
                    continue
                # Per-building modifiers driven by spatial interventions:
                #   targeted_vaccine → 3× rate inside the chosen area, ⅓×
                #     elsewhere (zero-sum so total city effort is constant)
                #   mobile_clinic    → use baseline μ even under overflow
                #   surge_testing    → γ × 2 (faster removal via isolation)
                if _vacc_any:
                    vax_mult = _VBT if i in _vacc_set else _VBB
                else:
                    vax_mult = 1.0
                mu_local = p.mu if (i in _clinic_set) else mu_eff
                gamma_local = p.gamma * (_SGM if i in _surge_set else 1.0)

                if i not in _infected_zone:
                    if do_vax:
                        for g in ("elder", "adult", "child"):
                            Sg = b.S[g]
                            if Sg > 0:
                                rate = 0.015 * vax_mult * (2.0 if g == "elder" else 1.0)
                                vax  = min(rate * Sg, Sg)
                                b.S[g] = max(0.0, Sg - vax)
                                b.R[g] += vax
                    continue
                foi = self._foi(i, p)
                if external_foi > 0.0:
                    for g in AGE_GROUPS:
                        foi[g] += external_foi
                for g in AGE_GROUPS:
                    mu_g = mu_local * _MU_MULT[g]
                    Sg, Eg, Ig = b.S[g], b.E[g], b.I[g]
                    mE = foi[g]              * Sg
                    mI = p.sigma             * Eg
                    mR = (1.0 - mu_g) * gamma_local * Ig
                    mD = mu_g         * gamma_local * Ig
                    if stochastic:
                        dE = self._perturb(mE, Sg)
                        dI = self._perturb(mI, Eg)
                        dR = self._perturb(mR, Ig)
                        dD = self._perturb(mD, max(0.0, Ig - dR))
                    else:
                        dE = min(mE, Sg)
                        dI = min(mI, Eg)
                        dR = min(mR, Ig)
                        dD = min(mD, Ig)

                    b.S[g] = max(0.0, Sg - dE)
                    b.E[g] = max(0.0, Eg + dE - dI)
                    b.I[g] = max(0.0, Ig + dI - dR - dD)
                    b.R[g] = max(0.0, b.R[g] + dR)
                    b.D[g] = max(0.0, b.D[g] + dD)

                if do_vax:
                    for g in ("elder", "adult", "child"):
                        Sg = b.S[g]
                        if Sg <= 0:
                            continue
                        rate = 0.015 * vax_mult * (2.0 if g == "elder" else 1.0)
                        vax  = min(rate * Sg, Sg)
                        b.S[g] = max(0.0, Sg - vax)
                        b.R[g] += vax

        # Waning immunity (SEIRS) — each group independently
        if p.waning_rate > 0:
            for b in self._buildings:
                for g in AGE_GROUPS:
                    Rg = b.R[g]
                    if Rg <= 0:
                        continue
                    waned = min(p.waning_rate * Rg, Rg)
                    b.R[g] = max(0.0, Rg - waned)
                    b.S[g] += waned

        # Transport dispatcher: process every trip arriving in the minute
        # range covered by this sub-step. Critical: must iterate the FULL
        # minute range (prev → cur) — picking only the current minute would
        # silently drop ~95% of trips when /api/step?n=24 advances a full
        # day in one call (each sub-step is then 60 minutes wide).
        if self._dispatcher is not None:
            cur_minute = int(flow_hour * 60.0) % 1440
            try:
                from metapop import metapop as _metapop_singleton
                _mp = _metapop_singleton if _metapop_singleton.is_available() else None
            except Exception:
                _mp = None
            self._dispatcher.process(self._last_minute, cur_minute, self, _mp)
            self._last_minute = cur_minute

        self._day += 1
        self._history.append(self._snapshot())

        # Auto-expire spatial interventions whose duration has elapsed
        # (mobile clinics and surge tests are time-bounded).
        self._spatial.expire(self._day)

        # Game-mode tick — runs once per simulated day, after compartments
        # have been advanced. No-op when _game_state is None (sandbox).
        if self._game_state is not None:
            sn = self._history[-1]
            self._game_state.tick(
                eng_state=sn,
                active_interventions=list(self._active),
                day=self._day,
                population=int(self._N0) if self._N0 > 0 else
                            sum(b.N for b in self._buildings),
                spatial_costs=self._spatial.daily_costs(),
            )

    # ── Stats ─────────────────────────────────────────────────────────────────

    def _snapshot(self) -> dict[str, Any]:
        S = E = I = R = D = 0.0
        by_age = {g: {"S": 0.0, "E": 0.0, "I": 0.0, "R": 0.0, "D": 0.0}
                  for g in AGE_GROUPS}
        for b in self._buildings:
            S += b.S_tot; E += b.E_tot; I += b.I_tot; R += b.R_tot; D += b.D_tot
            for g in AGE_GROUPS:
                a = by_age[g]
                a["S"] += b.S[g]; a["E"] += b.E[g]; a["I"] += b.I[g]
                a["R"] += b.R[g]; a["D"] += b.D[g]
        return {"day": self._day,
                "weekday": self._weekday(),
                "is_weekend": self._is_weekend(),
                "S": S, "E": E, "I": I, "R": R, "D": D,
                "age": by_age}

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def active_interventions(self) -> list[str]:
        return list(self._active)

    # ── Spatial-intervention helpers ─────────────────────────────────────────

    @property
    def spatial(self):
        """Expose the SpatialRegistry so main.py can add/remove items."""
        return self._spatial

    def select_buildings_in_deso(self, deso: str) -> list[int]:
        """Return idx of every building whose `b.deso` matches."""
        return [b.idx for b in self._buildings if b.deso == deso]

    def select_buildings_in_neighborhood(self, name: str) -> list[int]:
        """
        Return idx of every building whose DeSO maps to the given RegSO
        neighborhood name. The Areas overlay renders one polygon per
        RegSO group; this lets the user click that polygon and have the
        whole neighborhood (which may span several DeSO subareas)
        included in the selection.
        """
        try:
            import scb
            names = scb.load_regso_names(self._city_id)
        except Exception:
            names = {}
        target_desos = {d for d, n in names.items() if n == name}
        if not target_desos:
            # Allow callers to pass a raw DeSO id as fallback.
            target_desos = {name}
        return [b.idx for b in self._buildings if b.deso in target_desos]

    def select_buildings_in_polygon(self, polygon_lonlat: list[list[float]]
                                    ) -> list[int]:
        """
        Polygon hit-test in WGS84 using Shapely. Polygon is a closed ring
        of [lon, lat] pairs; the caller doesn't need to repeat the first
        point. Returns indices of buildings whose centroid lies inside.
        """
        if len(polygon_lonlat) < 3:
            return []
        from shapely.geometry import Polygon, Point
        poly = Polygon(polygon_lonlat)
        if not poly.is_valid:
            poly = poly.buffer(0)
        out: list[int] = []
        for b in self._buildings:
            if b.lon is None or b.lat is None:
                continue
            if poly.contains(Point(b.lon, b.lat)):
                out.append(b.idx)
        return out

    def select_buildings_in_radius(self, lon: float, lat: float,
                                   radius_m: float) -> list[int]:
        """
        Approximate radius hit-test using a degree → metre conversion at
        the city's centroid. Good enough for ≤ 5 km radii — the engine
        already uses planar metres internally so we go via world_x/z.
        """
        # Find the clicked point in world metres by nearest-building proxy:
        # locate the closest building, take its world coords as the centre.
        best_d2 = float("inf")
        bx = bz = 0.0
        for b in self._buildings:
            if b.lon is None or b.lat is None:
                continue
            dlon = (b.lon - lon) * 111000.0  # rough metres per degree
            dlat = (b.lat - lat) * 111000.0
            d2 = dlon * dlon + dlat * dlat
            if d2 < best_d2:
                best_d2 = d2
                bx, bz = b.world_x, b.world_z
        out: list[int] = []
        r2 = radius_m * radius_m
        for b in self._buildings:
            dx = b.world_x - bx
            dz = b.world_z - bz
            if dx * dx + dz * dz <= r2:
                out.append(b.idx)
        return out

    def select_building_and_neighbours(self, idx: int, n: int = 5
                                       ) -> list[int]:
        """The clicked building plus its top-N nearest neighbours by weight."""
        if idx < 0 or idx >= len(self._buildings):
            return []
        nbrs = sorted(self._neighbors[idx], key=lambda x: -x[1])[:max(0, n)]
        return [idx] + [j for j, _w in nbrs]

    def nearest_building(self, lon: float, lat: float) -> int:
        """Return the index of the building closest to (lon, lat), or -1."""
        best, best_d2 = -1, float("inf")
        for b in self._buildings:
            if b.lon is None or b.lat is None:
                continue
            dlon = (b.lon - lon) * 111000.0
            dlat = (b.lat - lat) * 111000.0
            d2 = dlon * dlon + dlat * dlat
            if d2 < best_d2:
                best_d2 = d2
                best = b.idx
        return best

    def population_of(self, indices: list[int]) -> int:
        return int(sum(self._buildings[i].pop for i in indices
                       if 0 <= i < len(self._buildings)))

    def deso_label(self, deso_id: str) -> str:
        """Human-readable RegSO name for a given DeSO id; falls back to id."""
        try:
            import scb
            names = scb.load_regso_names(self._city_id)
            return names.get(deso_id, deso_id)
        except Exception:
            return deso_id

    def toggle_intervention(self, iv_id: str) -> bool:
        # Vaccination is locked until the difficulty's vaccine_day in
        # game mode. Allow turning it off any time, but block turn-on
        # before the rollout date. Sandbox mode has no lock.
        if (iv_id == "vaccination"
                and iv_id not in self._active
                and self._game_state is not None
                and not self._game_state.vaccine_unlocked(self._day)):
            return False  # remains off
        if iv_id in self._active:
            self._active.discard(iv_id)
            now_on = False
        else:
            self._active.add(iv_id)
            now_on = True
        if self._game_state is not None:
            self._game_state.on_intervention_toggled(iv_id, now_on, self._day)
        return now_on

    def update_params(self, *, beta: float | None = None,
                      sigma: float | None = None, gamma: float | None = None,
                      mu: float | None = None, waning_days: int | None = None) -> None:
        if beta is not None:        self._base.beta  = float(beta)
        if sigma is not None:       self._base.sigma = float(sigma)
        if gamma is not None:       self._base.gamma = float(gamma)
        if mu is not None:          self._base.mu    = float(mu)
        if waning_days is not None:
            wd = int(waning_days)
            self._base.waning_rate = (1.0 / wd) if wd > 0 else 0.0

    def restore(self, day: int, cells_data: list[dict],
                chart_history: list[dict], active: list[str]) -> None:
        """
        Restore engine to a previously recorded snapshot.

        Accepts both age-stratified snapshots (each cell has an `age` dict
        with per-group S/E/I/R/D) and legacy homogeneous snapshots (just
        S/E/I/R/D totals), splitting totals by `age_frac` in the latter case.
        """
        self._day = day
        for cd in cells_data:
            b = self._buildings[cd["idx"]]
            age = cd.get("age")
            if isinstance(age, dict) and "child" in age:
                for g in AGE_GROUPS:
                    ag = age.get(g, {})
                    b.S[g] = float(ag.get("S", 0.0))
                    b.E[g] = float(ag.get("E", 0.0))
                    b.I[g] = float(ag.get("I", 0.0))
                    b.R[g] = float(ag.get("R", 0.0))
                    b.D[g] = float(ag.get("D", 0.0))
            else:
                frac = b.age_frac
                for g in AGE_GROUPS:
                    f = frac[g]
                    b.S[g] = float(cd.get("S", 0.0)) * f
                    b.E[g] = float(cd.get("E", 0.0)) * f
                    b.I[g] = float(cd.get("I", 0.0)) * f
                    b.R[g] = float(cd.get("R", 0.0)) * f
                    b.D[g] = float(cd.get("D", 0.0)) * f
        self._history    = deque(chart_history, maxlen=1000)
        self._active     = set(active)
        self._overloaded = False

    def attach_dispatcher(self, dispatcher: Any) -> None:
        """Attach a transport TripDispatcher. Pass None to detach."""
        self._dispatcher = dispatcher
        self._last_minute = 0
        if dispatcher is not None and hasattr(dispatcher, "reset"):
            dispatcher.reset()

    def reset(self) -> None:
        self._active.clear()
        self._overloaded = False
        self._base = SEIRParams()
        self._last_minute = 0
        # Wipe spatial interventions on reset — quarantine zones, mobile
        # clinics, etc. don't survive a city restart any more than the
        # citywide intervention set does.
        if hasattr(self, "_spatial"):
            self._spatial.clear()
        if self._dispatcher is not None and hasattr(self._dispatcher, "reset"):
            self._dispatcher.reset()
        if not self._buildings:
            self._load_city()
        self._seed_seir()

    def city_layout(self) -> dict[str, Any]:
        city = osm.CITIES[self._city_id]
        return {
            "city_id":    self._city_id,
            "name":       city["name"],
            "center_lat": city["center_lat"],
            "center_lon": city["center_lon"],
            "interventions": INTERVENTIONS_META,
            "roads": self._roads,
            "border": self._border,
            "nature_pois":     self._nature_pois,
            "flag_poles":      self._flag_poles,
            "lnu_flag_poles":  self._lnu_flag_poles,
            "easter_eggs":     self._easter_eggs,
            "buildings": [
                {
                    "idx":      b.idx,
                    "world_x":  b.world_x,
                    "world_z":  b.world_z,
                    "lon":      b.lon,
                    "lat":      b.lat,
                    "zone":     int(b.zone),
                    "pop":      b.pop,
                    # Synthetic occupancy for non-residential buildings —
                    # surfaced in the hover tooltip as "Capacity". 0 for
                    # forest/water/road, where an outbreak makes no sense.
                    **({"capacity": _synthetic_capacity(b)} if b.pop <= 0 else {}),
                    "levels":   b.levels,
                    "area_m2":  b.area_m2,
                    "polygon":  b.polygon,
                    "deso":     b.deso,
                    # Manual-override metadata — only emitted when set so the
                    # 18k-building payload doesn't balloon with null fields.
                    **({"name":           b.name}           if b.name           else {}),
                    **({"custom_builder": b.custom_builder} if b.custom_builder else {}),
                    **({"osm_id":         b.osm_id}         if b.osm_id         else {}),
                    # Natural-feature metadata (only set for FORESTRY cells).
                    **({"tree_species":   b.tree_species}   if b.tree_species   else {}),
                    **({"tree_count":     b.tree_count}     if b.tree_count is not None else {}),
                    **({"water_type":     b.water_type}     if b.water_type    else {}),
                    # Cemetery interior-hole rings (only set when
                    # _clip_cemetery_polygons cut buildings out of
                    # the cemetery footprint). Frontend
                    # _makeExtrudeGeo pushes these into THREE.Shape.holes.
                    **({"polygon_holes":  b.polygon_holes}  if b.polygon_holes  else {}),
                    # SCB DeSO-level mean net income for the Income
                    # color/height viz modes. Only emitted when set.
                    **({"income":         b.income}         if b.income is not None else {}),
                    # Human-readable neighborhood name (RegSO).
                    **({"neighborhood":   b.neighborhood}   if b.neighborhood     else {}),
                    # Gender distribution (male/female fractions).
                    **({"gender_frac":    b.gender_frac}    if b.gender_frac      else {}),
                    **({"gender":         b.gender}         if b.gender           else {}),
                    # Household stats for residential buildings (synthetic).
                    **({"household_avg_size": b.household_avg_size} if b.household_avg_size is not None else {}),
                    **({"households":     b.households}     if b.households is not None else {}),
                    **({"household_sizes": b.household_sizes} if b.household_sizes else {}),
                    # 6-vector country-of-birth distribution (sums to 1.0).
                    **({"origin":           b.origin}        if b.origin           else {}),
                    **({"predominant_origin": demography.predominant(b.origin)} if b.origin else {}),
                }
                for b in self._buildings
            ],
            "origin_groups":  demography.ORIGIN_GROUPS,
            "foreign_frac":   self._foreign_frac,
        }

    def _params_dict(self) -> dict[str, Any]:
        return {
            "beta":        self._base.beta,
            "sigma":       self._base.sigma,
            "gamma":       self._base.gamma,
            "mu":          self._base.mu,
            "waning_days": round(1.0 / self._base.waning_rate)
                           if self._base.waning_rate > 0 else 0,
        }

    def _city_beta_eff(self, p: SEIRParams) -> float:
        """Population-weighted city β across non-road buildings (per day)."""
        num = den = 0.0
        for b in self._buildings:
            if b.N <= 0:
                continue
            sf = p.school_factor if b.zone == Zone.SCHOOL else 1.0
            num += b.N * p.beta * _BETA_MULT.get(b.zone, 0.0) * sf
            den += b.N
        return num / den if den > 0 else 0.0

    def _baseline_params(self) -> SEIRParams:
        """User-base parameters with NO interventions applied."""
        return SEIRParams(
            beta=self._base.beta, sigma=self._base.sigma,
            gamma=self._base.gamma, mu=self._base.mu,
            waning_rate=self._base.waning_rate,
        )

    def _growth_metrics(self) -> dict[str, float | None]:
        """
        Fit r = d/dt ln(I) over the last 7 days using log-linear regression.
        Returns r (per day), doubling time T_d, and daily incidence (new
        cases since previous snapshot). Wallinga & Lipsitch 2007.
        """
        hist = list(self._history)
        if len(hist) < 3:
            return {"growth_rate": None, "doubling_time": None,
                    "new_infections": None}
        window = hist[-min(7, len(hist)):]
        xs, ys = [], []
        for h in window:
            i = h["I"]
            if i > 1e-6:
                xs.append(h["day"])
                ys.append(math.log(i))
        if len(xs) < 2:
            return {"growth_rate": None, "doubling_time": None,
                    "new_infections": None}
        n = len(xs)
        mx = sum(xs) / n
        my = sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = sum((x - mx) ** 2 for x in xs)
        r  = num / den if den > 0 else 0.0
        td = (math.log(2) / r) if r > 1e-6 else (
              math.log(0.5) / r if r < -1e-6 else None)

        prev = hist[-2]; cur = hist[-1]
        cases_now  = cur["E"] + cur["I"] + cur["R"] + cur["D"]
        cases_prev = prev["E"] + prev["I"] + prev["R"] + prev["D"]
        new_inf = max(0.0, cases_now - cases_prev)

        return {
            "growth_rate":   round(r, 4),
            "doubling_time": round(td, 2) if td is not None else None,
            "new_infections": round(new_inf, 1),
        }

    def origin_totals(self) -> list[float]:
        """Population-weighted origin distribution across the whole city."""
        from demography import ORIGIN_GROUPS
        totals = [0.0] * len(ORIGIN_GROUPS)
        for b in self._buildings:
            if not b.origin or b.N <= 0:
                continue
            for i, f in enumerate(b.origin):
                totals[i] += b.N * f
        return totals

    def state(self) -> dict[str, Any]:
        sn    = self._snapshot()
        p     = self._effective_params()
        bl    = self._baseline_params()
        # Apply current climate to both effective and baseline β so the delta
        # we report reflects ONLY the intervention contribution. Day-of-year
        # is shifted by the per-run random offset (matches _step_one).
        doy_now  = (self._day + self._day_of_year_offset) % 365
        clim     = climate_mod.compute(doy_now, self._last_hour)
        p.beta  *= clim.beta_mult
        bl.beta *= clim.beta_mult

        N_tot = sn["S"] + sn["E"] + sn["I"] + sn["R"]
        r_eff = round((p.beta / p.gamma) * (sn["S"] / N_tot), 3) if N_tot > 0 else 0.0
        r0    = round(p.beta / p.gamma, 3) if p.gamma > 0 else 0.0

        beta_eff      = self._city_beta_eff(p)
        beta_baseline = self._city_beta_eff(bl)
        beta_delta    = beta_eff - beta_baseline

        cum_cases   = sn["E"] + sn["I"] + sn["R"] + sn["D"]
        attack_rate = (cum_cases / self._N0) if self._N0 > 0 else 0.0
        cfr         = (sn["D"] / cum_cases) if cum_cases > 0 else 0.0
        ifr         = cfr
        hosp_load   = (sn["I"] / N_tot / p.hospital_capacity) if N_tot > 0 else 0.0
        growth      = self._growth_metrics()

        return {
            "day":                  self._day,
            "hour":                 round(self._last_hour, 2),
            "weekday":              self._weekday(),
            "is_weekend":           self._is_weekend(),
            "S": sn["S"], "E": sn["E"], "I": sn["I"], "R": sn["R"], "D": sn["D"],
            "cases":                cum_cases,
            "r_eff":                r_eff,
            "r0":                   r0,
            "transmission_rate":    round(beta_eff, 4),
            "transmission_baseline": round(beta_baseline, 4),
            "transmission_delta":   round(beta_delta, 4),
            "attack_rate":          round(attack_rate, 4),
            "cfr":                  round(cfr, 4),
            "ifr":                  round(ifr, 4),
            "peak_prevalence":      round(self._peak_I_frac, 4),
            "peak_prevalence_day":  self._peak_I_day,
            "hospital_load":        round(hosp_load, 3),
            "growth_rate":          growth["growth_rate"],
            "doubling_time":        growth["doubling_time"],
            "new_infections":       growth["new_infections"],
            "climate": {
                "temperature": clim.temperature,
                "humidity":    clim.humidity,
                "season":      clim.season,
                "beta_mult":   clim.beta_mult,
                "day_of_year": clim.day_of_year,
            },
            "origin_totals":        [round(v, 1) for v in self.origin_totals()],
            "foreign_frac":         self._foreign_frac,
            "hospital_overloaded":  self._overloaded,
            "active_interventions": self.active_interventions,
            "patient_zero":         list(self._patient_zero_indices),
            "spatial":              self._spatial.snapshot(self._day),
            "mode":                 self._mode,
            "difficulty":           self._difficulty,
            "game":                 (self._game_state.snapshot(self._day)
                                     if self._game_state is not None else None),
            "params":               self._params_dict(),
            "simulation_mode": {
                "stochastic":               self._stochastic,
                "random_patient_zero":      self._random_patient_zero,
                "seed":                     self._rng_seed,
                "manual_patient_zero_idx":  self._manual_patient_zero_idx,
            },
            "history":              list(self._history)[-300:],
            "cells": [
                {
                    "idx": b.idx,
                    "S":   b.S_tot, "E": b.E_tot, "I": b.I_tot,
                    "R":   b.R_tot, "D": b.D_tot,
                    "N":   b.N,
                    "age": {
                        g: {"S": b.S[g], "E": b.E[g], "I": b.I[g],
                            "R": b.R[g], "D": b.D[g]}
                        for g in AGE_GROUPS
                    },
                }
                for b in self._buildings
            ],
        }
