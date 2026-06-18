"""
osm.py — OpenStreetMap city data fetcher (buildings + roads + POI nodes).

Downloads building footprints, road polylines, and amenity/shop nodes from the
Overpass API in a single combined query, caches the raw response to disk, and
exposes a clean Python API for the engine.

Usage (standalone test):
    python osm.py
"""
from __future__ import annotations

import json
import math
import os
import random
import re
import urllib.parse
import urllib.request
from typing import Any

# ── City registry ─────────────────────────────────────────────────────────────

# ── Which countries' cities should be exposed to the UI and batch tooling ──
#
# This is a single-line master switch. Cities whose `country` field is NOT in
# this set are still fully defined in CITIES and can still be driven directly
# via the Python API (engine.EpidemicEngine('recife'), osm.load_city('recife'),
# etc.), but they're hidden from:
#
#   • /api/cities            (the macro-view catalogue)
#   • /api/cities/summary    (the macro-view marker payload)
#   • tools/download_city.py --all-extended
#
# To re-enable Brazil later: add "BR" to this set. No data migration needed,
# no code changes elsewhere — all the Recife geometry, IBGE caches, custom
# builder registrations, etc. stay in place and come back online the moment
# "BR" lands in the set.
VISIBLE_COUNTRIES: set[str] = {"SE"}


def is_visible(city_id: str) -> bool:
    """Whether `city_id` should be exposed through the UI + batch tooling."""
    meta = CITIES.get(city_id)
    if not meta:
        return False
    return meta.get("country", "SE") in VISIBLE_COUNTRIES


CITIES: dict[str, dict[str, Any]] = {
    "vaxjo": {
        "name":       "Växjö",
        "country":    "SE",
        "kommunkod":  "0780",
        "center_lat": 56.8777,
        "center_lon": 14.8091,
        # (south, west, north, east) — covers Teleborg, Vikaholm, Hovshaga,
        # Noremark, Sandsbro AND extends NW to include Småland Airport
        # (~56.929°N, 14.728°E, ~7 km NW of city centre).
        "bbox":       (56.825, 14.690, 56.955, 14.880),
        "available":  True,
        # Static stats from the processed city.json — used by the macro
        # view to size markers and populate hover cards. Set after running
        # tools/download_city.py.
        "population":      85346,
        "building_count":  17554,
    },
    "kalmar": {
        "name":       "Kalmar",
        "country":    "SE",
        "kommunkod":  "0880",
        "center_lat": 56.6634,
        "center_lon": 16.3568,
        # (south, west, north, east) — covers central Kalmar, Funkabo,
        # Lindsdal, Berga, Norrliden, Stensö, Smedby, Rinkabyholm
        "bbox":       (56.605, 16.275, 56.730, 16.430),
        "available":  True,
        "population":     57303,
        "building_count": 10477,
    },
    # ── Top 5 biggest Swedish cities by population ────────────────────────────
    "stockholm": {
        "name":       "Stockholm",
        "country":    "SE",
        "kommunkod":  "0180",
        "center_lat": 59.3293,
        "center_lon": 18.0686,
        # Central Stockholm + immediate inner districts (Norrmalm, Östermalm,
        # Södermalm, Vasastan, Kungsholmen, Gamla Stan, parts of Gärdet/
        # Hammarby/Liljeholmen). ~14×14 km — tighter than the full kommun
        # so the OSM payload stays manageable.
        "bbox":       (59.27, 17.95, 59.39, 18.18),
        "available":  True,
        "population":     1210099,
        "building_count": 50204,
    },
    "goteborg": {
        "name":       "Göteborg",
        "country":    "SE",
        "kommunkod":  "1480",
        "center_lat": 57.7089,
        "center_lon": 11.9746,
        # Central Göteborg + Hisingen + Mölndalsåns sider — covers Centrum,
        # Majorna, Lindholmen, Backa, Lunden, Krokslätt
        "bbox":       (57.65, 11.85, 57.77, 12.10),
        "available":  True,
        "population":     583095,
        "building_count": 51139,
    },
    "malmo": {
        "name":       "Malmö",
        "country":    "SE",
        "kommunkod":  "1280",
        "center_lat": 55.6050,
        "center_lon": 13.0038,
        # Central Malmö + Limhamn / Västra Hamnen / Rosengård / Husie
        "bbox":       (55.55, 12.93, 55.66, 13.10),
        "available":  True,
        "population":     366677,
        "building_count": 32225,
    },
    "uppsala": {
        "name":       "Uppsala",
        "country":    "SE",
        "kommunkod":  "0380",
        "center_lat": 59.8586,
        "center_lon": 17.6389,
        # Central Uppsala + university area + Sävja / Gottsunda / Stenhagen
        "bbox":       (59.80, 17.55, 59.92, 17.75),
        "available":  True,
        "population":     208167,
        "building_count": 23140,
    },
    "vasteras": {
        "name":       "Västerås",
        "country":    "SE",
        "kommunkod":  "1980",
        "center_lat": 59.6099,
        "center_lon": 16.5448,
        # Central Västerås + Hammarby / Bäckby / Skiljebo / Skultuna fringe
        "bbox":       (59.55, 16.45, 59.67, 16.66),
        "available":  True,
        "population":     150681,
        "building_count": 19762,
    },
    # ── Mid-sized regional capitals ───────────────────────────────────────────
    "linkoping": {
        "name":       "Linköping",
        "country":    "SE",
        "kommunkod":  "0580",
        "center_lat": 58.4108,
        "center_lon": 15.6214,
        # Central Linköping + Lambohov / Ryd / Vasastaden / Berga / Tannefors
        "bbox":       (58.36, 15.55, 58.46, 15.71),
        "available":  True,
        "population":     143921,
        "building_count": 24639,
    },
    "orebro": {
        "name":       "Örebro",
        "country":    "SE",
        "kommunkod":  "1880",
        "center_lat": 59.2741,
        "center_lon": 15.2066,
        # Central Örebro + Vivalla / Markbacken / Brickebacken / Sörbyängen
        "bbox":       (59.22, 15.13, 59.32, 15.30),
        "available":  True,
        "population":     140775,
        "building_count": 21804,
    },
    "helsingborg": {
        "name":       "Helsingborg",
        "country":    "SE",
        "kommunkod":  "1283",
        "center_lat": 56.0465,
        "center_lon": 12.6945,
        # Coastal city — long N-S along Öresund. Centrum + Olympia / Drottninghög
        # / Eneborg / Mariastaden / Råå
        "bbox":       (56.00, 12.65, 56.10, 12.79),
        "available":  True,
        "population":     133767,
        "building_count": 22436,
    },
    "norrkoping": {
        "name":       "Norrköping",
        "country":    "SE",
        "kommunkod":  "0581",
        "center_lat": 58.5877,
        "center_lon": 16.1924,
        # Centrum + Hageby / Navestad / Klockaretorpet / Ljura / Lindö
        "bbox":       (58.54, 16.10, 58.64, 16.30),
        "available":  True,
        "population":     115799,
        "building_count": 11578,
    },
    "jonkoping": {
        "name":       "Jönköping",
        "country":    "SE",
        "kommunkod":  "0680",
        "center_lat": 57.7826,
        "center_lon": 14.1618,
        # Stretched along the south shore of Vättern. Centrum + Huskvarna /
        # Råslätt / Öxnehaga / Bankeryd fringe
        "bbox":       (57.73, 14.07, 57.83, 14.26),
        "available":  True,
        "population":     93608,
        "building_count": 9874,
    },
    # ── University / regional capital cities ──────────────────────────────────
    "lund": {
        "name":       "Lund",
        "country":    "SE",
        "kommunkod":  "1281",
        "center_lat": 55.7047,
        "center_lon": 13.1910,
        # Compact university town. Centrum + Norra Fäladen / Klostergården /
        # Vipeholm / Linero / Värpinge
        "bbox":       (55.66, 13.13, 55.74, 13.27),
        "available":  True,
        "population":     121053,
        "building_count": 20756,
    },
    "sundsvall": {
        "name":       "Sundsvall",
        "country":    "SE",
        "kommunkod":  "2281",
        "center_lat": 62.3908,
        "center_lon": 17.3069,
        # Coastal city along Sundsvallsbukten. Centrum + Bosvedjan /
        # Bredsand / Granloholm / Skönsmon / Sidsjö
        "bbox":       (62.34, 17.20, 62.43, 17.42),
        "available":  True,
        "population":     70450,
        "building_count": 17331,
    },
    "umea": {
        "name":       "Umeå",
        "country":    "SE",
        "kommunkod":  "2480",
        "center_lat": 63.8258,
        "center_lon": 20.2630,
        # University city on the Umeälven river. Centrum + Ålidhem /
        # Mariehem / Berghem / Tomtebo / Tegs
        "bbox":       (63.78, 20.18, 63.88, 20.40),
        "available":  True,
        "population":     108973,
        "building_count": 15608,
    },
    # ── New cities (top-20 gap fill) ──────────────────────────────────────
    "boras": {
        "name":       "Boras",
        "country":    "SE",
        "kommunkod":  "1490",
        "center_lat": 57.7210,
        "center_lon": 12.9401,
        # Central Boras + Sjöbo / Hässleholmen / Norrby / Fristad fringe
        "bbox":       (57.67, 12.86, 57.77, 13.04),
        "available":  True,
    },
    "gavle": {
        "name":       "Gavle",
        "country":    "SE",
        "kommunkod":  "2180",
        "center_lat": 60.6749,
        "center_lon": 17.1413,
        # Central Gavle + Sätra / Andersberg / Bomhus / Brynäs / Valbo fringe
        "bbox":       (60.63, 17.05, 60.73, 17.25),
        "available":  True,
    },
    "eskilstuna": {
        "name":       "Eskilstuna",
        "country":    "SE",
        "kommunkod":  "0484",
        "center_lat": 59.3710,
        "center_lon": 16.5092,
        # Central Eskilstuna + Fröslunda / Lagersberg / Skiftinge / Torshälla fringe
        "bbox":       (59.32, 16.42, 59.42, 16.62),
        "available":  True,
    },
    "halmstad": {
        "name":       "Halmstad",
        "country":    "SE",
        "kommunkod":  "1380",
        "center_lat": 56.6745,
        "center_lon": 12.8564,
        # Central Halmstad + Andersberg / Vallås / Kärleken / Fyllinge / Tylösand fringe
        "bbox":       (56.62, 12.76, 56.72, 12.96),
        "available":  True,
    },
    "karlstad": {
        "name":       "Karlstad",
        "country":    "SE",
        "kommunkod":  "1780",
        "center_lat": 59.3793,
        "center_lon": 13.5036,
        # Central Karlstad + Kronoparken / Rud / Våxnäs / Gruvlyckan / Zakrisdal
        "bbox":       (59.33, 13.40, 59.43, 13.60),
        "available":  True,
    },
    "lulea": {
        "name":       "Luleå",
        "country":    "SE",
        "kommunkod":  "2580",
        "center_lat": 65.5842,
        "center_lon": 22.1547,
        # Northernmost major city. Centrum + Bergnäset / Porsön (university) /
        # Gammelstad / Notviken / Björkskatan
        "bbox":       (65.54, 22.05, 65.63, 22.28),
        "available":  True,
        "population":     80299,
        "building_count": 22312,
    },
    "visby": {
        "name":       "Visby",
        "country":    "SE",
        "kommunkod":  "0980",
        "center_lat": 57.6348,
        "center_lon": 18.2948,
        # Island city on Gotland. Visby town + Kneippbyn / Terra Nova /
        # Galgberget / Snäckgärdsbaden. Tight bbox around the urban area
        # since Gotland kommun covers the whole island.
        "bbox":       (57.59, 18.22, 57.67, 18.36),
        "available":  True,
        "population":     60840,
        "building_count": 32808,
    },
    "karlskrona": {
        "name":       "Karlskrona",
        "country":    "SE",
        "kommunkod":  "1080",
        "center_lat": 56.1612,
        "center_lon": 15.5869,
        # Naval base + archipelago ferry hub. Centrum + Saltö / Stumholmen /
        # Trossö / Aspö / Dragsö / Lyckeby
        "bbox":       (56.12, 15.48, 56.21, 15.70),
        "available":  True,
        "population":     66013,
        "building_count": 20026,
    },
    # ── Brazil ────────────────────────────────────────────────────────────────
    # First non-Swedish city. Uses `admin_code` (7-digit IBGE município id)
    # instead of `kommunkod`. Demographics come from IBGE SIDRA table 9514
    # (Censo 2022) + IBGE Malha v3 (municipal polygon). See brazil.py and
    # docs/ADD_CITY.md for how the dispatcher routes BR cities through the
    # same pipeline.
    "recife": {
        "name":       "Recife",
        "country":    "BR",
        "admin_code": "2611606",       # IBGE código do município
        "center_lat": -8.0476,
        "center_lon": -34.8770,
        # (south, west, north, east). Covers Centro, Boa Viagem, Casa
        # Amarela, Várzea, Ibura, and the port area; north edge reaches
        # the Olinda border. Recife's urban footprint is bigger than a
        # typical Swedish mid-city (1.49M residents).
        "bbox":       (-8.15, -34.95, -7.95, -34.80),
        # Hidden from the UI for now via osm.VISIBLE_COUNTRIES (country
        # filter above the CITIES dict). Flip both `available` and the
        # VISIBLE_COUNTRIES set to bring Recife back online — the IBGE
        # data caches, custom builders, and transport schedule remain on
        # disk so re-enabling is instant.
        "available":  False,
        "population":      1488920,     # IBGE Censo 2022
        "building_count":  124647,      # after first pipeline run (municipal polygon filter)
    },
}

# Multiple Overpass mirrors — tried in order on failure. The main API
# (overpass-api.de) is the most up-to-date but frequently overloaded;
# the kumi and Switzerland mirrors are good fallbacks.
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]
DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")


def _city_dir(city_id: str) -> str:
    """Per-city data subfolder, e.g. data/vaxjo/."""
    return os.path.join(DATA_DIR, city_id)


def _city_paths(city_id: str) -> dict[str, str]:
    """Resolve per-city file paths under data/{city_id}/."""
    d = _city_dir(city_id)
    return {
        "osm_raw":         os.path.join(d, "osm_raw.json"),
        "city":            os.path.join(d, "city.json"),
        "transport_infra": os.path.join(d, "transport_infra.json"),
    }

# ── Zone constants (mirrors engine.py Zone enum values) ──────────────────────

_ROAD       = 0
_RES_LO     = 1
_RES_MED    = 2
_RES_HI     = 3
_COMMERCIAL = 4
_INDUSTRIAL = 5
_HOSPITAL   = 6
_PARK       = 7
_SCHOOL     = 8
_PHARMACY   = 9
_UNIVERSITY = 10
_GROCERY    = 11
_DENTAL     = 12
_VETERINARY = 13
_CHURCH     = 14
_CASTLE     = 15
_MUSEUM     = 16
_THEATER    = 17
_STADIUM    = 18
_WATER      = 19
_FORESTRY   = 20
_NIGHTCLUB    = 21
_PLAYGROUND   = 22
_CEMETERY     = 23
_KINDERGARTEN = 24   # Förskola (preschool, ages 1-5)
_HIGH_SCHOOL  = 25   # Gymnasium (upper secondary, ages 16-19)
_LIBRARY      = 26
_RESTAURANT        = 27   # restaurant / café / fast-food
_SPORTS_CENTRE     = 28   # gym / swim hall / ice rink / fitness centre
_HOTEL             = 29   # hotel / hostel / chalet
_COMMUNITY_CENTRE  = 30   # community centre / social hall
_MALL              = 31   # shopping mall / department store
_PARKING           = 32   # parking garage / amenity=parking
_MANOR             = 33   # Swedish herrgård / historic=manor
_HISTORIC_LANDMARK = 34   # historic=building / yes / tourism=attraction
_POLICE            = 35   # amenity=police / building=police
_FIRE_STATION      = 36   # amenity=fire_station / building=fire_station
# Existing _SCHOOL (zone 8) = Grundskola (compulsory, ages 6-15). Kept
# as the default bucket for `amenity=school` without level hints.

# Zones that represent specific POIs — never overridden by node-based POI matching
_SPECIFIC_ZONES = {
    _HOSPITAL, _SCHOOL, _PHARMACY, _UNIVERSITY,
    _GROCERY,  _DENTAL, _VETERINARY, _CHURCH, _CASTLE,
    _MUSEUM,   _THEATER, _STADIUM, _WATER, _FORESTRY,
    _NIGHTCLUB, _PLAYGROUND, _CEMETERY,
    _KINDERGARTEN, _HIGH_SCHOOL, _LIBRARY,
    _RESTAURANT, _SPORTS_CENTRE, _HOTEL, _COMMUNITY_CENTRE,
    _MALL, _PARKING, _MANOR, _HISTORIC_LANDMARK,
    _POLICE, _FIRE_STATION,
}

# Refinement upgrades: when a POI node carries more specific info than
# the way-level classifier was able to extract, allow the node to win
# even though the building is already in _SPECIFIC_ZONES. E.g. a
# generic `amenity=school` polygon becomes SCHOOL (grundskola), but a
# neighbouring node named "Teknikum" (detected as gymnasium) should be
# allowed to upgrade it to HIGH_SCHOOL.
_ZONE_REFINEMENTS: dict[int, frozenset[int]] = {
    _SCHOOL: frozenset({_HIGH_SCHOOL, _KINDERGARTEN}),
    # A historic landmark node carrying a more specific historic tag
    # can upgrade itself — e.g. a historic=manor node landing on a
    # building already tagged historic=building.
    _HISTORIC_LANDMARK: frozenset({_MANOR, _CASTLE, _MUSEUM}),
}

# Natural/landcover zones (polygons that aren't OSM `building=*` ways).
# Loaded from OSM via `natural=water`, `landuse=forest`, `natural=wood`.
_NATURAL_ZONES = frozenset({_WATER, _FORESTRY})

# ── Population density (m² of floor area per person) ─────────────────────────

# NOTE: "pop" = *resident* population (people who live/sleep there).
# Non-residential zones (work/visit destinations) get a very low resident
# count — almost nobody actually lives in a hospital, school, grocery, etc.
# Day-time presence at those buildings is modelled by the spatial kernel
# in engine.py via the surrounding residential population.
_DENSITY_M2: dict[int, int] = {
    _RES_LO:      35,    # detached / low-rise — denser than before
    _RES_MED:     22,    # mid-rise apartments
    _RES_HI:      14,    # dense apartment blocks
    _COMMERCIAL: 600,    # offices/retail — almost zero residents
    _INDUSTRIAL: 800,    # factories — no residents
    _HOSPITAL:   500,    # almost zero residents (only on-call quarters)
    _SCHOOL:    1000,    # no residents
    _PARK:      5000,    # essentially zero
    _PHARMACY:   800,    # no residents
    _UNIVERSITY: 200,    # some dorms but mostly day-time
    _GROCERY:    800,    # no residents
    _DENTAL:     800,    # no residents
    _VETERINARY: 800,    # no residents
    _CHURCH:    2000,    # essentially no residents
    _CASTLE:    1500,    # tourist/event capacity, not residential
    _MUSEUM:     900,    # visitor capacity (no residents)
    _THEATER:    400,    # audience-dense venue
    _STADIUM:    300,    # mass-gathering venue
    # ── Natural features (sparse, "resident" = regular visitor/worker) ────
    _WATER:     2000,    # lake: ~1 bather / swimmer per 2000 m² surface
    _FORESTRY: 10000,    # forest: ~1 worker/hiker per hectare
    # ── Recreation / nightlife ──
    _NIGHTCLUB:   3,     # packed standing crowd — highest density in model
    _PLAYGROUND: 80,     # small outdoor footprint, high child turnover
    # ── Worship / memorial ──
    _CEMETERY:  2000,    # sparse outdoor, occasional funeral gatherings
    # ── Swedish school levels (grundskola stays at _SCHOOL above) ──
    _KINDERGARTEN: 1000, # förskola — small staff
    _HIGH_SCHOOL:  1000, # gymnasium — no residents, day-time only
    _LIBRARY:       300, # visitors packed into reading rooms for hours
    # ── Hospitality / commerce / recreation corrections ──
    _RESTAURANT:        5, # dense dining tables — close contact, long dwell
    _SPORTS_CENTRE:    15, # gym spacing, aerosol-heavy
    _HOTEL:            25, # hotel rooms + lobby / restaurant
    _COMMUNITY_CENTRE: 10, # event hall density
    _MALL:              8, # shopping crowds
    _PARKING:         400, # mostly empty — small staff presence only
    _MANOR:           200, # tourist visits to estate + small staff
    _HISTORIC_LANDMARK: 300, # tourist throughput
    _POLICE:           100, # office staff, some public
    _FIRE_STATION:      80, # crew on shift + vehicles
}

_RES_ZONES = frozenset({_RES_LO, _RES_MED, _RES_HI})

# Highway types we want to keep for the road network (vehicle-bearing)
_ROAD_HIGHWAY_TYPES = frozenset({
    "motorway", "trunk", "primary", "secondary", "tertiary",
    "unclassified", "residential", "service", "living_street",
    "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link",
})

# ── Overpass download ─────────────────────────────────────────────────────────

def download_osm_data(city_id: str) -> list[dict]:
    """
    Fetch buildings (ways), roads (highway ways), and POI amenity/shop nodes
    for city_id from Overpass API. Combined into a single request.

    Cached to data/{city_id}/osm_raw.json.
    """
    os.makedirs(_city_dir(city_id), exist_ok=True)
    cache_path = _city_paths(city_id)["osm_raw"]

    if os.path.exists(cache_path):
        print(f"[osm] Using cached data: {cache_path}")
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    city = CITIES[city_id]

    # Expand bbox to municipality bounds if DeSO data is available,
    # so the Overpass download covers the full kommun area.  The
    # _filter_buildings_by_boundary() step later clips to the actual
    # polygon — the bbox just needs to be large enough.
    s, w, n, e = city["bbox"]
    try:
        boundary, _ = _load_municipality_boundary(
            city_id, city["center_lat"], city["center_lon"])
        if boundary is not None:
            minx, miny, maxx, maxy = boundary.bounds
            # Use municipality bounds but keep a 0.01° margin
            s = min(s, miny - 0.01)
            w = min(w, minx - 0.01)
            n = max(n, maxy + 0.01)
            e = max(e, maxx + 0.01)
            print(f"[osm] Expanded bbox to municipality bounds: "
                  f"({s:.4f}, {w:.4f}, {n:.4f}, {e:.4f})")
    except Exception:
        pass  # Fall back to original bbox

    bbox = f"{s},{w},{n},{e}"

    query = (
        f"[out:json][timeout:600];\n"
        f"(\n"
        f'  way["building"]({bbox});\n'
        # Historic castles / ruins that aren't tagged with `building=*`.
        # Kronobergs slottsruin (way 54048626) is the canonical example:
        # `historic=castle` + `ruins=yes`, no building tag. Picked up
        # downstream by classify_zone() → CASTLE (zone 15).
        f'  way["historic"="castle"]({bbox});\n'
        f'  way["highway"~"^(motorway|trunk|primary|secondary|tertiary|'
        f'unclassified|residential|service|living_street|motorway_link|'
        f'trunk_link|primary_link|secondary_link|tertiary_link)$"]({bbox});\n'
        f'  node["amenity"~"^(pharmacy|dentist|veterinary|university|college|'
        f'school|kindergarten|hospital|clinic|nightclub|library)$"]({bbox});\n'
        f'  way["amenity"="library"]({bbox});\n'
        f'  node["shop"~"^(supermarket|convenience|grocery)$"]({bbox});\n'
        # ── Recreation / nightlife (full zones) ──
        # Playgrounds + sports pitches map to leisure=* ways, handled by
        # classify_zone. Nightclubs are node-only POIs that the node-POI
        # matching pass snaps onto the nearest building.
        f'  way["leisure"="playground"]({bbox});\n'
        f'  way["amenity"="nightclub"]({bbox});\n'
        # ── Transport infrastructure ──
        f'  node["aeroway"="aerodrome"]({bbox});\n'
        f'  node["railway"~"^(station|halt|tram_stop)$"]({bbox});\n'
        f'  node["public_transport"="station"]({bbox});\n'
        f'  node["amenity"="bus_station"]({bbox});\n'
        f'  node["highway"="bus_stop"]({bbox});\n'
        f'  node["amenity"="ferry_terminal"]({bbox});\n'
        f'  way["aeroway"~"^(runway|taxiway)$"]({bbox});\n'
        f'  way["aeroway"="terminal"]({bbox});\n'
        f'  way["aeroway"="aerodrome"]({bbox});\n'
        f'  way["railway"="rail"]({bbox});\n'
        f'  way["railway"="tram"]({bbox});\n'
        # ── Natural features ──
        # Lakes / rivers / ponds. Big lakes in OSM (Växjösjön, Södra
        # Bergundasjön, …) are stored as multipolygon RELATIONS rather
        # than single closed ways, so we fetch both and stitch the
        # relation outer rings in _load_buildings().
        f'  way["natural"="water"]({bbox});\n'
        f'  relation["natural"="water"]({bbox});\n'
        # Rivers and riverbanks: riverbank ways are already closed
        # polygons; waterway=river ways are open polylines and get
        # buffered into a polygon strip downstream.
        f'  way["waterway"="riverbank"]({bbox});\n'
        f'  way["waterway"="river"]({bbox});\n'
        # Forestry — `landuse=forest` is managed/planted forestry, while
        # `natural=wood` is natural woodland. Both map to FORESTRY and use
        # whatever species tags the way carries (leaf_type/genus/species).
        f'  way["landuse"="forest"]({bbox});\n'
        f'  way["natural"="wood"]({bbox});\n'
        f'  relation["landuse"="forest"]({bbox});\n'
        # ── Lightweight nature POIs (pins only, no epidemic impact) ──
        # These are extracted by _load_nature_pois() and surfaced as a
        # separate `nature_pois` list in the layout payload — they do
        # NOT become buildings, don't enter the SEIR kernel, and don't
        # touch _DENSITY_M2. We accept both ways and relations so large
        # protected areas (multipolygon relations) come through too.
        f'  way["leisure"="nature_reserve"]({bbox});\n'
        f'  relation["leisure"="nature_reserve"]({bbox});\n'
        f'  way["boundary"="protected_area"]({bbox});\n'
        f'  relation["boundary"="protected_area"]({bbox});\n'
        f'  way["landuse"="cemetery"]({bbox});\n'
        f'  relation["landuse"="cemetery"]({bbox});\n'
        f'  node["amenity"="grave_yard"]({bbox});\n'
        f'  way["amenity"="grave_yard"]({bbox});\n'
        f");\n"
        # `out geom;` defaults to body verbosity, which includes tags AND
        # relation member lists + their geometries. The older `out geom
        # tags;` dropped members from the response, so big lakes stored
        # as multipolygon relations (Växjösjön, S. Bergundasjön, …) came
        # back with member_count=0 and couldn't be stitched into rings.
        f"out geom;"
    )

    print(f"[osm] Downloading OSM data for {city['name']} (combined query) …")
    encoded = urllib.parse.urlencode({"data": query}).encode()

    # Try each mirror in order, with one short retry on transient failure.
    # Overpass commonly returns 429 / 504 / connection-reset under load,
    # and the public mirrors take turns being healthy. We don't backoff
    # aggressively — just rotate to the next mirror after a short pause.
    last_err: Exception | None = None
    result: dict | None = None
    for url in OVERPASS_URLS:
        for attempt in (1, 2):
            try:
                req = urllib.request.Request(
                    url, data=encoded,
                    headers={"User-Agent": "EpiCity/1.0"},
                )
                with urllib.request.urlopen(req, timeout=300) as resp:
                    result = json.loads(resp.read())
                    print(f"[osm]   ok via {url}")
                    break
            except Exception as e:
                last_err = e
                print(f"[osm]   {type(e).__name__} from {url} (attempt {attempt}): {e}")
                if attempt == 1:
                    import time
                    time.sleep(8)
        if result is not None:
            break
    if result is None:
        raise RuntimeError(
            f"All Overpass mirrors failed for {city_id}. Last error: {last_err}"
        )

    elements = result.get("elements", [])
    print(f"[osm] Downloaded {len(elements)} raw elements.")

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(elements, f)

    return elements

# ── Geometry helpers ──────────────────────────────────────────────────────────

def _compute_centroid(geometry: list[dict]) -> tuple[float, float]:
    """Simple centroid (average of polygon vertices)."""
    lats = [g["lat"] for g in geometry]
    lons = [g["lon"] for g in geometry]
    return sum(lats) / len(lats), sum(lons) / len(lons)


def _polygon_area_m2(geometry: list[dict], center_lat: float) -> float:
    """Approximate polygon area in m² using the shoelace formula in local metres."""
    if len(geometry) < 3:
        return 0.0
    lat0, lon0 = geometry[0]["lat"], geometry[0]["lon"]
    mpl  = 111_320.0
    mplx = 111_320.0 * math.cos(math.radians(center_lat))

    xs = [(g["lon"] - lon0) * mplx for g in geometry]
    ys = [(g["lat"] - lat0) * mpl  for g in geometry]

    n    = len(xs)
    area = 0.0
    for i in range(n):
        j     = (i + 1) % n
        area += xs[i] * ys[j] - xs[j] * ys[i]
    return abs(area) / 2.0

# ── Zone classification (way-level) ───────────────────────────────────────────

def _is_swedish_gymnasium(tags: dict) -> bool:
    """
    Heuristic for detecting a Swedish `gymnasium` (upper secondary,
    ages 16-19) vs. a regular `grundskola`. OSM tags schools very
    inconsistently in Sweden, so we combine multiple signals:

      • `isced:level` contains "3" (ISCED level 3 = upper secondary,
        the authoritative signal — but rarely tagged in OSM)
      • `school:type` / `school:level` contains "gymnasium" /
        "upper_secondary" / "secondary"
      • the name contains the substring "gymnasi" (case-insensitive)
        — catches both Swedish forms "Gymnasium" (indefinite) and
        "Gymnasiet" (definite), e.g. "LBS Kreativa Gymnasiet"
      • the name matches one of a small list of historically famous
        Swedish gymnasiums that don't carry the word in their name
        ("Katedralskolan", "Teknikum", "Kungsmadskolan", …)
      • `grades` tag contains "gy", "10", "11" or "12" (Swedish
        upper-secondary year labels)

    NOTE: `building=gymnasium` is deliberately NOT a signal — that OSM
    tag follows the English meaning (sports hall / gym) and would
    produce many false positives.
    """
    isced = (tags.get("isced:level", "") or "").strip()
    if "3" in isced.split(";"):
        return True
    school_type = (tags.get("school:type", "") or "").lower()
    if any(kw in school_type for kw in ("gymnasium", "upper_secondary", "secondary")):
        return True
    school_level = (tags.get("school:level", "") or "").lower()
    if any(kw in school_level for kw in ("gymnasium", "upper_secondary", "secondary")):
        return True
    name = (tags.get("name:sv") or tags.get("name") or "").lower()
    # Substring match catches "Gymnasium", "Gymnasiet", "Gymnasieskolan".
    if "gymnasi" in name:
        return True
    # Hardcoded keywords for historic Swedish gymnasiums whose names
    # predate the word and are widely known to be upper secondary.
    _GYM_NAMES = (
        "katedralskol",   # Katedralskolan (Växjö, Lund, Uppsala, Linköping)
        "teknikum",       # Växjö
        "kungsmadskol",   # Växjö
    )
    if any(kw in name for kw in _GYM_NAMES):
        return True
    grades = (tags.get("grades") or tags.get("grade") or "").lower()
    if "gy" in grades or "10" in grades or "11" in grades or "12" in grades:
        return True
    return False


def classify_zone(tags: dict, levels: int) -> int:
    """Map OSM building/way tags to a Zone integer."""
    amenity  = tags.get("amenity", "")
    building = tags.get("building", "").lower()
    leisure  = tags.get("leisure", "")
    landuse  = tags.get("landuse", "")
    shop     = tags.get("shop", "")
    tourism  = tags.get("tourism", "")
    natural  = tags.get("natural", "")
    historic = tags.get("historic", "")

    # ── Natural features (checked first — these are never buildings) ──────
    # Closed water polygons: lakes, ponds, reservoirs, riverbanks. Open-
    # water rivers are buffered into synthetic polygons upstream with
    # `natural=water`, so they hit this branch too.
    waterway = tags.get("waterway", "")
    if natural == "water" or waterway == "riverbank":
        return _WATER
    # Forestry — managed planted forest OR natural woodland. Both land in
    # the FORESTRY zone; species granularity comes from leaf_type/genus/
    # species tags handled by _extract_tree_species(), not the zone id.
    if landuse == "forest" or natural == "wood":
        return _FORESTRY

    # Specific POIs (high priority — checked first)
    # Nightclub — node-matched in most cases, but some are `way` + either
    # `amenity=nightclub` or `building=nightclub`.
    if amenity == "nightclub" or building == "nightclub":
        return _NIGHTCLUB
    # Playground — `leisure=playground` way polygons (picnic slot / slide
    # area). Small, open, high child turnover.
    if leisure == "playground":
        return _PLAYGROUND
    # Cemetery — `landuse=cemetery` way polygons + some `amenity=grave_yard`.
    # Funeral gatherings cluster here; grouped with churches in the POI
    # menu since it's the natural counterpart to worship services.
    if landuse == "cemetery" or amenity == "grave_yard":
        return _CEMETERY
    if amenity == "pharmacy":
        return _PHARMACY
    if amenity == "dentist":
        return _DENTAL
    if amenity == "veterinary":
        return _VETERINARY
    if amenity == "police" or building == "police":
        return _POLICE
    if amenity == "fire_station" or building == "fire_station":
        return _FIRE_STATION
    if amenity in ("university", "college") or building in ("university", "college"):
        return _UNIVERSITY
    # Library — public bibliotek. Promoted out of the generic
    # commercial bucket so it appears in the Education POI group
    # alongside schools and universities.
    if amenity == "library" or building == "library":
        return _LIBRARY
    if shop in ("supermarket", "convenience", "grocery"):
        return _GROCERY

    # ── Hospitality / commerce / recreation corrections ──
    # These previously fell through to the commercial catch-all
    # below. Extracting them gives each its own POI category so
    # users can filter dining / sports / hotels / malls separately
    # and the SEIR β can be tuned per-venue.
    #
    # Restaurant bundle — amenity=restaurant|cafe|fast_food|pub|bar
    # were all landing in COMMERCIAL. Indoor dining is close-contact
    # and long-dwell, deserving its own higher β.
    if amenity in ("restaurant", "cafe", "fast_food",
                   "pub", "bar", "food_court", "biergarten",
                   "ice_cream") \
            or building == "restaurant":
        return _RESTAURANT

    # Sports centre bundle — gyms, swim halls, ice rinks, fitness
    # centres. Aerosol-heavy indoor mixing, currently all commercial.
    if leisure in ("sports_centre", "sports_hall", "fitness_centre",
                   "swimming_pool", "ice_rink", "fitness_station") \
            or building in ("sports_centre", "sports_hall"):
        return _SPORTS_CENTRE

    # Hotel bundle — overnight stays. Includes Swedish stugby
    # (chalet) and hostels.
    if tourism in ("hotel", "hostel", "guest_house", "motel") \
            or building == "hotel":
        return _HOTEL

    # Community centre — indoor group events.
    if amenity == "community_centre" or building == "community_centre":
        return _COMMUNITY_CENTRE

    # Shopping mall — big destinations, high foot traffic.
    if shop == "mall" or building == "mall":
        return _MALL

    # Parking — multi-storey garages + designated surface lots
    # tagged as buildings. amenity=parking without a building tag
    # isn't currently fetched by Overpass, so only building=parking
    # and way=building with amenity=parking get caught here.
    if building == "parking" or amenity == "parking":
        return _PARKING

    # Swedish manor / herrgård — historic=manor OR historic=castle
    # with castle_type=manor. Previously fell through to residential
    # because the CASTLE branch above explicitly excludes manors.
    if historic == "manor" or tags.get("castle_type") in ("manor", "stately"):
        return _MANOR

    # Generic historic landmark — catches historic=building / yes
    # and tourism=attraction on building ways that would otherwise
    # be residential. Lower priority than manor + castle above, so
    # those still win when both tags are present.
    if historic in ("building", "yes") or tourism == "attraction":
        return _HISTORIC_LANDMARK

    # Hospital
    if amenity in ("hospital", "clinic", "doctors") or building == "hospital":
        return _HOSPITAL

    # Place of worship — churches, cathedrals, mosques, temples, synagogues.
    # Large gathering venues with distinctive epidemic dynamics (weekly
    # services, funerals, weddings) — break them out from generic commercial.
    _worship_buildings = (
        "church", "chapel", "cathedral", "mosque", "temple", "synagogue",
        "monastery", "religious", "shrine", "presbytery",
    )
    if building in _worship_buildings or amenity == "place_of_worship":
        return _CHURCH

    # Castles / fortresses — tourist attractions + event venues with their
    # own epidemic rhythm. Deliberately excludes MANORS (herrgårdar), which
    # OSM tags as `historic=manor` or `castle_type=manor`; manors are a
    # distinct Swedish building type and shouldn't be bucketed as castles.
    # The overrides file can still force individual buildings into any
    # zone + custom 3D asset. (`historic` is declared at the top of the
    # function so the MANOR/HISTORIC_LANDMARK branches below can use it.)
    castle_type = tags.get("castle_type", "")
    if building in ("castle", "fortress") \
            or (historic in ("castle", "fort", "fortress")
                and castle_type not in ("manor", "stately")):
        return _CASTLE

    # Museums — tourist destinations with slow, diffuse all-day traffic.
    # Distinct from commercial because visitors concentrate in a single
    # building for hours rather than short retail trips.
    if tourism == "museum" or amenity == "museum" or building == "museum":
        return _MUSEUM

    # Theaters / concert halls / opera houses / cinemas / arts centres —
    # audience-dense venues with 2-3 h concentrated sessions.
    if amenity in ("theatre", "concert_hall", "arts_centre",
                   "cinema", "opera_house", "events_venue") \
            or building in ("theatre", "opera_house", "cinema"):
        return _THEATER

    # Stadiums — mass-gathering sports venues (hockey, football, athletics).
    # Deliberately narrow: only `leisure=stadium` and dedicated
    # stadium/grandstand building tags. Gyms, swimming pools, and
    # multi-purpose sports centres stay in COMMERCIAL.
    if leisure == "stadium" or building in ("stadium", "grandstand"):
        return _STADIUM

    # Swedish school levels — split `amenity=school` + `kindergarten`
    # into three buckets:
    #   • förskola (kindergarten, ages 1-5)   → _KINDERGARTEN
    #   • grundskola (compulsory, ages 6-15)  → _SCHOOL (default)
    #   • gymnasium (upper secondary, 16-19)  → _HIGH_SCHOOL
    if amenity == "kindergarten" or building == "kindergarten":
        return _KINDERGARTEN
    if amenity == "school" or building == "school":
        return _HIGH_SCHOOL if _is_swedish_gymnasium(tags) else _SCHOOL

    # Park (rare for ways tagged as building)
    if leisure == "park" or landuse == "park" or building == "greenhouse":
        return _PARK

    # Industrial — heavy footprint, low resident count.
    # Outbuildings (garage, shed, hangar, carport, ruins…) get bucketed here
    # too: they aren't habitable, so the simulation should treat them like a
    # warehouse, not like a low-rise home.
    if building in ("industrial", "warehouse", "storage_tank", "manufacture",
                    "factory", "barn", "farm_auxiliary",
                    "garage", "garages", "carport", "shed", "hangar",
                    "service", "ruins", "construction", "roof", "bridge"):
        return _INDUSTRIAL

    # Commercial / office / civic / hospitality — anything that's a
    # destination people *visit* but don't sleep in. Note: theaters,
    # cinemas, concert halls, museums, and stadiums are routed to their
    # dedicated zones ABOVE and deliberately excluded from these lists.
    # Commercial catch-all. Note that restaurants/cafés/pubs,
    # sports centres, hotels, community centres, malls, parking
    # garages, manors and historic landmarks have all been lifted
    # out of this bucket above — they live in their own zones now.
    commercial_buildings = (
        "commercial", "retail", "office", "supermarket", "kiosk", "shop",
        "civic", "public", "government", "train_station",
        "transportation", "ferry_terminal", "fire_station",
        "pavilion", "gymnasium", "exhibition_centre",
    )
    commercial_amenities = (
        "bank", "atm", "post_office", "post_depot",
        "marketplace", "townhall", "courthouse",
        "fire_station", "police",
        "social_facility", "social_centre", "conference_centre",
        "fuel", "car_wash", "car_rental", "car_repair",
    )
    if (building in commercial_buildings
            or amenity in commercial_amenities
            or tourism in ("apartment", "chalet")
            or leisure in ("pitch", "track", "horse_riding", "bowling_alley")
            # Any shop tag we didn't recognise above is still retail.
            or (shop and shop != "no")):
        return _COMMERCIAL

    # Residential by levels
    if building in ("apartments", "dormitory", "residential", "block_of_flats"):
        return _RES_HI if levels >= 4 else (_RES_MED if levels >= 2 else _RES_LO)

    if building in ("house", "detached", "semidetached_house",
                    "terrace", "bungalow", "cabin", "cottage", "farm"):
        return _RES_LO

    # ── Catch-all: any remaining building with a non-residential purpose
    # tag (amenity, office, craft, healthcare, tourism, leisure) that
    # wasn't caught above → Commercial. Only truly untagged buildings
    # (building=yes with no purpose tag) fall through to residential.
    if (tags.get("amenity") or tags.get("office") or tags.get("craft")
            or tags.get("healthcare") or tags.get("industrial")):
        return _COMMERCIAL

    if building in ("yes", ""):
        if levels >= 5:   return _RES_HI
        elif levels >= 2: return _RES_MED
        else:             return _RES_LO

    return _RES_LO


def _classify_poi_node(tags: dict) -> int | None:
    """Map a standalone amenity/shop NODE to a specific POI zone, or None."""
    amenity = tags.get("amenity", "")
    shop    = tags.get("shop", "")

    if amenity == "pharmacy":   return _PHARMACY
    if amenity == "dentist":    return _DENTAL
    if amenity == "veterinary": return _VETERINARY
    if amenity in ("university", "college"): return _UNIVERSITY
    if amenity == "library":    return _LIBRARY
    if amenity == "kindergarten": return _KINDERGARTEN
    if amenity == "school":
        return _HIGH_SCHOOL if _is_swedish_gymnasium(tags) else _SCHOOL
    if amenity in ("hospital", "clinic"):     return _HOSPITAL
    if amenity == "nightclub":                return _NIGHTCLUB
    if amenity == "police" or building == "police":       return _POLICE
    if amenity == "fire_station" or building == "fire_station": return _FIRE_STATION
    if shop in ("supermarket", "convenience", "grocery"): return _GROCERY
    return None


def estimate_population(zone: int, area_m2: float, levels: int) -> int:
    total_area = area_m2 * max(1, levels)
    density    = _DENSITY_M2.get(zone, 30)
    return max(1, round(total_area / density))


# ── Tree species extraction (for FORESTRY polygons) ───────────────────────────

# Canonical dominant-species labels used downstream by the frontend
# `forestry_<species>` custom builders in buildings_custom.js. Keep this
# list in sync with the registered CUSTOM_BUILDERS entries.
_TREE_SPECIES: frozenset[str] = frozenset({
    "spruce", "pine", "birch", "oak", "mixed",
})

# Average stem spacing (m² of ground per tree) used both for the displayed
# tree count and for downstream capacity reporting. Values come from
# Swedish forestry practice: dense spruce plantations run ~2500 stems/ha,
# pine ~1600, birch ~1100, oak ~400. "Mixed" uses a weighted average.
_TREE_SPACING_M2: dict[str, float] = {
    "spruce": 4.0,
    "pine":   6.0,
    "birch":  9.0,
    "oak":   25.0,
    "mixed":  6.0,
}

# Latin genus → canonical species bucket
_GENUS_TO_SPECIES: dict[str, str] = {
    "picea":  "spruce",   # Gran
    "pinus":  "pine",     # Tall
    "betula": "birch",    # Björk
    "quercus": "oak",     # Ek
}


def _extract_tree_species(tags: dict) -> str:
    """
    Pick a canonical dominant tree species bucket from OSM tags.

    Priority order:
      1. `genus`  — most authoritative when present (e.g. "Picea" → spruce)
      2. `species` — Latin binomial prefix (e.g. "Picea abies" → spruce)
      3. `wood`   — older OSM tagging ("conifer"/"deciduous"/"mixed")
      4. `leaf_type` — common in Sweden:
           needleleaved → spruce (dominant in Swedish conifer stands)
           broadleaved  → birch  (dominant in Swedish broadleaf stands)
           mixed        → mixed
      5. Fallback: "mixed"
    """
    def _norm(v: str) -> str:
        return v.strip().lower()

    genus = _norm(tags.get("genus", ""))
    if genus:
        head = genus.split()[0] if genus else ""
        if head in _GENUS_TO_SPECIES:
            return _GENUS_TO_SPECIES[head]

    species = _norm(tags.get("species", ""))
    if species:
        head = species.split()[0]
        if head in _GENUS_TO_SPECIES:
            return _GENUS_TO_SPECIES[head]

    wood = _norm(tags.get("wood", ""))
    if wood in ("coniferous", "conifer", "needleleaved"):
        return "spruce"
    if wood in ("deciduous", "broadleaved"):
        return "birch"
    if wood == "mixed":
        return "mixed"

    leaf_type = _norm(tags.get("leaf_type", ""))
    if leaf_type == "needleleaved":
        return "spruce"
    if leaf_type == "broadleaved":
        return "birch"
    if leaf_type == "mixed":
        return "mixed"

    return "mixed"


def _forestry_tree_count(area_m2: float, species: str) -> int:
    """Estimated stem count for a forestry polygon, capped to int."""
    spacing = _TREE_SPACING_M2.get(species, 6.0)
    return max(1, int(round(area_m2 / spacing)))

# ── Coordinate conversion ─────────────────────────────────────────────────────

def latlon_to_meters(lat: float, lon: float,
                     center_lat: float, center_lon: float) -> tuple[float, float]:
    """
    Convert (lat, lon) to local (world_x, world_z) in metres from city centre.
        world_x  — positive = East
        world_z  — positive = South  (matches Three.js Z+ in the map layer)
    """
    mpl  = 111_320.0
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    return (lon - center_lon) * mplx, -(lat - center_lat) * mpl

# ── POI node → building matching ──────────────────────────────────────────────

def _match_pois_to_buildings(buildings: list[dict],
                             poi_nodes: list[dict],
                             center_lat: float,
                             center_lon: float) -> int:
    """
    For each amenity/shop node, find the nearest building centroid within 50 m
    using a 100-m grid hash. Override the building's zone unless it's already
    a specific POI.  Returns the number of overrides applied.
    """
    CELL = 100.0
    grid: dict[tuple[int, int], list[dict]] = {}
    for b in buildings:
        key = (int(b["world_x"] // CELL), int(b["world_z"] // CELL))
        grid.setdefault(key, []).append(b)

    overrides = 0
    for node in poi_nodes:
        new_zone = _classify_poi_node(node.get("tags", {}))
        if new_zone is None:
            continue

        nx, nz = latlon_to_meters(node["lat"], node["lon"], center_lat, center_lon)
        cx = int(nx // CELL)
        cz = int(nz // CELL)

        nearest = None
        best_d2 = 50.0 * 50.0   # 50 m max distance
        for dcx in (-1, 0, 1):
            for dcz in (-1, 0, 1):
                for b in grid.get((cx + dcx, cz + dcz), []):
                    d2 = (b["world_x"] - nx) ** 2 + (b["world_z"] - nz) ** 2
                    if d2 < best_d2:
                        best_d2 = d2
                        nearest = b

        if nearest:
            current = nearest["zone"]
            allow_override = (
                current not in _SPECIFIC_ZONES
                or new_zone in _ZONE_REFINEMENTS.get(current, frozenset())
            )
            if allow_override:
                nearest["zone"] = new_zone
                # Non-residential zones keep pop=0 (no residents at
                # schools / nightclubs / libraries). Only a downgrade
                # to a residential bucket would re-populate — which
                # is blocked by _SPECIFIC_ZONES anyway.
                if new_zone in _RES_ZONES:
                    nearest["pop"] = estimate_population(
                        new_zone, nearest["area_m2"], nearest["levels"]
                    )
                else:
                    nearest["pop"] = 0
                overrides += 1

    return overrides

# ── Transport infrastructure extraction ───────────────────────────────────────
#
# Extracts the discrete things vehicles depart from and routes they travel on:
#   * stops  — point locations (airports, train stations, tram stops, bus stops)
#   * routes — polylines (runways for plane animation, rail and tram polylines
#              that trains and trams visually follow)
#
# This data is *separate* from buildings — stops aren't building zones, they're
# spawn points for the visible 3D vehicles in the Transport layer. The output
# lives in data/<city>/transport_infra.json (NOT folded into city.json) so the
# building/road payload stays small and the schedule generator + frontend can
# fetch it independently.

# Map raw OSM tag → normalized stop type used by the rest of the pipeline.
def _stop_type_from_tags(tags: dict) -> str | None:
    if tags.get("aeroway") == "aerodrome":
        return "airport"
    railway = tags.get("railway", "")
    if railway in ("station", "halt"):
        # Distinguish train stations from tram terminals when both tags present.
        if tags.get("station") == "tram" or tags.get("tram") == "yes":
            return "tram"
        return "train"
    if railway == "tram_stop":
        return "tram"
    if tags.get("amenity") == "ferry_terminal":
        return "ferry"
    if tags.get("amenity") == "bus_station":
        return "bus_station"
    if tags.get("highway") == "bus_stop":
        return "bus"
    if tags.get("public_transport") == "station":
        # Generic station tag — try to disambiguate by other tags
        if tags.get("ferry") == "yes":
            return "ferry"
        if tags.get("bus") == "yes":
            return "bus_station"
        if tags.get("train") == "yes":
            return "train"
        if tags.get("tram") == "yes":
            return "tram"
        return "bus_station"  # safe default
    return None


def _extract_transport_nodes(elements: list[dict],
                             center_lat: float, center_lon: float) -> list[dict]:
    """
    Pull point-located transport infrastructure from raw OSM elements.

    Walks both nodes (bus stops, tram stops, train stations) AND ways
    (aerodrome polygons → centroid stop) so airports tagged as polygons —
    which is most Swedish airports — produce a single airport stop at the
    polygon centroid.

    Returns a list of stop dicts with shape:
        {idx, type, name, ref, lat, lon, world_x, world_z}
    `idx` is dense over the returned list (use it as the stop_id elsewhere).
    """
    stops: list[dict] = []

    # ── Node-based stops (bus, tram, train, occasional point-airport) ──
    for el in elements:
        if el.get("type") != "node":
            continue
        tags = el.get("tags") or {}
        stype = _stop_type_from_tags(tags)
        if stype is None:
            continue
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue
        wx, wz = latlon_to_meters(lat, lon, center_lat, center_lon)
        stops.append({
            "idx":     len(stops),
            "type":    stype,
            "name":    tags.get("name") or tags.get("ref") or "",
            "ref":     tags.get("ref") or "",
            "lat":     round(float(lat), 7),
            "lon":     round(float(lon), 7),
            "world_x": round(wx, 1),
            "world_z": round(wz, 1),
        })

    # ── Way-based airport stops ──
    #
    # Two passes so terminal-based positions take precedence over aerodrome
    # polygon centroids (terminals are the actual building, aerodromes can
    # centre in the middle of a runway). After both passes, dedupe so an
    # aerodrome that's within ~1 km of an existing terminal-based airport
    # stop is dropped — that's the same airport seen twice.
    # 2 km is the rule of thumb for "same airport" — Hässlö in Västerås has
    # the terminal building ~1.1 km from the aerodrome polygon centroid, so
    # a smaller threshold misses the dedupe. The two real airports inside
    # Västerås (Hässlö ↔ Johannisbergs) are ~7 km apart so they survive.
    DEDUPE_M = 2000.0
    DEDUPE_M2 = DEDUPE_M * DEDUPE_M

    def _add_way_stop(el: dict, stop_type: str) -> None:
        tags = el.get("tags") or {}
        geometry = el.get("geometry") or []
        if len(geometry) < 3:
            return
        lat, lon = _compute_centroid(geometry)
        wx, wz = latlon_to_meters(lat, lon, center_lat, center_lon)
        # Dedupe against existing same-type airport stops
        if stop_type == "airport":
            for existing in stops:
                if existing["type"] != "airport":
                    continue
                d2 = (existing["world_x"] - wx) ** 2 + (existing["world_z"] - wz) ** 2
                if d2 < DEDUPE_M2:
                    return
        stops.append({
            "idx":     len(stops),
            "type":    stop_type,
            "name":    tags.get("name") or tags.get("ref") or "",
            "ref":     tags.get("ref") or "",
            "lat":     round(lat, 7),
            "lon":     round(lon, 7),
            "world_x": round(wx, 1),
            "world_z": round(wz, 1),
        })

    # Pass 1: terminals (most accurate airport anchor)
    for el in elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags") or {}
        if tags.get("aeroway") == "terminal":
            _add_way_stop(el, "airport")

    # Pass 2: aerodrome polygons (skipped if a terminal already covered it)
    for el in elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags") or {}
        if tags.get("aeroway") == "aerodrome":
            _add_way_stop(el, "airport")

    # Pass 3: rare railway=station / public_transport=station polygons
    for el in elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags") or {}
        if (tags.get("railway") == "station"
                or tags.get("public_transport") == "station"):
            _add_way_stop(el, "train")

    return stops


def _extract_transport_routes(elements: list[dict],
                              center_lat: float, center_lon: float) -> list[dict]:
    """
    Pull linear transport infrastructure from raw OSM elements as polylines
    in local metres. Returns a list of route dicts with shape:
        {idx, type, polyline: [[world_x, world_z], ...]}

    Types: "runway", "taxiway", "rail", "tram", "aerodrome"
    The "aerodrome" type carries the airport polygon so the runway fallback
    in _synthesize_runway_from_aerodrome() can use it when no runway way
    exists for that airport.
    """
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0
    routes: list[dict] = []

    def _project(geometry: list[dict]) -> list[list[float]]:
        return [
            [
                round((g["lon"] - center_lon) * mplx, 1),
                round(-(g["lat"] - center_lat) * mpl, 1),
            ]
            for g in geometry
            if "lat" in g and "lon" in g
        ]

    for el in elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags") or {}
        rtype: str | None = None
        if tags.get("aeroway") in ("runway", "taxiway", "aerodrome"):
            rtype = tags["aeroway"]
        elif tags.get("railway") == "rail":
            rtype = "rail"
        elif tags.get("railway") == "tram":
            rtype = "tram"
        if rtype is None:
            continue
        geometry = el.get("geometry") or []
        if len(geometry) < 2:
            continue
        polyline = _project(geometry)
        if len(polyline) < 2:
            continue
        routes.append({
            "idx":      len(routes),
            "type":     rtype,
            "polyline": polyline,
        })
    return routes


def _synthesize_runway_from_aerodrome(aerodrome_polygon: list[list[float]]
                                      ) -> list[list[float]] | None:
    """
    For airports without an explicit runway way, synthesize a runway axis as
    the longest edge of the oriented bounding box of the aerodrome polygon.
    Used as a fallback so plane takeoff/landing always has a sensible direction
    even at small Swedish airports that lack `way["aeroway"="runway"]`.

    Returns a [start, end] polyline in world coordinates, or None if the
    polygon is too small/degenerate to use.
    """
    pts = aerodrome_polygon
    if len(pts) < 3:
        return None

    # Compute centroid
    cx = sum(p[0] for p in pts) / len(pts)
    cz = sum(p[1] for p in pts) / len(pts)

    # Covariance matrix → principal axis (no numpy, keep it tight)
    sxx = sum((p[0] - cx) ** 2 for p in pts)
    szz = sum((p[1] - cz) ** 2 for p in pts)
    sxz = sum((p[0] - cx) * (p[1] - cz) for p in pts)
    n   = len(pts)
    sxx /= n; szz /= n; sxz /= n

    # Eigenvector of the larger eigenvalue gives principal axis direction
    trace = sxx + szz
    det   = sxx * szz - sxz * sxz
    disc  = max(0.0, trace * trace / 4 - det)
    lam1  = trace / 2 + math.sqrt(disc)
    if abs(sxz) > 1e-9:
        ax_x = lam1 - szz
        ax_z = sxz
    else:
        ax_x, ax_z = (1.0, 0.0) if sxx >= szz else (0.0, 1.0)
    norm = math.hypot(ax_x, ax_z) or 1.0
    ax_x /= norm; ax_z /= norm

    # Project all polygon vertices onto the principal axis to find the extent
    projs = [(p[0] - cx) * ax_x + (p[1] - cz) * ax_z for p in pts]
    pmin, pmax = min(projs), max(projs)
    if pmax - pmin < 50:   # too small to be a real runway (< 50 m)
        return None

    start = [round(cx + pmin * ax_x, 1), round(cz + pmin * ax_z, 1)]
    end   = [round(cx + pmax * ax_x, 1), round(cz + pmax * ax_z, 1)]
    return [start, end]


def load_transport(city_id: str) -> dict[str, Any]:
    """
    Build (or load from cache) the transport infrastructure bundle for a city.

    Output shape, written to data/<city>/transport_infra.json:
        {
          "version": 1,
          "city_id": "vasteras",
          "stops":   [{idx, type, name, ref, lat, lon, world_x, world_z}, ...],
          "routes":  [{idx, type, polyline: [[wx, wz], ...]}, ...]
        }

    The cache is regenerated by re-running tools/download_city.py. The runtime
    engine and frontend never call into Overpass — they read this file.
    """
    if city_id not in CITIES:
        raise ValueError(f"Unknown city: {city_id!r}")

    os.makedirs(_city_dir(city_id), exist_ok=True)
    out_path = _city_paths(city_id)["transport_infra"]
    if os.path.exists(out_path):
        with open(out_path, encoding="utf-8") as f:
            return json.load(f)

    city       = CITIES[city_id]
    center_lat = city["center_lat"]
    center_lon = city["center_lon"]

    # Reuse the existing OSM cache — never re-hit Overpass for this. If the
    # cache predates the transport tag bump it just won't contain transport
    # nodes; the extraction returns empty lists and the feature degrades.
    elements = download_osm_data(city_id)

    stops  = _extract_transport_nodes(elements, center_lat, center_lon)
    routes = _extract_transport_routes(elements, center_lat, center_lon)

    # Runway fallback: every airport stop should have at least one runway
    # polyline somewhere nearby. If no runway way exists at all, derive one
    # from the aerodrome polygon's principal axis so plane animations have a
    # direction to take off / land along.
    has_runway = any(r["type"] == "runway" for r in routes)
    if not has_runway:
        aerodrome_polys = [r["polyline"] for r in routes if r["type"] == "aerodrome"]
        for poly in aerodrome_polys:
            synth = _synthesize_runway_from_aerodrome(poly)
            if synth is not None:
                routes.append({
                    "idx":      len(routes),
                    "type":     "runway",
                    "polyline": synth,
                })

    # Download extended-range rail data (~50km radius) so train lines
    # follow real rail corridors well beyond the municipality border.
    wide_rail = download_wide_rail(city_id)
    if wide_rail:
        routes.extend(wide_rail)

    # Download wide-area airport data (~30km radius) so major airports
    # in neighbouring municipalities are included (e.g. Landvetter for
    # Goteborg, Arlanda for Stockholm, Sturup for Malmo).
    wide_ap_stops, wide_ap_routes = download_wide_airports(city_id)
    if wide_ap_routes:
        routes.extend(wide_ap_routes)
    if wide_ap_stops:
        # Deduplicate: skip wide airports that are within 2000m of an
        # existing airport stop (already downloaded via municipality bbox)
        existing_airports = [(s["world_x"], s["world_z"])
                             for s in stops if s["type"] == "airport"]
        for ws in wide_ap_stops:
            too_close = False
            for ex, ez in existing_airports:
                if math.hypot(ws["world_x"] - ex, ws["world_z"] - ez) < 2000:
                    too_close = True
                    break
            if not too_close:
                ws["idx"] = len(stops)
                stops.append(ws)
                existing_airports.append((ws["world_x"], ws["world_z"]))

    # Download real OSM bus + ferry route relations (separate Overpass queries)
    bus_routes = download_bus_routes(city_id)
    ferry_routes = download_ferry_routes(city_id)

    bundle = {
        "version": 1,
        "city_id": city_id,
        "stops":   stops,
        "routes":  routes,
    }
    if bus_routes:
        bundle["bus_routes"] = bus_routes
    if ferry_routes:
        bundle["ferry_routes"] = ferry_routes

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, separators=(",", ":"))

    counts: dict[str, int] = {}
    for s in stops:
        counts[s["type"]] = counts.get(s["type"], 0) + 1
    rcounts: dict[str, int] = {}
    for r in routes:
        rcounts[r["type"]] = rcounts.get(r["type"], 0) + 1
    print(f"[osm] transport infra for {city_id}: stops={counts} routes={rcounts}")
    if bus_routes:
        print(f"[osm] real bus routes: {len(bus_routes)} OSM relations")
    if ferry_routes:
        print(f"[osm] real ferry routes: {len(ferry_routes)} OSM relations")

    return bundle


def download_bus_routes(city_id: str) -> list[dict]:
    """
    Download real bus route relations from OSM for a city.

    Returns a list of routes, each containing:
        {
          "ref":   "3",              # bus line number
          "name":  "Borås C - ...",  # route name
          "stops": [[lon, lat], ...],# ordered stop positions
        }

    Uses the expanded municipality bbox. Cached to
    data/{city_id}/bus_routes_raw.json.
    """
    if city_id not in CITIES:
        return []

    cache_path = os.path.join(_city_dir(city_id), "bus_routes_raw.json")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    city = CITIES[city_id]
    s, w, n, e = city["bbox"]
    # Expand to municipality bounds
    try:
        boundary, _ = _load_municipality_boundary(
            city_id, city["center_lat"], city["center_lon"])
        if boundary is not None:
            minx, miny, maxx, maxy = boundary.bounds
            s = min(s, miny - 0.01)
            w = min(w, minx - 0.01)
            n = max(n, maxy + 0.01)
            e = max(e, maxx + 0.01)
    except Exception:
        pass

    bbox = f"{s},{w},{n},{e}"

    # Query: get all bus route relations with their stop members resolved
    query = (
        f"[out:json][timeout:120];\n"
        f'relation["route"="bus"]({bbox});\n'
        f"out body;\n"
        f">;\n"
        f"out skel qt;\n"
    )

    print(f"[osm] Downloading OSM bus routes for {city['name']} …")
    data = None
    for url in OVERPASS_URLS:
        try:
            req = urllib.request.Request(
                url, data=query.encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read())
            print(f"[osm]   ok via {url}")
            break
        except Exception as ex:
            print(f"[osm]   failed {url}: {ex}")
            import time; time.sleep(5)

    if not data:
        print(f"[osm]   WARNING: could not download bus routes")
        return []

    elements = data.get("elements", [])

    # Index nodes and ways by ID
    nodes = {}
    ways = {}
    relations = []
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = el
        elif el["type"] == "way":
            ways[el["id"]] = el
        elif el["type"] == "relation":
            relations.append(el)

    center_lat = city["center_lat"]
    center_lon = city["center_lon"]
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0

    routes = []
    for rel in relations:
        tags = rel.get("tags", {})
        ref  = tags.get("ref", "")
        name = tags.get("name", "")
        members = rel.get("members", [])

        # Extract ordered stop positions (nodes with role "stop" or
        # "platform", or nodes that are part of the route)
        stop_coords = []
        way_node_ids = []

        for m in members:
            if m["type"] == "node" and m.get("role", "") in ("stop", "platform", ""):
                nd = nodes.get(m["ref"])
                if nd and "lat" in nd and "lon" in nd:
                    stop_coords.append([nd["lon"], nd["lat"]])
            elif m["type"] == "way":
                w = ways.get(m["ref"])
                if w and "nodes" in w:
                    way_node_ids.extend(w["nodes"])

        # Build polyline from ways (road geometry the bus follows)
        way_coords = []
        for nid in way_node_ids:
            nd = nodes.get(nid)
            if nd and "lat" in nd and "lon" in nd:
                wx = (nd["lon"] - center_lon) * mplx
                wz = -(nd["lat"] - center_lat) * mpl
                way_coords.append([round(wx, 1), round(wz, 1)])

        # Convert stop coords to world coordinates
        stop_world = []
        for lon, lat in stop_coords:
            wx = (lon - center_lon) * mplx
            wz = -(lat - center_lat) * mpl
            stop_world.append([round(wx, 1), round(wz, 1)])

        if len(stop_world) >= 2 or len(way_coords) >= 2:
            route = {"ref": ref, "name": name}
            if stop_world:
                route["stops"] = stop_world
            if way_coords:
                route["polyline"] = way_coords
            routes.append(route)

    # Cache
    os.makedirs(_city_dir(city_id), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(routes, f, separators=(",", ":"))

    return routes


def download_ferry_routes(city_id: str) -> list[dict]:
    """
    Download ferry route relations from OSM for a city.

    Returns a list of routes with stop positions and polylines in world
    coordinates, like ``download_bus_routes`` but for ``route=ferry``
    relations.  Cached to ``data/{city_id}/ferry_routes_raw.json``.
    """
    if city_id not in CITIES:
        return []

    cache_path = os.path.join(_city_dir(city_id), "ferry_routes_raw.json")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    city = CITIES[city_id]
    s, w, n, e = city["bbox"]
    # Expand to municipality bounds
    try:
        boundary, _ = _load_municipality_boundary(
            city_id, city["center_lat"], city["center_lon"])
        if boundary is not None:
            minx, miny, maxx, maxy = boundary.bounds
            s = min(s, miny - 0.02)
            w = min(w, minx - 0.02)
            n = max(n, maxy + 0.02)
            e = max(e, maxx + 0.02)
    except Exception:
        pass

    bbox = f"{s},{w},{n},{e}"

    query = (
        f"[out:json][timeout:120];\n"
        f'relation["route"="ferry"]({bbox});\n'
        f"out body;\n"
        f">;\n"
        f"out skel qt;\n"
    )

    print(f"[osm] Downloading OSM ferry routes for {city['name']} …")
    data = None
    for url in OVERPASS_URLS:
        try:
            req = urllib.request.Request(
                url, data=query.encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read())
            print(f"[osm]   ok via {url}")
            break
        except Exception as ex:
            print(f"[osm]   failed {url}: {ex}")
            import time; time.sleep(5)

    if not data:
        print(f"[osm]   WARNING: could not download ferry routes")
        return []

    elements = data.get("elements", [])

    nodes = {}
    ways = {}
    relations = []
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = el
        elif el["type"] == "way":
            ways[el["id"]] = el
        elif el["type"] == "relation":
            relations.append(el)

    center_lat = city["center_lat"]
    center_lon = city["center_lon"]
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0

    routes = []
    for rel in relations:
        tags = rel.get("tags", {})
        ref  = tags.get("ref", "")
        name = tags.get("name", "")
        operator = tags.get("operator", "")
        members = rel.get("members", [])

        stop_coords = []
        way_node_ids = []

        for m in members:
            if m["type"] == "node" and m.get("role", "") in ("stop", "platform", ""):
                nd = nodes.get(m["ref"])
                if nd and "lat" in nd and "lon" in nd:
                    stop_coords.append([nd["lon"], nd["lat"]])
            elif m["type"] == "way":
                w_el = ways.get(m["ref"])
                if w_el and "nodes" in w_el:
                    way_node_ids.extend(w_el["nodes"])

        # Build polyline from ways (water route geometry)
        way_coords = []
        for nid in way_node_ids:
            nd = nodes.get(nid)
            if nd and "lat" in nd and "lon" in nd:
                wx = (nd["lon"] - center_lon) * mplx
                wz = -(nd["lat"] - center_lat) * mpl
                way_coords.append([round(wx, 1), round(wz, 1)])

        # Convert stop coords to world coordinates
        stop_world = []
        for lon, lat in stop_coords:
            wx = (lon - center_lon) * mplx
            wz = -(lat - center_lat) * mpl
            stop_world.append([round(wx, 1), round(wz, 1)])

        if len(stop_world) >= 2 or len(way_coords) >= 2:
            route = {"ref": ref, "name": name}
            if operator:
                route["operator"] = operator
            if stop_world:
                route["stops"] = stop_world
            if way_coords:
                route["polyline"] = way_coords
            routes.append(route)

    os.makedirs(_city_dir(city_id), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(routes, f, separators=(",", ":"))

    print(f"[osm]   {len(routes)} ferry route relations")
    return routes


def download_wide_rail(city_id: str,
                       radius_deg_lat: float = 0.45,
                       radius_deg_lon: float = 0.90) -> list[dict]:
    """
    Download rail lines from a wide area (~50km radius) around the city
    so train routes follow real OSM rail corridors well beyond the
    municipality border — matching what's visible on the base map.

    Returns route dicts compatible with the existing routes list:
        [{idx, type: "rail", polyline: [[wx, wz], ...]}, ...]

    Cached to data/{city_id}/wide_rail_raw.json.
    """
    if city_id not in CITIES:
        return []

    cache_path = os.path.join(_city_dir(city_id), "wide_rail_raw.json")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    city = CITIES[city_id]
    clat = city["center_lat"]
    clon = city["center_lon"]

    s = clat - radius_deg_lat
    n = clat + radius_deg_lat
    w = clon - radius_deg_lon
    e = clon + radius_deg_lon
    bbox = f"{s},{w},{n},{e}"

    query = (
        f"[out:json][timeout:60];\n"
        f'way["railway"="rail"]({bbox});\n'
        f"(._;>;);\n"
        f"out body;\n"
    )

    print(f"[osm] Downloading wide-area rail for {city['name']} "
          f"(~{radius_deg_lat*111:.0f}km radius) …")
    data = None
    for url in OVERPASS_URLS:
        try:
            req = urllib.request.Request(
                url, data=query.encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
            print(f"[osm]   ok via {url}")
            break
        except Exception as ex:
            print(f"[osm]   failed {url}: {ex}")
            import time; time.sleep(5)

    if not data:
        print("[osm]   WARNING: could not download wide-area rail")
        return []

    elements = data.get("elements", [])
    nodes = {el["id"]: el for el in elements if el["type"] == "node"}
    ways = [el for el in elements if el["type"] == "way"]

    mplx = 111_320.0 * math.cos(math.radians(clat))
    mpl  = 111_320.0

    routes = []
    idx_base = 10000  # high idx to avoid collisions with existing routes
    for w_el in ways:
        pts = []
        for nid in w_el.get("nodes", []):
            nd = nodes.get(nid)
            if nd and "lat" in nd:
                wx = (nd["lon"] - clon) * mplx
                wz = -(nd["lat"] - clat) * mpl
                pts.append([round(wx, 1), round(wz, 1)])
        if len(pts) >= 2:
            routes.append({
                "idx":      idx_base + len(routes),
                "type":     "rail",
                "polyline": pts,
            })

    print(f"[osm]   {len(routes)} rail segments from {len(ways)} ways")

    os.makedirs(_city_dir(city_id), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(routes, f, separators=(",", ":"))

    return routes


def download_wide_airports(city_id: str,
                           radius_deg_lat: float = 0.40,
                           radius_deg_lon: float = 0.80) -> tuple[list[dict], list[dict]]:
    """
    Download airport infrastructure from a ~30km radius around the city
    so major airports in neighbouring municipalities are included.

    Returns (stops, routes) where:
        stops  = [{idx, type:"airport", name, lat, lon, world_x, world_z}]
        routes = [{idx, type:"runway"|"taxiway", polyline}]

    Cached to data/{city_id}/wide_airports_raw.json.
    """
    if city_id not in CITIES:
        return [], []

    cache_path = os.path.join(_city_dir(city_id), "wide_airports_raw.json")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            cached = json.load(f)
        return cached.get("stops", []), cached.get("routes", [])

    city = CITIES[city_id]
    clat = city["center_lat"]
    clon = city["center_lon"]

    s = clat - radius_deg_lat
    n = clat + radius_deg_lat
    w = clon - radius_deg_lon
    e = clon + radius_deg_lon
    bbox = f"{s},{w},{n},{e}"

    query = (
        f"[out:json][timeout:30];\n"
        f"(\n"
        f'  node["aeroway"="aerodrome"]({bbox});\n'
        f'  way["aeroway"~"^(aerodrome|terminal|runway|taxiway)$"]({bbox});\n'
        f");\n"
        f"out geom;\n"
    )

    print(f"[osm] Downloading wide-area airports for {city['name']} "
          f"(~{radius_deg_lat*111:.0f}km radius) …")
    data = None
    for url in OVERPASS_URLS:
        try:
            req = urllib.request.Request(
                url, data=query.encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            print(f"[osm]   ok via {url}")
            break
        except Exception as ex:
            print(f"[osm]   failed {url}: {ex}")
            import time; time.sleep(5)

    if not data:
        print("[osm]   WARNING: could not download wide-area airports")
        return [], []

    # Extract using the same logic as _extract_transport_nodes/routes
    elements = data.get("elements", [])
    stops = _extract_transport_nodes(elements, clat, clon)
    stops = [s for s in stops if s["type"] == "airport"]

    routes = _extract_transport_routes(elements, clat, clon)
    routes = [r for r in routes
              if r["type"] in ("runway", "taxiway", "aerodrome")]

    print(f"[osm]   {len(stops)} airport stops, "
          f"{len(routes)} runway/taxiway routes")

    os.makedirs(_city_dir(city_id), exist_ok=True)
    bundle = {"stops": stops, "routes": routes}
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, separators=(",", ":"))

    return stops, routes


# ── Country dispatch helpers ──────────────────────────────────────────────────
#
# EpiCity is multi-country capable: Swedish cities use SCB demographics keyed
# by `kommunkod`, Brazilian cities use IBGE demographics keyed by `admin_code`
# (7-digit IBGE município id). These helpers centralise the country lookup so
# no caller has to open-code `CITIES[city_id].get("country", "SE")`.

def _country_of(city_id: str) -> str:
    """Two-letter country code for a city_id. Defaults to 'SE' for legacy entries."""
    return CITIES.get(city_id, {}).get("country", "SE")


def _admin_code_of(city_id: str) -> str | None:
    """
    Administrative code for a city — `kommunkod` for Swedish cities,
    `admin_code` for everywhere else. One helper so callers don't have to
    know which field applies.
    """
    meta = CITIES.get(city_id, {})
    return meta.get("admin_code") or meta.get("kommunkod")


# Per-country filenames for the sub-municipal boundary geojson cache.
# Swedish = SCB DeSO polygons. Brazilian = IBGE setor censitário polygons
# (fetched by brazil.py). Both are unioned into one municipality boundary
# downstream.
_BOUNDARY_SRC_FILENAME = {
    "SE": "deso_geo_raw.geojson",
    "BR": "bairros_geo_raw.geojson",
}


# ── Municipality boundary from sub-municipal polygon union ────────────────────

def _load_municipality_boundary(city_id: str, center_lat: float, center_lon: float):
    """
    Build the municipality border polygon by unioning all sub-municipal
    polygons from the country-specific boundary source (DeSO for SE,
    setor/bairro for BR).
    Returns (shapely_polygon, border_lonlat_list) or (None, None) if
    shapely/data not available.  border_lonlat_list is a list of [lon, lat]
    rings suitable for GeoJSON / frontend rendering.
    """
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union
    except ImportError:
        return None, None

    country  = _country_of(city_id)
    filename = _BOUNDARY_SRC_FILENAME.get(country, "deso_geo_raw.geojson")
    deso_path = os.path.join(_city_dir(city_id), filename)
    if not os.path.exists(deso_path):
        return None, None

    with open(deso_path, encoding="utf-8") as f:
        geo = json.load(f)

    polys = []
    for feat in geo.get("features", []):
        try:
            polys.append(shape(feat["geometry"]))
        except Exception:
            continue
    if not polys:
        return None, None

    boundary = unary_union(polys)

    # Extract exterior ring(s) as [lon, lat] lists for the frontend
    rings = []
    if boundary.geom_type == "Polygon":
        rings.append([[round(c[0], 6), round(c[1], 6)]
                      for c in boundary.exterior.coords])
    elif boundary.geom_type == "MultiPolygon":
        for poly in boundary.geoms:
            rings.append([[round(c[0], 6), round(c[1], 6)]
                          for c in poly.exterior.coords])

    return boundary, rings


def _filter_buildings_by_boundary(buildings: list[dict], boundary) -> list[dict]:
    """Keep only buildings whose centroid falls inside the municipality polygon.

    Water features (zone 19) are exempted from the boundary filter so that
    lakes on the outskirts remain visible even outside the municipality
    border — useful for ferry route context and natural-looking coastlines.
    """
    from shapely.geometry import Point
    kept = []
    idx = 0
    for b in buildings:
        if b["zone"] == _WATER or boundary.contains(Point(b["lon"], b["lat"])):
            b["idx"] = idx
            kept.append(b)
            idx += 1
    removed = len(buildings) - len(kept)
    if removed:
        print(f"[osm] Municipality filter: kept {len(kept)}, "
              f"removed {removed} buildings outside boundary")
    return kept


def _filter_roads_by_boundary(roads: list[dict], boundary,
                              center_lat: float, center_lon: float) -> list[dict]:
    """Keep only roads whose midpoint falls inside the municipality polygon."""
    from shapely.geometry import Point
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0
    kept = []
    rid = 0
    for r in roads:
        pts = r["points"]
        mid = len(pts) // 2
        # Convert world coords back to lon/lat
        wx, wz = pts[mid]
        lon = wx / mplx + center_lon
        lat = -wz / mpl + center_lat
        if boundary.contains(Point(lon, lat)):
            r["id"] = rid
            kept.append(r)
            rid += 1
    removed = len(roads) - len(kept)
    if removed:
        print(f"[osm] Municipality filter: kept {len(kept)}, "
              f"removed {removed} roads outside boundary")
    return kept


# ── Demographics dispatcher ───────────────────────────────────────────────────

def _attach_demographics(buildings: list[dict], city_id: str) -> None:
    """
    Country-aware wrapper around _attach_scb_demographics.

    - Swedish cities: fetch SCB DeSO bundle via `scb.load_deso()` and attach.
      Log the same `[osm] SCB demographics skipped: ...` message on failure
      that existed before this dispatcher was introduced, so Swedish runs
      produce byte-identical city.json output.
    - Brazilian cities: fetch IBGE bairro/setor bundle via
      `brazil.load_bairros()`. Same downstream attach — the two bundles
      share the {areas: [{deso, polygon, total, age}]} schema.
    - Unknown countries: skipped with a warning (buildings keep
      `_DEFAULT_AGE_FRAC` as fallback via the engine loader).
    """
    country = _country_of(city_id)
    if country == "SE":
        try:
            import scb
            _attach_scb_demographics(buildings, scb.load_deso(city_id))
        except Exception as e:
            print(f"[osm] SCB demographics skipped: {type(e).__name__}: {e}")
        # Synthetic population distribution: income, gender, households,
        # and neighborhood names — per-building variation that preserves
        # area-level aggregates.
        try:
            import scb
            _attach_synthetic_demographics(buildings, city_id)
        except Exception as e:
            print(f"[osm] Synthetic demographics skipped: {type(e).__name__}: {e}")
        return

    if country == "BR":
        try:
            import brazil
            _attach_scb_demographics(buildings, brazil.load_bairros(city_id))
        except Exception as e:
            print(f"[osm] IBGE demographics skipped: {type(e).__name__}: {e}")
        return

    print(f"[osm] No demographics source for country {country!r} — "
          f"buildings will use default age fractions.")


# ── SCB demographic attach ────────────────────────────────────────────────────

def _attach_scb_demographics(buildings: list[dict], bundle: dict) -> None:
    """
    Redistribute residential populations so their per-DeSO sums match SCB,
    and stamp {deso, age_frac, age} on every building inside a DeSO polygon.

    - Residential buildings get `pop` rescaled by (SCB_total / local_sum).
    - Non-residential buildings keep their token resident count unchanged.
    - All buildings inside the polygon get the local SCB age mix so the UI
      tooltip can show realistic numbers for visitors/workers too.
    """
    from shapely.geometry import shape, Point
    from shapely.strtree  import STRtree

    areas = bundle.get("areas", [])
    if not areas:
        print("[osm] SCB bundle empty — skipping demographics attach.")
        return

    # Build shapely polygons + index
    polys: list[Any] = []
    area_recs: list[dict] = []
    for a in areas:
        geom = {"type": a.get("gtype", "Polygon"), "coordinates": a["polygon"]}
        try:
            polys.append(shape(geom))
            area_recs.append(a)
        except Exception:
            continue
    if not polys:
        return

    tree = STRtree(polys)

    # Point-in-polygon for every building centroid
    inside_res:  list[list[dict]] = [[] for _ in polys]
    inside_all:  list[list[dict]] = [[] for _ in polys]
    matched = 0
    for b in buildings:
        pt = Point(b["lon"], b["lat"])
        for idx in tree.query(pt):
            if polys[idx].contains(pt):
                inside_all[idx].append(b)
                if b["zone"] in _RES_ZONES:
                    inside_res[idx].append(b)
                matched += 1
                break

    rescaled_total = 0
    for idx, area in enumerate(area_recs):
        total = int(area.get("total", 0))
        if total <= 0:
            continue

        res = inside_res[idx]
        if res:
            # Distribute DeSO population proportionally to livable floor
            # area (area_m2 × levels). Largest-remainder ensures exact total.
            weights = [b.get("area_m2", 50) * max(1, b.get("levels", 1))
                       for b in res]
            w_sum = sum(weights)
            if w_sum > 0:
                exact = [w / w_sum * total for w in weights]
            else:
                exact = [total / len(res)] * len(res)
            floors = [max(1, int(v)) for v in exact]
            gap = total - sum(floors)
            if gap > 0:
                remainders = [(exact[i] - floors[i], i) for i in range(len(res))]
                remainders.sort(reverse=True)
                for j in range(min(gap, len(remainders))):
                    floors[remainders[j][1]] += 1
            elif gap < 0:
                excess = -gap
                order = sorted(range(len(res)), key=lambda i: floors[i], reverse=True)
                for i in order:
                    if excess <= 0:
                        break
                    trim = min(floors[i] - 1, excess)
                    floors[i] -= trim
                    excess -= trim
            for b, p in zip(res, floors):
                b["pop"] = p
            rescaled_total += total

        age = area.get("age", {"child": 0, "adult": 0, "elder": 0})
        age_total = max(1, sum(age.values()))
        frac = {k: v / age_total for k, v in age.items()}

        for b in inside_all[idx]:
            b["deso"] = area["deso"]
            # Age/population data only for residential buildings
            if b["zone"] in _RES_ZONES:
                b["age_frac"] = frac
                pop = b["pop"]
                age_exact = {k: pop * frac[k] for k in frac}
                age_floors = {k: int(v) for k, v in age_exact.items()}
                age_gap = pop - sum(age_floors.values())
                if age_gap > 0:
                    by_rem = sorted(frac.keys(),
                                    key=lambda k: age_exact[k] - age_floors[k],
                                    reverse=True)
                    for k in by_rem[:age_gap]:
                        age_floors[k] += 1
                b["age"] = age_floors

    # Fallback age mix for residential buildings outside all DeSO polygons
    city_frac = bundle.get("meta", {}).get(
        "age_frac", {"child": 0.18, "adult": 0.62, "elder": 0.20},
    )
    no_deso = 0
    for b in buildings:
        if b["zone"] in _RES_ZONES and "age_frac" not in b:
            b["deso"]     = None
            b["age_frac"] = city_frac
            b["age"] = {k: max(0, round(b["pop"] * city_frac[k])) for k in city_frac}
            no_deso += 1

    print(f"[osm] SCB attach: {matched} buildings in DeSO, "
          f"{no_deso} fallback. Residential pop after rescale: "
          f"{rescaled_total:,}")


# ── Synthetic per-building demographics ──────────────────────────────────────

def _attach_synthetic_demographics(buildings: list[dict],
                                   city_id: str) -> None:
    """
    Distribute area-level demographic aggregates to individual residential
    buildings with realistic per-building variation.  After summing all
    buildings in a DeSO, the area-level totals are preserved.

    Handles: income, gender, household structure, and neighborhood names.
    Reads only from cached data files — no API calls.
    """
    import scb
    import numpy as np

    rng = np.random.default_rng(seed=hash(city_id) & 0xFFFFFFFF)

    # ── Neighborhood names (straight lookup, no variation needed) ─────────
    name_map = {}
    try:
        name_map = scb.load_regso_names(city_id) or {}
        if name_map:
            attached = 0
            for b in buildings:
                d = b.get("deso")
                if d and d in name_map:
                    b["neighborhood"] = name_map[d]
                    attached += 1
            print(f"[osm] RegSO neighborhood names: attached to "
                  f"{attached} buildings.")
    except Exception as e:
        print(f"[osm] RegSO names skipped: {type(e).__name__}: {e}")

    # ── Group residential buildings by DeSO ───────────────────────────────
    from collections import defaultdict
    by_deso: dict[str, list[dict]] = defaultdict(list)
    for b in buildings:
        d = b.get("deso")
        if d and b["zone"] in _RES_ZONES:
            by_deso[d].append(b)

    # ── Income: log-normal variation preserving DeSO pop-weighted mean ────
    try:
        income_map = scb.load_deso_income(city_id) or {}
        if income_map:
            attached = 0
            # CV varies by zone: detached houses have wider income spread
            zone_cv = {_RES_LO: 0.12, _RES_MED: 0.08, _RES_HI: 0.05}
            for deso_id, mean_inc in income_map.items():
                res = by_deso.get(deso_id, [])
                if not res or mean_inc <= 0:
                    continue
                pops = np.array([b["pop"] for b in res], dtype=float)
                total_pop = pops.sum()
                if total_pop <= 0:
                    continue
                # Draw raw log-normal samples per building
                cvs = np.array([zone_cv.get(b["zone"], 0.22) for b in res])
                sigma = np.sqrt(np.log(1 + cvs**2))
                mu = np.log(mean_inc) - 0.5 * sigma**2
                raw = rng.lognormal(mu, sigma)
                # Rescale so pop-weighted mean matches the DeSO mean exactly
                weighted_mean = np.dot(pops, raw) / total_pop
                raw *= mean_inc / weighted_mean
                for b, inc in zip(res, raw):
                    b["income"] = round(float(inc), 1)
                    attached += 1
            print(f"[osm] Synthetic income: {attached} residential buildings "
                  f"across {len(income_map)} DeSOs (pop-weighted mean preserved).")
    except Exception as e:
        print(f"[osm] Synthetic income skipped: {type(e).__name__}: {e}")

    # ── Gender: binomial draw preserving DeSO total male/female counts ────
    try:
        gender_map = scb.load_deso_gender(city_id) or {}
        if gender_map:
            attached = 0
            for deso_id, fracs in gender_map.items():
                male_frac = fracs.get("male_frac", 0.5)
                res = by_deso.get(deso_id, [])
                if not res:
                    continue
                pops = np.array([b["pop"] for b in res], dtype=int)
                total_pop = int(pops.sum())
                if total_pop <= 0:
                    continue
                # Draw male counts from Binomial(pop_i, male_frac) per building
                male_counts = rng.binomial(pops, male_frac)
                # Adjust to hit exact DeSO total: target_males = round(total_pop * male_frac)
                target_males = round(total_pop * male_frac)
                diff = target_males - int(male_counts.sum())
                # Distribute the difference among random buildings
                if diff != 0:
                    idxs = rng.choice(len(res), size=min(abs(diff), len(res)),
                                      replace=False)
                    for i in idxs:
                        if diff > 0 and male_counts[i] < pops[i]:
                            male_counts[i] += 1
                            diff -= 1
                        elif diff < 0 and male_counts[i] > 0:
                            male_counts[i] -= 1
                            diff += 1
                for b, mc, p in zip(res, male_counts, pops):
                    p = max(1, int(p))
                    mf = float(mc) / p
                    b["gender_frac"] = {
                        "male_frac":   round(mf, 4),
                        "female_frac": round(1 - mf, 4),
                    }
                    b["gender"] = {"male": int(mc), "female": p - int(mc)}
                    attached += 1
            print(f"[osm] Synthetic gender: {attached} residential buildings "
                  f"(DeSO male/female totals preserved).")
    except Exception as e:
        print(f"[osm] Synthetic gender skipped: {type(e).__name__}: {e}")

    # ── Households: distribute kommun size distribution to buildings ───────
    try:
        hh = scb.load_households(city_id)
        if hh and hh.get("size_distribution"):
            avg = hh["avg_household_size"]
            size_dist = hh["size_distribution"]  # {"1": N, "2": N, ...}
            total_hh = hh.get("total_households", 1)
            # Build probability weights for each household size
            sizes = sorted(int(k) for k in size_dist.keys())
            probs = np.array([size_dist[str(s)] for s in sizes], dtype=float)
            probs /= probs.sum()

            # Zone-based bias: RES_LO → shift toward larger households,
            # RES_HI → shift toward smaller (apartments have more singles)
            zone_shift = {_RES_LO: 0.6, _RES_MED: 0.0, _RES_HI: -0.4}
            attached = 0
            for deso_id, res in by_deso.items():
                for b in res:
                    pop = b["pop"]
                    if pop <= 0:
                        continue
                    shift = zone_shift.get(b["zone"], 0.0)
                    # Shift the distribution by adjusting log-probabilities
                    shifted_logp = np.log(probs + 1e-10) + shift * np.arange(len(sizes))
                    shifted_p = np.exp(shifted_logp)
                    shifted_p /= shifted_p.sum()
                    # Draw households for this building
                    est_hh = max(1, round(pop / avg))
                    drawn_sizes = rng.choice(sizes, size=est_hh, p=shifted_p)
                    bld_avg = float(drawn_sizes.mean())
                    # Recompute household count to be consistent with pop
                    est_hh_adj = max(1, round(pop / bld_avg)) if bld_avg > 0 else est_hh
                    b["household_avg_size"] = round(bld_avg, 2)
                    b["households"] = est_hh_adj
                    # Size breakdown for this building
                    unique, counts = np.unique(drawn_sizes, return_counts=True)
                    b["household_sizes"] = {str(int(s)): int(c) for s, c in zip(unique, counts)}
                    attached += 1
            print(f"[osm] Synthetic households: {attached} residential buildings "
                  f"(kommun size distribution preserved).")
    except Exception as e:
        print(f"[osm] Synthetic households skipped: {type(e).__name__}: {e}")


# ── Road extraction ───────────────────────────────────────────────────────────

def load_roads(elements: list[dict],
               center_lat: float, center_lon: float) -> list[dict]:
    """Extract highway ways from raw OSM elements as polylines in local metres."""
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0
    roads: list[dict] = []
    rid = 0
    for el in elements:
        if el.get("type") != "way":
            continue
        tags    = el.get("tags", {})
        highway = tags.get("highway", "")
        if highway not in _ROAD_HIGHWAY_TYPES:
            continue
        geometry = el.get("geometry", [])
        if len(geometry) < 2:
            continue
        points = [
            [
                round((g["lon"] - center_lon) * mplx, 1),
                round(-(g["lat"] - center_lat) * mpl, 1),
            ]
            for g in geometry
        ]
        roads.append({"id": rid, "type": highway, "points": points})
        rid += 1
    return roads

# ── Building extraction ───────────────────────────────────────────────────────

def _stitch_outer_rings(
        members: list[dict],
) -> list[list[dict]]:
    """
    Stitch a multipolygon relation's `outer` member ways into closed rings.

    OSM stores a natural=water / landuse=forest multipolygon as one
    relation whose outer role is often split across several ways that
    need to be connected end-to-end. Inner holes are dropped here — the
    simulation treats the outer silhouette as the polygon and doesn't
    model islands inside a lake.

    Returns a list of rings; each ring is a list of `{lat, lon}` dicts
    matching the raw Overpass geometry format, so the downstream code
    can reuse `_compute_centroid` / `_polygon_area_m2` unchanged.
    """
    # Collect outer way geometries; drop ways with no/short geometry.
    pending: list[list[dict]] = []
    for m in members:
        if m.get("role") != "outer" or m.get("type") != "way":
            continue
        geo = m.get("geometry") or []
        if len(geo) < 2:
            continue
        pending.append(list(geo))

    rings: list[list[dict]] = []
    eps = 1e-9

    def _same(a: dict, b: dict) -> bool:
        return abs(a["lat"] - b["lat"]) < eps and abs(a["lon"] - b["lon"]) < eps

    while pending:
        ring = pending.pop(0)
        # Keep trying to extend the ring until it closes or we run out.
        grew = True
        while grew and not _same(ring[0], ring[-1]):
            grew = False
            for i, cand in enumerate(pending):
                if _same(ring[-1], cand[0]):
                    ring.extend(cand[1:])
                    pending.pop(i); grew = True; break
                if _same(ring[-1], cand[-1]):
                    ring.extend(list(reversed(cand))[1:])
                    pending.pop(i); grew = True; break
                if _same(ring[0], cand[-1]):
                    ring = cand[:-1] + ring
                    pending.pop(i); grew = True; break
                if _same(ring[0], cand[0]):
                    ring = list(reversed(cand))[:-1] + ring
                    pending.pop(i); grew = True; break
        # Only emit successfully closed rings; orphans are skipped.
        if len(ring) >= 4 and _same(ring[0], ring[-1]):
            rings.append(ring)
    return rings


def _buffer_line_to_polygon(
        geometry: list[dict], half_width_deg: float,
) -> list[dict]:
    """
    Turn an open polyline (waterway=river) into a closed polygon by
    inflating it perpendicular to each segment.

    `half_width_deg` is the half-width in degrees — callers pick a
    value based on the river's width tag, defaulting to a few metres.
    Returns a list of `{lat, lon}` dicts matching the raw Overpass format.
    """
    if len(geometry) < 2:
        return []
    # Compute perpendicular offsets for each segment, then walk one side
    # forward and the other side backward to form a closed ring.
    left:  list[dict] = []
    right: list[dict] = []
    for i, p in enumerate(geometry):
        if i == 0:
            nxt = geometry[i + 1]
            dx, dy = nxt["lon"] - p["lon"], nxt["lat"] - p["lat"]
        elif i == len(geometry) - 1:
            prv = geometry[i - 1]
            dx, dy = p["lon"] - prv["lon"], p["lat"] - prv["lat"]
        else:
            prv, nxt = geometry[i - 1], geometry[i + 1]
            dx, dy = nxt["lon"] - prv["lon"], nxt["lat"] - prv["lat"]
        mag = math.hypot(dx, dy) or 1.0
        # Perpendicular (rotate 90°): (-dy, dx) normalised.
        nx, ny = -dy / mag, dx / mag
        left.append({"lat": p["lat"] + ny * half_width_deg,
                      "lon": p["lon"] + nx * half_width_deg})
        right.append({"lat": p["lat"] - ny * half_width_deg,
                       "lon": p["lon"] - nx * half_width_deg})
    ring = left + list(reversed(right))
    # Close the ring.
    if ring:
        ring.append(ring[0])
    return ring


def _load_buildings(elements: list[dict],
                    center_lat: float, center_lon: float) -> list[dict]:
    buildings: list[dict] = []
    idx = 0
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0

    # ── Pre-pass: expand multipolygon relations and waterway polylines
    # into synthetic "ways" with the same shape the main loop expects.
    # This keeps the main loop logic simple — after this pass, every
    # natural polygon (big lake, relation-based forest, river strip)
    # looks like a plain closed way with one outer ring.
    synth: list[dict] = []
    for el in elements:
        tags = el.get("tags", {}) or {}
        t = el.get("type")

        if t == "relation" and (tags.get("natural") == "water"
                                or tags.get("landuse") in ("forest", "cemetery")):
            rings = _stitch_outer_rings(el.get("members") or [])
            for r_idx, ring in enumerate(rings):
                synth.append({
                    "type":     "way",
                    "id":       (el.get("id", 0) * 100) + r_idx,
                    "tags":     tags,                       # inherit relation tags
                    "geometry": ring,
                })
            continue

        if (t == "way"
                and tags.get("waterway") == "river"
                and (not el.get("geometry")
                     or el["geometry"][0] != el["geometry"][-1])):
            # Open river polyline → buffer to a thin polygon strip.
            # Width from OSM `width` tag if numeric, else default 8 m.
            try:
                width_m = float(tags.get("width", "") or 8.0)
            except ValueError:
                width_m = 8.0
            width_m = max(4.0, min(60.0, width_m))
            half_deg = (width_m / 2.0) / 111_320.0
            ring = _buffer_line_to_polygon(el.get("geometry") or [], half_deg)
            if len(ring) >= 4:
                synth.append({
                    "type":     "way",
                    "id":       el.get("id", 0),
                    "tags":     {"natural": "water",
                                 "water":    "river",
                                 **({"name": tags["name"]} if "name" in tags else {})},
                    "geometry": ring,
                })
            continue

    # Main loop iterates over original ways + synthetic polygons alike.
    iter_elements = list(elements) + synth

    for el in iter_elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags", {})
        # Accept ways tagged `building=*` (normal buildings) OR
        # `historic=castle` (castle ruins that typically have no building
        # tag — e.g. Kronobergs slottsruin in Växjö, OSM way 54048626) OR
        # natural/landcover polygons we treat as selectable POIs:
        #   natural=water       → WATER    (lakes, ponds)
        #   landuse=forest      → FORESTRY (managed forest)
        #   natural=wood        → FORESTRY (natural woodland)
        _is_natural = (
            tags.get("natural") in ("water", "wood")
            or tags.get("landuse") == "forest"
            or tags.get("waterway") == "riverbank"
        )
        # Non-building recreational polygons (playground ways) — these
        # are classified like regular ways but carry no `building=*` tag.
        _is_leisure_way = (tags.get("leisure") == "playground")
        # Cemetery polygons live under `landuse=cemetery` (or
        # `amenity=grave_yard`). Treat them like park / playground ways
        # — area feature, no `building=*` tag required.
        _is_cemetery = (tags.get("landuse") == "cemetery"
                        or tags.get("amenity") == "grave_yard")
        if ("building" not in tags
                and tags.get("historic") != "castle"
                and not _is_natural
                and not _is_leisure_way
                and not _is_cemetery):
            continue

        geometry = el.get("geometry", [])
        if len(geometry) < 3:
            continue

        # Natural polygons need to be closed rings — skip open ways (rivers
        # stored as open polylines are filtered here).
        if _is_natural:
            if (geometry[0]["lat"], geometry[0]["lon"]) != (
                    geometry[-1]["lat"], geometry[-1]["lon"]):
                continue

        lat, lon = _compute_centroid(geometry)
        area_m2  = _polygon_area_m2(geometry, center_lat)
        # Natural polygons have very different meaningful size thresholds
        # than buildings — a 20 m² "lake" is a puddle, a 500 m² "forest"
        # is a thicket. Require at least 400 m² for natural features to
        # keep the city.json payload manageable.
        min_area = 400.0 if _is_natural else 20.0
        if area_m2 < min_area:
            continue

        levels_raw = tags.get("building:levels", tags.get("levels", ""))
        try:
            levels = max(1, int(float(levels_raw)))
        except (ValueError, TypeError):
            levels = 1

        zone    = classify_zone(tags, levels)
        # Only residential zones hold the initial susceptible pool.
        # Non-residential venues (schools, hospitals, commercial,
        # industrial, playgrounds, libraries, forests, …) are purely
        # visit destinations — their SEIR dynamics come from the
        # spatial attraction kernel pulling susceptibles in from
        # nearby residential cells. Without this guard, huge
        # industrial / commercial footprints add tens of thousands
        # of phantom "factory workers" to the displayed city pop
        # (Borås used to inflate by 24 %).
        if zone in _RES_ZONES:
            pop = estimate_population(zone, area_m2, levels)
        else:
            pop = 0
        world_x = (lon - center_lon) * mplx
        world_z = -(lat - center_lat) * mpl

        poly = [
            [round((g["lon"] - center_lon) * mplx, 1),
             round(-(g["lat"] - center_lat) * mpl, 1)]
            for g in geometry
        ]
        if len(poly) > 1 and poly[0] == poly[-1]:
            poly = poly[:-1]

        entry = {
            "idx":     idx,
            "osm_id":  el.get("id"),
            "lat":     round(lat, 7),
            "lon":     round(lon, 7),
            "world_x": round(world_x, 1),
            "world_z": round(world_z, 1),
            "zone":    zone,
            "levels":  levels,
            "area_m2": round(area_m2, 1),
            "pop":     pop,
            "polygon": poly,
        }
        # Carry the OSM display name when one is set, so the frontend hover
        # tooltip can show it (landmarks, named institutions, etc.). Swedish
        # name takes priority when both are present. Overrides in
        # overrides.json still win because they're applied later.
        osm_name = tags.get("name:sv") or tags.get("name")
        if osm_name:
            entry["name"] = osm_name
        # ── Natural features: attach species + readable fallback name.
        # Water/forestry cells are rendered by the info-mode extruded
        # polygon mesh (see ZONE_COLORS + extrude-height special cases in
        # view.js) AND by a scene-wide batched tree detail layer in
        # buildings_custom.js buildNaturalDetailLayer(). No custom_builder
        # is assigned here — doing so would kick them out of the info
        # mesh and cost thousands of draw calls at the per-cell scale
        # (Växjö has ~1000 natural polygons).
        if zone == _FORESTRY:
            species = _extract_tree_species(tags)
            entry["tree_species"]   = species
            entry["tree_count"]     = _forestry_tree_count(area_m2, species)
            if "name" not in entry:
                entry["name"] = f"Forest ({species})"
        elif zone == _WATER:
            # Classify water type from OSM tags
            wt = tags.get("water", "")
            wway = tags.get("waterway", "")
            if wt in ("river", "canal", "stream", "ditch") or wway in ("river", "riverbank"):
                entry["water_type"] = "river"
            elif wt == "pond":
                entry["water_type"] = "pond"
            elif wt == "reservoir" or wt == "basin":
                entry["water_type"] = "reservoir"
            elif wt == "sea" or wt == "ocean" or wt == "bay":
                entry["water_type"] = "sea"
            else:
                entry["water_type"] = "lake"
            if "name" not in entry:
                _WATER_DEFAULT_NAMES = {
                    "lake": "Lake", "river": "River", "pond": "Pond",
                    "reservoir": "Reservoir", "sea": "Sea",
                }
                entry["name"] = _WATER_DEFAULT_NAMES.get(entry["water_type"], "Lake")
        # NOTE: pop is already 0 for any zone not in _RES_ZONES via
        # the branch above this `entry = {...}` block, so water and
        # forestry don't need an explicit override anymore.
        buildings.append(entry)
        idx += 1
    return buildings

# ── Cemetery polygon clipping ─────────────────────────────────────────────────
#
# Cemeteries in OSM almost always contain one or more real buildings
# inside their land — chapels, crematoria, small groundskeeper houses
# and, crucially, the parish church that the cemetery surrounds. Left
# as-is, the cemetery polygon renders as a flat extruded slab that
# z-fights with the church's own extruded polygon and creates visual
# glitches.
#
# Fix: for every cemetery cell, subtract the polygons of any
# *spatially overlapping* buildings from its footprint using Shapely
# boolean ops. The result is the actual ground around the church —
# exactly what you'd see walking the cemetery in real life.
#
# If the subtraction produces a MultiPolygon (cemetery split into
# several disjoint pieces by the buildings inside), keep only the
# largest piece. Holes in the result are dropped — we only need the
# outer ring for downstream rendering.
def _clip_cemetery_polygons(buildings: list[dict]) -> int:
    """
    Subtract overlapping building footprints from every cemetery
    polygon. When a building sits ENTIRELY INSIDE a cemetery (a church
    surrounded by its graveyard), Shapely's difference() produces a
    polygon with interior holes — those holes are persisted to the
    entry as `polygon_holes` and rendered by view.js _makeExtrudeGeo
    via THREE.Shape.holes so the cemetery mesh has the church cut out
    of it. This eliminates the z-fighting + occlusion that made the
    church unclickable before.

    For buildings that sit on the cemetery boundary (partial overlap),
    the outer ring itself gets re-routed. If the clip splits the
    cemetery into multiple disjoint pieces, the largest one wins.

    Modifies each affected cemetery in place: `polygon`, optional
    `polygon_holes`, and `area_m2`. Returns the number of cemeteries
    whose geometry was actually changed.
    """
    try:
        from shapely.geometry         import Polygon, MultiPolygon
        from shapely.geometry.polygon import orient
        from shapely.ops              import unary_union
        from shapely.strtree          import STRtree
    except ImportError:
        return 0

    polys: list[Any]    = []
    poly_idx: list[int] = []
    for i, b in enumerate(buildings):
        pts = b.get("polygon") or []
        if len(pts) < 3:
            continue
        try:
            p = Polygon(pts)
            if not p.is_valid:
                p = p.buffer(0)
            if p.is_empty:
                continue
            polys.append(p)
            poly_idx.append(i)
        except Exception:
            continue

    if not polys:
        return 0

    tree = STRtree(polys)
    clipped_count = 0

    # Force CCW exterior + CW interior winding so Three.js Earcut
    # consistently cuts holes (and the flip-z in _makeExtrudeGeo
    # produces the opposite pair Three.js actually expects).
    def _oriented(g):
        return orient(g, sign=1.0)

    for i, b in enumerate(buildings):
        if b.get("zone") != _CEMETERY:
            continue
        pts = b.get("polygon") or []
        if len(pts) < 3:
            continue
        try:
            cem = Polygon(pts)
            if not cem.is_valid:
                cem = cem.buffer(0)
        except Exception:
            continue
        if cem.is_empty or cem.area <= 0:
            continue

        # Find every building polygon whose bbox overlaps the cemetery.
        candidate_hits = tree.query(cem)
        to_subtract = []
        for q in candidate_hits:
            try:
                q_idx = int(q)         # Shapely 2.x returns integer indices
            except (TypeError, ValueError):
                continue
            j = poly_idx[q_idx]
            if j == i:
                continue
            if buildings[j].get("zone") == _CEMETERY:
                continue   # don't subtract a neighbouring cemetery
            other = polys[q_idx]
            try:
                if cem.intersects(other) and not cem.touches(other):
                    to_subtract.append(other)
            except Exception:
                continue

        if not to_subtract:
            continue

        try:
            # Buffer each building outward by ~2 m before subtracting,
            # creating a visible gap between the cemetery and the
            # buildings inside it (church, chapel, etc.).
            buffered = [s.buffer(2.0) for s in to_subtract]
            merged   = unary_union(buffered)
            new_geom = cem.difference(merged)
        except Exception:
            continue
        if new_geom.is_empty or new_geom.area <= 0:
            continue

        # If the clip fractured the cemetery, keep the largest piece.
        if isinstance(new_geom, MultiPolygon):
            new_geom = max(new_geom.geoms, key=lambda g: g.area)

        # Force CCW exterior + CW interiors (Shapely 2.x does not
        # guarantee winding through boolean ops).
        try:
            new_geom = _oriented(new_geom)
        except Exception:
            pass

        exterior = getattr(new_geom, "exterior", None)
        if exterior is None:
            continue
        outer_coords = list(exterior.coords)
        if len(outer_coords) > 1 and outer_coords[0] == outer_coords[-1]:
            outer_coords = outer_coords[:-1]
        if len(outer_coords) < 3:
            continue

        # Preserve every interior hole (buildings fully enclosed by
        # the cemetery) so the frontend can cut them out of the
        # extruded mesh via THREE.Shape.holes.
        holes: list[list[tuple[float, float]]] = []
        for ring in new_geom.interiors:
            hole_coords = list(ring.coords)
            if len(hole_coords) > 1 and hole_coords[0] == hole_coords[-1]:
                hole_coords = hole_coords[:-1]
            if len(hole_coords) >= 3:
                holes.append(hole_coords)

        b["polygon"] = [[round(x, 1), round(z, 1)] for x, z in outer_coords]
        if holes:
            b["polygon_holes"] = [
                [[round(x, 1), round(z, 1)] for x, z in h]
                for h in holes
            ]
        elif "polygon_holes" in b:
            del b["polygon_holes"]
        b["area_m2"] = round(new_geom.area, 1)
        clipped_count += 1

    return clipped_count


# ── Manual overrides ──────────────────────────────────────────────────────────

# Fields that a manual override is allowed to patch on a building dict.
# Intentionally narrow — we don't want an overrides file to be able to
# silently corrupt geometry (polygon, world_x/z, area_m2, osm_id, idx).
_OVERRIDE_ALLOWED_FIELDS = frozenset({
    "zone", "name", "levels", "pop", "custom_builder",
})


def _find_building_for_override(buildings: list[dict],
                                rule: dict,
                                center_lat: float,
                                center_lon: float) -> dict | None:
    """
    Locate the single building targeted by one override rule.

    Match priority (first hit wins):
      1. ``match.osm_id``            — exact OSM way id (most stable)
      2. ``match.name`` regex        — against the building's "name" field
                                       (only set by a prior override)
      3. ``match.lat`` + ``match.lon`` + optional ``match.max_distance_m``
         — nearest building centroid within max_distance_m (default 50 m)
    """
    match = rule.get("match", {}) or {}

    if "osm_id" in match:
        target = match["osm_id"]
        for b in buildings:
            if b.get("osm_id") == target:
                return b
        return None

    if "name" in match:
        pat = re.compile(match["name"])
        for b in buildings:
            if b.get("name") and pat.search(b["name"]):
                return b
        return None

    if "lat" in match and "lon" in match:
        mx, mz = latlon_to_meters(match["lat"], match["lon"], center_lat, center_lon)
        max_d  = float(match.get("max_distance_m", 50.0))
        max_d2 = max_d * max_d
        best: dict | None = None
        best_d2 = max_d2
        for b in buildings:
            d2 = (b["world_x"] - mx) ** 2 + (b["world_z"] - mz) ** 2
            if d2 < best_d2:
                best_d2 = d2
                best    = b
        return best

    return None


def _apply_overrides(city_id: str,
                     buildings: list[dict],
                     center_lat: float,
                     center_lon: float) -> int:
    """
    Apply manual patches from data/<city>/overrides.json on top of the
    freshly downloaded + classified building list.

    The overrides file is hand-edited and never touched by the downloader,
    so re-running tools/download_city.py always re-applies it.

    Returns the number of buildings patched.
    """
    overrides_path = os.path.join(_city_dir(city_id), "overrides.json")
    if not os.path.exists(overrides_path):
        return 0

    try:
        with open(overrides_path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[osm] overrides.json unreadable — skipping: {e}")
        return 0

    rules = doc.get("buildings", []) or []
    applied = 0
    for rule in rules:
        # ── Synthesize variant: create a NEW building from scratch ─────────
        # Used for landmarks that aren't in OSM at all, or are mapped as
        # relations (multipolygons) that our way-only Overpass query skips.
        # Canonical example: Kalmar Slott (OSM relation 1331955).
        if "synthesize" in rule:
            new_b = _synthesize_building(
                rule["synthesize"], buildings, center_lat, center_lon)
            if new_b is None:
                continue
            buildings.append(new_b)
            applied += 1
            continue

        # ── Match variant: patch an existing building ──────────────────────
        target = _find_building_for_override(
            buildings, rule, center_lat, center_lon)
        if target is None:
            label = rule.get("match", {})
            print(f"[osm] override: no building matched {label}")
            continue

        patch = rule.get("set", {}) or {}
        for key, value in patch.items():
            if key not in _OVERRIDE_ALLOWED_FIELDS:
                print(f"[osm] override: ignoring disallowed field {key!r}")
                continue
            target[key] = value

        # If zone changed, recompute population from the new density.
        if "zone" in patch and "pop" not in patch:
            target["pop"] = estimate_population(
                target["zone"], target["area_m2"], target["levels"])

        applied += 1

    return applied


def _synthesize_building(spec: dict,
                         existing: list[dict],
                         center_lat: float,
                         center_lon: float) -> dict | None:
    """
    Build a fresh entry for a landmark that isn't in the OSM cache (or is
    mapped as a relation, which our way-only query skips).

    The spec dict comes from an overrides.json rule under the "synthesize"
    key. Required fields: `lat`, `lon`. Optional fields: `osm_id` (for
    reference only — never matched), `name`, `zone`, `custom_builder`,
    `levels`, `pop`, `area_m2`, `polygon` (local world-coord ring; if
    omitted, a square is synthesized around the point with side =
    sqrt(area_m2)).

    The new building's `idx` is assigned as (max existing idx) + 1.
    Subsequent pipeline passes (demographics attach, shapely boundary
    filter, SEIR seeding) treat it identically to an OSM-sourced building.
    """
    lat = spec.get("lat")
    lon = spec.get("lon")
    if lat is None or lon is None:
        print(f"[osm] synthesize: missing lat/lon in {spec.get('name', '?')}")
        return None
    lat = float(lat); lon = float(lon)

    world_x, world_z = latlon_to_meters(lat, lon, center_lat, center_lon)

    next_idx = (max((b["idx"] for b in existing), default=-1) + 1) if existing else 0

    area_m2 = float(spec.get("area_m2", 400.0))
    polygon = spec.get("polygon")
    if polygon is None:
        # Synthesize a square polygon of the given area, centred on the
        # building's world coords, in world_x/world_z space.
        half = math.sqrt(max(area_m2, 1.0)) / 2
        polygon = [
            [round(world_x - half, 1), round(world_z - half, 1)],
            [round(world_x + half, 1), round(world_z - half, 1)],
            [round(world_x + half, 1), round(world_z + half, 1)],
            [round(world_x - half, 1), round(world_z + half, 1)],
        ]

    entry: dict = {
        "idx":     next_idx,
        "osm_id":  spec.get("osm_id"),
        "lat":     round(lat, 7),
        "lon":     round(lon, 7),
        "world_x": round(world_x, 1),
        "world_z": round(world_z, 1),
        "zone":    int(spec.get("zone", _COMMERCIAL)),
        "levels":  int(spec.get("levels", 1)),
        "area_m2": round(area_m2, 1),
        "pop":     int(spec.get("pop", 1)),
        "polygon": polygon,
    }
    for key in ("name", "custom_builder"):
        if spec.get(key) is not None:
            entry[key] = spec[key]

    label = spec.get("name") or spec.get("custom_builder") or f"synthesized@{lat:.4f},{lon:.4f}"
    print(f"[osm] synthesize: {label} at world=({world_x:.0f},{world_z:.0f}) "
          f"idx={next_idx} zone={entry['zone']}")
    return entry


# ── Main loader ───────────────────────────────────────────────────────────────

# ── Lightweight nature POIs ──────────────────────────────────────────────────
#
# These are NOT buildings. They ride alongside the buildings array as a
# separate `nature_pois` list, piped through /api/city as layout
# metadata. The frontend spawns them via the existing transport-stop
# pin pipeline (_buildPOIs / spawnTransportPin), with string keys like
# "nature_reserve", "cemetery" that map to POI_CATEGORIES entries. No
# epidemic impact — they never become Cell objects.
#
# Each emitted POI dict: {idx, type, world_x, world_z, lat, lon, name}.

def _nature_poi_type(tags: dict) -> str | None:
    """Classify raw OSM tags into one of our lightweight POI buckets.
    Returns None if the element doesn't match any nature POI type.

    NOTE: cemeteries are NOT handled here — they're promoted to a full
    building zone (_CEMETERY) so they participate in the SEIR kernel
    alongside churches. See classify_zone() + the _is_cemetery branch
    in _load_buildings()."""
    if tags.get("leisure") == "nature_reserve":
        return "nature_reserve"
    if tags.get("boundary") == "protected_area":
        return "nature_reserve"       # treat the same visually
    return None


def _load_nature_pois(
        elements: list[dict], center_lat: float, center_lon: float,
) -> list[dict]:
    """
    Extract lightweight nature POIs (nature reserves, cemeteries) from
    the raw Overpass response. Each POI gets one pin per way/relation/
    node — no polygon filtering, no population estimate, no SEIR.
    """
    pois: list[dict] = []
    idx = 0
    mplx = 111_320.0 * math.cos(math.radians(center_lat))
    mpl  = 111_320.0

    for el in elements:
        tags = el.get("tags") or {}
        ptype = _nature_poi_type(tags)
        if not ptype:
            continue

        t = el.get("type")
        if t == "node":
            lat = el.get("lat")
            lon = el.get("lon")
            if lat is None or lon is None:
                continue
        elif t == "way":
            geom = el.get("geometry") or []
            if len(geom) < 3:
                continue
            lat, lon = _compute_centroid(geom)
        elif t == "relation":
            # Average the outer-member ring centroids.
            rings = _stitch_outer_rings(el.get("members") or [])
            if not rings:
                continue
            lat_sum = 0.0; lon_sum = 0.0; n = 0
            for ring in rings:
                rc_lat, rc_lon = _compute_centroid(ring)
                lat_sum += rc_lat; lon_sum += rc_lon; n += 1
            if n == 0:
                continue
            lat = lat_sum / n
            lon = lon_sum / n
        else:
            continue

        entry = {
            "idx":     idx,
            "type":    ptype,
            "lat":     round(lat, 7),
            "lon":     round(lon, 7),
            "world_x": round((lon - center_lon) * mplx, 1),
            "world_z": round(-(lat - center_lat) * mpl, 1),
        }
        name = tags.get("name:sv") or tags.get("name")
        if name:
            entry["name"] = name
        pois.append(entry)
        idx += 1

    return pois


# ── Swedish flag pole easter egg ─────────────────────────────────────────────
#
# Every city gets a handful of Swedish flag poles planted next to random
# residential buildings — a deterministic, seeded easter egg. Positions
# are stored in city.json as a small `flag_poles` list and rendered as
# 3D assets on the frontend (see buildings_custom.js buildSwedishFlagPole).
#
# Strict constraints: only placed near RES_LO/MED/HI polygons — never
# on water, roads, forests, etc. Residential buildings by definition
# aren't inside those, so starting from a residential centroid + small
# offset is safe without explicit spatial exclusion.
_FLAG_POLE_COUNT_PER_CITY = 5

# ── LNU (Linnaeus University) flag poles ────────────────────────────────────
#
# Linnaeus University is headquartered on a Växjö campus (main) with a
# satellite in Kalmar. When loading those cities, drop a handful of
# LNU-branded flag poles next to buildings classified as UNIVERSITY
# so the user can find the campus via the POI menu.
_LNU_CITIES = {"vaxjo", "kalmar"}
_LNU_FLAG_COUNT = 4

def _build_flag_spatial_filter(
        buildings: list[dict], roads: list[dict],
):
    """
    Shared spatial-clear check for flag-pole placement. Returns an
    `is_clear(x, z)` callable that rejects any position inside a
    building polygon OR within ~3 m of a road centreline. Falls back
    to "always clear" if Shapely isn't available.
    """
    try:
        from shapely.geometry import Polygon, Point, LineString
        from shapely.strtree  import STRtree
    except ImportError:
        return (lambda x, z: True)

    geoms: list[Any]  = []
    kinds: list[str]  = []   # parallel to `geoms` — 'poly' or 'line'

    for b in buildings:
        pts = b.get("polygon") or []
        if len(pts) < 3:
            continue
        try:
            p = Polygon(pts)
            if not p.is_valid:
                p = p.buffer(0)
            if p.is_empty:
                continue
            geoms.append(p); kinds.append("poly")
        except Exception:
            continue

    for r in roads or []:
        pts = r.get("points") or []
        if len(pts) < 2:
            continue
        try:
            line = LineString(pts)
            if line.is_empty:
                continue
            geoms.append(line); kinds.append("line")
        except Exception:
            continue

    if not geoms:
        return (lambda x, z: True)

    tree = STRtree(geoms)
    ROAD_MIN_DIST = 3.5   # metres — keep a flag pole clear of tarmac

    def _is_clear(x: float, z: float) -> bool:
        pt = Point(x, z)
        # Query using a small buffer so nearby roads are caught too
        # (STRtree is bbox-based — a point on a road wouldn't hit the
        # line's zero-width bbox).
        region = pt.buffer(ROAD_MIN_DIST + 0.5)
        for idx in tree.query(region):
            try:
                q_idx = int(idx)
            except (TypeError, ValueError):
                continue
            g = geoms[q_idx]
            if kinds[q_idx] == "poly":
                if g.contains(pt):
                    return False
            else:  # line
                if g.distance(pt) < ROAD_MIN_DIST:
                    return False
        return True

    return _is_clear


def _pick_lnu_flag_positions(
        buildings: list[dict], roads: list[dict], city_id: str,
) -> list[dict]:
    """Pick a small number of university-building neighbours and drop
    an LNU flag pole next to each. Only fires for Växjö and Kalmar.
    The candidate position is rejected if it lands inside any building
    polygon or within ~3.5 m of any road centreline."""
    if city_id not in _LNU_CITIES:
        return []

    uni_buildings = [
        b for b in buildings
        if b.get("zone") == _UNIVERSITY and b.get("polygon")
    ]
    if not uni_buildings:
        return []

    _is_clear = _build_flag_spatial_filter(buildings, roads)

    rng = random.Random(f"lnu::{city_id}")
    pool = rng.sample(uni_buildings,
                      min(_LNU_FLAG_COUNT * 5, len(uni_buildings)))

    poles: list[dict] = []
    MAX_RETRIES = 20
    for b in pool:
        if len(poles) >= _LNU_FLAG_COUNT:
            break
        found = None
        for _ in range(MAX_RETRIES):
            angle = rng.uniform(0.0, 2.0 * math.pi)
            dist  = rng.uniform(8.0, 18.0)
            x = b["world_x"] + math.cos(angle) * dist
            z = b["world_z"] + math.sin(angle) * dist
            if _is_clear(x, z):
                found = (x, z); break
        if found is None:
            continue
        poles.append({
            "idx":     len(poles),
            "world_x": round(found[0], 1),
            "world_z": round(found[1], 1),
            "lat":     b["lat"],
            "lon":     b["lon"],
            "host_building_idx": b.get("idx"),
        })
    return poles


def _pick_flag_pole_positions(
        buildings: list[dict], roads: list[dict], city_id: str,
) -> list[dict]:
    """
    Pick a few residential buildings per city and drop a flag pole
    outside each one, on a clear piece of ground. Seeded on the city
    id so the choice is stable across reloads but different for each
    city. Rejects positions inside any polygon feature OR within
    ~3.5 m of any road centreline.
    """
    res = [
        b for b in buildings
        if b.get("zone") in (_RES_LO, _RES_MED, _RES_HI)
        and b.get("polygon")
    ]
    if not res:
        return []

    _is_clear = _build_flag_spatial_filter(buildings, roads)

    rng = random.Random(f"flagpole::{city_id}")
    # Sample a larger pool than we need so we can skip any building
    # where we can't find a clear flag spot in the yard.
    pool = rng.sample(res, min(_FLAG_POLE_COUNT_PER_CITY * 6, len(res)))

    poles: list[dict] = []
    MAX_RETRIES = 12
    for b in pool:
        if len(poles) >= _FLAG_POLE_COUNT_PER_CITY:
            break
        found = None
        for _ in range(MAX_RETRIES):
            angle = rng.uniform(0.0, 2.0 * math.pi)
            dist  = rng.uniform(6.0, 14.0)
            x = b["world_x"] + math.cos(angle) * dist
            z = b["world_z"] + math.sin(angle) * dist
            if _is_clear(x, z):
                found = (x, z)
                break
        if found is None:
            continue
        poles.append({
            "idx":     len(poles),
            "world_x": round(found[0], 1),
            "world_z": round(found[1], 1),
            "lat":     b["lat"],
            "lon":     b["lon"],
            "host_building_idx": b.get("idx"),
        })

    return poles


# ── Swedish cultural easter eggs ─────────────────────────────────────────────
# Five iconic Swedish objects hidden across every city:
#   dalahast      — classic red Dala horse
#   ikea_bag      — blue-yellow IKEA shopping bag
#   fika_cup      — coffee cup and saucer
#   maypole       — midsummer maypole (midsommarstång)
#   moose         — Swedish moose (älg)
#
# Each city gets one of each, placed near a residential building on clear
# ground (same spatial filter as flag poles).

_EASTER_EGG_TYPES = ["dalahast", "ikea_bag", "fika_cup", "maypole", "moose"]

def _pick_easter_egg_positions(
        buildings: list[dict], roads: list[dict], city_id: str,
) -> list[dict]:
    """
    Place one of each Swedish easter egg type per city. Uses the same
    spatial filter as flag poles to avoid roads and building interiors.
    Seeded on city_id for deterministic, stable placement.
    """
    res = [
        b for b in buildings
        if b.get("zone") in (_RES_LO, _RES_MED, _RES_HI)
        and b.get("polygon")
    ]
    if not res:
        return []

    _is_clear = _build_flag_spatial_filter(buildings, roads)
    rng = random.Random(f"easter::{city_id}")

    # Spread eggs across different parts of the city by sorting buildings
    # by distance from center and picking from different distance bands.
    cx = sum(b["world_x"] for b in res) / len(res)
    cz = sum(b["world_z"] for b in res) / len(res)
    by_dist = sorted(res, key=lambda b: (b["world_x"] - cx)**2 + (b["world_z"] - cz)**2)
    n = len(by_dist)

    eggs: list[dict] = []
    MAX_RETRIES = 20
    for i, egg_type in enumerate(_EASTER_EGG_TYPES):
        # Pick from a different distance band for each egg type
        band_start = int(n * i / len(_EASTER_EGG_TYPES))
        band_end   = int(n * (i + 1) / len(_EASTER_EGG_TYPES))
        pool = by_dist[band_start:band_end]
        if not pool:
            pool = res
        rng.shuffle(pool)

        placed = False
        for b in pool[:30]:
            for _ in range(MAX_RETRIES):
                angle = rng.uniform(0.0, 2.0 * math.pi)
                dist  = rng.uniform(5.0, 12.0)
                x = b["world_x"] + math.cos(angle) * dist
                z = b["world_z"] + math.sin(angle) * dist
                if _is_clear(x, z):
                    # Check minimum distance from existing eggs (~50m apart)
                    too_close = False
                    for e in eggs:
                        dx = e["world_x"] - x
                        dz = e["world_z"] - z
                        if dx*dx + dz*dz < 2500:  # 50m
                            too_close = True
                            break
                    if too_close:
                        continue
                    eggs.append({
                        "idx":     len(eggs),
                        "type":    egg_type,
                        "world_x": round(x, 1),
                        "world_z": round(z, 1),
                        "lat":     b["lat"],
                        "lon":     b["lon"],
                    })
                    placed = True
                    break
            if placed:
                break

    return eggs


def load_city(city_id: str) -> dict[str, list[dict]]:
    """
    Return a dict {"buildings": [...], "roads": [...]} for the given city.

    Checks for a pre-built processed cache (data/{city_id}_city.json) first.
    If found, returns it immediately — no API call, no parsing.
    Otherwise, downloads from Overpass, processes, saves the city cache, and returns.
    """
    if city_id not in CITIES:
        raise ValueError(f"Unknown city: {city_id!r}.  Available: {list(CITIES)}")

    os.makedirs(_city_dir(city_id), exist_ok=True)
    city_cache = _city_paths(city_id)["city"]
    if os.path.exists(city_cache):
        print(f"[osm] Using pre-built city data: {city_cache}")
        with open(city_cache, encoding="utf-8") as f:
            return json.load(f)

    city       = CITIES[city_id]
    center_lat = city["center_lat"]
    center_lon = city["center_lon"]

    elements = download_osm_data(city_id)

    # Buildings (way-level)
    buildings = _load_buildings(elements, center_lat, center_lon)

    # Municipality boundary filter — keep only buildings inside DeSO union
    boundary, border_rings = _load_municipality_boundary(
        city_id, center_lat, center_lon)
    if boundary is not None:
        buildings = _filter_buildings_by_boundary(buildings, boundary)

    # POI nodes — assign to nearest building
    poi_nodes = [el for el in elements if el.get("type") == "node"]
    overrides = _match_pois_to_buildings(buildings, poi_nodes, center_lat, center_lon)
    print(f"[osm] POI node matching: {overrides} buildings reassigned from {len(poi_nodes)} nodes.")

    # Manual per-city overrides — hand-edited patches that survive regeneration.
    manual = _apply_overrides(city_id, buildings, center_lat, center_lon)
    if manual:
        print(f"[osm] Manual overrides: {manual} buildings patched from overrides.json")

    # Cemetery polygon clipping — subtract overlapping buildings (churches,
    # chapels, groundskeeper houses) from cemetery outlines so the
    # cemetery shape renders as the actual ground AROUND the buildings
    # inside it, eliminating z-fighting with the church polygons.
    clipped = _clip_cemetery_polygons(buildings)
    if clipped:
        print(f"[osm] Cemetery polygons clipped against overlapping buildings: {clipped}")

    # Demographics dispatcher — SE uses SCB DeSO, BR uses IBGE bairros.
    # The downstream _attach_scb_demographics() is schema-generic and works
    # for either bundle as long as it matches {areas: [{deso, polygon, total, age}]}.
    _attach_demographics(buildings, city_id)

    # Roads
    roads = load_roads(elements, center_lat, center_lon)
    if boundary is not None:
        roads = _filter_roads_by_boundary(roads, boundary, center_lat, center_lon)

    # Lightweight nature POIs (nature reserves, cemeteries, …) —
    # delivered to the frontend as a separate list so they never enter
    # the SEIR kernel.
    nature_pois = _load_nature_pois(elements, center_lat, center_lon)
    print(f"[osm] Nature POIs: {len(nature_pois)}")

    # Swedish flag pole easter egg — deterministic handful per city.
    # Spatial filter now rejects positions inside any polygon feature
    # OR within ~3.5 m of any road centreline, so no flag lands on
    # tarmac or inside a building.
    flag_poles = _pick_flag_pole_positions(buildings, roads, city_id)
    print(f"[osm] Flag poles: {len(flag_poles)}")

    # LNU (Linnaeus University) flag poles — only fires for Växjö and
    # Kalmar, planted next to UNIVERSITY-zoned buildings on campus.
    lnu_flag_poles = _pick_lnu_flag_positions(buildings, roads, city_id)
    if lnu_flag_poles:
        print(f"[osm] LNU flag poles: {len(lnu_flag_poles)}")

    # Swedish cultural easter eggs — one of each type per city
    easter_eggs = _pick_easter_egg_positions(buildings, roads, city_id)
    print(f"[osm] Easter eggs: {len(easter_eggs)}")

    # Compute summary stats (used by macro view hover cards)
    total_pop = sum(b.get("pop", 0) for b in buildings)
    result = {
        "buildings":       buildings,
        "roads":           roads,
        "nature_pois":     nature_pois,
        "flag_poles":      flag_poles,
        "lnu_flag_poles":  lnu_flag_poles,
        "easter_eggs":     easter_eggs,
        "population":      total_pop,
        "building_count":  len(buildings),
    }

    # Include municipality border for frontend rendering
    if border_rings:
        result["border"] = border_rings

    print(f"[osm] Saving pre-built city data: {city_cache}")
    with open(city_cache, "w", encoding="utf-8") as f:
        json.dump(result, f, separators=(",", ":"))

    return result


# ── Operator / brand extraction ──────────────────────────────────────────────

# Swedish regional transit authorities mapped to their län. The key is the
# canonical operator/network name as it appears in OSM tags; the value is the
# dict that goes into operators.json. Entries here serve as a fallback when
# OSM relations lack an operator tag but do have a network tag, AND as a
# colour/brand source even when the OSM colour tag is missing.
_KNOWN_OPERATORS: dict[str, dict[str, Any]] = {
    # Regional bus/tram authorities
    "SL":                       {"brand": "SL",                       "colour": "#e4002b", "modes": ["bus", "tram", "train"]},
    "Storstockholms Lokaltrafik": {"brand": "SL",                     "colour": "#e4002b", "modes": ["bus", "tram", "train"]},
    "Västtrafik":               {"brand": "Västtrafik",               "colour": "#00427a", "modes": ["bus", "tram", "ferry"]},
    "Skånetrafiken":            {"brand": "Skånetrafiken",            "colour": "#ce1141", "modes": ["bus", "train"]},
    "UL":                       {"brand": "UL",                       "colour": "#e30613", "modes": ["bus"]},
    "Uppsalatrafik":            {"brand": "UL",                       "colour": "#e30613", "modes": ["bus"]},
    "Östgötatrafiken":          {"brand": "Östgötatrafiken",          "colour": "#009cdb", "modes": ["bus", "tram"]},
    "Länstrafiken Kronoberg":   {"brand": "Länstrafiken Kronoberg",   "colour": "#00923f", "modes": ["bus"]},
    "Kalmar Länstrafik":        {"brand": "Kalmar Länstrafik",        "colour": "#005baa", "modes": ["bus"]},
    "KLT":                      {"brand": "Kalmar Länstrafik",        "colour": "#005baa", "modes": ["bus"]},
    "Jönköpings Länstrafik":    {"brand": "Jönköpings Länstrafik",    "colour": "#004b87", "modes": ["bus"]},
    "JLT":                      {"brand": "Jönköpings Länstrafik",    "colour": "#004b87", "modes": ["bus"]},
    "Hallandstrafiken":         {"brand": "Hallandstrafiken",         "colour": "#e30613", "modes": ["bus"]},
    "Länstrafiken Örebro":      {"brand": "Länstrafiken Örebro",      "colour": "#007cc2", "modes": ["bus"]},
    "Värmlandstrafik":          {"brand": "Värmlandstrafik",          "colour": "#006633", "modes": ["bus"]},
    "Karlstadsbuss":            {"brand": "Karlstadsbuss",            "colour": "#ed1c24", "modes": ["bus"]},
    "VL":                       {"brand": "Västmanlands Lokaltrafik", "colour": "#0072bc", "modes": ["bus"]},
    "Västmanlands Lokaltrafik": {"brand": "Västmanlands Lokaltrafik", "colour": "#0072bc", "modes": ["bus"]},
    "X-trafik":                 {"brand": "X-trafik",                 "colour": "#ee3124", "modes": ["bus"]},
    "Din Tur":                  {"brand": "Din Tur",                  "colour": "#e4002b", "modes": ["bus"]},
    "Norrbottens Länstrafik":   {"brand": "Länstrafiken Norrbotten",  "colour": "#00a651", "modes": ["bus"]},
    "Länstrafiken Norrbotten":  {"brand": "Länstrafiken Norrbotten",  "colour": "#00a651", "modes": ["bus"]},
    "LLT":                      {"brand": "Luleå Lokaltrafik",        "colour": "#0071bc", "modes": ["bus"]},
    "Luleå Lokaltrafik":        {"brand": "Luleå Lokaltrafik",        "colour": "#0071bc", "modes": ["bus"]},
    "Tabussen":                 {"brand": "Tabussen",                 "colour": "#008fd5", "modes": ["bus"]},
    "Ultra":                    {"brand": "Ultra",                    "colour": "#ffcc00", "modes": ["bus"]},
    "Sörmlandstrafiken":        {"brand": "Sörmlandstrafiken",        "colour": "#95CC01", "modes": ["bus"]},
    "Flixbus":                  {"brand": "Flixbus",                  "colour": "#61CC00", "modes": ["bus"]},
    "Flygbussarna":             {"brand": "Flygbussarna",             "colour": "#C2B7BD", "modes": ["bus"]},
    "Swebus":                   {"brand": "Swebus",                   "colour": "#F3F3F3", "modes": ["bus"]},
    "Länstrafiken i Västerbotten": {"brand": "Länstrafiken Västerbotten", "colour": "#7a7a7a", "modes": ["bus"]},
    "Västerås Lokaltrafik":     {"brand": "Västerås Lokaltrafik",     "colour": "#7a7a7a", "modes": ["bus"]},
    "Bivab":                    {"brand": "Bivab",                    "colour": "#7a7a7a", "modes": ["bus"]},
    "Citybuss Boden":           {"brand": "Citybuss Boden",           "colour": "#7a7a7a", "modes": ["bus"]},
    "Connect Bus":              {"brand": "Connect Bus",              "colour": "#7a7a7a", "modes": ["bus"]},
    "Ybuss":                    {"brand": "Ybuss",                    "colour": "#7a7a7a", "modes": ["bus"]},
    "Nobina":                   {"brand": "Nobina",                   "colour": "#7a7a7a", "modes": ["bus"]},
    # National/inter-city train operators
    "SJ":                       {"brand": "SJ",                       "colour": "#1a1a1a", "modes": ["train"]},
    "SJ AB":                    {"brand": "SJ",                       "colour": "#1a1a1a", "modes": ["train"]},
    "MTRX":                     {"brand": "MTRX",                     "colour": "#ff6600", "modes": ["train"]},
    "MTR Express":              {"brand": "MTRX",                     "colour": "#ff6600", "modes": ["train"]},
    "Snälltåget":               {"brand": "Snälltåget",               "colour": "#f7941d", "modes": ["train"]},
    "Vy":                       {"brand": "Vy",                       "colour": "#00957a", "modes": ["train"]},
    "Vy Tåg":                   {"brand": "Vy",                       "colour": "#00957a", "modes": ["train"]},
    "Vy Tog":                   {"brand": "Vy",                       "colour": "#00957a", "modes": ["train"]},
    "Norrtåg":                  {"brand": "Norrtåg",                  "colour": "#004f9f", "modes": ["train"]},
    "Krösatågen":               {"brand": "Krösatågen",               "colour": "#0072bc", "modes": ["train"]},
    "Öresundståg":              {"brand": "Öresundståg",              "colour": "#c8cdd3", "modes": ["train"]},
    "Transdev;DSB Öresund":     {"brand": "Öresundståg",              "colour": "#c8cdd3", "modes": ["train"]},
    "DSB Öresund;Transdev":     {"brand": "Öresundståg",              "colour": "#c8cdd3", "modes": ["train"]},
    "DSBFirst":                 {"brand": "Öresundståg",              "colour": "#c8cdd3", "modes": ["train"]},
    "DSB Småland":              {"brand": "Öresundståg",              "colour": "#c8cdd3", "modes": ["train"]},
    "Tågåkeriet i Bergslagen":  {"brand": "Tågåkeriet i Bergslagen",  "colour": "#7a7a7a", "modes": ["train"]},
    "Arriva":                   {"brand": "Arriva",                   "colour": "#00a3e0", "modes": ["bus", "train"]},
    "Transdev Sverige":         {"brand": "Transdev",                 "colour": "#7a7a7a", "modes": ["train"]},
    "Mälartåg":                 {"brand": "Mälartåg",                 "colour": "#0098d4", "modes": ["train"]},
    "Västtågen":                {"brand": "Västtågen",                "colour": "#00427a", "modes": ["train"]},
    # Tram
    "Göteborgs Spårvägar":      {"brand": "Göteborgs Spårvägar",      "colour": "#00427a", "modes": ["tram"]},
    # Airlines (common domestic Swedish operators)
    "SAS":                      {"brand": "SAS",                      "colour": "#000066", "modes": ["plane"]},
    "Norwegian":                {"brand": "Norwegian",                "colour": "#d81939", "modes": ["plane"]},
    "BRA":                      {"brand": "BRA",                      "colour": "#005ca9", "modes": ["plane"]},
    "Braathens Regional Airlines": {"brand": "BRA",                   "colour": "#005ca9", "modes": ["plane"]},
    "Ryanair":                  {"brand": "Ryanair",                  "colour": "#073590", "modes": ["plane"]},
    "Wizz Air":                 {"brand": "Wizz Air",                 "colour": "#ce0888", "modes": ["plane"]},
}

# Kommun → primary airline operators (domestic flights). OSM doesn't carry
# airline route relations, so we map airports to their known operators.
_AIRPORT_AIRLINES: dict[str, list[str]] = {
    "stockholm":   ["SAS", "Norwegian", "Ryanair", "BRA"],
    "goteborg":    ["SAS", "Norwegian", "Ryanair", "BRA", "Wizz Air"],
    "malmo":       ["SAS", "Ryanair", "Wizz Air"],
    "umea":        ["SAS", "Norwegian", "BRA"],
    "lulea":       ["SAS", "Norwegian", "BRA"],
    "vasteras":    ["Ryanair"],
    "sundsvall":   ["SAS", "BRA"],
    "kalmar":      ["BRA"],
    "vaxjo":       ["BRA", "Wizz Air"],
    "linkoping":   ["BRA"],
    "karlstad":    ["BRA"],
    "jonkoping":   ["BRA"],
    "halmstad":    ["BRA"],
}
_DEFAULT_AIRLINES: list[str] = ["SAS"]


def download_operators(city_id: str, *, force: bool = False) -> dict:
    """
    Query OSM for all public-transport route relations in the city's
    municipality bbox, extract operator / network / colour tags, and merge
    with the known-operators table + static airline data.

    Saves ``data/{city_id}/operators.json`` and returns the bundle::

        {
          "city_id": "vaxjo",
          "operators": [
            {"name": "Länstrafiken Kronoberg", "brand": "Länstrafiken Kronoberg",
             "modes": ["bus"], "colour": "#00923f", "route_count": 12,
             "source": "osm"},
            ...
          ]
        }

    Cached; pass *force=True* to re-fetch.
    """
    if city_id not in CITIES:
        return {"city_id": city_id, "operators": []}

    cache_path = os.path.join(_city_dir(city_id), "operators.json")
    if os.path.exists(cache_path) and not force:
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    city = CITIES[city_id]
    s, w, n, e = city["bbox"]

    # Expand to municipality bounds (same logic as download_bus_routes)
    try:
        boundary, _ = _load_municipality_boundary(
            city_id, city["center_lat"], city["center_lon"])
        if boundary is not None:
            minx, miny, maxx, maxy = boundary.bounds
            s = min(s, miny - 0.01)
            w = min(w, minx - 0.01)
            n = max(n, maxy + 0.01)
            e = max(e, maxx + 0.01)
    except Exception:
        pass

    bbox = f"{s},{w},{n},{e}"

    # Single lightweight query: fetch ONLY relation tags (no geometry needed)
    query = (
        f"[out:json][timeout:60];\n"
        f"(\n"
        f'  relation["route"="bus"]({bbox});\n'
        f'  relation["route"="tram"]({bbox});\n'
        f'  relation["route"="train"]({bbox});\n'
        f'  relation["route"="light_rail"]({bbox});\n'
        f'  relation["route"="subway"]({bbox});\n'
        f'  relation["route"="trolleybus"]({bbox});\n'
        f'  relation["route"="ferry"]({bbox});\n'
        f");\n"
        f"out tags;\n"  # tags-only — no geometry, very fast
    )

    print(f"[osm] Downloading operator metadata for {city['name']} …")
    data = None
    for url in OVERPASS_URLS:
        try:
            req = urllib.request.Request(
                url, data=query.encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
            print(f"[osm]   ok via {url}")
            break
        except Exception as ex:
            print(f"[osm]   failed {url}: {ex}")
            import time; time.sleep(5)

    # Aggregate: collect unique operator+mode combos and route counts
    # key = (operator_or_network, mode)
    agg: dict[tuple[str, str], int] = {}
    colour_map: dict[str, str] = {}  # operator → first colour seen

    if data:
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            route_type = tags.get("route", "")
            mode = {
                "bus": "bus", "tram": "tram", "train": "train",
                "light_rail": "train", "subway": "train",
                "trolleybus": "bus", "ferry": "ferry",
            }.get(route_type, route_type)

            operator = (tags.get("operator") or "").strip()
            network = (tags.get("network") or "").strip()
            colour = (tags.get("colour") or tags.get("color") or "").strip()

            name = operator or network
            if not name:
                continue

            key = (name, mode)
            agg[key] = agg.get(key, 0) + 1

            if colour and name not in colour_map:
                colour_map[name] = colour

    # Build operator list, merging modes per unique name.
    # Store ONLY raw OSM data here — brand/colour enrichment happens at
    # serve time via operator_brands.json so colours can be changed without
    # re-downloading.
    by_name: dict[str, dict] = {}
    for (name, mode), count in agg.items():
        if name not in by_name:
            by_name[name] = {
                "name":        name,
                "modes":       [],
                "colour_osm":  colour_map.get(name) or None,
                "route_count": 0,
                "source":      "osm",
            }
        entry = by_name[name]
        if mode not in entry["modes"]:
            entry["modes"].append(mode)
        entry["route_count"] += count

    # Add airline operators (not in OSM route relations)
    airline_names = _AIRPORT_AIRLINES.get(city_id, _DEFAULT_AIRLINES)
    for aname in airline_names:
        if aname not in by_name:
            by_name[aname] = {
                "name":        aname,
                "modes":       ["plane"],
                "colour_osm":  None,
                "route_count": 0,
                "source":      "static",
            }
        elif "plane" not in by_name[aname]["modes"]:
            by_name[aname]["modes"].append("plane")

    operators = sorted(by_name.values(), key=lambda o: (-o["route_count"], o["name"]))

    bundle = {"city_id": city_id, "operators": operators}

    os.makedirs(_city_dir(city_id), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    mode_summary = {}
    for op in operators:
        for m in op["modes"]:
            mode_summary[m] = mode_summary.get(m, 0) + 1
    print(f"[osm]   {len(operators)} operators: {mode_summary}")

    return bundle


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    from collections import Counter
    ZONE_NAMES = {
        0: "Road", 1: "Res-Lo", 2: "Res-Med", 3: "Res-Hi",
        4: "Commercial", 5: "Industrial", 6: "Hospital", 7: "Park", 8: "School",
        9: "Pharmacy", 10: "University", 11: "Grocery", 12: "Dental", 13: "Veterinary",
    }
    data = load_city("vaxjo")
    buildings, roads = data["buildings"], data["roads"]

    print(f"\nVäxjö: {len(buildings)} buildings, {len(roads)} roads")
    total_pop = sum(b["pop"] for b in buildings)
    print(f"Estimated total population: {total_pop:,}")
    for zone, count in sorted(Counter(b["zone"] for b in buildings).items()):
        print(f"  {ZONE_NAMES.get(zone, zone):12s}: {count:5d} buildings")

    # Road type breakdown
    road_types = Counter(r["type"] for r in roads)
    print("\nRoad types:")
    for t, c in sorted(road_types.items(), key=lambda kv: -kv[1]):
        print(f"  {t:18s}: {c:4d}")
