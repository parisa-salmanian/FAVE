"""
brazil.py — IBGE demographic data for Brazilian cities.

Mirrors scb.py's surface: same public function names, same output bundle
shape, same per-city + nationwide split. Swedish DeSO polygons become
Brazilian municipalities/setores; SCB PxWeb population becomes IBGE
SIDRA population by age. The engine downstream treats both identically
via the generic {areas: [{deso, polygon, total, age}]} schema, so
engine.Cell.deso holds an IBGE code for Brazilian buildings and a
Swedish DeSO id for Swedish ones — it's opaque.

Current state (simplified Phase 2):

    load_bairros(city_id)  — FULL: single-area city-wide bundle from
                             IBGE Malha (boundary) + IBGE SIDRA (pop by
                             age). One polygon covering the whole
                             municipality. Every building inside gets
                             the same city-wide age mix.

    load_national()        — STUB: raises NotImplementedError. The
                             metapop layer tolerates missing data and
                             no-ops gracefully for non-Swedish cities.

A later pass can upgrade load_bairros to real setor-censitário
granularity without touching the dispatcher, osm.load_city(),
engine.Cell, or the frontend — the bundle schema is stable.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _city_dir(city_id: str) -> str:
    return os.path.join(DATA_DIR, city_id)


def _paths(city_id: str) -> dict[str, str]:
    d = _city_dir(city_id)
    return {
        "raw_geo":   os.path.join(d, "bairros_geo_raw.geojson"),
        "raw_pop":   os.path.join(d, "bairros_pop_raw.json"),
        "processed": os.path.join(d, "bairros.json"),
    }


# ── IBGE endpoints ────────────────────────────────────────────────────────────
#
# Malha v3 — municipal boundary as GeoJSON. Works for every Brazilian
# município. `qualidade=maxima` gives the highest-detail polygon.
_IBGE_MALHA_URL = (
    "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{code}"
    "?formato=application/vnd.geo+json&qualidade=maxima"
)

# SIDRA table 9514 = Censo 2022 "População residente, por sexo e idade".
#   t/9514   — table id
#   n6/CODE  — nível 6 (município) filter
#   v/93     — variable 93 (população residente)
#   p/2022   — period (Censo 2022)
#   c2/6794  — classification 2 (sexo) = total (both sexes combined)
#   c287/all — classification 287 (age groups) = all 5-year buckets
_IBGE_SIDRA_URL = (
    "https://apisidra.ibge.gov.br/values/t/9514/n6/{code}/v/93/p/2022"
    "/c2/6794/c287/all"
)


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _http_get_json(url: str, timeout: int = 60) -> Any:
    """Tiny JSON GET. Raises urllib.error on failure."""
    req = urllib.request.Request(url, headers={
        "User-Agent": "EpiCity/1.0 (Claude Code; github.com/anthropics/claude-code)",
        "Accept":     "application/json, application/vnd.geo+json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ── Malha (boundary) fetch + normalise ────────────────────────────────────────

def _fetch_malha(admin_code: str, cache_path: str) -> dict:
    """
    Fetch the IBGE municipal boundary GeoJSON. Cached to disk so repeat
    runs and `osm.load_city()` don't re-hit the network.

    Normalises the response to a GeoJSON FeatureCollection so the existing
    `osm._load_municipality_boundary()` (which iterates `geo["features"]`)
    can read it unchanged whether IBGE returns a FeatureCollection, a
    bare Feature, or a bare Geometry.
    """
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    url = _IBGE_MALHA_URL.format(code=admin_code)
    print(f"[brazil] Fetching IBGE Malha: {url}")
    raw = _http_get_json(url)

    normalised = _normalise_to_feature_collection(raw)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(normalised, f, separators=(",", ":"))
    return normalised


def _normalise_to_feature_collection(raw: dict) -> dict:
    """Wrap a loose Feature or Geometry into a FeatureCollection."""
    t = raw.get("type")
    if t == "FeatureCollection":
        return raw
    if t == "Feature":
        return {"type": "FeatureCollection", "features": [raw]}
    if t in ("Polygon", "MultiPolygon"):
        return {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {},
                "geometry": raw,
            }],
        }
    # Unknown shape — return as-is; boundary loader will no-op.
    return raw


# ── SIDRA (population by age) fetch ───────────────────────────────────────────

def _fetch_sidra_population(admin_code: str, cache_path: str) -> list:
    """Fetch IBGE SIDRA table 9514 population by age. Cached to disk."""
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    url = _IBGE_SIDRA_URL.format(code=admin_code)
    print(f"[brazil] Fetching IBGE SIDRA: {url}")
    data = _http_get_json(url)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    return data


# SIDRA table 9514 response ships 5-year aggregate buckets INTERLEAVED with
# single-year and month-level sub-aggregates that compose them, e.g.
# "0 a 4 anos" (aggregate), "Menos de 1 ano" (sub), "1 ano", "2 anos", ...
# Summing everything double- or triple-counts. These regexes match only
# the mutually-exclusive top-level 5-year ranges so the sum equals the
# real municipal population.
_RE_AGE_RANGE = re.compile(r"^\s*(\d+)\s+a\s+(\d+)\s+anos?\s*$", re.IGNORECASE)
_RE_AGE_OPEN  = re.compile(r"^\s*(\d+)\s+anos?\s+ou\s+mais\s*$", re.IGNORECASE)


def _parse_sidra_age(pop_raw: list) -> dict[str, int]:
    """
    Collapse SIDRA table 9514's age groups into EpiCity's 3 buckets.

    SIDRA response layout: a list whose first element is a header row
    (variable labels), followed by data rows. Each data row has:
        D1N  municipality name         e.g. "Recife (PE)"
        D3N  period                     e.g. "2022"
        D4N  sex classification         e.g. "Total"
        D5N  age-group label            e.g. "0 a 4 anos" / "100 anos ou mais"
        V    population count (str)    ".." / "-" means suppressed

    The API returns nested hierarchies (5-year range + single-year +
    month-level) so we keep only the 5-year ranges, which are mutually
    exclusive and sum to the municipal total:

        "0 a 4 anos", "5 a 9 anos", ..., "95 a 99 anos", "100 anos ou mais"

    Bucketing (IBGE ranges align cleanly on 15 / 65 boundaries):
        child = 0-14   (all range-top < 15)
        adult = 15-64  (15 <= range-top < 65)
        elder = 65+    (range-top >= 65, includes "100 anos ou mais")
    """
    buckets = {"child": 0.0, "adult": 0.0, "elder": 0.0}

    for row in pop_raw[1:] if isinstance(pop_raw, list) else []:
        label = (row.get("D5N") or "").strip()
        if not label:
            continue

        val_str = row.get("V")
        if val_str is None or val_str in ("..", "-", "...", "X"):
            continue
        try:
            val = float(val_str)
        except (ValueError, TypeError):
            continue

        m = _RE_AGE_RANGE.match(label)
        if m:
            hi = int(m.group(2))
        else:
            m = _RE_AGE_OPEN.match(label)
            if not m:
                continue
            hi = 150   # open-ended elder bucket ("100 anos ou mais")

        if hi < 15:
            buckets["child"] += val
        elif hi < 65:
            buckets["adult"] += val
        else:
            buckets["elder"] += val

    return {k: int(round(v)) for k, v in buckets.items()}


# ── Polygon extraction for the single-area bundle ────────────────────────────

def _extract_polygon_coords(geo: dict) -> tuple[str, list]:
    """
    Pull the municipal polygon coordinates out of the normalised Malha
    FeatureCollection. Returns (gtype, coords) where gtype is "Polygon"
    or "MultiPolygon" and coords is the raw GeoJSON coordinates array
    matching that type.
    """
    for feat in geo.get("features", []):
        geom = feat.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if gtype in ("Polygon", "MultiPolygon") and coords:
            return gtype, coords
    return "Polygon", []


# ── Public API ────────────────────────────────────────────────────────────────

def load_bairros(city_id: str) -> dict:
    """
    Fetch the Brazilian municipality boundary + city-wide population by
    age for ``city_id`` from IBGE, cache to disk, and return a bundle
    matching scb.load_deso()'s shape.

    Simplified Phase 2 implementation: ONE area covering the whole
    municipality. Every building inside the polygon shares the city-wide
    age mix. A future pass can swap this out for setor-censitário-level
    polygons without changing the bundle schema or any downstream code.

    Bundle shape (matches scb.load_deso):

        {
          "meta":  {"age_frac": {...}, "city_total": int,
                    "source": str, "country": "BR", "admin_code": str},
          "areas": [{
            "deso":    str,          # IBGE admin code as opaque area id
            "gtype":   "Polygon" | "MultiPolygon",
            "polygon": [...GeoJSON coordinates...],
            "total":   int,
            "age":     {"child": int, "adult": int, "elder": int}
          }]
        }
    """
    import osm  # lazy to avoid cycles

    if city_id not in osm.CITIES:
        raise ValueError(f"Unknown city: {city_id!r}")

    meta = osm.CITIES[city_id]
    admin_code = meta.get("admin_code")
    if not admin_code:
        raise ValueError(
            f"City {city_id!r} has no 'admin_code' field — cannot fetch "
            f"IBGE data."
        )

    city_dir = _city_dir(city_id)
    os.makedirs(city_dir, exist_ok=True)
    paths = _paths(city_id)

    # Short-circuit on processed cache (osm.load_city may call us twice
    # per run, and --force wipes the city dir at the outer level).
    if os.path.exists(paths["processed"]):
        with open(paths["processed"], encoding="utf-8") as f:
            return json.load(f)

    # Fetch raw IBGE data (each file cached independently).
    geo     = _fetch_malha(admin_code, paths["raw_geo"])
    pop_raw = _fetch_sidra_population(admin_code, paths["raw_pop"])

    # Collapse SIDRA age buckets.
    age_dist   = _parse_sidra_age(pop_raw)
    city_total = int(sum(age_dist.values()))
    if city_total <= 0:
        print(f"[brazil] WARNING: zero city_total from SIDRA for {city_id}")
        age_frac = {"child": 0.18, "adult": 0.62, "elder": 0.20}
    else:
        age_frac = {k: v / city_total for k, v in age_dist.items()}

    # Extract the municipal polygon from the Malha response.
    gtype, coords = _extract_polygon_coords(geo)
    if not coords:
        print(f"[brazil] WARNING: no polygon found in Malha response for {city_id}")

    # Single-area bundle — matches scb.load_deso() schema.
    bundle = {
        "meta": {
            "age_frac":   age_frac,
            "city_total": city_total,
            "source":     "IBGE SIDRA 9514 (Censo 2022) + Malha v3",
            "country":    "BR",
            "admin_code": admin_code,
        },
        "areas": [
            {
                "deso":    admin_code,
                "gtype":   gtype,
                "polygon": coords,
                "total":   city_total,
                "age":     age_dist,
            }
        ],
    }

    with open(paths["processed"], "w", encoding="utf-8") as f:
        json.dump(bundle, f, separators=(",", ":"))

    print(
        f"[brazil] {city_id}: pop={city_total:,}, "
        f"child={age_frac['child']:.1%}, "
        f"adult={age_frac['adult']:.1%}, "
        f"elder={age_frac['elder']:.1%}, "
        f"polygon={gtype}"
    )

    return bundle


def load_national() -> dict:
    """
    Fetch nationwide Brazilian municipality data (population, centroids,
    commuter flows) — mirrors scb.load_national().

    **Phase 3 stub.** The metapop layer falls back to an empty graph for
    Brazilian cities today; Recife runs as a standalone (no neighbouring
    cities, zero external FOI), which is the same behaviour a Swedish
    city gets when --national data is missing.
    """
    raise NotImplementedError(
        "brazil.load_national is not implemented yet — "
        "see docs/ADD_CITY.md / Recife plan Phase 3."
    )
