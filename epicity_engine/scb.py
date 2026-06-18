"""
scb.py — Statistics Sweden (SCB) demographic data, parameterised per city.

Downloads once, caches forever. No runtime network access after the first
successful run.  Two SCB endpoints are used, both free and unauthenticated:

  1. PxWebApi v1 table FolkmDesoAldKon — population by DeSO × age × sex × year
     https://api.scb.se/OV0104/v1/doris/en/ssd/BE/BE0101/BE0101Y/FolkmDesoAldKon
  2. WFS GeoServer — DeSO 2025 polygon geometries (kommunkod filter)
     https://geodata.scb.se/geoserver/stat/wfs

Per-city files live under data/{city_id}/:
  • deso_geo_raw.geojson  — raw WFS payload (cached)
  • deso_pop_raw.json     — raw PxWeb payload  (cached)
  • deso.json             — processed bundle consumed by osm.py

The processed bundle has the shape:

    {
      "meta":  {"age_frac": {"child": 0.18, "adult": 0.62, "elder": 0.20},
                "city_total": 95000, "source": "..."},
      "areas": [
        {
          "deso":    "0780A0010",
          "polygon": [[[lon, lat], ...]],   # GeoJSON Polygon coordinates
          "total":   1234,
          "age":     {"child": ..., "adult": ..., "elder": ...}
        },
        ...
      ]
    }
"""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
NATIONAL_DIR = os.path.join(DATA_DIR, "_national")


def _city_dir(city_id: str) -> str:
    return os.path.join(DATA_DIR, city_id)


def _paths(city_id: str) -> dict[str, str]:
    d = _city_dir(city_id)
    return {
        "raw_geo":           os.path.join(d, "deso_geo_raw.geojson"),
        "raw_pop":           os.path.join(d, "deso_pop_raw.json"),
        "processed":         os.path.join(d, "deso.json"),
        # Extended per-city SCB bundles (added in the metapopulation work —
        # see scb.load_households / load_education / etc. below). Each fetcher
        # writes one "raw" file with the verbatim PxWeb response plus one
        # processed file with a small kommun-keyed summary.
        "raw_birth":         os.path.join(d, "country_of_birth_raw.json"),
        "birth":             os.path.join(d, "country_of_birth.json"),
        "raw_households":    os.path.join(d, "households_raw.json"),
        "households":        os.path.join(d, "households.json"),
        "raw_education":     os.path.join(d, "education_raw.json"),
        "education":         os.path.join(d, "education.json"),
        "raw_income":        os.path.join(d, "income_raw.json"),
        "income":            os.path.join(d, "income.json"),
        "raw_deso_income":   os.path.join(d, "deso_income_raw.json"),
        "deso_income":       os.path.join(d, "deso_income.json"),
        "raw_employment":    os.path.join(d, "employment_raw.json"),
        "employment":        os.path.join(d, "employment.json"),
    }


def _national_paths() -> dict[str, str]:
    """
    Paths for nationwide (all-of-Sweden) SCB data used by the metapopulation
    wrapper. Lives under data/_national/ — a single dataset shared across
    all cities, not per-city.
    """
    d = NATIONAL_DIR
    return {
        "raw_kommuner":   os.path.join(d, "kommuner_raw.json"),
        "kommuner":       os.path.join(d, "kommuner.json"),
        "raw_centroids":  os.path.join(d, "kommun_centroids_raw.geojson"),
        "centroids":      os.path.join(d, "kommun_centroids.json"),
        "raw_flows":      os.path.join(d, "commuter_flows_raw.json"),
        "flows":          os.path.join(d, "commuter_flows.json"),
    }


# ── Configuration ─────────────────────────────────────────────────────────────

# SCB PxWeb Alder codes are 5-year bins: '-4', '5-9', ..., '75-79', '80-'
# Map each bin to one of our three buckets.
_AGE_BUCKETS: dict[str, tuple[str, ...]] = {
    "child": ("-4", "5-9", "10-14"),
    "adult": ("15-19", "20-24", "25-29", "30-34", "35-39",
              "40-44", "45-49", "50-54", "55-59", "60-64"),
    "elder": ("65-69", "70-74", "75-79", "80-"),
}
_BIN_TO_BUCKET: dict[str, str] = {
    b: bucket for bucket, bins in _AGE_BUCKETS.items() for b in bins
}

_PXWEB_URL = ("https://api.scb.se/OV0104/v1/doris/en/ssd/BE/BE0101/"
              "BE0101Y/FolkmDesoAldKon")
_WFS_URL = (
    "https://geodata.scb.se/geoserver/stat/wfs"
    "?service=WFS&request=GetFeature&version=1.1.0"
    "&typenames=stat:DeSO_2025&outputFormat=application/json"
    "&srsName=EPSG:4326&CQL_FILTER=kommunkod=%27{kommun}%27"
)

# ── Extended PxWeb tables (metapopulation pass) ──────────────────────────────
#
# SCB occasionally renames or retires tables. If any of these start returning
# 404 the table browser at https://statistikdatabasen.scb.se/pxweb/en/ssd/ is
# the source of truth. Each fetcher below catches HTTPError individually, so
# a stale code here fails *one* dataset — it does not abort the pipeline.
#
_BASE = "https://api.scb.se/OV0104/v1/doris/en/ssd"

# Population by region, age, sex, country of birth (inrikes/utrikes födda).
# "BefolkningR1860" lives under BE/BE0101/BE0101E.
_PXWEB_BIRTH_URL = f"{_BASE}/BE/BE0101/BE0101E/InrUtrFoddaRegAlKon"

# Households by region, type of housing and size (HE0111A/HushallT26).
# The original LE0102T18 was retired from PxWeb; this table provides
# household counts by 1–7+ person sizes per kommun.
_PXWEB_HOUSEHOLDS_URL = f"{_BASE}/HE/HE0111/HE0111A/HushallT26"

# Educational attainment: population 25–64 by highest level of education × region × sex.
_PXWEB_EDUCATION_URL = f"{_BASE}/UF/UF0506/UF0506B/Utbildning"

# Disposable income per consumption unit by region.
_PXWEB_INCOME_URL = f"{_BASE}/HE/HE0110/HE0110A/SamForvInk4"
# DeSO-level net income, SEK thousands, annual mean. 2024 release.
_PXWEB_DESO_INCOME_URL = f"{_BASE}/HE/HE0110/HE0110I/Tab2InkDesoRegso"

# Employed day-population by region × industry (SNI 2007).
_PXWEB_EMPLOYMENT_URL = f"{_BASE}/AM/AM0207/AM0207L/AM0207NA16N20"

# Nationwide total population by kommun + single-year age (used for
# 290-kommun coarse nodes). "FolkmangdNov" has no Civilstand breakdown, so
# the query stays well under the PxWeb cell limit.
_PXWEB_KOMMUNER_URL = f"{_BASE}/BE/BE0101/BE0101A/FolkmangdNov"

# Commuter flows — gainfully employed commuters 16-74 by municipality of
# residence (Bostadskommun) and municipality of work (Arbetsstallekommun).
# This is the gravity input for the metapopulation coupling.
_PXWEB_FLOWS_URL = f"{_BASE}/AM/AM0207/AM0207Z/AM0207PendlKomA04N"

# SCB's stat: WFS namespace does not expose a kommun polygon layer (only
# DeSO/RegSO/tätorter/grids). For now we derive coarse-node centroids from
# osm.CITIES — good for the 15 collected cities, other kommuner get (0, 0)
# until a kommun-boundary source is wired in.


def _pxweb_fetch(url: str, query: dict, cache_path: str,
                 label: str) -> dict | None:
    """
    Download a PxWeb JSON response, cache it to disk, and return the parsed
    body. Returns None (and prints a warning) on any error — callers should
    treat a missing result as "dataset unavailable, continue without it"
    rather than aborting the whole pipeline.

    Re-runs are free: if ``cache_path`` already exists, we skip the network
    entirely and just reload the file.
    """
    if os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[scb] {label}: cached file unreadable, refetching ({e})")

    print(f"[scb] {label}: POST {url}")
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(query).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "User-Agent":   "EpiCity/1.0"},
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = json.loads(r.read())
    except Exception as e:
        print(f"[scb] {label}: FAILED ({e}); skipping")
        return None

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(raw, f)
    print(f"[scb] {label}: cached {cache_path} "
          f"({len(raw.get('data', []))} rows)")
    return raw


def _pxweb_metadata(url: str) -> dict | None:
    """
    GET a PxWeb table's metadata (variable list + value codes). Used to
    discover things like "which SNI industry codes exist" without hard-coding
    them. Returns None on any error.
    """
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[scb] metadata fetch FAILED for {url}: {e}")
        return None

# ── Small helpers ─────────────────────────────────────────────────────────────


def _to_int(v: Any) -> int:
    """PxWeb returns counts as strings; coerce tolerantly (missing → 0)."""
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":"))


def _flatten_coords(geom: dict) -> list[list[float]]:
    """
    Walk a GeoJSON Polygon / MultiPolygon and return every [lon, lat]
    vertex as a flat list. Used to average polygon vertices into a
    cheap centroid.
    """
    if not geom:
        return []
    t = geom.get("type")
    coords = geom.get("coordinates") or []
    out: list[list[float]] = []
    if t == "Polygon":
        for ring in coords:
            for pt in ring:
                out.append(pt)
    elif t == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                for pt in ring:
                    out.append(pt)
    return out


# ── Public API ────────────────────────────────────────────────────────────────


def _kommunkod_for(city_id: str) -> str:
    """
    Look up the SCB kommunkod for a city by reading osm.CITIES at call
    time (avoids a circular import at module load).
    """
    import osm  # local import — osm.py doesn't import scb at module level
    if city_id not in osm.CITIES:
        raise ValueError(f"Unknown city: {city_id!r}")
    meta = osm.CITIES[city_id]
    if "kommunkod" not in meta:
        raise ValueError(
            f"City {city_id!r} has no 'kommunkod' field in osm.CITIES — "
            f"add the 4-digit SCB municipality code (e.g. '0780' for Växjö)."
        )
    return meta["kommunkod"]


def load_deso(city_id: str) -> dict[str, Any]:
    """
    Return the cached SCB DeSO bundle for `city_id`. Builds the cache from
    raw downloads the first time; subsequent calls are pure disk reads.
    """
    paths = _paths(city_id)
    if os.path.exists(paths["processed"]):
        with open(paths["processed"], encoding="utf-8") as f:
            return json.load(f)
    return _build_processed_cache(city_id)


# ── Extended per-city SCB datasets ────────────────────────────────────────────
#
# These are lightweight compared to DeSO (one row per kommun, not per DeSO
# polygon) and are orthogonal to the focused-city SEIR engine. They exist to
# feed the metapopulation wrapper and future socioeconomic / behavioural
# heterogeneity in the sim.
#
# Each loader:
#   • returns the processed dict on success
#   • returns {} on any error (missing table, bad response, etc.)
#   • caches both the raw PxWeb response and the processed summary
#
# Processed bundles are small JSON objects keyed on the single kommun this
# city lives in — callers can merge many of them later without worrying
# about shape collisions.

def load_country_of_birth(city_id: str) -> dict[str, Any]:
    kommun = _kommunkod_for(city_id)
    paths  = _paths(city_id)
    if os.path.exists(paths["birth"]):
        with open(paths["birth"], encoding="utf-8") as f:
            return json.load(f)

    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": [kommun]}},
            {"code": "Fodelseregion",
             "selection": {"filter": "item", "values": ["09", "11"]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_BIRTH_URL, query, paths["raw_birth"],
                       f"country_of_birth[{city_id}]")
    if not raw:
        return {}

    # Fodelseregion values: 09 = born in Sweden, 11 = foreign-born.
    # Aggregate over the implicit Alder/Kon dimensions SCB collapses for us.
    cols = raw.get("columns", [])
    fr_idx = next((i for i, c in enumerate(cols)
                   if c.get("code") == "Fodelseregion"), None)
    if fr_idx is None:
        return {}
    native = foreign = 0
    for row in raw.get("data", []):
        n = _to_int(row["values"][0])
        code = row["key"][fr_idx]
        if code == "09":
            native += n
        elif code == "11":
            foreign += n
    total = native + foreign
    out = {
        "kommunkod":      kommun,
        "native_born":    native,
        "foreign_born":   foreign,
        "total":          total,
        "foreign_frac":   (foreign / total) if total else 0.0,
        "source":         _PXWEB_BIRTH_URL,
    }
    _write_json(paths["birth"], out)
    return out


def load_households(city_id: str) -> dict[str, Any]:
    kommun = _kommunkod_for(city_id)
    paths  = _paths(city_id)
    if os.path.exists(paths["households"]):
        with open(paths["households"], encoding="utf-8") as f:
            return json.load(f)

    # HushallT26: households by region, housing type, household size.
    # Request only the "Number of households" content code, all housing
    # types summed implicitly via individual size buckets + TOTAL.
    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": [kommun]}},
            {"code": "Hushallsstorlek",
             "selection": {"filter": "item",
                           "values": ["1P", "2P", "3P", "4P",
                                      "5P", "6P", "7+P", "TOTAL"]}},
            {"code": "ContentsCode",
             "selection": {"filter": "item", "values": ["HE0111BZ"]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_HOUSEHOLDS_URL, query, paths["raw_households"],
                       f"households[{city_id}]")
    if not raw:
        return {}

    # Parse size buckets. Each row key contains [Region, Boendeform,
    # Hushallsstorlek, Tid]. We sum across all housing types.
    size_dist: dict[str, int] = {}
    total = 0
    for row in raw.get("data", []):
        n = _to_int(row["values"][0])
        key = row["key"]
        # Hushallsstorlek is typically key[2] — look for the size token.
        size_tok = None
        for tok in key:
            if tok == "TOTAL":
                total += n
                break
            if tok.endswith("P"):
                # "1P" → "1", "7+P" → "7+"
                digit = tok.removesuffix("P")
                size_tok = digit.rstrip("+")
                break
        if size_tok and size_tok.isdigit():
            size_dist[size_tok] = size_dist.get(size_tok, 0) + n

    # Compute average household size from the distribution.
    avg_size = 0.0
    if size_dist:
        num = sum(int(k) * v for k, v in size_dist.items())
        den = sum(size_dist.values()) or 1
        avg_size = round(num / den, 2)

    out = {
        "kommunkod":          kommun,
        "total_households":   total,
        "size_distribution":  size_dist,
        "avg_household_size": avg_size,
        "source":             _PXWEB_HOUSEHOLDS_URL,
    }
    _write_json(paths["households"], out)
    return out


def load_education(city_id: str) -> dict[str, Any]:
    kommun = _kommunkod_for(city_id)
    paths  = _paths(city_id)
    if os.path.exists(paths["education"]):
        with open(paths["education"], encoding="utf-8") as f:
            return json.load(f)

    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": [kommun]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_EDUCATION_URL, query, paths["raw_education"],
                       f"education[{city_id}]")
    if not raw:
        return {}

    # Most SCB education tables label levels with a 2-char code — we just
    # bucket the whole response by whatever the level-variable value is.
    by_level: dict[str, int] = {}
    total = 0
    for row in raw.get("data", []):
        n = _to_int(row["values"][0])
        total += n
        # The level variable is usually the last non-region/non-sex/non-age
        # key; we just take key[-1] as a coarse summary label.
        lvl = row["key"][-1] if row["key"] else "unknown"
        by_level[lvl] = by_level.get(lvl, 0) + n

    out = {
        "kommunkod":    kommun,
        "total":        total,
        "by_level":     by_level,
        "source":       _PXWEB_EDUCATION_URL,
    }
    _write_json(paths["education"], out)
    return out


def load_income(city_id: str) -> dict[str, Any]:
    kommun = _kommunkod_for(city_id)
    paths  = _paths(city_id)
    if os.path.exists(paths["income"]):
        with open(paths["income"], encoding="utf-8") as f:
            return json.load(f)

    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": [kommun]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_INCOME_URL, query, paths["raw_income"],
                       f"income[{city_id}]")
    if not raw:
        return {}

    # We just collect all numeric values and take the first as "representative"
    # — different income tables have different statistic breakdowns; the
    # processed bundle is meant as a provenance record, not as a full
    # breakdown. Callers who need the full shape should read the raw file.
    values: list[float] = []
    for row in raw.get("data", []):
        try:
            values.append(float(row["values"][0]))
        except (TypeError, ValueError):
            pass
    out = {
        "kommunkod":    kommun,
        "median_ish":   values[0] if values else None,
        "n_rows":       len(values),
        "source":       _PXWEB_INCOME_URL,
    }
    _write_json(paths["income"], out)
    return out


def load_deso_income(city_id: str) -> dict[str, float]:
    """
    Return mean net income (SEK thousands) per DeSO for a city.

    DeSO-level income is privacy-suppressed in SCB's public PxWeb
    output (every DeSO row comes back as "..", the "data withheld"
    marker). The coarser RegSO aggregation is *not* suppressed, so
    this function:

      1. reads the DeSO→RegSO crosswalk from the already-cached
         `deso_geo_raw.geojson` WFS response (each feature carries
         a `regsokod` property),
      2. fetches net income per RegSO via PxWeb table
         HE0110I/Tab2InkDesoRegso (using the `_RegSO2025` suffixed
         Region codes),
      3. broadcasts each RegSO's income to every DeSO that lives
         inside it, so `{deso_id: income}` callers don't need to
         know the RegSO layer exists.

    Returns an empty dict on any failure — callers downstream check
    for missing `income` and gracefully disable the viz mode.
    Results are cached per city under `data/<city>/deso_income.json`.
    """
    paths = _paths(city_id)
    if os.path.exists(paths["deso_income"]):
        with open(paths["deso_income"], encoding="utf-8") as f:
            return json.load(f)

    # 1. Build the DeSO → RegSO crosswalk from the cached WFS payload.
    geo_path = paths["raw_geo"]
    if not os.path.exists(geo_path):
        print(f"[scb] No DeSO WFS cache for {city_id} — can't fetch income")
        return {}
    try:
        with open(geo_path, encoding="utf-8") as f:
            geo = json.load(f)
    except Exception as e:
        print(f"[scb] deso_geo_raw.geojson unreadable: {e}")
        return {}

    deso_to_regso: dict[str, str] = {}
    for feat in geo.get("features", []):
        p = feat.get("properties") or {}
        deso = p.get("desokod")
        regso = p.get("regsokod")
        if deso and regso:
            deso_to_regso[deso] = regso
    if not deso_to_regso:
        print(f"[scb] WFS payload had no regsokod for {city_id}")
        return {}

    # 2. Query PxWeb for RegSO-level net income. The valid Region code
    # for 2025 geometry is the `<regso>_RegSO2025` suffixed form.
    unique_regsos = sorted(set(deso_to_regso.values()))
    suffixed = [f"{r}_RegSO2025" for r in unique_regsos]

    query = {
        "query": [
            {"code": "Region",             "selection": {"filter": "item", "values": suffixed}},
            {"code": "Inkomstkomponenter", "selection": {"filter": "item", "values": ["240"]}},
            {"code": "Kon",                "selection": {"filter": "item", "values": ["1+2"]}},
            {"code": "ContentsCode",       "selection": {"filter": "item", "values": ["000008A4"]}},
            {"code": "Tid",                "selection": {"filter": "top",  "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_DESO_INCOME_URL, query, paths["raw_deso_income"],
                      f"deso_income[{city_id}]")
    if not raw:
        return {}

    # Parse PxWeb rows back into {regso_plain: value_float}. Rows with
    # ".." (privacy-suppressed) are skipped — broadcasting those to
    # downstream buildings is worse than leaving income unset.
    regso_income: dict[str, float] = {}
    for row in raw.get("data", []):
        key = (row.get("key") or [None])[0]
        if not key:
            continue
        plain = key.replace("_RegSO2025", "")
        try:
            v = float(row["values"][0])
        except (TypeError, ValueError, IndexError):
            continue
        regso_income[plain] = v

    # 3. Broadcast RegSO income back to each DeSO that lives in it.
    out: dict[str, float] = {}
    for deso, regso in deso_to_regso.items():
        v = regso_income.get(regso)
        if v is not None:
            out[deso] = v

    _write_json(paths["deso_income"], out)
    return out


def load_employment(city_id: str) -> dict[str, Any]:
    """
    Employed day-population by SNI 2007 industry code.
    Returns {kommunkod, total, by_industry: {sni_code: count}, source}.
    """
    kommun = _kommunkod_for(city_id)
    paths  = _paths(city_id)
    if os.path.exists(paths["employment"]):
        with open(paths["employment"], encoding="utf-8") as f:
            return json.load(f)

    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": [kommun]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_EMPLOYMENT_URL, query, paths["raw_employment"],
                       f"employment[{city_id}]")
    if not raw:
        return {}

    by_industry: dict[str, int] = {}
    total = 0
    for row in raw.get("data", []):
        n = _to_int(row["values"][0])
        total += n
        # Industry slot is usually key[1] (Region is key[0]).
        if len(row["key"]) >= 2:
            sni = row["key"][1]
            by_industry[sni] = by_industry.get(sni, 0) + n

    out = {
        "kommunkod":    kommun,
        "total":        total,
        "by_industry":  by_industry,
        "source":       _PXWEB_EMPLOYMENT_URL,
    }
    _write_json(paths["employment"], out)
    return out


def load_regso_names(city_id: str) -> dict[str, str]:
    """
    Return {deso_code: neighborhood_name} by mapping each DeSO area to its
    parent RegSO, then looking up the human-readable RegSO name from the
    PxWeb table metadata.

    RegSO (Regional Statistical Areas) are larger than DeSO areas and carry
    descriptive names like "Teleborg", "Hovshaga", "Centrum" etc.  The name
    is extracted from the PxWeb `valueTexts` field for the Region variable,
    where the text has the form ``"Kommun (Neighborhood name)"``.

    Cached per city under ``data/<city>/regso_names.json``.
    """
    d = _city_dir(city_id)
    cache_path = os.path.join(d, "regso_names.json")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    # 1. Build DeSO → RegSO crosswalk from the cached WFS GeoJSON.
    geo_path = os.path.join(d, "deso_geo_raw.geojson")
    if not os.path.exists(geo_path):
        return {}
    try:
        with open(geo_path, encoding="utf-8") as f:
            geo = json.load(f)
    except Exception:
        return {}

    deso_to_regso: dict[str, str] = {}
    for feat in geo.get("features", []):
        p = feat.get("properties") or {}
        deso = p.get("desokod")
        regso = p.get("regsokod")
        if deso and regso:
            deso_to_regso[deso] = regso
    if not deso_to_regso:
        return {}

    # 2. Fetch PxWeb table metadata to get RegSO name labels.
    regso_label: dict[str, str] = {}
    meta = _pxweb_metadata(_PXWEB_URL)
    if meta:
        region_var = next(
            (v for v in meta.get("variables", []) if v.get("code") == "Region"),
            None,
        )
        if region_var:
            codes = region_var.get("values", [])
            texts = region_var.get("valueTexts", [])
            for code, text in zip(codes, texts):
                # RegSO codes look like "0780R001" or "0780R001_RegSO2025".
                plain = code.replace("_RegSO2025", "")
                if "R" in plain and len(plain) == 8:
                    # Text is "Kommun (Neighborhood)" — extract the part
                    # inside parentheses.
                    if "(" in text and text.endswith(")"):
                        name = text[text.index("(") + 1 : -1]
                    else:
                        name = text
                    regso_label[plain] = name

    # 3. Map each DeSO to its RegSO name.
    out: dict[str, str] = {}
    for deso, regso in deso_to_regso.items():
        name = regso_label.get(regso)
        if name:
            out[deso] = name
    if out:
        _write_json(cache_path, out)
    return out


def load_deso_gender(city_id: str) -> dict[str, dict[str, float]]:
    """
    Return {deso_code: {"male_frac": float, "female_frac": float}} for each
    DeSO area in the city, derived from FolkmDesoAldKon with sex=1 (male)
    and sex=2 (female) separately.

    Cached per city under ``data/<city>/deso_gender.json``.
    """
    kommun = _kommunkod_for(city_id)
    d = _city_dir(city_id)
    cache_path = os.path.join(d, "deso_gender.json")
    raw_path = os.path.join(d, "deso_gender_raw.json")

    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    # Discover DeSO region codes for this kommun.
    meta = _pxweb_metadata(_PXWEB_URL)
    if not meta:
        return {}
    region_var = next(
        (v for v in meta.get("variables", []) if v.get("code") == "Region"),
        None,
    )
    if not region_var:
        return {}
    deso_codes = [
        c for c in region_var["values"]
        if c.startswith(kommun) and c.endswith("_DeSO2025")
    ]
    if not deso_codes:
        return {}

    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": deso_codes}},
            {"code": "Alder",
             "selection": {"filter": "item", "values": ["totalt"]}},
            {"code": "Kon",
             "selection": {"filter": "item", "values": ["1", "2"]}},
            {"code": "ContentsCode",
             "selection": {"filter": "item", "values": ["000007Y7"]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_URL, query, raw_path,
                       f"deso_gender[{city_id}]")
    if not raw:
        return {}

    # Parse: key = [region, age, sex, year], values = [count].
    # Sex "1" = male, "2" = female.
    counts: dict[str, dict[str, int]] = {}  # {deso: {"1": n, "2": n}}
    for row in raw.get("data", []):
        key = row["key"]
        region = key[0].removesuffix("_DeSO2025")
        sex = key[2]
        n = _to_int(row["values"][0])
        counts.setdefault(region, {})
        counts[region][sex] = counts[region].get(sex, 0) + n

    out: dict[str, dict[str, float]] = {}
    for deso, sc in counts.items():
        m = sc.get("1", 0)
        f = sc.get("2", 0)
        total = m + f
        if total > 0:
            out[deso] = {
                "male_frac": round(m / total, 4),
                "female_frac": round(f / total, 4),
            }
    if out:
        _write_json(cache_path, out)
    return out


def load_extended(city_id: str) -> dict[str, Any]:
    """
    Run every extended per-city fetcher in sequence. Each failure is
    swallowed with a printed warning; the return value is whatever bundles
    succeeded, keyed by dataset name.
    """
    return {
        "country_of_birth": load_country_of_birth(city_id),
        "households":       load_households(city_id),
        "education":        load_education(city_id),
        "income":           load_income(city_id),
        "employment":       load_employment(city_id),
    }


# ── Nationwide (one-off) fetchers ─────────────────────────────────────────────
#
# Unlike the per-city bundles above, these are fetched once and shared across
# every city. They power the metapopulation wrapper's 290-kommun coarse-node
# pool and the commuter-flow coupling matrix.

def _single_year_age_to_bucket(age_code: str) -> str | None:
    """
    Map a PxWeb single-year age code (from FolkmangdNov) to our
    child/adult/elder bucket. 'tot' is filtered out by the caller.
    '100+' is elder.
    """
    if age_code == "tot":
        return None
    try:
        n = int(age_code.rstrip("+"))
    except ValueError:
        return None
    if n < 15:
        return "child"
    if n < 65:
        return "adult"
    return "elder"


def load_national_kommuner() -> dict[str, Any]:
    """
    Return {kommunkod: {"name": str, "total": int, "age": {child,adult,elder}}}
    for all 290 Swedish kommuner. Uses FolkmangdNov (population 1 November
    by region × single-year age × sex), then maps single-year ages into our
    child (<15) / adult (15-64) / elder (65+) buckets to match the focused
    engine's stratification.
    """
    paths = _national_paths()
    if os.path.exists(paths["kommuner"]):
        with open(paths["kommuner"], encoding="utf-8") as f:
            return json.load(f)

    # Discover the 290 kommun codes from the table metadata (4-digit Region
    # values). This is more robust than hard-coding "all regions" since the
    # Region variable also contains county and country totals. The parallel
    # valueTexts array gives us canonical kommun names for free.
    meta = _pxweb_metadata(_PXWEB_KOMMUNER_URL)
    if not meta:
        return {}
    region_var = next((v for v in meta["variables"] if v["code"] == "Region"), None)
    if not region_var:
        return {}
    codes = region_var["values"]
    texts = region_var.get("valueTexts") or codes
    code_to_name = {c: t for c, t in zip(codes, texts) if len(c) == 4}
    kommuner = list(code_to_name.keys())
    if not kommuner:
        return {}

    query = {
        "query": [
            {"code": "Region",
             "selection": {"filter": "item", "values": kommuner}},
            # Skip 'tot' — we want single years so we can bucket them cleanly.
            {"code": "Alder",
             "selection": {"filter": "all", "values": ["*"]}},
            {"code": "Kon",
             "selection": {"filter": "item", "values": ["1", "2"]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_KOMMUNER_URL, query, paths["raw_kommuner"],
                       "national_kommuner")
    if not raw:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for row in raw.get("data", []):
        key = row["key"]
        # Expected key order: Region, Alder, Kon, Tid.
        if len(key) < 4:
            continue
        region, age_code = key[0], key[1]
        if len(region) != 4:
            continue
        bucket_name = _single_year_age_to_bucket(age_code)
        if not bucket_name:
            continue
        n = _to_int(row["values"][0])
        entry = out.setdefault(region, {
            "kommunkod": region,
            "name":      code_to_name.get(region),
            "total":     0,
            "age":       {"child": 0, "adult": 0, "elder": 0},
        })
        entry["age"][bucket_name] += n
        entry["total"] += n

    _write_json(paths["kommuner"], out)
    print(f"[scb] national_kommuner: {len(out)} kommuner "
          f"({sum(v['total'] for v in out.values()):,} residents)")
    return out


def load_kommun_centroids() -> dict[str, Any]:
    """
    Return {kommunkod: {"lat": float, "lon": float, "name": str}}.

    SCB's public WFS does not expose a kommun polygon layer — only DeSO,
    RegSO, tätorter and grids. So until we wire in Lantmäteriet or OSM
    admin boundaries, centroids come from osm.CITIES (the 15 hand-curated
    cities we already have `center_lat/center_lon` for). Other kommuner
    get (0.0, 0.0) and the frontend treats those as "position unknown".
    """
    paths = _national_paths()
    if os.path.exists(paths["centroids"]):
        with open(paths["centroids"], encoding="utf-8") as f:
            return json.load(f)

    import osm  # local import — osm doesn't import scb at module load time
    out: dict[str, dict[str, Any]] = {}
    for city_id, cmeta in osm.CITIES.items():
        kommun = cmeta.get("kommunkod")
        if not kommun:
            continue
        out[kommun] = {
            "lat":  float(cmeta.get("center_lat") or 0.0),
            "lon":  float(cmeta.get("center_lon") or 0.0),
            "name": cmeta.get("name") or city_id,
        }

    _write_json(paths["centroids"], out)
    print(f"[scb] kommun_centroids: {len(out)} kommuner "
          f"(from osm.CITIES — WFS kommun layer unavailable)")
    return out


def load_commuter_flows() -> dict[str, dict[str, int]]:
    """
    Return the origin-destination commuter matrix as a nested dict:
    ``flows[from_kommun][to_kommun] = daily_commuter_count``.
    Zero and missing entries are dropped. Intra-kommun flows (diagonal)
    usually dominate by an order of magnitude — they're preserved so the
    metapopulation wrapper can recover "what fraction of residents work
    locally" without a second table lookup.
    """
    paths = _national_paths()
    if os.path.exists(paths["flows"]):
        with open(paths["flows"], encoding="utf-8") as f:
            return json.load(f)

    # Discover the actual Region codes from metadata so the query is
    # future-proof against the 290-kommuner list changing.
    meta = _pxweb_metadata(_PXWEB_FLOWS_URL)
    if not meta:
        return {}
    bk_var = next((v for v in meta["variables"] if v["code"] == "Bostadskommun"), None)
    ak_var = next((v for v in meta["variables"] if v["code"] == "Arbetsstallekommun"), None)
    if not bk_var or not ak_var:
        print("[scb] commuter_flows: unexpected variable names in metadata")
        return {}
    home_codes = [c for c in bk_var["values"] if len(c) == 4]
    work_codes = [c for c in ak_var["values"] if len(c) == 4]

    # The table has Kon values '1' (men), '2' (women), '4' (total).
    # We ask for '4' directly to avoid double-counting.
    query = {
        "query": [
            {"code": "Bostadskommun",
             "selection": {"filter": "item", "values": home_codes}},
            {"code": "Arbetsstallekommun",
             "selection": {"filter": "item", "values": work_codes}},
            {"code": "Kon",
             "selection": {"filter": "item", "values": ["4"]}},
            {"code": "Tid",
             "selection": {"filter": "top", "values": ["1"]}},
        ],
        "response": {"format": "json"},
    }
    raw = _pxweb_fetch(_PXWEB_FLOWS_URL, query, paths["raw_flows"],
                       "commuter_flows")
    if not raw:
        return {}

    flows: dict[str, dict[str, int]] = {}
    for row in raw.get("data", []):
        key = row["key"]
        if len(key) < 2:
            continue
        src, dst = key[0], key[1]
        n = _to_int(row["values"][0])
        if n <= 0:
            continue
        flows.setdefault(src, {})[dst] = n

    _write_json(paths["flows"], flows)
    non_zero = sum(len(d) for d in flows.values())
    print(f"[scb] commuter_flows: {len(flows)} source kommuner, "
          f"{non_zero} non-zero OD pairs")
    return flows


def load_national() -> dict[str, Any]:
    """Run all nationwide fetchers and return a merged summary."""
    kommuner  = load_national_kommuner()
    centroids = load_kommun_centroids()
    flows     = load_commuter_flows()
    # Stamp names from centroids onto the kommuner bundle if missing.
    for k, v in kommuner.items():
        if not v.get("name") and k in centroids:
            v["name"] = centroids[k].get("name")
    return {
        "kommuner":  kommuner,
        "centroids": centroids,
        "flows":     flows,
    }


# ── Processing ────────────────────────────────────────────────────────────────


def _build_processed_cache(city_id: str) -> dict[str, Any]:
    kommun = _kommunkod_for(city_id)
    geo = _fetch_geometry(city_id, kommun)
    pop = _fetch_population(city_id, kommun)   # {deso_id_no_suffix: {bin: count}}

    areas: list[dict[str, Any]] = []
    city_totals = {"child": 0, "adult": 0, "elder": 0}

    for feat in geo["features"]:
        props = feat["properties"]
        deso_id = props.get("desokod")
        if not deso_id or not deso_id.startswith(kommun):
            continue

        bins = pop.get(deso_id, {})
        bucket = {"child": 0, "adult": 0, "elder": 0}
        for b, n in bins.items():
            key = _BIN_TO_BUCKET.get(b)
            if key:
                bucket[key] += n
        total = sum(bucket.values())
        for k, v in bucket.items():
            city_totals[k] += v

        geom = feat["geometry"]
        # Keep only Polygon/MultiPolygon coordinates
        areas.append({
            "deso":    deso_id,
            "gtype":   geom["type"],
            "polygon": geom["coordinates"],
            "total":   total,
            "age":     bucket,
        })

    city_total = sum(city_totals.values()) or 1
    meta = {
        "age_frac": {k: v / city_total for k, v in city_totals.items()},
        "city_total": city_total,
        "source":     "SCB PxWeb FolkmDesoAldKon + WFS DeSO_2025",
        "kommunkod":  kommun,
    }

    out = {"meta": meta, "areas": areas}
    paths = _paths(city_id)
    os.makedirs(_city_dir(city_id), exist_ok=True)
    with open(paths["processed"], "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"[scb] Saved processed DeSO data: {paths['processed']} "
          f"({len(areas)} areas, {city_total:,} residents)")
    return out

# ── Raw downloads ─────────────────────────────────────────────────────────────


def _fetch_geometry(city_id: str, kommun: str) -> dict[str, Any]:
    paths = _paths(city_id)
    if os.path.exists(paths["raw_geo"]):
        with open(paths["raw_geo"], encoding="utf-8") as f:
            return json.load(f)

    url = _WFS_URL.format(kommun=kommun)
    print(f"[scb] Downloading DeSO polygons for {city_id} (kommunkod={kommun}) …")
    with urllib.request.urlopen(url, timeout=120) as r:
        data = json.loads(r.read())

    os.makedirs(_city_dir(city_id), exist_ok=True)
    with open(paths["raw_geo"], "w", encoding="utf-8") as f:
        json.dump(data, f)
    print(f"[scb] Cached raw geometry: {paths['raw_geo']} "
          f"({len(data.get('features', []))} features)")
    return data


def _fetch_population(city_id: str, kommun: str) -> dict[str, dict[str, int]]:
    """
    Query FolkmDesoAldKon for all of `kommun`'s DeSO 2025 areas, all 5-year
    age bins, both sexes, latest available year.  Returns
    {deso_id: {age_bin: count}} with the deso_id stripped of the
    '_DeSO2025' suffix to match WFS codes.
    """
    paths = _paths(city_id)
    if os.path.exists(paths["raw_pop"]):
        with open(paths["raw_pop"], encoding="utf-8") as f:
            raw = json.load(f)
    else:
        # Discover the DeSO 2025 region codes for this kommun from table metadata
        print(f"[scb] Fetching SCB table metadata …")
        with urllib.request.urlopen(_PXWEB_URL, timeout=60) as r:
            meta = json.loads(r.read())
        region_var = next(v for v in meta["variables"] if v["code"] == "Region")
        deso_codes = [
            c for c in region_var["values"]
            if c.startswith(kommun) and c.endswith("_DeSO2025")
        ]
        if not deso_codes:
            raise RuntimeError(
                f"No DeSO 2025 region codes found for kommunkod {kommun!r}. "
                f"Double-check the code (4 digits, leading zero for "
                f"municipalities < 1000)."
            )
        age_bins = [b for bins in _AGE_BUCKETS.values() for b in bins]

        query = {
            "query": [
                {"code": "Region",
                 "selection": {"filter": "item", "values": deso_codes}},
                {"code": "Alder",
                 "selection": {"filter": "item", "values": age_bins}},
                {"code": "Kon",
                 "selection": {"filter": "item", "values": ["1+2"]}},
                {"code": "Tid",
                 "selection": {"filter": "top", "values": ["1"]}},
            ],
            "response": {"format": "json"},
        }
        print(f"[scb] Downloading SCB population by age × DeSO "
              f"({len(deso_codes)} areas × {len(age_bins)} bins) for "
              f"{city_id} …")
        req = urllib.request.Request(
            _PXWEB_URL,
            data=json.dumps(query).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "User-Agent":   "EpiCity/1.0"},
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = json.loads(r.read())

        os.makedirs(_city_dir(city_id), exist_ok=True)
        with open(paths["raw_pop"], "w", encoding="utf-8") as f:
            json.dump(raw, f)
        print(f"[scb] Cached raw population: {paths['raw_pop']} "
              f"({len(raw.get('data', []))} rows)")

    # Reduce: PxWeb JSON = {"data": [{"key": [region, age, sex, year],
    #                                  "values": [n]}, ...]}
    out: dict[str, dict[str, int]] = {}
    for row in raw.get("data", []):
        key = row["key"]
        region, age_bin = key[0], key[1]
        n_str = row["values"][0]
        try:
            n = int(n_str)
        except (TypeError, ValueError):
            n = 0
        deso_short = region.removesuffix("_DeSO2025")
        out.setdefault(deso_short, {})
        out[deso_short][age_bin] = out[deso_short].get(age_bin, 0) + n
    return out

# ── Standalone runner ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    city_id = sys.argv[1] if len(sys.argv) > 1 else "vaxjo"
    bundle = load_deso(city_id)
    areas = bundle["areas"]
    meta  = bundle["meta"]
    print(f"\n{city_id}: {len(areas)} DeSO areas, "
          f"{meta['city_total']:,} residents total")
    print(f"Age mix: {meta['age_frac']}")
    for a in areas[:5]:
        print(f"  {a['deso']}: total={a['total']}  age={a['age']}")
