"""
city_stats.py — Tiny per-city stats cache used by the macro view.

Problem: /api/cities/summary needs three numbers per city — population,
building_count, landmark_count — to drive marker sizing on the map.
Reading them out of each city.json (20–50 MB each) on every cold start
turns the macro view's first paint into a 30–60 s wait while the server
parses ~700 MB of JSON.

Solution: pre-bake those three numbers into one tiny file at
data/_national/city_stats.json:

    {
        "vaxjo":     {"population": 95000, "building_count": 34232, "landmark_count": 7},
        "stockholm": {"population": 980000, ...},
        ...
    }

The file is written incrementally by tools/download_city.py at the end
of each city build, and seeded for already-downloaded cities by
tools/backfill_city_stats.py. main.py reads the whole file once per
process.
"""
from __future__ import annotations

import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_DATA = os.path.join(_HERE, "data")
_NATIONAL = os.path.join(_DATA, "_national")
_PATH = os.path.join(_NATIONAL, "city_stats.json")


def path() -> str:
    return _PATH


def load_all() -> dict[str, dict]:
    """Read the whole stats file. Returns {} if the file doesn't exist
    yet — the caller is expected to fall back to per-city.json reads
    only for missing entries."""
    if not os.path.exists(_PATH):
        return {}
    try:
        with open(_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def write_one(city_id: str, stats: dict) -> None:
    """Merge `stats` for `city_id` into the on-disk file. Atomic via
    write-to-temp + rename so a concurrent reader never sees a half-
    written file."""
    os.makedirs(_NATIONAL, exist_ok=True)
    current = load_all()
    current[city_id] = stats
    tmp = _PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2, sort_keys=True)
    os.replace(tmp, _PATH)


def compute_from_city_json(city_id: str) -> dict | None:
    """Read one data/<city>/city.json once and return its stats dict.
    Returns None when the file is absent or unparseable. This is the
    expensive path (full 20–50 MB parse) — it's only called by the
    one-shot backfill tool and as the last-resort fallback in
    main.py when an entry is missing from the cache."""
    city_json = os.path.join(_DATA, city_id, "city.json")
    if not os.path.exists(city_json):
        return None
    try:
        with open(city_json, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return {
        "population":     raw.get("population"),
        "building_count": raw.get("building_count"),
        # Landmarks = buildings with a custom_builder set (hand-authored
        # 3D assets via overrides.json).
        "landmark_count": sum(
            1 for b in raw.get("buildings", [])
            if b.get("custom_builder")
        ),
    }
