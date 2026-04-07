# C:\Users\pasaaa\city-newbuilds\api\fairness_routes.py
from typing import Any, Dict, List, Optional, Tuple
import math
import logging

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()

# Public OSRM demo (table API). We keep it simple but robust.
OSRM_BASE_URLS = [
    "https://router.project-osrm.org",
]

# Map our UI travel modes → OSRM profiles
PROFILE_MAP = {
    "walk": "foot",
    "bike": "bike",
    "car": "driving",
    # public transport ≈ foot, as an approximation
    "pt": "foot",
}

# Approximate speeds [m/s] to derive durations when we fall back to straight-line
DEFAULT_SPEED_MPS = {
    "walk": 1.4,   # ~5 km/h
    "bike": 4.0,   # ~14.4 km/h
    "car": 13.9,   # ~50 km/h
    "pt": 6.0,     # ~21.6 km/h (very rough)
    "euclid": 1.4,
}


class DistanceRequest(BaseModel):
    """
    Payload shape expected from main.js:

    {
      "travel_mode": "walk" | "bike" | "car" | "pt" | "euclid",
      "buildings": [[lon, lat], ...],
      "pois": [
        [lon, lat],
        {"c":[lon,lat],"name":"..."},
        ...
      ]
    }
    """
    travel_mode: str = "walk"
    buildings: List[List[float]]
    pois: List[Any]

class FairnessSearchEntity(BaseModel):
    """
    One candidate item that can be filtered by POI fairness constraints.
    level can be: building | mezo | macro (or any custom bucket name).
    fairness_by_poi stores a 0..1 fairness score per POI category/key.
    """

    id: str
    level: str = "building"
    name: Optional[str] = None
    fairness_by_poi: Dict[str, float]
    meta: Optional[Dict[str, Any]] = None


class FairnessSearchRequest(BaseModel):
    """
    Filter entities by fairness constraints.

    include_all: every listed POI key must have score >= include_threshold
    include_any: at least one listed POI key must have score >= include_threshold
    exclude_all: every listed POI key must have score <= exclude_threshold
    exclude_any: at least one listed POI key must have score <= exclude_threshold
    """

    entities: List[FairnessSearchEntity]
    include_all: List[str] = Field(default_factory=list)
    include_any: List[str] = Field(default_factory=list)
    exclude_all: List[str] = Field(default_factory=list)
    exclude_any: List[str] = Field(default_factory=list)
    include_threshold: float = 0.65
    exclude_threshold: float = 0.35
    limit: int = 100


def _normalize_lonlat_list(items: List[Any], what: str) -> List[Tuple[float, float]]:
    """
    Accepts things like:
      [lon, lat]
      {"c":[lon,lat], "name": "..."}
      {"lon": lon, "lat": lat}
    and returns a clean list of (lon, lat).
    """
    out: List[Tuple[float, float]] = []
    for idx, val in enumerate(items):
        lon = lat = None

        if isinstance(val, dict):
            if "c" in val and isinstance(val["c"], (list, tuple)) and len(val["c"]) >= 2:
                lon, lat = val["c"][0], val["c"][1]
            elif "lon" in val and "lat" in val:
                lon, lat = val["lon"], val["lat"]
        elif isinstance(val, (list, tuple)) and len(val) >= 2:
            lon, lat = val[0], val[1]

        if lon is None or lat is None:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid {what} coordinate at index {idx}: {val!r}"
            )

        try:
            lon_f = float(lon)
            lat_f = float(lat)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"Non-numeric {what} coordinate at index {idx}: {val!r}"
            )

        out.append((lon_f, lat_f))

    if not out:
        raise HTTPException(status_code=400, detail=f"No {what} points provided")

    return out

def _check_thresholds(include_threshold: float, exclude_threshold: float):
    if include_threshold < 0 or include_threshold > 1:
        raise HTTPException(status_code=400, detail="include_threshold must be in [0,1]")
    if exclude_threshold < 0 or exclude_threshold > 1:
        raise HTTPException(status_code=400, detail="exclude_threshold must be in [0,1]")


def _normalize_keys(keys: List[str]) -> List[str]:
    out = []
    seen = set()
    for key in keys:
        normalized = (key or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _score_or_default(score_map: Dict[str, float], key: str, default: float = 0.0) -> float:
    try:
        val = float(score_map.get(key, default))
    except Exception:
        return default
    return max(0.0, min(1.0, val))


def _haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """
    Straight-line distance (great-circle) in meters.
    a, b are (lon, lat).
    """
    R = 6371000.0
    lon1, lat1 = a
    lon2, lat2 = b
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    s = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2 * R * math.asin(math.sqrt(max(0.0, min(1.0, s))))


def _compute_euclid_all(
    travel_mode: str,
    buildings: List[Tuple[float, float]],
    pois: List[Tuple[float, float]],
    suffix: str = "euclid"
):
    """
    Pure straight-line distances and approximate durations.
    Returns (distances_m, nearest_index, durations_s, mode_string).
    """
    n_b = len(buildings)
    n_p = len(pois)

    distances_m: List[float] = [math.nan] * n_b
    nearest_index: List[int] = [-1] * n_b
    durations_s: List[float] = [math.nan] * n_b

    if n_b == 0 or n_p == 0:
        mode_label = suffix if travel_mode == "euclid" else f"{travel_mode}-{suffix}"
        return distances_m, nearest_index, durations_s, mode_label

    speed = DEFAULT_SPEED_MPS.get(travel_mode, DEFAULT_SPEED_MPS["walk"])

    for i, b in enumerate(buildings):
        best = math.inf
        best_j = -1
        for j, p in enumerate(pois):
            d = _haversine_m(b, p)
            if d < best:
                best = d
                best_j = j

        if math.isfinite(best) and best_j >= 0:
            distances_m[i] = best
            nearest_index[i] = best_j
            durations_s[i] = best / speed  # seconds

    mode_label = suffix if travel_mode == "euclid" else f"{travel_mode}-{suffix}"
    return distances_m, nearest_index, durations_s, mode_label


def _try_osrm_table(
    travel_mode: str,
    buildings: List[Tuple[float, float]],
    pois: List[Tuple[float, float]],
):
    """
    Try to use OSRM /table to get network distances (meters) and durations (seconds).
    If anything fails, we return (None, None, None, "<mode>-euclid-fallback", False)
    and the caller will fall back to Euclidean.
    """
    mode_key = (travel_mode or "walk").lower()
    profile = PROFILE_MAP.get(mode_key, "foot")

    n_b = len(buildings)
    n_p = len(pois)
    if n_b == 0 or n_p == 0:
        return None, None, None, f"{mode_key}-euclid-fallback", False

    # OSRM demo server is roughly limited to ~100 coordinates per request.
    # We pack: [buildings_chunk..., pois...]
    max_locs = 100
    max_chunk_b = max(1, max_locs - n_p)

    distances_m: List[float] = [math.nan] * n_b
    nearest_index: List[int] = [-1] * n_b
    durations_s: List[float] = [math.nan] * n_b

    for start in range(0, n_b, max_chunk_b):
        end = min(n_b, start + max_chunk_b)
        chunk = buildings[start:end]
        nb_chunk = len(chunk)

        coords = chunk + pois
        coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)

        # In the combined list, buildings = [0..nb_chunk-1], pois = [nb_chunk..nb_chunk+n_p-1]
        sources = ";".join(str(i) for i in range(nb_chunk))
        destinations = ";".join(str(nb_chunk + j) for j in range(n_p))

        ok_any_base = False
        last_err = None

        for base_url in OSRM_BASE_URLS:
            url = f"{base_url}/table/v1/{profile}/{coord_str}"
            params = {
                "sources": sources,
                "destinations": destinations,
                "annotations": "distance,duration",
            }
            try:
                r = requests.get(url, params=params, timeout=(5, 25))
            except Exception as e:
                last_err = e
                logger.warning("OSRM table request error (%s): %s", base_url, e)
                continue

            if not r.ok:
                last_err = (r.status_code, r.text[:200])
                logger.warning("OSRM table bad status (%s): %s %s", base_url, r.status_code, r.text[:200])
                continue

            data = r.json()
            dist_mat = data.get("distances") or []
            dur_mat = data.get("durations") or []

            if len(dist_mat) != nb_chunk:
                last_err = f"OSRM table returned unexpected matrix size: {len(dist_mat)} vs {nb_chunk}"
                logger.warning(str(last_err))
                break

            for local_i, global_i in enumerate(range(start, end)):
                row_d = dist_mat[local_i] or []
                if not row_d:
                    continue
                best = math.inf
                best_j = -1
                for k, d in enumerate(row_d):
                    if d is None:
                        continue
                    if d < best:
                        best = d
                        best_j = k
                if best_j >= 0 and math.isfinite(best):
                    distances_m[global_i] = float(best)
                    nearest_index[global_i] = best_j

                    if dur_mat and local_i < len(dur_mat):
                        row_t = dur_mat[local_i] or []
                        if best_j < len(row_t) and row_t[best_j] is not None:
                            durations_s[global_i] = float(row_t[best_j])

            ok_any_base = True
            break  # we succeeded with this base_url

        if not ok_any_base:
            logger.warning("OSRM table failed for chunk %s-%s, last_err=%r", start, end, last_err)
            return None, None, None, f"{mode_key}-euclid-fallback", False

    # Success
    return distances_m, nearest_index, durations_s, mode_key, True


@router.post("/fairness/distances")
def fairness_distances(req: DistanceRequest):
    """
    Main endpoint used by main.js:
      POST /api/fairness/distances

    Returns:
    {
      "distances_m": [...],
      "nearest_index": [...],
      "durations_s": [...],
      "mode": "walk" | "bike" | "car" | "walk-euclid-fallback" | "euclid" | ...
    }
    """
    travel_mode = (req.travel_mode or "walk").lower()

    buildings = _normalize_lonlat_list(req.buildings, "building")
    pois = _normalize_lonlat_list(req.pois, "poi")

    # If user explicitly chose straight-line, skip OSRM completely.
    if travel_mode == "euclid":
        d, idx, dur, mode_label = _compute_euclid_all(travel_mode, buildings, pois, suffix="euclid")
        return {
            "distances_m": d,
            "nearest_index": idx,
            "durations_s": dur,
            "mode": mode_label,
        }

    # Try OSRM network distances (table API)
    try:
        d_osrm, idx_osrm, dur_osrm, used_mode, ok = _try_osrm_table(travel_mode, buildings, pois)
    except Exception as e:
        logger.warning("OSRM exception, falling back to euclid: %s", e)
        ok = False
        d_osrm = idx_osrm = dur_osrm = None
        used_mode = f"{travel_mode}-euclid-fallback"

    if ok and d_osrm is not None:
        # SUCCESS: real network distances
        return {
            "distances_m": d_osrm,
            "nearest_index": idx_osrm,
            "durations_s": dur_osrm,
            "mode": used_mode,   # e.g. "walk" or "bike"
        }

    # Fallback: straight-line, but we tag mode with "-euclid-fallback"
    d, idx, dur, mode_label = _compute_euclid_all(travel_mode, buildings, pois, suffix="euclid-fallback")
    return {
        "distances_m": d,
        "nearest_index": idx,
        "durations_s": dur,
        "mode": mode_label,  # e.g. "walk-euclid-fallback"
    }

@router.post("/fairness/search")
def fairness_search(req: FairnessSearchRequest):
    """
    Find entities that are fair to selected POIs and unfair to other POIs.

    Example intent:
      include_all = ["grocery", "school_primary"]
      exclude_any = ["hospital", "university"]
    """

    include_all = _normalize_keys(req.include_all)
    include_any = _normalize_keys(req.include_any)
    exclude_all = _normalize_keys(req.exclude_all)
    exclude_any = _normalize_keys(req.exclude_any)
    _check_thresholds(req.include_threshold, req.exclude_threshold)

    if not req.entities:
        return {"matches": [], "total": 0, "by_level": {}, "filters": req.model_dump()}

    matches = []
    for entity in req.entities:
        score_map = {(k or "").strip().lower(): v for k, v in (entity.fairness_by_poi or {}).items()}

        pass_include_all = all(
            _score_or_default(score_map, key) >= req.include_threshold
            for key in include_all
        )
        pass_include_any = (not include_any) or any(
            _score_or_default(score_map, key) >= req.include_threshold
            for key in include_any
        )
        pass_exclude_all = all(
            _score_or_default(score_map, key) <= req.exclude_threshold
            for key in exclude_all
        )
        pass_exclude_any = (not exclude_any) or any(
            _score_or_default(score_map, key) <= req.exclude_threshold
            for key in exclude_any
        )

        if not (pass_include_all and pass_include_any and pass_exclude_all and pass_exclude_any):
            continue

        included_scores = [_score_or_default(score_map, k) for k in (include_all + include_any)]
        excluded_scores = [_score_or_default(score_map, k) for k in (exclude_all + exclude_any)]
        include_avg = (sum(included_scores) / len(included_scores)) if included_scores else None
        exclude_avg = (sum(excluded_scores) / len(excluded_scores)) if excluded_scores else None
        contrast = None
        if include_avg is not None and exclude_avg is not None:
            contrast = include_avg - exclude_avg

        matches.append({
            "id": entity.id,
            "level": entity.level,
            "name": entity.name,
            "fairness_by_poi": score_map,
            "include_avg": include_avg,
            "exclude_avg": exclude_avg,
            "contrast": contrast,
            "meta": entity.meta,
        })

    matches.sort(
        key=lambda row: (
            row["contrast"] is None,
            -(row["contrast"] or -1.0),
            -(row["include_avg"] or 0.0),
            (row["exclude_avg"] or 0.0),
        )
    )

    limit = max(1, min(int(req.limit or 100), 5000))
    limited = matches[:limit]

    by_level: Dict[str, int] = {}
    for row in limited:
        by_level[row["level"]] = by_level.get(row["level"], 0) + 1

    return {
        "total": len(matches),
        "returned": len(limited),
        "by_level": by_level,
        "matches": limited,
        "filters": {
            "include_all": include_all,
            "include_any": include_any,
            "exclude_all": exclude_all,
            "exclude_any": exclude_any,
            "include_threshold": req.include_threshold,
            "exclude_threshold": req.exclude_threshold,
            "limit": limit,
        },
    }
