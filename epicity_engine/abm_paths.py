"""
abm_paths.py — street-snapped commute paths for the ABM agents layer.

For the agents-as-points visualisation (static/agents-layer.js) to look
like people *walking on streets* instead of cutting straight lines
through buildings, we need to bend each home→work commute through
points that actually lie on the road network.

We do this cheaply: bucket every road vertex into a coarse spatial
grid (~200 m cells), then for each agent take three samples along the
straight home→work line (25 %, 50 %, 75 %) and snap each sample to its
nearest road point. The resulting 5-vertex polyline
``[home, mid₁, mid₂, mid₃, work]`` bends through streets without
needing a real router or graph search.

This is **visualisation only** — the contact graph in `engine_abm.py`
still groups agents by `current_location` building idx; agents in
between locations don't (yet) contribute to street-level FOI. Real
street-mediated transmission is `abm_phase2_v2_design.md` §4.3 (transit)
and §4.9 (ambient FOI on neighbours), neither of which require a path
geometry to land.

Cached per ``(home_idx, work_idx)`` so even 100 k agents share the
~5 k unique pairs typical for a city the size of Stockholm.
"""

from __future__ import annotations

import functools
from typing import Iterable

import numpy as np


# Grid cell size used by `RoadGrid` for snap queries. 200 m is wide
# enough that a 3×3 neighbourhood (600 m square) is virtually
# guaranteed to contain at least one road point on any developed
# Swedish kommun, and narrow enough that the per-cell candidate list
# stays under ~50 entries even in dense city centres.
_GRID_CELL_M: float = 200.0
_SAMPLES_ALONG_LINE: tuple[float, ...] = (0.25, 0.5, 0.75)


class RoadGrid:
    """
    Bucketed road-vertex index for fast nearest-road queries.

    Construction is one O(N) pass over every road segment. Query cost is
    O(K) where K = number of points in the 3×3 cell neighbourhood, which
    in practice is small (<100) regardless of city size.
    """

    def __init__(self, roads: Iterable[dict], cell_m: float = _GRID_CELL_M) -> None:
        self.cell = float(cell_m)
        self._cells: dict[tuple[int, int], list[tuple[float, float]]] = {}
        # Cache the line-snap result for unique (home_idx, work_idx)
        # pairs — many agents share workplaces so the unique-pair count
        # is bounded by N_residential × N_workplace_zones, which is far
        # less than N_agents.
        self._path_cache: dict[tuple[int, int], list[tuple[float, float]]] = {}
        for r in roads:
            pts = r.get("points") or []
            for pt in pts:
                if not pt or len(pt) < 2:
                    continue
                self._add(float(pt[0]), float(pt[1]))

    def _add(self, x: float, z: float) -> None:
        cx = int(x // self.cell)
        cz = int(z // self.cell)
        bucket = self._cells.get((cx, cz))
        if bucket is None:
            self._cells[(cx, cz)] = [(x, z)]
        else:
            bucket.append((x, z))

    def snap(self, x: float, z: float) -> tuple[float, float] | None:
        """Return the nearest road vertex to (x, z) within ~600 m, else None."""
        cx = int(x // self.cell)
        cz = int(z // self.cell)
        best: tuple[float, float] | None = None
        best_d = float("inf")
        for dx in (-1, 0, 1):
            for dz in (-1, 0, 1):
                for pt in self._cells.get((cx + dx, cz + dz), ()):
                    d = (pt[0] - x) * (pt[0] - x) + (pt[1] - z) * (pt[1] - z)
                    if d < best_d:
                        best_d = d
                        best = pt
        return best

    def commute_path(
        self,
        *,
        home_idx: int,
        work_idx: int,
        home_xz: tuple[float, float],
        work_xz: tuple[float, float],
    ) -> list[tuple[float, float]]:
        """
        Build a 5-vertex street-snapped polyline from `home_xz` to
        `work_xz`, cached by the integer building idxs so repeated calls
        for agents that share the same home + workplace are O(1).
        """
        key = (int(home_idx), int(work_idx))
        cached = self._path_cache.get(key)
        if cached is not None:
            return cached
        path: list[tuple[float, float]] = [home_xz]
        hx, hz = home_xz
        wx, wz = work_xz
        for t in _SAMPLES_ALONG_LINE:
            mx = hx + (wx - hx) * t
            mz = hz + (wz - hz) * t
            snap_pt = self.snap(mx, mz)
            if snap_pt is not None:
                path.append(snap_pt)
        path.append(work_xz)
        self._path_cache[key] = path
        return path


# ── Module-level cache so /api/agent_positions doesn't rebuild the grid
# on every request. Keyed by city id — switching cities discards the
# previous grid lazily on next lookup.
_GRID_CACHE: dict[str, RoadGrid] = {}


def get_or_build_grid(city_id: str, roads: Iterable[dict]) -> RoadGrid:
    g = _GRID_CACHE.get(city_id)
    if g is None:
        g = RoadGrid(roads)
        _GRID_CACHE[city_id] = g
    return g


def reset_grid(city_id: str | None = None) -> None:
    """Drop the cached grid (used by /api/select_city + reset hooks)."""
    if city_id is None:
        _GRID_CACHE.clear()
    else:
        _GRID_CACHE.pop(city_id, None)
