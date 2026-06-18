"""
transport.py — Synthetic transport schedules + runtime trip dispatcher.

Three responsibilities, all in one module so they share constants and the
on-disk schedule shape:

1. **Build-time** synthetic line + schedule generation. Reads
   ``data/<city>/transport_infra.json`` (produced by ``osm.load_transport``)
   and ``data/<city>/city.json`` (produced by ``osm.load_city``), generates
   bus/tram/train lines + a one-day timetable, and writes the result to
   ``data/<city>/transport_schedule.json``. Called from
   ``tools/download_city.py``. Never invoked at runtime.

2. **Runtime** ``TripDispatcher`` class. The engine attaches one of these on
   city load, and calls ``process(prev_min, cur_min, engine, metapop)``
   from inside ``_step_one`` after the SEIR loop. The dispatcher mass-
   transfers passengers between origin and destination buildings (intra-
   city trips) or between metapop coarse nodes (inter-city trips), with
   a small in-vehicle transmission delta along the way.

3. ``TRANSPORT_BETA_PER_HOUR`` — the in-vehicle force-of-infection rate in
   *per-hour* contact units (NOT a multiplier on the engine's daily β,
   which would silently produce wrong magnitudes).

The schedule shape is also what a future ``tools/download_transport.py``
will write when fed real Trafiklab GTFS data — the runtime never needs to
change to swap synthetic for real schedules.
"""
from __future__ import annotations

import heapq
import json
import math
import os
import random
from typing import Any

import osm


# ── Tunable constants ─────────────────────────────────────────────────────────

# In-vehicle transmission rate per hour of close contact, differentiated by
# vehicle type.  Values reflect ventilation quality, typical crowding and
# seating density.  Per-hour units are critical: the engine's β is a daily
# rate that already integrates a full day of contacts; multiplying it by
# trip hours would either be noise or apocalyptic depending on the day.
# Keep these independent of building β.
TRANSPORT_BETA_PER_HOUR: dict[str, float] = {
    "bus":             0.45,   # crowded, poor ventilation, standing passengers
    "tram":            0.35,   # somewhat less dense than bus
    "train":           0.20,   # seated, better airflow, more space
    "intercity_train": 0.18,   # reserved seating, lower density
    "flight":          0.10,   # HEPA filtration, assigned seats
    "ferry":           0.15,   # open-deck areas, high natural ventilation
}
_DEFAULT_TRANSPORT_BETA = 0.30  # fallback for unknown vehicle types

# Radius around a stop within which "passenger boarding/alighting" affects
# building SEIR. 300 m is a generous walking distance to a stop.
STOP_NEIGHBORHOOD_RADIUS_M = 300.0

# Max manifest size as a fraction of origin-cluster population. Caps the
# damage one synthetic bus can do to a small village.
MANIFEST_CAP_FRAC = 0.05

# Per-mode timetable parameters.
#
#   headway_min — minutes between successive departures (peak hours)
#   start_hour  — first departure of the day (24-hour clock)
#   end_hour    — last departure must be ≤ this (clamped to ensure
#                 arrive_minute < 1440 — no overnight wrap)
#   capacity    — passengers per vehicle
#   speed_kmh   — used to derive trip duration from polyline length
#
# Synthetic bus speed is intentionally slow (urban) so the visual progression
# along the line is readable. Plane "speed" is mostly ignored — flights use
# a fixed 60–120 minute duration regardless.
_MODE_PARAMS: dict[str, dict[str, float]] = {
    "bus":             {"headway_min": 15, "start_hour": 6.0,  "end_hour": 22.5,
                        "capacity": 40,  "speed_kmh": 25},
    "tram":            {"headway_min": 10, "start_hour": 5.5,  "end_hour": 23.0,
                        "capacity": 80,  "speed_kmh": 30},
    "train":           {"headway_min": 30, "start_hour": 5.0,  "end_hour": 22.5,
                        "capacity": 200, "speed_kmh": 60},
    "intercity_train": {"headway_min": 90, "start_hour": 6.0,  "end_hour": 21.0,
                        "capacity": 300, "speed_kmh": 120},
    "flight":          {"headway_min": 90, "start_hour": 6.0,  "end_hour": 21.0,
                        "capacity": 180, "speed_kmh": 700},
    "ferry":           {"headway_min": 20, "start_hour": 6.0,  "end_hour": 22.0,
                        "capacity": 150, "speed_kmh": 18},
}

# Per-city flight destination tables. Each value is a list of destination
# city codes — Swedish city ids in osm.CITIES, OR foreign airport codes
# in _FOREIGN_DESTINATIONS below. Foreign destinations are visual-only
# (no metapop epi coupling) since they're outside the Swedish kommun model.
_FLIGHT_DESTINATIONS: dict[str, list[str]] = {
    "stockholm":   ["goteborg", "malmo", "umea", "copenhagen", "oslo", "london"],
    "goteborg":    ["stockholm", "malmo", "copenhagen", "oslo"],
    "malmo":       ["stockholm", "goteborg", "copenhagen"],
    "umea":        ["stockholm", "helsinki"],
    "lulea":       ["stockholm"],
    "vasteras":    ["stockholm", "copenhagen"],
}

# Synthetic foreign destinations: lat/lon used only to compute the visual
# direction the plane flies away from the focused city. The "kommun" field
# is None — these never participate in metapop coupling.
_FOREIGN_DESTINATIONS: dict[str, dict[str, Any]] = {
    "copenhagen": {"name": "Copenhagen", "lat": 55.6761, "lon": 12.5683, "kommun": None},
    "oslo":       {"name": "Oslo",       "lat": 59.9139, "lon": 10.7522, "kommun": None},
    "london":     {"name": "London",     "lat": 51.5074, "lon": -0.1278, "kommun": None},
    "helsinki":   {"name": "Helsinki",   "lat": 60.1699, "lon": 24.9384, "kommun": None},
}

_DEFAULT_FLIGHT_DESTINATIONS = ["stockholm", "copenhagen"]


# ── Road graph (for synthetic bus routing) ────────────────────────────────────

def _build_road_graph(roads: list[dict]
                      ) -> tuple[dict[tuple[int, int], int],
                                 list[list[tuple[int, float]]],
                                 list[tuple[float, float]]]:
    """
    Build a node graph from road polylines for Dijkstra-based bus routing.

    Points are bucketed to a 4-metre grid so endpoints that should be the
    same intersection — but are stored a metre or two apart in OSM —
    merge into a single graph node. With 1-metre rounding many intersections
    silently failed to connect and Dijkstra fell back to straight lines
    cutting through buildings.

    Returns (node_index, adjacency, coords) where:
        node_index[(bucket_x, bucket_z)] -> graph node id
        adjacency[u] -> [(v, distance_m), ...]
        coords[u] -> (world_x, world_z)
    """
    BUCKET = 4.0  # metres — coarse enough to merge near-coincident endpoints

    node_index: dict[tuple[int, int], int] = {}
    coords: list[tuple[float, float]] = []
    adj: list[list[tuple[int, float]]] = []

    def get_node(x: float, z: float) -> int:
        key = (int(round(x / BUCKET)), int(round(z / BUCKET)))
        existing = node_index.get(key)
        if existing is not None:
            return existing
        idx = len(coords)
        node_index[key] = idx
        coords.append((float(x), float(z)))
        adj.append([])
        return idx

    for road in roads:
        pts = road.get("points") or []
        if len(pts) < 2:
            continue
        prev = get_node(pts[0][0], pts[0][1])
        for p in pts[1:]:
            cur = get_node(p[0], p[1])
            if cur == prev:
                continue
            dx = coords[cur][0] - coords[prev][0]
            dz = coords[cur][1] - coords[prev][1]
            d = math.sqrt(dx * dx + dz * dz)
            adj[prev].append((cur, d))
            adj[cur].append((prev, d))
            prev = cur

    return node_index, adj, coords


def _grid_index_nodes(coords: list[tuple[float, float]], cell_size: float = 200.0
                      ) -> tuple[dict[tuple[int, int], list[int]], float]:
    grid: dict[tuple[int, int], list[int]] = {}
    for i, (x, z) in enumerate(coords):
        key = (int(x // cell_size), int(z // cell_size))
        grid.setdefault(key, []).append(i)
    return grid, cell_size


def _nearest_road_node(x: float, z: float,
                       coords: list[tuple[float, float]],
                       grid: dict[tuple[int, int], list[int]],
                       cell_size: float,
                       max_dist: float = 400.0) -> int:
    """Snap a point to the nearest road graph node within max_dist, or -1."""
    cx, cz = int(x // cell_size), int(z // cell_size)
    best, best_d2 = -1, max_dist * max_dist
    search = max(2, int(math.ceil(max_dist / cell_size)))
    for dcx in range(-search, search + 1):
        for dcz in range(-search, search + 1):
            for i in grid.get((cx + dcx, cz + dcz), []):
                px, pz = coords[i]
                d2 = (px - x) ** 2 + (pz - z) ** 2
                if d2 < best_d2:
                    best_d2 = d2
                    best = i
    return best


def _dijkstra(adj: list[list[tuple[int, float]]],
              src: int, dst: int) -> list[int] | None:
    """Shortest path on the road graph. Returns list of node ids or None."""
    if src == dst:
        return [src]
    n = len(adj)
    dist = [math.inf] * n
    prev = [-1] * n
    dist[src] = 0.0
    pq: list[tuple[float, int]] = [(0.0, src)]
    while pq:
        d, u = heapq.heappop(pq)
        if d > dist[u]:
            continue
        if u == dst:
            break
        for v, w in adj[u]:
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                prev[v] = u
                heapq.heappush(pq, (nd, v))
    if dist[dst] == math.inf:
        return None
    path: list[int] = []
    u = dst
    while u != -1:
        path.append(u)
        u = prev[u]
    path.reverse()
    return path


# ── Line synthesis ────────────────────────────────────────────────────────────

def _bus_lines_from_osm(
        osm_routes: list[dict],
        bus_stops: list[dict],
        road_graph: tuple,
        city_id: str,
        max_lines: int = 60,
        max_proj_dist_m: float = 500.0) -> tuple[list[dict], set[int]]:
    """
    Build bus lines from real OSM bus route relations.

    Each OSM route has stop positions and/or a road polyline. We:
      1. Use the route's polyline directly if available (real road geometry).
      2. Match our bus stops to the route's stop positions by proximity.
      3. Project matched stops onto the polyline for stop_polyline_indices.
      4. Deduplicate routes with near-identical stop sequences.

    Returns (lines, covered_stop_idxs).
    """
    if not osm_routes or not bus_stops:
        return [], set()

    _node_index, adj, coords = road_graph

    # Build spatial index of our bus stops for fast nearest-stop lookup
    stop_by_pos: list[tuple[float, float, dict]] = [
        (s["world_x"], s["world_z"], s) for s in bus_stops
    ]

    def _find_nearest_stop(wx, wz, max_dist=max_proj_dist_m):
        best, best_d2 = None, max_dist * max_dist
        for sx, sz, s in stop_by_pos:
            d2 = (sx - wx) ** 2 + (sz - wz) ** 2
            if d2 < best_d2:
                best_d2 = d2
                best = s
        return best

    lines: list[dict] = []
    covered: set[int] = set()
    seen_refs: dict[str, int] = {}    # dedup: ref → count
    line_idx = 0

    # Sort routes by ref number (numeric first) for deterministic ordering
    def _sort_key(r):
        ref = r.get("ref", "")
        try:
            return (0, int(ref))
        except ValueError:
            return (1, ref)

    sorted_routes = sorted(osm_routes, key=_sort_key)

    for route in sorted_routes:
        if line_idx >= max_lines:
            break

        ref = route.get("ref", "")
        name = route.get("name", "")
        osm_stops = route.get("stops", [])
        osm_poly = route.get("polyline", [])

        # Skip routes with too little data
        if len(osm_stops) < 2 and len(osm_poly) < 4:
            continue

        # Skip intercity/regional routes (polyline > 80km is not urban)
        if osm_poly and len(osm_poly) >= 2:
            poly_len = sum(
                math.hypot(osm_poly[i][0] - osm_poly[i-1][0],
                           osm_poly[i][1] - osm_poly[i-1][1])
                for i in range(1, len(osm_poly)))
            if poly_len > 80_000:
                continue

        # Deduplicate: keep max 3 variants per ref (forward, backward, branch)
        if ref:
            seen_refs[ref] = seen_refs.get(ref, 0) + 1
            if seen_refs[ref] > 3:
                continue

        # Match our bus stops to OSM route stops by proximity
        matched_stops: list[dict] = []
        for osm_stop_wcoord in osm_stops:
            wx, wz = osm_stop_wcoord
            nearest = _find_nearest_stop(wx, wz)
            if nearest and nearest["idx"] not in {s["idx"] for s in matched_stops}:
                matched_stops.append(nearest)

        if len(matched_stops) < 2:
            continue

        # Use the OSM polyline if available, otherwise route via Dijkstra
        if osm_poly and len(osm_poly) >= 4:
            polyline = [[round(p[0], 1), round(p[1], 1)] for p in osm_poly]
        else:
            # Fallback: route matched stops via road Dijkstra
            # Never use straight lines — skip unreachable stops
            grid, cell_sz = _grid_index_nodes(coords)
            polyline = []
            last_node = -1
            for i, s in enumerate(matched_stops):
                cur_node = _nearest_road_node(s["world_x"], s["world_z"],
                                              coords, grid, cell_sz)
                if i == 0:
                    polyline.append([s["world_x"], s["world_z"]])
                    last_node = cur_node
                    continue
                if last_node >= 0 and cur_node >= 0:
                    path = _dijkstra(adj, last_node, cur_node)
                    if path and len(path) >= 2:
                        for n in path[1:]:
                            px, pz = coords[n]
                            polyline.append([round(px, 1), round(pz, 1)])
                        last_node = cur_node
                        continue
                # No road path — skip this segment (don't draw through forest)

        if len(polyline) < 2:
            continue

        # Repair jumps: replace gaps >300m with road-graph-routed paths
        polyline = _repair_polyline_jumps(polyline, road_graph, max_gap_m=300.0)

        if len(polyline) < 2:
            continue

        # Project matched stops onto polyline for stop_polyline_indices
        cum = [0.0]
        for i in range(1, len(polyline)):
            dx = polyline[i][0] - polyline[i - 1][0]
            dz = polyline[i][1] - polyline[i - 1][1]
            cum.append(cum[-1] + math.hypot(dx, dz))

        stop_polyline_indices = []
        for s in matched_stops:
            best_idx = 0
            best_dist = float("inf")
            for pi in range(len(polyline)):
                d = math.hypot(polyline[pi][0] - s["world_x"],
                               polyline[pi][1] - s["world_z"])
                if d < best_dist:
                    best_dist = d
                    best_idx = pi
            stop_polyline_indices.append(best_idx)

        lines.append({
            "id":                    f"bus_{city_id}_{ref or line_idx}",
            "type":                  "bus",
            "stops":                 [s["idx"] for s in matched_stops],
            "polyline":              polyline,
            "stop_polyline_indices": stop_polyline_indices,
            "is_intercity":          False,
        })
        for s in matched_stops:
            covered.add(s["idx"])
        line_idx += 1

    print(f"[transport] {city_id}: {len(lines)} real OSM bus lines, "
          f"{len(covered)}/{len(bus_stops)} stops covered")
    return lines, covered


def _repair_polyline_jumps(polyline: list[list[float]],
                           road_graph: tuple,
                           max_gap_m: float = 300.0) -> list[list[float]]:
    """
    Post-process a bus polyline: replace any segment >max_gap_m with a
    Dijkstra-routed path on the road graph. This fixes OSM route
    relations whose way members have gaps, preventing straight-line cuts
    through forests/buildings.

    When a gap can't be routed (Dijkstra fails), subsequent points are
    scanned until one reconnects to the last good position. This
    prevents orphan jumps from lingering in the output.
    """
    if len(polyline) < 2:
        return polyline

    _node_index, adj, coords = road_graph
    if not coords:
        return polyline

    grid, cell_sz = _grid_index_nodes(coords)
    result = [polyline[0]]

    i = 1
    while i < len(polyline):
        last = result[-1]
        cur = polyline[i]
        gap = math.hypot(cur[0] - last[0], cur[1] - last[1])

        if gap <= max_gap_m:
            # Short segment — keep as-is (real road geometry from OSM)
            result.append(cur)
            i += 1
            continue

        # Long jump — try to route via road graph
        n1 = _nearest_road_node(last[0], last[1], coords, grid, cell_sz)
        n2 = _nearest_road_node(cur[0], cur[1], coords, grid, cell_sz)
        routed = False
        if n1 >= 0 and n2 >= 0:
            path = _dijkstra(adj, n1, n2)
            if path and len(path) >= 2:
                # Check the routed path length is reasonable (not >5× gap)
                path_len = sum(
                    math.hypot(coords[path[j]][0] - coords[path[j-1]][0],
                               coords[path[j]][1] - coords[path[j-1]][1])
                    for j in range(1, len(path)))
                if path_len < gap * 5:
                    for n in path[1:]:
                        px, pz = coords[n]
                        result.append([round(px, 1), round(pz, 1)])
                    routed = True

        if not routed:
            # Can't route this gap — skip forward until we find a point
            # that's close to our last good position (reconnects the line)
            # or that can be routed to via Dijkstra.
            i += 1
            continue

        i += 1

    return result


def _cover_remaining_stops(
        all_stops: list[dict],
        already_covered: set[int],
        road_graph: tuple,
        rng: random.Random,
        city_id: str,
        max_stops_per_line: int = 18,
        max_step_m: float = 8000.0) -> list[dict]:
    """
    Coverage sweep: generate synthetic bus lines until every bus stop
    that can snap to the road graph is on at least one line.

    Iteratively picks a seed from the uncovered stops, walks a greedy
    nearest-neighbor chain via Dijkstra, and adds the resulting line.
    Stops that cannot snap to any road node (>400m from roads) are
    excluded and reported as unreachable.
    """
    _node_index, adj, coords = road_graph
    if not coords:
        return []

    grid, cell_sz = _grid_index_nodes(coords)

    # Snap all stops to road nodes; separate snappable from unreachable
    snapped: dict[int, tuple[dict, int]] = {}  # stop_idx → (stop, road_node)
    for s in all_stops:
        if s["idx"] in already_covered:
            continue
        node = _nearest_road_node(s["world_x"], s["world_z"],
                                  coords, grid, cell_sz)
        if node >= 0:
            snapped[s["idx"]] = (s, node)

    unreachable = len(all_stops) - len(already_covered) - len(snapped)
    if unreachable > 0:
        print(f"[transport] {city_id}: {unreachable} bus stops too far "
              f"from any road — excluded from coverage sweep")

    covered = set(already_covered)
    new_lines: list[dict] = []
    line_idx = 0

    while True:
        remaining = {idx: v for idx, v in snapped.items() if idx not in covered}
        if len(remaining) < 2:
            break

        # Pick seed: the remaining stop with the most neighbours within
        # max_step_m. This ensures we start from a dense cluster, not
        # from an isolated outlier that can't chain to anything.
        def _neighbour_count(idx):
            s = remaining[idx][0]
            return sum(1 for oidx in remaining if oidx != idx and
                       math.hypot(remaining[oidx][0]["world_x"] - s["world_x"],
                                  remaining[oidx][0]["world_z"] - s["world_z"])
                       < max_step_m)

        seed_idx = max(remaining.keys(), key=_neighbour_count)

        # Greedy walk from seed, picking nearest remaining stop each step
        order = [seed_idx]
        visited = {seed_idx}
        for _ in range(max_stops_per_line - 1):
            cur_s = remaining[order[-1]][0] if order[-1] in remaining else snapped[order[-1]][0]
            best_idx, best_d2 = -1, max_step_m * max_step_m
            for idx in remaining:
                if idx in visited:
                    continue
                other_s = remaining[idx][0]
                d2 = ((other_s["world_x"] - cur_s["world_x"]) ** 2
                      + (other_s["world_z"] - cur_s["world_z"]) ** 2)
                if d2 < best_d2:
                    best_d2 = d2
                    best_idx = idx
            if best_idx < 0:
                break
            order.append(best_idx)
            visited.add(best_idx)

        if len(order) < 2:
            # Isolated stop — mark it covered to avoid infinite loop
            for idx in order:
                covered.add(idx)
            continue

        # Route via Dijkstra on road graph (no straight lines)
        polyline: list[list[float]] = []
        stop_polyline_indices: list[int] = []
        last_node = -1
        valid_stops: list[int] = []

        for idx in order:
            s, node = snapped.get(idx, (None, -1))
            if s is None or node < 0:
                continue
            if not polyline:
                polyline.append([s["world_x"], s["world_z"]])
                stop_polyline_indices.append(0)
                last_node = node
                valid_stops.append(idx)
                continue
            if last_node < 0:
                last_node = node
                continue
            path = _dijkstra(adj, last_node, node)
            if path and len(path) >= 2:
                for n in path[1:]:
                    px, pz = coords[n]
                    polyline.append([round(px, 1), round(pz, 1)])
                stop_polyline_indices.append(len(polyline) - 1)
                last_node = node
                valid_stops.append(idx)
            # If Dijkstra fails, skip this stop (never straight line)

        if len(valid_stops) < 2 or len(polyline) < 2:
            # Mark these stops as covered to avoid infinite loop
            for idx in order:
                covered.add(idx)
            continue

        new_lines.append({
            "id":                    f"bus_sweep_{city_id}_{line_idx}",
            "type":                  "bus",
            "stops":                 valid_stops,
            "polyline":              polyline,
            "stop_polyline_indices": stop_polyline_indices,
            "is_intercity":          False,
        })
        for idx in valid_stops:
            covered.add(idx)
        line_idx += 1

    total_now = len(covered)
    total_stops = len(all_stops)
    print(f"[transport] {city_id}: coverage sweep added {len(new_lines)} lines, "
          f"now {total_now}/{total_stops} stops covered "
          f"({100*total_now/total_stops:.0f}%)")

    return new_lines


def _synthesize_bus_lines(bus_stops: list[dict],
                          road_graph: tuple,
                          rng: random.Random,
                          max_lines: int = 12,
                          max_stops_per_line: int = 22,
                          max_step_m: float = 3000.0) -> list[dict]:
    """
    Build radial bus lines that cross the city from edge to edge through
    the centre, routed via Dijkstra on the road graph.

    Strategy:
      1. Snap all bus stops to the road graph.
      2. Find the centroid of all stops (= city centre).
      3. Sort stops by angle around the centroid.
      4. For each line, pick a seed on the outer edge and walk *toward*
         the centroid, then continue past it to the opposite edge.
      5. Route each consecutive stop pair via Dijkstra for realistic
         road-following polylines.

    The result is bus lines that radiate through the city like real
    transit networks, covering far more of the municipality than the
    previous short greedy-walk clusters.
    """
    _node_index, adj, coords = road_graph
    if not coords or not bus_stops:
        return []

    grid, cell_sz = _grid_index_nodes(coords)

    # Snap each stop to the nearest road node. Drop stops with no road
    # within 400 m — they're not really part of the bus network.
    snapped: list[tuple[dict, int]] = []
    for s in bus_stops:
        node = _nearest_road_node(s["world_x"], s["world_z"],
                                  coords, grid, cell_sz)
        if node >= 0:
            snapped.append((s, node))
    if len(snapped) < 4:
        return []

    # Scale number of lines with city size
    n_lines = max(6, min(max_lines, len(snapped) // 50))

    cx = sum(s["world_x"] for s, _ in snapped) / len(snapped)
    cz = sum(s["world_z"] for s, _ in snapped) / len(snapped)

    # ── Sort stops by angle around the centroid ──────────────────────
    import math as _math
    for i, (s, _) in enumerate(snapped):
        s["_angle"] = _math.atan2(s["world_z"] - cz, s["world_x"] - cx)
        s["_dist"]  = _math.hypot(s["world_x"] - cx, s["world_z"] - cz)
        s["_snap_idx"] = i

    # Sort by angle
    by_angle = sorted(range(len(snapped)),
                      key=lambda i: snapped[i][0]["_angle"])

    # ── Build radial lines ───────────────────────────────────────────
    used: set[int] = set()
    lines: list[dict] = []
    line_idx = 0

    # Divide the angle circle into n_lines sectors.  For each sector,
    # pick a seed stop near the outer edge, walk inward through the
    # centroid, then continue to the opposite sector's edge.
    sector_size = len(by_angle) / n_lines

    for li in range(n_lines):
        if len(lines) >= n_lines:
            break

        # Sector range for this line
        lo = int(li * sector_size)
        hi = int((li + 0.5) * sector_size)
        # Pick the farthest unused stop in this sector as the seed
        sector_stops = by_angle[lo:hi]
        sector_stops = [i for i in sector_stops if i not in used]
        if not sector_stops:
            continue
        seed = max(sector_stops, key=lambda i: snapped[i][0]["_dist"])

        # Build one half: from seed (edge) toward center
        half1 = _greedy_walk_toward(snapped, seed, cx, cz, used,
                                    max_stops_per_line // 2, max_step_m,
                                    toward_center=True)
        # Build second half: from center outward to opposite edge
        if half1:
            last = half1[-1]
            half2 = _greedy_walk_toward(snapped, last, cx, cz, used,
                                        max_stops_per_line // 2, max_step_m,
                                        toward_center=False)
        else:
            half2 = []

        order = half1 + half2[1:]  # avoid duplicating the pivot stop
        if len(order) < 4:
            continue

        # Route via Dijkstra on road graph
        polyline, stop_polyline_indices = _route_bus_line(
            snapped, order, adj, coords)

        lines.append({
            "id":                    f"bus_{line_idx}",
            "type":                  "bus",
            "stops":                 [snapped[i][0]["idx"] for i in order],
            "polyline":              polyline,
            "stop_polyline_indices": stop_polyline_indices,
            "is_intercity":          False,
        })
        line_idx += 1

    return lines


def _greedy_walk_toward(snapped, seed, cx, cz, used, max_steps, max_step_m,
                        toward_center=True):
    """Walk from seed, preferring stops closer to (toward) or farther from
    (away) the centroid. Returns list of snapped indices."""
    import math as _math
    order = [seed]
    used.add(seed)
    for _ in range(max_steps - 1):
        cur = snapped[order[-1]][0]
        best, best_score = -1, float("inf")
        for j in range(len(snapped)):
            if j in used:
                continue
            other = snapped[j][0]
            dx = other["world_x"] - cur["world_x"]
            dz = other["world_z"] - cur["world_z"]
            dist = _math.hypot(dx, dz)
            if dist > max_step_m or dist < 50:
                continue
            # Score: prefer stops that move toward/away from centroid
            cur_r = _math.hypot(cur["world_x"] - cx, cur["world_z"] - cz)
            other_r = _math.hypot(other["world_x"] - cx, other["world_z"] - cz)
            if toward_center:
                # Prefer stops closer to center (decreasing radius)
                direction_bonus = (cur_r - other_r) / max(dist, 1)
            else:
                # Prefer stops farther from center (increasing radius)
                direction_bonus = (other_r - cur_r) / max(dist, 1)
            # Combine distance penalty with directional preference
            score = dist - direction_bonus * 800
            if score < best_score:
                best_score = score
                best = j
        if best < 0:
            break
        order.append(best)
        used.add(best)
    return order


def _route_bus_line(snapped, order, adj, coords):
    """Route a sequence of bus stops via Dijkstra on the road graph.
    Never falls back to straight lines — skips unreachable stops instead
    so routes always follow real roads."""
    polyline: list[list[float]] = []
    stop_polyline_indices: list[int] = []
    last_node = -1

    for i, line_stop_idx in enumerate(order):
        stop_obj, node_idx = snapped[line_stop_idx]
        if i == 0:
            polyline.append([stop_obj["world_x"], stop_obj["world_z"]])
            stop_polyline_indices.append(0)
            last_node = node_idx
            continue
        if last_node < 0:
            # Previous stop had no road connection — skip
            stop_polyline_indices.append(len(polyline) - 1)
            last_node = node_idx
            continue
        path = _dijkstra(adj, last_node, node_idx)
        if path is None or len(path) < 2:
            # No road path found — skip this stop (don't draw straight line)
            stop_polyline_indices.append(len(polyline) - 1)
            continue
        for n in path[1:]:
            px, pz = coords[n]
            polyline.append([round(px, 1), round(pz, 1)])
        stop_polyline_indices.append(len(polyline) - 1)
        last_node = node_idx

    return polyline, stop_polyline_indices


def _merge_polylines(polylines: list[list[list[float]]],
                     tolerance_m: float = 8.0) -> list[list[list[float]]]:
    """
    Greedily chain polylines that share endpoints (within tolerance metres)
    into longer continuous chains, rejecting connections that would create
    sharp back-loops (U-turns).

    Each input polyline is consumed at most once. The output preserves
    direction within each chain but may flip individual input polylines
    to make endpoints connect.
    """
    if not polylines:
        return []
    tol2 = tolerance_m * tolerance_m

    def _close(a: list[float], b: list[float]) -> bool:
        return (a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 < tol2

    def _dir_at_end(poly: list[list[float]]) -> tuple[float, float]:
        """Unit direction vector at the END of a polyline (last 2 pts)."""
        if len(poly) < 2:
            return (1.0, 0.0)
        dx = poly[-1][0] - poly[-2][0]
        dz = poly[-1][1] - poly[-2][1]
        ln = math.sqrt(dx * dx + dz * dz)
        return (dx / ln, dz / ln) if ln > 0.01 else (1.0, 0.0)

    def _dir_at_start(poly: list[list[float]]) -> tuple[float, float]:
        """Unit direction vector at the START of a polyline (first 2 pts)."""
        if len(poly) < 2:
            return (1.0, 0.0)
        dx = poly[1][0] - poly[0][0]
        dz = poly[1][1] - poly[0][1]
        ln = math.sqrt(dx * dx + dz * dz)
        return (dx / ln, dz / ln) if ln > 0.01 else (1.0, 0.0)

    def _aligned(d1: tuple, d2: tuple) -> bool:
        """True if two direction vectors point roughly the same way (< ~120°)."""
        dot = d1[0] * d2[0] + d1[1] * d2[1]
        return dot > -0.5   # cos(120°) = -0.5

    remaining: list[list[list[float]]] = [list(p) for p in polylines]
    chains: list[list[list[float]]] = []

    while remaining:
        chain = remaining.pop(0)
        changed = True
        while changed:
            changed = False
            for i in range(len(remaining) - 1, -1, -1):
                other = remaining[i]
                # chain tail → other start
                if _close(chain[-1], other[0]):
                    if _aligned(_dir_at_end(chain), _dir_at_start(other)):
                        chain.extend(other[1:])
                        remaining.pop(i)
                        changed = True
                        continue
                # chain tail → other end (reverse other)
                if _close(chain[-1], other[-1]):
                    rev = list(reversed(other))
                    if _aligned(_dir_at_end(chain), _dir_at_start(rev)):
                        chain.extend(rev[1:])
                        remaining.pop(i)
                        changed = True
                        continue
                # other end → chain start
                if _close(chain[0], other[-1]):
                    if _aligned(_dir_at_end(other), _dir_at_start(chain)):
                        chain[:0] = other[:-1]
                        remaining.pop(i)
                        changed = True
                        continue
                # other start (reversed) → chain start
                if _close(chain[0], other[0]):
                    rev = list(reversed(other))
                    if _aligned(_dir_at_end(rev), _dir_at_start(chain)):
                        chain[:0] = rev[:-1]
                        remaining.pop(i)
                        changed = True
                        continue
        chains.append(chain)

    return chains


def _polyline_length(polyline: list[list[float]]) -> float:
    total = 0.0
    for i in range(1, len(polyline)):
        dx = polyline[i][0] - polyline[i - 1][0]
        dz = polyline[i][1] - polyline[i - 1][1]
        total += math.sqrt(dx * dx + dz * dz)
    return total


def _synthesize_train_through_routes(
        stops: list[dict],
        raw_polylines: list[list[list[float]]],
        city_id: str,
        line_offset: int = 0,
        max_proj_dist_m: float = 300.0) -> list[dict]:
    """
    Build train lines as through-routes that cross the municipality border.

    1. Merge all rail polylines into a connected graph (snapping endpoints).
    2. Find endpoints near the municipality boundary (border crossings).
    3. For each pair of border crossings, find the shortest path through
       the rail graph that passes through train stations.
    4. Output train lines that always enter from one side and exit the other.
    """
    try:
        import osm as _osm
        from shapely.geometry import LineString, Point
    except ImportError:
        return []

    city = _osm.CITIES.get(city_id)
    if not city:
        return []

    boundary, _ = _osm._load_municipality_boundary(
        city_id, city["center_lat"], city["center_lon"])
    if boundary is None:
        return []

    # ── Step 1: Merge all raw polylines ──────────────────────────────────
    merged = _merge_polylines(raw_polylines, tolerance_m=25.0)
    if not merged:
        return []

    # ── Step 2: Build a graph of rail segments ───────────────────────────
    # Snap all polyline endpoints to a grid (10m resolution) so nearby
    # endpoints from different polylines connect.
    SNAP = 10.0

    def _snap(pt: list[float]) -> tuple[float, float]:
        return (round(pt[0] / SNAP) * SNAP, round(pt[1] / SNAP) * SNAP)

    # graph: node → list of (neighbor_node, polyline_index, cost)
    from collections import defaultdict
    import heapq

    graph: dict[tuple, list[tuple]] = defaultdict(list)
    poly_lookup: dict[int, list[list[float]]] = {}

    for pi, poly in enumerate(merged):
        if len(poly) < 2:
            continue
        length = _polyline_length(poly)
        if length < 10:
            continue
        start = _snap(poly[0])
        end   = _snap(poly[-1])
        poly_lookup[pi] = poly
        graph[start].append((end, pi, length))
        graph[end].append((start, pi, length))

    if not graph:
        return []

    # ── Step 3: Find border-crossing nodes ───────────────────────────────
    mplx = 111_320.0 * math.cos(math.radians(city["center_lat"]))
    mpl  = 111_320.0

    def _to_lonlat(wx: float, wz: float):
        return (wx / mplx + city["center_lon"],
                -wz / mpl + city["center_lat"])

    border_nodes: list[tuple] = []
    BORDER_DIST_M = 500.0  # nodes within 500m of border are candidates
    for node in graph:
        lon, lat = _to_lonlat(node[0], node[1])
        pt = Point(lon, lat)
        # Distance to boundary exterior (approximate, in degrees → metres)
        dist_deg = boundary.exterior.distance(pt)
        dist_m = dist_deg * 111_320.0
        if dist_m < BORDER_DIST_M or not boundary.contains(pt):
            border_nodes.append(node)

    if len(border_nodes) < 2:
        return []

    # ── Step 4: Find through-routes via Dijkstra ─────────────────────────
    def _dijkstra(src: tuple) -> dict[tuple, tuple]:
        """Return {node: (cost, prev_node, poly_idx)} for cheapest paths."""
        dist: dict[tuple, float] = {src: 0.0}
        prev: dict[tuple, tuple] = {}
        heap = [(0.0, src)]
        while heap:
            d, u = heapq.heappop(heap)
            if d > dist.get(u, float('inf')):
                continue
            for v, pi, cost in graph[u]:
                nd = d + cost
                if nd < dist.get(v, float('inf')):
                    dist[v] = nd
                    prev[v] = (u, pi)
                    heapq.heappush(heap, (nd, v))
        return dist, prev

    def _reconstruct(prev, src, dst):
        """Reconstruct the polyline path from src to dst."""
        if dst not in prev:
            return None
        path_polys = []
        node = dst
        while node != src:
            parent, pi = prev[node]
            poly = poly_lookup[pi]
            start_snap = _snap(poly[0])
            # If we arrived at `node` from `parent`, check direction
            if start_snap == parent or start_snap == node:
                # Figure out which direction to walk
                if _snap(poly[0]) == parent:
                    path_polys.append(list(poly))
                else:
                    path_polys.append(list(reversed(poly)))
            else:
                path_polys.append(list(poly))
            node = parent
        path_polys.reverse()
        # Concatenate into one polyline (skip duplicate junction points)
        result = list(path_polys[0])
        for seg in path_polys[1:]:
            if len(result) > 0 and len(seg) > 0:
                # Skip first point if it's close to the last point
                d2 = (result[-1][0] - seg[0][0])**2 + (result[-1][1] - seg[0][1])**2
                if d2 < SNAP * SNAP * 4:
                    result.extend(seg[1:])
                else:
                    result.extend(seg)
            else:
                result.extend(seg)
        return result

    # Run Dijkstra from each border node, find pairs with long through-routes
    used_pairs: set[tuple] = set()
    routes: list[list[list[float]]] = []

    # Sort border nodes for determinism
    border_nodes.sort()

    for src in border_nodes:
        dist, prev = _dijkstra(src)
        # Find reachable border nodes on the "other side"
        candidates = []
        for dst in border_nodes:
            if dst == src:
                continue
            pair_key = tuple(sorted([src, dst]))
            if pair_key in used_pairs:
                continue
            if dst not in dist:
                continue
            route_len = dist[dst]
            if route_len < 2000:
                continue  # Skip very short through-routes
            candidates.append((route_len, dst))

        # Take the longest route from this source (most likely a main line)
        candidates.sort(reverse=True)
        for route_len, dst in candidates[:1]:
            pair_key = tuple(sorted([src, dst]))
            used_pairs.add(pair_key)
            route = _reconstruct(prev, src, dst)
            if route and len(route) >= 3:
                routes.append(route)

    # Sort through-routes by length (longest first), no cap
    routes.sort(key=lambda r: -_polyline_length(r))

    # ── Step 5: Add back merged polylines >2km that are NOT already
    #    covered by a through-route. This preserves branch lines and
    #    spurs while avoiding hundreds of tiny siding fragments.
    # "Covered" = any sampled point lies within SNAP distance of a
    # through-route point.
    through_set = set()
    for route in routes:
        for pt in route:
            through_set.add(_snap(pt))

    extra = 0
    for poly in merged:
        length = _polyline_length(poly)
        if length < 2000:
            continue
        # Sample 10 evenly-spaced points and check coverage
        step = max(1, len(poly) // 10)
        sample_pts = [poly[i] for i in range(0, len(poly), step)]
        covered = sum(1 for pt in sample_pts if _snap(pt) in through_set)
        if covered > len(sample_pts) * 0.4:
            continue  # Already represented
        routes.append(poly)
        extra += 1

    # ── Step 6: Project stops onto routes and build train lines ──────────
    lines: list[dict] = []
    used_stops: set[int] = set()

    for ri, route in enumerate(routes):
        cum = [0.0]
        for i in range(1, len(route)):
            dx = route[i][0] - route[i - 1][0]
            dz = route[i][1] - route[i - 1][1]
            cum.append(cum[-1] + math.sqrt(dx * dx + dz * dz))

        # Project stops onto this route
        candidates = []
        max_d2 = max_proj_dist_m * max_proj_dist_m
        for stop in stops:
            if stop["idx"] in used_stops:
                continue
            best_d2 = max_d2
            best_seg = -1
            best_dist_along = 0.0
            for i in range(len(route) - 1):
                ax, az = route[i]
                bx, bz = route[i + 1]
                dx, dz = bx - ax, bz - az
                seg2 = dx * dx + dz * dz
                if seg2 < 1.0:
                    continue
                t = ((stop["world_x"] - ax) * dx +
                     (stop["world_z"] - az) * dz) / seg2
                t = max(0.0, min(1.0, t))
                px = ax + t * dx
                pz = az + t * dz
                d2 = (px - stop["world_x"])**2 + (pz - stop["world_z"])**2
                if d2 < best_d2:
                    best_d2 = d2
                    best_seg = i
                    best_dist_along = cum[i] + t * math.sqrt(seg2)
            if best_seg >= 0:
                candidates.append((stop, best_seg, best_dist_along))

        candidates.sort(key=lambda x: x[2])
        for stop, _, _ in candidates:
            used_stops.add(stop["idx"])

        line_idx = line_offset + ri
        lines.append({
            "id":                    f"train_{line_idx}",
            "type":                  "train",
            "stops":                 [c[0]["idx"] for c in candidates],
            "polyline":              [list(p) for p in route],
            "stop_polyline_indices": [c[1] for c in candidates],
            "is_intercity":          False,
        })

    through_count = len([r for r in routes[:len(routes)] if _polyline_length(r) >= 2000])
    print(f"[transport] {city_id}: {len(lines)} train lines "
          f"({through_count} through-routes, "
          f"longest {_polyline_length(routes[0]):.0f}m)")
    return lines


# ── Train line post-processing ────────────────────────────────────────────────

_MIN_TRAIN_LINE_M = 5000   # train lines shorter than this are replaced


def _ensure_long_train_lines(
        all_lines: list[dict],
        train_stops: list[dict],
        rail_polys: list[list[list[float]]],
        city_id: str) -> list[dict]:
    """
    Post-process train lines: if all intra-city train lines are shorter
    than ``_MIN_TRAIN_LINE_M``, replace them with the longest *real* OSM
    merged rail polyline, extended with straight segments at each end to
    reach beyond the city border.

    This handles cities where the through-route synthesis failed because
    the OSM rail graph was too fragmented, while still keeping the route
    on the real rail corridor (not a synthetic straight line).
    """
    # Separate train lines from everything else
    train_lines = [l for l in all_lines
                   if l["type"] == "train" and not l.get("is_intercity")]
    other_lines = [l for l in all_lines
                   if l["type"] != "train" or l.get("is_intercity")]

    # Check if any existing train line is long enough
    longest = max((_polyline_length(l["polyline"]) for l in train_lines),
                  default=0)
    if longest >= _MIN_TRAIN_LINE_M:
        return all_lines   # existing lines are fine

    if len(train_stops) == 0:
        return all_lines   # nothing to build from

    # ── Use the longest real merged rail polyline as the base route ───
    merged = _merge_polylines(rail_polys, tolerance_m=25.0)
    if not merged:
        return all_lines   # no rail data at all

    # Pick the longest merged chain — this follows the real rail corridor
    base_poly = max(merged, key=_polyline_length)
    base_len = _polyline_length(base_poly)

    # Use the real rail polyline as-is — no artificial straight-line
    # extensions. The OSM data already extends beyond the city border
    # when mapped; where it doesn't, the line ends at the last real
    # rail point rather than faking a path that deviates from the
    # actual rail corridor visible on the base map.
    polyline = [list(p) for p in base_poly]

    # ── Project train stops onto this polyline ───────────────────────
    # Compute cumulative distance along polyline
    cum = [0.0]
    for i in range(1, len(polyline)):
        dx = polyline[i][0] - polyline[i - 1][0]
        dz = polyline[i][1] - polyline[i - 1][1]
        cum.append(cum[-1] + math.hypot(dx, dz))

    stop_projections = []
    for s in train_stops:
        best_dist_along = None
        best_perp = float("inf")
        for i in range(len(polyline) - 1):
            ax, az = polyline[i]
            bx, bz = polyline[i + 1]
            segdx, segdz = bx - ax, bz - az
            seg_len = math.hypot(segdx, segdz)
            if seg_len < 0.01:
                continue
            t = max(0, min(1, ((s["world_x"] - ax) * segdx +
                               (s["world_z"] - az) * segdz) / (seg_len * seg_len)))
            px = ax + t * segdx
            pz = az + t * segdz
            perp = math.hypot(s["world_x"] - px, s["world_z"] - pz)
            if perp < best_perp:
                best_perp = perp
                best_dist_along = cum[i] + t * seg_len
        if best_perp < 500:  # within 500m of the rail
            stop_projections.append((s, best_dist_along))

    stop_projections.sort(key=lambda x: x[1])

    # Find polyline indices closest to each stop's projection
    stop_indices = []
    stop_list = []
    for s, dist_along in stop_projections:
        # Find the polyline segment containing this distance
        idx = 0
        for i in range(len(cum) - 1):
            if cum[i] <= dist_along <= cum[i + 1]:
                idx = i
                break
        stop_indices.append(idx)
        stop_list.append(s)

    line_idx = max((l.get("_idx", 0) for l in other_lines), default=-1) + 1
    new_line = {
        "id":                    f"train_{city_id}_{line_idx}",
        "type":                  "train",
        "stops":                 [s["idx"] for s in stop_list],
        "polyline":              polyline,
        "stop_polyline_indices": stop_indices if stop_indices else [0],
        "is_intercity":          False,
    }

    total_len = _polyline_length(polyline)
    print(f"[transport] {city_id}: replaced short train lines with "
          f"extended real rail route ({total_len:.0f}m, "
          f"{len(stop_list)} stations, {len(polyline)} points)")

    return other_lines + [new_line]


def _synthesize_polyline_lines(stops: list[dict],
                               polylines: list[list[list[float]]],
                               line_type: str,
                               max_stops: int = 12,
                               max_proj_dist_m: float = 250.0) -> list[dict]:
    """
    For tram and intra-city train lines: input polylines are first merged
    at shared endpoints into longer connected routes, then each merged
    polyline becomes one line. Stops within ``max_proj_dist_m`` of
    the polyline are projected onto it and ordered by distance-along-track.
    """
    lines: list[dict] = []
    if not stops or not polylines:
        return lines

    # Merge short OSM segments into longer continuous routes
    polylines = _merge_polylines(polylines, tolerance_m=25.0)

    used_stops: set[int] = set()
    line_idx = 0

    for poly in polylines:
        if len(poly) < 2:
            continue

        # Cumulative distance along this polyline
        cum: list[float] = [0.0]
        for i in range(1, len(poly)):
            dx = poly[i][0] - poly[i - 1][0]
            dz = poly[i][1] - poly[i - 1][1]
            cum.append(cum[-1] + math.sqrt(dx * dx + dz * dz))

        # Skip polylines that are too short to produce useful routes
        if cum[-1] < 500.0:
            continue

        # Project candidate stops onto this polyline
        candidates: list[tuple[dict, int, float]] = []
        max_d2 = max_proj_dist_m * max_proj_dist_m
        for stop in stops:
            if stop["idx"] in used_stops:
                continue
            best_d2 = max_d2
            best_seg = -1
            best_dist_along = 0.0
            for i in range(len(poly) - 1):
                ax, az = poly[i]
                bx, bz = poly[i + 1]
                dx, dz = bx - ax, bz - az
                seg2 = dx * dx + dz * dz
                if seg2 < 1.0:
                    continue
                t = ((stop["world_x"] - ax) * dx
                     + (stop["world_z"] - az) * dz) / seg2
                t = max(0.0, min(1.0, t))
                px = ax + t * dx
                pz = az + t * dz
                d2 = ((px - stop["world_x"]) ** 2
                      + (pz - stop["world_z"]) ** 2)
                if d2 < best_d2:
                    best_d2 = d2
                    best_seg = i
                    best_dist_along = cum[i] + t * math.sqrt(seg2)
            if best_seg >= 0:
                candidates.append((stop, best_seg, best_dist_along))

        if len(candidates) < 2:
            continue

        candidates.sort(key=lambda x: x[2])

        # Cap line length: sample evenly if too many stops fell on this polyline
        if len(candidates) > max_stops:
            step = len(candidates) / max_stops
            candidates = [candidates[int(i * step)] for i in range(max_stops)]

        for stop, _, _ in candidates:
            used_stops.add(stop["idx"])

        # Trim polyline between first and last stop projection (saves bytes
        # and avoids visual confusion when a track extends way past the line)
        first_seg = candidates[0][1]
        last_seg  = candidates[-1][1]
        trimmed = [list(p) for p in poly[first_seg : last_seg + 2]]
        if len(trimmed) < 2:
            trimmed = [list(p) for p in poly]

        lines.append({
            "id":                    f"{line_type}_{line_idx}",
            "type":                  line_type,
            "stops":                 [c[0]["idx"] for c in candidates],
            "polyline":              trimmed,
            "stop_polyline_indices": [max(0, c[1] - first_seg) for c in candidates],
            "is_intercity":          False,
        })
        line_idx += 1

    return lines


# ── Ferry line synthesis ─────────────────────────────────────────────────────

def _ferry_lines_from_osm(
        osm_routes: list[dict],
        ferry_stops: list[dict],
        city_id: str) -> list[dict]:
    """
    Build ferry lines from real OSM ferry route relations.

    Each OSM route has stop positions and optionally a waterway polyline.
    We match our ferry stops to the route's stop positions by proximity,
    and use the OSM polyline if available — otherwise connect stops with
    straight lines (ferries don't follow roads).
    """
    if not osm_routes or not ferry_stops:
        return []

    # Spatial index of ferry stops
    stop_by_pos = [(s["world_x"], s["world_z"], s) for s in ferry_stops]

    def _nearest(wx, wz, max_dist=1500.0):
        best, best_d2 = None, max_dist * max_dist
        for sx, sz, s in stop_by_pos:
            d2 = (sx - wx) ** 2 + (sz - wz) ** 2
            if d2 < best_d2:
                best_d2 = d2
                best = s
        return best

    lines: list[dict] = []
    seen_refs: dict[str, int] = {}
    line_idx = 0

    for route in osm_routes:
        ref = route.get("ref", "")
        osm_stops = route.get("stops", [])
        osm_poly = route.get("polyline", [])

        if len(osm_stops) < 2 and len(osm_poly) < 2:
            continue

        # Dedup: max 2 variants per ref (forward + backward)
        if ref:
            seen_refs[ref] = seen_refs.get(ref, 0) + 1
            if seen_refs[ref] > 2:
                continue

        # Match our ferry stops to the route's stops
        matched: list[dict] = []
        for coord in osm_stops:
            wx, wz = coord
            nearest = _nearest(wx, wz)
            if nearest and nearest["idx"] not in {s["idx"] for s in matched}:
                matched.append(nearest)

        # If the OSM relation had no stop members (common for ferry routes),
        # try matching the polyline start/end to nearby ferry stops instead.
        if len(matched) < 2 and osm_poly and len(osm_poly) >= 2:
            for coord in [osm_poly[0], osm_poly[-1]]:
                wx, wz = coord
                nearest = _nearest(wx, wz, max_dist=2000.0)
                if nearest and nearest["idx"] not in {s["idx"] for s in matched}:
                    matched.append(nearest)
            # Also check midpoints for routes that pass through intermediate stops
            if len(osm_poly) > 10:
                mid = len(osm_poly) // 2
                for sample_idx in [mid // 2, mid, mid + mid // 2]:
                    if sample_idx < len(osm_poly):
                        wx, wz = osm_poly[sample_idx]
                        nearest = _nearest(wx, wz, max_dist=800.0)
                        if nearest and nearest["idx"] not in {s["idx"] for s in matched}:
                            matched.append(nearest)

        # Ferry routes often cross borders (Helsingborg→Denmark, Halmstad→Grenå)
        # where the far end has no stop in our data. Allow 1-stop lines if we
        # have a good polyline — the vehicle will sail the full route visually.
        if len(matched) < 1:
            continue
        if len(matched) < 2 and (not osm_poly or len(osm_poly) < 4):
            continue

        # Use OSM polyline if available, else straight lines between stops
        if osm_poly and len(osm_poly) >= 2:
            polyline = [[round(p[0], 1), round(p[1], 1)] for p in osm_poly]
        else:
            polyline = [[s["world_x"], s["world_z"]] for s in matched]

        # Project stops onto polyline for indices
        stop_polyline_indices = []
        for s in matched:
            best_idx, best_d = 0, float("inf")
            for pi in range(len(polyline)):
                d = math.hypot(polyline[pi][0] - s["world_x"],
                               polyline[pi][1] - s["world_z"])
                if d < best_d:
                    best_d = d
                    best_idx = pi
            stop_polyline_indices.append(best_idx)

        lines.append({
            "id":                    f"ferry_{city_id}_{line_idx}",
            "type":                  "ferry",
            "stops":                 [s["idx"] for s in matched],
            "polyline":              polyline,
            "stop_polyline_indices": stop_polyline_indices,
            "is_intercity":          False,
        })
        line_idx += 1

    return lines


def _clip_long_ferry_lines(
        ferry_lines: list[dict],
        city_id: str,
        max_extent_m: float = 20_000.0) -> list[dict]:
    """
    Trim international ferry routes so they don't draw straight lines
    through the city centre.  Routes whose polyline extends beyond
    *max_extent_m* from the city origin (0,0) are clipped to keep only
    the portion within that radius.
    """
    clipped = 0
    for line in ferry_lines:
        poly = line.get("polyline", [])
        if len(poly) < 2:
            continue
        # Check if any point is beyond the max extent
        far = any(math.hypot(p[0], p[1]) > max_extent_m for p in poly)
        if not far:
            continue
        # Keep only points within the radius
        kept = [p for p in poly if math.hypot(p[0], p[1]) <= max_extent_m]
        if len(kept) >= 2:
            line["polyline"] = kept
            # Re-project stop indices onto the trimmed polyline
            stop_idxs = line.get("stops", [])
            # (stop_polyline_indices become invalid after clipping — reset
            # to 0 and len-1 as approximation; the visual is close enough)
            if len(stop_idxs) >= 2:
                line["stop_polyline_indices"] = [0] + [len(kept) - 1] * (len(stop_idxs) - 1)
            elif len(stop_idxs) == 1:
                line["stop_polyline_indices"] = [0]
            clipped += 1
    if clipped:
        print(f"[transport] {city_id}: clipped {clipped} long ferry routes "
              f"to {max_extent_m/1000:.0f}km radius")
    return ferry_lines


# ── Inter-city: flights and long-distance trains ─────────────────────────────

_kommun_centroids_cache: dict[str, dict] | None = None


def _load_kommun_centroids() -> dict[str, dict]:
    """Load the metapop kommun centroid table from data/_national/. Empty if missing."""
    global _kommun_centroids_cache
    if _kommun_centroids_cache is not None:
        return _kommun_centroids_cache
    path = os.path.join(os.path.dirname(__file__), "data", "_national",
                        "kommun_centroids.json")
    if not os.path.exists(path):
        _kommun_centroids_cache = {}
        return _kommun_centroids_cache
    try:
        with open(path, encoding="utf-8") as f:
            _kommun_centroids_cache = json.load(f)
    except Exception:
        _kommun_centroids_cache = {}
    return _kommun_centroids_cache


def _nearest_kommuner(focused_kommun: str | None, n: int = 3
                      ) -> list[tuple[str, dict, float]]:
    """Find ``n`` nearest kommuner by approximate flat-earth distance."""
    if not focused_kommun:
        return []
    centroids = _load_kommun_centroids()
    fc = centroids.get(focused_kommun)
    if not fc:
        return []
    f_lat = fc.get("lat")
    f_lon = fc.get("lon")
    if f_lat is None or f_lon is None:
        return []
    cosf = math.cos(math.radians(f_lat))
    out: list[tuple[float, str, dict]] = []
    for kommun, c in centroids.items():
        if kommun == focused_kommun:
            continue
        lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None:
            continue
        dlat = lat - f_lat
        dlon = (lon - f_lon) * cosf
        d2 = dlat * dlat + dlon * dlon
        out.append((d2, kommun, c))
    out.sort()
    return [(k, c, d2) for d2, k, c in out[:n]]


def _flight_destinations_for(city_id: str) -> list[str]:
    return _FLIGHT_DESTINATIONS.get(city_id, _DEFAULT_FLIGHT_DESTINATIONS)


def _resolve_destination(code: str) -> dict | None:
    """Map a destination code to a {name, kommun, lat, lon} dict, or None."""
    if code in osm.CITIES:
        meta = osm.CITIES[code]
        return {
            "name":   meta["name"],
            "kommun": meta.get("kommunkod"),
            "lat":    meta["center_lat"],
            "lon":    meta["center_lon"],
        }
    if code in _FOREIGN_DESTINATIONS:
        return _FOREIGN_DESTINATIONS[code]
    return None


def _generate_intercity(infra: dict, city_id: str, focused_kommun: str | None,
                        mode: str) -> tuple[list[dict], list[dict]]:
    """
    Build inter-city lines + trips for ``mode`` ∈ {"flight", "intercity_train"}.

    For each destination, creates one virtual line and a pair of timetabled
    trips (outbound + inbound) at each headway slot.
    """
    stops = infra.get("stops", [])
    if mode == "flight":
        sources = [s for s in stops if s["type"] == "airport"]
        if not sources:
            return [], []
        codes = _flight_destinations_for(city_id)
        destinations = [d for d in (_resolve_destination(c) for c in codes) if d]
        params = _MODE_PARAMS["flight"]
    elif mode == "intercity_train":
        sources = [s for s in stops if s["type"] == "train"]
        if not sources:
            return [], []
        # Pick a "main" station: closest to city center (world origin).
        src_stop = min(sources,
                       key=lambda s: s["world_x"] ** 2 + s["world_z"] ** 2)
        nearest = _nearest_kommuner(focused_kommun, n=3)
        destinations = []
        for kommun, c, _ in nearest:
            destinations.append({
                "name":   c.get("name") or kommun,
                "kommun": kommun,
                "lat":    c.get("lat"),
                "lon":    c.get("lon"),
            })
        params = _MODE_PARAMS["intercity_train"]

        # Visual: instead of synthesising a straight line to each remote
        # kommun centroid (which makes trains "go in every direction"
        # ignoring the rail network), pick the LONGEST rail polyline that
        # exists in the city's bbox and use it as the visual path for ALL
        # inter-city trains. The destination kommun still drives epi
        # coupling — only the visual rendering changes.
        #
        # Also try to merge polylines that share endpoints so a rail line
        # broken into multiple OSM ways becomes one continuous chain that
        # spans more of the city.
        rail_polys = [r["polyline"] for r in (infra.get("routes") or [])
                      if r["type"] == "rail" and len(r.get("polyline") or []) >= 2]
        merged = _merge_polylines(rail_polys, tolerance_m=8.0)
        shared_polyline = None
        if merged:
            # Pick the longest merged chain.
            def _length(poly):
                return sum(math.hypot(poly[i][0] - poly[i-1][0],
                                      poly[i][1] - poly[i-1][1])
                           for i in range(1, len(poly)))
            shared_polyline = max(merged, key=_length)
        for d in destinations:
            d["polyline"] = shared_polyline
    else:
        return [], []

    if not destinations:
        return [], []

    f_meta = osm.CITIES[city_id]
    f_lat, f_lon = f_meta["center_lat"], f_meta["center_lon"]
    cosf = math.cos(math.radians(f_lat))
    mplx = 111_320.0 * cosf
    mpl  = 111_320.0

    lines: list[dict] = []
    trips: list[dict] = []

    # For flights, generate lines from EVERY airport (not just the first).
    # The frontend filters out lines whose airport falls outside the city
    # border, so all in-border airports get visible airplane traffic.
    flight_sources = sources if mode == "flight" else [None]

    for src_stop in flight_sources:
      for dest in destinations:
        dest_kommun = dest.get("kommun")
        dest_name   = dest.get("name", "?")
        src_tag = f"_ap{src_stop['idx']}" if src_stop and len(flight_sources) > 1 else ""
        line_id = f"{mode}_{city_id}{src_tag}_to_{dest_kommun or dest_name.lower().replace(' ', '_')}"

        if dest.get("polyline"):
            # Inter-city train uses a real local rail polyline.
            polyline = [list(p) for p in dest["polyline"]]
        else:
            # Inter-city flight: U-shape so takeoff AND landing both
            # happen at the visible airport. Plane flies out 3 km in
            # the destination direction, turns around, comes back to
            # land. The destination kommun centroid still drives epi
            # routing — the visual is decoupled.
            VISIBLE_RANGE_M = 3000.0
            if dest.get("lat") is not None and dest.get("lon") is not None:
                dx = (dest["lon"] - f_lon) * mplx
                dz = -(dest["lat"] - f_lat) * mpl
                dist = math.hypot(dx, dz) or 1.0
                ux = dx / dist
                uz = dz / dist
            else:
                ux, uz = 1.0, 0.0
            sx = src_stop["world_x"]
            sz = src_stop["world_z"]
            mid_x = sx + ux * VISIBLE_RANGE_M
            mid_z = sz + uz * VISIBLE_RANGE_M
            # Slight perpendicular offset on the return leg so the U
            # has visible width — otherwise the plane retraces the same
            # line and the user can't tell it's coming back.
            perp_x = -uz * 200.0
            perp_z =  ux * 200.0
            polyline = [
                [round(sx,                  1), round(sz,                  1)],
                [round(mid_x,               1), round(mid_z,               1)],
                [round(sx + perp_x,         1), round(sz + perp_z,         1)],
            ]

        vehicle_type = "plane" if mode == "flight" else "train"
        _src_stop = src_stop if src_stop else sources[0]
        line = {
            "id":                    line_id,
            "type":                  vehicle_type,
            "stops":                 [_src_stop["idx"]],
            "polyline":              polyline,
            "stop_polyline_indices": [0],
            "is_intercity":          True,
            "destination_name":      dest_name,
            "destination_kommun":    dest_kommun,
        }
        lines.append(line)

        # Per-trip duration: clamp to a visible range so the takeoff/cruise/
        # descent phases all read on screen even at fast sim speeds. Real
        # Stockholm-Västerås at 700 km/h is ~7 min, which would flicker;
        # 60 min minimum gives the parametric arc time to play out.
        seg_dist = math.hypot(polyline[1][0] - polyline[0][0],
                              polyline[1][1] - polyline[0][1])
        speed_mpm = params["speed_kmh"] * 1000.0 / 60.0
        duration_min = int(seg_dist / max(speed_mpm, 1.0))
        duration_min = max(60, min(180, duration_min))

        depart = int(params["start_hour"] * 60)
        end_dep = int(params["end_hour"] * 60)
        headway = int(params["headway_min"])
        trip_idx = 0

        while depart <= end_dep and depart + duration_min < 1440:
            # Outbound: focused city → destination
            trips.append({
                "id":             f"{line_id}_out_{trip_idx}",
                "line_id":        line_id,
                "vehicle_type":   vehicle_type,
                "depart_minute":  depart,
                "arrive_minute":  depart + duration_min,
                "capacity":       int(params["capacity"]),
                "src_stop_idx":   _src_stop["idx"],
                "dst_stop_idx":   -1,
                "src_kommun":     focused_kommun,
                "dst_kommun":     dest_kommun,
                "direction":      "out",
            })
            # Inbound: destination → focused city, offset by 30 min
            in_depart = depart + 30
            if in_depart + duration_min < 1440:
                trips.append({
                    "id":             f"{line_id}_in_{trip_idx}",
                    "line_id":        line_id,
                    "vehicle_type":   vehicle_type,
                    "depart_minute":  in_depart,
                    "arrive_minute":  in_depart + duration_min,
                    "capacity":       int(params["capacity"]),
                    "src_stop_idx":   -1,
                    "dst_stop_idx":   _src_stop["idx"],
                    "src_kommun":     dest_kommun,
                    "dst_kommun":     focused_kommun,
                    "direction":      "in",
                })
            depart += headway
            trip_idx += 1

    return lines, trips


# ── Schedule generation for intra-city lines ─────────────────────────────────

def _generate_line_trips(line: dict) -> list[dict]:
    """Build the day's trip list for a single intra-city line (bus/tram/train)."""
    mode = line["type"]
    params = _MODE_PARAMS.get(mode, _MODE_PARAMS["bus"])
    poly_len = _polyline_length(line["polyline"])
    speed_mpm = params["speed_kmh"] * 1000.0 / 60.0
    duration_min = max(2, int(poly_len / max(speed_mpm, 1.0)))

    if not line["stops"] or len(line["stops"]) < 2:
        return []

    src_stop = line["stops"][0]
    dst_stop = line["stops"][-1]
    headway = int(params["headway_min"])
    start = int(params["start_hour"] * 60)
    end_dep = int(params["end_hour"] * 60)

    trips: list[dict] = []
    trip_idx = 0
    for direction in ("forward", "backward"):
        s = src_stop if direction == "forward" else dst_stop
        d = dst_stop if direction == "forward" else src_stop
        depart = start + (5 if direction == "backward" else 0)
        while depart <= end_dep and depart + duration_min < 1440:
            trips.append({
                "id":             f"{line['id']}_{direction[0]}{trip_idx}",
                "line_id":        line["id"],
                "vehicle_type":   mode,
                "depart_minute":  depart,
                "arrive_minute":  depart + duration_min,
                "capacity":       int(params["capacity"]),
                "src_stop_idx":   s,
                "dst_stop_idx":   d,
                "direction":      direction,
            })
            depart += headway
            trip_idx += 1
    return trips


# ── Top-level: build everything for one city ─────────────────────────────────

def generate_for_city(city_id: str,
                      infra: dict | None = None,
                      city_layout: dict | None = None) -> dict:
    """
    Build (or rebuild) the synthetic schedule for ``city_id``.

    Writes ``data/<city>/transport_schedule.json`` and returns the in-memory
    bundle. Re-runs are idempotent — call this from
    ``tools/download_city.py`` after ``osm.load_transport`` and the cache will
    be regenerated cleanly.

    Schema (also returned):
        {
          "version": 1,
          "city_id": "vasteras",
          "lines":   [{id, type, stops, polyline, stop_polyline_indices,
                       is_intercity, destination_name?, destination_kommun?}, ...],
          "trips":   [{id, line_id, vehicle_type, depart_minute,
                       arrive_minute, capacity, src_stop_idx, dst_stop_idx,
                       src_kommun?, dst_kommun?, direction}, ...]
        }
    """
    if infra is None:
        infra = osm.load_transport(city_id)
    if city_layout is None:
        city_layout = osm.load_city(city_id)

    stops = infra.get("stops", [])
    routes = infra.get("routes", [])

    bus_stops   = [s for s in stops if s["type"] in ("bus", "bus_station")]
    tram_stops  = [s for s in stops if s["type"] == "tram"]
    train_stops = [s for s in stops if s["type"] == "train"]
    ferry_stops = [s for s in stops if s["type"] == "ferry"]
    airports    = [s for s in stops if s["type"] == "airport"]

    rail_polys  = [r["polyline"] for r in routes if r["type"] == "rail"]
    tram_polys  = [r["polyline"] for r in routes if r["type"] == "tram"]

    rng = random.Random(hash(city_id) & 0xFFFFFFFF)

    lines: list[dict] = []

    # Build bus lines. Prefer real OSM bus route relations when available;
    # fall back to synthetic radial routes for uncovered stops.
    if bus_stops:
        road_graph = _build_road_graph(city_layout.get("roads", []))
        osm_bus_routes = infra.get("bus_routes", [])
        if osm_bus_routes and road_graph[2]:
            real_lines, covered = _bus_lines_from_osm(
                osm_bus_routes, bus_stops, road_graph, city_id)
            lines.extend(real_lines)
            # Synthetic radial lines for uncovered areas
            uncovered = [s for s in bus_stops if s["idx"] not in covered]
            if uncovered and len(uncovered) >= 4:
                synth = _synthesize_bus_lines(uncovered, road_graph, rng)
                lines.extend(synth)
                for sl in synth:
                    covered.update(sl["stops"])
            # Coverage sweep: pick up any remaining road-accessible stops
            sweep = _cover_remaining_stops(
                bus_stops, covered, road_graph, rng, city_id)
            lines.extend(sweep)
        elif road_graph[2]:
            lines.extend(_synthesize_bus_lines(bus_stops, road_graph, rng))
            # Coverage sweep for purely synthetic cities too
            covered_synth = set()
            for l in lines:
                if l["type"] == "bus":
                    covered_synth.update(l["stops"])
            sweep = _cover_remaining_stops(
                bus_stops, covered_synth, road_graph, rng, city_id)
            lines.extend(sweep)

    if tram_stops and tram_polys:
        lines.extend(_synthesize_polyline_lines(tram_stops, tram_polys, "tram"))

    if train_stops and rail_polys:
        # Try building through-routes that cross the city border first.
        # Falls back to the polyline-per-line approach if no border is available.
        train_lines = _synthesize_train_through_routes(
            train_stops, rail_polys, city_id, len(lines))
        if train_lines:
            lines.extend(train_lines)
        else:
            lines.extend(_synthesize_polyline_lines(
                train_stops, rail_polys, "train"))

        # Post-process: ensure train lines are long enough. Short or missing
        # lines get replaced by extended routes through the train stations
        # that reach beyond the city border.
        lines = _ensure_long_train_lines(
            lines, train_stops, rail_polys, city_id)

    # Ferry lines from OSM route relations
    if ferry_stops:
        osm_ferry_routes = infra.get("ferry_routes", [])
        ferry_lines = _ferry_lines_from_osm(osm_ferry_routes, ferry_stops, city_id)
        if ferry_lines:
            # Clip overly long international routes so they don't draw
            # straight lines through the city centre.
            ferry_lines = _clip_long_ferry_lines(ferry_lines, city_id)
            lines.extend(ferry_lines)
            print(f"[transport] {city_id}: {len(ferry_lines)} ferry lines, "
                  f"{len(ferry_stops)} ferry stops")

    # Trips: intra-city first
    trips: list[dict] = []
    for line in lines:
        trips.extend(_generate_line_trips(line))

    # Inter-city: flights and long-distance trains
    focused_kommun = osm.CITIES[city_id].get("kommunkod")
    if airports:
        ic_lines, ic_trips = _generate_intercity(infra, city_id, focused_kommun, "flight")
        lines.extend(ic_lines)
        trips.extend(ic_trips)
    if train_stops:
        ic_lines, ic_trips = _generate_intercity(infra, city_id, focused_kommun,
                                                 "intercity_train")
        lines.extend(ic_lines)
        trips.extend(ic_trips)

    trips.sort(key=lambda t: t["depart_minute"])

    bundle = {
        "version": 1,
        "city_id": city_id,
        "lines":   lines,
        "trips":   trips,
    }

    out_path = os.path.join(os.path.dirname(__file__), "data", city_id,
                            "transport_schedule.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, separators=(",", ":"))

    by_type: dict[str, int] = {}
    for line in lines:
        by_type[line["type"]] = by_type.get(line["type"], 0) + 1
    print(f"[transport] {city_id}: {len(lines)} lines ({by_type}), "
          f"{len(trips)} trips -> transport_schedule.json")
    return bundle


def load_schedule(city_id: str) -> dict | None:
    """Read ``data/<city>/transport_schedule.json`` if it exists, else None."""
    path = os.path.join(os.path.dirname(__file__), "data", city_id,
                        "transport_schedule.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ── Runtime trip dispatcher ──────────────────────────────────────────────────

class TripDispatcher:
    """
    Engine-attached dispatcher that processes trip arrivals each sub-step.

    The engine calls ``process(prev_minute, cur_minute, engine, metapop)``
    once per ``_step_one`` after the SEIR loop. The dispatcher iterates
    every minute in ``[prev_minute, cur_minute)`` (handling day rollover),
    looks up trips bucketed by arrival minute, and applies each trip as a
    discrete mass-transfer event:

      * intra-city: subtract manifest from origin neighborhood, run an
        in-vehicle SEIR delta, deposit into destination neighborhood
      * inter-city: same idea but the "remote" side is a metapop coarse
        node, manipulated via ``apply_discrete_arrival/departure``

    Construction is cheap apart from the per-stop "nearby buildings"
    precompute, which uses a grid hash so it scales linearly in
    (stops × avg neighbors).
    """

    def __init__(self,
                 infra: dict,
                 schedule: dict,
                 buildings: list,
                 focused_kommun: str | None) -> None:
        self._infra          = infra
        self._schedule       = schedule
        self._buildings      = buildings
        self._focused_kommun = focused_kommun

        # Bucket trips by arrival minute (1440 buckets)
        self._trips_by_arrival: list[list[dict]] = [[] for _ in range(1440)]
        for trip in schedule.get("trips", []) or []:
            am = int(trip.get("arrive_minute", 0)) % 1440
            self._trips_by_arrival[am].append(trip)

        # Precompute per-stop neighborhood: stop_idx -> [building_idx, ...]
        self._stop_neighbors: dict[int, list[int]] = {}
        self._precompute_neighborhoods()

        # Telemetry — exposed for debugging / tests
        self.trips_processed = 0

    # ── precomputation ───────────────────────────────────────────────────────

    def _precompute_neighborhoods(self) -> None:
        if not self._buildings:
            return
        radius = STOP_NEIGHBORHOOD_RADIUS_M
        cell = max(50.0, radius / 2)
        grid: dict[tuple[int, int], list[int]] = {}
        for b in self._buildings:
            key = (int(b.world_x // cell), int(b.world_z // cell))
            grid.setdefault(key, []).append(b.idx)

        r2 = radius * radius
        search = max(2, int(math.ceil(radius / cell)))
        for stop in self._infra.get("stops", []) or []:
            sx, sz = stop["world_x"], stop["world_z"]
            cx, cz = int(sx // cell), int(sz // cell)
            neighbors: list[int] = []
            for dcx in range(-search, search + 1):
                for dcz in range(-search, search + 1):
                    for bi in grid.get((cx + dcx, cz + dcz), []):
                        b = self._buildings[bi]
                        d2 = (b.world_x - sx) ** 2 + (b.world_z - sz) ** 2
                        if d2 <= r2:
                            neighbors.append(bi)
            self._stop_neighbors[stop["idx"]] = neighbors

    def reset(self) -> None:
        """Called by engine.reset(). No persistent state to clear today."""
        self.trips_processed = 0

    # ── tick ──────────────────────────────────────────────────────────────────

    def process(self, prev_minute: int, cur_minute: int,
                engine: Any, metapop: Any) -> None:
        """
        Apply every trip arriving in ``[prev_minute, cur_minute)``.
        Handles day rollover (cur < prev).
        """
        if cur_minute == prev_minute:
            return
        if cur_minute > prev_minute:
            minutes = range(prev_minute, cur_minute)
        else:
            minutes = list(range(prev_minute, 1440)) + list(range(0, cur_minute))

        for m in minutes:
            for trip in self._trips_by_arrival[m]:
                self._apply_trip(trip, engine, metapop)

    # ── per-trip mass transfer ───────────────────────────────────────────────

    def _apply_trip(self, trip: dict, engine: Any, metapop: Any) -> None:
        cap = float(trip.get("capacity", 0) or 0)
        if cap <= 0:
            return
        vtype = trip.get("vehicle_type", "bus")
        active = engine._active

        # ── Suspend-flights intervention: skip all flight trips ──
        if vtype == "flight" and "suspend_flights" in active:
            return
        # ── Full lockdown OR Stop Public Transport: all modes grounded ──
        if "lockdown" in active or "stop_transport" in active:
            return

        depart = int(trip.get("depart_minute", 0))
        arrive = int(trip.get("arrive_minute", 0))
        dt_hours = max(1.0 / 60.0, (arrive - depart) / 60.0)

        src_kommun = trip.get("src_kommun")
        dst_kommun = trip.get("dst_kommun")
        is_intercity = (src_kommun is not None or dst_kommun is not None)

        src_stop = int(trip.get("src_stop_idx", -1))
        dst_stop = int(trip.get("dst_stop_idx", -1))

        if not is_intercity:
            self._apply_local_trip(src_stop, dst_stop, cap, dt_hours,
                                   vtype, active)
            self.trips_processed += 1
            return

        # ── inter-city ──
        focused_is_src = (src_kommun == self._focused_kommun and src_stop >= 0)
        focused_is_dst = (dst_kommun == self._focused_kommun and dst_stop >= 0)

        if focused_is_src:
            manifest = self._sample_origin(src_stop, cap)
            if manifest is None:
                return
            self._remove_from_buildings(src_stop, manifest)
            # Station screening: catch ~40 % of infectious before boarding
            self._apply_screening(manifest, src_stop, active)
            self._in_vehicle_step(manifest, dt_hours, vtype, cap, active)
            if metapop is not None and dst_kommun:
                i_frac = manifest["I"] / max(manifest["N"], 1.0)
                metapop.apply_discrete_arrival(dst_kommun, manifest["N"], i_frac)
            self.trips_processed += 1
            return

        if focused_is_dst:
            if metapop is None or not src_kommun:
                # Foreign-origin or no metapop: skip the epi side, visualization
                # still runs because the trip is in transport_schedule.json.
                return
            src_node = metapop._nodes.get(src_kommun)
            if src_node is None or src_node.N <= 0:
                return
            i_frac = src_node.I / src_node.N
            n = min(cap, max(0.0, src_node.S * 0.001))
            if n <= 0:
                return
            metapop.apply_discrete_departure(src_kommun, n)
            manifest = {
                "S": n * (1.0 - i_frac),
                "E": 0.0,
                "I": n * i_frac,
                "R": 0.0,
                "D": 0.0,
                "N": n,
            }
            self._in_vehicle_step(manifest, dt_hours, vtype, cap, active)
            self._deposit_to_buildings(dst_stop, manifest)
            self.trips_processed += 1
            return

        # Neither side focused — pure metapop event
        if metapop is None or not src_kommun or not dst_kommun:
            return
        src_node = metapop._nodes.get(src_kommun)
        if src_node is None or src_node.N <= 0:
            return
        i_frac = src_node.I / src_node.N
        n = min(cap, max(0.0, src_node.S * 0.001))
        if n > 0:
            metapop.apply_discrete_departure(src_kommun, n)
            metapop.apply_discrete_arrival(dst_kommun, n, i_frac)
            self.trips_processed += 1

    # ── intra-city helper ────────────────────────────────────────────────────

    def _apply_local_trip(self, src_stop: int, dst_stop: int,
                          cap: float, dt_hours: float,
                          vehicle_type: str = "bus",
                          active: frozenset = frozenset()) -> None:
        if src_stop == dst_stop or src_stop < 0 or dst_stop < 0:
            return
        manifest = self._sample_origin(src_stop, cap)
        if manifest is None:
            return
        self._remove_from_buildings(src_stop, manifest)
        self._apply_screening(manifest, src_stop, active)
        self._in_vehicle_step(manifest, dt_hours, vehicle_type, cap, active)
        self._deposit_to_buildings(dst_stop, manifest)

    # ── manifest math ────────────────────────────────────────────────────────

    def _sample_origin(self, stop_idx: int, cap: float) -> dict | None:
        neighbors = self._stop_neighbors.get(stop_idx)
        if not neighbors:
            return None
        S = E = I = R = D = 0.0
        N = 0.0
        for bi in neighbors:
            b = self._buildings[bi]
            S += b.S_tot; E += b.E_tot; I += b.I_tot
            R += b.R_tot; D += b.D_tot
            N += b.N
        if N <= 0:
            return None
        n = min(cap, MANIFEST_CAP_FRAC * N)
        if n <= 0:
            return None
        return {
            "S": n * S / N,
            "E": n * E / N,
            "I": n * I / N,
            "R": n * R / N,
            "D": n * D / N,
            "N": n,
        }

    def _remove_from_buildings(self, stop_idx: int, manifest: dict) -> None:
        neighbors = self._stop_neighbors.get(stop_idx)
        if not neighbors:
            return
        N_total = sum(self._buildings[bi].N for bi in neighbors)
        if N_total <= 0:
            return
        for bi in neighbors:
            b = self._buildings[bi]
            if b.N <= 0:
                continue
            share = b.N / N_total
            for compartment_name, total in (
                ("S", manifest["S"]), ("E", manifest["E"]),
                ("I", manifest["I"]), ("R", manifest["R"]),
                ("D", manifest["D"]),
            ):
                comp = getattr(b, compartment_name)
                amt = total * share
                if amt <= 0:
                    continue
                cg_total = comp["child"] + comp["adult"] + comp["elder"]
                if cg_total <= 0:
                    continue
                for g in ("child", "adult", "elder"):
                    g_share = comp[g] / cg_total
                    take = min(amt * g_share, comp[g])
                    comp[g] = max(0.0, comp[g] - take)

    def _deposit_to_buildings(self, stop_idx: int, manifest: dict) -> None:
        neighbors = self._stop_neighbors.get(stop_idx)
        if not neighbors:
            return
        N_total = sum(self._buildings[bi].N for bi in neighbors)
        if N_total <= 0:
            # Empty cluster fallback: dump everything into the first neighbor as adults
            if neighbors:
                b = self._buildings[neighbors[0]]
                for compartment_name in ("S", "E", "I", "R", "D"):
                    getattr(b, compartment_name)["adult"] += manifest[compartment_name]
            return
        for bi in neighbors:
            b = self._buildings[bi]
            if b.N <= 0:
                continue
            share = b.N / N_total
            af = b.age_frac
            for compartment_name in ("S", "E", "I", "R", "D"):
                amt = manifest[compartment_name] * share
                if amt <= 0:
                    continue
                comp = getattr(b, compartment_name)
                for g in ("child", "adult", "elder"):
                    comp[g] = max(0.0, comp[g] + amt * af.get(g, 0.0))

    def _apply_screening(self, manifest: dict, src_stop: int,
                         active: frozenset) -> None:
        """Station screening: detect ~40 % of infectious passengers before
        boarding and return them to the origin neighbourhood.  The caught
        fraction was already removed from buildings by ``_remove_from_buildings``
        so we deposit it back."""
        if "transit_screening" not in active:
            return
        caught = manifest["I"] * 0.40
        if caught <= 0:
            return
        manifest["I"] -= caught
        manifest["N"] -= caught
        # Return the caught infectious back to origin buildings.
        self._deposit_to_buildings(src_stop, {
            "S": 0.0, "E": 0.0, "I": caught, "R": 0.0, "D": 0.0,
        })

    def _in_vehicle_step(self, manifest: dict, dt_hours: float,
                         vehicle_type: str = "bus", capacity: float = 1.0,
                         active: frozenset = frozenset()) -> None:
        N = max(manifest["N"], 1.0)
        S_v = manifest["S"]
        I_v = manifest["I"]
        if S_v <= 0 or I_v <= 0 or dt_hours <= 0:
            return
        beta = TRANSPORT_BETA_PER_HOUR.get(vehicle_type, _DEFAULT_TRANSPORT_BETA)
        # Wells-Riley crowding factor: denser occupancy → higher contact rate.
        # 30 % baseline (shared air even when sparse) scaling to 100 % at full.
        occupancy = manifest["N"] / max(capacity, 1.0)
        beta *= 0.3 + 0.7 * min(1.0, occupancy)
        # Transit mask intervention halves in-vehicle transmission.
        if "transit_masks" in active:
            beta *= 0.50
        dE = beta * dt_hours * S_v * I_v / N
        dE = min(dE, S_v)
        manifest["S"] = max(0.0, S_v - dE)
        manifest["E"] = manifest["E"] + dE


# ── Standalone CLI: regenerate schedule for one city ─────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python transport.py <city_id>")
        sys.exit(1)
    generate_for_city(sys.argv[1])
