#!/usr/bin/env python3
"""Bake a compact public-transport network for FAVE's transit accessibility mode.

Reads the EpiCity transit data that now lives under
  frontend/assets/data/cities/<key>/epicity/
    - transport_infra.json   (stops: idx, lat, lon, type)
    - transport_schedule.json (lines: ordered stop sequences; trips: for headway)

and writes
  frontend/assets/data/cities/<key>/transit/network.json
    { city, version, n, stops:[[lon,lat]...], wait_min:[...],
      time_min:[[...]...]  # NxN in-vehicle minutes, null = unreachable }

Travel-time model (offline, no external APIs — honors CLAUDE.md hard rule):
  edge(a,b) on a line = haversine(a,b) / in-vehicle speed[type] + dwell
  stop->stop time     = Dijkstra shortest path over the merged line graph
  wait_min[stop]      = headway/2 of the most frequent line serving the stop
Runtime (fairness.js) adds access/egress walk + wait at boarding.

Usage:
  python tools/bake_transit_network.py                 # all 7 cities
  python tools/bake_transit_network.py --cities vaxjo  # subset
"""
from __future__ import annotations
import argparse
import datetime as _dt
import heapq
import json
import math
import os
from pathlib import Path

CITIES = ["vaxjo", "malmo", "goteborg", "stockholm", "kalmar", "norrkoping", "uppsala"]

# In-vehicle speeds (km/h), realistic urban averages including acceleration.
SPEED_KMH = {
    "bus": 22.0, "tram": 24.0, "train": 60.0, "rail": 60.0,
    "intercity_train": 80.0, "ferry": 18.0, "subway": 32.0, "metro": 32.0,
}
DEFAULT_SPEED_KMH = 22.0
DWELL_MIN = 0.4              # per-stop dwell time added to each edge
SERVICE_SPAN_MIN = 18 * 60  # assumed daily service window if not derivable
MAX_WAIT_MIN = 20.0         # cap boarding wait (very infrequent lines)
MIN_WAIT_MIN = 2.0
# Walking-transfer edges: EpiCity stops are not deduped by location (one idx per
# line/platform), so co-located stops must be linked to enable transfers and to
# connect the network at hubs. Link stops within this radius by a short walk.
TRANSFER_RADIUS_M = 150.0
TRANSFER_WALK_KMH = 5.0
TRANSFER_PENALTY_MIN = 1.0  # boarding/penalty for changing vehicle

DATA_ROOT = Path(__file__).resolve().parents[1] / "frontend" / "assets" / "data" / "cities"


def haversine_m(lon1, lat1, lon2, lat2):
    R = 6371000.0
    rl1, rl2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rl1) * math.cos(rl2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def speed_for(t):
    return SPEED_KMH.get(str(t or "").lower(), DEFAULT_SPEED_KMH)


def build_city(key: str) -> dict | None:
    cdir = DATA_ROOT / key / "epicity"
    infra_p = cdir / "transport_infra.json"
    sched_p = cdir / "transport_schedule.json"
    if not infra_p.exists() or not sched_p.exists():
        print(f"  [{key}] SKIP — missing transport_infra/schedule")
        return None

    infra = json.loads(infra_p.read_text(encoding="utf-8"))
    sched = json.loads(sched_p.read_text(encoding="utf-8"))
    raw_stops = infra.get("stops", [])
    lines = sched.get("lines", [])
    trips = sched.get("trips", [])

    # Keep only stops with valid lat/lon; remap to a dense 0..n-1 index.
    old2new, stops_lonlat, stop_type = {}, [], []
    for s in raw_stops:
        lat, lon = s.get("lat"), s.get("lon")
        if lat is None or lon is None:
            continue
        old2new[s["idx"]] = len(stops_lonlat)
        stops_lonlat.append([round(float(lon), 6), round(float(lat), 6)])
        stop_type.append(s.get("type"))
    n = len(stops_lonlat)
    if n == 0:
        print(f"  [{key}] SKIP — no geolocated stops")
        return None

    # Adjacency from consecutive stops on each line.
    adj = [[] for _ in range(n)]
    serving = [[] for _ in range(n)]  # line indices serving each stop
    for li, line in enumerate(lines):
        seq = [old2new[o] for o in line.get("stops", []) if o in old2new]
        ltype = line.get("type")
        sp = speed_for(ltype)
        for a, b in zip(seq, seq[1:]):
            if a == b:
                continue
            lon1, lat1 = stops_lonlat[a]
            lon2, lat2 = stops_lonlat[b]
            d_km = haversine_m(lon1, lat1, lon2, lat2) / 1000.0
            w = (d_km / max(1e-6, sp)) * 60.0 + DWELL_MIN  # minutes
            adj[a].append((b, w))
            adj[b].append((a, w))
        for sidx in seq:
            serving[sidx].append(li)

    # Walking-transfer edges between nearby stops (connects co-located hubs).
    transfer_edges = 0
    for i in range(n):
        lon1, lat1 = stops_lonlat[i]
        for j in range(i + 1, n):
            lon2, lat2 = stops_lonlat[j]
            # cheap bbox reject before haversine (~0.0014 deg ~ 150m lat)
            if abs(lat1 - lat2) > 0.0016 or abs(lon1 - lon2) > 0.0030:
                continue
            d = haversine_m(lon1, lat1, lon2, lat2)
            if d <= TRANSFER_RADIUS_M:
                w = (d / 1000.0 / TRANSFER_WALK_KMH) * 60.0 + TRANSFER_PENALTY_MIN
                adj[i].append((j, w))
                adj[j].append((i, w))
                transfer_edges += 1

    # Headway -> wait per line, then per stop take the most frequent (min wait).
    trips_per_line: dict = {}
    for t in trips:
        lid = t.get("line_id")
        trips_per_line[lid] = trips_per_line.get(lid, 0) + 1
    line_wait = []
    for line in lines:
        cnt = trips_per_line.get(line.get("id"), 0)
        if cnt <= 0:
            line_wait.append(MAX_WAIT_MIN)
        else:
            headway = SERVICE_SPAN_MIN / cnt
            line_wait.append(min(MAX_WAIT_MIN, max(MIN_WAIT_MIN, headway / 2.0)))
    wait_min = []
    for sidx in range(n):
        ws = [line_wait[li] for li in serving[sidx]] if serving[sidx] else [MAX_WAIT_MIN]
        wait_min.append(round(min(ws), 2))

    # All-pairs shortest in-vehicle time via Dijkstra from each node.
    INF = float("inf")
    time_min = []
    for src in range(n):
        dist = [INF] * n
        dist[src] = 0.0
        pq = [(0.0, src)]
        while pq:
            d, u = heapq.heappop(pq)
            if d > dist[u]:
                continue
            for v, w in adj[u]:
                nd = d + w
                if nd < dist[v]:
                    dist[v] = nd
                    heapq.heappush(pq, (nd, v))
        time_min.append([None if x == INF else round(x, 1) for x in dist])

    reachable = sum(1 for row in time_min for x in row if x is not None)
    return {
        "city": key,
        "version": 1,
        "baked_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "n": n,
        "stops": stops_lonlat,
        "wait_min": wait_min,
        "time_min": time_min,
        "model": {
            "speed_kmh": SPEED_KMH, "default_speed_kmh": DEFAULT_SPEED_KMH,
            "dwell_min": DWELL_MIN, "service_span_min": SERVICE_SPAN_MIN,
        },
        "stats": {
            "stops": n, "lines": len(lines), "trips": len(trips),
            "transfer_edges": transfer_edges,
            "reachable_pairs": reachable, "pair_density": round(reachable / (n * n), 3),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cities", nargs="*", default=CITIES)
    args = ap.parse_args()
    for key in args.cities:
        print(f"[{key}] baking transit network...")
        net = build_city(key)
        if net is None:
            continue
        out_dir = DATA_ROOT / key / "transit"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_p = out_dir / "network.json"
        out_p.write_text(json.dumps(net, ensure_ascii=False), encoding="utf-8")
        size_mb = out_p.stat().st_size / 1e6
        st = net["stats"]
        print(f"  [{key}] -> {out_p.relative_to(DATA_ROOT.parents[2])}  "
              f"({size_mb:.1f} MB; {st['stops']} stops, {st['lines']} lines, "
              f"{st['trips']} trips, {int(st['pair_density']*100)}% pairs reachable)")


if __name__ == "__main__":
    main()
