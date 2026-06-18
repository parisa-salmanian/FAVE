"""
metapop.py — Coarse metapopulation layer wrapping the focused-city engine.

The focused city (whichever city the user is looking at in the 3D view) runs
the full per-building SEIR engine unchanged. Every *other* Swedish kommun is
represented by a single coarse SEIR node — scalar S/E/I/R/D totals, no
spatial resolution — and all 290 nodes (290 kommuner in Sweden) are coupled
through the commuter-flow matrix fetched by scb.load_commuter_flows().

Scale check: 290 nodes × a handful of scalars per step is nothing. The whole
coarse layer is a couple of milliseconds per day. The focused engine is the
only expensive piece, and this module does not touch its internals — it only
feeds one extra scalar (``external_foi``) into its existing ``step()``.

Design:

    ┌─────────────────┐         ┌───────────────────────┐
    │ focused city    │  ext    │ MetapopulationLayer   │
    │ (EpidemicEngine)│ ←──FOI──│  ─ coarse nodes       │
    │                 │         │  ─ commuter flows     │
    │ per-building    │  totals │  ─ focused "shadow"   │
    │ SEIR            │ ──────→ │                       │
    └─────────────────┘         └───────────────────────┘

Each daily tick:

    1. Ask the layer: "what external FOI should the focused city see today?"
       → sum over source kommuner of (flow × I_src / N_src × β / N_focused).
    2. Step the focused engine with that scalar (single-city math otherwise
       unchanged, bit-identical when the layer is empty / disabled).
    3. Pull total S/E/I/R/D back from the focused engine into the layer's
       "shadow" coarse node for the focused kommun, so other kommuner see
       the current state of the focused city in tomorrow's FOI.
    4. Advance every *other* coarse node one day with vanilla SEIR using
       its own external FOI term.

The layer is a module-level singleton — the coarse node pool is global
(same 290 kommuner regardless of which city is focused), so there's no
reason to pay the cost of rebuilding it when the user switches cities.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any

import scb


# ── Parameters ────────────────────────────────────────────────────────────────

# How strongly commuter flows translate into cross-kommun infection pressure.
# A value of 1.0 means "one infected commuter in the destination kommun
# contributes exactly as much force-of-infection as one infected resident".
# Real commuting is partial-day mixing, so a value in [0.3, 0.7] is more
# realistic. Tunable — start conservative.
COUPLING_STRENGTH = 0.5

# Base β for coarse nodes. The focused engine has its own β (including
# intervention multipliers); for coarse nodes we use a single fixed value
# roughly matching the focused city's default. Coarse nodes don't participate
# in user interventions in this first pass.
COARSE_BETA = 0.32
COARSE_SIGMA = 0.20    # E → I
COARSE_GAMMA = 0.10    # I → R
COARSE_MU    = 0.008   # I → D fraction of recovered


@dataclass
class CoarseNode:
    """
    A single-kommun scalar SEIR compartment model. No spatial structure,
    no age stratification — these are the "other cities" that exist only
    to carry commuter-linked pressure in and out of the focused city.
    """
    kommunkod: str
    name:      str
    pop:       float
    lat:       float
    lon:       float
    S: float
    E: float = 0.0
    I: float = 0.0
    R: float = 0.0
    D: float = 0.0

    @property
    def N(self) -> float:
        # Dead are excluded from the denominator of FOI to match the
        # focused engine's convention (see Cell.N in engine.py).
        return self.S + self.E + self.I + self.R

    def step(self, external_foi: float) -> None:
        """Vanilla SEIR advance by one day with an extra additive FOI."""
        if self.N <= 0:
            return
        # Intra-kommun FOI: β × I/N. External FOI is added on top.
        local_lambda = COARSE_BETA * (self.I / self.N)
        lam = max(0.0, local_lambda + external_foi)

        dE = min(lam * self.S, self.S)
        dI = min(COARSE_SIGMA * self.E, self.E)
        dR = min((1.0 - COARSE_MU) * COARSE_GAMMA * self.I, self.I)
        dD = min(COARSE_MU        * COARSE_GAMMA * self.I, max(0.0, self.I - dR))

        self.S = max(0.0, self.S - dE)
        self.E = max(0.0, self.E + dE - dI)
        self.I = max(0.0, self.I + dI - dR - dD)
        self.R = max(0.0, self.R + dR)
        self.D = max(0.0, self.D + dD)


class MetapopulationLayer:
    """
    Module-level singleton that owns the coarse-node pool and commuter
    matrix. Lazily loaded on first use — a cold start with cold caches
    will trigger scb.load_national() and pull everything from SCB, which
    only happens once per environment.
    """

    def __init__(self) -> None:
        self._nodes:   dict[str, CoarseNode]         = {}
        self._flows:   dict[str, dict[str, int]]     = {}
        # For every kommun k: sum of flows landing in k (denominator of the
        # "fraction of workplace population from source i" factor).
        self._inflow_totals: dict[str, float]        = {}
        self._focused: str | None                    = None
        self._day:     int                           = 0
        self._loaded:  bool                          = False

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def ensure_loaded(self) -> bool:
        """
        Build the coarse-node pool from cached SCB data if we haven't
        already. Returns True on success, False if the nationwide data is
        unavailable (in which case the layer goes into a no-op passthrough
        mode and the focused engine runs exactly as before).
        """
        if self._loaded:
            return bool(self._nodes)

        try:
            bundle = scb.load_national()
        except Exception as e:
            print(f"[metapop] load_national FAILED: {e}")
            self._loaded = True
            return False

        kommuner  = bundle.get("kommuner")  or {}
        centroids = bundle.get("centroids") or {}
        flows     = bundle.get("flows")     or {}

        if not kommuner:
            print("[metapop] nationwide kommuner data missing — layer disabled")
            self._loaded = True
            return False

        self._nodes = {}
        for kommun, meta in kommuner.items():
            cent = centroids.get(kommun) or {}
            pop  = float(meta.get("total") or 0)
            self._nodes[kommun] = CoarseNode(
                kommunkod=kommun,
                name=meta.get("name") or cent.get("name") or kommun,
                pop=pop,
                lat=float(cent.get("lat") or 0.0),
                lon=float(cent.get("lon") or 0.0),
                S=pop,
            )

        self._flows = flows
        self._recompute_inflow_totals()
        self._loaded = True
        print(f"[metapop] loaded {len(self._nodes)} coarse nodes, "
              f"{sum(len(d) for d in self._flows.values())} OD pairs")
        return True

    def _recompute_inflow_totals(self) -> None:
        totals: dict[str, float] = {}
        for src, dsts in self._flows.items():
            for dst, n in dsts.items():
                totals[dst] = totals.get(dst, 0.0) + float(n)
        self._inflow_totals = totals

    def set_focused(self, kommunkod: str | None) -> None:
        """Mark a kommun as the focused city. Pass None to detach."""
        self._focused = kommunkod

    def is_available(self) -> bool:
        return bool(self._nodes)

    # ── Coupling math ────────────────────────────────────────────────────────

    def external_foi_for(self, kommunkod: str) -> float:
        """
        Compute today's external force-of-infection landing on `kommunkod`
        from all *other* kommuner via the commuter flow matrix.

        Formula (frequency-dependent, daily-scale):

            λ_ext(k) = COUPLING_STRENGTH × COARSE_BETA × Σ_i (
                          F_i→k / N_k_work × I_i / N_i
                       )

        where F_i→k is the commuter flow from source i to destination k,
        and N_k_work is the total workplace population at k (sum of incoming
        flows). Self-flows (i == k) are excluded — those are already
        captured by the destination's own local FOI.
        """
        if not self._nodes:
            return 0.0
        dst_node = self._nodes.get(kommunkod)
        if dst_node is None or dst_node.N <= 0:
            return 0.0

        work_pop = self._inflow_totals.get(kommunkod, 0.0)
        if work_pop <= 0:
            return 0.0

        acc = 0.0
        for src_kommun, src_node in self._nodes.items():
            if src_kommun == kommunkod:
                continue
            if src_node.N <= 0 or src_node.I <= 0:
                continue
            flow = self._flows.get(src_kommun, {}).get(kommunkod, 0)
            if flow <= 0:
                continue
            acc += (flow / work_pop) * (src_node.I / src_node.N)

        return COUPLING_STRENGTH * COARSE_BETA * acc

    # ── Discrete inter-city events (transport.py hook) ───────────────────────

    def apply_discrete_arrival(self, dst_kommun: str, n: float,
                               src_I_frac: float) -> None:
        """
        Deposit ``n`` passengers into ``dst_kommun``'s coarse SEIR pool, with
        ``src_I_frac`` of them already infectious. Used by the transport
        dispatcher when an inter-city flight or long-distance train *arrives*
        at this kommun.

        This is a discrete event, NOT a continuous FOI term — do not fold it
        into ``external_foi_for()``: that function is stateless and recomputed
        from current state every call, so adding a transport term there would
        either double-count every tick or require breaking that purity.

        Math: ``n × src_I_frac`` move from S → I in the destination node.
        Susceptible passengers ride along but their boarding doesn't change
        ``S_dst`` (they came from outside the kommun pool but the coarse
        node abstraction treats them as part of the local population on
        arrival). Net effect: ``S -= n × I_frac;  I += n × I_frac``.
        """
        node = self._nodes.get(dst_kommun)
        if node is None or n <= 0 or src_I_frac <= 0:
            return
        i_part = n * float(src_I_frac)
        i_part = min(i_part, node.S)
        if i_part <= 0:
            return
        node.S = max(0.0, node.S - i_part)
        node.I = node.I + i_part

    def apply_discrete_departure(self, src_kommun: str, n: float) -> None:
        """
        Remove ``n`` passengers from ``src_kommun``'s coarse SEIR pool when
        an inter-city vehicle *departs*. Removed proportionally across S/E/I/R
        so the source node's compartment ratios are preserved. ``D`` is not
        touched (dead don't board planes).

        Caps the removal at the source node's remaining living population
        so we never go negative.
        """
        node = self._nodes.get(src_kommun)
        if node is None or n <= 0:
            return
        live = node.S + node.E + node.I + node.R
        if live <= 0:
            return
        take = min(n, live)
        node.S = max(0.0, node.S - take * (node.S / live))
        node.E = max(0.0, node.E - take * (node.E / live))
        node.I = max(0.0, node.I - take * (node.I / live))
        node.R = max(0.0, node.R - take * (node.R / live))

    # ── Per-day sync from focused engine ─────────────────────────────────────

    def sync_focused_totals(self, S: float, E: float, I: float,
                            R: float, D: float) -> None:
        """
        Copy the focused city's current totals into its shadow coarse node
        so other kommuner see the current state of the focused city on the
        next day's external-FOI computation. Called right after the focused
        engine's step() completes.
        """
        if not self._focused:
            return
        node = self._nodes.get(self._focused)
        if node is None:
            return
        node.S = max(0.0, float(S))
        node.E = max(0.0, float(E))
        node.I = max(0.0, float(I))
        node.R = max(0.0, float(R))
        node.D = max(0.0, float(D))

    # ── Daily tick ───────────────────────────────────────────────────────────

    def step_coarse_nodes(self) -> None:
        """
        Advance every coarse node by one day, skipping the focused kommun
        (which is driven by the focused engine). Each node's external FOI
        is computed from the current layer state, so we snapshot external
        FOIs for all nodes *first*, then apply them, so within-day ordering
        doesn't matter.
        """
        if not self._nodes:
            return
        # Snapshot external FOIs so they all see the same "current" state.
        ext_foi: dict[str, float] = {}
        for k in self._nodes:
            if k == self._focused:
                continue
            ext_foi[k] = self.external_foi_for(k)

        for k, node in self._nodes.items():
            if k == self._focused:
                continue
            node.step(ext_foi.get(k, 0.0))

        self._day += 1

    # ── Seeding / reset ──────────────────────────────────────────────────────

    def reset_all(self) -> None:
        """Reset every coarse node to fully susceptible (S = pop, everything else 0)."""
        for node in self._nodes.values():
            node.S = node.pop
            node.E = node.I = node.R = node.D = 0.0
        self._day = 0

    def seed_infection(self, kommunkod: str, n_infected: float = 10.0) -> bool:
        """
        Place ``n_infected`` exposed individuals in a specific kommun. Useful
        for testing cross-kommun spread without having to wait for the focused
        city to export infections via the commuter matrix.
        """
        node = self._nodes.get(kommunkod)
        if node is None or node.S <= 0:
            return False
        n = min(n_infected, node.S)
        node.S -= n
        node.E += n
        return True

    # ── State snapshot for the frontend / API ───────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        """
        Return a JSON-serialisable snapshot of every coarse node. Shape:

            {
              "day":   int,
              "focused": "0180" | None,
              "nodes": {
                "0180": {"name": ..., "lat": ..., "lon": ..., "pop": ...,
                          "S": ..., "E": ..., "I": ..., "R": ..., "D": ...},
                ...
              }
            }
        """
        return {
            "day":     self._day,
            "focused": self._focused,
            "nodes": {
                k: {
                    "name": n.name,
                    "lat":  n.lat,
                    "lon":  n.lon,
                    "pop":  n.pop,
                    "S":    n.S,
                    "E":    n.E,
                    "I":    n.I,
                    "R":    n.R,
                    "D":    n.D,
                }
                for k, n in self._nodes.items()
            },
        }


# Module-level singleton. Import-and-use.
metapop = MetapopulationLayer()
