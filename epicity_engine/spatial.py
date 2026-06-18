"""
spatial.py — Map-driven spatial interventions for EpiCity.

The five tools live here:

    quarantine_zone   — sealed perimeter around a neighbourhood / polygon
    targeted_vaccine  — concentrate vaccine effort in a chosen neighbourhood
    mobile_clinic     — temporary local healthcare-capacity boost
    surge_testing     — accelerate isolation in a building + its neighbours
    (lasso shares the quarantine_zone effect; just a different selection UI)

Each is stored as a `SpatialIntervention` instance in `SpatialRegistry`,
keyed by a server-generated id. The engine reads the registry on every
step to compute per-building modifications:

    quarantine_set()  → β × 0.2, neighbour edges zeroed across the boundary
    vacc_boost_set()  → 3× vaccination rate inside, 0.33× outside
    clinic_set()      → use baseline μ (not μ_overloaded) for these buildings
    surge_set()       → γ × 2

Sandbox AND game modes both support spatial interventions. Game-mode
costs are charged via the existing per-day intervention impact path —
the registry exposes a `daily_costs()` helper that game.py adds to the
bar drains.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any


# Per-tool effect strengths — calibrated alongside the citywide ones in
# game.py / engine.py so the spatial alternatives feel meaningful but
# not strictly better than their citywide counterparts.

QUARANTINE_BETA_MULT     = 0.20    # β inside a quarantine zone
VACC_BOOST_TARGET_MULT   = 3.0     # vaccination rate inside a targeted area
VACC_BOOST_BACKGROUND    = 0.33    # vaccination rate everywhere else
SURGE_GAMMA_MULT         = 2.0     # γ multiplier on surge-tested buildings
CLINIC_RADIUS_M          = 500.0   # mobile-clinic effective radius
QUARANTINE_BUILDING_LIMIT = 4000    # safety cap so a city-wide accidental
                                   # selection doesn't lock everything


# Per-day game-mode costs *per active spatial intervention*. Scales by
# the size of the affected population so a tiny clinic doesn't cost the
# same as quarantining downtown.

PER_DAY_COSTS: dict[str, dict[str, float]] = {
    # economy / morale / trust drains scale with affected pop fraction
    "quarantine_zone":  {"economy_per_kpop": -0.15, "morale_per_kpop": -0.10,
                         "trust_on_create":  -3.0,  "budget_per_kpop":  0.0},
    "targeted_vaccine": {"economy_per_kpop":  0.00, "morale_per_kpop":  0.05,
                         "trust_on_create":   0.0,  "budget_per_kpop": -800.0},
    "mobile_clinic":    {"economy_per_kpop":  0.00, "morale_per_kpop":  0.05,
                         "trust_on_create":  +1.0,  "budget_per_kpop": -1500.0},
    "surge_testing":    {"economy_per_kpop":  0.00, "morale_per_kpop":  0.02,
                         "trust_on_create":  +0.5,  "budget_per_kpop": -200.0},
}


VALID_TYPES = frozenset(PER_DAY_COSTS)


@dataclass
class SpatialIntervention:
    id:               str
    type:             str
    target_buildings: list[int]
    created_day:      int
    duration_days:    int           # -1 → no auto-expiry
    population:       int           # for cost scaling + UI labels
    label:            str           # English fallback ("Mobile clinic · 500 m")
    # i18n-aware label: the frontend formats `t(label_key, label_vars)`
    # so the active-zones list flips language live. The plain `label`
    # above is kept as a fallback for non-i18n consumers / API clients.
    label_key:        str  = ""     # e.g. "sp_lbl_clinic"
    label_vars:       dict = field(default_factory=dict)
    # Optional polygon ring(s) in lon/lat — used by the frontend to draw
    # the animated red border for quarantine zones. For lasso this is the
    # user-drawn polygon; for neighborhood quarantine it's the RegSO union.
    # `None` for non-quarantine items (mobile clinic / surge testing).
    polygon:          list | None = None
    # Optional center point in lon/lat — for mobile_clinic so the
    # frontend can spawn / track the ambulance asset.
    center_lonlat:    list | None = None

    def expires_on(self) -> int | None:
        if self.duration_days < 0:
            return None
        return self.created_day + self.duration_days

    def snapshot(self, today: int) -> dict[str, Any]:
        exp = self.expires_on()
        return {
            "id":              self.id,
            "type":            self.type,
            "label":           self.label,
            "label_key":       self.label_key,
            "label_vars":      self.label_vars,
            "buildings":       len(self.target_buildings),
            "population":      self.population,
            "created_day":     self.created_day,
            "duration_days":   self.duration_days,
            "expires_on":      exp,
            "days_remaining":  None if exp is None else max(0, exp - today),
            "polygon":         self.polygon,
            "center_lonlat":   self.center_lonlat,
        }


class SpatialRegistry:
    """In-memory registry attached to each EpidemicEngine."""

    def __init__(self) -> None:
        self._items: dict[str, SpatialIntervention] = {}
        # Cached unions over all active items. Invalidated on add/remove.
        self._cache: dict[str, set[int]] = {}

    # ── Mutations ────────────────────────────────────────────────────────────

    def add(self, *, type: str, target_buildings: list[int],
            label: str, population: int, created_day: int,
            duration_days: int = -1,
            polygon: list | None = None,
            center_lonlat: list | None = None,
            label_key: str = "",
            label_vars: dict | None = None) -> SpatialIntervention:
        if type not in VALID_TYPES:
            raise ValueError(f"Unknown spatial type: {type!r}")
        if not target_buildings:
            raise ValueError("target_buildings empty")
        # Safety cap — protect against runaway selections.
        if (type == "quarantine_zone"
                and len(target_buildings) > QUARANTINE_BUILDING_LIMIT):
            target_buildings = target_buildings[:QUARANTINE_BUILDING_LIMIT]

        item = SpatialIntervention(
            id=uuid.uuid4().hex[:10],
            type=type,
            target_buildings=list(target_buildings),
            created_day=created_day,
            duration_days=duration_days,
            population=int(population),
            label=label,
            label_key=label_key,
            label_vars=label_vars or {},
            polygon=polygon,
            center_lonlat=center_lonlat,
        )
        self._items[item.id] = item
        self._cache.clear()
        return item

    def remove(self, id: str) -> bool:
        if id in self._items:
            del self._items[id]
            self._cache.clear()
            return True
        return False

    def clear(self) -> None:
        if self._items:
            self._items.clear()
            self._cache.clear()

    def expire(self, today: int) -> list[str]:
        """Auto-remove items whose duration has elapsed."""
        expired: list[str] = []
        for it in list(self._items.values()):
            exp = it.expires_on()
            if exp is not None and today >= exp:
                expired.append(it.id)
                del self._items[it.id]
        if expired:
            self._cache.clear()
        return expired

    # ── Queries ──────────────────────────────────────────────────────────────

    def items(self) -> list[SpatialIntervention]:
        return list(self._items.values())

    def by_type(self, type: str) -> list[SpatialIntervention]:
        return [it for it in self._items.values() if it.type == type]

    def quarantine_set(self) -> set[int]:
        if "quarantine_set" not in self._cache:
            s: set[int] = set()
            for it in self._items.values():
                if it.type == "quarantine_zone":
                    s.update(it.target_buildings)
            self._cache["quarantine_set"] = s
        return self._cache["quarantine_set"]

    def vacc_boost_set(self) -> set[int]:
        if "vacc_boost_set" not in self._cache:
            s: set[int] = set()
            for it in self._items.values():
                if it.type == "targeted_vaccine":
                    s.update(it.target_buildings)
            self._cache["vacc_boost_set"] = s
        return self._cache["vacc_boost_set"]

    def clinic_set(self) -> set[int]:
        if "clinic_set" not in self._cache:
            s: set[int] = set()
            for it in self._items.values():
                if it.type == "mobile_clinic":
                    s.update(it.target_buildings)
            self._cache["clinic_set"] = s
        return self._cache["clinic_set"]

    def surge_set(self) -> set[int]:
        if "surge_set" not in self._cache:
            s: set[int] = set()
            for it in self._items.values():
                if it.type == "surge_testing":
                    s.update(it.target_buildings)
            self._cache["surge_set"] = s
        return self._cache["surge_set"]

    def has_any_vacc_boost(self) -> bool:
        return any(it.type == "targeted_vaccine" for it in self._items.values())

    # ── Game-mode cost integration ──────────────────────────────────────────

    def daily_costs(self) -> dict[str, float]:
        """
        Sum the per-day economy/morale/budget drains across all active
        spatial interventions, scaled by their affected population.
        Returns dict with keys: economy, morale, budget.
        """
        out = {"economy": 0.0, "morale": 0.0, "budget": 0.0}
        for it in self._items.values():
            cost = PER_DAY_COSTS.get(it.type, {})
            kpop = it.population / 1000.0
            out["economy"] += cost.get("economy_per_kpop", 0.0) * kpop
            out["morale"]  += cost.get("morale_per_kpop",  0.0) * kpop
            out["budget"]  += cost.get("budget_per_kpop",  0.0) * kpop
        return out

    def snapshot(self, today: int) -> list[dict]:
        return [it.snapshot(today) for it in self._items.values()]
