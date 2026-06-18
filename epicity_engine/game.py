"""
game.py — Gamified scenario layer for EpiCity.

When a city is entered with mode="game", the engine carries a GameState
that ticks each simulated day to update six resources:

    economy, morale, trust, healthcare_load, budget, deaths_total

The cumulative score is a weighted sum of resource trajectories; the user
plays a fixed 180-day scenario and is given a final cost-of-pandemic
score. There is no win/loss — just a number to minimize.

Sandbox mode is unaffected: GameState is never instantiated, the engine
runs identically to before, and main.py simply does not call .tick().

Three new interventions live here so the table is co-located with the
costs they incur:

    info_campaign      — β -10%, +trust, +morale, small daily budget
    hospital_surge     — capacity +40%, no β effect, large budget
    targeted_lockdown  — β -35%, ~⅓ economy/morale cost of full lockdown

Difficulty profiles only override the *base* SEIR params (β, μ) and
auto-seed parameters (number of patient-zero buildings, vaccine
availability day, starting budget). The rest of the engine is untouched.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ── Difficulty profiles ──────────────────────────────────────────────────────

DIFFICULTY_PROFILES: dict[str, dict] = {
    "easy": {
        "beta":             0.25,
        "mu":               0.004,
        # Single seed in every difficulty — the user prefers the "one
        # outbreak origin" feel of sandbox. Difficulty differentiates
        # via β, μ, vaccine arrival day, and resource-drain scale.
        "patient_zero_n":   1,
        "starting_budget":  1_500_000.0,
        "vaccine_day":      30,
        "death_cap_frac":   0.003,
        "drain_scale":      0.7,
    },
    "normal": {
        "beta":             0.35,
        "mu":               0.008,
        "patient_zero_n":   1,
        "starting_budget":  1_000_000.0,
        "vaccine_day":      60,
        "death_cap_frac":   0.007,
        "drain_scale":      1.0,
    },
    "hard": {
        "beta":             0.50,
        "mu":               0.015,
        "patient_zero_n":   1,
        "starting_budget":  600_000.0,
        "vaccine_day":      90,
        "death_cap_frac":   0.015,
        "drain_scale":      1.5,
    },
}

VALID_DIFFICULTIES = frozenset(DIFFICULTY_PROFILES)
GAME_DURATION_DAYS = 180


# ── New interventions added when game mode is on ─────────────────────────────
# Sandbox mode also exposes them — they're useful tools regardless. The cost
# table below only has teeth when GameState is ticking.

NEW_INTERVENTIONS_META: list[dict[str, str]] = [
    {"id": "info_campaign", "label": "Public Information", "icon": "📣",
     "desc": "Awareness campaign — β -10%, +trust, +morale"},
    {"id": "hospital_surge", "label": "Hospital Surge", "icon": "🏥",
     "desc": "Temporary +40% hospital capacity (mitigates overflow deaths)"},
    {"id": "targeted_lockdown", "label": "Targeted Lockdown", "icon": "🚧",
     "desc": "Hot-zone restriction — β -35% at ~⅓ the economic/social cost"},
]


# ── Resource-impact table ────────────────────────────────────────────────────
# Per-day drains/gains while an intervention is active, plus a one-time trust
# cost when toggling it on. Budget per day is in SEK.

INTERVENTION_IMPACT: dict[str, dict[str, float]] = {
    "lockdown":          {"economy": -2.0,  "morale": -1.5,  "trust_toggle": -10, "budget": 0.0},
    "masks":             {"economy":  0.0,  "morale": -0.2,  "trust_toggle":  -2, "budget": -200.0},
    "distancing":        {"economy": -0.5,  "morale": -0.5,  "trust_toggle":  -3, "budget": 0.0},
    "vaccination":       {"economy":  0.0,  "morale":  0.3,  "trust_toggle":   0, "budget": -8000.0,
                          "trust": 0.2},
    "schools":           {"economy": -0.8,  "morale": -0.7,  "trust_toggle":  -5, "budget": 0.0},
    "testing":           {"economy": -0.1,  "morale":  0.1,  "trust_toggle":   0, "budget": -3000.0,
                          "trust": 0.3},
    "transit_masks":     {"economy":  0.0,  "morale": -0.1,  "trust_toggle":  -1, "budget": -300.0},
    "suspend_flights":   {"economy": -1.0,  "morale": -0.3,  "trust_toggle":  -4, "budget": 0.0},
    "transit_screening": {"economy": -0.2,  "morale": -0.2,  "trust_toggle":   0, "budget": -2000.0,
                          "trust": 0.1},
    # New
    "info_campaign":     {"economy":  0.0,  "morale":  0.1,  "trust_toggle":   0, "budget": -500.0,
                          "trust": 0.3},
    "hospital_surge":    {"economy":  0.0,  "morale":  0.0,  "trust_toggle":   0, "budget": -10000.0},
    "targeted_lockdown": {"economy": -0.7,  "morale": -0.5,  "trust_toggle":  -4, "budget": 0.0},
    "stop_transport":    {"economy": -0.9,  "morale": -0.6,  "trust_toggle":  -5, "budget": 0.0},
}


# ── State ────────────────────────────────────────────────────────────────────

@dataclass
class GameState:
    """Resource bars + scoring state for game mode."""
    difficulty:        str   = "normal"
    duration_days:     int   = GAME_DURATION_DAYS

    # Bars (0–100 unless noted)
    economy:           float = 100.0
    morale:            float = 100.0
    trust:             float = 80.0
    healthcare_load:   float = 0.0      # % of hospital capacity (can exceed 100)
    budget:            float = 1_000_000.0    # SEK
    deaths_total:      int   = 0

    # Score (lower is better — total cost of the pandemic)
    cumulative_score:  float = 0.0

    # Bookkeeping for toggle-fatigue / stability bonuses
    last_toggle_day:   dict  = field(default_factory=dict)   # iv_id -> day
    toggle_count_14d:  dict  = field(default_factory=dict)   # iv_id -> [days]
    last_change_day:   int   = 0
    prev_active:       set   = field(default_factory=set)

    # Surge effect: temporary multiplier on hospital_capacity threshold
    hospital_surge_bonus: float = 0.0

    # Whether the scenario has hit duration_days (front-end shows summary)
    finished:          bool  = False

    # Final score breakdown — populated when finished=True
    final_breakdown:   Optional[dict] = None

    @classmethod
    def for_difficulty(cls, difficulty: str) -> "GameState":
        prof = DIFFICULTY_PROFILES.get(difficulty, DIFFICULTY_PROFILES["normal"])
        # Initial trust scales inversely with difficulty: a harder pandemic
        # comes with a more anxious public.
        trust0 = {"easy": 90.0, "normal": 75.0, "hard": 60.0}[
            difficulty if difficulty in DIFFICULTY_PROFILES else "normal"
        ]
        return cls(
            difficulty=difficulty,
            budget=prof["starting_budget"],
            trust=trust0,
        )

    @property
    def drain_scale(self) -> float:
        prof = DIFFICULTY_PROFILES.get(self.difficulty, DIFFICULTY_PROFILES["normal"])
        return float(prof.get("drain_scale", 1.0))

    @property
    def stability(self) -> float:
        """
        Composite headline reading (0–100). Equal-weighted average of
        Economy, Morale, Trust, and a healthcare-headroom score derived
        from the load bar:
            load   0 →  50%  → headroom 100
            load  50 → 100%  → headroom linearly 100 → 50  (warning)
            load 100 → 150%  → headroom linearly 50 → 0    (overflow)
            load >150%       → headroom 0
        This is what the player optimises in spirit — the score still
        scales with deaths separately so a 'stable but deadly' run is
        possible (and visibly bad on the score, even if Stability looks OK).
        """
        load = self.healthcare_load
        if load <= 50:
            head = 100.0
        elif load <= 100:
            head = 100.0 - (load - 50.0)            # 100 → 50
        elif load <= 150:
            head = 50.0  - (load - 100.0)           # 50 → 0
        else:
            head = 0.0
        head = max(0.0, min(100.0, head))
        return (self.economy + self.morale + self.trust + head) / 4.0

    @property
    def vaccine_day(self) -> int:
        """Sim-day on which vaccination becomes available."""
        prof = DIFFICULTY_PROFILES.get(self.difficulty, DIFFICULTY_PROFILES["normal"])
        return int(prof.get("vaccine_day", 60))

    def vaccine_unlocked(self, day: int) -> bool:
        return day >= self.vaccine_day

    def days_until_vaccine(self, day: int) -> int:
        return max(0, self.vaccine_day - int(day))

    # ── Tick ─────────────────────────────────────────────────────────────────

    def on_intervention_toggled(self, iv_id: str, active_now: bool, day: int) -> None:
        """Apply one-shot trust cost when a measure is turned ON; track flips."""
        impact = INTERVENTION_IMPACT.get(iv_id, {})
        if active_now:
            self.trust = max(0.0, self.trust + impact.get("trust_toggle", 0))
        # Toggle-fatigue: count flips per intervention in last 14 days
        hist = self.toggle_count_14d.setdefault(iv_id, [])
        hist.append(day)
        # Keep window
        self.toggle_count_14d[iv_id] = [d for d in hist if d >= day - 14]
        if len(self.toggle_count_14d[iv_id]) >= 4:   # 2 full on/off cycles
            self.trust = max(0.0, self.trust - 10)
        self.last_toggle_day[iv_id] = day
        self.last_change_day = day

    def tick(
        self,
        eng_state: dict,
        active_interventions: list[str],
        day: int,
        population: int,
        spatial_costs: dict | None = None,
    ) -> None:
        """Advance the game state by one simulated day."""
        if self.finished:
            return

        active = set(active_interventions)

        # 1) Sum per-day resource drains/gains from active interventions
        d_econ  = 0.0
        d_mor   = 0.0
        d_trust = 0.0
        d_bud   = 0.0
        for iv in active:
            imp = INTERVENTION_IMPACT.get(iv, {})
            d_econ  += imp.get("economy", 0.0)
            d_mor   += imp.get("morale",  0.0)
            d_trust += imp.get("trust",   0.0)
            d_bud   += imp.get("budget",  0.0)

        # 2) Hospital surge — apply while active. Bonus stays as long as the
        # intervention is on; clears the day it's turned off.
        self.hospital_surge_bonus = 0.40 if "hospital_surge" in active else 0.0

        # 3) Healthcare load — % of city pop currently infectious vs capacity.
        # Effective capacity = base 5% * (1 + surge_bonus). 100% load is
        # exactly at capacity; >100% means overflow.
        I_total = float(eng_state.get("I", 0.0))
        N       = max(1, int(population))
        cap_frac = 0.05 * (1.0 + self.hospital_surge_bonus)
        load_pct = (I_total / N) / cap_frac * 100.0 if cap_frac > 0 else 0.0
        self.healthcare_load = load_pct

        # 4) Passive drains from the epidemic itself.
        # Each channel is *capped* so a catastrophic day (10k deaths)
        # can't instant-zero a bar. The cumulative effect of multiple
        # channels still bites hard, but on a manageable timescale of
        # ~30 sim-days from full to empty under sustained inaction.
        # Round (don't truncate) so the HUD's deaths counter matches the
        # SEIRD KPI strip, which also rounds. `int()` was lopping off a
        # fractional death every tick, leaving the bar one behind.
        D_total = int(round(float(eng_state.get("D", 0))))
        new_deaths = max(0, D_total - self.deaths_total)
        self.deaths_total = D_total
        scale = self.drain_scale

        # Per-day new deaths — sublinear so a 1000-death day isn't 100x
        # worse than a 10-death day for the bar reading. Square root
        # tames the spike; the score still records the linear cost.
        deaths_per_k = (new_deaths / N) * 1000.0
        d_mor   -= min(1.5, 0.55 * (deaths_per_k ** 0.5)) * scale
        d_trust -= min(0.9, 0.35 * (deaths_per_k ** 0.5)) * scale

        # Cumulative death toll — slow grinding pressure that stops the
        # bars rebounding once enough damage is done.
        cum_per_k = (D_total / N) * 1000.0
        d_mor   -= min(0.4, 0.02 * cum_per_k) * scale
        d_trust -= min(0.3, 0.015 * cum_per_k) * scale

        # Visible infection prevalence drains morale, trust, and economy
        # (people self-isolate even without policy — supply chains
        # stutter, foot traffic dies). Capped low.
        prev = (I_total / N) * 100.0   # %
        d_mor   -= min(1.0, prev * 0.18) * scale
        d_trust -= min(0.6, prev * 0.10) * scale
        d_econ  -= min(0.7, prev * 0.08) * scale

        # Baseline "fear tax" while any active outbreak exists. Small but
        # always-on so bars don't sit at 100 just because cases are early.
        E_total = float(eng_state.get("E", 0.0))
        if (I_total + E_total) / N > 0.0005:    # >0.05% active
            d_mor   -= 0.15 * scale
            d_trust -= 0.10 * scale

        # Healthcare overflow: punishes letting the system collapse, but
        # bounded so it doesn't instantly zero everything in one bad day.
        if load_pct > 100.0:
            d_mor   -= 1.5 * scale
            d_trust -= 1.0 * scale
            d_econ  -= 0.6 * scale

        # 5) Stability bonus — only meaningful when there's nothing on
        # fire. No toggle in 7 days AND prevalence under 0.1% → trust
        # slowly recovers. Otherwise stability is just incompetence.
        stable = (day - self.last_change_day >= 7
                  and not active.symmetric_difference(self.prev_active))
        if stable and prev < 0.1:
            d_trust += 0.4

        # 6) Recovery — economy/morale only crawl back when the outbreak
        # is genuinely contained (low prevalence) AND no restrictive
        # measures are running. Active interventions aren't free even
        # if the player gets lucky on cases.
        restrictive = {"lockdown", "distancing", "schools", "suspend_flights",
                       "targeted_lockdown"}
        contained = prev < 0.05
        if not (active & restrictive) and contained:
            d_econ += 0.3
            d_mor  += 0.4

        # 7) Budget refilled slowly by economy (so a wrecked economy
        # eventually starves your medical budget). Easy mode tops up
        # a bit faster.
        budget_topup = {"easy": 700.0, "normal": 500.0, "hard": 350.0}.get(
            self.difficulty, 500.0)
        d_bud += budget_topup * (self.economy / 100.0)

        # 7b) Spatial-intervention drains (quarantine zones, mobile
        # clinics, targeted vaccination, surge testing). Population-scaled,
        # already computed by SpatialRegistry.daily_costs().
        if spatial_costs:
            d_econ += spatial_costs.get("economy", 0.0)
            d_mor  += spatial_costs.get("morale",  0.0)
            d_bud  += spatial_costs.get("budget",  0.0)

        # 8) Apply deltas (clamp 0..100 except budget which can go negative)
        self.economy = _clamp01(self.economy + d_econ)
        self.morale  = _clamp01(self.morale  + d_mor)
        self.trust   = _clamp01(self.trust   + d_trust)
        self.budget += d_bud
        # Negative budget pins emergency interventions off (frontend will gate)

        # 9) Score: per-day cost — higher = worse outcome
        # Components:
        #   - 0.5 per death (heaviest weight)
        #   - shortfalls from the ideal (100) on each bar
        #   - overflow penalty when load > 100%
        score_today = 0.0
        score_today += 0.5  * new_deaths
        score_today += 0.02 * (100.0 - self.economy)
        score_today += 0.02 * (100.0 - self.morale)
        score_today += 0.02 * (100.0 - self.trust)
        if load_pct > 100.0:
            score_today += 0.05 * (load_pct - 100.0)
        self.cumulative_score += score_today

        # 10) End-of-game
        self.prev_active = set(active)
        if day >= self.duration_days:
            self.finished = True
            self.final_breakdown = {
                "deaths":          self.deaths_total,
                "deaths_per_100k": round(self.deaths_total / N * 100_000, 1),
                "economy":         round(self.economy, 1),
                "morale":          round(self.morale, 1),
                "trust":           round(self.trust, 1),
                "budget_remaining": round(self.budget),
                "score":           round(self.cumulative_score, 1),
                "rating":          _rating_for(self.cumulative_score),
            }

    # ── Snapshot for the frontend HUD ────────────────────────────────────────

    def snapshot(self, day: int = 0) -> dict:
        return {
            "difficulty":       self.difficulty,
            "duration_days":    self.duration_days,
            "stability":        round(self.stability, 1),
            "economy":          round(self.economy, 1),
            "morale":           round(self.morale, 1),
            "trust":            round(self.trust, 1),
            "healthcare_load":  round(self.healthcare_load, 1),
            "budget":           round(self.budget),
            "deaths_total":     self.deaths_total,
            "score":            round(self.cumulative_score, 1),
            "finished":         self.finished,
            "final_breakdown":  self.final_breakdown,
            "vaccine_day":          self.vaccine_day,
            "vaccine_unlocked":     self.vaccine_unlocked(day),
            "days_until_vaccine":   self.days_until_vaccine(day),
        }


def _clamp01(v: float) -> float:
    return max(0.0, min(100.0, v))


def _rating_for(score: float) -> str:
    """Qualitative letter grade — rough thresholds, calibrated against
    the 'do nothing' baseline (~120) and 'permanent lockdown' (~80)."""
    if score < 30:   return "S"
    if score < 60:   return "A"
    if score < 100:  return "B"
    if score < 150:  return "C"
    if score < 220:  return "D"
    return "F"
