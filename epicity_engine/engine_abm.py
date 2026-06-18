"""
engine_abm.py — Phase 2 vectorised Agent-Based epidemic engine.

Each individual is a row across parallel NumPy arrays. One day of
simulation = `steps_per_day` hourly sub-steps. At each sub-step every
agent has a `current_location` (their home, work, or school building);
infection is resolved by grouping co-located agents and rolling a
Bernoulli per susceptible against a per-location probability derived
from beta × zone_multiplier × (infectious_count / total_at_location).

What this Phase 2 v1 ships:
  - Activity-based schedule (home / work / school) by age group.
  - Per-hour vectorised contact resolution by current_location.
  - Real `infector` recorded for every S→E transition (no spatial guesses).
  - Aggregation back to per-building S/E/I/R/D by age group so the
    existing /api/state response shape — and the entire frontend that
    consumes it — keeps working unchanged.
  - `edges_since(day)` exposes the true infection log for Phase 3's
    flow-viz hookup.
  - Six pathogen + behaviour interventions (lockdown, masks, distancing,
    schools, info_campaign, targeted_lockdown) applied as per-location
    contact-probability multipliers.

What this v1 does NOT yet do (deferred — see TODOs):
  - Vaccination intervention (S→R drip).
  - Waning immunity (R→S).
  - Transport-mediated transmission (agents teleport home↔work in v1;
    a transit vehicle pool with dwell time is Phase 2 v2).
  - Spatial interventions (quarantine zones, mobile clinics, surge
    testing, targeted vaccination).
  - Hospital overload mortality bump (uses baseline μ regardless).
  - External FOI from the metapopulation layer (intra-city only in v1).

Falling back to compartmental for those features in a hybrid is
deliberately not done here — when ABM is the selected engine, every
day is fully simulated agent-by-agent. The deferred features become
no-ops; users who need them stay on the compartmental engine until v2.

See docs/abm_design_brief.md for the full design rationale.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

import climate as climate_mod
from engine import (
    AGE_GROUPS,
    EpidemicEngine,
    Zone,
    _BETA_MULT,
    _DEFAULT_AGE_FRAC,
    _MU_MULT,
)


# ── SEIR state codes (int8 for compact arrays) ────────────────────────────────
S_STATE = 0
E_STATE = 1
I_STATE = 2
R_STATE = 3
D_STATE = 4

_STATE_NAMES = ("S", "E", "I", "R", "D")

# Age-group integer codes — must match the AGE_GROUPS tuple order
# ("child", "adult", "elder") from engine.py.
AG_CHILD = 0
AG_ADULT = 1
AG_ELDER = 2
_AG_NAMES = ("child", "adult", "elder")


# ── Configuration whitelist (mirrored in main.py for /api/engine_mode) ────────
VALID_POPULATION_SCALES: tuple[int, ...]  = (1, 2, 5, 10)
VALID_TIME_RESOLUTIONS: tuple[float, ...] = (1.0, 0.5, 0.25)


# ── Zone categories used to assign activity locations ────────────────────────

_RESIDENTIAL_ZONES = (Zone.RES_LO, Zone.RES_MED, Zone.RES_HI)

# Where adults work. Picked from the open-air commercial / civic / care
# pool, excluding schools (which children attend) and pure leisure POIs.
_WORK_ZONES = (
    Zone.COMMERCIAL, Zone.INDUSTRIAL, Zone.HOSPITAL, Zone.PHARMACY,
    Zone.UNIVERSITY, Zone.GROCERY, Zone.DENTAL, Zone.VETERINARY,
    Zone.MALL, Zone.HOTEL, Zone.RESTAURANT, Zone.SPORTS_CENTRE,
    Zone.LIBRARY, Zone.MUSEUM, Zone.THEATER, Zone.POLICE,
    Zone.FIRE_STATION, Zone.COMMUNITY_CENTRE,
)

# Where school-age children go on weekday mornings.
_SCHOOL_ZONES = (Zone.SCHOOL, Zone.HIGH_SCHOOL, Zone.KINDERGARTEN)


# ── POLYMOD-SE-style age-pair contact matrix ─────────────────────────────────
# C[susceptible_ag, infectious_ag] gives the relative intensity of a contact
# between those age groups when they share a current_location. Mean-scaled
# so an all-ones C reproduces v1's uniform mixing exactly — the matrix
# changes *who* mixes with whom, without inflating overall β. Source:
# Mossong et al. POLYMOD rough Swedish estimates; tweak via specialist
# consultation. Children-on-children (schools, playgrounds), adults-on-
# adults (workplaces) dominate; elder-on-child is low (intergenerational
# contact is mediated by adults).
_AGE_CONTACT_MATRIX_RAW: np.ndarray = np.array([
    [2.5, 1.0, 0.4],   # susceptible child  vs. child / adult / elder infectious
    [1.0, 1.8, 0.5],   # susceptible adult
    [0.4, 0.5, 1.5],   # susceptible elder
], dtype=np.float64)
_AGE_CONTACT_MATRIX: np.ndarray = _AGE_CONTACT_MATRIX_RAW / _AGE_CONTACT_MATRIX_RAW.mean()


# ── Synthetic commute-pool transit (Phase 2 v2 MVP) ──────────────────────────
# Real GTFS routing is the v2 backlog; for v1.1 we model the EFFECT of
# transit — strangers from different neighbourhoods sharing a closed box
# for an hour — with N synthetic pools. Adults with a workplace are
# assigned to one pool uniformly at random at engine reset; during
# weekday commute hours their current_location is the pool id, which
# lives in an integer range disjoint from building idxs. That is enough
# to break the "mixing only inside one building" bottleneck without
# needing the full GTFS-tied implementation in v2 §4.3.
_COMMUTE_POOL_COUNT: int    = 24      # ~city bus lines; coarse but separating
_COMMUTE_BETA_MULT:  float  = 1.6     # closed indoor space, ~30 min effective dwell
_COMMUTE_HOURS:      tuple  = (7, 17) # weekday morning + evening commute substep


# ── Per-building demographic β modulation ────────────────────────────────────
# household_avg_size (SCB HushallT26 per DeSO) is mapped to a contact
# multiplier in [0.7, 1.3] — denser households cluster tighter. income
# (SCB Tab2InkDesoRegso) is mapped to a multiplier in [0.8, 1.2] —
# lower-income DeSOs see more overcrowding and more multi-generational
# households, both of which raise per-contact β in epidemiological
# literature. Both are tiny perturbations on the zone β-multiplier; the
# point is that two RES_MED apartment blocks in different DeSOs now
# transmit at different rates instead of being interchangeable.
_HH_BETA_FLOOR: float = 0.7
_HH_BETA_CEIL:  float = 1.3
_INC_BETA_FLOOR: float = 0.8
_INC_BETA_CEIL:  float = 1.2


# ── Vectorised synthetic population ──────────────────────────────────────────

class _Population:
    """
    Parallel NumPy arrays — one row per agent.

    Built from the engine's building cells: each cell with pop > 0
    contributes round(pop_by_age / scale) agents per age group. The
    `scale` field records the people-per-agent ratio so cell-level
    totals stay in real-person units after aggregation.
    """

    def __init__(
        self,
        buildings: list,
        rng_seed: int,
        *,
        scale: int = 1,
        zone_beta_mult: np.ndarray | None = None,
    ) -> None:
        self.scale: int = int(scale)
        self.rng = np.random.default_rng(rng_seed)

        # ── Walk residential buildings to size the population ──────────
        res_local: list[tuple[int, float, float, float]] = []
        for b in buildings:
            if b.zone not in _RESIDENTIAL_ZONES or b.pop <= 0:
                continue
            af = b.age_frac if b.age_frac else _DEFAULT_AGE_FRAC
            res_local.append((
                b.idx,
                af.get("child", _DEFAULT_AGE_FRAC["child"]) * b.pop,
                af.get("adult", _DEFAULT_AGE_FRAC["adult"]) * b.pop,
                af.get("elder", _DEFAULT_AGE_FRAC["elder"]) * b.pop,
            ))

        # Each (building, age-group) pair contributes pop_ag / scale
        # agents. We use *probabilistic rounding* — int part guaranteed,
        # fractional part is a Bernoulli — so the total city population
        # is preserved in expectation at every scale. The naive
        # max(1, round(…)) floor would silently inflate cities with many
        # small buildings (a 1-resident apartment block at scale 5 would
        # still emit 1 agent, so the floor's worst case = N_buildings ×
        # 3 ag-groups of bonus agents). Probabilistic rounding fixes that
        # and makes population_scale a true population-size knob.
        per_cell: list[tuple[int, int, int]] = []   # (building_idx, age_int, n_agents)
        for b_idx, pc, pa, pe in res_local:
            for ag_int, pop_ag in ((AG_CHILD, pc), (AG_ADULT, pa), (AG_ELDER, pe)):
                if pop_ag <= 0:
                    continue
                frac = pop_ag / self.scale
                n = int(frac)
                if (frac - n) > self.rng.random():
                    n += 1
                if n > 0:
                    per_cell.append((b_idx, ag_int, n))

        total = sum(n for _, _, n in per_cell)
        self.n: int = total

        # ── Allocate arrays ─────────────────────────────────────────────
        self.home_building   = np.empty(total, dtype=np.int32)
        self.age_group       = np.empty(total, dtype=np.int8)
        self.gender          = np.empty(total, dtype=np.int8)   # 0 = M, 1 = F
        self.work_building   = np.full(total, -1, dtype=np.int32)
        self.current_location = np.empty(total, dtype=np.int32)
        self.state           = np.full(total, S_STATE, dtype=np.int8)
        self.t_exposed       = np.full(total, -1, dtype=np.int32)
        self.t_infectious    = np.full(total, -1, dtype=np.int32)
        self.infector        = np.full(total, -1, dtype=np.int32)

        # ── Fill home_building, age_group ──────────────────────────────
        pos = 0
        for b_idx, ag_int, n in per_cell:
            self.home_building[pos:pos + n] = b_idx
            self.age_group[pos:pos + n]     = ag_int
            pos += n
        assert pos == total

        # Gender 50/50 random (city-aggregate male_frac data is per-DeSO
        # and only marginally non-50%; not enough payoff to wire in v1).
        self.gender[:] = (self.rng.random(total) < 0.5).astype(np.int8)

        # ── Build work / school assignment pools ───────────────────────
        work_pool   = np.asarray(
            [b.idx for b in buildings if b.zone in _WORK_ZONES],
            dtype=np.int32,
        )
        school_pool = np.asarray(
            [b.idx for b in buildings if b.zone in _SCHOOL_ZONES],
            dtype=np.int32,
        )

        # ~75% of adults commute to a workplace; the rest stay home (a
        # rough proxy for retirees-with-an-adult-age-bucket, remote
        # workers, unemployment). Children attend a random school.
        adult_mask = (self.age_group == AG_ADULT)
        n_adults = int(adult_mask.sum())
        if n_adults > 0 and len(work_pool) > 0:
            has_work = self.rng.random(n_adults) < 0.75
            picks = work_pool[self.rng.integers(0, len(work_pool), size=n_adults)]
            picks[~has_work] = -1
            self.work_building[adult_mask] = picks

        child_mask = (self.age_group == AG_CHILD)
        n_children = int(child_mask.sum())
        if n_children > 0 and len(school_pool) > 0:
            picks = school_pool[self.rng.integers(0, len(school_pool), size=n_children)]
            self.work_building[child_mask] = picks
        # Elders' work_building stays -1 — they're home all day in v1.

        # ── Synthetic commute pool assignment ──────────────────────────
        # Every adult with a real workplace is assigned to one of
        # _COMMUTE_POOL_COUNT pools uniformly. During commute hours
        # (07–08, 17–18 weekdays) their current_location becomes the
        # pool id, which sits in a disjoint integer range above all
        # building idxs (see loc_beta_mult below). Adults without a
        # workplace and all non-adults keep commute_pool = -1 and stay
        # at home through commute hours.
        max_b_idx = max((b.idx for b in buildings), default=-1) + 1
        self._commute_base_idx: int = max_b_idx
        self.commute_pool = np.full(total, -1, dtype=np.int32)
        commuter_mask = (self.work_building != -1) & (self.age_group == AG_ADULT)
        n_commuters = int(commuter_mask.sum())
        if n_commuters > 0:
            pool_picks = self.rng.integers(
                0, _COMMUTE_POOL_COUNT, size=n_commuters).astype(np.int32)
            self.commute_pool[commuter_mask] = self._commute_base_idx + pool_picks

        # Initial location = home.
        self.current_location[:] = self.home_building

        # ── Per-location β multiplier (precomputed once) ───────────────
        # Combines three real-data signals into one lookup:
        #   • zone_β_mult (from engine.py:_BETA_MULT) — building type
        #   • household_avg_size  (SCB HushallT26)    — density factor
        #   • income              (SCB Tab2InkDesoRegso) — inequity factor
        # Plus a transit slab for the commute-pool location range. Storing
        # the combined multiplier lets step_hour skip three index lookups
        # per hour and reduces the per-bincount math to a single multiply.
        n_loc_slots = max_b_idx + _COMMUTE_POOL_COUNT
        self.building_zone = np.zeros(n_loc_slots, dtype=np.int32)
        self.loc_beta_mult = np.zeros(n_loc_slots, dtype=np.float64)
        # Income-median uses real data when present, otherwise falls back to
        # the neutral 1.0 floor so cells with missing income stay at no-op.
        _inc_vals = [float(b.income) for b in buildings
                     if b.income is not None and b.income > 0]
        inc_median = float(np.median(_inc_vals)) if _inc_vals else 0.0
        zbm = zone_beta_mult if zone_beta_mult is not None else None
        for b in buildings:
            self.building_zone[b.idx] = int(b.zone)
            zone_m = float(zbm[int(b.zone)]) if zbm is not None else 1.0
            hh_m = 1.0
            if b.household_avg_size and b.household_avg_size > 0:
                # SE mean household ≈ 2.1; mult = 0.6 + 0.2 · size hits
                # 1.0 at size 2.0 and 1.2 at size 3.0, clamped to band.
                hh_m = float(np.clip(
                    0.6 + 0.2 * b.household_avg_size,
                    _HH_BETA_FLOOR, _HH_BETA_CEIL,
                ))
            inc_m = 1.0
            if b.income is not None and b.income > 0 and inc_median > 0:
                # Lower than median → > 1.0; higher → < 1.0; clamped.
                inc_m = float(np.clip(
                    1.2 - 0.2 * (b.income / inc_median),
                    _INC_BETA_FLOOR, _INC_BETA_CEIL,
                ))
            self.loc_beta_mult[b.idx] = zone_m * hh_m * inc_m
        # Commute-pool slab — flat transit β multiplier (closed indoor box,
        # ~30 min effective dwell). Does NOT go through zone_β_mult.
        self.loc_beta_mult[max_b_idx:max_b_idx + _COMMUTE_POOL_COUNT] = _COMMUTE_BETA_MULT

        # ── Precompute (home_building, age_group) flat indices for the
        # daily aggregation step so we don't repeat the bincounts.
        # We'll use np.add.at over a (n_buildings, 5 states, 3 ages) array.
        # `_home_to_pos` maps the agent's home_building idx to a contiguous
        # position 0..n_residential-1; cells outside this set don't need
        # an aggregation row because the agent population only spans
        # residential buildings by construction.
        residential_idxs = np.unique(self.home_building) if total > 0 else np.array([], dtype=np.int32)
        self._residential_idxs = residential_idxs.astype(np.int32)
        # Inverse lookup: agent's home_building → position in residential_idxs.
        # np.searchsorted is O(log) — fine.
        self._home_pos = np.searchsorted(self._residential_idxs, self.home_building).astype(np.int32)

    # ── Hourly step ─────────────────────────────────────────────────────────

    def step_hour(
        self,
        *,
        hour_of_day:        float,
        weekday:            int,
        beta_per_hour:      float,
        sigma_per_hour:     float,
        gamma_per_hour:     float,
        mu_base:            float,
        external_foi_hour:  float,
        today_day:          int,
        zone_beta_mult:     np.ndarray,
        infection_log:      list[dict[str, Any]],
        max_log_size:       int,
        intervention:       dict[str, bool],
    ) -> None:
        """One hour of activity + infection draws + state transitions."""
        is_weekend     = weekday >= 5
        is_work_hour   = (8.0 <= hour_of_day < 18.0) and not is_weekend
        is_school_hour = (8.0 <= hour_of_day < 15.0) and not is_weekend
        # Commute pool fires for the morning + evening hours and only on
        # weekdays without lockdown. The pool location overrides the work
        # building for that single hour each direction — agents board the
        # synthetic transit "vehicle" at 07–08 and 17–18.
        h_int = int(hour_of_day)
        is_commute_hour = (
            (h_int in _COMMUTE_HOURS)
            and not is_weekend
            and not intervention["lockdown"]
        )

        # ── Locate every agent ───────────────────────────────────────────
        # Default: home. Override with school/work/commute-pool for the
        # appropriate age groups and hours.
        loc = self.home_building.copy()

        if is_school_hour and not intervention["schools_closed"] and not intervention["lockdown"]:
            at_school = (self.age_group == AG_CHILD) & (self.work_building != -1)
            loc[at_school] = self.work_building[at_school]

        if is_work_hour and not intervention["lockdown"]:
            at_work = (self.age_group == AG_ADULT) & (self.work_building != -1)
            loc[at_work] = self.work_building[at_work]

        if is_commute_hour:
            # commute_pool is -1 for non-commuters and for non-adult agents;
            # the mask isolates the adults who actually board transit. The
            # pool id is in [_commute_base_idx, _commute_base_idx+POOL_COUNT)
            # — disjoint from every building idx so the bincount naturally
            # groups all riders of pool P into one bucket.
            riding = (self.commute_pool != -1)
            loc[riding] = self.commute_pool[riding]

        self.current_location = loc

        # ── Infection resolution ─────────────────────────────────────────
        S_mask = (self.state == S_STATE)
        I_mask = (self.state == I_STATE)

        # Bernoulli for external FOI applies even when no internal I.
        if I_mask.any() or external_foi_hour > 0.0:
            max_loc = int(loc.max()) + 1
            n_per_loc = np.bincount(loc, minlength=max_loc).astype(np.float64)

            # Age-stratified infectious counts per location: one bincount
            # per age group. Shape (3, max_loc). This is the only thing
            # that lets the age-mixing matrix do its job — without it
            # we'd have to multiply by a single average n_I/n_total.
            n_I_by_age_per_loc = np.zeros((3, max_loc), dtype=np.float64)
            I_indices = np.where(I_mask)[0]
            if I_indices.size:
                I_ag = self.age_group[I_indices]
                I_loc = loc[I_indices]
                for ag_int in (AG_CHILD, AG_ADULT, AG_ELDER):
                    sel = (I_ag == ag_int)
                    if sel.any():
                        n_I_by_age_per_loc[ag_int] = np.bincount(
                            I_loc[sel], minlength=max_loc).astype(np.float64)

            # Per-location combined β multiplier — zone × density × income
            # for buildings, transit β-mult for commute pools. Slice
            # defensively in case loc.max() drifts beyond the precomputed
            # slab (shouldn't happen by construction).
            if max_loc <= len(self.loc_beta_mult):
                zb = self.loc_beta_mult[:max_loc]
            else:
                zb = np.zeros(max_loc, dtype=np.float64)
                zb[:len(self.loc_beta_mult)] = self.loc_beta_mult

            # Behaviour multipliers (mask mandate, distancing, info campaign).
            beta_eff = beta_per_hour * intervention["contact_mult"]

            # Age-mixing matrix collapses the per-age infectious counts
            # into a per-susceptible-age weighted-infectious term:
            #   weighted_I[a, ℓ] = Σ_b C[a, b] · n_I_b[ℓ]
            # giving a (3, max_loc) tensor of "infectious-equivalents
            # weighted by who a susceptible of age a is likely to contact".
            weighted_I = _AGE_CONTACT_MATRIX @ n_I_by_age_per_loc

            # Per-location-per-age probability that one susceptible of
            # age a is exposed this hour. Scale-aware: each agent stands
            # in for `scale` real people, so the exposure for one agent
            # aggregates that many people's contacts.
            denom = np.maximum(1.0, n_per_loc)
            p_loc_per_age = (
                beta_eff * zb[None, :] * (weighted_I / denom[None, :]) * self.scale
            )
            p_loc_per_age = p_loc_per_age + external_foi_hour
            np.clip(p_loc_per_age, 0.0, 0.99, out=p_loc_per_age)

            # Roll for each susceptible using (their age, their location).
            S_idx = np.where(S_mask)[0]
            if S_idx.size:
                p_each = p_loc_per_age[self.age_group[S_idx], loc[S_idx]]
                rolls  = self.rng.random(S_idx.size)
                new_E_idx = S_idx[rolls < p_each]

                if new_E_idx.size:
                    # ── Promote S → E and stamp the exposure clock ──────
                    self.state[new_E_idx]     = E_STATE
                    self.t_exposed[new_E_idx] = today_day * 24 + int(hour_of_day)

                    # Sample incubation. sigma_per_hour ≈ E→I rate; the
                    # waiting time is Exponential with mean 1/σ. Floor at
                    # 6 hours so nobody flips infectious the same step
                    # they get exposed.
                    mean_h = 1.0 / max(sigma_per_hour, 1e-6)
                    incub = np.maximum(6, self.rng.exponential(
                        mean_h, size=new_E_idx.size).astype(np.int32))
                    self.t_infectious[new_E_idx] = self.t_exposed[new_E_idx] + incub

                    # ── Assign infectors ────────────────────────────────
                    # For each newly exposed agent, sample a co-located
                    # infectious agent as the infector. (External-FOI
                    # cases — no co-located infector — keep -1.)
                    self._assign_infectors(
                        new_E_idx=new_E_idx,
                        loc=loc,
                        I_mask=I_mask,
                        today_day=today_day,
                        hour_of_day=hour_of_day,
                        infection_log=infection_log,
                        max_log_size=max_log_size,
                    )

        # ── E → I transitions ─────────────────────────────────────────────
        current_t = today_day * 24 + int(hour_of_day)
        mature_E = (self.state == E_STATE) & (self.t_infectious <= current_t) & (self.t_infectious >= 0)
        if mature_E.any():
            self.state[mature_E] = I_STATE

        # ── I → R / D transitions ─────────────────────────────────────────
        I_now = (self.state == I_STATE)
        if I_now.any():
            I_idx = np.where(I_now)[0]
            removing = self.rng.random(I_idx.size) < gamma_per_hour
            rm_idx = I_idx[removing]
            if rm_idx.size:
                # Age-stratified μ — children-favourable, elder-penalising.
                ag = self.age_group[rm_idx]
                mu_each = np.full(rm_idx.size, mu_base, dtype=np.float64)
                mu_each[ag == AG_CHILD] *= _MU_MULT["child"]
                mu_each[ag == AG_ELDER] *= _MU_MULT["elder"]
                # Adults stay at base μ (_MU_MULT["adult"] == 1.0).
                dies = self.rng.random(rm_idx.size) < mu_each
                self.state[rm_idx[dies]]  = D_STATE
                self.state[rm_idx[~dies]] = R_STATE

    # ── Infector selection ──────────────────────────────────────────────────

    def _assign_infectors(
        self,
        *,
        new_E_idx:    np.ndarray,
        loc:          np.ndarray,
        I_mask:       np.ndarray,
        today_day:    int,
        hour_of_day:  float,
        infection_log: list[dict[str, Any]],
        max_log_size: int,
    ) -> None:
        """
        For each newly exposed agent, randomly sample an infectious agent
        at the same current_location and record (susceptible, infector,
        location, day, hour) into the log.

        Vectorised by location: instead of a per-agent np.where we
        precompute the list of infectious agents per location once.
        """
        # Locations of the newly exposed.
        new_locs = loc[new_E_idx]

        # Group infectious agents by location.
        I_idx = np.where(I_mask)[0]
        if I_idx.size == 0:
            # All new exposures came from external FOI — infector stays -1.
            return
        I_loc = loc[I_idx]

        # Build {location_id → list of infectious agent indices}. We use a
        # plain dict because the number of unique locations with I is
        # typically small (~hundreds) even in a large city.
        I_at: dict[int, np.ndarray] = {}
        # Sort once by location so we can split with np.unique.
        order = np.argsort(I_loc, kind="stable")
        I_loc_sorted = I_loc[order]
        I_idx_sorted = I_idx[order]
        unique_locs, starts = np.unique(I_loc_sorted, return_index=True)
        ends = np.append(starts[1:], I_idx_sorted.size)
        for k, ul in enumerate(unique_locs):
            I_at[int(ul)] = I_idx_sorted[starts[k]:ends[k]]

        room = max(0, max_log_size - len(infection_log))
        for k, s_idx in enumerate(new_E_idx):
            ul = int(new_locs[k])
            pool = I_at.get(ul)
            if pool is None or pool.size == 0:
                # External-FOI exposure; leave infector as -1.
                continue
            inf_id = int(pool[self.rng.integers(0, pool.size)])
            self.infector[s_idx] = inf_id
            if room > 0:
                infection_log.append({
                    "susceptible": int(s_idx),
                    "infector":    inf_id,
                    "location":    ul,
                    "day":         int(today_day),
                    "hour":        int(hour_of_day),
                })
                room -= 1

    # ── Aggregate per-agent counts → per-cell SEIR ─────────────────────────

    def aggregate_to_cells(self, buildings: list) -> None:
        """
        Sum agent state by (home_building, age_group, SEIR-state) → cell
        dicts. After this call, building.S/E/I/R/D reflect the agent
        population and the existing state() output matches the
        EpidemicEngine shape.

        Hot path: vectorised np.add.at into a dense
        (n_residential, 5 states, 3 ages) tensor, then a single Python
        loop over residential cells to copy into their dicts.
        """
        # Reset every cell — the agent counts are authoritative.
        for b in buildings:
            b.S = {g: 0.0 for g in AGE_GROUPS}
            b.E = {g: 0.0 for g in AGE_GROUPS}
            b.I = {g: 0.0 for g in AGE_GROUPS}
            b.R = {g: 0.0 for g in AGE_GROUPS}
            b.D = {g: 0.0 for g in AGE_GROUPS}

        if self.n == 0:
            return

        n_res = self._residential_idxs.size
        counts = np.zeros((n_res, 5, 3), dtype=np.int64)
        # np.add.at is the atomic-add for fancy indexing in NumPy — exactly
        # one O(N) pass to fill the 15 buckets per residential cell.
        np.add.at(
            counts,
            (self._home_pos, self.state.astype(np.int64), self.age_group.astype(np.int64)),
            1,
        )

        b_by_idx = {b.idx: b for b in buildings}
        scale_f = float(self.scale)
        for pos in range(n_res):
            bidx = int(self._residential_idxs[pos])
            cell = b_by_idx.get(bidx)
            if cell is None:
                continue
            slab = counts[pos]
            # slab is (5, 3). Unroll the inner loop for speed + clarity.
            for s_int, s_name in enumerate(_STATE_NAMES):
                target = getattr(cell, s_name)
                row = slab[s_int]
                if row[0] > 0: target["child"] += float(row[0]) * scale_f
                if row[1] > 0: target["adult"] += float(row[1]) * scale_f
                if row[2] > 0: target["elder"] += float(row[2]) * scale_f


# ── AgentEngine ──────────────────────────────────────────────────────────────

class AgentEngine(EpidemicEngine):
    """
    Vectorised per-agent epidemic engine. Drop-in for EpidemicEngine —
    same public API, same /api/state shape — but the daily step iterates
    a synthetic population through hourly activity buckets and records a
    real infection log instead of approximating spread spatially.
    """

    def __init__(
        self,
        city_id: str = "vaxjo",
        *,
        population_scale:  int   = 1,
        time_resolution_h: float = 1.0,
    ) -> None:
        if population_scale not in VALID_POPULATION_SCALES:
            raise ValueError(
                f"population_scale must be one of {VALID_POPULATION_SCALES}, "
                f"got {population_scale!r}"
            )
        if time_resolution_h not in VALID_TIME_RESOLUTIONS:
            raise ValueError(
                f"time_resolution_h must be one of {VALID_TIME_RESOLUTIONS}, "
                f"got {time_resolution_h!r}"
            )
        self._population_scale:  int   = int(population_scale)
        self._time_resolution_h: float = float(time_resolution_h)
        self._population: _Population | None = None
        # Per-(susceptible, infector, location, day, hour) transmission log.
        # Capped so a long sandbox run doesn't accumulate without bound;
        # the most recent edges are what the flow viz wants anyway.
        self._infection_log: list[dict[str, Any]] = []
        self._max_log_size: int = 50_000

        # Build a Zone-int-indexed β multiplier array once.
        max_zone = max(int(z) for z in Zone)
        self._zone_beta_mult = np.zeros(max_zone + 1, dtype=np.float64)
        for z in Zone:
            self._zone_beta_mult[int(z)] = float(_BETA_MULT.get(z, 1.0))

        super().__init__(city_id=city_id)

    # ── ABM accessors (also used by main.py for /api/engine_mode round-trip) ──

    @property
    def population_scale(self) -> int:
        return self._population_scale

    @property
    def time_resolution_h(self) -> float:
        return self._time_resolution_h

    @property
    def steps_per_day(self) -> int:
        return max(1, round(24.0 / self._time_resolution_h))

    def edges_since(self, day_from: int = 0) -> list[dict[str, Any]]:
        """Real transmission edges for the infection-flow visualisation."""
        if not self._infection_log:
            return []
        return [e for e in self._infection_log if e["day"] >= day_from]

    # ── State emission ─────────────────────────────────────────────────────

    def state(self) -> dict[str, Any]:
        """
        Same shape as EpidemicEngine.state(), plus per-cell
        ``infections_here`` — the cumulative count of S→E transmission
        events that occurred AT this building (location field of the
        infection log). This is distinct from cells[i].I, which counts
        infectious agents whose HOME is building i — so the frontend
        can offer two epidemiologically distinct views: "where infected
        people live" vs. "where infections happened".

        Sparse emission: only cells with at least one recorded
        transmission get the field, so the JSON stays compact (a city
        with 70 k buildings and 200 transmissions ships ~200 extra
        fields instead of 70 k zeros). The frontend treats missing as 0.
        """
        s = super().state()
        # Always emit the transit-infection fields under ABM, even with
        # an empty log — the frontend's setTransitInfectionLevel() hook
        # reads state.transit_infections_total on every render, and a
        # missing key would leave the red glow at whatever level the
        # previous step left it. Per-cell `infections_here` stays sparse
        # because the cells array is hot-pathed by the renderer.
        s["transit_infections_total"]   = 0
        s["transit_infections_by_pool"] = [0] * _COMMUTE_POOL_COUNT

        if self._infection_log and self._population is not None:
            max_b = int(self._population.building_zone.shape[0])
            if max_b > 0:
                # Vectorised location aggregation. fromiter + bincount
                # keeps this O(L + B) at the 50k-entry log cap (≲ 5 ms).
                locs = np.fromiter(
                    (e["location"] for e in self._infection_log),
                    dtype=np.int64,
                    count=len(self._infection_log),
                )
                # ── Per-building infection sites ────────────────────────
                in_building = (locs >= 0) & (locs < max_b)
                counts = np.bincount(locs[in_building], minlength=max_b)
                # Sparse fill — skip zeros to keep the cells array tight.
                for c in s["cells"]:
                    idx = c["idx"]
                    if 0 <= idx < max_b:
                        v = int(counts[idx])
                        if v > 0:
                            c["infections_here"] = v
                # ── Transit-pool infection sites ────────────────────────
                # Locations >= max_b are synthetic commute pools (see
                # _Population.__init__ where the commute_pool id is set
                # to _commute_base_idx + pool_picks). Aggregate those
                # separately so the frontend can surface "infections in
                # transit" without conflating them with building counts.
                base = int(getattr(self._population, "_commute_base_idx", max_b))
                pool_count = _COMMUTE_POOL_COUNT
                in_transit = (locs >= base) & (locs < base + pool_count)
                transit_locs = locs[in_transit] - base
                if transit_locs.size > 0:
                    pool_counts = np.bincount(transit_locs, minlength=pool_count)
                    s["transit_infections_by_pool"] = [int(x) for x in pool_counts]
                    s["transit_infections_total"]   = int(pool_counts.sum())
        return s

    # ── Lifecycle ───────────────────────────────────────────────────────────

    def reset(self) -> None:
        """
        Build cells via the parent, seed patient zero (also via parent),
        then materialise the synthetic agent population from the cell
        state. The agents then drive simulation; cells are recomputed
        from agents at the end of every step for the state() output.
        """
        super().reset()
        self._population = _Population(
            self._buildings,
            rng_seed=self._rng_seed,
            scale=self._population_scale,
            zone_beta_mult=self._zone_beta_mult,
        )
        self._infection_log.clear()
        # The parent's _seed_seir put E counts into cell dicts. Translate
        # those into per-agent E states so the outbreak actually starts.
        self._seed_agents_from_cells()
        # Re-aggregate so cells reflect the (now sparse) agent state. After
        # this the totals should match the seed plus the susceptible pool
        # we built from cell.S.
        self._population.aggregate_to_cells(self._buildings)

    def _seed_agents_from_cells(self) -> None:
        """
        For every cell that carries seed exposures (E > 0 from
        _seed_seir), mark round(E / scale) agents at that home_building
        and age group as the outbreak's index cases.

        These agents are placed directly into the **I** (infectious)
        state — bypassing the E incubation window — so the patient-zero
        building visibly starts growing from day 1 in the default
        Color-by-Infection / Height-by-Infection view. Without this,
        the stochastic mean-4-day σ window kept seeds in E for the
        first few days and the origin building looked inert until the
        first agent matured, which made it easy to think nothing was
        happening yet. Epidemiologically this matches the "index case
        is already symptomatic and shedding" framing used by most
        situational-awareness models; the compartmental engine reaches
        the same visible state on day 1 via its deterministic
        sigma-rate E→I flow, so behaviour now matches across modes.
        """
        pop = self._population
        if pop is None:
            return
        scale_f = float(pop.scale)

        for b in self._buildings:
            for ag_int, g in enumerate(_AG_NAMES):
                e_count = b.E.get(g, 0.0)
                if e_count <= 0:
                    continue
                n_to_expose = max(1, int(round(e_count / scale_f)))
                mask = (
                    (pop.home_building == b.idx)
                    & (pop.age_group == ag_int)
                    & (pop.state == S_STATE)
                )
                idx_pool = np.where(mask)[0]
                if idx_pool.size == 0:
                    continue
                k = min(n_to_expose, idx_pool.size)
                chosen = idx_pool[:k]
                # Directly infectious — no incubation delay for seeds.
                pop.state[chosen]        = I_STATE
                pop.t_exposed[chosen]    = 0
                pop.t_infectious[chosen] = 0
                # infector stays -1 — these are patient zeros, not edges.

    # ── Daily step ──────────────────────────────────────────────────────────

    def step(
        self,
        n: int = 1,
        flow_hour: float = 9.0,
        external_foi: float = 0.0,
    ) -> None:
        """
        Advance the simulation by `n` days, each day = steps_per_day
        hourly agent sub-steps. The `n` parameter matches the compartmental
        signature (where it's animation-substep count); for ABM we treat
        it as a true day count so an /api/step?n=1 advances one calendar
        day either way and the existing UI playbar timing keeps working.
        """
        if self._population is None:
            super().step(n=n, flow_hour=flow_hour, external_foi=external_foi)
            return

        n = max(1, min(int(n), 60))
        for _ in range(n):
            self._step_one_day(flow_hour=flow_hour, external_foi=external_foi)

    def _step_one_day(self, *, flow_hour: float, external_foi: float) -> None:
        """One full calendar day = steps_per_day hourly ABM sub-steps."""
        # ── Resolve effective parameters (interventions × climate × weekend) ──
        p = self._effective_params()
        if self._is_weekend():
            beta_scale, lfrac_delta, weekend_school = self._weekend_mult(flow_hour)
            p.school_factor = min(p.school_factor, weekend_school)
        else:
            beta_scale, lfrac_delta = self._commute_mult(flow_hour)
        p.beta *= beta_scale

        doy = (self._day + self._day_of_year_offset) % 365
        clim = climate_mod.compute(doy, flow_hour)
        p.beta *= clim.beta_mult
        self._last_hour = flow_hour

        # Pre-day intervention flags (computed once for the whole day).
        active = self._active
        contact_mult = 1.0
        if "masks" in active:
            contact_mult *= 0.5
        if "distancing" in active:
            contact_mult *= 0.7
        if "info_campaign" in active:
            contact_mult *= 0.92
        if "targeted_lockdown" in active:
            contact_mult *= 0.65
        intervention = {
            "lockdown":        "lockdown" in active,
            "schools_closed":  "schools"  in active,
            "contact_mult":    contact_mult,
        }

        # Per-hour rate conversions. σ, γ are per-day; divide by 24.
        steps = self.steps_per_day
        hours_per_step = 24.0 / steps
        beta_per_hour  = p.beta  * hours_per_step / 24.0   # = p.beta * (1/steps)
        sigma_per_hour = p.sigma * hours_per_step / 24.0
        gamma_per_hour = p.gamma * hours_per_step / 24.0
        ext_per_hour   = external_foi * hours_per_step / 24.0

        weekday = self._day % 7

        for h_idx in range(steps):
            hour_of_day = (flow_hour + h_idx * hours_per_step) % 24.0
            self._population.step_hour(
                hour_of_day=hour_of_day,
                weekday=weekday,
                beta_per_hour=beta_per_hour,
                sigma_per_hour=sigma_per_hour,
                gamma_per_hour=gamma_per_hour,
                mu_base=p.mu,
                external_foi_hour=ext_per_hour,
                today_day=self._day,
                zone_beta_mult=self._zone_beta_mult,
                infection_log=self._infection_log,
                max_log_size=self._max_log_size,
                intervention=intervention,
            )

        # Day done — fold agent state back into cell SEIR for state() output.
        self._day += 1
        self._population.aggregate_to_cells(self._buildings)

        # Hospital overload + peak tracking (replicated from parent's _step_one
        # so /api/state's `hospital_overloaded` and `peak_prevalence` fields
        # stay sensible under the ABM engine).
        total_N = sum(b.N for b in self._buildings)
        total_I = sum(b.I_tot for b in self._buildings)
        self._overloaded = (total_I / total_N > p.hospital_capacity) if total_N else False
        if total_N > 0:
            i_frac = total_I / total_N
            if i_frac > self._peak_I_frac:
                self._peak_I_frac = i_frac
                self._peak_I_day  = self._day

        # History snapshot — same call the parent makes after each day.
        self._history.append(self._snapshot())
