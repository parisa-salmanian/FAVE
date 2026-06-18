"""
demography.py — Country-of-birth distributions per zone type.

EpiCity uses 6 origin groups (mirroring SCB's coarse grouping). Each zone
gets a base distribution; per-building origin is derived deterministically
from a Mulberry32-style jitter so neighbouring cells of the same zone
differ slightly.

The city-wide foreign-born fraction (from `scb.load_country_of_birth`)
is used to scale the non-Swedish columns up or down so the per-building
mix actually matches that city's SCB total.
"""
from __future__ import annotations


# `flag` is an emoji prefix rendered next to the group label in the
# Statistics panel and the building hover popup. Specific-country groups
# get their real flag; mixed regional groups use a globe emoji oriented
# to that region as a neutral stand-in.
ORIGIN_GROUPS: list[dict] = [
    {"id": "se",       "label_en": "Sweden",                  "label_sv": "Sverige",                "color": "#3b82f6", "flag": "🇸🇪"},
    {"id": "nordic",   "label_en": "Other Nordic",            "label_sv": "Övriga Norden",          "color": "#22d3ee", "flag": "🇳🇴"},
    {"id": "eu",       "label_en": "Other EU/EEA",            "label_sv": "Övriga EU/EES",          "color": "#a3e635", "flag": "🇪🇺"},
    {"id": "mena",     "label_en": "Middle East / N.Africa",  "label_sv": "Mellanöstern/Nordafrika","color": "#f59e0b", "flag": "🌍"},
    {"id": "ssa",      "label_en": "Sub-Saharan Africa",      "label_sv": "Övriga Afrika",          "color": "#f97316", "flag": "🌍"},
    {"id": "asia_oth", "label_en": "Asia / Other",            "label_sv": "Asien / Övrigt",         "color": "#a78bfa", "flag": "🌏"},
]

# Default base composition per zone type (roughly Swedish municipality avg).
# Rows sum to 1.0. Industrial/HighDensity skew more diverse; schools mostly
# Swedish-born (children); others somewhere in between.
ZONE_ORIGIN_BASE: dict[int, list[float]] = {
    1:  [0.86, 0.02, 0.04, 0.04, 0.01, 0.03],   # RES_LO
    2:  [0.78, 0.02, 0.05, 0.07, 0.02, 0.06],   # RES_MED
    3:  [0.65, 0.03, 0.07, 0.13, 0.04, 0.08],   # RES_HI
    4:  [0.74, 0.02, 0.06, 0.08, 0.03, 0.07],   # COMMERCIAL
    5:  [0.62, 0.02, 0.10, 0.13, 0.05, 0.08],   # INDUSTRIAL
    6:  [0.70, 0.04, 0.08, 0.09, 0.03, 0.06],   # HOSPITAL
    7:  [0.80, 0.02, 0.05, 0.06, 0.02, 0.05],   # PARK
    8:  [0.83, 0.02, 0.05, 0.05, 0.02, 0.03],   # SCHOOL
    10: [0.65, 0.03, 0.10, 0.10, 0.04, 0.08],   # UNIVERSITY (international students)
    11: [0.74, 0.02, 0.06, 0.08, 0.03, 0.07],   # GROCERY
    24: [0.84, 0.02, 0.05, 0.05, 0.02, 0.02],   # KINDERGARTEN
    25: [0.78, 0.02, 0.06, 0.07, 0.03, 0.04],   # HIGH_SCHOOL
}

# Default fallback — used for any zone not listed above.
DEFAULT_ORIGIN: list[float] = [0.78, 0.02, 0.06, 0.07, 0.03, 0.04]


def origin_for(zone: int, jitter01: float, foreign_frac: float | None = None) -> list[float]:
    """
    Per-building origin distribution.

    @param zone          OSM zone enum value
    @param jitter01      deterministic jitter in [0, 1) (use a stable hash)
    @param foreign_frac  city-wide non-Swedish fraction from SCB; if provided,
                         the non-Swedish columns are rescaled so the city total
                         matches this number.
    """
    base = ZONE_ORIGIN_BASE.get(zone, DEFAULT_ORIGIN)
    out  = list(base)

    # ±15 % multiplicative jitter on the non-Swedish columns
    swing = 0.30 * (jitter01 - 0.5)
    for i in range(1, 6):
        out[i] = max(0.0, out[i] * (1.0 + swing))

    # Scale non-Swedish columns to match city's foreign_frac (if provided
    # AND plausible — anything over 0.50 likely means SCB returned aggregate
    # totals only, in which case fall back to the zone defaults).
    if foreign_frac is not None and 0 < foreign_frac < 0.50:
        non_sw_sum = sum(out[1:])
        target_non_sw = max(0.0, min(0.50, foreign_frac))
        if non_sw_sum > 0:
            scale = target_non_sw / non_sw_sum
            for i in range(1, 6):
                out[i] *= scale

    out[0] = max(0.0, 1.0 - sum(out[1:]))
    s = sum(out)
    return [v / s for v in out] if s > 0 else base


def predominant(fractions: list[float]) -> int:
    """Index of the largest fraction."""
    best_i, best_v = 0, -1.0
    for i, v in enumerate(fractions):
        if v > best_v:
            best_i, best_v = i, v
    return best_i
