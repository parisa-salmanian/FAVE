"""
climate.py — Temperature / humidity → β multiplier for the SEIR engine.

Annual cycle parameters tuned to a southern-Swedish climate (Lund/Malmö):
mean ≈ 8 °C, summer peak ≈ 18 °C, winter trough ≈ -2 °C.
Daily cycle adds ±5 °C around the daily mean (warmest ~15:00, coldest ~05:00).

β multiplier (Lowen & Steel 2014; Wang et al. 2021):
    mult = exp(-k_T * (T - T_ref))            # colder → higher β
         * (1 + k_RH * (RH_ref - RH))         # drier → higher β
where k_T ≈ 0.04 °C⁻¹ (~3.1 % R reduction per +1 °C, Wang 2021).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

# Annual cycle (Lund/Malmö normals)
ANNUAL_MEAN_C   = 8.0
ANNUAL_AMPL_C   = 11.0
PEAK_DAY        = 200    # warmest day-of-year (mid-July)

# Daily cycle
DAILY_AMPL_C    = 5.0
DAILY_PEAK_HOUR = 15.0

# Humidity (rough inverse coupling to temperature)
ANNUAL_MEAN_RH  = 76.0
ANNUAL_AMPL_RH  = 8.0
RH_REF          = 50.0

# Sensitivities (Wang et al. 2021)
K_T  = 0.040
K_RH = 0.0040

T_REF = ANNUAL_MEAN_C


@dataclass
class ClimateState:
    day_of_year: int
    hour:        float
    temperature: float
    humidity:    float
    beta_mult:   float
    season:      str


def _season(day_of_year: int) -> str:
    if 60  <= day_of_year < 152: return "Spring"
    if 152 <= day_of_year < 244: return "Summer"
    if 244 <= day_of_year < 335: return "Autumn"
    return "Winter"


def compute(day_of_year: int, hour: float) -> ClimateState:
    """Compute temperature, humidity, and β multiplier at (day_of_year, hour)."""
    doy = day_of_year % 365
    daily_mean = ANNUAL_MEAN_C + ANNUAL_AMPL_C * math.cos(
        2.0 * math.pi * (doy - PEAK_DAY) / 365.0
    )
    temp = daily_mean + DAILY_AMPL_C * math.cos(
        2.0 * math.pi * (hour - DAILY_PEAK_HOUR) / 24.0
    )
    rh = ANNUAL_MEAN_RH - ANNUAL_AMPL_RH * math.cos(
        2.0 * math.pi * (doy - PEAK_DAY) / 365.0
    ) - 4.0 * math.cos(2.0 * math.pi * (hour - DAILY_PEAK_HOUR) / 24.0)
    rh = max(40.0, min(100.0, rh))
    mult = math.exp(-K_T * (temp - T_REF)) * (1.0 + K_RH * (RH_REF - rh))
    mult = max(0.55, min(1.70, mult))

    return ClimateState(
        day_of_year=doy,
        hour=hour,
        temperature=round(temp, 1),
        humidity=round(rh, 1),
        beta_mult=round(mult, 4),
        season=_season(doy),
    )
