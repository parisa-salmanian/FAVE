"""
fhm.py — Folkhälsomyndigheten surveillance-data loader.

FHM (Sweden's public-health agency) publishes weekly notifiable-disease
counts per kommun on https://dataportal.se as CSV files. The exact URL
changes per disease + release version, so v1 expects the user to
download the file once and place it under `data/fhm/`. Future versions
can wrap a real HTTP fetcher around the same cache layout (mirroring
`scb.py`).

Public function:
    locate_fhm_csv(name='*.csv')  → list[Path]
        List downloaded CSVs available for calibration.

For COVID-19 spring 2020 (the suggested first calibration target in
docs/abm_calibration_plan.md), the right dataset is:

    Owner:  Folkhälsomyndigheten
    Title:  "Antal fall av COVID-19 per vecka och kommun"
    URL:    https://dataportal.se/datasets (search the title)
    Format: CSV, semicolon-delimited
    Columns: KnKod, Region, Vecka, Antal_fall_vecka  (or similar)

Save as e.g. `data/fhm/covid19_kommun_weekly_2020.csv`. Then run
`python tools/calibrate.py vaxjo --target <that path> --weekly --probe`.
"""

from __future__ import annotations

import os
from pathlib import Path


def _data_root() -> Path:
    return Path(__file__).resolve().parent / "data" / "fhm"


def locate_fhm_csv(pattern: str = "*.csv") -> list[Path]:
    """List available FHM CSVs under data/fhm/, sorted by name."""
    root = _data_root()
    if not root.is_dir():
        return []
    return sorted(root.glob(pattern))


def ensure_fhm_dir() -> Path:
    """Make sure data/fhm/ exists; return its path."""
    root = _data_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def describe() -> str:
    """One-line summary of what's downloaded — useful for CLI banners."""
    files = locate_fhm_csv()
    if not files:
        return f"No FHM CSVs found at {_data_root()}. " \
               f"See module docstring for how to download."
    sizes = ", ".join(f"{p.name} ({p.stat().st_size // 1024} kB)" for p in files)
    return f"FHM CSVs in {_data_root()}: {sizes}"


if __name__ == "__main__":
    print(describe())
