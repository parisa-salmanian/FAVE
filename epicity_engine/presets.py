"""
presets.py — Pathogen-named scenario presets for the SEIR engine.

These complement (not replace) the existing Mild / Moderate / Severe difficulty
presets in static/settings.js. Each preset references a key in references.py
via `ref` so the About page can cross-link the preset to its source.

COVID-19 is flagged as default — most-cited SARS-CoV-2 parameter source.
"""
from __future__ import annotations


PRESETS: list[dict] = [
    {
        "id":       "covid19",
        "label_en": "SARS-CoV-2 (COVID-19, Li 2020)",
        "label_sv": "SARS-CoV-2 (covid-19, Li 2020)",
        "short":    "COVID-19",
        "icon":     "🦠",
        "ref":      "li-2020-incubation",
        "default":  True,
        "blurb_en": "Ancestral SARS-CoV-2 (Wuhan, 2020). Incubation 5.2 d, infectious "
                    "≈ 10 d, IFR ≈ 0.66 %, R₀ ≈ 2.5–3.3. Default scenario — Li 2020 and "
                    "Verity 2020 are the most-cited COVID-19 parameter studies.",
        "blurb_sv": "Ursprunglig SARS-CoV-2 (Wuhan 2020). Inkubation 5,2 d, smittsam "
                    "≈ 10 d, IFR ≈ 0,66 %, R₀ ≈ 2,5–3,3. Standardscenario — Li 2020 och "
                    "Verity 2020 är de mest citerade studierna av covid-19-parametrar.",
        "params":   {"beta": 0.32, "sigma": 1/5.2, "gamma": 1/10, "mu": 0.008,
                     "waning_days": 180},
    },
    {
        "id":       "flu_h1n1_2009",
        "label_en": "Influenza A/H1N1 2009 (Fraser 2009)",
        "label_sv": "Influensa A/H1N1 2009 (Fraser 2009)",
        "short":    "H1N1 2009",
        "icon":     "🤧",
        "ref":      "fraser-2009",
        "blurb_en": "Pandemic H1N1 from Mexico: R₀ ≈ 1.4–1.6, incubation ~2 d, "
                    "infectious ~4 d, CFR ≈ 0.4 %.",
        "blurb_sv": "Pandemisk H1N1 från Mexiko: R₀ ≈ 1,4–1,6, inkubation ~2 d, "
                    "smittsam ~4 d, CFR ≈ 0,4 %.",
        "params":   {"beta": 0.30, "sigma": 1/2, "gamma": 1/4, "mu": 0.004,
                     "waning_days": 365},
    },
    {
        "id":       "sars_2003",
        "label_en": "SARS-CoV-1 outbreak 2003 (Lipsitch 2003)",
        "label_sv": "SARS-CoV-1, utbrott 2003 (Lipsitch 2003)",
        "short":    "SARS 2003",
        "icon":     "😷",
        "ref":      "lipsitch-2003",
        "blurb_en": "SARS-CoV-1: incubation ~4 d, infectious ~8 d, CFR ≈ 9.6 %, "
                    "R₀ ≈ 2.2–3.6 before isolation.",
        "blurb_sv": "SARS-CoV-1: inkubation ~4 d, smittsam ~8 d, CFR ≈ 9,6 %, "
                    "R₀ ≈ 2,2–3,6 före isolering.",
        "params":   {"beta": 0.25, "sigma": 1/4, "gamma": 1/8, "mu": 0.096,
                     "waning_days": 365},
    },
    {
        "id":       "mers_2014",
        "label_en": "MERS-CoV (Cauchemez 2014)",
        "label_sv": "MERS-CoV (Cauchemez 2014)",
        "short":    "MERS",
        "icon":     "🐪",
        "ref":      "cauchemez-2014",
        "blurb_en": "MERS-CoV (Arabian Peninsula 2012–2014): incubation ≈ 5 d, "
                    "infectious ≈ 7 d, CFR ≈ 35 %, R₀ ≈ 0.6–0.9 — sub-critical, "
                    "mostly hospital-amplified.",
        "blurb_sv": "MERS-CoV (Arabiska halvön 2012–2014): inkubation ≈ 5 d, "
                    "smittsam ≈ 7 d, CFR ≈ 35 %, R₀ ≈ 0,6–0,9 — subkritiskt utbrott.",
        "params":   {"beta": 0.10, "sigma": 1/5, "gamma": 1/7, "mu": 0.35,
                     "waning_days": 365},
    },
    {
        "id":       "measles",
        "label_en": "Measles, pre-vaccine (Anderson & May 1991)",
        "label_sv": "Mässling, före vaccin (Anderson & May 1991)",
        "short":    "Measles",
        "icon":     "🔴",
        "ref":      "anderson-may-1991",
        "blurb_en": "Pre-vaccine measles: latent ~10–12 d, infectious ~8 d, "
                    "R₀ ≈ 12–18, CFR ≈ 0.1 % (high-income), lifelong immunity.",
        "blurb_sv": "Mässling före vaccin: latent ~10–12 d, smittsam ~8 d, "
                    "R₀ ≈ 12–18, CFR ≈ 0,1 % (höginkomst), livslång immunitet.",
        "params":   {"beta": 1.50, "sigma": 1/12, "gamma": 1/8, "mu": 0.001,
                     "waning_days": 365},
    },
    {
        "id":       "ebola_2014",
        "label_en": "Ebola Virus Disease (WHO Response Team 2014)",
        "label_sv": "Ebola Virus-sjukdom (WHO Response Team 2014)",
        "short":    "Ebola",
        "icon":     "☣",
        "ref":      "who-ebola-2014",
        "blurb_en": "EVD West Africa 2014: incubation ~9 d, infectious ~6 d, "
                    "CFR ≈ 70 %, R₀ ≈ 1.5–2.0.",
        "blurb_sv": "Ebola Västafrika 2014: inkubation ~9 d, smittsam ~6 d, "
                    "CFR ≈ 70 %, R₀ ≈ 1,5–2,0.",
        "params":   {"beta": 0.20, "sigma": 1/9, "gamma": 1/6, "mu": 0.70,
                     "waning_days": 365},
    },
    {
        "id":       "flu_1918",
        "label_en": "1918 H1N1 pandemic (Mills, Robins & Lipsitch 2004)",
        "label_sv": "Spanska sjukan 1918 (Mills, Robins & Lipsitch 2004)",
        "short":    "1918 Flu",
        "icon":     "📜",
        "ref":      "mills-2004-1918",
        "blurb_en": "1918 'Spanish flu': incubation ≈ 2 d, infectious ≈ 4 d, "
                    "R₀ ≈ 2.0–3.0, CFR ≈ 2.5 % in US/European cities.",
        "blurb_sv": "Spanska sjukan 1918: inkubation ≈ 2 d, smittsam ≈ 4 d, "
                    "R₀ ≈ 2,0–3,0, CFR ≈ 2,5 % i amerikanska/europeiska städer.",
        "params":   {"beta": 0.55, "sigma": 1/2, "gamma": 1/4, "mu": 0.025,
                     "waning_days": 365},
    },
]

VALID_PRESET_IDS = frozenset(p["id"] for p in PRESETS)


def get_presets() -> list[dict]:
    """Return preset metadata (params + citation), as a read-only copy."""
    return [dict(p, params=dict(p["params"])) for p in PRESETS]


def get_preset(pid: str) -> dict | None:
    for p in PRESETS:
        if p["id"] == pid:
            return dict(p, params=dict(p["params"]))
    return None


def default_preset() -> dict:
    """COVID-19 — most-cited SEIR baseline."""
    for p in PRESETS:
        if p.get("default"):
            return dict(p, params=dict(p["params"]))
    return dict(PRESETS[0], params=dict(PRESETS[0]["params"]))
