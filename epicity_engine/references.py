"""
references.py — Scientific references behind every parameter and feature.

Each entry has:
  id          : short anchor key (used by presets to cross-link)
  citation    : full citation (do NOT translate the title — it's the search key)
  doi         : stable identifier (DOI or canonical URL)
  context_en  : how this paper informed an EpiCity decision (English)
  context_sv  : same explanation in Swedish

Surfaced via GET /api/about and rendered in the About modal "References" tab.
"""
from __future__ import annotations

REFERENCES: list[dict] = [
    # ── Core SEIR & COVID-19 baseline parameters ────────────────────────────
    {
        "id": "kermack-1927",
        "citation": "Kermack WO, McKendrick AG. A contribution to the mathematical "
                    "theory of epidemics. Proc R Soc Lond A. 1927;115:700-721.",
        "doi": "10.1098/rspa.1927.0118",
        "context_en": "Original SIR/SEIR mass-action model — the mathematical "
                      "foundation EpiCity extends with spatial and temporal structure.",
        "context_sv": "Original SIR/SEIR-modellen — den matematiska grunden som "
                      "EpiCity bygger vidare på med spatial och temporal struktur.",
    },
    {
        "id": "li-2020-incubation",
        "citation": "Li Q, Guan X, Wu P, et al. Early Transmission Dynamics in Wuhan, "
                    "China, of Novel Coronavirus–Infected Pneumonia. NEJM. 2020;382:1199-1207.",
        "doi": "10.1056/NEJMoa2001316",
        "context_en": "Estimated mean incubation 5.2 d (95% CI 4.1–7.0). Drives the "
                      "default σ = 1/5 d⁻¹ and the COVID-19 preset in EpiCity.",
        "context_sv": "Uppskattade medelinkubation 5,2 d (95 % KI 4,1–7,0). Ligger till "
                      "grund för standardvärdet σ = 1/5 d⁻¹ och COVID-19-förinställningen.",
    },
    {
        "id": "verity-2020-cfr",
        "citation": "Verity R, Okell LC, Dorigatti I, et al. Estimates of the severity of "
                    "coronavirus disease 2019: a model-based analysis. Lancet Infect Dis. "
                    "2020;20(6):669-677.",
        "doi": "10.1016/S1473-3099(20)30243-7",
        "context_en": "Best-estimate IFR ≈ 0.66 % and CFR ≈ 1.4 % for SARS-CoV-2. "
                      "EpiCity default μ = 0.8 % sits in this range.",
        "context_sv": "Bästa skattning IFR ≈ 0,66 % och CFR ≈ 1,4 % för SARS-CoV-2. "
                      "EpiCitys standard μ = 0,8 % ligger i detta intervall.",
    },
    {
        "id": "byrne-2020-infectious-period",
        "citation": "Byrne AW, McEvoy D, Collins ÁB, et al. Inferred duration of "
                    "infectious period of SARS-CoV-2: rapid scoping review and analysis "
                    "of available evidence for asymptomatic and symptomatic COVID-19 cases. "
                    "BMJ Open. 2020;10:e039856.",
        "doi": "10.1136/bmjopen-2020-039856",
        "context_en": "Reviewed evidence for ~7–10 day infectious period. Justifies "
                      "γ = 1/10 d⁻¹ default in EpiCity.",
        "context_sv": "Genomgång av evidens för smittsam period på ~7–10 dagar. "
                      "Motiverar standardvärdet γ = 1/10 d⁻¹.",
    },
    {
        "id": "liu-2020-r0",
        "citation": "Liu Y, Gayle AA, Wilder-Smith A, Rocklöv J. The reproductive number "
                    "of COVID-19 is higher compared to SARS coronavirus. J Travel Med. "
                    "2020;27(2):taaa021.",
        "doi": "10.1093/jtm/taaa021",
        "context_en": "Pooled R₀ estimate ≈ 3.28 for ancestral SARS-CoV-2 — calibration "
                      "anchor for the COVID-19 preset's β/γ ratio.",
        "context_sv": "Sammanvägd R₀ ≈ 3,28 för ursprunglig SARS-CoV-2 — förankringspunkt "
                      "för COVID-19-förinställningens β/γ-kvot.",
    },

    # ── Spatial mixing kernel ───────────────────────────────────────────────
    {
        "id": "riley-2007-spatial",
        "citation": "Riley S. Large-Scale Spatial-Transmission Models of Infectious "
                    "Disease. Science. 2007;316(5829):1298-1301.",
        "doi": "10.1126/science.1134695",
        "context_en": "Reviews spatial kernels in epidemic models; supports EpiCity's "
                      "Gaussian neighbour kernel (σ = 150 m, cutoff 400 m).",
        "context_sv": "Granskning av spatiala kärnor i epidemimodeller; stödjer EpiCitys "
                      "gaussiska grannkärna (σ = 150 m, gräns 400 m).",
    },

    # ── Contact matrix ──────────────────────────────────────────────────────
    {
        "id": "mossong-2008-polymod",
        "citation": "Mossong J, Hens N, Jit M, et al. Social Contacts and Mixing Patterns "
                    "Relevant to the Spread of Infectious Diseases. PLoS Med. 2008;5(3):e74.",
        "doi": "10.1371/journal.pmed.0050074",
        "context_en": "POLYMOD survey — empirical age-stratified contact matrix used as "
                      "the basis for EpiCity's child/adult/elder C[g, g'] matrix.",
        "context_sv": "POLYMOD-undersökningen — empirisk åldersstratifierad kontaktmatris "
                      "som ligger till grund för EpiCitys C[g, g']-matris.",
    },

    # ── Per-mode transport β ────────────────────────────────────────────────
    {
        "id": "hu-2021-pnas",
        "citation": "Hu M, Lin H, Wang J, et al. Risk of Coronavirus Disease 2019 "
                    "Transmission in Train Passengers: An Epidemiological and Modeling Study. "
                    "Clin Infect Dis. 2021;72(4):604-610.",
        "doi": "10.1093/cid/ciaa1057",
        "context_en": "Quantified attack rates by seat distance on Chinese high-speed "
                      "trains: ≈ 0.32 %/h adjacent, ≈ 0.18 %/h same row. Justifies the "
                      "tram/train β values in transport.py.",
        "context_sv": "Mätte sekundär attack-rate efter sittavstånd i kinesiska "
                      "höghastighetståg: ≈ 0,32 %/h intill, ≈ 0,18 %/h samma rad. "
                      "Motiverar spårvagn/tåg-β i transport.py.",
    },
    {
        "id": "tirachini-2020",
        "citation": "Tirachini A, Cats O. COVID-19 and Public Transportation: Current "
                    "Assessment, Prospects, and Research Needs. J Public Transp. 2020;22(1):1-21.",
        "doi": "10.5038/2375-0901.22.1.1",
        "context_en": "Reviews ventilation and crowding in buses vs. trams vs. metros. "
                      "Buses have lower air-exchange than light rail → higher per-hour "
                      "transmission risk; basis for Bus β = 0.45 in transport.py.",
        "context_sv": "Översikt av ventilation/trängsel i bussar, spårvagnar, tunnelbana. "
                      "Bussar har lägre luftväxling → högre smittorisk per timme; grund "
                      "för Buss-β = 0,45 i transport.py.",
    },
    {
        "id": "chu-2020-lancet",
        "citation": "Chu DK, Akl EA, Duda S, et al. Physical distancing, face masks, and "
                    "eye protection to prevent person-to-person transmission of SARS-CoV-2 "
                    "and COVID-19: a systematic review and meta-analysis. Lancet. "
                    "2020;395(10242):1973-1987.",
        "doi": "10.1016/S0140-6736(20)31142-9",
        "context_en": "Meta-analysis: face masks reduce transmission risk by ~67 % "
                      "(adjusted OR 0.33). Drives EpiCity's mask intervention β × 0.50.",
        "context_sv": "Metaanalys: ansiktsmasker minskar smittrisken med ~67 % (justerad "
                      "OR 0,33). Underlag för EpiCitys masker-β × 0,50.",
    },
    {
        "id": "furuya-2007",
        "citation": "Furuya H. Risk of transmission of airborne influenza during a "
                    "domestic flight. Environ Health Prev Med. 2007;12(1):4-8.",
        "doi": "10.1007/BF02898187",
        "context_en": "In-cabin influenza attack-rate study supporting low per-hour β for "
                      "HEPA-filtered cabins; informs Flight β = 0.10 in transport.py.",
        "context_sv": "Studie av influensaspridning i kabin med HEPA-filtrering — stödjer "
                      "låg per-timme-β; underlag för Flyg-β = 0,10 i transport.py.",
    },

    # ── Climate / seasonality ───────────────────────────────────────────────
    {
        "id": "lowen-2014",
        "citation": "Lowen AC, Steel J. Roles of Humidity and Temperature in Shaping "
                    "Influenza Seasonality. J Virol. 2014;88(14):7692-7695.",
        "doi": "10.1128/JVI.03544-13",
        "context_en": "Cold, dry air dramatically increases respiratory virus transmission. "
                      "EpiCity scales β by exp(-k_T(T - T_ref)) — formula in climate.py.",
        "context_sv": "Kall, torr luft ökar dramatiskt smittspridning av luftvägsvirus. "
                      "EpiCity skalar β med exp(-k_T(T - T_ref)) — formel i climate.py.",
    },
    {
        "id": "wang-2021-bmj",
        "citation": "Wang J, Tang K, Feng K, et al. Impact of temperature and relative "
                    "humidity on the transmission of COVID-19: a modelling study in China "
                    "and the United States. BMJ Open. 2021;11:e043863.",
        "doi": "10.1136/bmjopen-2020-043863",
        "context_en": "Each +1 °C ≈ 3.1 % decrease in R; each +1 % RH ≈ 0.85 % decrease. "
                      "Calibrates k_T = 0.04 and k_RH = 0.004 in climate.py.",
        "context_sv": "Varje +1 °C ≈ 3,1 % minskning av R; +1 % RF ≈ 0,85 % minskning. "
                      "Kalibrerar k_T = 0,04 och k_RH = 0,004 i climate.py.",
    },
    {
        "id": "sajadi-2020",
        "citation": "Sajadi MM, Habibzadeh P, Vintzileos A, Shokouhi S, Miralles-Wilhelm F, "
                    "Amoroso A. Temperature, Humidity, and Latitude Analysis to Estimate "
                    "Potential Spread and Seasonality of Coronavirus Disease 2019. "
                    "JAMA Netw Open. 2020;3(6):e2011834.",
        "doi": "10.1001/jamanetworkopen.2020.11834",
        "context_en": "Latitudinal/temperature corridor analysis; supports the Swedish "
                      "annual cycle (mean 8 °C, ±11 °C) parametrised in climate.py.",
        "context_sv": "Latitud/temperaturanalys; stödjer den svenska årscykeln "
                      "(medelvärde 8 °C, amplitud ±11 °C) i climate.py.",
    },

    # ── School transmission and closure ─────────────────────────────────────
    {
        "id": "viner-2020-schools",
        "citation": "Viner RM, Russell SJ, Croker H, et al. School closure and management "
                    "practices during coronavirus outbreaks including COVID-19: a rapid "
                    "systematic review. Lancet Child Adolesc Health. 2020;4(5):397-404.",
        "doi": "10.1016/S2352-4642(20)30095-X",
        "context_en": "School-closure effect estimates: 2–4 % R-reduction alone, much "
                      "higher for influenza. Justifies the 'Close schools' intervention.",
        "context_sv": "Skolstängningens effekt: 2–4 % minskning av R isolerat, mycket "
                      "högre för influensa. Motiverar 'Stäng skolor'-åtgärden.",
    },

    # ── Lockdown / NPI effectiveness ────────────────────────────────────────
    {
        "id": "flaxman-2020",
        "citation": "Flaxman S, Mishra S, Gandy A, et al. Estimating the effects of "
                    "non-pharmaceutical interventions on COVID-19 in Europe. Nature. "
                    "2020;584:257-261.",
        "doi": "10.1038/s41586-020-2405-7",
        "context_en": "Estimated ≈ 81 % R-reduction from full lockdowns across 11 European "
                      "countries. Anchors EpiCity's lockdown β × 0.72 + mobility cut.",
        "context_sv": "Skattade ≈ 81 % minskning av R från fullständiga lockdowns i 11 "
                      "europeiska länder. Förankrar lockdown β × 0,72 + rörlighetsminskning.",
    },
    {
        "id": "brauner-2021",
        "citation": "Brauner JM, Mindermann S, Sharma M, et al. Inferring the effectiveness "
                    "of government interventions against COVID-19. Science. 2021;371:eabd9338.",
        "doi": "10.1126/science.abd9338",
        "context_en": "Bayesian analysis of NPIs across 41 countries: school+university "
                      "closure 38 %, gatherings ≤10 38 %, businesses 27 %, stay-home 13 % "
                      "additional R-reduction. Calibrates intervention strengths.",
        "context_sv": "Bayesiansk analys av NPI:er i 41 länder: skol-/universitetsstängning "
                      "38 %, sammankomster ≤10 personer 38 %, företag 27 %, stanna hemma "
                      "13 %. Kalibrerar åtgärdsstyrkor.",
    },

    # ── Vaccination ──────────────────────────────────────────────────────────
    {
        "id": "polack-2020",
        "citation": "Polack FP, Thomas SJ, Kitchin N, et al. Safety and Efficacy of the "
                    "BNT162b2 mRNA Covid-19 Vaccine. NEJM. 2020;383:2603-2615.",
        "doi": "10.1056/NEJMoa2034577",
        "context_en": "95 % vaccine efficacy against symptomatic disease — basis for "
                      "EpiCity treating vaccinated S as moving directly to R.",
        "context_sv": "95 % vaccineffekt mot symptomatisk sjukdom — grund för att "
                      "EpiCity flyttar vaccinerade S direkt till R.",
    },

    # ── Hospital capacity / mortality crowding ──────────────────────────────
    {
        "id": "kadri-2021",
        "citation": "Kadri SS, Sun J, Lawandi A, et al. Association Between Caseload "
                    "Surge and COVID-19 Survival in 558 U.S. Hospitals, March to August "
                    "2020. Ann Intern Med. 2021;174(9):1240-1251.",
        "doi": "10.7326/M21-1213",
        "context_en": "Mortality up to 2.0× higher during ICU surges. Underpins EpiCity's "
                      "μ_overloaded > μ when capacity is exceeded.",
        "context_sv": "Mortalitet upp till 2,0× högre vid IVA-överbelastning. Underbygger "
                      "EpiCitys μ_överbelastad > μ vid kapacitetsbrist.",
    },

    # ── Waning immunity / SEIRS ─────────────────────────────────────────────
    {
        "id": "townsend-2021",
        "citation": "Townsend JP, Hassler HB, Wang Z, et al. The durability of immunity "
                    "against reinfection by SARS-CoV-2: a comparative evolutionary study. "
                    "Lancet Microbe. 2021;2(12):e666-e675.",
        "doi": "10.1016/S2666-5247(21)00219-6",
        "context_en": "Modeled median time to reinfection ≈ 16 months from antibody "
                      "decay rates. Sets the upper bound for the immunity-waning slider.",
        "context_sv": "Modellerad mediantid till reinfektion ≈ 16 månader baserat på "
                      "antikroppsavtagning. Sätter övre gräns för immunitets-avtagandet.",
    },

    # ── Demography / country of birth ───────────────────────────────────────
    {
        "id": "scb-utrikes-fodda",
        "citation": "Statistiska centralbyrån (SCB). Befolkning efter födelseland och "
                    "ursprungsland. Statistikdatabasen, tabell BE0101E1.",
        "doi": "https://www.statistikdatabasen.scb.se/",
        "context_en": "Source for the country-of-birth distribution per Swedish "
                      "municipality, used by EpiCity's demography overlay.",
        "context_sv": "Källa för fördelning av födelseland per kommun, använd i "
                      "EpiCitys demografilager.",
    },
    {
        "id": "rostami-2022-covid-migrants",
        "citation": "Rostila M, Cederström A, Wallace M, et al. Disparities in Coronavirus "
                    "Disease 2019 Mortality by Country of Birth in Stockholm, Sweden: "
                    "A Total-Population–Based Cohort Study. Am J Epidemiol. 2022;191(8):1510-1518.",
        "doi": "10.1093/aje/kwab057",
        "context_en": "Documented elevated COVID-19 mortality among foreign-born residents "
                      "of Stockholm. Justifies tracking country-of-birth as an "
                      "epidemiological stratifier in EpiCity.",
        "context_sv": "Dokumenterade förhöjd covid-19-mortalitet bland utrikesfödda i "
                      "Stockholm. Motiverar att EpiCity stratifierar på födelseland.",
    },

    # ── Doubling-time methodology ───────────────────────────────────────────
    {
        "id": "wallinga-lipsitch-2007",
        "citation": "Wallinga J, Lipsitch M. How generation intervals shape the "
                    "relationship between growth rates and reproductive numbers. "
                    "Proc R Soc B. 2007;274:599-604.",
        "doi": "10.1098/rspb.2006.3754",
        "context_en": "Closed-form link between exponential growth rate r and R₀ via "
                      "the generation-interval distribution. Method behind EpiCity's "
                      "doubling-time and growth-rate displays.",
        "context_sv": "Slutet samband mellan exponentiell tillväxttakt r och R₀ via "
                      "generationsintervallets fördelning. Metoden bakom EpiCitys "
                      "fördubblingstid och tillväxttakt.",
    },

    # ── Pathogen presets ────────────────────────────────────────────────────
    {
        "id": "fraser-2009",
        "citation": "Fraser C, Donnelly CA, Cauchemez S, et al. Pandemic Potential "
                    "of a Strain of Influenza A (H1N1). Science. 2009;324(5934):1557-1561.",
        "doi": "10.1126/science.1176062",
        "context_en": "Real-time estimate of pandemic H1N1 2009: R₀ ≈ 1.4–1.6, incubation "
                      "≈ 2 d, infectious ≈ 4 d, CFR ≈ 0.4 %. Source for the H1N1 preset.",
        "context_sv": "Realtidsuppskattning av pandemisk H1N1 2009: R₀ ≈ 1,4–1,6, "
                      "inkubation ≈ 2 d, smittsam ≈ 4 d, CFR ≈ 0,4 %. Källa för H1N1-förinställningen.",
    },
    {
        "id": "lipsitch-2003",
        "citation": "Lipsitch M, Cohen T, Cooper B, et al. Transmission Dynamics and "
                    "Control of Severe Acute Respiratory Syndrome. Science. 2003;300(5627):1966-1970.",
        "doi": "10.1126/science.1086616",
        "context_en": "SARS-CoV-1 (2003): incubation ≈ 4 d, infectious ≈ 8 d, CFR ≈ 9.6 %, "
                      "R₀ ≈ 2.2–3.6 before isolation. Source for the SARS 2003 preset.",
        "context_sv": "SARS-CoV-1 (2003): inkubation ≈ 4 d, smittsam ≈ 8 d, CFR ≈ 9,6 %, "
                      "R₀ ≈ 2,2–3,6 före isolering. Källa för förinställningen SARS 2003.",
    },
    {
        "id": "anderson-may-1991",
        "citation": "Anderson RM, May RM. Infectious Diseases of Humans: Dynamics and "
                    "Control. Oxford University Press, 1991.",
        "doi": "https://global.oup.com/academic/product/infectious-diseases-of-humans-9780198540403",
        "context_en": "Foundational textbook for SEIR-style infectious-disease dynamics. "
                      "Source for the Measles preset (R₀ ≈ 12–18, latent ~10–12 d, "
                      "infectious ~8 d, lifelong immunity).",
        "context_sv": "Grundläggande lärobok om SEIR-dynamik. Källa för förinställningen "
                      "Mässling (R₀ ≈ 12–18, latent ~10–12 d, smittsam ~8 d).",
    },
    {
        "id": "who-ebola-2014",
        "citation": "WHO Ebola Response Team. Ebola Virus Disease in West Africa — "
                    "The First 9 Months of the Epidemic and Forward Projections. "
                    "NEJM. 2014;371(16):1481-1495.",
        "doi": "10.1056/NEJMoa1411100",
        "context_en": "EVD West Africa 2014: incubation ≈ 9 d, infectious ≈ 6 d, CFR ≈ 70 %, "
                      "R₀ ≈ 1.5–2.0. Source for the Ebola 2014 preset.",
        "context_sv": "Ebola Västafrika 2014: inkubation ≈ 9 d, smittsam ≈ 6 d, CFR ≈ 70 %, "
                      "R₀ ≈ 1,5–2,0. Källa för förinställningen Ebola 2014.",
    },
    {
        "id": "cauchemez-2014",
        "citation": "Cauchemez S, Fraser C, Van Kerkhove MD, et al. Middle East "
                    "respiratory syndrome coronavirus: quantification of the extent "
                    "of the epidemic, surveillance biases, and transmissibility. "
                    "Lancet Infect Dis. 2014;14(1):50-56.",
        "doi": "10.1016/S1473-3099(13)70304-9",
        "context_en": "MERS-CoV characterisation (Arabian Peninsula 2012–2014): R₀ ≈ "
                      "0.6–0.9 in the community, CFR ≈ 35 %, mostly hospital-amplified "
                      "transmission. Source for the MERS-CoV preset.",
        "context_sv": "Karakterisering av MERS-CoV (Arabiska halvön 2012–2014): R₀ ≈ "
                      "0,6–0,9 i samhället, CFR ≈ 35 %, främst sjukhusförstärkt smitta. "
                      "Källa för förinställningen MERS-CoV.",
    },
    {
        "id": "mills-2004-1918",
        "citation": "Mills CE, Robins JM, Lipsitch M. Transmissibility of 1918 pandemic "
                    "influenza. Nature. 2004;432(7019):904-906.",
        "doi": "10.1038/nature03063",
        "context_en": "Re-analysis of 1918 'Spanish flu' city-level data: R₀ ≈ 2.0–3.0, "
                      "CFR ≈ 2.5 %. Canonical reference for severe pandemic influenza; "
                      "source for the 1918 Flu preset.",
        "context_sv": "Återanalys av spanska sjukan 1918 på stadsnivå: R₀ ≈ 2,0–3,0, "
                      "CFR ≈ 2,5 %. Kanonisk referens för svår pandemisk influensa; "
                      "källa för förinställningen 1918 Flu.",
    },
]


def get_references() -> list[dict]:
    """Return the full reference list (read-only copy)."""
    return [dict(r) for r in REFERENCES]
