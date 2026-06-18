/**
 * i18n.js — Internationalisation module for EpiCity.
 *
 * Two languages: English (en) and Swedish (sv).
 * Language choice persists in localStorage.
 *
 * Usage:
 *   import { t, setLang, getLang, onLangChange } from './i18n.js';
 *   t('play')            // "Play" or "Spela"
 *   t('day_n', { n: 5 }) // "Day 5" or "Dag 5"
 */

// ── State ────────────────────────────────────────────────────────────────────

let _lang = localStorage.getItem('epicityLang') || 'en';
const _listeners = [];

export function getLang() { return _lang; }

export function setLang(lang) {
  if (lang !== 'en' && lang !== 'sv') return;
  _lang = lang;
  localStorage.setItem('epicityLang', lang);
  _listeners.forEach(fn => fn(lang));
}

/** Register a callback fired whenever the language changes. */
export function onLangChange(fn) { _listeners.push(fn); }

/**
 * Look up a translation key. Supports simple {{var}} interpolation.
 * Falls back to English if the key is missing in the current language.
 */
export function t(key, vars) {
  const s = (_strings[_lang] && _strings[_lang][key]) || _strings.en[key] || key;
  if (!vars) return s;
  return s.replace(/\{\{(\w+)\}\}/g, (_, k) => vars[k] ?? '');
}

// ── Translation strings ──────────────────────────────────────────────────────

const _strings = { en: {}, sv: {} };
const en = _strings.en;
const sv = _strings.sv;

// ── App title & navigation ──────────────────────────────────────────────────
en.app_title         = 'EpiCity';
sv.app_title         = 'EpiCity';
en.app_title_sweden  = 'EpiCity — Sweden';
sv.app_title_sweden  = 'EpiCity — Sverige';
en.macro_subtitle    = 'Click an available city to enter its simulation';
sv.macro_subtitle    = 'Klicka på en tillgänglig stad för att starta simuleringen';
en.back_to_sweden    = 'Back to Sweden map';
sv.back_to_sweden    = 'Tillbaka till Sverigekartan';
en.return_to_sweden  = 'Return to Sweden view';
sv.return_to_sweden  = 'Återgå till Sverigevyn';

// ── Loading ─────────────────────────────────────────────────────────────────
en.loading           = 'Loading…';
sv.loading           = 'Laddar…';
en.building_city     = 'Building the city';
sv.building_city     = 'Bygger upp staden';
en.loading_city      = 'Loading {{name}}…';
sv.loading_city      = 'Laddar {{name}}…';
en.returning_sweden  = 'Returning to Sweden…';
sv.returning_sweden  = 'Återgår till Sverige…';
en.saving_state      = 'Saving city state';
sv.saving_state      = 'Sparar stadstillstånd';

// ── Buttons ─────────────────────────────────────────────────────────────────
en.play              = '▶ Play';
sv.play              = '▶ Spela';
en.pause             = '⏸ Pause';
sv.pause             = '⏸ Paus';
en.reset             = '↺ Reset';
sv.reset             = '↺ Återställ';
en.btn_2d            = '⊙ 2D';
sv.btn_2d            = '⊙ 2D';
en.areas_off         = '🏘 Areas: Off';
sv.areas_off         = '🏘 Områden: Av';
en.areas_on          = '🏘 Areas: On';
sv.areas_on          = '🏘 Områden: På';
en.daynight          = '☀ Day/Night';
sv.daynight          = '☀ Dag/Natt';
en.daynight_label    = 'Day/Night';
sv.daynight_label    = 'Dag/Natt';
en.daynight_title    = 'Day/Night shadows';
sv.daynight_title    = 'Dag/natt-skuggor';
en.daynight_snow     = 'Day/Night — snowing';
sv.daynight_snow     = 'Dag/Natt — snöar';
en.daynight_rain     = 'Day/Night — raining';
sv.daynight_rain     = 'Dag/Natt — regnar';
en.daynight_cloud    = 'Day/Night — partly cloudy';
sv.daynight_cloud    = 'Dag/Natt — delvis molnigt';
en.flow_off          = '≈ Flow: Off';
sv.flow_off          = '≈ Flöde: Av';
en.flow_on           = '≈ Flow: On';
sv.flow_on           = '≈ Flöde: På';
en.nature_off        = '🌲 Nature: Off';
sv.nature_off        = '🌲 Natur: Av';
en.nature_on         = '🌲 Nature: On';
sv.nature_on         = '🌲 Natur: På';
en.nature_title      = 'Show water effect + 3D trees on lakes and forests';
sv.nature_title      = 'Visa vatteneffekt + 3D-träd på sjöar och skogar';
en.transport_off     = '🚆 Transport: Off';
sv.transport_off     = '🚆 Transport: Av';
en.transport_on      = '🚆 Transport: On';
sv.transport_on      = '🚆 Transport: På';
en.pois              = '📍 POIs';
sv.pois              = '📍 Platser';
en.pois_title        = 'Show points of interest';
sv.pois_title        = 'Visa intressepunkter';
en.settings          = '⚙ Settings';
sv.settings          = '⚙ Inställningar';
en.about             = '📖 About';
sv.about             = '📖 Om';
en.about_title       = 'About EpiCity';
sv.about_title       = 'Om EpiCity';
en.replay            = '▶ Run';
sv.replay            = '▶ Kör';
en.replay_title      = 'Run history replay from day 0';
sv.replay_title      = 'Kör historikuppspelning från dag 0';

// ── Comparison view ─────────────────────────────────────────────────────────
en.compare           = '⇆ Compare';
sv.compare           = '⇆ Jämför';
en.compare_two_days  = 'Comparison view';
sv.compare_two_days  = 'Jämförelsevy';
en.compare_title     = 'Comparison view (two days from history)';
sv.compare_title     = 'Jämförelsevy (två dagar från historiken)';
en.compare_off       = 'Exit compare';
sv.compare_off       = 'Avsluta jämförelse';
en.day_a             = 'Day A';
sv.day_a             = 'Dag A';
en.day_b             = 'Day B';
sv.day_b             = 'Dag B';
en.mode_diff         = 'Diff map';
sv.mode_diff         = 'Skillnadskarta';
en.mode_split        = 'Side by side';
sv.mode_split        = 'Sida vid sida';
en.compare_hint      = 'Drag the two thumbs to pick Day A and Day B';
sv.compare_hint      = 'Dra de två reglagen för att välja Dag A och Dag B';
en.compare_summary   = 'A: Day {{a}} · B: Day {{b}}';
sv.compare_summary   = 'A: Dag {{a}} · B: Dag {{b}}';
en.compare_delta     = 'Δ {{metric}}: {{sign}}{{val}}';
sv.compare_delta     = 'Δ {{metric}}: {{sign}}{{val}}';
en.compare_legend_inc = 'Increased';
sv.compare_legend_inc = 'Ökat';
en.compare_legend_dec = 'Decreased';
sv.compare_legend_dec = 'Minskat';
en.compare_legend_eq  = 'Unchanged';
sv.compare_legend_eq  = 'Oförändrat';
en.show_all          = 'Show all';
sv.show_all          = 'Visa alla';
en.hide_all          = 'Hide all';
sv.hide_all          = 'Dölj alla';
en.previous          = 'Previous';
sv.previous          = 'Föregående';
en.next              = 'Next';
sv.next              = 'Nästa';
en.prev_in_group     = 'Previous in group';
sv.prev_in_group     = 'Föregående i grupp';
en.next_in_group     = 'Next in group';
sv.next_in_group     = 'Nästa i grupp';

// ── Speed pips ──────────────────────────────────────────────────────────────
en.speed_1           = '1×';  sv.speed_1 = '1×';
en.speed_2           = '2×';  sv.speed_2 = '2×';
en.speed_3           = '3×';  sv.speed_3 = '3×';

// ── Right panel ─────────────────────────────────────────────────────────────
en.population        = 'Population';
sv.population        = 'Befolkning';
en.susceptible       = 'Susceptible';
sv.susceptible       = 'Mottagliga';
en.exposed           = 'Exposed';
sv.exposed           = 'Exponerade';
en.infectious        = 'Infectious';
sv.infectious        = 'Smittsamma';
en.recovered         = 'Recovered';
sv.recovered         = 'Tillfrisknade';
en.deaths            = 'Deaths';
sv.deaths            = 'Dödsfall';
en.total_cases       = 'Total Cases';
sv.total_cases       = 'Totala fall';
en.hospital_alert    = '⚠ Hospitals overwhelmed — mortality rising';
sv.hospital_alert    = '⚠ Sjukhus överväldigade — dödligheten ökar';
en.epidemic_curve    = 'Epidemic Curve';
sv.epidemic_curve    = 'Epidemikurva';
en.interventions     = 'Interventions';
sv.interventions     = 'Åtgärder';
en.city_legend       = 'City Legend';
sv.city_legend       = 'Stadsförklaring';
en.day_n             = 'Day {{n}}';
sv.day_n             = 'Dag {{n}}';

// ── Population info tooltip ─────────────────────────────────────────────────
en.seir_title        = 'SEIR Compartments';
sv.seir_title        = 'SEIR-fack';
en.seir_s_desc       = 'Susceptible — not yet infected';
sv.seir_s_desc       = 'Mottagliga — ännu inte smittade';
en.seir_e_desc       = 'Exposed — incubating (~5 days)';
sv.seir_e_desc       = 'Exponerade — inkuberar (~5 dagar)';
en.seir_i_desc       = 'Infectious — actively spreading';
sv.seir_i_desc       = 'Smittsamma — sprider aktivt';
en.seir_r_desc       = 'Recovered — immune';
sv.seir_r_desc       = 'Tillfrisknade — immuna';
en.seir_d_desc       = 'Deaths';
sv.seir_d_desc       = 'Dödsfall';
en.reff_title        = 'R<sub>eff</sub> — Effective Reproduction Number';
sv.reff_title        = 'R<sub>eff</sub> — Effektivt reproduktionstal';
en.reff_growing      = 'R > 1 — epidemic is growing';
sv.reff_growing      = 'R > 1 — epidemin växer';
en.reff_stable       = 'R = 1 — epidemic is stable';
sv.reff_stable       = 'R = 1 — epidemin är stabil';
en.reff_shrinking    = 'R < 1 — epidemic is shrinking';
sv.reff_shrinking    = 'R < 1 — epidemin minskar';

// ── Time of day ─────────────────────────────────────────────────────────────
en.tod_night         = 'Night';
sv.tod_night         = 'Natt';
en.tod_rush_morning  = 'Morning Rush';
sv.tod_rush_morning  = 'Morgonrusning';
en.tod_workday       = 'Working Day';
sv.tod_workday       = 'Arbetsdag';
en.tod_rush_evening  = 'Evening Rush';
sv.tod_rush_evening  = 'Kvällsrusning';

// ── Building tooltip ────────────────────────────────────────────────────────
en.neighborhood      = 'Neighborhood';
sv.neighborhood      = 'Stadsdel';
en.income_label      = 'Income';
sv.income_label      = 'Inkomst';
en.age_breakdown     = 'Age breakdown';
sv.age_breakdown     = 'Åldersfördelning';
en.age_child         = 'Child';
sv.age_child         = 'Barn';
en.age_adult         = 'Adult';
sv.age_adult         = 'Vuxen';
en.age_elder         = 'Elder';
sv.age_elder         = 'Äldre';
en.prevalence        = 'Prevalence';
sv.prevalence        = 'Prevalens';
en.surface           = 'Surface';
sv.surface           = 'Yta';
en.households        = 'Households';
sv.households        = 'Hushåll';
en.buildings         = 'Buildings';
sv.buildings         = 'Byggnader';
en.gender            = 'Gender';
sv.gender            = 'Kön';
en.children          = 'Children';
sv.children          = 'Barn';
en.adults            = 'Adults';
sv.adults            = 'Vuxna';
en.elderly           = 'Elderly';
sv.elderly           = 'Äldre';
en.avg_income        = 'Avg income';
sv.avg_income        = 'Medelinkomst';

// ── Tree species ────────────────────────────────────────────────────────────
en.tree_spruce       = 'Spruce (Gran)';
sv.tree_spruce       = 'Gran';
en.tree_pine         = 'Pine (Tall)';
sv.tree_pine         = 'Tall';
en.tree_birch        = 'Birch (Björk)';
sv.tree_birch        = 'Björk';
en.tree_oak          = 'Oak (Ek)';
sv.tree_oak          = 'Ek';
en.tree_mixed        = 'Mixed stand';
sv.tree_mixed        = 'Blandskog';

// ── Transport panel ─────────────────────────────────────────────────────────
en.highlight_routes  = 'Highlight Routes';
sv.highlight_routes  = 'Markera linjer';
en.train_lines       = 'Train lines';
sv.train_lines       = 'Tåglinjer';
en.tram_lines        = 'Tram lines';
sv.tram_lines        = 'Spårvagnslinjer';
en.ferry_routes      = 'Ferry routes';
sv.ferry_routes      = 'Färjelinjer';
en.bus_lines         = 'Bus lines';
sv.bus_lines         = 'Busslinjer';
en.transport_network = 'Transport Network';
sv.transport_network = 'Transportnätverk';

// ── POI panel ───────────────────────────────────────────────────────────────
en.points_of_interest = 'Points of Interest';
sv.points_of_interest = 'Intressepunkter';

// ── Legend ───────────────────────────────────────────────────────────────────
en.color_by          = 'Color by';
sv.color_by          = 'Färg efter';
en.height_by         = 'Height by';
sv.height_by         = 'Höjd efter';
en.scale             = 'Scale';
sv.scale             = 'Skala';
en.log_scale         = 'Log scale';
sv.log_scale         = 'Logaritmisk skala';
en.invert_scale      = 'Invert scale';
sv.invert_scale      = 'Invertera skala';
en.zone_type         = 'Zone Type';
sv.zone_type         = 'Zontyp';
en.none_grey         = 'None (grey)';
sv.none_grey         = 'Ingen (grå)';
en.infection         = 'Infection';
sv.infection         = 'Infektion';
// "Where infections happened" — paints/heightens each building by the
// count of S→E transmission events recorded AT that location (the
// `location` field of the ABM infection log). Distinct from `infection`
// above, which paints buildings by the count of infected residents
// (their home_building). Only meaningful under the ABM engine; the
// compartmental engine doesn't track per-event locations and all cells
// fall back to 0.
en.infections_here   = 'Infection sites';
sv.infections_here   = 'Smittplatser';
// Hover-tooltip labels (clearer in the per-object context than the
// short legend label above). On a building: "how many times has a S→E
// transmission been recorded at THIS site". On a transport tooltip:
// the city-wide fleet total (the 24 ABM commute pools aren't mapped
// to individual lines, so the count is the same regardless of which
// vehicle is hovered — the label says "in transit (fleet)" to make
// that scope honest).
en.tooltip_infections_here     = 'Transmissions here';
sv.tooltip_infections_here     = 'Smittor här';
en.tooltip_transit_transmissions = 'Transmissions on this route';
sv.tooltip_transit_transmissions = 'Smittor på denna linje';
en.income            = 'Income';
sv.income            = 'Inkomst';
en.zone_type_real    = 'Zone Type (real)';
sv.zone_type_real    = 'Zontyp (verklig)';

// Legend gradient labels
en.grad_0            = '0';              sv.grad_0 = '0';
en.grad_max_inf      = 'Max infected / building';
sv.grad_max_inf      = 'Max smittade / byggnad';
en.grad_max_infections_here = 'Max transmissions / building';
sv.grad_max_infections_here = 'Max smittor / byggnad';
en.grad_empty        = 'Empty';
sv.grad_empty        = 'Tom';
en.grad_dense        = 'Dense';
sv.grad_dense        = 'Tät';
en.grad_max_deaths   = 'Max deaths / building';
sv.grad_max_deaths   = 'Max dödsfall / byggnad';
en.grad_max_exp      = 'Max exposed / building';
sv.grad_max_exp      = 'Max exponerade / byggnad';
en.grad_max_sus      = 'Max susceptible / building';
sv.grad_max_sus      = 'Max mottagliga / byggnad';
en.grad_max_rec      = 'Max recovered / building';
sv.grad_max_rec      = 'Max tillfrisknade / byggnad';
en.exposed           = 'Exposed';        sv.exposed     = 'Exponerade';
en.susceptible       = 'Susceptible';    sv.susceptible = 'Mottagliga';
en.recovered         = 'Recovered';      sv.recovered   = 'Tillfrisknade';
en.prevalence        = 'Prevalence';     sv.prevalence  = 'Prevalens';
en.normalize_by      = 'Max scale';      sv.normalize_by= 'Maxskala';
en.norm_live         = 'Live max';       sv.norm_live   = 'Aktuell topp';
en.norm_cumulative   = 'Cumulative max'; sv.norm_cumulative = 'Kumulativ topp';
en.norm_monotonic    = 'Cumulative reach';
sv.norm_monotonic    = 'Kumulativ räckvidd';
en.norm_capacity     = 'Building capacity'; sv.norm_capacity = 'Byggnadskapacitet';
en.norm_total        = 'City total';     sv.norm_total  = 'Stadens totalt';
en.show_all_on_map   = 'Show all on map'; sv.show_all_on_map = 'Visa alla på kartan';
en.pick_patient_zero = 'Pick Patient Zero'; sv.pick_patient_zero = 'Välj patient noll';
en.controls          = 'Controls';       sv.controls    = 'Kontroller';
en.search_buildings  = 'Search buildings, POIs… (try \'church\' / \'kyrka\')';
sv.search_buildings  = 'Sök byggnader, POI… (testa \u2018kyrka\u2019 / \u2018church\u2019)';
en.search_places     = 'Search places or categories…';
sv.search_places     = 'Sök platser eller kategorier…';
en.search_cities     = 'Search cities, regions…';
sv.search_cities     = 'Sök städer, regioner…';
en.search_hint       = 'Type a building or POI name…';
sv.search_hint       = 'Skriv ett byggnads- eller POI-namn…';
en.search_no_match   = 'No matches.';
sv.search_no_match   = 'Inga träffar.';
en.about             = 'About';          sv.about       = 'Om';
en.about_title       = 'About EpiCity';  sv.about_title = 'Om EpiCity';
en.wk_mon            = 'Mon';            sv.wk_mon      = 'Mån';
en.wk_tue            = 'Tue';            sv.wk_tue      = 'Tis';
en.wk_wed            = 'Wed';            sv.wk_wed      = 'Ons';
en.wk_thu            = 'Thu';            sv.wk_thu      = 'Tor';
en.wk_fri            = 'Fri';            sv.wk_fri      = 'Fre';
en.wk_sat            = 'Sat';            sv.wk_sat      = 'Lör';
en.wk_sun            = 'Sun';            sv.wk_sun      = 'Sön';
en.grad_low          = 'Low';
sv.grad_low          = 'Låg';
en.grad_high_sek     = 'High (SEK thousands)';
sv.grad_high_sek     = 'Hög (tusen SEK)';

// Legend group labels
en.grp_residential   = 'Residential';       sv.grp_residential = 'Bostäder';
en.grp_health        = 'Health & Safety';   sv.grp_health = 'Hälsa & Säkerhet';
en.grp_education     = 'Education';         sv.grp_education = 'Utbildning';
en.grp_daily         = 'Daily Life';        sv.grp_daily = 'Vardagsliv';
en.grp_culture       = 'Culture';           sv.grp_culture = 'Kultur';
en.grp_nature        = 'Nature';            sv.grp_nature = 'Natur';
en.grp_transport     = 'Transport';         sv.grp_transport = 'Transport';
en.grp_commercial    = 'Commercial';        sv.grp_commercial = 'Kommersiell';
en.grp_industrial    = 'Industrial';        sv.grp_industrial = 'Industriell';

// ── Settings ────────────────────────────────────────────────────────────────
en.model_parameters  = 'Model Parameters';
sv.model_parameters  = 'Modellparametrar';
en.scenario_presets  = 'Scenario Presets';
sv.scenario_presets  = 'Scenarioförval';
en.transmission      = 'Transmission (β)';
sv.transmission      = 'Smittöverföring (β)';
en.incubation_period = 'Incubation period';
sv.incubation_period = 'Inkubationstid';
en.infectious_period = 'Infectious period';
sv.infectious_period = 'Smittsam period';
en.case_fatality     = 'Case fatality rate';
sv.case_fatality     = 'Dödlighet';
en.immunity_waning   = 'Immunity waning';
sv.immunity_waning   = 'Immunitetsavtagande';
en.off_permanent     = 'Off (permanent)';
sv.off_permanent     = 'Av (permanent)';
en.n_days            = '{{n}} days';
sv.n_days            = '{{n}} dagar';
en.preset_mild       = 'Mild Flu';
sv.preset_mild       = 'Mild influensa';
en.preset_moderate   = 'Moderate';
sv.preset_moderate   = 'Måttlig';
en.preset_severe     = 'Severe Pandemic';
sv.preset_severe     = 'Svår pandemi';
en.preset_mild_desc  = 'Low transmission, short incubation, low fatality';
sv.preset_mild_desc  = 'Låg smittsamhet, kort inkubation, låg dödlighet';
en.preset_mod_desc   = 'Default balanced scenario';
sv.preset_mod_desc   = 'Standardscenario i balans';
en.preset_sev_desc   = 'High transmission, long infectious, high fatality';
sv.preset_sev_desc   = 'Hög smittsamhet, lång smittoperiod, hög dödlighet';
en.simulation_mode   = 'Simulation mode';
sv.simulation_mode   = 'Simuleringsläge';
en.deterministic     = 'Deterministic';
sv.deterministic     = 'Deterministisk';
en.stochastic        = 'Stochastic';
sv.stochastic        = 'Stokastisk';
en.random            = 'Random';
sv.random            = 'Slumpmässig';
en.seed              = 'Seed';
sv.seed              = 'Frö';
en.randomise_seed    = 'Randomise seed';
sv.randomise_seed    = 'Slumpa frö';
en.pick_patient_zero = 'Pick patient zero';
sv.pick_patient_zero = 'Välj patient noll';
en.seed_hint         = 'Changes reset the simulation. Seed controls reproducibility of stochastic runs and of the random patient zero pick.';
sv.seed_hint         = 'Ändringar återställer simuleringen. Fröet styr reproducerbarheten för stokastiska körningar och för slumpmässigt val av patient noll.';

// ── Interventions ───────────────────────────────────────────────────────────
en.iv_lockdown       = 'Lockdown';
sv.iv_lockdown       = 'Nedstängning';
en.iv_lockdown_desc  = 'Restrict movement between zones';
sv.iv_lockdown_desc  = 'Begränsa rörelse mellan zoner';
en.iv_masks          = 'Mask Mandate';
sv.iv_masks          = 'Munskyddskrav';
en.iv_masks_desc     = 'Reduce droplet transmission ~50%';
sv.iv_masks_desc     = 'Minska droppsmittning ~50%';
en.iv_distancing     = 'Social Distancing';
sv.iv_distancing     = 'Social distansering';
en.iv_distancing_desc= 'Reduce contacts and movement';
sv.iv_distancing_desc= 'Minska kontakter och rörelse';
en.iv_vaccination    = 'Vaccination Drive';
sv.iv_vaccination    = 'Vaccinationskampanj';
en.iv_vaccination_desc='S → R at 1.5% per day';
sv.iv_vaccination_desc='S → R med 1,5% per dag';
en.iv_schools        = 'Close Schools';
sv.iv_schools        = 'Stäng skolor';
en.iv_schools_desc   = 'Schools shut — transmission drops 80%';
sv.iv_schools_desc   = 'Skolor stängs — smittspridning minskar 80%';
en.iv_testing        = 'Test & Isolate';
sv.iv_testing        = 'Testa & isolera';
en.iv_testing_desc   = 'Identify and isolate infectious faster';
sv.iv_testing_desc   = 'Identifiera och isolera smittsamma snabbare';
en.iv_transit_masks  = 'Transit Masks';
sv.iv_transit_masks  = 'Munskydd i kollektivtrafik';
en.iv_transit_masks_desc = 'Masks on public transport — halve in-vehicle transmission';
sv.iv_transit_masks_desc = 'Munskydd i kollektivtrafik — halverar smittspridning ombord';
en.iv_suspend_flights= 'Suspend Flights';
sv.iv_suspend_flights= 'Stoppa flyg';
en.iv_suspend_flights_desc = 'Ground all flights — stop airborne intercity spread';
sv.iv_suspend_flights_desc = 'Ställ in alla flygningar — stoppa luftburen smittspridning';
en.iv_screening      = 'Station Screening';
sv.iv_screening      = 'Stationskontroll';
en.iv_screening_desc = 'Screen passengers at stops — catch ~40% infectious';
sv.iv_screening_desc = 'Kontrollera resenärer vid hållplatser — fångar ~40% smittsamma';

// ── Macro hover card ────────────────────────────────────────────────────────
en.click_to_start    = 'Click to start simulation';
sv.click_to_start    = 'Klicka för att starta simulering';
en.simulation_day    = 'Simulation — Day {{n}}';
sv.simulation_day    = 'Simulering — Dag {{n}}';

// ── POI category names ──────────────────────────────────────────────────────
en.poi_hospital      = 'Hospital';         sv.poi_hospital = 'Sjukhus';
en.poi_pharmacy      = 'Pharmacy';         sv.poi_pharmacy = 'Apotek';
en.poi_dental        = 'Dental';           sv.poi_dental = 'Tandvård';
en.poi_veterinary    = 'Veterinary';       sv.poi_veterinary = 'Veterinär';
en.poi_kindergarten  = 'Kindergarten';     sv.poi_kindergarten = 'Förskola';
en.poi_school        = 'Primary school';   sv.poi_school = 'Grundskola';
en.poi_high_school   = 'High school';      sv.poi_high_school = 'Gymnasium';
en.poi_university    = 'University';       sv.poi_university = 'Universitet';
en.poi_library       = 'Library';          sv.poi_library = 'Bibliotek';
en.poi_grocery       = 'Grocery';          sv.poi_grocery = 'Livsmedel';
en.poi_park          = 'Park';             sv.poi_park = 'Park';
en.poi_restaurant    = 'Restaurant';       sv.poi_restaurant = 'Restaurang';
en.poi_community     = 'Community centre'; sv.poi_community = 'Kulturhus';
en.poi_mall          = 'Shopping mall';    sv.poi_mall = 'Köpcentrum';
en.poi_church        = 'Church';           sv.poi_church = 'Kyrka';
en.poi_cemetery      = 'Cemetery';         sv.poi_cemetery = 'Kyrkogård';
en.poi_castle        = 'Castle';           sv.poi_castle = 'Slott';
en.poi_museum        = 'Museum';           sv.poi_museum = 'Museum';
en.poi_theater       = 'Theater';          sv.poi_theater = 'Teater';
en.poi_stadium       = 'Stadium';          sv.poi_stadium = 'Arena';
en.poi_manor         = 'Manor';            sv.poi_manor = 'Herrgård';
en.poi_historic      = 'Historic landmark';sv.poi_historic = 'Historiskt landmärke';
en.poi_lake          = 'Lake';             sv.poi_lake = 'Sjö';
en.poi_water_lake    = 'Lake';             sv.poi_water_lake = 'Sjö';
en.poi_water_river   = 'River';            sv.poi_water_river = 'Å/Älv';
en.poi_water_pond    = 'Pond';             sv.poi_water_pond = 'Damm';
en.poi_water_reservoir='Reservoir';        sv.poi_water_reservoir = 'Reservoar';
en.poi_water_sea     = 'Sea';             sv.poi_water_sea = 'Hav';
en.poi_forest        = 'Forest';           sv.poi_forest = 'Skog';
en.poi_nature_reserve= 'Nature reserve';   sv.poi_nature_reserve = 'Naturreservat';
en.poi_nightclub     = 'Nightclub';        sv.poi_nightclub = 'Nattklubb';
en.poi_playground    = 'Playground';       sv.poi_playground = 'Lekplats';
en.poi_sports_centre = 'Sports centre';    sv.poi_sports_centre = 'Sportcenter';
en.poi_hotel         = 'Hotel';            sv.poi_hotel = 'Hotell';
en.poi_airport       = 'Airport';          sv.poi_airport = 'Flygplats';
en.poi_train_station = 'Train Station';    sv.poi_train_station = 'Tågstation';
en.poi_bus_station   = 'Bus Station';      sv.poi_bus_station = 'Busstation';
en.poi_tram_stop     = 'Tram Stop';        sv.poi_tram_stop = 'Spårvagnshållplats';
en.poi_ferry_terminal= 'Ferry Terminal';   sv.poi_ferry_terminal = 'Färjeterminal';
en.poi_bus_stop      = 'Bus Stop';         sv.poi_bus_stop = 'Busshållplats';
en.poi_parking       = 'Parking';          sv.poi_parking = 'Parkering';
en.poi_police        = 'Police';           sv.poi_police = 'Polis';
en.poi_fire_station  = 'Fire Station';     sv.poi_fire_station = 'Brandstation';
en.poi_swedish_flag  = 'Swedish flag';     sv.poi_swedish_flag = 'Svensk flagga';
en.poi_lnu_flag      = 'LNU flag';         sv.poi_lnu_flag = 'LNU-flagga';
en.poi_patient_zero  = 'Patient Zero';     sv.poi_patient_zero = 'Patient noll';
// Swedish easter eggs
en.poi_dalahast      = 'Dala Horse';       sv.poi_dalahast = 'Dalahäst';
en.poi_ikea_bag      = 'IKEA Bag';         sv.poi_ikea_bag = 'IKEA-kasse';
en.poi_fika_cup      = 'Fika Cup';         sv.poi_fika_cup = 'Fikakopp';
en.poi_maypole       = 'Maypole';          sv.poi_maypole = 'Midsommarstång';
en.poi_moose         = 'Moose';            sv.poi_moose = 'Älg';
en.poigrp_swedish    = 'Swedish Secrets';  sv.poigrp_swedish = 'Svenska hemligheter';

// ── POI group names ─────────────────────────────────────────────────────────
en.poigrp_health     = 'Health & Safety';   sv.poigrp_health = 'Hälsa & Säkerhet';
en.poigrp_education  = 'Education';        sv.poigrp_education = 'Utbildning';
en.poigrp_daily      = 'Daily Life';       sv.poigrp_daily = 'Vardagsliv';
en.poigrp_culture    = 'Culture';          sv.poigrp_culture = 'Kultur';
en.poigrp_nature     = 'Nature';           sv.poigrp_nature = 'Natur';
en.poigrp_transport  = 'Transport';        sv.poigrp_transport = 'Transport';

// ── Zone names (for building tooltip) ───────────────────────────────────────
en.zone_road         = 'Road';             sv.zone_road = 'Väg';
en.zone_res_lo       = 'House';            sv.zone_res_lo = 'Hus';
en.zone_res_med      = 'Apartment';        sv.zone_res_med = 'Lägenhet';
en.zone_res_hi       = 'High-rise';        sv.zone_res_hi = 'Höghus';
en.zone_commercial   = 'Commercial';       sv.zone_commercial = 'Kommersiell';
en.zone_industrial   = 'Industrial';       sv.zone_industrial = 'Industriell';

// ── About modal tabs ────────────────────────────────────────────────────────
en.tab_general       = '🌍 General';
sv.tab_general       = '🌍 Allmänt';
en.tab_expert        = '🧪 Advanced';
sv.tab_expert        = '🧪 Avancerat';

// ── About modal footer ──────────────────────────────────────────────────────
en.built_by          = 'Built by Claudio Linhares, Linnaeus University, 2026.';
sv.built_by          = 'Utvecklad av Claudio Linhares, Linnéuniversitetet, 2026.';

// ── Transport stats labels ───────────────────────────────────────────────────
en.stop_bus          = '🚏 Bus stops';       sv.stop_bus = '🚏 Busshållplatser';
en.stop_bus_station  = '🚌 Bus stations';    sv.stop_bus_station = '🚌 Busstationer';
en.stop_tram         = '🚊 Tram stops';      sv.stop_tram = '🚊 Spårvagnshållplatser';
en.stop_train        = '🚆 Train stations';  sv.stop_train = '🚆 Tågstationer';
en.stop_ferry        = '⛴️ Ferry terminals'; sv.stop_ferry = '⛴️ Färjeterminaler';
en.stop_airport      = '✈️ Airports';        sv.stop_airport = '✈️ Flygplatser';
en.line_bus          = '🚌 Bus lines';        sv.line_bus = '🚌 Busslinjer';
en.line_tram         = '🚊 Tram lines';       sv.line_tram = '🚊 Spårvagnslinjer';
en.line_train        = '🚆 Train lines';      sv.line_train = '🚆 Tåglinjer';
en.line_ferry        = '⛴️ Ferry routes';     sv.line_ferry = '⛴️ Färjelinjer';
en.line_plane        = '✈️ Flight routes';    sv.line_plane = '✈️ Flyglinjer';

// ── Transport popover (transport-pop.js mode rows) ─────────────────────────
// Plural mode names (heading), lowercase plurals (count text), and unit
// nouns (lines / stops). Kept separate from the existing line_/stop_ keys
// because those carry emoji prefixes that would clash with the popover's
// own bubble icons.
en.tp_planes         = 'Airplanes';        sv.tp_planes        = 'Flygplan';
en.tp_trains         = 'Trains';           sv.tp_trains        = 'Tåg';
en.tp_trams          = 'Trams';            sv.tp_trams         = 'Spårvagnar';
en.tp_ferries        = 'Ferries';          sv.tp_ferries       = 'Färjor';
en.tp_buses          = 'Buses';            sv.tp_buses         = 'Bussar';
en.tp_planes_lc      = 'airplanes';        sv.tp_planes_lc     = 'flygplan';
en.tp_trains_lc      = 'trains';           sv.tp_trains_lc     = 'tåg';
en.tp_trams_lc       = 'trams';            sv.tp_trams_lc      = 'spårvagnar';
en.tp_ferries_lc     = 'ferries';          sv.tp_ferries_lc    = 'färjor';
en.tp_buses_lc       = 'buses';            sv.tp_buses_lc      = 'bussar';
en.tp_unit_lines     = 'lines';            sv.tp_unit_lines    = 'linjer';
en.tp_unit_stops     = 'stops';            sv.tp_unit_stops    = 'hållplatser';
en.tp_no_data        = 'No transport data for this city.';
sv.tp_no_data        = 'Ingen kollektivtrafikdata för denna stad.';

// ── Misc UI strings ─────────────────────────────────────────────────────────
en.building          = 'Building';          sv.building = 'Byggnad';
en.households_est    = 'Households: ~{{n}} (avg {{avg}} persons)';
sv.households_est    = 'Hushåll: ~{{n}} (snitt {{avg}} personer)';
en.pick_residential  = 'Pick a residential building (house or apartment)';
sv.pick_residential  = 'Välj en bostadsbyggnad (hus eller lägenhet)';

// ── Transport vehicle tooltip ───────────────────────────────────────────────
en.veh_bus           = 'Bus';              sv.veh_bus = 'Buss';
en.veh_tram          = 'Tram';             sv.veh_tram = 'Spårvagn';
en.veh_train         = 'Train';            sv.veh_train = 'Tåg';
en.veh_ferry         = 'Ferry';            sv.veh_ferry = 'Färja';
en.veh_plane         = 'Plane';            sv.veh_plane = 'Flygplan';
en.veh_operator      = 'Operator';         sv.veh_operator = 'Operatör';
en.veh_capacity      = 'Capacity';         sv.veh_capacity = 'Kapacitet';
en.veh_stops         = 'Stops';            sv.veh_stops = 'Hållplatser';
en.veh_selected      = 'selected';         sv.veh_selected = 'vald';
en.veh_pax           = 'pax';              sv.veh_pax = 'pers';

// ── Neighborhood area popup ─────────────────────────────────────────────────
en.unknown_area      = 'Unknown';
sv.unknown_area      = 'Okänd';

// ── Merged HUD (sidebar / sections / picker) ───────────────────────────────
en.controls            = 'Controls';                sv.controls            = 'Kontroller';
en.map_layers          = 'Map layers';              sv.map_layers          = 'Kartlager';
en.visualization       = 'Visualization';           sv.visualization       = 'Visualisering';
en.epidemic_curve      = 'Epidemic curve';          sv.epidemic_curve      = 'Epidemikurva';
en.interventions       = 'Interventions';           sv.interventions       = 'Åtgärder';
en.layer_3d            = '3D heights';              sv.layer_3d            = '3D-höjder';
en.layer_areas         = 'Administrative areas';    sv.layer_areas         = 'Stadsdelsområden';
en.layer_daynight      = 'Day / Night';             sv.layer_daynight      = 'Dag / Natt';
en.layer_flow          = 'Infection flow';          sv.layer_flow          = 'Smittflöde';
en.layer_agents        = 'Agents (ABM)';            sv.layer_agents        = 'Agenter (ABM)';
en.layer_nature        = 'Nature';                  sv.layer_nature        = 'Natur';
en.layer_transport     = 'Transport';               sv.layer_transport     = 'Trafik';
en.layer_poi           = 'Points of interest';      sv.layer_poi           = 'Intressepunkter';
en.infection_intensity = 'Infection intensity';     sv.infection_intensity = 'Smittintensitet';
en.recently_simulated  = 'Recently simulated';      sv.recently_simulated  = 'Senast simulerat';

// ── City picker (state 01) ─────────────────────────────────────────────────
en.intro_headline      = 'Simulate an epidemic in any Swedish city.';
sv.intro_headline      = 'Simulera en epidemi i valfri svensk stad.';
en.intro_sub           = 'EpiCity runs SEIR simulations on real population & geography data — choose a city to begin.';
sv.intro_sub           = 'EpiCity kör SEIR-simuleringar på riktig befolknings- och geografidata — välj en stad för att börja.';
en.intro_step1_t       = 'Pick a city';             sv.intro_step1_t       = 'Välj en stad';
en.intro_step1_d       = 'Click any marker on the map'; sv.intro_step1_d   = 'Klicka på en markör på kartan';
en.intro_step2_t       = 'Seed patient zero';       sv.intro_step2_t       = 'Placera patient noll';
en.intro_step2_d       = 'Drop an infected person anywhere'; sv.intro_step2_d = 'Släpp en smittad person var som helst';
en.intro_step3_t       = 'Run & intervene';         sv.intro_step3_t       = 'Kör & ingrip';
en.intro_step3_d       = 'Watch the outbreak, apply measures'; sv.intro_step3_d = 'Se utbrottet, sätt in åtgärder';
en.poi_patient_zero    = 'Patient zero';            sv.poi_patient_zero    = 'Patient noll';
en.growth              = 'Growth';                  sv.growth              = 'Tillväxt';
en.city_one            = 'city';                    sv.city_one            = 'stad';
en.city_many           = 'cities';                  sv.city_many           = 'städer';
en.locked              = 'locked';                  sv.locked              = 'låsta';
en.show_all_routes     = 'Show all routes';         sv.show_all_routes     = 'Visa alla linjer';
en.clear               = 'Clear';                   sv.clear               = 'Rensa';
en.hospital_alert      = 'Hospitals overwhelmed — mortality rising';
sv.hospital_alert      = 'Sjukhusen överbelastade — dödligheten stiger';


// ── Country of birth & origin ──────────────────────────────────────────────
en.country_of_birth     = 'Country of Birth';        sv.country_of_birth     = 'Födelseland';
en.demography_origin    = 'Demography (Country of Birth)'; sv.demography_origin = 'Demografi (Födelseland)';
en.foreign_frac         = 'Foreign-born fraction';   sv.foreign_frac         = 'Andel utrikesfödda';

// ── Pathogen presets (settings panel + about modal) ───────────────────────
en.pathogen_presets     = 'Pathogen Presets';        sv.pathogen_presets     = 'Patogenförval';
en.preset_default       = 'default';                 sv.preset_default       = 'standard';
en.preset_covid19       = 'COVID-19';                sv.preset_covid19       = 'COVID-19';
en.preset_h1n1_2009     = 'H1N1 2009';               sv.preset_h1n1_2009     = 'H1N1 2009';
en.preset_sars_2003     = 'SARS 2003';               sv.preset_sars_2003     = 'SARS 2003';
en.preset_mers          = 'MERS';                    sv.preset_mers          = 'MERS';
en.preset_measles       = 'Measles';                 sv.preset_measles       = 'Mässling';
en.preset_ebola         = 'Ebola';                   sv.preset_ebola         = 'Ebola';
en.preset_flu_1918      = '1918 Flu';                sv.preset_flu_1918      = 'Spanska sjukan 1918';
en.preset_sources       = '📖 sources';              sv.preset_sources       = '📖 källor';

// ── Statistics panel ───────────────────────────────────────────────────────
en.stats_title          = 'Statistics';              sv.stats_title          = 'Statistik';
en.stats_show           = '📊 Stats';                sv.stats_show           = '📊 Statistik';
en.stats_hide           = 'Hide';                    sv.stats_hide           = 'Dölj';
en.stat_reproduction    = 'Reproduction';            sv.stat_reproduction    = 'Reproduktion';
en.stat_transmission    = 'Transmission rate β';     sv.stat_transmission    = 'Smittsamhet β';
en.stat_dynamics        = 'Dynamics';                sv.stat_dynamics        = 'Dynamik';
en.stat_severity        = 'Severity';                sv.stat_severity        = 'Allvarlighet';
en.stat_climate         = 'Climate';                 sv.stat_climate         = 'Klimat';
en.stat_r0              = 'R₀ (basic)';              sv.stat_r0              = 'R₀ (grund)';
en.stat_reff            = 'R_eff';                   sv.stat_reff            = 'R_eff';
en.stat_beta_eff        = 'β_eff (city)';            sv.stat_beta_eff        = 'β_eff (stad)';
en.stat_beta_baseline   = 'β baseline';              sv.stat_beta_baseline   = 'β baslinje';
en.stat_beta_delta      = 'Δ vs. baseline';          sv.stat_beta_delta      = 'Δ mot baslinje';
en.stat_active_npi      = 'Active NPIs';             sv.stat_active_npi      = 'Aktiva åtgärder';
en.stat_doubling_time   = 'Doubling time';           sv.stat_doubling_time   = 'Fördubblingstid';
en.stat_growth_rate     = 'Growth rate r';           sv.stat_growth_rate     = 'Tillväxttakt r';
en.stat_new_infections  = 'New infections';          sv.stat_new_infections  = 'Nya fall idag';
en.stat_attack_rate     = 'Attack rate';             sv.stat_attack_rate     = 'Attackkvot';
en.stat_cfr             = 'CFR';                     sv.stat_cfr             = 'CFR';
en.stat_peak_prev       = 'Peak prevalence';         sv.stat_peak_prev       = 'Topp-prevalens';
en.stat_peak_day        = 'Peak day';                sv.stat_peak_day        = 'Toppdag';
en.stat_hospital_load   = 'Hospital load';           sv.stat_hospital_load   = 'Sjukhusbelastning';
en.stat_temperature     = 'Temperature';             sv.stat_temperature     = 'Temperatur';
en.stat_humidity        = 'Humidity';                sv.stat_humidity        = 'Luftfuktighet';
en.stat_season          = 'Season';                  sv.stat_season          = 'Årstid';
en.stat_climate_mult    = 'Climate β×';              sv.stat_climate_mult    = 'Klimat β×';
en.season_winter        = 'Winter';                  sv.season_winter        = 'Vinter';
en.season_spring        = 'Spring';                  sv.season_spring        = 'Vår';
en.season_summer        = 'Summer';                  sv.season_summer        = 'Sommar';
en.season_autumn        = 'Autumn';                  sv.season_autumn        = 'Höst';

// ── About / References tab ─────────────────────────────────────────────────
en.about_tab_general    = '🌍 General';              sv.about_tab_general    = '🌍 Allmänt';
en.about_tab_advanced   = '🧪 Advanced';             sv.about_tab_advanced   = '🧪 Avancerat';
en.about_tab_references = '📚 References';           sv.about_tab_references = '📚 Referenser';
en.about_refs_intro_en  = "EpiCity's parameters are grounded in published literature. Each decision below lists its supporting source. Paper titles are kept in English so you can search for them directly.";
sv.about_refs_intro_en  = 'EpiCitys parametrar är förankrade i publicerad litteratur. Här hittar du varje val tillsammans med källan. Artikeltitlar är på engelska — översätts inte, så att du kan söka efter dem direkt.';
en.about_refs_section   = 'Scientific references behind EpiCity';
sv.about_refs_section   = 'Vetenskapliga referenser bakom EpiCity';
en.about_presets_section = 'Pathogen presets';        sv.about_presets_section = 'Patogenförval';

// ── Weather (timeline climate strip) ──────────────────────────────────────
en.wx_snow              = 'Snow';                    sv.wx_snow              = 'Snö';
en.wx_rain              = 'Rain';                    sv.wx_rain              = 'Regn';
en.wx_cloudy            = 'Cloudy';                  sv.wx_cloudy            = 'Molnigt';
en.wx_clear             = 'Sunny';                   sv.wx_clear             = 'Soligt';

// ── Engine mode (epidemic-engine picker on the macro view) ──────────────────
en.em_engine            = 'Engine';                  sv.em_engine            = 'Motor';
en.em_compartmental     = 'Compartmental';           sv.em_compartmental     = 'Kompartmentell';
en.em_abm               = 'Agent-based';             sv.em_abm               = 'Agentbaserad';
en.em_compartmental_sub = 'Building-level SEIR. Fast aggregate dynamics.';
sv.em_compartmental_sub = 'SEIR per byggnad. Snabb aggregerad dynamik.';
en.em_abm_sub           = 'Per-individual model. Real infection chains.';
sv.em_abm_sub           = 'Modell per individ. Verkliga smittkedjor.';
en.em_pop_scale         = 'Population scale';        sv.em_pop_scale         = 'Befolkningsskala';
en.em_pop_scale_desc    = '1 agent represents {{n}} residents';
sv.em_pop_scale_desc    = '1 agent representerar {{n}} invånare';
en.em_resolution        = 'Time resolution';         sv.em_resolution        = 'Tidsupplösning';
en.em_abm_help          = 'Higher scale = faster but coarser. Smaller resolution = better transit modelling but slower.';
sv.em_abm_help          = 'Högre skala = snabbare men grövre. Mindre upplösning = bättre kollektivtrafikmodellering men långsammare.';

// ── Game mode (mode picker, difficulty, season) ─────────────────────────────
en.gm_mode              = 'Mode';                    sv.gm_mode              = 'Läge';
en.gm_sandbox           = 'Sandbox';                 sv.gm_sandbox           = 'Sandlåda';
en.gm_game              = 'Game';                    sv.gm_game              = 'Spel';
en.gm_sandbox_sub       = 'Free customisation. Pick patient zero, tweak parameters, no scoring.';
sv.gm_sandbox_sub       = 'Fri anpassning. Välj patient noll, justera parametrar, ingen poängräkning.';
en.gm_game_sub          = '180-day scenario. Manage resources. Score = total cost.';
sv.gm_game_sub          = '180-dagars scenario. Hantera resurser. Poängen = total kostnad.';
en.gm_difficulty        = 'Difficulty';              sv.gm_difficulty        = 'Svårighetsgrad';
en.gm_easy              = 'Easy';                    sv.gm_easy              = 'Lätt';
en.gm_normal            = 'Normal';                  sv.gm_normal            = 'Normal';
en.gm_hard              = 'Hard';                    sv.gm_hard              = 'Svår';
en.gm_easy_desc         = 'Lower transmission, lower fatality, larger budget.';
sv.gm_easy_desc         = 'Lägre smittspridning, lägre dödlighet, större budget.';
en.gm_normal_desc       = 'Calibrated to a moderate pandemic.';
sv.gm_normal_desc       = 'Kalibrerad för en måttlig pandemi.';
en.gm_hard_desc         = 'High transmission and fatality, tight budget, vaccines arrive late.';
sv.gm_hard_desc         = 'Hög smittspridning och dödlighet, snäv budget, vaccin kommer sent.';

// Season picker (already had season_* keys; add panel labels + hints)
en.sp_starting_season   = 'Starting season';         sv.sp_starting_season   = 'Startsäsong';
en.sp_season_random     = 'Random';                  sv.sp_season_random     = 'Slump';
en.sp_winter_hint       = 'Cold + dry → β slightly higher, viral survival up.';
sv.sp_winter_hint       = 'Kallt + torrt → β något högre, virusöverlevnaden ökar.';
en.sp_spring_hint       = 'Mild — no climate β bonus or penalty.';
sv.sp_spring_hint       = 'Milt — ingen klimatpåverkan på β.';
en.sp_summer_hint       = 'Warm + humid → β slightly suppressed.';
sv.sp_summer_hint       = 'Varmt + fuktigt → β något dämpat.';
en.sp_autumn_hint       = 'Cooling weather, β starts to creep up.';
sv.sp_autumn_hint       = 'Svalare väder, β börjar krypa uppåt.';
en.sp_random_hint       = 'Each city entry rolls a fresh day-of-year.';
sv.sp_random_hint       = 'Varje stadsbesök slumpar fram en ny startdag.';

// ── Game HUD bars + chips ──────────────────────────────────────────────────
en.hud_stability        = 'Stability';               sv.hud_stability        = 'Stabilitet';
en.hud_economy          = 'Economy';                 sv.hud_economy          = 'Ekonomi';
en.hud_morale           = 'Morale';                  sv.hud_morale           = 'Moral';
en.hud_trust            = 'Trust';                   sv.hud_trust            = 'Förtroende';
en.hud_hospitals        = 'Hospitals';               sv.hud_hospitals        = 'Sjukhus';
en.hud_budget           = 'Budget';                  sv.hud_budget           = 'Budget';
en.hud_deaths           = 'Deaths';                  sv.hud_deaths           = 'Döda';
en.hud_day              = 'Day';                     sv.hud_day              = 'Dag';
en.hud_score            = 'Score';                   sv.hud_score            = 'Poäng';

// HUD rich-tooltip headlines (used as <b>title</b> + first sentence)
en.hud_tip_stability    = 'Headline reading of how the country is holding up.';
sv.hud_tip_stability    = 'Övergripande mått på hur landet klarar sig.';
en.hud_tip_economy      = 'Productive output of the city (0–100).';
sv.hud_tip_economy      = 'Stadens produktion (0–100).';
en.hud_tip_morale       = 'Public happiness / sanity bar (0–100).';
sv.hud_tip_morale       = 'Folkets välmående och tålamod (0–100).';
en.hud_tip_trust        = 'Your political capital (0–100).';
sv.hud_tip_trust        = 'Ditt politiska kapital (0–100).';
en.hud_tip_hospitals    = '% of hospital capacity in use.';
sv.hud_tip_hospitals    = 'Andel av sjukhusens kapacitet i bruk.';
en.hud_tip_budget       = 'Treasury (SEK) for medical interventions.';
sv.hud_tip_budget       = 'Statskassa (SEK) för medicinska åtgärder.';
en.hud_tip_deaths       = 'Cumulative deaths since day 0.';
sv.hud_tip_deaths       = 'Totalt antal dödsfall sedan dag 0.';
en.hud_tip_day          = '180-day fixed scenario.';
sv.hud_tip_day          = '180-dagars fast scenario.';
en.hud_tip_score        = 'Total cost of the pandemic. Lower is better.';
sv.hud_tip_score        = 'Pandemins totala kostnad. Lägre är bättre.';
en.hud_drains           = 'Drains';                  sv.hud_drains           = 'Minskar';
en.hud_recovers         = 'Recovers';                sv.hud_recovers         = 'Återhämtar';
en.hud_spent            = 'Spent';                   sv.hud_spent            = 'Förbrukas';
en.hud_refilled         = 'Refilled';                sv.hud_refilled         = 'Fylls på';

// End-of-scenario modal
en.end_complete         = 'Scenario complete · {{n}} days';
sv.end_complete         = 'Scenariot slut · {{n}} dagar';
en.end_cost_unit        = 'cost';                    sv.end_cost_unit        = 'kostnad';
en.end_deaths           = 'Deaths';                  sv.end_deaths           = 'Döda';
en.end_economy          = 'Economy';                 sv.end_economy          = 'Ekonomi';
en.end_morale           = 'Morale';                  sv.end_morale           = 'Moral';
en.end_trust            = 'Trust';                   sv.end_trust            = 'Förtroende';
en.end_budget_left      = 'Budget left';             sv.end_budget_left      = 'Budget kvar';
en.end_replay           = 'Try again';               sv.end_replay           = 'Försök igen';
en.end_back             = 'Back to map';             sv.end_back             = 'Tillbaka till kartan';

// Read-only banner shown over the locked settings popover in game mode
en.gm_settings_locked   = '🔒 Read-only — game mode';
sv.gm_settings_locked   = '🔒 Skrivskyddad — spelläge';

// ── Spatial-intervention palette (Map tools) ───────────────────────────────
en.sp_pal_h             = 'Map tools';               sv.sp_pal_h             = 'Kartverktyg';
en.sp_pal_active_h      = 'Active measures';        sv.sp_pal_active_h      = 'Aktiva åtgärder';
en.sp_quarantine_zone   = 'Quarantine Zone';        sv.sp_quarantine_zone   = 'Karantänområde';
en.sp_quarantine_zone_desc = 'Seal off a neighborhood — β crushed, sealed perimeter.';
sv.sp_quarantine_zone_desc = 'Spärra av ett område — β krossas, perimetern försluts.';
en.sp_quarantine_zone_hint = 'Click a neighborhood on the map (Areas layer is auto-shown).';
sv.sp_quarantine_zone_hint = 'Klicka på ett område på kartan (Områdeslagret visas automatiskt).';

en.sp_lasso             = 'Lasso Quarantine';       sv.sp_lasso             = 'Lasso-karantän';
en.sp_lasso_desc        = 'Draw a custom polygon. β crushed inside, edges to outside severed.';
sv.sp_lasso_desc        = 'Rita en egen polygon. β krossas inuti, kanter mot utsidan klipps.';
en.sp_lasso_hint        = 'Click & drag to trace the perimeter — release to close. Esc to cancel.';
sv.sp_lasso_hint        = 'Klicka och dra för att rita perimetern — släpp för att stänga. Esc avbryter.';

en.sp_targeted_vaccine  = 'Targeted Vaccine Drive'; sv.sp_targeted_vaccine  = 'Riktad vaccinationskampanj';
en.sp_targeted_vaccine_desc = 'Concentrate vaccination effort: 3× rate inside, ⅓× elsewhere.';
sv.sp_targeted_vaccine_desc = 'Koncentrera vaccinationen: 3× takt inuti, ⅓× på övriga platser.';
en.sp_targeted_vaccine_hint = 'Click a neighborhood. Vaccination intervention must be on for this to fire.';
sv.sp_targeted_vaccine_hint = 'Klicka på ett område. Vaccinationsåtgärden måste vara aktiv.';

en.sp_mobile_clinic     = 'Mobile Clinic';          sv.sp_mobile_clinic     = 'Mobil klinik';
en.sp_mobile_clinic_desc = '500 m field clinic — local hospital capacity stays high under overflow. 21 days.';
sv.sp_mobile_clinic_desc = '500 m fältklinik — lokal sjukhuskapacitet hålls hög även vid överbelastning. 21 dagar.';
en.sp_mobile_clinic_hint = 'Click anywhere on the map to deploy a clinic.';
sv.sp_mobile_clinic_hint = 'Klicka var som helst på kartan för att placera ut en klinik.';

en.sp_surge_testing     = 'Surge Testing';          sv.sp_surge_testing     = 'Förstärkt testning';
en.sp_surge_testing_desc = 'Click an infectious cluster — it + 8 nearest get γ × 2 for 30 days.';
sv.sp_surge_testing_desc = 'Klicka på en smittsam klunga — den + 8 närmaste får γ × 2 i 30 dagar.';
en.sp_surge_testing_hint = 'Click any point. Best used on the brightest spots in the infection map.';
sv.sp_surge_testing_hint = 'Klicka var som helst. Använd helst på de starkaste utbrottspunkterna.';

en.sp_locked_vacc_iv    = 'Enable the citywide Vaccination intervention first.';
sv.sp_locked_vacc_iv    = 'Aktivera den stadsövergripande vaccinationsåtgärden först.';
en.sp_locked_vacc_day   = 'Vaccination is locked for {{n}} more day(s).';
sv.sp_locked_vacc_day   = 'Vaccinationen är låst i {{n}} dag(ar) till.';
en.sp_lasso_too_short   = 'Drag a longer loop to define the area.';
sv.sp_lasso_too_short   = 'Dra en längre slinga för att definiera området.';
en.sp_no_neighborhood   = 'Click on a neighborhood polygon — toggle Areas layer if you need to.';
sv.sp_no_neighborhood   = 'Klicka på ett områdes polygon — aktivera Områdeslagret vid behov.';

// Vaccination countdown pill (intervention-gating.js)
en.vacc_lock_today      = 'today';                   sv.vacc_lock_today      = 'idag';
en.vacc_lock_tomorrow   = 'tomorrow';                sv.vacc_lock_tomorrow   = 'imorgon';
en.vacc_lock_days       = 'in {{n}} days';           sv.vacc_lock_days       = 'om {{n}} dagar';

// New citywide interventions added with game mode
en.iv_info_campaign     = 'Public Information';      sv.iv_info_campaign     = 'Informationskampanj';
en.iv_info_campaign_desc = 'Awareness campaign — small β reduction, slow trust gain.';
sv.iv_info_campaign_desc = 'Upplysningskampanj — liten minskning av β, långsam ökning av förtroende.';
en.iv_hospital_surge    = 'Hospital Surge';          sv.iv_hospital_surge    = 'Akutkapacitet';
en.iv_hospital_surge_desc = 'Temporary +40% hospital capacity — mitigates overflow deaths.';
sv.iv_hospital_surge_desc = 'Tillfälligt +40% sjukhuskapacitet — minskar dödsfall vid överbelastning.';
en.iv_targeted_lockdown = 'Targeted Lockdown';       sv.iv_targeted_lockdown = 'Riktad nedstängning';
en.iv_targeted_lockdown_desc = 'Hot-zone restriction — β -35% at lower social/economic cost.';
sv.iv_targeted_lockdown_desc = 'Restriktioner i utbrottszoner — β -35% till lägre social/ekonomisk kostnad.';
en.iv_stop_transport    = 'Stop Public Transport';   sv.iv_stop_transport    = 'Stoppa kollektivtrafik';
en.iv_stop_transport_desc = 'Halt every bus, tram, train, ferry and flight — no in-vehicle spread.';
sv.iv_stop_transport_desc = 'Stoppa varenda buss, spårvagn, tåg, färja och flyg — ingen smitta i fordonet.';

// ── Spatial-zone active list labels (translated client-side from
//    label_key + label_vars returned by /api/spatial) ──────────────
en.sp_lbl_lasso   = 'Lasso zone · {{n}} buildings';
sv.sp_lbl_lasso   = 'Lassozon · {{n}} byggnader';
en.sp_lbl_qua_nbh = 'Quarantine · {{name}}';
sv.sp_lbl_qua_nbh = 'Karantän · {{name}}';
en.sp_lbl_vac_nbh = 'Vaccine drive · {{name}}';
sv.sp_lbl_vac_nbh = 'Vaccinkampanj · {{name}}';
en.sp_lbl_clinic  = 'Mobile clinic · {{radius}} m';
sv.sp_lbl_clinic  = 'Mobil klinik · {{radius}} m';
en.sp_lbl_surge   = 'Surge testing · cluster of {{n}}';
sv.sp_lbl_surge   = 'Förstärkt testning · {{n}} byggnader';
// Plural-aware count units for active-zones meta line
en.sp_meta_bldg   = '{{n}} bldg';
sv.sp_meta_bldg   = '{{n}} byggn.';
en.sp_meta_days   = '{{n}}d left';
sv.sp_meta_days   = '{{n}}d kvar';
