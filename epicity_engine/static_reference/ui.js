/**
 * ui.js — Orchestrator: sim loop, API calls, DOM stats, tooltip, interventions.
 * Delegates 3D rendering to view.js (via MapLibre custom layer in map.js).
 * Delegates panel building to legend.js, settings.js.
 */

import {
  buildCity, updateColors, setColorMode, setHeightMode,
  setColorScale, setLogScale, setInvertScale, setNormMode, animateTopView, updateScene,
  setHoverCallback, setSelectionCallback, clearSelection, selectByIdx, getScene,
  setDayNight, updateDayNight, notifyNewDay, getCurrentWeather, resetWeather,
  setPOIVisible, setPOICategoryVisible, POI_CATEGORIES, POI_GROUPS,
  setPatientZeroIndices, PATIENT_ZERO_KEY, PATIENT_ZERO_META,
  setActiveInterventions, setTransportStops, setNaturePOIs,
  setFlagPoles, setLNUFlagPoles, setEasterEggs,
  getAvailablePOIKeys, getPOICounts,
  getVisibleWidth, getCameraQuat, getMVPMatrix, getCanvasRect,
  setNatureDetailVisible, isNatureDetailVisible,
  setPickPatientZeroMode,
  setAreasOverlay,
  poiName, poiGroupName,
} from './view.js';
import { initMap, getMap, flyToBuilding, flyToTopView, flyToPerspective, flyToCity, rebuildForCity, showTransportLines, highlightTransportLine, toggleNeighborhoods, isAreasActive } from './map.js';
import { initInfectionFlow, setInfectionFlowActive, setInfectionFlowSelected, updateInfectionFlow, rebuildInfectionFlowFromHistory, setInfectionFlowOptions } from './infection-flow.js';
import { initAgentsLayer, setAgentsLayerActive, isAgentsLayerActive, refreshAgents, setAgentsScenario, tickAgents } from './agents-layer.js';
import {
  initMacro, refreshMacro,
  flyMacroToCity, flyMacroToSweden, setMacroView, resizeMacro,
} from './macro.js';
import { renderChart, initChart }        from './chart.js';
import { initMinimap, renderMinimap }    from './minimap.js';
import { buildLegend, getColorMode, getHeightMode, getColorScale, getLogScale, getInvertScale, getNormMode } from './legend.js';
import {
  buildSettingsPanel, bindSettingsToggle, closeSettingsPanel,
  isManualPickChecked, setManualPickChecked, setManualPickHint,
} from './settings.js';
import { FlowField } from './flow-field.js';
import { drawLNUTree } from './buildings_custom.js';
import * as transport from './transport.js';
import { saveHistory, loadHistory, clearHistory } from './historyStore.js';
import {
  enableCompareMode, disableCompareMode,
  setMode as cmpSetMode, setDayA as cmpSetDayA, setDayB as cmpSetDayB,
  refreshIfActive as cmpRefreshIfActive, isActive as isCompareActive,
  compareState, onCompareChange, setCompareHoverCallback,
} from './compare.js';
import { refreshNatureVisibility as cmpRefreshNature } from './compare-view-b.js';
import { initGlobeButton } from './globeButton.js';
import { registerPopup, openPopup, closeAllPopups } from './popup-manager.js';
import { t, getLang, setLang, onLangChange } from './i18n.js';
import { bindStatsPanel, renderStats } from './stats.js';
import { initSidebar, setSectionBadge, setInterventionsCount, refreshRailMirrors } from './sidebar.js';
import { initGauge, setGauge, setGaugeColor, setGrowthGauge } from './gauge.js';
import { initPicker, selectCity, setPickerSummary } from './picker.js';
import { initGameMode } from './game-mode.js';
import { initEngineMode } from './engine-mode.js';
import { initSeasonPicker } from './season-picker.js';
import { render as renderGameHud, refresh as refreshGameHud } from './game-hud.js';
import { applyGating as applyIvGating, isLocked as isIvLocked } from './intervention-gating.js';
import { initSpatialTools, refreshSpatial as refreshSpatialTools, applyToolGating as applySpatialToolGating } from './spatial-tools.js';
import {
  ensureOverlay as ensureSpatialOverlay,
  setCityLayout as setSpatialCityLayout,
  setLatestState as setSpatialLatestState,
  teardown as teardownSpatialOverlay,
} from './spatial-overlay.js';
import * as ambulance3d from './ambulance3d.js';
import { initUiInteractionGuard } from './ui-interaction-guard.js';
import { initSearch, setSearchSummary, noteRecentCity } from './search.js';
import { initPoi, openPoiPopover, closePoiPopover } from './poi.js';
import { initBuildingSearch, openBuildingSearch, closeBuildingSearch, buildSearchIndex } from './building-search.js';
import { initTransportPop, openTransportPop, closeTransportPop, setTransportStats } from './transport-pop.js';
import { sampleScaleCSS } from './colorscales.js';

// ── Constants ─────────────────────────────────────────────────────────────────

const HISTORY_MAX = 200;

// Zone name keys for i18n lookup — indexed by Zone enum value
const _ZONE_I18N_KEYS = [
  'zone_road', 'zone_res_lo', 'zone_res_med', 'zone_res_hi',
  'zone_commercial', 'zone_industrial', 'poi_hospital', 'poi_park', 'poi_school',
  'poi_pharmacy', 'poi_university', 'poi_grocery', 'poi_dental', 'poi_veterinary',
  'poi_church', 'poi_castle', 'poi_museum', 'poi_theater', 'poi_stadium',
  'poi_lake', 'poi_forest', 'poi_nightclub', 'poi_playground', 'poi_cemetery',
  'poi_kindergarten', 'poi_high_school', 'poi_library',
  'poi_restaurant', 'poi_sports_centre', 'poi_hotel', 'poi_community',
  'poi_mall', 'poi_parking', 'poi_manor', 'poi_historic',
  'poi_police', 'poi_fire_station',
];
const ZONE_NAMES = new Proxy([], {
  get(_, prop) {
    const idx = parseInt(prop, 10);
    if (!isNaN(idx) && _ZONE_I18N_KEYS[idx]) return t(_ZONE_I18N_KEYS[idx]);
    return undefined;
  }
});

// ── Geometry helpers ─────────────────────────────────────────────────────────

/** Ray-casting point-in-polygon test (works for [lon,lat] rings). */
function _pointInRing(lon, lat, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    if ((yi > lat) !== (yj > lat) &&
        lon < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

/**
 * Filter transport stops to only those inside the city border polygon.
 * ALL types including airports are filtered — no POIs outside the border.
 * Falls back to returning all stops if no border data is available.
 */
function _filterStopsByBorder(stops, layout) {
  if (!layout || !layout.border || layout.border.length === 0) return stops;
  return stops.filter(s => {
    for (const ring of layout.border) {
      if (_pointInRing(s.lon, s.lat, ring)) return true;
    }
    return false;
  });
}

// ── Simulation state ──────────────────────────────────────────────────────────

let currentState  = null;
let cityLayout    = null;
// Local copy of the transport-stop list so the POI navigation arrows
// can fly the camera through transport-keyed categories (bus_station,
// train, airport, …). Kept in sync in the /api/transport fetch path.
let _poiTransportStops = [];
// POI → list of {lon, lat, idx} lookup buckets, rebuilt whenever the
// POI menu is rebuilt. `poi:<key>` holds individual categories;
// `group:<gid>` holds the union across every category in that group.
// Paired with `_poiCursor` which tracks the current camera target
// within each bucket for the left/right navigation arrows.
let _poiBuckets = {};
const _poiCursor = {};
let _selectedCell = null;

// Last building the cursor was over, so the tooltip can be re-rendered
// against fresh SEIR values when a new state lands while the user is
// still hovering. Cleared when the cursor leaves all buildings.
let _hoveredBuilding = null;

// Transport vehicle hover/selection — tooltip follows the moving vehicle
let _transportHoveredVi  = -1;  // vehicle index under cursor
let _transportSelectedVi = -1;  // click-locked vehicle index

let _history    = [];
let _historyIdx = -1;
// Debounced IndexedDB persistence: re-armed after each new day so a burst
// of fast steps only triggers one write. The whole _history is saved as a
// single packed blob keyed by city id (see historyStore.js).
let _historySaveTimer = null;

// Timeline replay (history playback). When non-null, a requestAnimationFrame
// loop is scrubbing through `_history` from `_historyIdx` toward the end. Live
// sim is paused for the duration so it can't fight the playback. Between day
// snapshots we interpolate cells (sub-day frames) to make buildings grow
// smoothly instead of popping from one day to the next.
let _replayRaf      = null;
let _interpBuf      = null;   // reusable buffer for interpolated cells
// Slow per-day animation for short histories so each day is readable; long
// histories sweep faster so the user doesn't sit through 50+ slow days.
// Adaptive: aim for ~11s total replay duration, clamped per-day so single
// frames stay legible at the low end and the easing stays smooth at the high.
const REPLAY_MS_PER_DAY_MIN = 450;     // bumped from 160 — slower so the
const REPLAY_MS_PER_DAY_MAX = 2000;    //   infection-flow tree has time
                                       //   to grow visibly between days
function _replayMsPerDay(dayCount) {
  if (dayCount < 8) return REPLAY_MS_PER_DAY_MAX;
  // Aim for ~24 s total sweep, clamped at both ends.
  const ms = 24000 / dayCount;
  return Math.max(REPLAY_MS_PER_DAY_MIN, Math.min(REPLAY_MS_PER_DAY_MAX, ms));
}

let _flowField  = null;
let _flowActive = false;

// Transport layer (buses, trams, trains, ferries, planes) defaults ON so
// the user lands on a populated map and the new per-vehicle red infection
// rings have something to surface against. Toggled via #btnTransport in
// the playbar; the initial visual state of the button is synced in
// _bindTransportPanel so the highlight matches the variable on first load.
let _transportActive = true;
let _transportLoaded = false;

let running     = false;
let simSpeed    = 2;
let _firstRunPoiApplied = false;   // becomes true after the first Play click
// Patient-zero pick mode: 'off' = idle, 'waiting' = checkbox checked but
// no building clicked yet, 'set' = manual building has been pinned.
let _pickPzState = 'off';

let flowHour = 0.0;
let _dayNightOn = false;

// ── Day / step pipeline ───────────────────────────────────────────────────────
// Days drive the clock, NOT the other way around. Every animation cycle of
// the 0→24h clock represents exactly one simulated day; when the clock
// reaches 24:00 we commit the day's state to the HUD and start the next day.
//
// The speed slider picks a TARGET real-time duration per day. The sim step
// for the next day is fired at the start of the current animation; if it
// lands before the clock reaches 24 we hold the new state until clock-wrap.
// If the sim is slower than the target, the clock holds at 24:00 until the
// step lands — guaranteeing that every simulated day is rendered exactly
// once with a visible animation window.

const SEC_PER_DAY_MAX = 5.0;  // speed 1 → slow  (5s per day)
const SEC_PER_DAY_MIN = 0.8;  // speed 3 → fast  (0.8s per day)

let _dayStartTs         = null;   // perf-timer ts at start of current day
let _stepInFlight       = false;
let _pendingStateFromStep = null; // step result waiting for next animation slot
// Day currently being animated TOWARD (currentState → _animTarget). When
// non-null, _dayStartTs is set and the smooth lerp is in progress. When the
// animation begins we prefetch the day AFTER this one into
// _pendingStateFromStep so the next animation can start back-to-back with no
// idle gap, eliminating the perceptible "wait" between days.
let _animTarget         = null;
let _stepEpoch          = 0;      // bumped on reset to invalidate in-flight steps

let mouseX = 0, mouseY = 0;

// ── View routing ──────────────────────────────────────────────────────────────
// The page hosts two top-level views:
//   • macro: the Sweden overview (macro.js owns it)
//   • city:  the existing 3D simulation (this file + view.js + map.js)
// Macro is the entry point. The city view is built once on first entry
// and then hidden/shown — never destroyed — so MapLibre + Three.js don't
// have to re-init each visit.

let _view         = null;        // 'macro' | 'city'
let _activeCityId = null;
let _cityInited   = false;       // becomes true after _initCityViewOnce()
let _cityMap      = null;        // saved MapLibre handle from initMap()
let _citiesSummary = [];         // cached /api/cities/summary payload

// ── Language toggle ──────────────────────────────────────────────────────────

function _initLangToggle() {
  // Match both the legacy .lang-toggle / .lang-opt structure and the new
  // .lang-switch / button[data-lang-btn] structure used by the merged HUD.
  const SELECTORS = '.lang-toggle .lang-opt, .lang-switch button[data-lang-btn]';

  function _syncAllToggles(lang) {
    document.querySelectorAll(SELECTORS).forEach(opt => {
      const on = opt.dataset.langBtn === lang;
      opt.classList.toggle('active', on);
      opt.classList.toggle('on',     on);
    });
    document.documentElement.setAttribute('lang', lang);
  }

  document.querySelectorAll(SELECTORS).forEach(opt => {
    opt.addEventListener('click', (e) => {
      e.stopPropagation();
      const lang = opt.dataset.langBtn;
      if (lang) setLang(lang);
    });
  });

  // Sync on init
  _syncAllToggles(getLang());
  window._epiCityLang = getLang();

  // Retranslate everything when language changes
  onLangChange((lang) => {
    _syncAllToggles(lang);
    document.body.classList.toggle('lang-sv', lang === 'sv');
    window._epiCityLang = lang;
    _retranslateStaticDOM();
    _rebuildDynamicPanels();
    // Re-render the macro-view "N cities" counter — its noun (city / stad
    // / cities / städer) is locale-dependent. setPickerSummary repaints
    // the header pill and intro counter using the latest language.
    if (Array.isArray(_citiesSummary)) setPickerSummary(_citiesSummary);
  });
  // Apply on init if already Swedish
  if (getLang() === 'sv') document.body.classList.add('lang-sv');
}

/** Retranslate all static DOM elements that have hardcoded text. */
function _retranslateStaticDOM() {
  // Macro view
  const macroTitle = document.getElementById('macroTitle');
  if (macroTitle) macroTitle.textContent = t('app_title_sweden');
  const macroSub = document.getElementById('macroSubtitle');
  if (macroSub) macroSub.textContent = t('macro_subtitle');

  // City view header
  const btnGlobe = document.getElementById('btnGlobe');
  if (btnGlobe) { btnGlobe.title = t('back_to_sweden'); btnGlobe.setAttribute('aria-label', t('return_to_sweden')); }

  // Generic [data-i18n] pass: any element with data-i18n="key" gets its
  // textContent replaced. New layout uses this for layer-row labels and
  // section headers — keeps SVG icons untouched.
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.dataset.i18n;
    const tr  = t(key);
    if (tr) el.textContent = tr;
  });
  // Translatable placeholder attribute (search inputs etc).
  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    const key = el.dataset.i18nPlaceholder;
    const tr  = t(key);
    if (tr) el.setAttribute('placeholder', tr);
  });
  // Translatable title (browser hover) attribute on action buttons.
  document.querySelectorAll('[data-i18n-title]').forEach(el => {
    const key = el.dataset.i18nTitle;
    const tr  = t(key);
    if (tr) el.setAttribute('title', tr);
  });
  // Layer rows are titled via the [title] attr (tooltips on hover) so
  // those need translation too.
  const _setTitle = (id, key) => { const el = document.getElementById(id); if (el) el.title = t(key); };
  _setTitle('btnNeighborhoods', 'areas_on');
  _setTitle('btnDayNight', 'daynight_title');
  _setTitle('btnNature',   'nature_title');
  _setTitle('btnAbout',    'about_title');
  _setTitle('btnPOI',      'pois_title');

  // Right panel headers (by ID)
  const _setT = (id, key) => { const el = document.getElementById(id); if (el) el.textContent = t(key); };
  _setT('h2Population', 'population');
  _setT('h2EpidemicCurve', 'epidemic_curve');
  _setT('h2Interventions', 'interventions');
  _setT('h2CityLegend', 'city_legend');

  // SEIR stat labels (by ID) — preserve colored letter prefix
  const _seirLabel = (id, letter, color, key) => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = `<span class="seir-letter" style="color:${color}">${letter}</span> ${t(key)}`;
  };
  _seirLabel('lblSusceptible', 'S', '#4ade80', 'susceptible');
  _seirLabel('lblExposed',     'E', '#facc15', 'exposed');
  _seirLabel('lblInfectious',  'I', '#f87171', 'infectious');
  _seirLabel('lblRecovered',   'R', '#60a5fa', 'recovered');
  _seirLabel('lblDeaths',      'D', '#94a3b8', 'deaths');
  _setT('lblTotalCases', 'total_cases');

  // Hospital alert
  _setT('hospitalAlert', 'hospital_alert');

  // Sim bar — buttons are icon-only in the new HUD; just translate titles.
  const btnReset = document.getElementById('btnReset');
  if (btnReset) btnReset.title = t('reset');
  const btnPlay = document.getElementById('btnPlay');
  if (btnPlay) btnPlay.title = btnPlay.classList.contains('playing') ? t('pause') : t('play');
  const btnReplay = document.getElementById('btnReplay');
  if (btnReplay) { btnReplay.textContent = t('replay'); btnReplay.title = t('replay_title'); }

  // Panel titles (by ID)
  _setT('lblHighlightRoutes', 'highlight_routes');
  _setT('lblPointsOfInterest', 'points_of_interest');
  _setT('lblModelParameters', 'model_parameters');

  // Transport highlight checkbox labels
  _setT('hlTrain', 'train_lines');
  _setT('hlTram', 'tram_lines');
  _setT('hlFerry', 'ferry_routes');
  _setT('hlBus', 'bus_lines');

  // About modal tabs
  document.querySelectorAll('.tab-btn').forEach(btn => {
    if (btn.dataset.tab === 'general') btn.textContent = t('tab_general');
    else if (btn.dataset.tab === 'expert') btn.textContent = t('tab_expert');
  });

  // About modal footers
  document.querySelectorAll('.modal-footer').forEach(f => f.textContent = t('built_by'));

  // Day counters — retranslate "Day N" / "Dag N" with the current day value
  const day = currentState?.day ?? 0;
  const dc = document.getElementById('dayCounter');
  if (dc) dc.textContent = t('day_n', { n: day });
  const tls = document.getElementById('tlStart');
  if (tls) tls.textContent = t('day_n', { n: _history.length > 0 ? _history[0].day : 0 });
  const tlc = document.getElementById('tlCurrent');
  if (tlc) tlc.textContent = t('day_n', { n: day });

  // Time-of-day phase label
  if (_flowField) {
    const phase = document.getElementById('todPhase');
    if (phase && phase.textContent !== '—') {
      phase.textContent = t(_flowField.getPhaseName(flowHour));
    }
  }
}

/**
 * Rebuild all dynamically-generated panels so their labels pick up
 * the new language. Called on every language switch.
 */
function _rebuildDynamicPanels() {
  if (!cityLayout) return;  // no city loaded yet

  // 1. Rebuild the City Legend (color/height selectors + group swatches)
  buildLegend(cityLayout, _onVizChange);

  // 2. Rebuild the POI list (category names + group names)
  _updatePOIAvailability();

  // 3. Rebuild intervention buttons (labels + descriptions)
  const ivList = document.getElementById('ivList');
  if (ivList && cityLayout.interventions) {
    ivList.innerHTML = '';
    _buildInterventionButtons(cityLayout.interventions);
    // Re-apply active states from current state
    if (currentState?.active_interventions) {
      for (const id of currentState.active_interventions) {
        const btn = document.getElementById('iv-' + id);
        if (btn) btn.classList.add('active');
      }
    }
  }

  // 4. Rebuild settings panel if it exists
  if (currentState) {
    buildSettingsPanel(currentState.params, currentState.simulation_mode,
                       _onModeChange, _onPickPatientZeroToggle);
  }

  // 5. Re-render the live Statistics panel (its labels are i18n-driven).
  if (currentState) renderStats(currentState);

  // 5b. Re-translate the timeline season pill + climate strip.
  if (currentState?.climate) {
    _updateSeasonPill(currentState.climate);
    _updateClimateStrip(currentState.climate, currentState.weather);
  }

  // 6. Force the About → References tab to re-render in the new language
  //    on next open. (No-op if the user hasn't viewed it yet.)
  _refsRendered = false;
  if (document.querySelector('#aboutBackdrop section.tab-pane[data-tab="references"].active')) {
    _ensureRefsRendered();
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  // Language toggle — must be first so retranslation wiring is ready
  _initLangToggle();
  // Apply initial translation if user previously chose Swedish
  if (getLang() === 'sv') _retranslateStaticDOM();

  // Globe button is the universal "back to macro" affordance.
  initGlobeButton();
  document.getElementById('btnGlobe').addEventListener('click', leaveCity);

  window.addEventListener('beforeunload', () => {
    if (_historySaveTimer !== null) {
      clearTimeout(_historySaveTimer);
      _historySaveTimer = null;
    }
    if (_activeCityId && _history.length > 0) {
      saveHistory(_activeCityId, _history);
    }
  });

  // Build macro first — this is the landing view per spec.
  _citiesSummary = await fetch('/api/cities/summary').then(r => r.json());
  initMacro(document.getElementById('macro-stage'), _citiesSummary, enterCity);
  switchView('macro');

  // Apply translations once at startup so data-i18n labels render in
  // the user's saved language (not just on a later language change).
  _retranslateStaticDOM();

  // Picker chrome (selected-city card, intro counter) + Cmd+K search.
  initPicker(_citiesSummary, enterCity);
  initSearch(_citiesSummary, enterCity);
  setPickerSummary(_citiesSummary);
  setSearchSummary(_citiesSummary);

  // Engine picker (compartmental vs ABM). Mounts ABOVE the mode picker
  // so the engine choice reads as the first decision on the macro view.
  initEngineMode().catch(() => {});
  // Mode + difficulty picker (sandbox vs game). Mounts under the intro
  // side panel; reads the latest selection from /api/game_mode.
  initGameMode().catch(() => {});
  // Starting-season picker — mounts under the same intro panel.
  initSeasonPicker().catch(() => {});
  // Generic chrome-hover guard — silences map hovers while the cursor
  // is over any menu (timeline, settings, stats, modals, …).
  initUiInteractionGuard();

  // About + Settings: top-bar icon buttons across both views.
  const _openAbout = () => {
    document.getElementById('aboutBackdrop')?.classList.add('open');
    document.getElementById('aboutBackdrop')?.setAttribute('aria-hidden', 'false');
  };
  document.getElementById('btnAboutMacro')?.addEventListener('click', _openAbout);

  // Auto-open About once per browser session as a lightweight onboarding,
  // then stay quiet for the rest of the session. Cleared when the tab is
  // closed (sessionStorage), so a fresh session re-runs the onboarding.
  try {
    if (!sessionStorage.getItem('epicity_about_seen')) {
      sessionStorage.setItem('epicity_about_seen', '1');
      _openAbout();
    }
  } catch {}
  document.getElementById('btnSettingsMacro')?.addEventListener('click', () =>
    document.getElementById('settingsPop')?.classList.toggle('open'));
  document.getElementById('btnSettings')?.addEventListener('click', () => {
    const pop = document.getElementById('settingsPop');
    if (!pop) return;
    pop.classList.toggle('open');
    if (pop.classList.contains('open')) openPopup('settingsPop');
  });

  // City-view search button opens the building search popover.
  document.getElementById('btnCmdK')?.addEventListener('click', () => {
    openPopup('buildingSearch');
    openBuildingSearch();
  });
  // Cmd/Ctrl+K from the city view also opens it.
  window.addEventListener('keydown', e => {
    if (!(e.key === 'k' || e.key === 'K')) return;
    if (!(e.metaKey || e.ctrlKey)) return;
    if (document.getElementById('introSearchInput')?.offsetParent) return;   // intro view owns Cmd+K there
    e.preventDefault();
    openPopup('buildingSearch');
    openBuildingSearch();
  });
  initBuildingSearch(entry => {
    if (entry?.lon != null && entry?.lat != null) flyToBuilding([entry.lon, entry.lat], 16.5);
    // Apply the same selection effect as a real building click — yellow
    // outline, dimming, info-card refresh — so the user immediately sees
    // which building the search picked.
    if (entry && Number.isFinite(entry.idx)) selectByIdx(entry.idx);
  });

  // Pick patient zero — top-bar chip. Click toggles arm/disarm; clicking
  // again while armed cancels the pick. The chip mirrors _pickPzState
  // ('off' / 'waiting' / 'set') visually.
  document.getElementById('btnPickZero')?.addEventListener('click', () => {
    if (_pickPzState === 'waiting') _onPickPatientZeroToggle(false);
    else                            _onPickPatientZeroToggle(true);
  });

  // Close settings popover on outside click.
  document.addEventListener('click', e => {
    const pop = document.getElementById('settingsPop');
    if (!pop?.classList.contains('open')) return;
    if (pop.contains(e.target)) return;
    if (e.target.closest('#btnSettings, #btnSettingsMacro')) return;
    pop.classList.remove('open');
  });

  // Space = play/pause hotkey (when not typing in an input).
  window.addEventListener('keydown', e => {
    if (e.key !== ' ' && e.code !== 'Space') return;
    const t = e.target;
    if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return;
    e.preventDefault();
    document.getElementById('btnPlay')?.click();
  });
});

/**
 * Switch from macro to a specific city. Orchestrates the full transition:
 *   1. Show loading overlay (blocks clicks immediately)
 *   2. Animate the macro map zooming into the city's coordinates
 *      (the user sees this through the dimmed overlay)
 *   3. Fetch state in parallel with the camera fly
 *   4. Make the city view visible (still covered by the overlay) so
 *      MapLibre initialises with correct container dimensions — this
 *      is the fix for the "Three.js layer misaligned with base map"
 *      regression caused by initialising MapLibre while the container
 *      was display:none on the very first entry
 *   5. Build / reset the city UI
 *   6. Drop the macro view, hide the loading overlay
 */
async function enterCity(cityId) {
  noteRecentCity(cityId);
  // ── Phase 1: light overlay (transparent backdrop, just spinner+text) ─────
  // Lets the user watch the macro map zoom into the city while clicks
  // are absorbed by the overlay above.
  const loading      = document.getElementById('loadingOverlay');
  const loadingTitle = document.getElementById('loadingTitle');
  const loadingSub   = document.getElementById('loadingSub');
  const cityName = _cityNameOf(cityId);
  if (loadingTitle) loadingTitle.textContent = t('loading_city', { name: cityName });
  if (loadingSub)   loadingSub.textContent   = t('building_city');
  if (loading) loading.classList.add('show-flying');

  // Reveal the selected-city badge in the header so the user always
  // knows which city they're simulating. Cleared again by leaveCity().
  const _nameBadge = document.getElementById('cityNameBadge');
  if (_nameBadge) {
    _nameBadge.textContent = cityName;
    _nameBadge.classList.add('visible');
  }

  // Fly the macro camera AND fetch state in parallel.
  const flyP   = flyMacroToCity(cityId, { zoom: 13.5, pitch: 45, duration: 1200 });
  const stateP = fetch(`/api/select_city/${cityId}`, { method: 'POST' })
    .then(r => {
      if (!r.ok) throw new Error(`select_city ${cityId} → ${r.status}`);
      return r.json();
    });

  let state;
  try {
    [state] = await Promise.all([stateP, flyP]);
  } catch (e) {
    console.error(e);
    if (loading) {
      loading.classList.remove('show-flying');
      loading.classList.remove('show');
    }
    return;
  }
  _activeCityId = cityId;
  currentState  = state;

  // ── Phase 2: full dark overlay during city init ─────────────────────────
  if (loading) {
    loading.classList.remove('show-flying');
    loading.classList.add('show');
  }

  // Reveal the city view BEHIND the dark overlay so MapLibre's container
  // has real width/height when it initialises. The `transitioning` body
  // class temporarily displays both views at once. This is the fix for
  // the "Three.js layer misaligned with base map" regression — without
  // it, MapLibre would init with a 0×0 container and produce stale
  // matrices that the custom layer's onAdd captures permanently.
  document.body.classList.add('transitioning');
  document.body.classList.add('in-city');
  document.body.classList.remove('in-macro');

  // Build (first time) or reset (subsequent visits) the city UI
  if (!_cityInited) {
    await _initCityViewOnce();
    _cityInited = true;
  } else {
    _resetCityUI(state);
    // Rebuild the 3D scene + transport for the newly-selected city.
    // Without this, _initCityViewOnce's one-shot fetch leaves the
    // FIRST city's buildings on screen forever.
    await _loadCityScene();
  }
  // Use the *latest* state for the first render — _initCityViewOnce
  // may have re-fetched it after a conditional reseed, in which case
  // `state` (captured from /api/select_city above) is stale and would
  // paint the 🦠 pin at the wrong building until the first Day step.
  _applyState(currentState || state);
  // Surface the patient-zero pin immediately on city entry — previously
  // it was gated on the first Play click, which made it easy to miss in
  // large cities (Stockholm especially) where the user might scroll the
  // map around before pressing Play and never realise where the outbreak
  // started. Also fly the camera onto the seed building so the 🦠 marker
  // is dead-centre the moment the overlay fades out.
  _showOnlyPatientZeroPOI();
  _firstRunPoiApplied = true;
  // Try to restore a previously-cached timeline for this city BEFORE we
  // drop the loading overlay, so the user sees the full scrubber on first
  // paint instead of a momentarily-empty timeline that then pops in.
  await _restoreHistory(cityId);

  // Force MapLibre to recompute container dimensions on every entry
  // (defensive — cheap and prevents subtle resize bugs after the first
  // visit when CSS transitions or scrollbar appearance shift the layout).
  if (_cityMap && typeof _cityMap.resize === 'function') {
    _cityMap.resize();
    requestAnimationFrame(() => _cityMap.resize());
  }

  // Drop the transition class (macro hides) and fade out the overlay.
  document.body.classList.remove('transitioning');
  _view = 'city';
  if (loading) loading.classList.remove('show');
}

function _cityNameOf(cityId) {
  const c = _citiesSummary.find(x => x.id === cityId);
  if (c) return c.name;
  return cityId.charAt(0).toUpperCase() + cityId.slice(1);
}

/**
 * Return to macro view with a reverse zoom-out transition that mirrors
 * the entry animation. Pauses any running sim silently and asks the
 * backend to drop its active-city pointer (engine state stays warm in
 * the pool, ready for the next visit).
 *
 * Sequence:
 *   1. Show full dark overlay (covers the city → macro swap)
 *   2. Pause sim, POST leave_city, refresh badge summary
 *   3. Snap the macro map to the city's coordinates at high zoom (no
 *      animation — jumpTo). The user can't see this because the dark
 *      overlay is covering everything.
 *   4. Switch to macro view (city hidden, macro now showing the city
 *      area at high zoom). Drop overlay to its light phase so the user
 *      can watch the zoom-out.
 *   5. flyMacroToSweden() animates from the city to the country view.
 *   6. Hide overlay.
 */
async function leaveCity() {
  if (_view !== 'city') return;

  const leavingCityId = _activeCityId;
  const leavingCity = _citiesSummary.find(c => c.id === leavingCityId);

  // Silent pause (no confirmation per spec)
  if (running) {
    running = false;
    const btnPlay = document.getElementById('btnPlay');
    if (btnPlay) {
      _syncPlayIcon(false);
    }
  }

  // Phase 1: full dark overlay covers the swap.
  const loading      = document.getElementById('loadingOverlay');
  const loadingTitle = document.getElementById('loadingTitle');
  const loadingSub   = document.getElementById('loadingSub');
  if (loadingTitle) loadingTitle.textContent = t('returning_sweden');
  if (loadingSub)   loadingSub.textContent   = t('saving_state');
  if (loading) {
    loading.classList.remove('show-flying');
    loading.classList.add('show');
  }

  // Hide the selected-city badge so it doesn't hang over the macro view.
  const _nameBadge = document.getElementById('cityNameBadge');
  if (_nameBadge) _nameBadge.classList.remove('visible');

  // Flush any pending IndexedDB save synchronously so we don't lose the
  // most recent days that the debounce hasn't fired yet.
  if (_historySaveTimer !== null) {
    clearTimeout(_historySaveTimer);
    _historySaveTimer = null;
    if (leavingCityId && _history.length > 0) {
      saveHistory(leavingCityId, _history);
    }
  }

  // Phase 2: backend bookkeeping in parallel.
  const leaveP   = fetch('/api/leave_city', { method: 'POST' }).catch(() => {});
  const summaryP = fetch('/api/cities/summary').then(r => r.json()).catch(() => null);
  const [, summary] = await Promise.all([leaveP, summaryP]);
  _activeCityId = null;
  if (summary) {
    _citiesSummary = summary;
    refreshMacro(summary);
  }

  // Phase 3: switch to macro view (still hidden behind the dark overlay).
  // The order matters: change the body class FIRST so the macro
  // container becomes display:block, THEN call resizeMacro() so MapLibre
  // recomputes its canvas dimensions, THEN jumpTo. Without the resize
  // step, the macro map keeps the stale 0×0 size it acquired while
  // display:none and subsequent flyTo lands the camera at the wrong
  // screen position (the "Sweden in the corner" bug).
  document.body.classList.remove('in-city');
  document.body.classList.add('in-macro');
  _view = 'macro';

  // Force MapLibre to remeasure now that the container is visible.
  await resizeMacro();

  // Phase 4: pre-position the macro map at the city we're leaving so
  // the upcoming flyMacroToSweden has the city as its starting point.
  if (leavingCity) {
    await setMacroView([leavingCity.center_lon, leavingCity.center_lat], 13.5, 45);
  }

  // Phase 5: drop overlay to its light phase so the user can watch the
  // camera pull back to Sweden.
  if (loading) {
    loading.classList.remove('show');
    loading.classList.add('show-flying');
  }

  // Phase 6: animate the pull-back to the full Sweden view.
  await flyMacroToSweden({ duration: 1300 });

  // Phase 7: clear the overlay.
  if (loading) loading.classList.remove('show-flying');
}

function switchView(v) {
  _view = v;
  document.body.classList.toggle('in-macro', v === 'macro');
  document.body.classList.toggle('in-city',  v === 'city');
}

/**
 * One-time city UI bootstrap. Runs the first time the user enters any
 * city. The per-visit work (fetching city layout, rebuilding the 3D
 * scene + flow field + transport) is delegated to `_loadCityScene`
 * which is also called from `enterCity` on subsequent visits so
 * switching cities actually works.
 */
async function _initCityViewOnce() {
  // First-visit only: bring in initial layout so initMap can anchor.
  cityLayout = await fetch('/api/city').then(r => r.json());
  // Stash origin-group palette globally so legend.js + view.js can read
  // it without an extra import roundtrip. Set before buildLegend().
  if (cityLayout && cityLayout.origin_groups) {
    window._epiCityOriginGroups = cityLayout.origin_groups;
  }

  // Initialise MapLibre map — Three.js scene is created inside the custom layer
  const { map, ready } = initMap('map-container', cityLayout);
  _cityMap = map;

  // Build UI panels that don't depend on the 3D scene
  _buildInterventionButtons(cityLayout.interventions);
  buildLegend(cityLayout, _onVizChange);

  // Wait for MapLibre + Three.js to be ready before any 3D mutations.
  await ready;

  // Wire scene callbacks ONCE
  setHoverCallback(_onHover);
  setSelectionCallback(_onSelect);

  // Build the 3D scene + flow + transport for this first city visit
  await _loadCityScene();

  // Track mouse for tooltip positioning (ONCE)
  document.addEventListener('mousemove', e => {
    mouseX = e.clientX; mouseY = e.clientY;
    _onTransportMouseMove();
  });
  // Only handle transport clicks on the map canvas — not on UI buttons
  const mapContainer = document.getElementById('map-container');
  if (mapContainer) mapContainer.addEventListener('click', _onTransportClick);

  // Patient-zero policy on fresh entry: clear any stale manual pin and
  // ensure random=true so the engine always seeds a central outbreak
  // the user can watch spread. The pick-zero button is the escape
  // hatch for reseeding anywhere the user prefers.
  //
  // Both endpoints trigger eng.reset() on the backend, which re-picks
  // patient zero — so calling them unconditionally on every entry made
  // the camera fly to multiple candidates in succession and left the
  // 🦠 pin at a stale building until the first Day step. We now POST
  // only when the engine's state genuinely doesn't match the policy
  // (no manual pin, random mode on), so a freshly-seeded engine after
  // /api/select_city is left alone — one seed, one fly, one pin.
  //
  // Skipped in game mode — the backend manages seeding (multi-cluster
  // in crowded venues) and reset on /api/select_city, and both endpoints
  // return 403 with game on.
  const _gameOn = document.body.classList.contains('game-mode');
  let _seedChanged = false;
  if (!_gameOn) {
    const _sm = currentState?.simulation_mode || {};
    if (_sm.manual_patient_zero_idx != null) {
      try {
        currentState = await fetch('/api/manual_patient_zero', {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ idx: null }),
        }).then(r => r.json());
        _seedChanged = true;
      } catch {}
    }
    if (currentState?.simulation_mode?.random_patient_zero === false) {
      try {
        currentState = await fetch('/api/simulation_mode', {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ random_patient_zero: true }),
        }).then(r => r.json());
        _seedChanged = true;
      } catch {}
    }
  } else {
    // Fetch a fresh state so currentState reflects the game-mode
    // reset done by /api/select_city (multi-seed, scaled β/μ).
    try {
      currentState = await fetch('/api/state').then(r => r.json());
    } catch {}
  }
  // Only re-fly the camera if a POST above actually re-picked the seed.
  // _loadCityScene has already flown to the original patient zero in
  // the common case, so a second flyTo to the SAME building would just
  // restart the animation and look like a stutter.
  if (_seedChanged) {
    const pzIdx = Array.isArray(currentState?.patient_zero) ? currentState.patient_zero[0] : null;
    if (pzIdx != null && cityLayout?.buildings?.[pzIdx]) {
      const pz = cityLayout.buildings[pzIdx];
      if (pz?.lon != null && pz?.lat != null) flyToBuilding([pz.lon, pz.lat], 15.6);
    }
  }

  // Push legend defaults into view.js before first paint
  _onVizChange();

  _bindControls(map, cityLayout);
  _bindCollapsibles();
  _bindFlowPanel();
  _bindAgentsLayerToggle();
  _bindNaturePanel();
  _bindHeaderHoverPanels();
  _bindTransportPanel();
  _bindTimeline();
  buildSettingsPanel(currentState.params, currentState.simulation_mode,
                     _onModeChange, _onPickPatientZeroToggle);
  // Map-driven spatial intervention palette (lasso, mobile clinics, etc.)
  initSpatialTools();
  // Quarantine-border layer (maplibre); 3D ambulances live in the
  // THREE scene and use the same city road network as buildings.
  ensureSpatialOverlay();
  setSpatialCityLayout(cityLayout);
  ambulance3d.setCityData(cityLayout);
  // Two-state pick model: chip is always idle on city load. The 🦠 pin
  // already conveys whether a seed is set; no extra chip state needed.
  _pickPzState = 'off';
  _syncPickZeroBtn?.();
  bindSettingsToggle();
  initChart();
  initSidebar();
  initGauge();
  initPoi(_cyclePoiCamera);

  // POI popover: open on hover so a click can stay dedicated to toggling
  // the POI layer visibility (the previous click-to-open made the layer
  // un-toggle in the same gesture). The popover stays open while the
  // cursor is over either the row or the popover itself.
  // Hover popovers only open when their underlying layer is ACTIVE —
  // otherwise hovering the row would surface a popover for a layer
  // that isn't even drawn on the map.
  const _isOn = el => !!el && (el.classList.contains('active') || el.classList.contains('on'));

  const _poiBtn = document.getElementById('btnPOI');
  const _poiPop = document.getElementById('poiPop');
  let _poiHoverTimer = null;
  const _poiOpen  = () => { if (!_isOn(_poiBtn)) return; clearTimeout(_poiHoverTimer); openPoiPopover(); };
  const _poiClose = () => { _poiHoverTimer = setTimeout(closePoiPopover, 220); };
  _poiBtn?.addEventListener('mouseenter', _poiOpen);
  _poiBtn?.addEventListener('mouseleave', _poiClose);
  _poiPop?.addEventListener('mouseenter', _poiOpen);
  _poiPop?.addEventListener('mouseleave', _poiClose);

  // Transport popover — same hover pattern, also gated on activation.
  initTransportPop();
  const _trBtn = document.getElementById('btnTransport');
  const _trPop = document.getElementById('transportPop');
  let _trTimer = null;
  const _trOpen  = () => { if (!_isOn(_trBtn)) return; clearTimeout(_trTimer); openTransportPop(); };
  const _trClose = () => { _trTimer = setTimeout(closeTransportPop, 220); };
  _trBtn?.addEventListener('mouseenter', _trOpen);
  _trBtn?.addEventListener('mouseleave', _trClose);
  _trPop?.addEventListener('mouseenter', _trOpen);
  _trPop?.addEventListener('mouseleave', _trClose);

  // Infection-flow popover — opens on hover of the Flow row when the
  // overlay is active, exactly like Transport. The controls write
  // through to setInfectionFlowOptions; choices are persisted in
  // localStorage so the user's preference survives a reload.
  _bindFlowPop();

  _startLoop();
}

// ── Infection-flow popover ────────────────────────────────────────────────────

// Bumped key suffix so anyone who saved flow options under the previous
// defaults (constant/cumulative/arrow-on) gets the new intensity-first
// defaults on next page load. Old entries are left in localStorage but
// no longer read; they age out on their own.
const _FLOW_OPT_LS_KEY = 'epicity.flowOptions.v2';
const _FLOW_OPT_DEFAULTS = {
  arrowhead:     false,
  colorMode:     'intensity',
  thicknessMode: 'intensity',
  speedMode:     'intensity',
  temporal:      'timestamp',
};
function _loadFlowOpts() {
  try {
    const raw = localStorage.getItem(_FLOW_OPT_LS_KEY);
    if (!raw) return { ..._FLOW_OPT_DEFAULTS };
    const parsed = JSON.parse(raw);
    return { ..._FLOW_OPT_DEFAULTS, ...(parsed && typeof parsed === 'object' ? parsed : {}) };
  } catch {
    return { ..._FLOW_OPT_DEFAULTS };
  }
}
function _saveFlowOpts(opts) {
  try { localStorage.setItem(_FLOW_OPT_LS_KEY, JSON.stringify(opts)); } catch {}
}
let _flowPopHoverTimer = null;
function _openFlowPop()  { document.getElementById('flowPop')?.classList.add('open'); }
function _closeFlowPop() { document.getElementById('flowPop')?.classList.remove('open'); }

function _bindFlowPop() {
  const flowBtn = document.getElementById('btnFlow');
  const flowPop = document.getElementById('flowPop');
  if (!flowBtn || !flowPop) return;
  // Hydrate controls from persisted options + push them into the overlay
  // module so the first render matches the user's last choice.
  const opts = _loadFlowOpts();
  const $color = document.getElementById('flowColorMode');
  const $thick = document.getElementById('flowThicknessMode');
  const $speed = document.getElementById('flowSpeedMode');
  const $temp  = document.getElementById('flowTemporal');
  const $arrow = document.getElementById('flowArrowhead');
  if ($color) $color.value = opts.colorMode;
  if ($thick) $thick.value = opts.thicknessMode;
  if ($speed) $speed.value = opts.speedMode;
  if ($temp)  $temp.value  = opts.temporal;
  if ($arrow) $arrow.checked = !!opts.arrowhead;
  setInfectionFlowOptions(opts);

  const apply = () => {
    const next = {
      colorMode:     $color?.value || 'constant',
      thicknessMode: $thick?.value || 'constant',
      speedMode:     $speed?.value || 'constant',
      temporal:      $temp?.value  || 'cumulative',
      arrowhead:     !!$arrow?.checked,
    };
    setInfectionFlowOptions(next);
    _saveFlowOpts(next);
  };
  [$color, $thick, $speed, $temp].forEach(el => el?.addEventListener('change', apply));
  $arrow?.addEventListener('change', apply);

  // Hover affordance — only opens when the layer itself is on, mirroring
  // the Transport / POI popovers. Clicking the row still toggles the
  // overlay; the popover is purely a "settings tray" for the active
  // layer.
  const isOn = el => !!el && (el.classList.contains('active') || el.classList.contains('on'));
  const open  = () => { if (!isOn(flowBtn)) return; clearTimeout(_flowPopHoverTimer); _openFlowPop(); };
  const close = () => { _flowPopHoverTimer = setTimeout(_closeFlowPop, 220); };
  flowBtn.addEventListener('mouseenter', open);
  flowBtn.addEventListener('mouseleave', close);
  flowPop.addEventListener('mouseenter', open);
  flowPop.addEventListener('mouseleave', close);
}

// Camera-cycle delegate handed to poi.js; resolves the current bucket
// (group:<gid> or poi:<key>) and flies the camera to the next/previous
// instance. Implementation lives further down in the existing POI nav code.
function _cyclePoiCamera(bucketKey, dir) {
  const list = _poiBuckets[bucketKey];
  if (!list || !list.length) return;
  const cur = _poiCursor[bucketKey] ?? -1;
  const next = (cur + dir + list.length) % list.length;
  _poiCursor[bucketKey] = next;
  const target = list[next];
  if (target?.lon != null && target?.lat != null) {
    flyToBuilding([target.lon, target.lat], 16.5);
  }
}

/**
 * Per-visit scene rebuild. Called on EVERY city entry — first visit
 * (from _initCityViewOnce) and every subsequent visit (from
 * enterCity's else branch). Refetches /api/city + /api/transport for
 * the active city and rebuilds the 3D geometry, flow field, and
 * transport layer. Cheap for warm engines because the backend caches
 * the city layout per city.
 */
async function _loadCityScene() {
  // Refetch the active city's layout — without this the first city's
  // buildings stay on screen on every subsequent visit.
  cityLayout = await fetch('/api/city').then(r => r.json());
  if (cityLayout && cityLayout.origin_groups) {
    window._epiCityOriginGroups = cityLayout.origin_groups;
  }

  // Re-anchor the MapLibre map projection on the new city. Without
  // this, world_x/world_z (which are RELATIVE to each city's center)
  // get rendered through the FIRST city's projection, and the new
  // buildings appear at the wrong screen position. rebuildForCity
  // also swaps the road layer for the new city's roads.
  rebuildForCity(cityLayout);

  // Rebuild 3D city geometry. buildCity disposes the previous mesh
  // internally so this is safe to call repeatedly.
  buildCity(cityLayout);
  // Re-wire ambulance3d's city data so any new mobile clinic spawns
  // against the new city's buildings + road graph. Without this, the
  // module keeps the FIRST city's data and a clinic placed in city #2
  // gets snapped to the nearest building in city #1 — making the camera
  // fly back to the previous city on drop.
  ambulance3d.clearAll();
  ambulance3d.setCityData(cityLayout);
  initMinimap(cityLayout);   // no-op stub
  buildSearchIndex(cityLayout);
  // Wipe the SEIR baseline so trend arrows don't compare across cities.
  _lastSEIR = null;
  // Re-sync the pick-zero button so it reflects the new city's state.
  _syncPickZeroBtn();
  // Initialise the infection-flow overlay once the map exists.
  initInfectionFlow(getMap());
  updateInfectionFlow(currentState, cityLayout);
  // Initialise the agents-as-points layer on the three.js scene. Safe
  // to call repeatedly — the layer keeps its GPU buffers between city
  // switches and just resets its draw range.
  initAgentsLayer({ scene: getScene() });
  // On every city entry: if a patient zero exists in state, fly to it
  // so the user always lands on the outbreak origin. Random PZ is
  // central-biased on the backend so this is meaningful out of the
  // box; manual picks fly there at commit time too.
  const _pzIdx = Array.isArray(currentState?.patient_zero) ? currentState.patient_zero[0] : null;
  if (_pzIdx != null && cityLayout?.buildings?.[_pzIdx]) {
    const pz = cityLayout.buildings[_pzIdx];
    if (pz?.lon != null && pz?.lat != null) flyToBuilding([pz.lon, pz.lat], 15.6);
  }

  // Lightweight nature POIs (nature reserves, cemeteries, …) live
  // alongside the buildings array in /api/city. Feed them into the
  // pin layer before the POI availability pass so their counts are
  // reflected in the menu badges.
  setNaturePOIs(cityLayout.nature_pois || []);
  // Swedish flag pole easter egg — builds the 3D assets AND registers
  // pins for the POI menu toggle.
  setFlagPoles(cityLayout.flag_poles || []);
  // LNU (Linnaeus University) flag poles on the Växjö + Kalmar campus.
  setLNUFlagPoles(cityLayout.lnu_flag_poles || []);
  // Swedish cultural easter eggs — Dalahäst, IKEA bag, fika cup, maypole, moose.
  setEasterEggs(cityLayout.easter_eggs || []);

  // Flow field — instantiated lazily on first visit, rebuilt every
  // visit thereafter. .build() disposes the previous geometry.
  if (!_flowField) {
    _flowField = new FlowField(getScene());
  }
  _flowField.build(cityLayout);

  // Transport layer — refetched every visit. transport.buildTransport
  // calls dispose() internally so the previous city's vehicles are
  // cleaned up before the new ones are built.
  _transportLoaded = false;
  _transportHoveredVi = -1; _transportSelectedVi = -1;
  _hideTransportTip();
  try {
    let tdata = await fetch('/api/transport').then(r => r.json());
    if (tdata && tdata.infra && tdata.schedule) {
      // Filter non-airport stops to city border; airports + planes kept as-is
      const filteredStops = _filterStopsByBorder(tdata.infra.stops || [], cityLayout);
      const filteredTdata = {
        ...tdata,
        infra: { ...tdata.infra, stops: filteredStops },
        buildings: cityLayout.buildings,
      };
      _transportLoaded = transport.buildTransport(getScene(), filteredTdata);
      transport.setVisible(_transportActive);
      setTransportStops(filteredStops);
      _poiTransportStops = filteredStops;
      showTransportLines(tdata, cityLayout.center_lat, cityLayout.center_lon);
      _updateTransportHighlightPanel(tdata);
    } else {
      // No transport data for this city — clean up any leftover
      // vehicles from the previous city before clearing the POIs.
      transport.dispose();
      setTransportStops([]);
      _poiTransportStops = [];
    }
  } catch (e) {
    console.warn('[transport] fetch failed:', e);
    transport.dispose();
    setTransportStops([]);
  }

  // Now that buildings + transport stops are loaded, grey-out POI
  // categories that don't exist in this city.
  _updatePOIAvailability();
}

/**
 * Reset the per-city UI on re-entry without rebuilding the 3D scene or
 * re-initialising MapLibre. Mirrors the cleanup the Reset button does.
 * Engine state is intact server-side; only the *frontend* affordances
 * are wiped (POI menu, panel collapse, selection, day-night, settings
 * panel mode block).
 */
function _resetCityUI(state) {
  // Clear any building selection from a previous visit
  clearSelection();
  // Reset areas overlay (neighborhoods get rebuilt per city)
  setAreasOverlay(false);
  const btnNbh = document.getElementById('btnNeighborhoods');
  if (btnNbh) btnNbh.classList.remove('active', 'on');
  // Drop POI layer + uncheck every category, re-arm first-Play behavior
  _resetPOIState();
  // Reset weather + day-night icon
  resetWeather();
  _updateDayNightIcon('clear');
  // Make sure the Play button is in the paused state
  running = false;
  _syncPlayIcon(false);
  // Drop interventions highlights
  document.querySelectorAll('.iv-row, .iv-btn').forEach(b => b.classList.remove('active', 'on'));
  // Rebuild the settings panel from the new state's mode block (so the
  // Pick-PZ checkbox, Random checkbox, and Seed input all reflect reality)
  buildSettingsPanel(state.params, state.simulation_mode,
                     _onModeChange, _onPickPatientZeroToggle);
  _pickPzState = state.simulation_mode?.manual_patient_zero_idx != null
    ? 'set' : 'off';
  // Drop any pending step from a previous run
  _pendingStateFromStep = null;
  _animTarget           = null;
  _stepEpoch += 1;
  _dayStartTs = null;
  flowHour    = 0.0;
  // Drop the in-memory timeline scrubber state. Persisted history is
  // intentionally preserved here — _restoreHistory() runs immediately
  // after _applyState() in enterCity() and will repopulate _history
  // from IndexedDB if the cache is consistent with the engine day.
  _stopReplay();
  _history    = [];
  _historyIdx = -1;
  if (_historySaveTimer !== null) { clearTimeout(_historySaveTimer); _historySaveTimer = null; }
  const tlPanel = document.getElementById('timelinePanel');
  if (tlPanel) tlPanel.classList.add('day-zero');
}

// ── Day/Night button icon ─────────────────────────────────────────────────────

/**
 * Swap the leading glyph of the #btnDayNight label based on current weather.
 *   clear → ☀   snow → ❄   rain → 🌧
 * Only the icon and title change; the "Day/Night" label text stays the same.
 */
function _updateDayNightIcon(weather) {
  const btn = document.getElementById('btnDayNight');
  if (!btn) return;
  // The new layer-row markup carries a child SVG icon — never overwrite
  // the row's textContent (that would nuke the icon and the slider).
  // The weather state is only reflected via the [data-weather] hook the
  // CSS reads; no text edit needed.
  let title;
  switch (weather) {
    case 'snow':   title = t('daynight_snow');     break;
    case 'rain':   title = t('daynight_rain');     break;
    case 'cloudy': title = t('daynight_cloud');    break;
    default:       title = t('daynight_title');    break;
  }
  btn.title = title;
  if (weather === 'snow' || weather === 'rain' || weather === 'cloudy') {
    btn.dataset.weather = weather;
  } else {
    delete btn.dataset.weather;
  }
}

// ── POI panel ─────────────────────────────────────────────────────────────────

/**
 * Programmatically force the POI layer ON with Patient Zero as the only
 * visible category. Mirrors the user-driven menu state by updating the
 * checkboxes (so reopening the menu shows the right ticks) and the POI
 * button's "active" highlight. Called once on the first Play click.
 */
function _showOnlyPatientZeroPOI() {
  const list = document.getElementById('poiList');
  const btn  = document.getElementById('btnPOI');
  if (!list || !btn) return;

  // Update each per-category checkbox + push to view.js
  list.querySelectorAll('input[data-key]').forEach(cb => {
    const key  = cb.dataset.key;
    const want = (key === PATIENT_ZERO_KEY);
    cb.checked = want;
    if (key === PATIENT_ZERO_KEY) {
      setPOICategoryVisible(PATIENT_ZERO_KEY, true);
    } else {
      // Pass key as-is — view.js String()s it. `Number(key)` would turn
      // string keys like "airport" into NaN.
      setPOICategoryVisible(key, false);
    }
  });
  // Master "Select all" checkbox should drop to its mixed/off state
  const allBox = document.getElementById('poi-select-all');
  if (allBox) {
    allBox.checked       = false;
    allBox.indeterminate = true;
  }

  // Mirror the visual "POI button active" state and turn the layer on
  btn.classList.add('active');
  setPOIVisible(true);
}


/**
 * Populate the POI popup with one checkbox per category, wire the button
 * toggle, and bind outside-click to close. Called once from _bindControls.
 */
function _bindPOIControls() {
  const btn   = document.getElementById('btnPOI');
  const panel = document.getElementById('poiPanel');
  const list  = document.getElementById('poiList');
  if (!btn || !panel || !list) return;

  // Swallow pointer events so clicks on checkboxes don't fall through
  // to the map canvas and trigger a building selection.
  ['click', 'mousedown', 'mouseup', 'pointerdown', 'pointerup'].forEach(ev => {
    panel.addEventListener(ev, e => e.stopPropagation());
  });

  // Build the POI checkbox list. Extracted so _updatePOIAvailability can
  // rebuild it when the city changes.
  _buildPOIList(list);

  // Panel opens on hover (CSS .header-hover-wrap:hover > #poiPanel).
  // Button click still toggles the POI layer on/off. When activating
  // we also auto-open the popover so the user can pick categories
  // straight away; deactivating closes it.
  let poiOn = false;
  btn.addEventListener('click', ev => {
    ev.stopPropagation();
    poiOn = !poiOn;
    btn.classList.toggle('active', poiOn);
    setPOIVisible(poiOn);
    if (poiOn) openPoiPopover();
    else       closePoiPopover();
  });
}

/**
 * Wire the Population panel info button to show a fixed-position tooltip
 * with SEIR compartment descriptions and R_eff explanation. Instant show.
 */
function _bindPopInfoTooltip() {
  // Bind a hover trigger to a tooltip element. `placement`: 'above' (the
  // default) hovers above the trigger; 'below' drops it under, leaving a
  // 12 px gap. The tooltip is pointer-events:none in CSS so it can
  // never catch the cursor and trigger a hide/show flicker loop.
  const _bind = (trigger, tip, placement) => {
    if (!trigger || !tip) return;
    trigger.addEventListener('mouseenter', () => {
      tip.style.display = 'block';
      const r = trigger.getBoundingClientRect();
      tip.style.right = 'auto';
      tip.style.left  = `${Math.max(8, r.left)}px`;
      if (placement === 'below') {
        tip.style.top = `${r.bottom + 12}px`;
      } else {
        tip.style.top = `${Math.max(8, r.top - tip.offsetHeight - 12)}px`;
      }
    });
    trigger.addEventListener('mouseleave', () => {
      tip.style.display = 'none';
    });
  };

  const seirTip   = document.getElementById('popInfoTip');
  const reffTip   = document.getElementById('reffInfoTip');
  const growthTip = document.getElementById('growthInfoTip');
  // KPI strip lives at the very top of the screen, so the SEIR
  // explainer drops BELOW it — clears the strip cleanly and the gap
  // prevents the cursor from ever touching the tooltip.
  _bind(document.getElementById('popInfoBtn'), seirTip, 'below');
  _bind(document.getElementById('kpiStrip'),   seirTip, 'below');
  // R-eff and Growth pills live at the bottom; their tooltips float ABOVE.
  _bind(document.getElementById('reffPill'),   reffTip,   'above');
  _bind(document.getElementById('growthPill'), growthTip, 'above');

  // Split-mode per-side overlays. Each overlay holds a row of pills
  // (R-eff · S · E · I · R · D · Growth) — different tooltip per pill.
  // Use event delegation so we don't have to rebind on every overlay
  // re-render (innerHTML is rewritten whenever a day changes).
  _bindOverlayPillTips('kpiOverlayA', seirTip, reffTip, growthTip);
  _bindOverlayPillTips('kpiOverlayB', seirTip, reffTip, growthTip);
}

/** Delegated mouseover/mouseout on a per-side overlay. Picks the right
 *  explainer tooltip based on the hovered pill: `.kpi.reff` shows
 *  reffTip, `.kpi.growth` shows growthTip, anything else (S/E/I/R/D
 *  or the head row) falls back to seirTip. The tooltip is anchored
 *  to whatever was hovered so it lines up under the right pill. */
function _bindOverlayPillTips(overlayId, seirTip, reffTip, growthTip) {
  const overlay = document.getElementById(overlayId);
  if (!overlay) return;
  let activeTip = null;
  const hide = () => {
    if (activeTip) { activeTip.style.display = 'none'; activeTip = null; }
  };
  const show = (tip, anchor) => {
    if (!tip) return hide();
    if (activeTip && activeTip !== tip) activeTip.style.display = 'none';
    activeTip = tip;
    tip.style.display = 'block';
    const r = anchor.getBoundingClientRect();
    tip.style.right = 'auto';
    // Centre the tooltip horizontally on the hovered pill / overlay,
    // then clamp to the viewport so far-right anchors (Growth pill on
    // overlay B) don't push it off-screen.
    const tipW       = tip.offsetWidth || 280;
    const anchorMid  = r.left + r.width / 2;
    const left       = Math.max(8,
                       Math.min(window.innerWidth - tipW - 8,
                                anchorMid - tipW / 2));
    tip.style.left = `${left}px`;
    tip.style.top  = `${r.bottom + 12}px`;
  };
  overlay.addEventListener('mouseover', (e) => {
    const pill = e.target.closest('.kpi');
    if (pill?.classList.contains('reff'))   return show(reffTip,   pill);
    if (pill?.classList.contains('growth')) return show(growthTip, pill);
    show(seirTip, pill || overlay);
  });
  overlay.addEventListener('mouseout', (e) => {
    // Hide only when leaving the overlay entirely — moving between
    // pills inside the overlay should swap tooltips, not flicker.
    if (e.relatedTarget && overlay.contains(e.relatedTarget)) return;
    hide();
  });
}

// ── Expanded-group state persistence ────────────────────────────────────────
// localStorage key → Set of group ids currently expanded. Persists across
// reloads so the user doesn't have to re-open their preferred groups.
const _POI_GROUP_STATE_KEY = 'epicity.poiGroupsOpen';

function _loadPOIGroupState() {
  try {
    const raw = localStorage.getItem(_POI_GROUP_STATE_KEY);
    if (!raw) return new Set();
    return new Set(JSON.parse(raw));
  } catch {
    return new Set();
  }
}
function _savePOIGroupState(openSet) {
  try {
    localStorage.setItem(_POI_GROUP_STATE_KEY,
      JSON.stringify([...openSet]));
  } catch { /* ignore quota / private mode */ }
}

/**
 * Build (or rebuild) the POI checkbox list inside the given container.
 * Only categories that exist in the current city are shown. An optional
 * `available` set can be passed; if omitted, all categories are included
 * (used on first init before city data is loaded).
 *
 * Categories are grouped by their `group` field (see POI_CATEGORIES +
 * POI_GROUPS in view.js). Each group renders as a collapsible
 * <details>/<summary> section with:
 *   - a caret + group name + a group-level checkbox on the header
 *   - nested child rows indented underneath
 *   - open state persisted to localStorage
 *   - the group checkbox toggles all children (tri-state: all / some / none)
 */
/**
 * Walk the current city layout + transport stops + nature POIs +
 * flag poles and group them by POI key so the left/right arrow
 * buttons on the POI panel can fly the camera through each cluster.
 *
 * Returns a map of:
 *   poi:<key>   → [{lon, lat}, ...] for a single category
 *   group:<gid> → [{lon, lat}, ...] for every category in the group
 */
// Cached yellow-disc LNU logo rendered to a data URL for the POI menu
// row. Drawn at 2× resolution so it stays crisp inside the 14 px .poi-dot.
let _lnuDotDataURL = null;
function _getLNUDotDataURL() {
  if (_lnuDotDataURL) return _lnuDotDataURL;
  const S = 28;
  const c = document.createElement('canvas');
  c.width = S; c.height = S;
  const ctx = c.getContext('2d');
  ctx.beginPath();
  ctx.arc(S / 2, S / 2, S / 2, 0, Math.PI * 2);
  ctx.fillStyle = '#fcd116';
  ctx.fill();
  drawLNUTree(ctx, S / 2, S / 2 + 1, S * 0.92);
  _lnuDotDataURL = c.toDataURL('image/png');
  return _lnuDotDataURL;
}

function _buildPOIBuckets() {
  const buckets = {};
  const push = (key, obj) => {
    if (obj == null || obj.lon == null || obj.lat == null) return;
    (buckets[key] ||= []).push({ lon: obj.lon, lat: obj.lat });
  };

  if (cityLayout?.buildings) {
    for (const b of cityLayout.buildings) {
      if (b.zone === 19 && b.water_type) {
        const wk = 'water_' + b.water_type;
        if (POI_CATEGORIES[wk]) push('poi:' + wk, b);
      } else if (POI_CATEGORIES[b.zone]) {
        push('poi:' + b.zone, b);
      }
    }
  }
  for (const s of _poiTransportStops) {
    if (POI_CATEGORIES[s.type]) push('poi:' + s.type, s);
  }
  if (cityLayout?.nature_pois) {
    for (const p of cityLayout.nature_pois) {
      if (POI_CATEGORIES[p.type]) push('poi:' + p.type, p);
    }
  }
  if (cityLayout?.flag_poles) {
    for (const f of cityLayout.flag_poles) push('poi:swedish_flag', f);
  }
  if (cityLayout?.lnu_flag_poles) {
    for (const f of cityLayout.lnu_flag_poles) push('poi:lnu_flag', f);
  }
  // Swedish cultural easter eggs
  if (cityLayout?.easter_eggs) {
    for (const e of cityLayout.easter_eggs) push('poi:' + e.type, e);
  }
  // Patient Zero: resolve each seed building idx to its lon/lat so the
  // nav arrows can fly the camera through the current outbreak seeds.
  const pzIdx = currentState?.patient_zero;
  if (Array.isArray(pzIdx) && cityLayout?.buildings) {
    for (const idx of pzIdx) {
      const b = cityLayout.buildings[idx];
      if (b) push('poi:' + PATIENT_ZERO_KEY, b);
    }
  }

  // Group buckets — union every category's positions by cat.group.
  for (const [key, cat] of Object.entries(POI_CATEGORIES)) {
    if (!cat.group) continue;
    const pts = buckets['poi:' + key];
    if (!pts || pts.length === 0) continue;
    const gk = 'group:' + cat.group;
    if (!buckets[gk]) buckets[gk] = [];
    buckets[gk].push(...pts);
  }
  return buckets;
}

/**
 * Advance the cursor for a POI bucket and fly the camera to the
 * next (dir=+1) or previous (dir=-1) target. Cursor wraps around
 * at both ends. Does nothing for empty buckets.
 */
function _poiJumpTo(bucketKey, dir) {
  const pts = _poiBuckets[bucketKey];
  if (!pts || pts.length === 0) return;
  const cur = _poiCursor[bucketKey] ?? -1;
  const next = (cur + dir + pts.length) % pts.length;
  _poiCursor[bucketKey] = next;
  const p = pts[next];
  flyToBuilding([p.lon, p.lat], 16.2);
  // Update the counter label for this bucket.
  const counter = document.querySelector(`.poi-counter[data-bucket="${bucketKey}"]`);
  if (counter) counter.textContent = `${next + 1}/${pts.length}`;
}

function _buildPOIList(list, available) {
  // Remember which checkbox states were in place so the rebuild
  // doesn't feel like a wipe. Arrow cursors persist via _poiCursor
  // across rebuilds.
  const prevChecked = {};
  list.querySelectorAll('input[data-key]').forEach(cb => {
    prevChecked[cb.dataset.key] = cb.checked;
  });

  list.innerHTML = '';
  const openGroups = _loadPOIGroupState();

  // One-shot snapshot of per-category counts so every addRow() call gets
  // its N without re-walking the buildings array.
  const counts = getPOICounts();

  // Rebuild the camera-navigation bucket lookup every time the POI
  // list refreshes (city switch, patient-zero pin, transport load, …).
  _poiBuckets = _buildPOIBuckets();

  // Helper: add a row for a single POI category.
  //
  // Default-checked rule on first load (when prevChecked[key] is undefined):
  //   - Patient Zero → CHECKED (epidemic seed is the main reason to open
  //     the panel, so always show it).
  //   - Everything else → UNCHECKED, so a fresh session starts with an
  //     empty map and the user explicitly opts in to each category or
  //     group. Previously everything defaulted to checked, which was
  //     inconsistent with the group header state on reopen.
  const addRow = (parent, key, cat, onChange) => {
    if (available && !available.has(key)) return null;
    const defaultOn = (key === PATIENT_ZERO_KEY);
    const wasChecked = prevChecked[key] ?? defaultOn;
    const n = counts[key] || 0;
    // Always render the arrows — even single-POI categories get them
    // so the UI is consistent. _poiJumpTo safely cycles (0 % 1 = 0)
    // when the bucket has only one entry, so repeated clicks just
    // re-fly to the same point.
    const bucketKey = 'poi:' + key;
    const cursorPos = _poiCursor[bucketKey];
    const counterText = cursorPos != null ? `${cursorPos + 1}/${n}` : `0/${n}`;
    const navHtml = `<span class="poi-nav-group">
      <button type="button" class="poi-nav poi-nav-prev" data-bucket="${bucketKey}" title="${t('previous')}">‹</button>
      <span class="poi-counter" data-bucket="${bucketKey}">${counterText}</span>
      <button type="button" class="poi-nav poi-nav-next" data-bucket="${bucketKey}" title="${t('next')}">›</button>
    </span>`;
    // LNU gets the actual yellow circle-tree brand logo in the row
    // dot so the menu matches the pin icon on the 3D map.
    const dotStyle = (key === 'lnu_flag')
      ? `background:#fcd116 center/contain no-repeat url("${_getLNUDotDataURL()}")`
      : `background:${cat.color}`;
    const dotGlyph = (key === 'lnu_flag') ? '' : cat.emoji;
    const row = document.createElement('label');
    row.className = 'poi-item';
    row.innerHTML = `
      <span class="poi-dot" style="${dotStyle}">${dotGlyph}</span>
      <input type="checkbox" ${wasChecked ? 'checked' : ''} data-key="${key}">
      <span class="poi-name">${poiName(key)}</span>
      ${navHtml}
    `;
    const cb = row.querySelector('input');
    cb.addEventListener('change', ev => onChange(ev.target.checked));
    onChange(wasChecked);   // sync view with restored state
    // Wire the arrow buttons. Stop propagation so the enclosing
    // <label> doesn't re-toggle the checkbox; preventDefault so
    // clicking inside the label doesn't trigger label→input activation.
    row.querySelectorAll('.poi-nav').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        const dir = btn.classList.contains('poi-nav-next') ? 1 : -1;
        _poiJumpTo(btn.dataset.bucket, dir);
      });
    });
    parent.appendChild(row);
    return row;
  };

  // ── Patient Zero stays ungrouped at the top — it's the epidemic seed,
  // not a location category ──
  addRow(list, PATIENT_ZERO_KEY, PATIENT_ZERO_META, checked => {
    setPOICategoryVisible(PATIENT_ZERO_KEY, checked);
  });

  const pzDivider = document.createElement('div');
  pzDivider.className = 'poi-divider';
  list.appendChild(pzDivider);

  // ── Bucket categories by their `group` field ──
  const buckets = {};
  for (const [zid, cat] of Object.entries(POI_CATEGORIES)) {
    const gid = cat.group || 'other';
    if (!buckets[gid]) buckets[gid] = [];
    buckets[gid].push([zid, cat]);
  }

  // Sort group IDs by the `order` field in POI_GROUPS; unknown groups
  // fall back to the "other" bucket at the end.
  const groupIds = Object.keys(buckets).sort((a, b) => {
    const oa = POI_GROUPS[a]?.order ?? 100;
    const ob = POI_GROUPS[b]?.order ?? 100;
    return oa - ob;
  });

  // Helper: render a group as a collapsible <details> with a tri-state
  // group-level checkbox on the summary.
  const buildGroup = (gid, entries) => {
    const groupMeta = POI_GROUPS[gid] || POI_GROUPS.other;
    // If every entry is filtered out by `available`, skip the whole group.
    const visibleEntries = available
      ? entries.filter(([k]) => available.has(k))
      : entries;
    if (visibleEntries.length === 0) return;

    const details = document.createElement('details');
    details.className = 'poi-group';
    if (openGroups.has(gid)) details.open = true;

    const groupBucketKey = 'group:' + gid;
    const groupBucketLen = (_poiBuckets[groupBucketKey] || []).length;
    const groupCursorPos = _poiCursor[groupBucketKey];
    const groupCounterText = groupCursorPos != null ? `${groupCursorPos + 1}/${groupBucketLen}` : `0/${groupBucketLen}`;
    const groupNavHtml = `<span class="poi-nav-group">
      <button type="button" class="poi-nav poi-nav-prev" data-bucket="${groupBucketKey}" title="${t('prev_in_group')}">‹</button>
      <span class="poi-counter" data-bucket="${groupBucketKey}">${groupCounterText}</span>
      <button type="button" class="poi-nav poi-nav-next" data-bucket="${groupBucketKey}" title="${t('next_in_group')}">›</button>
    </span>`;
    const summary = document.createElement('summary');
    summary.className = 'poi-group-header';
    summary.innerHTML = `
      <span class="poi-dot poi-group-emoji">${groupMeta.emoji}</span>
      <span class="poi-group-caret">▸</span>
      <input type="checkbox" class="poi-group-toggle" data-group="${gid}">
      <span class="poi-group-name">${poiGroupName(gid)}</span>
      <span class="poi-group-count"></span>
      ${groupNavHtml}
    `;
    // Wire group arrows. stopPropagation so clicking an arrow doesn't
    // toggle the <details> open/close state alongside it.
    summary.querySelectorAll('.poi-nav-group').forEach(g => {
      g.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); });
    });
    summary.querySelectorAll('.poi-nav').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        const dir = btn.classList.contains('poi-nav-next') ? 1 : -1;
        _poiJumpTo(btn.dataset.bucket, dir);
      });
    });
    details.appendChild(summary);

    // Persist open state across reloads whenever the user toggles.
    details.addEventListener('toggle', () => {
      if (details.open) openGroups.add(gid);
      else              openGroups.delete(gid);
      _savePOIGroupState(openGroups);
    });

    // Child rows
    for (const [zid, cat] of visibleEntries) {
      addRow(details, zid, cat,
        checked => setPOICategoryVisible(zid, checked));
    }

    // Tri-state group checkbox — ticks when ALL children are on, goes
    // indeterminate when some are on, unticks when all are off. Clicking
    // it toggles every child to match.
    const groupCb = summary.querySelector('.poi-group-toggle');
    const countEl = summary.querySelector('.poi-group-count');
    const childBoxes = () => details.querySelectorAll(
      'label.poi-item input[data-key]');

    // Group header badge = SUM of pin counts across every child row,
    // not the fraction of enabled checkboxes. e.g. a Health group with
    // Hospital(10) + Pharmacy(32) + Dentist(40) shows "(82)".
    // Group total is visible in the nav counter — no need for a separate badge.
    countEl.textContent = '';

    const refreshGroupCb = () => {
      const boxes = childBoxes();
      let on = 0;
      boxes.forEach(b => { if (b.checked) on++; });
      groupCb.checked       = on === boxes.length && boxes.length > 0;
      groupCb.indeterminate = on > 0 && on < boxes.length;
    };
    refreshGroupCb();

    // Child change → refresh group header
    childBoxes().forEach(b =>
      b.addEventListener('change', refreshGroupCb));

    // Group click → toggle all children. Prevent the click from also
    // toggling the <details> open state by stopping propagation on the
    // checkbox specifically (the rest of the summary still opens/closes
    // as normal).
    groupCb.addEventListener('click', e => e.stopPropagation());
    groupCb.addEventListener('change', () => {
      const targetOn = groupCb.checked;
      childBoxes().forEach(b => {
        if (b.checked !== targetOn) {
          b.checked = targetOn;
          b.dispatchEvent(new Event('change'));
        }
      });
      refreshGroupCb();
    });

    list.appendChild(details);
  };

  for (const gid of groupIds) buildGroup(gid, buckets[gid]);

  // ── Global "Toggle all" row at the bottom ──
  const divider = document.createElement('div');
  divider.className = 'poi-divider';
  list.appendChild(divider);

  const allRow = document.createElement('label');
  allRow.className = 'poi-item poi-select-all';
  allRow.innerHTML = `
    <input type="checkbox" id="poi-select-all">
    <span class="poi-dot" style="background:#475569">⇅</span>
    <span id="poi-select-all-label">Show all</span>
  `;
  const allBox = allRow.querySelector('input');
  const allLbl = allRow.querySelector('#poi-select-all-label');

  const _refreshSelectAll = () => {
    const boxes = list.querySelectorAll('input[data-key]');
    let any = false, allOn = true;
    boxes.forEach(b => { if (b.checked) any = true; else allOn = false; });
    allBox.checked       = allOn;
    allBox.indeterminate = any && !allOn;
    allLbl.textContent   = allOn ? t('hide_all') : t('show_all');
  };
  list.querySelectorAll('input[data-key]').forEach(b =>
    b.addEventListener('change', _refreshSelectAll));

  allBox.addEventListener('change', () => {
    const boxes = list.querySelectorAll('input[data-key]');
    let allOnBefore = true;
    boxes.forEach(b => { if (!b.checked) allOnBefore = false; });
    const targetOn = !allOnBefore;
    boxes.forEach(b => {
      if (b.checked !== targetOn) {
        b.checked = targetOn;
        b.dispatchEvent(new Event('change'));
      }
    });
    _refreshSelectAll();
  });
  list.appendChild(allRow);
  _refreshSelectAll();
}

// Cache of the last set of available POI keys so we don't rebuild the
// list DOM every day during animation. _applyState() calls us on every
// step; rebuilding wipes and recreates every checkbox and re-fires each
// addRow's onChange (setPOICategoryVisible). That's safe in steady
// state but races visibly with mid-animation toggles — a category the
// user just turned on can flip back off if its DOM checkbox hadn't yet
// been queried by prevChecked when the day-step rebuild fired. By
// short-circuiting when the available keys haven't changed (which they
// don't between days during a normal run) we avoid the race entirely.
let _lastPOIAvailableKeys = null;

function _setsEqual(a, b) {
  if (a === b) return true;
  if (!a || !b || a.size !== b.size) return false;
  for (const k of a) if (!b.has(k)) return false;
  return true;
}

/**
 * Rebuild the POI list showing only categories available in the current city.
 */
function _updatePOIAvailability() {
  const list = document.getElementById('poiList');
  if (!list) return;
  const keys = getAvailablePOIKeys();
  if (_setsEqual(_lastPOIAvailableKeys, keys)) return;
  _lastPOIAvailableKeys = new Set(keys);
  _buildPOIList(list, keys);
}

// ── Simulation-mode callback ──────────────────────────────────────────────────

/**
 * Called by the settings panel whenever simulation mode (deterministic /
 * stochastic, patient zero, seed) changes. The backend has already reset
 * at this point — we just need to mirror that reset in the frontend.
 */
function _onModeChange(state) {
  running               = false;
  flowHour              = 0.0;
  _dayStartTs           = null;
  _pendingStateFromStep = null;
  _animTarget           = null;
  _stepEpoch           += 1;   // invalidate any step currently in flight

  const btnPlay = document.getElementById('btnPlay');
  if (btnPlay) {
    _syncPlayIcon(false);
  }

  _stopReplay();
  _history    = [];
  _historyIdx = -1;
  if (_historySaveTimer !== null) { clearTimeout(_historySaveTimer); _historySaveTimer = null; }
  if (_activeCityId) clearHistory(_activeCityId);
  document.getElementById('timelinePanel').classList.add('day-zero');

  currentState = state;
  resetWeather(); _updateDayNightIcon('clear');
  _applyState(state);
  document.querySelectorAll('.iv-btn').forEach(b => b.classList.remove('active'));

  // NOTE: do NOT touch _pickPzState or the pick hint here. Pick state is
  // managed exclusively by the manual-patient-zero flow; clobbering it
  // from a sim-mode reset would race the user when they tick "Pick
  // patient zero" while Random is still on (the apply() round-trip
  // would land after the hint had been shown and erase it).
}

/**
 * The settings panel's "Pick patient zero" checkbox was toggled by the user.
 *   checked → enter waiting state, prompt them to click a building
 *   unchecked → clear the manual pin on the backend
 */
async function _onPickPatientZeroToggle(checked) {
  if (checked) {
    _pickPzState = 'waiting';
    _syncPickZeroBtn();
    setManualPickHint(t('pick_residential'));
    // Visually highlight residential targets by greying out every
    // non-residential cell in the 3D view. Flag is cleared on
    // commit, cancel, or play.
    setPickPatientZeroMode(true);
  } else {
    _pickPzState = 'off';
    _syncPickZeroBtn();
    setManualPickHint('');
    setPickPatientZeroMode(false);
    try {
      const state = await fetch('/api/manual_patient_zero', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ idx: null }),
      }).then(r => r.json());
      currentState = state;
      _applyState(state);
    } catch { /* network error — UI already reflects the intent */ }
  }
}

// ── Viz callbacks ─────────────────────────────────────────────────────────────

function _onVizChange() {
  setColorMode(getColorMode());
  setHeightMode(getHeightMode());
  setColorScale(getColorScale());
  setLogScale(getLogScale());
  setInvertScale(getInvertScale());
  setNormMode(getNormMode());
  _paintLegendPill();
  if (currentState) {
    updateColors(currentState.cells);
    renderMinimap(currentState.cells, getColorMode(), getColorScale(), _selectedCell);
  }
  // Compare mode bakes its color buffers from the same color pipeline, so a
  // legend change has to trigger a re-bake — otherwise the user sees stale
  // colors until they nudge a thumb.
  cmpRefreshIfActive();
}

/**
 * Sync the top-bar infection-intensity gradient pill with whatever scale
 * + invert flag the Visualization panel currently has selected.
 */
function _paintLegendPill() {
  const grad = document.getElementById('legendPillGrad');
  if (!grad) return;
  const scale  = getColorScale();
  const invert = getInvertScale();
  const stops = [];
  for (let i = 0; i <= 7; i++) {
    let frac = i / 7;
    if (invert) frac = 1 - frac;
    stops.push(`${sampleScaleCSS(frac, scale)} ${(i / 7 * 100).toFixed(1)}%`);
  }
  grad.style.background = `linear-gradient(to right, ${stops.join(', ')})`;
}

// ── Selection callback ────────────────────────────────────────────────────────

function _onSelect(cell) {
  // Ignore building clicks while a patient-zero commit is in flight so
  // spray-clicks don't queue up stale POSTs.
  if (_pickPzState === 'committing') return;
  // Pick-patient-zero: any building works (residential or not). Backend
  // gives non-residential buildings their own capacity for SEIR.
  if (cell && _pickPzState === 'waiting') {
    _commitManualPatientZero(cell.idx);
    return;
  }

  _selectedCell = cell;
  if (currentState) {
    renderMinimap(currentState.cells, getColorMode(), getColorScale(), _selectedCell);
    // In compare mode the color buffer is owned by compare.js; recolouring
    // here would replace the diff/split palette with the live SEIR view.
    if (!isCompareActive()) updateColors(currentState.cells);
  }
  // Restrict the flow field to the selected building's adjacent segments.
  // Passing null clears the filter and returns to city-wide traffic.
  if (_flowField) _flowField.setSelectedBuilding(cell ? cell.idx : null);
  // Same filter for the infection-flow canvas: when a building is
  // selected, only render the edges connected to it (in + out).
  setInfectionFlowSelected(cell ? cell.idx : null);
}

/** POST a chosen patient-zero building index and refresh state. */
async function _commitManualPatientZero(idx) {
  // Push the new index into view.js immediately so the 🦠 pin appears
  // without waiting for the round-trip — feels instant to the user.
  setPatientZeroIndices([idx]);
  _ensurePatientZeroPinVisible();
  // Fly the camera to the picked building so the pin is dead-centre,
  // matching the "city starts zoomed on patient zero" expectation.
  const b = cityLayout?.buildings?.[idx];
  if (b?.lon != null && b?.lat != null) flyToBuilding([b.lon, b.lat], 16.4);

  // Lock the picker UI while the POST is in flight so the user can't
  // spray-click more buildings and queue up stale commits. The flag
  // is read by _onSelect to short-circuit further picks.
  _pickPzState = 'committing';
  _syncPickZeroBtn();
  // Optimistically commit the top-bar chip to the "set" look — the
  // round-trip is only a confirmation.
  setPickPatientZeroMode(false);

  try {
    const state = await fetch('/api/manual_patient_zero', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ idx }),
    }).then(r => r.json());
    // Picking a new patient zero is a true sim reset on the backend
    // (eng.reset() re-seeds at the new building), so the frontend must
    // mirror it: stop the play animation, invalidate any in-flight day
    // step, drop the replay history + the timeline scrubber, wipe
    // intervention highlights, and clear any leftover building
    // selection. Without this, hitting Play right after a re-pick
    // would finish the *previous* outbreak's day animation before
    // starting the new one, and the timeline panel would keep painting
    // the old day curve. _onModeChange() is the canonical handler that
    // does all of this — re-use it so the re-pick path stays in lock-
    // step with the sim-mode reset path.
    clearSelection();
    _onModeChange(state);
    // Two-state model: commit returns to idle. The pin on the map is
    // the user's confirmation; the chip stays neutral.
    _pickPzState = 'off';
    _syncPickZeroBtn();
    setManualPickHint('');
  } catch {
    // Network error: drop pick mode entirely so the UI doesn't get stuck.
    _pickPzState = 'off';
    _syncPickZeroBtn();
    setManualPickChecked(false);
    setManualPickHint('');
  }
}

/**
 * Briefly clear and re-set the pick hint so the user gets a visible "flash"
 * confirming the new building ID — without it the same-prefixed text
 * looks identical to the previous pick.
 */
function _flashPickHint(text) {
  setManualPickHint('');
  // requestAnimationFrame ensures the empty state actually paints once
  // before we restore the new text.
  requestAnimationFrame(() => requestAnimationFrame(() => setManualPickHint(text)));
}

/**
 * Briefly draw attention to the Pick Patient Zero button when the user
 * hits Play without selecting a seed. Pulses the button + flashes a
 * hint inside the settings popover.
 */
function _flashPickZeroPrompt() {
  const btn = document.getElementById('btnPickZero');
  if (!btn) return;
  btn.classList.add('waiting');
  // Strong attention shake — short, then fall back to the normal idle
  // visual unless the user has actually armed pick mode.
  setTimeout(() => {
    if (_pickPzState !== 'waiting') btn.classList.remove('waiting');
  }, 2500);
  _flashPickHint(t('pick_residential'));
}

/**
 * Reflect _pickPzState in the top-bar #btnPickZero chip.
 * States: 'off' (idle), 'waiting' (armed, click a building), 'set' (pinned).
 */
function _syncPickZeroBtn() {
  const btn = document.getElementById('btnPickZero');
  if (!btn) return;
  // Two visual states only: idle and waiting (pulsing accent). After
  // a building is committed the chip returns to idle — the user sees
  // the seed via the 🦠 pin on the map, not via a chip "set" colour.
  btn.classList.remove('waiting', 'set');
  if (_pickPzState === 'waiting' || _pickPzState === 'committing') {
    btn.classList.add('waiting');
  }
}

/**
 * Find the nearest residential building (zones 1–3) to the supplied
 * cell. Used to snap pick-zero clicks so any building the user picks
 * resolves to a valid SEIR seed instead of being silently rejected.
 */
function _nearestResidentialIdx(cell) {
  if (!cell || !cityLayout?.buildings) return null;
  const cx = cell.lon, cy = cell.lat;
  if (cx == null || cy == null) return null;
  let bestIdx = -1, bestD = Infinity;
  const buildings = cityLayout.buildings;
  for (let i = 0; i < buildings.length; i++) {
    const b = buildings[i];
    if (!b || b.zone < 1 || b.zone > 3) continue;
    const dx = b.lon - cx, dy = b.lat - cy;
    const d  = dx * dx + dy * dy;
    if (d < bestD) { bestD = d; bestIdx = i; }
  }
  return bestIdx >= 0 ? bestIdx : null;
}

/**
 * Drop the entire POI layer and untick every category checkbox in the
 * menu. Used by Reset so a fresh run starts with no pins on the map and
 * the "first Play shows only Patient Zero" default re-arms.
 */
function _resetPOIState() {
  // Drop the availability cache so the next _updatePOIAvailability call
  // does a real rebuild (city switch / reset / engine swap).
  _lastPOIAvailableKeys = null;
  const list = document.getElementById('poiList');
  if (list) {
    list.querySelectorAll('input[data-key]').forEach(cb => {
      cb.checked = false;
      const key = cb.dataset.key;
      if (key === PATIENT_ZERO_KEY) {
        setPOICategoryVisible(PATIENT_ZERO_KEY, false);
      } else {
        setPOICategoryVisible(key, false);
      }
    });
    // Refresh the master "Show all / Hide all" label so it now reads
    // "Show all" (since nothing is checked).
    const allBox = document.getElementById('poi-select-all');
    const allLbl = document.getElementById('poi-select-all-label');
    if (allBox) { allBox.checked = false; allBox.indeterminate = false; }
    if (allLbl) { allLbl.textContent = t('show_all'); }
  }
  // Turn the layer off and drop the POI button highlight.
  setPOIVisible(false);
  const btnPOI = document.getElementById('btnPOI');
  if (btnPOI) btnPOI.classList.remove('active');
  // Re-arm the "first Play shows only Patient Zero" default.
  _firstRunPoiApplied = false;
}

/**
 * Force the POI layer on with at least the Patient Zero category visible,
 * mirroring the menu state so the user sees the freshly-pinned building.
 */
function _ensurePatientZeroPinVisible() {
  setPOICategoryVisible(PATIENT_ZERO_KEY, true);
  setPOIVisible(true);
  // Mirror in the DOM (POI menu checkbox + POI button "active" highlight)
  const pzBox = document.querySelector(`#poiList input[data-key="${PATIENT_ZERO_KEY}"]`);
  if (pzBox) pzBox.checked = true;
  const btnPOI = document.getElementById('btnPOI');
  if (btnPOI) btnPOI.classList.add('active');
}

// ── Day-step pipeline ─────────────────────────────────────────────────────────

/** Target real-time seconds per simulated day for a given pip value
 *  (1 / 2 / 3). Decoupled from the module-level `simSpeed` so the
 *  speed-change handler can compute both the old and new durations
 *  during the brief re-anchor window without mutating state.
 */
function _secPerDayFor(speed) {
  const t = (speed - 1) / 2;      // 0..1 as slider moves 1..3
  return SEC_PER_DAY_MAX - t * (SEC_PER_DAY_MAX - SEC_PER_DAY_MIN);
}

/** Target real-time seconds per simulated day, from the speed slider. */
function _secPerDay() {
  return _secPerDayFor(simSpeed);
}

/** Fire the step for the upcoming day. No-op if one is already in flight. */
function _fireNextStep() {
  if (_stepInFlight || !running) return;
  _stepInFlight = true;
  const myEpoch = _stepEpoch;
  // flow_hour is the intra-day phase the backend uses for its commute
  // multiplier. Send a midday value so commute curves are representative
  // regardless of animation speed.
  fetch(`/api/step?n=1&flow_hour=12.00`, { method: 'POST' })
    .then(r => r.json())
    .then(state => {
      // Discard if a Reset happened while this step was in flight.
      if (myEpoch === _stepEpoch) _pendingStateFromStep = state;
    })
    .catch(() => {})
    .finally(() => { _stepInFlight = false; });
}

// ── Apply a full state update ─────────────────────────────────────────────────

function _applyState(state) {
  updateColors(state.cells);
  renderMinimap(state.cells, getColorMode(), getColorScale(), _selectedCell);
  _updateStats(state);
  renderChart(state.history);
  // Feed the infection-flow overlay so its arcs follow the live state.
  updateInfectionFlow(state, cityLayout);

  // Push active interventions into systems that visualise them on the
  // scene. The flow field hides particles under lockdown / thins them
  // under social distancing; view.js stamps a closed marker on relevant
  // POI pins. Both are no-ops when the set hasn't changed.
  const activeIv = new Set(state.active_interventions || []);
  if (_flowField) _flowField.setInterventions(activeIv);
  setActiveInterventions(activeIv);
  transport.setInterventions(activeIv);

  // Drive the per-vehicle red infection ring. Pass the full 24-element
  // by-pool array so transport.js can colour each vehicle individually
  // based on the pool it's been hashed into (vehicles on the same
  // bus line share a pool). Also pass the city-wide level as a
  // fallback for snapshots without per-pool data (compartmental
  // engine, older saves). Reset / pick-zero zero both out via the
  // normal state apply path because eng.reset() clears the
  // infection log.
  transport.setTransitInfectionsByPool(state.transit_infections_by_pool || null);
  transport.setTransitInfectionLevel(
    Math.min(1, (state.transit_infections_total || 0) / 30)
  );

  // setPatientZeroIndices is a no-op when the list is unchanged, so this
  // is safe to call on every step (cheap diff inside view.js).
  if (state.patient_zero) {
    setPatientZeroIndices(state.patient_zero);
    _updatePOIAvailability();
  }

  // If the agents-as-points layer is active, pull the latest per-agent
  // snapshot. refreshAgents() is internally throttled to one request per
  // ~0.8 s so scrub + play don't stampede the endpoint. Push the day's
  // weekday + weather + season into the layer so its schedule can
  // branch on weekend / climate (different walk targets and dampened
  // movement under rain/snow/storm).
  if (isAgentsLayerActive()) {
    setAgentsScenario({
      is_weekend: !!state.is_weekend,
      weather:    state.weather || null,
      season:     state.climate?.season,
    });
    refreshAgents();
  }

  if (_historyIdx === -1) {
    _history.push(JSON.parse(JSON.stringify(state)));
    if (_history.length > HISTORY_MAX) _history.shift();
    _updateScrubber();
    _scheduleHistorySave();
  }

  // If the user is hovering over a building, refresh the tooltip with the
  // new SEIR values for that same building. Without this the label freezes
  // at whatever the values were when the cursor first landed and visibly
  // diverges from the height/colour the building is rendering with.
  if (_hoveredBuilding) _onHover(_hoveredBuilding);
}

// ── Animation loop ────────────────────────────────────────────────────────────

function _startLoop() {
  let _lastFrameTs = null;

  function frame(ts) {
    requestAnimationFrame(frame);

    const dt = _lastFrameTs !== null ? Math.min((ts - _lastFrameTs) / 1000, 0.1) : 0.016;
    _lastFrameTs = ts;

    if (running) {
      // Day-step pipeline. Mirrors the timeline replay: we only begin a
      // day's animation window once BOTH endpoints (currentState and the
      // _animTarget we're animating toward) are in hand, then smoothly
      // lerp building cells over the full durMs from t=0 → t=1. While
      // that animation is playing we *prefetch* the day after it into
      // _pendingStateFromStep, so the moment the current animation
      // finishes the next one can begin immediately with no idle gap.
      //
      // No animation in progress: try to promote pending → animTarget
      // and kick off the next prefetch.
      if (_animTarget === null) {
        if (_pendingStateFromStep !== null) {
          _animTarget          = _pendingStateFromStep;
          _pendingStateFromStep = null;
          _dayStartTs          = ts;
          flowHour             = 0;
          _fireNextStep();        // prefetch the day AFTER _animTarget
        } else {
          // No animation, no pending — make sure a step is in flight so
          // we have something to animate toward as soon as possible.
          _fireNextStep();
        }
      }

      if (_animTarget !== null && currentState?.cells) {
        const durMs    = _secPerDay() * 1000;
        const elapsed  = ts - _dayStartTs;
        const progress = Math.min(1.0, elapsed / durMs);
        flowHour = progress * 24;

        if (_interpBuf === null) _interpBuf = [];
        const eased = progress * progress * (3 - 2 * progress);
        _lerpCellsInto(
          _interpBuf,
          currentState.cells,
          _animTarget.cells,
          eased,
        );
        updateColors(_interpBuf);

        // Day boundary: commit the target as the new currentState and
        // refresh the HUD. The next frame will immediately promote any
        // already-arrived pending into a fresh animation, so back-to-back
        // days flow without a visible pause.
        if (progress >= 1.0) {
          const next = _animTarget;
          _animTarget = null;
          currentState = next;
          // Roll the new day's weather BEFORE _applyState so the climate
          // strip and stats panel both read the freshly-rolled weather on
          // the same frame. Otherwise the icon lags the actual snow/rain
          // on the map by one full simulated day.
          const wx = notifyNewDay(next.day, next.climate, cityLayout?.center_lat);
          // Stamp the rolled weather onto the state so the snapshot pushed
          // into _history (inside _applyState) preserves it. Scrubbing
          // back through the timeline can then surface each day's actual
          // weather instead of always showing the live one.
          next.weather = wx;
          _applyState(next);
          _updateDayNightIcon(wx);
          _dayStartTs = null;
          flowHour    = 0;
        }
      }
    }

    // Update scene non-render state (tween, indicator pulse)
    updateScene();

    // Drive sun / shadows / overlay from the on-screen clock. Called every
    // frame so the tween fades in AND out smoothly after a toggle.
    updateDayNight(flowHour, dt);

    // Drive the agents-as-points layer from the same hour-of-day clock
    // the sun + HUD read. Because flowHour stops advancing whenever the
    // sim is paused, this gives the user-requested behavior: agents
    // walk only while the sim is running, the dot cloud speed matches
    // the 1x / 2x / 3x speed pips, and a Stop button truly freezes the
    // cloud (including the breathing jitter, which is also indexed in
    // simulated-hour time).
    tickAgents(flowHour);

    // Update flow HUD. The green population-flow particles are now
    // permanently hidden — the "Infection flow" toggle drives the new
    // canvas overlay (infection-flow.js) instead.
    if (_flowField) {
      _flowField.update(currentState?.cells ?? null, dt, running ? simSpeed : 0, flowHour);
      _flowField.setVisible(false);
      _updateFlowHUD();
    }

    // Transport vehicles — driven by the local frontend clock (smooth) so
    // takeoffs/landings tween cleanly between backend ticks. Bus
    // headlights only show when the Day/Night cycle is on AND the
    // current sim hour is night-side (~19:00–06:30).
    if (_transportLoaded) {
      const isNight = _dayNightOn && (flowHour < 6.5 || flowHour >= 19);
      transport.setLOD(getVisibleWidth());
      transport.setCells(_interpBuf || currentState?.cells || null);
      transport.setCameraQuat(getCameraQuat());
      transport.update(flowHour, dt, running ? simSpeed : 0, isNight);
      _updateTransportTipPosition();
    }
    // 3D ambulances — walk the road network at bus-speed × simSpeed.
    ambulance3d.update(dt, running ? simSpeed : 0);
  }

  requestAnimationFrame(frame);
}

// ── Controls ──────────────────────────────────────────────────────────────────

function _bindControls(map, layout) {
  const btnPlay    = document.getElementById('btnPlay');
  const btnReset   = document.getElementById('btnReset');
  const btnTopView = document.getElementById('btnTopView');

  btnPlay.addEventListener('click', async () => {
    // Resuming the live sim must cancel any in-flight history replay so the
    // two timers don't tug-of-war over the scrubber and scene state.
    _stopReplay();

    // Block Play until there's actually a seed to spread from. The Pick
    // Patient Zero button pulses to direct the user — they either pick
    // a building or opt into Random in Model Parameters.
    if (!running) {
      const sm        = currentState?.simulation_mode || {};
      const hasManual = sm.manual_patient_zero_idx != null;
      const hasRandom = !!sm.random_patient_zero;
      const hasSeed   = (currentState?.I + currentState?.E) > 0;
      if (!hasManual && !hasRandom && !hasSeed) {
        _flashPickZeroPrompt();
        return;
      }
    }
    // Patient-zero pick fallback: starting the sim while the user has the
    // checkbox checked but has NOT clicked any building → silently revert
    // to random placement and untick the box so the run still starts.
    if (!running && _pickPzState === 'waiting') {
      _pickPzState = 'off';
      setManualPickChecked(false);
      setManualPickHint('');
      setPickPatientZeroMode(false);
      try {
        const state = await fetch('/api/manual_patient_zero', {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ idx: null }),
        }).then(r => r.json());
        currentState = state;
        _applyState(state);
      } catch { /* ignore — UI already untoggled */ }
    }

    if (!running && _historyIdx >= 0) {
      // User scrubbed back, then hit Play. Jump to the LAST simulated
      // day (not the scrubbed one) and resume from there — pressing
      // Play should never silently truncate the history the user just
      // walked through.
      const lastIdx = _history.length - 1;
      _historyIdx   = -1;
      const lastSnap = _history[lastIdx];
      if (lastSnap) {
        currentState = {
          ...currentState,
          day:    lastSnap.day,
          cells:  lastSnap.cells,
          S: lastSnap.S, E: lastSnap.E, I: lastSnap.I,
          R: lastSnap.R, D: lastSnap.D,
          history:              lastSnap.history ?? currentState.history,
          patient_zero:         currentState.patient_zero,
          active_interventions: lastSnap.active_interventions ?? currentState.active_interventions,
        };
        _applyState(currentState);
      }
      _updateScrubber();
    }

    running = !running;
    _syncPlayIcon(running);

    // Starting (or resuming) the sim should always dismiss the model
    // parameters panel — the user is committing to these settings.
    if (running) closeSettingsPanel();

    // First time the user ever starts the simulation: turn the POI layer
    // on showing only the Patient Zero pin so the seed location is obvious,
    // and fly the camera onto the seed building so the outbreak is centred.
    if (running && !_firstRunPoiApplied) {
      _firstRunPoiApplied = true;
      _showOnlyPatientZeroPOI();
    }

    if (!running) {
      // Pausing: freeze the clock and commit any day-step that the backend
      // already finished so the HUD reflects the true sim state. We
      // collapse the in-flight animation by snapping to the latest known
      // state — _animTarget first (the day we were animating into), then
      // _pendingStateFromStep (the prefetched day after that).
      let snapTo = null;
      if (_animTarget !== null) {
        snapTo = _animTarget;
        _animTarget = null;
      }
      if (_pendingStateFromStep !== null) {
        snapTo = _pendingStateFromStep;
        _pendingStateFromStep = null;
      }
      if (snapTo !== null) {
        currentState = snapTo;
        _applyState(currentState);
      }
      _dayStartTs = null;
    } else {
      // Resuming: start a fresh day animation window on the next frame.
      _dayStartTs = null;
    }
  });

  btnReset.addEventListener('click', async () => {
    running               = false;
    flowHour              = 0.0;
    _dayStartTs           = null;
    _pendingStateFromStep = null;
    _animTarget           = null;
    _stepEpoch           += 1;   // invalidate any step currently in flight
    _syncPlayIcon(false);

    _stopReplay();
    _history    = [];
    _historyIdx = -1;
    if (_historySaveTimer !== null) { clearTimeout(_historySaveTimer); _historySaveTimer = null; }
    if (_activeCityId) clearHistory(_activeCityId);
    document.getElementById('timelinePanel').classList.add('day-zero');

    // Drop any building selection so the highlight indicator, POI filter,
    // and flow-field constraint don't survive the reset.
    clearSelection();
    // Reset is a fresh start: drop the POI layer + uncheck every category
    // and re-arm the "first Play shows only Patient Zero" behavior.
    _resetPOIState();

    const state = await fetch('/api/reset', { method: 'POST' }).then(r => r.json());
    currentState = state;
    resetWeather(); _updateDayNightIcon('clear');
    _applyState(state);
    // Reset reseeds patient zero on the backend — surface the new pin.
    _showOnlyPatientZeroPOI();
    _firstRunPoiApplied = true;
    document.querySelectorAll('.iv-btn').forEach(b => b.classList.remove('active'));
    buildSettingsPanel(state.params, state.simulation_mode,
                       _onModeChange, _onPickPatientZeroToggle);
    _pickPzState = state.simulation_mode?.manual_patient_zero_idx != null
      ? 'set' : 'off';
  });

  // The button is a "3D heights" toggle: ON (default) = perspective with
  // building heights; OFF = flat top-down view. Both transitions only
  // adjust pitch — they DO NOT recenter or rezoom the camera, so the
  // user keeps the area they were inspecting in view.
  let _heights3D = true;
  btnTopView.classList.add('active');
  btnTopView.addEventListener('click', () => {
    _heights3D = !_heights3D;
    btnTopView.classList.toggle('active', _heights3D);
    if (_heights3D) {
      flyToPerspective();
    } else {
      flyToTopView();
      animateTopView();
    }
  });

  _bindPOIControls();
  _bindPopInfoTooltip();

  const btnDayNight = document.getElementById('btnDayNight');
  btnDayNight.addEventListener('click', () => {
    _dayNightOn = !_dayNightOn;
    btnDayNight.classList.toggle('active', _dayNightOn);
    setDayNight(_dayNightOn);
    // When turning the effect off, reset the icon to the sun glyph. The
    // rolled weather for the day is kept so re-enabling shows the same
    // precipitation again — feels consistent with "same day still ongoing".
    if (!_dayNightOn) _updateDayNightIcon('clear');
  });

  _bindSpeedPips();
  _bindAbout();
  bindStatsPanel();
}

// ── Speed pips (Sims-style 1×–5× button group) ───────────────────────────────
function _bindSpeedPips() {
  const pips = document.querySelectorAll('#playbar .speed-pip, #simBar .speed-pip');
  pips.forEach(pip => {
    pip.addEventListener('click', () => {
      const newSpeed = parseInt(pip.dataset.speed, 10);
      if (newSpeed === simSpeed) return;
      // Re-anchor the in-flight day animation so the clock + colour
      // lerp stay continuous across the speed change. Without this,
      // `durMs` is recomputed every frame from the new `simSpeed`
      // while `_dayStartTs` still points at the old anchor, so:
      //   • slow → fast (1× → 3×): elapsed/newDurMs > 1 → progress
      //     clamps to 1.0 instantly, the day ends, flowHour resets
      //     from e.g. 14:00 to 00:00 — looks like time jumped back.
      //   • fast → slow (3× → 1×): elapsed/newDurMs ≪ old progress,
      //     flowHour drops directly (e.g. 12:00 → 02:00).
      // Both read as a rollback. Solving it is just: pick a new
      // `_dayStartTs` such that with newDurMs we land on the same
      // progress we already had under oldDurMs.
      if (running && _dayStartTs !== null) {
        const oldSecPerDay = _secPerDayFor(simSpeed);
        const now          = performance.now();
        const elapsedMs    = now - _dayStartTs;
        const oldDurMs     = oldSecPerDay * 1000;
        const newDurMs     = _secPerDayFor(newSpeed) * 1000;
        const progress     = Math.max(0, Math.min(1, elapsedMs / oldDurMs));
        _dayStartTs        = now - progress * newDurMs;
      }
      simSpeed = newSpeed;
      // The CSS treats BOTH `.on` and `.active` as the selected appearance
      // (.play-pill .speed button.on, .play-pill .speed button.active),
      // and the initial HTML ships 2× with both classes. Toggle them in
      // lockstep so picking 1× or 3× actually clears the highlight on 2×
      // instead of leaving its `.on` behind.
      pips.forEach(p => {
        const isActive = parseInt(p.dataset.speed, 10) === simSpeed;
        p.classList.toggle('active', isActive);
        p.classList.toggle('on',     isActive);
      });
    });
  });
}

// ── Header hover panels (POI / Transport / Settings) ────────────────────────
//
// The three header dropdowns are pure CSS `:hover > .popup` by default.
// Register each one with the popup manager so that opening a modal
// (or any other popup) can force-hide them via `.popup-suppressed`.
// On mouseenter of each wrap, announce the popup as active — that
// forcibly closes any other popup still showing.
function _bindHeaderHoverPanels() {
  const panels = [
    { wrapId: 'poiWrap',       panelId: 'poiPanel' },
    { wrapId: 'transportWrap', panelId: 'transportPanel' },
    { wrapId: 'settingsWrap',  panelId: 'settingsPanel' },
  ];
  for (const { wrapId, panelId } of panels) {
    const wrap  = document.getElementById(wrapId);
    const panel = document.getElementById(panelId);
    if (!wrap || !panel) continue;

    // hide() forcibly closes the panel even while hovered, via the
    // !important CSS class in index.html. Cleared on the next
    // mouseleave so normal hover behaviour resumes afterwards.
    registerPopup(panelId, {
      hide: () => panel.classList.add('popup-suppressed'),
    });

    // On hover: announce as active (closes everything else), and
    // clear any lingering suppression on ourselves.
    wrap.addEventListener('mouseenter', () => {
      panel.classList.remove('popup-suppressed');
      openPopup(panelId);
    });
    // On leave: clear suppression so the next hover shows the panel
    // normally. Native :hover handles the actual display toggle.
    wrap.addEventListener('mouseleave', () => {
      panel.classList.remove('popup-suppressed');
    });
  }
}

// ── About modal (open/close + tab switching) ─────────────────────────────────
function _bindAbout() {
  const btn      = document.getElementById('btnAbout');
  const backdrop = document.getElementById('aboutBackdrop');
  const close    = document.getElementById('aboutClose');
  if (!btn || !backdrop || !close) return;

  const open = () => {
    openPopup('aboutBackdrop');           // closes every other popup
    backdrop.classList.add('open');
    backdrop.classList.remove('popup-suppressed');
    backdrop.setAttribute('aria-hidden', 'false');
    // Focus the close button for keyboard users.
    setTimeout(() => close.focus(), 0);
  };
  const hide = () => {
    backdrop.classList.remove('open');
    backdrop.setAttribute('aria-hidden', 'true');
    // Also clear any lingering .popup-suppressed class on hover
    // panels so the user can mouseover them again without having
    // to leave/re-enter the wrap.
    document.querySelectorAll('.popup-suppressed').forEach(el => {
      el.classList.remove('popup-suppressed');
    });
  };

  // Register with the popup manager so other popups can force-hide
  // this modal when they open.
  registerPopup('aboutBackdrop', { hide });

  btn.addEventListener('click', open);
  close.addEventListener('click', hide);
  backdrop.addEventListener('click', (e) => {
    // Only close when clicking the backdrop itself, not the card.
    if (e.target === backdrop) hide();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && backdrop.classList.contains('open')) hide();
  });

  // Tab switching: flip .active on both the buttons and the panes.
  const tabBtns  = backdrop.querySelectorAll('.tab-btn');
  const tabPanes = backdrop.querySelectorAll('.tab-pane');
  const selectTab = (name) => {
    tabBtns.forEach(b => {
      const on = b.dataset.tab === name;
      b.classList.toggle('active', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    tabPanes.forEach(p => p.classList.toggle('active', p.dataset.tab === name));
  };
  tabBtns.forEach(tabBtn => {
    tabBtn.addEventListener('click', () => {
      selectTab(tabBtn.dataset.tab);
      if (tabBtn.dataset.tab === 'references') _ensureRefsRendered();
    });
  });

  // Listen for an external request to open a specific tab (settings.js
  // dispatches this when the user clicks "📖 sources" on a preset).
  window.addEventListener('epicity:open-about', (e) => {
    open();
    if (e?.detail?.tab) {
      // Render references on demand the first time the tab is opened.
      _ensureRefsRendered().finally(() => selectTab(e.detail.tab));
    }
  });
}

// Cached references payload + flag so we only fetch once per page load.
let _refsPayload = null;
let _refsRendered = false;

async function _ensureRefsRendered() {
  if (_refsRendered) return;
  if (!_refsPayload) {
    try { _refsPayload = await fetch('/api/about').then(r => r.json()); }
    catch { return; }
  }
  const lang = window._epiCityLang || 'en';
  const host = document.querySelector('#aboutBackdrop section.tab-pane[data-tab="references"]');
  if (!host || !_refsPayload) return;

  const refs    = _refsPayload.references || [];
  const presets = _refsPayload.presets    || [];

  const ctxOf = r => (lang === 'sv' ? r.context_sv : r.context_en) || '';
  const refLink = r => r.doi ? (r.doi.startsWith('http')
      ? `<a href="${r.doi}" target="_blank" rel="noopener" style="color:#93c5fd;text-decoration:none">${r.doi}</a>`
      : `<a href="https://doi.org/${r.doi}" target="_blank" rel="noopener" style="color:#93c5fd;text-decoration:none">doi:${r.doi}</a>`) : '';

  const presetCards = presets.map(p => `
    <div style="background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:9px 12px;margin-bottom:8px">
      <div style="font-weight:600;color:#cbd5e1">${p.icon} ${(lang === 'sv' && p.label_sv) || p.label_en}${p.default ? ' <span style="font-size:0.6rem;color:#6ee7b7;border:1px solid #10b98155;border-radius:3px;padding:0 4px;margin-left:4px;vertical-align:middle">'+t('preset_default')+'</span>' : ''}</div>
      <div style="font-size:0.78rem;color:#94a3b8;margin-top:3px">${(lang === 'sv' ? p.blurb_sv : p.blurb_en) || ''}</div>
      <div style="font-size:0.72rem;color:#64748b;margin-top:4px">→ <a href="#ref-${p.ref}" style="color:#93c5fd;text-decoration:none">${p.ref}</a></div>
    </div>`).join('');

  const refCards = refs.map(r => `
    <div id="ref-${r.id}" style="background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:9px 12px;margin-bottom:8px">
      <div style="font-size:0.78rem;color:#cbd5e1">${r.citation} ${refLink(r)}</div>
      <div style="font-size:0.78rem;color:#94a3b8;margin-top:4px">${ctxOf(r)}</div>
    </div>`).join('');

  host.innerHTML = `
    <p style="margin-bottom:12px;color:#94a3b8;font-size:0.85rem">${t('about_refs_intro_en')}</p>
    <h3 style="font-size:0.85rem;text-transform:uppercase;letter-spacing:0.06em;color:#94a3b8;border-bottom:1px solid #1e293b;padding-bottom:4px;margin-bottom:10px">${t('about_presets_section')}</h3>
    ${presetCards}
    <h3 style="font-size:0.85rem;text-transform:uppercase;letter-spacing:0.06em;color:#94a3b8;border-bottom:1px solid #1e293b;padding-bottom:4px;margin:14px 0 10px">${t('about_refs_section')}</h3>
    ${refCards}
  `;
  _refsRendered = true;
}

function _bindCollapsibles() {
  // Each panel toggles independently — multiple can stay open at once.
  document.querySelectorAll('.collapsible-h2').forEach(h2 => {
    h2.addEventListener('click', () => {
      const body  = document.getElementById(h2.dataset.target);
      const arrow = h2.querySelector('.collapse-arrow');
      const nowCollapsed = body.classList.toggle('collapsed');
      arrow.textContent  = nowCollapsed ? '▸' : '▾';
    });
  });
}

// ── Flow header toggle ────────────────────────────────────────────────────────

function _bindFlowPanel() {
  const btnFlow = document.getElementById('btnFlow');
  if (!btnFlow) return;
  btnFlow.addEventListener('click', () => {
    _flowActive = !_flowActive;
    btnFlow.classList.toggle('active', _flowActive);
    // Drive the new infection-flow overlay from the same toggle — the
    // "flow" layer now visualises spread paths (patient zero → active
    // infections) instead of generic population vectors.
    setInfectionFlowActive(_flowActive);
    updateInfectionFlow(currentState, cityLayout);
    // Surface the customisation popover the instant the layer turns on
    // so the user discovers it without having to hover the row first;
    // hide it the moment the layer turns off (the controls only make
    // sense while there's an overlay being drawn).
    const flowPop = document.getElementById('flowPop');
    if (flowPop) flowPop.classList.toggle('open', _flowActive);
  });
}

// Agents-as-points layer (ABM only) — the demo affordance that lets the
// user *see* the per-agent contact graph the engine is running. Toggling
// it fires one fetch immediately; further refreshes happen on each
// day-step via `_applyState` so the dot cloud follows the simulation.
function _bindAgentsLayerToggle() {
  const btnAgents = document.getElementById('btnAgents');
  if (!btnAgents) return;
  btnAgents.addEventListener('click', () => {
    const next = !isAgentsLayerActive();
    btnAgents.classList.toggle('active', next);
    setAgentsLayerActive(next);
  });
}

// ── Nature detail header toggle ──────────────────────────────────────────────
// Controls the water shader, forest ground pads, and the species-aware
// tree scatter all at once. Independent of the Lake/Forest POI pin
// categories — those still work like any other POI and just spawn 🌊/🌲
// pins above the polygon centroids.
function _bindNaturePanel() {
  const btnNature = document.getElementById('btnNature');
  if (!btnNature) return;
  const sync = () => {
    const on = isNatureDetailVisible();
    btnNature.classList.toggle('active', on);
  };
  sync();
  btnNature.addEventListener('click', () => {
    setNatureDetailVisible(!isNatureDetailVisible());
    sync();
    // In side-by-side compare mode the nature layers live on map B's
    // MapLibre instance (three.js is hidden). Tell compare-view-b.js
    // to re-read the toggle and update its own layers' visibility so
    // both halves track the same Nature preference.
    cmpRefreshNature();
  });

  // ── Neighborhood borders button ──
  const btnNbh = document.getElementById('btnNeighborhoods');
  if (btnNbh) {
    btnNbh.addEventListener('click', async () => {
      const on = await toggleNeighborhoods();
      btnNbh.classList.toggle('active', on);
      setAreasOverlay(on);
    });
  }
}

/** Labels for the transport stats block injected into the panel. */
const _stopLabelKey = { bus:'stop_bus', bus_station:'stop_bus_station', tram:'stop_tram', train:'stop_train', ferry:'stop_ferry', airport:'stop_airport' };
const _lineLabelKey = { bus:'line_bus', tram:'line_tram', train:'line_train', ferry:'line_ferry', plane:'line_plane' };
const _STOP_LABELS = new Proxy({}, { get: (_, k) => t(_stopLabelKey[k] || k) });
const _LINE_LABELS = new Proxy({}, { get: (_, k) => t(_lineLabelKey[k] || k) });

/**
 * Update the transport hover panel: inject network stats and show only
 * highlight checkboxes for line types that exist in the current city.
 * Called after transport loads and on city switch.
 */
function _updateTransportHighlightPanel(tdata) {
  const panel = document.getElementById('transportPanel');
  if (!panel) return;

  // ── 1. Stats section ──
  let statsEl = panel.querySelector('.tp-stats');
  if (!statsEl) {
    statsEl = document.createElement('div');
    statsEl.className = 'tp-stats';
    // Insert at the top of the panel, before the highlight title
    panel.prepend(statsEl);
  }

  if (tdata && tdata.schedule) {
    const { stops, lines, trips } = transport.getStats();
    // Push per-mode counts into the new hover popover too. Each mode
    // gets line + stop + trip totals so the user sees the full network
    // shape (lines = routes, trips ≈ vehicles in rotation).
    setTransportStats({
      train: { lines: lines.train || 0, stops: stops.train || 0,                              trips: trips.train || 0 },
      tram:  { lines: lines.tram  || 0, stops: stops.tram  || 0,                              trips: trips.tram  || 0 },
      ferry: { lines: lines.ferry || 0, stops: stops.ferry || 0,                              trips: trips.ferry || 0 },
      bus:   { lines: lines.bus   || 0, stops: (stops.bus || 0) + (stops.bus_station || 0),   trips: trips.bus   || 0 },
      plane: { lines: lines.plane || 0, stops: stops.airport || 0,                            trips: trips.plane || 0 },
    });
    let html = `<div class="tt-title">${t('transport_network')}</div>`;
    const stopOrder = ['airport', 'train', 'ferry', 'bus_station', 'tram', 'bus'];
    for (const k of stopOrder) {
      if (stops[k]) html += `<div class="tt-row">${_STOP_LABELS[k] || k}<span>${stops[k]}</span></div>`;
    }
    const lineOrder = ['plane', 'train', 'ferry', 'tram', 'bus'];
    let anyLine = false;
    for (const k of lineOrder) { if (lines[k]) { anyLine = true; break; } }
    if (anyLine) {
      html += '<div class="tt-sep"></div>';
      for (const k of lineOrder) {
        if (lines[k]) html += `<div class="tt-row">${_LINE_LABELS[k] || k}<span>${lines[k]}</span></div>`;
      }
    }
    statsEl.innerHTML = html;
    statsEl.style.display = '';
  } else {
    statsEl.style.display = 'none';
  }

  // ── 2. Highlight checkboxes — only for available line types ──
  const available = new Set();
  if (tdata && tdata.schedule && tdata.schedule.lines) {
    for (const line of tdata.schedule.lines) {
      if (line.polyline && line.polyline.length >= 2) available.add(line.type);
    }
  }

  panel.querySelectorAll('label').forEach(label => {
    const cb = label.querySelector('input[data-hl]');
    if (!cb) return;
    const type = cb.dataset.hl;
    label.style.display = available.has(type) ? '' : 'none';
    if (!available.has(type) && cb.checked) {
      cb.checked = false;
      highlightTransportLine(type, false);
    }
  });

  // Show/hide the "Highlight Routes" title based on available types
  const hlTitle = panel.querySelector('.poi-title');
  if (hlTitle) hlTitle.style.display = available.size > 0 ? '' : 'none';
}

// ── Transport header toggle ───────────────────────────────────────────────────
// Mirrors _bindFlowPanel. Visuals-only: the epi coupling lives in the
// backend dispatcher and runs regardless of this toggle.

function _bindTransportPanel() {
  const btn  = document.getElementById('btnTransport');
  const wrap = document.getElementById('transportWrap');
  const panel = document.getElementById('transportPanel');
  if (!btn) return;

  // Reflect the current _transportActive on the chrome — runs once at
  // bind time so the button's "active" highlight matches the default
  // (now ON) without waiting for the user to click.
  btn.classList.toggle('active', _transportActive);
  if (wrap) wrap.classList.toggle('transport-active', _transportActive);

  btn.addEventListener('click', () => {
    _transportActive = !_transportActive;
    btn.classList.toggle('active', _transportActive);
    if (wrap) wrap.classList.toggle('transport-active', _transportActive);
    if (_transportLoaded) transport.setVisible(_transportActive);
    // Auto open / close the hover popover so the user sees the per-mode
    // controls + statistics the instant they activate the layer.
    if (_transportActive) openTransportPop();
    else                  closeTransportPop();
    // Clear transport hover/selection when toggling off
    if (!_transportActive) {
      _transportHoveredVi = -1; _transportSelectedVi = -1;
      transport.setHoveredVehicle(-1); transport.setSelectedVehicle(-1);
      _hideTransportTip();
    }
    // Turning transport off → uncheck all highlight routes
    if (!_transportActive && panel) {
      panel.querySelectorAll('input[data-hl]').forEach(cb => {
        if (cb.checked) {
          cb.checked = false;
          highlightTransportLine(cb.dataset.hl, false);
        }
      });
    }
  });

  // Wire highlight checkboxes in the transport panel
  if (panel) {
    // Swallow events to prevent map interaction
    ['click', 'mousedown', 'mouseup', 'pointerdown', 'pointerup'].forEach(ev => {
      panel.addEventListener(ev, e => e.stopPropagation());
    });
    panel.querySelectorAll('input[data-hl]').forEach(cb => {
      cb.addEventListener('change', () => {
        highlightTransportLine(cb.dataset.hl, cb.checked);
      });
    });
  }
}

function _updateFlowHUD() {
  const hour = flowHour;
  const hh   = Math.floor(hour).toString().padStart(2, '0');
  const mm   = Math.floor((hour % 1) * 60).toString().padStart(2, '0');
  document.getElementById('todHour').textContent  = `${hh}:${mm}`;
  document.getElementById('todPhase').textContent = _flowField
    ? t(_flowField.getPhaseName(hour)) : '—';
  document.getElementById('todBar').style.width   = `${(hour / 24) * 100}%`;
}

// ── Timeline / history scrubber ───────────────────────────────────────────────

function _bindTimeline() {
  const scrubber   = document.getElementById('tlScrubber');
  const btnReplay  = document.getElementById('btnReplay');
  const btnCompare = document.getElementById('btnCompare');

  scrubber.addEventListener('input', () => {
    _stopReplay();
    _scrubTo(parseInt(scrubber.value, 10));
    _paintScrubberVisuals();
  });

  if (btnCompare) {
    btnCompare.addEventListener('click', () => {
      if (isCompareActive()) _exitCompare();
      else                   _enterCompare();
    });
  }

  btnReplay.addEventListener('click', () => {
    if (isCompareActive()) return;          // replay is meaningless in compare
    if (_replayRaf !== null) { _stopReplay(); return; }
    _startReplay();
  });

  _bindComparePanel();
  // Per-side SEIR overlays — refresh whenever the compare state shifts.
  onCompareChange(_renderCompareOverlays);
  // Bridge split-mode hover into the existing rich tooltip used by the
  // single view. Looking up the building by idx and calling _onHover
  // re-uses every field the live tooltip already renders (name,
  // neighborhood, income, household, SEIR, prevalence, age).
  setCompareHoverCallback((idx) => {
    if (idx === null || idx === undefined) { _onHover(null); return; }
    const b = cityLayout?.buildings?.[idx];
    if (b) _onHover(b);
  });
}

/** Compute the per-day "active infections growth" %-change shown on the
 *  bottom playbar's Growth pill, but driven by an arbitrary snapshot
 *  rather than the live state. Returns { text, cls } so the per-side
 *  overlay can colour the badge the same way the live pill does. */
function _growthForSnap(snap) {
  const hist = snap?.history;
  if (!Array.isArray(hist) || hist.length < 2) {
    return { text: '—', cls: 'flat' };
  }
  const cur  = hist[hist.length - 1];
  const prev = hist[hist.length - 2];
  const active     = (cur.E  || 0) + (cur.I  || 0);
  const activePrev = (prev.E || 0) + (prev.I || 0);
  if (activePrev <= 1) {
    if (active > activePrev) return { text: '+new', cls: 'up' };
    return { text: '0%', cls: 'flat' };
  }
  const pct = ((active - activePrev) / activePrev) * 100;
  const cls = pct > 1 ? 'up' : pct < -1 ? 'down' : 'flat';
  const sign = pct > 0 ? '+' : '';
  return { text: `${sign}${pct.toFixed(1)}%`, cls };
}

function _reffForSnap(snap) {
  const seeded = snap && snap.day > 0 && (snap.I + snap.E) > 0;
  if (!seeded || typeof snap.r_eff !== 'number') return '—';
  return snap.r_eff.toFixed(2);
}

/** Build the inner HTML of a per-side overlay strip from a snapshot.
 *  Layout: R-eff · S · E · I · R · D · Growth — all rendered as
 *  same-sized pills so the row reads as a single horizontal SEIRD+R+G
 *  block. R-eff and Growth show '—' when the day is pre-outbreak so
 *  the user isn't shown a misleading number. */
function _kpiOverlayHTML(side, snap) {
  if (!snap) return '';
  const fmt = (v) => Math.round(v ?? 0).toLocaleString();
  const reff   = _reffForSnap(snap);
  const growth = _growthForSnap(snap);
  return `
    <div class="kpi-overlay-strip">
      <div class="kpi reff"><span class="lbl">R-eff</span><span class="val">${reff}</span></div>
      <div class="kpi"><span class="lbl"><span class="kpi-dot" style="background:var(--s-s)"></span>S</span><span class="val">${fmt(snap.S)}</span></div>
      <div class="kpi"><span class="lbl"><span class="kpi-dot" style="background:var(--s-e)"></span>E</span><span class="val">${fmt(snap.E)}</span></div>
      <div class="kpi"><span class="lbl"><span class="kpi-dot" style="background:var(--s-i)"></span>I</span><span class="val">${fmt(snap.I)}</span></div>
      <div class="kpi"><span class="lbl"><span class="kpi-dot" style="background:var(--s-r)"></span>R</span><span class="val">${fmt(snap.R)}</span></div>
      <div class="kpi"><span class="lbl"><span class="kpi-dot" style="background:var(--s-d)"></span>D</span><span class="val">${fmt(snap.D)}</span></div>
      <div class="kpi growth ${growth.cls}"><span class="lbl">${t('growth')}</span><span class="val">${growth.text}</span></div>
    </div>
  `;
}

/** Show/hide and (re)populate the per-side SEIR overlays based on
 *  compare state. Visibility is also CSS-gated by `body.compare-split`,
 *  so the inner population only matters when split mode is active. */
function _renderCompareOverlays(state) {
  const overlayA = document.getElementById('kpiOverlayA');
  const overlayB = document.getElementById('kpiOverlayB');
  if (!overlayA || !overlayB) return;
  if (!state.enabled || state.mode !== 'split') {
    overlayA.hidden = true;
    overlayB.hidden = true;
    return;
  }
  const a = state.history?.[state.dayAIdx];
  const b = state.history?.[state.dayBIdx];
  overlayA.innerHTML = _kpiOverlayHTML('a', a);
  overlayB.innerHTML = _kpiOverlayHTML('b', b);
  overlayA.hidden = false;
  overlayB.hidden = false;
}

/** Wire the popover's mode tabs, day sliders, close button, and metric. */
function _bindComparePanel() {
  const sliderA = document.getElementById('cmpDayARange');
  const sliderB = document.getElementById('cmpDayBRange');
  const numA    = document.getElementById('cmpDayANum');
  const numB    = document.getElementById('cmpDayBNum');
  const tabDiff = document.getElementById('cmpTabDiff');
  const tabSplit= document.getElementById('cmpTabSplit');
  const close   = document.getElementById('btnCompareClose');

  const _onSlider = (which) => () => {
    const slider = which === 'a' ? sliderA : sliderB;
    const num    = which === 'a' ? numA    : numB;
    const idx    = parseInt(slider.value, 10);
    // Reflect the day number immediately even if the apply call is debounced.
    if (num && _history[idx]) num.textContent = String(_history[idx].day);
    if (which === 'a') cmpSetDayA(idx);
    else               cmpSetDayB(idx);
  };
  if (sliderA) sliderA.addEventListener('input', _onSlider('a'));
  if (sliderB) sliderB.addEventListener('input', _onSlider('b'));

  const _setMode = (mode) => {
    cmpSetMode(mode);
    if (tabDiff)  tabDiff.classList.toggle('active',  mode === 'diff');
    if (tabSplit) tabSplit.classList.toggle('active', mode === 'split');
    const legend = document.getElementById('cmpLegendRow');
    if (legend) legend.classList.toggle('visible', mode === 'diff');
  };
  if (tabDiff)  tabDiff.addEventListener('click',  () => _setMode('diff'));
  if (tabSplit) tabSplit.addEventListener('click', () => _setMode('split'));

  if (close) close.addEventListener('click', () => _exitCompare());
}

/**
 * Open the compare popover and enter compare mode. Defaults: Day A at
 * ~30 % through history, Day B = latest. As long as the user has at
 * least 2 days of history we let them in — no day-count gate beyond that.
 */
function _enterCompare() {
  if (_history.length < 2) return;
  _stopReplay();
  running = false;
  _syncPlayIcon(false);

  const panel    = document.getElementById('comparePanel');
  const sliderA  = document.getElementById('cmpDayARange');
  const sliderB  = document.getElementById('cmpDayBRange');
  const numA     = document.getElementById('cmpDayANum');
  const numB     = document.getElementById('cmpDayBNum');
  const tabDiff  = document.getElementById('cmpTabDiff');
  const tabSplit = document.getElementById('cmpTabSplit');
  const legend   = document.getElementById('cmpLegendRow');
  const btn      = document.getElementById('btnCompare');

  const aIdx = Math.max(0, Math.floor(_history.length * 0.3));
  const bIdx = _history.length - 1;

  // Default mode: diff. The user can switch to split with one click.
  const mode = (tabSplit && tabSplit.classList.contains('active')) ? 'split' : 'diff';
  if (tabDiff)  tabDiff .classList.toggle('active', mode === 'diff');
  if (tabSplit) tabSplit.classList.toggle('active', mode === 'split');
  if (legend)   legend.classList.toggle('visible', mode === 'diff');

  if (sliderA) {
    sliderA.max   = String(_history.length - 1);
    sliderA.value = String(aIdx);
  }
  if (sliderB) {
    sliderB.max   = String(_history.length - 1);
    sliderB.value = String(bIdx);
  }
  if (numA && _history[aIdx]) numA.textContent = String(_history[aIdx].day);
  if (numB && _history[bIdx]) numB.textContent = String(_history[bIdx].day);

  if (panel) panel.hidden = false;
  if (btn)   btn.classList.add('active');

  enableCompareMode(_history, cityLayout, aIdx, bIdx, mode);
}

/** Close the popover and restore live scene. */
function _exitCompare() {
  const panel = document.getElementById('comparePanel');
  const btn   = document.getElementById('btnCompare');
  if (panel) panel.hidden = true;
  if (btn)   btn.classList.remove('active');

  disableCompareMode(currentState?.cells);
}

/** Apply the snapshot at `idx` and update HUD/scrubber to match. */
function _scrubTo(idx) {
  if (idx < 0 || idx >= _history.length) return;
  _historyIdx = idx;
  const snap  = _history[idx];
  if (!snap) return;

  updateColors(snap.cells);
  renderMinimap(snap.cells, getColorMode(), getColorScale(), _selectedCell);
  _updateStats(snap);
  renderChart(snap.history);
  // The flow chain is build-once-cache (see infection-flow.js); per-scrub
  // cost is now just a `_state` swap + redraw, so we call it inline.
  // The previous 80 ms debounce was paid every day boundary during the
  // run button replay, which left "Current day only" mode showing a
  // one-day-stale chain for the first 80 ms of each new day.
  rebuildInfectionFlowFromHistory(_history, currentState, cityLayout, idx);

  const scrubber = document.getElementById('tlScrubber');
  if (scrubber) scrubber.value = String(idx);
  document.getElementById('tlCurrent').textContent = t('day_n', { n: snap.day });
  _paintScrubberVisuals();
}

/** Sync #scrubberFill width and #scrubberKnob position to the slider value. */
function _paintScrubberVisuals() {
  const scrubber = document.getElementById('tlScrubber');
  const fill     = document.getElementById('scrubberFill');
  const knob     = document.getElementById('scrubberKnob');
  if (!scrubber) return;
  const max = parseInt(scrubber.max, 10) || 0;
  const val = parseInt(scrubber.value, 10) || 0;
  const pct = max > 0 ? (val / max) * 100 : 0;
  if (fill) fill.style.width = pct + '%';
  if (knob) knob.style.left  = pct + '%';
  _paintTimelineCurve();
}

/**
 * Render the active-infections (E + I) curve into the timeline track's
 * background SVG, mirroring the reference design where the epidemic
 * waveform crests above the red scrubber bar.
 */
function _paintTimelineCurve() {
  const svg = document.getElementById('timelineCurve');
  if (!svg) return;
  if (!_history || _history.length < 2) {
    svg.innerHTML = '';
    return;
  }
  const W = 100, H = 18;          // viewBox units; SVG stretches to track size
  let peak = 0;
  for (const h of _history) {
    const a = (h.E || 0) + (h.I || 0);
    if (a > peak) peak = a;
  }
  if (peak <= 0) { svg.innerHTML = ''; return; }
  const N = _history.length;
  const pts = _history.map((h, i) => {
    const x = (i / (N - 1)) * W;
    const a = (h.E || 0) + (h.I || 0);
    // Use a square-root scale so small early infections still register
    // visually without flattening once the wave gets large.
    const y = H - 1 - (Math.sqrt(a / peak)) * (H - 2);
    return [x, y];
  });
  const lineD = 'M ' + pts.map(p => `${p[0].toFixed(2)} ${p[1].toFixed(2)}`).join(' L ');
  const fillD = `${lineD} L ${W} ${H} L 0 ${H} Z`;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.innerHTML = `
    <path class="curve-fill"   d="${fillD}"/>
    <path class="curve-stroke" d="${lineD}"/>`;
}

/**
 * Lerp two day-snapshot `cells` arrays into `out` in-place. Avoids per-frame
 * allocation during replay so we don't churn the GC at 60 fps. Each output
 * cell is a reusable object that updateColors / _applyHeights can read just
 * like a real snapshot cell.
 */
function _lerpCellsInto(out, a, b, t) {
  const n = Math.max(a.length, b.length);
  out.length = n;
  for (let i = 0; i < n; i++) {
    const ca = a[i], cb = b[i];
    if (!ca && !cb) { out[i] = undefined; continue; }
    let dst = out[i];
    if (!dst || typeof dst !== 'object') {
      dst = out[i] = { S: 0, E: 0, I: 0, R: 0, D: 0, N: 0, infections_here: 0 };
    }
    if (!ca) {
      dst.S = cb.S; dst.E = cb.E; dst.I = cb.I;
      dst.R = cb.R; dst.D = cb.D ?? 0; dst.N = cb.N;
      // Sparse — most cells never receive the field. Fall back to 0 so
      // the colour/height pipeline never reads NaN. Same treatment as
      // SEIR's `?? 0` guards above.
      dst.infections_here = cb.infections_here ?? 0;
      continue;
    }
    if (!cb) {
      dst.S = ca.S; dst.E = ca.E; dst.I = ca.I;
      dst.R = ca.R; dst.D = ca.D ?? 0; dst.N = ca.N;
      dst.infections_here = ca.infections_here ?? 0;
      continue;
    }
    dst.S = ca.S + (cb.S - ca.S) * t;
    dst.E = ca.E + (cb.E - ca.E) * t;
    dst.I = ca.I + (cb.I - ca.I) * t;
    dst.R = ca.R + (cb.R - ca.R) * t;
    const da = ca.D ?? 0, db = cb.D ?? 0;
    dst.D = da + (db - da) * t;
    dst.N = ca.N + (cb.N - ca.N) * t;
    // Lerp the infection-sites counter too — without this the
    // 'infections_here' colour/height mode goes flat during the day
    // animation (only the SEIR fields were updating between snapshots)
    // and snaps to the real value on pause. The field is sparse on
    // both endpoints, so default missing → 0 on each side and the lerp
    // works even when only one endpoint has recorded transmissions.
    const iha = ca.infections_here ?? 0, ihb = cb.infections_here ?? 0;
    dst.infections_here = iha + (ihb - iha) * t;
  }
}

/**
 * Begin (or resume) playback through `_history`. Pauses the live sim for the
 * duration so new days can't append mid-replay. If the playhead is already
 * at the end (or at live), restart from day 0.
 *
 * Uses requestAnimationFrame to interpolate cells between day snapshots, so
 * building heights and colors animate smoothly instead of popping per day.
 */
function _startReplay() {
  if (_history.length < 2) return;

  // Pause live sim so it can't append new history while we're replaying.
  if (running) {
    running = false;
    const btnPlay = document.getElementById('btnPlay');
    if (btnPlay) {
      _syncPlayIcon(false);
    }
    // Collapse any in-flight animation: snap to the latest known state
    // (animTarget first, then pending) so the user doesn't lose days
    // when the replay takes over.
    let snapTo = null;
    if (_animTarget !== null) {
      snapTo = _animTarget;
      _animTarget = null;
    }
    if (_pendingStateFromStep !== null) {
      snapTo = _pendingStateFromStep;
      _pendingStateFromStep = null;
    }
    if (snapTo !== null) {
      currentState = snapTo;
      _applyState(currentState);
    }
    _dayStartTs = null;
  }

  // Start from 0 if we're at live or already at the end; otherwise resume.
  let idx = (_historyIdx === -1 || _historyIdx >= _history.length - 1)
    ? 0
    : _historyIdx;

  const btnReplay = document.getElementById('btnReplay');
  btnReplay.textContent = t('pause');
  btnReplay.classList.add('playing');

  _scrubTo(idx);
  if (_interpBuf === null) _interpBuf = [];

  // Pick a per-day duration based on how many days are in the buffer.
  // Long histories play fast so the whole sweep is bounded; short ones
  // keep the slow per-day pace so each frame is readable.
  const msPerDay = _replayMsPerDay(_history.length);

  let lastTs     = null;
  let dayElapsed = 0;

  const tick = (ts) => {
    if (lastTs === null) {
      lastTs = ts;
      _replayRaf = requestAnimationFrame(tick);
      return;
    }
    const dt = ts - lastTs;
    lastTs = ts;
    dayElapsed += dt;

    // Cross one or more day boundaries if we slipped a frame.
    while (dayElapsed >= msPerDay) {
      dayElapsed -= msPerDay;
      idx += 1;
      if (idx >= _history.length - 1) {
        _scrubTo(_history.length - 1);
        _stopReplay();
        return;
      }
      // Full update at day boundary: chart, stats, minimap, day counter.
      _scrubTo(idx);
    }

    // Sub-day interpolation: only refresh building geometry/colors so we
    // don't thrash the chart on every frame. Smoothstep easing gives a
    // gentle ease-in/ease-out per day instead of constant velocity.
    const a = _history[idx];
    const b = _history[idx + 1];
    if (a && b) {
      const t      = dayElapsed / msPerDay;
      const eased  = t * t * (3 - 2 * t);
      _lerpCellsInto(_interpBuf, a.cells, b.cells, eased);
      updateColors(_interpBuf);
      if (_hoveredBuilding) _onHover(_hoveredBuilding);
    }

    _replayRaf = requestAnimationFrame(tick);
  };
  _replayRaf = requestAnimationFrame(tick);
}

/** Halt any in-flight replay and reset the replay button to idle. */
function _stopReplay() {
  if (_replayRaf !== null) {
    cancelAnimationFrame(_replayRaf);
    _replayRaf = null;
  }
  const btnReplay = document.getElementById('btnReplay');
  if (btnReplay) {
    btnReplay.textContent = t('replay');
    btnReplay.classList.remove('playing');
  }
}

function _updateScrubber() {
  const panel    = document.getElementById('timelinePanel');
  const scrubber = document.getElementById('tlScrubber');
  if (!panel || !scrubber) return;

  // Day-0 state: the sim bar keeps the panel visible but dims the scrubber
  // and the ▶ Run button so the user can see the control exists without
  // being able to interact with an empty history.
  if (_history.length < 2) {
    panel.classList.add('day-zero');
    scrubber.max   = 0;
    scrubber.value = 0;
    document.getElementById('tlStart').textContent   = t('day_n', { n: 0 });
    document.getElementById('tlCurrent').textContent = t('day_n', { n: currentState?.day ?? 0 });
    const replay = document.getElementById('btnReplay');
    if (replay) replay.disabled = true;
    const cmpBtn = document.getElementById('btnCompare');
    if (cmpBtn) cmpBtn.disabled = true;
    return;
  }

  panel.classList.remove('day-zero');
  scrubber.max   = _history.length - 1;
  scrubber.value = _history.length - 1;
  document.getElementById('tlStart').textContent   = t('day_n', { n: _history[0].day });
  document.getElementById('tlCurrent').textContent = t('day_n', { n: currentState.day });
  _paintScrubberVisuals();
  // Replay + Compare only make sense once we have ≥ 2 days of history.
  const replay = document.getElementById('btnReplay');
  if (replay) replay.disabled = _history.length < 2;
  const cmpBtn = document.getElementById('btnCompare');
  if (cmpBtn) cmpBtn.disabled = _history.length < 2;

  // While compare is active and history grows, keep the popover's day-B
  // upper bound in sync; the Day-A and Day-B inputs are separate sliders
  // in the popover so they don't fight with the live scrubber.
  if (isCompareActive()) {
    const sA = document.getElementById('cmpDayARange');
    const sB = document.getElementById('cmpDayBRange');
    if (sA) sA.max = String(_history.length - 1);
    if (sB) sB.max = String(_history.length - 1);
  }
}

/**
 * Debounce a write of the full _history array to IndexedDB. Coalesces a
 * burst of day commits at high sim speed into a single save. The save is
 * keyed by the active city id; if no city is active we silently skip.
 */
function _scheduleHistorySave() {
  if (!_activeCityId) return;
  if (_historySaveTimer !== null) clearTimeout(_historySaveTimer);
  _historySaveTimer = setTimeout(() => {
    _historySaveTimer = null;
    const cityId = _activeCityId;
    const snap   = _history;
    if (cityId && snap.length > 0) saveHistory(cityId, snap);
  }, 500);
}

/**
 * Try to restore a previously-persisted timeline for the given city. Only
 * restores if the persisted history is consistent with the engine's
 * current day (the last persisted snapshot's day must match
 * `currentState.day`); otherwise the engine has moved on without us and
 * the cache is dropped to avoid mixing inconsistent state.
 */
async function _restoreHistory(cityId) {
  if (!cityId || !currentState) return;
  let loaded;
  try {
    loaded = await loadHistory(cityId);
  } catch {
    return;
  }
  if (!loaded || loaded.length === 0) return;
  // Active city or sim mode may have changed during the async load.
  if (cityId !== _activeCityId) return;
  if (_historyIdx !== -1) return;       // user is already scrubbing — don't clobber

  const lastDay = loaded[loaded.length - 1]?.day;
  if (lastDay !== currentState.day) {
    // Engine drifted from cache — drop the stale snapshot.
    clearHistory(cityId);
    return;
  }
  _history = loaded;
  if (_history.length > HISTORY_MAX) {
    _history = _history.slice(_history.length - HISTORY_MAX);
  }
  _updateScrubber();
}

// ── Stats ─────────────────────────────────────────────────────────────────────

// Last-painted SEIR snapshot, used to colour the up/down trend arrows
// drawn next to each KPI value. Reset whenever the city changes.
let _lastSEIR = null;

function _updateStats(state) {
  // Game HUD (resource bars). Snapshot lives on state.game when game
  // mode is active; null in sandbox so the HUD stays hidden.
  renderGameHud(state.game, state.day);
  // Time-locked interventions (e.g. vaccination rollout day).
  applyIvGating(state);
  // Disable spatial tools that depend on prerequisites (targeted vaccine
  // requires the citywide Vaccination intervention + rollout).
  applySpatialToolGating(state);
  // Spatial overlay — per-cell I values drive ambulance "seek hottest
  // building" targeting (the 3D ambulance walks on the road graph).
  setSpatialLatestState(state);
  ambulance3d.setCells(state.cells);
  document.getElementById('statS').textContent      = _fmt(state.S);
  document.getElementById('statE').textContent      = _fmt(state.E);
  document.getElementById('statI').textContent      = _fmt(state.I);
  document.getElementById('statR').textContent      = _fmt(state.R);
  document.getElementById('statD').textContent      = _fmt(state.D);
  document.getElementById('statCases').textContent  = _fmt(state.cases);
  // Day counter shows "Day N · Mon" with the weekday name from the
  // backend. Weekend days get an extra dot of accent so the user
  // notices the social-mixing change without reading the label.
  const wkKeys = ['wk_mon','wk_tue','wk_wed','wk_thu','wk_fri','wk_sat','wk_sun'];
  const wd = (typeof state.weekday === 'number') ? state.weekday : (state.day % 7);
  const wkLabel = t(wkKeys[wd] || 'wk_mon');
  const dc = document.getElementById('dayCounter');
  if (dc) {
    dc.textContent = `${t('day_n', { n: state.day })} · ${wkLabel}`;
    dc.classList.toggle('is-weekend', !!state.is_weekend || wd >= 5);
  }

  // Trend arrows — only after the sim has actually advanced. At day 0
  // there's no "previous" tick to compare against; showing arrows there
  // would just be noise.
  const _clearTrend = (id) => {
    const el = document.getElementById(id);
    if (el) el.className = 'trend';
  };
  if (state.day > 0 && _lastSEIR) {
    const _trend = (id, cur, prev) => {
      const el = document.getElementById(id);
      if (!el) return;
      const d = cur - prev;
      const dir = Math.abs(d) < 0.5 ? '' : (d > 0 ? 'up' : 'down');
      el.className = 'trend' + (dir ? ' ' + dir : '');
    };
    _trend('trendS', state.S, _lastSEIR.S);
    _trend('trendE', state.E, _lastSEIR.E);
    _trend('trendI', state.I, _lastSEIR.I);
    _trend('trendR', state.R, _lastSEIR.R);
    _trend('trendD', state.D, _lastSEIR.D);
  } else {
    ['trendS','trendE','trendI','trendR','trendD'].forEach(_clearTrend);
  }
  _lastSEIR = { S: state.S, E: state.E, I: state.I, R: state.R, D: state.D };

  // R-eff: only meaningful once an outbreak is seeded. Before the first
  // infectious day, show '—' and leave the gauge empty so the HUD doesn't
  // claim a spurious reproduction number.
  const badge   = document.getElementById('rEffBadge');
  const r       = state.r_eff;
  const seeded  = state.day > 0 && (state.I + state.E) > 0;
  if (badge) badge.textContent = seeded ? r.toFixed(2) : '—';
  if (seeded) {
    const reffState = r > 1.1 ? 'danger' : r < 0.9 ? 'safe' : 'warn';
    setGauge(r);
    setGaugeColor(reffState);
  } else {
    setGauge(0);
    setGaugeColor('flat');
  }

  // Growth indicator — % change in active infections (E + I) over the
  // last engine day. Sourced from the history tail rather than the live
  // last-render diff so scrub/replay don't poison the value.
  _updateGrowth(state);

  // Season pill in the timeline header. Pulls from state.climate.season
  // (the engine ships it on every step). Icon + translated label.
  _updateSeasonPill(state.climate);

  // Bottom climate strip — temperature + today's rolled weather. Pass the
  // snapshot's saved weather (set when the day rolled) so timeline scrubs
  // show each historical day's actual snow/rain/cloud/sun.
  _updateClimateStrip(state.climate, state.weather);

  // Live statistics panel (R₀, β_eff, dynamics, severity, climate, origin).
  renderStats(state);

  // Hospital-overload banner removed — once triggered the engine keeps
  // it on for the rest of the run, so it stops carrying any signal.
}

// Season → emoji icon. Engine ships season as 'Winter' / 'Spring' / 'Summer'
// / 'Autumn'. Pill is hidden gracefully when no climate data is available
// (e.g. before the first step lands).
const _SEASON_ICONS = {
  winter: '❄️', spring: '🌱', summer: '☀️', autumn: '🍂',
};

function _updateSeasonPill(climate) {
  const pill = document.getElementById('seasonPill');
  const ico  = document.getElementById('seasonIco');
  const lbl  = document.getElementById('seasonLbl');
  if (!pill || !ico || !lbl) return;
  const season = (climate?.season || '').toLowerCase();
  if (!season || !_SEASON_ICONS[season]) {
    pill.style.display = 'none';
    return;
  }
  pill.style.display = '';
  pill.dataset.season = season;
  ico.textContent = _SEASON_ICONS[season];
  lbl.textContent = t('season_' + season);
  // Tooltip: temperature + climate β multiplier so the user sees why
  // transmission rises in winter without opening the stats panel.
  if (typeof climate.temperature === 'number') {
    const bm = climate.beta_mult ?? 1;
    pill.title = `${t('stat_temperature')}: ${climate.temperature.toFixed(1)} °C · β× ${bm.toFixed(2)}`;
  } else {
    pill.title = '';
  }
}

// Weather → emoji + i18n key for the bottom climate strip.
const _WX_ICONS = {
  snow:   '❄️',
  rain:   '🌧️',
  cloudy: '☁️',
  clear:  '☀️',
};
const _WX_KEYS = {
  snow:   'wx_snow',
  rain:   'wx_rain',
  cloudy: 'wx_cloudy',
  clear:  'wx_clear',
};

function _updateClimateStrip(climate, weatherOverride) {
  const tempPill = document.getElementById('csTempPill');
  const tempIco  = document.getElementById('csTempIco');
  const tempVal  = document.getElementById('csTempVal');
  const wxPill   = document.getElementById('csWxPill');
  const wxIco    = document.getElementById('csWxIco');
  const wxVal    = document.getElementById('csWxVal');
  if (!tempPill || !wxPill) return;

  // Temperature
  if (climate && typeof climate.temperature === 'number') {
    const T  = climate.temperature;
    const bm = climate.beta_mult ?? 1;
    let band = 'mild';
    if (T < 0)         band = 'cold';
    else if (T > 22)   band = 'hot';
    else if (T > 14)   band = 'warm';
    tempPill.dataset.temp = band;
    tempIco.textContent   = T < 0 ? '🥶' : T > 22 ? '🥵' : '🌡';
    tempVal.textContent   = `${T.toFixed(1)} °C`;
    tempPill.title        = `${t('stat_temperature')}: ${T.toFixed(1)} °C · β× ${bm.toFixed(2)}`;
    tempPill.style.display = '';
  } else {
    tempPill.style.display = 'none';
  }

  // Prefer the snapshot's saved weather (stored on each day's state when
  // it rolled), so scrubbing through the timeline shows each historical
  // day's actual weather instead of the live one. Fall back to the live
  // weather (live mode) and finally to 'clear' if nothing has rolled yet.
  const wx = weatherOverride || getCurrentWeather() || 'clear';
  wxPill.dataset.wx = wx;
  wxIco.textContent = _WX_ICONS[wx] || '·';
  wxVal.textContent = t(_WX_KEYS[wx] || 'wx_clear');
  wxPill.style.display = '';
}

function _updateGrowth(state) {
  const badge = document.getElementById('growthBadge');
  if (!badge) return;
  const hist = state.history;
  if (!Array.isArray(hist) || hist.length < 2) {
    badge.textContent = '—';
    badge.className = 'val flat';
    setGrowthGauge(0, 'flat');
    return;
  }
  const cur  = hist[hist.length - 1];
  const prev = hist[hist.length - 2];
  const active     = (cur.E  || 0) + (cur.I  || 0);
  const activePrev = (prev.E || 0) + (prev.I || 0);
  if (activePrev <= 1) {
    badge.textContent = active > activePrev ? '+new' : '0%';
    badge.className   = 'val ' + (active > activePrev ? 'up' : 'flat');
    setGrowthGauge(active > activePrev ? 0.4 : 0, active > activePrev ? 'up' : 'flat');
    return;
  }
  const pct = ((active - activePrev) / activePrev) * 100;
  const cls = pct > 1 ? 'up' : pct < -1 ? 'down' : 'flat';
  const sign = pct > 0 ? '+' : '';
  badge.textContent = `${sign}${pct.toFixed(1)}%`;
  badge.className   = 'val ' + cls;
  // Map percentage to gauge fraction: ±50% pegs the arc, with a sign-
  // independent fill so users see magnitude regardless of direction.
  setGrowthGauge(Math.min(1, Math.abs(pct) / 50), cls);
}

function _fmt(n) { return Math.round(n).toLocaleString(); }

/** Swap the play/pause SVG icon and `.playing` class on #btnPlay. */
function _syncPlayIcon(isPlaying) {
  const btn = document.getElementById('btnPlay');
  if (!btn) return;
  btn.classList.toggle('playing', !!isPlaying);
  btn.title = isPlaying ? t('pause') : t('play');
  const useEl = btn.querySelector('use');
  if (useEl) useEl.setAttribute('href', isPlaying ? '#ic-pause' : '#ic-play');
}

// ── Interventions ─────────────────────────────────────────────────────────────

// Map intervention backend IDs to i18n keys
const _IV_I18N = {
  lockdown:        { label: 'iv_lockdown',       desc: 'iv_lockdown_desc' },
  masks:           { label: 'iv_masks',          desc: 'iv_masks_desc' },
  distancing:      { label: 'iv_distancing',     desc: 'iv_distancing_desc' },
  vaccination:     { label: 'iv_vaccination',    desc: 'iv_vaccination_desc' },
  schools:         { label: 'iv_schools',        desc: 'iv_schools_desc' },
  testing:         { label: 'iv_testing',        desc: 'iv_testing_desc' },
  transit_masks:   { label: 'iv_transit_masks',  desc: 'iv_transit_masks_desc' },
  suspend_flights: { label: 'iv_suspend_flights',desc: 'iv_suspend_flights_desc' },
  transit_screening:{ label: 'iv_screening',     desc: 'iv_screening_desc' },
  // Game-mode additions (translations live in i18n.js).
  info_campaign:    { label: 'iv_info_campaign',    desc: 'iv_info_campaign_desc' },
  hospital_surge:   { label: 'iv_hospital_surge',   desc: 'iv_hospital_surge_desc' },
  targeted_lockdown:{ label: 'iv_targeted_lockdown',desc: 'iv_targeted_lockdown_desc' },
  stop_transport:   { label: 'iv_stop_transport',   desc: 'iv_stop_transport_desc' },
};

function _buildInterventionButtons(interventions) {
  const list = document.getElementById('ivList');
  if (!list) return;
  list.innerHTML = '';
  // Reset the sidebar count chip + mini summary; _toggleIntervention
  // bumps both on click.
  const activeN = (currentState?.active_interventions || []).length;
  setSectionBadge('sp-iv', String(activeN));
  setInterventionsCount(activeN, interventions.length);
  for (const iv of interventions) {
    const btn = document.createElement('div');
    btn.className = 'iv-row';
    btn.id = 'iv-' + iv.id;
    btn.setAttribute('role', 'button');
    btn.tabIndex = 0;
    const i18n = _IV_I18N[iv.id];
    const label = i18n ? t(i18n.label) : iv.label;
    const desc  = i18n ? t(i18n.desc)  : iv.desc;
    btn.innerHTML = `
      <div class="ico">${iv.icon}</div>
      <div class="body">
        <div class="t">${label}</div>
        <div class="s">${desc}</div>
      </div>
      <div class="sw"></div>`;
    btn.addEventListener('click', () => _toggleIntervention(iv.id));
    btn.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); _toggleIntervention(iv.id); } });
    list.appendChild(btn);
  }
}

// When these interventions activate, auto-show their related POI categories
// so the user immediately sees the prohibition marks.
const _IV_AUTO_SHOW_POI = {
  suspend_flights: ['airport'],
  schools:         ['8'],          // Zone.SCHOOL
};

async function _toggleIntervention(id) {
  // Refuse early in JS too so a locked vaccination button feels dead
  // rather than failing silently after a backend round-trip.
  if (isIvLocked(id, currentState)) return;
  const resp = await fetch(`/api/intervention/${id}`, { method: 'POST' })
    .then(r => r.json());
  const active = new Set(resp.active_interventions);
  document.querySelectorAll('.iv-row, .iv-btn').forEach(btn => {
    btn.classList.toggle('active', active.has(btn.id.replace('iv-', '')));
  });
  // Live update of the intervention count chip in the sidebar header
  // plus the single mini-mode icon with an active/total badge.
  const totalIv = document.querySelectorAll('.iv-row, .iv-btn').length;
  setSectionBadge('sp-iv', String(active.size));
  setInterventionsCount(active.size, totalIv);
  refreshRailMirrors();
  // Push the change into the scene visuals immediately so the user sees
  // particles vanish under lockdown / school pins gain the closed marker
  // without having to wait for the next /api/step to land.
  if (_flowField) _flowField.setInterventions(active);
  setActiveInterventions(active);
  transport.setInterventions(active);
  // Refresh the game HUD so the trust hit from a new measure shows up
  // immediately instead of waiting for the next /api/step.
  refreshGameHud();
  // Spatial palette mirrors active items — refresh in case any spatial
  // costs ticked in response to a citywide toggle (no-op if list unchanged).
  refreshSpatialTools();

  // Auto-show related POI categories when the intervention is activated,
  // so the user can see the prohibition marks without manually toggling POIs.
  if (active.has(id) && _IV_AUTO_SHOW_POI[id]) {
    for (const k of _IV_AUTO_SHOW_POI[id]) {
      setPOICategoryVisible(k, true);
      // Sync the checkbox in the POI menu if it exists
      const cb = document.querySelector(`input[data-key="${k}"]`);
      if (cb) cb.checked = true;
    }
  }
}

// ── Age distribution by zone type ─────────────────────────────────────────────

// Fallback age distribution per zone — only used if the backend didn't
// return per-cell age data (e.g. very old snapshot). SCB data supersedes this.
const ZONE_AGE_DIST = {
  1: [25, 55, 20], 2: [22, 58, 20], 3: [15, 60, 25],
  4: [ 5, 85, 10], 5: [ 2, 90,  8], 6: [10, 50, 40],
  7: [30, 40, 30], 8: [75, 23,  2],
};

/**
 * Render the age breakdown block for the tooltip.
 * Prefers live age-stratified SEIR data from the backend; falls back to the
 * static ZONE_AGE_DIST heuristic only when no per-cell age data is present.
 */
function _ageHtml(layoutBuilding, seir) {
  let child, adult, elder;
  const live = seir && seir.age;

  if (live) {
    const sum = g => (g.S ?? 0) + (g.E ?? 0) + (g.I ?? 0) + (g.R ?? 0);
    child = sum(live.child);
    adult = sum(live.adult);
    elder = sum(live.elder);
  } else {
    const d = ZONE_AGE_DIST[layoutBuilding.zone];
    if (!d || !layoutBuilding.pop) return '';
    child = Math.round(layoutBuilding.pop * d[0] / 100);
    adult = Math.round(layoutBuilding.pop * d[1] / 100);
    elder = Math.round(layoutBuilding.pop * d[2] / 100);
  }

  return `<div style="border-top:1px solid #334155;margin-top:5px;padding-top:4px;font-size:0.69rem">
    <div style="color:#64748b;margin-bottom:2px">${t('age_breakdown')}</div>
    <span style="color:#86efac">${t('age_child')}: ${_fmt(child)}</span>&nbsp;
    <span style="color:#fde68a">${t('age_adult')}: ${_fmt(adult)}</span>&nbsp;
    <span style="color:#c4b5fd">${t('age_elder')}: ${_fmt(elder)}</span>
  </div>`;
}

/**
 * Country-of-birth breakdown for a building. Reads the engine-supplied
 * `origin` 6-vector + `predominant_origin` index. Only renders when both
 * are present and the building has population (skips roads, water, parks).
 */
function _originHtml(layoutBuilding) {
  const origin = layoutBuilding.origin;
  const groups = window._epiCityOriginGroups;
  if (!origin || !groups || origin.length !== groups.length) return '';
  if (!(layoutBuilding.pop > 0) && !(layoutBuilding.capacity > 0)) return '';
  const lang = window._epiCityLang || 'en';

  // Render top 3 groups by share, then "+N more" if any tail.
  const ranked = origin
    .map((f, i) => ({
      i, f,
      label: (lang === 'sv' && groups[i].label_sv) || groups[i].label_en,
      color: groups[i].color,
      flag:  groups[i].flag || '',
    }))
    .sort((a, b) => b.f - a.f);
  const top = ranked.slice(0, 3).filter(r => r.f >= 0.005);
  if (!top.length) return '';

  const rows = top.map(r => `
    <span style="display:inline-flex;align-items:center;gap:4px;margin-right:7px">
      ${r.flag ? `<span class="country-flag">${r.flag}</span>` : ''}
      <span style="color:#cbd5e1">${r.label}</span>
      <span style="color:#94a3b8">${(r.f * 100).toFixed(0)}%</span>
    </span>`).join('');

  return `<div style="border-top:1px solid #334155;margin-top:5px;padding-top:4px;font-size:0.69rem">
    <div style="color:#64748b;margin-bottom:3px">${t('country_of_birth')}</div>
    ${rows}
  </div>`;
}

// Format a building's footprint. Small footprints (<1000 m²) stay in
// m²; larger ones render in hectares to avoid long strings like "48,200 m²".
function _fmtArea(m2) {
  if (!m2 || m2 <= 0) return null;
  if (m2 >= 10000) return `${(m2 / 10000).toFixed(1)} ha`;
  if (m2 >= 1000)  return `${(m2 / 1000).toFixed(1)}k m²`;
  return `${Math.round(m2)} m²`;
}

// ── Hover tooltip ─────────────────────────────────────────────────────────────

function _onHover(layoutBuilding) {
  _hoveredBuilding = layoutBuilding;
  const tip = document.getElementById('tooltip');
  // Suppress building tooltip when the Areas overlay is active —
  // the user should only see area-level hover popups in that mode.
  if (isAreasActive()) { tip.style.display = 'none'; return; }
  // Suppress while the user is interacting with ANY chrome menu —
  // sidebar (`sidebar-interacting`) or anything else covered by the
  // generic guard (`ui-interacting`): timeline, top bar, settings,
  // stats, search popovers, modals, the game HUD, etc.
  if (document.body.classList.contains('sidebar-interacting')
      || document.body.classList.contains('ui-interacting')) {
    tip.style.display = 'none';
    return;
  }
  if (!layoutBuilding || !currentState) { tip.style.display = 'none'; return; }

  const cells = _historyIdx >= 0 ? _history[_historyIdx]?.cells : currentState.cells;
  if (!cells) { tip.style.display = 'none'; return; }

  // cells array is indexed by building idx
  const seir = cells[layoutBuilding.idx];
  if (!seir)  { tip.style.display = 'none'; return; }

  const N = seir.N;

  // Water buildings use their specific water_type for the zone label
  let _zname;
  if (layoutBuilding.zone === 19 && layoutBuilding.water_type) {
    _zname = poiName('water_' + layoutBuilding.water_type);
  } else {
    _zname = ZONE_NAMES[layoutBuilding.zone] ?? t('building');
  }
  const _title = layoutBuilding.name
    ? `${layoutBuilding.name} <span style="color:#94a3b8">· ${_zname}</span>`
    : _zname;

  // Neighborhood label — prefer the human-readable RegSO name (e.g.
  // "Teleborg", "Hovshaga") over the raw DeSO code.
  const nbhName = layoutBuilding.neighborhood || layoutBuilding.deso;
  const neighborhood = nbhName
    ? `<div style="color:#94a3b8;font-size:0.69rem;margin-top:1px">${t('neighborhood')}: ${nbhName}</div>`
    : '';

  // Mean net income (SEK thousands) for the DeSO the building sits in.
  // Residents-only: income is a property of the household, so showing
  // it on a school or restaurant would be misleading. Zones 1/2/3 are
  // the three residential densities (RES_LO / RES_MED / RES_HI).
  const _isResidential = (layoutBuilding.zone >= 1 && layoutBuilding.zone <= 3);
  const incomeHtml = (_isResidential && layoutBuilding.income != null)
    ? `<div style="color:#d4d4d8;font-size:0.72rem;margin-top:1px">${t('income_label')}: ${_fmt(layoutBuilding.income)} kSEK</div>`
    : '';

  // Gender distribution from SCB DeSO data.
  const gf = layoutBuilding.gender_frac;
  const genderHtml = gf
    ? `<div style="font-size:0.69rem;margin-top:1px;color:#94a3b8">` +
      `<span style="color:#60a5fa">♂ ${(gf.male_frac * 100).toFixed(1)}%</span> ` +
      `<span style="color:#f472b6">♀ ${(gf.female_frac * 100).toFixed(1)}%</span></div>`
    : '';

  // Household stats — estimated count from average household size.
  let householdHtml = '';
  if (_isResidential && layoutBuilding.household_avg_size && layoutBuilding.pop > 0) {
    const avgSz = layoutBuilding.household_avg_size;
    const estHH = Math.max(1, Math.round(layoutBuilding.pop / avgSz));
    householdHtml = `<div style="font-size:0.69rem;margin-top:1px;color:#94a3b8">` +
      `${t('households_est', { n: _fmt(estHH), avg: avgSz.toFixed(1) })}</div>`;
  }

  // Water (zone 19) is a hover-only natural feature with no SEIR —
  // skip the compartment block and render just the name + surface.
  if (layoutBuilding.zone === 19) {
    const ha = (layoutBuilding.area_m2 / 10000).toFixed(2);
    tip.innerHTML = `
      <b>${_title}</b><br>
      ${t('surface')}: ${ha} ha
    `;
  } else {
    // Natural-feature rows — shown above the SEIR block so the forest
    // capacity that drove the selection is the first thing the user sees.
    let natureHtml = '';
    if (layoutBuilding.zone === 20 && layoutBuilding.tree_species) {
      const specLabel = {
        spruce: t('tree_spruce'),
        pine:   t('tree_pine'),
        birch:  t('tree_birch'),
        oak:    t('tree_oak'),
        mixed:  t('tree_mixed'),
      }[layoutBuilding.tree_species] || layoutBuilding.tree_species;
      const tc  = layoutBuilding.tree_count != null ? _fmt(layoutBuilding.tree_count) : '?';
      natureHtml = `
        <div style="font-size:0.72rem;margin-top:1px">
          Species: <b>${specLabel}</b> &nbsp; Trees: ${tc}
        </div>
      `;
    }

    // Pack building metadata on one line: population · footprint · levels.
    // Each piece is optional so buildings missing a field don't render
    // dangling separators. Non-residential buildings show their occupancy
    // as "Capacity" since the inhabitants aren't household residents.
    const parts = [];
    if (_isResidential && layoutBuilding.pop) {
      parts.push(`Pop ${_fmt(layoutBuilding.pop)}`);
    } else if (!_isResidential) {
      // Non-residential: show the synthetic occupancy from the backend
      // (`capacity`), or fall back to whatever pop value exists. Always
      // shown so the user knows how many people the building can hold.
      const cap = layoutBuilding.capacity != null
        ? layoutBuilding.capacity
        : (layoutBuilding.pop || 0);
      if (cap > 0) parts.push(`Capacity ${_fmt(cap)}`);
    }
    const areaStr = _fmtArea(layoutBuilding.area_m2);
    if (areaStr)                         parts.push(areaStr);
    if (layoutBuilding.levels && layoutBuilding.levels > 1) {
      parts.push(`${layoutBuilding.levels} lv`);
    }
    const metaLine = parts.length
      ? `<div style="font-size:0.72rem;margin-top:2px">${parts.join(' · ')}</div>`
      : '';

    // Compact SEIR — all five compartments on a single line, with a
    // second line for prevalence when we have a valid N.
    const seirLine = `
      <div style="font-size:0.72rem;margin-top:4px;letter-spacing:-0.01em">
        <span style="color:#4ade80">S ${_fmt(seir.S)}</span>
        <span style="color:#facc15">E ${_fmt(seir.E)}</span>
        <span style="color:#f87171">I ${_fmt(seir.I)}</span>
        <span style="color:#60a5fa">R ${_fmt(seir.R)}</span>
        <span style="color:#94a3b8">D ${_fmt(seir.D)}</span>
      </div>
    `;

    // Infection-site counter — number of S→E transmission events that
    // occurred AT this building (sparse: ABM-only and only present
    // when > 0). Distinct from `I` above (count of infected residents
    // whose home_building is here); this is "how many times has this
    // building hosted a transmission event".
    const _ih = seir.infections_here ?? 0;
    const infHereHtml = _ih > 0
      ? `<div style="font-size:0.68rem;margin-top:1px;color:#fda4af">` +
        `${t('tooltip_infections_here')}: <b>${_fmt(_ih)}</b></div>`
      : '';

    tip.innerHTML = `
      <b>${_title}</b>
      ${neighborhood}
      ${metaLine}
      ${incomeHtml}
      ${genderHtml}
      ${householdHtml}
      ${natureHtml}
      ${_originHtml(layoutBuilding)}
      ${seirLine}
      ${N > 0 ? `<div style="font-size:0.68rem;color:#cbd5e1">${t('prevalence')}: ${(seir.I / N * 100).toFixed(1)}%</div>` : ''}
      ${infHereHtml}
      ${_ageHtml(layoutBuilding, seir)}
    `;
  }

  const tipW = 220, tipH = 150;
  const left = Math.min(mouseX + 14, window.innerWidth  - tipW - 6);
  const top  = Math.min(mouseY + 14, window.innerHeight - tipH - 6);
  tip.style.left    = left + 'px';
  tip.style.top     = top  + 'px';
  tip.style.display = 'block';
}

// ── Transport vehicle tooltip ───────────────────────────────────────────────

const _TYPE_EMOJI = { bus: '🚌', tram: '🚋', train: '🚆', ferry: '⛴️', plane: '✈️' };

let _ambHoveredId = null;

function _onTransportMouseMove() {
  const mvp = getMVPMatrix();
  const rect = getCanvasRect();

  // Bus / tram / train / ferry / plane hover (only when transport layer is on)
  const vi = (_transportActive && _transportLoaded)
    ? transport.hitTest(mouseX, mouseY, mvp, rect) : -1;
  if (vi !== _transportHoveredVi) {
    _transportHoveredVi = vi;
    transport.setHoveredVehicle(vi);
  }

  // Ambulance hover — runs regardless of the transport layer toggle so
  // mobile clinics are always interactive.
  const ambId = ambulance3d.hitTest(mouseX, mouseY, mvp, rect);
  if (ambId !== _ambHoveredId) _ambHoveredId = ambId;

  // Render priority: hovered bus first, then hovered ambulance, then the
  // selected vehicle (if any), else hide.
  if (vi >= 0) {
    _showTransportTip(vi);
    document.body.style.cursor = 'pointer';
    const bTip = document.getElementById('tooltip');
    if (bTip) bTip.style.display = 'none';
  } else if (ambId) {
    _showAmbulanceTip(ambId);
    document.body.style.cursor = 'pointer';
    const bTip = document.getElementById('tooltip');
    if (bTip) bTip.style.display = 'none';
  } else {
    document.body.style.cursor = '';
    if (_transportSelectedVi >= 0) {
      _showTransportTip(_transportSelectedVi);
    } else if (ambulance3d.getSelected()) {
      _showAmbulanceTip(ambulance3d.getSelected());
    } else {
      _hideTransportTip();
    }
  }
}

function _onTransportClick() {
  if (!_transportActive || !_transportLoaded) {
    // Even when the transport layer is off, ambulances may be on-screen
    // (mobile clinics spawn them independently). Let those handle clicks.
    return _onAmbulanceClick();
  }
  const mvp = getMVPMatrix();
  const rect = getCanvasRect();
  const vi = transport.hitTest(mouseX, mouseY, mvp, rect);
  if (vi >= 0) {
    // Toggle selection: click same vehicle again to deselect
    if (_transportSelectedVi === vi) {
      _transportSelectedVi = -1;
      transport.setSelectedVehicle(-1);
      _hideTransportTip();
    } else {
      _transportSelectedVi = vi;
      transport.setSelectedVehicle(vi);
      _showTransportTip(vi);
      ambulance3d.setSelected(null);
    }
    return;
  }
  // Bus miss — try the ambulance fleet (mobile clinics).
  if (_onAmbulanceClick()) return;
  // Clicked empty space — clear selection
  if (_transportSelectedVi >= 0) {
    _transportSelectedVi = -1;
    transport.setSelectedVehicle(-1);
    _hideTransportTip();
  }
  ambulance3d.setSelected(null);
}

/** Returns true if an ambulance was hit (and selected). */
function _onAmbulanceClick() {
  const mvp = getMVPMatrix();
  const rect = getCanvasRect();
  const id = ambulance3d.hitTest(mouseX, mouseY, mvp, rect);
  if (!id) {
    if (ambulance3d.getSelected()) { ambulance3d.setSelected(null); _hideTransportTip(); }
    return false;
  }
  // Toggle: click same ambulance again to deselect.
  if (ambulance3d.getSelected() === id) {
    ambulance3d.setSelected(null);
    _hideTransportTip();
    return true;
  }
  ambulance3d.setSelected(id);
  // Reuse the transport tooltip element for visual parity with buses.
  _showAmbulanceTip(id);
  // Clear bus selection so the two highlights don't compete.
  if (_transportSelectedVi >= 0) {
    _transportSelectedVi = -1;
    transport.setSelectedVehicle(-1);
  }
  return true;
}

function _showAmbulanceTip(id) {
  const tip = document.getElementById('transportTip');
  if (!tip) return;
  const items = currentState?.spatial || [];
  const item = items.find(it => it.id === id);
  const label = item
    ? (item.label_key ? t(item.label_key, item.label_vars || {}) : item.label)
    : t('sp_mobile_clinic');
  const info = ambulance3d.getInfo(id) || {};
  const isSelected = (ambulance3d.getSelected() === id);
  const selBadge = isSelected
    ? ` <span style="color:#fbbf24;font-size:0.69rem">● ${t('veh_selected')}</span>`
    : '';

  // Route line — "Building A → Building B". Falls back gracefully when
  // we can't resolve a real building name (small towns, water clicks).
  let route = '';
  if (info.from || info.to) {
    const from = info.from || '—';
    const to   = info.to   || '—';
    route = `<div style="font-size:0.72rem;color:#94a3b8">→ ${_escapeHtml(from)} → ${_escapeHtml(to)}</div>`;
  }

  const days = item?.days_remaining;
  const pop  = item?.population;
  tip.innerHTML = `
    <div style="font-weight:600;font-size:0.88rem">🚑 ${label}${selBadge}</div>
    ${pop != null ? `<div style="font-size:0.72rem;color:#cbd5e1">${pop.toLocaleString()} pop · ${days ?? '∞'}d</div>` : ''}
    ${route}
  `;
  tip.style.display = 'block';
  _positionAmbulanceTip(id);
}

function _positionAmbulanceTip(id) {
  const tip = document.getElementById('transportTip');
  if (!tip || tip.style.display === 'none') return;
  const tipW = 240, tipH = 110;
  const isSelected = ambulance3d.getSelected() === id;
  if (isSelected) {
    // Follow the moving ambulance via screen-space projection — same
    // pattern as bus selection.
    const mvp = getMVPMatrix();
    const rect = getCanvasRect();
    const sp = ambulance3d.getScreenPos(id, mvp, rect);
    if (!sp) return;
    const left = Math.min(Math.max(6, sp.sx + 18), window.innerWidth  - tipW - 6);
    const top  = Math.min(Math.max(6, sp.sy - tipH / 2), window.innerHeight - tipH - 6);
    tip.style.left = left + 'px';
    tip.style.top  = top  + 'px';
  } else {
    // Hovered (not selected) → cursor-anchored
    const left = Math.min(mouseX + 14, window.innerWidth - tipW - 6);
    const top  = Math.min(mouseY + 14, window.innerHeight - tipH - 6);
    tip.style.left = left + 'px';
    tip.style.top  = top  + 'px';
  }
}

function _escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function _showTransportTip(vi) {
  const tip = document.getElementById('transportTip');
  if (!tip) return;
  const info = transport.getVehicleInfo(vi);
  if (!info) { tip.style.display = 'none'; return; }

  const emoji = _TYPE_EMOJI[info.baseType] || '🚍';
  const isSelected = _transportSelectedVi === vi;
  const selBadge = isSelected
    ? ` <span style="color:#fbbf24;font-size:0.69rem">● ${t('veh_selected')}</span>` : '';

  const typeLabel = t('veh_' + info.baseType);
  let routeLabel = typeLabel;
  if (info.routeNum) routeLabel += ` #${info.routeNum}`;

  let dest = '';
  if (info.destination) dest = `<div style="font-size:0.72rem;color:#94a3b8">→ ${info.destination}</div>`;

  // Per-vehicle transit transmission counter. The vehicle's lineId is
  // hashed into one of the ABM's 24 synthetic commute pools at build
  // time, so this is "transmissions on this route's commute pool" —
  // vehicles on the same line share a count, vehicles on different
  // lines diverge as the outbreak hits some pools harder than others.
  // Only shown when > 0 to keep the tooltip compact for vehicles whose
  // pool has stayed clean and for the compartmental engine.
  const _pi = info.poolInfections | 0;
  const transitHtml = _pi > 0
    ? `<div style="font-size:0.68rem;margin-top:1px;color:#fda4af">` +
      `${t('tooltip_transit_transmissions')}: <b>${_fmt(_pi)}</b></div>`
    : '';

  tip.innerHTML = `
    <div style="font-weight:600;font-size:0.88rem">${emoji} ${routeLabel}${selBadge}</div>
    ${info.operator ? `<div style="font-size:0.72rem;color:#cbd5e1">${t('veh_operator')}: <b>${info.operator}</b></div>` : ''}
    <div style="font-size:0.72rem;color:#cbd5e1">${t('veh_capacity')}: ${info.capacity} ${t('veh_pax')}</div>
    <div style="font-size:0.72rem;color:#cbd5e1">${t('veh_stops')}: ${info.stops}</div>
    ${dest}
    ${transitHtml}
  `;
  tip.style.display = 'block';
  _positionTransportTip(vi);
}

function _positionTransportTip(vi) {
  const tip = document.getElementById('transportTip');
  if (!tip || tip.style.display === 'none') return;

  const isSelected = (_transportSelectedVi === vi);
  const tipW = 220, tipH = 150;

  if (isSelected) {
    // Selected: tooltip follows the vehicle's screen position
    const mvp = getMVPMatrix();
    const rect = getCanvasRect();
    const sp = transport.getVehicleScreenPos(vi, mvp, rect);
    if (!sp) return;
    const left = Math.min(Math.max(6, sp.sx + 18), window.innerWidth  - tipW - 6);
    const top  = Math.min(Math.max(6, sp.sy - tipH / 2), window.innerHeight - tipH - 6);
    tip.style.left = left + 'px';
    tip.style.top  = top  + 'px';
  } else {
    // Hovered: tooltip appears at the mouse cursor (like building tooltips)
    const left = Math.min(mouseX + 14, window.innerWidth  - tipW - 6);
    const top  = Math.min(mouseY + 14, window.innerHeight - tipH - 6);
    tip.style.left = left + 'px';
    tip.style.top  = top  + 'px';
  }
}

function _hideTransportTip() {
  const tip = document.getElementById('transportTip');
  if (tip) { tip.style.display = 'none'; }
}

/** Called from the animation frame to keep tooltip anchored to the moving vehicle. */
function _updateTransportTipPosition() {
  // Selected bus / tram / train / ferry / plane → track its on-screen
  // position each frame so the popup follows the vehicle.
  if (_transportSelectedVi >= 0) {
    _showTransportTip(_transportSelectedVi);
  } else if (ambulance3d.getSelected()) {
    // Selected ambulance does the same via ambulance3d.getScreenPos.
    _showAmbulanceTip(ambulance3d.getSelected());
  }
}
