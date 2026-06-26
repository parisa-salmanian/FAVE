// Global mutable state shared across all FAVE modules. Declared with `let`
// at top level so other classical scripts in the same Realm can read/write it.
// Loaded right after the constants in lib/config.js etc., before any module
// that references this state.

/* ======================= Global state ======================= */
let sourceMode = 'osm_s1';  // 's1' | 'osm_s1' (osm-API mode removed — see CLAUDE.md "Hard rule")
let viewMode   = 'all';  // 'all' | 'new'
let selectedYear = '';
let heightScale  = 3.2;
let lastCityName = 'Växjö';

let baseCityFC  = null;
let newbuildsFC = null;
let districtFC  = null;
let forbiddenZonesFC = null;   // water/natural-area polygons baked per city; null until loaded
// Växjö demand-weight state (population distributed proportionally by building floor area)
let vaxjoDistrictIndex  = null;   // [{feat, code}]
let vaxjoBuildingPopMap = null;   // Map<featureIndex, estimatedResidents>
let lantmaterietIndex = null; // [{centroid: [lng,lat], objekttyp: string}]

// Rich per-building DR features (modal-gap + built form + demo), baked offline by
// tools/bake_dr_features.py. Keyed by centroid "lon,lat" rounded to 5 decimals.
let cityDrFeatures      = null;   // Map<coordKey, {field: value}>
let drRichBuildingsJoined = false; // one-time join flag (reset on city change)

// SCB demographics + need-weighted fairness (revertible feature; off by default).
let cityDemographics    = null;   // Map<regsokod, {field: value}>  (from demographics_<city>.json)
let cityNeedByRegso     = null;   // Map<regsokod, needZ>  (composite deprivation, z-scored)
let vaxjoBuildingNeedMap = null;  // Map<featureIndex, needZ>  (building -> its district's needZ)
let needWeightEnabled   = false;  // UI toggle: fold demographic "need" into IF-City equity weight
let needWeightStrength  = 0.5;    // UI slider: 0 = off, higher = stronger need weighting

let firstFeat = null, secondFeat = null, routeGeoJSON = null;

// Categories to precompute for overall
var ALL_CATEGORIES = window.ALL_CATEGORIES || [
  'grocery',
  'hospital','pharmacy','dentistry','healthcare_center','veterinary',
  'university',
  'kindergarten','school_primary','school_high',
  ...(window.POI_EXTRA_KEYS || [])   // EpiCity-derived categories (poiCategories.js)
];
window.ALL_CATEGORIES = ALL_CATEGORIES;
const WHATIF_MOCK_TYPE_OPTIONS = ['residential', ...ALL_CATEGORIES];

// Fairness state
let fairActive = false;
let districtScoresSuppressed = false;
let fairCategory = '';           // single cat or 'mix'
let fairRecolorTick = 0;
let fairnessComputeGen = 0;  // incremented on clear; computations check this before writing results
let poiCache = {};               // per-category POI cache for current bbox
let currentPOIsFC = null;        // current marker POIs (single or union for mix)
let overallGini = null;
let currentCategoryGini = null;  // per-selection Gini; updated on every compute/clear
let fairnessTravelMode = FAIRNESS_TRAVEL_MODE_DEFAULT;
let fairnessModel = FAIRNESS_MODEL_DEFAULT;
let fairnessColorScheme = FAIRNESS_COLOR_SCHEME_DEFAULT;

// Multi-POI mix selection (from checklist)
let selectedPOIMix = [];         // [{cat, weight}, ...]

// What-if scenario state
let whatIfMode = 'off';          // 'off' | 'edit' | 'add'
let whatIfType = ALL_CATEGORIES[0] || 'grocery';
let lastFeatureClickAt = 0;
let whatIfSuggestions = [];
let whatIfSuggestionTick = 0;
let whatIfLastSuggestionConfig = null;
let whatIfMockBuildingsFC = null;
let whatIfMockTick = 0;

// === NEW: Best/Worst highlight state
let bw = { bldgBest:null, bldgWorst:null, districtBest:null, districtWorst:null, mode:null };
let bwTick = 0;

// remember latest summary for clickable jumps
let lastSummary = null;

// === What-if change history log
let whatIfChangeLog = [];
let changeLogIdCounter = 0;
let changeLogTick = 0;
let changeCompareBaseline = false; // when true, snapshot colors hidden = shows original state
let pinnedChangeId = null; // which change entry is currently shown on map

// === Global spinner state ===
let _globalSpinnerCount = 0;

function showGlobalSpinner(msg = 'Computing…') {
  _globalSpinnerCount++;
  const overlay = document.getElementById('globalSpinnerOverlay');
  const msgEl = document.getElementById('globalSpinnerMsg');
  if (msgEl) msgEl.textContent = msg;
  if (overlay) overlay.classList.remove('d-none');
}

/** Wait for the browser to actually paint the spinner before continuing. */
function waitForSpinnerPaint() {
  return new Promise(resolve => requestAnimationFrame(() => setTimeout(resolve, 0)));
}

function hideGlobalSpinner() {
  _globalSpinnerCount = Math.max(0, _globalSpinnerCount - 1);
  if (_globalSpinnerCount === 0) {
    const overlay = document.getElementById('globalSpinnerOverlay');
    if (overlay) overlay.classList.add('d-none');
  }
}

let drSelectionTick = 0;   // increments when DR selection changes
let drHasSelection = false;       // <— NEW: true when there is an active DR selection
let districtView   = false;       // toggle map between buildings vs. districts
let mezoView = false;
let districtScoreTick = 0;
let districtLoadPromise = null;
let districtLoadError = null;
let genderAgePopulation = null;
let genderAgePopulationPromise = null;
let districtPopulationPopup = null;
let lastPopulationContext = null;
let parallelCoordsOpen = false;
let parallelCoordsSelectionIds = new Set();
let parallelCoordsPending = false;
let parallelCoordsDistrictFilter = '';
let parallelCoordsBrushFilters = {};
let parallelCoordsBrushSelections = {};
let parallelCoordsForceEmptySelection = false;
let parallelCoordsColumnOrder = [];
let parallelCoordsMaxPoints = 0;
// PCP data-source: which axes to show — 'poi' (accessibility only),
// 'demo' (demographics only), or 'mixed' (both). See views/drView.js.
let parallelCoordsDataSourceMode = 'mixed';
const PARALLEL_COORDS_OVERALL_KEY = '__overall';
let additiveSelectionKeyActive = false;
let mezoHexData = [];
let mezoScoreTick = 0;
let mezoResolution = null;
let mezoMaskPolygon = null;
let districtLandClipSignature = '';
let activeDistrictURL = DEFAULT_DISTRICT_URL;
let activeDistrictCityKey = 'vaxjo';
let persistentBuildingSelection = new Set();

// Map lasso selection state
const mapLasso = {
  active: false,
  drawing: false,
  marqueeDrawing: false,
  points: [],
  marqueeStart: null,
  path: null,
  marqueeRect: null
};

const whatIfLasso = {
  active: false,
  drawing: false,
  marqueeDrawing: false,
  points: [],
  marqueeStart: null,
  path: null,
  marqueeRect: null,
  selectionRing: null
};

// UI refs
let sourceSelect, modeAllBtn, modeNewBtn, yearControlsWrap, distanceOut, heightScaleEl, heightScaleLabel;
let osmControls, cityInput, loadCityBtn, jobStatusEl, fairStatus, giniOut, overallGiniOut;
let modeAllLi, modeNewLi, heightControls, poiControls, districtToggleBtn;
let buildingTypeBtn, buildingTypeMenu;
let basemapStyleToggle;
let fairnessTravelModeSelect;
let fairnessModelSelect;
let fairnessColorSchemeSelect;
let fairnessSearchLevelSelect;
let fairnessSearchIncludeThresholdInput;
let fairnessSearchExcludeThresholdInput;
let fairnessSearchIncludeCatsSelect;
let fairnessSearchExcludeCatsSelect;
let fairnessSearchBtn;
let fairnessSearchClearBtn;
let fairnessSearchStatus;
let whatIfTypeSelect;
let whatIfSuggestBtn;
let whatIfUseBoundsToggle;
let whatIfRadiusInput;
let whatIfCountInput;
let whatIfModeSelect;
let whatIfSuggestCategoriesSelect;
let whatIfFairnessTargetSelect;
let whatIfAreaFocusSelect;
let whatIfLLMInput;
let whatIfApplySuggestionBtn;
let whatIfClearSuggestionBtn;
let whatIfVerifySuggestionBtn;
let whatIfSuggestionOut;
let whatIfMockTypeList;
let whatIfLassoBtn;
let whatIfLassoClearBtn;
let whatIfLassoStatus;
let selectedBuildingType = '';
let buildingTypeTick = 0;
let mezoToggleBtn;
let microToggleBtn;
let mezoEdgeOptionButtons = [];
let selectedMezoHexEdgeKm = DEFAULT_MEZO_HEX_EDGE_KM;

function syncSpatialToggleButtons() {
  if (districtToggleBtn) {
    districtToggleBtn.classList.toggle('btn-warning', districtView);
    districtToggleBtn.classList.toggle('btn-outline-light', !districtView);
    districtToggleBtn.setAttribute('aria-pressed', districtView ? 'true' : 'false');
    districtToggleBtn.title = districtView ? 'Hide district borders' : 'Toggle district borders';
  }

  if (mezoToggleBtn) {
    mezoToggleBtn.classList.toggle('btn-warning', mezoView);
    mezoToggleBtn.classList.toggle('btn-outline-light', !mezoView);
    mezoToggleBtn.setAttribute('aria-pressed', mezoView ? 'true' : 'false');
    mezoToggleBtn.title = mezoView ? 'Hide mezo hex layer' : 'Toggle mezo hex layer';
  }

  const microView = !districtView && !mezoView;
  if (microToggleBtn) {
    microToggleBtn.classList.toggle('btn-warning', microView);
    microToggleBtn.classList.toggle('btn-outline-light', !microView);
    microToggleBtn.setAttribute('aria-pressed', microView ? 'true' : 'false');
  }
}

function setMezoHexEdgeKm(value) {
  const next = Number.parseFloat(value);
  if (!MEZO_HEX_EDGE_OPTIONS_KM.includes(next) || next === selectedMezoHexEdgeKm) return;
  selectedMezoHexEdgeKm = next;
  mezoResolution = null;
  if (mezoView) {
    refreshMezoScores()
      .then(() => {
        updateLayers();
        // Push the new mezoHexData length into the top metric strip so the
        // "Cells" count tracks the chosen hex size.
        window.faveInspector?.refresh?.();
      })
      .catch(() => {
        updateLayers();
        window.faveInspector?.refresh?.();
      });
    return;
  }
  updateLayers();
  window.faveInspector?.refresh?.();
}

function setFairnessSearchStatus(msg, isError = false) {
  if (!fairnessSearchStatus) return;
  fairnessSearchStatus.textContent = msg || '';
  fairnessSearchStatus.classList.toggle('text-danger', !!isError);
  fairnessSearchStatus.classList.toggle('text-muted', !isError);
}

function getMultiSelectValues(selectEl) {
  if (!selectEl) return [];
  return Array.from(selectEl.selectedOptions || [])
    .map((opt) => (opt.value || '').trim())
    .filter(Boolean);
}

function fairnessEntityId(prefix, idx, fallback = '') {
  const raw = String(fallback || '').trim();
  return raw || `${prefix}-${idx + 1}`;
}

function populateFairnessSearchCategoryOptions() {
  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const makeOptions = () => catList
    .map((cat) => `<option value="${cat}">${prettyPOIName(cat)}</option>`)
    .join('');
  if (fairnessSearchIncludeCatsSelect) fairnessSearchIncludeCatsSelect.innerHTML = makeOptions();
  if (fairnessSearchExcludeCatsSelect) fairnessSearchExcludeCatsSelect.innerHTML = makeOptions();
}

function getMissingFairnessCategories(categories) {
  const neededCats = (categories || []).filter(Boolean);
  if (!baseCityFC?.features?.length) return neededCats;
  const missing = new Set();
  for (const f of (baseCityFC.features || [])) {
    const fm = f?.properties?.fair_multi || {};
    neededCats.forEach((cat) => {
      if (!Number.isFinite(fm?.[cat]?.score)) missing.add(cat);
    });
    if (missing.size === neededCats.length) break;
  }
  return Array.from(missing);
}

async function ensureFairnessSearchData(level, categories) {
  const neededCats = (categories || []).filter(Boolean);
  if (!baseCityFC?.features?.length) throw new Error('No buildings loaded yet.');

  const missingBefore = getMissingFairnessCategories(neededCats);

  if (missingBefore.length) {
    setFairnessSearchStatus('Computing fairness for selected POIs…');
    await computeOverallFairness(neededCats);

    const missingAfter = getMissingFairnessCategories(neededCats);
    if (missingAfter.length) {
      throw new Error(
        `Could not fetch enough POI data for: ${missingAfter.map(prettyPOIName).join(', ')}. ` +
        'Please retry in a few seconds.'
      );
    }
  }

  if (level === 'macro') {
    await ensureDistrictData();
    await refreshDistrictScores();
  } else if (level === 'mezo') {
    await refreshMezoScores();
  }
}

function buildFairnessSearchEntities(level) {
  if (level === 'macro') {
    const features = districtFC?.features || [];
    return features.map((feat, idx) => {
      const props = feat?.properties || {};
      return {
        id: fairnessEntityId('district', idx, props?.regso || props?.deso || props?.name || props?.__districtName),
        name: districtNameOf(props, idx),
        level: 'macro',
        fairness_by_poi: props.__fairByCat || {},
        entityRef: feat
      };
    });
  }

  if (level === 'mezo') {
    const cells = Array.isArray(mezoHexData) ? mezoHexData : [];
    return cells.map((cell, idx) => ({
      id: fairnessEntityId('mezo', idx, cell?.hex),
      name: cell?.hex || `Mezo ${idx + 1}`,
      level: 'mezo',
      fairness_by_poi: cell?.__fairByCat || {},
      entityRef: cell
    }));
  }

  const buildings = baseCityFC?.features || [];
  return buildings.map((feat, idx) => {
    const props = feat?.properties || {};
    const fairByPoi = {};
    const fm = props?.fair_multi || {};
    Object.keys(fm).forEach((cat) => {
      const score = fm?.[cat]?.score;
      if (Number.isFinite(score)) fairByPoi[cat] = score;
    });
    return {
      id: fairnessEntityId('building', idx, props?.id || props?.osm_id || props?.byggnadsid),
      name: props?.name || props?.objekttyp || `Building ${idx + 1}`,
      level: 'building',
      fairness_by_poi: fairByPoi,
      entityRef: feat
    };
  });
}

function localFairnessSearch(entities, includeCats, excludeCats, includeThreshold, excludeThreshold) {
  const matches = [];
  const fairnessSeed = Number(globalThis.FAIRNESS_SEARCH_SEED) || 202503;
  (entities || []).forEach((entity) => {
    const scoreMap = entity?.fairness_by_poi || {};
    const includePass = includeCats.every((cat) => Number(scoreMap?.[cat] ?? 0) >= includeThreshold);
    const excludePass = excludeCats.every((cat) => Number(scoreMap?.[cat] ?? 0) <= excludeThreshold);
    if (!includePass || !excludePass) return;

    const includeAvg = includeCats.length
      ? includeCats.reduce((sum, cat) => sum + Number(scoreMap?.[cat] ?? 0), 0) / includeCats.length
      : null;
    const excludeAvg = excludeCats.length
      ? excludeCats.reduce((sum, cat) => sum + Number(scoreMap?.[cat] ?? 0), 0) / excludeCats.length
      : null;
    const contrast = Number.isFinite(includeAvg) && Number.isFinite(excludeAvg)
      ? includeAvg - excludeAvg
      : includeAvg;

    const tieBreak = stableHashString(`${fairnessSeed}:${entity?.id || entity?.name || ''}`);
    matches.push({ ...entity, includeAvg, excludeAvg, contrast, tieBreak });
  });

  matches.sort((a, b) => {
    const ca = Number.isFinite(a.contrast) ? a.contrast : -Infinity;
    const cb = Number.isFinite(b.contrast) ? b.contrast : -Infinity;
    if (cb !== ca) return cb - ca;
    const ia = Number.isFinite(a.includeAvg) ? a.includeAvg : -Infinity;
    const ib = Number.isFinite(b.includeAvg) ? b.includeAvg : -Infinity;
    if (ib !== ia) return ib - ia;
    return (a.tieBreak ?? 0) - (b.tieBreak ?? 0);
  });

  return matches;
}

async function runFairnessSearchFromUI() {
  try {
    const level = fairnessSearchLevelSelect?.value || 'building';
    const includeCats = getMultiSelectValues(fairnessSearchIncludeCatsSelect);
    const excludeCats = getMultiSelectValues(fairnessSearchExcludeCatsSelect);
    const includeThreshold = Number(fairnessSearchIncludeThresholdInput?.value ?? 0.65);
    const excludeThreshold = Number(fairnessSearchExcludeThresholdInput?.value ?? 0.35);

    if (!includeCats.length) {
      setFairnessSearchStatus('Please choose at least one "fair with" POI category.', true);
      return;
    }
    if (!Number.isFinite(includeThreshold) || includeThreshold < 0 || includeThreshold > 1) {
      setFairnessSearchStatus('Fair threshold must be between 0 and 1.', true);
      return;
    }
    if (!Number.isFinite(excludeThreshold) || excludeThreshold < 0 || excludeThreshold > 1) {
      setFairnessSearchStatus('Not-fair threshold must be between 0 and 1.', true);
      return;
    }

    await ensureFairnessSearchData(level, [...includeCats, ...excludeCats]);

    const entities = buildFairnessSearchEntities(level);
    const matches = localFairnessSearch(entities, includeCats, excludeCats, includeThreshold, excludeThreshold);

    if (!matches.length) {
      clearMapSelection();
      setFairnessSearchStatus('No areas match these constraints. Try softer thresholds.', true);
      return;
    }

    const toSelect = matches.slice(0, 200).map((m) => m.entityRef).filter(Boolean);
    applyMapSelection(toSelect);

    const first = matches[0]?.entityRef;
    if (first?.geometry) {
      try {
        const c = turf.centroid(first).geometry.coordinates;
        if (Array.isArray(c)) map?.flyTo({ center: c, zoom: Math.max(map.getZoom(), 12), duration: 900 });
      } catch (_) {
        /* ignore fly errors */
      }
    }

    const label = level === 'macro' ? 'districts' : (level === 'mezo' ? 'mezo areas' : 'buildings');
    const top = matches[0];
    setFairnessSearchStatus(
      `Found ${matches.length} ${label}. Top: ${top?.name || top?.id} (fair ${Math.round((top?.includeAvg || 0) * 100)}%, not-fair ${Math.round((top?.excludeAvg || 0) * 100)}%).`
    );
  } catch (err) {
    console.error('Fairness search failed', err);
    setFairnessSearchStatus(`Search failed: ${err?.message || err}`, true);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  map = new maplibregl.Map({
    container: 'mapContainer',
    style: LIGHT_BASEMAP_STYLE,
    center: [14.805, 56.879],
    zoom: 15,
    pitch: 45,
    attributionControl: false
  });
  map.doubleClickZoom.disable();

  map.on('load', () => {
    console.warn('>>> map load event fired at', Date.now());
    overlay = new deck.MapboxOverlay({ interleaved: true, layers: [] });
    map.addControl(overlay);
    map.on('zoomend', () => updateLayers());
    map.on('style.load', () => {
      console.warn('>>> style.load event fired at', Date.now());
      updateLayers();
    });
    map.on('dblclick', handleDistrictDoubleClick);
    map.on('click', handleWhatIfMapClick);

    wireUI();
    initDRUI();
    wireMapLassoUI();
    toggleLocalOnlyUI(false);
    osmControls?.classList.remove('d-none-important');
    wireLLMExplainUI();
    ensureDistrictData()
      .then(() => { districtBoundaryFC = null; updateLayers(); })
      .catch(() => {});

    const initCityKey = document.getElementById('citySelect')?.value || 'vaxjo';
    const initDisplay = LOCAL_CITY_NAMES[initCityKey] || 'Växjö';
    lastCityName = initDisplay;

    // Show feedback for the initial load too, same as user-driven city changes.
    showGlobalSpinner(`Loading ${initDisplay}…`);
    const cityEl = document.getElementById('citySelect');
    if (cityEl) cityEl.disabled = true;
    const jobEl = document.getElementById('jobStatus');
    if (jobEl) jobEl.textContent = `Loading ${initDisplay}…`;

    const initialLoad = (sourceMode === 'osm_s1')
      ? loadCityLocal(initCityKey)
      : loadCityOSM(initDisplay);

    initialLoad
      .then(autoComputeOverall)
      .then(() => {
        // All POI checkboxes ship checked in the HTML so the user gets
        // colour on the map immediately. autoComputeOverall populates
        // fair_overall (for the metric strip) but leaves fairActive=false,
        // so we additionally fire the POI change handler — that runs the
        // weighted-mix compute, sets fairActive=true and triggers the
        // building recolour.
        const checkedPOI = document.querySelector('.poi-check:checked');
        if (checkedPOI) {
          checkedPOI.dispatchEvent(new Event('change', { bubbles: true }));
        }
      })
      .then(() => { lastCityKeyLoaded = initCityKey; })
      .catch(console.error)
      .finally(() => {
        if (cityEl) cityEl.disabled = false;
        hideGlobalSpinner();
      });
  });
});


function extractBuildingTypesList() {
  if (!baseCityFC?.features?.length) return [];
  const uniq = new Set();
  for (const f of baseCityFC.features) {
    const t = buildingTypeOf(f);
    if (t) uniq.add(String(t));
  }
  return BUILDING_TYPE_ORDER.filter(t => uniq.has(t));
}

function refreshBuildingTypeDropdown() {
  if (!buildingTypeMenu || !buildingTypeBtn) return;

  const types = extractBuildingTypesList();
  buildingTypeMenu.innerHTML = '';

  if (!types.length) {
    buildingTypeBtn.classList.add('disabled');
    buildingTypeBtn.textContent = 'Building types';
    return;
  }

  buildingTypeBtn.classList.remove('disabled');

  const frag = document.createDocumentFragment();
  const addItem = (label, type) => {
    const li = document.createElement('li');
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'dropdown-item building-type-item';
    btn.dataset.type = type;
    btn.textContent = label;
    li.appendChild(btn);
    frag.appendChild(li);
  };

  addItem('Show all types', '');
  types.forEach(t => addItem(t, t));

  buildingTypeMenu.appendChild(frag);
}

function refreshWhatIfTypeSelect() {
  if (!whatIfTypeSelect) return;
  whatIfTypeSelect.innerHTML = buildWhatIfTypeOptions(whatIfType);
  whatIfTypeSelect.value = whatIfType;
}

function refreshWhatIfSuggestCategories(selected = []) {
  if (!whatIfSuggestCategoriesSelect) return;
  const initial = selected.length ? selected : [whatIfType];
  whatIfSuggestCategoriesSelect.innerHTML = buildWhatIfSuggestCategoryOptions(initial);
}

function getSelectedWhatIfCategories() {
  if (!whatIfSuggestCategoriesSelect) return [];
  return Array.from(whatIfSuggestCategoriesSelect.selectedOptions || [])
    .map(option => option.value)
    .filter(Boolean);
}

function setSelectedBuildingType(type, skipUpdate = false) {
  selectedBuildingType = type || '';
  buildingTypeTick++;
  if (buildingTypeBtn) {
    buildingTypeBtn.textContent = selectedBuildingType ? `Type: ${selectedBuildingType}` : 'Building types';
  }
  closePopup();
  if (!skipUpdate) updateLayers();
}

function featureMatchesSelectedType(f) {
  if (!selectedBuildingType) return true;
  return buildingTypeOf(f) === selectedBuildingType;
}

function setDistrictView(on) {
  const prevMode = currentDRDataMode();
  districtView = !!on;
  if (districtView && mezoView) {
    mezoView = false;
  }
  syncSpatialToggleButtons();
  updateDRAndPCBadges();
  // Update the top metric strip's units label/count to follow the active scale.
  window.faveInspector?.refresh?.();

  if (districtView) {
    ensureDistrictData()
      .then(() => refreshDistrictScores())
      .then(() => updateLayers())
      .catch(() => updateLayers())
      .finally(() => {
        updateParallelCoordsPanel();
        maybeRefreshDROnSpatialModeChange(prevMode);
      });
  } else {
    updateLayers();
    updateParallelCoordsPanel();
    maybeRefreshDROnSpatialModeChange(prevMode);
  }
}

function setMezoView(on) {
  const prevMode = currentDRDataMode();
  mezoView = !!on;
  syncSpatialToggleButtons();
  updateDRAndPCBadges();
  if (mezoView && districtView) setDistrictView(false);
  window.faveInspector?.refresh?.();
  if (mezoView) {
    refreshMezoScores()
      .then(() => updateLayers())
      .catch(() => updateLayers())
      .finally(() => {
        updateParallelCoordsPanel();
        maybeRefreshDROnSpatialModeChange(prevMode);
      });
  } else {
    updateLayers();
    updateParallelCoordsPanel();
    maybeRefreshDROnSpatialModeChange(prevMode);
  }
}

// Session-scoped city cache: avoid re-fetching/re-parsing/re-computing when
// the user switches back to a city already loaded this session. Cleared on
// full page reload.
const sessionCityCache = new Map();
let lastCityKeyLoaded = null;

function snapshotCity() {
  return {
    baseCityFC, newbuildsFC,
    districtFC, districtBoundaryFC,
    activeDistrictURL, activeDistrictCityKey,
    mezoMaskPolygon, mezoHexData, districtLandClipSignature,
    vaxjoDistrictIndex, vaxjoBuildingPopMap, lantmaterietIndex,
    poiCache: { ...poiCache },
    currentPOIsFC,
    selectedPOIMix: selectedPOIMix.map(e => ({ ...e })),
    fairActive, fairCategory,
    overallGini,
    selectedPOIId, selectedPOIFeature,
  };
}

function restoreCity(snap) {
  baseCityFC                = snap.baseCityFC;
  newbuildsFC               = snap.newbuildsFC;
  districtFC                = snap.districtFC;
  districtBoundaryFC        = snap.districtBoundaryFC;
  activeDistrictURL         = snap.activeDistrictURL;
  activeDistrictCityKey     = snap.activeDistrictCityKey;
  mezoMaskPolygon           = snap.mezoMaskPolygon;
  mezoHexData               = snap.mezoHexData;
  districtLandClipSignature = snap.districtLandClipSignature;
  vaxjoDistrictIndex        = snap.vaxjoDistrictIndex;
  vaxjoBuildingPopMap       = snap.vaxjoBuildingPopMap;
  lantmaterietIndex         = snap.lantmaterietIndex;
  poiCache                  = { ...snap.poiCache };
  currentPOIsFC             = snap.currentPOIsFC;
  selectedPOIMix            = snap.selectedPOIMix.map(e => ({ ...e }));
  fairActive                = snap.fairActive;
  fairCategory              = snap.fairCategory;
  fairRecolorTick++;
  overallGini               = snap.overallGini;
  selectedPOIId             = snap.selectedPOIId;
  selectedPOIFeature        = snap.selectedPOIFeature;

  if (overallGiniOut) {
    overallGiniOut.textContent = (typeof formatFairnessBadgeValue === 'function')
      ? formatFairnessBadgeValue(overallGini)
      : (Number.isFinite(overallGini) ? overallGini.toFixed(3) : '—');
  }
  document.querySelectorAll('.poi-check').forEach(chk => {
    const cat = chk.getAttribute('data-cat');
    chk.checked = selectedPOIMix.some(e => e.cat === cat);
  });
  document.querySelectorAll('.poi-weight').forEach(el => {
    const cat = el.getAttribute('data-cat');
    const entry = selectedPOIMix.find(e => e.cat === cat);
    if (entry && Number.isFinite(entry.weight)) el.value = entry.weight;
    const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
    if (badge) badge.textContent = el.value;
  });
  // Programmatic chk.checked = ... above doesn't fire 'change', so the
  // shell's rail-poi listener wouldn't catch this restore on its own.
  window.syncPOIRailActive?.();

  refreshBuildingTypeDropdown?.();
  fitToData?.(baseCityFC);
  updateLayers?.();
  // Only re-aggregate district/mezo scores if those views are actually on —
  // refreshDistrictScores walks every building polygon, which is exactly the
  // kind of work the cache is supposed to skip.
  if (districtView && typeof refreshDistrictScores === 'function') refreshDistrictScores();
  if (mezoView && typeof refreshMezoScores === 'function') refreshMezoScores();
}

function wireUI() {
  // UI binds
  sourceSelect     = document.getElementById('sourceSelect');
  modeAllBtn       = document.getElementById('modeAll');
  modeNewBtn       = document.getElementById('modeNew');
  modeAllLi        = document.getElementById('modeAllLi');
  modeNewLi        = document.getElementById('modeNewLi');
  heightControls   = document.getElementById('heightControls');
  yearControlsWrap = document.getElementById('yearControls');
  const yearFilterEl = document.getElementById('yearFilter');
  heightScaleEl    = document.getElementById('heightScale');
  heightScaleLabel = document.getElementById('heightScaleLabel');
  distanceOut      = document.getElementById('distanceResult');
  osmControls      = document.getElementById('osmControls');
  cityInput        = document.getElementById('cityInput');
  loadCityBtn      = document.getElementById('loadCityBtn');
  jobStatusEl      = document.getElementById('jobStatus');
  fairStatus       = document.getElementById('fairStatus');
  giniOut          = document.getElementById('giniOut');
  overallGiniOut   = document.getElementById('overallGiniOut');
  fairnessTravelModeSelect = document.getElementById('fairnessTravelMode');
  fairnessModelSelect = document.getElementById('fairnessModelMode');
  fairnessColorSchemeSelect = document.getElementById('fairnessColorScheme');
  fairnessSearchLevelSelect = document.getElementById('fairnessSearchLevel');
  fairnessSearchIncludeThresholdInput = document.getElementById('fairnessSearchIncludeThreshold');
  fairnessSearchExcludeThresholdInput = document.getElementById('fairnessSearchExcludeThreshold');
  fairnessSearchIncludeCatsSelect = document.getElementById('fairnessSearchIncludeCats');
  fairnessSearchExcludeCatsSelect = document.getElementById('fairnessSearchExcludeCats');
  fairnessSearchBtn = document.getElementById('fairnessSearchBtn');
  fairnessSearchClearBtn = document.getElementById('fairnessSearchClearBtn');
  fairnessSearchStatus = document.getElementById('fairnessSearchStatus');
  poiControls      = document.getElementById('poiControls');
  buildingTypeBtn  = document.getElementById('buildingTypeDropdownBtn');
  buildingTypeMenu = document.getElementById('buildingTypeDropdownMenu');
  districtToggleBtn = document.getElementById('districtToggleBtn');
  mezoToggleBtn = document.getElementById('mezoToggleBtn');
  microToggleBtn = document.getElementById('microToggleBtn');
  mezoEdgeOptionButtons = Array.from(document.querySelectorAll('.mezo-edge-option'));
  basemapStyleToggle = document.getElementById('basemapStyleToggle');
  whatIfTypeSelect = document.getElementById('whatIfTypeSelect');
  whatIfSuggestBtn = document.getElementById('whatIfSuggestBtn');
  whatIfUseBoundsToggle = document.getElementById('whatIfUseBoundsToggle');
  whatIfRadiusInput = document.getElementById('whatIfRadiusKm');
  whatIfCountInput = document.getElementById('whatIfSuggestCount');
  whatIfModeSelect = document.getElementById('whatIfSuggestMode');
  whatIfSuggestCategoriesSelect = document.getElementById('whatIfSuggestCategories');
  whatIfFairnessTargetSelect = document.getElementById('whatIfFairnessTarget');
  whatIfAreaFocusSelect = document.getElementById('whatIfAreaFocus');
  whatIfLLMInput = document.getElementById('whatIfLLMInput');
  whatIfApplySuggestionBtn = document.getElementById('whatIfApplySuggestionBtn');
  whatIfClearSuggestionBtn = document.getElementById('whatIfClearSuggestionBtn');
  whatIfVerifySuggestionBtn = document.getElementById('whatIfVerifySuggestionBtn');
  whatIfSuggestionOut = document.getElementById('whatIfSuggestionOut');
  whatIfMockTypeList = document.getElementById('whatIfMockTypeList');
  whatIfLassoBtn = document.getElementById('whatIfLassoBtn');
  whatIfLassoClearBtn = document.getElementById('whatIfLassoClearBtn');
  whatIfLassoStatus = document.getElementById('whatIfLassoStatus');
  const districtPopulationBtn = document.getElementById('districtPopulationBtn');
  const districtPopulationClose = document.getElementById('districtPopulationClose');
  const parallelCoordsBtn = document.getElementById('parallelCoordsBtn');
  const parallelCoordsClose = document.getElementById('parallelCoordsClose');
  const parallelCoordsMaxPtsInput = document.getElementById('parallelCoordsMaxPts');
  const drOffcanvas = document.getElementById('drOffcanvas');
  const whatIfDropdownBtn = document.getElementById('whatIfDropdownBtn');

  // Symbols toggle in the dropdown footer (declare ONCE)
  const poiSymbolsToggle = document.getElementById('poiSymbolsToggle');

  const sidePanelClose = document.getElementById('sidePanelClose');
  sidePanelClose?.addEventListener('click', () => hideSidePanel());
  districtPopulationBtn?.addEventListener('click', () => toggleDistrictPopulationPanel());
  districtPopulationClose?.addEventListener('click', () => hideDistrictPopulationPanel());
  parallelCoordsBtn?.addEventListener('click', () => toggleParallelCoordsPanel());
  parallelCoordsClose?.addEventListener('click', () => hideParallelCoordsPanel());
  if (parallelCoordsMaxPtsInput) {
    parallelCoordsMaxPtsInput.value = String(parallelCoordsMaxPoints);
    const applyMaxPoints = () => {
      const rawVal = parseInt(parallelCoordsMaxPtsInput.value || '0', 10);
      parallelCoordsMaxPoints = (rawVal <= 0 || !Number.isFinite(rawVal)) ? 0 : Math.max(200, rawVal);
      parallelCoordsMaxPtsInput.value = String(parallelCoordsMaxPoints);
      if (parallelCoordsOpen) updateParallelCoordsPanel();
    };
    parallelCoordsMaxPtsInput.addEventListener('change', applyMaxPoints);
  }
  // Max button — load every available row in the current PC mode (no
  // sampling). Sets the value to the total count for the current spatial
  // mode (mezos / districts / buildings) so updateDRAndPCBadges treats
  // it as a real explicit cap and skips the 4 000 default.
  const parallelCoordsMaxBtn = document.getElementById('parallelCoordsMaxBtn');
  parallelCoordsMaxBtn?.addEventListener('click', () => {
    let total = 0;
    try {
      const mode = (typeof currentDRDataMode === 'function') ? currentDRDataMode() : 'building';
      if (mode === 'mezo') total = (mezoHexData || []).length;
      else if (mode === 'district') total = (districtFC?.features || []).length;
      else total = (baseCityFC?.features || []).length;
    } catch (_) { total = 0; }
    if (!Number.isFinite(total) || total <= 0) total = 100000;
    parallelCoordsMaxPoints = total;
    if (parallelCoordsMaxPtsInput) parallelCoordsMaxPtsInput.value = String(total);
    if (parallelCoordsOpen && typeof updateParallelCoordsPanel === 'function') {
      updateParallelCoordsPanel();
    }
  });
  mezoToggleBtn?.addEventListener('click', (event) => {
    if (event.target?.closest('.dropdown-menu')) return;
    setMezoView(!mezoView);
  });
  microToggleBtn?.addEventListener('click', (event) => {
    event.preventDefault();
    if (mezoView) {
      setMezoView(false);
      return;
    }
    if (districtView) {
      setDistrictView(false);
    }
  });
  mezoEdgeOptionButtons.forEach((btn) => {
    btn.classList.toggle('active', String(selectedMezoHexEdgeKm) === String(btn.dataset.value || ''));
    btn.addEventListener('click', () => {
      const next = btn.dataset.value;
      setMezoHexEdgeKm(next);
      mezoEdgeOptionButtons.forEach((item) => item.classList.toggle('active', item === btn));
      setMezoView(true);
    });
  });
  drOffcanvas?.addEventListener('shown.bs.offcanvas', () => updateParallelCoordsOffset());
  drOffcanvas?.addEventListener('hidden.bs.offcanvas', () => updateParallelCoordsOffset());
  whatIfDropdownBtn?.addEventListener('shown.bs.dropdown', () => updateParallelCoordsOffset());
  whatIfDropdownBtn?.addEventListener('hidden.bs.dropdown', () => updateParallelCoordsOffset());
  window.addEventListener('resize', () => updateParallelCoordsOffset());

  const citySelect = document.getElementById('citySelect');
  if (citySelect) citySelect.value = 'vaxjo';
  updateDRAndPCBadges();

  if (fairnessTravelModeSelect) {
    fairnessTravelModeSelect.value = fairnessTravelMode;
    fairnessTravelModeSelect.addEventListener('change', async () => {
      const prevMode = fairnessTravelMode;
      const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
      const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
      const scoreSnapshot = captureScoreSnapshot();
      fairnessTravelMode = normalizeTravelMode(fairnessTravelModeSelect.value);
      showGlobalSpinner('Switching travel mode…');
      await waitForSpinnerPaint();
      try {
        const result = await recomputeFairnessAfterWhatIf();
        const afterCatGini = Number.isFinite(result?.categoryGini) ? result.categoryGini : extractDisplayedGiniValue(giniOut?.textContent || '');
        const afterOverallGini = Number.isFinite(result?.overallGini) ? result.overallGini : (Number.isFinite(overallGini) ? overallGini : null);
        const nextId = changeLogIdCounter;
        const colorPairs = applyDeltaColorsFromSnapshot(scoreSnapshot, nextId);
        recordWhatIfChange({
          action: 'travel_mode',
          description: `Travel mode: ${prevMode} → ${fairnessTravelMode}`,
          category: fairCategory || null,
          beforeGini: beforeCatGini,
          afterGini: afterCatGini,
          beforeOverall: beforeOverallGini,
          afterOverall: afterOverallGini,
          affectedFeatures: colorPairs
        });
      } catch (err) {
        console.error('travel_mode record failed', err);
      } finally {
        hideGlobalSpinner();
        // Mode-dependent companion metric (2SFCA) in the open inspector must
        // follow the new mode immediately, not wait for the next map hover.
        if (window.faveInspector && typeof window.faveInspector.notifyTravelModeChanged === 'function') {
          window.faveInspector.notifyTravelModeChanged();
        }
        // The Supply provision map coloring is per-mode too — re-stamp it if it's
        // the active map color variable (no-op on the Fairness tab).
        if (typeof notifySupplyDataChanged === 'function') notifySupplyDataChanged();
      }
    });
  }

  if (fairnessModelSelect) {
    fairnessModelSelect.value = fairnessModel;
    fairnessModelSelect.addEventListener('change', async () => {
      const prevModel = fairnessModel;
      const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
      const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
      const scoreSnapshot = captureScoreSnapshot();
      fairnessModel = normalizeFairnessModel(fairnessModelSelect.value);
      showGlobalSpinner('Switching access model…');
      await waitForSpinnerPaint();
      try {
        const result = await recomputeFairnessAfterWhatIf();
        const afterCatGini = Number.isFinite(result?.categoryGini) ? result.categoryGini : extractDisplayedGiniValue(giniOut?.textContent || '');
        const afterOverallGini = Number.isFinite(result?.overallGini) ? result.overallGini : (Number.isFinite(overallGini) ? overallGini : null);
        const nextId = changeLogIdCounter;
        const colorPairs = applyDeltaColorsFromSnapshot(scoreSnapshot, nextId);
        recordWhatIfChange({
          action: 'access_model',
          description: `Access model: ${prevModel} → ${fairnessModel}`,
          category: fairCategory || null,
          beforeGini: beforeCatGini,
          afterGini: afterCatGini,
          beforeOverall: beforeOverallGini,
          afterOverall: afterOverallGini,
          affectedFeatures: colorPairs
        });
      } catch (err) {
        console.error('access_model record failed', err);
      } finally {
        hideGlobalSpinner();
      }
    });
  }

  if (fairnessColorSchemeSelect) {
    fairnessColorSchemeSelect.value = fairnessColorScheme;
    fairnessColorSchemeSelect.addEventListener('change', () => {
      setFairnessColorScheme(fairnessColorSchemeSelect.value, { refreshLayers: true });
    });
  }

  // Need-weighted fairness toggle + strength (SCB demographics → equity weight).
  const needToggleEl = document.getElementById('needWeightToggle');
  const needStrengthEl = document.getElementById('needWeightStrength');
  const needStrengthValEl = document.getElementById('needWeightStrengthVal');
  const runNeedRecompute = async (desc) => {
    if (typeof recomputeFairnessAfterWhatIf !== 'function') return;
    // Make sure demographics (and thus the need index) are loaded before recompute.
    if (needWeightEnabled && typeof ensureDemographicsData === 'function') {
      await ensureDemographicsData(activeDistrictCityKey).catch(() => null);
    }
    const scoreSnapshot = (typeof captureScoreSnapshot === 'function') ? captureScoreSnapshot() : null;
    showGlobalSpinner('Recomputing fairness…');
    await waitForSpinnerPaint();
    try {
      await recomputeFairnessAfterWhatIf();
      if (scoreSnapshot && typeof applyDeltaColorsFromSnapshot === 'function') {
        applyDeltaColorsFromSnapshot(scoreSnapshot, changeLogIdCounter);
      }
    } catch (err) {
      console.error('need-weight recompute failed', err);
    } finally {
      hideGlobalSpinner();
    }
  };
  if (needToggleEl) {
    needToggleEl.checked = !!needWeightEnabled;
    if (needStrengthEl) needStrengthEl.disabled = !needWeightEnabled;
    needToggleEl.addEventListener('change', async () => {
      needWeightEnabled = needToggleEl.checked;
      if (needStrengthEl) needStrengthEl.disabled = !needWeightEnabled;
      await runNeedRecompute();
    });
  }
  if (needStrengthEl) {
    needStrengthEl.value = String(needWeightStrength);
    if (needStrengthValEl) needStrengthValEl.textContent = String(needWeightStrength);
    needStrengthEl.addEventListener('input', () => {
      needWeightStrength = parseFloat(needStrengthEl.value) || 0;
      if (needStrengthValEl) needStrengthValEl.textContent = needStrengthEl.value;
    });
    needStrengthEl.addEventListener('change', async () => {
      needWeightStrength = parseFloat(needStrengthEl.value) || 0;
      if (needWeightEnabled) await runNeedRecompute();
    });
  }

  updateFairnessLegendUI();

  document.getElementById('changesReplayBtn')?.addEventListener('click', () => startTransitionReplay());

  populateFairnessSearchCategoryOptions();
  fairnessSearchBtn?.addEventListener('click', () => {
    runFairnessSearchFromUI();
  });
  fairnessSearchClearBtn?.addEventListener('click', () => {
    clearMapSelection();
    setFairnessSearchStatus('Selection cleared. Pick categories, then click “Find areas”.');
  });


  const setBasemapStyle = (isDark) => {
    const nextStyle = isDark ? DARK_BASEMAP_STYLE : LIGHT_BASEMAP_STYLE;
    if (currentBasemapStyle === nextStyle) return;
    currentBasemapStyle = nextStyle;
    map?.setStyle(nextStyle);
  };

  const updateBasemapLabel = () => {
    const label = document.querySelector('label[for="basemapStyleToggle"]');
    if (!label || !basemapStyleToggle) return;
    label.textContent = basemapStyleToggle.checked ? 'Dark' : 'Light';
  };

  basemapStyleToggle?.addEventListener('change', () => {
    setBasemapStyle(basemapStyleToggle.checked);
    updateBasemapLabel();
  });

  updateBasemapLabel();
  syncSpatialToggleButtons();

  buildingTypeMenu?.addEventListener('click', (e) => {
    const btn = e.target.closest('.building-type-item');
    if (!btn) return;
    e.preventDefault();
    setSelectedBuildingType(btn.dataset.type || '');
  });

  refreshBuildingTypeDropdown();
  refreshWhatIfTypeSelect();
  refreshWhatIfSuggestCategories();
  buildWhatIfMockTypeInputs();
  // Wire mock building sliders
  const fpMinEl = document.getElementById('whatIfFootprintMin');
  const fpMaxEl = document.getElementById('whatIfFootprintMax');
  const fpLabel = document.getElementById('whatIfFootprintLabel');
  const floorCountEl = document.getElementById('whatIfFloorCount');
  const floorCountLabel = document.getElementById('whatIfFloorCountLabel');
  const floorHeightEl = document.getElementById('whatIfFloorHeight');
  const floorHeightLabel = document.getElementById('whatIfFloorHeightLabel');

  function syncFootprintLabel() {
    const mnArea = Number(fpMinEl?.value || 60);
    const mxArea = Number(fpMaxEl?.value || 150);
    const minArea = Math.min(mnArea, mxArea);
    const maxArea = Math.max(mnArea, mxArea);
    // Convert area (m²) → side length (m): side ≈ sqrt(area / 0.85)
    // The 0.85 accounts for the non-square aspect ratio in createMockBuildingPolygon
    whatIfMockFootprintMin = Math.round(Math.sqrt(minArea / 0.85));
    whatIfMockFootprintMax = Math.round(Math.sqrt(maxArea / 0.85));
    if (fpLabel) fpLabel.textContent = `${minArea}–${maxArea} m²`;
  }
  function syncFloorLabels() {
    whatIfMockFloors = Number(floorCountEl?.value || 3);
    whatIfMockFloorHeight = Number(floorHeightEl?.value || 3);
    if (floorCountLabel) floorCountLabel.textContent = String(whatIfMockFloors);
    if (floorHeightLabel) floorHeightLabel.textContent = `${whatIfMockFloorHeight.toFixed(1)} m`;
  }

  fpMinEl?.addEventListener('input', syncFootprintLabel);
  fpMaxEl?.addEventListener('input', syncFootprintLabel);
  floorCountEl?.addEventListener('input', syncFloorLabels);
  floorHeightEl?.addEventListener('input', syncFloorLabels);
  syncFootprintLabel();
  syncFloorLabels();
  const shapeVarEl = document.getElementById('whatIfShapeVariation');
  const shapeVarLabel = document.getElementById('whatIfShapeVariationLabel');
  function syncShapeVariation() {
    whatIfMockShapeVariation = Number(shapeVarEl?.value ?? 50);
    if (shapeVarLabel) shapeVarLabel.textContent = `${whatIfMockShapeVariation}%`;
  }
  shapeVarEl?.addEventListener('input', syncShapeVariation);
  syncShapeVariation();

  const keepOpen = (handler) => (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
    handler(ev);
  };

  const whatIfModeInputs = document.querySelectorAll('input[name="whatIfMode"]');
  whatIfModeInputs.forEach((input) => {
    input.addEventListener('change', () => {
      if (input.checked) setWhatIfMode(input.value);
    });
  });
  const activeModeInput = document.querySelector('input[name="whatIfMode"]:checked');
  if (activeModeInput) setWhatIfMode(activeModeInput.value);

  whatIfTypeSelect?.addEventListener('change', () => setWhatIfType(whatIfTypeSelect.value));
  whatIfSuggestCategoriesSelect?.addEventListener('change', () => clearWhatIfSuggestions());
  whatIfFairnessTargetSelect?.addEventListener('change', () => clearWhatIfSuggestions());
  whatIfAreaFocusSelect?.addEventListener('change', () => clearWhatIfSuggestions());
  if (whatIfLassoBtn && !whatIfLassoBtn.__bound) {
    whatIfLassoBtn.addEventListener('click', keepOpen(() => toggleWhatIfLasso()));
    whatIfLassoBtn.__bound = true;
  }
  if (whatIfLassoClearBtn && !whatIfLassoClearBtn.__bound) {
    whatIfLassoClearBtn.addEventListener('click', keepOpen(() => clearWhatIfMockBuildings()));
    whatIfLassoClearBtn.__bound = true;
  }
  setWhatIfLassoButtonState();
  setWhatIfLassoClearDisabled(true);
  if (!document.body.__whatIfLassoEscBound) {
    document.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape' && whatIfLasso.active) {
        ev.preventDefault();
        setWhatIfLassoActive(false);
      }
    });
    document.body.__whatIfLassoEscBound = true;
  }

  const setWhatIfSuggestionUI = (text, { isError = false, isBusy = false, hasSuggestion = false } = {}) => {
    if (whatIfSuggestionOut) {
      whatIfSuggestionOut.textContent = text || '';
      whatIfSuggestionOut.classList.toggle('text-danger', isError);
      whatIfSuggestionOut.classList.toggle('text-muted', !isError);
    }
    if (whatIfApplySuggestionBtn) whatIfApplySuggestionBtn.disabled = !hasSuggestion || isBusy;
    if (whatIfClearSuggestionBtn) whatIfClearSuggestionBtn.disabled = !hasSuggestion || isBusy;
    if (whatIfVerifySuggestionBtn) whatIfVerifySuggestionBtn.disabled = !hasSuggestion || isBusy;
    if (whatIfSuggestBtn) whatIfSuggestBtn.disabled = !!isBusy;
  };

  const runWhatIfSuggestion = async () => {
    const prompt = (whatIfLLMInput?.value || '').trim();
    const fallbackKind = whatIfModeSelect?.value || 'add';
    const fallbackCount = Math.max(1, Math.min(10, parseInt(whatIfCountInput?.value || '1', 10) || 1));
    const selectedCategories = getSelectedWhatIfCategories();
    const fallbackCategories = selectedCategories.length
      ? selectedCategories
      : [whatIfType || ALL_CATEGORIES[0] || 'grocery'];
    const fallbackFairnessTarget = whatIfFairnessTargetSelect?.value || 'category';
    const fallbackAreaFocus = whatIfAreaFocusSelect?.value || 'any';
    const useBounds = !!whatIfUseBoundsToggle?.checked;
    const fallbackBbox = useBounds ? getMapBoundsBBox() : null;
    const lassoBbox = getWhatIfLassoBBox();
    const radiusKm = Math.max(0, parseFloat(whatIfRadiusInput?.value || '0') || 0);
    const center = radiusKm > 0 ? getMapCenter() : null;
    let lassoRequested = false;

    setWhatIfSuggestionUI('Thinking', { isBusy: true });
    try {
      let categories = fallbackCategories;
      let kind = fallbackKind;
      let count = fallbackCount;
      let bbox = fallbackBbox;
      let selectedRadiusKm = radiusKm;
      let selectedCenter = center;
      let fairnessTarget = fallbackFairnessTarget;
      let areaFocus = fallbackAreaFocus;

      let rationale = null;
      if (prompt) {
        const intent = await requestLLMWhatIfIntent(prompt, {
          available_categories: ALL_CATEGORIES,
          current_category: fallbackCategories[0],
          selected_categories: fallbackCategories,
          max_count: fallbackCount,
          radius_km: radiusKm,
          focus_default: useBounds ? 'viewport' : 'city',
          fairness_default: fallbackFairnessTarget,
          area_default: fallbackAreaFocus,
          lasso_available: !!whatIfLasso.selectionRing,
          lasso_bbox: lassoBbox
        });
        if (intent?.categories?.length) {
        // Only keep categories the user actually selected in the UI
        const allowed = new Set(fallbackCategories.map(c => c.toLowerCase()));
        const filtered = intent.categories.filter(c => allowed.has(c.toLowerCase()));
        categories = filtered.length ? filtered : fallbackCategories;
      }
        if (intent?.mode) kind = intent.mode;
        if (intent?.count) count = intent.count;
        if (intent?.fairness_target) fairnessTarget = intent.fairness_target;
        if (intent?.area) areaFocus = intent.area;
        if (intent?.focus === 'city') {
          bbox = null;
          selectedRadiusKm = 0;
          selectedCenter = null;
        }
        if (intent?.focus === 'viewport') {
          bbox = getMapBoundsBBox();
        }
        if (intent?.focus === 'lasso') {
          bbox = lassoBbox;
          selectedRadiusKm = 0;
          selectedCenter = null;
          lassoRequested = true;
        }
        if (intent?.rationale) rationale = intent.rationale;
      }

      if (lassoRequested && !bbox) {
        throw new Error('Draw a what-if lasso selection first.');
      }

      // When targeting overall fairness with multiple categories selected,
      // ensure count is at least the number of categories so each gets a chance.
      if (fairnessTarget === 'overall' && categories.length > 1 && count < categories.length) {
        count = Math.min(categories.length, 10);
      }

      // Force city-wide exact scope for LLM suggestions: full city candidate space under current model.
      bbox = null;
      selectedRadiusKm = 0;
      selectedCenter = null;
      areaFocus = 'any';

      whatIfLastSuggestionConfig = {
        categories: [...categories],
        kind,
        count,
        bbox,
        center: selectedCenter,
        radiusKm: selectedRadiusKm,
        fairnessTarget,
        fairnessCategories: categories.length ? [...categories] : [...ALL_CATEGORIES],
        areaFocus
      };
      const suggestions = await computeWhatIfSuggestions({
        categories,
        kind,
        count,
        bbox,
        center: selectedCenter,
        radiusKm: selectedRadiusKm,
        fairnessTarget,
        fairnessCategories: categories.length ? categories : ALL_CATEGORIES,
        areaFocus
      });
     const shouldAutoVerifyExact = count === 1 && categories.length === 1;
      let exactSuggestions = suggestions;
      if (shouldAutoVerifyExact) {
        const exactReport = await verifyWhatIfSuggestionsOptimality(whatIfLastSuggestionConfig, suggestions);
        exactSuggestions = Array.isArray(exactReport?.bestSuggestions) && exactReport.bestSuggestions.length
          ? exactReport.bestSuggestions
          : suggestions;
      }
      if (!exactSuggestions.length) {
        throw new Error('No feasible city-wide suggestions found under current filters/model.');
      }
      setWhatIfSuggestions(exactSuggestions);
      const summaryText = formatWhatIfSuggestionSummary(exactSuggestions);
      const verifyHint = shouldAutoVerifyExact ? '' : ' Verify optimality can run bounded search for multi-location requests.';
      const message = rationale ? `${summaryText} ${rationale}${verifyHint}` : `${summaryText}${verifyHint}`;
      setWhatIfSuggestionUI(message, {
        hasSuggestion: exactSuggestions.length > 0,
        isBusy: false
      });
      if (exactSuggestions[0]?.location) {
        flyToPoint(exactSuggestions[0].location);
      }
    } catch (err) {
      console.error(err);
      clearWhatIfSuggestions();
      setWhatIfSuggestionUI(err?.message || 'Unable to compute suggestions.', {
        isError: true,
        hasSuggestion: false,
        isBusy: false
      });
    }
  };

  if (whatIfSuggestBtn) {
    whatIfSuggestBtn.addEventListener('click', keepOpen(runWhatIfSuggestion));
  }
  if (whatIfApplySuggestionBtn) {
   whatIfApplySuggestionBtn.addEventListener('click', keepOpen(async () => {
      await applyWhatIfSuggestions();
    }));
  }
  if (whatIfClearSuggestionBtn) {
    whatIfClearSuggestionBtn.addEventListener('click', keepOpen(() => {
      clearWhatIfSuggestions();
      setWhatIfSuggestionUI('Suggestions cleared.', { hasSuggestion: false });
    }));
  }

  // Change log: compare baseline toggle (hold = show original, release = restore)
  const changeLogCompareBtn = document.getElementById('changeLogCompareBtn');
  if (changeLogCompareBtn) {
    const enterCompare = (ev) => {
      ev.preventDefault(); ev.stopPropagation();
      changeCompareBaseline = true;
      changeLogCompareBtn.classList.replace('btn-outline-info', 'btn-info');
      changeLogCompareBtn.textContent = '👁 Viewing baseline…';
      setSidePanelLegendMode('change');
      updateLayers();
    };
    const exitCompare = (ev) => {
      ev.preventDefault(); ev.stopPropagation();
      changeCompareBaseline = false;
      changeLogCompareBtn.classList.replace('btn-info', 'btn-outline-info');
      changeLogCompareBtn.textContent = '👁 Compare baseline';
      setSidePanelLegendMode('change');
      updateLayers();
    };
    // Support both mouse (hold) and click (toggle) — click toggles, mousedown/up holds
    changeLogCompareBtn.addEventListener('mousedown', enterCompare);
    changeLogCompareBtn.addEventListener('mouseup', exitCompare);
    changeLogCompareBtn.addEventListener('mouseleave', exitCompare);
    // Touch support
    changeLogCompareBtn.addEventListener('touchstart', enterCompare, { passive: false });
    changeLogCompareBtn.addEventListener('touchend', exitCompare);
  }

  // Change log: clear all
  const changeLogClearBtn = document.getElementById('changeLogClearBtn');
  if (changeLogClearBtn) {
    changeLogClearBtn.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      whatIfChangeLog = [];
      // Strip snapshot colors from all features in memory
      (baseCityFC?.features || []).forEach(f => {
        if (f?.properties) {
          delete f.properties._changeColor;
          delete f.properties._changeId;
        }
      });
      (newbuildsFC?.features || []).forEach(f => {
        if (f?.properties) {
          delete f.properties._changeColor;
          delete f.properties._changeId;
        }
      });
      pinnedChangeId = null;
      setSidePanelLegendMode('fairness');
      changeLogTick++;
      updateChangeLogUI();
      updateLayers();
    });
  }
  if (whatIfVerifySuggestionBtn) {
    whatIfVerifySuggestionBtn.addEventListener('click', keepOpen(async () => {
      setWhatIfSuggestionUI('Verifying mathematically…', {
        hasSuggestion: whatIfSuggestions.length > 0,
        isBusy: true
      });
      try {
        const report = await verifyWhatIfSuggestionsOptimality(whatIfLastSuggestionConfig, whatIfSuggestions);
        if (Array.isArray(report.perCategory)) {
          const parts = report.perCategory.map((item) => {
            const status = item.gap <= 0.00001
              ? (item.globalGuarantee ? 'optimal (globally proven under current filters)' : (item.exhaustiveWithinPool ? 'optimal (proven on searched pool)' : 'best found in partial search'))
              : `not optimal (gap ${formatObjectiveValue(item.gap)})`;
            return `${status}. ${summarizeVerificationAudit(item)}`;
          });
          const suffix = report.truncated
            ? ` Checked ${report.combosChecked} combos (partial).`
            : ` Checked ${report.combosChecked} combos.`;
          setWhatIfSuggestionUI(`Verification: ${parts.join(' | ')}.${suffix}`, {
            hasSuggestion: whatIfSuggestions.length > 0,
            isBusy: false,
            isError: report.gap > 0.00001
          });
         } else {
          const status = report.gap <= 0.00001
            ? (report.globalGuarantee ? 'optimal (globally proven under current filters)' : (report.exhaustiveWithinPool ? 'optimal (proven on searched pool)' : 'best found in partial search'))
            : `not optimal (gap ${formatObjectiveValue(report.gap)})`;
          const suffix = report.truncated
            ? ` Checked ${report.combosChecked} combos (partial).`
            : ` Checked ${report.combosChecked} combos.`;
          setWhatIfSuggestionUI(
            `Verification (${report.metricName}): ${status}. ${summarizeVerificationAudit(report)}${suffix}`,
            {
              hasSuggestion: whatIfSuggestions.length > 0,
              isBusy: false,
              isError: report.gap > 0.00001
            }
          );
        }
      } catch (err) {
        setWhatIfSuggestionUI(`Verification failed: ${err?.message || 'Unknown error.'}`, {
          hasSuggestion: whatIfSuggestions.length > 0,
          isBusy: false,
          isError: true
        });
      }
    }));
  }

  sourceSelect?.addEventListener('change', async () => {
    sourceMode = sourceSelect.value;

    if (sourceMode === 'osm' || sourceMode === 'osm_s1') {
      viewMode = 'all';
      selectedYear = '';
      modeAllBtn?.classList.add('active');
      modeNewBtn?.classList.remove('active');
      yearControlsWrap?.classList.add('d-none-important');
    }

    toggleLocalOnlyUI(sourceMode === 's1');
    osmControls?.classList.toggle('d-none-important', sourceMode === 's1');

    resetUIState();
    poiCache = {};

    if (sourceMode === 's1') {
      await loadSentinelData();
      await autoComputeOverall();
    } else if (sourceMode === 'osm_s1') {
      baseCityFC = null; newbuildsFC = null;
      districtLandClipSignature = '';
      const cityKey = document.getElementById('citySelect')?.value || 'vaxjo';
      await loadCityLocal(cityKey);
      await autoComputeOverall();
    } else {
      baseCityFC = null; newbuildsFC = null;
      districtLandClipSignature = '';
      const cityKey = document.getElementById('citySelect')?.value || 'vaxjo';
      lastCityName = LOCAL_CITY_NAMES[cityKey] || cityKey;
      await loadCityOSM(lastCityName);
      await autoComputeOverall();
    }
  });

  modeAllBtn?.addEventListener('click', (e) => { e.preventDefault(); setMode('all'); });
  modeNewBtn?.addEventListener('click', (e) => { e.preventDefault(); setMode('new'); });
  yearFilterEl?.addEventListener('change', () => { selectedYear = yearFilterEl.value; updateLayers(); });

  heightScaleEl?.addEventListener('input', () => {
    heightScale = parseFloat(heightScaleEl.value) || 1.0;
    heightScaleLabel.textContent = heightScale.toFixed(1);
    updateLayers();
  });

  let cityLoadInFlight = false;
  const triggerCityLoad = async () => {
    if (cityLoadInFlight) return;  // ignore extra clicks while a load is running
    cityLoadInFlight = true;
    const cityKey = citySelect?.value || 'vaxjo';
    const displayName = LOCAL_CITY_NAMES[cityKey] || cityKey;

    // Stash the city we're leaving BEFORE anything mutates it, then disconnect
    // baseCityFC so the upcoming resetUIState → clearFairness(true) doesn't wipe
    // fair_* props off the cached FC (which is the same object reference the
    // snapshot is holding). loadCityLocal/loadCityOSM will reassign baseCityFC.
    if (lastCityKeyLoaded && lastCityKeyLoaded !== cityKey && baseCityFC) {
      sessionCityCache.set(lastCityKeyLoaded, snapshotCity());
      baseCityFC = null;
      newbuildsFC = null;
    }

    lastCityName = displayName;

    // Cache hit — restore the previously parsed FC + computed fairness in one frame.
    const cached = sessionCityCache.get(cityKey);
    if (cached) {
      showGlobalSpinner(`Switching to ${displayName}…`);
      if (citySelect) citySelect.disabled = true;
      if (jobStatusEl) jobStatusEl.textContent = `Switching to ${displayName}…`;
      await waitForSpinnerPaint();
      try {
        restoreCity(cached);
        lastCityKeyLoaded = cityKey;
        if (jobStatusEl) {
          jobStatusEl.textContent = '';
        }
      } catch (err) {
        console.error('cache restore failed', err);
        if (jobStatusEl) jobStatusEl.textContent = 'Cache restore failed';
      } finally {
        if (citySelect) citySelect.disabled = false;
        hideGlobalSpinner();
        cityLoadInFlight = false;
        // City changed → snap the map back to Fairness (Supply/Mismatch are per-city).
        if (typeof resetMapColorVarToFairness === 'function') resetMapColorVarToFairness();
      }
      return;
    }

    // Cache miss — fresh load + compute, then implicitly cached when the user
    // switches away to another city.
    showGlobalSpinner(`Loading ${displayName}…`);
    if (citySelect) citySelect.disabled = true;
    if (jobStatusEl) jobStatusEl.textContent = `Loading ${displayName}…`;
    await waitForSpinnerPaint();

    try {
      resetUIState();
      poiCache = {};

      if (sourceMode === 'osm_s1') {
        await loadCityLocal(cityKey);
      } else {
        await loadCityOSM(displayName);
      }
      await autoComputeOverall();
      lastCityKeyLoaded = cityKey;
    } catch (err) {
      console.error('city load failed', err);
      if (jobStatusEl) jobStatusEl.textContent = 'Failed';
    } finally {
      if (citySelect) citySelect.disabled = false;
      hideGlobalSpinner();
      cityLoadInFlight = false;
      // City changed → snap the map back to Fairness (Supply/Mismatch are per-city).
      if (typeof resetMapColorVarToFairness === 'function') resetMapColorVarToFairness();
    }
  };

  citySelect?.addEventListener('change', () => {
    triggerCityLoad();
  });

  districtToggleBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    setDistrictView(!districtView);
  });

  // === POI checklist + weights ===
  const onPOIUIChange = debounce(async () => {
    const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
    const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
    const prevCatLabel = fairCategory && fairCategory !== '—' ? fairCategory : null;
    const poiScoreSnapshot = captureScoreSnapshot();
    fairStatus.textContent = '';
    fairStatus.classList.remove('text-danger');
    hideSidePanel();
    giniOut.textContent = '—';
    const mix = readPOIMixFromUI(); // [{cat, weight}, ...]
    selectedPOIMix = mix;
    if (mix.length) {
      const preferred = mix[0]?.cat;
      const includesCurrent = mix.some(entry => entry.cat === whatIfType);
      if (preferred && !includesCurrent) {
        setWhatIfType(preferred);
      }
    }
    if (!mix.length) {
      setParallelCoordsPending(false);
      if (districtView) {
        clearDistrictFairnessView();
      } else {
        clearFairness(false);
      }
      return;
    }

    showGlobalSpinner('Computing fairness…');
    await waitForSpinnerPaint();
    fairStatus.textContent = 'Computing…';
    setParallelCoordsPending(true);
    const genAtStart = fairnessComputeGen;
    try {
      if (mix.length === 1) {
        const singleCat = mix[0].cat;
        const res = await computeFairnessFast(singleCat);
        fairStatus.textContent = '';
        giniOut.textContent = `${prettyPOIName(singleCat)} Gini: ${formatFairnessBadgeValue(res.gini)}`;
      } else {
        const res = await computeFairnessWeighted(mix); // recomputes + recolors
        fairStatus.textContent = '';
        giniOut.textContent = `Mix Gini: ${formatFairnessBadgeValue(res.gini)}`;
        showSidePanel('mix', res.gini, res.poiCount, window.getFairnessSummary?.());
      }
    } catch (e) {
      console.error(e);
      fairStatus.textContent = 'Error';
      fairStatus.classList.add('text-danger');
    } finally {
      hideGlobalSpinner();
      setParallelCoordsPending(false);
    }

    if (mix.length) {
      const afterCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
      const afterOverallGini = Number.isFinite(overallGini) ? overallGini : null;
      const catNames = mix.map(m => prettyPOIName(m.cat)).join(', ');
      const prevLabel = prevCatLabel ? prettyPOIName(prevCatLabel) : null;
      const desc = prevLabel && prevLabel !== catNames
        ? `POI: ${prevLabel} → ${catNames}`
        : `POI selected: ${catNames}`;
      const nextId = changeLogIdCounter;
      const colorPairs = applyDeltaColorsFromSnapshot(poiScoreSnapshot, nextId);
      recordWhatIfChange({
        action: 'poi_change',
        description: desc,
        category: mix.length === 1 ? mix[0].cat : 'mix',
        beforeGini: beforeCatGini,
        afterGini: afterCatGini,
        beforeOverall: beforeOverallGini,
        afterOverall: afterOverallGini,
        affectedFeatures: colorPairs
      });
    }
  }, 150);

  document.querySelectorAll('.poi-check').forEach(el => el.addEventListener('change', onPOIUIChange));

  document.querySelectorAll('.poi-weight').forEach(el => {
    el.addEventListener('input', (e) => {
      const cat = e.target.getAttribute('data-cat');
      const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
      if (badge) badge.textContent = e.target.value;
      onPOIUIChange();
    });
  });

  document.querySelectorAll('.poi-weight').forEach(el => {
    const cat = el.getAttribute('data-cat');
    const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
    if (badge) badge.textContent = el.value;
  });

  const poiClearBtn = document.getElementById('poiClearBtn');
  poiClearBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();

    document.querySelectorAll('.poi-check').forEach(el => { el.checked = false; });
    document.querySelectorAll('.poi-weight').forEach(el => {
      const defVal = el.getAttribute('value') || '5';
      el.value = defVal;
      const cat = el.getAttribute('data-cat');
      const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
      if (badge) badge.textContent = defVal;
    });
    // Clear fairness immediately so that resetWhatIfChanges → clearWhatIfMockBuildings
    // → recomputeFairnessAfterWhatIf sees fairActive=false and skips recomputing.
    clearFairness(false);
    if (giniOut) giniOut.textContent = '—';
    hideSidePanel?.();
    resetWhatIfChanges();
    onPOIUIChange();
  });

  // Place "Select All" beside Clear (robust, non-closing)
  (function ensureSelectAllBesideClear() {
    const poiClearBtnEl = document.getElementById('poiClearBtn');
    if (!poiClearBtnEl || document.getElementById('poiSelectAllBtn')) return;

    const row = poiClearBtnEl.closest('.d-flex') || poiClearBtnEl.parentElement || poiControls;
    const selectAllBtn = document.createElement('button');
    selectAllBtn.id = 'poiSelectAllBtn';
    selectAllBtn.type = 'button';
    selectAllBtn.className = 'btn btn-sm btn-outline-secondary me-2';
    selectAllBtn.textContent = 'Select All';

    row.insertBefore(selectAllBtn, poiClearBtnEl);

    const keepOpen = (handler) => (ev) => { ev.preventDefault(); ev.stopPropagation(); handler(ev); };
    selectAllBtn.addEventListener('click', keepOpen(() => {
      document.querySelectorAll('.poi-check').forEach(el => { el.checked = true; });
      onPOIUIChange();
    }));

    // Keep dropdown open on press
    selectAllBtn.addEventListener('pointerdown', (ev) => { ev.preventDefault(); ev.stopPropagation(); });
    poiClearBtnEl.addEventListener('pointerdown', (ev) => { ev.preventDefault(); ev.stopPropagation(); });
  })();

  // === Symbols toggle
  if (poiSymbolsToggle && !poiSymbolsToggle.__bound) {
    poiSymbolsToggle.addEventListener('click', (ev) => { ev.stopPropagation(); });
    showPOISymbols = !!poiSymbolsToggle.checked;
    poiSymbolsToggle.addEventListener('change', (e) => {
      showPOISymbols = !!e.currentTarget.checked;
      poiStyleTick++;
      updateLayers();
    });
    poiSymbolsToggle.__bound = true;
  }

  if (!document.body.__additiveSelectionKeyBound) {
    const setModifierKeyState = (ev) => {
      additiveSelectionKeyActive = !!(ev?.metaKey || ev?.ctrlKey);
    };
    document.addEventListener('keydown', setModifierKeyState);
    document.addEventListener('keyup', setModifierKeyState);
    window.addEventListener('blur', () => { additiveSelectionKeyActive = false; });
    document.body.__additiveSelectionKeyBound = true;
  }
}

function wireMapLassoUI() {
  const lassoBtn = document.getElementById('mapLassoBtn');
  if (lassoBtn && !lassoBtn.__bound) {
    lassoBtn.addEventListener('click', (e) => { e.preventDefault(); toggleMapLasso(); });
    lassoBtn.__bound = true;
  }

  const clearBtn = document.getElementById('mapLassoClearBtn');
  if (clearBtn && !clearBtn.__bound) {
    clearBtn.addEventListener('click', (e) => { e.preventDefault(); clearMapSelection(); });
    clearBtn.__bound = true;
  }

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && mapLasso.active) {
      ev.preventDefault();
      setMapLassoActive(false);
    }
  });

  setMapLassoButtonState();
  setMapLassoClearDisabled(true);
}

/* Show/hide navbar items that should appear only for Local source */
function toggleLocalOnlyUI(isLocal) {
  modeAllLi?.classList.toggle('d-none-important', !isLocal);
  modeNewLi?.classList.toggle('d-none-important', !isLocal);
  heightControls?.classList.toggle('d-none-important', !isLocal);

  if (!isLocal) {
    yearControlsWrap?.classList.add('d-none-important');
  } else {
    yearControlsWrap?.classList.toggle('d-none-important', viewMode !== 'new');
  }
}

// District helpers + coastline clipping live in assets/js/models/districts.js
// Generic loaders live in assets/js/models/loaders.js
// City loader lives in assets/js/models/cityLoader.js

/* ---------- Animated transition (Changes) state ---------- */
let transitionAnimActive = false;
let transitionAnimT = 0;
let transitionAnimRAF = null;
let transitionAnimTick = 0;
let transitionHasData = false;
let transitionMezoPrevScoreByHex = new Map();

// POI selection state + small popup/sidebar helpers (moved from main.js residual)
let selectedPOIId = null;
let selectedPOIFeature = null;
let showPOISymbols = true;
let poiStyleTick = 0;

function poiCategoryOf(f) {
  return (f?.properties?.__cat || f?.properties?.category || 'default').toLowerCase();
}

function formatFairnessBadgeValue(value) {
  return Number.isFinite(value) ? value.toFixed(FAIRNESS_BADGE_DECIMALS) : '—';
}

function normalizeCityKey(city) {
  return String(city || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_]/g, '');
}

function districtCityKeyFromInput(city) {
  const key = normalizeCityKey(city);
  const compact = key.replace(/_/g, '');
  const aliases = {
    vaxjo: ['vaxjo', 'vaxjoe'],
    malmo: ['malmo', 'malmoe'],
    goteborg: ['goteborg', 'gothenburg'],
    norrkoping: ['norrkoping', 'norrkoeping'],
    stockholm: ['stockholm'],
    uppsala: ['uppsala']
  };

  for (const [cityKey, cityAliases] of Object.entries(aliases)) {
    for (const alias of cityAliases) {
      if (key.startsWith(alias)) return cityKey;
      if (key.includes(`_${alias}_`) || key.endsWith(`_${alias}`) || key.startsWith(`${alias}_`)) return cityKey;
      if (compact.includes(alias)) return cityKey;
    }
  }
  return null;
}

function applyDistrictDatasetForCity(city) {
  const explicitCityKey = districtCityKeyFromInput(city);
  const resolvedCityKey = explicitCityKey || null;
  const nextURL = resolvedCityKey ? (DISTRICT_URL_BY_CITY_KEY[resolvedCityKey] || null) : null;
  const hasChanged = nextURL !== activeDistrictURL;
  activeDistrictCityKey = resolvedCityKey;

  if (!explicitCityKey) {
    console.warn(`No district dataset configured for "${city}". District/macro boundary overlays are disabled to avoid mismatch.`);
  }

  activeDistrictURL = nextURL;

  if (!hasChanged) return;

  districtFC = null;
  districtBoundaryFC = null;
  districtLoadError = null;
  districtLoadPromise = null;
  mezoMaskPolygon = null;
  mezoHexData = [];
  districtLandClipSignature = '';
  forbiddenZonesFC = null;
}
