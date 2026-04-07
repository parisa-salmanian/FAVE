// One app, two views: OSM API (live) and Sentinel-1 (local files) + fairness + robust POI markers + DR Explorer
//To have app running both online and local
function resolveApiBase() {
  const cfgBase = typeof window !== 'undefined' ? window.APP_CONFIG?.API_BASE : undefined;
  if (cfgBase && String(cfgBase).trim()) return String(cfgBase).trim().replace(/\/$/, '');

  if (typeof window !== 'undefined') {
    const urlBase = new URLSearchParams(window.location.search).get('api_base');
    if (urlBase && urlBase.trim()) return urlBase.trim().replace(/\/$/, '');

    if (window.location.protocol !== 'file:') {
      const host = window.location.hostname;
      const port = window.location.port;
      const isLocalHost = host === '127.0.0.1' || host === 'localhost';
      const isLikelyStaticDevPort = port === '5500' || port === '3000' || port === '5173';
      if (isLocalHost && isLikelyStaticDevPort) {
        return 'http://127.0.0.1:8001';
      }
      return window.location.origin.replace(/\/$/, '');
    }
  }

  return 'http://127.0.0.1:8001';
}

const API_BASE = resolveApiBase();
const ROUTING_BASE_URL = 'https://router.project-osrm.org';
const ROUTING_PROFILE = 'walking';
const ROUTING_TIMEOUT_MS = 8000;
const ROUTING_CACHE_LIMIT = 5000;
const TRAVEL_SPEED_KMH = {
  walking: 5,
  cycling: 15,
  driving: 40
};
const FAIRNESS_EFFORT_MULTIPLIER = {
  walking: 1,
  cycling: 0.7,
  driving: 0.45
};
const FAIRNESS_TRAVEL_MODE_DEFAULT = 'walking';

/* ---------- Animated transition (Changes) state ---------- */
let transitionAnimActive = false;
let transitionAnimT = 0;          // 0 = fully "before", 1 = fully "after"
let transitionAnimRAF = null;
let transitionAnimTick = 0;
let transitionHasData = false;    // true once __prevScore is saved
let transitionMezoPrevScoreByHex = new Map();
const TRANSITION_DURATION_MS = 1200;
const TRANSITION_CHANGED_FLASH_MS = 420;
const TRANSITION_CHANGED_FLASH_MAX_INTENSITY = 0.9;
const TRANSITION_CHANGED_FLASH_COLOR = [220, 20, 60]; // Crimson
const TRANSITION_CHANGE_EPSILON = 1e-6;
const FAIRNESS_MODEL_DEFAULT = 'ifcity';
const FAIRNESS_COLOR_SCHEME_DEFAULT = 'cool';
const USE_TRAVEL_TIME = true;
const ROUTING_MAX_BUILDINGS = 400;
const ROUTING_MAX_POIS = 200;
const ROUTING_ENABLE_OVERALL = false;
const OVERPASS_ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://overpass.nchc.org.tw/api/interpreter'
];
const OVERPASS_TIMEOUT_MS = 20000;
const OVERPASS_RETRY_DELAY_MS = 1200;
const FAIRNESS_POI_FETCH_RETRIES = 2;
const IF_CITY_ALPHA = 2;
const IF_CITY_BASELINE_UTILITY = 0;
const IF_CITY_KAPPA_DEFAULT = 0.8;
const IF_CITY_KAPPA_BY_CAT = {
  grocery: 1.2,
  hospital: 0.6,
  pharmacy: 1.0,
  dentistry: 1.0,
  healthcare_center: 0.8,
  veterinary: 0.8,
  university: 0.5,
  kindergarten: 1.1,
  school_primary: 0.9,
  school_high: 0.7
};
const IF_CITY_PRIORITY_WEIGHTS = {
  grocery: 1.0,
  hospital: 1.4,
  pharmacy: 1.2,
  dentistry: 1.1,
  healthcare_center: 1.2,
  veterinary: 0.9,
  university: 0.8,
  kindergarten: 1.3,
  school_primary: 1.2,
  school_high: 1.1
};
const IF_CITY_EQUITY_WEIGHTS = {
  Residential: 1.0,
  Education: 1.1,
  Health: 1.2,
  Commercial: 0.95,
  Industrial: 0.9,
  'Other / unknown': 1.0
};
const IF_CITY_MODE_DISTANCE_FACTOR = {
  walking: 1.0,
  cycling: 1.55,
  driving: 1.85
};
const IF_CITY_MODE_DETOUR_FACTOR = {
  walking: 1.0,
  cycling: 1.15,
  driving: 1.3
};
const IF_CITY_MODE_SPEED_KMH = {
  walking: 5,
  cycling: 15,
  driving: 40
};
const IF_CITY_REFERENCE_SPEED_KMH = IF_CITY_MODE_SPEED_KMH.walking;

/* ---------- Local file paths ---------- */
const CITY_URL  = 'assets/data/lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson';
const STATS_URL = 'assets/data/bldg_web.geojson';
const OSM_GEOJSON_CITY_URLS = {
  malmo: 'assets/data/byggnad_malmo.geojson'
};
const DEFAULT_DISTRICT_URL = 'assets/data/Vaxjo_regso.geojson';
const DISTRICT_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/Vaxjo_regso.geojson',
  malmo: 'assets/data/malmo_regso.geojson',
  goteborg: 'assets/data/goteborg_regso.geojson',
  norrkoping: 'assets/data/norrkoping_regso.geojson',
  stockholm: 'assets/data/stockholm_regso.geojson',
  uppsala: 'assets/data/uppsala_regso.geojson',
  kalmar: 'assets/data/kalmar.geojson'
};

const BUILDING_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson',
  malmo: 'assets/data/byggnad_malmo.geojson',
  goteborg: 'assets/data/byggnad_goteborg.geojson',
  norrkoping: 'assets/data/byggnad_norrkoping.geojson',
  stockholm: 'assets/data/byggnad_stockholm.geojson',
  uppsala: 'assets/data/byggnad_uppsala.geojson',
  kalmar: 'assets/data/byggnad_kalmar.geojson'
};

const LOCAL_CITY_NAMES = {
  vaxjo: 'Växjö', malmo: 'Malmö', goteborg: 'Göteborg',
  stockholm: 'Stockholm', kalmar: 'Kalmar',
  norrkoping: 'Norrköping', uppsala: 'Uppsala'
};

const ANDAMAL_TO_POI_CATEGORY = {
  'sjukhus':       'hospital',
  'vårdcentral':   'healthcare_center',
  'apotek':        'pharmacy',
  'tandvård':      'dentistry',
  'tandläkare':    'dentistry',
  'veterinär':     'veterinary',
  'djursjukhus':   'veterinary',
  'universitet':   'university',
  'högskola':      'university',
  'förskola':      'kindergarten',
  'skola':         'school_primary',
  'grundskola':    'school_primary',
  'gymnasium':     'school_high',
  'livsmedel':     'grocery',
  'dagligvaror':   'grocery',
  'handel':        'grocery',
  'butik':         'grocery'
};

const ANDAMAL_TO_BUILDING_TYPE = {
  'bostad':              'residential',
  'flerfamiljshus':      'residential',
  'småhus':              'residential',
  'radhus':              'residential',
  'parhus':              'residential',
  'fritidshus':          'residential',

  'industri':            'industrial',
  'lager':               'industrial',
  'fabrik':              'industrial',

  'verksamhet':          'commercial',
  'kontor':              'commercial',
  'affär':               'retail',
  'handel':              'retail',
  'köpcentrum':          'retail',
  'varuhus':             'retail',
  'butik':               'retail',

  'hotell':              'hotel',
  'vandrarhem':          'hotel',

  'kyrka':               'religious',
  'moské':               'religious',
  'samfund':             'religious',
  'kapell':              'religious',

  'skola':               'education',
  'grundskola':          'education',
  'gymnasium':           'education',
  'universitet':         'education',
  'högskola':            'education',
  'förskola':            'education',

  'sjukhus':             'public',
  'vårdcentral':         'public',
  'samhällsfunktion':    'public',
  'brandstation':        'public',
  'polisstation':        'public',
  'bibliotek':           'public',

  'garage':              'garage',
  'komplementbyggnad':   'garage',
  'förråd':              'garage',
  'parkering':           'garage',

  'ekonomibyggnad':      'farm',
  'jordbruk':            'farm',
  'ladugård':            'farm',
  'stall':               'farm',

  'station':             'transportation',
  'terminal':            'transportation',
  'flygplats':           'transportation',

  'övrig':               'Other / unknown',
  'ospecificerad':       'Other / unknown'
};

const GENDER_AGE_POP_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/gender-age-population.json',
  malmo: 'assets/data/malmo_age_gender.json',
  stockholm: 'assets/data/stockholm_age_gender.json'
};
const DEFAULT_GENDER_AGE_POP_URL = 'assets/data/gender-age-population.json';
const GENDER_AGE_POP_URL = 'assets/data/gender-age-population.json';

// Summary filtering to avoid a permanent 0.00 outlier dominating "worst"
const SUMMARY_CUTOFF = 0.02;   // ignore buildings with fairness ≤ 0.02 when choosing worst
const HEX_CELL_KM    = 0.8;    // hex grid size for fallback districting
const HEX_MIN_COUNT  = 5;      // require at least this many buildings in a hex to include it
const MEZO_HEX_EDGE_OPTIONS_KM = [0.25, 0.5, 0.75, 1];
const DEFAULT_MEZO_HEX_EDGE_KM = 0.5;
const MEZO_MIN_COUNT = 3;      // require at least this many buildings in a mezo hex
const BUILDINGS_MIN_ZOOM = 13; // show building extrusions only when zoomed in it
const PARALLEL_COORDS_MAX_LINES = Number.isFinite(globalThis.PARALLEL_COORDS_MAX_LINES)
  ? globalThis.PARALLEL_COORDS_MAX_LINES
  : 5000;
globalThis.PARALLEL_COORDS_MAX_LINES = PARALLEL_COORDS_MAX_LINES;

// treat building as same-as-POI if nearest ≤ this distance
const SELF_POI_EPS_M = 5;
const WHATIF_SUGGESTION_LIMIT = 120;
const WHATIF_EXACT_MAX_COMBOS = 2000000n;

function isExactCityWideSingleCandidateMode({ count, bbox, center, radiusKm, areaFocus }) {
  return Number(count) === 1
    && !bbox
    && !center
    && (!Number.isFinite(radiusKm) || radiusKm <= 0)
    && (!areaFocus || areaFocus === 'any');
}
const WHATIF_MOCK_BUILDING_LIMIT = 5000;
// Mock building configurables (driven by UI sliders)
let whatIfMockFootprintMin = 10;   // meters
let whatIfMockFootprintMax = 500;   // meters
let whatIfMockFloorHeight  = 3;    // meters per floor
let whatIfMockFloors       = 3;    // number of floors
let whatIfMockShapeVariation = 50;  // 0–100 %, how much shapes differ from each other
const WHATIF_AVOID_SAMPLE_PX = 14;
const FAIRNESS_BADGE_DECIMALS = 3;
const WHATIF_AVOID_LAYER_KEYWORDS = [
  'water', 'lake', 'river', 'stream', 'canal', 'reservoir', 'ocean',
  'road', 'street', 'highway', 'bridge', 'tunnel', 'rail',
  'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
  'path', 'track', 'transit', 'ferry'
];

const DR_SELECTION_COLOR_DEFAULT = [241, 105, 19];  // default selection highlight (#f16913)
const BACKDROP_COLOR = [118, 134, 156];   // muted slate-blue for context buildings
const DEFAULT_BUILDING_COLOR = [140, 170, 198]; // [214, 183, 140]; cool slate-blue (clean contrast on gray basemap)
const DR_UNSELECTED_COLOR = [180, 180, 180];
const DR_DOT_SIZE_BY_MODE = {
  building: { normal: 3, selected: 6 },
  mezo: { normal: 3, selected: 6 },
  district: { normal: 9, selected: 14 }
};
const WHATIF_SUGGESTION_COLOR = [255, 193, 7];
const DISTRICT_DASH_EXT = new deck.PathStyleExtension({ dash: true });
let districtBoundaryFC = null;           // cached linework for district outlines
const DARK_BASEMAP_STYLE = '	https://basemaps.cartocdn.com/gl/positron-nolabels-gl-style/style.json';
const LIGHT_BASEMAP_STYLE = 'https://basemaps.cartocdn.com/gl/voyager-nolabels-gl-style/style.json';
let currentBasemapStyle = DARK_BASEMAP_STYLE;

/* ---------- Default colors (when fairness OFF) ---------- */
const CATEGORY_COLORS = {
  supermarket:[255,204,0], residential:[52,152,219], commercial:[255,159,67], retail:[255,204,0],
  industrial:[128,139,150], education:[46,204,113], health:[231,76,60], public:[142,68,173],
  religious:[241,196,15], transportation:[0,177,106], garage:[149,165,166], hotel:[255,118,117],
  sports:[39,174,96], farm:[110,44,0], house:[52,152,219], detached:[52,152,219],
  apartments:[52,152,219], terrace:[52,152,219], yes:BACKDROP_COLOR, unknown:BACKDROP_COLOR
};

// Unified highlight for buildings when filtering by type (purple)
const SELECTED_BUILDING_TYPE_COLOR = [128, 0, 128];
const BUILDING_MATERIAL = {
  ambient: 0.25,
  diffuse: 0.65,
  shininess: 48,
  specularColor: [255, 250, 240]
};

// Lantmäteriet local type colors
const TYPE_COLORS = {
  'Komplementbyggnad':[141,211,199], 'Bostad':[255,255,179], 'Samhällsfunktion':[190,186,218],
  'Verksamhet':[251,128,114], 'Ekonomibyggnad':[128,177,211], 'Övrig byggnad':[253,180,98],
  'Industri':[179,222,105], default:BACKDROP_COLOR
};

const YEAR_COLORS = { '0':[190,190,190], '2021':[52,152,219], '2022':[46,204,113], '2023':[243,156,18], '2024':[231,76,60], 'null':[155,89,182] };

/* --- High-contrast POI colors (for dots) --- */
const POI_MARK_COLORS = {
  grocery:[0,180,255], hospital:[0,180,255], pharmacy:[0,180,255], dentistry:[0,180,255],
  healthcare_center:[0,180,255], veterinary:[0,180,255], university:[0,180,255],
  kindergarten:[0,180,255], school_primary:[0,180,255], school_high:[0,180,255]
};

// === NEW: Symbol (glyph) and color per POI category ===
// const POI_SYMBOLS = {
//   grocery:           { glyph: '●'},
//   hospital:          { glyph: '⬛' },
//   pharmacy:          { glyph: '▲'  },
//   dentistry:         { glyph: '◆'  },
//   healthcare_center: { glyph: '◼'  },
//   veterinary:        { glyph: '⬢' },
//   university:        { glyph: '⬟' },
//   kindergarten:      { glyph: '◀'  },
//   school_primary:    { glyph: '▶'  },
//   school_high:       { glyph: '▼'  },
//   default:           { glyph: '⬭' }
// };

// const POI_SYMBOLS = {
//   grocery:           { glyph: '🛒', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-1.4 5.2a1 1 0 0 0 .97 1.3h11.86a1 1 0 0 0 .97-1.3L18 13"/><circle cx="9" cy="21" r="1.5" fill="currentColor"/><circle cx="18" cy="21" r="1.5" fill="currentColor"/></svg>' },

//   hospital:          { glyph: '🏥', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 9h18"/><line x1="12" y1="13" x2="12" y2="19" stroke-width="2.5" stroke-linecap="round"/><line x1="9" y1="16" x2="15" y2="16" stroke-width="2.5" stroke-linecap="round"/><path d="M8 5V3h8v2" stroke-linecap="round"/></svg>' },

//   pharmacy:          { glyph: '💊', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="5" y="2" width="14" height="20" rx="7"/><line x1="5" y1="12" x2="19" y2="12"/><rect x="5" y="12" width="14" height="10" rx="7" fill="currentColor" opacity="0.25"/></svg>' },

//   dentistry:         { glyph: '🦷', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"><path d="M12 2C9.5 2 7 3.5 5.5 6c-1.5 2.5-1 5.5 0 8 .7 1.8 1.5 4 2 6 .3 1 1.2 1.5 2 1 .8-.5 1-1.5 1.2-2.5.3-1.5.5-3 1.3-3s1 1.5 1.3 3c.2 1 .4 2 1.2 2.5.8.5 1.7 0 2-1 .5-2 1.3-4.2 2-6 1-2.5 1.5-5.5 0-8C17 3.5 14.5 2 12 2z"/></svg>' },

//   healthcare_center: { glyph: '⚕️', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"><path d="M12 3L4 7v6c0 5 3.5 9.7 8 11 4.5-1.3 8-6 8-11V7l-8-4z"/><line x1="12" y1="9" x2="12" y2="17" stroke-width="2.2" stroke-linecap="round"/><line x1="9" y1="13" x2="15" y2="13" stroke-width="2.2" stroke-linecap="round"/></svg>' },

//   veterinary:        { glyph: '🐾', svg: '<svg viewBox="0 0 24 24" fill="currentColor"><ellipse cx="12" cy="16" rx="3.5" ry="3"/><ellipse cx="7" cy="11" rx="2" ry="2.5"/><ellipse cx="17" cy="11" rx="2" ry="2.5"/><ellipse cx="9" cy="6.5" rx="1.8" ry="2.2"/><ellipse cx="15" cy="6.5" rx="1.8" ry="2.2"/></svg>' },

//   university:        { glyph: '🎓', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"><polygon points="12,3 2,9 12,15 22,9"/><path d="M6 11v5c0 2 3 4 6 4s6-2 6-4v-5"/><line x1="22" y1="9" x2="22" y2="17" stroke-linecap="round"/></svg>' },

//   kindergarten:      { glyph: '🧒', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="3"/><path d="M12 8v6"/><path d="M8 11l4 2 4-2"/><path d="M9 22l3-8 3 8"/></svg>' },

//   school_primary:    { glyph: '📚', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"><path d="M2 4c2-1 5-1.5 10 1v16C7 18.5 4 19 2 20V4z"/><path d="M22 4c-2-1-5-1.5-10 1v16c5-2.5 8-2 10-1V4z"/></svg>' },

//   school_high:       { glyph: '🏫', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="10" width="18" height="11" rx="1"/><path d="M8 10V6h8v4" stroke-linejoin="round"/><line x1="12" y1="6" x2="12" y2="2" stroke-linecap="round"/><polygon points="12,2 17,4 12,5.5" fill="currentColor"/><rect x="10" y="15" width="4" height="6"/></svg>' },

//   default:           { glyph: '📍', svg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z"/><circle cx="12" cy="9" r="2.5" fill="currentColor"/></svg>' },
// };

const POI_SYMBOLS = {
  grocery:           { icon: 'assets/icons/poi-grocery.svg' },
  hospital:          { icon: 'assets/icons/poi-hospital.svg' },
  pharmacy:          { icon: 'assets/icons/poi-pharmacy.svg' },
  dentistry:         { icon: 'assets/icons/poi-dentistry.svg' },
  healthcare_center: { icon: 'assets/icons/poi-healthcare_center.svg' },
  veterinary:        { icon: 'assets/icons/poi-veterinary.svg' },
  university:        { icon: 'assets/icons/poi-university.svg' },
  kindergarten:      { icon: 'assets/icons/poi-kindergarten.svg' },
  school_primary:    { icon: 'assets/icons/poi-school_primary.svg' },
  school_high:       { icon: 'assets/icons/poi-school_high.svg' },
  default:           { icon: 'assets/icons/poi-university.svg' },
};

// Build a canvas-based icon atlas from SVG strings for deck.gl IconLayer
function buildPOIIconAtlas(size = 64) {
  const cats = Object.keys(POI_SYMBOLS);
  const cols = Math.ceil(Math.sqrt(cats.length));
  const rows = Math.ceil(cats.length / cols);
  const canvas = document.createElement('canvas');
  canvas.width = cols * size;
  canvas.height = rows * size;
  const ctx = canvas.getContext('2d');
  const iconMapping = {};
  let loaded = 0;

  return new Promise((resolve) => {
    cats.forEach((cat, i) => {
      const col = i % cols;
      const row = Math.floor(i / cols);
      const img = new Image();
      img.onload = () => {
        ctx.drawImage(img, col * size, row * size, size, size);
        iconMapping[cat] = {
          x: col * size,
          y: row * size,
          width: size,
          height: size,
          mask: false
        };
        if (++loaded === cats.length) {
          resolve({ iconAtlas: canvas, iconMapping });
        }
      };
      img.onerror = () => {
        console.warn(`[POI Atlas] Failed to load icon for "${cat}":`, POI_SYMBOLS[cat].icon);
        if (++loaded === cats.length) {
          resolve({ iconAtlas: canvas, iconMapping });
        }
      };
      img.src = POI_SYMBOLS[cat].icon;
    });
  });
}

// Cache the atlas globally
let _poiIconAtlas = null;
let _poiIconMapping = null;
let _poiAtlasReady = false;
let _poiAtlasPromise = null;
function ensurePOIIconAtlas() {
  if (_poiAtlasReady) return;
  if (_poiAtlasPromise) return;
  _poiAtlasPromise = buildPOIIconAtlas(64).then(result => {
    _poiIconAtlas = result.iconAtlas;
    _poiIconMapping = result.iconMapping;
    _poiAtlasReady = true;
    console.log('[POI Atlas] Ready');
    updateLayers();
  });
}

// const POI_COLORS = {
// grocery:           [255, 165,   0],  // bright orange
// hospital:          [255,  50,  50],  // bright red
// pharmacy:          [255, 255, 255],  // white
// dentistry:         [255, 120,  70],  // coral
// healthcare_center: [255,  80,  80],  // soft red
// veterinary:        [160, 255,  50],  // lime green
// university:        [255, 215,   0],  // gold
// kindergarten:      [255, 240,  70],  // bright yellow
// school_primary:    [255, 180,  50],  // amber
// school_high:       [200, 255, 100],  // yellow-green
// default:           [255, 100,  30],  // red-orange
// };

const DISTRICT_LABEL_FONT_FAMILY = '"Noto Sans","Noto Sans Symbols 2","Noto Sans Symbols","Segoe UI","Arial Unicode MS",sans-serif';
const DISTRICT_LABEL_CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzÅÄÖåäö0123456789 .,-/()';

// Track a clicked POI to show selection emphasis
let selectedPOIId = null;
let selectedPOIFeature = null;
// Show/hide the symbol TextLayer (defaults ON)
let showPOISymbols = true;

// Bump this when styles/toggle change to force deck updateTriggers
let poiStyleTick = 0;

// Helpers for symbol layers
function poiCategoryOf(f) {
  return (f?.properties?.__cat || f?.properties?.category || 'default').toLowerCase();
}
// function poiGlyph(cat) { return (POI_SYMBOLS[cat]?.glyph || POI_SYMBOLS.default.glyph); }
// function poiColor(cat) { return (POI_COLORS[cat] || POI_COLORS.default); }

function formatFairnessBadgeValue(value) {
  return Number.isFinite(value) ? value.toFixed(FAIRNESS_BADGE_DECIMALS) : '—';
}

function normalizeCityKey(city) {
  return String(city || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
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
}

/* ======================= Helpers ======================= */
function colorForOSMFeature(props) {
  const cat = (props?.category || "").toLowerCase();
  if (cat && CATEGORY_COLORS[cat]) return CATEGORY_COLORS[cat];
  const raw = (props?.objekttyp || "").toLowerCase();
  if (raw && CATEGORY_COLORS[raw]) return CATEGORY_COLORS[raw];
  const shop = (props?.shop || "").toLowerCase();
  const amen = (props?.amenity || "").toLowerCase();
  if (shop === "supermarket" || amen === "supermarket") return CATEGORY_COLORS["supermarket"];
  if (shop) return CATEGORY_COLORS["retail"];
  if (["school","college","university","kindergarten"].includes(amen)) return CATEGORY_COLORS["education"];
  if (["hospital","clinic","doctors","dentist","pharmacy"].includes(amen)) return CATEGORY_COLORS["health"];
  return CATEGORY_COLORS["unknown"];
}
function baseBuildingColorForFeature() {
  // Keep the default city view visually clean: use one neutral color for all buildings.
  return DEFAULT_BUILDING_COLOR;
}
function mockBuildingFillColor(feature) {
  const props = feature?.properties || {};
  if (pinnedChangeId != null && !changeCompareBaseline) {
    if (Array.isArray(props._changeColor)) {
      const c = props._changeColor;
      return [c[0], c[1], c[2], c[3] ?? 220];
    }
    // Mock buildings that are new (__whatIfAdded) didn't exist in the "before"
    // snapshot, so their delta = entire current score (before was 0).
    // Compute the diverging RdBu change color on-the-fly to match the legend.
    if (props.__whatIfAdded && fairActive && Number.isFinite(props?.fair?.score)) {
      const delta = props.fair.score;
      const t = Math.max(0, Math.min(1, 0.5 + delta * 0.5));
      if (typeof d3 !== 'undefined' && typeof d3.interpolateRdBu === 'function') {
        const c = d3.color(d3.interpolateRdBu(t));
        if (c) return [Math.round(c.r), Math.round(c.g), Math.round(c.b), 220];
      }
      return [33, 102, 172, 220];
    }
    // In change map mode, dim unaffected mocked buildings like real buildings.
    return [90, 90, 90, 120];
  }
  if (props._drSelected) return DR_SELECTION_COLOR_DEFAULT;
  if (drHasSelection && (pinnedChangeId == null || changeCompareBaseline)) return DR_UNSELECTED_COLOR;
  if (fairActive && Number.isFinite(props?.fair?.score)) {
    return colorFromScore(props.fair.score);
  }
  return baseBuildingColorForFeature(feature);
}
const clampElev = (h) => (Number.isFinite(h) ? Math.max(12, Math.min(180, h)) : 14);

function getBuiltYear(props) {
  if (!props) return null;
  if (Number.isFinite(props.built_year)) return props.built_year;
  const candidates = ['built_year','year','Year','YEAR','byggnadsår','byggnadsar'];
  for (const k of candidates) {
    const v = props[k];
    if (v == null) continue;
    const m = String(v).match(/\b(19|20)\d{2}\b/);
    if (m) return parseInt(m[0], 10);
  }
  return null;
}
function prettyNum(n, digits=2) { return Number.isFinite(n) ? String(Number(n).toFixed(digits)) : '—'; }

function buildingTypeOf(f) {
  // Keep building type labels aligned with the fairness views.
  // We use the same category inference that powers "Fairness by building type"
  // so the dropdown values match the plot labels.
  const type = inferCategoryGroup(f?.properties);
  return type || '';
}

/* ======================= POPUP (concise + fairness) ======================= */
let map = null;
let overlay = null;
let currentPopup = null;
function closePopup() { if (currentPopup) { currentPopup.remove(); currentPopup = null; } }
function fmtValue(v) { return (v===null || v===undefined || v==='') ? '—' : String(v); }
function prettyPOIName(cat) {
  const dict = {
    __overall:'Overall',
    residential:'Residential',
    grocery:'Grocery', hospital:'Hospital', pharmacy:'Pharmacy', dentistry:'Dentistry',
    healthcare_center:'HC Center', veterinary:'Veterinary', university:'University',
    kindergarten:'Kindergarten', school_primary:'Primary.S',
    school_high:'High.S', mix:'Custom mix'
    };
return dict[cat] || cat;
}

function buildIfCityDebugReadout(props) {
  if (!props) return '';
  if (!props.__ifcity) {
    if (fairActive && fairCategory && props.fair) {
      return '<div class="small text-muted mt-2">Debug unavailable for this feature. Re-run fairness and click a building.</div>';
    }
    return '';
  }

  const fm = props.fair_multi || {};
  const lines = [];
  const mixEntries =
    fairActive && fairCategory === 'mix' && Array.isArray(selectedPOIMix) && selectedPOIMix.length
      ? selectedPOIMix
      : [];
  const activeEntries = mixEntries.length
    ? mixEntries
    : (fairActive && fairCategory && fairCategory !== 'mix')
      ? [{ cat: fairCategory, weight: 1 }]
      : Object.keys(fm).map((cat) => ({ cat, weight: 1 }));

  activeEntries.forEach(({ cat, weight }) => {
    const access = Number(fm?.[cat]?.access);
    if (!Number.isFinite(access)) return;
    const w = Number.isFinite(weight) ? weight : 1;
    lines.push(`${prettyPOIName(cat)}: raw=${access.toFixed(4)}, w=${w.toFixed(2)}, weighted=${(w * access).toFixed(4)}`);
  });

  const utility = Number(props.__ifcity.utility);
  const equityWeight = Number(props.__ifcity.equity_weight);
  const benefit = Number(props.__ifcity.benefit);
  const normalizedScore = Number(props?.fair?.score);

  lines.push(`utility(sum weighted raw)=${Number.isFinite(utility) ? utility.toFixed(4) : '—'}`);
  lines.push(`demand weight=${Number.isFinite(props.__ifcity?.demand_weight) ? props.__ifcity.demand_weight.toFixed(3) : '1.000'}`);
  lines.push(`benefit=max(0,utility-baseline)*equity=${Number.isFinite(benefit) ? benefit.toFixed(4) : '—'}`);
  lines.push(`normalized score=${Number.isFinite(normalizedScore) ? normalizedScore.toFixed(4) : '—'}`);

  return `
    <div class="mt-2 pt-2 border-top">
      <div class="small text-muted">IF-City debug values</div>
      <pre class="small mb-0 mt-1" style="white-space:pre-wrap;line-height:1.25;">${lines.join('\n')}</pre>
    </div>`;
}

function noteFeatureClick() {
  lastFeatureClickAt = Date.now();
}

function buildBuildingTypeOptions(selected) {
  return BUILDING_TYPE_ORDER.map(type => {
    const isSelected = type === selected ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${type}</option>`;
  }).join('');
}

function buildWhatIfTypeOptions(selected) {
  const types = ['residential', ...ALL_CATEGORIES];
  return types.map(type => {
    const isSelected = type === selected ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function buildWhatIfSuggestCategoryOptions(selected = []) {
  const selectedSet = new Set(selected);
  return ALL_CATEGORIES.map(type => {
    const isSelected = selectedSet.has(type) ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function buildWhatIfSuggestCategoryOptions(selected = []) {
  const selectedSet = new Set(selected);
  return ALL_CATEGORIES.map(type => {
    const isSelected = selectedSet.has(type) ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function buildWhatIfSuggestCategoryOptions(selected = []) {
  const selectedSet = new Set(selected);
  return ALL_CATEGORIES.map(type => {
    const isSelected = selectedSet.has(type) ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function setWhatIfMode(mode) {
  whatIfMode = mode || 'off';
  if (map?.getCanvas) {
    const cursor = whatIfMode === 'add' ? 'crosshair' : '';
    map.getCanvas().style.cursor = cursor;
  }
  if (whatIfMode === 'off') {
    closePopup();
  }
}

function setWhatIfType(nextType) {
  whatIfType = nextType || ALL_CATEGORIES[0] || 'grocery';
  if (whatIfTypeSelect) whatIfTypeSelect.value = whatIfType;
  if (whatIfSuggestCategoriesSelect && !getSelectedWhatIfCategories().length) {
    refreshWhatIfSuggestCategories([whatIfType]);
  }
  clearWhatIfSuggestions();
}

function getActiveWhatIfCategory() {
  if (whatIfType) return whatIfType;
  if (fairActive && fairCategory) {
    if (fairCategory === 'mix') {
      if (selectedPOIMix?.length) {
        return selectedPOIMix.reduce((best, entry) => {
          if (!best || entry.weight > best.weight) return entry;
          return best;
        }, null)?.cat;
      }
      return whatIfType;
    }
    return fairCategory;
  }
  return whatIfType;
}

function detectPOICategoryFromProps(props) {
  if (!props) return '';
  if (props.whatif_poi) return props.whatif_poi;
  for (const cat of ALL_CATEGORIES) {
    if (buildingMatchesPOI(props, cat)) return cat;
  }
  return '';
}

function rememberWhatIfOriginal(props) {
  if (!props || props.__whatIfOriginal) return;
  const keys = [
    'amenity', 'shop', 'healthcare', 'building', 'name', 'category', 'category_label',
    'school:level', 'education:level', 'level', 'isced:level', 'whatif_poi', 'objekttyp'
  ];
  const original = { values: {}, has: {} };
  keys.forEach((key) => {
    original.has[key] = Object.prototype.hasOwnProperty.call(props, key);
    original.values[key] = props[key];
  });
  props.__whatIfOriginal = original;
}

function restoreWhatIfOriginal(props) {
  if (!props?.__whatIfOriginal) return;
  const { values, has } = props.__whatIfOriginal;
  Object.keys(has).forEach((key) => {
    if (has[key]) {
      props[key] = values[key];
    } else {
      delete props[key];
    }
  });
  delete props.__whatIfOriginal;
  delete props.__whatIf;
  delete props.whatif_poi;
}

function resetWhatIfChanges() {
  if (!baseCityFC?.features?.length) return;
  const remaining = [];
  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (props.__whatIfAdded) {
      continue;
    }
    if (props.__whatIfOriginal) {
      restoreWhatIfOriginal(props);
    } else {
      delete props.__whatIf;
      delete props.whatif_poi;
    }
    remaining.push(feat);
  }
  baseCityFC.features = remaining;
  if (newbuildsFC?.features?.length) {
    newbuildsFC.features = newbuildsFC.features.filter((feat) => remaining.includes(feat));
  }
  refreshBuildingTypeDropdown();
  clearWhatIfSuggestions();
  clearWhatIfMockBuildings();
  updateLayers();
}

function applyPOITags(props, cat) {
  if (!props) return;
  rememberWhatIfOriginal(props);
  delete props.amenity;
  delete props.shop;
  delete props.healthcare;
  delete props['school:level'];
  delete props['education:level'];
  delete props.level;
  delete props['isced:level'];
  delete props.category_label;
  delete props.objekttyp;
  delete props.building;

  props.name = `What-if ${prettyPOIName(cat)}`;
  props.whatif_poi = cat;
  props.category = (() => {
    switch (cat) {
      case 'grocery':
        return 'supermarket';
      case 'hospital':
      case 'pharmacy':
      case 'dentistry':
      case 'healthcare_center':
      case 'veterinary':
        return 'health';
      case 'university':
      case 'kindergarten':
      case 'school_primary':
      case 'school_high':
        return 'education';
      default:
        return cat;
    }
  })();

  switch (cat) {
    case 'grocery':
      props.shop = 'supermarket';
      props.objekttyp = 'supermarket';
      break;
    case 'hospital':
      props.amenity = 'hospital';
      props.healthcare = 'hospital';
      props.objekttyp = 'hospital';
      break;
    case 'pharmacy':
      props.amenity = 'pharmacy';
      props.objekttyp = 'pharmacy';
      break;
    case 'dentistry':
      props.amenity = 'dentist';
      props.objekttyp = 'dentist';
      break;
    case 'healthcare_center':
      props.amenity = 'clinic';
      props.healthcare = 'clinic';
      props.objekttyp = 'clinic';
      break;
    case 'veterinary':
      props.amenity = 'veterinary';
      props.objekttyp = 'veterinary';
      break;
    case 'university':
      props.amenity = 'university';
      props.objekttyp = 'university';
      break;
    case 'kindergarten':
      props.amenity = 'kindergarten';
      props.objekttyp = 'kindergarten';
      break;
    case 'school_primary':
      props.amenity = 'school';
      props['isced:level'] = '1';
      props['school:level'] = 'primary';
      props.objekttyp = 'school';
      break;
    case 'school_high':
      props.amenity = 'school';
      props['isced:level'] = '3';
      props['school:level'] = 'upper';
      props.objekttyp = 'school';
      break;
    case 'residential':
      props.building = 'residential';
      props.category = 'residential';
      props.objekttyp = 'residential';
      delete props.whatif_poi;
      break;
    default:
      break;
  }
}

function buildPOIMapFromCurrent() {
  if (!currentPOIsFC?.features?.length) return null;
  const map = {};
  for (const feat of currentPOIsFC.features) {
    const cat = poiCategoryOf(feat);
    if (!map[cat]) map[cat] = [];
    let coords = null;
    if (feat.geometry?.type === 'Point') {
      coords = feat.geometry.coordinates;
    } else {
      try { coords = turf.centroid(feat).geometry.coordinates; } catch { coords = null; }
    }
    if (!coords || !Number.isFinite(coords[0]) || !Number.isFinite(coords[1])) continue;
    map[cat].push({ c: coords, name: feat.properties?.name || '(unnamed)' });
  }
  return map;
}

function collectWhatIfPOIsByCat(catList) {
  const result = {};
  if (!Array.isArray(catList) || !baseCityFC?.features?.length) return result;
  const cats = catList.filter(Boolean);
  if (!cats.length) return result;

  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (!props.__whatIf && !props.whatif_poi) continue;
    let coords = null;
    try { coords = turf.centroid(feat).geometry.coordinates; } catch { coords = null; }
    if (!coords || !Number.isFinite(coords[0]) || !Number.isFinite(coords[1])) continue;

    for (const cat of cats) {
      if (!buildingMatchesPOI(props, cat)) continue;
      if (!result[cat]) result[cat] = [];
      result[cat].push({
        c: coords,
        name: props.name || `What-if ${prettyPOIName(cat)}`
      });
    }
  }
  return result;
}

function getWhatIfChangedCentroids() {
  if (!baseCityFC?.features?.length) return [];
  const centroids = [];
  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (!props.__whatIf || props.__whatIfAdded) continue; // only changed, not added
    try { centroids.push(turf.centroid(feat).geometry.coordinates); } catch {}
  }
  return centroids;
}

function filterFetchedPOIsForWhatIf(features) {
  if (!Array.isArray(features) || !features.length) return features || [];
  const changed = getWhatIfChangedCentroids();
  if (!changed.length) return features;
  return features.filter(f => {
    const c = f.geometry?.coordinates;
    if (!c) return true;
    return !changed.some(cc => haversineMeters(c, cc) < 50);
  });
}

function collectBuildingPOIsByCat(catList, { includeWhatIf = true } = {}) {
  const result = {};
  if (!Array.isArray(catList) || !baseCityFC?.features?.length) return result;
  const cats = catList.filter(Boolean);
  if (!cats.length) return result;

  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (!includeWhatIf && (props.__whatIf || props.whatif_poi)) continue;
    let coords = null;
    try { coords = turf.centroid(feat).geometry.coordinates; } catch { coords = null; }
    if (!coords || !Number.isFinite(coords[0]) || !Number.isFinite(coords[1])) continue;

    for (const cat of cats) {
      if (!buildingMatchesPOI(props, cat)) continue;
      if (!result[cat]) result[cat] = [];
      result[cat].push({
        c: coords,
        name: props.name || `Building ${prettyPOIName(cat)}`
      });
    }
  }
  return result;
}

function syncWhatIfPOIs(catList) {
  if (!currentPOIsFC || !Array.isArray(catList)) return {};
  const cats = catList.filter(Boolean);
  if (!cats.length) return {};

  // Remove old what-if POI markers
  currentPOIsFC.features = (currentPOIsFC.features || []).filter(
    feat => !feat?.properties?.__whatif_poi
  );
  // Remove original fetched POIs that overlap with what-if changed buildings
  const changedCentroids = getWhatIfChangedCentroids();
  if (changedCentroids.length) {
    currentPOIsFC.features = currentPOIsFC.features.filter(feat => {
      const c = feat.geometry?.type === 'Point' ? feat.geometry.coordinates : null;
      if (!c) return true;
      return !changedCentroids.some(cc => haversineMeters(c, cc) < 50);
    });
  }

  const whatIfMap = collectWhatIfPOIsByCat(cats);
  Object.entries(whatIfMap).forEach(([cat, items]) => {
    items.forEach((item, idx) => {
      currentPOIsFC.features.push({
        type: 'Feature',
        properties: {
          name: item.name || `What-if ${prettyPOIName(cat)}`,
          __cat: cat,
          __whatif_poi: true,
          id: `whatif:${cat}:${idx}`
        },
        geometry: { type: 'Point', coordinates: item.c }
      });
    });
  });

  return whatIfMap;
}

async function recomputeFairnessSingleFromPOIs(cat, poiByCat) {
  const arr = poiByCat?.[cat] || [];
  if (!arr.length || !baseCityFC?.features?.length) return null;
  const scoresLack = [];
  const allowRouting = allowRoutingForFairness(arr.length);

  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    delete props.fair;
    props.fair_multi = {};

    let bestIdx = -1, bestD = Infinity;
    for (let i = 0; i < arr.length; i++) {
      const d = haversineMeters(cB, arr[i].c);
      if (d < bestD) { bestD = d; bestIdx = i; }
    }
    if (bestIdx < 0) continue;

    const nearest = arr[bestIdx];
    const metrics = await getTravelMetricsForPair(cB, nearest.c, bestD, allowRouting, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);

    props.fair = {
      cat,
      score,
      nearest_name: nearest.name,
      nearest_dist_m: metrics.meters,
      nearest_time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null,
      nearest_lonlat: nearest.c
    };
    props.fair_multi[cat] = {
      score,
      dist_m: metrics.meters,
      time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null
    };
    scoresLack.push(1 - score);
  }

  const G = gini(scoresLack);
  fairActive = true;
  fairCategory = cat;
  window.activePOICats = new Set([cat]);
  fairRecolorTick++;
  updateLayers();

  if (giniOut) giniOut.textContent = `${prettyPOIName(cat)} Gini: ${formatFairnessBadgeValue(G)}`;
  const summary = summarizeFairnessCurrent();
  showSidePanel(cat, G, arr.length, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  return { gini: G, poiCount: arr.length };
}

async function recomputeFairnessMixFromPOIs(mix, poiByCat) {
  if (!mix.length || !baseCityFC?.features?.length) return null;
  const scoresLack = [];
  const maxPois = Math.max(0, ...mix.map(({ cat }) => poiByCat?.[cat]?.length || 0));
  const allowRouting = allowRoutingForFairness(maxPois);

  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    const fm = {};
    delete props.fair;

    for (const {cat} of mix) {
      const arr = poiByCat?.[cat] || [];
      if (!arr.length) continue;
      let bestD = Infinity, bestIdx = -1;
      for (let i = 0; i < arr.length; i++) {
        const d = haversineMeters(cB, arr[i].c);
        if (d < bestD) { bestD = d; bestIdx = i; }
      }
      if (bestIdx < 0) continue;
      const nearest = arr[bestIdx];
      const metrics = await getTravelMetricsForPair(cB, nearest.c, bestD, allowRouting, fairnessTravelMode);
      const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);
      fm[cat] = {
        score,
        dist_m: metrics.meters,
        time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null,
        nearest
      };
    }
    const entries = mix
      .map(({cat, weight}) => ({ w: Math.max(0, weight), s: fm[cat]?.score }))
      .filter(e => Number.isFinite(e.s) && e.w > 0);

    if (entries.length) {
      const best = Math.max(...entries.map(entry => entry.s));
      props.fair = { cat: 'mix', score: best };
      scoresLack.push(1 - best);
    } else {
      delete props.fair;
    }

    props.fair_multi = fm;
  }

  const G = gini(scoresLack);
  fairActive = true;
  fairCategory = 'mix';
  window.activePOICats = new Set(mix.map(m => m.cat));
  fairRecolorTick++;
  updateLayers();

  if (giniOut) giniOut.textContent = `Mix Gini: ${formatFairnessBadgeValue(G)}`;
  const summary = summarizeFairnessCurrent();
  showSidePanel('mix', G, currentPOIsFC?.features?.length || 0, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  return { gini: G, poiCount: currentPOIsFC?.features?.length || 0 };
}


async function recomputeFairnessAfterWhatIf() {
  try {
    showGlobalSpinner('Recomputing fairness…');
    const result = {
      category: fairCategory || null,
      categoryGini: null,
      overallGini: null
    };
    if (fairActive && fairCategory) {
      const cats = fairCategory === 'mix'
        ? selectedPOIMix.map(m => m.cat)
        : [fairCategory];
      if (fairnessModel === 'ifcity') {
        const weightsByCat = fairCategory === 'mix'
          ? Object.fromEntries(selectedPOIMix.map(({ cat, weight }) => [cat, weight]))
          : { [fairCategory]: 1 };
        const ifCityRes = await computeIfCityFairness(cats, weightsByCat);
        result.categoryGini = Number.isFinite(ifCityRes?.inequality) ? ifCityRes.inequality : null;
        if (giniOut && Number.isFinite(result.categoryGini)) {
          const label = fairCategory === 'mix'
            ? 'Mix'
            : prettyPOIName(fairCategory);
          giniOut.textContent = `${label} GE(α=2): ${formatFairnessBadgeValue(result.categoryGini)}`;
        }
      } else if (fairCategory === 'mix' && selectedPOIMix.length) {
        const res = await computeFairnessWeighted(selectedPOIMix);
        result.categoryGini = res?.gini ?? null;
      } else {
        const res = await computeFairnessFast(fairCategory);
        result.categoryGini = res?.gini ?? null;
      }
    }
    const overallRes = await autoComputeOverall();
    result.overallGini = overallRes?.overall_gini ?? null;

    // Restore category Gini display — autoComputeOverall overwrites
    // props.__ifcity with all-categories values, which can cause
    // side effects that clobber giniOut with the overall value
    if (giniOut && Number.isFinite(result.categoryGini)) {
      const label = fairCategory === 'mix'
        ? 'Mix'
        : prettyPOIName(fairCategory);
      const prefix = fairnessModel === 'ifcity' ? 'GE(α=2)' : 'Gini';
      giniOut.textContent = `${label} ${prefix}: ${formatFairnessBadgeValue(result.categoryGini)}`;
    }

    hideGlobalSpinner();
    // --- Refresh DR and Parallel Coordinates so mock buildings are included ---
    if (drPlot.points) {
      // DR is active → re-run it (collectDRData reads baseCityFC.features fresh,
      // which now includes mock buildings with their fairness scores).
      // runDR also updates drPlot.sample, which PC uses in building mode.
      runDR();
    } else if (parallelCoordsOpen) {
      // DR not active, but PC is open → refresh PC directly.
      // Without a stale drPlot.sample, getParallelCoordsDataset falls back
      // to baseCityFC.features (which includes mock buildings).
      updateParallelCoordsPanel();
    }

    return result;
  } catch (err) {
    hideGlobalSpinner();
    console.warn('What-if recompute failed', err);
    return { category: fairCategory || null, categoryGini: null, overallGini: null };
  }
}

async function applyBuildingTypeChange(feature, nextType) {
  if (!feature) return;
  showGlobalSpinner('Changing building type…');
  await waitForSpinnerPaint();
  const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  const scoreSnapshot = captureScoreSnapshot();
  const props = feature.properties || (feature.properties = {});
  const nextCat = nextType || whatIfType;
  applyPOITags(props, nextCat);
  props.__whatIf = true;
  refreshBuildingTypeDropdown();
  updateLayers();

  // Re-show popup so user sees the updated building info
  let coord = null;
  try { coord = turf.centroid(feature).geometry.coordinates; } catch { coord = null; }
  if (coord) showPopup(feature, coord);

  const result = await recomputeFairnessAfterWhatIf();
  const afterCatGini = Number.isFinite(result?.categoryGini)
    ? result.categoryGini
    : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(result?.overallGini)
    ? result.overallGini
    : (Number.isFinite(overallGini) ? overallGini : null);
  const nextId = changeLogIdCounter;
  const colorPairs = applyDeltaColorsFromSnapshot(scoreSnapshot, nextId);
  recordWhatIfChange({
    action: 'change_type',
    description: `Changed building → ${prettyPOIName(nextCat)}`,
    category: nextCat,
    beforeGini: beforeCatGini,
    afterGini: afterCatGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: colorPairs
  });
  hideGlobalSpinner();
}

function collectPOICoordsForCategory(cat, fetchedPOIs) {
  const filtered = filterFetchedPOIsForWhatIf(fetchedPOIs?.features || []);
  const coords = filtered.map((p) => ({
    c: p.geometry.coordinates,
    name: p.properties?.name || '(unnamed)'
  }));
  const whatIfMap = collectWhatIfPOIsByCat([cat]);
  if (whatIfMap?.[cat]?.length) coords.push(...whatIfMap[cat]);
  const buildingPOIs = collectBuildingPOIsByCat([cat], { includeWhatIf: true });
  if (buildingPOIs?.[cat]?.length) coords.push(...buildingPOIs[cat]);
  return coords;
}

function computeBuildingDataForCategory(cat, poiCoords) {
  const data = [];
  if (!baseCityFC?.features?.length) return data;
  const cityCenter = getCityCenterCoord();
  for (const [idx, feat] of baseCityFC.features.entries()) {
    const props = feat.properties || {};
    const centroid = turf.centroid(feat).geometry.coordinates;
    const cityDistKm = cityCenter ? haversineMeters(centroid, cityCenter) / 1000 : null;
    let bestD = Infinity;
    for (let i = 0; i < poiCoords.length; i++) {
      const d = haversineMeters(centroid, poiCoords[i].c);
      if (d < bestD) bestD = d;
    }
    const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
    const score = Number.isFinite(bestD) ? scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode) : 0;
    data.push({
      idx,
      feature: feat,
      centroid,
      cityDistKm,
      bestD,
      score,
      isMatch: buildingMatchesPOI(props, cat),
      name: props.name || props.category || props.objekttyp || `Building #${idx + 1}`
    });
  }
  return data;
}

function giniFromBuildingScores(rows) {
  const lacks = rows.map(row => 1 - row.score);
  return gini(lacks);
}

function giniFromOverallRows(rows) {
  const lacks = rows.map(row => 1 - row.overallScore);
  return gini(lacks);
}

function computeBuildingDataForOverall(categories, catToCoords) {
  const rows = [];
  if (!baseCityFC?.features?.length) return rows;
  const cityCenter = getCityCenterCoord();
  for (const [idx, feat] of baseCityFC.features.entries()) {
    const props = feat.properties || {};
    const centroid = turf.centroid(feat).geometry.coordinates;
    const cityDistKm = cityCenter ? haversineMeters(centroid, cityCenter) / 1000 : null;
    const scoreByCat = {};
    const bestByCat = {};
    const isMatchByCat = {};
    categories.forEach((cat) => {
      const coords = catToCoords[cat] || [];
      let bestD = Infinity;
      for (let i = 0; i < coords.length; i++) {
        const d = haversineMeters(centroid, coords[i].c);
        if (d < bestD) bestD = d;
      }
      bestByCat[cat] = bestD;
      const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
      scoreByCat[cat] = Number.isFinite(bestD) ? scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode) : 0;
      isMatchByCat[cat] = buildingMatchesPOI(props, cat);
    });
    const scores = categories.map(cat => scoreByCat[cat]).filter(Number.isFinite);
    const overallScore = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
    rows.push({
      idx,
      feature: feat,
      centroid,
      cityDistKm,
      bestByCat,
      scoreByCat,
      isMatchByCat,
      overallScore,
      score: overallScore,
      name: props.name || props.category || props.objekttyp || `Building #${idx + 1}`
    });
  }
  return rows;
}

async function computeOverallSuggestionState(categories) {
  const unique = Array.from(new Set(categories.filter(Boolean)));
  const fetched = await Promise.all(
    unique.map(cat => fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] })))
  );
  const catToCoords = {};
  unique.forEach((cat, idx) => {
    catToCoords[cat] = collectPOICoordsForCategory(cat, fetched[idx]);
  });
  const rows = computeBuildingDataForOverall(unique, catToCoords);
  if (!rows.length) {
    throw new Error('No buildings available to score.');
  }
  return {
    rows,
    categories: unique,
    validCats: unique,
    baselineGini: giniFromOverallRows(rows),
    thresholds: getCityAreaThresholds(rows)
  };
}

function giniWithOverallCandidate(cat, rows, categories, candidateCoord) {
  const lacks = rows.map((row) => {
    const updatedScores = categories.map((category) => {
      const currentBest = row.bestByCat[category] ?? Infinity;
      if (category !== cat) return row.scoreByCat[category];
      const d = haversineMeters(row.centroid, candidateCoord);
      const bestD = Math.min(currentBest, d);
      const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
      return Number.isFinite(bestD) ? scoreFromTimeSeconds(category, timeSeconds, fairnessTravelMode) : 0;
    });
    const avgScore = updatedScores.length
      ? updatedScores.reduce((a, b) => a + b, 0) / updatedScores.length
      : 0;
    return 1 - avgScore;
  });
  return gini(lacks);
}

function applyOverallCandidate(cat, rows, categories, candidateCoord) {
  rows.forEach((row) => {
    const currentBest = row.bestByCat[cat] ?? Infinity;
    const d = haversineMeters(row.centroid, candidateCoord);
    if (d < currentBest) {
      row.bestByCat[cat] = d;
      const timeSeconds = estimateTravelTimeSecondsFromMeters(d, fairnessTravelMode);
      row.scoreByCat[cat] = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
    }
    const scores = categories.map(category => row.scoreByCat[category]).filter(Number.isFinite);
    row.overallScore = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
    row.score = row.overallScore;
  });
}

function findBestOverallCandidate({
  cat,
  kind,
  bbox,
  center,
  radiusKm,
  areaFocus,
  overallState,
  usedIds
}) {
  const { rows, categories, baselineGini, thresholds } = overallState;
  const candidates = pickCandidateRows(rows, {
    kind,
    bbox,
    usedIds,
    center,
    radiusKm,
    areaFocus,
    thresholds,
    cat
  });
  if (!candidates.length) return null;
  let best = null;
  for (const candidate of candidates) {
    const g = giniWithOverallCandidate(cat, rows, categories, candidate.centroid);
    if (!best || g < best.giniAfter) {
      best = { candidate, giniAfter: g };
    }
  }
  if (!best) return null;
  return {
    kind,
    cat,
    giniBefore: baselineGini,
    giniAfter: best.giniAfter,
    improvement: baselineGini - best.giniAfter,
    location: best.candidate.centroid,
    candidate: best.candidate
  };
}

function giniWithCandidate(cat, rows, candidateCoord) {
  const lacks = rows.map((row) => {
    const d = haversineMeters(row.centroid, candidateCoord);
    const bestD = Math.min(row.bestD, d);
    const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
    return 1 - score;
  });
  return gini(lacks);
}

function applyCandidateToRows(cat, rows, candidateCoord) {
  rows.forEach((row) => {
    const d = haversineMeters(row.centroid, candidateCoord);
    if (d < row.bestD) {
      row.bestD = d;
      const timeSeconds = estimateTravelTimeSecondsFromMeters(d, fairnessTravelMode);
      row.score = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
    }
  });
}

function ifCityCandidateContribution(cat, fromCoord, toCoord) {
  const rho = ifCityPriorityWeight(cat);
  const kappa = ifCityKappa(cat);
  const linearKm = haversineMeters(fromCoord, toCoord) / 1000;
  const dKm = ifCityDistanceForMode(linearKm, fairnessTravelMode);
  return rho * Math.exp(-kappa * dKm);
}

function buildIfCitySuggestionRows(categories, catToPOI, weightsByCat = {}) {
  const rows = [];
  if (!baseCityFC?.features?.length) return rows;
  const cityCenter = getCityCenterCoord();

  for (const [idx, feat] of baseCityFC.features.entries()) {
    const props = feat.properties || {};
    const centroid = turf.centroid(feat).geometry.coordinates;
    const cityDistKm = cityCenter ? haversineMeters(centroid, cityCenter) / 1000 : null;
    const accessByCat = {};
    const isMatchByCat = {};

    for (const cat of categories) {
      const arr = catToPOI[cat] || [];
      let access = 0;
      for (const poi of arr) {
        const v = Number.isFinite(poi?.v) ? poi.v : 1;
        access += v * ifCityCandidateContribution(cat, centroid, poi.c);
      }
      accessByCat[cat] = access;
      isMatchByCat[cat] = buildingMatchesPOI(props, cat);
    }

    let utility = 0;
    for (const cat of categories) {
      const weight = Number.isFinite(weightsByCat[cat]) ? weightsByCat[cat] : 1;
      utility += weight * (accessByCat[cat] || 0);
    }

    const equityWeight = ifCityEquityWeightForFeature(feat);
    const benefit = Math.max(0, utility - IF_CITY_BASELINE_UTILITY) * equityWeight;

    rows.push({
      idx,
      feature: feat,
      centroid,
      cityDistKm,
      accessByCat,
      isMatchByCat,
      utility,
      benefit,
      score: 0,
      name: props.name || props.category || props.objekttyp || `Building #${idx + 1}`
    });
  }

  const normalized = normalizeBenefitsToScores(rows.map(r => r.benefit));
  rows.forEach((row, i) => {
    row.score = normalized[i] ?? 0;
  });
  return rows;
}

function generalizedEntropyWithIfCityCandidate(cat, rows, categories, candidateCoord, weightsByCat = {}) {
  const benefits = rows.map((row) => {
    let utility = 0;
    for (const category of categories) {
      let access = row.accessByCat[category] || 0;
      if (category === cat) {
        access += ifCityCandidateContribution(category, row.centroid, candidateCoord);
      }
      const weight = Number.isFinite(weightsByCat[category]) ? weightsByCat[category] : 1;
      utility += weight * access;
    }
    const equityWeight = ifCityEquityWeightForFeature(row.feature);
    return Math.max(0, utility - IF_CITY_BASELINE_UTILITY) * equityWeight;
  });
  return generalizedEntropy(benefits, IF_CITY_ALPHA);
}

function applyIfCityCandidate(cat, rows, categories, candidateCoord, weightsByCat = {}) {
  rows.forEach((row) => {
    row.accessByCat[cat] = (row.accessByCat[cat] || 0) + ifCityCandidateContribution(cat, row.centroid, candidateCoord);
    let utility = 0;
    for (const category of categories) {
      const weight = Number.isFinite(weightsByCat[category]) ? weightsByCat[category] : 1;
      utility += weight * (row.accessByCat[category] || 0);
    }
    row.utility = utility;
    const equityWeight = ifCityEquityWeightForFeature(row.feature);
    row.benefit = Math.max(0, utility - IF_CITY_BASELINE_UTILITY) * equityWeight;
  });
  const normalized = normalizeBenefitsToScores(rows.map(r => r.benefit));
  rows.forEach((row, i) => {
    row.score = normalized[i] ?? 0;
  });
}

async function computeIfCitySuggestionState(categories, weightsByCat = {}) {
  const unique = Array.from(new Set((categories || []).filter(Boolean)));
  const fetched = await Promise.all(
    unique.map(cat => fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] })))
  );
  const catToPOI = {};
  unique.forEach((cat, idx) => {
    const filteredFeats = filterFetchedPOIsForWhatIf(fetched[idx]?.features || []);
    const fetchedRows = filteredFeats.map((feat) => ({
      c: feat.geometry.coordinates,
      name: feat.properties?.name || '(unnamed)',
      v: ifCityOpportunityWeightFromPOI(cat, feat)
    }));
    const whatIfRows = collectPOICoordsForCategory(cat, fetched[idx]).map((item) => ({ ...item, v: 1 }));
    catToPOI[cat] = dedupeIFCityPOIs([...(fetchedRows || []), ...(whatIfRows || [])]);
  });

  const rows = buildIfCitySuggestionRows(unique, catToPOI, weightsByCat);
  if (!rows.length) {
    throw new Error('No buildings available to score.');
  }
  return {
    rows,
    categories: unique,
    validCats: unique.filter((cat) => (catToPOI[cat] || []).length > 0),
    baselineInequality: generalizedEntropy(rows.map(r => r.benefit), IF_CITY_ALPHA),
    thresholds: getCityAreaThresholds(rows),
    weightsByCat
  };
}

function getMapBoundsBBox() {
  if (!map?.getBounds) return null;
  const bounds = map.getBounds();
  return [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()];
}

function getMapCenter() {
  if (!map?.getCenter) return null;
  const center = map.getCenter();
  return [center.lng, center.lat];
}

function getCityCenterCoord() {
  if (baseCityFC?.features?.length) {
    try {
      return turf.centroid(baseCityFC).geometry.coordinates;
    } catch {
      return null;
    }
  }
  return getMapCenter();
}

function getCityAreaThresholds(rows) {
  const distances = rows.map(row => row.cityDistKm).filter(Number.isFinite).sort((a, b) => a - b);
  if (!distances.length) {
    return { centerMaxKm: Infinity, outskirtsMinKm: Infinity };
  }
  return {
    centerMaxKm: quantileSorted(distances, 0.4),
    outskirtsMinKm: quantileSorted(distances, 0.7)
  };
}

function boundsDiagonalMeters(bounds) {
  if (!bounds) return null;
  const [minX, minY, maxX, maxY] = bounds;
  if (![minX, minY, maxX, maxY].every(Number.isFinite)) return null;
  return haversineMeters([minX, minY], [maxX, maxY]);
}

function getSuggestionMinDistanceMeters(bbox) {
  let cityBounds = null;
  if (baseCityFC?.features?.length) {
    try {
      const [minX, minY, maxX, maxY] = turf.bbox(baseCityFC);
      cityBounds = [minX, minY, maxX, maxY];
    } catch {
      cityBounds = null;
    }
  }
  const cityDiag = boundsDiagonalMeters(cityBounds);
  const viewDiag = boundsDiagonalMeters(bbox);
  const diag = Math.max(cityDiag || 0, viewDiag || 0);
  if (!diag) return 500;
  return Math.max(400, Math.min(2200, diag * 0.1));
}

function isFarFromUsed(coord, usedCoords, minDistanceMeters) {
  if (!usedCoords?.length) return true;
  return usedCoords.every((used) => haversineMeters(coord, used) >= minDistanceMeters);
}

function distanceToNearestUsed(coord, usedCoords) {
  if (!usedCoords?.length) return Infinity;
  let best = Infinity;
  for (const used of usedCoords) {
    const d = haversineMeters(coord, used);
    if (d < best) best = d;
  }
  return best;
}

function isWithinBBox(coord, bbox) {
  if (!bbox || !coord) return true;
  const [minX, minY, maxX, maxY] = bbox;
  return coord[0] >= minX && coord[0] <= maxX && coord[1] >= minY && coord[1] <= maxY;
}

function isWithinRadius(coord, center, radiusKm) {
  if (!center || !coord || !Number.isFinite(radiusKm) || radiusKm <= 0) return true;
  const distMeters = haversineMeters(coord, center);
  return distMeters <= radiusKm * 1000;
}

function pickCandidateRows(rows, {
  kind,
  bbox,
  usedIds,
  center,
  radiusKm,
  areaFocus,
  thresholds,
  cat,
  usedCoords,
  minSpacingMeters,
  limit = WHATIF_SUGGESTION_LIMIT,
  excludePOIBuildings = true
}) {
  return rows
    .filter(row => {
      if (kind !== 'change') return true;
      if (row.isMatchByCat && cat) return !row.isMatchByCat[cat];
      return !row.isMatch;
    })
    .filter(row => !excludePOIBuildings || !row.isMatch)
    .filter(row => isWithinBBox(row.centroid, bbox))
    .filter(row => isWithinRadius(row.centroid, center, radiusKm))
    .filter(row => {
      if (!areaFocus || areaFocus === 'any') return true;
      const dist = row.cityDistKm;
      if (!Number.isFinite(dist)) return true;
      if (areaFocus === 'center') return dist <= (thresholds?.centerMaxKm ?? Infinity);
      if (areaFocus === 'outskirts') return dist >= (thresholds?.outskirtsMinKm ?? Infinity);
      return true;
    })
    .filter(row => !usedIds.has(row.idx))
    .filter(row => isFarFromUsed(row.centroid, usedCoords, minSpacingMeters))
    .sort((a, b) => a.score - b.score)
    .slice(0, Number.isFinite(limit) ? limit : rows.length);
}

async function computeWhatIfSuggestionsForCategory({ cat, kind, count, bbox, center, radiusKm, areaFocus }) {
  const suggestions = [];
  const usedIds = new Set();
  const usedCoords = [];
  const minSpacingMeters = getSuggestionMinDistanceMeters(bbox);
  const exactMode = isExactCityWideSingleCandidateMode({ count, bbox, center, radiusKm, areaFocus });

  if (fairnessModel === 'ifcity') {
    const state = await computeIfCitySuggestionState([cat], { [cat]: 1 });
    const { rows, categories, baselineInequality, thresholds } = state;

    for (let n = 0; n < count; n += 1) {
      const candidates = pickCandidateRows(rows, {
        kind,
        bbox,
        usedIds,
        center,
        radiusKm,
        areaFocus,
        thresholds,
        usedCoords,
        minSpacingMeters,
        cat,
        limit: exactMode ? Number.POSITIVE_INFINITY : WHATIF_SUGGESTION_LIMIT
      });
      if (!candidates.length) break;

      let best = null;
      const metricEps = 0.0005;
      for (const candidate of candidates) {
        const inequality = generalizedEntropyWithIfCityCandidate(cat, rows, categories, candidate.centroid, { [cat]: 1 });
        if (!best || inequality < best.inequality - metricEps) {
          best = { candidate, inequality, dist: distanceToNearestUsed(candidate.centroid, usedCoords) };
          continue;
        }
        if (best && Math.abs(inequality - best.inequality) <= metricEps) {
          const dist = distanceToNearestUsed(candidate.centroid, usedCoords);
          if (dist > best.dist) best = { candidate, inequality, dist };
        }
      }
      if (!best) break;

      applyIfCityCandidate(cat, rows, categories, best.candidate.centroid, { [cat]: 1 });
      usedIds.add(best.candidate.idx);
      usedCoords.push(best.candidate.centroid);
      suggestions.push({
        kind,
        cat,
        giniBefore: baselineInequality,
        giniAfter: best.inequality,
        improvement: baselineInequality - best.inequality,
        location: best.candidate.centroid,
        candidate: best.candidate
      });
    }

    return suggestions;
  }

  const fetched = await fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] }));
  const poiCoords = collectPOICoordsForCategory(cat, fetched);
  const buildingRows = computeBuildingDataForCategory(cat, poiCoords);
  if (!buildingRows.length) {
    throw new Error('No buildings available to score.');
  }
  const baselineGini = giniFromBuildingScores(buildingRows);
  const thresholds = getCityAreaThresholds(buildingRows);

  for (let n = 0; n < count; n += 1) {
    const candidates = pickCandidateRows(buildingRows, {
      kind,
      bbox,
      usedIds,
      center,
      radiusKm,
      areaFocus,
      thresholds,
      usedCoords,
      minSpacingMeters,
      limit: exactMode ? Number.POSITIVE_INFINITY : WHATIF_SUGGESTION_LIMIT
    });
    if (!candidates.length) break;

    let best = null;
    const giniEps = 0.0005;
    for (const candidate of candidates) {
      const g = giniWithCandidate(cat, buildingRows, candidate.centroid);
      if (!best || g < best.gini - giniEps) {
        best = { candidate, gini: g, dist: distanceToNearestUsed(candidate.centroid, usedCoords) };
        continue;
      }
      if (best && Math.abs(g - best.gini) <= giniEps) {
        const dist = distanceToNearestUsed(candidate.centroid, usedCoords);
        if (dist > best.dist) {
          best = { candidate, gini: g, dist };
        }
      }
    }
    if (!best) break;

    applyCandidateToRows(cat, buildingRows, best.candidate.centroid);
    usedIds.add(best.candidate.idx);
    usedCoords.push(best.candidate.centroid);
    suggestions.push({
      kind,
      cat,
      giniBefore: baselineGini,
      giniAfter: best.gini,
      improvement: baselineGini - best.gini,
      location: best.candidate.centroid,
      candidate: best.candidate
    });
  }

  return suggestions;
}

async function computeWhatIfSuggestions({
  categories,
  kind,
  count,
  bbox,
  center,
  radiusKm,
  fairnessTarget,
  fairnessCategories,
  areaFocus
}) {
  if (!baseCityFC?.features?.length) {
    throw new Error('Load buildings before running suggestions.');
  }
  const target = fairnessTarget === 'overall' ? 'overall' : 'category';
  const results = [];
  const categoryList = Array.isArray(categories) ? categories : [];
  if (!categoryList.length) {
    throw new Error('Select at least one POI category to suggest.');
  }
  if (target === 'overall') {
    const overallCats = (fairnessCategories && fairnessCategories.length)
      ? fairnessCategories
      : ALL_CATEGORIES;
    const usedIds = new Set();
    const usedCoords = [];
    const minSpacingMeters = getSuggestionMinDistanceMeters(bbox);

    if (fairnessModel === 'ifcity') {
      const weightsByCat = Object.fromEntries(overallCats.map((cat) => [cat, 1]));
      const overallState = await computeIfCitySuggestionState(overallCats, weightsByCat);
      const { rows, categories, validCats, baselineInequality, thresholds } = overallState;
      for (let n = 0; n < count; n += 1) {
        let bestPick = null;
        for (const cat of categoryList) {
          if (!validCats.includes(cat)) continue;
          const candidates = pickCandidateRows(rows, {
            kind,
            bbox,
            usedIds,
            center,
            radiusKm,
            areaFocus,
            thresholds,
            cat,
            usedCoords,
            minSpacingMeters
          });
          if (!candidates.length) continue;
          for (const candidate of candidates) {
            const inequality = generalizedEntropyWithIfCityCandidate(cat, rows, categories, candidate.centroid, weightsByCat);
            if (!bestPick || inequality < bestPick.giniAfter) {
              bestPick = {
                kind,
                cat,
                giniBefore: baselineInequality,
                giniAfter: inequality,
                improvement: baselineInequality - inequality,
                location: candidate.centroid,
                candidate
              };
            }
          }
        }
        if (!bestPick) break;
        applyIfCityCandidate(bestPick.cat, rows, categories, bestPick.candidate.centroid, weightsByCat);
        usedIds.add(bestPick.candidate.idx);
        usedCoords.push(bestPick.candidate.centroid);
        results.push(bestPick);
      }
    } else {
      const overallState = await computeOverallSuggestionState(overallCats);
      for (let n = 0; n < count; n += 1) {
        let bestPick = null;
        for (const cat of categoryList) {
          const pick = findBestOverallCandidate({
            cat,
            kind,
            bbox,
            center,
            radiusKm,
            areaFocus,
            overallState,
            usedIds,
            usedCoords,
            minSpacingMeters
          });
          if (pick && (!bestPick || pick.giniAfter < bestPick.giniAfter)) {
            bestPick = pick;
          }
        }
        if (!bestPick) break;
        applyOverallCandidate(bestPick.cat, overallState.rows, overallState.validCats, bestPick.candidate.centroid);
        usedIds.add(bestPick.candidate.idx);
        usedCoords.push(bestPick.candidate.centroid);
        results.push(bestPick);
      }
    }
  } else {
    for (const cat of categoryList) {
      const batch = await computeWhatIfSuggestionsForCategory({
        cat,
        kind,
        count,
        bbox,
        center,
        radiusKm,
        areaFocus
      });
      results.push(...batch);
    }
  }
  return results;
}

function setWhatIfSuggestions(list) {
  whatIfSuggestions = Array.isArray(list) ? list : [];
  whatIfSuggestionTick++;
  updateLayers();
}

function clearWhatIfSuggestions() {
  whatIfSuggestions = [];
  whatIfLastSuggestionConfig = null;
  whatIfSuggestionTick++;
  updateLayers();
  if (whatIfSuggestionOut) {
    whatIfSuggestionOut.textContent = 'Suggestions cleared.';
    whatIfSuggestionOut.classList.remove('text-danger');
    whatIfSuggestionOut.classList.add('text-muted');
  }
  if (whatIfApplySuggestionBtn) whatIfApplySuggestionBtn.disabled = true;
  if (whatIfClearSuggestionBtn) whatIfClearSuggestionBtn.disabled = true;
  if (whatIfVerifySuggestionBtn) whatIfVerifySuggestionBtn.disabled = true;
}

function cloneRowsForWhatIf(rows, fairnessMode) {
  if (fairnessMode === 'ifcity') {
    return rows.map((row) => ({
      ...row,
      accessByCat: { ...(row.accessByCat || {}) },
      isMatchByCat: { ...(row.isMatchByCat || {}) }
    }));
  }
  return rows.map((row) => ({
    ...row,
    bestByCat: row.bestByCat ? { ...row.bestByCat } : undefined,
    scoreByCat: row.scoreByCat ? { ...row.scoreByCat } : undefined,
    isMatchByCat: row.isMatchByCat ? { ...row.isMatchByCat } : undefined
  }));
}

function formatObjectiveValue(value) {
  if (!Number.isFinite(value)) return 'n/a';
  return value.toFixed(5);
}

function combinationCountBigInt(n, k) {
  if (!Number.isInteger(n) || !Number.isInteger(k) || n < 0 || k < 0 || k > n) return 0n;
  const kk = Math.min(k, n - k);
  let result = 1n;
  for (let i = 1; i <= kk; i += 1) {
    result = (result * BigInt(n - kk + i)) / BigInt(i);
  }
  return result;
}

function formatBigInt(value) {
  if (typeof value !== 'bigint') return String(value ?? '0');
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function summarizeVerificationAudit(item) {
  const modeLabel = item.exhaustiveWithinPool ? 'full search' : 'partial search';
  const capNote = item.poolWasCapped
    ? ` Candidate pool capped at ${item.poolCap} from ${item.poolOriginalSize}.`
    : '';
  const guaranteeNote = item.globalGuarantee
    ? ' Global guarantee: exact optimum over all feasible candidates under current filters.'
    : (item.exhaustiveWithinPool
      ? ' Guarantee scope: exact optimum over searched pool.'
      : ' No global guarantee: bounded/partial enumeration.');
  const methodNote = " Candidate generation may be greedy/heuristic; 'best' is computed by enumerating feasible combinations in the verifier.";
  const planningNote = ' Use as planning support, not a replacement for professional urban planning judgement.';
  return `${item.metricName}: candidate=${formatObjectiveValue(item.candidateObjective)}, best=${formatObjectiveValue(item.bestObjective)}, gap=${formatObjectiveValue(item.gap)}, k=${item.count}, pool=${item.poolSize}, combos=${item.combosChecked}/${item.totalCombos}${item.totalCombosExact ? '' : '+'}, ${modeLabel}. best = min objective over feasible ${item.count}-combinations in pool.${capNote}${guaranteeNote}${methodNote}${planningNote}`;
}

async function verifyWhatIfSuggestionsOptimality(config, suggestions) {
  const currentSuggestions = Array.isArray(suggestions) ? suggestions : [];
  if (!currentSuggestions.length) throw new Error('Generate suggestions first.');
  if (!config) throw new Error('No suggestion parameters found. Generate suggestions again.');

  const categories = Array.isArray(config.categories) ? config.categories.filter(Boolean) : [];
  if (!categories.length) throw new Error('No categories to verify.');

  const fairnessTarget = config.fairnessTarget === 'overall' ? 'overall' : 'category';
  const kind = config.kind === 'change' ? 'change' : 'add';
  const bbox = config.bbox || null;
  const center = config.center || null;
  const radiusKm = Number.isFinite(config.radiusKm) ? config.radiusKm : 0;
  const areaFocus = config.areaFocus || 'any';
  const requestedCount = Math.max(1, Number.isFinite(config.count) ? config.count : currentSuggestions.length);
  const minSpacingMeters = getSuggestionMinDistanceMeters(bbox);
  const exactMode = requestedCount === 1 && categories.length === 1;
  const maxCombos = exactMode ? Number.MAX_SAFE_INTEGER : 50000;
  const maxPoolSize = exactMode ? Number.POSITIVE_INFINITY : 120;
  let combosChecked = 0;
  let truncated = false;

  const overallCats = (config.fairnessCategories && config.fairnessCategories.length)
    ? config.fairnessCategories
    : categories;

  const verifyTask = async ({
    rows,
    thresholds,
    categoriesForState,
    catChoices,
    evalObjective,
    applyCandidate,
    relevantSuggestions
  }) => {
    const suggestionsForTask = Array.isArray(relevantSuggestions) ? relevantSuggestions : [];
    const count = Math.min(requestedCount, Math.max(1, suggestionsForTask.length || requestedCount));

    const uncappedPool = [];
    for (const cat of catChoices) {
      const candidates = pickCandidateRows(rows, {
        kind,
        bbox,
        usedIds: new Set(),
        center,
        radiusKm,
        areaFocus,
        thresholds,
        cat,
        usedCoords: [],
        minSpacingMeters: 0,
        limit: exactMode ? Number.POSITIVE_INFINITY : WHATIF_SUGGESTION_LIMIT
       });
      candidates.forEach((candidate) => {
        uncappedPool.push({ cat, candidate });
      });
    }
    const poolWasCapped = uncappedPool.length > maxPoolSize;
    const pool = poolWasCapped ? uncappedPool.slice(0, maxPoolSize) : uncappedPool;
    if (!pool.length) {
      throw new Error('No candidate pool available for verification with current filters.');
    }

    const candidateRows = cloneRowsForWhatIf(rows, fairnessModel);
    suggestionsForTask.forEach((entry) => {
      if (!Array.isArray(entry.location) || entry.location.length < 2) return;
      const cat = entry.cat || catChoices[0];
      applyCandidate(candidateRows, cat, entry.location, categoriesForState);
    });
    const candidateObjective = evalObjective(candidateRows, categoriesForState);
    if (pool.length < count) {
      throw new Error(`Candidate pool too small for k=${count} (pool=${pool.length}).`);
    }

    const totalCombosBig = combinationCountBigInt(pool.length, count);
    const totalCombosExact = totalCombosBig <= BigInt(Number.MAX_SAFE_INTEGER);
    const totalCombos = totalCombosExact ? Number(totalCombosBig) : Number.MAX_SAFE_INTEGER;
    if (exactMode && totalCombosBig > WHATIF_EXACT_MAX_COMBOS) {
      throw new Error(`Exact verification infeasible for this request (combinations=${formatBigInt(totalCombosBig)}). Reduce count/categories or narrow scope.`);
    }
    const exhaustiveWithinPool = totalCombosBig <= BigInt(maxCombos);

    let bestObjective = Infinity;
    let bestSelection = null;
    const current = [];
    const usedIds = new Set();
    const usedCoords = [];
    let combosCheckedLocal = 0;

    const dfs = (start) => {
      if (combosChecked >= maxCombos) {
        truncated = true;
        return;
      }
      if (current.length === count) {
        combosChecked += 1;
        combosCheckedLocal += 1;
        const testRows = cloneRowsForWhatIf(rows, fairnessModel);
        current.forEach((entry) => {
          applyCandidate(testRows, entry.cat, entry.candidate.centroid, categoriesForState);
        });
        const objective = evalObjective(testRows, categoriesForState);
        if (objective < bestObjective) {
          bestObjective = objective;
          bestSelection = current.map((entry) => ({ ...entry }));
        }
        return;
      }

      for (let i = start; i < pool.length; i += 1) {
        if (combosChecked >= maxCombos) {
          truncated = true;
          return;
        }
        const item = pool[i];
        const idx = item.candidate.idx;
        if (usedIds.has(idx)) continue;
        if (!isFarFromUsed(item.candidate.centroid, usedCoords, minSpacingMeters)) continue;

        usedIds.add(idx);
        usedCoords.push(item.candidate.centroid);
        current.push(item);
        dfs(i + 1);
        current.pop();
        usedCoords.pop();
        usedIds.delete(idx);
      }
    };

    dfs(0);
    if (!Number.isFinite(bestObjective)) {
      throw new Error('Unable to evaluate combinations for this scenario.');
    }

    return {
      candidateObjective,
      bestObjective,
      gap: candidateObjective - bestObjective,
      count,
      poolSize: pool.length,
      poolOriginalSize: uncappedPool.length,
      poolCap: maxPoolSize,
      poolWasCapped,
      combosChecked: combosCheckedLocal,
      totalCombos: formatBigInt(totalCombosBig),
      totalCombosExact,
      exhaustiveWithinPool,
      globalGuarantee: exhaustiveWithinPool && !poolWasCapped,
      exactMode,
      bestSelection: Array.isArray(bestSelection)
        ? bestSelection.map((entry) => ({
            kind,
            cat: entry.cat,
            location: entry.candidate.centroid,
            candidate: entry.candidate
          }))
        : []
    };
  };

  if (fairnessTarget === 'overall') {
    if (fairnessModel === 'ifcity') {
      const weightsByCat = Object.fromEntries(overallCats.map((cat) => [cat, 1]));
      const state = await computeIfCitySuggestionState(overallCats, weightsByCat);
      const catChoices = categories.filter((cat) => state.validCats.includes(cat));
      const result = await verifyTask({
        rows: state.rows,
        thresholds: state.thresholds,
        categoriesForState: state.categories,
        catChoices,
        evalObjective: (rows) => generalizedEntropy(rows.map((row) => row.benefit), IF_CITY_ALPHA),
        applyCandidate: (rows, cat, coord, cats) => applyIfCityCandidate(cat, rows, cats, coord, weightsByCat),
        relevantSuggestions: currentSuggestions
      });
      return {
        ...result,
        metricName: 'Generalized entropy',
        bestSuggestions: result.bestSelection || [],
        combosChecked,
        truncated
      };
    }

    const state = await computeOverallSuggestionState(overallCats);
    const result = await verifyTask({
      rows: state.rows,
      thresholds: state.thresholds,
      categoriesForState: state.categories,
      catChoices: categories,
      evalObjective: (rows) => giniFromOverallRows(rows),
      applyCandidate: (rows, cat, coord, cats) => applyOverallCandidate(cat, rows, cats, coord),
      relevantSuggestions: currentSuggestions
    });
    return {
      ...result,
      metricName: 'Overall Gini',
      bestSuggestions: result.bestSelection || [],
      combosChecked,
      truncated
    };
  }

  const perCategory = [];
  for (const cat of categories) {
    const catSuggestions = currentSuggestions.filter((s) => s.cat === cat);
    if (!catSuggestions.length) continue;
    if (fairnessModel === 'ifcity') {
      const state = await computeIfCitySuggestionState([cat], { [cat]: 1 });
      const result = await verifyTask({
        rows: state.rows,
        thresholds: state.thresholds,
        categoriesForState: state.categories,
        catChoices: [cat],
        evalObjective: (rows) => generalizedEntropy(rows.map((row) => row.benefit), IF_CITY_ALPHA),
        applyCandidate: (rows, category, coord, cats) => applyIfCityCandidate(category, rows, cats, coord, { [category]: 1 }),
        relevantSuggestions: catSuggestions
      });
      perCategory.push({
        ...result,
        cat,
        metricName: `${prettyPOIName(cat)} entropy`
      });
      continue;
    }

    const fetched = await fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] }));
    const poiCoords = collectPOICoordsForCategory(cat, fetched);
    const rows = computeBuildingDataForCategory(cat, poiCoords);
    const result = await verifyTask({
      rows,
      thresholds: getCityAreaThresholds(rows),
      categoriesForState: [cat],
      catChoices: [cat],
      evalObjective: (testRows) => giniFromBuildingScores(testRows),
      applyCandidate: (testRows, category, coord) => applyCandidateToRows(category, testRows, coord),
      relevantSuggestions: catSuggestions
    });
    perCategory.push({
      ...result,
      cat,
      metricName: `${prettyPOIName(cat)} Gini`
    });
  }

  if (!perCategory.length) {
    throw new Error('No category suggestions available to verify.');
  }

  const worstGap = perCategory.reduce((acc, item) => Math.max(acc, item.gap), -Infinity);
  return {
    perCategory,
    bestSuggestions: perCategory.flatMap((item) => item.bestSelection || []),
    gap: worstGap,
    metricName: 'Per-category',
    combosChecked,
    truncated
  };
}

function mockTypeLabel(type) {
  if (type === 'residential') return 'Residential';
  return prettyPOIName(type);
}

function buildWhatIfMockTypeInputs() {
  if (!whatIfMockTypeList) return;
  whatIfMockTypeList.innerHTML = '';
  const frag = document.createDocumentFragment();
  WHATIF_MOCK_TYPE_OPTIONS.forEach((type) => {
    const row = document.createElement('div');
    row.className = 'd-flex align-items-center justify-content-between gap-2 mb-1';

    const label = document.createElement('label');
    label.className = 'small text-muted';
    label.textContent = mockTypeLabel(type);
    label.setAttribute('for', `whatIfMockType_${type}`);

    const input = document.createElement('input');
    input.type = 'number';
    input.min = '0';
    input.max = String(WHATIF_MOCK_BUILDING_LIMIT);
    input.value = '0';
    input.className = 'form-control form-control-sm';
    input.style.maxWidth = '110px';
    input.id = `whatIfMockType_${type}`;
    input.dataset.whatIfMockType = type;

    row.appendChild(label);
    row.appendChild(input);
    frag.appendChild(row);
  });
  whatIfMockTypeList.appendChild(frag);
}

function getWhatIfMockTypeCounts() {
  const inputs = whatIfMockTypeList?.querySelectorAll?.('[data-what-if-mock-type]') || [];
  const counts = [];
  let total = 0;
  inputs.forEach((input) => {
    const type = input.dataset.whatIfMockType;
    const raw = parseInt(input.value || '0', 10);
    const count = Number.isFinite(raw) ? Math.max(0, raw) : 0;
    if (count > 0) {
      counts.push({ type, count });
      total += count;
    }
  });
  const clampedTotal = Math.min(total, WHATIF_MOCK_BUILDING_LIMIT);
  if (total > WHATIF_MOCK_BUILDING_LIMIT) {
    let remaining = WHATIF_MOCK_BUILDING_LIMIT;
    counts.forEach((entry) => {
      entry.count = Math.min(entry.count, remaining);
      remaining -= entry.count;
    });
  }
  return { total: clampedTotal, counts };
}

function normalizeWhatIfMockCounts(rawCounts) {
  let entries = [];
  if (Array.isArray(rawCounts)) {
    entries = rawCounts;
  } else if (rawCounts && typeof rawCounts === 'object') {
    entries = Object.entries(rawCounts).map(([type, count]) => ({ type, count }));
  }
  const allowed = new Set(WHATIF_MOCK_TYPE_OPTIONS);
  const counts = [];
  let total = 0;
  entries.forEach((entry) => {
    const type = (entry?.type || entry?.category || entry?.cat || '').toString();
    if (!type || !allowed.has(type)) return;
    const raw = parseInt(entry.count || '0', 10);
    const count = Number.isFinite(raw) ? Math.max(0, raw) : 0;
    if (count > 0) {
      counts.push({ type, count });
      total += count;
    }
  });
  const clampedTotal = Math.min(total, WHATIF_MOCK_BUILDING_LIMIT);
  if (total > WHATIF_MOCK_BUILDING_LIMIT) {
    let remaining = WHATIF_MOCK_BUILDING_LIMIT;
    counts.forEach((entry) => {
      entry.count = Math.min(entry.count, remaining);
      remaining -= entry.count;
    });
  }
  return { total: clampedTotal, counts };
}

function setWhatIfMockTypeCounts(rawCounts) {
  if (!whatIfMockTypeList) return;
  const { counts } = normalizeWhatIfMockCounts(rawCounts);
  const byType = counts.reduce((acc, entry) => {
    acc[entry.type] = entry.count;
    return acc;
  }, {});
  const inputs = whatIfMockTypeList?.querySelectorAll?.('[data-what-if-mock-type]') || [];
  inputs.forEach((input) => {
    const type = input.dataset.whatIfMockType;
    const count = byType?.[type] || 0;
    input.value = String(count);
  });
}

function setWhatIfLassoStatus(text = '', isError = false) {
  if (!whatIfLassoStatus) return;
  whatIfLassoStatus.textContent = text;
  whatIfLassoStatus.classList.toggle('text-danger', !!isError);
  whatIfLassoStatus.classList.toggle('text-muted', !isError);
}

async function clearWhatIfMockBuildings() {
  if (baseCityFC?.features?.length) {
    baseCityFC.features = baseCityFC.features.filter((feat) => !feat?.properties?.__whatIfMock);
  }
  if (newbuildsFC?.features?.length) {
    newbuildsFC.features = newbuildsFC.features.filter((feat) => !feat?.properties?.__whatIfMock);
  }
  refreshBuildingTypeDropdown();
  updateLayers();
  setWhatIfLassoClearDisabled(true);
  setWhatIfLassoStatus('Mock buildings cleared.');
  showGlobalSpinner('Clearing & recomputing…');
  await waitForSpinnerPaint();
  await recomputeFairnessAfterWhatIf();
  hideGlobalSpinner();
}

async function applyWhatIfSuggestions() {
  if (!whatIfSuggestions.length) return;
  showGlobalSpinner('Applying suggestions…');
  await waitForSpinnerPaint();
  const beforeCategoryLabel = fairCategory && fairCategory !== 'mix'
    ? prettyPOIName(fairCategory)
    : (fairCategory === 'mix' ? 'Mix' : null);
  const beforeCategoryGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  let lastFeature = null;
  let lastCoord = null;
  whatIfSuggestions.forEach((suggestion) => {
    if (suggestion.kind === 'change' && suggestion.candidate?.feature) {
      const feature = suggestion.candidate.feature;
      const props = feature.properties || (feature.properties = {});
      applyPOITags(props, suggestion.cat);
      props.__whatIf = true;
      lastFeature = feature;
      lastCoord = suggestion.candidate.centroid;
    } else if (suggestion.kind === 'add' && Array.isArray(suggestion.location)) {
      lastFeature = createWhatIfBuilding(suggestion.location, suggestion.cat);
      lastCoord = suggestion.location;
    }
  });
  if (lastFeature && lastCoord) {
    showPopup(lastFeature, lastCoord);
  }
  const beforeSnapshot = new Map();
  (baseCityFC?.features || []).forEach((f, idx) => {
    const s = f?.properties?.fair?.score;
    if (Number.isFinite(s)) beforeSnapshot.set(idx, s);
  });
  const recomputeRes = await recomputeFairnessAfterWhatIf();
  const afterCategoryGini = Number.isFinite(recomputeRes?.categoryGini)
    ? recomputeRes.categoryGini
    : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(recomputeRes?.overallGini)
    ? recomputeRes.overallGini
    : (Number.isFinite(overallGini) ? overallGini : null);

  const beforeParts = [];
  const afterParts = [];
  if (beforeCategoryLabel && Number.isFinite(beforeCategoryGini)) {
    beforeParts.push(`${beforeCategoryLabel} Gini ${beforeCategoryGini.toFixed(3)}`);
  }
  if (Number.isFinite(beforeOverallGini)) {
    beforeParts.push(`Overall Gini ${beforeOverallGini.toFixed(3)}`);
  }
  if (beforeCategoryLabel && Number.isFinite(afterCategoryGini)) {
    afterParts.push(`${beforeCategoryLabel} Gini ${afterCategoryGini.toFixed(3)}`);
  }
  if (Number.isFinite(afterOverallGini)) {
    afterParts.push(`Overall Gini ${afterOverallGini.toFixed(3)}`);
  }

  const baseSummary = formatWhatIfSuggestionSummary(whatIfSuggestions);
  const metricSummary = (beforeParts.length && afterParts.length)
    ? ` Before: ${beforeParts.join(' | ')}. After: ${afterParts.join(' | ')}.`
    : '';
  updateWhatIfSuggestionUI(`${baseSummary}${metricSummary}`, {
    hasSuggestion: whatIfSuggestions.length > 0
  });
  const deltaPairs = applyDeltaColorsFromSnapshot(beforeSnapshot, changeLogIdCounter);
  recordWhatIfChange({
    action: 'ai_suggestion',
    description: baseSummary,
    category: whatIfSuggestions[0]?.cat || null,
    beforeGini: beforeCategoryGini,
    afterGini: afterCategoryGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: deltaPairs
  });
  hideGlobalSpinner();
  updateLayers();
}

function extractDisplayedGiniValue(text) {
  const match = String(text || '').match(/([-+]?\d*\.?\d+)\s*$/);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

// ======================= Change history log =======================

/**
 * Compute an RGBA snapshot color for a given Gini delta.
 * giniDelta < 0  → fairness improved → teal (positive)
 * giniDelta > 0  → fairness worsened → red  (negative)
 * Intensity scales with magnitude up to maxDelta.
 */
function getChangeSnapshotColor(giniDelta) {
  if (!Number.isFinite(giniDelta) || giniDelta === 0) return [128, 128, 128];
  const intensity = Math.min(1, Math.abs(giniDelta) / 0.1);
  if (giniDelta < 0) {
    // Improved: bright orange
    return [255, Math.round(100 + intensity * 80), 0];
  } else {
    // Worsened: crimson red
    return [Math.round(180 + intensity * 40), Math.round(20 * (1 - intensity)), Math.round(40 * (1 - intensity))];
  }
}

/** Capture current per-building fairness scores before a change. */
function captureScoreSnapshot() {
  const snapshot = new Map();
  (baseCityFC?.features || []).forEach((f, idx) => {
    const score = f?.properties?.fair?.score;
    if (Number.isFinite(score)) snapshot.set(idx, score);
  });
  return snapshot;
}

/**
 * After a recompute, diff each building's new score against the snapshot
 * and paint _changeColor based on per-building delta.
 */
// function applyDeltaColorsFromSnapshot(snapshot, changeId) {
//   // Returns {feature, color}[] pairs — does NOT write to features yet.
//   // Colors are applied only when user clicks the change entry.
//   const pairs = [];
//   (baseCityFC?.features || []).forEach((f, idx) => {
//     if (!f?.properties) return;
//     const score = f.properties.fair?.score;
//     if (!Number.isFinite(score)) return;
//     const before = snapshot.get(idx);
//     let delta = 0;
//     let useGlobal = false;
//     if (Number.isFinite(before)) {
//       delta = score - before; // positive = improved
//     } else {
//       // No before snapshot (first POI selection) — use score itself
//       useGlobal = true;
//       delta = (score - 0.5); // treat above-average as positive
//     }
//     if (!useGlobal && Math.abs(delta) < 0.002) {
//       pairs.push({ feature: f, color: [247, 247, 247], changeId });
//       return;
//     }
//     const maxDelta = useGlobal ? 0.4 : 0.06;
//     const intensity = Math.min(1, Math.max(0, Math.abs(delta) / maxDelta));
//     if (intensity < 0.05) {
//       pairs.push({ feature: f, color: [247, 247, 247], changeId });
//       return;
//     }
//     const alpha = Math.round(140 + intensity * 115);
//     // Diverging 9-color scale: red (worsened) ← neutral → blue (improved)
//     // #b2182b #d6604d #f4a582 #fddbc7 #f7f7f7 #d1e5f0 #92c5de #4393c3 #2166ac
//     const CHANGE_DIVERGING = [
//       [178,  24,  43], // #b2182b  (most worsened)
//       [214,  96,  77], // #d6604d
//       [244, 165, 130], // #f4a582
//       [253, 219, 199], // #fddbc7
//       [247, 247, 247], // #f7f7f7  (neutral center)
//       [209, 229, 240], // #d1e5f0
//       [146, 197, 222], // #92c5de
//       [ 67, 147, 195], // #4393c3
//       [ 33, 102, 172], // #2166ac  (most improved)

//       // [215,  48,  39], // #d73027 (most worsened)
//       // [244, 109,  67], // #f46d43
//       // [253, 174,  97], // #fdae61
//       // [254, 224, 139], // #fee08b
//       // [255, 255, 191], // #ffffbf (neutral center)
//       // [217, 239, 139], // #d9ef8b
//       // [166, 217, 106], // #a6d96a
//       // [102, 189,  99], // #66bd63
//       // [ 26, 152,  80], // #1a9850 (most improved)
//     ];
//     // Map intensity (0–1) + direction to a position in the 9-stop ramp
//     // delta > 0 → improved → blue half (indices 5–8)
//     // delta < 0 → worsened → red half  (indices 0–3)
//     // intensity 0 → near center, intensity 1 → most saturated end
//     // Skip the white center (#f7f7f7) — map directly to the colored halves
//     // so even low-intensity changes are visually distinct from gray "no change"
//     let t;
//     if (delta > 0) {
//       t = 5 + intensity * 3; // 5 → 8 (light blue to deep blue)
//     } else {
//       t = 3 - intensity * 3; // 3 → 0 (light pink to deep red)
//     }
//     t = Math.max(0, Math.min(8, t));
//     const lo = Math.floor(t);
//     const hi = Math.min(8, lo + 1);
//     const frac = t - lo;
//     const color = [
//       Math.round(CHANGE_DIVERGING[lo][0] + (CHANGE_DIVERGING[hi][0] - CHANGE_DIVERGING[lo][0]) * frac),
//       Math.round(CHANGE_DIVERGING[lo][1] + (CHANGE_DIVERGING[hi][1] - CHANGE_DIVERGING[lo][1]) * frac),
//       Math.round(CHANGE_DIVERGING[lo][2] + (CHANGE_DIVERGING[hi][2] - CHANGE_DIVERGING[lo][2]) * frac),
//       alpha
//     ];
//     pairs.push({ feature: f, color, changeId });
//   });
//   return pairs;
// }

function applyDeltaColorsFromSnapshot(snapshot, changeId) {
  const CHANGE_DIVERGING = [
    [178,  24,  43], // #b2182b  (most worsened)
    [214,  96,  77], // #d6604d
    [244, 165, 130], // #f4a582
    [253, 219, 199], // #fddbc7
    [247, 247, 247], // #f7f7f7  (neutral center)
    [209, 229, 240], // #d1e5f0
    [146, 197, 222], // #92c5de
    [ 67, 147, 195], // #4393c3
    [ 33, 102, 172], // #2166ac  (most improved)
  ];

  // --- Pass 1: compute deltas for every building ---
  const entries = [];
  (baseCityFC?.features || []).forEach((f, idx) => {
    if (!f?.properties) return;
    const score = f.properties.fair?.score;
    if (!Number.isFinite(score)) return;
    const before = snapshot.get(idx);
    let delta = 0;
    let useGlobal = false;
    if (Number.isFinite(before)) {
      delta = score - before;
    } else {
      useGlobal = true;
      delta = score - 0.5;
    }
    entries.push({ f, delta, useGlobal });
  });

  if (!entries.length) return [];

  // Find the actual min and max delta in this change
  const deltas = entries.map(e => e.delta);
  const minDelta = Math.min(...deltas);
  const maxDelta = Math.max(...deltas);

  // --- Pass 2: map each delta to the full 0–8 scale ---
  const pairs = [];
  for (const { f, delta, useGlobal } of entries) {
    // No-change → neutral gray
    if (!useGlobal && Math.abs(delta) < 0.002) {
      pairs.push({ feature: f, color: [247, 247, 247, 160], changeId });
      continue;
    }

    // Map delta to 0–8 using the actual range of this change
    // minDelta → 0 (deepest red), 0 → 4 (neutral), maxDelta → 8 (deepest blue)
    let t;
    if (delta < 0 && minDelta < 0) {
      t = 4 * (1 - delta / minDelta);  // minDelta → 0, zero → 4
    } else if (delta > 0 && maxDelta > 0) {
      t = 4 + 4 * (delta / maxDelta);  // zero → 4, maxDelta → 8
    } else {
      t = 4; // exactly zero or no range
    }

    t = Math.max(0, Math.min(8, t));
    const lo = Math.floor(t);
    const hi = Math.min(8, lo + 1);
    const frac = t - lo;
    const color = [
      Math.round(CHANGE_DIVERGING[lo][0] + (CHANGE_DIVERGING[hi][0] - CHANGE_DIVERGING[lo][0]) * frac),
      Math.round(CHANGE_DIVERGING[lo][1] + (CHANGE_DIVERGING[hi][1] - CHANGE_DIVERGING[lo][1]) * frac),
      Math.round(CHANGE_DIVERGING[lo][2] + (CHANGE_DIVERGING[hi][2] - CHANGE_DIVERGING[lo][2]) * frac),
      220
    ];
    pairs.push({ feature: f, color, changeId });
  }
  return pairs;
}

/**
 * Record a what-if change, paint affected buildings with a snapshot color,
 * and update the change log dropdown.
 */
function recordWhatIfChange({ action, description, category, beforeGini, afterGini, beforeOverall, afterOverall, affectedFeatures = [] }) {
  const city = lastCityName || '—';
  const source = sourceMode || 'osm';
  // Prefer category Gini delta; fall back to overall
  const catDelta = (Number.isFinite(beforeGini) && Number.isFinite(afterGini))
    ? afterGini - beforeGini : null;
  const overallDelta = (Number.isFinite(beforeOverall) && Number.isFinite(afterOverall))
    ? afterOverall - beforeOverall : null;
  const giniDelta = catDelta ?? overallDelta;

  const snapshotColor = getChangeSnapshotColor(giniDelta);

  // Colors are NOT applied here — only on-demand when user clicks the change entry

  // Compute centroids for map flyto + highlight
  // affectedFeatures may be raw features OR {feature, color, changeId}[] pairs
  const rawFeatures = affectedFeatures.map(f => f?.feature ?? f).filter(Boolean);
  const affectedCentroids = rawFeatures
    .map(f => { try { return turf.centroid(f).geometry.coordinates; } catch { return null; } })
    .filter(Boolean);
  const highlightCenter = affectedCentroids.length
    ? affectedCentroids.reduce((acc, c) => [acc[0] + c[0] / affectedCentroids.length, acc[1] + c[1] / affectedCentroids.length], [0, 0])
    : null;

  const record = {
    id: changeLogIdCounter++,
    timestamp: new Date(),
    city,
    source,
    action,
    description,
    category: category || null,
    beforeGini: Number.isFinite(beforeGini) ? beforeGini : null,
    afterGini: Number.isFinite(afterGini) ? afterGini : null,
    beforeOverall: Number.isFinite(beforeOverall) ? beforeOverall : null,
    afterOverall: Number.isFinite(afterOverall) ? afterOverall : null,
    giniDelta,
    overallDelta,
    snapshotColor,
    affectedCount: rawFeatures.length,
    affectedCentroids,
    highlightCenter,
    changeId: changeLogIdCounter - 1,
    colorPairs: affectedFeatures  // {feature, color, changeId}[] from applyDeltaColorsFromSnapshot
  };
  whatIfChangeLog.push(record);
  changeLogTick++;
  updateChangeLogUI();
  updateLayers();
  return record;
}

function deltaToColor(delta) {
  const CHANGE_DIVERGING = [
    [178,  24,  43],
    [214,  96,  77],
    [244, 165, 130],
    [253, 219, 199],
    [247, 247, 247],
    [209, 229, 240],
    [146, 197, 222],
    [ 67, 147, 195],
    [ 33, 102, 172],
  ];
  // Negative Gini delta = improvement = blue, positive = worsened = red
  const d = -delta;
  let t;
  if (d < 0) {
    t = Math.max(0, 4 * (1 - Math.min(1, Math.abs(d) / 0.1)));
  } else if (d > 0) {
    t = Math.min(8, 4 + 4 * Math.min(1, d / 0.1));
  } else {
    t = 4;
  }
  const lo = Math.floor(t);
  const hi = Math.min(8, lo + 1);
  const frac = t - lo;
  const r = Math.round(CHANGE_DIVERGING[lo][0] + (CHANGE_DIVERGING[hi][0] - CHANGE_DIVERGING[lo][0]) * frac);
  const g = Math.round(CHANGE_DIVERGING[lo][1] + (CHANGE_DIVERGING[hi][1] - CHANGE_DIVERGING[lo][1]) * frac);
  const b = Math.round(CHANGE_DIVERGING[lo][2] + (CHANGE_DIVERGING[hi][2] - CHANGE_DIVERGING[lo][2]) * frac);
  return `rgb(${r},${g},${b})`;
}

/** Rebuild the change log dropdown contents. */
function updateChangeLogUI() {
  const countBadge  = document.getElementById('changeLogCount');
  const list        = document.getElementById('changeLogList');
  const empty       = document.getElementById('changeLogEmpty');
  const footer      = document.getElementById('changeLogFooter');
  if (!list) return;

  const total = whatIfChangeLog.length;
  if (countBadge) {
    countBadge.textContent = total;
    countBadge.classList.toggle('d-none', total === 0);
  }
  if (total === 0) {
    list.innerHTML = '';
    empty?.classList.remove('d-none');
    footer?.classList.add('d-none');
    return;
  }
  empty?.classList.add('d-none');
  footer?.classList.remove('d-none');

  // Group by city + source (insertion order preserves chronology)
  const groups = new Map();
  whatIfChangeLog.forEach(rec => {
    const key = `${rec.city}||${rec.source}`;
    if (!groups.has(key)) groups.set(key, { city: rec.city, source: rec.source, entries: [] });
    groups.get(key).entries.push(rec);
  });

  let html = '';
  groups.forEach(group => {
    html += `<div class="change-group-header">🏙 ${group.city} · ${group.source.toUpperCase()}</div>`;
    // Newest first
    [...group.entries].reverse().forEach(rec => {
      const delta = rec.giniDelta;
      const dirClass = !Number.isFinite(delta) ? 'neutral'
        : delta < 0 ? 'positive' : delta > 0 ? 'negative' : 'neutral';
      const deltaText = Number.isFinite(delta)
        ? `${delta > 0 ? '+' : ''}${delta.toFixed(3)}`
        : '±—';
      const swatchBg = deltaToColor(Number.isFinite(delta) ? delta : 0);
      const textColor = Math.abs(Number.isFinite(delta) ? delta : 0) < 0.005 ? '#333' : '#fff';
      const timeStr = rec.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      const catLabel = rec.category ? `<span>📦 ${prettyPOIName(rec.category)}</span>` : '';
      const beforeAfter = (Number.isFinite(rec.beforeGini) && Number.isFinite(rec.afterGini))
        ? `<span>Gini ${rec.beforeGini.toFixed(3)} → ${rec.afterGini.toFixed(3)}</span>` : '';
      const hasFeatures = (rec.colorPairs?.length > 0) || rec.highlightCenter;
      const isPinned = pinnedChangeId === rec.id;
      const mapBtnHtml = hasFeatures
        ? `<button class="change-map-toggle btn btn-xs ms-2" data-change-id="${rec.id}"
            style="font-size:0.68rem;padding:1px 7px;border-radius:10px;border:1px solid ${isPinned ? '#0dcaf0' : '#aaa'};
            background:${isPinned ? '#0dcaf0' : 'transparent'};color:${isPinned ? '#000' : '#666'};cursor:pointer;white-space:nowrap;">
            ${isPinned ? '👁 Hide' : '👁 Show'}
           </button>`
        : '';
      html += `
        <div class="change-entry ${dirClass}" data-change-id="${rec.id}">
          <div class="d-flex justify-content-between align-items-start">
            <div class="fw-semibold" style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${rec.description}</div>
            <div class="d-flex align-items-center">
              ${mapBtnHtml}
              <span class="change-delta-badge ms-2" style="background:${swatchBg};color:${textColor};">${deltaText}</span>
            </div>
          </div>
          <div class="d-flex flex-wrap gap-2 mt-1" style="font-size:0.68rem;color:#555;">
            ${catLabel}${beforeAfter}
            <span class="ms-auto">${timeStr}</span>
          </div>
        </div>`;
    });
  });
  list.innerHTML = html;

  // Wire click handlers on entries that have affected features
  list.querySelectorAll('.change-map-toggle[data-change-id]').forEach(btn => {
    const id = Number(btn.dataset.changeId);
    const rec = whatIfChangeLog.find(r => r.id === id);
    if (rec) {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        highlightChangeOnMap(rec);
      });
    }
  });
}

let changeHighlightTimeout = null;
let activeHighlightPairs = [];

function clearChangeColorAggregates() {
  mezoHexData?.forEach(cell => { if (cell) delete cell._changeColor; });
  districtFC?.features?.forEach(f => { if (f?.properties) delete f.properties._changeColor; });
}

function highlightChangeOnMap(record) {
  if (!record) return;

  // If already pinned, unpin (hide)
  if (pinnedChangeId === record.id) {
    pinnedChangeId = null;
    activeHighlightPairs.forEach(({ feature }) => {
      if (feature?.properties) delete feature.properties._changeColor;
    });
    activeHighlightPairs = [];
    clearChangeColorAggregates();
    setSidePanelLegendMode((pinnedChangeId != null && !changeCompareBaseline) ? 'change' : 'fairness');
    changeLogTick++;
    updateLayers();
    updateChangeLogUI();
    return;
  }

  // Unpin previous
  activeHighlightPairs.forEach(({ feature }) => {
    if (feature?.properties) delete feature.properties._changeColor;
  });
  activeHighlightPairs = [];
  clearChangeColorAggregates();

  // Pin new
  pinnedChangeId = record.id;

  const pairs = record.colorPairs || [];
  // Only apply _changeColor for buildings with a meaningful color delta.
  // Neutral-gray pairs [247,247,247,*] indicate near-zero change — skip them
  // so those buildings fall through to the dim background ([90,90,90,120])
  // instead of being painted an almost-identical gray that makes the whole
  // map look washed out.
  const meaningfulPairs = pairs.filter(({ color }) => {
    if (!color) return false;
    return !(color[0] === 247 && color[1] === 247 && color[2] === 247);
  });
  meaningfulPairs.forEach(({ feature, color }) => {
    if (feature?.properties) feature.properties._changeColor = color;
  });
  activeHighlightPairs = meaningfulPairs;

  // Aggregate up to meso and district levels
  computeChangeColorAggregates(pairs);

  setSidePanelLegendMode('change');
  changeLogTick++;
  updateLayers();
  updateChangeLogUI();
}

function computeChangeColorAggregates(colorPairs) {
  // Use MAX intensity per cell (not average) so colors stay vivid.
  // Keep semantics aligned with building-level change map:
  // skip neutral no-change pairs ([247,247,247,*]).
  const meaningfulPairs = (colorPairs || []).filter(({ color }) => {
    if (!Array.isArray(color) || color.length < 3) return false;
    return !(color[0] === 247 && color[1] === 247 && color[2] === 247);
  });

  // ── Meso (H3 hex) aggregation ──
  const h3 = window.h3;
  const res = resolveMezoResolution();
  if (h3 && res != null && mezoHexData?.length) {
    const hexBest = new Map(); // hexId → {color, intensity}
    meaningfulPairs.forEach(({ feature, color }) => {
      if (!feature?.geometry) return;
      try {
        const [lng, lat] = turf.centroid(feature).geometry.coordinates;
        const cell = h3LatLngToCell(h3, lat, lng, res);
        if (!cell) return;
        const intensity = (color[3] ?? 200);
        const prev = hexBest.get(cell);
        if (!prev || intensity > prev.intensity) {
          hexBest.set(cell, { color, intensity });
        }
      } catch { /* ignore */ }
    });
    mezoHexData.forEach(cell => {
      if (!cell) return;
      const best = hexBest.get(cell.hex);
      if (best) {
        const c = best.color;
        // Boost alpha significantly for hex visibility
        cell._changeColor = [c[0], c[1], c[2], Math.min(255, (c[3] ?? 200) + 80)];
      } else {
        delete cell._changeColor;
      }
    });
  }

  // ── District aggregation ──
  if (districtFC?.features?.length) {
    // Build per-district: track dominant direction + max intensity
    const distBest = new Map(); // idx → {color, intensity}
    meaningfulPairs.forEach(({ feature, color }) => {
      if (!feature?.geometry) return;
      try {
        const pt = turf.centroid(feature);
        districtFC.features.forEach((dist, idx) => {
          if (!dist?.geometry) return;
          try {
            if (turf.booleanPointInPolygon(pt, dist)) {
              const intensity = (color[3] ?? 200);
              const prev = distBest.get(idx);
              if (!prev || intensity > prev.intensity) {
                distBest.set(idx, { color, intensity });
              }
            }
          } catch { /* ignore */ }
        });
      } catch { /* ignore */ }
    });
    districtFC.features.forEach((dist, idx) => {
      if (!dist?.properties) return;
      const best = distBest.get(idx);
      if (best) {
        const c = best.color;
        // Boost alpha significantly for district visibility
        dist.properties._changeColor = [c[0], c[1], c[2], Math.min(255, (c[3] ?? 200) + 100)];
      } else {
        delete dist.properties._changeColor;
      }
    });
  }
}

function formatWhatIfSuggestionSummary(suggestions) {
  if (!suggestions.length) return 'No suggestions available.';
  const byCat = suggestions.reduce((acc, s) => {
    const key = `${s.kind}:${s.cat}`;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const parts = Object.entries(byCat).map(([key, count]) => {
    const [kind, cat] = key.split(':');
    const verb = kind === 'change' ? 'Change' : 'Add';
    return `${verb} ${count} × ${prettyPOIName(cat)}`;
  });
  return `Suggestions ready: ${parts.join(', ')}.`;
}

function updateWhatIfSuggestionUI(text, { isError = false, isBusy = false, hasSuggestion = false } = {}) {
  if (whatIfSuggestionOut) {
    whatIfSuggestionOut.textContent = text || '';
    whatIfSuggestionOut.classList.toggle('text-danger', !!isError);
    whatIfSuggestionOut.classList.toggle('text-muted', !isError);
  }
  if (whatIfApplySuggestionBtn) whatIfApplySuggestionBtn.disabled = !hasSuggestion || isBusy;
  if (whatIfClearSuggestionBtn) whatIfClearSuggestionBtn.disabled = !hasSuggestion || isBusy;
  if (whatIfVerifySuggestionBtn) whatIfVerifySuggestionBtn.disabled = !hasSuggestion || isBusy;
  if (whatIfSuggestBtn) whatIfSuggestBtn.disabled = !!isBusy;
}

async function runWhatIfSuggestionFromChat(prompt, overrides = {}) {
  if (!baseCityFC) throw new Error('Load a city before requesting suggestions.');
  const fallbackKind = overrides.mode || whatIfModeSelect?.value || 'add';
  const fallbackCount = Number.isFinite(overrides.count)
    ? Math.max(1, Math.min(5, overrides.count))
    : Math.max(1, Math.min(5, parseInt(whatIfCountInput?.value || '1', 10) || 1));
  const selectedCategories = Array.isArray(overrides.categories) && overrides.categories.length
    ? overrides.categories
    : getSelectedWhatIfCategories();
  const fallbackCategories = selectedCategories.length
    ? selectedCategories
    : [whatIfType || ALL_CATEGORIES[0] || 'grocery'];
  const fallbackFairnessTarget = overrides.fairnessTarget || whatIfFairnessTargetSelect?.value || 'category';
  const fallbackAreaFocus = overrides.areaFocus || whatIfAreaFocusSelect?.value || 'any';

  const focusOverride = overrides.focus === 'viewport'
    ? 'viewport'
    : (overrides.focus === 'city'
      ? 'city'
      : (overrides.focus === 'lasso' ? 'lasso' : null));
  let lassoRequested = focusOverride === 'lasso';
  const useBounds = focusOverride === 'viewport'
    ? true
    : focusOverride === 'city'
      ? false
      : !!whatIfUseBoundsToggle?.checked;
  const lassoBbox = getWhatIfLassoBBox();
  const fallbackBbox = useBounds ? getMapBoundsBBox() : null;
  const radiusKm = Math.max(0, parseFloat(whatIfRadiusInput?.value || '0') || 0);
  const center = radiusKm > 0 ? getMapCenter() : null;

  updateWhatIfSuggestionUI('Thinking', { isBusy: true });
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
      if (intent?.categories?.length) categories = intent.categories;
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

    if (focusOverride === 'city') {
      bbox = null;
      selectedRadiusKm = 0;
      selectedCenter = null;
    }
    if (focusOverride === 'viewport') {
      bbox = getMapBoundsBBox();
    }
    if (focusOverride === 'lasso') {
      bbox = lassoBbox;
      selectedRadiusKm = 0;
      selectedCenter = null;
      lassoRequested = true;
    }
    if (Array.isArray(overrides.categories) && overrides.categories.length) {
      categories = overrides.categories;
    }

    if (lassoRequested && !bbox) {
      throw new Error('Draw a what-if lasso selection first.');
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
    updateWhatIfSuggestionUI(
      rationale ? `${summaryText} ${rationale}${verifyHint}` : `${summaryText}${verifyHint}`,
      { hasSuggestion: true }
    );
    return exactSuggestions;
  } catch (err) {
    updateWhatIfSuggestionUI(err?.message || 'Unable to generate suggestions.', { isError: true });
    throw err;
  }
}

function setPOISymbolsVisibility(enabled) {
  showPOISymbols = !!enabled;
  const poiSymbolsToggle = document.getElementById('poiSymbolsToggle');
  if (poiSymbolsToggle) poiSymbolsToggle.checked = !!enabled;
  poiStyleTick++;
  updateLayers();
}

function createWhatIfSuggestionLayers() {
  if (!whatIfSuggestions.length) return [];
  const points = whatIfSuggestions.map((suggestion, idx) => turf.point(suggestion.location, {
    label: `${idx + 1}`,
    kind: suggestion.kind
  }));
  return [
    new deck.ScatterplotLayer({
      id: 'whatif-suggestion-points',
      data: points,
      pickable: false,
      getPosition: f => f.geometry.coordinates,
      getRadius: 14,
      radiusUnits: 'pixels',
      filled: true,
      getFillColor: WHATIF_SUGGESTION_COLOR,
      stroked: true,
      getLineColor: [30, 30, 30],
      getLineWidth: 2,
      parameters: { depthTest: false },
      updateTriggers: { data: [whatIfSuggestionTick] }
    }),
    new deck.TextLayer({
      id: 'whatif-suggestion-labels',
      data: points,
      getPosition: f => f.geometry.coordinates,
      getText: f => f.properties?.label || '',
      getSize: 12,
      getColor: [255, 255, 255],
      getTextAnchor: 'middle',
      getAlignmentBaseline: 'top',
      parameters: { depthTest: false },
      updateTriggers: { data: [whatIfSuggestionTick] }
    })
  ];
}

function createMockBuildingPolygon(lngLat, sizeMeters = 12, shape = 'random', variationPct = 50) {
  const center = turf.point(lngLat);
  const half = Math.max(4, sizeMeters / 2);
  const v = Math.max(0, Math.min(100, variationPct)) / 100; // 0..1

  // Helper: offset a point from center by (dx, dy) in meters using bearing + distance
  const offset = (dxM, dyM) => {
    const dist = Math.sqrt(dxM * dxM + dyM * dyM);
    if (dist < 0.01) return lngLat;
    const bearing = (Math.atan2(dxM, dyM) * 180) / Math.PI;
    return turf.destination(center, dist, bearing, { units: 'meters' }).geometry.coordinates;
  };

  // Pick a shape: at v=0 always 'rect', at v=1 fully random across all shapes
  const shapes = ['rect', 'rect_wide', 'L', 'T', 'U'];
  let pick;
  if (shape !== 'random') {
    pick = shapes.includes(shape) ? shape : 'rect';
  } else if (Math.random() >= v) {
    // Below the variation threshold → default rect
    pick = 'rect';
  } else {
    // Above threshold → pick from all shapes (including rect for natural mix)
    pick = shapes[Math.floor(Math.random() * shapes.length)];
  }

  // Rotation: at v=0 all buildings face north (angle=0), at v=1 fully random
  const angle = v * Math.random() * 2 * Math.PI;
  const rot = (dx, dy) => {
    const rx = dx * Math.cos(angle) - dy * Math.sin(angle);
    const ry = dx * Math.sin(angle) + dy * Math.cos(angle);
    return offset(rx, ry);
  };

  // Aspect ratio jitter: at v=0 all identical proportions, at v=1 up to ±30% variation
  const aspectJitter = 1 + (Math.random() - 0.5) * 0.6 * v;  // 0.7..1.3 at full variation

  let ring;
  const w = half * aspectJitter;                  // half-width (jittered)
  const d = half * (0.85 / aspectJitter);         // half-depth (inverse jitter keeps area ~constant)
  const arm = half * (0.35 + 0.1 * v * Math.random()); // arm thickness varies slightly

  switch (pick) {
    case 'rect_wide': {
      // wider rectangle
      const ww = half * 1.4;
      const dd = half * 0.6;
      ring = [rot(-ww,-dd), rot(ww,-dd), rot(ww,dd), rot(-ww,dd)];
      break;
    }
    case 'L': {
      ring = [
        rot(-w, -d), rot(w, -d), rot(w, -d + arm),
        rot(-w + arm, -d + arm), rot(-w + arm, d), rot(-w, d)
      ];
      break;
    }
    case 'T': {
      const hw = arm / 2;
      ring = [
        rot(-w, d), rot(-w, d - arm), rot(-hw, d - arm),
        rot(-hw, -d), rot(hw, -d), rot(hw, d - arm),
        rot(w, d - arm), rot(w, d)
      ];
      break;
    }
    case 'U': {
      ring = [
        rot(-w, -d), rot(w, -d), rot(w, d),
        rot(w - arm, d), rot(w - arm, -d + arm),
        rot(-w + arm, -d + arm), rot(-w + arm, d), rot(-w, d)
      ];
      break;
    }
    default: { // 'rect'
      ring = [rot(-w, -d), rot(w, -d), rot(w, d), rot(-w, d)];
    }
  }

  ring.push(ring[0]); // close the ring
  return { type: 'Polygon', coordinates: [ring] };
}

function getWhatIfAvoidLayerIds() {
  if (!map?.getStyle) return [];
  const style = map.getStyle();
  const layers = style?.layers || [];
  if (!layers.length) return [];
  const keywords = WHATIF_AVOID_LAYER_KEYWORDS;
  return layers
    .filter((layer) => {
      if (!layer || !layer.id) return false;
      if (!['fill', 'line', 'symbol'].includes(layer.type)) return false;
      const hay = `${layer.id} ${layer['source-layer'] || ''}`.toLowerCase();
      return keywords.some((k) => hay.includes(k));
    })
    .map((layer) => layer.id);
}

function createWhatIfBuilding(lngLat, categoryOverride = null) {
  if (!baseCityFC?.features?.length) return null;
  const canonical = categoryOverride || whatIfType || ALL_CATEGORIES[0] || 'grocery';
  const point = turf.point(lngLat);
  const polygon = turf.buffer(point, 25, { units: 'meters' });
  polygon.properties = {
    name: `What-if ${prettyPOIName(canonical)}`,
    built_year: new Date().getFullYear(),
    __whatIf: true,
    __whatIfAdded: true
  };
  applyPOITags(polygon.properties, canonical);
  baseCityFC.features.push(polygon);
  if (newbuildsFC?.features) newbuildsFC.features.push(polygon);
  refreshBuildingTypeDropdown();
  return polygon;
}

function applyMockBuildingTags(props, type, { floors, floorHeight, footprint } = {}) {
  if (!props) return;
  const label = mockTypeLabel(type);
  props.__whatIf = true;
  props.__whatIfAdded = true;
  props.__whatIfMock = true;
  props.__whatIfMockType = type;
  props.name = `What-if ${label}`;
  props.built_year = new Date().getFullYear();

  // Height & floor metadata
  const nFloors = floors ?? whatIfMockFloors;
  const fHeight = floorHeight ?? whatIfMockFloorHeight;
  props.__mockFloors = nFloors;
  props.__mockFloorHeight = fHeight;
  props.__mockFootprint = footprint ?? null;
  props.height_m = nFloors * fHeight;

  // Population proxy: more floors + larger footprint → more residents/users
  // This feeds into equity weighting in the gravity model
  const fpArea = footprint ? (footprint * footprint) : 144; // fallback ~12m²
  props.__mockCapacity = Math.round(nFloors * (fpArea / 80)); // ~1 person per 80 m² per floor

  if (type === 'residential') {
    props.building = 'residential';
    props.category = 'residential';
    delete props.whatif_poi;
    return;
  }

  applyPOITags(props, type);
}

function getWhatIfMockBuildings() {
  return baseCityFC?.features?.filter((feat) => feat?.properties?.__whatIfMock) || [];
}

function createWhatIfMockBuildingsLayer() {
  const data = getWhatIfMockBuildings();
  if (!data.length) return null;
  return new deck.GeoJsonLayer({
    id: 'whatif-mock-outline',
    data,
    pickable: true,
    filled: true,
    stroked: false,
    extruded: true,
    opacity: 1,
    material: BUILDING_MATERIAL,
    getElevation: f => {
      const h = f.properties?.height_m;
      return (Number.isFinite(h) ? h : 10) * heightScale;
    },
    getFillColor: f => mockBuildingFillColor(f),
    updateTriggers: {
      getFillColor: [fairActive, fairCategory, fairRecolorTick, changeLogTick, drSelectionTick, drHasSelection, changeCompareBaseline, pinnedChangeId],
      getElevation: [heightScale]
    },
    onClick: handleClick
  });
}


async function handleWhatIfMapClick(event) {
  if (whatIfMode !== 'add' || !event?.lngLat) return;
  if (mapLasso.active) return;
  if (whatIfLasso.active) return;
  if (Date.now() - lastFeatureClickAt < 260) return;
  const lngLat = [event.lngLat.lng, event.lngLat.lat];
  const categoryOverride = getActiveWhatIfCategory();

  const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  const scoreSnapshot = captureScoreSnapshot();

  const feat = createWhatIfBuilding(lngLat, categoryOverride);
  if (!feat) return;
  showPopup(feat, lngLat);
  showGlobalSpinner('Adding building & recomputing…');
  await waitForSpinnerPaint();

  const recomputeRes = await recomputeFairnessAfterWhatIf();

  const afterCatGini = Number.isFinite(recomputeRes?.categoryGini)
    ? recomputeRes.categoryGini
    : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(recomputeRes?.overallGini)
    ? recomputeRes.overallGini
    : (Number.isFinite(overallGini) ? overallGini : null);

  const nextId = changeLogIdCounter;
  const colorPairs = applyDeltaColorsFromSnapshot(scoreSnapshot, nextId);

  // Keep the exact diverging change-map palette from applyDeltaColorsFromSnapshot.
  // Do NOT force a custom green color for new buildings, otherwise meso/macro
  // aggregation can inherit green and conflict with the red↔blue legend.

  // Filter out near-zero-delta buildings to avoid painting everything gray.
  // Keep only meaningful diverging colors for aggregation/highlight.
  const meaningfulPairs = colorPairs.filter(p =>
    !p.color || p.color[3] === undefined ||
    !(p.color[0] === 247 && p.color[1] === 247 && p.color[2] === 247)
  );

  recordWhatIfChange({
    action: 'add_building',
    description: `Added ${prettyPOIName(categoryOverride)} building`,
    category: categoryOverride || null,
    beforeGini: beforeCatGini,
    afterGini: afterCatGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: meaningfulPairs
  });

  hideGlobalSpinner();
  updateLayers();
}

function ensurePopupInView(popup, padding = 24) {
  const popupEl = popup?.getElement?.();
  const mapEl = map?.getContainer?.();
  if (!popupEl || !mapEl) return;

  const popupRect = popupEl.getBoundingClientRect();
  const mapRect = mapEl.getBoundingClientRect();

  const leftOverflow = (mapRect.left + padding) - popupRect.left;
  const rightOverflow = popupRect.right - (mapRect.right - padding);
  const topOverflow = (mapRect.top + padding) - popupRect.top;
  const bottomOverflow = popupRect.bottom - (mapRect.bottom - padding);

  let dx = 0;
  let dy = 0;

  if (rightOverflow > 0) dx = rightOverflow;
  else if (leftOverflow > 0) dx = -leftOverflow;

  if (bottomOverflow > 0) dy = bottomOverflow;
  else if (topOverflow > 0) dy = -topOverflow;

  if (dx !== 0 || dy !== 0) {
    map.panBy([dx, dy], { duration: 280 });
  }
}

function showPopup(payload, atLngLat) {
  closePopup();
  const feature = payload?.type === 'Feature' ? payload : null;
  const props = feature ? (feature.properties || {}) : (payload || {});
  const rows = [];
  const add = (k, v) => rows.push(`<tr><th>${k}</th><td>${fmtValue(v)}</td></tr>`);

  const isSingle = fairActive && fairCategory && fairCategory !== 'mix';
  const nearestDist = props?.fair?.nearest_dist_m;
  const isSelfPOI =
    isSingle &&
    (
      buildingMatchesPOI(props, fairCategory) ||
      (Number.isFinite(nearestDist) && nearestDist <= SELF_POI_EPS_M)
    );

  const showFairness = (fairActive || props?.__whatIfMock) && props?.fair;
  if (showFairness) {
    const label = fairActive
      ? (fairCategory === 'mix' ? 'Custom mix' : prettyPOIName(fairCategory))
      : 'Overall';
    add(`Fairness (${label})`, `${Math.round(props.fair.score * 100)}%`);

    if (isSingle && !isSelfPOI) {
      if (props.fair.nearest_name) add('Name', props.fair.nearest_name);
      if (Number.isFinite(props.fair.nearest_dist_m)) add('Distance', `${(props.fair.nearest_dist_m/1000).toFixed(2)} km`);
      if (Number.isFinite(props.fair.nearest_time_min)) add('Walk time', `${props.fair.nearest_time_min.toFixed(0)} min`);
    }
  }

  if (props?.fair_overall?.score != null) {
    add('Overall accessibility (0–1)', props.fair_overall.score.toFixed(2));
  }
  if (overallGini != null) add('City overall Gini', Number(overallGini).toFixed(2));

  const popupCategoryRaw = (props?.category || props?.category_label || props?.objekttyp || '').toString().trim();
  const popupCategoryLabel = (!popupCategoryRaw || popupCategoryRaw.toLowerCase() === 'unknown')
    ? 'unknown/residential'
    : popupCategoryRaw;

  add('Name', props?.name || props?.byggnadsnamn1);
  add('Type', props?.andamal1 || props?.objekttyp);
  add('Category', popupCategoryLabel !== 'unknown' && popupCategoryLabel !== 'Other / unknown' ? popupCategoryLabel : null);
  add('Built year', getBuiltYear(props));
  if (props.__whatIfMock) {
    if (Number.isFinite(props.__mockFloors)) add('Floors', props.__mockFloors);
    if (Number.isFinite(props.height_m)) add('Height', `${props.height_m.toFixed(1)} m`);
    if (Number.isFinite(props.__mockCapacity)) add('Est. capacity', `${props.__mockCapacity} people`);
  }

  const title = props?.name || props?.byggnadsnamn1 || props?.andamal1?.split(';')[0] || popupCategoryLabel || props?.objekttyp || 'Building';
  const geomType = feature?.geometry?.type || '';
  const isBuilding = geomType === 'Polygon' || geomType === 'MultiPolygon';
  const showWhatIfEdit = whatIfMode === 'edit' && isBuilding;
  const editControls = showWhatIfEdit
     ? `
      <div class="mt-2 pt-2 border-top">
        <div class="small text-muted mb-1">What-if: change building type</div>
        <select class="form-select form-select-sm" data-whatif="type">
          ${buildWhatIfTypeOptions(detectPOICategoryFromProps(props) || whatIfType)}
        </select>
        <div class="d-flex gap-2 mt-2">
          <button class="btn btn-sm btn-warning" data-whatif="apply">Apply</button>
          <button class="btn btn-sm btn-outline-secondary" data-whatif="close">Close</button>
        </div>
      </div>`
    : '';
  const html = `
    <div style="min-width:260px; max-width:360px;">
      <div class="popup-title">${title}</div>
      <table class="popup-table">${rows.join('')}</table>
      ${buildIfCityDebugReadout(props)}
      ${editControls}
    </div>`;
    currentPopup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: false,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '520px'
  }).setLngLat(atLngLat).setHTML(html).addTo(map);

  if (showWhatIfEdit && currentPopup && feature) {
    const popupEl = currentPopup.getElement();
    const selectEl = popupEl?.querySelector?.('[data-whatif="type"]');
    const applyBtn = popupEl?.querySelector?.('[data-whatif="apply"]');
    const closeBtn = popupEl?.querySelector?.('[data-whatif="close"]');
    if (selectEl) selectEl.value = detectPOICategoryFromProps(props) || whatIfType;
    applyBtn?.addEventListener('click', () => {
      const nextType = selectEl?.value || buildingTypeOf(feature);
      applyBuildingTypeChange(feature, nextType);
    });
    closeBtn?.addEventListener('click', () => closePopup());
  }
}

/* ======================= API (OSM) helpers ======================= */
async function runCityJob(city='Växjö', years=[2021,2022,2023,2024], onStatus=()=>{}) {
  const post = await fetch(`${API_BASE}/jobs`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ city, years })
  });
  if (!post.ok) throw new Error(`Cannot create job: ${post.status} ${await post.text().catch(()=> '')}`);
  const { job_id } = await post.json();

  let status;
  do {
    onStatus('running');
    await new Promise(r => setTimeout(r, 650));
    const res = await fetch(`${API_BASE}/jobs/${job_id}`);
    if (!res.ok) throw new Error(`Status error ${res.status}`);
    status = await res.json();
    onStatus(status.step || status.status || 'running');
  } while (status.status !== 'complete' && status.status !== 'failed');

  if (status.status === 'failed') throw new Error(status.step || 'failed');
  const b = await fetch(`${API_BASE}/city/${job_id}/buildings`).then(r => r.json());
  const buildingsUrl = b.geojson.startsWith('/') ? `${API_BASE}${b.geojson}` : b.geojson;
  return { buildingsUrl };
}

/* ---- DR helpers: encode categories/usage into numeric codes ---- */
const OSM_CAT_CODE = {
  residential: 0,
  house: 0,
  apartments: 0,
  terrace: 0,
  education: 1,
  school: 1,
  university: 1,
  health: 2,
  hospital: 2,
  clinic: 2,
  pharmacy: 2,
  retail: 3,
  supermarket: 3,
  commercial: 4,
  office: 4,
  public: 5,
  government: 5,
  religious: 6,
  industrial: 7
};

function osmCategoryCode(props = {}) {
  const raw = (props.category || props.objekttyp || props.building || '').toLowerCase();
  for (const key of Object.keys(OSM_CAT_CODE)) {
    if (raw.includes(key)) return OSM_CAT_CODE[key];
  }
  return 8; // other/unknown
}

function hybridCategoryCode(props = {}) {
  const osmCode = osmCategoryCode(props);
  if (osmCode !== 8) return osmCode;
  const localCode = localUsageCode(props);
  return Number.isFinite(localCode) ? localCode : 8;
}

const LOCAL_USAGE_CODE = {
  'Bostad': 0,
  'Samhällsfunktion': 1,
  'Verksamhet': 2,
  'Industri': 3,
  'Ekonomibyggnad': 4,
  'Komplementbyggnad': 5,
  'Övrig byggnad': 6
};

// Canonical building-type labels (used by both fairness charts + dropdown)
const BUILDING_TYPE_ORDER = [
  'farm',
  'religious',
  'hotel',
  'garage',
  'Other / unknown',
  'residential',
  'industrial',
  'education',
  'retail',
  'transportation',
  'public',
  'commercial'
];

// Synonyms → canonical building-type label
const BUILDING_TYPE_SYNONYMS = {
  farm: 'farm',
  barn: 'farm',
  agricultural: 'farm',
  agriculture: 'farm',
  stable: 'farm',

  religious: 'religious',
  church: 'religious',
  chapel: 'religious',
  mosque: 'religious',
  temple: 'religious',
  synagogue: 'religious',

  hotel: 'hotel',
  motel: 'hotel',
  hostel: 'hotel',
  guest_house: 'hotel',
  'guest house': 'hotel',
  resort: 'hotel',

  garage: 'garage',
  parking: 'garage',
  carport: 'garage',
  shed: 'garage',

  residential: 'residential',
  house: 'residential',
  apartments: 'residential',
  apartment: 'residential',
  terrace: 'residential',
  detached: 'residential',

  industrial: 'industrial',
  factory: 'industrial',
  manufactory: 'industrial',
  warehouse: 'industrial',

  education: 'education',
  school: 'education',
  university: 'education',
  college: 'education',
  kindergarten: 'education',

  retail: 'retail',
  supermarket: 'retail',
  shop: 'retail',
  mall: 'retail',
  store: 'retail',

  transportation: 'transportation',
  transport: 'transportation',
  station: 'transportation',
  railway: 'transportation',
  train: 'transportation',
  bus: 'transportation',
  terminal: 'transportation',
  aerodrome: 'transportation',

  public: 'public',
  civic: 'public',
  government: 'public',
  hospital: 'public',
  clinic: 'public',
  pharmacy: 'public',
  health: 'public',

  commercial: 'commercial',
  office: 'commercial',
  business: 'commercial',
};

// inverse lookup for category codes -> canonical building-type labels ---
const OSM_CODE_LABEL = {};
Object.entries(OSM_CAT_CODE).forEach(([name, code]) => {
  if (!(code in OSM_CODE_LABEL)) OSM_CODE_LABEL[code] = BUILDING_TYPE_SYNONYMS[name] || name;
});

const LOCAL_USAGE_CODE_LABEL = {};
Object.entries(LOCAL_USAGE_CODE).forEach(([name, code]) => {
  if (!(code in LOCAL_USAGE_CODE_LABEL)) {
    LOCAL_USAGE_CODE_LABEL[code] = name;
  }
});

const OSM_CODE_CANONICAL = {
  0: 'residential',
  1: 'education',
  2: 'public',
  3: 'retail',
  4: 'commercial',
  5: 'public',
  6: 'religious',
  7: 'industrial',
  8: 'Other / unknown'
};

const LOCAL_CODE_CANONICAL = {
  0: 'residential',          // Bostad
  1: 'public',               // Samhällsfunktion
  2: 'commercial',           // Verksamhet
  3: 'industrial',           // Industri
  4: 'farm',                 // Ekonomibyggnad (often agricultural)
  5: 'garage',               // Komplementbyggnad (sheds/garages)
  6: 'Other / unknown'
};

function canonicalBuildingType(label) {
  if (!label) return 'Other / unknown';
  const raw = String(label).trim();
  if (!raw) return 'Other / unknown';
  const low = raw.toLowerCase();

  // Exact match on known synonyms
  if (BUILDING_TYPE_SYNONYMS[low]) return BUILDING_TYPE_SYNONYMS[low];

  // Contains match (e.g., category strings with prefixes)
  for (const [key, val] of Object.entries(BUILDING_TYPE_SYNONYMS)) {
    if (low.includes(key)) return val;
  }

  // Preserve already canonical labels if user provided them
  if (BUILDING_TYPE_ORDER.includes(raw)) return raw;

  return 'Other / unknown';
}

/**
 * Map numeric category/usage code (metrics.categoryCode[i])
 * back to a canonical building-type label, depending on sourceMode.
 */
function categoryLabelFromCode(code) {
  if (!Number.isFinite(code)) return 'Other / unknown';

  if (sourceMode === 's1' || sourceMode === 'osm_s1') {
    // Lantmäteriet / local usage
    return LOCAL_CODE_CANONICAL[code] || 'Other / unknown';
  }
  // OSM-based categories
  return OSM_CODE_CANONICAL[code] || 'Other / unknown';
}



function localUsageCode(props = {}) {
  const o = props.objekttyp || '';
  if (Object.prototype.hasOwnProperty.call(LOCAL_USAGE_CODE, o)) {
    return LOCAL_USAGE_CODE[o];
  }
  return 0; // default bucket
}


/* ======================= Global state ======================= */
let sourceMode = 'osm';  // 'osm' | 's1' | 'osm_s1'
let viewMode   = 'all';  // 'all' | 'new'
let selectedYear = '';
let heightScale  = 3.2;
let lastCityName = 'Växjö';

let baseCityFC  = null;
let newbuildsFC = null;
let districtFC  = null;
// Växjö demand-weight state (population distributed proportionally by building floor area)
let vaxjoDistrictIndex  = null;   // [{feat, code}]
let vaxjoBuildingPopMap = null;   // Map<featureIndex, estimatedResidents>
let lantmaterietIndex = null; // [{centroid: [lng,lat], objekttyp: string}]

let firstFeat = null, secondFeat = null, routeGeoJSON = null;

// Categories to precompute for overall
var ALL_CATEGORIES = window.ALL_CATEGORIES || [
  'grocery',
  'hospital','pharmacy','dentistry','healthcare_center','veterinary',
  'university',
  'kindergarten','school_primary','school_high'
];
window.ALL_CATEGORIES = ALL_CATEGORIES;
const WHATIF_MOCK_TYPE_OPTIONS = ['residential', ...ALL_CATEGORIES];

// Fairness state
let fairActive = false;
let districtScoresSuppressed = false;
let fairCategory = '';           // single cat or 'mix'
let fairRecolorTick = 0;
let poiCache = {};               // per-category POI cache for current bbox
let currentPOIsFC = null;        // current marker POIs (single or union for mix)
let overallGini = null;
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
      .then(() => updateLayers())
      .catch(() => updateLayers());
    return;
  }
  updateLayers();
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
    style: DARK_BASEMAP_STYLE,
    center: [14.805, 56.879],
    zoom: 15,
    pitch: 45
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
    lastCityName = LOCAL_CITY_NAMES[initCityKey] || 'Växjö';

    if (sourceMode === 'osm_s1') {
      loadCityLocal(initCityKey)
        .then(autoComputeOverall)
        .catch(console.error);
    } else {
      loadCityOSM(lastCityName)
        .then(autoComputeOverall)
        .catch(console.error);
    }
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

  const triggerCityLoad = async () => {
    const cityKey = citySelect?.value || 'vaxjo';
    const displayName = LOCAL_CITY_NAMES[cityKey] || cityKey;
    lastCityName = displayName;

    resetUIState();
    poiCache = {};

    if (sourceMode === 'osm_s1') {
      await loadCityLocal(cityKey);
    } else {
      await loadCityOSM(displayName);
    }
    await autoComputeOverall();
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

/* ======================= District helpers ======================= */
function districtNameOf(props = {}, idx = 0) {
  const codeCandidates = [
    'regso','REGSO','regso_code','REGSO_CODE','REGSO_KOD',
    'deso','DESO','deso_code','DESO_CODE','DESO_KOD'
  ];
  const candidates = [
    'regsonamn','REGSONAMN','regso_namn','regso_namn2','REGSO_NAMN','REGSO_NAMN2',
    'name','Name','NAMN','namn',
    'deso_namn','DESO_NAMN','deso_name','DESO_NAME','REGSO','REGSO_CODE','DESO','DESO_CODE','DESO_KOD','Deso','deso'
  ];
  for (const k of candidates) {
    const v = props[k];
    if (v && String(v).trim()) return String(v).trim();
  }
  for (const k of codeCandidates) {
    const v = props[k];
    if (v && String(v).trim()) return `District ${String(v).trim()}`;
  }
  const keys = Object.keys(props || {});
  for (const k of keys) {
    const v = props[k];
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return `District ${idx + 1}`;
}

function hashStringToIndex(str, modulo) {
  let hash = 0;
  for (let i = 0; i < str.length; i += 1) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }
  return modulo ? (hash % modulo) : hash;
}

const DISTRICT_COLOR_VERSION = 1;
const DISTRICT_COLOR_ALPHA = 200;
const DISTRICT_COLOR_SATURATION = 0.68;
const DISTRICT_COLOR_LIGHTNESS = 0.5;

const DISTRICT_LABEL_VERSION = 2;
const DISTRICT_LABEL_BASE_ZOOM = 8.0;
const DISTRICT_LABEL_FULL_ZOOM = 9.0;

function hslToRgb(h, s, l) {
  const hue = ((h % 360) + 360) % 360;
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs(((hue / 60) % 2) - 1));
  const m = l - c / 2;
  let r = 0;
  let g = 0;
  let b = 0;
  if (hue < 60) {
    r = c; g = x; b = 0;
  } else if (hue < 120) {
    r = x; g = c; b = 0;
  } else if (hue < 180) {
    r = 0; g = c; b = x;
  } else if (hue < 240) {
    r = 0; g = x; b = c;
  } else if (hue < 300) {
    r = x; g = 0; b = c;
  } else {
    r = c; g = 0; b = x;
  }
  return [
    Math.round((r + m) * 255),
    Math.round((g + m) * 255),
    Math.round((b + m) * 255)
  ];
}

function ensureDistrictColors() {
  if (!districtFC?.features?.length) return;
  const needsUpdate = districtFC.features.some((feat) => {
    const props = feat?.properties || {};
    return props.__colorVersion !== DISTRICT_COLOR_VERSION;
  });
  if (!needsUpdate) return;

  const entries = districtFC.features.map((feat, idx) => ({
    idx,
    name: districtNameOf(feat?.properties || {}, idx)
  }));
  entries.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' }));

  const total = entries.length || 1;
  entries.forEach((entry, rank) => {
    const hue = (rank / total) * 360;
    const sat = (rank % 2 === 0) ? DISTRICT_COLOR_SATURATION : Math.min(0.92, DISTRICT_COLOR_SATURATION + 0.18);
    const light = (rank % 3 === 0) ? DISTRICT_COLOR_LIGHTNESS : Math.min(0.72, DISTRICT_COLOR_LIGHTNESS + 0.14);
    const [r, g, b] = hslToRgb(hue, sat, light);
    const feat = districtFC.features[entry.idx];
    feat.properties = {
      ...(feat.properties || {}),
      __fillColor: [r, g, b, DISTRICT_COLOR_ALPHA],
      __colorVersion: DISTRICT_COLOR_VERSION
    };
  });
}

function districtFillColor(props = {}, idx = 0) {
  if (Array.isArray(props.__fillColor)) return props.__fillColor;
  return [80, 80, 80, 60];
}

function ensureDistrictLabelThresholds() {
  if (!districtFC?.features?.length) return;
  const needsUpdate = districtFC.features.some((feat) => {
    const props = feat?.properties || {};
    return props.__labelVersion !== DISTRICT_LABEL_VERSION;
  });
  if (!needsUpdate) return;

  const maxZoom = (map && typeof map.getMaxZoom === 'function') ? map.getMaxZoom() : DISTRICT_LABEL_FULL_ZOOM;
  const minZoom = (map && typeof map.getMinZoom === 'function') ? map.getMinZoom() : 0;
  const baseZoom = Math.max(minZoom, DISTRICT_LABEL_BASE_ZOOM);
  const fullZoom = Math.min(maxZoom, DISTRICT_LABEL_FULL_ZOOM);
  const zoomSpan = Math.max(0.1, fullZoom - baseZoom);

  const entries = districtFC.features.map((feat, idx) => {
    let areaSqKm = Number(feat?.properties?.__labelAreaSqKm);
    if (!Number.isFinite(areaSqKm)) {
      try {
        areaSqKm = turf.area(feat) / 1e6;
      } catch (_) {
        areaSqKm = null;
      }
    }
    return { idx, areaSqKm };
  }).filter(entry => Number.isFinite(entry.areaSqKm));

  const sorted = entries.slice().sort((a, b) => b.areaSqKm - a.areaSqKm);
  const total = sorted.length || 1;

  sorted.forEach((entry, rank) => {
    const percentile = (rank + 1) / total;
    const minZoomForLabel = baseZoom + (percentile * zoomSpan);
    const feat = districtFC.features[entry.idx];
    feat.properties = {
      ...(feat.properties || {}),
      __labelAreaSqKm: entry.areaSqKm,
      __labelMinZoom: minZoomForLabel,
      __labelVersion: DISTRICT_LABEL_VERSION
    };
  });
}

function districtLabelData() {
  if (!districtFC?.features?.length) return [];
  ensureDistrictLabelThresholds();
  const zoom = (map && typeof map.getZoom === 'function') ? map.getZoom() : 0;
  return districtFC.features.map((f, idx) => {
    const pos = (() => {
      const calculators = [
        () => turf.pointOnFeature(f),
        () => turf.centerOfMass(f),
        () => turf.centroid(f)
      ];
      for (const calc of calculators) {
        try {
          const pt = calc();
          const coords = pt?.geometry?.coordinates;
          if (Array.isArray(coords) && coords.length >= 2) return coords;
        } catch (_) { /* ignore and try next */ }
      }
      return null;
    })();
    const props = f.properties || {};
    const name = props.__districtName || districtNameOf(props, idx);
    const minZoom = Number.isFinite(props.__labelMinZoom) ? props.__labelMinZoom : 0;
    if (zoom < minZoom) return null;
    return (pos && name) ? { position: pos, name, properties: props } : null;
  }).filter(Boolean);
}

function isFiniteLngLatPair(value) {
  return Array.isArray(value)
    && value.length >= 2
    && Number.isFinite(value[0])
    && Number.isFinite(value[1]);
}

function detectDistrictCoordSystem(fc) {
  if (!fc?.features?.length) return 'wgs84';

  const sample = [];
  const collect = (coords) => {
    if (!coords || sample.length >= 300) return;
    if (isFiniteLngLatPair(coords)) {
      sample.push(coords);
      return;
    }
    if (Array.isArray(coords)) {
      coords.forEach(collect);
    }
  };

  for (const feat of fc.features) {
    collect(feat?.geometry?.coordinates);
    if (sample.length >= 300) break;
  }
  if (!sample.length) return 'wgs84';

  const latInRange = sample.filter(([, lat]) => Math.abs(lat) <= 90).length;
  const lngInRange = sample.filter(([lng]) => Math.abs(lng) <= 180).length;
  const lonLatShare = Math.min(latInRange, lngInRange) / sample.length;
  if (lonLatShare > 0.95) return 'wgs84';

  return 'epsg3006';
}

function convertDistrictCoords(coords, srcCRS, dstCRS) {
  if (!coords) return coords;
  if (isFiniteLngLatPair(coords)) {
    const [x, y] = coords;
    const converted = proj4(srcCRS, dstCRS, [x, y]);
    return [converted[0], converted[1], ...coords.slice(2)];
  }
  if (!Array.isArray(coords)) return coords;
  return coords.map(part => convertDistrictCoords(part, srcCRS, dstCRS));
}

function normalizeDistrictGeometryCRS(fc) {
  if (!fc?.features?.length || typeof proj4 !== 'function') return fc;
  const crsName = String(fc?.crs?.properties?.name || '').toLowerCase();
  const likelyEPSG3006 = crsName.includes('3006') || detectDistrictCoordSystem(fc) === 'epsg3006';
  if (!likelyEPSG3006) return fc;

  const SRC = 'EPSG:3006';
  const DST = 'EPSG:4326';
  if (!proj4.defs(SRC)) {
    proj4.defs(SRC, '+proj=utm +zone=33 +ellps=GRS80 +units=m +no_defs');
  }

  (fc.features || []).forEach((feat) => {
    if (!feat?.geometry?.coordinates) return;
    feat.geometry.coordinates = convertDistrictCoords(feat.geometry.coordinates, SRC, DST);
  });
  return fc;
}

async function ensureDistrictData() {
  if (!activeDistrictURL) return null;
  if (districtFC) return districtFC;
  if (districtLoadPromise) return districtLoadPromise;
  districtLoadPromise = fetch(activeDistrictURL)
    .then(r => {
      if (!r.ok) throw new Error(`District fetch failed (${r.status})`);
      return r.json();
    })
    .then(fc => {
      normalizeDistrictGeometryCRS(fc);
      // Remove rural/non-urban districts so their buildings, meso cells,
      // and macro polygons are never loaded or displayed.
      const EXCLUDED_DISTRICTS = [
        'landsbygd', 'stadsnära landsbygd',
        'ingelstad', 'gemla', 'braås', 'rottne', 'lammhult',
        'växjö landsbygd'
      ];
      if (fc?.features) {
        fc.features = fc.features.filter((feat, idx) => {
          const name = normalizeDistrictName(
            districtNameOf(feat?.properties || {}, idx)
          );
          return !EXCLUDED_DISTRICTS.includes(name);
        });
      }
      districtFC = fc;
      initVaxjoDemandWeights();
      mezoMaskPolygon = null;
      districtLoadError = null;
      return fc;
    })
    .catch(err => { districtLoadError = err; districtLoadPromise = null; throw err; });
  return districtLoadPromise;
}

function ensureDistrictBoundaryLines() {
  if (!districtFC?.features?.length) return null;
  if (districtBoundaryFC) return districtBoundaryFC;

  const lines = [];
  for (const feat of districtFC.features) {
    try {
      const line = turf.polygonToLine(feat);
      if (line?.type === 'Feature') {
        lines.push(line);
      } else if (line?.type === 'FeatureCollection' && Array.isArray(line.features)) {
        lines.push(...line.features);
      }
    } catch (_) { /* ignore bad geometry */ }
  }
  districtBoundaryFC = turf.featureCollection(lines);
  return districtBoundaryFC;
}

/* ===== Coastline clipping: trim district/mezo polygons to land ===== */

/**
 * Build a land-mass polygon from building centroids.
 * Uses turf.concave (alpha shape) to trace the actual building footprint,
 * then buffers generously so no land is lost.
 */
function buildLandHullFromBuildings() {
  if (!baseCityFC?.features?.length) return null;

  const points = [];
  for (const feat of baseCityFC.features) {
    if (!feat?.geometry) continue;
    try {
      const c = turf.centroid(feat)?.geometry?.coordinates;
      if (Array.isArray(c) && c.length >= 2 && Number.isFinite(c[0]) && Number.isFinite(c[1])) {
        points.push(turf.point(c));
      }
    } catch (_) { /* skip invalid geometry */ }
  }
  if (points.length < 4) return null;

  const pointsFC = turf.featureCollection(points);
  let hull = null;

  // Try concave hull first — follows coastline much better than convex
  // maxEdge in km: smaller = tighter fit but slower and may fragment.
  // 1.5 km is a good balance for city-scale data.
  try {
    hull = turf.concave(pointsFC, { maxEdge: 1.5, units: 'kilometers' });
  } catch (_) { /* concave can fail on degenerate layouts */ }

  // Fallback to convex hull
  if (!hull) {
    try {
      hull = turf.convex(pointsFC);
    } catch (_) { /* extremely unlikely */ }
  }
  if (!hull) return null;

  // Buffer generously (300 m) so we never clip actual land edges.
  // This is much larger than any building-to-coastline gap,
  // but small enough to cut the sea parts that extend 500 m+ out.
  try {
    const buffered = turf.buffer(hull, 0.3, { units: 'kilometers' });
    if (buffered) return buffered;
  } catch (_) { /* buffer can fail on complex concave shapes */ }

  return hull;
}

/**
 * Clip a single Polygon or MultiPolygon feature to a land polygon
 * using turf.intersect. Preserves original properties.
 * Returns the clipped feature, or the original if clipping fails.
 */
function clipFeatureToLandHull(feature, landPoly) {
  if (!feature?.geometry || !landPoly?.geometry) return feature;
  const geomType = feature.geometry.type;

  try {
    if (geomType === 'Polygon') {
      const clipped = turf.intersect(feature, landPoly);
      if (clipped) {
        clipped.properties = { ...(feature.properties || {}) };
        return clipped;
      }
      // Entirely outside hull — check if it has buildings before dropping
      return feature; // keep as safe fallback
    }

    if (geomType === 'MultiPolygon') {
      const clippedParts = [];
      for (const coords of feature.geometry.coordinates) {
        try {
          const subPoly = turf.polygon(coords);
          const clipped = turf.intersect(subPoly, landPoly);
          if (clipped) {
            if (clipped.geometry.type === 'Polygon') {
              clippedParts.push(clipped.geometry.coordinates);
            } else if (clipped.geometry.type === 'MultiPolygon') {
              clippedParts.push(...clipped.geometry.coordinates);
            }
          }
        } catch (_) {
          clippedParts.push(coords); // keep original sub-polygon on failure
        }
      }
      if (!clippedParts.length) return feature; // safe fallback
      const result = clippedParts.length === 1
        ? turf.polygon(clippedParts[0], { ...(feature.properties || {}) })
        : turf.multiPolygon(clippedParts, { ...(feature.properties || {}) });
      return result;
    }
  } catch (err) {
    console.warn('clipFeatureToLandHull failed, keeping original:', err);
  }
  return feature;
}

/**
 * Clip all district polygons to the building-derived land hull.
 * Uses a signature to avoid redundant work when called multiple times
 * with the same data.
 */
function ensureDistrictLandClipping() {
  if (!districtFC?.features?.length || !baseCityFC?.features?.length) return;

  // Build a signature so we only clip once per dataset combination
  const cityCount = baseCityFC.features.length;
  const districtCount = districtFC.features.length;
  const citySampleId =
    baseCityFC.features[0]?.properties?.id ??
    baseCityFC.features[0]?.properties?.osm_id ??
    baseCityFC.features[0]?.properties?.byggnadsid ?? '';
  const signature = `${activeDistrictURL || 'none'}|${districtCount}|${cityCount}|${citySampleId}`;
  if (districtLandClipSignature === signature) return;

  const landHull = buildLandHullFromBuildings();
  if (!landHull) {
    districtLandClipSignature = signature;
    return;
  }

  let changed = 0;
  for (const feat of districtFC.features) {
    if (!feat?.geometry) continue;
    const clipped = clipFeatureToLandHull(feat, landHull);
    if (clipped !== feat && clipped?.geometry) {
      feat.geometry = clipped.geometry;
      changed += 1;
    }
  }

  if (changed > 0) {
    // Force rebuild of boundary lines and mezo mask from clipped geometry
    districtBoundaryFC = null;
    mezoMaskPolygon = null;
    console.info(`Coastline clip: reshaped ${changed} district polygon(s) to land hull (${baseCityFC.features.length} buildings).`);
  }

  districtLandClipSignature = signature;
}

function buildingScoreForDistrict(f) {
  if (!f?.properties) return null;
  if (fairActive && Number.isFinite(f.properties?.fair?.score)) return f.properties.fair.score;
  if (Number.isFinite(f.properties?.fair_overall?.score)) return f.properties.fair_overall.score;
  return null;
}

async function refreshDistrictScores() {
  if (!baseCityFC?.features?.length) { districtScoreTick++; return; }
  try { await ensureDistrictData(); } catch (e) { console.warn('District data missing', e); districtScoreTick++; return; }
  ensureDistrictLandClipping();
  if (!districtFC?.features?.length) { districtScoreTick++; return; }
  if (districtScoresSuppressed) { districtScoreTick++; return; }

  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const focusedPOICat = poiCategoryOf(selectedPOIFeature);
  const focusedPOICoords = (() => {
    const geom = selectedPOIFeature?.geometry;
    if (!geom) return null;
    if (geom.type === 'Point' && Array.isArray(geom.coordinates)) return geom.coordinates;
    try {
      const coords = turf.centroid(selectedPOIFeature).geometry.coordinates;
      return Array.isArray(coords) ? coords : null;
    } catch (_) {
      return null;
    }
  })();
  const pts = [];
  for (const f of baseCityFC.features) {
    const s = buildingScoreForDistrict(f);
    const props = f?.properties || {};
    const overallScore = Number.isFinite(props?.fair_overall?.score) ? props.fair_overall.score : null;
    const catScores = {};
    if (props?.fair_multi) {
      for (const cat of catList) {
        const val = props.fair_multi?.[cat]?.score;
        if (Number.isFinite(val)) catScores[cat] = val;
      }
    }
    const hasAnyScore = Number.isFinite(s) || Number.isFinite(overallScore) || Object.keys(catScores).length > 0;
    if (!hasAnyScore) continue;
    const c = turf.centroid(f).geometry.coordinates;
    let focusedScore = null;
    if (focusedPOICoords && focusedPOICat && focusedPOICat !== 'default') {
      const dMeters = haversineMeters(c, focusedPOICoords);
      const tSec = estimateTravelTimeSecondsFromMeters(dMeters, fairnessTravelMode);
      focusedScore = scoreFromTimeSeconds(focusedPOICat, tSec, fairnessTravelMode);
    }
    pts.push(turf.point(c, { score: s, overall: overallScore, cats: catScores, focused: focusedScore }));
  }
  const ptsFC = turf.featureCollection(pts);

  (districtFC.features || []).forEach((feat, idx) => {
    const name = districtNameOf(feat.properties, idx) || `District ${idx + 1}`;
    const within = turf.pointsWithinPolygon(ptsFC, feat);
    const scores = within.features.map(p => p.properties?.score).filter(Number.isFinite);
    const overallScores = within.features.map(p => p.properties?.overall).filter(Number.isFinite);
    const focusedScores = within.features.map(p => p.properties?.focused).filter(Number.isFinite);
    const sums = {};
    const counts = {};
    for (const cat of catList) {
      sums[cat] = 0;
      counts[cat] = 0;
    }
    within.features.forEach((p) => {
      const cats = p.properties?.cats || {};
      for (const cat of catList) {
        const val = cats?.[cat];
        if (!Number.isFinite(val)) continue;
        sums[cat] += val;
        counts[cat] += 1;
      }
    });
    const fairByCat = {};
    for (const cat of catList) {
      fairByCat[cat] = counts[cat] ? sums[cat] / counts[cat] : 0;
    }
    const mean = scores.length ? scores.reduce((a,b)=>a+b,0)/scores.length : null;
    const overallMean = overallScores.length ? overallScores.reduce((a, b) => a + b, 0) / overallScores.length : null;
    const focusedMean = focusedScores.length ? focusedScores.reduce((a, b) => a + b, 0) / focusedScores.length : null;
    feat.properties = {
      ...(feat.properties || {}),
      __districtName: name,
      __score: mean,
      __count: scores.length,
      __fairOverall: overallMean,
      __fairByCat: fairByCat,
      __fairFocused: focusedMean,
      __fairFocusedCat: focusedMean != null ? focusedPOICat : null
    };
  });

  districtScoreTick++;
  markAggregateSelectionsFromBuildings((baseCityFC?.features || []).filter(f => f?.properties?._drSelected));
  if (parallelCoordsOpen && currentParallelCoordsMode() === 'district') {
    updateParallelCoordsPanel();
  }
}

function resolveMezoResolution() {
  if (mezoResolution != null) return mezoResolution;
  const h3 = window.h3;
  if (!h3) return null;
  const getEdgeKm = (res) => {
    if (typeof h3.getHexagonEdgeLengthAvg === 'function') return h3.getHexagonEdgeLengthAvg(res, 'km');
    if (typeof h3.getHexagonEdgeLengthAvgKm === 'function') return h3.getHexagonEdgeLengthAvgKm(res);
    if (typeof h3.edgeLength === 'function') return h3.edgeLength(res, 'km');
    return null;
  };
  let bestRes = null;
  let bestDiff = Infinity;
  for (let res = 0; res <= 15; res += 1) {
    const km = getEdgeKm(res);
    if (!Number.isFinite(km)) continue;
    const diff = Math.abs(km - selectedMezoHexEdgeKm);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestRes = res;
    }
  }
  mezoResolution = bestRes;
  return mezoResolution;
}

function h3LatLngToCell(h3, lat, lng, res) {
  if (!h3) return null;
  if (typeof h3.latLngToCell === 'function') return h3.latLngToCell(lat, lng, res);
  if (typeof h3.geoToH3 === 'function') return h3.geoToH3(lat, lng, res);
  return null;
}

function h3PolygonToCells(h3, polygon, res) {
  if (!h3 || !polygon) return [];
  if (typeof h3.polygonToCells === 'function') {
    return h3.polygonToCells(polygon, res);
  }
  if (typeof h3.polyfill === 'function') {
    return h3.polyfill(polygon.coordinates, res, true);
  }
  return [];
}

function h3CellToLatLng(h3, cell) {
  if (!h3 || !cell) return null;
  if (typeof h3.cellToLatLng === 'function') return h3.cellToLatLng(cell);
  if (typeof h3.h3ToGeo === 'function') return h3.h3ToGeo(cell);
  return null;
}

function mezoFallbackScoreForCell(cell, h3, poiByCat) {
  if (!fairActive || !fairCategory || !poiByCat) return null;
  const latLng = h3CellToLatLng(h3, cell);
  if (!Array.isArray(latLng) || latLng.length < 2) return null;
  const coord = [latLng[1], latLng[0]];
  const cats = fairCategory === 'mix'
    ? selectedPOIMix.map(item => item.cat).filter(Boolean)
    : [fairCategory];
  if (!cats.length) return null;
  let best = null;
  for (const cat of cats) {
    const arr = poiByCat[cat] || [];
    if (!arr.length) continue;
    let bestDist = Infinity;
    for (let i = 0; i < arr.length; i += 1) {
      const dMeters = haversineMeters(coord, arr[i].c);
      if (dMeters < bestDist) bestDist = dMeters;
    }
    if (!Number.isFinite(bestDist)) continue;
    const tSec = estimateTravelTimeSecondsFromMeters(bestDist, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, tSec, fairnessTravelMode);
    if (!Number.isFinite(score)) continue;
    if (best == null || score > best) best = score;
  }
  return best;
}


function ensureMezoMaskPolygon() {
  if (mezoMaskPolygon || !districtFC?.features?.length) return mezoMaskPolygon;
  const polys = districtFC.features
    .filter(f => f?.geometry && (f.geometry.type === 'Polygon' || f.geometry.type === 'MultiPolygon'));
  if (!polys.length) return mezoMaskPolygon;
  try {
    let merged = polys[0];
    for (let i = 1; i < polys.length; i += 1) {
      merged = turf.union(merged, polys[i]) || merged;
    }
    mezoMaskPolygon = merged;
  } catch (err) {
    console.warn('Mezo mask union failed', err);
    mezoMaskPolygon = null;
  }
  return mezoMaskPolygon;
}

async function alignBuildingCoverageToDistricts(fc, contextLabel = 'dataset') {
  const features = Array.isArray(fc?.features) ? fc.features : null;
  if (!features?.length) return;
  if (!activeDistrictCityKey) {
    console.warn(`Skipping district alignment for ${contextLabel}: no district dataset mapped to the selected city.`);
    return;
  }
  try {
    await ensureDistrictData();
  } catch (err) {
    console.warn(`Skipping district alignment for ${contextLabel}: district data unavailable.`, err);
    return;
  }

  const districts = (districtFC?.features || []).filter((feat) => {
    const type = feat?.geometry?.type;
    return type === 'Polygon' || type === 'MultiPolygon';
  });
  if (!districts.length) return;

  const centroidPoints = [];
  features.forEach((feat, idx) => {
    if (!feat?.geometry) return;
    try {
      const center = turf.centroid(feat);
      centroidPoints.push(turf.point(center.geometry.coordinates, { __idx: idx }));
    } catch (_) {
      /* ignore invalid building geometry */
    }
  });
  if (!centroidPoints.length) return;

  let within = null;
  try {
    within = turf.pointsWithinPolygon(
      turf.featureCollection(centroidPoints),
      turf.featureCollection(districts)
    );
  } catch (err) {
    console.warn(`Skipping district alignment for ${contextLabel}: polygon test failed.`, err);
    return;
  }

  const keep = new Set((within?.features || []).map((pt) => pt?.properties?.__idx).filter(Number.isInteger));
  const filtered = features.filter((_, idx) => keep.has(idx));
  const before = features.length;
  const kept = filtered.length;

  if (!kept) {
    console.warn(`Skipping district alignment for ${contextLabel}: zero buildings matched district polygons (likely CRS/geometry mismatch).`);
    return;
  }

  const keepRatio = kept / Math.max(1, before);
  if (keepRatio < 0.05) {
    console.warn(`Skipping district alignment for ${contextLabel}: only ${kept}/${before} matched districts (<5%), would likely break bbox/POI fetch.`);
    return;
  }

  const removed = before - kept;
  if (removed > 0) {
    console.info(`Aligned ${contextLabel} to district boundary (${kept}/${before} kept, ${removed} removed).`);
  }
  fc.features = filtered;
}

async function refreshMezoScores() {
  if (!baseCityFC?.features?.length) { mezoScoreTick++; return; }
  const h3 = window.h3;
  const res = resolveMezoResolution();
  if (!h3 || res == null) { mezoHexData = []; mezoScoreTick++; return; }

  try {
    await ensureDistrictData();
    ensureDistrictLandClipping();
  } catch (_) {
    /* district mask optional */
  }

  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const poiByCat = fairActive ? (buildPOIMapFromCurrent() || {}) : null;
  const focusedPOICat = poiCategoryOf(selectedPOIFeature);
  const focusedPOICoords = (() => {
    const geom = selectedPOIFeature?.geometry;
    if (!geom) return null;
    if (geom.type === 'Point' && Array.isArray(geom.coordinates)) return geom.coordinates;
    try {
      const coords = turf.centroid(selectedPOIFeature).geometry.coordinates;
      return Array.isArray(coords) ? coords : null;
    } catch (_) {
      return null;
    }
  })();

  const mask = ensureMezoMaskPolygon();
  let coverageCells = null;
  if (mask?.geometry) {
    const cellSet = new Set();
    const pushCells = (geom) => {
      const polygon = { type: 'Polygon', coordinates: geom.coordinates };
      h3PolygonToCells(h3, polygon, res).forEach(cell => cellSet.add(cell));
    };
    if (mask.geometry.type === 'Polygon') {
      pushCells(mask.geometry);
    } else if (mask.geometry.type === 'MultiPolygon') {
      mask.geometry.coordinates.forEach((coords) => {
        pushCells({ coordinates: coords });
      });
    }
    coverageCells = Array.from(cellSet);
  }

  const hexMap = new Map();
  for (const f of baseCityFC.features) {
    const props = f?.properties || {};
    const score = buildingScoreForDistrict(f);
    const overallScore = Number.isFinite(props?.fair_overall?.score) ? props.fair_overall.score : null;
    const catScores = {};
    if (props?.fair_multi) {
      for (const cat of catList) {
        const val = props.fair_multi?.[cat]?.score;
        if (Number.isFinite(val)) catScores[cat] = val;
      }
    }
    const c = turf.centroid(f).geometry.coordinates;
    let cell = null;
    try {
      cell = h3LatLngToCell(h3, c[1], c[0], res);
    } catch (_) {
      cell = null;
    }
    if (!cell) continue;

    let focusedScore = null;
    if (focusedPOICoords && focusedPOICat && focusedPOICat !== 'default') {
      const dMeters = haversineMeters(c, focusedPOICoords);
      const tSec = estimateTravelTimeSecondsFromMeters(dMeters, fairnessTravelMode);
      focusedScore = scoreFromTimeSeconds(focusedPOICat, tSec, fairnessTravelMode);
    }

    if (!hexMap.has(cell)) {
      hexMap.set(cell, {
        hex: cell,
        total: 0,
        sum: 0,
        count: 0,
        overallSum: 0,
        overallCount: 0,
        catSums: Object.fromEntries(catList.map(cat => [cat, 0])),
        catCounts: Object.fromEntries(catList.map(cat => [cat, 0])),
        focusedSum: 0,
        focusedCount: 0,
        prevSum: 0,
        prevCount: 0
      });
    }
    const entry = hexMap.get(cell);
    entry.total += 1;
    if (Number.isFinite(score)) {
      entry.sum += score;
      entry.count += 1;
    }
    if (Number.isFinite(overallScore)) {
      entry.overallSum += overallScore;
      entry.overallCount += 1;
    }
    for (const cat of catList) {
      const val = catScores?.[cat];
      if (!Number.isFinite(val)) continue;
      entry.catSums[cat] += val;
      entry.catCounts[cat] += 1;
    }
    if (Number.isFinite(focusedScore)) {
      entry.focusedSum += focusedScore;
      entry.focusedCount += 1;
    }
    const prevScore = Number.isFinite(props?.__prevScore) ? props.__prevScore : null;
    if (Number.isFinite(prevScore)) {
      entry.prevSum += prevScore;
      entry.prevCount += 1;
    }
  }

  const data = [];
  const buildEntry = (entry) => {
    const fairByCat = {};
    for (const cat of catList) {
      const count = entry.catCounts[cat];
      fairByCat[cat] = count ? entry.catSums[cat] / count : null;
    }
    const mean = entry.count ? entry.sum / entry.count : null;
    const overallMean = entry.overallCount ? entry.overallSum / entry.overallCount : null;
    const focusedMean = entry.focusedCount ? entry.focusedSum / entry.focusedCount : null;
    const prevMean = entry.prevCount ? entry.prevSum / entry.prevCount : null;
    return {
      hex: entry.hex,
      __score: mean,
      __count: entry.total,
      __fairOverall: overallMean,
      __fairByCat: fairByCat,
      __fairFocused: focusedMean,
      __fairFocusedCat: focusedMean != null ? focusedPOICat : null,
      __prevScore: prevMean
    };
  };

  if (coverageCells?.length) {
    coverageCells.forEach((cell) => {
      const entry = hexMap.get(cell);
      const fallbackScore = entry?.total >= MEZO_MIN_COUNT ? null : mezoFallbackScoreForCell(cell, h3, poiByCat);
      if (!entry) {
        // Empty mezo cells should stay visually neutral (gray):
        // there are no buildings to aggregate fairness for.
        data.push(withMezoPrevScore({
          hex: cell,
          __score: null,
          __count: 0,
          __fairOverall: null,
          __fairByCat: {},
          __fairFocused: null,
          __fairFocusedCat: null
        }));
        return;
      }
      if (entry.total < MEZO_MIN_COUNT) {
        const fairByCat = fairCategory && fairCategory !== 'mix' && Number.isFinite(fallbackScore)
          ? { [fairCategory]: fallbackScore }
          : {};
        data.push(withMezoPrevScore({
          hex: entry.hex,
          __score: Number.isFinite(fallbackScore) ? fallbackScore : null,
          __count: entry.total,
          __fairOverall: null,
          __fairByCat: fairByCat,
          __fairFocused: null,
          __fairFocusedCat: null,
          __prevScore: entry.prevCount ? (entry.prevSum / entry.prevCount) : null
        }));
        return;
      }
      data.push(withMezoPrevScore(buildEntry(entry)));
    });
  } else {
    hexMap.forEach((entry) => {
      if (entry.total < MEZO_MIN_COUNT) return;
      data.push(withMezoPrevScore(buildEntry(entry)));
    });
  }

  mezoHexData = data;
  mezoScoreTick++;
  markAggregateSelectionsFromBuildings((baseCityFC?.features || []).filter(f => f?.properties?._drSelected));
  if (parallelCoordsOpen && currentParallelCoordsMode() === 'mezo') {
    updateParallelCoordsPanel();
  }
}




/* ======================= Loaders ======================= */
async function loadSentinelData() {
  const [city, stats] = await Promise.all([
    fetch(CITY_URL).then(r => r.json()),
    fetch(STATS_URL).then(r => r.json()).catch(()=>null)
  ]);
  baseCityFC  = city;
  districtLandClipSignature = '';
  newbuildsFC = stats || city;
  await alignBuildingCoverageToDistricts(baseCityFC, 'base buildings');
  if (newbuildsFC !== baseCityFC) {
    await alignBuildingCoverageToDistricts(newbuildsFC, 'new buildings');
  }
  refreshBuildingTypeDropdown();
  setSelectedBuildingType('', true);
  fitToData(baseCityFC);
  if (districtView) await refreshDistrictScores();
  if (mezoView) await refreshMezoScores();
  updateLayers();
  updateDRAndPCBadges();
}

/**
 * Enrich OSM-fetched buildings with properties from a local building GeoJSON
 * (e.g. byggnad_malmo.geojson). Matches features by proximity of centroids.
 * Properties from the local file are merged into the OSM feature's properties
 * without overwriting existing OSM keys.
 */
function enrichOSMWithLocalBuildings(osmFC, localFC) {
  if (!osmFC?.features?.length || !localFC?.features?.length) return;

  // Build a simple spatial index: round centroids to ~100 m grid cells
  const GRID_PRECISION = 3; // ~110 m at equator
  const localIndex = new Map();
  for (const feat of localFC.features) {
    try {
      const c = turf.centroid(feat).geometry.coordinates;
      const key = `${c[0].toFixed(GRID_PRECISION)},${c[1].toFixed(GRID_PRECISION)}`;
      if (!localIndex.has(key)) localIndex.set(key, []);
      localIndex.get(key).push({ feat, lon: c[0], lat: c[1] });
    } catch { /* skip malformed */ }
  }

  const MATCH_THRESHOLD_M = 30; // max distance to consider a match
  let enriched = 0;

  for (const osmFeat of osmFC.features) {
    try {
      const c = turf.centroid(osmFeat).geometry.coordinates;
      const keyBase = [c[0].toFixed(GRID_PRECISION), c[1].toFixed(GRID_PRECISION)];

      // Check the cell and its 8 neighbors
      let bestDist = Infinity;
      let bestLocal = null;
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          const probe = `${(parseFloat(keyBase[0]) + dx * Math.pow(10, -GRID_PRECISION)).toFixed(GRID_PRECISION)},${(parseFloat(keyBase[1]) + dy * Math.pow(10, -GRID_PRECISION)).toFixed(GRID_PRECISION)}`;
          const bucket = localIndex.get(probe);
          if (!bucket) continue;
          for (const entry of bucket) {
            const d = turf.distance([c[0], c[1]], [entry.lon, entry.lat], { units: 'meters' });
            if (d < bestDist) {
              bestDist = d;
              bestLocal = entry.feat;
            }
          }
        }
      }

      if (bestLocal && bestDist <= MATCH_THRESHOLD_M) {
        const localProps = bestLocal.properties || {};
        const osmProps = osmFeat.properties || {};
        // Merge local props without overwriting existing OSM props
        for (const [k, v] of Object.entries(localProps)) {
          if (!(k in osmProps) && v != null) {
            osmProps[k] = v;
          }
        }
        // Also store the local height if present and OSM lacks it
        if (!osmProps.height && (localProps.height || localProps.HOJD || localProps.hojd)) {
          osmProps.height = localProps.height || localProps.HOJD || localProps.hojd;
        }
        enriched++;
      }
    } catch { /* skip */ }
  }

  console.log(`enrichOSMWithLocalBuildings: matched ${enriched} of ${osmFC.features.length} buildings`);
}

/**
 * Enrich OSM buildings with properties from a local GeoJSON (e.g. byggnad_malmo.geojson).
 * Matches by centroid proximity (~30 m threshold).
 */
function enrichOSMWithLocalBuildings(osmFC, localFC) {
  if (!osmFC?.features?.length || !localFC?.features?.length) return;

  // Keys where local Lantmäteriet data should ALWAYS override OSM values
  // because OSM typically has generic/useless values for these
  const LOCAL_PREFERRED_KEYS = new Set([
    'objekttyp',      // OSM has "yes", local has "Bostad"/"Samhällsfunktion"/etc.
    'objekttypnr',    // OSM has generic code, local has proper Lantmäteriet code
    'andamal1',       // OSM may be empty/wrong, local has Swedish building purpose
    'husnummer',      // local is authoritative
    'huvudbyggnad',   // local is authoritative
    'insamlingslage'  // local is authoritative
  ]);

  const GRID = 3;
  const localIndex = new Map();
  for (const feat of localFC.features) {
    try {
      const c = turf.centroid(feat).geometry.coordinates;
      const key = `${c[0].toFixed(GRID)},${c[1].toFixed(GRID)}`;
      if (!localIndex.has(key)) localIndex.set(key, []);
      localIndex.get(key).push({ feat, lon: c[0], lat: c[1] });
    } catch { /* skip */ }
  }

  const THRESHOLD_M = 30;
  let enriched = 0;
  const step = Math.pow(10, -GRID);

  for (const osmFeat of osmFC.features) {
    try {
      const c = turf.centroid(osmFeat).geometry.coordinates;
      const baseLon = parseFloat(c[0].toFixed(GRID));
      const baseLat = parseFloat(c[1].toFixed(GRID));

      let bestDist = Infinity, bestLocal = null;
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          const probe = `${(baseLon + dx * step).toFixed(GRID)},${(baseLat + dy * step).toFixed(GRID)}`;
          const bucket = localIndex.get(probe);
          if (!bucket) continue;
          for (const entry of bucket) {
            const d = turf.distance([c[0], c[1]], [entry.lon, entry.lat], { units: 'meters' });
            if (d < bestDist) { bestDist = d; bestLocal = entry.feat; }
          }
        }
      }

      if (bestLocal && bestDist <= THRESHOLD_M) {
        const lp = bestLocal.properties || {};
        const op = osmFeat.properties || (osmFeat.properties = {});
        for (const [k, v] of Object.entries(lp)) {
          if (v == null) continue;
          if (LOCAL_PREFERRED_KEYS.has(k)) {
            // Always use local value for these keys
            op[k] = v;
          } else if (!(k in op)) {
            // For other keys, only add if OSM doesn't have it
            op[k] = v;
          }
        }
        // Use byggnadsnamn1 as display name if OSM name is missing
        if (!op.name && lp.byggnadsnamn1) {
          op.name = lp.byggnadsnamn1;
        }
        if (!op.height && (lp.height || lp.HOJD || lp.hojd)) {
          op.height = lp.height || lp.HOJD || lp.hojd;
        }
        enriched++;
      }
    } catch { /* skip */ }
  }
  console.log(`enrichOSMWithLocalBuildings: matched ${enriched}/${osmFC.features.length}`);
}

/**
 * Extract total population number from a population entry.
 */
function extractDistrictTotalPopulation(entry) {
  if (!entry) return NaN;

  for (const key of ['total', 'Total', 'population', 'Population', 'totalt', 'Totalt']) {
    if (Number.isFinite(Number(entry[key]))) return Number(entry[key]);
  }

  const yearKeys = Object.keys(entry).filter(k => /^\d{4}$/.test(k)).sort();
  if (yearKeys.length) {
    const latestYear = yearKeys[yearKeys.length - 1];
    const yearData = entry[latestYear];
    if (Number.isFinite(Number(yearData))) return Number(yearData);
    if (yearData && typeof yearData === 'object') {
      for (const key of ['total', 'Total', 'population', 'totalt']) {
        if (Number.isFinite(Number(yearData[key]))) return Number(yearData[key]);
      }
      const nums = Object.values(yearData).filter(v => Number.isFinite(Number(v))).map(Number);
      if (nums.length) return nums.reduce((a, b) => a + b, 0);
    }
  }

  const ageGroupMap = getPopulationAgeGroupMap(entry);
  if (ageGroupMap) {
    let grandTotal = 0;
    for (const [, yearBuckets] of Object.entries(ageGroupMap)) {
      const yKeys = Object.keys(yearBuckets).filter(k => /^\d{4}$/.test(k)).sort();
      if (!yKeys.length) continue;
      const latest = yearBuckets[yKeys[yKeys.length - 1]];
      if (latest && typeof latest === 'object') {
        const vals = Object.values(latest).filter(v => Number.isFinite(Number(v))).map(Number);
        grandTotal += vals.reduce((a, b) => a + b, 0);
      } else if (Number.isFinite(Number(latest))) {
        grandTotal += Number(latest);
      }
    }
    if (grandTotal > 0) return grandTotal;
  }

  return NaN;
}

/**
 * Distribute district-level population to buildings proportionally by footprint area.
 * Writes __buildingPopShare, __buildingArea, population_group, resident_group
 * onto each building's properties for use by the gravity model.
 */
async function distributePopulationToBuildings(buildingsFC) {
  if (!buildingsFC?.features?.length) return;

  let popData;
  try {
    popData = await ensureGenderAgePopulationData();
  } catch (err) {
    console.warn('distributePopulationToBuildings: could not load population data', err);
    return;
  }
  if (!popData) return;

  try {
    await ensureDistrictData();
  } catch (err) {
    console.warn('distributePopulationToBuildings: no district data', err);
    return;
  }

  const districts = (districtFC?.features || []).filter(
    f => f?.geometry && (f.geometry.type === 'Polygon' || f.geometry.type === 'MultiPolygon')
  );
  if (!districts.length) return;

  const t0 = performance.now();

  const districtMeta = districts.map(d => {
    const props = d.properties || {};
    const regsoCode = regsoCodeFromProps(props);
    const dName = props.__districtName || districtNameOf(props);
    let bbox = null;
    try { bbox = turf.bbox(d); } catch { /* skip */ }
    return { feature: d, regsoCode, dName, bbox, buildings: [], totalArea: 0 };
  });

  for (const feat of buildingsFC.features) {
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; }
    catch { continue; }

    let area;
    try { area = Math.max(turf.area(feat), 1); }
    catch { area = 1; }

    for (const dm of districtMeta) {
      if (dm.bbox) {
        const [minX, minY, maxX, maxY] = dm.bbox;
        if (centroid[0] < minX || centroid[0] > maxX || centroid[1] < minY || centroid[1] > maxY) continue;
      }
      try {
        if (turf.booleanPointInPolygon(turf.point(centroid), dm.feature)) {
          dm.buildings.push({ feat, area });
          dm.totalArea += area;
          break;
        }
      } catch { /* skip */ }
    }
  }

  let assignedCount = 0;
  let matchedDistricts = 0;
  for (const dm of districtMeta) {
    if (!dm.buildings.length || dm.totalArea <= 0) continue;

    const entry = lookupPopulationEntry(popData, dm.regsoCode);
    const totalPop = extractDistrictTotalPopulation(entry);
    if (!Number.isFinite(totalPop) || totalPop <= 0) {
      console.warn(`distributePopulation: no population for district "${dm.dName}" (regso: ${dm.regsoCode})`);
      continue;
    }
    matchedDistricts++;

    for (const bm of dm.buildings) {
      const share = (bm.area / dm.totalArea) * totalPop;
      const props = bm.feat.properties || (bm.feat.properties = {});
      props.__districtPop = totalPop;
      props.__buildingPopShare = share;
      props.__buildingArea = bm.area;
      if (!props.__districtName) props.__districtName = dm.dName;

      const popDensity = share / Math.max(1, bm.area);
      if (popDensity > 0.05) {
        props.population_group = 'Residential';
        props.resident_group = 'Residential';
      } else if (popDensity > 0.01) {
        props.population_group = 'Commercial';
        props.resident_group = 'Commercial';
      } else {
        props.population_group = 'Other / unknown';
        props.resident_group = 'Other / unknown';
      }
      assignedCount++;
    }
  }

  const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
  console.log(`distributePopulationToBuildings: ${assignedCount}/${buildingsFC.features.length} buildings, ${matchedDistricts}/${districtMeta.length} districts with pop data, in ${elapsed}s`);
}

async function loadCityOSM(city) {
  console.trace('>>> loadCityOSM called for:', city, 'at', Date.now());
  try {
    const _t0 = performance.now();
    const _elapsed = () => ((performance.now() - _t0) / 1000).toFixed(1) + 's';

    applyDistrictDatasetForCity(city);

    // Invalidate population cache so correct city file is fetched
    genderAgePopulation = null;
    genderAgePopulationPromise = null;
    _genderAgePopCityKey = null;

    loadCityBtn && (loadCityBtn.disabled = true);
    jobStatusEl && (jobStatusEl.textContent = 'Starting…');

    console.log(`[loadCityOSM] Starting ${city}...`);

    const years = [2021,2022,2023,2024];
    const { buildingsUrl } = await runCityJob(city, years, step => { if (jobStatusEl) { jobStatusEl.textContent = step; jobStatusEl.title = step; } });
    console.log(`[loadCityOSM] runCityJob done at ${_elapsed()}`);

    const fc = await fetch(buildingsUrl).then(r => r.json());
    console.log(`[loadCityOSM] Buildings fetched: ${fc?.features?.length} at ${_elapsed()}`);

    // Enrich with local building details (byggnad_malmo.geojson etc.)
    const cityKey = districtCityKeyFromInput(city);
    if (cityKey && BUILDING_URL_BY_CITY_KEY[cityKey]) {
      showGlobalSpinner('Loading city…');
      jobStatusEl && (jobStatusEl.textContent = 'Loading building details…');
      try {
        const localFC = await fetch(BUILDING_URL_BY_CITY_KEY[cityKey]).then(r => r.json());
        enrichOSMWithLocalBuildings(fc, localFC);
        console.log(`[loadCityOSM] Building enrichment done at ${_elapsed()}`);
      } catch (localErr) {
        console.warn('Could not load local building details:', localErr);
      }
    }

    await alignBuildingCoverageToDistricts(fc, 'OSM buildings');
    console.log(`[loadCityOSM] District alignment done at ${_elapsed()}`);

    // Distribute population from districts to buildings for gravity model
    baseCityFC = fc;
    districtLandClipSignature = '';
    jobStatusEl && (jobStatusEl.textContent = 'Distributing population…');
    try {
      await distributePopulationToBuildings(fc);
      console.log(`[loadCityOSM] Population distribution done at ${_elapsed()}`);
    } catch (popErr) {
      console.warn('Population distribution failed (non-fatal):', popErr);
    }

    newbuildsFC = fc;
    refreshBuildingTypeDropdown();
    setSelectedBuildingType('', true);
    jobStatusEl && (jobStatusEl.textContent = 'Ready');
    hideGlobalSpinner();
    fitToData(baseCityFC);
    if (districtView) await refreshDistrictScores();
    if (mezoView) await refreshMezoScores();
    updateLayers();
    updateDRAndPCBadges();
    console.log(`[loadCityOSM] Fully done at ${_elapsed()}`);
  } catch (err) {
    hideGlobalSpinner();
    console.error('[loadCityOSM] ERROR:', err);
    jobStatusEl && (jobStatusEl.textContent = 'Failed');
    alert(`Failed to load ${city}: ${err?.message || err}`);
  } finally {
    loadCityBtn && (loadCityBtn.disabled = false);
  }
}

/* ======================= Load city from local GeoJSON (OSM+GeoJSON mode) ======================= */
async function loadCityLocal(cityKey) {
  const url = BUILDING_URL_BY_CITY_KEY[cityKey];
  if (!url) {
    alert(`No local building file for "${cityKey}".`);
    return;
  }

  try {
    const displayName = LOCAL_CITY_NAMES[cityKey] || cityKey;
    lastCityName = displayName;
    applyDistrictDatasetForCity(displayName);
    jobStatusEl && (jobStatusEl.textContent = 'Loading local…');

    const fc = await fetch(url).then(r => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    });

    await alignBuildingCoverageToDistricts(fc, 'local buildings');
    baseCityFC  = fc;
    districtLandClipSignature = '';
    newbuildsFC = fc;
    poiCache = {};   // clear so local POI extraction runs fresh

    refreshBuildingTypeDropdown();
    setSelectedBuildingType('', true);
    jobStatusEl && (jobStatusEl.textContent = `Ready (${fc.features.length} bldgs)`);
    fitToData(baseCityFC);
    if (districtView) await refreshDistrictScores();
    if (mezoView) await refreshMezoScores();
    updateLayers();
    updateDRAndPCBadges();
  } catch (err) {
    console.error('loadCityLocal failed:', err);
    jobStatusEl && (jobStatusEl.textContent = 'Failed');
    alert(`Failed to load local data for ${cityKey}: ${err?.message || err}`);
  }
}

function cityGeoJSONURL(city) {
  const key = normalizeCityKey(city);
  return OSM_GEOJSON_CITY_URLS[key] || null;
}

function normalizeHybridFeatureProps(props = {}) {
  const out = { ...props };
  const name = out.name || out.namn || out.objektnamn || out.byggnadsnamn || out.byggnadsnamn1 || out.byggnadsnamn2 || out.byggnadsnamn3;
  if (name && !out.name) out.name = name;

  const building = out.building || out.objekttyp || out.byggnadstyp || out.typ;
  if (building && !out.building) out.building = building;

  if (!out.category && building) {
    out.category = String(building);
  }

  const purposes = [out.andamal1, out.andamal2, out.andamal3, out.andamal4, out.andamal5]
    .filter((v) => v != null && String(v).trim())
    .join(' ; ');
  if (purposes && !out.__purpose_text) out.__purpose_text = purposes;
  return out;
}

function normalizeHybridGeoJSON(fc) {
  if (!fc || !Array.isArray(fc.features)) return fc;
  return {
    ...fc,
    features: fc.features.map((feature) => ({
      ...feature,
      properties: normalizeHybridFeatureProps(feature?.properties || {})
    }))
  };
}

async function loadCityHybrid(city) {
  try {
    applyDistrictDatasetForCity(city);
    loadCityBtn && (loadCityBtn.disabled = true);
    jobStatusEl && (jobStatusEl.textContent = 'Loading GeoJSON…');

    const cityURL = cityGeoJSONURL(city);
    if (!cityURL) {
      throw new Error(`No GeoJSON dataset configured for "${city}". Add it to OSM_GEOJSON_CITY_URLS in main.js.`);
    }

    const rawFC = await fetch(cityURL).then((r) => {
      if (!r.ok) throw new Error(`GeoJSON fetch failed (${r.status})`);
      return r.json();
    });
    const fc = normalizeHybridGeoJSON(rawFC);
    await alignBuildingCoverageToDistricts(fc, 'OSM+GeoJSON buildings');
    baseCityFC = fc;
    newbuildsFC = fc;
    refreshBuildingTypeDropdown();
    setSelectedBuildingType('', true);
    jobStatusEl && (jobStatusEl.textContent = 'Ready');
    fitToData(baseCityFC);
    if (districtView) await refreshDistrictScores();
    if (mezoView) await refreshMezoScores();
    updateLayers();
    updateDRAndPCBadges();
  } catch (err) {
    console.error(err);
    jobStatusEl && (jobStatusEl.textContent = 'Failed');
    alert(`Failed to load GeoJSON for ${city}: ${err?.message || err}`);
  } finally {
    loadCityBtn && (loadCityBtn.disabled = false);
  }
}

function fitToData(fc) {
  try {
    const [minX, minY, maxX, maxY] = turf.bbox(fc);
    map.fitBounds([[minX, minY], [maxX, maxY]], { padding: 40, duration: 800 });
  } catch {}
}

function setMode(mode) {
  viewMode = mode;
  modeAllBtn?.classList.toggle('active', mode === 'all');
  modeNewBtn?.classList.toggle('active', mode === 'new');
  yearControlsWrap?.classList.toggle('d-none-important', mode !== 'new');
  clearSelections();
  closePopup();
  updateLayers();
}

function clearSelections() {
  firstFeat = null; secondFeat = null; routeGeoJSON = null;
  if (distanceOut) distanceOut.textContent = '—';
}

function resetUIState() {
  if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
  if (giniOut) giniOut.textContent = '—';
  if (overallGiniOut) overallGiniOut.textContent = '—';
  document.querySelectorAll('.poi-check').forEach(el => { el.checked = false; });
  document.querySelectorAll('.poi-weight').forEach(el => {
    const cat = el.getAttribute('data-cat');
    const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
    if (badge) badge.textContent = el.value;
  });

  clearFairness(true);
  closePopup();
  hideSidePanel();
  clearDRProjection(false);
  setSelectedBuildingType('', true);
  setDistrictView(false);
  setMezoView(false);
  clearWhatIfSuggestions();
  refreshWhatIfSuggestCategories([whatIfType]);
}

/* ======================= Year highlight ======================= */
function statsForYear(year) {
  if (!year || !newbuildsFC) return [];
  return newbuildsFC.features.filter(f => {
    const by = getBuiltYear(f.properties || {});
    if (year === 'null') return by == null;
    if (year === '0') return Number.isFinite(by) ? by <= 2020 : false;
    return by === Number(year);
  });
}

/* ======================= Fairness (scoring & Gini) ======================= */
function normalizeFairnessColorScheme(value) {
  const v = String(value || '').toLowerCase();
  return ['green-red', 'viridis', 'cool'].includes(v) ? v : FAIRNESS_COLOR_SCHEME_DEFAULT;
}

function colorFromInterpolator(interpolator, t, fallback=[128, 128, 128]) {
  if (typeof d3 === 'undefined' || typeof interpolator !== 'function') return fallback;
  const c = d3.color(interpolator(Math.max(0, Math.min(1, t))));
  if (!c) return fallback;
  return [Math.round(c.r), Math.round(c.g), Math.round(c.b)];
}

function fairnessLegendGradientCSS() {
  if (fairnessColorScheme === 'green-red') {
    return 'linear-gradient(90deg, #1a9641 0%, #fdae61 50%, #d7191c 100%)';
  }
  const stops = [1, 0.75, 0.5, 0.25, 0].map((score, idx, arr) => {
    const pct = Math.round((idx / (arr.length - 1)) * 100);
    const [r, g, b] = colorFromScore(score);
    return `rgb(${r}, ${g}, ${b}) ${pct}%`;
  });
  return `linear-gradient(90deg, ${stops.join(', ')})`;
}

function changeLegendGradientCSS() {
  const CHANGE_DIVERGING = [
    [178,  24,  43],
    [214,  96,  77],
    [244, 165, 130],
    [253, 219, 199],
    [247, 247, 247],
    [209, 229, 240],
    [146, 197, 222],
    [ 67, 147, 195],
    [ 33, 102, 172],
  ];
  const stops = CHANGE_DIVERGING.map((c, idx, arr) => {
    const pct = Math.round((idx / (arr.length - 1)) * 100);
    return `rgb(${c[0]}, ${c[1]}, ${c[2]}) ${pct}%`;
  });
  return `linear-gradient(90deg, ${stops.join(', ')})`;
}

function updateFairnessLegendUI() {
  const sidePanel = document.getElementById('sidePanel');
  if (sidePanel?.dataset?.legendMode !== 'change') {
    const sideLegend = document.getElementById('fairnessLegendBar');
    if (sideLegend) sideLegend.style.background = fairnessLegendGradientCSS();
  }

  const drLegend = document.getElementById('drLegendGradient');
  if (drLegend) drLegend.style.background = fairnessLegendGradientCSS();
}

function setFairnessColorScheme(value, opts = {}) {
  const { refreshLayers = true } = opts;
  const nextScheme = normalizeFairnessColorScheme(value);
  if (nextScheme === fairnessColorScheme) return;

  fairnessColorScheme = nextScheme;
  fairRecolorTick++;
  if (fairnessColorSchemeSelect) fairnessColorSchemeSelect.value = fairnessColorScheme;
  updateFairnessLegendUI();

  if (drPlot?.points?.length) redrawDR();
  if (refreshLayers) updateLayers();
}

function colorFromScore(score) {
  const s = Math.max(0, Math.min(1, Number(score) || 0));

  if (fairnessColorScheme === 'viridis') {
    return colorFromInterpolator(d3?.interpolateViridis, 1 - s, [68, 1, 84]);
  }
  if (fairnessColorScheme === 'cool') {
    return colorFromInterpolator(d3?.interpolateCool, 1 - s, [110, 64, 170]);
  }

  let r, g;
  if (s <= 0.5) { const t = s/0.5; r = 255; g = Math.round(255*t); }
  else { const t = (s-0.5)/0.5; r = Math.round(255*(1-t)); g = 255; }
  return [r,g,0];
}

/* ---------- Animated transition helpers ---------- */
function lerpColor(c1, c2, t) {
  return [
    Math.round(c1[0] + (c2[0] - c1[0]) * t),
    Math.round(c1[1] + (c2[1] - c1[1]) * t),
    Math.round(c1[2] + (c2[2] - c1[2]) * t)
  ];
}

function blendColor(base, tint, amount) {
  const a = Math.max(0, Math.min(1, Number(amount) || 0));
  if (a <= 0) return [base[0], base[1], base[2]];
  if (a >= 1) return [tint[0], tint[1], tint[2]];
  return lerpColor(base, tint, a);
}

function smoothstep01(x) {
  const t = Math.max(0, Math.min(1, Number(x) || 0));
  return t * t * (3 - 2 * t);
}

function transitionBlendState() {
  const flashRatio = Math.max(0, Math.min(0.85, TRANSITION_CHANGED_FLASH_MS / TRANSITION_DURATION_MS));
  if (flashRatio <= 0) {
    return {
      flashAmount: 0,
      colorT: smoothstep01(transitionAnimT)
    };
  }

  const inFlash = transitionAnimT < flashRatio;
  const flashProgress = inFlash ? transitionAnimT / flashRatio : 1;
  const flashAmount = inFlash
    ? (1 - smoothstep01(flashProgress)) * TRANSITION_CHANGED_FLASH_MAX_INTENSITY
    : 0;

  const postFlashT = inFlash
    ? 0
    : (transitionAnimT - flashRatio) / (1 - flashRatio);

  return {
    flashAmount,
    colorT: smoothstep01(postFlashT)
  };
}

function saveTransitionScores() {
  // Buildings
  if (baseCityFC?.features?.length) {
    for (const f of baseCityFC.features) {
      const p = f?.properties;
      if (!p) continue;
      p.__prevScore = p?.fair?.score ?? p?.fair_overall?.score ?? null;
    }
  }
  // Mezo hexes
  transitionMezoPrevScoreByHex = new Map();
  if (Array.isArray(mezoHexData)) {
    for (const cell of mezoHexData) {
      const prev = cell?.__fairFocused ?? cell?.__score ?? null;
      cell.__prevScore = prev;
      if (cell?.hex && Number.isFinite(prev)) {
        transitionMezoPrevScoreByHex.set(cell.hex, prev);
      }
    }
  }
  // Districts
  if (districtFC?.features?.length) {
    for (const f of districtFC.features) {
      const p = f?.properties;
      if (!p) continue;
      p.__prevScore = p?.__fairFocused ?? p?.__score ?? p?.__fairOverall ?? null;
    }
  }
  transitionHasData = true;
  const btn = document.getElementById('changesReplayBtn');
  if (btn) btn.disabled = false;
}

function startTransitionReplay() {
  if (!transitionHasData || !fairActive) return;
  // Cancel any running animation
  if (transitionAnimRAF) { cancelAnimationFrame(transitionAnimRAF); transitionAnimRAF = null; }

  transitionAnimActive = true;
  transitionAnimT = 0;
  transitionAnimTick++;
  updateLayers();

  const btn = document.getElementById('changesReplayBtn');
  if (btn) { btn.classList.replace('btn-outline-info', 'btn-info'); }

  const startTime = performance.now();
  function tick(now) {
    const elapsed = now - startTime;
    transitionAnimT = Math.min(1, elapsed / TRANSITION_DURATION_MS);
    transitionAnimTick++;
    updateLayers();
    if (transitionAnimT < 1) {
      transitionAnimRAF = requestAnimationFrame(tick);
    } else {
      transitionAnimActive = false;
      transitionAnimRAF = null;
      if (btn) { btn.classList.replace('btn-info', 'btn-outline-info'); }
      updateLayers();
    }
  }
  transitionAnimRAF = requestAnimationFrame(tick);
}

function clearTransitionData() {
  transitionHasData = false;
  transitionAnimActive = false;
  transitionMezoPrevScoreByHex = new Map();
  if (transitionAnimRAF) { cancelAnimationFrame(transitionAnimRAF); transitionAnimRAF = null; }
  const btn = document.getElementById('changesReplayBtn');
  if (btn) { btn.disabled = true; btn.classList.replace('btn-info', 'btn-outline-info'); }
}

function transitionBuildingColor(props) {
  const prev = props?.__prevScore;
  const curr = props?.fair?.score ?? props?.fair_overall?.score ?? null;
  if (!Number.isFinite(prev) || !Number.isFinite(curr)) {
    return Number.isFinite(curr) ? colorFromScore(curr) : [160, 160, 160];
  }
  const { flashAmount, colorT } = transitionBlendState();
  const base = lerpColor(colorFromScore(prev), colorFromScore(curr), colorT);
  const hasChanged = Math.abs(curr - prev) > TRANSITION_CHANGE_EPSILON;
  if (!hasChanged) return base;
  return blendColor(base, TRANSITION_CHANGED_FLASH_COLOR, flashAmount);
  }

function transitionDistrictColor(props) {
  const prev = props?.__prevScore;
  const curr = districtOverlayScore(props);
  if (!Number.isFinite(prev) || !Number.isFinite(curr)) {
    if (Number.isFinite(curr)) { const [r,g,b] = colorFromScore(curr); return [r,g,b,110]; }
    return [80, 80, 80, 25];
  }
  const { flashAmount, colorT } = transitionBlendState();
  const base = lerpColor(colorFromScore(prev), colorFromScore(curr), colorT);
  const hasChanged = Math.abs(curr - prev) > TRANSITION_CHANGE_EPSILON;
  const c = blendColor(base, TRANSITION_CHANGED_FLASH_COLOR, hasChanged ? flashAmount : 0);
  return [c[0], c[1], c[2], 110];
}

function transitionMezoColor(props) {
  const prev = props?.__prevScore;
  const curr = mezoOverlayScore(props);
  if (!Number.isFinite(prev) || !Number.isFinite(curr)) {
    if (Number.isFinite(curr)) { const [r,g,b] = colorFromScore(curr); return [r,g,b,130]; }
    return [80, 80, 80, 25];
  }
  const { flashAmount, colorT } = transitionBlendState();
  const base = lerpColor(colorFromScore(prev), colorFromScore(curr), colorT);
  const hasChanged = Math.abs(curr - prev) > TRANSITION_CHANGE_EPSILON;
  const c = blendColor(base, TRANSITION_CHANGED_FLASH_COLOR, hasChanged ? flashAmount : 0);
  return [c[0], c[1], c[2], 130];
}

function withMezoPrevScore(cell = {}) {
  if (!cell?.hex) return cell;
  if (Number.isFinite(cell.__prevScore)) return cell;
  const prev = transitionMezoPrevScoreByHex.get(cell.hex);
  if (!Number.isFinite(prev)) return cell;
  return { ...cell, __prevScore: prev };
}

function clearDistrictFairnessView() {
  if (districtFC?.features) {
    for (const f of districtFC.features) {
      if (!f.properties) continue;
      delete f.properties.__score;
      delete f.properties.__count;
      delete f.properties.__fairOverall;
      delete f.properties.__fairByCat;
      delete f.properties.__fairFocused;
      delete f.properties.__fairFocusedCat;
    }
  }

  fairActive = false;
  fairCategory = '';
  fairRecolorTick++;
  currentPOIsFC = null;
  selectedPOIMix = [];

  window.activePOICats = new Set();
  selectedPOIId = null;
  selectedPOIFeature = null;

  clearBestWorstHighlights();

  districtScoresSuppressed = true;
  districtScoreTick++;
  updateLayers();
  setParallelCoordsPending(false);
}

function clearFairness(clearOverall = false) {
  if (baseCityFC?.features) {
    for (const f of baseCityFC.features) {
      if (f.properties) { delete f.properties.fair; delete f.properties.fair_multi; if (clearOverall) delete f.properties.fair_overall; }
    }
  }
  fairActive = false;
  fairCategory = '';
  fairRecolorTick++;
  currentPOIsFC = null;
  selectedPOIMix = [];
  poiCache = clearOverall ? {} : poiCache;
  if (clearOverall) overallGini = null;

  window.activePOICats = new Set();
  selectedPOIId = null;
  selectedPOIFeature = null;

  clearBestWorstHighlights();

  refreshDistrictScores();
  refreshMezoScores();
  updateLayers();
  setParallelCoordsPending(false);
}

function bboxForFC(fc, pad = 0.06) {
  if (!fc?.features?.length) throw new Error('Cannot compute bbox: empty feature collection.');
  const [minX, minY, maxX, maxY] = turf.bbox(fc);
  if (![minX, minY, maxX, maxY].every(Number.isFinite)) {
    throw new Error('Cannot compute bbox: invalid feature bounds.');
  }
  const dx = (maxX-minX)*pad, dy=(maxY-minY)*pad;
  return [minY-dy, minX-dx, maxY+dy, maxX+dx]; // south, west, north, east
}

/* ---------- Overpass base selectors ---------- */
const POI_QUERIES = {
  grocery: [
    'nwr["shop"="supermarket"]',
    'nwr["shop"="convenience"]',
    'nwr["shop"="greengrocer"]',
    'nwr["amenity"="marketplace"]'
  ],
  hospital:  ['nwr["amenity"="hospital"]','nwr["healthcare"="hospital"]'],
  pharmacy:  ['nwr["amenity"="pharmacy"]'],
  dentistry: ['nwr["amenity"="dentist"]'],
  healthcare_center: ['nwr["amenity"="clinic"]','nwr["healthcare"~"clinic|centre|doctor",i]'],
  veterinary: ['nwr["amenity"="veterinary"]'],
  university: ['nwr["amenity"="university"]'],
  kindergarten:   ['nwr["amenity"="kindergarten"]','nwr["amenity"="childcare"]'],
  school_primary: ['nwr["amenity"="school"]'],
  school_high:    [
    'nwr["amenity"="school"]',
    'nwr["amenity"="college"]',
    'nwr["school:level"~"upper|secondary|gymnas|high",i]',
    'nwr["education:level"~"upper|secondary|gymnas|high",i]',
    'nwr["isced:level"~"3",i]'
  ]
};

/* ---------- JS-level tag classifiers for Swedish schools ---------- */
function tagsText(...vals){ return vals.filter(Boolean).map(v=>String(v).toLowerCase()).join(' '); }
function parseIscedDigits(str){ const m = String(str||'').match(/[0-9]/g); return m ? m.map(d=>parseInt(d,10)) : []; }
function isPrimarySchool(tags){
  const t = tagsText(tags['school:level'], tags['education:level'], tags['level']);
  if (/\bgrund/.test(t) || /\bprimary\b/.test(t) || /\blower\b/.test(t)) return true;
  const nums = parseIscedDigits(tags['isced:level']); return nums.includes(1) || nums.includes(2);
}
function isHighSchool(tags){
  const t = tagsText(tags['school:level'], tags['education:level'], tags['level']);
  if (/\bgymnas/.test(t) || /\bhigh\b/.test(t) || /\bupper\b/.test(t) || /\bsecondary\b/.test(t)) return true;
  const nums = parseIscedDigits(tags['isced:level']); return nums.includes(3);
}

function propsText(props = {}) {
  // Local GeoJSON schemas vary a lot. In OSM+GeoJSON mode we inspect both
  // common semantic keys and *all* primitive values as a robust fallback.
  const keyHints = [
    'name','namn','objektnamn','byggnadsnamn','building','objekttyp','category','category_label',
    'amenity','shop','healthcare','school:level','education:level','level','isced:level',
    'verksamhet','verksamhetstyp','verksamhet_typ','anvandning','anvandningstyp','beskrivning',
    'byggnadsnamn1','byggnadsnamn2','byggnadsnamn3','andamal1','andamal2','andamal3','andamal4','andamal5','__purpose_text'
  ];
  const hinted = keyHints.map((k) => props?.[k]);
  const primitiveValues = Object.values(props || {}).filter((v) =>
    ['string', 'number', 'boolean'].includes(typeof v)
  );
  return [...hinted, ...primitiveValues]
    .filter((v) => v != null && String(v).trim())
    .map((v) => String(v).toLowerCase())
    .join(' ');
}

function normalizeForMatch(txt = '') {
  return String(txt)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

function textHasAny(text, needles = []) {
  const t = normalizeForMatch(text);
  return needles.some((n) => t.includes(normalizeForMatch(n)));
}

function isPrimarySchoolLike(tags = {}) {
  const t = `${tagsText(tags['school:level'], tags['education:level'], tags['level'])} ${propsText(tags)}`;
  if (textHasAny(t, ['grundskol', 'primary', 'lower school', 'elementary', 'lågstad', 'mellanstad'])) return true;
  const nums = parseIscedDigits(tags['isced:level']);
  return nums.includes(1) || nums.includes(2);
}

function isHighSchoolLike(tags = {}) {
  const t = `${tagsText(tags['school:level'], tags['education:level'], tags['level'])} ${propsText(tags)}`;
  if (textHasAny(t, ['gymnas', 'high school', 'upper secondary', 'secondary school', 'isced 3'])) return true;
  const nums = parseIscedDigits(tags['isced:level']);
  return nums.includes(3);
}

function buildPOIsFromBuildings(cat, fc) {
  const feats = [];
  for (const feat of (fc?.features || [])) {
    const props = feat?.properties || {};
    if (!buildingMatchesPOI(props, cat)) continue;
    let c = null;
    try { c = turf.centroid(feat).geometry.coordinates; } catch { c = null; }
    if (!c || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    feats.push({
      type: 'Feature',
      properties: {
        id: props.id || props['@id'] || null,
        osm_type: 'local_building',
        name: props.name || props.namn || '',
        category: cat,
        tags: props,
        __from_building_geojson: true
      },
      geometry: { type: 'Point', coordinates: c }
    });
  }
  return { type: 'FeatureCollection', features: dedupeLocalBuildingPOIs(cat, feats) };
}

function dedupeLocalBuildingPOIs(cat, features = []) {
  // Local building datasets can contain several building polygons for the same
  // campus/facility name (e.g. many buildings all named "Heleneholms gymnasium").
  // Collapse those into one POI per name within a small radius.
  const byName = new Map();
  for (const feat of features) {
    const c = feat?.geometry?.coordinates;
    if (!Array.isArray(c) || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    const rawName = String(feat?.properties?.name || '').trim();
    const nameKey = normalizeForMatch(rawName || `__unnamed__${cat}`);
    if (!byName.has(nameKey)) byName.set(nameKey, []);
    byName.get(nameKey).push(feat);
  }

  const merged = [];
  const MERGE_RADIUS_M = 180;

  for (const arr of byName.values()) {
    const buckets = [];
    for (const feat of arr) {
      const c = feat.geometry.coordinates;
      let bucket = null;
      for (const b of buckets) {
        if (haversineMeters(c, b.center) <= MERGE_RADIUS_M) {
          bucket = b;
          break;
        }
      }
      if (!bucket) {
        buckets.push({ center: c, items: [feat] });
      } else {
        bucket.items.push(feat);
      }
    }

    for (const b of buckets) {
      // Prefer item that already has a non-empty name; otherwise first item.
      const named = b.items.find((f) => String(f?.properties?.name || '').trim());
      merged.push(named || b.items[0]);
    }
  }

  return merged;
}

function shouldFallbackToOverpass(cat, localFC) {
  // Keep the requested OSM+GeoJSON behavior (prefer local POIs), but avoid
  // a fully empty/green fairness state when local schema lacks explicit POI hints.
  if (sourceMode !== 'osm_s1') return false;
  const n = localFC?.features?.length || 0;
  // For schools the file often has many records; if we detected nothing, fallback.
  if (cat === 'school_primary' || cat === 'school_high') return n === 0;
  // For other categories, also fallback on empty local match set.
  return n === 0;
}

function propsText(props = {}) {
  const keys = [
    'name','namn','objektnamn','byggnadsnamn','building','objekttyp','category','category_label',
    'amenity','shop','healthcare','school:level','education:level','level','isced:level',
    'verksamhet','verksamhetstyp','verksamhet_typ','anvandning','anvandningstyp','beskrivning'
  ];
  return keys
    .map((k) => props?.[k])
    .filter((v) => v != null && String(v).trim())
    .map((v) => String(v).toLowerCase())
    .join(' ');
}

function isPrimarySchoolLike(tags = {}) {
  const t = `${tagsText(tags['school:level'], tags['education:level'], tags['level'])} ${propsText(tags)}`;
  if (/\bgrundskol/.test(t) || /\bprimary\b/.test(t) || /\blower\b/.test(t) || /\belementary\b/.test(t)) return true;
  const nums = parseIscedDigits(tags['isced:level']);
  return nums.includes(1) || nums.includes(2);
}

function isHighSchoolLike(tags = {}) {
  const t = `${tagsText(tags['school:level'], tags['education:level'], tags['level'])} ${propsText(tags)}`;
  if (/\bgymnas/.test(t) || /\bhigh\s*school\b/.test(t) || /\bupper\s*secondary\b/.test(t) || /\bsecondary\b/.test(t)) return true;
  const nums = parseIscedDigits(tags['isced:level']);
  return nums.includes(3);
}

function buildPOIsFromBuildings(cat, fc) {
  const feats = [];
  for (const feat of (fc?.features || [])) {
    const props = feat?.properties || {};
    if (!buildingMatchesPOI(props, cat)) continue;
    let c = null;
    try { c = turf.centroid(feat).geometry.coordinates; } catch { c = null; }
    if (!c || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    feats.push({
      type: 'Feature',
      properties: {
        id: props.id || props['@id'] || null,
        osm_type: 'local_building',
        name: props.name || props.namn || '',
        category: cat,
        tags: props,
        __from_building_geojson: true
      },
      geometry: { type: 'Point', coordinates: c }
    });
  }
  // In OSM+GeoJSON mode, one facility can span many polygons with the same
  // name (e.g. multiple school buildings for one campus). Collapse those so a
  // real-world POI is counted once in fairness + shown once on the map.
  return { type: 'FeatureCollection', features: dedupeLocalBuildingPOIs(cat, feats) };
}

/* ---------- Fetch POIs with optional post-filter ---------- */
async function fetchPOIs(cat, fc) {
  /* ---- OSM+GeoJSON mode: extract POIs locally, fall back to Overpass if empty ---- */
  if (sourceMode === 'osm_s1') {
    const localKey = `local:${cat}:${normalizeCityKey(lastCityName)}`;
    if (poiCache[localKey]) return poiCache[localKey];
    const localResult = extractLocalPOIs(cat, fc);
    console.log(`[OSM+GeoJSON] Extracted ${localResult.features.length} local POIs for "${cat}"`);
    if (localResult.features.length > 0) {
      poiCache[localKey] = localResult;
      return localResult;
    }
    console.warn(`[OSM+GeoJSON] No local POIs for "${cat}", falling back to Overpass…`);
    // fall through to Overpass fetch below
  }

  const [s,w,n,e] = bboxForFC(fc, 0.06);
  const key = `${sourceMode}:${cat}:${s.toFixed(3)},${w.toFixed(3)},${n.toFixed(3)},${e.toFixed(3)}`;
  if (poiCache[key]) return poiCache[key];

  if (sourceMode === 'osm_s1') {
    const localFC = buildPOIsFromBuildings(cat, fc);
    if (!shouldFallbackToOverpass(cat, localFC)) {
      poiCache[key] = localFC;
      return localFC;
    }
  }

  const selectors = POI_QUERIES[cat] || [];
  if (!selectors.length) return turf.featureCollection([]);

  const body = `
    [out:json][timeout:50];
    (
      ${selectors.map(q => `${q}(${s},${w},${n},${e});`).join('\n      ')}
    );
    out center tags;`;

  const json = await fetchOverpassJSON(body);
  if (!json) throw new Error('Overpass failed: no response');

  let feats = [];
  for (const el of (json.elements || [])) {
    let lon, lat;
    if (el.type === 'node') { lon = el.lon; lat = el.lat; }
    else if (el.center)     { lon = el.center.lon; lat = el.center.lat; }
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    const name = (el.tags && (el.tags.name || el.tags['name:sv'] || el.tags['name:en'])) ||
                 (el.tags && (el.tags.brand || el.tags['brand:sv'] || el.tags['brand:en'])) || '';
    feats.push({
      type:'Feature',
      properties: {
        id: el.id,
        osm_type: el.type,
        name,
        category: cat,
        tags: el.tags || {}
      },
      geometry:{ type:'Point', coordinates:[lon, lat] }
    });
  }

  // The same real-world POI can appear multiple times in Overpass output
  // (e.g. node + way + relation for one hospital, or overlapping selectors).
  // IF-City sums all POI contributions, so we must collapse these duplicates
  // to preserve paper logic (one physical facility should count once).
  const uniqueByObject = new Set();
  const uniqueByPlace = new Map();
  for (const feat of feats) {
    const [lon, lat] = feat?.geometry?.coordinates || [];
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    const props = feat.properties || {};
    const tags = props.tags || {};
    const osmType = String(props.osm_type || '').trim().toLowerCase();
    const osmId = props.id;

    // 1) exact duplicate object (same OSM type+id, repeated by overlapping selectors)
    if (osmType && Number.isFinite(osmId)) {
      const objectKey = `${osmType}:${osmId}`;
      if (uniqueByObject.has(objectKey)) continue;
      uniqueByObject.add(objectKey);
    }

    // 2) cross-object duplicate for one real facility (node + way + relation)
    const nameKey = String(props.name || '').trim().toLowerCase();
    const kindKey = String(tags.amenity || tags.healthcare || cat || '').trim().toLowerCase();
    const coordKey = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    const placeKey = `${coordKey}:${nameKey || kindKey}`;

    if (!uniqueByPlace.has(placeKey)) {
      uniqueByPlace.set(placeKey, feat);
    }
  }
  feats = Array.from(uniqueByPlace.values());

  if (cat === 'school_primary') {
    const filtered = feats.filter(f => isPrimarySchoolLike(f.properties.tags));
    feats = filtered.length ? filtered : feats;
  }
  if (cat === 'school_high') {
    const filtered = feats.filter(f => isHighSchoolLike(f.properties.tags));
    feats = filtered.length ? filtered : feats;
  }

  const fcOut = { type:'FeatureCollection', features: feats };
  poiCache[key] = fcOut;
  return fcOut;
}

/* ---------- Extract POIs from local Lantmäteriet building data ---------- */
/* --- Name-based patterns for byggnadsnamn fields (brands, common names) --- */
const NAME_POI_PATTERNS = {
  grocery:           /\b(ica|coop|willys|hemköp|lidl|netto|city\s*gross|matöppet|tempo|handlar|livs|dagligvaru|livsmedel|matbutik|supermarket)\b/i,
  pharmacy:          /\b(apotek|apotea|kronans|hjärtat)\b/i,
  dentistry:         /\b(tandläkar|folktandvård|tandklinik|tandvård|tand\s*(?:läkar|klinik|vård))\b/i,
  healthcare_center: /\b(vårdcentral|hälsocentral|husläkar|hälsovård)\b/i,
  veterinary:        /\b(veterinär|djurklinik|djursjukhus|evidensia|anicura)\b/i,
  kindergarten:      /\b(förskol|barnomsorg|daghem|dagis)\b/i,
  school_high:       /(gymnasium|gymnasie|gymnasi)/i,
  hospital:          /\b(sjukhus|lasarett)\b/i,
  university:        /\b(universitet|högskol)\b/i,
  school_primary:    /\b(grundskol|skola|skolan)\b/i,
};

function extractLocalPOIs(category, fc) {
  if (!fc?.features?.length) return turf.featureCollection([]);

  const feats = [];
  const seen = new Set();
  const namePattern = NAME_POI_PATTERNS[category] || null;

  for (const f of fc.features) {
    const props = f?.properties || {};

    // Collect all andamal + objekttyp fields into one lowercase string
    const searchText = [
      props.andamal1, props.andamal2, props.andamal3,
      props.andamal4, props.andamal5, props.objekttyp
    ].filter(Boolean).map(s => String(s).toLowerCase()).join(' ');

    // Collect byggnadsnamn fields
    const nameText = [
      props.byggnadsnamn1, props.byggnadsnamn2, props.byggnadsnamn3
    ].filter(Boolean).map(s => String(s)).join(' ');

    // 1) Check andamal keywords
    let matched = false;
    for (const [keyword, cat] of Object.entries(ANDAMAL_TO_POI_CATEGORY)) {
      if (cat !== category) continue;
      if (searchText.includes(keyword)) { matched = true; break; }
    }

    // 2) Check byggnadsnamn via regex patterns
    if (!matched && namePattern && nameText.trim()) {
      if (namePattern.test(nameText)) matched = true;
    }

    // 3) Also check byggnadsnamn against andamal keywords (e.g. name="Vårdcentral Oxhagen")
    if (!matched && nameText.trim()) {
      const nameLower = nameText.toLowerCase();
      for (const [keyword, cat] of Object.entries(ANDAMAL_TO_POI_CATEGORY)) {
        if (cat !== category) continue;
        if (nameLower.includes(keyword)) { matched = true; break; }
      }
    }

    if (!matched) continue;

    // Get centroid as POI point
    let lon, lat;
    try {
      const centroid = turf.centroid(f);
      [lon, lat] = centroid.geometry.coordinates;
    } catch { continue; }
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    // Dedupe by ~1m precision
    const key = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const name = props.byggnadsnamn1 || props.byggnadsnamn2 || props.objekttyp || '';
    feats.push({
      type: 'Feature',
      properties: {
        id: props.objektidentitet || props.fid || feats.length,
        name,
        category,
        tags: {},
        __localSource: true
      },
      geometry: { type: 'Point', coordinates: [lon, lat] }
    });
  }

  return turf.featureCollection(dedupeLocalBuildingPOIs(category, feats));
}

async function fetchPOIsWithRetry(cat, fc, retries = FAIRNESS_POI_FETCH_RETRIES) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fetchPOIs(cat, fc);
    } catch (err) {
      lastError = err;
      if (attempt < retries) await sleep(OVERPASS_RETRY_DELAY_MS);
    }
  }
  throw lastError || new Error(`Unable to fetch POIs for ${cat}`);
}

function haversineMeters(a, b) {
  const R = 6371000; const toRad = d => d*Math.PI/180;
  const dLat = toRad(b[1]-a[1]); const dLon = toRad(b[0]-a[0]);
  const s = Math.sin(dLat/2)**2 + Math.cos(toRad(a[1]))*Math.cos(toRad(b[1]))*Math.sin(dLon/2)**2;
  return 2*R*Math.asin(Math.sqrt(s));
}

function normalizeTravelMode(mode) {
  const normalized = String(mode || '').toLowerCase();
  if (normalized === 'car') return 'driving';
  if (['walking', 'cycling', 'driving'].includes(normalized)) return normalized;
  return FAIRNESS_TRAVEL_MODE_DEFAULT;
}

function normalizeFairnessModel(mode) {
  const normalized = String(mode || '').toLowerCase();
  if (normalized === 'if-city') return 'ifcity';
  if (['default', 'ifcity'].includes(normalized)) return normalized;
  return FAIRNESS_MODEL_DEFAULT;
}

function setFairnessTravelMode(mode, { recompute = true } = {}) {
  const normalized = normalizeTravelMode(mode);
  fairnessTravelMode = normalized;
  if (fairnessTravelModeSelect) fairnessTravelModeSelect.value = normalized;
  if (recompute) recomputeFairnessAfterWhatIf();
}

function setFairnessModel(mode, { recompute = true } = {}) {
  const normalized = normalizeFairnessModel(mode);
  fairnessModel = normalized;
  if (fairnessModelSelect) fairnessModelSelect.value = normalized;
  if (recompute) recomputeFairnessAfterWhatIf();
}

function profileForMode(mode) {
  const normalized = normalizeTravelMode(mode);
  return normalized === 'driving' ? 'driving' : normalized;
}

function speedKmhForMode(mode) {
  const normalized = normalizeTravelMode(mode);
  return TRAVEL_SPEED_KMH[normalized] || TRAVEL_SPEED_KMH[FAIRNESS_TRAVEL_MODE_DEFAULT];
}

const routeMetricsCache = new Map();

function routeCacheKey(a, b, profile) {
  const fmt = (coord) => `${Number(coord[0]).toFixed(5)},${Number(coord[1]).toFixed(5)}`;
  return `${profile}:${fmt(a)}|${fmt(b)}`;
}

function pruneRouteCache() {
  if (routeMetricsCache.size <= ROUTING_CACHE_LIMIT) return;
  let excess = routeMetricsCache.size - ROUTING_CACHE_LIMIT;
  for (const key of routeMetricsCache.keys()) {
    routeMetricsCache.delete(key);
    excess -= 1;
    if (excess <= 0) break;
  }
}

function estimateTravelTimeSecondsFromMeters(meters, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  if (!Number.isFinite(meters)) return Infinity;
  const speedKmh = speedKmhForMode(mode);
  const hours = (meters / 1000) / Math.max(0.1, speedKmh);
  return hours * 3600;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchOverpassJSON(body) {
  let lastError = null;
  const apiBase = (typeof API_BASE !== "undefined" && API_BASE) ? API_BASE : null;
  if (apiBase) {
    try {
      const res = await fetchWithTimeout(
        `${apiBase}/overpass/query`,
        { method: 'POST', body, headers: { 'Content-Type': 'text/plain' } },
        OVERPASS_TIMEOUT_MS
      );
      if (!res.ok) {
        const detail = await res.text();
        throw new Error(`Overpass proxy failed: ${res.status} ${detail}`.trim());
      }
      return await res.json();
    } catch (err) {
      lastError = err;
    }
  }

  for (let i = 0; i < OVERPASS_ENDPOINTS.length; i++) {
    const url = OVERPASS_ENDPOINTS[i];
    try {
      const res = await fetchWithTimeout(
        url,
        { method: 'POST', body, headers: { 'Content-Type': 'text/plain' } },
        OVERPASS_TIMEOUT_MS
      );
      if (!res.ok) {
        lastError = new Error(`Overpass failed: ${res.status}`);
        if (res.status === 429 || res.status === 504) {
          await sleep(OVERPASS_RETRY_DELAY_MS);
          continue;
        }
        continue;
      }
      return await res.json();
    } catch (err) {
      lastError = err;
      await sleep(OVERPASS_RETRY_DELAY_MS);
    }
  }
  if (lastError) throw lastError;
  return null;
}

function allowRoutingForFairness(poiCount) {
  if (!USE_TRAVEL_TIME) return false;
  const buildingCount = baseCityFC?.features?.length ?? 0;
  if (buildingCount > ROUTING_MAX_BUILDINGS) return false;
  if (Number.isFinite(poiCount) && poiCount > ROUTING_MAX_POIS) return false;
  return true;
}

async function getTravelMetricsForPair(
  from,
  to,
  fallbackMeters,
  allowRouting = true,
  mode = FAIRNESS_TRAVEL_MODE_DEFAULT
) {
  if (!allowRouting) {
    return {
      meters: fallbackMeters,
      seconds: estimateTravelTimeSecondsFromMeters(fallbackMeters, mode)
    };
  }
  const routeMetrics = await fetchRouteMetrics(from, to, { profile: profileForMode(mode) });
  const routeMeters = Number.isFinite(routeMetrics?.meters) ? routeMetrics.meters : fallbackMeters;
  const routeSeconds = Number.isFinite(routeMetrics?.seconds)
    ? routeMetrics.seconds
    : estimateTravelTimeSecondsFromMeters(routeMeters, mode);
  return { meters: routeMeters, seconds: routeSeconds };
}

async function fetchRouteMetrics(from, to, { profile = ROUTING_PROFILE } = {}) {
  if (!USE_TRAVEL_TIME) return null;
  if (!ROUTING_BASE_URL) return null;
  if (!Array.isArray(from) || !Array.isArray(to)) return null;
  if (![from[0], from[1], to[0], to[1]].every(Number.isFinite)) return null;

  const key = routeCacheKey(from, to, profile);
  if (routeMetricsCache.has(key)) return routeMetricsCache.get(key);

  const url = `${ROUTING_BASE_URL}/route/v1/${profile}/${from[0]},${from[1]};${to[0]},${to[1]}?overview=false&alternatives=false`;
  try {
    const res = await fetchWithTimeout(url, {}, ROUTING_TIMEOUT_MS);
    if (!res.ok) return null;
    const json = await res.json();
    if (json.code === 'Ok' && json.routes && json.routes[0]) {
      const route = json.routes[0];
      const metrics = {
        meters: Number(route.distance),
        seconds: Number(route.duration)
      };
      routeMetricsCache.set(key, metrics);
      pruneRouteCache();
      return metrics;
    }
  } catch (err) {
    return null;
  }
  return null;
}

function scoreFromDistanceMeters(cat, meters) {
  const km = meters/1000.0;
  const T = {
    grocery:[0.5,1.5], hospital:[1.2,4.0], pharmacy:[0.7,2.0], dentistry:[0.7,2.0],
    healthcare_center:[1.0,3.0], veterinary:[1.0,3.0], university:[1.2,4.0],
    kindergarten:[0.5,1.5], school_primary:[0.6,1.8], school_high:[1.0,2.5]
  };
  const [ideal,max] = T[cat] || [0.8,2.0];
  const t = (km - ideal) / Math.max(1e-6, (max - ideal));
  return 1 - Math.max(0, Math.min(1, t));
}


//Table for distances
function scoreFromTimeSeconds(cat, seconds, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  const effort = FAIRNESS_EFFORT_MULTIPLIER[normalizeTravelMode(mode)] ?? 1;
  const minutes = (seconds / 60) * effort;
  const T = {
    grocery:[6,18], hospital:[14,48], pharmacy:[8,24], dentistry:[8,24],
    healthcare_center:[12,36], veterinary:[12,36], university:[14,48],
    kindergarten:[6,18], school_primary:[7,22], school_high:[12,30]
  };
  const [ideal,max] = T[cat] || [10,24];
  const t = (minutes - ideal) / Math.max(1e-6, (max - ideal));
  return 1 - Math.max(0, Math.min(1, t));
}
function gini(values) {
  const x = values.filter(v => Number.isFinite(v)).slice().sort((a,b)=>a-b);
  const n = x.length; if (!n) return 0;
  const mean = x.reduce((s,v)=>s+v,0)/n; if (mean === 0) return 0;
  let cum = 0; for (let i=0;i<n;i++) cum += (2*(i+1)-n-1)*x[i];
  return Math.abs(cum)/(n*n*mean);
}

function generalizedEntropy(values, alpha = IF_CITY_ALPHA) {
  const x = values.filter(v => Number.isFinite(v));
  const n = x.length;
  if (!n) return 0;
  const mean = x.reduce((s, v) => s + v, 0) / n;
  if (mean === 0) return 0;
  if (alpha === 0) {
    const term = x.reduce((s, v) => s + Math.log(mean / Math.max(v, 1e-9)), 0);
    return term / n;
  }
  if (alpha === 1) {
    const term = x.reduce((s, v) => s + (v / mean) * Math.log(Math.max(v, 1e-9) / mean), 0);
    return term / n;
  }
  const term = x.reduce((s, v) => s + Math.pow(v / mean, alpha) - 1, 0);
  return term / (n * alpha * (alpha - 1));
}

function ifCityKappa(cat) {
  return IF_CITY_KAPPA_BY_CAT[cat] ?? IF_CITY_KAPPA_DEFAULT;
}

function ifCityPriorityWeight(cat) {
  return IF_CITY_PRIORITY_WEIGHTS[cat] ?? 1;
}

function ifCityDistanceForMode(km, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  const normalized = normalizeTravelMode(mode);
  const behaviorFactor = IF_CITY_MODE_DISTANCE_FACTOR[normalized] ?? 1;
  const detourFactor = IF_CITY_MODE_DETOUR_FACTOR[normalized] ?? 1;
  const speed = IF_CITY_MODE_SPEED_KMH[normalized] ?? IF_CITY_REFERENCE_SPEED_KMH;
  const networkKm = km * detourFactor;
  const travelHours = networkKm / Math.max(1e-6, speed);
  return travelHours * IF_CITY_REFERENCE_SPEED_KMH * behaviorFactor;
}

function ifCityEquityWeightForFeature(feature) {
  const props = feature?.properties || {};
  // IF-City equity weighting should come from population/social group signals.
  // If those are missing, do NOT fall back to building land-use/category, because
  // that can make neighboring buildings around the same POI appear unfairly
  // different (e.g. commercial vs residential parcel effects).
  // Keep neutral weight in that case.
  const group = props.resident_group || props.population_group || 'Other / unknown';
  let weight = IF_CITY_EQUITY_WEIGHTS[group] ?? 1;

  // For mock buildings, scale equity weight by estimated capacity (floors × area proxy).
  // More residents → more demand → gravity model treats this building as more important.
  if (props.__whatIfMock && Number.isFinite(props.__mockCapacity) && props.__mockCapacity > 0) {
    // Logarithmic scaling so very tall buildings don't dominate completely
    weight *= Math.max(1, Math.log2(props.__mockCapacity));
  }

  return weight;
}

function ifCityAccessibilityForBuilding(cB, poiArr, cat, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  const kappa = ifCityKappa(cat);
  const rho = ifCityPriorityWeight(cat);
  // Only consider the nearest 5 POIs to prevent city-centre accumulation effect
  // where buildings near many POIs unfairly dominate over suburbs with one nearby POI
  const IF_CITY_MAX_POIS = 3;
  const sorted = poiArr
    .map(poi => ({ poi, dist: haversineMeters(cB, poi.c) }))
    .sort((a, b) => a.dist - b.dist)
    .slice(0, IF_CITY_MAX_POIS);
  let sum = 0;
  for (const { poi, dist } of sorted) {
    const v = Number.isFinite(poi.v) ? poi.v : 1;
    const linearKm = dist / 1000;
    const dKm = ifCityDistanceForMode(linearKm, mode);
    sum += rho * v * Math.exp(-kappa * dKm);
  }
  return sum;
}

function normalizeBenefitsToScores(benefits, mode) {
  // Robust normalization for map coloring:
  // - avoids a single global outlier dominating (old min-max issue),
  // - avoids saturating almost everything to high scores (absolute exp issue).
  // We log-compress and then scale using middle percentiles.
  //
  // Mode-aware floor: walking (baseline) uses p10 as floor for maximum
  // within-mode contrast.  Faster modes (cycling, driving) anchor the
  // floor at 0 so their higher absolute accessibility translates into
  // visibly greener map colors, making cross-mode differences obvious.
  const vals = benefits.filter(v => Number.isFinite(v) && v > 0).map(v => Math.log1p(v));
  if (!vals.length) return benefits.map(() => 0);

  const sorted = vals.slice().sort((a, b) => a - b);
  const q = (p) => {
    if (sorted.length === 1) return sorted[0];
    const i = (sorted.length - 1) * p;
    const lo = Math.floor(i);
    const hi = Math.ceil(i);
    if (lo === hi) return sorted[lo];
    const t = i - lo;
    return sorted[lo] * (1 - t) + sorted[hi] * t;
  };

  const p10 = q(0.10);
  const p95 = q(0.95);

  const isBaseline = !mode || normalizeTravelMode(mode) === 'walking';
  const floor = isBaseline ? p10 : 0;
  const span = Math.max(1e-9, p95 - floor);

  return benefits.map((v) => {
    if (!Number.isFinite(v) || v <= 0) return 0;
    const x = Math.log1p(v);
    const score = (x - floor) / span;
    return Math.max(0, Math.min(1, score));
  });
}

function ifCityOpportunityWeightFromPOI(cat, feature) {
  const tags = feature?.properties?.tags || {};
  // Keep IF-City paper logic (opportunity mass v_j), but avoid using OSM
  // geometry area as a proxy because it can severely over-weight one POI.
  // For open OSM data, capacity tags are sparse/heterogeneous, so default
  // to equal opportunities unless we have an explicit numeric capacity signal.
  const beds = Number(tags.beds);
  const capacity = Number(tags.capacity);
  if (cat === 'hospital' && Number.isFinite(beds) && beds > 0) {
    return Math.max(1, Math.min(10, beds / 100));
  }
  if (Number.isFinite(capacity) && capacity > 0) {
    return Math.max(1, Math.min(10, capacity / 100));
  }
  return 1;
}

function dedupeIFCityPOIs(items) {
  const unique = new Map();
  for (const item of (items || [])) {
    const c = item?.c || [];
    const lon = Number(c[0]);
    const lat = Number(c[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    // In OSM, one physical facility can still appear as multiple entries with
    // different names/tags but identical coordinates (e.g. hospital wing names).
    // IF-City sums all opportunities, so keeping those duplicates can over-boost
    // one location and make nearby buildings look unfairly better.
    // Use coordinate-only dedupe at ~1 m precision to keep one opportunity per
    // colocated facility.
    const key = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    if (!unique.has(key)) unique.set(key, item);
  }
  return Array.from(unique.values());
}

function poiFeatureCoord(feature) {
  const geom = feature?.geometry;
  if (!geom) return null;
  if (geom.type === 'Point' && Array.isArray(geom.coordinates)) return geom.coordinates;
  try { return turf.centroid(feature).geometry.coordinates; } catch { return null; }
}

function dedupePOIFeaturesForDisplay(features) {
  const unique = new Map();
  for (const feat of (features || [])) {
    const coord = poiFeatureCoord(feat);
    if (!Array.isArray(coord)) continue;
    const lon = Number(coord[0]);
    const lat = Number(coord[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    const cat = String(poiCategoryOf(feat) || feat?.properties?.__cat || '').toLowerCase();
    const key = `${cat}|${lon.toFixed(5)},${lat.toFixed(5)}`;
    if (!unique.has(key)) unique.set(key, feat);
  }
  return Array.from(unique.values());
}

/* ---------- Read POI mix from UI ---------- */
function readPOIMixFromUI() {
  const entries = [];
  document.querySelectorAll('.poi-check').forEach(chk => {
    const cat = chk.getAttribute('data-cat');
    const wEl = document.querySelector(`.poi-weight[data-cat="${cat}"]`);
    const w = parseFloat(wEl?.value || '0');
    if (chk.checked && w > 0) entries.push({ cat, weight: w });
  });
  return entries;
}

/* ======================= Växjö population-weighted gravity helpers ======================= */
/**
 * Load the Lantmäteriet buildings file and build a centroid index.
 * Used to enrich OSM buildings (which have category='unknown') with
 * proper objekttyp values (e.g. 'Bostad' for residential).
 * Only loaded once and cached in lantmaterietIndex.
 */
async function ensureLantmaterietIndex() {
  if (lantmaterietIndex) return lantmaterietIndex;
  try {
    const fc = await fetch(CITY_URL).then(r => r.json());
    lantmaterietIndex = (fc.features || [])
      .filter(f => f?.geometry && f?.properties?.objekttyp)
      .map(f => {
        let centroid;
        try { centroid = turf.centroid(f).geometry.coordinates; } catch { return null; }
        return { centroid, objekttyp: f.properties.objekttyp };
      })
      .filter(Boolean);
    console.log('[Lantmäteriet] Index built:', lantmaterietIndex.length, 'buildings');
  } catch (err) {
    console.warn('[Lantmäteriet] Failed to load index:', err);
    lantmaterietIndex = [];
  }
  return lantmaterietIndex;
}

/**
 * Find the objekttyp from the Lantmäteriet index for a given [lng, lat] centroid.
 * Matches the nearest Lantmäteriet building within 40 metres.
 */
function lookupObjekttyp(coord, index) {
  if (!index?.length) return null;
  let bestDist = 120; // increased to 120 metres for large buildings
  let bestObjekttyp = null;
  for (const item of index) {
    const d = haversineMeters(coord, item.centroid);
    if (d < bestDist) {
      bestDist = d;
      bestObjekttyp = item.objekttyp;
    }
  }
  return bestObjekttyp;
}

/** Build a flat spatial index from districtFC for point-in-polygon lookups */
function buildVaxjoDistrictSpatialIndex() {
  if (!districtFC?.features) return [];
  return districtFC.features.map(feat => ({
    feat,
    code: regsoCodeFromProps(feat.properties || {})
  }));
}

/** Find the regso district code for a [lng, lat] coordinate */
function findDistrictCodeForPoint(coord, index) {
  const pt = turf.point(coord);
  for (const { feat, code } of index) {
    try { if (turf.booleanPointInPolygon(pt, feat)) return code; } catch {}
  }
  return null;
}

/**
 * Estimate building floor area: footprint_m2 × number_of_floors
 * Floors derived from height (÷3) or building:levels property.
 */
function buildingFloorAreaM2(feature) {
  let footprintM2 = 80;
  try {
    const area = turf.area(feature);
    if (area > 0) footprintM2 = area;
  } catch {}
  const props = feature?.properties || {};
  const height = Number(props.height || props.Hojd || props['building:height'] || 0);
  const levels = Number(props['building:levels'] || props.floors || 0);
  const floors = levels > 0 ? levels : (height > 0 ? Math.max(1, Math.round(height / 3)) : 1);
  return footprintM2 * Math.min(floors, 40);
}

/**
 * Distribute district population across residential buildings proportionally by floor area.
 *
 * For each district:
 *   totalFloorArea = sum of floorAreaM2 of all residential buildings inside it
 *   buildingPop[i] = (buildingFloorArea[i] / totalFloorArea) × districtTotalPopulation
 *
 * Returns Map<featureIndex, estimatedResidents>
 */
async function buildVaxjoBuildingPopulationMap(districtSpatialIndex) {
  const popData = await ensureGenderAgePopulationData().catch(() => null);
  if (!popData || !districtFC?.features || !baseCityFC?.features) return new Map();
  // Load Lantmäteriet index to resolve 'unknown' OSM building types
  const lmIndex = await ensureLantmaterietIndex();

  const demandDebug = {
    totalBuildings: Array.isArray(baseCityFC?.features) ? baseCityFC.features.length : 0,
    unknownTreatedAsResidentialStepB: 0,
    unknownTreatedAsResidentialStepC: 0,
    unknownResolvedByLantmateriet: 0,
    residentialBuildingsCounted: 0,
    assignedPopulationBuildings: 0
  };

  // Step A: build district → total population lookup
  const districtPop = new Map();
  for (const feat of districtFC.features) {
    const code = regsoCodeFromProps(feat.properties || {});
    if (!code) continue;
    const entry = lookupPopulationEntry(popData, code);
    if (!entry) continue;
    const years = extractAvailablePopulationYears(entry);
    const latestYear = years.length ? years[years.length - 1] : null;
    if (!latestYear) continue;
    const totals = aggregatePopulationTotals(entry, latestYear, 'All');
    if (totals?.total > 0) districtPop.set(code, totals.total);
  }

  // Step B: for each district, sum floor area of all residential buildings inside it
  const districtTotalFloor = new Map();
  baseCityFC.features.forEach((feat) => {
    const props = feat.properties || {};
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; } catch { return; }
    // Try OSM category first, fall back to Lantmäteriet lookup for 'unknown' buildings
    const rawCategory = props.category || props.building || props.objekttyp || '';
    const lmObjekttyp = (rawCategory === 'unknown' || !rawCategory)
      ? (lookupObjekttyp(centroid, lmIndex) || '')
      : '';
    const resolvedCategory = lmObjekttyp || rawCategory;
    const isUnknownRaw = String(rawCategory).toLowerCase() === 'unknown';
    const isUnknownWithoutMatch = !lmObjekttyp && isUnknownRaw;
    if (isUnknownRaw && lmObjekttyp) demandDebug.unknownResolvedByLantmateriet += 1;
    const isResidential =
      isUnknownWithoutMatch ||
      resolvedCategory.toLowerCase().includes('bostad') ||
      canonicalBuildingType(resolvedCategory) === 'residential';
    if (isUnknownWithoutMatch) demandDebug.unknownTreatedAsResidentialStepB += 1;
    if (!isResidential) return;
    demandDebug.residentialBuildingsCounted += 1;
    const code = findDistrictCodeForPoint(centroid, districtSpatialIndex);
    if (!code) return;
    const floorArea = buildingFloorAreaM2(feat);
    districtTotalFloor.set(code, (districtTotalFloor.get(code) || 0) + floorArea);
  });

  // Step C: assign each residential building its proportional population share
  const buildingPopMap = new Map();
  baseCityFC.features.forEach((feat, idx) => {
    const props = feat.properties || {};
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; } catch { return; }
    // Same resolution logic as Step B
    const rawCategory = props.category || props.building || props.objekttyp || '';
    const lmObjekttyp = (rawCategory === 'unknown' || !rawCategory)
      ? (lookupObjekttyp(centroid, lmIndex) || '')
      : '';
    const resolvedCategory = lmObjekttyp || rawCategory;
    const isUnknownWithoutMatch = !lmObjekttyp && String(rawCategory).toLowerCase() === 'unknown';
    const isResidential =
      isUnknownWithoutMatch ||
      resolvedCategory.toLowerCase().includes('bostad') ||
      canonicalBuildingType(resolvedCategory) === 'residential';
    if (isUnknownWithoutMatch) demandDebug.unknownTreatedAsResidentialStepC += 1;
    if (!isResidential) return;
    const code = findDistrictCodeForPoint(centroid, districtSpatialIndex);
    if (!code) return;
    const totalFloor = districtTotalFloor.get(code) || 0;
    const totalPop   = districtPop.get(code) || 0;
    if (totalFloor <= 0 || totalPop <= 0) return;
    const floorArea = buildingFloorAreaM2(feat);
    buildingPopMap.set(idx, (floorArea / totalFloor) * totalPop);
    demandDebug.assignedPopulationBuildings += 1;
  });

  window.__ifcityDemandDebug = demandDebug;
  return buildingPopMap;
}

/** Initialize demand weights once both districtFC and baseCityFC are loaded (Växjö only) */
async function initVaxjoDemandWeights() {
  if (!districtFC?.features?.length || !baseCityFC?.features?.length) return;
  if (districtCityKeyFromInput(lastCityName) !== 'vaxjo') return;
  vaxjoDistrictIndex  = buildVaxjoDistrictSpatialIndex();
  vaxjoBuildingPopMap = await buildVaxjoBuildingPopulationMap(vaxjoDistrictIndex);
  console.log('[IF-City] Växjö demand weights ready —',
    vaxjoBuildingPopMap.size, 'residential buildings assigned population estimates');
  if (window.__ifcityDemandDebug) {
    console.log('[IF-City] Demand debug:', window.__ifcityDemandDebug);
  }
}

async function computeIfCityFairness(catList, weightsByCat = {}, { setOverall = false } = {}) {
  if (!baseCityFC) throw new Error('No buildings loaded.');
  const updateUI = !setOverall;

  const fetched = await Promise.all(
    catList.map(cat =>
      fetchPOIs(cat, baseCityFC)
        .then(fc => ({ cat, fc }))
        .catch(() => ({ cat, fc: { type:'FeatureCollection', features:[] } }))
    )
  );

  const catToPOI = {};
  for (const { cat, fc } of fetched) {
    const filtered = filterFetchedPOIsForWhatIf(fc?.features || []);
    catToPOI[cat] = filtered.map(p => ({
      c: p.geometry.coordinates,
      name: p.properties?.name || '(unnamed)',
      v: ifCityOpportunityWeightFromPOI(cat, p)
    }));
  }

  const poiCount = fetched.reduce((sum, { fc }) => sum + (fc?.features?.length || 0), 0);

  if (updateUI) {
    const rawFeatures = fetched.flatMap(({cat, fc}) =>
      (fc?.features || []).map(feat => ({
        ...feat,
        properties: { ...(feat.properties || {}), __cat: cat }
      }))
    );
    currentPOIsFC = {
      type: 'FeatureCollection',
      features: dedupePOIFeaturesForDisplay(rawFeatures)
    };
    window.activePOICats = new Set(catList);
  }

  const whatIfMap = syncWhatIfPOIs(catList);
  if (updateUI && currentPOIsFC?.features?.length) {
    currentPOIsFC.features = dedupePOIFeaturesForDisplay(currentPOIsFC.features);
  }
  Object.entries(whatIfMap).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items.map(item => ({ ...item, v: 1 })));
  });
  const buildingPOIs = collectBuildingPOIsByCat(catList, { includeWhatIf: true });
  Object.entries(buildingPOIs).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items.map(item => ({ ...item, v: 1 })));
  });

  for (const cat of catList) {
    catToPOI[cat] = dedupeIFCityPOIs(catToPOI[cat]);
  }

  const benefits = [];
  baseCityFC.features.forEach((f, featIdx) => {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    const fm = {};
    if (updateUI) delete props.fair;

    let utility = 0;
    for (const cat of catList) {
      const arr = catToPOI[cat] || [];
      if (!arr.length) continue;
      const access = ifCityAccessibilityForBuilding(cB, arr, cat, fairnessTravelMode);
      fm[cat] = { access, score: access };
      const weight = Number.isFinite(weightsByCat[cat]) ? weightsByCat[cat] : 1;
      utility += weight * access;
    }

    const benefitRaw   = utility - IF_CITY_BASELINE_UTILITY;
    const equityWeight = ifCityEquityWeightForFeature(f);

    // Demand weight: proportional residents estimated for this building.
    // Large apartment block in a dense district → higher weight than a small cottage.
    // Non-residential buildings (not in the map) stay at neutral 1.0.
    let demandWeight = 1.0;
    if (vaxjoBuildingPopMap) {
      const estPop = vaxjoBuildingPopMap.get(featIdx) || 0;
      demandWeight = estPop > 0 ? Math.max(0.5, Math.min(5.0, Math.log1p(estPop))) : 1.0;
    }

    const benefit = Math.max(0, benefitRaw) * equityWeight * demandWeight;
    benefits.push(benefit);

    if (updateUI) {
      props.fair_multi = fm;
    }
    props.__ifcity = { utility, benefit, equity_weight: equityWeight, demand_weight: demandWeight };
  });

  const scores = normalizeBenefitsToScores(benefits, fairnessTravelMode);
  let idx = 0;
  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const score = scores[idx++] ?? 0;
    if (updateUI) {
      props.fair = { cat: catList.length > 1 ? 'mix' : catList[0], score };
    }
    if (setOverall) {
      props.fair_overall = { score };
    }
  }

  const inequality = generalizedEntropy(benefits, IF_CITY_ALPHA);
  if (updateUI) {
    fairActive = true;
    districtScoresSuppressed = false;
    fairCategory = catList.length > 1 ? 'mix' : catList[0];
    fairRecolorTick++;
    updateLayers();
  }

  if (updateUI) {
    const summary = summarizeFairnessCurrent();
    showSidePanel(fairCategory, inequality, currentPOIsFC.features.length, summary);
    window.getFairnessSummary = () => summary;
  }

  await refreshDistrictScores();
  await refreshMezoScores();
  if (updateUI) {
    updateLayers();
  }
  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }

  return { inequality, poiCount: updateUI ? currentPOIsFC.features.length : poiCount };
}

/* ---------- Single category (kept, still usable internally) ---------- */
async function computeFairnessFast(cat) {
  if (fairnessModel === 'ifcity') {
    const res = await computeIfCityFairness([cat], { [cat]: 1 });
    try {
      if (giniOut) giniOut.textContent = `${prettyPOIName(cat)} GE(α=2): ${formatFairnessBadgeValue(res.inequality)}`;
      if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
    } catch {}
    return { gini: res.inequality, poiCount: res.poiCount };
  }
  if (!baseCityFC) throw new Error('No buildings loaded.');
  const pois = await fetchPOIs(cat, baseCityFC);
  const filteredPOIFeatures = filterFetchedPOIsForWhatIf(pois.features || []);
  currentPOIsFC = {
    type: 'FeatureCollection',
    features: filteredPOIFeatures.map(feat => ({
      ...feat,
      properties: { ...(feat.properties || {}), __cat: cat }
    }))
  };

  const poiCoords = filteredPOIFeatures.map(p => ({ c: p.geometry.coordinates, name: p.properties?.name || '(unnamed)' }));
  const whatIfMap = syncWhatIfPOIs([cat]);
  if (whatIfMap?.[cat]?.length) {
    poiCoords.push(...whatIfMap[cat]);
  }
  const buildingPOIs = collectBuildingPOIsByCat([cat], { includeWhatIf: true });
  if (buildingPOIs?.[cat]?.length) {
    poiCoords.push(...buildingPOIs[cat]);
  }
  if (!poiCoords.length) throw new Error('No POIs found for this area.');
  const scoresLack = [];

  const allowRouting = allowRoutingForFairness(poiCoords.length);
  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    delete props.fair;
    props.fair_multi = {};

    let bestIdx = -1, bestD = Infinity;
    for (let i = 0; i < poiCoords.length; i++) {
      const d = haversineMeters(cB, poiCoords[i].c);
      if (d < bestD) { bestD = d; bestIdx = i; }
    }
    if (bestIdx < 0) continue;

    const nearest = poiCoords[bestIdx];
    const metrics = await getTravelMetricsForPair(cB, nearest.c, bestD, allowRouting, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);

    props.fair = {
      cat,
      score,
      nearest_name: nearest.name,
      nearest_dist_m: metrics.meters,
      nearest_time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null,
      nearest_lonlat: nearest.c
    };
    props.fair_multi[cat] = {
      score,
      dist_m: metrics.meters,
      time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null
    };

    scoresLack.push(1 - score);
  }

  const G = gini(scoresLack);

  fairActive = true;
  districtScoresSuppressed = false;
  fairCategory = cat;
  window.activePOICats = new Set([cat]);
  fairRecolorTick++;
  updateLayers();

  try {
    if (giniOut) giniOut.textContent = `${prettyPOIName(cat)} Gini: ${formatFairnessBadgeValue(G)}`;
    if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
  } catch {}

  const summary = summarizeFairnessCurrent();
  showSidePanel(cat, G, pois.features.length, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  return { gini: G, poiCount: pois.features.length };
}

/* ---------- Weighted mix across multiple categories ---------- */
async function computeFairnessWeighted(mix) {
  if (fairnessModel === 'ifcity') {
    const weightsByCat = mix.reduce((acc, item) => {
      acc[item.cat] = Number.isFinite(item.weight) ? item.weight : 1;
      return acc;
    }, {});
    const res = await computeIfCityFairness(mix.map(m => m.cat), weightsByCat);
    try {
      if (giniOut) giniOut.textContent = `Mix GE(α=2): ${formatFairnessBadgeValue(res.inequality)}`;
      if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
    } catch {}
    return { gini: res.inequality, poiCount: res.poiCount };
  }
  if (!baseCityFC) throw new Error('No buildings loaded.');

  const fetched = await Promise.all(
    mix.map(({cat}) =>
      fetchPOIs(cat, baseCityFC)
        .then(fc => ({ cat, fc }))
        .catch(() => ({ cat, fc: { type:'FeatureCollection', features:[] } }))
    )
  );

  const catToPOI = {};
  for (const {cat, fc} of fetched) {
    catToPOI[cat] = (fc?.features || []).map(p => ({ c: p.geometry.coordinates, name: p.properties?.name || '(unnamed)' }));
  }

  currentPOIsFC = {
    type: 'FeatureCollection',
    features: fetched.flatMap(({cat, fc}) =>
      (fc?.features || []).map(feat => ({
        ...feat,
        properties: { ...(feat.properties || {}), __cat: cat }
      }))
    )
  };

  window.activePOICats = new Set(mix.map(m => m.cat));
  const whatIfMap = syncWhatIfPOIs(mix.map(m => m.cat));
  Object.entries(whatIfMap).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items);
  });
  const buildingPOIs = collectBuildingPOIsByCat(mix.map(m => m.cat), { includeWhatIf: true });
  Object.entries(buildingPOIs).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items);
  });

  //I changed HERE//

  // const weightSum = mix.reduce((s, {weight}) => s + Math.max(0, weight), 0);
  const scoresLack = [];

  const maxPois = Math.max(0, ...mix.map(({ cat }) => catToPOI[cat]?.length || 0));
  const allowRouting = allowRoutingForFairness(maxPois);
  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    const fm = {};
    delete props.fair;

    for (const {cat} of mix) {
      const arr = catToPOI[cat] || [];
      if (!arr.length) continue;
      let bestD = Infinity, bestIdx = -1;
      for (let i = 0; i < arr.length; i++) {
        const d = haversineMeters(cB, arr[i].c);
        if (d < bestD) { bestD = d; bestIdx = i; }
      }
      if (bestIdx < 0) continue;
      const nearest = arr[bestIdx];
      const metrics = await getTravelMetricsForPair(cB, nearest.c, bestD, allowRouting, fairnessTravelMode);
      const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);
      fm[cat] = {
        score,
        dist_m: metrics.meters,
        time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null,
        nearest
      };
    }
      const entries = mix
      .map(({cat, weight}) => ({ w: Math.max(0, weight), s: fm[cat]?.score }))
      .filter(e => Number.isFinite(e.s) && e.w > 0);

    if (entries.length) {
      const best = Math.max(...entries.map(entry => entry.s));
      props.fair = { cat: 'mix', score: best };
      scoresLack.push(1 - best);
    } else {
      delete props.fair;
    }

    props.fair_multi = fm;
  }

  const G = gini(scoresLack);

  fairActive = true;
  districtScoresSuppressed = false;
  fairCategory = 'mix';
  fairRecolorTick++;
  updateLayers();

  const summary = summarizeFairnessCurrent();
  showSidePanel('mix', G, currentPOIsFC.features.length, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  return { gini: G, poiCount: currentPOIsFC.features.length };
}

/* ---------- Overall fairness across ALL categories (auto, parallel) ---------- */
async function computeOverallFairness(catList) {
  if (fairnessModel === 'ifcity') {
    const weightsByCat = catList.reduce((acc, cat) => {
      acc[cat] = 1;
      return acc;
    }, {});
    const res = await computeIfCityFairness(catList, weightsByCat, { setOverall: true });
    overallGini = res.inequality;
    districtScoresSuppressed = false;
    if (overallGiniOut) overallGiniOut.textContent = formatFairnessBadgeValue(res.inequality);
    if (parallelCoordsOpen) {
      updateParallelCoordsPanel();
    }
    return { overall_gini: res.inequality };
  }
  if (!baseCityFC) throw new Error('No buildings loaded.');
  const allowRouting = ROUTING_ENABLE_OVERALL && allowRoutingForFairness(catList?.length || 0);

  const jobs = catList.map(cat => (async () => {
    try {
      const fc = await fetchPOIsWithRetry(cat, baseCityFC);
      const arr = fc.features.map(p => ({ c: p.geometry.coordinates, name: p.properties?.name || '(unnamed)' }));
      return { cat, ok: true, arr };
    } catch (e) {
      console.warn('POI fetch failed for', cat, e);
      return { cat, ok: false, arr: [] };
    }
  })());
  const results = await Promise.all(jobs);

  const catToArr = {};
  catList.forEach((cat) => {
    catToArr[cat] = [];
  });

  results.forEach((r) => {
    if (!Array.isArray(catToArr[r.cat])) catToArr[r.cat] = [];
    if (r.ok && r.arr.length) {
      catToArr[r.cat].push(...r.arr);
    }
  });

  const buildingPOIs = collectBuildingPOIsByCat(catList, { includeWhatIf: true });
  Object.entries(buildingPOIs || {}).forEach(([cat, arr]) => {
    if (!Array.isArray(catToArr[cat])) catToArr[cat] = [];
    if (Array.isArray(arr) && arr.length) {
      catToArr[cat].push(...arr);
    }
  });

  const validCats = catList.filter((cat) => (catToArr[cat] || []).length > 0);

  if (!validCats.length) {
    overallGini = null;
    if (overallGiniOut) overallGiniOut.textContent = '—';
    if (parallelCoordsOpen) {
      updateParallelCoordsPanel();
    }
    return { overall_gini: null };
  }

  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    if (!props.fair_multi) props.fair_multi = {};
    const cB = turf.centroid(f).geometry.coordinates;

    for (const cat of validCats) {
      const arr = catToArr[cat];
      let bestD = Infinity;
      let bestIdx = -1;
      for (let i=0;i<arr.length;i++) {
        const d = haversineMeters(cB, arr[i].c);
        if (d < bestD) { bestD = d; bestIdx = i; }
      }
      const nearestCoord = bestIdx >= 0 ? arr[bestIdx].c : null;
      const metrics = nearestCoord
        ? await getTravelMetricsForPair(cB, nearestCoord, bestD, allowRouting, fairnessTravelMode)
        : { meters: bestD, seconds: estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode) };
      const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);
      props.fair_multi[cat] = {
        score,
        dist_m: metrics.meters,
        time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null
      };
    }

    const scores = Object.values(props.fair_multi).map(o => o.score).filter(Number.isFinite);
    if (scores.length) {
      const avg = scores.reduce((a,b)=>a+b,0)/scores.length;
      props.fair_overall = { score: avg };
    } else {
      delete props.fair_overall;
    }
  }

  const lack = baseCityFC.features
    .map(f => f.properties?.fair_overall?.score)
    .filter(Number.isFinite)
    .map(s => 1 - s);
  const overall = gini(lack);
  overallGini = overall;
  districtScoresSuppressed = false;
  if (overallGiniOut) overallGiniOut.textContent = formatFairnessBadgeValue(overall);

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  return { overall_gini: overall };
}



async function autoComputeOverall() {
  try {
    showGlobalSpinner('Computing overall fairness…');
    overallGiniOut && (overallGiniOut.textContent = '…');
    const res = await computeOverallFairness(ALL_CATEGORIES);
    hideGlobalSpinner();
    return res;
  } catch (e) {
    hideGlobalSpinner();
    console.error('Overall fairness error', e);
    overallGiniOut && (overallGiniOut.textContent = '—');
    return { overall_gini: null };
  }
}

/* ======================= POI markers ======================= */
function isPOISelectedFeature(f) {
  const id = f?.properties?.id ?? f?.properties?.osm_id ?? f?.properties?.['@id'];
  if (selectedPOIId && id && String(id) === String(selectedPOIId)) return true;

  if (window.activePOICats instanceof Set && window.activePOICats.size) {
    return window.activePOICats.has(poiCategoryOf(f));
  }

  if (fairActive && fairCategory && fairCategory !== 'mix') {
    return poiCategoryOf(f) === String(fairCategory).toLowerCase();
  }
  return false;
}

function resolveActivePOICategories() {
  const cats = new Set();
  if (window.activePOICats instanceof Set && window.activePOICats.size) {
    window.activePOICats.forEach(cat => {
      if (cat != null && cat !== '') cats.add(String(cat).toLowerCase());
    });
  }
  if (!cats.size) {
    if (fairCategory === 'mix') {
      selectedPOIMix.forEach(({ cat }) => {
        if (cat != null && cat !== '') cats.add(String(cat).toLowerCase());
      });
    } else if (fairCategory) {
      cats.add(String(fairCategory).toLowerCase());
    }
  }
  return cats;
}

function activePOIFeaturesForDisplay() {
  if (!fairCategory || !currentPOIsFC || !currentPOIsFC.features?.length) return [];
  const activeCats = resolveActivePOICategories();
  let base = activeCats.size
    ? currentPOIsFC.features.filter(f => activeCats.has(poiCategoryOf(f)))
    : currentPOIsFC.features;

  // Filter out POIs outside district boundaries (sea, neighbouring municipalities)
  const mask = ensureMezoMaskPolygon();
  if (mask?.geometry) {
    base = base.filter(f => {
      const coord = poiFeatureCoord(f);
      if (!coord) return false;
      try { return turf.booleanPointInPolygon(turf.point(coord), mask); } catch (_) { return true; }
    });
  }

  return dedupePOIFeaturesForDisplay(base);
}

function withPOIDisplayOffsets(features = []) {
  // Spread symbols that share the same coordinate so all POIs are visible.
  const byCoord = new Map();
  for (const f of features) {
    const c = f?.geometry?.coordinates;
    if (!Array.isArray(c) || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    const k = `${c[0].toFixed(6)},${c[1].toFixed(6)}`;
    if (!byCoord.has(k)) byCoord.set(k, []);
    byCoord.get(k).push(f);
  }

  const out = [];
  for (const arr of byCoord.values()) {
    if (arr.length === 1) {
      out.push(arr[0]);
      continue;
    }
    arr.forEach((f, i) => {
      const c = f.geometry.coordinates;
      const ring = 0.00008; // ~8m lat/lon visual spread (small)
      const ang = (2 * Math.PI * i) / arr.length;
      const lon = c[0] + Math.cos(ang) * ring;
      const lat = c[1] + Math.sin(ang) * ring;
      out.push({ ...f, __displayCoord: [lon, lat] });
    });
  }
  return out;
}

function createPOIDotLayer() {
  if (!showPOISymbols) return null;
  const baseData = activePOIFeaturesForDisplay();
  if (!baseData.length) return null;
  const data = withPOIDisplayOffsets(baseData);

  return new deck.ScatterplotLayer({
    id: 'poi-dots',
    data,
    pickable: true,
    stroked: true,
    radiusUnits: 'pixels',
    getRadius: f => isPOISelectedFeature(f) ? 7 : 5,
    getFillColor: f => poiColor(poiCategoryOf(f)),
    getLineColor: [20, 20, 20, 220],
    lineWidthMinPixels: 1,
    getPosition: f => f.__displayCoord || f?.geometry?.coordinates || [0, 0],
    parameters: { depthTest: false },
    updateTriggers: {
      getRadius: [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick],
      getFillColor: [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick]
    }
  });
}

// function createPOIPointLayer() {
//   // Draw only when fairness is active AND we have POIs AND symbols are toggled ON
//   if (!showPOISymbols) return null;
//   const baseData = activePOIFeaturesForDisplay();
//   if (!baseData.length) return null;
//   const data = withPOIDisplayOffsets(baseData);

//   // NOTE: Removed halo/circle layer entirely
//   return new deck.TextLayer({
//     id: 'poi-symbols',
//     data,
//     pickable: true,
//     collisionEnabled: false,
//     characterSet: [...'●⬛▲▼▶◀◆⬢⬟⬭◼'],
//     fontFamily: '"Noto Sans Symbols 2","Noto Sans Symbols","Segoe UI Symbol","Arial Unicode MS",sans-serif',
//     sizeUnits: 'pixels',
//     getSize: f => isPOISelectedFeature(f) ? 30 : 22,
//     sizeMinPixels: 10,
//     sizeMaxPixels: 50,
//     getText:  f => poiGlyph(poiCategoryOf(f)),
//     getColor: f => {
//       const c = poiColor(poiCategoryOf(f)).slice();
//       if ((window.activePOICats && window.activePOICats.size) && !isPOISelectedFeature(f)) {
//         c[3] = Math.min(160, c[3] ?? 255);
//       }
//       return c;
//     },
//     getPosition: f => {
//      if (Array.isArray(f.__displayCoord)) return f.__displayCoord;
//       const g = f.geometry;
//       if (!g) return [0,0];
//       if (g.type === 'Point') return g.coordinates;
//       try { return turf.centroid(f).geometry.coordinates; } catch { return [0,0]; }
//     },
//     onClick: async info => {
//       noteFeatureClick();
//       if (fairActive) saveTransitionScores();
//       const id = info?.object?.properties?.id ?? info?.object?.properties?.osm_id ?? info?.object?.properties?.['@id'];
//       selectedPOIId = (id != null) ? String(id) : null;
//       selectedPOIFeature = info?.object || null;
//       if (info && info.coordinate) {
//         showPopup(info.object, info.coordinate);
//       }
//       if (districtView) {
//         await refreshDistrictScores();
//       }
//       if (mezoView) {
//         await refreshMezoScores();
//       }
//       updateLayers();
//       if (info && info.coordinate) {
//         try { map.flyTo({ center: info.coordinate, zoom: Math.max(map.getZoom(), 16), speed: 0.8 }); } catch {}
//       }
//     },
//     // onClick: info => {
//     //   const id = info?.object?.properties?.id ?? info?.object?.properties?.osm_id ?? info?.object?.properties?.['@id'];
//     //   selectedPOIId = (id != null) ? String(id) : null;
//     //   const props = info?.object?.properties || {};
//     //   if (info && info.coordinate) {
//     //     showPopup(props, info.coordinate);
//     //   }
//     //   updateLayers();
//     //   if (info && info.coordinate) {
//     //     try { map.flyTo({ center: info.coordinate, zoom: Math.max(map.getZoom(), 16), speed: 0.8 }); } catch {}
//     //   }
//     // },
//     //Change for tooltip width part2
//     parameters: { depthTest: false },
//     updateTriggers: {
//       getSize:  [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick],
//       getColor: [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick],
//       getText:  [poiStyleTick]
//     }
//   });
// }

function createPOIPointLayer() {
  if (!showPOISymbols) return null;
  if (!_poiAtlasReady) {
    ensurePOIIconAtlas();
    return null;
  }
  const baseData = activePOIFeaturesForDisplay();
  if (!baseData.length) return null;
  const data = withPOIDisplayOffsets(baseData);

  return new deck.IconLayer({
    id: 'poi-symbols',
    data,
    pickable: true,
    iconAtlas: _poiIconAtlas,
    iconMapping: _poiIconMapping,
    getIcon: f => {
      const cat = poiCategoryOf(f);
      return _poiIconMapping[cat] ? cat : 'default';
    },
    getSize: f => isPOISelectedFeature(f) ? 36 : 24,
    sizeUnits: 'pixels',
    sizeMinPixels: 12,
    sizeMaxPixels: 50,
    getPosition: f => {
      if (Array.isArray(f.__displayCoord)) return f.__displayCoord;
      const g = f.geometry;
      if (!g) return [0, 0];
      if (g.type === 'Point') return g.coordinates;
      try { return turf.centroid(f).geometry.coordinates; } catch { return [0, 0]; }
    },
    onClick: async info => {
      noteFeatureClick();
      if (fairActive) saveTransitionScores();
      const id = info?.object?.properties?.id ?? info?.object?.properties?.osm_id ?? info?.object?.properties?.['@id'];
      selectedPOIId = (id != null) ? String(id) : null;
      selectedPOIFeature = info?.object || null;
      if (info && info.coordinate) showPopup(info.object, info.coordinate);
      if (districtView) await refreshDistrictScores();
      if (mezoView) await refreshMezoScores();
      updateLayers();
      if (info && info.coordinate) {
        try { map.flyTo({ center: info.coordinate, zoom: Math.max(map.getZoom(), 16), speed: 0.8 }); } catch {}
      }
    },
    parameters: { depthTest: false },
    updateTriggers: {
      getSize: [selectedPOIId, poiStyleTick],
      getIcon: [poiStyleTick]
    }
  });
}

function buildingMatchesPOI(props, cat) {
  if (!props) return false;
  const amen = String(props.amenity || '').toLowerCase();
  const shop = String(props.shop || '').toLowerCase();
  const healthcare = String(props.healthcare || '').toLowerCase();
  const name = String(props.name || props.namn || '').toLowerCase();
  const building = String(props.building || props.objekttyp || '').toLowerCase();
  const t = propsText(props);

  switch (cat) {
    case 'grocery':
      return shop === 'supermarket' || shop === 'convenience' || shop === 'greengrocer' ||
             amen === 'marketplace' || name.includes('ica') || name.includes('coop') ||
             name.includes('lidl') || name.includes('willys') ||
             textHasAny(t, ['matbutik', 'livsmedel', 'supermarket', 'grocery', 'dagligvaru']);
    case 'hospital':          return amen === 'hospital' || healthcare === 'hospital' || textHasAny(t, ['sjukhus', 'hospital', 'lasarett', 'akutmottagning']);
    case 'pharmacy':          return amen === 'pharmacy' || textHasAny(t, ['apotek', 'pharmacy']);
    case 'dentistry':         return amen === 'dentist' || textHasAny(t, ['tandvard', 'tandvård', 'tandlakare', 'tandläkare', 'dentist', 'dental']);
    case 'healthcare_center': return amen === 'clinic' || healthcare.includes('clinic') || healthcare.includes('centre') ||
                                        healthcare.includes('center') || healthcare.includes('doctor') ||
                                        textHasAny(t, ['vårdcentral', 'vardcentral', 'hälsocentral', 'halsocentral', 'clinic', 'medical center', 'care center', 'husläkare']);
    case 'veterinary':        return amen === 'veterinary' || textHasAny(t, ['veterinär', 'veterinar', 'djurklinik', 'animal hospital', 'djursjukhus']);
    case 'university':        return amen === 'university' || building === 'university' ||
                                        name.includes('university') || name.includes('college') ||
                                        name.includes('campus') || /\buniversitet\b/.test(t);
    case 'kindergarten':      return amen === 'kindergarten' || amen === 'childcare' || /\bforskola\b|\bförskola\b/.test(t);
    case 'school_primary': {
      const schoolLike = amen === 'school' || textHasAny(t, ['skola', 'school']);
      const highLike = isHighSchoolLike(props);
      return schoolLike && (isPrimarySchoolLike(props) || !highLike);
    }
    case 'school_high': {
      const schoolLike = amen === 'school' || textHasAny(t, ['skola', 'school', 'gymnas']);
      return schoolLike && isHighSchoolLike(props);
    }
    default: return false;
  }
}

function buildPOIBuildingDots(catList) {
  if (!baseCityFC?.features?.length) return [];
  const cats = Array.isArray(catList) ? catList : [catList];
  const pts = [];
  for (const f of baseCityFC.features) {
    if (!f.properties) continue;
    for (const cat of cats) {
      if (buildingMatchesPOI(f.properties, cat)) {
        const c = turf.centroid(f).geometry.coordinates;
        pts.push({ position: c, name: f.properties.name || '', category: cat });
        break;
      }
    }
  }
  const asFeatures = pts.map((p, idx) => ({
    type: 'Feature',
    properties: {
      id: `fallback:${p.category}:${idx}`,
      name: p.name || '',
      category: p.category
    },
    geometry: { type: 'Point', coordinates: p.position }
  }));
  const deduped = dedupeLocalBuildingPOIs('fallback', asFeatures);
  return deduped.map((f) => ({
    position: f.geometry.coordinates,
    name: f.properties?.name || '',
    category: f.properties?.category || ''
  }));
}

function createPOIBuildingMarkerLayer() {
  // If we already have fetched POIs (TextLayer) or symbols are toggled off, skip the fallback layer
  if (!fairCategory || !showPOISymbols) return null;
  if (activePOIFeaturesForDisplay().length) return null;
  let cats = [];
  if (fairCategory === 'mix') {
    cats = selectedPOIMix.map(e => e.cat);
  } else {
    cats = [fairCategory];
  }
  const data = buildPOIBuildingDots(cats);
  if (!data.length) return null;

  return new deck.ScatterplotLayer({
    id: 'poi-building-dots',
    data,
    pickable: false,
    getPosition: d => d.position,
    getFillColor: [0,180,255],
    getRadius: 5,
    radiusUnits: 'pixels',
    stroked: true,
    getLineColor: [20,20,20],
    getLineWidth: 1.5,
    parameters: { depthTest: false },
    opacity: 0.95,
    updateTriggers: { data: [fairCategory, fairRecolorTick, JSON.stringify(selectedPOIMix)] }
  });
}

/* ======================= NEW: Best/Least overlays ======================= */
function clearBestWorstHighlights() {
  bw = { bldgBest:null, bldgWorst:null, districtBest:null, districtWorst:null, mode:null };
  bwTick++;
}

function setBestWorstHighlights(summary) {
  clearBestWorstHighlights();
  updateLayers();
}

function createBestWorstLayers() {
  return [];
}

function formatDistrictMeta(props = {}) {
  const rows = [];
  for (const [key, value] of Object.entries(props)) {
    if (key.startsWith('__')) continue;
    if (value == null || value === '') continue;
    rows.push({ key, value });
  }
  rows.sort((a, b) => a.key.localeCompare(b.key));
  return rows.map(({ key, value }) => (
    `<tr><th>${escapeHTML(key)}</th><td>${escapeHTML(String(value))}</td></tr>`
  )).join('');
}

function formatDistrictFairnessRows(props = {}) {
  const rows = [];
  const focusedScore = Number.isFinite(props.__fairFocused) ? props.__fairFocused : null;
  const focusedCat = props.__fairFocusedCat;
  if (focusedScore != null && focusedCat) {
    rows.push(`<tr><th>Selected POI (${escapeHTML(prettyPOIName(focusedCat))})</th><td>${focusedScore.toFixed(2)}</td></tr>`);
  }
  const overall = Number.isFinite(props.__fairOverall) ? props.__fairOverall : null;
  rows.push(`<tr><th>Overall fairness</th><td>${overall != null ? overall.toFixed(2) : '—'}</td></tr>`);
  rows.push(`<tr><th colspan="2" style="padding-top:6px; font-weight:700;">Fairness by category</th></tr>`);
  const byCat = props.__fairByCat || {};
  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  catList.forEach((cat) => {
    const val = byCat?.[cat];
    rows.push(`<tr><th>${escapeHTML(prettyPOIName(cat))}</th><td>${Number.isFinite(val) ? val.toFixed(2) : '—'}</td></tr>`);
  });
  return rows.join('');
}

function buildDistrictPopupHTML({ name, score, count, props, income }) {
  const safeName = name ? escapeHTML(name) : 'District';
  const metaRows = formatDistrictMeta(props);
  const fairnessRows = formatDistrictFairnessRows(props);
  let incomeRows = '';
  if (income) {
    const total = income['1+2']?.mean_tkr;
    const men   = income['1']?.mean_tkr;
    const women = income['2']?.mean_tkr;
    const gap   = (men != null && women != null) ? (men - women).toFixed(1) : null;
    incomeRows = `
      <tr><th colspan="2" style="padding-top:6px; font-weight:700;">Income (tkr/year)</th></tr>
      <tr><th>Average (total)</th><td>${total != null ? total.toFixed(1) : '—'}</td></tr>
      <tr><th>Men</th><td>${men != null ? men.toFixed(1) : '—'}</td></tr>
      <tr><th>Women</th><td>${women != null ? women.toFixed(1) : '—'}</td></tr>
      <tr><th>Gender gap</th><td>${gap != null ? gap + ' tkr' : '—'}</td></tr>`;
  }
  return `
    <div style="min-width:220px; max-width:320px;">
      <div class="popup-title">${safeName}</div>
      <table class="popup-table">
        <tr><th>Buildings counted</th><td>${count || '—'}</td></tr>
        <tr><th>Mean fairness</th><td>${score != null ? score.toFixed(2) : '—'}</td></tr>
        ${fairnessRows}
        ${incomeRows}
        ${metaRows || '<tr><th>Details</th><td>—</td></tr>'}
      </table>
    </div>`;
}

function districtPopupDataForSummary(d) {
  if (!d) return null;
  let matchFeature = null;
  if (districtFC?.features?.length) {
    matchFeature = districtFC.features.find((feat, idx) => {
      const props = feat?.properties || {};
      const name = props.__districtName || districtNameOf(props, idx);
      return name === d.name;
    }) || null;
  }
  const props = matchFeature?.properties || {};
  const hasProps = Object.keys(props).length > 0;
  const name = props.__districtName || (hasProps ? districtNameOf(props) : d.name) || d.name;
  const score = Number.isFinite(props.__score) ? props.__score : (Number.isFinite(d.mean) ? d.mean : null);
  const count = Number.isFinite(props.__count) ? props.__count : (Number.isFinite(d.count) ? d.count : 0);
  const popupFeature = matchFeature || districtFeatureFromSummary(d);
  const coord = popupFeature ? turf.centroid(popupFeature).geometry.coordinates : null;
  return { name, score, count, props, coord };
}

function showDistrictPopup(atLngLat, data) {
  if (!atLngLat || !data) return;
  const html = buildDistrictPopupHTML(data);
  closePopup();
  currentPopup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: true,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '360px'
  }).setLngLat(atLngLat).setHTML(html).addTo(map);
}

function showDistrictSummaryPopup(d) {
  const data = districtPopupDataForSummary(d);
  if (!data?.coord) return;
  showDistrictPopup(data.coord, data);
}

function closeDistrictPopulationPopup() {
  if (districtPopulationPopup) {
    districtPopulationPopup.remove();
    districtPopulationPopup = null;
  }
}

/**
 * Convert SCB flat format [{key:[regso,ageGroup,gender,year], values:["count"]}, ...]
 * into nested per-district objects that the existing population UI can consume:
 *   { regsokod, <ageGroup>: { <year>: { male, female, total } }, ... }
 */
function convertSCBFlatToNested(flatData) {
  if (!Array.isArray(flatData) || !flatData.length) return flatData;
  // Detect SCB format: first entry has key array of length >= 4
  const sample = flatData[0];
  if (!Array.isArray(sample?.key) || sample.key.length < 4) return flatData;

  const byRegso = new Map();

  for (const row of flatData) {
    const [rawRegso, ageGroup, genderCode, year] = row.key;
    const count = Number(row.values?.[0]) || 0;

    // Strip suffix like "_RegSO2025" to get clean regso code
    const regso = String(rawRegso).replace(/_RegSO\d+/i, '');

    if (!byRegso.has(regso)) {
      byRegso.set(regso, { regsokod: regso });
    }
    const entry = byRegso.get(regso);

    // Create nested: entry[ageGroup][year] = { male, female, total }
    if (!entry[ageGroup]) entry[ageGroup] = {};
    if (!entry[ageGroup][year]) entry[ageGroup][year] = {};

    const bucket = entry[ageGroup][year];
    if (genderCode === '1') {
      bucket.male = count;
    } else if (genderCode === '2') {
      bucket.female = count;
    } else if (genderCode === '1+2') {
      bucket.total = count;
    }
  }

  const result = Array.from(byRegso.values());
  console.log(`convertSCBFlatToNested: ${flatData.length} flat rows → ${result.length} district entries`);
  return result;
}

function normalizePopulationData(raw) {
  if (Array.isArray(raw)) {
    // Detect SCB flat format: [{key:[...], values:[...]}, ...]
    if (raw.length && Array.isArray(raw[0]?.key) && raw[0].key.length >= 4) {
      return convertSCBFlatToNested(raw);
    }
    return raw;
  }
  if (raw && Array.isArray(raw.data)) return normalizePopulationData(raw.data);
  if (raw && Array.isArray(raw.records)) return normalizePopulationData(raw.records);
  return raw || null;
}

let _genderAgePopCityKey = null;

async function ensureGenderAgePopulationData() {
  const cityKey = districtCityKeyFromInput(lastCityName) || null;
  const popUrl = (cityKey && GENDER_AGE_POP_URL_BY_CITY_KEY[cityKey])
    ? GENDER_AGE_POP_URL_BY_CITY_KEY[cityKey]
    : DEFAULT_GENDER_AGE_POP_URL;

  // Invalidate cache when city changes
  if (_genderAgePopCityKey !== cityKey) {
    genderAgePopulation = null;
    genderAgePopulationPromise = null;
    _genderAgePopCityKey = cityKey;
  }

  if (genderAgePopulation) return genderAgePopulation;
  if (genderAgePopulationPromise) return genderAgePopulationPromise;

  console.log(`Loading population data for city "${cityKey}" from ${popUrl}`);
  genderAgePopulationPromise = fetch(popUrl)
    .then(r => {
      if (!r.ok) throw new Error(`Population fetch failed (${r.status})`);
      return r.json();
    })
    .then(raw => {
      genderAgePopulation = normalizePopulationData(raw);
      return genderAgePopulation;
    })
    .catch(err => {
      genderAgePopulationPromise = null;
      throw err;
    });
  return genderAgePopulationPromise;
}
function regsoCodeFromProps(props = {}) {
  return props.regsokod || props.REGSOKOD || props.regso || props.REGSO || props.regso_kod || props.Regso || null;
}

function lookupPopulationEntry(data, regsoCode) {
  if (!data || !regsoCode) return null;
  if (Array.isArray(data)) {
    return data.find(item => {
      const key = item?.regsokod || item?.regso || item?.code || item?.key || item?.id || item?.REGSOKOD;
      return key != null && String(key) === String(regsoCode);
    }) || null;
  }
  if (data && typeof data === 'object') {
    if (data[regsoCode]) return data[regsoCode];
    const altKey = String(regsoCode);
    if (data[altKey]) return data[altKey];
    if (Array.isArray(data.records)) return lookupPopulationEntry(data.records, regsoCode);
    if (Array.isArray(data.data)) return lookupPopulationEntry(data.data, regsoCode);
  }
  return null;
}

// function districtNameByRegsoCode(regsoCode) {
//   if (!districtFC?.features?.length || !regsoCode) return '';
//   const idx = districtFC.features.findIndex(feature => String(regsoCodeFromProps(feature?.properties || {})) === String(regsoCode));
//   const match = idx >= 0 ? districtFC.features[idx] : null;
//   if (!match?.properties) return '';
//   return districtNameOf(match.properties, idx);
// }

// function getPopulationEntriesWithRegso(data) {
//   if (!data) return [];

//   if (districtFC?.features?.length) {
//     const districtEntries = districtFC.features
//       .map((feature, idx) => {
//         const regsoCode = regsoCodeFromProps(feature?.properties || {});
//         if (!regsoCode) return null;
//         const entry = lookupPopulationEntry(data, regsoCode);
//         if (!entry) return null;
//         return { regsoCode: String(regsoCode), entry, districtName: districtNameOf(feature.properties || {}, idx) };
//       })
//       .filter(Boolean);
//     if (districtEntries.length) return districtEntries;
//   }

//   if (Array.isArray(data)) {
//     return data
//       .map(entry => ({
//         regsoCode: regsoCodeFromProps(entry || {}) || entry?.code || entry?.key || entry?.id || entry?.REGSOKOD || null,
//         entry
//       }))
//       .filter(item => item.regsoCode && item.entry);
//   }

//   if (typeof data === 'object') {
//     if (Array.isArray(data.records)) return getPopulationEntriesWithRegso(data.records);
//     if (Array.isArray(data.data)) return getPopulationEntriesWithRegso(data.data);
//     return Object.entries(data)
//       .map(([regsoCode, entry]) => ({ regsoCode, entry }))
//       .filter(item => item.regsoCode && item.entry && typeof item.entry === 'object' && /^\d{4}R\d{3}$/i.test(String(item.regsoCode)));
//   }

//   return [];
// }

// async function debugDistrictPopulationComparison(options = {}) {
//   const data = await ensureGenderAgePopulationData();
//   const entries = getPopulationEntriesWithRegso(data);

//   const requestedYear = Number(options.year);
//   const discoveredYears = new Set();
//   entries.forEach(({ entry }) => {
//     extractAvailablePopulationYears(entry).forEach(year => discoveredYears.add(year));
//   });
//   const sortedYears = Array.from(discoveredYears).sort((a, b) => a - b);
//   const fallbackYear = sortedYears.length ? sortedYears[sortedYears.length - 1] : null;
//   const year = Number.isFinite(requestedYear) ? requestedYear : (fallbackYear || 2023);

//   const ageGroup = options.ageGroup || 'All';
//   const maxRows = Number.isFinite(Number(options.maxRows)) ? Math.max(1, Number(options.maxRows)) : 200;
//   const onlyMismatched = !!options.onlyMismatched;

//   const rows = [];
//   let missingTotals = 0;

//   entries.forEach(({ regsoCode, entry, districtName }) => {
//     const totals = aggregatePopulationTotals(entry, year, ageGroup);
//     if (!totals) {
//       missingTotals += 1;
//       return;
//     }

//     const male = Number(totals.male) || 0;
//     const female = Number(totals.female) || 0;
//     const total = Number(totals.total) || 0;
//     const expectedTotal = male + female;
//     const delta = total - expectedTotal;

//     rows.push({
//       regsoCode: String(regsoCode),
//       districtName: districtName || districtNameByRegsoCode(regsoCode),
//       year,
//       ageGroup,
//       male,
//       female,
//       total,
//       expectedTotal,
//       delta,
//       maleFemaleRatio: female > 0 ? Number((male / female).toFixed(3)) : null
//     });
//   });

//   rows.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
//   const mismatches = rows.filter(row => Math.abs(row.delta) >= 0.5);
//   const shown = (onlyMismatched ? mismatches : rows).slice(0, maxRows);

//   console.groupCollapsed(`[Population Debug] ${lastCityName || 'City'} · year ${year} · age ${ageGroup}`);
//   console.log(`population entries discovered: ${entries.length}`);
//   console.log(`districts with computed totals: ${rows.length}`);
//   console.log(`districts with no totals for selected filters: ${missingTotals}`);
//   console.log(`districts where total != male + female: ${mismatches.length}`);
//   if (requestedYear && !discoveredYears.has(requestedYear)) {
//     console.warn(`Requested year ${requestedYear} not found in discovered data years: ${sortedYears.join(', ') || 'none'}`);
//   }
//   console.table(shown);
//   console.groupEnd();

//   return { rows, mismatches, missingTotals, discoveredYears: sortedYears };
// }

// if (typeof window !== 'undefined') {
//   window.debugDistrictPopulationComparison = debugDistrictPopulationComparison;
// }


function flattenPopulationRows(value, prefix = '') {
  const rows = [];
  const addRow = (label, val) => {
    if (val == null || val === '') return;
    rows.push({ label, value: val });
  };

  if (Array.isArray(value)) {
    if (!value.length) return rows;
    const isObjectArray = value.every(item => item && typeof item === 'object' && !Array.isArray(item));
    if (isObjectArray) {
      value.forEach((item, idx) => {
        const labelKeyField = ['age', 'age_group', 'ageGroup', 'group', 'label', 'range']
          .find(key => item && Object.prototype.hasOwnProperty.call(item, key));
        const labelValue = labelKeyField ? item[labelKeyField] : `Item ${idx + 1}`;
        const nextPrefix = prefix ? `${prefix} ${labelValue}` : String(labelValue);
        const clone = { ...item };
        if (labelKeyField) delete clone[labelKeyField];
        rows.push(...flattenPopulationRows(clone, nextPrefix));
      });
      return rows;
    }
    addRow(prefix || 'Values', value.join(', '));
    return rows;
  }

  if (value && typeof value === 'object') {
    for (const [key, val] of Object.entries(value)) {
      if (key.startsWith('__')) continue;
      const nextPrefix = prefix ? `${prefix} ${key}` : key;
      if (val && typeof val === 'object') {
        rows.push(...flattenPopulationRows(val, nextPrefix));
      } else {
        addRow(nextPrefix, val);
      }
    }
    return rows;
  }

  if (prefix) addRow(prefix, value);
  else addRow('Value', value);
  return rows;
}

function buildDistrictPopulationHTML({ name, regsoCode, rows }) {
  const safeName = name ? escapeHTML(name) : 'District';
  const safeRegso = regsoCode ? escapeHTML(String(regsoCode)) : '—';
  const safeRows = rows?.length
    ? rows.map(({ label, value }) => (
      `<tr><th>${escapeHTML(label)}</th><td>${escapeHTML(String(value))}</td></tr>`
    )).join('')
    : '<tr><th>Population data</th><td>Not available</td></tr>';
  return `
    <div style="min-width:260px; max-width:520px;">
      <div class="popup-title">${safeName}</div>
      <table class="popup-table">
        <tr><th>Regso code</th><td>${safeRegso}</td></tr>
        ${safeRows}
      </table>
    </div>`;
}

function showDistrictPopulationPopup(atLngLat, data) {
  if (!atLngLat || !data) return;
  const html = buildDistrictPopulationHTML(data);
  closeDistrictPopulationPopup();
  districtPopulationPopup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: true,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '520px'
  }).setLngLat(atLngLat).setHTML(html).addTo(map);
  districtPopulationPopup.addClassName?.('district-popup');
  ensurePopupInView(districtPopulationPopup);
}

function setDistrictPopulationPanelText(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value || '—';
}

function setDistrictPopulationPanelNote(text) {
  const el = document.getElementById('districtPopulationNote');
  if (!el) return;
  el.textContent = text || '';
}

const populationTotalKeyRegex = /^(total|totalt|overall|sum|population|pop|all)$/i;
const populationValueRegex = {
  total: /^(total|totalt|overall|sum|population|pop|all)$/i
};

function normalizePopulationFieldKey(key) {
  return String(key || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function isPopulationMaleFieldKey(key) {
  const raw = String(key || '').toLowerCase().trim();
  const normalized = normalizePopulationFieldKey(key);
  if (!normalized) return false;
  if (["m", "male", "man", "men", "boy", "boys"].includes(normalized)) return true;
  if (/(^|_)(male|man|men|boy|boys)(_|$)/.test(normalized)) return true;
  return /(män|pojkar?)/i.test(raw);
}

function isPopulationFemaleFieldKey(key) {
  const raw = String(key || '').toLowerCase().trim();
  const normalized = normalizePopulationFieldKey(key);
  if (!normalized) return false;
  if (["f", "female", "woman", "women", "girl", "girls"].includes(normalized)) return true;
  if (/(^|_)(female|woman|women|girl|girls|kvinn)(_|$)/.test(normalized)) return true;
  return /(kvinn|flickor?)/i.test(raw);
}

function isPopulationTotalFieldKey(key) {
  const normalized = normalizePopulationFieldKey(key);
  if (!normalized) return false;
  if (isPopulationMaleFieldKey(key) || isPopulationFemaleFieldKey(key)) return false;
  if (populationValueRegex.total.test(normalized) || populationTotalKeyRegex.test(normalized)) {
    return true;
  }
  return [
    'total_population',
    'population_total',
    'total_pop',
    'pop_total'
  ].includes(normalized);
}

function findPopulationAgeArray(value) {
  if (Array.isArray(value)) {
    const ageKey = ['age', 'age_group', 'ageGroup', 'group', 'range', 'label']
      .find(key => value.every(item => item && typeof item === 'object' && !Array.isArray(item) && key in item));
    if (ageKey) return { items: value, ageKey };
    for (const item of value) {
      if (item && typeof item === 'object') {
        const found = findPopulationAgeArray(item);
        if (found) return found;
      }
    }
    return null;
  }
  if (value && typeof value === 'object') {
    for (const nested of Object.values(value)) {
      const found = findPopulationAgeArray(nested);
      if (found) return found;
    }
  }
  return null;
}

function extractPopulationRowTotals(item, ageKey) {
  const numericEntries = Object.entries(item || {})
    .filter(([key, val]) => key !== ageKey && Number.isFinite(Number(val)));
  if (!numericEntries.length) return null;

  let male = 0;
  let female = 0;
  let hasMale = false;
  let hasFemale = false;
  let total = null;
  let fallbackTotal = 0;

  numericEntries.forEach(([key, val]) => {
    const num = Number(val);
    if (isPopulationTotalFieldKey(key)) total = num;
    if (isPopulationMaleFieldKey(key)) { male += num; hasMale = true; }
    if (isPopulationFemaleFieldKey(key)) { female += num; hasFemale = true; }
    fallbackTotal += num;
  });

  const resolvedTotal = (hasMale && hasFemale) ? (male + female) : (total != null ? total : (hasMale || hasFemale ? (male + female) : fallbackTotal));
  return { male, female, total: resolvedTotal, hasMale, hasFemale };
}

function isPopulationYearBucket(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const yearKeys = Object.keys(value).filter(key => /^\d{4}$/.test(key));
  if (!yearKeys.length) return false;
  const sample = value[yearKeys[0]];
  return !!(sample && typeof sample === 'object' && !Array.isArray(sample));
}

function getPopulationAgeGroupMap(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null;
  const ageMap = {};
  let found = false;
  Object.entries(entry).forEach(([key, val]) => {
    if (isPopulationYearBucket(val)) {
      ageMap[key] = val;
      found = true;
    }
  });
  return found ? ageMap : null;
}

function extractPopulationYearTotals(yearData) {
  if (!yearData || typeof yearData !== 'object') return null;
  const entries = Object.entries(yearData)
    .filter(([, val]) => Number.isFinite(Number(val)));
  if (!entries.length) return null;

  let male = 0;
  let female = 0;
  let hasMale = false;
  let hasFemale = false;
  let total = null;
  let fallbackTotal = 0;

  entries.forEach(([key, val]) => {
    const num = Number(val);
    if (isPopulationTotalFieldKey(key)) total = num;
    if (isPopulationMaleFieldKey(key)) { male += num; hasMale = true; }
    if (isPopulationFemaleFieldKey(key)) { female += num; hasFemale = true; }
    fallbackTotal += num;
  });

  const resolvedTotal = (hasMale && hasFemale) ? (male + female) : (total != null ? total : (hasMale || hasFemale ? (male + female) : fallbackTotal));
  return { male, female, total: resolvedTotal, hasMale, hasFemale };
}

function extractAvailablePopulationYears(entry) {
  if (!entry) return [];
  const years = new Set();
  const ageMap = getPopulationAgeGroupMap(entry);

  if (ageMap) {
    Object.values(ageMap).forEach(bucket => {
      if (!bucket || typeof bucket !== 'object') return;
      Object.keys(bucket).forEach(key => {
        const year = Number(key);
        if (Number.isFinite(year) && year >= 2010 && year <= 2023) {
          years.add(year);
        }
      });
    });
    return Array.from(years).sort((a, b) => a - b);
  }

  const checkYear = year => {
    if (!Number.isFinite(year)) return;
    if (year >= 2010 && year <= 2023) years.add(year);
  };

  const visit = value => {
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value)) {
      value.forEach(item => {
        if (item && typeof item === 'object') {
          const yearKey = Object.keys(item).find(key => /year/i.test(key));
          if (yearKey) checkYear(Number(item[yearKey]));
        }
        visit(item);
      });
      return;
    }
    Object.entries(value).forEach(([key, val]) => {
      const numericKey = Number(key);
      if (Number.isFinite(numericKey) && String(numericKey).length === 4) {
        checkYear(numericKey);
      }
      if (val && typeof val === 'object') {
        const yearKey = Object.keys(val).find(innerKey => /year/i.test(innerKey));
        if (yearKey) checkYear(Number(val[yearKey]));
        visit(val);
      }
    });
  };

  visit(entry);
  return Array.from(years).sort((a, b) => a - b);
}

function findPopulationYearNode(entry, year) {
  if (!entry || !Number.isFinite(Number(year))) return null;
  const targetYear = Number(year);
  let found = null;

  const visit = value => {
    if (found || !value || typeof value !== 'object') return;
    if (Array.isArray(value)) {
      for (const item of value) {
        if (found) return;
        if (item && typeof item === 'object') {
          const yearKey = Object.keys(item).find(key => /year/i.test(key));
          if (yearKey && Number(item[yearKey]) === targetYear) {
            found = item;
            return;
          }
        }
        visit(item);
      }
      return;
    }
    for (const [key, val] of Object.entries(value)) {
      if (Number(key) === targetYear) {
        found = val;
        return;
      }
      visit(val);
      if (found) return;
    }
  };

  visit(entry);
  return found;
}

function getPopulationAgeGroups(entry, year) {
  const ageMap = getPopulationAgeGroupMap(entry);
  if (ageMap) {
    return Object.keys(ageMap)
      .filter(key => !populationTotalKeyRegex.test(key));
  }
  const yearNode = year ? findPopulationYearNode(entry, year) : null;
  const found = findPopulationAgeArray(yearNode || entry);
  if (!found?.items?.length) return [];
  const groups = found.items
    .map(item => item?.[found.ageKey])
    .filter(label => label != null)
    .map(label => String(label));
  return Array.from(new Set(groups));
}

function aggregatePopulationTotals(entry, year, ageGroup) {
  if (!entry) return null;
  const ageMap = getPopulationAgeGroupMap(entry);
  if (ageMap) {
    if (!year) return null;
    const normalizedAge = ageGroup && ageGroup !== 'All' ? String(ageGroup) : null;
    const totalKey = Object.keys(ageMap).find(key => populationTotalKeyRegex.test(key));
    if (!normalizedAge) {
      if (totalKey) {
        const totals = extractPopulationYearTotals(ageMap[totalKey]?.[year]);
        if (!totals) return null;
        return { male: totals.male, female: totals.female, total: totals.total };
      }
      let male = 0;
      let female = 0;
      let total = 0;
      let hasAny = false;
      Object.entries(ageMap).forEach(([key, bucket]) => {
        if (populationTotalKeyRegex.test(key)) return;
        const totals = extractPopulationYearTotals(bucket?.[year]);
        if (!totals) return;
        hasAny = true;
        total += totals.total;
        if (totals.hasMale) male += totals.male;
        if (totals.hasFemale) female += totals.female;
      });
      if (!hasAny) return null;
      return { male, female, total };
    }

    const bucket = ageMap[normalizedAge];
    if (!bucket) return null;
    const totals = extractPopulationYearTotals(bucket?.[year]);
    if (!totals) return null;
    return { male: totals.male, female: totals.female, total: totals.total };
  }

  const yearNode = year ? findPopulationYearNode(entry, year) : entry;
  if (!yearNode) return null;
  const found = findPopulationAgeArray(yearNode);
  if (!found?.items?.length) return null;
  const { items, ageKey } = found;

  if (ageGroup && ageGroup !== 'All') {
    const match = items.find(item => String(item?.[ageKey]) === String(ageGroup));
    if (!match) return null;
    const totals = extractPopulationRowTotals(match, ageKey);
    if (!totals) return null;
    return { male: totals.male, female: totals.female, total: totals.total };
  }

  let male = 0;
  let female = 0;
  let total = 0;
  let hasAny = false;

  items.forEach(item => {
    const totals = extractPopulationRowTotals(item, ageKey);
    if (!totals) return;
    hasAny = true;
    total += totals.total;
    if (totals.hasMale) male += totals.male;
    if (totals.hasFemale) female += totals.female;
  });

  if (!hasAny) return null;
  return { male, female, total };
}

function buildPopulationSeries(entry, year) {
  if (!entry) return null;
  const ageMap = getPopulationAgeGroupMap(entry);
  if (ageMap && year) {
    const labels = [];
    const totalSeries = [];
    const maleSeries = [];
    const femaleSeries = [];
    Object.entries(ageMap).forEach(([label, bucket]) => {
      if (populationTotalKeyRegex.test(label)) return;
      const totals = extractPopulationYearTotals(bucket?.[year]);
      if (!totals) return;
      labels.push(String(label));
      totalSeries.push({ label: String(label), value: totals.total });
      if (totals.hasMale) maleSeries.push({ label: String(label), value: totals.male });
      if (totals.hasFemale) femaleSeries.push({ label: String(label), value: totals.female });
    });
    if (!labels.length) return null;
    const series = [{ name: 'Total', color: '#1f77b4', values: totalSeries }];
    if (maleSeries.length) series.push({ name: 'Male', color: '#2ca02c', values: maleSeries });
    if (femaleSeries.length) series.push({ name: 'Female', color: '#d62728', values: femaleSeries });
    return { labels, series };
  }
  const found = findPopulationAgeArray(entry);
  if (!found) return null;
  const { items, ageKey } = found;
  if (!items.length) return null;

  const labels = [];
  const totalSeries = [];
  const maleSeries = [];
  const femaleSeries = [];

  items.forEach(item => {
    const label = item?.[ageKey];
    if (label == null) return;
    const totals = extractPopulationRowTotals(item, ageKey);
    if (!totals) return;
    labels.push(String(label));
    totalSeries.push({ label: String(label), value: totals.total });
    if (totals.hasMale) maleSeries.push({ label: String(label), value: totals.male });
    if (totals.hasFemale) femaleSeries.push({ label: String(label), value: totals.female });
  });

  if (!labels.length) return null;

  const series = [
    { name: 'Total', color: '#1f77b4', values: totalSeries }
  ];
  if (maleSeries.length) series.push({ name: 'Male', color: '#2ca02c', values: maleSeries });
  if (femaleSeries.length) series.push({ name: 'Female', color: '#d62728', values: femaleSeries });

  return { labels, series };
}

function renderPopulationLineChart(container, dataset) {
  if (!container) return;
  if (typeof d3 === 'undefined') {
    container.innerHTML = '<div class="small text-muted">Line chart requires d3.js.</div>';
    return;
  }

  container.innerHTML = '';
  const { labels, series } = dataset || {};
  if (!labels || !series || !series.length) {
    container.innerHTML = '<div class="small text-muted">No age breakdown available.</div>';
    return;
  }

  const legend = document.createElement('div');
  legend.className = 'small text-muted mb-1 d-flex flex-wrap gap-2';
  series.forEach(s => {
    const item = document.createElement('span');
    item.className = 'd-inline-flex align-items-center gap-1';
    const swatch = document.createElement('span');
    swatch.style.display = 'inline-block';
    swatch.style.width = '10px';
    swatch.style.height = '10px';
    swatch.style.borderRadius = '2px';
    swatch.style.backgroundColor = s.color;
    item.appendChild(swatch);
    item.appendChild(document.createTextNode(s.name));
    legend.appendChild(item);
  });
  container.appendChild(legend);

  const width = container.clientWidth || 300;
  const height = container.clientHeight || 200;
  const margin = { top: 8, right: 10, bottom: 36, left: 36 };

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scalePoint()
    .domain(labels)
    .range([margin.left, width - margin.right])
    .padding(0.4);

  const maxY = d3.max(series.flatMap(s => s.values.map(v => v.value))) || 0;
  const y = d3.scaleLinear()
    .domain([0, maxY * 1.1 || 1])
    .range([height - margin.bottom, margin.top]);

  const xAxis = d3.axisBottom(x).tickSizeOuter(0);
  const yAxis = d3.axisLeft(y).ticks(4);

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => g.selectAll('text').attr('font-size', 9).attr('transform', 'rotate(-30)').attr('text-anchor', 'end'));

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(yAxis)
    .call(g => g.selectAll('text').attr('font-size', 9));

  const line = d3.line()
    .x(d => x(d.label))
    .y(d => y(d.value));

  series.forEach(s => {
    svg.append('path')
      .datum(s.values)
      .attr('fill', 'none')
      .attr('stroke', s.color)
      .attr('stroke-width', 1.6)
      .attr('d', line);
  });
}

function renderPopulationBarChart(container, data) {
  if (!container) return;
  if (typeof d3 === 'undefined') {
    container.innerHTML = '<div class="small text-muted">Bar chart requires d3.js.</div>';
    return;
  }
  container.innerHTML = '';
  const colorByLabel = {
    Male: '#2ca02c',
    Female: '#d62728',
    Total: '#1f77b4'
  };

  const values = data
    ? [
      { label: 'Male', value: data.male },
      { label: 'Female', value: data.female },
      { label: 'Total', value: data.total }
    ]
    : null;
  const validValues = values?.filter(item => Number.isFinite(item.value)) || [];
  if (!validValues.length) {
    container.innerHTML = '<div class="small text-muted">No data available for selection.</div>';
    return;
  }

  const width = container.clientWidth || 300;
  const height = container.clientHeight || 180;
  const margin = { top: 8, right: 10, bottom: 30, left: 40 };

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleBand()
    .domain(values.map(item => item.label))
    .range([margin.left, width - margin.right])
    .padding(0.3);

  const maxY = d3.max(validValues, item => item.value) || 0;
  const y = d3.scaleLinear()
    .domain([0, maxY * 1.1 || 1])
    .range([height - margin.bottom, margin.top]);

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x))
    .call(g => g.selectAll('text').attr('font-size', 9));

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(4))
    .call(g => g.selectAll('text').attr('font-size', 9));

  svg.selectAll('rect')
    .data(values)
    .enter()
    .append('rect')
    .attr('x', item => x(item.label))
    .attr('width', x.bandwidth())
    .attr('y', item => y(Number.isFinite(item.value) ? item.value : 0))
    .attr('height', item => (height - margin.bottom) - y(Number.isFinite(item.value) ? item.value : 0))
    .attr('fill', item => colorByLabel[item.label] || '#6f42c1');
}

function showDistrictPopulationPanel(context) {
  const panel = document.getElementById('districtPopulationPanel');
  if (!panel) return;
  panel.classList.remove('d-none');

  const yearSelect = document.getElementById('districtPopulationYearSelect');
  const ageSelect = document.getElementById('districtPopulationAgeSelect');
  const barChartContainer = document.getElementById('districtPopulationBarChart');

  const setSelectOptions = (selectEl, options, placeholder) => {
    if (!selectEl) return;
    selectEl.innerHTML = '';
    if (!options.length) {
      const option = document.createElement('option');
      option.value = '';
      option.textContent = placeholder;
      selectEl.appendChild(option);
      selectEl.disabled = true;
      return;
    }
    selectEl.disabled = false;
    options.forEach(opt => {
      const option = document.createElement('option');
      option.value = opt.value;
      option.textContent = opt.label;
      selectEl.appendChild(option);
    });
  };

  if (!context) {
    setDistrictPopulationPanelText('districtPopulationName', '—');
    setDistrictPopulationPanelText('districtPopulationRegso', '—');
    setDistrictPopulationPanelNote('Double-click a district to load population details.');
    renderPopulationLineChart(document.getElementById('districtPopulationChart'), null);
    renderPopulationBarChart(barChartContainer, null);
    setSelectOptions(yearSelect, [], 'No years available');
    setSelectOptions(ageSelect, [], 'No age groups');
    return;
  }

  setDistrictPopulationPanelText('districtPopulationName', context.name || 'District');
  setDistrictPopulationPanelText('districtPopulationRegso', context.regsoCode || '—');
  setDistrictPopulationPanelNote('');

  const chartContainer = document.getElementById('districtPopulationChart');

  const years = extractAvailablePopulationYears(context.entry);
  if (!years.length) {
    renderPopulationLineChart(chartContainer, null);
    renderPopulationBarChart(barChartContainer, null);
    setSelectOptions(yearSelect, [], 'No years available');
    setSelectOptions(ageSelect, [], 'No age groups');
    setDistrictPopulationPanelNote('No population series found for this district.');
    return;
  }
  const defaultYear = years.includes(2023) ? 2023 : years[years.length - 1];
  const yearOptions = years.map(year => ({ value: String(year), label: String(year) }));
  setSelectOptions(yearSelect, yearOptions, 'No years available');
  if (defaultYear && yearSelect) yearSelect.value = String(defaultYear);

  const refreshAgeOptions = (selectedYear, selectedAge) => {
    const groups = getPopulationAgeGroups(context.entry, selectedYear);
    const options = [
      { value: 'All', label: 'All' },
      ...groups.map(group => ({ value: group, label: group }))
    ];
    setSelectOptions(ageSelect, options, 'No age groups');
    if (selectedAge && options.some(option => option.value === selectedAge)) {
      ageSelect.value = selectedAge;
    } else if (ageSelect) {
      ageSelect.value = 'All';
    }
  };

  refreshAgeOptions(defaultYear, 'All');

  const updateLineChart = selectedYear => {
    const dataset = buildPopulationSeries(context.entry, selectedYear);
    const legend = dataset?.series?.length ? dataset.series.map(s => s.name).join(' · ') : '';
    const lineNote = dataset
      ? (legend ? `Line chart: Year ${selectedYear} · ${legend}.` : `Line chart: Year ${selectedYear}.`)
      : `Line chart: No age series found for ${selectedYear || 'selected year'}.`;
    if (!dataset) {
      renderPopulationLineChart(chartContainer, null);
    } else {
      renderPopulationLineChart(chartContainer, dataset);
    }
    return lineNote;
  };
  const updateBarChart = () => {
    const selectedYear = yearSelect?.value ? Number(yearSelect.value) : null;
    const selectedAge = ageSelect?.value || 'All';
    const totals = selectedYear ? aggregatePopulationTotals(context.entry, selectedYear, selectedAge) : null;
    renderPopulationBarChart(barChartContainer, totals);
    const lineNote = updateLineChart(selectedYear);
    const filterNote = totals
      ? `Bar chart: Year ${selectedYear} · Age group: ${selectedAge}.`
      : `No data available for selection (Year ${selectedYear || '—'} · Age group: ${selectedAge}).`;
    setDistrictPopulationPanelNote([lineNote, filterNote].filter(Boolean).join(' '));
  };

  if (yearSelect) {
    yearSelect.onchange = () => {
      const selectedYear = yearSelect.value ? Number(yearSelect.value) : null;
      const currentAge = ageSelect?.value || 'All';
      refreshAgeOptions(selectedYear, currentAge);
      updateBarChart();
    };
  }
  if (ageSelect) {
    ageSelect.onchange = () => updateBarChart();
  }

  updateBarChart();
}

function hideDistrictPopulationPanel() {
  const panel = document.getElementById('districtPopulationPanel');
  if (!panel) return;
  panel.classList.add('d-none');
}

function toggleDistrictPopulationPanel() {
  const panel = document.getElementById('districtPopulationPanel');
  if (!panel) return;
  if (!panel.classList.contains('d-none')) {
    hideDistrictPopulationPanel();
    return;
  }
  showDistrictPopulationPanel(lastPopulationContext);
}

function updateParallelCoordsOffset() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;

  const drOffcanvas = document.getElementById('drOffcanvas');
  const isDROpen = drOffcanvas?.classList.contains('show');
  const drOffset = isDROpen ? drOffcanvas.getBoundingClientRect().width : 0;

  const whatIfMenu = document.querySelector('#whatIfDropdownBtn + .dropdown-menu');
  const isWhatIfOpen = whatIfMenu?.classList.contains('show');
  const whatIfOffset = isWhatIfOpen ? whatIfMenu.getBoundingClientRect().width : 0;

  panel.style.left = `${12 + drOffset}px`;
  panel.style.right = `${12 + whatIfOffset}px`;

  if (parallelCoordsOpen && !panel.classList.contains('d-none')) {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (parallelCoordsOpen && !panel.classList.contains('d-none')) {
          updateParallelCoordsPanel();
        }
      });
    });
  }
}

function showParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;
  panel.classList.remove('d-none');
  parallelCoordsOpen = true;
  updateParallelCoordsOffset();
  updateParallelCoordsPanel();
}

function hideParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;
  panel.classList.add('d-none');
  parallelCoordsOpen = false;
}

function toggleParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;
  if (!panel.classList.contains('d-none')) {
    hideParallelCoordsPanel();
    return;
  }
  showParallelCoordsPanel();
}

function normalizeDistrictName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s-]/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

async function showDistrictStatsByName(name) {
  const target = normalizeDistrictName(name);
  if (!target) throw new Error('Provide a district name.');
  await ensureDistrictData();
  await refreshDistrictScores();
  setDistrictView(true);

  const match = (districtFC?.features || []).find((feat, idx) => {
    const props = feat?.properties || {};
    const candidate = normalizeDistrictName(props.__districtName || districtNameOf(props, idx));
    if (!candidate) return false;
    return candidate === target || candidate.includes(target) || target.includes(candidate);
  });
  if (!match) {
    throw new Error(`District "${name}" not found.`);
  }

  const props = match.properties || {};
  const displayName = props.__districtName || districtNameOf(props) || name;
  const score = Number.isFinite(props.__score) ? props.__score : null;
  const count = Number.isFinite(props.__count) ? props.__count : 0;
  const center = turf.center(match).geometry.coordinates;
  let income = null;
  try {
    if (typeof ensureDistrictIncomeData === 'function') {
      await ensureDistrictIncomeData();
      income = lookupDistrictIncome(regsoCodeFromProps(props));
    }
  } catch (_) {}
  showDistrictPopup(center, { name: displayName, score, count, props, income });

  let entry = null;
  let rows = [];
  const regsoCode = regsoCodeFromProps(props);
  try {
    const data = await ensureGenderAgePopulationData();
    entry = lookupPopulationEntry(data, regsoCode);
    rows = flattenPopulationRows(entry);
  } catch (err) {
    rows = [{ label: 'Population data', value: 'Unavailable' }];
  }
  lastPopulationContext = { name: displayName, regsoCode, entry, rows };
  showDistrictPopulationPanel(lastPopulationContext);
}

async function handleDistrictDoubleClick(e) {
  if (!districtView || !overlay || !map) return;
  const point = e?.point;
  if (!point) return;
  const pickArgs = { x: point.x, y: point.y, layerIds: ['district-polygons'] };
  const picked = overlay?.pickObject?.(pickArgs) || overlay?.deck?.pickObject?.(pickArgs);
  const feat = picked?.object;
  if (!feat) return;
  const props = feat.properties || {};
  const name = props.__districtName || districtNameOf(props) || 'District';
  const regsoCode = regsoCodeFromProps(props);
  let entry = null;
  let rows = [];
  try {
    const data = await ensureGenderAgePopulationData();
    entry = lookupPopulationEntry(data, regsoCode);
    rows = flattenPopulationRows(entry);
  } catch (err) {
    rows = [{ label: 'Population data', value: 'Unavailable' }];
  }
  const lngLat = e.lngLat || picked.coordinate || null;
  lastPopulationContext = { name, regsoCode, entry, rows };
  if (document.getElementById('districtPopulationPanel')?.classList.contains('d-none') === false) {
    showDistrictPopulationPanel(lastPopulationContext);
  }
  closeDistrictPopulationPopup();
}

async function handleDistrictClick(info) {
  const feat = info?.object;
  const at = info?.coordinate;
  if (!feat || !at) return;
  const props = feat.properties || {};
  const name = props.__districtName || districtNameOf(props);
  const score = Number.isFinite(props.__score) ? props.__score : null;
  const count = Number.isFinite(props.__count) ? props.__count : 0;
  let income = null;
  try {
    if (typeof ensureDistrictIncomeData === 'function') {
      await ensureDistrictIncomeData();
      income = lookupDistrictIncome(regsoCodeFromProps(props));
    }
  } catch (_) {}
  showDistrictPopup(at, { name, score, count, props, income });
  applyMapSelection([feat], { append: isAdditiveSelectionEvent(info?.srcEvent) });
}

function districtOverlayScore(props = {}) {
  if (!fairActive) return null;
  // 1. Category-specific aggregated score (from all POIs of that type)
  if (fairActive && fairCategory === 'mix' && Number.isFinite(props.__score)) return props.__score;
  if (fairActive && fairCategory && fairCategory !== 'mix') {
    const byCat = props.__fairByCat || {};
    const catScore = byCat?.[fairCategory];
    if (Number.isFinite(catScore)) return catScore;
    if (Number.isFinite(props.__score)) return props.__score;
  }
  // 2. Overall fairness
  if (Number.isFinite(props.__fairOverall)) return props.__fairOverall;
  // 3. Focused (single-POI proximity) only as last resort
  if (Number.isFinite(props.__fairFocused)) return props.__fairFocused;
  return null;
}


function districtOverlayColor(props = {}) {
  if (transitionAnimActive && transitionHasData) {
    return transitionDistrictColor(props);
  }
  if (props?._drSelected) {
    const c = props?._drColor;
    if (Array.isArray(c) && c.length >= 3) return [c[0], c[1], c[2], 220];
    return [DR_SELECTION_COLOR_DEFAULT[0], DR_SELECTION_COLOR_DEFAULT[1], DR_SELECTION_COLOR_DEFAULT[2], 220];
  }
  if (drHasSelection) {
    const score = districtOverlayScore(props);
    if (Number.isFinite(score)) {
      const [r, g, b] = colorFromScore(score);
      return [r, g, b, 90];
    }
    return [DR_UNSELECTED_COLOR[0], DR_UNSELECTED_COLOR[1], DR_UNSELECTED_COLOR[2], 90];
  }
  // Change highlight
  if (pinnedChangeId != null && !changeCompareBaseline && Array.isArray(props._changeColor)) {
    const c = props._changeColor;
    return [c[0], c[1], c[2], c[3] ?? 160];
  }
  if (pinnedChangeId != null && !changeCompareBaseline) {
    // Match building-level change-map behavior:
    // unchanged features are white, changed features carry red↔blue _changeColor.
    return [255, 255, 255, 160];
  }
  const score = districtOverlayScore(props);
  if (!Number.isFinite(score)) return [80, 80, 80, 25];
  const [r, g, b] = colorFromScore(score);
  return [r, g, b, 110];
}

function createDistrictFairnessLayer() {
  if (!districtFC?.features?.length) return null;
  return new deck.GeoJsonLayer({
    id: 'district-fairness-overlay',
    data: districtFC,
    stroked: false,
    filled: true,
    extruded: false,
    pickable: false,
    getFillColor: f => districtOverlayColor(f?.properties || {}),
    parameters: { depthTest: false },
    opacity: 1,
    updateTriggers: {
      getFillColor: [
        districtScoreTick,
        changeLogTick,
        changeCompareBaseline,
        fairActive,
        fairCategory,
        fairRecolorTick,
        drSelectionTick,
        drHasSelection,
        selectedPOIId,
        selectedPOIFeature?.properties?.__cat || selectedPOIFeature?.properties?.category || '',
        transitionAnimTick,
        transitionAnimActive
      ]
    }
  });
}

function mezoOverlayScore(props = {}) {
  if (!fairActive) return null;
  if (fairActive && fairCategory === 'mix' && Number.isFinite(props.__score)) return props.__score;
  if (fairActive && fairCategory && fairCategory !== 'mix') {
    const byCat = props.__fairByCat || {};
    const catScore = byCat?.[fairCategory];
    if (Number.isFinite(catScore)) return catScore;
    if (Number.isFinite(props.__score)) return props.__score;
  }
  if (Number.isFinite(props.__fairOverall)) return props.__fairOverall;
  if (Number.isFinite(props.__fairFocused)) return props.__fairFocused;
  return null;
}

function mezoOverlayColor(props = {}) {
  if (transitionAnimActive && transitionHasData) {
    return transitionMezoColor(props);
  }
  if (props?._drSelected) {
    const c = props?._drColor;
    if (Array.isArray(c) && c.length >= 3) return [c[0], c[1], c[2], 220];
    return [DR_SELECTION_COLOR_DEFAULT[0], DR_SELECTION_COLOR_DEFAULT[1], DR_SELECTION_COLOR_DEFAULT[2], 220];
  }
  if (drHasSelection) {
    const score = mezoOverlayScore(props);
    if (Number.isFinite(score)) {
      const [r, g, b] = colorFromScore(score);
      return [r, g, b, 95];
    }
    return [DR_UNSELECTED_COLOR[0], DR_UNSELECTED_COLOR[1], DR_UNSELECTED_COLOR[2], 90];
  }
  // Change highlight
  if (pinnedChangeId != null && !changeCompareBaseline && Array.isArray(props._changeColor)) {
    const c = props._changeColor;
    return [c[0], c[1], c[2], c[3] ?? 180];
  }
  if (pinnedChangeId != null && !changeCompareBaseline) {
    // Match building-level change-map behavior:
    // unchanged features are white, changed features carry red↔blue _changeColor.
    return [255, 255, 255, 160];
  }
  const score = mezoOverlayScore(props);
  if (!Number.isFinite(score)) return [80, 80, 80, 25];
  const [r, g, b] = colorFromScore(score);
  return [r, g, b, 130];
}

function buildMezoPopupHTML({ hex, score, count }) {
  return `
    <div class="small">
      <div><strong>Mezo hex</strong></div>
      <div>Cell: ${escapeHTML(hex || '—')}</div>
      <div>Buildings: ${Number.isFinite(count) ? count : '—'}</div>
      <div>Fairness: ${Number.isFinite(score) ? score.toFixed(2) : '—'}</div>
    </div>
  `;
}

function handleMezoClick(info) {
  const cell = info?.object;
  const at = info?.coordinate;
  if (!cell || !at || !map) return;
  const score = mezoOverlayScore(cell);
  const html = buildMezoPopupHTML({
    hex: cell.hex,
    score,
    count: cell.__count
  });
  closePopup();
  currentPopup = new maplibregl.Popup({ closeButton: true, closeOnClick: true })
    .setLngLat(at)
    .setHTML(html)
    .addTo(map);
  applyMapSelection([cell], { append: isAdditiveSelectionEvent(info?.srcEvent) });
}

function createMezoHexLayer() {
  if (!mezoHexData?.length) return null;
  return new deck.H3HexagonLayer({
    id: 'mezo-hex-layer',
    data: mezoHexData,
    pickable: true,
    extruded: false,
    getHexagon: d => d.hex,
    getFillColor: d => mezoOverlayColor(d || {}),
    getLineColor: [255, 255, 255, 80],
    lineWidthUnits: 'pixels',
    getLineWidth: 1,
    onClick: handleMezoClick,
    parameters: { depthTest: false },
    updateTriggers: {
      getFillColor: [
        mezoScoreTick,
        changeLogTick,
        changeCompareBaseline,
        fairActive,
        fairCategory,
        fairRecolorTick,
        drSelectionTick,
        drHasSelection,
        selectedPOIId,
        selectedPOIFeature?.properties?.__cat || selectedPOIFeature?.properties?.category || '',
        transitionAnimTick,
        transitionAnimActive
      ]
    }
  });
}

function createDistrictPickLayer() {
  if (!districtFC?.features?.length) return null;
  return new deck.GeoJsonLayer({
    id: 'district-polygons',
    data: districtFC,
    stroked: false,
    filled: true,
    extruded: false,
    pickable: true,
    getFillColor: [0, 0, 0, 0],
    parameters: { depthTest: false },
    opacity: 0,
    onClick: handleDistrictClick,
    updateTriggers: { data: [districtScoreTick] }
  });
}


function createDistrictBoundaryLayer() {
  const boundary = ensureDistrictBoundaryLines();
  if (!boundary?.features?.length) return null;
  return new deck.GeoJsonLayer({
    id: 'district-borders',
    data: boundary,
    stroked: true,
    filled: false,
    pickable: false,
    getLineColor: [55, 65, 80, 200],
    getLineWidth: 3,
    lineWidthMinPixels: 2,
    lineWidthUnits: 'pixels',
    getDashArray: [4, 3],
    dashJustified: true,
    extensions: [DISTRICT_DASH_EXT],
    parameters: { depthTest: false },
    updateTriggers: { getLineColor: [districtScoreTick], data: [districtScoreTick] }
  });
}


/* ======================= Layers ======================= */
function createCityLayer(grayBackdrop) {
  if (!baseCityFC) return null;


  const common = { id:'city-buildings', data:baseCityFC, pickable:true, wireframe:false, onClick: handleClick };

  const normalColor = (f) => baseBuildingColorForFeature(f);

  if (grayBackdrop) {
    return new deck.GeoJsonLayer({
      ...common,
      extruded:false,
      stroked:false,
      opacity:0.25,
      getFillColor: f => {
        if (f.properties && f.properties._drSelected) {
          const c = f.properties._drColor;
          if (Array.isArray(c) && c.length >= 3) {
            return [c[0], c[1], c[2]];
          }
          return DR_SELECTION_COLOR_DEFAULT;
        }
        if (selectedBuildingType) {
          return featureMatchesSelectedType(f)
            ? SELECTED_BUILDING_TYPE_COLOR
            : DR_UNSELECTED_COLOR;
        }
        return drHasSelection ? DR_UNSELECTED_COLOR : BACKDROP_COLOR;
      },
      updateTriggers: {
        getFillColor: [drSelectionTick, drHasSelection, buildingTypeTick, selectedBuildingType]
      }
    });
  }

  return new deck.GeoJsonLayer({
    ...common,
    extruded:true,
    stroked:false,
    opacity:0.95,
    material: BUILDING_MATERIAL,
    getElevation: f =>
      clampElev((f.properties?.height_m ?? f.properties?._mean)) * heightScale,

    getFillColor: f => {
      // 1) DR selection highlight overrides everything
      if (f.properties && f.properties._drSelected) {
        const c = f.properties._drColor;
        if (Array.isArray(c) && c.length >= 3) {
          return [c[0], c[1], c[2]];      // color from UMAP (red/yellow/green)
        }
        // fallback if something went wrong
        return DR_SELECTION_COLOR_DEFAULT;
      }

      // 2) If there is an active DR selection, dim all other buildings
      if (drHasSelection && (pinnedChangeId == null || changeCompareBaseline)) {
        return DR_UNSELECTED_COLOR;
      }

      // 3) Building-type highlighting (purple for matches, dim non-matching)
      if (selectedBuildingType) {
        return featureMatchesSelectedType(f)
          ? SELECTED_BUILDING_TYPE_COLOR
          : DR_UNSELECTED_COLOR;
      }

      // 3b) Highlight pulse when user clicks a change entry
      if (f.properties?._changeHighlight) {
        return [255, 255, 255, 255]; // bright white flash
      }

      // 3c) What-if change highlight — colored for changed, white for unaffected
      if (pinnedChangeId != null && !changeCompareBaseline) {
        if (f.properties?._changeColor) {
          const c = f.properties._changeColor;
          return [c[0], c[1], c[2], c[3] ?? 220];
        }
        // No change for this building — white so changed buildings stand out
        return [255, 255, 255, 160];
      }

      // 4) Animated transition replay
      if (transitionAnimActive && transitionHasData) {
        return transitionBuildingColor(f.properties);
      }

      // 5) Fairness coloring if active
      if (fairActive && f.properties?.fair) {
        return colorFromScore(f.properties.fair.score);
      }

      // 6) Default category-based color
      return normalColor(f);
    },

    updateTriggers: {
      getElevation:[heightScale],
      getFillColor:[fairActive, fairCategory, fairRecolorTick, drSelectionTick, drHasSelection, buildingTypeTick, selectedBuildingType, transitionAnimTick, transitionAnimActive, changeLogTick, changeCompareBaseline, pinnedChangeId]
    }
  });
}


function createHighlightLayer(year) {
  if (viewMode !== 'new' || !year) return null;
  const feats = statsForYear(year);
  if (!feats.length) return null;
  const color = YEAR_COLORS[year] || [255,255,255];

  return new deck.GeoJsonLayer({
    id:'newbuilds-highlight', data:feats, pickable:true, extruded:true, wireframe:false, stroked:true, opacity:1,
    material: BUILDING_MATERIAL,
    getElevation: f => clampElev((f.properties?.height_m ?? f.properties?._mean)) * heightScale,
    getFillColor: color, getLineColor:[255,255,255], getLineWidth:2, lineWidthUnits:'pixels',
    onClick: handleClick, updateTriggers:{ getElevation:[heightScale], data:[year] }
  });
}

function createRouteLayer() {
  if (!routeGeoJSON) return null;
  return new deck.GeoJsonLayer({
    id:'walking-route', data: routeGeoJSON, stroked:true, filled:false,
    getLineColor:[0,255,0], getLineWidth:4, lineWidthUnits:'pixels'
  });
}

function updateLayers() {
  if (!overlay) return;
  toggleLocalOnlyUI(sourceMode === 's1');

  const layers = [];
  const grayBackdrop = (viewMode === 'new' && !!selectedYear) || districtView || mezoView;

  const base = createCityLayer(grayBackdrop); if (base) layers.push(base);
  const hi = createHighlightLayer(selectedYear); if (hi) layers.push(hi);
  const mockOutline = createWhatIfMockBuildingsLayer(); if (mockOutline) layers.push(mockOutline);
  if (mezoView) {
    const mezoLayer = createMezoHexLayer();
    if (mezoLayer) layers.push(mezoLayer);
  }
  if (districtView) {
    const distPick = createDistrictPickLayer();
    const distFairness = createDistrictFairnessLayer();
    const districtBorder = createDistrictBoundaryLayer();
    if (distFairness) layers.push(distFairness);
    if (distPick) layers.push(distPick);
    if (districtBorder) layers.push(districtBorder);
  }

  const poiSymbols = createPOIPointLayer(); if (poiSymbols) layers.push(poiSymbols);
  const poiBldg = createPOIBuildingMarkerLayer(); if (poiBldg) layers.push(poiBldg);

  const route = createRouteLayer(); if (route) layers.push(route);

  createWhatIfSuggestionLayers().forEach(l => l && layers.push(l));

  overlay.setProps({ layers });
}

/* ======================= Map lasso selection ======================= */
function ensureMapLassoOverlay() {
  if (typeof d3 === 'undefined') return null;

  // Keep the overlay anchored to the map's canvas container so pointer math
  // matches deck/maplibre even when the sidebar pushes content around.
  const host = (map && map.getCanvasContainer()) || document.getElementById('mapContainer');
  if (!host) return null;

  let svg = d3.select('#mapLassoOverlay');
  if (!svg.node()) {
    svg = d3.select(host)
      .append('svg')
      .attr('id', 'mapLassoOverlay')
      .style('position', 'absolute')
      .style('inset', 0)
      .style('width', '100%')
      .style('height', '100%')
      .style('z-index', 5)
      .style('pointer-events', 'none')
      .style('touch-action', 'none');
  }
  return svg;
}

function setMapLassoButtonState() {
  const btn = document.getElementById('mapLassoBtn');
  if (btn) {
    btn.classList.toggle('btn-light', mapLasso.active);
    btn.classList.toggle('btn-outline', !mapLasso.active);
    btn.textContent = mapLasso.active ? 'Exit lasso' : 'Map lasso';
  }
}

function setMapLassoClearDisabled(disabled) {
  const btn = document.getElementById('mapLassoClearBtn');
  if (btn) btn.disabled = !!disabled;
}

function clearMapLassoGraphics() {
  mapLasso.points = [];
  mapLasso.marqueeStart = null;
  mapLasso.drawing = false;
  mapLasso.marqueeDrawing = false;
  if (mapLasso.path) { mapLasso.path.remove(); mapLasso.path = null; }
  if (mapLasso.marqueeRect) { mapLasso.marqueeRect.remove(); mapLasso.marqueeRect = null; }
}

function setMapLassoActive(active) {
  mapLasso.active = !!active;
  setMapLassoButtonState();
  if (active && whatIfLasso.active) setWhatIfLassoActive(false);

  const svg = ensureMapLassoOverlay();
  if (!svg) return;

  svg.style('pointer-events', active ? 'all' : 'none')
    .style('cursor', active ? 'crosshair' : 'default');

  if (typeof svg.on === 'function') {
    svg.on('mousedown', active ? handleMapLassoStart : null);
    svg.on('mousemove', active ? handleMapLassoMove : null);
    svg.on('mouseup',   active ? handleMapLassoEnd  : null);
    svg.on('mouseleave', active ? handleMapLassoEnd : null);
  }

  if (map && map.dragPan) {
    if (active) map.dragPan.disable();
    else map.dragPan.enable();
  }

  if (!active) clearMapLassoGraphics();
}

function toggleMapLasso() { setMapLassoActive(!mapLasso.active); }

function clearMapSelection() {
  clearParallelCoordsSelectionFromClearAction();
  clearDRMapSelection({ preservePersistent: true });
  applyDRSelection([], { skipMapSync: true });
  updateLayers();
  setMapLassoClearDisabled(true);
}

function isAdditiveSelectionEvent(event) {
  if (event && (event.ctrlKey || event.metaKey)) return true;
  return additiveSelectionKeyActive;
}

const buildingCentroidCache = new WeakMap();
function buildingCentroid(feature) {
  if (!feature) return null;
  if (buildingCentroidCache.has(feature)) return buildingCentroidCache.get(feature);
  let coords = null;
  try {
    coords = turf.centroid(feature)?.geometry?.coordinates || null;
  } catch (_) {
    coords = null;
  }
  buildingCentroidCache.set(feature, coords);
  return coords;
}

function mezoHexForBuilding(feature) {
  const coords = buildingCentroid(feature);
  if (!Array.isArray(coords)) return null;
  const res = resolveMezoResolution();
  if (res == null) return null;
  try {
    return h3LatLngToCell(window.h3, coords[1], coords[0], res);
  } catch (_) {
    return null;
  }
}

function districtForBuilding(feature) {
  if (!feature || !districtFC?.features?.length) return null;
  const pointCoords = buildingCentroid(feature);
  if (!Array.isArray(pointCoords)) return null;
  const pt = turf.point(pointCoords);
  for (const district of districtFC.features) {
    try {
      if (turf.booleanPointInPolygon(pt, district)) return district;
    } catch (_) { /* ignore geometry issues */ }
  }
  return null;
}

function selectedBuildingsFromEntities(entities = []) {
  const selected = [];
  const seen = new Set();
  const addBuilding = (building) => {
    if (!building || seen.has(building)) return;
    seen.add(building);
    selected.push(building);
  };

  const allBuildings = baseCityFC?.features || [];
  entities.forEach((entity) => {
    if (!entity) return;
    if (allBuildings.includes(entity)) {
      addBuilding(entity);
      return;
    }

    const maybeHex = entity?.hex || entity?.properties?.hex;
    if (maybeHex) {
      allBuildings.forEach((building) => {
        if (mezoHexForBuilding(building) === maybeHex) addBuilding(building);
      });
      return;
    }

    const hasGeometry = !!entity?.geometry;
    if (!hasGeometry) return;
    allBuildings.forEach((building) => {
      const c = buildingCentroid(building);
      if (!Array.isArray(c)) return;
      try {
        if (turf.booleanPointInPolygon(turf.point(c), entity)) addBuilding(building);
      } catch (_) {
        /* ignore */
      }
    });
  });

  return selected;
}

function setPersistentBuildingSelection(buildings = []) {
  const next = new Set();
  (Array.isArray(buildings) ? buildings : []).forEach((building) => {
    if (building) next.add(building);
  });
  persistentBuildingSelection = next;
}

function markAggregateSelectionsFromBuildings(selectedBuildings) {
  const selectedSet = new Set(Array.isArray(selectedBuildings) ? selectedBuildings : []);
  const selectedDistricts = new Set();
  selectedSet.forEach((building) => {
    const district = districtForBuilding(building);
    if (district) selectedDistricts.add(district);
  });

  (districtFC?.features || []).forEach((district) => {
    if (selectedDistricts.has(district)) {
      if (!district.properties) district.properties = {};
      district.properties._drSelected = true;
      district.properties._drColor = [...DR_SELECTION_COLOR_DEFAULT];
    }
  });

  const selectedHexes = new Set();
  selectedSet.forEach((building) => {
    const cell = mezoHexForBuilding(building);
    if (cell) selectedHexes.add(cell);
  });

  (mezoHexData || []).forEach((cell) => {
    if (!selectedHexes.has(cell?.hex)) return;
    cell._drSelected = true;
    cell._drColor = [...DR_SELECTION_COLOR_DEFAULT];
  });
}

function entitiesForSpatialModeFromBuildings(buildings, mode = currentDRDataMode()) {
  const selectedBuildings = Array.isArray(buildings) ? buildings.filter(Boolean) : [];
  if (!selectedBuildings.length) return [];

  if (mode === 'building') return selectedBuildings;

  if (mode === 'district') {
    const selectedDistricts = new Set();
    selectedBuildings.forEach((building) => {
      const district = districtForBuilding(building);
      if (district) selectedDistricts.add(district);
    });
    return Array.from(selectedDistricts);
  }

  if (mode === 'mezo') {
    const hexToCell = new Map((mezoHexData || []).map((cell) => [cell?.hex, cell]).filter(([hex]) => !!hex));
    const selectedHexes = new Set();
    selectedBuildings.forEach((building) => {
      const cell = mezoHexForBuilding(building);
      if (cell) selectedHexes.add(cell);
    });
    return Array.from(selectedHexes)
      .map((hex) => hexToCell.get(hex))
      .filter(Boolean);
  }

  return selectedBuildings;
}

function syncDRSelectionFromBuildings(buildings, opts = {}) {
  const { append = false, preserveMapSelection = true } = opts;
  const selectedBuildings = Array.isArray(buildings) ? buildings.filter(Boolean) : [];
  if (!drPlot.points || !Array.isArray(drPlot.sample)) return;

  const mode = drPlot.mode || currentDRDataMode();
  const entities = entitiesForSpatialModeFromBuildings(selectedBuildings, mode);
  const idx = [];
  entities.forEach((entity) => {
    const i = drPlot.sample.indexOf(entity);
    if (i !== -1) idx.push(i);
  });

  if (!drPlot.screenXY && drPlot.points) {
    drPlot.screenXY = computeScreenPositions(drPlot.points);
  }

  prepareDRSurface();
  initD3Overlay();
  applyDRSelection(idx, { skipMapSync: preserveMapSelection, append });
}

function applyMapSelection(selected, opts = {}) {
  const { append = false, skipDRSync = false, skipParallelSync = false } = opts;
  parallelCoordsForceEmptySelection = false;
  const existingSelected = append
    ? (baseCityFC?.features || []).filter(f => f?.properties?._drSelected)
    : [];
  const nextSelected = selectedBuildingsFromEntities(existingSelected);

  if (Array.isArray(selected)) {
    selectedBuildingsFromEntities(selected).forEach((feat) => {
      if (feat && !nextSelected.includes(feat)) nextSelected.push(feat);
    });
  }

  setPersistentBuildingSelection(nextSelected);

  clearDRMapSelection({ preservePersistent: true });

  if (nextSelected.length) {
    nextSelected.forEach((feat) => {
      if (!feat.properties) feat.properties = {};
      feat.properties._drSelected = true;
      feat.properties._drColor = [...DR_SELECTION_COLOR_DEFAULT];
    });
  }

  markAggregateSelectionsFromBuildings(nextSelected);


  drSelectionTick++;
  updateLayers();
  setMapLassoClearDisabled(nextSelected.length === 0);

 if (!skipDRSync) {
    syncDRSelectionFromBuildings(nextSelected, { preserveMapSelection: true, append });
  }

  if (parallelCoordsOpen && !skipParallelSync) {
    updateParallelCoordsPanel();
  }

  const { statusEl } = ensureDRUI();
}

function applyMapLassoSelection(polygon, opts = {}) {
  if (!Array.isArray(polygon) || polygon.length < 3) return;
  if (!map || !baseCityFC?.features?.length) return;

  const selected = [];
  for (const f of baseCityFC.features) {
    const c = turf.centroid(f).geometry.coordinates;
    const pt = map.project({ lng: c[0], lat: c[1] });
    if (!pt || !Number.isFinite(pt.x) || !Number.isFinite(pt.y)) continue;
    if (d3.polygonContains(polygon, [pt.x, pt.y])) selected.push(f);
  }

  applyMapSelection(selected, opts);
}

function handleMapLassoStart(event) {
  if (!mapLasso.active || (event.button != null && event.button !== 0)) return;
  const svg = ensureMapLassoOverlay();
  if (!svg) return;

  event.preventDefault(); event.stopPropagation();
  clearMapLassoGraphics();

  const [x, y] = d3.pointer(event, svg.node());
  if (event.shiftKey) {
    mapLasso.marqueeDrawing = true;
    mapLasso.drawing = false;
    mapLasso.marqueeStart = [x, y];
    mapLasso.marqueeRect = svg.append('rect')
      .attr('x', x).attr('y', y)
      .attr('width', 0).attr('height', 0)
      .attr('fill', 'rgba(13,110,253,0.14)')
      .attr('stroke', '#0d6efd')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4 2');
  } else {
    mapLasso.drawing = true;
    mapLasso.points = [[x, y]];
    mapLasso.path = svg.append('path')
      .attr('fill', 'rgba(13,110,253,0.14)')
      .attr('stroke', '#0d6efd')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4 2')
      .attr('d', d3.line()(mapLasso.points));
  }
}

function handleMapLassoMove(event) {
  if (!mapLasso.active) return;
  const svg = ensureMapLassoOverlay();
  if (!svg) return;
  const [x, y] = d3.pointer(event, svg.node());

  if (mapLasso.drawing && mapLasso.path) {
    mapLasso.points.push([x, y]);
    mapLasso.path.attr('d', d3.line()(mapLasso.points));
  } else if (mapLasso.marqueeDrawing && mapLasso.marqueeRect && mapLasso.marqueeStart) {
    const [x0, y0] = mapLasso.marqueeStart;
    const w = x - x0;
    const h = y - y0;
    mapLasso.marqueeRect
      .attr('x', Math.min(x0, x))
      .attr('y', Math.min(y0, y))
      .attr('width', Math.abs(w))
      .attr('height', Math.abs(h));
  }
}

function handleMapLassoEnd(event) {
  if (!mapLasso.drawing && !mapLasso.marqueeDrawing) return;
  const svg = ensureMapLassoOverlay();
  if (!svg) return;
  event.preventDefault(); event.stopPropagation();

  if (mapLasso.drawing) {
    mapLasso.drawing = false;
    if (mapLasso.points.length < 3) { clearMapLassoGraphics(); return; }
    applyMapLassoSelection(mapLasso.points.slice(), { append: isAdditiveSelectionEvent(event) });
  }

  if (mapLasso.marqueeDrawing) {
    mapLasso.marqueeDrawing = false;
    if (mapLasso.marqueeRect && mapLasso.marqueeStart) {
      const x = parseFloat(mapLasso.marqueeRect.attr('x')) || 0;
      const y = parseFloat(mapLasso.marqueeRect.attr('y')) || 0;
      const w = parseFloat(mapLasso.marqueeRect.attr('width')) || 0;
      const h = parseFloat(mapLasso.marqueeRect.attr('height')) || 0;
      if (w > 2 && h > 2) {
        const poly = [
          [x, y], [x + w, y], [x + w, y + h], [x, y + h]
        ];
        applyMapLassoSelection(poly, { append: isAdditiveSelectionEvent(event) });
      }
    }
  }

  clearMapLassoGraphics();
}

/* ======================= What-if mock buildings lasso ======================= */
function ensureWhatIfLassoOverlay() {
  if (typeof d3 === 'undefined') return null;
  const host = (map && map.getCanvasContainer()) || document.getElementById('mapContainer');
  if (!host) return null;

  let svg = d3.select('#whatIfLassoOverlay');
  if (!svg.node()) {
    svg = d3.select(host)
      .append('svg')
      .attr('id', 'whatIfLassoOverlay')
      .style('position', 'absolute')
      .style('inset', 0)
      .style('width', '100%')
      .style('height', '100%')
      .style('z-index', 6)
      .style('pointer-events', 'none')
      .style('touch-action', 'none');
  }
  return svg;
}

function buildWhatIfLassoRingFromScreen(polygon) {
  if (!map || !Array.isArray(polygon)) return null;
  const ring = polygon.map(([x, y]) => {
    const ll = map.unproject([x, y]);
    return ll ? [ll.lng, ll.lat] : null;
  }).filter(Boolean);
  if (ring.length < 3) return null;
  const first = ring[0];
  const last = ring[ring.length - 1];
  if (!last || first[0] !== last[0] || first[1] !== last[1]) {
    ring.push([first[0], first[1]]);
  }
  return ring;
}

function buildWhatIfLassoScreenPolygon(ring) {
  if (!map || !Array.isArray(ring)) return null;
  const openRing = ring.slice(0, -1);
  const poly = openRing.map(([lng, lat]) => {
    const pt = map.project({ lng, lat });
    return pt ? [pt.x, pt.y] : null;
  }).filter(Boolean);
  return poly.length >= 3 ? poly : null;
}

function setWhatIfLassoSelection(ring, { status } = {}) {
  whatIfLasso.selectionRing = Array.isArray(ring) ? ring : null;
  if (typeof status === 'string') {
    setWhatIfLassoStatus(status);
  }
}

function setWhatIfLassoButtonState() {
  if (!whatIfLassoBtn) return;
  whatIfLassoBtn.classList.toggle('btn-warning', whatIfLasso.active);
  whatIfLassoBtn.classList.toggle('btn-outline-warning', !whatIfLasso.active);
  whatIfLassoBtn.textContent = whatIfLasso.active ? 'Exit lasso' : 'Draw lasso';
}

function setWhatIfLassoClearDisabled(disabled) {
  if (whatIfLassoClearBtn) whatIfLassoClearBtn.disabled = !!disabled;
}

function clearWhatIfLassoGraphics() {
  whatIfLasso.points = [];
  whatIfLasso.marqueeStart = null;
  whatIfLasso.drawing = false;
  whatIfLasso.marqueeDrawing = false;
  if (whatIfLasso.path) { whatIfLasso.path.remove(); whatIfLasso.path = null; }
  if (whatIfLasso.marqueeRect) { whatIfLasso.marqueeRect.remove(); whatIfLasso.marqueeRect = null; }
}

function setWhatIfLassoActive(active) {
  whatIfLasso.active = !!active;
  setWhatIfLassoButtonState();
  if (active && mapLasso.active) setMapLassoActive(false);

  const svg = ensureWhatIfLassoOverlay();
  if (!svg) return;

  svg.style('pointer-events', active ? 'all' : 'none')
    .style('cursor', active ? 'crosshair' : 'default');

  if (typeof svg.on === 'function') {
    svg.on('mousedown', active ? handleWhatIfLassoStart : null);
    svg.on('mousemove', active ? handleWhatIfLassoMove : null);
    svg.on('mouseup', active ? handleWhatIfLassoEnd : null);
    svg.on('mouseleave', active ? handleWhatIfLassoEnd : null);
  }

  if (map && map.dragPan) {
    if (active) map.dragPan.disable();
    else map.dragPan.enable();
  }

  if (!active) clearWhatIfLassoGraphics();
}

function toggleWhatIfLasso() { setWhatIfLassoActive(!whatIfLasso.active); }

async function applyWhatIfLassoFromRing(lassoRing, { countsOverride = null } = {}) {
  if (!Array.isArray(lassoRing) || lassoRing.length < 3) return;
  if (!map) return;
  const countsPayload = countsOverride ? normalizeWhatIfMockCounts(countsOverride) : getWhatIfMockTypeCounts();
  const totalCount = countsPayload.total;
  if (!totalCount) {
    setWhatIfLassoSelection(
      lassoRing,
      { status: 'Selection saved. Enter building counts or ask the LLM to place them.' }
    );
    return;
  }

  setWhatIfLassoSelection(lassoRing);
  const lassoPoly = turf.polygon([lassoRing]);
  const screenPolygon = buildWhatIfLassoScreenPolygon(lassoRing);
  if (!screenPolygon) return;

  const bboxIntersects = (a, b) => (
    a[0] <= b[2] && a[2] >= b[0] &&
    a[1] <= b[3] && a[3] >= b[1]
  );

  const existingIndex = (baseCityFC?.features || [])
    .filter((feat) => {
      try {
        const c = turf.centroid(feat);
        return turf.booleanPointInPolygon(c, lassoPoly);
      } catch {
        return false;
      }
    })
    .map((feat) => {
      let bbox = null;
      try { bbox = turf.bbox(feat); } catch {}
      return { feat, bbox };
    })
    .filter((entry) => Array.isArray(entry.bbox));

  const bounds = screenPolygon.reduce((acc, [x, y]) => ({
    minX: Math.min(acc.minX, x),
    minY: Math.min(acc.minY, y),
    maxX: Math.max(acc.maxX, x),
    maxY: Math.max(acc.maxY, y)
  }), { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity });

  if (!Number.isFinite(bounds.minX) || !Number.isFinite(bounds.minY)) return;

  const buildPlacementQueue = () => {
    if (countsPayload.counts.length) {
      const queue = [];
      countsPayload.counts.forEach(({ type, count }) => {
        for (let i = 0; i < count; i += 1) queue.push(type);
      });
      return queue;
    }
    return Array.from({ length: totalCount }, () => 'generic');
  };

  const tryGenerate = ({ avoidRendered }) => {
    const avoidLayerIds = avoidRendered ? getWhatIfAvoidLayerIds() : [];
    const queue = buildPlacementQueue();
    const features = [];
    const mockIndex = [];
    const zoom = map.getZoom();
    const avoidPx = Math.max(2, Math.round(WHATIF_AVOID_SAMPLE_PX * Math.pow(2, zoom - 15)));
    const maxAttempts = Math.min(totalCount * 30, 200000);
    let attempts = 0;

    while (features.length < totalCount && attempts < maxAttempts) {
      attempts += 1;
      const x = bounds.minX + Math.random() * (bounds.maxX - bounds.minX);
      const y = bounds.minY + Math.random() * (bounds.maxY - bounds.minY);
      if (!d3.polygonContains(screenPolygon, [x, y])) continue;
      if (avoidLayerIds.length) {
        const hit = map.queryRenderedFeatures(
          [[x - avoidPx, y - avoidPx], [x + avoidPx, y + avoidPx]],
          { layers: avoidLayerIds }
        );
        if (hit && hit.length) continue;
      }
      const ll = map.unproject([x, y]);
      if (!ll) continue;
      const sizeRange = whatIfMockFootprintMax - whatIfMockFootprintMin;
      const size = whatIfMockFootprintMin + Math.random() * Math.max(0, sizeRange);
      const geom = createMockBuildingPolygon([ll.lng, ll.lat], size, 'random', whatIfMockShapeVariation);
      const mockType = queue[features.length] || 'generic';
      const candidate = {
        type: 'Feature',
        geometry: geom,
        properties: {}
      };

      // Check all corners of the building polygon against road/water layers
      if (avoidLayerIds.length) {
        const coords = geom.coordinates?.[0] || [];
        let hitsInfra = false;
        for (let ci = 0; ci < coords.length - 1; ci++) {
          const cp = map.project({ lng: coords[ci][0], lat: coords[ci][1] });
          if (!cp) continue;
          const cornerHit = map.queryRenderedFeatures(
            [[cp.x - avoidPx, cp.y - avoidPx], [cp.x + avoidPx, cp.y + avoidPx]],
            { layers: avoidLayerIds }
          );
          if (cornerHit && cornerHit.length) { hitsInfra = true; break; }
        }
        if (hitsInfra) continue;
      }

      applyMockBuildingTags(
        candidate.properties,
        mockType === 'generic' ? 'residential' : mockType,
        { floors: whatIfMockFloors, floorHeight: whatIfMockFloorHeight, footprint: size }
      );
      let candidateBBox = null;
      try { candidateBBox = turf.bbox(candidate); } catch {}
      if (!candidateBBox) continue;

      const overlapsExisting = existingIndex.some(({ feat, bbox }) => (
        bboxIntersects(candidateBBox, bbox) && turf.booleanIntersects(candidate, feat)
      ));
      if (overlapsExisting) continue;

      const overlapsMock = mockIndex.some(({ feat, bbox }) => (
        bboxIntersects(candidateBBox, bbox) && turf.booleanIntersects(candidate, feat)
      ));
      if (overlapsMock) continue;

      features.push(candidate);
      mockIndex.push({ feat: candidate, bbox: candidateBBox });
    }

    return { features, attempts };
  };

  let result = tryGenerate({ avoidRendered: true });
  if (!result.features.length) {
    result = tryGenerate({ avoidRendered: false });
    if (!result.features.length) {
      setWhatIfLassoStatus('No mock buildings could be placed. Try a larger area or reduce conflicts.', true);
      return;
    }
  }

  if (result.features.length) {
    if (!baseCityFC?.features) baseCityFC = { type: 'FeatureCollection', features: [] };
    baseCityFC.features.push(...result.features);
    if (newbuildsFC?.features) newbuildsFC.features.push(...result.features);
  }
  refreshBuildingTypeDropdown();
  updateLayers();
  setWhatIfLassoClearDisabled(!result.features.length);

  if (result.features.length < totalCount) {
    setWhatIfLassoStatus(`Placed ${result.features.length} mock buildings (requested ${totalCount}). Some placements were blocked by existing buildings or avoided features.`, true);
  } else {
    setWhatIfLassoStatus(`Placed ${result.features.length} mock buildings.`);
  }
  showGlobalSpinner('Placing mock buildings & recomputing…');
  await waitForSpinnerPaint();
  const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  const recomputeResult = await recomputeFairnessAfterWhatIf();
  const afterCatGini = Number.isFinite(recomputeResult?.categoryGini) ? recomputeResult.categoryGini : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(recomputeResult?.overallGini) ? recomputeResult.overallGini : (Number.isFinite(overallGini) ? overallGini : null);
  recordWhatIfChange({
    action: 'mock_buildings',
    description: `Added ${result.features.length} mock building${result.features.length !== 1 ? 's' : ''} via lasso`,
    category: null,
    beforeGini: beforeCatGini,
    afterGini: afterCatGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: result.features
  });
  hideGlobalSpinner();
}

function applyWhatIfLassoSelection(polygon, { countsOverride = null } = {}) {
  if (!Array.isArray(polygon) || polygon.length < 3) return;
  const lassoRing = buildWhatIfLassoRingFromScreen(polygon);
  if (!lassoRing) return;
  applyWhatIfLassoFromRing(lassoRing, { countsOverride });
}

function applyWhatIfLassoFromChat(rawCounts) {
  const selectionRing = whatIfLasso.selectionRing;
  if (!selectionRing) {
    throw new Error('Draw a what-if lasso selection first.');
  }
  const normalized = normalizeWhatIfMockCounts(rawCounts);
  if (!normalized.total) {
    throw new Error('Provide at least one building count to add.');
  }
  setWhatIfMockTypeCounts(normalized.counts);
  applyWhatIfLassoFromRing(selectionRing, { countsOverride: normalized.counts });
}

function getWhatIfLassoBBox() {
  if (!whatIfLasso.selectionRing) return null;
  try {
    const poly = turf.polygon([whatIfLasso.selectionRing]);
    return turf.bbox(poly);
  } catch {
    return null;
  }
}

function handleWhatIfLassoStart(event) {
  if (!whatIfLasso.active || (event.button != null && event.button !== 0)) return;
  const svg = ensureWhatIfLassoOverlay();
  if (!svg) return;

  event.preventDefault(); event.stopPropagation();
  clearWhatIfLassoGraphics();

  const [x, y] = d3.pointer(event, svg.node());
  if (event.shiftKey) {
    whatIfLasso.marqueeDrawing = true;
    whatIfLasso.drawing = false;
    whatIfLasso.marqueeStart = [x, y];
    whatIfLasso.marqueeRect = svg.append('rect')
      .attr('x', x).attr('y', y)
      .attr('width', 0).attr('height', 0)
      .attr('fill', 'rgba(255,193,7,0.14)')
      .attr('stroke', '#ffc107')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4 2');
  } else {
    whatIfLasso.drawing = true;
    whatIfLasso.points = [[x, y]];
    whatIfLasso.path = svg.append('path')
      .attr('fill', 'rgba(255,193,7,0.14)')
      .attr('stroke', '#ffc107')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4 2')
      .attr('d', d3.line()(whatIfLasso.points));
  }
}

function handleWhatIfLassoMove(event) {
  if (!whatIfLasso.active) return;
  const svg = ensureWhatIfLassoOverlay();
  if (!svg) return;
  const [x, y] = d3.pointer(event, svg.node());

  if (whatIfLasso.drawing && whatIfLasso.path) {
    whatIfLasso.points.push([x, y]);
    whatIfLasso.path.attr('d', d3.line()(whatIfLasso.points));
  } else if (whatIfLasso.marqueeDrawing && whatIfLasso.marqueeRect && whatIfLasso.marqueeStart) {
    const [x0, y0] = whatIfLasso.marqueeStart;
    const w = x - x0;
    const h = y - y0;
    whatIfLasso.marqueeRect
      .attr('x', Math.min(x0, x))
      .attr('y', Math.min(y0, y))
      .attr('width', Math.abs(w))
      .attr('height', Math.abs(h));
  }
}

function handleWhatIfLassoEnd(event) {
  if (!whatIfLasso.drawing && !whatIfLasso.marqueeDrawing) return;
  const svg = ensureWhatIfLassoOverlay();
  if (!svg) return;
  event.preventDefault(); event.stopPropagation();

  if (whatIfLasso.drawing) {
    whatIfLasso.drawing = false;
    if (whatIfLasso.points.length < 3) { clearWhatIfLassoGraphics(); return; }
    applyWhatIfLassoSelection(whatIfLasso.points.slice());
  }

  if (whatIfLasso.marqueeDrawing) {
    whatIfLasso.marqueeDrawing = false;
    if (whatIfLasso.marqueeRect && whatIfLasso.marqueeStart) {
      const x = parseFloat(whatIfLasso.marqueeRect.attr('x')) || 0;
      const y = parseFloat(whatIfLasso.marqueeRect.attr('y')) || 0;
      const w = parseFloat(whatIfLasso.marqueeRect.attr('width')) || 0;
      const h = parseFloat(whatIfLasso.marqueeRect.attr('height')) || 0;
      if (w > 2 && h > 2) {
        const poly = [
          [x, y], [x + w, y], [x + w, y + h], [x, y + h]
        ];
        applyWhatIfLassoSelection(poly);
      }
    }
  }

  clearWhatIfLassoGraphics();
}

/* ======================= Interaction: popup vs select ======================= */
let clickTimerId = null;
const DOUBLE_CLICK_MS = 280;

function handleClick(info, event) {
  const feat = info && info.object;
  if (!feat) return;
  noteFeatureClick();

  const e = event && event.srcEvent;
  const lngLat = info.coordinate || null;

  const selectingForDistance = !!(e && e.shiftKey);
  if (selectingForDistance) { selectForDistance(feat); return; }
  const appendSelection = isAdditiveSelectionEvent(e);

  if (clickTimerId) {
    clearTimeout(clickTimerId);
    clickTimerId = null;
    selectForDistance(feat);
    return;
  }

  clickTimerId = setTimeout(async () => {
    clickTimerId = null;

    if (lngLat) {
      if (fairActive && fairCategory !== 'mix' && feat.properties?.fair?.nearest_lonlat) {
        const cB = turf.centroid(feat).geometry.coordinates;
        const cP = feat.properties.fair.nearest_lonlat;
        try {
          const url = `${ROUTING_BASE_URL}/route/v1/${profileForMode(fairnessTravelMode)}/${cB[0]},${cB[1]};${cP[0]},${cP[1]}?overview=false&alternatives=false&geometries=geojson`;
          const r = await fetch(url);
          if (r.ok) {
            const j = await r.json();
            if (j.code === 'Ok' && j.routes && j.routes[0]) {
              const meters = j.routes[0].distance;
              const seconds = j.routes[0].duration;
              feat.properties.fair.nearest_dist_m = meters;
              feat.properties.fair.nearest_time_min = seconds / 60;
              feat.properties.fair.score = scoreFromTimeSeconds(fairCategory, seconds, fairnessTravelMode);
              fairRecolorTick++; updateLayers();
            }
          }
        } catch (e) {/* ignore */}
      }
      showPopup(feat, lngLat);
      applyMapSelection([feat], { append: appendSelection });
    }
  }, DOUBLE_CLICK_MS);
}

function selectForDistance(feat) {
  if (clickTimerId) { clearTimeout(clickTimerId); clickTimerId = null; }
  if (firstFeat === null) firstFeat = feat;
  else if (secondFeat === null && feat !== firstFeat) secondFeat = feat;
  else { firstFeat = feat; secondFeat = null; routeGeoJSON = null; if (distanceOut) distanceOut.textContent = '—'; }
  updateRouteIfReady();
}

function updateRouteIfReady() {
  if (!firstFeat || !secondFeat) { updateLayers(); return; }
  const c0 = turf.centroid(firstFeat).geometry.coordinates;
  const c1 = turf.centroid(secondFeat).geometry.coordinates;
  const osrmUrl = `${ROUTING_BASE_URL}/route/v1/${ROUTING_PROFILE}/${c0[0]},${c0[1]};${c1[0]},${c1[1]}?overview=full&geometries=geojson`;

  fetch(osrmUrl).then(r => r.json()).then(osrm => {
    if (osrm.code === 'Ok' && osrm.routes.length) {
      routeGeoJSON = { type:'FeatureCollection', features:[{ type:'Feature', geometry: osrm.routes[0].geometry }] };
      const km = (osrm.routes[0].distance / 1000).toFixed(2);
      if (distanceOut) distanceOut.textContent = `${km} km`;
    } else {
      routeGeoJSON = null; if (distanceOut) distanceOut.textContent = 'n/a';
    }
    updateLayers();
  }).catch(err => {
    console.error('OSRM error', err);
    routeGeoJSON = null; if (distanceOut) distanceOut.textContent = 'n/a';
    updateLayers();
  });
}

/* ======================= Summarize buildings & districts helpers ======================= */
function inferDistrict(props) {
  const keys = [
    'district','addr:district','city_district','addr:city_district',
    'suburb','addr:suburb','neighbourhood','neighborhood','addr:neighbourhood','borough'
  ];
  for (const k of keys) {
    const v = props?.[k];
    if (v && String(v).trim()) return String(v).trim();
  }
  return '';
}

// helper: group buildings by usage type (category)
function inferCategoryGroup(props) {
  if (!props) return null;

  // If the data already carries an explicit label, use it.
  if (props.category_label) {
    const label = canonicalBuildingType(props.category_label);
    if (label) return label;
  }

  // Prefer andamal1 from Lantmäteriet local data (most authoritative for Swedish buildings)
  if (props.andamal1) {
    const andamal = props.andamal1.toLowerCase();
    for (const [keyword, buildingType] of Object.entries(ANDAMAL_TO_BUILDING_TYPE)) {
      if (andamal.includes(keyword)) return buildingType;
    }
  }

  // Fall back to objekttyp from local data
  if (props.objekttyp && props.objekttyp !== 'yes' && props.objekttyp !== 'no' && props.objekttyp !== 'roof') {
    const objLow = props.objekttyp.toLowerCase();
    for (const [keyword, buildingType] of Object.entries(ANDAMAL_TO_BUILDING_TYPE)) {
      if (objLow.includes(keyword)) return buildingType;
    }
  }

  // Prefer the same coarse category mapping used by the fairness metrics
  const code = Number.isFinite(props.categoryCode)
    ? props.categoryCode
    : ((sourceMode === 's1' || sourceMode === 'osm_s1') ? localUsageCode(props) : osmCategoryCode(props));
  const label = categoryLabelFromCode(code);
  if (label && label !== 'Other / unknown') return label;

  const cat = props.category || props.building || props.name;
  const canon = canonicalBuildingType(cat);
  return canon || 'Other / unknown';
}


function summarizeByNamedDistricts(rows, { cutoff = 0.0, minCount = 1 } = {}) {
  const m = new Map();
  for (const r of rows) {
    if (!Number.isFinite(r.score) || r.score <= cutoff) continue;
    const d = (r.district && r.district.trim()) ? r.district.trim() : 'Unknown';
    let e = m.get(d);
    if (!e) e = { name: d, sum: 0, cnt: 0 };
    e.sum += r.score; e.cnt += 1;
    m.set(d, e);
  }
  return Array.from(m.values())
    .filter(e => e.cnt >= minCount)
    .map(e => ({ name: e.name, mean: e.sum / e.cnt, count: e.cnt }));
}

function summarizeByHex(rows, cellKm = HEX_CELL_KM, { cutoff = 0.0, minCount = HEX_MIN_COUNT } = {}) {
  const bbox = turf.bbox(baseCityFC);
  const hex = turf.hexGrid(bbox, cellKm, { units: 'kilometers' });

  const pts = turf.featureCollection(
    rows
      .filter(r => Number.isFinite(r.score) && r.score > cutoff)
      .map(r => turf.point(r.centroid, { score: r.score }))
  );

  const out = [];
  hex.features.forEach((poly, i) => {
    const within = turf.pointsWithinPolygon(pts, poly);
    if (!within.features.length) return;
    if (within.features.length < minCount) return;
    const scores = within.features.map(p => p.properties.score).filter(Number.isFinite);
    if (!scores.length) return;
    const mean = scores.reduce((s, v) => s + v, 0) / scores.length;
    out.push({ name: `Cell ${i + 1}`, mean, count: scores.length, polygon: poly });
  });

  return out;
}

function summarizeFairnessCurrent() {
  const feats = (baseCityFC?.features || [])
    .filter(f => f?.properties?.fair && Number.isFinite(f.properties.fair.score));
  if (!feats.length) return null;

  const rows = feats.map((f, idx) => ({
    idx,
    score: f.properties.fair.score,
    name: f.properties.name || f.properties.category || f.properties.objekttyp || `Building #${idx + 1}`,
    district: inferDistrict(f.properties),
    centroid: turf.centroid(f).geometry.coordinates
  }));

  const rowsNoOutliers = rows.filter(r => r.score > SUMMARY_CUTOFF);

  const baseForBldg = rowsNoOutliers.length ? rowsNoOutliers : rows;
  const sortedB = baseForBldg.slice().sort((a, b) => b.score - a.score);
  const bestBldg  = sortedB[0];
  const worstBldg = sortedB[sortedB.length - 1];

  let mode = 'district-prop';
  let districtStats;

  const withNameFiltered = rowsNoOutliers.filter(r => !!r.district);
  const useNamed = withNameFiltered.length >= (rowsNoOutliers.length || rows.length) * 0.5;

  if (useNamed && withNameFiltered.length) {
    districtStats = summarizeByNamedDistricts(rowsNoOutliers, { cutoff: SUMMARY_CUTOFF, minCount: 5 });
  } else {
    mode = 'hexgrid';
    const baseRows = rowsNoOutliers.length ? rowsNoOutliers : rows;
    districtStats = summarizeByHex(baseRows, HEX_CELL_KM, { cutoff: SUMMARY_CUTOFF, minCount: HEX_MIN_COUNT });
  }

  if (!districtStats.length) {
    return {
      bestBldg, worstBldg,
      bestDistrict: null, worstDistrict: null,
      nBuildings: rows.length, nDistricts: 0, mode
    };
  }

  const ds = districtStats.slice().sort((a, b) => b.mean - a.mean);
  const bestDistrict  = ds[0];
  const worstDistrict = ds[ds.length - 1];

  return {
    bestBldg, worstBldg, bestDistrict, worstDistrict,
    nBuildings: rows.length, nDistricts: ds.length, mode
  };
}

/* ======================= Side panel helpers ======================= */
function setText(sel, text) { const el = document.querySelector(sel); if (el) el.textContent = text; }
function setHTML(sel, html) { const el = document.querySelector(sel); if (el) el.innerHTML = html; }
function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&gt;','>':'&lt;','"':'&quot;',"'":'&#39;'}[c]) );
}
function linkify(label, role) {
  return `<a href="#" class="sp-jump" data-role="${role}" title="Zoom to ${escapeHTML(label)}">${escapeHTML(label)}</a>`;
}
function flyToPoint(lnglat) { try { map.flyTo({ center: lnglat, zoom: 16, pitch: 60, speed: 0.7 }); } catch {} }
function districtFeatureFromSummary(d) {
  if (!d) return null;
  if (d.polygon) return d.polygon;
  const pts = (baseCityFC?.features || [])
    .filter(f => (inferDistrict(f.properties) || 'Unknown') === d.name && f.properties?.fair)
    .map(f => turf.centroid(f));
  if (pts.length >= 3) {
    const hull = turf.convex(turf.featureCollection(pts));
    if (hull) return hull;
  }
  return null;
}
function fitToDistrict(d) {
  try {
    const feat = districtFeatureFromSummary(d);
    if (!feat) return;
    const [minX, minY, maxX, maxY] = turf.bbox(feat);
    map.fitBounds([[minX, minY], [maxX, maxY]], { padding: 40, duration: 800 });
  } catch {}
}

function setSidePanelLegendMode(mode = 'fairness') {
  const sidePanel = document.getElementById('sidePanel');
  if (!sidePanel) return;

  const showChangeLegend = mode === 'change' && pinnedChangeId != null && !changeCompareBaseline;
  sidePanel.dataset.legendMode = showChangeLegend ? 'change' : 'fairness';

  const titleEl = document.getElementById('spTitle');
  const legendEl = document.getElementById('fairnessLegendBar');

  if (showChangeLegend) {
    if (titleEl) titleEl.textContent = 'Change Map';
    if (legendEl) legendEl.style.background = changeLegendGradientCSS();
    setText('#spLegendLeft', 'Most worsened');
    setText('#spLegendMid', 'No change');
    setText('#spLegendRight', 'Most improved');
    setText('#spNote', 'Building color = per-building change from selected what-if (red = worsened, blue = improved).');
    return;
  }

  if (titleEl) titleEl.textContent = 'Fairness';
  if (legendEl) legendEl.style.background = fairnessLegendGradientCSS();
  setText('#spLegendLeft', 'Most fair');
  setText('#spLegendMid', 'Medium');
  setText('#spLegendRight', 'Least fair');
  const note = fairnessModel === 'ifcity'
    ? 'Building color = normalized benefit from gravity-based accessibility. Click a building to view IF-City debug values in the popup.'
    : 'Building color = accessibility score (same palette across map, DR, mezo, and macro views).';
  setText('#spNote', note);
}

function showSidePanel(cat, g, poiCount, summary = null) {
  const sidePanel = document.getElementById('sidePanel');
  if (!sidePanel) return;
  sidePanel.classList.remove('d-none');

  setText('#spCat',  prettyPOIName(cat));
  setText('#spGini', g.toFixed(2));
  setText('#spPois', String(poiCount));
  setSidePanelLegendMode((pinnedChangeId != null && !changeCompareBaseline) ? 'change' : 'fairness');
  clearBestWorstHighlights();
}

function hideSidePanel() {
  const sidePanel = document.getElementById('sidePanel');
  if (!sidePanel) return;
  sidePanel.classList.add('d-none');
  setText('#spCat',  '—');
  setText('#spGini', '—');
  setText('#spPois', '—');
  setSidePanelLegendMode('fairness');
  clearBestWorstHighlights();
}

/* ===================================================================== */
/* ===================== DR OFFCANVAS: PCA / UMAP, plotting, lasso ===== */
/* ===== Tiny blue spinner (top-right of DR area) ===== */
function ensureDRSpinnerStyles() {
  if (document.getElementById('drSpinnerCSS')) return;
  const st = document.createElement('style');
  st.id = 'drSpinnerCSS';
  st.textContent = `
    .dr-mini-spinner{
      position:absolute; top:50%; right:50%; z-index:20;
      width:1.25rem; height:1.25rem;
    }
  `;
  document.head.appendChild(st);
}

function ensureDRSpinner() {
  ensureDRSpinnerStyles();
  const wrap = document.getElementById('drCanvasWrap');
  if (!wrap) return;
  const pos = getComputedStyle(wrap).position;
  if (!pos || pos === 'static') wrap.style.position = 'relative';

  if (!document.getElementById('drGlobalSpinner')) {
    const sp = document.createElement('div');
    sp.id = 'drGlobalSpinner';
    sp.className = 'spinner-border text-primary dr-mini-spinner d-none';
    sp.setAttribute('role','status');
    sp.setAttribute('aria-hidden','true');
    sp.title = '';
    wrap.appendChild(sp);
  }
}

function showDRSpinner(statusMsg = '') {
  ensureDRSpinner();
  const sp = document.getElementById('drGlobalSpinner');
  const runBtn = document.getElementById('drRunBtn');
  const statusEl = document.getElementById('drStatus');
  if (sp) sp.classList.remove('d-none');
  if (runBtn) runBtn.disabled = true;
  if (statusEl) statusEl.textContent = statusMsg;
}

function hideDRSpinner(doneMsg = '') {
  const sp = document.getElementById('drGlobalSpinner');
  const runBtn = document.getElementById('drRunBtn');
  const statusEl = document.getElementById('drStatus');
  if (sp) sp.classList.add('d-none');
  if (runBtn) runBtn.disabled = false;
  if (statusEl) statusEl.textContent = doneMsg;
}

/* ---- Library detection ---- */
function hasUMAPGlobal() {
  if (window.UMAP && typeof window.UMAP === 'function') return true;
  if (window.umapjs && typeof window.umapjs.UMAP === 'function') { window.UMAP = window.umapjs.UMAP; return true; }
  if (window.umap && typeof window.umap.UMAP === 'function') { window.UMAP = window.umap.UMAP; return true; }
  return false;
}
let triedDynamicUMAP = false;
async function ensureUMAP() {
  if (hasUMAPGlobal()) return true;
  if (!triedDynamicUMAP) {
    triedDynamicUMAP = true;
    try {
      await new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.type = 'module';
        s.textContent = `
          import { UMAP } from 'https://cdn.skypack.dev/umap-js@1.3.3';
          window.UMAP = UMAP;
          window.dispatchEvent(new Event('umap:ready'));
        `;
        s.onerror = () => reject(new Error('ESM import failed'));
        window.addEventListener('umap:ready', () => resolve(), { once:true });
        document.head.appendChild(s);
      });
      return hasUMAPGlobal();
    } catch (e) {
      console.warn('UMAP dynamic import failed:', e);
      return false;
    }
  }
  return false;
}

/* ---- Canvas + overlay surface ---- */
// DR overlay + explanation globals
let drRO = null;

// Used to discard stale async engine responses (EBM + contrastive)
let drEngineRequestId = 0;

const drPlot = {
  // DR embedding + visual stuff
  points: null,
  colors: null,
  screenXY: null,
  minX: 0,
  maxX: 1,
  minY: 0,
  maxY: 1,
  pad: 20,
  width: 0,
  height: 0,
  sample: null,
  metrics: null,
  mode: "building",

  // DR feature matrix & labels (same feature space used for PCA/UMAP)
  features: null,          // X matrix from collectDRData()
  featureLabels: null,     // human-readable labels for each feature dimension

  // Cached stats & explanations (for your existing Stats / Model tabs)
  cityStats: null,             // per-feature stats over all DR points
  lastSelectionIdx: [],        // indices of the last lasso selection
  lastFeatureDiff: null,       // unsupervised feature differences for last selection (Stats tab)
  lastModelExplanation: null,  // supervised model explanation for last selection (Model tab)

  // engine-level feature-importance views above the tabs
  // engineMode chooses which engine drives the "EBM vs Contrast" bar chart
  engineMode: 'ebm',           // 'ebm' | 'contrast'
  engineEBM: null,             // last EBM response (from backend)
  engineContrast: null,        // last contrastive-distribution response (JS)
  runNonce: 0                  // increments on each DR run to vary sample unless reproducible mode is enabled
};

// Current explanation mode ("stats" | "model") - keep as-is for the lower tabs
let drExplainMode = 'stats';


/* ======================= DR ↔ Map selection helpers ======================= */
 /** Return currently selected buildings on the map (based on _drSelected). */
  function getCurrentMapSelection() {
    if (persistentBuildingSelection.size) {
      return Array.from(persistentBuildingSelection);
    }
    return (baseCityFC?.features || []).filter(f => f?.properties?._drSelected);
  }

  /** Remove any previous DR-based selection flag from buildings. */
  function clearDRMapSelection(opts = {}) {
    const { preservePersistent = false } = opts;
    const clearFeature = (obj) => {
      if (!obj) return;
      if (obj.properties) {
        if (obj.properties._drSelected) delete obj.properties._drSelected;
        if (obj.properties._drColor) delete obj.properties._drColor;
      } else {
        if (obj._drSelected) delete obj._drSelected;
        if (obj._drColor) delete obj._drColor;
      }
    };

    (baseCityFC?.features || []).forEach(clearFeature);
    (districtFC?.features || []).forEach(clearFeature);
    (mezoHexData || []).forEach(clearFeature);
    if (!preservePersistent) setPersistentBuildingSelection([]);
    drSelectionTick++;
}

function applyDRMapSelectionFromIndices(idxArray) {
  if (!idxArray || !idxArray.length || !drPlot.sample) {
    clearDRMapSelection();
    updateLayers();
    return;
  }

  const selectedEntities = idxArray
    .map(i => drPlot.sample[i])
    .filter(Boolean);
  applyMapSelection(selectedEntities, { skipDRSync: true });
}

// SAFE: returns refs if present, never throws
function ensureDRUI() {
  return {
    algoSel:    document.getElementById('drAlgo'),
    colorSel:   document.getElementById('drColorBy'),
    maxPtsEl:   document.getElementById('drMaxPts'),
    normEl:     document.getElementById('drNormalize'),
    canvas:     document.getElementById('drCanvas'),
    wrap:       document.getElementById('drCanvasWrap'),
    statusEl:   document.getElementById('drStatus'),
    infoEl:     document.getElementById('drInfo'),
    selInfoEl:  document.getElementById('drSelectInfo'),
    lassoBtn:   document.getElementById('drLassoBtn'),
    clearSelBtn:document.getElementById('drClearSelBtn'),
    clearProjBtn:document.getElementById('drClearProjBtn'),
    legendTitle:document.getElementById('drLegendTitle'),
    legendText: document.getElementById('drLegendText'),
    explainStatsEl:        document.getElementById('drExplainStats'),
    explainModelEl:        document.getElementById('drExplainModel'),
    explainModeStatsBtn:   document.getElementById('drExplainModeStats'),
    explainModeModelBtn:   document.getElementById('drExplainModeModel'),

    engineEBMBtn:          document.getElementById('drEngineEBMBtn'),
    engineContrastBtn:     document.getElementById('drEngineContrastBtn'),
    engineCalcBtn:         document.getElementById('drEngineCalcBtn'),
    enginePlotEl:          document.getElementById('drEnginePlot'),

    histWrap:          document.getElementById('drHistWrap'),
    districtBarWrap:   document.getElementById('drDistrictBarWrap'),
    scatterHeightWrap: document.getElementById('drScatterHeightWrap'),
    lorenzWrap:        document.getElementById('drLorenzWrap'),
    thresholdSlider:   document.getElementById('drThresholdSlider'),
    thresholdLabel:    document.getElementById('drThresholdLabel'),
    thresholdBarWrap:  document.getElementById('drThresholdBarWrap'),
    categoryHistsWrap: document.getElementById('drCategoryHistsWrap')
  };
}

function prepareDRSurface() {
  const { wrap } = ensureDRUI();
  if (!wrap) return;

  wrap.style.height = Math.max(213, Math.floor(window.innerHeight * 0.40)) + 'px';
  if (getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';

  if (!document.getElementById('drOverlay')) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.id = 'drOverlay';
    svg.setAttribute('style', 'position:absolute; inset:0; width:100%; height:100%; pointer-events:auto; z-index:2;');
    wrap.appendChild(svg);
  } else if (typeof d3 !== 'undefined' && !drPlot.points) {
    d3.select('#drOverlay').selectAll('*').remove();
  }

  if (!drRO) {
    drRO = new ResizeObserver(() => { resizeDRCanvas(); redrawDR(); });
    drRO.observe(wrap);
    window.addEventListener('resize', () => { resizeDRCanvas(); redrawDR(); });
  }
  resizeDRCanvas();
}

function resizeDRCanvas() {
  const { canvas, wrap } = ensureDRUI();
  if (!canvas || !wrap) return;
  const rect = wrap.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  ctx.fillStyle = '#f8f9fa';
  ctx.fillRect(0,0,rect.width,rect.height);
  // ctx.strokeStyle = 'rgba(0,0,0,0.08)';
  // for (let i=0;i<=rect.width;i+=50) { ctx.beginPath(); ctx.moveTo(i,0); ctx.lineTo(i,rect.height); ctx.stroke(); }
  // for (let i=0;i<=rect.height;i+=50){ ctx.beginPath(); ctx.moveTo(0,i); ctx.lineTo(rect.width,i); ctx.stroke(); }

  drPlot.width = rect.width; drPlot.height = rect.height;
}

function rampColor01(v) {
  const [r, g, b] = colorFromScore(v);
  return [r, g, b, 255];
}
function grey(a=180){ return [a,a,a,255]; }

function zscore(arr) {
  const vals = arr.filter(Number.isFinite);
  const mu = vals.reduce((a,b)=>a+b,0)/Math.max(1, vals.length);
  const sd = Math.sqrt(vals.reduce((s,v)=>s+(v-mu)*(v-mu),0)/Math.max(1, vals.length));
  return arr.map(v => Number.isFinite(v) ? (sd>0 ? (v-mu)/sd : 0) : 0);
}
function logAreaOfFeature(f) { try { return Math.log(Math.max(1, turf.area(f))); } catch { return 0; } }

function stableHashString(str) {
  let h = 2166136261;
  const text = String(str || '');
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function stableEntityId(entity, fallbackIdx = 0) {
  const props = entity?.properties || entity || {};
  const base =
    props.id ??
    props.osm_id ??
    props['@id'] ??
    props.hex ??
    props.__districtName ??
    props.name;
  if (base != null && String(base).trim()) return String(base);

  const coords = entity?.geometry?.coordinates;
  if (Array.isArray(coords)) {
    const flat = JSON.stringify(coords).slice(0, 160);
    if (flat) return `geom:${flat}`;
  }
  return `idx:${fallbackIdx}`;
}

function stableSampleIndices(sample, take, runNonce = 0) {
  const baseSeed = Number(globalThis.DR_SAMPLE_SEED) || 202502;
  const reproducible = !!globalThis.DR_REPRODUCIBLE_SAMPLE;
  const seed = reproducible ? baseSeed : (baseSeed + Math.imul(Number(runNonce) || 0, 2654435761));
  const ranked = sample.map((entity, idx) => {
    const key = stableEntityId(entity, idx);
    return { idx, score: stableHashString(`${seed}:${key}`) };
  });
  ranked.sort((a, b) => a.score - b.score || a.idx - b.idx);
  return ranked.slice(0, take).map((entry) => entry.idx);
}

function isWhatIfEntity(entity) {
  const props = entity?.properties || {};
  // Keep DR/PC visibility for synthetic what-if additions, but avoid pinning
  // every edited real building (which can destabilize the sample manifold).
  return !!(props.__whatIfMock || props.__whatIfAdded);
}

function isWhatIfEntity(entity) {
  const props = entity?.properties || {};
  return !!(props.__whatIf || props.__whatIfMock || props.__whatIfAdded);
}

/* ---- DR feature collection (different for OSM vs Local) ---- */
function collectDRData(maxPts = Infinity, normalize = true, colorBy = 'overall', runNonce = 0) {
  const mode = currentDRDataMode();

  const makeMatrix = (sample, rows, labels) => {
    if (!sample.length) throw new Error(`No ${mode}s available for DR.`);

    const N = sample.length;
    const take = Math.min(maxPts, N);
    const stableOrder = stableSampleIndices(sample, sample.length, runNonce);
    const forcedSet = new Set();
    sample.forEach((entity, i) => {
      if (isWhatIfEntity(entity)) forcedSet.add(i);
    });

    const forcedCandidates = stableOrder.filter(i => forcedSet.has(i));
    const regularCandidates = stableOrder.filter(i => !forcedSet.has(i));

    // Balanced pseudo-random mix: keep mocked buildings visible without letting
    // them fully dominate the DR sample when maxPts is capped.
    const hasBothGroups = forcedCandidates.length > 0 && regularCandidates.length > 0;
    let forcedTake = 0;
    if (hasBothGroups) {
      const populationShare = forcedCandidates.length / Math.max(1, N);
      const proportionalTarget = Math.round(take * populationShare);
      const forcedCap = Math.max(1, Math.floor(take * 0.35));
      forcedTake = Math.max(1, Math.min(forcedCandidates.length, forcedCap, proportionalTarget || 1));
    } else if (forcedCandidates.length > 0) {
      forcedTake = Math.min(forcedCandidates.length, take);
    }

    let regularTake = Math.min(regularCandidates.length, Math.max(0, take - forcedTake));
    let remainder = take - (forcedTake + regularTake);
    if (remainder > 0 && forcedCandidates.length > forcedTake) {
      const extraForced = Math.min(remainder, forcedCandidates.length - forcedTake);
      forcedTake += extraForced;
      remainder -= extraForced;
    }
    if (remainder > 0 && regularCandidates.length > regularTake) {
      regularTake += Math.min(remainder, regularCandidates.length - regularTake);
    }

    const forcedIdx = forcedCandidates.slice(0, forcedTake);
    const sampledIdx = regularCandidates.slice(0, regularTake);

    // Preserve deterministic pseudo-random ordering to keep UMAP behavior stable.
    const idxSet = new Set([...forcedIdx, ...sampledIdx]);
    const idx = stableOrder.filter(i => idxSet.has(i));

    const pickedSample = idx.map(i => sample[i]);
    const pickedRows = idx.map(i => rows[i]);
    const keys = Object.keys(labels);
    const vecByKey = {};

    keys.forEach((key) => {
      const vals = pickedRows.map(r => Number.isFinite(r[key]) ? r[key] : null);
      const valid = vals.filter(Number.isFinite);
      const mid = valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : 0;
      const imputed = vals.map(v => Number.isFinite(v) ? v : mid);
      vecByKey[key] = normalize ? zscore(imputed) : imputed;
    });

  const X = pickedRows.map((_, i) => keys.map(key => vecByKey[key][i]));

    let colors;
    if (colorBy === 'overall') {
      colors = pickedRows.map((r) => Number.isFinite(r.overall) ? rampColor01(r.overall) : grey(120));
      updateLegend('overall', `Legend (${mode} overall fairness)`, 'Most fair (left) → Least fair (right)');
    } else if (colorBy === 'poi') {
      colors = pickedRows.map((r) => Number.isFinite(r.focused) ? rampColor01(r.focused) : grey(100));
      updateLegend('poi', `Legend (${prettyPOIName(fairCategory) || 'Selected fairness'})`, 'Most fair (left) → Least fair (right)');
    } else if (colorBy === 'year') {
      colors = pickedRows.map((r) => Number.isFinite(r.yearLike) ? rampColor01(r.yearLike) : grey(110));
      updateLegend('year', `Legend (${mode} temporal proxy)`, 'Low (left) → High (right)');
    } else {
      colors = pickedRows.map((r) => Number.isFinite(r.heightLike) ? rampColor01(r.heightLike) : grey(110));
      updateLegend('height', `Legend (${mode} size proxy)`, 'Low (left) → High (right)');
    }

  return {
      X,
      colors,
      sampleCount: pickedSample.length,
      dims: X[0]?.length || 0,
      sample: pickedSample,
      metrics: {
        heights: pickedRows.map(r => r.heightLike),
        years: pickedRows.map(r => r.yearRaw),
        overall: pickedRows.map(r => r.overall),
        areaLog: pickedRows.map(r => r.areaLike),
        changeScore: pickedRows.map(r => r.changeLike),
        fairGrocery: pickedRows.map(r => r.grocery),
        fairHospital: pickedRows.map(r => r.hospital),
        fairPrimary: pickedRows.map(r => r.primary),
        fairPharmacy:    pickedRows.map(r => r.pharmacy),
        fairHealthcare:  pickedRows.map(r => r.healthcare),
        fairKindergarten:pickedRows.map(r => r.kindergarten),
        fairSchoolHigh:  pickedRows.map(r => r.schoolHigh),
        categoryCode: pickedRows.map(r => r.categoryLike),
        isChange: pickedRows.map(r => r.isChangeLike)
      },
      featureLabels: keys.map(key => labels[key]),
      mode
    };
  };

  if (mode === 'district') {
    if (!districtFC?.features?.length) throw new Error('No districts loaded.');
    const sample = districtFC.features;
    const rows = sample.map((f) => {
      const props = f?.properties || {};
      const byCat = props.__fairByCat || {};
      const areaSqKm = turf.area(f) / 1e6;
      return {
        overall: Number(props.__fairOverall),
        focused: Number(props.__score),
        grocery: Number(byCat.grocery),
        hospital: Number(byCat.hospital),
        primary: Number(byCat.school_primary),
        pharmacy: Number(byCat.pharmacy),
        healthcare: Number(byCat.healthcare_center),
        kindergarten: Number(byCat.kindergarten),
        schoolHigh: Number(byCat.school_high),
        areaLike: Number.isFinite(areaSqKm) ? Math.log1p(areaSqKm) : null,
        heightLike: Number(props.__count),
        yearLike: Number(props.__fairFocused),
        yearRaw: null,
        categoryLike: Number(props.__count),
        changeLike: 0,
        isChangeLike: 0
      };
    });
    return makeMatrix(sample, rows, {
      overall: 'overall fairness (z)',
      focused: 'focused fairness (z)',
      grocery: 'grocery fairness (z)',
      hospital: 'hospital fairness (z)',
      primary: 'primary school fairness (z)',
      pharmacy: 'pharmacy fairness (z)',
      healthcare: 'healthcare center fairness (z)',
      kindergarten: 'kindergarten fairness (z)',
      schoolHigh: 'high school fairness (z)',
      areaLike: 'log(area km²) (z)',
      heightLike: 'building count (z)'
    });
  }

  if (mode === 'mezo') {
    if (!mezoHexData?.length) throw new Error('No mezo cells available.');
    const sample = mezoHexData;
    const rows = sample.map((cell) => {
      const byCat = cell?.__fairByCat || {};
      return {
        overall: Number(cell?.__fairOverall),
        focused: Number(cell?.__score),
        grocery: Number(byCat.grocery),
        hospital: Number(byCat.hospital),
        primary: Number(byCat.school_primary),
        pharmacy: Number(byCat.pharmacy),
        healthcare: Number(byCat.healthcare_center),
        kindergarten: Number(byCat.kindergarten),
        schoolHigh: Number(byCat.school_high),
        areaLike: Number(cell?.__count),
        heightLike: Number(cell?.__count),
        yearLike: Number(cell?.__fairFocused),
        yearRaw: null,
        categoryLike: Number(cell?.__count),
        changeLike: 0,
        isChangeLike: 0
      };
    });
    return makeMatrix(sample, rows, {
      overall: 'overall fairness (z)',
      focused: 'focused fairness (z)',
      grocery: 'grocery fairness (z)',
      hospital: 'hospital fairness (z)',
      primary: 'primary school fairness (z)',
      pharmacy: 'pharmacy fairness (z)',
      healthcare: 'healthcare center fairness (z)',
      kindergarten: 'kindergarten fairness (z)',
      schoolHigh: 'high school fairness (z)',
      areaLike: 'building count (z)',
      heightLike: 'cell density proxy (z)'
    });
  }

  if (!baseCityFC?.features?.length) throw new Error('No buildings loaded.');

  const sample = baseCityFC.features;
  const rows = sample.map((f) => {
    const props = f.properties || {};
    const built = getBuiltYear(props);
    const fm = props.fair_multi || {};
    return {
      overall: Number(props.fair_overall?.score),
      focused: Number(props.fair?.score),
      grocery: Number(fm.grocery?.score),
      hospital: Number(fm.hospital?.score),
      primary: Number(fm.school_primary?.score),
      pharmacy: Number(fm.pharmacy?.score),
      healthcare: Number(fm.healthcare_center?.score),
      kindergarten: Number(fm.kindergarten?.score),
      schoolHigh: Number(fm.school_high?.score),
      areaLike: logAreaOfFeature(f),
      heightLike: clampElev((props.height_m ?? props._mean) || 10),
      yearLike: Number.isFinite(built) ? built : null,
      yearRaw: Number.isFinite(built) ? built : null,
      categoryLike: (sourceMode === 's1')
        ? localUsageCode(props)
        : (sourceMode === 'osm_s1' ? hybridCategoryCode(props) : osmCategoryCode(props)),
      changeLike: Number(props.change_score),
      isChangeLike: props.is_change ? 1 : 0
    };
  });

  return makeMatrix(sample, rows, {
    heightLike: 'height (z)',
    yearLike: 'built-year (z)',
    overall: 'overall fairness (z)',
    areaLike: 'log(area) (z)',
    grocery: 'grocery fairness (z)',
    hospital: 'hospital fairness (z)',
    primary: 'primary school fairness (z)',
    pharmacy: 'pharmacy fairness (z)',
    healthcare: 'healthcare center fairness (z)',
    kindergarten: 'kindergarten fairness (z)',
    schoolHigh: 'high school fairness (z)',
    categoryLike: 'category code (z)'
  });
}

function updateLegend(kind, title, text) {
  const { legendTitle, legendText } = ensureDRUI();
  if (legendTitle) legendTitle.textContent = title;
  if (legendText)  legendText.textContent  = text;
}

/* ---------- PCA ---------- */
function meanCenter(X) {
  const n = X.length, d = X[0].length;
  const mu = new Array(d).fill(0);
  for (let i=0;i<n;i++) for (let j=0;j<d;j++) mu[j]+=X[i][j];
  for (let j=0;j<d;j++) mu[j]/=n;
  const Y = new Array(n);
  for (let i=0;i<n;i++) {
    const row = new Array(d);
    for (let j=0;j<d;j++) row[j]=X[i][j]-mu[j];
    Y[i]=row;
  }
  return { Y, mu };
}
function covMatrix(Y) {
  const n = Y.length, d = Y[0].length;
  const C = Array.from({length:d},()=>new Array(d).fill(0));
  for (let i=0;i<n;i++) {
    const r=Y[i];
    for (let a=0;a<d;a++) for (let b=a;b<d;b++) C[a][b]+=r[a]*r[b];
  }
  for (let a=0;a<d;a++) for (let b=a;b<d;b++) C[b][a]=C[a][b];
  return C;
}
function matVec(C,v){ const d=C.length; const out=new Array(d).fill(0); for (let i=0;i<d;i++){ let s=0; for (let j=0;j<d;j++) s+=C[i][j]*v[j]; out[i]=s; } return out; }
function dot(a,b){ let s=0; for (let i=0;i<a.length;i++) s+=a[i]*b[i]; return s; }
function vecNorm(v){ let s=0; for (let i=0;i<v.length;i++) s+=v[i]*v[i]; return Math.sqrt(s); }
function powerIter(C, iters=500, eps=1e-9) {
  const d=C.length; let v=new Array(d).fill(0).map(()=>Math.random()-0.5);
  let nrm=vecNorm(v); for (let i=0;i<d;i++) v[i]/=nrm;
  let lambda=0;
  for (let k=0;k<iters;k++){
    const Cv=matVec(C,v);
    const n=vecNorm(Cv);
    if (n<1e-12) break;
    for (let i=0;i<d;i++) v[i]=Cv[i]/n;
    const lam=dot(v, matVec(C,v));
    if (Math.abs(lam-lambda)<eps) break;
    lambda=lam;
  }
  return { vec:v, val:dot(v, matVec(C,v)) };
}
function deflate(C, vec, val) {
  const d=C.length;
  for (let i=0;i<d;i++) for (let j=0;j<d;j++) C[i][j]-=val*vec[i]*vec[j];
  return C;
}
function runPCA(X) {
  const { Y } = meanCenter(X);
  const C = covMatrix(Y);
  const pc1 = powerIter(C, 500).vec;
  const val1 = dot(pc1, matVec(C,pc1));
  deflate(C, pc1, val1);
  const pc2 = powerIter(C, 500).vec;
  const pts = Y.map(r => [dot(r, pc1), dot(r, pc2)]);
  return pts;
}

/* ---------- UMAP ---------- */
async function runUMAP(X, {nNeighbors=15, minDist=0.1, nEpochs=200} = {}) {
  const ok = await ensureUMAP();
  if (!ok) throw new Error('UMAP library not loaded');
  const umap = new window.UMAP({ nNeighbors, minDist, nEpochs, random: Math.random });
  const Y = await umap.fitAsync(X);
  return Y;
}

/* ---------- Plot + selection drawing ---------- */
function computeScreenPositions(points) {
  const xs = points.map(p=>p[0]), ys = points.map(p=>p[1]);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const pad = 20, W = drPlot.width, H = drPlot.height;

  drPlot.minX=minX; drPlot.maxX=maxX; drPlot.minY=minY; drPlot.maxY=maxY; drPlot.pad=pad;

  const screenXY = new Array(points.length);
  for (let i=0;i<points.length;i++) {
    const x = points[i][0], y = points[i][1];
    const nx = (x - minX) / Math.max(1e-9, (maxX-minX));
    const ny = (y - minY) / Math.max(1e-9, (maxY-minY));
    const cx = pad + nx*(W - 2*pad);
    const cy = pad + (1-ny)*(H - 2*pad);
    screenXY[i] = [cx, cy];
  }
  return screenXY;
}

function redrawDR(selectedIdx = null) {
  const { canvas } = ensureDRUI();
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  resizeDRCanvas();

  if (!drPlot.points || !drPlot.colors) return;

  const mode = currentDRDataMode();
  const dotSize = DR_DOT_SIZE_BY_MODE[mode] || DR_DOT_SIZE_BY_MODE.building;
  const normalSize = Math.max(1, Math.round(dotSize.normal));
  const selectedSize = Math.max(normalSize + 1, Math.round(dotSize.selected));
  const normalHalf = Math.floor(normalSize / 2);
  const selectedHalf = Math.floor(selectedSize / 2);

  const baseAlpha = 0.35;
  const [ur, ug, ub] = DR_UNSELECTED_COLOR; 
  for (let i=0;i<drPlot.screenXY.length;i++) {
    const [cx, cy] = drPlot.screenXY[i];
    ctx.fillStyle = `rgba(${ur},${ug},${ub},${baseAlpha})`;
    ctx.fillRect(Math.round(cx) - normalHalf, Math.round(cy) - normalHalf, normalSize, normalSize);
  }

  if (selectedIdx && selectedIdx.length) {
    for (const i of selectedIdx) {
      const [cx, cy] = drPlot.screenXY[i];
      ctx.fillStyle = 'rgba(241,105,19,1)';
      ctx.fillRect(Math.round(cx) - selectedHalf, Math.round(cy) - selectedHalf, selectedSize, selectedSize);
    }
  } else {
    for (let i=0;i<drPlot.screenXY.length;i++) {
      const [cx, cy] = drPlot.screenXY[i];
      const c = drPlot.colors[i] || [255,255,255,255];
      ctx.fillStyle = `rgba(${c[0]},${c[1]},${c[2]},${(c[3]??255)/255})`;
      ctx.fillRect(Math.round(cx) - normalHalf, Math.round(cy) - normalHalf, normalSize, normalSize);
    }
  }
}

/* ---------- Lasso (d3) ---------- */
const lasso = {
  active: false,
  drawing: false,
  points: [],
  selectedIdx: [],
  path: null,
  appendMode: false,

  // marquee (rectangular) selection state
  marqueeDrawing: false,
  marqueeStart: null,   // [x,y] at Shift+drag start
  marqueeRect: null,     // d3 <rect> overlay while dragging
};

function setLassoActive(active) {
  lasso.active = !!active;

  if (!lasso.active) {
    lasso.drawing = false;
    lasso.points = [];
    lasso.appendMode = false;
    lasso.marqueeDrawing = false;
    lasso.marqueeStart = null;
  }

  const { lassoBtn } = ensureDRUI();
  if (lassoBtn) {
    lassoBtn.classList.toggle('btn-secondary', lasso.active);
    lassoBtn.classList.toggle('btn-outline-secondary', !lasso.active);
  }

  if (!lasso.active) {
    const svgEl = document.getElementById('drOverlay');
    if (svgEl && typeof d3 !== 'undefined') {
      d3.select(svgEl).selectAll('*').remove();
    }
  }
}


function toggleLasso() {
  setLassoActive(!lasso.active);
  initD3Overlay();
}

function initD3Overlay() {
  const svgEl = document.getElementById('drOverlay');
  if (!svgEl || typeof d3 === 'undefined') return;

  const svg = d3.select(svgEl);
  svg.selectAll('*').remove();

  if (lasso.selectedIdx.length) drawSelectionHull(svg);

  const getXY = (event) => {
    const rect = svgEl.getBoundingClientRect();
    return [event.clientX - rect.left, event.clientY - rect.top];
  };

  // ===== off-centred pointer helper ==================================
  function updateOffcenterPointer(x, y) {
    // No DR result yet → nothing to show
    if (!drPlot.screenXY || !drPlot.screenXY.length) {
      svg.selectAll('.off-pointer').remove();
      return;
    }

    // If user is currently drawing lasso / marquee, hide the pointer overlay
    if (lasso.drawing || lasso.marqueeDrawing) {
      svg.selectAll('.off-pointer').remove();
      return;
    }

    const pts = drPlot.screenXY;
    let bestIdx = -1;
    let bestD2 = Infinity;

    for (let i = 0; i < pts.length; i++) {
      const dx = x - pts[i][0];
      const dy = y - pts[i][1];
      const d2 = dx * dx + dy * dy;
      if (d2 < bestD2) {
        bestD2 = d2;
        bestIdx = i;
      }
    }

    // If the nearest point is too far from the cursor, hide overlay
    const MAX_DIST2 = 30 * 30; // ~30px radius
    if (bestIdx < 0 || bestD2 > MAX_DIST2) {
      svg.selectAll('.off-pointer').remove();
      return;
    }

    const [px, py] = pts[bestIdx];
    const feat = drPlot.sample ? drPlot.sample[bestIdx] : null;
    const props = (feat && feat.properties) || {};

    const name =
      props.name ||
      props.category ||
      props.objekttyp ||
      `Building #${bestIdx + 1}`;

    const heights = drPlot.metrics?.heights || [];
    const overall = drPlot.metrics?.overall || [];
    const h = Number.isFinite(heights[bestIdx]) ? heights[bestIdx].toFixed(1) : '—';
    const o = Number.isFinite(overall[bestIdx]) ? overall[bestIdx].toFixed(2) : '—';

    const labelText = `${name}  |  h≈${h} m  |  overall≈${o}`;

    // Position label near the mouse, not on top of the dense cluster
    const labelX = x + 16;
    const labelY = y - 16;

    const g = svg.selectAll('g.off-pointer').data([1]);
    const gEnter = g.enter().append('g').attr('class', 'off-pointer');
    gEnter.append('line').attr('class', 'off-pointer-link');
    gEnter.append('circle').attr('class', 'off-pointer-circle');
    gEnter.append('rect').attr('class', 'off-pointer-label-bg');
    gEnter.append('text').attr('class', 'off-pointer-label-text');
    const gAll = gEnter.merge(g);

    // line from mouse → actual point
    gAll.select('.off-pointer-link')
      .attr('x1', x)
      .attr('y1', y)
      .attr('x2', px)
      .attr('y2', py)
      .attr('stroke', 'rgba(255,255,255,0.6)')
      .attr('stroke-width', 1.2)
      .attr('pointer-events', 'none');

    // highlight circle on the true point
    gAll.select('.off-pointer-circle')
      .attr('cx', px)
      .attr('cy', py)
      .attr('r', 5)
      .attr('fill', 'rgba(255,255,255,0.95)')
      .attr('stroke', 'rgba(0,0,0,0.85)')
      .attr('stroke-width', 1.5)
      .attr('pointer-events', 'none');

    const text = gAll.select('.off-pointer-label-text')
      .attr('x', labelX + 8)
      .attr('y', labelY + 12)
      .attr('fill', '#ffffff')
      .attr('font-size', 10)
      .text(labelText);

    // Background rounded rect sized to the text
    const node = text.node();
    if (node) {
      const bbox = node.getBBox();
      gAll.select('.off-pointer-label-bg')
        .attr('x', bbox.x - 4)
        .attr('y', bbox.y - 2)
        .attr('width', bbox.width + 8)
        .attr('height', bbox.height + 4)
        .attr('rx', 4)
        .attr('ry', 4)
        .attr('fill', 'rgba(0,0,0,0.85)')
        .attr('stroke', 'rgba(255,255,255,0.4)')
        .attr('stroke-width', 0.8)
        .attr('pointer-events', 'none');
    }
  }
  // ======================================================================

  function onDown(event) {
    if (!lasso.active || (event.button != null && event.button !== 0)) return;
    event.preventDefault();
    lasso.appendMode = isAdditiveSelectionEvent(event);

    const useMarquee = !!event.shiftKey; // SHIFT + drag → rectangular
    if (useMarquee) {
      // start marquee
      lasso.marqueeDrawing = true;
      lasso.drawing = false;
      lasso.marqueeStart = getXY(event);

      if (lasso.marqueeRect) {
        lasso.marqueeRect.remove();
        lasso.marqueeRect = null;
      }
      if (lasso.path) {
        lasso.path.remove();
        lasso.path = null;
      }

      const [x, y] = lasso.marqueeStart;
      lasso.marqueeRect = svg.append('rect')
        .attr('x', x)
        .attr('y', y)
        .attr('width', 0)
        .attr('height', 0)
        .attr('fill', 'rgba(0,150,255,0.08)')
        .attr('stroke', 'rgba(0,150,255,0.9)')
        .attr('stroke-width', 2)
        .attr('pointer-events', 'none');
    } else {
      // start free-form lasso
      lasso.drawing = true;
      lasso.marqueeDrawing = false;
      lasso.points = [getXY(event)];

      if (lasso.path) lasso.path.remove();
      if (lasso.marqueeRect) {
        lasso.marqueeRect.remove();
        lasso.marqueeRect = null;
      }

      lasso.path = svg.append('path')
        .attr('fill', 'rgba(0,150,255,0.08)')
        .attr('stroke', 'rgba(0,150,255,0.9)')
        .attr('stroke-width', 2)
        .attr('d', d3.line()(lasso.points));
    }
  }

  function onMove(event) {
    const [x, y] = getXY(event);

    // always update off-centred pointer on move
    updateOffcenterPointer(x, y);

    // If we are not drawing anything, stop here
    if (!lasso.drawing && !lasso.marqueeDrawing) return;
    event.preventDefault();

    if (lasso.drawing) {
      // free-form lasso
      lasso.points.push([x, y]);
      if (lasso.path) {
        lasso.path.attr('d', d3.line()(lasso.points));
      }
    } else if (lasso.marqueeDrawing && lasso.marqueeRect && lasso.marqueeStart) {
      // rectangular marquee
      const [x0, y0] = lasso.marqueeStart;
      const rx = Math.min(x, x0);
      const ry = Math.min(y, y0);
      const rw = Math.abs(x - x0);
      const rh = Math.abs(y - y0);
      lasso.marqueeRect
        .attr('x', rx)
        .attr('y', ry)
        .attr('width', rw)
        .attr('height', rh);
    }
  }

  function finish(event) {
    if (!lasso.drawing && !lasso.marqueeDrawing) return;
    const append = lasso.appendMode || isAdditiveSelectionEvent(event);
    lasso.appendMode = false;

    // ---- free-form lasso finish ----
    if (lasso.drawing) {
      lasso.drawing = false;
      if (lasso.points.length < 3) {
        if (lasso.path) {
          lasso.path.remove();
          lasso.path = null;
        }
        return;
      }
      const poly = lasso.points.slice();
      poly.push(poly[0]);
      computeSelection(poly, { append });
    }

    // ---- marquee finish ----
    if (lasso.marqueeDrawing) {
      lasso.marqueeDrawing = false;

      if (lasso.marqueeRect) {
        const x  = parseFloat(lasso.marqueeRect.attr('x')) || 0;
        const y  = parseFloat(lasso.marqueeRect.attr('y')) || 0;
        const w  = parseFloat(lasso.marqueeRect.attr('width')) || 0;
        const h  = parseFloat(lasso.marqueeRect.attr('height')) || 0;

        // If rectangle is tiny, treat as no selection
        if (w >= 2 && h >= 2) {
          const poly = [
            [x,       y      ],
            [x + w,   y      ],
            [x + w,   y + h  ],
            [x,       y + h  ],
            [x,       y      ]
          ];
          computeSelection(poly, { append });
        }

        lasso.marqueeRect.remove();
        lasso.marqueeRect = null;
      }
    }
  }

  svg
    .on('pointerdown', onDown)
    .on('pointermove', onMove)
    .on('pointerup', finish)
    .on('mousedown', onDown)
    .on('mousemove', onMove)
    .on('mouseup', finish)
    .on('dblclick', finish)
    .style('cursor', lasso.active ? 'crosshair' : 'default');
}



function applyDRSelection(idxArray, opts = {}) {
  const { skipMapSync = false, skipParallelSync = false, append = false } = opts;

  if (!skipParallelSync) {
    parallelCoordsForceEmptySelection = false;
  }
  const incomingSelection = Array.isArray(idxArray) ? idxArray.slice() : [];
  const selection = append
    ? Array.from(new Set([...(Array.isArray(lasso.selectedIdx) ? lasso.selectedIdx : []), ...incomingSelection]))
    : incomingSelection;

  lasso.selectedIdx = selection;
  drHasSelection = selection.length > 0;

  if (typeof d3 !== 'undefined') {
    const svgEl = document.getElementById('drOverlay');
    if (svgEl) {
      const svg = d3.select(svgEl);
      svg.selectAll('.sel-hull').remove();
      drawSelectionHull(svg);
    }
  }

  redrawDR(selection.length ? selection : null);
  renderSelectionStats(selection);

  // push this selection into the map (mark buildings as _drSelected)
  if (!skipMapSync) {
    applyDRMapSelectionFromIndices(selection);
    setMapLassoClearDisabled(selection.length === 0);
  }

  const { clearSelBtn } = ensureDRUI();
  if (clearSelBtn) clearSelBtn.disabled = selection.length === 0;

  if (!skipParallelSync && parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
}

function computeSelection(polygon, opts = {}) {
  if (!drPlot.screenXY || !polygon) return;
  const idx = [];
  for (let i = 0; i < drPlot.screenXY.length; i++) {
    if (d3.polygonContains(polygon, drPlot.screenXY[i])) idx.push(i);
  }
  applyDRSelection(idx, opts);
}



function syncDRSelectionFromMapFeature(feat, opts = {}) {
  const feats = Array.isArray(feat) ? feat.filter(Boolean) : (feat ? [feat] : []);
  const { preserveMapSelection = false, append = false } = opts;

  const { statusEl } = ensureDRUI();

  if (!feats.length || !drPlot.points || !Array.isArray(drPlot.sample)) {
    // if (statusEl) statusEl.textContent = 'Run UMAP to sync selections from the map.';
    return;
  }

  const idx = [];
  feats.forEach((f) => {
    const i = drPlot.sample.indexOf(f);
    if (i !== -1) idx.push(i);
  });

  if (!drPlot.screenXY && drPlot.points) {
    drPlot.screenXY = computeScreenPositions(drPlot.points);
  }

  if (statusEl) {
    const noun = idx.length === 1 ? 'Building' : `${idx.length} buildings`;
    // statusEl.textContent = `${noun} synced to UMAP selection.`;
  }

  prepareDRSurface();
  initD3Overlay();
  applyDRSelection(idx, { skipMapSync: preserveMapSelection, append });
}

function drawSelectionHull(svg) {
  if (!svg || typeof d3 === 'undefined') return;
  if (!lasso.selectedIdx.length) return;
  const pts = lasso.selectedIdx.map(i => drPlot.screenXY[i]);
  const hull = d3.polygonHull(pts);
  if (!hull) return;
  svg.append('path')
    .attr('class', 'sel-hull')
    .attr('d', 'M'+hull.map(p=>p.join(',')).join('L')+'Z')
    .attr('fill', 'rgba(255,215,0,0.07)')
    .attr('stroke', 'rgba(255,215,0,0.9)')
    .attr('stroke-width', 2);
}

function clearSelection() {
  setPersistentBuildingSelection([]);
  clearParallelCoordsSelectionFromClearAction();
  lasso.selectedIdx = [];
  lasso.points = [];
  lasso.drawing = false;
  lasso.marqueeDrawing = false;
  lasso.marqueeStart = null;

  if (lasso.path) {
    lasso.path.remove();
    lasso.path = null;
  }
  if (lasso.marqueeRect) {
    lasso.marqueeRect.remove();
    lasso.marqueeRect = null;
  }

  drHasSelection = false;

  const svg = (typeof d3 !== 'undefined') ? d3.select('#drOverlay') : null;
  if (svg) svg.selectAll('*').remove();

  redrawDR();
  renderSelectionStats([]);

  // also clear DR-based highlights on the map
  clearDRMapSelection();

  updateLayers();

  const { clearSelBtn } = ensureDRUI();
  if (clearSelBtn) clearSelBtn.disabled = true;
}




function renderSelectionStats(idx) {
  const { selInfoEl } = ensureDRUI();
  if (!selInfoEl) return;

  const selection = Array.isArray(idx) ? idx.slice() : [];

  // Share selection with all DR-related views
  drPlot.lastSelectionIdx = selection;

  // Reset caches for the lower Stats/Model views
  drPlot.lastFeatureDiff = null;
  drPlot.lastModelExplanation = null;

  // Reset caches for the upper EBM / contrastive engines
  drPlot.engineEBM = null;
  drPlot.engineContrast = null;

  // ---- No selection case ----
  if (!selection.length) {
    selInfoEl.textContent = 'Selection: 0 points';

    if (typeof refreshExplanationView === 'function') {
      refreshExplanationView();
    }
    if (typeof refreshEnginePlot === 'function') {
      refreshEnginePlot();
    }

    // show city-wide views only (no selection overlay)
    renderFairnessHistogram([]);
    renderDistrictBar([]);
    renderHeightFairnessScatter([]);
    renderLorenzCurve([]);
    renderThresholdBar([]);
    renderCategoryHists([]);

    return;
  }

  // ---- Selection summary (unchanged core logic) ----
  const m = drPlot.metrics || {};
  const mean = (arr, pick) => {
    const vals = selection.map(i => pick(i)).filter(Number.isFinite);
    if (!vals.length) return NaN;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  };

  const count = selection.length;
  const hMean = mean(m.heights || [], (i) => m.heights[i]);
  const oMean = mean(m.overall || [], (i) => m.overall[i]);
  const yearsKnown = selection
    .map(i => (m.years || [])[i])
    .filter(Number.isFinite);
  const yMin = yearsKnown.length ? Math.min(...yearsKnown) : null;
  const yMax = yearsKnown.length ? Math.max(...yearsKnown) : null;

  selInfoEl.textContent =
    `Selection: ${count} points` +
    (Number.isFinite(hMean) ? ` | mean height ≈ ${hMean.toFixed(1)} m` : '') +
    (Number.isFinite(oMean) ? ` | mean overall ≈ ${oMean.toFixed(2)}` : '') +
    (yearsKnown.length ? ` | built year range ${yMin}–${yMax}` : ' | built year unknown/varied');

  const diff = computeFeatureDifferences(selection);
  drPlot.lastFeatureDiff = diff;

  if (drExplainMode === 'stats') {
    renderFeatureDiffPanel(diff);
  } else if (drExplainMode === 'model') {
    ensureModelExplanation(selection);
  }

  if (typeof refreshEnginePlot === 'function') {
    refreshEnginePlot();
  }

  // all coordinated city views get selection overlay
  renderFairnessHistogram(selection);
  renderDistrictBar(selection);
  renderHeightFairnessScatter(selection);
  renderLorenzCurve(selection);
  renderThresholdBar(selection);
  renderCategoryHists(selection);
}


// ======================= Unsupervised feature differences =======================

// Which numeric metrics to compare between selection and city
const DR_FEATURE_CONFIG = [
  { key: 'heights',      label: 'Height (m)' },
  { key: 'years',        label: 'Built year' },
  { key: 'overall',      label: 'Overall fairness (0–1)' },
  { key: 'fairGrocery',  label: 'Grocery fairness' },
  { key: 'fairHospital', label: 'Hospital fairness' },
  { key: 'fairPrimary',  label: 'Primary school fairness' },
  { key: 'fairPharmacy',     label: 'Pharmacy fairness' },
  { key: 'fairHealthcare',   label: 'Healthcare center fairness' },
  { key: 'fairKindergarten', label: 'Kindergarten fairness' },
  { key: 'fairSchoolHigh',   label: 'High school fairness' },
  { key: 'areaLog',      label: 'Footprint area (log m²)' },
  { key: 'changeScore',  label: 'Change score (S1)' },
];

// Small numeric helpers
function mean1(arr) {
  if (!arr.length) return NaN;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
function std1(arr, m) {
  if (!arr.length) return NaN;
  const mu = (m !== undefined && m !== null) ? m : mean1(arr);
  const v = arr.reduce((s, v) => s + (v - mu) * (v - mu), 0) / arr.length;
  return Math.sqrt(Math.max(v, 0));
}
function medianFromSorted(sorted) {
  const n = sorted.length;
  if (!n) return NaN;
  const mid = Math.floor(n / 2);
  if (n % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}
function quantileFromSorted(sorted, q) {
  const n = sorted.length;
  if (!n) return NaN;
  if (q <= 0) return sorted[0];
  if (q >= 1) return sorted[n - 1];
  const idx = (n - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const t = idx - lo;
  return sorted[lo] * (1 - t) + sorted[hi] * t;
}
function mad1(arr, med) {
  if (!arr.length) return NaN;
  const m = Number.isFinite(med) ? med : medianFromSorted(arr.slice().sort((a, b) => a - b));
  const devs = arr.map(v => Math.abs(v - m));
  return medianFromSorted(devs.sort((a, b) => a - b));
}

// 1D Kolmogorov–Smirnov distance between two sorted arrays
function ksDistance(sortedA, sortedB) {
  const nA = sortedA.length;
  const nB = sortedB.length;
  if (!nA || !nB) return 0;
  let i = 0, j = 0;
  let cdfA = 0, cdfB = 0;
  let dMax = 0;

  while (i < nA && j < nB) {
    const a = sortedA[i];
    const b = sortedB[j];
    if (a <= b) {
      i++;
      cdfA = i / nA;
    } else {
      j++;
      cdfB = j / nB;
    }
    const diff = Math.abs(cdfA - cdfB);
    if (diff > dMax) dMax = diff;
  }
  return dMax;
}

/**
 * Compute per-feature differences between selection and "city" (DR sample).
 * Returns features sorted by |effect size| descending.
 */
function computeFeatureDifferences(selectionIdx) {
  const metrics = drPlot.metrics || {};
  if (!selectionIdx || !selectionIdx.length) {
    return { features: [] };
  }

  const feats = [];

  for (const cfg of DR_FEATURE_CONFIG) {
    const arr = metrics[cfg.key];
    if (!arr || !arr.length) continue;

    const cityVals = arr.filter(Number.isFinite);
    if (cityVals.length < 5) continue; // too little data

    const selVals = selectionIdx
      .map(i => arr[i])
      .filter(Number.isFinite);
    if (!selVals.length) continue;

    const citySorted = cityVals.slice().sort((a, b) => a - b);
    const selSorted  = selVals.slice().sort((a, b) => a - b);

    const meanCity = mean1(cityVals);
    const meanSel  = mean1(selVals);
    const stdCity  = std1(cityVals, meanCity);

    const medCity  = medianFromSorted(citySorted);
    const medSel   = medianFromSorted(selSorted);
    const madCity  = mad1(cityVals, medCity);

    const effect   = stdCity > 0 ? (meanSel - meanCity) / stdCity : 0;
    const medEff   = madCity > 0 ? (medSel - medCity) / madCity : 0;
    const ks       = ksDistance(citySorted, selSorted);

    feats.push({
      key: cfg.key,
      label: cfg.label,
      nCity: cityVals.length,
      nSel: selVals.length,
      meanCity,
      meanSel,
      stdCity,
      medCity,
      medSel,
      effect,
      medEffect: medEff,
      ks
    });
  }

  feats.sort((a, b) => Math.abs(b.effect) - Math.abs(a.effect));
  return { features: feats };
}

/**
 * Render the top feature differences into the "Stats" panel.
 */
function renderFeatureDiffPanel(diff) {
  const { explainStatsEl } = ensureDRUI();
  if (!explainStatsEl) return;

  const feats = diff && diff.features ? diff.features : [];
  if (!feats.length) {
    explainStatsEl.textContent = 'No numeric features or selection is too small.';
    return;
  }

  const top = feats.slice(0, 5); // show top 5
  let html = '<table class="table table-sm table-borderless mb-0">';
  html += '<thead><tr>' +
    '<th class="text-muted small">Feature</th>' +
    '<th class="text-muted small">Median (sel / city)</th>' +
    '<th class="text-muted small">d (mean)</th>' +
    '<th class="text-muted small">KS</th>' +
    '</tr></thead><tbody>';

  for (const f of top) {
    const medSel  = Number.isFinite(f.medSel)  ? f.medSel.toFixed(f.key === 'years' ? 0 : 2) : '—';
    const medCity = Number.isFinite(f.medCity) ? f.medCity.toFixed(f.key === 'years' ? 0 : 2) : '—';
    const dMean   = Number.isFinite(f.effect)  ? f.effect.toFixed(2) : '—';
    const ks      = Number.isFinite(f.ks)      ? f.ks.toFixed(2) : '—';

    html += `<tr>
      <td>${f.label}</td>
      <td>${medSel} / ${medCity}</td>
      <td>${dMean}</td>
      <td>${ks}</td>
    </tr>`;
  }

  html += '</tbody></table>';
  explainStatsEl.innerHTML = html;
}

// ======================= Fairness histogram (overall fairness) =======================

function renderFairnessHistogram(selectionIdx) {
  const { histWrap } = ensureDRUI();
  if (!histWrap) return;

  if (typeof d3 === 'undefined') {
    histWrap.innerHTML = '<div class="small text-muted">Histogram requires d3.js.</div>';
    return;
  }

  // Make sure tooltip absolute positioning works
  const cs = getComputedStyle(histWrap);
  if (cs.position === 'static') {
    histWrap.style.position = 'relative';
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const n = overall.length;

  if (!n) {
    histWrap.innerHTML = '<div class="small text-muted mt-3">No overall fairness values available.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const allValues = overall.filter(v => Number.isFinite(v) && v >= 0 && v <= 1);
  const selectedValues = selection.length
    ? selection
        .filter(i => i >= 0 && i < overall.length)
        .map(i => overall[i])
        .filter(v => Number.isFinite(v) && v >= 0 && v <= 1)
    : [];

  const root = d3.select(histWrap);
  root.selectAll('*').remove();

  const width  = histWrap.clientWidth || 260;
  const height = histWrap.clientHeight || 140;
  const margin = { top: 6, right: 12, bottom: 40, left: 32 };

  const svg = root.append('svg')
    .attr('width',  width)
    .attr('height', height);

  // Tooltip (inside histWrap)
  const tooltip = root.append('div')
    .attr('class', 'dr-hist-tooltip')
    .style('position', 'absolute')
    .style('pointer-events', 'none')
    .style('background', 'rgba(0,0,0,0.85)')
    .style('color', '#fff')
    .style('padding', '4px 6px')
    .style('font-size', '11px')
    .style('border-radius', '4px')
    .style('z-index', '10')
    .style('opacity', 0);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .nice()
    .range([margin.left, width - margin.right]);

  const binGen = d3.bin()
    .domain(x.domain())
    .thresholds(15);

  const binsAll = binGen(allValues);
  const binsSel = binGen(selectedValues);

  const maxCount = d3.max([
    d3.max(binsAll, d => d.length) || 0,
    d3.max(binsSel, d => d.length) || 0
  ]) || 1;

  const y = d3.scaleLinear()
    .domain([0, maxCount])
    .nice()
    .range([height - margin.bottom, margin.top]);

  const totalAll = d3.sum(binsAll, b => b.length);
  const totalSel = d3.sum(binsSel, b => b.length);

  // City-wide distribution (gray)
  svg.append('g')
    .selectAll('rect.full')
    .data(binsAll)
    .join('rect')
    .attr('class', 'full')
    .attr('x', d => x(d.x0) + 1)
    .attr('y', d => y(d.length))
    .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 2))
    .attr('height', d => y(0) - y(d.length))
    .attr('fill', '#444')
    .attr('opacity', 0.4);

  // Selection overlay (green, narrower)
  if (selectedValues.length) {
    const widthFactor = 0.6;
    svg.append('g')
      .selectAll('rect.sel')
      .data(binsSel)
      .join('rect')
      .attr('class', 'sel')
      .attr('x', d => {
        const fullWidth = x(d.x1) - x(d.x0) - 2;
        const wSel = fullWidth * widthFactor;
        return x(d.x0) + 1 + (fullWidth - wSel) / 2;
      })
      .attr('y', d => y(d.length))
      .attr('width', d => {
        const fullWidth = x(d.x1) - x(d.x0) - 2;
        return Math.max(0, fullWidth * widthFactor);
      })
      .attr('height', d => y(0) - y(d.length))
      .attr('fill', '#2ecc71')
      .attr('opacity', 0.9);
  }

  // Hover hit-rectangles per bin (full vertical span)
  const hoverData = binsAll.map((b, i) => ({
    x0: b.x0,
    x1: b.x1,
    allCount: b.length,
    selCount: binsSel[i] ? binsSel[i].length : 0
  }));

  svg.append('g')
    .selectAll('rect.hit')
    .data(hoverData)
    .enter()
    .append('rect')
    .attr('class', 'hit')
    .attr('x', d => x(d.x0) + 1)
    .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 2))
    .attr('y', margin.top)
    .attr('height', y(0) - margin.top)
    .attr('fill', 'transparent')
    .on('mousemove', (event, d) => {
      const [mx, my] = d3.pointer(event, histWrap);
      const rangeText = `${d.x0.toFixed(2)} – ${d.x1.toFixed(2)}`;
      const pctAll = totalAll ? ((d.allCount / totalAll) * 100).toFixed(1) : '0.0';
      const pctSel = totalSel ? ((d.selCount / totalSel) * 100).toFixed(1) : '0.0';

      tooltip
        .style('left', `${mx + 8}px`)
        .style('top', `${my - 10}px`)
        .style('opacity', 1)
        .html(
          `<div><strong>Fairness bin:</strong> ${rangeText}</div>` +
          `<div>City buildings: ${d.allCount} (${pctAll}% of city)</div>` +
          `<div>Selected buildings: ${d.selCount} (${pctSel}% of selection)</div>`
        );
    })
    .on('mouseleave', () => {
      tooltip.style('opacity', 0);
    });

  // X-axis
  const xAxis = d3.axisBottom(x)
    .ticks(4)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Overall fairness (0–1)');

  if (selectedValues.length) {
    svg.append('text')
      .attr('x', margin.left)
      .attr('y', margin.top + 8)
      .attr('font-size', 9)
      .attr('fill', '#ccc')
      .text(`Selection: ${selectedValues.length} of ${allValues.length} buildings`);
  } else {
    svg.append('text')
      .attr('x', margin.left)
      .attr('y', margin.top + 8)
      .attr('font-size', 9)
      .attr('fill', '#ccc')
      .text(`City-wide distribution (${allValues.length} buildings)`);
  }
}

// ======================= Parallel coordinates (POI fairness) =======================
function currentParallelCoordsMode() {
  if (districtView) return 'district';
  if (mezoView) return 'mezo';
  return 'building';
}

function currentDRDataMode() {
  return currentParallelCoordsMode();
}

function spatialModeEntityStats(mode = currentDRDataMode()) {
  if (mode === 'district') {
    return { noun: 'districts', total: (districtFC?.features || []).length };
  }
  if (mode === 'mezo') {
    return { noun: 'mezo cells', total: Array.isArray(mezoHexData) ? mezoHexData.length : 0 };
  }
  return { noun: 'buildings', total: (baseCityFC?.features || []).length };
}

function updateDRAndPCBadges() {
  const drBadge = document.getElementById('drMaxBadge');
  const pcBadge = document.getElementById('parallelCoordsMaxBadge');
  const mode = currentDRDataMode();
  const { noun, total } = spatialModeEntityStats(mode);

  // --- DR input: default to total when 0 or missing ---
  const drMaxEl = document.getElementById('drMaxPts');
  let drRaw = parseInt(drMaxEl?.value || '0', 10);
  if (drRaw <= 0 || !Number.isFinite(drRaw)) {
    drRaw = total;
    if (drMaxEl) drMaxEl.value = String(total);
  }
  const drLabel = `${Math.min(drRaw, total).toLocaleString()} of ${total.toLocaleString()} ${noun}`;

  // --- PC input: default to total when 0 or missing ---
  const pcMaxEl = document.getElementById('parallelCoordsMaxPts');
  let pcRaw = parseInt(parallelCoordsMaxPoints, 10);
  if (pcRaw <= 0 || !Number.isFinite(pcRaw)) {
    pcRaw = total;
    parallelCoordsMaxPoints = total;
    if (pcMaxEl) pcMaxEl.value = String(total);
  }
  const pcLabel = `${Math.min(pcRaw, total).toLocaleString()} of ${total.toLocaleString()} ${noun}`;

  if (drBadge) drBadge.textContent = drLabel;
  if (pcBadge) pcBadge.textContent = pcLabel;
}

function maybeRefreshDROnSpatialModeChange(prevMode) {
  updateDRAndPCBadges();
  const nextMode = currentDRDataMode();
  if (prevMode === nextMode) return;

  if (!drPlot?.points) return;
  const { statusEl } = ensureDRUI();
  if (statusEl) statusEl.textContent = `Spatial mode switched to ${nextMode}; recomputing DR…`;
  runDR();
}

function setParallelCoordsPending(isPending) {
  parallelCoordsPending = isPending;
  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
}

function updateParallelCoordsDistrictFilterOptions() {
  const select = document.getElementById('parallelCoordsDistrictSelect');
  if (!select) return;

  if (!select.__bound) {
    select.addEventListener('change', () => {
      parallelCoordsDistrictFilter = select.value || '';
      updateParallelCoordsPanel();
    });
    select.__bound = true;
  }

  const feats = districtFC?.features || [];
  const options = [
    { value: '', label: 'All districts' },
    ...feats.map((feat, idx) => {
      const label = districtNameOf(feat.properties, idx) || `District ${idx + 1}`;
      return { value: label, label };
    })
  ];

  select.innerHTML = '';
  options.forEach(({ value, label }) => {
    const opt = document.createElement('option');
    opt.value = value;
    opt.textContent = label;
    select.appendChild(opt);
  });

  if (parallelCoordsDistrictFilter && !options.some(opt => opt.value === parallelCoordsDistrictFilter)) {
    parallelCoordsDistrictFilter = '';
  }
  select.value = parallelCoordsDistrictFilter;
}

function parallelCoordsModeLabel(mode) {
  if (mode === 'district') return 'districts';
  if (mode === 'mezo') return 'mezos';
  return 'buildings';
}

function getOrderedParallelCoordsCategories(categories) {
  const unique = Array.from(new Set((categories || []).filter(Boolean)));
  const hasOverall = unique.includes(PARALLEL_COORDS_OVERALL_KEY);
  const nonOverall = unique.filter(cat => cat !== PARALLEL_COORDS_OVERALL_KEY);
  if (!nonOverall.length) return hasOverall ? [PARALLEL_COORDS_OVERALL_KEY] : [];

  const ordered = parallelCoordsColumnOrder.filter(cat => nonOverall.includes(cat));
  nonOverall.forEach((cat) => {
    if (!ordered.includes(cat)) ordered.push(cat);
  });
  return hasOverall ? [PARALLEL_COORDS_OVERALL_KEY, ...ordered] : ordered;
}

function readFairValue(entry, cat) {
  if (!entry) return null;
  const val = entry[cat];
  if (Number.isFinite(val)) return val;
  if (val && Number.isFinite(val.score)) return val.score;
  return null;
}

function getParallelCoordsDataset(mode) {
  const allCategories = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const activeCats = resolveActivePOICategories();
  const categories = activeCats.size
    ? allCategories.filter(cat => activeCats.has(cat))
    : allCategories;
  const categoriesWithOverall = [PARALLEL_COORDS_OVERALL_KEY, ...categories];
  let rows = [];
  let total = 0;

  if (mode === 'district') {
    const feats = districtFC?.features || [];
    total = feats.length;
    rows = feats.map((f, idx) => ({
      id: f?.properties?.__districtName || `District ${idx + 1}`,
      label: f?.properties?.__districtName || `District ${idx + 1}`,
      values: f?.properties?.__fairByCat || {},
      source: f
    }));
    if (parallelCoordsDistrictFilter) {
      rows = rows.filter(row => row.id === parallelCoordsDistrictFilter);
    }
  } else if (mode === 'mezo') {
    const data = Array.isArray(mezoHexData) ? mezoHexData : [];
    total = data.length;
    rows = data.map((entry, idx) => ({
      id: entry?.hex || `Hex ${idx + 1}`,
      label: entry?.hex || `Hex ${idx + 1}`,
      values: entry?.__fairByCat || {},
      source: entry
    }));
  } else {
    const feats = baseCityFC?.features || [];
    const drSample = (drPlot.mode === mode && Array.isArray(drPlot.sample) && drPlot.sample.length)
      ? drPlot.sample
      : null;
    const sourceFeats = drSample || feats;
    total = sourceFeats.length;
    rows = sourceFeats.map((f, idx) => ({
      id: f?.properties?.id || f?.properties?.osm_id || f?.properties?.['@id'] || `B${idx + 1}`,
      label: f?.properties?.name || f?.properties?.id || f?.properties?.osm_id || f?.properties?.['@id'] || `Building ${idx + 1}`,
      values: f?.properties?.fair_multi || {},
      source: f
    }));
  }

  rows = rows.map((row) => {
    const values = {};
    let count = 0;
    const rawValues = [];
    let overall = null;
    if (mode === 'district' || mode === 'mezo') {
      overall = Number.isFinite(row?.source?.properties?.__fairOverall)
        ? row.source.properties.__fairOverall
        : Number.isFinite(row?.source?.__fairOverall)
          ? row.source.__fairOverall
          : null;
    } else {
      overall = Number.isFinite(row?.source?.properties?.fair_overall?.score)
        ? row.source.properties.fair_overall.score
        : null;
    }

    for (const cat of categories) {
      const val = readFairValue(row.values, cat);
      if (Number.isFinite(val)) count += 1;
      if (Number.isFinite(val)) rawValues.push(val);
      values[cat] = Number.isFinite(val) ? Math.max(0, Math.min(1, val)) : null;
    }
    const min = rawValues.length ? Math.min(...rawValues) : null;
    const max = rawValues.length ? Math.max(...rawValues) : null;
    const avg = rawValues.length ? rawValues.reduce((a, b) => a + b, 0) / rawValues.length : null;
    if (!Number.isFinite(overall)) overall = avg;
    values[PARALLEL_COORDS_OVERALL_KEY] = Number.isFinite(overall) ? Math.max(0, Math.min(1, overall)) : null;
    return { id: row.id, label: row.label || row.id, values, count, min, max, avg, source: row.source };
  }).filter(row => row.count > 0);

  const rawMaxPC = parseInt(parallelCoordsMaxPoints, 10);
  const maxLines = (rawMaxPC <= 0 || !Number.isFinite(rawMaxPC)) ? Infinity : Math.max(200, rawMaxPC);
  if (rows.length > maxLines) {
    const step = Math.ceil(rows.length / maxLines);
    rows = rows.filter((_, idx) => idx % step === 0);
  }

  return { rows, total, categories: categoriesWithOverall };
}

function isEntityDRSelected(entity) {
  if (!entity) return false;
  const props = entity.properties || entity;
  return !!props?._drSelected;
}

function resetParallelCoordsSelectionState({ forceEmpty = false } = {}) {
  parallelCoordsSelectionIds.clear();
  parallelCoordsBrushFilters = {};
  parallelCoordsBrushSelections = {};
  parallelCoordsForceEmptySelection = !!forceEmpty;
}

function clearParallelCoordsSelectionFromClearAction() {
  resetParallelCoordsSelectionState({ forceEmpty: true });
  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
}

function applyParallelCoordsSelection(selectedRows) {
  const rows = Array.isArray(selectedRows) ? selectedRows.filter(Boolean) : [];

  if (!rows.length) {
    clearDRMapSelection();
    applyDRSelection([], { skipMapSync: true, skipParallelSync: true });
    updateLayers();
    return;
  }

  const selectedEntities = rows.map((row) => row.source).filter(Boolean);
  applyMapSelection(selectedEntities, { skipDRSync: true, skipParallelSync: true });

  const drIndexByEntity = new Map();
  if (Array.isArray(drPlot.sample)) {
    drPlot.sample.forEach((entity, idx) => drIndexByEntity.set(entity, idx));
  }

  const drIdx = [];
  rows.forEach((row) => {
    const sampleIdx = drIndexByEntity.get(row.source);
    if (Number.isInteger(sampleIdx)) {
      drIdx.push(sampleIdx);
    }
  });

  if (drPlot.points) {
    applyDRSelection(drIdx, { skipMapSync: true, skipParallelSync: true });
  }
}

function updateParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel || panel.classList.contains('d-none')) return;

  const mode = currentParallelCoordsMode();
  const modeLabel = parallelCoordsModeLabel(mode);
  const modeEl = document.getElementById('parallelCoordsMode');
  if (modeEl) modeEl.textContent = `Mode: ${modeLabel}.`;

  const filterSelect = document.getElementById('parallelCoordsDistrictSelect');
  if (filterSelect) {
    const showFilter = mode === 'district';
    filterSelect.classList.toggle('d-none', !showFilter);
    if (showFilter) {
      updateParallelCoordsDistrictFilterOptions();
    } else if (parallelCoordsDistrictFilter) {
      parallelCoordsDistrictFilter = '';
    }
  }

  const { rows, total, categories } = getParallelCoordsDataset(mode);
  const orderedCategories = getOrderedParallelCoordsCategories(categories);
  parallelCoordsColumnOrder = orderedCategories.slice();

  if (!Object.keys(parallelCoordsBrushFilters).every(cat => orderedCategories.includes(cat))) {
    parallelCoordsBrushFilters = Object.fromEntries(
      Object.entries(parallelCoordsBrushFilters).filter(([cat]) => orderedCategories.includes(cat))
    );
  }
  if (!Object.keys(parallelCoordsBrushSelections).every(cat => orderedCategories.includes(cat))) {
    parallelCoordsBrushSelections = Object.fromEntries(
      Object.entries(parallelCoordsBrushSelections).filter(([cat]) => orderedCategories.includes(cat))
    );
  }
  Object.keys(parallelCoordsBrushSelections).forEach((cat) => {
    const filtered = new Set(
      Array.from(parallelCoordsBrushSelections[cat] || []).filter(id => rows.some(row => row.id === id))
    );
    if (filtered.size) parallelCoordsBrushSelections[cat] = filtered;
    else delete parallelCoordsBrushSelections[cat];
  });
  parallelCoordsSelectionIds = new Set(Array.from(parallelCoordsSelectionIds).filter(id => rows.some(row => row.id === id)));
  renderParallelCoords(rows, total, modeLabel, orderedCategories, { pending: parallelCoordsPending });
}

function renderParallelCoords(rows, total, modeLabel, categories, { pending = false } = {}) {
  const chart = document.getElementById('parallelCoordsChart');
  const note = document.getElementById('parallelCoordsNote');
  if (!chart || !note) return;

  if (typeof d3 === 'undefined') {
    chart.innerHTML = '<div class="small text-muted">Parallel coordinates require d3.js.</div>';
    note.textContent = '';
    return;
  }

  chart.innerHTML = '';
  chart.style.position = chart.style.position || 'relative';
  const available = rows.length;
  if (pending) {
    chart.innerHTML = '<div class="small text-muted mt-2">Computing POI fairness…</div>';
    note.textContent = '';
    return;
  }
  if (!available || !categories.length) {
    chart.innerHTML = '<div class="small text-muted mt-2">No POI fairness values yet. Select POIs or run overall fairness to populate per-POI scores.</div>';
    note.textContent = '';
    applyParallelCoordsSelection([]);
    return;
  }

  const viewportWidth = chart.clientWidth || 600;
  const height = chart.clientHeight || 170;
  const margin = { top: 10, right: 24, bottom: 26, left: 24 };
  const minAxisGap = 88;
  const minPlotWidth = margin.left + margin.right + Math.max(0, categories.length - 1) * minAxisGap;
  const width = Math.max(viewportWidth, minPlotWidth);

  const svg = d3.select(chart)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scalePoint()
    .domain(categories)
    .range([margin.left, width - margin.right]);

  const y = d3.scaleLinear()
    .domain([0, 1])
    .range([height - margin.bottom, margin.top]);

  const line = d3.line()
    .defined(d => Number.isFinite(d.value))
    .x(d => x(d.cat))
    .y(d => y(d.value));

  const lineData = rows.map(row => ({
    row,
    points: categories.map(cat => ({ cat, value: row.values[cat] }))
  }));

  const brushedCategories = () => Object.keys(parallelCoordsBrushFilters);
  const rowById = new Map(rows.map(row => [row.id, row]));
  const brushedRows = () => {
    const active = brushedCategories();
    if (!active.length) return [];

    let commonIds = null;
    active.forEach((cat) => {
      const ids = parallelCoordsBrushSelections[cat];
      if (!ids) {
        commonIds = new Set();
        return;
      }

      if (!commonIds) {
        commonIds = new Set(ids);
        return;
      }

      commonIds.forEach((id) => {
        if (!ids.has(id)) commonIds.delete(id);
      });
    });

    return Array.from(commonIds || []).map(id => rowById.get(id)).filter(Boolean);
  };

  const computeSelectedRows = () => {
    const brushed = brushedRows();
    if (brushed.length || brushedCategories().length) return brushed;
    if (parallelCoordsSelectionIds.size) {
      return rows.filter(r => parallelCoordsSelectionIds.has(r.id));
    }
    if (parallelCoordsForceEmptySelection) return [];
    const externallySelected = rows.filter(row => isEntityDRSelected(row.source));
    if (externallySelected.length) return externallySelected;
    return [];
  };

  let currentSelectedRows = computeSelectedRows();
  const selectedIds = () => new Set(currentSelectedRows.map(row => row.id));

  const tooltip = d3.select(chart)
    .append('div')
    .style('position', 'absolute')
    .style('pointer-events', 'none')
    .style('opacity', 0)
    .style('padding', '6px 8px')
    .style('background', 'rgba(20, 20, 20, 0.9)')
    .style('color', '#f2f2f2')
    .style('border-radius', '4px')
    .style('font-size', '11px');

  let lineAppendMode = false;
  const selectedLineColor = '#f16913';
  const defaultLineColor = '#1f77b4';
  const unselectedLineColor = '#b8b8b8';

  const linePaths = svg.append('g')
    .attr('fill', 'none')
    .attr('stroke-opacity', 0.25)
    .attr('stroke-width', 1)
    .selectAll('path')
    .data(lineData)
    .join('path')
    .attr('d', d => line(d.points))
    .style('cursor', 'pointer')
    .attr('stroke', d => selectedIds().size
      ? (selectedIds().has(d.row.id) ? selectedLineColor : unselectedLineColor)
      : defaultLineColor)
    .attr('stroke-opacity', d => selectedIds().size ? (selectedIds().has(d.row.id) ? 0.95 : 0.2) : 0.25)
    .attr('stroke-width', d => selectedIds().has(d.row.id) ? 2.2 : 1)
    .on('pointerdown mousedown', (event) => {
      lineAppendMode = isAdditiveSelectionEvent(event?.sourceEvent || event);
    })
    .on('click', (event, d) => {
      parallelCoordsBrushFilters = {};
      parallelCoordsBrushSelections = {};
      const append = lineAppendMode || isAdditiveSelectionEvent(event?.sourceEvent || event);
      lineAppendMode = false;
      const next = append ? new Set(parallelCoordsSelectionIds) : new Set();
      if (next.has(d.row.id)) next.delete(d.row.id);
      else next.add(d.row.id);
      parallelCoordsSelectionIds = next;
      parallelCoordsForceEmptySelection = parallelCoordsSelectionIds.size === 0;
      renderParallelCoords(rows, total, modeLabel, categories);
    })
    .on('mousemove', (event, d) => {
      const [mx, my] = d3.pointer(event, chart);
      const valueRows = categories.map(cat => {
        const val = d.row.values[cat];
        const pct = Number.isFinite(val) ? `${Math.round(val * 100)}%` : '—';
        return `<div>${prettyPOIName(cat)}: ${pct}</div>`;
      }).join('');
      tooltip
        .style('left', `${mx + 10}px`)
        .style('top', `${my - 10}px`)
        .style('opacity', 1)
        .html(`<div><strong>${d.row.label}</strong></div>${valueRows}`);
    })
    .on('mouseleave', () => {
      tooltip.style('opacity', 0);
    });

  const pointGroup = svg.append('g')
    .attr('fill', '#1f77b4')
    .attr('opacity', 0.25);

  pointGroup.selectAll('g')
    .data(lineData)
    .join('g')
    .selectAll('circle')
    .data(d => d.points.filter(p => Number.isFinite(p.value)))
    .join('circle')
    .attr('cx', d => x(d.cat))
    .attr('cy', d => y(d.value))
    .attr('r', 1.4);

  const updateSelectionStyles = ({ sync = false } = {}) => {
    const idSet = selectedIds();
    linePaths
      .attr('stroke', d => idSet.size
        ? (idSet.has(d.row.id) ? selectedLineColor : unselectedLineColor)
        : defaultLineColor)
      .attr('stroke-opacity', d => idSet.size ? (idSet.has(d.row.id) ? 0.95 : 0.2) : 0.25)
      .attr('stroke-width', d => idSet.has(d.row.id) ? 2.2 : 1)
      .sort((a, b) => {
        const aSelected = idSet.has(a.row.id) ? 1 : 0;
        const bSelected = idSet.has(b.row.id) ? 1 : 0;
        return aSelected - bSelected;
      });

    pointGroup.attr('opacity', idSet.size ? 0.3 : 0.25);
    if (sync) applyParallelCoordsSelection(currentSelectedRows);
  };

  const axisGroup = svg.append('g');
  categories.forEach((cat) => {
    const gx = axisGroup.append('g')
      .attr('transform', `translate(${x(cat)},0)`);
    gx.call(d3.axisLeft(y).ticks(4).tickSize(0));
    gx.selectAll('text').attr('font-size', 9);
    gx.selectAll('path').attr('stroke', '#999');
    gx.selectAll('line').remove();
    const axisLabel = gx.append('text')
      .attr('y', height - margin.bottom + 14)
      .attr('text-anchor', 'middle')
      .attr('font-size', 9)
      .attr('fill', '#666')
      .style('cursor', 'grab')
      .text(prettyPOIName(cat));

    axisLabel.call(
      d3.drag()
        .on('start', function () {
          d3.select(this).style('cursor', 'grabbing');
        })
        .on('drag', function (event) {
          d3.select(this).attr('transform', `translate(${event.x - x(cat)},0)`);
        })
        .on('end', function (event) {
          d3.select(this).style('cursor', 'grab').attr('transform', null);
          const currentOrder = categories.slice();
          if (currentOrder.length < 2) return;

          const nearestCat = currentOrder.reduce((best, candidate) => {
            const dist = Math.abs((x(candidate) ?? 0) - event.x);
            if (!best || dist < best.dist) return { cat: candidate, dist };
            return best;
          }, null)?.cat;

          if (!nearestCat || nearestCat === cat) return;

          const fromIdx = currentOrder.indexOf(cat);
          const toIdx = currentOrder.indexOf(nearestCat);
          if (fromIdx < 0 || toIdx < 0 || fromIdx === toIdx) return;

          currentOrder.splice(fromIdx, 1);
          currentOrder.splice(toIdx, 0, cat);
          parallelCoordsColumnOrder = currentOrder;
          updateParallelCoordsPanel();
        })
    );

    const brush = d3.brushY()
      .extent([[x(cat) - 12, margin.top], [x(cat) + 12, height - margin.bottom]])
      .on('brush end', (event) => {
        const append = isAdditiveSelectionEvent(event?.sourceEvent || event);
        if (!event.selection) {
          delete parallelCoordsBrushFilters[cat];
          delete parallelCoordsBrushSelections[cat];
        } else {
          const [y0, y1] = event.selection;
          const minVal = Math.max(0, Math.min(1, y.invert(y1)));
          const maxVal = Math.max(0, Math.min(1, y.invert(y0)));
          parallelCoordsBrushFilters[cat] = [Math.min(minVal, maxVal), Math.max(minVal, maxVal)];
          const idsInBrush = rows
            .filter((row) => {
              const val = row.values?.[cat];
              if (!Number.isFinite(val)) return false;
              const [minV, maxV] = parallelCoordsBrushFilters[cat];
              return val >= minV && val <= maxV;
            })
            .map(row => row.id);

          const priorIds = append ? (parallelCoordsBrushSelections[cat] || new Set()) : new Set();
          const mergedIds = new Set(priorIds);
          idsInBrush.forEach((id) => mergedIds.add(id));
          parallelCoordsBrushSelections[cat] = mergedIds;
        }

        if (Object.keys(parallelCoordsBrushFilters).length) {
          if (!append) parallelCoordsSelectionIds.clear();
          parallelCoordsForceEmptySelection = false;
        } else if (!parallelCoordsSelectionIds.size) {
          parallelCoordsForceEmptySelection = true;
        }

        currentSelectedRows = computeSelectedRows();
        if (append && parallelCoordsSelectionIds.size) {
          const brushedIdSet = new Set(currentSelectedRows.map(row => row.id));
          currentSelectedRows = rows.filter((row) =>
            parallelCoordsSelectionIds.has(row.id) || brushedIdSet.has(row.id)
          );
        }
        updateSelectionStyles({ sync: true });
      });

    const brushGroup = axisGroup.append('g').attr('class', `pc-brush-${cat}`);
    brushGroup.call(brush);
    const existingRange = parallelCoordsBrushFilters[cat];
    if (existingRange) {
      const [minVal, maxVal] = existingRange;
      brushGroup.call(brush.move, [y(maxVal), y(minVal)]);
    }
  });

  const scrollHint = width > viewportWidth ? ' Scroll horizontally to see all categories.' : '';
  const sampledNote = available < total
    ? `Showing ${available} of ${total} ${modeLabel} (sampled).`
    : `Showing ${available} ${modeLabel}.`;

  if (currentSelectedRows.length) {
    const selCount = currentSelectedRows.length;
    const selectedAvgValues = currentSelectedRows.map(r => r.avg).filter(Number.isFinite);
    const avg = selectedAvgValues.length
      ? selectedAvgValues.reduce((a, b) => a + b, 0) / selectedAvgValues.length
      : null;
    const avgText = Number.isFinite(avg) ? `${Math.round(avg * 100)}%` : '—';
    const brushText = brushedCategories().length
      ? ' (axis brush intersection)'
      : (parallelCoordsSelectionIds.size ? ' (line pick)' : '');
    note.textContent = `${sampledNote}${scrollHint} Selected: ${selCount} ${modeLabel}${brushText} · Mean fairness ${avgText}.`;
    updateSelectionStyles({ sync: true });
    return;
  }

  updateSelectionStyles({ sync: true });
  note.textContent = `${sampledNote}${scrollHint}`;
}

// ======================= Fairness by district / hex bar chart =======================
function renderDistrictBar(selectionIdx) {
  const { districtBarWrap } = ensureDRUI();
  if (!districtBarWrap) return;

  if (typeof d3 === 'undefined') {
    districtBarWrap.innerHTML =
      '<div class="small text-muted">District bar chart requires d3.js.</div>';
    return;
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample  = drPlot.sample  || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    districtBarWrap.innerHTML =
      '<div class="small text-muted">No DR sample available yet.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const selSet = new Set(selection);

  const agg = new Map(); // name -> { name, sum, count, selSum, selCount }

  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;

    const feat = sample[i];

    // 🔹 NEW: group by usage type (category) instead of district
    const nameRaw = inferCategoryGroup(feat && feat.properties);
    if (!nameRaw) continue; // skip if we really have no category

    const name = String(nameRaw).trim();
    if (!name) continue;

    let e = agg.get(name);
    if (!e) {
      e = { name, sum: 0, count: 0, selSum: 0, selCount: 0 };
      agg.set(name, e);
    }
    e.sum += s;
    e.count += 1;
    if (selSet.has(i)) {
      e.selSum += s;
      e.selCount += 1;
    }
  }

  // Less strict so the chart actually appears; require at least 3 buildings per category
  let rows = Array.from(agg.values()).filter(r => r.count >= 3);

  if (!rows.length) {
    districtBarWrap.innerHTML =
      '<div class="small text-muted">Too few buildings per category to show a chart.</div>';
    return;
  }

  rows.forEach(r => {
    r.meanAll = r.sum / r.count;
    r.meanSel = r.selCount ? (r.selSum / r.selCount) : null;
  });

  const hasSel = selection.length > 0;
  rows.sort((a, b) => {
    const va = (hasSel && a.meanSel != null) ? a.meanSel : a.meanAll;
    const vb = (hasSel && b.meanSel != null) ? b.meanSel : b.meanAll;
    return va - vb; // worst at top
  });

  const maxBars = 12;
  const data = rows.slice(0, maxBars);

  const root = d3.select(districtBarWrap);
  root.selectAll('*').remove();

  const width  = districtBarWrap.clientWidth || 260;
  const height = districtBarWrap.clientHeight || 170;
  const margin = { top: 6, right: 10, bottom: 24, left: 80 };

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleBand()
    .domain(data.map(d => d.name))
    .range([margin.top, height - margin.bottom])
    .padding(0.15);

  // Gray bars = city mean
  svg.append('g')
    .selectAll('rect.all')
    .data(data)
    .join('rect')
    .attr('class', 'all')
    .attr('x', x(0))
    .attr('y', d => y(d.name))
    .attr('width', d => Math.max(0, x(d.meanAll) - x(0)))
    .attr('height', y.bandwidth())
    .attr('fill', '#555')
    .attr('opacity', 0.4);

  // Green overlay = selection mean (if any)
  if (hasSel) {
    svg.append('g')
      .selectAll('rect.sel')
      .data(data.filter(d => d.meanSel != null))
      .join('rect')
      .attr('class', 'sel')
      .attr('x', x(0))
      .attr('y', d => y(d.name) + y.bandwidth() * 0.2)
      .attr('width', d => Math.max(0, x(d.meanSel) - x(0)))
      .attr('height', y.bandwidth() * 0.6)
      .attr('fill', '#2ecc71')
      .attr('opacity', 0.9);
  }

  // Y-axis (usage type names)
  svg.append('g')
    .attr('transform', `translate(${margin.left - 4},0)`)
    .call(d3.axisLeft(y).tickSize(0))
    .call(g => {
      g.selectAll('text').attr('font-size', 9);
      g.selectAll('path, line').remove();
    });

  // X-axis
  const xAxis = d3.axisBottom(x)
    .ticks(3)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Mean overall fairness (0–1)');
}




// ======================= usage / category vs fairness scatterplot =======================

function renderHeightFairnessScatter(selectionIdx) {
  const { scatterHeightWrap } = ensureDRUI();
  if (!scatterHeightWrap) return;

  if (typeof d3 === 'undefined') {
    scatterHeightWrap.innerHTML =
      '<div class="small text-muted">Scatterplot requires d3.js.</div>';
    return;
  }

  // Make sure tooltip positioning is relative to this container
  const cs = getComputedStyle(scatterHeightWrap);
  if (cs.position === 'static') {
    scatterHeightWrap.style.position = 'relative';
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample  = drPlot.sample  || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    scatterHeightWrap.innerHTML =
      '<div class="small text-muted">No DR sample available yet.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const selSet = new Set(selection);

  // Build points: category / usage type + fairness
  const points = [];
  for (let i = 0; i < n; i++) {
    const f = overall[i];
    const feat = sample[i];
    if (!Number.isFinite(f) || !feat) continue;

    // Try multiple possible property names for usage/category
    let cat =
      feat.properties?.usage ||
      feat.properties?.bldg_usage ||
      feat.properties?.category ||
      feat.properties?.objekttyp ||
      'Unknown';

    if (typeof cat === 'string') {
      cat = cat.trim();
      if (!cat) cat = 'Unknown';
    } else {
      cat = String(cat);
    }

    points.push({
      idx: i,
      cat,
      f,
      feat,
      selected: selSet.has(i)
    });
  }

  if (!points.length) {
    scatterHeightWrap.innerHTML =
      '<div class="small text-muted">No category / fairness values to plot.</div>';
    return;
  }

  const root = d3.select(scatterHeightWrap);
  root.selectAll('*').remove();

  const width  = scatterHeightWrap.clientWidth || 260;
  const height = scatterHeightWrap.clientHeight || 190;
  const margin = { top: 6, right: 12, bottom: 46, left: 34 }; // a bit more bottom space for rotated labels

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  // X: categorical usage / category
  const categories = Array.from(new Set(points.map(d => d.cat)));
  const x = d3.scaleBand()
    .domain(categories)
    .range([margin.left, width - margin.right])
    .padding(0.3);

  // Y: fairness 0–1
  const y = d3.scaleLinear()
    .domain([0, 1])
    .nice()
    .range([height - margin.bottom, margin.top]);

  // All points (gray)
  svg.append('g')
    .selectAll('circle.base')
    .data(points)
    .join('circle')
    .attr('class', 'base')
    .attr('cx', d => x(d.cat) + x.bandwidth() / 2)
    .attr('cy', d => y(d.f))
    .attr('r', 2)
    .attr('fill', '#666')
    .attr('opacity', 0.4);

  // Selection overlay (green, larger)
  if (selection.length) {
    svg.append('g')
      .selectAll('circle.sel')
      .data(points.filter(d => d.selected))
      .join('circle')
      .attr('class', 'sel')
      .attr('cx', d => x(d.cat) + x.bandwidth() / 2)
      .attr('cy', d => y(d.f))
      .attr('r', 3.5)
      .attr('fill', '#2ecc71')
      .attr('stroke', '#111')
      .attr('stroke-width', 0.5)
      .attr('opacity', 0.95);
  }

  // Axes
  const xAxis = d3.axisBottom(x).tickSizeOuter(0);
  const yAxis = d3.axisLeft(y).ticks(4).tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text')
        .attr('font-size', 8)
        .attr('transform', 'rotate(-30)')
        .style('text-anchor', 'end');
    });

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(yAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - 4)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Category / usage type');

  svg.append('text')
    .attr('transform', 'rotate(-90)')
    .attr('x', -((margin.top + height - margin.bottom) / 2))
    .attr('y', 10)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Overall fairness (0–1)');

  // Hover tooltip (for selected points)
  const tooltip = root.append('div')
    .attr('class', 'dr-hist-tooltip')
    .style('position', 'absolute')
    .style('pointer-events', 'none')
    .style('background', 'rgba(0,0,0,0.8)')
    .style('color', '#fff')
    .style('padding', '2px 6px')
    .style('border-radius', '3px')
    .style('font-size', '10px')
    .style('display', 'none');

  svg.selectAll('circle.sel')
    .on('mousemove', function (event, d) {
      const name = d.feat?.properties?.name ||
                   d.feat?.properties?.category ||
                   d.feat?.properties?.objekttyp ||
                   'Building';
      const [mx, my] = d3.pointer(event, scatterHeightWrap);
      tooltip
        .style('left', `${mx + 12}px`)
        .style('top', `${my + 12}px`)
        .style('display', 'block')
        .html(
          `${name}<br/>` +
          `category: ${d.cat}<br/>` +
          `fairness ≈ ${d.f.toFixed(2)}`
        );
    })
    .on('mouseleave', () => {
      tooltip.style('display', 'none');
    });
}



// ======================= Lorenz curve of lack-of-access =======================
function renderLorenzCurve(selectionIdx) {
  const { lorenzWrap } = ensureDRUI();
  if (!lorenzWrap) return;

  if (typeof d3 === 'undefined') {
    lorenzWrap.innerHTML =
      '<div class="small text-muted">Lorenz curve requires d3.js.</div>';
    return;
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const n = overall.length;

  if (!n) {
    lorenzWrap.innerHTML =
      '<div class="small text-muted">No overall fairness values available.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];

  const allLack = overall
    .filter(Number.isFinite)
    .map(v => 1 - v); // lack-of-access

  const selLack = selection.length
    ? selection
        .filter(i => i >= 0 && i < overall.length)
        .map(i => overall[i])
        .filter(Number.isFinite)
        .map(v => 1 - v)
    : [];

  function buildLorenz(values) {
    const vals = values.filter(Number.isFinite).slice().sort((a, b) => a - b);
    const n = vals.length;
    if (!n) return null;
    const total = vals.reduce((a, b) => a + b, 0) || 1;
    let cum = 0;
    const pts = [{ x: 0, y: 0 }];
    for (let i = 0; i < n; i++) {
      cum += vals[i];
      pts.push({ x: (i + 1) / n, y: cum / total });
    }
    return pts;
  }

  const allCurve = buildLorenz(allLack);
  const selCurve = buildLorenz(selLack);

  if (!allCurve) {
    lorenzWrap.innerHTML =
      '<div class="small text-muted">Not enough values for Lorenz curve.</div>';
    return;
  }

  const root = d3.select(lorenzWrap);
  root.selectAll('*').remove();

  const width  = lorenzWrap.clientWidth || 260;
  const height = lorenzWrap.clientHeight || 170;
  const margin = { top: 6, right: 10, bottom: 24, left: 28 };

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleLinear()
    .domain([0, 1])
    .range([height - margin.bottom, margin.top]);

  // Diagonal (perfect equality)
  svg.append('line')
    .attr('x1', x(0))
    .attr('y1', y(0))
    .attr('x2', x(1))
    .attr('y2', y(1))
    .attr('stroke', '#777')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '4,3')
    .attr('opacity', 0.8);

  const lineAll = d3.line()
    .x(d => x(d.x))
    .y(d => y(d.y));

  // City curve (gray)
  svg.append('path')
    .datum(allCurve)
    .attr('fill', 'none')
    .attr('stroke', '#cccccc')
    .attr('stroke-width', 1.5)
    .attr('d', lineAll);

  // Selection curve (green)
  if (selCurve && selCurve.length) {
    svg.append('path')
      .datum(selCurve)
      .attr('fill', 'none')
      .attr('stroke', '#2ecc71')
      .attr('stroke-width', 1.5)
      .attr('d', lineAll);
  }

  const axis = d3.axisBottom(x)
    .ticks(3)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(axis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  const yAxis = d3.axisLeft(y)
    .ticks(3)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(yAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Cumulative share of buildings');

  svg.append('text')
    .attr('transform', 'rotate(-90)')
    .attr('x', -((margin.top + height - margin.bottom) / 2))
    .attr('y', 10)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Cumulative share of lack-of-access');
}


// ======================= Threshold bar chart per district =======================
function renderThresholdBar(selectionIdx) {
  const { thresholdBarWrap, thresholdSlider } = ensureDRUI();
  if (!thresholdBarWrap || !thresholdSlider) return;

  if (typeof d3 === 'undefined') {
    thresholdBarWrap.innerHTML =
      '<div class="small text-muted">Threshold bar chart requires d3.js.</div>';
    return;
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample  = drPlot.sample  || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    thresholdBarWrap.innerHTML =
      '<div class="small text-muted">No DR sample available yet.</div>';
    return;
  }

  const thr = parseFloat(thresholdSlider.value || '0.5') || 0.5;

  const agg = new Map(); // name -> { name, total, below }

  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;

    const feat = sample[i];

    // 🔹 Use usage type / category instead of district
    const nameRaw = inferCategoryGroup(feat.properties);
    if (!nameRaw) continue;

    const name = String(nameRaw).trim();
    if (!name) continue;

    let e = agg.get(name);
    if (!e) {
      e = { name, total: 0, below: 0 };
      agg.set(name, e);
    }
    e.total += 1;
    if (s < thr) e.below += 1;
  }

  // Only keep categories with enough buildings
  let rows = Array.from(agg.values()).filter(r => r.total >= 5);

  if (!rows.length) {
    thresholdBarWrap.innerHTML =
      '<div class="small text-muted">Too few buildings per category to show shares.</div>';
    return;
  }

  rows.forEach(r => {
    r.share = r.total ? r.below / r.total : 0;
  });

  // Highest share (worst) at top
  rows.sort((a, b) => b.share - a.share);

  const maxBars = 12;
  const data = rows.slice(0, maxBars);

  const root = d3.select(thresholdBarWrap);
  root.selectAll('*').remove();

  const width  = thresholdBarWrap.clientWidth || 260;
  const height = thresholdBarWrap.clientHeight || 170;
  const margin = { top: 6, right: 10, bottom: 24, left: 80 };

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleBand()
    .domain(data.map(d => d.name))
    .range([margin.top, height - margin.bottom])
    .padding(0.15);

  svg.append('g')
    .selectAll('rect')
    .data(data)
    .join('rect')
    .attr('x', x(0))
    .attr('y', d => y(d.name))
    .attr('width', d => Math.max(0, x(d.share) - x(0)))
    .attr('height', y.bandwidth())
    .attr('fill', '#e67e22')
    .attr('opacity', 0.9);

  svg.append('g')
    .attr('transform', `translate(${margin.left - 4},0)`)
    .call(d3.axisLeft(y).tickSize(0))
    .call(g => {
      g.selectAll('text').attr('font-size', 9);
      g.selectAll('path, line').remove();
    });

  const xAxis = d3.axisBottom(x)
    .ticks(3)
    .tickFormat(d3.format('.0%'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    // 🔹 Update label to mention "usage type" or "category" if you want
    .text(`Share of buildings with fairness < ${thr.toFixed(2)}`);
}

// ======================= buildThresholdSummaryForLLM =======================



// ======================= LLM helpers =======================

const LLM_SUMMARY_BUILDERS = {
  umap: buildUMAPSummaryForLLM,
  engine_ebm: buildEBMSummaryForLLM,
  engine_contrast: buildContrastiveSummaryForLLM,
  stats: buildStatsSummaryForLLM,
  model: buildModelSummaryForLLM,
  overall_hist: buildOverallHistSummaryForLLM,
  building_type: buildBuildingTypeSummaryForLLM,
  scatter: buildScatterSummaryForLLM,
  lorenz: buildLorenzSummaryForLLM,
  threshold_bar: buildThresholdSummaryForLLM,
  category_hists: buildCategoryHistsSummaryForLLM,
};

function quantileSorted(arr, q) {
  if (!arr.length) return null;
  const idx = (arr.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return arr[lo];
  const h = idx - lo;
  return arr[lo] * (1 - h) + arr[hi] * h;
}

function buildUMAPSummaryForLLM() {
  const total = drPlot.sample?.length || 0;
  if (!total) return { error: 'No DR embedding available yet.' };

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const algo = document.getElementById('drAlgo')?.value || 'umap';
  const colorBy = document.getElementById('drColorBy')?.value || 'overall';

  return {
    algo,
    color_by: colorBy,
    total_points: total,
    selection_size: selection.length,
  };
}

function buildEBMSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'Select some buildings to train the EBM.' };

  const engine = drPlot.engineEBM;
  const ranked = engine?.ranked || [];

  const top = ranked.slice(0, 6).map((f) => ({
    feature: f.label,
    score: Number.isFinite(f.score) ? Number(f.score.toFixed(3)) : f.score,
    direction: f.direction || null,
  }));

  return {
    engine: 'ebm',
    selection_size: selection.length,
    training: {
      total: engine?.nTrain ?? null,
      positives: engine?.nPos ?? null,
      negatives: engine?.nNeg ?? null,
    },
    top_features: top,
    note: engine?.note || 'EBM not run yet (will train on selection).',
  };
}

function buildContrastiveSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'Select some buildings to compare distributions.' };

  if (!drPlot.engineContrast) {
    drPlot.engineContrast = buildContrastiveEngineExplanation();
  }

  const engine = drPlot.engineContrast;
  if (!engine || !engine.ranked?.length) return { error: 'Contrastive distribution has no features.' };

  const top = engine.ranked.slice(0, 6).map((f) => ({
    feature: f.label,
    score: Number.isFinite(f.score) ? Number(f.score.toFixed(3)) : f.score,
    direction: f.direction,
  }));

  return {
    engine: 'contrastive',
    selection_size: selection.length,
    top_features: top,
    note: engine.note,
  };
}

function buildStatsSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'No selection yet for stats.' };

  if (!drPlot.lastFeatureDiff) {
    drPlot.lastFeatureDiff = computeFeatureDifferences(selection);
  }

  const feats = drPlot.lastFeatureDiff?.features || [];
  if (!feats.length) return { error: 'No numeric features available.' };

  const top = feats.slice(0, 6).map((f) => ({
    feature: f.label,
    mean_city: Number.isFinite(f.meanCity) ? Number(f.meanCity.toFixed(3)) : null,
    mean_selection: Number.isFinite(f.meanSel) ? Number(f.meanSel.toFixed(3)) : null,
    effect_size: Number.isFinite(f.effect) ? Number(f.effect.toFixed(3)) : null,
    ks_distance: Number.isFinite(f.ks) ? Number(f.ks.toFixed(3)) : null,
  }));

  return {
    selection_size: selection.length,
    top_features: top,
  };
}

function buildModelSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'No selection yet for the model view.' };

  if (!drPlot.lastModelExplanation) {
    drPlot.lastModelExplanation = trainLogisticSelectionModel(selection);
  }

  const model = drPlot.lastModelExplanation;
  if (!model || model.error) {
    return { error: model?.error || 'Model explanation unavailable.' };
  }

  const top = (model.ranked || []).slice(0, 6).map((f) => ({
    feature: f.label,
    weight: Number.isFinite(f.absWeight) ? Number(f.absWeight.toFixed(3)) : null,
    direction: f.direction || null,
  }));

  return {
    selection_size: selection.length,
    training: {
      positives: model.nPos ?? selection.length,
      negatives: model.nNeg ?? null,
    },
    top_features: top,
  };
}

function buildOverallHistSummaryForLLM() {
  const overallRaw = drPlot.metrics?.overall || [];
  const values = overallRaw.filter((v) => Number.isFinite(v) && v >= 0 && v <= 1);
  if (!values.length) return { error: 'No overall fairness values available.' };

  const sorted = values.slice().sort((a, b) => a - b);
  const mean = mean1(values);
  const median = quantileSorted(sorted, 0.5);

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const selVals = selection
    .map((i) => (i >= 0 && i < overallRaw.length ? overallRaw[i] : null))
    .filter((v) => Number.isFinite(v));

  const selSorted = selVals.slice().sort((a, b) => a - b);

  return {
    total: values.length,
    mean_city: Number.isFinite(mean) ? Number(mean.toFixed(3)) : null,
    median_city: Number.isFinite(median) ? Number(median.toFixed(3)) : null,
    selection_size: selection.length,
    selection_mean: selVals.length ? Number(mean1(selVals).toFixed(3)) : null,
    selection_median: selSorted.length ? Number(quantileSorted(selSorted, 0.5).toFixed(3)) : null,
  };
}

function buildBuildingTypeSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample = drPlot.sample || [];
  const n = Math.min(overall.length, sample.length);
  if (!n) return { error: 'No DR sample available yet.' };

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const selSet = new Set(selection);

  const agg = new Map();
  for (let i = 0; i < n; i++) {
    const score = overall[i];
    if (!Number.isFinite(score)) continue;

    const feat = sample[i];
    const nameRaw = inferCategoryGroup(feat && feat.properties);
    if (!nameRaw) continue;
    const name = String(nameRaw).trim();
    if (!name) continue;

    let e = agg.get(name);
    if (!e) {
      e = { name, sum: 0, count: 0, selSum: 0, selCount: 0 };
      agg.set(name, e);
    }
    e.sum += score;
    e.count += 1;
    if (selSet.has(i)) {
      e.selSum += score;
      e.selCount += 1;
    }
  }

  const rows = Array.from(agg.values()).filter((r) => r.count >= 3);
  if (!rows.length) return { error: 'Too few buildings per category to summarize.' };

  rows.forEach((r) => {
    r.meanAll = r.sum / r.count;
    r.meanSel = r.selCount ? r.selSum / r.selCount : null;
  });

  const hasSel = selection.length > 0;
  rows.sort((a, b) => {
    const va = hasSel && Number.isFinite(a.meanSel) ? a.meanSel : a.meanAll;
    const vb = hasSel && Number.isFinite(b.meanSel) ? b.meanSel : b.meanAll;
    return va - vb;
  });

  const top = rows.slice(0, 8).map((r) => ({
    category: r.name,
    count: r.count,
    mean_city: Number(r.meanAll.toFixed(3)),
    mean_selection: Number.isFinite(r.meanSel) ? Number(r.meanSel.toFixed(3)) : null,
  }));

  return {
    selection_size: selection.length,
    categories: top,
  };
}

function buildScatterSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample = drPlot.sample || [];
  const n = Math.min(overall.length, sample.length);
  if (!n) return { error: 'No DR sample available yet.' };

  const selection = Array.isArray(drPlot.lastSelectionIdx)
    ? new Set(drPlot.lastSelectionIdx)
    : new Set();

  const cats = new Map();
  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;
    const feat = sample[i];
    const name = inferCategoryGroup(feat.properties) || 'Unknown';
    let e = cats.get(name);
    if (!e) {
      e = { name, count: 0, fairness: [], selected: [] };
      cats.set(name, e);
    }
    e.count += 1;
    e.fairness.push(s);
    if (selection.has(i)) e.selected.push(s);
  }

  const categories = Array.from(cats.values()).map((c) => ({
    name: c.name,
    count: c.count,
    mean_fairness: c.fairness.reduce((a, b) => a + b, 0) / c.fairness.length,
    selected_count: c.selected.length,
    selected_mean: c.selected.length
      ? c.selected.reduce((a, b) => a + b, 0) / c.selected.length
      : null,
  }));

  const overallMean = categories.reduce((sum, c) => sum + c.mean_fairness * c.count, 0) /
    (categories.reduce((s, c) => s + c.count, 0) || 1);

  return {
    total_points: n,
    categories,
    overall_mean_fairness: overallMean,
    selection_size: selection.size,
  };
}

function buildLorenzSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = (metrics.overall || []).filter(Number.isFinite).sort((a, b) => a - b);
  if (!overall.length) return { error: 'No overall fairness values for Lorenz curve.' };

  const gini = overallGini != null ? overallGini : null;
  return {
    total_buildings: overall.length,
    gini: gini,
    p10: quantileSorted(overall, 0.1),
    median: quantileSorted(overall, 0.5),
    p90: quantileSorted(overall, 0.9),
  };
}

function buildThresholdSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample = drPlot.sample || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    return { threshold: null, total_buildings: 0, below_threshold: 0, share_below: 0, note: 'no data' };
  }

  const sliderEl = document.getElementById('drThresholdSlider') || document.getElementById('thresholdSlider');
  const thr = parseFloat(sliderEl?.value || '0.5') || 0.5;

  let total = 0;
  let below = 0;
  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;
    total++;
    if (s < thr) below++;
  }

  const share = total ? below / total : 0;

  return {
    threshold: thr,
    total_buildings: total,
    below_threshold: below,
    share_below: share,
  };
}

function perCategorySeriesWithFocusedFallback(metricKey) {
  const metrics = drPlot.metrics || {};
  const sample = Array.isArray(drPlot.sample) ? drPlot.sample : [];
  const base = Array.isArray(metrics[metricKey]) ? metrics[metricKey] : [];
  const expectedCat = {
    fairGrocery: 'grocery',
    fairHospital: 'hospital',
    fairPrimary: 'school_primary',
    fairPharmacy: 'pharmacy',
    fairHealthcare: 'healthcare',
    fairKindergarten: 'kindergarten',
    fairSchoolHigh: 'school_high',
  }[metricKey];

  if (!expectedCat || !sample.length) return base.slice();

  return base.map((rawVal, idx) => {
    if (Number.isFinite(rawVal)) return Number(rawVal);

    const entity = sample[idx];
    const props = entity?.properties || {};
    const focusedCat = props?.fair?.cat ?? props?.__fairFocusedCat;
    const focusedScore = Number.isFinite(props?.fair?.score)
      ? Number(props.fair.score)
      : (Number.isFinite(props?.__fairFocused) ? Number(props.__fairFocused) : null);

    if (focusedCat === expectedCat && Number.isFinite(focusedScore)) {
      return focusedScore;
    }
    return null;
  });
}

function getCategoryHistogramConfig() {
  return DR_FEATURE_CONFIG
    .filter(cfg => typeof cfg?.key === 'string' && cfg.key.startsWith('fair') && cfg.key !== 'overall')
    .map(cfg => ({
      key: cfg.key,
      label: (cfg.label || cfg.key).replace(/\s+fairness$/i, '')
    }));
}

function buildCategoryHistsSummaryForLLM() {
  const cats = getCategoryHistogramConfig();

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const selectionSet = new Set(selection);

  const rows = cats.map((cfg) => {
    const values = perCategorySeriesWithFocusedFallback(cfg.key);
    const arr = values.filter(Number.isFinite);
    const selVals = values
      .map((v, idx) => (selectionSet.has(idx) ? v : null))
      .filter(Number.isFinite);

    const mean = arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
    const selMean = selVals.length ? selVals.reduce((a, b) => a + b, 0) / selVals.length : null;

    return {
      category: cfg.label,
      count: arr.length,
      mean,
      selected_count: selVals.length,
      selected_mean: selMean,
    };
  });

  const any = rows.some((r) => r.count > 0);
  return any ? { categories: rows, selection_size: selection.length } : { error: 'No per-category fairness values.' };
}

function wireLLMExplainUI() {
  const btn = document.getElementById('explain-view-btn');
  const out = document.getElementById('llm-output');
  const qEl = document.getElementById('llm-question');
  const viewSel = document.getElementById('llm-view-select');
  if (!btn || !out || !viewSel) return;

  let busy = false;

  const run = async () => {
    const view = viewSel.value || 'threshold_bar';
    const builder = LLM_SUMMARY_BUILDERS[view];
    if (!builder) {
      out.textContent = 'Unknown view for explanation.';
      return;
    }

    const summary = builder();
    if (summary?.error) {
      out.textContent = summary.error;
      return;
    }

    const question = qEl ? qEl.value.trim() : '';
    out.textContent = 'Thinking';
    busy = true;

    try {
      const text = await requestLLMExplain(view, summary, question);
      out.textContent = text;
    } catch (err) {
      console.error(err);
      out.textContent = 'Error asking the AI: ' + err.message;
    } finally {
      busy = false;
    }
  };

  btn.addEventListener('click', () => {
    if (!busy) run();
  });

  if (qEl) {
    qEl.addEventListener('keydown', (ev) => {
      if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter' && !busy) {
        run();
      }
    });
  }
}

// Call FastAPI /llm/explain, which uses local Ollama
async function requestLLMExplain(viewName, summary, question) {
  const res = await fetch(`${API_BASE}/llm/explain`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      view: viewName,
      summary: summary,
      question: question || null,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error('LLM API error: ' + text);
  }

  const data = await res.json();
  // Expect your FastAPI route to return: { "text": "..." }
  return data.text;
}

async function requestLLMWhatIfIntent(question, context) {
  const res = await fetch(`${API_BASE}/llm/whatif-intent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      question: question || '',
      context: context || null
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error('LLM intent error: ' + text);
  }

  return res.json();
}


// ======================= Small multiple histograms for categories =======================
function renderCategoryHists(selectionIdx) {
  const { categoryHistsWrap } = ensureDRUI();
  if (!categoryHistsWrap) return;

  if (typeof d3 === 'undefined') {
    categoryHistsWrap.innerHTML =
      '<div class="small text-muted">Category histograms require d3.js.</div>';
    return;
  }

  const allCats = getCategoryHistogramConfig();
  const valuesByCat = Object.fromEntries(
    allCats.map(cfg => [cfg.key, perCategorySeriesWithFocusedFallback(cfg.key)])
  );

  const cats = allCats.filter(cfg => (valuesByCat[cfg.key] || []).some(Number.isFinite));
  if (!cats.length) {
    categoryHistsWrap.innerHTML =
      '<div class="small text-muted">No per-category fairness values available.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const root = d3.select(categoryHistsWrap);
  root.selectAll('*').remove();

  const width  = categoryHistsWrap.clientWidth || 260;
  const height = categoryHistsWrap.clientHeight || 200;
  const labelMeasureCtx = document.createElement('canvas').getContext('2d');
  if (labelMeasureCtx) labelMeasureCtx.font = '9px sans-serif';
  const maxLabelWidth = cats.reduce((max, c) => {
    const label = String(c.label || '');
    const measured = labelMeasureCtx ? labelMeasureCtx.measureText(label).width : (label.length * 5.2);
    return Math.max(max, measured);
  }, 0);
  const maxPad = Math.min(120, Math.max(88, Math.floor(width * 0.24)));
  const labelPad = Math.max(56, Math.min(maxPad, Math.ceil(maxLabelWidth + 10)));
  const margin = { top: 6, right: 12, bottom: 24, left: labelPad };
  const rows = cats.length;
  const rowHeight = (height - margin.top - margin.bottom) / rows;

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  cats.forEach((cfg, idx) => {
    const arr = valuesByCat[cfg.key] || [];
    const allValues = arr.filter(Number.isFinite);
    if (!allValues.length) return;

    const selectedValues = selection.length
      ? selection
          .filter(i => i >= 0 && i < arr.length)
          .map(i => arr[i])
          .filter(Number.isFinite)
      : [];

    const binGen = d3.bin()
      .domain(x.domain())
      .thresholds(10);

    const binsAll = binGen(allValues);
    const binsSel = binGen(selectedValues);

    const maxCount = d3.max([
      d3.max(binsAll, d => d.length) || 0,
      d3.max(binsSel, d => d.length) || 0
    ]) || 1;

    const rowTop = margin.top + idx * rowHeight;
    const rowBottom = rowTop + rowHeight - 10;

    const y = d3.scaleLinear()
      .domain([0, maxCount])
      .range([rowBottom, rowTop + 4]);

    // Gray background bars
    svg.append('g')
      .selectAll(`rect.all-${cfg.key}`)
      .data(binsAll)
      .join('rect')
      .attr('class', `all-${cfg.key}`)
      .attr('x', d => x(d.x0) + 1)
      .attr('y', d => y(d.length))
      .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 2))
      .attr('height', d => y(0) - y(d.length))
      .attr('fill', '#555')
      .attr('opacity', 0.35);

    // Green overlay bars (selection)
    if (selectedValues.length) {
      const widthFactor = 0.6;
      svg.append('g')
        .selectAll(`rect.sel-${cfg.key}`)
        .data(binsSel)
        .join('rect')
        .attr('class', `sel-${cfg.key}`)
        .attr('x', d => {
          const fullWidth = x(d.x1) - x(d.x0) - 2;
          const wSel = fullWidth * widthFactor;
          return x(d.x0) + 1 + (fullWidth - wSel) / 2;
        })
        .attr('y', d => y(d.length))
        .attr('width', d => {
          const fullWidth = x(d.x1) - x(d.x0) - 2;
          return Math.max(0, fullWidth * widthFactor);
        })
        .attr('height', d => y(0) - y(d.length))
        .attr('fill', '#2ecc71')
        .attr('opacity', 0.9);
    }

    // Label on the left for each row
    svg.append('text')
      .attr('x', margin.left - 4)
      .attr('y', rowTop + 11)
      .attr('text-anchor', 'end')
      .attr('font-size', 9)
      .attr('fill', '#000')
      .text(cfg.label);
  });

  // Shared x-axis at the bottom
  const xAxis = d3.axisBottom(x)
    .ticks(4)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Fairness score (0–1)');
}





// ======================= Supervised explanation (logistic model) =======================

function shuffleArray(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
}

/**
 * Train a small logistic regression model in JS to distinguish
 * "selected" vs "rest" using the DR feature space.
 * Returns ranked feature importances.
 */
function trainLogisticSelectionModel(selectionIdx, opts = {}) {
  const Xall = drPlot.features;
  const labels = drPlot.featureLabels || [];
  if (!Xall || !Xall.length) return null;

  const n = Xall.length;
  const d = Xall[0].length;
  if (!selectionIdx || selectionIdx.length < 10) {
    return { error: 'Need at least ~10 selected points for a stable model.' };
  }

  const maxIter = opts.maxIter ?? 80;
  const negPosRatio = opts.negPosRatio ?? 3;
  const lr = opts.lr ?? 0.1;
  const lambda = opts.lambda ?? 0.01;

  const selectedSet = new Set(selectionIdx);

  // Build training dataset: all positives + sampled negatives
  const X = [];
  const y = [];
  // positives
  for (const i of selectionIdx) {
    if (i >= 0 && i < n) {
      X.push(Xall[i]);
      y.push(1);
    }
  }

  const nonSelected = [];
  for (let i = 0; i < n; i++) {
    if (!selectedSet.has(i)) nonSelected.push(i);
  }
  shuffleArray(nonSelected);

  const maxNeg = Math.min(nonSelected.length, selectionIdx.length * negPosRatio + 50);
  for (let k = 0; k < maxNeg; k++) {
    const idx = nonSelected[k];
    X.push(Xall[idx]);
    y.push(0);
  }

  const m = X.length;
  if (m < 20) {
    return { error: 'Not enough total examples to train the model.' };
  }

  // Simple gradient-descent logistic regression
  const w = new Array(d).fill(0);
  let b = 0;

  const sigmoid = (z) => 1 / (1 + Math.exp(-z));

  for (let iter = 0; iter < maxIter; iter++) {
    const gradW = new Array(d).fill(0);
    let gradB = 0;

    for (let i = 0; i < m; i++) {
      const xi = X[i];
      let z = b;
      for (let j = 0; j < d; j++) z += w[j] * xi[j];
      const p = sigmoid(z);
      const err = p - y[i];

      for (let j = 0; j < d; j++) gradW[j] += err * xi[j];
      gradB += err;
    }

    for (let j = 0; j < d; j++) {
      gradW[j] = gradW[j] / m + lambda * w[j]; // L2
      w[j] -= lr * gradW[j];
    }
    gradB /= m;
    b -= lr * gradB;
  }

  const ranked = [];
  for (let j = 0; j < d; j++) {
    const label = labels[j] || `Feature ${j + 1}`;
    const weight = w[j];
    ranked.push({
      index: j,
      label,
      weight,
      absWeight: Math.abs(weight),
      direction: weight >= 0 ? 'higher-in-cluster' : 'lower-in-cluster'
    });
  }
  ranked.sort((a, b) => b.absWeight - a.absWeight);

  return {
    weights: w,
    intercept: b,
    ranked,
    nTrain: m,
    nPos: selectionIdx.length,
    nNeg: m - selectionIdx.length
  };
}

/**
 * Render the model-based feature importance into the "Model" panel.
 */
function renderModelExplanationPanel(model) {
  const { explainModelEl } = ensureDRUI();
  if (!explainModelEl) return;

  if (!model || model.error) {
    explainModelEl.textContent = model && model.error
      ? model.error
      : 'No model available. Make a selection first.';
    return;
  }

  const top = model.ranked.slice(0, 5);
  if (!top.length) {
    explainModelEl.textContent = 'Model could not find informative features.';
    return;
  }

  let html = '';
  html += `<div class="small text-muted mb-1">
    Logistic model trained on the same feature space as PCA/UMAP.
  </div>`;
  html += `<div class="small mb-2">
    Training set: ${model.nPos} in-cluster vs ${model.nNeg} other buildings.
  </div>`;

  html += '<ul class="small mb-0 ps-3">';
  for (const f of top) {
    const dirText = f.direction === 'higher-in-cluster'
      ? 'tends to be higher inside the cluster'
      : 'tends to be lower inside the cluster';
    const arrow = f.direction === 'higher-in-cluster' ? '↑' : '↓';

    html += `<li>
      <strong>${f.label}</strong> (${arrow}):
      <span class="text-muted">|w| = ${f.absWeight.toFixed(3)}</span><br/>
      <span class="text-muted">${dirText}.</span>
    </li>`;
  }
  html += '</ul>';

  explainModelEl.innerHTML = html;
}

/**
 * Ensure we have a fresh model explanation for the current selection
 * and update the Model panel.
 */
function ensureModelExplanation(selectionIdx) {
  const { explainModelEl } = ensureDRUI();
  if (!explainModelEl) return;

  if (!selectionIdx || !selectionIdx.length) {
    explainModelEl.textContent = 'No selection yet.';
    return;
  }

  const model = trainLogisticSelectionModel(selectionIdx);
  drPlot.lastModelExplanation = model;
  renderModelExplanationPanel(model);
}


// ======================= Explanation mode wiring (Stats / Model) =======================

function refreshExplanationView() {
  const {
    explainStatsEl,
    explainModelEl
  } = ensureDRUI();

  const hasSelection = drPlot.lastSelectionIdx && drPlot.lastSelectionIdx.length > 0;

  if (drExplainMode === 'stats') {
    if (explainStatsEl) explainStatsEl.classList.remove('d-none');
    if (explainModelEl) explainModelEl.classList.add('d-none');

    if (!hasSelection) {
      if (explainStatsEl) explainStatsEl.textContent = 'No selection yet.';
      return;
    }

    if (!drPlot.lastFeatureDiff) {
      drPlot.lastFeatureDiff = computeFeatureDifferences(drPlot.lastSelectionIdx);
    }
    renderFeatureDiffPanel(drPlot.lastFeatureDiff);
  } else {
    if (explainModelEl) explainModelEl.classList.remove('d-none');
    if (explainStatsEl) explainStatsEl.classList.add('d-none');

    if (!hasSelection) {
      if (explainModelEl) explainModelEl.textContent = 'No selection yet.';
      return;
    }
    ensureModelExplanation(drPlot.lastSelectionIdx);
  }
}

function initExplainModeUI() {
  const {
    explainModeStatsBtn,
    explainModeModelBtn
  } = ensureDRUI();

  if (!explainModeStatsBtn || !explainModeModelBtn) return;
  if (explainModeStatsBtn.__bound) return;

  const applyMode = (mode) => {
    drExplainMode = mode;
    const isStats = mode === 'stats';
    explainModeStatsBtn.classList.toggle('active', isStats);
    explainModeModelBtn.classList.toggle('active', !isStats);
    refreshExplanationView();
  };

  explainModeStatsBtn.addEventListener('click', (e) => {
    e.preventDefault();
    applyMode('stats');
  });
  explainModeModelBtn.addEventListener('click', (e) => {
    e.preventDefault();
    applyMode('model');
  });

  explainModeStatsBtn.__bound = true;
  explainModeModelBtn.__bound = true;

  // Initialize based on current mode
  applyMode(drExplainMode || 'stats');
}



// ======================= Engine-level plots: EBM vs Contrastive =======================

// Small helper: build unsupervised contrastive engine data
function buildContrastiveEngineExplanation() {
  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) return null;

  // Use your existing per-feature difference computation
  const diff = drPlot.lastFeatureDiff ||
    computeFeatureDifferences(selection);

  const feats = diff && diff.features ? diff.features : [];
  if (!feats.length) return null;

  // Take top few features by |effect|
  const top = feats.slice(0, 8);

  return {
    mode: 'contrast',
    ranked: top.map(f => ({
      label: f.label,
      score: Math.abs(f.effect || 0),
      direction: (f.effect || 0) >= 0
        ? 'higher-in-cluster'
        : 'lower-in-cluster'
   })),
    note: 'Unsupervised: contrastive distribution between selection and city ' +
          'using standardized mean differences (effect sizes). ' +
          'Green = higher in selection, red = lower in selection.'
  };
}

// Build payload and call the Python EBM backend
async function buildEBMEngineExplanation() {
  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) return null;

  const Xall = drPlot.features;
  const featureNames = drPlot.featureLabels || [];
  if (!Xall || !Xall.length) {
    return {
      mode: 'ebm',
      ranked: [],
      note: 'No DR feature matrix available for EBM.'
    };
  }

  const n = Xall.length;
  const selectedSet = new Set(selection);

  // Label: 1 = selected, 0 = rest
  const X = [];
  const y = [];
  for (let i = 0; i < n; i++) {
    X.push(Xall[i]);
    y.push(selectedSet.has(i) ? 1 : 0);
  }

  const payload = {
    X,
    y,
    feature_names: featureNames   // <— send human-readable names to backend
  };

  const myId = ++drEngineRequestId;

  try {
    const resp = await fetch(`${API_BASE}/api/ebm/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!resp.ok) {
      throw new Error(`EBM HTTP ${resp.status}`);
    }
    const data = await resp.json();

    // Drop stale response if a newer request was started
    if (myId !== drEngineRequestId) {
      return null;
    }

    const rankedRaw = data.ranked || [];

    // Prefer backend labels (they already carry the true feature name after
    // backend ranking). If backend sends a generic "Feature N", map N back
    // to the original DR feature label for readability.
    const ranked = rankedRaw.map((f, idx) => {
      const nameFromBackend =
        (typeof f.label === 'string' && f.label.trim()) ? f.label.trim() : null;

      let label = nameFromBackend || null;
      if (label) {
        const generic = label.match(/^feature\s+(\d+)$/i);
        if (generic) {
          const oneBased = Number(generic[1]);
          const mapped = (Number.isFinite(oneBased) && oneBased >= 1 && oneBased <= featureNames.length)
            ? featureNames[oneBased - 1]
            : null;
          if (mapped) label = mapped;
        }
      }

      if (!label) {
        const fromPosition =
          (idx < featureNames.length && typeof featureNames[idx] === 'string' && featureNames[idx].trim())
            ? featureNames[idx].trim()
            : null;
        label = fromPosition || `Feature ${idx + 1}`;
      }

      const baseScore = f.importance ?? f.score ?? 0;
      return {
        label,
        score: Math.abs(baseScore),
        direction: f.direction || 'higher-in-cluster'
      };
    });

    return {
      mode: 'ebm',
      ranked,
      nTrain: data.n_train ?? null,
      nPos: data.n_pos ?? null,
      nNeg: data.n_neg ?? null,
      note: data.note ||
        'EBM (Explainable Boosting Machine) trained on selected vs other buildings.'
    };
  } catch (err) {
    console.error('EBM engine error:', err);
    if (myId !== drEngineRequestId) {
      return null;
    }
    return {
      mode: 'ebm',
      ranked: [],
      note: 'EBM error: ' + err.message
    };
  }
}

// Draw horizontal bar chart inside #drEnginePlot
function drawEngineBarChart(engineData) {
  const { enginePlotEl } = ensureDRUI();
  if (!enginePlotEl) return;

  // If d3 is missing, show a simple message and clear the note.
  if (typeof d3 === 'undefined') {
    enginePlotEl.textContent = 'd3 is required for engine plots.';
    const noteEl = document.getElementById('drEngineNote');
    if (noteEl) noteEl.textContent = '';
    return;
  }

  // Clear the plot area.
  const root = d3.select(enginePlotEl);
  root.html('');

  // Update the external note area instead of appending inside the plot.
  const noteEl = document.getElementById('drEngineNote');
  if (noteEl) {
    let noteTxt = engineData && engineData.note ? engineData.note : '';
    if (engineData?.mode === 'ebm') {
      noteTxt += (noteTxt ? ' ' : '') +
        'Color does not encode direction here; EBM bars show importance magnitude only.';
    }
    noteEl.textContent = noteTxt;
  }

  // No data case.
  if (!engineData || !engineData.ranked || !engineData.ranked.length) {
    root.append('div')
      .attr('class', 'small text-muted')
      .text('No informative features for this selection.');
    return;
  }

 const top = engineData.ranked.slice(0, 7);
  const width = enginePlotEl.clientWidth || 260;
  const margin = { top: 26, right: 12, bottom: 52, left: 170 };
  const minInnerHeight = top.length * 24;
  const height = Math.max(
    enginePlotEl.clientHeight || 0,
    margin.top + margin.bottom + minInnerHeight
  );

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height)
    .style('display', 'block');

  const xMax = d3.max(top, f => f.score) || 0.01;
  const x = d3.scaleLinear()
    .domain([0, xMax])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleBand()
    .domain(top.map(f => f.label))
    .range([margin.top, height - margin.bottom])
    .padding(0.2);

  // x-axis
  const xAxis = d3.axisBottom(x)
    .ticks(3)
    .tickSizeOuter(0);

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').remove();
      g.selectAll('line').attr('stroke', '#ccc');
      g.selectAll('text').attr('font-size', 9);
    });

  // x-axis label (different text for EBM vs contrastive)
  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - 14)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#666')
    .text(
      engineData.mode === 'ebm'
        ? 'EBM importance (log-odds magnitude)'
        : 'Effect size |d| (contrastive)'
    );

  // bars
  svg.append('g')
    .selectAll('rect')
    .data(top)
    .enter()
    .append('rect')
    .attr('x', x(0))
    .attr('y', d => y(d.label))
    .attr('height', y.bandwidth())
    .attr('width', d => x(d.score) - x(0))
    .attr('fill', d => {
      if (engineData.mode === 'ebm') return '#6c6c6c';
      return d.direction === 'higher-in-cluster' ? '#2ecc71' : '#e74c3c';
    });

  // y-axis labels (feature names)
  svg.append('g')
    .selectAll('text.feature-label')
    .data(top)
    .enter()
    .append('text')
    .attr('class', 'feature-label')
    .attr('x', margin.left - 6)
    .attr('y', d => y(d.label) + y.bandwidth() / 2)
    .attr('dy', '0.35em')
    .attr('text-anchor', 'end')
    .attr('font-size', 10)
    .text(d => d.label);

  // small title line
  const title = engineData.mode === 'ebm'
    ? 'Supervised engine (EBM)'
    : 'Unsupervised engine (contrastive distribution)';

  svg.append('text')
    .attr('x', margin.left)
    .attr('y', margin.top - 4)
    .attr('font-size', 10)
    .attr('fill', '#555')
    .text(title);
}


// Refresh engine plot whenever selection or mode changes
function engineNeedsCalculationMessage(mode) {
  return mode === 'contrast'
    ? 'Contrastive distribution is ready to run. Click "Calculate selected engine".'
    : 'EBM is ready to run. Click "Calculate selected engine".';
}

async function refreshEnginePlot() {
  const { enginePlotEl } = ensureDRUI();
  if (!enginePlotEl) return;

  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) {
    if (typeof d3 !== 'undefined') {
      d3.select(enginePlotEl).selectAll('*').remove();
    }
    enginePlotEl.textContent =
      'Select some buildings (lasso) to see ranked features here.';
    return;
  }

  const engineData = drPlot.engineMode === 'contrast'
    ? drPlot.engineContrast
    : drPlot.engineEBM;

  if (!engineData) {
    enginePlotEl.textContent = engineNeedsCalculationMessage(drPlot.engineMode);
    const noteEl = document.getElementById('drEngineNote');
    if (noteEl) noteEl.textContent = '';
    return;
  }

  drawEngineBarChart(engineData);
}

async function calculateEnginePlot() {
  const { enginePlotEl, engineCalcBtn } = ensureDRUI();
  if (!enginePlotEl) return;

  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) {
    enginePlotEl.textContent = 'Select some buildings (lasso) to see ranked features here.';
    return;
  }

  try {
    if (engineCalcBtn) engineCalcBtn.disabled = true;
    if (drPlot.engineMode === 'contrast') {
      enginePlotEl.textContent = 'Calculating contrastive distribution…';
      drPlot.engineContrast = buildContrastiveEngineExplanation();
    } else {
      enginePlotEl.textContent = 'Training EBM on selection…';
      drPlot.engineEBM = await buildEBMEngineExplanation();
    }
    await refreshEnginePlot();
  } finally {
    if (engineCalcBtn) engineCalcBtn.disabled = false;
  }
}


// Switch between EBM and Contrastive engines
function setEngineMode(mode) {
  if (mode !== 'ebm' && mode !== 'contrast') return;

  // If mode is already active, do nothing
  if (drPlot.engineMode === mode) return;

  drPlot.engineMode = mode;

  const { engineEBMBtn, engineContrastBtn } = ensureDRUI();
  if (engineEBMBtn) {
    engineEBMBtn.classList.toggle('btn-secondary', mode === 'ebm');
    engineEBMBtn.classList.toggle('btn-outline-secondary', mode !== 'ebm');
  }
  if (engineContrastBtn) {
    engineContrastBtn.classList.toggle('btn-secondary', mode === 'contrast');
    engineContrastBtn.classList.toggle('btn-outline-secondary', mode !== 'contrast');
  }

  // Just redraw using existing caches (or compute once if missing)
  refreshEnginePlot();
}


// Bind engine buttons once
function initEngineUI() {
  const { engineEBMBtn, engineContrastBtn, engineCalcBtn } = ensureDRUI();

  if (engineEBMBtn && !engineEBMBtn.__bound) {
    engineEBMBtn.addEventListener('click', (e) => {
      e.preventDefault();
      setEngineMode('ebm');
    });
    engineEBMBtn.__bound = true;
  }

  if (engineContrastBtn && !engineContrastBtn.__bound) {
    engineContrastBtn.addEventListener('click', (e) => {
      e.preventDefault();
      setEngineMode('contrast');
    });
    engineContrastBtn.__bound = true;
  }

  if (engineCalcBtn && !engineCalcBtn.__bound) {
    engineCalcBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      await calculateEnginePlot();
    });
    engineCalcBtn.__bound = true;
  }

  // Initialize default engine mode
  setEngineMode(drPlot.engineMode || 'ebm');
}





/* ---------- Bindings (robust) ---------- */
function bindDRButtonsIfReady() {
  const runBtn = document.getElementById('drRunBtn');
  if (runBtn && !runBtn.__bound) {
    runBtn.addEventListener('click', (e) => { e.preventDefault(); runDR(); });
    runBtn.__bound = true;
  }

  const lassoBtn = document.getElementById('drLassoBtn');
  if (lassoBtn && !lassoBtn.__bound) {
    lassoBtn.addEventListener('click', (e) => { e.preventDefault(); toggleLasso(); });
    lassoBtn.__bound = true;
  }

  const clearSelBtn = document.getElementById('drClearSelBtn');
  if (clearSelBtn && !clearSelBtn.__bound) {
    clearSelBtn.addEventListener('click', (e) => { e.preventDefault(); clearSelection(); });
    clearSelBtn.__bound = true;
  }

  const clearProjBtn = document.getElementById('drClearProjBtn');
  if (clearProjBtn && !clearProjBtn.__bound) {
    clearProjBtn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      clearDRProjection();
    });
    clearProjBtn.__bound = true;
  }

  // Defensive delegation in case the button is re-rendered or missed during
  // initial binding. This keeps the handler alive even if Bootstrap toggles
  // the offcanvas content or the element is replaced.
  if (!bindDRButtonsIfReady.__delegated) {
    const handleClear = (e) => {
      const btn = e.target?.closest?.('#drClearProjBtn,[data-action="clear-umap"]');
      if (!btn) return;
      e.preventDefault();
      e.stopPropagation();
      clearDRProjection();
    };

    document.addEventListener('click', handleClear, true);
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      handleClear(e);
    }, true);

    bindDRButtonsIfReady.__delegated = true;
  }
}

function initDRUI() {
  const offcanvasEl = document.getElementById('drOffcanvas');

  // Bind the DR buttons immediately so the controls work even before the
  // offcanvas "shown" event fires (e.g., if Bootstrap fails to emit it).
  bindDRButtonsIfReady();

  if (offcanvasEl && !offcanvasEl.__drBound) {
    offcanvasEl.addEventListener('shown.bs.offcanvas', () => {
      // 1) Make sure DR surface & base controls are ready
      showDRLibraryStatus();
      prepareDRSurface();
      bindDRButtonsIfReady();   // zoom/reset, algo, color, etc.

      // 2) Bind lower explanation tabs (Stats / Model)
      initExplainModeUI();

      // 3) Bind upper engine selector (EBM / Contrastive)
      //    initEngineUI() internally calls setEngineMode(...)
      initEngineUI();

      // 4) Threshold slider: label + re-render threshold chart
      const { thresholdSlider, thresholdLabel } = ensureDRUI();
      if (thresholdSlider && !thresholdSlider.__bound) {
        const onChange = () => {
          const v = parseFloat(thresholdSlider.value || '0.5') || 0.5;
          if (thresholdLabel) {
            thresholdLabel.textContent = v.toFixed(2);
          }
          // Re-render only the threshold bar chart with current selection
          if (typeof renderThresholdBar === 'function') {
            renderThresholdBar(drPlot.lastSelectionIdx || []);
          }
        };
        thresholdSlider.addEventListener('input', onChange);
        thresholdSlider.__bound = true;
        onChange(); // initial label + initial chart
      }

      // 5) If we already have DR results, restore view + all coordinated panels
      if (drPlot.points && drPlot.colors) {
        drPlot.screenXY = computeScreenPositions(drPlot.points);
        const selIdx = (lasso.selectedIdx && lasso.selectedIdx.length)
          ? lasso.selectedIdx
          : null;

        redrawDR(selIdx);
        initD3Overlay();          // re-draw lasso hull if any

        // single call that:
        // - updates selInfo text
        // - updates Stats/Model explanation panel
        // - updates EBM/contrastive engine plot
        // - updates histogram, district bar, scatter, Lorenz, threshold chart, category hists
        if (typeof renderSelectionStats === 'function') {
          renderSelectionStats(selIdx || []);
        }
      } else {
        // No DR yet: still initialise city-wide panels with "no selection"
        if (typeof renderSelectionStats === 'function') {
          renderSelectionStats([]);
        }
      }
    });

    offcanvasEl.__drBound = true;
  }

  // Early binding in case offcanvas is already visible or opened quickly
  bindDRButtonsIfReady();
  initExplainModeUI();
  initEngineUI();
}



function showDRLibraryStatus() {
  const libNote = document.getElementById('drLibNote');
  const umapOk = hasUMAPGlobal();
  if (libNote) {
    libNote.innerHTML = `
      <span class="${umapOk?'text-success':'text-danger'}">UMAP ${umapOk?'loaded':'not loaded'}</span><br/>
      UMAP needs <code>assets/vendor/umap.min.js</code> (or we’ll try an online module).
    `;
  }
}

function resetDROverlayAndSelection() {
  setLassoActive(false);
  const svgEl = document.getElementById('drOverlay');
  if (svgEl && typeof d3 !== 'undefined') d3.select(svgEl).selectAll('*').remove();

  lasso.selectedIdx = [];
  lasso.points = [];
  lasso.drawing = false;
  lasso.marqueeDrawing = false;
  lasso.marqueeStart = null;

  if (lasso.path) {
    lasso.path.remove();
    lasso.path = null;
  }
  if (lasso.marqueeRect) {
    lasso.marqueeRect.remove();
    lasso.marqueeRect = null;
  }

  drHasSelection = false;          // reset flag
  renderSelectionStats([]);

  // wipe selection state from map buildings
  clearDRMapSelection();

  updateLayers();
}

function clearDRProjection(showMessage = true) {
  const {
    canvas,
    statusEl,
    infoEl,
    legendTitle,
    legendText,
    clearSelBtn,
    clearProjBtn
  } = ensureDRUI();

  resetDROverlayAndSelection();
  clearParallelCoordsSelectionFromClearAction();

  drPlot.points = null;
  drPlot.colors = null;
  drPlot.screenXY = null;
  drPlot.sample = null;
  drPlot.metrics = null;
  drPlot.mode = currentDRDataMode();
  drPlot.features = null;
  drPlot.featureLabels = null;
  drPlot.cityStats = null;
  drPlot.lastSelectionIdx = [];
  drPlot.lastFeatureDiff = null;
  drPlot.lastModelExplanation = null;
  drPlot.engineEBM = null;
  drPlot.engineContrast = null;
  drPlot.runNonce = 0;

  drHasSelection = false;

 if (canvas) {
    const ctx = canvas.getContext('2d');
    ctx.save();
    ctx.setTransform(1, 0, 0, 1, 0, 0);   // reset any HiDPI scaling before clearing
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.restore();
    resizeDRCanvas();
  }


  const overlay = document.getElementById('drOverlay');
  if (overlay && typeof d3 !== 'undefined') {
    d3.select(overlay).selectAll('*').remove();
  }

  // if (statusEl) statusEl.textContent = showMessage ? 'Projection cleared.' : '';
  if (infoEl) infoEl.textContent = 'Run PCA/UMAP to see a projection.';
  if (legendTitle) legendTitle.textContent = 'Color legend';
  if (legendText) legendText.textContent = '—';
  if (clearSelBtn) clearSelBtn.disabled = true;
  if (clearProjBtn) clearProjBtn.disabled = true;
}



  async function runDR() {
    // By default, re-running DR/UMAP should start with a clean selection state.
    // Set `globalThis.DR_PRESERVE_SELECTION_ON_RERUN = true` to keep prior map selection.
    const preserveSelectionOnRerun = !!globalThis.DR_PRESERVE_SELECTION_ON_RERUN;
    const pendingMapSelection = preserveSelectionOnRerun ? getCurrentMapSelection() : [];

    showDRSpinner();
    try {
      const { algoSel, colorSel, maxPtsEl, normEl, statusEl, infoEl, clearSelBtn, clearProjBtn } = ensureDRUI();
    const algo    = (algoSel?.value || 'pca').toLowerCase();
    const colorBy = (colorSel?.value || 'overall');
    const rawMax  = parseInt(maxPtsEl?.value || '0', 10);
    const maxPts  = (rawMax <= 0 || !Number.isFinite(rawMax)) ? Infinity : Math.max(200, rawMax);
    const normalize = !!(normEl && normEl.checked);

    prepareDRSurface();
    resetDROverlayAndSelection(); // clear overlay & selection BEFORE plotting

    drPlot.runNonce = (Number(drPlot.runNonce) || 0) + 1;
    const { X, colors, sampleCount, sample, metrics, featureLabels, dims } =
      collectDRData(maxPts, normalize, colorBy, drPlot.runNonce);

    let Y = null;
    if (algo === 'pca') {
      Y = runPCA(X);
      if (statusEl) statusEl.textContent = `PCA done for ${sampleCount} points.`;
    } else if (algo === 'umap') {
      if (!hasUMAPGlobal()) {
        const ok = await ensureUMAP();
        if (!ok) {
          Y = runPCA(X);
          if (statusEl) statusEl.textContent = `UMAP not loaded → fell back to PCA (${sampleCount} points).`;
        } else {
          Y = await runUMAP(X, { nNeighbors: 15, minDist: 0.1, nEpochs: 200 });
          if (statusEl) statusEl.textContent = `UMAP done for ${sampleCount} points.`;
        }
      } else {
        Y = await runUMAP(X, { nNeighbors: 15, minDist: 0.1, nEpochs: 200 });
        if (statusEl) statusEl.textContent = `UMAP done for ${sampleCount} points.`;
      }
    } else {
      Y = runPCA(X);
      if (statusEl) statusEl.textContent = `PCA done for ${sampleCount} points.`;
    }

    // Store DR results + feature space for explanations
    drPlot.points   = Y;
    drPlot.colors   = colors;
    drPlot.sample   = sample;
    drPlot.metrics  = metrics;
    drPlot.features = X;                     //same features used for DR
    drPlot.featureLabels = featureLabels || null;
    drPlot.cityStats = null;                 // reset cached stats
    drPlot.mode = currentDRDataMode();
    drPlot.lastSelectionIdx = [];
    drPlot.lastFeatureDiff = null;
    drPlot.lastModelExplanation = null;

    drPlot.screenXY = computeScreenPositions(Y);
    redrawDR();

    const umapOk = hasUMAPGlobal();
    if (infoEl) {
      const labelsText = (featureLabels && featureLabels.length)
        ? featureLabels.join(', ')
        : `${dims || (X[0]?.length || 0)}D feature space`;
      const modeLabel = currentDRDataMode();
      infoEl.textContent =
        `Mode = ${modeLabel} | Features = ${labelsText} | UMAP: ${umapOk ? 'loaded' : 'not loaded'}`;
    }

      renderSelectionStats([]);
      initD3Overlay();
      refreshExplanationView(); //panels show "No selection yet"

     // Optional: preserve map-driven selection across reruns when explicitly enabled.
      if (preserveSelectionOnRerun && pendingMapSelection.length) {
        applyMapSelection(pendingMapSelection, { skipDRSync: true });
        syncDRSelectionFromBuildings(pendingMapSelection, { preserveMapSelection: true });
      }

      // Buttons: no selection yet
      if (clearSelBtn) clearSelBtn.disabled = true;
      if (clearProjBtn) clearProjBtn.disabled = false;

    hideDRSpinner();
  } catch (e) {
    console.error(e);
    alert(e?.message || e);
    try {
      const { statusEl } = ensureDRUI();
      if (statusEl) statusEl.textContent = 'Error while running DR.';
    } catch {}
    hideDRSpinner('Error');
  }
}


/* ======================= small utils ======================= */
function debounce(fn, ms=200) {
  let id=null;
  return (...args) => { if (id) clearTimeout(id); id=setTimeout(()=>fn(...args), ms); };
}