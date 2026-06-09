// Top-of-app config — extracted from main.js. Loaded as a classical script
// before main.js so its top-level `const`s are reachable in the same Realm.
// See CLAUDE.md → "Frontend code organization" for the target layout.

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

// Routing constants. ROUTING_BASE_URL is unused at runtime — see CLAUDE.md
// "OSRM and routing". Kept for the case where road-distance bake is added.
const ROUTING_BASE_URL = 'https://router.project-osrm.org';
const ROUTING_PROFILE = 'walking';
const ROUTING_TIMEOUT_MS = 8000;
const ROUTING_CACHE_LIMIT = 5000;
const TRAVEL_SPEED_KMH = {
  walking: 5,
  cycling: 15,
  driving: 40,
  // Transit has no single speed; this is only used by the haversine fallback
  // for buildings with no baked transit entry — i.e. "no service nearby, you
  // walk". The baked transit path uses real door-to-door seconds, not this.
  transit: 5,
};
const FAIRNESS_EFFORT_MULTIPLIER = {
  walking: 1,
  cycling: 0.7,
  driving: 0.45,
  transit: 0.8,
};
const FAIRNESS_TRAVEL_MODE_DEFAULT = 'walking';

const TRANSITION_DURATION_MS = 1200;
const TRANSITION_CHANGED_FLASH_MS = 420;
const TRANSITION_CHANGED_FLASH_MAX_INTENSITY = 0.9;
const TRANSITION_CHANGED_FLASH_COLOR = [220, 20, 60];
const TRANSITION_CHANGE_EPSILON = 1e-6;
const FAIRNESS_MODEL_DEFAULT = 'ifcity';
const FAIRNESS_COLOR_SCHEME_DEFAULT = 'cool';
// USE_TRAVEL_TIME=false hard-disables OSRM at runtime per the "no external
// APIs at runtime" rule (see CLAUDE.md). getTravelMetricsForPair then falls
// back to haversine distance + estimated time from TRAVEL_SPEED_KMH, which
// IS the distance model — no API contact needed for it to compute.
const USE_TRAVEL_TIME = false;
const ROUTING_MAX_BUILDINGS = 400;
const ROUTING_MAX_POIS = 200;
const ROUTING_ENABLE_OVERALL = false;

// Overpass endpoints — the runtime app must NOT call these. They are kept
// here because tools/bake_city_data.py imports the same selectors. See
// CLAUDE.md "Hard rule".
const OVERPASS_ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://overpass.nchc.org.tw/api/interpreter',
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
  school_high: 0.7,
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
  school_high: 1.1,
};
const IF_CITY_EQUITY_WEIGHTS = {
  Residential: 1.0,
  Education: 1.1,
  Health: 1.2,
  Commercial: 0.95,
  Industrial: 0.9,
  'Other / unknown': 1.0,
};
const IF_CITY_MODE_DISTANCE_FACTOR = {
  walking: 1.0,
  cycling: 1.55,
  driving: 1.85,
  // Perceived-cost multiplier applied to transit travel TIME (see
  // ifCityTransitTimeForMode). R5's seconds already include walking, waiting
  // and transfers, so we keep this modest rather than double-penalising.
  transit: 1.4,
};
const IF_CITY_MODE_DETOUR_FACTOR = {
  walking: 1.0,
  cycling: 1.15,
  driving: 1.3,
  // Only used by the haversine fallback (no baked transit entry → walk).
  transit: 1.0,
};
const IF_CITY_MODE_SPEED_KMH = {
  walking: 5,
  cycling: 15,
  driving: 40,
  // Fallback-only (no baked transit entry → walk). The baked transit path
  // decays on real R5 seconds via ifCityTransitTimeForMode, not this speed.
  transit: 5,
};
const IF_CITY_REFERENCE_SPEED_KMH = IF_CITY_MODE_SPEED_KMH.walking;

// HEX/MEZO/PARALLEL/WHATIF constants (moved from main.js residual)
const SUMMARY_CUTOFF = 0.02;
const HEX_CELL_KM    = 0.8;
const HEX_MIN_COUNT  = 5;
// 0.75 km removed: H3 res 7≈1.22 km / res 8≈0.46 km / res 9≈0.17 km — both
// 0.5 and 0.75 round to res 8 and rendered identically, so we keep three
// options that map to three distinct H3 resolutions.
const MEZO_HEX_EDGE_OPTIONS_KM = [0.25, 0.5, 1];
const DEFAULT_MEZO_HEX_EDGE_KM = 0.5;
const MEZO_MIN_COUNT = 3;
const BUILDINGS_MIN_ZOOM = 13;
const PARALLEL_COORDS_MAX_LINES = Number.isFinite(globalThis.PARALLEL_COORDS_MAX_LINES)
  ? globalThis.PARALLEL_COORDS_MAX_LINES
  : 5000;
globalThis.PARALLEL_COORDS_MAX_LINES = PARALLEL_COORDS_MAX_LINES;
const SELF_POI_EPS_M = 5;
const WHATIF_SUGGESTION_LIMIT = 120;
// Cap for exact city-wide search: top-N most under-served buildings by score.
// pickCandidateRows already sorts ascending by score so these are the best candidates.
// Infinity was the original value but caused O(N²) hangs for any real city.
const WHATIF_SUGGESTION_EXACT_LIMIT = 500;
const WHATIF_EXACT_MAX_COMBOS = 2000000n;

function isExactCityWideSingleCandidateMode({ count, bbox, center, radiusKm, areaFocus }) {
  return Number(count) === 1
    && !bbox
    && !center
    && (!Number.isFinite(radiusKm) || radiusKm <= 0)
    && (!areaFocus || areaFocus === 'any');
}
const WHATIF_MOCK_BUILDING_LIMIT = 5000;
let whatIfMockFootprintMin = 10;
let whatIfMockFootprintMax = 500;
let whatIfMockFloorHeight  = 3;
let whatIfMockFloors       = 3;
let whatIfMockShapeVariation = 50;
const WHATIF_AVOID_SAMPLE_PX = 14;
const FAIRNESS_BADGE_DECIMALS = 3;
const WHATIF_AVOID_LAYER_KEYWORDS = [
  'water', 'lake', 'river', 'stream', 'canal', 'reservoir', 'ocean',
  'road', 'street', 'highway', 'bridge', 'tunnel', 'rail',
  'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
  'path', 'track', 'transit', 'ferry'
];
