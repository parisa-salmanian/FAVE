"""One-shot script to distribute main.js residual into appropriate lib files.
Idempotent: not re-runnable as-is (it deletes from main.js). Run once, then
delete this file.
"""
import sys
import pathlib

sys.stdout.reconfigure(encoding='utf-8')

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

main_path = REPO_ROOT / 'frontend' / 'assets' / 'js' / 'main.js'
text = main_path.read_text(encoding='utf-8')

transition_block = """/* ---------- Animated transition (Changes) state ---------- */
let transitionAnimActive = false;
let transitionAnimT = 0;
let transitionAnimRAF = null;
let transitionAnimTick = 0;
let transitionHasData = false;
let transitionMezoPrevScoreByHex = new Map();"""

config_block = """// HEX/MEZO/PARALLEL/WHATIF constants (moved from main.js residual)
const SUMMARY_CUTOFF = 0.02;
const HEX_CELL_KM    = 0.8;
const HEX_MIN_COUNT  = 5;
const MEZO_HEX_EDGE_OPTIONS_KM = [0.25, 0.5, 0.75, 1];
const DEFAULT_MEZO_HEX_EDGE_KM = 0.5;
const MEZO_MIN_COUNT = 3;
const BUILDINGS_MIN_ZOOM = 13;
const PARALLEL_COORDS_MAX_LINES = Number.isFinite(globalThis.PARALLEL_COORDS_MAX_LINES)
  ? globalThis.PARALLEL_COORDS_MAX_LINES
  : 5000;
globalThis.PARALLEL_COORDS_MAX_LINES = PARALLEL_COORDS_MAX_LINES;
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
];"""

visuals_block = """// District label font/charset (moved from main.js residual)
const DISTRICT_LABEL_FONT_FAMILY = '"Noto Sans","Noto Sans Symbols 2","Noto Sans Symbols","Segoe UI","Arial Unicode MS",sans-serif';
const DISTRICT_LABEL_CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzÅÄÖåäö0123456789 .,-/()';"""

poi_atlas_block = """// POI icon atlas builder + global atlas cache. Extracted from main.js.
let districtBoundaryFC = null;
let currentBasemapStyle = DARK_BASEMAP_STYLE;

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
        iconMapping[cat] = { x: col * size, y: row * size, width: size, height: size, mask: false };
        if (++loaded === cats.length) resolve({ iconAtlas: canvas, iconMapping });
      };
      img.onerror = () => {
        console.warn(`[POI Atlas] Failed to load icon for "${cat}":`, POI_SYMBOLS[cat].icon);
        if (++loaded === cats.length) resolve({ iconAtlas: canvas, iconMapping });
      };
      img.src = POI_SYMBOLS[cat].icon;
    });
  });
}

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
}"""

poi_state_helpers_block = r"""// POI selection state + small popup/sidebar helpers (moved from main.js residual)
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
}"""


def append(path, content):
    p2 = REPO_ROOT / path
    existing = p2.read_text(encoding='utf-8')
    p2.write_text(existing.rstrip() + '\n\n' + content + '\n', encoding='utf-8')
    print(f'appended to {path}')


append('frontend/assets/js/lib/state.js', transition_block)
append('frontend/assets/js/lib/config.js', config_block)
append('frontend/assets/js/lib/visuals.js', visuals_block)
(REPO_ROOT / 'frontend/assets/js/lib/poiAtlas.js').write_text(
    '// POI icon atlas — extracted from main.js residual.\n'
    '// Loaded after lib/visuals.js (POI_SYMBOLS, DARK_BASEMAP_STYLE).\n\n'
    + poi_atlas_block + '\n',
    encoding='utf-8',
)
print('wrote lib/poiAtlas.js')
append('frontend/assets/js/lib/state.js', poi_state_helpers_block)

stub = """// FAVE entry-point shim. The original ~16k-line main.js has been split into
// classical scripts under frontend/assets/js/{lib,models,views,controllers}.
// This file is kept so index.html's existing <script src="assets/js/main.js">
// tag stays valid. Load order is enforced in index.html. See CLAUDE.md.
console.info('[FAVE] main.js loaded — modules under assets/js/{lib,models,views,controllers}');
"""
main_path.write_text(stub, encoding='utf-8')
print(f'main.js: -> {len(stub.splitlines())} lines')
