/**
 * view.js — Three.js epidemic renderer, embedded inside a MapLibre custom layer.
 *
 * Coordinate system (same as osm.py):
 *   world_x  — metres East  (Three.js X+)
 *   world_z  — metres South (Three.js Z+)
 *   world_y  — metres Up    (Three.js Y+)
 *
 * Buildings are rendered as extruded polygon shapes (real OSM footprints),
 * merged into a single BufferGeometry with per-vertex SEIR colour.
 */

import * as THREE from 'three';
import { mergeGeometries }     from 'three/addons/utils/BufferGeometryUtils.js';
import { LineSegments2 }        from 'three/addons/lines/LineSegments2.js';
import { LineSegmentsGeometry } from 'three/addons/lines/LineSegmentsGeometry.js';
import { LineMaterial }         from 'three/addons/lines/LineMaterial.js';
import { sampleScale }         from './colorscales.js';
import { t }                   from './i18n.js';
import { buildAppearanceCity, buildLandmarks } from './buildings.js';
import { buildNaturalDetailLayer, buildSwedishFlagPole, buildLNUFlagPole, drawLNUTree, buildDalahast, buildIkeaBag, buildFikaCup, buildMaypole, buildMoose } from './buildings_custom.js';

// Easter egg 3D builder dispatch table
const _EASTER_EGG_BUILDERS = {
  dalahast: buildDalahast, ikea_bag: buildIkeaBag, fika_cup: buildFikaCup,
  maypole: buildMaypole, moose: buildMoose,
};

// ── Visual constants ──────────────────────────────────────────────────────────

// ── Group-coherent colour palette ────────────────────────────────────────────
//
// Every POI category belongs to a POI group (health / education / …).
// All zones in the same group share one colour so the map, the POI pin
// backgrounds, and the legend all agree. Residential gets its own
// distinct warm yellow so houses stand out from every other category.
//
// Add a new zone → drop it into ZONE_GROUP below → ZONE_COLORS and
// POI_CATEGORIES (further down) automatically pick up the group hue.
export const GROUP_COLORS = {
  residential: 0xfbbf24,  // warm amber — people actually live here
  health:      0xef4444,  // red (hospitals, pharmacies, police, fire)
  education:   0xa855f7,  // purple
  daily:       0xf97316,  // orange (grocery, park, restaurant, hotel, nightclub, playground, sports)
  culture:     0x8b5cf6,  // violet (churches, castles, museums, theaters, stadiums, cemeteries)
  nature:      0x166534,  // dark forest green
  transport:   0x64748b,  // slate grey
  commercial:  0x0ea5e9,  // sky blue (offices, shops)
  industrial:  0x78716c,  // warm grey (factories, warehouses)
  swedish:     0x005eb8,  // Swedish blue (cultural easter eggs)
};

// Zone integer → POI group. The single source of truth that drives
// ZONE_COLORS (below) and overrides POI_CATEGORIES entry colours
// further down so every surface stays consistent.
const ZONE_GROUP = {
  1:  'residential', 2:  'residential', 3:  'residential',
  4:  'commercial',  5:  'industrial',                             // COMMERCIAL / INDUSTRIAL
  6:  'health',                                                  // HOSPITAL
  7:  'daily',                                                   // PARK
  8:  'education',                                               // SCHOOL (grundskola)
  9:  'health',                                                  // PHARMACY
  10: 'education',                                               // UNIVERSITY
  11: 'daily',                                                   // GROCERY
  12: 'health',                                                  // DENTAL
  13: 'health',                                                  // VETERINARY
  14: 'culture',                                                 // CHURCH
  15: 'culture',                                                 // CASTLE
  16: 'culture',                                                 // MUSEUM
  17: 'culture',                                                 // THEATER
  18: 'culture',                                                 // STADIUM
  19: 'nature',                                                  // WATER (rendered separately)
  20: 'nature',                                                  // FORESTRY (rendered separately)
  21: 'daily',                                                   // NIGHTCLUB
  22: 'daily',                                                   // PLAYGROUND
  23: 'culture',                                                 // CEMETERY
  24: 'education',                                               // KINDERGARTEN
  25: 'education',                                               // HIGH_SCHOOL
  26: 'education',                                               // LIBRARY
  27: 'daily',                                                   // RESTAURANT
  28: 'daily',                                                   // SPORTS_CENTRE
  29: 'daily',                                                   // HOTEL
  30: 'daily',                                                   // COMMUNITY_CENTRE
  31: 'daily',                                                   // MALL
  32: 'transport',                                               // PARKING
  33: 'culture',                                                 // MANOR
  34: 'culture',                                                 // HISTORIC_LANDMARK
  35: 'health',                                                  // POLICE
  36: 'health',                                                  // FIRE_STATION
};

// Derive ZONE_COLORS from the single source above — never hand-edit
// the integer→hex table directly, edit ZONE_GROUP and GROUP_COLORS.
export const ZONE_COLORS = {};
for (const [zid, grp] of Object.entries(ZONE_GROUP)) {
  ZONE_COLORS[Number(zid)] = GROUP_COLORS[grp];
}

const LABEL_ZONES = {
  6: { text: '\u{1F3E5} Hospital', yOffset: 14 },
  7: { text: '\u{1F333} Park',     yOffset:  4 },
  8: { text: '\u{1F3EB} School',   yOffset:  9 },
};

// POI categories shown as Google-Maps style pins.
// Numeric keys = building zones (the integer matches Cell.zone, see osm.py).
// String keys ('airport', 'train', 'bus_station', 'tram', 'bus') = transport
// stops, populated from /api/transport. The pin spawn loop in _buildPOIs
// renders both groups uniformly.
export const POI_CATEGORIES = {
  // ── Health & Safety ──
  6:  { name: 'Hospital',      color: '#ef4444', emoji: '\u{1F3E5}', group: 'health' },  // 🏥
  9:  { name: 'Pharmacy',      color: '#10b981', emoji: '\u{1F48A}', group: 'health' },  // 💊
  12: { name: 'Dental',        color: '#06b6d4', emoji: '\u{1F9B7}', group: 'health' },  // 🦷
  13: { name: 'Veterinary',    color: '#ec4899', emoji: '\u{1F43E}', group: 'health' },  // 🐾
  35: { name: 'Police',        color: '#1e3a5f', emoji: '\u{1F6A8}', group: 'health' },  // 🚨
  36: { name: 'Fire Station',  color: '#b91c1c', emoji: '\u{1F692}', group: 'health' },  // 🚒
  // ── Education ──
  24: { name: 'Kindergarten',   color: '#fb923c', emoji: '\u{1F9F8}', group: 'education' },  // 🧸
  8:  { name: 'Primary school', color: '#f59e0b', emoji: '\u{1F3EB}', group: 'education' },  // 🏫 (grundskola)
  25: { name: 'High school',    color: '#b45309', emoji: '\u{1F393}', group: 'education' },  // 🎓 (gymnasium)
  10: { name: 'University',     color: '#a855f7', emoji: '\u{1F3DB}\u{FE0F}', group: 'education' },  // 🏛️
  26: { name: 'Library',        color: '#7c3aed', emoji: '\u{1F4DA}', group: 'education' },  // 📚
  // ── Daily Life (grocery, leisure, hospitality, recreation) ──
  11: { name: 'Grocery',       color: '#f97316', emoji: '\u{1F6D2}', group: 'daily' },  // 🛒
  7:  { name: 'Park',          color: '#22c55e', emoji: '\u{1F333}', group: 'daily' },  // 🌳
  27: { name: 'Restaurant',    color: '#f97316', emoji: '\u{1F37D}\u{FE0F}', group: 'daily' },  // 🍽️
  30: { name: 'Community centre', color: '#0d9488', emoji: '\u{1F3AA}', group: 'daily' },  // 🎪
  31: { name: 'Shopping mall', color: '#ec4899', emoji: '\u{1F6CD}\u{FE0F}', group: 'daily' },  // 🛍️
  29: { name: 'Hotel',         color: '#1e40af', emoji: '\u{1F3E8}', group: 'daily' },  // 🏨
  21: { name: 'Nightclub',     color: '#a21caf', emoji: '\u{1F379}', group: 'daily' },  // 🍹
  22: { name: 'Playground',    color: '#facc15', emoji: '\u{1F6DD}', group: 'daily' },  // 🛝
  28: { name: 'Sports centre', color: '#84cc16', emoji: '\u{1F3CB}\u{FE0F}', group: 'daily' },  // 🏋️
  // ── Culture (worship, landmarks, heritage) ──
  14: { name: 'Church',        color: '#dc2626', emoji: '\u{26EA}',  group: 'culture' },  // ⛪
  23: { name: 'Cemetery',      color: '#64748b', emoji: '\u{26B0}\u{FE0F}', group: 'culture' },  // ⚰️
  15: { name: 'Castle',        color: '#7c3aed', emoji: '\u{1F3F0}', group: 'culture' },  // 🏰
  16: { name: 'Museum',        color: '#0ea5e9', emoji: '\u{1F3DB}\u{FE0F}', group: 'culture' },  // 🏛️
  17: { name: 'Theater',       color: '#f472b6', emoji: '\u{1F3AD}', group: 'culture' },  // 🎭
  18: { name: 'Stadium',       color: '#65a30d', emoji: '\u{1F3DF}\u{FE0F}', group: 'culture' },  // 🏟️
  33: { name: 'Manor',         color: '#991b1b', emoji: '\u{1F3E1}', group: 'culture' },  // 🏡 (Swedish herrgård)
  34: { name: 'Historic landmark', color: '#a16207', emoji: '\u{1F5FF}', group: 'culture' },  // 🗿
  // ── Nature ──
  water_lake:      { name: 'Lake',       color: '#1d4ed8', emoji: '\u{1F30A}', group: 'nature' },  // 🌊
  water_river:     { name: 'River',      color: '#2563eb', emoji: '\u{1F30A}', group: 'nature' },  // 🌊
  water_pond:      { name: 'Pond',       color: '#3b82f6', emoji: '\u{1F4A7}', group: 'nature' },  // 💧
  water_reservoir: { name: 'Reservoir',  color: '#1e40af', emoji: '\u{1F6B0}', group: 'nature' },  // 🚰
  water_sea:       { name: 'Sea',        color: '#1e3a8a', emoji: '\u{1F30D}', group: 'nature' },  // 🌍
  20: { name: 'Forest',        color: '#14532d', emoji: '\u{1F332}', group: 'nature' },  // 🌲
  nature_reserve: { name: 'Nature reserve', color: '#166534', emoji: '\u{1F33F}', group: 'nature' },  // 🌿
  // ── Transport ──
  airport:     { name: 'Airport',      color: '#0ea5e9', emoji: '\u{2708}\u{FE0F}', group: 'transport' },  // ✈️
  train:       { name: 'Train Station',color: '#dc2626', emoji: '\u{1F686}',        group: 'transport' },  // 🚆
  bus_station: { name: 'Bus Station',  color: '#eab308', emoji: '\u{1F68C}',        group: 'transport' },  // 🚌
  tram:        { name: 'Tram Stop',    color: '#16a34a', emoji: '\u{1F68A}',        group: 'transport' },  // 🚊
  ferry:       { name: 'Ferry Terminal',color: '#0891b2', emoji: '\u{26F4}\u{FE0F}', group: 'transport' },  // ⛴️
  bus:         { name: 'Bus Stop',     color: '#94a3b8', emoji: '\u{1F68F}',        group: 'transport' },  // 🚏
  32: { name: 'Parking',       color: '#475569', emoji: '\u{1F17F}\u{FE0F}', group: 'transport' },  // 🅿️
  // ── Swedish Secrets ──
  swedish_flag:   { name: 'Swedish flag',   color: '#005EB8', emoji: '\u{1F1F8}\u{1F1EA}', group: 'swedish' },  // 🇸🇪
  lnu_flag:       { name: 'LNU flag',       color: '#0e2a66', emoji: '\u{1F393}', group: 'swedish' },  // 🎓
  dalahast:       { name: 'Dala Horse',     color: '#dc2626', emoji: '\u{1F40E}', group: 'swedish' },  // 🐎
  ikea_bag:       { name: 'IKEA Bag',       color: '#2563eb', emoji: '\u{1F6CD}\u{FE0F}', group: 'swedish' },  // 🛍️
  fika_cup:       { name: 'Fika Cup',       color: '#92400e', emoji: '\u{2615}',  group: 'swedish' },  // ☕
  maypole:        { name: 'Maypole',        color: '#16a34a', emoji: '\u{1F33B}', group: 'swedish' },  // 🌻
  moose:          { name: 'Moose',          color: '#78350f', emoji: '\u{1F98C}', group: 'swedish' },  // 🦌
};

// Display metadata for each POI group. The `order` field drives the
// render order in the POI panel (top to bottom). Groups not listed here
// fall back to an "Other" group rendered last. Add a new entry to this
// map when introducing a new POI category theme.
export const POI_GROUPS = {
  health:    { name: 'Health & Safety',  emoji: '\u{1F489}', order: 1 },  // 💉
  education: { name: 'Education',        emoji: '\u{1F393}', order: 2 },  // 🎓
  daily:     { name: 'Daily Life',       emoji: '\u{1F6D2}', order: 3 },  // 🛒
  culture:   { name: 'Culture',          emoji: '\u{1F3DB}\u{FE0F}', order: 4 },  // 🏛️
  nature:    { name: 'Nature',           emoji: '\u{1F332}', order: 5 },  // 🌲
  transport: { name: 'Transport',        emoji: '\u{1F68C}', order: 6 },  // 🚌
  swedish:   { name: 'Swedish Secrets',  emoji: '\u{1F1F8}\u{1F1EA}', order: 7 }, // 🇸🇪
};

// ── i18n-aware POI name lookup ───────────────────────────────────────────────
// Maps POI category keys (zone int or string) to i18n translation keys.
const _POI_I18N = {
  6:'poi_hospital', 9:'poi_pharmacy', 12:'poi_dental', 13:'poi_veterinary',
  24:'poi_kindergarten', 8:'poi_school', 25:'poi_high_school', 10:'poi_university', 26:'poi_library',
  11:'poi_grocery', 7:'poi_park', 27:'poi_restaurant', 30:'poi_community', 31:'poi_mall',
  14:'poi_church', 23:'poi_cemetery',
  15:'poi_castle', 16:'poi_museum', 17:'poi_theater', 18:'poi_stadium', 33:'poi_manor', 34:'poi_historic',
  19:'poi_lake', water_lake:'poi_water_lake', water_river:'poi_water_river', water_pond:'poi_water_pond', water_reservoir:'poi_water_reservoir', water_sea:'poi_water_sea',
  20:'poi_forest', nature_reserve:'poi_nature_reserve',
  swedish_flag:'poi_swedish_flag', lnu_flag:'poi_lnu_flag',
  dalahast:'poi_dalahast', ikea_bag:'poi_ikea_bag', fika_cup:'poi_fika_cup', maypole:'poi_maypole', moose:'poi_moose',
  35:'poi_police', 36:'poi_fire_station',
  21:'poi_nightclub', 22:'poi_playground', 28:'poi_sports_centre',
  airport:'poi_airport', train:'poi_train_station', bus_station:'poi_bus_station',
  tram:'poi_tram_stop', ferry:'poi_ferry_terminal', bus:'poi_bus_stop', 32:'poi_parking',
  29:'poi_hotel',
};
const _GROUP_I18N = {
  health:'poigrp_health', education:'poigrp_education', daily:'poigrp_daily',
  culture:'poigrp_culture', nature:'poigrp_nature',
  transport:'poigrp_transport', swedish:'poigrp_swedish',
};
/** Get translated POI category name. */
export function poiName(key) {
  const i18nKey = _POI_I18N[key];
  return i18nKey ? t(i18nKey) : (POI_CATEGORIES[key]?.name ?? String(key));
}
/** Get translated POI group name. */
export function poiGroupName(groupKey) {
  const i18nKey = _GROUP_I18N[groupKey];
  return i18nKey ? t(i18nKey) : (POI_GROUPS[groupKey]?.name ?? String(groupKey));
}

// Set of POI category keys that come from transport stops, NOT from
// buildings. Used by _buildPOIs() to skip these in the building loop.
// Post-declaration pass: rewrite every POI_CATEGORIES entry's .color
// field to its GROUP_COLORS hex so the map zone colour, the map POI
// pin background, and the legend swatch all agree. Individual per-
// category colours are not preserved — the point is visual coherence
// across the whole UI.
for (const cat of Object.values(POI_CATEGORIES)) {
  if (!cat.group) continue;
  const hex = GROUP_COLORS[cat.group];
  if (typeof hex === 'number') {
    cat.color = '#' + hex.toString(16).padStart(6, '0');
  }
}

const _TRANSPORT_POI_KEYS = new Set(['airport', 'train', 'bus_station', 'tram', 'ferry', 'bus']);

// Set of lightweight nature POI keys — pins only, no Cell, no SEIR.
// Delivered via layout.nature_pois (see osm._load_nature_pois), stored
// in _naturePOIs, spawned by _buildPOIs() through the same path as
// transport stops. Cemeteries were moved to a full zone (CEMETERY=23,
// worship group) so they participate in the SEIR kernel alongside
// churches — they are NOT in this set anymore.
const _NATURE_POI_KEYS = new Set(['nature_reserve']);

// Per-transport-type Y offset (world units) for the pin float height.
// Sized to sit just above the procedural transport buildings created in
// static/transport.js (whose heights range 0.5 m for trams up to ~22 m
// for the airport tower).
const _TRANSPORT_PIN_BASE_Y = {
  airport:     35,
  train:       22,
  ferry:       18,
  bus_station: 14,
  tram:        10,
  bus:          8,
};

// Special non-zone POI category: marks buildings where the initial
// outbreak was seeded. Driven by setPatientZeroIndices() rather than
// by building zones, but rendered through the same pin pipeline.
export const PATIENT_ZERO_KEY  = 'patient_zero';
export const PATIENT_ZERO_META = {
  get name() { return t('poi_patient_zero'); },
  color: '#dc2626',           // strong red
  emoji: '\u{1F9A0}',         // 🦠 microbe
};

// ── Module state ──────────────────────────────────────────────────────────────

let renderer, scene, camera;
// Optional secondary WebGLRenderer wired to map B's GL context for the
// side-by-side compare view. Created on demand by attachSecondaryRenderer
// so the same scene (3D trees, animated dashed lakes, landmarks) renders
// into both halves; the cmp-buildings MapLibre fill-extrusion overlays
// the per-day SEIR colours on top of three.js nature.
let _rendererB = null;
// In split mode we hide every three.js mesh that the cmp-buildings
// MapLibre layer is responsible for (regular buildings, landmarks, POI
// pins, street lights) so the two layers never duplicate. Nature stays
// visible — that's the whole point of using three.js on both halves.
let _splitMode = false;
let _sunLight         = null;
let _ambientLight     = null;
let _shadowGround     = null;   // invisible plane that catches sun shadows
let _dayNightBlend    = 0;      // 0 = flat daylight, 1 = full day/night cycle
let _dayNightTarget   = 0;      // tween target (0 or 1)
const _DAY_NIGHT_FADE = 2.2;    // units per second (≈450 ms full fade)
let _buildingMesh    = null;   // merged polygon extrusion mesh
let _vertexRanges    = [];     // [{start, count}] per building (vertex index space)
let _buildings        = [];
// ── Nature detail layer ──────────────────────────────────────────────────
// The Nature button (setNatureDetailVisible) gates ONLY the batched
// tree scatter (buildNaturalDetailLayer). Water is a dashed animated
// blue outline and is unaffected by the toggle; forests have no base
// footprint at all, so when Nature is off they vanish entirely.
let _natureDetailOn   = false; // persistent across city switches

// ── Water layer (dashed animated borders, hover-only, no SEIR) ───────────
// Lakes and rivers are drawn by a single thick-dashed blue polyline
// mesh (LineSegments2 + LineMaterial with animated dashOffset) that
// traces every water polygon's outline. Raycasts use an INVISIBLE
// companion mesh (_waterRaycastMesh, a merged ShapeGeometry whose
// material writes neither colour nor depth) so the user can hover a
// lake anywhere on its surface to see its name + area — but click
// events on water are dropped by _onClick so water is never the
// selected cell.
let _waterLineMesh    = null;
let _waterLineMat     = null;  // shared LineMaterial — animated dashOffset
let _waterRaycastMesh = null;  // invisible flat plane used only for picking
let _waterCells       = [];    // parallel to _waterRanges
let _waterRanges      = [];    // [{start, count}] per water cell, vtx idx space
let _waterLineClockT0 = 0;
let _appearanceGroup  = null;
let _landmarkGroup    = null;   // always-on custom-asset buildings (e.g. Växjö Cathedral)
/** Scene-wide batched tree scatter for FORESTRY polygons (one Group,
 *  ≤16 InstancedMesh draw calls for the entire city). Built by
 *  buildNaturalDetailLayer() in buildings_custom.js. */
let _naturalGroup     = null;
/** Map<buildingIdx, THREE.Group> — landmark roots keyed by building index. */
let _landmarkByIdx    = new Map();
let _appearanceLayout = null;   // saved for lazy construction
let _labelGroup       = null;
let _origYs           = null;  // Float32Array — original Y coordinate per vertex
let _baseHeights      = [];    // per-building original extrusion height
let _lastCells        = null;  // last cells array (for re-applying heights on mode change)
const _NO_COLOR_HEX   = 0x9ca3af;   // grey for "no color" mode

let _selectedCell = null;
let _selectedIdx  = -1;
let _selCb        = null;
let _selIndicator = null;

// Patient-zero pick mode: when true, updateColors dims every non-
// residential cell to grey so the user can see at a glance which
// buildings are clickable targets. Residential (zones 1–3) stay in
// full colour. Toggled from ui.js via setPickPatientZeroMode().
let _pickPatientZeroMode = false;

// ── Street lights state (built once per buildCity) ──────────────────────────
// A single THREE.Points cloud sampled along the road network. Each point
// renders as a soft warm circular glow via a CanvasTexture, additively
// blended so overlapping pools brighten naturally. Visibility + opacity
// are driven by updateDayNight() so they only appear after sunset.
let _streetLights = null;

// ── Cloud state ──────────────────────────────────────────────────────────────
// Clouds are rendered on the same 2D #snowCanvas overlay as snow/rain
// (see _updateWeather). Soft drifting blobs in the upper portion of
// the screen, on top of EVERYTHING — guaranteed visibility regardless
// of camera angle or zoom (the previous Three.js plane meshes were too
// easy to miss because they competed with the building geometry depth).
//
// Distinct from precipitation: clouds DON'T trigger the overcast
// lighting attenuation (no _wxIntensity bump), they're a purely visual
// overlay. They have their own independent _cloudIntensity tween.
const WEATHER_CLOUDY      = 'cloudy';   // soft variant of CLEAR (no precip)
const _CLOUD_SPRITE_COUNT = 4;          // fewer sprites = more discreet
const _CLOUD_FADE         = 0.30;       // intensity tween (per second)
// Cloud probability is no longer a flat constant — it's derived from
// _SEASON_PROBS inside notifyNewDay() (cloudy / (cloudy + clear) on a
// clear-precipitation day). Higher in winter/spring/autumn, lower in summer.

let _cloudsToday       = false;
let _cloudIntensity    = 0;             // current visible alpha (0..1)
let _cloudTarget       = 0;             // tween target
const _clouds          = [];            // sprite list, populated by _initWeather
let _cloudSpriteCanvas = null;          // pre-rendered cloud silhouette

// ── POI pin state ────────────────────────────────────────────────────────────
let _poiGroup     = null;                   // THREE.Group holding all pin meshes
let _poiVisible   = false;                  // master on/off toggle
// Category key (zone ID as string OR PATIENT_ZERO_KEY) → bool; default all true
const _poiCatVisible = {};
const _poiTextures   = {};                  // category key → THREE.CanvasTexture
const _poiMaterials  = {};                  // category key → shared material
const _poiSubGroups  = {};                  // category key → THREE.Group
let   _poiPinGeo     = null;                // shared PlaneGeometry for all pins
let   _poiAllMeshes  = [];                  // flat list of every pin mesh
let   _patientZeroIdx = [];                 // building indices flagged as patient zero
let   _transportStops = [];                 // transport stops (from /api/transport)
let   _naturePOIs     = [];                 // lightweight nature POIs (from layout.nature_pois)
let   _flagPoles      = [];                 // Swedish flag pole easter egg (from layout.flag_poles)
let   _flagPoleGroup  = null;                // always-visible 3D flag pole assets
let   _lnuFlagPoles   = [];                  // LNU university flags (from layout.lnu_flag_poles)
let   _lnuFlagPoleGroup = null;              // always-visible LNU flag assets
let   _easterEggs     = [];                  // Swedish cultural easter eggs (from layout.easter_eggs)
let   _easterEggGroup = null;                // always-visible easter egg 3D assets
// Default visibility per category. Building POIs default ON; transport
// categories default ON for the rare ones (airport/train/bus_station/tram)
// and OFF for individual bus stops because cities like Uppsala have
// hundreds of them and they'd swamp the scene.
for (const z of Object.keys(POI_CATEGORIES)) {
  _poiCatVisible[z] = (z !== 'bus');   // bus stops off by default
}
_poiCatVisible[PATIENT_ZERO_KEY] = true;

// Active interventions, mirrored from the engine on every state update.
// Drives in-scene visuals like the closed-school marker on school POI pins.
let _activeInterventions = new Set();
// Map of intervention id → list of POI category keys whose pins should
// render the "closed" overlay when that intervention is active. Extending
// this to other interventions just means adding entries here.
const POI_CLOSED_BY_INTERVENTION = {
  schools:         ['8'],                                              // Zone.SCHOOL
  suspend_flights: ['airport'],                                        // grounded
  // Full lockdown closes every public-transport stop visually
  // (and the dispatcher skips all trips on the backend).
  lockdown:        ['airport', 'train', 'bus_station', 'tram', 'bus', 'ferry'],
  // Stop Public Transport — same closure overlay on every transit pin
  // and the airport (no flights either while it's on).
  stop_transport:  ['airport', 'train', 'bus_station', 'tram', 'bus', 'ferry'],
};

let hoverCb = null;
let _areasOverlayActive = false;

// Default to 'none' (flat grey) so the very first frame after buildCity()
// is neutral. ui.js immediately calls setColorMode() with the legend's
// real choice, so the user only ever sees grey → real-mode, never
// zone-colours flickering through.
let _colorMode      = 'none';
let _heightMode     = 'zone';   // height is fixed at OSM levels; mode kept for API compat
let _colorScale     = 'plasma';
let _logScale       = false;
let _invertScale    = false;
let _appearanceMode = false;
let _maxN           = 1;
let _maxI           = 1;
let _maxD           = 1;
let _maxE           = 1;
let _maxS           = 1;
let _maxR           = 1;
// "Infections here" — peak per-building count of S→E transmission
// events that occurred AT this building (state.cells[i].infections_here,
// emitted only by the ABM engine). Distinct from _maxI which tracks the
// peak count of infected residents per building. Used by the
// 'infections_here' colour + height mode for live normalisation.
let _maxIH          = 1;
let _incomeLo       = 0;   // DeSO mean net income min (SEK thousands)
let _incomeHi       = 1;   // DeSO mean net income max (SEK thousands)

// Stub used when a building has no SEIR entry — keeps the gradient pipeline
// happy without a fall-through to zone colour.
const _EMPTY_SEIR = Object.freeze({ S: 0, E: 0, I: 0, R: 0, D: 0, N: 0 });

// Log-normalize a value into [0,1] given a max. Uses log1p so 0 → 0 cleanly.
function _norm(v, max) {
  if (max <= 0) return 0;
  if (_logScale) return Math.log1p(Math.max(v, 0)) / Math.log1p(max);
  return v / max;
}

// Like _norm but applies invert for color sampling (not height).
function _cnorm(v, max) {
  const t = _norm(v, max);
  return _invertScale ? (1 - t) : t;
}

/**
 * Same idea as _normValue but for HEIGHTS — no invert applied (height
 * shouldn't flip when the user inverts the colour scale).
 */
function _normHeight(metric, v, nCap, liveMax) {
  let denom;
  switch (_normMode) {
    case 'cumulative':
      denom = ({ N:_cumMaxN, I:_cumMaxI, E:_cumMaxE, S:_cumMaxS, R:_cumMaxR, D:_cumMaxD })[metric] || 1;
      break;
    case 'capacity':
      denom = Math.max(1, nCap || 1);
      break;
    case 'total':
      // Precomputed once per render in updateColors.
      denom = ({ N:_totN, I:_totI, E:_totE, S:_totS, R:_totR, D:_totD })[metric] || 1;
      break;
    case 'live':
    default:
      denom = liveMax || 1;
  }
  return _norm(v, denom);
}

/**
 * "Cumulative reach" — the fraction of a building's capacity that has
 * been touched by the outbreak so far. Equals (cap − S)/cap, always
 * monotonic-growing because S only ever decreases (waning aside).
 * Returns the ramp value already in [0, 1]; the caller picks colour
 * vs height ramp height.
 */
function _cumulativeReach(seir, b) {
  const cap = (b.pop && b.pop > 0) ? b.pop : (seir?.N || 0);
  if (cap <= 0) return 0;
  const ever = Math.max(0, cap - (seir?.S ?? cap));
  return Math.min(1, ever / cap);
}

/**
 * Normalisation router: returns the 0..1 colour-sample value for `v`
 * (the raw count) given the current `_normMode` + the building's
 * capacity `nCap` (its N — used by 'capacity' mode).
 *
 *   metric ∈ {'N','I','E','S','R','D'}
 *   nCap   = building's current N (capacity proxy)
 *   idx    = building idx (only needed for 'monotonic' mode)
 */
function _normValue(metric, v, nCap) {
  let denom;
  switch (_normMode) {
    case 'cumulative':
      denom = ({ N:_cumMaxN, I:_cumMaxI, E:_cumMaxE, S:_cumMaxS, R:_cumMaxR, D:_cumMaxD })[metric] || 1;
      break;
    case 'capacity':
      denom = Math.max(1, nCap || 1);
      break;
    case 'total':
      denom = ({ N:_totN, I:_totI, E:_totE, S:_totS, R:_totR, D:_totD })[metric] || 1;
      break;
    case 'live':
    default:
      denom = ({ N:_maxN, I:_maxI, E:_maxE, S:_maxS, R:_maxR, D:_maxD })[metric] || 1;
  }
  return _cnorm(v, denom);
}

// Current MapLibre MVP matrix for raycasting
let _mvpMatrix   = null;
let _mapInstance = null;
let _lastVisWidth = 5000;  // visible world width in metres, updated each frame

const _col = new THREE.Color();
const _red = new THREE.Color(0xef4444);
const _yel = new THREE.Color(0xfbbf24);
const _blu = new THREE.Color(0x60a5fa);

// ── Public API ────────────────────────────────────────────────────────────────

export function getScene() { return scene; }
export function getIncomeRange() { return { lo: _incomeLo, hi: _incomeHi }; }
export function getMVPMatrix() { return _mvpMatrix; }
/** Return the map canvas bounding rect (CSS pixels, viewport-relative). */
export function getCanvasRect() {
  if (!renderer) return null;
  return renderer.domElement.getBoundingClientRect();
}

/**
 * Called from map.js CustomLayerInterface.onAdd(map, gl).
 */
export function initView(mapInstance, gl, _centerMC, _scale) {
  _mapInstance = mapInstance;

  renderer = new THREE.WebGLRenderer({
    canvas:    gl.canvas,
    context:   gl,
    antialias: true,
  });
  renderer.autoClear = false;
  renderer.outputColorSpace = THREE.SRGBColorSpace;

  // Shadow maps — off by default (sun.castShadow=false) but preconfigured
  // so the Day/Night toggle can flip them without reinitialising anything.
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type    = THREE.PCFSoftShadowMap;

  scene  = new THREE.Scene();
  camera = new THREE.Camera();

  // Lighting — module-level refs so setDayNight/updateDayNight can mutate them
  _ambientLight = new THREE.AmbientLight(0xffffff, 0.65);
  scene.add(_ambientLight);

  _sunLight = new THREE.DirectionalLight(0xfff8f0, 1.1);
  _sunLight.position.set(500, 800, 300);
  // castShadow is left ON permanently so Three.js allocates the shadow
  // render target up-front. The toggle hides shadows by fading the
  // ground-shadow plane's opacity in updateDayNight() instead of flipping
  // this flag (which caused a one-frame reset flash).
  _sunLight.castShadow = true;
  _sunLight.shadow.mapSize.set(2048, 2048);
  {
    const d = 2000;
    _sunLight.shadow.camera.left   = -d;
    _sunLight.shadow.camera.right  =  d;
    _sunLight.shadow.camera.top    =  d;
    _sunLight.shadow.camera.bottom = -d;
    _sunLight.shadow.camera.near   = 100;
    _sunLight.shadow.camera.far    = 4000;
    _sunLight.shadow.bias          = -0.0005;
  }
  scene.add(_sunLight);
  scene.add(_sunLight.target);

  gl.canvas.addEventListener('mousemove', _onMouseMove);
  gl.canvas.addEventListener('click',     _onClick);
}

/**
 * Build extruded polygon mesh from city layout.
 * @param {{ buildings: Array }} layout  — /api/city response
 */
export function buildCity(layout) {
  // Reproject every building polygon and centroid from osm.py's
  // equirectangular-at-centre coords to local Web Mercator coords. The
  // three.js scene is rendered through MapLibre's custom layer using a
  // SINGLE Mercator scale value (`meterInMercatorCoordinateUnits` at
  // the city centre); the equirect-at-centre coords diverge from where
  // MapLibre actually projects each lat/lon, especially toward the
  // bbox corners and at higher latitudes. Reprojecting up-front makes
  // every three.js mesh — buildings, landmarks, POIs, water lines,
  // tree scatter — line up with the basemap and the admin-area /
  // road / cmp-buildings layers (those go via the lat/lon round-trip
  // and are already correct). Idempotent thanks to a sentinel flag,
  // so re-entering buildCity for a re-coloured layout is safe.
  _reprojectLayoutForMercator(layout);

  _buildings = layout.buildings;

  // Dispose previous
  if (_buildingMesh) {
    scene.remove(_buildingMesh);
    _buildingMesh.geometry.dispose();
    _buildingMesh.material.dispose();
    _buildingMesh = null;
  }
  if (_appearanceGroup) { scene.remove(_appearanceGroup); _appearanceGroup = null; }
  if (_landmarkGroup) {
    scene.remove(_landmarkGroup);
    _landmarkGroup.traverse(o => {
      if (o.isMesh) { o.geometry.dispose?.(); }
    });
    _landmarkGroup = null;
  }
  if (_naturalGroup) {
    scene.remove(_naturalGroup);
    _naturalGroup.traverse(o => {
      if (o.isMesh) { o.geometry.dispose?.(); }
    });
    _naturalGroup = null;
  }
  if (_waterLineMesh) {
    scene.remove(_waterLineMesh);
    _waterLineMesh.geometry.dispose();
    _waterLineMat?.dispose();
    _waterLineMesh = null;
    _waterLineMat  = null;
  }
  if (_flagPoleGroup) {
    scene.remove(_flagPoleGroup);
    _flagPoleGroup.traverse(o => {
      if (o.isMesh) { o.geometry.dispose?.(); }
    });
    _flagPoleGroup = null;
  }
  if (_lnuFlagPoleGroup) {
    scene.remove(_lnuFlagPoleGroup);
    _lnuFlagPoleGroup.traverse(o => {
      if (o.isMesh) { o.geometry.dispose?.(); }
    });
    _lnuFlagPoleGroup = null;
  }
  if (_waterRaycastMesh) {
    scene.remove(_waterRaycastMesh);
    _waterRaycastMesh.geometry.dispose();
    _waterRaycastMesh.material.dispose();
    _waterRaycastMesh = null;
  }
  _waterCells  = [];
  _waterRanges = [];
  _landmarkByIdx = new Map();
  _appearanceLayout = null;
  if (_labelGroup) {
    scene.remove(_labelGroup);
    _labelGroup.children.forEach(s => s.material.map?.dispose());
    _labelGroup = null;
  }
  _disposeSelectionIndicator();
  _selectedCell = null;
  _selectedIdx  = -1;
  _vertexRanges = [];

  // Drop any previous POI pin layer — it's rebuilt below once heights exist
  if (_poiGroup) {
    scene.remove(_poiGroup);
    _poiGroup.traverse(o => { if (o.isSprite) o.material.dispose(); });
    _poiGroup = null;
  }

  // ── Build one ExtrudeGeometry per building, track vertex ranges ───────────
  const geos   = [];
  let vtxOffset = 0;
  _baseHeights  = new Array(_buildings.length).fill(0);

  for (let i = 0; i < _buildings.length; i++) {
    const b = _buildings[i];

    // Landmark buildings with a custom 3D asset (Växjö Cathedral, etc.) are
    // rendered by the always-on landmark layer below. Skip them from the
    // generic extruded-polygon mesh so the footprint isn't drawn as a
    // coloured block covering the asset. Push a zero-count range to keep
    // _vertexRanges aligned with _buildings by index — _buildingAtVertex's
    // binary search treats it as empty and moves past.
    if (b.custom_builder) {
      _vertexRanges.push({ start: vtxOffset, count: 0 });
      continue;
    }
    // Water (dashed animated outline via _waterLineMesh, invisible
    // raycast proxy for hover) and forestry (tree scatter via
    // buildNaturalDetailLayer) are NEVER in the main merged mesh —
    // that way the colour / height pipelines leave them alone.
    if (b.zone === 19 || b.zone === 20) {
      _vertexRanges.push({ start: vtxOffset, count: 0 });
      continue;
    }

    const built = _makeExtrudeGeo(b);
    if (!built) {
      _vertexRanges.push({ start: vtxOffset, count: 0 });
      continue;
    }

    _baseHeights[i] = built.height;
    const geo = built.geo;
    const vtxCount = geo.attributes.position.count;
    _vertexRanges.push({ start: vtxOffset, count: vtxCount });
    vtxOffset += vtxCount;
    geos.push(geo);
  }

  // ── Merge into a single mesh ──────────────────────────────────────────────
  const merged = mergeGeometries(geos, false);   // false = keep non-indexed

  // Cache original Y per vertex so dynamic height-mode scaling can use it.
  const posArr = merged.attributes.position.array;
  _origYs = new Float32Array(vtxOffset);
  for (let v = 0; v < vtxOffset; v++) _origYs[v] = posArr[v * 3 + 1];

  // Per-vertex RGB colour (one colour per building, replicated to all its vertices)
  const colorArr = new Float32Array(vtxOffset * 3);
  _buildings.forEach((b, i) => {
    const range = _vertexRanges[i];
    if (!range) return;
    const hex = _colorMode === 'none'
      ? _NO_COLOR_HEX
      : (ZONE_COLORS[b.zone] ?? 0x888888);
    const r   = ((hex >> 16) & 0xff) / 255;
    const g   = ((hex >> 8)  & 0xff) / 255;
    const bv  = ( hex        & 0xff) / 255;
    for (let v = range.start; v < range.start + range.count; v++) {
      colorArr[v * 3]     = r;
      colorArr[v * 3 + 1] = g;
      colorArr[v * 3 + 2] = bv;
    }
  });
  merged.setAttribute('color', new THREE.BufferAttribute(colorArr, 3));

  _buildingMesh = new THREE.Mesh(
    merged,
    new THREE.MeshLambertMaterial({ vertexColors: true, side: THREE.DoubleSide }),
  );
  _buildingMesh.castShadow    = true;
  _buildingMesh.receiveShadow = true;
  scene.add(_buildingMesh);

  // Invisible shadow-receiver at ground level. MapLibre renders the actual
  // ground, so we only need a plane whose sole job is catching Three.js
  // shadows — ShadowMaterial is transparent except where a shadow falls.
  if (_shadowGround) {
    scene.remove(_shadowGround);
    _shadowGround.geometry.dispose();
    _shadowGround.material.dispose();
  }
  _shadowGround = new THREE.Mesh(
    new THREE.PlaneGeometry(8000, 8000),
    new THREE.ShadowMaterial({ opacity: 0.0 }),   // opacity tweened by blend
  );
  _shadowGround.rotation.x    = -Math.PI / 2;
  _shadowGround.position.y    = 0.05;
  _shadowGround.receiveShadow = true;
  _shadowGround.visible       = true;
  scene.add(_shadowGround);

  // Swedish appearance layer is built lazily on first toggle (6264 buildings
  // × ~5 meshes each would freeze the browser on load).
  _appearanceLayout = layout;

  // Water layer — dashed animated shoreline outlines + an invisible
  // raycast plane for hover tooltips. Always visible; unaffected by
  // the Nature button or the colour/height modes.
  _buildWaterLineLayer(layout);
  _waterLineClockT0 = performance.now();

  // Scene-wide natural-features detail layer — species-aware tree scatter
  // across every FORESTRY polygon, batched into ~8–16 InstancedMesh draws
  // total. Gated by the Nature header button; forests have no other
  // rendering so "Nature off" leaves them invisible.
  _naturalGroup = buildNaturalDetailLayer(layout, scene, THREE);
  if (_naturalGroup) _naturalGroup.visible = _natureDetailOn;

  // Always-on landmark layer — custom 3D assets (Växjö Cathedral, etc.)
  // defined via the manual overrides pipeline. Rendered in BOTH info and
  // appearance mode, which is why it's a separate group from either.
  _landmarkGroup = buildLandmarks(layout, scene);
  for (const child of _landmarkGroup.children) {
    const idx = child.userData.buildingIdx;
    if (idx !== undefined) {
      _landmarkByIdx.set(idx, child);
      // Mirror the landmark's natural height into _baseHeights so the
      // shared height-mode pipeline in _applyHeights() has a valid
      // reference for this building.
      if (child.userData.baseHeight) {
        _baseHeights[idx] = child.userData.baseHeight;
      }
    }
  }

  // Street lights — sampled along the road network, only visible at night.
  _buildStreetLights(layout.roads);

  // Rebuild POI pins if the user already had them enabled before a reset.
  if (_poiVisible) _buildPOIs();

  // Selection indicator is built per-click in _setSelectionBuilding() so it
  // takes the exact shape of the selected building's footprint polygon.
}

export function setColorMode(mode)   {
  _colorMode = mode;
  if (_lastCells) updateColors(_lastCells);
}

/**
 * Toggle the "patient zero pick" visual hint. When enabled, every
 * non-residential cell is forcibly repainted grey so the user can
 * see which buildings are valid targets. The residential palette
 * (zones 1–3) stays in its normal colour.
 */
export function setPickPatientZeroMode(enabled) {
  _pickPatientZeroMode = !!enabled;
  if (_lastCells) updateColors(_lastCells);
}
export function setHeightMode(mode)  {
  _heightMode = mode;
  _applyHeights(_lastCells);
}
export function setColorScale(scale) {
  _colorScale = scale;
  if (_lastCells) updateColors(_lastCells);
}
export function setLogScale(enabled) {
  _logScale = !!enabled;
  if (_lastCells) updateColors(_lastCells);
}
export function setInvertScale(enabled) {
  _invertScale = !!enabled;
  if (_lastCells) updateColors(_lastCells);
}
// Normalisation mode for color/height gradients. 'live' (default),
// 'cumulative' (running max never decreases), 'capacity' (per-building
// fraction of N), 'total' (city-wide totals). View.js applies it
// inside the per-building color + height computation.
let _normMode = 'live';
export function setNormMode(mode) {
  _normMode = mode || 'live';
  // Reset trackers on entry so the user sees a clean baseline.
  if (_normMode === 'cumulative') {
    _cumMaxN = _cumMaxI = _cumMaxE = _cumMaxS = _cumMaxR = _cumMaxD = 1;
  } else if (_normMode === 'monotonic') {
    _resetBuildingMax();
  }
  if (_lastCells) updateColors(_lastCells);
}
// Cumulative-mode trackers: running max across the full run so peaks
// never shrink visually once they're reached.
let _cumMaxN = 1, _cumMaxI = 1, _cumMaxE = 1, _cumMaxS = 1, _cumMaxR = 1, _cumMaxD = 1;
// Total-mode totals: precomputed once per render so the inner draw
// loop is O(buildings), not O(buildings²) — that quadratic blow-up is
// what froze large cities when 'total' mode was selected.
let _totN = 1, _totI = 1, _totE = 1, _totS = 1, _totR = 1, _totD = 1;
// Monotonic-per-building trackers: each building's all-time max for
// every metric. Heights / colours computed against this never decrease
// for a given building — once it has hosted N infections it visually
// stays at that level even after it recovers.
const _bmaxI = new Map();
const _bmaxE = new Map();
const _bmaxS = new Map();
const _bmaxR = new Map();
const _bmaxD = new Map();
function _bumpBuildingMax(map, idx, v) {
  const cur = map.get(idx) || 0;
  if (v > cur) map.set(idx, v);
}
function _resetBuildingMax() {
  _bmaxI.clear(); _bmaxE.clear(); _bmaxS.clear();
  _bmaxR.clear(); _bmaxD.clear();
}
export function setSelectionCallback(fn) { _selCb = fn; }
export function setHoverCallback(fn) { hoverCb = fn; }
/** Suppress building hover + click when the area overlay is active. */
export function setAreasOverlay(active) { _areasOverlayActive = active; }

// ── Street lights ────────────────────────────────────────────────────────────
// One THREE.Points cloud sampled along the road network. Each point is a
// soft warm circular glow drawn additively, gated on the night portion of
// the day/night blend so they only show after sunset.

const _STREET_LIGHT_INTERVAL_M = 40;   // sampling distance along road polylines
const _STREET_LIGHT_HEIGHT_M   = 6;    // pole height above ground

// Screen-space size bounds for the lights. Because we're inside MapLibre's
// custom layer, PointsMaterial.sizeAttenuation produces an INVERTED size
// curve (huge when zoomed out, tiny when zoomed in — opposite of what
// you want). We disable size attenuation and instead recompute the size
// in pixels every frame from the current visible-world width, mirroring
// the POI pin trick in _updatePOIs.
const _STREET_LIGHT_MIN_PX = 3.0;       // far / overview
const _STREET_LIGHT_MAX_PX = 15.0;      // close / street level
// Tuned so the size hits MAX around visWidth ≈ 600 m (street-level zoom)
// and decays toward MIN as visWidth grows past ~5000 m (city overview).
const _STREET_LIGHT_SIZE_K = 9000;

let _streetLightGlowTex = null;        // shared CanvasTexture, built once

function _makeStreetLightGlowTexture() {
  if (_streetLightGlowTex) return _streetLightGlowTex;
  const SIZE = 64;
  const c = document.createElement('canvas');
  c.width = c.height = SIZE;
  const ctx = c.getContext('2d');
  // Soft warm radial gradient — bright white-yellow core, fading to
  // transparent amber at the edge. Additive blending will brighten the
  // surrounding pixels rather than darken them.
  const g = ctx.createRadialGradient(SIZE / 2, SIZE / 2, 0, SIZE / 2, SIZE / 2, SIZE / 2);
  g.addColorStop(0.00, 'rgba(255,245,200,1.00)');
  g.addColorStop(0.20, 'rgba(255,220,140,0.85)');
  g.addColorStop(0.55, 'rgba(255,180, 70,0.30)');
  g.addColorStop(1.00, 'rgba(255,160, 40,0.00)');
  ctx.fillStyle = g;
  ctx.fillRect(0, 0, SIZE, SIZE);
  _streetLightGlowTex = new THREE.CanvasTexture(c);
  _streetLightGlowTex.needsUpdate = true;
  return _streetLightGlowTex;
}

/**
 * Walk every road polyline and emit one (x, z) sample every
 * `interval` metres. The leftover at the end of one segment is carried
 * over so spacing stays even across vertices.
 */
function _sampleRoadPoints(roads, interval) {
  const out = [];
  for (const road of roads) {
    const pts = road.points;
    if (!pts || pts.length < 2) continue;
    let leftover = 0;
    for (let i = 0; i < pts.length - 1; i++) {
      const [x1, z1] = pts[i];
      const [x2, z2] = pts[i + 1];
      const dx = x2 - x1, dz = z2 - z1;
      const segLen = Math.hypot(dx, dz);
      if (segLen <= 0) continue;
      let d = leftover;
      while (d < segLen) {
        const t = d / segLen;
        out.push([x1 + dx * t, z1 + dz * t]);
        d += interval;
      }
      leftover = d - segLen;
    }
  }
  return out;
}

function _buildStreetLights(roads) {
  // Dispose previous (e.g. on city switch)
  if (_streetLights) {
    scene.remove(_streetLights);
    _streetLights.geometry.dispose();
    _streetLights.material.dispose();
    _streetLights = null;
  }
  if (!roads || roads.length === 0) return;

  const samples = _sampleRoadPoints(roads, _STREET_LIGHT_INTERVAL_M);
  if (samples.length === 0) return;

  const positions = new Float32Array(samples.length * 3);
  for (let i = 0; i < samples.length; i++) {
    positions[3 * i + 0] = samples[i][0];
    positions[3 * i + 1] = _STREET_LIGHT_HEIGHT_M;
    positions[3 * i + 2] = samples[i][1];
  }
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));

  const mat = new THREE.PointsMaterial({
    map:              _makeStreetLightGlowTexture(),
    color:            0xffd47a,         // warm amber
    size:             _STREET_LIGHT_MIN_PX,    // recomputed each frame
    sizeAttenuation:  false,            // we drive size manually in pixels
    transparent:      true,
    opacity:          0,                // hidden until updateDayNight raises it
    depthWrite:       false,
    blending:         THREE.AdditiveBlending,
    toneMapped:       false,
  });

  _streetLights = new THREE.Points(geo, mat);
  _streetLights.renderOrder = 50;
  _streetLights.visible = false;
  scene.add(_streetLights);
}

/**
 * Per-frame screen-space sizing for the street lights. Mirrors the POI
 * pin trick: project NDC (-1, 0, 0) and (+1, 0, 0) back into world
 * space, measure the visible world width at screen centre, then derive
 * a pixel size that's INVERSELY proportional to that width — so the
 * lights grow when zoomed in and shrink when zoomed out.
 *
 * Without this, MapLibre's custom-layer projection produces the
 * opposite curve from PointsMaterial's built-in sizeAttenuation:
 * lights end up huge in the city overview and invisible at street
 * level. The POI pins suffer the same inversion (see _updatePOIs).
 */
function _updateStreetLights() {
  if (!_streetLights || !_streetLights.visible || !_mvpMatrix) return;

  // Reuse the POI scratch vectors to avoid allocations.
  const invMVP = _mvpMatrix.clone().invert();
  _poiTmpL.set(-1, 0, 0).applyMatrix4(invMVP);
  _poiTmpR.set( 1, 0, 0).applyMatrix4(invMVP);
  const visWidth = _poiTmpL.distanceTo(_poiTmpR);
  _lastVisWidth = visWidth;  // share with LOD systems

  // Inverse curve clamped to [MIN, MAX]. Add a floor on visWidth so the
  // division never blows up at extreme close-zoom.
  const px = Math.max(
    _STREET_LIGHT_MIN_PX,
    Math.min(_STREET_LIGHT_MAX_PX, _STREET_LIGHT_SIZE_K / Math.max(visWidth, 80)),
  );
  _streetLights.material.size = px;
}

// ── Clouds (2D canvas sprites — see _updateWeather for the render call) ────

/**
 * Build the shared cloud silhouette canvas once and cache it. Each
 * cloud sprite is the same texture, blitted at different sizes and
 * opacities.  Drawn from several overlapping radial-gradient blobs to
 * give a soft puffy outline.
 */
function _initCloudSpriteCanvas() {
  if (_cloudSpriteCanvas) return _cloudSpriteCanvas;
  const W = 320, H = 160;
  const c = document.createElement('canvas');
  c.width = W; c.height = H;
  const ctx = c.getContext('2d');
  const blob = (cx, cy, r, alpha) => {
    const g = ctx.createRadialGradient(cx, cy, 0, cx, cy, r);
    g.addColorStop(0.00, `rgba(255,255,255,${alpha})`);
    g.addColorStop(0.45, `rgba(255,255,255,${alpha * 0.85})`);
    g.addColorStop(0.75, `rgba(255,255,255,${alpha * 0.40})`);
    g.addColorStop(1.00, `rgba(255,255,255,0)`);
    ctx.fillStyle = g;
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, Math.PI * 2);
    ctx.fill();
  };
  blob(W * 0.30, H * 0.55, 50, 0.95);
  blob(W * 0.50, H * 0.45, 64, 1.00);
  blob(W * 0.70, H * 0.55, 52, 0.95);
  blob(W * 0.42, H * 0.40, 42, 0.85);
  blob(W * 0.60, H * 0.62, 44, 0.85);
  _cloudSpriteCanvas = c;
  return c;
}

/** Spawn one cloud sprite. randomX places it across the screen
 *  initially; otherwise it spawns just off-screen on the left so it
 *  drifts in. */
function _newCloud(randomX) {
  const W = _wxCanvas?.width  ?? window.innerWidth;
  const H = _wxCanvas?.height ?? window.innerHeight;
  const sw = 240 + Math.random() * 200;   // 240..440 px wide (slightly smaller)
  const sh = sw * 0.5;
  return {
    x:      randomX ? Math.random() * W : -sw,
    y:      40 + Math.random() * (H * 0.30),    // upper third of screen
    w:      sw,
    h:      sh,
    vx:     6 + Math.random() * 10,             // 6..16 px/sec (slower drift)
    // Per-sprite peak alpha — much lower than the previous "fluffy white"
    // values so the clouds read as faint wisps rather than dense blobs.
    alpha:  0.25 + Math.random() * 0.20,        // 0.25..0.45
  };
}

// ── Day/night shadows ─────────────────────────────────────────────────────────

/**
 * Enable or disable the day/night lighting mode. When disabled, restores
 * the flat default lighting used elsewhere in the app.
 */
/**
 * Fade day/night mode on (1) or off (0). Actual lighting changes happen
 * inside updateDayNight() on the next frame, which tweens _dayNightBlend
 * toward this target. Fading everything (sun, ambient, shadow opacity,
 * overlay) in sync eliminates the "reset flash" of snapping transitions.
 */
export function setDayNight(enabled) {
  _dayNightTarget = enabled ? 1 : 0;
}

export function isDayNightOn() {
  return _dayNightTarget > 0 || _dayNightBlend > 0;
}

// Flat-daylight reference values (what the scene looks like with day/night
// mode fully OFF). Night values are computed per frame from flowHour.
const _FLAT_SUN_POS_X   = 500;
const _FLAT_SUN_POS_Y   = 800;
const _FLAT_SUN_POS_Z   = 300;
const _FLAT_SUN_INT     = 1.1;
const _FLAT_AMBIENT_INT = 0.65;

/**
 * Tween the blend factor and update sun / ambient / shadow opacity / overlay
 * every frame. Safe to call unconditionally; when blend is 0 and target is 0
 * this is nearly free.
 *
 * Sun path:
 *   06:00 → sunrise (east, low, warm)
 *   12:00 → noon    (high, white)
 *   18:00 → sunset  (west, low, warm)
 *   00:00 → midnight (below horizon, only ambient)
 */
export function updateDayNight(flowHour, dt = 0.016) {
  if (!_sunLight || !_ambientLight) return;

  // Tween blend toward target
  if (_dayNightBlend !== _dayNightTarget) {
    const step = _DAY_NIGHT_FADE * dt;
    if (_dayNightTarget > _dayNightBlend) {
      _dayNightBlend = Math.min(_dayNightTarget, _dayNightBlend + step);
    } else {
      _dayNightBlend = Math.max(_dayNightTarget, _dayNightBlend - step);
    }
  }

  // Fully off shortcut — restore flat defaults exactly once when we reach 0
  if (_dayNightBlend <= 0.0001 && _dayNightTarget <= 0) {
    _sunLight.position.set(_FLAT_SUN_POS_X, _FLAT_SUN_POS_Y, _FLAT_SUN_POS_Z);
    _sunLight.target.position.set(0, 0, 0);
    _sunLight.target.updateMatrixWorld();
    _sunLight.intensity     = _FLAT_SUN_INT;
    _ambientLight.intensity = _FLAT_AMBIENT_INT;
    _sunLight.color.setHex(0xfff8f0);
    if (_shadowGround) _shadowGround.material.opacity = 0.0;
    const overlay0 = document.getElementById('dayNightOverlay');
    if (overlay0) overlay0.style.background = 'rgba(0,0,0,0)';
    return;
  }

  // ── Compute "pure night-cycle" values from the clock hour ─────────────────
  const t       = ((flowHour - 6) / 24) * Math.PI * 2;  // 0 rad @ 06:00
  const radius  = 1500;
  const sunX    = Math.cos(t) * radius;                  // east → west
  const sunY    = Math.sin(t) * radius * 0.9;            // altitude (- at night)
  const sunZ    = -300;                                  // slight southern tilt

  const altitude = Math.max(-1, Math.min(1, sunY / (radius * 0.9)));

  // ── Smooth regime weights (no branches, continuous across horizon) ────────
  // nightW : 0 when sun is comfortably above the horizon, 1 when well below.
  //          Smoothstep across a ±0.15 band around altitude=0 so the tint
  //          slides continuously from amber → indigo rather than snapping.
  // warmW  : "golden-hour" weight — rises near horizon (above OR below),
  //          peaks at altitude≈0, falls off to either side. This gives the
  //          sunrise/sunset orange a soft bell shape instead of a hard edge.
  // dayW   : daylight weight — 0 below horizon, 1 when high in the sky.
  const nightW = _smoothstep(0.15, -0.15, altitude);
  const warmW  = Math.exp(-((altitude - 0.02) ** 2) / 0.045);   // bell, σ≈0.15
  const dayW   = _smoothstep(-0.05, 0.35, altitude);

  // Intensities envelope, smooth through the horizon crossing
  const nightSunInt     = 0.08 + 1.55 * dayW;
  const nightAmbientInt = 0.10 + 0.62 * dayW;

  // ── Snap sun to the clock-driven position (no lerp) ──────────────────────
  // Linearly interpolating the position between flat daylight and the
  // dynamic position caused the sun to visibly sweep across the sky during
  // the fade — e.g. at sunset the lerp midpoint passed through the map
  // centre, which looked like "the light comes back to the centre". By
  // snapping position and only fading INTENSITIES + shadow opacity, the sun
  // stays put while the rest of the day/night effect fades in or out.
  _sunLight.position.set(sunX, sunY, sunZ);
  _sunLight.target.position.set(0, 0, 0);
  _sunLight.target.updateMatrixWorld();

  // ── Blend between flat daylight (b=0) and full cycle (b=1) ────────────────
  const b  = _dayNightBlend;
  const ib = 1 - b;

  _sunLight.intensity     = nightSunInt     * b + _FLAT_SUN_INT     * ib;
  _ambientLight.intensity = nightAmbientInt * b + _FLAT_AMBIENT_INT * ib;

  // Weather attenuation: rain/snow days weaken shadows and swap the warm sun
  // tint for a flat cool-grey overcast. `wxMix` is 0 on clear days, ramps up
  // to 1 on precipitation days (shared smoothing with the snow/rain canvas).
  const wxMix = _wxIntensity;   // already tweened by _updateWeather()

  // Sunlight color — stronger warm tint that varies through the day.
  // • High sun  → mild golden (G≈0.90, B≈0.72)
  // • Horizon   → rich orange (G≈0.55, B≈0.25) for sunrise/sunset
  // • Below     → drifts cool-blue for moonlight feel
  // On overcast days, the warm tint is washed out toward neutral daylight
  // so the direct light matches the grey overlay.
  const horizonOrange = warmW * (1 - nightW);
  const sunR0 = 1.0;
  const sunG0 = (1.0 - 0.10 * dayW - 0.45 * horizonOrange) * (1 - nightW * 0.55) + 0.60 * nightW;
  const sunB0 = (1.0 - 0.28 * dayW - 0.75 * horizonOrange) * (1 - nightW * 0.30) + 0.85 * nightW;
  // Neutralise toward near-white on overcast days
  const n = wxMix * 0.85;
  const sunR = sunR0 * (1 - n) + 0.96 * n;
  const sunG = sunG0 * (1 - n) + 0.96 * n;
  const sunB = sunB0 * (1 - n) + 0.98 * n;
  // Blend with the flat-daylight color (0xfff8f0 → 1, 0.973, 0.941)
  _sunLight.color.setRGB(
    sunR * b + 1.000 * ib,
    sunG * b + 0.973 * ib,
    sunB * b + 0.941 * ib,
  );

  // Ground-shadow opacity — original clock-driven formula restored. The
  // `altitude*4 + 0.1` envelope keeps shadows faintly present right at the
  // horizon and ramps to full by altitude ≈ 0.22. On overcast days (rain or
  // snow) the whole thing is attenuated since there's no direct sun.
  if (_shadowGround) {
    const horizonFade = Math.max(0, Math.min(1, altitude * 4 + 0.1));
    const weatherAtt  = 1 - 0.75 * wxMix;    // 25% when fully overcast
    _shadowGround.material.opacity = 0.38 * b * horizonFade * weatherAtt;
  }

  // Street lights — visible only when it's actually dark AND day/night
  // mode is active. Opacity scales with the night weight so they fade in
  // around sunset and out around sunrise.
  if (_streetLights) {
    const lightStrength = nightW * b;
    if (lightStrength > 0.02) {
      _streetLights.visible = true;
      _streetLights.material.opacity = 0.95 * lightStrength;
    } else {
      _streetLights.visible = false;
    }
  }

  // ── Full-viewport colour filter ───────────────────────────────────────────
  // Two palettes blended by `wxMix`:
  //   • SUNNY: three-way smooth blend between golden daylight, horizon
  //            orange, and indigo night — colours shift as the sun moves.
  //   • OVERCAST: a single flat cool grey regardless of sun altitude, so
  //               rain/snow days feel uniformly dim all day long.
  const overlay = document.getElementById('dayNightOverlay');
  if (overlay) {
    // ── Sunny palette ──────────────────────────────────────────────────────
    const DAY   = [255, 196, 110];   // richer golden daylight
    const HORIZ = [255, 112,  32];   // rich sunrise/sunset orange
    const NIGHT = [ 12,  16,  42];   // deep indigo darkness

    // Smooth, normalised weights so the crossover is C¹-continuous.
    let wDay   = dayW   * (1 - nightW) * (1 - warmW * 0.7);
    let wHoriz = warmW  * (1 - nightW);
    let wNight = nightW;
    const sum  = wDay + wHoriz + wNight + 1e-6;
    wDay /= sum; wHoriz /= sum; wNight /= sum;

    const sR = DAY[0] * wDay + HORIZ[0] * wHoriz + NIGHT[0] * wNight;
    const sG = DAY[1] * wDay + HORIZ[1] * wHoriz + NIGHT[1] * wNight;
    const sB = DAY[2] * wDay + HORIZ[2] * wHoriz + NIGHT[2] * wNight;
    const sunnyOpacity =
      0.26 * wDay +      // stronger golden wash so sunny days feel warm
      0.45 * wHoriz +
      0.60 * wNight;

    // ── Overcast palette ───────────────────────────────────────────────────
    // Constant cool blue-grey the whole day, slightly darker than the sunny
    // noon wash so an overcast day is noticeably "duller".
    const OVERCAST     = [120, 130, 150];
    const overcastOp   = 0.40;

    // ── Mix by weather ─────────────────────────────────────────────────────
    const r  = Math.round(sR * (1 - wxMix) + OVERCAST[0] * wxMix);
    const g  = Math.round(sG * (1 - wxMix) + OVERCAST[1] * wxMix);
    const bc = Math.round(sB * (1 - wxMix) + OVERCAST[2] * wxMix);
    const op = (sunnyOpacity * (1 - wxMix) + overcastOp * wxMix) * b;

    overlay.style.background = `rgba(${r},${g},${bc},${op.toFixed(3)})`;
  }

  // Drive the weather layer (no-op unless a snow/rain/cloudy day is active)
  _updateWeather(dt);
}

// ── Smoothstep helper ────────────────────────────────────────────────────────
// Standard Hermite smoothstep; supports edge0 > edge1 for descending bands.
function _smoothstep(edge0, edge1, x) {
  if (edge0 === edge1) return x < edge0 ? 0 : 1;
  const t = Math.max(0, Math.min(1, (x - edge0) / (edge1 - edge0)));
  return t * t * (3 - 2 * t);
}

// ── Weather effects (snow / rain) ────────────────────────────────────────────
// A single full-viewport canvas renders either snowflakes or rain streaks
// depending on the current weather. One weather is rolled per simulated day
// via notifyNewDay() with independent probabilities:
//   • snow : 10%   (1/10)
//   • rain :  5%   (1/20)
//   • clear: 85%
// Fade in/out is tweened so transitions to/from a weather day are smooth,
// and the whole layer is gated by the day/night blend so precipitation only
// appears when the day/night effect is active.

const WEATHER_CLEAR = 'clear';
const WEATHER_SNOW  = 'snow';
const WEATHER_RAIN  = 'rain';

let _wxCanvas    = null;
let _wxCtx       = null;
const _snowFlakes = [];
const _rainDrops  = [];
let _wxIntensity = 0;              // current visible intensity (0..1)
let _wxTarget    = 0;              // target set by notifyNewDay()
let _wxLastDay   = -1;             // last day we rolled for
let _wxCurrent   = WEATHER_CLEAR;  // currently-rendering weather
const _WX_FADE   = 0.35;           // fade speed (per second)
const _SNOW_N    = 240;
const _RAIN_N    = 360;

/*
 * Per-season weather probabilities by latitude band, calibrated to SMHI
 * climate normals for southern (lat ~56°N: Malmö/Lund) vs northern (lat
 * ~67°N: Kiruna/Luleå) Sweden. Each entry is [snow, rain, cloudy, clear]
 * — cloudy is *not* a precipitation mode but an overlay rolled separately
 * (only on otherwise-clear days), with conditional probability
 *   cloudy / (cloudy + clear)
 * so the four numbers sum to 1.0 over a long-run season average.
 *
 * For a city at latitude L we lerp between south and north using
 *   t = clamp((L - 56) / 11, 0, 1)
 * so Malmö stays close to the south column and Luleå close to the north,
 * with mid-Sweden cities (Stockholm 59°N → t≈0.27, Sundsvall 62°N → t≈0.55)
 * landing smoothly between them.
 */
const _SEASON_PROBS = {
  winter: { south: [0.20, 0.30, 0.40, 0.10], north: [0.65, 0.05, 0.20, 0.10] },
  spring: { south: [0.04, 0.30, 0.35, 0.31], north: [0.25, 0.15, 0.30, 0.30] },
  summer: { south: [0.00, 0.25, 0.25, 0.50], north: [0.00, 0.25, 0.25, 0.50] },
  autumn: { south: [0.04, 0.40, 0.35, 0.21], north: [0.30, 0.25, 0.25, 0.20] },
};

function _seasonProbs(season, lat) {
  const s = _SEASON_PROBS[season] || _SEASON_PROBS.summer;
  const t = Math.max(0, Math.min(1, (lat - 56) / 11));
  return s.south.map((v, i) => v * (1 - t) + s.north[i] * t);
}

function _initWeather() {
  if (_wxCanvas) return;
  // Reuse the existing #snowCanvas element — it's just a canvas.
  _wxCanvas = document.getElementById('snowCanvas');
  if (!_wxCanvas) return;
  _wxCtx = _wxCanvas.getContext('2d');
  _resizeWeather();
  window.addEventListener('resize', _resizeWeather);
  for (let i = 0; i < _SNOW_N; i++) _snowFlakes.push(_newFlake(true));
  for (let i = 0; i < _RAIN_N; i++) _rainDrops.push(_newDrop(true));
  for (let i = 0; i < _CLOUD_SPRITE_COUNT; i++) _clouds.push(_newCloud(true));
  _initCloudSpriteCanvas();
}

function _resizeWeather() {
  if (!_wxCanvas) return;
  _wxCanvas.width  = window.innerWidth;
  _wxCanvas.height = window.innerHeight;
}

function _newFlake(randomY) {
  const H = _wxCanvas?.height ?? window.innerHeight;
  const W = _wxCanvas?.width  ?? window.innerWidth;
  return {
    x:     Math.random() * W,
    y:     randomY ? Math.random() * H : -10,
    r:     0.8 + Math.random() * 2.4,
    vy:    25 + Math.random() * 55,
    vx:    -8 + Math.random() * 16,
    phase: Math.random() * Math.PI * 2,
  };
}

function _newDrop(randomY) {
  const H = _wxCanvas?.height ?? window.innerHeight;
  const W = _wxCanvas?.width  ?? window.innerWidth;
  return {
    x:   Math.random() * W,
    y:   randomY ? Math.random() * H : -20,
    vy:  480 + Math.random() * 260,   // fast vertical streaks
    len: 10 + Math.random() * 14,
    a:   0.35 + Math.random() * 0.35,
  };
}

/**
 * Roll weather for a new day. Returns the chosen weather string so the
 * caller (ui.js) can update HUD icons. Called by ui.js whenever the day
 * counter advances. Safe to call with the same day repeatedly.
 *
 * @param {number}  day      simulated day counter (used as a no-op guard)
 * @param {object} [climate] optional engine climate snapshot
 *                          {temperature, humidity, season} from /api/state
 * @param {number} [lat]     city latitude (degrees N) — northern Sweden
 *                          (Kiruna ~67°N) gets more snow than south (Malmö ~56°N)
 */
export function notifyNewDay(day, climate = null, lat = 60) {
  if (day === _wxLastDay) return _wxCurrent;
  _wxLastDay = day;

  // Season + latitude → realistic Swedish weather distribution. The four
  // probabilities sum to 1.0; cloudy is rolled conditionally on a clear
  // outcome (clouds are an overlay, never combined with precipitation).
  const season = (climate?.season || 'Summer').toLowerCase();
  const [snowProb, rainProb, cloudyProb, clearProb] = _seasonProbs(season, lat);

  const r = Math.random();
  let next;
  if (r < snowProb)               next = WEATHER_SNOW;
  else if (r < snowProb + rainProb) next = WEATHER_RAIN;
  else                              next = WEATHER_CLEAR;

  if (next !== _wxCurrent) {
    // Fade the old weather out before swapping mode. Easiest: force fade to
    // 0 first, then change mode when intensity lands there. But that delays
    // the new weather by ~3s. Acceptable trade-off for visual smoothness.
    _wxCurrent = next;
  }
  _wxTarget = (next === WEATHER_CLEAR) ? 0 : 1;
  // Cloudy is the conditional probability cloudy / (cloudy + clear) on a
  // clear-precipitation day. Snow/rain days stay cloud-free so the sprite
  // overlay never fights with precipitation visually.
  const cloudyConditional = (cloudyProb + clearProb) > 0
    ? cloudyProb / (cloudyProb + clearProb)
    : 0;
  _cloudsToday = (next === WEATHER_CLEAR) && (Math.random() < cloudyConditional);
  _cloudTarget = _cloudsToday ? 1 : 0;
  // Surface cloudy as its own weather string so the UI can swap the
  // day/night icon even though _wxCurrent stays at CLEAR internally
  // (cloudy doesn't trigger the precipitation canvas / overcast
  // attenuation).
  return _cloudsToday ? WEATHER_CLOUDY : next;
}

/** Returns the current rolled weather (useful for HUD). */
export function getCurrentWeather() {
  if (_cloudsToday && _wxCurrent === WEATHER_CLEAR) return WEATHER_CLOUDY;
  return _wxCurrent;
}

function _updateWeather(dt) {
  if (!_wxCanvas) _initWeather();
  if (!_wxCtx) return;

  // Tween precipitation intensity toward target
  if (_wxIntensity !== _wxTarget) {
    const step = _WX_FADE * dt;
    _wxIntensity = _wxTarget > _wxIntensity
      ? Math.min(_wxTarget, _wxIntensity + step)
      : Math.max(_wxTarget, _wxIntensity - step);
  }
  // Tween cloud intensity toward target (independent from precipitation)
  if (_cloudIntensity !== _cloudTarget) {
    const step = _CLOUD_FADE * dt;
    _cloudIntensity = _cloudTarget > _cloudIntensity
      ? Math.min(_cloudTarget, _cloudIntensity + step)
      : Math.max(_cloudTarget, _cloudIntensity - step);
  }

  // The canvas is shown if EITHER clouds or precipitation are visible.
  // Both layers are gated on the day/night blend so the weather overlay
  // only appears when day/night mode is active.
  const precipEff = _wxIntensity  * _dayNightBlend;
  const cloudEff  = _cloudIntensity * _dayNightBlend;
  const showCanvas =
    (precipEff > 0.002 && _wxCurrent !== WEATHER_CLEAR) ||
    cloudEff > 0.002;

  if (!showCanvas) {
    if (_wxCanvas.style.display !== 'none') _wxCanvas.style.display = 'none';
    return;
  }
  if (_wxCanvas.style.display !== 'block') _wxCanvas.style.display = 'block';

  const W = _wxCanvas.width, H = _wxCanvas.height;
  _wxCtx.clearRect(0, 0, W, H);

  // ── Clouds (drawn FIRST so precipitation streaks render on top) ───────────
  if (cloudEff > 0.002) {
    const sprite = _initCloudSpriteCanvas();
    for (const c of _clouds) {
      c.x += c.vx * dt;
      if (c.x > W + 20) c.x = -c.w - 20;
      _wxCtx.globalAlpha = Math.max(0, Math.min(1, c.alpha * cloudEff));
      _wxCtx.drawImage(sprite, c.x, c.y, c.w, c.h);
    }
    _wxCtx.globalAlpha = 1;
  }

  // ── Precipitation (snow / rain) ──────────────────────────────────────────
  if (precipEff > 0.002 && _wxCurrent === WEATHER_SNOW) {
    _wxCtx.fillStyle = `rgba(255,255,255,${(0.88 * precipEff).toFixed(3)})`;
    for (const f of _snowFlakes) {
      f.phase += dt * 1.8;
      f.x += (f.vx + Math.sin(f.phase) * 12) * dt;
      f.y += f.vy * dt;
      if (f.y > H + 8) { f.y = -10; f.x = Math.random() * W; }
      if (f.x < -10)   f.x = W + 10;
      if (f.x > W + 10) f.x = -10;
      _wxCtx.beginPath();
      _wxCtx.arc(f.x, f.y, f.r, 0, Math.PI * 2);
      _wxCtx.fill();
    }
  } else if (precipEff > 0.002 && _wxCurrent === WEATHER_RAIN) {
    _wxCtx.lineWidth   = 1.1;
    _wxCtx.lineCap     = 'round';
    for (const d of _rainDrops) {
      d.y += d.vy * dt;
      if (d.y > H + 20) { d.y = -20; d.x = Math.random() * W; }
      _wxCtx.strokeStyle =
        `rgba(170,195,230,${(d.a * precipEff).toFixed(3)})`;
      _wxCtx.beginPath();
      _wxCtx.moveTo(d.x, d.y);
      _wxCtx.lineTo(d.x - 2, d.y - d.len);   // slight lean for motion
      _wxCtx.stroke();
    }
  }
}

/** Reset weather — called from ui.js on Reset so day 0 is clear. */
export function resetWeather() {
  _wxTarget       = 0;
  _wxIntensity    = 0;
  _wxLastDay      = -1;
  _wxCurrent      = WEATHER_CLEAR;
  _cloudsToday    = false;
  _cloudTarget    = 0;
  _cloudIntensity = 0;
}

// Back-compat alias — ui.js used to call resetSnow()
export const resetSnow = resetWeather;

export function setAppearanceMode(enabled) {
  _appearanceMode = enabled;
  if (_buildingMesh) _buildingMesh.visible = !enabled;

  // Lazy-build the Swedish appearance layer the first time it's enabled
  if (enabled && !_appearanceGroup && _appearanceLayout) {
    _appearanceGroup = buildAppearanceCity(_appearanceLayout, scene);
  }
  if (_appearanceGroup) _appearanceGroup.visible = enabled;
}

export function animateTopView() {
  if (_mapInstance) _mapInstance.flyTo({ pitch: 0, bearing: 0, duration: 600 });
}

/**
 * Update per-building colours from SEIR cell state.
 * @param {Array} cells  — flat array indexed by building idx
 */
export function updateColors(cells) {
  _lastCells = cells;
  _maxN = 1;
  _maxI = 1;
  _maxD = 1;
  _maxE = 1;
  _maxS = 1;
  _maxR = 1;
  _maxIH = 1;
  _incomeLo = Infinity;
  _incomeHi = 1;
  for (const b of _buildings) {
    const c = cells[b.idx];
    if (c) {
      if (c.N > _maxN) _maxN = c.N;
      if (c.I > _maxI) _maxI = c.I;
      if ((c.D ?? 0) > _maxD) _maxD = c.D;
      if ((c.E ?? 0) > _maxE) _maxE = c.E;
      if ((c.S ?? 0) > _maxS) _maxS = c.S;
      if ((c.R ?? 0) > _maxR) _maxR = c.R;
      const _ih = c.infections_here ?? 0;
      if (_ih > _maxIH) _maxIH = _ih;
    }
  }
  // Cumulative trackers — never decrease across ticks. Used by the
  // 'cumulative' normalisation mode so a building that peaked early
  // doesn't visually shrink as later infections eclipse it.
  if (_maxN > _cumMaxN) _cumMaxN = _maxN;
  if (_maxI > _cumMaxI) _cumMaxI = _maxI;
  if (_maxE > _cumMaxE) _cumMaxE = _maxE;
  if (_maxS > _cumMaxS) _cumMaxS = _maxS;
  if (_maxR > _cumMaxR) _cumMaxR = _maxR;
  if (_maxD > _cumMaxD) _cumMaxD = _maxD;

  // Per-building monotonic trackers + city-wide totals — both computed
  // in a single pass so 'monotonic' and 'total' normalisation are O(n)
  // instead of O(n²) per render.
  _totN = _totI = _totE = _totS = _totR = _totD = 0;
  for (const b of _buildings) {
    const c = cells[b.idx];
    if (!c) continue;
    _totN += c.N || 0;
    _totI += c.I || 0;
    _totE += c.E || 0;
    _totS += c.S || 0;
    _totR += c.R || 0;
    _totD += c.D || 0;
    _bumpBuildingMax(_bmaxI, b.idx, c.I || 0);
    _bumpBuildingMax(_bmaxE, b.idx, c.E || 0);
    _bumpBuildingMax(_bmaxS, b.idx, c.S || 0);
    _bumpBuildingMax(_bmaxR, b.idx, c.R || 0);
    _bumpBuildingMax(_bmaxD, b.idx, c.D || 0);
  }
  _totN = Math.max(1, _totN); _totI = Math.max(1, _totI);
  _totE = Math.max(1, _totE); _totS = Math.max(1, _totS);
  _totR = Math.max(1, _totR); _totD = Math.max(1, _totD);
  // Income range — residential buildings only (zones 1-3).
  for (const b of _buildings) {
    if (typeof b.income === 'number' && b.zone >= 1 && b.zone <= 3) {
      if (b.income < _incomeLo) _incomeLo = b.income;
      if (b.income > _incomeHi) _incomeHi = b.income;
    }
  }
  if (!isFinite(_incomeLo)) _incomeLo = 0;

  // Apply dynamic heights only when in a non-zone mode (zone mode is static)
  if (_heightMode !== 'zone') _applyHeights(cells);

  // Landmarks (custom 3D assets) are visible in every mode, so their color
  // update has to run even when we're in appearance mode or the extruded
  // building mesh hasn't been built yet.
  _applyLandmarkColors(cells);

  if (_appearanceMode || !_buildingMesh) return;

  const colorAttr  = _buildingMesh.geometry.attributes.color;
  const hasSelection = _selectedIdx >= 0;

  _buildings.forEach((b, i) => {
    const range = _vertexRanges[i];
    if (!range) return;

    if (hasSelection && i !== _selectedIdx) {
      // Dim non-selected buildings
      _setRangeColor(colorAttr.array, range, 0.36, 0.38, 0.40);
      return;
    }

    // Patient-zero pick mode: any building is now a valid target, no
    // grey-out — the user can pick a residence, school, hospital,
    // anything. Backend handles capacity for non-residential.
    const isResidential = (b.zone >= 1 && b.zone <= 3);

    // Missing SEIR cell: treat as all-zero so gradient modes still paint
    // the building (bottom of the scale) instead of falling back to zone
    // colour, which would make it visually escape the active color scheme.
    const seir = cells[b.idx] ?? _EMPTY_SEIR;

    const N = seir.N;
    let r, g, bv;

    // "Cumulative reach" mode short-circuits every infection-related
    // colour mode to the same monotonic signal: the fraction of this
    // building's capacity that has ever been touched by the outbreak.
    // This produces a smooth, growth-only colour ramp instead of one
    // that pegs at max the day a building first peaks.
    const _seirMode = (_colorMode === 'infection' || _colorMode === 'exposed'
                    || _colorMode === 'susceptible' || _colorMode === 'recovered'
                    || _colorMode === 'deaths');
    if (_normMode === 'monotonic' && _seirMode) {
      const f = _cumulativeReach(seir, b);
      [r, g, bv] = sampleScale(_invertScale ? (1 - f) : f, _colorScale).map(v => v / 255);

    } else if (_colorMode === 'infection') {
      [r, g, bv] = sampleScale(_normValue('I', seir.I, N), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'exposed') {
      [r, g, bv] = sampleScale(_normValue('E', seir.E ?? 0, N), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'susceptible') {
      [r, g, bv] = sampleScale(_normValue('S', seir.S ?? 0, N), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'recovered') {
      [r, g, bv] = sampleScale(_normValue('R', seir.R ?? 0, N), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'prevalence') {
      // I / N — fraction of building's population currently infectious.
      const denom = N || 1;
      const frac  = N > 0 ? (seir.I / denom) : 0;
      [r, g, bv] = sampleScale(_invertScale ? (1 - frac) : frac, _colorScale).map(v => v / 255);

    } else if (_colorMode === 'population') {
      [r, g, bv] = sampleScale(_normValue('N', N, N), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'deaths') {
      [r, g, bv] = sampleScale(_normValue('D', seir.D ?? 0, N), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'infections_here') {
      // "Where infections happened" — paints each building by the
      // count of S→E transmission events whose `location` was this
      // building. Cells without the field default to 0 (compartmental
      // engine, or no transmissions yet). Live-normalised only in v1
      // — _normMode (cumulative / capacity / total) is ignored for
      // this mode because the count is already cumulative-by-nature.
      const _ihVal = (cells[b.idx]?.infections_here) ?? 0;
      [r, g, bv] = sampleScale(_cnorm(_ihVal, _maxIH), _colorScale).map(v => v / 255);

    } else if (_colorMode === 'income') {
      // Income only applies to residential buildings (zones 1-3).
      // Non-residential buildings are greyed out since income is a
      // household property, not a commercial/industrial one.
      if (!isResidential) {
        r = 0.30; g = 0.30; bv = 0.32;
        _setRangeColor(colorAttr.array, range, r, g, bv);
        return;
      }
      const inc = b.income;
      let t = 0;
      if (typeof inc === 'number' && _incomeHi > _incomeLo) {
        t = (inc - _incomeLo) / (_incomeHi - _incomeLo);
      }
      [r, g, bv] = sampleScale(_invertScale ? (1 - t) : t, _colorScale).map(v => v / 255);

    } else if (_colorMode === 'origin') {
      // Country of birth: paint by the building's predominant origin group
      // (index into the origin_groups palette supplied by /api/city).
      const groups = window._epiCityOriginGroups;
      const idx    = (b.predominant_origin ?? 0);
      const css    = (groups && groups[idx] && groups[idx].color) || '#94a3b8';
      _col.setStyle(css);
      r = _col.r; g = _col.g; bv = _col.b;

    } else {
      // Zone or no-colour mode — flat base colour, **no SEIR overlay**.
      // Previously these modes lerp'd in red / yellow / blue based on
      // infection fractions; that muddied the palette the user picked
      // and made zone mode visually drift with the simulation. Now
      // both modes stay pinned to their base colour at all times.
      const baseHex = _colorMode === 'none'
        ? _NO_COLOR_HEX
        : (ZONE_COLORS[b.zone] ?? 0x888888);
      _col.setHex(baseHex);
      r = _col.r; g = _col.g; bv = _col.b;
    }

    _setRangeColor(colorAttr.array, range, r, g, bv);
  });

  colorAttr.needsUpdate = true;
}

/**
 * Landmark colour mode — every custom-asset building reacts to the same
 * _colorMode setting as the extruded building mesh.
 *
 *   'zone' → keep each landmark's original material palette (the brick /
 *            stone / copper colours the custom builder painted it with)
 *   otherwise → replace all meshes on the landmark with a single tinted
 *            MeshLambertMaterial computed from the cell's SEIR state
 *
 * A per-landmark tint material is cached in `root.userData.__tintMat` so we
 * aren't creating a new material every frame.
 */
function _applyLandmarkColors(cells) {
  if (!_landmarkGroup || _landmarkByIdx.size === 0) return;

  const hasSelection = _selectedIdx >= 0;

  for (const [idx, root] of _landmarkByIdx) {
    const dimmed = hasSelection && idx !== _selectedIdx;
    const useOriginal = !dimmed && _colorMode === 'zone';

    if (useOriginal) {
      // Restore the cached per-mesh originals.
      root.traverse(obj => {
        if (obj.isMesh && obj.userData.__origMat) {
          obj.material = obj.userData.__origMat;
        }
      });
      continue;
    }

    // Compute the tint colour for this landmark.
    let r, g, bv;

    if (dimmed) {
      r = 0.36; g = 0.38; bv = 0.40;
    } else {
      const seir = cells?.[idx] ?? _EMPTY_SEIR;
      const N    = seir.N;

      if (_colorMode === 'infection') {
        [r, g, bv] = sampleScale(_cnorm(seir.I, _maxI), _colorScale).map(v => v / 255);
      } else if (_colorMode === 'exposed') {
        [r, g, bv] = sampleScale(_cnorm(seir.E ?? 0, _maxE), _colorScale).map(v => v / 255);
      } else if (_colorMode === 'susceptible') {
        [r, g, bv] = sampleScale(_cnorm(seir.S ?? 0, _maxS), _colorScale).map(v => v / 255);
      } else if (_colorMode === 'recovered') {
        [r, g, bv] = sampleScale(_cnorm(seir.R ?? 0, _maxR), _colorScale).map(v => v / 255);
      } else if (_colorMode === 'prevalence') {
        const frac = N > 0 ? (seir.I / N) : 0;
        [r, g, bv] = sampleScale(_invertScale ? (1 - frac) : frac, _colorScale).map(v => v / 255);
      } else if (_colorMode === 'population') {
        [r, g, bv] = sampleScale(_cnorm(N, _maxN), _colorScale).map(v => v / 255);
      } else if (_colorMode === 'deaths') {
        [r, g, bv] = sampleScale(_cnorm(seir.D ?? 0, _maxD), _colorScale).map(v => v / 255);
      } else if (_colorMode === 'income') {
        // Landmarks are non-residential — grey in income mode
        const bld = _buildings[idx];
        const isRes = bld && bld.zone >= 1 && bld.zone <= 3;
        if (!isRes) { r = 0.30; g = 0.30; bv = 0.32; }
        else {
          const inc = bld?.income;
          let t2 = 0;
          if (typeof inc === 'number' && _incomeHi > _incomeLo) t2 = (inc - _incomeLo) / (_incomeHi - _incomeLo);
          [r, g, bv] = sampleScale(_invertScale ? (1 - t2) : t2, _colorScale).map(v => v / 255);
        }
      } else {
        // 'none' — flat base + SEIR overlay, matching the extruded mesh
        _col.setHex(_NO_COLOR_HEX);
        if (N > 0) {
          const iFrac = seir.I / N;
          const eFrac = seir.E / N;
          const rFrac = seir.R / N;
          if (iFrac > 0.005)       _col.lerp(_red, Math.min(iFrac * 3.5, 0.85));
          else if (eFrac > 0.005)  _col.lerp(_yel, Math.min(eFrac * 2.5, 0.65));
          if (rFrac > 0.3)         _col.lerp(_blu, Math.min((rFrac - 0.3) * 0.4, 0.3));
        }
        r = _col.r; g = _col.g; bv = _col.b;
      }
    }

    // Reuse a cached tint material per landmark.
    let tintMat = root.userData.__tintMat;
    if (!tintMat) {
      tintMat = new THREE.MeshLambertMaterial({ color: 0xffffff });
      root.userData.__tintMat = tintMat;
    }
    tintMat.color.setRGB(r, g, bv);

    root.traverse(obj => {
      if (obj.isMesh && obj.userData.__origMat) {
        obj.material = tintMat;
      }
    });
  }
}

/**
 * Scale every building's vertex Y based on the current _heightMode.
 * - 'zone'       → original geometry height (scale = 1.0)
 * - 'infection'  → height grows with infectious count
 * - 'population' → height grows with current population (N)
 * - 'deaths'     → height grows with cumulative death count
 */
function _applyHeights(cells) {
  // Landmarks still need to react to the height mode even if the
  // extruded building mesh hasn't been built yet or we're early in the
  // load sequence, so the landmark scaling happens below even when the
  // vertex-writing path bails.
  if (!_buildingMesh || !_origYs) {
    _applyLandmarkHeights(cells);
    return;
  }

  // Determine normalisation reference per mode
  let maxRef = 1;
  const _refKey = ({
    infection:  'I',
    exposed:    'E',
    susceptible:'S',
    recovered:  'R',
    deaths:     'D',
  })[_heightMode];
  if (_refKey) {
    if (cells) for (const b of _buildings) {
      const v = cells[b.idx]?.[_refKey] ?? 0;
      if (v > maxRef) maxRef = v;
    }
  } else if (_heightMode === 'population') {
    for (const b of _buildings) {
      const v = cells?.[b.idx]?.N ?? b.pop;
      if (v > maxRef) maxRef = v;
    }
  } else if (_heightMode === 'prevalence') {
    maxRef = 1;        // already a 0-1 fraction
  } else if (_heightMode === 'infections_here') {
    // Live-normalise against the city's peak transmission count.
    if (cells) for (const b of _buildings) {
      const v = cells[b.idx]?.infections_here ?? 0;
      if (v > maxRef) maxRef = v;
    }
  }
  // Income uses its own min/max already computed in updateColors()
  // against b.income, so maxRef above stays at the default.

  const posAttr = _buildingMesh.geometry.attributes.position;
  const arr     = posAttr.array;

  _buildings.forEach((b, i) => {
    const range = _vertexRanges[i];
    if (!range) return;
    const baseH = _baseHeights[i] || 1;

    let newH;
    // Same normalisation router used by colour. "Cumulative reach"
    // (monotonic) short-circuits to the same growth-only ramp.
    const cellHere = cells?.[b.idx];
    const nHere    = cellHere?.N ?? b.pop ?? 0;
    const _seirH   = (_heightMode === 'infection' || _heightMode === 'exposed'
                  || _heightMode === 'susceptible' || _heightMode === 'recovered'
                  || _heightMode === 'deaths');
    if (_normMode === 'monotonic' && _seirH) {
      const f = _cumulativeReach(cellHere, b);
      newH = 3 + f * 90;
    } else if (_heightMode === 'infection') {
      newH = 3 + _normHeight('I', cellHere?.I ?? 0, nHere, maxRef) * 90;
    } else if (_heightMode === 'exposed') {
      newH = 3 + _normHeight('E', cellHere?.E ?? 0, nHere, maxRef) * 80;
    } else if (_heightMode === 'susceptible') {
      newH = 3 + _normHeight('S', cellHere?.S ?? 0, nHere, maxRef) * 70;
    } else if (_heightMode === 'recovered') {
      newH = 3 + _normHeight('R', cellHere?.R ?? 0, nHere, maxRef) * 70;
    } else if (_heightMode === 'prevalence') {
      const frac = nHere > 0 ? ((cellHere?.I ?? 0) / nHere) : 0;
      newH = 3 + _norm(frac, 1) * 90;
    } else if (_heightMode === 'population') {
      newH = 3 + _normHeight('N', nHere, nHere, maxRef) * 70;
    } else if (_heightMode === 'deaths') {
      newH = 3 + _normHeight('D', cellHere?.D ?? 0, nHere, maxRef) * 80;
    } else if (_heightMode === 'infections_here') {
      // "Where infections happened" — height matches the colour mode:
      // taller bars over buildings that have hosted more S→E events.
      // Direct log-scale (no normMode router) — see the colour-mode
      // counterpart in updateColors for the rationale.
      const _ihVal = cellHere?.infections_here ?? 0;
      newH = 3 + _norm(_ihVal, maxRef) * 90;
    } else if (_heightMode === 'income') {
      // Only residential buildings (zones 1-3) get income-scaled height.
      // Non-residential stay at a low base height.
      const isRes = (b.zone >= 1 && b.zone <= 3);
      if (!isRes) { newH = 3; }
      else {
        let t = 0;
        if (typeof b.income === 'number' && _incomeHi > _incomeLo) {
          t = (b.income - _incomeLo) / (_incomeHi - _incomeLo);
        }
        newH = 3 + t * 70;
      }
    } else {
      newH = baseH;
    }

    const scale = newH / baseH;

    // Landmarks (custom 3D assets) scale as a single group on Y instead of
    // rewriting vertices in the merged mesh — their range has count 0 so
    // the vertex loop below is a no-op for them anyway.
    const landmarkRoot = _landmarkByIdx.get(i);
    if (landmarkRoot) {
      landmarkRoot.scale.y = scale;
      return;
    }

    for (let v = range.start; v < range.start + range.count; v++) {
      arr[v * 3 + 1] = _origYs[v] * scale;
    }
  });

  posAttr.needsUpdate = true;
}

/**
 * Stand-alone landmark height update — used when `_applyHeights()` bails
 * early because the extruded building mesh isn't ready yet. Mirrors the
 * formula in `_applyHeights()` so landmarks behave identically.
 */
function _applyLandmarkHeights(cells) {
  if (!_landmarkGroup || _landmarkByIdx.size === 0) return;

  let maxRef = 1;
  if (_heightMode === 'infection') {
    if (cells) for (const b of _buildings) {
      const v = cells[b.idx]?.I ?? 0;
      if (v > maxRef) maxRef = v;
    }
  } else if (_heightMode === 'population') {
    for (const b of _buildings) {
      const v = cells?.[b.idx]?.N ?? b.pop;
      if (v > maxRef) maxRef = v;
    }
  } else if (_heightMode === 'deaths') {
    if (cells) for (const b of _buildings) {
      const v = cells[b.idx]?.D ?? 0;
      if (v > maxRef) maxRef = v;
    }
  }

  for (const [idx, root] of _landmarkByIdx) {
    const baseH = _baseHeights[idx] || root.userData.baseHeight || 1;
    let newH;
    if (_heightMode === 'infection') {
      newH = 3 + _norm(cells?.[idx]?.I ?? 0, maxRef) * 90;
    } else if (_heightMode === 'population') {
      newH = 3 + _norm(cells?.[idx]?.N ?? _buildings[idx]?.pop ?? 0, maxRef) * 70;
    } else if (_heightMode === 'deaths') {
      newH = 3 + _norm(cells?.[idx]?.D ?? 0, maxRef) * 80;
    } else if (_heightMode === 'income') {
      // Landmarks are non-residential — stay low in income mode
      const bld = _buildings[idx];
      const isRes = bld && bld.zone >= 1 && bld.zone <= 3;
      if (!isRes) { newH = 3; }
      else {
        const inc = bld.income;
        let t = 0;
        if (typeof inc === 'number' && _incomeHi > _incomeLo) {
          t = (inc - _incomeLo) / (_incomeHi - _incomeLo);
        }
        newH = 3 + t * 70;
      }
    } else {
      newH = baseH;
    }
    root.scale.y = newH / baseH;
  }
}

// ── Compare-two-days renderers ──────────────────────────────────────────────
// State driven by static/compare.js. Two visualization modes:
//   • 'diff'  — single render pass; live color buffer painted with a
//               diverging palette of metric(B) − metric(A) per building.
//   • 'split' — two render passes per frame. We pre-bake A and B color
//               BufferAttributes once (when the user picks days), then swap
//               the geometry's `color` attribute between scissor-clipped
//               viewports. No per-frame buffer rewrites.
let _cmpEnabled       = false;
let _cmpMode          = 'diff';
let _cmpColorAttrLive = null;   // live color BufferAttribute (saved on entry)
let _cmpColorAttrA    = null;
let _cmpColorAttrB    = null;
// Last cells/metric supplied to enableCompare(diff) so a selection
// change can re-bake the diverging palette WITH the dim applied to
// non-selected buildings — same yellow-outline + grey-out affordance
// the live single view has, kept identical in diff mode.
let _lastCmpCellsA    = null;
let _lastCmpCellsB    = null;
let _lastCmpMetric    = null;

function _cmpMetricFn(metric) {
  switch (metric) {
    case 'I':          return c => c ? (c.I || 0) : 0;
    case 'E':          return c => c ? (c.E || 0) : 0;
    case 'R':          return c => c ? (c.R || 0) : 0;
    case 'D':          return c => c ? (c.D || 0) : 0;
    case 'active':     return c => c ? ((c.I || 0) + (c.E || 0)) : 0;
    case 'cumulative': return c => c ? ((c.I || 0) + (c.E || 0) + (c.R || 0) + (c.D || 0)) : 0;
    case 'prevalence':
    default:           return c => (c && c.N > 0) ? ((c.I || 0) / c.N) : 0;
  }
}

// Save & restore the cumulative + income trackers + _lastCells around an
// updateColors() call so a historical snapshot doesn't pollute the live-sim
// state. Per-building monotonic max maps (`_bmaxI` etc.) are not restored —
// they only ever grow and history values are always ≤ the running max.
function _captureColorSnapshot(cells) {
  const sLast = _lastCells;
  const sN = _cumMaxN, sI = _cumMaxI, sE = _cumMaxE;
  const sS = _cumMaxS, sR = _cumMaxR, sD = _cumMaxD;
  const sLo = _incomeLo, sHi = _incomeHi;
  updateColors(cells);
  const arr = _buildingMesh.geometry.attributes.color.array;
  const snap = new Float32Array(arr);
  _lastCells = sLast;
  _cumMaxN = sN; _cumMaxI = sI; _cumMaxE = sE;
  _cumMaxS = sS; _cumMaxR = sR; _cumMaxD = sD;
  _incomeLo = sLo; _incomeHi = sHi;
  return snap;
}

function _paintDiffColors(cellsA, cellsB, metric) {
  if (!_buildingMesh) return;
  const colorAttr = _buildingMesh.geometry.attributes.color;
  const arr       = colorAttr.array;
  const fn        = _cmpMetricFn(metric);

  // Drive heights from Day B's state under whatever height-mode the user
  // has active in the legend. Without this, heights stay frozen at
  // whatever the last live-sim updateColors() left behind, which makes
  // the diff map look "flat" relative to the running scene.
  if (_heightMode !== 'zone') _applyHeights(cellsB);

  const deltas = new Float32Array(_buildings.length);
  let maxAbs = 0;
  for (let i = 0; i < _buildings.length; i++) {
    const b = _buildings[i];
    const d = fn(cellsB?.[b.idx]) - fn(cellsA?.[b.idx]);
    deltas[i] = d;
    const ad = d < 0 ? -d : d;
    if (ad > maxAbs) maxAbs = ad;
  }
  if (maxAbs <= 0) maxAbs = 1;

  // Diverging palette: grey at zero, red ramp for Δ>0, blue ramp for Δ<0.
  // Endpoints chosen so the palette also reads correctly under the existing
  // dimmed-non-selected building style (which uses ~0.36 grey).
  const GR = [0.55, 0.55, 0.58];
  const RD = [1.00, 0.32, 0.21];
  const BL = [0.30, 0.65, 1.00];
  // When a building is selected, every other building dims to grey so the
  // picked one pops — matches updateColors()'s single-view behaviour.
  const hasSel = _selectedIdx >= 0;
  for (let i = 0; i < _buildings.length; i++) {
    const range = _vertexRanges[i];
    if (!range) continue;
    let r, g, bv;
    if (hasSel && i !== _selectedIdx) {
      r = 0.36; g = 0.38; bv = 0.40;
    } else {
      const d = deltas[i];
      const t = Math.min(1, (d < 0 ? -d : d) / maxAbs);
      if (d > 0) {
        r  = GR[0] + (RD[0] - GR[0]) * t;
        g  = GR[1] + (RD[1] - GR[1]) * t;
        bv = GR[2] + (RD[2] - GR[2]) * t;
      } else if (d < 0) {
        r  = GR[0] + (BL[0] - GR[0]) * t;
        g  = GR[1] + (BL[1] - GR[1]) * t;
        bv = GR[2] + (BL[2] - GR[2]) * t;
      } else {
        r = GR[0]; g = GR[1]; bv = GR[2];
      }
    }
    _setRangeColor(arr, range, r, g, bv);
  }
  colorAttr.needsUpdate = true;
}

/**
 * Toggle compare-two-days mode. Caller pauses the live sim before invoking.
 * Re-call with the same shape to refresh after a day-handle move.
 *   opts.mode    : 'diff' | 'split'
 *   opts.cellsA  : array of per-building cells for Day A
 *   opts.cellsB  : array of per-building cells for Day B
 *   opts.metric  : 'prevalence' (default), 'I', 'E', 'R', 'D', 'active', 'cumulative'
 */
export function enableCompare(opts) {
  if (!_buildingMesh) return;
  const { mode = 'diff', cellsA, cellsB, metric = 'prevalence' } = opts || {};
  _cmpMode = mode;

  if (!_cmpColorAttrLive) {
    _cmpColorAttrLive = _buildingMesh.geometry.attributes.color;
  }

  if (mode === 'diff') {
    if (_buildingMesh.geometry.attributes.color !== _cmpColorAttrLive) {
      _buildingMesh.geometry.setAttribute('color', _cmpColorAttrLive);
    }
    // Cache so a later selection-change refresh can re-bake the same
    // diff palette (with dim applied to non-selected buildings).
    _lastCmpCellsA = cellsA;
    _lastCmpCellsB = cellsB;
    _lastCmpMetric = metric;
    _paintDiffColors(cellsA, cellsB, metric);
  } else if (mode === 'split') {
    // Bake A first (so its colors land in the live array), snapshot,
    // then bake B last so the geometry's heights settle on B (the "newer"
    // day on the right side — matches the user's mental model).
    const snapA = _captureColorSnapshot(cellsA);
    updateColors(cellsB);
    const snapB = new Float32Array(_buildingMesh.geometry.attributes.color.array);

    if (!_cmpColorAttrA) {
      _cmpColorAttrA = new THREE.BufferAttribute(new Float32Array(snapA), 3);
    } else {
      _cmpColorAttrA.array.set(snapA);
      _cmpColorAttrA.needsUpdate = true;
    }
    if (!_cmpColorAttrB) {
      _cmpColorAttrB = new THREE.BufferAttribute(new Float32Array(snapB), 3);
    } else {
      _cmpColorAttrB.array.set(snapB);
      _cmpColorAttrB.needsUpdate = true;
    }
    _buildingMesh.geometry.setAttribute('color', _cmpColorAttrA);
  }

  _cmpEnabled = true;
}

/** Exit compare mode and restore the live color buffer. */
export function disableCompare(currentCells) {
  _cmpEnabled = false;
  _lastCmpCellsA = null;
  _lastCmpCellsB = null;
  _lastCmpMetric = null;
  if (_buildingMesh && _cmpColorAttrLive
      && _buildingMesh.geometry.attributes.color !== _cmpColorAttrLive) {
    _buildingMesh.geometry.setAttribute('color', _cmpColorAttrLive);
  }
  if (currentCells) updateColors(currentCells);
}

export function isCompareEnabled() { return _cmpEnabled; }
export function getCompareMode() { return _cmpMode; }

/**
 * Called by MapLibre CustomLayerInterface.render(gl, matrix).
 */
export function renderScene(matrix, centerMC, scale, useSecondary = false) {
  // The secondary renderer is wired to map B's GL context for the
  // side-by-side compare view; render the SAME scene through it so 3D
  // trees, animated water, and landmarks all appear on both halves.
  const r = useSecondary ? _rendererB : renderer;
  if (!r || !scene) return;

  const worldToMerc = new THREE.Matrix4()
    .makeTranslation(centerMC.x, centerMC.y, centerMC.z)
    .multiply(new THREE.Matrix4().makeScale(scale, -scale, scale))
    .multiply(new THREE.Matrix4().makeRotationX(Math.PI / 2));

  const mvp = new THREE.Matrix4().fromArray(matrix).multiply(worldToMerc);
  // _mvpMatrix is read by single-view picking; keep the primary's value
  // so raycasts against map A's canvas still land correctly.
  if (!useSecondary) _mvpMatrix = mvp;

  camera.projectionMatrix        = mvp;
  camera.projectionMatrixInverse = mvp.clone().invert();

  r.resetState();

  // Legacy three.js scissor-split path — kept for back-compat but
  // unused now that compare.js routes 'split' through compare-view-b.js.
  // Only the primary renderer ever drives this path.
  if (!useSecondary
      && _cmpEnabled && _cmpMode === 'split'
      && _buildingMesh && _cmpColorAttrA && _cmpColorAttrB) {
    const gl = r.getContext();
    const w = gl.drawingBufferWidth;
    const h = gl.drawingBufferHeight;
    const half = Math.floor(w / 2);
    const geo  = _buildingMesh.geometry;

    r.setScissorTest(true);

    geo.setAttribute('color', _cmpColorAttrA);
    r.setScissor(0, 0, half, h);
    r.render(scene, camera);

    geo.setAttribute('color', _cmpColorAttrB);
    r.setScissor(half, 0, w - half, h);
    r.render(scene, camera);

    r.setScissorTest(false);
  } else {
    r.render(scene, camera);
  }
}

/**
 * Update non-render scene state: indicator pulse. Called from ui.js rAF loop.
 */
export function updateScene() {
  // Water shoreline dash animation — slide the dash pattern along the
  // polygon edges so each lake reads as "flowing" around its border.
  // Speed is in world metres per second; negative sign so the pattern
  // advances in the edge direction instead of retreating.
  if (_waterLineMat) {
    const t = (performance.now() - _waterLineClockT0) / 1000;
    _waterLineMat.dashOffset = -t * 6;   // ~6 m/s drift
  }

  // Selection indicator pulse — breathes the outline opacity so it's
  // visible without distorting the building footprint. Keeps the line
  // width steady to avoid shimmer.
  if (_selIndicator?.visible) {
    const phase = 0.5 + 0.5 * Math.sin(Date.now() / 260);   // 0..1
    const line  = _selIndicator.userData.line;
    if (line) line.material.opacity = 0.55 + 0.45 * phase;   // 0.55..1.00
  }

  // POI pin billboard / bob / zoom-scale
  _updatePOIs();

  // Street light per-frame screen-space sizing (inverse-zoom curve)
  _updateStreetLights();
}

// ── Private helpers ───────────────────────────────────────────────────────────

/**
 * Create an extruded polygon geometry from OSM building data.
 * The shape is in the Three.js XZ plane (world ground level), Y = altitude.
 */
// ── Water line + raycast proxy ──────────────────────────────────────────────
//
// Builds two siblings for every WATER cell in the layout:
//
//   _waterLineMesh   — one LineSegments2 with every polygon's edges
//                      merged into a single positions array. Thick
//                      blue LineMaterial with `dashed: true` so the
//                      dashes animate by incrementing `dashOffset`
//                      each frame in updateScene().
//
//   _waterRaycastMesh — one merged ShapeGeometry mesh made from the
//                      same water polygons, lifted just above the
//                      ground plane. Its material writes neither
//                      colour nor depth (invisible) but the mesh
//                      still raycasts, so hover callbacks can
//                      resolve the cell under the mouse and show the
//                      tooltip anywhere on a lake — not just on the
//                      shoreline where the dashed line draws.
//
// Both meshes share a parallel lookup table (_waterCells,
// _waterRanges) so a raycast vertex index maps back to the owning
// cell via _vtxToRangeIdx().
function _buildWaterLineLayer(layout) {
  // Collect every water polygon's segment endpoints (flat [x,y,z,x,y,z,...]
  // with two endpoints per segment) and its ShapeGeometry for the
  // raycast proxy. Store a parallel ranges array keyed to the
  // raycast mesh's vertex indices.
  const segArrays = [];
  let totalSegFloats = 0;
  const shapes = [];
  _waterCells  = [];
  _waterRanges = [];
  let rcOff = 0;

  // Lift both meshes well above the MapLibre base plane (y=0) so the
  // translucent blue fill doesn't z-fight with the ground. Line
  // stays slightly above the fill so the dashed outline draws on top.
  const Y_LINE = 0.65;
  const Y_HIT  = 0.45;

  for (const b of layout.buildings) {
    if (b.zone !== 19) continue;
    const poly = b.polygon;
    if (!poly || poly.length < 3) continue;

    // Segment array: one segment per polygon edge + closing edge.
    const n = poly.length;
    const seg = new Float32Array(n * 6);
    for (let i = 0; i < n; i++) {
      const [ax, az] = poly[i];
      const [bx, bz] = poly[(i + 1) % n];
      const o = i * 6;
      seg[o]     = ax; seg[o + 1] = Y_LINE; seg[o + 2] = az;
      seg[o + 3] = bx; seg[o + 4] = Y_LINE; seg[o + 5] = bz;
    }
    segArrays.push(seg);
    totalSegFloats += seg.length;

    // Shape for the raycast proxy.
    const shape = new THREE.Shape();
    poly.forEach(([px, pz], i) => {
      if (i === 0) shape.moveTo(px, -pz);
      else         shape.lineTo(px, -pz);
    });
    shape.closePath();
    const geo = new THREE.ShapeGeometry(shape);
    geo.applyMatrix4(new THREE.Matrix4().makeRotationX(-Math.PI / 2));
    geo.translate(0, Y_HIT, 0);
    const vtxCount = geo.attributes.position.count;

    _waterCells.push(b);
    _waterRanges.push({ start: rcOff, count: vtxCount });
    rcOff += vtxCount;
    shapes.push(geo);
  }

  if (segArrays.length === 0) return;

  // ── Dashed line mesh ──────────────────────────────────────────────────
  const flatSegs = new Float32Array(totalSegFloats);
  {
    let off = 0;
    for (const s of segArrays) { flatSegs.set(s, off); off += s.length; }
  }
  const lineGeo = new LineSegmentsGeometry();
  lineGeo.setPositions(flatSegs);

  _waterLineMat = new LineMaterial({
    color:      0x3b82f6,      // bright water-blue
    linewidth:  3,             // world pixels
    transparent: true,
    opacity:    0.92,
    dashed:     true,
    dashSize:   12,            // metres
    gapSize:    6,             // metres
    dashScale:  1.0,
    dashOffset: 0.0,
    depthTest:  true,
  });
  const rs = new THREE.Vector2();
  renderer.getSize(rs);
  _waterLineMat.resolution.set(rs.x, rs.y);

  _waterLineMesh = new LineSegments2(lineGeo, _waterLineMat);
  _waterLineMesh.computeLineDistances();
  _waterLineMesh.renderOrder = 900;          // under the selection outline
  _waterLineMesh.raycast     = () => {};     // never picked
  _waterLineMesh.visible     = _natureDetailOn;
  scene.add(_waterLineMesh);

  // ── Translucent blue fill + raycast proxy (same mesh) ───────────────
  // The same merged ShapeGeometry does double duty: it's the hover
  // raycast target AND a light-blue tinted fill inside the dashed
  // shoreline. depthWrite stays off so the fill never z-fights
  // buildings near the lake edge, and transparent is true so the
  // underlying terrain (if any) bleeds through.
  const mergedRc = mergeGeometries(shapes, false);
  const waterFillMat = new THREE.MeshBasicMaterial({
    color:       0x3b82f6,
    transparent: true,
    opacity:     0.18,
    depthWrite:  false,
    side:        THREE.DoubleSide,
  });
  _waterRaycastMesh = new THREE.Mesh(mergedRc, waterFillMat);
  _waterRaycastMesh.renderOrder = 850;           // under the dashed line (900)
  _waterRaycastMesh.visible     = _natureDetailOn;
  scene.add(_waterRaycastMesh);
}

// Binary-search a [{start, count}] range table; returns the entry
// index whose range contains vtxIdx, or -1.
function _vtxToRangeIdx(ranges, vtxIdx) {
  let lo = 0, hi = ranges.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const { start, count } = ranges[mid];
    if (vtxIdx < start)              hi = mid - 1;
    else if (vtxIdx >= start + count) lo = mid + 1;
    else return mid;
  }
  return -1;
}

function _makeExtrudeGeo(b) {
  const polygon = b.polygon;
  if (!polygon || polygon.length < 3) return null;

  // Flat-ground polygons render as thin slabs so taller buildings
  // (churches, chapels) can sit on top of them without z-fighting.
  //   WATER (19)     → 0.3 m slab
  //   FORESTRY (20)  → 0.2 m ground pad
  //   CEMETERY (23)  → 0.5 m low terrain, church polygons rise above
  let height;
  if (b.zone === 19)      height = 0.3;
  else if (b.zone === 20) height = 0.2;
  else if (b.zone === 23) height = 0.5;
  else                    height = b.levels > 1 ? b.levels * 3.5 : (b.area_m2 > 200 ? 7 : 4);

  // Build Three.js Shape in XY plane using (world_x, -world_z) so that
  // after RotateX(-π/2) we get (world_x, 0, world_z) — correct XZ placement.
  const shape = new THREE.Shape();
  polygon.forEach(([px, pz], i) => {
    if (i === 0) shape.moveTo(px, -pz);
    else         shape.lineTo(px, -pz);
  });
  shape.closePath();

  // Interior holes — currently set by osm._clip_cemetery_polygons
  // when a cemetery fully contains another building (e.g. a church
  // surrounded by its graveyard). THREE.Shape.holes cuts these rings
  // out of the extruded mesh so the underlying building becomes
  // visible + clickable instead of being buried under the cemetery
  // slab.
  if (Array.isArray(b.polygon_holes) && b.polygon_holes.length > 0) {
    for (const hole of b.polygon_holes) {
      if (!Array.isArray(hole) || hole.length < 3) continue;
      const path = new THREE.Path();
      hole.forEach(([px, pz], i) => {
        if (i === 0) path.moveTo(px, -pz);
        else         path.lineTo(px, -pz);
      });
      path.closePath();
      shape.holes.push(path);
    }
  }

  const geo = new THREE.ExtrudeGeometry(shape, {
    depth:         height,
    bevelEnabled:  false,
  });

  // Rotate so extrusion faces up (Y+) instead of toward camera (Z+)
  // RotateX(-π/2): (x, y, z) → (x, z, -y)
  // Shape point (px, -pz, 0) → (px, 0, pz) ✓  Top (px, -pz, h) → (px, h, pz) ✓
  geo.applyMatrix4(new THREE.Matrix4().makeRotationX(-Math.PI / 2));

  return { geo, height };
}

function _setRangeColor(arr, range, r, g, b) {
  for (let v = range.start; v < range.start + range.count; v++) {
    arr[v * 3]     = r;
    arr[v * 3 + 1] = g;
    arr[v * 3 + 2] = b;
  }
}

function _resetBaseColor(arr, range, b) {
  const hex = _colorMode === 'none'
    ? _NO_COLOR_HEX
    : (ZONE_COLORS[b.zone] ?? 0x888888);
  _setRangeColor(arr, range,
    ((hex >> 16) & 0xff) / 255,
    ((hex >> 8)  & 0xff) / 255,
    ( hex        & 0xff) / 255,
  );
}

function _makeLabel(text) {
  const W = 512, H = 128;
  const canvas = document.createElement('canvas');
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext('2d');

  ctx.fillStyle = 'rgba(15,23,42,0.82)';
  const r = 16;
  ctx.beginPath();
  ctx.moveTo(r, 4); ctx.lineTo(W - r, 4);
  ctx.quadraticCurveTo(W - 4, 4, W - 4, r);
  ctx.lineTo(W - 4, H - r);
  ctx.quadraticCurveTo(W - 4, H - 4, W - r, H - 4);
  ctx.lineTo(r, H - 4);
  ctx.quadraticCurveTo(4, H - 4, 4, H - r);
  ctx.lineTo(4, r);
  ctx.quadraticCurveTo(4, 4, r, 4);
  ctx.closePath();
  ctx.fill();

  ctx.strokeStyle = 'rgba(148,163,184,0.35)';
  ctx.lineWidth   = 2;
  ctx.stroke();

  ctx.fillStyle    = '#e2e8f0';
  ctx.font         = 'bold 52px "Segoe UI", system-ui, sans-serif';
  ctx.textAlign    = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(text, W / 2, H / 2);

  const tex = new THREE.CanvasTexture(canvas);
  const mat = new THREE.SpriteMaterial({ map: tex, depthTest: false, transparent: true });
  const sprite = new THREE.Sprite(mat);
  sprite.scale.set(40, 10, 1);
  sprite.renderOrder = 1;
  return sprite;
}

// ── Raycasting ────────────────────────────────────────────────────────────────

function _getNDC(e) {
  const rect = e.target.getBoundingClientRect();
  return {
    x:  ((e.clientX - rect.left) / rect.width)  * 2 - 1,
    y: -((e.clientY - rect.top)  / rect.height) * 2 + 1,
  };
}

function _rayFromNDC(ndc) {
  if (!_mvpMatrix) return null;
  const inv  = _mvpMatrix.clone().invert();
  const near = new THREE.Vector3(ndc.x, ndc.y, -1).applyMatrix4(inv);
  const far  = new THREE.Vector3(ndc.x, ndc.y,  1).applyMatrix4(inv);
  return new THREE.Ray(near, new THREE.Vector3().subVectors(far, near).normalize());
}

/** Binary search: find which building owns vertex at index vtxIdx. */
function _buildingAtVertex(vtxIdx) {
  let lo = 0, hi = _vertexRanges.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const { start, count } = _vertexRanges[mid];
    if (vtxIdx < start)            hi = mid - 1;
    else if (vtxIdx >= start + count) lo = mid + 1;
    else return _buildings[mid];
  }
  return null;
}

function _hitBuilding(ndc) {
  const ray = _rayFromNDC(ndc);
  if (!ray) return null;

  const rc = new THREE.Raycaster();
  rc.ray.copy(ray);
  rc.near = 0.001;
  rc.far  = 1e9;

  // Helper: walk a raycast hit's parent chain to find the buildingIdx.
  const _resolveLandmark = (hit) => {
    let obj = hit.object;
    while (obj.userData.buildingIdx === undefined && obj.parent) obj = obj.parent;
    const idx = obj.userData.buildingIdx;
    return idx !== undefined ? _buildings[idx] ?? null : null;
  };

  if (_appearanceMode && _appearanceGroup) {
    // In appearance mode the landmark and appearance groups are both
    // visible and can both be hit — raycast against both and pick the
    // closest.
    const targets = _landmarkGroup
      ? [_appearanceGroup, _landmarkGroup]
      : [_appearanceGroup];
    const hits = rc.intersectObjects(targets, true);
    if (!hits.length) return null;
    return _resolveLandmark(hits[0]);
  }

  // Info mode — always-on landmarks (e.g. Växjö Cathedral) and the merged
  // extruded-polygon mesh are both picking targets. Raycast each and keep
  // the nearest hit. The nature overlay meshes have .raycast = () => {}
  // so they never intercept clicks — picks always flow through to the
  // base _buildingMesh which resolves water/forest cells via their own
  // vertex ranges.
  let best = null;
  if (_landmarkGroup) {
    const lHits = rc.intersectObject(_landmarkGroup, true);
    if (lHits.length) best = { d: lHits[0].distance, hit: lHits[0], kind: 'landmark' };
  }
  if (_buildingMesh) {
    const bHits = rc.intersectObject(_buildingMesh);
    if (bHits.length && (!best || bHits[0].distance < best.d)) {
      best = { d: bHits[0].distance, hit: bHits[0], kind: 'mesh' };
    }
  }
  // Water lakes/rivers: hover-only via the invisible raycast proxy,
  // gated by the Nature toggle. Note: Three.js's intersectObject does
  // NOT skip invisible meshes, so we check .visible explicitly instead
  // of relying on the earlier `_waterRaycastMesh.visible = false` flip.
  if (_waterRaycastMesh && _waterRaycastMesh.visible) {
    const wHits = rc.intersectObject(_waterRaycastMesh);
    if (wHits.length && (!best || wHits[0].distance < best.d)) {
      best = { d: wHits[0].distance, hit: wHits[0], kind: 'water' };
    }
  }
  if (!best) return null;

  if (best.kind === 'landmark') return _resolveLandmark(best.hit);
  const vtxIdx = best.hit.face.a;
  if (best.kind === 'water') {
    const i = _vtxToRangeIdx(_waterRanges, vtxIdx);
    return i >= 0 ? _waterCells[i] : null;
  }
  // face.a is the vertex index (non-indexed geometry: verified by toNonIndexed in mergeGeometries)
  return _buildingAtVertex(vtxIdx);
}

function _onMouseMove(e) {
  if (!hoverCb) return;
  if (_areasOverlayActive) { hoverCb(null); return; }
  // Spatial-tool selection mode owns the map cursor — no building hover.
  if (document.body.classList.contains('spatial-selecting')) { hoverCb(null); return; }
  hoverCb(_hitBuilding(_getNDC(e)) ?? null);
}

function _onClick(e) {
  if (_areasOverlayActive) return;
  // While a spatial intervention tool is in selection mode, the map
  // click belongs to that tool — never pick or select a 3D building.
  if (document.body.classList.contains('spatial-selecting')) return;
  const cell = _hitBuilding(_getNDC(e));

  // Water is hover-only — no selection, no outline, no patient-zero
  // pinning. The hover tooltip still shows via _onMouseMove.
  if (cell && cell.zone === 19) return;

  if (!cell || _selectedCell?.idx === cell.idx) {
    _clearSelection();
    if (_selCb) _selCb(null);
    return;
  }

  _selectedCell = cell;
  _selectedIdx  = cell.idx;
  _buildSelectionIndicator(cell);
  _applyPOISelectionFilter();
  // Re-paint the building colour buffer so non-selected buildings dim
  // immediately, regardless of mode (live or compare/diff). Side-by-side
  // hides _buildingMesh so this is a no-op there — the cmp-buildings
  // MapLibre layer handles its own selection paint.
  _refreshSelectionPaint();
  if (_selCb) _selCb(cell);
}

function _clearSelection() {
  _selectedCell = null;
  _selectedIdx  = -1;
  _disposeSelectionIndicator();
  _applyPOISelectionFilter();
  _refreshSelectionPaint();
}

/** Public: drop any active building selection. Used by the Reset button. */
export function clearSelection() {
  _clearSelection();
  if (_selCb) _selCb(null);
}

/** Re-paint the building colour buffer for the current selection state.
 *  Routes to the right pipeline based on whether compare/diff is on so
 *  the user sees consistent yellow-outline + grey-out behaviour in
 *  single view AND in diff view (split has its own MapLibre selection
 *  paint and its three.js mesh is hidden anyway). Idempotent. */
function _refreshSelectionPaint() {
  if (!_buildingMesh) return;
  if (_cmpEnabled && _cmpMode === 'diff' && _lastCmpCellsA && _lastCmpCellsB) {
    _paintDiffColors(_lastCmpCellsA, _lastCmpCellsB, _lastCmpMetric);
  } else if (_lastCells) {
    updateColors(_lastCells);
  }
}

/**
 * Public: programmatically select a building by its layout index. Mirrors
 * what _onClick does for a real click — builds the yellow outline overlay,
 * dims non-selected buildings, and fires the selection callback so ui.js
 * can refresh the minimap, flow filter, infection-flow filter, etc.
 *
 * Used by the Cmd+K building search so picking a result both flies the
 * camera AND visually highlights the chosen building.
 */
export function selectByIdx(idx) {
  if (!Number.isFinite(idx)) return;
  const cell = _buildings[idx];
  if (!cell) return;
  // Match the click-flow no-op for water (hover-only, never selectable).
  if (cell.zone === 19) return;
  _selectedCell = cell;
  _selectedIdx  = cell.idx;
  _buildSelectionIndicator(cell);
  _applyPOISelectionFilter();
  _refreshSelectionPaint();
  if (_selCb) _selCb(cell);
}

function _disposeSelectionIndicator() {
  if (!_selIndicator) return;
  scene.remove(_selIndicator);
  const line = _selIndicator.userData.line;
  if (line) { line.geometry.dispose(); line.material.dispose(); }
  _selIndicator = null;
}

/**
 * Build a polygon-shaped selection highlight over the given building:
 *   • a bright yellow outline traced along every edge (Line2, pixel-width
 *     so it stays visible at every zoom level — including huge lake /
 *     forest polygons where a translucent fill would wash the whole
 *     map yellow).
 * updateScene() pulses the line opacity for visual feedback.
 */
function _buildSelectionIndicator(cell) {
  _disposeSelectionIndicator();

  // Landmarks (custom 3D assets) provide their own silhouette polygon in
  // local coordinates via root.userData.customFootprint. Transform it with
  // the landmark's rotation + world position so the yellow highlight
  // actually hugs the cathedral cross shape instead of the OSM rectangle.
  let polygon = cell.polygon;
  if (cell.custom_builder) {
    const root = _landmarkByIdx.get(cell.idx);
    const local = root?.userData.customFootprint;
    if (root && Array.isArray(local) && local.length >= 3) {
      const cy = Math.cos(root.rotation.y);
      const sy = Math.sin(root.rotation.y);
      polygon = local.map(([lx, lz]) => [
        root.position.x + lx * cy + lz * sy,
        root.position.z - lx * sy + lz * cy,
      ]);
    }
  }

  if (!polygon || polygon.length < 3) return;

  const group = new THREE.Group();
  // Sit just above the ground plane so the outline draws cleanly without
  // z-fighting the shadow receiver or the extruded footprint bottom.
  group.position.y = 0.25;

  // ── Outline (LineSegments2) ──────────────────────────────────────────
  // Build one segment per polygon edge + the closing edge. Two endpoints
  // per segment → 6 floats per segment in the flat positions array that
  // LineSegmentsGeometry expects.
  const n = polygon.length;
  const segs = new Float32Array(n * 6);
  for (let i = 0; i < n; i++) {
    const [ax, az] = polygon[i];
    const [bx, bz] = polygon[(i + 1) % n];
    const o = i * 6;
    segs[o]     = ax; segs[o + 1] = 0;    segs[o + 2] = az;
    segs[o + 3] = bx; segs[o + 4] = 0;    segs[o + 5] = bz;
  }
  const lineGeo = new LineSegmentsGeometry();
  lineGeo.setPositions(segs);

  const lineMat = new LineMaterial({
    color:       0xfbbf24,
    linewidth:   5,           // in world pixels
    transparent: true,
    opacity:     0.95,
    depthTest:   false,       // stay visible through forest ground pads
  });
  // LineMaterial needs the renderer resolution to size pixels correctly.
  const rs = new THREE.Vector2();
  renderer.getSize(rs);
  lineMat.resolution.set(rs.x, rs.y);

  const line = new LineSegments2(lineGeo, lineMat);
  line.computeLineDistances();
  line.renderOrder = 1001;     // draw after water/forest overlays
  group.add(line);

  // Stash refs so updateScene() can pulse the outline opacity without
  // re-walking children every frame.
  group.userData.line = line;
  group.visible       = true;

  scene.add(group);
  _selIndicator = group;
}

// ── POI pins ────────────────────────────────────────────────────────────────

/** Look up the meta object for a category key (zone ID or PATIENT_ZERO_KEY). */
function _poiMeta(key) {
  if (key === PATIENT_ZERO_KEY) return PATIENT_ZERO_META;
  return POI_CATEGORIES[key] || null;
}

/**
 * Whether this POI category should render its "closed" variant given the
 * currently active interventions. Driven by POI_CLOSED_BY_INTERVENTION.
 */
function _isPoiClosed(key) {
  for (const [iv, keys] of Object.entries(POI_CLOSED_BY_INTERVENTION)) {
    if (_activeInterventions.has(iv) && keys.includes(key)) return true;
  }
  return false;
}

/**
 * Draw a Google-Maps-style teardrop pin with a coloured head and a centred
 * emoji glyph, then cache it as a THREE.CanvasTexture per category key.
 * If the category is currently "closed" by an active intervention (e.g.
 * schools while Close Schools is on), an unmistakable red prohibition
 * ring is composited over the pin head.
 */
function _makePinTexture(key) {
  if (_poiTextures[key]) return _poiTextures[key];

  const cat = _poiMeta(key);
  if (!cat) return null;

  const closed = _isPoiClosed(key);

  const W = 128, H = 160;
  const canvas = document.createElement('canvas');
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext('2d');

  // Pin body: circle with a tapered tail pointing down.
  const cx = W / 2;
  const cy = 56;
  const r  = 44;

  // Drop shadow
  ctx.save();
  ctx.shadowColor = 'rgba(0,0,0,0.45)';
  ctx.shadowBlur  = 10;
  ctx.shadowOffsetY = 4;

  // Tail
  ctx.beginPath();
  ctx.moveTo(cx - 18, cy + 20);
  ctx.quadraticCurveTo(cx, H - 6, cx + 18, cy + 20);
  ctx.closePath();
  ctx.fillStyle = closed ? '#6b7280' : cat.color;
  ctx.fill();

  // Head
  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.fillStyle = closed ? '#6b7280' : cat.color;
  ctx.fill();
  ctx.restore();

  // White inset ring
  ctx.lineWidth   = 4;
  ctx.strokeStyle = 'rgba(255,255,255,0.92)';
  ctx.beginPath();
  ctx.arc(cx, cy, r - 4, 0, Math.PI * 2);
  ctx.stroke();

  // Glyph inside the head. LNU gets the actual circle-tree brand logo
  // on a yellow disc; every other category renders the emoji glyph.
  ctx.save();
  if (closed) ctx.globalAlpha = 0.55;
  if (key === 'lnu_flag') {
    // Yellow disc filling the white ring, then the black tree over it
    ctx.beginPath();
    ctx.arc(cx, cy, r - 6, 0, Math.PI * 2);
    ctx.fillStyle = '#fcd116';           // LNU brand yellow
    ctx.fill();
    drawLNUTree(ctx, cx, cy + 2, (r - 6) * 1.85);
  } else {
    ctx.font = '52px "Segoe UI Emoji","Apple Color Emoji",sans-serif';
    ctx.textAlign    = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(cat.emoji, cx, cy + 2);
  }
  ctx.restore();

  if (closed) {
    // Prohibition mark: thick red ring + diagonal slash, drawn on top so
    // it's instantly readable at any zoom — the universal "closed" symbol.
    ctx.save();
    ctx.lineCap   = 'round';
    ctx.strokeStyle = '#ef4444';
    ctx.lineWidth   = 9;
    ctx.beginPath();
    ctx.arc(cx, cy, r + 2, 0, Math.PI * 2);
    ctx.stroke();
    // 45° slash from upper-left to lower-right of the head
    const off = (r + 2) * Math.SQRT1_2;
    ctx.beginPath();
    ctx.moveTo(cx - off, cy - off);
    ctx.lineTo(cx + off, cy + off);
    ctx.stroke();
    ctx.restore();
  }

  const tex = new THREE.CanvasTexture(canvas);
  tex.anisotropy = 4;
  tex.needsUpdate = true;
  _poiTextures[key] = tex;
  return tex;
}

/** Get-or-build the shared material for a POI category. */
function _getPOIMaterial(key) {
  if (_poiMaterials[key]) return _poiMaterials[key];
  const tex = _makePinTexture(key);
  if (!tex) return null;
  const mat = new THREE.MeshBasicMaterial({
    map:         tex,
    transparent: true,
    depthWrite:  false,
    depthTest:   false,        // pins always draw on top
    side:        THREE.DoubleSide,
    toneMapped:  false,
  });
  _poiMaterials[key] = mat;
  return mat;
}

/**
 * Build one mesh per POI-category building at the given building's
 * centroid, hovering just above the rooftop. Meshes are grouped by zone
 * so per-category visibility is a single Group.visible flip.
 *
 * A single PlaneGeometry is shared across all pins. Each mesh is manually
 * oriented every frame (see updateScene) to face the camera, which the
 * MapLibre custom-layer camera setup can't do automatically.
 */
function _buildPOIs() {
  // Dispose previous group
  if (_poiGroup) {
    scene.remove(_poiGroup);
    _poiGroup = null;
  }
  _poiAllMeshes = [];
  for (const k of Object.keys(_poiSubGroups)) delete _poiSubGroups[k];

  if (!_poiPinGeo) _poiPinGeo = new THREE.PlaneGeometry(1, 1);

  const group = new THREE.Group();
  group.visible = _poiVisible;

  // Helper to add one pin mesh for a building idx into a sub-group.
  // opts.sizeMult (default 1) scales this pin relative to the baseline
  // per-frame size computed in _updatePOIs. opts.maxW (default MAX_PIN_W)
  // overrides the on-screen size cap — used by patient-zero pins so they
  // stay visible in large cities like Stockholm where the normal cap
  // makes a single pin vanish against the cityscape.
  const spawnPin = (buildingIdx, key, baseYOffset, opts = {}) => {
    const b = _buildings[buildingIdx];
    if (!b) return;
    const mat = _getPOIMaterial(key);
    if (!mat) return;
    const mesh = new THREE.Mesh(_poiPinGeo, mat);
    const roofY = _baseHeights[buildingIdx] || 6;
    const baseY = roofY + baseYOffset;
    mesh.position.set(b.world_x, baseY, b.world_z);
    mesh.renderOrder = 1000;
    mesh.userData.buildingIdx = buildingIdx;
    mesh.userData.baseY       = baseY;
    mesh.userData.bobPhase    = Math.random() * Math.PI * 2;
    mesh.userData.poiKey      = key;
    mesh.userData.sizeMult    = opts.sizeMult || 1.0;
    mesh.userData.maxW        = opts.maxW     || 500;
    _poiSubGroups[key].add(mesh);
    _poiAllMeshes.push(mesh);
  };

  // One sub-group per category (zones first, then patient zero on top)
  for (const zid of Object.keys(POI_CATEGORIES)) {
    const sg = new THREE.Group();
    sg.visible = !!_poiCatVisible[zid];
    sg.userData.poiKey = zid;
    group.add(sg);
    _poiSubGroups[zid] = sg;
  }
  const pzGroup = new THREE.Group();
  pzGroup.visible = !!_poiCatVisible[PATIENT_ZERO_KEY];
  pzGroup.userData.poiKey = PATIENT_ZERO_KEY;
  group.add(pzGroup);
  _poiSubGroups[PATIENT_ZERO_KEY] = pzGroup;

  // Iterate buildings, spawn one pin per POI building at its centroid.
  // Water buildings (zone 19) use their water_type for sub-classification.
  for (let i = 0; i < _buildings.length; i++) {
    const b = _buildings[i];
    let poiKey;
    if (b.zone === 19 && b.water_type) {
      poiKey = 'water_' + b.water_type;
    } else {
      poiKey = String(b.zone);
    }
    const cat = POI_CATEGORIES[poiKey];
    if (!cat) continue;
    spawnPin(i, poiKey, 30);
  }

  // Transport stop pins — anchored at the transport stop world coords
  // (not building indices) so they sit above the procedural transport
  // buildings created by static/transport.js. Each pin's `userData` has
  // no buildingIdx so the building-selection filter correctly leaves them
  // alone (transport pins are always shown when their category is on).
  const spawnTransportPin = (stop) => {
    const key = stop.type;
    if (!POI_CATEGORIES[key]) return;
    const mat = _getPOIMaterial(key);
    if (!mat) return;
    const mesh = new THREE.Mesh(_poiPinGeo, mat);
    const baseY = _TRANSPORT_PIN_BASE_Y[key] ?? 12;
    mesh.position.set(stop.world_x, baseY, stop.world_z);
    mesh.renderOrder = 1000;
    mesh.userData.buildingIdx = -1;          // not a building
    mesh.userData.transportStopIdx = stop.idx;
    mesh.userData.baseY    = baseY;
    mesh.userData.bobPhase = Math.random() * Math.PI * 2;
    mesh.userData.poiKey   = key;
    mesh.userData.sizeMult = 1.0;
    mesh.userData.maxW     = 500;
    _poiSubGroups[key].add(mesh);
    _poiAllMeshes.push(mesh);
  };
  for (const stop of _transportStops) {
    if (!_TRANSPORT_POI_KEYS.has(stop.type)) continue;
    spawnTransportPin(stop);
  }

  // Lightweight nature POIs — same pin pipeline as transport stops.
  // Fixed base height above ground level since these don't have a
  // building to perch on. renderOrder matches transport pins so they
  // sort the same way in the z-buffer at long zooms.
  const spawnNaturePin = (poi) => {
    const key = poi.type;
    if (!POI_CATEGORIES[key]) return;
    const mat = _getPOIMaterial(key);
    if (!mat) return;
    const mesh = new THREE.Mesh(_poiPinGeo, mat);
    const baseY = 18;    // constant — no backing building
    mesh.position.set(poi.world_x, baseY, poi.world_z);
    mesh.renderOrder = 1000;
    mesh.userData.buildingIdx = -1;
    mesh.userData.naturePOIIdx = poi.idx;
    mesh.userData.baseY    = baseY;
    mesh.userData.bobPhase = Math.random() * Math.PI * 2;
    mesh.userData.poiKey   = key;
    mesh.userData.sizeMult = 1.0;
    mesh.userData.maxW     = 500;
    _poiSubGroups[key].add(mesh);
    _poiAllMeshes.push(mesh);
  };
  for (const poi of _naturePOIs) {
    if (!_NATURE_POI_KEYS.has(poi.type)) continue;
    spawnNaturePin(poi);
  }

  // Swedish flag easter egg — spawn one 🇸🇪 pin above each pole. The
  // 3D flag mesh itself is always visible in _flagPoleGroup; the pin
  // just makes them discoverable via the POI panel.
  const spawnFlagPin = (pole, key) => {
    const sg = _poiSubGroups[key];
    const mat = sg ? _getPOIMaterial(key) : null;
    if (!sg || !mat) return;
    const mesh = new THREE.Mesh(_poiPinGeo, mat);
    // Pole top finial sits at y≈8.2 and the flag plane at y≈7.25 with
    // a 1.5 m height. The billboarded pin is tall (sizeH ≥ 50 at close
    // zoom) so a small baseY plants it right on top of the flag. Lift
    // it well above so the flag stays visible under the pin.
    const baseY = 22;
    mesh.position.set(pole.world_x, baseY, pole.world_z);
    mesh.renderOrder = 1000;
    mesh.userData.buildingIdx = -1;
    mesh.userData.flagPoleIdx = pole.idx;
    mesh.userData.baseY    = baseY;
    mesh.userData.bobPhase = Math.random() * Math.PI * 2;
    mesh.userData.poiKey   = key;
    mesh.userData.sizeMult = 1.0;
    mesh.userData.maxW     = 500;
    sg.add(mesh);
    _poiAllMeshes.push(mesh);
  };
  if (_flagPoles.length > 0 && POI_CATEGORIES.swedish_flag) {
    for (const pole of _flagPoles) spawnFlagPin(pole, 'swedish_flag');
  }
  if (_lnuFlagPoles.length > 0 && POI_CATEGORIES.lnu_flag) {
    for (const pole of _lnuFlagPoles) spawnFlagPin(pole, 'lnu_flag');
  }
  // Easter egg pins
  if (_easterEggs.length > 0) {
    for (const egg of _easterEggs) {
      if (POI_CATEGORIES[egg.type]) spawnFlagPin(egg, egg.type);
    }
  }

  // Patient-zero pins use the same zoom/size rules as regular POIs.
  for (const idx of _patientZeroIdx) {
    spawnPin(idx, PATIENT_ZERO_KEY, 30);
  }

  scene.add(group);
  _poiGroup = group;
  _applyPOISelectionFilter();
}

/**
 * When a building is selected AND the POI layer is on, hide every POI
 * pin whose building isn't the selection. With no selection, show pins
 * according to the per-category checkbox state.
 */
function _applyPOISelectionFilter() {
  if (_poiAllMeshes.length === 0) return;
  const hasSel = _selectedIdx >= 0;
  for (const m of _poiAllMeshes) {
    // Transport pins (transportStopIdx set, no building) ignore the
    // building selection — keep them visible whenever their category is on.
    const isTransport = (m.userData.transportStopIdx !== undefined);
    if (hasSel && !isTransport) {
      m.visible = (m.userData.buildingIdx === _selectedIdx);
    } else {
      m.visible = true;   // category visibility handled by parent group
    }
  }
}

// Scratch vectors — reused each frame to avoid GC pressure
const _poiTmpNear = new THREE.Vector3();
const _poiTmpFar  = new THREE.Vector3();
const _poiTmpL    = new THREE.Vector3();
const _poiTmpR    = new THREE.Vector3();
const _poiTmpCam  = new THREE.Vector3();
const _poiTmpDir  = new THREE.Vector3();
const _upY        = new THREE.Vector3(0, 1, 0);
const _lookM      = new THREE.Matrix4();

/**
 * Per-frame animation for the POI pin layer. Called from updateScene().
 * Drives three effects:
 *   • Billboard: each pin faces the camera (the MapLibre custom-layer
 *     camera setup doesn't populate matrixWorld, so Sprites would lay
 *     flat in the world XY plane; we orient meshes manually).
 *   • Bob: a gentle sin wave on Y per pin with randomised phase.
 *   • Zoom-aware scale: pins grow when the camera pulls back, so they
 *     stay legible from far away without dominating close-up views.
 */
function _updatePOIs() {
  if (!_poiGroup || !_poiGroup.visible || !_mvpMatrix) return;
  if (_poiAllMeshes.length === 0) return;

  // Inverse MVP lets us back-project NDC points into world space
  const invMVP = _mvpMatrix.clone().invert();

  // NDC z=-1 → near plane center; z=+1 → far plane center.
  _poiTmpNear.set(0, 0, -1).applyMatrix4(invMVP);
  _poiTmpFar .set(0, 0,  1).applyMatrix4(invMVP);

  // Direction from far → near == vector pointing toward the camera
  _poiTmpDir.subVectors(_poiTmpNear, _poiTmpFar).normalize();
  // Synthetic camera position far away along that direction — far enough
  // that lookAt() produces effectively parallel orientation for every pin
  _poiTmpCam.copy(_poiTmpNear).addScaledVector(_poiTmpDir, 100000);

  // Export camera-facing quaternion for transport warning sprites.
  // A dummy matrix looking from origin toward _poiTmpCam gives us the quat.
  _lookM.lookAt(_poiTmpCam, _poiTmpNear, _upY);
  _exportedCamQuat.setFromRotationMatrix(_lookM);
  _camQuatReady = true;

  // Visible world width at screen-center: distance between the left and
  // right edges projected back at z=0. Used to grow/shrink pins smoothly.
  _poiTmpL.set(-1, 0, 0).applyMatrix4(invMVP);
  _poiTmpR.set( 1, 0, 0).applyMatrix4(invMVP);
  const visWidth = _poiTmpL.distanceTo(_poiTmpR);

  // Pin world-size must grow *faster* than visWidth to be visibly larger
  // when zoomed out. Perspective divides on-screen size by ~distance (≈
  // proportional to visWidth), so a linear worldSize ∝ visWidth produces
  // constant on-screen size — useless. We use a super-linear curve:
  //
  //   sizeW = 0.04 * visWidth * (1 + visWidth / 3000)
  //
  // On-screen size ≈ sizeW / visWidth = 0.04 + visWidth / 75000, which
  // grows linearly with zoom-out. Worked examples (aspect 1.25):
  //   visWidth = 1500 m  →  sizeW ≈  90  (close view)
  //   visWidth = 4000 m  →  sizeW ≈ 373  (city view)
  //   visWidth = 9000 m  →  sizeW ≈ 1440 (zoomed way out)
  const MIN_PIN_W = 40;
  const baseSizeW = visWidth * 0.04 * (1 + visWidth / 3000) * 10;

  // Bob parameters — slightly more pronounced: ±5 world units, ~1.6 rad/s
  const now  = performance.now() * 0.001;
  const BOB_AMP   = 5.0;
  const BOB_SPEED = 1.6;

  for (const m of _poiAllMeshes) {
    if (!m.visible) continue;
    // Per-mesh size: multiplier lifts special pins (patient zero) above
    // the baseline, maxW is the on-screen cap. Both default to
    // ordinary-pin values when userData wasn't set on spawn.
    const mult = m.userData.sizeMult || 1.0;
    const cap  = m.userData.maxW     || 500;
    const sizeW = Math.min(cap, Math.max(MIN_PIN_W * mult, baseSizeW * mult));
    const sizeH = sizeW * 1.25;

    // All pins share the same phase so they bob in unison
    m.position.y = m.userData.baseY
      + Math.sin(now * BOB_SPEED) * BOB_AMP;
    m.lookAt(_poiTmpCam);
    m.scale.set(sizeW, sizeH, 1);
  }
}

/** Toggle the entire POI pin layer on/off. */
export function setPOIVisible(visible) {
  _poiVisible = !!visible;
  if (!_poiGroup && _poiVisible && _buildings.length > 0) _buildPOIs();
  if (_poiGroup) {
    _poiGroup.visible = _poiVisible;
    _applyPOISelectionFilter();
  }
}

/**
 * Toggle visibility of a single POI category. Accepts a zone ID
 * (number or numeric string) or PATIENT_ZERO_KEY ('patient_zero').
 */
export function setPOICategoryVisible(key, visible) {
  const k = String(key);
  _poiCatVisible[k] = !!visible;
  const sg = _poiSubGroups[k];
  if (sg) sg.visible = !!visible;
}

// Toggle every nature layer at once:
//   • _waterLineMesh    — dashed animated lake/river outlines
//   • _waterRaycastMesh — invisible hover proxy (Three.js skips raycast
//                         on `.visible = false` objects, so flipping
//                         this also disables the water hover tooltip)
//   • _naturalGroup     — species-aware tree scatter on forestry polygons
// Persists across city switches so a city swap doesn't silently reset
// the preference.
export function setNatureDetailVisible(visible) {
  _natureDetailOn = !!visible;
  if (_waterLineMesh)    _waterLineMesh.visible    = _natureDetailOn;
  if (_waterRaycastMesh) _waterRaycastMesh.visible = _natureDetailOn;
  if (_naturalGroup)     _naturalGroup.visible     = _natureDetailOn;
}

export function isNatureDetailVisible() { return _natureDetailOn; }

// ── Secondary renderer + split mode ────────────────────────────────────────
// compare-view-b.js attaches a second three.js WebGLRenderer to map B's
// custom-layer GL context so the same scene renders into both halves —
// 3D trees, animated dashed lakes, and landmarks all appear on the right
// side too. The cmp-buildings MapLibre fill-extrusion overlays the
// per-day SEIR colours on top of three.js, and setSplitMode hides the
// three.js building / POI meshes that the overlay duplicates.
export function attachSecondaryRenderer(gl) {
  if (_rendererB) return;
  _rendererB = new THREE.WebGLRenderer({
    canvas:    gl.canvas,
    context:   gl,
    antialias: true,
  });
  _rendererB.autoClear        = false;
  _rendererB.outputColorSpace = THREE.SRGBColorSpace;
  _rendererB.shadowMap.enabled = true;
  _rendererB.shadowMap.type    = THREE.PCFSoftShadowMap;
}

export function detachSecondaryRenderer() {
  if (!_rendererB) return;
  // dispose() releases the renderer's per-context WebGL caches without
  // touching the shared Geometry/Material objects (those still belong
  // to the primary renderer + the JS scene graph).
  try { _rendererB.dispose(); } catch {}
  _rendererB = null;
}

export function setSplitMode(on) {
  _splitMode = !!on;
  if (_buildingMesh)    _buildingMesh.visible    = !_splitMode;
  if (_landmarkGroup)   _landmarkGroup.visible   = !_splitMode;
  if (_appearanceGroup) _appearanceGroup.visible = !_splitMode && _appearanceMode;
  if (_poiGroup)        _poiGroup.visible        = !_splitMode && _poiVisible;
  // Selection indicator disappears in split mode — compare-view-b.js's
  // own outline expression handles the highlight on both halves.
  if (_selIndicator)    _selIndicator.visible    = !_splitMode;
  // Street lights are gated by updateDayNight; force off in split so we
  // don't get duplicate dawn/dusk halos through the cmp overlay.
  if (_splitMode && _streetLights) _streetLights.visible = false;
  // Patient-zero / flag / easter-egg groups stay visible — they're
  // small, non-interactive, and add character to the scene.
}

export function isSplitMode() { return _splitMode; }

// ── Resize plumbing ────────────────────────────────────────────────────────
// MapLibre auto-resizes its canvas on window resize, but three.js's
// internal _currentViewport (set on construction via setSize) goes
// stale — and resetState() only forgets GL state, it doesn't re-read
// the viewport from GL. Without re-syncing it the next render writes
// the OLD viewport over MapLibre's freshly-set new one, which is the
// source of the long-standing "buildings desalign with the basemap on
// resize" bug. Each map's 'resize' event handler should call
// notifyRendererResize so its corresponding renderer's _size + viewport
// stay in sync with the actual canvas.
//
// LineMaterials (water dashes, landmark outlines, selection indicator)
// also need their .resolution updated for crisp pixel-perfect lines —
// refreshLineMaterials walks the scene once and pokes every one.
export function notifyRendererResize(useSecondary, width, height) {
  const r = useSecondary ? _rendererB : renderer;
  if (!r || !width || !height) return;
  const dpr = window.devicePixelRatio || 1;
  if (r.getPixelRatio() !== dpr) r.setPixelRatio(dpr);
  // setSize(w, h, false) updates _size + viewport without touching the
  // canvas style. The canvas.width/height it writes match what MapLibre
  // already set (same w * dpr / h * dpr), so they're a no-op write.
  r.setSize(width, height, false);
}

export function refreshLineMaterials() {
  if (!scene) return;
  const W = window.innerWidth, H = window.innerHeight;
  scene.traverse((obj) => {
    const mat = obj.material;
    if (mat && mat.resolution && (mat.isLineMaterial || mat.constructor?.name === 'LineMaterial')) {
      mat.resolution.set(W, H);
    }
  });
}

// ── Equirect → Mercator reprojection for three.js coordinates ────────────
// Constants are inlined to avoid a per-call allocation when looping over
// hundreds of thousands of polygon vertices. Earth radius matches the
// WGS-84 mean used inside MapLibre's MercatorCoordinate.
const _EARTH_RADIUS_M = 6371008.8;
const _EARTH_CIRC_M   = 2 * Math.PI * _EARTH_RADIUS_M;

function _reprojectLayoutForMercator(layout) {
  if (!layout || layout._mercator_projected) return;
  const cLat = layout.center_lat;
  const cLon = layout.center_lon;
  if (typeof cLat !== 'number' || typeof cLon !== 'number') return;
  const cosCLat = Math.cos(cLat * Math.PI / 180);
  if (cosCLat === 0) return;

  // Inverse equirect (osm.py's latlon_to_meters): wx = (lon - cLon) * mplx,
  // wz = -(lat - cLat) * mpl.
  const equirectMpl  = 111320;
  const equirectMplx = 111320 * cosCLat;

  // MapLibre's normalised Web Mercator helpers, inlined.
  const PI    = Math.PI;
  const cmx   = (180 + cLon) / 360;
  const cmy   = (180 - 180 / PI * Math.log(Math.tan(PI / 4 + cLat * PI / 360))) / 360;
  // meterInMercatorCoordinateUnits at the city centre — the same scale
  // value three.js's worldToMerc applies uniformly to every vertex.
  const scale = 1 / (_EARTH_CIRC_M * cosCLat);

  function reproj(wx, wz) {
    // equirect-at-centre → lat/lon
    const lon = wx / equirectMplx + cLon;
    const lat = -wz / equirectMpl + cLat;
    // lat/lon → normalised Web Mercator
    const mx = (180 + lon) / 360;
    const my = (180 - 180 / PI * Math.log(Math.tan(PI / 4 + lat * PI / 360))) / 360;
    // Mercator delta from centre, divided by the centre scale → the
    // "metres-at-centre" value that, when fed back through three.js's
    // worldToMerc with the same uniform scale, lands at the *correct*
    // Mercator position (i.e. where MapLibre projects this lat/lon).
    return [(mx - cmx) / scale, (my - cmy) / scale];
  }

  // Building polygons + holes + centroids.
  for (const b of layout.buildings || []) {
    if (Array.isArray(b.polygon)) {
      const out = new Array(b.polygon.length);
      for (let i = 0; i < b.polygon.length; i++) {
        const p = b.polygon[i];
        out[i] = reproj(p[0], p[1]);
      }
      b.polygon = out;
    }
    if (Array.isArray(b.polygon_holes)) {
      b.polygon_holes = b.polygon_holes.map(hole =>
        hole.map(p => reproj(p[0], p[1])),
      );
    }
    if (typeof b.world_x === 'number' && typeof b.world_z === 'number') {
      const r = reproj(b.world_x, b.world_z);
      b.world_x = r[0]; b.world_z = r[1];
    }
  }

  // Point features that three.js places by world_x / world_z.
  for (const key of ['nature_pois', 'flag_poles', 'lnu_flag_poles', 'easter_eggs']) {
    const arr = layout[key];
    if (!Array.isArray(arr)) continue;
    for (const p of arr) {
      if (typeof p.world_x === 'number' && typeof p.world_z === 'number') {
        const r = reproj(p.world_x, p.world_z);
        p.world_x = r[0]; p.world_z = r[1];
      }
    }
  }

  // NB: layout.roads is intentionally NOT reprojected. map.js converts
  // roads back to lat/lon via the equirect inverse formula and feeds
  // them to MapLibre, which projects them natively — the round-trip is
  // exact, so roads end up at their original OSM lat/lon and align with
  // the basemap. Reprojecting them here would break that round-trip.

  layout._mercator_projected = true;
}

/**
 * Set the list of building indices that hosted the initial outbreak.
 * Rebuilds the patient-zero pin sub-group only when the list actually
 * changes, so this is safe to call on every state update.
 */
/**
 * Update the list of transport stops fed to the POI layer. Called by
 * ui.js after /api/transport resolves. Triggers a rebuild of the pin
 * group so the new transport pins appear immediately.
 */
/** Current visible world width (metres). Used by LOD systems. */
export function getVisibleWidth() { return _lastVisWidth; }

/** Camera-facing quaternion computed from the POI billboard pass.
 *  Used by transport.js to billboard warning sprites. */
const _exportedCamQuat = new THREE.Quaternion();
let _camQuatReady = false;
export function getCameraQuat() { return _camQuatReady ? _exportedCamQuat : null; }

export function setTransportStops(stops) {
  _transportStops = Array.isArray(stops) ? stops.slice() : [];
  if (_poiGroup && _buildings.length > 0) _buildPOIs();
}

/** Update the list of lightweight nature POIs (nature reserves,
 *  cemeteries, …) fed into the pin layer. Same rebuild semantics as
 *  setTransportStops — the pin group is rebuilt from scratch if it
 *  already exists. */
export function setNaturePOIs(pois) {
  _naturePOIs = Array.isArray(pois) ? pois.slice() : [];
  if (_poiGroup && _buildings.length > 0) _buildPOIs();
}

/** Install the Swedish flag pole easter egg: builds one 3D flag pole
 *  Group per entry in layout.flag_poles and adds it to a dedicated
 *  scene layer that's always visible (never recoloured or height-
 *  scaled). The POI pin list is rebuilt so the 🇸🇪 pin category
 *  also updates. */
export function setFlagPoles(poles) {
  _flagPoles = Array.isArray(poles) ? poles.slice() : [];
  // Dispose old 3D group (fresh scene on every call).
  if (_flagPoleGroup) {
    scene.remove(_flagPoleGroup);
    _flagPoleGroup.traverse(o => { if (o.isMesh) o.geometry.dispose?.(); });
    _flagPoleGroup = null;
  }
  if (_flagPoles.length > 0) {
    _flagPoleGroup = new THREE.Group();
    _flagPoleGroup.name = 'swedishFlagPoles';
    for (const p of _flagPoles) {
      _flagPoleGroup.add(
        buildSwedishFlagPole(THREE, p.world_x, p.world_z, p.idx ?? 0),
      );
    }
    scene.add(_flagPoleGroup);
  }
  if (_poiGroup && _buildings.length > 0) _buildPOIs();
}

/** Install the LNU (Linnaeus University) flag poles — same pattern
 *  as setFlagPoles but with a different 3D asset (blue + yellow "LNU"
 *  wordmark). Only Växjö + Kalmar currently provide lnu_flag_poles. */
export function setLNUFlagPoles(poles) {
  _lnuFlagPoles = Array.isArray(poles) ? poles.slice() : [];
  if (_lnuFlagPoleGroup) {
    scene.remove(_lnuFlagPoleGroup);
    _lnuFlagPoleGroup.traverse(o => { if (o.isMesh) o.geometry.dispose?.(); });
    _lnuFlagPoleGroup = null;
  }
  if (_lnuFlagPoles.length > 0) {
    _lnuFlagPoleGroup = new THREE.Group();
    _lnuFlagPoleGroup.name = 'lnuFlagPoles';
    // Alternate between the two official LNU identities so a campus
    // visibly shows both brand variants (yellow field / white field).
    _lnuFlagPoles.forEach((p, i) => {
      const variant = (i % 2 === 0) ? 'yellow' : 'white';
      _lnuFlagPoleGroup.add(
        buildLNUFlagPole(THREE, p.world_x, p.world_z, p.idx ?? 0, variant),
      );
    });
    scene.add(_lnuFlagPoleGroup);
  }
  if (_poiGroup && _buildings.length > 0) _buildPOIs();
}

/** Install Swedish cultural easter eggs — one of each type per city. */
export function setEasterEggs(eggs) {
  _easterEggs = Array.isArray(eggs) ? eggs.slice() : [];
  if (_easterEggGroup) {
    scene.remove(_easterEggGroup);
    _easterEggGroup.traverse(o => { if (o.isMesh) o.geometry.dispose?.(); });
    _easterEggGroup = null;
  }
  if (_easterEggs.length > 0) {
    _easterEggGroup = new THREE.Group();
    _easterEggGroup.name = 'easterEggs';
    for (const egg of _easterEggs) {
      const fn = _EASTER_EGG_BUILDERS[egg.type];
      if (fn) {
        const obj = fn(THREE, egg.world_x, egg.world_z);
        if (obj) _easterEggGroup.add(obj);
      }
    }
    scene.add(_easterEggGroup);
    // Non-raycastable — clicks pass through to real buildings
    _easterEggGroup.traverse(o => { if (o.isMesh) o.raycast = () => {}; });
  }
  if (_poiGroup && _buildings.length > 0) _buildPOIs();
}

export function setPatientZeroIndices(indices) {
  const next = Array.isArray(indices) ? indices.slice() : [];
  // Cheap diff via stringified comparison — these arrays are tiny.
  const a = JSON.stringify(next);
  const b = JSON.stringify(_patientZeroIdx);
  if (a === b) return;
  _patientZeroIdx = next;
  // Rebuild only if the POI layer has been built at least once;
  // otherwise the next setPOIVisible(true) will pick it up.
  if (_poiGroup && _buildings.length > 0) _buildPOIs();
}

/** Whether the POI pin layer is currently on. */
export function isPOIVisible() { return _poiVisible; }

/**
 * Return the set of POI category keys that actually have at least one
 * pin in the current city. Used by the UI to disable empty categories.
 */
export function getAvailablePOIKeys() {
  const keys = new Set();
  // Building-zone POIs (water uses water_type sub-key)
  for (const b of _buildings) {
    if (b.zone === 19 && b.water_type) {
      const wk = 'water_' + b.water_type;
      if (POI_CATEGORIES[wk]) keys.add(wk);
    } else if (POI_CATEGORIES[b.zone]) {
      keys.add(String(b.zone));
    }
  }
  // Transport POIs
  for (const s of _transportStops) {
    if (POI_CATEGORIES[s.type]) keys.add(s.type);
  }
  // Nature POIs (lightweight, string-keyed)
  for (const p of _naturePOIs) {
    if (POI_CATEGORIES[p.type]) keys.add(p.type);
  }
  // Swedish flag easter egg
  if (_flagPoles.length > 0) keys.add('swedish_flag');
  // LNU flag (Växjö + Kalmar only)
  if (_lnuFlagPoles.length > 0) keys.add('lnu_flag');
  // Swedish cultural easter eggs
  for (const egg of _easterEggs) {
    if (POI_CATEGORIES[egg.type]) keys.add(egg.type);
  }
  // Patient zero
  if (_patientZeroIdx.length > 0) keys.add(PATIENT_ZERO_KEY);
  return keys;
}

/**
 * Return the current count per POI category key, mirroring the three
 * passes of getAvailablePOIKeys() (buildings, transport stops, nature
 * POIs, patient zero). The POI menu renders these counts next to the
 * category name: `Hospital (64)`, `Train (3)`, etc.
 */
export function getPOICounts() {
  const counts = {};
  for (const b of _buildings) {
    let k;
    if (b.zone === 19 && b.water_type) {
      k = 'water_' + b.water_type;
    } else {
      k = String(b.zone);
    }
    if (POI_CATEGORIES[k]) counts[k] = (counts[k] || 0) + 1;
  }
  for (const s of _transportStops) {
    if (POI_CATEGORIES[s.type]) counts[s.type] = (counts[s.type] || 0) + 1;
  }
  for (const p of _naturePOIs) {
    if (POI_CATEGORIES[p.type]) counts[p.type] = (counts[p.type] || 0) + 1;
  }
  if (_flagPoles.length > 0) {
    counts.swedish_flag = _flagPoles.length;
  }
  if (_lnuFlagPoles.length > 0) {
    counts.lnu_flag = _lnuFlagPoles.length;
  }
  // Easter eggs
  for (const egg of _easterEggs) {
    counts[egg.type] = (counts[egg.type] || 0) + 1;
  }
  if (_patientZeroIdx.length > 0) {
    counts[PATIENT_ZERO_KEY] = _patientZeroIdx.length;
  }
  return counts;
}

/**
 * Mirror the engine's active-intervention set into the view layer. When the
 * "closed by intervention" state of any POI category changes (e.g. Close
 * Schools is toggled), the cached pin texture for that category is dropped
 * and the POI layer rebuilt so the prohibition mark appears/disappears.
 * No-op if nothing relevant changed — safe to call on every state apply.
 */
export function setActiveInterventions(activeSet) {
  const set = activeSet instanceof Set ? activeSet : new Set(activeSet || []);

  // Detect which POI categories need a texture refresh (closed ↔ open).
  const dirtyKeys = [];
  for (const [iv, keys] of Object.entries(POI_CLOSED_BY_INTERVENTION)) {
    const wasOn = _activeInterventions.has(iv);
    const nowOn = set.has(iv);
    if (wasOn === nowOn) continue;
    for (const k of keys) dirtyKeys.push(k);
  }

  _activeInterventions = set;

  if (dirtyKeys.length === 0) return;

  // Hot-swap: regenerate the texture for each dirty category and patch it
  // into the existing shared material. Every pin mesh that uses this
  // material picks up the change instantly — no scene-graph rebuild, no lag.
  const seen = new Set();
  for (const k of dirtyKeys) {
    if (seen.has(k)) continue;
    seen.add(k);
    // Drop the cached texture so _makePinTexture redraws from scratch.
    delete _poiTextures[k];
    const newTex = _makePinTexture(k);
    if (!newTex) continue;
    const mat = _poiMaterials[k];
    if (mat) {
      // Patch the existing material in-place — all meshes update instantly.
      mat.map = newTex;
      mat.needsUpdate = true;
    }
    // If no material exists yet (POIs not built), the next _buildPOIs will
    // pick up the fresh _poiTextures[k] automatically.
  }
}
