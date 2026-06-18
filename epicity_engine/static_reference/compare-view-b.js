/**
 * compare-view-b.js — Side-by-side compare rendering.
 *
 * Two MapLibre maps share one camera state and one source of GeoJSON
 * building features; each renders the chosen day's colours + heights
 * via fill-extrusion. Map A keeps its three.js custom layer; map B
 * gets a SECOND three.js renderer attached to its GL context (via
 * view.js's attachSecondaryRenderer + setSplitMode) so the same
 * scene — 3D trees, animated dashed lakes, landmarks — appears on
 * BOTH halves. Inside three.js, setSplitMode hides the building /
 * landmark / POI meshes (cmp-buildings overlays them with the day's
 * SEIR colours) but keeps nature visible.
 *
 * Nature: 3D trees and the animated dashed water borders are now
 * rendered through three.js on both halves. The Nature toggle
 * continues to gate them via setNatureDetailVisible — no MapLibre
 * fallback is needed in split mode. refreshNatureVisibility() is
 * kept as a no-op for ui.js back-compat.
 *
 * Selection + hover route through ui.js's existing callbacks: a fill-
 * extrusion click runs `selectByIdx` which the rest of the app already
 * reacts to (info panel, flow filter, three.js indicator). Hover also
 * sets a `hovered` feature-state on BOTH halves so the yellow outline
 * appears on the same building in either map — matching the user's
 * request that hover be a synchronised cross-side indicator.
 *
 * Selection visuals: a yellow outline on the selected polygon, a
 * lighter yellow outline on the hovered polygon, plus a grey-out cast
 * over every non-selected building so the picked one pops.
 *
 * Style sync to map B is rate-limited and gated by a layer-fingerprint
 * check so paint-only mutations (selection grey-out, hover outline)
 * don't trigger a full setStyle resync — that was the source of the
 * "right side blinks on every interaction" bug.
 *
 * Neighborhood (admin area) selection is mirrored across both halves
 * via map.js's addNbhSyncMap registration.
 *
 * Public API:
 *   enableSplit(layout, cellsA, cellsB, opts)  — open + style
 *   applyDays(cellsA, cellsB)                  — refresh colour/height
 *   setSelectedIdx(idx)                        — sync click selection
 *   setHoveredIdx(idx)                         — sync hover highlight
 *   refreshNatureVisibility()                  — re-read Nature toggle
 *   disableSplit()                             — tear down, restore
 *   getMapB()
 */

import {
  getMap, addNbhSyncMap, removeNbhSyncMap,
  getCenterMercator, getMeterScale, isAreasActive,
} from './map.js';
import { sampleScaleCSS } from './colorscales.js';
import {
  getColorMode, getHeightMode, getColorScale,
  getLogScale,  getInvertScale, getNormMode,
} from './legend.js';
import {
  ZONE_COLORS, isNatureDetailVisible,
  attachSecondaryRenderer, detachSecondaryRenderer,
  setSplitMode, renderScene as _viewRenderScene,
  notifyRendererResize, refreshLineMaterials,
} from './view.js';
import {
  setSecondaryMap as _ifSetSecondaryMap,
  unsetSecondaryMap as _ifUnsetSecondaryMap,
} from './infection-flow.js';

const SOURCE_ID            = 'cmp-buildings';
const LAYER_ID             = 'cmp-buildings-layer';
const OUTLINE_LAYER_ID     = 'cmp-buildings-outline';
const SECONDARY_3D_LAYER_ID = 'epicity-secondary';   // three.js on map B
// view.js reprojects every building polygon from equirect-at-centre to
// local Web Mercator coords on city load (so three.js draws aligned
// with the basemap). cmp-buildings GeoJSON wants the actual lat/lon —
// _toLngLat is the inverse of that Mercator reprojection.
const _EARTH_RADIUS_M      = 6371008.8;
const _EARTH_CIRC_M        = 2 * Math.PI * _EARTH_RADIUS_M;
const DIM_COLOR            = '#5b6068';   // matches view.js's grey-out tone

// Layer ids we create. Used to filter our own layers out of the
// "external style fingerprint" so paint-only mutations don't resync
// the secondary map's basemap.
const _OWN_LAYERS = new Set([
  LAYER_ID, OUTLINE_LAYER_ID, SECONDARY_3D_LAYER_ID,
]);

let _mapB           = null;
let _featureBase    = null;
let _layout         = null;
let _syncing        = false;
let _styleSyncing   = false;
let _styleSubscribed = false;
let _styledataTimer = null;
let _lastStyleFingerprint = null;
let _hoverIdxA      = null;
let _hoverIdxB      = null;
let _selectedIdx    = null;
let _hoveredIdx     = null;
let _onSelectCb     = null;
let _onHoverCb      = null;
let _interactionsBound = false;
let _lastCellsA = null, _lastCellsB = null;

// ── Coord helpers ───────────────────────────────────────────────────────────

function _toLngLat(wx, wz, centerLat, centerLon) {
  // (wx, wz) are mercator-relative metres at centre — see view.js's
  // _reprojectLayoutForMercator. Convert back to lat/lon by:
  //   1) scale → mercator delta from centre
  //   2) add centre mercator → mercator coord
  //   3) invert the normalised Web Mercator formula → lat/lon
  const PI      = Math.PI;
  const cosCLat = Math.cos(centerLat * PI / 180);
  if (cosCLat === 0) return [centerLon, centerLat];
  const cmx     = (180 + centerLon) / 360;
  const cmy     = (180 - 180 / PI * Math.log(Math.tan(PI / 4 + centerLat * PI / 360))) / 360;
  const scale   = 1 / (_EARTH_CIRC_M * cosCLat);
  const mx      = cmx + wx * scale;
  const my      = cmy + wz * scale;
  const lon     = mx * 360 - 180;
  const lat     = 360 / PI * (Math.atan(Math.exp((180 - my * 360) * PI / 180)) - PI / 4);
  return [lon, lat];
}

function _hexToCss(hex) {
  return typeof hex === 'number'
    ? '#' + hex.toString(16).padStart(6, '0')
    : '#888';
}

// ── GeoJSON build ───────────────────────────────────────────────────────────

function _buildBuildingsFeature(layout) {
  const { center_lat: cLat, center_lon: cLon, buildings } = layout;
  const features = [];
  for (const b of buildings) {
    if (!b.polygon || b.polygon.length < 3) continue;
    if (b.zone === 19 || b.zone === 20) continue;          // water + forest
    const ring = b.polygon.map(([wx, wz]) => _toLngLat(wx, wz, cLat, cLon));
    const [x0, y0] = ring[0];
    const [xn, yn] = ring[ring.length - 1];
    if (x0 !== xn || y0 !== yn) ring.push([x0, y0]);
    features.push({
      type: 'Feature',
      id:   b.idx,
      properties: {
        idx:    b.idx,
        zone:   b.zone,
        levels: b.levels || 1,
        pop:    b.pop || 0,
        income: b.income ?? null,
      },
      geometry: { type: 'Polygon', coordinates: [ring] },
    });
  }
  return { type: 'FeatureCollection', features };
}

// ── Maxes for normalisation (mirrors view.js's _normValue / _normHeight) ────

function _computeMaxes(cells) {
  let maxN=1,maxI=1,maxE=1,maxS=1,maxR=1,maxD=1;
  let totN=0,totI=0,totE=0,totS=0,totR=0,totD=0;
  if (cells) {
    for (let i = 0; i < cells.length; i++) {
      const c = cells[i];
      if (!c) continue;
      if (c.N > maxN) maxN = c.N;
      if (c.I > maxI) maxI = c.I;
      if ((c.E ?? 0) > maxE) maxE = c.E;
      if ((c.S ?? 0) > maxS) maxS = c.S;
      if ((c.R ?? 0) > maxR) maxR = c.R;
      if ((c.D ?? 0) > maxD) maxD = c.D;
      totN += c.N || 0; totI += c.I || 0;
      totE += c.E ?? 0; totS += c.S ?? 0;
      totR += c.R ?? 0; totD += c.D ?? 0;
    }
  }
  return {
    maxN, maxI, maxE, maxS, maxR, maxD,
    totN: Math.max(1, totN), totI: Math.max(1, totI),
    totE: Math.max(1, totE), totS: Math.max(1, totS),
    totR: Math.max(1, totR), totD: Math.max(1, totD),
  };
}

function _norm01(v, denom, log) {
  if (!denom || denom <= 0) return 0;
  if (log) {
    const l = Math.log10(1 + Math.max(0, v));
    const d = Math.log10(1 + denom);
    return d > 0 ? Math.max(0, Math.min(1, l / d)) : 0;
  }
  return Math.max(0, Math.min(1, v / denom));
}

function _denomFor(metric, normMode, nCap, maxes) {
  switch (normMode) {
    case 'capacity': return Math.max(1, nCap || 1);
    case 'total': {
      const k = ({ N:'totN',I:'totI',E:'totE',S:'totS',R:'totR',D:'totD' })[metric];
      return k ? maxes[k] : 1;
    }
    case 'cumulative':
    case 'live':
    default: {
      const k = ({ N:'maxN',I:'maxI',E:'maxE',S:'maxS',R:'maxR',D:'maxD' })[metric];
      return k ? maxes[k] : 1;
    }
  }
}

// ── Per-feature colour / height ─────────────────────────────────────────────

function _colorForCell(cell, props, env) {
  const { colorMode, colorScale, invert, log, normMode, maxes } = env;
  const N = cell?.N ?? 0;
  const zoneCss = _hexToCss(ZONE_COLORS[props.zone] ?? 0x888888);
  if (colorMode === 'zone' || colorMode === 'none') return zoneCss;

  const isResidential = (props.zone >= 1 && props.zone <= 3);
  if (colorMode === 'income') {
    if (!isResidential) return '#4d4d52';
    const lo = env.incomeLo, hi = env.incomeHi;
    const t = (typeof props.income === 'number' && hi > lo)
      ? (props.income - lo) / (hi - lo) : 0;
    return sampleScaleCSS(invert ? (1 - t) : t, colorScale);
  }

  if (!cell || N <= 0) return '#3a4150';

  let t;
  if (colorMode === 'prevalence') {
    t = N > 0 ? (cell.I / N) : 0;
    if (log) t = Math.log10(1 + 100 * t) / Math.log10(101);
  } else if (colorMode === 'population') {
    t = _norm01(N, _denomFor('N', normMode, N, maxes), log);
  } else {
    const m = ({ infection:'I', exposed:'E', susceptible:'S',
                 recovered:'R', deaths:'D' })[colorMode] ?? 'I';
    const v = cell[m] ?? 0;
    t = _norm01(v, _denomFor(m, normMode, N, maxes), log);
  }
  if (invert) t = 1 - t;
  return sampleScaleCSS(t, colorScale);
}

function _heightForCell(cell, props, env) {
  const { heightMode, log, normMode, maxes } = env;
  const baseH = Math.max(3, (props.levels || 1) * 3.5);
  const N = cell?.N ?? 0;
  if (!heightMode || heightMode === 'zone') return baseH;

  if (heightMode === 'income') {
    const isRes = (props.zone >= 1 && props.zone <= 3);
    if (!isRes) return 3;
    const lo = env.incomeLo, hi = env.incomeHi;
    const t = (typeof props.income === 'number' && hi > lo)
      ? (props.income - lo) / (hi - lo) : 0;
    return 3 + t * 70;
  }
  if (heightMode === 'prevalence') {
    const frac = N > 0 ? ((cell?.I ?? 0) / N) : 0;
    return 3 + frac * 90;
  }

  const k = ({ infection:'I', exposed:'E', susceptible:'S',
               recovered:'R', deaths:'D', population:'N' })[heightMode];
  if (!k) return baseH;
  const value = (k === 'N') ? N : (cell?.[k] ?? 0);
  const denom = _denomFor(k, normMode, N, maxes);
  const t = _norm01(value, denom, log);
  const ramp = ({ I:90, E:80, D:80, S:70, R:70, N:70 })[k] ?? 70;
  return 3 + t * ramp;
}

function _readLegendEnv(maxes, incomeLo, incomeHi) {
  return {
    colorMode:  getColorMode()  || 'infection',
    heightMode: getHeightMode() || 'zone',
    colorScale: getColorScale() || 'plasma',
    log:        !!getLogScale(),
    invert:     !!getInvertScale(),
    normMode:   getNormMode()   || 'live',
    maxes, incomeLo, incomeHi,
  };
}

let _incomeRange = null;
function _scanIncome(layout) {
  if (_incomeRange) return _incomeRange;
  let lo = Infinity, hi = 1;
  for (const b of layout.buildings) {
    if (b.zone < 1 || b.zone > 3) continue;
    if (typeof b.income !== 'number') continue;
    if (b.income < lo) lo = b.income;
    if (b.income > hi) hi = b.income;
  }
  if (!isFinite(lo)) lo = 0;
  _incomeRange = { lo, hi };
  return _incomeRange;
}

function _decorate(cells, env) {
  const out = new Array(_featureBase.features.length);
  for (let i = 0; i < _featureBase.features.length; i++) {
    const src  = _featureBase.features[i];
    const cell = cells ? cells[src.properties.idx] : null;
    out[i] = {
      type: 'Feature',
      id:   src.id,
      geometry: src.geometry,
      properties: {
        idx:    src.properties.idx,
        zone:   src.properties.zone,
        levels: src.properties.levels,
        color:  _colorForCell(cell, src.properties, env),
        height: _heightForCell(cell, src.properties, env),
      },
    };
  }
  return { type: 'FeatureCollection', features: out };
}

// ── Layers ──────────────────────────────────────────────────────────────────

// The primary fill colour expression respects the selection state: when a
// building is selected, every other building dims to grey so the picked
// one pops — same affordance as the single view's three.js path.
function _fillColorExpr(hasSelection) {
  return hasSelection ? [
    'case',
    ['boolean', ['feature-state', 'selected'], false], ['get', 'color'],
    DIM_COLOR,
  ] : ['get', 'color'];
}

// Outline opacity handles BOTH selection and hover states. Selection wins
// over hover when the same building is both. The hover outline is dimmer
// so the user can still tell which one is the click-selected building.
const _OUTLINE_OPACITY_EXPR = [
  'case',
  ['boolean', ['feature-state', 'selected'], false], 1,
  ['boolean', ['feature-state', 'hovered'],  false], 0.85,
  0,
];

/** No-op kept for ui.js back-compat. Nature visibility is now driven by
 *  three.js's setNatureDetailVisible — the same scene renders into both
 *  halves via a secondary three.js renderer attached to map B, so the
 *  Nature toggle automatically applies to both sides without us
 *  touching MapLibre fills. */
export function refreshNatureVisibility() { /* no-op — three.js handles it */ }
// Read at attach time when we still had MapLibre fill nature; preserved
// here as a hint the import is intentional even though we no longer
// gate visibility through MapLibre layout properties.
void isNatureDetailVisible;

function _attachLayer(map) {
  if (map.getSource(SOURCE_ID)) return;
  map.addSource(SOURCE_ID, {
    type: 'geojson',
    data: { type: 'FeatureCollection', features: [] },
    promoteId: 'idx',
  });
  // Initial fill-color expression already accounts for any active
  // selection — covers the resync path where a selection survives a
  // basemap rebuild.
  const hasSel = _selectedIdx !== null;
  map.addLayer({
    id:     LAYER_ID,
    type:   'fill-extrusion',
    source: SOURCE_ID,
    paint: {
      'fill-extrusion-color':   _fillColorExpr(hasSel),
      'fill-extrusion-height':  ['get', 'height'],
      'fill-extrusion-base':    0,
      'fill-extrusion-opacity': 0.95,
    },
  });
  // Yellow selection / hover outline. A single line layer covers both
  // states via _OUTLINE_OPACITY_EXPR — keeps the layer count stable so
  // we never hit setStyle for an interaction.
  map.addLayer({
    id:     OUTLINE_LAYER_ID,
    type:   'line',
    source: SOURCE_ID,
    paint: {
      'line-color':   '#facc15',
      'line-width':   2.5,
      'line-opacity': _OUTLINE_OPACITY_EXPR,
    },
  });
}

function _detachLayer(map) {
  if (!map) return;
  for (const id of [OUTLINE_LAYER_ID, LAYER_ID, SECONDARY_3D_LAYER_ID]) {
    if (map.getLayer(id)) map.removeLayer(id);
  }
  if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID);
}

function _updateSelectionPaint() {
  const hasSel = _selectedIdx !== null;
  const expr = _fillColorExpr(hasSel);
  // Suppress the styledata-driven resync for our own paint mutations —
  // a setPaintProperty triggers styledata even though no layer was added
  // or removed, and a full setStyle on map B is what the user perceives
  // as the "blink on every interaction".
  _styleSyncing = true;
  try {
    for (const m of [getMap(), _mapB]) {
      if (m?.getLayer(LAYER_ID)) {
        m.setPaintProperty(LAYER_ID, 'fill-extrusion-color', expr);
      }
    }
  } finally {
    // Release on the next tick — styledata is async.
    setTimeout(() => { _styleSyncing = false; }, 0);
  }
}

// ── Camera sync ─────────────────────────────────────────────────────────────

function _bindCameraSync(mapA, mapB) {
  const push = (src, dst) => () => {
    if (_syncing) return;
    _syncing = true;
    try {
      dst.jumpTo({
        center:  src.getCenter(),
        zoom:    src.getZoom(),
        bearing: src.getBearing(),
        pitch:   src.getPitch(),
      });
    } finally { _syncing = false; }
  };
  mapA.on('move', push(mapA, mapB));
  mapB.on('move', push(mapB, mapA));
}

// ── Style sync (admin / transport / roads etc.) ─────────────────────────────

function _styleWithoutInternals(style) {
  const sources = { ...style.sources };
  delete sources[SOURCE_ID];
  const layers = (style.layers || []).filter(
    l => l.id !== 'epidemic-buildings' && !_OWN_LAYERS.has(l.id),
  );
  return { ...style, sources, layers };
}

// Fingerprint the *external* style of map A — the layers we don't own.
// Using sorted IDs catches add/remove (transport toggle, neighborhoods
// toggle, etc.) but ignores paint/layout-only mutations. This is what
// stops the right-side blink when the user hovers or selects.
function _externalStyleFingerprint() {
  const map = getMap();
  if (!map) return '';
  const layers = map.getStyle()?.layers || [];
  const ids = [];
  for (const l of layers) {
    if (l.id === 'epidemic-buildings') continue;
    if (_OWN_LAYERS.has(l.id)) continue;
    // Layout 'visibility' counts: e.g. toggling neighborhoods off
    // should sync to map B even though the layer count is unchanged.
    const vis = (l.layout && l.layout.visibility) || 'visible';
    ids.push(`${l.id}:${vis}`);
  }
  ids.sort();
  return ids.join('|');
}

function _attachSecondary3DLayer() {
  if (!_mapB || _mapB.getLayer(SECONDARY_3D_LAYER_ID)) return;
  _mapB.addLayer({
    id:            SECONDARY_3D_LAYER_ID,
    type:          'custom',
    renderingMode: '3d',
    onAdd(_, gl) { attachSecondaryRenderer(gl); },
    render(_, matrix) {
      const mc = getCenterMercator();
      const sc = getMeterScale();
      if (!mc) return;
      _viewRenderScene(matrix, mc, sc, true);
      _mapB.triggerRepaint();
    },
  });
}

function _resyncMapBStyle() {
  if (!_mapB || _styleSyncing) return;
  const mapA = getMap();
  if (!mapA) return;
  _styleSyncing = true;
  try {
    const cleaned = _styleWithoutInternals(mapA.getStyle());
    _mapB.setStyle(cleaned, { diff: true });
    _mapB.once('styledata', () => {
      // The secondary three.js layer renders nature (3D trees, animated
      // dashed lakes) on map B. setStyle wipes custom layers — re-add
      // before cmp-buildings so cmp sits on top.
      _attachSecondary3DLayer();
      _attachLayer(_mapB);
      // If a selection / hover survived the resync, re-apply its
      // feature-state.
      if (_selectedIdx !== null) {
        _mapB.setFeatureState({ source: SOURCE_ID, id: _selectedIdx }, { selected: true });
      }
      if (_hoveredIdx !== null) {
        _mapB.setFeatureState({ source: SOURCE_ID, id: _hoveredIdx }, { hovered: true });
      }
      _replayLastApplied();
      _styleSyncing = false;
    });
  } catch (err) {
    _styleSyncing = false;
    console.warn('[compare] style sync failed:', err);
  }
}

function _subscribeStyleEvents() {
  if (_styleSubscribed) return;
  const mapA = getMap();
  if (!mapA) return;
  _lastStyleFingerprint = _externalStyleFingerprint();
  mapA.on('styledata', () => {
    if (!_mapB || _styleSyncing) return;
    const fp = _externalStyleFingerprint();
    if (fp === _lastStyleFingerprint) return;   // paint-only change → skip
    _lastStyleFingerprint = fp;
    if (_styledataTimer) clearTimeout(_styledataTimer);
    _styledataTimer = setTimeout(() => {
      _styledataTimer = null;
      _resyncMapBStyle();
    }, 120);
  });
  _styleSubscribed = true;
}

// ── Apply / replay ──────────────────────────────────────────────────────────

function _replayLastApplied() {
  if (_lastCellsA && _lastCellsB) applyDays(_lastCellsA, _lastCellsB);
}

export function applyDays(cellsA, cellsB) {
  const mapA = getMap();
  if (!_featureBase || !mapA) return;
  _lastCellsA = cellsA; _lastCellsB = cellsB;
  // Per-side maxes so dragging Day B's slider can't rescale Day A's
  // building heights / colours (and vice-versa). Each half normalises
  // against its own day's distribution — what the user expects when
  // they slide one handle and only that side should change.
  const inc = _scanIncome(_layout);
  const envA = _readLegendEnv(_computeMaxes(cellsA), inc.lo, inc.hi);
  const envB = _readLegendEnv(_computeMaxes(cellsB), inc.lo, inc.hi);

  if (mapA.getSource(SOURCE_ID)) {
    mapA.getSource(SOURCE_ID).setData(_decorate(cellsA, envA));
  }
  if (_mapB?.getSource(SOURCE_ID)) {
    _mapB.getSource(SOURCE_ID).setData(_decorate(cellsB, envB));
  }
}

// ── Selection + hover (delegates to ui.js callbacks) ────────────────────────

function _setFeatureStateBoth(idx, state) {
  for (const m of [getMap(), _mapB]) {
    if (m?.getSource(SOURCE_ID)) {
      m.setFeatureState({ source: SOURCE_ID, id: idx }, state);
    }
  }
}

export function setSelectedIdx(idx) {
  if (_selectedIdx !== null) {
    _setFeatureStateBoth(_selectedIdx, { selected: false });
  }
  _selectedIdx = (idx === null || idx === undefined) ? null : idx;
  if (_selectedIdx !== null) {
    _setFeatureStateBoth(_selectedIdx, { selected: true });
  }
  _updateSelectionPaint();
}

/** Hover highlight mirrored across both halves. Setting feature-state is
 *  cheap and does NOT trigger styledata, so this never blinks. */
export function setHoveredIdx(idx) {
  const next = (idx === null || idx === undefined) ? null : idx;
  if (next === _hoveredIdx) return;
  if (_hoveredIdx !== null) _setFeatureStateBoth(_hoveredIdx, { hovered: false });
  _hoveredIdx = next;
  if (_hoveredIdx !== null) _setFeatureStateBoth(_hoveredIdx, { hovered: true });
}

function _bindInteractions(map, which) {
  // Click → select. A bare-canvas click clears. When the admin-area
  // overlay is active (same single-view rule), buildings are NOT
  // pickable — clicks fall through to the nbh-fill layer's own click
  // handler in map.js, which selects the area instead.
  map.on('click', LAYER_ID, (e) => {
    if (isAreasActive()) return;
    const idx = e.features?.[0]?.properties?.idx;
    if (idx === undefined) return;
    setSelectedIdx(idx);
    if (typeof _onSelectCb === 'function') _onSelectCb(idx);
  });
  map.on('click', (e) => {
    if (isAreasActive()) return;
    const hits = map.queryRenderedFeatures(e.point, { layers: [LAYER_ID] });
    if (hits.length === 0) {
      setSelectedIdx(null);
      if (typeof _onSelectCb === 'function') _onSelectCb(null);
    }
  });

  // Hover: route through ui.js's _onHover so the rich tooltip (name,
  // neighborhood, income, gender, household, SEIR) is the same as the
  // single view. ALSO mirror a hovered-feature outline to BOTH maps so
  // the user can spot the same building on both halves at a glance.
  // When the admin-area overlay is on the building hover is suppressed
  // — same affordance as the single view, so the area popup isn't
  // fighting for cursor focus with the building tooltip.
  map.on('mousemove', LAYER_ID, (e) => {
    if (isAreasActive()) {
      // Make sure no stale yellow outline lingers from a pre-areas hover.
      if (which === 'a') _hoverIdxA = null; else _hoverIdxB = null;
      if (_hoverIdxA === null && _hoverIdxB === null) {
        setHoveredIdx(null);
        if (typeof _onHoverCb === 'function') _onHoverCb(null);
      }
      return;
    }
    const idx = e.features?.[0]?.properties?.idx;
    if (idx === undefined) return;
    map.getCanvas().style.cursor = 'pointer';
    if (which === 'a') _hoverIdxA = idx; else _hoverIdxB = idx;
    setHoveredIdx(idx);
    if (typeof _onHoverCb === 'function') _onHoverCb(idx);
  });
  map.on('mouseleave', LAYER_ID, () => {
    map.getCanvas().style.cursor = '';
    if (which === 'a') _hoverIdxA = null; else _hoverIdxB = null;
    if (_hoverIdxA === null && _hoverIdxB === null) {
      setHoveredIdx(null);
      if (typeof _onHoverCb === 'function') _onHoverCb(null);
    }
  });
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

export async function enableSplit(layout, cellsA, cellsB, opts = {}) {
  const mapA = getMap();
  if (!mapA) return false;
  _layout      = layout;
  _onSelectCb  = opts.onSelect || null;
  _onHoverCb   = opts.onHover  || null;
  if (!_featureBase) _featureBase = _buildBuildingsFeature(layout);

  document.body.classList.add('compare-split');
  await new Promise(r => requestAnimationFrame(r));
  mapA.resize();

  if (!_mapB) {
    const containerB = document.getElementById('map-container-b');
    containerB.hidden = false;
    const cleaned = _styleWithoutInternals(mapA.getStyle());
    _mapB = new maplibregl.Map({
      container: containerB,
      style: cleaned,
      center:  mapA.getCenter(),
      zoom:    mapA.getZoom(),
      bearing: mapA.getBearing(),
      pitch:   mapA.getPitch(),
      antialias: true,
      attributionControl: false,
    });
    await new Promise(r => _mapB.once('load', r));
    _bindCameraSync(mapA, _mapB);
    _subscribeStyleEvents();

    // Mirror the primary map's resize plumbing on map B so the
    // secondary three.js renderer's viewport + LineMaterials stay in
    // sync after window resizes — same fix as map.js for the primary.
    _mapB.on('resize', () => {
      const canvas = _mapB.getCanvas();
      if (canvas) {
        notifyRendererResize(true, canvas.clientWidth, canvas.clientHeight);
      }
      refreshLineMaterials();
      _mapB.triggerRepaint();
    });
  }

  // Hide three.js's regular building/POI/landmark meshes (cmp-buildings
  // overlays them with day-specific colours instead). Nature stays
  // visible so 3D trees + animated dashed lakes show on BOTH halves.
  setSplitMode(true);

  // Add a secondary three.js custom layer to map B so the same scene
  // (nature) renders into the right half too. Must be added before the
  // cmp-buildings fill-extrusion so cmp sits on top of trees + water.
  _attachSecondary3DLayer();

  _attachLayer(mapA);
  _attachLayer(_mapB);
  if (!_interactionsBound) {
    _bindInteractions(mapA,  'a');
    _bindInteractions(_mapB, 'b');
    _interactionsBound = true;
  }
  applyDays(cellsA, cellsB);

  // Wire infection-flow's secondary projection to Map B so chains render
  // into the right half too (it walks the existing chain twice — once
  // per map's project()).
  _ifSetSecondaryMap(_mapB);

  // Mirror admin-area (neighborhood) selection and hover across both
  // halves. map.js owns the nbh state and broadcasts feature-state
  // changes to every registered map.
  addNbhSyncMap(_mapB);
  return true;
}

export function disableSplit() {
  document.body.classList.remove('compare-split');
  setHoveredIdx(null);
  setSelectedIdx(null);
  _ifUnsetSecondaryMap();
  if (_mapB) removeNbhSyncMap(_mapB);
  const mapA = getMap();
  if (mapA) {
    _detachLayer(mapA);
    requestAnimationFrame(() => mapA.resize());
  }
  if (_mapB) {
    // Drop the secondary renderer's GL caches before tearing down the
    // map so we don't leak references to a freed WebGL context.
    detachSecondaryRenderer();
    _mapB.remove();
    _mapB = null;
  } else {
    detachSecondaryRenderer();
  }
  // Restore three.js's regular meshes (buildings, landmarks, POIs).
  setSplitMode(false);
  if (_styledataTimer) {
    clearTimeout(_styledataTimer);
    _styledataTimer = null;
  }
  _interactionsBound = false;
  const containerB = document.getElementById('map-container-b');
  if (containerB) containerB.hidden = true;
  _lastCellsA = null;
  _lastCellsB = null;
}

export function getMapB()       { return _mapB; }
export function isSplitActive() { return _mapB !== null; }
