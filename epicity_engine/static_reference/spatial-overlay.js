/**
 * spatial-overlay.js — Map visuals for active spatial interventions.
 *
 *   1. Animated red dashed borders around every quarantine_zone (whether
 *      neighborhood-click or freehand lasso). MapLibre line layer with
 *      its `line-dasharray` mutated on a ~6 Hz tick for "marching ants".
 *   2. A 2D maplibre marker per targeted_vaccine (💉) and surge_testing
 *      (🔬), with a pulsing ring so it reads at any zoom. Mobile clinics
 *      are drawn as real 3D meshes by ambulance3d.js — not here.
 */

import { getMap } from './map.js';

let _map = null;
let _bordersAdded = false;
let _dashTimer = null;
let _dashIndex = 0;

const DASH_FRAMES = [
  [3, 2], [2.6, 2.4], [2.2, 2.8], [1.8, 3.2],
  [1.4, 3.6], [1.0, 4.0], [1.4, 3.6], [1.8, 3.2],
  [2.2, 2.8], [2.6, 2.4],
];

const _markers   = new Map();    // id → { marker, type } — for vaccine + surge
let _cityLayout = null;          // captured from ui.js for road snap
let _lastState  = null;

/**
 * Wire the overlay. Called once per city entry. Subsequent calls are
 * no-ops if the layer is already mounted.
 */
export function ensureOverlay() {
  _map = getMap();
  if (!_map) return;
  if (!_map.getSource('sp-q-src')) {
    _map.addSource('sp-q-src', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    });
  }
  if (!_map.getLayer('sp-q-fill')) {
    _map.addLayer({
      id: 'sp-q-fill', type: 'fill', source: 'sp-q-src',
      paint: { 'fill-color': '#ef4444', 'fill-opacity': 0.05 },
    });
  }
  if (!_map.getLayer('sp-q-border')) {
    _map.addLayer({
      id: 'sp-q-border', type: 'line', source: 'sp-q-src',
      paint: {
        'line-color': '#ef4444',
        'line-width': 3,
        'line-dasharray': DASH_FRAMES[0],
        'line-opacity': 0.95,
      },
    });
  }
  _bordersAdded = true;
  if (_dashTimer == null) {
    // ~6 Hz dash cycle — the eye reads it as motion without burning CPU.
    _dashTimer = setInterval(() => {
      if (!_map || !_map.getLayer('sp-q-border')) return;
      _dashIndex = (_dashIndex + 1) % DASH_FRAMES.length;
      try {
        _map.setPaintProperty('sp-q-border', 'line-dasharray',
                              DASH_FRAMES[_dashIndex]);
      } catch (_) {}
    }, 160);
  }
}

/** Capture the city layout once it's loaded so we can road-snap ambulances. */
export function setCityLayout(layout) {
  _cityLayout = layout || null;
}

/** Update _lastState; ambulance step uses building idx to find hot buildings. */
export function setLatestState(state) {
  _lastState = state || null;
}

/** Repaint borders + ambulances from the latest spatial-items list. */
export function applyItems(items) {
  ensureOverlay();
  if (!_map) return;
  items = Array.isArray(items) ? items : [];

  // ── Quarantine borders ────────────────────────────────────────────
  // Polygon shape is a list of rings (each ring = [[lon,lat], ...]).
  // Lasso submits a single ring (wrapped server-side); neighborhood
  // quarantines submit one ring per polygon piece — both arrive here
  // as the same shape.
  const qPolys = [];
  for (const it of items) {
    if (it.type !== 'quarantine_zone' || !Array.isArray(it.polygon)) continue;
    for (const ring of it.polygon) {
      if (!Array.isArray(ring) || ring.length < 3) continue;
      qPolys.push({
        type: 'Feature',
        properties: { id: it.id },
        geometry: {
          type: 'Polygon',
          coordinates: [[...ring, ring[0]]],
        },
      });
    }
  }
  const src = _map.getSource('sp-q-src');
  if (src) src.setData({ type: 'FeatureCollection', features: qPolys });
  // Promote the quarantine layers to the top of the stack — the lasso
  // drawing layers are added later and otherwise sit on top, which
  // could mask the freshly-set quarantine border on the first submit.
  // Then trigger an immediate repaint so the new polygon renders this
  // frame instead of waiting for the next dash-animation tick.
  try {
    if (_map.getLayer('sp-q-fill'))   _map.moveLayer('sp-q-fill');
    if (_map.getLayer('sp-q-border')) _map.moveLayer('sp-q-border');
  } catch (_) {}
  try { _map.triggerRepaint(); } catch (_) {}

  // Mobile-clinic ambulances are real 3D meshes (ambulance3d.js).
  // Vaccine-drive + surge-testing get a 2D maplibre marker so the
  // user can see at a glance where their effect is concentrated.
  const liveIds = new Set();
  for (const it of items) {
    if (it.type === 'targeted_vaccine' || it.type === 'surge_testing') {
      liveIds.add(it.id);
      if (!_markers.has(it.id)) _spawnMarker(it);
    }
  }
  for (const id of [..._markers.keys()]) {
    if (!liveIds.has(id)) {
      const m = _markers.get(id);
      try { m.marker?.remove(); } catch (_) {}
      _markers.delete(id);
    }
  }
}

/** Stop animation + clear visuals. Called on city leave / reset. */
export function teardown() {
  if (_dashTimer != null) { clearInterval(_dashTimer); _dashTimer = null; }
  for (const m of _markers.values()) {
    try { m.marker?.remove(); } catch (_) {}
  }
  _markers.clear();
  if (_map) {
    const src = _map.getSource('sp-q-src');
    if (src) src.setData({ type: 'FeatureCollection', features: [] });
  }
  _bordersAdded = false;
}

/* ── Vaccine + surge map markers ─────────────────────────────────────── */

const MARKER_TEMPLATES = {
  targeted_vaccine: {
    cls: 'sp-marker sp-marker-vaccine',
    body: '💉',
  },
  surge_testing: {
    cls: 'sp-marker sp-marker-surge',
    body: '🔬',
  },
};

function _spawnMarker(item) {
  if (!window.maplibregl) return;
  const tpl = MARKER_TEMPLATES[item.type];
  if (!tpl) return;
  const lonlat = item.center_lonlat || _polygonCentroid(item.polygon);
  if (!lonlat) return;
  const el = document.createElement('div');
  el.className = tpl.cls;
  el.innerHTML = `
    <div class="sp-marker-body">${tpl.body}</div>
    <div class="sp-marker-pulse"></div>`;
  const marker = new window.maplibregl.Marker({ element: el, anchor: 'center' })
    .setLngLat(lonlat)
    .addTo(_map);
  _markers.set(item.id, { id: item.id, marker, type: item.type });
}

function _polygonCentroid(poly) {
  if (!Array.isArray(poly) || poly.length === 0) return null;
  // `poly` is a list of rings (new shape). Average all points across
  // every ring — adequate for a marker anchor on a multi-piece area.
  let sx = 0, sy = 0, n = 0;
  for (const ring of poly) {
    if (!Array.isArray(ring)) continue;
    for (const p of ring) {
      if (!Array.isArray(p) || p.length < 2) continue;
      sx += p[0]; sy += p[1]; n++;
    }
  }
  return n > 0 ? [sx / n, sy / n] : null;
}

