/**
 * spatial-tools.js — Map-driven intervention tools (game + sandbox).
 *
 * Five tools, each with its own selection mode:
 *
 *   quarantine_zone   click a neighborhood polygon
 *   lasso_quarantine  click vertices, double-click to close
 *   targeted_vaccine  click a neighborhood polygon
 *   mobile_clinic     click any point on the map (500 m radius)
 *   surge_testing     click any point on the map (it + 8 nearest neighbours)
 *
 * UX flow:
 *   1. User picks a tool from the palette → cursor changes, banner appears.
 *   2. Map clicks are routed through this module's handler.
 *   3. After selection, POST /api/spatial.
 *   4. Active items render in the palette with × to dismiss.
 *
 * Sandbox + game both supported. Game mode bills resource costs through
 * the SpatialRegistry's daily_costs() path on the backend.
 */

import { getMap, toggleNeighborhoods, isAreasActive, clearNeighborhoodSelection } from './map.js';
import { applyItems as applyOverlayItems, ensureOverlay } from './spatial-overlay.js';
import * as ambulance3d from './ambulance3d.js';
import { t, onLangChange } from './i18n.js';

let _palette = null;
let _banner  = null;
let _activeTool = null;           // "quarantine_zone" | ... | null
let _lassoVertices = [];          // [[lon, lat], ...]
let _lassoMarkers  = [];          // maplibre Marker instances for vertices
let _lassoPolygonLayerAdded = false;
let _items = [];                  // last-known list of active spatial items

const TOOL_DEFS = () => [
  {
    id:    'quarantine_zone',
    label: t('sp_quarantine_zone'),
    ico:   '🚧',
    desc:  t('sp_quarantine_zone_desc'),
    select: 'neighborhood',
    hint:  t('sp_quarantine_zone_hint'),
  },
  {
    id:    'lasso_quarantine',
    label: t('sp_lasso'),
    ico:   '✏️',
    desc:  t('sp_lasso_desc'),
    select: 'lasso',
    hint:  t('sp_lasso_hint'),
  },
  {
    id:    'targeted_vaccine',
    label: t('sp_targeted_vaccine'),
    ico:   '🎯',
    desc:  t('sp_targeted_vaccine_desc'),
    select: 'neighborhood',
    hint:  t('sp_targeted_vaccine_hint'),
  },
  {
    id:    'mobile_clinic',
    label: t('sp_mobile_clinic'),
    ico:   '🚑',
    desc:  t('sp_mobile_clinic_desc'),
    select: 'point',
    hint:  t('sp_mobile_clinic_hint'),
  },
  {
    id:    'surge_testing',
    label: t('sp_surge_testing'),
    ico:   '🔬',
    desc:  t('sp_surge_testing_desc'),
    select: 'point',
    hint:  t('sp_surge_testing_hint'),
  },
];

const _tool = (id) => TOOL_DEFS().find(t => t.id === id);
const TOOL_BY_ID = new Proxy({}, { get: (_, id) => _tool(String(id)) });

let _langSubscribedSpatial = false;

/** Mount the palette into the sidebar's iv section. */
export function initSpatialTools() {
  if (_palette && document.body.contains(_palette)) return;
  const ivBody = document.getElementById('ivBody');
  if (!ivBody) return;
  // On language switch, drop the palette so the next initSpatialTools
  // (or refreshSpatial) call rebuilds the labels from the new strings.
  if (!_langSubscribedSpatial) {
    _langSubscribedSpatial = true;
    onLangChange(() => {
      if (_palette && _palette.parentNode) _palette.parentNode.removeChild(_palette);
      _palette = null;
      initSpatialTools();   // remount with new language
    });
  }
  // Insert palette wrapper *before* the citywide iv list so it visually
  // sits at the top of the Interventions section.
  _palette = document.createElement('div');
  _palette.id = 'spatialPalette';
  _palette.innerHTML = `
    <div class="sp-pal-h">${t('sp_pal_h')}</div>
    <div class="sp-pal-grid"></div>
    <div class="sp-pal-active-h" hidden>${t('sp_pal_active_h')}</div>
    <div class="sp-pal-active"></div>
  `;
  ivBody.insertBefore(_palette, ivBody.firstChild);

  const grid = _palette.querySelector('.sp-pal-grid');
  for (const def of TOOL_DEFS()) {
    const btn = document.createElement('button');
    btn.className = 'sp-tool';
    btn.dataset.tool = def.id;
    btn.title = def.desc;
    btn.innerHTML = `
      <span class="sp-tool-ico">${def.ico}</span>
      <span class="sp-tool-lbl">${def.label}</span>
    `;
    btn.addEventListener('click', () => _activate(def.id));
    grid.appendChild(btn);
  }

  _ensureBanner();
  _attachMapHandlers();
  refreshSpatial();
}

/** Re-fetch active items list and repaint the palette + active list. */
export async function refreshSpatial() {
  try {
    const r = await fetch('/api/spatial');
    if (!r.ok) return;
    const j = await r.json();
    _items = j.items || [];
  } catch (_) { return; }
  _renderActiveList();
  // Repaint quarantine borders + spawn/dismiss ambulances based on the
  // fresh items list. The maplibre overlay handles quarantine borders;
  // the 3D ambulance walks the road network in the THREE scene.
  ensureOverlay();
  applyOverlayItems(_items);
  ambulance3d.syncFromItems(_items);
}

/**
 * Refresh the disabled state of tool buttons based on the latest engine
 * state. Currently only the targeted-vaccine drive: it requires the
 * citywide Vaccination intervention to be ON, and (in game mode) the
 * vaccine to have rolled out.
 */
export function applyToolGating(state) {
  if (!_palette) return;
  const vaccBtn = _palette.querySelector('[data-tool="targeted_vaccine"]');
  if (!vaccBtn) return;
  const vaxOn       = (state?.active_interventions || []).includes('vaccination');
  const vaxUnlocked = state?.game ? state.game.vaccine_unlocked : true;
  const ok = vaxOn && vaxUnlocked;
  vaccBtn.classList.toggle('sp-tool-disabled', !ok);
  vaccBtn.title = ok
    ? t('sp_targeted_vaccine_desc')
    : (!vaxUnlocked
        ? t('sp_locked_vacc_day', { n: state?.game?.days_until_vaccine ?? '?' })
        : t('sp_locked_vacc_iv'));
  // If the user clicks while disabled, _activate bails (see below).
}

/* ── Activation / cancellation ─────────────────────────────────────────── */

function _activate(toolId) {
  // Disabled tools (e.g. targeted_vaccine before vaccine rollout) are
  // marked via the .sp-tool-disabled class — bail with a quick banner.
  const btn = _palette?.querySelector(`[data-tool="${toolId}"]`);
  if (btn?.classList.contains('sp-tool-disabled')) {
    _flashBanner(btn.title || 'This tool isn’t available yet.');
    return;
  }
  if (_activeTool === toolId) {
    _cancel();
    return;
  }
  _cancel();   // clear any prior tool state
  _activeTool = toolId;
  const t = TOOL_BY_ID[toolId];
  document.body.classList.add('spatial-selecting');
  document.body.dataset.spatialTool = toolId;
  _palette.querySelectorAll('.sp-tool').forEach(b => {
    b.classList.toggle('on', b.dataset.tool === toolId);
  });
  _showBanner(`${t.ico} ${t.label} — ${t.hint}`);
  // Auto-show neighborhoods when a polygon-targeted tool is picked
  if (t.select === 'neighborhood' && !isAreasActive()) {
    toggleNeighborhoods();
  }
  if (t.select === 'lasso') {
    _lassoVertices = [];
    _clearLassoMarkers();
    _ensureLassoLayer();
    // Suspend map panning so each click adds a vertex cleanly. Zoom
    // (scroll / pinch) is left enabled so the user can zoom in for
    // precision while drawing.
    const map = getMap();
    if (map) {
      try { map.dragPan.disable(); }   catch (_) {}
      try { map.dragRotate.disable(); } catch (_) {}
      try { map.touchZoomRotate.disable(); } catch (_) {}
    }
  }
}

function _cancel() {
  _activeTool = null;
  document.body.classList.remove('spatial-selecting');
  delete document.body.dataset.spatialTool;
  _palette?.querySelectorAll('.sp-tool').forEach(b => b.classList.remove('on'));
  _hideBanner();
  _clearLassoMarkers();
  _clearLassoLayer();
  _lassoVertices = [];
  // Restore map interactions in case lasso was active.
  const map = getMap();
  if (map) {
    try { map.dragPan.enable(); }   catch (_) {}
    try { map.dragRotate.enable(); } catch (_) {}
    try { map.touchZoomRotate.enable(); } catch (_) {}
  }
}

/* ── Map click routing ─────────────────────────────────────────────────── */

function _attachMapHandlers() {
  const map = getMap();
  if (!map) {
    // map.js may not have init'd yet on first city entry — retry shortly.
    setTimeout(_attachMapHandlers, 250);
    return;
  }
  map.on('click', _onMapClick);
  map.on('mousedown', _onMapDown);
  map.on('mousemove', _onMapMove);
  map.on('mouseup',   _onMapUp);
  document.addEventListener('keydown', (e) => {
    if (!_activeTool) return;
    if (e.key === 'Escape') _cancel();
  });
}

function _onMapClick(e) {
  if (!_activeTool) return;
  const t = TOOL_BY_ID[_activeTool];
  if (!t) return;

  // Lasso uses mousedown/move/up, not click — bail.
  if (t.select === 'lasso') return;
  e.preventDefault?.();

  if (t.select === 'neighborhood') {
    const map = getMap();
    const hits = map.queryRenderedFeatures(e.point, { layers: ['nbh-fill'] });
    if (!hits.length) {
      _flashBanner(t('sp_no_neighborhood'));
      return;
    }
    const name = hits[0].properties?.name;
    if (!name) return;
    const submitType = t.id === 'targeted_vaccine'
      ? 'targeted_vaccine'
      : 'quarantine_zone';
    _submit({ type: submitType, neighborhood: name });
    _cancel();
  } else if (t.select === 'point') {
    const submitType = t.id === 'mobile_clinic' ? 'mobile_clinic' : 'surge_testing';
    const body = { type: submitType, center_lonlat: [e.lngLat.lng, e.lngLat.lat] };
    if (submitType === 'mobile_clinic') body.radius_m = 500;
    _submit(body);
    _cancel();
  }
}

/* ── Lasso freehand drag ─────────────────────────────────────────────── */

let _lassoDrawing = false;
const _LASSO_MIN_M = 25;   // sampling resolution along the drag

function _onMapDown(e) {
  if (_activeTool !== 'lasso_quarantine') return;
  e.preventDefault?.();
  _lassoDrawing = true;
  _lassoVertices = [[e.lngLat.lng, e.lngLat.lat]];
  _redrawLasso();
}

function _onMapMove(e) {
  if (!_lassoDrawing) return;
  const last = _lassoVertices[_lassoVertices.length - 1];
  const dlon = (e.lngLat.lng - last[0]) * 111000
             * Math.cos((e.lngLat.lat + last[1]) / 2 * Math.PI / 180);
  const dlat = (e.lngLat.lat - last[1]) * 111000;
  if (Math.hypot(dlon, dlat) < _LASSO_MIN_M) return;
  _lassoVertices.push([e.lngLat.lng, e.lngLat.lat]);
  _redrawLasso();
}

function _onMapUp(e) {
  if (!_lassoDrawing) return;
  _lassoDrawing = false;
  // Need at least a 3-vertex closed polygon to make sense.
  if (_lassoVertices.length < 3) {
    _flashBanner(t('sp_lasso_too_short'));
    _lassoVertices = [];
    _redrawLasso();
    return;
  }
  _closeLasso();
}

/* ── Lasso drawing ─────────────────────────────────────────────────────── */

function _ensureLassoLayer() {
  const map = getMap();
  if (!map || _lassoPolygonLayerAdded) return;
  if (!map.getSource('sp-lasso-src')) {
    map.addSource('sp-lasso-src', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    });
  }
  if (!map.getLayer('sp-lasso-line')) {
    map.addLayer({
      id: 'sp-lasso-line', type: 'line', source: 'sp-lasso-src',
      paint: {
        'line-color': '#f87171',
        'line-width': 2.5,
        'line-dasharray': [2, 1.5],
      },
    });
  }
  if (!map.getLayer('sp-lasso-fill')) {
    map.addLayer({
      id: 'sp-lasso-fill', type: 'fill', source: 'sp-lasso-src',
      paint: {
        'fill-color': '#f87171',
        'fill-opacity': 0.08,
      },
    }, 'sp-lasso-line');
  }
  _lassoPolygonLayerAdded = true;
}

function _redrawLasso() {
  const map = getMap();
  if (!map || !_lassoPolygonLayerAdded) return;
  const src = map.getSource('sp-lasso-src');
  if (!src) return;
  const verts = _lassoVertices;
  if (verts.length < 2) {
    src.setData({ type: 'FeatureCollection', features: [] });
    return;
  }
  // Render the open chain as a LineString while drawing; once we have
  // 3+ verts, also show the closed polygon.
  const features = [];
  if (verts.length >= 3) {
    features.push({
      type: 'Feature',
      geometry: { type: 'Polygon', coordinates: [[...verts, verts[0]]] },
    });
  } else {
    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: verts },
    });
  }
  src.setData({ type: 'FeatureCollection', features });
}

function _closeLasso() {
  if (_lassoVertices.length < 3) {
    _flashBanner('Need at least 3 vertices.');
    return;
  }
  const polygon = [..._lassoVertices];
  _submit({ type: 'quarantine_zone', polygon });
  _cancel();
}

function _clearLassoMarkers() {
  for (const m of _lassoMarkers) try { m.remove(); } catch (_) {}
  _lassoMarkers = [];
}

function _clearLassoLayer() {
  const map = getMap();
  if (!map) return;
  const src = map.getSource('sp-lasso-src');
  if (src) src.setData({ type: 'FeatureCollection', features: [] });
}

/* ── Submission + active list ──────────────────────────────────────────── */

async function _submit(body) {
  // Optimistic insert — paint the polygon / marker IMMEDIATELY so the
  // user sees the red border / ambulance the same frame they release
  // the lasso. Backend Shapely hit-test can take ~1 s on a big polygon,
  // and waiting for it makes the UI feel sluggish.
  const optimistic = _buildOptimisticItem(body);
  if (optimistic) {
    // Each optimistic entry has its own random id (`__opt_xxx`), so
    // multiple in-flight submissions coexist cleanly. The previous
    // `_items.filter(it => !it.__optimistic)` was wiping every prior
    // pending entry — which made adding a second mobile clinic
    // appear to replace the first one until the server response
    // landed.
    _items.push(optimistic);
    _renderActiveList();
    applyOverlayItems(_items);
    if (optimistic.type === 'mobile_clinic') {
      ambulance3d.syncFromItems(_items);
      // Same camera + selection treatment as clicking the row in
      // Active measures — fly tight to the new ambulance and select
      // it so the yellow ring + follow-tooltip appear right away.
      _flyToItem(optimistic);
      try { ambulance3d.setSelected(optimistic.id); } catch (_) {}
    }
    if (optimistic.type === 'quarantine_zone') {
      try { clearNeighborhoodSelection(); } catch (_) {}
      if (isAreasActive()) {
        try { toggleNeighborhoods(); } catch (_) {}
        document.getElementById('btnNeighborhoods')?.classList.remove('active');
      }
    }
  }

  try {
    const r = await fetch('/api/spatial', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      _flashBanner(err.detail || `Failed (${r.status})`);
      _rollbackOptimistic();
      return;
    }
    await refreshSpatial();   // canonical sync — drops the optimistic entry
  } catch (e) {
    _flashBanner('Network error — try again.');
    _rollbackOptimistic();
  }
}

function _rollbackOptimistic() {
  _items = _items.filter(it => !it.__optimistic);
  _renderActiveList();
  applyOverlayItems(_items);
  ambulance3d.syncFromItems(_items);
}

function _buildOptimisticItem(body) {
  const id = '__opt_' + Math.random().toString(36).slice(2, 9);
  const base = {
    id, __optimistic: true,
    buildings: 0, population: 0, created_day: 0,
    duration_days: -1, expires_on: null, days_remaining: null,
  };
  if (body.type === 'quarantine_zone' && Array.isArray(body.polygon)) {
    return { ...base, type: 'quarantine_zone',
             label_key: 'sp_lbl_lasso', label_vars: { n: '…' },
             label: 'Lasso zone · pending',
             polygon: [body.polygon] };
  }
  if (body.type === 'quarantine_zone' && body.neighborhood) {
    return { ...base, type: 'quarantine_zone',
             label_key: 'sp_lbl_qua_nbh', label_vars: { name: body.neighborhood },
             label: `Quarantine · ${body.neighborhood}` };
  }
  if (body.type === 'targeted_vaccine' && body.neighborhood) {
    return { ...base, type: 'targeted_vaccine',
             label_key: 'sp_lbl_vac_nbh', label_vars: { name: body.neighborhood },
             label: `Vaccine drive · ${body.neighborhood}` };
  }
  if (body.type === 'mobile_clinic' && Array.isArray(body.center_lonlat)) {
    const r = body.radius_m || 500;
    return { ...base, type: 'mobile_clinic',
             label_key: 'sp_lbl_clinic', label_vars: { radius: Math.round(r) },
             label: `Mobile clinic · ${Math.round(r)} m`,
             center_lonlat: [...body.center_lonlat],
             duration_days: 21 };
  }
  if (body.type === 'surge_testing' && Array.isArray(body.center_lonlat)) {
    return { ...base, type: 'surge_testing',
             label_key: 'sp_lbl_surge', label_vars: { n: '…' },
             label: 'Surge testing · pending',
             center_lonlat: [...body.center_lonlat],
             duration_days: 30 };
  }
  return null;
}

async function _remove(id) {
  try {
    await fetch(`/api/spatial/${id}`, { method: 'DELETE' });
    await refreshSpatial();
  } catch (_) {}
}

function _renderActiveList() {
  if (!_palette) return;
  const head = _palette.querySelector('.sp-pal-active-h');
  const list = _palette.querySelector('.sp-pal-active');
  list.innerHTML = '';
  if (!_items.length) {
    head.hidden = true;
    return;
  }
  head.hidden = false;
  for (const it of _items) {
    const row = document.createElement('div');
    row.className = 'sp-pal-row';
    row.title = 'Click to fly the camera here';
    // i18n: prefer label_key+label_vars (translated client-side) over
    // the server's English `label` so the active-zones list flips
    // language live when the user toggles EN ↔ SV.
    const displayLabel = it.label_key
      ? t(it.label_key, it.label_vars || {})
      : (it.label || '');
    const remaining = it.days_remaining != null
      ? ` · ${t('sp_meta_days', { n: it.days_remaining })}`
      : '';
    const popK = it.population >= 1000 ? `${(it.population/1000).toFixed(1)}k` : `${it.population}`;
    row.innerHTML = `
      <span class="sp-pal-row-ico">${TOOL_BY_ID[it.type]?.ico || '📍'}</span>
      <span class="sp-pal-row-body">
        <span class="sp-pal-row-lbl">${_escapeHtml(displayLabel)}</span>
        <span class="sp-pal-row-meta">${t('sp_meta_bldg', { n: it.buildings })} · ${popK} pop${remaining}</span>
      </span>
      <button class="sp-pal-row-x" title="${t('sp_dismiss') || 'Dismiss'}">×</button>`;
    row.querySelector('.sp-pal-row-x').addEventListener('click', (e) => {
      e.stopPropagation();
      _remove(it.id);
    });
    // Click anywhere else on the row → fly the camera to the zone.
    row.addEventListener('click', () => _flyToItem(it));
    list.appendChild(row);
  }
}

/** Centre the maplibre camera on a spatial item.
 *  - mobile_clinic → uses the ambulance's *current* live position (so
 *    the camera follows the moving vehicle, not the original click).
 *  - quarantine/vaccine → polygon centroid (averaged across all rings).
 *  - surge_testing → original click center.
 */
function _flyToItem(item) {
  const map = getMap();
  if (!map) return;
  let target = null;

  if (item.type === 'mobile_clinic') {
    // Prefer the ambulance's live world position over the stored
    // center_lonlat so the camera lands where it is *now*, not where
    // the clinic was originally placed.
    const live = ambulance3d.getInfo(item.id);
    if (live && live.currentLonlat) target = live.currentLonlat;
    else if (item.center_lonlat) target = item.center_lonlat;
    // Also select the ambulance — same effect as clicking it on the
    // map: yellow ground ring + tooltip that follows the vehicle as
    // it drives. ui.js#_updateTransportTipPosition picks up the new
    // selection on the next animation tick, so the tooltip appears
    // and tracks the ambulance without an explicit show call.
    try { ambulance3d.setSelected(item.id); } catch (_) {}
  } else if (item.center_lonlat && item.center_lonlat.length === 2) {
    target = item.center_lonlat;
  } else if (Array.isArray(item.polygon) && item.polygon.length) {
    let sx = 0, sy = 0, n = 0;
    for (const ring of item.polygon) {
      if (!Array.isArray(ring)) continue;
      for (const p of ring) {
        if (Array.isArray(p) && p.length >= 2) { sx += p[0]; sy += p[1]; n++; }
      }
    }
    if (n > 0) target = [sx / n, sy / n];
  }
  if (!target) return;
  // Tighter zoom for mobile clinics — the user is honing in on a single
  // ambulance, not a whole neighborhood, and the yellow selection ring
  // reads clearly only when the ambulance fills more of the frame.
  const minZoom = item.type === 'mobile_clinic' ? 16.2 : 14.5;
  try {
    map.flyTo({ center: target, zoom: Math.max(map.getZoom(), minZoom),
                speed: 0.9, essential: true });
  } catch (_) {}
}

/* ── Banner / utility ──────────────────────────────────────────────────── */

function _ensureBanner() {
  if (_banner && document.body.contains(_banner)) return;
  _banner = document.createElement('div');
  _banner.id = 'spatialBanner';
  _banner.style.display = 'none';
  document.body.appendChild(_banner);
}

function _showBanner(text) {
  _ensureBanner();
  _banner.textContent = text;
  _banner.style.display = 'block';
}
function _hideBanner() {
  if (_banner) _banner.style.display = 'none';
}
function _flashBanner(text) {
  _showBanner(text);
  setTimeout(() => {
    if (_banner && _banner.textContent === text) _hideBanner();
  }, 2200);
}
function _escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => (
    {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
