/**
 * macro.js — Sweden macro view (entry point of the app).
 *
 * Owns the country-level overview: a grayscale OSM tile basemap with one
 * marker per known city. Marker size is proportional to population,
 * clamped at both ends. Available cities are clickable and reveal a
 * floating hover card with static stats + live simulation status. City
 * name labels fade in based on the current zoom level (big cities
 * appear earlier, small ones later).
 *
 * Public API:
 *   initMacro(container, summary, onCityClick) — first-time setup
 *   refreshMacro(summary)                       — pick up new sim status
 *   resizeMacro()                               — call after un-hide
 *   setMacroView(centerLonLat, zoom, pitch)     — instant jumpTo
 *   flyMacroToCity(cityId, opts)                — animate dive into a city
 *   flyMacroToSweden(opts)                      — animate pull-back
 */

import { t } from './i18n.js';

// ── Module state ──────────────────────────────────────────────────────────────

let _container   = null;
let _summary     = [];           // last `/api/cities/summary` payload
let _onCityClick = null;

let _map         = null;         // maplibregl.Map instance
let _hoverCard   = null;         // floating <div> for the hover card
let _hoveredId   = null;
let _ready       = false;
let _readyResolvers = [];

// DOM marker layer state — replaces the old MapLibre circle layer for
// available cities. Each city gets one wrapper div with an inline SVG
// donut showing its current SEIR state.
let _markerLayer = null;
const _markerEls = {};           // city_id → wrapper div

// ── Sizing constants ─────────────────────────────────────────────────────────
// Population endpoints used by the marker-radius interpolation.  Below MIN_POP
// the marker stays at MIN_RADIUS; above MAX_POP it caps at MAX_RADIUS.  Two
// radius pairs: one for low zoom (zoomed-out, small markers) and one for high
// zoom (zoomed-in, larger but still capped to keep things readable).
const MIN_POP = 50_000;
const MAX_POP = 1_500_000;
const MIN_RADIUS_LOW  = 5;       // small city, zoomed out
const MAX_RADIUS_LOW  = 12;      // big city,   zoomed out
const MIN_RADIUS_HIGH = 8;       // small city, zoomed in
const MAX_RADIUS_HIGH = 20;      // big city,   zoomed in (CAP)

// Same idea for label text size.
const MIN_LABEL_SIZE_LOW  = 9;
const MAX_LABEL_SIZE_LOW  = 14;
const MIN_LABEL_SIZE_HIGH = 12;
const MAX_LABEL_SIZE_HIGH = 18;

// Public API ────────────────────────────────────────────────────────────────

export async function initMacro(container, summary, onCityClick) {
  _container   = container;
  _summary     = summary;
  _onCityClick = onCityClick;

  // Guard: MapLibre silently creates a 0×0 canvas when the container has
  // no dimensions (e.g. CSS from CDN not yet applied on cold first load).
  // Wait until the container actually has size before initialising.
  if (container.clientWidth === 0 || container.clientHeight === 0) {
    await new Promise(resolve => {
      const ro = new ResizeObserver(entries => {
        for (const e of entries) {
          if (e.contentRect.width > 0 && e.contentRect.height > 0) {
            ro.disconnect();
            resolve();
            return;
          }
        }
      });
      ro.observe(container);
      // Safety timeout — if the container never sizes (broken CSS),
      // proceed anyway after 2 s so the app isn't stuck forever.
      setTimeout(() => { ro.disconnect(); resolve(); }, 2000);
    });
  }

  _renderTiles();
}

export function refreshMacro(summary) {
  _summary = summary;
  // Update the GeoJSON source so the labels symbol layer picks up any
  // new feature properties.
  if (_map && _ready) {
    const src = _map.getSource('cities');
    if (src) src.setData(_buildFeatureCollection());
  }
  // Rebuild the DOM markers' SVG content (new sim state → new donut).
  _renderMarkerContent();
  // If the user is currently hovering a card, refresh its content too.
  if (_hoveredId) _showHoverCard(_hoveredId);
}

export function resizeMacro() {
  if (!_map) return Promise.resolve();
  return _whenReady().then(() => new Promise(resolve => {
    requestAnimationFrame(() => {
      _map.resize();
      requestAnimationFrame(resolve);
    });
  }));
}

export function setMacroView(centerLonLat, zoom, pitch = 0) {
  if (!_map || !centerLonLat) return Promise.resolve();
  return _whenReady().then(() => {
    _map.jumpTo({
      center:  centerLonLat,
      zoom:    zoom,
      pitch:   pitch,
      bearing: 0,
    });
  });
}

export function flyMacroToCity(cityId, opts = {}) {
  const c = _summary.find(x => x.id === cityId);
  if (!c || !_map) return Promise.resolve();
  return _whenReady().then(() => new Promise(resolve => {
    const handler = () => { _map.off('moveend', handler); resolve(); };
    _map.on('moveend', handler);
    _map.flyTo({
      center:   [c.center_lon, c.center_lat],
      zoom:     opts.zoom    ?? 12.5,
      pitch:    opts.pitch   ?? 45,
      bearing:  0,
      duration: opts.duration ?? 1100,
      curve:    1.3,
      essential: true,
    });
    setTimeout(resolve, (opts.duration ?? 1100) + 200);
  }));
}

export function flyMacroToSweden(opts = {}) {
  if (!_map) return Promise.resolve();
  return _whenReady().then(() => new Promise(resolve => {
    const handler = () => { _map.off('moveend', handler); resolve(); };
    _map.on('moveend', handler);
    _map.flyTo({
      center:   [16.5, 62.5],
      zoom:     4.0,
      pitch:    0,
      bearing:  0,
      duration: opts.duration ?? 1300,
      curve:    1.3,
      essential: true,
    });
    setTimeout(resolve, (opts.duration ?? 1300) + 200);
  }));
}

function _whenReady() {
  if (_ready) return Promise.resolve();
  return new Promise(r => _readyResolvers.push(r));
}

// ── GeoJSON construction ─────────────────────────────────────────────────────

/**
 * Build the cities FeatureCollection. Each feature carries the data the
 * styling expressions need (population, label_min_zoom) plus enough
 * metadata for the click + hover handlers to identify the city.
 *
 * `label_min_zoom` is computed per-city so big cities (Stockholm) show
 * their name from quite zoomed-out, while small cities (Kalmar) only
 * show theirs after a few more zoom steps. Avoids label clutter at low
 * zoom while still letting users discover smaller cities by zooming in.
 */
function _buildFeatureCollection() {
  return {
    type: 'FeatureCollection',
    features: _summary.map(c => ({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [c.center_lon, c.center_lat] },
      properties: {
        id:        c.id,
        name:      c.name,
        available: c.available,
        // Use a sensible default for stub cities so the styling expression
        // doesn't choke on null. Stubs render as small grey dots anyway.
        population: c.population || 0,
        // Bigger cities appear at lower zoom. Math: log10(pop) maps
        // roughly 50k → 4.7, 1.2M → 6.08, so subtracting from 9 gives
        // 50k → 4.3 and 1.2M → 2.92. Clamp to a sane range.
        label_min_zoom: c.population
          ? Math.max(3.5, Math.min(6.0, 9 - Math.log10(c.population)))
          : 6.5,
      },
    })),
  };
}

// ── Tiles renderer ────────────────────────────────────────────────────────────

function _renderTiles() {
  _container.innerHTML = '';
  _hoverCard = null;
  _hoveredId = null;
  _ready = false;

  // Reuse the same grayscale OSM style as the city view's map.js so the
  // visual language is consistent.
  const grayscaleStyle = {
    version: 8,
    // Glyph PBFs are required for any symbol layer with `text-field`
    // (the cities-labels layer below). The MapLibre demo CDN serves
    // Open Sans + a few standard families.
    glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
    sources: {
      'osm': {
        type: 'raster',
        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
        tileSize: 256,
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        maxzoom: 19,
      },
    },
    layers: [{
      id: 'osm-tiles', type: 'raster', source: 'osm',
      paint: {
        'raster-saturation':     -1,
        'raster-brightness-max':  0.78,
        'raster-contrast':        0.12,
      },
    }],
  };

  _map = new maplibregl.Map({
    container:          _container,
    style:              grayscaleStyle,
    center:             [16.5, 62.5],   // ~middle of Sweden
    zoom:               4.0,
    minZoom:            3.5,
    pitch:              0,
    bearing:            0,
    doubleClickZoom:    false,
    attributionControl: false,
    // Lock the macro view to pure 2D pan+zoom — no pitch (right-drag),
    // no rotate (right-drag-rotate), no two-finger pitch on touch.
    dragRotate:         false,
    pitchWithRotate:    false,
    touchPitch:         false,
  });
  // Two-finger pinch-zoom stays enabled, but the rotate component of
  // the touchZoomRotate handler is disabled — pinch zooms only.
  if (_map.touchZoomRotate) _map.touchZoomRotate.disableRotation();

  _map.on('load', () => {
    _map.addSource('cities', {
      type: 'geojson',
      data: _buildFeatureCollection(),
    });

    // ── Unavailable cities (small grey dots) ────────────────────────────────
    _map.addLayer({
      id: 'cities-unavailable', type: 'circle', source: 'cities',
      filter: ['==', ['get', 'available'], false],
      paint: {
        'circle-radius':       5,
        'circle-color':        '#475569',
        'circle-opacity':      0.7,
        'circle-stroke-width': 1,
        'circle-stroke-color': '#1e293b',
      },
    });

    // ── Available cities are now DOM markers, not a MapLibre layer ──────────
    // (See _buildMarkers below). The MapLibre source still carries the
    // city features so the labels symbol layer can read names + the
    // label_min_zoom expression. We just don't draw circles in MapLibre.

    // ── Available city labels (zoom-fade-in, population-driven size) ───────
    // text-opacity uses a per-feature label_min_zoom: cities don't appear
    // until the camera reaches their personal threshold, then fade in
    // over the next 0.7 zoom levels.
    _map.addLayer({
      id: 'cities-labels', type: 'symbol', source: 'cities',
      filter: ['==', ['get', 'available'], true],
      layout: {
        'text-field':  ['get', 'name'],
        'text-size': [
          'interpolate', ['linear'], ['zoom'],
          4.0, [
            'interpolate', ['linear'], ['get', 'population'],
            MIN_POP, MIN_LABEL_SIZE_LOW,
            MAX_POP, MAX_LABEL_SIZE_LOW,
          ],
          7.0, [
            'interpolate', ['linear'], ['get', 'population'],
            MIN_POP, MIN_LABEL_SIZE_HIGH,
            MAX_POP, MAX_LABEL_SIZE_HIGH,
          ],
        ],
        'text-offset': [0, 1.6],
        'text-anchor': 'top',
        'text-font':   ['Open Sans Regular', 'Arial Unicode MS Regular'],
        'text-allow-overlap': false,
      },
      paint: {
        'text-color':       '#f1f5f9',
        'text-halo-color':  '#0b1120',
        'text-halo-width':  1.8,
        // MapLibre rejects feature-property expressions as `interpolate`
        // input values — the inputs must be literal numbers. Emulate the
        // "fade in starting at label_min_zoom over 0.7 zoom levels" with
        // a `case`/clamp combination that's evaluated per-feature.
        'text-opacity': [
          'case',
          ['<', ['zoom'], ['get', 'label_min_zoom']], 0,
          ['>=', ['zoom'], ['+', ['get', 'label_min_zoom'], 0.7]], 1,
          // Linear ramp inside the [min_zoom, min_zoom + 0.7] window
          ['/',
            ['-', ['zoom'], ['get', 'label_min_zoom']],
            0.7,
          ],
        ],
      },
    });

    // ── DOM marker layer + hover card ───────────────────────────────────────
    // Build the marker DOM (one wrapper div per available city) and
    // the hover card container. Markers handle their own click/hover
    // events directly — no MapLibre layer event delegation needed.
    _buildMarkers();

    _hoverCard = document.createElement('div');
    _hoverCard.className = 'macro-hover-card';
    _container.appendChild(_hoverCard);

    // Reposition + resize markers (and the hover card if visible)
    // whenever the camera moves. `move` fires on both pan and zoom.
    _map.on('move', () => {
      _updateMarkers();
      if (_hoveredId) _positionHoverCard(_hoveredId);
    });
    // Initial layout pass
    _updateMarkers();

    _ready = true;
    _readyResolvers.splice(0).forEach(r => r());
  });
}

// ── DOM marker layer ─────────────────────────────────────────────────────────

/**
 * Create one wrapper div per available city, attach click + hover
 * handlers, and stash them in _markerEls. Each div will be sized and
 * positioned by _updateMarkers on every map move. The SVG content
 * (donut + dot) is rendered once here and rebuilt by refreshMacro
 * whenever the cities/summary payload changes.
 */
function _buildMarkers() {
  // Tear down any previous layer (e.g. on re-init)
  if (_markerLayer && _markerLayer.parentNode) {
    _markerLayer.parentNode.removeChild(_markerLayer);
  }
  for (const k of Object.keys(_markerEls)) delete _markerEls[k];

  _markerLayer = document.createElement('div');
  _markerLayer.className = 'macro-marker-layer';
  _container.appendChild(_markerLayer);

  for (const c of _summary) {
    if (!c.available) continue;
    const el = document.createElement('div');
    el.className = 'macro-marker';
    el.dataset.id = c.id;

    // Click → enter the city
    el.addEventListener('click', e => {
      e.stopPropagation();
      if (_onCityClick) _onCityClick(c.id);
    });
    // Hover → show / hide hover card
    el.addEventListener('mouseenter', () => _showHoverCard(c.id));
    el.addEventListener('mouseleave', _hideHoverCard);

    _markerLayer.appendChild(el);
    _markerEls[c.id] = el;
  }
  _renderMarkerContent();
}

/**
 * Reposition + resize every marker. Called on every map `move` event
 * (pans + zooms). The wrapper div's left/top/width/height are updated
 * via inline style; the SVG inside scales automatically because it
 * uses a fixed viewBox.
 */
function _updateMarkers() {
  if (!_map || !_markerLayer) return;
  const zoom = _map.getZoom();
  for (const c of _summary) {
    if (!c.available) continue;
    const el = _markerEls[c.id];
    if (!el) continue;
    const radius = _markerRadius(c.population, zoom);
    const size = 2 * radius;
    el.style.width  = `${size}px`;
    el.style.height = `${size}px`;
    const p = _map.project([c.center_lon, c.center_lat]);
    el.style.left = `${p.x}px`;
    el.style.top  = `${p.y}px`;
  }
}

/**
 * (Re)build the SVG inside every marker. Called on initial setup and
 * on refreshMacro (after a city's sim state changes). Uses the fixed
 * 100×100 viewBox so the wrapper's CSS width/height controls the
 * actual screen size.
 */
function _renderMarkerContent() {
  for (const c of _summary) {
    if (!c.available) continue;
    const el = _markerEls[c.id];
    if (!el) continue;
    el.innerHTML = _markerSvg(c);
  }
}

/**
 * Compute a marker's outer radius in screen pixels. Mirrors the same
 * population × zoom interpolation that the dropped MapLibre circle
 * layer used, so visual sizing is consistent with the original spec.
 */
function _markerRadius(population, zoom) {
  // Population factor (clamped 0..1 across MIN_POP..MAX_POP)
  const pop = Math.max(MIN_POP, Math.min(MAX_POP, population || MIN_POP));
  const popT = (pop - MIN_POP) / (MAX_POP - MIN_POP);
  const lowR  = MIN_RADIUS_LOW  + popT * (MAX_RADIUS_LOW  - MIN_RADIUS_LOW);
  const highR = MIN_RADIUS_HIGH + popT * (MAX_RADIUS_HIGH - MIN_RADIUS_HIGH);
  // Zoom factor (clamped 4..7 — past 7 the marker stays at MAX_RADIUS_HIGH)
  const z = Math.max(4, Math.min(7, zoom));
  const zT = (z - 4) / 3;
  return lowR + zT * (highR - lowR);
}

/**
 * Build the SVG for a city marker:
 *   • central black dot (the city itself)
 *   • SEIR donut ring around it (S green, E yellow, I red, R blue, D grey)
 *   • cold cities (no sim yet) get a thin grey ring instead
 *
 * Returned as a string ready for innerHTML. ViewBox is 100×100; the
 * wrapper div's width/height controls actual pixel size.
 */
function _markerSvg(city) {
  const cx = 50, cy = 50;
  // Concentric layout inside the 100×100 viewBox:
  const dotR  = 30;            // central black dot radius
  const ringR = 41;             // donut centerline radius
  const ringW = 14;             // donut stroke width
  const C = 2 * Math.PI * ringR;

  // The status donut. For warm cities, render five SEIR arcs.
  let donut;
  if (city.status) {
    const s = city.status;
    const total = s.total || 1;
    const fS = s.S / total;
    const fE = s.E / total;
    const fI = s.I / total;
    const fR = s.R / total;
    const fD = s.D / total;
    const arc = (frac, color, offsetFrac) => `
      <circle cx="${cx}" cy="${cy}" r="${ringR}" fill="none" stroke="${color}"
              stroke-width="${ringW}"
              stroke-dasharray="${(frac * C).toFixed(2)} ${C.toFixed(2)}"
              stroke-dashoffset="${(-offsetFrac * C).toFixed(2)}"
              transform="rotate(-90 ${cx} ${cy})"/>`;
    donut = `
      <circle cx="${cx}" cy="${cy}" r="${ringR}" fill="none" stroke="#1e293b" stroke-width="${ringW}"/>
      ${arc(fS, '#4ade80', 0)}
      ${arc(fE, '#facc15', fS)}
      ${arc(fI, '#f87171', fS + fE)}
      ${arc(fR, '#60a5fa', fS + fE + fI)}
      ${arc(fD, '#94a3b8', fS + fE + fI + fR)}`;
  } else {
    // Cold city — uniform thin ring so the marker still has the
    // characteristic donut shape.
    donut = `
      <circle cx="${cx}" cy="${cy}" r="${ringR}" fill="none"
              stroke="#475569" stroke-width="${ringW}"/>`;
  }

  return `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid meet">
    ${donut}
    <circle cx="${cx}" cy="${cy}" r="${dotR}" fill="#0b1120"/>
    <circle cx="${cx}" cy="${cy}" r="${dotR}" fill="none"
            stroke="#1e293b" stroke-width="1.5"/>
  </svg>`;
}

// ── Hover card ───────────────────────────────────────────────────────────────

function _showHoverCard(cityId) {
  if (!_hoverCard) return;
  const c = _summary.find(x => x.id === cityId);
  if (!c) return;
  _hoveredId = cityId;
  _hoverCard.innerHTML = _buildHoverCardHtml(c);
  _hoverCard.classList.add('show');
  _positionHoverCard(cityId);
}

function _hideHoverCard() {
  if (!_hoverCard) return;
  _hoveredId = null;
  _hoverCard.classList.remove('show');
}

function _positionHoverCard(cityId) {
  if (!_hoverCard || !_map) return;
  const c = _summary.find(x => x.id === cityId);
  if (!c) return;
  const p = _map.project([c.center_lon, c.center_lat]);
  _hoverCard.style.left = `${p.x}px`;
  _hoverCard.style.top  = `${p.y}px`;
}

function _buildHoverCardHtml(city) {
  const name = `<div class="hc-title">${_escapeHtml(city.name)}</div>`;

  // ── Static data section (always shown) ─────────────────────────────────
  const pop = city.population != null ? _fmtInt(city.population) : '—';
  const bld = city.building_count != null ? _fmtInt(city.building_count) : '—';
  const stats = `
    <div class="hc-row">
      <span class="hc-label">${t('population')}</span>
      <span class="hc-value">${pop}</span>
    </div>
    <div class="hc-row">
      <span class="hc-label">${t('buildings')}</span>
      <span class="hc-value">${bld}</span>
    </div>`;

  // ── Live simulation section (only when warm) ───────────────────────────
  let sim = '';
  if (city.status) {
    const s = city.status;
    const donut = _donutSvg(s);
    sim = `
      <div class="hc-section">
        <div class="hc-section-title">${donut}<span>${t('simulation_day', { n: s.day })}</span></div>
        <div class="hc-row">
          <span class="hc-label hc-S">${t('susceptible')}</span>
          <span class="hc-value">${_fmtInt(s.S)}</span>
        </div>
        <div class="hc-row">
          <span class="hc-label hc-E">${t('exposed')}</span>
          <span class="hc-value">${_fmtInt(s.E)}</span>
        </div>
        <div class="hc-row">
          <span class="hc-label hc-I">${t('infectious')}</span>
          <span class="hc-value">${_fmtInt(s.I)}</span>
        </div>
        <div class="hc-row">
          <span class="hc-label hc-R">${t('recovered')}</span>
          <span class="hc-value">${_fmtInt(s.R)}</span>
        </div>
        <div class="hc-row">
          <span class="hc-label hc-D">${t('deaths')}</span>
          <span class="hc-value">${_fmtInt(s.D)}</span>
        </div>
      </div>`;
  } else if (city.available) {
    sim = `<div class="hc-cta">${t('click_to_start')}</div>`;
  }

  return name + stats + sim;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function _fmtInt(n) {
  if (n == null) return '—';
  // Compact display for big numbers; precise for small.
  const x = Math.round(n);
  if (Math.abs(x) >= 1_000_000) return (x / 1_000_000).toFixed(1) + 'M';
  if (Math.abs(x) >= 10_000)    return (x / 1000).toFixed(0) + 'k';
  return x.toLocaleString('en-US');
}

/**
 * Tiny S/I/R donut chart used inside the hover card. Three concentric
 * arcs on a 14×14 SVG.
 */
function _donutSvg(status) {
  const total = status.total || 1;
  const fS = status.S / total;
  const fI = (status.I + status.E) / total;
  const fR = (status.R + status.D) / total;

  const cx = 7, cy = 7, r = 5;
  const C  = 2 * Math.PI * r;

  const arc = (frac, color, offsetFrac) =>
    `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${color}"
             stroke-width="2.4"
             stroke-dasharray="${(frac * C).toFixed(2)} ${C.toFixed(2)}"
             stroke-dashoffset="${(-offsetFrac * C).toFixed(2)}"
             transform="rotate(-90 ${cx} ${cy})"/>`;

  return `<svg viewBox="0 0 14 14">
    <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="#1e293b" stroke-width="2.4"/>
    ${arc(fS, '#4ade80', 0)}
    ${arc(fI, '#f87171', fS)}
    ${arc(fR, '#60a5fa', fS + fI)}
  </svg>`;
}

function _escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, ch => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[ch]));
}
