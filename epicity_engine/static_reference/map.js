/**
 * map.js — MapLibre GL JS initialisation + Three.js CustomLayerInterface.
 *
 * Exports:
 *   initMap(containerId, cityLayout) → { map, ready: Promise }
 *
 * The `ready` promise resolves when MapLibre has loaded AND the Three.js
 * scene has been initialised inside the custom layer's onAdd callback.
 *
 * Coordinate convention (shared with osm.py):
 *   world_x  — metres East  of city centre  (Three.js X+)
 *   world_z  — metres South of city centre  (Three.js Z+)
 *   world_y  — metres above ground          (Three.js Y+)
 *
 * The MapLibre ↔ Three.js bridge uses a "world-to-Mercator" matrix:
 *   translate to city-centre MercatorCoordinate,
 *   scale by meterInMercatorCoordinateUnits(),
 *   rotate X by +π/2 to align Three.js Y-up with MapLibre Z-up.
 */

import {
  initView, renderScene,
  notifyRendererResize, refreshLineMaterials,
} from './view.js';
import { t } from './i18n.js';

// ── Module state ──────────────────────────────────────────────────────────────

let _map   = null;
let _mc    = null;   // MercatorCoordinate of city centre
let _scale = 1;      // meterInMercatorCoordinateUnits

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * @param {string}  containerId  DOM element ID for the MapLibre container
 * @param {object}  cityLayout   /api/city response
 * @returns {{ map: maplibregl.Map, ready: Promise<void> }}
 */
export function initMap(containerId, cityLayout) {
  let resolveReady;
  const ready = new Promise(resolve => { resolveReady = resolve; });

  // Grayscale raster style — OSM tiles fully desaturated so 3D buildings pop
  const grayscaleStyle = {
    version: 8,
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
      id:     'osm-tiles',
      type:   'raster',
      source: 'osm',
      paint:  {
        'raster-saturation':      -1,    // full grayscale
        'raster-brightness-max':   0.85, // slightly dim so buildings contrast
        'raster-contrast':         0.1,
      },
    }],
  };

  _map = new maplibregl.Map({
    container:       containerId,
    style:           grayscaleStyle,
    center:          [cityLayout.center_lon, cityLayout.center_lat],
    zoom:            15,
    pitch:           45,
    bearing:         0,
    antialias:       true,
    doubleClickZoom: false,   // disable: a double-click should not zoom
  });

  // Keep three.js's viewport + LineMaterial resolutions in sync with the
  // canvas after every MapLibre resize (window resize, container resize,
  // manual map.resize). Without this, three.js writes its stale cached
  // viewport over MapLibre's freshly-set one and the buildings appear
  // shifted relative to the basemap until the next interaction.
  _map.on('resize', () => {
    const canvas = _map.getCanvas();
    if (canvas) {
      notifyRendererResize(false, canvas.clientWidth, canvas.clientHeight);
    }
    refreshLineMaterials();
    _map.triggerRepaint();
  });

  _map.on('load', () => {
    _mc    = maplibregl.MercatorCoordinate.fromLngLat(
      [cityLayout.center_lon, cityLayout.center_lat], 0
    );
    _scale = _mc.meterInMercatorCoordinateUnits();

    // ── Dark road vector layer ────────────────────────────────────────────
    const mpl  = 111320.0;
    const mplx = 111320.0 * Math.cos(cityLayout.center_lat * Math.PI / 180);
    const roadFeatures = cityLayout.roads.map(r => ({
      type: 'Feature',
      properties: { highway: r.type },
      geometry: {
        type: 'LineString',
        coordinates: r.points.map(([wx, wz]) => [
          wx / mplx + cityLayout.center_lon,
          -wz / mpl  + cityLayout.center_lat,
        ]),
      },
    }));

    _map.addSource('road-lines', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: roadFeatures },
    });

    _map.addLayer({
      id:     'road-dark',
      type:   'line',
      source: 'road-lines',
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint:  {
        'line-color': '#2e2e2e',
        // Width scales with zoom (exponential base 2 mirrors map resolution doubling).
        // Values chosen to match approximate real-world road widths in metres.
        'line-width': [
          'interpolate', ['exponential', 2], ['zoom'],
          10, ['match', ['get', 'highway'],
            ['motorway', 'trunk'],                       1.5,
            ['primary', 'secondary'],                    1.0,
            ['tertiary', 'unclassified', 'residential'], 0.6,
            0.3,
          ],
          15, ['match', ['get', 'highway'],
            ['motorway', 'trunk'],                       10,
            ['primary', 'secondary'],                    7,
            ['tertiary', 'unclassified', 'residential'], 4,
            2,
          ],
          18, ['match', ['get', 'highway'],
            ['motorway', 'trunk'],                       55,
            ['primary', 'secondary'],                    35,
            ['tertiary', 'unclassified', 'residential'], 20,
            10,
          ],
        ],
        'line-opacity': 0.85,
      },
    });
    // ── Municipality border (red outline) ──
    if (cityLayout.border && cityLayout.border.length > 0) {
      const borderFeatures = cityLayout.border.map(ring => ({
        type: 'Feature',
        properties: {},
        geometry: {
          type: 'Polygon',
          coordinates: [ring],
        },
      }));

      _map.addSource('city-border-src', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: borderFeatures },
      });

      _map.addLayer({
        id:     'city-border',
        type:   'line',
        source: 'city-border-src',
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint:  {
          'line-color': '#ef4444',
          'line-width': 3,
          'line-opacity': 0.8,
          'line-dasharray': [3, 2],
        },
      });
    }
    // ─────────────────────────────────────────────────────────────────────

    _map.addLayer(_buildCustomLayer(cityLayout, resolveReady));
  });

  return { map: _map, ready };
}

/** Get the active MapLibre instance (for overlays that need projection). */
export function getMap() { return _map; }

/** City-centre MercatorCoordinate, set on map load + on city switches.
 *  Compare-view-b.js's secondary three.js custom layer uses this to
 *  build the same world-to-Mercator matrix the primary layer does, so
 *  the same scene aligns with map B's basemap. */
export function getCenterMercator() { return _mc; }

/** Mercator units per metre at the city centre. Same use as
 *  getCenterMercator — needed by the secondary renderer's matrix. */
export function getMeterScale() { return _scale; }

/** Fly to top-down view. Preserves the current center & zoom. */
export function flyToTopView() {
  if (_map) _map.flyTo({ pitch: 0, bearing: 0, duration: 600 });
}

/** Fly back to a 3D perspective without changing center or zoom. */
export function flyToPerspective() {
  if (_map) _map.flyTo({ pitch: 45, bearing: 0, duration: 600 });
}

/** Reset map to initial city view. */
export function flyToCity(cityLayout) {
  if (_map) _map.flyTo({
    center: [cityLayout.center_lon, cityLayout.center_lat],
    zoom: 15, pitch: 45, bearing: 0, duration: 800,
  });
}

/**
 * Re-anchor the map projection on a new city, replace the road
 * layer, and recenter. Called from ui.js _loadCityScene whenever
 * the user switches cities. The Three.js custom layer doesn't need
 * to be re-added — it just receives the new (_mc, _scale) via the
 * per-frame render callback.
 */
export function rebuildForCity(cityLayout) {
  if (!_map || !cityLayout) return;

  // Update projection anchor — renderScene reads these every frame.
  _mc    = maplibregl.MercatorCoordinate.fromLngLat(
    [cityLayout.center_lon, cityLayout.center_lat], 0
  );
  _scale = _mc.meterInMercatorCoordinateUnits();

  // Snap (no fly) to the new city center so the camera doesn't drift
  // through space mid-transition. The macro-view fly already brought
  // us to the right area; this just makes sure the city map is anchored.
  _map.jumpTo({
    center: [cityLayout.center_lon, cityLayout.center_lat],
    zoom: 15, pitch: 45, bearing: 0,
  });

  // Replace the road layer with the new city's roads.
  if (_map.getLayer('road-dark')) _map.removeLayer('road-dark');
  if (_map.getSource('road-lines')) _map.removeSource('road-lines');

  const mpl  = 111320.0;
  const mplx = 111320.0 * Math.cos(cityLayout.center_lat * Math.PI / 180);
  const roadFeatures = cityLayout.roads.map(r => ({
    type: 'Feature',
    properties: { highway: r.type },
    geometry: {
      type: 'LineString',
      coordinates: r.points.map(([wx, wz]) => [
        wx / mplx + cityLayout.center_lon,
        -wz / mpl  + cityLayout.center_lat,
      ]),
    },
  }));

  _map.addSource('road-lines', {
    type: 'geojson',
    data: { type: 'FeatureCollection', features: roadFeatures },
  });

  _map.addLayer({
    id:     'road-dark',
    type:   'line',
    source: 'road-lines',
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint:  {
      'line-color': '#2e2e2e',
      'line-width': [
        'interpolate', ['exponential', 2], ['zoom'],
        10, ['match', ['get', 'highway'],
          ['motorway', 'trunk'],                       1.5,
          ['primary', 'secondary'],                    1.0,
          ['tertiary', 'unclassified', 'residential'], 0.6,
          0.3,
        ],
        15, ['match', ['get', 'highway'],
          ['motorway', 'trunk'],                       10,
          ['primary', 'secondary'],                    7,
          ['tertiary', 'unclassified', 'residential'], 4,
          2,
        ],
        18, ['match', ['get', 'highway'],
          ['motorway', 'trunk'],                       55,
          ['primary', 'secondary'],                    35,
          ['tertiary', 'unclassified', 'residential'], 20,
          10,
        ],
      ],
      'line-opacity': 0.85,
    },
  }, _map.getLayer('epidemic-buildings') ? 'epidemic-buildings' : undefined);

  // ── Municipality border (red outline) ──
  if (_map.getLayer('city-border')) _map.removeLayer('city-border');
  if (_map.getSource('city-border-src')) _map.removeSource('city-border-src');

  if (cityLayout.border && cityLayout.border.length > 0) {
    const borderFeatures = cityLayout.border.map(ring => ({
      type: 'Feature',
      properties: {},
      geometry: {
        type: 'Polygon',
        coordinates: [ring],   // ring is already [[lon, lat], ...]
      },
    }));

    _map.addSource('city-border-src', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: borderFeatures },
    });

    _map.addLayer({
      id:     'city-border',
      type:   'line',
      source: 'city-border-src',
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint:  {
        'line-color': '#ef4444',
        'line-width': 3,
        'line-opacity': 0.8,
        'line-dasharray': [3, 2],
      },
    });
  }

  // ── Remove old neighborhood layers (will be re-fetched for new city) ──
  _clearNbhSelection();
  for (const id of ['nbh-labels', 'nbh-border', 'nbh-fill']) {
    if (_map.getLayer(id)) _map.removeLayer(id);
  }
  for (const id of ['nbh-border-src', 'nbh-label-src']) {
    if (_map.getSource(id)) _map.removeSource(id);
  }
  _neighborhoodsVisible = false;
  _nbhCentroids = {};
  _nbhPopup = null;
}

/**
 * Smoothly center the map on a specific building. The default `zoom`
 * is intentionally just slightly above the city zoom (15) so the user
 * still sees surrounding context — not a tight street-level shot.
 *
 * `curve` and `speed` are tuned for a noticeably gentler arc than
 * MapLibre's defaults: the camera lifts a touch, glides, and settles.
 */
export function flyToBuilding(lonLat, zoom = 15.6) {
  if (!_map || !lonLat) return;
  _map.flyTo({
    center:    lonLat,
    zoom:      zoom,
    duration:  1600,
    curve:     1.2,    // flatter arc → less zoom-out at the midpoint
    speed:     0.6,    // slower overall pacing
    easing:    t => t * (2 - t),  // ease-out for a soft landing
    essential: true,
  });
}

/**
 * Draw train and tram schedule lines on the map so their routes are visible.
 * Called from ui.js after /api/transport resolves.
 * @param {object} tdata  {infra, schedule} from /api/transport
 * @param {number} centerLat
 * @param {number} centerLon
 */
export function showTransportLines(tdata, centerLat, centerLon) {
  if (!_map || !tdata || !tdata.schedule) return;

  // Remove previous layers
  for (const id of ['rail-lines', 'tram-lines', 'bus-lines', 'ferry-lines']) {
    if (_map.getLayer(id)) _map.removeLayer(id);
  }
  if (_map.getSource('transport-lines-src')) _map.removeSource('transport-lines-src');

  const mpl  = 111320.0;
  const mplx = 111320.0 * Math.cos(centerLat * Math.PI / 180);

  const features = [];
  for (const line of tdata.schedule.lines || []) {
    if (line.type !== 'train' && line.type !== 'tram' && line.type !== 'bus' && line.type !== 'ferry') continue;
    if (!line.polyline || line.polyline.length < 2) continue;
    features.push({
      type: 'Feature',
      properties: { lineType: line.type, id: line.id },
      geometry: {
        type: 'LineString',
        coordinates: line.polyline.map(([wx, wz]) => [
          wx / mplx + centerLon,
          -wz / mpl + centerLat,
        ]),
      },
    });
  }

  if (features.length === 0) return;

  _map.addSource('transport-lines-src', {
    type: 'geojson',
    data: { type: 'FeatureCollection', features },
  });

  // Insert all route layers BELOW the 3D building layer so vehicles render on top
  const beforeId = _map.getLayer('epidemic-buildings') ? 'epidemic-buildings' : undefined;

  // Rail lines — black by default
  _map.addLayer({
    id:     'rail-lines',
    type:   'line',
    source: 'transport-lines-src',
    filter: ['==', ['get', 'lineType'], 'train'],
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint:  {
      'line-color': '#1c1917',
      'line-width': 2.5,
      'line-opacity': 0.7,
      'line-dasharray': [4, 2],
    },
  }, beforeId);

  // Tram lines — black by default
  _map.addLayer({
    id:     'tram-lines',
    type:   'line',
    source: 'transport-lines-src',
    filter: ['==', ['get', 'lineType'], 'tram'],
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint:  {
      'line-color': '#1c1917',
      'line-width': 2,
      'line-opacity': 0.7,
      'line-dasharray': [3, 2],
    },
  }, beforeId);

  // Bus lines — black by default
  _map.addLayer({
    id:     'bus-lines',
    type:   'line',
    source: 'transport-lines-src',
    filter: ['==', ['get', 'lineType'], 'bus'],
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint:  {
      'line-color': '#1c1917',
      'line-width': 1.5,
      'line-opacity': 0.5,
      'line-dasharray': [2, 2],
    },
  }, beforeId);

  // Ferry lines — cyan when highlighted
  _map.addLayer({
    id:     'ferry-lines',
    type:   'line',
    source: 'transport-lines-src',
    filter: ['==', ['get', 'lineType'], 'ferry'],
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint:  {
      'line-color': '#1c1917',
      'line-width': 2.5,
      'line-opacity': 0.7,
      'line-dasharray': [6, 3],
    },
  }, beforeId);
}

/** Default (non-highlighted) line colours per type. */
const _defaultLineColors = { train: '#1c1917', tram: '#1c1917', bus: '#1c1917', ferry: '#1c1917' };
/** Highlighted line colours per type. Train = tangerine orange (distinct
 *  from the red city-border outline, the bright blue water outline, AND
 *  the amber bus highlight). */
const _highlightColors   = { train: '#f97316', tram: '#22c55e', bus: '#f59e0b', ferry: '#0891b2' };
const _layerIds          = { train: 'rail-lines', tram: 'tram-lines', bus: 'bus-lines', ferry: 'ferry-lines' };

/**
 * Toggle highlight for a transport line type.
 * @param {'train'|'tram'|'bus'|'ferry'} type
 * @param {boolean} highlighted
 */
export function highlightTransportLine(type, highlighted) {
  if (!_map) return;
  const layerId = _layerIds[type];
  if (!layerId || !_map.getLayer(layerId)) return;
  const color = highlighted ? _highlightColors[type] : _defaultLineColors[type];
  const opacity = highlighted ? 0.85 : (type === 'bus' ? 0.5 : 0.7);
  _map.setPaintProperty(layerId, 'line-color', color);
  _map.setPaintProperty(layerId, 'line-opacity', opacity);
}

// ── Neighborhood borders + interactive area overlay ─────────────────────────

let _neighborhoodsVisible = false;
let _nbhPopup = null;
let _nbhHoveredId = null;
let _nbhSelectedId = null;
let _nbhCentroids = {};     // fid → [lon, lat]

// Side-by-side compare-view registers its secondary map here so neighborhood
// hover/click/select state is mirrored across both halves. Selection and
// highlight always operate on the union of (_map + _nbhSyncMaps), so a click
// on either side updates both.
const _nbhSyncMaps = [];
function _allNbhMaps() {
  const out = [];
  if (_map) out.push(_map);
  for (const m of _nbhSyncMaps) if (m) out.push(m);
  return out;
}
function _setNbhFeatureStateAll(id, state) {
  for (const m of _allNbhMaps()) {
    if (m.getSource('nbh-border-src')) {
      m.setFeatureState({ source: 'nbh-border-src', id }, state);
    }
  }
}
function _setNbhLayoutAll(layerId, prop, val) {
  for (const m of _allNbhMaps()) {
    if (m.getLayer(layerId)) m.setLayoutProperty(layerId, prop, val);
  }
}

/** Register a secondary map (compare side-by-side) so neighborhood state
 *  mirrors to it. Bind interactions lazily once the secondary map has the
 *  nbh source replicated by compare-view-b.js's style sync. */
export function addNbhSyncMap(map) {
  if (!map || _nbhSyncMaps.includes(map)) return;
  _nbhSyncMaps.push(map);
  const tryBind = () => {
    if (map.getSource('nbh-border-src') && !map._nbhBound) {
      _bindNbhInteractions(map);
      // Replay current selection on the freshly-bound map.
      if (_nbhSelectedId !== null) {
        map.setFeatureState(
          { source: 'nbh-border-src', id: _nbhSelectedId },
          { hover: true, selected: true },
        );
      }
    }
  };
  tryBind();
  map.on('styledata', tryBind);
  map._nbhTryBindHandler = tryBind;
}

export function removeNbhSyncMap(map) {
  const i = _nbhSyncMaps.indexOf(map);
  if (i >= 0) _nbhSyncMaps.splice(i, 1);
  if (map?._nbhTryBindHandler) {
    try { map.off('styledata', map._nbhTryBindHandler); } catch {}
    map._nbhTryBindHandler = null;
    map._nbhBound = false;
  }
}

/** @returns {boolean} Whether the area overlay is currently active. */
export function isAreasActive() { return _neighborhoodsVisible; }

function _buildNbhPopupHtml(p) {
  const pop = Number(p.pop) || 0;
  const male = Number(p.male) || 0;
  const female = Number(p.female) || 0;
  const malePct = pop > 0 ? ((male / pop) * 100).toFixed(1) : '–';
  const femalePct = pop > 0 ? ((female / pop) * 100).toFixed(1) : '–';
  const income = Number(p.avg_income);
  const incomeStr = income > 0 ? `${income.toFixed(0)} kSEK` : '–';
  const hh = Number(p.households) || 0;
  const child = Number(p.child) || 0;
  const adult = Number(p.adult) || 0;
  const elder = Number(p.elder) || 0;
  return `
    <div style="font-family:system-ui,sans-serif;font-size:12px;line-height:1.5;min-width:190px">
      <b style="font-size:13px;color:#c4b5fd">${p.name || t('unknown_area')}</b>
      <div style="font-size:0.72rem;margin-top:3px">
        Pop ${pop.toLocaleString()} · ${Number(p.buildings).toLocaleString()} ${t('buildings').toLowerCase()}
      </div>
      <div style="color:#d4d4d8;font-size:0.72rem;margin-top:1px">
        ${t('income_label')}: ${incomeStr}
      </div>
      <div style="font-size:0.69rem;margin-top:1px">
        <span style="color:#60a5fa">♂ ${malePct}%</span>
        <span style="color:#f472b6">♀ ${femalePct}%</span>
      </div>
      <div style="font-size:0.69rem;margin-top:1px;color:#94a3b8">
        ${t('households_est', { n: hh.toLocaleString(), avg: (pop > 0 && hh > 0 ? (pop / hh).toFixed(1) : '–') })}
      </div>
      <div style="border-top:1px solid #334155;margin-top:5px;padding-top:4px;font-size:0.69rem">
        <div style="color:#64748b;margin-bottom:2px">${t('age_breakdown')}</div>
        <span style="color:#86efac">${t('age_child')}: ${child.toLocaleString()}</span>&nbsp;
        <span style="color:#fde68a">${t('age_adult')}: ${adult.toLocaleString()}</span>&nbsp;
        <span style="color:#c4b5fd">${t('age_elder')}: ${elder.toLocaleString()}</span>
      </div>
      ${_areaOriginHtml(p)}
    </div>`;
}

/**
 * Country-of-birth breakdown for an area popup. Reads the engine-supplied
 * 6-vector (passed through as a JSON string in the GeoJSON properties).
 */
function _areaOriginHtml(p) {
  const groups = window._epiCityOriginGroups;
  if (!groups || !p.origin) return '';
  let origin;
  try { origin = JSON.parse(p.origin); } catch { return ''; }
  if (!Array.isArray(origin) || origin.length !== groups.length) return '';
  const lang = window._epiCityLang || 'en';
  const ranked = origin
    .map((f, i) => ({
      f,
      label: (lang === 'sv' && groups[i].label_sv) || groups[i].label_en,
      color: groups[i].color,
      flag:  groups[i].flag || '',
    }))
    .sort((a, b) => b.f - a.f)
    .slice(0, 4)
    .filter(r => r.f >= 0.005);
  if (!ranked.length) return '';
  const rows = ranked.map(r => `
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

function _showNbhPopup(map, fid, props, lngLat) {
  const anchor = lngLat || _nbhCentroids[fid];
  if (!anchor) return;
  if (!_nbhPopup) {
    _nbhPopup = new maplibregl.Popup({
      closeButton: false, closeOnClick: false,
      maxWidth: '280px',
      className: 'nbh-popup',
      offset: 12,
    });
  }
  // .addTo() removes the popup from any previous map automatically, so
  // hovering across the divider in split mode hands the popup over to
  // the active half rather than leaving a stale anchor on the other side.
  _nbhPopup.setLngLat(anchor).setHTML(_buildNbhPopupHtml(props)).addTo(map || _map);
}

function _clearNbhSelection() {
  if (_nbhSelectedId !== null) {
    _setNbhFeatureStateAll(_nbhSelectedId, { hover: false, selected: false });
    _nbhSelectedId = null;
  }
  _nbhHoveredId = null;
  if (_nbhPopup) _nbhPopup.remove();
}

/**
 * Public-facing wrapper so spatial-tools (and similar) can clear the
 * neighborhood selection ring after a quarantine zone is created — the
 * red dashed border stands on its own and the purple selection halo
 * fights with it visually.
 */
export function clearNeighborhoodSelection() {
  _clearNbhSelection();
}

function _bindNbhInteractions(map) {
  if (map._nbhBound) return;
  map._nbhBound = true;

  map.on('mousemove', 'nbh-fill', (e) => {
    if (!_neighborhoodsVisible) return;
    map.getCanvas().style.cursor = 'pointer';
    const f = e.features?.[0];
    if (!f) return;
    const newId = f.id ?? f.properties.fid;

    if (newId !== _nbhHoveredId) {
      if (_nbhHoveredId !== null && _nbhHoveredId !== _nbhSelectedId) {
        _setNbhFeatureStateAll(_nbhHoveredId, { hover: false });
      }
      if (newId !== _nbhSelectedId) {
        _setNbhFeatureStateAll(newId, { hover: true });
      }
      _nbhHoveredId = newId;
    }
    _showNbhPopup(map, newId, f.properties, e.lngLat);
  });

  map.on('mouseleave', 'nbh-fill', () => {
    map.getCanvas().style.cursor = '';
    if (_nbhHoveredId !== null && _nbhHoveredId !== _nbhSelectedId) {
      _setNbhFeatureStateAll(_nbhHoveredId, { hover: false });
    }
    _nbhHoveredId = null;
    if (_nbhPopup) _nbhPopup.remove();
  });

  map.on('click', 'nbh-fill', (e) => {
    if (!_neighborhoodsVisible) return;
    // While a spatial intervention tool is in selection mode, the click
    // is owned by spatial-tools — skip the Areas-layer purple selection
    // here so it doesn't fight the red dashed quarantine border.
    if (document.body.classList.contains('spatial-selecting')) return;
    const f = e.features?.[0];
    if (!f) return;
    const clickedId = f.id ?? f.properties.fid;

    if (_nbhSelectedId === clickedId) {
      _clearNbhSelection();
      return;
    }
    if (_nbhSelectedId !== null) {
      _setNbhFeatureStateAll(_nbhSelectedId, { hover: false, selected: false });
    }
    _nbhSelectedId = clickedId;
    _nbhHoveredId = clickedId;
    _setNbhFeatureStateAll(clickedId, { hover: true, selected: true });
    _showNbhPopup(map, clickedId, f.properties);
  });

  map.on('click', (e) => {
    if (!_neighborhoodsVisible || _nbhSelectedId === null) return;
    const hits = map.queryRenderedFeatures(e.point, { layers: ['nbh-fill'] });
    if (!hits.length) _clearNbhSelection();
  });
}

/**
 * Toggle neighborhood (DeSO/RegSO) borders, fill overlay, and name labels.
 * Fetches polygon + demographic data lazily from /api/neighborhoods on first show.
 * When visible, hovering an area highlights it with a translucent purple fill
 * and shows a popup with the area name and household/demographic summary.
 */
export async function toggleNeighborhoods() {
  if (!_map) return false;
  _neighborhoodsVisible = !_neighborhoodsVisible;

  if (_neighborhoodsVisible) {
    // Add layers if not yet created
    if (!_map.getSource('nbh-border-src')) {
      let areas;
      try {
        const res = await fetch('/api/neighborhoods');
        areas = await res.json();
      } catch { areas = []; }
      if (!areas.length) { _neighborhoodsVisible = false; return false; }

      const features = [];
      const labelFeatures = [];
      for (let i = 0; i < areas.length; i++) {
        const a = areas[i];
        const coords = a.gtype === 'MultiPolygon' ? a.polygon : [a.polygon];
        const geomType = a.gtype === 'MultiPolygon' ? 'MultiPolygon' : 'Polygon';
        const geomCoords = a.gtype === 'MultiPolygon' ? coords : coords[0];
        features.push({
          type: 'Feature',
          properties: {
            fid:        i,
            name:       a.name || '',
            pop:        a.pop || 0,
            buildings:  a.buildings || 0,
            child:      a.age?.child || 0,
            adult:      a.age?.adult || 0,
            elder:      a.age?.elder || 0,
            male:       a.male || 0,
            female:     a.female || 0,
            avg_income: a.avg_income || 0,
            households: a.households || 0,
            // 6-vector country-of-birth distribution. Stringified for the
            // GeoJSON properties bag (MapLibre flattens arrays oddly).
            origin:     a.origin ? JSON.stringify(a.origin) : '',
          },
          geometry: { type: geomType, coordinates: geomCoords },
        });
        // Compute centroid from first ring of first polygon
        const ring = a.gtype === 'MultiPolygon' ? coords[0][0] : coords[0];
        if (ring && ring.length > 2) {
          let cx = 0, cy = 0;
          for (const pt of ring) { cx += pt[0]; cy += pt[1]; }
          cx /= ring.length; cy /= ring.length;
          labelFeatures.push({
            type: 'Feature',
            properties: { name: a.name },
            geometry: { type: 'Point', coordinates: [cx, cy] },
          });
        }
      }

      _map.addSource('nbh-border-src', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features },
        promoteId: 'fid',
      });

      // Translucent purple fill — base nearly invisible, hover light, selected darker
      _map.addLayer({
        id: 'nbh-fill', type: 'fill', source: 'nbh-border-src',
        paint: {
          'fill-color': [
            'case',
            ['boolean', ['feature-state', 'selected'], false],
            '#7c3aed',   // selected — deeper purple
            '#a78bfa',   // default / hover — lighter purple
          ],
          'fill-opacity': [
            'case',
            ['boolean', ['feature-state', 'selected'], false],
            0.38,   // selected — more opaque
            ['boolean', ['feature-state', 'hover'], false],
            0.25,   // hovered
            0.05,   // default — barely visible tint
          ],
        },
      });

      // Dashed border lines
      _map.addLayer({
        id: 'nbh-border', type: 'line', source: 'nbh-border-src',
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint: {
          'line-color': '#a78bfa',
          'line-width': 2,
          'line-opacity': 0.7,
          'line-dasharray': [3, 2],
        },
      });

      _map.addSource('nbh-label-src', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: labelFeatures },
      });
      _map.addLayer({
        id: 'nbh-labels', type: 'symbol', source: 'nbh-label-src',
        layout: {
          'text-field': ['get', 'name'],
          'text-size': 11,
          'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
          'text-allow-overlap': false,
          'text-ignore-placement': false,
        },
        paint: {
          'text-color': '#c4b5fd',
          'text-halo-color': 'rgba(15,23,42,0.85)',
          'text-halo-width': 1.5,
        },
      });

      // ── Hover + click interaction ────────────────────────────────────

      // Pre-compute centroid LngLat per feature for stable popup anchoring
      const centroids = {};
      for (const f of features) {
        const fid = f.properties.fid;
        const coords = f.geometry.type === 'MultiPolygon'
          ? f.geometry.coordinates[0][0]
          : f.geometry.coordinates[0];
        if (coords && coords.length > 2) {
          let cx = 0, cy = 0;
          for (const pt of coords) { cx += pt[0]; cy += pt[1]; }
          centroids[fid] = [cx / coords.length, cy / coords.length];
        }
      }
      _nbhCentroids = centroids;

      // Bind hover/click on every map currently registered for nbh sync
      // (primary + any secondary that compare-view-b.js has attached).
      for (const m of _allNbhMaps()) {
        if (m.getSource('nbh-border-src')) _bindNbhInteractions(m);
      }
    }
    _setNbhLayoutAll('nbh-fill',   'visibility', 'visible');
    _setNbhLayoutAll('nbh-border', 'visibility', 'visible');
    _setNbhLayoutAll('nbh-labels', 'visibility', 'visible');
  } else {
    _clearNbhSelection();
    _setNbhLayoutAll('nbh-fill',   'visibility', 'none');
    _setNbhLayoutAll('nbh-border', 'visibility', 'none');
    _setNbhLayoutAll('nbh-labels', 'visibility', 'none');
  }
  return _neighborhoodsVisible;
}

// ── Custom Layer ──────────────────────────────────────────────────────────────

function _buildCustomLayer(cityLayout, resolveReady) {
  return {
    id:            'epidemic-buildings',
    type:          'custom',
    renderingMode: '3d',

    onAdd(map, gl) {
      initView(map, gl, _mc, _scale);
      resolveReady();
    },

    render(gl, matrix) {
      renderScene(matrix, _mc, _scale);
      _map.triggerRepaint();
    },
  };
}
