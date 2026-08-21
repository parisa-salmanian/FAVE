// Map lasso selection (d3-lasso on top of the deck.gl canvas).
// Extracted from main.js; loaded before main.js.

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
  if (!feature) return null;
  const res = resolveMezoResolution();
  if (res == null) return null;
  // Cache the resolved cell per building per resolution. Selection expansion can
  // call this once per building (tens of thousands); recomputing centroid + h3
  // every time is what froze the tab. Invalidated automatically when res changes.
  const cached = feature.__mezoHexCache;
  if (cached && cached.res === res) return cached.hex;
  const coords = buildingCentroid(feature);
  if (!Array.isArray(coords)) return null;
  let hex = null;
  try {
    hex = h3LatLngToCell(window.h3, coords[1], coords[0], res);
  } catch (_) {
    hex = null;
  }
  feature.__mezoHexCache = { res, hex };
  return hex;
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
  if (!entities.length || !allBuildings.length) return selected;

  // Bucket entities by type ONCE, then resolve hex-cell and polygon entities in a
  // SINGLE pass over the buildings. The old code looped every building for every
  // entity — O(entities × buildings), ~120M ops for a large hex selection, which
  // froze the tab. Building references use a Set for O(1) membership.
  const buildingRefSet = new Set(allBuildings);
  const hexTargets = new Set();
  const polyEntities = [];
  entities.forEach((entity) => {
    if (!entity) return;
    if (buildingRefSet.has(entity)) { addBuilding(entity); return; }
    const maybeHex = entity?.hex || entity?.properties?.hex;
    if (maybeHex) { hexTargets.add(maybeHex); return; }
    if (entity?.geometry) polyEntities.push(entity);
  });

  if (!hexTargets.size && !polyEntities.length) return selected;

  for (const building of allBuildings) {
    if (seen.has(building)) continue;
    if (hexTargets.size) {
      const hx = mezoHexForBuilding(building);
      if (hx && hexTargets.has(hx)) { addBuilding(building); continue; }
    }
    if (polyEntities.length) {
      const c = buildingCentroid(building);
      if (!Array.isArray(c)) continue;
      const pt = turf.point(c);
      for (const poly of polyEntities) {
        try {
          if (turf.booleanPointInPolygon(pt, poly)) { addBuilding(building); break; }
        } catch (_) { /* ignore */ }
      }
    }
  }

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
  // Only mark the ACTIVE aggregate. Marking both used to run districtForBuilding
  // (point-in-polygon) over every selected building even in hex mode — ~680k turf
  // calls for a large selection, a major freeze. The other scale re-marks itself
  // from the _drSelected buildings when refreshDistrictScores/refreshMezoScores
  // runs on a scale switch, so nothing is lost.
  const mode = (typeof currentDRDataMode === 'function') ? currentDRDataMode() : null;

  if (mode === 'district') {
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
    return;
  }

  if (mode === 'mezo') {
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
  // A non-empty building set that resolves to zero plot entities is a FAILED
  // sync (stale objects after a re-aggregation), not a user clear — keep the
  // persisted cohort ids so restoreDRSelectionFromIds can still recover it.
  applyDRSelection(idx, {
    skipMapSync: preserveMapSelection, append,
    keepCohortOnEmpty: selectedBuildings.length > 0 && idx.length === 0
  });
}

function applyMapSelection(selected, opts = {}) {
  const { append = false, skipDRSync = false, skipParallelSync = false } = opts;
  parallelCoordsForceEmptySelection = false;
  const existingSelected = append
    ? (baseCityFC?.features || []).filter(f => f?.properties?._drSelected)
    : [];
  const nextSelected = selectedBuildingsFromEntities(existingSelected);

  if (Array.isArray(selected)) {
    // Set-based dedup: Array.includes here was O(n²) and froze on large
    // (tens-of-thousands) building selections.
    const nextSet = new Set(nextSelected);
    selectedBuildingsFromEntities(selected).forEach((feat) => {
      if (feat && !nextSet.has(feat)) { nextSet.add(feat); nextSelected.push(feat); }
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
    // Cached ring-average centroid (was turf.centroid per building — a full
    // polygon centroid over ~58k features on every map lasso).
    const c = buildingCentroid(f);
    if (!Array.isArray(c)) continue;
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

