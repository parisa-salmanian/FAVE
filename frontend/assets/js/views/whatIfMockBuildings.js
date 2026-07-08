// What-if scenario — mock buildings draggable lasso for what-if analysis.
// Extracted from main.js; loaded before main.js.

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
  if (typeof clearWhatIfSeededRing === 'function') clearWhatIfSeededRing();
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

/* ============ Bridge: DR/map selection → what-if region ====================
 * Closes the analytical loop "select an underserved area in the DR view → see
 * WHY it is underserved (EBM/contrastive) → act on it". Instead of re-drawing
 * the same region by hand in the what-if tool, this loads the current selection
 * (buildings flagged _drSelected by the DR or Map lasso) as the what-if region,
 * so the planner only has to enter POI counts and place them. This is the link
 * that makes the multivariate views decision-driving rather than descriptive.
 * ------------------------------------------------------------------------- */
let _whatIfSeededRingMoveHandler = null;

function getSelectedBuildingFeaturesForWhatIf() {
  const out = [];
  const scan = (fc) => (fc?.features || []).forEach((f) => {
    if (f?.properties?._drSelected) out.push(f);
  });
  if (typeof baseCityFC !== 'undefined') scan(baseCityFC);
  if (!out.length && typeof districtFC !== 'undefined') scan(districtFC);
  return out;
}

// Build a closed lng/lat ring enclosing the selected features: convex hull of
// their centroids, buffered outward a little so placed POIs land inside the
// served area. Falls back to a buffered bbox for <3 / collinear points.
function ringFromSelectedFeatures(feats, bufferKm = 0.04) {
  try {
    const pts = turf.featureCollection(
      feats.map((f) => { try { return turf.centroid(f); } catch { return null; } }).filter(Boolean)
    );
    if (!pts.features.length) return null;
    let poly = pts.features.length >= 3 ? turf.convex(pts) : null;
    if (!poly) poly = turf.bboxPolygon(turf.bbox(pts));
    const buffered = turf.buffer(poly, bufferKm, { units: 'kilometers' }) || poly;
    const ring = buffered?.geometry?.coordinates?.[0];
    return (Array.isArray(ring) && ring.length >= 4) ? ring : null;
  } catch (e) {
    console.warn('[whatif-bridge] ring build failed', e);
    return null;
  }
}

function drawWhatIfSeededRing(ring) {
  const svg = ensureWhatIfLassoOverlay();
  if (!svg || !map || !Array.isArray(ring)) return;
  svg.selectAll('.whatif-seeded-ring').remove();
  const pts = ring
    .map(([lng, lat]) => { const p = map.project({ lng, lat }); return p ? [p.x, p.y] : null; })
    .filter(Boolean);
  if (pts.length < 3) return;
  const d = 'M' + pts.map((p) => p.join(',')).join('L') + 'Z';
  svg.append('path')
    .attr('class', 'whatif-seeded-ring')
    .attr('d', d)
    .style('fill', 'rgba(240,173,78,0.12)')
    .style('stroke', '#f0ad4e')
    .style('stroke-width', 2)
    .style('stroke-dasharray', '6 4')
    .style('pointer-events', 'none');
}

function clearWhatIfSeededRing() {
  if (typeof d3 !== 'undefined') {
    const svg = d3.select('#whatIfLassoOverlay');
    if (svg && svg.node()) svg.selectAll('.whatif-seeded-ring').remove();
  }
  if (map && _whatIfSeededRingMoveHandler) {
    map.off('move', _whatIfSeededRingMoveHandler);
    _whatIfSeededRingMoveHandler = null;
  }
}

function seedWhatIfFromSelection() {
  const feats = getSelectedBuildingFeaturesForWhatIf();
  if (!feats.length) {
    setWhatIfLassoStatus('Select an area first (DR lasso or Map lasso), then click "Use map selection".', true);
    return;
  }
  const ring = ringFromSelectedFeatures(feats);
  if (!ring) {
    setWhatIfLassoStatus('Could not build a region from the selection — select a few more buildings.', true);
    return;
  }
  if (whatIfLasso.active) setWhatIfLassoActive(false); // avoid a stale freehand lasso
  setWhatIfLassoClearDisabled(false);
  // Detach any previous move handler, then draw and attach a fresh one so the
  // outline stays aligned while the planner pans/zooms.
  if (map && _whatIfSeededRingMoveHandler) {
    map.off('move', _whatIfSeededRingMoveHandler);
    _whatIfSeededRingMoveHandler = null;
  }
  drawWhatIfSeededRing(ring);
  _whatIfSeededRingMoveHandler = () => drawWhatIfSeededRing(ring);
  if (map) map.on('move', _whatIfSeededRingMoveHandler);

  // Place POIs now if counts are set (the decision payoff: select → act →
  // recompute); otherwise applyWhatIfLassoFromRing just saves the region and
  // prompts for counts. Re-clicking the button after entering counts places them.
  const counts = (typeof getWhatIfMockTypeCounts === 'function') ? getWhatIfMockTypeCounts() : { total: 0 };
  applyWhatIfLassoFromRing(ring);
  if (!counts.total) {
    setWhatIfLassoStatus(
      `What-if region loaded from ${feats.length} selected building${feats.length !== 1 ? 's' : ''}. ` +
      'Set POI counts above and click again to place them (or ask the LLM).'
    );
  }
}

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
  await ensureForbiddenZones();
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

      // Forbidden-zone check via baked GeoJSON (works with raster basemap)
      if (isLngLatInForbiddenZone([ll.lng, ll.lat])) continue;

      const sizeRange = whatIfMockFootprintMax - whatIfMockFootprintMin;
      const size = whatIfMockFootprintMin + Math.random() * Math.max(0, sizeRange);
      const geom = createMockBuildingPolygon([ll.lng, ll.lat], size, 'random', whatIfMockShapeVariation);
      const mockType = queue[features.length] || 'generic';
      const candidate = {
        type: 'Feature',
        geometry: geom,
        properties: {}
      };

      // Check all corners of the building polygon against road/water layers (vector basemap only)
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
    // newbuildsFC can alias baseCityFC (same FC object — see loaders.js, which
    // guards with `newbuildsFC !== baseCityFC`). Push again only if it's a
    // distinct array, otherwise every mock building is added twice.
    if (newbuildsFC?.features && newbuildsFC.features !== baseCityFC.features) {
      newbuildsFC.features.push(...result.features);
    }
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

