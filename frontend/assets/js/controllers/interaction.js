// Click vs popup interaction dispatcher (single click → popup,
// double click or shift-click → selection, etc.). Extracted from main.js.

/* ======================= Interaction: popup vs select ======================= */
let clickTimerId = null;
const DOUBLE_CLICK_MS = 280;

function handleClick(info, event) {
  const feat = info && info.object;
  if (!feat) return;
  noteFeatureClick();

  const e = event && event.srcEvent;
  const lngLat = info.coordinate || null;

  const selectingForDistance = !!(e && e.shiftKey);
  if (selectingForDistance) { selectForDistance(feat); return; }
  const appendSelection = isAdditiveSelectionEvent(e);

  if (clickTimerId) {
    clearTimeout(clickTimerId);
    clickTimerId = null;
    selectForDistance(feat);
    return;
  }

  clickTimerId = setTimeout(async () => {
    clickTimerId = null;

    if (lngLat) {
      // Snapshot camera state before any side-effect runs. Some downstream
      // call (popup focus, deck.gl pickInfo, applyMapSelection → DR sync,
      // inspector slide-in) was nudging the viewport on click; rather than
      // chase every cause, freeze zoom/center/bearing/pitch and restore
      // them after the click flow settles. Using requestAnimationFrame
      // twice covers both the synchronous DOM mutations and the next paint.
      const camBefore = USE_CAMERA_LOCK_ON_CLICK ? snapshotCamera() : null;

      // OSRM-on-click block intentionally removed: with USE_TRAVEL_TIME=false
      // the fairness pipeline is fully offline, so reaching out to OSRM here
      // (a) violates the no-runtime-API rule and (b) was the slow network
      // call that made the click feel laggy.
      showPopup(feat, lngLat);
      applyMapSelection([feat], { append: appendSelection });
      window.faveInspector?.setBuildingSelection?.(feat);

      if (camBefore) {
        requestAnimationFrame(() => requestAnimationFrame(() => restoreCamera(camBefore)));
      }
    }
  }, DOUBLE_CLICK_MS);
}

const USE_CAMERA_LOCK_ON_CLICK = true;
function snapshotCamera() {
  if (!map || typeof map.getZoom !== 'function') return null;
  const c = map.getCenter();
  return {
    zoom:    map.getZoom(),
    center:  [c.lng, c.lat],
    bearing: map.getBearing(),
    pitch:   map.getPitch(),
  };
}
function restoreCamera(cam) {
  if (!cam || !map) return;
  // Only correct when something actually moved — avoids fighting valid
  // user-initiated camera changes during the same animation frame.
  const moved =
    Math.abs(map.getZoom() - cam.zoom) > 0.001 ||
    Math.abs(map.getBearing() - cam.bearing) > 0.001 ||
    Math.abs(map.getPitch() - cam.pitch) > 0.001;
  const cur = map.getCenter();
  const dxd = Math.abs(cur.lng - cam.center[0]) > 1e-7;
  const dyd = Math.abs(cur.lat - cam.center[1]) > 1e-7;
  if (!moved && !dxd && !dyd) return;
  map.jumpTo({ center: cam.center, zoom: cam.zoom, bearing: cam.bearing, pitch: cam.pitch });
}

function selectForDistance(feat) {
  if (clickTimerId) { clearTimeout(clickTimerId); clickTimerId = null; }
  if (firstFeat === null) firstFeat = feat;
  else if (secondFeat === null && feat !== firstFeat) secondFeat = feat;
  else { firstFeat = feat; secondFeat = null; routeGeoJSON = null; if (distanceOut) distanceOut.textContent = '—'; }
  updateRouteIfReady();
}

function updateRouteIfReady() {
  if (!firstFeat || !secondFeat) { updateLayers(); return; }
  const c0 = turf.centroid(firstFeat).geometry.coordinates;
  const c1 = turf.centroid(secondFeat).geometry.coordinates;
  const osrmUrl = `${ROUTING_BASE_URL}/route/v1/${ROUTING_PROFILE}/${c0[0]},${c0[1]};${c1[0]},${c1[1]}?overview=full&geometries=geojson`;

  fetch(osrmUrl).then(r => r.json()).then(osrm => {
    if (osrm.code === 'Ok' && osrm.routes.length) {
      routeGeoJSON = { type:'FeatureCollection', features:[{ type:'Feature', geometry: osrm.routes[0].geometry }] };
      const km = (osrm.routes[0].distance / 1000).toFixed(2);
      if (distanceOut) distanceOut.textContent = `${km} km`;
    } else {
      routeGeoJSON = null; if (distanceOut) distanceOut.textContent = 'n/a';
    }
    updateLayers();
  }).catch(err => {
    console.error('OSRM error', err);
    routeGeoJSON = null; if (distanceOut) distanceOut.textContent = 'n/a';
    updateLayers();
  });
}

