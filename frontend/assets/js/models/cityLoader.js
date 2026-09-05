// Load buildings + districts + demographics for a city from local GeoJSON.
// Extracted from main.js; loaded before main.js.

/* ======================= Load city from local GeoJSON (OSM+GeoJSON mode) ======================= */
async function loadCityLocal(cityKey) {
  const url = BUILDING_URL_BY_CITY_KEY[cityKey];
  if (!url) {
    alert(`No local building file for "${cityKey}".`);
    return;
  }

  try {
    const displayName = LOCAL_CITY_NAMES[cityKey] || cityKey;
    lastCityName = displayName;
    applyDistrictDatasetForCity(displayName);
    jobStatusEl && (jobStatusEl.textContent = 'Loading local…');

    const fc = await fetch(url).then(r => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    });

    // Göteborg's extract (283k features) exceeds what the tab can hold — 48% are
    // Komplementbyggnad (sheds/garages: no residents, no service role). Dropping
    // them lands at ~148k, inside the proven range (Stockholm loads at 242k).
    // Göteborg-only: other cities load fine, and dropping komplement city-wide
    // would shift already-reported metrics.
    if (cityKey === 'goteborg' && Array.isArray(fc.features)) {
      const before = fc.features.length;
      fc.features = fc.features.filter(f =>
        !String(f?.properties?.andamal1 || '').startsWith('Komplementbyggnad'));
      console.log(`[cityLoader] goteborg: dropped ${before - fc.features.length} komplement buildings, kept ${fc.features.length}`);
    }

    await alignBuildingCoverageToDistricts(fc, 'local buildings');
    baseCityFC  = fc;
    districtLandClipSignature = '';
    newbuildsFC = fc;
    poiCache = {};   // clear so local POI extraction runs fresh

    refreshBuildingTypeDropdown();
    setSelectedBuildingType('', true);
    jobStatusEl && (jobStatusEl.textContent = '');
    fitToData(baseCityFC);
    if (districtView) await refreshDistrictScores();
    if (mezoView) await refreshMezoScores();
    updateLayers();
    updateDRAndPCBadges();
  } catch (err) {
    console.error('loadCityLocal failed:', err);
    jobStatusEl && (jobStatusEl.textContent = 'Failed');
    alert(`Failed to load local data for ${cityKey}: ${err?.message || err}`);
  }
}

function cityGeoJSONURL(city) {
  const key = normalizeCityKey(city);
  return OSM_GEOJSON_CITY_URLS[key] || null;
}

function normalizeHybridFeatureProps(props = {}) {
  const out = { ...props };
  const name = out.name || out.namn || out.objektnamn || out.byggnadsnamn || out.byggnadsnamn1 || out.byggnadsnamn2 || out.byggnadsnamn3;
  if (name && !out.name) out.name = name;

  const building = out.building || out.objekttyp || out.byggnadstyp || out.typ;
  if (building && !out.building) out.building = building;

  if (!out.category && building) {
    out.category = String(building);
  }

  const purposes = [out.andamal1, out.andamal2, out.andamal3, out.andamal4, out.andamal5]
    .filter((v) => v != null && String(v).trim())
    .join(' ; ');
  if (purposes && !out.__purpose_text) out.__purpose_text = purposes;
  return out;
}

function normalizeHybridGeoJSON(fc) {
  if (!fc || !Array.isArray(fc.features)) return fc;
  return {
    ...fc,
    features: fc.features.map((feature) => ({
      ...feature,
      properties: normalizeHybridFeatureProps(feature?.properties || {})
    }))
  };
}

async function loadCityHybrid(city) {
  try {
    applyDistrictDatasetForCity(city);
    loadCityBtn && (loadCityBtn.disabled = true);
    jobStatusEl && (jobStatusEl.textContent = 'Loading GeoJSON…');

    const cityURL = cityGeoJSONURL(city);
    if (!cityURL) {
      throw new Error(`No GeoJSON dataset configured for "${city}". Add it to OSM_GEOJSON_CITY_URLS in main.js.`);
    }

    const rawFC = await fetch(cityURL).then((r) => {
      if (!r.ok) throw new Error(`GeoJSON fetch failed (${r.status})`);
      return r.json();
    });
    const fc = normalizeHybridGeoJSON(rawFC);
    await alignBuildingCoverageToDistricts(fc, 'OSM+GeoJSON buildings');
    baseCityFC = fc;
    newbuildsFC = fc;
    refreshBuildingTypeDropdown();
    setSelectedBuildingType('', true);
    jobStatusEl && (jobStatusEl.textContent = 'Ready');
    fitToData(baseCityFC);
    if (districtView) await refreshDistrictScores();
    if (mezoView) await refreshMezoScores();
    updateLayers();
    updateDRAndPCBadges();
  } catch (err) {
    console.error(err);
    jobStatusEl && (jobStatusEl.textContent = 'Failed');
    alert(`Failed to load GeoJSON for ${city}: ${err?.message || err}`);
  } finally {
    loadCityBtn && (loadCityBtn.disabled = false);
  }
}

function fitToData(fc) {
  try {
    const [minX, minY, maxX, maxY] = turf.bbox(fc);
    map.fitBounds([[minX, minY], [maxX, maxY]], { padding: 40, duration: 800 });
  } catch {}
}

// The camera the map starts on, and the one the topbar's reset button
// restores. Kept here next to fitToData so the "home" framing is defined once.
const HOME_PITCH = 45;
const HOME_BEARING = 0;

/**
 * Re-frame the current city exactly as it looks right after loading: the whole
 * dataset in view, with the default pitch and bearing. Panning, zooming or a
 * fly-to elsewhere is always one click away from this same view.
 */
function resetMapView({ duration = 700 } = {}) {
  if (!map) return;
  try {
    const fc = baseCityFC;
    if (!fc?.features?.length) {
      // Nothing loaded yet — at least straighten the camera out.
      map.easeTo({ pitch: HOME_PITCH, bearing: HOME_BEARING, duration });
      return;
    }
    const [minX, minY, maxX, maxY] = turf.bbox(fc);
    const bounds = [[minX, minY], [maxX, maxY]];
    // cameraForBounds gives centre+zoom without committing the move, so pitch
    // and bearing can be reset in the same easeTo instead of a second hop.
    const cam = map.cameraForBounds(bounds, { padding: 40 });
    if (cam) {
      map.easeTo({
        center: cam.center, zoom: cam.zoom,
        pitch: HOME_PITCH, bearing: HOME_BEARING, duration,
      });
    } else {
      map.fitBounds(bounds, { padding: 40, bearing: HOME_BEARING, duration });
    }
  } catch (err) {
    console.warn('[FAVE] reset map view failed:', err);
  }
}

function setMode(mode) {
  viewMode = mode;
  modeAllBtn?.classList.toggle('active', mode === 'all');
  modeNewBtn?.classList.toggle('active', mode === 'new');
  yearControlsWrap?.classList.toggle('d-none-important', mode !== 'new');
  clearSelections();
  closePopup();
  updateLayers();
}

function clearSelections() {
  firstFeat = null; secondFeat = null; routeGeoJSON = null;
  if (distanceOut) distanceOut.textContent = '—';
}

function resetUIState() {
  if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
  if (giniOut) giniOut.textContent = '—';
  if (overallGiniOut) overallGiniOut.textContent = '—';
  document.querySelectorAll('.poi-check').forEach(el => { el.checked = false; });
  document.querySelectorAll('.poi-weight').forEach(el => {
    const cat = el.getAttribute('data-cat');
    const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
    if (badge) badge.textContent = el.value;
  });
  // City switch wipes the POI selection — drop the rail-poi orange.
  if (typeof window.syncPOIRailActive === 'function') window.syncPOIRailActive();

  clearFairness(true);
  closePopup();
  hideSidePanel();
  clearDRProjection(false);
  setSelectedBuildingType('', true);
  setDistrictView(false);
  setMezoView(false);
  clearWhatIfSuggestions();
  refreshWhatIfSuggestCategories([whatIfType]);
}

