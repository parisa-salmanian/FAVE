// Generic data loaders (GeoJSON fetch, JSON helpers, byggnad enrichment).
// Extracted from main.js; loaded before main.js.

/* ======================= Loaders ======================= */
async function loadSentinelData() {
  const [city, stats] = await Promise.all([
    fetch(CITY_URL).then(r => r.json()),
    fetch(STATS_URL).then(r => r.json()).catch(()=>null)
  ]);
  baseCityFC  = city;
  districtLandClipSignature = '';
  newbuildsFC = stats || city;
  await alignBuildingCoverageToDistricts(baseCityFC, 'base buildings');
  if (newbuildsFC !== baseCityFC) {
    await alignBuildingCoverageToDistricts(newbuildsFC, 'new buildings');
  }
  refreshBuildingTypeDropdown();
  setSelectedBuildingType('', true);
  fitToData(baseCityFC);
  if (districtView) await refreshDistrictScores();
  if (mezoView) await refreshMezoScores();
  updateLayers();
  updateDRAndPCBadges();
}

/**
 * Enrich OSM-fetched buildings with properties from a local building GeoJSON
 * (e.g. byggnad_malmo.geojson). Matches features by proximity of centroids.
 * Properties from the local file are merged into the OSM feature's properties
 * without overwriting existing OSM keys.
 */
function enrichOSMWithLocalBuildings(osmFC, localFC) {
  if (!osmFC?.features?.length || !localFC?.features?.length) return;

  // Build a simple spatial index: round centroids to ~100 m grid cells
  const GRID_PRECISION = 3; // ~110 m at equator
  const localIndex = new Map();
  for (const feat of localFC.features) {
    try {
      const c = turf.centroid(feat).geometry.coordinates;
      const key = `${c[0].toFixed(GRID_PRECISION)},${c[1].toFixed(GRID_PRECISION)}`;
      if (!localIndex.has(key)) localIndex.set(key, []);
      localIndex.get(key).push({ feat, lon: c[0], lat: c[1] });
    } catch { /* skip malformed */ }
  }

  const MATCH_THRESHOLD_M = 30; // max distance to consider a match
  let enriched = 0;

  for (const osmFeat of osmFC.features) {
    try {
      const c = turf.centroid(osmFeat).geometry.coordinates;
      const keyBase = [c[0].toFixed(GRID_PRECISION), c[1].toFixed(GRID_PRECISION)];

      // Check the cell and its 8 neighbors
      let bestDist = Infinity;
      let bestLocal = null;
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          const probe = `${(parseFloat(keyBase[0]) + dx * Math.pow(10, -GRID_PRECISION)).toFixed(GRID_PRECISION)},${(parseFloat(keyBase[1]) + dy * Math.pow(10, -GRID_PRECISION)).toFixed(GRID_PRECISION)}`;
          const bucket = localIndex.get(probe);
          if (!bucket) continue;
          for (const entry of bucket) {
            const d = turf.distance([c[0], c[1]], [entry.lon, entry.lat], { units: 'meters' });
            if (d < bestDist) {
              bestDist = d;
              bestLocal = entry.feat;
            }
          }
        }
      }

      if (bestLocal && bestDist <= MATCH_THRESHOLD_M) {
        const localProps = bestLocal.properties || {};
        const osmProps = osmFeat.properties || {};
        // Merge local props without overwriting existing OSM props
        for (const [k, v] of Object.entries(localProps)) {
          if (!(k in osmProps) && v != null) {
            osmProps[k] = v;
          }
        }
        // Also store the local height if present and OSM lacks it
        if (!osmProps.height && (localProps.height || localProps.HOJD || localProps.hojd)) {
          osmProps.height = localProps.height || localProps.HOJD || localProps.hojd;
        }
        enriched++;
      }
    } catch { /* skip */ }
  }

  console.log(`enrichOSMWithLocalBuildings: matched ${enriched} of ${osmFC.features.length} buildings`);
}

/**
 * Enrich OSM buildings with properties from a local GeoJSON (e.g. byggnad_malmo.geojson).
 * Matches by centroid proximity (~30 m threshold).
 */
function enrichOSMWithLocalBuildings(osmFC, localFC) {
  if (!osmFC?.features?.length || !localFC?.features?.length) return;

  // Keys where local Lantmäteriet data should ALWAYS override OSM values
  // because OSM typically has generic/useless values for these
  const LOCAL_PREFERRED_KEYS = new Set([
    'objekttyp',      // OSM has "yes", local has "Bostad"/"Samhällsfunktion"/etc.
    'objekttypnr',    // OSM has generic code, local has proper Lantmäteriet code
    'andamal1',       // OSM may be empty/wrong, local has Swedish building purpose
    'husnummer',      // local is authoritative
    'huvudbyggnad',   // local is authoritative
    'insamlingslage'  // local is authoritative
  ]);

  const GRID = 3;
  const localIndex = new Map();
  for (const feat of localFC.features) {
    try {
      const c = turf.centroid(feat).geometry.coordinates;
      const key = `${c[0].toFixed(GRID)},${c[1].toFixed(GRID)}`;
      if (!localIndex.has(key)) localIndex.set(key, []);
      localIndex.get(key).push({ feat, lon: c[0], lat: c[1] });
    } catch { /* skip */ }
  }

  const THRESHOLD_M = 30;
  let enriched = 0;
  const step = Math.pow(10, -GRID);

  for (const osmFeat of osmFC.features) {
    try {
      const c = turf.centroid(osmFeat).geometry.coordinates;
      const baseLon = parseFloat(c[0].toFixed(GRID));
      const baseLat = parseFloat(c[1].toFixed(GRID));

      let bestDist = Infinity, bestLocal = null;
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          const probe = `${(baseLon + dx * step).toFixed(GRID)},${(baseLat + dy * step).toFixed(GRID)}`;
          const bucket = localIndex.get(probe);
          if (!bucket) continue;
          for (const entry of bucket) {
            const d = turf.distance([c[0], c[1]], [entry.lon, entry.lat], { units: 'meters' });
            if (d < bestDist) { bestDist = d; bestLocal = entry.feat; }
          }
        }
      }

      if (bestLocal && bestDist <= THRESHOLD_M) {
        const lp = bestLocal.properties || {};
        const op = osmFeat.properties || (osmFeat.properties = {});
        for (const [k, v] of Object.entries(lp)) {
          if (v == null) continue;
          if (LOCAL_PREFERRED_KEYS.has(k)) {
            // Always use local value for these keys
            op[k] = v;
          } else if (!(k in op)) {
            // For other keys, only add if OSM doesn't have it
            op[k] = v;
          }
        }
        // Use byggnadsnamn1 as display name if OSM name is missing
        if (!op.name && lp.byggnadsnamn1) {
          op.name = lp.byggnadsnamn1;
        }
        if (!op.height && (lp.height || lp.HOJD || lp.hojd)) {
          op.height = lp.height || lp.HOJD || lp.hojd;
        }
        enriched++;
      }
    } catch { /* skip */ }
  }
  console.log(`enrichOSMWithLocalBuildings: matched ${enriched}/${osmFC.features.length}`);
}

/**
 * Extract total population number from a population entry.
 */
function extractDistrictTotalPopulation(entry) {
  if (!entry) return NaN;

  for (const key of ['total', 'Total', 'population', 'Population', 'totalt', 'Totalt']) {
    if (Number.isFinite(Number(entry[key]))) return Number(entry[key]);
  }

  const yearKeys = Object.keys(entry).filter(k => /^\d{4}$/.test(k)).sort();
  if (yearKeys.length) {
    const latestYear = yearKeys[yearKeys.length - 1];
    const yearData = entry[latestYear];
    if (Number.isFinite(Number(yearData))) return Number(yearData);
    if (yearData && typeof yearData === 'object') {
      for (const key of ['total', 'Total', 'population', 'totalt']) {
        if (Number.isFinite(Number(yearData[key]))) return Number(yearData[key]);
      }
      const nums = Object.values(yearData).filter(v => Number.isFinite(Number(v))).map(Number);
      if (nums.length) return nums.reduce((a, b) => a + b, 0);
    }
  }

  const ageGroupMap = getPopulationAgeGroupMap(entry);
  if (ageGroupMap) {
    let grandTotal = 0;
    for (const [, yearBuckets] of Object.entries(ageGroupMap)) {
      const yKeys = Object.keys(yearBuckets).filter(k => /^\d{4}$/.test(k)).sort();
      if (!yKeys.length) continue;
      const latest = yearBuckets[yKeys[yKeys.length - 1]];
      if (latest && typeof latest === 'object') {
        const vals = Object.values(latest).filter(v => Number.isFinite(Number(v))).map(Number);
        grandTotal += vals.reduce((a, b) => a + b, 0);
      } else if (Number.isFinite(Number(latest))) {
        grandTotal += Number(latest);
      }
    }
    if (grandTotal > 0) return grandTotal;
  }

  return NaN;
}

/**
 * Distribute district-level population to buildings proportionally by footprint area.
 * Writes __buildingPopShare, __buildingArea, population_group, resident_group
 * onto each building's properties for use by the gravity model.
 */
async function distributePopulationToBuildings(buildingsFC) {
  if (!buildingsFC?.features?.length) return;

  let popData;
  try {
    popData = await ensureGenderAgePopulationData();
  } catch (err) {
    console.warn('distributePopulationToBuildings: could not load population data', err);
    return;
  }
  if (!popData) return;

  try {
    await ensureDistrictData();
  } catch (err) {
    console.warn('distributePopulationToBuildings: no district data', err);
    return;
  }

  const districts = (districtFC?.features || []).filter(
    f => f?.geometry && (f.geometry.type === 'Polygon' || f.geometry.type === 'MultiPolygon')
  );
  if (!districts.length) return;

  const t0 = performance.now();

  const districtMeta = districts.map(d => {
    const props = d.properties || {};
    const regsoCode = regsoCodeFromProps(props);
    const dName = props.__districtName || districtNameOf(props);
    let bbox = null;
    try { bbox = turf.bbox(d); } catch { /* skip */ }
    return { feature: d, regsoCode, dName, bbox, buildings: [], totalArea: 0 };
  });

  for (const feat of buildingsFC.features) {
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; }
    catch { continue; }

    let area;
    try { area = Math.max(turf.area(feat), 1); }
    catch { area = 1; }

    for (const dm of districtMeta) {
      if (dm.bbox) {
        const [minX, minY, maxX, maxY] = dm.bbox;
        if (centroid[0] < minX || centroid[0] > maxX || centroid[1] < minY || centroid[1] > maxY) continue;
      }
      try {
        if (turf.booleanPointInPolygon(turf.point(centroid), dm.feature)) {
          dm.buildings.push({ feat, area });
          dm.totalArea += area;
          break;
        }
      } catch { /* skip */ }
    }
  }

  let assignedCount = 0;
  let matchedDistricts = 0;
  for (const dm of districtMeta) {
    if (!dm.buildings.length || dm.totalArea <= 0) continue;

    const entry = lookupPopulationEntry(popData, dm.regsoCode);
    const totalPop = extractDistrictTotalPopulation(entry);
    if (!Number.isFinite(totalPop) || totalPop <= 0) {
      console.warn(`distributePopulation: no population for district "${dm.dName}" (regso: ${dm.regsoCode})`);
      continue;
    }
    matchedDistricts++;

    for (const bm of dm.buildings) {
      const share = (bm.area / dm.totalArea) * totalPop;
      const props = bm.feat.properties || (bm.feat.properties = {});
      props.__districtPop = totalPop;
      props.__buildingPopShare = share;
      props.__buildingArea = bm.area;
      if (!props.__districtName) props.__districtName = dm.dName;

      const popDensity = share / Math.max(1, bm.area);
      if (popDensity > 0.05) {
        props.population_group = 'Residential';
        props.resident_group = 'Residential';
      } else if (popDensity > 0.01) {
        props.population_group = 'Commercial';
        props.resident_group = 'Commercial';
      } else {
        props.population_group = 'Other / unknown';
        props.resident_group = 'Other / unknown';
      }
      assignedCount++;
    }
  }

  const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
  console.log(`distributePopulationToBuildings: ${assignedCount}/${buildingsFC.features.length} buildings, ${matchedDistricts}/${districtMeta.length} districts with pop data, in ${elapsed}s`);
}

async function loadCityOSM(city) {
  console.trace('>>> loadCityOSM called for:', city, 'at', Date.now());
  try {
    const _t0 = performance.now();
    const _elapsed = () => ((performance.now() - _t0) / 1000).toFixed(1) + 's';

    applyDistrictDatasetForCity(city);

    // Invalidate population cache so correct city file is fetched
    genderAgePopulation = null;
    genderAgePopulationPromise = null;
    _genderAgePopCityKey = null;

    loadCityBtn && (loadCityBtn.disabled = true);
    jobStatusEl && (jobStatusEl.textContent = 'Starting…');

    console.log(`[loadCityOSM] Starting ${city}...`);

    const years = [2021,2022,2023,2024];
    const { buildingsUrl } = await runCityJob(city, years, step => { if (jobStatusEl) { jobStatusEl.textContent = step; jobStatusEl.title = step; } });
    console.log(`[loadCityOSM] runCityJob done at ${_elapsed()}`);

    const fc = await fetch(buildingsUrl).then(r => r.json());
    console.log(`[loadCityOSM] Buildings fetched: ${fc?.features?.length} at ${_elapsed()}`);

    // Enrich with local building details (byggnad_malmo.geojson etc.)
    const cityKey = districtCityKeyFromInput(city);
    if (cityKey && BUILDING_URL_BY_CITY_KEY[cityKey]) {
      showGlobalSpinner('Loading city…');
      jobStatusEl && (jobStatusEl.textContent = 'Loading building details…');
      try {
        const localFC = await fetch(BUILDING_URL_BY_CITY_KEY[cityKey]).then(r => r.json());
        enrichOSMWithLocalBuildings(fc, localFC);
        console.log(`[loadCityOSM] Building enrichment done at ${_elapsed()}`);
      } catch (localErr) {
        console.warn('Could not load local building details:', localErr);
      }
    }

    await alignBuildingCoverageToDistricts(fc, 'OSM buildings');
    console.log(`[loadCityOSM] District alignment done at ${_elapsed()}`);

    // Distribute population from districts to buildings for gravity model
    baseCityFC = fc;
    districtLandClipSignature = '';
    jobStatusEl && (jobStatusEl.textContent = 'Distributing population…');
    try {
      await distributePopulationToBuildings(fc);
      console.log(`[loadCityOSM] Population distribution done at ${_elapsed()}`);
    } catch (popErr) {
      console.warn('Population distribution failed (non-fatal):', popErr);
    }

    newbuildsFC = fc;
    refreshBuildingTypeDropdown();
    setSelectedBuildingType('', true);
    jobStatusEl && (jobStatusEl.textContent = 'Ready');
    hideGlobalSpinner();
    fitToData(baseCityFC);
    if (districtView) await refreshDistrictScores();
    if (mezoView) await refreshMezoScores();
    updateLayers();
    updateDRAndPCBadges();
    console.log(`[loadCityOSM] Fully done at ${_elapsed()}`);
  } catch (err) {
    hideGlobalSpinner();
    console.error('[loadCityOSM] ERROR:', err);
    jobStatusEl && (jobStatusEl.textContent = 'Failed');
    alert(`Failed to load ${city}: ${err?.message || err}`);
  } finally {
    loadCityBtn && (loadCityBtn.disabled = false);
  }
}

