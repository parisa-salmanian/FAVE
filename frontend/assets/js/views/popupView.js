// Popup rendering (map building popups + the per-building 'side panel' info).
// Extracted from main.js. Loaded as a classical script BEFORE main.js so the
// shared top-level state (map, overlay, currentPopup) is visible across the
// Realm. Functions reference main.js globals (lastCityName, fairActive, etc.)
// at call time, which resolves once main.js has finished its top-level pass.

/* ======================= POPUP (concise + fairness) ======================= */
let map = null;
let overlay = null;
let currentPopup = null;
function closePopup() { if (currentPopup) { currentPopup.remove(); currentPopup = null; } }
function fmtValue(v) { return (v===null || v===undefined || v==='') ? '—' : String(v); }
function prettyPOIName(cat) {
  const dict = {
    __overall:'Overall',
    residential:'Residential',
    grocery:'Grocery', hospital:'Hospital', pharmacy:'Pharmacy', dentistry:'Dentistry',
    healthcare_center:'HC Center', veterinary:'Veterinary', university:'University',
    kindergarten:'Kindergarten', school_primary:'Primary.S',
    school_high:'High.S', mix:'Custom mix'
    };
return dict[cat] || cat;
}

function buildIfCityDebugReadout(props) {
  if (!props) return '';
  if (!props.__ifcity) {
    if (fairActive && fairCategory && props.fair) {
      return '<div class="small text-muted mt-2">Debug unavailable for this feature. Re-run fairness and click a building.</div>';
    }
    return '';
  }

  const fm = props.fair_multi || {};
  const lines = [];
  const mixEntries =
    fairActive && fairCategory === 'mix' && Array.isArray(selectedPOIMix) && selectedPOIMix.length
      ? selectedPOIMix
      : [];
  const activeEntries = mixEntries.length
    ? mixEntries
    : (fairActive && fairCategory && fairCategory !== 'mix')
      ? [{ cat: fairCategory, weight: 1 }]
      : Object.keys(fm).map((cat) => ({ cat, weight: 1 }));

  activeEntries.forEach(({ cat, weight }) => {
    const access = Number(fm?.[cat]?.access);
    if (!Number.isFinite(access)) return;
    const w = Number.isFinite(weight) ? weight : 1;
    lines.push(`${prettyPOIName(cat)}: raw=${access.toFixed(4)}, w=${w.toFixed(2)}, weighted=${(w * access).toFixed(4)}`);
  });

  const utility = Number(props.__ifcity.utility);
  const equityWeight = Number(props.__ifcity.equity_weight);
  const benefit = Number(props.__ifcity.benefit);
  const normalizedScore = Number(props?.fair?.score);

  lines.push(`utility(sum weighted raw)=${Number.isFinite(utility) ? utility.toFixed(4) : '—'}`);
  lines.push(`demand weight=${Number.isFinite(props.__ifcity?.demand_weight) ? props.__ifcity.demand_weight.toFixed(3) : '1.000'}`);
  lines.push(`benefit=max(0,utility-baseline)*equity=${Number.isFinite(benefit) ? benefit.toFixed(4) : '—'}`);
  lines.push(`normalized score=${Number.isFinite(normalizedScore) ? normalizedScore.toFixed(4) : '—'}`);

  return `
    <div class="mt-2 pt-2 border-top">
      <div class="small text-muted">IF-City debug values</div>
      <pre class="small mb-0 mt-1" style="white-space:pre-wrap;line-height:1.25;">${lines.join('\n')}</pre>
    </div>`;
}

function noteFeatureClick() {
  lastFeatureClickAt = Date.now();
}

function buildBuildingTypeOptions(selected) {
  return BUILDING_TYPE_ORDER.map(type => {
    const isSelected = type === selected ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${type}</option>`;
  }).join('');
}

function buildWhatIfTypeOptions(selected) {
  const types = ['residential', ...ALL_CATEGORIES];
  return types.map(type => {
    const isSelected = type === selected ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function buildWhatIfSuggestCategoryOptions(selected = []) {
  const selectedSet = new Set(selected);
  return ALL_CATEGORIES.map(type => {
    const isSelected = selectedSet.has(type) ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function buildWhatIfSuggestCategoryOptions(selected = []) {
  const selectedSet = new Set(selected);
  return ALL_CATEGORIES.map(type => {
    const isSelected = selectedSet.has(type) ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function buildWhatIfSuggestCategoryOptions(selected = []) {
  const selectedSet = new Set(selected);
  return ALL_CATEGORIES.map(type => {
    const isSelected = selectedSet.has(type) ? ' selected' : '';
    return `<option value="${type}"${isSelected}>${prettyPOIName(type)}</option>`;
  }).join('');
}

function setWhatIfMode(mode) {
  whatIfMode = mode || 'off';
  if (map?.getCanvas) {
    const cursor = whatIfMode === 'add' ? 'crosshair' : '';
    map.getCanvas().style.cursor = cursor;
  }
  if (whatIfMode === 'off') {
    closePopup();
  }
}

function setWhatIfType(nextType) {
  whatIfType = nextType || ALL_CATEGORIES[0] || 'grocery';
  if (whatIfTypeSelect) whatIfTypeSelect.value = whatIfType;
  if (whatIfSuggestCategoriesSelect && !getSelectedWhatIfCategories().length) {
    refreshWhatIfSuggestCategories([whatIfType]);
  }
  clearWhatIfSuggestions();
}

function getActiveWhatIfCategory() {
  if (whatIfType) return whatIfType;
  if (fairActive && fairCategory) {
    if (fairCategory === 'mix') {
      if (selectedPOIMix?.length) {
        return selectedPOIMix.reduce((best, entry) => {
          if (!best || entry.weight > best.weight) return entry;
          return best;
        }, null)?.cat;
      }
      return whatIfType;
    }
    return fairCategory;
  }
  return whatIfType;
}

function detectPOICategoryFromProps(props) {
  if (!props) return '';
  if (props.whatif_poi) return props.whatif_poi;
  for (const cat of ALL_CATEGORIES) {
    if (buildingMatchesPOI(props, cat)) return cat;
  }
  return '';
}

function rememberWhatIfOriginal(props) {
  if (!props || props.__whatIfOriginal) return;
  const keys = [
    'amenity', 'shop', 'healthcare', 'building', 'name', 'category', 'category_label',
    'school:level', 'education:level', 'level', 'isced:level', 'whatif_poi', 'objekttyp'
  ];
  const original = { values: {}, has: {} };
  keys.forEach((key) => {
    original.has[key] = Object.prototype.hasOwnProperty.call(props, key);
    original.values[key] = props[key];
  });
  props.__whatIfOriginal = original;
}

function restoreWhatIfOriginal(props) {
  if (!props?.__whatIfOriginal) return;
  const { values, has } = props.__whatIfOriginal;
  Object.keys(has).forEach((key) => {
    if (has[key]) {
      props[key] = values[key];
    } else {
      delete props[key];
    }
  });
  delete props.__whatIfOriginal;
  delete props.__whatIf;
  delete props.whatif_poi;
}

function resetWhatIfChanges() {
  if (!baseCityFC?.features?.length) return;
  const remaining = [];
  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (props.__whatIfAdded) {
      continue;
    }
    if (props.__whatIfOriginal) {
      restoreWhatIfOriginal(props);
    } else {
      delete props.__whatIf;
      delete props.whatif_poi;
    }
    remaining.push(feat);
  }
  baseCityFC.features = remaining;
  if (newbuildsFC?.features?.length) {
    newbuildsFC.features = newbuildsFC.features.filter((feat) => remaining.includes(feat));
  }
  refreshBuildingTypeDropdown();
  clearWhatIfSuggestions();
  clearWhatIfMockBuildings();
  updateLayers();
}

function applyPOITags(props, cat) {
  if (!props) return;
  rememberWhatIfOriginal(props);
  delete props.amenity;
  delete props.shop;
  delete props.healthcare;
  delete props['school:level'];
  delete props['education:level'];
  delete props.level;
  delete props['isced:level'];
  delete props.category_label;
  delete props.objekttyp;
  delete props.building;

  props.name = `What-if ${prettyPOIName(cat)}`;
  props.whatif_poi = cat;
  props.category = (() => {
    switch (cat) {
      case 'grocery':
        return 'supermarket';
      case 'hospital':
      case 'pharmacy':
      case 'dentistry':
      case 'healthcare_center':
      case 'veterinary':
        return 'health';
      case 'university':
      case 'kindergarten':
      case 'school_primary':
      case 'school_high':
        return 'education';
      default:
        return cat;
    }
  })();

  switch (cat) {
    case 'grocery':
      props.shop = 'supermarket';
      props.objekttyp = 'supermarket';
      break;
    case 'hospital':
      props.amenity = 'hospital';
      props.healthcare = 'hospital';
      props.objekttyp = 'hospital';
      break;
    case 'pharmacy':
      props.amenity = 'pharmacy';
      props.objekttyp = 'pharmacy';
      break;
    case 'dentistry':
      props.amenity = 'dentist';
      props.objekttyp = 'dentist';
      break;
    case 'healthcare_center':
      props.amenity = 'clinic';
      props.healthcare = 'clinic';
      props.objekttyp = 'clinic';
      break;
    case 'veterinary':
      props.amenity = 'veterinary';
      props.objekttyp = 'veterinary';
      break;
    case 'university':
      props.amenity = 'university';
      props.objekttyp = 'university';
      break;
    case 'kindergarten':
      props.amenity = 'kindergarten';
      props.objekttyp = 'kindergarten';
      break;
    case 'school_primary':
      props.amenity = 'school';
      props['isced:level'] = '1';
      props['school:level'] = 'primary';
      props.objekttyp = 'school';
      break;
    case 'school_high':
      props.amenity = 'school';
      props['isced:level'] = '3';
      props['school:level'] = 'upper';
      props.objekttyp = 'school';
      break;
    case 'residential':
      props.building = 'residential';
      props.category = 'residential';
      props.objekttyp = 'residential';
      delete props.whatif_poi;
      break;
    default:
      break;
  }
}

function buildPOIMapFromCurrent() {
  if (!currentPOIsFC?.features?.length) return null;
  const map = {};
  for (const feat of currentPOIsFC.features) {
    const cat = poiCategoryOf(feat);
    if (!map[cat]) map[cat] = [];
    let coords = null;
    if (feat.geometry?.type === 'Point') {
      coords = feat.geometry.coordinates;
    } else {
      try { coords = turf.centroid(feat).geometry.coordinates; } catch { coords = null; }
    }
    if (!coords || !Number.isFinite(coords[0]) || !Number.isFinite(coords[1])) continue;
    map[cat].push({ c: coords, name: feat.properties?.name || '(unnamed)' });
  }
  return map;
}

function collectWhatIfPOIsByCat(catList) {
  const result = {};
  if (!Array.isArray(catList) || !baseCityFC?.features?.length) return result;
  const cats = catList.filter(Boolean);
  if (!cats.length) return result;

  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (!props.__whatIf && !props.whatif_poi) continue;
    let coords = null;
    try { coords = turf.centroid(feat).geometry.coordinates; } catch { coords = null; }
    if (!coords || !Number.isFinite(coords[0]) || !Number.isFinite(coords[1])) continue;

    for (const cat of cats) {
      if (!buildingMatchesPOI(props, cat)) continue;
      if (!result[cat]) result[cat] = [];
      result[cat].push({
        c: coords,
        name: props.name || `What-if ${prettyPOIName(cat)}`
      });
    }
  }
  return result;
}

function getWhatIfChangedCentroids() {
  if (!baseCityFC?.features?.length) return [];
  const centroids = [];
  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (!props.__whatIf || props.__whatIfAdded) continue; // only changed, not added
    try { centroids.push(turf.centroid(feat).geometry.coordinates); } catch {}
  }
  return centroids;
}

function filterFetchedPOIsForWhatIf(features) {
  if (!Array.isArray(features) || !features.length) return features || [];
  const changed = getWhatIfChangedCentroids();
  if (!changed.length) return features;
  return features.filter(f => {
    const c = f.geometry?.coordinates;
    if (!c) return true;
    return !changed.some(cc => haversineMeters(c, cc) < 50);
  });
}

function collectBuildingPOIsByCat(catList, { includeWhatIf = true } = {}) {
  const result = {};
  if (!Array.isArray(catList) || !baseCityFC?.features?.length) return result;
  const cats = catList.filter(Boolean);
  if (!cats.length) return result;

  for (const feat of baseCityFC.features) {
    const props = feat.properties || {};
    if (!includeWhatIf && (props.__whatIf || props.whatif_poi)) continue;
    let coords = null;
    try { coords = turf.centroid(feat).geometry.coordinates; } catch { coords = null; }
    if (!coords || !Number.isFinite(coords[0]) || !Number.isFinite(coords[1])) continue;

    for (const cat of cats) {
      if (!buildingMatchesPOI(props, cat)) continue;
      if (!result[cat]) result[cat] = [];
      result[cat].push({
        c: coords,
        name: props.name || `Building ${prettyPOIName(cat)}`
      });
    }
  }
  return result;
}

function syncWhatIfPOIs(catList) {
  if (!currentPOIsFC || !Array.isArray(catList)) return {};
  const cats = catList.filter(Boolean);
  if (!cats.length) return {};

  // Remove old what-if POI markers
  currentPOIsFC.features = (currentPOIsFC.features || []).filter(
    feat => !feat?.properties?.__whatif_poi
  );
  // Remove original fetched POIs that overlap with what-if changed buildings
  const changedCentroids = getWhatIfChangedCentroids();
  if (changedCentroids.length) {
    currentPOIsFC.features = currentPOIsFC.features.filter(feat => {
      const c = feat.geometry?.type === 'Point' ? feat.geometry.coordinates : null;
      if (!c) return true;
      return !changedCentroids.some(cc => haversineMeters(c, cc) < 50);
    });
  }

  const whatIfMap = collectWhatIfPOIsByCat(cats);
  Object.entries(whatIfMap).forEach(([cat, items]) => {
    items.forEach((item, idx) => {
      currentPOIsFC.features.push({
        type: 'Feature',
        properties: {
          name: item.name || `What-if ${prettyPOIName(cat)}`,
          __cat: cat,
          __whatif_poi: true,
          id: `whatif:${cat}:${idx}`
        },
        geometry: { type: 'Point', coordinates: item.c }
      });
    });
  });

  return whatIfMap;
}

async function recomputeFairnessSingleFromPOIs(cat, poiByCat) {
  const arr = poiByCat?.[cat] || [];
  if (!arr.length || !baseCityFC?.features?.length) return null;
  const scoresLack = [];
  const allowRouting = allowRoutingForFairness(arr.length);

  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    delete props.fair;
    props.fair_multi = {};

    let bestIdx = -1, bestD = Infinity;
    for (let i = 0; i < arr.length; i++) {
      const d = haversineMeters(cB, arr[i].c);
      if (d < bestD) { bestD = d; bestIdx = i; }
    }
    if (bestIdx < 0) continue;

    const nearest = arr[bestIdx];
    const metrics = await getTravelMetricsForPair(cB, nearest.c, bestD, allowRouting, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);

    props.fair = {
      cat,
      score,
      nearest_name: nearest.name,
      nearest_dist_m: metrics.meters,
      nearest_time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null,
      nearest_lonlat: nearest.c
    };
    props.fair_multi[cat] = {
      score,
      dist_m: metrics.meters,
      time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null
    };
    scoresLack.push(1 - score);
  }

  const G = gini(scoresLack);
  fairActive = true;
  fairCategory = cat;
  window.activePOICats = new Set([cat]);
  fairRecolorTick++;
  updateLayers();

  if (giniOut) giniOut.textContent = `${prettyPOIName(cat)} Gini: ${formatFairnessBadgeValue(G)}`;
  const summary = summarizeFairnessCurrent();
  showSidePanel(cat, G, arr.length, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  return { gini: G, poiCount: arr.length };
}

async function recomputeFairnessMixFromPOIs(mix, poiByCat) {
  if (!mix.length || !baseCityFC?.features?.length) return null;
  const scoresLack = [];
  const maxPois = Math.max(0, ...mix.map(({ cat }) => poiByCat?.[cat]?.length || 0));
  const allowRouting = allowRoutingForFairness(maxPois);

  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    const fm = {};
    delete props.fair;

    for (const {cat} of mix) {
      const arr = poiByCat?.[cat] || [];
      if (!arr.length) continue;
      let bestD = Infinity, bestIdx = -1;
      for (let i = 0; i < arr.length; i++) {
        const d = haversineMeters(cB, arr[i].c);
        if (d < bestD) { bestD = d; bestIdx = i; }
      }
      if (bestIdx < 0) continue;
      const nearest = arr[bestIdx];
      const metrics = await getTravelMetricsForPair(cB, nearest.c, bestD, allowRouting, fairnessTravelMode);
      const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);
      fm[cat] = {
        score,
        dist_m: metrics.meters,
        time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null,
        nearest
      };
    }
    const entries = mix
      .map(({cat, weight}) => ({ w: Math.max(0, weight), s: fm[cat]?.score }))
      .filter(e => Number.isFinite(e.s) && e.w > 0);

    if (entries.length) {
      const best = Math.max(...entries.map(entry => entry.s));
      props.fair = { cat: 'mix', score: best };
      scoresLack.push(1 - best);
    } else {
      delete props.fair;
    }

    props.fair_multi = fm;
  }

  const G = gini(scoresLack);
  fairActive = true;
  fairCategory = 'mix';
  window.activePOICats = new Set(mix.map(m => m.cat));
  fairRecolorTick++;
  updateLayers();

  if (giniOut) giniOut.textContent = `Mix Gini: ${formatFairnessBadgeValue(G)}`;
  const summary = summarizeFairnessCurrent();
  showSidePanel('mix', G, currentPOIsFC?.features?.length || 0, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  return { gini: G, poiCount: currentPOIsFC?.features?.length || 0 };
}


async function recomputeFairnessAfterWhatIf() {
  try {
    showGlobalSpinner('Recomputing fairness…');
    const result = {
      category: fairCategory || null,
      categoryGini: null,
      overallGini: null
    };
    if (fairActive && fairCategory) {
      const cats = fairCategory === 'mix'
        ? selectedPOIMix.map(m => m.cat)
        : [fairCategory];
      if (fairnessModel === 'ifcity') {
        const weightsByCat = fairCategory === 'mix'
          ? Object.fromEntries(selectedPOIMix.map(({ cat, weight }) => [cat, weight]))
          : { [fairCategory]: 1 };
        const ifCityRes = await computeIfCityFairness(cats, weightsByCat);
        result.categoryGini = Number.isFinite(ifCityRes?.giniCoeff) ? ifCityRes.giniCoeff
          : Number.isFinite(ifCityRes?.inequality) ? ifCityRes.inequality : null;
        currentCategoryGini = result.categoryGini;
        if (giniOut && Number.isFinite(result.categoryGini)) {
          const label = fairCategory === 'mix'
            ? 'Mix'
            : prettyPOIName(fairCategory);
          giniOut.textContent = `${label} GE(α=2): ${formatFairnessBadgeValue(result.categoryGini)}`;
        }
      } else if (fairCategory === 'mix' && selectedPOIMix.length) {
        const res = await computeFairnessWeighted(selectedPOIMix);
        result.categoryGini = res?.gini ?? null;
      } else {
        const res = await computeFairnessFast(fairCategory);
        result.categoryGini = res?.gini ?? null;
      }
    }
    const overallRes = await autoComputeOverall();
    result.overallGini = overallRes?.overall_gini ?? null;

    // Restore category Gini display — autoComputeOverall overwrites
    // props.__ifcity with all-categories values, which can cause
    // side effects that clobber giniOut with the overall value
    if (giniOut && Number.isFinite(result.categoryGini)) {
      const label = fairCategory === 'mix'
        ? 'Mix'
        : prettyPOIName(fairCategory);
      const prefix = fairnessModel === 'ifcity' ? 'GE(α=2)' : 'Gini';
      giniOut.textContent = `${label} ${prefix}: ${formatFairnessBadgeValue(result.categoryGini)}`;
    }
    window.faveInspector?.refresh?.();

    hideGlobalSpinner();
    // --- Refresh DR and Parallel Coordinates so mock buildings are included ---
    if (drPlot.points) {
      // DR is active → re-run it (collectDRData reads baseCityFC.features fresh,
      // which now includes mock buildings with their fairness scores).
      // runDR also updates drPlot.sample, which PC uses in building mode.
      runDR();
    } else if (parallelCoordsOpen) {
      // DR not active, but PC is open → refresh PC directly.
      // Without a stale drPlot.sample, getParallelCoordsDataset falls back
      // to baseCityFC.features (which includes mock buildings).
      updateParallelCoordsPanel();
    }

    return result;
  } catch (err) {
    hideGlobalSpinner();
    console.warn('What-if recompute failed', err);
    return { category: fairCategory || null, categoryGini: null, overallGini: null };
  }
}

async function applyBuildingTypeChange(feature, nextType) {
  if (!feature) return;
  showGlobalSpinner('Changing building type…');
  await waitForSpinnerPaint();
  const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  const scoreSnapshot = captureScoreSnapshot();
  const props = feature.properties || (feature.properties = {});
  const nextCat = nextType || whatIfType;
  applyPOITags(props, nextCat);
  props.__whatIf = true;
  refreshBuildingTypeDropdown();
  updateLayers();

  // Re-show popup so user sees the updated building info
  let coord = null;
  try { coord = turf.centroid(feature).geometry.coordinates; } catch { coord = null; }
  if (coord) showPopup(feature, coord);

  const result = await recomputeFairnessAfterWhatIf();
  const afterCatGini = Number.isFinite(result?.categoryGini)
    ? result.categoryGini
    : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(result?.overallGini)
    ? result.overallGini
    : (Number.isFinite(overallGini) ? overallGini : null);
  const nextId = changeLogIdCounter;
  const colorPairs = applyDeltaColorsFromSnapshot(scoreSnapshot, nextId);
  recordWhatIfChange({
    action: 'change_type',
    description: `Changed building → ${prettyPOIName(nextCat)}`,
    category: nextCat,
    beforeGini: beforeCatGini,
    afterGini: afterCatGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: colorPairs
  });
  hideGlobalSpinner();
}

function collectPOICoordsForCategory(cat, fetchedPOIs) {
  const filtered = filterFetchedPOIsForWhatIf(fetchedPOIs?.features || []);
  const coords = filtered.map((p) => ({
    c: p.geometry.coordinates,
    name: p.properties?.name || '(unnamed)'
  }));
  const whatIfMap = collectWhatIfPOIsByCat([cat]);
  if (whatIfMap?.[cat]?.length) coords.push(...whatIfMap[cat]);
  const buildingPOIs = collectBuildingPOIsByCat([cat], { includeWhatIf: true });
  if (buildingPOIs?.[cat]?.length) coords.push(...buildingPOIs[cat]);
  return coords;
}

function computeBuildingDataForCategory(cat, poiCoords) {
  const data = [];
  if (!baseCityFC?.features?.length) return data;
  const cityCenter = getCityCenterCoord();
  for (const [idx, feat] of baseCityFC.features.entries()) {
    const props = feat.properties || {};
    const centroid = turf.centroid(feat).geometry.coordinates;
    const cityDistKm = cityCenter ? haversineMeters(centroid, cityCenter) / 1000 : null;
    let bestD = Infinity;
    for (let i = 0; i < poiCoords.length; i++) {
      const d = haversineMeters(centroid, poiCoords[i].c);
      if (d < bestD) bestD = d;
    }
    const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
    const score = Number.isFinite(bestD) ? scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode) : 0;
    data.push({
      idx,
      feature: feat,
      centroid,
      cityDistKm,
      bestD,
      score,
      isMatch: buildingMatchesPOI(props, cat),
      name: props.name || props.category || props.objekttyp || `Building #${idx + 1}`
    });
  }
  return data;
}

function giniFromBuildingScores(rows) {
  const lacks = rows.map(row => 1 - row.score);
  return gini(lacks);
}

function giniFromOverallRows(rows) {
  const lacks = rows.map(row => 1 - row.overallScore);
  return gini(lacks);
}

function computeBuildingDataForOverall(categories, catToCoords) {
  const rows = [];
  if (!baseCityFC?.features?.length) return rows;
  const cityCenter = getCityCenterCoord();
  for (const [idx, feat] of baseCityFC.features.entries()) {
    const props = feat.properties || {};
    const centroid = turf.centroid(feat).geometry.coordinates;
    const cityDistKm = cityCenter ? haversineMeters(centroid, cityCenter) / 1000 : null;
    const scoreByCat = {};
    const bestByCat = {};
    const isMatchByCat = {};
    categories.forEach((cat) => {
      const coords = catToCoords[cat] || [];
      let bestD = Infinity;
      for (let i = 0; i < coords.length; i++) {
        const d = haversineMeters(centroid, coords[i].c);
        if (d < bestD) bestD = d;
      }
      bestByCat[cat] = bestD;
      const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
      scoreByCat[cat] = Number.isFinite(bestD) ? scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode) : 0;
      isMatchByCat[cat] = buildingMatchesPOI(props, cat);
    });
    const scores = categories.map(cat => scoreByCat[cat]).filter(Number.isFinite);
    const overallScore = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
    rows.push({
      idx,
      feature: feat,
      centroid,
      cityDistKm,
      bestByCat,
      scoreByCat,
      isMatchByCat,
      overallScore,
      score: overallScore,
      name: props.name || props.category || props.objekttyp || `Building #${idx + 1}`
    });
  }
  return rows;
}

async function computeOverallSuggestionState(categories) {
  const unique = Array.from(new Set(categories.filter(Boolean)));
  const fetched = await Promise.all(
    unique.map(cat => fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] })))
  );
  const catToCoords = {};
  unique.forEach((cat, idx) => {
    catToCoords[cat] = collectPOICoordsForCategory(cat, fetched[idx]);
  });
  const rows = computeBuildingDataForOverall(unique, catToCoords);
  if (!rows.length) {
    throw new Error('No buildings available to score.');
  }
  return {
    rows,
    categories: unique,
    validCats: unique,
    baselineGini: giniFromOverallRows(rows),
    thresholds: getCityAreaThresholds(rows)
  };
}

function giniWithOverallCandidate(cat, rows, categories, candidateCoord) {
  const lacks = rows.map((row) => {
    const updatedScores = categories.map((category) => {
      const currentBest = row.bestByCat[category] ?? Infinity;
      if (category !== cat) return row.scoreByCat[category];
      const d = haversineMeters(row.centroid, candidateCoord);
      const bestD = Math.min(currentBest, d);
      const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
      return Number.isFinite(bestD) ? scoreFromTimeSeconds(category, timeSeconds, fairnessTravelMode) : 0;
    });
    const avgScore = updatedScores.length
      ? updatedScores.reduce((a, b) => a + b, 0) / updatedScores.length
      : 0;
    return 1 - avgScore;
  });
  return gini(lacks);
}

function applyOverallCandidate(cat, rows, categories, candidateCoord) {
  rows.forEach((row) => {
    const currentBest = row.bestByCat[cat] ?? Infinity;
    const d = haversineMeters(row.centroid, candidateCoord);
    if (d < currentBest) {
      row.bestByCat[cat] = d;
      const timeSeconds = estimateTravelTimeSecondsFromMeters(d, fairnessTravelMode);
      row.scoreByCat[cat] = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
    }
    const scores = categories.map(category => row.scoreByCat[category]).filter(Number.isFinite);
    row.overallScore = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
    row.score = row.overallScore;
  });
}

async function findBestOverallCandidate({
  cat,
  kind,
  bbox,
  center,
  radiusKm,
  areaFocus,
  overallState,
  usedIds
}) {
  const { rows, categories, baselineGini, thresholds } = overallState;
  const metricRows = overallState.metricRows || rows;
  const candidates = pickCandidateRows(rows, {
    kind,
    bbox,
    usedIds,
    center,
    radiusKm,
    areaFocus,
    thresholds,
    cat
  });
  if (!candidates.length) return null;
  let best = null;
  for (let ci = 0; ci < candidates.length; ci += 1) {
    if (ci > 0 && ci % 5 === 0) await new Promise(r => setTimeout(r, 0));
    const candidate = candidates[ci];
    const g = giniWithOverallCandidate(cat, metricRows, categories, candidate.centroid);
    if (!best || g < best.giniAfter) {
      best = { candidate, giniAfter: g };
    }
  }
  if (!best) return null;
  return {
    kind,
    cat,
    giniBefore: baselineGini,
    giniAfter: best.giniAfter,
    improvement: baselineGini - best.giniAfter,
    location: best.candidate.centroid,
    candidate: best.candidate
  };
}

function giniWithCandidate(cat, rows, candidateCoord) {
  const lacks = rows.map((row) => {
    const d = haversineMeters(row.centroid, candidateCoord);
    const bestD = Math.min(row.bestD, d);
    const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
    return 1 - score;
  });
  return gini(lacks);
}

function applyCandidateToRows(cat, rows, candidateCoord) {
  rows.forEach((row) => {
    const d = haversineMeters(row.centroid, candidateCoord);
    if (d < row.bestD) {
      row.bestD = d;
      const timeSeconds = estimateTravelTimeSecondsFromMeters(d, fairnessTravelMode);
      row.score = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
    }
  });
}

function ifCityCandidateContribution(cat, fromCoord, toCoord) {
  const rho = ifCityPriorityWeight(cat);
  const kappa = ifCityKappa(cat);
  const linearKm = haversineMeters(fromCoord, toCoord) / 1000;
  const dKm = ifCityDistanceForMode(linearKm, fairnessTravelMode);
  return rho * Math.exp(-kappa * dKm);
}

function buildIfCitySuggestionRows(categories, catToPOI, weightsByCat = {}) {
  const rows = [];
  if (!baseCityFC?.features?.length) return rows;
  const cityCenter = getCityCenterCoord();

  for (const [idx, feat] of baseCityFC.features.entries()) {
    const props = feat.properties || {};
    const centroid = turf.centroid(feat).geometry.coordinates;
    const cityDistKm = cityCenter ? haversineMeters(centroid, cityCenter) / 1000 : null;
    const accessByCat = {};
    const isMatchByCat = {};

    for (const cat of categories) {
      const arr = catToPOI[cat] || [];
      let access = 0;
      for (const poi of arr) {
        const v = Number.isFinite(poi?.v) ? poi.v : 1;
        access += v * ifCityCandidateContribution(cat, centroid, poi.c);
      }
      accessByCat[cat] = access;
      isMatchByCat[cat] = buildingMatchesPOI(props, cat);
    }

    let utility = 0;
    for (const cat of categories) {
      const weight = Number.isFinite(weightsByCat[cat]) ? weightsByCat[cat] : 1;
      utility += weight * (accessByCat[cat] || 0);
    }

    const equityWeight = ifCityEquityWeightForFeature(feat);
    const benefit = Math.max(0, utility - IF_CITY_BASELINE_UTILITY) * equityWeight;

    rows.push({
      idx,
      feature: feat,
      centroid,
      cityDistKm,
      accessByCat,
      isMatchByCat,
      utility,
      benefit,
      score: 0,
      name: props.name || props.category || props.objekttyp || `Building #${idx + 1}`
    });
  }

  const normalized = normalizeBenefitsToScores(rows.map(r => r.benefit));
  rows.forEach((row, i) => {
    row.score = normalized[i] ?? 0;
  });
  return rows;
}

function generalizedEntropyWithIfCityCandidate(cat, rows, categories, candidateCoord, weightsByCat = {}) {
  const benefits = rows.map((row) => {
    let utility = 0;
    for (const category of categories) {
      let access = row.accessByCat[category] || 0;
      if (category === cat) {
        access += ifCityCandidateContribution(category, row.centroid, candidateCoord);
      }
      const weight = Number.isFinite(weightsByCat[category]) ? weightsByCat[category] : 1;
      utility += weight * access;
    }
    const equityWeight = ifCityEquityWeightForFeature(row.feature);
    return Math.max(0, utility - IF_CITY_BASELINE_UTILITY) * equityWeight;
  });
  return generalizedEntropy(benefits, IF_CITY_ALPHA);
}

function applyIfCityCandidate(cat, rows, categories, candidateCoord, weightsByCat = {}) {
  rows.forEach((row) => {
    row.accessByCat[cat] = (row.accessByCat[cat] || 0) + ifCityCandidateContribution(cat, row.centroid, candidateCoord);
    let utility = 0;
    for (const category of categories) {
      const weight = Number.isFinite(weightsByCat[category]) ? weightsByCat[category] : 1;
      utility += weight * (row.accessByCat[category] || 0);
    }
    row.utility = utility;
    const equityWeight = ifCityEquityWeightForFeature(row.feature);
    row.benefit = Math.max(0, utility - IF_CITY_BASELINE_UTILITY) * equityWeight;
  });
  const normalized = normalizeBenefitsToScores(rows.map(r => r.benefit));
  rows.forEach((row, i) => {
    row.score = normalized[i] ?? 0;
  });
}

async function computeIfCitySuggestionState(categories, weightsByCat = {}) {
  const unique = Array.from(new Set((categories || []).filter(Boolean)));
  const fetched = await Promise.all(
    unique.map(cat => fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] })))
  );
  const catToPOI = {};
  unique.forEach((cat, idx) => {
    const filteredFeats = filterFetchedPOIsForWhatIf(fetched[idx]?.features || []);
    const fetchedRows = filteredFeats.map((feat) => ({
      c: feat.geometry.coordinates,
      name: feat.properties?.name || '(unnamed)',
      v: ifCityOpportunityWeightFromPOI(cat, feat)
    }));
    const whatIfRows = collectPOICoordsForCategory(cat, fetched[idx]).map((item) => ({ ...item, v: 1 }));
    catToPOI[cat] = dedupeIFCityPOIs([...(fetchedRows || []), ...(whatIfRows || [])]);
  });

  const rows = buildIfCitySuggestionRows(unique, catToPOI, weightsByCat);
  if (!rows.length) {
    throw new Error('No buildings available to score.');
  }
  return {
    rows,
    categories: unique,
    validCats: unique.filter((cat) => (catToPOI[cat] || []).length > 0),
    baselineInequality: generalizedEntropy(rows.map(r => r.benefit), IF_CITY_ALPHA),
    thresholds: getCityAreaThresholds(rows),
    weightsByCat
  };
}

function getMapBoundsBBox() {
  if (!map?.getBounds) return null;
  const bounds = map.getBounds();
  return [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()];
}

function getMapCenter() {
  if (!map?.getCenter) return null;
  const center = map.getCenter();
  return [center.lng, center.lat];
}

function getCityCenterCoord() {
  if (baseCityFC?.features?.length) {
    try {
      return turf.centroid(baseCityFC).geometry.coordinates;
    } catch {
      return null;
    }
  }
  return getMapCenter();
}

function getCityAreaThresholds(rows) {
  const distances = rows.map(row => row.cityDistKm).filter(Number.isFinite).sort((a, b) => a - b);
  if (!distances.length) {
    return { centerMaxKm: Infinity, outskirtsMinKm: Infinity };
  }
  return {
    centerMaxKm: quantileSorted(distances, 0.4),
    outskirtsMinKm: quantileSorted(distances, 0.7)
  };
}

function boundsDiagonalMeters(bounds) {
  if (!bounds) return null;
  const [minX, minY, maxX, maxY] = bounds;
  if (![minX, minY, maxX, maxY].every(Number.isFinite)) return null;
  return haversineMeters([minX, minY], [maxX, maxY]);
}

function getSuggestionMinDistanceMeters(bbox) {
  let cityBounds = null;
  if (baseCityFC?.features?.length) {
    try {
      const [minX, minY, maxX, maxY] = turf.bbox(baseCityFC);
      cityBounds = [minX, minY, maxX, maxY];
    } catch {
      cityBounds = null;
    }
  }
  const cityDiag = boundsDiagonalMeters(cityBounds);
  const viewDiag = boundsDiagonalMeters(bbox);
  const diag = Math.max(cityDiag || 0, viewDiag || 0);
  if (!diag) return 500;
  return Math.max(400, Math.min(2200, diag * 0.1));
}

function isFarFromUsed(coord, usedCoords, minDistanceMeters) {
  if (!usedCoords?.length) return true;
  return usedCoords.every((used) => haversineMeters(coord, used) >= minDistanceMeters);
}

function distanceToNearestUsed(coord, usedCoords) {
  if (!usedCoords?.length) return Infinity;
  let best = Infinity;
  for (const used of usedCoords) {
    const d = haversineMeters(coord, used);
    if (d < best) best = d;
  }
  return best;
}

function isWithinBBox(coord, bbox) {
  if (!bbox || !coord) return true;
  const [minX, minY, maxX, maxY] = bbox;
  return coord[0] >= minX && coord[0] <= maxX && coord[1] >= minY && coord[1] <= maxY;
}

function isWithinRadius(coord, center, radiusKm) {
  if (!center || !coord || !Number.isFinite(radiusKm) || radiusKm <= 0) return true;
  const distMeters = haversineMeters(coord, center);
  return distMeters <= radiusKm * 1000;
}

function pickCandidateRows(rows, {
  kind,
  bbox,
  usedIds,
  center,
  radiusKm,
  areaFocus,
  thresholds,
  cat,
  usedCoords,
  minSpacingMeters,
  limit = WHATIF_SUGGESTION_LIMIT,
  excludePOIBuildings = true
}) {
  return rows
    .filter(row => {
      if (kind !== 'change') return true;
      if (row.isMatchByCat && cat) return !row.isMatchByCat[cat];
      return !row.isMatch;
    })
    .filter(row => !excludePOIBuildings || !row.isMatch)
    .filter(row => isWithinBBox(row.centroid, bbox))
    .filter(row => isWithinRadius(row.centroid, center, radiusKm))
    .filter(row => {
      if (!areaFocus || areaFocus === 'any') return true;
      const dist = row.cityDistKm;
      if (!Number.isFinite(dist)) return true;
      if (areaFocus === 'center') return dist <= (thresholds?.centerMaxKm ?? Infinity);
      if (areaFocus === 'outskirts') return dist >= (thresholds?.outskirtsMinKm ?? Infinity);
      return true;
    })
    .filter(row => !usedIds.has(row.idx))
    .filter(row => isFarFromUsed(row.centroid, usedCoords, minSpacingMeters))
    .sort((a, b) => a.score - b.score)
    .slice(0, Number.isFinite(limit) ? limit : rows.length);
}

async function computeWhatIfSuggestionsForCategory({ cat, kind, count, bbox, center, radiusKm, areaFocus }) {
  const suggestions = [];
  const usedIds = new Set();
  const usedCoords = [];
  const minSpacingMeters = getSuggestionMinDistanceMeters(bbox);
  const exactMode = isExactCityWideSingleCandidateMode({ count, bbox, center, radiusKm, areaFocus });

  // Gini/entropy is computed over a stratified sample when cities have many buildings.
  // Candidate SELECTION still uses the full row set; only the O(N) objective evaluation uses the sample.
  // Sample elements are object references, so applyCandidateToRows / applyIfCityCandidate mutations
  // propagate correctly into the sample for multi-candidate placement.
  const METRIC_SAMPLE_SIZE = 4000;

  if (fairnessModel === 'ifcity') {
    const state = await computeIfCitySuggestionState([cat], { [cat]: 1 });
    const { rows, categories, thresholds } = state;
    const sampleStep = rows.length > METRIC_SAMPLE_SIZE ? Math.floor(rows.length / METRIC_SAMPLE_SIZE) : 1;
    const entropyRows = sampleStep > 1 ? rows.filter((_, i) => i % sampleStep === 0) : rows;
    const baselineInequality = generalizedEntropy(entropyRows.map(r => r.benefit), IF_CITY_ALPHA);

    for (let n = 0; n < count; n += 1) {
      const candidates = pickCandidateRows(rows, {
        kind,
        bbox,
        usedIds,
        center,
        radiusKm,
        areaFocus,
        thresholds,
        usedCoords,
        minSpacingMeters,
        cat,
        limit: exactMode ? WHATIF_SUGGESTION_EXACT_LIMIT : WHATIF_SUGGESTION_LIMIT
      });
      if (!candidates.length) break;

      let best = null;
      const metricEps = 0.0005;
      for (let ci = 0; ci < candidates.length; ci++) {
        if (ci > 0 && ci % 5 === 0) await new Promise(r => setTimeout(r, 0));
        const candidate = candidates[ci];
        const inequality = generalizedEntropyWithIfCityCandidate(cat, entropyRows, categories, candidate.centroid, { [cat]: 1 });
        if (!best || inequality < best.inequality - metricEps) {
          best = { candidate, inequality, dist: distanceToNearestUsed(candidate.centroid, usedCoords) };
          continue;
        }
        if (best && Math.abs(inequality - best.inequality) <= metricEps) {
          const dist = distanceToNearestUsed(candidate.centroid, usedCoords);
          if (dist > best.dist) best = { candidate, inequality, dist };
        }
      }
      if (!best) break;

      applyIfCityCandidate(cat, rows, categories, best.candidate.centroid, { [cat]: 1 });
      usedIds.add(best.candidate.idx);
      usedCoords.push(best.candidate.centroid);
      suggestions.push({
        kind,
        cat,
        giniBefore: baselineInequality,
        giniAfter: best.inequality,
        improvement: baselineInequality - best.inequality,
        location: best.candidate.centroid,
        candidate: best.candidate
      });
    }

    return suggestions;
  }

  const fetched = await fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] }));
  const poiCoords = collectPOICoordsForCategory(cat, fetched);
  const buildingRows = computeBuildingDataForCategory(cat, poiCoords);
  if (!buildingRows.length) {
    throw new Error('No buildings available to score.');
  }
  const giniSampleStep = buildingRows.length > METRIC_SAMPLE_SIZE ? Math.floor(buildingRows.length / METRIC_SAMPLE_SIZE) : 1;
  const giniRows = giniSampleStep > 1 ? buildingRows.filter((_, i) => i % giniSampleStep === 0) : buildingRows;
  const baselineGini = giniFromBuildingScores(giniRows);
  const thresholds = getCityAreaThresholds(buildingRows);

  for (let n = 0; n < count; n += 1) {
    const candidates = pickCandidateRows(buildingRows, {
      kind,
      bbox,
      usedIds,
      center,
      radiusKm,
      areaFocus,
      thresholds,
      usedCoords,
      minSpacingMeters,
      limit: exactMode ? WHATIF_SUGGESTION_EXACT_LIMIT : WHATIF_SUGGESTION_LIMIT
    });
    if (!candidates.length) break;

    let best = null;
    const giniEps = 0.0005;
    for (let ci = 0; ci < candidates.length; ci++) {
      if (ci > 0 && ci % 5 === 0) await new Promise(r => setTimeout(r, 0));
      const candidate = candidates[ci];
      const g = giniWithCandidate(cat, giniRows, candidate.centroid);
      if (!best || g < best.gini - giniEps) {
        best = { candidate, gini: g, dist: distanceToNearestUsed(candidate.centroid, usedCoords) };
        continue;
      }
      if (best && Math.abs(g - best.gini) <= giniEps) {
        const dist = distanceToNearestUsed(candidate.centroid, usedCoords);
        if (dist > best.dist) {
          best = { candidate, gini: g, dist };
        }
      }
    }
    if (!best) break;

    applyCandidateToRows(cat, buildingRows, best.candidate.centroid);
    usedIds.add(best.candidate.idx);
    usedCoords.push(best.candidate.centroid);
    suggestions.push({
      kind,
      cat,
      giniBefore: baselineGini,
      giniAfter: best.gini,
      improvement: baselineGini - best.gini,
      location: best.candidate.centroid,
      candidate: best.candidate
    });
  }

  return suggestions;
}

async function computeWhatIfSuggestions({
  categories,
  kind,
  count,
  bbox,
  center,
  radiusKm,
  fairnessTarget,
  fairnessCategories,
  areaFocus
}) {
  if (!baseCityFC?.features?.length) {
    throw new Error('Load buildings before running suggestions.');
  }
  const target = fairnessTarget === 'overall' ? 'overall' : 'category';
  const results = [];
  const categoryList = Array.isArray(categories) ? categories : [];
  if (!categoryList.length) {
    throw new Error('Select at least one POI category to suggest.');
  }
  const METRIC_SAMPLE_SIZE_OVERALL = 4000;

  if (target === 'overall') {
    const overallCats = (fairnessCategories && fairnessCategories.length)
      ? fairnessCategories
      : ALL_CATEGORIES;
    const usedIds = new Set();
    const usedCoords = [];
    const minSpacingMeters = getSuggestionMinDistanceMeters(bbox);

    if (fairnessModel === 'ifcity') {
      const weightsByCat = Object.fromEntries(overallCats.map((cat) => [cat, 1]));
      const overallState = await computeIfCitySuggestionState(overallCats, weightsByCat);
      const { rows, categories, validCats, thresholds } = overallState;
      const stepO = rows.length > METRIC_SAMPLE_SIZE_OVERALL ? Math.floor(rows.length / METRIC_SAMPLE_SIZE_OVERALL) : 1;
      const entropyRows = stepO > 1 ? rows.filter((_, i) => i % stepO === 0) : rows;
      const baselineInequality = generalizedEntropy(entropyRows.map(r => r.benefit), IF_CITY_ALPHA);
      for (let n = 0; n < count; n += 1) {
        let bestPick = null;
        let ci = 0;
        for (const cat of categoryList) {
          if (!validCats.includes(cat)) continue;
          const candidates = pickCandidateRows(rows, {
            kind,
            bbox,
            usedIds,
            center,
            radiusKm,
            areaFocus,
            thresholds,
            cat,
            usedCoords,
            minSpacingMeters
          });
          if (!candidates.length) continue;
          for (const candidate of candidates) {
            if (ci > 0 && ci % 5 === 0) await new Promise(r => setTimeout(r, 0));
            ci += 1;
            const inequality = generalizedEntropyWithIfCityCandidate(cat, entropyRows, categories, candidate.centroid, weightsByCat);
            if (!bestPick || inequality < bestPick.giniAfter) {
              bestPick = {
                kind,
                cat,
                giniBefore: baselineInequality,
                giniAfter: inequality,
                improvement: baselineInequality - inequality,
                location: candidate.centroid,
                candidate
              };
            }
          }
        }
        if (!bestPick) break;
        applyIfCityCandidate(bestPick.cat, rows, categories, bestPick.candidate.centroid, weightsByCat);
        usedIds.add(bestPick.candidate.idx);
        usedCoords.push(bestPick.candidate.centroid);
        results.push(bestPick);
      }
    } else {
      const overallState = await computeOverallSuggestionState(overallCats);
      const stepO = overallState.rows.length > METRIC_SAMPLE_SIZE_OVERALL ? Math.floor(overallState.rows.length / METRIC_SAMPLE_SIZE_OVERALL) : 1;
      overallState.metricRows = stepO > 1 ? overallState.rows.filter((_, i) => i % stepO === 0) : overallState.rows;
      for (let n = 0; n < count; n += 1) {
        let bestPick = null;
        for (const cat of categoryList) {
          const pick = await findBestOverallCandidate({
            cat,
            kind,
            bbox,
            center,
            radiusKm,
            areaFocus,
            overallState,
            usedIds,
            usedCoords,
            minSpacingMeters
          });
          if (pick && (!bestPick || pick.giniAfter < bestPick.giniAfter)) {
            bestPick = pick;
          }
        }
        if (!bestPick) break;
        applyOverallCandidate(bestPick.cat, overallState.rows, overallState.validCats, bestPick.candidate.centroid);
        usedIds.add(bestPick.candidate.idx);
        usedCoords.push(bestPick.candidate.centroid);
        results.push(bestPick);
      }
    }
  } else {
    for (const cat of categoryList) {
      const batch = await computeWhatIfSuggestionsForCategory({
        cat,
        kind,
        count,
        bbox,
        center,
        radiusKm,
        areaFocus
      });
      results.push(...batch);
    }
  }
  return results;
}

function setWhatIfSuggestions(list) {
  whatIfSuggestions = Array.isArray(list) ? list : [];
  whatIfSuggestionTick++;
  updateLayers();
}

function clearWhatIfSuggestions() {
  whatIfSuggestions = [];
  whatIfLastSuggestionConfig = null;
  whatIfSuggestionTick++;
  updateLayers();
  if (whatIfSuggestionOut) {
    whatIfSuggestionOut.textContent = 'Suggestions cleared.';
    whatIfSuggestionOut.classList.remove('text-danger');
    whatIfSuggestionOut.classList.add('text-muted');
  }
  if (whatIfApplySuggestionBtn) whatIfApplySuggestionBtn.disabled = true;
  if (whatIfClearSuggestionBtn) whatIfClearSuggestionBtn.disabled = true;
  if (whatIfVerifySuggestionBtn) whatIfVerifySuggestionBtn.disabled = true;
}

function cloneRowsForWhatIf(rows, fairnessMode) {
  if (fairnessMode === 'ifcity') {
    return rows.map((row) => ({
      ...row,
      accessByCat: { ...(row.accessByCat || {}) },
      isMatchByCat: { ...(row.isMatchByCat || {}) }
    }));
  }
  return rows.map((row) => ({
    ...row,
    bestByCat: row.bestByCat ? { ...row.bestByCat } : undefined,
    scoreByCat: row.scoreByCat ? { ...row.scoreByCat } : undefined,
    isMatchByCat: row.isMatchByCat ? { ...row.isMatchByCat } : undefined
  }));
}

function formatObjectiveValue(value) {
  if (!Number.isFinite(value)) return 'n/a';
  return value.toFixed(5);
}

function combinationCountBigInt(n, k) {
  if (!Number.isInteger(n) || !Number.isInteger(k) || n < 0 || k < 0 || k > n) return 0n;
  const kk = Math.min(k, n - k);
  let result = 1n;
  for (let i = 1; i <= kk; i += 1) {
    result = (result * BigInt(n - kk + i)) / BigInt(i);
  }
  return result;
}

function formatBigInt(value) {
  if (typeof value !== 'bigint') return String(value ?? '0');
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function summarizeVerificationAudit(item) {
  const modeLabel = item.exhaustiveWithinPool ? 'full search' : 'partial search';
  const capNote = item.poolWasCapped
    ? ` Candidate pool capped at ${item.poolCap} from ${item.poolOriginalSize}.`
    : '';
  const guaranteeNote = item.globalGuarantee
    ? ' Global guarantee: exact optimum over all feasible candidates under current filters.'
    : (item.exhaustiveWithinPool
      ? ' Guarantee scope: exact optimum over searched pool.'
      : ' No global guarantee: bounded/partial enumeration.');
  const methodNote = " Candidate generation may be greedy/heuristic; 'best' is computed by enumerating feasible combinations in the verifier.";
  const planningNote = ' Use as planning support, not a replacement for professional urban planning judgement.';
  return `${item.metricName}: candidate=${formatObjectiveValue(item.candidateObjective)}, best=${formatObjectiveValue(item.bestObjective)}, gap=${formatObjectiveValue(item.gap)}, k=${item.count}, pool=${item.poolSize}, combos=${item.combosChecked}/${item.totalCombos}${item.totalCombosExact ? '' : '+'}, ${modeLabel}. best = min objective over feasible ${item.count}-combinations in pool.${capNote}${guaranteeNote}${methodNote}${planningNote}`;
}

async function verifyWhatIfSuggestionsOptimality(config, suggestions) {
  const currentSuggestions = Array.isArray(suggestions) ? suggestions : [];
  if (!currentSuggestions.length) throw new Error('Generate suggestions first.');
  if (!config) throw new Error('No suggestion parameters found. Generate suggestions again.');

  const categories = Array.isArray(config.categories) ? config.categories.filter(Boolean) : [];
  if (!categories.length) throw new Error('No categories to verify.');

  const fairnessTarget = config.fairnessTarget === 'overall' ? 'overall' : 'category';
  const kind = config.kind === 'change' ? 'change' : 'add';
  const bbox = config.bbox || null;
  const center = config.center || null;
  const radiusKm = Number.isFinite(config.radiusKm) ? config.radiusKm : 0;
  const areaFocus = config.areaFocus || 'any';
  const requestedCount = Math.max(1, Number.isFinite(config.count) ? config.count : currentSuggestions.length);
  const minSpacingMeters = getSuggestionMinDistanceMeters(bbox);
  const exactMode = requestedCount === 1 && categories.length === 1;
  const maxCombos = exactMode ? WHATIF_SUGGESTION_EXACT_LIMIT : 50000;
  const maxPoolSize = exactMode ? WHATIF_SUGGESTION_EXACT_LIMIT : 120;
  let combosChecked = 0;
  let truncated = false;

  const overallCats = (config.fairnessCategories && config.fairnessCategories.length)
    ? config.fairnessCategories
    : categories;

  const verifyTask = async ({
    rows,
    thresholds,
    categoriesForState,
    catChoices,
    evalObjective,
    applyCandidate,
    relevantSuggestions
  }) => {
    const suggestionsForTask = Array.isArray(relevantSuggestions) ? relevantSuggestions : [];
    const count = Math.min(requestedCount, Math.max(1, suggestionsForTask.length || requestedCount));

    const uncappedPool = [];
    for (const cat of catChoices) {
      const candidates = pickCandidateRows(rows, {
        kind,
        bbox,
        usedIds: new Set(),
        center,
        radiusKm,
        areaFocus,
        thresholds,
        cat,
        usedCoords: [],
        minSpacingMeters: 0,
        limit: exactMode ? WHATIF_SUGGESTION_EXACT_LIMIT : WHATIF_SUGGESTION_LIMIT
       });
      candidates.forEach((candidate) => {
        uncappedPool.push({ cat, candidate });
      });
    }
    const poolWasCapped = uncappedPool.length > maxPoolSize;
    const pool = poolWasCapped ? uncappedPool.slice(0, maxPoolSize) : uncappedPool;
    if (!pool.length) {
      throw new Error('No candidate pool available for verification with current filters.');
    }

    const candidateRows = cloneRowsForWhatIf(rows, fairnessModel);
    suggestionsForTask.forEach((entry) => {
      if (!Array.isArray(entry.location) || entry.location.length < 2) return;
      const cat = entry.cat || catChoices[0];
      applyCandidate(candidateRows, cat, entry.location, categoriesForState);
    });
    const candidateObjective = evalObjective(candidateRows, categoriesForState);
    if (pool.length < count) {
      throw new Error(`Candidate pool too small for k=${count} (pool=${pool.length}).`);
    }

    const totalCombosBig = combinationCountBigInt(pool.length, count);
    const totalCombosExact = totalCombosBig <= BigInt(Number.MAX_SAFE_INTEGER);
    const totalCombos = totalCombosExact ? Number(totalCombosBig) : Number.MAX_SAFE_INTEGER;
    if (exactMode && totalCombosBig > WHATIF_EXACT_MAX_COMBOS) {
      throw new Error(`Exact verification infeasible for this request (combinations=${formatBigInt(totalCombosBig)}). Reduce count/categories or narrow scope.`);
    }
    const exhaustiveWithinPool = totalCombosBig <= BigInt(maxCombos);

    let bestObjective = Infinity;
    let bestSelection = null;
    const current = [];
    const usedIds = new Set();
    const usedCoords = [];
    let combosCheckedLocal = 0;

    if (count === 1) {
      // Flat async loop — yields every 50 iterations so the browser stays responsive.
      for (let i = 0; i < pool.length; i += 1) {
        if (i > 0 && i % 5 === 0) await new Promise(r => setTimeout(r, 0));
        if (combosChecked >= maxCombos) { truncated = true; break; }
        const item = pool[i];
        if (usedIds.has(item.candidate.idx)) continue;
        combosChecked += 1;
        combosCheckedLocal += 1;
        const testRows = cloneRowsForWhatIf(rows, fairnessModel);
        applyCandidate(testRows, item.cat, item.candidate.centroid, categoriesForState);
        const objective = evalObjective(testRows, categoriesForState);
        if (objective < bestObjective) {
          bestObjective = objective;
          bestSelection = [{ ...item }];
        }
      }
    } else {
      const dfs = (start) => {
        if (combosChecked >= maxCombos) { truncated = true; return; }
        if (current.length === count) {
          combosChecked += 1;
          combosCheckedLocal += 1;
          const testRows = cloneRowsForWhatIf(rows, fairnessModel);
          current.forEach((entry) => {
            applyCandidate(testRows, entry.cat, entry.candidate.centroid, categoriesForState);
          });
          const objective = evalObjective(testRows, categoriesForState);
          if (objective < bestObjective) {
            bestObjective = objective;
            bestSelection = current.map((entry) => ({ ...entry }));
          }
          return;
        }
        for (let i = start; i < pool.length; i += 1) {
          if (combosChecked >= maxCombos) { truncated = true; return; }
          const item = pool[i];
          const idx = item.candidate.idx;
          if (usedIds.has(idx)) continue;
          if (!isFarFromUsed(item.candidate.centroid, usedCoords, minSpacingMeters)) continue;
          usedIds.add(idx);
          usedCoords.push(item.candidate.centroid);
          current.push(item);
          dfs(i + 1);
          current.pop();
          usedCoords.pop();
          usedIds.delete(idx);
        }
      };
      dfs(0);
    }
    if (!Number.isFinite(bestObjective)) {
      throw new Error('Unable to evaluate combinations for this scenario.');
    }

    return {
      candidateObjective,
      bestObjective,
      gap: candidateObjective - bestObjective,
      count,
      poolSize: pool.length,
      poolOriginalSize: uncappedPool.length,
      poolCap: maxPoolSize,
      poolWasCapped,
      combosChecked: combosCheckedLocal,
      totalCombos: formatBigInt(totalCombosBig),
      totalCombosExact,
      exhaustiveWithinPool,
      globalGuarantee: exhaustiveWithinPool && !poolWasCapped,
      exactMode,
      bestSelection: Array.isArray(bestSelection)
        ? bestSelection.map((entry) => ({
            kind,
            cat: entry.cat,
            location: entry.candidate.centroid,
            candidate: entry.candidate
          }))
        : []
    };
  };

  if (fairnessTarget === 'overall') {
    if (fairnessModel === 'ifcity') {
      const weightsByCat = Object.fromEntries(overallCats.map((cat) => [cat, 1]));
      const state = await computeIfCitySuggestionState(overallCats, weightsByCat);
      const catChoices = categories.filter((cat) => state.validCats.includes(cat));
      const result = await verifyTask({
        rows: state.rows,
        thresholds: state.thresholds,
        categoriesForState: state.categories,
        catChoices,
        evalObjective: (rows) => generalizedEntropy(rows.map((row) => row.benefit), IF_CITY_ALPHA),
        applyCandidate: (rows, cat, coord, cats) => applyIfCityCandidate(cat, rows, cats, coord, weightsByCat),
        relevantSuggestions: currentSuggestions
      });
      return {
        ...result,
        metricName: 'Generalized entropy',
        bestSuggestions: result.bestSelection || [],
        combosChecked,
        truncated
      };
    }

    const state = await computeOverallSuggestionState(overallCats);
    const result = await verifyTask({
      rows: state.rows,
      thresholds: state.thresholds,
      categoriesForState: state.categories,
      catChoices: categories,
      evalObjective: (rows) => giniFromOverallRows(rows),
      applyCandidate: (rows, cat, coord, cats) => applyOverallCandidate(cat, rows, cats, coord),
      relevantSuggestions: currentSuggestions
    });
    return {
      ...result,
      metricName: 'Overall Gini',
      bestSuggestions: result.bestSelection || [],
      combosChecked,
      truncated
    };
  }

  const perCategory = [];
  for (const cat of categories) {
    const catSuggestions = currentSuggestions.filter((s) => s.cat === cat);
    if (!catSuggestions.length) continue;
    if (fairnessModel === 'ifcity') {
      const state = await computeIfCitySuggestionState([cat], { [cat]: 1 });
      const result = await verifyTask({
        rows: state.rows,
        thresholds: state.thresholds,
        categoriesForState: state.categories,
        catChoices: [cat],
        evalObjective: (rows) => generalizedEntropy(rows.map((row) => row.benefit), IF_CITY_ALPHA),
        applyCandidate: (rows, category, coord, cats) => applyIfCityCandidate(category, rows, cats, coord, { [category]: 1 }),
        relevantSuggestions: catSuggestions
      });
      perCategory.push({
        ...result,
        cat,
        metricName: `${prettyPOIName(cat)} entropy`
      });
      continue;
    }

    const fetched = await fetchPOIs(cat, baseCityFC).catch(() => ({ type: 'FeatureCollection', features: [] }));
    const poiCoords = collectPOICoordsForCategory(cat, fetched);
    const rows = computeBuildingDataForCategory(cat, poiCoords);
    const result = await verifyTask({
      rows,
      thresholds: getCityAreaThresholds(rows),
      categoriesForState: [cat],
      catChoices: [cat],
      evalObjective: (testRows) => giniFromBuildingScores(testRows),
      applyCandidate: (testRows, category, coord) => applyCandidateToRows(category, testRows, coord),
      relevantSuggestions: catSuggestions
    });
    perCategory.push({
      ...result,
      cat,
      metricName: `${prettyPOIName(cat)} Gini`
    });
  }

  if (!perCategory.length) {
    throw new Error('No category suggestions available to verify.');
  }

  const worstGap = perCategory.reduce((acc, item) => Math.max(acc, item.gap), -Infinity);
  return {
    perCategory,
    bestSuggestions: perCategory.flatMap((item) => item.bestSelection || []),
    gap: worstGap,
    metricName: 'Per-category',
    combosChecked,
    truncated
  };
}

function mockTypeLabel(type) {
  if (type === 'residential') return 'Residential';
  return prettyPOIName(type);
}

function buildWhatIfMockTypeInputs() {
  if (!whatIfMockTypeList) return;
  whatIfMockTypeList.innerHTML = '';
  const frag = document.createDocumentFragment();
  WHATIF_MOCK_TYPE_OPTIONS.forEach((type) => {
    const row = document.createElement('div');
    row.className = 'd-flex align-items-center justify-content-between gap-2 mb-1';

    const label = document.createElement('label');
    label.className = 'small text-muted';
    label.textContent = mockTypeLabel(type);
    label.setAttribute('for', `whatIfMockType_${type}`);

    const input = document.createElement('input');
    input.type = 'number';
    input.min = '0';
    input.max = String(WHATIF_MOCK_BUILDING_LIMIT);
    input.value = '0';
    input.className = 'form-control form-control-sm';
    input.style.maxWidth = '110px';
    input.id = `whatIfMockType_${type}`;
    input.dataset.whatIfMockType = type;

    row.appendChild(label);
    row.appendChild(input);
    frag.appendChild(row);
  });
  whatIfMockTypeList.appendChild(frag);
}

function getWhatIfMockTypeCounts() {
  const inputs = whatIfMockTypeList?.querySelectorAll?.('[data-what-if-mock-type]') || [];
  const counts = [];
  let total = 0;
  inputs.forEach((input) => {
    const type = input.dataset.whatIfMockType;
    const raw = parseInt(input.value || '0', 10);
    const count = Number.isFinite(raw) ? Math.max(0, raw) : 0;
    if (count > 0) {
      counts.push({ type, count });
      total += count;
    }
  });
  const clampedTotal = Math.min(total, WHATIF_MOCK_BUILDING_LIMIT);
  if (total > WHATIF_MOCK_BUILDING_LIMIT) {
    let remaining = WHATIF_MOCK_BUILDING_LIMIT;
    counts.forEach((entry) => {
      entry.count = Math.min(entry.count, remaining);
      remaining -= entry.count;
    });
  }
  return { total: clampedTotal, counts };
}

function normalizeWhatIfMockCounts(rawCounts) {
  let entries = [];
  if (Array.isArray(rawCounts)) {
    entries = rawCounts;
  } else if (rawCounts && typeof rawCounts === 'object') {
    entries = Object.entries(rawCounts).map(([type, count]) => ({ type, count }));
  }
  const allowed = new Set(WHATIF_MOCK_TYPE_OPTIONS);
  const counts = [];
  let total = 0;
  entries.forEach((entry) => {
    const type = (entry?.type || entry?.category || entry?.cat || '').toString();
    if (!type || !allowed.has(type)) return;
    const raw = parseInt(entry.count || '0', 10);
    const count = Number.isFinite(raw) ? Math.max(0, raw) : 0;
    if (count > 0) {
      counts.push({ type, count });
      total += count;
    }
  });
  const clampedTotal = Math.min(total, WHATIF_MOCK_BUILDING_LIMIT);
  if (total > WHATIF_MOCK_BUILDING_LIMIT) {
    let remaining = WHATIF_MOCK_BUILDING_LIMIT;
    counts.forEach((entry) => {
      entry.count = Math.min(entry.count, remaining);
      remaining -= entry.count;
    });
  }
  return { total: clampedTotal, counts };
}

function setWhatIfMockTypeCounts(rawCounts) {
  if (!whatIfMockTypeList) return;
  const { counts } = normalizeWhatIfMockCounts(rawCounts);
  const byType = counts.reduce((acc, entry) => {
    acc[entry.type] = entry.count;
    return acc;
  }, {});
  const inputs = whatIfMockTypeList?.querySelectorAll?.('[data-what-if-mock-type]') || [];
  inputs.forEach((input) => {
    const type = input.dataset.whatIfMockType;
    const count = byType?.[type] || 0;
    input.value = String(count);
  });
}

function setWhatIfLassoStatus(text = '', isError = false) {
  if (!whatIfLassoStatus) return;
  whatIfLassoStatus.textContent = text;
  whatIfLassoStatus.classList.toggle('text-danger', !!isError);
  whatIfLassoStatus.classList.toggle('text-muted', !isError);
}

async function clearWhatIfMockBuildings() {
  if (baseCityFC?.features?.length) {
    baseCityFC.features = baseCityFC.features.filter((feat) => !feat?.properties?.__whatIfMock);
  }
  if (newbuildsFC?.features?.length) {
    newbuildsFC.features = newbuildsFC.features.filter((feat) => !feat?.properties?.__whatIfMock);
  }
  refreshBuildingTypeDropdown();
  updateLayers();
  setWhatIfLassoClearDisabled(true);
  setWhatIfLassoStatus('Mock buildings cleared.');
  showGlobalSpinner('Clearing & recomputing…');
  await waitForSpinnerPaint();
  await recomputeFairnessAfterWhatIf();
  hideGlobalSpinner();
}

async function applyWhatIfSuggestions() {
  if (!whatIfSuggestions.length) return;
  showGlobalSpinner('Applying suggestions…');
  await waitForSpinnerPaint();
  const beforeCategoryLabel = fairCategory && fairCategory !== 'mix'
    ? prettyPOIName(fairCategory)
    : (fairCategory === 'mix' ? 'Mix' : null);
  const beforeCategoryGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  let lastFeature = null;
  let lastCoord = null;
  whatIfSuggestions.forEach((suggestion) => {
    if (suggestion.kind === 'change' && suggestion.candidate?.feature) {
      const feature = suggestion.candidate.feature;
      const props = feature.properties || (feature.properties = {});
      applyPOITags(props, suggestion.cat);
      props.__whatIf = true;
      lastFeature = feature;
      lastCoord = suggestion.candidate.centroid;
    } else if (suggestion.kind === 'add' && Array.isArray(suggestion.location)) {
      lastFeature = createWhatIfBuilding(suggestion.location, suggestion.cat);
      lastCoord = suggestion.location;
    }
  });
  if (lastFeature && lastCoord) {
    showPopup(lastFeature, lastCoord);
  }
  const beforeSnapshot = new Map();
  (baseCityFC?.features || []).forEach((f, idx) => {
    const s = f?.properties?.fair?.score;
    if (Number.isFinite(s)) beforeSnapshot.set(idx, s);
  });
  const recomputeRes = await recomputeFairnessAfterWhatIf();
  const afterCategoryGini = Number.isFinite(recomputeRes?.categoryGini)
    ? recomputeRes.categoryGini
    : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(recomputeRes?.overallGini)
    ? recomputeRes.overallGini
    : (Number.isFinite(overallGini) ? overallGini : null);

  const beforeParts = [];
  const afterParts = [];
  if (beforeCategoryLabel && Number.isFinite(beforeCategoryGini)) {
    beforeParts.push(`${beforeCategoryLabel} Gini ${beforeCategoryGini.toFixed(3)}`);
  }
  if (Number.isFinite(beforeOverallGini)) {
    beforeParts.push(`Overall Gini ${beforeOverallGini.toFixed(3)}`);
  }
  if (beforeCategoryLabel && Number.isFinite(afterCategoryGini)) {
    afterParts.push(`${beforeCategoryLabel} Gini ${afterCategoryGini.toFixed(3)}`);
  }
  if (Number.isFinite(afterOverallGini)) {
    afterParts.push(`Overall Gini ${afterOverallGini.toFixed(3)}`);
  }

  const baseSummary = formatWhatIfSuggestionSummary(whatIfSuggestions);
  const metricSummary = (beforeParts.length && afterParts.length)
    ? ` Before: ${beforeParts.join(' | ')}. After: ${afterParts.join(' | ')}.`
    : '';
  updateWhatIfSuggestionUI(`${baseSummary}${metricSummary}`, {
    hasSuggestion: whatIfSuggestions.length > 0
  });
  const deltaPairs = applyDeltaColorsFromSnapshot(beforeSnapshot, changeLogIdCounter);
  recordWhatIfChange({
    action: 'ai_suggestion',
    description: baseSummary,
    category: whatIfSuggestions[0]?.cat || null,
    beforeGini: beforeCategoryGini,
    afterGini: afterCategoryGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: deltaPairs
  });
  hideGlobalSpinner();
  updateLayers();
}

function extractDisplayedGiniValue(text) {
  const match = String(text || '').match(/([-+]?\d*\.?\d+)\s*$/);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

