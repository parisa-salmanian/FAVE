// Fairness model — gravity / IF-City scoring, Gini computation, animated
// transitions, POI fetch+local extraction, POI-mix UI reader, and
// single/weighted/overall fairness compute. Extracted from main.js.
// Loaded as a classical script before main.js so its functions and
// shared state (poiCache, etc.) are visible across the Realm.

/* ======================= Fairness (scoring & Gini) ======================= */
function normalizeFairnessColorScheme(value) {
  const v = String(value || '').toLowerCase();
  return ['green-red', 'viridis', 'cool'].includes(v) ? v : FAIRNESS_COLOR_SCHEME_DEFAULT;
}

function colorFromInterpolator(interpolator, t, fallback=[128, 128, 128]) {
  if (typeof d3 === 'undefined' || typeof interpolator !== 'function') return fallback;
  const c = d3.color(interpolator(Math.max(0, Math.min(1, t))));
  if (!c) return fallback;
  return [Math.round(c.r), Math.round(c.g), Math.round(c.b)];
}

function fairnessLegendGradientCSS() {
  // Convention: gradient left → right = least fair → most fair, mirroring
  // the inspector / shell legend / map color scheme.
  if (fairnessColorScheme === 'green-red') {
    return 'linear-gradient(90deg, #d7191c 0%, #fdae61 50%, #1a9641 100%)';
  }
  const stops = [0, 0.25, 0.5, 0.75, 1].map((score, idx, arr) => {
    const pct = Math.round((idx / (arr.length - 1)) * 100);
    const [r, g, b] = colorFromScore(score);
    return `rgb(${r}, ${g}, ${b}) ${pct}%`;
  });
  return `linear-gradient(90deg, ${stops.join(', ')})`;
}

function changeLegendGradientCSS() {
  const CHANGE_DIVERGING = [
    [178,  24,  43],
    [214,  96,  77],
    [244, 165, 130],
    [253, 219, 199],
    [247, 247, 247],
    [209, 229, 240],
    [146, 197, 222],
    [ 67, 147, 195],
    [ 33, 102, 172],
  ];
  const stops = CHANGE_DIVERGING.map((c, idx, arr) => {
    const pct = Math.round((idx / (arr.length - 1)) * 100);
    return `rgb(${c[0]}, ${c[1]}, ${c[2]}) ${pct}%`;
  });
  return `linear-gradient(90deg, ${stops.join(', ')})`;
}

function updateFairnessLegendUI() {
  const sidePanel = document.getElementById('sidePanel');
  if (sidePanel?.dataset?.legendMode !== 'change') {
    const sideLegend = document.getElementById('fairnessLegendBar');
    if (sideLegend) sideLegend.style.background = fairnessLegendGradientCSS();
  }

  const drLegend = document.getElementById('drLegendGradient');
  if (drLegend) drLegend.style.background = fairnessLegendGradientCSS();
}

function setFairnessColorScheme(value, opts = {}) {
  const { refreshLayers = true } = opts;
  const nextScheme = normalizeFairnessColorScheme(value);
  if (nextScheme === fairnessColorScheme) return;

  fairnessColorScheme = nextScheme;
  fairRecolorTick++;
  if (fairnessColorSchemeSelect) fairnessColorSchemeSelect.value = fairnessColorScheme;
  updateFairnessLegendUI();

  if (drPlot?.points?.length) redrawDR();
  if (refreshLayers) updateLayers();
}

function colorFromScore(score) {
  const s = Math.max(0, Math.min(1, Number(score) || 0));

  // Convention used everywhere in the app: green = most fair (score → 1),
  // purple = least fair (score → 0). Pass the score through unchanged so
  // viridis/cool place yellow-green at the high-score end and dark purple
  // at the low-score end.
  if (fairnessColorScheme === 'viridis') {
    return colorFromInterpolator(d3?.interpolateViridis, s, [253, 231, 37]);
  }
  if (fairnessColorScheme === 'cool') {
    return colorFromInterpolator(d3?.interpolateCool, s, [110, 64, 170]);
  }

  let r, g;
  if (s <= 0.5) { const t = s/0.5; r = 255; g = Math.round(255*t); }
  else { const t = (s-0.5)/0.5; r = Math.round(255*(1-t)); g = 255; }
  return [r,g,0];
}

/* ---------- Animated transition helpers ---------- */
function lerpColor(c1, c2, t) {
  return [
    Math.round(c1[0] + (c2[0] - c1[0]) * t),
    Math.round(c1[1] + (c2[1] - c1[1]) * t),
    Math.round(c1[2] + (c2[2] - c1[2]) * t)
  ];
}

function blendColor(base, tint, amount) {
  const a = Math.max(0, Math.min(1, Number(amount) || 0));
  if (a <= 0) return [base[0], base[1], base[2]];
  if (a >= 1) return [tint[0], tint[1], tint[2]];
  return lerpColor(base, tint, a);
}

function smoothstep01(x) {
  const t = Math.max(0, Math.min(1, Number(x) || 0));
  return t * t * (3 - 2 * t);
}

function transitionBlendState() {
  const flashRatio = Math.max(0, Math.min(0.85, TRANSITION_CHANGED_FLASH_MS / TRANSITION_DURATION_MS));
  if (flashRatio <= 0) {
    return {
      flashAmount: 0,
      colorT: smoothstep01(transitionAnimT)
    };
  }

  const inFlash = transitionAnimT < flashRatio;
  const flashProgress = inFlash ? transitionAnimT / flashRatio : 1;
  const flashAmount = inFlash
    ? (1 - smoothstep01(flashProgress)) * TRANSITION_CHANGED_FLASH_MAX_INTENSITY
    : 0;

  const postFlashT = inFlash
    ? 0
    : (transitionAnimT - flashRatio) / (1 - flashRatio);

  return {
    flashAmount,
    colorT: smoothstep01(postFlashT)
  };
}

function saveTransitionScores() {
  // Buildings
  if (baseCityFC?.features?.length) {
    for (const f of baseCityFC.features) {
      const p = f?.properties;
      if (!p) continue;
      p.__prevScore = p?.fair?.score ?? p?.fair_overall?.score ?? null;
    }
  }
  // Mezo hexes
  transitionMezoPrevScoreByHex = new Map();
  if (Array.isArray(mezoHexData)) {
    for (const cell of mezoHexData) {
      const prev = cell?.__fairFocused ?? cell?.__score ?? null;
      cell.__prevScore = prev;
      if (cell?.hex && Number.isFinite(prev)) {
        transitionMezoPrevScoreByHex.set(cell.hex, prev);
      }
    }
  }
  // Districts
  if (districtFC?.features?.length) {
    for (const f of districtFC.features) {
      const p = f?.properties;
      if (!p) continue;
      p.__prevScore = p?.__fairFocused ?? p?.__score ?? p?.__fairOverall ?? null;
    }
  }
  transitionHasData = true;
  const btn = document.getElementById('changesReplayBtn');
  if (btn) btn.disabled = false;
}

function startTransitionReplay() {
  if (!transitionHasData || !fairActive) return;
  // Cancel any running animation
  if (transitionAnimRAF) { cancelAnimationFrame(transitionAnimRAF); transitionAnimRAF = null; }

  transitionAnimActive = true;
  transitionAnimT = 0;
  transitionAnimTick++;
  updateLayers();

  const btn = document.getElementById('changesReplayBtn');
  if (btn) { btn.classList.replace('btn-outline-info', 'btn-info'); }

  const startTime = performance.now();
  function tick(now) {
    const elapsed = now - startTime;
    transitionAnimT = Math.min(1, elapsed / TRANSITION_DURATION_MS);
    transitionAnimTick++;
    updateLayers();
    if (transitionAnimT < 1) {
      transitionAnimRAF = requestAnimationFrame(tick);
    } else {
      transitionAnimActive = false;
      transitionAnimRAF = null;
      if (btn) { btn.classList.replace('btn-info', 'btn-outline-info'); }
      updateLayers();
    }
  }
  transitionAnimRAF = requestAnimationFrame(tick);
}

function clearTransitionData() {
  transitionHasData = false;
  transitionAnimActive = false;
  transitionMezoPrevScoreByHex = new Map();
  if (transitionAnimRAF) { cancelAnimationFrame(transitionAnimRAF); transitionAnimRAF = null; }
  const btn = document.getElementById('changesReplayBtn');
  if (btn) { btn.disabled = true; btn.classList.replace('btn-info', 'btn-outline-info'); }
}

function transitionBuildingColor(props) {
  const prev = props?.__prevScore;
  const curr = props?.fair?.score ?? props?.fair_overall?.score ?? null;
  if (!Number.isFinite(prev) || !Number.isFinite(curr)) {
    return Number.isFinite(curr) ? colorFromScore(curr) : [160, 160, 160];
  }
  const { flashAmount, colorT } = transitionBlendState();
  const base = lerpColor(colorFromScore(prev), colorFromScore(curr), colorT);
  const hasChanged = Math.abs(curr - prev) > TRANSITION_CHANGE_EPSILON;
  if (!hasChanged) return base;
  return blendColor(base, TRANSITION_CHANGED_FLASH_COLOR, flashAmount);
  }

function transitionDistrictColor(props) {
  const prev = props?.__prevScore;
  const curr = districtOverlayScore(props);
  if (!Number.isFinite(prev) || !Number.isFinite(curr)) {
    if (Number.isFinite(curr)) { const [r,g,b] = colorFromScore(curr); return [r,g,b,110]; }
    return [80, 80, 80, 25];
  }
  const { flashAmount, colorT } = transitionBlendState();
  const base = lerpColor(colorFromScore(prev), colorFromScore(curr), colorT);
  const hasChanged = Math.abs(curr - prev) > TRANSITION_CHANGE_EPSILON;
  const c = blendColor(base, TRANSITION_CHANGED_FLASH_COLOR, hasChanged ? flashAmount : 0);
  return [c[0], c[1], c[2], 110];
}

function transitionMezoColor(props) {
  const prev = props?.__prevScore;
  const curr = mezoOverlayScore(props);
  if (!Number.isFinite(prev) || !Number.isFinite(curr)) {
    if (Number.isFinite(curr)) { const [r,g,b] = colorFromScore(curr); return [r,g,b,130]; }
    return [80, 80, 80, 25];
  }
  const { flashAmount, colorT } = transitionBlendState();
  const base = lerpColor(colorFromScore(prev), colorFromScore(curr), colorT);
  const hasChanged = Math.abs(curr - prev) > TRANSITION_CHANGE_EPSILON;
  const c = blendColor(base, TRANSITION_CHANGED_FLASH_COLOR, hasChanged ? flashAmount : 0);
  return [c[0], c[1], c[2], 130];
}

function withMezoPrevScore(cell = {}) {
  if (!cell?.hex) return cell;
  if (Number.isFinite(cell.__prevScore)) return cell;
  const prev = transitionMezoPrevScoreByHex.get(cell.hex);
  if (!Number.isFinite(prev)) return cell;
  return { ...cell, __prevScore: prev };
}

function clearDistrictFairnessView() {
  fairnessComputeGen++;
  if (districtFC?.features) {
    for (const f of districtFC.features) {
      if (!f.properties) continue;
      delete f.properties.__score;
      delete f.properties.__count;
      delete f.properties.__fairOverall;
      delete f.properties.__fairByCat;
      delete f.properties.__fairFocused;
      delete f.properties.__fairFocusedCat;
    }
  }

  fairActive = false;
  fairCategory = '';
  fairRecolorTick++;
  currentPOIsFC = null;
  selectedPOIMix = [];

  window.activePOICats = new Set();
  selectedPOIId = null;
  selectedPOIFeature = null;

  clearBestWorstHighlights();

  districtScoresSuppressed = true;
  districtScoreTick++;
  updateLayers();
  setParallelCoordsPending(false);
}

function clearFairness(clearOverall = false) {
  fairnessComputeGen++;
  if (baseCityFC?.features) {
    for (const f of baseCityFC.features) {
      if (f.properties) { delete f.properties.fair; delete f.properties.fair_multi; if (clearOverall) delete f.properties.fair_overall; }
    }
  }
  fairActive = false;
  fairCategory = '';
  fairRecolorTick++;
  currentPOIsFC = null;
  selectedPOIMix = [];
  currentCategoryGini = null;
  poiCache = clearOverall ? {} : poiCache;
  if (clearOverall) overallGini = null;

  window.activePOICats = new Set();
  selectedPOIId = null;
  selectedPOIFeature = null;

  clearBestWorstHighlights();

  refreshDistrictScores();
  refreshMezoScores();
  updateLayers();
  setParallelCoordsPending(false);
  window.faveInspector?.refresh?.();
}

function bboxForFC(fc, pad = 0.06) {
  if (!fc?.features?.length) throw new Error('Cannot compute bbox: empty feature collection.');
  const [minX, minY, maxX, maxY] = turf.bbox(fc);
  if (![minX, minY, maxX, maxY].every(Number.isFinite)) {
    throw new Error('Cannot compute bbox: invalid feature bounds.');
  }
  const dx = (maxX-minX)*pad, dy=(maxY-minY)*pad;
  return [minY-dy, minX-dx, maxY+dy, maxX+dx]; // south, west, north, east
}

// POI_QUERIES + Swedish school tag classifiers live in assets/js/lib/poi.js

// Earlier V1 of propsText/normalizeForMatch/textHasAny/isPrimarySchoolLike/isHighSchoolLike/buildPOIsFromBuildings was shadowed by a V2 below it; both V1 and V2 are now in assets/js/lib/poi.js (V1 deleted as dead code)

function dedupeLocalBuildingPOIs(cat, features = []) {
  // Local building datasets can contain several building polygons for the same
  // campus/facility name (e.g. many buildings all named "Heleneholms gymnasium").
  // Collapse those into one POI per name within a small radius.
  const byName = new Map();
  for (const feat of features) {
    const c = feat?.geometry?.coordinates;
    if (!Array.isArray(c) || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    const rawName = String(feat?.properties?.name || '').trim();
    const nameKey = normalizeForMatch(rawName || `__unnamed__${cat}`);
    if (!byName.has(nameKey)) byName.set(nameKey, []);
    byName.get(nameKey).push(feat);
  }

  const merged = [];
  const MERGE_RADIUS_M = 180;

  for (const arr of byName.values()) {
    const buckets = [];
    for (const feat of arr) {
      const c = feat.geometry.coordinates;
      let bucket = null;
      for (const b of buckets) {
        if (haversineMeters(c, b.center) <= MERGE_RADIUS_M) {
          bucket = b;
          break;
        }
      }
      if (!bucket) {
        buckets.push({ center: c, items: [feat] });
      } else {
        bucket.items.push(feat);
      }
    }

    for (const b of buckets) {
      // Prefer item that already has a non-empty name; otherwise first item.
      const named = b.items.find((f) => String(f?.properties?.name || '').trim());
      merged.push(named || b.items[0]);
    }
  }

  return merged;
}

function shouldFallbackToOverpass(cat, localFC) {
  // Keep the requested OSM+GeoJSON behavior (prefer local POIs), but avoid
  // a fully empty/green fairness state when local schema lacks explicit POI hints.
  if (sourceMode !== 'osm_s1') return false;
  const n = localFC?.features?.length || 0;
  // For schools the file often has many records; if we detected nothing, fallback.
  if (cat === 'school_primary' || cat === 'school_high') return n === 0;
  // For other categories, also fallback on empty local match set.
  return n === 0;
}



function buildPOIsFromBuildings(cat, fc) {
  const feats = [];
  for (const feat of (fc?.features || [])) {
    const props = feat?.properties || {};
    if (!buildingMatchesPOI(props, cat)) continue;
    let c = null;
    c = fastCentroid(feat);
    if (!c || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    feats.push({
      type: 'Feature',
      properties: {
        id: props.id || props['@id'] || null,
        osm_type: 'local_building',
        name: props.name || props.namn || '',
        category: cat,
        tags: props,
        __from_building_geojson: true
      },
      geometry: { type: 'Point', coordinates: c }
    });
  }
  // In OSM+GeoJSON mode, one facility can span many polygons with the same
  // name (e.g. multiple school buildings for one campus). Collapse those so a
  // real-world POI is counted once in fairness + shown once on the map.
  return { type: 'FeatureCollection', features: dedupeLocalBuildingPOIs(cat, feats) };
}

/* ---------- Fetch POIs with optional post-filter ---------- */
async function fetchPOIs(cat, fc) {
  /* ---- Baked POI file: the only path that should ever fire for supported cities ---- */
  const bakedCityKey = normalizeCityKey(lastCityName);
  if (bakedCityKey) {
    const bakedKey = `baked:${cat}:${bakedCityKey}`;
    if (poiCache[bakedKey]) return poiCache[bakedKey];
    try {
      const r = await fetch(`assets/data/cities/${bakedCityKey}/pois/${cat}.geojson`, { cache: 'force-cache' });
      if (r.ok) {
        const fcBaked = await r.json();
        const feats = fcBaked?.features || [];
        if (feats.length) {
          const out = { type: 'FeatureCollection', features: feats };
          poiCache[bakedKey] = out;
          console.log(`[FAVE] POIs from cache: ${bakedCityKey}/${cat} (${feats.length})`);
          return out;
        }
      }
    } catch (e) {
      console.warn(`[FAVE] baked POI read failed for ${bakedCityKey}/${cat}; falling through`, e);
    }
  }

  /* ---- OSM+GeoJSON mode: extract POIs locally, fall back to Overpass if empty ---- */
  if (sourceMode === 'osm_s1') {
    const localKey = `local:${cat}:${normalizeCityKey(lastCityName)}`;
    if (poiCache[localKey]) return poiCache[localKey];
    const localResult = extractLocalPOIs(cat, fc);
    console.log(`[OSM+GeoJSON] Extracted ${localResult.features.length} local POIs for "${cat}"`);
    if (localResult.features.length > 0) {
      poiCache[localKey] = localResult;
      return localResult;
    }
    console.warn(`[OSM+GeoJSON] No local POIs for "${cat}", falling back to Overpass…`);
    // fall through to Overpass fetch below
  }

  const [s,w,n,e] = bboxForFC(fc, 0.06);
  const key = `${sourceMode}:${cat}:${s.toFixed(3)},${w.toFixed(3)},${n.toFixed(3)},${e.toFixed(3)}`;
  if (poiCache[key]) return poiCache[key];

  if (sourceMode === 'osm_s1') {
    const localFC = buildPOIsFromBuildings(cat, fc);
    if (!shouldFallbackToOverpass(cat, localFC)) {
      poiCache[key] = localFC;
      return localFC;
    }
  }

  const selectors = POI_QUERIES[cat] || [];
  if (!selectors.length) return turf.featureCollection([]);

  const body = `
    [out:json][timeout:50];
    (
      ${selectors.map(q => `${q}(${s},${w},${n},${e});`).join('\n      ')}
    );
    out center tags;`;

  const json = await fetchOverpassJSON(body);
  if (!json) throw new Error('Overpass failed: no response');

  let feats = [];
  for (const el of (json.elements || [])) {
    let lon, lat;
    if (el.type === 'node') { lon = el.lon; lat = el.lat; }
    else if (el.center)     { lon = el.center.lon; lat = el.center.lat; }
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    const name = (el.tags && (el.tags.name || el.tags['name:sv'] || el.tags['name:en'])) ||
                 (el.tags && (el.tags.brand || el.tags['brand:sv'] || el.tags['brand:en'])) || '';
    feats.push({
      type:'Feature',
      properties: {
        id: el.id,
        osm_type: el.type,
        name,
        category: cat,
        tags: el.tags || {}
      },
      geometry:{ type:'Point', coordinates:[lon, lat] }
    });
  }

  // The same real-world POI can appear multiple times in Overpass output
  // (e.g. node + way + relation for one hospital, or overlapping selectors).
  // IF-City sums all POI contributions, so we must collapse these duplicates
  // to preserve paper logic (one physical facility should count once).
  const uniqueByObject = new Set();
  const uniqueByPlace = new Map();
  for (const feat of feats) {
    const [lon, lat] = feat?.geometry?.coordinates || [];
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    const props = feat.properties || {};
    const tags = props.tags || {};
    const osmType = String(props.osm_type || '').trim().toLowerCase();
    const osmId = props.id;

    // 1) exact duplicate object (same OSM type+id, repeated by overlapping selectors)
    if (osmType && Number.isFinite(osmId)) {
      const objectKey = `${osmType}:${osmId}`;
      if (uniqueByObject.has(objectKey)) continue;
      uniqueByObject.add(objectKey);
    }

    // 2) cross-object duplicate for one real facility (node + way + relation)
    const nameKey = String(props.name || '').trim().toLowerCase();
    const kindKey = String(tags.amenity || tags.healthcare || cat || '').trim().toLowerCase();
    const coordKey = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    const placeKey = `${coordKey}:${nameKey || kindKey}`;

    if (!uniqueByPlace.has(placeKey)) {
      uniqueByPlace.set(placeKey, feat);
    }
  }
  feats = Array.from(uniqueByPlace.values());

  if (cat === 'school_primary') {
    const filtered = feats.filter(f => isPrimarySchoolLike(f.properties.tags));
    feats = filtered.length ? filtered : feats;
  }
  if (cat === 'school_high') {
    const filtered = feats.filter(f => isHighSchoolLike(f.properties.tags));
    feats = filtered.length ? filtered : feats;
  }

  const fcOut = { type:'FeatureCollection', features: feats };
  poiCache[key] = fcOut;
  return fcOut;
}

/* ---------- Extract POIs from local Lantmäteriet building data ---------- */
/* --- Name-based patterns for byggnadsnamn fields (brands, common names) --- */
const NAME_POI_PATTERNS = {
  grocery:           /\b(ica|coop|willys|hemköp|lidl|netto|city\s*gross|matöppet|tempo|handlar|livs|dagligvaru|livsmedel|matbutik|supermarket)\b/i,
  pharmacy:          /\b(apotek|apotea|kronans|hjärtat)\b/i,
  dentistry:         /\b(tandläkar|folktandvård|tandklinik|tandvård|tand\s*(?:läkar|klinik|vård))\b/i,
  healthcare_center: /\b(vårdcentral|hälsocentral|husläkar|hälsovård)\b/i,
  veterinary:        /\b(veterinär|djurklinik|djursjukhus|evidensia|anicura)\b/i,
  kindergarten:      /\b(förskol|barnomsorg|daghem|dagis)\b/i,
  school_high:       /(gymnasium|gymnasie|gymnasi)/i,
  hospital:          /\b(sjukhus|lasarett)\b/i,
  university:        /\b(universitet|högskol)\b/i,
  school_primary:    /\b(grundskol|skola|skolan)\b/i,
};

function extractLocalPOIs(category, fc) {
  if (!fc?.features?.length) return turf.featureCollection([]);

  const feats = [];
  const seen = new Set();
  const namePattern = NAME_POI_PATTERNS[category] || null;

  for (const f of fc.features) {
    const props = f?.properties || {};

    // Collect all andamal + objekttyp fields into one lowercase string
    const searchText = [
      props.andamal1, props.andamal2, props.andamal3,
      props.andamal4, props.andamal5, props.objekttyp
    ].filter(Boolean).map(s => String(s).toLowerCase()).join(' ');

    // Collect byggnadsnamn fields
    const nameText = [
      props.byggnadsnamn1, props.byggnadsnamn2, props.byggnadsnamn3
    ].filter(Boolean).map(s => String(s)).join(' ');

    // 1) Check andamal keywords
    let matched = false;
    for (const [keyword, cat] of Object.entries(ANDAMAL_TO_POI_CATEGORY)) {
      if (cat !== category) continue;
      if (searchText.includes(keyword)) { matched = true; break; }
    }

    // 2) Check byggnadsnamn via regex patterns
    if (!matched && namePattern && nameText.trim()) {
      if (namePattern.test(nameText)) matched = true;
    }

    // 3) Also check byggnadsnamn against andamal keywords (e.g. name="Vårdcentral Oxhagen")
    if (!matched && nameText.trim()) {
      const nameLower = nameText.toLowerCase();
      for (const [keyword, cat] of Object.entries(ANDAMAL_TO_POI_CATEGORY)) {
        if (cat !== category) continue;
        if (nameLower.includes(keyword)) { matched = true; break; }
      }
    }

    if (!matched) continue;

    // Get centroid as POI point
    let lon, lat;
    let centroid;
    try { centroid = turf.centroid(f).geometry.coordinates; } catch (_) { continue; }
    if (!centroid) continue;
    [lon, lat] = centroid;
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    // Dedupe by ~1m precision
    const key = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const name = props.byggnadsnamn1 || props.byggnadsnamn2 || props.objekttyp || '';
    feats.push({
      type: 'Feature',
      properties: {
        id: props.objektidentitet || props.fid || feats.length,
        name,
        category,
        tags: {},
        __localSource: true
      },
      geometry: { type: 'Point', coordinates: [lon, lat] }
    });
  }

  return turf.featureCollection(dedupeLocalBuildingPOIs(category, feats));
}

async function fetchPOIsWithRetry(cat, fc, retries = FAIRNESS_POI_FETCH_RETRIES) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fetchPOIs(cat, fc);
    } catch (err) {
      lastError = err;
      if (attempt < retries) await sleep(OVERPASS_RETRY_DELAY_MS);
    }
  }
  throw lastError || new Error(`Unable to fetch POIs for ${cat}`);
}

function haversineMeters(a, b) {
  const R = 6371000; const toRad = d => d*Math.PI/180;
  const dLat = toRad(b[1]-a[1]); const dLon = toRad(b[0]-a[0]);
  const s = Math.sin(dLat/2)**2 + Math.cos(toRad(a[1]))*Math.cos(toRad(b[1]))*Math.sin(dLon/2)**2;
  return 2*R*Math.asin(Math.sqrt(s));
}

function normalizeTravelMode(mode) {
  const normalized = String(mode || '').toLowerCase();
  if (normalized === 'car') return 'driving';
  if (normalized === 'pt' || normalized === 'public_transport' || normalized === 'public-transport') return 'transit';
  if (['walking', 'cycling', 'driving', 'transit'].includes(normalized)) return normalized;
  return FAIRNESS_TRAVEL_MODE_DEFAULT;
}

function normalizeFairnessModel(mode) {
  const normalized = String(mode || '').toLowerCase();
  if (normalized === 'if-city') return 'ifcity';
  if (['default', 'ifcity'].includes(normalized)) return normalized;
  return FAIRNESS_MODEL_DEFAULT;
}

function setFairnessTravelMode(mode, { recompute = true } = {}) {
  const normalized = normalizeTravelMode(mode);
  fairnessTravelMode = normalized;
  if (fairnessTravelModeSelect) fairnessTravelModeSelect.value = normalized;
  if (recompute) recomputeFairnessAfterWhatIf();
}

function setFairnessModel(mode, { recompute = true } = {}) {
  const normalized = normalizeFairnessModel(mode);
  fairnessModel = normalized;
  if (fairnessModelSelect) fairnessModelSelect.value = normalized;
  if (recompute) recomputeFairnessAfterWhatIf();
}

function profileForMode(mode) {
  const normalized = normalizeTravelMode(mode);
  return normalized === 'driving' ? 'driving' : normalized;
}

function speedKmhForMode(mode) {
  const normalized = normalizeTravelMode(mode);
  return TRAVEL_SPEED_KMH[normalized] || TRAVEL_SPEED_KMH[FAIRNESS_TRAVEL_MODE_DEFAULT];
}

const routeMetricsCache = new Map();

function routeCacheKey(a, b, profile) {
  const fmt = (coord) => `${Number(coord[0]).toFixed(5)},${Number(coord[1]).toFixed(5)}`;
  return `${profile}:${fmt(a)}|${fmt(b)}`;
}

function pruneRouteCache() {
  if (routeMetricsCache.size <= ROUTING_CACHE_LIMIT) return;
  let excess = routeMetricsCache.size - ROUTING_CACHE_LIMIT;
  for (const key of routeMetricsCache.keys()) {
    routeMetricsCache.delete(key);
    excess -= 1;
    if (excess <= 0) break;
  }
}

function estimateTravelTimeSecondsFromMeters(meters, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  if (!Number.isFinite(meters)) return Infinity;
  const speedKmh = speedKmhForMode(mode);
  const hours = (meters / 1000) / Math.max(0.1, speedKmh);
  return hours * 3600;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchOverpassJSON(body) {
  let lastError = null;
  const apiBase = (typeof API_BASE !== "undefined" && API_BASE) ? API_BASE : null;
  if (apiBase) {
    try {
      const res = await fetchWithTimeout(
        `${apiBase}/overpass/query`,
        { method: 'POST', body, headers: { 'Content-Type': 'text/plain' } },
        OVERPASS_TIMEOUT_MS
      );
      if (!res.ok) {
        const detail = await res.text();
        throw new Error(`Overpass proxy failed: ${res.status} ${detail}`.trim());
      }
      return await res.json();
    } catch (err) {
      lastError = err;
    }
  }

  for (let i = 0; i < OVERPASS_ENDPOINTS.length; i++) {
    const url = OVERPASS_ENDPOINTS[i];
    try {
      const res = await fetchWithTimeout(
        url,
        { method: 'POST', body, headers: { 'Content-Type': 'text/plain' } },
        OVERPASS_TIMEOUT_MS
      );
      if (!res.ok) {
        lastError = new Error(`Overpass failed: ${res.status}`);
        if (res.status === 429 || res.status === 504) {
          await sleep(OVERPASS_RETRY_DELAY_MS);
          continue;
        }
        continue;
      }
      return await res.json();
    } catch (err) {
      lastError = err;
      await sleep(OVERPASS_RETRY_DELAY_MS);
    }
  }
  if (lastError) throw lastError;
  return null;
}

function allowRoutingForFairness(poiCount) {
  if (!USE_TRAVEL_TIME) return false;
  const buildingCount = baseCityFC?.features?.length ?? 0;
  if (buildingCount > ROUTING_MAX_BUILDINGS) return false;
  if (Number.isFinite(poiCount) && poiCount > ROUTING_MAX_POIS) return false;
  return true;
}

async function getTravelMetricsForPair(
  from,
  to,
  fallbackMeters,
  allowRouting = true,
  mode = FAIRNESS_TRAVEL_MODE_DEFAULT
) {
  if (!allowRouting) {
    return {
      meters: fallbackMeters,
      seconds: estimateTravelTimeSecondsFromMeters(fallbackMeters, mode)
    };
  }
  const routeMetrics = await fetchRouteMetrics(from, to, { profile: profileForMode(mode) });
  const routeMeters = Number.isFinite(routeMetrics?.meters) ? routeMetrics.meters : fallbackMeters;
  const routeSeconds = Number.isFinite(routeMetrics?.seconds)
    ? routeMetrics.seconds
    : estimateTravelTimeSecondsFromMeters(routeMeters, mode);
  return { meters: routeMeters, seconds: routeSeconds };
}

async function fetchRouteMetrics(from, to, { profile = ROUTING_PROFILE } = {}) {
  if (!USE_TRAVEL_TIME) return null;
  if (!ROUTING_BASE_URL) return null;
  if (!Array.isArray(from) || !Array.isArray(to)) return null;
  if (![from[0], from[1], to[0], to[1]].every(Number.isFinite)) return null;

  const key = routeCacheKey(from, to, profile);
  if (routeMetricsCache.has(key)) return routeMetricsCache.get(key);

  const url = `${ROUTING_BASE_URL}/route/v1/${profile}/${from[0]},${from[1]};${to[0]},${to[1]}?overview=false&alternatives=false`;
  try {
    const res = await fetchWithTimeout(url, {}, ROUTING_TIMEOUT_MS);
    if (!res.ok) return null;
    const json = await res.json();
    if (json.code === 'Ok' && json.routes && json.routes[0]) {
      const route = json.routes[0];
      const metrics = {
        meters: Number(route.distance),
        seconds: Number(route.duration)
      };
      routeMetricsCache.set(key, metrics);
      pruneRouteCache();
      return metrics;
    }
  } catch (err) {
    return null;
  }
  return null;
}

function scoreFromDistanceMeters(cat, meters) {
  const km = meters/1000.0;
  const T = {
    grocery:[0.5,1.5], hospital:[1.2,4.0], pharmacy:[0.7,2.0], dentistry:[0.7,2.0],
    healthcare_center:[1.0,3.0], veterinary:[1.0,3.0], university:[1.2,4.0],
    kindergarten:[0.5,1.5], school_primary:[0.6,1.8], school_high:[1.0,2.5]
  };
  const [ideal,max] = T[cat] || [0.8,2.0];
  const t = (km - ideal) / Math.max(1e-6, (max - ideal));
  return 1 - Math.max(0, Math.min(1, t));
}


//Table for distances
function scoreFromTimeSeconds(cat, seconds, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  const effort = FAIRNESS_EFFORT_MULTIPLIER[normalizeTravelMode(mode)] ?? 1;
  const minutes = (seconds / 60) * effort;
  const T = {
    grocery:[6,18], hospital:[14,48], pharmacy:[8,24], dentistry:[8,24],
    healthcare_center:[12,36], veterinary:[12,36], university:[14,48],
    kindergarten:[6,18], school_primary:[7,22], school_high:[12,30]
  };
  const [ideal,max] = T[cat] || [10,24];
  const t = (minutes - ideal) / Math.max(1e-6, (max - ideal));
  return 1 - Math.max(0, Math.min(1, t));
}
function gini(values) {
  const x = values.filter(v => Number.isFinite(v)).slice().sort((a,b)=>a-b);
  const n = x.length; if (!n) return 0;
  const mean = x.reduce((s,v)=>s+v,0)/n; if (mean === 0) return 0;
  let cum = 0; for (let i=0;i<n;i++) cum += (2*(i+1)-n-1)*x[i];
  return Math.abs(cum)/(n*n*mean);
}

function generalizedEntropy(values, alpha = IF_CITY_ALPHA) {
  const x = values.filter(v => Number.isFinite(v));
  const n = x.length;
  if (!n) return 0;
  const mean = x.reduce((s, v) => s + v, 0) / n;
  if (mean === 0) return 0;
  if (alpha === 0) {
    const term = x.reduce((s, v) => s + Math.log(mean / Math.max(v, 1e-9)), 0);
    return term / n;
  }
  if (alpha === 1) {
    const term = x.reduce((s, v) => s + (v / mean) * Math.log(Math.max(v, 1e-9) / mean), 0);
    return term / n;
  }
  const term = x.reduce((s, v) => s + Math.pow(v / mean, alpha) - 1, 0);
  return term / (n * alpha * (alpha - 1));
}

function ifCityKappa(cat) {
  return IF_CITY_KAPPA_BY_CAT[cat] ?? IF_CITY_KAPPA_DEFAULT;
}

function ifCityPriorityWeight(cat) {
  return IF_CITY_PRIORITY_WEIGHTS[cat] ?? 1;
}

function ifCityDistanceForMode(km, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  const normalized = normalizeTravelMode(mode);
  const behaviorFactor = IF_CITY_MODE_DISTANCE_FACTOR[normalized] ?? 1;
  const detourFactor = IF_CITY_MODE_DETOUR_FACTOR[normalized] ?? 1;
  const speed = IF_CITY_MODE_SPEED_KMH[normalized] ?? IF_CITY_REFERENCE_SPEED_KMH;
  const networkKm = km * detourFactor;
  const travelHours = networkKm / Math.max(1e-6, speed);
  return travelHours * IF_CITY_REFERENCE_SPEED_KMH * behaviorFactor;
}

// Like ifCityDistanceForMode but for REAL network distances (baked routing
// matrices): the detour factor is dropped because the distance already follows
// the street/path network. Speed (→ travel time) and the per-mode behaviour
// factor are kept so modes stay comparable.
function ifCityNetworkDistanceForMode(km, mode = FAIRNESS_TRAVEL_MODE_DEFAULT) {
  const normalized = normalizeTravelMode(mode);
  const behaviorFactor = IF_CITY_MODE_DISTANCE_FACTOR[normalized] ?? 1;
  const speed = IF_CITY_MODE_SPEED_KMH[normalized] ?? IF_CITY_REFERENCE_SPEED_KMH;
  const travelHours = km / Math.max(1e-6, speed);
  return travelHours * IF_CITY_REFERENCE_SPEED_KMH * behaviorFactor;
}

function ifCityEquityWeightForFeature(feature) {
  const props = feature?.properties || {};
  // IF-City equity weighting should come from population/social group signals.
  // If those are missing, do NOT fall back to building land-use/category, because
  // that can make neighboring buildings around the same POI appear unfairly
  // different (e.g. commercial vs residential parcel effects).
  // Keep neutral weight in that case.
  const group = props.resident_group || props.population_group || 'Other / unknown';
  let weight = IF_CITY_EQUITY_WEIGHTS[group] ?? 1;

  // For mock buildings, scale equity weight by estimated capacity (floors × area proxy).
  // More residents → more demand → gravity model treats this building as more important.
  if (props.__whatIfMock && Number.isFinite(props.__mockCapacity) && props.__mockCapacity > 0) {
    // Logarithmic scaling so very tall buildings don't dominate completely
    weight *= Math.max(1, Math.log2(props.__mockCapacity));
  }

  return weight;
}

function ifCityAccessibilityForBuilding(cB, poiArr, cat, mode = FAIRNESS_TRAVEL_MODE_DEFAULT, transitCtx = null, netCtx = null, out = null) {
  // `out` (optional): if provided, out.distM is set to the raw distance to the
  // nearest POI of this category — network metres in the routing branch, straight-
  // line metres otherwise — so callers can surface "how far is the nearest one"
  // (the inspector distance line) without a second pass over the POIs.
  // Only the nearest 3 POIs contribute, to prevent city-centre accumulation effect.
  // Hot path: this runs ~N_buildings × N_categories times during a fairness compute,
  // so we avoid the previous map().sort() (O(P log P) + 1 alloc per call) and pick
  // the top-3 distances in a single pass with no intermediate allocation.
  const kappa = ifCityKappa(cat);
  const rho = ifCityPriorityWeight(cat);

  // Network branch: use baked REAL network distances (walk/cycle/drive) when a
  // routing matrix is loaded for this category and it has no what-if edits.
  if (netCtx && netCtx.pt && netCtx.cats && netCtx.cats.has(cat)
      && typeof routingDistsForPoint === 'function') {
    const r = routingDistsForPoint(cat, netCtx.pt[0], netCtx.pt[1]);
    if (r && r.dists && r.dists.length) {
      let sum = 0;
      const k = Math.min(3, r.dists.length);
      for (let i = 0; i < k; i++) {
        const dm = r.dists[i];
        if (!Number.isFinite(dm)) continue;
        const v = Number.isFinite(r.vals?.[i]) ? r.vals[i] : 1;
        sum += rho * v * Math.exp(-kappa * ifCityNetworkDistanceForMode(dm / 1000, mode));
      }
      if (out) out.distM = Number.isFinite(r.dists[0]) ? r.dists[0] : NaN;
      return sum;
    }
    // no baked row for this building → fall through to haversine
  }

  let d0 = Infinity, d1 = Infinity, d2 = Infinity;
  let p0 = null, p1 = null, p2 = null;
  const len = poiArr.length;
  for (let k = 0; k < len; k++) {
    const poi = poiArr[k];
    const dist = haversineMeters(cB, poi.c);
    if (dist < d0)      { d2 = d1; p2 = p1; d1 = d0; p1 = p0; d0 = dist; p0 = poi; }
    else if (dist < d1) { d2 = d1; p2 = p1; d1 = dist; p1 = poi; }
    else if (dist < d2) { d2 = dist; p2 = poi; }
  }

  // Effective travel distance (normalised to walking-equivalent km). For the
  // transit mode with a baked network loaded, use the real stop network; fall
  // back to the per-mode speed model (also used by walking/cycling/driving).
  const useTransit = !!(transitCtx && transitCtx.ready);
  const effKm = (poi, distMeters) => {
    if (useTransit) {
      const tk = transitEffectiveKm(transitCtx.originStops, poi);
      if (tk != null) return tk;
    }
    return ifCityDistanceForMode(distMeters / 1000, mode);
  };

  let sum = 0;
  if (p0) {
    const v = Number.isFinite(p0.v) ? p0.v : 1;
    sum += rho * v * Math.exp(-kappa * effKm(p0, d0));
  }
  if (p1) {
    const v = Number.isFinite(p1.v) ? p1.v : 1;
    sum += rho * v * Math.exp(-kappa * effKm(p1, d1));
  }
  if (p2) {
    const v = Number.isFinite(p2.v) ? p2.v : 1;
    sum += rho * v * Math.exp(-kappa * effKm(p2, d2));
  }
  if (out) out.distM = Number.isFinite(d0) ? d0 : NaN;
  return sum;
}

function normalizeBenefitsToScores(benefits, mode, allowAbsolute = false) {
  // EXPERIMENTAL absolute color scale (removable — see lib/absoluteColorScale.js).
  // Only the overall building-color pass opts in (allowAbsolute=true); per-category
  // and DR/PC normalizations keep the relative logic. When the toggle is off, or
  // the module isn't loaded, this guard is false and the original path runs.
  if (allowAbsolute
      && typeof window !== 'undefined'
      && typeof window.fairAbsoluteScaleActive === 'function'
      && window.fairAbsoluteScaleActive()) {
    return window.fairAbsoluteNormalize(benefits, mode);
  }
  // Robust normalization for map coloring:
  // - avoids a single global outlier dominating (old min-max issue),
  // - avoids saturating almost everything to high scores (absolute exp issue).
  // We log-compress and then scale using middle percentiles.
  //
  // Mode-aware floor: walking (baseline) uses p10 as floor for maximum
  // within-mode contrast.  Faster modes (cycling, driving) anchor the
  // floor at 0 so their higher absolute accessibility translates into
  // visibly greener map colors, making cross-mode differences obvious.
  const vals = benefits.filter(v => Number.isFinite(v) && v > 0).map(v => Math.log1p(v));
  if (!vals.length) return benefits.map(() => 0);

  const sorted = vals.slice().sort((a, b) => a - b);
  const q = (p) => {
    if (sorted.length === 1) return sorted[0];
    const i = (sorted.length - 1) * p;
    const lo = Math.floor(i);
    const hi = Math.ceil(i);
    if (lo === hi) return sorted[lo];
    const t = i - lo;
    return sorted[lo] * (1 - t) + sorted[hi] * t;
  };

  const p10 = q(0.10);
  const p95 = q(0.95);

  const isBaseline = !mode || normalizeTravelMode(mode) === 'walking';
  const floor = isBaseline ? p10 : 0;
  const span = Math.max(1e-9, p95 - floor);

  return benefits.map((v) => {
    if (!Number.isFinite(v) || v <= 0) return 0;
    const x = Math.log1p(v);
    const score = (x - floor) / span;
    return Math.max(0, Math.min(1, score));
  });
}

function ifCityOpportunityWeightFromPOI(cat, feature) {
  const tags = feature?.properties?.tags || {};
  // Keep IF-City paper logic (opportunity mass v_j), but avoid using OSM
  // geometry area as a proxy because it can severely over-weight one POI.
  // For open OSM data, capacity tags are sparse/heterogeneous, so default
  // to equal opportunities unless we have an explicit numeric capacity signal.
  const beds = Number(tags.beds);
  const capacity = Number(tags.capacity);
  if (cat === 'hospital' && Number.isFinite(beds) && beds > 0) {
    return Math.max(1, Math.min(10, beds / 100));
  }
  if (Number.isFinite(capacity) && capacity > 0) {
    return Math.max(1, Math.min(10, capacity / 100));
  }
  return 1;
}

function dedupeIFCityPOIs(items) {
  const unique = new Map();
  for (const item of (items || [])) {
    const c = item?.c || [];
    const lon = Number(c[0]);
    const lat = Number(c[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    // In OSM, one physical facility can still appear as multiple entries with
    // different names/tags but identical coordinates (e.g. hospital wing names).
    // IF-City sums all opportunities, so keeping those duplicates can over-boost
    // one location and make nearby buildings look unfairly better.
    // Use coordinate-only dedupe at ~1 m precision to keep one opportunity per
    // colocated facility.
    const key = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    if (!unique.has(key)) unique.set(key, item);
  }
  return Array.from(unique.values());
}

function poiFeatureCoord(feature) {
  const geom = feature?.geometry;
  if (!geom) return null;
  if (geom.type === 'Point' && Array.isArray(geom.coordinates)) return geom.coordinates;
  try { return turf.centroid(feature).geometry.coordinates; } catch { return null; }
}

function dedupePOIFeaturesForDisplay(features) {
  const unique = new Map();
  for (const feat of (features || [])) {
    const coord = poiFeatureCoord(feat);
    if (!Array.isArray(coord)) continue;
    const lon = Number(coord[0]);
    const lat = Number(coord[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    const cat = String(poiCategoryOf(feat) || feat?.properties?.__cat || '').toLowerCase();
    const key = `${cat}|${lon.toFixed(5)},${lat.toFixed(5)}`;
    if (!unique.has(key)) unique.set(key, feat);
  }
  return Array.from(unique.values());
}

/* ---------- Read POI mix from UI ---------- */
function readPOIMixFromUI() {
  const entries = [];
  document.querySelectorAll('.poi-check').forEach(chk => {
    const cat = chk.getAttribute('data-cat');
    const wEl = document.querySelector(`.poi-weight[data-cat="${cat}"]`);
    const w = parseFloat(wEl?.value || '0');
    if (chk.checked && w > 0) entries.push({ cat, weight: w });
  });
  return entries;
}

/* ======================= Växjö population-weighted gravity helpers ======================= */
/**
 * Load the Lantmäteriet buildings file and build a centroid index.
 * Used to enrich OSM buildings (which have category='unknown') with
 * proper objekttyp values (e.g. 'Bostad' for residential).
 * Only loaded once and cached in lantmaterietIndex.
 */
async function ensureLantmaterietIndex() {
  if (lantmaterietIndex) return lantmaterietIndex;
  try {
    const fc = await fetch(CITY_URL).then(r => r.json());
    lantmaterietIndex = (fc.features || [])
      .filter(f => f?.geometry && f?.properties?.objekttyp)
      .map(f => {
        let centroid;
        try { centroid = turf.centroid(f).geometry.coordinates; } catch { return null; }
        return { centroid, objekttyp: f.properties.objekttyp };
      })
      .filter(Boolean);
    console.log('[Lantmäteriet] Index built:', lantmaterietIndex.length, 'buildings');
  } catch (err) {
    console.warn('[Lantmäteriet] Failed to load index:', err);
    lantmaterietIndex = [];
  }
  return lantmaterietIndex;
}

/**
 * Find the objekttyp from the Lantmäteriet index for a given [lng, lat] centroid.
 * Matches the nearest Lantmäteriet building within 40 metres.
 */
function lookupObjekttyp(coord, index) {
  if (!index?.length) return null;
  let bestDist = 120; // increased to 120 metres for large buildings
  let bestObjekttyp = null;
  for (const item of index) {
    const d = haversineMeters(coord, item.centroid);
    if (d < bestDist) {
      bestDist = d;
      bestObjekttyp = item.objekttyp;
    }
  }
  return bestObjekttyp;
}

/** Build a flat spatial index from districtFC for point-in-polygon lookups */
function buildVaxjoDistrictSpatialIndex() {
  if (!districtFC?.features) return [];
  return districtFC.features.map(feat => ({
    feat,
    code: regsoCodeFromProps(feat.properties || {})
  }));
}

/** Find the regso district code for a [lng, lat] coordinate */
function findDistrictCodeForPoint(coord, index) {
  const pt = turf.point(coord);
  for (const { feat, code } of index) {
    try { if (turf.booleanPointInPolygon(pt, feat)) return code; } catch {}
  }
  return null;
}

/**
 * Estimate building floor area: footprint_m2 × number_of_floors
 * Floors derived from height (÷3) or building:levels property.
 */
function buildingFloorAreaM2(feature) {
  let footprintM2 = 80;
  try {
    const area = turf.area(feature);
    if (area > 0) footprintM2 = area;
  } catch {}
  const props = feature?.properties || {};
  const height = Number(props.height || props.Hojd || props['building:height'] || 0);
  const levels = Number(props['building:levels'] || props.floors || 0);
  const floors = levels > 0 ? levels : (height > 0 ? Math.max(1, Math.round(height / 3)) : 1);
  return footprintM2 * Math.min(floors, 40);
}

/**
 * Distribute district population across residential buildings proportionally by floor area.
 *
 * For each district:
 *   totalFloorArea = sum of floorAreaM2 of all residential buildings inside it
 *   buildingPop[i] = (buildingFloorArea[i] / totalFloorArea) × districtTotalPopulation
 *
 * Returns Map<featureIndex, estimatedResidents>
 */
async function buildVaxjoBuildingPopulationMap(districtSpatialIndex) {
  const popData = await ensureGenderAgePopulationData().catch(() => null);
  if (!popData || !districtFC?.features || !baseCityFC?.features) return new Map();
  // Load Lantmäteriet index to resolve 'unknown' OSM building types
  const lmIndex = await ensureLantmaterietIndex();

  const demandDebug = {
    totalBuildings: Array.isArray(baseCityFC?.features) ? baseCityFC.features.length : 0,
    unknownTreatedAsResidentialStepB: 0,
    unknownTreatedAsResidentialStepC: 0,
    unknownResolvedByLantmateriet: 0,
    residentialBuildingsCounted: 0,
    assignedPopulationBuildings: 0
  };

  // Step A: build district → total population lookup
  const districtPop = new Map();
  for (const feat of districtFC.features) {
    const code = regsoCodeFromProps(feat.properties || {});
    if (!code) continue;
    const entry = lookupPopulationEntry(popData, code);
    if (!entry) continue;
    const years = extractAvailablePopulationYears(entry);
    const latestYear = years.length ? years[years.length - 1] : null;
    if (!latestYear) continue;
    const totals = aggregatePopulationTotals(entry, latestYear, 'All');
    if (totals?.total > 0) districtPop.set(code, totals.total);
  }

  // Step B: for each district, sum floor area of all residential buildings inside it
  const districtTotalFloor = new Map();
  baseCityFC.features.forEach((feat) => {
    const props = feat.properties || {};
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; } catch { return; }
    if (!centroid) return;
    // Try OSM category first, fall back to Lantmäteriet lookup for 'unknown' buildings
    const rawCategory = props.category || props.building || props.objekttyp || '';
    const lmObjekttyp = (rawCategory === 'unknown' || !rawCategory)
      ? (lookupObjekttyp(centroid, lmIndex) || '')
      : '';
    const resolvedCategory = lmObjekttyp || rawCategory;
    const isUnknownRaw = String(rawCategory).toLowerCase() === 'unknown';
    const isUnknownWithoutMatch = !lmObjekttyp && isUnknownRaw;
    if (isUnknownRaw && lmObjekttyp) demandDebug.unknownResolvedByLantmateriet += 1;
    const isResidential =
      isUnknownWithoutMatch ||
      resolvedCategory.toLowerCase().includes('bostad') ||
      canonicalBuildingType(resolvedCategory) === 'residential';
    if (isUnknownWithoutMatch) demandDebug.unknownTreatedAsResidentialStepB += 1;
    if (!isResidential) return;
    demandDebug.residentialBuildingsCounted += 1;
    const code = findDistrictCodeForPoint(centroid, districtSpatialIndex);
    if (!code) return;
    const floorArea = buildingFloorAreaM2(feat);
    districtTotalFloor.set(code, (districtTotalFloor.get(code) || 0) + floorArea);
  });

  // Step C: assign each residential building its proportional population share
  const buildingPopMap = new Map();
  baseCityFC.features.forEach((feat, idx) => {
    const props = feat.properties || {};
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; } catch { return; }
    if (!centroid) return;
    // Same resolution logic as Step B
    const rawCategory = props.category || props.building || props.objekttyp || '';
    const lmObjekttyp = (rawCategory === 'unknown' || !rawCategory)
      ? (lookupObjekttyp(centroid, lmIndex) || '')
      : '';
    const resolvedCategory = lmObjekttyp || rawCategory;
    const isUnknownWithoutMatch = !lmObjekttyp && String(rawCategory).toLowerCase() === 'unknown';
    const isResidential =
      isUnknownWithoutMatch ||
      resolvedCategory.toLowerCase().includes('bostad') ||
      canonicalBuildingType(resolvedCategory) === 'residential';
    if (isUnknownWithoutMatch) demandDebug.unknownTreatedAsResidentialStepC += 1;
    if (!isResidential) return;
    const code = findDistrictCodeForPoint(centroid, districtSpatialIndex);
    if (!code) return;
    const totalFloor = districtTotalFloor.get(code) || 0;
    const totalPop   = districtPop.get(code) || 0;
    if (totalFloor <= 0 || totalPop <= 0) return;
    const floorArea = buildingFloorAreaM2(feat);
    buildingPopMap.set(idx, (floorArea / totalFloor) * totalPop);
    demandDebug.assignedPopulationBuildings += 1;
  });

  window.__ifcityDemandDebug = demandDebug;
  return buildingPopMap;
}

/**
 * Map each building to its district's composite need (deprivation) z-score, so the
 * IF-City equity weight can be raised in high-need areas. Geometry-only (independent
 * of the toggle/strength), built once at init. Needs cityNeedByRegso (demographics.js).
 */
function buildVaxjoBuildingNeedMap(districtSpatialIndex) {
  const out = new Map();
  if (!cityNeedByRegso || !cityNeedByRegso.size || !baseCityFC?.features) return out;
  baseCityFC.features.forEach((feat, idx) => {
    let centroid;
    try { centroid = turf.centroid(feat).geometry.coordinates; } catch { return; }
    if (!centroid) return;
    const code = findDistrictCodeForPoint(centroid, districtSpatialIndex);
    if (!code) return;
    const nz = cityNeedByRegso.get(code);
    if (Number.isFinite(nz)) out.set(idx, nz);
  });
  return out;
}

/** Initialize demand weights once both districtFC and baseCityFC are loaded (Växjö only) */
async function initVaxjoDemandWeights() {
  if (!districtFC?.features?.length || !baseCityFC?.features?.length) return;
  if (districtCityKeyFromInput(lastCityName) !== 'vaxjo') return;
  vaxjoDistrictIndex  = buildVaxjoDistrictSpatialIndex();
  vaxjoBuildingPopMap = await buildVaxjoBuildingPopulationMap(vaxjoDistrictIndex);
  // Make sure demographics are loaded before mapping buildings -> need index.
  if (typeof ensureDemographicsData === 'function') {
    await ensureDemographicsData(activeDistrictCityKey).catch(() => null);
  }
  vaxjoBuildingNeedMap = buildVaxjoBuildingNeedMap(vaxjoDistrictIndex);
  console.log('[IF-City] Växjö demand weights ready —',
    vaxjoBuildingPopMap.size, 'residential buildings assigned population estimates;',
    vaxjoBuildingNeedMap.size, 'buildings with need index');
  if (window.__ifcityDemandDebug) {
    console.log('[IF-City] Demand debug:', window.__ifcityDemandDebug);
  }
}

async function computeIfCityFairness(catList, weightsByCat = {}, { setOverall = false } = {}) {
  if (!baseCityFC) throw new Error('No buildings loaded.');
  const updateUI = !setOverall;
  const myGen = updateUI ? fairnessComputeGen : null;

  // Lazy-build the per-building need map if need-weighting is on but the map is
  // empty. This must NOT gate on vaxjoDistrictIndex: initVaxjoDemandWeights bails
  // early when districts load before buildings, leaving the index null — so we
  // (re)build the district index here too, from districtFC (already loaded).
  if (needWeightEnabled && cityNeedByRegso?.size
      && (!vaxjoBuildingNeedMap || !vaxjoBuildingNeedMap.size)) {
    if ((!vaxjoDistrictIndex || !vaxjoDistrictIndex.length) && districtFC?.features?.length) {
      vaxjoDistrictIndex = buildVaxjoDistrictSpatialIndex();
    }
    if (vaxjoDistrictIndex?.length) {
      vaxjoBuildingNeedMap = buildVaxjoBuildingNeedMap(vaxjoDistrictIndex);
    }
  }

  const fetched = await Promise.all(
    catList.map(cat =>
      fetchPOIs(cat, baseCityFC)
        .then(fc => ({ cat, fc }))
        .catch(() => ({ cat, fc: { type:'FeatureCollection', features:[] } }))
    )
  );

  const catToPOI = {};
  for (const { cat, fc } of fetched) {
    const filtered = filterFetchedPOIsForWhatIf(fc?.features || []);
    catToPOI[cat] = filtered.map(p => ({
      c: p.geometry.coordinates,
      name: p.properties?.name || '(unnamed)',
      v: ifCityOpportunityWeightFromPOI(cat, p)
    }));
  }

  const poiCount = fetched.reduce((sum, { fc }) => sum + (fc?.features?.length || 0), 0);

  if (updateUI) {
    const rawFeatures = fetched.flatMap(({cat, fc}) =>
      (fc?.features || []).map(feat => ({
        ...feat,
        properties: { ...(feat.properties || {}), __cat: cat }
      }))
    );
    currentPOIsFC = {
      type: 'FeatureCollection',
      features: dedupePOIFeaturesForDisplay(rawFeatures)
    };
    window.activePOICats = new Set(catList);
  }

  const whatIfMap = syncWhatIfPOIs(catList);
  if (updateUI && currentPOIsFC?.features?.length) {
    currentPOIsFC.features = dedupePOIFeaturesForDisplay(currentPOIsFC.features);
  }
  Object.entries(whatIfMap).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items.map(item => ({ ...item, v: 1 })));
  });
  const buildingPOIs = collectBuildingPOIsByCat(catList, { includeWhatIf: true });
  Object.entries(buildingPOIs).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items.map(item => ({ ...item, v: 1 })));
  });

  for (const cat of catList) {
    catToPOI[cat] = dedupeIFCityPOIs(catToPOI[cat]);
  }

  // Per-building loop: for cities like Göteborg (~283k features) this runs long
  // enough that Firefox warns the user with "stop this page". Yield to the
  // event loop every BATCH features so the browser can render the spinner,
  // process clicks, and stay responsive.
  const benefits = [];
  const features = baseCityFC.features;
  const len = features.length;
  const BATCH = 5000;
  // Per-category access values, indexed by featIdx, used in a normalising
  // pass after the loop. Storing raw `access` as `fair_multi[cat].score`
  // previously produced values >1 (and >2 in car mode) because the IF-City
  // accessibility is unbounded.
  const accessByCat = Object.fromEntries(
    catList.map((cat) => [cat, new Array(len).fill(NaN)])
  );

  // Public-transport mode: load the baked transit network for the active city
  // once up front, so each building's travel time comes from the real stop
  // network (access walk + wait + in-vehicle + egress). Falls back silently to
  // the per-mode speed model if no network is baked for this city.
  const transitMode = normalizeTravelMode(fairnessTravelMode) === 'transit';
  let transitNetReadyFlag = false;
  if (transitMode && typeof ensureTransitNetwork === 'function') {
    try { await ensureTransitNetwork(); transitNetReadyFlag = transitReady(); }
    catch { transitNetReadyFlag = false; }
  }

  // EpiCity per-DESO demographics → demand (population) + need (income/age) weights.
  // Loaded and mapped to buildings once; drives demandWeight/socioWeight below.
  if (typeof ensureEpicityDemographics === 'function') {
    try { await ensureEpicityDemographics(); } catch (e) { /* fall back to neutral weights */ }
  }
  // EpiCity SYNTHETIC per-building population → preferred demand signal (genuine
  // per-building residents vs the DESO-distributed proxy). All 7 cities.
  if (typeof ensureSynthpop === 'function') {
    try { await ensureSynthpop(); } catch (e) { /* fall back to DESO/floor-area demand */ }
  }

  // Real network distances (walk/cycle/drive): load baked routing matrices for the
  // active mode. Categories with what-if edits keep the haversine model (their
  // added/removed POIs aren't in the static matrices).
  let netCats = null;
  const networkMode = !transitMode &&
    ['walking', 'cycling', 'driving'].includes(normalizeTravelMode(fairnessTravelMode));
  if (networkMode && typeof ensureRoutingMatrices === 'function') {
    try {
      await ensureRoutingMatrices(undefined, normalizeTravelMode(fairnessTravelMode), catList);
      const whatIfCats = new Set();
      for (const cat of catList) {
        if ((whatIfMap[cat] && whatIfMap[cat].length) ||
            (buildingPOIs[cat] && buildingPOIs[cat].length)) whatIfCats.add(cat);
      }
      netCats = new Set(catList.filter(c => routingHasCat(c) && !whatIfCats.has(c)));
      if (!netCats.size) netCats = null;
    } catch { netCats = null; }
  }

  for (let batchStart = 0; batchStart < len; batchStart += BATCH) {
    const batchEnd = Math.min(batchStart + BATCH, len);
    for (let featIdx = batchStart; featIdx < batchEnd; featIdx++) {
      const f = features[featIdx];
      const props = f.properties || (f.properties = {});
      let cB;
      try { cB = turf.centroid(f).geometry.coordinates; } catch { benefits.push(0); continue; }
      if (!cB) { benefits.push(0); continue; }

      // Per-building transit origin stops (computed once, reused across cats).
      const transitCtx = transitNetReadyFlag
        ? { ready: true, originStops: transitNearestStops(cB) }
        : null;
      // Per-building routing context: carry the POINT (not a single shared row) so
      // each category resolves its own matrix row — categories can be baked in
      // different building orders (see routingDistsForPoint).
      const netCtx = netCats ? { pt: cB, cats: netCats } : null;

      const fm = {};
      if (updateUI) delete props.fair;

      let utility = 0;
      for (const cat of catList) {
        const arr = catToPOI[cat] || [];
        if (!arr.length) continue;
        const distOut = {};
        const access = ifCityAccessibilityForBuilding(cB, arr, cat, fairnessTravelMode, transitCtx, netCtx, distOut);
        // Keep the raw access available, but score is filled in by the
        // city-wide normalisation pass below so it always lands in 0..1.
        // dist_m = raw distance to the nearest POI (network metres for walk/cycle/
        // drive, straight-line otherwise) → feeds the inspector distance line.
        fm[cat] = { access, score: 0, dist_m: Number.isFinite(distOut.distM) ? distOut.distM : null };
        accessByCat[cat][featIdx] = access;
        const weight = Number.isFinite(weightsByCat[cat]) ? weightsByCat[cat] : 1;
        utility += weight * access;
      }

      const benefitRaw   = utility - IF_CITY_BASELINE_UTILITY;
      const equityWeight = ifCityEquityWeightForFeature(f);

      // Demand: prefer EpiCity SYNTHETIC per-building population (genuine
      // residents, all 7 cities); else the per-DESO population distributed over
      // RESIDENTIAL buildings (residential via andamal1='Bostad'); else the
      // Växjö residential SCB map. Denser residential buildings weigh more.
      let demandWeight = 1.0;
      const popMap = (typeof synthBuildingPopMap !== 'undefined' && synthBuildingPopMap && synthBuildingPopMap.size)
        ? synthBuildingPopMap
        : ((typeof epiBuildingPopMap !== 'undefined' && epiBuildingPopMap && epiBuildingPopMap.size)
          ? epiBuildingPopMap
          : ((vaxjoBuildingPopMap && vaxjoBuildingPopMap.size) ? vaxjoBuildingPopMap : null));
      if (popMap) {
        const estPop = popMap.get(featIdx) || 0;
        demandWeight = estPop > 0 ? Math.max(0.5, Math.min(5.0, Math.log1p(estPop))) : 1.0;
      }

      // Need-weighted equity (revertible; off unless the UI toggle is on). Folds the
      // district's composite deprivation z-score into the IF-City equity weight, so
      // under-served high-need areas read as more unfair and skew Gini/inequality.
      let needWeight = 1.0;
      let needZ = null;
      if (needWeightEnabled && vaxjoBuildingNeedMap) {
        needZ = vaxjoBuildingNeedMap.get(featIdx);
        needWeight = (typeof needWeightFromZ === 'function')
          ? needWeightFromZ(needZ, needWeightStrength) : 1.0;
      }

      // EpiCity socioeconomic need (low income + high age-dependency, per DESO).
      // On by default (EPI_SOCIO_STRENGTH) so the demographics shape fairness/Gini:
      // deprived areas with poor access read as more unfair. Separate from the
      // manual SCB need toggle above.
      let socioWeight = 1.0;
      let socioZ = null;
      if (typeof epiBuildingNeedMap !== 'undefined' && epiBuildingNeedMap
          && typeof needWeightFromZ === 'function') {
        socioZ = epiBuildingNeedMap.get(featIdx);
        if (Number.isFinite(socioZ)) {
          const strength = (typeof EPI_SOCIO_STRENGTH !== 'undefined') ? EPI_SOCIO_STRENGTH : 0;
          socioWeight = needWeightFromZ(socioZ, strength);
        }
      }

      const benefit = Math.max(0, benefitRaw) * equityWeight * demandWeight * needWeight * socioWeight;
      benefits.push(benefit);

      // FIX: always expose per-category data. fm holds {access, score} per category;
      // the headline (setOverall) path used to skip this assignment, so the
      // DR/PCP/contrastive/district views read a missing fair_multi → dead axes.
      props.fair_multi = fm;
      props.__ifcity = { utility, benefit, equity_weight: equityWeight,
        demand_weight: demandWeight, need_weight: needWeight,
        need_z: Number.isFinite(needZ) ? needZ : null,
        socio_weight: socioWeight, socio_z: Number.isFinite(socioZ) ? socioZ : null };
    }
    if (batchEnd < len) {
      // setTimeout(0) is enough for Firefox to mark the page responsive.
      await new Promise(r => setTimeout(r, 0));
    }
  }

  const scores = normalizeBenefitsToScores(benefits, fairnessTravelMode, true);
  // Second pass: normalise each category's accessibility array to 0..1
  // using the same logic as the overall benefit. This is the fix for
  // fair_multi[cat].score sometimes exceeding 1 (or even 2 in car mode).
  const normByCat = {};
  for (const cat of catList) {
    const raw = accessByCat[cat] || [];
    // normalizeBenefitsToScores ignores non-finite / non-positive values
    // and clamps to 0..1, so it is a safe drop-in for raw IF-City access.
    normByCat[cat] = normalizeBenefitsToScores(raw, fairnessTravelMode);
  }
  // FIX: always normalize per-category access into .score (0..1). Previously gated
  // behind updateUI, so the headline setOverall compute left every per-category
  // score unset — making the multivariate views' 7 service axes dead constants.
  {
    for (let i = 0; i < len; i++) {
      const props = baseCityFC.features[i]?.properties;
      if (!props?.fair_multi) continue;
      for (const cat of catList) {
        if (!props.fair_multi[cat]) continue;
        const norm = normByCat[cat]?.[i];
        props.fair_multi[cat].score = Number.isFinite(norm) ? norm : 0;
      }
    }
  }
  let idx = 0;
  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const score = scores[idx++] ?? 0;
    if (updateUI) {
      props.fair = { cat: catList.length > 1 ? 'mix' : catList[0], score };
    }
    if (setOverall) {
      props.fair_overall = { score };
    }
  }

  // EXPERIMENTAL adequacy metric (removable — see lib/absoluteColorScale.js).
  // Hand the selected-mix benefits (not the all-categories overall pass) to the
  // adequacy module so it can report the population share above its provision bar.
  if (!setOverall && typeof window !== 'undefined'
      && typeof window.fairAdequacyOnComputed === 'function') {
    try { window.fairAdequacyOnComputed(benefits, fairnessTravelMode); } catch (e) { /* ignore */ }
  }

  // GE(2) on raw benefits — the IF-City inequality metric. Used for both the
  // metric strip display and the per-selection Gini badge. Exclude accessory
  // structures (garages/sheds): a house and its garage share a location, so
  // counting both double-weights residential areas. benefits[i] is aligned with
  // baseCityFC.features[i]. Per-building scores/colors above are untouched.
  const benefitsForInequality = (typeof isAccessoryBuilding === 'function')
    ? benefits.filter((_, i) => !isAccessoryBuilding(baseCityFC.features[i]?.properties))
    : benefits;
  const inequality = generalizedEntropy(benefitsForInequality, IF_CITY_ALPHA);
  const giniCoeff = inequality;
  // True bounded Gini (0..1) on the same benefits — shown beside GE(2) in the UI.
  // NB the model OPTIMISES GE(2) (more sensitive to deep deficits); Gini is a
  // familiar companion readout, not what the policy reasoning uses.
  const giniTrue = (typeof gini === 'function') ? gini(benefitsForInequality) : NaN;
  // Bump the recolor tick on every compute (overall *or* per-category) so
  // deck.gl's updateTriggers re-evaluate getFillColor.
  fairRecolorTick++;
  if (updateUI) {
    fairActive = true;
    districtScoresSuppressed = false;
    fairCategory = catList.length > 1 ? 'mix' : catList[0];
    updateLayers();
  } else {
    updateLayers();
  }

  if (updateUI) {
    const summary = summarizeFairnessCurrent();
    showSidePanel(fairCategory, inequality, currentPOIsFC.features.length, summary);
    window.getFairnessSummary = () => summary;
  }

  await refreshDistrictScores();
  await refreshMezoScores();
  if (updateUI) {
    updateLayers();
  }
  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  // Update per-selection Gini for the inspector strip, then refresh it.
  if (updateUI) {
    currentCategoryGini = Number.isFinite(giniCoeff) ? giniCoeff : null;
    currentCategoryGiniTrue = Number.isFinite(giniTrue) ? giniTrue : null;
  }
  window.faveInspector?.refresh?.();

  return { inequality, giniCoeff, giniTrue, poiCount: updateUI ? currentPOIsFC.features.length : poiCount };
}

/* ---------- Single category (kept, still usable internally) ---------- */
async function computeFairnessFast(cat) {
  const myGen = fairnessComputeGen;
  if (fairnessModel === 'ifcity') {
    const res = await computeIfCityFairness([cat], { [cat]: 1 });
    const displayGini = Number.isFinite(res.giniCoeff) ? res.giniCoeff : res.inequality;
    currentCategoryGini = displayGini;
    try {
      if (giniOut) giniOut.textContent = `${prettyPOIName(cat)} Gini: ${formatFairnessBadgeValue(res.inequality)}`;
      if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
    } catch {}
    window.faveInspector?.refresh?.();
    return { gini: displayGini, poiCount: res.poiCount };
  }
  if (!baseCityFC) throw new Error('No buildings loaded.');
  const pois = await fetchPOIs(cat, baseCityFC);
  const filteredPOIFeatures = filterFetchedPOIsForWhatIf(pois.features || []);
  currentPOIsFC = {
    type: 'FeatureCollection',
    features: filteredPOIFeatures.map(feat => ({
      ...feat,
      properties: { ...(feat.properties || {}), __cat: cat }
    }))
  };

  const poiCoords = filteredPOIFeatures.map(p => ({ c: p.geometry.coordinates, name: p.properties?.name || '(unnamed)' }));
  const whatIfMap = syncWhatIfPOIs([cat]);
  if (whatIfMap?.[cat]?.length) {
    poiCoords.push(...whatIfMap[cat]);
  }
  const buildingPOIs = collectBuildingPOIsByCat([cat], { includeWhatIf: true });
  if (buildingPOIs?.[cat]?.length) {
    poiCoords.push(...buildingPOIs[cat]);
  }
  if (!poiCoords.length) throw new Error('No POIs found for this area.');
  const scoresLack = [];

  const allowRouting = allowRoutingForFairness(poiCoords.length);
  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    delete props.fair;
    props.fair_multi = {};

    let bestIdx = -1, bestD = Infinity;
    for (let i = 0; i < poiCoords.length; i++) {
      const d = haversineMeters(cB, poiCoords[i].c);
      if (d < bestD) { bestD = d; bestIdx = i; }
    }
    if (bestIdx < 0) continue;

    const nearest = poiCoords[bestIdx];
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
  districtScoresSuppressed = false;
  fairCategory = cat;
  window.activePOICats = new Set([cat]);
  fairRecolorTick++;
  updateLayers();

  try {
    if (giniOut) giniOut.textContent = `${prettyPOIName(cat)} Gini: ${formatFairnessBadgeValue(G)}`;
    if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
  } catch {}

  const summary = summarizeFairnessCurrent();
  showSidePanel(cat, G, pois.features.length, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  return { gini: G, poiCount: pois.features.length };
}

/* ---------- Weighted mix across multiple categories ---------- */
async function computeFairnessWeighted(mix) {
  const myGen = fairnessComputeGen;
  if (fairnessModel === 'ifcity') {
    const weightsByCat = mix.reduce((acc, item) => {
      acc[item.cat] = Number.isFinite(item.weight) ? item.weight : 1;
      return acc;
    }, {});
    const res = await computeIfCityFairness(mix.map(m => m.cat), weightsByCat);
    const displayGini = Number.isFinite(res.giniCoeff) ? res.giniCoeff : res.inequality;
    currentCategoryGini = displayGini;
    try {
      if (giniOut) giniOut.textContent = `Mix Gini: ${formatFairnessBadgeValue(res.inequality)}`;
      if (fairStatus) { fairStatus.textContent = ''; fairStatus.classList.remove('text-danger'); }
    } catch {}
    return { gini: displayGini, poiCount: res.poiCount };
  }
  if (!baseCityFC) throw new Error('No buildings loaded.');

  const fetched = await Promise.all(
    mix.map(({cat}) =>
      fetchPOIs(cat, baseCityFC)
        .then(fc => ({ cat, fc }))
        .catch(() => ({ cat, fc: { type:'FeatureCollection', features:[] } }))
    )
  );

  const catToPOI = {};
  for (const {cat, fc} of fetched) {
    catToPOI[cat] = (fc?.features || []).map(p => ({ c: p.geometry.coordinates, name: p.properties?.name || '(unnamed)' }));
  }

  currentPOIsFC = {
    type: 'FeatureCollection',
    features: fetched.flatMap(({cat, fc}) =>
      (fc?.features || []).map(feat => ({
        ...feat,
        properties: { ...(feat.properties || {}), __cat: cat }
      }))
    )
  };

  window.activePOICats = new Set(mix.map(m => m.cat));
  const whatIfMap = syncWhatIfPOIs(mix.map(m => m.cat));
  Object.entries(whatIfMap).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items);
  });
  const buildingPOIs = collectBuildingPOIsByCat(mix.map(m => m.cat), { includeWhatIf: true });
  Object.entries(buildingPOIs).forEach(([cat, items]) => {
    if (!catToPOI[cat]) catToPOI[cat] = [];
    catToPOI[cat].push(...items);
  });

  //I changed HERE//

  // const weightSum = mix.reduce((s, {weight}) => s + Math.max(0, weight), 0);
  const scoresLack = [];

  const maxPois = Math.max(0, ...mix.map(({ cat }) => catToPOI[cat]?.length || 0));
  const allowRouting = allowRoutingForFairness(maxPois);
  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    const cB = turf.centroid(f).geometry.coordinates;

    const fm = {};
    delete props.fair;

    for (const {cat} of mix) {
      const arr = catToPOI[cat] || [];
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
  districtScoresSuppressed = false;
  fairCategory = 'mix';
  fairRecolorTick++;
  updateLayers();

  const summary = summarizeFairnessCurrent();
  showSidePanel('mix', G, currentPOIsFC.features.length, summary);
  window.getFairnessSummary = () => summary;

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  return { gini: G, poiCount: currentPOIsFC.features.length };
}

/* ---------- Overall fairness across ALL categories (auto, parallel) ---------- */
async function computeOverallFairness(catList) {
  if (fairnessModel === 'ifcity') {
    const weightsByCat = catList.reduce((acc, cat) => {
      acc[cat] = 1;
      return acc;
    }, {});
    const res = await computeIfCityFairness(catList, weightsByCat, { setOverall: true });
    overallGini = Number.isFinite(res.giniCoeff) ? res.giniCoeff : res.inequality;
    overallGiniTrue = Number.isFinite(res.giniTrue) ? res.giniTrue : null;
    districtScoresSuppressed = false;
    if (overallGiniOut) overallGiniOut.textContent = formatFairnessBadgeValue(overallGini);
    if (parallelCoordsOpen) {
      updateParallelCoordsPanel();
    }
    return { overall_gini: overallGini };
  }
  if (!baseCityFC) throw new Error('No buildings loaded.');
  const allowRouting = ROUTING_ENABLE_OVERALL && allowRoutingForFairness(catList?.length || 0);

  const jobs = catList.map(cat => (async () => {
    try {
      const fc = await fetchPOIsWithRetry(cat, baseCityFC);
      const filtered = filterFetchedPOIsForWhatIf(fc.features);
      const arr = filtered.map(p => ({ c: p.geometry.coordinates, name: p.properties?.name || '(unnamed)' }));
      return { cat, ok: true, arr };
    } catch (e) {
      console.warn('POI fetch failed for', cat, e);
      return { cat, ok: false, arr: [] };
    }
  })());
  const results = await Promise.all(jobs);

  const catToArr = {};
  catList.forEach((cat) => {
    catToArr[cat] = [];
  });

  results.forEach((r) => {
    if (!Array.isArray(catToArr[r.cat])) catToArr[r.cat] = [];
    if (r.ok && r.arr.length) {
      catToArr[r.cat].push(...r.arr);
    }
  });

  const buildingPOIs = collectBuildingPOIsByCat(catList, { includeWhatIf: true });
  Object.entries(buildingPOIs || {}).forEach(([cat, arr]) => {
    if (!Array.isArray(catToArr[cat])) catToArr[cat] = [];
    if (Array.isArray(arr) && arr.length) {
      catToArr[cat].push(...arr);
    }
  });

  const validCats = catList.filter((cat) => (catToArr[cat] || []).length > 0);

  if (!validCats.length) {
    overallGini = null;
    if (overallGiniOut) overallGiniOut.textContent = '—';
    if (parallelCoordsOpen) {
      updateParallelCoordsPanel();
    }
    return { overall_gini: null };
  }

  for (const f of baseCityFC.features) {
    const props = f.properties || (f.properties = {});
    if (!props.fair_multi) props.fair_multi = {};
    const cB = turf.centroid(f).geometry.coordinates;

    for (const cat of validCats) {
      const arr = catToArr[cat];
      let bestD = Infinity;
      let bestIdx = -1;
      for (let i=0;i<arr.length;i++) {
        const d = haversineMeters(cB, arr[i].c);
        if (d < bestD) { bestD = d; bestIdx = i; }
      }
      const nearestCoord = bestIdx >= 0 ? arr[bestIdx].c : null;
      const metrics = nearestCoord
        ? await getTravelMetricsForPair(cB, nearestCoord, bestD, allowRouting, fairnessTravelMode)
        : { meters: bestD, seconds: estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode) };
      const score = scoreFromTimeSeconds(cat, metrics.seconds, fairnessTravelMode);
      props.fair_multi[cat] = {
        score,
        dist_m: metrics.meters,
        time_min: Number.isFinite(metrics.seconds) ? metrics.seconds / 60 : null
      };
    }

    const scores = Object.values(props.fair_multi).map(o => o.score).filter(Number.isFinite);
    if (scores.length) {
      const avg = scores.reduce((a,b)=>a+b,0)/scores.length;
      props.fair_overall = { score: avg };
    } else {
      delete props.fair_overall;
    }
  }

  const lack = baseCityFC.features
    .map(f => f.properties?.fair_overall?.score)
    .filter(Number.isFinite)
    .map(s => 1 - s);
  const overall = gini(lack);
  overallGini = overall;
  districtScoresSuppressed = false;
  if (overallGiniOut) overallGiniOut.textContent = formatFairnessBadgeValue(overall);

  await refreshDistrictScores();
  await refreshMezoScores();
  updateLayers();

  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
  return { overall_gini: overall };
}



async function autoComputeOverall() {
  try {
    showGlobalSpinner('Computing overall fairness…');
    overallGiniOut && (overallGiniOut.textContent = '…');
    const res = await computeOverallFairness(ALL_CATEGORIES);
    hideGlobalSpinner();
    return res;
  } catch (e) {
    hideGlobalSpinner();
    console.error('Overall fairness error', e);
    overallGiniOut && (overallGiniOut.textContent = '—');
    return { overall_gini: null };
  }
}

