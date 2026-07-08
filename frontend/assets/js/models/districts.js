// District boundary handling: load DESO/regso polygons, clip to coastline,
// build mezo hex grid, label placement, etc. Extracted from main.js;
// loaded as classical script before main.js.

/* ======================= District helpers ======================= */
function districtNameOf(props = {}, idx = 0) {
  const codeCandidates = [
    'regso','REGSO','regso_code','REGSO_CODE','REGSO_KOD',
    'deso','DESO','deso_code','DESO_CODE','DESO_KOD'
  ];
  const candidates = [
    'regsonamn','REGSONAMN','regso_namn','regso_namn2','REGSO_NAMN','REGSO_NAMN2',
    'name','Name','NAMN','namn',
    'deso_namn','DESO_NAMN','deso_name','DESO_NAME','REGSO','REGSO_CODE','DESO','DESO_CODE','DESO_KOD','Deso','deso'
  ];
  for (const k of candidates) {
    const v = props[k];
    if (v && String(v).trim()) return String(v).trim();
  }
  for (const k of codeCandidates) {
    const v = props[k];
    if (v && String(v).trim()) return `District ${String(v).trim()}`;
  }
  const keys = Object.keys(props || {});
  for (const k of keys) {
    const v = props[k];
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return `District ${idx + 1}`;
}

function hashStringToIndex(str, modulo) {
  let hash = 0;
  for (let i = 0; i < str.length; i += 1) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }
  return modulo ? (hash % modulo) : hash;
}

const DISTRICT_COLOR_VERSION = 1;
const DISTRICT_COLOR_ALPHA = 200;
const DISTRICT_COLOR_SATURATION = 0.68;
const DISTRICT_COLOR_LIGHTNESS = 0.5;

const DISTRICT_LABEL_VERSION = 2;
const DISTRICT_LABEL_BASE_ZOOM = 8.0;
const DISTRICT_LABEL_FULL_ZOOM = 9.0;

function hslToRgb(h, s, l) {
  const hue = ((h % 360) + 360) % 360;
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs(((hue / 60) % 2) - 1));
  const m = l - c / 2;
  let r = 0;
  let g = 0;
  let b = 0;
  if (hue < 60) {
    r = c; g = x; b = 0;
  } else if (hue < 120) {
    r = x; g = c; b = 0;
  } else if (hue < 180) {
    r = 0; g = c; b = x;
  } else if (hue < 240) {
    r = 0; g = x; b = c;
  } else if (hue < 300) {
    r = x; g = 0; b = c;
  } else {
    r = c; g = 0; b = x;
  }
  return [
    Math.round((r + m) * 255),
    Math.round((g + m) * 255),
    Math.round((b + m) * 255)
  ];
}

function ensureDistrictColors() {
  if (!districtFC?.features?.length) return;
  const needsUpdate = districtFC.features.some((feat) => {
    const props = feat?.properties || {};
    return props.__colorVersion !== DISTRICT_COLOR_VERSION;
  });
  if (!needsUpdate) return;

  const entries = districtFC.features.map((feat, idx) => ({
    idx,
    name: districtNameOf(feat?.properties || {}, idx)
  }));
  entries.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' }));

  const total = entries.length || 1;
  entries.forEach((entry, rank) => {
    const hue = (rank / total) * 360;
    const sat = (rank % 2 === 0) ? DISTRICT_COLOR_SATURATION : Math.min(0.92, DISTRICT_COLOR_SATURATION + 0.18);
    const light = (rank % 3 === 0) ? DISTRICT_COLOR_LIGHTNESS : Math.min(0.72, DISTRICT_COLOR_LIGHTNESS + 0.14);
    const [r, g, b] = hslToRgb(hue, sat, light);
    const feat = districtFC.features[entry.idx];
    feat.properties = {
      ...(feat.properties || {}),
      __fillColor: [r, g, b, DISTRICT_COLOR_ALPHA],
      __colorVersion: DISTRICT_COLOR_VERSION
    };
  });
}

function districtFillColor(props = {}, idx = 0) {
  if (Array.isArray(props.__fillColor)) return props.__fillColor;
  return [80, 80, 80, 60];
}

function ensureDistrictLabelThresholds() {
  if (!districtFC?.features?.length) return;
  const needsUpdate = districtFC.features.some((feat) => {
    const props = feat?.properties || {};
    return props.__labelVersion !== DISTRICT_LABEL_VERSION;
  });
  if (!needsUpdate) return;

  const maxZoom = (map && typeof map.getMaxZoom === 'function') ? map.getMaxZoom() : DISTRICT_LABEL_FULL_ZOOM;
  const minZoom = (map && typeof map.getMinZoom === 'function') ? map.getMinZoom() : 0;
  const baseZoom = Math.max(minZoom, DISTRICT_LABEL_BASE_ZOOM);
  const fullZoom = Math.min(maxZoom, DISTRICT_LABEL_FULL_ZOOM);
  const zoomSpan = Math.max(0.1, fullZoom - baseZoom);

  const entries = districtFC.features.map((feat, idx) => {
    let areaSqKm = Number(feat?.properties?.__labelAreaSqKm);
    if (!Number.isFinite(areaSqKm)) {
      try {
        areaSqKm = turf.area(feat) / 1e6;
      } catch (_) {
        areaSqKm = null;
      }
    }
    return { idx, areaSqKm };
  }).filter(entry => Number.isFinite(entry.areaSqKm));

  const sorted = entries.slice().sort((a, b) => b.areaSqKm - a.areaSqKm);
  const total = sorted.length || 1;

  sorted.forEach((entry, rank) => {
    const percentile = (rank + 1) / total;
    const minZoomForLabel = baseZoom + (percentile * zoomSpan);
    const feat = districtFC.features[entry.idx];
    feat.properties = {
      ...(feat.properties || {}),
      __labelAreaSqKm: entry.areaSqKm,
      __labelMinZoom: minZoomForLabel,
      __labelVersion: DISTRICT_LABEL_VERSION
    };
  });
}

function districtLabelData() {
  if (!districtFC?.features?.length) return [];
  ensureDistrictLabelThresholds();
  const zoom = (map && typeof map.getZoom === 'function') ? map.getZoom() : 0;
  return districtFC.features.map((f, idx) => {
    const pos = (() => {
      const calculators = [
        () => turf.pointOnFeature(f),
        () => turf.centerOfMass(f),
        () => turf.centroid(f)
      ];
      for (const calc of calculators) {
        try {
          const pt = calc();
          const coords = pt?.geometry?.coordinates;
          if (Array.isArray(coords) && coords.length >= 2) return coords;
        } catch (_) { /* ignore and try next */ }
      }
      return null;
    })();
    const props = f.properties || {};
    const name = props.__districtName || districtNameOf(props, idx);
    const minZoom = Number.isFinite(props.__labelMinZoom) ? props.__labelMinZoom : 0;
    if (zoom < minZoom) return null;
    return (pos && name) ? { position: pos, name, properties: props } : null;
  }).filter(Boolean);
}

function isFiniteLngLatPair(value) {
  return Array.isArray(value)
    && value.length >= 2
    && Number.isFinite(value[0])
    && Number.isFinite(value[1]);
}

function detectDistrictCoordSystem(fc) {
  if (!fc?.features?.length) return 'wgs84';

  const sample = [];
  const collect = (coords) => {
    if (!coords || sample.length >= 300) return;
    if (isFiniteLngLatPair(coords)) {
      sample.push(coords);
      return;
    }
    if (Array.isArray(coords)) {
      coords.forEach(collect);
    }
  };

  for (const feat of fc.features) {
    collect(feat?.geometry?.coordinates);
    if (sample.length >= 300) break;
  }
  if (!sample.length) return 'wgs84';

  const latInRange = sample.filter(([, lat]) => Math.abs(lat) <= 90).length;
  const lngInRange = sample.filter(([lng]) => Math.abs(lng) <= 180).length;
  const lonLatShare = Math.min(latInRange, lngInRange) / sample.length;
  if (lonLatShare > 0.95) return 'wgs84';

  return 'epsg3006';
}

function convertDistrictCoords(coords, srcCRS, dstCRS) {
  if (!coords) return coords;
  if (isFiniteLngLatPair(coords)) {
    const [x, y] = coords;
    const converted = proj4(srcCRS, dstCRS, [x, y]);
    return [converted[0], converted[1], ...coords.slice(2)];
  }
  if (!Array.isArray(coords)) return coords;
  return coords.map(part => convertDistrictCoords(part, srcCRS, dstCRS));
}

function normalizeDistrictGeometryCRS(fc) {
  if (!fc?.features?.length || typeof proj4 !== 'function') return fc;
  const crsName = String(fc?.crs?.properties?.name || '').toLowerCase();
  const likelyEPSG3006 = crsName.includes('3006') || detectDistrictCoordSystem(fc) === 'epsg3006';
  if (!likelyEPSG3006) return fc;

  const SRC = 'EPSG:3006';
  const DST = 'EPSG:4326';
  if (!proj4.defs(SRC)) {
    proj4.defs(SRC, '+proj=utm +zone=33 +ellps=GRS80 +units=m +no_defs');
  }

  (fc.features || []).forEach((feat) => {
    if (!feat?.geometry?.coordinates) return;
    feat.geometry.coordinates = convertDistrictCoords(feat.geometry.coordinates, SRC, DST);
  });
  return fc;
}

async function ensureDistrictData() {
  if (!activeDistrictURL) return null;
  if (districtFC) return districtFC;
  if (districtLoadPromise) return districtLoadPromise;
  districtLoadPromise = fetch(activeDistrictURL)
    .then(r => {
      if (!r.ok) throw new Error(`District fetch failed (${r.status})`);
      return r.json();
    })
    .then(fc => {
      normalizeDistrictGeometryCRS(fc);
      // Author-curated exclusion list, matching the main project's
      // decision for Växjö: drop the peri-urban rectangular frame plus
      // the satellite villages so the macro/meso views focus on the
      // urban core and its immediate surroundings.
      const EXCLUDED_DISTRICTS = [
        'stadsnära landsbygd',
        'växjö landsbygd',
        'gemla',
        'ingelstad',
        'braås',
        'rottne',
        'lammhult'
      ];
      if (fc?.features) {
        fc.features = fc.features.filter((feat, idx) => {
          const name = normalizeDistrictName(
            districtNameOf(feat?.properties || {}, idx)
          );
          return !EXCLUDED_DISTRICTS.includes(name);
        });
      }
      districtFC = fc;
      initVaxjoDemandWeights();
      mezoMaskPolygon = null;
      districtLoadError = null;
      // Join baked SCB demographics onto district features (offline). Optional —
      // failures are swallowed so districts still work without demographics.
      if (typeof ensureDemographicsData === 'function') {
        return ensureDemographicsData(activeDistrictCityKey)
          .then(() => { attachDemographicsToDistricts(fc); return fc; })
          .catch(() => fc);
      }
      return fc;
    })
    .catch(err => { districtLoadError = err; districtLoadPromise = null; throw err; });
  return districtLoadPromise;
}

function ensureDistrictBoundaryLines() {
  if (!districtFC?.features?.length) return null;
  if (districtBoundaryFC) return districtBoundaryFC;

  const lines = [];
  for (const feat of districtFC.features) {
    try {
      const line = turf.polygonToLine(feat);
      if (line?.type === 'Feature') {
        lines.push(line);
      } else if (line?.type === 'FeatureCollection' && Array.isArray(line.features)) {
        lines.push(...line.features);
      }
    } catch (_) { /* ignore bad geometry */ }
  }
  districtBoundaryFC = turf.featureCollection(lines);
  return districtBoundaryFC;
}

/* ===== Coastline clipping: trim district/mezo polygons to land ===== */

/**
 * Build a land-mass polygon from building centroids.
 * Uses turf.concave (alpha shape) to trace the actual building footprint,
 * then buffers generously so no land is lost.
 */
function buildLandHullFromBuildings() {
  if (!baseCityFC?.features?.length) return null;

  const points = [];
  for (const feat of baseCityFC.features) {
    if (!feat?.geometry) continue;
    try {
      const c = turf.centroid(feat)?.geometry?.coordinates;
      if (Array.isArray(c) && c.length >= 2 && Number.isFinite(c[0]) && Number.isFinite(c[1])) {
        points.push(turf.point(c));
      }
    } catch (_) { /* skip invalid geometry */ }
  }
  if (points.length < 4) return null;

  const pointsFC = turf.featureCollection(points);
  let hull = null;

  // Concave hull with a generous maxEdge — a tighter fit creates straight
  // edges where buildings get sparse near the urban/rural boundary,
  // which shows up as knife-cut artefacts on the district overlay.
  try {
    hull = turf.concave(pointsFC, { maxEdge: 3.0, units: 'kilometers' });
  } catch (_) { /* concave can fail on degenerate layouts */ }

  // Fallback to convex hull
  if (!hull) {
    try {
      hull = turf.convex(pointsFC);
    } catch (_) { /* extremely unlikely */ }
  }
  if (!hull) return null;

  // Generous 1 km buffer so the hull comfortably encloses every district
  // boundary rather than slicing across it. Inland water (e.g. Växjösjön)
  // will still be clipped because no building centroids sit on it.
  try {
    const buffered = turf.buffer(hull, 1.0, { units: 'kilometers' });
    if (buffered) return buffered;
  } catch (_) { /* buffer can fail on complex concave shapes */ }

  return hull;
}

/**
 * Clip a single Polygon or MultiPolygon feature to a land polygon
 * using turf.intersect. Preserves original properties.
 * Returns the clipped feature, or the original if clipping fails.
 */
function clipFeatureToLandHull(feature, landPoly) {
  if (!feature?.geometry || !landPoly?.geometry) return feature;
  const geomType = feature.geometry.type;

  try {
    if (geomType === 'Polygon') {
      const clipped = turf.intersect(feature, landPoly);
      if (clipped) {
        clipped.properties = { ...(feature.properties || {}) };
        return clipped;
      }
      // Entirely outside hull — check if it has buildings before dropping
      return feature; // keep as safe fallback
    }

    if (geomType === 'MultiPolygon') {
      const clippedParts = [];
      for (const coords of feature.geometry.coordinates) {
        try {
          const subPoly = turf.polygon(coords);
          const clipped = turf.intersect(subPoly, landPoly);
          if (clipped) {
            if (clipped.geometry.type === 'Polygon') {
              clippedParts.push(clipped.geometry.coordinates);
            } else if (clipped.geometry.type === 'MultiPolygon') {
              clippedParts.push(...clipped.geometry.coordinates);
            }
          }
        } catch (_) {
          clippedParts.push(coords); // keep original sub-polygon on failure
        }
      }
      if (!clippedParts.length) return feature; // safe fallback
      const result = clippedParts.length === 1
        ? turf.polygon(clippedParts[0], { ...(feature.properties || {}) })
        : turf.multiPolygon(clippedParts, { ...(feature.properties || {}) });
      return result;
    }
  } catch (err) {
    console.warn('clipFeatureToLandHull failed, keeping original:', err);
  }
  return feature;
}

/**
 * Previously this routine clipped every district polygon against a
 * building-centroid concave hull so lakes/sea inside districts wouldn't
 * be coloured. The clip's straight edges (where building density ran
 * out) showed up as knife-cut artefacts on the macro overlay, so it is
 * now a no-op — district polygons render with their original REGSO
 * shape. Inland water inside districts will be coloured along with the
 * land; this is a cosmetically smaller issue than the knife cuts and
 * keeps the user's curated EXCLUDED_DISTRICTS list as the sole source
 * of truth for which areas are visible.
 *
 * The hull helpers (buildLandHullFromBuildings, clipFeatureToLandHull)
 * are kept above for future use if a real coastline mask becomes
 * available — re-enable by reinstating the loop here.
 */
function ensureDistrictLandClipping() {
  // Intentional no-op. Macro/meso shapes follow the curated district
  // dataset directly.
  return;
}

function buildingScoreForDistrict(f) {
  if (!f?.properties) return null;
  if (fairActive && Number.isFinite(f.properties?.fair?.score)) return f.properties.fair.score;
  if (Number.isFinite(f.properties?.fair_overall?.score)) return f.properties.fair_overall.score;
  return null;
}

async function refreshDistrictScores() {
  if (!baseCityFC?.features?.length) { districtScoreTick++; return; }
  try { await ensureDistrictData(); } catch (e) { console.warn('District data missing', e); districtScoreTick++; return; }
  ensureDistrictLandClipping();
  if (!districtFC?.features?.length) { districtScoreTick++; return; }
  if (districtScoresSuppressed) { districtScoreTick++; return; }

  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const focusedPOICat = poiCategoryOf(selectedPOIFeature);
  const focusedPOICoords = (() => {
    const geom = selectedPOIFeature?.geometry;
    if (!geom) return null;
    if (geom.type === 'Point' && Array.isArray(geom.coordinates)) return geom.coordinates;
    try {
      const coords = turf.centroid(selectedPOIFeature).geometry.coordinates;
      return Array.isArray(coords) ? coords : null;
    } catch (_) {
      return null;
    }
  })();
  const pts = [];
  for (const f of baseCityFC.features) {
    const s = buildingScoreForDistrict(f);
    const props = f?.properties || {};
    const overallScore = Number.isFinite(props?.fair_overall?.score) ? props.fair_overall.score : null;
    const catScores = {};
    if (props?.fair_multi) {
      for (const cat of catList) {
        const val = props.fair_multi?.[cat]?.score;
        if (Number.isFinite(val)) catScores[cat] = val;
      }
    }
    const hasAnyScore = Number.isFinite(s) || Number.isFinite(overallScore) || Object.keys(catScores).length > 0;
    if (!hasAnyScore) continue;
    const c = fastCentroid(f);
    if (!c) continue;
    let focusedScore = null;
    if (focusedPOICoords && focusedPOICat && focusedPOICat !== 'default') {
      const dMeters = haversineMeters(c, focusedPOICoords);
      const tSec = estimateTravelTimeSecondsFromMeters(dMeters, fairnessTravelMode);
      focusedScore = scoreFromTimeSeconds(focusedPOICat, tSec, fairnessTravelMode);
    }
    // Real SCB child share of the building's DESO — aggregated to the district
    // below so the macro DR / map can colour districts by "who lives there".
    const childRaw = (typeof epiDesoProps === 'function' && props?.__deso != null)
      ? Number(epiDesoProps(props.__deso)?.child_frac) : NaN;
    pts.push(turf.point(c, { score: s, overall: overallScore, cats: catScores, focused: focusedScore,
      childReal: Number.isFinite(childRaw) ? childRaw : null }));
  }
  const ptsFC = turf.featureCollection(pts);

  (districtFC.features || []).forEach((feat, idx) => {
    const name = districtNameOf(feat.properties, idx) || `District ${idx + 1}`;
    const within = turf.pointsWithinPolygon(ptsFC, feat);
    const scores = within.features.map(p => p.properties?.score).filter(Number.isFinite);
    const overallScores = within.features.map(p => p.properties?.overall).filter(Number.isFinite);
    const focusedScores = within.features.map(p => p.properties?.focused).filter(Number.isFinite);
    const sums = {};
    const counts = {};
    for (const cat of catList) {
      sums[cat] = 0;
      counts[cat] = 0;
    }
    within.features.forEach((p) => {
      const cats = p.properties?.cats || {};
      for (const cat of catList) {
        const val = cats?.[cat];
        if (!Number.isFinite(val)) continue;
        sums[cat] += val;
        counts[cat] += 1;
      }
    });
    const fairByCat = {};
    for (const cat of catList) {
      fairByCat[cat] = counts[cat] ? sums[cat] / counts[cat] : 0;
    }
    const mean = scores.length ? scores.reduce((a,b)=>a+b,0)/scores.length : null;
    const childVals = within.features.map(p => p.properties?.childReal).filter(Number.isFinite);
    const childMean = childVals.length ? childVals.reduce((a, b) => a + b, 0) / childVals.length : null;
    const overallMean = overallScores.length ? overallScores.reduce((a, b) => a + b, 0) / overallScores.length : null;
    const focusedMean = focusedScores.length ? focusedScores.reduce((a, b) => a + b, 0) / focusedScores.length : null;
    feat.properties = {
      ...(feat.properties || {}),
      __districtName: name,
      __score: mean,
      __childReal: childMean,
      __count: scores.length,
      __fairOverall: overallMean,
      __fairByCat: fairByCat,
      __fairFocused: focusedMean,
      __fairFocusedCat: focusedMean != null ? focusedPOICat : null
    };
  });

  districtScoreTick++;
  markAggregateSelectionsFromBuildings((baseCityFC?.features || []).filter(f => f?.properties?._drSelected));
  if (parallelCoordsOpen && currentParallelCoordsMode() === 'district') {
    updateParallelCoordsPanel();
  }
}

function resolveMezoResolution() {
  if (mezoResolution != null) return mezoResolution;
  const h3 = window.h3;
  if (!h3) return null;
  const getEdgeKm = (res) => {
    if (typeof h3.getHexagonEdgeLengthAvg === 'function') return h3.getHexagonEdgeLengthAvg(res, 'km');
    if (typeof h3.getHexagonEdgeLengthAvgKm === 'function') return h3.getHexagonEdgeLengthAvgKm(res);
    if (typeof h3.edgeLength === 'function') return h3.edgeLength(res, 'km');
    return null;
  };
  let bestRes = null;
  let bestDiff = Infinity;
  for (let res = 0; res <= 15; res += 1) {
    const km = getEdgeKm(res);
    if (!Number.isFinite(km)) continue;
    const diff = Math.abs(km - selectedMezoHexEdgeKm);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestRes = res;
    }
  }
  mezoResolution = bestRes;
  return mezoResolution;
}

function h3LatLngToCell(h3, lat, lng, res) {
  if (!h3) return null;
  if (typeof h3.latLngToCell === 'function') return h3.latLngToCell(lat, lng, res);
  if (typeof h3.geoToH3 === 'function') return h3.geoToH3(lat, lng, res);
  return null;
}

function h3PolygonToCells(h3, polygon, res) {
  if (!h3 || !polygon) return [];
  if (typeof h3.polygonToCells === 'function') {
    return h3.polygonToCells(polygon, res);
  }
  if (typeof h3.polyfill === 'function') {
    return h3.polyfill(polygon.coordinates, res, true);
  }
  return [];
}

function h3CellToLatLng(h3, cell) {
  if (!h3 || !cell) return null;
  if (typeof h3.cellToLatLng === 'function') return h3.cellToLatLng(cell);
  if (typeof h3.h3ToGeo === 'function') return h3.h3ToGeo(cell);
  return null;
}

function mezoFallbackScoreForCell(cell, h3, poiByCat) {
  if (!fairActive || !fairCategory || !poiByCat) return null;
  const latLng = h3CellToLatLng(h3, cell);
  if (!Array.isArray(latLng) || latLng.length < 2) return null;
  const coord = [latLng[1], latLng[0]];
  const cats = fairCategory === 'mix'
    ? selectedPOIMix.map(item => item.cat).filter(Boolean)
    : [fairCategory];
  if (!cats.length) return null;
  let best = null;
  for (const cat of cats) {
    const arr = poiByCat[cat] || [];
    if (!arr.length) continue;
    let bestDist = Infinity;
    for (let i = 0; i < arr.length; i += 1) {
      const dMeters = haversineMeters(coord, arr[i].c);
      if (dMeters < bestDist) bestDist = dMeters;
    }
    if (!Number.isFinite(bestDist)) continue;
    const tSec = estimateTravelTimeSecondsFromMeters(bestDist, fairnessTravelMode);
    const score = scoreFromTimeSeconds(cat, tSec, fairnessTravelMode);
    if (!Number.isFinite(score)) continue;
    if (best == null || score > best) best = score;
  }
  return best;
}


function ensureMezoMaskPolygon() {
  if (mezoMaskPolygon || !districtFC?.features?.length) return mezoMaskPolygon;
  const polys = districtFC.features
    .filter(f => f?.geometry && (f.geometry.type === 'Polygon' || f.geometry.type === 'MultiPolygon'));
  if (!polys.length) return mezoMaskPolygon;
  try {
    let merged = polys[0];
    for (let i = 1; i < polys.length; i += 1) {
      merged = turf.union(merged, polys[i]) || merged;
    }
    mezoMaskPolygon = merged;
  } catch (err) {
    console.warn('Mezo mask union failed', err);
    mezoMaskPolygon = null;
  }
  return mezoMaskPolygon;
}

async function alignBuildingCoverageToDistricts(fc, contextLabel = 'dataset') {
  const features = Array.isArray(fc?.features) ? fc.features : null;
  if (!features?.length) return;
  if (!activeDistrictCityKey) {
    console.warn(`Skipping district alignment for ${contextLabel}: no district dataset mapped to the selected city.`);
    return;
  }
  try {
    await ensureDistrictData();
  } catch (err) {
    console.warn(`Skipping district alignment for ${contextLabel}: district data unavailable.`, err);
    return;
  }

  const districts = (districtFC?.features || []).filter((feat) => {
    const type = feat?.geometry?.type;
    return type === 'Polygon' || type === 'MultiPolygon';
  });
  if (!districts.length) return;

  // Use fastCentroid (bbox midpoint, cached) — turf.centroid was the dominant
  // cost in city load for >10k-building cities.
  const centroidPoints = [];
  for (let idx = 0; idx < features.length; idx++) {
    const feat = features[idx];
    if (!feat?.geometry) continue;
    const c = fastCentroid(feat);
    if (!c) continue;
    centroidPoints.push(turf.point(c, { __idx: idx }));
  }
  if (!centroidPoints.length) return;

  let within = null;
  try {
    within = turf.pointsWithinPolygon(
      turf.featureCollection(centroidPoints),
      turf.featureCollection(districts)
    );
  } catch (err) {
    console.warn(`Skipping district alignment for ${contextLabel}: polygon test failed.`, err);
    return;
  }

  const keep = new Set((within?.features || []).map((pt) => pt?.properties?.__idx).filter(Number.isInteger));
  const filtered = features.filter((_, idx) => keep.has(idx));
  const before = features.length;
  const kept = filtered.length;

  if (!kept) {
    console.warn(`Skipping district alignment for ${contextLabel}: zero buildings matched district polygons (likely CRS/geometry mismatch).`);
    return;
  }

  // Minimum-count sanity check (replaces the old 5% ratio guard). The
  // ratio guard was rejecting Växjö's Lantmäteriet export — 50k buildings
  // spread across forests, farms, and outlying villages — once the
  // EXCLUDED_DISTRICTS list dropped the rural districts: only ~10% of the
  // raw buildings sit inside the kept urban polygons, but that is
  // *exactly* what we want kept. The remaining catch is to make sure we
  // didn't accidentally throw away everything because of a CRS mismatch.
  if (kept < 50) {
    console.warn(`Skipping district alignment for ${contextLabel}: only ${kept}/${before} matched districts (likely CRS mismatch — too few survivors).`);
    return;
  }

  const removed = before - kept;
  if (removed > 0) {
    console.info(`Aligned ${contextLabel} to district boundary (${kept}/${before} kept, ${removed} removed).`);
  }
  fc.features = filtered;
}

async function refreshMezoScores() {
  if (!baseCityFC?.features?.length) { mezoScoreTick++; return; }
  const h3 = window.h3;
  const res = resolveMezoResolution();
  if (!h3 || res == null) { mezoHexData = []; mezoScoreTick++; return; }

  try {
    await ensureDistrictData();
    ensureDistrictLandClipping();
  } catch (_) {
    /* district mask optional */
  }

  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const poiByCat = fairActive ? (buildPOIMapFromCurrent() || {}) : null;
  const focusedPOICat = poiCategoryOf(selectedPOIFeature);
  const focusedPOICoords = (() => {
    const geom = selectedPOIFeature?.geometry;
    if (!geom) return null;
    if (geom.type === 'Point' && Array.isArray(geom.coordinates)) return geom.coordinates;
    try {
      const coords = turf.centroid(selectedPOIFeature).geometry.coordinates;
      return Array.isArray(coords) ? coords : null;
    } catch (_) {
      return null;
    }
  })();

  const mask = ensureMezoMaskPolygon();
  let coverageCells = null;
  if (mask?.geometry) {
    const cellSet = new Set();
    const pushCells = (geom) => {
      const polygon = { type: 'Polygon', coordinates: geom.coordinates };
      h3PolygonToCells(h3, polygon, res).forEach(cell => cellSet.add(cell));
    };
    if (mask.geometry.type === 'Polygon') {
      pushCells(mask.geometry);
    } else if (mask.geometry.type === 'MultiPolygon') {
      mask.geometry.coordinates.forEach((coords) => {
        pushCells({ coordinates: coords });
      });
    }
    coverageCells = Array.from(cellSet);
  }

  const hexMap = new Map();
  for (const f of baseCityFC.features) {
    const props = f?.properties || {};
    const score = buildingScoreForDistrict(f);
    const overallScore = Number.isFinite(props?.fair_overall?.score) ? props.fair_overall.score : null;
    const catScores = {};
    if (props?.fair_multi) {
      for (const cat of catList) {
        const val = props.fair_multi?.[cat]?.score;
        if (Number.isFinite(val)) catScores[cat] = val;
      }
    }
    const c = turf.centroid(f).geometry.coordinates;
    let cell = null;
    try {
      cell = h3LatLngToCell(h3, c[1], c[0], res);
    } catch (_) {
      cell = null;
    }
    if (!cell) continue;

    let focusedScore = null;
    if (focusedPOICoords && focusedPOICat && focusedPOICat !== 'default') {
      const dMeters = haversineMeters(c, focusedPOICoords);
      const tSec = estimateTravelTimeSecondsFromMeters(dMeters, fairnessTravelMode);
      focusedScore = scoreFromTimeSeconds(focusedPOICat, tSec, fairnessTravelMode);
    }

    if (!hexMap.has(cell)) {
      hexMap.set(cell, {
        hex: cell,
        total: 0,
        sum: 0,
        count: 0,
        overallSum: 0,
        overallCount: 0,
        catSums: Object.fromEntries(catList.map(cat => [cat, 0])),
        catCounts: Object.fromEntries(catList.map(cat => [cat, 0])),
        focusedSum: 0,
        focusedCount: 0,
        childSum: 0,
        childCount: 0,
        prevSum: 0,
        prevCount: 0
      });
    }
    const entry = hexMap.get(cell);
    entry.total += 1;
    // Real SCB child share of the building's DESO — aggregated to the hex cell
    // below so the mezo DR / map can colour cells by "who lives there" (same as
    // the district path). Color-only; never enters the fairness matrix.
    const childRaw = (typeof epiDesoProps === 'function' && props?.__deso != null)
      ? Number(epiDesoProps(props.__deso)?.child_frac) : NaN;
    if (Number.isFinite(childRaw)) {
      entry.childSum += childRaw;
      entry.childCount += 1;
    }
    if (Number.isFinite(score)) {
      entry.sum += score;
      entry.count += 1;
    }
    if (Number.isFinite(overallScore)) {
      entry.overallSum += overallScore;
      entry.overallCount += 1;
    }
    for (const cat of catList) {
      const val = catScores?.[cat];
      if (!Number.isFinite(val)) continue;
      entry.catSums[cat] += val;
      entry.catCounts[cat] += 1;
    }
    if (Number.isFinite(focusedScore)) {
      entry.focusedSum += focusedScore;
      entry.focusedCount += 1;
    }
    const prevScore = Number.isFinite(props?.__prevScore) ? props.__prevScore : null;
    if (Number.isFinite(prevScore)) {
      entry.prevSum += prevScore;
      entry.prevCount += 1;
    }
  }

  const data = [];
  const buildEntry = (entry) => {
    const fairByCat = {};
    for (const cat of catList) {
      const count = entry.catCounts[cat];
      fairByCat[cat] = count ? entry.catSums[cat] / count : null;
    }
    const mean = entry.count ? entry.sum / entry.count : null;
    const overallMean = entry.overallCount ? entry.overallSum / entry.overallCount : null;
    const focusedMean = entry.focusedCount ? entry.focusedSum / entry.focusedCount : null;
    const childMean = entry.childCount ? entry.childSum / entry.childCount : null;
    const prevMean = entry.prevCount ? entry.prevSum / entry.prevCount : null;
    return {
      hex: entry.hex,
      __score: mean,
      __count: entry.total,
      __fairOverall: overallMean,
      __fairByCat: fairByCat,
      __fairFocused: focusedMean,
      __fairFocusedCat: focusedMean != null ? focusedPOICat : null,
      __childReal: childMean,
      __prevScore: prevMean
    };
  };

  if (coverageCells?.length) {
    coverageCells.forEach((cell) => {
      const entry = hexMap.get(cell);
      const fallbackScore = entry?.total >= MEZO_MIN_COUNT ? null : mezoFallbackScoreForCell(cell, h3, poiByCat);
      if (!entry) {
        // Empty mezo cells should stay visually neutral (gray):
        // there are no buildings to aggregate fairness for.
        data.push(withMezoPrevScore({
          hex: cell,
          __score: null,
          __count: 0,
          __fairOverall: null,
          __fairByCat: {},
          __fairFocused: null,
          __fairFocusedCat: null
        }));
        return;
      }
      if (entry.total < MEZO_MIN_COUNT) {
        const fairByCat = fairCategory && fairCategory !== 'mix' && Number.isFinite(fallbackScore)
          ? { [fairCategory]: fallbackScore }
          : {};
        data.push(withMezoPrevScore({
          hex: entry.hex,
          __score: Number.isFinite(fallbackScore) ? fallbackScore : null,
          __count: entry.total,
          __fairOverall: null,
          __fairByCat: fairByCat,
          __fairFocused: null,
          __fairFocusedCat: null,
          __prevScore: entry.prevCount ? (entry.prevSum / entry.prevCount) : null
        }));
        return;
      }
      data.push(withMezoPrevScore(buildEntry(entry)));
    });
  } else {
    hexMap.forEach((entry) => {
      if (entry.total < MEZO_MIN_COUNT) return;
      data.push(withMezoPrevScore(buildEntry(entry)));
    });
  }

  mezoHexData = data;
  mezoScoreTick++;
  markAggregateSelectionsFromBuildings((baseCityFC?.features || []).filter(f => f?.properties?._drSelected));
  if (parallelCoordsOpen && currentParallelCoordsMode() === 'mezo') {
    updateParallelCoordsPanel();
  }
}




