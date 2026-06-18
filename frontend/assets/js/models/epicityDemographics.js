// epicityDemographics.js — per-DESO socioeconomic layer (from EpiCity SCB data)
// wired into the fairness/Gini model. Loads the baked
//   assets/data/cities/<key>/demographics/deso.geojson
// (DESO polygons + pop, income, child_frac, elder_frac, dependency, needZ),
// then maps every building to its DESO to produce:
//   epiBuildingNeedMap : Map<featIdx, needZ>   (composite deprivation z-score)
//   epiBuildingPopMap  : Map<featIdx, residents> (DESO pop, floor-area distributed)
// Used by models/fairness.js as demand + need/equity weights. Pure local data
// (no runtime APIs). The same per-DESO props also back demographic map overlays.

let EPI_DEMO = null;                 // { city, features, index:[{props,bbox,feat}] }
let epiBuildingNeedMap = null;       // Map<featIdx, needZ>
let epiBuildingPopMap = null;        // Map<featIdx, residents>
let epiBuildingDesoMap = null;       // Map<featIdx, desoCode> (for overlays/popups)
let _epiDemoCity = null;
let _epiDemoLoadPromise = null;

function _epiCurrentCityKey() {
  return (document.getElementById('citySelect')?.value) || 'vaxjo';
}

function _epiPolyBBox(geom) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const scan = (coords) => {
    for (const ring of coords) {
      const rings = Array.isArray(ring[0][0]) ? ring : [ring]; // MultiPolygon vs Polygon ring
      for (const r of rings) {
        for (const pt of r) {
          if (pt[0] < minX) minX = pt[0];
          if (pt[0] > maxX) maxX = pt[0];
          if (pt[1] < minY) minY = pt[1];
          if (pt[1] > maxY) maxY = pt[1];
        }
      }
    }
  };
  try {
    if (geom.type === 'Polygon') scan(geom.coordinates);
    else if (geom.type === 'MultiPolygon') for (const poly of geom.coordinates) scan(poly);
  } catch {}
  return [minX, minY, maxX, maxY];
}

async function loadEpicityDemographics(cityKey) {
  if (EPI_DEMO && EPI_DEMO.city === cityKey) return EPI_DEMO;
  if (_epiDemoCity === cityKey && _epiDemoLoadPromise) return _epiDemoLoadPromise;
  _epiDemoCity = cityKey;
  _epiDemoLoadPromise = (async () => {
    try {
      const res = await fetch(`assets/data/cities/${cityKey}/demographics/deso.geojson`, { cache: 'force-cache' });
      if (!res.ok) { EPI_DEMO = null; return null; }
      const fc = await res.json();
      const index = (fc.features || []).map(feat => ({
        feat, props: feat.properties || {}, bbox: _epiPolyBBox(feat.geometry || {}),
      }));
      EPI_DEMO = { city: cityKey, features: fc.features || [], meta: fc.meta || null, index };
      return EPI_DEMO;
    } catch (e) {
      console.warn('[epi-demo] load failed for', cityKey, e);
      EPI_DEMO = null; return null;
    }
  })();
  return _epiDemoLoadPromise;
}

// Inline floor-area proxy (footprint * floors), mirrors fairness.js logic so the
// module is independent of script load order.
function _epiFloorAreaM2(feature) {
  let footprint = 80;
  try { const a = turf.area(feature); if (a > 0) footprint = a; } catch {}
  const p = feature?.properties || {};
  const height = Number(p.height || p.Hojd || p['building:height'] || 0);
  const levels = Number(p['building:levels'] || p.floors || 0);
  const floors = levels > 0 ? levels : (height > 0 ? Math.max(1, Math.round(height / 3)) : 1);
  return footprint * Math.min(floors, 40);
}

function _epiDesoForPoint(lon, lat) {
  if (!EPI_DEMO) return null;
  const pt = turf.point([lon, lat]);
  for (const entry of EPI_DEMO.index) {
    const b = entry.bbox;
    if (lon < b[0] || lon > b[2] || lat < b[1] || lat > b[3]) continue; // bbox reject
    try { if (turf.booleanPointInPolygon(pt, entry.feat)) return entry; } catch {}
  }
  return null;
}

// Residential test via Lantmäteriet purpose (`andamal1` = "Bostad;…"), present in
// all byggnad files. Falls back to OSM/category heuristics if andamal1 is absent.
function _epiIsResidential(props) {
  const a = String(props.andamal1 || '').toLowerCase();
  if (a) return a.startsWith('bostad');
  const cat = String(props.category || props.building || props.objekttyp || '').toLowerCase();
  return cat.includes('bostad') || cat === 'residential' || cat === 'house' ||
         cat === 'apartments' || cat === 'detached';
}

// Build per-building need + demand maps via point-in-DESO (once per city).
// needZ is set for ALL buildings; population (demand) is distributed only across
// RESIDENTIAL buildings by floor area, so non-residential parcels don't absorb
// residents (the earlier all-buildings version over-weighted commercial → Gini 0.66).
function buildEpicityBuildingMaps() {
  epiBuildingNeedMap = new Map();
  epiBuildingPopMap = new Map();
  epiBuildingDesoMap = new Map();
  if (!EPI_DEMO || !baseCityFC?.features) return;

  const desoFloor = new Map();            // residential floor area per DESO
  const perBuilding = new Array(baseCityFC.features.length);
  let residentialCount = 0;
  baseCityFC.features.forEach((feat, idx) => {
    let c; try { c = turf.centroid(feat).geometry.coordinates; } catch { return; }
    if (!c) return;
    const entry = _epiDesoForPoint(c[0], c[1]);
    if (!entry) return;
    const code = entry.props.deso;
    const needZ = entry.props.needZ;
    if (Number.isFinite(needZ)) epiBuildingNeedMap.set(idx, needZ);
    epiBuildingDesoMap.set(idx, code);
    const residential = _epiIsResidential(feat.properties || {});
    const fa = residential ? _epiFloorAreaM2(feat) : 0;
    perBuilding[idx] = { code, fa, pop: entry.props.pop || 0, residential };
    if (residential) { desoFloor.set(code, (desoFloor.get(code) || 0) + fa); residentialCount++; }
  });

  // Distribute each DESO's population across its RESIDENTIAL buildings by floor area.
  for (let idx = 0; idx < perBuilding.length; idx++) {
    const b = perBuilding[idx];
    if (!b || !b.residential) continue;
    const totalFloor = desoFloor.get(b.code) || 0;
    if (totalFloor > 0 && b.pop > 0) {
      epiBuildingPopMap.set(idx, (b.fa / totalFloor) * b.pop);
    }
  }
  console.log('[epi-demo] built maps —',
    epiBuildingNeedMap.size, 'buildings with needZ;',
    residentialCount, 'residential;',
    epiBuildingPopMap.size, 'with population estimate;',
    EPI_DEMO.features.length, 'DESOs.');
}

// Ensure the active city's demographics are loaded AND mapped to buildings.
async function ensureEpicityDemographics(cityKey) {
  if (typeof EPI_DEMOGRAPHICS_ENABLED !== 'undefined' && !EPI_DEMOGRAPHICS_ENABLED) return null;
  const key = cityKey || _epiCurrentCityKey();
  await loadEpicityDemographics(key);
  // (Re)build building maps if missing or the city changed under them.
  if (EPI_DEMO && (!epiBuildingNeedMap || EPI_DEMO.city !== _epiMappedCity)) {
    buildEpicityBuildingMaps();
    _epiMappedCity = EPI_DEMO.city;
  } else if (!EPI_DEMO) {
    epiBuildingNeedMap = null; epiBuildingPopMap = null; epiBuildingDesoMap = null;
  }
  return EPI_DEMO;
}
let _epiMappedCity = null;

// Reset building maps when buildings reload (city switch); call from city loader.
function resetEpicityBuildingMaps() {
  epiBuildingNeedMap = null; epiBuildingPopMap = null;
  epiBuildingDesoMap = null; _epiMappedCity = null;
}
