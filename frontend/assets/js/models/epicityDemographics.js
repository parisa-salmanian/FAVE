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

// Expanded per-DESO SCB profile (demographics/deso_full.json, baked by
// tools/bake_scb_full.py) — the FULL rich socio-demographic set that backs the DR
// "All Data" feature. Kept separate from EPI_DEMO (which is the deso.geojson base).
let EPI_DESO_FULL = null;            // { city, fields:[manifest], byDeso:{deso:{key:val}} }
let _epiDesoFullMap = null;          // Map<desoCode, {fieldKey: value}>

// Load deso_full.json for a city and splice its field manifest into the DR SCB
// feature list. Missing file / older city → the base 9 SCB features still work.
async function loadDesoFull(cityKey) {
  if (EPI_DESO_FULL && EPI_DESO_FULL.city === cityKey) return EPI_DESO_FULL;
  try {
    const res = await fetch(`assets/data/cities/${cityKey}/demographics/deso_full.json`, { cache: 'force-cache' });
    if (!res.ok) throw new Error('http ' + res.status);
    const d = await res.json();
    const by = d.byDeso || {};
    const m = new Map();
    for (const k in by) m.set(k, by[k]);
    EPI_DESO_FULL = { city: cityKey, fields: d.fields || [], byDeso: by };
    _epiDesoFullMap = m;
    if (typeof applyDrScbFullFields === 'function') applyDrScbFullFields(EPI_DESO_FULL.fields);
    console.log('[epi-demo] deso_full loaded —', (d.fields || []).length, 'fields,', m.size, 'DESOs for', cityKey);
    return EPI_DESO_FULL;
  } catch (e) {
    console.warn('[epi-demo] deso_full load failed for', cityKey, e);
    EPI_DESO_FULL = null; _epiDesoFullMap = null;
    if (typeof applyDrScbFullFields === 'function') applyDrScbFullFields(null); // fall back to base 9
    return null;
  }
}

// DESO code -> its expanded deso_full field dict ({fieldKey: value}) or null.
// Shared O(1) lookup used by DR (scbRowFields) to resolve the rich SCB fields at
// every scale without stamping ~160 props on every building/cell.
function epiDesoFull(code) {
  if (code == null || !_epiDesoFullMap) return null;
  return _epiDesoFullMap.get(code) || null;
}

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
    // Stamp the DESO code on the feature so views (e.g. the parallel-coordinates
    // plot) can read per-building demographics in O(1) without a featIdx lookup.
    // __bidx is a stable per-building id → deterministic seed for the runtime
    // per-building synthesis of the rich SCB fields at micro (drView _scbSynthValue).
    if (feat.properties) { feat.properties.__deso = code; feat.properties.__bidx = idx; }
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
  await loadDesoFull(key);   // rich SCB profile for the DR "All Data" set
  // (Re)build building maps if missing, the city changed, OR the building set
  // (baseCityFC) was swapped under them. Keying on the city NAME alone missed
  // city switches: the dropdown flips to the new city before the async building
  // load swaps baseCityFC (which is briefly null mid-switch), and the cache-hit
  // restore reassigns baseCityFC without a recompute — in both cases the maps got
  // built against a stale/empty building set, then the name guard latched and
  // never rebuilt, leaving demographics unbound. Tracking the baseCityFC identity
  // makes the rebuild self-heal on any building-set change.
  const fc = (typeof baseCityFC !== 'undefined') ? baseCityFC : null;
  if (EPI_DEMO && (!epiBuildingNeedMap || EPI_DEMO.city !== _epiMappedCity || fc !== _epiMappedFC)) {
    buildEpicityBuildingMaps();
    _epiMappedCity = EPI_DEMO.city;
    _epiMappedFC = fc;
  } else if (!EPI_DEMO) {
    epiBuildingNeedMap = null; epiBuildingPopMap = null; epiBuildingDesoMap = null;
  }
  return EPI_DEMO;
}
let _epiMappedCity = null;
let _epiMappedFC = null;

// DESO code -> baked socioeconomic props (income, needZ, child/elder/dependency,
// higher_ed, neet, income_support, male_frac). One shared lookup so every view
// (synthetic per-building reader, inspector, parallel-coords) resolves the
// "rest of the demographics" from the SAME source. Rebuilt on city change.
let _epiDesoByCode = null;
function epiDesoProps(code) {
  if (!EPI_DEMO || code == null) return null;
  if (!_epiDesoByCode || _epiDesoByCode.city !== EPI_DEMO.city) {
    const m = new Map();
    for (const f of (EPI_DEMO.features || [])) {
      const p = f.properties || {};
      if (p.deso != null) m.set(p.deso, p);
    }
    _epiDesoByCode = { city: EPI_DEMO.city, map: m };
  }
  return _epiDesoByCode.map.get(code) || null;
}

// Reset building maps when buildings reload (city switch); call from city loader.
function resetEpicityBuildingMaps() {
  _epiDesoByCode = null;
  epiBuildingNeedMap = null; epiBuildingPopMap = null;
  epiBuildingDesoMap = null; _epiMappedCity = null; _epiMappedFC = null;
  EPI_DESO_FULL = null; _epiDesoFullMap = null;   // force deso_full re-fetch for the new city
}
