// access2sfca.js — runtime consumer for the baked network-accurate E2SFCA
// (Enhanced 2-Step Floating Catchment Area) accessibility, a COMPANION metric to
// the headline gravity fairness model. The gravity model answers "how close are
// opportunities"; 2SFCA answers "how much supply is actually available to me once
// everyone else competing for it is accounted for" (supply-to-demand crowding).
//
// Baked by tools/bake_access2sfca.py to:
//   assets/data/cities/<key>/access2sfca/index.json   { key:["lon,lat",...], cats, modes }
//   assets/data/cities/<key>/access2sfca/<mode>.json   { cats:{ <cat>:{ pois, a:[[idx,A],...] } } }
// `a` is sparse (only buildings that can reach a POI in the catchment); `idx`
// indexes the shared `key` array in index.json. Runtime joins a building to a row
// by coordinate snap (≤45 m), mirroring lib/routingMatrix.js — no featIdx coupling.
//
// 2SFCA values scale inversely with each category's total demand (a single
// hospital serving 26 000 people yields a tiny absolute A; a grocery a large one),
// so raw values are NOT comparable across categories. We normalise PER CATEGORY
// (log1p + robust p10..p95 clamp, like tools/analysis/city_pipeline.py) to a 0..1
// "provision" score, then combine categories by mean for an overall score.

let ACCESS2SFCA = null;            // { sig, city, mode, coords, keyToRow, grid, cats:{cat:{poiById,rowToA,stats}} }
let _a2sLoadPromise = null;
let _a2sLoadSig = null;

const A2S_SNAP_TOL_M = 45;         // nearest-key snap tolerance (key IS the building coord)
const _A2S_CELL = 0.006;           // ~0.6 km grid cells for snap

function a2sCurrentCity() {
  return (document.getElementById('citySelect')?.value) || 'vaxjo';
}
function _a2sKey(lon, lat) { return `${(+lon).toFixed(6)},${(+lat).toFixed(6)}`; }
function _a2sHav(aLon, aLat, bLon, bLat) {
  const R = 6371000, tr = d => d * Math.PI / 180;
  const dLat = tr(bLat - aLat), dLon = tr(bLon - aLon);
  const s = Math.sin(dLat / 2) ** 2 + Math.cos(tr(aLat)) * Math.cos(tr(bLat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(s));
}

// Robust percentile of a (already nonzero) sorted-or-unsorted numeric array.
function _a2sPercentile(sorted, p) {
  const n = sorted.length;
  if (n === 0) return 0;
  const i = (n - 1) * p;
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? sorted[lo] : sorted[lo] * (1 - (i - lo)) + sorted[hi] * (i - lo);
}

// Per-category normalisation stats from the sparse A values (log1p space,
// p10 floor / p95 span clamp). Buildings absent from `a` have A=0 → norm 0.
function _a2sBuildStats(aPairs) {
  const logs = [];
  for (let k = 0; k < aPairs.length; k++) {
    const v = aPairs[k][1];
    if (v > 0) logs.push(Math.log1p(v));
  }
  logs.sort((x, y) => x - y);
  const floor = _a2sPercentile(logs, 0.10);
  const span = Math.max(1e-9, _a2sPercentile(logs, 0.95) - floor);
  return { floor, span, count: logs.length };
}
function _a2sNorm(rawA, stats) {
  if (!(rawA > 0)) return 0;
  const v = Math.log1p(rawA);
  return Math.min(1, Math.max(0, (v - stats.floor) / stats.span));
}

function _a2sBuildSnapIndex(keyList) {
  const coords = new Array(keyList.length);
  const keyToRow = new Map();
  const grid = new Map();
  for (let i = 0; i < keyList.length; i++) {
    const k = keyList[i];
    keyToRow.set(k, i);
    const comma = k.indexOf(',');
    const lon = +k.slice(0, comma), lat = +k.slice(comma + 1);
    coords[i] = [lon, lat];
    const gk = `${Math.floor(lon / _A2S_CELL)}_${Math.floor(lat / _A2S_CELL)}`;
    let bucket = grid.get(gk); if (!bucket) { bucket = []; grid.set(gk, bucket); }
    bucket.push(i);
  }
  return { coords, keyToRow, grid };
}

// Load index + mode file for (city, mode); build snap index, per-cat row→A maps,
// and per-cat normalisation stats. Rebuilds when city/mode changes.
async function ensureAccess2sfca(cityKey, mode) {
  const city = cityKey || a2sCurrentCity();
  const m = mode || 'walking';
  const sig = `${city}|${m}`;
  if (ACCESS2SFCA && ACCESS2SFCA.sig === sig) return ACCESS2SFCA;
  if (_a2sLoadSig === sig && _a2sLoadPromise) return _a2sLoadPromise;
  _a2sLoadSig = sig;
  _a2sLoadPromise = (async () => {
    try {
      const base = `assets/data/cities/${city}/access2sfca`;
      // no-cache (revalidate, 304 when unchanged) — NOT force-cache. These files
      // are re-baked whenever categories are added (e.g. the 16 EpiCity cats added
      // for the smaller cities), and force-cache would keep serving a stale index/
      // mode file that lacks the new categories, so they'd silently never appear in
      // the inspector. Same trap synthpop.js hit with its demo arrays.
      const [idxRes, modeRes] = await Promise.all([
        fetch(`${base}/index.json`, { cache: 'no-cache' }),
        fetch(`${base}/${m}.json`, { cache: 'no-cache' }),
      ]);
      if (!idxRes.ok || !modeRes.ok) { ACCESS2SFCA = null; return null; }
      const idx = await idxRes.json();
      const modeData = await modeRes.json();
      const snap = _a2sBuildSnapIndex(idx.key || []);

      const cats = {};
      for (const [cat, cd] of Object.entries(modeData.cats || {})) {
        const rowToA = new Map();
        for (const [row, A] of (cd.a || [])) rowToA.set(row, A);
        const poiById = new Map();
        for (const p of (cd.pois || [])) if (p.id != null) poiById.set(p.id, p);
        cats[cat] = {
          rowToA, poiById, pois: cd.pois || [],
          catchment_m: cd.catchment_m, sigma_m: cd.sigma_m,
          n_pois: cd.n_pois, n_reachable: cd.n_reachable,
          stats: _a2sBuildStats(cd.a || []),
        };
      }
      ACCESS2SFCA = {
        sig, city, mode: m, ...snap, cats,
        catList: idx.cats || Object.keys(cats), params: modeData.params || null,
      };
      return ACCESS2SFCA;
    } catch (e) {
      console.warn('[2sfca] load failed for', city, m, e);
      ACCESS2SFCA = null; return null;
    }
  })();
  return _a2sLoadPromise;
}

// Resolve a building centroid [lon,lat] to a baked row index (exact key, else
// nearest within A2S_SNAP_TOL_M). Returns -1 if no baked building is close.
function access2sfcaRowForPoint(lon, lat) {
  if (!ACCESS2SFCA) return -1;
  const exact = ACCESS2SFCA.keyToRow.get(_a2sKey(lon, lat));
  if (exact !== undefined) return exact;
  const cx = Math.floor(lon / _A2S_CELL), cy = Math.floor(lat / _A2S_CELL);
  let best = -1, bestD = A2S_SNAP_TOL_M;
  for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
    const bucket = ACCESS2SFCA.grid.get(`${cx + dx}_${cy + dy}`);
    if (!bucket) continue;
    for (const i of bucket) {
      const c = ACCESS2SFCA.coords[i];
      const d = _a2sHav(lon, lat, c[0], c[1]);
      if (d < bestD) { bestD = d; best = i; }
    }
  }
  return best;
}

// Per-building 2SFCA: { row, overall, cats:{ cat:{ raw, norm, catchment_m, n_pois } } }
// overall = mean of per-category normalised provision (unreachable cats count as 0,
// so poor reach is penalised). Returns null if no baked building is near the point.
function access2sfcaForRow(row) {
  if (!ACCESS2SFCA || row < 0) return null;
  const cats = {};
  let sum = 0, nCat = 0;
  for (const cat of ACCESS2SFCA.catList) {
    const cd = ACCESS2SFCA.cats[cat];
    if (!cd) continue;
    const raw = cd.rowToA.get(row) || 0;
    const norm = _a2sNorm(raw, cd.stats);
    cats[cat] = { raw, norm, catchment_m: cd.catchment_m, n_pois: cd.n_pois };
    sum += norm; nCat++;
  }
  return { row, overall: nCat ? sum / nCat : 0, cats };
}

function access2sfcaForPoint(lon, lat) {
  return access2sfcaForRow(access2sfcaRowForPoint(lon, lat));
}

// Convenience: resolve straight from a GeoJSON feature (uses turf centroid).
function access2sfcaForFeature(feature) {
  try {
    const c = turf.centroid(feature).geometry.coordinates;
    return access2sfcaForPoint(c[0], c[1]);
  } catch { return null; }
}

// ---- Meso / macro aggregation -------------------------------------------------
// Aggregate per-building 2SFCA over a set of baked rows (the buildings inside a
// district or hex). Per category we report the MEAN normalised provision across
// the member buildings (unreachable buildings contribute 0, so poor coverage is
// penalised) plus the share of members that can reach the category at all.
// overall = mean of the per-category means — same definition as per building, so
// micro/meso/macro read on one comparable 0..1 scale.
function access2sfcaAggregateForRows(rows) {
  if (!ACCESS2SFCA || !rows || !rows.length) return null;
  const cats = {};
  let overallSum = 0, overallN = 0;
  for (const cat of ACCESS2SFCA.catList) {
    const cd = ACCESS2SFCA.cats[cat];
    if (!cd) continue;
    let sum = 0, reach = 0;
    for (const row of rows) {
      const raw = cd.rowToA.get(row) || 0;
      sum += _a2sNorm(raw, cd.stats);
      if (raw > 0) reach++;
    }
    const normMean = sum / rows.length;
    cats[cat] = { normMean, reachableFrac: reach / rows.length, n: rows.length,
                  catchment_m: cd.catchment_m, n_pois: cd.n_pois };
    overallSum += normMean; overallN++;
  }
  return { n: rows.length, overall: overallN ? overallSum / overallN : 0, cats };
}

// Baked rows whose centroid falls inside a GeoJSON polygon/multipolygon feature.
// Bbox pre-filter then turf point-in-polygon over ACCESS2SFCA.coords (~50k pts).
function access2sfcaRowsInPolygon(feature) {
  if (!ACCESS2SFCA || !feature) return [];
  let bbox;
  try { bbox = turf.bbox(feature); } catch { return []; }
  const [minX, minY, maxX, maxY] = bbox;
  const out = [];
  const coords = ACCESS2SFCA.coords;
  for (let i = 0; i < coords.length; i++) {
    const c = coords[i];
    if (c[0] < minX || c[0] > maxX || c[1] < minY || c[1] > maxY) continue;
    try { if (turf.booleanPointInPolygon(c, feature)) out.push(i); } catch { /* skip */ }
  }
  return out;
}

// Baked rows whose centroid falls in the given H3 cell (resolution read from the
// cell id, so it works at any mezo zoom). Uses the global h3-js (v3 API).
// Cache: hexId -> [rowIdx] for the current city/mode (ACCESS2SFCA.sig) + h3 res.
// Built once by a single pass over all coords. Without it, every call rescanned
// all ~50k coords with an h3 conversion, and the mezo DR calls this once PER CELL
// → tens of millions of conversions per Run (froze the tab, "page unresponsive").
// With the index each call is an O(1) Map lookup and the pass runs once per city.
let _a2sHexIndex = null;
function access2sfcaRowsInHex(hexId) {
  if (!ACCESS2SFCA || !hexId || typeof h3 === 'undefined') return [];
  const res = (typeof h3.h3GetResolution === 'function') ? h3.h3GetResolution(hexId) : null;
  if (res == null) return [];
  const toCell = h3.geoToH3 || h3.latLngToCell;
  if (typeof toCell !== 'function') return [];
  if (!_a2sHexIndex || _a2sHexIndex.sig !== ACCESS2SFCA.sig || _a2sHexIndex.res !== res) {
    const map = new Map();
    const coords = ACCESS2SFCA.coords;
    for (let i = 0; i < coords.length; i++) {
      const c = coords[i];
      const h = toCell(c[1], c[0], res);
      let arr = map.get(h);
      if (!arr) { arr = []; map.set(h, arr); }
      arr.push(i);
    }
    _a2sHexIndex = { sig: ACCESS2SFCA.sig, res, map };
  }
  return _a2sHexIndex.map.get(hexId) || [];
}

function access2sfcaAggregateForPolygon(feature) {
  return access2sfcaAggregateForRows(access2sfcaRowsInPolygon(feature));
}
function access2sfcaAggregateForHex(hexId) {
  return access2sfcaAggregateForRows(access2sfcaRowsInHex(hexId));
}
