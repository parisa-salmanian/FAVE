// routingMatrix.js — consume the baked road/walk/cycle distance matrices so
// accessibility uses real NETWORK distances instead of straight-line haversine.
//
// Baked files: assets/data/cities/<key>/routing/<mode>/<cat>.json
//   { mode, cat, n, key:[ "lon,lat", ... ],  // one per building (shared order)
//     m:[ [d0..d14], ... ],                  // network distances (metres) to nearest n POIs
//     v:[ [v0..v14], ... ] }                 // opportunity values
//
// All categories for a mode share the same `key` order, so a building resolves
// to one row index reused across categories. Exact key match is ~81% (the bake
// used a slightly different centroid), so unmatched centroids snap to the nearest
// baked key within a tolerance (the key IS the building coord) → ~100% coverage.

let ROUTING = null;                 // { sig, city, mode, coords, keyToRow, grid, cell, cats:{cat:{m,v}} }
let _routingLoadPromise = null;
let _routingLoadSig = null;

const ROUTING_SNAP_TOL_M = 45;      // nearest-key snap tolerance
const _ROUTING_CELL = 0.006;        // ~0.6 km grid cells

function routingCurrentCity() {
  return (document.getElementById('citySelect')?.value) || 'vaxjo';
}
function _rkey(lon, lat) { return `${(+lon).toFixed(6)},${(+lat).toFixed(6)}`; }
function _rhav(aLon, aLat, bLon, bLat) {
  const R = 6371000, tr = d => d * Math.PI / 180;
  const dLat = tr(bLat - aLat), dLon = tr(bLon - aLon);
  const s = Math.sin(dLat / 2) ** 2 + Math.cos(tr(aLat)) * Math.cos(tr(bLat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(s));
}

async function _fetchCat(city, mode, cat) {
  try {
    const res = await fetch(`assets/data/cities/${city}/routing/${mode}/${cat}.json`, { cache: 'force-cache' });
    if (!res.ok) return null;
    const j = await res.json();
    return (j && Array.isArray(j.m)) ? j : null;
  } catch { return null; }
}

function _buildIndex(keyList) {
  const coords = new Array(keyList.length);
  const keyToRow = new Map();
  const grid = new Map();
  for (let i = 0; i < keyList.length; i++) {
    const k = keyList[i];
    keyToRow.set(k, i);
    const comma = k.indexOf(',');
    const lon = +k.slice(0, comma), lat = +k.slice(comma + 1);
    coords[i] = [lon, lat];
    const gk = `${Math.floor(lon / _ROUTING_CELL)}_${Math.floor(lat / _ROUTING_CELL)}`;
    let bucket = grid.get(gk); if (!bucket) { bucket = []; grid.set(gk, bucket); }
    bucket.push(i);
  }
  return { coords, keyToRow, grid, rowCache: new Map() };
}

// Cheap signature of a key array so categories that share the SAME building
// order reuse one index (they usually do), while a category baked with a
// different order/length (e.g. Kalmar walking hospital/university/veterinary =
// 85 745 rows vs 108 604 for the rest) gets its OWN index. This is the fix for
// the row-aliasing bug: a single shared index built from the first category
// silently mapped the wrong building's distances for any category whose key
// order differed.
function _sigOfKeys(keyList) {
  const n = keyList.length;
  if (!n) return '0';
  return `${n}|${keyList[0]}|${keyList[n >> 1]}|${keyList[n - 1]}`;
}

// Resolve a point to a row WITHIN a specific index (exact key, else nearest-key
// snap within tolerance). Per-index rowCache so the fairness hot-path pays the
// snap once per building per distinct key order.
function _rowInIndex(idx, lon, lat) {
  if (!idx) return -1;
  const k = _rkey(lon, lat);
  const exact = idx.keyToRow.get(k);
  if (exact !== undefined) return exact;
  const cached = idx.rowCache.get(k);
  if (cached !== undefined) return cached;
  const cx = Math.floor(lon / _ROUTING_CELL), cy = Math.floor(lat / _ROUTING_CELL);
  let best = -1, bestD = Infinity;
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      const bucket = idx.grid.get(`${cx + dx}_${cy + dy}`);
      if (!bucket) continue;
      for (const i of bucket) {
        const c = idx.coords[i];
        const d = _rhav(lon, lat, c[0], c[1]);
        if (d < bestD) { bestD = d; best = i; }
      }
    }
  }
  const row = bestD <= ROUTING_SNAP_TOL_M ? best : -1;
  idx.rowCache.set(k, row);
  return row;
}

// Load matrices for (city, mode) covering `cats`. Rebuilds when city/mode changes.
async function ensureRoutingMatrices(cityKey, mode, cats) {
  const city = cityKey || routingCurrentCity();
  const sig = `${city}|${mode}`;
  const need = (sig === _routingLoadSig && _routingLoadPromise) ? _routingLoadPromise : null;
  if (need) await need;
  if (!ROUTING || ROUTING.sig !== sig) {
    ROUTING = { sig, city, mode, coords: null, keyToRow: null, grid: null, cell: _ROUTING_CELL,
                cats: {}, rowCache: new Map(), indexBySig: new Map(), sharedIdx: null };
  }
  const missing = cats.filter(c => !(c in ROUTING.cats));
  if (!missing.length) return ROUTING;
  _routingLoadSig = sig;
  _routingLoadPromise = (async () => {
    const loaded = await Promise.all(missing.map(c => _fetchCat(city, mode, c).then(j => [c, j])));
    for (const [c, j] of loaded) {
      ROUTING.cats[c] = j ? { m: j.m, v: j.v, key: j.key, idx: null } : null;
      if (j && j.key) {
        // Give this category the index matching ITS OWN key order — reused across
        // categories that share the same order (deduped by signature), a fresh one
        // otherwise. This is what makes routingDistsForPoint read the RIGHT row.
        const s = _sigOfKeys(j.key);
        let idx = ROUTING.indexBySig.get(s);
        if (!idx) { idx = _buildIndex(j.key); ROUTING.indexBySig.set(s, idx); }
        ROUTING.cats[c].idx = idx;
        // Keep the first index as the shared default (routingReady/routingRowForPoint).
        if (!ROUTING.keyToRow) {
          ROUTING.coords = idx.coords; ROUTING.keyToRow = idx.keyToRow; ROUTING.grid = idx.grid;
          ROUTING.sharedIdx = idx;
        }
      }
    }
    return ROUTING;
  })();
  await _routingLoadPromise;
  return ROUTING;
}

function routingHasCat(cat) { return !!(ROUTING && ROUTING.cats[cat]); }

// Network / straight-line ratio of a category, measured on the loaded matrix with
// the same definition as tools' net/crow audit: mean nearest NETWORK distance over
// mean nearest STRAIGHT-LINE distance (pairs closer than 25 m excluded, rows sampled
// evenly). A facility that has no baked row — a what-if addition — is scored as
// straight-line × this factor so it sits on the same footing as the baked ones
// instead of flipping the whole category to straight-line. `poiCoords` = the
// [lon,lat] of the category's EXISTING (baked) facilities. Cached per POI set.
const ROUTING_DETOUR_DEFAULT = 1.3;
const _ROUTING_DETOUR_SAMPLE = 6000;
function routingDetourFactor(cat, poiCoords) {
  if (!ROUTING) return ROUTING_DETOUR_DEFAULT;
  const mat = ROUTING.cats[cat];
  if (!mat || !mat.m || !mat.m.length) return ROUTING_DETOUR_DEFAULT;
  const pts = (poiCoords || []).filter(c => Array.isArray(c) && Number.isFinite(c[0]) && Number.isFinite(c[1]));
  if (!pts.length) return ROUTING_DETOUR_DEFAULT;
  let h = 0; for (const p of pts) h += p[0] * 7.31 + p[1] * 3.17;
  const cacheKey = `${cat}|${pts.length}|${h.toFixed(4)}`;
  if (!ROUTING.detourCache) ROUTING.detourCache = new Map();
  if (ROUTING.detourCache.has(cacheKey)) return ROUTING.detourCache.get(cacheKey);
  const idx = mat.idx || ROUTING.sharedIdx;
  const n = mat.m.length;
  const step = Math.max(1, Math.floor(n / _ROUTING_DETOUR_SAMPLE));
  let netSum = 0, crowSum = 0, cnt = 0;
  for (let row = 0; row < n; row += step) {
    const d0 = mat.m[row] ? mat.m[row][0] : NaN;
    if (!Number.isFinite(d0) || d0 <= 0) continue;
    const c = idx && idx.coords ? idx.coords[row] : null;
    if (!c) continue;
    let best = Infinity;
    for (let k = 0; k < pts.length; k++) { const d = _rhav(c[0], c[1], pts[k][0], pts[k][1]); if (d < best) best = d; }
    if (!(best >= 25)) continue;
    netSum += d0; crowSum += best; cnt++;
  }
  let f = (cnt >= 20 && crowSum > 0) ? netSum / crowSum : ROUTING_DETOUR_DEFAULT;
  f = Math.min(2.5, Math.max(1.0, f));
  ROUTING.detourCache.set(cacheKey, f);
  return f;
}
function routingReady() { return !!(ROUTING && ROUTING.keyToRow); }

// Resolve a building centroid to a row in the SHARED (first-loaded) index. Kept
// for backward compat / callers that don't need per-category correctness. NB: a
// row from here is only valid to pass to routingDistsForRow for categories that
// share the shared index's key order — prefer routingDistsForPoint (below), which
// resolves per category and is always correct.
function routingRowForPoint(lon, lat) {
  if (!ROUTING || !ROUTING.sharedIdx) return -1;
  return _rowInIndex(ROUTING.sharedIdx, lon, lat);
}

// Network distances (metres) + opportunity values for a category at a row that
// was resolved against THAT category's own index. Correct regardless of whether
// categories share a key order.
function routingDistsForRow(cat, row) {
  if (row < 0 || !ROUTING) return null;
  const mat = ROUTING.cats[cat];
  if (!mat || !mat.m || row >= mat.m.length) return null;
  return { dists: mat.m[row], vals: mat.v ? mat.v[row] : null };
}

// PREFERRED accessor: resolve a point to the category's own row, then return its
// distances. This is the fix for the row-aliasing bug — a building's row is looked
// up in the index built from `cat`'s key array, so `mat.m[row]` is always this
// building's real distances even when categories were baked in different orders.
function routingDistsForPoint(cat, lon, lat) {
  if (!ROUTING) return null;
  const mat = ROUTING.cats[cat];
  if (!mat || !mat.m) return null;
  const row = _rowInIndex(mat.idx || ROUTING.sharedIdx, lon, lat);
  if (row < 0 || row >= mat.m.length) return null;
  return { dists: mat.m[row], vals: mat.v ? mat.v[row] : null };
}
