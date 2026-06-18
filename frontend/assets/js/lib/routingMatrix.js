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
  return { coords, keyToRow, grid };
}

// Load matrices for (city, mode) covering `cats`. Rebuilds when city/mode changes.
async function ensureRoutingMatrices(cityKey, mode, cats) {
  const city = cityKey || routingCurrentCity();
  const sig = `${city}|${mode}`;
  const need = (sig === _routingLoadSig && _routingLoadPromise) ? _routingLoadPromise : null;
  if (need) await need;
  if (!ROUTING || ROUTING.sig !== sig) {
    ROUTING = { sig, city, mode, coords: null, keyToRow: null, grid: null, cell: _ROUTING_CELL,
                cats: {}, rowCache: new Map() };
  }
  const missing = cats.filter(c => !(c in ROUTING.cats));
  if (!missing.length) return ROUTING;
  _routingLoadSig = sig;
  _routingLoadPromise = (async () => {
    const loaded = await Promise.all(missing.map(c => _fetchCat(city, mode, c).then(j => [c, j])));
    for (const [c, j] of loaded) {
      ROUTING.cats[c] = j ? { m: j.m, v: j.v, key: j.key } : null;
      // Build the shared index from the first matrix that has a key list.
      if (j && j.key && !ROUTING.keyToRow) {
        const idx = _buildIndex(j.key);
        ROUTING.coords = idx.coords; ROUTING.keyToRow = idx.keyToRow; ROUTING.grid = idx.grid;
      }
    }
    return ROUTING;
  })();
  await _routingLoadPromise;
  return ROUTING;
}

function routingHasCat(cat) { return !!(ROUTING && ROUTING.cats[cat]); }
function routingReady() { return !!(ROUTING && ROUTING.keyToRow); }

// Resolve a building centroid to its matrix row: exact key, else nearest-key snap.
function routingRowForPoint(lon, lat) {
  if (!ROUTING || !ROUTING.keyToRow) return -1;
  const k = _rkey(lon, lat);
  const exact = ROUTING.keyToRow.get(k);
  if (exact !== undefined) return exact;
  const cached = ROUTING.rowCache.get(k);
  if (cached !== undefined) return cached;
  // snap: scan 3x3 grid neighborhood for nearest baked key within tolerance
  const cx = Math.floor(lon / ROUTING.cell), cy = Math.floor(lat / ROUTING.cell);
  let best = -1, bestD = Infinity;
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      const bucket = ROUTING.grid.get(`${cx + dx}_${cy + dy}`);
      if (!bucket) continue;
      for (const i of bucket) {
        const c = ROUTING.coords[i];
        const d = _rhav(lon, lat, c[0], c[1]);
        if (d < bestD) { bestD = d; best = i; }
      }
    }
  }
  const row = bestD <= ROUTING_SNAP_TOL_M ? best : -1;
  ROUTING.rowCache.set(k, row);
  return row;
}

// Network distances (metres) + opportunity values for a category at a row.
function routingDistsForRow(cat, row) {
  if (row < 0 || !ROUTING) return null;
  const mat = ROUTING.cats[cat];
  if (!mat || !mat.m || row >= mat.m.length) return null;
  return { dists: mat.m[row], vals: mat.v ? mat.v[row] : null };
}
