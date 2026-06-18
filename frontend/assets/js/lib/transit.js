// transit.js — runtime public-transport network model for the "transit" travel
// mode. Loads the baked per-city network (assets/data/cities/<key>/transit/
// network.json: stops + stop->stop time matrix + per-stop boarding wait), and
// estimates building->POI transit travel time as:
//   access walk (origin -> nearest stop) + boarding wait + in-vehicle (matrix)
//   + egress walk (nearest stop -> POI).
// No external APIs at runtime (CLAUDE.md hard rule): all data is pre-baked.
//
// Consumed by models/fairness.js when fairnessTravelMode === 'transit'. When no
// network exists / a pair is unreachable, callers fall back to the speed model.

let TRANSIT_NET = null;            // { city, n, stops, waitMin, time, grid, cell }
let _transitLoadingKey = null;     // in-flight fetch guard
let _transitLoadPromise = null;

function _transitHaversineM(aLon, aLat, bLon, bLat) {
  const R = 6371000, toRad = d => d * Math.PI / 180;
  const dLat = toRad(bLat - aLat), dLon = toRad(bLon - aLon);
  const s = Math.sin(dLat / 2) ** 2 +
            Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(s));
}

function _transitBuildGrid(stops, cellDeg) {
  const grid = new Map();
  for (let i = 0; i < stops.length; i++) {
    const [lon, lat] = stops[i];
    const key = `${Math.floor(lon / cellDeg)}_${Math.floor(lat / cellDeg)}`;
    let bucket = grid.get(key);
    if (!bucket) { bucket = []; grid.set(key, bucket); }
    bucket.push(i);
  }
  return grid;
}

// Returns the k nearest stops within TRANSIT_MAX_ACCESS_M as
// [{ idx, accessMin }], ascending by walk time. Empty if none in range.
function transitNearestStops(lonlat, k = (typeof TRANSIT_NEAREST_STOPS_K !== 'undefined' ? TRANSIT_NEAREST_STOPS_K : 3)) {
  if (!TRANSIT_NET) return [];
  const [lon, lat] = lonlat;
  const cell = TRANSIT_NET.cell;
  const maxM = (typeof TRANSIT_MAX_ACCESS_M !== 'undefined') ? TRANSIT_MAX_ACCESS_M : 1500;
  const walkKmh = (typeof TRANSIT_ACCESS_WALK_KMH !== 'undefined') ? TRANSIT_ACCESS_WALK_KMH : 5;
  const cx = Math.floor(lon / cell), cy = Math.floor(lat / cell);
  const found = [];
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      const bucket = TRANSIT_NET.grid.get(`${cx + dx}_${cy + dy}`);
      if (!bucket) continue;
      for (const idx of bucket) {
        const s = TRANSIT_NET.stops[idx];
        const d = _transitHaversineM(lon, lat, s[0], s[1]);
        if (d <= maxM) found.push({ idx, accessMin: (d / 1000 / walkKmh) * 60 });
      }
    }
  }
  found.sort((a, b) => a.accessMin - b.accessMin);
  return found.slice(0, k);
}

// Nearest stops for a POI, cached on the POI object across a fairness compute.
function _transitPoiStops(poi) {
  if (poi.__tstops !== undefined) return poi.__tstops;
  poi.__tstops = transitNearestStops(poi.c);
  return poi.__tstops;
}

// Effective km (normalised to the reference walking speed, matching
// ifCityDistanceForMode's output) for a transit trip from precomputed
// originStops to a POI. Returns null when no transit path is usable.
function transitEffectiveKm(originStops, poi) {
  if (!TRANSIT_NET || !originStops || !originStops.length) return null;
  const poiStops = _transitPoiStops(poi);
  if (!poiStops.length) return null;
  const time = TRANSIT_NET.time, waitMin = TRANSIT_NET.waitMin;
  let best = Infinity;
  for (const os of originStops) {
    const row = time[os.idx];
    if (!row) continue;
    const board = os.accessMin + waitMin[os.idx];
    if (board >= best) continue;
    for (const ps of poiStops) {
      const ride = row[ps.idx];
      if (ride == null) continue;
      const total = board + ride + ps.egressMin;
      if (total < best) best = total;
    }
  }
  if (!Number.isFinite(best)) return null;
  const refSpeed = (typeof IF_CITY_REFERENCE_SPEED_KMH !== 'undefined') ? IF_CITY_REFERENCE_SPEED_KMH : 5;
  return (best / 60) * refSpeed;
}

function transitReady() {
  return !!(TRANSIT_NET && TRANSIT_NET.n > 0);
}

function _transitCurrentCityKey() {
  return (document.getElementById('citySelect')?.value) || 'vaxjo';
}

// Fetch + index the baked network for a city (idempotent per city).
async function loadTransitNetwork(cityKey) {
  if (TRANSIT_NET && TRANSIT_NET.city === cityKey) return TRANSIT_NET;
  if (_transitLoadingKey === cityKey && _transitLoadPromise) return _transitLoadPromise;
  _transitLoadingKey = cityKey;
  _transitLoadPromise = (async () => {
    try {
      const res = await fetch(`assets/data/cities/${cityKey}/transit/network.json`, { cache: 'force-cache' });
      if (!res.ok) { TRANSIT_NET = null; return null; }
      const net = await res.json();
      const cellDeg = 0.01; // ~1.1 km cells; query scans a 3x3 neighborhood
      // Precompute egressMin onto POI lookups lazily; stops grid here.
      TRANSIT_NET = {
        city: cityKey,
        n: net.n,
        stops: net.stops,
        waitMin: net.wait_min,
        time: net.time_min,
        cell: cellDeg,
        grid: _transitBuildGrid(net.stops, cellDeg),
        stats: net.stats || null,
      };
      // egressMin is symmetric to accessMin; transitNearestStops returns
      // accessMin, so reuse it as egressMin for POI stops.
      return TRANSIT_NET;
    } catch (e) {
      console.warn('[transit] failed to load network for', cityKey, e);
      TRANSIT_NET = null;
      return null;
    }
  })();
  return _transitLoadPromise;
}

// Ensure the network for the active city is loaded before a transit compute.
async function ensureTransitNetwork(cityKey) {
  const key = cityKey || _transitCurrentCityKey();
  return loadTransitNetwork(key);
}

// transitNearestStops returns {idx, accessMin}; POI egress reuses the same
// field name expected by transitEffectiveKm.
(function _aliasEgress() {
  const orig = transitNearestStops;
  // wrap to also expose accessMin as egressMin for POI-side usage
  // (kept as a thin alias so the matrix lookup reads ps.egressMin).
  // eslint-disable-next-line no-func-assign
  transitNearestStops = function (lonlat, k) {
    const arr = orig(lonlat, k);
    for (const s of arr) s.egressMin = s.accessMin;
    return arr;
  };
})();
