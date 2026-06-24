// synthpop.js — EpiCity SYNTHETIC per-building data at runtime.
//
// Loads the compact baked file
//   assets/data/cities/<key>/synthpop/buildings.json
// (per-byggnad-footprint synthetic pop / levels / area / zone, produced by
// tools/bake_synthpop.py by spatially transferring epicity/city.json onto the
// Lantmäteriet footprints) and joins it to the rendered buildings by centroid
// — exact turf.centroid key first, grid-nearest fallback ≤ tol — exactly like
// lib/access2sfca.js / lib/routingMatrix.js (no featIdx coupling to the bake).
//
// Produces, once per city:
//   synthBuildingPopMap : Map<featIdx, residents>   (genuine synthetic demand)
// and stamps each feature with __synthPop/__synthLevels/__synthArea/__synthZone
// so views (inspector, parallel-coords) read per-building synthetic values in
// O(1). Used by models/fairness.js as the preferred demand signal. Pure local
// data — honours the "no external APIs at runtime" rule.

let SYNTHPOP = null;                 // { city, keyToRow, coords, grid, pop, lv, ar, zn }
let synthBuildingPopMap = null;      // Map<featIdx, residents>
let _synMappedCity = null;
let _synLoadCity = null;
let _synLoadPromise = null;

const _SYN_SNAP_TOL_M = 45;          // match routing/2SFCA snap (CLAUDE.md): baked
                                     // footprints are a ~19k OSM-derived subset of
                                     // the ~50k rendered byggnad, so non-baked
                                     // footprints borrow the nearest baked record.
                                     // 12 m left ~19% of vaxjo unmatched; 45 m → ~90%.
const _SYN_CELL = 0.006;             // ~0.6 km grid cells (matches access2sfca)

function _synCurrentCity() {
  return (document.getElementById('citySelect')?.value) || 'vaxjo';
}
function _synKey(lon, lat) { return `${(+lon).toFixed(6)},${(+lat).toFixed(6)}`; }
function _synHav(aLon, aLat, bLon, bLat) {
  const R = 6371000, toR = Math.PI / 180;
  const dLat = (bLat - aLat) * toR, dLon = (bLon - aLon) * toR;
  const s = Math.sin(dLat / 2) ** 2 +
    Math.cos(aLat * toR) * Math.cos(bLat * toR) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s)));
}

function _synBuildSnapIndex(keyList) {
  const coords = new Array(keyList.length);
  const keyToRow = new Map();
  const grid = new Map();
  for (let i = 0; i < keyList.length; i++) {
    const parts = keyList[i].split(',');
    const lon = +parts[0], lat = +parts[1];
    coords[i] = [lon, lat];
    keyToRow.set(keyList[i], i);
    const gk = `${Math.floor(lon / _SYN_CELL)}_${Math.floor(lat / _SYN_CELL)}`;
    let bucket = grid.get(gk); if (!bucket) { bucket = []; grid.set(gk, bucket); }
    bucket.push(i);
  }
  return { coords, keyToRow, grid };
}

// featIdx-free row lookup for an arbitrary point: exact key, then grid-nearest.
function _synRowForPoint(lon, lat) {
  if (!SYNTHPOP) return -1;
  const exact = SYNTHPOP.keyToRow.get(_synKey(lon, lat));
  if (exact != null) return exact;
  const cx = Math.floor(lon / _SYN_CELL), cy = Math.floor(lat / _SYN_CELL);
  let best = -1, bestD = Infinity;
  for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
    const bucket = SYNTHPOP.grid.get(`${cx + dx}_${cy + dy}`);
    if (!bucket) continue;
    for (const i of bucket) {
      const c = SYNTHPOP.coords[i];
      const d = _synHav(lon, lat, c[0], c[1]);
      if (d < bestD) { bestD = d; best = i; }
    }
  }
  return (best >= 0 && bestD <= _SYN_SNAP_TOL_M) ? best : -1;
}

async function loadSynthpop(cityKey) {
  if (SYNTHPOP && SYNTHPOP.city === cityKey) return SYNTHPOP;
  if (_synLoadCity === cityKey && _synLoadPromise) return _synLoadPromise;
  _synLoadCity = cityKey;
  _synLoadPromise = (async () => {
    try {
      const res = await fetch(`assets/data/cities/${cityKey}/synthpop/buildings.json`, { cache: 'force-cache' });
      if (!res.ok) { SYNTHPOP = null; return null; }
      const d = await res.json();
      const snap = _synBuildSnapIndex(d.k || []);
      SYNTHPOP = { city: cityKey, meta: d.meta || null, ...snap,
                   pop: d.pop || [], lv: d.lv || [], ar: d.ar || [], zn: d.zn || [],
                   inc: d.inc || [], nz: d.nz || [], de: d.de || [] };
      return SYNTHPOP;
    } catch (e) {
      console.warn('[synthpop] load failed for', cityKey, e);
      SYNTHPOP = null; return null;
    }
  })();
  return _synLoadPromise;
}

// Build the per-building demand map + stamp synthetic props onto features.
function buildSynthpopMaps() {
  synthBuildingPopMap = new Map();
  if (!SYNTHPOP || !(typeof baseCityFC !== 'undefined' && baseCityFC?.features)) return;
  let stamped = 0;
  baseCityFC.features.forEach((feat, idx) => {
    // Accessory structures (garages/sheds) aren't dwellings — don't give them
    // synthetic residents/demand or they'd double-count a neighbour's people.
    if (typeof isAccessoryBuilding === 'function' && isAccessoryBuilding(feat.properties)) return;
    let c; try { c = turf.centroid(feat).geometry.coordinates; } catch { return; }
    if (!c) return;
    const row = _synRowForPoint(c[0], c[1]);
    if (row < 0) return;
    const pop = SYNTHPOP.pop[row];
    const props = feat.properties || (feat.properties = {});
    if (Number.isFinite(pop) && pop > 0) { synthBuildingPopMap.set(idx, pop); props.__synthPop = pop; }
    if (SYNTHPOP.lv[row] != null) props.__synthLevels = SYNTHPOP.lv[row];
    if (SYNTHPOP.ar[row] != null) props.__synthArea = SYNTHPOP.ar[row];
    if (SYNTHPOP.zn[row] != null) props.__synthZone = SYNTHPOP.zn[row];
    if (SYNTHPOP.inc[row] != null) props.__synthIncome = SYNTHPOP.inc[row];
    if (SYNTHPOP.nz[row] != null) props.__synthNeed = SYNTHPOP.nz[row];
    if (SYNTHPOP.de[row] != null) props.__synthDeso = SYNTHPOP.de[row];
    stamped++;
  });
  console.log('[synthpop] mapped —', synthBuildingPopMap.size, 'buildings with synthetic pop;',
    stamped, 'footprints matched of', baseCityFC.features.length, 'rendered.');
}

// Ensure the active city's synthetic data is loaded AND mapped to buildings.
async function ensureSynthpop(cityKey) {
  const key = cityKey || _synCurrentCity();
  await loadSynthpop(key);
  if (SYNTHPOP && (!synthBuildingPopMap || SYNTHPOP.city !== _synMappedCity)) {
    buildSynthpopMaps();
    _synMappedCity = SYNTHPOP.city;
  } else if (!SYNTHPOP) {
    synthBuildingPopMap = null;
  }
  return SYNTHPOP;
}

function resetSynthpopMaps() { synthBuildingPopMap = null; _synMappedCity = null; }

// ---- Read helpers for views (inspector / parallel-coords) ----
// Per-building synthetic record. Prefers stamped props (O(1)); else snaps.
function synthpopForFeature(feat) {
  const p = feat?.properties || {};
  // Accessory structures (garages/sheds) are not dwellings — never return a
  // (borrowed) synthetic record for them, so a clicked garage reads "No
  // synthetic residents" instead of a neighbour's people.
  if (typeof isAccessoryBuilding === 'function' && isAccessoryBuilding(p)) return null;
  if (p.__synthPop != null || p.__synthLevels != null || p.__synthArea != null) {
    return { pop: p.__synthPop ?? null, levels: p.__synthLevels ?? null,
             area: p.__synthArea ?? null, zone: p.__synthZone ?? null,
             income: p.__synthIncome ?? null, need: p.__synthNeed ?? null, deso: p.__synthDeso ?? null };
  }
  if (!SYNTHPOP) return null;
  let c; try { c = turf.centroid(feat).geometry.coordinates; } catch { return null; }
  const row = c ? _synRowForPoint(c[0], c[1]) : -1;
  if (row < 0) return null;
  return { pop: SYNTHPOP.pop[row] ?? null, levels: SYNTHPOP.lv[row] ?? null,
           area: SYNTHPOP.ar[row] ?? null, zone: SYNTHPOP.zn[row] ?? null,
           income: SYNTHPOP.inc[row] ?? null, need: SYNTHPOP.nz[row] ?? null, deso: SYNTHPOP.de[row] ?? null };
}

// Aggregate synthetic pop over a set of building features (meso/macro scope):
// total residents + count of footprints carrying any synthetic residents.
function synthpopAggregateForFeatures(feats) {
  if (!Array.isArray(feats) || !feats.length) return null;
  let pop = 0, n = 0;
  for (const f of feats) {
    const r = synthpopForFeature(f);
    if (r && Number.isFinite(r.pop) && r.pop > 0) { pop += r.pop; n++; }
  }
  return { pop, n };
}

// Fields inherited per-building from the building's DESO (no synthetic version —
// SCB resolution). Resolved through the shared epiDesoProps() lookup so every
// view uses the SAME source.
const SYN_DESO_FIELDS = ['child_frac', 'elder_frac', 'dependency', 'higher_ed', 'neet', 'income_support', 'male_frac'];

// Unified per-building demographic record: synthetic (pop/income/need/levels/
// area/zone) + DESO-inherited fractions. THE single source for the inspector and
// parallel-coords building rows.
function synthDemographicsForFeature(feat) {
  const s = synthpopForFeature(feat) || {};
  const code = s.deso || feat?.properties?.__deso || null;
  const out = { pop: s.pop ?? null, income: s.income ?? null, need: s.need ?? null,
                levels: s.levels ?? null, area: s.area ?? null, zone: s.zone ?? null, deso: code };
  const dp = (typeof epiDesoProps === 'function') ? epiDesoProps(code) : null;
  for (const k of SYN_DESO_FIELDS) out[k] = (dp && Number.isFinite(dp[k])) ? dp[k] : null;
  return out;
}

// ---- Meso/macro aggregation over the RENDERED buildings ----
// Cache (centroid + synthetic fields + DESO code) once per city so per-selection
// polygon/hex aggregates don't recompute turf.centroid over ~45k features
// (mirrors access2sfca's coords-driven aggregation; run in the gated inspector
// flow). Aggregates are population-weighted so they match the building source.
let _SYN_CENTROIDS = null;
function _synEnsureCentroids() {
  if (_SYN_CENTROIDS && _SYN_CENTROIDS.city === _synMappedCity) return _SYN_CENTROIDS;
  if (!(typeof baseCityFC !== 'undefined' && baseCityFC?.features)) return null;
  const pts = [], pop = [], inc = [], need = [], deso = [];
  for (const f of baseCityFC.features) {
    const pr = f.properties || {};
    const p = pr.__synthPop;
    if (!(Number.isFinite(p) && p > 0)) continue;          // only residents-bearing footprints
    let c; try { c = turf.centroid(f).geometry.coordinates; } catch { continue; }
    if (!c) continue;
    pts.push(c); pop.push(p);
    inc.push(Number.isFinite(pr.__synthIncome) ? pr.__synthIncome : null);
    need.push(Number.isFinite(pr.__synthNeed) ? pr.__synthNeed : null);
    deso.push(pr.__synthDeso || pr.__deso || null);
  }
  _SYN_CENTROIDS = { city: _synMappedCity, pts, pop, inc, need, deso };
  return _SYN_CENTROIDS;
}

// Population-weighted aggregate over member building indices: residents (sum),
// income/need (pop-wt mean), and the DESO fractions (pop-wt mean via the shared
// DESO lookup). Returns the same shape as synthDemographicsForFeature + {n}.
function _synAggregate(members, cc) {
  let pop = 0, n = 0, incW = 0, incP = 0, ndW = 0, ndP = 0;
  const fW = {}, fP = {};
  for (const k of SYN_DESO_FIELDS) { fW[k] = 0; fP[k] = 0; }
  for (const i of members) {
    const w = cc.pop[i];
    pop += w; n++;
    if (Number.isFinite(cc.inc[i])) { incW += cc.inc[i] * w; incP += w; }
    if (Number.isFinite(cc.need[i])) { ndW += cc.need[i] * w; ndP += w; }
    const dp = (typeof epiDesoProps === 'function') ? epiDesoProps(cc.deso[i]) : null;
    if (dp) for (const k of SYN_DESO_FIELDS) {
      if (Number.isFinite(dp[k])) { fW[k] += dp[k] * w; fP[k] += w; }
    }
  }
  const out = { pop, n, income: incP > 0 ? incW / incP : null, need: ndP > 0 ? ndW / ndP : null };
  for (const k of SYN_DESO_FIELDS) out[k] = fP[k] > 0 ? fW[k] / fP[k] : null;
  return out;
}

function synthpopAggregateForPolygon(feature) {
  const cc = _synEnsureCentroids();
  if (!cc || !feature) return null;
  let bbox; try { bbox = turf.bbox(feature); } catch { return null; }
  const [minX, minY, maxX, maxY] = bbox;
  const members = [];
  for (let i = 0; i < cc.pts.length; i++) {
    const c = cc.pts[i];
    if (c[0] < minX || c[0] > maxX || c[1] < minY || c[1] > maxY) continue;
    try { if (turf.booleanPointInPolygon(c, feature)) members.push(i); } catch { /* skip */ }
  }
  return _synAggregate(members, cc);
}

function synthpopAggregateForHex(hexId) {
  const cc = _synEnsureCentroids();
  if (!cc || !hexId || typeof h3 === 'undefined') return null;
  const res = (typeof h3.h3GetResolution === 'function') ? h3.h3GetResolution(hexId) : null;
  if (res == null) return null;
  const toCell = h3.geoToH3 || h3.latLngToCell;
  if (typeof toCell !== 'function') return null;
  const members = [];
  for (let i = 0; i < cc.pts.length; i++) {
    const c = cc.pts[i];
    if (toCell(c[1], c[0], res) === hexId) members.push(i);
  }
  return _synAggregate(members, cc);
}

// All hexes at a resolution in ONE pass → Map<hexId, aggregate>. Used by the
// parallel-coords meso view so it doesn't re-scan every building per hex.
function synthpopAggregateAllHexes(res) {
  const cc = _synEnsureCentroids();
  if (!cc || res == null || typeof h3 === 'undefined') return null;
  const toCell = h3.geoToH3 || h3.latLngToCell;
  if (typeof toCell !== 'function') return null;
  const groups = new Map();
  for (let i = 0; i < cc.pts.length; i++) {
    const c = cc.pts[i];
    const id = toCell(c[1], c[0], res);
    let g = groups.get(id); if (!g) { g = []; groups.set(id, g); }
    g.push(i);
  }
  const out = new Map();
  for (const [id, members] of groups) out.set(id, _synAggregate(members, cc));
  return out;
}
