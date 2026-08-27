// modeGain.js — per-building "what does switching travel mode change" fields
// for the multivariate views (DR/PCP/contrastive/EBM).
//
// Motivation (TVCG revision): mode comparisons used to live only in maps + the
// summary index. These fields carry the comparison INTO the multivariate views:
// per category, gain = access score under the TO mode minus access score under
// the FROM mode, where BOTH scores use the same gravity model as the live
// fairness compute and BOTH are normalised with the FROM-mode run's anchors
// (log1p, p10..p95, clamped 0..1 — mirroring normalizeBenefitsToScores).
// So a gain reads as "how far up the FROM-mode access scale the TO mode lifts
// this building": ~0 where the FROM mode already saturates (served) AND where
// even the TO mode can't reach (needs facilities, not mobility) — large where
// the mode switch actually rescues access. That non-monotone shape is the
// signal; a raw log-benefit ratio would just re-rank by distance.
//
// The pair is USER-SELECTABLE across all four travel modes (From/To selects in
// the Custom picker's "Mode gain" group; default walking→cycling):
//   walking / cycling / driving — baked routing matrices (network metres),
//     the same source the fairness network branch reads.
//   transit — the baked stop network (access walk + wait + in-vehicle + egress,
//     converted to effective km at the reference walking speed), the same model
//     the live transit fairness uses. Transit benefits are computed at the
//     companion matrix mode's building keys, so both sides of a pair align on
//     the same points. Cities without a baked transit network skip the layer.
//
// Data loads into this module's own store, independent of the fairness ROUTING
// global (which holds only the current travel mode). No runtime API calls —
// everything ships in assets/data. What-if edits are NOT reflected (static
// matrices / baseline POI set), same as the dist* fields.
//
// Uses globals from routingMatrix.js (_fetchCat, _buildIndex, _rowInIndex,
// _sigOfKeys), transit.js (ensureTransitNetwork, transitReady,
// transitNearestStops, transitEffectiveKm) and fairness.js (ifCityKappa,
// ifCityPriorityWeight, ifCityNetworkDistanceForMode, ifCityDistanceForMode,
// fetchPOIs, ifCityOpportunityWeightFromPOI) — all resolved lazily at call
// time, so load order only requires routingMatrix.js before this file.

const MGAIN_ALL_MODES = ['walking', 'cycling', 'driving', 'transit'];
const MGAIN_MATRIX_MODES = ['walking', 'cycling', 'driving'];
const MGAIN_KEY_BY_CAT = {
  grocery: 'mgainGrocery', hospital: 'mgainHospital', healthcare_center: 'mgainHealthcare',
  pharmacy: 'mgainPharmacy', veterinary: 'mgainVeterinary', university: 'mgainUniversity',
  school_high: 'mgainSchoolHigh', school_primary: 'mgainPrimary',
  kindergarten: 'mgainKindergarten', dentistry: 'mgainDentistry'
};
// One-letter mode codes → the per-pair column-key suffix (mgainGrocery_w2d).
const MGAIN_MODE_CODE = { walking: 'w', cycling: 'c', driving: 'd', transit: 't' };

// SEVERAL pairs can be active at once (e.g. walking→cycling AND walking→driving
// AND walking→transit), so the DR matrix can carry more than one mode comparison
// side by side. Every pair contributes its own 11 columns, suffixed by the pair
// code; nothing is shared between pairs except the baked matrices underneath.
// Changed via setModeGainPairs (Custom-picker controls); every change bumps
// MGAIN_EPOCH so per-feature caches invalidate.
let MGAIN_PAIRS = [{ from: 'walking', to: 'cycling' }];
let MGAIN_EPOCH = 1;

let MGAIN = null;              // { sig, city, any, pairs: [{from,to,key,cats}] }
let _mgainPromise = null;      // { sig, p } in-flight guard
// Per-pair results and per-(mode,cat) benefit sides, both keyed by city. Pairs
// that share a baseline (the usual walking→X fan-out) reuse the same from-side
// instead of recomputing it, so activating a third pair is much cheaper than the
// first. Dropped wholesale on a city change (see _mgainResetCachesIfCity).
const _mgainPairCache = new Map();   // `${city}|${from}>${to}` -> { any, cats }
const _mgainSideCache = new Map();   // `${city}|${mode}|${cat}` -> { keys, benefits }
let _mgainCacheCity = null;

function modeGainReady() { return !!(MGAIN && MGAIN.any); }
function modeGainEpoch() { return MGAIN_EPOCH; }
function modeGainPairKey(pair) {
  return `${MGAIN_MODE_CODE[pair?.from] || '?'}2${MGAIN_MODE_CODE[pair?.to] || '?'}`;
}
function modeGainPairLabelOf(pair) { return `${pair?.from}→${pair?.to}`; }
function modeGainPairs() { return MGAIN_PAIRS.map(p => ({ from: p.from, to: p.to })); }
function modeGainPairsSig() { return MGAIN_PAIRS.map(p => `${p.from}>${p.to}`).join(','); }
// Compat shims for callers that still think in terms of a single pair: they get
// the FIRST active pair (the historical walking→cycling default).
function modeGainPair() { return { from: MGAIN_PAIRS[0].from, to: MGAIN_PAIRS[0].to }; }
function modeGainPairLabel() { return modeGainPairLabelOf(MGAIN_PAIRS[0]); }
// The per-pair column key for a base key ('mgainGrocery' / 'mgainOverall').
function modeGainMetricKey(baseKey, pair) { return `${baseKey}_${modeGainPairKey(pair)}`; }
// The overall-gain column of every active pair — the "did any pair resolve here"
// probe used where the code used to test the single `mgainOverall`.
function modeGainOverallKeys() {
  return MGAIN_PAIRS.map(p => modeGainMetricKey('mgainOverall', p));
}

// Mutated IN PLACE (same binding) so drView's metrics spread and the meso
// aggregation always see the live key list — the DR_SCB_FEATURES pattern.
const MGAIN_METRIC_KEYS = [];
function _mgainRebuildKeys() {
  MGAIN_METRIC_KEYS.length = 0;
  for (const p of MGAIN_PAIRS) {
    for (const base of Object.values(MGAIN_KEY_BY_CAT)) {
      MGAIN_METRIC_KEYS.push(modeGainMetricKey(base, p));
    }
    MGAIN_METRIC_KEYS.push(modeGainMetricKey('mgainOverall', p));
  }
}
_mgainRebuildKeys();

// Set the ACTIVE pair list. Returns true when it changed (caller reloads /
// refreshes labels). Invalid and same-mode pairs are dropped — their gain would
// be all-zero — and duplicates collapse; an empty result is rejected outright.
function setModeGainPairs(pairs) {
  const seen = new Set();
  const next = [];
  for (const p of (Array.isArray(pairs) ? pairs : [])) {
    const f = String(p?.from || '').toLowerCase();
    const t = String(p?.to || '').toLowerCase();
    if (!MGAIN_ALL_MODES.includes(f) || !MGAIN_ALL_MODES.includes(t) || f === t) continue;
    const sig = `${f}>${t}`;
    if (seen.has(sig)) continue;
    seen.add(sig);
    next.push({ from: f, to: t });
  }
  if (!next.length) return false;
  if (next.map(p => `${p.from}>${p.to}`).join(',') === modeGainPairsSig()) return false;
  MGAIN_PAIRS = next;
  MGAIN_EPOCH++;
  MGAIN = null;                // force a rebuild for the new pair set
  _mgainPromise = null;
  _mgainRebuildKeys();
  return true;
}
// Single-pair compat wrapper.
function setModeGainPair(from, to) { return setModeGainPairs([{ from, to }]); }

function _mgainResetCachesIfCity(city) {
  if (_mgainCacheCity === city) return;
  _mgainPairCache.clear();
  _mgainSideCache.clear();
  _mgainCacheCity = city;
}

// Gravity benefit per matrix row — the SAME formula as the fairness network
// branch (ifCityAccessibilityForBuilding): nearest ≤3 POIs, ρ·v·exp(−κ·effkm).
function _mgainBenefits(j, cat, mode) {
  const kappa = ifCityKappa(cat);
  const rho = ifCityPriorityWeight(cat);
  const n = j.m.length;
  const out = new Float64Array(n);
  for (let r = 0; r < n; r++) {
    const dists = j.m[r];
    const vals = j.v ? j.v[r] : null;
    let sum = 0;
    const k = Math.min(3, dists.length);
    for (let i = 0; i < k; i++) {
      const dm = dists[i];
      if (!Number.isFinite(dm)) continue;
      const v = (vals && Number.isFinite(vals[i])) ? vals[i] : 1;
      sum += rho * v * Math.exp(-kappa * ifCityNetworkDistanceForMode(dm / 1000, mode));
    }
    out[r] = sum;
  }
  return out;
}

// Transit benefit at each key point of a companion matrix (so both sides of the
// pair share the same building keys). Mirrors the fairness transit branch:
// nearest ≤3 POIs by straight line as candidates, each priced via the stop
// network (transitEffectiveKm) with the per-mode speed model as fallback.
function _mgainTransitBenefits(keyList, originStopsByRow, pois, cat) {
  const kappa = ifCityKappa(cat);
  const rho = ifCityPriorityWeight(cat);
  const n = keyList.length;
  const out = new Float64Array(n);
  if (!pois.length) return out;
  for (let r = 0; r < n; r++) {
    const k = keyList[r];
    const comma = k.indexOf(',');
    const lon = +k.slice(0, comma), lat = +k.slice(comma + 1);
    // nearest-3 candidate POIs by straight line (same candidate rule as fairness)
    let d0 = Infinity, d1 = Infinity, d2 = Infinity;
    let p0 = null, p1 = null, p2 = null;
    for (let i = 0; i < pois.length; i++) {
      const poi = pois[i];
      const dist = _rhav(lon, lat, poi.c[0], poi.c[1]);
      if (dist < d0)      { d2 = d1; p2 = p1; d1 = d0; p1 = p0; d0 = dist; p0 = poi; }
      else if (dist < d1) { d2 = d1; p2 = p1; d1 = dist; p1 = poi; }
      else if (dist < d2) { d2 = dist; p2 = poi; }
    }
    const originStops = originStopsByRow[r];
    let sum = 0;
    const add = (poi, distM) => {
      if (!poi) return;
      let effKm = null;
      if (originStops && originStops.length) {
        effKm = transitEffectiveKm(originStops, poi);
      }
      if (effKm == null) effKm = ifCityDistanceForMode(distM / 1000, 'transit');
      const v = Number.isFinite(poi.v) ? poi.v : 1;
      sum += rho * v * Math.exp(-kappa * effKm);
    };
    add(p0, d0); add(p1, d1); add(p2, d2);
    out[r] = sum;
  }
  return out;
}

// FROM-mode anchors (log1p p10/p95 over positive benefits) → 0..1 scores for
// BOTH modes, so the to−from difference is in FROM-mode score units.
function _mgainAnchors(benefits) {
  const vals = [];
  for (let i = 0; i < benefits.length; i++) if (benefits[i] > 0) vals.push(Math.log1p(benefits[i]));
  if (!vals.length) return null;
  vals.sort((a, b) => a - b);
  const q = (p) => {
    if (vals.length === 1) return vals[0];
    const x = (vals.length - 1) * p, lo = Math.floor(x), hi = Math.ceil(x);
    return lo === hi ? vals[lo] : vals[lo] * (1 - (x - lo)) + vals[hi] * (x - lo);
  };
  const floor = q(0.10);
  return { floor, span: Math.max(1e-9, q(0.95) - floor) };
}
function _mgainScores(benefits, anchors) {
  const out = new Float32Array(benefits.length);
  for (let i = 0; i < benefits.length; i++) {
    const b = benefits[i];
    if (!(b > 0)) { out[i] = 0; continue; }
    const s = (Math.log1p(b) - anchors.floor) / anchors.span;
    out[i] = s < 0 ? 0 : (s > 1 ? 1 : s);
  }
  return out;
}

// Baseline POI arrays per category for the transit side (same source + shape as
// the fairness compute: {c, v}; what-if edits intentionally excluded — the
// matrices on the other side of the pair are baseline too).
async function _mgainPoisByCat(cats) {
  const out = {};
  if (typeof fetchPOIs !== 'function' || typeof baseCityFC === 'undefined' || !baseCityFC) return out;
  await Promise.all(cats.map(async (cat) => {
    try {
      const fc = await fetchPOIs(cat, baseCityFC);
      out[cat] = (fc?.features || []).map(p => ({
        c: p.geometry.coordinates,
        v: (typeof ifCityOpportunityWeightFromPOI === 'function')
          ? ifCityOpportunityWeightFromPOI(cat, p) : 1
      }));
    } catch { out[cat] = []; }
  }));
  return out;
}

// Shared index store: two pairs that reference the same baked key order reuse
// one index instead of rebuilding it (walking→cycling and walking→driving both
// resolve their walking side against the same keys).
const _mgainIndexBySig = new Map();
function _mgainIdxFor(keyList) {
  const s = _sigOfKeys(keyList);
  let idx = _mgainIndexBySig.get(s);
  if (!idx) { idx = _buildIndex(keyList); _mgainIndexBySig.set(s, idx); }
  return idx;
}

// One matrix-mode side (keys + raw gravity benefits) for a category, memoised
// per (city, mode, cat). This is what makes a walking→X fan-out cheap: the
// walking side is computed once and reused by every pair that baselines on it.
async function _mgainMatrixSide(city, mode, cat) {
  const ck = `${city}|${mode}|${cat}`;
  if (_mgainSideCache.has(ck)) return _mgainSideCache.get(ck);
  const j = await _fetchCat(city, mode, cat);
  const side = (j && j.key) ? { keys: j.key, benefits: _mgainBenefits(j, cat, mode) } : null;
  _mgainSideCache.set(ck, side);
  return side;
}

// Load ONE pair's data for all categories and precompute per-row scores.
// Idempotent per (city, pair); raw matrices are NOT retained (only index +
// scores), so the store stays a few MB. Categories missing either side yield null.
async function _mgainLoadPair(city, pair) {
  const ck = `${city}|${pair.from}>${pair.to}`;
  if (_mgainPairCache.has(ck)) return _mgainPairCache.get(ck);
  const cats = Object.keys(MGAIN_KEY_BY_CAT);
  const usesTransit = pair.from === 'transit' || pair.to === 'transit';

  // Transit prerequisites: the stop network + baseline POIs. If the network
  // isn't baked for this city, the pair simply doesn't resolve (any=false).
  let poisByCat = null;
  if (usesTransit) {
    try { if (typeof ensureTransitNetwork === 'function') await ensureTransitNetwork(city); } catch {}
    if (typeof transitReady !== 'function' || !transitReady()) {
      const empty = { any: false, cats: {} };
      _mgainPairCache.set(ck, empty);
      return empty;
    }
    poisByCat = await _mgainPoisByCat(cats);
  }

  const matrixModes = [pair.from, pair.to].filter(m => MGAIN_MATRIX_MODES.includes(m));
  const jobs = [];
  for (const cat of cats) for (const mode of matrixModes) {
    jobs.push(_mgainMatrixSide(city, mode, cat).then(side => ({ cat, mode, side })));
  }
  const loaded = await Promise.all(jobs);
  const byCat = {};
  for (const { cat, mode, side } of loaded) {
    (byCat[cat] = byCat[cat] || {})[mode] = side;
  }

  // Per-keylist transit origin stops, computed once and shared across
  // categories that use the same key order (they usually all do).
  const originStopsBySig = new Map();
  const originStopsFor = (keyList) => {
    const s = _sigOfKeys(keyList);
    let stops = originStopsBySig.get(s);
    if (!stops) {
      stops = new Array(keyList.length);
      for (let r = 0; r < keyList.length; r++) {
        const k = keyList[r];
        const comma = k.indexOf(',');
        stops[r] = transitNearestStops([+k.slice(0, comma), +k.slice(comma + 1)]);
      }
      originStopsBySig.set(s, stops);
    }
    return stops;
  };

  const store = { any: false, cats: {} };
  for (const cat of cats) {
    // The companion matrix supplies the key list a transit side aligns to.
    const companion = matrixModes.map(m => byCat[cat]?.[m]).find(s => s && s.keys) || null;
    const sideFor = (mode) => {
      if (MGAIN_MATRIX_MODES.includes(mode)) return byCat[cat]?.[mode] || null;
      // transit side
      if (!companion) return null;
      return {
        keys: companion.keys,
        benefits: _mgainTransitBenefits(companion.keys, originStopsFor(companion.keys), poisByCat?.[cat] || [], cat)
      };
    };
    const fromSide = sideFor(pair.from);
    const toSide = fromSide ? sideFor(pair.to) : null;
    if (!fromSide || !toSide) { store.cats[cat] = null; continue; }
    const anchors = _mgainAnchors(fromSide.benefits);
    if (!anchors) { store.cats[cat] = null; continue; }
    store.cats[cat] = {
      from: { idx: _mgainIdxFor(fromSide.keys), score: _mgainScores(fromSide.benefits, anchors) },
      to:   { idx: _mgainIdxFor(toSide.keys),   score: _mgainScores(toSide.benefits, anchors) }
    };
    store.any = true;
  }
  _mgainPairCache.set(ck, store);
  return store;
}

// Load EVERY active pair. Pairs load concurrently but share the side cache, so a
// walking→{cycling,driving,transit} fan-out reads each mode's matrices once.
// A pair that can't resolve (e.g. transit in a city with no baked network) is
// kept with any=false: its columns come back null rather than disappearing.
async function ensureModeGain(cityKey) {
  const city = cityKey || (typeof routingCurrentCity === 'function' ? routingCurrentCity() : 'vaxjo');
  const pairs = modeGainPairs();
  const sig = `${city}|${modeGainPairsSig()}`;
  if (MGAIN && MGAIN.sig === sig) return MGAIN;
  if (_mgainPromise && _mgainPromise.sig === sig) return _mgainPromise.p;
  const p = (async () => {
    if (typeof ifCityKappa !== 'function' || typeof ifCityNetworkDistanceForMode !== 'function'
        || typeof _fetchCat !== 'function' || typeof _buildIndex !== 'function') return null;
    _mgainResetCachesIfCity(city);
    const loaded = await Promise.all(pairs.map(pair =>
      _mgainLoadPair(city, pair).then(store => ({ pair, store }))
    ));
    const store = {
      sig, city, any: false,
      pairs: loaded.map(({ pair, store: s }) => ({
        from: pair.from, to: pair.to, key: modeGainPairKey(pair), cats: s.cats, any: s.any
      }))
    };
    store.any = store.pairs.some(p2 => p2.any);
    MGAIN = store;
    return store;
  })();
  _mgainPromise = { sig, p };
  try { return await p; } finally { if (_mgainPromise && _mgainPromise.p === p) _mgainPromise = null; }
}

// Which of the active pairs actually resolved for this city — used by the picker
// to tell the user a pair (usually transit) has no data here.
function modeGainUnresolvedPairs() {
  if (!MGAIN || !Array.isArray(MGAIN.pairs)) return [];
  return MGAIN.pairs.filter(p => !p.any).map(p => `${p.from}→${p.to}`);
}

// Per-point gains: each side's row is resolved against its OWN index (bakes can
// differ in key order/length). Overall = mean of the finite per-cat gains.
// Null where either side has no baked row for the point.
function modeGainForPoint(lon, lat) {
  if (!modeGainReady()) return null;
  const out = {};
  for (const P of MGAIN.pairs) {
    let sum = 0, n = 0;
    for (const cat of Object.keys(MGAIN_KEY_BY_CAT)) {
      const key = `${MGAIN_KEY_BY_CAT[cat]}_${P.key}`;
      const cd = P.cats[cat];
      let g = null;
      if (cd) {
        const rf = _rowInIndex(cd.from.idx, lon, lat);
        const rt = _rowInIndex(cd.to.idx, lon, lat);
        if (rf >= 0 && rt >= 0) g = cd.to.score[rt] - cd.from.score[rf];
      }
      out[key] = Number.isFinite(g) ? g : null;
      if (Number.isFinite(g)) { sum += g; n++; }
    }
    out[`mgainOverall_${P.key}`] = n ? sum / n : null;
  }
  return out;
}
