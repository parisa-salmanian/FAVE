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
const MGAIN_METRIC_KEYS = [...Object.values(MGAIN_KEY_BY_CAT), 'mgainOverall'];

// The comparison pair. Changed via setModeGainPair (Custom-picker From/To
// selects); every change bumps MGAIN_EPOCH so per-feature caches invalidate.
let MGAIN_PAIR = { from: 'walking', to: 'cycling' };
let MGAIN_EPOCH = 1;

let MGAIN = null;              // { city, pairSig, any, cats: { cat: { from:{idx,score}, to:{idx,score} } } }
let _mgainPromise = null;      // { sig, p } in-flight guard

function modeGainReady() { return !!(MGAIN && MGAIN.any); }
function modeGainEpoch() { return MGAIN_EPOCH; }
function modeGainPair() { return { from: MGAIN_PAIR.from, to: MGAIN_PAIR.to }; }
function modeGainPairLabel() { return `${MGAIN_PAIR.from}→${MGAIN_PAIR.to}`; }

// Set the comparison pair. Returns true when it changed (caller reloads /
// refreshes labels). Same-mode pairs are rejected — the gain would be all-zero.
function setModeGainPair(from, to) {
  const f = String(from || '').toLowerCase(), t = String(to || '').toLowerCase();
  if (!MGAIN_ALL_MODES.includes(f) || !MGAIN_ALL_MODES.includes(t) || f === t) return false;
  if (MGAIN_PAIR.from === f && MGAIN_PAIR.to === t) return false;
  MGAIN_PAIR = { from: f, to: t };
  MGAIN_EPOCH++;
  MGAIN = null;                // force a reload for the new pair
  _mgainPromise = null;
  return true;
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

// Load both modes' data for all categories and precompute per-row scores.
// Idempotent per (city, pair); raw matrices are NOT retained (only index +
// scores), so the store stays a few MB. Categories missing either side yield null.
async function ensureModeGain(cityKey) {
  const city = cityKey || (typeof routingCurrentCity === 'function' ? routingCurrentCity() : 'vaxjo');
  const pair = { ...MGAIN_PAIR };
  const sig = `${city}|${pair.from}>${pair.to}`;
  if (MGAIN && MGAIN.sig === sig) return MGAIN;
  if (_mgainPromise && _mgainPromise.sig === sig) return _mgainPromise.p;
  const p = (async () => {
    if (typeof ifCityKappa !== 'function' || typeof ifCityNetworkDistanceForMode !== 'function'
        || typeof _fetchCat !== 'function' || typeof _buildIndex !== 'function') return null;
    const cats = Object.keys(MGAIN_KEY_BY_CAT);
    const usesTransit = pair.from === 'transit' || pair.to === 'transit';

    // Transit prerequisites: the stop network + baseline POIs. If the network
    // isn't baked for this city, the layer simply doesn't resolve (any=false).
    let poisByCat = null;
    if (usesTransit) {
      try { if (typeof ensureTransitNetwork === 'function') await ensureTransitNetwork(city); } catch {}
      if (typeof transitReady !== 'function' || !transitReady()) { MGAIN = { sig, city, any: false, cats: {} }; return MGAIN; }
      poisByCat = await _mgainPoisByCat(cats);
    }

    const matrixModes = [pair.from, pair.to].filter(m => MGAIN_MATRIX_MODES.includes(m));
    const jobs = [];
    for (const cat of cats) for (const mode of matrixModes) {
      jobs.push(_fetchCat(city, mode, cat).then(j => ({ cat, mode, j })));
    }
    const loaded = await Promise.all(jobs);
    const byCat = {};
    for (const { cat, mode, j } of loaded) {
      (byCat[cat] = byCat[cat] || {})[mode] = j;
    }

    const store = { sig, city, any: false, cats: {}, indexBySig: new Map() };
    const idxFor = (keyList) => {
      const s = _sigOfKeys(keyList);
      let idx = store.indexBySig.get(s);
      if (!idx) { idx = _buildIndex(keyList); store.indexBySig.set(s, idx); }
      return idx;
    };
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

    for (const cat of cats) {
      // The companion matrix supplies the key list a transit side aligns to.
      const companionJ = matrixModes.map(m => byCat[cat]?.[m]).find(j => j && j.key) || null;
      const sideFor = (mode) => {
        if (MGAIN_MATRIX_MODES.includes(mode)) {
          const j = byCat[cat]?.[mode];
          if (!j || !j.key) return null;
          return { keys: j.key, benefits: _mgainBenefits(j, cat, mode) };
        }
        // transit side
        if (!companionJ) return null;
        return {
          keys: companionJ.key,
          benefits: _mgainTransitBenefits(companionJ.key, originStopsFor(companionJ.key), poisByCat?.[cat] || [], cat)
        };
      };
      const fromSide = sideFor(pair.from);
      const toSide = fromSide ? sideFor(pair.to) : null;
      if (!fromSide || !toSide) { store.cats[cat] = null; continue; }
      const anchors = _mgainAnchors(fromSide.benefits);
      if (!anchors) { store.cats[cat] = null; continue; }
      store.cats[cat] = {
        from: { idx: idxFor(fromSide.keys), score: _mgainScores(fromSide.benefits, anchors) },
        to:   { idx: idxFor(toSide.keys),   score: _mgainScores(toSide.benefits, anchors) }
      };
      store.any = true;
    }
    MGAIN = store;
    return store;
  })();
  _mgainPromise = { sig, p };
  try { return await p; } finally { if (_mgainPromise && _mgainPromise.p === p) _mgainPromise = null; }
}

// Per-point gains: each side's row is resolved against its OWN index (bakes can
// differ in key order/length). Overall = mean of the finite per-cat gains.
// Null where either side has no baked row for the point.
function modeGainForPoint(lon, lat) {
  if (!modeGainReady()) return null;
  const out = {};
  let sum = 0, n = 0;
  for (const cat of Object.keys(MGAIN_KEY_BY_CAT)) {
    const key = MGAIN_KEY_BY_CAT[cat];
    const cd = MGAIN.cats[cat];
    let g = null;
    if (cd) {
      const rf = _rowInIndex(cd.from.idx, lon, lat);
      const rt = _rowInIndex(cd.to.idx, lon, lat);
      if (rf >= 0 && rt >= 0) g = cd.to.score[rt] - cd.from.score[rf];
    }
    out[key] = Number.isFinite(g) ? g : null;
    if (Number.isFinite(g)) { sum += g; n++; }
  }
  out.mgainOverall = n ? sum / n : null;
  return out;
}
