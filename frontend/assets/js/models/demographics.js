// SCB demographics loader + composite "need" (deprivation) index.
// Classical script (shared global scope). Loaded after districts.js so it can
// annotate district features, and before fairness uses cityNeedByRegso at runtime.
//
// OFFLINE ONLY: reads the bundled demographics_<city>.json baked by
// tools/bake_scb_demographics.py + tools/analysis/scb_parse.py. Never the SCB API.
//
// Need index (user-chosen composite): z(-median income) + z(unemployed) +
// z(income support share) + z(-higher-ed eligible), z-scored across the districts
// the app actually shows (shown_on_map). Fed into IF-City equityWeight when the
// "Need-weighted fairness" toggle is on (see fairness.js).

// field, sign: +1 = more of it means more need; -1 = inverse.
const NEED_INDEX_FIELDS = [
  { json: 'median_disp_income_pba',   sign: -1 },
  { json: 'unemployed_pct',           sign: +1 },
  { json: 'income_support_share_pct', sign: +1 },
  { json: 'eligible_higher_edu_pct',  sign: -1 }
];

let __demographicsLoadPromise = null;
let __demographicsLoadedCity = null;

function _zscoreMap(values) {
  const finite = values.filter(Number.isFinite);
  const n = finite.length;
  if (!n) return { mean: 0, std: 1 };
  const mean = finite.reduce((a, b) => a + b, 0) / n;
  const variance = finite.reduce((s, v) => s + (v - mean) * (v - mean), 0) / n;
  const std = Math.sqrt(Math.max(variance, 1e-12));
  return { mean, std: std || 1 };
}

// Build cityNeedByRegso (regsokod -> needZ) from cityDemographics, using only the
// districts shown on the map so the z-scores match what the user sees.
function computeNeedIndex(demoByCode) {
  const codes = Object.keys(demoByCode).filter(c => demoByCode[c]?.shown_on_map);
  // per-field standardisation stats
  const stats = {};
  for (const f of NEED_INDEX_FIELDS) {
    stats[f.json] = _zscoreMap(codes.map(c => Number(demoByCode[c][f.json])));
  }
  const need = new Map();
  for (const c of codes) {
    let sum = 0, used = 0;
    for (const f of NEED_INDEX_FIELDS) {
      const v = Number(demoByCode[c][f.json]);
      if (!Number.isFinite(v)) continue;
      const { mean, std } = stats[f.json];
      sum += f.sign * (v - mean) / std;
      used += 1;
    }
    // average of available components, then re-standardise to unit scale below
    if (used) need.set(c, sum / used);
  }
  // re-standardise the composite so needZ has ~unit std (keeps strength slider meaningful)
  const comp = _zscoreMap(Array.from(need.values()));
  for (const [c, v] of need) need.set(c, (v - comp.mean) / comp.std);
  return need;
}

// Load demographics_<city>.json for the active district city. Idempotent per city.
async function ensureDemographicsData(cityKey) {
  const key = cityKey || (typeof activeDistrictCityKey !== 'undefined' ? activeDistrictCityKey : null);
  const url = key && typeof DEMOGRAPHICS_URL_BY_CITY_KEY !== 'undefined'
    ? DEMOGRAPHICS_URL_BY_CITY_KEY[key] : null;
  if (!url) { cityDemographics = null; cityNeedByRegso = null; return null; }
  if (__demographicsLoadedCity === key && cityDemographics) return cityDemographics;
  if (__demographicsLoadPromise && __demographicsLoadedCity === key) return __demographicsLoadPromise;

  __demographicsLoadedCity = key;
  __demographicsLoadPromise = fetch(url)
    .then(r => { if (!r.ok) throw new Error(`demographics fetch failed (${r.status})`); return r.json(); })
    .then(obj => {
      cityDemographics = obj;                 // { regsokod: {field:value,...} }
      cityNeedByRegso = computeNeedIndex(obj);
      console.log(`[demographics] ${key}: ${Object.keys(obj).length} districts, `
        + `${cityNeedByRegso.size} with need index`);
      return obj;
    })
    .catch(err => {
      console.warn('[demographics] load failed:', err);
      cityDemographics = null; cityNeedByRegso = null;
      __demographicsLoadPromise = null;
      return null;
    });
  return __demographicsLoadPromise;
}

// Annotate each district feature with its demographic record + needZ, so the
// DR / PCP / district views can read props.__demo and props.__needZ directly.
function attachDemographicsToDistricts(fc) {
  if (!fc?.features || !cityDemographics) return;
  for (const feat of fc.features) {
    const code = (typeof regsoCodeFromProps === 'function')
      ? regsoCodeFromProps(feat.properties || {})
      : (feat.properties || {}).regsokod;
    const rec = code ? cityDemographics[code] : null;
    if (rec) {
      feat.properties.__demo = rec;
      const nz = cityNeedByRegso ? cityNeedByRegso.get(code) : null;
      feat.properties.__needZ = Number.isFinite(nz) ? nz : null;
    }
  }
}

// Convert a district needZ into a multiplicative equity weight.
//   weight = clamp(exp(strength * clamp(needZ,-3,3)), 0.25, 4)
// strength 0 -> all weights 1 (off). Symmetric in log space: deprived >1, affluent <1.
function needWeightFromZ(needZ, strength) {
  if (!Number.isFinite(needZ) || !Number.isFinite(strength) || strength === 0) return 1;
  const z = Math.max(-3, Math.min(3, needZ));
  const w = Math.exp(strength * z);
  return Math.max(0.25, Math.min(4, w));
}

// ---- Rich per-building DR features (modal-gap + built form + demo) ----
// Coordinate key must match the bake: turf.centroid rounded to 5 decimals.
function drFeatureKey(lon, lat) {
  return `${Number(lon).toFixed(5)},${Number(lat).toFixed(5)}`;
}

let __drFeaturesLoadPromise = null;
let __drFeaturesLoadedCity = null;
function ensureDrFeaturesData(cityKey) {
  const key = cityKey || (typeof lastCityName !== 'undefined' && typeof normalizeCityKey === 'function'
    ? normalizeCityKey(lastCityName) : null);
  const url = key && typeof DR_FEATURES_URL_BY_CITY_KEY !== 'undefined'
    ? DR_FEATURES_URL_BY_CITY_KEY[key] : null;
  if (!url) { cityDrFeatures = null; return Promise.resolve(null); }
  if (__drFeaturesLoadedCity === key && cityDrFeatures) return Promise.resolve(cityDrFeatures);
  if (__drFeaturesLoadPromise && __drFeaturesLoadedCity === key) return __drFeaturesLoadPromise;
  __drFeaturesLoadedCity = key;
  drRichBuildingsJoined = false;
  __drFeaturesLoadPromise = fetch(url)
    .then(r => { if (!r.ok) throw new Error(`dr-features fetch failed (${r.status})`); return r.json(); })
    .then(obj => {
      cityDrFeatures = obj;
      console.log(`[dr-features] ${key}: ${Object.keys(obj).length} buildings loaded`);
      return obj;
    })
    .catch(err => {
      console.warn('[dr-features] load failed:', err);
      cityDrFeatures = null; __drFeaturesLoadPromise = null; return null;
    });
  return __drFeaturesLoadPromise;
}

// Spatial hash of the baked points for nearest-match (robust to centroid-method
// differences between the bake and turf.centroid). Cell ~ 0.002 deg (~150 m).
let __drGrid = null;
const DR_GRID_CELL = 0.002;
function _drGridKey(lon, lat) {
  return `${Math.floor(lon / DR_GRID_CELL)},${Math.floor(lat / DR_GRID_CELL)}`;
}
function buildDrGrid() {
  __drGrid = new Map();
  for (const k in cityDrFeatures) {
    const [lon, lat] = k.split(',').map(Number);
    const gk = _drGridKey(lon, lat);
    let arr = __drGrid.get(gk);
    if (!arr) { arr = []; __drGrid.set(gk, arr); }
    arr.push([lon, lat, cityDrFeatures[k]]);
  }
}
function nearestDrRecord(lon, lat) {
  if (!__drGrid) return null;
  const gx = Math.floor(lon / DR_GRID_CELL), gy = Math.floor(lat / DR_GRID_CELL);
  let best = null, bestD = Infinity;
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      const arr = __drGrid.get(`${gx + dx},${gy + dy}`);
      if (!arr) continue;
      for (const [plon, plat, rec] of arr) {
        const d = (plon - lon) * (plon - lon) + (plat - lat) * (plat - lat);
        if (d < bestD) { bestD = d; best = rec; }
      }
    }
  }
  // ~30 m tolerance in deg^2 (0.0003^2); rec are per-building so nearest is fine.
  return bestD <= 9e-8 ? best : null;
}

// Load (if needed) and attach rich features onto each building's props.__drRich,
// once per city. Called from runDR before collectDRData (building mode).
async function ensureRichBuildingFeatures(cityKey) {
  await ensureDrFeaturesData(cityKey).catch(() => null);
  if (!cityDrFeatures || drRichBuildingsJoined) return;
  if (!(typeof baseCityFC !== 'undefined') || !baseCityFC?.features?.length) return;
  buildDrGrid();
  let hits = 0;
  for (const f of baseCityFC.features) {
    let c;
    try { c = turf.centroid(f).geometry.coordinates; } catch { continue; }
    if (!c) continue;
    const rec = nearestDrRecord(c[0], c[1]);
    if (rec) { f.properties = f.properties || {}; f.properties.__drRich = rec; hits++; }
  }
  drRichBuildingsJoined = true;
  console.log(`[dr-features] joined ${hits}/${baseCityFC.features.length} buildings (nearest-match)`);
}

// UI entry: toggle / strength change -> update state + recompute fairness.
function setNeedWeighting({ enabled, strength } = {}, { recompute = true } = {}) {
  if (typeof enabled === 'boolean') needWeightEnabled = enabled;
  if (Number.isFinite(strength)) needWeightStrength = strength;
  if (recompute && typeof recomputeFairnessAfterWhatIf === 'function') {
    recomputeFairnessAfterWhatIf();
  }
}
