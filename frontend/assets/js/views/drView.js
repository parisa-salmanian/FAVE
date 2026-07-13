// Dimensionality-Reduction Explorer — extracted from main.js. Loaded AS A
// CLASSICAL SCRIPT before main.js so that its state declarations and
// functions are visible across the same Realm. DR-internal state is
// self-contained; functions reference main.js globals (baseCityFC etc.) by
// name, so they resolve at call time — order is fine as long as the DOM
// handlers that invoke them run after main.js has set those globals up.

// Color-by options that map a #drColorBy value to a synthetic per-building
// demographic share (keys stamped by synthpop.js / fed via DR_SYNTH_DEMO_FEATURES).
// Lets a UMAP cluster be named by its dominant demographic.
const DR_DEMO_COLOR_BY = {
  elderly:       { key: 'demSynElder',    label: 'Elderly share',    dir: 'Fewer → More elderly' },
  dependency:    { key: 'demSynDep',      label: 'Dependency ratio', dir: 'Low → High dependency' },
  incomesupport: { key: 'demSynIncSup',   label: 'Income support',   dir: 'Low → High' },
  higheredu:     { key: 'demSynHigherEd', label: 'Higher education', dir: 'Low → High' },
  child:         { key: 'demSynChild',    label: 'Child share',      dir: 'Fewer → More children' }
};

/* ---- Library detection ---- */
function hasUMAPGlobal() {
  if (window.UMAP && typeof window.UMAP === 'function') return true;
  if (window.umapjs && typeof window.umapjs.UMAP === 'function') { window.UMAP = window.umapjs.UMAP; return true; }
  if (window.umap && typeof window.umap.UMAP === 'function') { window.UMAP = window.umap.UMAP; return true; }
  return false;
}
let triedDynamicUMAP = false;
async function ensureUMAP() {
  if (hasUMAPGlobal()) return true;
  if (!triedDynamicUMAP) {
    triedDynamicUMAP = true;
    try {
      await new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.type = 'module';
        s.textContent = `
          import { UMAP } from 'https://cdn.skypack.dev/umap-js@1.3.3';
          window.UMAP = UMAP;
          window.dispatchEvent(new Event('umap:ready'));
        `;
        s.onerror = () => reject(new Error('ESM import failed'));
        window.addEventListener('umap:ready', () => resolve(), { once:true });
        document.head.appendChild(s);
      });
      return hasUMAPGlobal();
    } catch (e) {
      console.warn('UMAP dynamic import failed:', e);
      return false;
    }
  }
  return false;
}

/* ---- Canvas + overlay surface ---- */
// DR overlay + explanation globals
let drRO = null;

// Used to discard stale async engine responses (EBM + contrastive)
let drEngineRequestId = 0;

const drPlot = {
  // DR embedding + visual stuff
  points: null,
  colors: null,
  screenXY: null,
  minX: 0,
  maxX: 1,
  minY: 0,
  maxY: 1,
  pad: 20,
  width: 0,
  height: 0,
  sample: null,
  metrics: null,
  mode: "building",

  // DR feature matrix & labels (same feature space used for PCA/UMAP)
  features: null,          // X matrix from collectDRData()
  featureLabels: null,     // human-readable labels for each feature dimension

  // Cached stats & explanations (for your existing Stats / Model tabs)
  cityStats: null,             // per-feature stats over all DR points
  lastSelectionIdx: [],        // indices of the last lasso selection
  lastFeatureDiff: null,       // unsupervised feature differences for last selection (Stats tab)
  lastModelExplanation: null,  // supervised model explanation for last selection (Model tab)

  // engine-level feature-importance views above the tabs
  // engineMode chooses which engine drives the "EBM vs Contrast" bar chart
  engineMode: 'ebm',           // 'ebm' | 'contrast'
  engineEBM: null,             // last EBM response (from backend)
  engineContrast: null,        // last contrastive-distribution response (JS)
  runNonce: 0                  // increments on each DR run to vary sample unless reproducible mode is enabled
};

// Current explanation mode ("stats" | "model") - keep as-is for the lower tabs
let drExplainMode = 'stats';


/* ======================= DR ↔ Map selection helpers ======================= */
 /** Return currently selected buildings on the map (based on _drSelected). */
  function getCurrentMapSelection() {
    if (persistentBuildingSelection.size) {
      return Array.from(persistentBuildingSelection);
    }
    return (baseCityFC?.features || []).filter(f => f?.properties?._drSelected);
  }

  /** Remove any previous DR-based selection flag from buildings. */
  function clearDRMapSelection(opts = {}) {
    const { preservePersistent = false } = opts;
    const clearFeature = (obj) => {
      if (!obj) return;
      if (obj.properties) {
        if (obj.properties._drSelected) delete obj.properties._drSelected;
        if (obj.properties._drColor) delete obj.properties._drColor;
      } else {
        if (obj._drSelected) delete obj._drSelected;
        if (obj._drColor) delete obj._drColor;
      }
    };

    (baseCityFC?.features || []).forEach(clearFeature);
    (districtFC?.features || []).forEach(clearFeature);
    (mezoHexData || []).forEach(clearFeature);
    if (!preservePersistent) setPersistentBuildingSelection([]);
    drSelectionTick++;
}

function applyDRMapSelectionFromIndices(idxArray) {
  if (!idxArray || !idxArray.length || !drPlot.sample) {
    clearDRMapSelection();
    updateLayers();
    return;
  }

  const selectedEntities = idxArray
    .map(i => drPlot.sample[i])
    .filter(Boolean);
  applyMapSelection(selectedEntities, { skipDRSync: true });
}

// SAFE: returns refs if present, never throws
function ensureDRUI() {
  return {
    algoSel:    document.getElementById('drAlgo'),
    colorSel:   document.getElementById('drColorBy'),
    maxPtsEl:   document.getElementById('drMaxPts'),
    normEl:     document.getElementById('drNormalize'),
    canvas:     document.getElementById('drCanvas'),
    wrap:       document.getElementById('drCanvasWrap'),
    statusEl:   document.getElementById('drStatus'),
    infoEl:     document.getElementById('drInfo'),
    selInfoEl:  document.getElementById('drSelectInfo'),
    lassoBtn:   document.getElementById('drLassoBtn'),
    clearSelBtn:document.getElementById('drClearSelBtn'),
    clearProjBtn:document.getElementById('drClearProjBtn'),
    legendTitle:document.getElementById('drLegendTitle'),
    legendText: document.getElementById('drLegendText'),
    explainStatsEl:        document.getElementById('drExplainStats'),
    explainModelEl:        document.getElementById('drExplainModel'),
    explainModeStatsBtn:   document.getElementById('drExplainModeStats'),
    explainModeModelBtn:   document.getElementById('drExplainModeModel'),

    engineEBMBtn:          document.getElementById('drEngineEBMBtn'),
    engineContrastBtn:     document.getElementById('drEngineContrastBtn'),
    engineCalcBtn:         document.getElementById('drEngineCalcBtn'),
    enginePlotEl:          document.getElementById('drEnginePlot'),

    histWrap:          document.getElementById('drHistWrap'),
    districtBarWrap:   document.getElementById('drDistrictBarWrap'),
    scatterHeightWrap: document.getElementById('drScatterHeightWrap'),
    lorenzWrap:        document.getElementById('drLorenzWrap'),
    thresholdSlider:   document.getElementById('drThresholdSlider'),
    thresholdLabel:    document.getElementById('drThresholdLabel'),
    thresholdBarWrap:  document.getElementById('drThresholdBarWrap'),
    categoryHistsWrap: document.getElementById('drCategoryHistsWrap')
  };
}

function prepareDRSurface() {
  const { wrap } = ensureDRUI();
  if (!wrap) return;

  wrap.style.height = Math.max(213, Math.floor(window.innerHeight * 0.40)) + 'px';
  if (getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';

  if (!document.getElementById('drOverlay')) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.id = 'drOverlay';
    svg.setAttribute('style', 'position:absolute; inset:0; width:100%; height:100%; pointer-events:auto; z-index:2;');
    wrap.appendChild(svg);
  } else if (typeof d3 !== 'undefined' && !drPlot.points) {
    d3.select('#drOverlay').selectAll('*').remove();
  }

  if (!drRO) {
    drRO = new ResizeObserver(() => { resizeDRCanvas(); redrawDR(); });
    drRO.observe(wrap);
    window.addEventListener('resize', () => { resizeDRCanvas(); redrawDR(); });
  }
  resizeDRCanvas();
}

function resizeDRCanvas() {
  const { canvas, wrap } = ensureDRUI();
  if (!canvas || !wrap) return;
  const rect = wrap.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  ctx.fillStyle = '#f8f9fa';
  ctx.fillRect(0,0,rect.width,rect.height);
  // ctx.strokeStyle = 'rgba(0,0,0,0.08)';
  // for (let i=0;i<=rect.width;i+=50) { ctx.beginPath(); ctx.moveTo(i,0); ctx.lineTo(i,rect.height); ctx.stroke(); }
  // for (let i=0;i<=rect.height;i+=50){ ctx.beginPath(); ctx.moveTo(0,i); ctx.lineTo(rect.width,i); ctx.stroke(); }

  drPlot.width = rect.width; drPlot.height = rect.height;
}

function rampColor01(v) {
  const [r, g, b] = colorFromScore(v);
  return [r, g, b, 255];
}
function grey(a=180){ return [a,a,a,255]; }

function zscore(arr) {
  const vals = arr.filter(Number.isFinite);
  const mu = vals.reduce((a,b)=>a+b,0)/Math.max(1, vals.length);
  const sd = Math.sqrt(vals.reduce((s,v)=>s+(v-mu)*(v-mu),0)/Math.max(1, vals.length));
  return arr.map(v => Number.isFinite(v) ? (sd>0 ? (v-mu)/sd : 0) : 0);
}
// Robust low/high bounds (percentile clamp) so a few outlier buildings don't
// stretch a color ramp and crush all real variation into one end. Skewed
// demographics (income-support, dependency) need this; min–max does not work.
function robustLoHi(vals, pLo = 0.02, pHi = 0.98) {
  const s = vals.filter(Number.isFinite).slice().sort((a, b) => a - b);
  if (!s.length) return [0, 1];
  const q = (p) => {
    const i = (s.length - 1) * p, f = Math.floor(i);
    return s[f] + (s[Math.min(f + 1, s.length - 1)] - s[f]) * (i - f);
  };
  let lo = q(pLo), hi = q(pHi);
  if (hi <= lo) { lo = s[0]; hi = s[s.length - 1]; }
  return [lo, hi];
}
function clamp01(v) { return v < 0 ? 0 : (v > 1 ? 1 : v); }
function logAreaOfFeature(f) { try { return Math.log(Math.max(1, turf.area(f))); } catch { return 0; } }

// Per-building RAW network distance (metres) to the nearest facility of each
// service, read from the baked routing matrices (the DISTANCE accessibility model
// — the paper's companion to the gravity score). Cached on the feature so a DR
// re-run is cheap. These let the multivariate views dissect distance composition,
// where the child-share coupling is NON-tautological (child-heavy areas sit far
// from university/hospital) — a structure the gravity score de-emphasises.
// cat -> dr-row key for the 10 services in ALL_CATEGORIES.
const DR_DIST_KEY_BY_CAT = {
  grocery: 'distGrocery', hospital: 'distHospital', healthcare_center: 'distHealthcare',
  pharmacy: 'distPharmacy', veterinary: 'distVeterinary', university: 'distUniversity',
  school_high: 'distSchoolHigh', school_primary: 'distPrimary',
  kindergarten: 'distKindergarten', dentistry: 'distDentistry'
};
function rawDistsForFeature(f) {
  const p = f.properties || (f.properties = {});
  if (p.__rawDist) return p.__rawDist;
  const out = {};
  if (typeof routingReady === 'function' && routingReady() && typeof routingDistsForPoint === 'function') {
    let lon = NaN, lat = NaN;
    // Fast ring-average centroid (avoids turf.centroid overhead over ~100k
    // buildings); the 45 m routing snap tolerance absorbs the small offset.
    try {
      const g = f.geometry; let cs = null;
      if (g) { if (g.type === 'Polygon') cs = g.coordinates[0]; else if (g.type === 'MultiPolygon') cs = g.coordinates[0] && g.coordinates[0][0]; }
      if (cs && cs.length) { let x = 0, y = 0, n = 0; for (const c of cs) { x += c[0]; y += c[1]; n++; } if (n) { lon = x / n; lat = y / n; } }
    } catch {}
    if (Number.isFinite(lon) && Number.isFinite(lat)) {
      // Resolve per category by point — each category's own key order (see the
      // routing-matrix row-aliasing fix), so distUniversity/distHospital aren't
      // read from the wrong building.
      for (const cat of Object.keys(DR_DIST_KEY_BY_CAT)) {
        const d = routingDistsForPoint(cat, lon, lat);
        const m = d && d.dists ? Number(d.dists[0]) : NaN;
        out[DR_DIST_KEY_BY_CAT[cat]] = Number.isFinite(m) ? m : null;
      }
    }
  }
  p.__rawDist = out;
  return out;
}

// ---- 2SFCA SUPPLY features for the DR ---------------------------------------
// Proximity/gravity access is ~1-D (essentially one "how close is town" axis, so
// UMAP/contrastive/EBM on it are near-descriptive). The E2SFCA supply-to-demand
// provision is genuinely MULTI-dimensional and decorrelates from proximity: an
// area can be CLOSE to a service yet under-supplied (crowded), or far yet
// adequately supplied. These per-category provision values (0..1, the SAME
// normalisation as the inspector's Supply panel — access2sfca.js) feed the
// "Supply (2SFCA)" / "Access + Supply" feature sets so the coordinated views can
// surface supply patterns no single map layer shows.
const DR_SUPPLY_KEY_BY_CAT = {
  grocery: 'supplyGrocery', hospital: 'supplyHospital', healthcare_center: 'supplyHealthcare',
  pharmacy: 'supplyPharmacy', veterinary: 'supplyVeterinary', university: 'supplyUniversity',
  school_high: 'supplySchoolHigh', school_primary: 'supplyPrimary',
  kindergarten: 'supplyKindergarten', dentistry: 'supplyDentistry'
};
const DR_SUPPLY_LABELS = {
  supplyGrocery: 'grocery supply (2sfca)', supplyHospital: 'hospital supply (2sfca)',
  supplyHealthcare: 'healthcare supply (2sfca)', supplyPharmacy: 'pharmacy supply (2sfca)',
  supplyVeterinary: 'veterinary supply (2sfca)', supplyUniversity: 'university supply (2sfca)',
  supplySchoolHigh: 'high school supply (2sfca)', supplyPrimary: 'primary school supply (2sfca)',
  supplyKindergarten: 'kindergarten supply (2sfca)', supplyDentistry: 'dentistry supply (2sfca)'
};
// Map an ACCESS2SFCA per-category object ({cat:{norm|normMean,...}}) onto DR row
// keys. `field` = 'norm' (per building) or 'normMean' (hex/district aggregate).
function supplyRowFromCats(cats, field) {
  const out = {};
  for (const cat of Object.keys(DR_SUPPLY_KEY_BY_CAT)) {
    const cd = cats ? cats[cat] : null;
    const v = cd ? Number(cd[field]) : NaN;
    out[DR_SUPPLY_KEY_BY_CAT[cat]] = Number.isFinite(v) ? v : null;
  }
  return out;
}
// Per-building provision (norm 0..1), fast ring-average centroid, cached on the
// feature keyed by travel mode (so a mode change re-reads the right layer). Needs
// ensureAccess2sfca() to have loaded the layer (runDR preloads it — see below).
function supplyValsForFeature(f) {
  const p = f.properties || (f.properties = {});
  const mode = (document.getElementById('fairnessTravelMode')?.value || 'walking');
  if (p.__supplyVals && p.__supplyMode === mode) return p.__supplyVals;
  let out = {};
  if (typeof access2sfcaForPoint === 'function') {
    let lon = NaN, lat = NaN;
    try {
      const g = f.geometry; let cs = null;
      if (g) { if (g.type === 'Polygon') cs = g.coordinates[0]; else if (g.type === 'MultiPolygon') cs = g.coordinates[0] && g.coordinates[0][0]; }
      if (cs && cs.length) { let x = 0, y = 0, n = 0; for (const c of cs) { x += c[0]; y += c[1]; n++; } if (n) { lon = x / n; lat = y / n; } }
    } catch {}
    if (Number.isFinite(lon) && Number.isFinite(lat)) {
      const s = access2sfcaForPoint(lon, lat);
      if (s && s.cats) out = supplyRowFromCats(s.cats, 'norm');
    }
  }
  p.__supplyVals = out; p.__supplyMode = mode;
  return out;
}

// ---- MISMATCH (proximity − 2SFCA supply) features for the DR ----------------
// Proximity and supply are POSITIVELY correlated on average (central areas are
// both close AND well-supplied — verified r≈+0.24..+0.67 across the 7 cities),
// so the decision-relevant signal is where they DISAGREE: close-but-crowded
// (positive) vs supply-rich-for-its-distance (negative). This is the same
// quantity the "Mismatch" map layer paints (supplyProvisionLens.js), computed
// here per service + overall so the DR projection carries the overall residual
// axis while the contrastive/EBM attribute a selection's crowding to specific
// services. Both terms are the SAME 0..1 city-relative normalisation (fairness
// score vs 2SFCA provision), so the difference is meaningful; the DR z-scores
// the column anyway. Absent pairs → null (ignored downstream).
const DR_MISMATCH_PAIRS = {
  mismatchGrocery:      ['grocery',      'supplyGrocery'],
  mismatchHospital:     ['hospital',     'supplyHospital'],
  mismatchHealthcare:   ['healthcare',   'supplyHealthcare'],
  mismatchPharmacy:     ['pharmacy',     'supplyPharmacy'],
  mismatchVeterinary:   ['veterinary',   'supplyVeterinary'],
  mismatchUniversity:   ['university',   'supplyUniversity'],
  mismatchSchoolHigh:   ['schoolHigh',   'supplySchoolHigh'],
  mismatchPrimary:      ['primary',      'supplyPrimary'],
  mismatchKindergarten: ['kindergarten', 'supplyKindergarten'],
  mismatchDentistry:    ['dentistry',    'supplyDentistry']
};
const DR_MISMATCH_LABELS = {
  mismatchGrocery: 'grocery mismatch (prox−supply)', mismatchHospital: 'hospital mismatch (prox−supply)',
  mismatchHealthcare: 'healthcare mismatch (prox−supply)', mismatchPharmacy: 'pharmacy mismatch (prox−supply)',
  mismatchVeterinary: 'veterinary mismatch (prox−supply)', mismatchUniversity: 'university mismatch (prox−supply)',
  mismatchSchoolHigh: 'high school mismatch (prox−supply)', mismatchPrimary: 'primary school mismatch (prox−supply)',
  mismatchKindergarten: 'kindergarten mismatch (prox−supply)', mismatchDentistry: 'dentistry mismatch (prox−supply)'
};
// From a built DR row (proximity per-cat + supply per-cat already populated),
// derive per-category mismatch (access − supply, both 0..1) plus ONE overall
// mismatch (overall access − mean per-cat supply). Called at the end of every
// scale's row builder.
function mismatchRowFromRow(row) {
  const out = {};
  let supSum = 0, supN = 0;
  for (const mk of Object.keys(DR_MISMATCH_PAIRS)) {
    const [ak, sk] = DR_MISMATCH_PAIRS[mk];
    const a = Number(row[ak]);
    const s = Number(row[sk]);
    out[mk] = (Number.isFinite(a) && Number.isFinite(s)) ? (a - s) : null;
    if (Number.isFinite(s)) { supSum += s; supN++; }
  }
  const ov = Number(row.overall);
  const supOverall = supN ? supSum / supN : NaN;
  out.mismatch = (Number.isFinite(ov) && Number.isFinite(supOverall)) ? (ov - supOverall) : null;
  // Overall 2SFCA supply (mean per-cat provision) — for the "Supply provision"
  // colour option (not a matrix/metrics column).
  out.supplyOverall = Number.isFinite(supOverall) ? supOverall : null;
  return out;
}

function stableHashString(str) {
  let h = 2166136261;
  const text = String(str || '');
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function stableEntityId(entity, fallbackIdx = 0) {
  const props = entity?.properties || entity || {};
  const base =
    props.id ??
    props.osm_id ??
    props['@id'] ??
    props.hex ??
    props.__districtName ??
    props.name;
  if (base != null && String(base).trim()) return String(base);

  const coords = entity?.geometry?.coordinates;
  if (Array.isArray(coords)) {
    const flat = JSON.stringify(coords).slice(0, 160);
    if (flat) return `geom:${flat}`;
  }
  return `idx:${fallbackIdx}`;
}

function stableSampleIndices(sample, take, runNonce = 0) {
  const baseSeed = Number(globalThis.DR_SAMPLE_SEED) || 202502;
  const reproducible = !!globalThis.DR_REPRODUCIBLE_SAMPLE;
  const seed = reproducible ? baseSeed : (baseSeed + Math.imul(Number(runNonce) || 0, 2654435761));
  const ranked = sample.map((entity, idx) => {
    const key = stableEntityId(entity, idx);
    return { idx, score: stableHashString(`${seed}:${key}`) };
  });
  ranked.sort((a, b) => a.score - b.score || a.idx - b.idx);
  return ranked.slice(0, take).map((entry) => entry.idx);
}

function isWhatIfEntity(entity) {
  const props = entity?.properties || {};
  // Keep DR/PC visibility for synthetic what-if additions, but avoid pinning
  // every edited real building (which can destabilize the sample manifold).
  return !!(props.__whatIfMock || props.__whatIfAdded);
}

function isWhatIfEntity(entity) {
  const props = entity?.properties || {};
  return !!(props.__whatIf || props.__whatIfMock || props.__whatIfAdded);
}

/* ---- DR feature collection (different for OSM vs Local) ---- */
function collectDRData(maxPts = Infinity, normalize = true, colorBy = 'overall', runNonce = 0) {
  const mode = currentDRDataMode();

  const makeMatrix = (sample, rows, labels) => {
    if (!sample.length) throw new Error(`No ${mode}s available for DR.`);

    const N = sample.length;
    const take = Math.min(maxPts, N);
    const stableOrder = stableSampleIndices(sample, sample.length, runNonce);
    const forcedSet = new Set();
    sample.forEach((entity, i) => {
      if (isWhatIfEntity(entity)) forcedSet.add(i);
    });

    const forcedCandidates = stableOrder.filter(i => forcedSet.has(i));
    const regularCandidates = stableOrder.filter(i => !forcedSet.has(i));

    // Balanced pseudo-random mix: keep mocked buildings visible without letting
    // them fully dominate the DR sample when maxPts is capped.
    const hasBothGroups = forcedCandidates.length > 0 && regularCandidates.length > 0;
    let forcedTake = 0;
    if (hasBothGroups) {
      const populationShare = forcedCandidates.length / Math.max(1, N);
      const proportionalTarget = Math.round(take * populationShare);
      const forcedCap = Math.max(1, Math.floor(take * 0.35));
      forcedTake = Math.max(1, Math.min(forcedCandidates.length, forcedCap, proportionalTarget || 1));
    } else if (forcedCandidates.length > 0) {
      forcedTake = Math.min(forcedCandidates.length, take);
    }

    let regularTake = Math.min(regularCandidates.length, Math.max(0, take - forcedTake));
    let remainder = take - (forcedTake + regularTake);
    if (remainder > 0 && forcedCandidates.length > forcedTake) {
      const extraForced = Math.min(remainder, forcedCandidates.length - forcedTake);
      forcedTake += extraForced;
      remainder -= extraForced;
    }
    if (remainder > 0 && regularCandidates.length > regularTake) {
      regularTake += Math.min(remainder, regularCandidates.length - regularTake);
    }

    const forcedIdx = forcedCandidates.slice(0, forcedTake);
    const sampledIdx = regularCandidates.slice(0, regularTake);

    // Preserve deterministic pseudo-random ordering to keep UMAP behavior stable.
    const idxSet = new Set([...forcedIdx, ...sampledIdx]);
    const idx = stableOrder.filter(i => idxSet.has(i));

    const pickedSample = idx.map(i => sample[i]);
    const pickedRows = idx.map(i => rows[i]);
    const keys = Object.keys(labels);
    const vecByKey = {};

    keys.forEach((key) => {
      const vals = pickedRows.map(r => Number.isFinite(r[key]) ? r[key] : null);
      const valid = vals.filter(Number.isFinite);
      const mid = valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : 0;
      const imputed = vals.map(v => Number.isFinite(v) ? v : mid);
      vecByKey[key] = normalize ? zscore(imputed) : imputed;
    });

  const X = pickedRows.map((_, i) => keys.map(key => vecByKey[key][i]));

    let colors;
    if (colorBy === 'overall') {
      colors = pickedRows.map((r) => Number.isFinite(r.overall) ? rampColor01(r.overall) : grey(120));
      updateLegend('overall', `Legend (${mode} overall fairness)`, 'Most fair (left) → Least fair (right)');
    } else if (colorBy === 'poi') {
      colors = pickedRows.map((r) => Number.isFinite(r.focused) ? rampColor01(r.focused) : grey(100));
      updateLegend('poi', `Legend (${prettyPOIName(fairCategory) || 'Selected fairness'})`, 'Most fair (left) → Least fair (right)');
    } else if (colorBy === 'mismatch') {
      // Diverging red↔blue on the overall mismatch (proximity − supply). Red =
      // close-but-crowded (the map overstates provision), blue = supply-rich for
      // its distance, pale = the two agree. This is THE colour that reveals the
      // supply_plus layout's residual structure. Robust symmetric clamp (p95 of
      // |mismatch|) so outliers don't wash out the gradient. No-data → hidden.
      const get = (r) => (r.mismatch == null ? NaN : Number(r.mismatch));
      const absSorted = pickedRows.map(get).filter(Number.isFinite).map(Math.abs).sort((a, b) => a - b);
      const M = absSorted.length ? (quantileFromSorted(absSorted, 0.95) || 1) : 1;
      colors = pickedRows.map((r) => {
        const v = get(r);
        if (!Number.isFinite(v)) return [0, 0, 0, 0];
        const d = Math.max(-M, Math.min(M, v));
        const t = 0.5 - d / (2 * M);   // +M → 0 (red), −M → 1 (blue)
        if (typeof d3 !== 'undefined' && d3.interpolateRdBu && d3.color) {
          const c = d3.color(d3.interpolateRdBu(t));
          if (c) return [Math.round(c.r), Math.round(c.g), Math.round(c.b)];
        }
        return rampColor01(clamp01(1 - t));
      });
      updateLegend('mismatch', 'Legend (Mismatch: proximity − supply)', 'Supply-rich (blue) → Close-but-crowded (red)');
    } else if (colorBy === 'supply') {
      // Sequential on overall 2SFCA supply provision (mean per-cat norm 0..1).
      const get = (r) => (r.supplyOverall == null ? NaN : Number(r.supplyOverall));
      const vals = pickedRows.map(get).filter(Number.isFinite);
      const [lo, hi] = robustLoHi(vals);
      const span = (hi - lo) || 1;
      colors = pickedRows.map((r) => {
        const v = get(r);
        return Number.isFinite(v) ? rampColor01(clamp01((v - lo) / span)) : grey(120);
      });
      updateLegend('supply', 'Legend (2SFCA supply provision)', 'Least → Most supply');
    } else if (colorBy === 'need' || colorBy === 'income') {
      // Color by an SCB axis (district need or median income). Min–max normalise
      // across the sample so the ramp spans the actual value range.
      const get = (r) => colorBy === 'need'
        ? (Number.isFinite(r.richNeed) ? r.richNeed : r.demNeed)
        : (Number.isFinite(r.richIncome) ? r.richIncome : r.demIncome);
      const vals = pickedRows.map(get).filter(Number.isFinite);
      const [lo, hi] = robustLoHi(vals);
      const span = (hi - lo) || 1;
      colors = pickedRows.map((r) => {
        const v = get(r);
        return Number.isFinite(v) ? rampColor01(clamp01((v - lo) / span)) : grey(120);
      });
      const label = colorBy === 'need' ? 'SCB need (deprivation)' : 'SCB median income';
      const dir = colorBy === 'need' ? 'Low need → High need' : 'Low income → High income';
      updateLegend(colorBy, `Legend (${label})`, dir);
    } else if (colorBy === 'childreal') {
      // Color by the REAL SCB child share of each point's DESO (building mode:
      // row.childReal via __deso → epiDesoProps). Measured data, not the synthetic
      // per-building spread — this is the defensible "access composition vs who
      // lives there" coupling. Robust percentile clamp so outliers don't crush it.
      // Cells/buildings with no measured DESO child share (null) → NaN → grey, so
      // empty rural mezo cells stay neutral (not miscoloured as "0% children") and
      // the ramp spans only the real child range for a crisp family gradient.
      const get = (r) => (r.childReal == null ? NaN : Number(r.childReal));
      const vals = pickedRows.map(get).filter(Number.isFinite);
      const [lo, hi] = robustLoHi(vals);
      const span = (hi - lo) || 1;
      colors = pickedRows.map((r) => {
        const v = get(r);
        // No measured DESO child share → HIDE the point (fully transparent) rather
        // than draw a meaningless grey dot. These cells have accessibility data but
        // no real child overlay (rural cells outside DESO coverage); hiding them
        // keeps the child gradient clean. They stay in the sample (UMAP layout is
        // unchanged); only the render is suppressed for this colour mode.
        return Number.isFinite(v) ? rampColor01(clamp01((v - lo) / span)) : [0, 0, 0, 0];
      });
      updateLegend('childreal', 'Legend (Child share — SCB DESO, real)', 'Fewer → More children');
    } else if (DR_DEMO_COLOR_BY[colorBy]) {
      // Color by a synthetic per-building demographic share so each UMAP cluster
      // can be NAMED (e.g. "this band is elderly-heavy"). Min–max normalised over
      // the sample. NB synthetic spread around the DESO mean — read at the cluster
      // level, not as a per-building fact.
      const spec = DR_DEMO_COLOR_BY[colorBy];
      const get = (r) => Number(r[spec.key]);
      const vals = pickedRows.map(get).filter(Number.isFinite);
      const [lo, hi] = robustLoHi(vals);
      const span = (hi - lo) || 1;
      colors = pickedRows.map((r) => {
        const v = get(r);
        return Number.isFinite(v) ? rampColor01(clamp01((v - lo) / span)) : grey(120);
      });
      updateLegend(colorBy, `Legend (${spec.label})`, spec.dir);
    } else if (colorBy === 'year') {
      colors = pickedRows.map((r) => Number.isFinite(r.yearLike) ? rampColor01(r.yearLike) : grey(110));
      updateLegend('year', `Legend (${mode} temporal proxy)`, 'Low (left) → High (right)');
    } else {
      colors = pickedRows.map((r) => Number.isFinite(r.heightLike) ? rampColor01(r.heightLike) : grey(110));
      updateLegend('height', `Legend (${mode} size proxy)`, 'Low (left) → High (right)');
    }

  return {
      X,
      colors,
      sampleCount: pickedSample.length,
      dims: X[0]?.length || 0,
      sample: pickedSample,
      metrics: {
        // Height/footprint are per-BUILDING facts only. In aggregate (mezo/district)
        // modes these fields carry no real height/area (they held building count),
        // so emit NaN → the contrastive/EBM drop them instead of showing a bar
        // mislabeled "Height (m)" / "Footprint area".
        heights: (mode === 'building') ? pickedRows.map(r => r.heightLike) : pickedRows.map(() => NaN),
        years: pickedRows.map(r => r.yearRaw),
        overall: pickedRows.map(r => r.overall),
        areaLog: (mode === 'building') ? pickedRows.map(r => r.areaLike) : pickedRows.map(() => NaN),
        changeScore: pickedRows.map(r => r.changeLike),
        fairGrocery: pickedRows.map(r => r.grocery),
        fairHospital: pickedRows.map(r => r.hospital),
        fairPrimary: pickedRows.map(r => r.primary),
        fairPharmacy:    pickedRows.map(r => r.pharmacy),
        fairHealthcare:  pickedRows.map(r => r.healthcare),
        fairKindergarten:pickedRows.map(r => r.kindergarten),
        fairSchoolHigh:  pickedRows.map(r => r.schoolHigh),
        fairUniversity:  pickedRows.map(r => r.university),
        fairDentistry:   pickedRows.map(r => r.dentistry),
        fairVeterinary:  pickedRows.map(r => r.veterinary),
        // RAW network distance (m) per service — the distance model, where the
        // child-share coupling is non-tautological (university/hospital lead).
        distGrocery:     pickedRows.map(r => r.distGrocery),
        distHospital:    pickedRows.map(r => r.distHospital),
        distHealthcare:  pickedRows.map(r => r.distHealthcare),
        distPharmacy:    pickedRows.map(r => r.distPharmacy),
        distVeterinary:  pickedRows.map(r => r.distVeterinary),
        distUniversity:  pickedRows.map(r => r.distUniversity),
        distSchoolHigh:  pickedRows.map(r => r.distSchoolHigh),
        distPrimary:     pickedRows.map(r => r.distPrimary),
        distKindergarten:pickedRows.map(r => r.distKindergarten),
        distDentistry:   pickedRows.map(r => r.distDentistry),
        categoryCode: pickedRows.map(r => r.categoryLike),
        isChange: pickedRows.map(r => r.isChangeLike),
        // Per-category 2SFCA SUPPLY provision (0..1) — the genuinely multivariate
        // supply signal (proximity above is ~1-D). Populated in every scale's row
        // builder; absent rows fall to null → ignored by the contrastive/EBM.
        ...Object.fromEntries(Object.values(DR_SUPPLY_KEY_BY_CAT).map(k => [k, pickedRows.map(r => r[k])])),
        // Mismatch (proximity − supply): overall feeds the DR projection + EBM;
        // per-category feed the contrastive so a selection's crowding can be
        // attributed to specific services. Absent rows are null → ignored.
        mismatch: pickedRows.map(r => r.mismatch),
        ...Object.fromEntries(Object.keys(DR_MISMATCH_PAIRS).map(k => [k, pickedRows.map(r => r[k])])),
        // Demographic metrics (district mode only; undefined elsewhere -> ignored
        // by computeFeatureDifferences' Number.isFinite filter). Includes the
        // composite need index used by need-weighted fairness.
        ...Object.fromEntries(
          (typeof DEMOGRAPHIC_FEATURES !== 'undefined' ? DEMOGRAPHIC_FEATURES : [])
            .map(f => [f.key, pickedRows.map(r => r[f.key])])
        ),
        demNeed: pickedRows.map(r => r.demNeed),
        // Rich building-level features (modal-gap + built form + demo)
        ...Object.fromEntries(
          (typeof DR_RICH_FEATURES !== 'undefined' ? DR_RICH_FEATURES : [])
            .map(f => [f.key, pickedRows.map(r => r[f.key])])
        )
      },
      featureLabels: keys.map(key => labels[key]),
      mode
    };
  };

  if (mode === 'district') {
    if (!districtFC?.features?.length) throw new Error('No districts loaded.');
    const sample = districtFC.features;
    const demoFeats = (typeof DEMOGRAPHIC_FEATURES !== 'undefined') ? DEMOGRAPHIC_FEATURES : [];
    const rows = sample.map((f) => {
      const props = f?.properties || {};
      const byCat = props.__fairByCat || {};
      const areaSqKm = turf.area(f) / 1e6;
      const demo = props.__demo || {};
      const row = {
        overall: Number(props.__fairOverall),
        focused: Number(props.__score),
        // Real DESO child share aggregated to the district (color-only, like the
        // building path) — lets the macro DR colour districts by child share.
        childReal: Number.isFinite(props.__childReal) ? props.__childReal : null,
        grocery: Number(byCat.grocery),
        hospital: Number(byCat.hospital),
        primary: Number(byCat.school_primary),
        pharmacy: Number(byCat.pharmacy),
        healthcare: Number(byCat.healthcare_center),
        kindergarten: Number(byCat.kindergarten),
        schoolHigh: Number(byCat.school_high),
        university: Number(byCat.university),
        dentistry: Number(byCat.dentistry),
        veterinary: Number(byCat.veterinary),
        areaLike: Number.isFinite(areaSqKm) ? Math.log1p(areaSqKm) : null,
        heightLike: Number(props.__count),
        yearLike: Number(props.__fairFocused),
        yearRaw: null,
        categoryLike: Number(props.__count),
        changeLike: 0,
        isChangeLike: 0,
        demNeed: Number.isFinite(props.__needZ) ? props.__needZ : null
      };
      demoFeats.forEach((d) => {
        const v = Number(demo[d.json]);
        row[d.key] = Number.isFinite(v) ? v : null;
      });
      // 2SFCA supply provision aggregated over the district's baked buildings.
      const supAgg = (typeof access2sfcaAggregateForPolygon === 'function') ? access2sfcaAggregateForPolygon(f) : null;
      Object.assign(row, supplyRowFromCats(supAgg && supAgg.cats, 'normMean'));
      // Mismatch (proximity − supply) per service + overall — the residual axis.
      Object.assign(row, mismatchRowFromRow(row));
      return row;
    });
    const districtLabels = {
      overall: 'overall fairness (z)',
      focused: 'focused fairness (z)',
      grocery: 'grocery fairness (z)',
      hospital: 'hospital fairness (z)',
      primary: 'primary school fairness (z)',
      pharmacy: 'pharmacy fairness (z)',
      healthcare: 'healthcare center fairness (z)',
      kindergarten: 'kindergarten fairness (z)',
      schoolHigh: 'high school fairness (z)',
      university: 'university fairness (z)',
      dentistry: 'dentistry fairness (z)',
      veterinary: 'veterinary fairness (z)',
      areaLike: 'log(area km²) (z)',
      heightLike: 'building count (z)'
    };
    // Add demographic columns only if at least one district carries them, so the
    // matrix never gains dead all-zero dimensions when demographics are absent.
    const hasDemo = rows.some(r => demoFeats.some(d => Number.isFinite(r[d.key])));
    if (hasDemo) {
      demoFeats.forEach((d) => { districtLabels[d.key] = d.label; });
      if (rows.some(r => Number.isFinite(r.demNeed))) {
        districtLabels.demNeed = 'Need index (dem)';
      }
    }
    const hasSupply = rows.some(r => Object.values(DR_SUPPLY_KEY_BY_CAT).some(k => Number.isFinite(r[k])));
    if (hasSupply) {
      Object.entries(DR_SUPPLY_LABELS).forEach(([k, lab]) => { districtLabels[k] = lab; });
      if (rows.some(r => Number.isFinite(r.mismatch))) districtLabels.mismatch = 'overall mismatch (prox−supply)';
    }
    return makeMatrix(sample, rows, districtLabels);
  }

  if (mode === 'mezo') {
    if (!mezoHexData?.length) throw new Error('No mezo cells available.');
    // Only embed POPULATED cells (real accessibility data). Empty coverage cells
    // (no buildings → all-null features) collapse to one degenerate blob, add no
    // information, and inflate the UMAP cost. Dropping them declutters the DR and
    // speeds it up — it discards NO real data (empty cells have no residents).
    const sample = mezoHexData.filter(c => Number.isFinite(c?.__fairOverall) || Number.isFinite(c?.__score));
    if (!sample.length) throw new Error('No populated mezo cells available.');
    // Per-cell raw NETWORK distance (m): aggregate each building's own distance into
    // its hex cell (one pass, on Run, ~0.2 s). Buildings resolve to their OWN matrix
    // row (~100% coverage) — far better than snapping a 250 m cell centroid, which
    // usually lands >45 m from any baked key (~36% coverage). This gives the everyday
    // "nearer to family cells" tier accurately. (Note: the regional university/hospital
    // "farther" tier is a BUILDING-scale finding — it averages out at 250 m cells.)
    const h3lib = window.h3;
    const mezoRes = (typeof resolveMezoResolution === 'function') ? resolveMezoResolution() : null;
    const distReady = (typeof routingReady === 'function' && routingReady()
      && typeof rawDistsForFeature === 'function' && typeof h3LatLngToCell === 'function'
      && h3lib && mezoRes != null && baseCityFC?.features?.length);
    const distAgg = new Map(); // hex -> { s:{key:sum}, c:{key:count} }
    if (distReady) {
      const distKeys = Object.values(DR_DIST_KEY_BY_CAT);
      for (const f of baseCityFC.features) {
        const rd = rawDistsForFeature(f);
        let lon = NaN, lat = NaN;
        try {
          const g = f.geometry; let cs = null;
          if (g) { if (g.type === 'Polygon') cs = g.coordinates[0]; else if (g.type === 'MultiPolygon') cs = g.coordinates[0] && g.coordinates[0][0]; }
          if (cs && cs.length) { let x = 0, y = 0, n = 0; for (const c of cs) { x += c[0]; y += c[1]; n++; } if (n) { lon = x / n; lat = y / n; } }
        } catch (_) {}
        if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
        let hx = null;
        try { hx = h3LatLngToCell(h3lib, lat, lon, mezoRes); } catch (_) {}
        if (!hx) continue;
        let a = distAgg.get(hx);
        if (!a) { a = { s: {}, c: {} }; distAgg.set(hx, a); }
        for (const k of distKeys) {
          const v = Number(rd[k]);
          if (Number.isFinite(v)) { a.s[k] = (a.s[k] || 0) + v; a.c[k] = (a.c[k] || 0) + 1; }
        }
      }
    }
    const cellDists = (cell) => {
      const out = {};
      const a = distAgg.get(cell?.hex);
      if (!a) return out;
      for (const k of Object.values(DR_DIST_KEY_BY_CAT)) {
        const c = a.c[k];
        out[k] = c ? a.s[k] / c : null;
      }
      return out;
    };
    const rows = sample.map((cell) => {
      const byCat = cell?.__fairByCat || {};
      const rd = cellDists(cell);
      const row = {
        overall: Number(cell?.__fairOverall),
        focused: Number(cell?.__score),
        // Real DESO child share aggregated to the cell (color-only, like the
        // building/district paths) — lets the mezo DR colour cells by child share.
        childReal: Number.isFinite(cell?.__childReal) ? cell.__childReal : null,
        grocery: Number(byCat.grocery),
        hospital: Number(byCat.hospital),
        primary: Number(byCat.school_primary),
        pharmacy: Number(byCat.pharmacy),
        healthcare: Number(byCat.healthcare_center),
        kindergarten: Number(byCat.kindergarten),
        schoolHigh: Number(byCat.school_high),
        // Full 10-service parity with building mode so the contrastive/EBM can show
        // the two-tier (regional university/hospital vs everyday services) at hex scale.
        university: Number(byCat.university),
        dentistry: Number(byCat.dentistry),
        veterinary: Number(byCat.veterinary),
        // No per-cell building height/area: these fields used to hold building COUNT,
        // which surfaced mislabeled as "Height (m)" / "Footprint area". Leave null.
        areaLike: null,
        heightLike: null,
        yearLike: Number(cell?.__fairFocused),
        yearRaw: null,
        categoryLike: Number(cell?.__count),
        changeLike: 0,
        isChangeLike: 0
      };
      Object.values(DR_DIST_KEY_BY_CAT).forEach((k) => {
        const v = Number(rd[k]);
        row[k] = Number.isFinite(v) ? v : null;
      });
      // 2SFCA supply provision aggregated over the cell's baked buildings.
      const supAgg = (typeof access2sfcaAggregateForHex === 'function' && cell?.hex) ? access2sfcaAggregateForHex(cell.hex) : null;
      Object.assign(row, supplyRowFromCats(supAgg && supAgg.cats, 'normMean'));
      // Mismatch (proximity − supply) per service + overall — the residual axis.
      Object.assign(row, mismatchRowFromRow(row));
      return row;
    });
    const mezoLabels = {
      overall: 'overall fairness (z)',
      focused: 'focused fairness (z)',
      grocery: 'grocery fairness (z)',
      hospital: 'hospital fairness (z)',
      primary: 'primary school fairness (z)',
      pharmacy: 'pharmacy fairness (z)',
      healthcare: 'healthcare center fairness (z)',
      kindergarten: 'kindergarten fairness (z)',
      schoolHigh: 'high school fairness (z)',
      university: 'university fairness (z)',
      dentistry: 'dentistry fairness (z)',
      veterinary: 'veterinary fairness (z)'
    };
    // Raw-distance (m) dims — added only if routing matrices resolved (else the
    // matrix never gains dead all-null columns). The " distance (m)" suffix is how
    // drFeatureMode.js keeps them classed as access features.
    const DR_DIST_LABELS_MEZO = {
      distGrocery: 'grocery distance (m)', distHospital: 'hospital distance (m)',
      distHealthcare: 'healthcare distance (m)', distPharmacy: 'pharmacy distance (m)',
      distVeterinary: 'veterinary distance (m)', distUniversity: 'university distance (m)',
      distSchoolHigh: 'high school distance (m)', distPrimary: 'primary school distance (m)',
      distKindergarten: 'kindergarten distance (m)', distDentistry: 'dentistry distance (m)'
    };
    const hasRawDist = rows.some(r => Object.keys(DR_DIST_LABELS_MEZO).some(k => Number.isFinite(r[k])));
    if (hasRawDist) Object.entries(DR_DIST_LABELS_MEZO).forEach(([k, lab]) => { mezoLabels[k] = lab; });
    const hasSupply = rows.some(r => Object.values(DR_SUPPLY_KEY_BY_CAT).some(k => Number.isFinite(r[k])));
    if (hasSupply) {
      Object.entries(DR_SUPPLY_LABELS).forEach(([k, lab]) => { mezoLabels[k] = lab; });
      if (rows.some(r => Number.isFinite(r.mismatch))) mezoLabels.mismatch = 'overall mismatch (prox−supply)';
    }
    return makeMatrix(sample, rows, mezoLabels);
  }

  if (!baseCityFC?.features?.length) throw new Error('No buildings loaded.');

  // Drop accessory structures (garages/sheds — Komplementbyggnad) from the
  // building set: they aren't dwellings and shouldn't be counted as buildings
  // or padded into the synthetic axes. They remain on the map (see
  // isAccessoryBuilding). This also fixes the "of N buildings" count.
  const sample = (typeof isAccessoryBuilding === 'function')
    ? baseCityFC.features.filter(f => !isAccessoryBuilding(f.properties))
    : baseCityFC.features;
  const richFeats = (typeof DR_RICH_FEATURES !== 'undefined') ? DR_RICH_FEATURES : [];
  const synthDemoFeats = (typeof DR_SYNTH_DEMO_FEATURES !== 'undefined') ? DR_SYNTH_DEMO_FEATURES : [];
  const rows = sample.map((f) => {
    const props = f.properties || {};
    const built = getBuiltYear(props);
    const fm = props.fair_multi || {};
    const rich = props.__drRich || null;
    const row = {
      overall: Number(props.fair_overall?.score),
      focused: Number(props.fair?.score),
      grocery: Number(fm.grocery?.score),
      hospital: Number(fm.hospital?.score),
      primary: Number(fm.school_primary?.score),
      pharmacy: Number(fm.pharmacy?.score),
      healthcare: Number(fm.healthcare_center?.score),
      kindergarten: Number(fm.kindergarten?.score),
      schoolHigh: Number(fm.school_high?.score),
      university: Number(fm.university?.score),
      dentistry: Number(fm.dentistry?.score),
      veterinary: Number(fm.veterinary?.score),
      areaLike: logAreaOfFeature(f),
      heightLike: clampElev(props.height_m ?? props._mean ?? props.hojd ?? props.Hojd),
      yearLike: Number.isFinite(built) ? built : null,
      yearRaw: Number.isFinite(built) ? built : null,
      categoryLike: (sourceMode === 's1')
        ? localUsageCode(props)
        : (sourceMode === 'osm_s1' ? hybridCategoryCode(props) : osmCategoryCode(props)),
      changeLike: Number(props.change_score),
      isChangeLike: props.is_change ? 1 : 0
    };
    richFeats.forEach((rf) => {
      const v = rich ? Number(rich[rf.json]) : NaN;
      row[rf.key] = Number.isFinite(v) ? v : null;
    });
    synthDemoFeats.forEach((sf) => {
      const v = Number(props[sf.prop]);
      row[sf.key] = Number.isFinite(v) ? (sf.log ? Math.log1p(Math.max(0, v)) : v) : null;
    });
    // REAL SCB child share of the building's DESO (color-only — deliberately NOT
    // added to buildingLabels, so it never enters the embedding/EBM matrix and
    // selecting high-child areas can't trivially "explain itself"). This is the
    // measured DESO value, not the synthetic per-building spread (__synthChild).
    let childReal = NaN;
    if (typeof epiDesoProps === 'function' && props.__deso != null) {
      const dp = epiDesoProps(props.__deso);
      childReal = dp ? Number(dp.child_frac) : NaN;
    }
    row.childReal = Number.isFinite(childReal) ? childReal : null;
    // RAW network distance (m) to nearest of each service — the distance model.
    const rd = rawDistsForFeature(f);
    Object.values(DR_DIST_KEY_BY_CAT).forEach((k) => {
      const v = Number(rd[k]);
      row[k] = Number.isFinite(v) ? v : null;
    });
    // 2SFCA supply provision (0..1) per service for this building.
    Object.assign(row, supplyValsForFeature(f));
    // Mismatch (proximity − supply) per service + overall — the residual axis.
    Object.assign(row, mismatchRowFromRow(row));
    return row;
  });

  const buildingLabels = {
    heightLike: 'height (z)',
    yearLike: 'built-year (z)',
    overall: 'overall fairness (z)',
    areaLike: 'log(area) (z)',
    grocery: 'grocery fairness (z)',
    hospital: 'hospital fairness (z)',
    primary: 'primary school fairness (z)',
    pharmacy: 'pharmacy fairness (z)',
    healthcare: 'healthcare center fairness (z)',
    kindergarten: 'kindergarten fairness (z)',
    schoolHigh: 'high school fairness (z)',
    university: 'university fairness (z)',
    dentistry: 'dentistry fairness (z)',
    veterinary: 'veterinary fairness (z)',
    categoryLike: 'category code (z)'
  };
  // Add the baked rich dimensions only if at least one sampled building has them.
  const hasRich = rows.some(r => richFeats.some(rf => Number.isFinite(r[rf.key])));
  if (hasRich) richFeats.forEach((rf) => { buildingLabels[rf.key] = rf.label; });

  // Add synthetic demographic shares only if present (all cities once synthpop
  // is loaded). These give UMAP/PCP real equity-relevant structure.
  const hasSynthDemo = rows.some(r => synthDemoFeats.some(sf => Number.isFinite(r[sf.key])));
  if (hasSynthDemo) synthDemoFeats.forEach((sf) => { buildingLabels[sf.key] = sf.label; });

  // Add the raw-distance (m) features only if routing matrices were loaded (so
  // the matrix never gains dead all-null dims). The " distance (m)" suffix is how
  // drFeatureMode.js keeps them as access features.
  const DR_DIST_LABELS = {
    distGrocery: 'grocery distance (m)', distHospital: 'hospital distance (m)',
    distHealthcare: 'healthcare distance (m)', distPharmacy: 'pharmacy distance (m)',
    distVeterinary: 'veterinary distance (m)', distUniversity: 'university distance (m)',
    distSchoolHigh: 'high school distance (m)', distPrimary: 'primary school distance (m)',
    distKindergarten: 'kindergarten distance (m)', distDentistry: 'dentistry distance (m)'
  };
  const hasRawDist = rows.some(r => Object.keys(DR_DIST_LABELS).some(k => Number.isFinite(r[k])));
  if (hasRawDist) Object.entries(DR_DIST_LABELS).forEach(([k, lab]) => { buildingLabels[k] = lab; });

  // Add the 2SFCA supply dims only if the layer resolved (else no dead columns).
  const hasSupply = rows.some(r => Object.values(DR_SUPPLY_KEY_BY_CAT).some(k => Number.isFinite(r[k])));
  if (hasSupply) {
    Object.entries(DR_SUPPLY_LABELS).forEach(([k, lab]) => { buildingLabels[k] = lab; });
    // Overall mismatch = the map-invisible residual axis for the "Supply +
    // Mismatch" (supply_plus) set; per-category mismatch stays in the contrastive.
    if (rows.some(r => Number.isFinite(r.mismatch))) buildingLabels.mismatch = 'overall mismatch (prox−supply)';
  }

  return makeMatrix(sample, rows, buildingLabels);
}

function updateLegend(kind, title, text) {
  const { legendTitle, legendText } = ensureDRUI();
  if (legendTitle) legendTitle.textContent = title;
  if (legendText)  legendText.textContent  = text;
}

/* ---------- PCA ---------- */
function meanCenter(X) {
  const n = X.length, d = X[0].length;
  const mu = new Array(d).fill(0);
  for (let i=0;i<n;i++) for (let j=0;j<d;j++) mu[j]+=X[i][j];
  for (let j=0;j<d;j++) mu[j]/=n;
  const Y = new Array(n);
  for (let i=0;i<n;i++) {
    const row = new Array(d);
    for (let j=0;j<d;j++) row[j]=X[i][j]-mu[j];
    Y[i]=row;
  }
  return { Y, mu };
}
function covMatrix(Y) {
  const n = Y.length, d = Y[0].length;
  const C = Array.from({length:d},()=>new Array(d).fill(0));
  for (let i=0;i<n;i++) {
    const r=Y[i];
    for (let a=0;a<d;a++) for (let b=a;b<d;b++) C[a][b]+=r[a]*r[b];
  }
  for (let a=0;a<d;a++) for (let b=a;b<d;b++) C[b][a]=C[a][b];
  return C;
}
function matVec(C,v){ const d=C.length; const out=new Array(d).fill(0); for (let i=0;i<d;i++){ let s=0; for (let j=0;j<d;j++) s+=C[i][j]*v[j]; out[i]=s; } return out; }
function dot(a,b){ let s=0; for (let i=0;i<a.length;i++) s+=a[i]*b[i]; return s; }
function vecNorm(v){ let s=0; for (let i=0;i<v.length;i++) s+=v[i]*v[i]; return Math.sqrt(s); }
function powerIter(C, iters=500, eps=1e-9) {
  const d=C.length; let v=new Array(d).fill(0).map(()=>Math.random()-0.5);
  let nrm=vecNorm(v); for (let i=0;i<d;i++) v[i]/=nrm;
  let lambda=0;
  for (let k=0;k<iters;k++){
    const Cv=matVec(C,v);
    const n=vecNorm(Cv);
    if (n<1e-12) break;
    for (let i=0;i<d;i++) v[i]=Cv[i]/n;
    const lam=dot(v, matVec(C,v));
    if (Math.abs(lam-lambda)<eps) break;
    lambda=lam;
  }
  return { vec:v, val:dot(v, matVec(C,v)) };
}
function deflate(C, vec, val) {
  const d=C.length;
  for (let i=0;i<d;i++) for (let j=0;j<d;j++) C[i][j]-=val*vec[i]*vec[j];
  return C;
}
function runPCA(X) {
  const { Y } = meanCenter(X);
  const C = covMatrix(Y);
  const pc1 = powerIter(C, 500).vec;
  const val1 = dot(pc1, matVec(C,pc1));
  deflate(C, pc1, val1);
  const pc2 = powerIter(C, 500).vec;
  const pts = Y.map(r => [dot(r, pc1), dot(r, pc2)]);
  return pts;
}

/* ---------- UMAP ---------- */
async function runUMAP(X, {nNeighbors=15, minDist=0.1, nEpochs=200} = {}) {
  const ok = await ensureUMAP();
  if (!ok) throw new Error('UMAP library not loaded');
  const umap = new window.UMAP({ nNeighbors, minDist, nEpochs, random: Math.random });
  const Y = await umap.fitAsync(X);
  return Y;
}

/* ---------- Plot + selection drawing ---------- */
function computeScreenPositions(points) {
  const xs = points.map(p=>p[0]), ys = points.map(p=>p[1]);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const pad = 20, W = drPlot.width, H = drPlot.height;

  drPlot.minX=minX; drPlot.maxX=maxX; drPlot.minY=minY; drPlot.maxY=maxY; drPlot.pad=pad;

  const screenXY = new Array(points.length);
  for (let i=0;i<points.length;i++) {
    const x = points[i][0], y = points[i][1];
    const nx = (x - minX) / Math.max(1e-9, (maxX-minX));
    const ny = (y - minY) / Math.max(1e-9, (maxY-minY));
    const cx = pad + nx*(W - 2*pad);
    const cy = pad + (1-ny)*(H - 2*pad);
    screenXY[i] = [cx, cy];
  }
  return screenXY;
}

function redrawDR(selectedIdx = null) {
  const { canvas } = ensureDRUI();
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  resizeDRCanvas();

  if (!drPlot.points || !drPlot.colors) return;

  const mode = currentDRDataMode();
  const dotSize = DR_DOT_SIZE_BY_MODE[mode] || DR_DOT_SIZE_BY_MODE.building;
  const normalSize = Math.max(1, Math.round(dotSize.normal));
  const selectedSize = Math.max(normalSize + 1, Math.round(dotSize.selected));
  const normalHalf = Math.floor(normalSize / 2);
  const selectedHalf = Math.floor(selectedSize / 2);

  const baseAlpha = 0.35;
  const [ur, ug, ub] = DR_UNSELECTED_COLOR;
  // A fully-transparent colour (alpha 0) marks a HIDDEN point — e.g. a cell with
  // no measured value under a demographic colour. Skip it here too, otherwise the
  // base layer would still draw a faint dot for it.
  const isHidden = (i) => { const c = drPlot.colors[i]; return !!c && c[3] === 0; };
  for (let i=0;i<drPlot.screenXY.length;i++) {
    if (isHidden(i)) continue;
    const [cx, cy] = drPlot.screenXY[i];
    ctx.fillStyle = `rgba(${ur},${ug},${ub},${baseAlpha})`;
    ctx.fillRect(Math.round(cx) - normalHalf, Math.round(cy) - normalHalf, normalSize, normalSize);
  }

  if (selectedIdx && selectedIdx.length) {
    for (const i of selectedIdx) {
      const [cx, cy] = drPlot.screenXY[i];
      ctx.fillStyle = 'rgba(241,105,19,1)';
      ctx.fillRect(Math.round(cx) - selectedHalf, Math.round(cy) - selectedHalf, selectedSize, selectedSize);
    }
  } else {
    for (let i=0;i<drPlot.screenXY.length;i++) {
      const [cx, cy] = drPlot.screenXY[i];
      const c = drPlot.colors[i] || [255,255,255,255];
      ctx.fillStyle = `rgba(${c[0]},${c[1]},${c[2]},${(c[3]??255)/255})`;
      ctx.fillRect(Math.round(cx) - normalHalf, Math.round(cy) - normalHalf, normalSize, normalSize);
    }
  }
}

/* ---------- Lasso (d3) ---------- */
const lasso = {
  active: false,
  drawing: false,
  points: [],
  selectedIdx: [],
  path: null,
  appendMode: false,

  // marquee (rectangular) selection state
  marqueeDrawing: false,
  marqueeStart: null,   // [x,y] at Shift+drag start
  marqueeRect: null,     // d3 <rect> overlay while dragging
};

function setLassoActive(active) {
  lasso.active = !!active;

  if (!lasso.active) {
    lasso.drawing = false;
    lasso.points = [];
    lasso.appendMode = false;
    lasso.marqueeDrawing = false;
    lasso.marqueeStart = null;
  }

  const { lassoBtn } = ensureDRUI();
  if (lassoBtn) {
    lassoBtn.classList.toggle('btn-secondary', lasso.active);
    lassoBtn.classList.toggle('btn-outline-secondary', !lasso.active);
  }

  if (!lasso.active) {
    const svgEl = document.getElementById('drOverlay');
    if (svgEl && typeof d3 !== 'undefined') {
      d3.select(svgEl).selectAll('*').remove();
    }
  }
}


function toggleLasso() {
  setLassoActive(!lasso.active);
  initD3Overlay();
}

function initD3Overlay() {
  const svgEl = document.getElementById('drOverlay');
  if (!svgEl || typeof d3 === 'undefined') return;

  const svg = d3.select(svgEl);
  svg.selectAll('*').remove();

  if (lasso.selectedIdx.length) drawSelectionHull(svg);

  const getXY = (event) => {
    const rect = svgEl.getBoundingClientRect();
    return [event.clientX - rect.left, event.clientY - rect.top];
  };

  // ===== off-centred pointer helper ==================================
  function updateOffcenterPointer(x, y) {
    // No DR result yet → nothing to show
    if (!drPlot.screenXY || !drPlot.screenXY.length) {
      svg.selectAll('.off-pointer').remove();
      return;
    }

    // If user is currently drawing lasso / marquee, hide the pointer overlay
    if (lasso.drawing || lasso.marqueeDrawing) {
      svg.selectAll('.off-pointer').remove();
      return;
    }

    const pts = drPlot.screenXY;
    let bestIdx = -1;
    let bestD2 = Infinity;

    for (let i = 0; i < pts.length; i++) {
      const dx = x - pts[i][0];
      const dy = y - pts[i][1];
      const d2 = dx * dx + dy * dy;
      if (d2 < bestD2) {
        bestD2 = d2;
        bestIdx = i;
      }
    }

    // If the nearest point is too far from the cursor, hide overlay
    const MAX_DIST2 = 30 * 30; // ~30px radius
    if (bestIdx < 0 || bestD2 > MAX_DIST2) {
      svg.selectAll('.off-pointer').remove();
      return;
    }

    const [px, py] = pts[bestIdx];
    const feat = drPlot.sample ? drPlot.sample[bestIdx] : null;
    const props = (feat && feat.properties) || {};

    const name =
      props.name ||
      props.category ||
      props.objekttyp ||
      `Building #${bestIdx + 1}`;

    const heights = drPlot.metrics?.heights || [];
    const overall = drPlot.metrics?.overall || [];
    const h = Number.isFinite(heights[bestIdx]) ? heights[bestIdx].toFixed(1) : '—';
    const o = Number.isFinite(overall[bestIdx]) ? overall[bestIdx].toFixed(2) : '—';

    const labelText = `${name}  |  h≈${h} m  |  overall≈${o}`;

    // Position label near the mouse, not on top of the dense cluster
    const labelX = x + 16;
    const labelY = y - 16;

    const g = svg.selectAll('g.off-pointer').data([1]);
    const gEnter = g.enter().append('g').attr('class', 'off-pointer');
    gEnter.append('line').attr('class', 'off-pointer-link');
    gEnter.append('circle').attr('class', 'off-pointer-circle');
    gEnter.append('rect').attr('class', 'off-pointer-label-bg');
    gEnter.append('text').attr('class', 'off-pointer-label-text');
    const gAll = gEnter.merge(g);

    // line from mouse → actual point
    gAll.select('.off-pointer-link')
      .attr('x1', x)
      .attr('y1', y)
      .attr('x2', px)
      .attr('y2', py)
      .attr('stroke', 'rgba(255,255,255,0.6)')
      .attr('stroke-width', 1.2)
      .attr('pointer-events', 'none');

    // highlight circle on the true point
    gAll.select('.off-pointer-circle')
      .attr('cx', px)
      .attr('cy', py)
      .attr('r', 5)
      .attr('fill', 'rgba(255,255,255,0.95)')
      .attr('stroke', 'rgba(0,0,0,0.85)')
      .attr('stroke-width', 1.5)
      .attr('pointer-events', 'none');

    const text = gAll.select('.off-pointer-label-text')
      .attr('x', labelX + 8)
      .attr('y', labelY + 12)
      .attr('fill', '#ffffff')
      .attr('font-size', 10)
      .text(labelText);

    // Background rounded rect sized to the text
    const node = text.node();
    if (node) {
      const bbox = node.getBBox();
      gAll.select('.off-pointer-label-bg')
        .attr('x', bbox.x - 4)
        .attr('y', bbox.y - 2)
        .attr('width', bbox.width + 8)
        .attr('height', bbox.height + 4)
        .attr('rx', 4)
        .attr('ry', 4)
        .attr('fill', 'rgba(0,0,0,0.85)')
        .attr('stroke', 'rgba(255,255,255,0.4)')
        .attr('stroke-width', 0.8)
        .attr('pointer-events', 'none');
    }
  }
  // ======================================================================

  function onDown(event) {
    if (!lasso.active || (event.button != null && event.button !== 0)) return;
    event.preventDefault();
    lasso.appendMode = isAdditiveSelectionEvent(event);

    const useMarquee = !!event.shiftKey; // SHIFT + drag → rectangular
    if (useMarquee) {
      // start marquee
      lasso.marqueeDrawing = true;
      lasso.drawing = false;
      lasso.marqueeStart = getXY(event);

      if (lasso.marqueeRect) {
        lasso.marqueeRect.remove();
        lasso.marqueeRect = null;
      }
      if (lasso.path) {
        lasso.path.remove();
        lasso.path = null;
      }

      const [x, y] = lasso.marqueeStart;
      lasso.marqueeRect = svg.append('rect')
        .attr('x', x)
        .attr('y', y)
        .attr('width', 0)
        .attr('height', 0)
        .attr('fill', 'rgba(0,150,255,0.08)')
        .attr('stroke', 'rgba(0,150,255,0.9)')
        .attr('stroke-width', 2)
        .attr('pointer-events', 'none');
    } else {
      // start free-form lasso
      lasso.drawing = true;
      lasso.marqueeDrawing = false;
      lasso.points = [getXY(event)];

      if (lasso.path) lasso.path.remove();
      if (lasso.marqueeRect) {
        lasso.marqueeRect.remove();
        lasso.marqueeRect = null;
      }

      lasso.path = svg.append('path')
        .attr('fill', 'rgba(0,150,255,0.08)')
        .attr('stroke', 'rgba(0,150,255,0.9)')
        .attr('stroke-width', 2)
        .attr('d', d3.line()(lasso.points));
    }
  }

  function onMove(event) {
    const [x, y] = getXY(event);

    // always update off-centred pointer on move
    updateOffcenterPointer(x, y);

    // If we are not drawing anything, stop here
    if (!lasso.drawing && !lasso.marqueeDrawing) return;
    event.preventDefault();

    if (lasso.drawing) {
      // free-form lasso
      lasso.points.push([x, y]);
      if (lasso.path) {
        lasso.path.attr('d', d3.line()(lasso.points));
      }
    } else if (lasso.marqueeDrawing && lasso.marqueeRect && lasso.marqueeStart) {
      // rectangular marquee
      const [x0, y0] = lasso.marqueeStart;
      const rx = Math.min(x, x0);
      const ry = Math.min(y, y0);
      const rw = Math.abs(x - x0);
      const rh = Math.abs(y - y0);
      lasso.marqueeRect
        .attr('x', rx)
        .attr('y', ry)
        .attr('width', rw)
        .attr('height', rh);
    }
  }

  function finish(event) {
    if (!lasso.drawing && !lasso.marqueeDrawing) return;
    const append = lasso.appendMode || isAdditiveSelectionEvent(event);
    lasso.appendMode = false;

    // ---- free-form lasso finish ----
    if (lasso.drawing) {
      lasso.drawing = false;
      if (lasso.points.length < 3) {
        if (lasso.path) {
          lasso.path.remove();
          lasso.path = null;
        }
        return;
      }
      const poly = lasso.points.slice();
      poly.push(poly[0]);
      computeSelection(poly, { append });
    }

    // ---- marquee finish ----
    if (lasso.marqueeDrawing) {
      lasso.marqueeDrawing = false;

      if (lasso.marqueeRect) {
        const x  = parseFloat(lasso.marqueeRect.attr('x')) || 0;
        const y  = parseFloat(lasso.marqueeRect.attr('y')) || 0;
        const w  = parseFloat(lasso.marqueeRect.attr('width')) || 0;
        const h  = parseFloat(lasso.marqueeRect.attr('height')) || 0;

        // If rectangle is tiny, treat as no selection
        if (w >= 2 && h >= 2) {
          const poly = [
            [x,       y      ],
            [x + w,   y      ],
            [x + w,   y + h  ],
            [x,       y + h  ],
            [x,       y      ]
          ];
          computeSelection(poly, { append });
        }

        lasso.marqueeRect.remove();
        lasso.marqueeRect = null;
      }
    }
  }

  svg
    .on('pointerdown', onDown)
    .on('pointermove', onMove)
    .on('pointerup', finish)
    .on('mousedown', onDown)
    .on('mousemove', onMove)
    .on('mouseup', finish)
    .on('dblclick', finish)
    .style('cursor', lasso.active ? 'crosshair' : 'default');
}



function applyDRSelection(idxArray, opts = {}) {
  const { skipMapSync = false, skipParallelSync = false, append = false } = opts;

  if (!skipParallelSync) {
    parallelCoordsForceEmptySelection = false;
  }
  const incomingSelection = Array.isArray(idxArray) ? idxArray.slice() : [];
  const selection = append
    ? Array.from(new Set([...(Array.isArray(lasso.selectedIdx) ? lasso.selectedIdx : []), ...incomingSelection]))
    : incomingSelection;

  lasso.selectedIdx = selection;
  drHasSelection = selection.length > 0;

  if (typeof d3 !== 'undefined') {
    const svgEl = document.getElementById('drOverlay');
    if (svgEl) {
      const svg = d3.select(svgEl);
      svg.selectAll('.sel-hull').remove();
      drawSelectionHull(svg);
    }
  }

  redrawDR(selection.length ? selection : null);
  renderSelectionStats(selection);

  // push this selection into the map (mark buildings as _drSelected)
  if (!skipMapSync) {
    applyDRMapSelectionFromIndices(selection);
    setMapLassoClearDisabled(selection.length === 0);
  }

  const { clearSelBtn } = ensureDRUI();
  if (clearSelBtn) clearSelBtn.disabled = selection.length === 0;

  if (!skipParallelSync && parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
}

function computeSelection(polygon, opts = {}) {
  if (!drPlot.screenXY || !polygon) return;
  const idx = [];
  for (let i = 0; i < drPlot.screenXY.length; i++) {
    if (d3.polygonContains(polygon, drPlot.screenXY[i])) idx.push(i);
  }
  applyDRSelection(idx, opts);
}



function syncDRSelectionFromMapFeature(feat, opts = {}) {
  const feats = Array.isArray(feat) ? feat.filter(Boolean) : (feat ? [feat] : []);
  const { preserveMapSelection = false, append = false } = opts;

  const { statusEl } = ensureDRUI();

  if (!feats.length || !drPlot.points || !Array.isArray(drPlot.sample)) {
    // if (statusEl) statusEl.textContent = 'Run UMAP to sync selections from the map.';
    return;
  }

  const idx = [];
  feats.forEach((f) => {
    const i = drPlot.sample.indexOf(f);
    if (i !== -1) idx.push(i);
  });

  if (!drPlot.screenXY && drPlot.points) {
    drPlot.screenXY = computeScreenPositions(drPlot.points);
  }

  if (statusEl) {
    const noun = idx.length === 1 ? 'Building' : `${idx.length} buildings`;
    // statusEl.textContent = `${noun} synced to UMAP selection.`;
  }

  prepareDRSurface();
  initD3Overlay();
  applyDRSelection(idx, { skipMapSync: preserveMapSelection, append });
}

function drawSelectionHull(svg) {
  if (!svg || typeof d3 === 'undefined') return;
  if (!lasso.selectedIdx.length) return;
  const pts = lasso.selectedIdx.map(i => drPlot.screenXY[i]);
  const hull = d3.polygonHull(pts);
  if (!hull) return;
  svg.append('path')
    .attr('class', 'sel-hull')
    .attr('d', 'M'+hull.map(p=>p.join(',')).join('L')+'Z')
    .attr('fill', 'rgba(255,215,0,0.07)')
    .attr('stroke', 'rgba(255,215,0,0.9)')
    .attr('stroke-width', 2);
}

function clearSelection() {
  setPersistentBuildingSelection([]);
  clearParallelCoordsSelectionFromClearAction();
  lasso.selectedIdx = [];
  lasso.points = [];
  lasso.drawing = false;
  lasso.marqueeDrawing = false;
  lasso.marqueeStart = null;

  if (lasso.path) {
    lasso.path.remove();
    lasso.path = null;
  }
  if (lasso.marqueeRect) {
    lasso.marqueeRect.remove();
    lasso.marqueeRect = null;
  }

  drHasSelection = false;

  const svg = (typeof d3 !== 'undefined') ? d3.select('#drOverlay') : null;
  if (svg) svg.selectAll('*').remove();

  redrawDR();
  renderSelectionStats([]);

  // also clear DR-based highlights on the map
  clearDRMapSelection();

  updateLayers();

  const { clearSelBtn } = ensureDRUI();
  if (clearSelBtn) clearSelBtn.disabled = true;
}




function renderSelectionStats(idx) {
  const { selInfoEl } = ensureDRUI();
  if (!selInfoEl) return;

  const selection = Array.isArray(idx) ? idx.slice() : [];

  // Share selection with all DR-related views
  drPlot.lastSelectionIdx = selection;

  // Reset caches for the lower Stats/Model views
  drPlot.lastFeatureDiff = null;
  drPlot.lastModelExplanation = null;

  // Reset caches for the upper EBM / contrastive engines
  drPlot.engineEBM = null;
  drPlot.engineContrast = null;

  // ---- No selection case ----
  if (!selection.length) {
    selInfoEl.textContent = 'Selection: 0 points';

    if (typeof refreshExplanationView === 'function') {
      refreshExplanationView();
    }
    if (typeof refreshEnginePlot === 'function') {
      refreshEnginePlot();
    }

    // show city-wide views only (no selection overlay)
    renderFairnessHistogram([]);
    renderDistrictBar([]);
    renderHeightFairnessScatter([]);
    renderLorenzCurve([]);
    renderThresholdBar([]);
    renderCategoryHists([]);

    return;
  }

  // ---- Selection summary (unchanged core logic) ----
  const m = drPlot.metrics || {};
  const mean = (arr, pick) => {
    const vals = selection.map(i => pick(i)).filter(Number.isFinite);
    if (!vals.length) return NaN;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  };

  const count = selection.length;
  const hMean = mean(m.heights || [], (i) => m.heights[i]);
  const oMean = mean(m.overall || [], (i) => m.overall[i]);
  const yearsKnown = selection
    .map(i => (m.years || [])[i])
    .filter(Number.isFinite);
  const yMin = yearsKnown.length ? Math.min(...yearsKnown) : null;
  const yMax = yearsKnown.length ? Math.max(...yearsKnown) : null;

  selInfoEl.textContent =
    `Selection: ${count} points` +
    (Number.isFinite(hMean) ? ` | mean height ≈ ${hMean.toFixed(1)} m` : '') +
    (Number.isFinite(oMean) ? ` | mean overall ≈ ${oMean.toFixed(2)}` : '') +
    (yearsKnown.length ? ` | built year range ${yMin}–${yMax}` : ' | built year unknown/varied');

  const diff = computeFeatureDifferences(selection);
  drPlot.lastFeatureDiff = diff;

  if (drExplainMode === 'stats') {
    renderFeatureDiffPanel(diff);
  } else if (drExplainMode === 'model') {
    ensureModelExplanation(selection);
  }

  if (typeof refreshEnginePlot === 'function') {
    refreshEnginePlot();
  }

  // all coordinated city views get selection overlay
  renderFairnessHistogram(selection);
  renderDistrictBar(selection);
  renderHeightFairnessScatter(selection);
  renderLorenzCurve(selection);
  renderThresholdBar(selection);
  renderCategoryHists(selection);
}


// ======================= Unsupervised feature differences =======================

// Which numeric metrics to compare between selection and city
const DR_FEATURE_CONFIG = [
  { key: 'heights',      label: 'Height (m)' },
  { key: 'years',        label: 'Built year' },
  { key: 'overall',      label: 'Overall fairness (0–1)' },
  { key: 'fairGrocery',  label: 'Grocery fairness' },
  { key: 'fairHospital', label: 'Hospital fairness' },
  { key: 'fairPrimary',  label: 'Primary school fairness' },
  { key: 'fairPharmacy',     label: 'Pharmacy fairness' },
  { key: 'fairHealthcare',   label: 'Healthcare center fairness' },
  { key: 'fairKindergarten', label: 'Kindergarten fairness' },
  { key: 'fairSchoolHigh',   label: 'High school fairness' },
  { key: 'fairUniversity',   label: 'University access' },
  { key: 'fairDentistry',    label: 'Dentistry access' },
  { key: 'fairVeterinary',   label: 'Veterinary access' },
  // Raw network distance (m) per service — the distance model. The child-share
  // coupling is non-tautological here (university/hospital lead), unlike the
  // gravity scores above which the everyday-service density dominates.
  { key: 'distGrocery',      label: 'Grocery distance (m)' },
  { key: 'distHospital',     label: 'Hospital distance (m)' },
  { key: 'distHealthcare',   label: 'Healthcare distance (m)' },
  { key: 'distPharmacy',     label: 'Pharmacy distance (m)' },
  { key: 'distVeterinary',   label: 'Veterinary distance (m)' },
  { key: 'distUniversity',   label: 'University distance (m)' },
  { key: 'distSchoolHigh',   label: 'High school distance (m)' },
  { key: 'distPrimary',      label: 'Primary school distance (m)' },
  { key: 'distKindergarten', label: 'Kindergarten distance (m)' },
  { key: 'distDentistry',    label: 'Dentistry distance (m)' },
  // Per-category 2SFCA SUPPLY provision (0..1; higher = better supplied). Unlike
  // the ~1-D proximity above, supply is genuinely multivariate and decorrelates
  // from proximity — the "Supply (2SFCA)" / "Access + Supply" feature sets embed
  // these so the contrastive/EBM can attribute a selection to specific services.
  { key: 'supplyGrocery',      label: 'Grocery supply (2SFCA)' },
  { key: 'supplyHospital',     label: 'Hospital supply (2SFCA)' },
  { key: 'supplyHealthcare',   label: 'Healthcare supply (2SFCA)' },
  { key: 'supplyPharmacy',     label: 'Pharmacy supply (2SFCA)' },
  { key: 'supplyVeterinary',   label: 'Veterinary supply (2SFCA)' },
  { key: 'supplyUniversity',   label: 'University supply (2SFCA)' },
  { key: 'supplySchoolHigh',   label: 'High school supply (2SFCA)' },
  { key: 'supplyPrimary',      label: 'Primary school supply (2SFCA)' },
  { key: 'supplyKindergarten', label: 'Kindergarten supply (2SFCA)' },
  { key: 'supplyDentistry',    label: 'Dentistry supply (2SFCA)' },
  // Mismatch (proximity − 2SFCA supply): overall (also the DR projection residual
  // axis + EBM feature) + per-category, so the contrastive can name which service
  // looks close but is under-supplied (positive) or is supply-rich for its
  // distance (negative) — the map-invisible signal answering R1.
  { key: 'mismatch',             label: 'Overall mismatch (proximity−supply)' },
  { key: 'mismatchGrocery',      label: 'Grocery mismatch (prox−supply)' },
  { key: 'mismatchHospital',     label: 'Hospital mismatch (prox−supply)' },
  { key: 'mismatchHealthcare',   label: 'Healthcare mismatch (prox−supply)' },
  { key: 'mismatchPharmacy',     label: 'Pharmacy mismatch (prox−supply)' },
  { key: 'mismatchVeterinary',   label: 'Veterinary mismatch (prox−supply)' },
  { key: 'mismatchUniversity',   label: 'University mismatch (prox−supply)' },
  { key: 'mismatchSchoolHigh',   label: 'High school mismatch (prox−supply)' },
  { key: 'mismatchPrimary',      label: 'Primary school mismatch (prox−supply)' },
  { key: 'mismatchKindergarten', label: 'Kindergarten mismatch (prox−supply)' },
  { key: 'mismatchDentistry',    label: 'Dentistry mismatch (prox−supply)' },
  { key: 'areaLog',      label: 'Footprint area (log m²)' },
  { key: 'changeScore',  label: 'Change score (S1)' },
  // Demographic features (district mode). Generated from DEMOGRAPHIC_FEATURES so
  // the contrastive/EBM panels rank them alongside accessibility.
  ...((typeof DEMOGRAPHIC_FEATURES !== 'undefined' ? DEMOGRAPHIC_FEATURES : [])
      .map(f => ({ key: f.key, label: f.label }))),
  { key: 'demNeed', label: 'Need index (dem)' },
  // Rich building-level features (modal-gap + built form + demo) for EBM/contrastive.
  ...((typeof DR_RICH_FEATURES !== 'undefined' ? DR_RICH_FEATURES : [])
      .map(f => ({ key: f.key, label: f.label }))),
];

// Small numeric helpers
function mean1(arr) {
  if (!arr.length) return NaN;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
function std1(arr, m) {
  if (!arr.length) return NaN;
  const mu = (m !== undefined && m !== null) ? m : mean1(arr);
  const v = arr.reduce((s, v) => s + (v - mu) * (v - mu), 0) / arr.length;
  return Math.sqrt(Math.max(v, 0));
}
function medianFromSorted(sorted) {
  const n = sorted.length;
  if (!n) return NaN;
  const mid = Math.floor(n / 2);
  if (n % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}
function quantileFromSorted(sorted, q) {
  const n = sorted.length;
  if (!n) return NaN;
  if (q <= 0) return sorted[0];
  if (q >= 1) return sorted[n - 1];
  const idx = (n - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const t = idx - lo;
  return sorted[lo] * (1 - t) + sorted[hi] * t;
}
function mad1(arr, med) {
  if (!arr.length) return NaN;
  const m = Number.isFinite(med) ? med : medianFromSorted(arr.slice().sort((a, b) => a - b));
  const devs = arr.map(v => Math.abs(v - m));
  return medianFromSorted(devs.sort((a, b) => a - b));
}

// 1D Kolmogorov–Smirnov distance between two sorted arrays
function ksDistance(sortedA, sortedB) {
  const nA = sortedA.length;
  const nB = sortedB.length;
  if (!nA || !nB) return 0;
  let i = 0, j = 0;
  let cdfA = 0, cdfB = 0;
  let dMax = 0;

  while (i < nA && j < nB) {
    const a = sortedA[i];
    const b = sortedB[j];
    if (a <= b) {
      i++;
      cdfA = i / nA;
    } else {
      j++;
      cdfB = j / nB;
    }
    const diff = Math.abs(cdfA - cdfB);
    if (diff > dMax) dMax = diff;
  }
  return dMax;
}

/**
 * Compute per-feature differences between selection and "city" (DR sample).
 * Returns features sorted by |effect size| descending.
 */
function computeFeatureDifferences(selectionIdx) {
  const metrics = drPlot.metrics || {};
  if (!selectionIdx || !selectionIdx.length) {
    return { features: [] };
  }

  const feats = [];

  for (const cfg of DR_FEATURE_CONFIG) {
    const arr = metrics[cfg.key];
    if (!arr || !arr.length) continue;

    const cityVals = arr.filter(Number.isFinite);
    if (cityVals.length < 5) continue; // too little data

    const selVals = selectionIdx
      .map(i => arr[i])
      .filter(Number.isFinite);
    if (!selVals.length) continue;

    const citySorted = cityVals.slice().sort((a, b) => a - b);
    const selSorted  = selVals.slice().sort((a, b) => a - b);

    const meanCity = mean1(cityVals);
    const meanSel  = mean1(selVals);
    const stdCity  = std1(cityVals, meanCity);

    // Skip constant columns (no variance across the city): they carry no
    // contrastive signal and only render an empty 0-effect bar. This is what
    // "Change score (S1)" is with no what-if edits — a per-building what-if delta,
    // hard-0 in hex/district and 0 in building mode until something is changed.
    if (!(stdCity > 0)) continue;

    const medCity  = medianFromSorted(citySorted);
    const medSel   = medianFromSorted(selSorted);
    const madCity  = mad1(cityVals, medCity);

    const effect   = stdCity > 0 ? (meanSel - meanCity) / stdCity : 0;
    const medEff   = madCity > 0 ? (medSel - medCity) / madCity : 0;
    const ks       = ksDistance(citySorted, selSorted);

    feats.push({
      key: cfg.key,
      label: cfg.label,
      nCity: cityVals.length,
      nSel: selVals.length,
      meanCity,
      meanSel,
      stdCity,
      medCity,
      medSel,
      effect,
      medEffect: medEff,
      ks
    });
  }

  feats.sort((a, b) => Math.abs(b.effect) - Math.abs(a.effect));
  return { features: feats };
}

/**
 * Render the top feature differences into the "Stats" panel.
 */
function renderFeatureDiffPanel(diff) {
  const { explainStatsEl } = ensureDRUI();
  if (!explainStatsEl) return;

  const feats = diff && diff.features ? diff.features : [];
  if (!feats.length) {
    explainStatsEl.textContent = 'No numeric features or selection is too small.';
    return;
  }

  const top = feats.slice(0, 5); // show top 5
  let html = '<table class="table table-sm table-borderless mb-0">';
  html += '<thead><tr>' +
    '<th class="text-muted small">Feature</th>' +
    '<th class="text-muted small">Median (sel / city)</th>' +
    '<th class="text-muted small">d (mean)</th>' +
    '<th class="text-muted small">KS</th>' +
    '</tr></thead><tbody>';

  for (const f of top) {
    const medSel  = Number.isFinite(f.medSel)  ? f.medSel.toFixed(f.key === 'years' ? 0 : 2) : '—';
    const medCity = Number.isFinite(f.medCity) ? f.medCity.toFixed(f.key === 'years' ? 0 : 2) : '—';
    const dMean   = Number.isFinite(f.effect)  ? f.effect.toFixed(2) : '—';
    const ks      = Number.isFinite(f.ks)      ? f.ks.toFixed(2) : '—';

    html += `<tr>
      <td>${f.label}</td>
      <td>${medSel} / ${medCity}</td>
      <td>${dMean}</td>
      <td>${ks}</td>
    </tr>`;
  }

  html += '</tbody></table>';
  explainStatsEl.innerHTML = html;
}

// ======================= Fairness histogram (overall fairness) =======================

function renderFairnessHistogram(selectionIdx) {
  const { histWrap } = ensureDRUI();
  if (!histWrap) return;

  if (typeof d3 === 'undefined') {
    histWrap.innerHTML = '<div class="small text-muted">Histogram requires d3.js.</div>';
    return;
  }

  // Make sure tooltip absolute positioning works
  const cs = getComputedStyle(histWrap);
  if (cs.position === 'static') {
    histWrap.style.position = 'relative';
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const n = overall.length;

  if (!n) {
    histWrap.innerHTML = '<div class="small text-muted mt-3">No overall fairness values available.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const allValues = overall.filter(v => Number.isFinite(v) && v >= 0 && v <= 1);
  const selectedValues = selection.length
    ? selection
        .filter(i => i >= 0 && i < overall.length)
        .map(i => overall[i])
        .filter(v => Number.isFinite(v) && v >= 0 && v <= 1)
    : [];

  const root = d3.select(histWrap);
  root.selectAll('*').remove();

  const width  = histWrap.clientWidth || 260;
  const height = histWrap.clientHeight || 140;
  const margin = { top: 6, right: 12, bottom: 40, left: 32 };

  const svg = root.append('svg')
    .attr('width',  width)
    .attr('height', height);

  // Tooltip (inside histWrap)
  const tooltip = root.append('div')
    .attr('class', 'dr-hist-tooltip')
    .style('position', 'absolute')
    .style('pointer-events', 'none')
    .style('background', 'rgba(0,0,0,0.85)')
    .style('color', '#fff')
    .style('padding', '4px 6px')
    .style('font-size', '11px')
    .style('border-radius', '4px')
    .style('z-index', '10')
    .style('opacity', 0);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .nice()
    .range([margin.left, width - margin.right]);

  const binGen = d3.bin()
    .domain(x.domain())
    .thresholds(15);

  const binsAll = binGen(allValues);
  const binsSel = binGen(selectedValues);

  const maxCount = d3.max([
    d3.max(binsAll, d => d.length) || 0,
    d3.max(binsSel, d => d.length) || 0
  ]) || 1;

  const y = d3.scaleLinear()
    .domain([0, maxCount])
    .nice()
    .range([height - margin.bottom, margin.top]);

  const totalAll = d3.sum(binsAll, b => b.length);
  const totalSel = d3.sum(binsSel, b => b.length);

  // City-wide distribution (gray)
  svg.append('g')
    .selectAll('rect.full')
    .data(binsAll)
    .join('rect')
    .attr('class', 'full')
    .attr('x', d => x(d.x0) + 1)
    .attr('y', d => y(d.length))
    .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 2))
    .attr('height', d => y(0) - y(d.length))
    .attr('fill', '#444')
    .attr('opacity', 0.4);

  // Selection overlay (green, narrower)
  if (selectedValues.length) {
    const widthFactor = 0.6;
    svg.append('g')
      .selectAll('rect.sel')
      .data(binsSel)
      .join('rect')
      .attr('class', 'sel')
      .attr('x', d => {
        const fullWidth = x(d.x1) - x(d.x0) - 2;
        const wSel = fullWidth * widthFactor;
        return x(d.x0) + 1 + (fullWidth - wSel) / 2;
      })
      .attr('y', d => y(d.length))
      .attr('width', d => {
        const fullWidth = x(d.x1) - x(d.x0) - 2;
        return Math.max(0, fullWidth * widthFactor);
      })
      .attr('height', d => y(0) - y(d.length))
      .attr('fill', '#2ecc71')
      .attr('opacity', 0.9);
  }

  // Hover hit-rectangles per bin (full vertical span)
  const hoverData = binsAll.map((b, i) => ({
    x0: b.x0,
    x1: b.x1,
    allCount: b.length,
    selCount: binsSel[i] ? binsSel[i].length : 0
  }));

  svg.append('g')
    .selectAll('rect.hit')
    .data(hoverData)
    .enter()
    .append('rect')
    .attr('class', 'hit')
    .attr('x', d => x(d.x0) + 1)
    .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 2))
    .attr('y', margin.top)
    .attr('height', y(0) - margin.top)
    .attr('fill', 'transparent')
    .on('mousemove', (event, d) => {
      const [mx, my] = d3.pointer(event, histWrap);
      const rangeText = `${d.x0.toFixed(2)} – ${d.x1.toFixed(2)}`;
      const pctAll = totalAll ? ((d.allCount / totalAll) * 100).toFixed(1) : '0.0';
      const pctSel = totalSel ? ((d.selCount / totalSel) * 100).toFixed(1) : '0.0';

      tooltip
        .style('left', `${mx + 8}px`)
        .style('top', `${my - 10}px`)
        .style('opacity', 1)
        .html(
          `<div><strong>Fairness bin:</strong> ${rangeText}</div>` +
          `<div>City buildings: ${d.allCount} (${pctAll}% of city)</div>` +
          `<div>Selected buildings: ${d.selCount} (${pctSel}% of selection)</div>`
        );
    })
    .on('mouseleave', () => {
      tooltip.style('opacity', 0);
    });

  // X-axis
  const xAxis = d3.axisBottom(x)
    .ticks(4)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Overall fairness (0–1)');

  if (selectedValues.length) {
    svg.append('text')
      .attr('x', margin.left)
      .attr('y', margin.top + 8)
      .attr('font-size', 9)
      .attr('fill', '#ccc')
      .text(`Selection: ${selectedValues.length} of ${allValues.length} buildings`);
  } else {
    svg.append('text')
      .attr('x', margin.left)
      .attr('y', margin.top + 8)
      .attr('font-size', 9)
      .attr('fill', '#ccc')
      .text(`City-wide distribution (${allValues.length} buildings)`);
  }
}

// ======================= Parallel coordinates (POI fairness) =======================
function currentParallelCoordsMode() {
  if (districtView) return 'district';
  if (mezoView) return 'mezo';
  return 'building';
}

function currentDRDataMode() {
  return currentParallelCoordsMode();
}

function spatialModeEntityStats(mode = currentDRDataMode()) {
  if (mode === 'district') {
    return { noun: 'districts', total: (districtFC?.features || []).length };
  }
  if (mode === 'mezo') {
    return { noun: 'mezo cells', total: Array.isArray(mezoHexData) ? mezoHexData.length : 0 };
  }
  const bFeats = (typeof isAccessoryBuilding === 'function')
    ? (baseCityFC?.features || []).filter(f => !isAccessoryBuilding(f.properties))
    : (baseCityFC?.features || []);
  return { noun: 'buildings', total: bFeats.length };
}

function updateDRAndPCBadges() {
  const drBadge = document.getElementById('drMaxBadge');
  const pcBadge = document.getElementById('parallelCoordsMaxBadge');
  const mode = currentDRDataMode();
  const { noun, total } = spatialModeEntityStats(mode);

  // Default sample sizes:
  //   * DR — full count (the user explicitly wants UMAP/PCA over every
  //     building, even if it takes a while; they will trigger Run when
  //     they're ready).
  //   * PC — 4 000 by default for an interactive chart. Beside the input
  //     a "Max" button is offered (see index.html) which sets the value
  //     to the full count when clicked.
  const PC_DEFAULT_CAP = 4000;

  // --- DR input: default to total when 0 or missing ---
  const drMaxEl = document.getElementById('drMaxPts');
  let drRaw = parseInt(drMaxEl?.value || '0', 10);
  if (drRaw <= 0 || !Number.isFinite(drRaw)) {
    drRaw = total;
    if (drMaxEl) drMaxEl.value = String(drRaw);
  }
  const drLabel = `${Math.min(drRaw, total).toLocaleString()} of ${total.toLocaleString()} ${noun}`;

  // --- PC input: default to capped sample when 0 or missing ---
  const pcMaxEl = document.getElementById('parallelCoordsMaxPts');
  let pcRaw = parseInt(parallelCoordsMaxPoints, 10);
  if (pcRaw <= 0 || !Number.isFinite(pcRaw)) {
    pcRaw = Math.min(total, PC_DEFAULT_CAP);
    parallelCoordsMaxPoints = pcRaw;
    if (pcMaxEl) pcMaxEl.value = String(pcRaw);
  }
  const pcLabel = `${Math.min(pcRaw, total).toLocaleString()} of ${total.toLocaleString()} ${noun}`;

  if (drBadge) drBadge.textContent = drLabel;
  if (pcBadge) pcBadge.textContent = pcLabel;
}

function maybeRefreshDROnSpatialModeChange(prevMode) {
  updateDRAndPCBadges();
  const nextMode = currentDRDataMode();
  if (prevMode === nextMode) return;

  if (!drPlot?.points) return;
  const { statusEl } = ensureDRUI();
  if (statusEl) statusEl.textContent = `Spatial mode switched to ${nextMode}; recomputing DR…`;
  // Keep the current selection when the spatial scale changes: runDR re-projects
  // it (buildings → new-mode entities) so DR/PCP/Map stay consistent across the
  // macro/meso/micro switch instead of clearing on rerun.
  runDR({ preserveSelection: true });
}

function setParallelCoordsPending(isPending) {
  parallelCoordsPending = isPending;
  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
}

function bindParallelCoordsSourceSelect() {
  const sel = document.getElementById('parallelCoordsSource');
  if (!sel) return;
  if (!sel.__bound) {
    sel.addEventListener('change', () => {
      parallelCoordsDataSourceMode = sel.value || 'mixed';
      updateParallelCoordsPanel();
    });
    sel.__bound = true;
  }
  sel.value = parallelCoordsDataSourceMode;
}

function updateParallelCoordsDistrictFilterOptions() {
  const select = document.getElementById('parallelCoordsDistrictSelect');
  if (!select) return;

  if (!select.__bound) {
    select.addEventListener('change', () => {
      parallelCoordsDistrictFilter = select.value || '';
      updateParallelCoordsPanel();
    });
    select.__bound = true;
  }

  const feats = districtFC?.features || [];
  const options = [
    { value: '', label: 'All districts' },
    ...feats.map((feat, idx) => {
      const label = districtNameOf(feat.properties, idx) || `District ${idx + 1}`;
      return { value: label, label };
    })
  ];

  select.innerHTML = '';
  options.forEach(({ value, label }) => {
    const opt = document.createElement('option');
    opt.value = value;
    opt.textContent = label;
    select.appendChild(opt);
  });

  if (parallelCoordsDistrictFilter && !options.some(opt => opt.value === parallelCoordsDistrictFilter)) {
    parallelCoordsDistrictFilter = '';
  }
  select.value = parallelCoordsDistrictFilter;
}

function parallelCoordsModeLabel(mode) {
  if (mode === 'district') return 'districts';
  if (mode === 'mezo') return 'mezos';
  return 'buildings';
}

// --- Demographic PCP axes (district mode) ---------------------------------
// Each axis carries a normalised [0,1] value for plotting (shared y-scale) plus
// the real value for the tooltip. Built from DEMOGRAPHIC_FEATURES + need index.
function pcDemoAxes() {
  const base = (typeof DEMOGRAPHIC_FEATURES !== 'undefined' ? DEMOGRAPHIC_FEATURES : [])
    .map(f => ({ key: f.key, label: f.label.replace(/\s*\(dem\)$/i, ''), json: f.json }));
  base.push({ key: 'demNeed', label: 'Need index', json: '__needZ' });
  return base;
}
// --- Demographic PCP axes (building mode) ---------------------------------
// Building rows read demographics from their DESO (the Phase-3 baked
// deso.geojson via EPI_DEMO + the feature's stamped __deso code), so the PCP
// can show the multi-domain need index alongside per-category accessibility.
// SYNTHETIC per-building demographic axes. SCB publishes these 7 shares only per
// DESO, so tools/bake_synthpop.py SYNTHESIZES a per-building value (sampled
// around the building's DESO share with binomial-style spread, pop-weighted DESO
// mean preserved) — stamped onto features by lib/synthpop.js. So every building
// carries its OWN value and the axes vary building-by-building (not ~N_DESO
// bands). `desoProp` is the DESO field used as a fallback when a building has no
// synthetic value stamped.
const PC_BUILDING_DEMO_AXES = [
  { key: 'demChild',         label: 'Children % (synthetic)',           prop: '__synthChild',          desoProp: 'child_frac' },
  { key: 'demElder',         label: 'Elderly % (synthetic)',            prop: '__synthElder',          desoProp: 'elder_frac' },
  { key: 'demDependency',    label: 'Dependency ratio (synthetic)',     prop: '__synthDependency',     desoProp: 'dependency' },
  { key: 'demHigherEdu',     label: 'Higher-ed eligible % (synthetic)', prop: '__synthHigherEd',       desoProp: 'higher_ed' },
  { key: 'demNeet',          label: 'NEET % (synthetic)',               prop: '__synthNeet',           desoProp: 'neet' },
  { key: 'demIncomeSupport', label: 'Income support % (synthetic)',     prop: '__synthIncomeSupport',  desoProp: 'income_support' },
  { key: 'demMale',          label: 'Male % (synthetic)',               prop: '__synthMale',           desoProp: 'male_frac' },
];
// SYNTHETIC per-building axes (from EpiCity city.json, stamped onto features by
// lib/synthpop.js). Unlike the DESO axes above — which are identical for every
// building in a district, so ~N_DESO distinct polylines — these vary building by
// building, so the plot shows as many lines as buildings.
const PC_BUILDING_SYNTH_AXES = [
  { key: 'synPop',    label: 'Synthetic residents',  prop: '__synthPop' },
  { key: 'synIncome', label: 'Income (synthetic)',   prop: '__synthIncome' },
  { key: 'synNeed',   label: 'Need index (synthetic)', prop: '__synthNeed' },
  { key: 'synLevels', label: 'Storeys (synthetic)',  prop: '__synthLevels' },
  { key: 'synArea',   label: 'Footprint area (synthetic)', prop: '__synthArea' },
  { key: 'synZone',   label: 'Land-use zone (synthetic)',  prop: '__synthZone' },
];
// Synthetic per-building axes FIRST (so the per-building income/need/population
// are immediately visible next to Overall), then the DESO-inherited fractions.
function pcBuildingDemoAxes() {
  return [...PC_BUILDING_SYNTH_AXES, ...PC_BUILDING_DEMO_AXES].map(a => ({ key: a.key, label: a.label }));
}
// Maps a hex/district synthetic aggregate (from synthpopAggregate*) onto the
// building demo-axis keys, so meso/macro PCP rows share the building axes.
const PC_SYNTH_AGG_FIELD = {
  synPop: 'pop', synIncome: 'income', synNeed: 'need',
  demChild: 'child_frac', demElder: 'elder_frac', demDependency: 'dependency',
  demHigherEdu: 'higher_ed', demNeet: 'neet', demIncomeSupport: 'income_support', demMale: 'male_frac',
};
// DESO code -> baked demographic properties (built once per render; ≤ a few
// hundred DESOs). Empty when demographics aren't loaded → demo axes drop out.
function pcDesoPropsMap() {
  const m = new Map();
  const feats = (typeof EPI_DEMO !== 'undefined' && EPI_DEMO && EPI_DEMO.features) ? EPI_DEMO.features : [];
  for (const f of feats) { const p = f.properties || {}; if (p.deso != null) m.set(p.deso, p); }
  return m;
}
// Friendly axis label, demographic-aware (district + building demo axes).
function prettyParallelAxis(cat) {
  const demo = pcDemoAxes().find(a => a.key === cat)
    || PC_BUILDING_DEMO_AXES.find(a => a.key === cat)
    || PC_BUILDING_SYNTH_AXES.find(a => a.key === cat);
  if (demo) return demo.label;
  return prettyPOIName(cat);
}

function getOrderedParallelCoordsCategories(categories) {
  const unique = Array.from(new Set((categories || []).filter(Boolean)));
  const hasOverall = unique.includes(PARALLEL_COORDS_OVERALL_KEY);
  const nonOverall = unique.filter(cat => cat !== PARALLEL_COORDS_OVERALL_KEY);
  if (!nonOverall.length) return hasOverall ? [PARALLEL_COORDS_OVERALL_KEY] : [];

  const ordered = parallelCoordsColumnOrder.filter(cat => nonOverall.includes(cat));
  nonOverall.forEach((cat) => {
    if (!ordered.includes(cat)) ordered.push(cat);
  });
  return hasOverall ? [PARALLEL_COORDS_OVERALL_KEY, ...ordered] : ordered;
}

function readFairValue(entry, cat) {
  if (!entry) return null;
  const val = entry[cat];
  if (Number.isFinite(val)) return val;
  if (val && Number.isFinite(val.score)) return val.score;
  return null;
}

function getParallelCoordsDataset(mode) {
  const allCategories = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  const activeCats = resolveActivePOICategories();
  const categories = activeCats.size
    ? allCategories.filter(cat => activeCats.has(cat))
    : allCategories;
  const categoriesWithOverall = [PARALLEL_COORDS_OVERALL_KEY, ...categories];
  let rows = [];
  let total = 0;

  if (mode === 'district') {
    const feats = districtFC?.features || [];
    total = feats.length;
    rows = feats.map((f, idx) => ({
      id: f?.properties?.__districtName || `District ${idx + 1}`,
      label: f?.properties?.__districtName || `District ${idx + 1}`,
      values: f?.properties?.__fairByCat || {},
      source: f
    }));
    if (parallelCoordsDistrictFilter) {
      rows = rows.filter(row => row.id === parallelCoordsDistrictFilter);
    }
  } else if (mode === 'mezo') {
    const data = Array.isArray(mezoHexData) ? mezoHexData : [];
    total = data.length;
    rows = data.map((entry, idx) => ({
      id: entry?.hex || `Hex ${idx + 1}`,
      label: entry?.hex || `Hex ${idx + 1}`,
      values: entry?.__fairByCat || {},
      source: entry
    }));
  } else {
    // Exclude accessory structures (garages/sheds) so the PCP rows and the
    // "of N buildings" count reflect real buildings — consistent with the DR
    // matrix sample (which is already filtered in buildBuildingMatrix).
    const feats = (typeof isAccessoryBuilding === 'function')
      ? (baseCityFC?.features || []).filter(f => !isAccessoryBuilding(f.properties))
      : (baseCityFC?.features || []);
    const drSample = (drPlot.mode === mode && Array.isArray(drPlot.sample) && drPlot.sample.length)
      ? drPlot.sample
      : null;
    const sourceFeats = drSample || feats;
    total = sourceFeats.length;
    rows = sourceFeats.map((f, idx) => ({
      id: f?.properties?.id || f?.properties?.osm_id || f?.properties?.['@id'] || `B${idx + 1}`,
      label: f?.properties?.name || f?.properties?.id || f?.properties?.osm_id || f?.properties?.['@id'] || `Building ${idx + 1}`,
      values: f?.properties?.fair_multi || {},
      source: f
    }));
  }

  // Per-building demographics come from each building's DESO (Phase-3 baked
  // deso.geojson). Built once; null for district/mezo (district uses __demo).
  const pcDesoProps = (mode !== 'district' && mode !== 'mezo') ? pcDesoPropsMap() : null;
  // Meso (hex): aggregate the SAME synthetic per-building source per hex in one
  // pass, so the hex view shows demographics consistently with building/district.
  let pcMezoAgg = null;
  if (mode === 'mezo' && typeof synthpopAggregateAllHexes === 'function'
      && typeof h3 !== 'undefined' && Array.isArray(mezoHexData) && mezoHexData.length) {
    const res = (typeof h3.h3GetResolution === 'function' && mezoHexData[0]?.hex)
      ? h3.h3GetResolution(mezoHexData[0].hex) : null;
    if (res != null) pcMezoAgg = synthpopAggregateAllHexes(res);
  }

  rows = rows.map((row) => {
    const values = {};
    let count = 0;
    const rawValues = [];
    let overall = null;
    if (mode === 'district' || mode === 'mezo') {
      overall = Number.isFinite(row?.source?.properties?.__fairOverall)
        ? row.source.properties.__fairOverall
        : Number.isFinite(row?.source?.__fairOverall)
          ? row.source.__fairOverall
          : null;
    } else {
      overall = Number.isFinite(row?.source?.properties?.fair_overall?.score)
        ? row.source.properties.fair_overall.score
        : null;
    }

    for (const cat of categories) {
      const val = readFairValue(row.values, cat);
      if (Number.isFinite(val)) count += 1;
      if (Number.isFinite(val)) rawValues.push(val);
      values[cat] = Number.isFinite(val) ? Math.max(0, Math.min(1, val)) : null;
    }
    const min = rawValues.length ? Math.min(...rawValues) : null;
    const max = rawValues.length ? Math.max(...rawValues) : null;
    const avg = rawValues.length ? rawValues.reduce((a, b) => a + b, 0) / rawValues.length : null;
    if (!Number.isFinite(overall)) overall = avg;
    values[PARALLEL_COORDS_OVERALL_KEY] = Number.isFinite(overall) ? Math.max(0, Math.min(1, overall)) : null;
    // Capture real demographic values (district mode) for later normalisation.
    const realValues = {};
    if (mode === 'district') {
      const props = row?.source?.properties || {};
      const demo = props.__demo || {};
      pcDemoAxes().forEach((a) => {
        const v = a.json === '__needZ' ? Number(props.__needZ) : Number(demo[a.json]);
        if (Number.isFinite(v)) realValues[a.key] = v;
      });
    } else if (pcDesoProps) {
      // Building rows: ALL per-building axes (synthetic descriptive + synthetic
      // demographic shares) read straight off the feature, stamped by
      // synthpop.js, so every building contributes its own distinct value. Demo
      // axes fall back to the building's DESO share (via __deso) when no
      // synthetic value is stamped (e.g. non-residential / unmatched footprints).
      const sp = row?.source?.properties || {};
      const code = sp.__deso;
      const dp = (code != null) ? pcDesoProps.get(code) : null;
      [...PC_BUILDING_SYNTH_AXES, ...PC_BUILDING_DEMO_AXES].forEach((a) => {
        let v = Number(sp[a.prop]);
        if (!Number.isFinite(v) && a.desoProp && dp) v = Number(dp[a.desoProp]);
        if (Number.isFinite(v)) realValues[a.key] = v;
      });
    } else if (mode === 'mezo' && pcMezoAgg) {
      // Meso rows: per-hex aggregate of the SAME synthetic source, mapped onto
      // the building demo-axis keys.
      const agg = pcMezoAgg.get(row?.source?.hex);
      if (agg && agg.pop > 0) {
        pcBuildingDemoAxes().forEach((a) => {
          const v = Number(agg[PC_SYNTH_AGG_FIELD[a.key]]);
          if (Number.isFinite(v)) realValues[a.key] = v;
        });
      }
    }
    return { id: row.id, label: row.label || row.id, values, realValues, count, min, max, avg, source: row.source };
  }).filter(row => row.count > 0);

  // Append demographic axes in DISTRICT mode whenever the data is present —
  // independent of the DR scatter's Features dropdown (the district PCP is the
  // place these belong). Plotted as per-axis min–max normalised [0,1]; the
  // renderer relabels the endpoints with real values and the tooltip shows them.
  // Demographic axes: district reads __demo + need index; building rows read
  // their DESO's baked props (filled into realValues above). Mezo has none.
  // Each axis is min–max normalised into [0,1]; the renderer relabels the
  // endpoints with the real values and the tooltip shows them.
  let extraCategories = [];
  const demoAxisDefs = (mode === 'district') ? pcDemoAxes()
    : (mode === 'mezo') ? pcBuildingDemoAxes()   // hex now aggregates the synthetic source
    : pcBuildingDemoAxes();
  if (demoAxisDefs.length) {
    // Heavy-tailed synthetic axes (residents, footprint area, income) are
    // power-law distributed: a handful of large apartment blocks stretch the
    // axis so every normal house collapses onto the floor under linear min–max
    // (e.g. residents 1..498 → a 3-person house sits at 0.006). Log-scale those
    // so the bulk of the distribution is actually readable. Endpoints stay the
    // real min/max (log is monotonic), so the relabeled ticks remain correct.
    const LOG_PC_AXES = new Set(['synPop', 'synArea', 'synIncome']);
    const axes = demoAxisDefs.filter(a => rows.some(r => Number.isFinite(r.realValues?.[a.key])));
    axes.forEach((a) => {
      const vals = rows.map(r => r.realValues?.[a.key]).filter(Number.isFinite);
      const useLog = LOG_PC_AXES.has(a.key) && Math.min(...vals) >= 0;
      const tf = useLog ? (v => Math.log1p(v)) : (v => v);
      const lo = Math.min(...vals.map(tf)), hi = Math.max(...vals.map(tf));
      const span = (hi - lo) || 1;
      rows.forEach((r) => {
        const v = r.realValues?.[a.key];
        r.values[a.key] = Number.isFinite(v) ? (tf(v) - lo) / span : null;
      });
    });
    extraCategories = axes.map(a => a.key);
  }

  // The PCP data-source selector decides which axis groups appear:
  //   'poi'   = fairness/POI accessibility axes only
  //   'demo'  = demographic axes only (kept anchored by the Overall axis)
  //   'mixed' = both (default)
  // Falls back to POI axes whenever no demographic axes are available.
  const srcMode = parallelCoordsDataSourceMode || 'mixed';
  let outCategories;
  if (srcMode === 'poi' || !extraCategories.length) {
    outCategories = categoriesWithOverall;
  } else if (srcMode === 'demo') {
    outCategories = [PARALLEL_COORDS_OVERALL_KEY, ...extraCategories];
  } else {
    outCategories = categoriesWithOverall.concat(extraCategories);
  }

  const rawMaxPC = parseInt(parallelCoordsMaxPoints, 10);
  const maxLines = (rawMaxPC <= 0 || !Number.isFinite(rawMaxPC)) ? Infinity : Math.max(200, rawMaxPC);
  if (rows.length > maxLines) {
    const step = Math.ceil(rows.length / maxLines);
    rows = rows.filter((_, idx) => idx % step === 0);
  }

  return { rows, total, categories: outCategories };
}

function isEntityDRSelected(entity) {
  if (!entity) return false;
  const props = entity.properties || entity;
  return !!props?._drSelected;
}

function resetParallelCoordsSelectionState({ forceEmpty = false } = {}) {
  parallelCoordsSelectionIds.clear();
  parallelCoordsBrushFilters = {};
  parallelCoordsBrushSelections = {};
  parallelCoordsForceEmptySelection = !!forceEmpty;
}

function clearParallelCoordsSelectionFromClearAction() {
  resetParallelCoordsSelectionState({ forceEmpty: true });
  if (parallelCoordsOpen) {
    updateParallelCoordsPanel();
  }
}

function applyParallelCoordsSelection(selectedRows) {
  const rows = Array.isArray(selectedRows) ? selectedRows.filter(Boolean) : [];

  if (!rows.length) {
    clearDRMapSelection();
    applyDRSelection([], { skipMapSync: true, skipParallelSync: true });
    updateLayers();
    return;
  }

  const selectedEntities = rows.map((row) => row.source).filter(Boolean);
  applyMapSelection(selectedEntities, { skipDRSync: true, skipParallelSync: true });

  const drIndexByEntity = new Map();
  if (Array.isArray(drPlot.sample)) {
    drPlot.sample.forEach((entity, idx) => drIndexByEntity.set(entity, idx));
  }

  const drIdx = [];
  rows.forEach((row) => {
    const sampleIdx = drIndexByEntity.get(row.source);
    if (Number.isInteger(sampleIdx)) {
      drIdx.push(sampleIdx);
    }
  });

  if (drPlot.points) {
    applyDRSelection(drIdx, { skipMapSync: true, skipParallelSync: true });
  }
}

function updateParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel || panel.classList.contains('d-none')) return;

  const mode = currentParallelCoordsMode();
  const modeLabel = parallelCoordsModeLabel(mode);
  const modeEl = document.getElementById('parallelCoordsMode');
  if (modeEl) modeEl.textContent = `Mode: ${modeLabel}.`;

  bindParallelCoordsSourceSelect();

  const filterSelect = document.getElementById('parallelCoordsDistrictSelect');
  if (filterSelect) {
    const showFilter = mode === 'district';
    filterSelect.classList.toggle('d-none', !showFilter);
    if (showFilter) {
      updateParallelCoordsDistrictFilterOptions();
    } else if (parallelCoordsDistrictFilter) {
      parallelCoordsDistrictFilter = '';
    }
  }

  const { rows, total, categories } = getParallelCoordsDataset(mode);
  const orderedCategories = getOrderedParallelCoordsCategories(categories);
  parallelCoordsColumnOrder = orderedCategories.slice();

  if (!Object.keys(parallelCoordsBrushFilters).every(cat => orderedCategories.includes(cat))) {
    parallelCoordsBrushFilters = Object.fromEntries(
      Object.entries(parallelCoordsBrushFilters).filter(([cat]) => orderedCategories.includes(cat))
    );
  }
  if (!Object.keys(parallelCoordsBrushSelections).every(cat => orderedCategories.includes(cat))) {
    parallelCoordsBrushSelections = Object.fromEntries(
      Object.entries(parallelCoordsBrushSelections).filter(([cat]) => orderedCategories.includes(cat))
    );
  }
  Object.keys(parallelCoordsBrushSelections).forEach((cat) => {
    const filtered = new Set(
      Array.from(parallelCoordsBrushSelections[cat] || []).filter(id => rows.some(row => row.id === id))
    );
    if (filtered.size) parallelCoordsBrushSelections[cat] = filtered;
    else delete parallelCoordsBrushSelections[cat];
  });
  parallelCoordsSelectionIds = new Set(Array.from(parallelCoordsSelectionIds).filter(id => rows.some(row => row.id === id)));
  renderParallelCoords(rows, total, modeLabel, orderedCategories, { pending: parallelCoordsPending });
}

function renderParallelCoords(rows, total, modeLabel, categories, { pending = false } = {}) {
  const chart = document.getElementById('parallelCoordsChart');
  const note = document.getElementById('parallelCoordsNote');
  if (!chart || !note) return;

  if (typeof d3 === 'undefined') {
    chart.innerHTML = '<div class="small text-muted">Parallel coordinates require d3.js.</div>';
    note.textContent = '';
    return;
  }

  chart.innerHTML = '';
  chart.style.position = chart.style.position || 'relative';
  const available = rows.length;
  if (pending) {
    chart.innerHTML = '<div class="small text-muted mt-2">Computing POI fairness…</div>';
    note.textContent = '';
    return;
  }
  if (!available || !categories.length) {
    chart.innerHTML = '<div class="small text-muted mt-2">No POI fairness values yet. Select POIs or run overall fairness to populate per-POI scores.</div>';
    note.textContent = '';
    applyParallelCoordsSelection([]);
    return;
  }

  const viewportWidth = chart.clientWidth || 600;
  const height = chart.clientHeight || 170;
  // Top margin keeps dimension labels above each axis (matches the
  // design's ParallelCoords). Bottom margin is enough to keep the
  // lowest data points (score = 0) clear of the chart's bottom edge,
  // so they don't visually collide with the parallelCoordsNote text
  // below the chart.
  const margin = { top: 28, right: 24, bottom: 22, left: 24 };
  const minAxisGap = 88;
  const minPlotWidth = margin.left + margin.right + Math.max(0, categories.length - 1) * minAxisGap;
  const width = Math.max(viewportWidth, minPlotWidth);

  const svg = d3.select(chart)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scalePoint()
    .domain(categories)
    .range([margin.left, width - margin.right]);

  const y = d3.scaleLinear()
    .domain([0, 1])
    .range([height - margin.bottom, margin.top]);

  const line = d3.line()
    .defined(d => Number.isFinite(d.value))
    .x(d => x(d.cat))
    .y(d => y(d.value));

  const lineData = rows.map(row => ({
    row,
    points: categories.map(cat => ({ cat, value: row.values[cat] }))
  }));

  const brushedCategories = () => Object.keys(parallelCoordsBrushFilters);
  const rowById = new Map(rows.map(row => [row.id, row]));
  const brushedRows = () => {
    const active = brushedCategories();
    if (!active.length) return [];

    let commonIds = null;
    active.forEach((cat) => {
      const ids = parallelCoordsBrushSelections[cat];
      if (!ids) {
        commonIds = new Set();
        return;
      }

      if (!commonIds) {
        commonIds = new Set(ids);
        return;
      }

      commonIds.forEach((id) => {
        if (!ids.has(id)) commonIds.delete(id);
      });
    });

    return Array.from(commonIds || []).map(id => rowById.get(id)).filter(Boolean);
  };

  const computeSelectedRows = () => {
    const brushed = brushedRows();
    if (brushed.length || brushedCategories().length) return brushed;
    if (parallelCoordsSelectionIds.size) {
      return rows.filter(r => parallelCoordsSelectionIds.has(r.id));
    }
    if (parallelCoordsForceEmptySelection) return [];
    const externallySelected = rows.filter(row => isEntityDRSelected(row.source));
    if (externallySelected.length) return externallySelected;
    return [];
  };

  let currentSelectedRows = computeSelectedRows();
  const selectedIds = () => new Set(currentSelectedRows.map(row => row.id));

  const tooltip = d3.select(chart)
    .append('div')
    .style('position', 'absolute')
    .style('pointer-events', 'none')
    .style('opacity', 0)
    .style('padding', '6px 8px')
    .style('background', 'rgba(20, 20, 20, 0.9)')
    .style('color', '#f2f2f2')
    .style('border-radius', '4px')
    .style('font-size', '11px');

  let lineAppendMode = false;

  // Per-line colour from each row's mean fairness across the visible
  // dimensions, using the global colorFromScore so the palette matches
  // the map / DR / inspector ramp (purple → green = least → most fair).
  // Selected lines override this with the canonical orange selection
  // colour so DR scatter ↔ PC ↔ map all show the same highlight hue.
  const colorByRow = new Map();
  lineData.forEach(d => {
    // Colour by the row's OVERALL fairness — the same value the map paints —
    // NOT the mean of the visible axes. Averaging visible axes meant that in
    // "Demographics only" mode lines were coloured by average demographic value
    // (which clusters low → everything read blue), instead of by accessibility.
    const ov = d.row?.values?.[PARALLEL_COORDS_OVERALL_KEY];
    let score = Number.isFinite(ov) ? ov : null;
    if (score == null) {
      const vals = d.points.map(p => p.value).filter(Number.isFinite);
      score = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : 0.5;
    }
    const [r, g, b] = (typeof colorFromScore === 'function')
      ? colorFromScore(score)
      : [60, 120, 180];
    colorByRow.set(d.row.id, `rgb(${r}, ${g}, ${b})`);
  });
  const colorForRow = (rowId) => colorByRow.get(rowId) || '#888';
  const selectedLineColor = (typeof DR_SELECTION_COLOR_DEFAULT !== 'undefined' && DR_SELECTION_COLOR_DEFAULT)
    ? `rgb(${DR_SELECTION_COLOR_DEFAULT[0]}, ${DR_SELECTION_COLOR_DEFAULT[1]}, ${DR_SELECTION_COLOR_DEFAULT[2]})`
    : '#f16913';
  const unselectedLineColor = 'rgba(170, 170, 170, 0.25)';

  const linePaths = svg.append('g')
    .attr('fill', 'none')
    .attr('stroke-opacity', 0.4)
    .attr('stroke-width', 0.7)
    .selectAll('path')
    .data(lineData)
    .join('path')
    .attr('d', d => line(d.points))
    .style('cursor', 'pointer')
    .attr('stroke', d => selectedIds().size
      ? (selectedIds().has(d.row.id) ? selectedLineColor : unselectedLineColor)
      : colorForRow(d.row.id))
    .attr('stroke-opacity', d => selectedIds().size ? (selectedIds().has(d.row.id) ? 0.95 : 0.18) : 0.4)
    .attr('stroke-width', d => selectedIds().has(d.row.id) ? 1.8 : 0.7);

  // Event delegation: one set of listeners on the parent SVG instead of
  // per-line handlers. Attaching 4 listeners to each of 4 000 paths
  // creates 16 000 handlers — the dominant cost for large samples.
  // d3's `.target.__data__` carries the bound datum so we can recover
  // the row from the actual event target.
  svg.on('pointerdown.pc-line mousedown.pc-line', (event) => {
    if (event.target?.tagName === 'path') {
      lineAppendMode = isAdditiveSelectionEvent(event);
    }
  });
  svg.on('click.pc-line', (event) => {
    const datum = event.target?.__data__;
    if (!datum?.row) return;
    parallelCoordsBrushFilters = {};
    parallelCoordsBrushSelections = {};
    const append = lineAppendMode || isAdditiveSelectionEvent(event);
    lineAppendMode = false;
    const next = append ? new Set(parallelCoordsSelectionIds) : new Set();
    if (next.has(datum.row.id)) next.delete(datum.row.id);
    else next.add(datum.row.id);
    parallelCoordsSelectionIds = next;
    parallelCoordsForceEmptySelection = parallelCoordsSelectionIds.size === 0;
    renderParallelCoords(rows, total, modeLabel, categories);
  });
  svg.on('mousemove.pc-line', (event) => {
    const datum = event.target?.__data__;
    if (!datum?.row) { tooltip.style('opacity', 0); return; }
    const [mx, my] = d3.pointer(event, chart);
    const valueRows = categories.map(cat => {
      const real = datum.row.realValues?.[cat];
      if (Number.isFinite(real)) {
        const shown = (cat === 'demNeed') ? real.toFixed(2) : real.toFixed(1);
        return `<div>${prettyParallelAxis(cat)}: ${shown}</div>`;
      }
      const val = datum.row.values[cat];
      const pct = Number.isFinite(val) ? `${Math.round(val * 100)}%` : '—';
      return `<div>${prettyParallelAxis(cat)}: ${pct}</div>`;
    }).join('');
    tooltip
      .style('left', `${mx + 10}px`)
      .style('top', `${my - 10}px`)
      .style('opacity', 1)
      .html(`<div><strong>${datum.row.label}</strong></div>${valueRows}`);
  });
  svg.on('mouseleave.pc-line', () => { tooltip.style('opacity', 0); });

  // The dot overlay is decorative — at high row counts it is the dominant
  // SVG cost (4 000 lines × 7 dims = 28 000 circles, freezes the browser).
  // Render it only when the line count is small enough to stay snappy.
  const pointGroup = svg.append('g')
    .attr('fill', '#1f77b4')
    .attr('opacity', 0.25);
  const SHOW_POINTS_THRESHOLD = 500;
  if (lineData.length <= SHOW_POINTS_THRESHOLD) {
    pointGroup.selectAll('g')
      .data(lineData)
      .join('g')
      .selectAll('circle')
      .data(d => d.points.filter(p => Number.isFinite(p.value)))
      .join('circle')
      .attr('cx', d => x(d.cat))
      .attr('cy', d => y(d.value))
      .attr('r', 1.4);
  }

  const updateSelectionStyles = ({ sync = false } = {}) => {
    const idSet = selectedIds();
    linePaths
      .attr('stroke', d => idSet.size
        ? (idSet.has(d.row.id) ? selectedLineColor : unselectedLineColor)
        : colorForRow(d.row.id))
      .attr('stroke-opacity', d => idSet.size ? (idSet.has(d.row.id) ? 0.95 : 0.18) : 0.4)
      .attr('stroke-width', d => idSet.has(d.row.id) ? 1.8 : 0.7)
      .sort((a, b) => {
        const aSelected = idSet.has(a.row.id) ? 1 : 0;
        const bSelected = idSet.has(b.row.id) ? 1 : 0;
        return aSelected - bSelected;
      });

    pointGroup.attr('opacity', idSet.size ? 0.3 : 0.25);
    if (sync) applyParallelCoordsSelection(currentSelectedRows);
  };

  // Real value range per axis. Demographic/rich axes are min–max normalised to
  // [0,1] for plotting, so their 0–1 ticks are meaningless; we relabel the
  // endpoints with the actual values (e.g. unemployment 4.8% … 38.6%).
  const realRange = {};
  categories.forEach((cat) => {
    const vs = rows.map((r) => r.realValues?.[cat]).filter(Number.isFinite);
    if (vs.length) realRange[cat] = { min: Math.min(...vs), max: Math.max(...vs) };
  });
  const fmtReal = (v) => Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(1);

  const axisGroup = svg.append('g');
  categories.forEach((cat) => {
    const gx = axisGroup.append('g')
      .attr('transform', `translate(${x(cat)},0)`);
    gx.call(d3.axisLeft(y).ticks(4).tickSize(0));
    const rr = realRange[cat];
    if (rr) {
      // Replace the 0–1 ticks with the real min/max of this normalised axis.
      gx.selectAll('text').remove();
      gx.append('text').attr('x', 4).attr('y', y(1)).attr('font-size', 7)
        .attr('fill', 'var(--ink-3, #8a8278)').text(fmtReal(rr.max));
      gx.append('text').attr('x', 4).attr('y', y(0)).attr('font-size', 7)
        .attr('fill', 'var(--ink-3, #8a8278)').text(fmtReal(rr.min));
    } else {
      gx.selectAll('text').attr('font-size', 8).attr('fill', 'var(--ink-3, #8a8278)');
    }
    gx.selectAll('path').attr('stroke', 'var(--line-strong, rgba(28, 25, 22, 0.16))');
    gx.selectAll('line').remove();
    // Dimension label at the TOP of each axis. Truncate long demographic names
    // so axes don't overlap; full name shown on hover.
    const fullName = prettyParallelAxis(cat);
    const shortName = fullName.length > 12 ? `${fullName.slice(0, 11)}…` : fullName;
    const axisLabel = gx.append('text')
      .attr('y', margin.top - 12)
      .attr('text-anchor', 'middle')
      .attr('font-size', 9)
      .attr('font-weight', 500)
      .attr('fill', 'var(--ink-2, #5b544c)')
      .style('cursor', 'grab')
      .text(shortName);
    axisLabel.append('title').text(fullName);

    axisLabel.call(
      d3.drag()
        .on('start', function () {
          d3.select(this).style('cursor', 'grabbing');
        })
        .on('drag', function (event) {
          d3.select(this).attr('transform', `translate(${event.x - x(cat)},0)`);
        })
        .on('end', function (event) {
          d3.select(this).style('cursor', 'grab').attr('transform', null);
          const currentOrder = categories.slice();
          if (currentOrder.length < 2) return;

          const nearestCat = currentOrder.reduce((best, candidate) => {
            const dist = Math.abs((x(candidate) ?? 0) - event.x);
            if (!best || dist < best.dist) return { cat: candidate, dist };
            return best;
          }, null)?.cat;

          if (!nearestCat || nearestCat === cat) return;

          const fromIdx = currentOrder.indexOf(cat);
          const toIdx = currentOrder.indexOf(nearestCat);
          if (fromIdx < 0 || toIdx < 0 || fromIdx === toIdx) return;

          currentOrder.splice(fromIdx, 1);
          currentOrder.splice(toIdx, 0, cat);
          parallelCoordsColumnOrder = currentOrder;
          updateParallelCoordsPanel();
        })
    );

    const brush = d3.brushY()
      .extent([[x(cat) - 12, margin.top], [x(cat) + 12, height - margin.bottom]])
      .on('brush end', (event) => {
        const append = isAdditiveSelectionEvent(event?.sourceEvent || event);
        if (!event.selection) {
          delete parallelCoordsBrushFilters[cat];
          delete parallelCoordsBrushSelections[cat];
        } else {
          const [y0, y1] = event.selection;
          const minVal = Math.max(0, Math.min(1, y.invert(y1)));
          const maxVal = Math.max(0, Math.min(1, y.invert(y0)));
          parallelCoordsBrushFilters[cat] = [Math.min(minVal, maxVal), Math.max(minVal, maxVal)];
          const idsInBrush = rows
            .filter((row) => {
              const val = row.values?.[cat];
              if (!Number.isFinite(val)) return false;
              const [minV, maxV] = parallelCoordsBrushFilters[cat];
              return val >= minV && val <= maxV;
            })
            .map(row => row.id);

          const priorIds = append ? (parallelCoordsBrushSelections[cat] || new Set()) : new Set();
          const mergedIds = new Set(priorIds);
          idsInBrush.forEach((id) => mergedIds.add(id));
          parallelCoordsBrushSelections[cat] = mergedIds;
        }

        if (Object.keys(parallelCoordsBrushFilters).length) {
          if (!append) parallelCoordsSelectionIds.clear();
          parallelCoordsForceEmptySelection = false;
        } else if (!parallelCoordsSelectionIds.size) {
          parallelCoordsForceEmptySelection = true;
        }

        currentSelectedRows = computeSelectedRows();
        if (append && parallelCoordsSelectionIds.size) {
          const brushedIdSet = new Set(currentSelectedRows.map(row => row.id));
          currentSelectedRows = rows.filter((row) =>
            parallelCoordsSelectionIds.has(row.id) || brushedIdSet.has(row.id)
          );
        }
        updateSelectionStyles({ sync: true });
      });

    const brushGroup = axisGroup.append('g').attr('class', `pc-brush-${cat}`);
    brushGroup.call(brush);
    const existingRange = parallelCoordsBrushFilters[cat];
    if (existingRange) {
      const [minVal, maxVal] = existingRange;
      brushGroup.call(brush.move, [y(maxVal), y(minVal)]);
    }
  });

  const scrollHint = width > viewportWidth ? ' Scroll horizontally to see all categories.' : '';
  const sampledNote = available < total
    ? `Showing ${available} of ${total} ${modeLabel} (sampled).`
    : `Showing ${available} ${modeLabel}.`;

  if (currentSelectedRows.length) {
    const selCount = currentSelectedRows.length;
    const selectedAvgValues = currentSelectedRows.map(r => r.avg).filter(Number.isFinite);
    const avg = selectedAvgValues.length
      ? selectedAvgValues.reduce((a, b) => a + b, 0) / selectedAvgValues.length
      : null;
    const avgText = Number.isFinite(avg) ? `${Math.round(avg * 100)}%` : '—';
    const brushText = brushedCategories().length
      ? ' (axis brush intersection)'
      : (parallelCoordsSelectionIds.size ? ' (line pick)' : '');
    note.textContent = `${sampledNote}${scrollHint} Selected: ${selCount} ${modeLabel}${brushText} · Mean fairness ${avgText}.`;
    updateSelectionStyles({ sync: true });
    return;
  }

  updateSelectionStyles({ sync: true });
  note.textContent = `${sampledNote}${scrollHint}`;
}

// ======================= Fairness by district / hex bar chart =======================
function renderDistrictBar(selectionIdx) {
  const { districtBarWrap } = ensureDRUI();
  if (!districtBarWrap) return;

  if (typeof d3 === 'undefined') {
    districtBarWrap.innerHTML =
      '<div class="small text-muted">District bar chart requires d3.js.</div>';
    return;
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample  = drPlot.sample  || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    districtBarWrap.innerHTML =
      '<div class="small text-muted">No DR sample available yet.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const selSet = new Set(selection);

  const agg = new Map(); // name -> { name, sum, count, selSum, selCount }

  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;

    const feat = sample[i];

    // 🔹 NEW: group by usage type (category) instead of district
    const nameRaw = inferCategoryGroup(feat && feat.properties);
    if (!nameRaw) continue; // skip if we really have no category

    const name = String(nameRaw).trim();
    if (!name) continue;

    let e = agg.get(name);
    if (!e) {
      e = { name, sum: 0, count: 0, selSum: 0, selCount: 0 };
      agg.set(name, e);
    }
    e.sum += s;
    e.count += 1;
    if (selSet.has(i)) {
      e.selSum += s;
      e.selCount += 1;
    }
  }

  // Less strict so the chart actually appears; require at least 3 buildings per category
  let rows = Array.from(agg.values()).filter(r => r.count >= 3);

  if (!rows.length) {
    districtBarWrap.innerHTML =
      '<div class="small text-muted">Too few buildings per category to show a chart.</div>';
    return;
  }

  rows.forEach(r => {
    r.meanAll = r.sum / r.count;
    r.meanSel = r.selCount ? (r.selSum / r.selCount) : null;
  });

  const hasSel = selection.length > 0;
  rows.sort((a, b) => {
    const va = (hasSel && a.meanSel != null) ? a.meanSel : a.meanAll;
    const vb = (hasSel && b.meanSel != null) ? b.meanSel : b.meanAll;
    return va - vb; // worst at top
  });

  const maxBars = 12;
  const data = rows.slice(0, maxBars);

  const root = d3.select(districtBarWrap);
  root.selectAll('*').remove();

  const width  = districtBarWrap.clientWidth || 260;
  const height = districtBarWrap.clientHeight || 170;
  const margin = { top: 6, right: 10, bottom: 24, left: 80 };

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleBand()
    .domain(data.map(d => d.name))
    .range([margin.top, height - margin.bottom])
    .padding(0.15);

  // Gray bars = city mean
  svg.append('g')
    .selectAll('rect.all')
    .data(data)
    .join('rect')
    .attr('class', 'all')
    .attr('x', x(0))
    .attr('y', d => y(d.name))
    .attr('width', d => Math.max(0, x(d.meanAll) - x(0)))
    .attr('height', y.bandwidth())
    .attr('fill', '#555')
    .attr('opacity', 0.4);

  // Green overlay = selection mean (if any)
  if (hasSel) {
    svg.append('g')
      .selectAll('rect.sel')
      .data(data.filter(d => d.meanSel != null))
      .join('rect')
      .attr('class', 'sel')
      .attr('x', x(0))
      .attr('y', d => y(d.name) + y.bandwidth() * 0.2)
      .attr('width', d => Math.max(0, x(d.meanSel) - x(0)))
      .attr('height', y.bandwidth() * 0.6)
      .attr('fill', '#2ecc71')
      .attr('opacity', 0.9);
  }

  // Y-axis (usage type names)
  svg.append('g')
    .attr('transform', `translate(${margin.left - 4},0)`)
    .call(d3.axisLeft(y).tickSize(0))
    .call(g => {
      g.selectAll('text').attr('font-size', 9);
      g.selectAll('path, line').remove();
    });

  // X-axis
  const xAxis = d3.axisBottom(x)
    .ticks(3)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Mean overall fairness (0–1)');
}




// ======================= usage / category vs fairness scatterplot =======================

function renderHeightFairnessScatter(selectionIdx) {
  const { scatterHeightWrap } = ensureDRUI();
  if (!scatterHeightWrap) return;

  if (typeof d3 === 'undefined') {
    scatterHeightWrap.innerHTML =
      '<div class="small text-muted">Scatterplot requires d3.js.</div>';
    return;
  }

  // Make sure tooltip positioning is relative to this container
  const cs = getComputedStyle(scatterHeightWrap);
  if (cs.position === 'static') {
    scatterHeightWrap.style.position = 'relative';
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample  = drPlot.sample  || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    scatterHeightWrap.innerHTML =
      '<div class="small text-muted">No DR sample available yet.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const selSet = new Set(selection);

  // Build points: category / usage type + fairness
  const points = [];
  for (let i = 0; i < n; i++) {
    const f = overall[i];
    const feat = sample[i];
    if (!Number.isFinite(f) || !feat) continue;

    // Try multiple possible property names for usage/category
    let cat =
      feat.properties?.usage ||
      feat.properties?.bldg_usage ||
      feat.properties?.category ||
      feat.properties?.objekttyp ||
      'Unknown';

    if (typeof cat === 'string') {
      cat = cat.trim();
      if (!cat) cat = 'Unknown';
    } else {
      cat = String(cat);
    }

    points.push({
      idx: i,
      cat,
      f,
      feat,
      selected: selSet.has(i)
    });
  }

  if (!points.length) {
    scatterHeightWrap.innerHTML =
      '<div class="small text-muted">No category / fairness values to plot.</div>';
    return;
  }

  const root = d3.select(scatterHeightWrap);
  root.selectAll('*').remove();

  const width  = scatterHeightWrap.clientWidth || 260;
  const height = scatterHeightWrap.clientHeight || 190;
  const margin = { top: 6, right: 12, bottom: 46, left: 34 }; // a bit more bottom space for rotated labels

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  // X: categorical usage / category
  const categories = Array.from(new Set(points.map(d => d.cat)));
  const x = d3.scaleBand()
    .domain(categories)
    .range([margin.left, width - margin.right])
    .padding(0.3);

  // Y: fairness 0–1
  const y = d3.scaleLinear()
    .domain([0, 1])
    .nice()
    .range([height - margin.bottom, margin.top]);

  // All points (gray)
  svg.append('g')
    .selectAll('circle.base')
    .data(points)
    .join('circle')
    .attr('class', 'base')
    .attr('cx', d => x(d.cat) + x.bandwidth() / 2)
    .attr('cy', d => y(d.f))
    .attr('r', 2)
    .attr('fill', '#666')
    .attr('opacity', 0.4);

  // Selection overlay (green, larger)
  if (selection.length) {
    svg.append('g')
      .selectAll('circle.sel')
      .data(points.filter(d => d.selected))
      .join('circle')
      .attr('class', 'sel')
      .attr('cx', d => x(d.cat) + x.bandwidth() / 2)
      .attr('cy', d => y(d.f))
      .attr('r', 3.5)
      .attr('fill', '#2ecc71')
      .attr('stroke', '#111')
      .attr('stroke-width', 0.5)
      .attr('opacity', 0.95);
  }

  // Axes
  const xAxis = d3.axisBottom(x).tickSizeOuter(0);
  const yAxis = d3.axisLeft(y).ticks(4).tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text')
        .attr('font-size', 8)
        .attr('transform', 'rotate(-30)')
        .style('text-anchor', 'end');
    });

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(yAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - 4)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Category / usage type');

  svg.append('text')
    .attr('transform', 'rotate(-90)')
    .attr('x', -((margin.top + height - margin.bottom) / 2))
    .attr('y', 10)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Overall fairness (0–1)');

  // Hover tooltip (for selected points)
  const tooltip = root.append('div')
    .attr('class', 'dr-hist-tooltip')
    .style('position', 'absolute')
    .style('pointer-events', 'none')
    .style('background', 'rgba(0,0,0,0.8)')
    .style('color', '#fff')
    .style('padding', '2px 6px')
    .style('border-radius', '3px')
    .style('font-size', '10px')
    .style('display', 'none');

  svg.selectAll('circle.sel')
    .on('mousemove', function (event, d) {
      const name = d.feat?.properties?.name ||
                   d.feat?.properties?.category ||
                   d.feat?.properties?.objekttyp ||
                   'Building';
      const [mx, my] = d3.pointer(event, scatterHeightWrap);
      tooltip
        .style('left', `${mx + 12}px`)
        .style('top', `${my + 12}px`)
        .style('display', 'block')
        .html(
          `${name}<br/>` +
          `category: ${d.cat}<br/>` +
          `fairness ≈ ${d.f.toFixed(2)}`
        );
    })
    .on('mouseleave', () => {
      tooltip.style('display', 'none');
    });
}



// ======================= Lorenz curve of lack-of-access =======================
function renderLorenzCurve(selectionIdx) {
  const { lorenzWrap } = ensureDRUI();
  if (!lorenzWrap) return;

  if (typeof d3 === 'undefined') {
    lorenzWrap.innerHTML =
      '<div class="small text-muted">Lorenz curve requires d3.js.</div>';
    return;
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const n = overall.length;

  if (!n) {
    lorenzWrap.innerHTML =
      '<div class="small text-muted">No overall fairness values available.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];

  const allLack = overall
    .filter(Number.isFinite)
    .map(v => 1 - v); // lack-of-access

  const selLack = selection.length
    ? selection
        .filter(i => i >= 0 && i < overall.length)
        .map(i => overall[i])
        .filter(Number.isFinite)
        .map(v => 1 - v)
    : [];

  function buildLorenz(values) {
    const vals = values.filter(Number.isFinite).slice().sort((a, b) => a - b);
    const n = vals.length;
    if (!n) return null;
    const total = vals.reduce((a, b) => a + b, 0) || 1;
    let cum = 0;
    const pts = [{ x: 0, y: 0 }];
    for (let i = 0; i < n; i++) {
      cum += vals[i];
      pts.push({ x: (i + 1) / n, y: cum / total });
    }
    return pts;
  }

  const allCurve = buildLorenz(allLack);
  const selCurve = buildLorenz(selLack);

  if (!allCurve) {
    lorenzWrap.innerHTML =
      '<div class="small text-muted">Not enough values for Lorenz curve.</div>';
    return;
  }

  const root = d3.select(lorenzWrap);
  root.selectAll('*').remove();

  const width  = lorenzWrap.clientWidth || 260;
  const height = lorenzWrap.clientHeight || 170;
  const margin = { top: 6, right: 10, bottom: 24, left: 28 };

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleLinear()
    .domain([0, 1])
    .range([height - margin.bottom, margin.top]);

  // Diagonal (perfect equality)
  svg.append('line')
    .attr('x1', x(0))
    .attr('y1', y(0))
    .attr('x2', x(1))
    .attr('y2', y(1))
    .attr('stroke', '#777')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '4,3')
    .attr('opacity', 0.8);

  const lineAll = d3.line()
    .x(d => x(d.x))
    .y(d => y(d.y));

  // City curve (gray)
  svg.append('path')
    .datum(allCurve)
    .attr('fill', 'none')
    .attr('stroke', '#cccccc')
    .attr('stroke-width', 1.5)
    .attr('d', lineAll);

  // Selection curve (green)
  if (selCurve && selCurve.length) {
    svg.append('path')
      .datum(selCurve)
      .attr('fill', 'none')
      .attr('stroke', '#2ecc71')
      .attr('stroke-width', 1.5)
      .attr('d', lineAll);
  }

  const axis = d3.axisBottom(x)
    .ticks(3)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(axis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  const yAxis = d3.axisLeft(y)
    .ticks(3)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(yAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Cumulative share of buildings');

  svg.append('text')
    .attr('transform', 'rotate(-90)')
    .attr('x', -((margin.top + height - margin.bottom) / 2))
    .attr('y', 10)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Cumulative share of lack-of-access');
}


// ======================= Threshold bar chart per district =======================
function renderThresholdBar(selectionIdx) {
  const { thresholdBarWrap, thresholdSlider } = ensureDRUI();
  if (!thresholdBarWrap || !thresholdSlider) return;

  if (typeof d3 === 'undefined') {
    thresholdBarWrap.innerHTML =
      '<div class="small text-muted">Threshold bar chart requires d3.js.</div>';
    return;
  }

  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample  = drPlot.sample  || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    thresholdBarWrap.innerHTML =
      '<div class="small text-muted">No DR sample available yet.</div>';
    return;
  }

  const thr = parseFloat(thresholdSlider.value || '0.5') || 0.5;

  const agg = new Map(); // name -> { name, total, below }

  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;

    const feat = sample[i];

    // 🔹 Use usage type / category instead of district
    const nameRaw = inferCategoryGroup(feat.properties);
    if (!nameRaw) continue;

    const name = String(nameRaw).trim();
    if (!name) continue;

    let e = agg.get(name);
    if (!e) {
      e = { name, total: 0, below: 0 };
      agg.set(name, e);
    }
    e.total += 1;
    if (s < thr) e.below += 1;
  }

  // Only keep categories with enough buildings
  let rows = Array.from(agg.values()).filter(r => r.total >= 5);

  if (!rows.length) {
    thresholdBarWrap.innerHTML =
      '<div class="small text-muted">Too few buildings per category to show shares.</div>';
    return;
  }

  rows.forEach(r => {
    r.share = r.total ? r.below / r.total : 0;
  });

  // Highest share (worst) at top
  rows.sort((a, b) => b.share - a.share);

  const maxBars = 12;
  const data = rows.slice(0, maxBars);

  const root = d3.select(thresholdBarWrap);
  root.selectAll('*').remove();

  const width  = thresholdBarWrap.clientWidth || 260;
  const height = thresholdBarWrap.clientHeight || 170;
  const margin = { top: 6, right: 10, bottom: 24, left: 80 };

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleBand()
    .domain(data.map(d => d.name))
    .range([margin.top, height - margin.bottom])
    .padding(0.15);

  svg.append('g')
    .selectAll('rect')
    .data(data)
    .join('rect')
    .attr('x', x(0))
    .attr('y', d => y(d.name))
    .attr('width', d => Math.max(0, x(d.share) - x(0)))
    .attr('height', y.bandwidth())
    .attr('fill', '#e67e22')
    .attr('opacity', 0.9);

  svg.append('g')
    .attr('transform', `translate(${margin.left - 4},0)`)
    .call(d3.axisLeft(y).tickSize(0))
    .call(g => {
      g.selectAll('text').attr('font-size', 9);
      g.selectAll('path, line').remove();
    });

  const xAxis = d3.axisBottom(x)
    .ticks(3)
    .tickFormat(d3.format('.0%'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    // 🔹 Update label to mention "usage type" or "category" if you want
    .text(`Share of buildings with fairness < ${thr.toFixed(2)}`);
}

// ======================= buildThresholdSummaryForLLM =======================



// ======================= LLM helpers =======================

const LLM_SUMMARY_BUILDERS = {
  umap: buildUMAPSummaryForLLM,
  engine_ebm: buildEBMSummaryForLLM,
  engine_contrast: buildContrastiveSummaryForLLM,
  stats: buildStatsSummaryForLLM,
  model: buildModelSummaryForLLM,
  overall_hist: buildOverallHistSummaryForLLM,
  building_type: buildBuildingTypeSummaryForLLM,
  scatter: buildScatterSummaryForLLM,
  lorenz: buildLorenzSummaryForLLM,
  threshold_bar: buildThresholdSummaryForLLM,
  category_hists: buildCategoryHistsSummaryForLLM,
};

function quantileSorted(arr, q) {
  if (!arr.length) return null;
  const idx = (arr.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return arr[lo];
  const h = idx - lo;
  return arr[lo] * (1 - h) + arr[hi] * h;
}

function buildUMAPSummaryForLLM() {
  const total = drPlot.sample?.length || 0;
  if (!total) return { error: 'No DR embedding available yet.' };

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const algo = document.getElementById('drAlgo')?.value || 'umap';
  const colorBy = document.getElementById('drColorBy')?.value || 'overall';

  return {
    algo,
    color_by: colorBy,
    total_points: total,
    selection_size: selection.length,
  };
}

function buildEBMSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'Select some buildings to train the EBM.' };

  const engine = drPlot.engineEBM;
  const ranked = engine?.ranked || [];

  const top = ranked.slice(0, 6).map((f) => ({
    feature: f.label,
    score: Number.isFinite(f.score) ? Number(f.score.toFixed(3)) : f.score,
    direction: f.direction || null,
  }));

  return {
    engine: 'ebm',
    selection_size: selection.length,
    training: {
      total: engine?.nTrain ?? null,
      positives: engine?.nPos ?? null,
      negatives: engine?.nNeg ?? null,
    },
    top_features: top,
    note: engine?.note || 'EBM not run yet (will train on selection).',
  };
}

function buildContrastiveSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'Select some buildings to compare distributions.' };

  if (!drPlot.engineContrast) {
    drPlot.engineContrast = buildContrastiveEngineExplanation();
  }

  const engine = drPlot.engineContrast;
  if (!engine || !engine.ranked?.length) return { error: 'Contrastive distribution has no features.' };

  const top = engine.ranked.slice(0, 6).map((f) => ({
    feature: f.label,
    score: Number.isFinite(f.score) ? Number(f.score.toFixed(3)) : f.score,
    direction: f.direction,
  }));

  return {
    engine: 'contrastive',
    selection_size: selection.length,
    top_features: top,
    note: engine.note,
  };
}

function buildStatsSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'No selection yet for stats.' };

  if (!drPlot.lastFeatureDiff) {
    drPlot.lastFeatureDiff = computeFeatureDifferences(selection);
  }

  const feats = drPlot.lastFeatureDiff?.features || [];
  if (!feats.length) return { error: 'No numeric features available.' };

  const top = feats.slice(0, 6).map((f) => ({
    feature: f.label,
    mean_city: Number.isFinite(f.meanCity) ? Number(f.meanCity.toFixed(3)) : null,
    mean_selection: Number.isFinite(f.meanSel) ? Number(f.meanSel.toFixed(3)) : null,
    effect_size: Number.isFinite(f.effect) ? Number(f.effect.toFixed(3)) : null,
    ks_distance: Number.isFinite(f.ks) ? Number(f.ks.toFixed(3)) : null,
  }));

  return {
    selection_size: selection.length,
    top_features: top,
  };
}

function buildModelSummaryForLLM() {
  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  if (!selection.length) return { error: 'No selection yet for the model view.' };

  if (!drPlot.lastModelExplanation) {
    drPlot.lastModelExplanation = trainLogisticSelectionModel(selection);
  }

  const model = drPlot.lastModelExplanation;
  if (!model || model.error) {
    return { error: model?.error || 'Model explanation unavailable.' };
  }

  const top = (model.ranked || []).slice(0, 6).map((f) => ({
    feature: f.label,
    weight: Number.isFinite(f.absWeight) ? Number(f.absWeight.toFixed(3)) : null,
    direction: f.direction || null,
  }));

  return {
    selection_size: selection.length,
    training: {
      positives: model.nPos ?? selection.length,
      negatives: model.nNeg ?? null,
    },
    top_features: top,
  };
}

function buildOverallHistSummaryForLLM() {
  const overallRaw = drPlot.metrics?.overall || [];
  const values = overallRaw.filter((v) => Number.isFinite(v) && v >= 0 && v <= 1);
  if (!values.length) return { error: 'No overall fairness values available.' };

  const sorted = values.slice().sort((a, b) => a - b);
  const mean = mean1(values);
  const median = quantileSorted(sorted, 0.5);

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const selVals = selection
    .map((i) => (i >= 0 && i < overallRaw.length ? overallRaw[i] : null))
    .filter((v) => Number.isFinite(v));

  const selSorted = selVals.slice().sort((a, b) => a - b);

  return {
    total: values.length,
    mean_city: Number.isFinite(mean) ? Number(mean.toFixed(3)) : null,
    median_city: Number.isFinite(median) ? Number(median.toFixed(3)) : null,
    selection_size: selection.length,
    selection_mean: selVals.length ? Number(mean1(selVals).toFixed(3)) : null,
    selection_median: selSorted.length ? Number(quantileSorted(selSorted, 0.5).toFixed(3)) : null,
  };
}

function buildBuildingTypeSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample = drPlot.sample || [];
  const n = Math.min(overall.length, sample.length);
  if (!n) return { error: 'No DR sample available yet.' };

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const selSet = new Set(selection);

  const agg = new Map();
  for (let i = 0; i < n; i++) {
    const score = overall[i];
    if (!Number.isFinite(score)) continue;

    const feat = sample[i];
    const nameRaw = inferCategoryGroup(feat && feat.properties);
    if (!nameRaw) continue;
    const name = String(nameRaw).trim();
    if (!name) continue;

    let e = agg.get(name);
    if (!e) {
      e = { name, sum: 0, count: 0, selSum: 0, selCount: 0 };
      agg.set(name, e);
    }
    e.sum += score;
    e.count += 1;
    if (selSet.has(i)) {
      e.selSum += score;
      e.selCount += 1;
    }
  }

  const rows = Array.from(agg.values()).filter((r) => r.count >= 3);
  if (!rows.length) return { error: 'Too few buildings per category to summarize.' };

  rows.forEach((r) => {
    r.meanAll = r.sum / r.count;
    r.meanSel = r.selCount ? r.selSum / r.selCount : null;
  });

  const hasSel = selection.length > 0;
  rows.sort((a, b) => {
    const va = hasSel && Number.isFinite(a.meanSel) ? a.meanSel : a.meanAll;
    const vb = hasSel && Number.isFinite(b.meanSel) ? b.meanSel : b.meanAll;
    return va - vb;
  });

  const top = rows.slice(0, 8).map((r) => ({
    category: r.name,
    count: r.count,
    mean_city: Number(r.meanAll.toFixed(3)),
    mean_selection: Number.isFinite(r.meanSel) ? Number(r.meanSel.toFixed(3)) : null,
  }));

  return {
    selection_size: selection.length,
    categories: top,
  };
}

function buildScatterSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample = drPlot.sample || [];
  const n = Math.min(overall.length, sample.length);
  if (!n) return { error: 'No DR sample available yet.' };

  const selection = Array.isArray(drPlot.lastSelectionIdx)
    ? new Set(drPlot.lastSelectionIdx)
    : new Set();

  const cats = new Map();
  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;
    const feat = sample[i];
    const name = inferCategoryGroup(feat.properties) || 'Unknown';
    let e = cats.get(name);
    if (!e) {
      e = { name, count: 0, fairness: [], selected: [] };
      cats.set(name, e);
    }
    e.count += 1;
    e.fairness.push(s);
    if (selection.has(i)) e.selected.push(s);
  }

  const categories = Array.from(cats.values()).map((c) => ({
    name: c.name,
    count: c.count,
    mean_fairness: c.fairness.reduce((a, b) => a + b, 0) / c.fairness.length,
    selected_count: c.selected.length,
    selected_mean: c.selected.length
      ? c.selected.reduce((a, b) => a + b, 0) / c.selected.length
      : null,
  }));

  const overallMean = categories.reduce((sum, c) => sum + c.mean_fairness * c.count, 0) /
    (categories.reduce((s, c) => s + c.count, 0) || 1);

  return {
    total_points: n,
    categories,
    overall_mean_fairness: overallMean,
    selection_size: selection.size,
  };
}

function buildLorenzSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = (metrics.overall || []).filter(Number.isFinite).sort((a, b) => a - b);
  if (!overall.length) return { error: 'No overall fairness values for Lorenz curve.' };

  const gini = overallGini != null ? overallGini : null;
  return {
    total_buildings: overall.length,
    gini: gini,
    p10: quantileSorted(overall, 0.1),
    median: quantileSorted(overall, 0.5),
    p90: quantileSorted(overall, 0.9),
  };
}

function buildThresholdSummaryForLLM() {
  const metrics = drPlot.metrics || {};
  const overall = metrics.overall || [];
  const sample = drPlot.sample || [];
  const n = Math.min(overall.length, sample.length);

  if (!n) {
    return { threshold: null, total_buildings: 0, below_threshold: 0, share_below: 0, note: 'no data' };
  }

  const sliderEl = document.getElementById('drThresholdSlider') || document.getElementById('thresholdSlider');
  const thr = parseFloat(sliderEl?.value || '0.5') || 0.5;

  let total = 0;
  let below = 0;
  for (let i = 0; i < n; i++) {
    const s = overall[i];
    if (!Number.isFinite(s)) continue;
    total++;
    if (s < thr) below++;
  }

  const share = total ? below / total : 0;

  return {
    threshold: thr,
    total_buildings: total,
    below_threshold: below,
    share_below: share,
  };
}

function perCategorySeriesWithFocusedFallback(metricKey) {
  const metrics = drPlot.metrics || {};
  const sample = Array.isArray(drPlot.sample) ? drPlot.sample : [];
  const base = Array.isArray(metrics[metricKey]) ? metrics[metricKey] : [];
  const expectedCat = {
    fairGrocery: 'grocery',
    fairHospital: 'hospital',
    fairPrimary: 'school_primary',
    fairPharmacy: 'pharmacy',
    fairHealthcare: 'healthcare',
    fairKindergarten: 'kindergarten',
    fairSchoolHigh: 'school_high',
  }[metricKey];

  if (!expectedCat || !sample.length) return base.slice();

  return base.map((rawVal, idx) => {
    if (Number.isFinite(rawVal)) return Number(rawVal);

    const entity = sample[idx];
    const props = entity?.properties || {};
    const focusedCat = props?.fair?.cat ?? props?.__fairFocusedCat;
    const focusedScore = Number.isFinite(props?.fair?.score)
      ? Number(props.fair.score)
      : (Number.isFinite(props?.__fairFocused) ? Number(props.__fairFocused) : null);

    if (focusedCat === expectedCat && Number.isFinite(focusedScore)) {
      return focusedScore;
    }
    return null;
  });
}

function getCategoryHistogramConfig() {
  return DR_FEATURE_CONFIG
    .filter(cfg => typeof cfg?.key === 'string' && cfg.key.startsWith('fair') && cfg.key !== 'overall')
    .map(cfg => ({
      key: cfg.key,
      label: (cfg.label || cfg.key).replace(/\s+fairness$/i, '')
    }));
}

function buildCategoryHistsSummaryForLLM() {
  const cats = getCategoryHistogramConfig();

  const selection = Array.isArray(drPlot.lastSelectionIdx) ? drPlot.lastSelectionIdx : [];
  const selectionSet = new Set(selection);

  const rows = cats.map((cfg) => {
    const values = perCategorySeriesWithFocusedFallback(cfg.key);
    const arr = values.filter(Number.isFinite);
    const selVals = values
      .map((v, idx) => (selectionSet.has(idx) ? v : null))
      .filter(Number.isFinite);

    const mean = arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
    const selMean = selVals.length ? selVals.reduce((a, b) => a + b, 0) / selVals.length : null;

    return {
      category: cfg.label,
      count: arr.length,
      mean,
      selected_count: selVals.length,
      selected_mean: selMean,
    };
  });

  const any = rows.some((r) => r.count > 0);
  return any ? { categories: rows, selection_size: selection.length } : { error: 'No per-category fairness values.' };
}

function wireLLMExplainUI() {
  const btn = document.getElementById('explain-view-btn');
  const out = document.getElementById('llm-output');
  const qEl = document.getElementById('llm-question');
  const viewSel = document.getElementById('llm-view-select');
  if (!btn || !out || !viewSel) return;

  let busy = false;

  const run = async () => {
    const view = viewSel.value || 'threshold_bar';
    const builder = LLM_SUMMARY_BUILDERS[view];
    if (!builder) {
      out.textContent = 'Unknown view for explanation.';
      return;
    }

    const summary = builder();
    if (summary?.error) {
      out.textContent = summary.error;
      return;
    }

    const question = qEl ? qEl.value.trim() : '';
    out.textContent = 'Thinking';
    busy = true;

    try {
      const text = await requestLLMExplain(view, summary, question);
      out.textContent = text;
    } catch (err) {
      console.error(err);
      out.textContent = 'Error asking the AI: ' + err.message;
    } finally {
      busy = false;
    }
  };

  btn.addEventListener('click', () => {
    if (!busy) run();
  });

  if (qEl) {
    qEl.addEventListener('keydown', (ev) => {
      if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter' && !busy) {
        run();
      }
    });
  }
}

// Call FastAPI /llm/explain, which uses local Ollama
async function requestLLMExplain(viewName, summary, question) {
  const res = await fetch(`${API_BASE}/llm/explain`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      view: viewName,
      summary: summary,
      question: question || null,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error('LLM API error: ' + text);
  }

  const data = await res.json();
  // Expect your FastAPI route to return: { "text": "..." }
  return data.text;
}

async function requestLLMWhatIfIntent(question, context) {
  const res = await fetch(`${API_BASE}/llm/whatif-intent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      question: question || '',
      context: context || null
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error('LLM intent error: ' + text);
  }

  return res.json();
}


// ======================= Small multiple histograms for categories =======================
function renderCategoryHists(selectionIdx) {
  const { categoryHistsWrap } = ensureDRUI();
  if (!categoryHistsWrap) return;

  if (typeof d3 === 'undefined') {
    categoryHistsWrap.innerHTML =
      '<div class="small text-muted">Category histograms require d3.js.</div>';
    return;
  }

  const allCats = getCategoryHistogramConfig();
  const valuesByCat = Object.fromEntries(
    allCats.map(cfg => [cfg.key, perCategorySeriesWithFocusedFallback(cfg.key)])
  );

  const cats = allCats.filter(cfg => (valuesByCat[cfg.key] || []).some(Number.isFinite));
  if (!cats.length) {
    categoryHistsWrap.innerHTML =
      '<div class="small text-muted">No per-category fairness values available.</div>';
    return;
  }

  const selection = Array.isArray(selectionIdx) ? selectionIdx : [];
  const root = d3.select(categoryHistsWrap);
  root.selectAll('*').remove();

  const width  = categoryHistsWrap.clientWidth || 260;
  const height = categoryHistsWrap.clientHeight || 200;
  const labelMeasureCtx = document.createElement('canvas').getContext('2d');
  if (labelMeasureCtx) labelMeasureCtx.font = '9px sans-serif';
  const maxLabelWidth = cats.reduce((max, c) => {
    const label = String(c.label || '');
    const measured = labelMeasureCtx ? labelMeasureCtx.measureText(label).width : (label.length * 5.2);
    return Math.max(max, measured);
  }, 0);
  const maxPad = Math.min(120, Math.max(88, Math.floor(width * 0.24)));
  const labelPad = Math.max(56, Math.min(maxPad, Math.ceil(maxLabelWidth + 10)));
  const margin = { top: 6, right: 12, bottom: 24, left: labelPad };
  const rows = cats.length;
  const rowHeight = (height - margin.top - margin.bottom) / rows;

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([0, 1])
    .range([margin.left, width - margin.right]);

  cats.forEach((cfg, idx) => {
    const arr = valuesByCat[cfg.key] || [];
    const allValues = arr.filter(Number.isFinite);
    if (!allValues.length) return;

    const selectedValues = selection.length
      ? selection
          .filter(i => i >= 0 && i < arr.length)
          .map(i => arr[i])
          .filter(Number.isFinite)
      : [];

    const binGen = d3.bin()
      .domain(x.domain())
      .thresholds(10);

    const binsAll = binGen(allValues);
    const binsSel = binGen(selectedValues);

    const maxCount = d3.max([
      d3.max(binsAll, d => d.length) || 0,
      d3.max(binsSel, d => d.length) || 0
    ]) || 1;

    const rowTop = margin.top + idx * rowHeight;
    const rowBottom = rowTop + rowHeight - 10;

    const y = d3.scaleLinear()
      .domain([0, maxCount])
      .range([rowBottom, rowTop + 4]);

    // Gray background bars
    svg.append('g')
      .selectAll(`rect.all-${cfg.key}`)
      .data(binsAll)
      .join('rect')
      .attr('class', `all-${cfg.key}`)
      .attr('x', d => x(d.x0) + 1)
      .attr('y', d => y(d.length))
      .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 2))
      .attr('height', d => y(0) - y(d.length))
      .attr('fill', '#555')
      .attr('opacity', 0.35);

    // Green overlay bars (selection)
    if (selectedValues.length) {
      const widthFactor = 0.6;
      svg.append('g')
        .selectAll(`rect.sel-${cfg.key}`)
        .data(binsSel)
        .join('rect')
        .attr('class', `sel-${cfg.key}`)
        .attr('x', d => {
          const fullWidth = x(d.x1) - x(d.x0) - 2;
          const wSel = fullWidth * widthFactor;
          return x(d.x0) + 1 + (fullWidth - wSel) / 2;
        })
        .attr('y', d => y(d.length))
        .attr('width', d => {
          const fullWidth = x(d.x1) - x(d.x0) - 2;
          return Math.max(0, fullWidth * widthFactor);
        })
        .attr('height', d => y(0) - y(d.length))
        .attr('fill', '#2ecc71')
        .attr('opacity', 0.9);
    }

    // Label on the left for each row
    svg.append('text')
      .attr('x', margin.left - 4)
      .attr('y', rowTop + 11)
      .attr('text-anchor', 'end')
      .attr('font-size', 9)
      .attr('fill', '#000')
      .text(cfg.label);
  });

  // Shared x-axis at the bottom
  const xAxis = d3.axisBottom(x)
    .ticks(4)
    .tickFormat(d3.format('.1f'));

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').attr('stroke', '#777');
      g.selectAll('line').attr('stroke', '#777');
      g.selectAll('text').attr('font-size', 9);
    });

  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - margin.bottom / 2 + 12)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#aaa')
    .text('Fairness score (0–1)');
}





// ======================= Supervised explanation (logistic model) =======================

function shuffleArray(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
}

/**
 * Train a small logistic regression model in JS to distinguish
 * "selected" vs "rest" using the DR feature space.
 * Returns ranked feature importances.
 */
function trainLogisticSelectionModel(selectionIdx, opts = {}) {
  const Xall = drPlot.features;
  const labels = drPlot.featureLabels || [];
  if (!Xall || !Xall.length) return null;

  const n = Xall.length;
  const d = Xall[0].length;
  if (!selectionIdx || selectionIdx.length < 10) {
    return { error: 'Need at least ~10 selected points for a stable model.' };
  }

  const maxIter = opts.maxIter ?? 80;
  const negPosRatio = opts.negPosRatio ?? 3;
  const lr = opts.lr ?? 0.1;
  const lambda = opts.lambda ?? 0.01;

  const selectedSet = new Set(selectionIdx);

  // Build training dataset: all positives + sampled negatives
  const X = [];
  const y = [];
  // positives
  for (const i of selectionIdx) {
    if (i >= 0 && i < n) {
      X.push(Xall[i]);
      y.push(1);
    }
  }

  const nonSelected = [];
  for (let i = 0; i < n; i++) {
    if (!selectedSet.has(i)) nonSelected.push(i);
  }
  shuffleArray(nonSelected);

  const maxNeg = Math.min(nonSelected.length, selectionIdx.length * negPosRatio + 50);
  for (let k = 0; k < maxNeg; k++) {
    const idx = nonSelected[k];
    X.push(Xall[idx]);
    y.push(0);
  }

  const m = X.length;
  if (m < 20) {
    return { error: 'Not enough total examples to train the model.' };
  }

  // Simple gradient-descent logistic regression
  const w = new Array(d).fill(0);
  let b = 0;

  const sigmoid = (z) => 1 / (1 + Math.exp(-z));

  for (let iter = 0; iter < maxIter; iter++) {
    const gradW = new Array(d).fill(0);
    let gradB = 0;

    for (let i = 0; i < m; i++) {
      const xi = X[i];
      let z = b;
      for (let j = 0; j < d; j++) z += w[j] * xi[j];
      const p = sigmoid(z);
      const err = p - y[i];

      for (let j = 0; j < d; j++) gradW[j] += err * xi[j];
      gradB += err;
    }

    for (let j = 0; j < d; j++) {
      gradW[j] = gradW[j] / m + lambda * w[j]; // L2
      w[j] -= lr * gradW[j];
    }
    gradB /= m;
    b -= lr * gradB;
  }

  const ranked = [];
  for (let j = 0; j < d; j++) {
    const label = labels[j] || `Feature ${j + 1}`;
    const weight = w[j];
    ranked.push({
      index: j,
      label,
      weight,
      absWeight: Math.abs(weight),
      direction: weight >= 0 ? 'higher-in-cluster' : 'lower-in-cluster'
    });
  }
  ranked.sort((a, b) => b.absWeight - a.absWeight);

  return {
    weights: w,
    intercept: b,
    ranked,
    nTrain: m,
    nPos: selectionIdx.length,
    nNeg: m - selectionIdx.length
  };
}

/**
 * Render the model-based feature importance into the "Model" panel.
 */
function renderModelExplanationPanel(model) {
  const { explainModelEl } = ensureDRUI();
  if (!explainModelEl) return;

  if (!model || model.error) {
    explainModelEl.textContent = model && model.error
      ? model.error
      : 'No model available. Make a selection first.';
    return;
  }

  const top = model.ranked.slice(0, 5);
  if (!top.length) {
    explainModelEl.textContent = 'Model could not find informative features.';
    return;
  }

  let html = '';
  html += `<div class="small text-muted mb-1">
    Logistic model trained on the same feature space as PCA/UMAP.
  </div>`;
  html += `<div class="small mb-2">
    Training set: ${model.nPos} in-cluster vs ${model.nNeg} other buildings.
  </div>`;

  html += '<ul class="small mb-0 ps-3">';
  for (const f of top) {
    const dirText = f.direction === 'higher-in-cluster'
      ? 'tends to be higher inside the cluster'
      : 'tends to be lower inside the cluster';
    const arrow = f.direction === 'higher-in-cluster' ? '↑' : '↓';

    html += `<li>
      <strong>${f.label}</strong> (${arrow}):
      <span class="text-muted">|w| = ${f.absWeight.toFixed(3)}</span><br/>
      <span class="text-muted">${dirText}.</span>
    </li>`;
  }
  html += '</ul>';

  explainModelEl.innerHTML = html;
}

/**
 * Ensure we have a fresh model explanation for the current selection
 * and update the Model panel.
 */
function ensureModelExplanation(selectionIdx) {
  const { explainModelEl } = ensureDRUI();
  if (!explainModelEl) return;

  if (!selectionIdx || !selectionIdx.length) {
    explainModelEl.textContent = 'No selection yet.';
    return;
  }

  const model = trainLogisticSelectionModel(selectionIdx);
  drPlot.lastModelExplanation = model;
  renderModelExplanationPanel(model);
}


// ======================= Explanation mode wiring (Stats / Model) =======================

function refreshExplanationView() {
  const {
    explainStatsEl,
    explainModelEl
  } = ensureDRUI();

  const hasSelection = drPlot.lastSelectionIdx && drPlot.lastSelectionIdx.length > 0;

  if (drExplainMode === 'stats') {
    if (explainStatsEl) explainStatsEl.classList.remove('d-none');
    if (explainModelEl) explainModelEl.classList.add('d-none');

    if (!hasSelection) {
      if (explainStatsEl) explainStatsEl.textContent = 'No selection yet.';
      return;
    }

    if (!drPlot.lastFeatureDiff) {
      drPlot.lastFeatureDiff = computeFeatureDifferences(drPlot.lastSelectionIdx);
    }
    renderFeatureDiffPanel(drPlot.lastFeatureDiff);
  } else {
    if (explainModelEl) explainModelEl.classList.remove('d-none');
    if (explainStatsEl) explainStatsEl.classList.add('d-none');

    if (!hasSelection) {
      if (explainModelEl) explainModelEl.textContent = 'No selection yet.';
      return;
    }
    ensureModelExplanation(drPlot.lastSelectionIdx);
  }
}

function initExplainModeUI() {
  const {
    explainModeStatsBtn,
    explainModeModelBtn
  } = ensureDRUI();

  if (!explainModeStatsBtn || !explainModeModelBtn) return;
  if (explainModeStatsBtn.__bound) return;

  const applyMode = (mode) => {
    drExplainMode = mode;
    const isStats = mode === 'stats';
    explainModeStatsBtn.classList.toggle('active', isStats);
    explainModeModelBtn.classList.toggle('active', !isStats);
    refreshExplanationView();
  };

  explainModeStatsBtn.addEventListener('click', (e) => {
    e.preventDefault();
    applyMode('stats');
  });
  explainModeModelBtn.addEventListener('click', (e) => {
    e.preventDefault();
    applyMode('model');
  });

  explainModeStatsBtn.__bound = true;
  explainModeModelBtn.__bound = true;

  // Initialize based on current mode
  applyMode(drExplainMode || 'stats');
}



// ======================= Engine-level plots: EBM vs Contrastive =======================

// True when the lasso covers every DR point. Both engines compare the selection
// against the rest of the city, so a full selection has nothing to contrast —
// every effect size collapses to ~0 (a misleading near-zero chart).
function isEntireSampleSelected(selection) {
  const total = Array.isArray(drPlot.sample) ? drPlot.sample.length : 0;
  const n = Array.isArray(selection) ? new Set(selection).size : 0;
  return total > 0 && n >= total;
}

// Message payload shown by both engines when there's nothing to contrast.
function allSelectedEngineMessage(mode) {
  return {
    mode,
    ranked: [],
    message: 'Everything is selected — there is no subset to contrast against the ' +
      'rest of the city. Lasso a smaller group to compare it against the whole.',
    note: ''
  };
}

// Small helper: build unsupervised contrastive engine data
function buildContrastiveEngineExplanation() {
  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) return null;
  if (isEntireSampleSelected(selection)) return allSelectedEngineMessage('contrast');

  // Recompute fresh (do NOT reuse drPlot.lastFeatureDiff): the cached diff was
  // computed for whatever feature set was active earlier, which froze the bar
  // list to the same features. Recomputing reflects the current Features set.
  const diff = computeFeatureDifferences(selection);
  drPlot.lastFeatureDiff = diff;

  const feats = diff && diff.features ? diff.features : [];
  if (!feats.length) return null;

  // Show every feature (was capped at 16, which silently dropped services with a
  // tiny effect — e.g. Hospital, which barely varies at hex scale in rural
  // Kalmar). 30 is comfortably above the ~20 access features.
  const top = feats.slice(0, 30);

  return {
    mode: 'contrast',
    ranked: top.map(f => {
      const effect = f.effect || 0;
      // Colour by GOOD/BAD for the selection, not raw direction: for distance
      // features LOWER is better (closer); for fairness/access HIGHER is better.
      // So "closer" reads green and "farther" reads red — the intuitive mapping.
      const lowerIsBetter = /distance/i.test(f.label || '');
      const better = lowerIsBetter ? effect < 0 : effect > 0;
      return {
        label: f.label,
        score: Math.abs(effect),
        direction: effect >= 0 ? 'higher-in-cluster' : 'lower-in-cluster',
        better
      };
    }),
    note: 'Unsupervised: contrastive distribution between the selection and the ' +
          'wider city (standardized mean differences). Green = the selection is ' +
          'BETTER served (closer / fairer); red = worse served (farther / less fair).'
  };
}

// Build payload and call the Python EBM backend
async function buildEBMEngineExplanation() {
  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) return null;
  if (isEntireSampleSelected(selection)) return allSelectedEngineMessage('ebm');

  const Xall = drPlot.features;
  const featureNames = drPlot.featureLabels || [];
  if (!Xall || !Xall.length) {
    return {
      mode: 'ebm',
      ranked: [],
      note: 'No DR feature matrix available for EBM.'
    };
  }

  const n = Xall.length;
  const selectedSet = new Set(selection);

  // Label: 1 = selected, 0 = rest
  const X = [];
  const y = [];
  for (let i = 0; i < n; i++) {
    X.push(Xall[i]);
    y.push(selectedSet.has(i) ? 1 : 0);
  }

  const payload = {
    X,
    y,
    feature_names: featureNames   // <— send human-readable names to backend
  };

  const myId = ++drEngineRequestId;

  try {
    const resp = await fetch(`${API_BASE}/api/ebm/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!resp.ok) {
      throw new Error(`EBM HTTP ${resp.status}`);
    }
    const data = await resp.json();

    // Drop stale response if a newer request was started
    if (myId !== drEngineRequestId) {
      return null;
    }

    const rankedRaw = data.ranked || [];

    // Prefer backend labels (they already carry the true feature name after
    // backend ranking). If backend sends a generic "Feature N", map N back
    // to the original DR feature label for readability.
    const ranked = rankedRaw.map((f, idx) => {
      const nameFromBackend =
        (typeof f.label === 'string' && f.label.trim()) ? f.label.trim() : null;

      let label = nameFromBackend || null;
      if (label) {
        const generic = label.match(/^feature\s+(\d+)$/i);
        if (generic) {
          const oneBased = Number(generic[1]);
          const mapped = (Number.isFinite(oneBased) && oneBased >= 1 && oneBased <= featureNames.length)
            ? featureNames[oneBased - 1]
            : null;
          if (mapped) label = mapped;
        }
      }

      if (!label) {
        const fromPosition =
          (idx < featureNames.length && typeof featureNames[idx] === 'string' && featureNames[idx].trim())
            ? featureNames[idx].trim()
            : null;
        label = fromPosition || `Feature ${idx + 1}`;
      }

      const baseScore = f.importance ?? f.score ?? 0;
      return {
        label,
        score: Math.abs(baseScore),
        direction: f.direction || 'higher-in-cluster'
      };
    });

    return {
      mode: 'ebm',
      ranked,
      nTrain: data.n_train ?? null,
      nPos: data.n_pos ?? null,
      nNeg: data.n_neg ?? null,
      note: data.note ||
        'EBM (Explainable Boosting Machine) trained on selected vs other buildings.'
    };
  } catch (err) {
    console.error('EBM engine error:', err);
    if (myId !== drEngineRequestId) {
      return null;
    }
    return {
      mode: 'ebm',
      ranked: [],
      note: 'EBM error: ' + err.message
    };
  }
}

// Draw horizontal bar chart inside #drEnginePlot
function drawEngineBarChart(engineData) {
  const { enginePlotEl } = ensureDRUI();
  if (!enginePlotEl) return;

  // If d3 is missing, show a simple message and clear the note.
  if (typeof d3 === 'undefined') {
    enginePlotEl.textContent = 'd3 is required for engine plots.';
    const noteEl = document.getElementById('drEngineNote');
    if (noteEl) noteEl.textContent = '';
    return;
  }

  // Clear the plot area.
  const root = d3.select(enginePlotEl);
  root.html('');

  // Update the external note area instead of appending inside the plot.
  const noteEl = document.getElementById('drEngineNote');
  if (noteEl) {
    let noteTxt = engineData && engineData.note ? engineData.note : '';
    if (engineData?.mode === 'ebm') {
      noteTxt += (noteTxt ? ' ' : '') +
        'Color does not encode direction here; EBM bars show importance magnitude only.';
    }
    noteEl.textContent = noteTxt;
  }

  // No data case.
  if (!engineData || !engineData.ranked || !engineData.ranked.length) {
    const reason = (engineData && engineData.note && engineData.note.trim())
      ? engineData.note.trim()
      : 'No informative features for this selection.';
    root.append('div')
      .attr('class', 'small text-muted')
      .text(reason);
    return;
  }

 const top = engineData.ranked.slice(0, 30); // show full ranking (all services, no silent cap)
  const width = enginePlotEl.clientWidth || 260;
  const margin = { top: 26, right: 12, bottom: 52, left: 170 };
  const minInnerHeight = top.length * 24;
  const height = Math.max(
    enginePlotEl.clientHeight || 0,
    margin.top + margin.bottom + minInnerHeight
  );

  const svg = root.append('svg')
    .attr('width', width)
    .attr('height', height)
    .style('display', 'block');

  const xMax = d3.max(top, f => f.score) || 0.01;
  const x = d3.scaleLinear()
    .domain([0, xMax])
    .range([margin.left, width - margin.right]);

  const y = d3.scaleBand()
    .domain(top.map(f => f.label))
    .range([margin.top, height - margin.bottom])
    .padding(0.2);

  // x-axis
  const xAxis = d3.axisBottom(x)
    .ticks(3)
    .tickSizeOuter(0);

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => {
      g.selectAll('path').remove();
      g.selectAll('line').attr('stroke', '#ccc');
      g.selectAll('text').attr('font-size', 9);
    });

  // x-axis label (different text for EBM vs contrastive)
  svg.append('text')
    .attr('x', (margin.left + width - margin.right) / 2)
    .attr('y', height - 14)
    .attr('text-anchor', 'middle')
    .attr('font-size', 9)
    .attr('fill', '#666')
    .text(
      engineData.mode === 'ebm'
        ? 'EBM importance (log-odds magnitude)'
        : 'Effect size |d| (contrastive)'
    );

  // bars
  svg.append('g')
    .selectAll('rect')
    .data(top)
    .enter()
    .append('rect')
    .attr('x', x(0))
    .attr('y', d => y(d.label))
    .attr('height', y.bandwidth())
    .attr('width', d => x(d.score) - x(0))
    .attr('fill', d => {
      if (engineData.mode === 'ebm') return '#6c6c6c';
      // Green = better for the selection (closer / fairer), red = worse. `better`
      // already accounts for distance polarity; fall back to raw direction if absent.
      if (typeof d.better === 'boolean') return d.better ? '#2ecc71' : '#e74c3c';
      return d.direction === 'higher-in-cluster' ? '#2ecc71' : '#e74c3c';
    });

  // y-axis labels (feature names)
  svg.append('g')
    .selectAll('text.feature-label')
    .data(top)
    .enter()
    .append('text')
    .attr('class', 'feature-label')
    .attr('x', margin.left - 6)
    .attr('y', d => y(d.label) + y.bandwidth() / 2)
    .attr('dy', '0.35em')
    .attr('text-anchor', 'end')
    .attr('font-size', 10)
    .text(d => d.label);

  // small title line
  const title = engineData.mode === 'ebm'
    ? 'Supervised engine (EBM)'
    : 'Unsupervised engine (contrastive distribution)';

  svg.append('text')
    .attr('x', margin.left)
    .attr('y', margin.top - 4)
    .attr('font-size', 10)
    .attr('fill', '#555')
    .text(title);
}


// Refresh engine plot whenever selection or mode changes
function engineNeedsCalculationMessage(mode) {
  return mode === 'contrast'
    ? 'Contrastive distribution is ready to run. Click "Calculate selected engine".'
    : 'EBM is ready to run. Click "Calculate selected engine".';
}

async function refreshEnginePlot() {
  const { enginePlotEl } = ensureDRUI();
  if (!enginePlotEl) return;

  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) {
    if (typeof d3 !== 'undefined') {
      d3.select(enginePlotEl).selectAll('*').remove();
    }
    enginePlotEl.textContent =
      'Select some buildings (lasso) to see ranked features here.';
    return;
  }

  const engineData = drPlot.engineMode === 'contrast'
    ? drPlot.engineContrast
    : drPlot.engineEBM;

  if (!engineData) {
    enginePlotEl.textContent = engineNeedsCalculationMessage(drPlot.engineMode);
    const noteEl = document.getElementById('drEngineNote');
    if (noteEl) noteEl.textContent = '';
    return;
  }

  // Degenerate case (e.g. everything selected): show the guidance message instead
  // of an empty / near-zero bar chart.
  if (engineData.message && (!engineData.ranked || !engineData.ranked.length)) {
    if (typeof d3 !== 'undefined') d3.select(enginePlotEl).selectAll('*').remove();
    enginePlotEl.textContent = engineData.message;
    const noteEl = document.getElementById('drEngineNote');
    if (noteEl) noteEl.textContent = '';
    return;
  }

  drawEngineBarChart(engineData);
}

async function calculateEnginePlot() {
  const { enginePlotEl, engineCalcBtn } = ensureDRUI();
  if (!enginePlotEl) return;

  const selection = drPlot.lastSelectionIdx || [];
  if (!selection.length) {
    enginePlotEl.textContent = 'Select some buildings (lasso) to see ranked features here.';
    return;
  }

  try {
    if (engineCalcBtn) engineCalcBtn.disabled = true;
    if (drPlot.engineMode === 'contrast') {
      enginePlotEl.textContent = 'Calculating contrastive distribution…';
      drPlot.engineContrast = buildContrastiveEngineExplanation();
    } else {
      enginePlotEl.textContent = 'Training EBM on selection…';
      drPlot.engineEBM = await buildEBMEngineExplanation();
    }
    await refreshEnginePlot();
  } finally {
    if (engineCalcBtn) engineCalcBtn.disabled = false;
  }
}


// Switch between EBM and Contrastive engines
function setEngineMode(mode) {
  if (mode !== 'ebm' && mode !== 'contrast') return;

  // If mode is already active, do nothing
  if (drPlot.engineMode === mode) return;

  drPlot.engineMode = mode;

  const { engineEBMBtn, engineContrastBtn } = ensureDRUI();
  if (engineEBMBtn) {
    engineEBMBtn.classList.toggle('btn-secondary', mode === 'ebm');
    engineEBMBtn.classList.toggle('btn-outline-secondary', mode !== 'ebm');
  }
  if (engineContrastBtn) {
    engineContrastBtn.classList.toggle('btn-secondary', mode === 'contrast');
    engineContrastBtn.classList.toggle('btn-outline-secondary', mode !== 'contrast');
  }

  // Just redraw using existing caches (or compute once if missing)
  refreshEnginePlot();
}


// Bind engine buttons once
function initEngineUI() {
  const { engineEBMBtn, engineContrastBtn, engineCalcBtn } = ensureDRUI();

  if (engineEBMBtn && !engineEBMBtn.__bound) {
    engineEBMBtn.addEventListener('click', (e) => {
      e.preventDefault();
      setEngineMode('ebm');
    });
    engineEBMBtn.__bound = true;
  }

  if (engineContrastBtn && !engineContrastBtn.__bound) {
    engineContrastBtn.addEventListener('click', (e) => {
      e.preventDefault();
      setEngineMode('contrast');
    });
    engineContrastBtn.__bound = true;
  }

  if (engineCalcBtn && !engineCalcBtn.__bound) {
    engineCalcBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      await calculateEnginePlot();
    });
    engineCalcBtn.__bound = true;
  }

  // Initialize default engine mode
  setEngineMode(drPlot.engineMode || 'ebm');
}





/* ---------- Bindings (robust) ---------- */
function bindDRButtonsIfReady() {
  const runBtn = document.getElementById('drRunBtn');
  if (runBtn && !runBtn.__bound) {
    runBtn.addEventListener('click', (e) => { e.preventDefault(); runDR(); });
    runBtn.__bound = true;
  }

  const lassoBtn = document.getElementById('drLassoBtn');
  if (lassoBtn && !lassoBtn.__bound) {
    lassoBtn.addEventListener('click', (e) => { e.preventDefault(); toggleLasso(); });
    lassoBtn.__bound = true;
  }

  const clearSelBtn = document.getElementById('drClearSelBtn');
  if (clearSelBtn && !clearSelBtn.__bound) {
    clearSelBtn.addEventListener('click', (e) => { e.preventDefault(); clearSelection(); });
    clearSelBtn.__bound = true;
  }

  const clearProjBtn = document.getElementById('drClearProjBtn');
  if (clearProjBtn && !clearProjBtn.__bound) {
    clearProjBtn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      clearDRProjection();
    });
    clearProjBtn.__bound = true;
  }

  // Defensive delegation in case the button is re-rendered or missed during
  // initial binding. This keeps the handler alive even if Bootstrap toggles
  // the offcanvas content or the element is replaced.
  if (!bindDRButtonsIfReady.__delegated) {
    const handleClear = (e) => {
      const btn = e.target?.closest?.('#drClearProjBtn,[data-action="clear-umap"]');
      if (!btn) return;
      e.preventDefault();
      e.stopPropagation();
      clearDRProjection();
    };

    document.addEventListener('click', handleClear, true);
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      handleClear(e);
    }, true);

    bindDRButtonsIfReady.__delegated = true;
  }
}

function initDRUI() {
  const offcanvasEl = document.getElementById('drOffcanvas');

  // Bind the DR buttons immediately so the controls work even before the
  // offcanvas "shown" event fires (e.g., if Bootstrap fails to emit it).
  bindDRButtonsIfReady();

  if (offcanvasEl && !offcanvasEl.__drBound) {
    offcanvasEl.addEventListener('shown.bs.offcanvas', () => {
      // 1) Make sure DR surface & base controls are ready
      showDRLibraryStatus();
      prepareDRSurface();
      bindDRButtonsIfReady();   // zoom/reset, algo, color, etc.

      // 2) Bind lower explanation tabs (Stats / Model)
      initExplainModeUI();

      // 3) Bind upper engine selector (EBM / Contrastive)
      //    initEngineUI() internally calls setEngineMode(...)
      initEngineUI();

      // 4) Threshold slider: label + re-render threshold chart
      const { thresholdSlider, thresholdLabel } = ensureDRUI();
      if (thresholdSlider && !thresholdSlider.__bound) {
        const onChange = () => {
          const v = parseFloat(thresholdSlider.value || '0.5') || 0.5;
          if (thresholdLabel) {
            thresholdLabel.textContent = v.toFixed(2);
          }
          // Re-render only the threshold bar chart with current selection
          if (typeof renderThresholdBar === 'function') {
            renderThresholdBar(drPlot.lastSelectionIdx || []);
          }
        };
        thresholdSlider.addEventListener('input', onChange);
        thresholdSlider.__bound = true;
        onChange(); // initial label + initial chart
      }

      // 5) If we already have DR results, restore view + all coordinated panels
      if (drPlot.points && drPlot.colors) {
        drPlot.screenXY = computeScreenPositions(drPlot.points);
        const selIdx = (lasso.selectedIdx && lasso.selectedIdx.length)
          ? lasso.selectedIdx
          : null;

        redrawDR(selIdx);
        initD3Overlay();          // re-draw lasso hull if any

        // single call that:
        // - updates selInfo text
        // - updates Stats/Model explanation panel
        // - updates EBM/contrastive engine plot
        // - updates histogram, district bar, scatter, Lorenz, threshold chart, category hists
        if (typeof renderSelectionStats === 'function') {
          renderSelectionStats(selIdx || []);
        }
      } else {
        // No DR yet: still initialise city-wide panels with "no selection"
        if (typeof renderSelectionStats === 'function') {
          renderSelectionStats([]);
        }
      }
    });

    offcanvasEl.__drBound = true;
  }

  // Early binding in case offcanvas is already visible or opened quickly
  bindDRButtonsIfReady();
  initExplainModeUI();
  initEngineUI();
}



function showDRLibraryStatus() {
  const libNote = document.getElementById('drLibNote');
  const umapOk = hasUMAPGlobal();
  if (libNote) {
    libNote.innerHTML = `
      <span class="${umapOk?'text-success':'text-danger'}">UMAP ${umapOk?'loaded':'not loaded'}</span><br/>
      UMAP needs <code>assets/vendor/umap.min.js</code> (or we’ll try an online module).
    `;
  }
}

function resetDROverlayAndSelection() {
  setLassoActive(false);
  const svgEl = document.getElementById('drOverlay');
  if (svgEl && typeof d3 !== 'undefined') d3.select(svgEl).selectAll('*').remove();

  lasso.selectedIdx = [];
  lasso.points = [];
  lasso.drawing = false;
  lasso.marqueeDrawing = false;
  lasso.marqueeStart = null;

  if (lasso.path) {
    lasso.path.remove();
    lasso.path = null;
  }
  if (lasso.marqueeRect) {
    lasso.marqueeRect.remove();
    lasso.marqueeRect = null;
  }

  drHasSelection = false;          // reset flag
  renderSelectionStats([]);

  // wipe selection state from map buildings
  clearDRMapSelection();

  updateLayers();
}

function clearDRProjection(showMessage = true) {
  const {
    canvas,
    statusEl,
    infoEl,
    legendTitle,
    legendText,
    clearSelBtn,
    clearProjBtn
  } = ensureDRUI();

  resetDROverlayAndSelection();
  clearParallelCoordsSelectionFromClearAction();

  drPlot.points = null;
  drPlot.colors = null;
  drPlot.screenXY = null;
  drPlot.sample = null;
  drPlot.metrics = null;
  drPlot.mode = currentDRDataMode();
  drPlot.features = null;
  drPlot.featureLabels = null;
  drPlot.cityStats = null;
  drPlot.lastSelectionIdx = [];
  drPlot.lastFeatureDiff = null;
  drPlot.lastModelExplanation = null;
  drPlot.engineEBM = null;
  drPlot.engineContrast = null;
  drPlot.runNonce = 0;

  drHasSelection = false;

 if (canvas) {
    const ctx = canvas.getContext('2d');
    ctx.save();
    ctx.setTransform(1, 0, 0, 1, 0, 0);   // reset any HiDPI scaling before clearing
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.restore();
    resizeDRCanvas();
  }


  const overlay = document.getElementById('drOverlay');
  if (overlay && typeof d3 !== 'undefined') {
    d3.select(overlay).selectAll('*').remove();
  }

  // if (statusEl) statusEl.textContent = showMessage ? 'Projection cleared.' : '';
  if (infoEl) infoEl.textContent = 'Run PCA/UMAP to see a projection.';
  if (legendTitle) legendTitle.textContent = 'Color legend';
  if (legendText) legendText.textContent = '—';
  if (clearSelBtn) clearSelBtn.disabled = true;
  if (clearProjBtn) clearProjBtn.disabled = true;
}



  async function runDR(opts = {}) {
    // By default, re-running DR/UMAP starts with a clean selection state (the
    // manual "Run" button). Passing { preserveSelection: true } — or setting
    // `globalThis.DR_PRESERVE_SELECTION_ON_RERUN` — keeps the current selection
    // and re-projects it onto the new sample. Used on a spatial-scale change so
    // the DR/PCP/Map views stay consistent (see maybeRefreshDROnSpatialModeChange).
    const preserveSelectionOnRerun =
      !!globalThis.DR_PRESERVE_SELECTION_ON_RERUN || !!opts.preserveSelection;
    const pendingMapSelection = preserveSelectionOnRerun ? getCurrentMapSelection() : [];

    showDRSpinner();
    try {
      const { algoSel, colorSel, maxPtsEl, normEl, statusEl, infoEl, clearSelBtn, clearProjBtn } = ensureDRUI();
    const algo    = (algoSel?.value || 'pca').toLowerCase();
    const colorBy = (colorSel?.value || 'overall');
    const rawMax  = parseInt(maxPtsEl?.value || '0', 10);
    const maxPts  = (rawMax <= 0 || !Number.isFinite(rawMax)) ? Infinity : Math.max(200, rawMax);
    const normalize = !!(normEl && normEl.checked);

    prepareDRSurface();
    resetDROverlayAndSelection(); // clear overlay & selection BEFORE plotting

    drPlot.runNonce = (Number(drPlot.runNonce) || 0) + 1;
    // Building mode: attach baked rich features (modal-gap + built form + demo)
    // so the matrix carries the extra independent dimensions. One-time per city.
    if (currentDRDataMode() === 'building' && typeof ensureRichBuildingFeatures === 'function') {
      try { await ensureRichBuildingFeatures(); } catch (_) { /* optional */ }
    }
    // Preload the 2SFCA supply layer when a supply feature set is active, so the
    // synchronous collectDRData below finds it. Mode follows the fairness travel
    // mode, exactly like the inspector's Supply panel.
    const fsNow = document.getElementById('drFeatureSet')?.value || '';
    const colorNow = document.getElementById('drColorBy')?.value || '';
    const needsSupply = fsNow === 'supply' || fsNow === 'access_supply' || fsNow === 'supply_plus'
      || colorNow === 'mismatch' || colorNow === 'supply';
    if (needsSupply && typeof ensureAccess2sfca === 'function') {
      const a2sMode = (document.getElementById('fairnessTravelMode')?.value || 'walking').toLowerCase();
      const a2sCity = (typeof a2sCurrentCity === 'function') ? a2sCurrentCity() : undefined;
      try { await ensureAccess2sfca(a2sCity, a2sMode); } catch (_) { /* supply is best-effort */ }
    }
    const { X, colors, sampleCount, sample, metrics, featureLabels, dims } =
      collectDRData(maxPts, normalize, colorBy, drPlot.runNonce);

    let Y = null;
    if (algo === 'pca') {
      Y = runPCA(X);
      if (statusEl) statusEl.textContent = `PCA done for ${sampleCount} points.`;
    } else if (algo === 'umap') {
      if (!hasUMAPGlobal()) {
        const ok = await ensureUMAP();
        if (!ok) {
          Y = runPCA(X);
          if (statusEl) statusEl.textContent = `UMAP not loaded → fell back to PCA (${sampleCount} points).`;
        } else {
          Y = await runUMAP(X, { nNeighbors: 15, minDist: 0.1, nEpochs: 200 });
          if (statusEl) statusEl.textContent = `UMAP done for ${sampleCount} points.`;
        }
      } else {
        Y = await runUMAP(X, { nNeighbors: 15, minDist: 0.1, nEpochs: 200 });
        if (statusEl) statusEl.textContent = `UMAP done for ${sampleCount} points.`;
      }
    } else {
      Y = runPCA(X);
      if (statusEl) statusEl.textContent = `PCA done for ${sampleCount} points.`;
    }

    // Store DR results + feature space for explanations
    drPlot.points   = Y;
    drPlot.colors   = colors;
    drPlot.sample   = sample;
    drPlot.metrics  = metrics;
    drPlot.features = X;                     //same features used for DR
    drPlot.featureLabels = featureLabels || null;
    drPlot.cityStats = null;                 // reset cached stats
    drPlot.mode = currentDRDataMode();
    drPlot.lastSelectionIdx = [];
    drPlot.lastFeatureDiff = null;
    drPlot.lastModelExplanation = null;

    drPlot.screenXY = computeScreenPositions(Y);
    redrawDR();

    const umapOk = hasUMAPGlobal();
    if (infoEl) {
      const labelsText = (featureLabels && featureLabels.length)
        ? featureLabels.join(', ')
        : `${dims || (X[0]?.length || 0)}D feature space`;
      const modeLabel = currentDRDataMode();
      infoEl.textContent =
        `Mode = ${modeLabel} | Features = ${labelsText} | UMAP: ${umapOk ? 'loaded' : 'not loaded'}`;
    }

      renderSelectionStats([]);
      initD3Overlay();
      refreshExplanationView(); //panels show "No selection yet"

     // Optional: preserve map-driven selection across reruns when explicitly enabled.
      if (preserveSelectionOnRerun && pendingMapSelection.length) {
        applyMapSelection(pendingMapSelection, { skipDRSync: true });
        syncDRSelectionFromBuildings(pendingMapSelection, { preserveMapSelection: true });
      }

      // Buttons: no selection yet
      if (clearSelBtn) clearSelBtn.disabled = true;
      if (clearProjBtn) clearProjBtn.disabled = false;

    hideDRSpinner();
  } catch (e) {
    console.error(e);
    alert(e?.message || e);
    try {
      const { statusEl } = ensureDRUI();
      if (statusEl) statusEl.textContent = 'Error while running DR.';
    } catch {}
    hideDRSpinner('Error');
  }
}


