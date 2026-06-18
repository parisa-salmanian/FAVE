// Demographic lens — an alternate building-coloring mode that paints each
// building by a demographic attribute of its DESO (Phase-3 baked deso.geojson)
// instead of by accessibility fairness. Lets an urban planner ask "where do the
// elderly / children / low-income / high-need live?" on the same map.
//
// Data source: EPI_DEMO (models/epicityDemographics.js) + the per-building
// __deso stamp. No runtime network calls. When a field is active the building
// layer's getFillColor (views/layers.js) routes through demoLensColorForFeature,
// and the side-panel legend (#fairnessLegendBar) is repurposed to the lens ramp
// with real min/max labels. Off (empty field) restores the fairness coloring.

// Each field: prop = key in deso.geojson props; fmt = real-value formatter for
// the legend; hint = one-line tooltip. Order matches the inspector/PCP axes.
const DEMO_LENS_FIELDS = [
  { key: 'needZ',          label: 'Need index',        fmt: v => v.toFixed(2),               hint: 'Composite multi-domain deprivation z-score (higher = more need).' },
  { key: 'income',         label: 'Income',            fmt: v => `${Math.round(v)} tkr`,     hint: 'Median disposable income per DESO (tkr/year).' },
  { key: 'child_frac',     label: 'Children %',        fmt: v => `${(v * 100).toFixed(0)}%`, hint: 'Share of residents aged 0–15.' },
  { key: 'elder_frac',     label: 'Elderly %',         fmt: v => `${(v * 100).toFixed(0)}%`, hint: 'Share of residents aged 65+.' },
  { key: 'dependency',     label: 'Dependency ratio',  fmt: v => v.toFixed(2),               hint: '(children + elderly) / working-age population.' },
  { key: 'higher_ed',      label: 'Higher-ed eligible %', fmt: v => `${v.toFixed(0)}%`,      hint: 'Share eligible for higher education.' },
  { key: 'neet',           label: 'NEET %',            fmt: v => `${v.toFixed(1)}%`,         hint: 'Youth not in employment, education or training.' },
  { key: 'income_support', label: 'Income support %',  fmt: v => `${v.toFixed(1)}%`,         hint: 'Share receiving long-term income support.' },
  { key: 'male_frac',      label: 'Male %',            fmt: v => `${(v * 100).toFixed(0)}%`, hint: 'Share of male residents.' },
  { key: 'pop',            label: 'Population',         fmt: v => Math.round(v).toLocaleString(), hint: 'Total DESO population.' },
];

let demoLensField = '';        // '' = lens off (fairness coloring); else a field key
let demoLensTick = 0;          // bump to invalidate the layer getFillColor cache
let _demoLensStats = null;     // { city, field, lo, hi } robust p2..p98 range
let _demoLensPropsCache = null;// { city, map: Map<desoCode, props> }

function demoLensActive() { return !!demoLensField; }
function demoLensFieldDef(key = demoLensField) { return DEMO_LENS_FIELDS.find(f => f.key === key) || null; }

// DESO code -> baked props, cached per city (rebuilt when EPI_DEMO changes city).
function _demoLensPropsMap() {
  const city = (typeof EPI_DEMO !== 'undefined' && EPI_DEMO) ? EPI_DEMO.city : null;
  if (!city) return null;
  if (_demoLensPropsCache && _demoLensPropsCache.city === city) return _demoLensPropsCache.map;
  const map = new Map();
  for (const f of (EPI_DEMO.features || [])) {
    const p = f.properties || {};
    if (p.deso != null) map.set(p.deso, p);
  }
  _demoLensPropsCache = { city, map };
  return map;
}

function _demoLensPercentile(sorted, p) {
  const n = sorted.length;
  if (!n) return 0;
  const i = (n - 1) * p;
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? sorted[lo] : sorted[lo] * (1 - (i - lo)) + sorted[hi] * (i - lo);
}

// Robust [p2, p98] range of the active field across all DESOs, cached per
// (city, field). Robust clamp keeps a single outlier DESO from washing out the
// ramp. Returns null when there's no usable spread.
function _demoLensStatsFor(field) {
  const city = (typeof EPI_DEMO !== 'undefined' && EPI_DEMO) ? EPI_DEMO.city : null;
  if (!city || !field) return null;
  if (_demoLensStats && _demoLensStats.city === city && _demoLensStats.field === field) return _demoLensStats;
  const vals = [];
  for (const f of (EPI_DEMO.features || [])) {
    const v = Number(f.properties?.[field]);
    if (Number.isFinite(v)) vals.push(v);
  }
  if (vals.length < 2) { _demoLensStats = { city, field, lo: 0, hi: 1 }; return _demoLensStats; }
  vals.sort((a, b) => a - b);
  let lo = _demoLensPercentile(vals, 0.02);
  let hi = _demoLensPercentile(vals, 0.98);
  if (!(hi > lo)) { lo = vals[0]; hi = vals[vals.length - 1]; }
  if (!(hi > lo)) hi = lo + 1;
  _demoLensStats = { city, field, lo, hi, min: vals[0], max: vals[vals.length - 1] };
  return _demoLensStats;
}

// Raw field value for a building (via its DESO), or null when unknown.
function demoLensRawForFeature(feature) {
  if (!demoLensField) return null;
  const code = feature?.properties?.__deso;
  if (code == null) return null;
  const map = _demoLensPropsMap();
  const props = map ? map.get(code) : null;
  if (!props) return null;
  const v = Number(props[demoLensField]);
  return Number.isFinite(v) ? v : null;
}

// Normalised 0..1 position of a building on the active field's robust range.
function demoLensNormForFeature(feature) {
  const raw = demoLensRawForFeature(feature);
  if (raw == null) return null;
  const st = _demoLensStatsFor(demoLensField);
  if (!st) return null;
  return Math.max(0, Math.min(1, (raw - st.lo) / (st.hi - st.lo)));
}

// Sequential magnitude ramp (YlOrRd): low = pale yellow, high = deep red, so the
// areas with the most of a given attribute draw the eye. Distinct from the
// fairness ramp on purpose, so a lens is never mistaken for accessibility.
function demoLensRampColor(t) {
  const s = Math.max(0, Math.min(1, Number(t) || 0));
  if (typeof d3 !== 'undefined' && d3.interpolateYlOrRd && d3.color) {
    const c = d3.color(d3.interpolateYlOrRd(0.12 + 0.88 * s));
    if (c) return [Math.round(c.r), Math.round(c.g), Math.round(c.b)];
  }
  // Fallback linear yellow→red if d3 is unavailable.
  return [255, Math.round(255 * (1 - s)), Math.round(40 * (1 - s))];
}

// Building fill color under the lens; grey for buildings with no DESO match.
function demoLensColorForFeature(feature) {
  const norm = demoLensNormForFeature(feature);
  if (norm == null) return [120, 120, 120, 140];
  return demoLensRampColor(norm);
}

// Repurpose the side-panel legend for the lens (gradient + real min/max labels).
function updateDemoLensLegend() {
  const bar = document.getElementById('fairnessLegendBar');
  const def = demoLensFieldDef();
  if (!bar || !def) return;
  const stops = [0, 0.25, 0.5, 0.75, 1].map((t) => {
    const [r, g, b] = demoLensRampColor(t);
    return `rgb(${r}, ${g}, ${b}) ${Math.round(t * 100)}%`;
  });
  bar.style.background = `linear-gradient(90deg, ${stops.join(', ')})`;
  const st = _demoLensStatsFor(demoLensField);
  const left = document.getElementById('spLegendLeft');
  const mid = document.getElementById('spLegendMid');
  const right = document.getElementById('spLegendRight');
  if (st) {
    if (left) left.textContent = def.fmt(st.lo);
    if (mid) mid.textContent = def.fmt((st.lo + st.hi) / 2);
    if (right) right.textContent = def.fmt(st.hi);
  }
  const note = document.getElementById('spNote');
  if (note) note.textContent = `Building color = ${def.label} of its DESO (yellow→red = low→high). ${def.hint}`;
}

// Restore the fairness legend labels/note when the lens turns off.
function restoreFairnessLegend() {
  const left = document.getElementById('spLegendLeft');
  const mid = document.getElementById('spLegendMid');
  const right = document.getElementById('spLegendRight');
  if (left) left.textContent = 'Least fair';
  if (mid) mid.textContent = 'Medium';
  if (right) right.textContent = 'Most fair';
  const note = document.getElementById('spNote');
  if (note) note.textContent = 'Building color = accessibility score (near = greener).';
  if (typeof updateFairnessLegendUI === 'function') updateFairnessLegendUI();
}

function setDemoLensField(key, opts = {}) {
  const { refreshLayers = true } = opts;
  const next = DEMO_LENS_FIELDS.some(f => f.key === key) ? key : '';
  if (next === demoLensField) return;
  demoLensField = next;
  demoLensTick++;
  const sel = document.getElementById('demoLensField');
  if (sel && sel.value !== demoLensField) sel.value = demoLensField;
  if (demoLensField) updateDemoLensLegend();
  else restoreFairnessLegend();
  if (refreshLayers && typeof updateLayers === 'function') updateLayers();
}

// Populate + bind the lens selector. Idempotent (guarded by __bound).
function bindDemoLensSelect() {
  const sel = document.getElementById('demoLensField');
  if (!sel) return;
  if (!sel.options.length) {
    sel.add(new Option('Off (fairness)', ''));
    for (const f of DEMO_LENS_FIELDS) sel.add(new Option(f.label, f.key));
  }
  if (!sel.__bound) {
    sel.addEventListener('change', () => setDemoLensField(sel.value));
    sel.__bound = true;
  }
  sel.value = demoLensField;
}

document.addEventListener('DOMContentLoaded', bindDemoLensSelect);
