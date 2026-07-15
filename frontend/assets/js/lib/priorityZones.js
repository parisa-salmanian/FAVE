// Priority zones — a planner's "where to intervene first" overlay. A building is
// a priority when it combines HIGH social need (its DESO's chosen need signal)
// with POOR accessibility (low fairness score = far from services). Highlighting
// the intersection points planners at the underserved-and-vulnerable areas where
// new services would do the most good.
//
//   priority P = needNorm × accessGap
//     needNorm  = building's DESO need mapped onto the city's robust need range
//                 (need signal is selectable: deprivation / children / elderly /
//                  low income / low education)
//     accessGap = 1 − fairness score (overall accessibility; higher gap = worse)
//
// When active, buildings with P below the threshold are dimmed to a neutral grey
// and those at/above it are painted on an orange→deep-red ramp by P, so the
// priority pockets stand out. Reuses the demographic-lens need stats + __deso
// stamp (lib/demographicLens.js, models/epicityDemographics.js); no network.

let priorityZonesActive = false;
let priorityZonesTick = 0;
let priorityTopFrac = 0.20;          // flag the worst 20% of buildings by priority P
let priorityNeedField = 'needZ';     // which DESO need signal drives the score

// Adaptive cutoff: the raw P scale depends on the need signal (needZ is peaky,
// child/elder shares are broad), so a fixed threshold flags wildly different
// counts. Instead we flag the top `priorityTopFrac` of the city's own P
// distribution — always a crisp minority — recomputed when the city, need
// signal, or fairness result changes. `_pzCutoff` is the P at that quantile.
let _pzCutoff = null;
let _pzMaxP = 1;
let _pzNeedLo = 0;   // robust p2..p98 need range on the ACTIVE scale (buildings/cells/districts)
let _pzNeedHi = 1;

// Selectable need signals. `field` = key in the baked DESO props (deso.geojson).
// `invert` = true when a LOW raw value means HIGH need (income, higher-ed share),
// so the deprived end maps to needNorm ≈ 1. `label` is used in the legend note.
const PRIORITY_NEED_META = {
  needZ:      { label: 'deprivation',   invert: false },
  child_frac: { label: 'children',      invert: false },
  elder_frac: { label: 'elderly',       invert: false },
  income:     { label: 'low income',    invert: true  },
  higher_ed:  { label: 'low education', invert: true  },
};

// The overlay is active when the shell legend's Priority tab is selected
// (mapColorVar === 'priority', owned by lib/supplyProvisionLens.js) — the single
// source of truth. priorityZonesActive is kept mirrored for the layer
// updateTriggers, and is the fallback if the supply-lens module isn't present.
function priorityZonesOn() {
  if (typeof mapColorVar !== 'undefined') return mapColorVar === 'priority';
  return !!priorityZonesActive;
}

// Fields for the SELECTED need signal. A props-bag may be a building feature's
// properties (resolves via __deso), a meso hex CELL (fields live directly on the
// cell), or a district feature's properties — meso/macro carry the aggregated
// __*Real / __needZ; buildings carry only __deso. _PZ_CELL_KEY maps each need
// field to its aggregated key; buildings fall through to the DESO lookup.
const _PZ_CELL_KEY = { needZ: '__needZ', child_frac: '__childReal', elder_frac: '__elderReal', income: '__incomeReal', higher_ed: '__higherEdReal' };

// Overall accessibility score (0..1) for a props-bag, across all three scales:
// meso/macro carry __fairOverall / __score; buildings carry fair_overall / fair.
function _pzAccessFromProps(p) {
  if (!p) return null;
  let s = null;
  if (Number.isFinite(p.__fairOverall)) s = p.__fairOverall;
  else if (Number.isFinite(p.__score)) s = p.__score;
  else if (p.fair_overall && Number.isFinite(p.fair_overall.score)) s = p.fair_overall.score;
  else if (p.fair && Number.isFinite(p.fair.score)) s = p.fair.score;
  return Number.isFinite(s) ? Math.max(0, Math.min(1, s)) : null;
}

// Raw need value for a props-bag: meso/macro read the aggregated cell field;
// buildings resolve their DESO's value via the demographic-lens props map.
function _pzNeedRawFromProps(p) {
  if (!p) return NaN;
  const field = PRIORITY_NEED_META[priorityNeedField] ? priorityNeedField : 'needZ';
  const ck = _PZ_CELL_KEY[field];
  if (ck && Number.isFinite(Number(p[ck]))) return Number(p[ck]);   // meso / macro aggregate
  const code = p.__deso;                                            // building → DESO lookup
  if (code == null || typeof _demoLensPropsMap !== 'function') return NaN;
  const map = _demoLensPropsMap();
  const dp = map ? map.get(code) : null;
  if (!dp) return NaN;
  const v = Number(dp[field]);
  return Number.isFinite(v) ? v : NaN;
}

// Need normalised to 0..1 on the ACTIVE scale's robust range (_pzNeedLo/_pzNeedHi,
// set by _pzRecomputeCutoff over the features currently on the map). Inverted
// signals (income, higher-ed share) flip so the deprived end maps to ≈1.
function _pzNeedNormFromProps(p) {
  const v = _pzNeedRawFromProps(p);
  if (!Number.isFinite(v)) return null;
  const span = (_pzNeedHi > _pzNeedLo) ? (_pzNeedHi - _pzNeedLo) : 1;
  let t = Math.max(0, Math.min(1, (v - _pzNeedLo) / span));
  if ((PRIORITY_NEED_META[priorityNeedField] || {}).invert) t = 1 - t;
  return t;
}

// Priority score P in 0..1 (need × access-gap) for a props-bag, or null when
// inputs are missing.
function priorityScoreFromProps(p) {
  const need = _pzNeedNormFromProps(p);
  const access = _pzAccessFromProps(p);
  if (need == null || access == null) return null;
  return need * (1 - access);
}

// Building-feature convenience wrapper (the building map layer passes features).
function priorityScoreForFeature(feature) {
  return priorityScoreFromProps(feature?.properties || null);
}

// Orange→deep-red ramp for priority magnitude (above the threshold).
function _pzRampColor(t) {
  const s = Math.max(0, Math.min(1, Number(t) || 0));
  if (typeof d3 !== 'undefined' && d3.interpolateOrRd && d3.color) {
    const c = d3.color(d3.interpolateOrRd(0.25 + 0.75 * s));
    if (c) return [Math.round(c.r), Math.round(c.g), Math.round(c.b)];
  }
  return [255, Math.round(160 * (1 - s)), Math.round(40 * (1 - s))];
}

// Linear-interpolated quantile of an already-sorted ascending numeric array.
function _pzQuantile(sorted, p) {
  const n = sorted.length;
  if (!n) return 0;
  const i = (n - 1) * Math.max(0, Math.min(1, p));
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? sorted[lo] : sorted[lo] * (1 - (i - lo)) + sorted[hi] * (i - lo);
}

// Props-bags for the features currently rendered: building properties at micro,
// hex CELLS at meso (fields directly on the cell), district properties at macro.
// Priority colours whichever layer is showing, so the cutoff is scale-relative.
function _pzActivePropsList() {
  if (typeof mezoView !== 'undefined' && mezoView && typeof mezoHexData !== 'undefined' && Array.isArray(mezoHexData)) {
    return mezoHexData;
  }
  if (typeof districtView !== 'undefined' && districtView && typeof districtFC !== 'undefined' && districtFC && districtFC.features) {
    return districtFC.features.map((f) => f.properties || {});
  }
  return ((typeof baseCityFC !== 'undefined' && baseCityFC && baseCityFC.features) ? baseCityFC.features : []).map((f) => f.properties || {});
}

// Recompute the robust need range + adaptive cutoff over the ACTIVE scale's
// features (micro buildings / meso cells / macro districts), for the current
// need signal + fairness. One O(N) scan; call on city / need field / scale /
// fairness change.
function _pzRecomputeCutoff() {
  const list = _pzActivePropsList();
  // 1) robust p2..p98 range of the need signal on this scale
  const nvals = [];
  for (const p of list) { const v = _pzNeedRawFromProps(p); if (Number.isFinite(v)) nvals.push(v); }
  if (nvals.length >= 2) {
    nvals.sort((a, b) => a - b);
    _pzNeedLo = _pzQuantile(nvals, 0.02);
    _pzNeedHi = _pzQuantile(nvals, 0.98);
    if (!(_pzNeedHi > _pzNeedLo)) { _pzNeedLo = nvals[0]; _pzNeedHi = nvals[nvals.length - 1]; }
    if (!(_pzNeedHi > _pzNeedLo)) _pzNeedHi = _pzNeedLo + 1;
  } else { _pzNeedLo = 0; _pzNeedHi = 1; }
  // 2) top-fraction cutoff of the P distribution on this scale
  const ps = [];
  for (const p of list) { const P = priorityScoreFromProps(p); if (P != null) ps.push(P); }
  if (!ps.length) { _pzCutoff = null; _pzMaxP = 1; return; }
  ps.sort((a, b) => a - b);
  _pzCutoff = _pzQuantile(ps, 1 - priorityTopFrac);
  _pzMaxP = ps[ps.length - 1];
  if (!(_pzMaxP > _pzCutoff)) _pzCutoff = ps[0];   // degenerate spread → show all scored
}

// Fill for a props-bag under the priority overlay: dim grey below the adaptive
// cutoff (and where need/access is missing), ramped orange→deep-red above it.
// Used by the building, meso-hex, and district map layers alike.
function priorityColorFromProps(p) {
  const P = priorityScoreFromProps(p);
  if (P == null || _pzCutoff == null || P < _pzCutoff) return [105, 105, 110, 120];
  const span = (_pzMaxP > _pzCutoff) ? (_pzMaxP - _pzCutoff) : 1;
  const t = Math.max(0, Math.min(1, (P - _pzCutoff) / span));
  return [..._pzRampColor(t), 235];
}

// Building map-layer accessor (views/layers.js getFillColor passes a feature).
function priorityColorForFeature(feature) {
  return priorityColorFromProps(feature?.properties || null);
}

// Count of flagged features at the ACTIVE scale (for the legend/status read-out).
function priorityZonesCount() {
  if (_pzCutoff == null) return 0;
  let n = 0;
  for (const p of _pzActivePropsList()) {
    const P = priorityScoreFromProps(p);
    if (P != null && P >= _pzCutoff) n++;
  }
  return n;
}

function _pzUpdateLegend() {
  const left = document.getElementById('spLegendLeft');
  const mid = document.getElementById('spLegendMid');
  const right = document.getElementById('spLegendRight');
  if (left) left.textContent = 'Not priority';
  if (mid) mid.textContent = 'Moderate';
  if (right) right.textContent = 'High priority';
  const bar = document.getElementById('fairnessLegendBar');
  if (bar) {
    const stops = [0, 0.5, 1].map((t) => {
      const [r, g, b] = _pzRampColor(t);
      return `rgb(${r}, ${g}, ${b}) ${Math.round(t * 100)}%`;
    });
    bar.style.background = `linear-gradient(90deg, rgb(105,105,110) 0%, ${stops.join(', ')})`;
  }
  const note = document.getElementById('spNote');
  if (note) {
    const n = priorityZonesCount();
    const lbl = (PRIORITY_NEED_META[priorityNeedField] || {}).label || 'need';
    note.textContent = `Priority = ${lbl} × poor access. ${n.toLocaleString()} ${_pzUnitLabel()} flagged (worst ${Math.round(priorityTopFrac * 100)}%).`;
  }
  _pzUpdateShellLegend();
}

// Unit noun for the active map scale, for the legend read-out.
function _pzUnitLabel() {
  if (typeof mezoView !== 'undefined' && mezoView) return 'cells';
  if (typeof districtView !== 'undefined' && districtView) return 'districts';
  return 'buildings';
}

// The visible bottom-left legend (.shell-legend, owned by supplyProvisionLens)
// is repurposed to the priority ramp/labels/note/active-tab while the overlay is
// active, so the map is self-explanatory. Restored via refreshLegend() when a
// different tab is picked (see setMapColorVar → priorityLeave).
function _pzUpdateShellLegend() {
  const leg = document.querySelector('.shell-legend');
  if (!leg) return;
  const spans = leg.querySelectorAll('.ramp-labels span');
  const labels = ['Not priority', 'Moderate', 'High priority'];
  if (spans.length >= 3) labels.forEach((t, i) => { if (spans[i]) spans[i].textContent = t; });
  const ramp = leg.querySelector('.ramp');
  if (ramp) {
    const stops = [0, 0.5, 1].map((t) => {
      const [r, g, b] = _pzRampColor(t);
      return `rgb(${r}, ${g}, ${b}) ${Math.round(50 + t * 50)}%`;
    });
    ramp.style.background = `linear-gradient(90deg, rgb(105,105,110) 0%, rgb(105,105,110) 45%, ${stops.join(', ')})`;
  }
  const noteEl = leg.querySelector('#legendNote');
  if (noteEl) {
    const lbl = (PRIORITY_NEED_META[priorityNeedField] || {}).label || 'need';
    noteEl.textContent = `Priority zones = ${lbl} × poor access (worst ${Math.round(priorityTopFrac * 100)}% highlighted; grey = not a priority).`;
  }
  // Mark the Priority tab active (mirrors supplyProvisionLens updateLegendRamp).
  leg.querySelectorAll('.legend-tab').forEach((b) => {
    b.setAttribute('data-active', String(b.dataset.colorvar === 'priority'));
  });
}

// Show/hide the need-lens <select> row in the shell legend (Priority tab only).
function _pzShowNeedSelect(show) {
  const row = document.getElementById('legendPriorityNeed');
  if (row) { if (show) row.removeAttribute('hidden'); else row.setAttribute('hidden', ''); }
}

// Keep the need selectors (shell #priorityNeedSel + legacy #priorityNeedField)
// and the legacy checkbox in sync with the current field / active state.
function _pzSyncControls() {
  const on = priorityZonesOn();
  const shellSel = document.getElementById('priorityNeedSel');
  if (shellSel && shellSel.value !== priorityNeedField) shellSel.value = priorityNeedField;
  const legacySel = document.getElementById('priorityNeedField');
  if (legacySel) { legacySel.value = priorityNeedField; legacySel.disabled = !on; }
  const chk = document.getElementById('priorityZonesToggle');
  if (chk && chk.checked !== on) chk.checked = on;
}

// Enter/leave the priority overlay — called by setMapColorVar when the Priority
// tab is selected / deselected. Enter owns the legend; leave lets the caller's
// refreshLegend() restore it.
function priorityEnter() {
  priorityZonesActive = true;
  priorityZonesTick++;
  _pzRecomputeCutoff();
  _pzShowNeedSelect(true);
  _pzSyncControls();
  _pzUpdateLegend();            // side-panel + shell legend + active tab
}
function priorityLeave() {
  priorityZonesActive = false;
  priorityZonesTick++;
  _pzShowNeedSelect(false);
  _pzSyncControls();
  if (typeof restoreFairnessLegend === 'function') restoreFairnessLegend();
}

// Switch which DESO need signal drives the priority score (deprivation / children
// / elderly / low income / low education). Repaints when the overlay is active.
function setPriorityNeedField(key, opts = {}) {
  const { refreshLayers = true } = opts;
  const next = PRIORITY_NEED_META[key] ? key : 'needZ';
  if (next === priorityNeedField) { _pzSyncControls(); return; }
  priorityNeedField = next;
  priorityZonesTick++;
  _pzSyncControls();
  if (priorityZonesOn()) {
    _pzRecomputeCutoff();
    _pzUpdateLegend();
    if (refreshLayers && typeof updateLayers === 'function') updateLayers();
  }
}

// Legacy checkbox entry point → route through the shell tab system so the tab,
// the legacy checkbox, and mapColorVar (single source of truth) stay consistent.
function setPriorityZones(on) {
  if (typeof setMapColorVar === 'function') { setMapColorVar(on ? 'priority' : 'fairness'); return; }
  // Fallback when the supply-lens module is absent: drive the overlay directly.
  if (on) priorityEnter(); else priorityLeave();
  if (typeof updateLayers === 'function') updateLayers();
}

// Re-derive the adaptive cutoff after the underlying fairness changes (travel
// mode switch, what-if recompute) and repaint — only when the overlay is on.
function notifyPriorityDataChanged() {
  if (!priorityZonesOn()) return;
  _pzRecomputeCutoff();
  _pzUpdateLegend();
  priorityZonesTick++;
  if (typeof updateLayers === 'function') updateLayers();
}

// Bind the legacy checkbox (hidden in the shell) if present. The shell tab is
// handled by supplyProvisionLens's delegated click; the shell need-select is
// bound by delegation below (both mount asynchronously).
function bindPriorityZonesToggle() {
  const chk = document.getElementById('priorityZonesToggle');
  if (chk && !chk.__bound) {
    chk.addEventListener('change', () => setPriorityZones(chk.checked));
    chk.__bound = true;
    chk.checked = priorityZonesOn();
  }
  const sel = document.getElementById('priorityNeedField');
  if (sel && !sel.__bound) {
    sel.addEventListener('change', () => setPriorityNeedField(sel.value));
    sel.__bound = true;
    sel.value = priorityNeedField;
    sel.disabled = !priorityZonesOn();
  }
}

// The shell legend's need-select (#priorityNeedSel) mounts after this script.
document.addEventListener('change', (e) => {
  if (e.target && e.target.id === 'priorityNeedSel') setPriorityNeedField(e.target.value);
});

document.addEventListener('DOMContentLoaded', bindPriorityZonesToggle);
