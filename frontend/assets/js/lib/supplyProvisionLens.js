// supplyProvisionLens.js — adds a "Supply provision" map-coloring mode as a
// first-class sibling to the headline fairness coloring, switchable from the
// bottom-left legend's tabs.
//
//   • Fairness (default): buildings colored by the gravity accessibility score
//     (props.fair.score) — "how close are opportunities".
//   • Supply provision: buildings colored by the network-accurate E2SFCA overall
//     score (access2sfca.js → access2sfcaForFeature(...).overall) — "how much
//     supply is actually available once competing demand is counted".
//
// Both paint on the SAME green→purple ramp (colorFromScore) on purpose, so the
// two views are directly comparable on one click.
//
// The 2SFCA overall is independent of the fairness compute AND of the selected
// POI mix — it depends only on (city, travel mode). So we stamp every building
// feature's props._supplyScore once per (city|mode) and reuse it until the city
// or travel mode changes. Buildings with no baked 2SFCA row paint as no-data grey.
//
// This module owns the legend's ramp labels / note / tab state / scale-enabled
// flag via updateLegendRamp(); the experimental absolute-scale module
// (lib/absoluteColorScale.js) defers to it through window.faveRefreshLegend and
// is queried through window.fairAbsoluteScaleActive — both typeof-guarded, so
// either module works with the other absent.

let mapColorVar = 'fairness';      // 'fairness' | 'supply' — active map color variable
let supplyTick = 0;                // bump to invalidate the layer getFillColor cache

let _supplyStampSig = null;        // `${city}|${mode}` last stamped
let _supplyStampedFC = null;       // baseCityFC reference last stamped (city reload swaps it)
let _supplyStampInFlight = false;

// Grey for buildings with no baked 2SFCA row — matches demographicLens no-match.
const SUPPLY_NODATA_COLOR = [120, 120, 120, 140];

function supplyActive() { return mapColorVar === 'supply'; }
function mismatchActive() { return mapColorVar === 'mismatch'; }
// Both supply and mismatch views need the per-building 2SFCA score stamped.
function _needsSupplyStamp() { return mapColorVar === 'supply' || mapColorVar === 'mismatch'; }

// Linear-interpolated quantile of an already-sorted ascending numeric array.
function _supplyQuantile(sorted, p) {
  const n = sorted.length;
  if (!n) return 0;
  const i = (n - 1) * p;
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? sorted[lo] : sorted[lo] * (1 - (i - lo)) + sorted[hi] * (i - lo);
}

// Per-variable ramp labels + explanatory note. The fairness variant has two
// flavors because the experimental absolute scale relabels the same ramp.
const SUPPLY_LEGEND_TEXT = {
  fairnessRelative: {
    labels: ['Least fair', 'Medium', 'Most fair'],
    note: 'Building color = accessibility fairness (greener = fairer).',
  },
  fairnessAbsolute: {
    labels: ['Under-served', 'Medium', 'Well-served'],
    note: 'Building color = provision on a fixed absolute scale (greener = better served).',
  },
  supply: {
    labels: ['Least supply', 'Medium', 'Most supply'],
    note: 'Building color = 2SFCA supply provision — how much supply is available '
        + 'once competing demand is counted (greener = more).',
  },
  mismatch: {
    labels: ['Close but crowded', 'Agree', 'Supply-rich'],
    note: 'Building color = where the two views disagree. Red = close to services '
        + 'but less actual supply (crowding); blue = more supply than proximity '
        + 'implies; pale = the two agree.',
  },
};

// Plain-English explanation for each tab, toggled by its "?" (mirrors the rich
// chat answers). Rendered as HTML into #legendHelp — keep the markup to <b>.
const TAB_HELP = {
  fairness:
    '<b>Fairness — how close are opportunities.</b> Each building is colored by '
    + 'its network travel time to the selected services (greener = nearer). It '
    + 'measures proximity only — it does not account for how many people compete '
    + 'for those services.',
  supply:
    '<b>Supply provision (2SFCA) — how much is actually available.</b> Colors each '
    + 'building by the supply it can reach once everyone competing for it is counted '
    + '(greener = more supply per resident). Network-accurate and normalized within '
    + 'each service type, then spread across the city so the map stays readable. The '
    + 'inspector shows the absolute per-service %.',
  mismatch:
    '<b>Mismatch — where the two views disagree.</b> Proximity (Fairness) minus '
    + 'available supply (2SFCA). <b>Red</b> = close to services but less actual '
    + 'supply (crowding); <b>blue</b> = more supply than proximity implies; pale = '
    + 'the two agree. Red zones are typically dense central blocks — near services, '
    + 'but many residents share each one.',
  priority:
    '<b>Priority — where to intervene first.</b> Highlights the worst 20% of '
    + 'buildings by <b>need × poor access</b>: high social need (pick the need '
    + 'signal above — deprivation, children, elderly, low income, low education) '
    + 'AND far from services. Grey = not a priority. Needs a fairness compute first '
    + '(it reads the overall accessibility score). Lasso a red pocket to explain it '
    + 'in the DR / EBM panel.',
};
let _openHelpKey = null;   // which tab's explanation is currently shown (or null)

// Diverging ramp shown in the legend for the Mismatch view (red→pale→blue,
// matching d3.interpolateRdBu used for the building colors).
const MISMATCH_RAMP_CSS =
  'linear-gradient(to right, #b2182b, #ef8a62, #fddbc7, #f7f7f7, #d1e5f0, #67a9cf, #2166ac)';
const MISMATCH_CLAMP = 0.5;   // |fair − supply| beyond this saturates the color

// Travel mode for the 2SFCA layer, mirroring inspector.js _a2sModeFromUI.
function _supplyMode() {
  const m = (document.getElementById('fairnessTravelMode')?.value || 'walking').toLowerCase();
  return (m === 'walking' || m === 'cycling' || m === 'driving' || m === 'transit') ? m : 'walking';
}

// Which legend text set applies right now: supply / mismatch tabs, or fairness
// with/without the experimental absolute scale active.
function _supplyLegendMode() {
  if (mapColorVar === 'supply') return 'supply';
  if (mapColorVar === 'mismatch') return 'mismatch';
  const abs = (typeof window.fairAbsoluteScaleActive === 'function') && window.fairAbsoluteScaleActive();
  return abs ? 'fairnessAbsolute' : 'fairnessRelative';
}

// Set the legend tabs' active state, ramp labels, note, and the scale-control
// enabled flag (Relative/Absolute is a fairness-only concept). Pure DOM write,
// safe to call repeatedly.
function updateLegendRamp() {
  const leg = document.querySelector('.shell-legend');
  if (!leg) return;
  // The priority overlay owns the legend while active — let it keep it, even if
  // another module fires refreshLegend() (e.g. the absolute-scale hook).
  if (mapColorVar === 'priority') {
    if (typeof _pzUpdateShellLegend === 'function') _pzUpdateShellLegend();
    return;
  }
  const conf = SUPPLY_LEGEND_TEXT[_supplyLegendMode()];
  const spans = leg.querySelectorAll('.ramp-labels span');
  if (spans.length >= 3) conf.labels.forEach((t, i) => { if (spans[i]) spans[i].textContent = t; });
  const note = leg.querySelector('#legendNote');
  if (note) note.textContent = conf.note;
  // Mismatch uses a diverging ramp; Fairness/Supply use the stylesheet ramp.
  const ramp = leg.querySelector('.ramp');
  if (ramp) ramp.style.background = (mapColorVar === 'mismatch') ? MISMATCH_RAMP_CSS : '';
  leg.querySelectorAll('.legend-tab').forEach((b) => {
    b.setAttribute('data-active', String(b.dataset.colorvar === mapColorVar));
  });
  // Relative/Absolute only means something for fairness — grey it out elsewhere.
  const slot = leg.querySelector('#legendScaleSlot');
  if (slot) slot.setAttribute('data-disabled', String(mapColorVar !== 'fairness'));
}

// Refresh the whole legend: ramp text (here) + the experimental below-bar row
// (absolute module, if present). Both modules call this so the legend stays
// consistent no matter which one triggered the change.
function refreshLegend() {
  updateLegendRamp();
  if (typeof window.fairApplyBelowBarRow === 'function') window.fairApplyBelowBarRow();
}

// Stamp props._supplyScore (0..1 overall 2SFCA provision) onto every building
// feature for the current city+mode. Guarded by signature + FC reference so it
// recomputes only when the city, the travel mode, or the loaded FC changes.
async function ensureSupplyStamped() {
  if (typeof ensureAccess2sfca !== 'function' || typeof access2sfcaForFeature !== 'function') return false;
  if (typeof baseCityFC === 'undefined' || !baseCityFC || !baseCityFC.features) return false;
  const city = (typeof a2sCurrentCity === 'function') ? a2sCurrentCity() : undefined;
  const mode = _supplyMode();
  const sig = `${city}|${mode}`;
  if (_supplyStampSig === sig && _supplyStampedFC === baseCityFC) return true;
  if (_supplyStampInFlight) return false;
  _supplyStampInFlight = true;
  try {
    const net = await ensureAccess2sfca(city, mode);
    if (!net || !baseCityFC || !baseCityFC.features) return false;
    const feats = baseCityFC.features;
    // Pass 1: raw overall (mean of per-category provision) per building.
    const raws = new Array(feats.length);
    const present = [];
    for (let i = 0; i < feats.length; i++) {
      const res = access2sfcaForFeature(feats[i]);
      const r = (res && Number.isFinite(res.overall)) ? res.overall : null;
      raws[i] = r;
      if (r != null) present.push(r);
    }
    // The raw overall is the mean across ~30 service types, most unreachable on
    // foot (counted as 0), so it bunches into the bottom of 0..1 (city-wide max
    // ≈ 0.5) — coloring it directly leaves the whole map purple. So we spread it
    // across the ramp with the SAME city-relative robust clamp fairness and the
    // demographic lens use (p2..p98), making relative provision legible. The
    // inspector keeps showing the absolute per-category %.
    present.sort((a, b) => a - b);
    const lo = _supplyQuantile(present, 0.02);
    const hi = _supplyQuantile(present, 0.98);
    const span = (hi > lo) ? (hi - lo) : 0;
    for (let i = 0; i < feats.length; i++) {
      const props = feats[i].properties || (feats[i].properties = {});
      const r = raws[i];
      if (r == null) { props._supplyScore = null; continue; }
      props._supplyRaw = r;   // absolute mean provision (kept for reference)
      props._supplyScore = span ? Math.max(0, Math.min(1, (r - lo) / span)) : r;
    }
    _supplyStampSig = sig;
    _supplyStampedFC = baseCityFC;
    return true;
  } catch (e) {
    console.warn('[supply-lens] stamp failed', e);
    return false;
  } finally {
    _supplyStampInFlight = false;
  }
}

// Synchronous color accessor for views/layers.js getFillColor.
function supplyColorForFeature(feature) {
  const s = feature?.properties?._supplyScore;
  if (Number.isFinite(s) && typeof colorFromScore === 'function') return colorFromScore(s);
  return SUPPLY_NODATA_COLOR;
}

// Mismatch color: proximity (Fairness, all-category overall) minus available
// supply (2SFCA), on a diverging red↔blue scale. Red = closer than it is
// supplied (crowding); blue = more supply than proximity implies; pale = agree.
// Prefers fair_overall (all categories) so it isolates competition rather than
// the selected POI mix; falls back to the active fairness score. Grey when
// either score is missing (no fairness computed, or no baked 2SFCA row).
function mismatchColorForFeature(feature) {
  const p = feature?.properties;
  const fair = p && (p.fair_overall && Number.isFinite(p.fair_overall.score)
    ? p.fair_overall.score
    : (p.fair && Number.isFinite(p.fair.score) ? p.fair.score : null));
  const sup = p ? p._supplyScore : null;
  if (!Number.isFinite(fair) || !Number.isFinite(sup)) return SUPPLY_NODATA_COLOR;
  const d = Math.max(-MISMATCH_CLAMP, Math.min(MISMATCH_CLAMP, fair - sup));
  const t = 0.5 - d / (2 * MISMATCH_CLAMP);   // d=+clamp → 0 (red); d=−clamp → 1 (blue)
  if (typeof d3 !== 'undefined' && d3.interpolateRdBu && d3.color) {
    const c = d3.color(d3.interpolateRdBu(t));
    if (c) return [Math.round(c.r), Math.round(c.g), Math.round(c.b)];
  }
  // Fallback diverging red→white→blue if d3 is unavailable.
  const r = t < 0.5 ? 255 : Math.round(255 * (1 - (t - 0.5) * 2));
  const b = t > 0.5 ? 255 : Math.round(255 * (t * 2));
  const g = Math.round(255 * (1 - Math.abs(t - 0.5) * 2));
  return [r, g, b];
}

// Switch the active map color variable (legend tab click). Repaints immediately,
// then (for supply) stamps the 2SFCA scores async and repaints again once ready.
// 'priority' is the need × poor-access overlay (lib/priorityZones.js): it owns
// the legend while active, so we hand off to priorityEnter/priorityLeave instead
// of refreshLegend().
async function setMapColorVar(v) {
  const next = (v === 'supply' || v === 'mismatch' || v === 'priority') ? v : 'fairness';
  if (next === mapColorVar) return;
  const leavingPriority = (mapColorVar === 'priority');
  mapColorVar = next;
  supplyTick++;
  if (next === 'priority') {
    if (typeof priorityEnter === 'function') priorityEnter();   // recompute cutoff + own the legend
  } else {
    if (leavingPriority && typeof priorityLeave === 'function') priorityLeave();
    refreshLegend();
  }
  if (typeof updateLayers === 'function') updateLayers();   // immediate (grey while stamping)
  if (_needsSupplyStamp()) {
    const ok = await ensureSupplyStamped();
    if (ok) { supplyTick++; if (typeof updateLayers === 'function') updateLayers(); }
  }
}

// Re-stamp when the underlying data (city / travel mode) changes, if supply is
// the active view. Idempotent; a cheap no-op when fairness is showing (it just
// drops the cached signature so the next supply switch re-stamps fresh).
async function notifySupplyDataChanged() {
  if (!_needsSupplyStamp()) { _supplyStampSig = null; return; }
  _supplyStampSig = null;                       // force re-stamp for the new city/mode
  const ok = await ensureSupplyStamped();
  if (ok) { supplyTick++; if (typeof updateLayers === 'function') updateLayers(); }
}

// Snap the map back to the Fairness view. Called on city change — the Supply /
// Mismatch views are per-city, so they shouldn't persist when the city switches.
// Also clears the stamp signature + any open help panel for the fresh city.
function resetMapColorVarToFairness() {
  _supplyStampSig = null;
  const leg = document.querySelector('.shell-legend');
  const panel = leg && leg.querySelector('#legendHelp');
  if (panel) panel.setAttribute('hidden', '');
  _openHelpKey = null;
  if (leg) leg.querySelectorAll('.legend-tab-help').forEach((b) => b.setAttribute('aria-expanded', 'false'));
  const changed = mapColorVar !== 'fairness';
  mapColorVar = 'fairness';
  refreshLegend();
  if (changed) { supplyTick++; if (typeof updateLayers === 'function') updateLayers(); }
}

// Show/hide a tab's plain-English explanation in #legendHelp. Clicking the same
// "?" again closes it; clicking another swaps the text and keeps it open.
function toggleTabHelp(key) {
  const leg = document.querySelector('.shell-legend');
  if (!leg) return;
  const panel = leg.querySelector('#legendHelp');
  if (!panel) return;
  _openHelpKey = (_openHelpKey === key) ? null : key;
  if (_openHelpKey) { panel.innerHTML = TAB_HELP[_openHelpKey] || ''; panel.removeAttribute('hidden'); }
  else { panel.setAttribute('hidden', ''); }
  leg.querySelectorAll('.legend-tab-help').forEach((b) => {
    b.setAttribute('aria-expanded', String(b.dataset.help === _openHelpKey));
  });
}

// Tab + "?" clicks via delegation, so we don't need to wait for the async-built
// legend. The "?" check comes first since it sits next to the tab button.
document.addEventListener('click', (e) => {
  if (!e.target || !e.target.closest) return;
  const helpBtn = e.target.closest('.shell-legend .legend-tab-help');
  if (helpBtn) { toggleTabHelp(helpBtn.dataset.help); return; }
  const tab = e.target.closest('.shell-legend .legend-tab');
  if (tab) setMapColorVar(tab.dataset.colorvar);
});

// Set initial legend text once the legend mounts (shell topbar/legend is built
// asynchronously after this script runs).
(function _initSupplyLegend() {
  let tries = 0;
  const tick = () => {
    if (document.querySelector('.shell-legend')) { refreshLegend(); return; }
    if (++tries < 60) setTimeout(tick, 250);     // give up after ~15s
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', tick);
  else tick();
})();

// Hooks consumed by the experimental absolute-scale module (window so they
// survive across the classical-script modules).
window.faveRefreshLegend = refreshLegend;
window.faveSupplyActive = supplyActive;
