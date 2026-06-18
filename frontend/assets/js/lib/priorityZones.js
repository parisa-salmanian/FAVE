// Priority zones — a planner's "where to intervene first" overlay. A building is
// a priority when it combines HIGH social need (its DESO's composite need index)
// with POOR accessibility (low fairness score = far from services). Highlighting
// the intersection points planners at the underserved-and-vulnerable areas where
// new services would do the most good.
//
//   priority P = needNorm × accessGap
//     needNorm  = building's DESO needZ mapped onto the city's robust need range
//     accessGap = 1 − fairness score (overall accessibility; higher gap = worse)
//
// When active, buildings with P below the threshold are dimmed to a neutral grey
// and those at/above it are painted on an orange→deep-red ramp by P, so the
// priority pockets stand out. Reuses the demographic-lens need stats + __deso
// stamp (lib/demographicLens.js, models/epicityDemographics.js); no network.

let priorityZonesActive = false;
let priorityZonesTick = 0;
let priorityZonesThreshold = 0.20;   // P below this = "not a priority" (dimmed)

function priorityZonesOn() { return !!priorityZonesActive; }

// Building's overall accessibility score (0..1), preferring the all-category
// overall, then the current mix. null when fairness hasn't been computed.
function _pzAccessScore(feature) {
  const p = feature?.properties || {};
  const s = (p.fair_overall && Number.isFinite(p.fair_overall.score)) ? p.fair_overall.score
          : (p.fair && Number.isFinite(p.fair.score)) ? p.fair.score
          : null;
  return Number.isFinite(s) ? Math.max(0, Math.min(1, s)) : null;
}

// Building's need normalised to 0..1 on the city's robust needZ range. Reuses
// the demographic-lens machinery so the two views stay consistent.
function _pzNeedNorm(feature) {
  const code = feature?.properties?.__deso;
  if (code == null) return null;
  if (typeof _demoLensPropsMap !== 'function' || typeof _demoLensStatsFor !== 'function') return null;
  const map = _demoLensPropsMap();
  const props = map ? map.get(code) : null;
  if (!props) return null;
  const z = Number(props.needZ);
  if (!Number.isFinite(z)) return null;
  const st = _demoLensStatsFor('needZ');
  if (!st) return null;
  return Math.max(0, Math.min(1, (z - st.lo) / (st.hi - st.lo)));
}

// Priority score P in 0..1 (need × access-gap), or null when inputs are missing.
function priorityScoreForFeature(feature) {
  const need = _pzNeedNorm(feature);
  const access = _pzAccessScore(feature);
  if (need == null || access == null) return null;
  return need * (1 - access);
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

// Building fill under the priority overlay: dim grey below threshold (and for
// buildings with no need/access data), ramped red at/above it.
function priorityColorForFeature(feature) {
  const P = priorityScoreForFeature(feature);
  if (P == null || P < priorityZonesThreshold) return [105, 105, 110, 120];
  // Rescale [threshold..1] → [0..1] so the ramp uses its full range above the cut.
  const t = (P - priorityZonesThreshold) / (1 - priorityZonesThreshold || 1);
  return [..._pzRampColor(t), 235];
}

// Count of priority buildings in the current city (for the legend/status read-out).
function priorityZonesCount() {
  const feats = (typeof baseCityFC !== 'undefined' && baseCityFC) ? baseCityFC.features : [];
  let n = 0;
  for (const f of feats) {
    const P = priorityScoreForFeature(f);
    if (P != null && P >= priorityZonesThreshold) n++;
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
    note.textContent = `Priority = high need × poor access. ${n.toLocaleString()} buildings flagged (P ≥ ${priorityZonesThreshold.toFixed(2)}).`;
  }
}

function setPriorityZones(on, opts = {}) {
  const { refreshLayers = true } = opts;
  const next = !!on;
  if (next === priorityZonesActive) return;
  priorityZonesActive = next;
  priorityZonesTick++;
  const chk = document.getElementById('priorityZonesToggle');
  if (chk && chk.checked !== priorityZonesActive) chk.checked = priorityZonesActive;
  if (priorityZonesActive) {
    _pzUpdateLegend();
  } else if (typeof restoreFairnessLegend === 'function') {
    restoreFairnessLegend();
  }
  if (refreshLayers && typeof updateLayers === 'function') updateLayers();
}

function bindPriorityZonesToggle() {
  const chk = document.getElementById('priorityZonesToggle');
  if (!chk || chk.__bound) return;
  chk.addEventListener('change', () => setPriorityZones(chk.checked));
  chk.__bound = true;
  chk.checked = priorityZonesActive;
}

document.addEventListener('DOMContentLoaded', bindPriorityZonesToggle);
