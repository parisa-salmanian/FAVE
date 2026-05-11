// General-purpose helpers (formatters, string utilities, geometry math).
// Extracted from main.js; loaded as classical script before main.js.

/* ======================= Helpers ======================= */
function colorForOSMFeature(props) {
  const cat = (props?.category || "").toLowerCase();
  if (cat && CATEGORY_COLORS[cat]) return CATEGORY_COLORS[cat];
  const raw = (props?.objekttyp || "").toLowerCase();
  if (raw && CATEGORY_COLORS[raw]) return CATEGORY_COLORS[raw];
  const shop = (props?.shop || "").toLowerCase();
  const amen = (props?.amenity || "").toLowerCase();
  if (shop === "supermarket" || amen === "supermarket") return CATEGORY_COLORS["supermarket"];
  if (shop) return CATEGORY_COLORS["retail"];
  if (["school","college","university","kindergarten"].includes(amen)) return CATEGORY_COLORS["education"];
  if (["hospital","clinic","doctors","dentist","pharmacy"].includes(amen)) return CATEGORY_COLORS["health"];
  return CATEGORY_COLORS["unknown"];
}
function baseBuildingColorForFeature() {
  // Keep the default city view visually clean: use one neutral color for all buildings.
  return DEFAULT_BUILDING_COLOR;
}
function mockBuildingFillColor(feature) {
  const props = feature?.properties || {};
  if (pinnedChangeId != null && !changeCompareBaseline) {
    if (Array.isArray(props._changeColor)) {
      const c = props._changeColor;
      return [c[0], c[1], c[2], c[3] ?? 220];
    }
    // Mock buildings that are new (__whatIfAdded) didn't exist in the "before"
    // snapshot, so their delta = entire current score (before was 0).
    // Compute the diverging RdBu change color on-the-fly to match the legend.
    if (props.__whatIfAdded && fairActive && Number.isFinite(props?.fair?.score)) {
      const delta = props.fair.score;
      const t = Math.max(0, Math.min(1, 0.5 + delta * 0.5));
      if (typeof d3 !== 'undefined' && typeof d3.interpolateRdBu === 'function') {
        const c = d3.color(d3.interpolateRdBu(t));
        if (c) return [Math.round(c.r), Math.round(c.g), Math.round(c.b), 220];
      }
      return [33, 102, 172, 220];
    }
    // In change map mode, dim unaffected mocked buildings like real buildings.
    return [90, 90, 90, 120];
  }
  if (props._drSelected) return DR_SELECTION_COLOR_DEFAULT;
  if (drHasSelection && (pinnedChangeId == null || changeCompareBaseline)) return DR_UNSELECTED_COLOR;
  if (fairActive && Number.isFinite(props?.fair?.score)) {
    return colorFromScore(props.fair.score);
  }
  return baseBuildingColorForFeature(feature);
}
const clampElev = (h) => (Number.isFinite(h) ? Math.max(3, Math.min(300, h)) : 6);

function getBuiltYear(props) {
  if (!props) return null;
  if (Number.isFinite(props.built_year)) return props.built_year;
  const candidates = ['built_year','year','Year','YEAR','byggnadsår','byggnadsar'];
  for (const k of candidates) {
    const v = props[k];
    if (v == null) continue;
    const m = String(v).match(/\b(19|20)\d{2}\b/);
    if (m) return parseInt(m[0], 10);
  }
  return null;
}
function prettyNum(n, digits=2) { return Number.isFinite(n) ? String(Number(n).toFixed(digits)) : '—'; }

function buildingTypeOf(f) {
  // Keep building type labels aligned with the fairness views.
  // We use the same category inference that powers "Fairness by building type"
  // so the dropdown values match the plot labels.
  const type = inferCategoryGroup(f?.properties);
  return type || '';
}

// Popup rendering (map popups + side panel) lives in assets/js/views/popupView.js
// Change-history log lives in assets/js/models/changeHistory.js

// Cheap centroid: bounding-box midpoint. Replaces turf.centroid for fairness
// inner loops. Order-of-magnitude faster than turf's mass-weighted centroid
// because we walk vertices once with no allocations or polygon-area math.
const _fastCentroidCache = new WeakMap();
function fastCentroid(feature) {
  if (!feature) return null;
  const cached = _fastCentroidCache.get(feature);
  if (cached !== undefined) return cached;
  const geom = feature.geometry;
  if (!geom) { _fastCentroidCache.set(feature, null); return null; }

  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const walk = (node) => {
    if (typeof node[0] === 'number') {
      const x = node[0], y = node[1];
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    } else {
      for (let i = 0; i < node.length; i++) walk(node[i]);
    }
  };
  try { walk(geom.coordinates); } catch (_) {}
  const out = (minX === Infinity) ? null : [(minX + maxX) / 2, (minY + maxY) / 2];
  _fastCentroidCache.set(feature, out);
  return out;
}
