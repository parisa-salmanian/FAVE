// Side panel rendering — the per-selection details panel that shows
// building/district fairness, badges, and category breakdowns. Extracted from main.js.

/* ======================= Side panel helpers ======================= */
function setText(sel, text) { const el = document.querySelector(sel); if (el) el.textContent = text; }
function setHTML(sel, html) { const el = document.querySelector(sel); if (el) el.innerHTML = html; }
function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&gt;','>':'&lt;','"':'&quot;',"'":'&#39;'}[c]) );
}
function linkify(label, role) {
  return `<a href="#" class="sp-jump" data-role="${role}" title="Zoom to ${escapeHTML(label)}">${escapeHTML(label)}</a>`;
}
function flyToPoint(lnglat) { try { map.flyTo({ center: lnglat, zoom: 16, pitch: 60, speed: 0.7 }); } catch {} }
function districtFeatureFromSummary(d) {
  if (!d) return null;
  if (d.polygon) return d.polygon;
  const pts = (baseCityFC?.features || [])
    .filter(f => (inferDistrict(f.properties) || 'Unknown') === d.name && f.properties?.fair)
    .map(f => turf.centroid(f));
  if (pts.length >= 3) {
    const hull = turf.convex(turf.featureCollection(pts));
    if (hull) return hull;
  }
  return null;
}
function fitToDistrict(d) {
  try {
    const feat = districtFeatureFromSummary(d);
    if (!feat) return;
    const [minX, minY, maxX, maxY] = turf.bbox(feat);
    map.fitBounds([[minX, minY], [maxX, maxY]], { padding: 40, duration: 800 });
  } catch {}
}

function setSidePanelLegendMode(mode = 'fairness') {
  const sidePanel = document.getElementById('sidePanel');
  if (!sidePanel) return;

  const showChangeLegend = mode === 'change' && pinnedChangeId != null && !changeCompareBaseline;
  sidePanel.dataset.legendMode = showChangeLegend ? 'change' : 'fairness';

  const titleEl = document.getElementById('spTitle');
  const legendEl = document.getElementById('fairnessLegendBar');

  if (showChangeLegend) {
    if (titleEl) titleEl.textContent = 'Change Map';
    if (legendEl) legendEl.style.background = changeLegendGradientCSS();
    setText('#spLegendLeft', 'Most worsened');
    setText('#spLegendMid', 'No change');
    setText('#spLegendRight', 'Most improved');
    setText('#spNote', 'Building color = per-building change from selected what-if (red = worsened, blue = improved).');
    return;
  }

  if (titleEl) titleEl.textContent = 'Fairness';
  if (legendEl) legendEl.style.background = fairnessLegendGradientCSS();
  // Match the gradient orientation: purple on the left = least fair,
  // green on the right = most fair (consistent across map, inspector,
  // shell-legend, and side panel).
  setText('#spLegendLeft', 'Least fair');
  setText('#spLegendMid', 'Medium');
  setText('#spLegendRight', 'Most fair');
  const note = fairnessModel === 'ifcity'
    ? 'Building color = normalized benefit from gravity-based accessibility. Click a building to view IF-City debug values in the popup.'
    : 'Building color = accessibility score (same palette across map, DR, mezo, and macro views).';
  setText('#spNote', note);
}

function showSidePanel(cat, g, poiCount, summary = null) {
  const sidePanel = document.getElementById('sidePanel');
  if (!sidePanel) return;
  sidePanel.classList.remove('d-none');

  setText('#spCat',  prettyPOIName(cat));
  setText('#spGini', g.toFixed(2));
  setText('#spPois', String(poiCount));
  setSidePanelLegendMode((pinnedChangeId != null && !changeCompareBaseline) ? 'change' : 'fairness');
  clearBestWorstHighlights();
}

function hideSidePanel() {
  const sidePanel = document.getElementById('sidePanel');
  if (!sidePanel) return;
  sidePanel.classList.add('d-none');
  setText('#spCat',  '—');
  setText('#spGini', '—');
  setText('#spPois', '—');
  setSidePanelLegendMode('fairness');
  clearBestWorstHighlights();
}

