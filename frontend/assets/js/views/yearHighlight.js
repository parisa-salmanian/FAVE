// Year-of-construction highlight overlay. Extracted from main.js.

/* ======================= Year highlight ======================= */
function statsForYear(year) {
  if (!year || !newbuildsFC) return [];
  return newbuildsFC.features.filter(f => {
    const by = getBuiltYear(f.properties || {});
    if (year === 'null') return by == null;
    if (year === '0') return Number.isFinite(by) ? by <= 2020 : false;
    return by === Number(year);
  });
}

// Fairness model (scoring, Gini, transition helpers, POI fetch, POI mix UI,
// Växjö gravity, single/weighted/overall fairness) lives in
// assets/js/models/fairness.js
// POI markers live in assets/js/views/poiMarkers.js
// Map layers live in assets/js/views/layers.js
// Map lasso lives in assets/js/controllers/mapLasso.js
// What-if mock buildings lasso lives in assets/js/views/whatIfMockBuildings.js
