// Aggregate fairness summaries over buildings / districts. Extracted from main.js.

/* ======================= Summarize buildings & districts helpers ======================= */
function inferDistrict(props) {
  const keys = [
    'district','addr:district','city_district','addr:city_district',
    'suburb','addr:suburb','neighbourhood','neighborhood','addr:neighbourhood','borough'
  ];
  for (const k of keys) {
    const v = props?.[k];
    if (v && String(v).trim()) return String(v).trim();
  }
  return '';
}

// helper: group buildings by usage type (category)
function inferCategoryGroup(props) {
  if (!props) return null;

  // If the data already carries an explicit label, use it.
  if (props.category_label) {
    const label = canonicalBuildingType(props.category_label);
    if (label) return label;
  }

  // Prefer andamal1 from Lantmäteriet local data (most authoritative for Swedish buildings)
  if (props.andamal1) {
    const andamal = props.andamal1.toLowerCase();
    for (const [keyword, buildingType] of Object.entries(ANDAMAL_TO_BUILDING_TYPE)) {
      if (andamal.includes(keyword)) return buildingType;
    }
  }

  // Fall back to objekttyp from local data
  if (props.objekttyp && props.objekttyp !== 'yes' && props.objekttyp !== 'no' && props.objekttyp !== 'roof') {
    const objLow = props.objekttyp.toLowerCase();
    for (const [keyword, buildingType] of Object.entries(ANDAMAL_TO_BUILDING_TYPE)) {
      if (objLow.includes(keyword)) return buildingType;
    }
  }

  // Prefer the same coarse category mapping used by the fairness metrics
  const code = Number.isFinite(props.categoryCode)
    ? props.categoryCode
    : ((sourceMode === 's1' || sourceMode === 'osm_s1') ? localUsageCode(props) : osmCategoryCode(props));
  const label = categoryLabelFromCode(code);
  if (label && label !== 'Other / unknown') return label;

  const cat = props.category || props.building || props.name;
  const canon = canonicalBuildingType(cat);
  return canon || 'Other / unknown';
}


function summarizeByNamedDistricts(rows, { cutoff = 0.0, minCount = 1 } = {}) {
  const m = new Map();
  for (const r of rows) {
    if (!Number.isFinite(r.score) || r.score <= cutoff) continue;
    const d = (r.district && r.district.trim()) ? r.district.trim() : 'Unknown';
    let e = m.get(d);
    if (!e) e = { name: d, sum: 0, cnt: 0 };
    e.sum += r.score; e.cnt += 1;
    m.set(d, e);
  }
  return Array.from(m.values())
    .filter(e => e.cnt >= minCount)
    .map(e => ({ name: e.name, mean: e.sum / e.cnt, count: e.cnt }));
}

function summarizeByHex(rows, cellKm = HEX_CELL_KM, { cutoff = 0.0, minCount = HEX_MIN_COUNT } = {}) {
  const bbox = turf.bbox(baseCityFC);
  const hex = turf.hexGrid(bbox, cellKm, { units: 'kilometers' });

  const pts = turf.featureCollection(
    rows
      .filter(r => Number.isFinite(r.score) && r.score > cutoff)
      .map(r => turf.point(r.centroid, { score: r.score }))
  );

  const out = [];
  hex.features.forEach((poly, i) => {
    const within = turf.pointsWithinPolygon(pts, poly);
    if (!within.features.length) return;
    if (within.features.length < minCount) return;
    const scores = within.features.map(p => p.properties.score).filter(Number.isFinite);
    if (!scores.length) return;
    const mean = scores.reduce((s, v) => s + v, 0) / scores.length;
    out.push({ name: `Cell ${i + 1}`, mean, count: scores.length, polygon: poly });
  });

  return out;
}

function summarizeFairnessCurrent() {
  const feats = (baseCityFC?.features || [])
    .filter(f => f?.properties?.fair && Number.isFinite(f.properties.fair.score));
  if (!feats.length) return null;

  const rows = feats.map((f, idx) => ({
    idx,
    score: f.properties.fair.score,
    name: f.properties.name || f.properties.category || f.properties.objekttyp || `Building #${idx + 1}`,
    district: inferDistrict(f.properties),
    centroid: turf.centroid(f).geometry.coordinates
  }));

  const rowsNoOutliers = rows.filter(r => r.score > SUMMARY_CUTOFF);

  const baseForBldg = rowsNoOutliers.length ? rowsNoOutliers : rows;
  const sortedB = baseForBldg.slice().sort((a, b) => b.score - a.score);
  const bestBldg  = sortedB[0];
  const worstBldg = sortedB[sortedB.length - 1];

  let mode = 'district-prop';
  let districtStats;

  const withNameFiltered = rowsNoOutliers.filter(r => !!r.district);
  const useNamed = withNameFiltered.length >= (rowsNoOutliers.length || rows.length) * 0.5;

  if (useNamed && withNameFiltered.length) {
    districtStats = summarizeByNamedDistricts(rowsNoOutliers, { cutoff: SUMMARY_CUTOFF, minCount: 5 });
  } else {
    mode = 'hexgrid';
    const baseRows = rowsNoOutliers.length ? rowsNoOutliers : rows;
    districtStats = summarizeByHex(baseRows, HEX_CELL_KM, { cutoff: SUMMARY_CUTOFF, minCount: HEX_MIN_COUNT });
  }

  if (!districtStats.length) {
    return {
      bestBldg, worstBldg,
      bestDistrict: null, worstDistrict: null,
      nBuildings: rows.length, nDistricts: 0, mode
    };
  }

  const ds = districtStats.slice().sort((a, b) => b.mean - a.mean);
  const bestDistrict  = ds[0];
  const worstDistrict = ds[ds.length - 1];

  return {
    bestBldg, worstBldg, bestDistrict, worstDistrict,
    nBuildings: rows.length, nDistricts: ds.length, mode
  };
}

