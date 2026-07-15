// Best/Least overlays — highlights the highest- and lowest-fairness buildings
// (and their best/worst category breakdowns) on the map. Extracted from main.js;
// loaded as a classical script before main.js.

/* ======================= NEW: Best/Least overlays ======================= */
function clearBestWorstHighlights() {
  bw = { bldgBest:null, bldgWorst:null, districtBest:null, districtWorst:null, mode:null };
  bwTick++;
}

function setBestWorstHighlights(summary) {
  clearBestWorstHighlights();
  updateLayers();
}

function createBestWorstLayers() {
  return [];
}

function formatDistrictMeta(props = {}) {
  const rows = [];
  for (const [key, value] of Object.entries(props)) {
    if (key.startsWith('__')) continue;
    if (value == null || value === '') continue;
    rows.push({ key, value });
  }
  rows.sort((a, b) => a.key.localeCompare(b.key));
  return rows.map(({ key, value }) => (
    `<tr><th>${escapeHTML(key)}</th><td>${escapeHTML(String(value))}</td></tr>`
  )).join('');
}

function formatDistrictFairnessRows(props = {}) {
  const rows = [];
  const focusedScore = Number.isFinite(props.__fairFocused) ? props.__fairFocused : null;
  const focusedCat = props.__fairFocusedCat;
  if (focusedScore != null && focusedCat) {
    rows.push(`<tr><th>Selected POI (${escapeHTML(prettyPOIName(focusedCat))})</th><td>${focusedScore.toFixed(2)}</td></tr>`);
  }
  const overall = Number.isFinite(props.__fairOverall) ? props.__fairOverall : null;
  rows.push(`<tr><th>Overall fairness</th><td>${overall != null ? overall.toFixed(2) : '—'}</td></tr>`);
  rows.push(`<tr><th colspan="2" style="padding-top:6px; font-weight:700;">Fairness by category</th></tr>`);
  const byCat = props.__fairByCat || {};
  const catList = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
  catList.forEach((cat) => {
    const val = byCat?.[cat];
    rows.push(`<tr><th>${escapeHTML(prettyPOIName(cat))}</th><td>${Number.isFinite(val) ? val.toFixed(2) : '—'}</td></tr>`);
  });
  return rows.join('');
}

function buildDistrictPopupHTML({ name, score, count }) {
  // Lean hover tooltip — title + colored fairness chip + buildings count.
  // Mirrors the building popup layout so the three scales feel consistent.
  const safeName = name ? escapeHTML(name) : 'District';
  let chip = '';
  if (Number.isFinite(score)) {
    const c = (typeof colorFromScore === 'function') ? colorFromScore(score) : null;
    const bg = Array.isArray(c) ? `rgb(${c[0]}, ${c[1]}, ${c[2]})` : '#3A6EA5';
    chip = `<span class="popup-fair-chip" title="Mean fairness" style="background:${bg};">${Math.round(score * 100)}%</span>`;
  }
  const countRow = Number.isFinite(count) && count > 0
    ? `<table class="popup-table"><tr><th>Buildings</th><td>${count}</td></tr></table>` : '';
  return `
    <div class="building-popup">
      <div class="popup-header">
        <div class="popup-title">${safeName}</div>
        ${chip}
      </div>
      ${countRow}
    </div>`;
}

function districtPopupDataForSummary(d) {
  if (!d) return null;
  let matchFeature = null;
  if (districtFC?.features?.length) {
    matchFeature = districtFC.features.find((feat, idx) => {
      const props = feat?.properties || {};
      const name = props.__districtName || districtNameOf(props, idx);
      return name === d.name;
    }) || null;
  }
  const props = matchFeature?.properties || {};
  const hasProps = Object.keys(props).length > 0;
  const name = props.__districtName || (hasProps ? districtNameOf(props) : d.name) || d.name;
  const score = Number.isFinite(props.__score) ? props.__score : (Number.isFinite(d.mean) ? d.mean : null);
  const count = Number.isFinite(props.__count) ? props.__count : (Number.isFinite(d.count) ? d.count : 0);
  const popupFeature = matchFeature || districtFeatureFromSummary(d);
  const coord = popupFeature ? turf.centroid(popupFeature).geometry.coordinates : null;
  return { name, score, count, props, coord };
}

function showDistrictPopup(atLngLat, data) {
  if (!atLngLat || !data) return;
  const html = buildDistrictPopupHTML(data);
  closePopup();
  currentPopup = new maplibregl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '320px',
    focusAfterOpen: false,
    className: 'district-hover-pop'
  }).setLngLat(atLngLat).setHTML(html).addTo(map);
}

function showDistrictSummaryPopup(d) {
  const data = districtPopupDataForSummary(d);
  if (!data?.coord) return;
  showDistrictPopup(data.coord, data);
}

// ── Hover handlers (macro / meso / micro) ───────────────────────────────────
// Performance-sensitive path: deck.gl onHover fires on every pixel move, so
// we (1) dedupe by feature key, (2) reuse one maplibregl.Popup instance for
// every hover (setLngLat/setHTML on the same DOM, never re-create), and
// (3) coalesce inspector updates onto a single requestAnimationFrame so
// fast cursor movement still produces at most one inspector re-render per
// frame instead of dozens. The building handler additionally rAF-throttles
// the entire dispatch — at micro zoom the cursor crosses many small
// adjacent buildings per second, and even the early-return key check costs
// real time when invoked dozens of times per frame.
let _lastHoverKey = null;
let _hoverPopup = null;
let _pendingInspectorUpdate = null;
let _pendingInspectorRAF = null;
let _bldgHoverThrottleRAF = null;
let _pendingBldgHoverInfo = null;
let _pendingCameraLockRAF = null;

function _hoverKey(layerId, obj) {
  if (!obj) return null;
  if (layerId === 'mezo-hex-layer') return `mezo:${obj.hex}`;
  if (layerId === 'district-polygons') {
    const p = obj.properties || {};
    return `district:${p.regso || p.deso || p.code || p.__districtName || p.name || p.namn || ''}`;
  }
  const p = obj.properties || {};
  return `bldg:${p.objektidentitet || p.id || p['@id'] || p.__uid || ''}`;
}

function _ensureHoverPopup() {
  if (_hoverPopup) return _hoverPopup;
  _hoverPopup = new maplibregl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '220px',
    focusAfterOpen: false,
    className: 'fave-hover-pop',
  });
  return _hoverPopup;
}

function _showHoverPopup(at, html) {
  const pop = _ensureHoverPopup();
  pop.setLngLat(at).setHTML(html);
  if (!pop.isOpen()) pop.addTo(map);
}

function _hideHoverPopup() {
  if (_hoverPopup && _hoverPopup.isOpen()) _hoverPopup.remove();
}

// Only push hover state into the inspector when it's already open — the
// user explicitly does NOT want hovering to slide the panel in. This is
// also the single biggest perf win on the building layer (10k+ features),
// since when the inspector is closed we skip the entire re-render path.
function _isInspectorOpen() {
  const insp = document.getElementById('inspector');
  return insp ? insp.getAttribute('data-open') === 'true' : false;
}

function _scheduleInspectorUpdate(fn) {
  if (!_isInspectorOpen()) return;
  _pendingInspectorUpdate = fn;
  if (_pendingInspectorRAF) return;
  _pendingInspectorRAF = requestAnimationFrame(() => {
    _pendingInspectorRAF = null;
    const run = _pendingInspectorUpdate;
    _pendingInspectorUpdate = null;
    // Re-check the gate at run-time too — user may have closed the panel
    // between scheduling and the rAF callback firing.
    if (typeof run === 'function' && _isInspectorOpen()) {
      try { run(); } catch (e) { /* swallow — hover is purely visual */ }
    }
  });
}

// Camera lock on hover — snapshot once when the cursor first enters a
// pickable layer, restore after the inspector update settles. Belt-and-
// braces against any side-effect (deck.gl re-render, inspector resize,
// CSS reflow) that nudges zoom/pitch while the user is just exploring.
// Coalesced via _pendingCameraLockRAF so rapidly-changing hover keys
// (cursor sweeping across many adjacent features) don't queue an unbounded
// chain of double-rAF callbacks.
function _lockCameraDuringHover() {
  if (!map || typeof map.getZoom !== 'function') return;
  if (_pendingCameraLockRAF) return;
  const cam = {
    zoom:    map.getZoom(),
    center:  [map.getCenter().lng, map.getCenter().lat],
    bearing: map.getBearing(),
    pitch:   map.getPitch(),
  };
  _pendingCameraLockRAF = requestAnimationFrame(() => requestAnimationFrame(() => {
    _pendingCameraLockRAF = null;
    if (!map) return;
    const cur = map.getCenter();
    const moved =
      Math.abs(map.getZoom() - cam.zoom) > 0.001 ||
      Math.abs(map.getBearing() - cam.bearing) > 0.001 ||
      Math.abs(map.getPitch() - cam.pitch) > 0.001 ||
      Math.abs(cur.lng - cam.center[0]) > 1e-7 ||
      Math.abs(cur.lat - cam.center[1]) > 1e-7;
    if (moved) {
      map.jumpTo({ center: cam.center, zoom: cam.zoom, bearing: cam.bearing, pitch: cam.pitch });
    }
  }));
}

function handleDistrictHover(info) {
  const feat = info?.object;
  const at = info?.coordinate;
  if (!feat || !at) {
    if (_lastHoverKey && _lastHoverKey.startsWith('district:')) {
      _lastHoverKey = null;
      _hideHoverPopup();
    }
    return;
  }
  const key = _hoverKey('district-polygons', feat);
  if (key === _lastHoverKey) return;
  _lastHoverKey = key;
  const props = feat.properties || {};
  const name = props.__districtName || (typeof districtNameOf === 'function' ? districtNameOf(props) : null);
  const score = Number.isFinite(props.__score) ? props.__score : null;
  const count = Number.isFinite(props.__count) ? props.__count : 0;
  _showHoverPopup(at, buildDistrictPopupHTML({ name, score, count }));
  _scheduleInspectorUpdate(() => window.faveInspector?.setDistrictSelection?.(feat));
  _lockCameraDuringHover();
}

function handleMezoHover(info) {
  const cell = info?.object;
  const at = info?.coordinate;
  if (!cell || !at) {
    if (_lastHoverKey && _lastHoverKey.startsWith('mezo:')) {
      _lastHoverKey = null;
      _hideHoverPopup();
    }
    return;
  }
  const key = _hoverKey('mezo-hex-layer', cell);
  if (key === _lastHoverKey) return;
  _lastHoverKey = key;
  const score = (typeof mezoOverlayScore === 'function') ? mezoOverlayScore(cell) : null;
  _showHoverPopup(at, buildMezoPopupHTML({ hex: cell.hex, score, count: cell.__count }));
  _scheduleInspectorUpdate(() => window.faveInspector?.setMezoSelection?.(cell));
  _lockCameraDuringHover();
}

// Outer rAF-throttle: deck.gl fires onHover on every mouse-move event
// (~120/s on fast pointers). At micro zoom, the cursor sweeps across many
// small adjacent buildings, so even the cheap key-compute + early-return
// happens dozens of times per frame. We capture only the latest info per
// frame and process it once. The camera lock IS retained at this scale
// (deck.gl re-renders + autoHighlight occasionally nudge the viewport on
// hover) but is coalesced via _pendingCameraLockRAF so it costs at most
// one outstanding double-rAF check at a time.
function handleBuildingHover(info) {
  _pendingBldgHoverInfo = info;
  if (_bldgHoverThrottleRAF) return;
  _bldgHoverThrottleRAF = requestAnimationFrame(() => {
    _bldgHoverThrottleRAF = null;
    const latest = _pendingBldgHoverInfo;
    _pendingBldgHoverInfo = null;
    _processBuildingHover(latest);
  });
}

function _processBuildingHover(info) {
  const feat = info?.object;
  if (!feat) {
    if (_lastHoverKey && _lastHoverKey.startsWith('bldg:')) {
      _lastHoverKey = null;
    }
    return;
  }
  const key = _hoverKey('city-buildings', feat);
  if (key === _lastHoverKey) return;
  _lastHoverKey = key;
  // No popup. Inspector mirrors only when already open (gated inside
  // _scheduleInspectorUpdate). When the inspector is closed, the building
  // layer omits onHover entirely (see createCityLayer), so this code path
  // doesn't even fire — deck.gl's picking pass on mouse-move is skipped.
  _scheduleInspectorUpdate(() => window.faveInspector?.setBuildingSelection?.(feat));
  _lockCameraDuringHover();
}

function closeDistrictPopulationPopup() {
  if (districtPopulationPopup) {
    districtPopulationPopup.remove();
    districtPopulationPopup = null;
  }
}

/**
 * Convert SCB flat format [{key:[regso,ageGroup,gender,year], values:["count"]}, ...]
 * into nested per-district objects that the existing population UI can consume:
 *   { regsokod, <ageGroup>: { <year>: { male, female, total } }, ... }
 */
function convertSCBFlatToNested(flatData) {
  if (!Array.isArray(flatData) || !flatData.length) return flatData;
  // Detect SCB format: first entry has key array of length >= 4
  const sample = flatData[0];
  if (!Array.isArray(sample?.key) || sample.key.length < 4) return flatData;

  const byRegso = new Map();

  for (const row of flatData) {
    const [rawRegso, ageGroup, genderCode, year] = row.key;
    const count = Number(row.values?.[0]) || 0;

    // Strip suffix like "_RegSO2025" to get clean regso code
    const regso = String(rawRegso).replace(/_RegSO\d+/i, '');

    if (!byRegso.has(regso)) {
      byRegso.set(regso, { regsokod: regso });
    }
    const entry = byRegso.get(regso);

    // Create nested: entry[ageGroup][year] = { male, female, total }
    if (!entry[ageGroup]) entry[ageGroup] = {};
    if (!entry[ageGroup][year]) entry[ageGroup][year] = {};

    const bucket = entry[ageGroup][year];
    if (genderCode === '1') {
      bucket.male = count;
    } else if (genderCode === '2') {
      bucket.female = count;
    } else if (genderCode === '1+2') {
      bucket.total = count;
    }
  }

  const result = Array.from(byRegso.values());
  console.log(`convertSCBFlatToNested: ${flatData.length} flat rows → ${result.length} district entries`);
  return result;
}

function normalizePopulationData(raw) {
  if (Array.isArray(raw)) {
    // Detect SCB flat format: [{key:[...], values:[...]}, ...]
    if (raw.length && Array.isArray(raw[0]?.key) && raw[0].key.length >= 4) {
      return convertSCBFlatToNested(raw);
    }
    return raw;
  }
  if (raw && Array.isArray(raw.data)) return normalizePopulationData(raw.data);
  if (raw && Array.isArray(raw.records)) return normalizePopulationData(raw.records);
  return raw || null;
}

let _genderAgePopCityKey = null;

async function ensureGenderAgePopulationData() {
  const cityKey = districtCityKeyFromInput(lastCityName) || null;
  const popUrl = (cityKey && GENDER_AGE_POP_URL_BY_CITY_KEY[cityKey])
    ? GENDER_AGE_POP_URL_BY_CITY_KEY[cityKey]
    : DEFAULT_GENDER_AGE_POP_URL;

  // Invalidate cache when city changes
  if (_genderAgePopCityKey !== cityKey) {
    genderAgePopulation = null;
    genderAgePopulationPromise = null;
    _genderAgePopCityKey = cityKey;
  }

  if (genderAgePopulation) return genderAgePopulation;
  if (genderAgePopulationPromise) return genderAgePopulationPromise;

  console.log(`Loading population data for city "${cityKey}" from ${popUrl}`);
  genderAgePopulationPromise = fetch(popUrl)
    .then(r => {
      if (!r.ok) throw new Error(`Population fetch failed (${r.status})`);
      return r.json();
    })
    .then(raw => {
      genderAgePopulation = normalizePopulationData(raw);
      return genderAgePopulation;
    })
    .catch(err => {
      genderAgePopulationPromise = null;
      throw err;
    });
  return genderAgePopulationPromise;
}
function regsoCodeFromProps(props = {}) {
  return props.regsokod || props.REGSOKOD || props.regso || props.REGSO || props.regso_kod || props.Regso || null;
}

function lookupPopulationEntry(data, regsoCode) {
  if (!data || !regsoCode) return null;
  if (Array.isArray(data)) {
    return data.find(item => {
      const key = item?.regsokod || item?.regso || item?.code || item?.key || item?.id || item?.REGSOKOD;
      return key != null && String(key) === String(regsoCode);
    }) || null;
  }
  if (data && typeof data === 'object') {
    if (data[regsoCode]) return data[regsoCode];
    const altKey = String(regsoCode);
    if (data[altKey]) return data[altKey];
    if (Array.isArray(data.records)) return lookupPopulationEntry(data.records, regsoCode);
    if (Array.isArray(data.data)) return lookupPopulationEntry(data.data, regsoCode);
  }
  return null;
}

// function districtNameByRegsoCode(regsoCode) {
//   if (!districtFC?.features?.length || !regsoCode) return '';
//   const idx = districtFC.features.findIndex(feature => String(regsoCodeFromProps(feature?.properties || {})) === String(regsoCode));
//   const match = idx >= 0 ? districtFC.features[idx] : null;
//   if (!match?.properties) return '';
//   return districtNameOf(match.properties, idx);
// }

// function getPopulationEntriesWithRegso(data) {
//   if (!data) return [];

//   if (districtFC?.features?.length) {
//     const districtEntries = districtFC.features
//       .map((feature, idx) => {
//         const regsoCode = regsoCodeFromProps(feature?.properties || {});
//         if (!regsoCode) return null;
//         const entry = lookupPopulationEntry(data, regsoCode);
//         if (!entry) return null;
//         return { regsoCode: String(regsoCode), entry, districtName: districtNameOf(feature.properties || {}, idx) };
//       })
//       .filter(Boolean);
//     if (districtEntries.length) return districtEntries;
//   }

//   if (Array.isArray(data)) {
//     return data
//       .map(entry => ({
//         regsoCode: regsoCodeFromProps(entry || {}) || entry?.code || entry?.key || entry?.id || entry?.REGSOKOD || null,
//         entry
//       }))
//       .filter(item => item.regsoCode && item.entry);
//   }

//   if (typeof data === 'object') {
//     if (Array.isArray(data.records)) return getPopulationEntriesWithRegso(data.records);
//     if (Array.isArray(data.data)) return getPopulationEntriesWithRegso(data.data);
//     return Object.entries(data)
//       .map(([regsoCode, entry]) => ({ regsoCode, entry }))
//       .filter(item => item.regsoCode && item.entry && typeof item.entry === 'object' && /^\d{4}R\d{3}$/i.test(String(item.regsoCode)));
//   }

//   return [];
// }

// async function debugDistrictPopulationComparison(options = {}) {
//   const data = await ensureGenderAgePopulationData();
//   const entries = getPopulationEntriesWithRegso(data);

//   const requestedYear = Number(options.year);
//   const discoveredYears = new Set();
//   entries.forEach(({ entry }) => {
//     extractAvailablePopulationYears(entry).forEach(year => discoveredYears.add(year));
//   });
//   const sortedYears = Array.from(discoveredYears).sort((a, b) => a - b);
//   const fallbackYear = sortedYears.length ? sortedYears[sortedYears.length - 1] : null;
//   const year = Number.isFinite(requestedYear) ? requestedYear : (fallbackYear || 2023);

//   const ageGroup = options.ageGroup || 'All';
//   const maxRows = Number.isFinite(Number(options.maxRows)) ? Math.max(1, Number(options.maxRows)) : 200;
//   const onlyMismatched = !!options.onlyMismatched;

//   const rows = [];
//   let missingTotals = 0;

//   entries.forEach(({ regsoCode, entry, districtName }) => {
//     const totals = aggregatePopulationTotals(entry, year, ageGroup);
//     if (!totals) {
//       missingTotals += 1;
//       return;
//     }

//     const male = Number(totals.male) || 0;
//     const female = Number(totals.female) || 0;
//     const total = Number(totals.total) || 0;
//     const expectedTotal = male + female;
//     const delta = total - expectedTotal;

//     rows.push({
//       regsoCode: String(regsoCode),
//       districtName: districtName || districtNameByRegsoCode(regsoCode),
//       year,
//       ageGroup,
//       male,
//       female,
//       total,
//       expectedTotal,
//       delta,
//       maleFemaleRatio: female > 0 ? Number((male / female).toFixed(3)) : null
//     });
//   });

//   rows.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
//   const mismatches = rows.filter(row => Math.abs(row.delta) >= 0.5);
//   const shown = (onlyMismatched ? mismatches : rows).slice(0, maxRows);

//   console.groupCollapsed(`[Population Debug] ${lastCityName || 'City'} · year ${year} · age ${ageGroup}`);
//   console.log(`population entries discovered: ${entries.length}`);
//   console.log(`districts with computed totals: ${rows.length}`);
//   console.log(`districts with no totals for selected filters: ${missingTotals}`);
//   console.log(`districts where total != male + female: ${mismatches.length}`);
//   if (requestedYear && !discoveredYears.has(requestedYear)) {
//     console.warn(`Requested year ${requestedYear} not found in discovered data years: ${sortedYears.join(', ') || 'none'}`);
//   }
//   console.table(shown);
//   console.groupEnd();

//   return { rows, mismatches, missingTotals, discoveredYears: sortedYears };
// }

// if (typeof window !== 'undefined') {
//   window.debugDistrictPopulationComparison = debugDistrictPopulationComparison;
// }


function flattenPopulationRows(value, prefix = '') {
  const rows = [];
  const addRow = (label, val) => {
    if (val == null || val === '') return;
    rows.push({ label, value: val });
  };

  if (Array.isArray(value)) {
    if (!value.length) return rows;
    const isObjectArray = value.every(item => item && typeof item === 'object' && !Array.isArray(item));
    if (isObjectArray) {
      value.forEach((item, idx) => {
        const labelKeyField = ['age', 'age_group', 'ageGroup', 'group', 'label', 'range']
          .find(key => item && Object.prototype.hasOwnProperty.call(item, key));
        const labelValue = labelKeyField ? item[labelKeyField] : `Item ${idx + 1}`;
        const nextPrefix = prefix ? `${prefix} ${labelValue}` : String(labelValue);
        const clone = { ...item };
        if (labelKeyField) delete clone[labelKeyField];
        rows.push(...flattenPopulationRows(clone, nextPrefix));
      });
      return rows;
    }
    addRow(prefix || 'Values', value.join(', '));
    return rows;
  }

  if (value && typeof value === 'object') {
    for (const [key, val] of Object.entries(value)) {
      if (key.startsWith('__')) continue;
      const nextPrefix = prefix ? `${prefix} ${key}` : key;
      if (val && typeof val === 'object') {
        rows.push(...flattenPopulationRows(val, nextPrefix));
      } else {
        addRow(nextPrefix, val);
      }
    }
    return rows;
  }

  if (prefix) addRow(prefix, value);
  else addRow('Value', value);
  return rows;
}

function buildDistrictPopulationHTML({ name, regsoCode, rows }) {
  const safeName = name ? escapeHTML(name) : 'District';
  const safeRegso = regsoCode ? escapeHTML(String(regsoCode)) : '—';
  const safeRows = rows?.length
    ? rows.map(({ label, value }) => (
      `<tr><th>${escapeHTML(label)}</th><td>${escapeHTML(String(value))}</td></tr>`
    )).join('')
    : '<tr><th>Population data</th><td>Not available</td></tr>';
  return `
    <div style="min-width:260px; max-width:520px;">
      <div class="popup-title">${safeName}</div>
      <table class="popup-table">
        <tr><th>Regso code</th><td>${safeRegso}</td></tr>
        ${safeRows}
      </table>
    </div>`;
}

function showDistrictPopulationPopup(atLngLat, data) {
  if (!atLngLat || !data) return;
  const html = buildDistrictPopulationHTML(data);
  closeDistrictPopulationPopup();
  districtPopulationPopup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: true,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '520px'
  }).setLngLat(atLngLat).setHTML(html).addTo(map);
  districtPopulationPopup.addClassName?.('district-popup');
  ensurePopupInView(districtPopulationPopup);
}

function setDistrictPopulationPanelText(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value || '—';
}

function setDistrictPopulationPanelNote(text) {
  const el = document.getElementById('districtPopulationNote');
  if (!el) return;
  el.textContent = text || '';
}

const populationTotalKeyRegex = /^(total|totalt|overall|sum|population|pop|all)$/i;
const populationValueRegex = {
  total: /^(total|totalt|overall|sum|population|pop|all)$/i
};

function normalizePopulationFieldKey(key) {
  return String(key || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function isPopulationMaleFieldKey(key) {
  const raw = String(key || '').toLowerCase().trim();
  const normalized = normalizePopulationFieldKey(key);
  if (!normalized) return false;
  if (["m", "male", "man", "men", "boy", "boys"].includes(normalized)) return true;
  if (/(^|_)(male|man|men|boy|boys)(_|$)/.test(normalized)) return true;
  return /(män|pojkar?)/i.test(raw);
}

function isPopulationFemaleFieldKey(key) {
  const raw = String(key || '').toLowerCase().trim();
  const normalized = normalizePopulationFieldKey(key);
  if (!normalized) return false;
  if (["f", "female", "woman", "women", "girl", "girls"].includes(normalized)) return true;
  if (/(^|_)(female|woman|women|girl|girls|kvinn)(_|$)/.test(normalized)) return true;
  return /(kvinn|flickor?)/i.test(raw);
}

function isPopulationTotalFieldKey(key) {
  const normalized = normalizePopulationFieldKey(key);
  if (!normalized) return false;
  if (isPopulationMaleFieldKey(key) || isPopulationFemaleFieldKey(key)) return false;
  if (populationValueRegex.total.test(normalized) || populationTotalKeyRegex.test(normalized)) {
    return true;
  }
  return [
    'total_population',
    'population_total',
    'total_pop',
    'pop_total'
  ].includes(normalized);
}

function findPopulationAgeArray(value) {
  if (Array.isArray(value)) {
    const ageKey = ['age', 'age_group', 'ageGroup', 'group', 'range', 'label']
      .find(key => value.every(item => item && typeof item === 'object' && !Array.isArray(item) && key in item));
    if (ageKey) return { items: value, ageKey };
    for (const item of value) {
      if (item && typeof item === 'object') {
        const found = findPopulationAgeArray(item);
        if (found) return found;
      }
    }
    return null;
  }
  if (value && typeof value === 'object') {
    for (const nested of Object.values(value)) {
      const found = findPopulationAgeArray(nested);
      if (found) return found;
    }
  }
  return null;
}

function extractPopulationRowTotals(item, ageKey) {
  const numericEntries = Object.entries(item || {})
    .filter(([key, val]) => key !== ageKey && Number.isFinite(Number(val)));
  if (!numericEntries.length) return null;

  let male = 0;
  let female = 0;
  let hasMale = false;
  let hasFemale = false;
  let total = null;
  let fallbackTotal = 0;

  numericEntries.forEach(([key, val]) => {
    const num = Number(val);
    if (isPopulationTotalFieldKey(key)) total = num;
    if (isPopulationMaleFieldKey(key)) { male += num; hasMale = true; }
    if (isPopulationFemaleFieldKey(key)) { female += num; hasFemale = true; }
    fallbackTotal += num;
  });

  const resolvedTotal = (hasMale && hasFemale) ? (male + female) : (total != null ? total : (hasMale || hasFemale ? (male + female) : fallbackTotal));
  return { male, female, total: resolvedTotal, hasMale, hasFemale };
}

function isPopulationYearBucket(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const yearKeys = Object.keys(value).filter(key => /^\d{4}$/.test(key));
  if (!yearKeys.length) return false;
  const sample = value[yearKeys[0]];
  return !!(sample && typeof sample === 'object' && !Array.isArray(sample));
}

function getPopulationAgeGroupMap(entry) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null;
  const ageMap = {};
  let found = false;
  Object.entries(entry).forEach(([key, val]) => {
    if (isPopulationYearBucket(val)) {
      ageMap[key] = val;
      found = true;
    }
  });
  return found ? ageMap : null;
}

function extractPopulationYearTotals(yearData) {
  if (!yearData || typeof yearData !== 'object') return null;
  const entries = Object.entries(yearData)
    .filter(([, val]) => Number.isFinite(Number(val)));
  if (!entries.length) return null;

  let male = 0;
  let female = 0;
  let hasMale = false;
  let hasFemale = false;
  let total = null;
  let fallbackTotal = 0;

  entries.forEach(([key, val]) => {
    const num = Number(val);
    if (isPopulationTotalFieldKey(key)) total = num;
    if (isPopulationMaleFieldKey(key)) { male += num; hasMale = true; }
    if (isPopulationFemaleFieldKey(key)) { female += num; hasFemale = true; }
    fallbackTotal += num;
  });

  const resolvedTotal = (hasMale && hasFemale) ? (male + female) : (total != null ? total : (hasMale || hasFemale ? (male + female) : fallbackTotal));
  return { male, female, total: resolvedTotal, hasMale, hasFemale };
}

function extractAvailablePopulationYears(entry) {
  if (!entry) return [];
  const years = new Set();
  const ageMap = getPopulationAgeGroupMap(entry);

  if (ageMap) {
    Object.values(ageMap).forEach(bucket => {
      if (!bucket || typeof bucket !== 'object') return;
      Object.keys(bucket).forEach(key => {
        const year = Number(key);
        if (Number.isFinite(year) && year >= 2010 && year <= 2023) {
          years.add(year);
        }
      });
    });
    return Array.from(years).sort((a, b) => a - b);
  }

  const checkYear = year => {
    if (!Number.isFinite(year)) return;
    if (year >= 2010 && year <= 2023) years.add(year);
  };

  const visit = value => {
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value)) {
      value.forEach(item => {
        if (item && typeof item === 'object') {
          const yearKey = Object.keys(item).find(key => /year/i.test(key));
          if (yearKey) checkYear(Number(item[yearKey]));
        }
        visit(item);
      });
      return;
    }
    Object.entries(value).forEach(([key, val]) => {
      const numericKey = Number(key);
      if (Number.isFinite(numericKey) && String(numericKey).length === 4) {
        checkYear(numericKey);
      }
      if (val && typeof val === 'object') {
        const yearKey = Object.keys(val).find(innerKey => /year/i.test(innerKey));
        if (yearKey) checkYear(Number(val[yearKey]));
        visit(val);
      }
    });
  };

  visit(entry);
  return Array.from(years).sort((a, b) => a - b);
}

function findPopulationYearNode(entry, year) {
  if (!entry || !Number.isFinite(Number(year))) return null;
  const targetYear = Number(year);
  let found = null;

  const visit = value => {
    if (found || !value || typeof value !== 'object') return;
    if (Array.isArray(value)) {
      for (const item of value) {
        if (found) return;
        if (item && typeof item === 'object') {
          const yearKey = Object.keys(item).find(key => /year/i.test(key));
          if (yearKey && Number(item[yearKey]) === targetYear) {
            found = item;
            return;
          }
        }
        visit(item);
      }
      return;
    }
    for (const [key, val] of Object.entries(value)) {
      if (Number(key) === targetYear) {
        found = val;
        return;
      }
      visit(val);
      if (found) return;
    }
  };

  visit(entry);
  return found;
}

function getPopulationAgeGroups(entry, year) {
  const ageMap = getPopulationAgeGroupMap(entry);
  if (ageMap) {
    return Object.keys(ageMap)
      .filter(key => !populationTotalKeyRegex.test(key));
  }
  const yearNode = year ? findPopulationYearNode(entry, year) : null;
  const found = findPopulationAgeArray(yearNode || entry);
  if (!found?.items?.length) return [];
  const groups = found.items
    .map(item => item?.[found.ageKey])
    .filter(label => label != null)
    .map(label => String(label));
  return Array.from(new Set(groups));
}

function aggregatePopulationTotals(entry, year, ageGroup) {
  if (!entry) return null;
  const ageMap = getPopulationAgeGroupMap(entry);
  if (ageMap) {
    if (!year) return null;
    const normalizedAge = ageGroup && ageGroup !== 'All' ? String(ageGroup) : null;
    const totalKey = Object.keys(ageMap).find(key => populationTotalKeyRegex.test(key));
    if (!normalizedAge) {
      if (totalKey) {
        const totals = extractPopulationYearTotals(ageMap[totalKey]?.[year]);
        if (!totals) return null;
        return { male: totals.male, female: totals.female, total: totals.total };
      }
      let male = 0;
      let female = 0;
      let total = 0;
      let hasAny = false;
      Object.entries(ageMap).forEach(([key, bucket]) => {
        if (populationTotalKeyRegex.test(key)) return;
        const totals = extractPopulationYearTotals(bucket?.[year]);
        if (!totals) return;
        hasAny = true;
        total += totals.total;
        if (totals.hasMale) male += totals.male;
        if (totals.hasFemale) female += totals.female;
      });
      if (!hasAny) return null;
      return { male, female, total };
    }

    const bucket = ageMap[normalizedAge];
    if (!bucket) return null;
    const totals = extractPopulationYearTotals(bucket?.[year]);
    if (!totals) return null;
    return { male: totals.male, female: totals.female, total: totals.total };
  }

  const yearNode = year ? findPopulationYearNode(entry, year) : entry;
  if (!yearNode) return null;
  const found = findPopulationAgeArray(yearNode);
  if (!found?.items?.length) return null;
  const { items, ageKey } = found;

  if (ageGroup && ageGroup !== 'All') {
    const match = items.find(item => String(item?.[ageKey]) === String(ageGroup));
    if (!match) return null;
    const totals = extractPopulationRowTotals(match, ageKey);
    if (!totals) return null;
    return { male: totals.male, female: totals.female, total: totals.total };
  }

  let male = 0;
  let female = 0;
  let total = 0;
  let hasAny = false;

  items.forEach(item => {
    const totals = extractPopulationRowTotals(item, ageKey);
    if (!totals) return;
    hasAny = true;
    total += totals.total;
    if (totals.hasMale) male += totals.male;
    if (totals.hasFemale) female += totals.female;
  });

  if (!hasAny) return null;
  return { male, female, total };
}

function buildPopulationSeries(entry, year) {
  if (!entry) return null;
  const ageMap = getPopulationAgeGroupMap(entry);
  if (ageMap && year) {
    const labels = [];
    const totalSeries = [];
    const maleSeries = [];
    const femaleSeries = [];
    Object.entries(ageMap).forEach(([label, bucket]) => {
      if (populationTotalKeyRegex.test(label)) return;
      const totals = extractPopulationYearTotals(bucket?.[year]);
      if (!totals) return;
      labels.push(String(label));
      totalSeries.push({ label: String(label), value: totals.total });
      if (totals.hasMale) maleSeries.push({ label: String(label), value: totals.male });
      if (totals.hasFemale) femaleSeries.push({ label: String(label), value: totals.female });
    });
    if (!labels.length) return null;
    const series = [{ name: 'Total', color: '#1f77b4', values: totalSeries }];
    if (maleSeries.length) series.push({ name: 'Male', color: '#2ca02c', values: maleSeries });
    if (femaleSeries.length) series.push({ name: 'Female', color: '#d62728', values: femaleSeries });
    return { labels, series };
  }
  const found = findPopulationAgeArray(entry);
  if (!found) return null;
  const { items, ageKey } = found;
  if (!items.length) return null;

  const labels = [];
  const totalSeries = [];
  const maleSeries = [];
  const femaleSeries = [];

  items.forEach(item => {
    const label = item?.[ageKey];
    if (label == null) return;
    const totals = extractPopulationRowTotals(item, ageKey);
    if (!totals) return;
    labels.push(String(label));
    totalSeries.push({ label: String(label), value: totals.total });
    if (totals.hasMale) maleSeries.push({ label: String(label), value: totals.male });
    if (totals.hasFemale) femaleSeries.push({ label: String(label), value: totals.female });
  });

  if (!labels.length) return null;

  const series = [
    { name: 'Total', color: '#1f77b4', values: totalSeries }
  ];
  if (maleSeries.length) series.push({ name: 'Male', color: '#2ca02c', values: maleSeries });
  if (femaleSeries.length) series.push({ name: 'Female', color: '#d62728', values: femaleSeries });

  return { labels, series };
}

function renderPopulationLineChart(container, dataset) {
  if (!container) return;
  if (typeof d3 === 'undefined') {
    container.innerHTML = '<div class="small text-muted">Line chart requires d3.js.</div>';
    return;
  }

  container.innerHTML = '';
  const { labels, series } = dataset || {};
  if (!labels || !series || !series.length) {
    container.innerHTML = '<div class="small text-muted">No age breakdown available.</div>';
    return;
  }

  const legend = document.createElement('div');
  legend.className = 'small text-muted mb-1 d-flex flex-wrap gap-2';
  series.forEach(s => {
    const item = document.createElement('span');
    item.className = 'd-inline-flex align-items-center gap-1';
    const swatch = document.createElement('span');
    swatch.style.display = 'inline-block';
    swatch.style.width = '10px';
    swatch.style.height = '10px';
    swatch.style.borderRadius = '2px';
    swatch.style.backgroundColor = s.color;
    item.appendChild(swatch);
    item.appendChild(document.createTextNode(s.name));
    legend.appendChild(item);
  });
  container.appendChild(legend);

  const width = container.clientWidth || 300;
  const height = container.clientHeight || 200;
  const margin = { top: 8, right: 10, bottom: 36, left: 36 };

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scalePoint()
    .domain(labels)
    .range([margin.left, width - margin.right])
    .padding(0.4);

  const maxY = d3.max(series.flatMap(s => s.values.map(v => v.value))) || 0;
  const y = d3.scaleLinear()
    .domain([0, maxY * 1.1 || 1])
    .range([height - margin.bottom, margin.top]);

  const xAxis = d3.axisBottom(x).tickSizeOuter(0);
  const yAxis = d3.axisLeft(y).ticks(4);

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call(g => g.selectAll('text').attr('font-size', 9).attr('transform', 'rotate(-30)').attr('text-anchor', 'end'));

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(yAxis)
    .call(g => g.selectAll('text').attr('font-size', 9));

  const line = d3.line()
    .x(d => x(d.label))
    .y(d => y(d.value));

  series.forEach(s => {
    svg.append('path')
      .datum(s.values)
      .attr('fill', 'none')
      .attr('stroke', s.color)
      .attr('stroke-width', 1.6)
      .attr('d', line);
  });
}

function renderPopulationBarChart(container, data) {
  if (!container) return;
  if (typeof d3 === 'undefined') {
    container.innerHTML = '<div class="small text-muted">Bar chart requires d3.js.</div>';
    return;
  }
  container.innerHTML = '';
  const colorByLabel = {
    Male: '#2ca02c',
    Female: '#d62728',
    Total: '#1f77b4'
  };

  const values = data
    ? [
      { label: 'Male', value: data.male },
      { label: 'Female', value: data.female },
      { label: 'Total', value: data.total }
    ]
    : null;
  const validValues = values?.filter(item => Number.isFinite(item.value)) || [];
  if (!validValues.length) {
    container.innerHTML = '<div class="small text-muted">No data available for selection.</div>';
    return;
  }

  const width = container.clientWidth || 300;
  const height = container.clientHeight || 180;
  const margin = { top: 8, right: 10, bottom: 30, left: 40 };

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleBand()
    .domain(values.map(item => item.label))
    .range([margin.left, width - margin.right])
    .padding(0.3);

  const maxY = d3.max(validValues, item => item.value) || 0;
  const y = d3.scaleLinear()
    .domain([0, maxY * 1.1 || 1])
    .range([height - margin.bottom, margin.top]);

  svg.append('g')
    .attr('transform', `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x))
    .call(g => g.selectAll('text').attr('font-size', 9));

  svg.append('g')
    .attr('transform', `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(4))
    .call(g => g.selectAll('text').attr('font-size', 9));

  svg.selectAll('rect')
    .data(values)
    .enter()
    .append('rect')
    .attr('x', item => x(item.label))
    .attr('width', x.bandwidth())
    .attr('y', item => y(Number.isFinite(item.value) ? item.value : 0))
    .attr('height', item => (height - margin.bottom) - y(Number.isFinite(item.value) ? item.value : 0))
    .attr('fill', item => colorByLabel[item.label] || '#6f42c1');
}

function showDistrictPopulationPanel(context) {
  const panel = document.getElementById('districtPopulationPanel');
  if (!panel) return;
  panel.classList.remove('d-none');

  const yearSelect = document.getElementById('districtPopulationYearSelect');
  const ageSelect = document.getElementById('districtPopulationAgeSelect');
  const barChartContainer = document.getElementById('districtPopulationBarChart');

  const setSelectOptions = (selectEl, options, placeholder) => {
    if (!selectEl) return;
    selectEl.innerHTML = '';
    if (!options.length) {
      const option = document.createElement('option');
      option.value = '';
      option.textContent = placeholder;
      selectEl.appendChild(option);
      selectEl.disabled = true;
      return;
    }
    selectEl.disabled = false;
    options.forEach(opt => {
      const option = document.createElement('option');
      option.value = opt.value;
      option.textContent = opt.label;
      selectEl.appendChild(option);
    });
  };

  if (!context) {
    setDistrictPopulationPanelText('districtPopulationName', '—');
    setDistrictPopulationPanelText('districtPopulationRegso', '—');
    setDistrictPopulationPanelNote('Double-click a district to load population details.');
    renderPopulationLineChart(document.getElementById('districtPopulationChart'), null);
    renderPopulationBarChart(barChartContainer, null);
    setSelectOptions(yearSelect, [], 'No years available');
    setSelectOptions(ageSelect, [], 'No age groups');
    return;
  }

  setDistrictPopulationPanelText('districtPopulationName', context.name || 'District');
  setDistrictPopulationPanelText('districtPopulationRegso', context.regsoCode || '—');
  setDistrictPopulationPanelNote('');

  const chartContainer = document.getElementById('districtPopulationChart');

  const years = extractAvailablePopulationYears(context.entry);
  if (!years.length) {
    renderPopulationLineChart(chartContainer, null);
    renderPopulationBarChart(barChartContainer, null);
    setSelectOptions(yearSelect, [], 'No years available');
    setSelectOptions(ageSelect, [], 'No age groups');
    setDistrictPopulationPanelNote('No population series found for this district.');
    return;
  }
  const defaultYear = years.includes(2023) ? 2023 : years[years.length - 1];
  const yearOptions = years.map(year => ({ value: String(year), label: String(year) }));
  setSelectOptions(yearSelect, yearOptions, 'No years available');
  if (defaultYear && yearSelect) yearSelect.value = String(defaultYear);

  const refreshAgeOptions = (selectedYear, selectedAge) => {
    const groups = getPopulationAgeGroups(context.entry, selectedYear);
    const options = [
      { value: 'All', label: 'All' },
      ...groups.map(group => ({ value: group, label: group }))
    ];
    setSelectOptions(ageSelect, options, 'No age groups');
    if (selectedAge && options.some(option => option.value === selectedAge)) {
      ageSelect.value = selectedAge;
    } else if (ageSelect) {
      ageSelect.value = 'All';
    }
  };

  refreshAgeOptions(defaultYear, 'All');

  const updateLineChart = selectedYear => {
    const dataset = buildPopulationSeries(context.entry, selectedYear);
    const legend = dataset?.series?.length ? dataset.series.map(s => s.name).join(' · ') : '';
    const lineNote = dataset
      ? (legend ? `Line chart: Year ${selectedYear} · ${legend}.` : `Line chart: Year ${selectedYear}.`)
      : `Line chart: No age series found for ${selectedYear || 'selected year'}.`;
    if (!dataset) {
      renderPopulationLineChart(chartContainer, null);
    } else {
      renderPopulationLineChart(chartContainer, dataset);
    }
    return lineNote;
  };
  const updateBarChart = () => {
    const selectedYear = yearSelect?.value ? Number(yearSelect.value) : null;
    const selectedAge = ageSelect?.value || 'All';
    const totals = selectedYear ? aggregatePopulationTotals(context.entry, selectedYear, selectedAge) : null;
    renderPopulationBarChart(barChartContainer, totals);
    const lineNote = updateLineChart(selectedYear);
    const filterNote = totals
      ? `Bar chart: Year ${selectedYear} · Age group: ${selectedAge}.`
      : `No data available for selection (Year ${selectedYear || '—'} · Age group: ${selectedAge}).`;
    setDistrictPopulationPanelNote([lineNote, filterNote].filter(Boolean).join(' '));
  };

  if (yearSelect) {
    yearSelect.onchange = () => {
      const selectedYear = yearSelect.value ? Number(yearSelect.value) : null;
      const currentAge = ageSelect?.value || 'All';
      refreshAgeOptions(selectedYear, currentAge);
      updateBarChart();
    };
  }
  if (ageSelect) {
    ageSelect.onchange = () => updateBarChart();
  }

  updateBarChart();
}

function hideDistrictPopulationPanel() {
  const panel = document.getElementById('districtPopulationPanel');
  if (!panel) return;
  panel.classList.add('d-none');
}

function toggleDistrictPopulationPanel() {
  const panel = document.getElementById('districtPopulationPanel');
  if (!panel) return;
  if (!panel.classList.contains('d-none')) {
    hideDistrictPopulationPanel();
    return;
  }
  showDistrictPopulationPanel(lastPopulationContext);
}

function updateParallelCoordsOffset() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;

  const drOffcanvas = document.getElementById('drOffcanvas');
  const isDROpen = drOffcanvas?.classList.contains('show');
  const drOffset = isDROpen ? drOffcanvas.getBoundingClientRect().width : 0;

  const whatIfMenu = document.querySelector('#whatIfDropdownBtn + .dropdown-menu');
  const isWhatIfOpen = whatIfMenu?.classList.contains('show');
  const whatIfOffset = isWhatIfOpen ? whatIfMenu.getBoundingClientRect().width : 0;

  panel.style.left = `${12 + drOffset}px`;
  panel.style.right = `${12 + whatIfOffset}px`;

  if (parallelCoordsOpen && !panel.classList.contains('d-none')) {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (parallelCoordsOpen && !panel.classList.contains('d-none')) {
          updateParallelCoordsPanel();
        }
      });
    });
  }
}

function showParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;
  panel.classList.remove('d-none');
  parallelCoordsOpen = true;
  updateParallelCoordsOffset();
  updateParallelCoordsPanel();
}

function hideParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;
  panel.classList.add('d-none');
  parallelCoordsOpen = false;
}

function toggleParallelCoordsPanel() {
  const panel = document.getElementById('parallelCoordsPanel');
  if (!panel) return;
  if (!panel.classList.contains('d-none')) {
    hideParallelCoordsPanel();
    return;
  }
  showParallelCoordsPanel();
}

function normalizeDistrictName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s-]/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

async function showDistrictStatsByName(name) {
  const target = normalizeDistrictName(name);
  if (!target) throw new Error('Provide a district name.');
  await ensureDistrictData();
  await refreshDistrictScores();
  setDistrictView(true);

  const match = (districtFC?.features || []).find((feat, idx) => {
    const props = feat?.properties || {};
    const candidate = normalizeDistrictName(props.__districtName || districtNameOf(props, idx));
    if (!candidate) return false;
    return candidate === target || candidate.includes(target) || target.includes(candidate);
  });
  if (!match) {
    throw new Error(`District "${name}" not found.`);
  }

  const props = match.properties || {};
  const displayName = props.__districtName || districtNameOf(props) || name;
  const score = Number.isFinite(props.__score) ? props.__score : null;
  const count = Number.isFinite(props.__count) ? props.__count : 0;
  const center = turf.center(match).geometry.coordinates;
  let income = null;
  try {
    if (typeof ensureDistrictIncomeData === 'function') {
      await ensureDistrictIncomeData();
      income = lookupDistrictIncome(regsoCodeFromProps(props));
    }
  } catch (_) {}
  showDistrictPopup(center, { name: displayName, score, count, props, income });

  let entry = null;
  let rows = [];
  const regsoCode = regsoCodeFromProps(props);
  try {
    const data = await ensureGenderAgePopulationData();
    entry = lookupPopulationEntry(data, regsoCode);
    rows = flattenPopulationRows(entry);
  } catch (err) {
    rows = [{ label: 'Population data', value: 'Unavailable' }];
  }
  lastPopulationContext = { name: displayName, regsoCode, entry, rows };
  showDistrictPopulationPanel(lastPopulationContext);
}

async function handleDistrictDoubleClick(e) {
  if (!districtView || !overlay || !map) return;
  const point = e?.point;
  if (!point) return;
  const pickArgs = { x: point.x, y: point.y, layerIds: ['district-polygons'] };
  const picked = overlay?.pickObject?.(pickArgs) || overlay?.deck?.pickObject?.(pickArgs);
  const feat = picked?.object;
  if (!feat) return;
  const props = feat.properties || {};
  const name = props.__districtName || districtNameOf(props) || 'District';
  const regsoCode = regsoCodeFromProps(props);
  let entry = null;
  let rows = [];
  try {
    const data = await ensureGenderAgePopulationData();
    entry = lookupPopulationEntry(data, regsoCode);
    rows = flattenPopulationRows(entry);
  } catch (err) {
    rows = [{ label: 'Population data', value: 'Unavailable' }];
  }
  const lngLat = e.lngLat || picked.coordinate || null;
  lastPopulationContext = { name, regsoCode, entry, rows };
  window.faveInspector?.setDistrictSelection?.(feat);
  if (document.getElementById('districtPopulationPanel')?.classList.contains('d-none') === false) {
    showDistrictPopulationPanel(lastPopulationContext);
  }
  closeDistrictPopulationPopup();
}

async function handleDistrictClick(info) {
  const feat = info?.object;
  const at = info?.coordinate;
  if (!feat || !at) return;
  const props = feat.properties || (feat.properties = {});
  const additive = isAdditiveSelectionEvent(info?.srcEvent);

  // Toggle behaviour: a non-additive click on a district that is already
  // selected clears the selection so the user can deselect by clicking
  // the same district again. Additive (Shift/Ctrl) clicks still let the
  // user toggle individual districts in/out of a multi-selection.
  const wasSelected = !!props._drSelected;
  if (wasSelected && !additive) {
    closePopup?.();
    if (typeof clearDRMapSelection === 'function') clearDRMapSelection();
    drSelectionTick++;
    if (typeof updateLayers === 'function') updateLayers();
    window.faveInspector?.clearSelection?.();
    return;
  }

  const name = props.__districtName || districtNameOf(props);
  const score = Number.isFinite(props.__score) ? props.__score : null;
  const count = Number.isFinite(props.__count) ? props.__count : 0;
  let income = null;
  try {
    if (typeof ensureDistrictIncomeData === 'function') {
      await ensureDistrictIncomeData();
      income = lookupDistrictIncome(regsoCodeFromProps(props));
    }
  } catch (_) {}
  // Click no longer pops a tooltip — that lives on hover now. Clicking just
  // pins the selection and updates the right inspector.
  closePopup();
  applyMapSelection([feat], { append: additive });
  // Explicitly mark the clicked district as selected. applyMapSelection
  // → clearDRMapSelection() wipes every district's _drSelected at the
  // start of the call, then marks aggregates from selected buildings.
  // For empty/sparse districts that path leaves the macro polygon
  // unhighlighted, so reinstate the flag here and bump the tick.
  feat.properties._drSelected = true;
  feat.properties._drColor = [...DR_SELECTION_COLOR_DEFAULT];
  drSelectionTick++;
  if (typeof updateLayers === 'function') updateLayers();
  // Sync the right inspector so the Fairness tab shows the clicked district.
  window.faveInspector?.setDistrictSelection?.(feat);
}

function districtOverlayScore(props = {}) {
  if (!fairActive) return null;
  // 1. Category-specific aggregated score (from all POIs of that type)
  if (fairActive && fairCategory === 'mix' && Number.isFinite(props.__score)) return props.__score;
  if (fairActive && fairCategory && fairCategory !== 'mix') {
    const byCat = props.__fairByCat || {};
    const catScore = byCat?.[fairCategory];
    if (Number.isFinite(catScore)) return catScore;
    if (Number.isFinite(props.__score)) return props.__score;
  }
  // 2. Overall fairness
  if (Number.isFinite(props.__fairOverall)) return props.__fairOverall;
  // 3. Focused (single-POI proximity) only as last resort
  if (Number.isFinite(props.__fairFocused)) return props.__fairFocused;
  return null;
}


function districtOverlayColor(props = {}) {
  if (transitionAnimActive && transitionHasData) {
    return transitionDistrictColor(props);
  }
  if (props?._drSelected) {
    const c = props?._drColor;
    if (Array.isArray(c) && c.length >= 3) return [c[0], c[1], c[2], 220];
    return [DR_SELECTION_COLOR_DEFAULT[0], DR_SELECTION_COLOR_DEFAULT[1], DR_SELECTION_COLOR_DEFAULT[2], 220];
  }
  if (drHasSelection) {
    const score = districtOverlayScore(props);
    if (Number.isFinite(score)) {
      const [r, g, b] = colorFromScore(score);
      return [r, g, b, 90];
    }
    return [DR_UNSELECTED_COLOR[0], DR_UNSELECTED_COLOR[1], DR_UNSELECTED_COLOR[2], 90];
  }
  // Change highlight
  if (pinnedChangeId != null && !changeCompareBaseline && Array.isArray(props._changeColor)) {
    const c = props._changeColor;
    return [c[0], c[1], c[2], c[3] ?? 160];
  }
  if (pinnedChangeId != null && !changeCompareBaseline) {
    // Match building-level change-map behavior:
    // unchanged features are white, changed features carry red↔blue _changeColor.
    return [255, 255, 255, 160];
  }
  // Priority overlay (need × poor access) replaces the fairness fill for the
  // district polygons, using each district's aggregated demographics + fairness.
  if (typeof priorityZonesOn === 'function' && priorityZonesOn() && typeof priorityColorFromProps === 'function') {
    return priorityColorFromProps(props);
  }
  const score = districtOverlayScore(props);
  if (!Number.isFinite(score)) return [80, 80, 80, 25];
  const [r, g, b] = colorFromScore(score);
  return [r, g, b, 110];
}

function createDistrictFairnessLayer() {
  if (!districtFC?.features?.length) return null;
  return new deck.GeoJsonLayer({
    id: 'district-fairness-overlay',
    data: districtFC,
    stroked: false,
    filled: true,
    extruded: false,
    pickable: false,
    getFillColor: f => districtOverlayColor(f?.properties || {}),
    parameters: { depthTest: false },
    opacity: 1,
    updateTriggers: {
      getFillColor: [
        districtScoreTick,
        changeLogTick,
        changeCompareBaseline,
        fairActive,
        fairCategory,
        fairRecolorTick,
        drSelectionTick,
        drHasSelection,
        selectedPOIId,
        selectedPOIFeature?.properties?.__cat || selectedPOIFeature?.properties?.category || '',
        transitionAnimTick,
        transitionAnimActive,
        (typeof priorityZonesActive !== 'undefined' ? priorityZonesActive : false),
        (typeof priorityZonesTick !== 'undefined' ? priorityZonesTick : 0),
        (typeof priorityNeedField !== 'undefined' ? priorityNeedField : ''),
        (typeof mapColorVar !== 'undefined' ? mapColorVar : 'fairness')
      ]
    }
  });
}

function mezoOverlayScore(props = {}) {
  if (!fairActive) return null;
  if (fairActive && fairCategory === 'mix' && Number.isFinite(props.__score)) return props.__score;
  if (fairActive && fairCategory && fairCategory !== 'mix') {
    const byCat = props.__fairByCat || {};
    const catScore = byCat?.[fairCategory];
    if (Number.isFinite(catScore)) return catScore;
    if (Number.isFinite(props.__score)) return props.__score;
  }
  if (Number.isFinite(props.__fairOverall)) return props.__fairOverall;
  if (Number.isFinite(props.__fairFocused)) return props.__fairFocused;
  return null;
}

function mezoOverlayColor(props = {}) {
  if (transitionAnimActive && transitionHasData) {
    return transitionMezoColor(props);
  }
  if (props?._drSelected) {
    const c = props?._drColor;
    if (Array.isArray(c) && c.length >= 3) return [c[0], c[1], c[2], 220];
    return [DR_SELECTION_COLOR_DEFAULT[0], DR_SELECTION_COLOR_DEFAULT[1], DR_SELECTION_COLOR_DEFAULT[2], 220];
  }
  if (drHasSelection) {
    const score = mezoOverlayScore(props);
    if (Number.isFinite(score)) {
      const [r, g, b] = colorFromScore(score);
      return [r, g, b, 95];
    }
    return [DR_UNSELECTED_COLOR[0], DR_UNSELECTED_COLOR[1], DR_UNSELECTED_COLOR[2], 90];
  }
  // Change highlight
  if (pinnedChangeId != null && !changeCompareBaseline && Array.isArray(props._changeColor)) {
    const c = props._changeColor;
    return [c[0], c[1], c[2], c[3] ?? 180];
  }
  if (pinnedChangeId != null && !changeCompareBaseline) {
    // Match building-level change-map behavior:
    // unchanged features are white, changed features carry red↔blue _changeColor.
    return [255, 255, 255, 160];
  }
  // Priority overlay (need × poor access) replaces the fairness fill for the hex
  // cells, using each cell's aggregated demographics + fairness. Scale-relative
  // top-fraction cutoff is kept fresh by refreshMezoScores → notifyPriorityDataChanged.
  if (typeof priorityZonesOn === 'function' && priorityZonesOn() && typeof priorityColorFromProps === 'function') {
    return priorityColorFromProps(props);
  }
  const score = mezoOverlayScore(props);
  if (!Number.isFinite(score)) return [80, 80, 80, 25];
  const [r, g, b] = colorFromScore(score);
  return [r, g, b, 130];
}

function buildMezoPopupHTML({ hex, score, count }) {
  // Same lean layout as the district hover tooltip. Title shows the H3
  // cell id (last 6 chars are unique within the city) so neighbouring hexes
  // are distinguishable; the rest of the id is full-width-truncatable.
  let chip = '';
  if (Number.isFinite(score)) {
    const c = (typeof colorFromScore === 'function') ? colorFromScore(score) : null;
    const bg = Array.isArray(c) ? `rgb(${c[0]}, ${c[1]}, ${c[2]})` : '#3A6EA5';
    chip = `<span class="popup-fair-chip" title="Mean fairness" style="background:${bg};">${Math.round(score * 100)}%</span>`;
  }
  const cellName = hex
    ? `Cell <span class="popup-cell-id">${escapeHTML(String(hex).slice(-6))}</span>`
    : 'Meso cell';
  const countRow = Number.isFinite(count) && count > 0
    ? `<table class="popup-table"><tr><th>Buildings</th><td>${count}</td></tr></table>` : '';
  return `
    <div class="building-popup">
      <div class="popup-header">
        <div class="popup-title">${cellName}</div>
        ${chip}
      </div>
      ${countRow}
    </div>`;
}

function handleMezoClick(info) {
  const cell = info?.object;
  const at = info?.coordinate;
  if (!cell || !at || !map) return;
  const additive = isAdditiveSelectionEvent(info?.srcEvent);

  // Toggle: a non-additive click on a hex that is already selected
  // clears the selection so the user can deselect by clicking the same
  // hex again. Additive (Shift/Ctrl) clicks keep the multi-select
  // toggle behaviour from applyMapSelection.
  if (cell._drSelected && !additive) {
    closePopup();
    if (typeof clearDRMapSelection === 'function') clearDRMapSelection();
    drSelectionTick++;
    if (typeof updateLayers === 'function') updateLayers();
    window.faveInspector?.clearSelection?.();
    return;
  }

  // Click pins the selection; the hover popup already showed the numbers,
  // so we don't add another one here. Just clear any lingering hover popup
  // and update the inspector / map selection.
  closePopup();
  applyMapSelection([cell], { append: additive });
  window.faveInspector?.setMezoSelection?.(cell);
}

function createMezoHexLayer() {
  if (!mezoHexData?.length) return null;
  return new deck.H3HexagonLayer({
    id: 'mezo-hex-layer',
    data: mezoHexData,
    pickable: true,
    autoHighlight: true,
    highlightColor: [40, 50, 65, 110],
    extruded: false,
    getHexagon: d => d.hex,
    getFillColor: d => mezoOverlayColor(d || {}),
    getLineColor: [255, 255, 255, 80],
    lineWidthUnits: 'pixels',
    getLineWidth: 1,
    onClick: handleMezoClick,
    onHover: handleMezoHover,
    parameters: { depthTest: false },
    updateTriggers: {
      getFillColor: [
        mezoScoreTick,
        changeLogTick,
        changeCompareBaseline,
        fairActive,
        fairCategory,
        fairRecolorTick,
        drSelectionTick,
        drHasSelection,
        selectedPOIId,
        selectedPOIFeature?.properties?.__cat || selectedPOIFeature?.properties?.category || '',
        transitionAnimTick,
        (typeof priorityZonesActive !== 'undefined' ? priorityZonesActive : false),
        (typeof priorityZonesTick !== 'undefined' ? priorityZonesTick : 0),
        (typeof priorityNeedField !== 'undefined' ? priorityNeedField : ''),
        (typeof mapColorVar !== 'undefined' ? mapColorVar : 'fairness'),
        transitionAnimActive
      ]
    }
  });
}

// (Stub handleDistrictClick previously lived here and hoisted over the
// real handler above, which broke the macro-view map highlight. The full
// handler at the top of this file now drives both the map highlight and
// the inspector update.)

function createDistrictPickLayer() {
  if (!districtFC?.features?.length) return null;
  return new deck.GeoJsonLayer({
    id: 'district-polygons',
    data: districtFC,
    stroked: false,
    filled: true,
    extruded: false,
    pickable: true,
    autoHighlight: true,
    highlightColor: [55, 65, 80, 90],
    getFillColor: [0, 0, 0, 0],
    parameters: { depthTest: false },
    opacity: 0,
    onClick: handleDistrictClick,
    onHover: handleDistrictHover,
    updateTriggers: { data: [districtScoreTick] }
  });
}


function createDistrictBoundaryLayer() {
  const boundary = ensureDistrictBoundaryLines();
  if (!boundary?.features?.length) return null;
  return new deck.GeoJsonLayer({
    id: 'district-borders',
    data: boundary,
    stroked: true,
    filled: false,
    pickable: false,
    // Softer borders — the previous near-black 200α at 3 px read as a
    // heavy outline that fought with the ramp colours.
    getLineColor: [120, 128, 140, 130],
    getLineWidth: 1.5,
    lineWidthMinPixels: 1,
    lineWidthUnits: 'pixels',
    getDashArray: [5, 4],
    dashJustified: true,
    extensions: [DISTRICT_DASH_EXT],
    parameters: { depthTest: false },
    updateTriggers: { getLineColor: [districtScoreTick], data: [districtScoreTick] }
  });
}


