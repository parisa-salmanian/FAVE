// Right-side inspector panel (Fairness / Stats / Compare tabs).
// Adapted from the Claude Design handoff. Reads existing FAVE state
// (mezoHexData, overallGini, currentPOIsFC, selectedPOIMix, ALL_CATEGORIES,
// LOCAL_CITY_NAMES, lastCityName) — no new state of its own except the
// currently selected hex/district object.

(function () {
  const POI_LABEL = {
    grocery: 'Grocery',
    hospital: 'Hospital',
    pharmacy: 'Pharmacy',
    dentistry: 'Dentistry',
    healthcare_center: 'Healthcare center',
    veterinary: 'Veterinary',
    university: 'University',
    kindergarten: 'Kindergarten',
    school_primary: 'Primary school',
    school_high: 'High school',
  };
  const POI_COLOR = {
    grocery: 'oklch(62% 0.14 40)',
    hospital: 'oklch(58% 0.16 25)',
    pharmacy: 'oklch(60% 0.14 340)',
    dentistry: 'oklch(60% 0.12 310)',
    healthcare_center: 'oklch(58% 0.13 280)',
    veterinary: 'oklch(62% 0.13 100)',
    university: 'oklch(58% 0.13 245)',
    kindergarten: 'oklch(70% 0.13 80)',
    school_primary: 'oklch(62% 0.12 200)',
    school_high: 'oklch(55% 0.13 220)',
  };

  // Ramp gradient samples, in score order (0 → 1). Match #inspector --insp-fair-*.
  const FAIR_RAMP = [
    'oklch(35% 0.13 285)',
    'oklch(45% 0.13 250)',
    'oklch(58% 0.12 200)',
    'oklch(72% 0.13 155)',
    'oklch(85% 0.16 110)',
  ];
  function rampColor(score) {
    if (!Number.isFinite(score)) return 'var(--insp-surface-2)';
    const t = Math.max(0, Math.min(1, score));
    const i = t * (FAIR_RAMP.length - 1);
    return FAIR_RAMP[Math.round(i)] || FAIR_RAMP[0];
  }

  function fmt(n, digits = 3) {
    return Number.isFinite(n) ? n.toFixed(digits) : '—';
  }

  // Currently inspected object: { kind: 'mezo' | 'district' | null, ... }
  let selected = null;

  // Persistent monotonic scroll floor (px) for the active pane — see the long
  // note in renderSelection. Reset when the panel closes or the tab changes so
  // a fresh context starts at its natural height.
  let _scrollFloor = 0;

  // ---- 2SFCA companion metric (network-accurate supply-to-demand provision) ----
  // Loaded lazily per building click; guarded by a token so a stale async load
  // (user clicked another building meanwhile) doesn't overwrite the selection.
  let _a2sToken = 0;
  function _a2sModeFromUI() {
    let m = (document.getElementById('fairnessTravelMode')?.value || 'walking').toLowerCase();
    return (m === 'walking' || m === 'cycling' || m === 'driving') ? m : 'walking';
  }
  // Normalise per-building 2SFCA cats into the uniform shape the renderer reads:
  // { norm (0..1), unreachable (no POI of this cat in range), sub (optional note) }.
  function _a2sCatsFromBuilding(res) {
    const out = {};
    for (const [cat, cd] of Object.entries(res.cats || {})) {
      out[cat] = { norm: cd.norm, unreachable: !(cd.raw > 0), sub: null };
    }
    return out;
  }
  // Same uniform shape from an aggregate (district/hex): norm = mean provision,
  // unreachable when NO member building reaches the cat, sub = % of members served.
  function _a2sCatsFromAggregate(agg) {
    const out = {};
    for (const [cat, cd] of Object.entries(agg.cats || {})) {
      const pct = Math.round((cd.reachableFrac || 0) * 100);
      out[cat] = { norm: cd.normMean, unreachable: !(cd.reachableFrac > 0),
                   sub: `${pct}% served` };
    }
    return out;
  }

  async function _attachAccess2sfca(feat) {
    if (typeof ensureAccess2sfca !== 'function' || typeof access2sfcaForFeature !== 'function') return;
    const token = ++_a2sToken;
    const mode = _a2sModeFromUI();
    const city = (typeof a2sCurrentCity === 'function') ? a2sCurrentCity() : undefined;
    try {
      const net = await ensureAccess2sfca(city, mode);
      if (token !== _a2sToken || !selected || selected.kind !== 'building') return;
      selected.a2sPending = false;
      const res = net ? access2sfcaForFeature(feat) : null;
      selected.a2s = res ? { mode, scope: 'building', overall: res.overall, cats: _a2sCatsFromBuilding(res) } : null;
      renderSelection();
    } catch (_) { /* companion metric is best-effort */ }
  }

  // Meso (hex) / macro (district) aggregation: average the per-building network
  // provision over every baked building inside the selection. Best-effort and
  // token-guarded against stale clicks, exactly like the per-building load.
  async function _attachAccess2sfcaAggregate(kind, payload) {
    if (typeof ensureAccess2sfca !== 'function') return;
    const token = ++_a2sToken;
    const mode = _a2sModeFromUI();
    const city = (typeof a2sCurrentCity === 'function') ? a2sCurrentCity() : undefined;
    try {
      const net = await ensureAccess2sfca(city, mode);
      if (token !== _a2sToken || !selected || selected.kind !== kind) return;
      let agg = null;
      if (kind === 'mezo' && typeof access2sfcaAggregateForHex === 'function') {
        agg = net ? access2sfcaAggregateForHex(payload) : null;
      } else if (kind === 'district' && typeof access2sfcaAggregateForPolygon === 'function') {
        agg = net ? access2sfcaAggregateForPolygon(payload) : null;
      }
      if (token !== _a2sToken || !selected || selected.kind !== kind) return;
      selected.a2sPending = false;
      selected.a2s = (agg && agg.n) ? { mode, scope: kind, overall: agg.overall, n: agg.n, cats: _a2sCatsFromAggregate(agg) } : null;
      // Synthetic resident sum over the buildings inside this hex/district.
      selected.synthAgg = (kind === 'mezo')
        ? (typeof synthpopAggregateForHex === 'function' ? synthpopAggregateForHex(payload) : null)
        : (typeof synthpopAggregateForPolygon === 'function' ? synthpopAggregateForPolygon(payload) : null);
      renderSelection();
    } catch (_) { /* companion metric is best-effort */ }
  }

  function pickActiveCategories() {
    if (typeof selectedPOIMix !== 'undefined' && Array.isArray(selectedPOIMix) && selectedPOIMix.length) {
      return selectedPOIMix.map(e => e.cat);
    }
    if (typeof ALL_CATEGORIES !== 'undefined' && Array.isArray(ALL_CATEGORIES)) {
      return ALL_CATEGORIES.slice(0, 10);
    }
    return [];
  }

  function renderCityOverview() {
    const isFairActive = (typeof fairActive !== 'undefined' && fairActive);
    const catGini2 = (typeof currentCategoryGini !== 'undefined' && Number.isFinite(currentCategoryGini))
      ? currentCategoryGini : null;
    const displayGini = (isFairActive && catGini2 != null) ? catGini2 : null;
    const poiCount = (typeof currentPOIsFC !== 'undefined' && currentPOIsFC?.features?.length) || 0;
    const activeCats = pickActiveCategories();
    const cityName = (typeof lastCityName === 'string' && lastCityName) ? lastCityName : '—';

    const setText = (id, text) => {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    };
    setText('inspCityName', cityName);
    setText('inspOverallFairness', displayGini != null ? fmt(displayGini) : '—');
    setText('inspGini', displayGini != null ? fmt(displayGini) : '—');
    setText('inspPoiCount', String(poiCount));
    setText('inspActiveCats', String(activeCats.length));
  }

  function renderSelection() {
    const titleEl = document.getElementById('inspSelectionTitle');
    const idEl = document.getElementById('inspSelectionId');
    const listEl = document.getElementById('inspSelectionList');
    const emptyEl = document.getElementById('inspSelectionEmpty');
    const markerEl = document.getElementById('inspRampMarker');
    if (!titleEl || !listEl || !emptyEl) return;

    // Keep the scrollbar exactly where the user left it across re-renders, in
    // ANY condition — the panel must never creep upward. Hover mirrors a new
    // feature on every mouse-move and the async 2SFCA load re-renders a frame
    // later; both rewrite the content. When the new content is SHORTER (a "no
    // data" placeholder), the browser clamps scrollTop to the new max and the
    // view jumps up; the previous attempt then re-measured by CLEARING the
    // spacer, which itself momentarily shrank the content and let the clamp
    // through — so the position crept up a little on every render.
    //
    // Robust fix: a PERSISTENT, monotonic floor. We reserve a min-height on the
    // active pane that only ever GROWS while the panel stays scrolled, so the
    // scroll content can never shrink and the browser can never clamp. We never
    // clear it to "measure" (that was the leak); instead we read scrollHeight
    // WITH the floor in place — it already equals max(content, floor), so it
    // tells us when real content has outgrown the floor. The reserved space is
    // released only when the user is back at the top (keepScroll <= 0), where
    // shrinking is harmless. White space below a sparse selection is intentional.
    const bodyEl = document.querySelector('#inspector .inspector-body');
    const paneEl = bodyEl ? bodyEl.querySelector('.insp-pane[data-active="true"]') : null;
    const keepScroll = bodyEl ? bodyEl.scrollTop : 0;
    const restoreScroll = () => {
      if (!bodyEl) return;
      if (paneEl) {
        if (keepScroll <= 0) _scrollFloor = 0;          // at top → safe to release
        paneEl.style.minHeight = `${_scrollFloor}px`;   // hold the floor (never cleared)
        const sh = paneEl.scrollHeight;                 // = max(content, floor)
        if (sh > _scrollFloor) {                         // real content outgrew floor
          _scrollFloor = sh;
          paneEl.style.minHeight = `${_scrollFloor}px`;
        }
      }
      if (bodyEl.scrollTop !== keepScroll) bodyEl.scrollTop = keepScroll;
    };

    if (!selected) {
      titleEl.textContent = 'Hover or click';
      if (idEl) idEl.textContent = '';
      listEl.style.display = 'none';
      emptyEl.style.display = 'block';
      if (markerEl) markerEl.setAttribute('data-shown', 'false');
      // Scale-aware empty hint, mirroring the design.
      const isDistrictView = (typeof districtView !== 'undefined' && districtView);
      const isMezoView = (typeof mezoView !== 'undefined' && mezoView);
      emptyEl.textContent = isMezoView
        ? 'Click any hexagon to inspect its accessibility breakdown.'
        : isDistrictView
        ? 'Click a district to compare aggregations.'
        : 'Click a building for IF-City debug values.';
      renderCategoryBars(null);
      renderAccess2sfcaBars();
      renderDemographics();
      restoreScroll();
      return;
    }

    const titleByKind = { district: 'District', mezo: 'Cell', building: 'Building' };
    titleEl.textContent = titleByKind[selected.kind] || 'Selection';
    if (idEl) idEl.textContent = selected.id || '';

    listEl.style.display = '';
    emptyEl.style.display = 'none';

    const rows = [];
    if (selected.name) rows.push(['Name', selected.name]);
    rows.push(['Fairness score', fmt(selected.fairness)]);
    if (Number.isFinite(selected.gravity)) rows.push(['Gravity utility', fmt(selected.gravity)]);
    if (Number.isFinite(selected.distance)) rows.push(['Distance model', fmt(selected.distance)]);
    if (selected.kind !== 'building' && Number.isFinite(selected.count)) {
      rows.push(['Buildings', selected.count.toLocaleString()]);
    }
    if (Number.isFinite(selected.population)) rows.push(['Population', selected.population.toLocaleString()]);

    listEl.innerHTML = rows.map(([k, v]) =>
      `<div class="kv"><span class="kv-key">${k}</span><span class="kv-val">${v}</span></div>`
    ).join('');

    if (markerEl) {
      if (Number.isFinite(selected.fairness)) {
        markerEl.style.left = `${Math.max(0, Math.min(1, selected.fairness)) * 100}%`;
        markerEl.setAttribute('data-shown', 'true');
      } else {
        markerEl.setAttribute('data-shown', 'false');
      }
    }

    renderCategoryBars(selected.byCat || null);
    renderAccess2sfcaBars();
    renderDemographics();
    restoreScroll();
  }

  // Population & demographics card (below supply provision). Single source: the
  // unified synthetic record (building) or its pop-weighted aggregate (hex/
  // district) — synthetic pop/income/need + DESO-inherited shares.
  function renderDemographics() {
    const section = document.getElementById('inspDemoSection');
    const list = document.getElementById('inspDemoList');
    const titleEl = document.getElementById('inspDemoTitle');
    if (!section || !list) return;
    const agg = selected && selected.kind !== 'building';
    const d = selected ? (agg ? selected.synthAgg : selected.synth) : null;
    // Only fully remove the card when NOTHING is selected (the panel's empty
    // state). When something IS selected but has no synthetic residents, keep
    // the card MOUNTED with a muted note — removing it collapses the panel
    // height and yanks the scroll to the top while you sweep the map.
    if (!selected) {
      section.style.display = 'none'; list.innerHTML = ''; return;
    }
    if (!d || !(Number.isFinite(d.pop) && d.pop > 0)) {
      if (titleEl) titleEl.textContent = 'Population & demographics';
      section.style.display = '';
      list.innerHTML = '<div class="empty">No synthetic residents for this selection.</div>';
      return;
    }
    if (titleEl) {
      const scope = selected.kind === 'district' ? 'district' : selected.kind === 'mezo' ? 'cell' : null;
      titleEl.textContent = scope
        ? `Population & demographics (${scope}${Number.isFinite(d.n) ? ` · ${d.n.toLocaleString()} bldgs` : ''})`
        : 'Population & demographics';
    }
    const pctFrac = v => `${(v * 100).toFixed(1)}%`;     // 0..1 fraction → %
    const pctRaw  = v => `${v.toFixed(1)}%`;             // already a percentage
    const rows = [];
    rows.push(['Residents (synthetic)', Math.round(d.pop).toLocaleString() + (agg ? ' (sum)' : '')]);
    if (Number.isFinite(d.income)) rows.push([`Income (synthetic${agg ? ', mean' : ''})`, `${Math.round(d.income).toLocaleString()} kSEK`]);
    if (Number.isFinite(d.need))   rows.push([`Need index (synthetic${agg ? ', mean' : ''})`, d.need.toFixed(2)]);
    if (!agg) {
      if (Number.isFinite(d.levels)) rows.push(['Building storeys', String(d.levels)]);
      if (Number.isFinite(d.area))   rows.push(['Footprint area', `${Math.round(d.area).toLocaleString()} m²`]);
      if (Number.isFinite(d.zone))   rows.push(['Land-use zone', `#${d.zone}`]);
    }
    if (Number.isFinite(d.child_frac))     rows.push(['Children %', pctFrac(d.child_frac)]);
    if (Number.isFinite(d.elder_frac))     rows.push(['Elderly %', pctFrac(d.elder_frac)]);
    if (Number.isFinite(d.dependency))     rows.push(['Dependency ratio', d.dependency.toFixed(2)]);
    if (Number.isFinite(d.higher_ed))      rows.push(['Higher-ed %', pctRaw(d.higher_ed)]);
    if (Number.isFinite(d.neet))           rows.push(['NEET %', pctRaw(d.neet)]);
    if (Number.isFinite(d.income_support)) rows.push(['Income support %', pctRaw(d.income_support)]);
    if (Number.isFinite(d.male_frac))      rows.push(['Male %', pctFrac(d.male_frac)]);
    if (!agg && d.deso) rows.push(['DESO', String(d.deso)]);
    section.style.display = '';
    list.innerHTML = rows.map(([k, v]) =>
      `<div class="kv"><span class="kv-key">${k}</span><span class="kv-val">${v}</span></div>`
    ).join('');
  }

  // Network-accurate E2SFCA companion: overall supply-to-demand provision plus a
  // per-category breakdown, rendered BELOW the per-category fairness bars in the
  // same bar-row style (legend icon + coloured track + number). Normalised 0..1
  // PER CATEGORY in the bake consumer, so values are comparable within a service
  // type, not across types. Section only shows for a building with loaded 2SFCA.
  //
  // Unreachable vs lowest-served: raw A === 0 means no POI of that category is
  // reachable over the network within the catchment for the active mode — shown
  // as a greyed "—" with an empty track, distinct from "0%" (reachable but at the
  // p10 provision floor), so planners can tell "no service in range" apart from
  // "served but worst-off".
  function renderAccess2sfcaBars() {
    const section = document.getElementById('insp2sfcaSection');
    const host = document.getElementById('insp2sfcaBars');
    const titleEl = document.getElementById('insp2sfcaTitle');
    if (!section || !host) return;

    // Works at all three scales: building (per-building provision) and the
    // meso/macro aggregates (mean provision over the buildings inside the hex /
    // district). The cats are pre-normalised into a uniform { norm, unreachable,
    // sub } shape by the attach functions, so the renderer is scale-agnostic.
    if (!selected || !selected.a2s || !selected.a2s.cats) {
      // Only fully remove the card when NOTHING is selected. When a selection
      // exists but its provision isn't ready (fetched async) or none is in
      // range, keep the card MOUNTED so the panel height stays stable and the
      // scroll never jumps — collapsing it is what yanked the panel to the top.
      if (!selected) {
        section.style.display = 'none';
        section.removeAttribute('data-loading');
        host.innerHTML = '';
        return;
      }
      section.style.display = '';
      const hasRealBars = host.children.length && !host.querySelector('.empty');
      if (hasRealBars && selected.a2sPending) {
        // Stale bars from the previous selection — dim them while the new
        // provision loads, instead of swapping to a placeholder (less flicker).
        section.setAttribute('data-loading', 'true');
      } else {
        section.removeAttribute('data-loading');
        host.innerHTML = `<div class="empty">${selected.a2sPending
          ? 'Loading supply provision…'
          : 'No supply provision in range for this selection.'}</div>`;
      }
      return;
    }
    section.removeAttribute('data-loading');
    const a = selected.a2s;
    const scopeLabel = a.scope === 'district' ? 'district'
                     : a.scope === 'mezo' ? 'cell' : null;
    if (titleEl) {
      titleEl.textContent = scopeLabel
        ? `Supply provision (2SFCA · ${a.mode} · ${scopeLabel} mean${a.n ? `, ${a.n.toLocaleString()} bldgs` : ''})`
        : `Supply provision (2SFCA · ${a.mode})`;
    }

    const cats = pickActiveCategories();
    const rowsHtml = cats.map(cat => {
      const cd = a.cats?.[cat];
      if (!cd) return null;
      const label = POI_LABEL[cat] || cat;
      const unreachable = !!cd.unreachable;       // no service in range (per bldg or whole area)
      const norm = Number.isFinite(cd.norm) ? cd.norm : 0;
      const w = unreachable ? 0 : Math.max(2, Math.min(100, norm * 100));
      const num = unreachable ? '—' : `${Math.round(norm * 100)}%`;
      const numStyle = unreachable ? ' style="opacity:.5"' : '';
      const sub = cd.sub ? `<span class="insp-tiny muted" style="margin-left:4px">${cd.sub}</span>` : '';
      const title = unreachable
        ? `No ${label.toLowerCase()} reachable over the ${a.mode} network within range`
        : `${label}: ${Math.round(norm * 100)}% provision (normalized within category)${cd.sub ? ' · ' + cd.sub : ''}`;
      // a2s-row: flat supply hue from CSS (magnitude = width), distinct from the
      // colour-coded fairness bars above. No inline background here.
      return `
        <div class="bar-row a2s-row${unreachable ? ' a2s-unreachable' : ''}" title="${title}">
          <span class="label"><img src="assets/icons/legend-${cat}.svg" alt="" class="label-icon" aria-hidden="true">${label}${sub}</span>
          <div class="bar-track"><div class="bar-fill" style="width:${w}%"></div></div>
          <span class="num"${numStyle}>${num}</span>
        </div>`;
    }).filter(Boolean);

    if (!rowsHtml.length) {
      section.style.display = 'none';
      host.innerHTML = '';
      return;
    }

    const overall = Number.isFinite(a.overall) ? `${Math.round(a.overall * 100)}%` : '—';
    section.style.display = '';
    host.innerHTML =
      `<div class="bar-row a2s-row a2s-overall" style="font-weight:700">
        <span class="label">Overall</span>
        <div class="bar-track"><div class="bar-fill" style="width:${Number.isFinite(a.overall) ? Math.max(2, Math.min(100, a.overall * 100)) : 0}%"></div></div>
        <span class="num">${overall}</span>
      </div>` + rowsHtml.join('');
  }

  function renderCategoryBars(byCat) {
    const host = document.getElementById('inspCategoryBars');
    if (!host) return;
    const cats = pickActiveCategories();
    if (!cats.length) {
      host.innerHTML = '<div class="empty">Pick categories to see per-category fairness.</div>';
      return;
    }
    host.innerHTML = cats.map(cat => {
      const label = POI_LABEL[cat] || cat;
      const v = byCat && Number.isFinite(byCat[cat]) ? byCat[cat] : null;
      const w = v != null ? Math.max(2, Math.min(100, v * 100)) : 0;
      const num = v != null ? v.toFixed(2) : '—';
      const fillBg = v != null ? rampColor(v) : 'var(--insp-surface-2)';
      // Use the legend SVGs (already coloured circle + white glyph baked
      // in) so the per-category row matches the POIs & Weights menu and
      // the map's POI markers.
      return `
        <div class="bar-row">
          <span class="label"><img src="assets/icons/legend-${cat}.svg" alt="" class="label-icon" aria-hidden="true">${label}</span>
          <div class="bar-track"><div class="bar-fill" style="width:${w}%;background:${fillBg}"></div></div>
          <span class="num">${num}</span>
        </div>`;
    }).join('');
  }

  function renderStatsTab() {
    const data = (typeof mezoHexData !== 'undefined' && Array.isArray(mezoHexData)) ? mezoHexData : [];
    const scores = data
      .map(c => Number.isFinite(c?.__score) ? c.__score
              : Number.isFinite(c?.__fairOverall) ? c.__fairOverall
              : null)
      .filter(v => Number.isFinite(v))
      .sort((a, b) => a - b);

    const setText = (id, text) => {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    };

    // Population-weighted per-group equity (all cities) — independent of the
    // mezo histogram below, so it renders even before the Mezo view is opened.
    if (typeof renderGroupEquity === 'function') { try { renderGroupEquity(); } catch (_) {} }

    if (!scores.length) {
      setText('statsN', '0');
      setText('statsMedian', '—');
      setText('statsMean', '—');
      setText('statsP25P75', '—');
      const host = document.getElementById('statsHistogram');
      if (host) host.innerHTML = '<div class="empty">No mezo data yet — open Mezo view and run fairness.</div>';
      return;
    }

    const n = scores.length;
    const median = scores[Math.floor(n / 2)];
    const mean = scores.reduce((a, b) => a + b, 0) / n;
    const p25 = scores[Math.floor(n * 0.25)];
    const p75 = scores[Math.floor(n * 0.75)];

    setText('statsN', `n=${n}`);
    setText('statsMedian', fmt(median));
    setText('statsMean', fmt(mean));
    setText('statsP25P75', `${p25.toFixed(2)} / ${p75.toFixed(2)}`);

    const bins = 12;
    const counts = new Array(bins).fill(0);
    for (const v of scores) counts[Math.min(bins - 1, Math.floor(v * bins))]++;
    const max = Math.max(...counts) || 1;

    const host = document.getElementById('statsHistogram');
    if (host) {
      host.innerHTML = counts.map((v, i) => {
        const t = i / (bins - 1);
        const h = (v / max) * 100;
        return `<div class="histogram-bar" style="height:${h}%;background:${rampColor(t)}"></div>`;
      }).join('');
    }
  }

  function renderCompareTab() {
    const setText = (id, text) => {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    };

    const overall = (typeof overallGini === 'number' && Number.isFinite(overallGini)) ? overallGini : null;

    // ----- Macro (district mean fairness + count) -----
    const districtFeats = (typeof districtFC !== 'undefined' && Array.isArray(districtFC?.features))
      ? districtFC.features : [];
    const districtScores = districtFeats
      .map(f => Number.isFinite(f?.properties?.__score) ? f.properties.__score
              : Number.isFinite(f?.properties?.__fairOverall) ? f.properties.__fairOverall
              : null)
      .filter(v => Number.isFinite(v));
    const macroMean = districtScores.length
      ? districtScores.reduce((a, b) => a + b, 0) / districtScores.length
      : null;
    setText('cmpMacroFairness', macroMean != null ? fmt(macroMean) : '—');
    const macroCountEl = document.querySelector('.compare-grid > div:nth-child(1) .c-sub');
    if (macroCountEl) {
      macroCountEl.textContent = districtFeats.length
        ? `${districtFeats.length} districts`
        : 'districts';
    }

    // ----- Meso (overall Gini + cell count) -----
    setText('cmpMesoFairness', overall != null ? fmt(overall) : '—');
    const cellCount = (typeof mezoHexData !== 'undefined' && mezoHexData?.length) || 0;
    setText('cmpMesoCells', cellCount ? `${cellCount} cells` : '—');

    // ----- Micro (per-building mean fairness + count) -----
    const cityFeats = (typeof baseCityFC !== 'undefined' && Array.isArray(baseCityFC?.features))
      ? baseCityFC.features : [];
    const microScores = cityFeats
      .map(f => Number.isFinite(f?.properties?.fair?.score) ? f.properties.fair.score
              : Number.isFinite(f?.properties?.fair_overall?.score) ? f.properties.fair_overall.score
              : null)
      .filter(v => Number.isFinite(v));
    const microMean = microScores.length
      ? microScores.reduce((a, b) => a + b, 0) / microScores.length
      : null;
    setText('cmpMicroFairness', microMean != null ? fmt(microMean) : '—');
    const bldgCount = cityFeats.length;
    setText('cmpMicroBldgs', bldgCount ? `${bldgCount.toLocaleString()} bld` : '—');
  }

  function renderMetricStrip() {
    const setText = (id, text) => {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    };
    const isFairActive = (typeof fairActive !== 'undefined' && fairActive);
    const catGini = (typeof currentCategoryGini !== 'undefined' && Number.isFinite(currentCategoryGini))
      ? currentCategoryGini : null;
    // Show "—" when no POIs are selected; only show a value when fairActive.
    const giniVal = (isFairActive && catGini != null) ? catGini : null;
    const cityFeats = (typeof baseCityFC !== 'undefined' && Array.isArray(baseCityFC?.features))
      ? baseCityFC.features : [];
    // Use per-selection scores (fair.score) only when active; "—" otherwise.
    const microScores = isFairActive
      ? cityFeats
          .map(f => Number.isFinite(f?.properties?.fair?.score) ? f.properties.fair.score : null)
          .filter(v => Number.isFinite(v))
      : [];
    const meanFair = microScores.length
      ? microScores.reduce((a, b) => a + b, 0) / microScores.length
      : null;
    const poiCount = (typeof currentPOIsFC !== 'undefined' && currentPOIsFC?.features?.length) || 0;

    // Units metric tracks the active scale: districts in Macro, hex cells in
    // Meso, buildings in Micro. The label updates in lock-step so the number
    // is never ambiguous.
    const inDistrict = (typeof districtView !== 'undefined' && districtView);
    const inMezo = (typeof mezoView !== 'undefined' && mezoView);
    let unitsLabel, unitsCount;
    if (inDistrict) {
      const districtFeats = (typeof districtFC !== 'undefined' && Array.isArray(districtFC?.features))
        ? districtFC.features : [];
      unitsLabel = 'Districts';
      unitsCount = districtFeats.length;
    } else if (inMezo) {
      unitsLabel = 'Cells';
      unitsCount = (typeof mezoHexData !== 'undefined' && mezoHexData?.length) || 0;
    } else {
      unitsLabel = 'Buildings';
      unitsCount = cityFeats.length;
    }

    setText('metricOverall', meanFair != null ? fmt(meanFair) : '—');
    setText('metricGini', giniVal != null ? fmt(giniVal) : '—');
    setText('metricPois', String(poiCount));
    setText('metricUnitsLabel', unitsLabel);
    setText('metricCells', String(unitsCount));
  }

  function refreshActiveTab() {
    renderCityOverview();
    renderSelection();
    renderStatsTab();
    renderCompareTab();
    renderMetricStrip();
  }

  // ===== Public API =====
  window.faveInspector = {
    /** Called by overlays.js handleMezoClick. */
    setMezoSelection(cell) {
      if (!cell) { selected = null; renderSelection(); return; }
      const score = (typeof mezoOverlayScore === 'function')
        ? mezoOverlayScore(cell)
        : (Number.isFinite(cell.__score) ? cell.__score
           : Number.isFinite(cell.__fairOverall) ? cell.__fairOverall
           : null);
      selected = {
        kind: 'mezo',
        id: String(cell.hex || ''),
        name: null,
        fairness: Number.isFinite(score) ? score : null,
        count: Number.isFinite(cell.__count) ? cell.__count : null,
        byCat: cell.__fairByCat || null,
        a2s: null,
        a2sPending: true,
      };
      open();
      renderSelection();
      if (cell.hex) _attachAccess2sfcaAggregate('mezo', cell.hex);
    },

    /** Called by overlays.js handleDistrictClick / handleDistrictDoubleClick. */
    setDistrictSelection(districtFeat) {
      if (!districtFeat) { selected = null; renderSelection(); return; }
      const props = districtFeat.properties || {};
      const score = Number.isFinite(props.__score) ? props.__score
                  : Number.isFinite(props.__fairOverall) ? props.__fairOverall
                  : null;
      selected = {
        kind: 'district',
        id: String(props.regso || props.deso || props.code || ''),
        name: props.__districtName || props.name || props.namn || null,
        fairness: Number.isFinite(score) ? score : null,
        count: Number.isFinite(props.__count) ? props.__count : null,
        byCat: props.__fairByCat || null,
        a2s: null,
        a2sPending: true,
      };
      open();
      renderSelection();
      _attachAccess2sfcaAggregate('district', districtFeat);
    },

    /** Called by interaction.js handleClick (single-click on a building). */
    setBuildingSelection(buildingFeat) {
      if (!buildingFeat) { selected = null; renderSelection(); return; }
      const props = buildingFeat.properties || {};
      // Per-building score: prefer fair_overall.score, else current category, else __ifcity benefit.
      let score = null;
      if (props.fair_overall && Number.isFinite(props.fair_overall.score)) score = props.fair_overall.score;
      else if (props.fair && Number.isFinite(props.fair.score)) score = props.fair.score;
      // Per-category breakdown — fair_multi has shape { cat: { score, dist_m, time_min } }
      const byCat = {};
      if (props.fair_multi && typeof props.fair_multi === 'object') {
        for (const k of Object.keys(props.fair_multi)) {
          const v = props.fair_multi[k];
          if (v && Number.isFinite(v.score)) byCat[k] = v.score;
        }
      }
      const name = props.name || props.namn
        || props.byggnadsnamn1 || props.byggnadsnamn2
        || props.objekttyp || props.building || null;
      selected = {
        kind: 'building',
        id: String(props.objektidentitet || props.id || props['@id'] || ''),
        name,
        fairness: score,
        count: 1,
        gravity: props.__ifcity?.utility,
        byCat: Object.keys(byCat).length ? byCat : null,
        a2s: null,
        a2sPending: true,
        synth: (typeof synthDemographicsForFeature === 'function') ? synthDemographicsForFeature(buildingFeat) : null,
      };
      open();
      renderSelection();
      _attachAccess2sfca(buildingFeat);
    },

    clearSelection() { selected = null; renderSelection(); },

    /** Called externally when fairness recomputes. */
    refresh: refreshActiveTab,
  };

  function open() {
    const root = document.getElementById('inspector');
    if (!root) return;
    const wasOpen = root.getAttribute('data-open') === 'true';
    if (wasOpen) return;  // truly idempotent — avoid re-triggering the
                          // shell's MutationObserver on data-open, which
                          // chains into scheduleMapResize.
    root.setAttribute('data-open', 'true');
    const btn = document.getElementById('inspectorToggleBtn');
    if (btn) btn.setAttribute('data-active', 'true');
  }

  function close() {
    const root = document.getElementById('inspector');
    if (!root) return;
    const wasOpen = root.getAttribute('data-open') === 'true';
    if (!wasOpen) return;
    root.setAttribute('data-open', 'false');
    const btn = document.getElementById('inspectorToggleBtn');
    if (btn) btn.setAttribute('data-active', 'false');
    _scrollFloor = 0;
    const pane = root.querySelector('.inspector-body .insp-pane[data-active="true"]');
    if (pane) pane.style.minHeight = '';
  }

  function toggle() {
    const root = document.getElementById('inspector');
    if (!root) return;
    if (root.getAttribute('data-open') === 'true') close();
    else { open(); refreshActiveTab(); }
  }

  document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('inspector');
    if (!root) return;

    // Tab switching.
    root.querySelectorAll('.inspector-tab').forEach(btn => {
      btn.addEventListener('click', () => {
        const tab = btn.dataset.tab;
        root.querySelectorAll('.inspector-tab').forEach(b => {
          b.setAttribute('data-active', b.dataset.tab === tab ? 'true' : 'false');
        });
        root.querySelectorAll('.insp-pane').forEach(p => {
          p.setAttribute('data-active', p.dataset.tab === tab ? 'true' : 'false');
          p.style.minHeight = '';   // drop any reserved spacer from the old tab
        });
        _scrollFloor = 0;           // fresh tab → start at natural height
        refreshActiveTab();
      });
    });

    const closeBtn = document.getElementById('inspectorCloseBtn');
    if (closeBtn) closeBtn.addEventListener('click', close);

    const toggleBtn = document.getElementById('inspectorToggleBtn');
    if (toggleBtn) toggleBtn.addEventListener('click', toggle);

    refreshActiveTab();
  });
})();
