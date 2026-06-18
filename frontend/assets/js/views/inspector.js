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

  // ---- 2SFCA companion metric (network-accurate supply-to-demand provision) ----
  // Loaded lazily per building click; guarded by a token so a stale async load
  // (user clicked another building meanwhile) doesn't overwrite the selection.
  let _a2sToken = 0;
  function _a2sModeFromUI() {
    let m = (document.getElementById('fairnessTravelMode')?.value || 'walking').toLowerCase();
    return (m === 'walking' || m === 'cycling' || m === 'driving') ? m : 'walking';
  }
  async function _attachAccess2sfca(feat) {
    if (typeof ensureAccess2sfca !== 'function' || typeof access2sfcaForFeature !== 'function') return;
    const token = ++_a2sToken;
    const mode = _a2sModeFromUI();
    const city = (typeof a2sCurrentCity === 'function') ? a2sCurrentCity() : undefined;
    try {
      const net = await ensureAccess2sfca(city, mode);
      if (token !== _a2sToken || !selected || selected.kind !== 'building') return;
      const res = net ? access2sfcaForFeature(feat) : null;
      selected.a2s = res ? { mode, overall: res.overall, cats: res.cats } : null;
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
    ).join('') + renderAccess2sfcaBlock();

    if (markerEl) {
      if (Number.isFinite(selected.fairness)) {
        markerEl.style.left = `${Math.max(0, Math.min(1, selected.fairness)) * 100}%`;
        markerEl.setAttribute('data-shown', 'true');
      } else {
        markerEl.setAttribute('data-shown', 'false');
      }
    }

    renderCategoryBars(selected.byCat || null);
  }

  // Network-accurate E2SFCA companion: overall supply-to-demand provision plus a
  // per-category breakdown. Normalised 0..1 PER CATEGORY in the bake consumer, so
  // values are comparable within a service type (not across types). Rendered as a
  // self-contained HTML block (no CSS-grid coupling) inside the selection list.
  function renderAccess2sfcaBlock() {
    if (!selected || selected.kind !== 'building' || !selected.a2s) return '';
    const a = selected.a2s;
    const cats = pickActiveCategories();
    const lines = cats.map(cat => {
      const cd = a.cats?.[cat];
      if (!cd) return null;
      const label = (POI_LABEL[cat] || cat).padEnd(18);
      const pct = `${Math.round((cd.norm || 0) * 100)}%`.padStart(4);
      const bar = '█'.repeat(Math.round((cd.norm || 0) * 10)).padEnd(10, '·');
      return `${label}${pct} ${bar}`;
    }).filter(Boolean);
    const overall = Number.isFinite(a.overall) ? `${Math.round(a.overall * 100)}%` : '—';
    return `
      <div class="kv" style="margin-top:6px;border-top:1px solid var(--insp-border,rgba(0,0,0,.1));padding-top:6px;">
        <span class="kv-key" title="Enhanced 2-Step Floating Catchment Area: how much supply is actually available to you once competing demand is accounted for, over the real ${a.mode} street network.">Supply provision (2SFCA · ${a.mode})</span>
        <span class="kv-val">${overall}</span>
      </div>
      ${lines.length ? `<pre style="white-space:pre;line-height:1.3;font-size:11px;margin:4px 0 0;opacity:.85;">${lines.join('\n')}</pre>` : ''}`;
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
      };
      open();
      renderSelection();
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
      };
      open();
      renderSelection();
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
        });
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
