// FAVE shell — builds the design's full UI (topbar, rail, popovers, AI
// sidebar, map overlays) at runtime and wires user actions back to the
// legacy form controls (citySelect, poi-check, etc.) which stay hidden in
// the DOM so existing fairness/POI/DR handlers keep firing untouched.

(function () {
  // ---------------- icon set ----------------
  const ICONS = {
    district: '<path d="M3 6 L9 3 L15 5 L21 3 L21 18 L15 20 L9 18 L3 21 Z"/><path d="M9 3v15M15 5v15" opacity="0.5"/>',
    hexagons: '<path d="M9 3 L15 3 L18 8 L15 13 L9 13 L6 8 Z"/><path d="M3 13 L9 13 L12 18 L9 23" opacity="0.5"/>',
    building: '<path d="M4 21V8l8-5 8 5v13"/><path d="M9 9h2M13 9h2M9 13h2M13 13h2M9 17h6"/>',
    layers: '<path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5" opacity="0.5"/><path d="M2 12l10 5 10-5" opacity="0.7"/>',
    sparkle: '<path d="M12 3l1.7 5.3L19 10l-5.3 1.7L12 17l-1.7-5.3L5 10l5.3-1.7z"/>',
    wand: '<path d="M15 4V2M15 16v-2M8 9h2M20 9h2M17.8 11.8l1.4 1.4M17.8 6.2l1.4-1.4"/><path d="M3 21l9-9 3 3-9 9z"/>',
    parallel: '<path d="M4 4v16M9 4v16M14 4v16M19 4v16" opacity="0.4"/><path d="M4 8 L9 14 L14 6 L19 11"/>',
    scatter: '<circle cx="6" cy="17" r="1.5" fill="currentColor"/><circle cx="10" cy="12" r="1.5" fill="currentColor"/><circle cx="14" cy="14" r="1.5" fill="currentColor"/><circle cx="17" cy="8" r="1.5" fill="currentColor"/><circle cx="8" cy="7" r="1.5" fill="currentColor"/><circle cx="19" cy="17" r="1.5" fill="currentColor"/>',
    history: '<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l3 2"/>',
    compare: '<path d="M3 6h7v12H3zM14 6h7v12h-7z"/><path d="M10 12h4M12 10v4" opacity="0.5"/>',
    eye:     '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7-10-7-10-7z"/><circle cx="12" cy="12" r="3"/>',
    eyeOff:  '<path d="M3 3l18 18M10.6 10.6a3 3 0 0 0 4.2 4.2M9.4 5.5A10.5 10.5 0 0 1 22 12a13 13 0 0 1-3.1 3.7M6.5 6.7A14 14 0 0 0 2 12s4 7 10 7c1.6 0 3.1-.4 4.4-1.1"/>',
    trash:   '<path d="M3 6h18M8 6V4h8v2M6 6l1 14h10l1-14"/>',
    sliders: '<line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/><circle cx="9" cy="6" r="2.2" fill="white" stroke-width="1.6"/><circle cx="15" cy="12" r="2.2" fill="white" stroke-width="1.6"/><circle cx="11" cy="18" r="2.2" fill="white" stroke-width="1.6"/>',
    settings: '<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M5 5l2.1 2.1M16.9 16.9 19 19M5 19l2.1-2.1M16.9 7.1 19 5"/>',
    walk: '<circle cx="13" cy="4" r="1.6"/><path d="M11.5 9l1.5 4 3 1 1 5"/><path d="M11 8.5L8 12l1 4-1 5"/>',
    bike: '<circle cx="6" cy="17" r="3"/><circle cx="18" cy="17" r="3"/><path d="M6 17 9 7l4 1 3 6 2-4"/><path d="M9 7h2"/>',
    car: '<path d="M5 13l1.5-4.5h11L19 13"/><path d="M3 17v-4h18v4"/><circle cx="7" cy="17" r="1.5"/><circle cx="17" cy="17" r="1.5"/>',
    bus: '<rect x="4" y="4" width="16" height="13" rx="2"/><path d="M4 11h16"/><circle cx="8" cy="20" r="1.4"/><circle cx="16" cy="20" r="1.4"/><path d="M6 17v2M18 17v2"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    minus: '<path d="M5 12h14"/>',
    chev: '<path d="M6 9l6 6 6-6"/>',
    close: '<path d="M18 6L6 18M6 6l12 12"/>',
    moon: '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>',
    help: '<circle cx="12" cy="12" r="9"/><path d="M9.1 9a3 3 0 1 1 5.5 1.7c-.6.6-1.6 1-1.6 2.3M12 17v.01"/>',
    send: '<path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/>',
    // FAVE Reach Compass — accessibility rays converging on a centroid.
    // Drawn for the shell's 24×24 viewBox; explicit stroke-widths override
    // the wrapper's 1.6 default so the outer ring and inner ring read at 16 px.
    logo: '<circle cx="12" cy="12" r="9.5" stroke="currentColor" stroke-width="1.4" fill="none"/><path d="M12 3 L13 12 L12 21 L11 12 Z" fill="currentColor" stroke="none"/><path d="M3 12 L12 11 L21 12 L12 13 Z" fill="currentColor" opacity="0.55" stroke="none"/><circle cx="12" cy="12" r="1.3" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="1.3" stroke="currentColor" stroke-width="0.6" fill="none"/>',
  };
  function svg(key, size = 18) {
    return `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" style="display:block">${ICONS[key]}</svg>`;
  }

  const POI_META = [
    { id: 'grocery',           label: 'Grocery',     color: 'oklch(62% 0.14 40)' },
    { id: 'hospital',          label: 'Hospital',    color: 'oklch(58% 0.16 25)' },
    { id: 'pharmacy',          label: 'Pharmacy',    color: 'oklch(60% 0.14 340)' },
    { id: 'dentistry',         label: 'Dentistry',   color: 'oklch(60% 0.12 310)' },
    { id: 'healthcare_center', label: 'HC Center',   color: 'oklch(58% 0.13 280)' },
    { id: 'veterinary',        label: 'Veterinary',  color: 'oklch(62% 0.13 100)' },
    { id: 'university',        label: 'University',  color: 'oklch(58% 0.13 245)' },
    { id: 'kindergarten',      label: 'Kindergarten',color: 'oklch(70% 0.13 80)' },
    { id: 'school_primary',    label: 'Primary.S',   color: 'oklch(62% 0.12 200)' },
    { id: 'school_high',       label: 'High.S',      color: 'oklch(55% 0.13 220)' },
  ];
  const CITIES = [
    { key: 'vaxjo',      label: 'Växjö' },
    { key: 'goteborg',   label: 'Göteborg' },
    { key: 'malmo',      label: 'Malmö' },
    { key: 'stockholm',  label: 'Stockholm' },
    { key: 'kalmar',     label: 'Kalmar' },
    { key: 'norrkoping', label: 'Norrköping' },
    { key: 'uppsala',    label: 'Uppsala' },
  ];

  // ---------------- topbar ----------------
  function buildTopbar() {
    const top = document.createElement('header');
    top.className = 'fave-topbar';
    top.id = 'faveTopbar';
    top.innerHTML = `
      <div class="topbar-section brand">
        <div class="brand-mark" data-tooltip="FAVE — Fairness Accessibility Visual Explorer" data-tooltip-pos="below">${svg('logo', 16)}</div>
        <div class="brand-name"><strong>FAVE</strong></div>
      </div>
      <div class="topbar-divider"></div>
      <div class="topbar-section">
        <div class="field">
          <span class="field-label">City</span>
          <button class="control" id="topCityBtn" type="button" data-tooltip="Choose city" data-tooltip-pos="below" aria-label="Choose city">
            <span id="topCityLabel">Växjö</span>${svg('chev', 11)}
          </button>
        </div>
        <span id="topJobStatus" style="font-size:11px;color:var(--ink-3);"></span>
      </div>
      <div class="topbar-divider"></div>
      <div class="topbar-section">
        <div class="field">
          <span class="field-label">Model</span>
          <div class="segmented">
            <button data-active="true" data-model="gravity" type="button" data-tooltip="Gravity-based fairness" data-tooltip-pos="below" aria-label="Gravity model">Gravity</button>
            <button data-active="false" data-model="distance" type="button" data-tooltip="Distance-based fairness" data-tooltip-pos="below" aria-label="Distance model">Distance</button>
          </div>
        </div>
      </div>
      <div class="topbar-spacer"></div>
      <div class="topbar-section">
        <button class="icon-btn" id="topThemeBtn" data-tooltip="Toggle light/dark theme" data-tooltip-pos="below" aria-label="Toggle theme" type="button">${svg('moon', 14)}</button>
        <button class="icon-btn" id="topHelpBtn" data-tooltip="Help · FAVE guide" data-tooltip-pos="below" aria-label="Help" type="button">${svg('help', 14)}</button>
      </div>
    `;
    return top;
  }

  // ---------------- rail ----------------
  function buildRail() {
    const rail = document.createElement('aside');
    rail.className = 'fave-rail';
    rail.id = 'faveRail';
    const sections = [
      { label: 'SCALE', items: [
        { id: 'rail-macro', icon: 'district', tip: 'Macro · Districts',  click: () => clickIfExists('districtToggleBtn') },
        { id: 'rail-meso',  icon: 'hexagons', tip: 'Meso · Hexagons',     click: () => toggleMesoPopover() },
        { id: 'rail-micro', icon: 'building', tip: 'Micro · Buildings',   click: () => clickIfExists('microToggleBtn') },
      ]},
      { label: 'TOOLS', items: [
        { id: 'rail-poi',    icon: 'layers',  tip: 'POIs & weights',     click: () => togglePOIPopover() },
        { id: 'rail-equity', icon: 'sparkle', tip: 'Equity analysis',    click: () => toggleEquityAnalysis() },
        { id: 'rail-wif',    icon: 'wand',    tip: 'What-if scenario',   click: () => toggleWhatIfPopover() },
        { id: 'rail-changes',icon: 'compare', tip: 'Changes',            click: () => toggleChangesPopover() },
      ]},
      { label: 'VIEWS', items: [
        { id: 'rail-parallel', icon: 'parallel', tip: 'Parallel coords', click: () => openParallelDrawer() },
        { id: 'rail-dr',       icon: 'scatter',  tip: 'DR Explorer',     click: () => openDRDrawer() },
      ]},
    ];
    const sides = [
      { id: 'rail-ai',        icon: 'sparkle',  tip: 'Ask the map',      click: () => toggleAISidebar() },
      { id: 'rail-history',   icon: 'history',  tip: 'History stack',    click: () => toggleHistoryPopover() },
      { id: 'rail-inspector', icon: 'sliders',  tip: 'Toggle inspector', click: () => toggleInspector() },
    ];
    function btn(item) {
      const b = document.createElement('button');
      b.className = 'rail-btn';
      b.id = item.id;
      b.type = 'button';
      b.setAttribute('data-tooltip', item.tip);
      b.setAttribute('data-tooltip-pos', 'right');
      b.setAttribute('aria-label', item.tip);
      b.innerHTML = svg(item.icon, 18);
      b.addEventListener('click', item.click);
      return b;
    }
    for (const sec of sections) {
      const lbl = document.createElement('div');
      lbl.className = 'rail-section-label';
      lbl.textContent = sec.label;
      rail.appendChild(lbl);
      for (const it of sec.items) rail.appendChild(btn(it));
      const div = document.createElement('div');
      div.className = 'rail-divider';
      rail.appendChild(div);
    }
    for (const it of sides) rail.appendChild(btn(it));
    const spacer = document.createElement('div');
    spacer.style.flex = '1';
    rail.appendChild(spacer);
    return rail;
  }

  // ---------------- map overlays ----------------
  function buildLegend() {
    const w = document.createElement('div');
    w.className = 'shell-legend';
    w.innerHTML = `
      <div class="legend-head">Fairness</div>
      <div class="ramp"></div>
      <div class="ramp-labels"><span>Least fair</span><span>Medium</span><span>Most fair</span></div>
    `;
    return w;
  }
  function buildZoom() {
    const w = document.createElement('div');
    w.className = 'shell-zoom';
    w.id = 'shellZoom';
    w.innerHTML = `
      <button id="shellZoomIn" data-tooltip="Zoom in" aria-label="Zoom in" type="button">${svg('plus', 14)}</button>
      <button id="shellZoomOut" data-tooltip="Zoom out" aria-label="Zoom out" type="button">${svg('minus', 14)}</button>
    `;
    return w;
  }
  function buildTravel() {
    const w = document.createElement('div');
    w.className = 'shell-travel';
    w.id = 'shellTravel';
    const modes = [
      { id: 'walking', icon: 'walk', label: 'Walk', tip: 'Walking distance' },
      { id: 'cycling', icon: 'bike', label: 'Cycle', tip: 'Cycling distance' },
      { id: 'driving', icon: 'car',  label: 'Car', tip: 'Driving distance' },
      { id: 'transit', icon: 'bus',  label: 'Transit', tip: 'Public transport' },
    ];
    w.innerHTML = modes.map(m =>
      `<button data-mode="${m.id}" data-active="${m.id === 'walking' ? 'true' : 'false'}" type="button" data-tooltip="${m.tip}" aria-label="${m.tip}">${svg(m.icon, 13)} ${m.label}</button>`
    ).join('');
    return w;
  }

  // ---------------- popovers ----------------
  function buildCityMenu() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-city-menu';
    p.id = 'shellCityMenu';
    p.innerHTML = `
      <div class="popover-body">
        ${CITIES.map(c => `<button class="city-row" data-key="${c.key}" type="button">${c.label}</button>`).join('')}
      </div>
    `;
    return p;
  }

  function buildPOIPopover() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-poi-menu';
    p.id = 'shellPOIPopover';
    // Use the project's legend-*.svg files for each POI category. They
    // sit at frontend/assets/icons and are named legend-<id>.svg.
    const rows = POI_META.map(m =>
      `<div class="poi-row" data-cat="${m.id}" data-checked="false">
        <label>
          <input type="checkbox" class="shell-poi-check" data-cat="${m.id}">
          <span class="pl-icon">
            <img src="assets/icons/legend-${m.id}.svg" alt="" aria-hidden="true" class="poi-legend-img">
          </span>
          <span>${m.label}</span>
        </label>
        <div></div>
        <input type="range" min="1" max="10" step="1" value="5" class="shell-poi-weight" data-cat="${m.id}" disabled>
        <span class="num shell-poi-numb" data-cat="${m.id}">5</span>
      </div>`
    ).join('');
    p.innerHTML = `
      <div class="popover-header">
        <span class="title">${svg('layers', 13)} POIs &amp; weights</span>
        <button class="close-btn" data-close="poi" type="button">${svg('close', 14)}</button>
      </div>
      <div class="popover-body">
        <div style="font-size:11px;color:var(--ink-3);line-height:1.5;margin-bottom:10px;">
          Select categories and weights (1–10). Map updates live.
        </div>
        ${rows}
        <div class="poi-foot-row">
          <button class="shell-btn" id="shellPOIClear" type="button">Clear</button>
          <button class="shell-btn" id="shellPOIAll" type="button">All</button>
          <span class="spacer"></span>
          <label class="toggle"><input type="checkbox" id="shellPOISymbolsToggle" checked> Symbols</label>
        </div>
      </div>
    `;
    return p;
  }

  function buildChangesPopover() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-changes';
    p.id = 'shellChanges';
    // Explicitly closed at boot. The popover only opens when the user
    // clicks the rail's Changes button (toggleChangesPopover).
    p.setAttribute('data-open', 'false');
    p.innerHTML = `
      <div class="popover-header">
        <span class="title">${svg('compare', 13)} Changes <span class="changes-meta tiny muted"></span></span>
        <button class="close-btn" data-close="changes" type="button" title="Close" aria-label="Close">${svg('close', 14)}</button>
      </div>
      <div class="popover-body" id="shellChangesBody">
        <div class="changes-empty">No changes recorded yet.</div>
      </div>
      <div class="popover-footer">
        <button class="shell-btn" id="shellChangesCompare" type="button">${svg('sparkle', 11)} Compare baseline</button>
        <button class="shell-btn shell-btn-danger" id="shellChangesClear" type="button">${svg('trash', 11)} Clear all changes</button>
      </div>
    `;
    return p;
  }

  function buildHistoryPopover() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-history';
    p.id = 'shellHistory';
    p.innerHTML = `
      <div class="popover-header">
        <span class="title">${svg('history', 13)} History stack</span>
        <span style="display:flex;gap:6px;align-items:center;">
          <button class="shell-btn" id="shellHistoryClear" type="button" title="Clear all">Clear all</button>
          <button class="close-btn" data-close="history" type="button" title="Close" aria-label="Close">${svg('close', 14)}</button>
        </span>
      </div>
      <div class="popover-body" id="shellHistoryBody">
        <div class="history-empty">No changes yet.</div>
      </div>
    `;
    return p;
  }

  function buildAISidebar() {
    const a = document.createElement('aside');
    a.className = 'shell-ai';
    a.id = 'shellAI';
    a.innerHTML = `
      <div class="ai-header">
        <div class="title"><span class="glow"></span> Ask the map</div>
        <button class="close-btn" data-close="ai" type="button" style="background:transparent;border:0;color:var(--ink-3);cursor:pointer;width:24px;height:24px;border-radius:4px;">${svg('close', 14)}</button>
      </div>
      <div class="ai-body" id="shellAIBody">
        <div class="ai-msg">
          I can load cities, change modes, compute fairness, or explain what you see on the map.
          Try one of these:
        </div>
        <div class="ai-suggest" id="shellAISuggest">
          <button type="button" data-prompt="Fairness for hospitals on cycling distance">Fairness for hospitals on cycling distance</button>
          <button type="button" data-prompt="Fairness for veterinary with weight 1 and university with weight 10">Fairness for veterinary w=1, university w=10</button>
          <button type="button" data-prompt="Show fairness for all">Show fairness for all POIs</button>
          <button type="button" data-prompt="Show districts mode and the statistic for Araby">Districts mode · stats for Araby</button>
          <button type="button" data-prompt="Switch to walking distance and recompute">Switch to walking distance</button>
          <button type="button" data-prompt="Where should I add 5 grocery stores to improve fairness?">Where to add 5 grocery stores?</button>
        </div>
      </div>
      <div class="ai-input-wrap">
        <textarea class="ai-input" id="shellAIInput" placeholder="Ask anything about fairness, scale, modes, or sites…"></textarea>
        <button class="shell-btn shell-btn-primary" id="shellAISend" type="button">${svg('send', 12)} Send</button>
      </div>
    `;
    return a;
  }

  // ---------------- JS-positioned tooltip ----------------
  let _tip = null;
  function getTip() {
    if (_tip) return _tip;
    _tip = document.createElement('div');
    _tip.className = 'fave-tip';
    _tip.setAttribute('aria-hidden', 'true');
    document.body.appendChild(_tip);
    return _tip;
  }
  function showTip(el) {
    const text = el.getAttribute('data-tooltip');
    if (!text) return;
    const t = getTip();
    t.textContent = text;
    // Force a layout pass so we can read its size before positioning.
    t.style.left = '0px';
    t.style.top = '0px';
    t.setAttribute('data-shown', 'true');
    const tipRect = t.getBoundingClientRect();
    const r = el.getBoundingClientRect();
    const pos = el.getAttribute('data-tooltip-pos') || 'right';
    let left, top;
    if (pos === 'below') {
      left = r.left + r.width / 2 - tipRect.width / 2;
      top = r.bottom + 6;
    } else if (pos === 'above') {
      left = r.left + r.width / 2 - tipRect.width / 2;
      top = r.top - tipRect.height - 6;
    } else if (pos === 'left') {
      left = r.left - tipRect.width - 8;
      top = r.top + r.height / 2 - tipRect.height / 2;
    } else { // right (default — used for rail buttons)
      left = r.right + 8;
      top = r.top + r.height / 2 - tipRect.height / 2;
    }
    // Keep on-screen
    left = Math.max(4, Math.min(left, window.innerWidth - tipRect.width - 4));
    top = Math.max(4, Math.min(top, window.innerHeight - tipRect.height - 4));
    t.style.left = `${left}px`;
    t.style.top = `${top}px`;
  }
  function hideTip() {
    if (_tip) _tip.setAttribute('data-shown', 'false');
  }
  function wireTooltipEvents() {
    document.addEventListener('mouseover', (e) => {
      const el = e.target.closest && e.target.closest('[data-tooltip]');
      if (el) showTip(el);
    });
    document.addEventListener('mouseout', (e) => {
      const el = e.target.closest && e.target.closest('[data-tooltip]');
      if (el) hideTip();
    });
    document.addEventListener('focusin', (e) => {
      if (e.target.matches && e.target.matches('[data-tooltip]')) showTip(e.target);
    });
    document.addEventListener('focusout', (e) => {
      if (e.target.matches && e.target.matches('[data-tooltip]')) hideTip();
    });
  }

  // ---------------- helpers ----------------
  function clickIfExists(id) {
    const el = document.getElementById(id);
    if (el) el.click();
  }
  function clickAtSelector(sel) {
    const el = document.querySelector(sel);
    if (el) el.click();
  }
  function isDROpen() {
    const el = document.getElementById('drOffcanvas');
    return el && el.classList.contains('show');
  }
  function isParallelOpen() {
    const el = document.getElementById('parallelCoordsPanel');
    return el && !el.classList.contains('d-none');
  }
  function closeDR() {
    const el = document.getElementById('drOffcanvas');
    if (!el || typeof bootstrap === 'undefined') return;
    if (isDROpen()) bootstrap.Offcanvas.getOrCreateInstance(el).hide();
  }
  function closeParallel() {
    if (isParallelOpen()) clickIfExists('parallelCoordsBtn');  // legacy toggle
  }
  function openDRDrawer() {
    // Allow Parallel and DR to coexist as side-by-side panes in the bottom
    // drawer (matches the design's AnalyticsDrawer). Old behavior closed
    // Parallel before opening DR; new behavior leaves both visible.
    const el = document.getElementById('drOffcanvas');
    if (!el || typeof bootstrap === 'undefined') return clickIfExists('openDRBtn');
    bootstrap.Offcanvas.getOrCreateInstance(el).toggle();
  }
  function openParallelDrawer() {
    clickIfExists('parallelCoordsBtn');
  }
  function toggleInspector() {
    const insp = document.getElementById('inspector');
    if (!insp) return;
    const open = insp.getAttribute('data-open') === 'true';
    // Inspector and Ask-the-map are mutually exclusive: both occupy the
    // right side of the screen, so opening one closes the other.
    if (!open) {
      const ai = document.getElementById('shellAI');
      if (ai && ai.getAttribute('data-open') === 'true') toggleAISidebar();
    }
    insp.setAttribute('data-open', open ? 'false' : 'true');
    syncInspectorBody();
  }
  function syncInspectorBody() {
    const insp = document.getElementById('inspector');
    if (!insp) return;
    const open = insp.getAttribute('data-open') === 'true';
    document.body.setAttribute('data-inspector-open', open ? 'true' : 'false');
    const railBtn = document.getElementById('rail-inspector');
    if (railBtn) railBtn.setAttribute('data-active', open ? 'true' : 'false');
    scheduleMapResize();
  }

  // Tell MapLibre + deck.gl that the canvas size changed. The CSS transition
  // for the inspector/AI sidebar is ~250 ms so we resize once at frame 1
  // (so deck rebuilds its canvas) and again after the transition settles.
  let _resizeRAF = null;
  function doResize() {
    try { if (typeof map !== 'undefined' && map?.resize) map.resize(); } catch (_) {}
    try { window.dispatchEvent(new Event('resize')); } catch (_) {}
    try { if (typeof overlay !== 'undefined' && overlay?.deck?.redraw) overlay.deck.redraw(true); } catch (_) {}
    try { if (typeof updateLayers === 'function') updateLayers(); } catch (_) {}
  }
  function scheduleMapResize() {
    if (_resizeRAF) cancelAnimationFrame(_resizeRAF);
    _resizeRAF = requestAnimationFrame(() => {
      _resizeRAF = null;
      doResize();
      // Run again at the start, middle, and end of the inspector/AI slide
      // transition (~250 ms) so the deck.gl canvas always tracks the live
      // container size — single resize wasn't enough on Firefox.
      setTimeout(doResize, 100);
      setTimeout(doResize, 360);
      // (Previously fitToData(baseCityFC) ran here so the city's right
      // edge wasn't clipped under the inspector. Removed: the user wants
      // the camera to stay where they put it across inspector toggles
      // and hover-driven inspector updates.)
    });
  }

  function syncScaleActiveStates() {
    const macro = document.getElementById('rail-macro');
    const meso = document.getElementById('rail-meso');
    const micro = document.getElementById('rail-micro');
    const inDistrict = (typeof districtView !== 'undefined' && districtView);
    const inMezo = (typeof mezoView !== 'undefined' && mezoView);
    if (macro) macro.setAttribute('data-active', inDistrict && !inMezo ? 'true' : 'false');
    if (meso)  meso.setAttribute('data-active',  inMezo ? 'true' : 'false');
    if (micro) micro.setAttribute('data-active', !inDistrict && !inMezo ? 'true' : 'false');
  }

  // Reflect "POIs & weights has a non-empty selection" on the rail-poi
  // button so it lights up orange like the SCALE buttons. Reads the legacy
  // .poi-check checkboxes (the source of truth that drives selectedPOIMix).
  // Called from change handlers, from closeAllPopovers, and exposed on
  // window so history/state restores can sync after they programmatically
  // toggle checkboxes (which doesn't fire 'change').
  function syncPOIRailActive() {
    const rail = document.getElementById('rail-poi');
    if (!rail) return;
    const anyChecked = document.querySelectorAll('.poi-check:checked').length > 0;
    rail.setAttribute('data-active', anyChecked ? 'true' : 'false');
  }
  window.syncPOIRailActive = syncPOIRailActive;

  // ---------------- popover toggling ----------------
  function closeEquityPanel() {
    const panel = document.getElementById('equityAnalysisPanel');
    if (!panel || panel.classList.contains('d-none')) return;
    panel.classList.add('d-none');
    const rail = document.getElementById('rail-equity');
    if (rail) rail.setAttribute('data-active', 'false');
  }

  function closeAllPopovers(except) {
    document.querySelectorAll('.shell-popover[data-open="true"]').forEach(p => {
      if (p !== except) {
        p.setAttribute('data-open', 'false');
        // Reset the corresponding rail button's active state so the accent
        // stripe disappears when the panel closes. EXCEPT rail-poi, whose
        // active state tracks "non-empty POI selection", not "popover open"
        // — that's resynced via syncPOIRailActive() below.
        const railIdByPanelId = {
          shellChanges: 'rail-changes',
          shellHistory: 'rail-history',
          shellWhatIf:  'rail-wif',
        };
        const railBtnId = railIdByPanelId[p.id];
        if (railBtnId) {
          const rail = document.getElementById(railBtnId);
          if (rail) rail.setAttribute('data-active', 'false');
        }
      }
    });
    // Equity analysis is a legacy panel (not a .shell-popover), so it
    // wasn't covered by the loop above. Close it too so the TOOLS group
    // (POIs / Equity / What-if / Changes) stays mutually exclusive.
    closeEquityPanel();
    syncPOIRailActive();
  }
  function positionPopoverNear(popover, anchor, opts = {}) {
    const r = anchor.getBoundingClientRect();
    if (opts.right) {
      // For rail buttons (anchored to the right of the rail).
      popover.style.left = `${r.right + 10}px`;
      popover.style.top = `${Math.max(60, r.top)}px`;
    } else {
      // For topbar buttons (anchored below).
      popover.style.left = `${r.left}px`;
      popover.style.top = `${r.bottom + 6}px`;
    }
  }
  function togglePOIPopover() {
    const pop = document.getElementById('shellPOIPopover');
    const anchor = document.getElementById('rail-poi');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      positionPopoverNear(pop, anchor, { right: true });
      pop.setAttribute('data-open', 'true');
      syncPOIPopoverFromLegacy();
    }
  }
  function toggleCityMenu() {
    const pop = document.getElementById('shellCityMenu');
    const anchor = document.getElementById('topCityBtn');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      positionPopoverNear(pop, anchor);
      pop.setAttribute('data-open', 'true');
      syncCityMenu();
    }
  }
  // Build a What-if popover by lifting the legacy .whatif-menu out of the
  // hidden navbar's Bootstrap dropdown and wrapping it in a free-standing
  // .shell-popover. The legacy form controls keep their IDs so all the
  // existing what-if JS handlers continue to fire.
  function ensureWhatIfPopover() {
    const existing = document.getElementById('shellWhatIf');
    if (existing) return existing;
    const menu = document.querySelector('.whatif-menu');
    if (!menu) return null;
    const wrap = document.createElement('div');
    wrap.className = 'shell-popover';
    wrap.id = 'shellWhatIf';
    wrap.style.width = '420px';
    wrap.style.maxHeight = 'calc(100vh - 100px)';
    const header = document.createElement('div');
    header.className = 'popover-header';
    header.innerHTML = `<span class="title">${svg('wand', 13)} What-if scenario</span>
      <button class="close-btn" data-close="wif" type="button" aria-label="Close">${svg('close', 14)}</button>`;
    wrap.appendChild(header);
    const body = document.createElement('div');
    body.className = 'popover-body';
    // Detach the legacy menu from its <div class="dropdown"> parent and stash
    // it inside the popover body. All event listeners on the form controls
    // travel with the elements.
    body.appendChild(menu);
    // Strip Bootstrap classes that would keep it hidden as a dropdown.
    menu.classList.remove('dropdown-menu', 'dropdown-menu-end', 'show');
    menu.style.position = 'static';
    menu.style.padding = '0';
    menu.style.boxShadow = 'none';
    menu.style.border = '0';
    menu.style.display = 'block';
    menu.style.width = '100%';
    menu.style.maxWidth = '100%';
    menu.style.minWidth = '0';
    menu.style.overflowX = 'hidden';
    body.style.overflowY = 'auto';
    body.style.maxHeight = 'calc(100vh - 160px)';
    wrap.appendChild(body);
    document.body.appendChild(wrap);
    wrap.querySelector('[data-close="wif"]').addEventListener('click', () => {
      wrap.setAttribute('data-open', 'false');
      const railBtn = document.getElementById('rail-wif');
      if (railBtn) railBtn.setAttribute('data-active', 'false');
    });
    return wrap;
  }

  function toggleWhatIfPopover() {
    const pop = ensureWhatIfPopover();
    const anchor = document.getElementById('rail-wif');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      positionPopoverNear(pop, anchor, { right: true });
      // The What-if form is tall (mode picker + new-POI + mock-building
      // controls). Pin it near the top so the bottom doesn't fall off the
      // viewport from a mid-rail anchor — the user shouldn't need to scroll
      // the popover to reach the action buttons.
      pop.style.top = '60px';
      pop.setAttribute('data-open', 'true');
    }
    const railBtn = document.getElementById('rail-wif');
    if (railBtn) railBtn.setAttribute('data-active', isOpen ? 'false' : 'true');
  }

  // Meso popover — built once on demand. Lists the same edge-size options
  // as the legacy navbar dropdown (`.mezo-edge-option`) plus a "Hide meso"
  // row, so the rail button covers both the toggle and the size selection.
  function ensureMesoPopover() {
    const existing = document.getElementById('shellMeso');
    if (existing) return existing;
    const wrap = document.createElement('div');
    wrap.className = 'shell-popover';
    wrap.id = 'shellMeso';
    wrap.style.width = '200px';
    const header = document.createElement('div');
    header.className = 'popover-header';
    header.innerHTML = `<span class="title">${svg('hexagons', 13)} Meso · Hex size</span>
      <button class="close-btn" data-close="meso" type="button" aria-label="Close">${svg('close', 14)}</button>`;
    wrap.appendChild(header);
    const body = document.createElement('div');
    body.className = 'popover-body';
    // H3 only exposes a few discrete edge lengths in the city-scale band:
    // res 7 ≈ 1.22 km, res 8 ≈ 0.46 km, res 9 ≈ 0.17 km. 0.5 km and 0.75 km
    // both round to res 8 and rendered identically — drop 0.75 from the
    // picker so the three options map to three distinct hex sizes.
    const sizes = [
      { value: '0.25', label: '0.25 km' },
      { value: '0.5',  label: '0.5 km'  },
      { value: '1',    label: '1 km'    },
    ];
    body.innerHTML = `
      <div class="meso-size-list">
        ${sizes.map(s => `<button type="button" class="meso-size-row" data-value="${s.value}">${s.label}</button>`).join('')}
      </div>
    `;
    wrap.appendChild(body);
    document.body.appendChild(wrap);

    // Highlight a size only once meso is actually on. Before the user picks
    // anything we leave every row neutral so the default value (0.25 km)
    // doesn't masquerade as "user's choice".
    const refreshActive = () => {
      const isMezoOn = (typeof mezoView !== 'undefined' && mezoView);
      const cur = isMezoOn && typeof selectedMezoHexEdgeKm !== 'undefined'
        ? String(selectedMezoHexEdgeKm) : '';
      wrap.querySelectorAll('.meso-size-row').forEach(b => {
        b.setAttribute('data-active', cur && String(b.dataset.value) === cur ? 'true' : 'false');
      });
    };
    refreshActive();

    wrap.querySelectorAll('.meso-size-row').forEach(btn => {
      btn.addEventListener('click', () => {
        const val = btn.dataset.value;
        // Drive the legacy `.mezo-edge-option` so the existing handler runs
        // (setMezoHexEdgeKm + setMezoView(true) + recompute) — single source
        // of truth for the meso size pipeline.
        const legacy = document.querySelector(`.mezo-edge-option[data-value="${val}"]`);
        if (legacy) legacy.click();
        refreshActive();
      });
    });
    wrap.querySelector('[data-close="meso"]').addEventListener('click', () => {
      wrap.setAttribute('data-open', 'false');
    });
    // Expose the refresher so toggleMesoPopover can re-sync without
    // duplicating the highlight logic.
    wrap.__refreshActive = refreshActive;
    return wrap;
  }

  function toggleMesoPopover() {
    const pop = ensureMesoPopover();
    const anchor = document.getElementById('rail-meso');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      // First time selecting meso: default to 0.25 km and apply it to the
      // map immediately, so the user sees hexagons without a second click.
      // Delegates to the popover's own row click → legacy .mezo-edge-option
      // → setMezoHexEdgeKm + setMezoView(true) → refreshMezoScores +
      // updateLayers. If meso is already on we leave the current size alone.
      if (typeof mezoView !== 'undefined' && !mezoView) {
        const defaultRow = pop.querySelector('.meso-size-row[data-value="0.25"]');
        if (defaultRow) defaultRow.click();
      }
      pop.__refreshActive?.();
      positionPopoverNear(pop, anchor, { right: true });
      pop.setAttribute('data-open', 'true');
    }
  }

  // Equity Analysis lives in the legacy panel (#equityAnalysisPanel toggled
  // via inline onclick). Forward the click to that handler, then mirror the
  // panel's visibility back onto the rail button so the orange accent treats
  // "panel open" as active state. Opening it closes all popovers so the
  // TOOLS group stays mutually exclusive (Equity / POIs / What-if /
  // Changes can't be on at the same time).
  function toggleEquityAnalysis() {
    const panel = document.getElementById('equityAnalysisPanel');
    if (!panel) return;
    const willOpen = panel.classList.contains('d-none');
    if (willOpen) closeAllPopovers();
    panel.classList.toggle('d-none');
    const rail = document.getElementById('rail-equity');
    if (rail) {
      rail.setAttribute('data-active', panel.classList.contains('d-none') ? 'false' : 'true');
    }
  }

  function ensureHelpModal() {
    const existing = document.getElementById('shellHelpModal');
    if (existing) return existing;
    const wrap = document.createElement('div');
    wrap.id = 'shellHelpModal';
    wrap.className = 'shell-help-modal';
    wrap.setAttribute('data-open', 'false');
    wrap.innerHTML = `
      <div class="shell-help-backdrop" data-close-help></div>
      <div class="shell-help-card" role="dialog" aria-modal="true" aria-labelledby="shellHelpTitle">
        <div class="shell-help-head">
          <div class="shell-help-brand">
            <span class="brand-mark" aria-hidden="true">${svg('logo', 16)}</span>
            <div>
              <div class="shell-help-title" id="shellHelpTitle">FAVE — Fairness, Accessibility &amp; Visualization Explorer</div>
              <div class="shell-help-sub">Quick guide to the interface</div>
            </div>
          </div>
          <button class="close-btn" type="button" data-close-help aria-label="Close">${svg('close', 14)}</button>
        </div>
        <div class="shell-help-body">
          <section class="help-callout">
            <div class="help-callout-icon">${svg('logo', 22)}</div>
            <div>
              <strong>FAVE</strong> measures and visualises how fairly a city distributes access to amenities (POIs). Every building gets a 0–1 fairness score per category and a combined overall score; the map colours buildings, hex cells, or districts on that ramp.
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('sparkle', 13)}</span> Top metric strip</h4>
            <div class="help-grid">
              <div class="help-card">
                <div class="help-card-head"><span class="dir-up">↑</span> Overall</div>
                <p>Mean per-building fairness (0–1). Higher = more fair on average.</p>
              </div>
              <div class="help-card">
                <div class="help-card-head"><span class="dir-down">↓</span> Gini</div>
                <p>Inequality of fairness across the city. Lower = more evenly distributed.</p>
              </div>
              <div class="help-card">
                <div class="help-card-head">POIs</div>
                <p>Total points-of-interest in the active categories.</p>
              </div>
              <div class="help-card">
                <div class="help-card-head">Districts · Cells · Buildings</div>
                <p>Count of units at the active scale (Macro / Meso / Micro).</p>
              </div>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('hexagons', 13)}</span> Scale · left rail</h4>
            <div class="help-grid help-grid-3">
              <div class="help-card help-card-row">
                <span class="help-icon-tile">${svg('district', 14)}</span>
                <div><strong>Macro</strong><p>RegSO districts.</p></div>
              </div>
              <div class="help-card help-card-row">
                <span class="help-icon-tile">${svg('hexagons', 14)}</span>
                <div><strong>Meso</strong><p>Hex grid. Pick edge size 0.25 / 0.5 / 0.75 / 1 km.</p></div>
              </div>
              <div class="help-card help-card-row">
                <span class="help-icon-tile">${svg('building', 14)}</span>
                <div><strong>Micro</strong><p>Individual buildings (default).</p></div>
              </div>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('layers', 13)}</span> POIs &amp; Weights</h4>
            <p>Open the layers popover, tick the categories you care about and weight each 1–10. The map recolours immediately. Flip <em>Symbols</em> to show or hide POI pins.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('walk', 13)}</span> Travel mode · bottom-centre</h4>
            <p>Drives the time → fairness conversion. The selected mode shows the orange-accent box.</p>
            <div class="help-mode-row">
              <span class="help-mode help-mode-active">${svg('walk', 13)} Walk</span>
              <span class="help-mode">${svg('bike', 13)} Cycle</span>
              <span class="help-mode">${svg('car', 13)} Car</span>
              <span class="help-mode">${svg('bus', 13)} Transit</span>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('compare', 13)}</span> Model · top bar</h4>
            <div class="help-grid">
              <div class="help-card"><div class="help-card-head">Gravity</div><p>IF-City utility model — closer POIs and crowding both matter.</p></div>
              <div class="help-card"><div class="help-card-head">Distance</div><p>Straight nearest-POI travel time.</p></div>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('wand', 13)}</span> What-if &amp; Changes</h4>
            <p>Add or change buildings to test scenarios. The Changes panel logs each tweak so you can compare before-vs-after Gini and overall fairness.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('sliders', 13)}</span> Inspector · right</h4>
            <p>Click a building, hex, or district to see its score, per-category breakdown, and population/income context. Toggle from the rail's slider icon.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('scatter', 13)}</span> DR Explorer &amp; Parallel Coords</h4>
            <p>Bottom drawers for projection-based exploration. Lasso a cluster on the DR scatter to drive the EBM explanation, or drag PC axes to filter buildings by raw features.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('sparkle', 13)}</span> Ask the map · AI</h4>
            <p>Open the AI panel and type a natural-language request. Try one of these:</p>
            <ul class="help-examples">
              <li><code>Fairness for hospitals on cycling distance</code></li>
              <li><code>Fairness for veterinary with weight 1 and university with weight 10</code></li>
              <li><code>Show fairness for all</code></li>
              <li><code>Show districts mode and the statistic for Araby</code></li>
              <li><code>Where should I add 5 grocery stores to improve fairness?</code></li>
            </ul>
          </section>

          <section class="shell-help-foot">
            <div>FAVE is research software developed at Linnaeus University.</div>
            <div>Tile data © OpenStreetMap contributors · Esri.</div>
          </section>
        </div>
      </div>
    `;
    document.body.appendChild(wrap);
    wrap.querySelectorAll('[data-close-help]').forEach(el => {
      el.addEventListener('click', () => wrap.setAttribute('data-open', 'false'));
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && wrap.getAttribute('data-open') === 'true') {
        wrap.setAttribute('data-open', 'false');
      }
    });
    return wrap;
  }

  function toggleHelpModal() {
    const m = ensureHelpModal();
    const open = m.getAttribute('data-open') === 'true';
    m.setAttribute('data-open', open ? 'false' : 'true');
  }

  function toggleChangesPopover() {
    const pop = document.getElementById('shellChanges');
    const anchor = document.getElementById('rail-changes');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      // Anchor the panel to the right of the map, just under the topbar.
      // This keeps it out of the way of the bottom-left legend, the
      // bottom-center travel mode strip, and the metric strip — and it
      // tucks against the inspector when the inspector is open instead
      // of overlapping the map's main viewport.
      pop.style.top = '68px';
      pop.style.bottom = 'auto';
      pop.style.left = 'auto';
      pop.style.right = (document.body.getAttribute('data-inspector-open') === 'true')
        ? `calc(var(--inspector-w, 320px) + 16px)`
        : '16px';
      pop.setAttribute('data-open', 'true');
      renderChangesItems();
    } else {
      pop.setAttribute('data-open', 'false');
    }
    const railBtn = document.getElementById('rail-changes');
    if (railBtn) railBtn.setAttribute('data-active', isOpen ? 'false' : 'true');
  }

  function renderChangesItems() {
    const body = document.getElementById('shellChangesBody');
    const meta = document.querySelector('#shellChanges .changes-meta');
    if (!body) return;
    const log = (typeof whatIfChangeLog !== 'undefined' && Array.isArray(whatIfChangeLog)) ? whatIfChangeLog : [];
    const cityName = (typeof lastCityName === 'string' && lastCityName) ? lastCityName.toUpperCase() : '—';
    const sourceName = (typeof sourceMode === 'string' && sourceMode) ? sourceMode.toUpperCase() : 'OSM';
    if (meta) meta.textContent = `· ${cityName} · ${sourceName}`;

    if (!log.length) {
      body.innerHTML = '<div class="changes-empty">No changes recorded yet.</div>';
      return;
    }
    // Newest first.
    const items = [...log].reverse().map(rec => {
      const label = rec.description || rec.label || `Change #${rec.id}`;
      const sub = [
        rec.category ? prettyCatLabel(rec.category) : '',
        Number.isFinite(rec.beforeGini) && Number.isFinite(rec.afterGini)
          ? `Gini ${rec.beforeGini.toFixed(3)} → ${rec.afterGini.toFixed(3)}` : ''
      ].filter(Boolean).join(' · ');
      const t = rec.timestamp instanceof Date ? rec.timestamp : (rec.timestamp ? new Date(rec.timestamp) : null);
      const time = t ? t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
      const delta = Number.isFinite(rec.giniDelta) ? rec.giniDelta : null;
      const dir = delta == null ? 'zero' : delta > 0 ? 'up' : delta < 0 ? 'down' : 'zero';
      const deltaText = delta != null ? `${delta > 0 ? '+' : ''}${delta.toFixed(3)}` : '±—';
      const isPinned = (typeof pinnedChangeId !== 'undefined' && rec.id === pinnedChangeId);
      const hasFeatures = (rec.colorPairs?.length > 0) || rec.highlightCenter;
      return `
        <div class="change-row" data-id="${rec.id}" data-active="${isPinned ? 'true' : 'false'}">
          <div class="change-main">
            <div class="change-label">${escapeHTML(label)}</div>
            ${sub ? `<div class="change-sub tiny muted">${escapeHTML(sub)}</div>` : ''}
          </div>
          <div class="change-meta">
            ${hasFeatures ? `<button class="btn-toggle-vis" data-on="${isPinned ? 'true' : 'false'}" data-id="${rec.id}" type="button">
              ${isPinned ? svg('eyeOff', 11) + ' Hide' : svg('eye', 11) + ' Show'}
            </button>` : ''}
            <span class="delta-pill" data-dir="${dir}">${deltaText}</span>
            <span class="tiny muted change-time">${time}</span>
          </div>
        </div>`;
    }).join('');
    body.innerHTML = items;

    // Wire Show/Hide toggles to the existing highlightChangeOnMap function.
    body.querySelectorAll('.btn-toggle-vis').forEach(btn => {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        const id = Number(btn.dataset.id);
        const rec = log.find(r => r.id === id);
        if (rec && typeof highlightChangeOnMap === 'function') {
          highlightChangeOnMap(rec);
          // Re-render so the pin state and button labels stay in sync.
          renderChangesItems();
        }
      });
    });
  }

  function prettyCatLabel(cat) {
    return (typeof prettyPOIName === 'function')
      ? prettyPOIName(cat)
      : String(cat).replace(/_/g, ' ');
  }

  function wireChangesPopover() {
    const pop = document.getElementById('shellChanges');
    if (!pop) return;
    pop.querySelector('[data-close="changes"]')?.addEventListener('click', () => {
      pop.setAttribute('data-open', 'false');
      const railBtn = document.getElementById('rail-changes');
      if (railBtn) railBtn.setAttribute('data-active', 'false');
    });
    pop.querySelector('#shellChangesClear')?.addEventListener('click', () => {
      const legacy = document.getElementById('changeLogClearBtn');
      if (legacy) legacy.click();
      else if (typeof whatIfChangeLog !== 'undefined' && Array.isArray(whatIfChangeLog)) {
        whatIfChangeLog.length = 0;
        if (typeof updateChangeLogUI === 'function') updateChangeLogUI();
      }
      // Drop pinned highlight if any.
      if (typeof pinnedChangeId !== 'undefined' && pinnedChangeId != null) {
        try { pinnedChangeId = null; if (typeof updateLayers === 'function') updateLayers(); } catch (_) {}
      }
      renderChangesItems();
    });
    pop.querySelector('#shellChangesCompare')?.addEventListener('click', () => {
      // Toggle the "compare-baseline" mode if the legacy state is exposed.
      try {
        if (typeof changeCompareBaseline !== 'undefined') {
          changeCompareBaseline = !changeCompareBaseline;
          if (typeof updateLayers === 'function') updateLayers();
        }
      } catch (_) {}
      renderChangesItems();
    });
  }

  // Update both the History and Changes popovers when the legacy log mutates.
  // updateChangeLogUI is called whenever an entry is added/removed/toggled.
  function patchChangeLogUI() {
    if (typeof updateChangeLogUI !== 'function' || updateChangeLogUI.__shellPatched) return;
    const original = updateChangeLogUI;
    const patched = function (...args) {
      const result = original.apply(this, args);
      try { renderChangesItems(); } catch (_) {}
      try { renderHistoryItems(); } catch (_) {}
      return result;
    };
    patched.__shellPatched = true;
    // Replace the global so all callers go through the patched version.
    try { window.updateChangeLogUI = patched; } catch (_) {}
    try { globalThis.updateChangeLogUI = patched; } catch (_) {}
  }

  function toggleHistoryPopover() {
    const pop = document.getElementById('shellHistory');
    if (!pop) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      // Anchor history popover bottom-right (matches the design's HistoryStack
      // popover, and stays out of the way of the rail/inspector).
      pop.style.left = 'auto';
      pop.style.top = 'auto';
      pop.style.right = (document.body.getAttribute('data-inspector-open') === 'true')
        ? 'calc(var(--inspector-w, 320px) + 16px)'
        : '16px';
      pop.style.bottom = '16px';
      pop.setAttribute('data-open', 'true');
      renderHistoryItems();
    }
    const railBtn = document.getElementById('rail-history');
    if (railBtn) railBtn.setAttribute('data-active', isOpen ? 'false' : 'true');
  }

  function renderHistoryItems() {
    const body = document.getElementById('shellHistoryBody');
    if (!body) return;
    const log = (typeof whatIfChangeLog !== 'undefined' && Array.isArray(whatIfChangeLog)) ? whatIfChangeLog : [];
    if (!log.length) {
      body.innerHTML = '<div class="history-empty">No changes yet. Try toggling POI categories, switching scale, or adding a what-if scenario.</div>';
      return;
    }
    // Newest first.
    const items = [...log].reverse().map((rec, i) => {
      const num = log.length - i;
      const label = rec.description || rec.label || `Change #${num}`;
      const t = rec.timestamp instanceof Date ? rec.timestamp : (rec.timestamp ? new Date(rec.timestamp) : null);
      const time = t ? t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
      const delta = Number.isFinite(rec.giniDelta) ? rec.giniDelta : null;
      const dir = delta == null ? 'zero' : delta > 0 ? 'up' : delta < 0 ? 'down' : 'zero';
      const deltaText = delta != null ? `${delta > 0 ? '+' : ''}${delta.toFixed(3)}` : '';
      const isActive = (typeof pinnedChangeId !== 'undefined' && rec.id === pinnedChangeId);
      return `
        <div class="history-item" data-id="${rec.id || num}" data-active="${isActive ? 'true' : 'false'}">
          <span class="num">${num}</span>
          <span class="label">${escapeHTML(label)}${deltaText ? `<span class="delta-pill" data-dir="${dir}">${deltaText}</span>` : ''}</span>
          <span class="time">${time}</span>
        </div>`;
    }).join('');
    body.innerHTML = items;
  }

  function escapeHTML(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function wireHistoryPopover() {
    const pop = document.getElementById('shellHistory');
    if (!pop) return;
    pop.querySelector('[data-close="history"]')?.addEventListener('click', () => {
      pop.setAttribute('data-open', 'false');
      const railBtn = document.getElementById('rail-history');
      if (railBtn) railBtn.setAttribute('data-active', 'false');
    });
    pop.querySelector('#shellHistoryClear')?.addEventListener('click', () => {
      const legacy = document.getElementById('changeLogClearBtn');
      if (legacy) legacy.click();
      // If the legacy button isn't reachable (it lives in the hidden navbar
      // dropdown), fall back to clearing the array directly.
      else if (typeof whatIfChangeLog !== 'undefined' && Array.isArray(whatIfChangeLog)) {
        whatIfChangeLog.length = 0;
        if (typeof updateChangeLogUI === 'function') updateChangeLogUI();
      }
      renderHistoryItems();
    });
  }

  function toggleAISidebar() {
    const a = document.getElementById('shellAI');
    if (!a) return;
    const open = a.getAttribute('data-open') === 'true';
    // AI sidebar and Inspector are mutually exclusive (both occupy the
    // right edge). Closing the inspector first prevents both panels from
    // overlapping each other.
    if (!open) {
      const insp = document.getElementById('inspector');
      if (insp && insp.getAttribute('data-open') === 'true') toggleInspector();
    }
    a.setAttribute('data-open', open ? 'false' : 'true');
    document.body.setAttribute('data-ai-open', open ? 'false' : 'true');
    const railBtn = document.getElementById('rail-ai');
    if (railBtn) railBtn.setAttribute('data-active', open ? 'false' : 'true');
    scheduleMapResize();
  }

  // ---------------- city menu wiring ----------------
  function syncCityMenu() {
    const legacyCity = document.getElementById('citySelect');
    const current = legacyCity?.value || 'vaxjo';
    document.querySelectorAll('#shellCityMenu .city-row').forEach(b => {
      b.setAttribute('data-active', b.dataset.key === current ? 'true' : 'false');
    });
    const lbl = document.getElementById('topCityLabel');
    if (lbl) {
      const c = CITIES.find(x => x.key === current);
      if (c) lbl.textContent = c.label;
    }
  }
  function pickCity(key) {
    const legacyCity = document.getElementById('citySelect');
    if (!legacyCity) return;
    legacyCity.value = key;
    legacyCity.dispatchEvent(new Event('change', { bubbles: true }));
    syncCityMenu();
    document.getElementById('shellCityMenu').setAttribute('data-open', 'false');
  }
  function syncJobStatus() {
    const legacy = document.getElementById('jobStatus');
    const el = document.getElementById('topJobStatus');
    if (legacy && el) el.textContent = legacy.textContent;
  }

  // ---------------- POI popover wiring ----------------
  function syncPOIPopoverFromLegacy() {
    POI_META.forEach(m => {
      const legacy = document.getElementById('poi_' + m.id);
      const shellChk = document.querySelector(`#shellPOIPopover .shell-poi-check[data-cat="${m.id}"]`);
      const shellRng = document.querySelector(`#shellPOIPopover .shell-poi-weight[data-cat="${m.id}"]`);
      const shellNum = document.querySelector(`#shellPOIPopover .shell-poi-numb[data-cat="${m.id}"]`);
      const shellRow = document.querySelector(`#shellPOIPopover .poi-row[data-cat="${m.id}"]`);
      if (!legacy || !shellChk) return;
      shellChk.checked = legacy.checked;
      if (shellRng) shellRng.disabled = !legacy.checked;
      if (shellRow) shellRow.setAttribute('data-checked', legacy.checked ? 'true' : 'false');
      const lwEl = document.querySelector(`.poi-weight[data-cat="${m.id}"]`);
      if (lwEl && shellRng) shellRng.value = lwEl.value;
      if (lwEl && shellNum) shellNum.textContent = lwEl.value;
    });
    const sym = document.getElementById('poiSymbolsToggle');
    const ssym = document.getElementById('shellPOISymbolsToggle');
    if (sym && ssym) ssym.checked = sym.checked;
  }
  function wirePOIPopover() {
    const pop = document.getElementById('shellPOIPopover');
    if (!pop) return;
    pop.querySelectorAll('.shell-poi-check').forEach(chk => {
      chk.addEventListener('change', () => {
        const cat = chk.dataset.cat;
        const legacy = document.getElementById('poi_' + cat);
        if (legacy) {
          legacy.checked = chk.checked;
          legacy.dispatchEvent(new Event('change', { bubbles: true }));
        }
        const shellRng = document.querySelector(`#shellPOIPopover .shell-poi-weight[data-cat="${cat}"]`);
        const shellRow = document.querySelector(`#shellPOIPopover .poi-row[data-cat="${cat}"]`);
        if (shellRng) shellRng.disabled = !chk.checked;
        if (shellRow) shellRow.setAttribute('data-checked', chk.checked ? 'true' : 'false');
      });
    });
    pop.querySelectorAll('.shell-poi-weight').forEach(rng => {
      rng.addEventListener('input', () => {
        const cat = rng.dataset.cat;
        const legacy = document.querySelector(`.poi-weight[data-cat="${cat}"]`);
        if (legacy) {
          legacy.value = rng.value;
          legacy.dispatchEvent(new Event('input', { bubbles: true }));
          legacy.dispatchEvent(new Event('change', { bubbles: true }));
        }
        const num = document.querySelector(`#shellPOIPopover .shell-poi-numb[data-cat="${cat}"]`);
        if (num) num.textContent = rng.value;
      });
    });
    pop.querySelector('#shellPOIClear')?.addEventListener('click', () => {
      const legacy = document.getElementById('poiClearBtn');
      if (legacy) legacy.click();
      setTimeout(syncPOIPopoverFromLegacy, 0);
    });
    pop.querySelector('#shellPOIAll')?.addEventListener('click', () => {
      POI_META.forEach(m => {
        const legacy = document.getElementById('poi_' + m.id);
        if (legacy && !legacy.checked) {
          legacy.checked = true;
          legacy.dispatchEvent(new Event('change', { bubbles: true }));
        }
      });
      setTimeout(syncPOIPopoverFromLegacy, 0);
    });
    pop.querySelector('#shellPOISymbolsToggle')?.addEventListener('change', e => {
      const sym = document.getElementById('poiSymbolsToggle');
      if (sym) {
        sym.checked = e.target.checked;
        sym.dispatchEvent(new Event('change', { bubbles: true }));
      }
    });
    pop.querySelector('[data-close="poi"]')?.addEventListener('click', () => pop.setAttribute('data-open', 'false'));
  }

  // ---------------- AI sidebar wiring ----------------
  function wireAISidebar() {
    const a = document.getElementById('shellAI');
    if (!a) return;
    a.querySelector('[data-close="ai"]')?.addEventListener('click', toggleAISidebar);
    const send = a.querySelector('#shellAISend');
    const input = a.querySelector('#shellAIInput');
    const body = a.querySelector('#shellAIBody');
    function appendMsg(text, who) {
      const div = document.createElement('div');
      div.className = 'ai-msg' + (who === 'user' ? ' user' : '');
      div.textContent = text;
      body.appendChild(div);
      body.scrollTop = body.scrollHeight;
    }
    async function fire() {
      const txt = (input.value || '').trim();
      if (!txt) return;
      appendMsg(txt, 'user');
      input.value = '';
      // Run the real planner pipeline directly. callLLM/dispatchActions are
      // global functions declared in controllers/llm.js (classical script),
      // resolved here at call time. The earlier approach proxied to the legacy
      // #chatbotPanel, but its send button is type="button" with no <form>, so
      // neither the submit nor the fallback selector ever fired — the request
      // was never sent and the map never updated.
      if (typeof callLLM !== 'function' || typeof dispatchActions !== 'function') {
        appendMsg('Assistant is not available right now.', 'assistant');
        return;
      }
      const reply = document.createElement('div');
      reply.className = 'ai-msg';
      reply.textContent = 'Thinking…';
      body.appendChild(reply);
      body.scrollTop = body.scrollHeight;
      try {
        const plan = await callLLM(txt);
        const note = await dispatchActions(plan);
        reply.textContent = note || 'Done.';
      } catch (e) {
        console.error('Ask the map failed', e);
        const base = (typeof API_BASE !== 'undefined' && API_BASE) ? API_BASE : 'the API';
        const hint = (e instanceof TypeError || /Failed to fetch/i.test(e?.message || ''))
          ? ` Check that ${base} is running and CORS allows this origin.`
          : '';
        reply.textContent = `Sorry, something went wrong: ${e?.message || 'Unknown error.'}${hint}`;
      }
      body.scrollTop = body.scrollHeight;
    }
    send?.addEventListener('click', fire);
    input?.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); fire(); }
    });
    // Suggestion buttons populate the textarea and submit immediately.
    a.querySelectorAll('#shellAISuggest button[data-prompt]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (!input) return;
        input.value = btn.dataset.prompt || btn.textContent.trim();
        fire();
      });
    });
  }

  function wireTopbar() {
    document.getElementById('topCityBtn')?.addEventListener('click', toggleCityMenu);
    document.querySelectorAll('#shellCityMenu .city-row').forEach(btn => {
      btn.addEventListener('click', () => pickCity(btn.dataset.key));
    });
    document.getElementById('topThemeBtn')?.addEventListener('click', () => {
      const cur = document.body.getAttribute('data-theme') || 'light';
      const next = cur === 'light' ? 'dark' : 'light';
      document.body.setAttribute('data-theme', next);
      // Drive the legacy basemapStyleToggle (checked = dark) so the existing
      // change handler swaps the MapLibre style. Dispatching change fires the
      // listener installed in lib/state.js → setBasemapStyle().
      const bm = document.getElementById('basemapStyleToggle');
      if (bm) {
        // Always dispatch — setBasemapStyle is idempotent and we need it to
        // fire even if the checkbox's stored state already matches the
        // desired one (which can happen because the HTML default is
        // checked but the map starts on the light style).
        bm.checked = (next === 'dark');
        bm.dispatchEvent(new Event('change', { bubbles: true }));
      }
    });
    document.getElementById('topHelpBtn')?.addEventListener('click', toggleHelpModal);
    document.querySelectorAll('.fave-topbar .segmented [data-model]').forEach(b => {
      b.addEventListener('click', () => {
        document.querySelectorAll('.fave-topbar .segmented [data-model]').forEach(x => x.setAttribute('data-active', 'false'));
        b.setAttribute('data-active', 'true');
        const mode = b.dataset.model === 'distance' ? 'default' : 'ifcity';
        if (typeof setFairnessModel === 'function') {
          // recompute:true → recomputeFairnessAfterWhatIf runs the category
          // computation (when fairActive) and always calls autoComputeOverall.
          setFairnessModel(mode, { recompute: true });
        }
      });
    });
    syncCityMenu();
    syncJobStatus();
    new MutationObserver(syncJobStatus).observe(document.getElementById('jobStatus') || document.body, { childList: true, subtree: true, characterData: true });
  }

  function wireTravelMode(strip) {
    strip.querySelectorAll('button[data-mode]').forEach(btn => {
      btn.addEventListener('click', () => {
        const mode = btn.dataset.mode;
        strip.querySelectorAll('button[data-mode]').forEach(b => b.setAttribute('data-active', b.dataset.mode === mode ? 'true' : 'false'));
        // Drive the legacy state. Default recompute=true so per-building
        // fairness re-runs (recomputeFairnessAfterWhatIf) and the map
        // recolours. Also dispatch the legacy <select> change event so any
        // listeners on #fairnessTravelMode fire.
        if (typeof setFairnessTravelMode === 'function') {
          setFairnessTravelMode(mode);
        }
        const sel = document.getElementById('fairnessTravelMode');
        if (sel) {
          sel.value = mode;
          sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
        // Belt-and-braces: if fairness isn't active yet, refresh the
        // overall colouring so the map still reacts to the mode change.
        if (typeof autoComputeOverall === 'function') autoComputeOverall();
      });
    });
  }
  function wireZoom(zoom) {
    zoom.querySelector('#shellZoomIn')?.addEventListener('click', () => { if (typeof map !== 'undefined' && map?.zoomIn) map.zoomIn(); });
    zoom.querySelector('#shellZoomOut')?.addEventListener('click', () => { if (typeof map !== 'undefined' && map?.zoomOut) map.zoomOut(); });
  }

  // Floating "Clear selection" pill — visible whenever DR/PC/map have
  // an active selection. Polls for selection state since selection
  // mutations happen in many places (DR lasso, PC click, district click,
  // map lasso) and the legacy code does not emit a single event.
  function wireClearSelectionPill() {
    const pill = document.getElementById('clearSelectionPill');
    const count = document.getElementById('clearSelectionPillCount');
    if (!pill) return;
    pill.addEventListener('click', () => {
      // Mirror the existing Clear button in the DR toolbar — covers
      // persistent building selection + Parallel Coords selection + DR
      // lasso state. Then click the map-lasso Clear so the map overlay
      // also resets.
      try { if (typeof clearSelection === 'function') clearSelection(); } catch (_) {}
      try { if (typeof clearParallelCoordsSelectionFromClearAction === 'function') clearParallelCoordsSelectionFromClearAction(); } catch (_) {}
      try { if (typeof clearDRMapSelection === 'function') clearDRMapSelection(); } catch (_) {}
      try { if (typeof updateLayers === 'function') updateLayers(); } catch (_) {}
      const mapClear = document.getElementById('mapLassoClearBtn');
      if (mapClear && !mapClear.disabled) mapClear.click();
      try { window.faveInspector?.clearSelection?.(); } catch (_) {}
      pill.hidden = true;
    });
    // Cheap polling — selection-state surfaces vary across files; one
    // 400 ms tick is enough to keep the pill in sync without wiring
    // listeners into every selection mutator.
    setInterval(() => {
      let n = 0;
      try {
        if (typeof persistentBuildingSelection !== 'undefined' && persistentBuildingSelection?.size) {
          n = persistentBuildingSelection.size;
        } else if (typeof baseCityFC !== 'undefined' && Array.isArray(baseCityFC?.features)) {
          n = baseCityFC.features.reduce(
            (acc, f) => acc + (f?.properties?._drSelected ? 1 : 0), 0);
        }
        // Districts/cells also count as a selection when they're the
        // only thing flagged (e.g. macro click).
        if (n === 0 && typeof districtFC !== 'undefined') {
          const ds = (districtFC?.features || []).filter(
            f => f?.properties?._drSelected).length;
          if (ds) n = ds;
        }
        if (n === 0 && typeof mezoHexData !== 'undefined') {
          const ms = (mezoHexData || []).filter(
            c => c?._drSelected).length;
          if (ms) n = ms;
        }
      } catch (_) { n = 0; }
      pill.hidden = n === 0;
      if (count) count.textContent = n ? `· ${n.toLocaleString()}` : '';
    }, 400);
  }

  // Wire the new design's DR Explorer algorithm segmented buttons. They
  // mirror the hidden <select id="drAlgo"> so legacy drView.js (which
  // reads the select by ID) keeps working unchanged.
  function wireDRDesignToolbar() {
    const algoSelect = document.getElementById('drAlgo');
    const btns = document.querySelectorAll('.dr-algo-btn');
    if (!algoSelect || !btns.length) return;
    const setActive = (algo) => {
      btns.forEach(b => b.setAttribute('data-active', b.dataset.algo === algo ? 'true' : 'false'));
      const lbl = document.getElementById('drPlotLabel');
      if (lbl) lbl.textContent = `${algo.toUpperCase()} embedding`;
      // Pane header sub-label (e.g. "UMAP" / "PCA").
      const sub = document.getElementById('drPaneSub');
      if (sub) sub.textContent = algo.toUpperCase();
    };
    btns.forEach(btn => {
      btn.addEventListener('click', () => {
        const algo = btn.dataset.algo;
        if (!algo) return;
        algoSelect.value = algo;
        algoSelect.dispatchEvent(new Event('change', { bubbles: true }));
        setActive(algo);
      });
    });
    setActive(algoSelect.value || 'umap');
  }

  function watchInspector() {
    const insp = document.getElementById('inspector');
    if (!insp) return;
    new MutationObserver(syncInspectorBody).observe(insp, { attributes: true, attributeFilter: ['data-open'] });
    syncInspectorBody();
  }

  function syncDrawerBodyAttrs() {
    const dr = document.getElementById('drOffcanvas');
    const pc = document.getElementById('parallelCoordsPanel');
    const drOpen = !!(dr && dr.classList.contains('show'));
    const pcOpen = !!(pc && !pc.classList.contains('d-none'));
    document.body.setAttribute('data-dr-open', drOpen ? 'true' : 'false');
    document.body.setAttribute('data-pc-open', pcOpen ? 'true' : 'false');
    // When everything closes, also drop the collapsed state so reopening
    // doesn't leave the new drawer hidden behind the collapsed strip.
    if (!drOpen && !pcOpen) document.body.setAttribute('data-drawer-collapsed', 'false');
    // Mirror active state on rail buttons so the accent stripe lights up
    // for whichever views are currently visible in the drawer.
    const railDR = document.getElementById('rail-dr');
    if (railDR) railDR.setAttribute('data-active', drOpen ? 'true' : 'false');
    const railPC = document.getElementById('rail-parallel');
    if (railPC) railPC.setAttribute('data-active', pcOpen ? 'true' : 'false');
    // Layout: when DR is alone, use the design's default side-by-side grid
    // (.dr-explorer 1fr/380px). When DR + PC are both open, switch to the
    // vertical stacked layout (.dr-explorer-stacked) so the split-pane
    // narrow widths read better. Toggling the class via JS is more
    // reliable than relying on body[data-pc-open] descendant selectors,
    // which were not flipping the layout for some users.
    const drExp = document.querySelector('#drOffcanvas .dr-explorer');
    if (drExp) {
      drExp.classList.toggle('dr-explorer-stacked', pcOpen);
    }
    syncDrawerBar();
  }

  // --- Drawer resize / collapse polish ---------------------------------
  // Adds a thin draggable handle at the top edge of each bottom drawer so
  // users can resize the drawer height by dragging. Updates --drawer-h on
  // <html> so both #drOffcanvas and #parallelCoordsPanel stay in sync.
  function installDrawerHandle(drawerEl) {
    if (!drawerEl || drawerEl.querySelector('.shell-drawer-handle')) return;
    const handle = document.createElement('div');
    handle.className = 'shell-drawer-handle';
    handle.title = 'Drag to resize';
    handle.innerHTML = '<span class="grip"></span>';
    drawerEl.appendChild(handle);

    let dragging = false;
    let startY = 0;
    let startH = 0;
    const onMove = (e) => {
      if (!dragging) return;
      const dy = startY - e.clientY;
      const next = Math.max(160, Math.min(window.innerHeight - 120, startH + dy));
      document.documentElement.style.setProperty('--drawer-h', `${next}px`);
      try { if (typeof scheduleMapResize === 'function') scheduleMapResize(); } catch (_) {}
    };
    const onUp = () => {
      dragging = false;
      handle.removeAttribute('data-dragging');
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      dragging = true;
      startY = e.clientY;
      const cssVal = getComputedStyle(document.documentElement).getPropertyValue('--drawer-h').trim();
      startH = parseFloat(cssVal) || drawerEl.getBoundingClientRect().height;
      handle.setAttribute('data-dragging', 'true');
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }

  function ensureDrawerHandles() {
    installDrawerHandle(document.getElementById('drOffcanvas'));
    installDrawerHandle(document.getElementById('parallelCoordsPanel'));
  }

  // Floating toolbar that summarises which drawer panes are open and offers
  // a single collapse/expand control. Lives at the top of the active drawer
  // surface, above the rail/inspector cutouts.
  function ensureDrawerBar() {
    if (document.getElementById('shellDrawerBar')) return;
    const bar = document.createElement('div');
    bar.id = 'shellDrawerBar';
    bar.className = 'shell-drawer-bar';
    bar.innerHTML = `
      <button class="icon-btn" id="shellDrawerCollapse" type="button" title="Collapse / expand drawer" aria-label="Collapse drawer">
        ${svg('chev', 12)}
      </button>
      <span class="bar-label">Bottom drawer · drag top edge to resize</span>
      <span class="bar-count" id="shellDrawerCount"></span>
    `;
    document.body.appendChild(bar);
    bar.querySelector('#shellDrawerCollapse').addEventListener('click', () => {
      const collapsed = document.body.getAttribute('data-drawer-collapsed') === 'true';
      document.body.setAttribute('data-drawer-collapsed', collapsed ? 'false' : 'true');
      try { scheduleMapResize(); } catch (_) {}
    });
  }

  function syncDrawerBar() {
    const bar = document.getElementById('shellDrawerBar');
    if (!bar) return;
    const drOpen = document.body.getAttribute('data-dr-open') === 'true';
    const pcOpen = document.body.getAttribute('data-pc-open') === 'true';
    const open = drOpen || pcOpen;
    bar.setAttribute('data-shown', open ? 'true' : 'false');
    const count = (drOpen ? 1 : 0) + (pcOpen ? 1 : 0);
    const lbl = bar.querySelector('#shellDrawerCount');
    if (lbl) lbl.textContent = count ? `${count} pane${count > 1 ? 's' : ''}` : '';
  }

  function watchDrawers() {
    ensureDrawerBar();
    ensureDrawerHandles();
    const dr = document.getElementById('drOffcanvas');
    if (dr) {
      dr.addEventListener('shown.bs.offcanvas', () => { syncDrawerBodyAttrs(); scheduleMapResize(); });
      dr.addEventListener('hidden.bs.offcanvas', () => { syncDrawerBodyAttrs(); scheduleMapResize(); });
    }
    const pc = document.getElementById('parallelCoordsPanel');
    if (pc) {
      new MutationObserver(() => { syncDrawerBodyAttrs(); scheduleMapResize(); })
        .observe(pc, { attributes: true, attributeFilter: ['class'] });
    }
    syncDrawerBodyAttrs();
  }

  document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('faveTopbar')) return;

    document.body.appendChild(buildTopbar());
    document.body.appendChild(buildRail());
    document.body.appendChild(buildLegend());
    document.body.appendChild(buildZoom());
    const travel = buildTravel();
    document.body.appendChild(travel);
    document.body.appendChild(buildCityMenu());
    document.body.appendChild(buildPOIPopover());
    document.body.appendChild(buildHistoryPopover());
    document.body.appendChild(buildChangesPopover());
    document.body.appendChild(buildAISidebar());

    wireTopbar();
    wireTravelMode(travel);
    wireZoom(document.getElementById('shellZoom'));
    wirePOIPopover();
    wireHistoryPopover();
    wireChangesPopover();
    wireAISidebar();
    wireDRDesignToolbar();
    wireClearSelectionPill();
    wireTooltipEvents();
    watchInspector();
    watchDrawers();
    syncScaleActiveStates();
    syncPOIRailActive();
    // Mirror POI checkbox state onto rail-poi instantly. Both the legacy
    // navbar checkboxes and the shell popover's mirror checkboxes feed
    // into the same selection (the shell ones dispatch 'change' on the
    // legacy ones), so listening on .poi-check covers both UIs.
    document.querySelectorAll('.poi-check').forEach(el => {
      el.addEventListener('change', syncPOIRailActive);
    });
    // Patch updateChangeLogUI so any change-log mutation refreshes the
    // shell popovers in lockstep with the legacy dropdown.
    patchChangeLogUI();
    // Initial resize once map exists (covers the page-load case where
    // #mapContainer's bbox changes after the deck.gl canvas mounts).
    setTimeout(scheduleMapResize, 200);
    setTimeout(scheduleMapResize, 800);

    // Click-outside closes popovers.
    document.addEventListener('mousedown', e => {
      const target = e.target;
      if (target.closest('.shell-popover')) return;
      // ignore clicks on triggers themselves (they'll toggle via their own handler)
      if (target.closest('#topCityBtn') || target.closest('#rail-poi') || target.closest('#rail-history') || target.closest('#rail-wif') || target.closest('#rail-changes')) return;
      closeAllPopovers();
    });

    // Periodically resync SCALE active states + city/job status.
    let lastDistrict = null, lastMezo = null;
    setInterval(() => {
      const dV = (typeof districtView !== 'undefined' ? districtView : false);
      const mV = (typeof mezoView !== 'undefined' ? mezoView : false);
      if (dV !== lastDistrict || mV !== lastMezo) {
        lastDistrict = dV; lastMezo = mV;
        syncScaleActiveStates();
      }
      syncJobStatus();
      syncCityMenu();
    }, 600);
  });
})();
