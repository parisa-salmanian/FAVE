/* ============================================================
   history.js – Action History Stack (Observer-based, v3)
   ============================================================
   KEY FIX: main.js declares state with `let` (not `var`), so
   they are in the global LEXICAL environment, NOT on `window`.
   We reference them DIRECTLY (no `window.` prefix).

   Include in index.html AFTER main.js, BEFORE llm.js:
     <script src="assets/js/history.js"></script>
   ============================================================ */
(function () {

  /* ---------- constants ---------- */
  var HISTORY_MAX = 40;
  var DEBOUNCE_MS = 600;
  var INITIAL_DELAY_MS = 3000;

  /* ---------- state ---------- */
  var stack = [];
  var _cursor = -1;          // index of the currently active snapshot
  var _timer = null;
  var _prevFP = null;
  var _pauseObservers = false;

  /* ---------- deep clone ---------- */
  function deepClone(obj) {
    if (!obj) return obj;
    try { if (typeof structuredClone === 'function') return structuredClone(obj); } catch (e) {}
    return JSON.parse(JSON.stringify(obj));
  }

  /* ------------------------------------------------------------------
     Read current state – ALL references are direct (no window.)
     because main.js uses `let` declarations
     ------------------------------------------------------------------ */
  function readState() {
    // Safely read a let-declared variable; if history.js loads
    // before main.js somehow, this avoids ReferenceError
    try {
      return {
        sourceMode:          sourceMode,
        viewMode:            viewMode,
        selectedYear:        selectedYear,
        heightScale:         heightScale,
        lastCityName:        lastCityName,
        fairActive:          fairActive,
        fairCategory:        fairCategory,
        fairRecolorTick:     fairRecolorTick,
        fairnessTravelMode:  fairnessTravelMode,
        fairnessModel:       fairnessModel,
        fairnessColorScheme: fairnessColorScheme,
        overallGini:         overallGini,
        whatIfMode:          whatIfMode,
        whatIfType:          whatIfType,
        districtView:        districtView,
        mezoView:            mezoView,
        selectedPOIMix:      deepClone(selectedPOIMix || []),
        baseCityFC:          deepClone(baseCityFC),
        newbuildsFC:         deepClone(newbuildsFC),
        selectedBuildingType: (typeof selectedBuildingType !== 'undefined') ? selectedBuildingType : '',
        currentPOIsFC:        deepClone(currentPOIsFC),
        activePOICats:        window.activePOICats ? Array.from(window.activePOICats) : []
      };
    } catch (e) {
      console.warn('[history] readState failed:', e);
      return null;
    }
  }

  /* ------------------------------------------------------------------
     Write state back (restore) – direct assignment to let-scoped vars
     ------------------------------------------------------------------ */
  function writeState(s) {
    try {
      sourceMode          = s.sourceMode;
      viewMode            = s.viewMode;
      selectedYear        = s.selectedYear;
      heightScale         = s.heightScale;
      lastCityName        = s.lastCityName;
      fairActive          = s.fairActive;
      fairCategory        = s.fairCategory;
      fairRecolorTick     = s.fairRecolorTick;
      fairnessTravelMode  = s.fairnessTravelMode;
      fairnessModel       = s.fairnessModel;
      fairnessColorScheme = s.fairnessColorScheme;
      overallGini         = s.overallGini;
      whatIfMode           = s.whatIfMode;
      whatIfType            = s.whatIfType;
      districtView          = s.districtView;
      mezoView              = s.mezoView;
      selectedPOIMix        = deepClone(s.selectedPOIMix);
      baseCityFC            = deepClone(s.baseCityFC);
      newbuildsFC           = deepClone(s.newbuildsFC);
      currentPOIsFC         = s.currentPOIsFC ? deepClone(s.currentPOIsFC) : null;
      window.activePOICats  = new Set(s.activePOICats || []);
    } catch (e) {
      console.error('[history] writeState failed:', e);
    }
  }

  /* ------------------------------------------------------------------
     Fingerprint – detect whether state actually changed
     ------------------------------------------------------------------ */
  function fingerprint() {
    try {
      var fc = baseCityFC;
      var feats = (fc && fc.features) ? fc.features : [];
      var mockN = 0, wifN = 0;
      for (var i = 0; i < feats.length; i++) {
        var p = feats[i] && feats[i].properties;
        if (p) {
          if (p.__whatIfMock) mockN++;
          if (p.__whatIf) wifN++;
        }
      }
      return [
        sourceMode, viewMode, lastCityName,
        fairCategory, fairnessTravelMode, fairnessModel,
        fairnessColorScheme, districtView, mezoView, whatIfMode,
        feats.length, mockN, wifN,
        (document.getElementById('overallGiniOut') || {}).textContent || '',
        (document.getElementById('giniOut') || {}).textContent || ''
      ].join('|');
    } catch (e) {
      return Math.random().toString();
    }
  }

  /* ------------------------------------------------------------------
     Meta – lightweight state summary for label inference
     ------------------------------------------------------------------ */
  function buildMeta() {
    try {
      var fc = baseCityFC;
      var feats = (fc && fc.features) ? fc.features : [];
      var mockN = 0, wifN = 0;
      for (var i = 0; i < feats.length; i++) {
        var p = feats[i] && feats[i].properties;
        if (p) { if (p.__whatIfMock) mockN++; if (p.__whatIf) wifN++; }
      }
      return {
        sourceMode:          sourceMode,
        viewMode:            viewMode,
        lastCityName:        lastCityName,
        fairCategory:        fairCategory || '',
        fairnessTravelMode:  fairnessTravelMode,
        fairnessModel:       fairnessModel,
        fairnessColorScheme: fairnessColorScheme,
        districtView:        districtView,
        mezoView:            mezoView,
        whatIfMode:           whatIfMode,
        featureCount:         feats.length,
        mockCount:            mockN,
        whatIfCount:           wifN,
        giniText:             (document.getElementById('giniOut') || {}).textContent || '',
        overallGiniText:      (document.getElementById('overallGiniOut') || {}).textContent || ''
      };
    } catch (e) {
      return {};
    }
  }

  /* ------------------------------------------------------------------
     Label inference
     ------------------------------------------------------------------ */
  function inferLabel(prev, cur) {
    if (!prev || !prev.lastCityName) return 'Initial state';
    if (prev.lastCityName !== cur.lastCityName) return 'Loaded city: ' + cur.lastCityName;
    if (prev.sourceMode !== cur.sourceMode) return 'Source \u2192 ' + cur.sourceMode;
    if (prev.mockCount !== cur.mockCount) {
      if (cur.mockCount === 0 && prev.mockCount > 0) return 'Cleared mock buildings';
      if (cur.mockCount > prev.mockCount) return 'Added ' + (cur.mockCount - prev.mockCount) + ' mock buildings';
      return 'Mock buildings: ' + cur.mockCount;
    }
    if (prev.whatIfCount !== cur.whatIfCount) {
      if (cur.whatIfCount > prev.whatIfCount) return 'What-if: +'+(cur.whatIfCount - prev.whatIfCount)+' buildings';
      if (cur.whatIfCount < prev.whatIfCount) return 'Reset what-if changes';
    }
    if (Math.abs((prev.featureCount||0) - (cur.featureCount||0)) > 10) return 'Buildings updated (' + cur.featureCount + ')';
    if (prev.fairnessTravelMode !== cur.fairnessTravelMode) return 'Travel mode \u2192 ' + cur.fairnessTravelMode;
    if (prev.fairnessModel !== cur.fairnessModel) return 'Fairness model \u2192 ' + cur.fairnessModel;
    if (prev.fairCategory !== cur.fairCategory) {
      if (!cur.fairCategory) return 'POI cleared';
      if (cur.fairCategory === 'mix') {
        try {
          var names = (selectedPOIMix || []).map(function (m) {
            return typeof prettyPOIName === 'function' ? prettyPOIName(m.cat) : m.cat;
          });
          if (names.length) return 'POI \u2192 ' + names.join(' + ');
        } catch (e) {}
        return 'POI \u2192 mix';
      }
      var pretty = '';
      try { pretty = typeof prettyPOIName === 'function' ? prettyPOIName(cur.fairCategory) : cur.fairCategory; } catch(e) { pretty = cur.fairCategory; }
      return 'POI \u2192 ' + pretty;
    }
    // Detect POI mix content changed while category stayed 'mix'
    if (cur.fairCategory === 'mix' && prev.fairCategory === 'mix') {
      var prevGini = prev.giniText || '';
      var curGini = cur.giniText || '';
      if (prevGini !== curGini && curGini && curGini !== '\u2014' && curGini !== '\u2026') {
        try {
          var mixNames = (selectedPOIMix || []).map(function (m) {
            return typeof prettyPOIName === 'function' ? prettyPOIName(m.cat) : m.cat;
          });
          if (mixNames.length) return 'POI \u2192 ' + mixNames.join(' + ');
        } catch (e) {}
      }
    }
    if (prev.fairnessColorScheme !== cur.fairnessColorScheme) return 'Color scheme \u2192 ' + cur.fairnessColorScheme;
    if (prev.districtView !== cur.districtView) return 'District view: ' + (cur.districtView ? 'on' : 'off');
    if (prev.mezoView !== cur.mezoView) return 'Mezo view: ' + (cur.mezoView ? 'on' : 'off');
    if (prev.whatIfMode !== cur.whatIfMode) return 'What-if mode \u2192 ' + cur.whatIfMode;
    if (prev.viewMode !== cur.viewMode) return 'View \u2192 ' + cur.viewMode;
    if (prev.giniText !== cur.giniText && cur.giniText && cur.giniText !== '\u2014' && cur.giniText !== '\u2026') return 'Fairness updated';
    if (prev.overallGiniText !== cur.overallGiniText && cur.overallGiniText && cur.overallGiniText !== '\u2014' && cur.overallGiniText !== '\u2026') return 'Overall fairness updated';
    return null; // no meaningful change detected
  }

  /* ------------------------------------------------------------------
     Capture
     ------------------------------------------------------------------ */
  function captureSnapshot(labelOverride) {
    var fp = fingerprint();
    if (fp === _prevFP && stack.length > 0 && !labelOverride) return;

    var curMeta = buildMeta();
    var prevMeta = (_cursor >= 0 && _cursor < stack.length) ? stack[_cursor]._meta : null;
    var label = labelOverride || inferLabel(prevMeta, curMeta);

    // If no meaningful change detected, skip
    if (!label) return;

    _prevFP = fp;

    var state = readState();
    if (!state) return;

    stack.push({ label: label, timestamp: Date.now(), state: state, _meta: curMeta });
    _cursor = stack.length - 1;
    if (stack.length > HISTORY_MAX) { stack.shift(); _cursor = stack.length - 1; }
    renderPanel();
  }

  /* ------------------------------------------------------------------
     Debounced capture
     ------------------------------------------------------------------ */
  function debouncedCapture() {
    if (_pauseObservers) return;   // don't even queue during restore
    if (_timer) clearTimeout(_timer);
    _timer = setTimeout(function () {
      _timer = null;
      if (_pauseObservers) return; // check again after delay
      captureSnapshot(null);
    }, DEBOUNCE_MS);
  }

  /* ------------------------------------------------------------------
     Restore
     ------------------------------------------------------------------ */
  function restoreSnapshot(index) {
    if (index < 0 || index >= stack.length) return;
    _pauseObservers = true;

    _cursor = index;
    var s = stack[index].state;

    writeState(s);

    // ---- sync UI controls ----
    setVal('sourceSelect', s.sourceMode);
    setVal('fairnessTravelMode', s.fairnessTravelMode);
    setVal('fairnessModelMode', s.fairnessModel);
    setVal('fairnessColorScheme', s.fairnessColorScheme);
    setVal('yearFilter', s.selectedYear);
    setVal('whatIfTypeSelect', s.whatIfType);

    var cityEl = document.getElementById('cityInput');
    if (cityEl) cityEl.value = s.lastCityName;

    var hsEl = document.getElementById('heightScale');
    if (hsEl) {
      hsEl.value = s.heightScale;
      var hsLbl = document.getElementById('heightScaleLabel');
      if (hsLbl) hsLbl.textContent = Number(s.heightScale).toFixed(1);
    }

    var mAll = document.getElementById('modeAll');
    var mNew = document.getElementById('modeNew');
    if (mAll) mAll.classList.toggle('active', s.viewMode === 'all');
    if (mNew) mNew.classList.toggle('active', s.viewMode === 'new');

    var wifRadio = document.querySelector('input[name="whatIfMode"][value="' + s.whatIfMode + '"]');
    if (wifRadio) wifRadio.checked = true;

    var oGini = document.getElementById('overallGiniOut');
    if (oGini) {
      oGini.textContent = Number.isFinite(s.overallGini)
        ? (typeof formatFairnessBadgeValue === 'function'
            ? formatFairnessBadgeValue(s.overallGini)
            : s.overallGini.toFixed(4))
        : '\u2014';
    }

    try { setSelectedBuildingType(s.selectedBuildingType || '', true); } catch (e) {}
    try { refreshBuildingTypeDropdown(); } catch (e) {}
    try { syncSpatialToggleButtons(); } catch (e) {}
    try { updateFairnessLegendUI(); } catch (e) {}

    // Sync POI checkboxes to match restored state
    try {
      var restoredCats = new Set();
      if (s.fairCategory === 'mix' && Array.isArray(s.selectedPOIMix)) {
        s.selectedPOIMix.forEach(function (m) { restoredCats.add(m.cat); });
      } else if (s.fairCategory && s.fairCategory !== 'mix') {
        restoredCats.add(s.fairCategory);
      }
      document.querySelectorAll('.poi-check').forEach(function (el) {
        var cat = el.getAttribute('data-cat') || el.value;
        el.checked = restoredCats.has(cat);
      });
      // Sync weights if mix
      if (s.fairCategory === 'mix' && Array.isArray(s.selectedPOIMix)) {
        s.selectedPOIMix.forEach(function (m) {
          var weightEl = document.querySelector('.poi-weight[data-cat="' + m.cat + '"]');
          if (weightEl) {
            weightEl.value = m.weight;
            var badge = document.querySelector('.poi-weight-val[data-cat="' + m.cat + '"]');
            if (badge) badge.textContent = m.weight;
          }
        });
      }
    } catch (e) {}

    // Re-trigger fairness computation to rebuild currentPOIsFC and recolor
    try {
      if (s.fairActive && s.fairCategory) {
        if (s.fairCategory === 'mix' && Array.isArray(s.selectedPOIMix) && s.selectedPOIMix.length) {
          computeFairnessWeighted(s.selectedPOIMix);
        } else if (s.fairCategory !== 'mix') {
          computeFairnessFast(s.fairCategory);
        }
      } else {
        // No fairness was active — clear POIs and update
        currentPOIsFC = null;
        if (typeof window !== 'undefined') window.activePOICats = new Set();
        updateLayers();
      }
    } catch (e) {
      try { updateLayers(); } catch (e2) {}
    }

    _prevFP = fingerprint();

    setTimeout(function () { _pauseObservers = false; }, 1500);
    renderPanel();
  }

  function setVal(id, value) {
    var el = document.getElementById(id);
    if (el && value !== undefined) el.value = value;
  }

  /* ------------------------------------------------------------------
     MutationObserver on key DOM elements
     ------------------------------------------------------------------ */
  function observeText(elementId) {
    var el = document.getElementById(elementId);
    if (!el) { console.warn('[history] #' + elementId + ' not found'); return; }

    var SKIP = ['\u2026', '\u2014', '', 'Starting\u2026', 'Loading\u2026',
                'Loading local\u2026', 'Loading GeoJSON\u2026',
                'Computing\u2026', 'Switching travel mode\u2026',
                'Switching access model\u2026'];

    new MutationObserver(function () {
      if (_pauseObservers) return;
      var text = (el.textContent || '').trim();
      if (SKIP.indexOf(text) >= 0) return;
      debouncedCapture();
    }).observe(el, { childList: true, characterData: true, subtree: true });
  }

  /* ------------------------------------------------------------------
     UI event listeners (fire after main.js has bound its own)
     ------------------------------------------------------------------ */
  function addUIListeners() {
    ['fairnessTravelMode', 'fairnessModelMode', 'fairnessColorScheme'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.addEventListener('change', function () {
        setTimeout(debouncedCapture, 100);
      });
    });

    document.querySelectorAll('input[name="whatIfMode"]').forEach(function (el) {
      el.addEventListener('change', function () {
        setTimeout(debouncedCapture, 200);
      });
    });

    ['modeAll', 'modeNew', 'districtToggleBtn', 'mezoToggleBtn', 'microToggleBtn'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.addEventListener('click', function () {
        setTimeout(debouncedCapture, 300);
      });
    });
  }

  /* ------------------------------------------------------------------
     Panel UI
     ------------------------------------------------------------------ */
  function createPanel() {
    if (document.getElementById('historyPanel')) return;

    var panel = document.createElement('div');
    panel.id = 'historyPanel';
    panel.style.cssText = [
      'position:fixed', 'bottom:12px', 'right:80px', 'width:300px',
      'max-height:360px', 'background:rgba(20,20,24,0.97)',
      'border:1px solid #444', 'border-radius:10px', 'z-index:9999',
      'display:flex', 'flex-direction:column', 'color:#ddd',
      'font-family:system-ui,sans-serif', 'font-size:13px',
      'box-shadow:0 6px 24px rgba(0,0,0,0.55)', 'backdrop-filter:blur(8px)'
    ].join(';');

    var hdr = document.createElement('div');
    hdr.style.cssText = 'padding:7px 12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center;user-select:none;';
    hdr.innerHTML = '<strong>\u23F1 History Stack</strong>';

    var btns = document.createElement('span');
    btns.style.cssText = 'display:flex;gap:5px;';

    var clearBtn = mkBtn('\u2715 Clear', '#c0392b', function () {
      stack.length = 0; _cursor = -1; _prevFP = null; renderPanel();
    });
    var collapseBtn = mkBtn('\u25BC', '#555', function () {
      var list = document.getElementById('historyList');
      if (!list) return;
      var hidden = list.style.display === 'none';
      list.style.display = hidden ? '' : 'none';
      collapseBtn.textContent = hidden ? '\u25BC' : '\u25B2';
    });
    btns.appendChild(clearBtn);
    btns.appendChild(collapseBtn);
    hdr.appendChild(btns);

    var list = document.createElement('ul');
    list.id = 'historyList';
    list.style.cssText = 'list-style:none;margin:0;padding:0;overflow-y:auto;max-height:300px;';

    panel.appendChild(hdr);
    panel.appendChild(list);
    document.body.appendChild(panel);
    renderPanel();
  }

  function mkBtn(text, bg, handler) {
    var b = document.createElement('button');
    b.textContent = text;
    b.style.cssText = 'background:' + bg + ';color:#eee;border:none;border-radius:4px;cursor:pointer;font-size:11px;padding:2px 7px;line-height:1.4;';
    b.addEventListener('click', handler);
    return b;
  }

  function renderPanel() {
    var list = document.getElementById('historyList');
    if (!list) return;
    list.innerHTML = '';

    if (!stack.length) {
      var empty = document.createElement('li');
      empty.style.cssText = 'padding:12px;text-align:center;opacity:0.4;font-style:italic;';
      empty.textContent = 'Waiting for actions\u2026';
      list.appendChild(empty);
      return;
    }

    for (var i = 0; i < stack.length; i++) {
      (function (idx) {
        var entry = stack[idx];
        var isCurrent = idx === _cursor;

        var li = document.createElement('li');
        li.style.cssText = 'padding:5px 10px;border-bottom:1px solid #2a2a2a;cursor:pointer;transition:background .15s;'
          + (isCurrent ? 'background:#0d6efd22;color:#8ec5ff;' : '');
        li.title = 'Restore \u2192 ' + entry.label;

        var num = document.createElement('span');
        num.style.cssText = 'display:inline-block;width:24px;opacity:0.45;font-size:11px;';
        num.textContent = (idx + 1) + '.';

        var lbl = document.createElement('span');
        lbl.textContent = entry.label;

        var time = document.createElement('span');
        time.style.cssText = 'float:right;opacity:0.35;font-size:11px;';
        time.textContent = new Date(entry.timestamp).toLocaleTimeString();

        li.appendChild(num);
        li.appendChild(lbl);
        li.appendChild(time);

        li.addEventListener('mouseenter', function () { if (!isCurrent) li.style.background = '#ffffff10'; });
        li.addEventListener('mouseleave', function () { li.style.background = isCurrent ? '#0d6efd22' : ''; });
        li.addEventListener('click', function () { restoreSnapshot(idx); });

        list.appendChild(li);
      })(i);
    }

    list.scrollTop = list.scrollHeight;
  }

  /* ------------------------------------------------------------------
     Boot
     ------------------------------------------------------------------ */
  function init() {
    createPanel();

    // Observe key DOM elements
    observeText('jobStatus');
    observeText('overallGiniOut');
    observeText('giniOut');
    observeText('whatIfLassoStatus');
    observeText('whatIfSuggestionOut');

    addUIListeners();

    // Capture initial state after city finishes loading
    setTimeout(function () {
      try {
        if (baseCityFC && baseCityFC.features && baseCityFC.features.length) {
          captureSnapshot('Initial state');
        }
      } catch (e) {}
    }, INITIAL_DELAY_MS);
  }

  /* ------------------------------------------------------------------
     Public
     ------------------------------------------------------------------ */
  window.historySnap    = captureSnapshot;
  window.historyRestore = restoreSnapshot;
  window.historyStack   = stack;

  /* ------------------------------------------------------------------
     Start
     ------------------------------------------------------------------ */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(init, 200); });
  } else {
    setTimeout(init, 200);
  }

})();