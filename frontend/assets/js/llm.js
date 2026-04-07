// /Users/pasaaa/city-newbuilds/api/frontend/assets/js/llm.js
// Uses globals/functions from main.js (API_BASE, lastCityName, loadCityOSM, etc.)

async function callLLM(question) {
  const ctx = {
    city: typeof lastCityName !== "undefined" ? lastCityName : null,
    source: typeof sourceMode !== "undefined" ? sourceMode : null,
    mode: typeof viewMode !== "undefined" ? viewMode : null,
    year: (typeof selectedYear !== "undefined" && selectedYear !== "") ? selectedYear : null,
    poi: typeof fairCategory !== "undefined" ? fairCategory : null,
    bbox: (typeof baseCityFC !== "undefined" && baseCityFC) ? turf.bbox(baseCityFC) : null,
    categories: (typeof ALL_CATEGORIES !== "undefined") ? ALL_CATEGORIES : [],
    mock_types: (typeof WHATIF_MOCK_TYPE_OPTIONS !== "undefined") ? WHATIF_MOCK_TYPE_OPTIONS : [],
    has_lasso: (typeof whatIfLasso !== "undefined") ? !!whatIfLasso.selectionRing : false
  };

  const r = await fetch(`${API_BASE}/llm/plan`, {
    method: 'POST',
    headers: { 'Content-Type':'application/json' },
    body: JSON.stringify({ question, context: ctx })
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json(); // {actions:[...]}
}

async function dispatchActions(plan) {
  const explain = [];
  const actions = (plan && Array.isArray(plan.actions)) ? plan.actions : [];

  // --- FIX: track whether travel mode was changed and whether fairness was
  // already (re)computed by a later action, so we can avoid the fire-and-forget
  // race condition that left the map all-gray. ---
  let travelModeChanged = false;
  let fairnessComputed = false;

  for (const a of actions) {
    try {
      switch (a.type) {
        case 'LOAD_CITY':
          if (typeof loadCityOSM === "function") {
            lastCityName = a.city;
            await loadCityOSM(a.city);
            if (typeof autoComputeOverall === "function") await autoComputeOverall();
          } else {
            explain.push("LOAD_CITY not available.");
          }
          break;

        case 'SET_SOURCE':
          if (typeof sourceSelect !== "undefined") {
            sourceMode = a.source;
            if (sourceSelect) sourceSelect.value = a.source;
            if (a.source === 's1' && typeof loadSentinelData === "function") {
              await loadSentinelData();
            } else if (typeof loadCityOSM === "function") {
              await loadCityOSM(lastCityName);
            }
            if (typeof autoComputeOverall === "function") await autoComputeOverall();
          }
          break;

        case 'SET_MODE':
          if (typeof setMode === "function") setMode(a.mode);
          break;

        case 'SET_YEAR':
          if (typeof document !== "undefined") {
            selectedYear = String(a.year);
            const yf = document.getElementById('yearFilter');
            if (yf) yf.value = selectedYear;
            if (typeof updateLayers === "function") updateLayers();
          }
          break;

        case 'SET_HEIGHT':
          if (typeof document !== "undefined") {
            if (typeof heightScaleEl !== "undefined" && heightScaleEl) {
              heightScale = a.scale;
              heightScaleEl.value = a.scale;
              if (typeof heightScaleLabel !== "undefined" && heightScaleLabel)
                heightScaleLabel.textContent = a.scale.toFixed(1);
              if (typeof updateLayers === "function") updateLayers();
            }
          }
          break;

        case 'SET_FAIRNESS_TRAVEL_MODE':
          // FIX: suppress the automatic recompute ({recompute: false}) to
          // prevent a fire-and-forget async recomputeFairnessAfterWhatIf()
          // that races with the subsequent SET_POI / COMPUTE_FAIRNESS action.
          // The unawaited promise could delete props.fair on all buildings
          // (Phase 1 of computeIfCityFairness) while the SET_POI computation
          // is also running, leaving the map all-gray.
          // Instead, we defer the recompute to the end of the action loop.
          if (typeof setFairnessTravelMode === "function") {
            setFairnessTravelMode(a.mode, { recompute: false });
          } else if (typeof fairnessTravelMode !== "undefined") {
            fairnessTravelMode = (typeof normalizeTravelMode === "function")
              ? normalizeTravelMode(a.mode)
              : a.mode;
            if (typeof fairnessTravelModeSelect !== "undefined" && fairnessTravelModeSelect) {
              fairnessTravelModeSelect.value = fairnessTravelMode;
            }
          }
          travelModeChanged = true;
          break;

        case 'SET_POI':
          if (typeof document !== "undefined") {
            if (typeof baseCityFC === "undefined" || !baseCityFC) {
              explain.push("Load a city first before setting POI fairness.");
              break;
            }
            const normalizedCat = typeof a.category === 'string' ? a.category.toLowerCase() : '';
            // FIX: route ALL SET_POI commands through the checkbox mechanism
            // (same as SET_POI_MIX) so that selectedPOIMix is properly updated
            // and onPOIUIChange runs the full fairness + UI flow.
            // The old code called computeFairnessFast() directly, which left
            // selectedPOIMix empty. Any later recompute or UI refresh would
            // then find zero selected categories and clear the fairness colors.
            if (normalizedCat === 'mix') {
              const mixCats = Array.isArray(ALL_CATEGORIES) ? ALL_CATEGORIES : [];
              const checks = document.querySelectorAll('.poi-check');
              checks.forEach((chk) => {
                const cat = chk.getAttribute('data-cat') || '';
                chk.checked = mixCats.includes(cat);
              });
            } else {
              if (typeof ALL_CATEGORIES !== "undefined" && Array.isArray(ALL_CATEGORIES)) {
                if (!ALL_CATEGORIES.includes(normalizedCat)) {
                  explain.push(`Unknown POI category "${a.category}". Try one of: ${ALL_CATEGORIES.join(', ')}.`);
                  break;
                }
              }
              // Check ONLY the target checkbox, uncheck all others
              const checks = document.querySelectorAll('.poi-check');
              checks.forEach((chk) => {
                const cat = chk.getAttribute('data-cat') || '';
                chk.checked = (cat.toLowerCase() === normalizedCat);
              });
            }
            // Dispatch change event → triggers debounced onPOIUIChange which
            // reads checkboxes, sets selectedPOIMix, and computes fairness.
            const first = document.querySelector('.poi-check');
            if (first) {
              first.dispatchEvent(new Event('change', { bubbles: true }));
            }
            fairnessComputed = true;
          } else {
            explain.push("SET_POI not available.");
          }
          break;

          case 'SET_POI_MIX':
          if (typeof document !== "undefined") {
            const cats = Array.isArray(a.categories) ? a.categories : [];
            const normalized = cats
              .filter((cat) => typeof cat === 'string')
              .map((cat) => cat.toLowerCase());
            const weights = (a && typeof a.weights === 'object' && a.weights) ? a.weights : {};
            const weightEntries = Object.entries(weights)
              .filter(([cat, weight]) => typeof cat === 'string' && Number.isFinite(weight))
              .map(([cat, weight]) => [cat.toLowerCase(), Number(weight)]);
            const checks = document.querySelectorAll('.poi-check');
            checks.forEach((chk) => {
              const cat = chk.getAttribute('data-cat') || '';
              chk.checked = normalized.includes(cat.toLowerCase());
            });
            weightEntries.forEach(([cat, weight]) => {
              const clamped = Math.max(1, Math.min(10, weight));
              const wEl = document.querySelector(`.poi-weight[data-cat="${cat}"]`);
              if (wEl) {
                wEl.value = String(clamped);
                const badge = document.querySelector(`.poi-weight-val[data-cat="${cat}"]`);
                if (badge) badge.textContent = String(clamped);
              }
            });
            const first = document.querySelector('.poi-check');
            if (first) {
              first.dispatchEvent(new Event('change', { bubbles: true }));
            }
            fairnessComputed = true;
          } else {
            explain.push("SET_POI_MIX not available.");
          }
          break;

        case 'SET_POI_SYMBOLS':
          if (typeof setPOISymbolsVisibility === "function") {
            setPOISymbolsVisibility(!!a.enabled);
          } else if (typeof showPOISymbols !== "undefined") {
            showPOISymbols = !!a.enabled;
            if (typeof poiStyleTick !== "undefined") poiStyleTick++;
            if (typeof updateLayers === "function") updateLayers();
          } else {
            explain.push("SET_POI_SYMBOLS not available.");
          }
          break;

        case 'COMPUTE_FAIRNESS':
          if (typeof fairCategory !== "undefined" && fairCategory && typeof computeFairnessFast === "function") {
            await computeFairnessFast(fairCategory);
            fairnessComputed = true;
          }
          break;

        case 'ROUTE_BETWEEN':
          if (typeof baseCityFC !== "undefined" && baseCityFC && baseCityFC.features) {
            const fA = baseCityFC.features.find(f => (f.properties?.name||'').toLowerCase().includes(a.from.toLowerCase()));
            const fB = baseCityFC.features.find(f => (f.properties?.name||'').toLowerCase().includes(a.to.toLowerCase()));
            if (fA && fB) {
              firstFeat = fA; secondFeat = fB;
              if (typeof updateRouteIfReady === "function") updateRouteIfReady();
            } else {
              explain.push(`Couldn't find "${a.from}" or "${a.to}" in current data.`);
            }
          }
          break;
        
        case 'WHATIF_SUGGEST':
          if (typeof runWhatIfSuggestionFromChat === "function") {
            await runWhatIfSuggestionFromChat(a.prompt || '', {
              categories: Array.isArray(a.categories) ? a.categories : [],
              count: Number.isFinite(a.count) ? a.count : undefined,
              mode: typeof a.mode === 'string' ? a.mode : undefined,
              focus: typeof a.focus === 'string' ? a.focus : undefined,
              fairnessTarget: typeof a.fairness_target === 'string' ? a.fairness_target : undefined,
              areaFocus: typeof a.area === 'string' ? a.area : undefined
            });
          } else {
            explain.push("WHATIF_SUGGEST not available.");
          }
          break;

        case 'WHATIF_APPLY':
          if (typeof applyWhatIfSuggestions === "function") {
            await applyWhatIfSuggestions();
          } else {
            explain.push("WHATIF_APPLY not available.");
          }
          break;

        case 'WHATIF_LASSO_APPLY':
          if (typeof applyWhatIfLassoFromChat === "function") {
            await applyWhatIfLassoFromChat(Array.isArray(a.counts) ? a.counts : []);
          } else {
            explain.push("WHATIF_LASSO_APPLY not available.");
          }
          break;
        
        case 'SET_DISTRICT_VIEW':
          if (typeof setDistrictView === "function") {
            setDistrictView(!!a.enabled);
          } else {
            explain.push("SET_DISTRICT_VIEW not available.");
          }
          break;

        case 'SHOW_DISTRICT_STATS':
          if (typeof showDistrictStatsByName === "function") {
            await showDistrictStatsByName(a.district);
          } else {
            explain.push("SHOW_DISTRICT_STATS not available.");
          }
          break;

        case 'EXPLAIN':
          explain.push(a.message);
          break;
      }
    } catch (err) {
      console.error('Dispatch error', a, err);
      const msg = err?.message || 'Unknown error';
      if (typeof msg === 'string' && msg.includes('No POIs found')) {
        explain.push('No POIs found for this area. Try a different category, load a different city, or request what-if suggestions to add POIs.');
      } else {
        explain.push(`Problem applying action ${a.type}: ${msg}`);
      }
    }
  }

  // --- FIX: deferred recompute when travel mode changed but no fairness
  // action already recomputed (e.g. user said "switch to car distance"
  // without mentioning a POI category). Properly awaited, no race. ---
  if (travelModeChanged && !fairnessComputed) {
    if (typeof fairActive !== "undefined" && fairActive && typeof recomputeFairnessAfterWhatIf === "function") {
      try {
        await recomputeFairnessAfterWhatIf();
      } catch (err) {
        console.warn('Deferred travel-mode recompute failed', err);
      }
    }
  }

  return explain.join(' ');
}

(function initAI() {
  function ready(fn){ (document.readyState !== 'loading') ? fn() : document.addEventListener('DOMContentLoaded', fn); }
  ready(() => {
    const inp = document.getElementById('aiInput');
    const btn = document.getElementById('aiSend');
    const ans = document.getElementById('aiAnswer');
    if (!inp || !btn || !ans) return;

    const flushUI = () => new Promise((resolve) => setTimeout(resolve, 0));

    const send = async () => {
      const q = (inp.value||'').trim(); if (!q) return;
      ans.textContent = 'Thinking';
      await flushUI();
      try {
        const plan = await callLLM(q);
        const note = await dispatchActions(plan);
        ans.textContent = note || 'Done';
      } catch (e) {
        console.error(e);
        const base = (typeof API_BASE !== "undefined" && API_BASE) ? API_BASE : "the API";
        const hint = (e instanceof TypeError || /Failed to fetch/i.test(e?.message || ""))
          ? ` Check that ${base} is running and CORS allows this origin.`
          : "";
        ans.textContent = `Sorry, something went wrong: ${e?.message || 'Unknown error.'}${hint}`;
      }
    };
    btn.addEventListener('click', send);
    inp.addEventListener('keydown', (e)=>{ if (e.key==='Enter') send(); });
  });
})();