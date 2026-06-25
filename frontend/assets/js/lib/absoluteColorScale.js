// absoluteColorScale.js — EXPERIMENTAL · ISOLATED · MEANT TO BE REMOVED.
//
// Adds an "Absolute scale" toggle to the fairness toolbar. The map normally
// colors buildings on a RELATIVE scale: every recompute, fairness.js rescales
// all benefits to the city's own p10..p95 spread (normalizeBenefitsToScores), so
// an abundant category (e.g. parking, present everywhere) lifts every building
// roughly equally and gets normalized away — the colors barely move. This toggle
// swaps in an ABSOLUTE scale anchored to a FIXED yardstick, so adding such a
// category visibly greens the whole map. Built purely for A/B comparison.
//
// ── HOW TO REMOVE COMPLETELY (no residue in working code) ──────────────────
//   1. delete this file
//   2. delete its <script> tag in index.html
//   3. in models/fairness.js, delete the small guard block at the top of
//      normalizeBenefitsToScores AND drop the trailing `true` argument on
//      `const scores = normalizeBenefitsToScores(benefits, fairnessTravelMode, true);`
//      (revert it to `normalizeBenefitsToScores(benefits, fairnessTravelMode)`).
//   4. in models/fairness.js, delete the `window.fairAdequacyOnComputed(...)`
//      guard block (just after the per-building `props.fair`/`fair_overall` loop).
//   5. in views/layers.js, delete the `window.fairAdequacyRecolor(...)` guard
//      block inside the building getFillColor (just before `colorFromScore(...)`).
//   Nothing else in the app references this module. The relative path is the
//   untouched original, so removal can never break the working coloring.
//
// Everything experimental lives behind window.* hooks so fairness.js can defer
// to us with a typeof guard that is simply false when this file is gone.
(function () {
  'use strict';

  let _on = false;
  let _refByKey = Object.create(null);   // `${city}|${mode}` -> { floor, span }

  function _key(mode) {
    const city = (typeof lastCityName === 'string' && lastCityName) ? lastCityName : 'city';
    const m = (typeof normalizeTravelMode === 'function')
      ? normalizeTravelMode(mode) : (mode || 'walking');
    return `${city}|${m}`;
  }

  function _quantile(sorted, p) {
    if (!sorted.length) return 0;
    if (sorted.length === 1) return sorted[0];
    const i = (sorted.length - 1) * p, lo = Math.floor(i), hi = Math.ceil(i);
    if (lo === hi) return sorted[lo];
    const t = i - lo;
    return sorted[lo] * (1 - t) + sorted[hi] * t;
  }

  // The FIXED yardstick for a city+mode: floor pinned at 0 (true "no access"),
  // span FROZEN at the p95 (log1p space) of the FIRST mix seen after the toggle
  // was switched on. Because the span never re-derives from the current
  // distribution, adding/removing a category slides every building up/down a
  // fixed ruler instead of being rescaled into 0..1. The color scale AND the
  // adequacy metric below share this exact ruler. Returns null if no positive
  // benefits exist yet.
  function _ensureRef(mode, benefits) {
    const key = _key(mode);
    let ref = _refByKey[key];
    if (!ref) {
      const vals = [];
      for (let i = 0; i < benefits.length; i++) {
        const v = benefits[i];
        if (Number.isFinite(v) && v > 0) vals.push(Math.log1p(v));
      }
      if (!vals.length) return null;
      vals.sort((a, b) => a - b);
      ref = { floor: 0, span: Math.max(1e-9, _quantile(vals, 0.95)) };
      _refByKey[key] = ref;
    }
    return ref;
  }

  function _scoreFor(benefit, ref) {
    if (!Number.isFinite(benefit) || benefit <= 0) return 0;
    return Math.max(0, Math.min(1, (Math.log1p(benefit) - ref.floor) / ref.span));
  }

  function fairAbsoluteNormalize(benefits, mode) {
    const ref = _ensureRef(mode, benefits);
    if (!ref) return benefits.map(() => 0);
    return benefits.map((v) => _scoreFor(v, ref));
  }

  function fairAbsoluteScaleActive() { return _on; }

  // ===================== THRESHOLD ADEQUACY METRIC ==========================
  // "What share of residents clear a fixed provision bar?" — a sufficiency
  // (provision-level) metric, the natural counterpart to the relative/Gini
  // inequality view. Computed on the SAME absolute yardstick as the color scale:
  // each building's absolute score (0..1) is compared to a threshold; the score
  // is population-weighted so it reads as "% of population adequately served".
  let _threshold = 0.5;          // default bar — see UI tooltip / chat for rationale
  let _lastBenefits = null;      // most recent SELECTED-MIX benefits (index = featIdx)
  let _lastMode = null;
  let _adequacy = null;          // 0..1 share of POPULATION at/above the bar
  let _adequacyBldg = null;      // 0..1 share of BUILDINGS at/above the bar (same denom set)
  let _maskOn = true;            // flag below-bar buildings on the map
  // Strong RED — high contrast against BOTH the light-grey basemap AND the
  // blue→green→yellow fairness ramp (its complementary opposite), and the
  // intuitive "deficient / under-served" colour. Distinct 4th state, not a ramp hue.
  const _MASK_COLOR = [220, 53, 53, 240];
  const _MASK_CSS = 'rgb(220,53,53)';   // same colour for the HTML legend swatches

  // Map-recolor hook (called by views/layers.js for each building). Returns the
  // grey mask colour when the absolute scale + masking are on AND this building's
  // absolute provision score is below the bar; otherwise null → keep the normal
  // ramp colour. props.fair.score IS the absolute score while Absolute is active,
  // the same number the adequacy % thresholds, so map and metric always agree.
  function fairAdequacyRecolor(props) {
    if (!_on || !_maskOn) return null;
    const s = props && props.fair ? props.fair.score : null;
    if (!Number.isFinite(s)) return null;
    return s < _threshold ? _MASK_COLOR : null;
  }

  // Repaint the building layer (re-evaluates getFillColor via the fairRecolorTick
  // updateTrigger) without recomputing fairness — used when only the bar/mask moves.
  function _repaintMap() {
    try { fairRecolorTick++; } catch (e) { /* not declared yet — ignore */ }
    if (typeof updateLayers === 'function') updateLayers();
  }

  // Population weight per building (mirrors fairness.js demand fallback chain).
  function _popMap() {
    if (typeof synthBuildingPopMap !== 'undefined' && synthBuildingPopMap && synthBuildingPopMap.size) return synthBuildingPopMap;
    if (typeof epiBuildingPopMap !== 'undefined' && epiBuildingPopMap && epiBuildingPopMap.size) return epiBuildingPopMap;
    if (typeof vaxjoBuildingPopMap !== 'undefined' && vaxjoBuildingPopMap && vaxjoBuildingPopMap.size) return vaxjoBuildingPopMap;
    return null;
  }

  // Recompute the population-weighted adequacy share from the cached benefits and
  // the frozen yardstick. Re-thresholding (slider drag) reuses this — no fairness
  // recompute needed. Denominator = residents with a known population weight (so
  // unserved residents count as inadequate, NOT excluded); without a pop map we
  // fall back to counting served buildings.
  function recomputeAdequacy() {
    if (!_lastBenefits || !_lastBenefits.length) { _adequacy = _adequacyBldg = null; renderBadge(); return; }
    const ref = _ensureRef(_lastMode, _lastBenefits);
    if (!ref) { _adequacy = _adequacyBldg = null; renderBadge(); return; }
    const pop = _popMap();
    // Two shares over the SAME set (residents with a known pop weight, so they're
    // directly comparable): population-weighted (people) and unweighted (buildings).
    // They differ because dense central blocks hold many residents in few buildings.
    let num = 0, den = 0, numB = 0, denB = 0;
    for (let i = 0; i < _lastBenefits.length; i++) {
      const w = pop ? (pop.get(i) || 0) : (_lastBenefits[i] > 0 ? 1 : 0);
      if (w <= 0) continue;
      den += w; denB += 1;
      if (_scoreFor(_lastBenefits[i], ref) >= _threshold) { num += w; numB += 1; }
    }
    _adequacy = den > 0 ? num / den : null;
    _adequacyBldg = denB > 0 ? numB / denB : null;
    renderBadge();
  }

  // Hook called by fairness.js at the end of every SELECTED-MIX compute.
  function fairAdequacyOnComputed(benefits, mode) {
    _lastBenefits = benefits;
    _lastMode = mode;
    if (_on) recomputeAdequacy();   // only meaningful while the absolute scale is on
  }

  // Toggle handler: re-baseline (clear the frozen yardstick so the CURRENT mix
  // re-anchors the ruler), then recompute fairness so the map recolors under the
  // new scale. If fairness isn't active there's nothing colored to re-scale, so
  // just bump the recolor tick.
  function setAbsolute(on) {
    _on = !!on;
    _refByKey = Object.create(null);   // re-freeze yardstick to the current mix
    const field = document.getElementById('fairAdequacyField');
    if (field) field.style.display = _on ? '' : 'none';
    _applyLegend();
    const apply = () => {
      try { fairRecolorTick++; } catch (e) { /* not declared yet — ignore */ }
      if (typeof updateLayers === 'function') updateLayers();
      if (_on) recomputeAdequacy();    // benefits refreshed by the recompute below
    };
    if (typeof fairActive !== 'undefined' && fairActive
        && typeof recomputeFairnessAfterWhatIf === 'function') {
      Promise.resolve(recomputeFairnessAfterWhatIf()).then(apply).catch(apply);
    } else {
      apply();
    }
  }

  // Paint the adequacy % into its badge (created in injectToggle).
  function renderBadge() {
    const out = document.getElementById('fairAdequacyVal');
    if (!out) return;
    if (_adequacy == null) { out.textContent = '—'; out.title = ''; return; }
    const ppl = Math.round(_adequacy * 100);
    const bld = (_adequacyBldg == null) ? '—' : Math.round(_adequacyBldg * 100);
    out.textContent = `${ppl}% people · ${bld}% bldgs`;
    out.title = 'Share above the bar: of residents (population-weighted) vs of '
      + 'buildings (count). They differ because dense blocks hold many people in '
      + 'few buildings; the map mask colours buildings, so it tracks the bldgs %.';
  }

  // Relabel the bottom-left fairness legend while Absolute is active (the ramp
  // means provision level, not fairness, in this mode) and append a red
  // "below bar" marker row. Originals are stashed on the element and restored
  // when Absolute is turned off — so removing this module leaves no trace.
  function _applyLegend() {
    const leg = document.querySelector('.shell-legend');
    if (!leg) return;
    const head = leg.querySelector('.legend-head');
    const labels = leg.querySelectorAll('.ramp-labels span');
    if (head && leg.dataset.adqOrigHead == null) leg.dataset.adqOrigHead = head.textContent;
    if (labels.length && leg.dataset.adqOrigLabels == null) {
      leg.dataset.adqOrigLabels = JSON.stringify(Array.from(labels).map((s) => s.textContent));
    }
    if (_on) {
      if (head) head.textContent = 'Provision (absolute)';
      if (labels.length >= 3) {
        labels[0].textContent = 'Under-served';
        labels[1].textContent = 'Medium';
        labels[2].textContent = 'Well-served';
      }
    } else {
      if (head && leg.dataset.adqOrigHead != null) head.textContent = leg.dataset.adqOrigHead;
      if (labels.length && leg.dataset.adqOrigLabels != null) {
        const orig = JSON.parse(leg.dataset.adqOrigLabels);
        labels.forEach((s, i) => { if (orig[i] != null) s.textContent = orig[i]; });
      }
    }
    let row = leg.querySelector('#adqLegendBelow');
    const show = _on && _maskOn;
    if (show && !row) {
      row = document.createElement('div');
      row.id = 'adqLegendBelow';
      row.style.cssText = 'display:flex;align-items:center;gap:6px;margin-top:6px;font-size:11px';
      row.innerHTML = '<span style="display:inline-block;width:12px;height:12px;border-radius:2px;'
        + 'background:' + _MASK_CSS + ';border:1px solid rgba(0,0,0,.15)"></span>'
        + '<span>Below bar (under-served)</span>';
      leg.appendChild(row);
    }
    if (row) row.style.display = show ? 'flex' : 'none';
  }

  // --- Inject into the shell topbar, right after the "Model" segmented control,
  //     as a matching "Color scale: Relative / Absolute" segmented control. -----
  function injectToggle() {
    if (document.getElementById('fairColorScaleSeg')) return true;
    // The visible toolbar is the shell topbar (views/shell.js), NOT the legacy
    // Bootstrap navbar — target the Model segmented control's field.
    const modelBtn = document.querySelector('.fave-topbar .segmented [data-model]');
    if (!modelBtn) return false;   // shell topbar not mounted yet — caller retries
    const modelField = modelBtn.closest('.field') || modelBtn.closest('.topbar-section');
    if (!modelField || !modelField.parentElement) return false;

    const field = document.createElement('div');
    field.className = 'field';
    field.dataset.experimental = 'absolute-color-scale';
    field.title = 'EXPERIMENTAL: color buildings on a fixed ABSOLUTE scale instead '
      + 'of the per-recompute RELATIVE one. Absolute → an abundant category like '
      + 'parking greens the whole map; Relative (default) normalizes it away.';
    field.innerHTML =
      '<span class="field-label">Color scale</span>' +
      '<div class="segmented" id="fairColorScaleSeg">' +
      '<button data-active="true" data-colorscale="relative" type="button" ' +
      'aria-label="Relative color scale">Relative</button>' +
      '<button data-active="false" data-colorscale="absolute" type="button" ' +
      'aria-label="Absolute color scale">Absolute</button>' +
      '</div>';
    // Give Color scale its OWN topbar section preceded by a divider, exactly like
    // City|Model, so a 1px separator sits between Model and Color scale. Fall back
    // to appending inside the Model section if the expected structure isn't found.
    const modelSection = modelField.closest('.topbar-section');
    const topbar = modelSection ? modelSection.parentElement : null;
    if (modelSection && topbar) {
      const divider = document.createElement('div');
      divider.className = 'topbar-divider';
      divider.dataset.experimental = 'absolute-color-scale';
      const section = document.createElement('div');
      section.className = 'topbar-section';
      section.dataset.experimental = 'absolute-color-scale';
      section.appendChild(field);
      topbar.insertBefore(divider, modelSection.nextSibling);
      topbar.insertBefore(section, divider.nextSibling);
    } else {
      modelField.parentElement.insertBefore(field, modelField.nextSibling);
    }

    field.querySelectorAll('[data-colorscale]').forEach((b) => {
      b.addEventListener('click', () => {
        field.querySelectorAll('[data-colorscale]')
          .forEach((x) => x.setAttribute('data-active', 'false'));
        b.setAttribute('data-active', 'true');
        setAbsolute(b.dataset.colorscale === 'absolute');
      });
    });

    // Adequacy metric field (hidden until Absolute is active). A draggable bar:
    // the threshold is a policy choice, so it's adjustable for sensitivity.
    const adq = document.createElement('div');
    adq.className = 'field';
    adq.id = 'fairAdequacyField';
    adq.dataset.experimental = 'absolute-color-scale';
    adq.style.display = 'none';
    adq.title = 'Share of population whose ABSOLUTE provision score clears the bar. '
      + 'A sufficiency metric (provision level), complementing the relative/Gini '
      + 'inequality view. Drag the bar to test different sufficiency thresholds.';
    adq.innerHTML =
      '<span class="field-label">Adequacy ≥</span>' +
      '<input type="range" id="fairAdequacyThresh" min="0" max="1" step="0.05" value="0.5" '
      + 'style="width:84px;vertical-align:middle">' +
      '<span id="fairAdequacyThreshVal" style="min-width:2.4ch;display:inline-block;'
      + 'font-variant-numeric:tabular-nums">0.50</span>' +
      '<span id="fairAdequacyVal" class="badge" '
      + 'style="margin-left:6px;background:var(--accent,#2d7);color:#fff">—</span>' +
      // Mask toggle. Its label carries a grey swatch so it doubles as the legend:
      // "grey = below bar". The green→purple ramp legend (bottom-left) already
      // explains the above-bar provision levels.
      '<label id="fairAdequacyMaskLbl" style="margin-left:10px;display:inline-flex;'
      + 'align-items:center;gap:4px;font-size:11px;color:var(--ink-3,#667);'
      + 'white-space:nowrap;cursor:pointer">'
      + '<input type="checkbox" id="fairAdequacyMask" checked style="vertical-align:middle">'
      + '<span style="display:inline-block;width:11px;height:11px;border-radius:2px;'
      + 'background:rgb(220,53,53);border:1px solid rgba(0,0,0,.15)"></span>'
      + 'red = below bar</label>';
    field.parentElement.insertBefore(adq, field.nextSibling);

    const slider = adq.querySelector('#fairAdequacyThresh');
    const sliderVal = adq.querySelector('#fairAdequacyThreshVal');
    if (slider) {
      slider.addEventListener('input', () => {
        _threshold = parseFloat(slider.value);
        if (sliderVal) sliderVal.textContent = _threshold.toFixed(2);
        recomputeAdequacy();          // re-threshold the % (no fairness recompute)
        if (_on && _maskOn) _repaintMap();   // move the grey mask with the bar
      });
    }
    const maskChk = adq.querySelector('#fairAdequacyMask');
    if (maskChk) {
      maskChk.addEventListener('change', () => {
        _maskOn = maskChk.checked;
        _applyLegend();
        if (_on) _repaintMap();
      });
    }

    console.info('[absolute-scale] "Color scale" + adequacy metric + map mask injected after the Model control.');
    return true;
  }

  // Expose the hooks fairness.js looks for (window so they survive the IIFE).
  window.fairAbsoluteScaleActive = fairAbsoluteScaleActive;
  window.fairAbsoluteNormalize = fairAbsoluteNormalize;
  window.fairAdequacyOnComputed = fairAdequacyOnComputed;
  window.fairAdequacyRecolor = fairAdequacyRecolor;

  // The shell topbar is built asynchronously after this script runs, so try now,
  // retry on a timer, AND watch the DOM until the Model control appears.
  let _observer = null;
  function tryInject() {
    if (injectToggle()) { if (_observer) { _observer.disconnect(); _observer = null; } return true; }
    return false;
  }
  function start() {
    if (tryInject()) return;
    _observer = new MutationObserver(() => tryInject());
    _observer.observe(document.body, { childList: true, subtree: true });
    // Safety: stop observing after 30 s even if never found.
    setTimeout(() => { if (_observer) { _observer.disconnect(); _observer = null; } }, 30000);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
