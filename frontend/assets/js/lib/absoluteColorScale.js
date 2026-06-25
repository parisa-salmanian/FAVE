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

  // ABSOLUTE normalization. The floor is pinned at 0 (true "no access"), and the
  // span is FROZEN at the p95 (log1p space) of the FIRST mix computed after the
  // toggle was switched on for this city+mode. Because the span never re-derives
  // from the current distribution, adding or removing a category slides every
  // building up/down a fixed ruler instead of being rescaled back into 0..1 —
  // which is exactly the relative behaviour we want to contrast against.
  function fairAbsoluteNormalize(benefits, mode) {
    const key = _key(mode);
    let ref = _refByKey[key];
    if (!ref) {
      const vals = [];
      for (let i = 0; i < benefits.length; i++) {
        const v = benefits[i];
        if (Number.isFinite(v) && v > 0) vals.push(Math.log1p(v));
      }
      if (!vals.length) return benefits.map(() => 0);
      vals.sort((a, b) => a - b);
      ref = { floor: 0, span: Math.max(1e-9, _quantile(vals, 0.95)) };
      _refByKey[key] = ref;
    }
    return benefits.map((v) => {
      if (!Number.isFinite(v) || v <= 0) return 0;
      return Math.max(0, Math.min(1, (Math.log1p(v) - ref.floor) / ref.span));
    });
  }

  function fairAbsoluteScaleActive() { return _on; }

  // Toggle handler: re-baseline (clear the frozen yardstick so the CURRENT mix
  // re-anchors the ruler), then recompute fairness so the map recolors under the
  // new scale. If fairness isn't active there's nothing colored to re-scale, so
  // just bump the recolor tick.
  function setAbsolute(on) {
    _on = !!on;
    _refByKey = Object.create(null);   // re-freeze yardstick to the current mix
    const apply = () => {
      try { fairRecolorTick++; } catch (e) { /* not declared yet — ignore */ }
      if (typeof updateLayers === 'function') updateLayers();
    };
    if (typeof fairActive !== 'undefined' && fairActive
        && typeof recomputeFairnessAfterWhatIf === 'function') {
      Promise.resolve(recomputeFairnessAfterWhatIf()).then(apply).catch(apply);
    } else {
      apply();
    }
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
    modelField.parentElement.insertBefore(field, modelField.nextSibling);

    field.querySelectorAll('[data-colorscale]').forEach((b) => {
      b.addEventListener('click', () => {
        field.querySelectorAll('[data-colorscale]')
          .forEach((x) => x.setAttribute('data-active', 'false'));
        b.setAttribute('data-active', 'true');
        setAbsolute(b.dataset.colorscale === 'absolute');
      });
    });
    console.info('[absolute-scale] "Color scale" toggle injected after the Model control.');
    return true;
  }

  // Expose the hooks fairness.js looks for (window so they survive the IIFE).
  window.fairAbsoluteScaleActive = fairAbsoluteScaleActive;
  window.fairAbsoluteNormalize = fairAbsoluteNormalize;

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
