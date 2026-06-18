/**
 * compare.js — Comparison-view mode for EpiCity (two days).
 *
 * Owns the compare-mode state and routes day picks into view.js's two
 * compare renderers (diff map and side-by-side scissor split). The live
 * sim is paused by ui.js before this module takes over the scene; on
 * disable it hands the live state back to view.js.
 *
 * Public API:
 *   import {
 *     compareState, enableCompareMode, disableCompareMode,
 *     setMode, setDayA, setDayB, refreshIfActive,
 *   } from './compare.js';
 *
 * UI plumbing (toggle button, dropdown, second scrubber thumb) lives in
 * ui.js — keeping presentation glue with the rest of the timeline code,
 * and pure compare logic in here.
 */

import {
  enableCompare,
  disableCompare,
  isCompareEnabled,
  selectByIdx,
  clearSelection,
} from './view.js';
import {
  enableSplit  as _enableSplitB,
  disableSplit as _disableSplitB,
  applyDays    as _applySplitDays,
} from './compare-view-b.js';
import {
  setSplitChainDays as _flowSetSplitDays,
  clearSplitChains  as _flowClearSplitChains,
} from './infection-flow.js';
import { t } from './i18n.js';

// ── State ────────────────────────────────────────────────────────────────────

export const compareState = {
  enabled: false,
  mode: 'diff',          // 'diff' | 'split'
  dayAIdx: 0,            // index into the history array (NOT a sim-day number)
  dayBIdx: 0,
  metric: 'prevalence',  // see _cmpMetricFn in view.js for the full set
  history: null,         // reference to the live history array
  layout:  null,         // city layout ref — needed for the split mode
                         // fill-extrusion source build (compare-view-b.js).
};

// Tracks which renderer is currently driving the scene so transitions
// (e.g. split → diff) can correctly tear down the previous one.
let _activeRenderer = null;   // 'diff' | 'split' | null

// ui.js installs the hover callback so the rich tooltip used by the
// single view also drives split-mode hover. We keep it here (not in
// compare-view-b.js) to avoid an import cycle.
let _hoverCb = null;
export function setCompareHoverCallback(fn) { _hoverCb = fn; }

// Notify ui.js when the day pair changes so the per-side SEIR overlays
// can refresh without re-driving the dispatch path.
const _changeListeners = [];
export function onCompareChange(fn) { _changeListeners.push(fn); }
function _notifyChange() {
  for (const fn of _changeListeners) {
    try { fn(compareState); } catch {}
  }
}

// ── Derived helpers ──────────────────────────────────────────────────────────

function _snapAt(idx) {
  if (!compareState.history) return null;
  if (idx < 0 || idx >= compareState.history.length) return null;
  return compareState.history[idx];
}

function _updateLabels() {
  const a = _snapAt(compareState.dayAIdx);
  const b = _snapAt(compareState.dayBIdx);
  const elA = document.getElementById('cmpDayANum');
  const elB = document.getElementById('cmpDayBNum');
  if (elA) elA.textContent = String(a?.day ?? '—');
  if (elB) elB.textContent = String(b?.day ?? '—');
  // Mirror onto the on-canvas divider labels so the side-by-side view
  // is unambiguous without the user having to look back at the popover.
  const divA = document.getElementById('cmpDividerLabelA');
  const divB = document.getElementById('cmpDividerLabelB');
  if (divA) divA.textContent = String(a?.day ?? '—');
  if (divB) divB.textContent = String(b?.day ?? '—');
}

function _toggleDivider(show) {
  const div = document.getElementById('cmpDivider');
  if (!div) return;
  if (show) div.classList.add('visible');
  else      div.classList.remove('visible');
}

function _toggleDiffLegend(show) {
  const el = document.getElementById('cmpLegendRow');
  if (!el) return;
  if (show) el.classList.add('visible');
  else      el.classList.remove('visible');
}

// ── Apply ────────────────────────────────────────────────────────────────────

/**
 * Push the current compare state into view.js. Safe to call repeatedly —
 * view.js handles diff vs. split internally and re-uses its color buffers.
 */
async function _applyNow() {
  if (!compareState.enabled) return;
  const a = _snapAt(compareState.dayAIdx);
  const b = _snapAt(compareState.dayBIdx);
  if (!a || !b) return;

  if (compareState.mode === 'split') {
    // Tear down any prior diff render on the three.js side first so the
    // primary map is in a clean state before we hide its building layer.
    if (_activeRenderer === 'diff') disableCompare(b.cells);
    if (_activeRenderer !== 'split') {
      await _enableSplitB(compareState.layout, a.cells, b.cells, {
        // Route fill-extrusion clicks back through view.js's selection
        // pipeline so the rest of the app (info panel, flow filters,
        // tooltip) reacts as if the user clicked in the live three.js
        // scene. Idx === null clears any previous selection.
        onSelect: (idx) => {
          if (idx === null || idx === undefined) clearSelection();
          else                                   selectByIdx(idx);
        },
        // Hover delegated to ui.js's _onHover via the registered cb.
        onHover: (idx) => { if (_hoverCb) _hoverCb(idx); },
      });
      _activeRenderer = 'split';
    } else {
      _applySplitDays(a.cells, b.cells);
    }
    // Re-replay the infection chain to each side's selected day so the
    // flow tree on the left shows Day A and on the right shows Day B
    // (instead of the latest cumulative chain on both halves).
    _flowSetSplitDays(
      compareState.history, compareState.layout,
      compareState.dayAIdx, compareState.dayBIdx,
    );
  } else {
    // Diff mode uses the existing three.js diverging-palette render.
    if (_activeRenderer === 'split') {
      _disableSplitB();
      _activeRenderer = null;
    }
    // Diff mode shares one canvas — no per-side flow chain.
    _flowClearSplitChains();
    enableCompare({
      mode:    'diff',
      cellsA:  a.cells,
      cellsB:  b.cells,
      metric:  compareState.metric,
    });
    _activeRenderer = 'diff';
  }
  _toggleDivider(compareState.mode === 'split');
  _toggleDiffLegend(compareState.mode === 'diff');
  _updateLabels();
  _notifyChange();
}

// Drag-coalescing: a rapid scrubber drag can fire ~60 setDayA calls/sec,
// each of which would otherwise rebake an 80k-building color buffer. We
// reuse the same 80 ms cadence the live scrubber uses for its infection
// flow rebuild — feels instant but skips ~95 % of intermediate frames.
const APPLY_DEBOUNCE_MS = 80;
let _applyTimer = null;
function _apply({ immediate = false } = {}) {
  _updateLabels();
  if (!compareState.enabled) return;
  if (immediate) {
    if (_applyTimer !== null) { clearTimeout(_applyTimer); _applyTimer = null; }
    _applyNow();
    return;
  }
  if (_applyTimer !== null) return;
  _applyTimer = setTimeout(() => {
    _applyTimer = null;
    _applyNow();
  }, APPLY_DEBOUNCE_MS);
}

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Enter compare mode. `history` should be the live history array (each
 * entry must include a per-building `cells` array — confirmed by the
 * existing scrubber path which also reads `snap.cells`).
 *
 * Defaults to comparing the first vs. last entry in history.
 */
export function enableCompareMode(history, layout, dayAIdx, dayBIdx, mode = 'diff') {
  if (!Array.isArray(history) || history.length === 0) return false;
  compareState.history  = history;
  compareState.layout   = layout;
  compareState.enabled  = true;
  compareState.mode     = mode;
  compareState.dayAIdx  = Math.max(0, Math.min(history.length - 1,
                                  dayAIdx ?? 0));
  compareState.dayBIdx  = Math.max(0, Math.min(history.length - 1,
                                  dayBIdx ?? history.length - 1));
  _apply({ immediate: true });
  return true;
}

export function disableCompareMode(currentCells) {
  compareState.enabled = false;
  _toggleDivider(false);
  _toggleDiffLegend(false);
  if (_activeRenderer === 'split') {
    _disableSplitB();
  } else if (_activeRenderer === 'diff') {
    disableCompare(currentCells);
  }
  _activeRenderer = null;
  // Drop the per-side flow chains so the live single-view rendering
  // takes over again on the next infection-flow draw.
  _flowClearSplitChains();
  _notifyChange();
}

export function setMode(mode) {
  if (mode !== 'diff' && mode !== 'split') return;
  compareState.mode = mode;
  if (compareState.enabled) _apply({ immediate: true });
}

export function setDayA(idx) {
  if (!compareState.history) return;
  const clamped = Math.max(0, Math.min(compareState.history.length - 1, idx | 0));
  if (clamped === compareState.dayAIdx) return;
  compareState.dayAIdx = clamped;
  if (compareState.enabled) _apply();
  else _updateLabels();
}

export function setDayB(idx) {
  if (!compareState.history) return;
  const clamped = Math.max(0, Math.min(compareState.history.length - 1, idx | 0));
  if (clamped === compareState.dayBIdx) return;
  compareState.dayBIdx = clamped;
  if (compareState.enabled) _apply();
  else _updateLabels();
}

export function setMetric(metric) {
  compareState.metric = metric || 'prevalence';
  if (compareState.enabled) _apply();
}

/** Re-apply if compare is on — used after the history array grows or the
 *  user changes a setting that affects coloring. */
export function refreshIfActive() {
  if (compareState.enabled) _apply({ immediate: true });
}

/** Convenience predicate so ui.js can branch in its scrubber handlers. */
export function isActive() { return compareState.enabled && isCompareEnabled(); }

/**
 * Tooltip refresh hook — called by ui.js when the language changes so
 * any compare-specific i18n attributes get re-read. The bulk of the
 * compare DOM uses data-i18n attributes that the i18n system already
 * picks up automatically; this is here for any future runtime-built
 * strings (e.g. delta summaries) that don't have static markup.
 */
export function relocalize() {
  // Reserved for runtime-generated strings — kept as a stable hook so
  // ui.js can call it without checking whether it's needed yet.
  void t;
}
