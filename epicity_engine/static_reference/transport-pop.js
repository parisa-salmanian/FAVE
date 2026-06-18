/**
 * transport-pop.js — Transport hover popover.
 *
 * Mirrors the POI popover's chrome: a header, a checkbox list per
 * transport mode (train / tram / ferry / bus), a count chip showing
 * the live highlighted-line total, and a footer with Clear + Show all
 * actions. Toggling a row drives the same per-mode highlight pipeline
 * the legacy sub-row checkboxes already used (`highlightTransportLine`).
 */

import { highlightTransportLine } from './map.js';
import { t, onLangChange } from './i18n.js';

// Per-mode metadata. `labelKey` and `lcKey` are i18n keys for the
// heading label and the lowercase plural shown in the count line —
// looked up via t() at render time so toggling language refreshes
// every row in place.
const MODES = [
  { key: 'plane', color: '#0ea5e9', icon: 'ic-airport', labelKey: 'tp_planes',  lcKey: 'tp_planes_lc'  },
  { key: 'train', color: '#f97316', icon: 'ic-train',   labelKey: 'tp_trains',  lcKey: 'tp_trains_lc'  },
  { key: 'tram',  color: '#22c55e', icon: 'ic-bus2',    labelKey: 'tp_trams',   lcKey: 'tp_trams_lc'   },
  { key: 'ferry', color: '#0891b2', icon: 'ic-bus2',    labelKey: 'tp_ferries', lcKey: 'tp_ferries_lc' },
  { key: 'bus',   color: '#f59e0b', icon: 'ic-bus',     labelKey: 'tp_buses',   lcKey: 'tp_buses_lc'   },
];

let _state = null;          // { mode → bool }
let _stats = null;          // { mode → { stops, lines } }

export function initTransportPop() {
  const pop = document.getElementById('transportPop');
  if (!pop) return;
  _state = Object.fromEntries(MODES.map(m => [m.key, false]));
  // Tri-state Show-all click: all-off / all-on toggle. The full / partial
  // visual is reflected automatically by _render below.
  pop.querySelector('#transportShowAll')?.addEventListener('click', () => {
    const allOn = MODES.filter(m => m.key !== 'plane').every(m => _state[m.key]);
    for (const m of MODES) {
      if (m.key === 'plane') continue;       // plane has no toggle
      _setMode(m.key, !allOn);
    }
    _render();
  });
  // Re-render when the user flips EN ↔ SV so every mode label, the
  // lowercase plural in the count line, and the empty-state message
  // pick up the new language without needing to reopen the popover.
  onLangChange(_render);
  _render();
}

/** Refresh the per-mode counts shown in the popover (called on city load). */
export function setTransportStats(stats) {
  _stats = stats || null;
  _render();
}

export function openTransportPop() {
  document.getElementById('transportPop')?.classList.add('open');
}

export function closeTransportPop() {
  document.getElementById('transportPop')?.classList.remove('open');
}

/* ── Internals ────────────────────────────────────────────────────────────── */

function _setMode(mode, on) {
  if (!_state) return;
  _state[mode] = !!on;
  highlightTransportLine(mode, !!on);
  // Mirror into the legacy hidden sub-row checkbox so anything that
  // queries the old DOM (e.g. the transport panel toggle path) stays
  // in sync.
  const cb = document.querySelector(`#transportPanel input[data-hl="${mode}"]`);
  if (cb && cb.checked !== !!on) cb.checked = !!on;
}

function _render() {
  const list = document.getElementById('transportPopList');
  const count = document.getElementById('transportPopCount');
  if (!list || !_state) return;
  list.innerHTML = '';
  let on = 0;
  let totalLines = 0;
  let availableCount = 0;
  for (const m of MODES) {
    const s = _stats?.[m.key] || { stops: 0, lines: 0, trips: 0 };
    // Only show modes that actually exist in this city.
    if ((s.lines || 0) + (s.stops || 0) + (s.trips || 0) === 0) continue;
    availableCount++;
    if (_state[m.key]) on++;
    totalLines += s.lines || 0;
    // Plane has no per-mode highlight (no lines on the map), so leave
    // it as a stat-only row.
    const interactive = m.key !== 'plane';
    const label = t(m.labelKey);
    const lc    = t(m.lcKey);
    const row = document.createElement('div');
    row.className = `poi-row child ${_state[m.key] ? 'checked' : ''} ${interactive ? '' : 'no-toggle'}`;
    row.innerHTML = `
      <span class="chev"></span>
      <span class="cb">${interactive
        ? (_state[m.key] ? '<svg class="i" style="width:10px;height:10px;color:#fff"><use href="#ic-check"/></svg>' : '')
        : ''}</span>
      <span class="bubble" style="background:${m.color};font-size:11px"><svg class="i" style="width:12px;height:12px;color:#fff"><use href="#${m.icon}"/></svg></span>
      <span class="lbl">${label}</span>
      <span class="nav">
        <span class="count">${s.trips || 0} ${lc} · ${s.lines || 0} ${t('tp_unit_lines')} · ${s.stops || 0} ${t('tp_unit_stops')}</span>
      </span>`;
    if (interactive) row.addEventListener('click', () => {
      _setMode(m.key, !_state[m.key]);
      _render();
    });
    list.appendChild(row);
  }
  if (count) count.textContent = availableCount === 0 ? '—' : `${on}/${availableCount}`;
  if (availableCount === 0) {
    list.innerHTML = `<div class="search-pop-empty">${t('tp_no_data')}</div>`;
  }

  // Tri-state Show-all visual: only the toggleable modes count.
  const tri = document.getElementById('transportShowAllToggle');
  if (tri) {
    const toggleable = MODES.filter(m => m.key !== 'plane' &&
      (((_stats?.[m.key]?.lines || 0) + (_stats?.[m.key]?.stops || 0) + (_stats?.[m.key]?.trips || 0)) > 0));
    const onCount = toggleable.filter(m => _state[m.key]).length;
    tri.classList.remove('partial', 'full');
    if (toggleable.length > 0 && onCount === toggleable.length)        tri.classList.add('full');
    else if (onCount > 0)                                              tri.classList.add('partial');
  }
}
