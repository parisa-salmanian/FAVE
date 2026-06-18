/**
 * picker.js — State 01 (City Picker) chrome around macro.js.
 *
 * Keeps the Sweden map (driven by macro.js) untouched and adds:
 *   • selected-city preview card (right side)
 *   • brand-pill in the top header
 *   • intro counter (city/scenario stats)
 *   • CTA on the city card → enters that city
 */

import { t } from './i18n.js';

let _summary    = [];
let _onEnter    = null;       // (cityId) → starts the simulation
let _selectedId = null;

/**
 * Initialise. Call after macro.js has the cities summary fetched.
 * @param {Array} summary    — list of {id, name, population, area, ...}
 * @param {Function} onEnter — called with the selected cityId when CTA clicked
 */
export function initPicker(summary, onEnter) {
  _summary = Array.isArray(summary) ? summary : [];
  _onEnter = onEnter;

  // The selected-city card was removed in favor of the macro hover-card
  // owned by macro.js. picker.js now only manages footer counters.

  // Brand pill on the macro header is a no-op (it's already on the macro view).
  // Footer counter:
  _renderCounter();
  _renderHeaderCount();
}

/** Show the city card with stats for the picked city. */
export function selectCity(cityId) {
  const c = _summary.find(s => s.id === cityId);
  if (!c) return;
  _selectedId = cityId;

  const card = document.getElementById('cityCard');
  if (!card) return;
  card.classList.add('on');

  _set('cityCardName', c.name || c.id);
  _set('cityCardCountry', c.region ? `${c.region} · Sweden` : 'Sweden');
  _set('ccPop',  (c.population || 0).toLocaleString());
  _set('ccArea', c.area_km2 != null ? `${Math.round(c.area_km2)} km²` : '—');
  _set('ccDens',
    (c.population && c.area_km2) ? `${Math.round(c.population / c.area_km2).toLocaleString()} /km²` : '—');
  _set('ccPois', c.poi_count != null ? c.poi_count.toLocaleString() : '—');

  const last = c.last_baseline_r0 != null
    ? `Last baseline: <b style="color:var(--text-dim)">R₀ = ${c.last_baseline_r0.toFixed(2)}</b>`
    : 'No previous runs';
  const lastEl = document.getElementById('ccLastRun');
  if (lastEl) lastEl.innerHTML = last;
}

/** Hide the city card (e.g. on language change or re-init). */
export function clearCity() {
  _selectedId = null;
  document.getElementById('cityCard')?.classList.remove('on');
}

/** Update the cached summary so counts refresh. */
export function setPickerSummary(summary) {
  _summary = Array.isArray(summary) ? summary : [];
  _renderCounter();
  _renderHeaderCount();
}

/* ── Internals ────────────────────────────────────────────────────────────── */

function _set(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function _renderCounter() {
  // Counter chips moved into the header pill; nothing to do here now.
}

function _renderHeaderCount() {
  const lbl = document.getElementById('macroCountText');
  if (!lbl) return;
  const avail  = _summary.filter(c => c.available !== false && !c.locked).length;
  const locked = _summary.filter(c => c.locked).length;
  const noun = avail === 1 ? t('city_one') : t('city_many');
  lbl.textContent = locked
    ? `${avail} ${noun} · ${locked} ${t('locked')}`
    : `${avail} ${noun}`;
}
