/**
 * gauge.js — Reusable circular SVG gauges. Two instances are wired up
 * for the bottom playbar: the R-eff gauge (#reffGauge) and the growth
 * gauge (#growthGauge). Each is rendered into its host container on
 * init and then updated through setGaugeFrac / setGaugeStroke.
 */

const SIZE = 22;
const STROKE = 2.6;
const R = SIZE / 2 - STROKE - 1;
const C = SIZE / 2;
const CIRC = 2 * Math.PI * R;

const _arcs = new Map();   // hostId → SVG <circle> arc element

function _build(hostId) {
  const host = document.getElementById(hostId);
  if (!host || _arcs.has(hostId)) return;
  host.innerHTML = `
    <svg width="${SIZE}" height="${SIZE}" viewBox="0 0 ${SIZE} ${SIZE}">
      <circle cx="${C}" cy="${C}" r="${R}" fill="none"
              stroke="var(--surface-3)" stroke-width="${STROKE}"/>
      <circle cx="${C}" cy="${C}" r="${R}" fill="none"
              stroke="var(--accent)" stroke-width="${STROKE}"
              stroke-linecap="round"
              stroke-dasharray="${CIRC}" stroke-dashoffset="${CIRC}"
              transform="rotate(-90 ${C} ${C})"/>
    </svg>`;
  _arcs.set(hostId, host.querySelector('circle:nth-of-type(2)'));
}

/** Wire both gauges. Call once after DOMContentLoaded. */
export function initGauge() {
  _build('reffGauge');
  _build('growthGauge');
}

function _setFrac(hostId, frac) {
  _build(hostId);
  const arc = _arcs.get(hostId);
  if (!arc) return;
  const f = Math.max(0, Math.min(1, Number(frac) || 0));
  arc.setAttribute('stroke-dashoffset', String(CIRC * (1 - f)));
}

function _setStroke(hostId, color) {
  _build(hostId);
  const arc = _arcs.get(hostId);
  if (!arc) return;
  arc.setAttribute('stroke', color);
}

function _stateColor(state) {
  if (state === 'danger' || state === 'up')   return 'var(--accent)';
  if (state === 'warn')                       return 'var(--s-e)';
  if (state === 'safe'   || state === 'down') return 'var(--s-s)';
  return 'var(--text-mute)';
}

/* ── R-eff (legacy API kept for ui.js call sites) ─────────────────────────── */

export function setGauge(value, max = 4) {
  const frac = (Number(value) || 0) / max;
  _setFrac('reffGauge', frac);
}

export function setGaugeColor(state) {
  const pill = document.getElementById('reffPill');
  if (pill) {
    pill.classList.remove('safe', 'warn', 'danger');
    if (state) pill.classList.add(state);
  }
  _setStroke('reffGauge', _stateColor(state));
}

/* ── Growth gauge ─────────────────────────────────────────────────────────── */

export function setGrowthGauge(frac, state) {
  _setFrac('growthGauge', frac);
  _setStroke('growthGauge', _stateColor(state));
  const pill = document.getElementById('growthPill');
  if (pill) {
    pill.classList.remove('safe', 'warn', 'danger', 'up', 'down', 'flat');
    if (state) pill.classList.add(state);
  }
}
