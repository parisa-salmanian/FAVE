/**
 * chart.js — Epidemic curve canvas renderer.
 *
 * The five SEIR series can be toggled individually by clicking their
 * legend chips (`.cl-item` in #chartBody) or the "All" chip at the end.
 * Y-axis is auto-scaled to the visible series so hiding the dominant
 * compartment lets the smaller curves regain detail.
 */

const SERIES = [
  { key: 'S', color: '#4ade80' },
  { key: 'E', color: '#facc15' },
  { key: 'I', color: '#f87171' },
  { key: 'R', color: '#60a5fa' },
  { key: 'D', color: '#94a3b8' },
];

let _history = [];
// Default: only the Infectious (I) curve is visible on first load —
// it's the headline epidemic metric and the only one that climbs
// meaningfully from day 0. S dwarfs everything on the y-axis early
// on; E/R/D add noise the user can opt back in via the legend chips.
const _visible = { S: false, E: false, I: true, R: false, D: false };

/** Render the epidemic curve with the given history array. */
export function renderChart(history) {
  _history = history;
  _draw();
}

/** Set up the resize observer + wire the per-series toggle chips. */
export function initChart() {
  _resize();
  new ResizeObserver(_resize).observe(document.getElementById('chartPanel'));
  _bindLegend();
}

/* ── Private ────────────────────────────────────────────────────────────── */

function _resize() {
  const cvs = document.getElementById('chartCanvas');
  if (!cvs) return;
  cvs.width  = cvs.clientWidth;
  cvs.height = cvs.clientHeight;
  _draw();
}

function _bindLegend() {
  const legend = document.querySelector('#chartBody .chart-legend');
  if (!legend) return;
  // Append an "All" chip at the end if not already present so the user
  // can flip every series back on with one click.
  if (!legend.querySelector('[data-cl="ALL"]')) {
    const all = document.createElement('span');
    all.className = 'cl-item cl-item-all';
    all.dataset.cl = 'ALL';
    all.innerHTML = '<span class="cl-dot"></span>All';
    legend.appendChild(all);
  }
  // Tag the existing five chips with data-cl + a small toggle dot
  // (re-using the existing .cl-line element as the dot).
  legend.querySelectorAll('.cl-item').forEach(item => {
    if (!item.dataset.cl) {
      const txt = (item.textContent || '').trim();
      const k = txt[0];
      if (SERIES.find(s => s.key === k)) item.dataset.cl = k;
    }
  });
  legend.addEventListener('click', e => {
    const chip = e.target.closest('.cl-item');
    if (!chip) return;
    const k = chip.dataset.cl;
    if (!k) return;
    if (k === 'ALL') {
      const allOn = SERIES.every(s => _visible[s.key]);
      for (const s of SERIES) _visible[s.key] = !allOn;
    } else {
      _visible[k] = !_visible[k];
    }
    _syncLegend();
    _draw();
  });
  _syncLegend();
}

function _syncLegend() {
  const legend = document.querySelector('#chartBody .chart-legend');
  if (!legend) return;
  legend.querySelectorAll('.cl-item').forEach(item => {
    const k = item.dataset.cl;
    if (!k) return;
    const on = (k === 'ALL')
      ? SERIES.every(s => _visible[s.key])
      : !!_visible[k];
    item.classList.toggle('off', !on);
  });
}

function _draw() {
  const cvs = document.getElementById('chartCanvas');
  if (!cvs) return;
  const cc  = cvs.getContext('2d');
  const W   = cvs.width;
  const H   = cvs.height;
  const pad = { t: 8, r: 8, b: 20, l: 44 };
  const iW  = W - pad.l - pad.r;
  const iH  = H - pad.t - pad.b;

  cc.clearRect(0, 0, W, H);
  cc.fillStyle = '#1e293b';
  cc.fillRect(0, 0, W, H);

  if (_history.length < 2) return;

  // Auto-scale Y to whatever series are currently visible — hiding S
  // lets the smaller compartments regain vertical detail.
  let maxVal = 0;
  for (const h of _history) {
    let sum = 0;
    for (const s of SERIES) if (_visible[s.key]) sum = Math.max(sum, h[s.key]);
    if (sum > maxVal) maxVal = sum;
  }
  if (maxVal === 0) return;

  const xs = iW / (_history.length - 1);
  const ys = iH / maxVal;

  // Horizontal grid lines + Y-axis labels
  cc.lineWidth = 0.8; cc.strokeStyle = '#1e3a5f';
  cc.font = '10px sans-serif'; cc.textAlign = 'right'; cc.fillStyle = '#475569';
  for (let i = 0; i <= 4; i++) {
    const yy = pad.t + iH - iH * i / 4;
    cc.beginPath(); cc.moveTo(pad.l, yy); cc.lineTo(pad.l + iW, yy); cc.stroke();
    cc.fillText(Math.round(maxVal * i / 4).toLocaleString(), pad.l - 3, yy + 3);
  }

  // X-axis day labels
  const step = Math.max(1, Math.floor(_history.length / 6));
  cc.textAlign = 'center'; cc.fillStyle = '#475569';
  for (let i = 0; i < _history.length; i += step)
    cc.fillText(`D${_history[i].day}`, pad.l + i * xs, H - 4);

  // Axes
  cc.strokeStyle = '#334155'; cc.lineWidth = 1;
  cc.beginPath();
  cc.moveTo(pad.l, pad.t);
  cc.lineTo(pad.l, pad.t + iH);
  cc.lineTo(pad.l + iW, pad.t + iH);
  cc.stroke();

  // Data series — only the toggled-on compartments are drawn.
  for (const { key, color } of SERIES) {
    if (!_visible[key]) continue;
    cc.beginPath();
    cc.strokeStyle = color; cc.lineWidth = 1.8; cc.lineJoin = 'round';
    _history.forEach((h, i) => {
      const xx = pad.l + i * xs;
      const yy = pad.t + iH - h[key] * ys;
      i === 0 ? cc.moveTo(xx, yy) : cc.lineTo(xx, yy);
    });
    cc.stroke();
  }
}
