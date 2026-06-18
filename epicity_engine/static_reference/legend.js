/**
 * legend.js — City Legend panel: group swatches + independent Color/Height/Scale selectors
 *             + gradient bar (infection/population modes) + appearance mode toggle.
 */

import { sampleScaleCSS } from './colorscales.js';
import { GROUP_COLORS, getIncomeRange }   from './view.js';
import { t } from './i18n.js';

// Helper: convert a GROUP_COLORS integer hex to a CSS #rrggbb string.
const _hex = (n) => '#' + n.toString(16).padStart(6, '0');

// Legend entries — one row per POI group, sourced from the same
// GROUP_COLORS table view.js uses for the map + pins. Residential
// stays at the top as a distinct row so houses have an obvious
// label. "Other" (commercial + industrial fallback) lives at the
// bottom so the user can see what neutral grey represents.
// Labels are functions so they pick up the current language at render time
const GROUP_ENTRIES = [
  { label: () => t('grp_residential'), key: 'residential' },
  { label: () => t('grp_health'),      key: 'health'      },
  { label: () => t('grp_education'),   key: 'education'   },
  { label: () => t('grp_daily'),       key: 'daily'       },
  { label: () => t('grp_culture'),     key: 'culture'     },
  { label: () => t('grp_nature'),      key: 'nature'      },
  { label: () => t('grp_transport'),   key: 'transport'   },
  { label: () => t('grp_commercial'),  key: 'commercial'  },
  { label: () => t('grp_industrial'),  key: 'industrial'  },
];

// Labels for the gradient bar endpoints per color mode (functions for i18n)
const GRAD_LABELS = {
  infection:        { lo: () => t('grad_0'), hi: () => t('grad_max_inf') },
  infections_here:  { lo: () => t('grad_0'), hi: () => t('grad_max_infections_here') },
  exposed:     { lo: () => t('grad_0'),     hi: () => t('grad_max_exp') },
  susceptible: { lo: () => t('grad_0'),     hi: () => t('grad_max_sus') },
  recovered:   { lo: () => t('grad_0'),     hi: () => t('grad_max_rec') },
  prevalence:  { lo: () => '0%',            hi: () => '100%' },
  population:  { lo: () => t('grad_empty'), hi: () => t('grad_dense')   },
  deaths:      { lo: () => t('grad_0'),     hi: () => t('grad_max_deaths') },
  income:      {
    lo: () => { const r = getIncomeRange(); return `${Math.round(r.lo)} kSEK`; },
    hi: () => { const r = getIncomeRange(); return `${Math.round(r.hi)} kSEK`; },
  },
};

// ── Viz state ─────────────────────────────────────────────────────────────────

// Default to the ABM-only "Infection sites" view — paints + heightens
// each building by the count of S→E transmission events recorded AT
// that location. Falls back gracefully on the compartmental engine
// (cells have no `infections_here`, so everything is 0 = flat) — in
// that case the user can flip to 'infection' / 'prevalence' via the
// Color by / Height by selectors. Match heightMode so the building
// silhouette tracks the colour story instead of crossing it.
let _colorMode  = 'infections_here';
let _heightMode = 'infections_here';
let _colorScale = 'reds';
let _logScale   = true;
let _invertScale = false;
// Normalisation mode for color + height gradients:
//   'live'       — current peak across buildings (existing behaviour;
//                  buildings shrink as new infections eclipse old)
//   'cumulative' — running max across the whole run (only ever grows;
//                  early infections stay at full size after they peak)
//   'capacity'   — value / building capacity (each building shows its
//                  own infection rate, 0..1 — buildings approach max
//                  size only when their full population is infected)
//   'total'      — value / city-wide total (consistent absolute scale
//                  across the city; small buildings stay small)
let _normMode    = 'live';

export const getColorMode  = () => _colorMode;
export const getHeightMode = () => _heightMode;
export const getColorScale = () => _colorScale;
export const getLogScale   = () => _logScale;
export const getInvertScale = () => _invertScale;
export const getNormMode    = () => _normMode;

// ── Build ─────────────────────────────────────────────────────────────────────

/**
 * Build the legend controls inside #legendBody.
 *
 * @param {object}   _cityLayout — unused (reserved)
 * @param {Function} onVizChange — called whenever any viz selector changes
 */
export function buildLegend(_cityLayout, onVizChange) {
  const body = document.getElementById('legendBody');
  body.innerHTML = '';

  // ── Selectors ──────────────────────────────────────────────────────────────
  const controls = document.createElement('div');
  controls.style.cssText = 'margin-bottom:10px';

  const selColorRow  = _makeRow(t('color_by'),  'viz-color-select',
    ['zone', 'none', 'infection', 'infections_here', 'exposed', 'susceptible', 'recovered', 'prevalence', 'population', 'deaths', 'income', 'origin'],
    [t('zone_type'), t('none_grey'), t('infection'), t('infections_here'), t('exposed'), t('susceptible'), t('recovered'), t('prevalence'), t('population'), t('deaths'), t('income'), t('country_of_birth')]);
  const selHeightRow = _makeRow(t('height_by'), 'viz-height-select',
    ['zone', 'infection', 'infections_here', 'exposed', 'susceptible', 'recovered', 'prevalence', 'population', 'deaths', 'income'],
    [t('zone_type_real'), t('infection'), t('infections_here'), t('exposed'), t('susceptible'), t('recovered'), t('prevalence'), t('population'), t('deaths'), t('income')]);
  const selNormRow   = _makeRow(t('normalize_by'), 'viz-norm-select',
    ['live', 'cumulative', 'monotonic', 'capacity', 'total'],
    [t('norm_live'), t('norm_cumulative'), t('norm_monotonic'), t('norm_capacity'), t('norm_total')]);
  const selScaleRow  = _makeRow(t('scale'),     'viz-scale-select',
    [
      'plasma', 'viridis', 'inferno', 'magma', 'cividis', 'turbo',
      'bluered', 'coolwarm', 'rdylgn', 'spectral',
      'blues', 'reds', 'greens', 'oranges', 'purples', 'greys',
      'hot',  'cool',
    ],
    [
      'Plasma', 'Viridis', 'Inferno', 'Magma', 'Cividis', 'Turbo',
      'Blue → Red', 'Cool → Warm', 'Green → Red', 'Spectral',
      'Blues', 'Reds', 'Greens', 'Oranges', 'Purples', 'Greys',
      'Hot', 'Cool',
    ]);

  selScaleRow.style.display = 'none';   // hidden unless gradient mode

  // Log-scale + Invert toggles share one consistent two-checkbox row.
  const logRow = document.createElement('div');
  logRow.className = 'viz-row viz-row-toggles';
  logRow.style.display = 'none';   // hidden unless gradient/infection mode
  logRow.innerHTML = `
    <label class="viz-toggle">
      <input type="checkbox" id="viz-log-toggle">
      <span>${t('log_scale')}</span>
    </label>
    <label class="viz-toggle">
      <input type="checkbox" id="viz-invert-toggle">
      <span>${t('invert_scale')}</span>
    </label>`;

  // Height on top, Color below — the user reads "shape first, then
  // paint" which matches how the eye parses the scene.
  controls.appendChild(selHeightRow);
  controls.appendChild(selColorRow);
  controls.appendChild(selNormRow);
  controls.appendChild(selScaleRow);
  controls.appendChild(logRow);
  body.appendChild(controls);

  // ── Divider ────────────────────────────────────────────────────────────────
  const hr = document.createElement('div');
  hr.style.cssText = 'border-top:1px solid #1e293b;margin-bottom:8px';
  body.appendChild(hr);

  // ── Zone swatches ──────────────────────────────────────────────────────────
  const grid = document.createElement('div');
  grid.className = 'legend-grid';
  body.appendChild(grid);

  for (const e of GROUP_ENTRIES) {
    const hex = GROUP_COLORS[e.key];
    if (typeof hex !== 'number') continue;
    const item = document.createElement('div');
    item.className = 'legend-item';
    item.innerHTML = `
      <div class="legend-dot" style="background:${_hex(hex)};border:1px solid #334155"></div>
      ${typeof e.label === 'function' ? e.label() : e.label}`;
    grid.appendChild(item);
  }

  // ── Gradient bar ───────────────────────────────────────────────────────────
  const gradBar = document.createElement('div');
  gradBar.className = 'legend-grad-bar';
  gradBar.style.display = 'none';

  const gradCanvas = document.createElement('canvas');
  gradCanvas.height = 18;
  gradCanvas.style.cssText = 'width:100%;border-radius:3px;display:block';
  gradBar.appendChild(gradCanvas);

  const gradLabels = document.createElement('div');
  gradLabels.style.cssText = 'display:flex;justify-content:space-between;font-size:0.68rem;color:#cbd5e1;margin-top:3px';
  const gradLo = document.createElement('span');
  const gradHi = document.createElement('span');
  gradLabels.appendChild(gradLo);
  gradLabels.appendChild(gradHi);
  gradBar.appendChild(gradLabels);

  body.appendChild(gradBar);

  // ── Origin legend (predominant country of birth) ──────────────────────────
  const originBox = document.createElement('div');
  originBox.style.cssText = 'display:none;flex-direction:column;gap:4px;font-size:0.7rem;color:#cbd5e1;margin-top:6px';
  body.appendChild(originBox);

  function _renderOriginLegend() {
    // Source group metadata from the city layout (set on /api/city load).
    const groups = (window._epiCityOriginGroups) || null;
    if (!groups) { originBox.innerHTML = ''; return; }
    const lang = (window._epiCityLang || 'en');
    originBox.innerHTML = groups.map(g => `
      <div style="display:flex;align-items:center;gap:6px">
        <div style="width:11px;height:11px;border-radius:2px;background:${g.color};border:1px solid #334155"></div>
        <span>${(lang === 'sv' && g.label_sv) || g.label_en}</span>
      </div>`).join('');
  }
  _renderOriginLegend();

  // ── Event listeners ────────────────────────────────────────────────────────

  const selColor  = document.getElementById('viz-color-select');
  const selHeight = document.getElementById('viz-height-select');
  const selNorm   = document.getElementById('viz-norm-select');
  const selScale  = document.getElementById('viz-scale-select');

  // Reflect module-level defaults in the select DOM
  selColor.value  = _colorMode;
  selHeight.value = _heightMode;
  selNorm.value   = _normMode;
  selScale.value  = _colorScale;
  selNorm.addEventListener('change', () => {
    _normMode = selNorm.value;
    onVizChange();
  });

  function _updateSwatches() {
    // Group swatches are shown in both "zone" and "none" modes — in
    // "none" the palette is still the single source of truth the
    // user needs to understand building colours on the map, just
    // with everything in neutral grey instead of zone colours.
    const GRAD_MODES = new Set(['infection','infections_here','exposed','susceptible','recovered','prevalence','population','deaths','income']);
    const isFlat     = _colorMode === 'zone' || _colorMode === 'none';
    const isGradient = GRAD_MODES.has(_colorMode);
    const isOrigin   = _colorMode === 'origin';
    const showLog    = isGradient || GRAD_MODES.has(_heightMode);

    grid.style.display        = isFlat     ? '' : 'none';
    gradBar.style.display     = isGradient ? '' : 'none';
    originBox.style.display   = isOrigin   ? 'flex' : 'none';
    selScaleRow.style.display = isGradient ? '' : 'none';
    logRow.style.display      = showLog    ? '' : 'none';

    if (isGradient) {
      _drawGradient(gradCanvas, _colorScale);
      const lbl = GRAD_LABELS[_colorMode] ?? { lo: () => '0%', hi: () => '100%' };
      gradLo.textContent = typeof lbl.lo === 'function' ? lbl.lo() : lbl.lo;
      gradHi.textContent = typeof lbl.hi === 'function' ? lbl.hi() : lbl.hi;
    }
    if (isOrigin) _renderOriginLegend();
  }

  selColor.addEventListener('change', () => {
    _colorMode = selColor.value;
    _updateSwatches();
    onVizChange();
  });

  selHeight.addEventListener('change', () => {
    _heightMode = selHeight.value;
    _updateSwatches();
    onVizChange();
  });

  const logToggle = document.getElementById('viz-log-toggle');
  logToggle.checked = _logScale;
  logToggle.addEventListener('change', () => {
    _logScale = logToggle.checked;
    onVizChange();
  });

  const invertToggle = document.getElementById('viz-invert-toggle');
  invertToggle.checked = _invertScale;
  invertToggle.addEventListener('change', () => {
    _invertScale = invertToggle.checked;
    if (_colorMode !== 'zone') _drawGradient(gradCanvas, _colorScale);
    onVizChange();
  });

  selScale.addEventListener('change', () => {
    _colorScale = selScale.value;
    if (_colorMode !== 'zone') _drawGradient(gradCanvas, _colorScale);
    onVizChange();
  });

  // Apply the default-mode visibility (scale + log rows, gradient bar)
  _updateSwatches();
}

// ── Private ───────────────────────────────────────────────────────────────────

function _makeRow(labelText, selectId, values, labels) {
  const row = document.createElement('div');
  row.className = 'viz-row';

  const lbl = document.createElement('span');
  lbl.className   = 'viz-row-label';
  lbl.textContent = labelText;
  row.appendChild(lbl);

  const sel = document.createElement('select');
  sel.id        = selectId;
  sel.className = 'viz-select';
  for (let i = 0; i < values.length; i++) {
    const opt       = document.createElement('option');
    opt.value       = values[i];
    opt.textContent = labels[i];
    sel.appendChild(opt);
  }
  row.appendChild(sel);

  return row;
}

/**
 * Draw a horizontal gradient onto a canvas element.
 * @param {HTMLCanvasElement} canvas
 * @param {string} scale — 'plasma' | 'viridis'
 */
function _drawGradient(canvas, scale) {
  const W = canvas.offsetWidth || 160;
  canvas.width = W;
  const ctx = canvas.getContext('2d');

  for (let px = 0; px < W; px++) {
    let t = px / (W - 1);
    if (_invertScale) t = 1 - t;
    ctx.fillStyle = sampleScaleCSS(t, scale);
    ctx.fillRect(px, 0, 1, canvas.height);
  }
}
