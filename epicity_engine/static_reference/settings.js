/**
 * settings.js — Model parameter sliders panel.
 * Extracted from ui.js. Manages the ⚙ gear button and settings panel DOM.
 */

import { t } from './i18n.js';

// References to the "Pick patient zero" widgets, populated when the
// sim-mode block is built. ui.js drives them through the helpers below.
let _pickCheckbox = null;
let _pickHintEl   = null;

/** Whether the "Pick patient zero" checkbox is currently checked. */
export function isManualPickChecked() {
  return !!(_pickCheckbox && _pickCheckbox.checked);
}

/**
 * Programmatically tick or untick the checkbox without firing its change
 * handler — used by the Play-fallback path in ui.js.
 */
export function setManualPickChecked(checked) {
  if (_pickCheckbox) _pickCheckbox.checked = !!checked;
}

/** Update the small hint line under the checkbox. Pass '' to hide. */
export function setManualPickHint(text) {
  if (!_pickHintEl) return;
  _pickHintEl.textContent = text || '';
  _pickHintEl.style.display = text ? 'block' : 'none';
}

// Slider definitions — each entry describes one model parameter
const PARAM_DEFS = [
  {
    label: () => t('transmission'),
    min: 0.05, max: 1.0, step: 0.01,
    toDisplay: v => `β = ${parseFloat(v).toFixed(2)}`,
    toPayload: v => ({ beta: parseFloat(v) }),
    initSlider: p => p.beta,
  },
  {
    label: () => t('incubation_period'),
    min: 2, max: 14, step: 1,
    toDisplay: v => t('n_days', { n: v }),
    toPayload: v => ({ sigma: 1 / parseInt(v, 10) }),
    initSlider: p => Math.round(1 / p.sigma),
  },
  {
    label: () => t('infectious_period'),
    min: 3, max: 21, step: 1,
    toDisplay: v => t('n_days', { n: v }),
    toPayload: v => ({ gamma: 1 / parseInt(v, 10) }),
    initSlider: p => Math.round(1 / p.gamma),
  },
  {
    label: () => t('case_fatality'),
    min: 0.0, max: 5.0, step: 0.1,
    toDisplay: v => `${parseFloat(v).toFixed(1)}%`,
    toPayload: v => ({ mu: parseFloat(v) / 100 }),
    initSlider: p => +(p.mu * 100).toFixed(1),
  },
  {
    label: () => t('immunity_waning'),
    min: 0, max: 365, step: 1,
    toDisplay: v => v == 0 ? t('off_permanent') : t('n_days', { n: v }),
    toPayload: v => ({ waning_days: parseInt(v, 10) }),
    initSlider: p => Math.max(0, p.waning_days),
  },
];

// ── Difficulty presets ──────────────────────────────────────────────────────
// Each preset sets all 5 model parameters at once for a quick scenario switch.
const PRESETS = [
  {
    name: () => t('preset_mild'),
    params: { beta: 0.18, sigma: 0.33, gamma: 0.14, mu: 0.001, waning_days: 90 },
    desc: () => t('preset_mild_desc'),
  },
  {
    name: () => t('preset_moderate'),
    params: { beta: 0.32, sigma: 0.20, gamma: 0.10, mu: 0.008, waning_days: 13 },
    desc: () => t('preset_mod_desc'),
  },
  {
    name: () => t('preset_severe'),
    params: { beta: 0.65, sigma: 0.14, gamma: 0.05, mu: 0.035, waning_days: 180 },
    desc: () => t('preset_sev_desc'),
  },
];

/**
 * Build (or rebuild) the parameter sliders inside #settingsPanel.
 * Called on init and again after Reset to sync sliders with server defaults.
 *
 * @param {{ beta, sigma, gamma, mu, waning_days }} params
 * @param {{ stochastic, random_patient_zero, seed, manual_patient_zero_idx }} [mode]
 * @param {Function} [onModeChange]   called with the new state after mode POST
 * @param {Function} [onPickToggle]   called with `checked: bool` when the user
 *                                    toggles the "Pick patient zero" checkbox
 */
export function buildSettingsPanel(params, mode, onModeChange, onPickToggle) {
  const panel = document.getElementById('settingsPop');
  const title = panel.querySelector('.sp-title');
  panel.innerHTML = '';
  panel.appendChild(title);

  // ── Simulation-mode block (renders before the sliders) ─────────────────────
  if (mode) _buildSimModeBlock(panel, mode, onModeChange, onPickToggle);

  for (const def of PARAM_DEFS) {
    const initVal = def.initSlider(params);

    const row = document.createElement('div');
    row.className = 'sp-row';

    const label = document.createElement('span');
    label.className   = 'sp-label';
    label.textContent = typeof def.label === 'function' ? def.label() : def.label;

    const slider = document.createElement('input');
    slider.type      = 'range';
    slider.className = 'sp-slider';
    slider.min       = def.min;
    slider.max       = def.max;
    slider.step      = def.step;
    slider.value     = initVal;

    const valEl = document.createElement('span');
    valEl.className   = 'sp-val';
    valEl.textContent = def.toDisplay(initVal);

    slider.addEventListener('input', () => {
      valEl.textContent = def.toDisplay(slider.value);
    });

    slider.addEventListener('change', () => {
      fetch('/api/params', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(def.toPayload(slider.value)),
      });
    });

    row.appendChild(label);
    row.appendChild(slider);
    row.appendChild(valEl);
    panel.appendChild(row);
  }

  // ── Preset buttons ──────────────────────────────────────────────────────
  // Presenter-mode hide: the Mild / Moderate / Severe pathogen presets
  // are hidden from the panel during the specialist meeting so the
  // model parameters can't be silently swapped behind the user's back.
  // The buttons stay in the DOM (display:none on their containers) so
  // the moderate-default state is still in effect. Remove the inline
  // styles to restore the picker.
  const presetLabel = document.createElement('div');
  presetLabel.style.cssText = 'font-size:0.65rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;margin-top:10px;margin-bottom:5px;display:none';
  presetLabel.textContent = t('scenario_presets');
  panel.appendChild(presetLabel);

  const presetRow = document.createElement('div');
  presetRow.style.cssText = 'display:none;gap:6px;margin-bottom:4px';

  // Detect which preset matches the current params. If none match
  // (e.g. user tweaked a slider manually), default to Moderate (index 1).
  const _presetMatch = (p, preset) =>
    Math.abs(p.beta - preset.beta) < 0.005 &&
    Math.abs(p.sigma - preset.sigma) < 0.005 &&
    Math.abs(p.gamma - preset.gamma) < 0.005 &&
    Math.abs(p.mu - preset.mu) < 0.002 &&
    Math.abs((p.waning_days || 0) - (preset.waning_days || 0)) < 1;

  let activeIdx = PRESETS.findIndex(pr => _presetMatch(params, pr.params));
  if (activeIdx < 0) activeIdx = 1;  // default to Moderate

  const presetBtns = [];

  const _stylePresetBtn = (btn, active) => {
    if (active) {
      btn.style.background = '#1d4ed8';
      btn.style.borderColor = '#3b82f6';
      btn.style.color = '#fff';
      btn.style.fontWeight = '700';
    } else {
      btn.style.background = '#1e293b';
      btn.style.borderColor = '#334155';
      btn.style.color = '#cbd5e1';
      btn.style.fontWeight = '400';
    }
  };

  for (let pi = 0; pi < PRESETS.length; pi++) {
    const preset = PRESETS[pi];
    const isActive = (pi === activeIdx);
    const btn = document.createElement('button');
    btn.textContent = typeof preset.name === 'function' ? preset.name() : preset.name;
    btn.title = typeof preset.desc === 'function' ? preset.desc() : preset.desc;
    btn.style.cssText = `flex:1;padding:5px 0;font-size:0.68rem;border-radius:4px;
      border:1px solid #334155;cursor:pointer;transition:all 0.15s`;
    _stylePresetBtn(btn, isActive);
    btn.addEventListener('mouseenter', () => {
      if (btn.style.background !== 'rgb(29, 78, 216)') btn.style.background = '#334155';
    });
    btn.addEventListener('mouseleave', () => {
      // Re-apply correct style based on active state
      const idx = presetBtns.indexOf(btn);
      _stylePresetBtn(btn, btn.dataset.active === '1');
    });
    btn.dataset.active = isActive ? '1' : '0';
    btn.addEventListener('click', async () => {
      await fetch('/api/params', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(preset.params),
      });
      // Rebuild sliders to reflect the new values
      buildSettingsPanel(preset.params, mode, onModeChange, onPickToggle);
    });
    presetBtns.push(btn);
    presetRow.appendChild(btn);
  }
  panel.appendChild(presetRow);

  // ── Pathogen presets (real-world calibrations) ─────────────────────────
  _buildPathogenPresets(panel, params, mode, onModeChange, onPickToggle);
}

let _pathogenPresetsCache = null;

// Presenter-mode hide: the real-world pathogen calibrations (COVID-19,
// SARS 2003, H1N1, Ebola, Measles, etc.) are hidden from the Settings
// popover during the specialist meeting so the demo doesn't get
// derailed into "is that calibration peer-reviewed?" questions. The
// section is built as a section header + grid + blurb, all appended
// to the panel; an early return here skips the whole block. The
// backend /api/presets endpoint is left alone — flipping this flag
// back to false restores the section without any other changes.
const HIDE_PATHOGEN_PRESETS = true;

async function _buildPathogenPresets(panel, params, mode, onModeChange, onPickToggle) {
  if (HIDE_PATHOGEN_PRESETS) return;
  // Lazy-fetch — only one network call per session.
  if (!_pathogenPresetsCache) {
    try {
      _pathogenPresetsCache = await fetch('/api/presets').then(r => r.json());
    } catch { return; }
  }
  const presets = _pathogenPresetsCache;

  // Visual separator from the difficulty preset row above so the section
  // is easy to spot at a glance.
  const sep = document.createElement('div');
  sep.style.cssText = 'border-top:1px solid #1e293b;margin-top:12px;padding-top:8px';
  panel.appendChild(sep);

  const lbl = document.createElement('div');
  lbl.style.cssText = 'font-size:0.7rem;color:#cbd5e1;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center';
  lbl.innerHTML = `<span>🧬 ${t('pathogen_presets')}</span>
    <a href="#" id="sp-preset-sources" style="color:#64748b;font-size:0.65rem;text-transform:none;letter-spacing:0;font-weight:400;text-decoration:none">${t('preset_sources')}</a>`;
  panel.appendChild(lbl);

  const grid = document.createElement('div');
  grid.style.cssText = 'display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:4px';

  const blurb = document.createElement('div');
  blurb.id = 'sp-pathogen-blurb';
  blurb.style.cssText = 'margin-top:6px;padding:6px 8px;background:#0b1120;border-left:2px solid #3b82f6;font-size:0.7rem;line-height:1.5;color:#94a3b8;border-radius:0 4px 4px 0;display:none';

  // Highlight the current params if they match a pathogen preset.
  let activePid = null;
  for (const p of presets) {
    if (Math.abs(params.beta - p.params.beta) < 0.005 &&
        Math.abs(params.sigma - p.params.sigma) < 0.005 &&
        Math.abs(params.gamma - p.params.gamma) < 0.005 &&
        Math.abs(params.mu - p.params.mu) < 0.002 &&
        Math.abs((params.waning_days || 0) - (p.params.waning_days || 0)) < 1) {
      activePid = p.id;
      break;
    }
  }

  for (const preset of presets) {
    const btn = document.createElement('button');
    const isActive = preset.id === activePid;
    btn.textContent = `${preset.icon} ${preset.short}` + (preset.default ? ' ★' : '');
    btn.title = (window._epiCityLang === 'sv' ? preset.label_sv : preset.label_en);
    btn.style.cssText = `padding:5px 6px;font-size:0.68rem;border-radius:4px;
      border:1px solid #334155;cursor:pointer;transition:all 0.15s;text-align:center;
      background:${isActive ? '#1d4ed8' : '#1e293b'};
      color:${isActive ? '#fff' : '#cbd5e1'};
      font-weight:${isActive ? '700' : '400'}`;
    btn.addEventListener('click', async () => {
      try {
        const resp = await fetch(`/api/preset/${preset.id}`, { method: 'POST' })
          .then(r => r.json());
        // Rebuild sliders with the new params + re-render preset row.
        buildSettingsPanel(resp.params, mode, onModeChange, onPickToggle);
      } catch (e) { console.error('preset apply failed', e); }
    });
    btn.addEventListener('mouseenter', () => _showPathogenBlurb(preset));
    grid.appendChild(btn);
  }
  panel.appendChild(grid);
  panel.appendChild(blurb);

  // If a preset is currently active, show its blurb so the user sees why.
  if (activePid) {
    const cur = presets.find(p => p.id === activePid);
    if (cur) _showPathogenBlurb(cur);
  }

  panel.querySelector('#sp-preset-sources').addEventListener('click', e => {
    e.preventDefault();
    // Open the About modal on the References tab — handled in ui.js
    window.dispatchEvent(new CustomEvent('epicity:open-about', { detail: { tab: 'references' } }));
  });
}

function _showPathogenBlurb(preset) {
  const blurb = document.getElementById('sp-pathogen-blurb');
  if (!blurb) return;
  const lang = window._epiCityLang || 'en';
  const txt  = (lang === 'sv' ? preset.blurb_sv : preset.blurb_en) || '';
  const pp   = preset.params;
  const sig  = pp.sigma > 0 ? (1 / pp.sigma).toFixed(1) : '∞';
  const gam  = pp.gamma > 0 ? (1 / pp.gamma).toFixed(1) : '∞';
  const label = lang === 'sv' ? preset.label_sv : preset.label_en;
  blurb.innerHTML = `
    <div style="color:#cbd5e1;font-weight:600;margin-bottom:3px">${label}</div>
    <div style="color:#94a3b8;margin-bottom:3px">
      β = ${pp.beta.toFixed(2)} · ${t('incubation_period')} ${sig}d ·
      ${t('infectious_period')} ${gam}d · CFR ${(pp.mu*100).toFixed(1)}% ·
      ${t('immunity_waning')} ${pp.waning_days}d
    </div>
    <div style="color:#64748b">${txt}</div>`;
  blurb.style.display = 'block';
}

/**
 * Build the "Simulation mode" block at the top of the settings panel.
 *
 * The block lets the user switch between deterministic and stochastic
 * dynamics, toggle random patient zero, and pick an RNG seed. All three
 * controls post to /api/simulation_mode, which resets the engine — the
 * returned state is fed back to the caller so the HUD updates instantly.
 */
function _buildSimModeBlock(panel, mode, onModeChange, onPickToggle) {
  // Pick-patient-zero affordance lives in the top bar now (see ui.js).
  // This block is just: section header → mode radios → seed row → hint.
  const wrap = document.createElement('div');
  wrap.className = 'sp-mode';

  // Presenter-mode hide: the deterministic / stochastic radio row is
  // hidden from the UI to avoid questions during the specialist
  // meeting. The radios stay in the DOM (display:none) so apply()
  // still reads a sensible value — whichever was the stochastic flag
  // when the session started. Remove the inline style to restore the
  // picker.
  wrap.innerHTML = `
    <div class="sp-section-h">${t('simulation_mode')}</div>
    <div class="sp-mode-row" id="sp-mode-radios" style="display:none">
      <label><input type="radio" name="sp-dyn" value="det" ${mode.stochastic ? '' : 'checked'}>${t('deterministic')}</label>
      <label><input type="radio" name="sp-dyn" value="sto" ${mode.stochastic ? 'checked' : ''}>${t('stochastic')}</label>
    </div>
    <div class="sp-mode-row" id="sp-seed-row">
      <label><input type="checkbox" id="sp-rand-pz" ${mode.random_patient_zero ? 'checked' : ''}>${t('random')}</label>
      <span class="sp-mode-lbl">${t('seed')}</span>
      <input type="number" id="sp-seed" value="${mode.seed}" step="1">
      <button id="sp-rand-seed" type="button" title="${t('randomise_seed')}">🎲</button>
    </div>
    <div class="sp-mode-hint">Changes reset the simulation. Seed controls reproducibility of stochastic runs and of the random patient-zero pick.</div>
  `;

  // Hint element kept around so ui.js can still show pick-mode hints
  // (e.g. "Click a residential building"). Lives at the bottom of the
  // mode block, above the parameter sliders.
  const pickHint = document.createElement('div');
  pickHint.id = 'sp-pick-hint';
  pickHint.className = 'sp-mode-hint sp-pick-hint';
  pickHint.style.display = 'none';
  wrap.appendChild(pickHint);

  panel.appendChild(wrap);

  const dynRow   = wrap.querySelector('#sp-mode-radios');
  const randCb   = wrap.querySelector('#sp-rand-pz');
  const seedIn   = wrap.querySelector('#sp-seed');
  const seedBtn  = wrap.querySelector('#sp-rand-seed');

  // The pick-checkbox now lives in the top bar (#btnPickZero); ui.js owns
  // its toggle. The settings panel only exposes the hint element so ui.js
  // can flash messages from anywhere.
  _pickCheckbox = null;
  _pickHintEl   = pickHint;

  const apply = async () => {
    const stochastic = dynRow.querySelector('input[value="sto"]').checked;
    const seedNum    = parseInt(seedIn.value, 10);
    const body = {
      stochastic,
      random_patient_zero: randCb.checked,
      seed: Number.isFinite(seedNum) ? seedNum : 42,
    };
    try {
      const state = await fetch('/api/simulation_mode', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(body),
      }).then(r => r.json());
      onModeChange?.(state);
    } catch { /* ignore — UI already reflects the intent */ }
  };

  dynRow.querySelectorAll('input[name="sp-dyn"]').forEach(r =>
    r.addEventListener('change', apply));
  randCb.addEventListener('change', apply);
  seedIn.addEventListener('change', apply);
  seedBtn.addEventListener('click', e => {
    e.preventDefault();
    seedIn.value = Math.floor(Math.random() * 1_000_000);
    apply();
  });
}

/**
 * Wire the settings panel to open on hover (CSS handles the show/hide
 * via .header-hover-wrap:hover > .settings-panel).
 * Swallow pointer events inside the panel so slider interactions
 * don't fall through to the map.
 * Call once after DOMContentLoaded.
 */
export function bindSettingsToggle() {
  const panel = document.getElementById('settingsPop');
  if (!panel) return;
  // Swallow events so slider/checkbox interactions don't propagate
  ['click', 'mousedown', 'mouseup', 'pointerdown', 'pointerup'].forEach(ev => {
    panel.addEventListener(ev, e => e.stopPropagation());
  });
}

/** Programmatically close the settings panel (e.g. on Play). */
export function closeSettingsPanel() {
  const panel = document.getElementById('settingsPop');
  if (panel) panel.classList.remove('open');
}
