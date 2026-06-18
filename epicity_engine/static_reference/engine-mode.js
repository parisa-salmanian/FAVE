/**
 * engine-mode.js — Epidemic engine picker on the macro view.
 *
 * Flow:
 *   1. Macro view renders a 2-up card row: Compartmental · Agent-based.
 *   2. Picking Agent-based reveals population_scale (1, 2, 5, 10) and
 *      time_resolution (1 h / 30 min / 15 min) controls.
 *   3. Every change posts /api/engine_mode so the backend remembers it.
 *      Switching engine kind clears the backend's warm engine pool and
 *      drops the active-city pointer, so the user returns to the city
 *      picker. Tweaking scale/resolution while staying in ABM leaves
 *      warm engines untouched — they pick up the new values on the next
 *      /api/select_city call.
 *   4. The body element gets `.engine-abm` while ABM is selected — CSS
 *      can use that to surface ABM-specific UI later (e.g. a per-agent
 *      inspect button on building click in Phase 2).
 *
 * Borrows the .gm-block / .gm-mode-card / .gm-diff-pip styling already
 * shipped by game-mode.js so this picker reads as a peer of the
 * Sandbox/Game and Starting-season blocks.
 */

import { t, onLangChange } from './i18n.js';

let _state = {
  engine:            'compartmental',
  population_scale:  1,
  time_resolution_h: 1.0,
};

const POP_SCALES = [1, 2, 5, 10];
const RES_HOURS  = [1.0, 0.5, 0.25];

const RES_LABEL = (h) => {
  if (h === 1.0)  return '1 h';
  if (h === 0.5)  return '30 min';
  if (h === 0.25) return '15 min';
  return `${h} h`;
};

let _langSubscribed = false;

// Temporary presenter-mode flag: the macro view no longer exposes the
// engine picker (Compartmental vs Agent-based) to avoid raising
// follow-up questions during the specialist meeting. ABM with default
// scale 1 and 1-hour resolution is force-pushed to the backend at init
// so every city load runs with the same settings. To restore the
// picker, set this to false — _renderUI() will rebuild the block on
// the macro view like before.
const HIDE_ENGINE_PICKER = false;

export async function initEngineMode() {
  if (HIDE_ENGINE_PICKER) {
    // Force the canonical demo configuration regardless of whatever the
    // backend was holding from a previous session. This deliberately
    // ignores _refreshFromServer so a stale localStorage / cookie
    // preference can't fall back to compartmental behind the user's
    // back.
    _state.engine            = 'abm';
    _state.population_scale  = 1;
    _state.time_resolution_h = 1.0;
    await _push();
    _applyBodyClass();
    return;
  }
  await _refreshFromServer();
  _renderUI();
  _applyBodyClass();
  if (!_langSubscribed) {
    onLangChange(() => _renderUI());
    _langSubscribed = true;
  }
}

export function currentEngine()           { return _state.engine; }
export function currentPopulationScale()  { return _state.population_scale; }
export function currentTimeResolutionH()  { return _state.time_resolution_h; }

async function _refreshFromServer() {
  try {
    const r = await fetch('/api/engine_mode');
    if (!r.ok) return;
    const j = await r.json();
    if (j && j.engine)                          _state.engine            = j.engine;
    if (j && typeof j.population_scale  === 'number') _state.population_scale  = j.population_scale;
    if (j && typeof j.time_resolution_h === 'number') _state.time_resolution_h = j.time_resolution_h;
  } catch (_) { /* offline picker still functional */ }
}

async function _push() {
  try {
    await fetch('/api/engine_mode', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(_state),
    });
  } catch (_) {}
}

function _applyBodyClass() {
  document.body.classList.toggle('engine-abm', _state.engine === 'abm');
}

function _renderUI() {
  const host = document.querySelector('.intro-sidepanel') || document.body;
  host.querySelectorAll('.em-block').forEach(n => n.remove());

  const block = document.createElement('div');
  // Borrow gm-block layout — keeps the picker visually consistent with
  // the existing Mode picker without new CSS in Phase 1a.
  block.className = 'em-block gm-block';
  const isAbm = _state.engine === 'abm';

  block.innerHTML = `
    <div class="gm-title">${t('em_engine')}</div>
    <div class="gm-mode-row">
      <button class="gm-mode-card ${!isAbm ? 'on' : ''}" data-engine="compartmental" type="button">
        <span class="gm-mode-ico">🧬</span>
        <span class="gm-mode-name">${t('em_compartmental')}</span>
        <span class="gm-mode-sub">${t('em_compartmental_sub')}</span>
      </button>
      <button class="gm-mode-card ${isAbm ? 'on' : ''}" data-engine="abm" type="button">
        <span class="gm-mode-ico">👥</span>
        <span class="gm-mode-name">${t('em_abm')}</span>
        <span class="gm-mode-sub">${t('em_abm_sub')}</span>
      </button>
    </div>
    <div class="gm-diff-row" ${isAbm ? '' : 'hidden'}>
      <div class="gm-diff-label">${t('em_pop_scale')}</div>
      <div class="gm-diff-pips">
        ${POP_SCALES.map(s => `
          <button class="gm-diff-pip ${_state.population_scale === s ? 'on' : ''}"
                  data-pop="${s}" type="button"
                  title="${t('em_pop_scale_desc', { n: s })}">
            1:${s}
          </button>
        `).join('')}
      </div>
      <div class="gm-diff-label" style="margin-top:6px">${t('em_resolution')}</div>
      <div class="gm-diff-pips">
        ${RES_HOURS.map(h => `
          <button class="gm-diff-pip ${_state.time_resolution_h === h ? 'on' : ''}"
                  data-res="${h}" type="button">
            ${RES_LABEL(h)}
          </button>
        `).join('')}
      </div>
      <div class="gm-diff-desc">${t('em_abm_help')}</div>
    </div>
  `;

  // Slot the Engine block ABOVE the Mode (gm-block) and Season (sp-block)
  // blocks so the engine choice reads as the first decision.
  // Query excludes our own .em-block so we don't anchor to ourselves on
  // re-render — the previous em-block has already been removed above,
  // but the selector is defensive anyway.
  const gmBlock = host.querySelector('.gm-block:not(.em-block)');
  const spBlock = host.querySelector('.sp-block');
  const anchor  = gmBlock || spBlock;
  if (anchor) host.insertBefore(block, anchor);
  else        host.appendChild(block);

  block.querySelectorAll('[data-engine]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const next = btn.dataset.engine;
      if (next === _state.engine) return;
      _state.engine = next;
      await _push();
      _renderUI();
      _applyBodyClass();
    });
  });

  block.querySelectorAll('[data-pop]').forEach(btn => {
    btn.addEventListener('click', async () => {
      _state.population_scale = parseInt(btn.dataset.pop, 10);
      await _push();
      _renderUI();
    });
  });

  block.querySelectorAll('[data-res]').forEach(btn => {
    btn.addEventListener('click', async () => {
      _state.time_resolution_h = parseFloat(btn.dataset.res);
      await _push();
      _renderUI();
    });
  });
}
