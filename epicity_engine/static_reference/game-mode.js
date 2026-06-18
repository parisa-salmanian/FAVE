/**
 * game-mode.js — Mode + difficulty picker on the macro view.
 *
 * Flow:
 *   1. Macro view renders a 2-up card row: Sandbox · Game.
 *   2. Picking Game expands an inline difficulty row (Easy / Normal / Hard).
 *   3. Every change posts /api/game_mode so the backend remembers the
 *      selection. The next /api/select_city call applies it.
 *   4. The body element gets `.game-mode` while game is selected — CSS in
 *      style.css uses that to hide the patient-zero pill, lock parameter
 *      sliders, and reveal the HUD container in the city view.
 *
 * Sandbox mode is the legacy free-customise behaviour. Picking it is a
 * full reset of game state on the backend.
 */

import { t, onLangChange } from './i18n.js';

let _state = {
  mode:       'sandbox',
  difficulty: 'normal',
};

const DIFF_LABELS = () => ({
  easy:   t('gm_easy'),
  normal: t('gm_normal'),
  hard:   t('gm_hard'),
});

const DIFF_DESCS = () => ({
  easy:   t('gm_easy_desc'),
  normal: t('gm_normal_desc'),
  hard:   t('gm_hard_desc'),
});

/**
 * Initialise the mode picker. Must be called after macro.js has rendered
 * the intro side-panel (we anchor under it). Idempotent — safe to call
 * twice on language switch.
 */
let _langSubscribed = false;

export async function initGameMode() {
  await _refreshFromServer();
  _renderUI();
  _applyBodyClass();
  // Re-render text when the user flips the language toggle.
  if (!_langSubscribed) {
    onLangChange(() => _renderUI());
    _langSubscribed = true;
  }
}

/** Returns 'sandbox' or 'game'. */
export function currentMode() {
  return _state.mode;
}

/** Returns the difficulty key. */
export function currentDifficulty() {
  return _state.difficulty;
}

async function _refreshFromServer() {
  try {
    const r = await fetch('/api/game_mode');
    if (!r.ok) return;
    const j = await r.json();
    if (j && j.mode)       _state.mode       = j.mode;
    if (j && j.difficulty) _state.difficulty = j.difficulty;
  } catch (_) { /* offline mode picker still functional */ }
}

async function _push() {
  try {
    await fetch('/api/game_mode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_state),
    });
  } catch (_) {}
}

function _applyBodyClass() {
  document.body.classList.toggle('game-mode', _state.mode === 'game');
  document.body.dataset.difficulty = _state.difficulty;
}

function _renderUI() {
  // Insert into the intro side panel (left column) under the steps.
  // Falls back to body if the side panel isn't present yet.
  const host = document.querySelector('.intro-sidepanel') || document.body;

  // Remove any previous render so language switches don't stack copies.
  host.querySelectorAll('.gm-block').forEach(n => n.remove());

  const block = document.createElement('div');
  block.className = 'gm-block';
  const labels = DIFF_LABELS();
  const descs  = DIFF_DESCS();
  block.innerHTML = `
    <div class="gm-title">${t('gm_mode')}</div>
    <div class="gm-mode-row">
      <button class="gm-mode-card ${_state.mode === 'sandbox' ? 'on' : ''}" data-mode="sandbox" type="button">
        <span class="gm-mode-ico">🧪</span>
        <span class="gm-mode-name">${t('gm_sandbox')}</span>
        <span class="gm-mode-sub">${t('gm_sandbox_sub')}</span>
      </button>
      <button class="gm-mode-card ${_state.mode === 'game' ? 'on' : ''}" data-mode="game" type="button">
        <span class="gm-mode-ico">🎮</span>
        <span class="gm-mode-name">${t('gm_game')}</span>
        <span class="gm-mode-sub">${t('gm_game_sub')}</span>
      </button>
    </div>
    <div class="gm-diff-row" ${_state.mode === 'game' ? '' : 'hidden'}>
      <div class="gm-diff-label">${t('gm_difficulty')}</div>
      <div class="gm-diff-pips">
        ${['easy','normal','hard'].map(d => `
          <button class="gm-diff-pip ${_state.difficulty === d ? 'on' : ''}" data-diff="${d}" type="button" title="${descs[d]}">
            ${labels[d]}
          </button>
        `).join('')}
      </div>
      <div class="gm-diff-desc">${descs[_state.difficulty] || ''}</div>
    </div>
  `;
  // Always slot the Mode block ABOVE the Starting-season block (which
  // was rendered first or last shouldn't matter — Season has to read
  // visually as the "next step" beneath Mode). When the user clicks a
  // mode / difficulty button, _renderUI removes the previous gm-block
  // and re-inserts; without this insertBefore the new gm-block would
  // appendChild past the existing sp-block, putting Season above Mode.
  const seasonBlock = host.querySelector('.sp-block');
  if (seasonBlock) host.insertBefore(block, seasonBlock);
  else host.appendChild(block);

  // Mode toggle
  block.querySelectorAll('[data-mode]').forEach(btn => {
    btn.addEventListener('click', async () => {
      _state.mode = btn.dataset.mode;
      await _push();
      _renderUI();
      _applyBodyClass();
    });
  });

  // Difficulty pips
  block.querySelectorAll('[data-diff]').forEach(btn => {
    btn.addEventListener('click', async () => {
      _state.difficulty = btn.dataset.diff;
      await _push();
      _renderUI();
      _applyBodyClass();
    });
  });
}
