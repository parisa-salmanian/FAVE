import { t } from './i18n.js';

/**
 * intervention-gating.js — Time-locked interventions in game mode.
 *
 * Today only vaccination is gated (rolls out on day 30 / 60 / 90 by
 * difficulty). The same machinery generalises: hand any intervention
 * a `days_until_X` field on the game snapshot and add a check below.
 *
 * Responsibilities:
 *   - Stamp the vaccination intervention button with a 🔒 Day N pill
 *     while the rollout is still days away.
 *   - Block clicks on locked buttons (and refuse user toggles in JS,
 *     even though the backend also returns false on early-toggle).
 *   - Reverse the lock the moment day ≥ vaccine_day.
 *
 * Sandbox mode is a no-op: every button is unlocked from day 0.
 */

const LOCK_TARGETS = {
  // intervention id → (gameSnapshot) => { locked, daysLeft }
  vaccination: (game) => ({
    locked:   !!game && !game.vaccine_unlocked,
    daysLeft: (game && game.days_until_vaccine) ?? 0,
  }),
};

/**
 * Apply lock badges to the rendered intervention list. Idempotent —
 * call on every state tick. The intervention DOM is built by
 * ui.js _buildInterventionButtons; this function only reads it.
 *
 * @param {{ game?: { vaccine_unlocked?: boolean, days_until_vaccine?: number } }} state
 */
export function applyGating(state) {
  const game    = state && state.game;
  const inGame  = !!game;
  for (const [ivId, fn] of Object.entries(LOCK_TARGETS)) {
    const row = document.getElementById('iv-' + ivId);
    if (!row) continue;
    if (!inGame) {
      _clearLock(row);
      continue;
    }
    const { locked, daysLeft } = fn(game);
    if (locked) _setLock(row, daysLeft);
    else        _clearLock(row);
  }
}

/** Returns true when this intervention is currently lock-blocked. */
export function isLocked(ivId, state) {
  const game = state && state.game;
  const fn   = LOCK_TARGETS[ivId];
  if (!fn || !game) return false;
  return fn(game).locked;
}

function _setLock(row, daysLeft) {
  row.classList.add('iv-locked');
  let pill = row.querySelector('.iv-lock-pill');
  if (!pill) {
    pill = document.createElement('div');
    pill.className = 'iv-lock-pill';
    row.appendChild(pill);
  }
  const txt = daysLeft <= 0 ? t('vacc_lock_today')
            : daysLeft === 1 ? t('vacc_lock_tomorrow')
            : t('vacc_lock_days', { n: daysLeft });
  pill.innerHTML = `<span class="iv-lock-ico">🔒</span><span>${txt}</span>`;
  pill.title = txt;
}

function _clearLock(row) {
  row.classList.remove('iv-locked');
  const pill = row.querySelector('.iv-lock-pill');
  if (pill) pill.remove();
}
