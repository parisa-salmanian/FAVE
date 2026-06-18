/**
 * season-picker.js — Pin the starting season before entering a city.
 *
 * Lives next to the mode/difficulty card on the macro view. The default
 * is "random" (legacy behaviour: each reset rolls a fresh day-of-year).
 * Picking a specific season locks day 0 to that season's midpoint so
 * the climate model boots into the chosen weather pattern.
 *
 * Persisted on the backend via /api/start_season — applied to every
 * subsequent city entry until changed.
 */

import { t, onLangChange } from './i18n.js';

const SEASONS = () => [
  { id: 'random', label: t('sp_season_random'), ico: '🎲' },
  { id: 'winter', label: t('season_winter'),    ico: '❄️' },
  { id: 'spring', label: t('season_spring'),    ico: '🌱' },
  { id: 'summer', label: t('season_summer'),    ico: '☀️' },
  { id: 'autumn', label: t('season_autumn'),    ico: '🍂' },
];

let _state = { season: 'random' };

let _langSubscribed = false;

export async function initSeasonPicker() {
  await _refreshFromServer();
  _renderUI();
  if (!_langSubscribed) {
    onLangChange(() => _renderUI());
    _langSubscribed = true;
  }
}

async function _refreshFromServer() {
  try {
    const r = await fetch('/api/start_season');
    if (r.ok) {
      const j = await r.json();
      _state.season = j.season || 'random';
    }
  } catch (_) {}
}

async function _push() {
  try {
    await fetch('/api/start_season', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ season: _state.season }),
    });
  } catch (_) {}
}

function _renderUI() {
  const host = document.querySelector('.intro-sidepanel') || document.body;
  host.querySelectorAll('.sp-block').forEach(n => n.remove());

  const block = document.createElement('div');
  block.className = 'sp-block';
  const seasons = SEASONS();
  block.innerHTML = `
    <div class="sp-block-h">${t('sp_starting_season')}</div>
    <div class="sp-block-row">
      ${seasons.map(s => `
        <button class="sp-season ${_state.season === s.id ? 'on' : ''}" data-season="${s.id}" type="button" title="${s.label}">
          <span class="sp-season-ico">${s.ico}</span>
          <span class="sp-season-lbl">${s.label}</span>
        </button>`).join('')}
    </div>
    <div class="sp-block-foot">${_footHint(_state.season)}</div>
  `;
  host.appendChild(block);

  block.querySelectorAll('[data-season]').forEach(btn => {
    btn.addEventListener('click', async () => {
      _state.season = btn.dataset.season;
      await _push();
      _renderUI();
    });
  });
}

function _footHint(season) {
  switch (season) {
    case 'winter': return t('sp_winter_hint');
    case 'spring': return t('sp_spring_hint');
    case 'summer': return t('sp_summer_hint');
    case 'autumn': return t('sp_autumn_hint');
    case 'random': default:
      return t('sp_random_hint');
  }
}
