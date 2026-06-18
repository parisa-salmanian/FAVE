/**
 * stats.js — Live epidemiology statistics panel.
 *
 * Toggled by #btnStats in the header. Renders R₀/R_eff, β_eff with the
 * intervention delta, dynamics (doubling time, growth rate, daily incidence,
 * attack rate), severity (CFR, peak prevalence, hospital load), climate, and
 * the city's country-of-birth breakdown.
 *
 * Tip text on each metric explains the formula and cites the relevant paper.
 */

import { t } from './i18n.js';
import { registerPopup, openPopup } from './popup-manager.js';

const FMT = n => Math.round(n).toLocaleString();

const TIPS = {
  r0:    'R₀ = β/γ — average secondary infections in a fully susceptible population (Liu 2020).',
  reff:  'R_eff = R₀ · S/N — current immunity & interventions applied.',
  beta:  'City-wide transmission rate (β_eff): population-weighted β across non-road buildings, with current interventions and climate applied.',
  delta: 'Δ = β_eff − β_baseline at the same climate. Negative values mean active interventions are reducing β.',
  ar:    'Attack rate: cumulative cases / initial population. Tracks how much of the city has ever been infected.',
  cfr:   'CFR = deaths / cumulative cases (Verity 2020).',
  td:    'Doubling time T_d = ln(2)/r, fitted on a 7-day rolling log-linear regression of I(t) (Wallinga & Lipsitch 2007).',
  gr:    'Daily exponential growth rate r = d/dt ln(I), same window as doubling time.',
  ni:    'New infections in the past day (cumulative cases today − yesterday).',
  peak:  'Peak prevalence so far: highest fraction of the population infectious at any point.',
  hosp:  'Hospital load = current I/N divided by hospital capacity (5 % default). >100 % triggers overload mortality (Kadri 2021).',
  clim:  'Climate β multiplier: cold/dry → higher β (Lowen 2014; Wang 2021).',
};

let _open = false;

/** Wire the header button + ✕ close button. Call once on init. */
export function bindStatsPanel() {
  const btn  = document.getElementById('btnStats');
  const hide = document.getElementById('statsHide');
  if (btn)  btn.addEventListener('click',  () => toggleStats());
  if (hide) hide.addEventListener('click', () => toggleStats(false));
  // Register with the popup manager so opening Settings, About, or Search
  // automatically closes the stats panel (only one popup at a time).
  registerPopup('statsPanel', { hide: () => toggleStats(false) });
  // Default closed — the user opens it on demand from the topbar 📊
  // button. The panel covers the right-edge area where the side-by-side
  // comparison overlay sits, so opening it on every load was getting in
  // the way of the compare workflow.
}

export function isStatsOpen() { return _open; }

export function toggleStats(force = null) {
  const panel = document.getElementById('statsPanel');
  const btn   = document.getElementById('btnStats');
  if (!panel) return;
  _open = force == null ? panel.style.display === 'none' : !!force;
  panel.style.display = _open ? '' : 'none';
  if (btn) {
    btn.style.background  = _open ? '#1e3a5f' : '';
    btn.style.borderColor = _open ? '#3b82f6' : '';
  }
  // When opening, announce so the popup-manager closes the others (POI /
  // Transport / Settings dropdowns, About modal, etc.).
  if (_open) openPopup('statsPanel');
}

function _stat(label, value, valueColor = '#e2e8f0', tip = null) {
  const tipHtml = tip
    ? `<span class="stat-tip" title="${tip.replace(/"/g, '&quot;')}" style="cursor:help;color:#475569;font-size:0.66rem;border:1px solid #334155;border-radius:50%;width:12px;height:12px;display:inline-flex;align-items:center;justify-content:center;line-height:1">ⓘ</span>`
    : '';
  return `
    <div style="background:#0f172a;border-radius:5px;padding:6px 8px">
      <div style="font-size:0.66rem;color:#475569;margin-bottom:1px;display:flex;justify-content:space-between;align-items:center">
        <span>${label}</span>${tipHtml}
      </div>
      <div style="font-size:0.9rem;font-weight:700;color:${valueColor}">${value}</div>
    </div>`;
}

function _section(title, html) {
  return `
    <div style="margin-top:8px">
      <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;color:#475569;margin-bottom:5px">${title}</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px">${html}</div>
    </div>`;
}

/**
 * Re-render the stats panel from a state snapshot. Cheap — called by ui.js
 * inside _updateStats() after every state change.
 */
export function renderStats(state) {
  if (!state) return;
  const body = document.getElementById('statsBody');
  if (!body) return;

  const c   = state.climate || {};
  const r0  = (state.r0 ?? 0).toFixed(2);
  const rE  = (state.r_eff ?? 0).toFixed(2);
  const be  = (state.transmission_rate ?? 0).toFixed(3);
  const bb  = (state.transmission_baseline ?? 0).toFixed(3);
  const bd  = state.transmission_delta ?? 0;
  const bdStr = (bd >= 0 ? '+' : '') + bd.toFixed(3);
  const bdCol = bd < -0.001 ? '#6ee7b7' : (bd > 0.001 ? '#fca5a5' : '#94a3b8');

  const ar   = ((state.attack_rate ?? 0) * 100).toFixed(2) + '%';
  const cfr  = ((state.cfr ?? 0) * 100).toFixed(2) + '%';
  const peak = ((state.peak_prevalence ?? 0) * 100).toFixed(2) + '%';
  const hosp = ((state.hospital_load ?? 0) * 100).toFixed(0) + '%';
  const td   = state.doubling_time;
  const tdStr = td == null
    ? '—'
    : (td > 0 ? `${td.toFixed(1)} d` : `½: ${(-td).toFixed(1)} d`);
  const gr   = state.growth_rate;
  const grStr = gr == null ? '—' : `${(gr * 100).toFixed(1)} %/d`;
  const ni   = state.new_infections == null ? '—' : FMT(state.new_infections);

  const rEffCol = state.r_eff > 1.05 ? '#fca5a5' : state.r_eff < 0.95 ? '#6ee7b7' : '#e2e8f0';
  const grCol   = (gr ?? 0) >  0.01 ? '#fca5a5' : (gr ?? 0) < -0.01 ? '#6ee7b7' : '#e2e8f0';
  const hospCol = (state.hospital_load ?? 0) >= 1 ? '#fca5a5' : '#e2e8f0';

  const temp = c.temperature ?? 0;
  const rh   = c.humidity ?? 0;
  const bm   = c.beta_mult ?? 1;
  const tCol = temp <  0 ? '#93c5fd' : temp > 18 ? '#fca5a5' : '#e2e8f0';
  const bmStr = (bm >= 1 ? '+' : '') + ((bm - 1) * 100).toFixed(0) + '%';
  const bmCol = bm > 1.02 ? '#fca5a5' : bm < 0.98 ? '#6ee7b7' : '#94a3b8';

  // Origin breakdown
  const groups = window._epiCityOriginGroups;
  const tot    = (state.origin_totals || []).reduce((a, b) => a + b, 0);
  const lang   = window._epiCityLang || 'en';
  let originHtml = '';
  if (groups && tot > 0) {
    originHtml = `<div style="margin-top:8px"><div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;color:#475569;margin-bottom:5px">${t('demography_origin')}</div>`;
    for (let i = 0; i < groups.length; i++) {
      const f = (state.origin_totals[i] ?? 0) / tot;
      const w = (f * 100).toFixed(1);
      const lbl = (lang === 'sv' && groups[i].label_sv) || groups[i].label_en;
      const flag = groups[i].flag || '';
      originHtml += `
        <div style="display:flex;align-items:center;gap:6px;font-size:0.7rem;margin-bottom:3px">
          ${flag ? `<span class="country-flag">${flag}</span>` : ''}
          <span style="flex:1;color:#cbd5e1">${lbl}</span>
          <span style="color:#94a3b8;font-variant-numeric:tabular-nums">${w}%</span>
        </div>`;
    }
    originHtml += '</div>';
  }

  body.innerHTML =
    _section(t('stat_reproduction'), `
      ${_stat(t('stat_r0'),  r0, '#cbd5e1', TIPS.r0)}
      ${_stat(t('stat_reff'), rE, rEffCol, TIPS.reff)}
    `) +
    _section(t('stat_transmission'), `
      ${_stat(t('stat_beta_eff'),       be,    '#e2e8f0', TIPS.beta)}
      ${_stat(t('stat_beta_baseline'),  bb,    '#94a3b8',
              'No-intervention baseline at the same climate.')}
      ${_stat(t('stat_beta_delta'),     bdStr, bdCol,    TIPS.delta)}
      ${_stat(t('stat_active_npi'),     (state.active_interventions || []).length,
              '#cbd5e1', 'Number of interventions currently active.')}
    `) +
    _section(t('stat_dynamics'), `
      ${_stat(t('stat_doubling_time'),  tdStr, '#cbd5e1', TIPS.td)}
      ${_stat(t('stat_growth_rate'),    grStr, grCol,    TIPS.gr)}
      ${_stat(t('stat_new_infections'), ni,    '#cbd5e1', TIPS.ni)}
      ${_stat(t('stat_attack_rate'),    ar,    '#cbd5e1', TIPS.ar)}
    `) +
    _section(t('stat_severity'), `
      ${_stat(t('stat_cfr'),           cfr,  '#cbd5e1', TIPS.cfr)}
      ${_stat(t('stat_peak_prev'),     peak, '#cbd5e1', TIPS.peak)}
      ${_stat(t('stat_peak_day'),      'D' + (state.peak_prevalence_day ?? 0),
              '#94a3b8', 'Simulated day on which peak prevalence was reached.')}
      ${_stat(t('stat_hospital_load'), hosp, hospCol,  TIPS.hosp)}
    `) +
    _section(t('stat_climate'), `
      ${_stat(t('stat_temperature'),  temp.toFixed(1) + ' °C', tCol, TIPS.clim)}
      ${_stat(t('stat_humidity'),     rh.toFixed(0) + ' %', '#cbd5e1',
              'Relative humidity from the seasonal model.')}
      ${_stat(t('stat_season'),       _seasonLabel(c.season), '#cbd5e1',
              'Northern-hemisphere meteorological season.')}
      ${_stat(t('stat_climate_mult'), bmStr, bmCol, TIPS.clim)}
    `) +
    originHtml;
}

function _seasonLabel(season) {
  if (!season) return '—';
  const key = 'season_' + season.toLowerCase();
  return t(key);
}
