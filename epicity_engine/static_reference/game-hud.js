/**
 * game-hud.js — Resource-bar HUD for game mode.
 *
 * The HUD floats top-centre in the city view and shows five bars +
 * deaths counter + score. Polls /api/game_state at the same cadence
 * the sim is stepping; falls back to manual refresh on intervention
 * toggle.
 *
 * Hidden in sandbox mode — see CSS rule `body:not(.game-mode) #gameHud`.
 */

import { t, getLang, onLangChange } from './i18n.js';

// Tear down the HUD on language switch so the next render rebuilds it
// from the new translation strings. Subscribed once via the
// idempotent guard below.
let _langSubscribed = false;
function _subscribeLang() {
  if (_langSubscribed) return;
  _langSubscribed = true;
  onLangChange(() => {
    if (_hud && _hud.parentNode) _hud.parentNode.removeChild(_hud);
    _hud = null;
    // Reset trend bookkeeping so the rebuilt bars don't show a stale
    // arrow before a fresh day-over-day comparison is computed.
    _prevSnapshot = null;
    _prevSnapDay  = -1;
    // The next tick of ui.js#_updateStats → renderGameHud will re-mount.
  });
}

let _hud      = null;
let _modal    = null;
let _lastDay  = -1;
let _shownEnd = false;
// Previous-tick snapshot for trend arrows. Only updated when the day
// counter advances so flicker between sub-day refreshes doesn't toggle
// the arrows. Stored as a flat object of bar key → numeric value.
let _prevSnapshot = null;
let _prevSnapDay  = -1;

/** Idempotent. Mounts the HUD + shared rich tooltip into the city overlay. */
export function ensureHud() {
  _subscribeLang();
  if (_hud && document.body.contains(_hud)) return _hud;
  const overlay = document.getElementById('overlay') || document.body;
  _hud = document.createElement('div');
  _hud.id = 'gameHud';
  _hud.innerHTML = `
    <div class="ghud-row ghud-headline" data-key="stability">
      <div class="ghud-headline-h">
        <span class="ghud-headline-ico">🏛️</span>
        <span class="ghud-headline-lbl">${t('hud_stability')}</span>
        <span class="ghud-headline-val">—</span>
      </div>
      <div class="ghud-headline-track"><div class="ghud-headline-fill"></div></div>
    </div>
    <div class="ghud-row ghud-bars">
      ${_barHTML('economy',         '💰', t('hud_economy'))}
      ${_barHTML('morale',          '😊', t('hud_morale'))}
      ${_barHTML('trust',           '🤝', t('hud_trust'))}
      ${_barHTML('healthcare_load', '🏥', t('hud_hospitals'))}
    </div>
    <div class="ghud-row ghud-stats">
      <div class="ghud-chip" data-key="budget"><span class="ghud-chip-ico">💵</span><span class="ghud-chip-lbl">${t('hud_budget')}</span><span class="ghud-chip-val">—</span></div>
      <div class="ghud-chip" data-key="deaths_total"><span class="ghud-chip-ico">⚰️</span><span class="ghud-chip-lbl">${t('hud_deaths')}</span><span class="ghud-chip-val">—</span></div>
      <div class="ghud-chip" data-key="day"><span class="ghud-chip-ico">📅</span><span class="ghud-chip-lbl">${t('hud_day')}</span><span class="ghud-chip-val">—</span></div>
      <div class="ghud-chip ghud-chip-score" data-key="score"><span class="ghud-chip-ico">📉</span><span class="ghud-chip-lbl">${t('hud_score')}</span><span class="ghud-chip-val">—</span></div>
    </div>
  `;
  overlay.appendChild(_hud);
  _ensureInfoTip();
  _bindHudHovers();
  return _hud;
}

let _hudInfoTip = null;

function _ensureInfoTip() {
  if (_hudInfoTip && document.body.contains(_hudInfoTip)) return _hudInfoTip;
  _hudInfoTip = document.createElement('div');
  _hudInfoTip.id = 'ghudInfoTip';
  _hudInfoTip.className = 'pop-info-tooltip';
  _hudInfoTip.style.display = 'none';
  document.body.appendChild(_hudInfoTip);
  return _hudInfoTip;
}

function _bindHudHovers() {
  const tip = _ensureInfoTip();
  const showFor = (key, anchor) => {
    // Pick the localised tooltip blob based on the current language.
    const dict = getLang() === 'sv' ? HUD_RICH_TIPS_SV : HUD_RICH_TIPS;
    const html = dict[key] || HUD_RICH_TIPS[key];
    if (!html) return hide();
    tip.innerHTML = html;
    tip.style.display = 'block';
    // Position above the anchor with the right edge aligned roughly.
    const r = anchor.getBoundingClientRect();
    const tipW = tip.offsetWidth  || 320;
    const tipH = tip.offsetHeight || 180;
    let left = r.left + r.width / 2 - tipW / 2;
    left = Math.max(8, Math.min(window.innerWidth - tipW - 8, left));
    tip.style.left  = `${left}px`;
    tip.style.right = 'auto';
    // Float ABOVE the HUD (HUD lives bottom-right). Falls back below
    // if the popup would clip the top of the viewport.
    let top = r.top - tipH - 12;
    if (top < 8) top = r.bottom + 12;
    tip.style.top = `${top}px`;
  };
  const hide = () => { tip.style.display = 'none'; };

  _hud.addEventListener('mouseover', (e) => {
    const item = e.target.closest('[data-key]');
    if (!item) return;
    showFor(item.dataset.key, item);
  });
  _hud.addEventListener('mouseout', (e) => {
    if (e.relatedTarget && _hud.contains(e.relatedTarget)) return;
    hide();
  });
}

/* Rich-format tooltips, mirroring the R-eff / Growth pop-info style.
 * One HTML blob per HUD item, swapped into a single shared tooltip. */
const HUD_RICH_TIPS = {
  stability: `
    <b>🏛️ Stability</b> — headline reading of how the country is holding up.<br><br>
    Equal-weighted average of:<br>
    &nbsp;&nbsp;<b>Economy</b> · <b>Morale</b> · <b>Trust</b> · <b>Healthcare headroom</b><br><br>
    Healthcare headroom is derived from the load bar:<br>
    &nbsp;&nbsp;load 0–50% &nbsp;→&nbsp; 100<br>
    &nbsp;&nbsp;load 50–100% &nbsp;→&nbsp; ramps 100 → 50<br>
    &nbsp;&nbsp;load 100–150% &nbsp;→&nbsp; ramps 50 → 0<br>
    &nbsp;&nbsp;load &gt; 150% &nbsp;→&nbsp; 0<br><br>
    <span style="color:#34d399">≥ 70 — society is coping</span><br>
    <span style="color:#f59e0b">40–70 — strained</span><br>
    <span style="color:#fb7185">&lt; 40 — collapse risk</span><br><br>
    <i style="color:var(--text-mute)">Stability does not directly drive the score — deaths and bar-shortfalls do.
    But a low stability run usually means the score is already bleeding.</i>`,

  economy: `
    <b>💰 Economy</b> — productive output of the city (0–100).<br><br>
    <span style="color:#fb7185">Drains</span> from lockdown, school closures,
    flights ban, distancing — and from visible outbreak even without policy
    (people self-isolate, supply chains stutter, foot traffic dies).<br><br>
    <span style="color:#34d399">Recovers</span> slowly only when no restrictive
    measure is on AND prevalence is under 0.05%.<br><br>
    <i style="color:var(--text-mute)">Refills the budget at ~${'`'}500 × economy/100${'`'} kr/day.</i>`,

  morale: `
    <b>😊 Public Morale</b> — happiness / sanity bar (0–100).<br><br>
    <span style="color:#fb7185">Drops</span> with new deaths
    (sublinear, capped −1.5/day), cumulative death toll, visible
    infections, healthcare overflow, and lifestyle restrictions
    (lockdown, masks, distancing).<br><br>
    <span style="color:#34d399">Recovers</span> only when the outbreak is
    contained (&lt;0.05% prevalence) AND no restrictive measure is on.<br><br>
    <i style="color:var(--text-mute)">If morale hits 0 for 14 consecutive days, society collapses.</i>`,

  trust: `
    <b>🤝 Public Trust</b> — your political capital (0–100).<br><br>
    <span style="color:#fb7185">Spent</span> every time you toggle ON a
    measure (lockdown −10, schools −5, suspend flights −4, distancing −3,
    masks −2, transit masks −1; vaccination, testing, screening,
    info campaigns are free).<br>
    <span style="color:#fb7185">Erodes</span> with toggle-fatigue
    (4+ flips in 14 days = −10) and from deaths + overflow.<br><br>
    <span style="color:#34d399">Regenerates</span> +0.4/day when no toggle
    for 7 days AND prevalence &lt;0.1%.<br><br>
    <i style="color:var(--text-mute)">If trust hits 0, the public stops complying with new measures.</i>`,

  healthcare_load: `
    <b>🏥 Hospital Load</b> — % of hospital capacity in use.<br><br>
    Active infections ÷ hospital capacity (5% of pop, +40% with
    Hospital Surge).<br><br>
    <span style="color:#fb7185">Above 100%</span> the system is
    overwhelmed — fatality rate jumps (μ → 5× the user-set value),
    morale &amp; trust drains hit harder, economy takes −0.6/day.<br><br>
    <i style="color:var(--text-mute)">Hospital Surge intervention raises capacity by +40%.</i>`,

  budget: `
    <b>💵 Treasury</b> — your medical budget in SEK.<br><br>
    <span style="color:#fb7185">Spent</span> daily on vaccination
    (~8 000 kr), hospital surge (~10 000), testing (~3 000), transit
    screening (~2 000), masks (~200), transit masks (~300), info
    campaign (~500).<br><br>
    <span style="color:#34d399">Refilled</span> ~500 × (economy / 100) kr/day.<br><br>
    <i style="color:var(--text-mute)">Can go negative — once empty, paid interventions stall.</i>`,

  deaths_total: `
    <b>⚰️ Cumulative Deaths</b><br><br>
    Total deaths since day 0. Each death adds <b>0.5</b> to the score
    (the heaviest single weight) and feeds the per-day morale &amp;
    trust drains.<br><br>
    <i style="color:var(--text-mute)">Reported per 100 k population in the end-of-scenario summary.</i>`,

  day: `
    <b>📅 Day Counter</b><br><br>
    180-day fixed scenario. The end-of-game modal opens on day 180 with
    a final score, letter rating (S/A/B/C/D/F) and a breakdown of all
    six resources.<br><br>
    <i style="color:var(--text-mute)">No win/loss — just minimise the total cost.</i>`,

  score: `
    <b>📉 Score</b> — total cost of the pandemic. <b>Lower is better.</b><br><br>
    Per-day cost = <code>0.5 × new_deaths + 0.02 × (300 − E − M − T) +
    0.05 × max(0, load − 100)</code><br><br>
    Rating thresholds at scenario end:<br>
    <span style="color:#22d3ee">S</span> &lt; 30 ·
    <span style="color:#22c55e">A</span> &lt; 60 ·
    <span style="color:#84cc16">B</span> &lt; 100 ·
    <span style="color:#f59e0b">C</span> &lt; 150 ·
    <span style="color:#f97316">D</span> &lt; 220 ·
    <span style="color:#ef4444">F</span> ≥ 220`,
};

function _barHTML(key, icon, label) {
  return `
    <div class="ghud-bar" data-key="${key}">
      <div class="ghud-bar-h">
        <span class="ghud-bar-ico">${icon}</span>
        <span class="ghud-bar-lbl">${label}</span>
        <span class="ghud-bar-val">—</span>
      </div>
      <div class="ghud-bar-track"><div class="ghud-bar-fill"></div></div>
    </div>`;
}

/**
 * Render a fresh game-state snapshot. Pass null/undefined to clear.
 * `day` is the current sim day (from /api/state) — used to drive the
 * end-of-180-day modal even if the engine skips a tick.
 */
export function render(snapshot, day) {
  if (!document.body.classList.contains('game-mode')) {
    if (_hud) _hud.style.display = 'none';
    return;
  }
  ensureHud();
  _hud.style.display = '';

  if (!snapshot) {
    _hud.classList.add('ghud-empty');
    return;
  }
  _hud.classList.remove('ghud-empty');

  // Trend arrows: compare against the snapshot we stored at the
  // previous day boundary. We invert polarity for healthcare_load
  // (lower is better there).
  const trends = _computeTrends(snapshot, day);

  _setHeadline(snapshot.stability, trends.stability);
  _setBar('economy',         snapshot.economy,         100, '%', trends.economy);
  _setBar('morale',          snapshot.morale,          100, '%', trends.morale);
  _setBar('trust',           snapshot.trust,           100, '%', trends.trust);
  _setBar('healthcare_load', snapshot.healthcare_load, 200, '%', trends.healthcare_load);

  _setChip('budget',       _formatSEK(snapshot.budget));
  _setChip('deaths_total', snapshot.deaths_total.toLocaleString());
  _setChip('day',          (day != null ? `${day} / ${snapshot.duration_days}` : '—'));
  _setChip('score',        snapshot.score.toFixed(1));

  if (snapshot.finished && !_shownEnd) {
    _shownEnd = true;
    _showEndModal(snapshot);
  }
  if (!snapshot.finished) _shownEnd = false;
  _lastDay = day != null ? day : _lastDay;
}

function _setHeadline(value, trend) {
  const root = _hud.querySelector('.ghud-headline');
  if (!root) return;
  const v = Number.isFinite(value) ? value : 0;
  const pct = Math.max(0, Math.min(100, v));
  const fill = root.querySelector('.ghud-headline-fill');
  fill.style.width = pct + '%';
  // Tighter ramp than the sub-bars: this is the headline reading.
  const level = v >= 70 ? 'good' : v >= 40 ? 'warn' : 'bad';
  root.dataset.level = level;
  root.dataset.trend = trend || 'flat';
  root.querySelector('.ghud-headline-val').textContent = `${Math.round(v)}`;
}

/* Compute up/down/flat per bar from the previous-day snapshot. */
function _computeTrends(snap, day) {
  const out = {
    stability: 'flat', economy: 'flat', morale: 'flat',
    trust: 'flat', healthcare_load: 'flat',
  };
  if (_prevSnapshot && _prevSnapDay >= 0 && day > _prevSnapDay) {
    const eps = 0.5;   // ignore sub-pp noise
    const trend = (cur, prev, invert = false) => {
      if (cur == null || prev == null) return 'flat';
      const d = cur - prev;
      if (Math.abs(d) < eps) return 'flat';
      const up = d > 0;
      // For most bars, up = good. For healthcare_load, up = bad.
      return invert ? (up ? 'down' : 'up') : (up ? 'up' : 'down');
    };
    out.stability       = trend(snap.stability,       _prevSnapshot.stability);
    out.economy         = trend(snap.economy,         _prevSnapshot.economy);
    out.morale          = trend(snap.morale,          _prevSnapshot.morale);
    out.trust           = trend(snap.trust,           _prevSnapshot.trust);
    out.healthcare_load = trend(snap.healthcare_load, _prevSnapshot.healthcare_load, true);
  }
  // Snapshot only when day advances — sub-day refreshes (post-toggle
  // refreshGameHud) re-render with the SAME prev snapshot, so arrows
  // reflect day-over-day movement, not flicker.
  if (day !== _prevSnapDay) {
    _prevSnapshot = {
      stability:       snap.stability,
      economy:         snap.economy,
      morale:          snap.morale,
      trust:           snap.trust,
      healthcare_load: snap.healthcare_load,
    };
    _prevSnapDay = day != null ? day : _prevSnapDay;
  }
  return out;
}

function _setBar(key, value, maxV, suffix, trend) {
  const root = _hud.querySelector(`.ghud-bar[data-key="${key}"]`);
  if (!root) return;
  const v = Number.isFinite(value) ? value : 0;
  const pct = Math.max(0, Math.min(100, (v / maxV) * 100));
  const fill = root.querySelector('.ghud-bar-fill');
  fill.style.width = pct + '%';
  // Colour ramp: green when healthy, amber middling, red bad. For healthcare_load
  // the polarity is inverted (high load = bad).
  let level;
  if (key === 'healthcare_load') {
    level = v < 70 ? 'good' : v < 100 ? 'warn' : 'bad';
  } else {
    level = v > 60 ? 'good' : v > 30 ? 'warn' : 'bad';
  }
  root.dataset.level = level;
  root.dataset.trend = trend || 'flat';
  root.querySelector('.ghud-bar-val').textContent = `${Math.round(v)}${suffix || ''}`;
}

function _setChip(key, text) {
  const el = _hud.querySelector(`.ghud-chip[data-key="${key}"] .ghud-chip-val`);
  if (el) el.textContent = text;
}

function _formatSEK(n) {
  if (!Number.isFinite(n)) return '—';
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)} M kr`;
  if (Math.abs(n) >= 1_000)     return `${(n / 1_000).toFixed(0)} k kr`;
  return `${Math.round(n)} kr`;
}

function _showEndModal(snapshot) {
  if (_modal && document.body.contains(_modal)) _modal.remove();
  const fb = snapshot.final_breakdown || {};
  _modal = document.createElement('div');
  _modal.id = 'gameEndModal';
  _modal.className = 'modal-backdrop on';
  _modal.innerHTML = `
    <div class="modal-card gend-card" role="dialog" aria-modal="true">
      <button class="modal-close" id="gendClose" aria-label="Close">×</button>
      <div class="gend-rating gend-rating-${(fb.rating||'C').toLowerCase()}">${fb.rating || '—'}</div>
      <div class="gend-headline">Scenario complete · ${snapshot.duration_days} days</div>
      <div class="gend-score-big">${(fb.score ?? snapshot.score).toFixed(1)} <span class="gend-score-unit">cost</span></div>
      <table class="gend-tbl">
        <tr><td>Deaths</td><td>${(fb.deaths ?? snapshot.deaths_total).toLocaleString()} <span class="gend-sub">(${fb.deaths_per_100k ?? '—'} / 100k)</span></td></tr>
        <tr><td>Economy</td><td>${(fb.economy ?? snapshot.economy).toFixed(0)} / 100</td></tr>
        <tr><td>Morale</td><td>${(fb.morale ?? snapshot.morale).toFixed(0)} / 100</td></tr>
        <tr><td>Trust</td><td>${(fb.trust ?? snapshot.trust).toFixed(0)} / 100</td></tr>
        <tr><td>Budget left</td><td>${_formatSEK(fb.budget_remaining ?? snapshot.budget)}</td></tr>
      </table>
      <div class="gend-actions">
        <button class="gend-btn gend-btn-primary" id="gendReplay">Try again</button>
        <button class="gend-btn" id="gendBack">Back to map</button>
      </div>
    </div>
  `;
  document.body.appendChild(_modal);
  _modal.querySelector('#gendClose').addEventListener('click', () => _modal.remove());
  _modal.querySelector('#gendReplay').addEventListener('click', async () => {
    await fetch('/api/reset', { method: 'POST' });
    _modal.remove();
    _shownEnd = false;
    // ui.js will pick up the fresh state on its next tick.
  });
  _modal.querySelector('#gendBack').addEventListener('click', () => {
    _modal.remove();
    document.getElementById('btnGlobe')?.click();
  });
}

/** Force-fetch the latest /api/game_state — useful right after a toggle. */
export async function refresh() {
  if (!document.body.classList.contains('game-mode')) return;
  try {
    const r = await fetch('/api/game_state');
    if (!r.ok) return;
    const snap = await r.json();
    if (snap) render(snap, _lastDay >= 0 ? _lastDay : null);
  } catch (_) {}
}

/* Swedish-language counterparts for the rich tooltips. Wikipedia-grade
 * literal translations of the English source in HUD_RICH_TIPS, with
 * the same colour cues and formula formatting. */
const HUD_RICH_TIPS_SV = {
  stability: `
    <b>🏛️ Stabilitet</b> — övergripande mått på hur landet klarar sig.<br><br>
    Ovägt medelvärde av:<br>
    &nbsp;&nbsp;<b>Ekonomi</b> · <b>Moral</b> · <b>Förtroende</b> · <b>Sjukhusmarginal</b><br><br>
    Sjukhusmarginal beräknas från belastningsbaren:<br>
    &nbsp;&nbsp;belastning 0–50% &nbsp;→&nbsp; 100<br>
    &nbsp;&nbsp;belastning 50–100% &nbsp;→&nbsp; rampar 100 → 50<br>
    &nbsp;&nbsp;belastning 100–150% &nbsp;→&nbsp; rampar 50 → 0<br>
    &nbsp;&nbsp;belastning &gt; 150% &nbsp;→&nbsp; 0<br><br>
    <span style="color:#34d399">≥ 70 — samhället klarar sig</span><br>
    <span style="color:#f59e0b">40–70 — ansträngt</span><br>
    <span style="color:#fb7185">&lt; 40 — kollapsrisk</span><br><br>
    <i style="color:var(--text-mute)">Stabiliteten påverkar inte poängen direkt — det gör dödsfall och staplarnas underskott. Men ett spel med låg stabilitet brukar redan blöda poäng.</i>`,

  economy: `
    <b>💰 Ekonomi</b> — stadens produktion (0–100).<br><br>
    <span style="color:#fb7185">Minskar</span> av nedstängning, skolstängning, flygförbud, social distansering — och av synligt utbrott även utan åtgärder (folk självisolerar, försörjningskedjor stockar sig, fottrafiken dör).<br><br>
    <span style="color:#34d399">Återhämtar</span> långsamt endast när ingen restriktiv åtgärd är aktiv OCH prevalensen är under 0,05%.<br><br>
    <i style="color:var(--text-mute)">Fyller på budgeten med ~${'`'}500 × ekonomi/100${'`'} kr/dag.</i>`,

  morale: `
    <b>😊 Folkets moral</b> — välmående/tålamod (0–100).<br><br>
    <span style="color:#fb7185">Sjunker</span> av nya dödsfall (sublinjärt, max −1,5/dag), totalt antal dödsfall, synliga smittor, sjukhusöverbelastning och livsstilsrestriktioner (nedstängning, munskydd, distansering).<br><br>
    <span style="color:#34d399">Återhämtar</span> endast när utbrottet är under kontroll (&lt;0,05% prevalens) OCH ingen restriktiv åtgärd är aktiv.<br><br>
    <i style="color:var(--text-mute)">Om moralen ligger på 0 i 14 dagar i sträck kollapsar samhället.</i>`,

  trust: `
    <b>🤝 Allmänhetens förtroende</b> — ditt politiska kapital (0–100).<br><br>
    <span style="color:#fb7185">Förbrukas</span> varje gång du slår på en åtgärd (nedstängning −10, skolor −5, flygförbud −4, distansering −3, munskydd −2, kollektivtrafiksmunskydd −1; vaccin, testning, screening och informationskampanj är gratis).<br>
    <span style="color:#fb7185">Eroderar</span> av åtgärds-jojo (4+ växlingar inom 14 dagar = −10) samt av dödsfall + överbelastning.<br><br>
    <span style="color:#34d399">Regenererar</span> +0,4/dag när inga växlingar gjorts på 7 dagar OCH prevalensen är &lt;0,1%.<br><br>
    <i style="color:var(--text-mute)">Om förtroendet når 0 slutar allmänheten följa nya åtgärder.</i>`,

  healthcare_load: `
    <b>🏥 Sjukhusbelastning</b> — andel av sjukhusens kapacitet i bruk.<br><br>
    Aktiva infektioner ÷ sjukhuskapacitet (5% av befolkningen, +40% med Akutkapacitet).<br><br>
    <span style="color:#fb7185">Över 100%</span> är systemet överbelastat — dödligheten skenar (μ → 5× det inställda värdet), moral & förtroende dräneras dubbelt så snabbt, ekonomin tappar −0,6/dag.<br><br>
    <i style="color:var(--text-mute)">Akutkapacitet-åtgärden höjer kapaciteten med +40%.</i>`,

  budget: `
    <b>💵 Statskassa</b> — din medicinska budget i SEK.<br><br>
    <span style="color:#fb7185">Förbrukas</span> dagligen av vaccinationer (~8 000 kr), akutkapacitet (~10 000), testning (~3 000), stationskontroll (~2 000), munskydd (~200), kollektivtrafiksmunskydd (~300), informationskampanj (~500).<br><br>
    <span style="color:#34d399">Fylls på</span> ~500 × (ekonomi / 100) kr/dag.<br><br>
    <i style="color:var(--text-mute)">Kan gå minus — när budgeten är tom stannar betalda åtgärder.</i>`,

  deaths_total: `
    <b>⚰️ Totalt antal dödsfall</b><br><br>
    Totala dödsfall sedan dag 0. Varje dödsfall lägger till <b>0,5</b> till poängen (den tyngsta enskilda vikten) och driver de dagliga moral- och förtroendedräneringarna.<br><br>
    <i style="color:var(--text-mute)">Rapporteras per 100 000 invånare i slutskärmen.</i>`,

  day: `
    <b>📅 Dagräknare</b><br><br>
    180-dagars fast scenario. Slutdialogen öppnas på dag 180 med slutpoäng, betyg (S/A/B/C/D/F) och en sammanfattning av alla sex resurserna.<br><br>
    <i style="color:var(--text-mute)">Inget vinst/förlust — bara minimera totalkostnaden.</i>`,

  score: `
    <b>📉 Poäng</b> — pandemins totala kostnad. <b>Lägre är bättre.</b><br><br>
    Daglig kostnad = <code>0,5 × nya_dödsfall + 0,02 × (300 − E − M − T) + 0,05 × max(0, belastning − 100)</code><br><br>
    Betygsgränser i slutet:<br>
    <span style="color:#22d3ee">S</span> &lt; 30 ·
    <span style="color:#22c55e">A</span> &lt; 60 ·
    <span style="color:#84cc16">B</span> &lt; 100 ·
    <span style="color:#f59e0b">C</span> &lt; 150 ·
    <span style="color:#f97316">D</span> &lt; 220 ·
    <span style="color:#ef4444">F</span> ≥ 220`,
};
