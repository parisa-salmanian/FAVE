/**
 * search.js — Cmd/Ctrl+K command palette (intro view) + search-input wiring.
 *
 * Filters the intro markers as the user types and persists a "recently
 * simulated" list to localStorage.
 */

const RECENT_KEY = 'epicity_recent_cities';
const MAX_RECENT = 5;

let _summary = [];          // last fetched cities summary
let _onPick  = null;         // callback(cityId)

/** Wire Cmd+K + the intro search input. */
export function initSearch(summary, onPick) {
  _summary = Array.isArray(summary) ? summary : [];
  _onPick  = onPick;

  // Cmd/Ctrl+K — focus the visible search input.
  window.addEventListener('keydown', _onKeydown);

  const input = document.getElementById('introSearchInput');
  if (input) {
    input.addEventListener('input', () => _filter(input.value.trim().toLowerCase()));
  }
  _renderRecent();
}

/** Refresh the underlying cities list. */
export function setSearchSummary(summary) {
  _summary = Array.isArray(summary) ? summary : [];
  _renderRecent();
}

/** Push a city onto the "recently simulated" stack. */
export function noteRecentCity(cityId) {
  if (!cityId) return;
  let recent = [];
  try { recent = JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); } catch {}
  recent = [cityId, ...recent.filter(id => id !== cityId)].slice(0, MAX_RECENT);
  try { localStorage.setItem(RECENT_KEY, JSON.stringify(recent)); } catch {}
  _renderRecent();
}

/* ── Internals ────────────────────────────────────────────────────────────── */

function _onKeydown(e) {
  const isK = e.key === 'k' || e.key === 'K';
  if (!(isK && (e.metaKey || e.ctrlKey))) return;
  // Only trigger Cmd+K when an intro search input is on screen.
  const input = document.getElementById('introSearchInput');
  if (!input || !input.offsetParent) return;
  e.preventDefault();
  input.focus();
  input.select();
}

function _filter(q) {
  // Toggle the marker's visibility class — visual filter only; the actual
  // markers belong to macro.js. We use a CSS class hook on each marker el.
  document.querySelectorAll('.macro-marker, .macro-city-marker').forEach(el => {
    const name = (el.dataset.name || '').toLowerCase();
    const id   = (el.dataset.id || el.dataset.cityId || '').toLowerCase();
    const hit  = !q || name.includes(q) || id.includes(q);
    el.style.opacity = hit ? '1' : '.15';
    el.style.pointerEvents = hit ? 'auto' : 'none';
  });
}

function _renderRecent() {
  const list = document.getElementById('introRecentList');
  if (!list) return;
  let recent = [];
  try { recent = JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); } catch {}
  list.innerHTML = '';
  // Show up to 3 recent cities; if none, show 3 popular ones from the summary.
  const ids = recent.length
    ? recent
    : _summary.slice(0, 3).map(c => c.id);
  for (const id of ids) {
    const c = _summary.find(s => s.id === id);
    if (!c) continue;
    const initials = (c.name || c.id || '?').split(/\s+/).map(s => s[0]).join('').slice(0, 2).toUpperCase();
    const sub = recent.includes(id) ? 'Recently simulated' : 'Featured';
    const row = document.createElement('div');
    row.className = 'sug';
    row.innerHTML = `
      <div class="avatar${recent[0] === id ? ' hot' : ''}">${initials}</div>
      <div class="meta">
        <div class="city-name">${c.name || c.id}</div>
        <div class="city-sub">${sub}</div>
      </div>
      <span class="pop">${(c.population || 0).toLocaleString()} pop.</span>`;
    row.addEventListener('click', () => _onPick?.(c.id));
    list.appendChild(row);
  }
}
