/**
 * building-search.js — Fast in-city search for buildings + POIs.
 *
 * Builds a flat lowercase index once per city (kept under 200k entries
 * even for the largest Swedish cities) and serves up to 5 best matches
 * per keystroke. Matching is substring + simple synonym aliases (kyrka
 * → church, sjukhus → hospital, …) so Swedish queries find the right
 * building zone even when the displayed name is in English.
 */

import { POI_CATEGORIES, POI_GROUPS, poiName } from './view.js';
import { t } from './i18n.js';

// ── Synonyms — keep small. Maps query terms (any language) onto the
//    canonical category-name fragments the index can match against.
const SYNONYMS = {
  'kyrka':       'church',
  'kyrkor':      'church',
  'sjukhus':     'hospital',
  'apotek':      'pharmacy',
  'skola':       'school',
  'skolor':      'school',
  'förskola':    'kindergarten',
  'forskola':    'kindergarten',
  'universitet': 'university',
  'bibliotek':   'library',
  'park':        'park',
  'museum':      'museum',
  'teater':      'theater',
  'restaurang':  'restaurant',
  'butik':       'shop',
  'station':     'station',
  'tåg':         'train',
  'tag':         'train',
  'buss':        'bus',
  'spårvagn':    'tram',
  'sparvagn':    'tram',
  'färja':       'ferry',
  'farja':       'ferry',
  'flygplats':   'airport',
  'sjö':         'lake',
  'sjo':         'lake',
  'skog':        'forest',
};

let _index = [];                    // flat array of {name, hay, lon, lat, idx, color, emoji, sub}
let _onPick = null;                 // (entry) → fly camera there

/** Build (or rebuild) the search index for the current city. */
export function buildSearchIndex(cityLayout, opts = {}) {
  _onPick = opts.onPick || _onPick;
  _index = [];
  if (!cityLayout?.buildings) return;

  for (let i = 0; i < cityLayout.buildings.length; i++) {
    const b = cityLayout.buildings[i];
    if (!b) continue;
    // Only index buildings with a real name OR that map to a known POI.
    const cat   = POI_CATEGORIES[b.zone] || POI_CATEGORIES[b.kind];
    const named = !!b.name;
    if (!named && !cat) continue;
    const display = b.name || (cat ? cat.name : `#${i}`);
    const subParts = [];
    if (cat?.name) subParts.push(cat.name);
    if (b.neighborhood) subParts.push(b.neighborhood);
    _index.push({
      name:    display,
      sub:     subParts.join(' · '),
      hay:    `${display} ${cat?.name || ''} ${b.kind || ''} ${b.zone || ''}`.toLowerCase(),
      lon:     b.lon,
      lat:     b.lat,
      idx:     i,
      color:   cat?.color || '#94a3b8',
      emoji:   cat?.emoji || '\u{1F3E2}',           // 🏢 fallback
    });
  }
}

/** Wire the popover input + Esc handler. Call once after DOMContentLoaded. */
export function initBuildingSearch(onPick) {
  _onPick = onPick;
  const pop   = document.getElementById('buildingSearch');
  const input = document.getElementById('buildingSearchInput');
  const list  = document.getElementById('buildingSearchList');
  if (!pop || !input || !list) return;

  input.addEventListener('input', () => _render(input.value.trim()));
  input.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeBuildingSearch();
    if (e.key === 'Enter') {
      const first = list.querySelector('.search-pop-row');
      first?.click();
    }
  });
  document.addEventListener('keydown', e => {
    if (e.key !== 'Escape') return;
    if (!pop.classList.contains('hidden')) closeBuildingSearch();
  });
  document.addEventListener('click', e => {
    if (pop.classList.contains('hidden')) return;
    if (pop.contains(e.target)) return;
    if (e.target.closest('#btnCmdK')) return;
    closeBuildingSearch();
  });
}

export function openBuildingSearch() {
  const pop   = document.getElementById('buildingSearch');
  const input = document.getElementById('buildingSearchInput');
  pop?.classList.remove('hidden');
  if (input) {
    input.value = '';
    requestAnimationFrame(() => input.focus());
    _render('');
  }
}

export function closeBuildingSearch() {
  document.getElementById('buildingSearch')?.classList.add('hidden');
}

/* ── Internals ────────────────────────────────────────────────────────────── */

function _render(rawQ) {
  const list = document.getElementById('buildingSearchList');
  if (!list) return;
  const q = (rawQ || '').toLowerCase().trim();
  // Empty query → show a hint instead of dumping the entire index.
  if (!q) {
    list.innerHTML = `<div class="search-pop-empty">${t('search_hint')}</div>`;
    return;
  }
  const expanded = SYNONYMS[q] ? `${q} ${SYNONYMS[q]}` : q;
  const terms = expanded.split(/\s+/).filter(Boolean);
  // Score = +2 if name starts with first term, +1 per term hit in haystack.
  const hits = [];
  for (const e of _index) {
    let score = 0;
    let allHit = true;
    for (const t of terms) {
      if (e.hay.includes(t)) score += 1;
      else { allHit = false; break; }
    }
    if (!allHit) continue;
    if (e.name.toLowerCase().startsWith(terms[0])) score += 2;
    hits.push([score, e]);
    if (hits.length > 200) break;     // cap before sorting for speed
  }
  hits.sort((a, b) => b[0] - a[0]);
  const top = hits.slice(0, 5).map(([, e]) => e);
  if (top.length === 0) {
    list.innerHTML = `<div class="search-pop-empty">${t('search_no_match')}</div>`;
    return;
  }
  list.innerHTML = '';
  for (const e of top) {
    const row = document.createElement('div');
    row.className = 'search-pop-row';
    row.innerHTML = `
      <span class="bubble" style="background:${e.color}">${e.emoji}</span>
      <span class="meta">
        <div class="name">${_escape(e.name)}</div>
        <div class="sub">${_escape(e.sub)}</div>
      </span>`;
    row.addEventListener('click', () => {
      _onPick?.(e);
      closeBuildingSearch();
    });
    list.appendChild(row);
  }
}

function _escape(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
