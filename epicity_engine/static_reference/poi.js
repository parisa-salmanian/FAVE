/**
 * poi.js — POI popover (state 03 of the merged HUD).
 *
 * Renders a grouped, searchable, parent/child checkbox tree of POI
 * categories. Toggles drive setPOICategoryVisible() in view.js. The
 * popover opens when the user clicks the POI row in the sidebar.
 *
 * Selection is per-category. "Apply" persists the visible set; "Clear"
 * unchecks everything. Per-row chevron arrows cycle the camera through
 * matching POI instances (delegated to ui.js — exported hooks below).
 */

import {
  POI_CATEGORIES, POI_GROUPS, poiName, poiGroupName,
  setPOICategoryVisible, getPOICounts,
  PATIENT_ZERO_KEY, PATIENT_ZERO_META,
} from './view.js';

// Five small "Swedish secrets" — folded into one collective entry in
// the popover so the list stays scannable. Toggling the row toggles
// every member at once on the map.
const EASTER_EGG_KEYS = ['dalahast', 'ikea_bag', 'fika_cup', 'maypole', 'moose'];
const EASTER_EGG_META = {
  name:  'Easter eggs',
  color: '#d9c37e',
  emoji: '\u{1F95A}',   // 🥚
};

// Group → categories lookup, built once.
let _groupBuckets = null;
let _state        = null;        // { [catKey]: bool }
let _query        = '';
let _onCycle      = null;        // (key, dir) → camera cycle delegate

// ── Public API ───────────────────────────────────────────────────────────────

/** Open the popover (or close it if already open). */
export function togglePoiPopover() {
  const pop = document.getElementById('poiPop');
  if (!pop) return;
  if (pop.classList.contains('open')) closePoiPopover();
  else openPoiPopover();
}

export function openPoiPopover() {
  const pop = document.getElementById('poiPop');
  if (!pop) return;
  _ensureBuckets();
  _ensureState();
  _render();
  pop.classList.add('open');
}

export function closePoiPopover() {
  document.getElementById('poiPop')?.classList.remove('open');
}

/** Wire popover events. Call once after DOMContentLoaded. */
export function initPoi(onCycle) {
  _onCycle = onCycle;
  const pop   = document.getElementById('poiPop');
  if (!pop) return;
  pop.querySelector('#poiPopClose')?.addEventListener('click', closePoiPopover);
  pop.querySelector('#poiSearchInput')?.addEventListener('input', e => {
    _query = e.target.value.trim().toLowerCase();
    _render();
  });
  // Tri-state Show-all click cycles through: none → all → none.
  // Selection state auto-applies (no Clear/Apply buttons needed).
  pop.querySelector('#poiShowAll')?.addEventListener('click', () => {
    _ensureBuckets();
    _ensureState();
    const allOn = Object.values(_state).every(Boolean);
    for (const k of Object.keys(_state)) _state[k] = !allOn;
    _applyAll();
    _render();
  });
}

/* ── Internals ────────────────────────────────────────────────────────────── */

function _ensureBuckets() {
  if (_groupBuckets) return;
  _groupBuckets = {};
  for (const [key, def] of Object.entries(POI_CATEGORIES)) {
    const g = def.group || 'other';
    (_groupBuckets[g] ||= []).push(key);
  }
}

function _ensureState() {
  if (_state) return;
  _state = {};
  for (const k of Object.keys(POI_CATEGORIES)) _state[k] = false;
  // Patient-zero is tracked outside POI_CATEGORIES — it's the marker for
  // the seed building(s). On by default so the user can always find the
  // outbreak origin.
  _state[PATIENT_ZERO_KEY] = true;
}

function _clearAll() {
  for (const k of Object.keys(_state)) _state[k] = false;
  _render();
}

function _applyAll() {
  for (const [k, on] of Object.entries(_state)) {
    setPOICategoryVisible(k, !!on);
  }
}

function _toggleCat(key) {
  _state[key] = !_state[key];
  // Auto-apply: flip the category on/off in the 3D scene immediately.
  // PATIENT_ZERO_KEY is treated like any other category — view.js
  // handles it through the same setPOICategoryVisible path.
  setPOICategoryVisible(key, _state[key]);
  // Patient-zero pin also depends on the global POI layer being on.
  if (key === PATIENT_ZERO_KEY && _state[key]) _ensurePoiLayerOn();
  _render();
}

/**
 * Make sure the global POI layer is visible AND #btnPOI shows as
 * active when the user toggles the patient-zero pin on. Without this,
 * the pin can render in view.js but stay hidden because the parent
 * layer is off.
 */
function _ensurePoiLayerOn() {
  const btn = document.getElementById('btnPOI');
  if (btn && !btn.classList.contains('active')) {
    btn.click();   // routes through ui.js's click handler so state stays in sync
  }
}

function _toggleGroup(groupKey) {
  const cats = _groupBuckets[groupKey] || [];
  const allOn = cats.every(c => _state[c]);
  for (const c of cats) {
    _state[c] = !allOn;
    setPOICategoryVisible(c, _state[c]);     // auto-apply
  }
  _render();
}

function _toggleGroupOpen(groupKey) {
  const list = document.getElementById('poiPopList');
  const child = list?.querySelector(`[data-group="${groupKey}"][data-children]`);
  const head  = list?.querySelector(`[data-group="${groupKey}"][data-parent]`);
  if (!child || !head) return;
  const willOpen = child.classList.toggle('hidden') === false;
  head.classList.toggle('open', willOpen);
}

function _render() {
  _ensureBuckets();
  _ensureState();
  const list = document.getElementById('poiPopList');
  const countLbl = document.getElementById('poiPopCount');
  if (!list) return;
  list.innerHTML = '';

  const counts = (() => { try { return getPOICounts(); } catch { return {}; } })();
  let totalSelected = 0;
  let totalAvail    = 0;

  // Render the patient_zero leaf first — it's the outbreak seed and
  // deserves top billing independent of groups.
  const pzOn    = !!_state[PATIENT_ZERO_KEY];
  const pzAvail = counts[PATIENT_ZERO_KEY] || 0;
  if (!_query || 'patient zero'.includes(_query)) {
    const pz = document.createElement('div');
    pz.className = `poi-row leaf ${pzOn ? 'checked' : ''}`;
    pz.innerHTML = `
      <span class="chev"></span>
      <span class="cb">${pzOn ? '<svg class="i" style="width:10px;height:10px;color:#fff"><use href="#ic-check"/></svg>' : ''}</span>
      <span class="bubble" style="background:${PATIENT_ZERO_META.color};font-size:12px">${PATIENT_ZERO_META.emoji}</span>
      <span class="lbl">${PATIENT_ZERO_META.name}</span>
      <span class="nav">
        <button data-cycle="-1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-left"/></svg></button>
        <span class="count">${pzOn ? pzAvail : 0}/${pzAvail || 1}</span>
        <button data-cycle="1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-right"/></svg></button>
      </span>`;
    pz.addEventListener('click', () => _toggleCat(PATIENT_ZERO_KEY));
    pz.querySelector('.cb').addEventListener('click', e => { e.stopPropagation(); _toggleCat(PATIENT_ZERO_KEY); });
    pz.querySelectorAll('[data-cycle]').forEach(btn => btn.addEventListener('click', e => {
      e.stopPropagation();
      _onCycle?.(`poi:${PATIENT_ZERO_KEY}`, +btn.dataset.cycle);
    }));
    list.appendChild(pz);
    if (pzOn) { totalSelected += pzAvail; }
    totalAvail += pzAvail;
  }

  // Sort groups by their `order` field; unknown groups go last.
  const groupKeys = Object.keys(_groupBuckets).sort((a, b) => {
    const oa = POI_GROUPS[a]?.order ?? 99;
    const ob = POI_GROUPS[b]?.order ?? 99;
    return oa - ob;
  });

  for (const gKey of groupKeys) {
    const cats = _groupBuckets[gKey];
    const visibleCats = _query
      ? cats.filter(c => poiName(c).toLowerCase().includes(_query) ||
                          (POI_GROUPS[gKey]?.name || gKey).toLowerCase().includes(_query))
      : cats;
    if (visibleCats.length === 0) continue;

    const groupOn  = visibleCats.filter(c => _state[c]).length;
    const groupAvail = visibleCats.reduce((s, c) => s + (counts[c] || 0), 0);
    const groupSel   = visibleCats.reduce((s, c) => s + (_state[c] ? (counts[c] || 0) : 0), 0);
    totalSelected += groupSel;
    totalAvail    += groupAvail;
    const partial = groupOn > 0 && groupOn < visibleCats.length;
    const checked = groupOn === visibleCats.length;
    const groupName = poiGroupName ? poiGroupName(gKey) : (POI_GROUPS[gKey]?.name || gKey);

    // Pick first category's color for the group bubble.
    const firstColor = POI_CATEGORIES[visibleCats[0]]?.color || '#888';

    const parent = document.createElement('div');
    parent.className = `poi-row parent ${checked ? 'checked' : ''} ${partial ? 'partial' : ''}`;
    parent.dataset.group  = gKey;
    parent.dataset.parent = '1';
    parent.innerHTML = `
      <span class="chev"><svg class="i" style="width:12px;height:12px"><use href="#ic-chev-right"/></svg></span>
      <span class="cb">${checked && !partial ? '<svg class="i" style="width:10px;height:10px;color:#fff"><use href="#ic-check"/></svg>' : ''}</span>
      <span class="bubble" style="background:${firstColor};font-size:12px">${POI_GROUPS[gKey]?.emoji || ''}</span>
      <span class="lbl">${groupName}</span>
      <span class="nav">
        <button data-cycle="-1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-left"/></svg></button>
        <span class="count">${groupSel}/${groupAvail}</span>
        <button data-cycle="1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-right"/></svg></button>
      </span>`;
    parent.querySelector('.chev').addEventListener('click', e => { e.stopPropagation(); _toggleGroupOpen(gKey); });
    parent.querySelector('.cb').addEventListener('click', e => { e.stopPropagation(); _toggleGroup(gKey); });
    parent.addEventListener('click', () => _toggleGroupOpen(gKey));
    parent.querySelectorAll('[data-cycle]').forEach(btn => btn.addEventListener('click', e => {
      e.stopPropagation();
      _onCycle?.(`group:${gKey}`, +btn.dataset.cycle);
    }));
    list.appendChild(parent);

    const children = document.createElement('div');
    children.className = 'poi-children hidden';
    children.dataset.group = gKey;
    children.dataset.children = '1';

    // Render order: keep flag rows on top, then collapse the five
    // misc easter eggs into a single "Easter eggs" row that toggles
    // them all at once.
    const easterCats = gKey === 'swedish'
      ? visibleCats.filter(c => EASTER_EGG_KEYS.includes(c))
      : [];
    const renderCats = gKey === 'swedish'
      ? visibleCats.filter(c => !EASTER_EGG_KEYS.includes(c))
      : visibleCats;

    for (const cKey of renderCats) {
      const def = POI_CATEGORIES[cKey];
      const on  = !!_state[cKey];
      const avail = counts[cKey] || 0;
      const child = document.createElement('div');
      child.className = `poi-row child ${on ? 'checked' : ''}`;
      child.innerHTML = `
        <span class="chev"></span>
        <span class="cb">${on ? '<svg class="i" style="width:10px;height:10px;color:#fff"><use href="#ic-check"/></svg>' : ''}</span>
        <span class="bubble" style="background:${def.color};font-size:11px">${def.emoji || ''}</span>
        <span class="lbl">${poiName(cKey)}</span>
        <span class="nav">
          <button data-cycle="-1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-left"/></svg></button>
          <span class="count">${on ? avail : 0}/${avail}</span>
          <button data-cycle="1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-right"/></svg></button>
        </span>`;
      child.addEventListener('click', () => _toggleCat(cKey));
      child.querySelector('.cb').addEventListener('click', e => { e.stopPropagation(); _toggleCat(cKey); });
      child.querySelectorAll('[data-cycle]').forEach(btn => btn.addEventListener('click', e => {
        e.stopPropagation();
        _onCycle?.(`poi:${cKey}`, +btn.dataset.cycle);
      }));
      children.appendChild(child);
    }

    // One synthetic "Easter eggs" row representing dalahast / ikea bag /
    // fika cup / maypole / moose — clicking flips them all together.
    if (easterCats.length > 0) {
      const onCount = easterCats.filter(c => _state[c]).length;
      const allOn   = onCount === easterCats.length;
      const partial = onCount > 0 && !allOn;
      const totalAvail2 = easterCats.reduce((s, c) => s + (counts[c] || 0), 0);
      const row = document.createElement('div');
      row.className = `poi-row child ${allOn ? 'checked' : ''} ${partial ? 'partial' : ''}`;
      row.innerHTML = `
        <span class="chev"></span>
        <span class="cb">${allOn && !partial ? '<svg class="i" style="width:10px;height:10px;color:#fff"><use href="#ic-check"/></svg>' : ''}</span>
        <span class="bubble" style="background:${EASTER_EGG_META.color};font-size:12px">${EASTER_EGG_META.emoji}</span>
        <span class="lbl">${EASTER_EGG_META.name}</span>
        <span class="nav">
          <button data-cycle="-1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-left"/></svg></button>
          <span class="count">${onCount}/${easterCats.length}</span>
          <button data-cycle="1"><svg class="i" style="width:11px;height:11px"><use href="#ic-chev-right"/></svg></button>
        </span>`;
      const _flipAll = () => {
        const turnOn = !allOn;
        for (const c of easterCats) _state[c] = turnOn;
        _render();
      };
      row.addEventListener('click', _flipAll);
      row.querySelector('.cb').addEventListener('click', e => { e.stopPropagation(); _flipAll(); });
      // Camera cycling sweeps across every member's positions in turn.
      let _eggCursor = -1;
      row.querySelectorAll('[data-cycle]').forEach(btn => btn.addEventListener('click', e => {
        e.stopPropagation();
        const dir = +btn.dataset.cycle;
        const live = easterCats.filter(c => (counts[c] || 0) > 0);
        if (!live.length) return;
        _eggCursor = (_eggCursor + dir + live.length) % live.length;
        _onCycle?.(`poi:${live[_eggCursor]}`, dir);
      }));
      children.appendChild(row);
      // Don't double-count totals — they're already in the parent group.
    }

    list.appendChild(children);
  }

  if (countLbl) countLbl.textContent = `${totalSelected.toLocaleString()} / ${totalAvail.toLocaleString()}`;

  // Tri-state Show-all visual: none / partial / full.
  const tri = document.getElementById('poiShowAllToggle');
  if (tri) {
    const all = Object.entries(_state);
    const onCount  = all.filter(([, v]) => v).length;
    const allOn    = onCount === all.length && all.length > 0;
    const noneOn   = onCount === 0;
    tri.classList.remove('partial', 'full');
    if (allOn)        tri.classList.add('full');
    else if (!noneOn) tri.classList.add('partial');
  }
}
