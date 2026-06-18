/**
 * sidebar.js — Single-element sidebar that animates between mini (icons
 * only) and full (icons + labels + switches) when hovered. The sidebar
 * IS the rail; there is no separate panel to render.
 *
 * Most of the visual transition is handled in style.css via the
 * `#sidebar.mini:not(:hover)` selector hiding the row labels and switches.
 * This module exists for the small bits of state that JS still owns:
 *   • intervention badge counts
 *   • a way to programmatically expand/collapse from outside
 *   • setting `body.sidebar-interacting` so other UI (building tooltips,
 *     hover popups) can suppress themselves while the user is busy in
 *     the rail
 */

import { closeAllPopups } from './popup-manager.js';

let _hoverPinTimer = null;

export function initSidebar() {
  const sb = document.getElementById('sidebar');
  if (!sb) return;

  // The sidebar + its hover-popovers (#poiPop, #transportPop, #flowPop)
  // form one logical "hover group". A single document-level mousemove
  // watcher tracks whether the cursor is anywhere in the group; the
  // sidebar stays open as long as it is, and collapses 250ms after it
  // leaves.
  let _inGroup    = false;
  let _collapseTm = null;
  const _isInside = (x, y) => {
    const el = document.elementFromPoint(x, y);
    if (!el) return false;
    return !!el.closest('#sidebar, #poiPop, #transportPop, #flowPop');
  };
  document.addEventListener('mousemove', e => {
    const inside = _isInside(e.clientX, e.clientY);
    if (inside === _inGroup) return;
    _inGroup = inside;
    if (inside) {
      clearTimeout(_collapseTm); _collapseTm = null;
      // Pin the sidebar to its expanded width via the .open class. CSS
      // :hover alone collapses the moment the cursor leaves #sidebar
      // (e.g. moves into the POI popover sibling), which is exactly
      // the bug the user reported.
      sb.classList.add('open');
      document.body.classList.add('sidebar-interacting');
      // Map tooltips shouldn't compete with sidebar interaction. Hide
      // the building / transport tooltips and slam shut any popup that
      // was anchored to a building / area / transport line — the user
      // is now working in the menu, not the map.
      const t1 = document.getElementById('tooltip');
      const t2 = document.getElementById('transportTip');
      if (t1) t1.style.display = 'none';
      if (t2) t2.style.display = 'none';
      try { closeAllPopups(); } catch (_) {}
    } else {
      clearTimeout(_collapseTm);
      _collapseTm = setTimeout(() => {
        if (!_inGroup) {
          sb.classList.remove('open');
          document.body.classList.remove('sidebar-interacting');
        }
      }, 250);
    }
  });

  // Clicking the mini interventions chip expands the sidebar so the
  // user can actually see / tweak which interventions are active.
  document.getElementById('ivMini')?.addEventListener('click', () => {
    sb.classList.add('open');
  });
}

/** Update the intervention count chip. Called from ui.js on toggle. */
export function setSectionBadge(sectionId, text /* , kind */) {
  const sec   = document.getElementById(sectionId);
  const badge = sec?.querySelector('.ivCountBadge');
  const empty = !text || text === '0';
  if (badge) {
    badge.textContent = text;
    badge.classList.toggle('hidden', empty);
  }
}

/** Dedicated updater for the mini interventions chip. */
export function setInterventionsCount(active, total) {
  const mini  = document.getElementById('ivMini');
  const count = document.getElementById('ivMiniCount');
  if (count) count.textContent = `${active}/${total}`;
  if (mini)  mini.classList.toggle('on', active > 0);
}

/** No-op kept for backwards compat — the rail mirror is now CSS-only. */
export function refreshRailMirrors() {}

/** Programmatic expand/collapse (used by Cmd+K and similar). */
export function setSidebarOpen(open) {
  const sb = document.getElementById('sidebar');
  if (!sb) return;
  sb.classList.toggle('open', !!open);
}
