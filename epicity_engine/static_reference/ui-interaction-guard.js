/**
 * ui-interaction-guard.js — Suppress map hover popups while the cursor
 * is over any chrome menu (timeline, topbar, settings popover, stats,
 * game HUD, infection legend, search popovers, modals…).
 *
 * Mirrors the existing sidebar.js group-hover trick but generalised to
 * every overlay region. Sets `body.ui-interacting` so callers like
 * ui.js#_onHover can return early, AND aggressively hides the existing
 * tooltip elements + closes any anchored popup the instant the user
 * crosses into a menu.
 *
 * The sidebar already manages its own subset; this file handles the
 * rest. Both classes can coexist — `body.sidebar-interacting` is
 * sidebar-specific, `body.ui-interacting` is the generic guard.
 */

// closeAllPopups intentionally not imported — see comment in initUiInteractionGuard.

// Selector list — anything that should silence map hovers when the
// cursor is over it. New menus should be appended here.
const GUARD_SELECTORS = [
  '#topbar',
  '#playbar',
  '#settingsPop',
  '#statsPanel',
  '#buildingSearch',
  '#aboutBackdrop',
  '#gameHud',
  '#legendPill',
  '#spatialBanner',
  '#poiPop',
  '#transportPop',
  '.modal-card',
  '.pop-info-tooltip',
];

let _inside = false;
let _collapseTm = null;

export function initUiInteractionGuard() {
  const selector = GUARD_SELECTORS.join(', ');

  const _isInside = (x, y) => {
    const el = document.elementFromPoint(x, y);
    if (!el) return false;
    return !!el.closest(selector);
  };

  document.addEventListener('mousemove', e => {
    const inside = _isInside(e.clientX, e.clientY);
    if (inside === _inside) return;
    _inside = inside;
    if (inside) {
      clearTimeout(_collapseTm); _collapseTm = null;
      document.body.classList.add('ui-interacting');
      // Slam the map-anchored building / transport tooltips shut — they
      // follow the cursor, not the menu, so they're stale the moment
      // the user moves into chrome. We do NOT call `closeAllPopups()`
      // here: that would dismiss the very panel the user is hovering
      // (the bug where Live Statistics closed the instant the cursor
      // entered it), since this guard's selector list includes panels
      // that are themselves popups.
      const t1 = document.getElementById('tooltip');
      const t2 = document.getElementById('transportTip');
      if (t1) t1.style.display = 'none';
      if (t2) t2.style.display = 'none';
    } else {
      // Small grace period so cursor jitter at the edge doesn't blink
      // the class. Same delay sidebar.js uses for symmetry.
      clearTimeout(_collapseTm);
      _collapseTm = setTimeout(() => {
        if (!_inside) document.body.classList.remove('ui-interacting');
      }, 250);
    }
  });
}
