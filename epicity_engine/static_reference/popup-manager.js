/**
 * popup-manager.js — Global "only one popup at a time" coordinator.
 *
 * EpiCity has several independently-implemented popups that used to
 * be able to overlap:
 *
 *   - #poiPanel         (header hover dropdown — POI categories)
 *   - #transportPanel   (header hover dropdown — transit highlights)
 *   - #settingsPanel    (header hover dropdown — sim params)
 *   - #aboutBackdrop    (click modal — project info)
 *   - #popInfoTip       (hover tooltip — SEIR legend)
 *   - .macro-hover-card (macro-view city preview card)
 *
 * Mixing hover-driven (`:hover > .popup`) and click-driven (`.open`)
 * patterns meant the modal could sit on top of a still-hovered
 * dropdown, producing visual clutter. This module centralises the
 * "only the latest popup is visible" rule:
 *
 *   registerPopup(id, { hide })   — advertise a popup and how to hide it
 *   openPopup(id)                 — close everyone else, mark `id` active
 *   closeAllPopups()              — clear suppression and mark nothing active
 *
 * Any popup that can overlap another should call `openPopup(id)` on
 * activation (mouseenter for hover panels, click handler for modals,
 * etc.). Hover panels additionally need a `hide` callback so the
 * manager can forcibly close them — a CSS class
 * (`.popup-suppressed`) that beats the native `:hover display:block`
 * via `!important`.
 *
 * The module is intentionally tiny — a Map + two functions. No event
 * bus, no framework.
 */

/** @typedef {{ hide: () => void }} PopupEntry */

/** @type {Map<string, PopupEntry>} */
const _popups = new Map();

/** Currently-active popup id, or null if none. */
let _activeId = null;

/**
 * Register a popup with the manager.
 *   id    — DOM id or stable string key (e.g. 'aboutBackdrop')
 *   hide  — callback that forcibly closes this popup. For CSS-hover
 *           popups this should add the .popup-suppressed class (or
 *           equivalent) to the panel element.
 */
export function registerPopup(id, { hide }) {
  _popups.set(id, { hide });
}

/**
 * Unregister a popup (e.g. on teardown).
 */
export function unregisterPopup(id) {
  _popups.delete(id);
  if (_activeId === id) _activeId = null;
}

/**
 * Announce that a popup is opening. All other registered popups are
 * forcibly hidden; `id` becomes the active popup.
 */
export function openPopup(id) {
  for (const [otherId, entry] of _popups) {
    if (otherId === id) continue;
    try { entry.hide(); } catch { /* ignore */ }
  }
  _activeId = id;
}

/**
 * Close every registered popup. Used when the user explicitly
 * dismisses everything (Escape key, click outside, etc.).
 */
export function closeAllPopups() {
  for (const entry of _popups.values()) {
    try { entry.hide(); } catch { /* ignore */ }
  }
  _activeId = null;
}

/**
 * Return the id of the currently-active popup, or null.
 */
export function getActivePopup() {
  return _activeId;
}
