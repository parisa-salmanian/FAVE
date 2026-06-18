/**
 * historyStore.js — IndexedDB persistence for the timeline scrubber history.
 *
 * The frontend keeps an in-memory `_history` array of day snapshots so the
 * user can scrub backward and replay the city's evolution. Without this
 * module that array is wiped on page reload, on city switch, and on
 * simulation-mode reset. Here we persist it per-city in IndexedDB so it
 * survives reloads and round-trips through the macro view.
 *
 * localStorage isn't viable: a single snapshot for a 15k-building city is
 * already ~7 MB if we save the raw state, so HISTORY_MAX=200 snapshots blow
 * past the ~5 MB localStorage quota. IndexedDB has GB-scale headroom and
 * accepts typed arrays without round-tripping through JSON.
 *
 * Storage layout: one key per city ID, value is a packed object containing
 * a Float32 typed array per SEIR compartment (S/E/I/R/D/N) for *all* days
 * concatenated, plus a small per-day metadata array. This minimises both
 * IndexedDB transaction count and structured-clone overhead.
 */

const DB_NAME    = 'epicity';
const DB_VERSION = 1;
const STORE_NAME = 'history';

let _dbPromise = null;

function _open() {
  if (_dbPromise) return _dbPromise;
  if (typeof indexedDB === 'undefined') {
    _dbPromise = Promise.reject(new Error('IndexedDB unavailable'));
    return _dbPromise;
  }
  _dbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror   = () => reject(req.error);
  });
  return _dbPromise;
}

/**
 * Persist the in-memory `history` array for a given city. Strips heavy
 * fields (per-cell age breakdowns, params, simulation_mode) before saving
 * — those are static across days and the tooltip falls back gracefully
 * when `cell.age` is missing.
 */
export async function saveHistory(cityId, history) {
  if (!cityId || !Array.isArray(history) || history.length === 0) return;
  try {
    const db     = await _open();
    const packed = _packHistory(history);
    await new Promise((res, rej) => {
      const tx = db.transaction(STORE_NAME, 'readwrite');
      tx.objectStore(STORE_NAME).put(packed, cityId);
      tx.oncomplete = res;
      tx.onerror    = () => rej(tx.error);
      tx.onabort    = () => rej(tx.error);
    });
  } catch (e) {
    console.warn('[historyStore] save failed', e);
  }
}

/**
 * Load the persisted history array for a given city. Returns [] if no
 * cache exists or on any error. The returned snapshots are shaped to
 * match what `_scrubTo` and `_updateStats` read from in-memory snapshots.
 */
export async function loadHistory(cityId) {
  if (!cityId) return [];
  try {
    const db = await _open();
    const packed = await new Promise((res, rej) => {
      const tx  = db.transaction(STORE_NAME, 'readonly');
      const req = tx.objectStore(STORE_NAME).get(cityId);
      req.onsuccess = () => res(req.result);
      req.onerror   = () => rej(req.error);
    });
    if (!packed) return [];
    return _unpackHistory(packed);
  } catch (e) {
    console.warn('[historyStore] load failed', e);
    return [];
  }
}

/** Drop persisted history for a given city (sim mode reset, full reset). */
export async function clearHistory(cityId) {
  if (!cityId) return;
  try {
    const db = await _open();
    await new Promise((res, rej) => {
      const tx = db.transaction(STORE_NAME, 'readwrite');
      tx.objectStore(STORE_NAME).delete(cityId);
      tx.oncomplete = res;
      tx.onerror    = () => rej(tx.error);
      tx.onabort    = () => rej(tx.error);
    });
  } catch (e) {
    console.warn('[historyStore] clear failed', e);
  }
}

// ── Packing ───────────────────────────────────────────────────────────────────

/**
 * Pack `_history` into a single object with Float32 typed arrays for each
 * SEIR compartment. Each array length is `days * cellCount`, indexed as
 * `array[dayIdx * cellCount + cellIdx]`.
 */
function _packHistory(history) {
  const days = history.length;
  // Use the first snapshot to determine the cell-array length. We assume
  // it's stable across days for a given city (it is — buildings don't
  // appear or disappear between days).
  const cellCount = history[0]?.cells?.length || 0;
  const total = days * cellCount;

  const S = new Float32Array(total);
  const E = new Float32Array(total);
  const I = new Float32Array(total);
  const R = new Float32Array(total);
  const D = new Float32Array(total);
  const N = new Float32Array(total);

  // Per-day metadata (small): everything _scrubTo / _updateStats need
  // besides the cells, plus the chart history series and patient_zero.
  const meta = new Array(days);

  for (let d = 0; d < days; d++) {
    const snap   = history[d];
    const cells  = snap.cells || [];
    const offset = d * cellCount;
    for (let i = 0; i < cellCount; i++) {
      const c = cells[i];
      if (!c) continue;
      S[offset + i] = c.S ?? 0;
      E[offset + i] = c.E ?? 0;
      I[offset + i] = c.I ?? 0;
      R[offset + i] = c.R ?? 0;
      D[offset + i] = c.D ?? 0;
      N[offset + i] = c.N ?? 0;
    }
    meta[d] = {
      day:     snap.day,
      S_total: snap.S,
      E_total: snap.E,
      I_total: snap.I,
      R_total: snap.R,
      D_total: snap.D,
      cases:   snap.cases,
      r_eff:   snap.r_eff,
      hospital_overloaded: snap.hospital_overloaded,
      active_interventions: snap.active_interventions,
      history:      snap.history,       // chart series, ~5 nums × day
      patient_zero: snap.patient_zero,  // tiny
      // Climate + rolled weather so timeline scrubs after reload still
      // show each historical day's actual snow/rain/cloud/sun + temperature.
      climate:      snap.climate,
      weather:      snap.weather,
    };
  }

  return { version: 1, days, cellCount, meta, S, E, I, R, D, N };
}

/**
 * Inverse of _packHistory: rebuild an in-memory `_history` array of plain
 * objects compatible with the existing snapshot consumers.
 */
function _unpackHistory(packed) {
  if (!packed || !packed.days || !packed.cellCount) return [];
  const { days, cellCount, meta, S, E, I, R, D, N } = packed;
  const out = new Array(days);

  for (let d = 0; d < days; d++) {
    const offset = d * cellCount;
    const cells  = new Array(cellCount);
    for (let i = 0; i < cellCount; i++) {
      cells[i] = {
        S: S[offset + i],
        E: E[offset + i],
        I: I[offset + i],
        R: R[offset + i],
        D: D[offset + i],
        N: N[offset + i],
      };
    }
    const m = meta[d] || {};
    out[d] = {
      day:                  m.day,
      cells,
      S:                    m.S_total,
      E:                    m.E_total,
      I:                    m.I_total,
      R:                    m.R_total,
      D:                    m.D_total,
      cases:                m.cases,
      r_eff:                m.r_eff,
      hospital_overloaded:  m.hospital_overloaded,
      active_interventions: m.active_interventions,
      history:              m.history,
      patient_zero:         m.patient_zero,
      climate:              m.climate,
      weather:              m.weather,
    };
  }
  return out;
}
