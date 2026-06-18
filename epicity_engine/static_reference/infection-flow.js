/**
 * infection-flow.js — Visualises the chain of infection over time.
 *
 * The engine doesn't ship per-edge events, so we approximate the chain
 * locally: every time a building flips from non-infectious to
 * infectious, we attach it as a child of its nearest already-infected
 * neighbour (or to the patient-zero seed when none exist yet). The
 * resulting tree is rendered as glowing arcs A → B → C, animated dashed
 * strokes plus arrowheads near each child.
 *
 * A semi-transparent dark wash underneath dims the basemap so the
 * accent strokes are easy to read against the city's red building
 * tints.
 *
 * Endpoint gating tracks the active legend colour mode: e.g. in
 * 'infections_here' (Infection sites) mode an arc is only drawn if
 * BOTH endpoints have a non-zero infections_here count, so the user
 * doesn't see lines crossing from a red site to a grey building that
 * has been recovered for days. Other compartmental modes
 * (infection / exposed / recovered / deaths / prevalence) use a small
 * positive threshold on the matching SEIR fraction.
 */

import { getColorMode } from './legend.js';

let _map         = null;
let _mapB        = null;        // optional secondary map for compare side-by-side
let _canvas      = null;
let _ctx         = null;
let _wash        = null;        // dim layer behind the arcs
let _active      = false;
let _layout      = null;
let _state       = null;
let _animRaf     = null;
let _t0          = 0;
let _selectedIdx = null;        // when set, only edges touching this building render

// Chain bookkeeping (per active city; cleared via _resetChain).
//
// The model is a directed multigraph, NOT a single-parent tree: every
// time a building enters the infectious state (with hysteresis to
// suppress noise around the 0.5 threshold) we attach it to its nearest
// CURRENTLY-infectious neighbour as a fresh edge. The same pair (A, B)
// can therefore generate both A→B (B's first episode, A was active)
// AND later B→A (A's later re-infection wave, B was active at that
// moment). The stable per-edge hash uses (parent, child) ordering so
// the two reverse directions curve as parallel arcs in opposite
// senses — see _drawArc + _stableHash.
let _edges          = [];         // [{child, parent, day}]
let _everInfected   = new Set();  // any idx that has ever crossed I≥0.5
let _prevInfectious = new Set();  // idx with I≥0.5 in the PREVIOUS ingest
let _recoveredSince = new Set();  // idx that has dipped below 0.3 since its
                                  //   last episode — re-infection eligible
let _lastIngestIdx  = -1;         // last history index folded into the chain

// Chain cache. The chain is monotonic — every birthDay it stamps is "the
// day this building first crossed I≥0.5", which is independent of the
// playhead. Per-day timestamp filtering happens at draw time using
// `state.day`, so a chain built once over the full history renders any
// past day correctly. We invalidate by (history reference, history
// length): live sim grows the array by pushing days, which trips the
// length check and forces a one-time full rebuild on the next replay
// scrub. After that, every scrub is O(1) — just `_state = history[idx]`
// + `_draw()` — so the run button can fly through days without losing
// per-day birthDay stamps to debounce timing.
let _chainHistoryRef = null;
let _chainHistoryLen = 0;

// ── Visual options (toggled from the Flow popover in the sidebar) ─────────
//   arrowhead     : draw the triangle at the child end of each arc
//   colorMode     : 'constant' (default warm tan) | 'intensity' (yellow→red ramp)
//   thicknessMode : 'constant' (1.5px)            | 'intensity' (0.8–3.4 by I)
//   speedMode     : 'constant'                    | 'intensity' (dash anim faster
//                                                                where I is higher)
//   temporal      : 'cumulative' (all edges so far) |
//                   'timestamp'  (only edges whose child became infectious on
//                                 the *current* day — the chain itself is still
//                                 accumulated, but rendering is gated per-tick)
const DEFAULT_OPTIONS = {
  arrowhead:     true,
  colorMode:     'constant',
  thicknessMode: 'constant',
  speedMode:     'constant',
  temporal:      'cumulative',
};
let _options = { ...DEFAULT_OPTIONS };

export function setInfectionFlowOptions(partial) {
  if (!partial || typeof partial !== 'object') return;
  _options = { ..._options, ...partial };
  _draw();
}
export function getInfectionFlowOptions() {
  return { ..._options };
}

// Per-side chains for compare side-by-side. When set (via
// setSplitChainDays) each half of the screen renders its own chain
// replayed to that side's selected day, so dragging Day A doesn't
// pollute Day B's flow tree and vice-versa. Each entry shape:
//   { edges: Array<{child, parent, day}>, everInfected: Set<idx>,
//     prevInfectious: Set<idx>, recoveredSince: Set<idx>,
//     state: <snapshot at that day, used for cells + seeds + threshold> }
let _chainA = null;
let _chainB = null;

// Per-mode endpoint gating — the legend colour mode picks the SEIR
// field whose value must clear `_VIZ_MIN_BY_MODE` for an endpoint to
// be considered "lit up" and eligible for an arc. Modes not listed
// here (zone, income, origin, etc.) fall back to the 'infection'
// (current-I) gate so the flow tree still tracks active outbreaks.
const _VIZ_FIELD_BY_MODE = {
  infection:        'I',
  exposed:          'E',
  susceptible:      'S',
  recovered:        'R',
  deaths:           'D',
  prevalence:       'I',
  infections_here:  'infections_here',
};
const _VIZ_MIN_BY_MODE = {
  infection:        0.3,
  exposed:          0.3,
  susceptible:      0.0,
  recovered:        0.5,
  deaths:           0.0,
  prevalence:       0.3,
  infections_here:  0,    // any building that has hosted ≥1 transmission counts
};

const ARC_LIFT     = 0.18;      // bezier curvature factor (fraction of len)
const MAX_LIFT_PX  = 60;        // but never more than this, so long edges
                                //   at high zoom don't bow across the viewport
const CULL_MARGIN  = 120;       // px margin outside the viewport for culling
const MAX_ARCS     = 220;       // hard cap on per-frame arc count (perf)
const _DASH        = [6, 5];    // module-level dash array — recreating
                                //   the array every paint was thrash

/**
 * Tiny deterministic hash → fraction in [0, 1]. Used for the per-edge
 * curvature offset so the arc shape stays identical across zoom levels.
 */
function _stableHash(a, b) {
  let h = (a * 2654435761) ^ (b * 1597334677);
  h = (h ^ (h >>> 13)) >>> 0;
  return (h % 10007) / 10007;
}

export function initInfectionFlow(mapInstance) {
  _map = mapInstance;
  _canvas = document.getElementById('infectionFlowCanvas');
  _wash   = document.getElementById('infectionFlowWash');
  if (!_canvas || !_map) return;
  _ctx = _canvas.getContext('2d');
  _resetChain();
  _resize();
  window.addEventListener('resize', _resize);
  _map.on('move',  _draw);
  _map.on('zoom',  _draw);
  _map.on('rotate',_draw);
  _map.on('pitch', _draw);
}

/**
 * Restrict rendering to edges that touch a specific building (its parent
 * + its descendants). Pass `null` to clear the filter and show every
 * edge again. Used by the building-selection callback in ui.js.
 */
export function setInfectionFlowSelected(idx) {
  _selectedIdx = (idx == null || idx < 0) ? null : idx;
  _draw();
}

export function setInfectionFlowActive(on) {
  _active = !!on;
  if (_canvas) _canvas.style.display = on ? 'block' : 'none';
  if (_wash)   _wash.style.display   = on ? 'block' : 'none';
  if (on) {
    _t0 = performance.now();
    _loop();
  } else if (_animRaf !== null) {
    cancelAnimationFrame(_animRaf);
    _animRaf = null;
  }
  _draw();
}

/**
 * Called on every sim tick + city load. Diffs the previous infectious
 * set against the new one and assigns parents to the freshly-infected
 * buildings. Resets the chain when the engine restarts (state.day < last day).
 */
export function updateInfectionFlow(state, layout) {
  // Reset on day rollback (replay, reset, city change).
  if (_state && state && state.day < _state.day) _resetChain();
  if (layout && layout !== _layout) _resetChain();

  _state  = state;
  _layout = layout;

  if (state && Array.isArray(state.cells) && layout?.buildings) {
    const seeds = Array.isArray(state.patient_zero) ? state.patient_zero : [];
    _ingestNewInfections(state.cells, layout.buildings, seeds, state.day);
  }
  _draw();
}

/**
 * Replay the chain from a list of historical snapshots, stopping at
 * `idx`. Used by the timeline scrubber + ▶ Run replay.
 *
 * Build-once-cache: the first call for a given history reference (or
 * after it grows) walks all snapshots and stamps every birthDay. Every
 * subsequent call — forward scrub, backward scrub, run-button day
 * boundary — just sets `_state = history[idx]` and redraws. That keeps
 * per-day birthDay stamps stable as the playhead moves, so the
 * "Current day only" filter always has the correct timestamp index to
 * compare against.
 */
export function rebuildInfectionFlowFromHistory(history, currentSnap, layout, idx) {
  if (!layout?.buildings) return;
  if (layout !== _layout) { _layout = layout; _resetChain(); }
  if (!Array.isArray(history) || history.length === 0) {
    _state = currentSnap || null;
    _draw();
    return;
  }
  const stop = Math.max(0, Math.min(history.length - 1, idx));
  const seeds = Array.isArray(currentSnap?.patient_zero) ? currentSnap.patient_zero : [];

  if (_chainHistoryRef !== history || _chainHistoryLen !== history.length) {
    _resetChain();
    for (const s of seeds) {
      _everInfected.add(s);
      _prevInfectious.add(s);
    }
    for (let d = 0; d < history.length; d++) {
      const snap = history[d];
      if (snap?.cells) _ingestNewInfections(snap.cells, layout.buildings, seeds, snap.day ?? d);
    }
    _chainHistoryRef = history;
    _chainHistoryLen = history.length;
    _lastIngestIdx   = history.length - 1;
  }
  _state = history[stop] || currentSnap || null;
  _draw();
}

/* ── Chain bookkeeping ────────────────────────────────────────────────────── */

function _resetChain() {
  _edges           = [];
  _everInfected    = new Set();
  _prevInfectious  = new Set();
  _recoveredSince  = new Set();
  _lastIngestIdx   = -1;
  _chainHistoryRef = null;
  _chainHistoryLen = 0;
}

/** Fold the new ingest's infectious activity into a chain object.
 *  Mutates chain.edges, chain.everInfected, chain.prevInfectious, and
 *  chain.recoveredSince in place. The same helper drives both the
 *  global single-view chain and the per-side compare chains.
 *
 *  Episode detection uses a two-threshold hysteresis on the per-cell
 *  infectious fraction (`cells[i].I`):
 *
 *      ≥ 0.5     → "infectious"   (top of the hysteresis band)
 *      < 0.3     → "recovered"    (bottom of the band — gates re-infection)
 *      [0.3,0.5) → carry-over     (no state change, suppresses 0.5-noise)
 *
 *  An edge is emitted whenever a building transitions INTO the
 *  "infectious" state from below, AND it has either (a) never been
 *  infected before, or (b) dipped under 0.3 since its last episode.
 *  Each emitted edge is attached to the nearest currently-infectious
 *  neighbour, so a re-infection wave on building A will naturally
 *  attach to building B if B was active at that moment — producing
 *  the B→A reverse edge alongside the original A→B from the first
 *  wave, drawn as a separate parallel arc thanks to the
 *  order-sensitive _stableHash. `day` stamps every emitted edge so
 *  the renderer can filter by current day in 'timestamp' mode. */
function _ingestNewInfectionsInto(chain, cells, buildings, seeds, day) {
  const dayStamp = Number.isFinite(day) ? day : 0;

  // Seeds are roots — pre-populated infectious set, no incoming edge.
  for (const s of seeds) {
    if (!chain.everInfected.has(s)) {
      chain.everInfected.add(s);
      chain.prevInfectious.add(s);
    }
  }

  // Walk cells: compute the new infectious set + collect new-episode
  // newcomers, AND mark fully-recovered buildings so they're eligible
  // for re-infection edges the next time they cross 0.5 again.
  const nowInfectious = new Set();
  const newEpisodes   = [];
  for (let i = 0; i < cells.length; i++) {
    const c = cells[i];
    if (!c) continue;
    const I = c.I ?? 0;
    if (I >= 0.5) {
      nowInfectious.add(i);
      if (!chain.prevInfectious.has(i)) {
        // Just crossed the upper threshold from below in this frame.
        const firstTime  = !chain.everInfected.has(i);
        const reinfected =  chain.recoveredSince.has(i);
        if (firstTime || reinfected) newEpisodes.push(i);
        // else: oscillation between 0.3 and 0.5 — suppress edge.
      }
    } else if (I < 0.3 && chain.everInfected.has(i)) {
      // Dipped past the lower threshold — flag for re-infection on
      // the next upper crossing.
      chain.recoveredSince.add(i);
    }
  }

  if (newEpisodes.length > 0) {
    // Parent pool: buildings that were infectious in the previous
    // frame (so the natural source for each new episode is "who was
    // actively spreading right before this building lit up"). Empty
    // on the very first ingest when only seeds exist — fall back to
    // everInfected so first-day newcomers still attach to a seed.
    let pool = chain.prevInfectious.size > 0
      ? [...chain.prevInfectious]
      : [...chain.everInfected];
    if (pool.length > 0) {
      for (const idx of newEpisodes) {
        const b = buildings[idx];
        if (!b) {
          chain.everInfected.add(idx);
          chain.recoveredSince.delete(idx);
          continue;
        }
        let bestParent = -1;
        let bestD = Infinity;
        for (const k of pool) {
          if (k === idx) continue;
          const kb = buildings[k];
          if (!kb) continue;
          const dx = kb.lon - b.lon, dy = kb.lat - b.lat;
          const d  = dx * dx + dy * dy;
          if (d < bestD) { bestD = d; bestParent = k; }
        }
        if (bestParent >= 0) {
          chain.edges.push({ child: idx, parent: bestParent, day: dayStamp });
        }
        chain.everInfected.add(idx);
        chain.recoveredSince.delete(idx);  // reset hysteresis on new episode
        // Let subsequent newcomers in the same frame attach to this
        // one, so a cluster lighting up together produces a fan of
        // edges instead of all converging on one distant parent.
        pool.push(idx);
      }
    }
  }

  // Refresh prevInfectious IN PLACE so the module-level Set the
  // wrapper passed in is updated for the next ingest (reassigning
  // chain.prevInfectious would only update the local wrapper object).
  chain.prevInfectious.clear();
  for (const i of nowInfectious) chain.prevInfectious.add(i);
}

function _ingestNewInfections(cells, buildings, seeds, day) {
  _ingestNewInfectionsInto(
    {
      edges:           _edges,
      everInfected:    _everInfected,
      prevInfectious:  _prevInfectious,
      recoveredSince:  _recoveredSince,
    },
    cells, buildings, seeds, day,
  );
}

/** Replay the chain from day 0 up to `idx` into a fresh chain object,
 *  using the state at `idx` as the reference snapshot for drawing
 *  (cells + seeds + selection threshold). Used by the per-side
 *  compare chains so each half shows its own selected day's tree. */
function _replayChainTo(history, layout, idx) {
  const chain = {
    edges:           [],
    everInfected:    new Set(),
    prevInfectious:  new Set(),
    recoveredSince:  new Set(),
    state: null,
  };
  if (!Array.isArray(history) || !layout?.buildings) return chain;
  const stop = Math.max(0, Math.min(history.length - 1, idx));
  const seeds = Array.isArray(history[stop]?.patient_zero) ? history[stop].patient_zero : [];
  for (const s of seeds) {
    chain.everInfected.add(s);
    chain.prevInfectious.add(s);
  }
  for (let d = 0; d <= stop; d++) {
    const snap = history[d];
    if (snap?.cells) _ingestNewInfectionsInto(chain, snap.cells, layout.buildings, seeds, snap.day ?? d);
  }
  chain.state = history[stop] || null;
  return chain;
}

/** Public: swap in per-day chains for the side-by-side compare view.
 *  Call whenever Day A or Day B moves so the on-canvas flow tree
 *  matches each side's selected day. */
export function setSplitChainDays(history, layout, dayAIdx, dayBIdx) {
  if (layout) _layout = layout;
  _chainA = _replayChainTo(history, layout, dayAIdx);
  _chainB = _replayChainTo(history, layout, dayBIdx);
  _draw();
}

/** Public: drop the per-side chains and revert to the single global
 *  chain (live single-view rendering). Called when compare mode
 *  exits or when switching from split → diff. */
export function clearSplitChains() {
  _chainA = null;
  _chainB = null;
  _draw();
}

/* ── Drawing ──────────────────────────────────────────────────────────────── */

function _resize() {
  if (!_canvas) return;
  const dpr = window.devicePixelRatio || 1;
  _canvas.width  = window.innerWidth  * dpr;
  _canvas.height = window.innerHeight * dpr;
  _canvas.style.width  = window.innerWidth  + 'px';
  _canvas.style.height = window.innerHeight + 'px';
  if (_ctx) _ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  _draw();
}

// Animation loop throttled to ~20 fps. The dashed-stroke "movement"
// effect doesn't need 60fps and at high zooms the per-frame paint of
// hundreds of bezier arcs was the dominant lag source.
const ANIM_INTERVAL_MS = 50;
let _lastDrawTs = 0;
function _loop() {
  if (!_active) return;
  _animRaf = requestAnimationFrame(_loop);
  const now = performance.now();
  if (now - _lastDrawTs < ANIM_INTERVAL_MS) return;
  _lastDrawTs = now;
  _draw();
}

function _draw() {
  if (!_active || !_ctx || !_canvas) return;
  const W = window.innerWidth, H = window.innerHeight;
  _ctx.clearRect(0, 0, W, H);
  if (!_layout?.buildings) return;

  // Single-map (default) — primary render uses the global chain (live
  // chain incrementally folded as the engine ticks). When a secondary
  // map is attached (compare side-by-side) AND per-side chains are set
  // (compare.js called setSplitChainDays), each half draws its own
  // day's chain so dragging Day A doesn't pollute Day B and vice-versa.
  // Each pass is also CLIPPED to its own half so an arc whose endpoints
  // lie outside that half doesn't bleed across the divider.
  if (_mapB) {
    const rectB = _mapB.getContainer().getBoundingClientRect();
    const chainA = _chainA || _globalChain();
    const chainB = _chainB || _globalChain();
    if (chainA?.state && chainA?.edges) {
      _drawOnMap(_map,  0,           { clipX0: 0,           clipX1: rectB.left }, chainA);
    }
    if (chainB?.state && chainB?.edges) {
      _drawOnMap(_mapB, rectB.left,  { clipX0: rectB.left,  clipX1: W },          chainB);
    }
  } else if (_state) {
    _drawOnMap(_map, 0, null, _globalChain());
  }
}

function _globalChain() {
  return {
    edges:           _edges,
    everInfected:    _everInfected,
    prevInfectious:  _prevInfectious,
    state:           _state,
  };
}

function _drawOnMap(map, offsetX, clip, chain) {
  const W = window.innerWidth, H = window.innerHeight;
  const buildings = _layout.buildings;
  const state = chain.state;
  if (!state) return;
  const seeds = Array.isArray(state.patient_zero) ? state.patient_zero : [];
  const t = (performance.now() - _t0) / 1000;
  const cells = state.cells || [];
  const minX = -CULL_MARGIN - offsetX, minY = -CULL_MARGIN;
  const maxX = W + CULL_MARGIN - offsetX, maxY = H + CULL_MARGIN;
  const projCache = new Map();
  const _proj = idx => {
    let p = projCache.get(idx);
    if (p) return p;
    const b = buildings[idx];
    if (!b) return null;
    const pp = map.project([b.lon, b.lat]);
    p = { x: pp.x + offsetX, y: pp.y };
    projCache.set(idx, p);
    return p;
  };
  // Clip to this map's half so arcs from the other side don't bleed
  // across the divider during zoom/pan.
  if (clip) {
    _ctx.save();
    _ctx.beginPath();
    _ctx.rect(clip.clipX0, 0, clip.clipX1 - clip.clipX0, H);
    _ctx.clip();
  }
  // Two passes: first collect on-screen candidates + the max per-edge
  // intensity (used to normalise color/thickness/speed ramps), then
  // draw at most MAX_ARCS of them.
  const curDay        = state.day;
  const isTimestamp   = _options.temporal === 'timestamp';
  // Match the legend's active colour mode so arcs only connect buildings
  // that are visibly highlighted in the city paint. Without this, the
  // "Infection sites" mode painted only buildings with infections_here>0
  // red but the flow tree still drew arcs through long-recovered grey
  // ones, which read as lines floating between unrelated buildings.
  const _vizMode = (() => { try { return getColorMode(); } catch { return 'infection'; } })();
  const _gateField = _VIZ_FIELD_BY_MODE[_vizMode] || 'I';
  const _gateMin   = _VIZ_MIN_BY_MODE[_vizMode]   ?? 0.3;
  const candidates = [];
  let maxI = 0;
  // De-dupe identical (parent, child, day) tuples so a chain accidentally
  // ingested twice doesn't overdraw the same arc.
  const _seenEdge = new Set();
  const edges = chain.edges || [];
  // Walk newest-first so the cap (MAX_ARCS) keeps the latest waves
  // rather than truncating them — the user is usually looking at
  // current spread, not the origin tree from day 0.
  for (let ei = edges.length - 1; ei >= 0; ei--) {
    const edge = edges[ei];
    const child  = edge.child;
    const parent = edge.parent;
    const cc = cells[child];
    const cp = cells[parent];
    if (!cc) continue;
    // Both endpoints must clear the visibility threshold for the
    // current colour mode. A patient-zero seed has no parent cell, but
    // it's drawn separately at the end of _drawOnMap so we just skip
    // the arc when its parent vanished from the viz.
    if ((cc[_gateField] ?? 0) <= _gateMin) continue;
    if (!cp || (cp[_gateField] ?? 0) <= _gateMin) continue;
    if (_selectedIdx != null && parent !== _selectedIdx && child !== _selectedIdx) continue;
    if (isTimestamp && edge.day !== curDay) continue;
    const tag = parent + ':' + child + ':' + edge.day;
    if (_seenEdge.has(tag)) continue;
    _seenEdge.add(tag);
    const a = _proj(parent), b = _proj(child);
    if (!a || !b) continue;
    const aIn = a.x > -CULL_MARGIN && a.x < W + CULL_MARGIN && a.y > minY && a.y < maxY;
    const bIn = b.x > -CULL_MARGIN && b.x < W + CULL_MARGIN && b.y > minY && b.y < maxY;
    if (!aIn && !bIn) continue;
    // Intensity ramps use the same field that gates visibility so the
    // brightness ramp lines up with the basemap paint (infection-sites
    // arcs get brighter where the destination has hosted more
    // transmissions, exposed-mode arcs get brighter where E is higher,
    // etc.).
    const intensity = cc[_gateField] ?? cc.I ?? 0;
    if (intensity > maxI) maxI = intensity;
    candidates.push({ child, parent, a, b, intensity });
    if (candidates.length >= MAX_ARCS) break;
  }
  const denom = maxI > 0 ? maxI : 1;
  for (const e of candidates) {
    const norm = Math.min(1, e.intensity / denom);
    const off  = _stableHash(e.parent, e.child);
    _drawArc(e.a, e.b, t, off, norm);
  }
  for (const s of seeds) {
    const sb = buildings[s];
    if (!sb) continue;
    const pp = map.project([sb.lon, sb.lat]);
    const px = pp.x + offsetX, py = pp.y;
    const r = 5 + Math.sin(t * 4) * 2;
    _ctx.beginPath();
    _ctx.arc(px, py, r * 2.6, 0, Math.PI * 2);
    _ctx.fillStyle = 'rgba(255,255,255,.18)';
    _ctx.fill();
    _ctx.beginPath();
    _ctx.arc(px, py, r, 0, Math.PI * 2);
    _ctx.fillStyle = '#ffd6c4';
    _ctx.fill();
  }
  if (clip) _ctx.restore();
}

/**
 * Attach a second map for compare side-by-side. The chain is rendered
 * once per map so arcs appear in both halves. Move/zoom/rotate events
 * on the secondary map also re-render the canvas so the right half
 * tracks its own camera.
 */
export function setSecondaryMap(mapB) {
  if (!mapB) return;
  _mapB = mapB;
  _mapB.on('move',   _draw);
  _mapB.on('zoom',   _draw);
  _mapB.on('rotate', _draw);
  _mapB.on('pitch',  _draw);
  _draw();
}

export function unsetSecondaryMap() {
  if (_mapB) {
    try {
      _mapB.off('move',   _draw);
      _mapB.off('zoom',   _draw);
      _mapB.off('rotate', _draw);
      _mapB.off('pitch',  _draw);
    } catch {}
    _mapB = null;
  }
  _draw();
}

// Yellow → orange → red ramp used when colorMode === 'intensity'. Input
// `u` is the normalised per-edge intensity in [0, 1].
function _rampColor(u) {
  const c0 = [255, 236, 153]; // soft yellow
  const c1 = [255, 138,  61]; // orange
  const c2 = [200,  52,  28]; // red
  let r, g, b;
  if (u < 0.5) {
    const k = u / 0.5;
    r = c0[0] + (c1[0] - c0[0]) * k;
    g = c0[1] + (c1[1] - c0[1]) * k;
    b = c0[2] + (c1[2] - c0[2]) * k;
  } else {
    const k = (u - 0.5) / 0.5;
    r = c1[0] + (c2[0] - c1[0]) * k;
    g = c1[1] + (c2[1] - c1[1]) * k;
    b = c1[2] + (c2[2] - c1[2]) * k;
  }
  return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
}

function _drawArc(a, b, t, stableOffset, intensity) {
  const norm = Math.max(0, Math.min(1, intensity ?? 1));
  const dx = b.x - a.x, dy = b.y - a.y;
  const len = Math.hypot(dx, dy);
  if (len < 2) return;
  // Perpendicular lift — curvature derived from a stable per-edge hash
  // so the bezier control point doesn't shift on zoom/pan. Capped in
  // absolute pixels so long edges at high zoom don't balloon into a
  // curve that sweeps across the whole viewport.
  const nx = -dy / len, ny = dx / len;
  let lift = ARC_LIFT * len * (stableOffset - 0.5) * 2;
  if (lift >  MAX_LIFT_PX) lift =  MAX_LIFT_PX;
  if (lift < -MAX_LIFT_PX) lift = -MAX_LIFT_PX;
  const cx = (a.x + b.x) / 2 + nx * lift;
  const cy = (a.y + b.y) / 2 + ny * lift;
  // Per-option styling. The "constant" branches reproduce the original
  // look (#ffd9b8, 1.5 px, ~28 px/s dash speed) bit-for-bit so the
  // default flow toggle still matches every screenshot in the docs.
  const color = _options.colorMode === 'intensity'
    ? _rampColor(norm)
    : '#ffd9b8';
  const lineWidth = _options.thicknessMode === 'intensity'
    ? 0.8 + 2.6 * norm
    : 1.5;
  const speedMult = _options.speedMode === 'intensity'
    ? 0.3 + 1.9 * norm
    : 1;
  _ctx.save();
  _ctx.setLineDash(_DASH);
  _ctx.lineDashOffset = -((t * 28 * speedMult) % 11);
  _ctx.strokeStyle = color;
  _ctx.lineWidth   = lineWidth;
  _ctx.beginPath();
  _ctx.moveTo(a.x, a.y);
  _ctx.quadraticCurveTo(cx, cy, b.x, b.y);
  _ctx.stroke();
  _ctx.restore();
  if (!_options.arrowhead) return;
  // Arrowhead near the child end. Sized so it tracks the stroke width
  // when intensity drives thickness — otherwise a thick arc would have
  // a comically small arrowhead at its tip.
  const headT = 0.92;
  const hx = (1 - headT) * (1 - headT) * a.x + 2 * (1 - headT) * headT * cx + headT * headT * b.x;
  const hy = (1 - headT) * (1 - headT) * a.y + 2 * (1 - headT) * headT * cy + headT * headT * b.y;
  const ang = Math.atan2(b.y - hy, b.x - hx);
  const headSize = 7 + Math.max(0, lineWidth - 1.5) * 1.4;
  _ctx.fillStyle = color;
  _ctx.beginPath();
  _ctx.moveTo(b.x, b.y);
  _ctx.lineTo(b.x - headSize * Math.cos(ang - 0.4), b.y - headSize * Math.sin(ang - 0.4));
  _ctx.lineTo(b.x - headSize * Math.cos(ang + 0.4), b.y - headSize * Math.sin(ang + 0.4));
  _ctx.closePath();
  _ctx.fill();
}
