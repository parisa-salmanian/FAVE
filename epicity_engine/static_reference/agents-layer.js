/**
 * agents-layer.js — visualises individual ABM agents as coloured points
 * floating over the 3D city. Connects the per-building SEIR aggregate the
 * specialist is looking at on screen to the per-agent representation the
 * engine is actually running, so "infection at building X" is grounded
 * in a visible cluster of red dots rather than just a heatmap change.
 *
 * Wire-up:
 *   - ui.js calls `initAgentsLayer({ THREE, scene })` once the city is
 *     loaded.
 *   - The sidebar toggle calls `setAgentsLayerActive(on)`.
 *   - `refreshAgents()` re-fetches the snapshot from /api/agent_positions
 *     and updates the Points buffers. ui.js calls it on every day
 *     boundary (the same hook as the per-day stats refresh).
 *
 * Static-snapshot mode (current): each dot sits at the agent's
 * backend-reported world-XY at the moment the snapshot was fetched
 * (`cx`, `cz`). When refreshAgents() lands a new snapshot, dots
 * teleport to their new positions — no per-frame interpolation, no
 * commute walk, no jitter. The earlier sub-day animation made the
 * cloud constantly slosh between home/work and was hard to read; this
 * frozen-frame variant is easier for a viewer to follow ("at this
 * day/hour these agents are here, period"). The schedule helpers
 * below (_scheduleAt etc.) are no longer called and can be removed
 * if we never restore the walk.
 *
 * Renders as THREE.Points with per-vertex colour. The buffer is
 * preallocated to the cap (MAX_AGENTS_DISPLAY) so re-fetches never
 * trigger a geometry rebuild — only buffer.needsUpdate flips.
 */

import * as THREE from 'three';

const MAX_AGENTS_DISPLAY = 8000;   // matches the server-side default cap
const DEFAULT_STATES     = 'E,I,R,D';  // skip S so the cloud is readable
const PATH_LEN           = 5;          // backend always returns 5 waypoints

// Per-agent activity jitter while "static" (at home / at work / at a
// leisure spot). Without this, dots that are nominally fixed look
// frozen; a small breathing oscillation makes the cloud feel alive.
// Time is denominated in SIMULATED hours (`flowHour`) so the jitter
// freezes whenever the sim is paused and speeds up under the 2x/3x
// pips — same coupling as the commute walk.
const JITTER_AMPLITUDE_M  = 3.0;
const JITTER_CYC_PER_HOUR = 0.12;  // ≈ 3 cycles per simulated day
const JITTER_CYC_SPREAD   = 0.04;  // per-agent random freq offset

// SEIR colours — match the rest of the UI's --s-{s,e,i,r,d} CSS vars.
const STATE_COLOR = {
  0: new THREE.Color('#7aa9d6'),  // S — soft blue
  1: new THREE.Color('#f4c94a'),  // E — yellow
  2: new THREE.Color('#e3503c'),  // I — red
  3: new THREE.Color('#7bb674'),  // R — green
  4: new THREE.Color('#2a2a2a'),  // D — near-black
};

let _scene        = null;
let _points       = null;
let _geo          = null;
let _mat          = null;
let _active       = false;
let _snapshot     = null;
let _last_fetch_t = 0;
let _refresh_lock = false;

// CPU-side mirror of the world-XY data so the per-frame lerp can
// recompute positions without re-fetching.
let _data = {
  n:         0,
  hx:        null,    // Float32Array length MAX — home X
  hz:        null,
  cx:        null,    // current snapshot location (commute/work/home)
  cz:        null,
  wx:        null,    // work X
  wz:        null,
  state:     null,    // Int32Array
  // Street-snapped commute polyline. PATH_LEN waypoints per agent
  // stored flat: agent i's path occupies indices [i*PATH_LEN, (i+1)*PATH_LEN).
  // Path[0] = home, Path[PATH_LEN-1] = work, intermediates are road
  // points the backend snapped via abm_paths.RoadGrid.
  px:        null,
  pz:        null,
  has_path:  false,
  fetched_hour: 9.0,  // hour-of-day at which cx/cz were captured
};

// Scenario knobs pushed in from ui.js whenever a new sim day lands.
// Schedule branches on `is_weekend`; the climate effect dampens both
// commute distance and jitter so rainy/snowy/stormy days visibly cut
// agent activity. Adjusting these without re-fetching is safe — the
// per-frame _writePositions() reads them straight from this object.
let _scenario = {
  is_weekend: false,
  weather:    null,   // 'clear' | 'rain' | 'snow' | 'storm' | null
  season:     'Spring',
};

export function setAgentsScenario(s) {
  if (!s) return;
  if (s.is_weekend != null) _scenario.is_weekend = !!s.is_weekend;
  if (s.weather    !== undefined) _scenario.weather = s.weather || null;
  if (s.season)             _scenario.season = String(s.season);
}



export function initAgentsLayer({ scene, y = 2.0 } = {}) {
  if (!scene) throw new Error('agents-layer: scene required');
  _scene = scene;
  if (_points) {
    // Already initialised — caller may have re-called after a city
    // switch. Keep the layer but reset its buffers.
    _data.n = 0;
    _geo.setDrawRange(0, 0);
    return;
  }
  const positions = new Float32Array(MAX_AGENTS_DISPLAY * 3);
  const colors    = new Float32Array(MAX_AGENTS_DISPLAY * 3);
  // Preinit the Y component to the requested height; per-frame lerp
  // only touches X (idx*3) and Z (idx*3+2).
  for (let i = 0; i < MAX_AGENTS_DISPLAY; i++) positions[i * 3 + 1] = y;
  _geo = new THREE.BufferGeometry();
  _geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
  _geo.setAttribute('color',    new THREE.BufferAttribute(colors,    3));
  _geo.setDrawRange(0, 0);
  _mat = new THREE.PointsMaterial({
    size:            6,
    sizeAttenuation: false,
    vertexColors:    true,
    transparent:     true,
    opacity:         0.92,
    depthWrite:      false,
  });
  _points = new THREE.Points(_geo, _mat);
  _points.renderOrder = 800;
  _points.visible = false;
  _data.hx = new Float32Array(MAX_AGENTS_DISPLAY);
  _data.hz = new Float32Array(MAX_AGENTS_DISPLAY);
  _data.cx = new Float32Array(MAX_AGENTS_DISPLAY);
  _data.cz = new Float32Array(MAX_AGENTS_DISPLAY);
  _data.wx = new Float32Array(MAX_AGENTS_DISPLAY);
  _data.wz = new Float32Array(MAX_AGENTS_DISPLAY);
  _data.px = new Float32Array(MAX_AGENTS_DISPLAY * PATH_LEN);
  _data.pz = new Float32Array(MAX_AGENTS_DISPLAY * PATH_LEN);
  _data.state = new Int32Array(MAX_AGENTS_DISPLAY);
  _scene.add(_points);
}


export function setAgentsLayerActive(on) {
  _active = !!on;
  if (!_points) return;
  _points.visible = _active && _data.n > 0;
  if (_active) refreshAgents();
}

export function isAgentsLayerActive() { return _active; }

/**
 * Per-frame tick. No-op in static-snapshot mode — positions are
 * committed once by _ingest() when a new snapshot lands and stay
 * frozen until the next refresh. Kept exported (and called from
 * ui.js's render loop) so the wire-up survives if we ever restore
 * the sub-day walk.
 */
export function tickAgents(_hour) {
  /* no-op — see file header */
}


/**
 * Re-fetch the latest per-agent snapshot and copy it into the GPU buffers.
 * Throttled to one request per second so day-step + scrub events don't
 * both fire it within the same render frame.
 */
export async function refreshAgents() {
  if (!_points || !_active) return;
  const now = performance.now();
  if (_refresh_lock) return;
  if (now - _last_fetch_t < 800) return;
  _refresh_lock = true;
  try {
    const r = await fetch(`/api/agent_positions?states=${DEFAULT_STATES}` +
                          `&max_agents=${MAX_AGENTS_DISPLAY}` +
                          `&include_home_work=1&include_path=1`);
    if (!r.ok) return;
    const j = await r.json();
    _ingest(j);
    _last_fetch_t = performance.now();
  } catch (_) {
    /* network blip — leave previous frame in place */
  } finally {
    _refresh_lock = false;
  }
}


function _ingest(j) {
  if (!j || !j.agents_available) {
    _data.n = 0;
    if (_geo) _geo.setDrawRange(0, 0);
    if (_points) _points.visible = false;
    _snapshot = null;
    return;
  }
  const n = Math.min(MAX_AGENTS_DISPLAY, j.n | 0);
  _data.n            = n;
  _data.fetched_hour = +j.hour || 0;
  _data.has_path     = !!(j.path_x && j.path_z && (j.path_len | 0) === PATH_LEN);
  for (let i = 0; i < n; i++) {
    _data.cx[i]    = j.x[i]  || 0;
    _data.cz[i]    = j.z[i]  || 0;
    _data.hx[i]    = (j.hx && j.hx[i] != null) ? j.hx[i] : _data.cx[i];
    _data.hz[i]    = (j.hz && j.hz[i] != null) ? j.hz[i] : _data.cz[i];
    _data.wx[i]    = (j.wx && j.wx[i] != null) ? j.wx[i] : _data.cx[i];
    _data.wz[i]    = (j.wz && j.wz[i] != null) ? j.wz[i] : _data.cz[i];
    _data.state[i] = j.state[i] | 0;
    if (_data.has_path) {
      const row_x = j.path_x[i];
      const row_z = j.path_z[i];
      const base  = i * PATH_LEN;
      for (let k = 0; k < PATH_LEN; k++) {
        _data.px[base + k] = (row_x && row_x[k] != null) ? row_x[k] : _data.hx[i];
        _data.pz[base + k] = (row_z && row_z[k] != null) ? row_z[k] : _data.hz[i];
      }
    }
  }
  _writeColors();
  _writePositions(_data.fetched_hour);
  _snapshot = j;
  _geo.setDrawRange(0, n);
  if (_points) _points.visible = _active && n > 0;
}


function _writeColors() {
  const colors = _geo.attributes.color.array;
  for (let i = 0; i < _data.n; i++) {
    const c = STATE_COLOR[_data.state[i]] || STATE_COLOR[0];
    colors[i * 3]     = c.r;
    colors[i * 3 + 1] = c.g;
    colors[i * 3 + 2] = c.b;
  }
  _geo.attributes.color.needsUpdate = true;
}


/**
 * Resolve "what is each agent doing at hour h" into one of four modes
 * plus a 0..1 progress fraction for the path-following modes. The
 * schedule differs between weekdays and weekends:
 *
 *   Weekday — heavily activity-padded so dots are visibly moving for
 *   ~half the day, not just two 1-hour commute slivers:
 *     00–06   home (jitter)
 *     06–10   home → work (long morning commute on the road path)
 *     10–12   work (jitter)
 *     12–14   lunch dart — wander to mid-path waypoint and back
 *     14–17   work (jitter)
 *     17–21   work → home (long evening commute)
 *     21–24   home (jitter)
 *
 *   Weekend — no work commute; agents instead drift to a leisure
 *   waypoint (the path's midpoint stands in for "the city centre" /
 *   nearest POI cluster) and back later in the day:
 *     00–10   home (jitter, slower wake-up)
 *     10–14   home → leisure (slow walk along the path)
 *     14–17   leisure (jitter at the midpoint)
 *     17–21   leisure → home (slow walk back)
 *     21–24   home (jitter)
 *
 * Climate dampening: rain/snow/storm scales the commute distance back
 * toward home (people stay closer to their starting point) and cuts the
 * jitter amplitude in half. Sun / clear get the full schedule.
 */
function _scheduleAt(h) {
  if (_scenario.is_weekend) {
    if      (h < 10) return { mode: 'home',     t: 0 };
    else if (h < 14) return { mode: 'forward',  t: (h - 10) / 4 };
    else if (h < 17) return { mode: 'leisure',  t: 0 };
    else if (h < 21) return { mode: 'backward', t: (h - 17) / 4 };
    else             return { mode: 'home',     t: 0 };
  }
  // Weekday.
  if      (h <  6) return { mode: 'home',      t: 0 };
  else if (h < 10) return { mode: 'forward',   t: (h - 6) / 4 };
  else if (h < 12) return { mode: 'work',      t: 0 };
  else if (h < 14) return { mode: 'lunch',     t: (h - 12) / 2 };
  else if (h < 17) return { mode: 'work',      t: 0 };
  else if (h < 21) return { mode: 'backward',  t: (h - 17) / 4 };
  else             return { mode: 'home',      t: 0 };
}

function _climateMultiplier() {
  switch (_scenario.weather) {
    case 'storm': return 0.25;
    case 'snow':  return 0.45;
    case 'rain':  return 0.65;
    default:      return 1.0;
  }
}

function _pathPoint(i, t, out) {
  // Sample agent i's polyline at parameter t ∈ [0, 1]. Falls back to a
  // straight-line lerp between home and work when the backend hasn't
  // shipped a path (older snapshots / engines without abm_paths).
  if (_data.has_path) {
    const segs    = PATH_LEN - 1;
    const segT    = Math.max(0, Math.min(1, t)) * segs;
    const segIdx  = Math.min(segs - 1, Math.max(0, Math.floor(segT)));
    const segFrac = segT - segIdx;
    const base    = i * PATH_LEN;
    const ax = _data.px[base + segIdx];
    const az = _data.pz[base + segIdx];
    const bx = _data.px[base + segIdx + 1];
    const bz = _data.pz[base + segIdx + 1];
    out.x = ax + (bx - ax) * segFrac;
    out.z = az + (bz - az) * segFrac;
  } else {
    const u = Math.max(0, Math.min(1, t));
    out.x = _data.hx[i] + (_data.wx[i] - _data.hx[i]) * u;
    out.z = _data.hz[i] + (_data.wz[i] - _data.hz[i]) * u;
  }
}

const _pp = { x: 0, z: 0 };   // scratch object so _pathPoint doesn't allocate

function _writePositions(_hour) {
  // Static-snapshot mode: each agent is rendered at the backend-
  // reported (cx, cz) for the moment the latest snapshot was fetched.
  // When refreshAgents() lands a new snapshot, dots teleport to the
  // new positions (the previous slot in the buffer is just overwritten
  // — the visual effect is "delete-and-add at new location").
  //
  // The `hour` argument is ignored. Previously this function ran the
  // home→commute→work→commute→home schedule plus per-agent jitter
  // every frame; that animation was confusing to read, so it was
  // replaced with this frozen-frame variant. The schedule helpers
  // above (_scheduleAt, _climateMultiplier, _pathPoint) are no longer
  // used by this writer.
  const pos = _geo.attributes.position.array;
  for (let i = 0; i < _data.n; i++) {
    pos[i * 3]     = _data.cx[i];
    pos[i * 3 + 2] = _data.cz[i];
  }
  _geo.attributes.position.needsUpdate = true;
}


/** Drop the GPU points (used when leaving the city). */
export function disposeAgentsLayer() {
  if (!_scene || !_points) return;
  _scene.remove(_points);
  _points.geometry.dispose();
  _points.material.dispose();
  _points = null;
  _geo    = null;
  _mat    = null;
  _data.n = 0;
}
