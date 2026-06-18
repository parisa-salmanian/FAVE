/**
 * transport.js — TransportLayer: 3D vehicles (planes, trains, trams, buses)
 * animated against the synthetic schedule from /api/transport.
 *
 * Architecture:
 *   - One THREE.InstancedMesh per vehicle type. Capacity is sized for the
 *     worst-case concurrent vehicles in any Swedish city (~256 buses, etc.).
 *   - Trips are admitted/retired with two cursors (sorted by depart and
 *     arrive minute) so per-frame work is O(active), not O(total trips).
 *   - Per active trip we compute progress along the line's baked polyline
 *     (or a parametric arc for planes), build a transformation matrix and
 *     write it into the instance.
 *   - The toggle controls **visuals only**. The epi coupling lives in the
 *     backend dispatcher and runs regardless of whether the layer is shown.
 *
 * Coordinate system: same as view.js — world_x = East (X+), world_z = South
 * (Z+), world_y = Up (Y+).
 */

import * as THREE from 'three';

// ── Capacity ─────────────────────────────────────────────────────────────────
// One instanced mesh per vehicle type. Sized generously so a peak-hour
// Stockholm doesn't run out of slots. The "_headlight" entries are a
// parallel pool of additive-blended head-lamp meshes that share slot
// indices with their corresponding bus type — see _updateInstance.
const MAX_INSTANCES = {
  bus:                256,
  bus_b:              256,
  bus_c:              256,
  bus_headlight:      256,
  bus_b_headlight:    256,
  bus_c_headlight:    256,
  ferry:               32,
  ferry_headlight:     32,
  tram:               128,
  tram_headlight:     128,
  train:              128,
  train_b:            128,
  train_c:            128,
  train_oresund:      128,
  train_headlight:    128,
  train_b_headlight:  128,
  train_c_headlight:  128,
  train_oresund_headlight: 128,
  plane:               64,
};

// Variant lists — trimmed by _configureBranding() to match operator count.
const _ALL_BUS_VARIANTS   = ['bus', 'bus_b', 'bus_c'];
const _ALL_TRAIN_VARIANTS = ['train', 'train_b', 'train_c'];
// Öresundståg operators — matched by name fragment in _configureBranding()
const _ORESUND_NAMES = ['resund', 'dsbfirst', 'dsb småland', 'dsb sm'];
let BUS_VARIANTS   = _ALL_BUS_VARIANTS.slice();
let TRAIN_VARIANTS = _ALL_TRAIN_VARIANTS.slice();

// ── Operator-driven branding ────────────────────────────────────────────────
// Populated by _configureBranding() from /api/transport operator data.
// Falls back to hardcoded defaults when no operator data is present.

const _CSS_COLORS = {
  red: 0xff0000, blue: 0x0000ff, green: 0x008000, yellow: 0xffff00,
  gold: 0xffd700, white: 0xffffff, black: 0x000000, orange: 0xffa500,
  grey: 0x808080, silver: 0xc0c0c0, purple: 0x800080, pink: 0xffc0cb,
};

function _hexToInt(hex) {
  if (typeof hex === 'number') return hex;
  if (!hex) return null;
  const s = String(hex).trim().toLowerCase();
  if (_CSS_COLORS[s] !== undefined) return _CSS_COLORS[s];
  const n = parseInt(s.replace(/^#/, ''), 16);
  return isNaN(n) ? null : n;
}

function _darkenCol(color, factor) {
  const r = ((color >> 16) & 0xff) * factor;
  const g = ((color >> 8) & 0xff) * factor;
  const b = (color & 0xff) * factor;
  return (Math.round(r) << 16) | (Math.round(g) << 8) | Math.round(b);
}

function _lightenCol(color, factor) {
  const r = (color >> 16) & 0xff;
  const g = (color >> 8) & 0xff;
  const b = color & 0xff;
  return (Math.round(r + (255 - r) * factor) << 16) |
         (Math.round(g + (255 - g) * factor) << 8) |
         Math.round(b + (255 - b) * factor);
}

function _clampBrightness(color, minBrightness) {
  // Ensure a colour has at least *minBrightness* (0-255) in its max channel
  // so very dark brand colours (pure black) still produce visible vehicles.
  const r = (color >> 16) & 0xff;
  const g = (color >> 8) & 0xff;
  const b = color & 0xff;
  const mx = Math.max(r, g, b);
  if (mx >= minBrightness) return color;
  const boost = minBrightness / Math.max(mx, 1);
  return (Math.min(255, Math.round(r * boost + 30)) << 16) |
         (Math.min(255, Math.round(g * boost + 30)) << 8) |
         Math.min(255, Math.round(b * boost + 30));
}

function _deriveBusPalette(bodyColor) {
  const c = _clampBrightness(bodyColor, 50);
  return {
    BODY: c, LOWER: _darkenCol(c, 0.45),
    WINDOW: 0x1e293b, BUMPER: 0x292524, WHEEL: 0x1c1917,
    GRILLE: 0x44403c, ROOF_AC: 0x78716c, MIRROR: 0x44403c,
    STRIPE: _lightenCol(c, 0.6),
  };
}

function _deriveTrainPalette(bodyColor) {
  const c = _clampBrightness(bodyColor, 50);
  return {
    BODY: c, STRIPE: _lightenCol(c, 0.55),
    WINDOW: 0x111827, BOGIE: 0x1c1917,
    ROOF: _lightenCol(c, 0.45), LOWER: _darkenCol(c, 0.55),
    CAB_FACE: c, CHEVRON: _lightenCol(c, 0.7),
    HEADLIGHT: 0xfef3c7,
  };
}

// Default palettes — exact replicas of the original hardcoded values
const _DEFAULT_BUS_PALETTES = [
  { BODY: 0xdc2626, LOWER: 0x7f1d1d, WINDOW: 0x1e293b, BUMPER: 0x292524,
    WHEEL: 0x1c1917, GRILLE: 0x44403c, ROOF_AC: 0x78716c, MIRROR: 0x44403c,
    STRIPE: 0xffffff },
  { BODY: 0x15803d, LOWER: 0x0a4d25, WINDOW: 0x1e293b, BUMPER: 0x1c1917,
    WHEEL: 0x1c1917, GRILLE: 0x374151, ROOF_AC: 0x6b7280, MIRROR: 0x374151,
    STRIPE: 0xa3e635 },
  { BODY: 0xea580c, LOWER: 0x9a3412, WINDOW: 0x1e293b, BUMPER: 0x292524,
    WHEEL: 0x1c1917, GRILLE: 0x44403c, ROOF_AC: 0x78716c, MIRROR: 0x374151,
    STRIPE: 0xfbbf24 },
];
const _DEFAULT_TRAIN_PALETTES = [
  { BODY: 0x3b5bdb, STRIPE: 0xc0c0c0, WINDOW: 0x111827, BOGIE: 0x1c1917,
    ROOF: 0x94a3b8, LOWER: 0x374151, CAB_FACE: 0x3b5bdb,
    CHEVRON: 0xfbbf24, HEADLIGHT: 0xfef3c7 },
  { BODY: 0xd1d5db, STRIPE: 0x1d4ed8, WINDOW: 0x111827, BOGIE: 0x1c1917,
    ROOF: 0x9ca3af, LOWER: 0x4b5563, CAB_FACE: 0xb0b8c4,
    CHEVRON: 0x1d4ed8, HEADLIGHT: 0xfef3c7 },
  { BODY: 0xf1f5f9, STRIPE: 0x2563eb, WINDOW: 0x111827, BOGIE: 0x1c1917,
    ROOF: 0xcbd5e1, LOWER: 0x64748b, CAB_FACE: 0xe2e8f0,
    CHEVRON: 0x2563eb, HEADLIGHT: 0xfef3c7 },
];
const _DEFAULT_TRAM_COLOR  = 0x16a34a;
const _DEFAULT_PLANE_COLOR = 0x65a30d;

let _busPalettes   = _DEFAULT_BUS_PALETTES.slice();
let _trainPalettes = _DEFAULT_TRAIN_PALETTES.slice();
let _tramBodyColor  = _DEFAULT_TRAM_COLOR;
let _planeBodyColor = _DEFAULT_PLANE_COLOR;

function _configureBranding(operators) {
  _operators = operators || [];
  _busPalettes   = _DEFAULT_BUS_PALETTES.slice();
  _trainPalettes = _DEFAULT_TRAIN_PALETTES.slice();
  _tramBodyColor  = _DEFAULT_TRAM_COLOR;
  _planeBodyColor = _DEFAULT_PLANE_COLOR;
  BUS_VARIANTS   = _ALL_BUS_VARIANTS.slice();
  TRAIN_VARIANTS = _ALL_TRAIN_VARIANTS.slice();

  if (!operators || !operators.length) return;

  // Group operators by mode (already sorted by route_count in backend)
  const byMode = {};
  for (const op of operators) {
    for (const mode of (op.modes || [])) {
      if (!byMode[mode]) byMode[mode] = [];
      byMode[mode].push(op);
    }
  }

  // Buses: use only as many variants as there are operators with colours.
  // 1 operator → all buses are that colour; 2 → alternate; 3 → cycle 3.
  const busOps = (byMode['bus'] || []).filter(o => _hexToInt(o.colour) !== null);
  if (busOps.length) {
    const n = Math.min(3, busOps.length);
    for (let i = 0; i < n; i++) {
      _busPalettes[i] = _deriveBusPalette(_hexToInt(busOps[i].colour));
    }
    BUS_VARIANTS = _ALL_BUS_VARIANTS.slice(0, n);
  }

  // Trains: same logic, plus detect Öresundståg for dedicated model
  const allTrainOps = byMode['train'] || [];
  const hasOresund = allTrainOps.some(o => {
    const nm = (o.brand || o.name || '').toLowerCase();
    return _ORESUND_NAMES.some(frag => nm.includes(frag));
  });
  const trainOps = allTrainOps.filter(o => {
    if (_hexToInt(o.colour) === null) return false;
    // Exclude Öresundståg from generic palette — it has its own geometry
    if (hasOresund) {
      const nm = (o.brand || o.name || '').toLowerCase();
      if (_ORESUND_NAMES.some(frag => nm.includes(frag))) return false;
    }
    return true;
  });
  if (trainOps.length) {
    const n = Math.min(3, trainOps.length);
    for (let i = 0; i < n; i++) {
      _trainPalettes[i] = _deriveTrainPalette(_hexToInt(trainOps[i].colour));
    }
    TRAIN_VARIANTS = _ALL_TRAIN_VARIANTS.slice(0, n);
  }
  if (hasOresund) {
    TRAIN_VARIANTS.push('train_oresund');
  }

  // Tram: primary operator
  const tramOps = (byMode['tram'] || []).filter(o => _hexToInt(o.colour) !== null);
  if (tramOps.length) _tramBodyColor = _hexToInt(tramOps[0].colour);

  // Plane: primary airline
  const planeOps = (byMode['plane'] || []).filter(o => _hexToInt(o.colour) !== null);
  if (planeOps.length) _planeBodyColor = _hexToInt(planeOps[0].colour);
}

// Cruising altitude for planes (world units = metres). 600 m looks right
// for a city-scale view; raise it for a flatter look.
const PLANE_CRUISE_ALT = 600;

// Procedural geometry sizes (world units = metres). Scaled up vs. real
// vehicles so they're visible against building geometry without zooming.
const VEHICLE_SCALE = 2.5;

// Visual movement speed at sim slider = 1x, in world metres per REAL
// second. Multiplied by `simSpeed` in update() so cranking the speed
// slider also speeds up the vehicles. The schedule only drives the
// BACKEND epi coupling — visually each line has a small persistent
// pool of vehicles walking back and forth at this base speed × the
// current slider value.
// One base speed for ALL ground vehicles, in world metres per REAL
// second. Bus, tram, train all use this same value so the user only
// has one knob to tune. Multiplied by `simSpeed` in update() so the
// speed slider also drives vehicle pace.
const VEHICLE_BASE_SPEED = 45;

const VEHICLE_VISUAL_SPEED = {
  bus:      VEHICLE_BASE_SPEED,
  bus_b:    VEHICLE_BASE_SPEED,
  bus_c:    VEHICLE_BASE_SPEED,
  tram:     VEHICLE_BASE_SPEED,
  train:    VEHICLE_BASE_SPEED,
  train_b:  VEHICLE_BASE_SPEED,
  train_c:  VEHICLE_BASE_SPEED,
  train_oresund: VEHICLE_BASE_SPEED,
  ferry:    VEHICLE_BASE_SPEED * 0.4,  // ferries are slower (~18 km/h)
  plane:    0,
};

// Lateral offset (metres) from the polyline centerline so opposing
// vehicles drive on the right side of the road instead of colliding
// head-on along the centerline. Sweden drives on the right.
const LANE_OFFSET = 3.5;

// Vehicles slow down — but never actually stop — when passing close
// to a stop. SLOW_RADIUS_M is the radius around each stop within
// which the speed factor ramps from 1 down to STOP_MIN_FACTOR. The
// minimum is non-zero so vehicles always keep moving smoothly.
const STOP_SLOW_RADIUS_M = 35;
const STOP_MIN_FACTOR    = 0.25;

// Proximity / collision avoidance: vehicles of the same base type slow
// down when another is close ahead.  A spatial hash grid (cleared every
// frame) keeps this O(N) instead of O(N²).
const _PROX_CELL     = 80;   // grid cell size in metres
const _PROX_MIN_DIST = 30;   // below this distance → start braking
const _PROX_MIN_DIST2 = _PROX_MIN_DIST * _PROX_MIN_DIST;
let   _proxGrid      = {};   // "cx,cz,baseType" → [vehicle indices]

// Planes can't share VEHICLE_BASE_SPEED directly because their
// polylines span tens of kilometres (50 km to a foreign destination)
// and walking that at 45 m/s would take 18 minutes — far too slow.
// Instead the plane plays its full takeoff/cruise/landing arc over
// this many REAL seconds at slider 1×, scaled by simSpeed below so
// cranking the speed slider also speeds up the plane in step with
// ground vehicles. Bumped from 60 → 120 so the plane feels paced
// with the others (per user feedback).
const PLANE_ARC_REAL_SEC = 120;
const PLANE_IDLE_REAL_SEC = 6;

// How many persistent visualizer vehicles to spawn per line. Multiple
// planes per line are evenly phased through the arc cycle so the user
// always sees something in the air.
const PERSISTENT_PER_LINE = {
  bus:   3,
  tram:  2,
  train: 3,   // bumped from 1 — train polylines are now ~12-20 km
  ferry: 2,
  plane: 3,
};

// ── Module state ─────────────────────────────────────────────────────────────

let _scene = null;
let _infra = null;          // {stops, routes, ...}
let _schedule = null;       // {lines, trips, ...}
let _linesById = new Map(); // line_id → line object
let _stopsByIdx = new Map();// stop_idx → stop object
let _operators = [];        // operator branding array from API

// Vehicle capacities mirroring backend MODE_PARAMS
const _CAPACITY = { bus: 40, tram: 80, train: 200, ferry: 150, plane: 180 };

// ── Hover / selection state ─────────────────────────────────────────────────
let _hoveredVi   = -1;   // index into _persistentVehicles, or -1
let _selectedVi  = -1;   // selected (click-locked) vehicle index, or -1
let _selRingMesh = null;  // yellow ring mesh following the selected vehicle

// Persistent visualizer pool. Each entry is one always-rendered vehicle
// that walks back and forth along its line's polyline (or, for planes,
// loops the takeoff/cruise/landing arc). Independent of the schedule's
// trip events — those drive the BACKEND epi coupling only.
//
// Shape: { lineId, type, slot, distance, direction, arcSec }
let _persistentVehicles = [];

let _slotPool = {};
let _nextSlot = {};
// Initialise from MAX_INSTANCES keys
for (const k of Object.keys(MAX_INSTANCES)) { _slotPool[k] = []; _nextSlot[k] = 0; }

let _instancedMeshes = {};   // type → THREE.InstancedMesh (vehicles)
let _stopMeshes = {};        // type → THREE.InstancedMesh (static stop landmarks)
let _runwayMesh = null;      // single Mesh for all runway strips
let _rootGroup = null;
let _visible = false;

// ── Building spatial grid (for future infection visualisation) ───────────────
const _HALO_GRID_CELL = 200; // metres — spatial grid cell size for buildings
let _bGrid      = null;      // Map<"gx,gz", [buildingIndex, ...]>
let _buildings   = null;     // reference to layout.buildings array
let _latestCells = null;     // reference to latest SEIR cell array from ui.js

// ── Transit mask icon state ─────────────────────────────────────────────────
let _maskMeshes = {};        // type → InstancedMesh (mask billboard sprites)
let _maskTex    = null;      // shared canvas texture for mask icon
const _MASK_SPRITE_Y    = 10;   // metres above ground
const _MASK_SPRITE_SIZE = 5;    // quad size in metres
const _MASK_SHOW_FRAC   = 0.3;  // show mask on ~30 % of vehicles (visual hint)


// Reused per-frame matrices (avoid GC pressure)
const _tmpMat = new THREE.Matrix4();
const _tmpPos = new THREE.Vector3();
const _tmpQuat = new THREE.Quaternion();
const _tmpScale = new THREE.Vector3(1, 1, 1);
const _tmpEuler = new THREE.Euler();

// ── Procedural vehicle geometries ────────────────────────────────────────────

function _buildPlaneGeometry() {
  // High-poly regional jet (~10k faces). Based on reference model sheet:
  // rounded fuselage, swept wings, underwing hex-cylinder engine pods on
  // struts, T-tail with high horizontal stabilisers, dual landing gear,
  // window rows, doors as flat polys.
  const BODY     = _planeBodyColor;               // operator brand fuselage
  const BELLY    = 0x9ca3af;                      // grey belly panel
  const WINDOW   = 0x1e293b;                      // dark glass
  const NOSE_TIP = 0x334155;                      // dark radome
  const WING     = _lightenCol(_planeBodyColor, 0.3); // muted wing
  const TAIL_FIN = _darkenCol(_planeBodyColor, 0.6);  // darker fin
  const ENGINE   = 0x374151;                      // dark engine cowl
  const EXHAUST  = 0x1f2937;                      // exhaust nozzle
  const GEAR     = 0x292524;                      // landing gear dark
  const DOOR     = _darkenCol(_planeBodyColor, 0.5);  // door
  const HS = 32;  // high segment count for smooth round sections
  const p = [];

  // ═══════════════════════════════════════════════════════════════════════
  // 1. FUSELAGE — multi-section tapered cylinder for smooth body
  // ═══════════════════════════════════════════════════════════════════════

  // Main fuselage barrel (constant cross-section)
  const fusMain = new THREE.CylinderGeometry(1.15, 1.15, 12.0, HS, 1);
  fusMain.rotateX(Math.PI / 2);
  fusMain.translate(0, 1.15, 0);
  p.push({ geo: fusMain, color: BODY });

  // Forward fuselage taper (wider → nose)
  const fusFwd = new THREE.CylinderGeometry(0.75, 1.15, 3.0, HS, 1);
  fusFwd.rotateX(Math.PI / 2);
  fusFwd.translate(0, 1.15, 7.5);
  p.push({ geo: fusFwd, color: BODY });

  // Nose cone (rounded tip — sphere-like)
  const noseCone = new THREE.CylinderGeometry(0.10, 0.75, 2.0, HS, 1);
  noseCone.rotateX(Math.PI / 2);
  noseCone.translate(0, 1.15, 10.0);
  p.push({ geo: noseCone, color: BODY });
  // Radome tip sphere
  const radome = new THREE.SphereGeometry(0.14, HS, HS);
  radome.translate(0, 1.15, 10.95);
  p.push({ geo: radome, color: NOSE_TIP });

  // Aft fuselage taper (barrel → tail)
  const fusAft = new THREE.CylinderGeometry(0.50, 1.15, 4.5, HS, 1);
  fusAft.rotateX(Math.PI / 2);
  fusAft.translate(0, 1.15, -8.25);
  p.push({ geo: fusAft, color: BODY });

  // Tail cone / APU exhaust
  const tailCone = new THREE.CylinderGeometry(0.08, 0.50, 1.5, HS, 1);
  tailCone.rotateX(Math.PI / 2);
  tailCone.translate(0, 1.15, -11.25);
  p.push({ geo: tailCone, color: EXHAUST });

  // Belly fairing (flattened underside strip)
  const belly = new THREE.BoxGeometry(1.8, 0.10, 13.0);
  belly.translate(0, 0.02, -0.5);
  p.push({ geo: belly, color: BELLY });

  // Upper fuselage spine highlight (subtle ridge)
  const spine = new THREE.BoxGeometry(0.3, 0.06, 12.0);
  spine.translate(0, 2.28, 0);
  p.push({ geo: spine, color: BODY });

  // ═══════════════════════════════════════════════════════════════════════
  // 2. COCKPIT WINDSHIELD — multi-pane wraparound
  // ═══════════════════════════════════════════════════════════════════════

  // Four windshield panes (2 upper, 2 lower)
  for (const dx of [-0.22, 0.22]) {
    // Upper pane
    const upPane = new THREE.BoxGeometry(0.38, 0.30, 0.5);
    upPane.translate(dx, 1.72, 8.9);
    p.push({ geo: upPane, color: WINDOW });
    // Lower pane
    const loPane = new THREE.BoxGeometry(0.40, 0.25, 0.45);
    loPane.translate(dx, 1.42, 9.0);
    p.push({ geo: loPane, color: WINDOW });
  }
  // Centre divider frame
  const cDiv = new THREE.BoxGeometry(0.04, 0.55, 0.55);
  cDiv.translate(0, 1.57, 8.95);
  p.push({ geo: cDiv, color: BODY });
  // Horizontal divider
  const hDiv = new THREE.BoxGeometry(0.84, 0.04, 0.5);
  hDiv.translate(0, 1.55, 8.95);
  p.push({ geo: hDiv, color: BODY });
  // Side cockpit windows (one per side)
  for (const dx of [-0.62, 0.62]) {
    const sideW = new THREE.BoxGeometry(0.06, 0.22, 0.35);
    sideW.translate(dx, 1.60, 8.6);
    p.push({ geo: sideW, color: WINDOW });
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 3. PASSENGER WINDOWS — individual oval panes both sides
  // ═══════════════════════════════════════════════════════════════════════

  const winCount = 28;
  const winStart = -5.5;
  const winEnd = 6.0;
  const winSpacing = (winEnd - winStart) / winCount;
  for (let i = 0; i < winCount; i++) {
    const wz = winStart + i * winSpacing + winSpacing / 2;
    for (const dx of [-1.13, 1.13]) {
      // Each window is a small cylinder (round shape) for higher fidelity
      const win = new THREE.CylinderGeometry(0.09, 0.09, 0.06, 8);
      win.rotateZ(Math.PI / 2);
      win.translate(dx, 1.55, wz);
      p.push({ geo: win, color: WINDOW });
      // Window frame ring (slightly larger)
      const frame = new THREE.CylinderGeometry(0.12, 0.12, 0.04, 8);
      frame.rotateZ(Math.PI / 2);
      frame.translate(dx, 1.55, wz);
      p.push({ geo: frame, color: BODY });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 4. DOORS — flat polys with frames (2 per side: forward + aft)
  // ═══════════════════════════════════════════════════════════════════════

  for (const dz of [5.0, -2.5]) {
    for (const dx of [-1.16, 1.16]) {
      const sign = dx > 0 ? 1 : -1;
      // Door panel
      const door = new THREE.BoxGeometry(0.05, 1.1, 0.55);
      door.translate(dx, 0.95, dz);
      p.push({ geo: door, color: DOOR });
      // Door frame (outline)
      const frameT = new THREE.BoxGeometry(0.05, 0.05, 0.58);
      frameT.translate(dx, 1.52, dz);
      p.push({ geo: frameT, color: BODY });
      const frameB = new THREE.BoxGeometry(0.05, 0.05, 0.58);
      frameB.translate(dx, 0.40, dz);
      p.push({ geo: frameB, color: BODY });
      // Door window (small rectangle at top)
      const dWin = new THREE.BoxGeometry(0.05, 0.25, 0.30);
      dWin.translate(dx, 1.35, dz);
      p.push({ geo: dWin, color: WINDOW });
      // Door handle
      const handle = new THREE.BoxGeometry(0.06, 0.04, 0.12);
      handle.translate(dx + sign * 0.01, 0.95, dz + 0.15);
      p.push({ geo: handle, color: BELLY });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 5. T-TAIL ASSEMBLY — tall vertical fin + high horizontal stabilisers
  // ═══════════════════════════════════════════════════════════════════════

  // Vertical fin (tapered: thinner at top)
  const finMain = new THREE.BoxGeometry(0.16, 3.6, 2.8);
  finMain.translate(0, 3.5, -9.2);
  p.push({ geo: finMain, color: TAIL_FIN });
  // Fin leading edge (rounded cylinder)
  const finLE = new THREE.CylinderGeometry(0.08, 0.08, 3.6, HS);
  finLE.translate(0, 3.5, -7.8);
  p.push({ geo: finLE, color: TAIL_FIN });
  // Fin trailing edge (thin taper)
  const finTE = new THREE.BoxGeometry(0.10, 2.8, 0.4);
  finTE.translate(0, 3.2, -10.8);
  p.push({ geo: finTE, color: TAIL_FIN });
  // Fin tip cap (small box)
  const finCap = new THREE.BoxGeometry(0.20, 0.15, 2.4);
  finCap.translate(0, 5.32, -9.2);
  p.push({ geo: finCap, color: TAIL_FIN });
  // Fin root fairing (blends into fuselage)
  const finRoot = new THREE.BoxGeometry(0.40, 0.6, 3.2);
  finRoot.translate(0, 1.9, -9.0);
  p.push({ geo: finRoot, color: TAIL_FIN });

  // Horizontal stabilisers (high-mounted on fin tip)
  for (const side of [-1, 1]) {
    // Main stabiliser spar
    const stab = new THREE.BoxGeometry(3.5, 0.12, 1.5);
    stab.translate(side * 1.75, 5.25, -9.8);
    p.push({ geo: stab, color: TAIL_FIN });
    // Stabiliser leading edge
    const stabLE = new THREE.CylinderGeometry(0.06, 0.06, 3.5, HS);
    stabLE.rotateZ(Math.PI / 2);
    stabLE.translate(side * 1.75, 5.28, -9.05);
    p.push({ geo: stabLE, color: TAIL_FIN });
    // Stabiliser tip (tapered end)
    const stabTip = new THREE.BoxGeometry(0.5, 0.10, 1.0);
    stabTip.translate(side * 3.7, 5.25, -10.0);
    p.push({ geo: stabTip, color: TAIL_FIN });
    // Elevator hinge line
    const elev = new THREE.BoxGeometry(3.0, 0.08, 0.15);
    elev.translate(side * 1.75, 5.22, -10.5);
    p.push({ geo: elev, color: TAIL_FIN });
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 6. WINGS — swept, tapered, with control surfaces and details
  // ═══════════════════════════════════════════════════════════════════════

  for (const side of [-1, 1]) {
    // ── Wing root fairing (blends into fuselage) ──
    const rootFair = new THREE.BoxGeometry(1.8, 0.35, 3.5);
    rootFair.translate(side * 0.9, 0.45, -0.5);
    p.push({ geo: rootFair, color: BODY });

    // ── Inner wing panel (root to engine) ──
    const innerWing = new THREE.BoxGeometry(3.0, 0.26, 3.2);
    innerWing.translate(side * 2.6, 0.45, -0.8);
    p.push({ geo: innerWing, color: WING });

    // ── Outer wing panel (engine to tip, swept back) ──
    const outerWing = new THREE.BoxGeometry(4.5, 0.20, 2.2);
    outerWing.translate(side * 6.35, 0.55, -1.4);
    p.push({ geo: outerWing, color: WING });

    // ── Wing tip (tapered) ──
    const tipBlock = new THREE.BoxGeometry(1.0, 0.14, 1.2);
    tipBlock.translate(side * 9.1, 0.6, -1.8);
    p.push({ geo: tipBlock, color: WING });

    // ── Winglet (upturned) ──
    const winglet = new THREE.BoxGeometry(0.10, 0.9, 0.5);
    winglet.translate(side * 9.6, 1.0, -1.9);
    p.push({ geo: winglet, color: TAIL_FIN });

    // ── Leading edge (rounded cylinder along span) ──
    // Inner
    const leInner = new THREE.CylinderGeometry(0.12, 0.12, 3.0, HS);
    leInner.rotateZ(Math.PI / 2);
    leInner.translate(side * 2.6, 0.50, 0.8);
    p.push({ geo: leInner, color: WING });
    // Outer
    const leOuter = new THREE.CylinderGeometry(0.10, 0.10, 4.5, HS);
    leOuter.rotateZ(Math.PI / 2);
    leOuter.translate(side * 6.35, 0.60, -0.3);
    p.push({ geo: leOuter, color: WING });

    // ── Trailing edge flaps (3 segments) ──
    for (let f = 0; f < 3; f++) {
      const flapX = side * (2.0 + f * 2.2);
      const flapZ = -2.0 - f * 0.3;
      const flapW = 1.8;
      const flap = new THREE.BoxGeometry(flapW, 0.08, 0.6);
      flap.translate(flapX, 0.35, flapZ);
      p.push({ geo: flap, color: WING });
      // Flap track fairing
      const fairing = new THREE.BoxGeometry(0.4, 0.18, 0.25);
      fairing.translate(flapX, 0.28, flapZ + 0.2);
      p.push({ geo: fairing, color: BELLY });
    }

    // ── Aileron (outboard) ──
    const aileron = new THREE.BoxGeometry(2.0, 0.06, 0.4);
    aileron.translate(side * 7.5, 0.50, -2.5);
    p.push({ geo: aileron, color: WING });

    // ── Slats (leading edge, 2 segments) ──
    for (let s = 0; s < 2; s++) {
      const slatX = side * (2.5 + s * 3.0);
      const slat = new THREE.BoxGeometry(2.5, 0.08, 0.3);
      slat.translate(slatX, 0.55, 1.1 - s * 0.2);
      p.push({ geo: slat, color: WING });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 7. ENGINE NACELLES — hex-cylinder pods on underwing struts
  // ═══════════════════════════════════════════════════════════════════════

  for (const side of [-1, 1]) {
    const ex = side * 3.8;  // engine X position (under inner wing)

    // ── Pylon / strut (connects wing to nacelle) ──
    const pylon = new THREE.BoxGeometry(0.16, 0.8, 1.8);
    pylon.translate(ex, 0.05, -0.5);
    p.push({ geo: pylon, color: WING });
    // Pylon fairing (streamlined)
    const pylonFair = new THREE.BoxGeometry(0.30, 0.3, 2.0);
    pylonFair.translate(ex, 0.30, -0.5);
    p.push({ geo: pylonFair, color: WING });

    // ── Nacelle body (hex-cylinder as per reference) ──
    const nacBody = new THREE.CylinderGeometry(0.55, 0.52, 3.8, 6, 1);
    nacBody.rotateX(Math.PI / 2);
    nacBody.translate(ex, -0.35, -0.4);
    p.push({ geo: nacBody, color: ENGINE });

    // ── Intake cowl (slightly wider ring) ──
    const intake = new THREE.CylinderGeometry(0.62, 0.60, 0.20, HS, 1);
    intake.rotateX(Math.PI / 2);
    intake.translate(ex, -0.35, 1.5);
    p.push({ geo: intake, color: BELLY });

    // ── Fan face (visible disc) ──
    const fan = new THREE.CylinderGeometry(0.55, 0.55, 0.06, HS, 1);
    fan.rotateX(Math.PI / 2);
    fan.translate(ex, -0.35, 1.55);
    p.push({ geo: fan, color: NOSE_TIP });

    // ── Fan spinner (cone in centre) ──
    const spinner = new THREE.CylinderGeometry(0.06, 0.22, 0.35, HS, 1);
    spinner.rotateX(Math.PI / 2);
    spinner.translate(ex, -0.35, 1.65);
    p.push({ geo: spinner, color: NOSE_TIP });

    // ── Fan blades (8 flat planes radially) ──
    for (let b = 0; b < 8; b++) {
      const angle = (b / 8) * Math.PI * 2;
      const blade = new THREE.BoxGeometry(0.04, 0.38, 0.08);
      blade.translate(0, 0.22, 0);
      blade.rotateZ(angle);
      blade.translate(ex, -0.35, 1.56);
      p.push({ geo: blade, color: BELLY });
    }

    // ── Exhaust nozzle (tapered) ──
    const exhOuter = new THREE.CylinderGeometry(0.35, 0.52, 0.6, HS, 1);
    exhOuter.rotateX(Math.PI / 2);
    exhOuter.translate(ex, -0.35, -2.6);
    p.push({ geo: exhOuter, color: EXHAUST });
    // Inner exhaust cone
    const exhInner = new THREE.CylinderGeometry(0.20, 0.30, 0.5, HS, 1);
    exhInner.rotateX(Math.PI / 2);
    exhInner.translate(ex, -0.35, -2.55);
    p.push({ geo: exhInner, color: NOSE_TIP });

    // ── Thrust reverser cowl lines (rings on nacelle) ──
    for (const rz of [-0.8, -1.4]) {
      const ring = new THREE.CylinderGeometry(0.57, 0.57, 0.06, HS, 1);
      ring.rotateX(Math.PI / 2);
      ring.translate(ex, -0.35, rz);
      p.push({ geo: ring, color: BELLY });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 8. LANDING GEAR — nose gear + main gear bogies with detail
  // ═══════════════════════════════════════════════════════════════════════

  // ── Nose gear ──
  // Main strut
  const nStrut = new THREE.CylinderGeometry(0.06, 0.06, 1.3, HS);
  nStrut.translate(0, -0.50, 7.0);
  p.push({ geo: nStrut, color: GEAR });
  // Oleo (telescoping section, slightly thicker)
  const nOleo = new THREE.CylinderGeometry(0.08, 0.06, 0.5, HS);
  nOleo.translate(0, -0.90, 7.0);
  p.push({ geo: nOleo, color: GEAR });
  // Axle
  const nAxle = new THREE.CylinderGeometry(0.03, 0.03, 0.5, 8);
  nAxle.rotateZ(Math.PI / 2);
  nAxle.translate(0, -1.15, 7.0);
  p.push({ geo: nAxle, color: GEAR });
  // Wheels (2, side by side)
  for (const dx of [-0.15, 0.15]) {
    const nw = new THREE.CylinderGeometry(0.24, 0.24, 0.14, HS);
    nw.rotateZ(Math.PI / 2);
    nw.translate(dx, -1.15, 7.0);
    p.push({ geo: nw, color: GEAR });
    // Tire tread (slightly larger ring)
    const tread = new THREE.TorusGeometry(0.24, 0.03, 8, HS);
    tread.rotateY(Math.PI / 2);
    tread.translate(dx, -1.15, 7.0);
    p.push({ geo: tread, color: 0x111111 });
  }
  // Nose gear door (2 panels)
  for (const dx of [-0.25, 0.25]) {
    const ngd = new THREE.BoxGeometry(0.35, 0.04, 0.9);
    ngd.translate(dx, 0.02, 7.0);
    p.push({ geo: ngd, color: BELLY });
  }
  // Steering linkage
  const steerLink = new THREE.BoxGeometry(0.12, 0.3, 0.04);
  steerLink.translate(0.08, -0.30, 7.05);
  p.push({ geo: steerLink, color: GEAR });

  // ── Main gear (2 bogies, one per side) ──
  for (const side of [-1, 1]) {
    const gx = side * 1.6;

    // Main strut
    const mStrut = new THREE.CylinderGeometry(0.07, 0.07, 1.5, HS);
    mStrut.translate(gx, -0.55, -1.0);
    p.push({ geo: mStrut, color: GEAR });
    // Oleo
    const mOleo = new THREE.CylinderGeometry(0.09, 0.07, 0.6, HS);
    mOleo.translate(gx, -1.05, -1.0);
    p.push({ geo: mOleo, color: GEAR });

    // Bogie beam
    const beam = new THREE.BoxGeometry(0.14, 0.10, 1.0);
    beam.translate(gx, -1.30, -1.0);
    p.push({ geo: beam, color: GEAR });

    // Side brace (angled strut to fuselage)
    const brace = new THREE.CylinderGeometry(0.03, 0.03, 1.2, 8);
    brace.rotateZ(side * 0.3);
    brace.translate(gx * 0.7, -0.2, -1.0);
    p.push({ geo: brace, color: GEAR });

    // Wheels (4: 2 axles × 2 per axle)
    for (const dz of [-0.30, 0.30]) {
      // Axle
      const axle = new THREE.CylinderGeometry(0.025, 0.025, 0.55, 8);
      axle.rotateZ(Math.PI / 2);
      axle.translate(gx, -1.35, -1.0 + dz);
      p.push({ geo: axle, color: GEAR });
      for (const dxW of [-0.18, 0.18]) {
        const mw = new THREE.CylinderGeometry(0.30, 0.30, 0.16, HS);
        mw.rotateZ(Math.PI / 2);
        mw.translate(gx + dxW, -1.35, -1.0 + dz);
        p.push({ geo: mw, color: GEAR });
        // Tire tread
        const tread = new THREE.TorusGeometry(0.30, 0.035, 8, HS);
        tread.rotateY(Math.PI / 2);
        tread.translate(gx + dxW, -1.35, -1.0 + dz);
        p.push({ geo: tread, color: 0x111111 });
      }
    }

    // Main gear doors (2 panels)
    const mgd1 = new THREE.BoxGeometry(0.7, 0.04, 1.2);
    mgd1.translate(gx, 0.02, -1.0);
    p.push({ geo: mgd1, color: BELLY });
    const mgd2 = new THREE.BoxGeometry(0.4, 0.04, 0.8);
    mgd2.translate(gx + side * 0.4, 0.02, -1.0);
    p.push({ geo: mgd2, color: BELLY });
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 9. SURFACE DETAILS — panel lines, antennas, lights
  // ═══════════════════════════════════════════════════════════════════════

  // Wing root panel lines (horizontal seams on fuselage sides)
  for (const dx of [-1.14, 1.14]) {
    for (const z of [2.0, -2.0, -5.0]) {
      const line = new THREE.BoxGeometry(0.06, 0.02, 0.8);
      line.translate(dx, 0.45, z);
      p.push({ geo: line, color: BELLY });
    }
  }

  // Belly antennas (2 small fins)
  for (const az of [3.0, -3.5]) {
    const ant = new THREE.BoxGeometry(0.04, 0.15, 0.30);
    ant.translate(0, -0.05, az);
    p.push({ geo: ant, color: BELLY });
  }

  // Dorsal antenna (top of fuselage)
  const dorsalAnt = new THREE.BoxGeometry(0.04, 0.18, 0.25);
  dorsalAnt.translate(0, 2.35, 1.0);
  p.push({ geo: dorsalAnt, color: BELLY });

  // Navigation lights (wingtip)
  for (const side of [-1, 1]) {
    const navLight = new THREE.SphereGeometry(0.06, 8, 8);
    navLight.translate(side * 9.65, 0.6, -1.8);
    p.push({ geo: navLight, color: side > 0 ? 0x22c55e : 0xef4444 });
  }

  // Tail navigation light (white, aft)
  const tailNav = new THREE.SphereGeometry(0.05, 8, 8);
  tailNav.translate(0, 1.15, -11.95);
  p.push({ geo: tailNav, color: 0xffffff });

  // Anti-collision beacon (top of fin)
  const beacon = new THREE.SphereGeometry(0.06, 8, 8);
  beacon.translate(0, 5.45, -9.2);
  p.push({ geo: beacon, color: 0xef4444 });

  // Landing lights (on belly near nose gear)
  for (const dx of [-0.35, 0.35]) {
    const lLight = new THREE.CylinderGeometry(0.08, 0.08, 0.04, 8);
    lLight.translate(dx, -0.02, 6.0);
    p.push({ geo: lLight, color: 0xfef3c7 });
  }

  return _mergeGeomsColored(p);
}

/** Helper: rounded box approximation — a box body + 4 lengthwise edge
 *  cylinders so the silhouette reads as rounded, especially at night. */
function _roundedBody(w, h, len, segs = 6) {
  const parts = [];
  // Core box slightly inset
  const rr = Math.min(w, h) * 0.22;  // corner radius
  const bw = w - rr * 2;
  const bh = h - rr * 2;
  parts.push(new THREE.BoxGeometry(w, bh, len));       // tall centre slab
  parts.push(new THREE.BoxGeometry(bw, h, len));       // wide centre slab
  // 4 edge cylinders along Z
  for (const sx of [-1, 1]) {
    for (const sy of [-1, 1]) {
      const cyl = new THREE.CylinderGeometry(rr, rr, len, segs);
      cyl.rotateX(Math.PI / 2);
      cyl.translate(sx * bw / 2, sy * bh / 2, 0);
      parts.push(cyl);
    }
  }
  const merged = _mergeGeoms(parts);
  return merged;
}

// ── High-poly train builder ─────────────────────────────────────────────────
const S = 20;  // cylinder segments for smooth curves

/**
 * Build the cab (front/rear face) of a train car. Each variant calls this
 * with different cab geometry to get angular vs. sloped vs. flat profiles.
 * `dir` is +1 for front (facing +Z) or -1 for rear (facing -Z).
 */
function _buildCab(p, palette, cabZ, dir, style) {
  const { BODY, WINDOW, CAB_FACE, CHEVRON, HEADLIGHT, LOWER } = palette;
  const d = dir;  // +1 front, -1 rear

  if (style === 'angular') {
    // ── Skånetrafiken Påinter: high angular cab ──
    // Flat front face (full width, slightly raked at top)
    const face = new THREE.BoxGeometry(2.8, 3.2, 0.15);
    face.translate(0, 1.8, cabZ + d * 0.08);
    p.push({ geo: face, color: CAB_FACE });
    // Windshield — large, raked (two-piece split)
    const wind = new THREE.BoxGeometry(2.2, 1.2, 0.12);
    wind.translate(0, 2.8, cabZ + d * 0.12);
    p.push({ geo: wind, color: WINDOW });
    // Lower windshield / destination display
    const dest = new THREE.BoxGeometry(1.6, 0.3, 0.1);
    dest.translate(0, 3.45, cabZ + d * 0.12);
    p.push({ geo: dest, color: 0x111827 });
    // Safety chevron (yellow V-shape at bottom)
    const chevL = new THREE.BoxGeometry(1.2, 0.5, 0.1);
    chevL.translate(-0.5, 0.6, cabZ + d * 0.12);
    p.push({ geo: chevL, color: CHEVRON });
    const chevR = new THREE.BoxGeometry(1.2, 0.5, 0.1);
    chevR.translate(0.5, 0.6, cabZ + d * 0.12);
    p.push({ geo: chevR, color: CHEVRON });
    // Headlights
    for (const dx of [-1.0, 1.0]) {
      const hl = new THREE.CylinderGeometry(0.18, 0.18, 0.1, S);
      hl.rotateX(Math.PI / 2);
      hl.translate(dx, 1.2, cabZ + d * 0.15);
      p.push({ geo: hl, color: HEADLIGHT });
    }

  } else if (style === 'sloped') {
    // ── SJ Train: aerodynamic sloped front (NOT a cone) ──
    // Raked front face — angled slab (taller at top, shorter at bottom)
    // Upper face (wide, at body top)
    const upperFace = new THREE.BoxGeometry(2.6, 1.6, 0.15);
    upperFace.translate(0, 2.6, cabZ + d * 0.08);
    p.push({ geo: upperFace, color: CAB_FACE });
    // Lower face (narrower, stepped forward to create rake angle)
    const lowerFace = new THREE.BoxGeometry(2.4, 1.2, 0.15);
    lowerFace.translate(0, 1.0, cabZ + d * 0.8);
    p.push({ geo: lowerFace, color: CAB_FACE });
    // Sloped panel connecting upper to lower (angled surface)
    const slope = new THREE.BoxGeometry(2.5, 0.3, 1.0);
    slope.translate(0, 1.7, cabZ + d * 0.5);
    p.push({ geo: slope, color: CAB_FACE });
    // Windshield — wide panoramic
    const wind = new THREE.BoxGeometry(2.3, 1.0, 0.1);
    wind.translate(0, 2.8, cabZ + d * 0.12);
    p.push({ geo: wind, color: WINDOW });
    // Headlights — integrated into lower face
    for (const dx of [-0.9, 0.9]) {
      const hl = new THREE.CylinderGeometry(0.15, 0.15, 0.12, S);
      hl.rotateX(Math.PI / 2);
      hl.translate(dx, 0.8, cabZ + d * 0.9);
      p.push({ geo: hl, color: HEADLIGHT });
    }

  } else {
    // ── SL Suburban: flat angular front with blue chevron ──
    // Full flat face
    const face = new THREE.BoxGeometry(2.8, 3.2, 0.15);
    face.translate(0, 1.8, cabZ + d * 0.08);
    p.push({ geo: face, color: CAB_FACE });
    // Large windshield
    const wind = new THREE.BoxGeometry(2.4, 1.3, 0.12);
    wind.translate(0, 2.7, cabZ + d * 0.12);
    p.push({ geo: wind, color: WINDOW });
    // Blue chevron / stripe on lower face
    const chevron = new THREE.BoxGeometry(2.4, 0.6, 0.1);
    chevron.translate(0, 1.0, cabZ + d * 0.12);
    p.push({ geo: chevron, color: CHEVRON });
    // Destination display
    const dest = new THREE.BoxGeometry(1.4, 0.25, 0.1);
    dest.translate(0, 3.4, cabZ + d * 0.12);
    p.push({ geo: dest, color: 0x111827 });
    // Headlights
    for (const dx of [-1.05, 1.05]) {
      const hl = new THREE.CylinderGeometry(0.15, 0.15, 0.1, S);
      hl.rotateX(Math.PI / 2);
      hl.translate(dx, 1.5, cabZ + d * 0.15);
      p.push({ geo: hl, color: HEADLIGHT });
    }
  }

  // Coupler / bumper beam at the very front/rear
  const coupler = new THREE.BoxGeometry(1.0, 0.25, 0.2);
  coupler.translate(0, 0.25, cabZ + d * 0.2);
  p.push({ geo: coupler, color: LOWER });
}

/**
 * Build a complete train consist. `cabStyle` controls the front/rear
 * profile shape ('angular', 'sloped', 'flat').
 */
function _trainCommon(palette, numCars, cabStyle) {
  const { BODY, STRIPE, WINDOW, BOGIE, ROOF, LOWER } = palette;
  const p = [];
  const carLen = 9.0;
  const gap = 1.0;
  const totalLen = numCars * carLen + (numCars - 1) * gap;
  const startZ = totalLen / 2;

  for (let c = 0; c < numCars; c++) {
    const cz = startZ - c * (carLen + gap) - carLen / 2;
    const isLead = (c === 0);
    const isTail = (c === numCars - 1);

    // ── Car body — main rectangular slab ──
    const bodyMain = new THREE.BoxGeometry(2.8, 2.0, carLen);
    bodyMain.translate(0, 1.6, cz);
    p.push({ geo: bodyMain, color: BODY });

    // Lower body / skirt (slightly wider)
    const skirt = new THREE.BoxGeometry(2.85, 0.4, carLen);
    skirt.translate(0, 0.4, cz);
    p.push({ geo: skirt, color: LOWER });

    // Upper body (above window line to roof)
    const upperBody = new THREE.BoxGeometry(2.75, 0.6, carLen);
    upperBody.translate(0, 2.9, cz);
    p.push({ geo: upperBody, color: BODY });

    // ── Window band (both sides) ──
    for (const dx of [-1.42, 1.42]) {
      const winBand = new THREE.BoxGeometry(0.06, 0.9, carLen - 1.5);
      winBand.translate(dx, 2.15, cz);
      p.push({ geo: winBand, color: WINDOW });
    }

    // Individual window panes (panels between pillars)
    const windowCount = Math.floor(carLen / 1.2);
    for (let w = 0; w < windowCount; w++) {
      const wz = cz - carLen / 2 + 0.8 + w * (carLen - 1.2) / windowCount;
      for (const dx of [-1.43, 1.43]) {
        const pane = new THREE.BoxGeometry(0.05, 0.75, 0.7);
        pane.translate(dx, 2.15, wz);
        p.push({ geo: pane, color: WINDOW });
      }
    }

    // ── Livery stripe (horizontal colour band) ──
    for (const dx of [-1.43, 1.43]) {
      const stripe = new THREE.BoxGeometry(0.05, 0.2, carLen - 0.5);
      stripe.translate(dx, 1.2, cz);
      p.push({ geo: stripe, color: STRIPE });
    }

    // ── Doors (2 per side per car) ──
    for (const dz of [carLen * 0.25, -carLen * 0.25]) {
      for (const dx of [-1.44, 1.44]) {
        // Door frame
        const frame = new THREE.BoxGeometry(0.05, 2.2, 1.0);
        frame.translate(dx, 1.7, cz + dz);
        p.push({ geo: frame, color: LOWER });
        // Door window
        const dWin = new THREE.BoxGeometry(0.05, 0.7, 0.6);
        dWin.translate(dx, 2.3, cz + dz);
        p.push({ geo: dWin, color: WINDOW });
      }
    }

    // ── Roof — flat top ──
    const roof = new THREE.BoxGeometry(2.8, 0.18, carLen - 0.3);
    roof.translate(0, 3.25, cz);
    p.push({ geo: roof, color: ROOF });

    // Roof equipment (AC units — rectangular boxes)
    for (const eqZ of [-1.5, 1.5]) {
      const ac = new THREE.BoxGeometry(1.6, 0.2, 1.8);
      ac.translate(0, 3.55, cz + eqZ);
      p.push({ geo: ac, color: ROOF });
    }

    // ── Bogies (2 per car, near each end) ──
    for (const endSign of [-1, 1]) {
      const bz = cz + endSign * (carLen / 2 - 1.3);
      // Bogie frame
      const frame = new THREE.BoxGeometry(2.4, 0.35, 1.8);
      frame.translate(0, 0.0, bz);
      p.push({ geo: frame, color: BOGIE });
      // Side frames
      for (const dx of [-1.1, 1.1]) {
        const sf = new THREE.BoxGeometry(0.15, 0.5, 1.6);
        sf.translate(dx, 0.0, bz);
        p.push({ geo: sf, color: BOGIE });
      }
      // Wheels (4 per bogie — 2 axles × 2 sides)
      for (const dx of [-1.15, 1.15]) {
        for (const wdz of [-0.4, 0.4]) {
          const wh = new THREE.CylinderGeometry(0.4, 0.4, 0.3, S);
          wh.rotateZ(Math.PI / 2);
          wh.translate(dx, -0.1, bz + wdz);
          p.push({ geo: wh, color: BOGIE });
        }
      }
    }

    // ── Cab faces on lead and tail cars ──
    if (isLead) {
      _buildCab(p, palette, startZ, +1, cabStyle);
    }
    if (isTail) {
      _buildCab(p, palette, -startZ, -1, cabStyle);
    }
  }

  // ── Inter-car gangway connections ──
  for (let j = 0; j < numCars - 1; j++) {
    const jz = startZ - (j + 1) * carLen - j * gap - gap / 2;
    // Gangway body
    const gang = new THREE.BoxGeometry(2.0, 2.2, gap - 0.15);
    gang.translate(0, 1.7, jz);
    p.push({ geo: gang, color: LOWER });
    // Gangway bellows ribs
    for (const ribOff of [-0.2, 0, 0.2]) {
      const rib = new THREE.BoxGeometry(2.3, 2.4, 0.06);
      rib.translate(0, 1.7, jz + ribOff);
      p.push({ geo: rib, color: BOGIE });
    }
  }

  return _mergeGeomsColored(p);
}

// ── Train A: Skånetrafiken Påinter (blue, angular cab, 3 cars) ──
function _buildTrainGeometry() {
  return _trainCommon(_trainPalettes[0], 3, 'angular');
}

function _buildTrainBGeometry() {
  return _trainCommon(_trainPalettes[1], 4, 'sloped');
}

function _buildTrainCGeometry() {
  return _trainCommon(_trainPalettes[2], 3, 'flat');
}

// ── Öresundståg (Bombardier Contessa X31K) ─────────────────────────────────
// Dedicated high-poly model (~10k faces) for cities in the Öresund corridor.
// Distinctive features: dark rubber "nose" cab surround, silver body, red
// doors, orange accent stripe.  Reference: Skånetrafiken/DSB livery.
function _buildOresundstagGeometry() {
  const BODY       = 0xc8cdd3;   // silver-grey body
  const LOWER      = 0x6b7280;   // dark grey skirt
  const WINDOW     = 0x111827;   // very dark glass
  const ROOF       = 0x9ca3af;   // grey roof
  const BOGIE      = 0x1c1917;   // black
  const CAB_RUBBER = 0x2d2d2d;   // dark charcoal rubber surround
  const DOOR       = 0xdc2626;   // bright red doors
  const STRIPE_ORG = 0xe88c30;   // orange accent stripe
  const STRIPE_RED = 0xcc3333;   // red "ØRESUNDSTÅG" branding band
  const HEADLIGHT  = 0xfef3c7;   // warm white
  const DEST       = 0x111827;   // destination display
  const PANTO      = 0x44403c;   // pantograph
  const HS = 24;  // high segments for smooth rubber surround

  const p = [];
  const numCars = 3;
  const carLen = 9.0;
  const gap = 1.0;
  const totalLen = numCars * carLen + (numCars - 1) * gap;
  const startZ = totalLen / 2;

  for (let c = 0; c < numCars; c++) {
    const cz = startZ - c * (carLen + gap) - carLen / 2;
    const isLead = (c === 0);
    const isTail = (c === numCars - 1);

    // ── Car body (rounded) ──
    const bodyMain = _roundedBody(2.9, 2.0, carLen, HS);
    bodyMain.translate(0, 1.6, cz);
    p.push({ geo: bodyMain, color: BODY });

    // Lower skirt
    const skirt = new THREE.BoxGeometry(2.95, 0.4, carLen);
    skirt.translate(0, 0.4, cz);
    p.push({ geo: skirt, color: LOWER });

    // Upper body above window line
    const upperBody = new THREE.BoxGeometry(2.85, 0.55, carLen);
    upperBody.translate(0, 2.9, cz);
    p.push({ geo: upperBody, color: BODY });

    // ── Window band (both sides) ──
    for (const dx of [-1.47, 1.47]) {
      const winBand = new THREE.BoxGeometry(0.06, 0.85, carLen - 1.8);
      winBand.translate(dx, 2.15, cz);
      p.push({ geo: winBand, color: WINDOW });
    }

    // Individual window panes with pillars between
    const windowCount = 8;
    const winSpacing = (carLen - 2.4) / windowCount;
    for (let w = 0; w < windowCount; w++) {
      const wz = cz - carLen / 2 + 1.4 + w * winSpacing;
      for (const dx of [-1.48, 1.48]) {
        // Window pane
        const pane = new THREE.BoxGeometry(0.05, 0.72, winSpacing * 0.7);
        pane.translate(dx, 2.15, wz);
        p.push({ geo: pane, color: WINDOW });
        // Pillar between windows
        const pillar = new THREE.BoxGeometry(0.06, 0.85, 0.08);
        pillar.translate(dx, 2.15, wz + winSpacing * 0.42);
        p.push({ geo: pillar, color: BODY });
      }
    }

    // ── Orange accent stripe at window level ──
    for (const dx of [-1.48, 1.48]) {
      const stripe = new THREE.BoxGeometry(0.05, 0.10, carLen - 0.8);
      stripe.translate(dx, 2.62, cz);
      p.push({ geo: stripe, color: STRIPE_ORG });
    }

    // ── Red branding strip (lower body) ──
    for (const dx of [-1.48, 1.48]) {
      const redStripe = new THREE.BoxGeometry(0.05, 0.14, carLen * 0.35);
      redStripe.translate(dx, 1.35, cz - carLen * 0.05);
      p.push({ geo: redStripe, color: STRIPE_RED });
    }

    // ── RED DOORS (2 per side per car) — the distinctive feature ──
    for (const dz of [carLen * 0.28, -carLen * 0.28]) {
      for (const dx of [-1.49, 1.49]) {
        // Door panel (bright red)
        const door = new THREE.BoxGeometry(0.06, 2.1, 1.1);
        door.translate(dx, 1.65, cz + dz);
        p.push({ geo: door, color: DOOR });
        // Door window (upper half)
        const dWin = new THREE.BoxGeometry(0.06, 0.65, 0.7);
        dWin.translate(dx, 2.35, cz + dz);
        p.push({ geo: dWin, color: WINDOW });
        // Door frame outline
        for (const edgeDz of [-0.58, 0.58]) {
          const frame = new THREE.BoxGeometry(0.07, 2.1, 0.04);
          frame.translate(dx, 1.65, cz + dz + edgeDz);
          p.push({ geo: frame, color: LOWER });
        }
      }
    }

    // ── Roof (slightly arched via box) ──
    const roof = new THREE.BoxGeometry(2.8, 0.18, carLen - 0.3);
    roof.translate(0, 3.22, cz);
    p.push({ geo: roof, color: ROOF });

    // Roof edge trim (darker band)
    for (const dx of [-1.42, 1.42]) {
      const edge = new THREE.BoxGeometry(0.06, 0.18, carLen - 0.3);
      edge.translate(dx, 3.22, cz);
      p.push({ geo: edge, color: LOWER });
    }

    // AC units (rectangular boxes, 2 per car)
    for (const eqZ of [-1.5, 1.5]) {
      const ac = new THREE.BoxGeometry(1.5, 0.22, 1.8);
      ac.translate(0, 3.48, cz + eqZ);
      p.push({ geo: ac, color: ROOF });
      // AC grille lines
      for (let g = -2; g <= 2; g++) {
        const grille = new THREE.BoxGeometry(1.3, 0.02, 0.04);
        grille.translate(0, 3.60, cz + eqZ + g * 0.35);
        p.push({ geo: grille, color: LOWER });
      }
    }

    // Pantograph on middle car
    if (c === 1) {
      // Base insulator
      const base = new THREE.BoxGeometry(0.8, 0.15, 0.6);
      base.translate(0, 3.55, cz);
      p.push({ geo: base, color: PANTO });
      // Lower arm
      const armL = new THREE.BoxGeometry(0.08, 1.0, 0.08);
      armL.translate(-0.15, 4.05, cz);
      p.push({ geo: armL, color: PANTO });
      const armR = new THREE.BoxGeometry(0.08, 1.0, 0.08);
      armR.translate(0.15, 4.05, cz);
      p.push({ geo: armR, color: PANTO });
      // Upper arm (angled)
      const upper = new THREE.BoxGeometry(0.06, 0.7, 0.06);
      upper.translate(0, 4.85, cz);
      p.push({ geo: upper, color: PANTO });
      // Contact strip
      const contact = new THREE.BoxGeometry(1.6, 0.04, 0.06);
      contact.translate(0, 5.20, cz);
      p.push({ geo: contact, color: PANTO });
      // Horn tips
      for (const dx of [-0.85, 0.85]) {
        const horn = new THREE.BoxGeometry(0.04, 0.12, 0.04);
        horn.translate(dx, 5.18, cz);
        p.push({ geo: horn, color: PANTO });
      }
    }

    // ── Bogies (2 per car) ──
    for (const endSign of [-1, 1]) {
      const bz = cz + endSign * (carLen / 2 - 1.3);
      // Bogie frame
      const bFrame = new THREE.BoxGeometry(2.5, 0.35, 1.8);
      bFrame.translate(0, 0.0, bz);
      p.push({ geo: bFrame, color: BOGIE });
      // Side frames
      for (const dx of [-1.15, 1.15]) {
        const sf = new THREE.BoxGeometry(0.15, 0.5, 1.6);
        sf.translate(dx, 0.0, bz);
        p.push({ geo: sf, color: BOGIE });
      }
      // Wheels (4 per bogie)
      for (const dx of [-1.2, 1.2]) {
        for (const wdz of [-0.4, 0.4]) {
          const wh = new THREE.CylinderGeometry(0.42, 0.42, 0.3, HS);
          wh.rotateZ(Math.PI / 2);
          wh.translate(dx, -0.1, bz + wdz);
          p.push({ geo: wh, color: BOGIE });
        }
      }
      // Brake discs (visible between wheels)
      for (const wdz of [-0.4, 0.4]) {
        const disc = new THREE.CylinderGeometry(0.3, 0.3, 0.06, HS);
        disc.rotateZ(Math.PI / 2);
        disc.translate(0, -0.1, bz + wdz);
        p.push({ geo: disc, color: LOWER });
      }
    }

    // ── Cab faces on lead and tail cars ──
    // The Öresundståg's signature: thick dark rubber surround with
    // rounded corners, large inset windshield.
    if (isLead || isTail) {
      const d = isLead ? +1 : -1;
      const cabZ = isLead ? startZ : -startZ;

      // Rubber surround — thick frame that protrudes forward
      // Main rubber face block (full width/height of the end)
      const rubberFace = _roundedBody(3.0, 3.2, 0.5, HS);
      rubberFace.translate(0, 1.75, cabZ + d * 0.35);
      p.push({ geo: rubberFace, color: CAB_RUBBER });

      // Deeper rubber extension at centre (the "nose" bulge)
      const noseBulge = _roundedBody(2.6, 2.6, 0.2, HS);
      noseBulge.translate(0, 1.85, cabZ + d * 0.55);
      p.push({ geo: noseBulge, color: CAB_RUBBER });

      // Windshield — large, inset into the rubber surround
      // Main windshield pane
      const windMain = new THREE.BoxGeometry(2.0, 1.1, 0.08);
      windMain.translate(0, 2.55, cabZ + d * 0.45);
      p.push({ geo: windMain, color: WINDOW });

      // Windshield divider (centre pillar)
      const divider = new THREE.BoxGeometry(0.06, 1.1, 0.10);
      divider.translate(0, 2.55, cabZ + d * 0.46);
      p.push({ geo: divider, color: CAB_RUBBER });

      // Side windows (wrap-around, smaller panes flanking main)
      for (const dx of [-1.15, 1.15]) {
        const sideWin = new THREE.BoxGeometry(0.35, 0.85, 0.08);
        sideWin.translate(dx, 2.55, cabZ + d * 0.42);
        p.push({ geo: sideWin, color: WINDOW });
      }

      // Number display panel (upper left)
      const numPanel = new THREE.BoxGeometry(0.4, 0.22, 0.06);
      numPanel.translate(-0.7, 3.15, cabZ + d * 0.50);
      p.push({ geo: numPanel, color: DEST });

      // Destination display (top centre)
      const dest = new THREE.BoxGeometry(1.2, 0.2, 0.06);
      dest.translate(0, 3.2, cabZ + d * 0.50);
      p.push({ geo: dest, color: DEST });

      // Lower face panel (below windshield, body-coloured)
      const lowerPanel = new THREE.BoxGeometry(2.2, 0.8, 0.12);
      lowerPanel.translate(0, 1.2, cabZ + d * 0.40);
      p.push({ geo: lowerPanel, color: BODY });

      // Headlights (rectangular, recessed into rubber)
      for (const dx of [-0.85, 0.85]) {
        // Headlight housing
        const hlHousing = new THREE.BoxGeometry(0.4, 0.25, 0.12);
        hlHousing.translate(dx, 0.85, cabZ + d * 0.50);
        p.push({ geo: hlHousing, color: CAB_RUBBER });
        // Headlight lens
        const hlLens = new THREE.CylinderGeometry(0.1, 0.1, 0.08, HS);
        hlLens.rotateX(Math.PI / 2);
        hlLens.translate(dx, 0.85, cabZ + d * 0.56);
        p.push({ geo: hlLens, color: HEADLIGHT });
      }

      // Tail lights (red, at lower corners)
      for (const dx of [-1.1, 1.1]) {
        const tl = new THREE.BoxGeometry(0.2, 0.15, 0.06);
        tl.translate(dx, 0.65, cabZ + d * 0.52);
        p.push({ geo: tl, color: 0xdc2626 });
      }

      // Rubber edge trim (raised rim around entire face)
      // Top edge
      const topRim = new THREE.BoxGeometry(2.8, 0.15, 0.55);
      topRim.translate(0, 3.4, cabZ + d * 0.35);
      p.push({ geo: topRim, color: CAB_RUBBER });
      // Bottom edge
      const botRim = new THREE.BoxGeometry(2.8, 0.15, 0.55);
      botRim.translate(0, 0.25, cabZ + d * 0.35);
      p.push({ geo: botRim, color: CAB_RUBBER });
      // Side edges
      for (const dx of [-1.45, 1.45]) {
        const sideRim = new THREE.BoxGeometry(0.15, 3.0, 0.55);
        sideRim.translate(dx, 1.85, cabZ + d * 0.35);
        p.push({ geo: sideRim, color: CAB_RUBBER });
      }

      // Coupler (below cab)
      const coupler = new THREE.BoxGeometry(0.8, 0.25, 0.2);
      coupler.translate(0, 0.25, cabZ + d * 0.6);
      p.push({ geo: coupler, color: BOGIE });
    }
  }

  // ── Inter-car gangway connections ──
  for (let j = 0; j < numCars - 1; j++) {
    const jz = startZ - (j + 1) * carLen - j * gap - gap / 2;
    const gang = new THREE.BoxGeometry(2.0, 2.2, gap - 0.15);
    gang.translate(0, 1.7, jz);
    p.push({ geo: gang, color: LOWER });
    // Bellows ribs
    for (const ribOff of [-0.25, -0.08, 0.08, 0.25]) {
      const rib = new THREE.BoxGeometry(2.3, 2.4, 0.05);
      rib.translate(0, 1.7, jz + ribOff);
      p.push({ geo: rib, color: BOGIE });
    }
  }

  return _mergeGeomsColored(p);
}

function _buildOresundstagHeadlightGeometry() { return _trainHeadlight(3); }

// ── Swedish Passenger Ferry (Waxholmsbolaget-style) ─────────────────────────
// High-poly ~20k faces.  White hull, blue+yellow Swedish livery stripes,
// two passenger decks, articulated bow ramp, bridge, radar mast, funnel.
function _buildFerryGeometry() {
  const HULL    = 0xf0ede8;   // white hull
  const DECK    = 0xd4cfc8;   // light grey deck surface
  const BELOW   = 0x2d2d2d;   // dark waterline / keel
  const WINDOW  = 0x0f1a2b;   // very dark glass
  const BLUE    = 0x005ca9;   // Swedish blue stripe
  const YELLOW  = 0xfecc02;   // Swedish yellow stripe
  const RAIL    = 0xc8c4be;   // deck railings
  const BRIDGE  = 0xe8e4de;   // bridge superstructure
  const MAST    = 0x888888;   // mast/antenna grey
  const FUNNEL  = 0xf0ede8;   // funnel body (white)
  const DOOR    = 0x374151;   // dark doors
  const RAMP    = 0x9ca3af;   // bow ramp metal
  const HS = 20;
  const p = [];

  // Dimensions (metres, model scale — ferries are big)
  const hullLen = 28;
  const hullW = 6.5;
  const halfL = hullLen / 2;
  const halfW = hullW / 2;

  // ═══════════════════════════════════════════════════════════════
  // 1. HULL — above-waterline box + smooth curved underwater hull
  // ═══════════════════════════════════════════════════════════════

  // Above-waterline hull: box sides (from waterline up to deck)
  const waterY = -0.5;  // waterline Y position
  const freeboard = 1.5; // height above waterline
  const hullTop = waterY + freeboard;

  // Mid-section walls (above waterline, vertical sides)
  const hullAbove = new THREE.BoxGeometry(hullW, freeboard, hullLen * 0.6);
  hullAbove.translate(0, waterY + freeboard / 2, -hullLen * 0.05);
  p.push({ geo: hullAbove, color: HULL });

  // Bow taper — narrowing forward section (above waterline)
  for (let i = 0; i < 8; i++) {
    const t = i / 8;
    const bw = hullW * (1.0 - t * 0.7);
    const bz = halfL - hullLen * 0.2 + (hullLen * 0.22) * t;
    const bl = hullLen * 0.03;
    const bowH = freeboard * (1.0 - t * 0.3);
    const bow = new THREE.BoxGeometry(bw, bowH, bl);
    bow.translate(0, waterY + bowH / 2, bz);
    p.push({ geo: bow, color: HULL });
  }
  // Bow tip (sharp prow)
  const bowTip = new THREE.BoxGeometry(hullW * 0.15, freeboard * 0.5, 0.6);
  bowTip.translate(0, waterY + freeboard * 0.35, halfL + 0.3);
  p.push({ geo: bowTip, color: HULL });

  // Stern transom (flat rear wall)
  const stern = new THREE.BoxGeometry(hullW, freeboard, 0.2);
  stern.translate(0, waterY + freeboard / 2, -halfL - 0.05);
  p.push({ geo: stern, color: HULL });

  // ── Underwater hull: smooth half-ellipsoid (bathtub / canoe shape) ──
  // A stretched bottom-hemisphere gives a natural boat curve that
  // is widest at midships and tapers at bow and stern.
  const draft = 1.3;  // depth below waterline
  const hullBottom = new THREE.SphereGeometry(
    1, HS * 2, Math.ceil(HS / 2),
    0, Math.PI * 2,
    Math.PI / 2, Math.PI / 2   // bottom hemisphere only
  );
  hullBottom.scale(halfW * 0.98, draft, halfL * 0.95);
  hullBottom.translate(0, waterY, -hullLen * 0.02);
  p.push({ geo: hullBottom, color: HULL });

  // Stern underwater extension (the ellipsoid tapers, but stern should
  // stay relatively full — add a small curved cap)
  const sternCap = new THREE.SphereGeometry(
    1, HS, Math.ceil(HS / 2),
    0, Math.PI * 2,
    Math.PI / 2, Math.PI / 2
  );
  sternCap.scale(halfW * 0.85, draft * 0.7, 1.5);
  sternCap.translate(0, waterY, -halfL + 0.5);
  p.push({ geo: sternCap, color: HULL });

  // Waterline accent (thin dark line at the water surface)
  for (const dx of [-halfW * 0.99, halfW * 0.99]) {
    const wl = new THREE.BoxGeometry(0.05, 0.08, hullLen * 0.7);
    wl.translate(dx, waterY, -hullLen * 0.02);
    p.push({ geo: wl, color: BELOW });
  }

  // ═══════════════════════════════════════════════════════════════
  // 2. BLUE + YELLOW LIVERY STRIPES
  // ═══════════════════════════════════════════════════════════════

  for (const dx of [-halfW - 0.02, halfW + 0.02]) {
    // Blue stripe (wider, lower)
    const blue = new THREE.BoxGeometry(0.06, 0.25, hullLen * 0.75);
    blue.translate(dx, 0.3, -hullLen * 0.02);
    p.push({ geo: blue, color: BLUE });
    // Yellow stripe (thinner, above blue)
    const yellow = new THREE.BoxGeometry(0.06, 0.15, hullLen * 0.75);
    yellow.translate(dx, 0.60, -hullLen * 0.02);
    p.push({ geo: yellow, color: YELLOW });
  }

  // ═══════════════════════════════════════════════════════════════
  // 3. LOWER DECK (main car/passenger deck)
  // ═══════════════════════════════════════════════════════════════

  // Deck floor
  const deck1 = new THREE.BoxGeometry(hullW - 0.2, 0.15, hullLen * 0.85);
  deck1.translate(0, 1.0, -hullLen * 0.02);
  p.push({ geo: deck1, color: DECK });

  // Lower deck walls (with window cutouts approximated by alternating hull/window)
  for (const dx of [-halfW + 0.05, halfW - 0.05]) {
    // Solid wall base
    const wallBase = new THREE.BoxGeometry(0.12, 0.8, hullLen * 0.6);
    wallBase.translate(dx, 1.5, -hullLen * 0.08);
    p.push({ geo: wallBase, color: HULL });
    // Window band
    const winBand = new THREE.BoxGeometry(0.08, 0.55, hullLen * 0.55);
    winBand.translate(dx, 1.65, -hullLen * 0.06);
    p.push({ geo: winBand, color: WINDOW });
    // Individual window panes (10 per side)
    for (let w = 0; w < 10; w++) {
      const wz = -hullLen * 0.3 + w * (hullLen * 0.55 / 10);
      const pane = new THREE.BoxGeometry(0.06, 0.45, hullLen * 0.04);
      pane.translate(dx, 1.65, wz);
      p.push({ geo: pane, color: WINDOW });
      // Pillar between windows
      const pillar = new THREE.BoxGeometry(0.13, 0.8, 0.08);
      pillar.translate(dx, 1.5, wz + hullLen * 0.025);
      p.push({ geo: pillar, color: HULL });
    }
  }

  // Lower deck doors (2 per side, near bow and midships)
  for (const dx of [-halfW + 0.04, halfW - 0.04]) {
    for (const dz of [hullLen * 0.15, -hullLen * 0.15]) {
      const door = new THREE.BoxGeometry(0.14, 1.6, 0.8);
      door.translate(dx, 1.5, dz);
      p.push({ geo: door, color: DOOR });
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 4. UPPER DECK (passenger saloon)
  // ═══════════════════════════════════════════════════════════════

  // Upper deck floor
  const deck2 = new THREE.BoxGeometry(hullW - 0.5, 0.15, hullLen * 0.65);
  deck2.translate(0, 2.15, -hullLen * 0.08);
  p.push({ geo: deck2, color: DECK });

  // Upper deck walls + windows
  for (const dx of [-halfW + 0.3, halfW - 0.3]) {
    const uWall = new THREE.BoxGeometry(0.10, 1.2, hullLen * 0.55);
    uWall.translate(dx, 2.85, -hullLen * 0.06);
    p.push({ geo: uWall, color: HULL });
    // Upper window band
    const uWin = new THREE.BoxGeometry(0.07, 0.7, hullLen * 0.5);
    uWin.translate(dx, 2.95, -hullLen * 0.05);
    p.push({ geo: uWin, color: WINDOW });
    // Individual upper windows (8 per side)
    for (let w = 0; w < 8; w++) {
      const wz = -hullLen * 0.25 + w * (hullLen * 0.5 / 8);
      const pane = new THREE.BoxGeometry(0.06, 0.6, hullLen * 0.045);
      pane.translate(dx, 2.95, wz);
      p.push({ geo: pane, color: WINDOW });
      const pillar = new THREE.BoxGeometry(0.11, 1.2, 0.06);
      pillar.translate(dx, 2.85, wz + hullLen * 0.03);
      p.push({ geo: pillar, color: HULL });
    }
  }

  // Upper deck roof
  const roof2 = new THREE.BoxGeometry(hullW - 0.4, 0.12, hullLen * 0.56);
  roof2.translate(0, 3.5, -hullLen * 0.07);
  p.push({ geo: roof2, color: HULL });

  // ═══════════════════════════════════════════════════════════════
  // 5. BRIDGE / WHEELHOUSE (set back towards stern on upper deck)
  // ═══════════════════════════════════════════════════════════════

  const bridgeZ = -hullLen * 0.18;
  const bridgeW = hullW - 1.2;
  const bridgeLen = 3.5;

  // Bridge walls
  const bridgeBox = new THREE.BoxGeometry(bridgeW, 1.8, bridgeLen);
  bridgeBox.translate(0, 4.45, bridgeZ);
  p.push({ geo: bridgeBox, color: BRIDGE });

  // Bridge roof
  const bridgeRoof = new THREE.BoxGeometry(bridgeW + 0.5, 0.12, bridgeLen + 0.6);
  bridgeRoof.translate(0, 5.4, bridgeZ);
  p.push({ geo: bridgeRoof, color: BRIDGE });

  // Bridge windshield (panoramic, wrap-around)
  // Front
  const bWindFront = new THREE.BoxGeometry(bridgeW - 0.3, 1.0, 0.08);
  bWindFront.translate(0, 4.6, bridgeZ + bridgeLen / 2 + 0.04);
  p.push({ geo: bWindFront, color: WINDOW });
  // Side windows
  for (const dx of [-(bridgeW / 2 + 0.02), bridgeW / 2 + 0.02]) {
    const bWindSide = new THREE.BoxGeometry(0.08, 1.0, bridgeLen - 0.3);
    bWindSide.translate(dx, 4.6, bridgeZ);
    p.push({ geo: bWindSide, color: WINDOW });
  }
  // Rear window
  const bWindRear = new THREE.BoxGeometry(bridgeW * 0.6, 0.7, 0.08);
  bWindRear.translate(0, 4.7, bridgeZ - bridgeLen / 2 - 0.04);
  p.push({ geo: bWindRear, color: WINDOW });

  // Bridge console (dark strip at bottom of windshield)
  const console = new THREE.BoxGeometry(bridgeW - 0.4, 0.3, 0.15);
  console.translate(0, 3.9, bridgeZ + bridgeLen / 2 + 0.03);
  p.push({ geo: console, color: BELOW });

  // ═══════════════════════════════════════════════════════════════
  // 6. RADAR MAST + NAVIGATION EQUIPMENT
  // ═══════════════════════════════════════════════════════════════

  // Main mast (vertical pole)
  const mast = new THREE.CylinderGeometry(0.08, 0.08, 3.5, HS);
  mast.translate(0, 7.1, bridgeZ);
  p.push({ geo: mast, color: MAST });

  // Radar dome
  const radarDome = new THREE.SphereGeometry(0.35, HS, HS);
  radarDome.translate(0, 8.9, bridgeZ);
  p.push({ geo: radarDome, color: HULL });

  // Radar scanner bar
  const radarBar = new THREE.BoxGeometry(2.8, 0.06, 0.18);
  radarBar.translate(0, 8.4, bridgeZ);
  p.push({ geo: radarBar, color: MAST });

  // Cross-tree / yardarm
  const yardarm = new THREE.BoxGeometry(2.0, 0.04, 0.04);
  yardarm.translate(0, 7.8, bridgeZ);
  p.push({ geo: yardarm, color: MAST });

  // Navigation lights on yardarm tips
  for (const dx of [-1.0, 1.0]) {
    const navLight = new THREE.SphereGeometry(0.05, 8, 8);
    navLight.translate(dx, 7.85, bridgeZ);
    p.push({ geo: navLight, color: dx > 0 ? 0x22c55e : 0xef4444 });
  }

  // Antennas (2 whip antennas)
  for (const dx of [-0.3, 0.3]) {
    const ant = new THREE.CylinderGeometry(0.02, 0.02, 2.0, 6);
    ant.translate(dx, 7.5, bridgeZ + 0.5);
    p.push({ geo: ant, color: MAST });
  }

  // ═══════════════════════════════════════════════════════════════
  // 7. FUNNEL (short, with Swedish blue+yellow band)
  // ═══════════════════════════════════════════════════════════════

  const funnelZ = bridgeZ - 2.5;
  // Funnel body
  const funnel = new THREE.CylinderGeometry(0.5, 0.6, 1.8, HS);
  funnel.translate(0, 4.5, funnelZ);
  p.push({ geo: funnel, color: FUNNEL });
  // Funnel top cap (dark exhaust ring)
  const funnelTop = new THREE.CylinderGeometry(0.45, 0.5, 0.15, HS);
  funnelTop.translate(0, 5.45, funnelZ);
  p.push({ geo: funnelTop, color: BELOW });
  // Blue band on funnel
  const funnelBlue = new THREE.CylinderGeometry(0.52, 0.56, 0.35, HS);
  funnelBlue.translate(0, 4.35, funnelZ);
  p.push({ geo: funnelBlue, color: BLUE });
  // Yellow band on funnel
  const funnelYellow = new THREE.CylinderGeometry(0.51, 0.55, 0.15, HS);
  funnelYellow.translate(0, 4.58, funnelZ);
  p.push({ geo: funnelYellow, color: YELLOW });

  // ═══════════════════════════════════════════════════════════════
  // 8. BOW RAMP (articulated, distinctive feature)
  // ═══════════════════════════════════════════════════════════════

  const rampZ = halfL + 0.3;
  // Ramp frame (raised position — closed during sailing)
  const rampPanel = new THREE.BoxGeometry(hullW * 0.6, 2.0, 0.15);
  rampPanel.translate(0, 1.6, rampZ);
  p.push({ geo: rampPanel, color: RAMP });
  // Ramp hinge brackets
  for (const dx of [-hullW * 0.25, hullW * 0.25]) {
    const hinge = new THREE.BoxGeometry(0.2, 0.3, 0.3);
    hinge.translate(dx, 0.6, rampZ);
    p.push({ geo: hinge, color: BELOW });
  }
  // Ramp surface detail (horizontal ribs for traction)
  for (let r = 0; r < 5; r++) {
    const rib = new THREE.BoxGeometry(hullW * 0.55, 0.04, 0.12);
    rib.translate(0, 0.8 + r * 0.35, rampZ + 0.02);
    p.push({ geo: rib, color: MAST });
  }
  // Ramp arm (curved support, simplified as angled box)
  for (const dx of [-hullW * 0.28, hullW * 0.28]) {
    const arm = new THREE.BoxGeometry(0.12, 1.8, 0.12);
    arm.translate(dx, 1.5, rampZ - 0.2);
    p.push({ geo: arm, color: RAMP });
  }

  // ═══════════════════════════════════════════════════════════════
  // 9. DECK RAILINGS (both decks, bow and stern)
  // ═══════════════════════════════════════════════════════════════

  // Upper deck railings (open aft sun-deck area)
  for (const dx of [-halfW + 0.4, halfW - 0.4]) {
    // Aft section (behind bridge)
    for (let r = 0; r < 4; r++) {
      const rz = -halfL + 1.0 + r * 1.2;
      const post = new THREE.BoxGeometry(0.04, 0.9, 0.04);
      post.translate(dx, 2.6, rz);
      p.push({ geo: post, color: RAIL });
    }
    // Horizontal rail
    const hRail = new THREE.BoxGeometry(0.03, 0.03, 5.0);
    hRail.translate(dx, 3.0, -halfL + 3.0);
    p.push({ geo: hRail, color: RAIL });
  }

  // Lower deck bow railings
  for (const dx of [-halfW + 0.15, halfW - 0.15]) {
    for (let r = 0; r < 3; r++) {
      const rz = halfL - 3.0 + r * 1.0;
      const post = new THREE.BoxGeometry(0.04, 0.8, 0.04);
      post.translate(dx, 1.4, rz);
      p.push({ geo: post, color: RAIL });
    }
    const hRailBow = new THREE.BoxGeometry(0.03, 0.03, 3.0);
    hRailBow.translate(dx, 1.75, halfL - 2.0);
    p.push({ geo: hRailBow, color: RAIL });
  }

  // Stern railings
  for (const dx of [-halfW + 0.15, halfW - 0.15]) {
    for (let r = 0; r < 2; r++) {
      const post = new THREE.BoxGeometry(0.04, 0.8, 0.04);
      post.translate(dx, 1.4, -halfL + 0.3 + r * 1.0);
      p.push({ geo: post, color: RAIL });
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 10. DETAILS — bollards, stern platform, flag
  // ═══════════════════════════════════════════════════════════════

  // Stern platform / swim deck
  const sternPlat = new THREE.BoxGeometry(hullW * 0.7, 0.12, 1.2);
  sternPlat.translate(0, -0.1, -halfL - 0.5);
  p.push({ geo: sternPlat, color: DECK });

  // Bollards (4 — 2 bow, 2 stern)
  for (const dz of [halfL - 1.5, -halfL + 0.5]) {
    for (const dx of [-halfW + 0.5, halfW - 0.5]) {
      const bollard = new THREE.CylinderGeometry(0.08, 0.1, 0.35, HS);
      bollard.translate(dx, 1.2, dz);
      p.push({ geo: bollard, color: BELOW });
    }
  }

  // Swedish flag at stern (simplified as blue rectangle + yellow cross)
  const flagPole = new THREE.CylinderGeometry(0.02, 0.02, 1.5, 6);
  flagPole.translate(0, 1.8, -halfL - 0.3);
  p.push({ geo: flagPole, color: MAST });
  const flagBlue = new THREE.BoxGeometry(0.04, 0.6, 0.9);
  flagBlue.translate(0, 2.7, -halfL - 0.3);
  p.push({ geo: flagBlue, color: BLUE });
  // Yellow cross (horizontal + vertical bars)
  const crossH = new THREE.BoxGeometry(0.05, 0.12, 0.9);
  crossH.translate(0, 2.7, -halfL - 0.3);
  p.push({ geo: crossH, color: YELLOW });
  const crossV = new THREE.BoxGeometry(0.05, 0.6, 0.12);
  crossV.translate(0, 2.7, -halfL - 0.55);
  p.push({ geo: crossV, color: YELLOW });

  // Searchlight on bridge roof
  const searchlight = new THREE.CylinderGeometry(0.12, 0.12, 0.2, HS);
  searchlight.rotateX(Math.PI / 2);
  searchlight.translate(0, 5.55, bridgeZ + bridgeLen / 2 + 0.2);
  p.push({ geo: searchlight, color: 0xfef3c7 });

  // Lifebuoys (4, mounted on railings)
  for (const [dx, dz] of [[-halfW + 0.1, 2], [halfW - 0.1, 2],
                           [-halfW + 0.1, -4], [halfW - 0.1, -4]]) {
    const buoy = new THREE.TorusGeometry(0.22, 0.06, 8, HS);
    buoy.rotateY(Math.PI / 2);
    buoy.translate(dx, 1.7, dz);
    p.push({ geo: buoy, color: 0xef4444 }); // red/orange
  }

  return _mergeGeomsColored(p);
}

function _buildFerryHeadlightGeometry() {
  // Headlights at bow, similar approach to train headlights
  const hullLen = 28;
  const frontZ = hullLen / 2 + 0.5;
  const parts = [];
  for (const dx of [-0.8, 0.8]) {
    parts.push(..._buildHeadlightCluster(dx, 1.5, frontZ));
  }
  const beam = new THREE.CylinderGeometry(0.3, 1.6, 14.0, S);
  beam.rotateX(Math.PI / 2);
  beam.translate(0, 1.4, frontZ + 7.5);
  parts.push(beam);
  const spill = new THREE.CylinderGeometry(2.0, 2.0, 0.15, S);
  spill.translate(0, 0.15, frontZ + 1.0);
  parts.push(spill);
  return _mergeGeoms(parts);
}

function _buildTramGeometry() {
  // High-poly 4-car articulated tram (~5k faces).
  // Chamfered rectangular cars, detailed windows with individual panes,
  // articulated bellows with ribs, detailed bogies, pantograph,
  // front/rear cab faces with headlights.
  const BODY     = _tramBodyColor;
  const TRIM     = _darkenCol(_tramBodyColor, 0.45);
  const WINDOW   = 0x111827;
  const ROOF     = 0x6b7280;
  const BELLOW   = 0x374151;
  const BOGIE    = 0x1c1917;
  const PANTO    = 0x44403c;
  const DEST     = 0xf59e0b;
  const HEADLT   = 0xfef3c7;
  const DOOR     = _darkenCol(_tramBodyColor, 0.55);
  const p = [];

  const carLen   = 5.5;
  const carW     = 2.5;
  const bellowW  = 0.7;
  const numCars  = 4;
  const totalLen = numCars * carLen + (numCars - 1) * bellowW;
  const startZ   = totalLen / 2;

  for (let c = 0; c < numCars; c++) {
    const cz = startZ - c * (carLen + bellowW) - carLen / 2;

    // ── Car body (chamfered via _roundedBody) ──
    const body = _roundedBody(carW, 2.0, carLen, S);
    body.translate(0, 1.3, cz);
    p.push({ geo: body, color: BODY });

    // Lower skirt
    const skirt = new THREE.BoxGeometry(carW + 0.04, 0.25, carLen);
    skirt.translate(0, 0.15, cz);
    p.push({ geo: skirt, color: TRIM });

    // Trim line (darker green strip between body and windows)
    for (const dx of [-carW / 2 - 0.02, carW / 2 + 0.02]) {
      const trim = new THREE.BoxGeometry(0.05, 0.12, carLen - 0.3);
      trim.translate(dx, 1.95, cz);
      p.push({ geo: trim, color: TRIM });
    }

    // ── Window band ──
    // Upper body section (window area)
    const upper = new THREE.BoxGeometry(carW - 0.08, 0.8, carLen - 0.3);
    upper.translate(0, 2.5, cz);
    p.push({ geo: upper, color: WINDOW });

    // Individual window panes (6 per side per car)
    const paneCount = 6;
    const paneSpacing = (carLen - 1.0) / paneCount;
    for (let w = 0; w < paneCount; w++) {
      const wz = cz - carLen / 2 + 0.5 + w * paneSpacing + paneSpacing / 2;
      for (const dx of [-(carW / 2 + 0.01), (carW / 2 + 0.01)]) {
        const pane = new THREE.BoxGeometry(0.04, 0.7, paneSpacing * 0.7);
        pane.translate(dx, 2.5, wz);
        p.push({ geo: pane, color: WINDOW });
      }
    }

    // Pillars between windows
    for (let w = 1; w < paneCount; w++) {
      const wz = cz - carLen / 2 + 0.5 + w * paneSpacing;
      for (const dx of [-(carW / 2 + 0.02), (carW / 2 + 0.02)]) {
        const pillar = new THREE.BoxGeometry(0.05, 0.8, 0.08);
        pillar.translate(dx, 2.5, wz);
        p.push({ geo: pillar, color: BODY });
      }
    }

    // ── Doors (1 per side per car) ──
    for (const dx of [-(carW / 2 + 0.02), (carW / 2 + 0.02)]) {
      const doorFrame = new THREE.BoxGeometry(0.04, 1.8, 0.9);
      doorFrame.translate(dx, 1.4, cz);
      p.push({ geo: doorFrame, color: DOOR });
      const doorWin = new THREE.BoxGeometry(0.04, 0.6, 0.6);
      doorWin.translate(dx, 2.2, cz);
      p.push({ geo: doorWin, color: WINDOW });
    }

    // ── Roof (flat top) ──
    const roof = new THREE.BoxGeometry(carW, 0.18, carLen - 0.15);
    roof.translate(0, 2.98, cz);
    p.push({ geo: roof, color: ROOF });

    // ── Bogies (2 per car) ──
    for (const endSign of [-1, 1]) {
      const bz = cz + endSign * (carLen / 2 - 0.9);
      // Frame
      const frame = new THREE.BoxGeometry(2.1, 0.2, 1.3);
      frame.translate(0, -0.05, bz);
      p.push({ geo: frame, color: BOGIE });
      // Side frames
      for (const dx of [-0.9, 0.9]) {
        const sf = new THREE.BoxGeometry(0.12, 0.35, 1.2);
        sf.translate(dx, -0.05, bz);
        p.push({ geo: sf, color: BOGIE });
      }
      // Wheels (4 per bogie: 2 axles × 2 sides)
      for (const dx of [-0.85, 0.85]) {
        for (const wdz of [-0.3, 0.3]) {
          const wh = new THREE.CylinderGeometry(0.3, 0.3, 0.25, S);
          wh.rotateZ(Math.PI / 2);
          wh.translate(dx, -0.1, bz + wdz);
          p.push({ geo: wh, color: BOGIE });
        }
      }
    }
  }

  // ── Articulated bellows ──
  for (let j = 0; j < numCars - 1; j++) {
    const jz = startZ - (j + 1) * carLen - j * bellowW - bellowW / 2;
    const bellow = new THREE.BoxGeometry(carW - 0.3, 2.0, bellowW - 0.1);
    bellow.translate(0, 1.3, jz);
    p.push({ geo: bellow, color: BELLOW });
    // 5 accordion ribs per joint
    for (let r = -2; r <= 2; r++) {
      const rib = new THREE.BoxGeometry(carW - 0.15, 2.1, 0.05);
      rib.translate(0, 1.3, jz + r * (bellowW / 5));
      p.push({ geo: rib, color: BOGIE });
    }
    // Gangway floor
    const floor = new THREE.BoxGeometry(carW - 0.4, 0.1, bellowW - 0.15);
    floor.translate(0, 0.25, jz);
    p.push({ geo: floor, color: BELLOW });
  }

  // ── Front cab face ──
  const fz = startZ;
  // Full face
  const fFace = new THREE.BoxGeometry(carW, 2.4, 0.12);
  fFace.translate(0, 1.5, fz + 0.06);
  p.push({ geo: fFace, color: BODY });
  // Large windshield
  const fWind = new THREE.BoxGeometry(carW - 0.5, 1.0, 0.1);
  fWind.translate(0, 2.4, fz + 0.08);
  p.push({ geo: fWind, color: WINDOW });
  // Windshield divider
  const fDiv = new THREE.BoxGeometry(0.06, 1.0, 0.11);
  fDiv.translate(0, 2.4, fz + 0.09);
  p.push({ geo: fDiv, color: BODY });
  // Destination display
  const fDest = new THREE.BoxGeometry(1.4, 0.28, 0.08);
  fDest.translate(0, 2.95, fz + 0.08);
  p.push({ geo: fDest, color: DEST });
  // Lower front panel
  const fLower = new THREE.BoxGeometry(carW - 0.2, 0.6, 0.1);
  fLower.translate(0, 0.9, fz + 0.08);
  p.push({ geo: fLower, color: TRIM });
  // Headlights
  for (const dx of [-0.85, 0.85]) {
    const hl = new THREE.CylinderGeometry(0.15, 0.15, 0.1, S);
    hl.rotateX(Math.PI / 2);
    hl.translate(dx, 1.5, fz + 0.12);
    p.push({ geo: hl, color: HEADLT });
  }
  // Bumper
  const fBump = new THREE.BoxGeometry(carW, 0.18, 0.12);
  fBump.translate(0, 0.18, fz + 0.1);
  p.push({ geo: fBump, color: BOGIE });
  // Coupler
  const fCoup = new THREE.BoxGeometry(0.4, 0.2, 0.3);
  fCoup.translate(0, 0.3, fz + 0.2);
  p.push({ geo: fCoup, color: BOGIE });

  // ── Rear cab face (mirror of front) ──
  const rz = -startZ;
  const rFace = new THREE.BoxGeometry(carW, 2.4, 0.12);
  rFace.translate(0, 1.5, rz - 0.06);
  p.push({ geo: rFace, color: BODY });
  const rWind = new THREE.BoxGeometry(carW - 0.5, 1.0, 0.1);
  rWind.translate(0, 2.4, rz - 0.08);
  p.push({ geo: rWind, color: WINDOW });
  const rDiv = new THREE.BoxGeometry(0.06, 1.0, 0.11);
  rDiv.translate(0, 2.4, rz - 0.09);
  p.push({ geo: rDiv, color: BODY });
  const rDest = new THREE.BoxGeometry(1.4, 0.28, 0.08);
  rDest.translate(0, 2.95, rz - 0.08);
  p.push({ geo: rDest, color: DEST });
  const rLower = new THREE.BoxGeometry(carW - 0.2, 0.6, 0.1);
  rLower.translate(0, 0.9, rz - 0.08);
  p.push({ geo: rLower, color: TRIM });
  for (const dx of [-0.85, 0.85]) {
    // Tail lights (red)
    const tl = new THREE.CylinderGeometry(0.12, 0.12, 0.1, S);
    tl.rotateX(Math.PI / 2);
    tl.translate(dx, 1.5, rz - 0.12);
    p.push({ geo: tl, color: 0xdc2626 });
  }
  const rBump = new THREE.BoxGeometry(carW, 0.18, 0.12);
  rBump.translate(0, 0.18, rz - 0.1);
  p.push({ geo: rBump, color: BOGIE });
  const rCoup = new THREE.BoxGeometry(0.4, 0.2, 0.3);
  rCoup.translate(0, 0.3, rz - 0.2);
  p.push({ geo: rCoup, color: BOGIE });

  // ── Pantograph (on car 1 roof) ──
  const pantoZ = startZ - 1 * (carLen + bellowW) - carLen / 2;
  // Base insulators
  for (const dx of [-0.25, 0.25]) {
    const insul = new THREE.CylinderGeometry(0.08, 0.08, 0.2, S);
    insul.translate(dx, 3.15, pantoZ);
    p.push({ geo: insul, color: PANTO });
  }
  // Base frame
  const pBase = new THREE.BoxGeometry(0.8, 0.1, 0.5);
  pBase.translate(0, 3.25, pantoZ);
  p.push({ geo: pBase, color: PANTO });
  // Lower arm (angled)
  const armL = new THREE.BoxGeometry(0.08, 1.0, 0.08);
  armL.translate(-0.15, 3.7, pantoZ - 0.1);
  p.push({ geo: armL, color: PANTO });
  const armR = new THREE.BoxGeometry(0.08, 1.0, 0.08);
  armR.translate(0.15, 3.7, pantoZ + 0.1);
  p.push({ geo: armR, color: PANTO });
  // Upper arm
  const armU = new THREE.BoxGeometry(0.06, 0.8, 0.06);
  armU.translate(0, 4.4, pantoZ);
  p.push({ geo: armU, color: PANTO });
  // Contact strip
  const contact = new THREE.BoxGeometry(1.4, 0.04, 0.04);
  contact.translate(0, 4.8, pantoZ);
  p.push({ geo: contact, color: PANTO });
  // Cross brace
  const brace = new THREE.BoxGeometry(0.5, 0.04, 0.04);
  brace.translate(0, 4.2, pantoZ);
  p.push({ geo: brace, color: PANTO });

  return _mergeGeomsColored(p);
}

// ── High-poly bus helper ────────────────────────────────────────────────────
// Shared builder for all 3 bus variants. Accepts colour palette + length.

function _busCommon(palette, busLen) {
  const { BODY, LOWER, WINDOW, BUMPER, WHEEL, GRILLE, ROOF_AC, MIRROR, STRIPE } = palette;
  const halfL = busLen / 2;
  const p = [];

  // ── 1. Main body — rounded rectangle with high segment count ──
  const bodyLow = _roundedBody(2.5, 1.6, busLen, S);
  bodyLow.translate(0, 1.1, 0);
  p.push({ geo: bodyLow, color: BODY });

  // Lower skirt (darker trim)
  const skirt = new THREE.BoxGeometry(2.55, 0.3, busLen);
  skirt.translate(0, 0.15, 0);
  p.push({ geo: skirt, color: LOWER });

  // Upper body (window band area)
  const bodyUp = new THREE.BoxGeometry(2.4, 1.2, busLen - 0.3);
  bodyUp.translate(0, 2.5, 0);
  p.push({ geo: bodyUp, color: WINDOW });

  // Body pillars between windows (every ~1.5m)
  const pillarCount = Math.floor(busLen / 1.5);
  for (let i = 1; i < pillarCount; i++) {
    const pz = -halfL + i * (busLen / pillarCount);
    for (const dx of [-1.22, 1.22]) {
      const pillar = new THREE.BoxGeometry(0.06, 1.2, 0.1);
      pillar.translate(dx, 2.5, pz);
      p.push({ geo: pillar, color: BODY });
    }
  }

  // Horizontal stripe (livery accent line)
  if (STRIPE) {
    for (const dx of [-1.28, 1.28]) {
      const str = new THREE.BoxGeometry(0.06, 0.12, busLen - 0.6);
      str.translate(dx, 1.95, 0);
      p.push({ geo: str, color: STRIPE });
    }
  }

  // ── 2. Roof (flat top) ──
  const roof = new THREE.BoxGeometry(2.5, 0.18, busLen - 0.2);
  roof.translate(0, 3.18, 0);
  p.push({ geo: roof, color: BODY });

  // AC unit
  const acUnit = new THREE.BoxGeometry(1.4, 0.3, 2.4);
  acUnit.translate(0, 3.5, -1.0);
  p.push({ geo: acUnit, color: ROOF_AC });

  // ── 3. Front face ──
  const windshield = new THREE.BoxGeometry(2.2, 1.3, 0.12);
  windshield.translate(0, 2.5, halfL + 0.02);
  p.push({ geo: windshield, color: WINDOW });
  // Destination display
  const dest = new THREE.BoxGeometry(1.4, 0.3, 0.08);
  dest.translate(0, 3.1, halfL + 0.04);
  p.push({ geo: dest, color: 0xf59e0b });
  // Front grille panel
  const frontPanel = new THREE.BoxGeometry(2.4, 0.8, 0.15);
  frontPanel.translate(0, 1.3, halfL);
  p.push({ geo: frontPanel, color: GRILLE });
  // Headlight housings
  for (const dx of [-0.85, 0.85]) {
    const hl = new THREE.CylinderGeometry(0.2, 0.2, 0.15, S);
    hl.rotateX(Math.PI / 2);
    hl.translate(dx, 1.0, halfL + 0.08);
    p.push({ geo: hl, color: 0xfef3c7 });
  }
  // Front bumper
  const bumper = new THREE.BoxGeometry(2.5, 0.35, 0.25);
  bumper.translate(0, 0.45, halfL + 0.05);
  p.push({ geo: bumper, color: BUMPER });

  // ── 4. Rear face ──
  const rearPanel = new THREE.BoxGeometry(2.4, 1.8, 0.12);
  rearPanel.translate(0, 2.0, -halfL - 0.02);
  p.push({ geo: rearPanel, color: BODY });
  const rearGrille = new THREE.BoxGeometry(2.2, 0.7, 0.15);
  rearGrille.translate(0, 0.65, -halfL - 0.04);
  p.push({ geo: rearGrille, color: GRILLE });
  // Tail lights
  for (const dx of [-0.9, 0.9]) {
    const tl = new THREE.BoxGeometry(0.3, 0.2, 0.1);
    tl.translate(dx, 1.6, -halfL - 0.06);
    p.push({ geo: tl, color: 0xdc2626 });
  }
  const rearBumper = new THREE.BoxGeometry(2.5, 0.35, 0.25);
  rearBumper.translate(0, 0.45, -halfL - 0.05);
  p.push({ geo: rearBumper, color: BUMPER });

  // ── 5. Wheels (high-poly cylinders) ──
  // Front axle
  for (const dx of [-1.3, 1.3]) {
    const wf = new THREE.CylinderGeometry(0.55, 0.55, 0.4, S);
    wf.rotateZ(Math.PI / 2);
    wf.translate(dx, 0.55, halfL - 1.8);
    p.push({ geo: wf, color: WHEEL });
    // Hub cap
    const hub = new THREE.CylinderGeometry(0.25, 0.25, 0.42, S);
    hub.rotateZ(Math.PI / 2);
    hub.translate(dx, 0.55, halfL - 1.8);
    p.push({ geo: hub, color: GRILLE });
  }
  // Rear axle — dual wheels
  for (const dx of [-1.2, 1.2]) {
    for (const mult of [0.85, 1.15]) {
      const w = new THREE.CylinderGeometry(0.55, 0.55, 0.25, S);
      w.rotateZ(Math.PI / 2);
      w.translate(dx * mult, 0.55, -halfL + 1.8);
      p.push({ geo: w, color: WHEEL });
    }
  }

  // ── 6. Wheel arches ──
  for (const dz of [halfL - 1.8, -halfL + 1.8]) {
    const arch = new THREE.CylinderGeometry(0.72, 0.72, 2.6, S, 1, false, 0, Math.PI);
    arch.rotateZ(Math.PI / 2);
    arch.translate(0, 0.55, dz);
    p.push({ geo: arch, color: LOWER });
  }

  // ── 7. Side mirrors ──
  for (const dx of [-1.4, 1.4]) {
    const arm = new THREE.BoxGeometry(0.35, 0.06, 0.3);
    arm.translate(dx, 2.6, halfL - 0.2);
    p.push({ geo: arm, color: MIRROR });
    const glass = new THREE.BoxGeometry(0.2, 0.2, 0.12);
    glass.translate(dx, 2.55, halfL);
    p.push({ geo: glass, color: WINDOW });
  }

  // ── 8. Doors (2 per side) ──
  for (const dz of [halfL - 3.0, -0.5]) {
    for (const dx of [-1.27, 1.27]) {
      const door = new THREE.BoxGeometry(0.06, 2.0, 0.9);
      door.translate(dx, 1.6, dz);
      p.push({ geo: door, color: WINDOW });
    }
  }

  return _mergeGeomsColored(p);
}

// Bus variants — colours driven by operator branding from _configureBranding()
function _buildBusGeometry()  { return _busCommon(_busPalettes[0], 9.5); }
function _buildBusBGeometry() { return _busCommon(_busPalettes[1], 9.5); }
function _buildBusCGeometry() { return _busCommon(_busPalettes[2], 8.5); }

// Headlights use spheres for soft glow + a tapered cylinder beam cone.
// Overlapping additive layers compound brightness in the centre.

function _buildHeadlightCluster(centerX, lampY, lampZ) {
  const parts = [];
  // Outer halo — sphere
  const halo = new THREE.SphereGeometry(0.8, 8, 6);
  halo.translate(centerX, lampY, lampZ);
  parts.push(halo);
  // Mid glow
  const mid = new THREE.SphereGeometry(0.5, 8, 6);
  mid.translate(centerX, lampY, lampZ);
  parts.push(mid);
  // Bright core
  const core = new THREE.SphereGeometry(0.25, 6, 4);
  core.translate(centerX, lampY, lampZ);
  parts.push(core);
  return parts;
}

function _busHeadlight(frontZ) {
  const parts = [];
  for (const dx of [-0.8, 0.8]) {
    parts.push(..._buildHeadlightCluster(dx, 1.0, frontZ));
  }
  const beam = new THREE.CylinderGeometry(0.3, 1.3, 10.0, S);
  beam.rotateX(Math.PI / 2);
  beam.translate(0, 0.9, frontZ + 5.5);
  parts.push(beam);
  const spill = new THREE.CylinderGeometry(1.5, 1.5, 0.15, S);
  spill.translate(0, 0.15, frontZ + 1.0);
  parts.push(spill);
  return _mergeGeoms(parts);
}
function _buildBusHeadlightGeometry()  { return _busHeadlight(9.5 / 2 + 0.1); }
function _buildBusBHeadlightGeometry() { return _busHeadlight(9.5 / 2 + 0.1); }
function _buildBusCHeadlightGeometry() { return _busHeadlight(8.5 / 2 + 0.1); }

function _buildTramHeadlightGeometry() {
  // Tram front is at z ≈ 10.9 (totalLen/2 = 21.8/2)
  const frontZ = 10.9;
  const parts = [];
  for (const dx of [-0.6, 0.6]) {
    parts.push(..._buildHeadlightCluster(dx, 1.0, frontZ + 0.3));
  }
  const beam = new THREE.CylinderGeometry(0.2, 1.2, 9.0, 8);
  beam.rotateX(Math.PI / 2);
  beam.translate(0, 0.9, frontZ + 5.0);
  parts.push(beam);
  const spill = new THREE.CylinderGeometry(1.3, 1.3, 0.15, 8);
  spill.translate(0, 0.1, frontZ + 1.0);
  parts.push(spill);
  return _mergeGeoms(parts);
}

function _trainHeadlight(numCars) {
  // Front face is at totalLen/2 (flush with body — no cone)
  const totalLen = numCars * 9.0 + (numCars - 1) * 1.0;
  const frontZ = totalLen / 2 + 0.3;
  const parts = [];
  // Dual headlights at cab height
  for (const dx of [-0.9, 0.9]) {
    parts.push(..._buildHeadlightCluster(dx, 1.3, frontZ));
  }
  const beam = new THREE.CylinderGeometry(0.3, 1.4, 12.0, S);
  beam.rotateX(Math.PI / 2);
  beam.translate(0, 1.2, frontZ + 6.5);
  parts.push(beam);
  const spill = new THREE.CylinderGeometry(1.6, 1.6, 0.15, S);
  spill.translate(0, 0.15, frontZ + 0.8);
  parts.push(spill);
  return _mergeGeoms(parts);
}
function _buildTrainHeadlightGeometry()  { return _trainHeadlight(3); }
function _buildTrainBHeadlightGeometry() { return _trainHeadlight(4); }
function _buildTrainCHeadlightGeometry() { return _trainHeadlight(3); }

/**
 * Merge geometry parts that each carry a colour tag. Returns a single
 * BufferGeometry with a per-vertex `color` attribute baked in.
 * @param {Array<{geo: BufferGeometry, color: number}>} entries
 */
function _mergeGeomsColored(entries) {
  const geoms  = entries.map(e => e.geo);
  const merged = _mergeGeoms(geoms);

  // Walk entries again, painting vertex colours into the merged buffer.
  const count  = merged.attributes.position.count;
  const colors = new Float32Array(count * 3);
  const c = new THREE.Color();
  let vOff = 0;
  for (const entry of entries) {
    c.setHex(entry.color);
    const n = entry.geo.attributes.position.count;
    for (let i = 0; i < n; i++) {
      colors[(vOff + i) * 3    ] = c.r;
      colors[(vOff + i) * 3 + 1] = c.g;
      colors[(vOff + i) * 3 + 2] = c.b;
    }
    vOff += n;
  }
  merged.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  return merged;
}

/** Cheap geometry merge by concatenating attribute arrays. Three.js has a
 *  proper utility but we'd need to import it; this hand-rolled version
 *  works for our merge case (all BoxGeometries, position+normal only). */
function _mergeGeoms(geoms) {
  let posCount = 0, normCount = 0, indexCount = 0;
  let hasIndex = true;
  for (const g of geoms) {
    posCount += g.attributes.position.count;
    if (g.attributes.normal) normCount += g.attributes.normal.count;
    if (g.index) indexCount += g.index.count;
    else hasIndex = false;
  }

  const positions = new Float32Array(posCount * 3);
  const normals = new Float32Array(normCount * 3);
  const IndexArray = posCount > 65535 ? Uint32Array : Uint16Array;
  const indices = hasIndex ? new IndexArray(indexCount) : null;
  let pOff = 0, nOff = 0, iOff = 0, vOff = 0;

  for (const g of geoms) {
    const p = g.attributes.position.array;
    positions.set(p, pOff * 3);
    if (g.attributes.normal) {
      normals.set(g.attributes.normal.array, nOff * 3);
    }
    if (hasIndex && g.index) {
      const idx = g.index.array;
      for (let i = 0; i < idx.length; i++) {
        indices[iOff + i] = idx[i] + vOff;
      }
      iOff += idx.length;
    }
    vOff += g.attributes.position.count;
    pOff += g.attributes.position.count;
    nOff += g.attributes.normal ? g.attributes.normal.count : 0;
  }

  const merged = new THREE.BufferGeometry();
  merged.setAttribute('position', new THREE.BufferAttribute(positions, 3));
  merged.setAttribute('normal', new THREE.BufferAttribute(normals, 3));
  if (hasIndex) merged.setIndex(new THREE.BufferAttribute(indices, 1));
  return merged;
}

// ── Per-mode visuals ─────────────────────────────────────────────────────────

// All transports in shades of green per user pref. Hue varied so each
// type is still recognisable from the others at a glance.
const VEHICLE_COLORS = {
  bus:                0xdc2626, // red — Stockholm SL
  bus_b:              0x15803d, // green — Skånetrafiken
  bus_c:              0xea580c, // orange — Västtrafik
  bus_headlight:      0xfff8dc,
  bus_b_headlight:    0xfff8dc,
  bus_c_headlight:    0xfff8dc,
  ferry:              0xf0ede8, // white hull
  ferry_headlight:    0xfff8dc,
  tram:               0x16a34a, // green — Lund
  tram_headlight:     0xfff8dc,
  train:              0x2563eb, // blue — Pågatåg
  train_b:            0x94a3b8, // silver — Öresundståg
  train_c:            0xe2e8f0, // white — SL Pendeltåg
  train_headlight:    0xfff8dc,
  train_b_headlight:  0xfff8dc,
  train_c_headlight:  0xfff8dc,
  train_oresund:      0xc8cdd3,
  train_oresund_headlight: 0xfff8dc,
  plane:              0xe2e8f0,
};

function _buildInstancedMeshes(scene) {
  const builders = {
    plane:               _buildPlaneGeometry,
    train:               _buildTrainGeometry,
    train_b:             _buildTrainBGeometry,
    train_c:             _buildTrainCGeometry,
    train_oresund:       _buildOresundstagGeometry,
    train_headlight:     _buildTrainHeadlightGeometry,
    train_b_headlight:   _buildTrainBHeadlightGeometry,
    train_c_headlight:   _buildTrainCHeadlightGeometry,
    train_oresund_headlight: _buildOresundstagHeadlightGeometry,
    ferry:               _buildFerryGeometry,
    ferry_headlight:     _buildFerryHeadlightGeometry,
    tram:                _buildTramGeometry,
    tram_headlight:      _buildTramHeadlightGeometry,
    bus:                 _buildBusGeometry,
    bus_b:               _buildBusBGeometry,
    bus_c:               _buildBusCGeometry,
    bus_headlight:       _buildBusHeadlightGeometry,
    bus_b_headlight:     _buildBusBHeadlightGeometry,
    bus_c_headlight:     _buildBusCHeadlightGeometry,
  };
  const HEADLIGHT_TYPES = new Set([
    'bus_headlight', 'bus_b_headlight', 'bus_c_headlight',
    'tram_headlight',
    'train_headlight', 'train_b_headlight', 'train_c_headlight', 'train_oresund_headlight',
    'ferry_headlight',
  ]);
  for (const type of Object.keys(MAX_INSTANCES)) {
    const geo = builders[type]();
    geo.scale(VEHICLE_SCALE, VEHICLE_SCALE, VEHICLE_SCALE);
    const isHeadlight = HEADLIGHT_TYPES.has(type);
    const hasVertexColors = !!geo.attributes.color;
    const mat = isHeadlight
      ? new THREE.MeshBasicMaterial({
          color:       VEHICLE_COLORS[type],
          transparent: true,
          opacity:     0.40,
          blending:    THREE.AdditiveBlending,
          depthWrite:  false,
        })
      : new THREE.MeshLambertMaterial({
          color:        hasVertexColors ? 0xffffff : VEHICLE_COLORS[type],
          vertexColors: hasVertexColors,
          side:         THREE.DoubleSide,
        });
    const inst = new THREE.InstancedMesh(geo, mat, MAX_INSTANCES[type]);
    inst.count = 0;
    inst.frustumCulled = false; // we handle visibility via count
    inst.castShadow    = !isHeadlight;
    inst.receiveShadow = !isHeadlight;
    if (isHeadlight) {
      inst.visible = false;     // toggled by isNight in update()
      inst.renderOrder = 5;     // draw after solid geo so additive blends right
    }
    _instancedMeshes[type] = inst;
    _rootGroup.add(inst);
  }
}

// ── Drop shadows ────────────────────────────────────────────────────────────
// A flat silhouette under each vehicle matching its top-down shape.

let _shadowTex = null;
let _shadowMeshes = {};  // type → InstancedMesh

function _getShadowTexture() {
  if (_shadowTex) return _shadowTex;
  // Soft-edged shadow: opaque centre, feathered edges via radial gradient.
  const size = 128;
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = size;
  const ctx = canvas.getContext('2d');
  const grad = ctx.createRadialGradient(size/2, size/2, 0, size/2, size/2, size/2);
  grad.addColorStop(0,    'rgba(0,0,0,0.5)');
  grad.addColorStop(0.6,  'rgba(0,0,0,0.35)');
  grad.addColorStop(0.85, 'rgba(0,0,0,0.15)');
  grad.addColorStop(1,    'rgba(0,0,0,0)');
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, size, size);
  _shadowTex = new THREE.CanvasTexture(canvas);
  return _shadowTex;
}

/**
 * Build a flat shadow silhouette from a 2D outline (array of [x, z] points).
 * The outline is drawn onto a canvas, then used as an alpha-map on a plane.
 * Cheaper alternative: just merge flat boxes that approximate the top-down
 * footprint and apply the radial shadow texture.
 */
function _buildShadowGeo(parts) {
  // parts = [{w, l, x, z}] — rectangles in local vehicle space (top-down)
  // Each becomes a flat box on the XZ plane.
  const geoms = [];
  for (const { w, l, x, z } of parts) {
    const g = new THREE.PlaneGeometry(w, l);
    g.rotateX(-Math.PI / 2);
    g.translate(x || 0, 0, z || 0);
    geoms.push(g);
  }
  return _mergeGeoms(geoms);
}

function _busShadowGeo() {
  return _buildShadowGeo([
    { w: 2.8, l: 10.0, x: 0, z: 0 },       // main body
    { w: 2.4, l: 1.0,  x: 0, z: 5.0 },      // front
    { w: 2.3, l: 0.6,  x: 0, z: -5.0 },     // rear
  ]);
}

function _tramShadowGeo() {
  // 4 cars + 3 joints
  const carLen = 5.5, bellowW = 0.7, numCars = 4;
  const totalLen = numCars * carLen + (numCars - 1) * bellowW;
  const startZ = totalLen / 2;
  const parts = [];
  for (let c = 0; c < numCars; c++) {
    const cz = startZ - c * (carLen + bellowW) - carLen / 2;
    parts.push({ w: 2.6, l: carLen, x: 0, z: cz });
  }
  for (let j = 0; j < numCars - 1; j++) {
    const jz = startZ - (j + 1) * carLen - j * bellowW - bellowW / 2;
    parts.push({ w: 2.0, l: bellowW, x: 0, z: jz });
  }
  return _buildShadowGeo(parts);
}

function _trainShadowGeo(numCars) {
  const carLen = 9.0, gap = 1.0;
  const totalLen = numCars * carLen + (numCars - 1) * gap;
  const startZ = totalLen / 2;
  const parts = [];
  for (let c = 0; c < numCars; c++) {
    const cz = startZ - c * (carLen + gap) - carLen / 2;
    parts.push({ w: 3.0, l: carLen, x: 0, z: cz });
  }
  for (let j = 0; j < numCars - 1; j++) {
    const jz = startZ - (j + 1) * carLen - j * gap - gap / 2;
    parts.push({ w: 2.0, l: gap, x: 0, z: jz });
  }
  return _buildShadowGeo(parts);
}

function _planeShadowGeo() {
  return _buildShadowGeo([
    { w: 2.2, l: 18.0, x: 0, z: 0 },        // fuselage
    { w: 2.0, l: 3.5,  x: 0, z: 8.5 },       // nose
    { w: 18.0, l: 3.0, x: 0, z: -0.5 },      // wings
    { w: 6.0, l: 1.6,  x: 0, z: -9.5 },      // tail stabilisers
  ]);
}

const _SHADOW_GEO_BUILDERS = {
  bus:     _busShadowGeo,
  bus_b:   _busShadowGeo,
  bus_c:   () => _buildShadowGeo([
    { w: 2.6, l: 9.0, x: 0, z: 0 },
    { w: 2.2, l: 0.8, x: 0, z: 4.8 },
  ]),
  tram:    _tramShadowGeo,
  train:   () => _trainShadowGeo(3),
  train_b: () => _trainShadowGeo(4),
  train_c: () => _trainShadowGeo(3),
  plane:   _planeShadowGeo,
};

function _buildShadowMeshes() {
  const tex = _getShadowTexture();
  const mat = new THREE.MeshBasicMaterial({
    map:            tex,
    transparent:    true,
    opacity:        0.8,
    depthWrite:     false,
    depthTest:      true,
    side:           THREE.DoubleSide,
    // Push shadow toward the camera in the depth buffer so it renders
    // on top of the map tiles and ground plane without z-fighting.
    polygonOffset:       true,
    polygonOffsetFactor: -4,
    polygonOffsetUnits:  -4,
  });
  for (const type of Object.keys(MAX_INSTANCES)) {
    if (type.endsWith('_headlight')) continue;
    const builder = _SHADOW_GEO_BUILDERS[type];
    if (!builder) continue;
    const geo = builder();
    geo.scale(VEHICLE_SCALE, VEHICLE_SCALE, VEHICLE_SCALE);
    const inst = new THREE.InstancedMesh(geo, mat, MAX_INSTANCES[type]);
    inst.count = 0;
    inst.frustumCulled = false;
    inst.renderOrder = 999;  // render last so it composites on top of map
    _shadowMeshes[type] = inst;
    _rootGroup.add(inst);
  }
}

const _shadowPos   = new THREE.Vector3();
const _shadowQuat  = new THREE.Quaternion();
const _shadowScale = new THREE.Vector3(1, 1, 1);
const _shadowMat   = new THREE.Matrix4();
const _shadowYAxis = new THREE.Vector3(0, 1, 0);

function _updateShadow(type, slot, x, z, dirX, dirZ) {
  const mesh = _shadowMeshes[type];
  if (!mesh) return;
  _shadowPos.set(x, 0.5, z);
  const yaw = Math.atan2(dirX, dirZ);
  _shadowQuat.setFromAxisAngle(_shadowYAxis, yaw);
  // Track the body's LOD upscale so the shadow stays under the vehicle
  // at city-wide zoom.
  _shadowScale.set(_lodMult, _lodMult, _lodMult);
  _shadowMat.compose(_shadowPos, _shadowQuat, _shadowScale);
  mesh.setMatrixAt(slot, _shadowMat);
}

// ── Building spatial grid (for infection sampling) ──────────────────────────

function _buildBuildingGrid(buildings) {
  _buildings = buildings;
  _bGrid = new Map();
  if (!buildings) return;
  for (let bi = 0; bi < buildings.length; bi++) {
    const b = buildings[bi];
    const key = `${Math.floor(b.world_x / _HALO_GRID_CELL)},${Math.floor(b.world_z / _HALO_GRID_CELL)}`;
    let arr = _bGrid.get(key);
    if (!arr) { arr = []; _bGrid.set(key, arr); }
    arr.push(bi);
  }
}

function _sampleInfection(worldX, worldZ) {
  if (!_bGrid || !_latestCells) return 0;
  const gx = Math.floor(worldX / _HALO_GRID_CELL);
  const gz = Math.floor(worldZ / _HALO_GRID_CELL);
  let I = 0, N = 0;
  for (let dx = -1; dx <= 1; dx++) {
    for (let dz = -1; dz <= 1; dz++) {
      const arr = _bGrid.get(`${gx + dx},${gz + dz}`);
      if (!arr) continue;
      for (let k = 0; k < arr.length; k++) {
        const c = _latestCells[arr[k]];
        if (c && c.N > 0) { I += c.I; N += c.N; }
      }
    }
  }
  return N > 0 ? I / N : 0;
}

// ── Transit mask sprites ────────────────────────────────────────────────────
// Small mask emoji billboard shown on a fraction of vehicles when the
// "transit_masks" intervention is active.  Gives immediate visual feedback
// that the policy is in effect without cluttering every single vehicle.

function _getMaskTexture() {
  if (_maskTex) return _maskTex;
  const size = 128;
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = size;
  const ctx = canvas.getContext('2d');
  // Light semi-transparent circle background
  ctx.beginPath();
  ctx.arc(size/2, size/2, size/2 - 4, 0, Math.PI * 2);
  ctx.fillStyle = 'rgba(220,240,255,0.7)';
  ctx.fill();
  ctx.lineWidth = 3;
  ctx.strokeStyle = '#3b82f6';
  ctx.stroke();
  // Mask emoji
  ctx.font = `${size * 0.55}px sans-serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = '#000000';
  ctx.fillText('\uD83D\uDE37', size/2, size/2 + 2);   // 😷
  _maskTex = new THREE.CanvasTexture(canvas);
  _maskTex.colorSpace = THREE.SRGBColorSpace;
  return _maskTex;
}

function _buildMaskSprites() {
  const tex = _getMaskTexture();
  const mat = new THREE.MeshBasicMaterial({
    map:         tex,
    transparent: true,
    depthWrite:  false,
    depthTest:   true,
    side:        THREE.DoubleSide,
  });
  const baseGeo = new THREE.PlaneGeometry(_MASK_SPRITE_SIZE, _MASK_SPRITE_SIZE);
  for (const type of Object.keys(MAX_INSTANCES)) {
    if (type.endsWith('_headlight')) continue;
    const inst = new THREE.InstancedMesh(baseGeo.clone(), mat, MAX_INSTANCES[type]);
    inst.count = 0;
    inst.frustumCulled = false;
    inst.renderOrder = 1002;  // above biohazard sprites
    inst.visible = false;     // hidden until transit_masks is active
    _maskMeshes[type] = inst;
    _rootGroup.add(inst);
  }
}

const _maskPos   = new THREE.Vector3();
const _maskScale = new THREE.Vector3(1, 1, 1);
const _maskMat4  = new THREE.Matrix4();
const _maskQuat  = new THREE.Quaternion();
// Pre-compute: flat on the ground (rotated -90° around X, same as shadows)
_maskQuat.setFromAxisAngle(new THREE.Vector3(1, 0, 0), -Math.PI / 2);

function _updateMaskSprite(type, slot, x, z, show) {
  const mesh = _maskMeshes[type];
  if (!mesh) return;
  if (!show) {
    _maskScale.set(0, 0, 0);
    _maskPos.set(0, -9999, 0);
  } else {
    _maskScale.set(1, 1, 1);
    _maskPos.set(x, _MASK_SPRITE_Y, z);
  }
  _maskMat4.compose(_maskPos, _maskQuat, _maskScale);
  mesh.setMatrixAt(slot, _maskMat4);
}

// ── Static stop landmarks ────────────────────────────────────────────────────
//
// Each transport stop is rendered as a distinct procedural building so you
// can see WHERE the airport / train station / bus stops are even when no
// vehicle is currently in transit. Sized roughly to match the existing
// city building geometry (4–15 m tall) so the stops sit naturally in the
// scene rather than floating as abstract markers.
//
//   airport      → flat 60×12×30 terminal (light grey) + a dark runway
//                  ribbon following the OSM runway polyline
//   train        → 22×14×40 station building (red brick)
//   bus_station  → 18×6×22 station building (yellow)
//   bus          → 0.4×4 pole + 2.5×1.2 sign on top (slate)
//   tram         → 2×0.4×8 platform strip (green)

const _STOP_GRAY = 0x9ca3af;
const STOP_VISUALS = {
  airport:     { color: _STOP_GRAY, builder: () => _buildTerminalGeom(),     y: 6 },
  train:       { color: _STOP_GRAY, builder: () => _buildStationGeom(),      y: 7 },
  ferry:       { color: _STOP_GRAY, builder: () => _buildFerryTerminalGeom(), y: 2 },
  bus_station: { color: _STOP_GRAY, builder: () => _buildBusStationGeom(),   y: 3 },
  bus:         { color: _STOP_GRAY, builder: () => _buildBusStopGeom(),      y: 0 },
  tram:        { color: _STOP_GRAY, builder: () => _buildTramStopGeom(),     y: 0.2 },
};

function _buildTerminalGeom() {
  // Long flat terminal — distinctive against bus_station shape
  const main = new THREE.BoxGeometry(60, 10, 24);
  const tower = new THREE.BoxGeometry(6, 18, 6);
  tower.translate(20, 4, 0);
  return _mergeGeoms([main, tower]);
}

function _buildStationGeom() {
  // Long building with a slightly taller central section
  const main = new THREE.BoxGeometry(20, 10, 50);
  const peak = new THREE.BoxGeometry(20, 4, 14);
  peak.translate(0, 7, 0);
  return _mergeGeoms([main, peak]);
}

function _buildBusStationGeom() {
  return new THREE.BoxGeometry(16, 6, 22);
}

function _buildBusStopGeom() {
  // Pole + sign — small but recognisable
  const pole = new THREE.BoxGeometry(0.4, 4, 0.4);
  pole.translate(0, 2, 0);
  const sign = new THREE.BoxGeometry(2.5, 1.2, 0.2);
  sign.translate(0, 4.2, 0);
  return _mergeGeoms([pole, sign]);
}

function _buildTramStopGeom() {
  return new THREE.BoxGeometry(2, 0.4, 8);
}

function _buildFerryTerminalGeom() {
  // Pier / dock — long narrow rectangle extending from shore
  const pier = new THREE.BoxGeometry(8, 3, 30);
  const bollardL = new THREE.CylinderGeometry(0.5, 0.6, 2, 8);
  bollardL.translate(-3, 2, 10);
  const bollardR = new THREE.CylinderGeometry(0.5, 0.6, 2, 8);
  bollardR.translate(3, 2, 10);
  const shelter = new THREE.BoxGeometry(6, 4, 5);
  shelter.translate(0, 3.5, -5);
  return _mergeGeoms([pier, bollardL, bollardR, shelter]);
}

function _buildStaticStops() {
  if (!_infra || !_infra.stops) return;

  // Group stops by type
  const stopsByType = {};
  for (const stop of _infra.stops) {
    if (!stopsByType[stop.type]) stopsByType[stop.type] = [];
    stopsByType[stop.type].push(stop);
  }

  const tmpMat = new THREE.Matrix4();
  const tmpPos = new THREE.Vector3();
  const tmpQuat = new THREE.Quaternion();
  const tmpScale = new THREE.Vector3(1, 1, 1);

  for (const type of Object.keys(stopsByType)) {
    const visual = STOP_VISUALS[type];
    if (!visual) continue;
    const stops = stopsByType[type];
    if (stops.length === 0) continue;

    const geom = visual.builder();
    const mat = new THREE.MeshLambertMaterial({
      color: visual.color,
      side: THREE.DoubleSide,
    });
    const inst = new THREE.InstancedMesh(geom, mat, stops.length);
    inst.frustumCulled = false;
    inst.castShadow = true;
    inst.receiveShadow = true;

    for (let i = 0; i < stops.length; i++) {
      const s = stops[i];
      tmpPos.set(s.world_x, visual.y, s.world_z);
      tmpMat.compose(tmpPos, tmpQuat, tmpScale);
      inst.setMatrixAt(i, tmpMat);
    }
    inst.instanceMatrix.needsUpdate = true;
    inst.count = stops.length;
    inst.visible = false;          // hidden — POI pins provide the visual
    _stopMeshes[type] = inst;
    _rootGroup.add(inst);
  }
}

/** Build dark ribbon strips along every runway polyline. Sits 0.5 m above
 *  the ground so it doesn't z-fight the map base layer. 60 m wide, which
 *  is roughly real-world for a regional Swedish airport. */
function _buildRunwayStrips() {
  if (!_infra || !_infra.routes) return;
  const runways = _infra.routes.filter(r => r.type === 'runway');
  if (runways.length === 0) return;

  const HALF_W = 30; // metres
  const positions = [];
  const indices = [];
  let vBase = 0;

  for (const r of runways) {
    const poly = r.polyline;
    if (!poly || poly.length < 2) continue;

    for (let i = 0; i < poly.length; i++) {
      // Tangent: average of incoming and outgoing edges
      let tx = 0, tz = 0;
      if (i > 0) { tx += poly[i][0] - poly[i-1][0]; tz += poly[i][1] - poly[i-1][1]; }
      if (i < poly.length - 1) {
        tx += poly[i+1][0] - poly[i][0];
        tz += poly[i+1][1] - poly[i][1];
      }
      const len = Math.hypot(tx, tz) || 1;
      tx /= len; tz /= len;
      const px = -tz, pz = tx;  // perpendicular

      positions.push(poly[i][0] + px * HALF_W, 0.5, poly[i][1] + pz * HALF_W);
      positions.push(poly[i][0] - px * HALF_W, 0.5, poly[i][1] - pz * HALF_W);
    }

    for (let i = 0; i < poly.length - 1; i++) {
      const a = vBase + i * 2;
      const b = vBase + i * 2 + 1;
      const c = vBase + (i + 1) * 2;
      const d = vBase + (i + 1) * 2 + 1;
      indices.push(a, c, b, b, c, d);
    }
    vBase += poly.length * 2;
  }

  if (positions.length === 0) return;

  const geom = new THREE.BufferGeometry();
  geom.setAttribute('position',
    new THREE.BufferAttribute(new Float32Array(positions), 3));
  geom.setIndex(indices);
  geom.computeVertexNormals();

  const mat = new THREE.MeshLambertMaterial({
    color: 0x1f2937,           // dark slate
    side: THREE.DoubleSide,
  });
  _runwayMesh = new THREE.Mesh(geom, mat);
  _runwayMesh.frustumCulled = false;
  _rootGroup.add(_runwayMesh);
}

// ── Init / dispose ───────────────────────────────────────────────────────────

/**
 * Build the transport layer for the active city. Re-callable: disposes the
 * previous layer first. Pass `data = null` to dispose without rebuilding.
 *
 * @param {THREE.Scene} scene  the Three.js scene from view.getScene()
 * @param {object|null} data   {infra, schedule} from /api/transport, or null
 */
export function buildTransport(scene, data) {
  dispose();
  if (!scene || !data || !data.infra || !data.schedule) {
    return false;
  }
  _scene = scene;
  _infra = data.infra;
  _schedule = data.schedule;

  // Apply operator branding (colours) before building geometry
  _configureBranding(data.operators || []);

  _rootGroup = new THREE.Group();
  _rootGroup.visible = _visible;
  scene.add(_rootGroup);

  // Static landmarks first so vehicles render on top in case of overlap
  _buildRunwayStrips();
  _buildStaticStops();
  _buildInstancedMeshes(scene);
  _buildShadowMeshes();
  _buildWakeMesh();
  _buildMaskSprites();
  _buildBuildingGrid(data.buildings || null);
  _buildSelectionOutline('bus');

  // Index lines and stops by id for fast lookup
  _linesById.clear();
  for (const line of _schedule.lines || []) _linesById.set(line.id, line);
  _stopsByIdx.clear();
  for (const stop of _infra.stops || []) _stopsByIdx.set(stop.idx, stop);

  // Reset slot pool counters
  _persistentVehicles = [];
  for (const k of Object.keys(_slotPool)) _slotPool[k] = [];
  for (const k of Object.keys(_nextSlot)) _nextSlot[k] = 0;

  // Spawn persistent visualizers — one or more per line.
  // Track per-type line index so vehicles on lines that share an
  // identical polyline (e.g. inter-city trains all routed on the same
  // local rail polyline) get staggered starting offsets — otherwise
  // they all overlap at the start.
  const linesSeenByType = { bus: 0, tram: 0, train: 0, ferry: 0, plane: 0 };
  for (const line of _schedule.lines || []) {
    const baseType = _vehicleType({ vehicle_type: line.type });
    if (!MAX_INSTANCES[baseType]) continue;
    if (!line.polyline || line.polyline.length < 2) continue;

    // Cache polyline length on the polyline object
    _samplePolyline(line.polyline, 0);
    const totalLen = line.polyline._total || 1;

    // Precompute the distance along the polyline of each stop, used
    // by the dwell logic in update() to slow down and stop at stops.
    if (line.stop_polyline_indices && line.polyline._cum) {
      line._stopDistances = line.stop_polyline_indices.map(idx =>
        line.polyline._cum[Math.min(idx, line.polyline._cum.length - 1)] || 0);
    } else {
      line._stopDistances = [];
    }

    const count = PERSISTENT_PER_LINE[baseType] || 1;
    const spacing = totalLen / Math.max(count, 1);

    // Stagger by line index so multiple lines sharing a polyline don't
    // pile their vehicles on top of each other.
    const lineIdx = linesSeenByType[baseType]++;
    const lineOffset = (lineIdx * totalLen / 4) % totalLen;

    const cycle = PLANE_ARC_REAL_SEC + PLANE_IDLE_REAL_SEC;
    for (let i = 0; i < count; i++) {
      // Cycle through operator-driven variants. BUS_VARIANTS / TRAIN_VARIANTS
      // are trimmed by _configureBranding() to match the number of operators
      // with valid colours — 1 op → single colour, 2 → alternate, 3 → cycle.
      let type = baseType;
      if (baseType === 'bus') {
        type = BUS_VARIANTS[(lineIdx * count + i) % BUS_VARIANTS.length];
      } else if (baseType === 'train') {
        type = TRAIN_VARIANTS[(lineIdx * count + i) % TRAIN_VARIANTS.length];
      }

      const slot = _acquireSlot(type);
      if (slot < 0) break;

      // Phase planes evenly through the cycle so the user always sees
      // at least one in the air. With 3 planes per line, they're spaced
      // (cycle / 3) apart in time.
      let arcSec = 0;
      if (type === 'plane') {
        const perLineOffset = -lineIdx * (cycle / 4);
        const perVehicleOffset = -i * (cycle / count);
        arcSec = perLineOffset + perVehicleOffset;
      }
      // Buses and trams alternate direction (even=forward, odd=backward)
      // with lane offsets so opposing streams don't collide.
      // Trains always go forward — they follow a single track linearly
      // and wrap back to the start when they reach the end.
      const isTrainType = type.startsWith('train') || type === 'ferry';
      const direction = isTrainType ? 1 : ((i % 2 === 0) ? 1 : -1);
      _persistentVehicles.push({
        lineId:    line.id,
        type,
        slot,
        distance:  (spacing * i + lineOffset) % Math.max(totalLen, 1),
        direction,
        arcSec,
      });
    }
  }

  // Build the red infection rings now that _persistentVehicles is
  // populated — one ring per vehicle, hidden by default.
  _buildInfectionRings();

  return true;
}

export function dispose() {
  if (_rootGroup && _scene) {
    _scene.remove(_rootGroup);
  }
  for (const type of Object.keys(_instancedMeshes)) {
    const m = _instancedMeshes[type];
    if (m) {
      if (m.geometry) m.geometry.dispose();
      if (m.material) m.material.dispose();
    }
  }
  for (const type of Object.keys(_stopMeshes)) {
    const m = _stopMeshes[type];
    if (m) {
      if (m.geometry) m.geometry.dispose();
      if (m.material) m.material.dispose();
    }
  }
  if (_runwayMesh) {
    if (_runwayMesh.geometry) _runwayMesh.geometry.dispose();
    if (_runwayMesh.material) _runwayMesh.material.dispose();
  }
  for (const type of Object.keys(_shadowMeshes)) {
    const m = _shadowMeshes[type];
    if (m && m.geometry) m.geometry.dispose();
    // material is shared — disposed once below
  }
  if (_shadowTex) { _shadowTex.dispose(); _shadowTex = null; }
  if (_wakeMesh) {
    if (_wakeMesh.geometry) _wakeMesh.geometry.dispose();
    if (_wakeMesh.material) _wakeMesh.material.dispose();
    _wakeMesh = null;
  }
  for (const type of Object.keys(_maskMeshes)) {
    const m = _maskMeshes[type];
    if (m && m.geometry) m.geometry.dispose();
  }
  if (_maskTex) { _maskTex.dispose(); _maskTex = null; }
  _maskMeshes = {};
  _bGrid = null;
  _buildings = null;
  _latestCells = null;
  _instancedMeshes = {};
  _shadowMeshes = {};
  _stopMeshes = {};
  _runwayMesh = null;
  if (_selRingMesh) {
    if (_selRingMesh.geometry) _selRingMesh.geometry.dispose();
    if (_selRingMesh.material) _selRingMesh.material.dispose();
    _selRingMesh = null;
  }
  _hoveredVi = -1;
  _selectedVi = -1;
  _rootGroup = null;
  _scene = null;
  _infra = null;
  _schedule = null;
  _persistentVehicles = [];
}

export function setVisible(v) {
  _visible = !!v;
  if (_rootGroup) _rootGroup.visible = _visible;
}

// LOD: scale up vehicles when the camera pulls back. At close zoom
// (≤ LOD_NEAR_M visible across the viewport) vehicles render at their
// base size. As the camera zooms out beyond that, vehicles grow
// linearly with the visible-width ratio up to LOD_MAX_MULT × base —
// otherwise the 30-m bus footprint shrinks below a pixel at
// city-wide views and the entire fleet disappears.
const LOD_NEAR_M    = 1500;   // viewport width below this → no upscale
const LOD_FAR_M     = 12000;  // viewport width above this → max upscale
const LOD_MAX_MULT  = 4.0;    // hardest zoom-out → 4× base size
let   _lodMult      = 1.0;

export function setLOD(visWidth) {
  const w = +visWidth || 0;
  if (w <= LOD_NEAR_M) { _lodMult = 1.0; return; }
  if (w >= LOD_FAR_M)  { _lodMult = LOD_MAX_MULT; return; }
  const t = (w - LOD_NEAR_M) / (LOD_FAR_M - LOD_NEAR_M);
  _lodMult = 1.0 + t * (LOD_MAX_MULT - 1.0);
}

export function getLODMult() { return _lodMult; }

/** Store reference to latest SEIR cell data for infection warning sampling. */
export function setCells(cells) { _latestCells = cells; }

/** @deprecated — kept as no-op so callers don't break. */
export function setCameraQuat(_q) {}

/** Active intervention set — used for mask icon visibility. */
let _activeInterventions = new Set();
export function setInterventions(ivSet) {
  _activeInterventions = ivSet instanceof Set ? ivSet : new Set(ivSet || []);
}

// ── Transit infection ring (per-vehicle) ──────────────────────────────────
//
// Driven by state.transit_infections_by_pool — the ABM engine's 24
// synthetic commute pools, each carrying its own count of S→E
// transmissions that happened in that pool. We distribute the real
// vehicles across the 24 pools at build time by hashing veh.lineId so
// every vehicle on bus line 5 ends up in the same pool. Result: some
// vehicles are bright red (pool got hit hard), some are off (pool
// stayed clean), and the ring intensity grows independently per
// pool as the outbreak develops — closer to the per-line situational
// awareness the user asked for.
//
// Each vehicle has its own red rectangular ring mesh modelled after the
// yellow selection ring (_selRingMesh) — same outline geometry per
// type, same per-frame positioning + heading, but rendered for every
// vehicle and pulsed in red instead of yellow. Emissive on the body
// material was the first attempt; invisible because the liveries are
// MeshBasic/Standard with bright base colours that swamp the emissive.
// A dedicated outline mesh reads cleanly regardless of livery.
//
// `_transitInfectionLevel` is kept as a city-wide fallback for when
// state.transit_infections_by_pool isn't shipped (compartmental engine,
// older snapshots) — in that case every vehicle pulses together at the
// same level instead of having no ring at all.
const _COMMUTE_POOL_COUNT = 24;          // mirrors engine_abm._COMMUTE_POOL_COUNT
// Per-vehicle baseline (the "barely visible" thickness the user
// reported) and the cap for the worst-hit vehicle in its category.
// Multiplier on top of FRAME_THICKNESS — peak vehicles get 3× the
// baseline border. Tweak this single constant if buses should stand
// out even more dramatically against trains/trams.
const RING_BASELINE_THICKNESS  = 1.4 * VEHICLE_SCALE / 2.5;   // ≈ 1.4 m
const RING_MAX_THICKNESS_MULT  = 3.0;
const RING_THICKNESS_DELTA_M   = 0.25;    // only rebuild geo when target differs by this much
let _transitInfectionLevel    = 0;
let _transitInfectionsByPool  = null;    // number[24] or null
let _infectionRings           = [];      // parallel to _persistentVehicles
let _infectionRingGroup       = null;

function _hashStr(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

/**
 * Rebuild a vehicle ring's flat frame geometry at the given outer
 * thickness `t` (world metres on the post-scale plane). The inner edge
 * stays pinned to the vehicle outline; only the outer edge moves so
 * the border grows OUTWARD as infection intensity climbs.
 */
function _rebuildRingFrame(ring, baseType, thickness) {
  const dims = _VEH_DIMS[baseType] || _VEH_DIMS.bus;
  const hl = dims.l / 2 * VEHICLE_SCALE;
  const hw = dims.w / 2 * VEHICLE_SCALE;
  const ot = thickness;

  const outer = new THREE.Shape();
  outer.moveTo(-hw - ot, -hl - ot);
  outer.lineTo( hw + ot, -hl - ot);
  outer.lineTo( hw + ot,  hl + ot);
  outer.lineTo(-hw - ot,  hl + ot);
  outer.lineTo(-hw - ot, -hl - ot);
  const hole = new THREE.Path();
  hole.moveTo(-hw, -hl);
  hole.lineTo( hw, -hl);
  hole.lineTo( hw,  hl);
  hole.lineTo(-hw,  hl);
  hole.lineTo(-hw, -hl);
  outer.holes.push(hole);

  if (ring.geometry) ring.geometry.dispose();
  const geo = new THREE.ShapeGeometry(outer);
  geo.rotateX(-Math.PI / 2);
  ring.geometry = geo;
  ring._currentThickness = thickness;
}

function _buildInfectionRings() {
  // Wipe any previous rings (city switch / dispose).
  if (_infectionRingGroup) {
    _rootGroup.remove(_infectionRingGroup);
    for (const ring of _infectionRings) {
      if (ring) {
        ring.geometry.dispose();
        ring.material.dispose();
      }
    }
  }
  _infectionRings = [];
  _infectionRingGroup = new THREE.Group();
  _infectionRingGroup.renderOrder = 999;   // just under the yellow selection ring (1001)

  // Baseline frame thickness in scaled world metres. The minimum a
  // visible ring will ever be — used when a vehicle is in a pool with
  // any infection at all but isn't the worst-hit in its category.
  // Vehicles in the worst-hit pool of their category grow up to
  // BASE_FRAME_THICKNESS * RING_MAX_THICKNESS_MULT (see _updateInfectionRings).
  const FRAME_THICKNESS = 1.4 * VEHICLE_SCALE / 2.5;   // ≈ 1.4 m post-scale

  for (let i = 0; i < _persistentVehicles.length; i++) {
    const veh  = _persistentVehicles[i];
    const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
    const dims = _VEH_DIMS[base] || _VEH_DIMS.bus;
    const hl   = dims.l / 2 * VEHICLE_SCALE;
    const hw   = dims.w / 2 * VEHICLE_SCALE;
    const ot   = FRAME_THICKNESS;                    // outward extension

    // Build a 2D rectangular Shape with a rectangular hole — flat
    // ShapeGeometry rendered in the XZ plane. Reads as a chunky red
    // border around the vehicle outline, visible at any zoom and
    // unaffected by the WebGL "linewidth always 1px" limitation that
    // killed the earlier THREE.Line approach.
    const outer = new THREE.Shape();
    outer.moveTo(-hw - ot, -hl - ot);
    outer.lineTo( hw + ot, -hl - ot);
    outer.lineTo( hw + ot,  hl + ot);
    outer.lineTo(-hw - ot,  hl + ot);
    outer.lineTo(-hw - ot, -hl - ot);
    const hole = new THREE.Path();
    hole.moveTo(-hw, -hl);
    hole.lineTo( hw, -hl);
    hole.lineTo( hw,  hl);
    hole.lineTo(-hw,  hl);
    hole.lineTo(-hw, -hl);
    outer.holes.push(hole);

    const geo = new THREE.ShapeGeometry(outer);
    // ShapeGeometry emits XY-plane verts; rotate so the frame lies
    // flat in the XZ plane, matching the yellow selection ring.
    geo.rotateX(-Math.PI / 2);

    const mat = new THREE.MeshBasicMaterial({
      color: 0xef4444,
      transparent: false,
      opacity: 1,
      depthTest: false,
      side: THREE.DoubleSide,
    });
    const ring = new THREE.Mesh(geo, mat);
    ring.visible = false;
    _infectionRingGroup.add(ring);
    _infectionRings.push(ring);
    // Map this vehicle into one of the 24 synthetic commute pools by
    // hashing the line id, so every vehicle on the same line shares
    // the same pool. Stable across frames + reproducible across
    // city reloads.
    veh._poolIdx = _hashStr(String(veh.lineId || '')) % _COMMUTE_POOL_COUNT;
  }
  _rootGroup.add(_infectionRingGroup);
}

function _updateInfectionRings() {
  if (_infectionRings.length === 0 || !_visible) return;

  const byPool      = _transitInfectionsByPool;
  const hasPerPool  = Array.isArray(byPool) && byPool.length > 0;
  const cityTotal   = hasPerPool ? byPool.reduce((a, b) => a + (b | 0), 0) : 0;

  // Group visibility short-circuit: if both per-pool counts AND the
  // city-wide fallback level are zero, hide the whole group.
  if (cityTotal <= 0 && _transitInfectionLevel <= 0) {
    if (_infectionRingGroup && _infectionRingGroup.visible) {
      _infectionRingGroup.visible = false;
    }
    return;
  }
  if (_infectionRingGroup && !_infectionRingGroup.visible) {
    _infectionRingGroup.visible = true;
  }
  // Per-pool intensity threshold — 15 transmissions in a pool brings
  // its vehicles to the visibility threshold. The ring is now fully
  // opaque whenever it's drawn; outbreak severity is communicated
  // solely through the per-category thickness ranking below so the
  // border reads cleanly from any zoom level instead of fading into
  // the basemap. The earlier per-frame pulse was removed at the same
  // time — see also the material above (transparent:false, opacity:1).
  const POOL_SCALE = 15;

  // Per-category max pool count, computed once per update tick.
  // Used to map each vehicle's pool count to a 0..1 ranking within
  // its category so thickness only grows when a vehicle is among the
  // worst-hit of its kind. Without this, every vehicle on a busy
  // line would balloon together; here, a moderately-hit bus stays
  // close to baseline while the absolute worst-hit one swells to
  // RING_MAX_THICKNESS_MULT * baseline.
  const maxPerCat = {};
  if (hasPerPool) {
    for (let i = 0; i < _persistentVehicles.length; i++) {
      const veh = _persistentVehicles[i];
      const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
      const pc = byPool[veh._poolIdx] | 0;
      if (pc > (maxPerCat[base] || 0)) maxPerCat[base] = pc;
    }
  }

  for (let i = 0; i < _persistentVehicles.length; i++) {
    const veh  = _persistentVehicles[i];
    const ring = _infectionRings[i];
    if (!ring || veh._lastX === undefined) {
      if (ring) ring.visible = false;
      continue;
    }
    const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');

    // Per-vehicle level: use this vehicle's pool count when ABM is
    // shipping by-pool data, otherwise fall back to the city-wide
    // _transitInfectionLevel so vehicles still pulse together under
    // the compartmental engine (or pre-fix clients).
    let vehLevel;
    let poolCount = 0;
    if (hasPerPool && veh._poolIdx !== undefined) {
      poolCount = byPool[veh._poolIdx] | 0;
      vehLevel  = Math.min(1, poolCount / POOL_SCALE);
    } else {
      vehLevel = _transitInfectionLevel;
    }
    if (vehLevel <= 0) {
      ring.visible = false;
      continue;
    }

    // Thickness ratio: where does THIS vehicle's pool sit between
    // "uninfected" and "worst-in-its-category"? When ranking data
    // isn't available we fall back to vehLevel so the ring at least
    // grows with overall transit activity.
    let thicknessRatio;
    if (hasPerPool) {
      const maxInCat = maxPerCat[base] || 0;
      thicknessRatio = maxInCat > 0 ? (poolCount / maxInCat) : 0;
    } else {
      thicknessRatio = vehLevel;
    }
    const targetThickness =
      RING_BASELINE_THICKNESS *
      (1 + thicknessRatio * (RING_MAX_THICKNESS_MULT - 1));

    // Rebuild the frame geometry only when the desired thickness has
    // moved noticeably — every change costs an Earcut triangulation,
    // and the 0.25 m delta is below visual perception at typical
    // camera zoom.
    const curThickness = ring._currentThickness ?? 0;
    if (Math.abs(curThickness - targetThickness) > RING_THICKNESS_DELTA_M) {
      _rebuildRingFrame(ring, base, targetThickness);
    }

    const y = veh._lastY !== undefined ? veh._lastY + 0.15
            : veh.type === 'ferry'      ? 0.15
            : veh.type.startsWith('bus') ? 1.4
            : 0.95;
    ring.position.set(veh._lastX, y, veh._lastZ);
    if (veh._lastDirX !== undefined) {
      ring.rotation.y = Math.atan2(veh._lastDirX, veh._lastDirZ);
    }
    // Match the body's LOD upscale so the border tracks the vehicle
    // outline at every zoom level instead of sitting inside it.
    ring.scale.set(_lodMult, _lodMult, _lodMult);
    ring.visible          = true;
  }
}

export function setTransitInfectionLevel(level) {
  // Kept for back-compat / fallback when no by-pool array is supplied.
  _transitInfectionLevel = Math.min(1, Math.max(0, +level || 0));
}
export function setTransitInfectionsByPool(arr) {
  _transitInfectionsByPool = Array.isArray(arr) ? arr.slice() : null;
}
export function getTransitInfectionLevel() { return _transitInfectionLevel; }

export function isReady() {
  return _rootGroup !== null;
}

/** Return summary stats for the current city's transport network. */
export function getStats() {
  const stops = {};
  const lines = {};
  const trips = {};   // total scheduled trips per vehicle type (proxy for fleet size)
  if (_infra && _infra.stops) {
    for (const s of _infra.stops) {
      stops[s.type] = (stops[s.type] || 0) + 1;
    }
  }
  if (_schedule && _schedule.lines) {
    for (const l of _schedule.lines) {
      const t = _vehicleType({ vehicle_type: l.type });
      lines[t] = (lines[t] || 0) + 1;
    }
  }
  if (_schedule && Array.isArray(_schedule.trips)) {
    for (const tr of _schedule.trips) {
      const t = _vehicleType({ vehicle_type: tr.type || tr.vehicle_type });
      trips[t] = (trips[t] || 0) + 1;
    }
  }
  return { stops, lines, trips };
}

// ── Slot allocator ───────────────────────────────────────────────────────────

function _acquireSlot(type) {
  if (_slotPool[type].length > 0) return _slotPool[type].pop();
  if (_nextSlot[type] < MAX_INSTANCES[type]) {
    return _nextSlot[type]++;
  }
  return -1; // full
}

function _releaseSlot(type, slot) {
  _slotPool[type].push(slot);
  // Hide the released slot by zeroing its matrix (we adjust .count below)
  const mesh = _instancedMeshes[type];
  if (mesh) {
    _tmpMat.makeScale(0, 0, 0);
    mesh.setMatrixAt(slot, _tmpMat);
  }
}

// ── Polyline interpolation ───────────────────────────────────────────────────

/** Walk a polyline at parameter t∈[0,1] of TOTAL distance. Returns
 *  {x, z, dirX, dirZ} where dir is the unit tangent at that point. */
function _samplePolyline(polyline, t) {
  if (!polyline || polyline.length < 2) return null;
  // Cumulative lengths — compute lazily if not cached on the line
  if (!polyline._cum) {
    const cum = [0];
    for (let i = 1; i < polyline.length; i++) {
      const dx = polyline[i][0] - polyline[i - 1][0];
      const dz = polyline[i][1] - polyline[i - 1][1];
      cum.push(cum[i - 1] + Math.hypot(dx, dz));
    }
    polyline._cum = cum;
    polyline._total = cum[cum.length - 1];
  }
  const total = polyline._total;
  if (total <= 0) {
    return { x: polyline[0][0], z: polyline[0][1], dirX: 1, dirZ: 0 };
  }
  const target = Math.max(0, Math.min(1, t)) * total;
  const cum = polyline._cum;
  // Linear scan is fine for ≤200 points
  let i = 1;
  while (i < cum.length - 1 && cum[i] < target) i++;
  const segStart = cum[i - 1];
  const segLen = cum[i] - segStart;
  const localT = segLen > 0 ? (target - segStart) / segLen : 0;
  const ax = polyline[i - 1][0], az = polyline[i - 1][1];
  const bx = polyline[i][0],     bz = polyline[i][1];
  const x = ax + (bx - ax) * localT;
  const z = az + (bz - az) * localT;
  let dirX = bx - ax, dirZ = bz - az;
  const len = Math.hypot(dirX, dirZ);
  if (len > 1e-6) { dirX /= len; dirZ /= len; }
  else            { dirX = 1; dirZ = 0; }
  return { x, z, dirX, dirZ };
}

// ── Plane parametric arc ─────────────────────────────────────────────────────
//
// Phases of a flight, parameterised by p ∈ [0,1]:
//   [0.00, 0.10] taxi: ground, slow accel along runway
//   [0.10, 0.20] climb: lift to cruise altitude (eased)
//   [0.20, 0.80] cruise: level flight
//   [0.80, 0.90] descent: drop to ground (eased)
//   [0.90, 1.00] taxi to gate: ground, slow decel
//
// Pitch is computed from the altitude derivative.

// Multi-phase plane arc, parameterised by p ∈ [0, 1] of the arc cycle.
//
// Position, altitude and pitch are each defined by a list of keyframes
// at fixed p values; the helper _interpKeyframes does an eased linear
// interpolation between adjacent keyframes for smooth motion at every
// boundary.
//
// IMPORTANT: in Three.js with YXZ Euler order, applying a positive
// rotation around X to a vector at +Z (the plane's nose) tips it
// toward -Y — i.e. positive pitch = nose DOWN. So climb (nose up) uses
// NEGATIVE pitch values; descent (nose down) uses POSITIVE pitch.

function _interpKeyframes(points, p) {
  if (p <= points[0][0]) return points[0][1];
  for (let i = 1; i < points.length; i++) {
    if (p <= points[i][0]) {
      const t  = (p - points[i - 1][0]) / (points[i][0] - points[i - 1][0]);
      const u  = _easeInOut(Math.max(0, Math.min(1, t)));
      return points[i - 1][1] * (1 - u) + points[i][1] * u;
    }
  }
  return points[points.length - 1][1];
}

// Position progress along the polyline. Slow start (taxi accel),
// fast middle (cruise), slow end (landing decel) — pure eased S-curve.
const _PLANE_POS_KEYS = [
  [0.00, 0.000],
  [0.04, 0.005],   // taxi: accelerating from a standstill
  [0.10, 0.040],   // rotation + lift-off
  [0.18, 0.110],   // climb out
  [0.25, 0.200],   // reaching cruise speed
  [0.50, 0.500],   // mid-cruise (max horizontal speed)
  [0.75, 0.800],
  [0.85, 0.900],   // top of descent
  [0.92, 0.965],   // approach
  [0.97, 0.992],   // flare — slowing fast
  [1.00, 1.000],   // ground roll, stopped at gate
];

// Altitude in metres. Smooth bell curve.
const _PLANE_ALT_KEYS = (() => {
  const A = PLANE_CRUISE_ALT;
  return [
    [0.00, 0],
    [0.05, 0],
    [0.10, A * 0.10],
    [0.18, A * 0.85],
    [0.25, A],
    [0.75, A],
    [0.82, A * 0.75],
    [0.88, A * 0.40],
    [0.93, A * 0.15],
    [0.97, A * 0.04],
    [0.99, 0],
    [1.00, 0],
  ];
})();

// Pitch in radians (NEGATIVE = nose up, POSITIVE = nose down).
const _PLANE_PITCH_KEYS = [
  [0.00,  0.00],   // taxi: level
  [0.06,  0.00],
  [0.10, -0.42],   // rotation: nose UP sharply (~24°)
  [0.18, -0.22],   // climb out: still nose up but easing
  [0.25,  0.00],   // levelling off into cruise
  [0.75,  0.00],   // cruise
  [0.82,  0.10],   // descent: nose DOWN gently
  [0.90,  0.16],   // approach: steeper nose down
  [0.94,  0.16],   // hold
  [0.97, -0.10],   // flare: nose UP to bleed speed for touchdown
  [0.99,  0.00],   // touchdown: level
  [1.00,  0.00],
];

function _planeState(line, p) {
  const poly = line.polyline;
  if (!poly || poly.length < 2) return null;

  const lerpT = _interpKeyframes(_PLANE_POS_KEYS, p);
  const alt   = _interpKeyframes(_PLANE_ALT_KEYS, p);
  const pitch = _interpKeyframes(_PLANE_PITCH_KEYS, p);

  // Walk the actual polyline (not just first→last) so a 3-point
  // U-shape produces a real out-and-back arc.
  const samp = _samplePolyline(poly, lerpT);
  if (!samp) return null;

  return { x: samp.x, y: alt, z: samp.z,
           dirX: samp.dirX, dirZ: samp.dirZ, pitch };
}

function _easeInOut(u) { return u * u * (3 - 2 * u); }
function _easeIn(u)    { return u * u; }
function _easeOut(u)   { return 1 - (1 - u) * (1 - u); }

// ── Ferry wake / bow wave effect ─────────────────────────────────────────────
// A V-shaped wake trail behind each ferry, rendered as a flat instanced
// mesh sitting at water level. Each ferry slot gets one wake instance.

let _wakeMesh = null;
let _wakeCount = 0;

function _buildWakeGeometry() {
  // V-shaped wake: two angled flat quads trailing behind the bow
  const parts = [];
  const WAKE_LEN = 18;  // trail length behind ferry (scaled by VEHICLE_SCALE)
  const WAKE_W = 6;     // width of wake spread at the end

  // Left wake arm
  const left = new THREE.BufferGeometry();
  const lv = new Float32Array([
    0, 0, 0,                           // bow point
    -WAKE_W * 0.3, 0, -WAKE_LEN * 0.4, // mid-left
    -WAKE_W, 0, -WAKE_LEN,             // end-left
    0, 0, -WAKE_LEN * 0.3,             // inner mid
  ]);
  const li = new Uint16Array([0, 1, 3, 1, 2, 3]);
  left.setAttribute('position', new THREE.BufferAttribute(lv, 3));
  left.setIndex(new THREE.BufferAttribute(li, 1));
  left.computeVertexNormals();
  parts.push(left);

  // Right wake arm (mirrored)
  const right = new THREE.BufferGeometry();
  const rv = new Float32Array([
    0, 0, 0,
    WAKE_W * 0.3, 0, -WAKE_LEN * 0.4,
    WAKE_W, 0, -WAKE_LEN,
    0, 0, -WAKE_LEN * 0.3,
  ]);
  const ri = new Uint16Array([0, 3, 1, 1, 3, 2]);
  right.setAttribute('position', new THREE.BufferAttribute(rv, 3));
  right.setIndex(new THREE.BufferAttribute(ri, 1));
  right.computeVertexNormals();
  parts.push(right);

  // Bow splash (small triangle at the very front)
  const splash = new THREE.BufferGeometry();
  const sv = new Float32Array([
    0, 0.1, 1.5,      // tip (slightly above water)
    -1.5, 0, -1,
    1.5, 0, -1,
  ]);
  const si = new Uint16Array([0, 1, 2]);
  splash.setAttribute('position', new THREE.BufferAttribute(sv, 3));
  splash.setIndex(new THREE.BufferAttribute(si, 1));
  splash.computeVertexNormals();
  parts.push(splash);

  return _mergeGeoms(parts);
}

function _buildWakeMesh() {
  if (_wakeMesh) return;
  const geo = _buildWakeGeometry();
  geo.scale(VEHICLE_SCALE, VEHICLE_SCALE, VEHICLE_SCALE);
  const mat = new THREE.MeshBasicMaterial({
    color:       0xffffff,
    transparent: true,
    opacity:     0.35,
    depthWrite:  false,
    side:        THREE.DoubleSide,
    blending:    THREE.AdditiveBlending,
  });
  _wakeMesh = new THREE.InstancedMesh(geo, mat, MAX_INSTANCES['ferry'] || 32);
  _wakeMesh.count = 0;
  _wakeMesh.frustumCulled = false;
  _wakeMesh.renderOrder = 4;
  _rootGroup.add(_wakeMesh);
}

const _wakePos   = new THREE.Vector3();
const _wakeQuat  = new THREE.Quaternion();
const _wakeScale = new THREE.Vector3(1, 1, 1);
const _wakeMat4  = new THREE.Matrix4();
const _wakeEuler = new THREE.Euler();

function _updateWake(slot, x, y, z, dirX, dirZ) {
  if (!_wakeMesh || slot < 0) return;
  _wakePos.set(x, 0.2, z);    // just above ground level (water surface)
  const yaw = Math.atan2(dirX, dirZ);
  _wakeEuler.set(0, yaw, 0, 'YXZ');
  _wakeQuat.setFromEuler(_wakeEuler);
  // Track the ferry's LOD upscale so the wake trail grows with the hull.
  _wakeScale.set(_lodMult, _lodMult, _lodMult);
  _wakeMat4.compose(_wakePos, _wakeQuat, _wakeScale);
  _wakeMesh.setMatrixAt(slot, _wakeMat4);
}

// ── Per-frame matrix update ──────────────────────────────────────────────────

function _updateInstance(type, slot, x, y, z, dirX, dirZ, pitch = 0) {
  _tmpPos.set(x, y, z);
  // Yaw to face direction of travel (Three.js default forward is -Z, so the
  // body length lies on Z; rotate around Y so the +Z axis points into the
  // direction of motion).
  const yaw = Math.atan2(dirX, dirZ);
  _tmpEuler.set(pitch, yaw, 0, 'YXZ');
  _tmpQuat.setFromEuler(_tmpEuler);
  // LOD upscale — vehicles grow with the camera's visible-width so the
  // fleet stays legible at city-wide zoom. Planes already cruise at 600 m
  // altitude and look comically huge if we scale them too — give them
  // half the upscale factor.
  const m = (type === 'plane') ? (1.0 + (_lodMult - 1.0) * 0.5) : _lodMult;
  _tmpScale.set(m, m, m);
  _tmpMat.compose(_tmpPos, _tmpQuat, _tmpScale);
  _instancedMeshes[type].setMatrixAt(slot, _tmpMat);

  // Each ground-vehicle type also drives a parallel headlight instance.
  const hlKey = type + '_headlight';
  if (_instancedMeshes[hlKey]) {
    _instancedMeshes[hlKey].setMatrixAt(slot, _tmpMat);
  }

  // Drop shadow on the ground (skip for ferries — they're on water)
  if (type !== 'ferry') {
    _updateShadow(type, slot, x, z, dirX, dirZ);
  } else {
    _updateWake(slot, x, y, z, dirX, dirZ);
  }
}

// ── Main update loop ─────────────────────────────────────────────────────────

/**
 * Called from the ui.js animation frame. `flowHour` is the smooth frontend
 * clock (0..24); the dispatcher's epi coupling uses the backend clock so
 * the visual landing of a plane may lead/lag its infection deposit by a
 * frame or two — that's expected and invisible.
 *
 * `dtSec` is the real-time delta since the previous frame; `simSpeed` is
 * 0 when paused. Vehicle visual movement is driven by accumulated real
 * time, NOT by sim trip duration, so vehicles glide at a smooth fixed
 * speed regardless of how compressed the sim clock is.
 */
let _planeApronCount = {};   // reset each frame, tracks idle planes per line

export function update(flowHour, dtSec = 0, simSpeed = 0, isNight = false) {
  if (!_rootGroup || !_visible) return;

  const isRunning = simSpeed > 0;
  dtSec = Math.max(0, dtSec);
  _planeApronCount = {};   // reset per-frame idle counter
  _proxGrid = {};          // reset proximity grid for collision avoidance

  const masksOn = _activeInterventions.has('transit_masks');
  // Per-frame freeze flags driven by active interventions. Vehicles whose
  // mode is suspended hold their last position and orientation — same
  // visual feel as pausing the sim, but only for that mode.
  const allTransitStopped = _activeInterventions.has('lockdown')
                         || _activeInterventions.has('stop_transport');
  const planesGrounded    = allTransitStopped
                         || _activeInterventions.has('suspend_flights');

  // Walk every persistent visualizer at its fixed visual pace.
  for (let vi = 0; vi < _persistentVehicles.length; vi++) {
    const veh = _persistentVehicles[vi];
    const line = _linesById.get(veh.lineId);
    if (!line || !line.polyline) continue;

    if (veh.type === 'plane') {
      const planeMoving = isRunning && !planesGrounded;
      // Plane plays the full takeoff/cruise/landing arc, idles, then
      // restarts. Arc duration scales inversely with simSpeed so the
      // plane visually accelerates when the user cranks the slider —
      // matching how ground vehicles speed up. At slider 1× the arc
      // takes PLANE_ARC_REAL_SEC seconds; at slider 5× it takes 1/5
      // of that.
      const speedMult = Math.max(1, simSpeed);
      if (planeMoving) veh.arcSec += dtSec * speedMult;
      const cycle = PLANE_ARC_REAL_SEC + PLANE_IDLE_REAL_SEC;
      while (veh.arcSec > cycle) veh.arcSec -= cycle;

      const t = veh.arcSec < 0
        ? 0   // negative = staggered start, hold on the runway
        : Math.min(1, veh.arcSec / PLANE_ARC_REAL_SEC);

      const isIdle = (t === 0);

      // Track how many planes are idle at this line's airport so we
      // can spread up to 10 across the apron and hide the rest.
      // Group by airport origin (polyline start point).
      const p0 = line.polyline[0];
      const apronKey = `${p0[0]},${p0[1]}`;
      if (isIdle) {
        _planeApronCount[apronKey] = (_planeApronCount[apronKey] || 0) + 1;
        if (_planeApronCount[apronKey] > 10) {
          // Hide excess idle planes by placing them underground
          _updateInstance('plane', veh.slot, 0, -9999, 0, 1, 0, 0);
          continue;
        }
      }

      const st = _planeState(line, t);
      if (st) {
        if (isIdle) {
          // Spread idle planes around the airport apron in a row
          // perpendicular to the runway direction.
          const idx = (_planeApronCount[apronKey] || 1) - 1;
          const perpX = -st.dirZ;     // perpendicular to runway
          const perpZ =  st.dirX;
          const spacing = 45;         // metres between parked planes
          const offset = (idx - 4.5) * spacing;  // centre the row
          st.x += perpX * offset;
          st.z += perpZ * offset;
        }
        _updateInstance('plane', veh.slot, st.x, st.y, st.z,
                        st.dirX, st.dirZ, st.pitch);
        _updateMaskSprite('plane', veh.slot, st.x, st.z,
                          masksOn && (vi % 3 === 0));
        veh._lastX = st.x;
        veh._lastZ = st.z;
        veh._lastY = st.y;
        veh._lastDirX = st.dirX;
        veh._lastDirZ = st.dirZ;
      }
      continue;
    }

    // Ground vehicle: walk along the polyline at base speed × sim
    // slider value. Vehicles do NOT bounce — they wrap from end to
    // start invisibly. They also do NOT actually stop at stops, just
    // slow down passing nearby (the user said full stops looked stuck).
    const baseSpeed = VEHICLE_VISUAL_SPEED[veh.type] || 5;
    const totalLen = line.polyline._total || 1;
    const stopDistances = line._stopDistances || [];

    // Slowdown factor: 1 outside SLOW_RADIUS, drops linearly to
    // STOP_MIN_FACTOR as the vehicle approaches the nearest stop.
    let speedFactor = 1.0;
    if (stopDistances.length > 0) {
      let minDist = Infinity;
      for (const sd of stopDistances) {
        let d = Math.abs(sd - veh.distance);
        if (d > totalLen / 2) d = totalLen - d;   // wrap-aware
        if (d < minDist) minDist = d;
      }
      if (minDist < STOP_SLOW_RADIUS_M) {
        const u = minDist / STOP_SLOW_RADIUS_M;   // 0 at stop → 1 at edge
        speedFactor = STOP_MIN_FACTOR + (1 - STOP_MIN_FACTOR) * u;
      }
    }

    // Collision avoidance: if another vehicle of a compatible type is
    // close ahead on this line, slow down to maintain a gap.  Only
    // checked between same-direction vehicles on the same polyline.
    if (veh._lastX !== undefined) {
      const cx = Math.floor(veh._lastX / _PROX_CELL);
      const cz = Math.floor(veh._lastZ / _PROX_CELL);
      const baseType = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
      for (let dx = -1; dx <= 1; dx++) {
        for (let dz = -1; dz <= 1; dz++) {
          const others = _proxGrid[`${cx+dx},${cz+dz},${baseType}`];
          if (!others) continue;
          for (const oth of others) {
            if (oth === vi) continue;
            const ov = _persistentVehicles[oth];
            if (!ov || ov._lastX === undefined) continue;
            const ddx = ov._lastX - veh._lastX;
            const ddz = ov._lastZ - veh._lastZ;
            const dist2 = ddx * ddx + ddz * ddz;
            if (dist2 < _PROX_MIN_DIST2 && dist2 > 1) {
              // Only slow down if the other is AHEAD (dot product > 0)
              const dot = ddx * (veh._lastDirX || 0) + ddz * (veh._lastDirZ || 0);
              if (dot > 0) {
                const dist = Math.sqrt(dist2);
                const f = dist / _PROX_MIN_DIST;
                speedFactor *= (0.15 + 0.85 * f);  // ramp from 15% to 100%
              }
            }
          }
        }
      }
    }

    const speed = baseSpeed * Math.max(1, simSpeed) * speedFactor;
    // Ground vehicles freeze in place under lockdown / stop_transport.
    if (isRunning && !allTransitStopped) {
      veh.distance += dtSec * speed * veh.direction;
    }

    if (totalLen > 0) {
      veh.distance = ((veh.distance % totalLen) + totalLen) % totalLen;
    }

    const t = totalLen > 0 ? veh.distance / totalLen : 0;
    const samp = _samplePolyline(line.polyline, t);
    if (samp) {
      // Flip the tangent when walking backward so the vehicle nose
      // points the way it's going.
      const dirX = samp.dirX * veh.direction;
      const dirZ = samp.dirZ * veh.direction;

      // Lane offset: drive on the RIGHT side of the road relative to
      // the direction of motion. The right perpendicular of the motion
      // tangent (dirX, dirZ) is (-dirZ, dirX). Without this, vehicles
      // moving in opposite directions on the same line collide head-on
      // along the centerline. Trains stay centered (single track).
      let posX = samp.x;
      let posZ = samp.z;
      const isBus = veh.type.startsWith('bus');
      if (isBus || veh.type === 'tram') {
        posX += -dirZ * LANE_OFFSET;
        posZ +=  dirX * LANE_OFFSET;
      }

      // Store position for next-frame proximity check
      veh._lastX = posX;
      veh._lastZ = posZ;
      veh._lastDirX = dirX;
      veh._lastDirZ = dirZ;
      // Register in proximity grid
      const baseType = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
      const gk = `${Math.floor(posX / _PROX_CELL)},${Math.floor(posZ / _PROX_CELL)},${baseType}`;
      if (!_proxGrid[gk]) _proxGrid[gk] = [];
      _proxGrid[gk].push(vi);

      const y = veh.type === 'ferry' ? -0.5 : isBus ? 1.5 : 1.0;
      _updateInstance(veh.type, veh.slot, posX, y, posZ, dirX, dirZ, 0);

      _updateMaskSprite(veh.type, veh.slot, posX, posZ,
                        masksOn && (vi % 3 === 0));
    }
  }

  // Push instance counts + matrix updates. Headlight meshes share
  // slot space with their parent vehicle type and inherit its count.
  // Visibility is controlled by isNight here.
  for (const type of Object.keys(_instancedMeshes)) {
    const mesh = _instancedMeshes[type];
    if (!mesh) continue;
    // Headlight types end with '_headlight' — parent is everything before.
    if (type.endsWith('_headlight')) {
      const parent = type.slice(0, -'_headlight'.length);
      mesh.count = _nextSlot[parent] || 0;
      mesh.visible = isNight;
    } else {
      mesh.count = _nextSlot[type] || 0;
    }
    mesh.instanceMatrix.needsUpdate = true;
  }

  // Shadow instance counts mirror their vehicle type
  for (const type of Object.keys(_shadowMeshes)) {
    const sm = _shadowMeshes[type];
    if (!sm) continue;
    sm.count = _nextSlot[type] || 0;
    sm.instanceMatrix.needsUpdate = true;
  }

  // Ferry wake instance count mirrors ferry vehicles
  if (_wakeMesh) {
    _wakeMesh.count = _nextSlot['ferry'] || 0;
    _wakeMesh.instanceMatrix.needsUpdate = true;
  }

  // Mask sprites — counts + visibility toggle
  for (const type of Object.keys(_maskMeshes)) {
    const mm = _maskMeshes[type];
    if (!mm) continue;
    mm.visible = masksOn;
    mm.count = _nextSlot[type] || 0;
    mm.instanceMatrix.needsUpdate = true;
  }

  // Yellow selection ring follows the selected vehicle
  _updateSelectionRing();
  // Red transit-infection rings follow every vehicle (city-wide intensity)
  _updateInfectionRings();
}

function _vehicleType(trip) {
  const t = trip.vehicle_type || 'bus';
  if (t === 'bus' || t === 'tram' || t === 'train' || t === 'ferry' || t === 'plane') return t;
  // Map intercity_train + flight aliases
  if (t === 'flight') return 'plane';
  if (t === 'intercity_train') return 'train';
  return 'bus';
}

// ── Vehicle hover / selection ───────────────────────────────────────────────

// Bounding-box dimensions per base type (pre-scale metres, matching geometry).
// Used for both oriented-rectangle hit-testing and the selection outline.
const _VEH_DIMS = {
  bus:   { l: 10,  w: 3   },
  tram:  { l: 25,  w: 3   },
  train: { l: 30,  w: 3.5 },
  ferry: { l: 29,  w: 6   },
  plane: { l: 32,  w: 9   },
};

/**
 * Project a world position to viewport CSS pixels using the MVP matrix
 * and the canvas bounding rect (accounts for canvas offset + DPR).
 * Returns { sx, sy } or null if behind camera.
 */
function _worldToViewport(wx, wy, wz, mvp, rect) {
  const v = new THREE.Vector4(wx, wy, wz, 1).applyMatrix4(mvp);
  if (v.w <= 0) return null;  // behind camera
  const ndcX = v.x / v.w;
  const ndcY = v.y / v.w;
  return {
    sx: ( ndcX * 0.5 + 0.5) * rect.width  + rect.left,
    sy: (-ndcY * 0.5 + 0.5) * rect.height + rect.top,
  };
}

function _vehY(type) {
  if (type === 'ferry') return -0.5;
  if (type.startsWith('bus')) return 1.5;
  return 1.0;
}

function _baseType(type) {
  return type.replace(/_[bc]$/, '').replace('_oresund', '');
}

/**
 * Oriented-bounding-box hit-test in world space.  Projects the cursor
 * to a ground-plane ray, then checks if it lands inside the vehicle's
 * oriented rectangle (half-lengths scaled by VEHICLE_SCALE).
 *
 * Cheaper alternative: project the vehicle's 4 OBB corners to screen
 * and test point-in-quad.  But the world-space OBB test is simpler
 * and works well because the ground plane is flat.
 *
 * Returns the vehicle index into _persistentVehicles, or -1.
 */
export function hitTest(mouseX, mouseY, mvpMatrix, rect) {
  if (!_rootGroup || !_visible || !mvpMatrix || !rect) return -1;

  // Unproject cursor to world-space ray via inverse MVP
  const ndcX =  ((mouseX - rect.left) / rect.width)  * 2 - 1;
  const ndcY = -((mouseY - rect.top)  / rect.height) * 2 + 1;
  const inv = mvpMatrix.clone().invert();
  const near = new THREE.Vector3(ndcX, ndcY, -1).applyMatrix4(inv);
  const far  = new THREE.Vector3(ndcX, ndcY,  1).applyMatrix4(inv);
  const dir  = new THREE.Vector3().subVectors(far, near).normalize();

  // For each vehicle, intersect the ray with the vehicle's Y-plane,
  // then test if the hit point falls inside the oriented bounding box.
  let bestVi = -1;
  let bestD2 = Infinity;
  for (let vi = 0; vi < _persistentVehicles.length; vi++) {
    const veh = _persistentVehicles[vi];
    if (veh._lastX === undefined || veh._lastDirX === undefined) continue;

    const y = veh._lastY !== undefined ? veh._lastY : _vehY(veh.type);
    // Ray–plane intersection: near.y + t * dir.y = y
    if (Math.abs(dir.y) < 1e-6) continue;  // ray parallel to plane
    const t = (y - near.y) / dir.y;
    if (t < 0) continue;  // behind camera
    const hx = near.x + t * dir.x;
    const hz = near.z + t * dir.z;

    // Vector from vehicle center to hit point
    const dx = hx - veh._lastX;
    const dz = hz - veh._lastZ;

    // Vehicle's local axes (forward = dir, right = perp)
    const fwdX = veh._lastDirX, fwdZ = veh._lastDirZ;
    const rgtX = -fwdZ,         rgtZ =  fwdX;

    // Project onto local axes
    const along = dx * fwdX + dz * fwdZ;  // forward axis
    const across = dx * rgtX + dz * rgtZ; // right axis

    const base = _baseType(veh.type);
    const dims = _VEH_DIMS[base] || _VEH_DIMS.bus;
    const halfL = dims.l / 2 * VEHICLE_SCALE;
    const halfW = dims.w / 2 * VEHICLE_SCALE;

    if (Math.abs(along) <= halfL && Math.abs(across) <= halfW) {
      // Inside the OBB — pick the closest to center
      const d2 = dx * dx + dz * dz;
      if (d2 < bestD2) { bestD2 = d2; bestVi = vi; }
    }
  }
  return bestVi;
}

/** Resolve the most likely operator name for a vehicle's type variant. */
function _operatorForVehicle(veh) {
  if (!_operators.length) return null;
  const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
  const ops = _operators.filter(o => (o.modes || []).includes(base));
  if (!ops.length) return null;
  const validOps = ops.filter(o => _hexToInt(o.colour) !== null);
  if (veh.type.endsWith('_b') && validOps.length >= 2) return validOps[1];
  if (veh.type.endsWith('_c') && validOps.length >= 3) return validOps[2];
  return validOps[0] || ops[0];
}

/**
 * Return a display-data object for vehicle index `vi`, or null.
 */
export function getVehicleInfo(vi) {
  if (vi < 0 || vi >= _persistentVehicles.length) return null;
  const veh = _persistentVehicles[vi];
  const line = _linesById.get(veh.lineId);
  const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
  const op = _operatorForVehicle(veh);

  // Extract route number from line id  e.g. "bus_stockholm_42" → "42"
  let routeNum = '';
  if (line) {
    const parts = line.id.split('_');
    routeNum = parts[parts.length - 1];
  }

  // This vehicle's transit-pool transmission count — the ABM tracks
  // 24 city-wide pools and we hashed each line into one of them at
  // build time, so this number reflects "transmissions on this
  // route's commute-pool". Falls back to 0 if by-pool data isn't
  // available (compartmental engine).
  const poolIdx = veh._poolIdx;
  const poolInfections = (poolIdx !== undefined && Array.isArray(_transitInfectionsByPool))
    ? (_transitInfectionsByPool[poolIdx] | 0)
    : 0;

  return {
    vi,
    baseType:  base,
    routeNum,
    operator:  op ? op.name : null,
    capacity:  _CAPACITY[base] || '?',
    stops:     line ? (line.stops?.length || 0) : 0,
    isIntercity: line?.is_intercity || false,
    destination: line?.destination_name || null,
    poolIdx,
    poolInfections,
  };
}

/** Get viewport-space position for a vehicle (for tooltip tracking). */
export function getVehicleScreenPos(vi, mvpMatrix, rect) {
  if (vi < 0 || vi >= _persistentVehicles.length || !mvpMatrix || !rect) return null;
  const veh = _persistentVehicles[vi];
  if (veh._lastX === undefined) return null;
  const y = veh._lastY !== undefined ? veh._lastY : _vehY(veh.type);
  return _worldToViewport(veh._lastX, y, veh._lastZ, mvpMatrix, rect);
}

// ── Selection outline (yellow rectangle matching the vehicle footprint) ────

let _selOutlineType = '';  // base type of the current outline geometry

function _buildSelectionOutline(baseType) {
  if (_selRingMesh) {
    _rootGroup.remove(_selRingMesh);
    _selRingMesh.geometry.dispose();
    _selRingMesh.material.dispose();
    _selRingMesh = null;
  }
  const dims = _VEH_DIMS[baseType] || _VEH_DIMS.bus;
  const hl = dims.l / 2 * VEHICLE_SCALE;
  const hw = dims.w / 2 * VEHICLE_SCALE;

  // Simple rectangle: 4 corners closed loop
  const pts = [
    new THREE.Vector3( hw, 0, -hl),
    new THREE.Vector3( hw, 0,  hl),
    new THREE.Vector3(-hw, 0,  hl),
    new THREE.Vector3(-hw, 0, -hl),
    new THREE.Vector3( hw, 0, -hl),  // close
  ];

  const geo = new THREE.BufferGeometry().setFromPoints(pts);
  const mat = new THREE.LineBasicMaterial({
    color: 0xfbbf24, transparent: true, opacity: 0.95,
    depthTest: false,
  });
  _selRingMesh = new THREE.Line(geo, mat);
  _selRingMesh.renderOrder = 1001;
  _selRingMesh.visible = false;
  _rootGroup.add(_selRingMesh);
  _selOutlineType = baseType;
}

/** Update the yellow outline position + rotation + pulse each frame. */
function _updateSelectionRing() {
  if (!_selRingMesh) return;
  if (_selectedVi < 0 || _selectedVi >= _persistentVehicles.length) {
    _selRingMesh.visible = false;
    return;
  }
  const veh = _persistentVehicles[_selectedVi];
  if (veh._lastX === undefined) { _selRingMesh.visible = false; return; }

  // Rebuild outline geometry if the selected vehicle's type changed
  const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
  if (base !== _selOutlineType) _buildSelectionOutline(base);

  const y = veh._lastY !== undefined ? veh._lastY + 0.2
          : veh.type === 'ferry' ? 0.2
          : veh.type.startsWith('bus') ? 1.5 : 1.0;
  _selRingMesh.position.set(veh._lastX, y, veh._lastZ);

  // Orient with vehicle heading
  if (veh._lastDirX !== undefined) {
    const yaw = Math.atan2(veh._lastDirX, veh._lastDirZ);
    _selRingMesh.rotation.y = yaw;
  }

  // Pulse opacity
  const phase = 0.5 + 0.5 * Math.sin(Date.now() / 260);
  _selRingMesh.material.opacity = 0.55 + 0.45 * phase;
  _selRingMesh.visible = true;
}

export function setHoveredVehicle(vi) { _hoveredVi = vi; }

export function setSelectedVehicle(vi) {
  if (vi >= 0 && vi < _persistentVehicles.length) {
    const veh = _persistentVehicles[vi];
    const base = veh.type.replace(/_[bc]$/, '').replace('_oresund', '');
    if (base !== _selOutlineType) _buildSelectionOutline(base);
  }
  _selectedVi = vi;
  if (_selRingMesh) _selRingMesh.visible = vi >= 0;
}

export function getSelectedVehicle() { return _selectedVi; }
export function getHoveredVehicle() { return _hoveredVi; }
