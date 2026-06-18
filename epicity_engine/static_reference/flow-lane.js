/**
 * flow-lane.js — Road-ribbon flow visualization for high-speed mode.
 *
 * Shown when simSpeed >= TRAIL_SPEED_THRESHOLD (imported from flow-field.js).
 * Replaces particles with animated bidirectional flow ribbons that follow
 * the city's road grid — inspired by traffic flow density visualizations.
 *
 * Each road segment between two intersections gets TWO ribbon lanes:
 *   - Forward lane: flows toward the city centre (morning rush)
 *   - Return  lane: flows away from city centre (reversed at evening rush)
 *
 * The lanes use a custom GLSL shader that produces a sawtooth "streak"
 * animation (bright front, fading tail) scaled by phase intensity so
 * rush hours glow brightly and night is barely visible.
 *
 * Lane color is the SEIR-weighted blend of adjacent non-road zone states:
 *   green=S  yellow=E  red=I  blue=R
 *
 * When simSpeed === 0 (paused) the function returns immediately so the
 * last rendered frame stays perfectly frozen.
 */

import * as THREE from 'three';

const GRID   = 20;
const ROAD_Y = 0.005;

// Vivid SEIR colors — must match flow-field.js
const COL_S = [0.00, 1.00, 0.42];
const COL_E = [1.00, 0.93, 0.00];
const COL_I = [1.00, 0.08, 0.15];
const COL_R = [0.00, 0.53, 1.00];

const PHASES = [
  { startH:  0, endH:  6, dir:  1, intensity: 0.05 },
  { startH:  6, endH:  9, dir:  1, intensity: 1.00 },
  { startH:  9, endH: 17, dir:  1, intensity: 0.18 },
  { startH: 17, endH: 20, dir: -1, intensity: 0.92 },
  { startH: 20, endH: 24, dir:  1, intensity: 0.05 },
];
function _phase(h) {
  for (const p of PHASES) if (h >= p.startH && h < p.endH) return p;
  return PHASES[0];
}

// Intersection x positions in world coordinates (road cell centre = index + 0.5)
// Roads at grid index 0,5,10,15 → world centres 0.5,5.5,10.5,15.5
// Far boundary at 19.5 (last cell centre)
const SEG_X = [0.5, 5.5, 10.5, 15.5, 19.5];
const SEG_Z = [0.5, 5.5, 10.5, 15.5, 19.5];

const ROAD_ROWS = [0.5, 5.5, 10.5, 15.5];   // world-z of horizontal roads
const ROAD_COLS = [0.5, 5.5, 10.5, 15.5];   // world-x of vertical roads

const CCX = 10.0, CCZ = 10.0;   // approximate city centre
const LANE_HW  = 0.18;           // half-width of each lane ribbon
const LANE_OFF = 0.14;           // lane-centre offset from road centre

// ── GLSL shaders ─────────────────────────────────────────────────────────────

const VERT = /* glsl */`
varying vec2 vUv;
void main() {
  vUv = uv;
  gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
}`;

const FRAG = /* glsl */`
uniform float uTime;
uniform float uDir;
uniform vec3  uColor;
uniform float uBright;

varying vec2 vUv;

void main() {
  // ── Edge glow: zero at ribbon edges, full at centre ──────────────────────
  float edge = smoothstep(0.0, 0.20, vUv.y) * smoothstep(1.0, 0.80, vUv.y);
  edge = pow(edge, 0.70);

  // ── Directional streak along ribbon (uv.x = along-road axis) ─────────────
  // sawtooth: bright front, gradual fade behind → looks like moving cars/light
  float f      = fract(vUv.x * 4.5 - uDir * uTime * 0.90);
  float streak = pow(1.0 - f, 2.0);

  // secondary shimmer for richer look
  float shimmer = 0.5 + 0.5 * sin(vUv.x * 30.0 - uDir * uTime * 9.0);
  float pattern = streak * 0.78 + shimmer * 0.22;

  float alpha = edge * pattern * uBright * 0.90;
  if (alpha < 0.007) discard;

  // Tip brightening
  gl_FragColor = vec4(uColor * (0.50 + pattern * 0.50), alpha);
}`;

// ── Geometry helper ───────────────────────────────────────────────────────────

/**
 * Build a flat 2-triangle quad.
 *
 * The UV convention (critical for the shader):
 *   uv.x goes from p0→p1 (along-road direction)
 *   uv.y goes from p0→p2 (across-road direction)
 *
 * All p are [worldX, worldZ] pairs; Y is ROAD_Y.
 */
function _quad(p0, p1, p2, p3) {
  const Y = ROAD_Y;
  const pos = new Float32Array([
    p0[0],Y,p0[1],  p1[0],Y,p1[1],  p3[0],Y,p3[1],   // tri A
    p0[0],Y,p0[1],  p3[0],Y,p3[1],  p2[0],Y,p2[1],   // tri B
  ]);
  const uvs = new Float32Array([
    0,0, 1,0, 1,1,
    0,0, 1,1, 0,1,
  ]);
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.BufferAttribute(pos, 3));
  g.setAttribute('uv',       new THREE.BufferAttribute(uvs, 2));
  return g;
}

// ── FlowLanes ─────────────────────────────────────────────────────────────────

export class FlowLanes {
  constructor(scene) {
    this._scene   = scene;
    this._lanes   = [];     // {mesh, uniforms, cx, cz, baseDir}
    this._isRoad  = null;
    this._time    = 0;
    this._visible = false;
  }

  // ── Build ──────────────────────────────────────────────────────────────────

  build(layout) {
    this._isRoad = new Uint8Array(GRID * GRID);
    for (const c of layout.cells) {
      if (c.zone === 0) this._isRoad[c.y * GRID + c.x] = 1;
    }

    // ── Horizontal road ribbons (run along x, per road row) ──
    for (const rz of ROAD_ROWS) {
      for (let i = 0; i < SEG_X.length - 1; i++) {
        const xa = SEG_X[i], xb = SEG_X[i + 1];
        const cx = (xa + xb) / 2;
        // baseDir: +1 = flowing right (toward centre for left-side segments)
        const bDir = Math.sign(CCX - cx) || 1;

        // Forward lane (south side of road, lower z offset)
        this._addLane(
          [xa, rz - LANE_OFF - LANE_HW], [xb, rz - LANE_OFF - LANE_HW],
          [xa, rz - LANE_OFF + LANE_HW], [xb, rz - LANE_OFF + LANE_HW],
          cx, rz,  bDir
        );
        // Return lane (north side)
        this._addLane(
          [xa, rz + LANE_OFF - LANE_HW], [xb, rz + LANE_OFF - LANE_HW],
          [xa, rz + LANE_OFF + LANE_HW], [xb, rz + LANE_OFF + LANE_HW],
          cx, rz, -bDir
        );
      }
    }

    // ── Vertical road ribbons (run along z, per road column) ──
    for (const rx of ROAD_COLS) {
      for (let i = 0; i < SEG_Z.length - 1; i++) {
        const za = SEG_Z[i], zb = SEG_Z[i + 1];
        const cz = (za + zb) / 2;
        // baseDir: +1 = flowing toward +z (toward centre for top-half segments)
        const bDir = Math.sign(CCZ - cz) || 1;

        // For vertical roads: uv.x goes along z (p0→p1 = za→zb)
        // Forward lane (west side of road, lower x offset)
        this._addLane(
          [rx - LANE_OFF - LANE_HW, za], [rx - LANE_OFF - LANE_HW, zb],
          [rx - LANE_OFF + LANE_HW, za], [rx - LANE_OFF + LANE_HW, zb],
          rx, cz,  bDir
        );
        // Return lane (east side)
        this._addLane(
          [rx + LANE_OFF - LANE_HW, za], [rx + LANE_OFF - LANE_HW, zb],
          [rx + LANE_OFF + LANE_HW, za], [rx + LANE_OFF + LANE_HW, zb],
          rx, cz, -bDir
        );
      }
    }
  }

  /**
   * Add one lane ribbon mesh.
   * p0 = start/near  →  uv(0,0)
   * p1 = end/near    →  uv(1,0)
   * p2 = start/far   →  uv(0,1)
   * p3 = end/far     →  uv(1,1)
   */
  _addLane(p0, p1, p2, p3, cx, cz, baseDir) {
    const geo = _quad(p0, p1, p2, p3);
    const uniforms = {
      uTime:   { value: 0 },
      uDir:    { value: baseDir },
      uColor:  { value: new THREE.Color(...COL_S) },
      uBright: { value: 0.5 },
    };
    const mat = new THREE.ShaderMaterial({
      uniforms, vertexShader: VERT, fragmentShader: FRAG,
      transparent: true, depthWrite: false,
      blending: THREE.AdditiveBlending, side: THREE.DoubleSide,
    });
    const mesh = new THREE.Mesh(geo, mat);
    mesh.visible     = false;
    mesh.renderOrder = 3;
    this._scene.add(mesh);
    this._lanes.push({ mesh, uniforms, cx, cz, baseDir });
  }

  // ── Update ────────────────────────────────────────────────────────────────

  /**
   * @param {Array|null} cells    — SEIR cell array
   * @param {number}     dtSec   — real elapsed seconds
   * @param {number}     simSpeed — 0 = paused (freeze)
   * @param {number}     flowHour — 0-24 simulated hour
   */
  update(cells, dtSec, simSpeed, flowHour) {
    if (!this._lanes.length) return;

    // Freeze when paused — lanes stay exactly as rendered
    if (simSpeed === 0) return;

    this._time += dtSec * Math.min(simSpeed, 28) * 0.32;
    const ph = _phase(flowHour);

    for (const lane of this._lanes) {
      // Actual flow direction = base (toward centre) × phase direction
      lane.uniforms.uTime.value   = this._time;
      lane.uniforms.uDir.value    = lane.baseDir * ph.dir;
      lane.uniforms.uBright.value = ph.intensity;
      const [r, g, b] = this._color(lane.cx, lane.cz, cells);
      lane.uniforms.uColor.value.setRGB(r, g, b);
    }
  }

  // ── Colour sampling ────────────────────────────────────────────────────────

  _color(cx, cz, cells) {
    if (!cells) return COL_S;
    const rx = Math.floor(cx), rz = Math.floor(cz);
    let S=0,E=0,I=0,R=0,N=0;
    for (const [dx,dz] of [[-1,-1],[0,-1],[1,-1],[-1,0],[1,0],[-1,1],[0,1],[1,1]]) {
      const nx=rx+dx, nz=rz+dz;
      if (nx<0||nx>=GRID||nz<0||nz>=GRID) continue;
      if (this._isRoad[nz*GRID+nx]) continue;
      const c = cells[nz*GRID+nx];
      if (c&&c.N>0) { S+=c.S; E+=c.E; I+=c.I; R+=c.R; N+=c.N; }
    }
    if (N<=0) return COL_S;
    const sF=S/N,eF=E/N,iF=I/N,rF=R/N;
    return [
      sF*COL_S[0]+eF*COL_E[0]+iF*COL_I[0]+rF*COL_R[0],
      sF*COL_S[1]+eF*COL_E[1]+iF*COL_I[1]+rF*COL_R[1],
      sF*COL_S[2]+eF*COL_E[2]+iF*COL_I[2]+rF*COL_R[2],
    ];
  }

  // ── Public interface ───────────────────────────────────────────────────────

  setVisible(bool) {
    this._visible = bool;
    for (const l of this._lanes) l.mesh.visible = bool;
  }

  dispose() {
    for (const l of this._lanes) {
      this._scene.remove(l.mesh);
      l.mesh.geometry.dispose();
      l.mesh.material.dispose();
    }
    this._lanes = [];
  }
}
