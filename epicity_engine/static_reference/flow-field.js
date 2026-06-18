/**
 * flow-field.js — Particle flow along OSM road polylines.
 *
 * Visual modes (switched by simSpeed):
 *   < TRAIL_SPEED_THRESHOLD  → circular dots, SEIR-colored
 *   >= TRAIL_SPEED_THRESHOLD → directional line streaks, SEIR-colored
 *
 * Particles are spawned weighted by adjacent building population so
 * high-density areas generate more traffic.  Color is a SEIR-fraction
 * blend sampled from buildings within SEG_SEARCH_R metres of the segment.
 */

import * as THREE from 'three';
import { LineSegments2 }       from 'three/addons/lines/LineSegments2.js';
import { LineSegmentsGeometry } from 'three/addons/lines/LineSegmentsGeometry.js';
import { LineMaterial }        from 'three/addons/lines/LineMaterial.js';

export const TRAIL_SPEED_THRESHOLD = 3;

const PARTICLE_COUNT = 2000;
const ALTITUDE       = 1.5;    // metres above ground
const SEG_SEARCH_R   = 150.0;  // m — radius to find nearby buildings per segment
const TRAIL_LEN_MAX  = 35.0;   // m — maximum trail length at high speed
const DOT_SIZE       = 6;      // pixel size of circular dots
const TRAIL_WIDTH    = 2;      // pixel width of trail lines

// SEIR palette — green / yellow / red / blue
const COL_S = [0.00, 1.00, 0.42];
const COL_E = [1.00, 0.93, 0.00];
const COL_I = [1.00, 0.08, 0.15];
const COL_R = [0.00, 0.53, 1.00];

// ── Circle texture ────────────────────────────────────────────────────────────

function _makeCircleTexture() {
  const size = 64;
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = size;
  const ctx = canvas.getContext('2d');
  const r = size / 2;
  // soft radial gradient so the disc has a slightly glowing edge
  const grad = ctx.createRadialGradient(r, r, 0, r, r, r - 1);
  grad.addColorStop(0.0, 'rgba(255,255,255,1.0)');
  grad.addColorStop(0.6, 'rgba(255,255,255,0.95)');
  grad.addColorStop(1.0, 'rgba(255,255,255,0.0)');
  ctx.beginPath();
  ctx.arc(r, r, r - 1, 0, Math.PI * 2);
  ctx.fillStyle = grad;
  ctx.fill();
  const tex = new THREE.CanvasTexture(canvas);
  return tex;
}

// ── FlowField ─────────────────────────────────────────────────────────────────

export class FlowField {
  constructor(scene) {
    this.scene        = scene;
    this._dots        = null;   // THREE.Points  (low speed)
    this._trails      = null;   // THREE.LineSegments (high speed)
    this._segments    = [];
    this._segBuildings = [];    // [segIdx] → [buildingArrayPos, ...]
    this._cumWeights  = null;   // Float32Array — cumulative population weights
    this._totalWeight = 0;
    this._state       = [];
    this._visible     = false;
    this._useTrails   = false;
    this._bldgSegs    = [];     // [buildingIdx] → [segIdx, ...]  (reverse index)
    this._filterSegs  = null;   // Array<segIdx> when restricted to a building
    // Intervention effects: lockdown removes traffic entirely, social
    // distancing keeps a fraction of particles on the road. Drives both the
    // overall flow visualisation and the per-building selected-flow view.
    this._lockdown        = false;
    this._distanceFactor  = 1.0;   // 1.0 = full traffic, 0.3 = thinned
  }

  // ── Build ──────────────────────────────────────────────────────────────────

  build(layout) {
    if (!this.scene) return;
    this._disposeGeometry();

    // 1. Flatten road polylines → individual segments
    this._segments = [];
    for (const road of layout.roads || []) {
      for (let i = 0; i < road.points.length - 1; i++) {
        const [x0, z0] = road.points[i];
        const [x1, z1] = road.points[i + 1];
        const len = Math.hypot(x1 - x0, z1 - z0);
        if (len < 0.5) continue;
        this._segments.push({ x0, z0, x1, z1, len });
      }
    }
    if (this._segments.length === 0) return;

    // 2. Spatial grid of buildings for fast radius lookup
    const buildings = layout.buildings || [];
    const R2        = SEG_SEARCH_R * SEG_SEARCH_R;
    const GCELL     = SEG_SEARCH_R;
    const bGrid     = new Map();  // "gx,gz" → [buildingArrayIndex, ...]

    for (let bi = 0; bi < buildings.length; bi++) {
      const b   = buildings[bi];
      const key = `${Math.floor(b.world_x / GCELL)},${Math.floor(b.world_z / GCELL)}`;
      if (!bGrid.has(key)) bGrid.set(key, []);
      bGrid.get(key).push(bi);
    }

    // 3. Per-segment: nearby buildings + population weight
    const rawWeights       = new Float32Array(this._segments.length);
    this._segBuildings     = new Array(this._segments.length);

    for (let si = 0; si < this._segments.length; si++) {
      const seg = this._segments[si];
      const mx  = (seg.x0 + seg.x1) * 0.5;
      const mz  = (seg.z0 + seg.z1) * 0.5;
      const cgx = Math.floor(mx / GCELL);
      const cgz = Math.floor(mz / GCELL);

      const nearby = [];
      let pop = 0;
      for (let dgx = -1; dgx <= 1; dgx++) {
        for (let dgz = -1; dgz <= 1; dgz++) {
          const cell = bGrid.get(`${cgx + dgx},${cgz + dgz}`);
          if (!cell) continue;
          for (const bi of cell) {
            const b  = buildings[bi];
            const dx = b.world_x - mx;
            const dz = b.world_z - mz;
            if (dx * dx + dz * dz <= R2) {
              nearby.push(bi);
              pop += b.pop;
            }
          }
        }
      }
      this._segBuildings[si] = nearby;
      rawWeights[si]         = Math.max(pop, 1);
    }

    // 3b. Reverse index: buildingIdx → [segIdx, …] (used by setSelectedBuilding)
    this._bldgSegs = [];
    for (let si = 0; si < this._segBuildings.length; si++) {
      for (const bi of this._segBuildings[si]) {
        if (!this._bldgSegs[bi]) this._bldgSegs[bi] = [];
        this._bldgSegs[bi].push(si);
      }
    }

    // 4. Cumulative weight array for O(log n) weighted random selection
    this._cumWeights  = new Float32Array(this._segments.length);
    let cum = 0;
    for (let i = 0; i < rawWeights.length; i++) {
      cum += rawWeights[i];
      this._cumWeights[i] = cum;
    }
    this._totalWeight = cum;

    // 5. Spawn particles
    this._state = Array.from({ length: PARTICLE_COUNT }, () => this._spawn());

    // 6. Dot geometry (THREE.Points — circles)
    const dotGeo = new THREE.BufferGeometry();
    dotGeo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(PARTICLE_COUNT * 3), 3));
    dotGeo.setAttribute('color',    new THREE.BufferAttribute(new Float32Array(PARTICLE_COUNT * 3), 3));

    const dotMat = new THREE.PointsMaterial({
      size:            DOT_SIZE,
      map:             _makeCircleTexture(),
      vertexColors:    true,
      transparent:     true,
      opacity:         0.95,
      sizeAttenuation: false,   // pixel-sized — readable at every zoom
      depthWrite:      false,
      alphaTest:       0.05,
    });

    this._dots         = new THREE.Points(dotGeo, dotMat);
    this._dots.visible = false;
    this.scene.add(this._dots);

    // 7. Trail geometry (LineSegments2 — supports real pixel widths via shader)
    this._trailPosBuf = new Float32Array(PARTICLE_COUNT * 6);
    this._trailColBuf = new Float32Array(PARTICLE_COUNT * 6);

    const trailGeo = new LineSegmentsGeometry();
    trailGeo.setPositions(this._trailPosBuf);
    trailGeo.setColors(this._trailColBuf);

    const trailMat = new LineMaterial({
      vertexColors: true,
      linewidth:    TRAIL_WIDTH,        // in pixels (worldUnits=false default)
      transparent:  true,
      opacity:      0.95,
      depthWrite:   false,
      resolution:   new THREE.Vector2(window.innerWidth, window.innerHeight),
    });

    this._trailMat = trailMat;
    window.addEventListener('resize', () => {
      trailMat.resolution.set(window.innerWidth, window.innerHeight);
    });

    this._trails         = new LineSegments2(trailGeo, trailMat);
    this._trails.computeLineDistances?.();
    this._trails.visible = false;
    this.scene.add(this._trails);

    // Initial geometry fill (all green — no cells yet)
    this._writeGeometries(null);
  }

  // ── Spawn ──────────────────────────────────────────────────────────────────

  _spawn() {
    let si;
    if (this._filterSegs && this._filterSegs.length > 0) {
      // Uniform over the selected-building's segments
      si = this._filterSegs[(Math.random() * this._filterSegs.length) | 0];
    } else {
      // Weighted binary search on cumulative population weights
      const r  = Math.random() * this._totalWeight;
      let lo = 0, hi = this._cumWeights.length - 1;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (this._cumWeights[mid] < r) lo = mid + 1; else hi = mid;
      }
      si = lo;
    }
    return {
      segIdx: si,
      t:      Math.random(),
      speed:  6 + Math.random() * 8,   // 6–14 m/s
    };
  }

  /**
   * Restrict the flow to segments adjacent to a single building. Pass null
   * to go back to showing all traffic. Respawns every particle onto the
   * filtered set so the effect is immediate rather than waiting for natural
   * turnover.
   */
  setSelectedBuilding(buildingIdx) {
    if (buildingIdx == null || !this._bldgSegs[buildingIdx] ||
        this._bldgSegs[buildingIdx].length === 0) {
      this._filterSegs = null;
    } else {
      this._filterSegs = this._bldgSegs[buildingIdx];
    }
    // Re-spawn every particle so the change is visible on the next frame
    for (let i = 0; i < this._state.length; i++) {
      this._state[i] = this._spawn();
    }
  }

  // ── SEIR colour for a segment ──────────────────────────────────────────────

  _seirColor(segIdx, cells) {
    if (!cells) return COL_S;
    const bIdxs = this._segBuildings[segIdx];
    if (!bIdxs || bIdxs.length === 0) return COL_S;
    let S = 0, E = 0, I = 0, R = 0, N = 0;
    for (const bi of bIdxs) {
      const c = cells[bi];
      if (c && c.N > 0) { S += c.S; E += c.E; I += c.I; R += c.R; N += c.N; }
    }
    if (N <= 0) return COL_S;
    const sF = S / N, eF = E / N, iF = I / N, rF = R / N;
    return [
      sF * COL_S[0] + eF * COL_E[0] + iF * COL_I[0] + rF * COL_R[0],
      sF * COL_S[1] + eF * COL_E[1] + iF * COL_I[1] + rF * COL_R[1],
      sF * COL_S[2] + eF * COL_E[2] + iF * COL_I[2] + rF * COL_R[2],
    ];
  }

  // ── Write positions + colours to both geometries ───────────────────────────

  _writeGeometries(cells) {
    if (!this._dots || !this._trails) return;

    const dotPos   = this._dots.geometry.attributes.position.array;
    const dotCol   = this._dots.geometry.attributes.color.array;
    const trlPos   = this._trailPosBuf;
    const trlCol   = this._trailColBuf;

    // Intervention thinning: only the first `visCount` particles are drawn
    // at their real positions; the rest are parked far below the ground so
    // they're effectively invisible without changing geometry buffer sizes.
    // Lockdown collapses visCount to zero (handled at update() entry too).
    const visCount = this._lockdown
      ? 0
      : Math.max(0, Math.floor(this._state.length * this._distanceFactor));
    const PARK_Y = -10000;

    for (let i = 0; i < this._state.length; i++) {
      const base = i * 6;

      if (i >= visCount) {
        // Park the dot AND the trail vertices below the world.
        dotPos[i * 3]     = 0; dotPos[i * 3 + 1] = PARK_Y; dotPos[i * 3 + 2] = 0;
        trlPos[base]      = 0; trlPos[base + 1] = PARK_Y; trlPos[base + 2] = 0;
        trlPos[base + 3]  = 0; trlPos[base + 4] = PARK_Y; trlPos[base + 5] = 0;
        // Colours don't matter when parked, but zero them so a stale
        // bright vertex can't bleed through if the buffer is reused.
        dotCol[i * 3] = 0; dotCol[i * 3 + 1] = 0; dotCol[i * 3 + 2] = 0;
        trlCol[base] = 0; trlCol[base + 1] = 0; trlCol[base + 2] = 0;
        trlCol[base + 3] = 0; trlCol[base + 4] = 0; trlCol[base + 5] = 0;
        continue;
      }

      const p   = this._state[i];
      const seg = this._segments[p.segIdx];

      const cx = seg.x0 + (seg.x1 - seg.x0) * p.t;
      const cz = seg.z0 + (seg.z1 - seg.z0) * p.t;

      // -- dot --
      dotPos[i * 3]     = cx;
      dotPos[i * 3 + 1] = ALTITUDE;
      dotPos[i * 3 + 2] = cz;

      // -- trail: tail (dim) → head (full color) --
      const dx = (seg.x1 - seg.x0) / seg.len;
      const dz = (seg.z1 - seg.z0) / seg.len;
      const tl = Math.min(seg.len * 0.18, TRAIL_LEN_MAX);

      const tx = cx - dx * tl;
      const tz = cz - dz * tl;

      trlPos[base]     = tx;   trlPos[base + 1] = ALTITUDE; trlPos[base + 2] = tz;  // tail
      trlPos[base + 3] = cx;   trlPos[base + 4] = ALTITUDE; trlPos[base + 5] = cz;  // head

      // -- colour --
      const [r, g, b] = this._seirColor(p.segIdx, cells);

      dotCol[i * 3]     = r;
      dotCol[i * 3 + 1] = g;
      dotCol[i * 3 + 2] = b;

      // Tail is 15% brightness → gives a fading-streak look
      trlCol[base]     = r * 0.15; trlCol[base + 1] = g * 0.15; trlCol[base + 2] = b * 0.15;
      trlCol[base + 3] = r;        trlCol[base + 4] = g;        trlCol[base + 5] = b;
    }

    this._dots.geometry.attributes.position.needsUpdate = true;
    this._dots.geometry.attributes.color.needsUpdate    = true;

    // LineSegments2: re-upload via setPositions/setColors (rebuilds instance buffers)
    this._trails.geometry.setPositions(trlPos);
    this._trails.geometry.setColors(trlCol);
  }

  // ── Update (called every rAF) ──────────────────────────────────────────────

  /**
   * @param {Array|null} cells     — state.cells from /api/state
   * @param {number}     dtSec    — real elapsed seconds
   * @param {number}     simSpeed — 0 = paused
   * @param {number}     _flowHour
   */
  update(cells, dtSec, simSpeed, _flowHour) {
    if (!this._dots || !this._trails) return;

    this._useTrails = simSpeed >= TRAIL_SPEED_THRESHOLD;

    if (simSpeed === 0) return;   // freeze — visibility handled by setVisible()

    // Lockdown: no movement, no positions written. We still call
    // _writeGeometries once so the parked-vertex pass clears any leftover
    // visible particles, then bail.
    if (this._lockdown) {
      this._writeGeometries(cells);
      return;
    }

    const dt = dtSec * simSpeed;

    // Only the visible slice is actually advanced — parked particles can
    // stay frozen (we overwrite their positions in _writeGeometries anyway).
    const visCount = Math.max(0, Math.floor(this._state.length * this._distanceFactor));
    for (let i = 0; i < visCount; i++) {
      const p   = this._state[i];
      const seg = this._segments[p.segIdx];
      p.t += (p.speed * dt) / seg.len;
      if (p.t >= 1) Object.assign(p, this._spawn());
    }

    this._writeGeometries(cells);
  }

  // ── Visibility ─────────────────────────────────────────────────────────────

  setVisible(v) {
    this._visible = v;
    this._applyVisibility();
  }

  /**
   * Update active interventions. Lockdown hides the layer entirely; social
   * distancing thins traffic to ~30%. Other interventions don't affect flow.
   */
  setInterventions(activeSet) {
    const set = activeSet instanceof Set ? activeSet : new Set(activeSet || []);
    this._lockdown       = set.has('lockdown');
    this._distanceFactor = set.has('distancing') ? 0.3 : 1.0;
    this._applyVisibility();
  }

  _applyVisibility() {
    if (!this._dots || !this._trails) return;
    const want = this._visible && !this._lockdown;
    this._dots.visible   = want && !this._useTrails;
    this._trails.visible = want &&  this._useTrails;
  }

  // ── Dispose ────────────────────────────────────────────────────────────────

  _disposeGeometry() {
    for (const mesh of [this._dots, this._trails]) {
      if (!mesh) continue;
      this.scene.remove(mesh);
      mesh.geometry.dispose();
      mesh.material.dispose();
    }
    this._dots = this._trails = null;
  }

  // ── Misc ───────────────────────────────────────────────────────────────────

  getPhaseName(hour) {
    // Returns i18n key — caller translates via t()
    if (hour >= 6  && hour < 9)  return 'tod_rush_morning';
    if (hour >= 9  && hour < 17) return 'tod_workday';
    if (hour >= 17 && hour < 20) return 'tod_rush_evening';
    return 'tod_night';
  }
}
