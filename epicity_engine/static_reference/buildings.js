/**
 * buildings.js — Swedish appearance-mode building factory.
 *
 * Builds a static THREE.Group (the "appearance layer") — one sub-Group per
 * non-road city cell — with stylised 3D geometry inspired by Swedish architecture:
 *
 *   RES_LO     — Falun-red cottage, square-pyramid hip roof, white corner trim
 *   RES_MED    — Swedish-yellow apartment, flat roof parapet, white window strips
 *   RES_HI     — Concrete slab tower, horizontal cornice bands
 *   COMMERCIAL — Dark-glass curtain-wall box, taller
 *   INDUSTRIAL — Steel-gray wide shed + cylindrical chimney
 *   HOSPITAL   — Crisp-white body + red cross on roof
 *   PARK       — Flat green base + 2–3 deterministic spruce trees
 *   SCHOOL     — Brick-red L-shaped body
 *
 * The group is added to the scene but starts hidden (info mode is the default).
 * Call `group.visible = true` to activate appearance mode.
 *
 * No SEIR state dependency — purely geometric; never updated after build.
 */

import * as THREE from 'three';
import { CUSTOM_BUILDERS } from './buildings_custom.js';

// ── Shared materials (built once, reused across all buildings) ────────────────

const M = {
  faluRed:    mat(0x8B2020),
  roofBrown:  mat(0x3d2008),
  white:      mat(0xF5F5F5),
  yellow:     mat(0xE8B824),
  concrete:   mat(0x9CA3AF),
  glass:      mat(0x1e3a5f),
  glassFront: mat(0x2d4e80),
  steel:      mat(0x6B7280),
  hospital:   mat(0xF8FAFC),
  cross:      mat(0xef4444),
  park:       mat(0x1a4a1a),
  foliage:    mat(0x14532d),
  trunk:      mat(0x5c3d1e),
  brickRed:   mat(0xA93226),
  concrete2:  mat(0x7f8c8d),
  churchRed:  mat(0x9E2B25),   // warm red brick
  churchTrim: mat(0xEAE0D0),   // pale stone dressings
  churchRoof: mat(0x2f4a3a),   // weathered copper / oxidised verdigris mix
  churchSpire:mat(0x4a6b5a),   // copper-oxide spire
  castleStone:mat(0x8e8572),   // warm grey limestone
  castleRoof: mat(0x3b7b6e),   // verdigris-green copper roof
  castleTrim: mat(0xc7bfa8),   // pale cornice stone
  ruinStone:  mat(0x8a847a),   // cool grey rubble masonry (Kronoberg ruin)
  ruinMoss:   mat(0x5a6b48),   // mossy / lichen patches on wall tops
  bridgeWood: mat(0x6b4422),   // weathered brown plank
  museumBrick:mat(0xa05030),   // warm red museum brick
  museumStone:mat(0xd8cfb8),   // pale limestone cornice / quoins
  museumRoof: mat(0x2d3a4a),   // dark slate roof
  theaterRed: mat(0x7f1d1d),   // marquee red
  theaterGold:mat(0xd4a017),   // gilded trim
  arenaMetal: mat(0xa3a8b0),   // pale steel panel
  arenaGlass: mat(0x58b6d6),   // light arena glass curtain wall
  arenaRoof:  mat(0x3a3f47),   // dark standing-seam metal roof
  moatWater:  mat(0x2d4a68),   // dark blue castle moat
  baroqueStucco: mat(0xe8d9a8),// warm cream baroque stucco
  castleTile: mat(0x7a3223),   // terracotta red clay roof tile
  lakeBlue:   mat(0x1e5f9b),   // flat lake/river polygon
};

function mat(hex) {
  return new THREE.MeshLambertMaterial({ color: hex });
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Build the full appearance layer for the city.
 *
 * @param {{ buildings: Array<{idx,world_x,world_z,zone,pop,levels,area_m2}> }} layout
 * @param {THREE.Scene} scene   — the group is added here immediately
 * @returns {THREE.Group}       — outer container (toggle .visible to switch modes)
 */
export function buildAppearanceCity(layout, scene) {
  const outer = new THREE.Group();

  for (const c of layout.buildings) {
    // Landmark buildings with custom 3D assets live in their own always-on
    // layer (see buildLandmarks below) — skip them here so they don't
    // render twice when appearance mode is active.
    if (c.custom_builder) continue;

    const building = _buildCell(c);
    if (building) {
      building.userData.buildingIdx = c.idx;
      outer.add(building);
    }
  }

  scene.add(outer);
  return outer;
}

/**
 * Build the always-on landmark layer: only buildings that have a
 * `custom_builder` field set (via the manual overrides file). Used for
 * landmark assets like Växjö Cathedral that should be visible regardless
 * of whether the user is in info mode or appearance mode.
 *
 * @param {{ buildings: Array }}  layout
 * @param {THREE.Scene}           scene
 * @returns {THREE.Group}  — outer group, already added to the scene
 */
export function buildLandmarks(layout, scene) {
  const outer = new THREE.Group();

  for (const c of layout.buildings) {
    if (!c.custom_builder) continue;
    const building = _buildCell(c);
    if (building) {
      building.userData.buildingIdx = c.idx;

      // Cache each mesh's original material so view.js can swap them out
      // for an SEIR-tinted clone in non-zone colour modes and then restore
      // them without re-building geometry. Also enable shadow casting /
      // receiving so landmarks participate in the day/night sun-shadow
      // pass the rest of the city uses.
      building.traverse(obj => {
        if (obj.isMesh && obj.material && !obj.userData.__origMat) {
          obj.userData.__origMat = obj.material;
        }
        if (obj.isMesh) {
          obj.castShadow    = true;
          obj.receiveShadow = true;
        }
      });

      outer.add(building);
    }
  }

  scene.add(outer);
  return outer;
}

// Materials and helpers re-exported so buildings_custom.js can share them
// without duplicating the Lambert material instances.
export { M };
export function makeGroup(c)                { return _group(c); }
export function addBox(group, w,h,d, mat, x,y,z) { _box(group, w,h,d, mat, x,y,z); }

// ── Per-zone builders ─────────────────────────────────────────────────────────

function _buildCell(c) {
  // Per-building custom 3D model (override system). Takes priority over
  // zone — used for landmark buildings like Växjö Cathedral. Custom
  // builders receive an unscaled root group (real metre units) so landmark
  // geometry can be authored at its true size, unlike zone defaults which
  // get stretched to the building footprint.
  if (c.custom_builder) {
    const fn = CUSTOM_BUILDERS[c.custom_builder];
    if (fn) {
      const root = new THREE.Group();
      root.position.set(c.world_x, 0, c.world_z);
      const ok = fn(root, c, { THREE, M });
      if (ok !== false) return root;
    }
    // Fall through to zone default if the builder name is unknown.
  }
  switch (c.zone) {
    case 1:  return _resLo(c);
    case 2:  return _resMed(c);
    case 3:  return _resHi(c);
    case 4:  return _commercial(c);
    case 5:  return _industrial(c);
    case 6:  return _hospital(c);
    case 7:  return _park(c);
    case 8:  return _school(c);
    case 14: return _church(c);
    case 15: return _castle(c);
    case 16: return _museum(c);
    case 17: return _theater(c);
    case 18: return _stadium(c);
    case 19: return _waterPad(c);    // flat blue lake/river polygon
    case 20: return null;            // forests render only as tree scatter
    default: return null;
  }
}

// ── WATER — Flat blue pad (appearance-mode base) ────────────────────────────
// A thin blue square matching the stored footprint. Info mode uses the
// extruded-polygon mesh for the same effect via ZONE_COLORS[19]; this
// builder handles the appearance-mode path only.
function _waterPad(c) {
  const g  = _group(c);
  const bH = 0.04;
  _box(g, 0.98, bH, 0.98, M.lakeBlue, 0, bH / 2, 0);
  return g;
}

// ── RES_LO — Falun-red Swedish cottage ───────────────────────────────────────

function _resLo(c) {
  const g   = _group(c);
  const bW  = 0.80;
  const bH  = 0.30;

  // Main body
  _box(g, bW, bH, bW, M.faluRed, 0, bH / 2, 0);

  // Hip roof — 4-sided pyramid
  const roof = new THREE.Mesh(
    new THREE.CylinderGeometry(0, bW * 0.62, 0.32, 4),
    M.roofBrown,
  );
  roof.rotation.y = Math.PI / 4;   // align pyramid corners with walls
  roof.position.set(0, bH + 0.16, 0);
  g.add(roof);

  // White corner trim — 4 vertical strips
  const trimH = bH + 0.01;
  const off   = bW / 2;
  for (const [dx, dz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    _box(g, 0.045, trimH, 0.045, M.white, dx * off, trimH / 2, dz * off);
  }

  return g;
}

// ── RES_MED — Swedish yellow apartment ───────────────────────────────────────

function _resMed(c) {
  const g  = _group(c);
  const bH = 0.58;

  _box(g, 0.84, bH, 0.84, M.yellow, 0, bH / 2, 0);

  // Flat roof parapet
  _box(g, 0.88, 0.04, 0.88, M.white, 0, bH + 0.02, 0);

  // White window strips (horizontal bands at 1/3 and 2/3 height)
  for (const fy of [0.33, 0.66]) {
    _box(g, 0.86, 0.04, 0.04, M.white, 0, bH * fy, -0.42);
    _box(g, 0.86, 0.04, 0.04, M.white, 0, bH * fy,  0.42);
    _box(g, 0.04, 0.04, 0.86, M.white, -0.42, bH * fy, 0);
    _box(g, 0.04, 0.04, 0.86, M.white,  0.42, bH * fy, 0);
  }

  return g;
}

// ── RES_HI — Concrete high-rise ──────────────────────────────────────────────

function _resHi(c) {
  const g  = _group(c);
  const bH = 1.55;

  _box(g, 0.82, bH, 0.82, M.concrete, 0, bH / 2, 0);

  // Horizontal cornice bands every ~0.4 units
  for (let y = 0.4; y < bH; y += 0.38) {
    _box(g, 0.85, 0.035, 0.85, M.concrete2, 0, y, 0);
  }

  return g;
}

// ── COMMERCIAL — Dark-glass curtain-wall ──────────────────────────────────────

function _commercial(c) {
  const g  = _group(c);
  const bH = 1.20;

  // Main glass body
  _box(g, 0.86, bH, 0.86, M.glass, 0, bH / 2, 0);

  // Lighter glass-panel overlay on front and sides (thin offset boxes)
  for (const [dx, dz, sx, sz] of [
    [0, -0.44, 0.70, 0.01],
    [0,  0.44, 0.70, 0.01],
    [-0.44, 0, 0.01, 0.70],
    [ 0.44, 0, 0.01, 0.70],
  ]) {
    _box(g, sx, bH * 0.9, sz, M.glassFront, dx, bH / 2, dz);
  }

  return g;
}

// ── INDUSTRIAL — Steel-gray shed + chimney ────────────────────────────────────

function _industrial(c) {
  const g  = _group(c);
  const bH = 0.55;

  // Wide low body
  _box(g, 0.88, bH, 0.70, M.steel, 0, bH / 2, 0);

  // Chimney
  const chimney = new THREE.Mesh(
    new THREE.CylinderGeometry(0.055, 0.07, 0.42, 8),
    M.concrete2,
  );
  chimney.position.set(0.26, bH + 0.21, 0.18);
  g.add(chimney);

  // Ridge cap on roof
  _box(g, 0.90, 0.04, 0.12, M.concrete2, 0, bH + 0.02, 0);

  return g;
}

// ── HOSPITAL — White box + red cross ─────────────────────────────────────────

function _hospital(c) {
  const g  = _group(c);
  const bH = 0.95;

  _box(g, 0.86, bH, 0.86, M.hospital, 0, bH / 2, 0);

  // Red cross on roof top (two perpendicular bars)
  const crossY = bH + 0.025;
  _box(g, 0.10, 0.05, 0.38, M.cross, 0, crossY, 0);   // N–S bar
  _box(g, 0.38, 0.05, 0.10, M.cross, 0, crossY, 0);   // E–W bar

  // White parapet
  _box(g, 0.90, 0.05, 0.90, M.white, 0, bH + 0.025, 0);

  return g;
}

// ── PARK — Green base + spruce trees ─────────────────────────────────────────

function _park(c) {
  const g = _group(c);

  // Green ground pad
  _box(g, 0.92, 0.06, 0.92, M.park, 0, 0.03, 0);

  // 2–3 trees, deterministic positions seeded by building index
  const seed  = c.idx * 48 + 7;
  const count = 2 + (seed % 2);   // 2 or 3 trees
  const offsets = [
    [-0.22,  0.18],
    [ 0.24, -0.20],
    [ 0.02,  0.26],
  ];

  for (let i = 0; i < count; i++) {
    const [ox, oz] = offsets[i];
    // Trunk
    const trunk = new THREE.Mesh(
      new THREE.CylinderGeometry(0.03, 0.04, 0.14, 6),
      M.trunk,
    );
    trunk.position.set(ox, 0.13, oz);
    g.add(trunk);

    // Foliage cone — slightly randomised height via seed
    const fh = 0.42 + ((seed * (i + 3)) % 7) * 0.02;
    const foliage = new THREE.Mesh(
      new THREE.ConeGeometry(0.18, fh, 7),
      M.foliage,
    );
    foliage.position.set(ox, 0.20 + fh / 2, oz);
    g.add(foliage);
  }

  return g;
}

// ── SCHOOL — Brick-red L-shaped body ─────────────────────────────────────────

function _school(c) {
  const g  = _group(c);
  const bH = 0.65;

  // Main wing
  _box(g, 0.86, bH, 0.50, M.brickRed,  0.00, bH / 2, -0.18);

  // Cross wing (shorter)
  _box(g, 0.44, bH, 0.38, M.brickRed, -0.21, bH / 2,  0.20);

  // White fascia strip at roofline
  _box(g, 0.88, 0.045, 0.52, M.white,  0.00, bH + 0.022, -0.18);
  _box(g, 0.46, 0.045, 0.40, M.white, -0.21, bH + 0.022,  0.20);

  return g;
}

// ── CHURCH — Generic Swedish parish church ───────────────────────────────────

function _church(c) {
  const g  = _group(c);
  const bW = 0.80;
  const bH = 0.70;

  // Red-brick nave
  _box(g, bW, bH, bW * 0.65, M.churchRed, 0, bH / 2, 0);

  // Pale stone corner quoins
  const trimH = bH;
  const off   = bW / 2;
  for (const [dx, dz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    _box(g, 0.05, trimH, 0.05, M.churchTrim,
         dx * off, trimH / 2, dz * off * 0.65);
  }

  // Steep gable roof — long prism across the nave
  const roof = new THREE.Mesh(
    new THREE.CylinderGeometry(0, bW * 0.47, bW * 0.65, 3),
    M.churchRoof,
  );
  roof.rotation.z = Math.PI / 2;
  roof.rotation.y = Math.PI / 2;
  roof.position.set(0, bH + bW * 0.23, 0);
  roof.scale.set(1, bW * 0.9 / (bW * 0.65), 1);
  g.add(roof);

  // Square tower base at the west end
  const towerH = bH * 1.3;
  _box(g, bW * 0.30, towerH, bW * 0.30, M.churchRed,
       -bW * 0.32, towerH / 2, 0);

  // Octagonal spire on top of the tower
  const spire = new THREE.Mesh(
    new THREE.ConeGeometry(bW * 0.20, bW * 0.75, 8),
    M.churchSpire,
  );
  spire.position.set(-bW * 0.32, towerH + bW * 0.375, 0);
  g.add(spire);

  // Small cross on the spire tip
  _box(g, 0.012, 0.08, 0.012, M.churchTrim,
       -bW * 0.32, towerH + bW * 0.75 + 0.04, 0);
  _box(g, 0.04, 0.012, 0.012, M.churchTrim,
       -bW * 0.32, towerH + bW * 0.75 + 0.02, 0);

  return g;
}

// ── CASTLE — Generic stone castle with corner tower ─────────────────────────

function _castle(c) {
  const g  = _group(c);
  const bW = 0.80;
  const bH = 1.05;

  // Main stone keep
  _box(g, bW, bH, bW * 0.78, M.castleStone, 0, bH / 2, 0);

  // Pale cornice ring around the top
  _box(g, bW + 0.03, 0.08, bW * 0.78 + 0.03, M.castleTrim, 0, bH, 0);

  // Steep pitched roof on the main block
  const roof = new THREE.Mesh(
    new THREE.CylinderGeometry(0, bW * 0.56, bW * 0.48, 3),
    M.castleRoof,
  );
  roof.rotation.z = Math.PI / 2;
  roof.scale.set(1, (bW * 0.9) / (bW * 0.56 * 2), 1);
  roof.position.set(0, bH + bW * 0.24, 0);
  g.add(roof);

  // Corner tower — cylindrical, taller than the main block
  const towerH = bH * 1.55;
  const tower  = new THREE.Mesh(
    new THREE.CylinderGeometry(0.14, 0.14, towerH, 24),
    M.castleStone,
  );
  tower.position.set(-bW * 0.36, towerH / 2, -bW * 0.30);
  g.add(tower);

  // Conical copper-green roof on top of the tower
  const coneR = 0.18;
  const coneH = 0.42;
  const cone  = new THREE.Mesh(
    new THREE.ConeGeometry(coneR, coneH, 20),
    M.castleRoof,
  );
  cone.position.set(-bW * 0.36, towerH + coneH / 2, -bW * 0.30);
  g.add(cone);

  // Secondary smaller tower at another corner
  const tower2H = bH * 1.15;
  const tower2  = new THREE.Mesh(
    new THREE.CylinderGeometry(0.10, 0.10, tower2H, 20),
    M.castleStone,
  );
  tower2.position.set(bW * 0.36, tower2H / 2, bW * 0.30);
  g.add(tower2);

  const cone2 = new THREE.Mesh(
    new THREE.ConeGeometry(0.13, 0.32, 18),
    M.castleRoof,
  );
  cone2.position.set(bW * 0.36, tower2H + 0.16, bW * 0.30);
  g.add(cone2);

  return g;
}

// ── MUSEUM — Red-brick museum with pale stone trim ──────────────────────────

function _museum(c) {
  const g  = _group(c);
  const bW = 0.82;
  const bH = 0.80;

  // Main brick body
  _box(g, bW, bH, bW * 0.72, M.museumBrick, 0, bH / 2, 0);

  // Pale stone base course
  _box(g, bW + 0.02, 0.06, bW * 0.72 + 0.02, M.museumStone, 0, 0.03, 0);

  // Pale stone cornice just under the roof
  _box(g, bW + 0.02, 0.06, bW * 0.72 + 0.02, M.museumStone, 0, bH - 0.03, 0);

  // Central entrance portico — a slightly projecting lighter block
  _box(g, bW * 0.28, bH * 0.75, 0.06, M.museumStone,
       0, bH * 0.375, bW * 0.72 / 2 + 0.02);
  // Four slim columns on the portico
  for (let i = 0; i < 4; i++) {
    const cx = -bW * 0.11 + (i / 3) * bW * 0.22;
    const col = new THREE.Mesh(
      new THREE.CylinderGeometry(0.015, 0.015, bH * 0.55, 12),
      M.museumStone,
    );
    col.position.set(cx, bH * 0.275, bW * 0.72 / 2 + 0.05);
    g.add(col);
  }

  // Pitched roof with dark slate
  const roof = new THREE.Mesh(
    new THREE.CylinderGeometry(0, bW * 0.5, bW * 0.22, 3),
    M.museumRoof,
  );
  roof.rotation.z = Math.PI / 2;
  roof.scale.set(1, (bW * 0.9) / (bW * 0.5 * 2), 1);
  roof.position.set(0, bH + bW * 0.11, 0);
  g.add(roof);

  return g;
}

// ── THEATER — Red facade + gilded cornice + marquee ─────────────────────────

function _theater(c) {
  const g  = _group(c);
  const bW = 0.80;
  const bH = 0.95;

  // Deep red facade
  _box(g, bW, bH, bW * 0.68, M.theaterRed, 0, bH / 2, 0);

  // Gilded horizontal cornice stripes
  for (const y of [bH * 0.35, bH * 0.65, bH * 0.92]) {
    _box(g, bW + 0.02, 0.03, bW * 0.68 + 0.02, M.theaterGold, 0, y, 0);
  }

  // Projecting marquee canopy over the entrance
  _box(g, bW * 0.55, 0.08, 0.18, M.theaterGold,
       0, bH * 0.22, bW * 0.68 / 2 + 0.08);

  // Three tall windows on the facade (dark glass)
  for (let i = -1; i <= 1; i++) {
    _box(g, bW * 0.12, bH * 0.32, 0.02, M.glass,
         i * bW * 0.22, bH * 0.55, bW * 0.68 / 2 + 0.02);
  }

  // Flat parapet roof
  _box(g, bW + 0.04, 0.03, bW * 0.68 + 0.04, M.theaterGold, 0, bH + 0.015, 0);

  return g;
}

// ── STADIUM — Low rectangular body + curved barrel roof ─────────────────────

function _stadium(c) {
  const g  = _group(c);
  const bW = 0.85;
  const bH = 0.45;

  // Low steel-panel body
  _box(g, bW, bH, bW * 0.62, M.arenaMetal, 0, bH / 2, 0);

  // Glass entry facade (front)
  _box(g, bW * 0.75, bH * 0.85, 0.02, M.arenaGlass,
       0, bH * 0.425, bW * 0.62 / 2 + 0.005);

  // Horizontal accent stripe
  _box(g, bW + 0.02, 0.04, bW * 0.62 + 0.02, M.arenaGlass, 0, bH * 0.7, 0);

  // Curved barrel roof (half-cylinder) spanning the long axis
  const roof = new THREE.Mesh(
    new THREE.CylinderGeometry(
      bW * 0.34, bW * 0.34, bW * 0.95, 20, 1, false, 0, Math.PI),
    M.arenaRoof,
  );
  roof.rotation.z = Math.PI / 2;
  roof.position.set(0, bH + bW * 0.005, 0);
  g.add(roof);

  return g;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Reference heights per zone (internal geometry units, ~1 = full zone height)
const _ZONE_REF_H = {
  1: 0.46, 2: 0.62, 3: 1.60, 4: 1.24, 5: 0.59, 6: 1.00, 7: 0.09, 8: 0.69,
  14: 0.90, 15: 1.20, 16: 0.95, 17: 1.05, 18: 0.80,
  19: 0.04,             // flat lake pad — no real height (zone 20 returns null)
};

/**
 * Create a positioned + scaled Group at real-world coordinates.
 * The internal geometry was designed for a unit cell (width ≈ 0.86, height ≈ zone ref).
 * We scale it to the building's actual footprint (metres) and storey height.
 */
function _group(c) {
  const g = new THREE.Group();
  g.position.set(c.world_x, 0, c.world_z);

  const footprintM  = Math.max(5, Math.min(28, Math.sqrt(c.area_m2 || 100)));
  const targetHeightM = (c.levels || 1) * 3.5;
  const refH        = _ZONE_REF_H[c.zone] || 0.65;
  g.scale.set(footprintM, targetHeightM / refH, footprintM);
  return g;
}

/** Add a box mesh to a group at a local offset. */
function _box(group, w, h, d, material, lx, ly, lz) {
  const mesh = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), material);
  mesh.position.set(lx, ly, lz);
  group.add(mesh);
}
