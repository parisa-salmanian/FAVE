/**
 * buildings_custom.js — Landmark / hand-authored 3D building assets.
 *
 * Each entry in CUSTOM_BUILDERS is a function(root, cell, ctx) that populates
 * `root` (an already-positioned THREE.Group at the building's world_x/world_z)
 * with geometry authored in **real metre units**. Unlike the zone defaults in
 * buildings.js, custom builders are NOT scaled to the OSM footprint — the
 * point of a custom builder is to represent a specific, recognisable
 * landmark at its true proportions.
 *
 * Builders are selected by the string in a building's `custom_builder` field,
 * which is set through the per-city manual overrides file
 * (data/<city>/overrides.json) and preserved across city regeneration.
 *
 * ctx = { THREE, M } — shared Three.js module + the material palette from
 * buildings.js, so all custom builders reuse the same Lambert materials.
 */

// ── Registry ──────────────────────────────────────────────────────────────────

export const CUSTOM_BUILDERS = {
  // ── Växjö ──
  vaxjo_cathedral:  buildVaxjoCathedral,
  teleborg_castle:  buildTeleborgCastle,
  kronoberg_ruin:   buildKronobergRuin,
  smalands_museum:  buildSmalandsMuseum,
  vida_arena:       buildVidaArena,
  // ── Kalmar ──
  kalmar_castle:    buildKalmarCastle,
  kalmar_domkyrka:  buildKalmarDomkyrka,
  kalmar_lans_museum: buildKalmarLansMuseum,
  kalmar_teater:    buildKalmarTeater,
  hatstore_arena:   buildHatstoreArena,
  // ── Stockholm ──
  stockholm_storkyrkan:    buildStockholmStorkyrkan,
  stockholm_royal_palace:  buildStockholmRoyalPalace,
  stockholm_vasamuseet:    buildStockholmVasamuseet,
  stockholm_dramaten:      buildStockholmDramaten,
  stockholm_avicii_arena:  buildStockholmAviciiArena,
  // ── Göteborg ──
  goteborg_domkyrka:       buildGoteborgDomkyrka,
  goteborg_skansen_kronan: buildGoteborgSkansenKronan,
  goteborg_sjofartsmuseet: buildGoteborgSjofartsmuseet,
  goteborg_stora_teatern:  buildGoteborgStoraTeatern,
  goteborg_scandinavium:   buildGoteborgScandinavium,
  // ── Uppsala ──
  uppsala_domkyrka:        buildUppsalaDomkyrka,
  uppsala_slott:           buildUppsalaSlott,
  uppsala_gustavianum:     buildUppsalaGustavianum,
  uppsala_ukk:             buildUppsalaUKK,
  uppsala_studenternas:    buildUppsalaStudenternas,
  // ── Malmö ──
  malmo_sankt_petri:       buildMalmoSanktPetri,
  malmo_malmohus:          buildMalmoMalmohus,
  malmo_moderna_museet:    buildMalmoModernaMuseet,
  malmo_opera:             buildMalmoOpera,
  malmo_eleda_stadion:     buildMalmoEledaStadion,
  // ── Linköping ──
  linkoping_domkyrka:      buildLinkopingDomkyrka,
  linkoping_slott:         buildLinkopingSlott,
  linkoping_jarnvagsmuseum:buildLinkopingJarnvagsmuseum,
  linkoping_konsert_kongress:buildLinkopingKonsertKongress,
  linkoping_saab_arena:    buildLinkopingSaabArena,
  // ── Örebro ──
  orebro_nikolai:          buildOrebroNikolai,
  orebro_slott:            buildOrebroSlott,
  orebro_tekniska_kvarnen: buildOrebroTekniskaKvarnen,
  orebro_conventum:        buildOrebroConventum,
  orebro_behrn_arena:      buildOrebroBehrnArena,
  // ── Norrköping ──
  norrkoping_sankt_olai:   buildNorrkopingSanktOlai,
  norrkoping_johannisborg: buildNorrkopingJohannisborg,
  norrkoping_konstmuseum:  buildNorrkopingKonstmuseum,
  norrkoping_louis_de_geer:buildNorrkopingLouisDeGeer,
  norrkoping_ostgotaporten:buildNorrkopingOstgotaporten,
  // ── Helsingborg ──
  helsingborg_sankta_maria:buildHelsingborgSanktaMaria,
  helsingborg_karnan:      buildHelsingborgKarnan,
  helsingborg_dunkers:     buildHelsingborgDunkers,
  helsingborg_stadsteater: buildHelsingborgStadsteater,
  helsingborg_arena:       buildHelsingborgArena,
  // ── Jönköping ──
  jonkoping_sofiakyrkan:   buildJonkopingSofiakyrkan,
  jonkoping_slottsruin:    buildJonkopingSlottsruin,
  jonkoping_tandsticksmuseet:buildJonkopingTandsticksmuseet,
  jonkoping_spira:         buildJonkopingSpira,
  jonkoping_energi_arena:  buildJonkopingEnergiArena,
  // ─�� Västerås ──
  vasteras_domkyrka:       buildVasterasDomkyrka,
  vasteras_slott:          buildVasterasSlott,
  vasteras_vallby:         buildVasterasVallby,
  vasteras_konserthus:     buildVasterasKonserthus,
  vasteras_hitachi_arena:  buildVasterasHitachiArena,
  // ── Borås ──
  boras_caroli:            buildBorasCaroli,
  boras_museum_castle:     buildBorasMuseumCastle,
  boras_textilmuseet:      buildBorasTextilmuseet,
  boras_stadsteater:       buildBorasStadsteater,
  boras_ishall:            buildBorasIshall,
  // ── Eskilstuna ──
  eskilstuna_eskilskyrkan: buildEskilstunaEskilskyrkan,
  eskilstuna_slott:        buildEskilstunaSlott,
  eskilstuna_bergstromska: buildEskilstunaBergstromska,
  eskilstuna_teater:       buildEskilstunaTeater,
  eskilstuna_tunavallen:   buildEskilstunaTunavallen,
  // ── Gävle ──
  gavle_trefaldighet:      buildGavleTrefaldighet,
  gavle_slott:             buildGavleSlott,
  gavle_lansmuseum:        buildGavleLansmuseum,
  gavle_teater:            buildGavleTeater,
  gavle_gevlevallen:       buildGavleGevlevallen,
  // ── Halmstad ──
  halmstad_nikolai:        buildHalmstadNikolai,
  halmstad_slott:          buildHalmstadSlott,
  halmstad_konstmuseum:    buildHalmstadKonstmuseum,
  halmstad_kulturhuset:    buildHalmstadKulturhuset,
  halmstad_orjans_vall:    buildHalmstadOrjansVall,
  // ── Karlskrona ──
  karlskrona_fredrikskyrkan:buildKarlskronaFredrikskyrkan,
  karlskrona_kastell:      buildKarlskronaKastell,
  karlskrona_marinmuseum:  buildKarlskronaMarinmuseum,
  karlskrona_sparre:       buildKarlskronaSparre,
  karlskrona_nkt_arena:    buildKarlskronaNktArena,
  // ── Karlstad ──
  karlstad_domkyrka:       buildKarlstadDomkyrka,
  karlstad_residens:       buildKarlstadResidens,
  karlstad_sandgrund:      buildKarlstadSandgrund,
  karlstad_wermland_opera: buildKarlstadWermlandOpera,
  karlstad_lofbergs_arena: buildKarlstadLofbergsArena,
  // ── Luleå ──
  lulea_ornasket:          buildLuleaOrnasket,
  lulea_fortress:          buildLuleaFortress,
  lulea_norrbottens_museum:buildLuleaNorrbottensMuseum,
  lulea_norrbottensteatern:buildLuleaNorrbottensteatern,
  lulea_coop_arena:        buildLuleaCoopArena,
  // ── Lund ──
  lund_domkyrka:           buildLundDomkyrka,
  lund_ortofta:            buildLundOrtofta,
  lund_historiska:         buildLundHistoriska,
  lund_stadsteater:        buildLundStadsteater,
  lund_ip:                 buildLundIp,
  // ── Sundsvall ──
  sundsvall_brokyrkan:     buildSundsvallBrokyrkan,
  sundsvall_fortress:      buildSundsvallFortress,
  sundsvall_museum:        buildSundsvallMuseum,
  sundsvall_teater:        buildSundsvallTeater,
  sundsvall_norrporten:    buildSundsvallNorrporten,
  // ── Umeå ──
  umea_stadskyrka:         buildUmeaStadskyrka,
  umea_fortress:           buildUmeaFortress,
  umea_bildmuseet:         buildUmeaBildmuseet,
  umea_folkets_hus:        buildUmeaFolketsHus,
  umea_visionite_arena:    buildUmeaVisioniteArena,
  // ── Visby ──
  visby_domkyrka:          buildVisbyDomkyrka,
  visby_visborg:           buildVisbyVisborg,
  visby_konstmuseum:       buildVisbyKonstmuseum,
  visby_almedalen:         buildVisbyAlmedalen,
  visby_gutavallen:        buildVisbyGutavallen,
  // NOTE: Natural features (zone 19 WATER / zone 20 FORESTRY) are NOT
  // registered here. The flat footprints are drawn by the info-mode
  // extruded polygon mesh in view.js (ZONE_COLORS + height special
  // cases), and the species-aware 3D tree scatter is built in one
  // scene-wide batch by buildNaturalDetailLayer() below.
};

// ── Växjö Cathedral (Växjö Domkyrka) ─────────────────────────────────────────
//
// Matches the blueprint west elevation + south elevation supplied by the
// user. Key features:
//
//   - Twin tall octagonal copper spires at the west end, near the N/S edges
//     of the tower block, each with multi-stage taper and decorative rings.
//   - Stepped brick gable rising BETWEEN the two spires as the central west
//     facade, with clock / rose window / west portal.
//   - Long red-brick nave with steep pitched roof.
//   - Tessellated roof tile pattern via InstancedMesh (one draw call).
//   - Tall arched clerestory windows above the side aisles.
//   - Lower side aisles with their own arched window rhythm and lean-to
//     roofs.
//   - Stepped-gable apse (flat east wall with a step profile) — matches the
//     south elevation more than a semi-circular Romanesque apse.
//
// Face budget ~1M, concentrated in the spires (high SPIRE_RADIAL × many
// sections × high-tubular torus rings) and the instanced roof-tile grid.

function buildVaxjoCathedral(root, cell, ctx) {
  const { THREE, M } = ctx;

  // ── Axes ───────────────────────────────────────────────────────────────────
  // Local +X = East (nave length), +Z = transverse (N/S), +Y = up.
  // Local origin is approximately at the nave centroid so the model sits on
  // the OSM building location.

  // ── Dimensions (metres) ────────────────────────────────────────────────────
  //
  // The tower is deliberately narrower than the nave+aisles so that the
  // aisles' west ends extend PAST the tower's N/S edges, forming the
  // "lateral bottom parts" visible in the front elevation — short, low
  // wings flanking the tall tower block at aisle-height.
  const NAVE_LEN            = 52;
  const NAVE_W              = 13;
  const NAVE_H              = 22;
  const NAVE_ROOF_H         = 11;    // peak above walls — steep pitch
  const AISLE_W             = 5;
  const AISLE_H             = 13;

  const TOWER_W             = 13;    // narrow enough for aisles to stick out past its N/S edges
  const TOWER_D             = 13;
  const TOWER_H             = 28;
  const GABLE_STEPS         = 7;
  const GABLE_STEP_H        = 1.4;
  const GABLE_EXTRA_H       = GABLE_STEPS * GABLE_STEP_H;

  const SPIRE_BASE_R        = 2.8;
  const SPIRE_DRUM_H        = 5;
  const SPIRE_MAIN_H        = 32;   // main tapered section — tall & dominating
  const SPIRE_NEEDLE_H      = 4;    // sharp tip
  const SPIRE_RADIAL        = 96;
  const SPIRE_SECTIONS      = 14;
  const SPIRE_RING_RADIAL   = 32;
  const SPIRE_RING_TUBULAR  = 192;
  const SPIRE_SEP_Z         = 3.4;   // half-distance between the two spires — fits in a 13m-wide tower
  const APSE_W              = 14;    // width of stepped-gable apse body
  const APSE_D              = 9;     // how far the apse protrudes east of the nave
  const APSE_GABLE_STEPS    = 5;

  const CLERESTORY_BAYS     = 10;

  // Tile grid for the nave roof (2 slope faces × COLS × ROWS instances).
  const TILE_COLS           = 130;
  const TILE_ROWS           = 32;

  // ── Materials ──────────────────────────────────────────────────────────────
  const brick    = M.churchRed;
  const stone    = M.churchTrim;
  const roof     = M.churchRoof;
  const spireMat = M.churchSpire;
  const glass    = M.glass;
  const dark     = M.roofBrown;

  // ── Face-count tracker ─────────────────────────────────────────────────────
  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index
      ? g.index.count / 3
      : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };

  // ── Helpers ────────────────────────────────────────────────────────────────
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };

  // Tessellated box — segs controls face count cubically.
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Orient the model to match the OSM building footprint ──────────────────
  //
  // The cathedral is built in a local frame where +X is the nave (west→east)
  // axis and +Z is transverse. The OSM polygon for Växjö Domkyrka is
  // typically at a slight angle relative to the world east axis because the
  // street grid isn't perfectly cardinal. We compute the polygon's dominant
  // axis with a 2x2 PCA and rotate the root group so the local +X aligns
  // with it — otherwise the model sits at an angle to the surrounding roads.
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;

    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    // Closed-form 2x2 PCA: angle of the major axis measured from world +X
    // toward +Z. Ranges roughly in [-π/4, π/4] for an elongated building
    // close to axis-aligned.
    const polygonAngle = 0.5 * Math.atan2(2 * sxz, sxx - szz);

    // three.js Y-rotation direction: +θ sends local +X toward -Z. We want
    // local +X to point to (cos α, 0, sin α) in world, so θ = -α.
    root.rotation.y = -polygonAngle;
  }

  // ── West tower block (single mass — the two spires rise from its top) ─────
  const towerX = -NAVE_LEN / 2 - TOWER_D / 2;
  add(hiBox(TOWER_D, TOWER_H, TOWER_W, 24),
      brick, towerX, TOWER_H / 2, 0);

  // Tower corner quoins — 4 tall pale-stone strips
  const quoinW = 0.7;
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(quoinW, TOWER_H, quoinW, 12),
        stone,
        towerX + sx * (TOWER_D / 2 - quoinW / 2), TOWER_H / 2,
        sz * (TOWER_W / 2 - quoinW / 2));
  }

  // Horizontal stone string courses
  for (let y = 5; y < TOWER_H; y += 5) {
    add(hiBox(TOWER_D + 0.25, 0.35, TOWER_W + 0.25, 10),
        stone, towerX, y, 0);
  }

  // ── Lateral wings flanking the tower (west ends of the aisles) ────────────
  // These are the low masses that stick out to the N and S of the main
  // tower block at aisle-height, visible in the front elevation as the
  // wider base beneath the tall tower. They connect the tower front to
  // the aisle walls running east along the nave.
  for (const sz of [-1, 1]) {
    const wingCz = sz * (TOWER_W / 2 + AISLE_W / 2);
    add(hiBox(TOWER_D, AISLE_H, AISLE_W, 12),
        brick, towerX, AISLE_H / 2, wingCz);
    // Corner quoins at the wing's front/back corners
    for (const ex of [-1, 1]) {
      add(hiBox(0.45, AISLE_H, 0.45, 8), stone,
          towerX + ex * (TOWER_D / 2 - 0.22), AISLE_H / 2,
          wingCz - sz * (AISLE_W / 2 - 0.22));
    }
    // Lean-to roof on top of each wing, sloping AWAY from the central tower
    const wingRoofGeo = new THREE.CylinderGeometry(0, 2.5, AISLE_W, 3, 32);
    const wingRoofMesh = new THREE.Mesh(wingRoofGeo, roof);
    wingRoofMesh.rotation.z = Math.PI / 2;
    wingRoofMesh.scale.set(1, TOWER_D / (2.5 * 2), 1);
    wingRoofMesh.position.set(towerX, AISLE_H + 1.25, wingCz);
    root.add(wingRoofMesh);
    track(wingRoofMesh);
  }

  // ── West portal — Gothic pointed-arch doorway ─────────────────────────────
  //
  // Built from a stone frame shape (outer pentagon with an inner pentagon
  // hole) extruded outward from the west wall, plus a slightly-smaller dark
  // door slab set into the opening. The hole gives you the stepped-voussoir
  // recession effect properly — with a solid extrude you'd only ever see
  // the outermost face.
  const westFaceX = towerX - TOWER_D / 2;

  // Pentagonal Gothic-arch outline drawn in the XY plane.
  const buildGothicArch = (w, straightH, peakExtra) => {
    const s = new THREE.Shape();
    s.moveTo(-w / 2, 0);
    s.lineTo(-w / 2, straightH);
    s.lineTo(0, straightH + peakExtra);
    s.lineTo(w / 2, straightH);
    s.lineTo(w / 2, 0);
    s.closePath();
    return s;
  };
  const gothicArchPath = (w, straightH, peakExtra) => {
    const p = new THREE.Path();
    p.moveTo(-w / 2, 0);
    p.lineTo(-w / 2, straightH);
    p.lineTo(0, straightH + peakExtra);
    p.lineTo(w / 2, straightH);
    p.lineTo(w / 2, 0);
    p.closePath();
    return p;
  };

  // Outer stone surround (frame with a pentagonal hole cut through it).
  {
    const outer = buildGothicArch(5.4, 6.2, 2.8);
    outer.holes.push(gothicArchPath(3.6, 5.2, 2.1));
    const geo = new THREE.ExtrudeGeometry(outer, {
      depth: 1.0, bevelEnabled: false, curveSegments: 4,
    });
    const mesh = new THREE.Mesh(geo, stone);
    // Rotate so extrusion axis +Z → -X (out of the tower, westward).
    mesh.rotation.y = -Math.PI / 2;
    mesh.position.set(westFaceX, 0, 0);
    root.add(mesh);
    track(mesh);
  }

  // Inner stepped frame (slightly smaller frame, set back deeper into the
  // opening — visible through the outer hole).
  {
    const middle = buildGothicArch(3.6, 5.2, 2.1);
    middle.holes.push(gothicArchPath(2.8, 4.5, 1.7));
    const geo = new THREE.ExtrudeGeometry(middle, {
      depth: 0.6, bevelEnabled: false, curveSegments: 4,
    });
    const mesh = new THREE.Mesh(geo, stone);
    mesh.rotation.y = -Math.PI / 2;
    mesh.position.set(westFaceX + 0.3, 0, 0);  // recessed 0.3m inside the outer
    root.add(mesh);
    track(mesh);
  }

  // Dark wood door slab (solid pentagonal) behind the inner frame.
  {
    const door = buildGothicArch(2.8, 4.4, 1.6);
    const geo = new THREE.ExtrudeGeometry(door, {
      depth: 0.15, bevelEnabled: false, curveSegments: 4,
    });
    const mesh = new THREE.Mesh(geo, dark);
    mesh.rotation.y = -Math.PI / 2;
    mesh.position.set(westFaceX + 0.8, 0, 0);  // deepest layer
    root.add(mesh);
    track(mesh);
  }

  // Central vertical stone mullion splitting the door
  add(hiBox(0.18, 4.4, 0.22, 3),
      stone, westFaceX + 0.7, 2.2, 0);

  // ── Rose window above the portal ───────────────────────────────────────────
  // Portal goes up to ~9m (outer arch straightH=6.2 + peak 2.8). Put the
  // rose window a bit above that.
  const roseY = 13.5;
  const roseR = 3.0;
  add(new THREE.CircleGeometry(roseR + 0.4, 96),
      stone, westFaceX - 0.03, roseY, 0, 0, -Math.PI / 2);
  add(new THREE.CircleGeometry(roseR, 96),
      glass, westFaceX - 0.05, roseY, 0, 0, -Math.PI / 2);
  // 16 radial tracery spokes
  for (let i = 0; i < 16; i++) {
    const a = (i / 16) * Math.PI * 2;
    const spoke = new THREE.Mesh(
      hiBox(0.14, roseR * 2 - 0.2, 0.22, 6),
      stone,
    );
    spoke.position.set(westFaceX - 0.06, roseY, 0);
    spoke.rotation.x = a;
    root.add(spoke);
    track(spoke);
  }
  // Inner ring
  add(new THREE.TorusGeometry(roseR * 0.55, 0.14, 20, 96),
      stone, westFaceX - 0.07, roseY, 0, 0, -Math.PI / 2);
  add(new THREE.TorusGeometry(roseR * 0.85, 0.12, 16, 96),
      stone, westFaceX - 0.07, roseY, 0, 0, -Math.PI / 2);

  // ── Clock face (above the rose, at tower top) ─────────────────────────────
  const clockY = TOWER_H - 3;
  const clockR = 2.1;
  add(new THREE.CircleGeometry(clockR + 0.4, 64),
      stone, westFaceX - 0.04, clockY, 0, 0, -Math.PI / 2);
  add(new THREE.CircleGeometry(clockR, 64),
      M.glassFront, westFaceX - 0.06, clockY, 0, 0, -Math.PI / 2);
  add(new THREE.TorusGeometry(clockR, 0.12, 16, 64),
      stone, westFaceX - 0.06, clockY, 0, 0, -Math.PI / 2);
  for (let i = 0; i < 12; i++) {
    const a  = (i / 12) * Math.PI * 2;
    const cy = clockY + Math.sin(a) * clockR * 0.82;
    const cz = Math.cos(a) * clockR * 0.82;
    add(hiBox(0.1, 0.28, 0.14, 4),
        stone, westFaceX - 0.07, cy, cz);
  }

  // ── Stepped brick gable rising between the two spires ─────────────────────
  // Each step is a brick row capped with a narrow stone sill.
  for (let step = 0; step < GABLE_STEPS; step++) {
    // Width narrows linearly from the tower width toward a peak
    const widthFrac = 1 - (step / GABLE_STEPS) * 0.85;
    const gw = TOWER_W * widthFrac - 2 * SPIRE_BASE_R * 1.2;  // leave space for spires
    const gy = TOWER_H + step * GABLE_STEP_H + GABLE_STEP_H / 2;
    // Brick step
    add(hiBox(TOWER_D * 0.6, GABLE_STEP_H, Math.max(0.5, gw), 8),
        brick, towerX + TOWER_D * 0.05, gy, 0);
    // Pale stone cap on top of each step (only on the outward edges)
    for (const sz of [-1, 1]) {
      add(hiBox(TOWER_D * 0.62, 0.28, 0.6, 4),
          stone, towerX + TOWER_D * 0.05,
          TOWER_H + step * GABLE_STEP_H + GABLE_STEP_H,
          sz * (Math.max(0.5, gw) / 2 + 0.3));
    }
  }
  // Topmost pinnacle cross on the stepped gable peak
  add(hiBox(0.2, 2.2, 0.2, 6), stone,
      towerX + TOWER_D * 0.05, TOWER_H + GABLE_EXTRA_H + 1.1, 0);
  add(hiBox(1.2, 0.2, 0.2, 4), stone,
      towerX + TOWER_D * 0.05, TOWER_H + GABLE_EXTRA_H + 0.8, 0);

  // ── Twin octagonal spires ──────────────────────────────────────────────────
  //
  // Each spire is built as a cluster of high-tessellation revolutions:
  //
  //   1.  Wide octagonal drum at TOWER_H → TOWER_H+SPIRE_DRUM_H
  //   2.  SPIRE_SECTIONS stacked tapered cylinder sections forming the main
  //       copper-oxide body
  //   3.  A torus ring between every section pair (major face-count sink)
  //   4.  A sharp needle cone at the top
  //   5.  Gilded capstone sphere + stone cross
  //
  // Face budget per spire ≈ 400-500k.
  function buildSpire(cx, cz) {
    // Octagonal drum (smooth cylinder with high radial segs reads copper)
    add(new THREE.CylinderGeometry(SPIRE_BASE_R * 1.05, SPIRE_BASE_R * 1.12,
                                   SPIRE_DRUM_H, SPIRE_RADIAL, 12),
        spireMat, cx, TOWER_H + SPIRE_DRUM_H / 2, cz);

    // On each of the eight octagonal faces, build a gabled pinnacle dormer
    // (small stone niche with a pointed roof and a round stone finial
    // above it). These are the pale elements the user called the "rounded
    // white circles" on the spire — pinnacle ball finials against the
    // copper drum.
    for (let face = 0; face < 8; face++) {
      const a  = (face / 8) * Math.PI * 2;
      const ca = Math.cos(a);
      const sa = Math.sin(a);
      // Niche body — a small stone block pushed outward from the drum face
      const nicheR = SPIRE_BASE_R * 1.16;
      const nx     = cx + ca * nicheR;
      const nz     = cz + sa * nicheR;
      const nicheY = TOWER_H + SPIRE_DRUM_H * 0.55;
      const niche  = add(hiBox(0.55, SPIRE_DRUM_H * 0.75, 0.95, 8),
                         stone, nx, nicheY, nz);
      niche.rotation.y = a;

      // Pointed gable roof on the dormer — 3-radial cylinder rotated to be
      // a triangular prism sticking out from the drum face.
      const gableH = 0.9;
      const gable  = new THREE.Mesh(
        new THREE.CylinderGeometry(0, 0.55, gableH, 3, 4),
        spireMat,
      );
      gable.position.set(nx, nicheY + SPIRE_DRUM_H * 0.4 + gableH / 2, nz);
      gable.rotation.y = a;
      root.add(gable);
      track(gable);

      // Pinnacle ball finial on top — the "rounded white circle" from the
      // blueprint. Small high-segment sphere in pale stone.
      const ballR = 0.32;
      const ballY = nicheY + SPIRE_DRUM_H * 0.4 + gableH + ballR;
      add(new THREE.SphereGeometry(ballR, 32, 24), stone, nx, ballY, nz);
      // Thin stone spike through the ball for extra definition
      add(hiBox(0.08, 0.55, 0.08, 3), stone, nx, ballY + 0.25, nz);
    }

    // Main tapered spire sections
    let baseY = TOWER_H + SPIRE_DRUM_H;
    let baseR = SPIRE_BASE_R;
    const sectionH = SPIRE_MAIN_H / SPIRE_SECTIONS;
    for (let s = 0; s < SPIRE_SECTIONS; s++) {
      // Each section tapers ~8% — gradual for early sections, sharper later
      const taper = 0.92 - (s / SPIRE_SECTIONS) * 0.06;
      const topR  = baseR * taper;
      add(new THREE.CylinderGeometry(topR, baseR, sectionH, SPIRE_RADIAL, 14),
          spireMat, cx, baseY + sectionH / 2, cz);
      // Decorative stone ring between sections — highest-poly element
      const ring = new THREE.Mesh(
        new THREE.TorusGeometry(baseR * 1.02, 0.17, SPIRE_RING_RADIAL, SPIRE_RING_TUBULAR),
        stone,
      );
      ring.position.set(cx, baseY + 0.05, cz);
      ring.rotation.x = Math.PI / 2;
      root.add(ring);
      track(ring);

      baseY += sectionH;
      baseR  = topR;
    }

    // Sharp needle tip
    add(new THREE.CylinderGeometry(0.05, baseR, SPIRE_NEEDLE_H, SPIRE_RADIAL, 16),
        spireMat, cx, baseY + SPIRE_NEEDLE_H / 2, cz);
    baseY += SPIRE_NEEDLE_H;

    // Capstone sphere (gilded stone)
    add(new THREE.SphereGeometry(0.55, 48, 32),
        stone, cx, baseY + 0.55, cz);
    // Cross on top
    add(hiBox(0.18, 1.8, 0.18, 6), stone, cx, baseY + 1.6, cz);
    add(hiBox(0.9, 0.18, 0.18, 4), stone, cx, baseY + 1.3, cz);
  }

  buildSpire(towerX, -SPIRE_SEP_Z);
  buildSpire(towerX,  SPIRE_SEP_Z);

  // ── Nave (central body) ────────────────────────────────────────────────────
  const naveCX = 0;   // nave centred on local origin
  add(hiBox(NAVE_LEN, NAVE_H, NAVE_W, 32),
      brick, naveCX, NAVE_H / 2, 0);

  // Nave corner quoins
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.55, NAVE_H, 0.55, 12), stone,
        naveCX + sx * (NAVE_LEN / 2 - 0.28), NAVE_H / 2,
        sz * (NAVE_W / 2 - 0.28));
  }

  // Clerestory arched windows on both sides of the nave
  for (let b = 0; b < CLERESTORY_BAYS; b++) {
    const bx = naveCX - NAVE_LEN / 2 + (b + 0.5) * (NAVE_LEN / CLERESTORY_BAYS);
    for (const sz of [-1, 1]) {
      const wy = NAVE_H * 0.72;
      // Glass body
      add(hiBox(1.6, 3.6, 0.28, 8),
          glass, bx, wy, sz * (NAVE_W / 2 + 0.14));
      // Pointed-arch cylinder top
      add(new THREE.CylinderGeometry(0.8, 0.8, 0.28, 40, 1, false, 0, Math.PI),
          glass, bx, wy + 1.8, sz * (NAVE_W / 2 + 0.14), 0, 0, Math.PI / 2);
      // Stone frame
      add(hiBox(2.0, 0.18, 0.38, 6),
          stone, bx, wy - 1.9, sz * (NAVE_W / 2 + 0.15));
      // Sill
      add(hiBox(2.1, 0.22, 0.4, 4),
          stone, bx, wy - 2.0, sz * (NAVE_W / 2 + 0.16));
    }
  }

  // Buttresses between bays on the nave wall
  for (let b = 0; b <= CLERESTORY_BAYS; b++) {
    const bx = naveCX - NAVE_LEN / 2 + b * (NAVE_LEN / CLERESTORY_BAYS);
    for (const sz of [-1, 1]) {
      add(hiBox(0.85, NAVE_H, 0.9, 16),
          brick, bx, NAVE_H / 2, sz * (NAVE_W / 2 + 0.45));
      add(hiBox(1.0, 0.35, 1.05, 8),
          stone, bx, NAVE_H, sz * (NAVE_W / 2 + 0.45));
    }
  }

  // ── Side aisles ────────────────────────────────────────────────────────────
  for (const sz of [-1, 1]) {
    const aCz = sz * (NAVE_W / 2 + AISLE_W / 2);
    add(hiBox(NAVE_LEN, AISLE_H, AISLE_W, 16),
        brick, naveCX, AISLE_H / 2, aCz);

    // Aisle corner quoins
    for (const ex of [-1, 1]) {
      add(hiBox(0.45, AISLE_H, 0.45, 8), stone,
          naveCX + ex * (NAVE_LEN / 2 - 0.22), AISLE_H / 2,
          aCz - sz * (AISLE_W / 2 - 0.22));
    }

    // Arched windows in the aisles — same bay count as clerestory.
    // On the SOUTH side (sz === +1), replace bay index 4 with a Gothic
    // side door between the neighbouring windows.
    const sideDoorBay = 4;
    const aisleOuterZ = sz * (NAVE_W / 2 + AISLE_W);
    for (let b = 0; b < CLERESTORY_BAYS; b++) {
      const bx = naveCX - NAVE_LEN / 2 + (b + 0.5) * (NAVE_LEN / CLERESTORY_BAYS);

      if (sz === 1 && b === sideDoorBay) {
        // ── South aisle entrance — small Gothic pointed-arch door ──
        // Outer frame (pentagon with a pentagon hole cut through it) is
        // the stone surround; inner solid extrude is the dark wood door.
        const outerW = 3.0, outerSH = 3.4, outerPk = 1.4;
        const holeW  = 2.2, holeSH  = 2.8, holePk  = 1.1;
        const doorW  = 2.0, doorSH  = 2.7, doorPk  = 1.0;
        {
          const frame = buildGothicArch(outerW, outerSH, outerPk);
          frame.holes.push(gothicArchPath(holeW, holeSH, holePk));
          const geo = new THREE.ExtrudeGeometry(frame, {
            depth: 0.5, bevelEnabled: false, curveSegments: 4,
          });
          const mesh = new THREE.Mesh(geo, stone);
          // Shape in XY, extrudes +Z → already faces outward on the south
          // side. Position with the back cap flush with the aisle wall.
          mesh.position.set(bx, 0, aisleOuterZ);
          root.add(mesh);
          track(mesh);
        }
        {
          const door = buildGothicArch(doorW, doorSH, doorPk);
          const geo = new THREE.ExtrudeGeometry(door, {
            depth: 0.12, bevelEnabled: false, curveSegments: 4,
          });
          const mesh = new THREE.Mesh(geo, dark);
          mesh.position.set(bx, 0, aisleOuterZ - 0.15);   // recessed back into the wall
          root.add(mesh);
          track(mesh);
        }
        // Central vertical mullion
        add(hiBox(0.12, doorSH + doorPk - 0.2, 0.15, 3),
            stone, bx, (doorSH + doorPk - 0.2) / 2,
            aisleOuterZ - 0.02);
        continue;
      }

      const wy = AISLE_H * 0.52;
      add(hiBox(1.3, 2.8, 0.28, 6),
          glass, bx, wy, sz * (NAVE_W / 2 + AISLE_W + 0.14));
      add(new THREE.CylinderGeometry(0.65, 0.65, 0.28, 32, 1, false, 0, Math.PI),
          glass, bx, wy + 1.4,
          sz * (NAVE_W / 2 + AISLE_W + 0.14), 0, 0, Math.PI / 2);
      add(hiBox(1.65, 0.15, 0.35, 4),
          stone, bx, wy - 1.4,
          sz * (NAVE_W / 2 + AISLE_W + 0.15));
    }

    // Aisle lean-to roof
    const leanGeo = new THREE.CylinderGeometry(
      0, 2.5, AISLE_W, 3, 32);
    const leanMesh = new THREE.Mesh(leanGeo, roof);
    leanMesh.rotation.z = Math.PI / 2;
    leanMesh.scale.set(1, NAVE_LEN / (2.5 * 2), 1);
    leanMesh.position.set(naveCX, AISLE_H + 1.25, aCz);
    root.add(leanMesh);
    track(leanMesh);
  }

  // ── Nave gable roof — two flat planes + instanced tile pattern ────────────
  const pitchRad  = Math.atan2(NAVE_ROOF_H, NAVE_W / 2);
  const slantLen  = Math.sqrt((NAVE_W / 2) ** 2 + NAVE_ROOF_H ** 2);

  // Flat slope bases (one per side) — big single quads that give each side
  // of the roof a solid underlying colour before the tile instances sit on
  // top. Built from PlaneGeometry which is only 2 triangles.
  for (const sz of [-1, 1]) {
    const planeGeo = new THREE.PlaneGeometry(NAVE_LEN, slantLen, 1, 1);
    const planeMesh = new THREE.Mesh(planeGeo, roof);
    // The plane starts in the XY-plane facing +Z. We need it tilted so
    // its long edge runs along X and it leans from the eave outward up to
    // the ridge. Apply Euler XYZ: first rotate around X so it stands up
    // at the pitch angle, then orient outward via sign on Z.
    planeMesh.rotation.x = -Math.PI / 2;                    // lay flat along XZ
    planeMesh.rotation.y = 0;
    planeMesh.rotation.z = 0;
    planeMesh.rotation.x = -Math.PI / 2 + sz * pitchRad;    // tilt up to pitch
    // Position: centred along X, half-height + NAVE_H in Y, halfway along slope in Z
    const midY = NAVE_H + (slantLen / 2) * Math.sin(pitchRad);
    const midZ = sz * (NAVE_W / 2 - (slantLen / 2) * Math.cos(pitchRad));
    planeMesh.position.set(naveCX, midY, midZ);
    root.add(planeMesh);
    track(planeMesh);
  }

  // Close the gable ends (triangles) at the east and west of the nave so the
  // roof doesn't look hollow.
  for (const ex of [-1, 1]) {
    const triShape = new THREE.Shape();
    triShape.moveTo(-NAVE_W / 2, 0);
    triShape.lineTo( NAVE_W / 2, 0);
    triShape.lineTo(0, NAVE_ROOF_H);
    triShape.lineTo(-NAVE_W / 2, 0);
    const triGeo = new THREE.ShapeGeometry(triShape);
    const triMesh = new THREE.Mesh(triGeo, brick);
    triMesh.position.set(naveCX + ex * (NAVE_LEN / 2), NAVE_H, 0);
    triMesh.rotation.y = ex > 0 ? -Math.PI / 2 : Math.PI / 2;
    root.add(triMesh);
    track(triMesh);
  }

  // Instanced tile grid — one instance per ceramic tile on each roof slope.
  // Single draw call even with thousands of instances.
  {
    const tileGeo = new THREE.BoxGeometry(
      (NAVE_LEN / TILE_COLS) * 0.95,
      0.10,
      (slantLen / TILE_ROWS) * 0.92,
      2, 2, 2,
    );
    const tileMat = M.churchRoof;
    const totalTiles = TILE_COLS * TILE_ROWS * 2;  // both slopes
    const inst = new THREE.InstancedMesh(tileGeo, tileMat, totalTiles);

    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      const tilt = -sz * pitchRad;
      for (let r = 0; r < TILE_ROWS; r++) {
        // u runs 0 → slantLen from the eave up to the ridge
        const u = (r + 0.5) * (slantLen / TILE_ROWS);
        const y = NAVE_H + u * Math.sin(pitchRad);
        const z = sz * (NAVE_W / 2 - u * Math.cos(pitchRad));
        for (let c = 0; c < TILE_COLS; c++) {
          const x = naveCX - NAVE_LEN / 2 + (c + 0.5) * (NAVE_LEN / TILE_COLS);
          tmp.position.set(x, y, z);
          tmp.rotation.set(tilt, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, totalTiles);
  }

  // ── Ridgeline frieze ───────────────────────────────────────────────────────
  const knobCount = 180;
  for (let i = 0; i < knobCount; i++) {
    const bx = naveCX - NAVE_LEN / 2 + (i + 0.5) * (NAVE_LEN / knobCount);
    add(hiBox(0.32, 0.55, 0.32, 4),
        stone, bx, NAVE_H + NAVE_ROOF_H + 0.27, 0);
  }

  // ── Stepped-gable apse (east end) ──────────────────────────────────────────
  const apseX = NAVE_LEN / 2 + APSE_D / 2;
  // Main apse block
  add(hiBox(APSE_D, NAVE_H, APSE_W, 16),
      brick, apseX, NAVE_H / 2, 0);
  // Apse corner quoins
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.5, NAVE_H, 0.5, 10), stone,
        apseX + sx * (APSE_D / 2 - 0.25), NAVE_H / 2,
        sz * (APSE_W / 2 - 0.25));
  }
  // Stepped gable rising above NAVE_H on the east face
  for (let step = 0; step < APSE_GABLE_STEPS; step++) {
    const widthFrac = 1 - (step / APSE_GABLE_STEPS) * 0.85;
    add(hiBox(APSE_D * 0.6, GABLE_STEP_H, APSE_W * widthFrac, 8),
        brick, apseX, NAVE_H + step * GABLE_STEP_H + GABLE_STEP_H / 2, 0);
    // Stone caps on the step corners
    for (const sz of [-1, 1]) {
      add(hiBox(APSE_D * 0.62, 0.28, 0.55, 4),
          stone, apseX,
          NAVE_H + step * GABLE_STEP_H + GABLE_STEP_H,
          sz * (APSE_W * widthFrac / 2 + 0.28));
    }
  }
  // Apse east-wall windows (three tall arched windows on the flat east face)
  const apseEastX = apseX + APSE_D / 2 + 0.02;
  for (let i = -1; i <= 1; i++) {
    const wz = i * (APSE_W / 4);
    add(hiBox(0.38, 5, 1.5, 6),
        glass, apseEastX, NAVE_H * 0.55, wz);
    add(new THREE.CylinderGeometry(0.75, 0.75, 0.38, 32, 1, false, 0, Math.PI),
        glass, apseEastX, NAVE_H * 0.55 + 2.5, wz, 0, 0, Math.PI / 2);
    add(hiBox(0.45, 0.15, 1.7, 4),
        stone, apseEastX, NAVE_H * 0.55 - 2.5, wz);
  }
  // Final pinnacle cross on the apse gable peak
  add(hiBox(0.15, 1.8, 0.15, 4), stone,
      apseX, NAVE_H + APSE_GABLE_STEPS * GABLE_STEP_H + 0.9, 0);
  add(hiBox(0.9, 0.15, 0.15, 3), stone,
      apseX, NAVE_H + APSE_GABLE_STEPS * GABLE_STEP_H + 0.6, 0);

  // ── Landmark metadata consumed by view.js ─────────────────────────────────
  //
  // customFootprint: simplified outline of the model's ground-plane silhouette,
  //   in LOCAL coordinates (before root.rotation.y). view.js transforms this
  //   to world space when drawing the selection highlight so it hugs the
  //   actual 3D shape instead of the OSM rectangle.
  //
  // baseHeight: natural height of the tallest part of the model (spire tip).
  //   Used by _applyHeights() to compute scale.y for height modes.
  const mainSouthZ = NAVE_W / 2 + AISLE_W;     //  11.5
  const mainWestX  = -NAVE_LEN / 2 - TOWER_D;  // -32.5
  const mainEastX  =  NAVE_LEN / 2;            //  26
  const footEastX  = mainEastX + APSE_D;       //  35 — apse protrusion
  const apseHalfZ  = APSE_W / 2;               //   7
  root.userData.customFootprint = [
    [mainWestX,  mainSouthZ],
    [mainEastX,  mainSouthZ],
    [mainEastX,  apseHalfZ],
    [footEastX,  apseHalfZ],
    [footEastX, -apseHalfZ],
    [mainEastX, -apseHalfZ],
    [mainEastX, -mainSouthZ],
    [mainWestX, -mainSouthZ],
  ];
  root.userData.baseHeight = TOWER_H + SPIRE_DRUM_H + SPIRE_MAIN_H + SPIRE_NEEDLE_H + 2.5;

  // ── Log the final face count so the user can see what we're shipping ──
  // eslint-disable-next-line no-console
  console.log(
    `[vaxjo_cathedral] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Teleborg Castle (Teleborgs slott) ────────────────────────────────────────
//
// A romantic revival castle from 1900 on the southern shore of Lake Trummen in
// Växjö. Built for Count Fredrik Bonde as a wedding gift to his wife, today
// used as a hotel and conference venue (Linnaeus University property). The
// silhouette is defined by:
//
//   - a stone main block with steep pitched slate/copper roof
//   - a tall round corner tower with a conical copper spire
//   - a shorter secondary tower/turret with its own cone roof
//   - multiple dormers and chimneys on the main roof
//   - a raised arched entrance porch with an external double staircase
//   - rows of arched stone-framed windows
//
// Face budget ~400-600k — primarily the tessellated corner tower cylinders,
// their cone roofs, a dense stone-course wall tessellation, and the usual
// instanced-tile roof pattern.

function buildTeleborgCastle(root, cell, ctx) {
  const { THREE, M } = ctx;

  // ── PCA rotation so the castle aligns with the OSM footprint orientation
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    const polygonAngle = 0.5 * Math.atan2(2 * sxz, sxx - szz);
    root.rotation.y = -polygonAngle;
  }

  // ── Dimensions (metres) ──
  const MAIN_LEN    = 22;   // along polygon long axis (local +X)
  const MAIN_W      = 13;   // transverse (local +Z)
  const MAIN_H      = 14;   // 3 floors to cornice
  const ROOF_H      = 7;    // steep pitched roof rise

  const TOWER_R     = 3.2;  // main tall corner tower radius
  const TOWER_H     = 26;
  const TOWER_CONE_H = 9;

  const TURRET_R     = 2.2;  // shorter secondary turret
  const TURRET_H     = 19;
  const TURRET_CONE_H = 6.5;

  const PORCH_W      = 5.5;  // entrance porch projecting south
  const PORCH_D      = 4.5;
  const PORCH_H      = 7.5;
  const STAIR_STEPS  = 9;

  const TILE_COLS    = 80;
  const TILE_ROWS    = 20;

  // ── Materials ──
  const stone    = M.castleStone;
  const trim     = M.castleTrim;
  const roofMat  = M.castleRoof;
  const coneMat  = M.castleRoof;
  const dark     = M.roofBrown;
  const glass    = M.glass;

  // Face-count tracker
  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };

  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Main block (stone keep) ──
  add(hiBox(MAIN_LEN, MAIN_H, MAIN_W, 22),
      stone, 0, MAIN_H / 2, 0);

  // Stone cornice ring at the top of the walls
  add(hiBox(MAIN_LEN + 0.4, 0.55, MAIN_W + 0.4, 8),
      trim, 0, MAIN_H + 0.27, 0);

  // Corner quoins (4 tall pale-stone strips)
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.55, MAIN_H, 0.55, 12),
        trim,
        sx * (MAIN_LEN / 2 - 0.28), MAIN_H / 2,
        sz * (MAIN_W / 2 - 0.28));
  }

  // String courses (horizontal stone bands between floors)
  for (const y of [4.5, 9.0]) {
    add(hiBox(MAIN_LEN + 0.1, 0.25, MAIN_W + 0.1, 8),
        trim, 0, y, 0);
  }

  // ── Arched windows (3 rows × 7 bays on the long facades) ──
  const winBays = 7;
  for (let row = 0; row < 3; row++) {
    const wy = 2.4 + row * 4.5;
    for (let b = 0; b < winBays; b++) {
      const wx = -MAIN_LEN / 2 + (b + 0.5) * (MAIN_LEN / winBays);
      for (const sz of [-1, 1]) {
        const wz = sz * (MAIN_W / 2 + 0.13);
        // Glass body
        add(hiBox(1.0, 2.0, 0.25, 6), glass, wx, wy, wz);
        // Arched cylinder top
        add(new THREE.CylinderGeometry(0.5, 0.5, 0.25, 24, 1, false, 0, Math.PI),
            glass, wx, wy + 1.0, wz, 0, 0, Math.PI / 2);
        // Stone frame
        add(hiBox(1.25, 0.12, 0.32, 4), trim, wx, wy - 1.0, wz);
      }
    }
  }

  // Arched windows on the short walls (2 bays × 3 rows)
  for (let row = 0; row < 3; row++) {
    const wy = 2.4 + row * 4.5;
    for (let b = 0; b < 2; b++) {
      const wz = -MAIN_W / 2 + (b + 0.5) * (MAIN_W / 2);
      for (const sx of [-1, 1]) {
        const wx = sx * (MAIN_LEN / 2 + 0.13);
        add(hiBox(0.25, 2.0, 1.0, 6), glass, wx, wy, wz);
        add(hiBox(0.32, 0.12, 1.25, 4), trim, wx, wy - 1.0, wz);
      }
    }
  }

  // ── Steep pitched main roof ──
  const pitch    = Math.atan2(ROOF_H, MAIN_W / 2);
  const slantLen = Math.sqrt((MAIN_W / 2) ** 2 + ROOF_H ** 2);

  for (const sz of [-1, 1]) {
    const planeGeo = new THREE.PlaneGeometry(MAIN_LEN, slantLen, 1, 1);
    const planeMesh = new THREE.Mesh(planeGeo, roofMat);
    planeMesh.rotation.x = -Math.PI / 2 + sz * pitch;
    const midY = MAIN_H + (slantLen / 2) * Math.sin(pitch);
    const midZ = sz * (MAIN_W / 2 - (slantLen / 2) * Math.cos(pitch));
    planeMesh.position.set(0, midY, midZ);
    root.add(planeMesh);
    track(planeMesh);
  }

  // Gable ends (triangular faces closing the roof at the short ends)
  for (const ex of [-1, 1]) {
    const tri = new THREE.Shape();
    tri.moveTo(-MAIN_W / 2, 0);
    tri.lineTo( MAIN_W / 2, 0);
    tri.lineTo(0, ROOF_H);
    tri.lineTo(-MAIN_W / 2, 0);
    const triMesh = new THREE.Mesh(new THREE.ShapeGeometry(tri), stone);
    triMesh.position.set(ex * (MAIN_LEN / 2), MAIN_H, 0);
    triMesh.rotation.y = ex > 0 ? -Math.PI / 2 : Math.PI / 2;
    root.add(triMesh);
    track(triMesh);
  }

  // Instanced copper roof tiles
  {
    const tileGeo = new THREE.BoxGeometry(
      (MAIN_LEN / TILE_COLS) * 0.95,
      0.08,
      (slantLen / TILE_ROWS) * 0.92,
      2, 2, 2,
    );
    const tileCount = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, roofMat, tileCount);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      const tilt = -sz * pitch;
      for (let r = 0; r < TILE_ROWS; r++) {
        const u = (r + 0.5) * (slantLen / TILE_ROWS);
        const y = MAIN_H + u * Math.sin(pitch);
        const z = sz * (MAIN_W / 2 - u * Math.cos(pitch));
        for (let c = 0; c < TILE_COLS; c++) {
          const x = -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / TILE_COLS);
          tmp.position.set(x, y, z);
          tmp.rotation.set(tilt, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, tileCount);
  }

  // ── Main corner tower (NW of the main block) ──
  // Positioned so half of the tower's footprint overlaps the main block
  // corner — the real castle has a "built-into" tower, not a free-standing
  // one stuck to the wall.
  const towerCX = -MAIN_LEN / 2 + TOWER_R * 0.5;
  const towerCZ = -MAIN_W / 2   + TOWER_R * 0.5;

  add(new THREE.CylinderGeometry(TOWER_R, TOWER_R * 1.05, TOWER_H, 64, 18),
      stone, towerCX, TOWER_H / 2, towerCZ);

  // Stone string courses on the tower
  for (const y of [4.5, 9.0, 13.5, 18.0, 22.5]) {
    add(new THREE.TorusGeometry(TOWER_R + 0.05, 0.18, 16, 64),
        trim, towerCX, y, towerCZ, Math.PI / 2, 0, 0);
  }

  // Tower arched windows — 5 levels × 4 directions each
  for (let lvl = 0; lvl < 5; lvl++) {
    const wy = 2.8 + lvl * 4.5;
    for (let dir = 0; dir < 8; dir++) {
      const a  = (dir / 8) * Math.PI * 2;
      const wx = towerCX + Math.cos(a) * (TOWER_R + 0.12);
      const wz = towerCZ + Math.sin(a) * (TOWER_R + 0.12);
      const win = add(hiBox(0.6, 1.3, 0.24, 4), glass, wx, wy, wz, 0, a, 0);
      win.rotation.y = a;
    }
  }

  // Conical copper roof on top of the tower
  add(new THREE.ConeGeometry(TOWER_R * 1.06, TOWER_CONE_H, 64, 12),
      coneMat, towerCX, TOWER_H + TOWER_CONE_H / 2, towerCZ);

  // Capstone sphere + spike on the cone
  add(new THREE.SphereGeometry(0.4, 24, 18),
      trim, towerCX, TOWER_H + TOWER_CONE_H + 0.4, towerCZ);
  add(hiBox(0.12, 1.5, 0.12, 3),
      trim, towerCX, TOWER_H + TOWER_CONE_H + 1.2, towerCZ);

  // ── Secondary turret (SE corner) ──
  const turretCX = MAIN_LEN / 2 - TURRET_R * 0.5;
  const turretCZ = MAIN_W / 2   - TURRET_R * 0.5;

  add(new THREE.CylinderGeometry(TURRET_R, TURRET_R * 1.05, TURRET_H, 48, 14),
      stone, turretCX, TURRET_H / 2, turretCZ);

  // Turret string courses
  for (const y of [4.5, 9.0, 13.5]) {
    add(new THREE.TorusGeometry(TURRET_R + 0.04, 0.14, 12, 48),
        trim, turretCX, y, turretCZ, Math.PI / 2, 0, 0);
  }

  // Turret windows — 3 levels × 6 directions
  for (let lvl = 0; lvl < 3; lvl++) {
    const wy = 3.0 + lvl * 4.5;
    for (let dir = 0; dir < 6; dir++) {
      const a  = (dir / 6) * Math.PI * 2;
      const wx = turretCX + Math.cos(a) * (TURRET_R + 0.12);
      const wz = turretCZ + Math.sin(a) * (TURRET_R + 0.12);
      const win = add(hiBox(0.5, 1.1, 0.24, 3), glass, wx, wy, wz);
      win.rotation.y = a;
    }
  }

  // Turret cone
  add(new THREE.ConeGeometry(TURRET_R * 1.06, TURRET_CONE_H, 48, 10),
      coneMat, turretCX, TURRET_H + TURRET_CONE_H / 2, turretCZ);
  add(new THREE.SphereGeometry(0.32, 20, 16),
      trim, turretCX, TURRET_H + TURRET_CONE_H + 0.32, turretCZ);

  // ── Dormers on the main roof (4 evenly spaced along the south slope) ──
  const dormerCount = 4;
  for (let d = 0; d < dormerCount; d++) {
    const dx = -MAIN_LEN / 2 + (d + 0.5) * (MAIN_LEN / dormerCount);
    const dy = MAIN_H + ROOF_H * 0.35;
    // Dormer body
    add(hiBox(1.6, 1.4, 1.6, 4), stone, dx, dy, MAIN_W / 2 - 0.8);
    // Small arched window
    add(hiBox(0.8, 1.0, 0.2, 3),
        glass, dx, dy, MAIN_W / 2 - 0.8 + 0.8);
    // Mini gable roof on the dormer
    const dormerRoof = new THREE.Mesh(
      new THREE.CylinderGeometry(0, 1.0, 0.9, 3),
      coneMat,
    );
    dormerRoof.rotation.z = Math.PI / 2;
    dormerRoof.position.set(dx, dy + 1.2, MAIN_W / 2 - 0.8);
    root.add(dormerRoof);
    track(dormerRoof);
  }

  // ── Chimneys — 5 stone chimneys rising from the roof ridge ──
  const chimneyCount = 5;
  for (let c = 0; c < chimneyCount; c++) {
    const cx = -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / chimneyCount);
    add(hiBox(0.9, 3.0, 0.9, 6), stone, cx, MAIN_H + ROOF_H + 1.5, 0);
    add(hiBox(1.1, 0.3, 1.1, 4), trim, cx, MAIN_H + ROOF_H + 3.0, 0);
  }

  // ── Raised entrance porch (south facade, central) ──
  const porchZ  = MAIN_W / 2 + PORCH_D / 2;
  add(hiBox(PORCH_W, PORCH_H, PORCH_D, 10),
      stone, 0, PORCH_H / 2, porchZ);
  // Porch cornice
  add(hiBox(PORCH_W + 0.2, 0.3, PORCH_D + 0.2, 4),
      trim, 0, PORCH_H, porchZ);
  // Small pitched roof on the porch
  const porchRoof = new THREE.Mesh(
    new THREE.CylinderGeometry(0, PORCH_W * 0.55, 1.8, 3),
    coneMat,
  );
  porchRoof.rotation.z = Math.PI / 2;
  porchRoof.scale.set(1, (PORCH_D * 1.1) / (PORCH_W * 0.55 * 2), 1);
  porchRoof.position.set(0, PORCH_H + 0.9, porchZ);
  root.add(porchRoof);
  track(porchRoof);

  // Arched entrance doorway (simple Gothic arch + dark wooden door)
  {
    const doorShape = new THREE.Shape();
    doorShape.moveTo(-1.4, 0);
    doorShape.lineTo(-1.4, 3.0);
    doorShape.lineTo(0, 4.2);
    doorShape.lineTo(1.4, 3.0);
    doorShape.lineTo(1.4, 0);
    doorShape.closePath();
    const holePath = new THREE.Path();
    holePath.moveTo(-1.1, 0.2);
    holePath.lineTo(-1.1, 2.7);
    holePath.lineTo(0, 3.7);
    holePath.lineTo(1.1, 2.7);
    holePath.lineTo(1.1, 0.2);
    holePath.closePath();
    doorShape.holes.push(holePath);

    const frame = new THREE.Mesh(
      new THREE.ExtrudeGeometry(doorShape, {
        depth: 0.4, bevelEnabled: false, curveSegments: 4,
      }),
      trim,
    );
    frame.position.set(0, PORCH_H * 0.1, porchZ + PORCH_D / 2);
    root.add(frame);
    track(frame);

    // Dark door slab (solid, behind the hole)
    const doorSlab = new THREE.Shape();
    doorSlab.moveTo(-1.05, 0.25);
    doorSlab.lineTo(-1.05, 2.7);
    doorSlab.lineTo(0, 3.65);
    doorSlab.lineTo(1.05, 2.7);
    doorSlab.lineTo(1.05, 0.25);
    doorSlab.closePath();
    const slabMesh = new THREE.Mesh(
      new THREE.ExtrudeGeometry(doorSlab, {
        depth: 0.15, bevelEnabled: false, curveSegments: 4,
      }),
      dark,
    );
    slabMesh.position.set(0, PORCH_H * 0.1, porchZ + PORCH_D / 2 - 0.08);
    root.add(slabMesh);
    track(slabMesh);
  }

  // ── External double staircase leading up to the porch ──
  // Two symmetric flights meeting at the porch landing.
  const stairRise = PORCH_H * 0.1 + 0.2;   // landing height
  for (let side = -1; side <= 1; side += 2) {
    for (let s = 0; s < STAIR_STEPS; s++) {
      const t    = s / STAIR_STEPS;
      const stpY = (s + 0.5) * (stairRise / STAIR_STEPS);
      const stpZ = porchZ + PORCH_D / 2 + 0.5 + (1 - t) * 4.5;
      const stpX = side * (PORCH_W / 2 - 1.2 + t * 1.5);
      add(hiBox(2.0, 0.25, 0.9, 3),
          stone, stpX, stpY, stpZ);
    }
  }
  // Stair side walls (balustrades)
  for (const sx of [-1, 1]) {
    add(hiBox(0.3, 1.5, 5.0, 6),
        trim, sx * (PORCH_W / 2 - 0.3), 1.0, porchZ + PORCH_D / 2 + 3);
  }

  // ── Landmark metadata consumed by view.js ─────────────────────────────────
  // Footprint outline (local coords, before rotation): main block + porch.
  root.userData.customFootprint = [
    [-MAIN_LEN / 2, -MAIN_W / 2],
    [ MAIN_LEN / 2, -MAIN_W / 2],
    [ MAIN_LEN / 2,  MAIN_W / 2],
    [ PORCH_W / 2,   MAIN_W / 2],
    [ PORCH_W / 2,   MAIN_W / 2 + PORCH_D],
    [-PORCH_W / 2,   MAIN_W / 2 + PORCH_D],
    [-PORCH_W / 2,   MAIN_W / 2],
    [-MAIN_LEN / 2,  MAIN_W / 2],
  ];
  root.userData.baseHeight = TOWER_H + TOWER_CONE_H + 2;   // spike top

  // eslint-disable-next-line no-console
  console.log(
    `[teleborg_castle] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Kronoberg Castle Ruin (Kronobergs slottsruin) ───────────────────────────
//
// Medieval stone castle from ~1444, abandoned and partially ruined since the
// 1700s. Sits on Kronobergsholm island in Lake Helgasjön, ~7 km north of
// Växjö city centre. Reference photos: rectangular rubble-stone enclosure
// with four round corner bastions, ragged/worn wall tops (no roof), small
// square windows scattered along the walls, a small gatehouse on the south
// side with a wooden plank entrance bridge crossing the shallow moat.
//
// Key modelling choices vs a fully-restored castle:
//   - walls are SOLID boxes with a jagged top edge (varied height per
//     segment to read as "ruined")
//   - no roof at all — the courtyard is open to the sky
//   - corner bastions are LOW cylinders (partially collapsed, ~70% of
//     the wall height)
//   - interior courtyard has a grassy green pad and a couple of trees
//   - exterior wooden plank bridge with handrails extending south toward
//     where the shore would be
//
// Face budget: ~100-200k. Simpler than the cathedral/Teleborg because
// there's no roof, no spires, no complex tracery — mostly tessellated
// boxes and high-segment cylinder bastions.

function buildKronobergRuin(root, cell, ctx) {
  const { THREE, M } = ctx;

  // ── PCA rotation so the castle's long axis aligns with the OSM polygon
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) — scaled from the aerial reference photo ──
  const ENCL_LEN   = 32;    // E-W (long axis)
  const ENCL_W     = 24;    // N-S
  const WALL_H     = 7;     // average wall height
  const WALL_T     = 2.2;   // wall thickness

  const BAST_R     = 3.4;   // corner bastion radius
  const BAST_H     = 6.2;   // bastion height (slightly lower than walls — ruined)

  const GATE_W     = 3.2;   // gatehouse width
  const GATE_D     = 2.0;   // gatehouse depth (projecting outward)
  const GATE_H     = 5.0;   // gatehouse height

  const BRIDGE_LEN = 9.0;   // length of the wooden entrance bridge
  const BRIDGE_W   = 1.8;
  const BRIDGE_H   = 0.2;
  const BRIDGE_Y   = 0.4;   // bridge deck height above ground

  const WINDOW_W   = 0.45;  // small square embrasure windows
  const WINDOW_H   = 0.6;
  const WINDOWS_PER_WALL = 5;

  // ── Materials ──
  const stone   = M.ruinStone;
  const moss    = M.ruinMoss;
  const wood    = M.bridgeWood;
  const grass   = M.park;
  const foliage = M.foliage;
  const trunk   = M.trunk;

  // ── Face counter ──
  let faceCount = 0;
  const track = (m, n = 1) => {
    const g = m.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * n;
    return m;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // Deterministic pseudo-random so the ragged wall top is stable across
  // reloads but still looks varied.
  let _rnd = 1.0;
  const rand = () => {
    _rnd = (_rnd * 9301 + 49297) % 233280;
    return _rnd / 233280;
  };

  // ── Grassy courtyard pad ──
  add(hiBox(ENCL_LEN - WALL_T * 2 + 0.4, 0.12,
            ENCL_W   - WALL_T * 2 + 0.4, 4),
      grass, 0, 0.06, 0);

  // ── Four wall segments (N/S/E/W) built as a SERIES of ragged boxes ──
  // Each wall is composed of small box chunks with slightly varied heights
  // so the top reads as worn/ruined rather than perfectly level.
  const CHUNKS_PER_WALL = 16;

  const buildRaggedWall = (len, tx, tz, horizontal) => {
    for (let i = 0; i < CHUNKS_PER_WALL; i++) {
      const chunkLen = len / CHUNKS_PER_WALL;
      // Vary height between 85% and 110% of WALL_H for jagged silhouette
      const h = WALL_H * (0.85 + rand() * 0.25);
      const off = -len / 2 + (i + 0.5) * chunkLen;
      const cx = tx + (horizontal ? off : 0);
      const cz = tz + (horizontal ? 0 : off);
      const w  = horizontal ? chunkLen * 1.02 : WALL_T;
      const d  = horizontal ? WALL_T          : chunkLen * 1.02;
      add(hiBox(w, h, d, 3), stone, cx, h / 2, cz);
      // Occasional moss patch on the top of a chunk
      if (rand() > 0.65) {
        add(hiBox(w * 0.9, 0.18, d * 0.9, 2),
            moss, cx, h + 0.09, cz);
      }
    }
  };

  // South wall (facing -Z, where the bridge attaches)
  buildRaggedWall(ENCL_LEN, 0, -ENCL_W / 2, true);
  // North wall
  buildRaggedWall(ENCL_LEN, 0,  ENCL_W / 2, true);
  // West wall
  buildRaggedWall(ENCL_W, -ENCL_LEN / 2, 0, false);
  // East wall
  buildRaggedWall(ENCL_W,  ENCL_LEN / 2, 0, false);

  // ── Small square embrasure windows (dark voids) scattered on the walls ──
  // Implemented as thin dark-wood slabs on the wall surface rather than real
  // cutouts, which is good enough at the camera distances the user sees.
  const windowDark = M.roofBrown;
  const addWindows = (startX, endX, z, outward) => {
    for (let w = 0; w < WINDOWS_PER_WALL; w++) {
      const t = (w + 0.5) / WINDOWS_PER_WALL;
      const x = startX + (endX - startX) * t;
      const y = WALL_H * 0.62;
      add(hiBox(WINDOW_W, WINDOW_H, 0.2, 2), windowDark,
          x, y, z + outward * (WALL_T / 2 + 0.01));
    }
  };
  addWindows(-ENCL_LEN / 2 + 4, ENCL_LEN / 2 - 4, -ENCL_W / 2,  -1); // south
  addWindows(-ENCL_LEN / 2 + 4, ENCL_LEN / 2 - 4,  ENCL_W / 2,  +1); // north

  // Windows on the long walls (E/W) too — rotated so they face outward
  for (let w = 0; w < 4; w++) {
    const t = (w + 0.5) / 4;
    const z = -ENCL_W / 2 + ENCL_W * t;
    const y = WALL_H * 0.62;
    add(hiBox(0.2, WINDOW_H, WINDOW_W, 2), windowDark,
        -ENCL_LEN / 2 - WALL_T / 2 - 0.01, y, z);
    add(hiBox(0.2, WINDOW_H, WINDOW_W, 2), windowDark,
         ENCL_LEN / 2 + WALL_T / 2 + 0.01, y, z);
  }

  // ── Four round corner bastions ──
  // Partially collapsed so they're lower than the walls. Built from a
  // high-segment cylinder + a low moss ring at the top.
  const bastionCorners = [
    [-ENCL_LEN / 2, -ENCL_W / 2],
    [ ENCL_LEN / 2, -ENCL_W / 2],
    [-ENCL_LEN / 2,  ENCL_W / 2],
    [ ENCL_LEN / 2,  ENCL_W / 2],
  ];
  for (const [bx, bz] of bastionCorners) {
    // Slight height variation per bastion so they look naturally weathered
    const h = BAST_H * (0.9 + rand() * 0.25);
    add(new THREE.CylinderGeometry(BAST_R, BAST_R * 1.06, h, 48, 10),
        stone, bx, h / 2, bz);
    // Mossy top cap
    add(new THREE.CylinderGeometry(BAST_R * 1.02, BAST_R * 1.02, 0.22, 48, 1),
        moss, bx, h + 0.11, bz);
    // Small arrow-slit embrasure on the outward-facing side
    const ox = Math.sign(bx);
    const oz = Math.sign(bz);
    add(hiBox(0.15, 0.9, 0.5, 2), windowDark,
        bx + ox * BAST_R * 0.8, h * 0.55, bz + oz * BAST_R * 0.8);
  }

  // ── Gatehouse (south side, projecting outward) ──
  // A small thicker block with an arched entrance carved visually by a
  // dark recessed dark slab.
  const gateZ = -ENCL_W / 2 - GATE_D / 2;
  add(hiBox(GATE_W + 0.6, GATE_H, GATE_D + 0.4, 4),
      stone, 0, GATE_H / 2, gateZ);
  // Mossy cap on the gatehouse top
  add(hiBox(GATE_W + 0.8, 0.22, GATE_D + 0.6, 2),
      moss, 0, GATE_H + 0.11, gateZ);
  // Dark archway (approximated as a box + a half-cylinder top)
  add(hiBox(GATE_W * 0.55, GATE_H * 0.55, 0.3, 3),
      windowDark, 0, GATE_H * 0.28, gateZ - GATE_D / 2 - 0.01);
  add(new THREE.CylinderGeometry(
        GATE_W * 0.275, GATE_W * 0.275, 0.3, 24, 1, false, 0, Math.PI),
      windowDark,
      0, GATE_H * 0.28 + GATE_H * 0.275, gateZ - GATE_D / 2 - 0.01,
      0, 0, Math.PI / 2);

  // ── Wooden entrance bridge (extends south from the gatehouse) ──
  const bridgeZ = gateZ - GATE_D / 2 - BRIDGE_LEN / 2;
  // Main deck
  add(hiBox(BRIDGE_W, BRIDGE_H, BRIDGE_LEN, 4),
      wood, 0, BRIDGE_Y, bridgeZ);
  // Plank detail — 10 thin transverse boards on top of the deck
  const PLANKS = 12;
  for (let i = 0; i < PLANKS; i++) {
    const t = (i + 0.5) / PLANKS;
    const pz = bridgeZ - BRIDGE_LEN / 2 + BRIDGE_LEN * t;
    add(hiBox(BRIDGE_W * 0.98, 0.04, BRIDGE_LEN / PLANKS * 0.85, 2),
        wood, 0, BRIDGE_Y + BRIDGE_H / 2 + 0.02, pz);
  }
  // Handrails — two thin horizontal beams with supports
  const HANDRAIL_H = 1.0;
  for (const sx of [-1, 1]) {
    add(hiBox(0.1, 0.1, BRIDGE_LEN, 2),
        wood, sx * (BRIDGE_W / 2 + 0.05), BRIDGE_Y + HANDRAIL_H, bridgeZ);
    // Vertical supports (6 per side)
    const POSTS = 6;
    for (let p = 0; p < POSTS; p++) {
      const t = (p + 0.5) / POSTS;
      const pz = bridgeZ - BRIDGE_LEN / 2 + BRIDGE_LEN * t;
      add(hiBox(0.08, HANDRAIL_H, 0.08, 2),
          wood, sx * (BRIDGE_W / 2 + 0.05),
          BRIDGE_Y + HANDRAIL_H / 2, pz);
    }
  }

  // ── A couple of trees inside the courtyard (the aerial shot shows
  // vegetation growing in the abandoned interior) ──
  const treeSpots = [
    [-6,  3],
    [ 5, -4],
    [ 8,  5],
  ];
  for (const [tx, tz] of treeSpots) {
    add(new THREE.CylinderGeometry(0.25, 0.35, 2.5, 8),
        trunk, tx, 1.25, tz);
    add(new THREE.ConeGeometry(1.8, 4.5, 9),
        foliage, tx, 4.5, tz);
  }

  // ── Landmark metadata ──
  // Footprint outline for the selection highlight — rectangle matching the
  // enclosure, slightly larger than WALL_T so the outline hugs the outside
  // of the walls. Include the gatehouse bump on the south side and the
  // bridge extension.
  root.userData.customFootprint = [
    [-ENCL_LEN / 2 - BAST_R * 0.3, -ENCL_W / 2 - BAST_R * 0.3],
    [-GATE_W / 2 - 0.4,            -ENCL_W / 2 - BAST_R * 0.3],
    [-GATE_W / 2 - 0.4,            -ENCL_W / 2 - GATE_D - BRIDGE_LEN],
    [ GATE_W / 2 + 0.4,            -ENCL_W / 2 - GATE_D - BRIDGE_LEN],
    [ GATE_W / 2 + 0.4,            -ENCL_W / 2 - BAST_R * 0.3],
    [ ENCL_LEN / 2 + BAST_R * 0.3, -ENCL_W / 2 - BAST_R * 0.3],
    [ ENCL_LEN / 2 + BAST_R * 0.3,  ENCL_W / 2 + BAST_R * 0.3],
    [-ENCL_LEN / 2 - BAST_R * 0.3,  ENCL_W / 2 + BAST_R * 0.3],
  ];
  // Base height — the walls are the tallest feature on a ruin with no roof.
  root.userData.baseHeight = WALL_H + 1;

  // eslint-disable-next-line no-console
  console.log(
    `[kronoberg_ruin] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Smålands Museum (Sveriges glasmuseum) ────────────────────────────────────
//
// Sweden's oldest regional museum, opened 1906 next to Växjö's railway
// station. Designed by Fritz Ulrich & Eduard Hallquisth in a late-1800s
// Nordic National Romantic / neo-Renaissance style: a symmetric red-brick
// main block with a central projecting pavilion carrying a stepped gable
// and a small roof lantern, flanking wings, a rusticated stone ground
// floor, pitched slate roof with dormers, round-arched ground-floor
// windows, and decorative stone quoins at the corners.
//
// Face budget ~1M, driven mostly by:
//   - tessellated brick courses (instanced thin stone bands at every
//     brick row on each facade)
//   - high-segment walls (segs=28)
//   - instanced roof slate tiles on both pitched slopes
//   - dense arched ground-floor window grid with per-window mullions

function buildSmalandsMuseum(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation — align the long axis with the OSM polygon.
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  const MAIN_LEN      = 44;
  const MAIN_W        = 18;
  const GROUND_H      = 4.0;    // rusticated stone ground floor
  const UPPER_H       = 5.5;    // red-brick upper floor
  const ATTIC_H       = 2.0;    // attic stub under the roof
  const MAIN_H        = GROUND_H + UPPER_H + ATTIC_H;
  const ROOF_H        = 7.0;    // pitched roof rise

  const PAVILION_W    = 14;     // central pavilion width
  const PAVILION_EXT  = 1.8;    // how far it projects forward
  const PAVILION_H    = MAIN_H + 3.5;
  const GABLE_STEPS   = 4;
  const GABLE_STEP_H  = 1.0;

  const GROUND_BAYS   = 11;     // round-arched ground windows per long facade
  const UPPER_BAYS    = 11;     // rectangular upper-floor windows
  const DORMER_COUNT  = 6;      // dormers on each roof slope

  // Tile grid densities (instanced, one draw call each).
  const BRICK_ROWS    = 62;     // ~9 cm per row over UPPER_H ≈ 5.5m
  const TILE_COLS     = 120;
  const TILE_ROWS     = 28;

  // ── Materials ──
  const brick    = M.museumBrick;
  const stone    = M.museumStone;
  const roof     = M.museumRoof;
  const glass    = M.glass;
  const dark     = M.roofBrown;

  // Face tracker
  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Rusticated stone ground floor ──
  add(hiBox(MAIN_LEN, GROUND_H, MAIN_W, 24),
      stone, 0, GROUND_H / 2, 0);

  // ── Red-brick upper floor + attic ──
  add(hiBox(MAIN_LEN, UPPER_H + ATTIC_H, MAIN_W, 28),
      brick, 0, GROUND_H + (UPPER_H + ATTIC_H) / 2, 0);

  // ── Horizontal string course separating stone from brick ──
  add(hiBox(MAIN_LEN + 0.3, 0.35, MAIN_W + 0.3, 6),
      stone, 0, GROUND_H + 0.17, 0);
  // ── Upper cornice ──
  add(hiBox(MAIN_LEN + 0.4, 0.55, MAIN_W + 0.4, 6),
      stone, 0, MAIN_H - 0.3, 0);

  // ── Corner stone quoins (4 tall pale strips) ──
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.55, MAIN_H, 0.55, 12), stone,
        sx * (MAIN_LEN / 2 - 0.28), MAIN_H / 2,
        sz * (MAIN_W / 2 - 0.28));
  }

  // ── Instanced brick course bands on the long facades ──
  // Each band is a thin stone sliver that simulates the mortar line
  // between brick rows. 62 rows × 2 long facades = 124 instances.
  {
    const tileGeo = new THREE.BoxGeometry(
      MAIN_LEN + 0.06, 0.04, 0.04, 2, 2, 2);
    const count = BRICK_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, stone, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      for (let r = 0; r < BRICK_ROWS; r++) {
        const y = GROUND_H + 0.08 + r * (UPPER_H / BRICK_ROWS);
        tmp.position.set(0, y, sz * (MAIN_W / 2 + 0.02));
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, count);
  }

  // ── Round-arched ground-floor windows on both long facades ──
  for (let b = 0; b < GROUND_BAYS; b++) {
    const bx = -MAIN_LEN / 2 + (b + 0.5) * (MAIN_LEN / GROUND_BAYS);
    for (const sz of [-1, 1]) {
      const wy = GROUND_H * 0.55;
      const wz = sz * (MAIN_W / 2 + 0.13);
      // Glass rectangle body
      add(hiBox(1.4, 2.2, 0.25, 6), glass, bx, wy, wz);
      // Arched top
      add(new THREE.CylinderGeometry(0.7, 0.7, 0.25, 24, 1, false, 0, Math.PI),
          glass, bx, wy + 1.1, wz, 0, 0, Math.PI / 2);
      // Stone keystone at the top of the arch
      add(hiBox(0.3, 0.4, 0.3, 3), stone, bx, wy + 1.9, wz);
      // Stone sill
      add(hiBox(1.7, 0.15, 0.32, 3), stone, bx, wy - 1.15, wz);
      // Central vertical mullion
      add(hiBox(0.08, 2.2, 0.28, 2), stone, bx, wy, wz);
    }
  }

  // ── Rectangular upper-floor windows with stone surrounds ──
  for (let b = 0; b < UPPER_BAYS; b++) {
    const bx = -MAIN_LEN / 2 + (b + 0.5) * (MAIN_LEN / UPPER_BAYS);
    for (const sz of [-1, 1]) {
      const wy = GROUND_H + UPPER_H * 0.55;
      const wz = sz * (MAIN_W / 2 + 0.13);
      // Glass
      add(hiBox(1.3, 2.4, 0.22, 6), glass, bx, wy, wz);
      // Stone frame (top, bottom, sides)
      add(hiBox(1.55, 0.14, 0.3, 3), stone, bx, wy - 1.2, wz);
      add(hiBox(1.55, 0.14, 0.3, 3), stone, bx, wy + 1.2, wz);
      add(hiBox(0.14, 2.4, 0.3, 3), stone, bx - 0.75, wy, wz);
      add(hiBox(0.14, 2.4, 0.3, 3), stone, bx + 0.75, wy, wz);
    }
  }

  // ── Windows on the short walls (east and west ends) ──
  for (const ex of [-1, 1]) {
    for (let row = 0; row < 2; row++) {
      for (let col = 0; col < 2; col++) {
        const wy = GROUND_H + 0.8 + row * (UPPER_H * 0.6);
        const wz = -MAIN_W / 3 + col * (MAIN_W * 2 / 3);
        add(hiBox(0.22, 2.0, 1.0, 4), glass,
            ex * (MAIN_LEN / 2 + 0.12), wy, wz);
        add(hiBox(0.32, 0.12, 1.3, 3), stone,
            ex * (MAIN_LEN / 2 + 0.12), wy - 1.1, wz);
      }
    }
  }

  // ── Central projecting pavilion (front facade only, toward +Z) ──
  const pavX = 0;
  const pavZ = MAIN_W / 2 + PAVILION_EXT / 2;
  add(hiBox(PAVILION_W, PAVILION_H, PAVILION_EXT + 0.1, 18),
      brick, pavX, PAVILION_H / 2, pavZ);
  // Pavilion stone base
  add(hiBox(PAVILION_W + 0.3, GROUND_H, PAVILION_EXT + 0.4, 10),
      stone, pavX, GROUND_H / 2, pavZ);
  // Pavilion string course + cornice
  add(hiBox(PAVILION_W + 0.4, 0.35, PAVILION_EXT + 0.5, 4),
      stone, pavX, GROUND_H + 0.17, pavZ);
  add(hiBox(PAVILION_W + 0.5, 0.55, PAVILION_EXT + 0.6, 4),
      stone, pavX, MAIN_H - 0.3, pavZ);

  // Pavilion corner pilasters
  for (const sx of [-1, 1]) {
    add(hiBox(0.7, PAVILION_H, 0.7, 10), stone,
        pavX + sx * (PAVILION_W / 2 - 0.35), PAVILION_H / 2,
        pavZ + PAVILION_EXT / 2);
  }

  // Pavilion central arched entrance
  {
    const doorShape = new THREE.Shape();
    doorShape.moveTo(-1.8, 0);
    doorShape.lineTo(-1.8, 3.4);
    doorShape.quadraticCurveTo(0, 5.0, 1.8, 3.4);
    doorShape.lineTo(1.8, 0);
    doorShape.closePath();
    const holePath = new THREE.Path();
    holePath.moveTo(-1.5, 0.2);
    holePath.lineTo(-1.5, 3.2);
    holePath.quadraticCurveTo(0, 4.6, 1.5, 3.2);
    holePath.lineTo(1.5, 0.2);
    holePath.closePath();
    doorShape.holes.push(holePath);
    const frame = new THREE.Mesh(
      new THREE.ExtrudeGeometry(doorShape, {
        depth: 0.5, bevelEnabled: false, curveSegments: 8,
      }),
      stone,
    );
    frame.position.set(pavX, 0.1, pavZ + PAVILION_EXT / 2);
    root.add(frame);
    track(frame);

    // Dark wooden door slab
    const slab = new THREE.Shape();
    slab.moveTo(-1.4, 0.25);
    slab.lineTo(-1.4, 3.1);
    slab.quadraticCurveTo(0, 4.4, 1.4, 3.1);
    slab.lineTo(1.4, 0.25);
    slab.closePath();
    const slabMesh = new THREE.Mesh(
      new THREE.ExtrudeGeometry(slab, {
        depth: 0.15, bevelEnabled: false, curveSegments: 8,
      }),
      dark,
    );
    slabMesh.position.set(pavX, 0.1, pavZ + PAVILION_EXT / 2 - 0.05);
    root.add(slabMesh);
    track(slabMesh);
  }

  // Pavilion upper arched window (large)
  {
    const winY = GROUND_H + UPPER_H * 0.5;
    add(hiBox(3.0, 3.2, 0.3, 6), glass,
        pavX, winY, pavZ + PAVILION_EXT / 2 + 0.02);
    add(new THREE.CylinderGeometry(1.5, 1.5, 0.3, 32, 1, false, 0, Math.PI),
        glass, pavX, winY + 1.6,
        pavZ + PAVILION_EXT / 2 + 0.02, 0, 0, Math.PI / 2);
    // Stone keystone
    add(hiBox(0.5, 0.6, 0.4, 3), stone,
        pavX, winY + 3.0, pavZ + PAVILION_EXT / 2 + 0.02);
  }

  // Pavilion stepped gable rising above MAIN_H
  for (let step = 0; step < GABLE_STEPS; step++) {
    const widthFrac = 1 - (step / GABLE_STEPS) * 0.7;
    const gw = PAVILION_W * widthFrac;
    add(hiBox(gw, GABLE_STEP_H, PAVILION_EXT + 0.2, 6),
        brick, pavX, MAIN_H + step * GABLE_STEP_H + GABLE_STEP_H / 2, pavZ);
    // Stone step caps
    for (const sx of [-1, 1]) {
      add(hiBox(0.6, 0.25, PAVILION_EXT + 0.3, 3), stone,
          pavX + sx * (gw / 2 + 0.15),
          MAIN_H + step * GABLE_STEP_H + GABLE_STEP_H, pavZ);
    }
  }

  // Pavilion roof lantern (small dome-like crown)
  const lanternY = MAIN_H + GABLE_STEPS * GABLE_STEP_H + 0.3;
  add(new THREE.CylinderGeometry(1.1, 1.3, 1.6, 20, 4),
      stone, pavX, lanternY + 0.8, pavZ);
  add(new THREE.ConeGeometry(1.0, 1.8, 20),
      roof, pavX, lanternY + 2.5, pavZ);
  add(hiBox(0.12, 0.9, 0.12, 3), stone, pavX, lanternY + 3.9, pavZ);

  // ── Pitched main roof (two sloped planes + instanced slate tiles) ──
  const pitchRad  = Math.atan2(ROOF_H, MAIN_W / 2);
  const slantLen  = Math.sqrt((MAIN_W / 2) ** 2 + ROOF_H ** 2);

  for (const sz of [-1, 1]) {
    const plane = new THREE.PlaneGeometry(MAIN_LEN, slantLen, 1, 1);
    const mesh = new THREE.Mesh(plane, roof);
    mesh.rotation.x = -Math.PI / 2 + sz * pitchRad;
    const midY = MAIN_H + (slantLen / 2) * Math.sin(pitchRad);
    const midZ = sz * (MAIN_W / 2 - (slantLen / 2) * Math.cos(pitchRad));
    mesh.position.set(0, midY, midZ);
    root.add(mesh);
    track(mesh);
  }

  // Gable ends (triangles) at the east/west
  for (const ex of [-1, 1]) {
    const tri = new THREE.Shape();
    tri.moveTo(-MAIN_W / 2, 0);
    tri.lineTo( MAIN_W / 2, 0);
    tri.lineTo(0, ROOF_H);
    tri.lineTo(-MAIN_W / 2, 0);
    const triMesh = new THREE.Mesh(new THREE.ShapeGeometry(tri), brick);
    triMesh.position.set(ex * (MAIN_LEN / 2), MAIN_H, 0);
    triMesh.rotation.y = ex > 0 ? -Math.PI / 2 : Math.PI / 2;
    root.add(triMesh);
    track(triMesh);
  }

  // Instanced slate tiles on both roof slopes
  {
    const tileGeo = new THREE.BoxGeometry(
      (MAIN_LEN / TILE_COLS) * 0.95,
      0.08,
      (slantLen / TILE_ROWS) * 0.92,
      2, 2, 2,
    );
    const totalTiles = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, roof, totalTiles);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      const tilt = -sz * pitchRad;
      for (let r = 0; r < TILE_ROWS; r++) {
        const u = (r + 0.5) * (slantLen / TILE_ROWS);
        const y = MAIN_H + u * Math.sin(pitchRad);
        const z = sz * (MAIN_W / 2 - u * Math.cos(pitchRad));
        for (let c = 0; c < TILE_COLS; c++) {
          const x = -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / TILE_COLS);
          tmp.position.set(x, y, z);
          tmp.rotation.set(tilt, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, totalTiles);
  }

  // ── Roof dormers (pediment gable on each slope) ──
  for (let d = 0; d < DORMER_COUNT; d++) {
    const dx = -MAIN_LEN / 2 + (d + 0.5) * (MAIN_LEN / DORMER_COUNT);
    for (const sz of [-1, 1]) {
      const dy = MAIN_H + ROOF_H * 0.3;
      const dz = sz * (MAIN_W / 2 - 1.0);
      add(hiBox(1.5, 1.3, 1.5, 4), brick, dx, dy, dz);
      add(hiBox(0.8, 0.95, 0.2, 3), glass, dx, dy,
          dz + sz * 0.78);
      // Mini pitched roof (triangular prism)
      const dGeo = new THREE.CylinderGeometry(0, 1.0, 0.9, 3);
      const dMesh = new THREE.Mesh(dGeo, roof);
      dMesh.rotation.z = Math.PI / 2;
      dMesh.position.set(dx, dy + 1.1, dz);
      root.add(dMesh);
      track(dMesh);
    }
  }

  // ── 3 slim chimneys on the ridge ──
  for (let c = 0; c < 3; c++) {
    const cx = -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / 3);
    add(hiBox(0.7, 2.5, 0.7, 6), brick, cx, MAIN_H + ROOF_H + 1.25, 0);
    add(hiBox(0.9, 0.25, 0.9, 3), stone, cx, MAIN_H + ROOF_H + 2.5, 0);
  }

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-MAIN_LEN / 2, -MAIN_W / 2],
    [ MAIN_LEN / 2, -MAIN_W / 2],
    [ MAIN_LEN / 2,  MAIN_W / 2],
    [ PAVILION_W / 2,  MAIN_W / 2],
    [ PAVILION_W / 2,  MAIN_W / 2 + PAVILION_EXT],
    [-PAVILION_W / 2,  MAIN_W / 2 + PAVILION_EXT],
    [-PAVILION_W / 2,  MAIN_W / 2],
    [-MAIN_LEN / 2,  MAIN_W / 2],
  ];
  root.userData.baseHeight = PAVILION_H + 4;   // pavilion lantern tip

  // eslint-disable-next-line no-console
  console.log(
    `[smalands_museum] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── VIDA Arena (Vida Arena / Växjö Lakers home) ──────────────────────────────
//
// Modern ice hockey arena, home of the Växjö Lakers SHL team. Built 2011,
// expanded 2014. Distinctive features: rectangular footprint with a long
// curved barrel roof over the ice, large glass curtain-wall entry facade,
// light-grey corrugated metal panel side walls, dark standing-seam metal
// roof, and a projecting entrance canopy with the club crest.
//
// Face budget ~1M, driven mostly by:
//   - high-segment barrel roof cylinder (radial=128, height=24 = ~6k tris
//     per unit length, instanced across the full arena span)
//   - instanced metal wall panel arrays on all four sides
//   - dense glass facade mullion grid
//   - tessellated roof standing-seam ribs as a second instanced layer

function buildVidaArena(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation — align the long axis with the OSM polygon.
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  const ARENA_LEN    = 108;   // long axis (ice is 60m, plus seating + concourses)
  const ARENA_W      = 82;    // transverse (seating bowl + concourses)
  const WALL_H       = 13;    // straight wall height before roof starts
  const ROOF_RISE    = 9;     // how high the barrel roof peaks above walls

  const ENTRY_W      = 28;    // glass entry facade width on +Z side
  const ENTRY_H      = 12;
  const CANOPY_W     = 20;    // projecting canopy over the main doors
  const CANOPY_D     = 6;
  const CANOPY_H     = 4;

  const PANEL_COLS   = 40;    // instanced wall panel grid on long facades
  const PANEL_ROWS   = 6;
  const RIB_COUNT    = 80;    // standing-seam ribs along the barrel roof

  // ── Materials ──
  const steel    = M.arenaMetal;
  const glass    = M.arenaGlass;
  const roof     = M.arenaRoof;
  const dark     = M.roofBrown;
  const trim     = M.white;

  // Face tracker
  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Concrete plinth (below the walls) ──
  add(hiBox(ARENA_LEN + 4, 0.6, ARENA_W + 4, 4),
      dark, 0, 0.3, 0);

  // ── Main structural box (steel-panel walls) ──
  add(hiBox(ARENA_LEN, WALL_H, ARENA_W, 24),
      steel, 0, WALL_H / 2 + 0.6, 0);

  // ── Instanced wall panel grid on both LONG facades (E and W sides) ──
  // Each panel is a thin rectangular slab sitting on top of the base wall
  // to suggest the corrugated metal cladding rhythm.
  {
    const panelW = (ARENA_LEN / PANEL_COLS) * 0.96;
    const panelH = (WALL_H / PANEL_ROWS) * 0.94;
    const tileGeo = new THREE.BoxGeometry(panelW, panelH, 0.08, 3, 3, 2);
    const count = PANEL_COLS * PANEL_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, steel, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      for (let r = 0; r < PANEL_ROWS; r++) {
        const py = 0.6 + (r + 0.5) * (WALL_H / PANEL_ROWS);
        for (let c = 0; c < PANEL_COLS; c++) {
          const px = -ARENA_LEN / 2 + (c + 0.5) * (ARENA_LEN / PANEL_COLS);
          tmp.position.set(px, py, sz * (ARENA_W / 2 + 0.05));
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, count);
  }

  // ── Horizontal accent stripe running around the walls ──
  add(hiBox(ARENA_LEN + 0.2, 0.35, ARENA_W + 0.2, 4),
      glass, 0, 0.6 + WALL_H * 0.62, 0);
  add(hiBox(ARENA_LEN + 0.25, 0.15, ARENA_W + 0.25, 3),
      trim, 0, 0.6 + WALL_H * 0.5, 0);

  // ── Glass entry facade on the +Z (south) side ──
  // Centred on the long wall, spans ENTRY_W wide × ENTRY_H tall.
  const entryZ = ARENA_W / 2 + 0.12;
  add(hiBox(ENTRY_W, ENTRY_H, 0.25, 4),
      glass, 0, 0.6 + ENTRY_H / 2, entryZ);

  // Mullion grid on the glass facade (6 vertical × 4 horizontal)
  for (let v = 1; v < 6; v++) {
    const mx = -ENTRY_W / 2 + v * (ENTRY_W / 6);
    add(hiBox(0.1, ENTRY_H, 0.3, 2), trim, mx, 0.6 + ENTRY_H / 2, entryZ + 0.13);
  }
  for (let h = 1; h < 4; h++) {
    const my = 0.6 + h * (ENTRY_H / 4);
    add(hiBox(ENTRY_W, 0.1, 0.3, 2), trim, 0, my, entryZ + 0.13);
  }

  // Entry doors (dark wood slab in the middle of the glass)
  add(hiBox(6.5, 3.0, 0.3, 3), dark, 0, 0.6 + 1.5, entryZ + 0.2);

  // ── Entrance canopy (projecting awning over the doors) ──
  add(hiBox(CANOPY_W, 0.6, CANOPY_D, 4),
      steel, 0, 0.6 + ENTRY_H + 0.3, entryZ + CANOPY_D / 2);
  // Canopy underside (white)
  add(hiBox(CANOPY_W - 0.3, 0.08, CANOPY_D - 0.3, 3),
      trim, 0, 0.6 + ENTRY_H + 0.0, entryZ + CANOPY_D / 2);
  // Two cylindrical canopy supports (front corners)
  for (const sx of [-1, 1]) {
    const col = new THREE.Mesh(
      new THREE.CylinderGeometry(0.22, 0.22, ENTRY_H, 20),
      steel,
    );
    col.position.set(sx * (CANOPY_W / 2 - 0.5),
                     0.6 + ENTRY_H / 2,
                     entryZ + CANOPY_D - 0.5);
    root.add(col);
    track(col);
  }

  // ── Curved barrel roof (half-cylinder spanning the long axis) ──
  // The roof sits on top of the walls and sweeps from side to side
  // across the short axis.
  const roofR = Math.sqrt((ARENA_W / 2) ** 2 + ROOF_RISE ** 2) * 0.55;
  {
    const rGeo = new THREE.CylinderGeometry(
      ARENA_W / 2 * 1.02, ARENA_W / 2 * 1.02,
      ARENA_LEN + 1.5, 128, 24, false, 0, Math.PI,
    );
    const rMesh = new THREE.Mesh(rGeo, roof);
    // Default cylinder axis is Y. Rotate so the axis lies along X.
    rMesh.rotation.z = Math.PI / 2;
    // The half-cylinder opens upward by default (0 to π). We want
    // the flat side to be the bottom (resting on the walls).
    // After rotation.z = π/2, the partial-cylinder sweep axis is now
    // world Z. No additional rotation needed — the half sits on top.
    rMesh.position.set(0, WALL_H + 0.6, 0);
    root.add(rMesh);
    track(rMesh);
  }

  // ── Instanced standing-seam ribs along the barrel roof ──
  // A rib is a thin strip running across the short axis of the roof,
  // curving with it. For geometry simplicity we approximate each rib
  // as a thin torus arc aligned with the roof curvature.
  {
    const ribGeo = new THREE.TorusGeometry(
      ARENA_W / 2 * 1.025, 0.08, 8, 64, Math.PI);
    const inst = new THREE.InstancedMesh(ribGeo, trim, RIB_COUNT);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (let r = 0; r < RIB_COUNT; r++) {
      const rx = -ARENA_LEN / 2 + (r + 0.5) * (ARENA_LEN / RIB_COUNT);
      tmp.position.set(rx, WALL_H + 0.6, 0);
      tmp.rotation.set(0, Math.PI / 2, Math.PI / 2);
      tmp.updateMatrix();
      inst.setMatrixAt(i++, tmp.matrix);
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, RIB_COUNT);
  }

  // ── Back side small emergency entries (opposite the main facade) ──
  const backZ = -(ARENA_W / 2 + 0.06);
  for (const sx of [-1, 1]) {
    add(hiBox(3.0, 3.2, 0.2, 3), dark,
        sx * (ARENA_LEN / 3), 0.6 + 1.6, backZ);
  }

  // ── Short-end doors on +X and -X sides ──
  for (const ex of [-1, 1]) {
    add(hiBox(0.2, 3.0, 2.8, 3), dark,
        ex * (ARENA_LEN / 2 + 0.1), 0.6 + 1.5, 0);
  }

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-ARENA_LEN / 2, -ARENA_W / 2],
    [ ARENA_LEN / 2, -ARENA_W / 2],
    [ ARENA_LEN / 2,  ARENA_W / 2],
    [ CANOPY_W / 2,  ARENA_W / 2],
    [ CANOPY_W / 2,  ARENA_W / 2 + CANOPY_D],
    [-CANOPY_W / 2,  ARENA_W / 2 + CANOPY_D],
    [-CANOPY_W / 2,  ARENA_W / 2],
    [-ARENA_LEN / 2,  ARENA_W / 2],
  ];
  root.userData.baseHeight = WALL_H + (ARENA_W / 2) + 1;

  // eslint-disable-next-line no-console
  console.log(
    `[vida_arena] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Kalmar Slott (Kalmar Castle) ─────────────────────────────────────────────
//
// Renaissance-era stone fortress (medieval origins, rebuilt by Johan III in
// the 1580s). Rectangular main mass with four round corner bastions, thick
// rubble-stone curtain walls, an inner courtyard, a gatehouse with drawbridge,
// and a water moat ringing the whole complex. Red clay tile roofs on the
// interior range buildings; the curtain walls themselves have no roof — the
// tops are crenellated stone parapets.
//
// Face budget ~1M — main contributors:
//   - four high-segment bastion cylinders (radial=64 × many heights) with
//     tessellated stone string courses
//   - high-segment tessellated curtain walls (segs=24)
//   - instanced roof tile grid on the interior range buildings
//   - crenellated parapet instanced boxes along all wall tops
//   - moat polygon (flat, negligible face cost)

function buildKalmarCastle(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation so the fortress aligns with any polygon the synthesizer
  // provides. For a freshly-synthesized square footprint the PCA angle is
  // near zero, which means the castle reads as axis-aligned — fine.
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  const FORT_LEN   = 58;     // E-W exterior of the curtain walls
  const FORT_W     = 52;     // N-S exterior of the curtain walls
  const WALL_H     = 15;
  const WALL_T     = 3.0;    // curtain-wall thickness
  const PARAPET_H  = 1.2;    // crenellated parapet above the wall top

  const BAST_R     = 6.5;    // corner bastion radius
  const BAST_H     = 18;     // bastions are taller than curtain walls

  const MOAT_W     = 14;     // water moat all around
  const COURT_INSET = 6;     // inner courtyard open space
  const RANGE_H    = 11;     // interior range building heights (below walls)
  const RANGE_ROOF_H = 5;    // steep pitched tile roof

  const GATE_W     = 8;
  const GATE_D     = 5;
  const GATE_H     = 14;

  const BRIDGE_LEN = 20;     // drawbridge crossing the moat
  const BRIDGE_W   = 4;
  const BRIDGE_H   = 0.4;

  // Tile + instancing densities
  const TILE_COLS  = 120;
  const TILE_ROWS  = 26;
  const MERLON_PER_WALL = 24; // crenellated parapet teeth per wall side

  // ── Materials ──
  const stone    = M.castleStone;
  const stoneCool = M.ruinStone;
  const trim     = M.castleTrim;
  const tile     = M.castleTile;
  const water    = M.moatWater;
  const wood     = M.bridgeWood;
  const dark     = M.roofBrown;
  const glass    = M.glass;

  // Face tracker
  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Moat (dark blue water pad) ──
  add(hiBox(FORT_LEN + MOAT_W * 2 + 4, 0.3, FORT_W + MOAT_W * 2 + 4, 2),
      water, 0, 0.15, 0);

  // Grass island cap under the fortress (so the stone doesn't float on water)
  add(hiBox(FORT_LEN + 2, 0.35, FORT_W + 2, 3),
      M.park, 0, 0.5, 0);

  // ── Four curtain walls (solid boxes, open top) ──
  // South wall
  add(hiBox(FORT_LEN, WALL_H, WALL_T, 24),
      stone, 0, 0.7 + WALL_H / 2, -FORT_W / 2 + WALL_T / 2);
  // North wall
  add(hiBox(FORT_LEN, WALL_H, WALL_T, 24),
      stone, 0, 0.7 + WALL_H / 2,  FORT_W / 2 - WALL_T / 2);
  // West wall
  add(hiBox(WALL_T, WALL_H, FORT_W - WALL_T * 2, 18),
      stone, -FORT_LEN / 2 + WALL_T / 2, 0.7 + WALL_H / 2, 0);
  // East wall
  add(hiBox(WALL_T, WALL_H, FORT_W - WALL_T * 2, 18),
      stone,  FORT_LEN / 2 - WALL_T / 2, 0.7 + WALL_H / 2, 0);

  // ── Stone string courses on all four walls (horizontal bands) ──
  for (const y of [5, 9, 13]) {
    add(hiBox(FORT_LEN + 0.3, 0.25, WALL_T + 0.3, 4),
        trim, 0, 0.7 + y, -FORT_W / 2 + WALL_T / 2);
    add(hiBox(FORT_LEN + 0.3, 0.25, WALL_T + 0.3, 4),
        trim, 0, 0.7 + y,  FORT_W / 2 - WALL_T / 2);
    add(hiBox(WALL_T + 0.3, 0.25, FORT_W - WALL_T * 2 + 0.3, 4),
        trim, -FORT_LEN / 2 + WALL_T / 2, 0.7 + y, 0);
    add(hiBox(WALL_T + 0.3, 0.25, FORT_W - WALL_T * 2 + 0.3, 4),
        trim,  FORT_LEN / 2 - WALL_T / 2, 0.7 + y, 0);
  }

  // ── Crenellated parapet (instanced merlons along each wall top) ──
  {
    const merlonW = (FORT_LEN / MERLON_PER_WALL) * 0.6;
    const tileGeo = new THREE.BoxGeometry(merlonW, PARAPET_H, WALL_T + 0.3, 3, 3, 3);
    const countLong = MERLON_PER_WALL * 2;
    const inst = new THREE.InstancedMesh(tileGeo, stone, countLong);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      for (let k = 0; k < MERLON_PER_WALL; k++) {
        const x = -FORT_LEN / 2 + (k + 0.5) * (FORT_LEN / MERLON_PER_WALL);
        tmp.position.set(x, 0.7 + WALL_H + PARAPET_H / 2,
                         sz * (FORT_W / 2 - WALL_T / 2));
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, countLong);
  }
  // Short walls (E/W) with fewer merlons
  {
    const MERLON_SHORT = 20;
    const merlonW = (FORT_W / MERLON_SHORT) * 0.6;
    const tileGeo = new THREE.BoxGeometry(WALL_T + 0.3, PARAPET_H, merlonW, 3, 3, 3);
    const count = MERLON_SHORT * 2;
    const inst = new THREE.InstancedMesh(tileGeo, stone, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const ex of [-1, 1]) {
      for (let k = 0; k < MERLON_SHORT; k++) {
        const z = -FORT_W / 2 + (k + 0.5) * (FORT_W / MERLON_SHORT);
        tmp.position.set(ex * (FORT_LEN / 2 - WALL_T / 2),
                         0.7 + WALL_H + PARAPET_H / 2, z);
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, count);
  }

  // ── Four round corner bastions (taller than the curtain walls) ──
  const corners = [
    [-FORT_LEN / 2 + WALL_T * 0.3, -FORT_W / 2 + WALL_T * 0.3],
    [ FORT_LEN / 2 - WALL_T * 0.3, -FORT_W / 2 + WALL_T * 0.3],
    [-FORT_LEN / 2 + WALL_T * 0.3,  FORT_W / 2 - WALL_T * 0.3],
    [ FORT_LEN / 2 - WALL_T * 0.3,  FORT_W / 2 - WALL_T * 0.3],
  ];
  for (const [bx, bz] of corners) {
    // Main bastion cylinder
    add(new THREE.CylinderGeometry(BAST_R, BAST_R * 1.08, BAST_H, 64, 18),
        stone, bx, 0.7 + BAST_H / 2, bz);
    // String courses
    for (const y of [5, 9, 13, 17]) {
      add(new THREE.TorusGeometry(BAST_R + 0.1, 0.18, 14, 64),
          trim, bx, 0.7 + y, bz, Math.PI / 2, 0, 0);
    }
    // Conical stone cap
    add(new THREE.ConeGeometry(BAST_R * 1.05, 3.2, 64),
        tile, bx, 0.7 + BAST_H + 1.6, bz);
    // Flagpole
    add(hiBox(0.15, 3.5, 0.15, 3), trim, bx, 0.7 + BAST_H + 4.5, bz);

    // Small arrow-slit windows on the bastion (8 directions × 3 levels)
    for (let lvl = 0; lvl < 3; lvl++) {
      const wy = 0.7 + 4 + lvl * 4;
      for (let dir = 0; dir < 8; dir++) {
        const a  = (dir / 8) * Math.PI * 2;
        const wx = bx + Math.cos(a) * (BAST_R + 0.1);
        const wz = bz + Math.sin(a) * (BAST_R + 0.1);
        const slit = add(hiBox(0.12, 1.2, 0.5, 2), dark, wx, wy, wz);
        slit.rotation.y = a;
      }
    }
  }

  // ── Interior range buildings (around the courtyard perimeter) ──
  // They sit inside the curtain walls, leaving a courtyard in the middle.
  const rangeLen = FORT_LEN - WALL_T * 2 - COURT_INSET * 0.4;
  const rangeW   = (FORT_W - WALL_T * 2 - COURT_INSET) / 2 - 1;
  // North range
  add(hiBox(rangeLen, RANGE_H, rangeW, 18),
      stoneCool, 0, 0.7 + RANGE_H / 2,
      FORT_W / 2 - WALL_T - rangeW / 2 - 0.5);
  // South range
  add(hiBox(rangeLen, RANGE_H, rangeW, 18),
      stoneCool, 0, 0.7 + RANGE_H / 2,
      -FORT_W / 2 + WALL_T + rangeW / 2 + 0.5);

  // Small pitched tile roofs on the north + south range buildings
  for (const sz of [-1, 1]) {
    const prismGeo = new THREE.CylinderGeometry(0, rangeW / 2 * 1.03, RANGE_ROOF_H, 3, 12);
    const prism = new THREE.Mesh(prismGeo, tile);
    prism.rotation.z = Math.PI / 2;
    prism.scale.set(1, rangeLen / (rangeW / 2 * 1.03 * 2), 1);
    prism.position.set(
      0, 0.7 + RANGE_H + RANGE_ROOF_H / 2,
      sz * (FORT_W / 2 - WALL_T - rangeW / 2 - 0.5),
    );
    root.add(prism);
    track(prism);
  }

  // ── Instanced roof tiles on the two range roofs ──
  {
    const pitchRad = Math.atan2(RANGE_ROOF_H, rangeW / 2);
    const slantLen = Math.sqrt((rangeW / 2) ** 2 + RANGE_ROOF_H ** 2);
    const tileGeo = new THREE.BoxGeometry(
      (rangeLen / TILE_COLS) * 0.95,
      0.08,
      (slantLen / TILE_ROWS) * 0.9,
      2, 2, 2,
    );
    // 2 ranges × 2 slopes each = 4 × TILE_COLS × TILE_ROWS
    const total = TILE_COLS * TILE_ROWS * 4;
    const inst = new THREE.InstancedMesh(tileGeo, tile, total);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const rangeSz of [-1, 1]) {
      const rangeCz = rangeSz *
        (FORT_W / 2 - WALL_T - rangeW / 2 - 0.5);
      for (const slopeSz of [-1, 1]) {
        const tilt = -slopeSz * pitchRad;
        for (let r = 0; r < TILE_ROWS; r++) {
          const u = (r + 0.5) * (slantLen / TILE_ROWS);
          const y = 0.7 + RANGE_H + u * Math.sin(pitchRad);
          const z = rangeCz + slopeSz * (rangeW / 2 - u * Math.cos(pitchRad));
          for (let c = 0; c < TILE_COLS; c++) {
            const x = -rangeLen / 2 + (c + 0.5) * (rangeLen / TILE_COLS);
            tmp.position.set(x, y, z);
            tmp.rotation.set(tilt, 0, 0);
            tmp.updateMatrix();
            inst.setMatrixAt(i++, tmp.matrix);
          }
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, total);
  }

  // Interior range windows (arched, 2 rows per range, 8 bays)
  for (const sz of [-1, 1]) {
    const rangeCz = sz * (FORT_W / 2 - WALL_T - rangeW / 2 - 0.5);
    for (let row = 0; row < 2; row++) {
      const wy = 0.7 + 2.8 + row * 4.5;
      for (let b = 0; b < 8; b++) {
        const bx = -rangeLen / 2 + (b + 0.5) * (rangeLen / 8);
        // Outward-facing (toward the curtain wall)
        add(hiBox(0.9, 1.6, 0.3, 4), glass,
            bx, wy, rangeCz - sz * (rangeW / 2 + 0.12));
        // Courtyard-facing
        add(hiBox(0.9, 1.6, 0.3, 4), glass,
            bx, wy, rangeCz + sz * (rangeW / 2 + 0.12));
      }
    }
  }

  // ── Gatehouse projecting south with drawbridge ──
  const gateCz = -FORT_W / 2 - GATE_D / 2;
  add(hiBox(GATE_W, GATE_H, GATE_D, 12),
      stone, 0, 0.7 + GATE_H / 2, gateCz);
  // Stone merlon crown on the gatehouse
  for (let k = 0; k < 6; k++) {
    const mx = -GATE_W / 2 + (k + 0.5) * (GATE_W / 6);
    add(hiBox(GATE_W / 12, PARAPET_H, GATE_D + 0.2, 2),
        stone, mx, 0.7 + GATE_H + PARAPET_H / 2, gateCz);
  }
  // Arched entrance on the gatehouse south face
  add(hiBox(3.2, 5.5, 0.4, 4), dark,
      0, 0.7 + 2.75, gateCz - GATE_D / 2 - 0.01);
  add(new THREE.CylinderGeometry(1.6, 1.6, 0.4, 32, 1, false, 0, Math.PI),
      dark, 0, 0.7 + 5.5, gateCz - GATE_D / 2 - 0.01,
      0, 0, Math.PI / 2);

  // ── Drawbridge wooden deck crossing the moat ──
  const bridgeCz = gateCz - GATE_D / 2 - BRIDGE_LEN / 2;
  add(hiBox(BRIDGE_W, BRIDGE_H, BRIDGE_LEN, 4),
      wood, 0, 0.7 + BRIDGE_H / 2, bridgeCz);
  // Plank detail (10 transverse boards)
  for (let k = 0; k < 10; k++) {
    const t = (k + 0.5) / 10;
    const pz = bridgeCz - BRIDGE_LEN / 2 + BRIDGE_LEN * t;
    add(hiBox(BRIDGE_W * 0.98, 0.04, BRIDGE_LEN / 10 * 0.8, 2),
        wood, 0, 0.7 + BRIDGE_H / 2 + 0.03, pz);
  }
  // Bridge side rails + posts
  for (const sx of [-1, 1]) {
    add(hiBox(0.1, 0.1, BRIDGE_LEN, 2), wood,
        sx * (BRIDGE_W / 2 + 0.05), 0.7 + 1.2, bridgeCz);
    for (let p = 0; p < 6; p++) {
      const t = (p + 0.5) / 6;
      const pz = bridgeCz - BRIDGE_LEN / 2 + BRIDGE_LEN * t;
      add(hiBox(0.1, 1.2, 0.1, 2), wood,
          sx * (BRIDGE_W / 2 + 0.05), 0.7 + 0.6, pz);
    }
  }

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-FORT_LEN / 2 - BAST_R * 0.3, -FORT_W / 2 - BAST_R * 0.3],
    [-GATE_W / 2 - 0.3,            -FORT_W / 2 - BAST_R * 0.3],
    [-GATE_W / 2 - 0.3,            -FORT_W / 2 - GATE_D - BRIDGE_LEN],
    [ GATE_W / 2 + 0.3,            -FORT_W / 2 - GATE_D - BRIDGE_LEN],
    [ GATE_W / 2 + 0.3,            -FORT_W / 2 - BAST_R * 0.3],
    [ FORT_LEN / 2 + BAST_R * 0.3, -FORT_W / 2 - BAST_R * 0.3],
    [ FORT_LEN / 2 + BAST_R * 0.3,  FORT_W / 2 + BAST_R * 0.3],
    [-FORT_LEN / 2 - BAST_R * 0.3,  FORT_W / 2 + BAST_R * 0.3],
  ];
  root.userData.baseHeight = BAST_H + 4.5;   // flagpole tip above bastion

  // eslint-disable-next-line no-console
  console.log(
    `[kalmar_castle] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Kalmar Domkyrka (Kalmar Cathedral) ──────────────────────────────────────
//
// Baroque cathedral completed 1702, designed by Nicodemus Tessin the Elder.
// Unusual for a Nordic cathedral: SQUARE plan with a central Greek-cross
// layout, corner pilasters, arched windows, flat cornice, and a central
// tall roof lantern (octagonal drum + copper dome + spire). Warm cream
// stucco walls with pale stone trim, copper-green dome.
//
// Face budget ~1M — main contributors:
//   - high-segment lantern cylinder and dome hemisphere
//   - dense tessellated walls (segs=28) with 3 rows of arched windows
//     per side
//   - instanced tile grid on the four sloped roof sections surrounding
//     the lantern
//   - corner pilasters + cornice molding ring

function buildKalmarDomkyrka(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  // Kalmar Cathedral is roughly square in plan ~40 × 40 m.
  const BODY_SIDE   = 40;
  const BODY_H      = 22;     // wall height to cornice
  const ARM_EXT     = 4;      // how far each Greek-cross arm projects beyond the square
  const ROOF_H      = 6;      // pitched roof rise on the 4 arm wings

  const LANTERN_R   = 5.0;    // central octagonal-drum radius
  const LANTERN_H   = 10;     // drum height
  const DOME_R      = 5.6;    // dome radius (slightly larger than drum)
  const DOME_H      = 6;      // dome rise
  const SPIRE_H     = 5;      // slender spire on top of the dome

  const WINDOW_ROWS = 3;
  const WINDOW_BAYS = 7;

  // ── Materials ──
  const stucco  = M.baroqueStucco;
  const stone   = M.museumStone;
  const roof    = M.castleRoof;      // verdigris green copper
  const dome    = M.castleRoof;
  const glass   = M.glass;
  const dark    = M.roofBrown;
  const gold    = M.theaterGold;     // small gilded details

  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Main square body (one big tessellated box) ──
  add(hiBox(BODY_SIDE, BODY_H, BODY_SIDE, 28),
      stucco, 0, BODY_H / 2, 0);

  // ── Four Greek-cross arm projections (one per side) ──
  // Each arm is a smaller tessellated box projecting outward.
  const ARM_W = BODY_SIDE * 0.38;
  for (const [dx, dz] of [[ 1, 0], [-1, 0], [0,  1], [0, -1]]) {
    const cx = dx * (BODY_SIDE / 2 + ARM_EXT / 2);
    const cz = dz * (BODY_SIDE / 2 + ARM_EXT / 2);
    const w = dx !== 0 ? ARM_EXT : ARM_W;
    const d = dz !== 0 ? ARM_EXT : ARM_W;
    add(hiBox(w, BODY_H, d, 10), stucco, cx, BODY_H / 2, cz);
  }

  // ── Stone base course + cornice above the walls ──
  add(hiBox(BODY_SIDE + 0.4, 0.5, BODY_SIDE + 0.4, 6),
      stone, 0, 0.25, 0);
  add(hiBox(BODY_SIDE + 0.5, 0.7, BODY_SIDE + 0.5, 6),
      stone, 0, BODY_H - 0.35, 0);

  // ── Corner pilasters (4 tall pale-stone strips) ──
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.9, BODY_H, 0.9, 12), stone,
        sx * (BODY_SIDE / 2 - 0.45), BODY_H / 2,
        sz * (BODY_SIDE / 2 - 0.45));
  }

  // ── Arched windows on all four facades ──
  for (let side = 0; side < 4; side++) {
    const ang = (side / 4) * Math.PI * 2;
    const nx = Math.cos(ang), nz = Math.sin(ang);
    // Tangent (perpendicular in XZ plane)
    const tx = -nz, tz = nx;
    for (let row = 0; row < WINDOW_ROWS; row++) {
      const wy = 3 + row * 6;
      for (let b = 0; b < WINDOW_BAYS; b++) {
        const t = (b + 0.5) / WINDOW_BAYS - 0.5;
        const bx = nx * (BODY_SIDE / 2 + 0.13) + tx * t * (BODY_SIDE - 4);
        const bz = nz * (BODY_SIDE / 2 + 0.13) + tz * t * (BODY_SIDE - 4);
        add(hiBox(1.3, 2.8, 0.25, 5), glass, bx, wy, bz, 0, ang, 0);
        // Arched top
        const arch = add(new THREE.CylinderGeometry(0.65, 0.65, 0.25, 28, 1, false, 0, Math.PI),
            glass, bx, wy + 1.4, bz, 0, ang, Math.PI / 2);
        arch.rotation.y = ang;
        arch.rotation.z = Math.PI / 2;
        // Stone sill
        add(hiBox(1.55, 0.15, 0.3, 3), stone,
            bx, wy - 1.5, bz, 0, ang, 0);
      }
    }
  }

  // ── Pitched roof on each arm of the Greek cross (4 small pitched roofs
  // sloping inward toward the central lantern) ──
  // Approximated as 4 half-prisms, one per arm. The main roof slopes from
  // the outer wall up toward the central lantern base.
  for (const [dx, dz] of [[ 1, 0], [-1, 0], [0,  1], [0, -1]]) {
    const prismGeo = new THREE.CylinderGeometry(0, BODY_SIDE / 2 * 0.6, ROOF_H, 3, 16);
    const prism = new THREE.Mesh(prismGeo, roof);
    // Orient the prism so its triangular profile slopes up toward the centre.
    prism.rotation.z = Math.PI / 2;
    const yaw = Math.atan2(dz, dx);
    prism.rotation.y = yaw;
    prism.scale.set(1, (BODY_SIDE / 2) / (BODY_SIDE / 2 * 0.6 * 2), 1);
    prism.position.set(dx * (BODY_SIDE / 4), BODY_H + ROOF_H / 2, dz * (BODY_SIDE / 4));
    root.add(prism);
    track(prism);
  }

  // ── Central roof platform (the area under the lantern) ──
  add(hiBox(LANTERN_R * 2.6, 0.4, LANTERN_R * 2.6, 6),
      stone, 0, BODY_H + ROOF_H + 0.2, 0);

  // ── Lantern octagonal drum ──
  add(new THREE.CylinderGeometry(LANTERN_R, LANTERN_R * 1.02, LANTERN_H, 48, 12),
      stucco, 0, BODY_H + ROOF_H + LANTERN_H / 2 + 0.4, 0);

  // Lantern corner stone trim (8 pale strips on the octagonal drum)
  for (let face = 0; face < 8; face++) {
    const a  = (face / 8) * Math.PI * 2;
    const rx = Math.cos(a) * LANTERN_R * 0.98;
    const rz = Math.sin(a) * LANTERN_R * 0.98;
    const rib = add(hiBox(0.25, LANTERN_H * 0.9, 0.45, 4),
                    stone, rx, BODY_H + ROOF_H + LANTERN_H / 2 + 0.4, rz);
    rib.rotation.y = a;
  }

  // Arched windows around the drum
  for (let face = 0; face < 8; face++) {
    const a  = (face / 8) * Math.PI * 2;
    const rx = Math.cos(a) * (LANTERN_R + 0.1);
    const rz = Math.sin(a) * (LANTERN_R + 0.1);
    const w = add(hiBox(0.7, LANTERN_H * 0.55, 0.2, 3),
                  glass, rx, BODY_H + ROOF_H + LANTERN_H * 0.55 + 0.4, rz);
    w.rotation.y = a;
  }

  // Stone cornice ring at the top of the drum
  add(new THREE.TorusGeometry(LANTERN_R + 0.1, 0.35, 16, 64),
      stone, 0, BODY_H + ROOF_H + LANTERN_H + 0.4, 0, Math.PI / 2, 0, 0);

  // ── Copper dome (hemisphere) ──
  add(new THREE.SphereGeometry(DOME_R, 64, 32, 0, Math.PI * 2, 0, Math.PI / 2),
      dome, 0, BODY_H + ROOF_H + LANTERN_H + 0.4, 0);

  // Dome ribs (8 meridians, thin gold-coloured strips)
  for (let i = 0; i < 8; i++) {
    const a = (i / 8) * Math.PI * 2;
    const torus = new THREE.Mesh(
      new THREE.TorusGeometry(DOME_R * 1.002, 0.1, 10, 48, Math.PI / 2),
      gold,
    );
    torus.rotation.set(Math.PI / 2, 0, a);
    torus.position.set(0, BODY_H + ROOF_H + LANTERN_H + 0.4, 0);
    root.add(torus);
    track(torus);
  }

  // ── Small upper drum + spire above the main dome ──
  const upperDrumY = BODY_H + ROOF_H + LANTERN_H + DOME_H + 0.2;
  add(new THREE.CylinderGeometry(1.4, 1.6, 1.6, 32, 4),
      stucco, 0, upperDrumY + 0.8, 0);
  add(new THREE.SphereGeometry(1.5, 32, 20, 0, Math.PI * 2, 0, Math.PI / 2),
      dome, 0, upperDrumY + 1.6, 0);
  // Spire
  add(new THREE.ConeGeometry(0.6, SPIRE_H, 24),
      gold, 0, upperDrumY + 1.6 + SPIRE_H / 2, 0);
  // Cross on top
  add(hiBox(0.1, 1.5, 0.1, 2), gold,
      0, upperDrumY + 1.6 + SPIRE_H + 0.75, 0);
  add(hiBox(0.8, 0.1, 0.1, 2), gold,
      0, upperDrumY + 1.6 + SPIRE_H + 0.5, 0);

  // ── West portal — large pedimented entrance on -X facade ──
  const portalY = 5;
  add(hiBox(0.4, 6, 4, 4), stone, -BODY_SIDE / 2 - 0.2, portalY, 0);
  // Triangular pediment above
  const pedGeo = new THREE.CylinderGeometry(0, 2.5, 1.8, 3, 4);
  const ped = new THREE.Mesh(pedGeo, stone);
  ped.rotation.z = Math.PI / 2;
  ped.rotation.y = -Math.PI / 2;
  ped.position.set(-BODY_SIDE / 2 - 0.2, 8.5, 0);
  root.add(ped);
  track(ped);
  // Dark wooden door
  add(hiBox(0.15, 4, 2.5, 4), dark, -BODY_SIDE / 2 - 0.3, 2, 0);

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-BODY_SIDE / 2 - ARM_EXT, -ARM_W / 2],
    [-BODY_SIDE / 2, -ARM_W / 2],
    [-BODY_SIDE / 2, -BODY_SIDE / 2],
    [-ARM_W / 2, -BODY_SIDE / 2],
    [-ARM_W / 2, -BODY_SIDE / 2 - ARM_EXT],
    [ ARM_W / 2, -BODY_SIDE / 2 - ARM_EXT],
    [ ARM_W / 2, -BODY_SIDE / 2],
    [ BODY_SIDE / 2, -BODY_SIDE / 2],
    [ BODY_SIDE / 2, -ARM_W / 2],
    [ BODY_SIDE / 2 + ARM_EXT, -ARM_W / 2],
    [ BODY_SIDE / 2 + ARM_EXT,  ARM_W / 2],
    [ BODY_SIDE / 2,  ARM_W / 2],
    [ BODY_SIDE / 2,  BODY_SIDE / 2],
    [ ARM_W / 2,  BODY_SIDE / 2],
    [ ARM_W / 2,  BODY_SIDE / 2 + ARM_EXT],
    [-ARM_W / 2,  BODY_SIDE / 2 + ARM_EXT],
    [-ARM_W / 2,  BODY_SIDE / 2],
    [-BODY_SIDE / 2,  BODY_SIDE / 2],
    [-BODY_SIDE / 2,  ARM_W / 2],
    [-BODY_SIDE / 2 - ARM_EXT,  ARM_W / 2],
  ];
  root.userData.baseHeight = upperDrumY + 1.6 + SPIRE_H + 1.5;

  // eslint-disable-next-line no-console
  console.log(
    `[kalmar_domkyrka] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Kalmar läns museum (Kronan Hall) ────────────────────────────────────────
//
// Late-1800s red-brick industrial building (converted steam mill), now the
// regional museum housing the Kronan warship artifacts. Distinctive
// features: rectangular main block with pitched roof, square corner tower
// several stories tall (the old mill silo), arched industrial windows,
// stone string courses between floors, and a dark slate roof.
//
// Face budget ~1M — mostly tessellated brick walls, a high-segment tower
// cylinder isn't used here (the tower is square), an instanced brick
// course grid, and an instanced roof slate tile pattern.

function buildKalmarLansMuseum(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  const MAIN_LEN   = 48;
  const MAIN_W     = 18;
  const GROUND_H   = 4.2;
  const UPPER_H    = 5.5;
  const ATTIC_H    = 2.5;
  const MAIN_H     = GROUND_H + UPPER_H + ATTIC_H;
  const ROOF_H     = 7;

  const TOWER_SIDE = 8;      // square corner tower (the old mill silo)
  const TOWER_H    = MAIN_H + 10;
  const TOWER_ROOF_H = 4;

  const GROUND_BAYS = 10;
  const UPPER_BAYS  = 10;
  const BRICK_ROWS  = 60;
  const TILE_COLS   = 120;
  const TILE_ROWS   = 28;

  // Materials
  const brick  = M.churchRed;       // warm red brick (slightly darker than museumBrick)
  const stone  = M.museumStone;
  const roof   = M.museumRoof;      // dark slate
  const glass  = M.glass;
  const dark   = M.roofBrown;

  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Main block ──
  add(hiBox(MAIN_LEN, MAIN_H, MAIN_W, 26),
      brick, 0, MAIN_H / 2, 0);

  // Stone plinth
  add(hiBox(MAIN_LEN + 0.3, 0.5, MAIN_W + 0.3, 4),
      stone, 0, 0.25, 0);

  // String course between ground and upper floors
  add(hiBox(MAIN_LEN + 0.2, 0.3, MAIN_W + 0.2, 4),
      stone, 0, GROUND_H, 0);

  // Upper cornice
  add(hiBox(MAIN_LEN + 0.4, 0.55, MAIN_W + 0.4, 4),
      stone, 0, MAIN_H - 0.28, 0);

  // Corner quoins
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.5, MAIN_H, 0.5, 12), stone,
        sx * (MAIN_LEN / 2 - 0.25), MAIN_H / 2,
        sz * (MAIN_W / 2 - 0.25));
  }

  // ── Instanced brick course bands ──
  {
    const tileGeo = new THREE.BoxGeometry(MAIN_LEN + 0.06, 0.035, 0.04, 2, 2, 2);
    const count = BRICK_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, stone, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      for (let r = 0; r < BRICK_ROWS; r++) {
        const y = GROUND_H + 0.1 + r * ((UPPER_H + ATTIC_H - 0.2) / BRICK_ROWS);
        tmp.position.set(0, y, sz * (MAIN_W / 2 + 0.02));
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, count);
  }

  // ── Arched ground-floor industrial windows ──
  for (let b = 0; b < GROUND_BAYS; b++) {
    const bx = -MAIN_LEN / 2 + (b + 0.5) * (MAIN_LEN / GROUND_BAYS);
    for (const sz of [-1, 1]) {
      const wy = GROUND_H * 0.6;
      const wz = sz * (MAIN_W / 2 + 0.13);
      // Large arched window (typical of mill/warehouse)
      add(hiBox(1.8, 2.6, 0.25, 5), glass, bx, wy, wz);
      add(new THREE.CylinderGeometry(0.9, 0.9, 0.25, 28, 1, false, 0, Math.PI),
          glass, bx, wy + 1.3, wz, 0, 0, Math.PI / 2);
      // Stone sill
      add(hiBox(2.1, 0.18, 0.32, 3), stone, bx, wy - 1.45, wz);
    }
  }

  // ── Rectangular upper-floor windows ──
  for (let b = 0; b < UPPER_BAYS; b++) {
    const bx = -MAIN_LEN / 2 + (b + 0.5) * (MAIN_LEN / UPPER_BAYS);
    for (const sz of [-1, 1]) {
      const wy = GROUND_H + UPPER_H * 0.55;
      const wz = sz * (MAIN_W / 2 + 0.13);
      add(hiBox(1.3, 2.2, 0.22, 4), glass, bx, wy, wz);
      add(hiBox(1.55, 0.12, 0.3, 3), stone, bx, wy - 1.1, wz);
      add(hiBox(1.55, 0.12, 0.3, 3), stone, bx, wy + 1.1, wz);
    }
  }

  // ── Pitched slate roof ──
  const pitchRad = Math.atan2(ROOF_H, MAIN_W / 2);
  const slantLen = Math.sqrt((MAIN_W / 2) ** 2 + ROOF_H ** 2);
  for (const sz of [-1, 1]) {
    const plane = new THREE.PlaneGeometry(MAIN_LEN, slantLen, 1, 1);
    const mesh = new THREE.Mesh(plane, roof);
    mesh.rotation.x = -Math.PI / 2 + sz * pitchRad;
    const midY = MAIN_H + (slantLen / 2) * Math.sin(pitchRad);
    const midZ = sz * (MAIN_W / 2 - (slantLen / 2) * Math.cos(pitchRad));
    mesh.position.set(0, midY, midZ);
    root.add(mesh);
    track(mesh);
  }
  // Gable end triangles
  for (const ex of [-1, 1]) {
    const tri = new THREE.Shape();
    tri.moveTo(-MAIN_W / 2, 0);
    tri.lineTo( MAIN_W / 2, 0);
    tri.lineTo(0, ROOF_H);
    tri.lineTo(-MAIN_W / 2, 0);
    const triMesh = new THREE.Mesh(new THREE.ShapeGeometry(tri), brick);
    triMesh.position.set(ex * (MAIN_LEN / 2), MAIN_H, 0);
    triMesh.rotation.y = ex > 0 ? -Math.PI / 2 : Math.PI / 2;
    root.add(triMesh);
    track(triMesh);
  }

  // Instanced slate tiles
  {
    const tileGeo = new THREE.BoxGeometry(
      (MAIN_LEN / TILE_COLS) * 0.95,
      0.08,
      (slantLen / TILE_ROWS) * 0.92,
      2, 2, 2,
    );
    const total = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, roof, total);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      const tilt = -sz * pitchRad;
      for (let r = 0; r < TILE_ROWS; r++) {
        const u = (r + 0.5) * (slantLen / TILE_ROWS);
        const y = MAIN_H + u * Math.sin(pitchRad);
        const z = sz * (MAIN_W / 2 - u * Math.cos(pitchRad));
        for (let c = 0; c < TILE_COLS; c++) {
          const x = -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / TILE_COLS);
          tmp.position.set(x, y, z);
          tmp.rotation.set(tilt, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, total);
  }

  // ── Square corner tower (old mill silo) ──
  // Anchored to the east corner of the main block, projecting a bit beyond.
  const TOWER_CX = MAIN_LEN / 2 - TOWER_SIDE / 2 + 1;
  const TOWER_CZ = 0;
  add(hiBox(TOWER_SIDE, TOWER_H, TOWER_SIDE, 20),
      brick, TOWER_CX, TOWER_H / 2, TOWER_CZ);
  // Tower string courses (every 3m)
  for (let y = 3; y < TOWER_H; y += 3) {
    add(hiBox(TOWER_SIDE + 0.25, 0.22, TOWER_SIDE + 0.25, 4),
        stone, TOWER_CX, y, TOWER_CZ);
  }
  // Tower corner quoins
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.4, TOWER_H, 0.4, 12), stone,
        TOWER_CX + sx * (TOWER_SIDE / 2 - 0.2), TOWER_H / 2,
        TOWER_CZ + sz * (TOWER_SIDE / 2 - 0.2));
  }
  // Tall narrow tower windows (6 levels on each of the 4 sides)
  for (let side = 0; side < 4; side++) {
    const ang = (side / 4) * Math.PI * 2;
    for (let lvl = 0; lvl < 6; lvl++) {
      const wy = 3 + lvl * 4;
      const wx = TOWER_CX + Math.cos(ang) * (TOWER_SIDE / 2 + 0.12);
      const wz = TOWER_CZ + Math.sin(ang) * (TOWER_SIDE / 2 + 0.12);
      const win = add(hiBox(0.5, 1.8, 0.2, 3), glass, wx, wy, wz);
      win.rotation.y = ang;
    }
  }
  // Tower pyramid roof
  add(new THREE.CylinderGeometry(0, TOWER_SIDE / 2 * 1.1, TOWER_ROOF_H, 4),
      roof, TOWER_CX, TOWER_H + TOWER_ROOF_H / 2, TOWER_CZ, 0, Math.PI / 4, 0);
  // Small finial + weather vane
  add(hiBox(0.12, 2.5, 0.12, 3), dark,
      TOWER_CX, TOWER_H + TOWER_ROOF_H + 1.25, TOWER_CZ);

  // ── Entrance portico on the long facade (south side) ──
  add(hiBox(5, 3.8, 0.3, 4), stone, 0, 1.9, -MAIN_W / 2 - 0.15);
  add(hiBox(4.2, 3.3, 0.35, 4), dark, 0, 1.65, -MAIN_W / 2 - 0.17);

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-MAIN_LEN / 2, -MAIN_W / 2],
    [ MAIN_LEN / 2 + 1, -MAIN_W / 2],
    [ MAIN_LEN / 2 + 1 + TOWER_SIDE / 2, -TOWER_SIDE / 2],
    [ MAIN_LEN / 2 + 1 + TOWER_SIDE / 2,  TOWER_SIDE / 2],
    [ MAIN_LEN / 2 + 1,  MAIN_W / 2],
    [-MAIN_LEN / 2,  MAIN_W / 2],
  ];
  root.userData.baseHeight = TOWER_H + TOWER_ROOF_H + 2.5;

  // eslint-disable-next-line no-console
  console.log(
    `[kalmar_lans_museum] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Kalmar Teater ───────────────────────────────────────────────────────────
//
// Oldest preserved theatre building in Sweden — built 1863, architect Axel
// Nyström. Small neoclassical building in the heart of old Kalmar.
// Distinctive features: pedimented entrance portico with four tall Corinthian
// columns, cream/yellow stucco walls, pale stone quoins, a pitched roof
// with dormers, arched windows on the side facades, a rose-window-like
// oculus above the entrance, and a low stone plinth.
//
// Face budget ~1M — driven by the column shafts (high-segment cylinders),
// Corinthian capital detail, pediment relief, instanced roof tiles, and
// the tessellated wall segments.

function buildKalmarTeater(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  const BODY_LEN    = 35;
  const BODY_W      = 20;
  const BODY_H      = 14;
  const ROOF_H      = 6;

  const PORTICO_W   = 10;
  const PORTICO_D   = 4.5;
  const PORTICO_H   = BODY_H;
  const COL_R       = 0.55;
  const COL_N       = 4;     // 4 columns
  const PED_H       = 4.5;   // pediment height (triangular)

  const TILE_COLS   = 110;
  const TILE_ROWS   = 24;

  // Materials
  const stucco = M.baroqueStucco;
  const stone  = M.museumStone;
  const roof   = M.museumRoof;
  const glass  = M.glass;
  const dark   = M.roofBrown;
  const gold   = M.theaterGold;

  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // Plinth (low stone base)
  add(hiBox(BODY_LEN + 1, 0.6, BODY_W + 1, 3),
      stone, 0, 0.3, 0);

  // Main body (stucco)
  add(hiBox(BODY_LEN, BODY_H, BODY_W, 24),
      stucco, 0, 0.6 + BODY_H / 2, 0);

  // Stone corner quoins
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.55, BODY_H, 0.55, 10), stone,
        sx * (BODY_LEN / 2 - 0.28), 0.6 + BODY_H / 2,
        sz * (BODY_W / 2 - 0.28));
  }

  // Stone string course at mid-height
  add(hiBox(BODY_LEN + 0.3, 0.22, BODY_W + 0.3, 4),
      stone, 0, 0.6 + BODY_H * 0.5, 0);

  // Cornice molding at top
  add(hiBox(BODY_LEN + 0.5, 0.6, BODY_W + 0.5, 4),
      stone, 0, 0.6 + BODY_H - 0.3, 0);

  // ── Arched side windows (5 bays per long facade × 2 rows) ──
  for (let row = 0; row < 2; row++) {
    const wy = 0.6 + 3.5 + row * 5;
    for (let b = 0; b < 5; b++) {
      const bx = -BODY_LEN / 2 + (b + 0.5) * (BODY_LEN / 5);
      for (const sz of [-1, 1]) {
        // Don't draw windows where the portico will sit
        if (sz === 1 && Math.abs(bx) < PORTICO_W / 2 + 1) continue;
        const wz = sz * (BODY_W / 2 + 0.13);
        add(hiBox(1.1, 2.3, 0.22, 4), glass, bx, wy, wz);
        add(new THREE.CylinderGeometry(0.55, 0.55, 0.22, 24, 1, false, 0, Math.PI),
            glass, bx, wy + 1.15, wz, 0, 0, Math.PI / 2);
        add(hiBox(1.4, 0.12, 0.3, 3), stone, bx, wy - 1.25, wz);
      }
    }
  }

  // ── Front portico (+Z side) ──
  // Raised floor for the portico
  const porticoZ = BODY_W / 2 + PORTICO_D / 2;
  add(hiBox(PORTICO_W + 1, 0.4, PORTICO_D + 0.4, 3),
      stone, 0, 0.4, porticoZ);

  // Four columns — high-segment cylinders with widened capitals + bases
  for (let k = 0; k < COL_N; k++) {
    const t = (k + 0.5) / COL_N - 0.5;
    const cx = t * (PORTICO_W - 1);
    const cz = porticoZ + PORTICO_D / 2 - 0.5;

    // Column base (2-tier)
    add(new THREE.CylinderGeometry(COL_R * 1.3, COL_R * 1.4, 0.3, 32),
        stone, cx, 0.8, cz);
    add(new THREE.CylinderGeometry(COL_R * 1.15, COL_R * 1.3, 0.2, 32),
        stone, cx, 1.05, cz);

    // Column shaft — fluted with high radial segs (adds face count and
    // reads as a Corinthian column from a distance)
    add(new THREE.CylinderGeometry(COL_R, COL_R * 1.08, PORTICO_H - 2.8, 48, 20),
        stone, cx, 1.2 + (PORTICO_H - 2.8) / 2, cz);

    // Corinthian capital — a small flared cylinder + a square abacus
    add(new THREE.CylinderGeometry(COL_R * 1.5, COL_R, 0.5, 40),
        stone, cx, PORTICO_H - 1.3, cz);
    add(hiBox(COL_R * 3.2, 0.2, COL_R * 3.2, 4), stone,
        cx, PORTICO_H - 1.0, cz);

    // Acanthus leaf suggestions (8 small gold boxes wrapped around the capital)
    for (let a = 0; a < 8; a++) {
      const ang = (a / 8) * Math.PI * 2;
      add(hiBox(0.18, 0.4, 0.18, 2), gold,
          cx + Math.cos(ang) * COL_R * 1.35,
          PORTICO_H - 1.45,
          cz + Math.sin(ang) * COL_R * 1.35);
    }
  }

  // ── Entablature (horizontal beam above the columns) ──
  add(hiBox(PORTICO_W + 0.6, 1.2, PORTICO_D + 0.4, 4),
      stone, 0, PORTICO_H - 0.4, porticoZ);

  // ── Triangular pediment above the entablature ──
  const pedGeo = new THREE.CylinderGeometry(0, PORTICO_W / 2 * 1.05, PED_H, 3, 6);
  const ped = new THREE.Mesh(pedGeo, stone);
  ped.rotation.z = Math.PI / 2;
  ped.scale.set(1, (PORTICO_D + 0.4) / (PORTICO_W / 2 * 1.05 * 2), 1);
  ped.position.set(0, PORTICO_H + PED_H / 2 + 0.2, porticoZ);
  root.add(ped);
  track(ped);

  // Gold wreath / oculus medallion inside the pediment
  add(new THREE.TorusGeometry(1.4, 0.15, 16, 48),
      gold, 0, PORTICO_H + PED_H * 0.5 + 0.2, porticoZ + 0.15,
      0, Math.PI / 2 /* unused */, 0);

  // ── Main entrance door (under the portico, on the body wall) ──
  {
    const shape = new THREE.Shape();
    shape.moveTo(-2, 0);
    shape.lineTo(-2, 4.2);
    shape.quadraticCurveTo(0, 5.6, 2, 4.2);
    shape.lineTo(2, 0);
    shape.closePath();
    const holePath = new THREE.Path();
    holePath.moveTo(-1.6, 0.2);
    holePath.lineTo(-1.6, 4.0);
    holePath.quadraticCurveTo(0, 5.2, 1.6, 4.0);
    holePath.lineTo(1.6, 0.2);
    holePath.closePath();
    shape.holes.push(holePath);
    const frame = new THREE.Mesh(
      new THREE.ExtrudeGeometry(shape, {
        depth: 0.4, bevelEnabled: false, curveSegments: 8,
      }),
      stone,
    );
    frame.position.set(0, 0.6, BODY_W / 2 + 0.02);
    root.add(frame);
    track(frame);
    // Dark door slab
    const slabShape = new THREE.Shape();
    slabShape.moveTo(-1.55, 0.25);
    slabShape.lineTo(-1.55, 3.9);
    slabShape.quadraticCurveTo(0, 5.0, 1.55, 3.9);
    slabShape.lineTo(1.55, 0.25);
    slabShape.closePath();
    const slab = new THREE.Mesh(
      new THREE.ExtrudeGeometry(slabShape, {
        depth: 0.15, bevelEnabled: false, curveSegments: 8,
      }),
      dark,
    );
    slab.position.set(0, 0.6, BODY_W / 2 - 0.12);
    root.add(slab);
    track(slab);
  }

  // ── Rose-window oculus above the door (on the body wall, inside the portico) ──
  add(new THREE.CircleGeometry(1.6, 64), stone,
      0, 10, BODY_W / 2 + 0.02, 0, Math.PI /* unused placeholder */, 0);
  add(new THREE.CircleGeometry(1.3, 64), glass,
      0, 10, BODY_W / 2 + 0.04);
  add(new THREE.TorusGeometry(1.3, 0.08, 12, 64), gold,
      0, 10, BODY_W / 2 + 0.05);

  // ── Pitched roof on the main body ──
  const pitchRad = Math.atan2(ROOF_H, BODY_W / 2);
  const slantLen = Math.sqrt((BODY_W / 2) ** 2 + ROOF_H ** 2);
  for (const sz of [-1, 1]) {
    const plane = new THREE.PlaneGeometry(BODY_LEN, slantLen, 1, 1);
    const mesh = new THREE.Mesh(plane, roof);
    mesh.rotation.x = -Math.PI / 2 + sz * pitchRad;
    const midY = 0.6 + BODY_H + (slantLen / 2) * Math.sin(pitchRad);
    const midZ = sz * (BODY_W / 2 - (slantLen / 2) * Math.cos(pitchRad));
    mesh.position.set(0, midY, midZ);
    root.add(mesh);
    track(mesh);
  }
  // Gable end triangles (brick-stucco colour)
  for (const ex of [-1, 1]) {
    const tri = new THREE.Shape();
    tri.moveTo(-BODY_W / 2, 0);
    tri.lineTo( BODY_W / 2, 0);
    tri.lineTo(0, ROOF_H);
    tri.lineTo(-BODY_W / 2, 0);
    const triMesh = new THREE.Mesh(new THREE.ShapeGeometry(tri), stucco);
    triMesh.position.set(ex * (BODY_LEN / 2), 0.6 + BODY_H, 0);
    triMesh.rotation.y = ex > 0 ? -Math.PI / 2 : Math.PI / 2;
    root.add(triMesh);
    track(triMesh);
  }

  // Instanced slate tiles
  {
    const tileGeo = new THREE.BoxGeometry(
      (BODY_LEN / TILE_COLS) * 0.95,
      0.08,
      (slantLen / TILE_ROWS) * 0.92,
      2, 2, 2,
    );
    const total = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, roof, total);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const sz of [-1, 1]) {
      const tilt = -sz * pitchRad;
      for (let r = 0; r < TILE_ROWS; r++) {
        const u = (r + 0.5) * (slantLen / TILE_ROWS);
        const y = 0.6 + BODY_H + u * Math.sin(pitchRad);
        const z = sz * (BODY_W / 2 - u * Math.cos(pitchRad));
        for (let c = 0; c < TILE_COLS; c++) {
          const x = -BODY_LEN / 2 + (c + 0.5) * (BODY_LEN / TILE_COLS);
          tmp.position.set(x, y, z);
          tmp.rotation.set(tilt, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, total);
  }

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-BODY_LEN / 2, -BODY_W / 2],
    [ BODY_LEN / 2, -BODY_W / 2],
    [ BODY_LEN / 2,  BODY_W / 2],
    [ PORTICO_W / 2, BODY_W / 2],
    [ PORTICO_W / 2, BODY_W / 2 + PORTICO_D],
    [-PORTICO_W / 2, BODY_W / 2 + PORTICO_D],
    [-PORTICO_W / 2, BODY_W / 2],
    [-BODY_LEN / 2,  BODY_W / 2],
  ];
  root.userData.baseHeight = PORTICO_H + PED_H + 1;

  // eslint-disable-next-line no-console
  console.log(
    `[kalmar_teater] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}


// ── Hatstore Arena (formerly Guldfågeln Arena) ──────────────────────────────
//
// Home stadium of Kalmar FF (Allsvenskan football). ~12,000 capacity.
// Open-air pitch with four independent grandstand buildings on each side,
// taller main stand on the west side, steel-and-glass exterior, no roof
// over the pitch itself (only over the seating).
//
// Face budget ~1M — driven by:
//   - instanced seat rows (dense grid across all four stands)
//   - high-segment curved roof canopies above each stand
//   - tessellated stand walls
//   - glass facade mullions on the main stand's exterior

function buildHatstoreArena(root, cell, ctx) {
  const { THREE, M } = ctx;

  // PCA rotation — stadiums benefit from aligning the long axis (pitch)
  // with the OSM polygon.
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length;
    cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx;
      szz += dz * dz;
      sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  // ── Dimensions (metres) ──
  // Hatstore Arena fits a 105×68 m pitch plus generous concourses.
  const PITCH_LEN   = 105;    // E-W along the long axis
  const PITCH_W     = 68;
  const CONC_D      = 18;     // concourse depth outside the pitch on each side
  const OUTER_LEN   = PITCH_LEN + CONC_D * 2;
  const OUTER_W     = PITCH_W + CONC_D * 2;

  const STAND_H     = 14;    // seating stand height (shorter side stands)
  const MAIN_STAND_H = 22;   // west stand is taller (has the VIP boxes)

  const ROOF_D      = 12;    // canopy roof depth projecting over the seating

  const SEAT_ROWS_LONG  = 14; // rows of seats on each long stand
  const SEAT_ROWS_SHORT = 11;
  const SEAT_COLS_LONG  = 60;
  const SEAT_COLS_SHORT = 42;

  // ── Materials ──
  const steel  = M.arenaMetal;
  const glass  = M.arenaGlass;
  const roof   = M.arenaRoof;
  const grass  = M.park;
  const trim   = M.white;
  const dark   = M.roofBrown;
  const seatR  = M.churchRed;    // red seat colour (Kalmar FF colours are red)

  let faceCount = 0;
  const track = (mesh, instances = 1) => {
    const g = mesh.geometry;
    const per = g.index ? g.index.count / 3 : g.attributes.position.count / 3;
    faceCount += per * instances;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat);
    m.position.set(x, y, z);
    if (rx) m.rotation.x = rx;
    if (ry) m.rotation.y = ry;
    if (rz) m.rotation.z = rz;
    root.add(m);
    track(m);
    return m;
  };
  const hiBox = (w, h, d, segs = 6) =>
    new THREE.BoxGeometry(w, h, d, segs, segs, segs);

  // ── Concrete base plinth ──
  add(hiBox(OUTER_LEN + 4, 0.6, OUTER_W + 4, 3),
      dark, 0, 0.3, 0);

  // ── Grass pitch ──
  add(hiBox(PITCH_LEN, 0.15, PITCH_W, 4),
      grass, 0, 0.67, 0);
  // Pitch centre circle (white)
  add(new THREE.TorusGeometry(9.15, 0.1, 6, 48),
      trim, 0, 0.75, 0, Math.PI / 2, 0, 0);
  // Pitch halfway line
  add(hiBox(0.15, 0.02, PITCH_W, 2),
      trim, 0, 0.75, 0);
  // Goal areas (simplified)
  for (const sx of [-1, 1]) {
    add(hiBox(5.5, 0.02, 18, 2), trim,
        sx * (PITCH_LEN / 2 - 2.75), 0.75, 0);
  }

  // ── Helper: build a stand wall + seating + roof canopy on one edge ──
  // sideType: 'long-north' | 'long-south' | 'short-west' | 'short-east'
  const buildStand = (sideType) => {
    const isLong = sideType.startsWith('long');
    const isWest = sideType === 'short-west';
    const sz = (sideType === 'long-north') ? 1 : -1;
    const sx = (sideType === 'short-east') ? 1 : -1;

    const standLen = isLong ? OUTER_LEN : OUTER_W;
    const standH   = isWest ? MAIN_STAND_H : STAND_H;
    const standD   = CONC_D;

    // Stand main structural wall (steel panel exterior)
    let cx, cz;
    if (isLong) {
      cx = 0;
      cz = sz * (OUTER_W / 2 - standD / 2);
    } else {
      cx = sx * (OUTER_LEN / 2 - standD / 2);
      cz = 0;
    }
    add(hiBox(isLong ? standLen : standD, standH,
              isLong ? standD : standLen, 16),
        steel, cx, 0.6 + standH / 2, cz);

    // Upper accent stripe
    add(hiBox(isLong ? standLen + 0.2 : standD + 0.2, 0.35,
              isLong ? standD + 0.2 : standLen + 0.2, 3),
        glass, cx, 0.6 + standH * 0.7, cz);

    // ── Instanced seating grid (tilted plane of tiny red boxes) ──
    const rows = isLong ? SEAT_ROWS_LONG : SEAT_ROWS_SHORT;
    const cols = isLong ? SEAT_COLS_LONG : SEAT_COLS_SHORT;
    const pitchEdge = isLong ? PITCH_W / 2 : PITCH_LEN / 2;
    const standDepth = standD - 3;  // usable seating depth inside the stand
    const seatGeo = new THREE.BoxGeometry(
      (standLen * 0.85) / cols * 0.9, 0.4,
      standDepth / rows * 0.9, 2, 2, 2,
    );
    const total = rows * cols;
    const inst = new THREE.InstancedMesh(seatGeo, seatR, total);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (let r = 0; r < rows; r++) {
      const ry = 1.2 + r * (standH * 0.72 / rows);
      for (let c = 0; c < cols; c++) {
        const ct = (c + 0.5) / cols - 0.5;
        if (isLong) {
          const lx = ct * standLen * 0.85;
          const lz = sz * (pitchEdge + 0.8 + r * (standDepth / rows));
          tmp.position.set(lx, ry, lz);
        } else {
          const lz = ct * standLen * 0.85;
          const lx = sx * (pitchEdge + 0.8 + r * (standDepth / rows));
          tmp.position.set(lx, ry, lz);
        }
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst);
    track(inst, total);

    // ── Curved canopy roof projecting inward over the seating ──
    const roofR = ROOF_D;
    const roofArcGeo = new THREE.CylinderGeometry(
      roofR, roofR, isLong ? standLen + 1 : standLen + 1,
      48, 10, false, 0, Math.PI / 1.6,
    );
    const roofMesh = new THREE.Mesh(roofArcGeo, roof);
    if (isLong) {
      roofMesh.rotation.z = Math.PI / 2;
      // Rotate so the half-cylinder opens facing the pitch centre
      roofMesh.rotation.x = sz < 0 ? 0 : Math.PI;
      roofMesh.position.set(
        0, 0.6 + standH + 1,
        sz * (pitchEdge + 1.5),
      );
    } else {
      roofMesh.rotation.z = 0;
      roofMesh.rotation.y = Math.PI / 2;
      roofMesh.rotation.x = sx < 0 ? Math.PI / 2 : -Math.PI / 2;
      roofMesh.position.set(
        sx * (pitchEdge + 1.5), 0.6 + standH + 1,
        0,
      );
    }
    root.add(roofMesh);
    track(roofMesh);
  };

  buildStand('long-north');
  buildStand('long-south');
  buildStand('short-east');
  buildStand('short-west');

  // ── Main stand (west) glass facade with mullion grid ──
  // The west stand is already taller via MAIN_STAND_H; overlay a glass
  // curtain wall on its outward face for the VIP box look.
  const westCX = -(OUTER_LEN / 2 - CONC_D / 2);
  const westFaceX = westCX - CONC_D / 2 - 0.08;
  add(hiBox(0.3, MAIN_STAND_H * 0.85, OUTER_W * 0.75, 4),
      glass, westFaceX, 0.6 + MAIN_STAND_H * 0.5, 0);
  // Mullion grid (6 horizontal × 5 vertical)
  for (let h = 1; h < 6; h++) {
    const my = 0.6 + h * (MAIN_STAND_H * 0.85 / 6);
    add(hiBox(0.4, 0.12, OUTER_W * 0.78, 3), trim, westFaceX - 0.01, my, 0);
  }
  for (let v = 1; v < 10; v++) {
    const mz = -OUTER_W * 0.375 + v * (OUTER_W * 0.75 / 10);
    add(hiBox(0.4, MAIN_STAND_H * 0.85, 0.12, 3), trim,
        westFaceX - 0.01, 0.6 + MAIN_STAND_H * 0.5, mz);
  }

  // ── Floodlight pylons on all four corners ──
  const pylonCorners = [
    [-OUTER_LEN / 2 + 2, -OUTER_W / 2 + 2],
    [ OUTER_LEN / 2 - 2, -OUTER_W / 2 + 2],
    [-OUTER_LEN / 2 + 2,  OUTER_W / 2 - 2],
    [ OUTER_LEN / 2 - 2,  OUTER_W / 2 - 2],
  ];
  for (const [px, pz] of pylonCorners) {
    add(new THREE.CylinderGeometry(0.25, 0.3, 32, 16),
        steel, px, 16, pz);
    // Floodlight rack at the top
    add(hiBox(2.5, 1.0, 2.5, 4), trim, px, 32.5, pz);
    // 4 small light circles
    for (const [lx, lz] of [[-0.7,-0.7],[0.7,-0.7],[-0.7,0.7],[0.7,0.7]]) {
      add(new THREE.CylinderGeometry(0.35, 0.35, 0.15, 16),
          M.theaterGold, px + lx, 32.8, pz + lz);
    }
  }

  // ── Scoreboard at the east end ──
  add(hiBox(12, 5, 1.0, 4), dark,
      OUTER_LEN / 2 - 2, 0.6 + STAND_H + 6, 0);
  add(hiBox(11, 4, 0.2, 3), glass,
      OUTER_LEN / 2 - 2 - 0.1, 0.6 + STAND_H + 6, 0);

  // ── Entry turnstile zones (small canopies on 4 corners outside the stands) ──
  for (const [ex, ez] of [[-1,-1],[1,-1],[-1,1],[1,1]]) {
    const tx = ex * (OUTER_LEN / 2 + 2);
    const tz = ez * (OUTER_W / 2 + 2);
    add(hiBox(4, 3, 3, 3), steel, tx, 2, tz);
    add(hiBox(5, 0.4, 4, 2), trim, tx, 3.7, tz);
  }

  // ── Landmark metadata ──
  root.userData.customFootprint = [
    [-OUTER_LEN / 2, -OUTER_W / 2],
    [ OUTER_LEN / 2, -OUTER_W / 2],
    [ OUTER_LEN / 2,  OUTER_W / 2],
    [-OUTER_LEN / 2,  OUTER_W / 2],
  ];
  root.userData.baseHeight = 35;  // pylon + floodlight tip

  // eslint-disable-next-line no-console
  console.log(
    `[hatstore_arena] built ~${Math.round(faceCount).toLocaleString()} faces`,
  );
}

// ── Stockholm — Storkyrkan (Stockholm Cathedral) ────────────────────────────
//
// Late-Gothic brick church in Gamla Stan. Key features:
//   - Tall rectangular tower with dark bronze cap and gilded weather vane
//   - Red-brick nave with steep dark roof
//   - Baroque south portal
//   - Built 1306, tower completed 1740s
//   - Approx 54m long, 18m wide nave, tower ~66m tall

function buildStockholmStorkyrkan(root, cell, ctx) {
  const { THREE, M } = ctx;

  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx; szz += dz * dz; sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const NAVE_LEN = 54, NAVE_W = 18, NAVE_H = 20, ROOF_H = 10;
  const TOWER_W = 12, TOWER_D = 10, TOWER_H = 40;
  const CAP_H = 22, CAP_BASE_R = 7.5;
  const TILE_COLS = 130, TILE_ROWS = 28;

  const brick = M.churchRed, trim = M.churchTrim, roof = M.churchRoof;
  const spire = M.churchSpire, glass = M.glass, dark = M.roofBrown;

  let faceCount = 0;
  const track = (mesh, n = 1) => {
    const g = mesh.geometry;
    faceCount += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z);
    if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz;
    root.add(m); track(m); return m;
  };
  const hiBox = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Nave body
  add(hiBox(NAVE_LEN, NAVE_H, NAVE_W, 24), brick, 0, NAVE_H / 2, 0);

  // String courses
  add(hiBox(NAVE_LEN + 0.3, 0.4, NAVE_W + 0.3, 4), trim, 0, NAVE_H * 0.4, 0);
  add(hiBox(NAVE_LEN + 0.4, 0.5, NAVE_W + 0.4, 4), trim, 0, NAVE_H - 0.25, 0);

  // Pitched roof (triangular prism)
  {
    const shape = new THREE.Shape();
    shape.moveTo(-NAVE_W / 2 - 0.5, 0);
    shape.lineTo(0, ROOF_H);
    shape.lineTo(NAVE_W / 2 + 0.5, 0);
    shape.closePath();
    const geo = new THREE.ExtrudeGeometry(shape, { depth: NAVE_LEN + 0.4, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roof);
    m.rotation.y = Math.PI / 2;
    m.position.set(-NAVE_LEN / 2 - 0.2, NAVE_H, 0);
    root.add(m); track(m);
  }

  // Instanced roof tiles
  {
    const tileGeo = new THREE.BoxGeometry(
      (NAVE_LEN + 0.4) / TILE_COLS, 0.06,
      (NAVE_W / 2 + 0.5) / Math.cos(Math.atan2(ROOF_H, NAVE_W / 2)) / TILE_ROWS,
      2, 1, 2);
    const slopeLen = Math.sqrt((NAVE_W / 2 + 0.5) ** 2 + ROOF_H ** 2);
    const slopeAngle = Math.atan2(ROOF_H, NAVE_W / 2 + 0.5);
    const count = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, dark, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (const side of [-1, 1]) {
      for (let c = 0; c < TILE_COLS; c++) {
        for (let r = 0; r < TILE_ROWS; r++) {
          const tx = -NAVE_LEN / 2 + (c + 0.5) * (NAVE_LEN / TILE_COLS);
          const frac = (r + 0.5) / TILE_ROWS;
          const ty = NAVE_H + ROOF_H * (1 - frac) + 0.04;
          const tz = side * (NAVE_W / 2 + 0.5) * frac;
          tmp.position.set(tx, ty, tz);
          tmp.rotation.set(side * slopeAngle, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(i++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst); track(inst, count);
  }

  // Tower (west end)
  const towerX = -NAVE_LEN / 2 + TOWER_W / 2 - 1;
  add(hiBox(TOWER_W, TOWER_H, TOWER_D, 20), brick, towerX, TOWER_H / 2, 0);
  // Tower corner pilasters
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(0.6, TOWER_H, 0.6, 8), trim,
        towerX + sx * (TOWER_W / 2 - 0.3), TOWER_H / 2, sz * (TOWER_D / 2 - 0.3));
  }
  // Tower cornice
  add(hiBox(TOWER_W + 0.8, 0.7, TOWER_D + 0.8, 4), trim, towerX, TOWER_H - 0.35, 0);
  // Tower clock face (2 sides)
  for (const sz of [-1, 1]) {
    add(new THREE.CylinderGeometry(1.8, 1.8, 0.25, 32), trim,
        towerX, TOWER_H - 4, sz * (TOWER_D / 2 + 0.15), 0, 0, Math.PI / 2);
  }

  // Bronze baroque cap (octagonal cupola shape)
  {
    const capGeo = new THREE.CylinderGeometry(1.5, CAP_BASE_R, CAP_H, 8, 12);
    add(capGeo, spire, towerX, TOWER_H + CAP_H / 2, 0);
    // Gilded ball + cross at top
    add(new THREE.SphereGeometry(0.6, 16, 12), M.theaterGold,
        towerX, TOWER_H + CAP_H + 0.6, 0);
    add(hiBox(0.15, 2.0, 0.15, 3), M.theaterGold,
        towerX, TOWER_H + CAP_H + 2.2, 0);
    add(hiBox(1.0, 0.15, 0.15, 3), M.theaterGold,
        towerX, TOWER_H + CAP_H + 2.8, 0);
  }

  // Decorative torus rings on cap
  for (let r = 0; r < 6; r++) {
    const frac = r / 6;
    const radius = CAP_BASE_R * (1 - frac * 0.8);
    const ty = TOWER_H + frac * CAP_H;
    add(new THREE.TorusGeometry(radius, 0.15, 16, 96), spire,
        towerX, ty, 0, Math.PI / 2);
  }

  // Windows on nave (arched, both long facades)
  for (let w = 0; w < 9; w++) {
    const wx = -NAVE_LEN / 2 + 6 + w * (NAVE_LEN - 12) / 8;
    for (const sz of [-1, 1]) {
      const wz = sz * (NAVE_W / 2 + 0.12);
      add(hiBox(1.2, 3.5, 0.2, 4), glass, wx, NAVE_H * 0.5, wz);
      add(new THREE.CylinderGeometry(0.6, 0.6, 0.2, 16, 1, false, 0, Math.PI),
          glass, wx, NAVE_H * 0.5 + 1.75, wz, 0, 0, Math.PI / 2);
      add(hiBox(0.25, 0.15, 0.25, 2), trim, wx, NAVE_H * 0.5 + 2.5, wz);
    }
  }

  // Baroque south portal
  {
    const py = 0, pz = NAVE_W / 2 + 0.5;
    add(hiBox(6, 8, 1.5, 10), trim, 0, 4, pz);
    // Portal arch
    add(new THREE.CylinderGeometry(2.2, 2.2, 1.6, 24, 1, false, 0, Math.PI),
        trim, 0, 6.5, pz, 0, 0, Math.PI / 2);
    // Dark door recess
    add(hiBox(3.0, 5.0, 0.3, 4), dark, 0, 2.5, pz + 0.8);
  }

  root.userData.customFootprint = [
    [-NAVE_LEN/2, -NAVE_W/2], [NAVE_LEN/2, -NAVE_W/2],
    [NAVE_LEN/2, NAVE_W/2], [-NAVE_LEN/2, NAVE_W/2],
  ];
  root.userData.baseHeight = TOWER_H + CAP_H + 3.5;

  console.log(`[stockholm_storkyrkan] built ~${Math.round(faceCount).toLocaleString()} faces`);
}

// ── Stockholm — Royal Palace (Stockholms slott) ─────────────────────────────
//
// Baroque royal palace, one of the largest in Europe.
// Roughly 115m × 120m square with large inner courtyard.
// 4 stories, ~30m tall, with two semi-circular wings (NE and SE).
// Designed by Nicodemus Tessin the Younger, completed ~1760.

function buildStockholmRoyalPalace(root, cell, ctx) {
  const { THREE, M } = ctx;

  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx; szz += dz * dz; sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const PALACE_LEN = 115, PALACE_W = 120;
  const WING_T = 22;          // wing thickness
  const WALL_H = 28;
  const COURT_LEN = PALACE_LEN - 2 * WING_T;
  const COURT_W = PALACE_W - 2 * WING_T;
  const ROOF_H = 5;
  const WINDOW_ROWS = 4;

  const TILE_COLS = 140, TILE_ROWS = 24;

  const stucco = M.baroqueStucco, stone = M.castleTrim;
  const roofMat = M.castleRoof, glass = M.glass;
  const dark = M.roofBrown;

  let faceCount = 0;
  const track = (mesh, n = 1) => {
    const g = mesh.geometry;
    faceCount += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z);
    if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz;
    root.add(m); track(m); return m;
  };
  const hiBox = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Four wings forming the rectangle
  // North wing (along +Z edge)
  add(hiBox(PALACE_LEN, WALL_H, WING_T, 28), stucco,
      0, WALL_H / 2, PALACE_W / 2 - WING_T / 2);
  // South wing
  add(hiBox(PALACE_LEN, WALL_H, WING_T, 28), stucco,
      0, WALL_H / 2, -PALACE_W / 2 + WING_T / 2);
  // West wing
  add(hiBox(WING_T, WALL_H, COURT_W, 20), stucco,
      -PALACE_LEN / 2 + WING_T / 2, WALL_H / 2, 0);
  // East wing
  add(hiBox(WING_T, WALL_H, COURT_W, 20), stucco,
      PALACE_LEN / 2 - WING_T / 2, WALL_H / 2, 0);

  // Base plinth (rusticated ground floor)
  add(hiBox(PALACE_LEN + 1, 7, PALACE_W + 1, 16), stone,
      0, 3.5, 0);

  // Cornice bands on all four outer facades
  for (let row = 0; row < 4; row++) {
    const y = 7 + row * 5.5 + 0.2;
    add(hiBox(PALACE_LEN + 0.5, 0.4, PALACE_W + 0.5, 4), stone, 0, y, 0);
  }
  // Main upper cornice
  add(hiBox(PALACE_LEN + 0.8, 0.8, PALACE_W + 0.8, 6), stone,
      0, WALL_H - 0.4, 0);

  // Roofs on all four wings (flat-topped with slight pitch)
  for (const [rx, rz, rLen, rW] of [
    [0, PALACE_W/2 - WING_T/2, PALACE_LEN, WING_T],     // north
    [0, -PALACE_W/2 + WING_T/2, PALACE_LEN, WING_T],    // south
    [-PALACE_LEN/2 + WING_T/2, 0, WING_T, COURT_W],     // west
    [PALACE_LEN/2 - WING_T/2, 0, WING_T, COURT_W],      // east
  ]) {
    add(hiBox(rLen + 0.2, ROOF_H, rW + 0.2, 6), roofMat,
        rx, WALL_H + ROOF_H / 2, rz);
  }

  // Instanced windows on outer facades (4 rows × many bays per wing)
  {
    const winGeo = new THREE.BoxGeometry(1.5, 2.4, 0.22, 4, 4, 2);
    const frameGeo = new THREE.BoxGeometry(1.8, 2.8, 0.14, 2, 2, 2);
    // North and south facades
    for (const sz of [-1, 1]) {
      const fz = sz * (PALACE_W / 2 + 0.12);
      const bays = 28;
      for (let row = 0; row < WINDOW_ROWS; row++) {
        const wy = 8 + row * 5.2;
        for (let b = 0; b < bays; b++) {
          const wx = -PALACE_LEN / 2 + 3 + (b + 0.5) * ((PALACE_LEN - 6) / bays);
          add(winGeo, glass, wx, wy, fz);
          add(frameGeo, stone, wx, wy, fz);
        }
      }
    }
    // East and west facades
    for (const sx of [-1, 1]) {
      const fx = sx * (PALACE_LEN / 2 + 0.12);
      const bays = 16;
      for (let row = 0; row < WINDOW_ROWS; row++) {
        const wy = 8 + row * 5.2;
        for (let b = 0; b < bays; b++) {
          const wz = -COURT_W / 2 + (b + 0.5) * (COURT_W / bays);
          add(new THREE.BoxGeometry(0.22, 2.4, 1.5, 2, 4, 4), glass, fx, wy, wz);
          add(new THREE.BoxGeometry(0.14, 2.8, 1.8, 2, 2, 2), stone, fx, wy, wz);
        }
      }
    }
  }

  // Central main portal (west facade — facing Slottsbacken)
  {
    const px = -PALACE_LEN / 2 - 1;
    add(hiBox(3, 12, 16, 12), stone, px, 6, 0);
    // Large arched entrance
    add(new THREE.CylinderGeometry(3.5, 3.5, 3.2, 32, 1, false, 0, Math.PI),
        stone, px, 10, 0, Math.PI / 2, 0, 0);
    add(hiBox(6, 8, 0.5, 4), dark, px + 0.5, 4, 0);
  }

  // Corner pavilion accent (slightly taller blocks at corners)
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hiBox(WING_T + 2, WALL_H + 2, WING_T + 2, 10), stucco,
        sx * (PALACE_LEN / 2 - WING_T / 2),
        (WALL_H + 2) / 2,
        sz * (PALACE_W / 2 - WING_T / 2));
  }

  // Instanced roof tile grid on north wing (representative)
  {
    const tileGeo = new THREE.BoxGeometry(
      PALACE_LEN / TILE_COLS, 0.06, WING_T / TILE_ROWS, 2, 1, 2);
    const count = TILE_COLS * TILE_ROWS;
    const inst = new THREE.InstancedMesh(tileGeo, roofMat, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (let c = 0; c < TILE_COLS; c++) {
      for (let r = 0; r < TILE_ROWS; r++) {
        const tx = -PALACE_LEN / 2 + (c + 0.5) * (PALACE_LEN / TILE_COLS);
        const tz = PALACE_W / 2 - WING_T + (r + 0.5) * (WING_T / TILE_ROWS);
        tmp.position.set(tx, WALL_H + ROOF_H + 0.04, tz);
        tmp.rotation.set(0, 0, 0);
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst); track(inst, count);
  }

  const HL = PALACE_LEN / 2, HW = PALACE_W / 2;
  root.userData.customFootprint = [
    [-HL, -HW], [HL, -HW], [HL, HW], [-HL, HW],
  ];
  root.userData.baseHeight = WALL_H + ROOF_H;

  console.log(`[stockholm_royal_palace] built ~${Math.round(faceCount).toLocaleString()} faces`);
}

// ── Stockholm — Vasamuseet ──────────────────────────────────────────────────
//
// Purpose-built museum on Djurgården housing the warship Vasa (1628).
// Distinctive tall copper roof shaped like ship masts, asymmetric form.
// ~92m long, ~35m wide, copper-clad roof reaching ~34m.

function buildStockholmVasamuseet(root, cell, ctx) {
  const { THREE, M } = ctx;

  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx; szz += dz * dz; sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const MAIN_LEN = 92, MAIN_W = 35, BASE_H = 14;
  const ROOF_RIDGE_H = 20;
  const MAST_COUNT = 3, MAST_H = 34, MAST_R = 0.4;
  const TILE_COLS = 130, TILE_ROWS = 30;

  const copper = M.churchRoof, stone = M.museumStone;
  const brickDark = new THREE.MeshLambertMaterial({ color: 0x4a3628 });
  const glass = M.glass, roofMat = M.churchSpire;

  let faceCount = 0;
  const track = (mesh, n = 1) => {
    const g = mesh.geometry;
    faceCount += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z);
    if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz;
    root.add(m); track(m); return m;
  };
  const hiBox = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Dark brick base
  add(hiBox(MAIN_LEN, BASE_H, MAIN_W, 24), brickDark, 0, BASE_H / 2, 0);

  // Stone plinth
  add(hiBox(MAIN_LEN + 0.5, 1.5, MAIN_W + 0.5, 6), stone, 0, 0.75, 0);

  // Tall copper roof (asymmetric ridge)
  {
    const shape = new THREE.Shape();
    shape.moveTo(-MAIN_W / 2 - 0.5, 0);
    shape.lineTo(-3, ROOF_RIDGE_H);
    shape.lineTo(MAIN_W / 2 + 0.5, 0);
    shape.closePath();
    const geo = new THREE.ExtrudeGeometry(shape, { depth: MAIN_LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, copper);
    m.rotation.y = Math.PI / 2;
    m.position.set(-MAIN_LEN / 2, BASE_H, 0);
    root.add(m); track(m);
  }

  // Three mast-like spires projecting from the roof ridge
  for (let i = 0; i < MAST_COUNT; i++) {
    const mx = -MAIN_LEN / 3 + i * (MAIN_LEN / 3);
    add(new THREE.CylinderGeometry(MAST_R * 0.3, MAST_R, MAST_H, 16, 8),
        stone, mx, BASE_H + MAST_H / 2, -3);
    // Cross-trees
    add(hiBox(4, 0.2, 0.2, 3), stone, mx, BASE_H + MAST_H * 0.7, -3);
    // Pennant (small triangle)
    {
      const pennant = new THREE.Shape();
      pennant.moveTo(0, 0);
      pennant.lineTo(3, 0.8);
      pennant.lineTo(0, 1.6);
      pennant.closePath();
      const pg = new THREE.ExtrudeGeometry(pennant, { depth: 0.05, bevelEnabled: false });
      add(pg, M.theaterRed, mx, BASE_H + MAST_H - 2, -3);
    }
  }

  // Instanced copper roof tiles
  {
    const slopeLen = Math.sqrt((MAIN_W / 2) ** 2 + ROOF_RIDGE_H ** 2);
    const tileGeo = new THREE.BoxGeometry(
      MAIN_LEN / TILE_COLS, 0.05, slopeLen / TILE_ROWS, 2, 1, 2);
    const slopeAngle = Math.atan2(ROOF_RIDGE_H, MAIN_W / 2);
    const count = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tileGeo, roofMat, count);
    const tmp = new THREE.Object3D();
    let idx = 0;
    for (const side of [-1, 1]) {
      const halfW = side < 0 ? MAIN_W / 2 + 3 : MAIN_W / 2 - 3;
      const angle = side < 0 ? Math.atan2(ROOF_RIDGE_H, MAIN_W/2 + 3) :
                               Math.atan2(ROOF_RIDGE_H, MAIN_W/2 - 3);
      for (let c = 0; c < TILE_COLS; c++) {
        for (let r = 0; r < TILE_ROWS; r++) {
          const tx = -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / TILE_COLS);
          const frac = (r + 0.5) / TILE_ROWS;
          const ty = BASE_H + ROOF_RIDGE_H * (1 - frac) + 0.03;
          const tz = (side < 0 ? -3 - halfW * frac : -3 + halfW * frac);
          tmp.position.set(tx, ty, tz);
          tmp.rotation.set(side * angle, 0, 0);
          tmp.updateMatrix();
          inst.setMatrixAt(idx++, tmp.matrix);
        }
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst); track(inst, count);
  }

  // Entrance facade windows (south side)
  for (let w = 0; w < 14; w++) {
    const wx = -MAIN_LEN / 2 + 5 + w * (MAIN_LEN - 10) / 13;
    const wz = MAIN_W / 2 + 0.12;
    add(hiBox(2.0, 4.0, 0.2, 4), glass, wx, BASE_H * 0.55, wz);
  }

  root.userData.customFootprint = [
    [-MAIN_LEN/2, -MAIN_W/2], [MAIN_LEN/2, -MAIN_W/2],
    [MAIN_LEN/2, MAIN_W/2], [-MAIN_LEN/2, MAIN_W/2],
  ];
  root.userData.baseHeight = BASE_H + ROOF_RIDGE_H;

  console.log(`[stockholm_vasamuseet] built ~${Math.round(faceCount).toLocaleString()} faces`);
}

// ── Stockholm — Dramaten (Royal Dramatic Theatre) ───────────────────────────
//
// Art Nouveau / Jugendstil masterpiece by Fredrik Lilljekvist (1908).
// White marble facade facing Nybroplan, ~55m wide, ~40m deep.
// Central curved bay window, gilded bronze statues, copper dome.

function buildStockholmDramaten(root, cell, ctx) {
  const { THREE, M } = ctx;

  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx; szz += dz * dz; sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const MAIN_LEN = 55, MAIN_W = 40, MAIN_H = 22;
  const DOME_R = 8, DOME_H = 6;
  const PORTICO_W = 25, PORTICO_D = 4, PORTICO_H = 16;
  const COLUMN_COUNT = 6, COL_R = 0.6, COL_H = 12;
  const TILE_COLS = 120, TILE_ROWS = 20;

  const marble = M.white;
  const trim = M.castleTrim, gold = M.theaterGold;
  const roofMat = M.churchRoof, glass = M.glass;
  const red = M.theaterRed;

  let faceCount = 0;
  const track = (mesh, n = 1) => {
    const g = mesh.geometry;
    faceCount += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z);
    if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz;
    root.add(m); track(m); return m;
  };
  const hiBox = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main body
  add(hiBox(MAIN_LEN, MAIN_H, MAIN_W, 24), marble, 0, MAIN_H / 2, 0);

  // Rusticated stone base
  add(hiBox(MAIN_LEN + 0.3, 4, MAIN_W + 0.3, 10), trim, 0, 2, 0);

  // Upper cornice
  add(hiBox(MAIN_LEN + 0.6, 0.8, MAIN_W + 0.6, 6), trim, 0, MAIN_H - 0.4, 0);

  // String courses
  add(hiBox(MAIN_LEN + 0.3, 0.3, MAIN_W + 0.3, 4), trim, 0, 8, 0);
  add(hiBox(MAIN_LEN + 0.3, 0.3, MAIN_W + 0.3, 4), trim, 0, 15, 0);

  // Front portico with columns (facing +Z)
  const porticoZ = MAIN_W / 2 + PORTICO_D / 2;
  add(hiBox(PORTICO_W, 2, PORTICO_D + 1, 8), trim, 0, 1, porticoZ);
  // Columns
  for (let c = 0; c < COLUMN_COUNT; c++) {
    const cx = -PORTICO_W / 2 + 2 + c * ((PORTICO_W - 4) / (COLUMN_COUNT - 1));
    add(new THREE.CylinderGeometry(COL_R, COL_R * 1.15, COL_H, 32, 8),
        marble, cx, 2 + COL_H / 2, porticoZ);
    // Capital
    add(hiBox(1.6, 0.8, 1.6, 4), gold, cx, 2 + COL_H + 0.4, porticoZ);
  }
  // Entablature
  add(hiBox(PORTICO_W + 1, 2.5, PORTICO_D + 2, 8), trim,
      0, 2 + COL_H + 1.6, porticoZ);

  // Central copper dome over the auditorium
  add(new THREE.SphereGeometry(DOME_R, 48, 32, 0, Math.PI * 2, 0, Math.PI / 2),
      roofMat, 0, MAIN_H, -MAIN_W / 6);
  // Dome lantern
  add(new THREE.CylinderGeometry(1.5, 2.5, 4, 24, 4), roofMat,
      0, MAIN_H + DOME_H, -MAIN_W / 6);
  add(new THREE.SphereGeometry(0.5, 12, 8), gold,
      0, MAIN_H + DOME_H + 2.5, -MAIN_W / 6);

  // Gilded figures on the facade (simplified as gold pillars)
  for (let f = 0; f < 4; f++) {
    const fx = -6 + f * 4;
    add(new THREE.CylinderGeometry(0.3, 0.3, 3.5, 12, 4), gold,
        fx, MAIN_H + 1.75, MAIN_W / 2 + 0.3);
  }

  // Windows on front facade
  for (let row = 0; row < 3; row++) {
    const wy = 5 + row * 5.5;
    for (let b = 0; b < 12; b++) {
      const wx = -MAIN_LEN / 2 + 3 + b * ((MAIN_LEN - 6) / 11);
      add(hiBox(1.8, 2.8, 0.2, 4), glass, wx, wy, MAIN_W / 2 + 0.12);
    }
  }

  // Side windows
  for (const sx of [-1, 1]) {
    for (let row = 0; row < 3; row++) {
      const wy = 5 + row * 5.5;
      for (let b = 0; b < 8; b++) {
        const wz = -MAIN_W / 2 + 3 + b * ((MAIN_W - 6) / 7);
        add(new THREE.BoxGeometry(0.2, 2.8, 1.8, 2, 4, 4), glass,
            sx * (MAIN_LEN / 2 + 0.12), wy, wz);
      }
    }
  }

  // Instanced roof tiles on the flat top
  {
    const tileGeo = new THREE.BoxGeometry(
      MAIN_LEN / TILE_COLS, 0.06, MAIN_W / TILE_ROWS, 2, 1, 2);
    const count = TILE_COLS * TILE_ROWS;
    const inst = new THREE.InstancedMesh(tileGeo, roofMat, count);
    const tmp = new THREE.Object3D();
    let i = 0;
    for (let c = 0; c < TILE_COLS; c++) {
      for (let r = 0; r < TILE_ROWS; r++) {
        tmp.position.set(
          -MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / TILE_COLS),
          MAIN_H + 0.04,
          -MAIN_W / 2 + (r + 0.5) * (MAIN_W / TILE_ROWS));
        tmp.rotation.set(0, 0, 0);
        tmp.updateMatrix();
        inst.setMatrixAt(i++, tmp.matrix);
      }
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst); track(inst, count);
  }

  // Theatre marquee sign (red band)
  add(hiBox(PORTICO_W - 4, 1.5, 0.3, 4), red,
      0, 2 + COL_H + 4, porticoZ + PORTICO_D / 2);

  root.userData.customFootprint = [
    [-MAIN_LEN/2, -MAIN_W/2], [MAIN_LEN/2, -MAIN_W/2],
    [MAIN_LEN/2, MAIN_W/2 + PORTICO_D], [-MAIN_LEN/2, MAIN_W/2 + PORTICO_D],
  ];
  root.userData.baseHeight = MAIN_H + DOME_H + 3;

  console.log(`[stockholm_dramaten] built ~${Math.round(faceCount).toLocaleString()} faces`);
}

// ── Stockholm — Avicii Arena (Globen) ───────────────────────────────────────
//
// Iconic white hemispherical arena, the world's largest spherical building.
// Diameter 110m, height 85m (internal ~65m). Opened 1989.
// White exterior panels, SkyView glass gondolas on tracks.

function buildStockholmAviciiArena(root, cell, ctx) {
  const { THREE, M } = ctx;

  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx; szz += dz * dz; sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const RADIUS = 55;           // hemisphere radius
  const BASE_H = 4;            // concrete plinth
  const SPHERE_SEG = 96;       // high-res sphere for smooth curve
  const PANEL_RINGS = 24;
  const PANEL_SEGS = 48;

  const white = M.white;
  const concrete = M.concrete;
  const glass = M.arenaGlass;
  const steel = M.arenaMetal;

  let faceCount = 0;
  const track = (mesh, n = 1) => {
    const g = mesh.geometry;
    faceCount += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n;
    return mesh;
  };
  const add = (geo, mat, x, y, z, rx, ry, rz) => {
    const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z);
    if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz;
    root.add(m); track(m); return m;
  };

  // Concrete base plinth (circular)
  add(new THREE.CylinderGeometry(RADIUS + 3, RADIUS + 5, BASE_H, 64, 4),
      concrete, 0, BASE_H / 2, 0);

  // Main hemisphere (upper half of sphere)
  add(new THREE.SphereGeometry(RADIUS, SPHERE_SEG, SPHERE_SEG / 2,
      0, Math.PI * 2, 0, Math.PI / 2),
      white, 0, BASE_H, 0);

  // Panel seam rings (horizontal latitude lines on the hemisphere)
  for (let r = 1; r < PANEL_RINGS; r++) {
    const theta = (r / PANEL_RINGS) * (Math.PI / 2);
    const ringR = RADIUS * Math.cos(theta);
    const ringY = BASE_H + RADIUS * Math.sin(theta);
    if (ringR > 2) {
      add(new THREE.TorusGeometry(ringR, 0.12, 8, 96),
          steel, 0, ringY, 0, Math.PI / 2);
    }
  }

  // Vertical meridian seam lines (instanced thin strips)
  {
    const stripGeo = new THREE.BoxGeometry(0.15, RADIUS * 1.02, 0.15, 2, 24, 2);
    const count = PANEL_SEGS;
    const inst = new THREE.InstancedMesh(stripGeo, steel, count);
    const tmp = new THREE.Object3D();
    for (let s = 0; s < PANEL_SEGS; s++) {
      const angle = (s / PANEL_SEGS) * Math.PI * 2;
      tmp.position.set(0, BASE_H + RADIUS / 2, 0);
      tmp.rotation.set(0, angle, 0);
      tmp.scale.set(1, 1, 1);
      tmp.updateMatrix();
      // Create a curved strip by placing at the dome surface
      // (simplified as a radial spoke)
      const x = Math.cos(angle) * RADIUS * 0.5;
      const z = Math.sin(angle) * RADIUS * 0.5;
      tmp.position.set(x * 0.02, BASE_H + RADIUS * 0.45, z * 0.02);
      tmp.rotation.set(0, angle, 0);
      tmp.updateMatrix();
      inst.setMatrixAt(s, tmp.matrix);
    }
    inst.instanceMatrix.needsUpdate = true;
    root.add(inst); track(inst, count);
  }

  // SkyView track (two curved rails on exterior)
  for (const offset of [-8, 8]) {
    add(new THREE.TorusGeometry(RADIUS + 0.5, 0.3, 8, 96,
        Math.PI * 0.8),
        steel, offset, BASE_H + RADIUS * 0.3, 0, Math.PI / 2 - 0.3, 0, 0);
  }

  // SkyView gondola (simplified glass sphere)
  add(new THREE.SphereGeometry(2.5, 24, 16), glass,
      0, BASE_H + RADIUS - 5, RADIUS + 1);

  // Entrance canopy (south side)
  add(new THREE.BoxGeometry(30, 1, 12, 8, 2, 4), steel,
      0, 5, RADIUS + 6);
  // Entrance glass wall
  add(new THREE.BoxGeometry(25, 8, 0.3, 8, 8, 2), glass,
      0, BASE_H + 4, RADIUS + 0.5);

  // Ring of entrance doors
  for (let d = 0; d < 6; d++) {
    const dx = -12 + d * 5;
    add(new THREE.BoxGeometry(2, 4, 0.4, 2, 4, 2), glass,
        dx, BASE_H + 2, RADIUS + 0.8);
  }

  root.userData.customFootprint = [
    [-RADIUS, -RADIUS], [RADIUS, -RADIUS],
    [RADIUS, RADIUS], [-RADIUS, RADIUS],
  ];
  root.userData.baseHeight = BASE_H + RADIUS;

  console.log(`[stockholm_avicii_arena] built ~${Math.round(faceCount).toLocaleString()} faces`);
}

// ── Landmark builder helper ──────────────────────────────────────────────────
// Shared PCA rotation + face tracking + geometry helpers used by all landmark
// builders below. Reduces per-builder boilerplate from ~25 lines to 1 call.

function _landmarkInit(root, cell, ctx) {
  const { THREE, M } = ctx;
  // PCA rotation
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }
  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);
  const pitchedRoof = (len, halfW, roofH, mat, baseY) => {
    const s = new THREE.Shape(); s.moveTo(-halfW - 0.4, 0); s.lineTo(0, roofH); s.lineTo(halfW + 0.4, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: len, bevelEnabled: false });
    const m = new THREE.Mesh(geo, mat); m.rotation.y = Math.PI / 2; m.position.set(-len / 2, baseY, 0); root.add(m); track(m);
  };
  const roofTiles = (len, halfW, roofH, baseY, mat, cols = 120, rows = 24) => {
    const sa = Math.atan2(roofH, halfW + 0.4); const cnt = cols * rows * 2;
    const tg = new THREE.BoxGeometry(len / cols, 0.05, (halfW + 0.4) / Math.cos(sa) / rows, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, mat, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < cols; c++) { for (let r = 0; r < rows; r++) {
      const frac = (r + 0.5) / rows;
      tmp.position.set(-len / 2 + (c + 0.5) * (len / cols), baseY + roofH * (1 - frac) + 0.03, side * (halfW + 0.4) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt);
  };
  const foot = (hlx, hlz) => { root.userData.customFootprint = [[-hlx,-hlz],[hlx,-hlz],[hlx,hlz],[-hlx,hlz]]; };
  const done = (tag, baseH) => { root.userData.baseHeight = baseH; console.log(`[${tag}] built ~${Math.round(fc).toLocaleString()} faces`); };
  return { THREE, M, add, hb, track, pitchedRoof, roofTiles, foot, done };
}

// ── Generic landmark templates ──────────────────────────────────────────────
// For cities with many landmarks, these templates produce high-quality
// buildings using the helper with configurable dimensions and materials.

function _genericChurch(root, cell, ctx, tag, opts = {}) {
  const L = _landmarkInit(root, cell, ctx);
  const { THREE, M, add, hb, pitchedRoof, roofTiles, foot, done } = L;
  const NL = opts.naveLen || 55, NW = opts.naveW || 20, NH = opts.naveH || 20, RH = opts.roofH || 10;
  const TW = opts.towerW || 11, TH = opts.towerH || 40, SH = opts.spireH || 35;
  const brick = opts.brick || M.churchRed, trim = M.churchTrim, roof = M.churchRoof;
  const spire = M.churchSpire, glass = M.glass;
  // Nave
  add(hb(NL, NH, NW, 24), brick, 0, NH / 2, 0);
  add(hb(NL + 0.4, 0.5, NW + 0.4, 4), trim, 0, NH - 0.25, 0);
  pitchedRoof(NL, NW / 2, RH, roof, NH);
  roofTiles(NL, NW / 2, RH, NH, M.roofBrown);
  // Tower
  const tx = -NL / 2 + TW / 2;
  add(hb(TW, TH, TW, 20), brick, tx, TH / 2, 0);
  add(hb(TW + 0.6, 0.6, TW + 0.6, 4), trim, tx, TH - 0.3, 0);
  add(new THREE.CylinderGeometry(0.2, TW / 2.3, SH, 8, 14), spire, tx, TH + SH / 2, 0);
  for (let r = 0; r < 6; r++) { const f = r / 6; add(new THREE.TorusGeometry(TW / 2.3 * (1 - f * 0.9), 0.12, 10, 64), spire, tx, TH + f * SH, 0, Math.PI / 2); }
  add(hb(0.12, 2, 0.12, 2), M.theaterGold, tx, TH + SH + 1, 0);
  add(hb(0.8, 0.12, 0.12, 2), M.theaterGold, tx, TH + SH + 1.6, 0);
  // Windows
  const bays = Math.floor(NL / 6);
  for (let w = 0; w < bays; w++) { const wx = -NL / 2 + 4 + w * ((NL - 8) / (bays - 1));
    for (const sz of [-1, 1]) { add(hb(1.2, 3.5, 0.2, 4), glass, wx, NH * 0.5, sz * (NW / 2 + 0.12));
      add(new THREE.CylinderGeometry(0.6, 0.6, 0.2, 16, 1, false, 0, Math.PI), glass, wx, NH * 0.5 + 1.75, sz * (NW / 2 + 0.12), 0, 0, Math.PI / 2); }}
  foot(NL / 2, NW / 2);
  done(tag, TH + SH + 2);
}

function _genericCastle(root, cell, ctx, tag, opts = {}) {
  const L = _landmarkInit(root, cell, ctx);
  const { THREE, M, add, hb, roofTiles, foot, done } = L;
  const FL = opts.fortLen || 60, FW = opts.fortW || 50, WH = opts.wallH || 14, WT = opts.wallT || 3;
  const KL = opts.keepLen || 30, KW = opts.keepW || 20, KH = opts.keepH || 18;
  const stone = opts.stone || M.castleStone, trim = M.castleTrim, roof = M.castleRoof;
  // Curtain walls
  add(hb(FL, WH, WT, 14), stone, 0, WH / 2, FW / 2 - WT / 2);
  add(hb(FL, WH, WT, 14), stone, 0, WH / 2, -FW / 2 + WT / 2);
  add(hb(WT, WH, FW, 14), stone, -FL / 2 + WT / 2, WH / 2, 0);
  add(hb(WT, WH, FW, 14), stone, FL / 2 - WT / 2, WH / 2, 0);
  // Corner towers
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(new THREE.CylinderGeometry(5, 5.3, WH + 4, 20, 8), stone, sx * FL / 2, (WH + 4) / 2, sz * FW / 2);
    add(new THREE.ConeGeometry(5.5, 5, 20, 3), roof, sx * FL / 2, WH + 6.5, sz * FW / 2);
  }
  // Keep
  add(hb(KL, KH, KW, 18), stone, 0, KH / 2, 0);
  add(hb(KL + 0.4, 0.5, KW + 0.4, 4), trim, 0, KH - 0.25, 0);
  { const s = new THREE.Shape(); s.moveTo(-KW / 2 - 0.3, 0); s.lineTo(0, 6); s.lineTo(KW / 2 + 0.3, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: KL, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roof); m.rotation.y = Math.PI / 2; m.position.set(-KL / 2, KH, 0); root.add(m); L.track(m); }
  roofTiles(KL, KW / 2, 6, KH, M.castleTile || M.roofBrown, 80, 16);
  // Windows
  for (let row = 0; row < 3; row++) { const wy = 3 + row * 5;
    for (let b = 0; b < 5; b++) { const wx = -KL / 2 + 3 + b * ((KL - 6) / 4);
      for (const sz of [-1, 1]) add(hb(1.2, 2, 0.2, 3), M.glass, wx, wy, sz * (KW / 2 + 0.12)); }}
  foot(FL / 2, FW / 2);
  done(tag, KH + 6);
}

function _genericMuseum(root, cell, ctx, tag, opts = {}) {
  const L = _landmarkInit(root, cell, ctx);
  const { THREE, M, add, hb, pitchedRoof, roofTiles, foot, done } = L;
  const LEN = opts.len || 48, W = opts.w || 30, H = opts.h || 14, RH = opts.roofH || 6;
  const brick = opts.brick || M.museumBrick, stone = M.museumStone, roof = M.museumRoof;
  add(hb(LEN, H, W, 20), brick, 0, H / 2, 0);
  add(hb(LEN + 0.3, 3, W + 0.3, 6), stone, 0, 1.5, 0);
  add(hb(LEN + 0.4, 0.5, W + 0.4, 4), stone, 0, H - 0.25, 0);
  pitchedRoof(LEN, W / 2, RH, roof, H);
  roofTiles(LEN, W / 2, RH, H, M.roofBrown, 100, 20);
  // Windows
  const bays = Math.floor(LEN / 5);
  for (let row = 0; row < 2; row++) { const wy = 4 + row * 4.5;
    for (let b = 0; b < bays; b++) { const wx = -LEN / 2 + 3 + b * ((LEN - 6) / (bays - 1));
      for (const sz of [-1, 1]) { add(hb(1.5, 2.5, 0.2, 4), M.glass, wx, wy, sz * (W / 2 + 0.12));
        add(hb(1.8, 0.12, 0.25, 2), stone, wx, wy + 1.25, sz * (W / 2 + 0.12)); }}}
  // Entrance
  add(hb(8, 6, 2, 6), stone, 0, 3, W / 2 + 1);
  foot(LEN / 2, W / 2);
  done(tag, H + RH);
}

function _genericTheater(root, cell, ctx, tag, opts = {}) {
  const L = _landmarkInit(root, cell, ctx);
  const { THREE, M, add, hb, foot, done } = L;
  const LEN = opts.len || 50, W = opts.w || 35, H = opts.h || 18, FLY = opts.flyH || 8;
  const stucco = opts.stucco || M.baroqueStucco, trim = M.castleTrim, roof = M.arenaRoof;
  const glass = M.glass, red = M.theaterRed;
  add(hb(LEN, H, W, 20), stucco, 0, H / 2, 0);
  add(hb(LEN + 0.4, 0.5, W + 0.4, 4), trim, 0, H - 0.25, 0);
  add(hb(LEN + 0.3, 0.3, W + 0.3, 4), trim, 0, 7, 0);
  // Fly tower
  add(hb(LEN * 0.4, FLY, W * 0.45, 10), stucco, -LEN * 0.1, H + FLY / 2, 0);
  add(hb(LEN + 0.3, 1, W + 0.3, 6), roof, 0, H + 0.5, 0);
  // Columns
  for (let c = 0; c < 4; c++) add(new THREE.CylinderGeometry(0.45, 0.5, 10, 20, 4), stucco, -6 + c * 4, 5, W / 2 + 2);
  add(hb(16, 1.5, 3, 6), trim, 0, 10.75, W / 2 + 2);
  // Windows
  for (let row = 0; row < 3; row++) { const wy = 4 + row * 5;
    for (let b = 0; b < 8; b++) { const wx = -LEN / 2 + 4 + b * ((LEN - 8) / 7);
      add(hb(1.8, 2.6, 0.2, 4), glass, wx, wy, W / 2 + 0.12); }}
  // Marquee
  add(hb(14, 1.8, 0.3, 4), red, 0, 5, W / 2 + 3.5);
  foot(LEN / 2, W / 2 + 3);
  done(tag, H + FLY);
}

function _genericArena(root, cell, ctx, tag, opts = {}) {
  const L = _landmarkInit(root, cell, ctx);
  const { THREE, M, add, hb, foot, done } = L;
  const RX = opts.rx || 55, RZ = opts.rz || 45, BASE = 3, WH = opts.wallH || 18, DH = opts.domeH || 10;
  const metal = M.arenaMetal, roof = M.arenaRoof, glass = M.arenaGlass, concrete = M.concrete, steel = M.steel;
  // Base
  add(new THREE.CylinderGeometry(RX + 2, RX + 3, BASE, 48, 3), concrete, 0, BASE / 2, 0);
  // Oval wall
  { const g = new THREE.CylinderGeometry(1, 1, WH, 48, 6, true); const m = new THREE.Mesh(g, metal);
    m.scale.set(RX, 1, RZ); m.position.set(0, BASE + WH / 2, 0); root.add(m); L.track(m); }
  // Dome
  { const g = new THREE.SphereGeometry(1, 48, 24, 0, Math.PI * 2, 0, Math.PI / 3); const m = new THREE.Mesh(g, roof);
    m.scale.set(RX, DH / 0.5, RZ); m.position.set(0, BASE + WH, 0); root.add(m); L.track(m); }
  // Seam rings
  for (let r = 1; r < 6; r++) { const f = r / 6; const rr = RX * Math.cos(f * Math.PI / 3);
    const ry = BASE + WH + DH * Math.sin(f * Math.PI / 3);
    const t = new THREE.Mesh(new THREE.TorusGeometry(1, 0.12, 6, 64), steel);
    t.scale.set(rr, 1, rr * RZ / RX); t.position.set(0, ry, 0); t.rotation.x = Math.PI / 2; root.add(t); L.track(t); }
  // Entrances
  for (let i = 0; i < 4; i++) { const a = (i / 4) * Math.PI * 2;
    add(new THREE.BoxGeometry(10, 5, 0.8, 4, 4, 2), glass,
        Math.cos(a) * (RX + 0.8), BASE + 2.5, Math.sin(a) * (RZ + 0.8), 0, -a, 0); }
  // Facade panels
  { const pg = new THREE.BoxGeometry(1.5, WH, 0.25, 2, 6, 2); const cnt = 48;
    const inst = new THREE.InstancedMesh(pg, metal, cnt); const tmp = new THREE.Object3D();
    for (let i = 0; i < cnt; i++) { const a = (i / cnt) * Math.PI * 2;
      tmp.position.set(Math.cos(a) * (RX + 0.15), BASE + WH / 2, Math.sin(a) * (RZ + 0.15));
      tmp.rotation.set(0, -a, 0); tmp.updateMatrix(); inst.setMatrixAt(i, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); L.track(inst, cnt); }
  foot(RX, RZ);
  done(tag, BASE + WH + DH);
}

function _genericStadium(root, cell, ctx, tag, opts = {}) {
  const L = _landmarkInit(root, cell, ctx);
  const { THREE, M, add, hb, foot, done } = L;
  const FL = opts.fieldLen || 115, FW = opts.fieldW || 72;
  const SH = opts.standH || 14, SD = opts.standD || 22, RH = opts.roofH || 18;
  const concrete = M.concrete, grass = M.park, roof = M.arenaMetal, steel = M.steel;
  // Pitch
  add(hb(FL, 0.3, FW, 6), grass, 0, 0.15, 0);
  // Main stand (west)
  { const s = new THREE.Shape(); s.moveTo(0, 0); s.lineTo(SD, 0); s.lineTo(SD, SH); s.lineTo(0, SH * 0.3); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: FL - 8, bevelEnabled: false });
    const m = new THREE.Mesh(geo, concrete); m.rotation.y = -Math.PI / 2; m.position.set((FL - 8) / 2, 0, -(FW / 2 + SD)); root.add(m); L.track(m); }
  // East stand (shorter)
  { const s = new THREE.Shape(); s.moveTo(0, 0); s.lineTo(SD * 0.6, 0); s.lineTo(SD * 0.6, SH * 0.6); s.lineTo(0, SH * 0.15); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: FL - 16, bevelEnabled: false });
    const m = new THREE.Mesh(geo, concrete); m.rotation.y = Math.PI / 2; m.position.set(-(FL - 16) / 2, 0, FW / 2); root.add(m); L.track(m); }
  // End stands
  for (const sx of [-1, 1]) add(hb(SD * 0.5, SH * 0.5, FW * 0.5, 6), concrete, sx * (FL / 2 + SD * 0.25), SH * 0.25, 0);
  // Roof on main stand
  add(hb(FL - 6, 0.4, SD + 4, 6), roof, 0, RH, -(FW / 2 + SD / 2));
  // Floodlights
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(new THREE.CylinderGeometry(0.35, 0.5, 22, 6, 6), steel, sx * (FL / 2 + 4), 11, sz * (FW / 2 + SD + 2));
    add(hb(3, 1.5, 2, 3), M.white, sx * (FL / 2 + 4), 22, sz * (FW / 2 + SD + 2)); }
  const HL = FL / 2 + SD, HW = FW / 2 + SD;
  foot(HL, HW);
  done(tag, RH + 2);
}

// ── Malmö — Sankt Petri kyrka ────────────────────────────────────────────────
// Gothic brick church, ~1300s. Malmö's oldest church. ~38m x 77m.
// Red brick, tall tower with copper spire.

function buildMalmoSanktPetri(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }
  const NAVE_LEN = 70, NAVE_W = 22, NAVE_H = 22, ROOF_H = 11;
  const TOWER_W = 11, TOWER_H = 45, SPIRE_H = 50;
  const TILE_COLS = 130, TILE_ROWS = 28;
  const brick = M.churchRed, trim = M.churchTrim, roofM = M.churchRoof, spire = M.churchSpire, glass = M.glass, dark = M.roofBrown;
  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  add(hb(NAVE_LEN, NAVE_H, NAVE_W, 24), brick, 0, NAVE_H / 2, 0);
  add(hb(NAVE_LEN + 0.4, 0.5, NAVE_W + 0.4, 4), trim, 0, NAVE_H - 0.25, 0);
  // Pitched roof
  { const s = new THREE.Shape(); s.moveTo(-NAVE_W / 2 - 0.5, 0); s.lineTo(0, ROOF_H); s.lineTo(NAVE_W / 2 + 0.5, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: NAVE_LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-NAVE_LEN / 2, NAVE_H, 0); root.add(m); track(m); }
  // Roof tiles
  { const sa = Math.atan2(ROOF_H, NAVE_W / 2 + 0.5); const cnt = TILE_COLS * TILE_ROWS * 2;
    const tg = new THREE.BoxGeometry(NAVE_LEN / TILE_COLS, 0.06, (NAVE_W / 2) / Math.cos(sa) / TILE_ROWS, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, dark, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < TILE_COLS; c++) { for (let r = 0; r < TILE_ROWS; r++) {
      const frac = (r + 0.5) / TILE_ROWS;
      tmp.position.set(-NAVE_LEN / 2 + (c + 0.5) * (NAVE_LEN / TILE_COLS), NAVE_H + ROOF_H * (1 - frac) + 0.04, side * (NAVE_W / 2 + 0.5) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }
  // Tower
  const tx = -NAVE_LEN / 2 + TOWER_W / 2;
  add(hb(TOWER_W, TOWER_H, TOWER_W, 20), brick, tx, TOWER_H / 2, 0);
  add(hb(TOWER_W + 0.6, 0.6, TOWER_W + 0.6, 4), trim, tx, TOWER_H - 0.3, 0);
  // Spire
  add(new THREE.CylinderGeometry(0.2, TOWER_W / 2.3, SPIRE_H, 8, 14), spire, tx, TOWER_H + SPIRE_H / 2, 0);
  for (let r = 0; r < 7; r++) { const frac = r / 7; add(new THREE.TorusGeometry((TOWER_W / 2.3) * (1 - frac * 0.9), 0.12, 10, 64), spire, tx, TOWER_H + frac * SPIRE_H, 0, Math.PI / 2); }
  add(hb(0.12, 2.0, 0.12, 2), M.theaterGold, tx, TOWER_H + SPIRE_H + 1, 0);
  add(hb(0.8, 0.12, 0.12, 2), M.theaterGold, tx, TOWER_H + SPIRE_H + 1.6, 0);
  // Windows
  for (let w = 0; w < 10; w++) { const wx = -NAVE_LEN / 2 + 8 + w * (NAVE_LEN - 16) / 9;
    for (const sz of [-1, 1]) { add(hb(1.2, 4.0, 0.2, 4), glass, wx, NAVE_H * 0.55, sz * (NAVE_W / 2 + 0.12));
      add(new THREE.CylinderGeometry(0.6, 0.6, 0.2, 16, 1, false, 0, Math.PI), glass, wx, NAVE_H * 0.55 + 2.0, sz * (NAVE_W / 2 + 0.12), 0, 0, Math.PI / 2); }}

  root.userData.customFootprint = [[-NAVE_LEN/2, -NAVE_W/2], [NAVE_LEN/2, -NAVE_W/2], [NAVE_LEN/2, NAVE_W/2], [-NAVE_LEN/2, NAVE_W/2]];
  root.userData.baseHeight = TOWER_H + SPIRE_H + 2;
  console.log(`[malmo_sankt_petri] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Malmö — Malmöhus slott ──────────────────────────────────────────────────
// Renaissance fortress, 1434/1537. Moated. ~120m × 120m complex.
// Red brick walls, round corner bastions, central keep.

function buildMalmoMalmohus(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }
  const FORT_LEN = 70, FORT_W = 60, WALL_H = 12, WALL_T = 3;
  const KEEP_LEN = 35, KEEP_W = 25, KEEP_H = 18;
  const BAST_R = 8, BAST_H = 14;
  const MOAT_W = 12;
  const brick = M.churchRed, stone = M.castleStone, roofM = M.castleRoof, trim = M.castleTrim;
  const water = M.moatWater;
  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Moat
  add(hb(FORT_LEN + MOAT_W * 2 + 4, 0.4, FORT_W + MOAT_W * 2 + 4, 6), water, 0, 0.2, 0);
  // Island
  add(hb(FORT_LEN + 2, 0.6, FORT_W + 2, 4), M.park, 0, 0.5, 0);
  // Curtain walls
  add(hb(FORT_LEN, WALL_H, WALL_T, 16), brick, 0, WALL_H / 2, FORT_W / 2 - WALL_T / 2);
  add(hb(FORT_LEN, WALL_H, WALL_T, 16), brick, 0, WALL_H / 2, -FORT_W / 2 + WALL_T / 2);
  add(hb(WALL_T, WALL_H, FORT_W, 16), brick, -FORT_LEN / 2 + WALL_T / 2, WALL_H / 2, 0);
  add(hb(WALL_T, WALL_H, FORT_W, 16), brick, FORT_LEN / 2 - WALL_T / 2, WALL_H / 2, 0);
  // Corner bastions
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(new THREE.CylinderGeometry(BAST_R, BAST_R + 0.3, BAST_H, 24, 8), brick,
        sx * FORT_LEN / 2, BAST_H / 2, sz * FORT_W / 2);
    add(new THREE.ConeGeometry(BAST_R + 0.3, 5, 24, 3), roofM,
        sx * FORT_LEN / 2, BAST_H + 2.5, sz * FORT_W / 2);
  }
  // Central keep
  add(hb(KEEP_LEN, KEEP_H, KEEP_W, 20), brick, 0, KEEP_H / 2, 0);
  add(hb(KEEP_LEN + 0.4, 0.5, KEEP_W + 0.4, 4), trim, 0, KEEP_H - 0.25, 0);
  // Keep roof
  { const s = new THREE.Shape(); s.moveTo(-KEEP_W / 2 - 0.3, 0); s.lineTo(0, 6); s.lineTo(KEEP_W / 2 + 0.3, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: KEEP_LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-KEEP_LEN / 2, KEEP_H, 0); root.add(m); track(m); }
  // Instanced roof tiles on keep
  { const sa = Math.atan2(6, KEEP_W / 2); const cnt = 90 * 18 * 2;
    const tg = new THREE.BoxGeometry(KEEP_LEN / 90, 0.05, (KEEP_W / 2) / Math.cos(sa) / 18, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, M.castleTile, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < 90; c++) { for (let r = 0; r < 18; r++) {
      const frac = (r + 0.5) / 18;
      tmp.position.set(-KEEP_LEN / 2 + (c + 0.5) * (KEEP_LEN / 90), KEEP_H + 6 * (1 - frac) + 0.03, side * (KEEP_W / 2 + 0.3) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }
  // Keep windows
  for (let row = 0; row < 3; row++) { const wy = 3.5 + row * 5;
    for (let b = 0; b < 6; b++) { const wx = -KEEP_LEN / 2 + 3 + b * ((KEEP_LEN - 6) / 5);
      for (const sz of [-1, 1]) add(hb(1.3, 2.0, 0.2, 4), M.glass, wx, wy, sz * (KEEP_W / 2 + 0.12)); }}
  // Drawbridge
  add(hb(5, 0.4, MOAT_W + 4, 4), M.bridgeWood, 0, 1.0, FORT_W / 2 + MOAT_W / 2 + 1);
  // Gatehouse
  add(hb(8, WALL_H + 3, WALL_T + 2, 8), brick, 0, (WALL_H + 3) / 2, FORT_W / 2);
  add(hb(3, 4, 0.5, 3), M.roofBrown, 0, 2, FORT_W / 2 + WALL_T / 2 + 1.2);
  // Crenellated merlons on walls
  { const mGeo = new THREE.BoxGeometry(1.0, 1.2, WALL_T + 0.2, 2, 2, 2);
    const cnt = 50; const inst = new THREE.InstancedMesh(mGeo, brick, cnt); const tmp = new THREE.Object3D();
    for (let i = 0; i < cnt; i++) { tmp.position.set(-FORT_LEN / 2 + 1.5 + i * ((FORT_LEN - 3) / cnt), WALL_H + 0.6, FORT_W / 2 - WALL_T / 2);
      tmp.updateMatrix(); inst.setMatrixAt(i, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  const HL = FORT_LEN / 2 + MOAT_W, HW = FORT_W / 2 + MOAT_W;
  root.userData.customFootprint = [[-HL, -HW], [HL, -HW], [HL, HW], [-HL, HW]];
  root.userData.baseHeight = KEEP_H + 6;
  console.log(`[malmo_malmohus] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Malmö — Moderna Museet Malmö ────────────────────────────────────────────
// Converted 1901 power station (Rooseum). Red brick, ~55m x 37m.

function buildMalmoModernaMuseet(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }
  const LEN = 52, W = 34, H = 14, ROOF_H = 6;
  const brick = M.museumBrick, stone = M.museumStone, roofM = M.museumRoof, glass = M.glass;
  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  add(hb(LEN, H, W, 20), brick, 0, H / 2, 0);
  add(hb(LEN + 0.3, 3, W + 0.3, 6), stone, 0, 1.5, 0);
  add(hb(LEN + 0.4, 0.5, W + 0.4, 4), stone, 0, H - 0.25, 0);
  // Roof
  { const s = new THREE.Shape(); s.moveTo(-W / 2 - 0.3, 0); s.lineTo(0, ROOF_H); s.lineTo(W / 2 + 0.3, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-LEN / 2, H, 0); root.add(m); track(m); }
  // Instanced tiles
  { const sa = Math.atan2(ROOF_H, W / 2); const cnt = 110 * 22 * 2;
    const tg = new THREE.BoxGeometry(LEN / 110, 0.05, (W / 2) / Math.cos(sa) / 22, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, M.roofBrown, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < 110; c++) { for (let r = 0; r < 22; r++) {
      const frac = (r + 0.5) / 22;
      tmp.position.set(-LEN / 2 + (c + 0.5) * (LEN / 110), H + ROOF_H * (1 - frac) + 0.03, side * (W / 2 + 0.3) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }
  // Industrial-style tall windows
  for (let w = 0; w < 8; w++) { const wx = -LEN / 2 + 4 + w * ((LEN - 8) / 7);
    for (const sz of [-1, 1]) { add(hb(2.0, 5.0, 0.2, 4), glass, wx, H * 0.5, sz * (W / 2 + 0.12));
      add(hb(2.3, 0.12, 0.25, 2), stone, wx, H * 0.5 + 2.5, sz * (W / 2 + 0.12)); }}
  // Modern glass entrance extension
  add(hb(14, 8, 5, 8), M.arenaGlass, 0, 4, W / 2 + 2.5);
  add(hb(14, 0.4, 6, 4), stone, 0, 8.2, W / 2 + 2.5);

  root.userData.customFootprint = [[-LEN/2, -W/2], [LEN/2, -W/2], [LEN/2, W/2 + 5], [-LEN/2, W/2 + 5]];
  root.userData.baseHeight = H + ROOF_H;
  console.log(`[malmo_moderna_museet] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Malmö — Malmö Opera ─────────────────────────────────────────────────────
// Functionalist opera house, 1944. ~107m x 146m. Red facade, glass front.

function buildMalmoOpera(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }
  const LEN = 100, W = 55, H = 20, FLY_H = 12;
  const brick = M.brickRed, trim = M.castleTrim, roofM = M.arenaRoof;
  const glass = M.glass, red = M.theaterRed;
  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main body
  add(hb(LEN, H, W, 24), brick, 0, H / 2, 0);
  add(hb(LEN + 0.4, 0.5, W + 0.4, 4), trim, 0, H - 0.25, 0);
  // Fly tower
  add(hb(LEN * 0.35, FLY_H, W * 0.45, 12), brick, -LEN * 0.1, H + FLY_H / 2, 0);
  // Flat roof
  add(hb(LEN + 0.3, 1, W + 0.3, 6), roofM, 0, H + 0.5, 0);
  // Glass front facade
  add(hb(LEN * 0.6, H * 0.75, 0.3, 12, 12, 2), glass, 0, H * 0.4, W / 2 + 0.2);
  // Functionalist horizontal bands
  for (let row = 0; row < 5; row++) add(hb(LEN + 0.1, 0.2, W + 0.1, 4), trim, 0, 3 + row * 4, 0);
  // Side windows (regular grid)
  for (const sx of [-1, 1]) { for (let row = 0; row < 3; row++) { const wy = 5 + row * 5.5;
    for (let b = 0; b < 10; b++) { const wz = -W / 2 + 4 + b * ((W - 8) / 9);
      add(new THREE.BoxGeometry(0.2, 2.5, 2.0, 2, 4, 4), glass, sx * (LEN / 2 + 0.12), wy, wz); }}}
  // Entrance portico
  add(hb(25, 8, 4, 8), trim, 0, 4, W / 2 + 2);
  for (let c = 0; c < 6; c++) add(new THREE.CylinderGeometry(0.5, 0.5, 8, 20, 4), trim, -10 + c * 4, 4, W / 2 + 4);
  // Marquee
  add(hb(18, 2, 0.3, 4), red, 0, 9, W / 2 + 4.2);
  // Instanced facade panels
  { const cnt = 120; const pg = new THREE.BoxGeometry(LEN / 30, H * 0.6, 0.15, 2, 6, 2);
    const inst = new THREE.InstancedMesh(pg, brick, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const sz of [-1, 1]) { for (let c = 0; c < 60; c++) {
      tmp.position.set(-LEN / 2 + 0.8 + c * (LEN / 60), H * 0.35, sz * (W / 2 + 0.1));
      tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  root.userData.customFootprint = [[-LEN/2, -W/2], [LEN/2, -W/2], [LEN/2, W/2 + 4], [-LEN/2, W/2 + 4]];
  root.userData.baseHeight = H + FLY_H;
  console.log(`[malmo_opera] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Malmö — Eleda Stadion ───────────────────────────────────────────────────
// Modern football stadium, 2009. ~194m × 216m. Capacity ~22,500.
// White tensile roof canopy. Home of Malmö FF.

function buildMalmoEledaStadion(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }
  const FIELD_LEN = 115, FIELD_W = 75;
  const STAND_H = 18, STAND_D = 28;
  const ROOF_H = 24;
  const metal = M.white, roofM = M.arenaMetal, concrete = M.concrete;
  const grass = M.park, steel = M.steel;
  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Pitch
  add(hb(FIELD_LEN, 0.3, FIELD_W, 6), grass, 0, 0.15, 0);
  // Four stands (trapezoidal cross-section)
  for (const [isLong, sx, sz] of [[true, 0, -1], [true, 0, 1], [false, -1, 0], [false, 1, 0]]) {
    const len = isLong ? FIELD_LEN + 10 : FIELD_W - 10;
    const s = new THREE.Shape();
    s.moveTo(0, 0); s.lineTo(STAND_D, 0); s.lineTo(STAND_D, STAND_H); s.lineTo(0, STAND_H * 0.3); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: len, bevelEnabled: false });
    const m = new THREE.Mesh(geo, concrete);
    if (isLong) { m.rotation.y = sz > 0 ? -Math.PI / 2 : Math.PI / 2;
      m.position.set(sz > 0 ? len / 2 : -len / 2, 0, sz * (FIELD_W / 2)); }
    else { m.rotation.y = sx > 0 ? Math.PI : 0;
      m.position.set(sx * (FIELD_LEN / 2), 0, sx > 0 ? len / 2 : -len / 2); }
    root.add(m); track(m);
  }
  // Roof canopy (4 curved sheets)
  for (const [isLong, sx, sz] of [[true, 0, -1], [true, 0, 1], [false, -1, 0], [false, 1, 0]]) {
    const len = isLong ? FIELD_LEN + 12 : FIELD_W - 8;
    const roofGeo = hb(len, 0.5, STAND_D + 5, 8);
    const rm = new THREE.Mesh(roofGeo, roofM);
    if (isLong) rm.position.set(0, ROOF_H, sz * (FIELD_W / 2 + STAND_D / 2));
    else rm.position.set(sx * (FIELD_LEN / 2 + STAND_D / 2), ROOF_H, 0);
    root.add(rm); track(rm);
  }
  // Roof support columns (V-shaped supports at corners)
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    const px = sx * (FIELD_LEN / 2 + STAND_D / 2);
    const pz = sz * (FIELD_W / 2 + STAND_D / 2);
    add(new THREE.CylinderGeometry(0.4, 0.6, ROOF_H, 8, 6), steel, px, ROOF_H / 2, pz);
  }
  // Instanced seating (colored strips)
  { const seatGeo = new THREE.BoxGeometry(FIELD_LEN + 8, 0.12, 0.5, 4, 1, 2);
    const cnt = 20; const inst = new THREE.InstancedMesh(seatGeo, M.arenaGlass, cnt);
    const tmp = new THREE.Object3D();
    for (let r = 0; r < cnt; r++) { const frac = r / cnt;
      tmp.position.set(0, 1 + frac * STAND_H, -(FIELD_W / 2 + 1 + frac * (STAND_D - 1)));
      tmp.updateMatrix(); inst.setMatrixAt(r, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }
  // Floodlights (4 on roof edges)
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(hb(4, 2.5, 4, 3), M.white, sx * (FIELD_LEN / 2), ROOF_H + 1.5, sz * (FIELD_W / 2 + STAND_D)); }

  const HL = FIELD_LEN / 2 + STAND_D + 2, HW = FIELD_W / 2 + STAND_D + 2;
  root.userData.customFootprint = [[-HL, -HW], [HL, -HW], [HL, HW], [-HL, HW]];
  root.userData.baseHeight = ROOF_H + 3;
  console.log(`[malmo_eleda_stadion] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Uppsala — Uppsala domkyrka ───────────────────────────────────────────────
// Scandinavia's largest cathedral. Gothic brick, ~119m long, twin spires ~118m.
// Red brick with copper-patina spires. Built 1270–1435.

function buildUppsalaDomkyrka(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const NAVE_LEN = 107, NAVE_W = 20, NAVE_H = 27, ROOF_H = 14;
  const AISLE_W = 7, AISLE_H = 14;
  const TOWER_W = 14, TOWER_H = 55, SPIRE_H = 60;
  const TILE_COLS = 140, TILE_ROWS = 30;

  const brick = M.churchRed, trim = M.churchTrim, roofM = M.churchRoof;
  const spire = M.churchSpire, glass = M.glass, dark = M.roofBrown;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Central nave
  add(hb(NAVE_LEN, NAVE_H, NAVE_W, 28), brick, 0, NAVE_H / 2, 0);
  // Side aisles
  for (const sz of [-1, 1]) add(hb(NAVE_LEN - 20, AISLE_H, AISLE_W, 20), brick, 0, AISLE_H / 2, sz * (NAVE_W / 2 + AISLE_W / 2));

  // String courses
  add(hb(NAVE_LEN + 0.4, 0.5, NAVE_W + AISLE_W * 2 + 0.4, 4), trim, 0, NAVE_H - 0.25, 0);
  add(hb(NAVE_LEN + 0.3, 0.35, NAVE_W + AISLE_W * 2 + 0.3, 4), trim, 0, AISLE_H - 0.2, 0);

  // Main pitched roof
  { const s = new THREE.Shape(); s.moveTo(-NAVE_W / 2 - 0.5, 0); s.lineTo(0, ROOF_H); s.lineTo(NAVE_W / 2 + 0.5, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: NAVE_LEN + 0.4, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-NAVE_LEN / 2 - 0.2, NAVE_H, 0); root.add(m); track(m); }

  // Aisle lean-to roofs
  for (const sz of [-1, 1]) {
    const s = new THREE.Shape();
    s.moveTo(0, 0); s.lineTo(AISLE_W + 0.5, 0); s.lineTo(0, 5); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: NAVE_LEN - 20, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM);
    m.rotation.y = Math.PI / 2;
    m.position.set(-(NAVE_LEN - 20) / 2, AISLE_H, sz > 0 ? NAVE_W / 2 : -(NAVE_W / 2 + AISLE_W + 0.5));
    root.add(m); track(m);
  }

  // Instanced roof tiles on main roof
  { const sa = Math.atan2(ROOF_H, NAVE_W / 2 + 0.5); const cnt = TILE_COLS * TILE_ROWS * 2;
    const tg = new THREE.BoxGeometry((NAVE_LEN + 0.4) / TILE_COLS, 0.06, (NAVE_W / 2) / Math.cos(sa) / TILE_ROWS, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, dark, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < TILE_COLS; c++) { for (let r = 0; r < TILE_ROWS; r++) {
      const frac = (r + 0.5) / TILE_ROWS;
      tmp.position.set(-NAVE_LEN / 2 + (c + 0.5) * (NAVE_LEN / TILE_COLS), NAVE_H + ROOF_H * (1 - frac) + 0.04, side * (NAVE_W / 2 + 0.5) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Twin towers at west end
  for (const sz of [-1, 1]) {
    const tx = -NAVE_LEN / 2 + TOWER_W / 2;
    const tz = sz * (NAVE_W / 2 + AISLE_W / 2);
    add(hb(TOWER_W, TOWER_H, TOWER_W, 20), brick, tx, TOWER_H / 2, tz);
    add(hb(TOWER_W + 0.6, 0.6, TOWER_W + 0.6, 4), trim, tx, TOWER_H - 0.3, tz);
    // Corner pinnacles at tower top
    for (const [sx2, sz2] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
      add(new THREE.CylinderGeometry(0.15, 0.5, 4, 8, 4), trim,
          tx + sx2 * (TOWER_W / 2 - 0.5), TOWER_H + 2, tz + sz2 * (TOWER_W / 2 - 0.5));
    }
    // Tall slender spire (octagonal taper)
    add(new THREE.CylinderGeometry(0.3, TOWER_W / 2.5, SPIRE_H, 8, 16), spire,
        tx, TOWER_H + SPIRE_H / 2, tz);
    // Decorative torus rings on spire
    for (let r = 0; r < 8; r++) { const frac = r / 8;
      const sr = (TOWER_W / 2.5) * (1 - frac * 0.85);
      add(new THREE.TorusGeometry(sr, 0.12, 12, 64), spire,
          tx, TOWER_H + frac * SPIRE_H, tz, Math.PI / 2); }
    // Cross at very top
    add(hb(0.12, 2.5, 0.12, 2), M.theaterGold, tx, TOWER_H + SPIRE_H + 1.25, tz);
    add(hb(1.0, 0.12, 0.12, 2), M.theaterGold, tx, TOWER_H + SPIRE_H + 2.0, tz);
  }

  // Clerestory windows
  for (let w = 0; w < 16; w++) { const wx = -NAVE_LEN / 2 + 12 + w * (NAVE_LEN - 24) / 15;
    for (const sz of [-1, 1]) {
      add(hb(1.4, 5.0, 0.2, 4), glass, wx, NAVE_H * 0.65, sz * (NAVE_W / 2 + 0.12));
      add(new THREE.CylinderGeometry(0.7, 0.7, 0.2, 16, 1, false, 0, Math.PI), glass,
          wx, NAVE_H * 0.65 + 2.5, sz * (NAVE_W / 2 + 0.12), 0, 0, Math.PI / 2);
    }}
  // Aisle windows
  for (let w = 0; w < 12; w++) { const wx = -NAVE_LEN / 2 + 15 + w * (NAVE_LEN - 30) / 11;
    for (const sz of [-1, 1]) {
      const wz = sz * (NAVE_W / 2 + AISLE_W + 0.12);
      add(hb(1.0, 3.0, 0.2, 4), glass, wx, AISLE_H * 0.5, wz);
    }}

  // West rose window
  add(new THREE.CylinderGeometry(3.5, 3.5, 0.3, 32), glass,
      -NAVE_LEN / 2 - 0.2, NAVE_H * 0.7, 0, 0, 0, Math.PI / 2);
  add(new THREE.TorusGeometry(3.5, 0.25, 8, 32), trim,
      -NAVE_LEN / 2 - 0.15, NAVE_H * 0.7, 0, 0, Math.PI / 2);

  const HW = NAVE_W / 2 + AISLE_W;
  root.userData.customFootprint = [[-NAVE_LEN/2, -HW], [NAVE_LEN/2, -HW], [NAVE_LEN/2, HW], [-NAVE_LEN/2, HW]];
  root.userData.baseHeight = TOWER_H + SPIRE_H + 3;
  console.log(`[uppsala_domkyrka] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Uppsala — Uppsala slott (Uppsala Castle) ────────────────────────────────
// Pink Renaissance castle on Kasåsberget hill. Built 1540s. ~100m long.
// Distinctive salmon-pink stucco walls, two round corner bastions.

function buildUppsalaSlott(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const MAIN_LEN = 95, MAIN_W = 25, WALL_H = 18, ROOF_H = 7;
  const BAST_R = 9, BAST_H = 22;
  const pink = new THREE.MeshLambertMaterial({ color: 0xd4917a }); // salmon pink
  const trim = M.castleTrim, roofM = M.castleRoof, glass = M.glass;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main body
  add(hb(MAIN_LEN, WALL_H, MAIN_W, 24), pink, 0, WALL_H / 2, 0);
  // Cornice
  add(hb(MAIN_LEN + 0.4, 0.6, MAIN_W + 0.4, 4), trim, 0, WALL_H - 0.3, 0);
  add(hb(MAIN_LEN + 0.3, 0.3, MAIN_W + 0.3, 4), trim, 0, WALL_H * 0.5, 0);

  // Pitched roof
  { const s = new THREE.Shape(); s.moveTo(-MAIN_W / 2 - 0.3, 0); s.lineTo(0, ROOF_H); s.lineTo(MAIN_W / 2 + 0.3, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: MAIN_LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-MAIN_LEN / 2, WALL_H, 0); root.add(m); track(m); }

  // Two round corner bastions (SE and NE corners)
  for (const sx of [-1, 1]) {
    const bx = sx * (MAIN_LEN / 2 - 2);
    add(new THREE.CylinderGeometry(BAST_R, BAST_R + 0.3, BAST_H, 32, 10), pink, bx, BAST_H / 2, MAIN_W / 2 + BAST_R * 0.3);
    // Bastion cornice
    add(new THREE.TorusGeometry(BAST_R + 0.2, 0.25, 8, 48), trim, bx, BAST_H - 0.3, MAIN_W / 2 + BAST_R * 0.3, Math.PI / 2);
    // Conical roof on bastion
    add(new THREE.ConeGeometry(BAST_R + 0.3, 8, 32, 4), roofM, bx, BAST_H + 4, MAIN_W / 2 + BAST_R * 0.3);
  }

  // Instanced roof tiles
  { const sa = Math.atan2(ROOF_H, MAIN_W / 2); const cnt = 120 * 24 * 2;
    const tg = new THREE.BoxGeometry(MAIN_LEN / 120, 0.06, (MAIN_W / 2) / Math.cos(sa) / 24, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, M.castleTile, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < 120; c++) { for (let r = 0; r < 24; r++) {
      const frac = (r + 0.5) / 24;
      tmp.position.set(-MAIN_LEN / 2 + (c + 0.5) * (MAIN_LEN / 120), WALL_H + ROOF_H * (1 - frac) + 0.04, side * (MAIN_W / 2 + 0.3) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Windows (3 rows on south facade)
  for (let row = 0; row < 3; row++) { const wy = 3.5 + row * 5;
    for (let b = 0; b < 18; b++) { const wx = -MAIN_LEN / 2 + 4 + b * ((MAIN_LEN - 8) / 17);
      add(hb(1.5, 2.2, 0.2, 4), glass, wx, wy, MAIN_W / 2 + 0.12);
      add(hb(1.8, 0.12, 0.25, 2), trim, wx, wy + 1.1, MAIN_W / 2 + 0.12);
    }}

  // Central entrance portal
  add(hb(6, 7, 2, 8), trim, 0, 3.5, MAIN_W / 2 + 1);
  add(hb(3.5, 5, 0.3, 4), M.roofBrown, 0, 2.5, MAIN_W / 2 + 2.2);

  root.userData.customFootprint = [[-MAIN_LEN/2, -MAIN_W/2], [MAIN_LEN/2, -MAIN_W/2], [MAIN_LEN/2, MAIN_W/2 + BAST_R], [-MAIN_LEN/2, MAIN_W/2 + BAST_R]];
  root.userData.baseHeight = BAST_H + 8;
  console.log(`[uppsala_slott] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Uppsala — Gustavianum ───────────────────────────────────────────────────
// 17th-century university museum with iconic copper cupola (Anatomical Theatre).
// ~64m x 20m, 3 stories, baroque cupola on roof.

function buildUppsalaGustavianum(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const LEN = 55, W = 18, H = 14, ROOF_H = 7;
  const CUPOLA_R = 5, CUPOLA_H = 8;
  const stucco = M.baroqueStucco, trim = M.castleTrim, roofM = M.churchRoof;
  const glass = M.glass, copper = M.churchSpire;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main body
  add(hb(LEN, H, W, 20), stucco, 0, H / 2, 0);
  add(hb(LEN + 0.3, 0.4, W + 0.3, 4), trim, 0, H - 0.2, 0);

  // Pitched roof
  { const s = new THREE.Shape(); s.moveTo(-W / 2 - 0.3, 0); s.lineTo(0, ROOF_H); s.lineTo(W / 2 + 0.3, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-LEN / 2, H, 0); root.add(m); track(m); }

  // The famous Anatomical Theatre cupola
  // Octagonal drum
  add(new THREE.CylinderGeometry(CUPOLA_R, CUPOLA_R + 0.5, 3, 8, 4), stucco, 0, H + ROOF_H / 2 + 1.5, 0);
  // Dome
  add(new THREE.SphereGeometry(CUPOLA_R, 32, 24, 0, Math.PI * 2, 0, Math.PI / 2.5), copper, 0, H + ROOF_H / 2 + 3, 0);
  // Lantern
  add(new THREE.CylinderGeometry(1.2, 1.8, 3, 8, 4), copper, 0, H + ROOF_H / 2 + 3 + CUPOLA_H * 0.6, 0);
  add(new THREE.SphereGeometry(0.5, 12, 8), M.theaterGold, 0, H + ROOF_H / 2 + 3 + CUPOLA_H * 0.6 + 2, 0);
  // Torus rings on dome
  for (let r = 0; r < 4; r++) { const frac = r / 4;
    add(new THREE.TorusGeometry(CUPOLA_R * (1 - frac * 0.6), 0.1, 8, 48), copper,
        0, H + ROOF_H / 2 + 3 + frac * CUPOLA_H * 0.5, 0, Math.PI / 2); }

  // Windows (2 rows)
  for (let row = 0; row < 2; row++) { const wy = 4 + row * 5;
    for (let b = 0; b < 10; b++) { const wx = -LEN / 2 + 3.5 + b * ((LEN - 7) / 9);
      for (const sz of [-1, 1]) {
        add(hb(1.3, 2.2, 0.2, 4), glass, wx, wy, sz * (W / 2 + 0.12));
        add(hb(1.5, 0.12, 0.25, 2), trim, wx, wy + 1.1, sz * (W / 2 + 0.12));
      }}}

  // Cupola windows (8 around the drum)
  for (let i = 0; i < 8; i++) { const a = (i / 8) * Math.PI * 2;
    add(hb(1.0, 1.8, 0.2, 3), glass,
        Math.cos(a) * (CUPOLA_R + 0.12), H + ROOF_H / 2 + 1.5, Math.sin(a) * (CUPOLA_R + 0.12), 0, -a, 0); }

  // Instanced roof tiles
  { const sa = Math.atan2(ROOF_H, W / 2); const cnt = 110 * 22 * 2;
    const tg = new THREE.BoxGeometry(LEN / 110, 0.05, (W / 2) / Math.cos(sa) / 22, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, M.roofBrown, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < 110; c++) { for (let r = 0; r < 22; r++) {
      const frac = (r + 0.5) / 22;
      tmp.position.set(-LEN / 2 + (c + 0.5) * (LEN / 110), H + ROOF_H * (1 - frac) + 0.03, side * (W / 2 + 0.3) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  root.userData.customFootprint = [[-LEN/2, -W/2], [LEN/2, -W/2], [LEN/2, W/2], [-LEN/2, W/2]];
  root.userData.baseHeight = H + ROOF_H + CUPOLA_H + 3;
  console.log(`[uppsala_gustavianum] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Uppsala — Uppsala Konsert & Kongress (UKK) ─────────────────────────────
// Modern concert hall and congress center. ~71m x 70m.

function buildUppsalaUKK(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const LEN = 70, W = 65, H = 18, FLY_H = 8;
  const metal = M.arenaMetal, glass = M.arenaGlass, concrete = M.concrete;
  const roofM = M.arenaRoof;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main volume
  add(hb(LEN, H, W, 20), metal, 0, H / 2, 0);
  // Glass curtain wall facade
  add(hb(LEN + 0.2, H * 0.8, 0.3, 12, 12, 2), glass, 0, H * 0.45, W / 2 + 0.2);
  // Fly tower
  add(hb(LEN * 0.4, FLY_H, W * 0.5, 10), metal, -LEN * 0.15, H + FLY_H / 2, 0);
  // Roof
  add(hb(LEN + 0.5, 1.5, W + 0.5, 8), roofM, 0, H + 0.75, 0);

  // Horizontal metal panel bands
  for (let row = 0; row < 6; row++) add(hb(LEN + 0.1, 0.15, W + 0.1, 4), M.steel, 0, 2 + row * 3, 0);

  // Entrance canopy
  add(hb(20, 0.8, 8, 6), metal, 0, 5, W / 2 + 4);
  // Support columns
  for (let c = 0; c < 4; c++) add(new THREE.CylinderGeometry(0.3, 0.3, 5, 12, 4), M.steel, -6 + c * 4, 2.5, W / 2 + 7);

  // Instanced facade panels
  { const cnt = 100;
    const pg = new THREE.BoxGeometry(LEN / 25, H * 0.7, 0.2, 2, 6, 2);
    const inst = new THREE.InstancedMesh(pg, metal, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const sz of [-1, 1]) { for (let c = 0; c < 50; c++) {
      tmp.position.set(-LEN / 2 + 0.7 + c * (LEN / 50), H * 0.4, sz * (W / 2 + 0.15));
      tmp.rotation.set(0, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  root.userData.customFootprint = [[-LEN/2, -W/2], [LEN/2, -W/2], [LEN/2, W/2 + 4], [-LEN/2, W/2 + 4]];
  root.userData.baseHeight = H + FLY_H;
  console.log(`[uppsala_ukk] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Uppsala — Studenternas IP ───────────────────────────────────────────────
// Historic open-air multi-sport stadium, home of IK Sirius.
// ~167m x 109m. Classic Swedish grandstand with covered main stand.

function buildUppsalaStudenternas(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const FIELD_LEN = 120, FIELD_W = 75;
  const STAND_H = 12, STAND_D = 18;
  const ROOF_SPAN = 22;
  const metal = M.arenaMetal, concrete = M.concrete, roofM = M.arenaRoof;
  const grass = M.park, steel = M.steel;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Green pitch
  add(hb(FIELD_LEN, 0.3, FIELD_W, 8), grass, 0, 0.15, 0);

  // Main stand (west side — taller, with roof)
  { const s = new THREE.Shape();
    s.moveTo(0, 0); s.lineTo(STAND_D, 0); s.lineTo(STAND_D, STAND_H); s.lineTo(0, STAND_H * 0.3); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: FIELD_LEN - 10, bevelEnabled: false });
    const m = new THREE.Mesh(geo, concrete); m.rotation.y = -Math.PI / 2;
    m.position.set((FIELD_LEN - 10) / 2, 0, -(FIELD_W / 2 + STAND_D)); root.add(m); track(m); }
  // Main stand roof
  add(hb(FIELD_LEN - 8, 0.4, ROOF_SPAN, 8), roofM, 0, STAND_H + 0.2, -(FIELD_W / 2 + STAND_D / 2));
  // Roof support columns
  for (let c = 0; c < 10; c++) add(new THREE.CylinderGeometry(0.25, 0.25, STAND_H, 8, 4), steel,
      -FIELD_LEN / 2 + 10 + c * ((FIELD_LEN - 20) / 9), STAND_H / 2, -(FIELD_W / 2 + 2));

  // Opposite stand (east — smaller, no roof)
  { const s = new THREE.Shape();
    s.moveTo(0, 0); s.lineTo(STAND_D * 0.6, 0); s.lineTo(STAND_D * 0.6, STAND_H * 0.6); s.lineTo(0, STAND_H * 0.15); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: FIELD_LEN - 20, bevelEnabled: false });
    const m = new THREE.Mesh(geo, concrete); m.rotation.y = Math.PI / 2;
    m.position.set(-(FIELD_LEN - 20) / 2, 0, FIELD_W / 2); root.add(m); track(m); }

  // End stands (shorter)
  for (const sx of [-1, 1]) {
    add(hb(STAND_D * 0.5, STAND_H * 0.5, FIELD_W * 0.6, 8), concrete,
        sx * (FIELD_LEN / 2 + STAND_D * 0.25), STAND_H * 0.25, 0);
  }

  // Instanced seating rows (thin colored strips)
  { const seatGeo = new THREE.BoxGeometry(FIELD_LEN - 12, 0.15, 0.6, 4, 1, 2);
    const cnt = 16; const inst = new THREE.InstancedMesh(seatGeo, M.steel, cnt);
    const tmp = new THREE.Object3D();
    for (let r = 0; r < cnt; r++) {
      const frac = r / cnt;
      tmp.position.set(0, 0.5 + frac * STAND_H, -(FIELD_W / 2 + 2 + frac * (STAND_D - 2)));
      tmp.updateMatrix(); inst.setMatrixAt(r, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Floodlight towers (4 corners)
  for (const [sx, sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    const fx = sx * (FIELD_LEN / 2 + 5), fz = sz * (FIELD_W / 2 + STAND_D + 3);
    add(new THREE.CylinderGeometry(0.4, 0.5, 25, 8, 8), steel, fx, 12.5, fz);
    add(hb(3, 2, 2, 3), M.white, fx, 25, fz); }

  const HL = FIELD_LEN / 2 + STAND_D, HW = FIELD_W / 2 + STAND_D;
  root.userData.customFootprint = [[-HL, -HW], [HL, -HW], [HL, HW], [-HL, HW]];
  root.userData.baseHeight = STAND_H + 2;
  console.log(`[uppsala_studenternas] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Göteborg — Göteborgs domkyrka ────────────────────────────────────────────
// Neoclassical cathedral (1815). Single tower ~60m, pale stucco facade.
// ~55m long, 30m wide.

function buildGoteborgDomkyrka(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) {
      const dx = x - cx, dz = z - cz;
      sxx += dx * dx; szz += dz * dz; sxz += dx * dz;
    }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const NAVE_LEN = 55, NAVE_W = 22, NAVE_H = 18, ROOF_H = 9;
  const TOWER_W = 10, TOWER_H = 38, CAP_H = 18;
  const TILE_COLS = 130, TILE_ROWS = 26;
  const stucco = M.baroqueStucco, trim = M.castleTrim, roofM = M.churchRoof;
  const glass = M.glass, spire = M.churchSpire;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Nave
  add(hb(NAVE_LEN, NAVE_H, NAVE_W, 24), stucco, 0, NAVE_H / 2, 0);
  add(hb(NAVE_LEN + 0.4, 0.5, NAVE_W + 0.4, 4), trim, 0, NAVE_H - 0.25, 0);
  add(hb(NAVE_LEN + 0.3, 0.35, NAVE_W + 0.3, 4), trim, 0, NAVE_H * 0.45, 0);

  // Pitched roof
  { const s = new THREE.Shape(); s.moveTo(-NAVE_W / 2 - 0.5, 0); s.lineTo(0, ROOF_H); s.lineTo(NAVE_W / 2 + 0.5, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: NAVE_LEN + 0.4, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-NAVE_LEN / 2 - 0.2, NAVE_H, 0); root.add(m); track(m); }

  // Instanced roof tiles
  { const tg = new THREE.BoxGeometry((NAVE_LEN + 0.4) / TILE_COLS, 0.06, (NAVE_W / 2 + 0.5) / Math.cos(Math.atan2(ROOF_H, NAVE_W / 2)) / TILE_ROWS, 2, 1, 2);
    const sa = Math.atan2(ROOF_H, NAVE_W / 2 + 0.5); const cnt = TILE_COLS * TILE_ROWS * 2;
    const inst = new THREE.InstancedMesh(tg, M.roofBrown, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < TILE_COLS; c++) { for (let r = 0; r < TILE_ROWS; r++) {
      const tx = -NAVE_LEN / 2 + (c + 0.5) * (NAVE_LEN / TILE_COLS);
      const frac = (r + 0.5) / TILE_ROWS;
      tmp.position.set(tx, NAVE_H + ROOF_H * (1 - frac) + 0.04, side * (NAVE_W / 2 + 0.5) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Tower (west end)
  const tx = -NAVE_LEN / 2 + TOWER_W / 2;
  add(hb(TOWER_W, TOWER_H, TOWER_W, 20), stucco, tx, TOWER_H / 2, 0);
  add(hb(TOWER_W + 0.6, 0.6, TOWER_W + 0.6, 4), trim, tx, TOWER_H - 0.3, 0);
  // Clock faces
  for (const sz of [-1, 1]) add(new THREE.CylinderGeometry(1.5, 1.5, 0.2, 24), trim, tx, TOWER_H - 5, sz * (TOWER_W / 2 + 0.12), 0, 0, Math.PI / 2);
  // Neoclassical tower cap (dome-like)
  add(new THREE.CylinderGeometry(1.0, TOWER_W / 2.2, CAP_H, 8, 10), spire, tx, TOWER_H + CAP_H / 2, 0);
  // Cross
  add(hb(0.15, 2.5, 0.15, 2), M.theaterGold, tx, TOWER_H + CAP_H + 1.25, 0);
  add(hb(1.2, 0.15, 0.15, 2), M.theaterGold, tx, TOWER_H + CAP_H + 2.0, 0);
  // Torus rings on cap
  for (let r = 0; r < 5; r++) { const frac = r / 5; add(new THREE.TorusGeometry(TOWER_W / 2.2 * (1 - frac * 0.75), 0.12, 12, 64), spire, tx, TOWER_H + frac * CAP_H, 0, Math.PI / 2); }

  // Windows
  for (let w = 0; w < 9; w++) { const wx = -NAVE_LEN / 2 + 5 + w * (NAVE_LEN - 10) / 8;
    for (const sz of [-1, 1]) { add(hb(1.4, 3.0, 0.2, 4), glass, wx, NAVE_H * 0.5, sz * (NAVE_W / 2 + 0.12));
      add(new THREE.CylinderGeometry(0.7, 0.7, 0.2, 16, 1, false, 0, Math.PI), glass, wx, NAVE_H * 0.5 + 1.5, sz * (NAVE_W / 2 + 0.12), 0, 0, Math.PI / 2); }}

  // Entrance portico (neoclassical columns at west)
  for (let c = 0; c < 4; c++) { const cxp = tx - TOWER_W / 2 - 2; const cz = -3 + c * 2;
    add(new THREE.CylinderGeometry(0.4, 0.45, 8, 20, 4), stucco, cxp, 4, cz); }
  add(hb(3, 1.5, 8, 6), trim, tx - TOWER_W / 2 - 2, 8.75, 0);

  root.userData.customFootprint = [[-NAVE_LEN/2, -NAVE_W/2], [NAVE_LEN/2, -NAVE_W/2], [NAVE_LEN/2, NAVE_W/2], [-NAVE_LEN/2, NAVE_W/2]];
  root.userData.baseHeight = TOWER_H + CAP_H + 3;
  console.log(`[goteborg_domkyrka] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Göteborg — Skansen Kronan ───────────────────────────────────────────────
// 17th-century circular fortress/redoubt on Risåsberget hill.
// ~30m diameter, 3 stories, thick stone walls, copper dome roof.

function buildGoteborgSkansenKronan(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const R = 15, WALL_H = 14, WALL_T = 3, DOME_H = 8;
  const stone = M.castleStone, roofM = M.castleRoof, trim = M.castleTrim;
  const glass = M.glass;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };

  // Circular stone tower
  add(new THREE.CylinderGeometry(R, R + 0.5, WALL_H, 48, 12), stone, 0, WALL_H / 2, 0);
  // Inner hollow (courtyard)
  add(new THREE.CylinderGeometry(R - WALL_T, R - WALL_T + 0.3, WALL_H + 0.5, 48, 8), stone, 0, WALL_H / 2, 0);

  // String courses
  for (const h of [4, 8, 12]) add(new THREE.TorusGeometry(R + 0.2, 0.2, 8, 96), trim, 0, h, 0, Math.PI / 2);

  // Crenellated parapet (instanced merlons)
  { const mGeo = new THREE.BoxGeometry(1.2, 1.5, WALL_T + 0.2, 3, 3, 3);
    const cnt = 36; const inst = new THREE.InstancedMesh(mGeo, stone, cnt);
    const tmp = new THREE.Object3D();
    for (let i = 0; i < cnt; i++) { const a = (i / cnt) * Math.PI * 2;
      tmp.position.set(Math.cos(a) * R, WALL_H + 0.75, Math.sin(a) * R);
      tmp.rotation.set(0, -a, 0); tmp.updateMatrix(); inst.setMatrixAt(i, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Copper dome roof
  add(new THREE.SphereGeometry(R - 0.5, 48, 24, 0, Math.PI * 2, 0, Math.PI / 3), roofM, 0, WALL_H, 0);
  // Dome lantern
  add(new THREE.CylinderGeometry(1.5, 2.5, 4, 16, 4), roofM, 0, WALL_H + DOME_H - 2, 0);
  add(new THREE.SphereGeometry(0.8, 12, 8), M.theaterGold, 0, WALL_H + DOME_H + 0.5, 0);

  // Gun ports (small dark rectangles around the circumference)
  for (let row = 0; row < 2; row++) { for (let i = 0; i < 12; i++) {
    const a = (i / 12) * Math.PI * 2;
    const gy = 3 + row * 5;
    add(new THREE.BoxGeometry(0.8, 0.6, 0.5, 2, 2, 2), glass,
        Math.cos(a) * (R + 0.3), gy, Math.sin(a) * (R + 0.3), 0, -a, 0);
  }}

  // Entrance (arched door on one side)
  add(new THREE.BoxGeometry(3, 4, WALL_T + 1, 4, 4, 4), trim, R - 1, 2, 0);

  root.userData.customFootprint = [[-R, -R], [R, -R], [R, R], [-R, R]];
  root.userData.baseHeight = WALL_H + DOME_H + 1;
  console.log(`[goteborg_skansen_kronan] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Göteborg — Sjöfartsmuseet (Maritime Museum) ─────────────────────────────
// 1930s neoclassical brick museum. ~50m x 40m, 3 stories.

function buildGoteborgSjofartsmuseet(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const LEN = 50, W = 40, H = 16, ROOF_H = 6;
  const TILE_COLS = 120, TILE_ROWS = 22;
  const brick = M.museumBrick, stone = M.museumStone, roofM = M.museumRoof, glass = M.glass;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main body
  add(hb(LEN, H, W, 24), brick, 0, H / 2, 0);
  add(hb(LEN + 0.3, 3.5, W + 0.3, 8), stone, 0, 1.75, 0); // stone base
  add(hb(LEN + 0.4, 0.5, W + 0.4, 4), stone, 0, H - 0.25, 0); // cornice

  // Hipped roof
  { const s = new THREE.Shape(); s.moveTo(-W / 2 - 0.5, 0); s.lineTo(0, ROOF_H); s.lineTo(W / 2 + 0.5, 0); s.closePath();
    const geo = new THREE.ExtrudeGeometry(s, { depth: LEN, bevelEnabled: false });
    const m = new THREE.Mesh(geo, roofM); m.rotation.y = Math.PI / 2; m.position.set(-LEN / 2, H, 0); root.add(m); track(m); }

  // Instanced roof tiles
  { const sa = Math.atan2(ROOF_H, W / 2 + 0.5); const cnt = TILE_COLS * TILE_ROWS * 2;
    const tg = new THREE.BoxGeometry(LEN / TILE_COLS, 0.05, (W / 2) / Math.cos(sa) / TILE_ROWS, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, M.roofBrown, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (const side of [-1, 1]) { for (let c = 0; c < TILE_COLS; c++) { for (let r = 0; r < TILE_ROWS; r++) {
      const frac = (r + 0.5) / TILE_ROWS;
      tmp.position.set(-LEN / 2 + (c + 0.5) * (LEN / TILE_COLS), H + ROOF_H * (1 - frac) + 0.03, side * (W / 2 + 0.5) * frac);
      tmp.rotation.set(side * sa, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }}} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Windows (3 rows on long facades)
  for (let row = 0; row < 3; row++) { const wy = 4 + row * 4;
    for (let b = 0; b < 10; b++) { const wx = -LEN / 2 + 3 + b * ((LEN - 6) / 9);
      for (const sz of [-1, 1]) { add(hb(1.6, 2.4, 0.2, 4), glass, wx, wy, sz * (W / 2 + 0.12));
        add(hb(1.9, 0.12, 0.28, 2), stone, wx, wy + 1.2, sz * (W / 2 + 0.12)); }}}

  // Central entrance portico
  add(hb(10, 8, 3, 8), stone, 0, 4, W / 2 + 1.5);
  for (let c = 0; c < 4; c++) { add(new THREE.CylinderGeometry(0.4, 0.45, 6, 20, 4), stone, -3 + c * 2, 3, W / 2 + 3); }
  add(hb(10, 1.5, 4, 4), stone, 0, 7.5, W / 2 + 1.5);

  root.userData.customFootprint = [[-LEN/2, -W/2], [LEN/2, -W/2], [LEN/2, W/2], [-LEN/2, W/2]];
  root.userData.baseHeight = H + ROOF_H;
  console.log(`[goteborg_sjofartsmuseet] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Göteborg — Stora Teatern ────────────────────────────────────────────────
// 1859 Renaissance Revival theater on Kungsparken. ~45m x 37m.
// Ornate facade with arched windows, pilasters, and balustrade.

function buildGoteborgStoraTeatern(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const LEN = 45, W = 37, H = 20, FLY_H = 10;
  const TILE_COLS = 110, TILE_ROWS = 20;
  const stucco = M.baroqueStucco, trim = M.castleTrim, gold = M.theaterGold;
  const roofM = M.museumRoof, glass = M.glass, red = M.theaterRed;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };
  const hb = (w, h, d, s = 8) => new THREE.BoxGeometry(w, h, d, s, s, s);

  // Main body
  add(hb(LEN, H, W, 24), stucco, 0, H / 2, 0);
  add(hb(LEN + 0.4, 0.6, W + 0.4, 4), trim, 0, H - 0.3, 0);
  add(hb(LEN + 0.3, 0.35, W + 0.3, 4), trim, 0, 7, 0);

  // Fly tower (raised stage house at the back)
  add(hb(LEN * 0.4, FLY_H, W * 0.5, 12), stucco, 0, H + FLY_H / 2, -W * 0.15);
  add(hb(LEN * 0.4 + 0.3, 0.4, W * 0.5 + 0.3, 4), trim, 0, H + FLY_H - 0.2, -W * 0.15);

  // Flat roof with tile grid
  { const cnt = TILE_COLS * TILE_ROWS;
    const tg = new THREE.BoxGeometry(LEN / TILE_COLS, 0.06, W / TILE_ROWS, 2, 1, 2);
    const inst = new THREE.InstancedMesh(tg, roofM, cnt); const tmp = new THREE.Object3D(); let i = 0;
    for (let c = 0; c < TILE_COLS; c++) { for (let r = 0; r < TILE_ROWS; r++) {
      tmp.position.set(-LEN / 2 + (c + 0.5) * (LEN / TILE_COLS), H + 0.04, -W / 2 + (r + 0.5) * (W / TILE_ROWS));
      tmp.rotation.set(0, 0, 0); tmp.updateMatrix(); inst.setMatrixAt(i++, tmp.matrix);
    }} inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  // Facade pilasters (front — facing +Z)
  for (let p = 0; p < 7; p++) { const px = -LEN / 2 + 3 + p * ((LEN - 6) / 6);
    add(hb(0.7, H, 0.7, 8), trim, px, H / 2, W / 2 + 0.1); }

  // Windows (arched on ground, rectangular above)
  for (let b = 0; b < 6; b++) { const wx = -LEN / 2 + 6.5 + b * ((LEN - 6) / 6);
    // Ground floor arched
    add(hb(2.2, 3.5, 0.2, 4), glass, wx, 4.5, W / 2 + 0.15);
    add(new THREE.CylinderGeometry(1.1, 1.1, 0.2, 16, 1, false, 0, Math.PI), glass, wx, 6.25, W / 2 + 0.15, 0, 0, Math.PI / 2);
    // Upper floor
    add(hb(2.0, 3.0, 0.2, 4), glass, wx, 11, W / 2 + 0.15);
    add(hb(2.0, 3.0, 0.2, 4), glass, wx, 16.5, W / 2 + 0.15);
  }

  // Balustrade above cornice
  { const balGeo = new THREE.BoxGeometry(0.15, 1.5, 0.15, 2, 3, 2);
    const cnt = 60; const inst = new THREE.InstancedMesh(balGeo, trim, cnt);
    const tmp = new THREE.Object3D();
    for (let i = 0; i < cnt; i++) { tmp.position.set(-LEN / 2 + 1 + i * ((LEN - 2) / cnt), H + 0.75, W / 2 + 0.3);
      tmp.updateMatrix(); inst.setMatrixAt(i, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }
  add(hb(LEN, 0.15, 0.6, 4), trim, 0, H + 1.55, W / 2 + 0.3);

  // Entrance canopy
  add(hb(12, 1, 5, 6), trim, 0, 4, W / 2 + 3);
  // Marquee
  add(hb(10, 1.5, 0.3, 4), red, 0, 5.5, W / 2 + 5.3);

  // Gold accents at roofline
  for (let i = 0; i < 3; i++) add(new THREE.SphereGeometry(0.4, 12, 8), gold, -8 + i * 8, H + 2, W / 2 + 0.3);

  root.userData.customFootprint = [[-LEN/2, -W/2], [LEN/2, -W/2], [LEN/2, W/2 + 5], [-LEN/2, W/2 + 5]];
  root.userData.baseHeight = H + FLY_H;
  console.log(`[goteborg_stora_teatern] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Göteborg — Scandinavium ─────────────────────────────────────────────────
// 1971 indoor arena, oval shape ~130m × 100m. Domed roof.
// Capacity ~14,000. Built for Ice Hockey World Championship.

function buildGoteborgScandinavium(root, cell, ctx) {
  const { THREE, M } = ctx;
  if (cell.polygon && cell.polygon.length >= 3) {
    let cx = 0, cz = 0;
    for (const [x, z] of cell.polygon) { cx += x; cz += z; }
    cx /= cell.polygon.length; cz /= cell.polygon.length;
    let sxx = 0, szz = 0, sxz = 0;
    for (const [x, z] of cell.polygon) { const dx = x - cx, dz = z - cz; sxx += dx * dx; szz += dz * dz; sxz += dx * dz; }
    root.rotation.y = -0.5 * Math.atan2(2 * sxz, sxx - szz);
  }

  const RX = 65, RZ = 50, BASE_H = 4, WALL_H = 20, DOME_H = 12;
  const metal = M.arenaMetal, roofM = M.arenaRoof, glass = M.arenaGlass;
  const concrete = M.concrete, steel = M.steel;

  let fc = 0;
  const track = (m, n = 1) => { const g = m.geometry; fc += (g.index ? g.index.count / 3 : g.attributes.position.count / 3) * n; return m; };
  const add = (geo, mat, x, y, z, rx, ry, rz) => { const m = new THREE.Mesh(geo, mat); m.position.set(x, y, z); if (rx) m.rotation.x = rx; if (ry) m.rotation.y = ry; if (rz) m.rotation.z = rz; root.add(m); track(m); return m; };

  // Concrete base
  add(new THREE.CylinderGeometry(RX + 2, RX + 3, BASE_H, 48, 4), concrete, 0, BASE_H / 2, 0);

  // Oval wall (approximate with scaled cylinder)
  { const wallGeo = new THREE.CylinderGeometry(1, 1, WALL_H, 64, 8, true);
    const m = new THREE.Mesh(wallGeo, metal); m.scale.set(RX, 1, RZ);
    m.position.set(0, BASE_H + WALL_H / 2, 0); root.add(m); track(m); }

  // Dome roof (flattened sphere)
  { const domeGeo = new THREE.SphereGeometry(1, 64, 32, 0, Math.PI * 2, 0, Math.PI / 3);
    const m = new THREE.Mesh(domeGeo, roofM); m.scale.set(RX, DOME_H / 0.5, RZ);
    m.position.set(0, BASE_H + WALL_H, 0); root.add(m); track(m); }

  // Horizontal seam rings on dome
  for (let r = 1; r < 8; r++) { const frac = r / 8;
    const ringRX = RX * Math.cos(frac * Math.PI / 3);
    const ringRZ = RZ * Math.cos(frac * Math.PI / 3);
    const ringY = BASE_H + WALL_H + DOME_H * Math.sin(frac * Math.PI / 3);
    // Approximate oval torus with scaled torus
    const torus = new THREE.Mesh(new THREE.TorusGeometry(1, 0.12, 6, 96), steel);
    torus.scale.set(ringRX, 1, ringRZ); torus.position.set(0, ringY, 0);
    torus.rotation.x = Math.PI / 2; root.add(torus); track(torus); }

  // Glass entrance bands (4 entrances around the perimeter)
  for (let i = 0; i < 4; i++) { const a = (i / 4) * Math.PI * 2;
    add(new THREE.BoxGeometry(12, 6, 1, 6, 6, 2), glass,
        Math.cos(a) * (RX + 1), BASE_H + 3, Math.sin(a) * (RZ + 1), 0, -a, 0); }

  // Instanced facade panels (vertical strips around wall)
  { const panelGeo = new THREE.BoxGeometry(2, WALL_H, 0.3, 2, 6, 2);
    const cnt = 64; const inst = new THREE.InstancedMesh(panelGeo, metal, cnt);
    const tmp = new THREE.Object3D();
    for (let i = 0; i < cnt; i++) { const a = (i / cnt) * Math.PI * 2;
      tmp.position.set(Math.cos(a) * (RX + 0.2), BASE_H + WALL_H / 2, Math.sin(a) * (RZ + 0.2));
      tmp.rotation.set(0, -a, 0); tmp.updateMatrix(); inst.setMatrixAt(i, tmp.matrix); }
    inst.instanceMatrix.needsUpdate = true; root.add(inst); track(inst, cnt); }

  root.userData.customFootprint = [[-RX, -RZ], [RX, -RZ], [RX, RZ], [-RX, RZ]];
  root.userData.baseHeight = BASE_H + WALL_H + DOME_H;
  console.log(`[goteborg_scandinavium] built ~${Math.round(fc).toLocaleString()} faces`);
}

// ── Linköping landmarks ─────────────────────────────────────────────────────
function buildLinkopingDomkyrka(r,c,x)    { _genericChurch(r,c,x,'linkoping_domkyrka',{naveLen:68,naveW:24,naveH:24,roofH:12,towerW:13,towerH:50,spireH:55}); }
function buildLinkopingSlott(r,c,x)       { _genericCastle(r,c,x,'linkoping_slott',{fortLen:50,fortW:40,wallH:12,keepLen:28,keepW:18,keepH:16}); }
function buildLinkopingJarnvagsmuseum(r,c,x){ _genericMuseum(r,c,x,'linkoping_jarnvagsmuseum',{len:35,w:20,h:10,roofH:4}); }
function buildLinkopingKonsertKongress(r,c,x){ _genericTheater(r,c,x,'linkoping_konsert_kongress',{len:80,w:55,h:20,flyH:10}); }
function buildLinkopingSaabArena(r,c,x)   { _genericArena(r,c,x,'linkoping_saab_arena',{rx:60,rz:48,wallH:18,domeH:10}); }

// ── Örebro landmarks ────────────────────────────────────────────────────────
function buildOrebroNikolai(r,c,x)        { _genericChurch(r,c,x,'orebro_nikolai',{naveLen:50,naveW:18,naveH:18,roofH:9,towerW:10,towerH:38,spireH:30}); }
function buildOrebroSlott(r,c,x)          { _genericCastle(r,c,x,'orebro_slott',{fortLen:55,fortW:45,wallH:14,keepLen:30,keepW:22,keepH:18}); }
function buildOrebroTekniskaKvarnen(r,c,x){ _genericMuseum(r,c,x,'orebro_tekniska_kvarnen',{len:35,w:18,h:12,roofH:5}); }
function buildOrebroConventum(r,c,x)      { _genericTheater(r,c,x,'orebro_conventum',{len:60,w:40,h:18,flyH:8}); }
function buildOrebroBehrnArena(r,c,x)     { _genericStadium(r,c,x,'orebro_behrn_arena',{fieldLen:105,fieldW:68,standH:12,standD:20,roofH:16}); }

// ── Norrköping landmarks ────────────────────────────────────────────────────
function buildNorrkopingSanktOlai(r,c,x)  { _genericChurch(r,c,x,'norrkoping_sankt_olai',{naveLen:48,naveW:18,naveH:18,roofH:9,towerW:10,towerH:35,spireH:28}); }
function buildNorrkopingJohannisborg(r,c,x){
  // Castle ruin — simplified: partial walls, no roof
  const L = _landmarkInit(r,c,x);
  const { M, add, hb, foot, done } = L;
  const stone = M.ruinStone || M.castleStone, moss = M.ruinMoss || M.park;
  add(hb(30,10,20,14), stone, 0, 5, 0);
  add(hb(28,2,18,6), moss, 0, 10, 0);
  // Broken corner towers
  for (const [sx,sz] of [[-1,-1],[-1,1],[1,-1],[1,1]])
    add(new L.THREE.CylinderGeometry(3,3.3,12,16,6), stone, sx*15, 6, sz*10);
  // Window openings
  for (let i=0;i<4;i++) add(hb(1.5,2.5,0.3,3), M.glass, -10+i*7, 6, 10.2);
  foot(15,10); done('norrkoping_johannisborg', 12);
}
function buildNorrkopingKonstmuseum(r,c,x){ _genericMuseum(r,c,x,'norrkoping_konstmuseum',{len:45,w:28,h:14,roofH:6}); }
function buildNorrkopingLouisDeGeer(r,c,x){ _genericTheater(r,c,x,'norrkoping_louis_de_geer',{len:65,w:40,h:18,flyH:8}); }
function buildNorrkopingOstgotaporten(r,c,x){ _genericArena(r,c,x,'norrkoping_ostgotaporten',{rx:65,rz:50,wallH:18,domeH:10}); }

// ── Helsingborg landmarks ───────────────────────────────────────────────────
function buildHelsingborgSanktaMaria(r,c,x){ _genericChurch(r,c,x,'helsingborg_sankta_maria',{naveLen:50,naveW:18,naveH:18,roofH:9,towerW:10,towerH:36,spireH:30}); }
function buildHelsingborgKarnan(r,c,x) {
  // Kärnan — iconic medieval tower, ~35m tall, square brick keep
  const L = _landmarkInit(r,c,x);
  const { THREE, M, add, hb, foot, done } = L;
  const brick = M.churchRed, trim = M.castleTrim, roof = M.castleRoof;
  const TW = 15, TH = 35;
  add(hb(TW, TH, TW, 24), brick, 0, TH/2, 0);
  add(hb(TW+0.6, 0.6, TW+0.6, 4), trim, 0, TH-0.3, 0);
  // Crenellated parapet
  for (let i=0;i<20;i++) { const a=(i/20)*Math.PI*2;
    add(hb(1.2,1.5,1.5,3), brick, Math.cos(a)*(TW/2), TH+0.75, Math.sin(a)*(TW/2)); }
  // Corner turrets
  for (const [sx,sz] of [[-1,-1],[-1,1],[1,-1],[1,1]]) {
    add(new THREE.CylinderGeometry(1.5,1.7,TH+4,12,8), brick, sx*(TW/2+0.5), (TH+4)/2, sz*(TW/2+0.5));
    add(new THREE.ConeGeometry(2, 4, 12, 3), roof, sx*(TW/2+0.5), TH+4, sz*(TW/2+0.5)); }
  // Stepped cap roof
  add(hb(TW+1, 2, TW+1, 6), roof, 0, TH+1, 0);
  // Windows (narrow slits)
  for (let row=0;row<4;row++) { const wy=5+row*8;
    for (const sz of [-1,1]) add(hb(0.8,2.5,0.3,3), M.glass, 0, wy, sz*(TW/2+0.15)); }
  foot(TW/2+2, TW/2+2); done('helsingborg_karnan', TH+6);
}
function buildHelsingborgDunkers(r,c,x)   { _genericMuseum(r,c,x,'helsingborg_dunkers',{len:80,w:45,h:16,roofH:4,brick:x.M.white}); }
function buildHelsingborgStadsteater(r,c,x){ _genericTheater(r,c,x,'helsingborg_stadsteater',{len:60,w:42,h:18,flyH:8}); }
function buildHelsingborgArena(r,c,x)     { _genericArena(r,c,x,'helsingborg_arena',{rx:50,rz:40,wallH:16,domeH:8}); }

// ── Jönköping landmarks ─────────────────────────────────────────────────────
function buildJonkopingSofiakyrkan(r,c,x) { _genericChurch(r,c,x,'jonkoping_sofiakyrkan',{naveLen:52,naveW:20,naveH:20,roofH:10,towerW:11,towerH:42,spireH:38}); }
function buildJonkopingSlottsruin(r,c,x) {
  // Castle ruin — partial stone walls, no roof
  const L = _landmarkInit(r,c,x);
  const { M, add, hb, foot, done } = L;
  const stone = M.ruinStone || M.castleStone, moss = M.ruinMoss || M.park;
  add(hb(25,8,18,12), stone, 0, 4, 0);
  add(hb(23,1.5,16,4), moss, 0, 8.5, 0);
  for (const [sx,sz] of [[-1,1],[1,1]]) add(hb(3,10,3,6), stone, sx*11, 5, sz*8);
  foot(13,9); done('jonkoping_slottsruin', 10);
}
function buildJonkopingTandsticksmuseet(r,c,x){ _genericMuseum(r,c,x,'jonkoping_tandsticksmuseet',{len:30,w:18,h:10,roofH:4}); }
function buildJonkopingSpira(r,c,x) {
  // Kulturhuset Spira — modern glass-and-wood concert hall on the waterfront
  const L = _landmarkInit(r,c,x);
  const { THREE, M, add, hb, foot, done } = L;
  const glass = M.arenaGlass, metal = M.arenaMetal, wood = M.bridgeWood || M.roofBrown;
  add(hb(75, 22, 35, 20), glass, 0, 11, 0);
  add(hb(75+0.3, 1, 35+0.3, 6), metal, 0, 22.5, 0);
  // Wood-clad auditorium volume
  add(hb(30, 24, 30, 14), wood, -10, 12, 0);
  // Horizontal metal bands
  for (let i=0;i<6;i++) add(hb(75+0.1, 0.15, 35+0.1, 4), M.steel, 0, 3+i*3.5, 0);
  // Entrance canopy
  add(hb(20, 0.6, 8, 4), metal, 15, 5, 35/2+4);
  foot(75/2, 35/2+4); done('jonkoping_spira', 24);
}
function buildJonkopingEnergiArena(r,c,x) { _genericStadium(r,c,x,'jonkoping_energi_arena',{fieldLen:100,fieldW:65,standH:10,standD:18,roofH:14}); }

// ── Västerås landmarks ──────────────────────────────────────────────────────
function buildVasterasDomkyrka(r,c,x)     { _genericChurch(r,c,x,'vasteras_domkyrka',{naveLen:72,naveW:22,naveH:24,roofH:12,towerW:12,towerH:48,spireH:44}); }
function buildVasterasSlott(r,c,x)        { _genericCastle(r,c,x,'vasteras_slott',{fortLen:55,fortW:40,wallH:12,keepLen:30,keepW:20,keepH:16}); }
function buildVasterasVallby(r,c,x)       { _genericMuseum(r,c,x,'vasteras_vallby',{len:40,w:22,h:10,roofH:5}); }
function buildVasterasKonserthus(r,c,x)   { _genericTheater(r,c,x,'vasteras_konserthus',{len:80,w:55,h:20,flyH:10}); }
function buildVasterasHitachiArena(r,c,x) { _genericArena(r,c,x,'vasteras_hitachi_arena',{rx:65,rz:55,wallH:20,domeH:12}); }

// ── Borås landmarks ─────────────────────────────────────────────────────────
function buildBorasCaroli(r,c,x)          { _genericChurch(r,c,x,'boras_caroli',{naveLen:45,naveW:18,naveH:18,roofH:9,towerW:10,towerH:35,spireH:28}); }
function buildBorasMuseumCastle(r,c,x)    { _genericCastle(r,c,x,'boras_museum_castle',{fortLen:35,fortW:28,wallH:10,keepLen:20,keepW:14,keepH:12}); }
function buildBorasTextilmuseet(r,c,x)    { _genericMuseum(r,c,x,'boras_textilmuseet',{len:45,w:25,h:12,roofH:5}); }
function buildBorasStadsteater(r,c,x)     { _genericTheater(r,c,x,'boras_stadsteater',{len:50,w:35,h:16,flyH:7}); }
function buildBorasIshall(r,c,x)          { _genericArena(r,c,x,'boras_ishall',{rx:45,rz:35,wallH:14,domeH:8}); }

// ── Eskilstuna landmarks ────────────────────────────────────────────────────
function buildEskilstunaEskilskyrkan(r,c,x){ _genericChurch(r,c,x,'eskilstuna_eskilskyrkan',{naveLen:35,naveW:15,naveH:15,roofH:7,towerW:8,towerH:30,spireH:22}); }
function buildEskilstunaSlott(r,c,x)     { _genericCastle(r,c,x,'eskilstuna_slott',{fortLen:50,fortW:35,wallH:12,keepLen:25,keepW:18,keepH:15}); }
function buildEskilstunaBergstromska(r,c,x){ _genericMuseum(r,c,x,'eskilstuna_bergstromska',{len:20,w:14,h:10,roofH:4}); }
function buildEskilstunaTeater(r,c,x)    { _genericTheater(r,c,x,'eskilstuna_teater',{len:45,w:30,h:15,flyH:6}); }
function buildEskilstunaTunavallen(r,c,x) { _genericStadium(r,c,x,'eskilstuna_tunavallen',{fieldLen:105,fieldW:68,standH:10,standD:18,roofH:14}); }

// ── Gävle landmarks ─────────────────────────────────────────────────────────
function buildGavleTrefaldighet(r,c,x)    { _genericChurch(r,c,x,'gavle_trefaldighet',{naveLen:52,naveW:20,naveH:20,roofH:10,towerW:10,towerH:38,spireH:32}); }
function buildGavleSlott(r,c,x)           { _genericCastle(r,c,x,'gavle_slott',{fortLen:40,fortW:30,wallH:12,keepLen:22,keepW:16,keepH:14}); }
function buildGavleLansmuseum(r,c,x)      { _genericMuseum(r,c,x,'gavle_lansmuseum',{len:42,w:24,h:14,roofH:6}); }
function buildGavleTeater(r,c,x)          { _genericTheater(r,c,x,'gavle_teater',{len:45,w:30,h:16,flyH:7}); }
function buildGavleGevlevallen(r,c,x)     { _genericStadium(r,c,x,'gavle_gevlevallen',{fieldLen:105,fieldW:68,standH:10,standD:18,roofH:14}); }

// ── Halmstad landmarks ──────────────────────────────────────────────────────
function buildHalmstadNikolai(r,c,x)      { _genericChurch(r,c,x,'halmstad_nikolai',{naveLen:50,naveW:18,naveH:18,roofH:9,towerW:10,towerH:38,spireH:30}); }
function buildHalmstadSlott(r,c,x)        { _genericCastle(r,c,x,'halmstad_slott',{fortLen:50,fortW:38,wallH:12,keepLen:28,keepW:18,keepH:16}); }
function buildHalmstadKonstmuseum(r,c,x)  { _genericMuseum(r,c,x,'halmstad_konstmuseum',{len:48,w:28,h:14,roofH:6}); }
function buildHalmstadKulturhuset(r,c,x)  { _genericTheater(r,c,x,'halmstad_kulturhuset',{len:55,w:35,h:16,flyH:7}); }
function buildHalmstadOrjansVall(r,c,x)   { _genericStadium(r,c,x,'halmstad_orjans_vall',{fieldLen:105,fieldW:68,standH:10,standD:18,roofH:14}); }

// ── Karlskrona landmarks ────────────────────────────────────────────────────
function buildKarlskronaFredrikskyrkan(r,c,x){ _genericChurch(r,c,x,'karlskrona_fredrikskyrkan',{naveLen:48,naveW:20,naveH:20,roofH:10,towerW:10,towerH:40,spireH:35,brick:x.M.baroqueStucco}); }
function buildKarlskronaKastell(r,c,x)    { _genericCastle(r,c,x,'karlskrona_kastell',{fortLen:35,fortW:30,wallH:10,keepLen:18,keepW:14,keepH:12}); }
function buildKarlskronaMarinmuseum(r,c,x){ _genericMuseum(r,c,x,'karlskrona_marinmuseum',{len:70,w:40,h:14,roofH:5}); }
function buildKarlskronaSparre(r,c,x)     { _genericTheater(r,c,x,'karlskrona_sparre',{len:45,w:30,h:14,flyH:6}); }
function buildKarlskronaNktArena(r,c,x)   { _genericArena(r,c,x,'karlskrona_nkt_arena',{rx:70,rz:55,wallH:18,domeH:10}); }

// ── Karlstad landmarks ──────────────────────────────────────────────────────
function buildKarlstadDomkyrka(r,c,x)     { _genericChurch(r,c,x,'karlstad_domkyrka',{naveLen:55,naveW:20,naveH:20,roofH:10,towerW:11,towerH:42,spireH:36}); }
function buildKarlstadResidens(r,c,x)     { _genericCastle(r,c,x,'karlstad_residens',{fortLen:45,fortW:30,wallH:12,keepLen:25,keepW:18,keepH:14}); }
function buildKarlstadSandgrund(r,c,x)    { _genericMuseum(r,c,x,'karlstad_sandgrund',{len:55,w:30,h:12,roofH:5}); }
function buildKarlstadWermlandOpera(r,c,x){ _genericTheater(r,c,x,'karlstad_wermland_opera',{len:50,w:35,h:18,flyH:8}); }
function buildKarlstadLofbergsArena(r,c,x){ _genericArena(r,c,x,'karlstad_lofbergs_arena',{rx:55,rz:45,wallH:18,domeH:10}); }

// ── Luleå landmarks ─────────────────────────────────────────────────────────
function buildLuleaOrnasket(r,c,x)        { _genericChurch(r,c,x,'lulea_ornasket',{naveLen:38,naveW:16,naveH:14,roofH:7,towerW:8,towerH:28,spireH:20}); }
function buildLuleaFortress(r,c,x)        { _genericCastle(r,c,x,'lulea_fortress',{fortLen:35,fortW:25,wallH:10,keepLen:18,keepW:12,keepH:12}); }
function buildLuleaNorrbottensMuseum(r,c,x){ _genericMuseum(r,c,x,'lulea_norrbottens_museum',{len:35,w:20,h:12,roofH:5}); }
function buildLuleaNorrbottensteatern(r,c,x){ _genericTheater(r,c,x,'lulea_norrbottensteatern',{len:65,w:40,h:18,flyH:8}); }
function buildLuleaCoopArena(r,c,x)       { _genericArena(r,c,x,'lulea_coop_arena',{rx:55,rz:45,wallH:18,domeH:10}); }

// ── Lund landmarks ──────────────────────────────────────────────────────────
function buildLundDomkyrka(r,c,x) {
  // Lund Cathedral — Romanesque, twin towers, ~60m long, towers ~55m
  _genericChurch(r,c,x,'lund_domkyrka',{naveLen:60,naveW:22,naveH:22,roofH:11,towerW:12,towerH:45,spireH:10,brick:x.M.castleStone});
}
function buildLundOrtofta(r,c,x)          { _genericCastle(r,c,x,'lund_ortofta',{fortLen:40,fortW:30,wallH:12,keepLen:22,keepW:16,keepH:14}); }
function buildLundHistoriska(r,c,x)       { _genericMuseum(r,c,x,'lund_historiska',{len:35,w:18,h:12,roofH:5}); }
function buildLundStadsteater(r,c,x)      { _genericTheater(r,c,x,'lund_stadsteater',{len:55,w:35,h:16,flyH:7}); }
function buildLundIp(r,c,x)              { _genericStadium(r,c,x,'lund_ip',{fieldLen:105,fieldW:68,standH:10,standD:18,roofH:14}); }

// ── Sundsvall landmarks ─────────────────────────────────────────────────────
function buildSundsvallBrokyrkan(r,c,x)   { _genericChurch(r,c,x,'sundsvall_brokyrkan',{naveLen:38,naveW:16,naveH:14,roofH:7,towerW:8,towerH:30,spireH:22}); }
function buildSundsvallFortress(r,c,x)    { _genericCastle(r,c,x,'sundsvall_fortress',{fortLen:35,fortW:25,wallH:10,keepLen:18,keepW:12,keepH:12}); }
function buildSundsvallMuseum(r,c,x)      { _genericMuseum(r,c,x,'sundsvall_museum',{len:25,w:14,h:10,roofH:4}); }
function buildSundsvallTeater(r,c,x)      { _genericTheater(r,c,x,'sundsvall_teater',{len:45,w:30,h:16,flyH:7}); }
function buildSundsvallNorrporten(r,c,x)  { _genericArena(r,c,x,'sundsvall_norrporten',{rx:55,rz:45,wallH:16,domeH:8}); }

// ── Umeå landmarks ──────────────────────────────────────────────────────────
function buildUmeaStadskyrka(r,c,x)       { _genericChurch(r,c,x,'umea_stadskyrka',{naveLen:42,naveW:18,naveH:16,roofH:8,towerW:9,towerH:32,spireH:24}); }
function buildUmeaFortress(r,c,x)         { _genericCastle(r,c,x,'umea_fortress',{fortLen:35,fortW:25,wallH:10,keepLen:18,keepW:12,keepH:12}); }
function buildUmeaBildmuseet(r,c,x)       { _genericMuseum(r,c,x,'umea_bildmuseet',{len:32,w:18,h:14,roofH:4,brick:x.M.white}); }
function buildUmeaFolketsHus(r,c,x)       { _genericTheater(r,c,x,'umea_folkets_hus',{len:65,w:40,h:18,flyH:8}); }
function buildUmeaVisioniteArena(r,c,x)   { _genericArena(r,c,x,'umea_visionite_arena',{rx:55,rz:45,wallH:18,domeH:10}); }

// ── Visby landmarks ─────────────────────────────────────────────────────────
function buildVisbyDomkyrka(r,c,x)        { _genericChurch(r,c,x,'visby_domkyrka',{naveLen:55,naveW:20,naveH:20,roofH:10,towerW:12,towerH:42,spireH:8,brick:x.M.castleStone}); }
function buildVisbyVisborg(r,c,x)         { _genericCastle(r,c,x,'visby_visborg',{fortLen:50,fortW:40,wallH:12,keepLen:28,keepW:18,keepH:16}); }
function buildVisbyKonstmuseum(r,c,x)     { _genericMuseum(r,c,x,'visby_konstmuseum',{len:30,w:18,h:10,roofH:4}); }
function buildVisbyAlmedalen(r,c,x)       { _genericTheater(r,c,x,'visby_almedalen',{len:30,w:20,h:10,flyH:4}); }
function buildVisbyGutavallen(r,c,x)      { _genericStadium(r,c,x,'visby_gutavallen',{fieldLen:100,fieldW:65,standH:8,standD:15,roofH:12}); }

// ── Natural features ─────────────────────────────────────────────────────────
//
// Lakes (natural=water) and forestry (landuse=forest / natural=wood) are not
// "buildings", but the engine treats them as selectable POI cells so the
// frontend can render them like any other custom landmark. Capacity comes
// from polygon area via osm._DENSITY_M2[WATER/FORESTRY]. Tree species for
// forestry comes from OSM tags (leaf_type/genus/species) and selects which
// of the per-species builders below is dispatched via
// `custom_builder = "forestry_<species>"`, set in osm._load_buildings().
//
// ── Tree density (m² of ground per visible tree, display-capped) ──
// These match _TREE_SPACING_M2 in osm.py so the on-screen visual density
// roughly tracks the capacity number shown in the tooltip. For very large
// polygons we cap the instance count at _FORESTRY_MAX_INSTANCES — the
// metadata still reports the real stem count, only the rendered scatter
// is thinned.
const _TREE_SPACING_M2 = {
  spruce: 4.0,
  pine:   6.0,
  birch:  9.0,
  oak:   25.0,
  mixed:  6.0,
};
const _FORESTRY_MAX_INSTANCES = 400;   // per polygon, per species component

// Deterministic PRNG (Mulberry32) — seeded per-cell so tree positions are
// stable between reloads instead of jittering every page refresh.
function _mulberry32(a) {
  return function () {
    let t = (a += 0x6D2B79F5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Ray-casting point-in-polygon test in the XZ plane (local coords).
function _pointInPoly(x, z, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], zi = ring[i][1];
    const xj = ring[j][0], zj = ring[j][1];
    const intersect = ((zi > z) !== (zj > z))
      && (x < ((xj - xi) * (z - zi)) / (zj - zi) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

// Translate an absolute-metre polygon (as stored in city.json) into the
// root group's local coordinates. The root sits at (cell.world_x, 0,
// cell.world_z), so local = absolute − centroid.
function _polygonToLocal(cell) {
  const wx = cell.world_x, wz = cell.world_z;
  return cell.polygon.map(([px, pz]) => [px - wx, pz - wz]);
}

// Generate ~count points uniformly scattered inside a local polygon using
// seeded jittered-grid sampling (cheap, deterministic, well-distributed).
// Returns [[x, z], ...] in local metres.
function _scatterPointsInPolygon(localPoly, count, seed) {
  let minX =  Infinity, maxX = -Infinity;
  let minZ =  Infinity, maxZ = -Infinity;
  for (const [x, z] of localPoly) {
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (z < minZ) minZ = z;
    if (z > maxZ) maxZ = z;
  }
  const W = Math.max(1, maxX - minX);
  const H = Math.max(1, maxZ - minZ);
  const rng = _mulberry32(seed);

  // Oversample the grid because ~π/4 of bbox cells fall outside an
  // inscribed polygon; we rely on PIP rejection to trim them back.
  const gridN = Math.max(2, Math.ceil(Math.sqrt(count) * 1.25));
  const cellW = W / gridN;
  const cellH = H / gridN;

  const out = [];
  for (let gy = 0; gy < gridN && out.length < count; gy++) {
    for (let gx = 0; gx < gridN && out.length < count; gx++) {
      const jx = rng();
      const jz = rng();
      const x = minX + (gx + jx) * cellW;
      const z = minZ + (gy + jz) * cellH;
      if (_pointInPoly(x, z, localPoly)) out.push([x, z]);
    }
  }
  return out;
}

// Shared low-poly tree-part geometries — built once per species on first
// use and reused across every forestry polygon in the city. Avoids 400 ×
// N polygons worth of BufferGeometry allocation.
let _treePartCache = null;

function _treePartsFor(THREE, species) {
  if (_treePartCache && _treePartCache[species]) return _treePartCache[species];
  if (!_treePartCache) _treePartCache = {};
  const mat = (hex) => new THREE.MeshLambertMaterial({ color: hex });

  // Per-species tree component definitions. Each entry produces one
  // InstancedMesh; multiple entries stack (e.g. spruce = trunk + 2 cones).
  //
  //   geo     — reusable BufferGeometry (NOT scaled per-instance inside it)
  //   mat     — Lambert material (vertex colours would be nicer but 4
  //             instanced draws per species is still cheap)
  //   yOffset — where the geometry's *centre* sits above ground, metres
  //
  // The per-instance Matrix4 then applies: world (x, yOffset, z), a random
  // Y-rotation, and a small scale jitter (~±15%). yOffset is baked in via
  // the matrix composition in _instanceTrees() below.
  //
  // Segment counts were dropped from 6–12 → 5–8 after the Växjö perf pass
  // (~40% tri reduction while keeping each species' silhouette readable).
  let parts;
  switch (species) {
    case 'spruce': {
      // Gran (Picea abies): narrow tapered tree built from a trunk and 2
      // stacked cone tiers. Third tier was dropped as redundant with the
      // first two at typical zoom levels.
      parts = [
        { geo: new THREE.CylinderGeometry(0.28, 0.40, 2.5, 5), mat: mat(0x4a3018), yOffset: 1.25 },
        { geo: new THREE.ConeGeometry(2.6, 6.0, 7),            mat: mat(0x0e3a1e), yOffset: 5.3 },
        { geo: new THREE.ConeGeometry(1.6, 4.5, 7),            mat: mat(0x155c2d), yOffset: 10.0 },
      ];
      break;
    }
    case 'pine': {
      // Tall (Pinus sylvestris): long bare reddish trunk topped by one
      // rounded umbrella canopy. Dropped from 2 spheres to 1 (huge win).
      parts = [
        { geo: new THREE.CylinderGeometry(0.35, 0.55, 11, 6),  mat: mat(0x7b3f1a), yOffset: 5.5 },
        { geo: new THREE.SphereGeometry(3.2, 9, 6),            mat: mat(0x3e6930), yOffset: 12.8 },
      ];
      break;
    }
    case 'birch': {
      // Björk (Betula pendula): slim near-white trunk with a light-green
      // airy crown. Shorter than pine. Trunk is pale cream, not brown.
      parts = [
        { geo: new THREE.CylinderGeometry(0.18, 0.25, 9, 5),   mat: mat(0xd8d3c4), yOffset: 4.5 },
        { geo: new THREE.SphereGeometry(2.0, 8, 6),            mat: mat(0x8bba48), yOffset: 10.5 },
      ];
      break;
    }
    case 'oak': {
      // Ek (Quercus robur): thick short trunk, big broad canopy in a
      // darker forest green.
      parts = [
        { geo: new THREE.CylinderGeometry(0.55, 0.75, 4, 6),   mat: mat(0x5a3818), yOffset: 2.0 },
        { geo: new THREE.SphereGeometry(3.4, 10, 6),           mat: mat(0x2d5a1e), yOffset: 7.0 },
      ];
      break;
    }
    case 'mixed':
    default: {
      // Mixed stand: the caller in buildNaturalDetailLayer splits the
      // scatter 50/30/20 into spruce/pine/birch, so this fallback never
      // fires in practice — but kept as a defensive default.
      parts = [
        { geo: new THREE.CylinderGeometry(0.28, 0.40, 2.5, 5), mat: mat(0x4a3018), yOffset: 1.25 },
        { geo: new THREE.ConeGeometry(2.6, 5.0, 7),            mat: mat(0x0e3a1e), yOffset: 5.0 },
      ];
      break;
    }
  }
  _treePartCache[species] = parts;
  return parts;
}

// Emit one InstancedMesh per tree-part for `positions` trees of `species`,
// deterministically seeded by `seed`. Each instance gets a random Y
// rotation and a small scale jitter so the forest doesn't look stamped.
function _instanceTrees(root, ctx, species, positions, seed) {
  const { THREE } = ctx;
  if (positions.length === 0) return;
  const parts = _treePartsFor(THREE, species);
  const rng = _mulberry32(seed);

  // Pre-roll per-tree rotation + scale so every part sees the same jitter.
  // Without this, trunk and canopy would twist independently and look broken.
  const n = positions.length;
  const rot   = new Float32Array(n);
  const scale = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    rot[i]   = rng() * Math.PI * 2;
    scale[i] = 0.82 + rng() * 0.36;   // 0.82..1.18
  }

  const vPos = new THREE.Vector3();
  const vScl = new THREE.Vector3();
  const vAxis = new THREE.Vector3(0, 1, 0);
  const q    = new THREE.Quaternion();
  const m    = new THREE.Matrix4();

  for (const part of parts) {
    const mesh = new THREE.InstancedMesh(part.geo, part.mat, n);
    mesh.castShadow = true;
    mesh.receiveShadow = true;
    for (let i = 0; i < n; i++) {
      const [x, z] = positions[i];
      const s = scale[i];
      vPos.set(x, part.yOffset * s, z);
      q.setFromAxisAngle(vAxis, rot[i]);
      vScl.set(s, s, s);
      m.compose(vPos, q, vScl);
      mesh.setMatrixAt(i, m);
    }
    mesh.instanceMatrix.needsUpdate = true;
    mesh.userData.__origMat = part.mat;
    root.add(mesh);
  }
}

// ── Scene-wide natural detail layer ──
//
// Builds ONE Group containing a handful of InstancedMesh draws that cover
// every forest polygon in the city. Called from view.js buildCity() after
// the info-mode extruded-polygon mesh is built. Returns the Group (or
// null) so view.js can track it for disposal on city switch.
//
// Draw budget: 4 species × ~2–4 parts per species = ≤16 draws, regardless
// of how many forest polygons the city has. Per-polygon would be ~4000
// draws for a city like Växjö (~560 forests) and melt the browser.
//
// Water surfaces are NOT drawn here — the info-mode merged mesh already
// paints them in blue via the ZONE_COLORS lookup + 0.3 m height cap in
// view.js _makeExtrudeGeo(), which is free since they ride on the single
// draw call that handles every non-landmark building.
export function buildNaturalDetailLayer(layout, scene, THREE) {
  if (!layout || !Array.isArray(layout.buildings)) return null;

  // Collect scattered tree positions in WORLD metres, keyed by species.
  // Each entry is [worldX, worldZ, rotY, scale] so the flush step can
  // compose Matrix4 instances without recomputing anything.
  const buckets = { spruce: [], pine: [], birch: [], oak: [] };
  let forestCells = 0;

  for (const cell of layout.buildings) {
    if (cell.zone !== 20) continue;                 // FORESTRY
    const poly = cell.polygon;
    if (!poly || poly.length < 3) continue;
    forestCells++;

    const species = cell.tree_species || 'mixed';
    const wx = cell.world_x, wz = cell.world_z;

    // Local coords for the scatter + PIP test.
    const localPoly = poly.map(([px, pz]) => [px - wx, pz - wz]);

    const area    = cell.area_m2 || 1000;
    const spacing = _TREE_SPACING_M2[species] || 6.0;
    const real    = Math.max(1, Math.round(area / spacing));
    const visible = Math.min(real, _FORESTRY_MAX_INSTANCES);
    const seed    = (((cell.osm_id ?? cell.idx ?? 0) | 0) >>> 0) || 1;

    const points = _scatterPointsInPolygon(localPoly, visible, seed);
    if (points.length === 0) continue;

    // Per-tree rotation + scale pre-roll so every species part uses the
    // same jitter sequence.
    const rng = _mulberry32(seed);
    const push = (bucket, pts) => {
      for (const [lx, lz] of pts) {
        const rot = rng() * Math.PI * 2;
        const s   = 0.82 + rng() * 0.36;
        bucket.push(lx + wx, lz + wz, rot, s);
      }
    };

    if (species === 'mixed') {
      // Assign each scatter point a species via an independent
      // seeded random draw — 50% spruce, 30% pine, 20% birch. This
      // spatially interleaves the three species across the polygon
      // instead of grouping them by contiguous grid slice (which
      // visibly clumped all pines on one side of the forest, etc).
      const mixedSpruce = [];
      const mixedPine   = [];
      const mixedBirch  = [];
      for (const pt of points) {
        const r = rng();
        if      (r < 0.50) mixedSpruce.push(pt);
        else if (r < 0.80) mixedPine.push(pt);
        else               mixedBirch.push(pt);
      }
      push(buckets.spruce, mixedSpruce);
      push(buckets.pine,   mixedPine);
      push(buckets.birch,  mixedBirch);
    } else if (buckets[species]) {
      push(buckets[species], points);
    } else {
      push(buckets.spruce, points);
    }
  }

  if (forestCells === 0) return null;

  // ── Flush: one InstancedMesh per species-part ─────────────────────────
  const group = new THREE.Group();
  group.name  = 'naturalDetailLayer';
  let totalInstances = 0;
  let totalDraws     = 0;

  for (const [species, flat] of Object.entries(buckets)) {
    const n = flat.length / 4;                      // 4 floats per instance
    if (n === 0) continue;
    const parts = _treePartsFor(THREE, species);

    const vPos  = new THREE.Vector3();
    const vScl  = new THREE.Vector3();
    const vAxis = new THREE.Vector3(0, 1, 0);
    const q     = new THREE.Quaternion();
    const m     = new THREE.Matrix4();

    for (const part of parts) {
      const mesh = new THREE.InstancedMesh(part.geo, part.mat, n);
      mesh.castShadow    = true;
      mesh.receiveShadow = true;
      for (let i = 0; i < n; i++) {
        const base = i * 4;
        const wx   = flat[base];
        const wz   = flat[base + 1];
        const rot  = flat[base + 2];
        const s    = flat[base + 3];
        vPos.set(wx, part.yOffset * s, wz);
        q.setFromAxisAngle(vAxis, rot);
        vScl.set(s, s, s);
        m.compose(vPos, q, vScl);
        mesh.setMatrixAt(i, m);
      }
      mesh.instanceMatrix.needsUpdate = true;
      mesh.userData.__origMat = part.mat;
      group.add(mesh);
      totalDraws++;
    }
    totalInstances += n;
  }

  scene.add(group);
  // eslint-disable-next-line no-console
  console.log(
    `[naturalDetailLayer] ${forestCells} forests → `
    + `${totalInstances.toLocaleString()} trees in ${totalDraws} draw calls`,
  );
  return group;
}

// ── Swedish flag pole (easter egg) ──────────────────────────────────────────
//
// One deterministic handful per city, placed next to random residential
// buildings by osm._pick_flag_pole_positions. Renders a white pole with a
// gold finial and a flag plane painted from a shared CanvasTexture. The
// group lives in its own scene layer (view.js _flagPoleGroup) so the
// SEIR colour / height pipelines never touch it — the flag always
// reads in the correct Swedish colours regardless of the selected
// visualisation mode.

let _swedishFlagTex = null;

function _getSwedishFlagTexture(THREE) {
  if (_swedishFlagTex) return _swedishFlagTex;

  // Swedish flag — blue field with an off-centre yellow Nordic cross.
  // Drawn into a 160 × 100 canvas (close to the official 8 : 5 ratio).
  const W = 160, H = 100;
  const c = document.createElement('canvas');
  c.width = W;  c.height = H;
  const ctx = c.getContext('2d');

  // Base field — Swedish flag blue
  ctx.fillStyle = '#005EB8';
  ctx.fillRect(0, 0, W, H);

  // Yellow cross — Swedish flag yellow (Nordic cross, hoist-offset)
  ctx.fillStyle = '#FECC00';
  // Horizontal arm: full width, middle 2/10 of the height
  ctx.fillRect(0, Math.round(H * 4 / 10), W, Math.round(H * 2 / 10));
  // Vertical arm: offset 4/16 from the hoist, 2/16 wide
  ctx.fillRect(Math.round(W * 4 / 16), 0,
               Math.round(W * 2 / 16), H);

  _swedishFlagTex = new THREE.CanvasTexture(c);
  _swedishFlagTex.minFilter       = THREE.LinearFilter;
  _swedishFlagTex.magFilter       = THREE.LinearFilter;
  _swedishFlagTex.generateMipmaps = false;
  return _swedishFlagTex;
}

// ── LNU (Linnaeus University) flag texture ──
// Two variants of the Linnaeus circle-tree brand logo painted onto a
// flag canvas:
//
//   'yellow' → black tree on LNU brand yellow
//   'white'  → black tree on white
//
// The LNU identity is a stylised linden tree made of ~17 black discs
// clustered above a thick curved trunk. We draw it directly into a
// 2D canvas so there's no external image dependency — cheap, shared
// across every flag pole via CanvasTexture caching.
const _lnuFlagTexCache = {};

function _getLNUFlagTexture(THREE, variant = 'yellow') {
  if (_lnuFlagTexCache[variant]) return _lnuFlagTexCache[variant];

  // Flag aspect 8:5 → 400×250 so the logo has enough resolution.
  const W = 400, H = 250;
  const c = document.createElement('canvas');
  c.width = W;  c.height = H;
  const ctx = c.getContext('2d');

  // Background field
  if (variant === 'yellow') {
    ctx.fillStyle = '#fcd116';     // LNU brand yellow
  } else {
    ctx.fillStyle = '#ffffff';     // white variant
  }
  ctx.fillRect(0, 0, W, H);

  // Draw the circle-tree logo centred in a square region
  drawLNUTree(ctx, W / 2, H / 2, Math.min(W, H) * 0.82);

  const tex = new THREE.CanvasTexture(c);
  tex.minFilter       = THREE.LinearFilter;
  tex.magFilter       = THREE.LinearFilter;
  tex.generateMipmaps = false;
  _lnuFlagTexCache[variant] = tex;
  return tex;
}

/**
 * Paint the Linnaeus University circle-tree silhouette in black into
 * the given 2D context. The logo is drawn inside a bounding square
 * of side `size`, centred at (cxPx, cyPx). Trunk curves gently to
 * the right out of a wide base, and ~17 foliage discs of varying
 * radii are arranged above it to approximate the identity silhouette
 * from the Linnaeus University brand images.
 *
 * Exported so view.js can reuse the same drawing path for the LNU
 * POI pin icon without duplicating circle coordinates.
 */
export function drawLNUTree(ctx, cxPx, cyPx, size) {
  const S = size;
  const x0 = cxPx - S / 2;
  const y0 = cyPx - S / 2;
  const px = (u) => x0 + u * S;
  const py = (v) => y0 + v * S;

  ctx.fillStyle = '#000000';

  // Trunk: wide at the base (y=1.00), curving upward and slightly to
  // the right as it tapers into the foliage (y≈0.50). Built as a
  // closed path with two quadratic bezier edges for the outer sides
  // plus straight caps at top and bottom.
  const trunk = new Path2D();
  // Base (left corner)
  trunk.moveTo(px(0.30), py(1.00));
  // Left edge sweeping up and right into the foliage
  trunk.quadraticCurveTo(px(0.30), py(0.78), px(0.40), py(0.50));
  // Top cap (narrow — hides behind the central foliage cluster)
  trunk.lineTo(px(0.50), py(0.50));
  // Right edge sweeping back down and out to the base
  trunk.quadraticCurveTo(px(0.48), py(0.78), px(0.55), py(1.00));
  trunk.closePath();
  ctx.fill(trunk);

  // Foliage — ~17 discs of varying radius laid out to match the
  // Linnaeus identity silhouette. Coordinates are in the logo's
  // unit square (0..1); radius is a fraction of that square's side.
  const CIRCLES = [
    // Top row
    [0.28, 0.10, 0.075],   // small top-left
    [0.47, 0.04, 0.088],   // top centre
    [0.66, 0.07, 0.110],   // top centre-right (large)
    [0.87, 0.19, 0.090],   // upper-right

    // Upper-left cluster
    [0.10, 0.22, 0.092],   // upper-left (large)
    [0.32, 0.22, 0.085],   // upper-mid-left

    // Mid row
    [0.02, 0.40, 0.052],   // far-left small
    [0.19, 0.40, 0.095],   // mid-left (large)
    [0.40, 0.30, 0.085],   // mid-upper centre
    [0.55, 0.22, 0.092],   // mid-upper right-centre
    [0.73, 0.28, 0.100],   // mid-right (large)
    [0.96, 0.34, 0.048],   // far-right small

    // Central cluster
    [0.32, 0.42, 0.088],   // centre-left
    [0.51, 0.42, 0.100],   // dead centre (large)
    [0.70, 0.46, 0.082],   // centre-right

    // Lower foliage row
    [0.14, 0.55, 0.070],   // lower-left
    [0.38, 0.55, 0.078],   // lower-mid-left
    [0.57, 0.55, 0.072],   // lower-mid-right
  ];
  for (const [cxu, cyu, r] of CIRCLES) {
    ctx.beginPath();
    ctx.arc(px(cxu), py(cyu), r * S, 0, Math.PI * 2);
    ctx.fill();
  }
}

/**
 * Build one LNU flag pole. Same geometry as the Swedish flag pole,
 * but the flag texture is the Linnaeus University circle-tree logo.
 * The caller passes a `variant` ('yellow' or 'white') so the set of
 * poles on each campus visibly alternates between the two official
 * LNU identities.
 */
export function buildLNUFlagPole(THREE, worldX, worldZ, seed = 0, variant = 'yellow') {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);
  const h = Math.sin(seed * 12.9898 + 78.233) * 43758.5453;
  group.rotation.y = (h - Math.floor(h)) * Math.PI * 2;

  const poleMat = new THREE.MeshLambertMaterial({ color: 0xf5f5f5 });
  const pole = new THREE.Mesh(
    new THREE.CylinderGeometry(0.08, 0.10, 8.0, 8),
    poleMat,
  );
  pole.position.y = 4.0;
  group.add(pole);

  const goldMat = new THREE.MeshLambertMaterial({ color: 0xc9a227 });
  const finial = new THREE.Mesh(
    new THREE.SphereGeometry(0.16, 10, 8),
    goldMat,
  );
  finial.position.y = 8.2;
  group.add(finial);

  const flagTex = _getLNUFlagTexture(THREE, variant);
  const flagMat = new THREE.MeshLambertMaterial({
    map:  flagTex,
    side: THREE.DoubleSide,
  });
  const flag = new THREE.Mesh(new THREE.PlaneGeometry(2.4, 1.5), flagMat);
  flag.position.set(1.2, 7.25, 0);
  group.add(flag);

  group.traverse(obj => { if (obj.isMesh) obj.raycast = () => {}; });
  return group;
}

/**
 * Build one Swedish flag pole at a given world position.
 *
 *   - Pole: thin white cylinder, ~8 m tall
 *   - Finial: small gold sphere at the top
 *   - Flag: 2.4 × 1.5 m plane textured with the Swedish cross
 *
 * All meshes are non-raycastable so hovering / clicking flows past
 * them to the real buildings underneath.
 */
export function buildSwedishFlagPole(THREE, worldX, worldZ, seed = 0) {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);

  // Deterministic pseudo-random Y rotation per pole so they don't all
  // face the same way — cheap jitter from the seed.
  const h = Math.sin(seed * 12.9898 + 78.233) * 43758.5453;
  group.rotation.y = (h - Math.floor(h)) * Math.PI * 2;

  // ── Pole ──
  const poleMat = new THREE.MeshLambertMaterial({ color: 0xf5f5f5 });
  const pole = new THREE.Mesh(
    new THREE.CylinderGeometry(0.08, 0.10, 8.0, 8),
    poleMat,
  );
  pole.position.y = 4.0;
  group.add(pole);

  // ── Finial (gold ball) ──
  const goldMat = new THREE.MeshLambertMaterial({ color: 0xc9a227 });
  const finial = new THREE.Mesh(
    new THREE.SphereGeometry(0.16, 10, 8),
    goldMat,
  );
  finial.position.y = 8.2;
  group.add(finial);

  // ── Flag (hoisted from the upper pole) ──
  const flagTex = _getSwedishFlagTexture(THREE);
  const flagMat = new THREE.MeshLambertMaterial({
    map:  flagTex,
    side: THREE.DoubleSide,
  });
  const flag = new THREE.Mesh(new THREE.PlaneGeometry(2.4, 1.5), flagMat);
  // Hoist edge at local x=0 (pole), flag extends to x=+2.4. Top edge
  // sits just below the finial at y ≈ 8.
  flag.position.set(1.2, 7.25, 0);
  group.add(flag);

  // Non-raycastable — clicks pass through to real buildings underneath.
  group.traverse(obj => { if (obj.isMesh) obj.raycast = () => {}; });

  return group;
}

// ── Swedish cultural easter eggs ────────────────────────────────────────────
// Five small decorative 3D objects hidden across every city.
// Each is ~1-3m tall, placed on the ground near residential buildings.

/**
 * Dalahäst (Dala horse) — iconic red wooden horse from Dalarna.
 * ~1.5m tall, red body with blue/white saddle decoration.
 */
export function buildDalahast(THREE, worldX, worldZ) {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);
  group.rotation.y = Math.random() * Math.PI * 2;

  const red    = new THREE.MeshLambertMaterial({ color: 0xcc2222 });
  const blue   = new THREE.MeshLambertMaterial({ color: 0x1e40af });
  const white  = new THREE.MeshLambertMaterial({ color: 0xf8fafc });
  const yellow = new THREE.MeshLambertMaterial({ color: 0xfbbf24 });

  // Body (rounded box)
  const body = new THREE.Mesh(new THREE.BoxGeometry(1.4, 0.9, 0.5, 4, 4, 4), red);
  body.position.set(0, 1.1, 0);
  group.add(body);

  // Neck (angled box)
  const neck = new THREE.Mesh(new THREE.BoxGeometry(0.4, 0.8, 0.45, 3, 3, 3), red);
  neck.position.set(0.5, 1.7, 0);
  neck.rotation.z = -0.3;
  group.add(neck);

  // Head
  const head = new THREE.Mesh(new THREE.BoxGeometry(0.55, 0.35, 0.4, 3, 3, 3), red);
  head.position.set(0.75, 2.1, 0);
  group.add(head);

  // Ears (two small cones)
  for (const sz of [-0.12, 0.12]) {
    const ear = new THREE.Mesh(new THREE.ConeGeometry(0.06, 0.15, 6), red);
    ear.position.set(0.7, 2.35, sz);
    group.add(ear);
  }

  // Legs (4 posts)
  for (const [lx, lz] of [[-0.4,-0.15],[-0.4,0.15],[0.4,-0.15],[0.4,0.15]]) {
    const leg = new THREE.Mesh(new THREE.BoxGeometry(0.18, 0.65, 0.18, 2, 3, 2), red);
    leg.position.set(lx, 0.32, lz);
    group.add(leg);
  }

  // Saddle decoration (blue band around body)
  const saddle = new THREE.Mesh(new THREE.BoxGeometry(0.5, 0.92, 0.52, 2, 2, 2), blue);
  saddle.position.set(0.05, 1.1, 0);
  group.add(saddle);

  // White/yellow harness lines
  const harness = new THREE.Mesh(new THREE.BoxGeometry(1.42, 0.06, 0.52, 2, 1, 2), white);
  harness.position.set(0, 1.35, 0);
  group.add(harness);
  const harness2 = new THREE.Mesh(new THREE.BoxGeometry(1.42, 0.06, 0.52, 2, 1, 2), yellow);
  harness2.position.set(0, 0.85, 0);
  group.add(harness2);

  // Small plinth
  const base = new THREE.Mesh(new THREE.CylinderGeometry(0.7, 0.8, 0.15, 16), white);
  base.position.set(0, 0.07, 0);
  group.add(base);

  return group;
}

/**
 * IKEA bag — the iconic blue FRAKTA bag with yellow handles.
 * ~0.6m tall, sitting on the ground as if just set down.
 */
export function buildIkeaBag(THREE, worldX, worldZ) {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);
  group.rotation.y = Math.random() * Math.PI * 2;

  const blue   = new THREE.MeshLambertMaterial({ color: 0x0051ba });
  const yellow = new THREE.MeshLambertMaterial({ color: 0xfbce0a });

  // Bag body (slightly tapered box)
  const bag = new THREE.Mesh(new THREE.BoxGeometry(0.55, 0.55, 0.35, 4, 4, 4), blue);
  bag.position.set(0, 0.28, 0);
  group.add(bag);

  // Bag opening (slightly wider at top)
  const rim = new THREE.Mesh(new THREE.BoxGeometry(0.58, 0.04, 0.38, 2, 1, 2), yellow);
  rim.position.set(0, 0.56, 0);
  group.add(rim);

  // Two yellow handles (torus arcs)
  for (const sz of [-0.19, 0.19]) {
    const handle = new THREE.Mesh(
      new THREE.TorusGeometry(0.12, 0.02, 8, 16, Math.PI),
      yellow
    );
    handle.position.set(0, 0.58, sz);
    handle.rotation.x = 0;
    handle.rotation.z = 0;
    group.add(handle);
  }

  // IKEA text on side (simplified as a yellow rectangle)
  const label = new THREE.Mesh(new THREE.BoxGeometry(0.3, 0.08, 0.01, 2, 1, 1), yellow);
  label.position.set(0, 0.35, 0.18);
  group.add(label);
  const label2 = label.clone();
  label2.position.set(0, 0.35, -0.18);
  group.add(label2);

  return group;
}

/**
 * Fika cup — a coffee cup on a saucer, representing Swedish fika culture.
 * ~0.4m tall on a small table/saucer.
 */
export function buildFikaCup(THREE, worldX, worldZ) {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);

  const ceramic = new THREE.MeshLambertMaterial({ color: 0xf5f0e8 }); // cream white
  const coffee  = new THREE.MeshLambertMaterial({ color: 0x3e2723 }); // dark brown
  const blue    = new THREE.MeshLambertMaterial({ color: 0x1565c0 }); // Swedish blue accent

  // Small table/stump to sit on
  const stump = new THREE.Mesh(new THREE.CylinderGeometry(0.25, 0.28, 0.5, 12, 4),
    new THREE.MeshLambertMaterial({ color: 0x6d4c2e }));
  stump.position.set(0, 0.25, 0);
  group.add(stump);

  // Saucer
  const saucer = new THREE.Mesh(new THREE.CylinderGeometry(0.2, 0.18, 0.03, 16, 2), ceramic);
  saucer.position.set(0, 0.52, 0);
  group.add(saucer);

  // Cup body (cylinder)
  const cup = new THREE.Mesh(new THREE.CylinderGeometry(0.1, 0.08, 0.16, 16, 4), ceramic);
  cup.position.set(0, 0.63, 0);
  group.add(cup);

  // Blue band on cup (Swedish style)
  const band = new THREE.Mesh(new THREE.CylinderGeometry(0.101, 0.085, 0.04, 16, 2), blue);
  band.position.set(0, 0.60, 0);
  group.add(band);

  // Coffee inside
  const coffeeTop = new THREE.Mesh(new THREE.CylinderGeometry(0.09, 0.09, 0.02, 12), coffee);
  coffeeTop.position.set(0, 0.70, 0);
  group.add(coffeeTop);

  // Handle (torus segment)
  const handle = new THREE.Mesh(
    new THREE.TorusGeometry(0.055, 0.015, 6, 12, Math.PI),
    ceramic
  );
  handle.position.set(0.11, 0.63, 0);
  handle.rotation.y = Math.PI / 2;
  group.add(handle);

  // Small kanelbulle (cinnamon bun) on saucer
  const bulle = new THREE.Mesh(new THREE.TorusGeometry(0.04, 0.02, 8, 16),
    new THREE.MeshLambertMaterial({ color: 0xc68a3c }));
  bulle.position.set(0.12, 0.54, 0.08);
  bulle.rotation.x = Math.PI / 2;
  group.add(bulle);

  return group;
}

/**
 * Midsommarstång (Maypole) — the iconic midsummer pole.
 * ~4m tall with cross-arms and hanging wreaths/rings.
 */
export function buildMaypole(THREE, worldX, worldZ) {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);

  const wood   = new THREE.MeshLambertMaterial({ color: 0x8b6914 });
  const green  = new THREE.MeshLambertMaterial({ color: 0x2d7a2d });
  const flower = new THREE.MeshLambertMaterial({ color: 0xfbcfe8 }); // pink
  const blue   = new THREE.MeshLambertMaterial({ color: 0x2563eb }); // ribbon blue
  const yellow = new THREE.MeshLambertMaterial({ color: 0xfbbf24 }); // ribbon yellow

  // Main pole
  const pole = new THREE.Mesh(new THREE.CylinderGeometry(0.06, 0.08, 4.0, 12, 8), wood);
  pole.position.set(0, 2.0, 0);
  group.add(pole);

  // Cross-arm at top
  const crossarm = new THREE.Mesh(new THREE.CylinderGeometry(0.04, 0.04, 2.0, 8, 4), wood);
  crossarm.position.set(0, 3.5, 0);
  crossarm.rotation.z = Math.PI / 2;
  group.add(crossarm);

  // Garland wrapping around the pole (green cylinder)
  const garland = new THREE.Mesh(new THREE.CylinderGeometry(0.1, 0.1, 3.8, 12, 6), green);
  garland.position.set(0, 2.0, 0);
  group.add(garland);

  // Top crown/ring
  const crown = new THREE.Mesh(new THREE.TorusGeometry(0.2, 0.06, 8, 24), green);
  crown.position.set(0, 4.1, 0);
  crown.rotation.x = Math.PI / 2;
  group.add(crown);

  // Two hanging rings (one on each side of the crossarm)
  for (const sx of [-0.7, 0.7]) {
    const ring = new THREE.Mesh(new THREE.TorusGeometry(0.25, 0.04, 8, 20), green);
    ring.position.set(sx, 3.0, 0);
    group.add(ring);
    // Flowers on rings
    for (let i = 0; i < 6; i++) {
      const a = (i / 6) * Math.PI * 2;
      const f = new THREE.Mesh(new THREE.SphereGeometry(0.03, 6, 4), flower);
      f.position.set(sx + Math.cos(a) * 0.25, 3.0 + Math.sin(a) * 0.25, 0);
      group.add(f);
    }
    // Ribbons hanging down
    const ribbon1 = new THREE.Mesh(new THREE.BoxGeometry(0.03, 0.8, 0.01, 1, 4, 1), blue);
    ribbon1.position.set(sx - 0.1, 2.5, 0);
    group.add(ribbon1);
    const ribbon2 = new THREE.Mesh(new THREE.BoxGeometry(0.03, 0.7, 0.01, 1, 4, 1), yellow);
    ribbon2.position.set(sx + 0.1, 2.55, 0);
    group.add(ribbon2);
  }

  return group;
}

/**
 * Moose (Älg) — the king of the Swedish forest.
 * ~2m tall at the shoulder, standing in profile.
 */
export function buildMoose(THREE, worldX, worldZ) {
  const group = new THREE.Group();
  group.position.set(worldX, 0, worldZ);
  group.rotation.y = Math.random() * Math.PI * 2;

  const brown     = new THREE.MeshLambertMaterial({ color: 0x5c3a1e });
  const darkBrown = new THREE.MeshLambertMaterial({ color: 0x3e2410 });
  const antler    = new THREE.MeshLambertMaterial({ color: 0x8b7355 });
  const nose      = new THREE.MeshLambertMaterial({ color: 0x2d1a0e });

  // Body (large oval)
  const body = new THREE.Mesh(new THREE.BoxGeometry(2.0, 1.0, 0.8, 6, 4, 4), brown);
  body.position.set(0, 1.6, 0);
  group.add(body);

  // Shoulder hump
  const hump = new THREE.Mesh(new THREE.SphereGeometry(0.45, 8, 6), brown);
  hump.position.set(-0.3, 2.25, 0);
  group.add(hump);

  // Neck
  const neck = new THREE.Mesh(new THREE.BoxGeometry(0.35, 0.7, 0.5, 3, 3, 3), brown);
  neck.position.set(-0.7, 2.3, 0);
  neck.rotation.z = 0.4;
  group.add(neck);

  // Head
  const head = new THREE.Mesh(new THREE.BoxGeometry(0.6, 0.4, 0.45, 4, 3, 3), brown);
  head.position.set(-1.1, 2.5, 0);
  group.add(head);

  // Long moose nose/muzzle
  const muzzle = new THREE.Mesh(new THREE.BoxGeometry(0.35, 0.25, 0.35, 3, 3, 3), darkBrown);
  muzzle.position.set(-1.4, 2.35, 0);
  group.add(muzzle);

  // Nose tip
  const noseTip = new THREE.Mesh(new THREE.SphereGeometry(0.1, 6, 4), nose);
  noseTip.position.set(-1.55, 2.35, 0);
  group.add(noseTip);

  // Bell (dewlap) under chin
  const bell = new THREE.Mesh(new THREE.ConeGeometry(0.08, 0.25, 6), darkBrown);
  bell.position.set(-1.1, 2.15, 0);
  group.add(bell);

  // Antlers (palmate — wide flat paddles)
  for (const sz of [-0.3, 0.3]) {
    // Main beam going up and out
    const beam = new THREE.Mesh(new THREE.CylinderGeometry(0.03, 0.04, 0.8, 6, 3), antler);
    beam.position.set(-0.9, 2.9, sz);
    beam.rotation.z = sz > 0 ? 0.3 : -0.3;
    beam.rotation.x = sz > 0 ? -0.3 : 0.3;
    group.add(beam);
    // Palm (flat paddle)
    const palm = new THREE.Mesh(new THREE.BoxGeometry(0.5, 0.4, 0.04, 3, 3, 1), antler);
    palm.position.set(-0.85, 3.35, sz * 1.5);
    palm.rotation.z = sz > 0 ? 0.2 : -0.2;
    group.add(palm);
    // Tines (3 points)
    for (let t = 0; t < 3; t++) {
      const tine = new THREE.Mesh(new THREE.ConeGeometry(0.02, 0.2, 5), antler);
      tine.position.set(-0.85 + (t - 1) * 0.15, 3.6, sz * 1.5);
      group.add(tine);
    }
  }

  // Legs (4)
  for (const [lx, lz] of [[-0.6,-0.2],[-0.6,0.2],[0.6,-0.2],[0.6,0.2]]) {
    const leg = new THREE.Mesh(new THREE.BoxGeometry(0.15, 1.1, 0.15, 2, 4, 2), darkBrown);
    leg.position.set(lx, 0.55, lz);
    group.add(leg);
    // Hoof
    const hoof = new THREE.Mesh(new THREE.BoxGeometry(0.13, 0.08, 0.17, 2, 1, 2), nose);
    hoof.position.set(lx, 0.04, lz);
    group.add(hoof);
  }

  // Tail (small stub)
  const tail = new THREE.Mesh(new THREE.BoxGeometry(0.1, 0.12, 0.08, 2, 2, 2), darkBrown);
  tail.position.set(0.95, 1.8, 0);
  group.add(tail);

  return group;
}
