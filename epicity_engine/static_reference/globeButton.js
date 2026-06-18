/**
 * globeButton.js — tiny self-contained Three.js scene embedded inside the
 * #btnGlobe header button. Renders a lit sphere with a procedural
 * continent texture, continuously rotating on the Y axis so the button
 * reads as a live spinning Earth instead of a dead glyph.
 *
 * Idle spin speed: 10 s / revolution.
 * Hover  spin speed: 3 s / revolution (snaps immediately on mouse enter).
 *
 * No external textures — the continent map is drawn into a 2D canvas at
 * init time and uploaded to a THREE.CanvasTexture. This keeps the whole
 * feature asset-free and makes the button work even if we later vendor
 * the app for offline use.
 */

import * as THREE from 'three';

const CANVAS_PX   = 44;    // visual size in CSS pixels
const RENDER_PX   = 88;    // backing store for crisp rendering on hi-dpi
const IDLE_PERIOD = 10;    // seconds / revolution, no hover
const FAST_PERIOD = 3;     // seconds / revolution, hover

export function initGlobeButton() {
  const btn = document.getElementById('btnGlobe');
  if (!btn) return;

  // Wipe any previous glyph (we used to put an inline SVG here) and mount
  // a WebGL canvas in its place. Keep the button's text accessible to
  // screen readers via aria-label (set in index.html).
  btn.innerHTML = '';
  const canvas = document.createElement('canvas');
  canvas.width  = RENDER_PX;
  canvas.height = RENDER_PX;
  canvas.style.width  = `${CANVAS_PX}px`;
  canvas.style.height = `${CANVAS_PX}px`;
  canvas.style.display = 'block';
  btn.appendChild(canvas);

  // ── Renderer / scene / camera ───────────────────────────────────────
  // alpha:true so the button's background colour shows through behind
  // the sphere. antialias keeps the silhouette smooth at this small
  // size. No shadows, no tone mapping — overkill for a 44×44 button.
  const renderer = new THREE.WebGLRenderer({
    canvas, alpha: true, antialias: true,
  });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.setSize(RENDER_PX, RENDER_PX, false);
  renderer.setClearColor(0x000000, 0);

  const scene  = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(40, 1, 0.1, 10);
  camera.position.set(0, 0, 3.1);
  camera.lookAt(0, 0, 0);

  // ── Procedural Earth texture ────────────────────────────────────────
  // A simple equirectangular (2:1) canvas with an ocean fill plus a few
  // green continent blobs. Not geographically accurate — just needs to
  // read as "planet with land" when spinning at small scale.
  const tex = _buildEarthTexture();

  // ── Sphere mesh ─────────────────────────────────────────────────────
  const sphere = new THREE.Mesh(
    new THREE.SphereGeometry(1, 48, 48),
    new THREE.MeshPhongMaterial({
      map:       tex,
      specular:  0x223355,
      shininess: 18,
    }),
  );
  // Tilt slightly so the continents read as "spinning globe" from the
  // side instead of a flat disc — ~18° axial tilt like Earth-ish.
  sphere.rotation.z = 0.32;
  scene.add(sphere);

  // ── Lighting ────────────────────────────────────────────────────────
  // A single directional key light from the upper-right gives the
  // sphere an obvious day/night terminator, plus a soft cool ambient
  // so the dark side doesn't go fully black against the header bg.
  const key = new THREE.DirectionalLight(0xffffff, 1.15);
  key.position.set(2.5, 1.8, 3);
  scene.add(key);
  scene.add(new THREE.AmbientLight(0x8ab4f8, 0.55));

  // ── Spin loop ───────────────────────────────────────────────────────
  // A dedicated rAF loop — completely independent from the main sim
  // animation loop in ui.js so the globe keeps spinning even when the
  // sim is paused on the macro view (where #btnGlobe is hidden anyway,
  // but it saves us a visibility check each frame).
  let period = IDLE_PERIOD;
  btn.addEventListener('mouseenter', () => { period = FAST_PERIOD; });
  btn.addEventListener('mouseleave', () => { period = IDLE_PERIOD; });

  let last = performance.now();
  const tick = (now) => {
    const dt = Math.min(0.1, (now - last) / 1000);
    last = now;
    sphere.rotation.y += (Math.PI * 2) * (dt / period);
    renderer.render(scene, camera);
    requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
}

/**
 * Paint a stylised Earth-ish equirectangular map onto a 2D canvas and
 * return it as a THREE.CanvasTexture. Continent positions are chosen to
 * read as land from any longitude so the rotation always shows green.
 */
function _buildEarthTexture() {
  const W = 512, H = 256;
  const c = document.createElement('canvas');
  c.width = W; c.height = H;
  const ctx = c.getContext('2d');

  // Ocean
  const ocean = ctx.createLinearGradient(0, 0, 0, H);
  ocean.addColorStop(0,   '#123a6b');
  ocean.addColorStop(0.5, '#0f2d5a');
  ocean.addColorStop(1,   '#0b1f40');
  ctx.fillStyle = ocean;
  ctx.fillRect(0, 0, W, H);

  // Helper: draw a blobby "continent" at (cx, cy) with rough radii
  const blob = (cx, cy, rx, ry, rot, fill) => {
    ctx.save();
    ctx.translate(cx, cy);
    ctx.rotate(rot);
    ctx.fillStyle = fill;
    ctx.beginPath();
    // Main body
    ctx.ellipse(0, 0, rx, ry, 0, 0, Math.PI * 2);
    ctx.fill();
    // A few bumps for a less-round silhouette
    ctx.beginPath();
    ctx.ellipse(rx * 0.6,  ry * 0.4, rx * 0.35, ry * 0.5, 0, 0, Math.PI * 2);
    ctx.fill();
    ctx.beginPath();
    ctx.ellipse(-rx * 0.5, -ry * 0.6, rx * 0.4, ry * 0.35, 0, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  };

  // A few shades of green so continents don't look flat
  const LAND  = '#3d9b5c';
  const LAND2 = '#57b86f';

  // Rough continent placements across the 0–512 longitude range so
  // something is always visible as the sphere rotates.
  blob( 60,  90, 40, 42, 0.15, LAND);   // Americas-ish (left)
  blob( 70, 150, 22, 40, -0.1, LAND2);
  blob(160, 110, 32, 44, 0.2,  LAND);   // Africa-ish
  blob(180, 175, 18, 28, 0.0,  LAND2);
  blob(260, 100, 55, 38, -0.1, LAND);   // Eurasia-ish
  blob(330, 120, 30, 25, 0.05, LAND2);
  blob(380, 180, 30, 18, 0.1,  LAND);   // Australia-ish
  blob(450,  90, 35, 30, 0.1,  LAND2);  // wrap-around continent

  // Soft polar caps
  ctx.fillStyle = 'rgba(255,255,255,0.55)';
  ctx.fillRect(0, 0, W, 14);
  ctx.fillRect(0, H - 14, W, 14);

  const tex = new THREE.CanvasTexture(c);
  tex.anisotropy = 4;
  tex.needsUpdate = true;
  return tex;
}
