/**
 * colorscales.js — Perceptually uniform continuous color scales.
 * Shared by view.js (THREE.Color) and minimap.js (CSS strings).
 *
 * Each scale is defined as 5 keypoints: [t, r, g, b] with r/g/b in 0-255.
 * Values between keypoints are linearly interpolated.
 */

const _SCALES = {
  // ── Perceptually uniform (matplotlib family) ────────────────────────────
  // Viridis: dark-purple → teal → yellow-green
  viridis: [
    [0.00,  68,   1,  84],
    [0.25,  59,  82, 139],
    [0.50,  33, 145, 140],
    [0.75,  94, 201,  98],
    [1.00, 253, 231,  37],
  ],
  // Plasma: dark-purple → magenta → orange → yellow
  plasma: [
    [0.00,  13,   8, 135],
    [0.25, 126,   3, 168],
    [0.50, 204,  71, 120],
    [0.75, 248, 149,  64],
    [1.00, 240, 249,  33],
  ],
  // Inferno: black → red → yellow (high contrast for dark backgrounds)
  inferno: [
    [0.00,   0,   0,   4],
    [0.25,  87,  16, 110],
    [0.50, 187,  55,  84],
    [0.75, 249, 142,   9],
    [1.00, 252, 255, 164],
  ],
  // Magma: black → purple → pink → cream
  magma: [
    [0.00,   0,   0,   4],
    [0.25,  80,  18, 123],
    [0.50, 182,  54, 121],
    [0.75, 251, 136,  97],
    [1.00, 252, 253, 191],
  ],
  // Cividis: color-blind-friendly dark-blue → yellow
  cividis: [
    [0.00,   0,  32,  77],
    [0.25,  52,  76, 108],
    [0.50, 124, 123, 120],
    [0.75, 187, 175,  98],
    [1.00, 255, 233,  69],
  ],

  // ── Google Turbo (vivid rainbow, high dynamic range) ────────────────────
  turbo: [
    [0.00,  48,  18,  59],
    [0.25,  71, 160, 240],
    [0.50, 107, 253, 123],
    [0.75, 253, 181,  56],
    [1.00, 122,  4,   3],
  ],

  // ── Diverging / cool → hot ──────────────────────────────────────────────
  // Blue → red — intuitive "cool → hot" for infection
  bluered: [
    [0.00,  59, 130, 246],   // cool blue — no infection
    [0.25, 147, 197, 253],   // pale blue
    [0.50, 254, 240, 138],   // yellow mid
    [0.75, 251, 146,  60],   // orange
    [1.00, 220,  38,  38],   // deep red — fully infected
  ],
  // Cool-warm (ParaView-style diverging, neutral mid)
  coolwarm: [
    [0.00,  59,  76, 192],
    [0.25, 144, 178, 254],
    [0.50, 220, 220, 220],
    [0.75, 245, 156, 125],
    [1.00, 180,   4,  38],
  ],
  // RdYlGn reversed — green (safe) → yellow → red (danger)
  rdylgn: [
    [0.00,  26, 150,  65],
    [0.25, 166, 217, 106],
    [0.50, 255, 255, 191],
    [0.75, 253, 174,  97],
    [1.00, 215,  25,  28],
  ],
  // Spectral (ColorBrewer) — smooth rainbow with softer tones
  spectral: [
    [0.00,  50, 136, 189],
    [0.25, 171, 221, 164],
    [0.50, 255, 255, 191],
    [0.75, 253, 174,  97],
    [1.00, 213,  62,  79],
  ],

  // ── Single-hue ramps ────────────────────────────────────────────────────
  // Blues — light → dark blue
  blues: [
    [0.00, 247, 251, 255],
    [0.25, 198, 219, 239],
    [0.50, 107, 174, 214],
    [0.75,  33, 113, 181],
    [1.00,   8,  48, 107],
  ],
  // Reds — cream → deep red (infection-severity ramp)
  reds: [
    [0.00, 255, 245, 240],
    [0.25, 252, 187, 161],
    [0.50, 252, 110,  84],
    [0.75, 203,  24,  29],
    [1.00, 103,   0,  13],
  ],
  // Greens — pale → forest
  greens: [
    [0.00, 247, 252, 245],
    [0.25, 199, 233, 192],
    [0.50, 116, 196, 118],
    [0.75,  35, 139,  69],
    [1.00,   0,  68,  27],
  ],
  // Oranges — cream → burnt orange
  oranges: [
    [0.00, 255, 245, 235],
    [0.25, 253, 208, 162],
    [0.50, 253, 141,  60],
    [0.75, 217,  72,   1],
    [1.00, 127,  39,   4],
  ],
  // Purples — pale lavender → deep violet
  purples: [
    [0.00, 252, 251, 253],
    [0.25, 218, 218, 235],
    [0.50, 158, 154, 200],
    [0.75, 106,  81, 163],
    [1.00,  63,   0, 125],
  ],
  // Greys — monochrome
  greys: [
    [0.00, 255, 255, 255],
    [0.25, 217, 217, 217],
    [0.50, 150, 150, 150],
    [0.75,  82,  82,  82],
    [1.00,   0,   0,   0],
  ],

  // ── Thermal / cyclic ─────────────────────────────────────────────────────
  // Hot: black → red → yellow → white
  hot: [
    [0.00,   0,   0,   0],
    [0.25, 182,   0,   0],
    [0.50, 255,  91,   0],
    [0.75, 255, 219,   0],
    [1.00, 255, 255, 255],
  ],
  // Cool: cyan → magenta
  cool: [
    [0.00,   0, 255, 255],
    [0.25,  64, 191, 255],
    [0.50, 128, 127, 255],
    [0.75, 191,  63, 255],
    [1.00, 255,   0, 255],
  ],
};

/**
 * Sample a color scale at t ∈ [0, 1].
 * Returns [r, g, b] with components in 0–255.
 *
 * @param {number} t       — normalized value (clamped to [0,1])
 * @param {string} scale   — 'plasma' | 'viridis'
 * @returns {[number, number, number]}
 */
export function sampleScale(t, scale = 'plasma') {
  const kp = _SCALES[scale] ?? _SCALES.plasma;
  t = Math.max(0, Math.min(1, t));

  // Find the two surrounding keypoints
  let lo = kp[0];
  let hi = kp[kp.length - 1];
  for (let i = 0; i < kp.length - 1; i++) {
    if (t <= kp[i + 1][0]) {
      lo = kp[i];
      hi = kp[i + 1];
      break;
    }
  }

  const span = hi[0] - lo[0];
  const f    = span > 0 ? (t - lo[0]) / span : 0;

  return [
    Math.round(lo[1] + f * (hi[1] - lo[1])),
    Math.round(lo[2] + f * (hi[2] - lo[2])),
    Math.round(lo[3] + f * (hi[3] - lo[3])),
  ];
}

/**
 * Sample a color scale and return a CSS `rgb(r,g,b)` string.
 *
 * @param {number} t
 * @param {string} scale
 * @returns {string}
 */
export function sampleScaleCSS(t, scale = 'plasma') {
  const [r, g, b] = sampleScale(t, scale);
  return `rgb(${r},${g},${b})`;
}
