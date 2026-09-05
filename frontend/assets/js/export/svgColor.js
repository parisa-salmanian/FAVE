/* FAVE vector export — colour + gradient normalisation.
 *
 * CorelDraw only reads plain `#rrggbb` plus a separate opacity. It does not
 * understand oklch()/lab()/color(), CSS custom properties, or currentColor,
 * all of which this UI uses heavily. Every colour therefore goes through a
 * 1x1 canvas so the browser resolves it to concrete RGBA for us.
 */
(function () {
  'use strict';
  const NS = (window.FAVEExport = window.FAVEExport || {});

  const cvs = document.createElement('canvas');
  cvs.width = cvs.height = 1;
  const ctx = cvs.getContext('2d', { willReadFrequently: true });
  const cache = new Map();

  const NONE = Object.freeze({ hex: '#000000', alpha: 0 });

  function hex2(n) { return ('0' + (n & 255).toString(16)).slice(-2); }

  /** Any CSS colour string -> {hex, alpha}. Never throws. */
  function color(css) {
    if (css == null) return NONE;
    const s = String(css).trim();
    if (!s || s === 'none' || s === 'transparent') return NONE;
    const hit = cache.get(s);
    if (hit) return hit;

    let out = NONE;
    try {
      // Sentinel: an invalid assignment leaves fillStyle untouched, which is
      // how we tell "browser could not parse this" from a real colour.
      ctx.fillStyle = '#010203';
      ctx.fillStyle = s;
      if (ctx.fillStyle === '#010203' && s.toLowerCase().replace(/\s/g, '') !== '#010203') {
        out = NONE;
      } else {
        ctx.clearRect(0, 0, 1, 1);
        ctx.fillRect(0, 0, 1, 1);
        const d = ctx.getImageData(0, 0, 1, 1).data;
        out = { hex: '#' + hex2(d[0]) + hex2(d[1]) + hex2(d[2]), alpha: d[3] / 255 };
      }
    } catch (_) { out = NONE; }

    cache.set(s, out);
    return out;
  }

  /** Split on commas that sit at paren-depth 0. */
  function splitTop(str) {
    const parts = [];
    let depth = 0, start = 0;
    for (let i = 0; i < str.length; i++) {
      const c = str[i];
      if (c === '(') depth++;
      else if (c === ')') depth--;
      else if (c === ',' && depth === 0) { parts.push(str.slice(start, i)); start = i + 1; }
    }
    parts.push(str.slice(start));
    return parts.map((p) => p.trim()).filter(Boolean);
  }

  const SIDE_ANGLE = {
    'to top': 0, 'to right': 90, 'to bottom': 180, 'to left': 270,
    'to top right': 45, 'to right top': 45,
    'to bottom right': 135, 'to right bottom': 135,
    'to bottom left': 225, 'to left bottom': 225,
    'to top left': 315, 'to left top': 315,
  };

  function angleOf(token) {
    const t = token.trim().toLowerCase();
    if (t in SIDE_ANGLE) return SIDE_ANGLE[t];
    let m = /^(-?[\d.]+)deg$/.exec(t);
    if (m) return parseFloat(m[1]);
    m = /^(-?[\d.]+)turn$/.exec(t);
    if (m) return parseFloat(m[1]) * 360;
    m = /^(-?[\d.]+)rad$/.exec(t);
    if (m) return (parseFloat(m[1]) * 180) / Math.PI;
    return null;
  }

  /**
   * Parse a `linear-gradient(...)` / `repeating-linear-gradient(...)` value
   * into SVG userSpaceOnUse coordinates for the given border box.
   * Returns {x1,y1,x2,y2,stops:[{offset,hex,alpha}]} or null.
   */
  function linearGradient(cssImage, rect) {
    if (!cssImage) return null;
    const m = /(?:repeating-)?linear-gradient\(([\s\S]*)\)/.exec(cssImage);
    if (!m) return null;

    const args = splitTop(m[1]);
    if (!args.length) return null;

    let deg = 180; // CSS default is `to bottom`
    let first = angleOf(args[0]);
    if (first !== null) { deg = first; args.shift(); }
    if (args.length < 2) return null;

    const W = Math.max(rect.width, 0.01);
    const H = Math.max(rect.height, 0.01);
    const rad = (deg * Math.PI) / 180;
    // CSS angles run clockwise from "to top"; y grows downwards on screen.
    const dx = Math.sin(rad), dy = -Math.cos(rad);
    const len = Math.abs(W * Math.sin(rad)) + Math.abs(H * Math.cos(rad));
    const cx = rect.left + W / 2, cy = rect.top + H / 2;

    const stops = [];
    args.forEach((arg, i) => {
      // "<colour> [pos]" — the colour itself may contain spaces inside parens.
      const pm = /^([\s\S]*?)(?:\s+(-?[\d.]+(?:%|px|em|rem)))?$/.exec(arg.trim());
      let colTxt = arg.trim(), posTxt = null;
      if (pm && pm[2]) { colTxt = pm[1].trim(); posTxt = pm[2]; }
      const c = color(colTxt);
      let offset = null;
      if (posTxt) {
        if (posTxt.endsWith('%')) offset = parseFloat(posTxt) / 100;
        else if (posTxt.endsWith('px')) offset = len ? parseFloat(posTxt) / len : 0;
      }
      if (offset === null) offset = args.length === 1 ? 0 : i / (args.length - 1);
      stops.push({ offset: Math.min(1, Math.max(0, offset)), hex: c.hex, alpha: c.alpha });
    });
    if (stops.length < 2) return null;

    return {
      x1: cx - (dx * len) / 2, y1: cy - (dy * len) / 2,
      x2: cx + (dx * len) / 2, y2: cy + (dy * len) / 2,
      stops,
    };
  }

  NS.color = color;
  NS.linearGradient = linearGradient;
  NS.splitTopLevel = splitTop;
})();
