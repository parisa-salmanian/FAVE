/* FAVE vector export — canvas + external asset capture.
 *
 * The MapLibre/deck.gl map is a single interleaved WebGL canvas, so it can
 * only travel as a raster. Everything else we try to keep as vectors: .svg
 * assets are fetched and inlined as real paths rather than embedded bitmaps.
 */
(function () {
  'use strict';
  const NS = (window.FAVEExport = window.FAVEExport || {});
  const S = NS.svg;

  NS.warnings = [];

  function warn(msg) {
    if (NS.warnings.indexOf(msg) === -1) NS.warnings.push(msg);
  }

  /** Reach the MapLibre instance: state.js declares it as a script-level let. */
  function getMap() {
    try { if (typeof map !== 'undefined' && map && map.getCanvas) return map; } catch (e) { /* not defined */ }
    if (window.map && window.map.getCanvas) return window.map;
    return null;
  }

  /**
   * Can this canvas still be read after compositing?
   * Asking the context directly is exact. Sampling pixels is not: downscaling
   * a map full of thin strokes averages them away and reports a false blank.
   */
  function bufferReadable(cv) {
    try {
      const gl = cv.getContext('webgl2') || cv.getContext('webgl');
      if (!gl) return true;                       // 2D canvas — always readable
      const attrs = gl.getContextAttributes();
      return !attrs || attrs.preserveDrawingBuffer !== false;
    } catch (e) {
      return true;
    }
  }

  /**
   * Make sure the map canvas holds the frame we are about to capture.
   *
   * With preserveDrawingBuffer the last painted frame IS the current view, so
   * there is nothing to wait for. That matters: an earlier version waited for
   * the map to go 'idle', and a map that never settles (deck.gl re-rendering
   * behind a loading city) left the export spinning forever.
   */
  function prepare() {
    const m = getMap();
    if (!m || typeof m.triggerRepaint !== 'function') return Promise.resolve();

    let cv = null;
    try { cv = m.getCanvas(); } catch (e) { cv = null; }
    if (!cv || bufferReadable(cv)) return Promise.resolve();

    // No preserved buffer: nudge one repaint and grab the next frame, with a
    // hard ceiling so a busy main thread can never block the export.
    return new Promise(function (resolve) {
      let done = false;
      const finish = function () { if (!done) { done = true; resolve(); } };
      try { m.once('render', finish); m.triggerRepaint(); } catch (e) { finish(); }
      setTimeout(finish, 500);
    });
  }

  /** Canvas -> PNG data URI, or null if it cannot be read. */
  function captureCanvas(cv) {
    if (!cv || !cv.width || !cv.height) return null;
    if (!bufferReadable(cv)) {
      warn('The map layer may come out blank: this WebGL canvas was created ' +
           'without preserveDrawingBuffer. Reload the page and export again.');
    }
    try {
      return cv.toDataURL('image/png');
    } catch (e) {
      warn('A canvas could not be read (cross-origin tainted): ' + (e.message || e));
      return null;
    }
  }

  function objectFitToPAR(fit) {
    if (fit === 'fill') return 'none';
    if (fit === 'cover') return 'xMidYMid slice';
    return 'xMidYMid meet';   // contain / none / scale-down
  }

  function blobToDataURL(blob) {
    return new Promise(function (resolve, reject) {
      const fr = new FileReader();
      fr.onload = function () { resolve(fr.result); };
      fr.onerror = reject;
      fr.readAsDataURL(blob);
    });
  }

  /** Drop an inline <svg> asset into `holder`, fitted to `rect`. */
  function placeInlineSvg(svgText, holder, rect, cs) {
    let doc;
    try { doc = new DOMParser().parseFromString(svgText, 'image/svg+xml'); } catch (e) { return false; }
    const src = doc && doc.documentElement;
    if (!src || src.tagName.toLowerCase() !== 'svg') return false;
    if (doc.querySelector('parsererror')) return false;

    // Authoring tools ship these assets with <style> blocks and class="..".
    // CorelDraw's CSS support is unreliable, so mount the asset off-screen,
    // let the browser resolve its own cascade, then flatten the result onto a
    // clone as plain presentation attributes (this also resolves currentColor).
    const stage = document.createElement('div');
    stage.setAttribute('data-export-ignore', '1');
    stage.style.cssText =
      'position:absolute;left:-99999px;top:0;width:256px;height:256px;overflow:hidden;';

    let clone;
    try {
      const live = document.importNode(src, true);
      if (cs && cs.color) live.style.color = cs.color;
      stage.appendChild(live);
      document.body.appendChild(stage);
      clone = live.cloneNode(true);
      NS.copySvgStyles(live, clone);
    } catch (e) {
      clone = src.cloneNode(true);
    } finally {
      stage.remove();
    }

    clone.querySelectorAll('script,style,metadata,foreignObject,title,desc')
      .forEach(function (n) { n.remove(); });
    if (NS.stripClips) NS.stripClips(clone);        // no PowerClips in Corel
    if (NS.flattenGroups) NS.flattenGroups(clone);  // fewer groups to dig into

    let vw = 0, vh = 0, vx = 0, vy = 0;
    const vbAttr = clone.getAttribute('viewBox');
    if (vbAttr) {
      const p = vbAttr.trim().split(/[\s,]+/).map(parseFloat);
      if (p.length === 4) { vx = p[0]; vy = p[1]; vw = p[2]; vh = p[3]; }
    }
    if (!vw || !vh) {
      vw = parseFloat(clone.getAttribute('width')) || rect.width;
      vh = parseFloat(clone.getAttribute('height')) || rect.height;
    }

    const s = (vw > 0 && vh > 0) ? Math.min(rect.width / vw, rect.height / vh) : 1;
    const tx = rect.left + (rect.width - vw * s) / 2 - vx * s;
    const ty = rect.top + (rect.height - vh * s) / 2 - vy * s;

    NS.uniquifyIds(clone, 'a' + Math.floor(Math.random() * 1e6) + '_');

    const g = S.group({
      transform: 'translate(' + S.n(tx) + ',' + S.n(ty) + ') scale(' + S.n(s) + ')',
    });
    while (clone.firstChild) g.appendChild(clone.firstChild);
    holder.appendChild(g);
    return true;
  }

  /**
   * Resolve an <img> src or background url() into `holder`.
   * SVG sources become real vectors; anything else becomes an <image> with an
   * embedded data URI so the exported file is self-contained.
   */
  function resolveAsset(url, holder, rect, cs, ctx) {
    if (!url) return Promise.resolve();

    let abs;
    try { abs = new URL(url, document.baseURI).href; } catch (e) { return Promise.resolve(); }

    const isSvg = /\.svg(\?|#|$)/i.test(abs) || abs.indexOf('data:image/svg+xml') === 0;

    const asRaster = function () {
      return fetch(abs, { credentials: 'same-origin' })
        .then(function (res) { return res.ok ? res.blob() : null; })
        .then(function (blob) {
          if (!blob) return;
          return blobToDataURL(blob).then(function (dataUrl) {
            const img = S.el('image', {
              x: rect.left, y: rect.top, width: rect.width, height: rect.height,
              preserveAspectRatio: objectFitToPAR(cs ? cs.objectFit : 'contain'),
            });
            S.setHref(img, dataUrl);
            holder.appendChild(img);
            if (ctx) ctx.rasterCount++;
          });
        })
        .catch(function () { warn('Could not embed image: ' + abs); });
    };

    if (!isSvg) return asRaster();

    if (abs.indexOf('data:image/svg+xml') === 0) {
      let text = '';
      try {
        text = abs.indexOf(';base64,') !== -1
          ? atob(abs.split(';base64,')[1])
          : decodeURIComponent(abs.slice(abs.indexOf(',') + 1));
      } catch (e) { text = ''; }
      if (text && placeInlineSvg(text, holder, rect, cs)) return Promise.resolve();
      return asRaster();
    }

    return fetch(abs, { credentials: 'same-origin' })
      .then(function (res) { return res.ok ? res.text() : null; })
      .then(function (text) {
        if (!text || !placeInlineSvg(text, holder, rect, cs)) return asRaster();
      })
      .catch(function () { return asRaster(); });
  }

  NS.getMap = getMap;
  NS.prepare = prepare;
  NS.captureCanvas = captureCanvas;
  NS.resolveAsset = resolveAsset;
  NS.exportWarn = warn;
})();
