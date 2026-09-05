/* FAVE vector export — live DOM -> native SVG primitives.
 *
 * We do NOT use <foreignObject> (CorelDraw ignores it entirely). Instead we
 * walk the rendered tree, read getComputedStyle + getBoundingClientRect for
 * every visible element, and emit rect/path/text/image. Everything therefore
 * lands in Corel as editable objects at the exact on-screen position.
 *
 * The walk is fully synchronous so the snapshot is consistent (no layout can
 * change mid-capture). Anything needing I/O — <img> data, inlined SVG assets —
 * appends a promise to ctx.deferred that patches its placeholder node later.
 */
(function () {
  'use strict';
  const NS = (window.FAVEExport = window.FAVEExport || {});
  const S = NS.svg;
  const col = NS.color;

  const SKIP_TAGS = new Set([
    'script', 'style', 'noscript', 'template', 'link', 'meta', 'title',
    'head', 'option', 'optgroup', 'datalist', 'br', 'source', 'track',
  ]);

  // Presentation properties copied onto cloned <svg> subtrees so the result
  // survives without our stylesheets.
  // fill / stroke and every *-opacity are handled separately in copySvgStyles,
  // because their alpha has to be folded into the element's own `opacity`.
  const SVG_PROPS = [
    'fill-rule', 'stroke-width', 'stroke-linecap', 'stroke-linejoin',
    'stroke-dasharray', 'stroke-dashoffset', 'stroke-miterlimit',
    'paint-order', 'visibility',
  ];

  // Only meaningful on text; copying them onto every path just bloats the file.
  const SVG_TEXT_PROPS = [
    'font-family', 'font-size', 'font-weight', 'font-style',
    'text-anchor', 'letter-spacing',
  ];

  const TEXTISH = new Set(['text', 'tspan', 'textPath']);

  // Values that match the SVG default and can be left out entirely.
  const SVG_DEFAULTS = {
    'fill-opacity': '1', 'stroke-opacity': '1', 'opacity': '1',
    'stroke-dasharray': 'none', 'stroke-dashoffset': '0px',
    'stroke-linecap': 'butt', 'stroke-linejoin': 'miter',
    'stroke-miterlimit': '4', 'fill-rule': 'nonzero',
    'paint-order': 'normal', 'visibility': 'visible',
    'font-style': 'normal', 'letter-spacing': 'normal',
    'text-anchor': 'start',
  };

  const measureCvs = document.createElement('canvas').getContext('2d');

  /* ---------------------------------------------------------------- utils */

  function fontShorthand(cs) {
    const fs = parseFloat(cs.fontSize) || 12;
    return (cs.fontStyle || 'normal') + ' ' + (cs.fontWeight || 400) + ' ' +
           fs + 'px ' + (cs.fontFamily || 'sans-serif');
  }

  /** Distance from a range rect's top to the text baseline. */
  function ascentFor(cs, boxHeight) {
    measureCvs.font = fontShorthand(cs);
    const m = measureCvs.measureText('Hxg');
    const asc = m.fontBoundingBoxAscent, desc = m.fontBoundingBoxDescent;
    if (!isFinite(asc) || !isFinite(desc)) return (parseFloat(cs.fontSize) || 12) * 0.8;
    return (boxHeight - (asc + desc)) / 2 + asc;
  }

  function measureTextWidth(cs, text) {
    measureCvs.font = fontShorthand(cs);
    return measureCvs.measureText(text).width;
  }

  function applyTextTransform(text, tt) {
    if (tt === 'uppercase') return text.toUpperCase();
    if (tt === 'lowercase') return text.toLowerCase();
    if (tt === 'capitalize') return text.replace(/\b\p{L}/gu, function (c) { return c.toUpperCase(); });
    return text;
  }

  function zOf(node) {
    if (node.nodeType !== 1) return 0;
    const z = parseInt(getComputedStyle(node).zIndex, 10);
    return isFinite(z) ? z : 0;
  }

  /** Child nodes in paint order: DOM order, stably re-sorted by z-index. */
  function childNodesOrdered(el) {
    const list = Array.prototype.map.call(el.childNodes, function (node, i) {
      return { node: node, i: i, z: zOf(node) };
    });
    list.sort(function (a, b) { return (a.z - b.z) || (a.i - b.i); });
    return list.map(function (x) { return x.node; });
  }

  /* ------------------------------------------------- clipping (by culling) */

  function intersects(r, clip) {
    return r.right > clip.left && r.left < clip.right &&
           r.bottom > clip.top && r.top < clip.bottom;
  }

  function intersectClip(a, b) {
    return {
      left: Math.max(a.left, b.left), top: Math.max(a.top, b.top),
      right: Math.min(a.right, b.right), bottom: Math.min(a.bottom, b.bottom),
    };
  }

  /* -------------------------------------------------------------- opacity */

  /**
   * CorelDraw drops fill-opacity / stroke-opacity on import but does honour
   * `opacity`, so alpha has to ride on the element itself wherever possible.
   * Multiplies into any opacity already set rather than overwriting it.
   */
  function multiplyOpacity(node, a) {
    if (!(a < 1)) return;
    const prev = parseFloat(node.getAttribute('opacity'));
    node.setAttribute('opacity', S.n((isFinite(prev) ? prev : 1) * a));
  }

  /**
   * Fold paint alpha into `opacity` when the element has a single paint, or
   * when fill and stroke share one alpha. Only genuinely mixed alphas fall
   * back to the per-paint attributes Corel ignores.
   */
  function setPaintAlpha(node, fillAlpha, strokeAlpha) {
    const hasFill = fillAlpha !== null && fillAlpha !== undefined;
    const hasStroke = strokeAlpha !== null && strokeAlpha !== undefined;

    if (hasFill && hasStroke) {
      if (Math.abs(fillAlpha - strokeAlpha) < 0.005) {
        multiplyOpacity(node, fillAlpha);
        return;
      }
      if (fillAlpha < 1) node.setAttribute('fill-opacity', S.n(fillAlpha));
      if (strokeAlpha < 1) node.setAttribute('stroke-opacity', S.n(strokeAlpha));
      return;
    }
    multiplyOpacity(node, hasFill ? fillAlpha : (hasStroke ? strokeAlpha : 1));
  }

  /**
   * Last resort for shapes whose fill and stroke carry different alphas: split
   * them into a fill-only and a stroke-only copy, each with its alpha on
   * `opacity`. After this pass nothing in the file relies on fill-opacity or
   * stroke-opacity, which CorelDraw ignores.
   */
  function splitMixedAlpha(root) {
    root.querySelectorAll('[fill-opacity],[stroke-opacity]').forEach(function (el) {
      const fo = parseFloat(el.getAttribute('fill-opacity'));
      const so = parseFloat(el.getAttribute('stroke-opacity'));
      const fill = el.getAttribute('fill');
      const stroke = el.getAttribute('stroke');
      const paintedFill = fill && fill !== 'none';
      const paintedStroke = stroke && stroke !== 'none';

      if (!paintedFill || !paintedStroke || !el.parentNode) {
        // Single paint — fold whichever alpha applies and drop the attributes.
        multiplyOpacity(el, paintedStroke && isFinite(so) ? so : (isFinite(fo) ? fo : 1));
        el.removeAttribute('fill-opacity');
        el.removeAttribute('stroke-opacity');
        return;
      }

      const strokeCopy = el.cloneNode(true);
      strokeCopy.removeAttribute('id');

      el.setAttribute('stroke', 'none');
      el.removeAttribute('fill-opacity');
      el.removeAttribute('stroke-opacity');
      multiplyOpacity(el, isFinite(fo) ? fo : 1);

      strokeCopy.setAttribute('fill', 'none');
      strokeCopy.removeAttribute('fill-opacity');
      strokeCopy.removeAttribute('stroke-opacity');
      multiplyOpacity(strokeCopy, isFinite(so) ? so : 1);

      el.parentNode.insertBefore(strokeCopy, el.nextSibling);
    });
  }

  /** Hoist children out of <g> wrappers that carry nothing, to cut nesting. */
  function flattenGroups(root) {
    root.querySelectorAll('g').forEach(function (g) {
      // Empty ones are left alone: they are placeholders whose content is
      // still being fetched and would otherwise be hoisted out of the tree.
      if (g.attributes.length === 0 && g.childNodes.length > 0 && g.parentNode) {
        while (g.firstChild) g.parentNode.insertBefore(g.firstChild, g);
        g.remove();
      }
    });
  }

  /** Clips become PowerClips in CorelDraw, which read as locked objects. */
  function stripClips(root) {
    root.querySelectorAll('clipPath,mask').forEach(function (n) { n.remove(); });
    root.querySelectorAll('[clip-path],[mask],[clip-rule]').forEach(function (n) {
      n.removeAttribute('clip-path');
      n.removeAttribute('mask');
      n.removeAttribute('clip-rule');
    });
  }

  /** Rewrite ids inside a cloned subtree so multiple clones cannot collide. */
  function uniquifyIds(root, prefix) {
    const map = new Map();
    root.querySelectorAll('[id]').forEach(function (n) {
      const old = n.getAttribute('id');
      map.set(old, prefix + old);
      n.setAttribute('id', prefix + old);
    });
    if (!map.size) return;
    const REFS = ['clip-path', 'mask', 'fill', 'stroke', 'filter',
                  'marker-start', 'marker-mid', 'marker-end'];
    root.querySelectorAll('*').forEach(function (n) {
      REFS.forEach(function (attr) {
        const v = n.getAttribute(attr);
        if (!v || v.indexOf('url(#') === -1) return;
        n.setAttribute(attr, v.replace(/url\(#([^)]+)\)/g, function (m0, id) {
          return 'url(#' + (map.get(id) || id) + ')';
        }));
      });
      const xh = n.getAttributeNS('http://www.w3.org/1999/xlink', 'href') || n.getAttribute('href');
      if (xh && xh.charAt(0) === '#' && map.has(xh.slice(1))) {
        n.setAttribute('href', '#' + map.get(xh.slice(1)));
      }
    });
  }

  /* ------------------------------------------------------------ box paint */

  function paintBackground(node, cs, r, out, ctx) {
    const radii = S.radiiOf(cs);
    const bg = col(cs.backgroundColor);
    if (bg.alpha > 0.001) {
      const shape = S.shapeNode(r.left, r.top, r.width, r.height, radii);
      shape.setAttribute('fill', bg.hex);
      multiplyOpacity(shape, bg.alpha);
      out.appendChild(shape);
    }

    const bi = cs.backgroundImage;
    if (!bi || bi === 'none') return;

    const grad = NS.linearGradient(bi, r);
    if (grad) {
      const id = 'faveGrad' + (++ctx.idc);
      const lg = S.el('linearGradient', {
        id: id, gradientUnits: 'userSpaceOnUse',
        x1: grad.x1, y1: grad.y1, x2: grad.x2, y2: grad.y2,
      });
      grad.stops.forEach(function (s) {
        lg.appendChild(S.el('stop', {
          offset: S.n(s.offset),
          'stop-color': s.hex,
          'stop-opacity': s.alpha < 1 ? S.n(s.alpha) : null,
        }));
      });
      ctx.defs.appendChild(lg);
      const shape = S.shapeNode(r.left, r.top, r.width, r.height, radii);
      shape.setAttribute('fill', 'url(#' + id + ')');
      out.appendChild(shape);
      return;
    }

    const um = /url\((['"]?)([^'")]+)\1\)/.exec(bi);
    if (um) {
      const holder = S.group({});
      out.appendChild(holder);
      ctx.deferred.push(NS.resolveAsset(um[2], holder, r, cs, ctx));
    }
  }

  function paintBorders(node, cs, r, out) {
    const sides = [
      { w: parseFloat(cs.borderTopWidth) || 0, c: cs.borderTopColor, s: cs.borderTopStyle },
      { w: parseFloat(cs.borderRightWidth) || 0, c: cs.borderRightColor, s: cs.borderRightStyle },
      { w: parseFloat(cs.borderBottomWidth) || 0, c: cs.borderBottomColor, s: cs.borderBottomStyle },
      { w: parseFloat(cs.borderLeftWidth) || 0, c: cs.borderLeftColor, s: cs.borderLeftStyle },
    ];
    const live = sides.filter(function (s) {
      return s.w > 0 && s.s !== 'none' && s.s !== 'hidden' && col(s.c).alpha > 0.001;
    });
    if (!live.length) return;

    function dashFor(style, w) {
      if (style === 'dashed') return S.n(w * 3) + ',' + S.n(w * 2);
      if (style === 'dotted') return S.n(w) + ',' + S.n(w * 2);
      return null;
    }

    const uniform = live.length === 4 && sides.every(function (s) {
      return s.w === sides[0].w && s.c === sides[0].c && s.s === sides[0].s;
    });

    if (uniform) {
      const w = sides[0].w, c = col(sides[0].c), h = w / 2;
      const radii = S.radiiOf(cs).map(function (v) { return Math.max(0, v - h); });
      const shape = S.shapeNode(r.left + h, r.top + h,
        Math.max(0, r.width - w), Math.max(0, r.height - w), radii);
      shape.setAttribute('fill', 'none');
      shape.setAttribute('stroke', c.hex);
      shape.setAttribute('stroke-width', S.n(w));
      multiplyOpacity(shape, c.alpha);
      const d = dashFor(sides[0].s, w);
      if (d) shape.setAttribute('stroke-dasharray', d);
      out.appendChild(shape);
      return;
    }

    // Mixed borders: one line per side, centred on the border-box edge.
    const edges = [
      [r.left, r.top + sides[0].w / 2, r.right, r.top + sides[0].w / 2],
      [r.right - sides[1].w / 2, r.top, r.right - sides[1].w / 2, r.bottom],
      [r.left, r.bottom - sides[2].w / 2, r.right, r.bottom - sides[2].w / 2],
      [r.left + sides[3].w / 2, r.top, r.left + sides[3].w / 2, r.bottom],
    ];
    sides.forEach(function (s, i) {
      if (s.w <= 0 || s.s === 'none' || s.s === 'hidden') return;
      const c = col(s.c);
      if (c.alpha <= 0.001) return;
      const e = edges[i];
      const line = S.el('line', {
        x1: e[0], y1: e[1], x2: e[2], y2: e[3],
        stroke: c.hex, 'stroke-width': s.w,
      });
      multiplyOpacity(line, c.alpha);
      const d = dashFor(s.s, s.w);
      if (d) line.setAttribute('stroke-dasharray', d);
      out.appendChild(line);
    });
  }

  /* ----------------------------------------------------------- text paint */

  /**
   * Split a text node into rendered lines using per-character client rects.
   * This mirrors the browser's own wrapping and white-space collapsing, so
   * wrapped labels land exactly where they appear on screen.
   */
  function collapseWs(text, ws) {
    if (ws === 'pre' || ws === 'pre-wrap' || ws === 'break-spaces') return text;
    return text.replace(/\s+/g, ' ');
  }

  function measureLines(node, cs) {
    const raw = node.nodeValue || '';
    if (!raw.trim()) return [];

    // Range over exactly the non-whitespace-bounded run, so the rect starts at
    // the first painted glyph.
    const s0 = raw.length - raw.replace(/^\s+/, '').length;
    const s1 = raw.replace(/\s+$/, '').length;
    if (s1 <= s0) return [];

    const range = document.createRange();
    let rects = null;
    try {
      range.setStart(node, s0);
      range.setEnd(node, s1);
      rects = range.getClientRects();
    } catch (e) { return []; }
    if (!rects || rects.length === 0) return [];

    // Fast path — the overwhelming majority of UI labels sit on one line, and
    // per-character ranges force a reflow each, which is ruinous on this DOM.
    if (rects.length === 1) {
      const rc = rects[0];
      const text = collapseWs(raw.slice(s0, s1), cs.whiteSpace);
      return text ? [{ text: text, left: rc.left, top: rc.top, bottom: rc.bottom }] : [];
    }

    // Wrapped text: group characters into lines by their rect top.
    const lines = [];
    let cur = null;
    for (let i = s0; i < s1; i++) {
      let rc = null;
      try {
        range.setStart(node, i);
        range.setEnd(node, i + 1);
        rc = range.getClientRects()[0] || null;
      } catch (e) { rc = null; }
      const ch = raw.charAt(i);
      if (!rc) continue;
      if (!ch.trim() && rc.width === 0) continue;               // collapsed space
      if (rc.width === 0 && rc.height === 0) continue;
      if (!cur && !ch.trim()) continue;                          // leading space
      const key = Math.round(rc.top * 2);
      if (!cur || cur.key !== key) {
        cur = { key: key, text: '', left: rc.left, top: rc.top, bottom: rc.bottom };
        lines.push(cur);
      }
      cur.text += ch;
      cur.top = Math.min(cur.top, rc.top);
      cur.bottom = Math.max(cur.bottom, rc.bottom);
    }

    return lines.map(function (l) {
      return {
        text: collapseWs(l.text, cs.whiteSpace).replace(/\s+$/, ''),
        left: l.left, top: l.top, bottom: l.bottom,
      };
    }).filter(function (l) { return l.text; });
  }

  function textAttrs(cs) {
    const c = col(cs.color);
    const a = {
      fill: c.hex,
      'font-family': cs.fontFamily,
      'font-size': S.n(parseFloat(cs.fontSize) || 12),
      'font-weight': cs.fontWeight,
      'xml:space': 'preserve',
    };
    if (cs.fontStyle && cs.fontStyle !== 'normal') a['font-style'] = cs.fontStyle;
    const ls = parseFloat(cs.letterSpacing);
    if (isFinite(ls) && ls !== 0) a['letter-spacing'] = S.n(ls);
    return a;
  }

  function paintTextNode(node, cs, out, ctx, clip) {
    const lines = measureLines(node, cs);
    if (!lines.length) return;
    const attrs = textAttrs(cs);
    const inkAlpha = col(cs.color).alpha;
    const tt = cs.textTransform;
    const deco = cs.textDecorationLine || cs.textDecoration || '';
    const underline = deco.indexOf('underline') !== -1;
    const strike = deco.indexOf('line-through') !== -1;
    const fs = parseFloat(cs.fontSize) || 12;

    lines.forEach(function (ln) {
      // A line scrolled out of a clipping ancestor is dropped by its centre,
      // so half-visible rows do not survive as stray text.
      const mid = (ln.top + ln.bottom) / 2;
      if (mid < clip.top || mid > clip.bottom) return;
      if (ln.left > clip.right) return;

      const baseline = ln.top + ascentFor(cs, ln.bottom - ln.top);
      const label = applyTextTransform(ln.text, tt);
      const t = S.el('text', Object.assign({}, attrs, { x: ln.left, y: baseline }));
      t.textContent = label;
      multiplyOpacity(t, inkAlpha);
      out.appendChild(t);

      if (underline || strike) {
        const w = measureTextWidth(cs, label);
        const lc = col(cs.textDecorationColor || cs.color);
        const yy = underline ? baseline + fs * 0.12 : baseline - fs * 0.28;
        const dl = S.el('line', {
          x1: ln.left, y1: yy, x2: ln.left + w, y2: yy,
          stroke: lc.hex, 'stroke-width': Math.max(0.75, fs / 14),
        });
        multiplyOpacity(dl, lc.alpha);
        out.appendChild(dl);
      }
    });
  }

  /* ------------------------------------------------------- inline <svg>'s */

  /** Copy resolved presentation props from a live SVG tree onto its clone. */
  function copySvgStyles(src, clone) {
    const a = [src], b = [clone];
    while (a.length) {
      const sn = a.pop(), cn = b.pop();
      if (sn.nodeType === 1 && cn.nodeType === 1) {
        const cs = getComputedStyle(sn);
        const tag = (cn.tagName || '').toLowerCase();

        // Paints first: their alpha (from rgba() colours and from
        // fill-opacity/stroke-opacity alike) has to end up on `opacity`.
        let fillAlpha = null, strokeAlpha = null;
        ['fill', 'stroke'].forEach(function (p) {
          const v = cs.getPropertyValue(p);
          if (!v) return;
          if (v === 'none') { cn.setAttribute(p, 'none'); return; }
          if (v.indexOf('url(') === 0) { cn.setAttribute(p, v); return; } // gradient ref
          const c = col(v);
          if (c.alpha <= 0.001) { cn.setAttribute(p, 'none'); return; }
          cn.setAttribute(p, c.hex);
          const extra = parseFloat(cs.getPropertyValue(p + '-opacity'));
          const a = c.alpha * (isFinite(extra) ? extra : 1);
          if (p === 'fill') fillAlpha = a; else strokeAlpha = a;
        });

        const eo = parseFloat(cs.getPropertyValue('opacity'));
        if (isFinite(eo) && eo < 1) cn.setAttribute('opacity', S.n(eo));

        // cloneNode kept the source's own fill-opacity/stroke-opacity
        // attributes. Their alpha is already folded into `opacity` above, so
        // leaving them would apply it twice.
        cn.removeAttribute('fill-opacity');
        cn.removeAttribute('stroke-opacity');
        setPaintAlpha(cn, fillAlpha, strokeAlpha);

        const props = TEXTISH.has(tag) ? SVG_PROPS.concat(SVG_TEXT_PROPS) : SVG_PROPS;
        props.forEach(function (p) {
          let v = cs.getPropertyValue(p);
          if (!v) return;
          if (SVG_DEFAULTS[p] === v) return;
          if (p === 'font-size') v = S.n(parseFloat(v) || 12);
          cn.setAttribute(p, v);
        });
        cn.removeAttribute('class');
        cn.removeAttribute('style');

        // CorelDraw ignores dominant-baseline; fold it into dy instead.
        const db = cs.getPropertyValue('dominant-baseline');
        if (cn.tagName && cn.tagName.toLowerCase() === 'text' &&
            db && db !== 'auto' && db !== 'normal') {
          const fs = parseFloat(cs.fontSize) || 12;
          let shift = 0;
          if (/middle|central/.test(db)) shift = fs * 0.32;
          else if (/hanging|text-before-edge/.test(db)) shift = fs * 0.8;
          else if (/text-after-edge|ideographic/.test(db)) shift = -fs * 0.2;
          const prev = cn.getAttribute('dy');
          let prevPx = 0;
          if (prev) prevPx = /em$/.test(prev) ? parseFloat(prev) * fs : (parseFloat(prev) || 0);
          cn.setAttribute('dy', S.n(prevPx + shift));
          cn.removeAttribute('dominant-baseline');
          cn.removeAttribute('alignment-baseline');
        }
      }
      for (let i = 0; i < sn.childNodes.length; i++) {
        if (!cn.childNodes[i]) break;
        a.push(sn.childNodes[i]);
        b.push(cn.childNodes[i]);
      }
    }
  }

  /**
   * <image> inside a cloned chart (the POI symbols on the parallel-coords
   * axes, for one) points at a relative URL that means nothing once the file
   * is opened elsewhere. Swap each for its real content — .svg sources come
   * back as true vectors, everything else as an embedded data URI.
   * Coordinates stay in the clone's own user units, since the holder sits
   * inside the same transform.
   */
  function embedSvgImages(clone, ctx) {
    clone.querySelectorAll('image').forEach(function (img) {
      const url = img.getAttribute('href') ||
                  img.getAttributeNS('http://www.w3.org/1999/xlink', 'href');
      if (!url || url.indexOf('data:') === 0) return;

      const local = {
        left: parseFloat(img.getAttribute('x')) || 0,
        top: parseFloat(img.getAttribute('y')) || 0,
        width: parseFloat(img.getAttribute('width')) || 0,
        height: parseFloat(img.getAttribute('height')) || 0,
      };
      if (!(local.width > 0 && local.height > 0)) return;

      const holder = S.group({});
      img.parentNode.replaceChild(holder, img);
      ctx.deferred.push(NS.resolveAsset(url, holder, local, null, ctx));
    });
  }

  function inlineSvgElement(srcSvg, r, out, ctx) {
    let clone;
    try { clone = srcSvg.cloneNode(true); } catch (e) { return; }
    copySvgStyles(srcSvg, clone);
    clone.querySelectorAll('script,style,foreignObject').forEach(function (n) { n.remove(); });
    stripClips(clone);
    flattenGroups(clone);
    embedSvgImages(clone, ctx);
    uniquifyIds(clone, 'e' + (++ctx.idc) + '_');

    let sx = 1, tx = r.left, ty = r.top;
    const vb = srcSvg.viewBox && srcSvg.viewBox.baseVal;
    if (vb && vb.width > 0 && vb.height > 0) {
      sx = Math.min(r.width / vb.width, r.height / vb.height);
      tx = r.left + (r.width - vb.width * sx) / 2 - vb.x * sx;
      ty = r.top + (r.height - vb.height * sx) / 2 - vb.y * sx;
    }

    const g = S.group({
      transform: 'translate(' + S.n(tx) + ',' + S.n(ty) + ') scale(' + S.n(sx) + ')',
    });
    while (clone.firstChild) g.appendChild(clone.firstChild);
    out.appendChild(g);
  }

  /* ----------------------------------------------------- media + controls */

  function paintCanvas(cv, r, out, ctx) {
    const url = NS.captureCanvas(cv);
    if (!url) return;
    const img = S.el('image', {
      x: r.left, y: r.top, width: r.width, height: r.height,
      preserveAspectRatio: 'none',
    });
    S.setHref(img, url);
    out.appendChild(img);
    ctx.rasterCount++;
  }

  function paintImg(imgEl, r, out, ctx) {
    const holder = S.group({});
    out.appendChild(holder);
    const cs = getComputedStyle(imgEl);
    ctx.deferred.push(NS.resolveAsset(imgEl.currentSrc || imgEl.src, holder, r, cs, ctx));
  }

  function paintFormControl(el, cs, r, out, ctx) {
    const tag = el.tagName.toLowerCase();
    const type = (el.type || '').toLowerCase();
    const accent = col(cs.accentColor && cs.accentColor !== 'auto' ? cs.accentColor : '#3A6EA5');

    if (tag === 'input' && (type === 'checkbox' || type === 'radio')) {
      const rad = type === 'radio' ? r.width / 2 : 2;
      const box = S.shapeNode(r.left, r.top, r.width, r.height, [rad, rad, rad, rad]);
      box.setAttribute('fill', el.checked ? accent.hex : '#ffffff');
      box.setAttribute('stroke', el.checked ? accent.hex : '#9aa0a6');
      box.setAttribute('stroke-width', 1);
      out.appendChild(box);
      if (el.checked && type === 'radio') {
        out.appendChild(S.el('circle', {
          cx: r.left + r.width / 2, cy: r.top + r.height / 2,
          r: Math.max(1.5, r.width * 0.22), fill: '#ffffff',
        }));
      } else if (el.checked) {
        const d = 'M' + S.n(r.left + r.width * 0.24) + ',' + S.n(r.top + r.height * 0.52) +
                  ' L' + S.n(r.left + r.width * 0.44) + ',' + S.n(r.top + r.height * 0.72) +
                  ' L' + S.n(r.left + r.width * 0.78) + ',' + S.n(r.top + r.height * 0.3);
        out.appendChild(S.el('path', {
          d: d, fill: 'none', stroke: '#ffffff',
          'stroke-width': Math.max(1.2, r.width * 0.14),
          'stroke-linecap': 'round', 'stroke-linejoin': 'round',
        }));
      }
      return;
    }

    if (tag === 'input' && type === 'range') {
      const min = parseFloat(el.min || '0'), max = parseFloat(el.max || '100');
      const val = parseFloat(el.value || '0');
      const frac = max > min ? Math.min(1, Math.max(0, (val - min) / (max - min))) : 0;
      const cy = r.top + r.height / 2, th = Math.max(3, r.height * 0.22);
      out.appendChild(S.el('rect', {
        x: r.left, y: cy - th / 2, width: r.width, height: th, rx: th / 2, fill: '#d7d7d7',
      }));
      if (frac > 0) {
        out.appendChild(S.el('rect', {
          x: r.left, y: cy - th / 2, width: r.width * frac, height: th,
          rx: th / 2, fill: accent.hex,
        }));
      }
      out.appendChild(S.el('circle', {
        cx: r.left + r.width * frac, cy: cy, r: Math.max(4, r.height * 0.34),
        fill: '#ffffff', stroke: accent.hex, 'stroke-width': 1.5,
      }));
      return;
    }

    // Text-ish controls and <select>: synthesise the visible value.
    let text = '';
    if (tag === 'select') {
      const o = el.options && el.options[el.selectedIndex];
      text = o ? o.text : '';
    } else if (tag === 'input' || tag === 'textarea') {
      text = el.value || el.placeholder || '';
    }
    if (!text) return;

    const fs = parseFloat(cs.fontSize) || 12;
    const attrs = textAttrs(cs);
    const isPlaceholder = !el.value && !!el.placeholder;

    let x = r.left + (parseFloat(cs.borderLeftWidth) || 0) + (parseFloat(cs.paddingLeft) || 0);
    if (cs.textAlign === 'center') {
      x = r.left + r.width / 2;
      attrs['text-anchor'] = 'middle';
    } else if (cs.textAlign === 'right' || cs.textAlign === 'end') {
      x = r.right - (parseFloat(cs.paddingRight) || 0) - (parseFloat(cs.borderRightWidth) || 0);
      attrs['text-anchor'] = 'end';
    }

    const single = tag !== 'textarea';
    const y = single
      ? r.top + r.height / 2 + fs * 0.34
      : r.top + (parseFloat(cs.borderTopWidth) || 0) + (parseFloat(cs.paddingTop) || 0) + fs * 0.85;

    const t = S.el('text', Object.assign({}, attrs, { x: x, y: y }));
    t.textContent = text.split('\n')[0];
    multiplyOpacity(t, col(cs.color).alpha * (isPlaceholder ? 0.55 : 1));
    out.appendChild(t);

    if (tag === 'select') {
      const cx = r.right - 12, cy = r.top + r.height / 2;
      out.appendChild(S.el('path', {
        d: 'M' + S.n(cx - 4) + ',' + S.n(cy - 2) + ' L' + S.n(cx) + ',' + S.n(cy + 2.5) +
           ' L' + S.n(cx + 4) + ',' + S.n(cy - 2),
        fill: 'none', stroke: col(cs.color).hex, 'stroke-width': 1.3,
        'stroke-linecap': 'round', 'stroke-linejoin': 'round',
      }));
    }
  }

  /* ------------------------------------------------------------ main walk */

  function walk(node, out, ctx, clip) {
    if (node.nodeType !== 1) return;

    const tag = node.tagName.toLowerCase();
    if (SKIP_TAGS.has(tag)) return;
    if (node.hasAttribute('data-export-ignore')) return;
    if (ctx.ignore.has(node)) return;

    const cs = getComputedStyle(node);
    if (cs.display === 'none' || cs.visibility === 'hidden') return;
    const opacity = parseFloat(cs.opacity);
    if (opacity === 0) return;

    const r = node.getBoundingClientRect();
    const boxed = r.width > 0 && r.height > 0;
    if (!boxed && node.childElementCount === 0) return;
    if (boxed && !intersects(r, clip)) return;

    let host = out;
    if (opacity < 1) {
      host = S.group({ opacity: S.n(opacity) });
      out.appendChild(host);
    }

    if (boxed) {
      paintBackground(node, cs, r, host, ctx);
      paintBorders(node, cs, r, host);
    }

    if (tag === 'svg') { inlineSvgElement(node, r, host, ctx); return; }
    if (tag === 'canvas') { paintCanvas(node, r, host, ctx); return; }
    if (tag === 'img') { paintImg(node, r, host, ctx); return; }
    if (tag === 'input' || tag === 'select' || tag === 'textarea') {
      paintFormControl(node, cs, r, host, ctx);
      return;
    }

    // Overflow is honoured by culling what falls outside, never by <clipPath>:
    // CorelDraw imports every clip as a PowerClip, whose contents cannot be
    // selected or moved without unlocking them one at a time.
    let childClip = clip;
    if (boxed && /hidden|scroll|auto|clip/.test(cs.overflowX + ' ' + cs.overflowY)) {
      childClip = intersectClip(clip, r);
    }

    childNodesOrdered(node).forEach(function (child) {
      if (child.nodeType === 3) paintTextNode(child, cs, host, ctx, childClip);
      else walk(child, host, ctx, childClip);
    });
  }

  /**
   * Snapshot the current viewport.
   * Returns { root, deferred, rasterCount }. Await `deferred` before
   * serialising so image hrefs are filled in.
   */
  function captureViewport(options) {
    const opts = options || {};
    const W = window.innerWidth, H = window.innerHeight;
    const root = S.svgRoot(W, H);

    const title = S.el('title');
    title.textContent = opts.title || 'FAVE interface export';
    root.appendChild(title);

    const defs = S.el('defs');
    root.appendChild(defs);

    const ctx = {
      W: W, H: H, defs: defs, idc: 0,
      deferred: [], rasterCount: 0,
      ignore: opts.ignore || new Set(),
    };

    // Page ground: <html> then <body> background.
    [document.documentElement, document.body].forEach(function (n) {
      const c = col(getComputedStyle(n).backgroundColor);
      if (c.alpha > 0.001) {
        const ground = S.el('rect', { x: 0, y: 0, width: W, height: H, fill: c.hex });
        multiplyOpacity(ground, c.alpha);
        root.appendChild(ground);
      }
    });

    const layer = S.group({ id: 'fave-interface' });
    root.appendChild(layer);

    const viewport = { left: 0, top: 0, right: W, bottom: H };
    const bodyCs = getComputedStyle(document.body);
    childNodesOrdered(document.body).forEach(function (child) {
      if (child.nodeType === 3) paintTextNode(child, bodyCs, layer, ctx, viewport);
      else walk(child, layer, ctx, viewport);
    });

    flattenGroups(layer);
    splitMixedAlpha(layer);
    if (!defs.childNodes.length) defs.remove();

    return { root: root, deferred: ctx.deferred, rasterCount: ctx.rasterCount };
  }

  NS.captureViewport = captureViewport;
  NS.inlineSvgElement = inlineSvgElement;
  NS.copySvgStyles = copySvgStyles;
  NS.uniquifyIds = uniquifyIds;
  NS.stripClips = stripClips;
  NS.flattenGroups = flattenGroups;
})();
