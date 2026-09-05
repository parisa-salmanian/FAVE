/* FAVE vector export — SVG primitive builders.
 *
 * Deliberately conservative: only elements CorelDraw imports reliably
 * (g / rect / path / line / circle / text / tspan / image / clipPath /
 * linearGradient). No foreignObject, no filters, no CSS.
 */
(function () {
  'use strict';
  const NS = (window.FAVEExport = window.FAVEExport || {});
  const SVGNS = 'http://www.w3.org/2000/svg';
  const XLINKNS = 'http://www.w3.org/1999/xlink';

  /** Trim float noise so the file stays small and Corel-friendly. */
  function n(v) {
    if (!isFinite(v)) return 0;
    return Math.round(v * 100) / 100;
  }

  function el(name, attrs) {
    const node = document.createElementNS(SVGNS, name);
    if (attrs) for (const k in attrs) {
      const v = attrs[k];
      if (v === null || v === undefined || v === '') continue;
      node.setAttribute(k, typeof v === 'number' ? n(v) : v);
    }
    return node;
  }

  function group(attrs) { return el('g', attrs); }

  /**
   * Rounded-rectangle path with four independent corner radii.
   * radii = [topLeft, topRight, bottomRight, bottomLeft].
   */
  function roundedRectPath(x, y, w, h, radii) {
    const max = Math.min(w, h) / 2;
    const r = (radii || [0, 0, 0, 0]).map((v) => Math.max(0, Math.min(v || 0, max)));
    const [tl, tr, br, bl] = r;
    if (!tl && !tr && !br && !bl) {
      return `M${n(x)},${n(y)} H${n(x + w)} V${n(y + h)} H${n(x)} Z`;
    }
    return [
      `M${n(x + tl)},${n(y)}`,
      `H${n(x + w - tr)}`,
      tr ? `A${n(tr)},${n(tr)} 0 0 1 ${n(x + w)},${n(y + tr)}` : '',
      `V${n(y + h - br)}`,
      br ? `A${n(br)},${n(br)} 0 0 1 ${n(x + w - br)},${n(y + h)}` : '',
      `H${n(x + bl)}`,
      bl ? `A${n(bl)},${n(bl)} 0 0 1 ${n(x)},${n(y + h - bl)}` : '',
      `V${n(y + tl)}`,
      tl ? `A${n(tl)},${n(tl)} 0 0 1 ${n(x + tl)},${n(y)}` : '',
      'Z',
    ].filter(Boolean).join(' ');
  }

  /** Read the four computed border radii (px) off a computed style. */
  function radiiOf(cs) {
    const one = (v) => {
      if (!v) return 0;
      const f = parseFloat(String(v).split(/\s+/)[0]);
      return isFinite(f) ? f : 0;
    };
    return [
      one(cs.borderTopLeftRadius), one(cs.borderTopRightRadius),
      one(cs.borderBottomRightRadius), one(cs.borderBottomLeftRadius),
    ];
  }

  function shapeNode(x, y, w, h, radii) {
    const r = radii || [0, 0, 0, 0];
    if (!r[0] && !r[1] && !r[2] && !r[3]) {
      return el('rect', { x, y, width: w, height: h });
    }
    if (r[0] === r[1] && r[1] === r[2] && r[2] === r[3]) {
      return el('rect', { x, y, width: w, height: h, rx: r[0], ry: r[0] });
    }
    return el('path', { d: roundedRectPath(x, y, w, h, r) });
  }

  function setHref(node, url) {
    node.setAttribute('href', url);
    node.setAttributeNS(XLINKNS, 'xlink:href', url); // CorelDraw prefers xlink
  }

  function svgRoot(width, height) {
    const root = el('svg', {
      xmlns: SVGNS,
      'xmlns:xlink': XLINKNS,
      width: width + 'px',
      height: height + 'px',
      viewBox: `0 0 ${n(width)} ${n(height)}`,
      version: '1.1',
    });
    return root;
  }

  function serialize(root) {
    const xml = new XMLSerializer().serializeToString(root);
    return '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n' + xml;
  }

  NS.svg = { SVGNS, XLINKNS, n, el, group, roundedRectPath, radiiOf, shapeNode, setHref, svgRoot, serialize };
})();
