/* FAVE vector export — orchestration + download.
 *
 * Public entry point: FAVEExport.run().  Snapshots the viewport exactly as it
 * looks at click time (every open menu, popover, drawer and panel, at its real
 * position) into one self-contained SVG that CorelDraw imports directly.
 *
 * What stays vector: panels, menus, buttons, labels, all text, every icon, and
 * the d3 charts (DR Explorer, parallel coordinates, equity bars).
 * What is embedded as a raster: the MapLibre/deck.gl WebGL canvas — a GPU
 * frame has no vector geometry to recover.
 */
(function () {
  'use strict';
  const NS = (window.FAVEExport = window.FAVEExport || {});
  const S = NS.svg;

  let busy = false;

  /* --------------------------------------------------------------- toast */

  function toast(message, isError) {
    const old = document.getElementById('faveExportToast');
    if (old) old.remove();
    const box = document.createElement('div');
    box.id = 'faveExportToast';
    box.setAttribute('data-export-ignore', '1');
    box.textContent = message;
    box.style.cssText = [
      'position:fixed', 'z-index:99999', 'left:50%', 'bottom:28px',
      'transform:translateX(-50%)', 'max-width:min(560px,86vw)',
      'padding:10px 16px', 'border-radius:8px',
      'font:500 12.5px/1.45 Inter,system-ui,sans-serif',
      'box-shadow:0 6px 24px rgba(0,0,0,.22)', 'white-space:pre-wrap',
      'background:' + (isError ? '#7a2230' : '#1c1f23'), 'color:#f5f2ec',
    ].join(';');
    document.body.appendChild(box);
    setTimeout(function () { box.remove(); }, isError ? 9000 : 4500);
  }

  /* ------------------------------------------------------------ filename */

  // Built from an escape string so no raw combining marks live in this file.
  const COMBINING_MARKS = new RegExp('[\u0300-\u036f]', 'g');
  function slug(s) {
    return String(s || '').toLowerCase()
      .normalize('NFD').replace(COMBINING_MARKS, '')
      .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'view';
  }

  function currentCity() {
    const n = document.getElementById('topCityLabel');
    return n ? n.textContent.trim() : 'fave';
  }

  function stamp() {
    const d = new Date(), p = function (v) { return ('0' + v).slice(-2); };
    return d.getFullYear() + p(d.getMonth() + 1) + p(d.getDate()) + '-' +
           p(d.getHours()) + p(d.getMinutes()) + p(d.getSeconds());
  }

  function download(text, filename) {
    const blob = new Blob([text], { type: 'image/svg+xml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    setTimeout(function () { a.remove(); URL.revokeObjectURL(url); }, 1500);
  }

  /* ------------------------------------------------------------- capture */

  /** Elements that are on-screen furniture, not part of the interface. */
  function ignoreSet() {
    const ignore = new Set();
    ['.fave-tip', '#faveExportToast', '[data-export-ignore]'].forEach(function (sel) {
      document.querySelectorAll(sel).forEach(function (n) { ignore.add(n); });
    });
    return ignore;
  }

  function describe(root) {
    const bits = [
      'FAVE interface export',
      'city: ' + currentCity(),
      'theme: ' + (document.body.getAttribute('data-theme') || 'light'),
      'viewport: ' + window.innerWidth + '×' + window.innerHeight + ' px',
      'captured: ' + new Date().toISOString(),
    ];
    const m = NS.getMap && NS.getMap();
    if (m && m.getCenter) {
      try {
        const c = m.getCenter();
        bits.push('map: ' + c.lng.toFixed(5) + ',' + c.lat.toFixed(5) +
                  ' z' + m.getZoom().toFixed(2) +
                  ' pitch' + m.getPitch().toFixed(0) +
                  ' bearing' + m.getBearing().toFixed(0));
      } catch (e) { /* ignore */ }
    }
    const desc = S.el('desc');
    desc.textContent = bits.join('\n');
    root.insertBefore(desc, root.firstChild);
  }

  /**
   * Build the SVG for the current viewport.
   * `onSnapshotStart` fires immediately before the synchronous DOM walk, so
   * the caller can make sure no transient UI (a spinner on the export button)
   * is showing at the moment the snapshot is taken.
   */
  const ASSET_TIMEOUT_MS = 5000;

  /** Never let one stalled asset fetch hold the whole export hostage. */
  function settledWithin(promise, ms) {
    return Promise.race([
      Promise.resolve(promise).catch(function () { return null; }),
      new Promise(function (resolve) {
        setTimeout(function () {
          NS.exportWarn('An image took too long to embed and was left out.');
          resolve(null);
        }, ms);
      }),
    ]);
  }

  function build(onSnapshotStart) {
    return NS.prepare().then(function () {
      if (onSnapshotStart) onSnapshotStart();
      const snap = NS.captureViewport({
        ignore: ignoreSet(),
        title: 'FAVE — ' + currentCity(),
      });
      describe(snap.root);
      const settled = snap.deferred.map(function (p) {
        return settledWithin(p, ASSET_TIMEOUT_MS);
      });
      return Promise.all(settled).then(function () {
        return { xml: S.serialize(snap.root), rasterCount: snap.rasterCount };
      });
    });
  }

  /* ----------------------------------------------------------------- run */

  function run() {
    if (busy) return Promise.resolve();
    busy = true;
    NS.warnings.length = 0;

    const btn = document.getElementById('topExportBtn');
    const restore = btn ? btn.innerHTML : null;
    const setBusy = function () {
      if (btn) {
        btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" ' +
          'stroke="currentColor" stroke-width="2" stroke-linecap="round" style="display:block">' +
          '<path d="M12 3a9 9 0 1 0 9 9" opacity="0.9"/></svg>';
        btn.setAttribute('aria-busy', 'true');
      }
    };
    const unBusy = function () {
      if (btn && restore !== null) { btn.innerHTML = restore; btn.removeAttribute('aria-busy'); }
    };

    setBusy();

    return build(unBusy)
      .then(function (out) {
        setBusy();
        const name = 'fave-' + slug(currentCity()) + '-' + stamp() + '.svg';
        download(out.xml, name);
        const kb = Math.round(out.xml.length / 1024);
        let msg = 'Exported ' + name + '  ·  ' + kb.toLocaleString() + ' KB  ·  ' +
                  (out.rasterCount ? out.rasterCount + ' embedded raster layer' +
                    (out.rasterCount > 1 ? 's' : '') + ' (the map)' : 'fully vector');
        if (NS.warnings.length) msg += '\n\n' + NS.warnings.join('\n');
        toast(msg, NS.warnings.length > 0);
        return out;
      })
      .catch(function (err) {
        console.error('[FAVE export]', err);
        toast('Export failed: ' + (err && err.message ? err.message : err), true);
      })
      .then(function (out) {
        unBusy();
        busy = false;
        return out;
      });
  }

  NS.run = run;
  NS.buildSvg = build;
  NS.toast = toast;
})();
