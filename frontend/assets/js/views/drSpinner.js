// DR Explorer mini-spinner (the small blue spinner inside the DR offcanvas).
// Extracted from main.js. Loaded with drView.js since it serves the DR UI.

/* ===================================================================== */
/* ===================== DR OFFCANVAS: PCA / UMAP, plotting, lasso ===== */
/* ===== Tiny blue spinner (top-right of DR area) ===== */
function ensureDRSpinnerStyles() {
  if (document.getElementById('drSpinnerCSS')) return;
  const st = document.createElement('style');
  st.id = 'drSpinnerCSS';
  st.textContent = `
    .dr-mini-spinner{
      position:absolute; top:50%; right:50%; z-index:20;
      width:1.25rem; height:1.25rem;
    }
  `;
  document.head.appendChild(st);
}

function ensureDRSpinner() {
  ensureDRSpinnerStyles();
  const wrap = document.getElementById('drCanvasWrap');
  if (!wrap) return;
  const pos = getComputedStyle(wrap).position;
  if (!pos || pos === 'static') wrap.style.position = 'relative';

  if (!document.getElementById('drGlobalSpinner')) {
    const sp = document.createElement('div');
    sp.id = 'drGlobalSpinner';
    sp.className = 'spinner-border text-primary dr-mini-spinner d-none';
    sp.setAttribute('role','status');
    sp.setAttribute('aria-hidden','true');
    sp.title = '';
    wrap.appendChild(sp);
  }
}

function showDRSpinner(statusMsg = '') {
  ensureDRSpinner();
  const sp = document.getElementById('drGlobalSpinner');
  const runBtn = document.getElementById('drRunBtn');
  const statusEl = document.getElementById('drStatus');
  if (sp) sp.classList.remove('d-none');
  if (runBtn) runBtn.disabled = true;
  if (statusEl) statusEl.textContent = statusMsg;
}

function hideDRSpinner(doneMsg = '') {
  const sp = document.getElementById('drGlobalSpinner');
  const runBtn = document.getElementById('drRunBtn');
  const statusEl = document.getElementById('drStatus');
  if (sp) sp.classList.add('d-none');
  if (runBtn) runBtn.disabled = false;
  if (statusEl) statusEl.textContent = doneMsg;
}

// DR Explorer (Library detection, Canvas/overlay, PCA/UMAP, Lasso, Bindings,
// runDR, etc.) lives in assets/js/views/drView.js, loaded before main.js.
