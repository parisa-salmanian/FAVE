// DR feature-mode patch: policy-first vs legacy feature presets
// Loaded after main.js so we can safely wrap global functions.
(function () {
  const MODE_POLICY = 'policy';
  const MODE_LEGACY = 'legacy';

  // Contrastive keys used in policy mode (service-first but still informative).
  const POLICY_CONTRAST_KEYS = new Set([
    'overall',
    'fairGrocery',
    'fairHospital',
    'fairPrimary',
    'fairPharmacy',
    'fairHealthcare',
    'fairKindergarten',
    'heights',
    'years',
    'areaLog',
    'changeScore',
    'isChange'
  ]);

  // Label fragments retained for DR matrix / EBM in policy mode.
  const POLICY_LABEL_MATCHERS = [
    'overall fairness',
    'grocery fairness',
    'hospital fairness',
    'primary school fairness',
    'pharmacy fairness',
    'healthcare center fairness',
    'kindergarten fairness'
  ];

  function currentMode() {
    const sel = document.getElementById('drFeatureMode');
    const mode = sel?.value || globalThis.DR_FEATURE_MODE || MODE_POLICY;
    return (mode === MODE_LEGACY) ? MODE_LEGACY : MODE_POLICY;
  }

  function isPolicyLabel(label) {
    const s = String(label || '').toLowerCase();
    return POLICY_LABEL_MATCHERS.some((frag) => s.includes(frag));
  }

  function filterDRMatrixForPolicy(payload) {
    if (!payload || !Array.isArray(payload.X) || !Array.isArray(payload.featureLabels)) return payload;

    const keepIdx = [];
    payload.featureLabels.forEach((label, idx) => {
      if (isPolicyLabel(label)) keepIdx.push(idx);
    });

    // Fallback: never return an empty matrix.
    if (!keepIdx.length) return payload;

    const X2 = payload.X.map((row) => keepIdx.map((j) => row[j]));
    const labels2 = keepIdx.map((j) => payload.featureLabels[j]);

    return {
      ...payload,
      X: X2,
      featureLabels: labels2,
      dims: labels2.length
    };
  }

  function filterContrastiveForPolicy(diff) {
    if (!diff || !Array.isArray(diff.features)) return diff;
    return {
      ...diff,
      features: diff.features.filter((f) => POLICY_CONTRAST_KEYS.has(f.key))
    };
  }

  function installFunctionWrappers() {
    if (typeof globalThis.collectDRData === 'function' && !globalThis.__drModeCollectWrapped) {
      const originalCollectDRData = globalThis.collectDRData;
      globalThis.collectDRData = function wrappedCollectDRData(...args) {
        const out = originalCollectDRData.apply(this, args);
        return currentMode() === MODE_POLICY ? filterDRMatrixForPolicy(out) : out;
      };
      globalThis.__drModeCollectWrapped = true;
    }

    if (typeof globalThis.computeFeatureDifferences === 'function' && !globalThis.__drModeContrastWrapped) {
      const originalComputeFeatureDifferences = globalThis.computeFeatureDifferences;
      globalThis.computeFeatureDifferences = function wrappedComputeFeatureDifferences(...args) {
        const out = originalComputeFeatureDifferences.apply(this, args);
        return currentMode() === MODE_POLICY ? filterContrastiveForPolicy(out) : out;
      };
      globalThis.__drModeContrastWrapped = true;
    }
  }

  function refreshHints() {
    const infoEl = document.getElementById('drSelectInfo');
    if (!infoEl) return;

    const mode = currentMode();
    if (mode === MODE_POLICY) {
      infoEl.textContent = 'Feature mode: Policy (service-first for UMAP/EBM/contrastive).';
    } else {
      infoEl.textContent = 'Feature mode: Legacy (original full feature set).';
    }
  }

  async function rerunDRIfPossible() {
    if (typeof globalThis.runDR !== 'function') return;
    try {
      await globalThis.runDR();
    } catch (err) {
      console.warn('DR rerun after mode switch failed:', err);
    }
  }

  function initModeUI() {
    const modeSel = document.getElementById('drFeatureMode');
    if (!modeSel || modeSel.dataset.bound === '1') return;

    modeSel.value = MODE_POLICY;
    globalThis.DR_FEATURE_MODE = MODE_POLICY;

    modeSel.addEventListener('change', async () => {
      globalThis.DR_FEATURE_MODE = currentMode();
      refreshHints();
      await rerunDRIfPossible();
    });

    modeSel.dataset.bound = '1';
    refreshHints();
  }

  function init() {
    installFunctionWrappers();
    initModeUI();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();