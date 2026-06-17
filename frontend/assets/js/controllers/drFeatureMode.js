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

  // Feature SET (orthogonal to policy/legacy): which family of features to embed.
  const FS_ACCESS = 'access';        // accessibility only (default, original behaviour)
  const FS_ACCESS_DEMO = 'access_demo'; // accessibility + SCB demographics
  const FS_DEMO = 'demo';            // SCB demographics only

  function currentMode() {
    const sel = document.getElementById('drFeatureMode');
    const mode = sel?.value || globalThis.DR_FEATURE_MODE || MODE_POLICY;
    return (mode === MODE_LEGACY) ? MODE_LEGACY : MODE_POLICY;
  }

  function currentFeatureSet() {
    const sel = document.getElementById('drFeatureSet');
    const v = sel?.value || globalThis.DR_FEATURE_SET || FS_ACCESS_DEMO;
    return [FS_ACCESS, FS_ACCESS_DEMO, FS_DEMO].includes(v) ? v : FS_ACCESS_DEMO;
  }

  function isPolicyLabel(label) {
    const s = String(label || '').toLowerCase();
    return POLICY_LABEL_MATCHERS.some((frag) => s.includes(frag));
  }

  // "Extra" (non-accessibility) features are tagged with a label suffix
  // (dem)/(morph)/(modal) and a dem*/rich* key. These are the demographics
  // (district mode) and the rich building-level dims (modal-gap + built form).
  function isDemoLabel(label) {
    return /\((dem|morph|modal)\)/i.test(String(label || ''));
  }
  function isDemoKey(key) {
    return /^dem[A-Z]/.test(String(key || '')) || /^rich[A-Z]/.test(String(key || ''));
  }

  // Should this feature be kept, given policy/legacy mode AND the feature set?
  function keepFeature({ isDemo, isAccessBaseline }) {
    const fs = currentFeatureSet();
    if (fs === FS_DEMO) return isDemo;
    if (fs === FS_ACCESS_DEMO) return isAccessBaseline || isDemo;
    return isAccessBaseline && !isDemo; // FS_ACCESS
  }

  function filterDRMatrixForPolicy(payload) {
    if (!payload || !Array.isArray(payload.X) || !Array.isArray(payload.featureLabels)) return payload;

    const legacy = currentMode() === MODE_LEGACY;
    const keepIdx = [];
    payload.featureLabels.forEach((label, idx) => {
      const demo = isDemoLabel(label);
      const accessBaseline = legacy ? !demo : isPolicyLabel(label);
      if (keepFeature({ isDemo: demo, isAccessBaseline: accessBaseline })) keepIdx.push(idx);
    });

    // Fallback: never return an empty matrix (e.g. demo-only outside district mode).
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
    const legacy = currentMode() === MODE_LEGACY;
    const filtered = diff.features.filter((f) => {
      const demo = isDemoKey(f.key);
      const accessBaseline = legacy ? !demo : POLICY_CONTRAST_KEYS.has(f.key);
      return keepFeature({ isDemo: demo, isAccessBaseline: accessBaseline });
    });
    // Never blank the panel out entirely.
    return { ...diff, features: filtered.length ? filtered : diff.features };
  }

  function installFunctionWrappers() {
    if (typeof globalThis.collectDRData === 'function' && !globalThis.__drModeCollectWrapped) {
      const originalCollectDRData = globalThis.collectDRData;
      globalThis.collectDRData = function wrappedCollectDRData(...args) {
        const out = originalCollectDRData.apply(this, args);
        // Filter always runs: it applies the policy/legacy baseline AND feature set.
        return filterDRMatrixForPolicy(out);
      };
      globalThis.__drModeCollectWrapped = true;
    }

    if (typeof globalThis.computeFeatureDifferences === 'function' && !globalThis.__drModeContrastWrapped) {
      const originalComputeFeatureDifferences = globalThis.computeFeatureDifferences;
      globalThis.computeFeatureDifferences = function wrappedComputeFeatureDifferences(...args) {
        const out = originalComputeFeatureDifferences.apply(this, args);
        return filterContrastiveForPolicy(out);
      };
      globalThis.__drModeContrastWrapped = true;
    }
  }

  function refreshHints() {
    const infoEl = document.getElementById('drSelectInfo');
    if (!infoEl) return;

    const fs = currentFeatureSet();
    const fsLabel = fs === FS_DEMO ? 'SCB demographics only'
      : fs === FS_ACCESS_DEMO ? 'accessibility + SCB demographics'
      : 'accessibility only';
    const note = (fs !== FS_ACCESS)
      ? ' — demographics apply in District mode (Växjö).'
      : '';
    infoEl.textContent = `Features: ${fsLabel}${note}`;
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
    if (modeSel && modeSel.dataset.bound !== '1') {
      modeSel.value = MODE_POLICY;
      globalThis.DR_FEATURE_MODE = MODE_POLICY;
      modeSel.addEventListener('change', async () => {
        globalThis.DR_FEATURE_MODE = currentMode();
        refreshHints();
        await rerunDRIfPossible();
      });
      modeSel.dataset.bound = '1';
    }

    const setSel = document.getElementById('drFeatureSet');
    if (setSel && setSel.dataset.bound !== '1') {
      globalThis.DR_FEATURE_SET = currentFeatureSet();
      setSel.addEventListener('change', async () => {
        globalThis.DR_FEATURE_SET = currentFeatureSet();
        refreshHints();
        // Invalidate cached engine results so EBM/contrastive recompute with the
        // new feature set instead of showing the frozen previous feature list.
        // drPlot is a shared top-level const (not on globalThis).
        if (typeof drPlot !== 'undefined' && drPlot) {
          drPlot.lastFeatureDiff = null;
          drPlot.engineContrast = null;
          drPlot.engineEBM = null;
        }
        await rerunDRIfPossible();
        if (typeof calculateEnginePlot === 'function'
            && typeof drPlot !== 'undefined' && drPlot?.lastSelectionIdx?.length) {
          try { await calculateEnginePlot(); } catch (_) { /* optional */ }
        }
      });
      setSel.dataset.bound = '1';
    }

    // Need-weighted fairness toggle, mirrored into the DR toolbar so it's easy to
    // find. Drives the same global state + recompute as the navbar switch.
    const nwSel = document.getElementById('drNeedWeightToggle');
    if (nwSel && nwSel.dataset.bound !== '1') {
      if (typeof needWeightEnabled !== 'undefined') nwSel.checked = !!needWeightEnabled;
      nwSel.addEventListener('change', async () => {
        const topToggle = document.getElementById('needWeightToggle');
        if (topToggle) { topToggle.checked = nwSel.checked; }
        const topStrength = document.getElementById('needWeightStrength');
        if (topStrength) topStrength.disabled = !nwSel.checked;
        if (typeof setNeedWeighting === 'function') {
          setNeedWeighting({ enabled: nwSel.checked });
        }
      });
      nwSel.dataset.bound = '1';
    }
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