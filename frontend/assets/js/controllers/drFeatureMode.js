// DR feature-mode patch: policy-first vs legacy feature presets
// Loaded after main.js so we can safely wrap global functions.
(function () {
  const MODE_POLICY = 'policy';
  const MODE_LEGACY = 'legacy';

  // Contrastive keys used in policy mode (service-first but still informative).
  const POLICY_CONTRAST_KEYS = new Set([
    // 'overall' EXCLUDED: it is a weighted composite of the per-category scores,
    // so contrasting on it is circular. Per-category access only (all 10 services)
    // so the panel attributes a selection's difference to specific services.
    'fairGrocery',
    'fairHospital',
    'fairPrimary',
    'fairPharmacy',
    'fairHealthcare',
    'fairKindergarten',
    'fairSchoolHigh',
    'fairUniversity',
    'fairDentistry',
    'fairVeterinary',
    'distGrocery',
    'distHospital',
    'distHealthcare',
    'distPharmacy',
    'distVeterinary',
    'distUniversity',
    'distSchoolHigh',
    'distPrimary',
    'distKindergarten',
    'distDentistry',
    'heights',
    'years',
    'areaLog',
    'changeScore',
    'isChange'
  ]);

  // Label fragments retained for DR matrix / EBM in policy mode.
  const POLICY_LABEL_MATCHERS = [
    // 'overall fairness' EXCLUDED on purpose: it is a weighted composite of the
    // per-category scores, so embedding/EBM-ing on it is circular. Keeping only
    // the per-category access lets the EBM attribute a selection's difference
    // (e.g. high child-share areas) to specific services such as university.
    'grocery fairness',
    'hospital fairness',
    'primary school fairness',
    'pharmacy fairness',
    'healthcare center fairness',
    'kindergarten fairness',
    'high school fairness',
    'university fairness',
    'dentistry fairness',
    'veterinary fairness',
    // Raw network distance (m) per service — the DISTANCE model. Matches every
    // 'X distance (m)' label so they enter the embedding/EBM as access features.
    'distance (m)'
  ];

  // Feature SET (orthogonal to policy/legacy): which family of features to embed.
  const FS_ACCESS = 'access';        // accessibility only (default, original behaviour)
  const FS_ACCESS_DEMO = 'access_demo'; // accessibility + SCB demographics
  const FS_DEMO = 'demo';            // SCB demographics only
  // Curated "equity profile": ONE accessibility anchor (overall) + demographics +
  // modal-access gaps, dropping the ~9 collinear per-category fairness scores AND
  // the (morph) built-form dims. Goal: clusters that mean "a type of underserved
  // neighbourhood" instead of the 1-D accessibility filament or a built-form blob.
  const FS_EQUITY = 'equity';
  // Supply-anchored sets. Proximity/gravity access is ~1-D (near-descriptive DR);
  // the 2SFCA supply provision is genuinely multivariate and decorrelates from
  // proximity, so these give the coordinated views a real, map-invisible job.
  const FS_SUPPLY = 'supply';               // 2SFCA supply provision only
  const FS_ACCESS_SUPPLY = 'access_supply'; // per-category access + 2SFCA supply
  // 11-D "Supply + Mismatch": the 10 multivariate supply dims + ONE overall
  // mismatch residual (proximity − supply). Overall-access is NOT a projection
  // dim (gate: redundant, strongest PC1 loader → re-shows the map); it stays a
  // COLOR overlay and its non-redundant residual enters via mismatch. This makes
  // the layout driven by the map-invisible signal — the R1 answer.
  const FS_SUPPLY_PLUS = 'supply_plus';

  // Supply features are tagged with a 'supply (2sfca)' label suffix and a
  // supply* key. Detect from either a label or a bare key (the contrastive path
  // only has keys), so they can be kept for the supply sets and dropped elsewhere.
  function isSupplyFeature(s) {
    const t = String(s || '').toLowerCase();
    return /^supply[a-z]/.test(t) || t.includes('supply (2sfca)');
  }

  // Mismatch dims (overall + per-category): key 'mismatch*' or a label containing
  // 'mismatch'. Matches both the bare contrastive keys and the DR labels.
  function isMismatchFeature(s) {
    return String(s || '').toLowerCase().includes('mismatch');
  }

  function currentMode() {
    const sel = document.getElementById('drFeatureMode');
    const mode = sel?.value || globalThis.DR_FEATURE_MODE || MODE_POLICY;
    return (mode === MODE_LEGACY) ? MODE_LEGACY : MODE_POLICY;
  }

  function currentFeatureSet() {
    const sel = document.getElementById('drFeatureSet');
    const v = sel?.value || globalThis.DR_FEATURE_SET || FS_ACCESS_DEMO;
    return [FS_ACCESS, FS_ACCESS_DEMO, FS_DEMO, FS_EQUITY, FS_SUPPLY, FS_ACCESS_SUPPLY, FS_SUPPLY_PLUS].includes(v) ? v : FS_ACCESS_DEMO;
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
  function keepFeature({ isDemo, isAccessBaseline, label }) {
    const fs = currentFeatureSet();
    const isSupply = isSupplyFeature(label);
    const isMismatch = isMismatchFeature(label);
    // Supply sets: only the 2SFCA supply dims (FS_SUPPLY), or per-category access
    // plus supply (FS_ACCESS_SUPPLY). Supply dims are never access/demo baseline,
    // so they're correctly excluded from all the other sets below.
    if (fs === FS_SUPPLY) return isSupply;
    if (fs === FS_ACCESS_SUPPLY) return isSupply || (isAccessBaseline && !isDemo);
    if (fs === FS_SUPPLY_PLUS) {
      // Projection = 10 supply dims + the mismatch residual (overall in the
      // projection, per-cat in the contrastive). Overall-access is DELIBERATELY
      // NOT a projection dim: the gate showed it's the strongest PC1 loader and
      // partly redundant (r≈0.78 with mean supply), so as a dim it just re-shows
      // the choropleth's central↔peripheral gradient. It stays available as a
      // COLOR overlay, and its NON-redundant part (access − supply) already
      // enters via mismatch. So the layout is driven by the map-invisible signal.
      return isSupply || isMismatch;
    }
    if (isSupply || isMismatch) return false; // keep supply/mismatch out of access/demo/equity
    if (fs === FS_EQUITY) {
      const s = String(label || '').toLowerCase();
      if (/\(morph\)/.test(s)) return false;   // drop built-form (height/area/density/multi)
      if (s.includes('overall')) return true;  // keep ONE accessibility anchor
      return isDemo;                            // keep demographics + modal-access gaps
    }
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
      if (keepFeature({ isDemo: demo, isAccessBaseline: accessBaseline, label })) keepIdx.push(idx);
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
      // For the contrastive panel we only have keys, not labels; pass the key so
      // the equity branch can still spot the 'overall' accessibility anchor.
      return keepFeature({ isDemo: demo, isAccessBaseline: accessBaseline, label: f.key });
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
      : fs === FS_EQUITY ? 'equity profile (overall access + demographics + modal gaps)'
      : fs === FS_SUPPLY ? '2SFCA supply provision only'
      : fs === FS_ACCESS_SUPPLY ? 'per-category access + 2SFCA supply'
      : fs === FS_SUPPLY_PLUS ? 'supply + mismatch (11-D) — colour by overall access'
      : 'accessibility only';
    // The demographics note only applies to the demographic-bearing sets.
    const demoSets = (fs === FS_ACCESS_DEMO || fs === FS_DEMO || fs === FS_EQUITY);
    const note = demoSets ? ' — demographics apply in District mode (Växjö).' : '';
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