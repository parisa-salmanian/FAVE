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
    'grocery access',
    'hospital access',
    'primary school access',
    'pharmacy access',
    'healthcare center access',
    'kindergarten access',
    'high school access',
    'university access',
    'dentistry access',
    'veterinary access',
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
  // "All Data": the full SCB socio-demographic profile (labelled "(scb)" / keyed
  // scb*) + accessibility + supply, ALL as embedding features, at EVERY scale
  // (real DESO at meso/macro, synthetic per-building at micro). Purely
  // exploratory (no colouring) — lasso a cluster, read it in EBM/Contrastive/PCP.
  const FS_ALL_DATA = 'all_data';
  // "Custom": the user hand-picks exactly which inputs feed DR from a grouped
  // checklist (see the #drCustomPanel picker). drCustomKeys holds the selected
  // DR_FEATURE_CONFIG keys; keepFeature matches a column by its key OR its label.
  const FS_CUSTOM = 'custom';
  const drCustomKeys = new Set();     // selected CANONICAL column keys (from the live matrix)
  let drLabelToKey = null;            // Map<label, key> — fallback when only a label is available
  let drFeatureUniverse = [];         // [{key,label}] — the live matrix's full column set
  let drUniverseSig = '';             // signature of the universe key-set (rebuild on change)
  let drPickerBuilding = false;       // reentrancy guard (dry-capture triggers the wrapper)
  let drCustomInit = false;           // seeded all-on once? (so "Deselect all" stays empty)
  // Buildings to sample when capturing the column universe for the Custom picker.
  // The universe only depends on which dimension GROUPS are populated, so a small
  // strided subsample suffices and keeps the picker instant on first open.
  const DR_UNIVERSE_CAP = 1000;

  // SCB profile features carry a '(scb)' label suffix + a scb* key. Detect from a
  // label OR a bare key (the contrastive path only has keys), like isSupplyFeature.
  function isScbFeature(s) {
    const t = String(s || '');
    return /^scb[A-Z]/.test(t) || /\(scb\)/i.test(t);
  }

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

  // Mode-gain dims: key 'mgain*' or a 'gain (from→to)' label (any mode pair).
  // Opt-in via the Custom picker ONLY — they never enter the standard feature
  // sets, so adding them changes no existing embedding.
  function isModeGainFeature(s) {
    const t = String(s || '');
    return /^mgain/i.test(t) || /gain \([a-z]+→[a-z]+\)/i.test(t);
  }

  function currentMode() {
    const sel = document.getElementById('drFeatureMode');
    const mode = sel?.value || globalThis.DR_FEATURE_MODE || MODE_POLICY;
    return (mode === MODE_LEGACY) ? MODE_LEGACY : MODE_POLICY;
  }

  function currentFeatureSet() {
    const sel = document.getElementById('drFeatureSet');
    const v = sel?.value || globalThis.DR_FEATURE_SET || FS_ACCESS_DEMO;
    return [FS_ACCESS, FS_ACCESS_DEMO, FS_DEMO, FS_EQUITY, FS_SUPPLY, FS_ACCESS_SUPPLY, FS_SUPPLY_PLUS, FS_ALL_DATA, FS_CUSTOM].includes(v) ? v : FS_ACCESS_DEMO;
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
  function keepFeature({ isDemo, isAccessBaseline, label, key }) {
    const fs = currentFeatureSet();
    // "Custom": keep exactly the user's picks, matched by the CANONICAL column key
    // (the DR matrix labels differ from the config labels — e.g. "grocery fairness
    // (z)" vs "Grocery fairness" — so key matching is the robust path). Fall back to
    // the label / label→key map when a key isn't supplied.
    if (fs === FS_CUSTOM) {
      if (key != null && drCustomKeys.has(key)) return true;
      if (drCustomKeys.has(label)) return true;
      const k = drLabelToKey ? drLabelToKey.get(label) : null;
      return k ? drCustomKeys.has(k) : false;
    }
    // Mode-gain columns are Custom-only (handled above): every named set keeps
    // its exact pre-existing feature list.
    if (isModeGainFeature(label) || isModeGainFeature(key)) return false;
    const isSupply = isSupplyFeature(label);
    const isMismatch = isMismatchFeature(label);
    const isScb = isScbFeature(label);
    // "All Data": everything real — accessibility + the full SCB profile + supply.
    // NOT the "(dem)" building-synth shares (scb* already carries demographics
    // uniformly at every scale, so keeping (dem) too would just duplicate them at
    // micro). Checked before the supply/mismatch guard so supply stays in.
    if (fs === FS_ALL_DATA) return isAccessBaseline || isScb || isSupply;
    // The SCB columns exist in the matrix at every scale now, but ONLY all_data
    // keeps them — every other set must drop them (else real demographics would
    // enter the embedding and make the EBM coupling tautological).
    if (isScb) return false;
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
    captureFeatureUniverse(payload);   // record the UNFILTERED column set for the picker

    const legacy = currentMode() === MODE_LEGACY;
    const keys = Array.isArray(payload.featureKeys) ? payload.featureKeys : [];
    const keepIdx = [];
    payload.featureLabels.forEach((label, idx) => {
      const demo = isDemoLabel(label);
      const accessBaseline = legacy ? !demo : isPolicyLabel(label);
      if (keepFeature({ isDemo: demo, isAccessBaseline: accessBaseline, label, key: keys[idx] })) keepIdx.push(idx);
    });

    // Fallback: never return an empty matrix (e.g. demo-only outside district mode,
    // or a Custom pick that matched nothing at this scale).
    if (!keepIdx.length) return payload;

    const X2 = payload.X.map((row) => keepIdx.map((j) => row[j]));
    const labels2 = keepIdx.map((j) => payload.featureLabels[j]);
    const keys2 = keys.length ? keepIdx.map((j) => keys[j]) : payload.featureKeys;

    return {
      ...payload,
      X: X2,
      featureLabels: labels2,
      featureKeys: keys2,
      dims: labels2.length
    };
  }

  function filterContrastiveForPolicy(diff) {
    if (!diff || !Array.isArray(diff.features)) return diff;
    // Custom set: the projection is user-picked, but let the contrastive/EBM rank
    // the FULL feature set so a lassoed cluster is explained against everything
    // (its key vocabulary differs from the matrix's anyway).
    if (currentFeatureSet() === FS_CUSTOM) return diff;
    const legacy = currentMode() === MODE_LEGACY;
    const filtered = diff.features.filter((f) => {
      const demo = isDemoKey(f.key);
      const accessBaseline = legacy ? !demo : POLICY_CONTRAST_KEYS.has(f.key);
      // For the contrastive panel we only have keys, not labels; pass the key as
      // both so the equity branch spots the 'overall' anchor AND custom matches it.
      return keepFeature({ isDemo: demo, isAccessBaseline: accessBaseline, label: f.key, key: f.key });
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
      : fs === FS_ALL_DATA ? 'ALL DATA — access + supply + full SCB profile (explore: lasso a cluster, read it in EBM/Contrastive/PCP)'
      : fs === FS_CUSTOM ? `CUSTOM — ${drCustomKeys.size} hand-picked inputs (tick fields above, then Run)`
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

  // ---- Custom field picker -------------------------------------------------
  // Human group names for the SCB manifest `group` tags (from bake_scb_full.py).
  const SCB_GROUP_LABEL = {
    income: 'Income (SCB)', labour: 'Labour market (SCB)', transfers: 'Transfers & benefits (SCB)',
    education: 'Education (SCB)', students_neet: 'Students & NEET (SCB)', age: 'Age bands (SCB)',
    age_stage: 'Age life-stages (SCB)', sex: 'Sex (SCB)', sfi: 'SFI (SCB)',
  };
  // Display order — related groups sit next to each other (the "list close things
  // close together" requirement): economic block together, education block, age block.
  const GROUP_ORDER = [
    'Accessibility — fairness', 'Accessibility — distance', 'Mode gain',
    'Supply (2SFCA)', 'Mismatch',
    'Built form & mobility', 'Demographics — summary',
    'Income (SCB)', 'Transfers & benefits (SCB)', 'Labour market (SCB)',
    'Education (SCB)', 'Students & NEET (SCB)',
    'Age bands (SCB)', 'Age life-stages (SCB)', 'Sex (SCB)', 'SFI (SCB)', 'Other',
  ];

  // key -> SCB manifest group (built from DR_SCB_FEATURES which carries `group`).
  function scbKeyGroupMap() {
    const m = new Map();
    const feats = (typeof DR_SCB_FEATURES !== 'undefined') ? DR_SCB_FEATURES : [];
    for (const f of feats) if (f.group) m.set(f.key, f.group);
    return m;
  }

  // Group a matrix column by its LABEL (robust — labels are stable across the two
  // key vocabularies; e.g. fairness keys are 'grocery' at mezo but 'fairGrocery' in
  // the config). SCB group comes from the manifest via the scbF_ key.
  function fieldGroupOf(item, scbGroups) {
    const key = String(item.key || '');
    const label = String(item.label || '').toLowerCase();
    if (/\(scb\)/.test(label) || /^scb/i.test(key)) {
      if (/^scbF_/.test(key)) return SCB_GROUP_LABEL[scbGroups.get(key)] || 'Demographics — summary';
      return 'Demographics — summary';
    }
    if (/^mgain/i.test(key) || /gain \([a-z]+→[a-z]+\)/.test(label)) return 'Mode gain';
    if (/supply \(2sfca\)/.test(label) || /^supply/i.test(key)) return 'Supply (2SFCA)';
    if (/mismatch/.test(label) || /^mismatch/i.test(key)) return 'Mismatch';
    if (/distance \(m\)/.test(label) || /^dist/i.test(key)) return 'Accessibility — distance';
    if (/fairness|access\b/.test(label) || /^fair/i.test(key) || key === 'overall') return 'Accessibility — fairness';
    if (/\(dem\)/.test(label) || /^dem/i.test(key)) return 'Demographics — summary';
    if (/\((morph|modal)\)/.test(label) || /^rich/i.test(key)) return 'Built form & mobility';
    if (/height|built year|footprint|change score/.test(label)
        || ['heights', 'years', 'arealog', 'changescore'].includes(key.toLowerCase())) return 'Built form & mobility';
    return 'Other';
  }

  function updateCustomCount() {
    const el = document.getElementById('drCustomCount');
    if (el) el.textContent = `${drCustomKeys.size} selected`;
  }

  // Record the live matrix's UNFILTERED column set (key+label) so the picker lists
  // exactly what can be embedded at the current scale/city. Rebuilds a visible
  // picker when the column set changes (scale/city switch). Guarded against the
  // reentrancy from buildCustomPicker's own dry-capture.
  function captureFeatureUniverse(payload) {
    if (!Array.isArray(payload.featureKeys) || !Array.isArray(payload.featureLabels)) return;
    const uni = payload.featureKeys.map((k, i) => ({ key: k, label: payload.featureLabels[i] }))
      .filter(f => typeof f.key === 'string');
    if (!uni.length) return;
    drFeatureUniverse = uni;
    const sig = uni.map(f => f.key).join('|');
    if (sig !== drUniverseSig) {
      drUniverseSig = sig;
      const panel = document.getElementById('drCustomPanel');
      if (panel && !panel.hidden && !drPickerBuilding) buildCustomPicker();
    }
  }

  // (Re)build the grouped checklist from the LIVE matrix universe. Selection is
  // preserved by key across rebuilds. First open (empty selection) starts with
  // everything ON, so "Deselect all" → pick a few is the natural flow.
  function buildCustomPicker() {
    const list = document.getElementById('drCustomList');
    if (!list) return;
    drPickerBuilding = true;
    try {
      // The picker lists the live matrix's column universe. If we don't have it
      // yet, capture it with a dry collectDRData — but over a small strided
      // SUBSAMPLE (DR_UNIVERSE_CAP), which is all it takes to learn which
      // dimension groups are populated. That's a few tens of ms (vs ~2s for the
      // full all-buildings pass), so the picker opens instantly. captureFeature-
      // Universe (called inside collectDRData) fills drFeatureUniverse as a side
      // effect; drPickerBuilding is already true here so it won't re-enter.
      if (!drFeatureUniverse.length && typeof globalThis.collectDRData === 'function') {
        try { globalThis.collectDRData(DR_UNIVERSE_CAP, true, 'none', 0, DR_UNIVERSE_CAP); } catch (_) { /* needs data loaded */ }
      }
      // Hide the synthetic "(dem)" demographic columns that DUPLICATE a real
      // "(scb)" twin (same base label). "(scb)" is the measured SCB value; the
      // "(dem)" spread is just a synthetic per-building version of the SAME
      // quantity (at building scale they even read the identical __synth* prop),
      // so offering both only clutters the picker — keep "(scb)". Växjö-only
      // "(dem)" fields with NO "(scb)" twin (employment, students, …) are kept.
      const cfgsAll = drFeatureUniverse.slice();
      const baseOfLabel = (label) => String(label || '')
        .replace(/\s*\((?:dem|scb)\)\s*$/i, '').trim().toLowerCase();
      const scbBases = new Set(
        cfgsAll.filter(c => /\(scb\)\s*$/i.test(c.label)).map(c => baseOfLabel(c.label))
      );
      const cfgs = cfgsAll.filter(c => {
        if (/\(dem\)\s*$/i.test(c.label) && scbBases.has(baseOfLabel(c.label))) {
          drCustomKeys.delete(c.key);   // also drop from any lingering selection
          return false;                 // hide the redundant "(dem)" duplicate
        }
        return true;
      });
      if (!cfgs.length) { list.innerHTML = '<div class="tiny muted p-2">Run DR once to populate the field list.</div>'; return; }
      drLabelToKey = new Map();
      for (const c of cfgs) drLabelToKey.set(c.label, c.key);
      // Seed everything ON exactly once (first open) — EXCEPT the supply/mismatch
      // columns, which are shown but start OFF (they're the heavy 2SFCA extras; the
      // user opts into them). After that, an empty set means the user deliberately
      // deselected all — don't re-add.
      if (!drCustomInit) {
        drCustomInit = true;
        for (const c of cfgs) {
          if (isSupplyFeature(c.key) || isSupplyFeature(c.label)
              || isMismatchFeature(c.key) || isMismatchFeature(c.label)
              || isModeGainFeature(c.key) || isModeGainFeature(c.label)) continue;
          drCustomKeys.add(c.key);
        }
      }

      const scbGroups = scbKeyGroupMap();
      const groups = new Map();  // groupName -> [{key,label}]
      for (const c of cfgs) {
        const g = fieldGroupOf(c, scbGroups);
        (groups.get(g) || groups.set(g, []).get(g)).push(c);
      }
      const orderedNames = [...GROUP_ORDER.filter(g => groups.has(g)),
        ...[...groups.keys()].filter(g => !GROUP_ORDER.includes(g))];

    // Bootstrap accordion — one collapsible panel per group (nice checkboxes +
    // menu feel). Groups collapsed by default so 12 groups stay compact.
    list.innerHTML = '';
    const acc = document.createElement('div');
    acc.className = 'accordion accordion-flush';
    acc.id = 'drCustomAccordion';
    let gi = 0;
    for (const gname of orderedNames) {
      const items = groups.get(gname);
      const bodyId = `drcg_${gi++}`;
      const item = document.createElement('div');
      item.className = 'accordion-item';

      // header: group toggle checkbox + collapse button (name, size, selected badge)
      const hdr = document.createElement('div');
      hdr.className = 'accordion-header d-flex align-items-center';
      const gWrap = document.createElement('div');
      gWrap.className = 'form-check m-0 ms-2 me-1';
      const gToggle = document.createElement('input');
      gToggle.type = 'checkbox'; gToggle.className = 'form-check-input dr-cg-toggle';
      gToggle.style.cursor = 'pointer'; gToggle.title = 'Toggle all in group';
      gWrap.appendChild(gToggle); hdr.appendChild(gWrap);

      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'accordion-button collapsed py-2 px-2 shadow-none';
      btn.setAttribute('data-bs-toggle', 'collapse');
      btn.setAttribute('data-bs-target', `#${bodyId}`);
      const selBadge = document.createElement('span');
      selBadge.className = 'badge rounded-pill text-bg-primary ms-2';
      btn.innerHTML = `<span class="fw-semibold small">${gname}</span>` +
        `<span class="badge rounded-pill text-bg-light text-muted border ms-2">${items.length}</span>`;
      btn.appendChild(selBadge);
      hdr.appendChild(btn);
      item.appendChild(hdr);

      // body: responsive grid of form-check checkboxes
      const coll = document.createElement('div');
      coll.id = bodyId; coll.className = 'accordion-collapse collapse';
      const body = document.createElement('div');
      body.className = 'accordion-body p-2';
      // The Mode gain group carries its From/To pair controls above the fields.
      if (gname === 'Mode gain') body.appendChild(buildModeGainPairControls());
      const row = document.createElement('div');
      row.className = 'row row-cols-1 row-cols-sm-2 row-cols-md-3 g-1';

      const syncGroupHead = () => {
        const on = items.filter(x => drCustomKeys.has(x.key)).length;
        gToggle.checked = on === items.length;
        gToggle.indeterminate = on > 0 && on < items.length;
        selBadge.textContent = on ? `${on} on` : '';
        selBadge.style.display = on ? '' : 'none';
      };

      items.forEach((c, ci) => {
        const col = document.createElement('div'); col.className = 'col';
        const fc = document.createElement('div'); fc.className = 'form-check mb-0';
        const cb = document.createElement('input');
        cb.type = 'checkbox'; cb.className = 'form-check-input'; cb.id = `${bodyId}_${ci}`;
        cb.dataset.key = c.key; cb.checked = drCustomKeys.has(c.key); cb.style.cursor = 'pointer';
        const lab = document.createElement('label');
        lab.className = 'form-check-label small text-truncate d-block';
        lab.style.cssText = 'cursor:pointer; max-width:100%;';
        lab.setAttribute('for', cb.id); lab.title = c.label; lab.textContent = c.label;
        cb.addEventListener('change', () => {
          if (cb.checked) drCustomKeys.add(c.key); else drCustomKeys.delete(c.key);
          syncGroupHead(); updateCustomCount();
        });
        fc.appendChild(cb); fc.appendChild(lab); col.appendChild(fc); row.appendChild(col);
      });
      body.appendChild(row); coll.appendChild(body); item.appendChild(coll);
      acc.appendChild(item);

      gToggle.addEventListener('click', (e) => e.stopPropagation());
      gToggle.addEventListener('change', () => {
        const on = gToggle.checked;
        for (const c of items) { if (on) drCustomKeys.add(c.key); else drCustomKeys.delete(c.key); }
        coll.querySelectorAll('input.form-check-input').forEach(cb => { cb.checked = on; });
        syncGroupHead(); updateCustomCount();
      });
      syncGroupHead();
    }
    list.appendChild(acc);
      updateCustomCount();
    } finally { drPickerBuilding = false; }
  }

  function setAllCustom(on) {
    drCustomKeys.clear();
    if (on) for (const c of drFeatureUniverse) if (typeof c.key === 'string') drCustomKeys.add(c.key);
    buildCustomPicker();   // rebuild so every checkbox + group header/badge re-syncs
  }

  // From/To travel-mode selects for the mode-gain pair (reviewer point: the
  // mode comparison must not be hardcoded walk→cycle — any two of the four
  // modes can be compared). Changing the pair reloads the layer, relabels the
  // mgain fields everywhere and, if a projection is live, re-runs DR (runDR is
  // serialized and restores the selection + contrastive automatically).
  function buildModeGainPairControls() {
    const wrap = document.createElement('div');
    wrap.className = 'd-flex align-items-center gap-1 mb-2 flex-wrap';
    const pair = (typeof globalThis.modeGainPair === 'function')
      ? globalThis.modeGainPair() : { from: 'walking', to: 'cycling' };
    const modes = (typeof MGAIN_ALL_MODES !== 'undefined')
      ? MGAIN_ALL_MODES : ['walking', 'cycling', 'driving', 'transit'];
    const mkSel = (val, title) => {
      const s = document.createElement('select');
      s.className = 'form-select form-select-sm w-auto';
      s.title = title;
      for (const m of modes) {
        const o = document.createElement('option');
        o.value = m; o.textContent = m;
        if (m === val) o.selected = true;
        s.appendChild(o);
      }
      return s;
    };
    const fromSel = mkSel(pair.from, 'Baseline travel mode (anchors the 0..1 gain scale)');
    const toSel = mkSel(pair.to, 'Comparison travel mode');
    const lbl = document.createElement('span');
    lbl.className = 'small text-muted'; lbl.textContent = 'Gain =';
    const arrow = document.createElement('span');
    arrow.className = 'small text-muted'; arrow.textContent = '→';
    const note = document.createElement('span');
    note.className = 'tiny text-muted ms-1';
    const onChange = async () => {
      const f = fromSel.value, t = toSel.value;
      if (f === t) { note.textContent = 'pick two different modes'; return; }
      if (typeof globalThis.setModeGainPair !== 'function'
          || !globalThis.setModeGainPair(f, t)) { note.textContent = ''; return; }
      note.textContent = 'loading…';
      fromSel.disabled = toSel.disabled = true;
      try {
        await globalThis.ensureModeGain();
        if (typeof globalThis.notifyModeGainPairChanged === 'function') globalThis.notifyModeGainPairChanged();
        drFeatureUniverse = []; drUniverseSig = '';   // labels changed → recapture
        const ok = typeof globalThis.modeGainReady === 'function' && globalThis.modeGainReady();
        note.textContent = ok ? '' : 'not available for this city';
        buildCustomPicker();
        if (ok && typeof globalThis.runDR === 'function'
            && typeof drPlot !== 'undefined' && drPlot?.points) {
          await globalThis.runDR();
        }
      } finally { fromSel.disabled = toSel.disabled = false; }
    };
    fromSel.addEventListener('change', onChange);
    toSel.addEventListener('change', onChange);
    wrap.append(lbl, fromSel, arrow, toSel, note);
    return wrap;
  }

  // Preload the 2SFCA supply layer (same call the inspector's Supply panel uses) so
  // the supply/mismatch columns exist in the matrix → appear in the picker. Cached,
  // so it's instant once loaded (e.g. after an inspector hover). Then force a fresh
  // universe capture so those new columns are picked up.
  async function ensureSupplyForPicker() {
    let loadedAny = false;
    try {
      if (typeof globalThis.ensureAccess2sfca === 'function') {
        const mode = (document.getElementById('fairnessTravelMode')?.value || 'walking').toLowerCase();
        const city = (typeof globalThis.a2sCurrentCity === 'function') ? globalThis.a2sCurrentCity() : undefined;
        await globalThis.ensureAccess2sfca(city, mode);
        loadedAny = true;
      }
    } catch (_) { /* supply is best-effort */ }
    // Also preload the mode-gain (walk→cycle) layer so its columns show up in
    // the picker (they start unchecked, like supply/mismatch).
    try {
      if (typeof globalThis.ensureModeGain === 'function') {
        await globalThis.ensureModeGain();
        loadedAny = true;
      }
    } catch (_) { /* mode gain is best-effort */ }
    if (loadedAny) { drFeatureUniverse = []; drUniverseSig = ''; }   // force re-capture
  }

  // The custom picker behaves like a DROPDOWN: it floats under the toolbar
  // (absolute overlay, doesn't push the layout) and can be dismissed without
  // leaving the Custom feature set. Opening/closing only toggles visibility —
  // DR_FEATURE_SET stays 'custom' and the picked keys persist, so Run still uses
  // them while the picker is closed.
  function positionCustomPanel(panel) {
    const toolbar = document.querySelector('#drOffcanvas .dr-plot-toolbar');
    if (toolbar) panel.style.top = toolbar.offsetHeight + 'px';
  }
  function isCustomPickerOpen() {
    const panel = document.getElementById('drCustomPanel');
    return !!(panel && !panel.hidden);
  }
  // The dedicated "Fields ▾" toggle button sits in the toolbar next to the
  // Features <select>. It only shows while Custom is the active set (nothing to
  // toggle otherwise) and flips its caret + aria-expanded with the panel state.
  function updateCustomToggleBtn() {
    const btn = document.getElementById('drCustomToggle');
    if (!btn) return;
    const isCustom = currentFeatureSet() === FS_CUSTOM;
    btn.hidden = !isCustom;
    const open = isCustomPickerOpen();
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    btn.dataset.active = open ? 'true' : 'false';
    btn.textContent = open ? 'Fields ▴' : 'Fields ▾';
  }
  function setCustomPickerOpen(open) {
    const panel = document.getElementById('drCustomPanel');
    if (!panel) return;
    if (open) {
      positionCustomPanel(panel);
      panel.hidden = false;
      buildCustomPicker();
    } else {
      panel.hidden = true;
    }
    updateCustomToggleBtn();
  }

  // Auto-open iff Custom is the active feature set; auto-close for any other set.
  function syncCustomPanelVisibility() {
    setCustomPickerOpen(currentFeatureSet() === FS_CUSTOM);
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
        const isCustom = currentFeatureSet() === FS_CUSTOM;
        // "All Data" / "Custom" are explore sets → default to NO colouring so the
        // layout is read as clusters (via EBM/Contrastive/PCP), not tinted.
        if (currentFeatureSet() === FS_ALL_DATA || isCustom) {
          const colorSel = document.getElementById('drColorBy');
          if (colorSel && colorSel.value !== 'none') colorSel.value = 'none';
        }
        // Invalidate cached engine results so EBM/contrastive recompute with the
        // new feature set instead of showing the frozen previous feature list.
        // drPlot is a shared top-level const (not on globalThis).
        if (typeof drPlot !== 'undefined' && drPlot) {
          drPlot.lastFeatureDiff = null;
          drPlot.engineContrast = null;
          drPlot.engineEBM = null;
        }
        if (isCustom) {
          // Show + build the picker IMMEDIATELY from the columns already in the
          // live matrix — do NOT block on the async 2SFCA load (awaiting it here
          // was the multi-second delay before the panel appeared). Preload supply
          // in the BACKGROUND; when it lands, rebuild so the supply/mismatch
          // columns appear (they start unchecked). Custom never auto-runs — the
          // user picks fields first, then presses Run.
          syncCustomPanelVisibility();
          refreshHints();
          ensureSupplyForPicker().then(() => {
            if (currentFeatureSet() === FS_CUSTOM) buildCustomPicker();
          }).catch(() => { /* supply is best-effort */ });
          return;
        }
        syncCustomPanelVisibility();   // hide the picker for non-custom sets
        refreshHints();
        await rerunDRIfPossible();
        if (typeof calculateEnginePlot === 'function'
            && typeof drPlot !== 'undefined' && drPlot?.lastSelectionIdx?.length) {
          try { await calculateEnginePlot(); } catch (_) { /* optional */ }
        }
      });
      setSel.dataset.bound = '1';
    }

    // Custom picker buttons.
    const selAllBtn = document.getElementById('drCustomSelectAll');
    if (selAllBtn && selAllBtn.dataset.bound !== '1') {
      selAllBtn.addEventListener('click', () => setAllCustom(true));
      selAllBtn.dataset.bound = '1';
    }
    const clrAllBtn = document.getElementById('drCustomClearAll');
    if (clrAllBtn && clrAllBtn.dataset.bound !== '1') {
      clrAllBtn.addEventListener('click', () => setAllCustom(false));
      clrAllBtn.dataset.bound = '1';
    }

    // Dropdown dismissal — the picker closes without changing the feature set.
    // (1) the × button:
    const closeBtn = document.getElementById('drCustomClose');
    if (closeBtn && closeBtn.dataset.bound !== '1') {
      closeBtn.addEventListener('click', () => setCustomPickerOpen(false));
      closeBtn.dataset.bound = '1';
    }
    // (2) the dedicated "Fields ▾" toggle button — opens/collapses the picker on
    //     EVERY click. This is the reliable toggle: a native <select> fires no
    //     `change` when you re-pick the already-selected "Custom" option, so the
    //     field can't double as the toggle. The <select> is therefore left fully
    //     native (one click = switch feature sets); the button owns open/close.
    const toggleBtn = document.getElementById('drCustomToggle');
    if (toggleBtn && toggleBtn.dataset.bound !== '1') {
      toggleBtn.addEventListener('click', (e) => {
        e.preventDefault();
        if (currentFeatureSet() !== FS_CUSTOM) return;   // only meaningful for Custom
        setCustomPickerOpen(!isCustomPickerOpen());
      });
      toggleBtn.dataset.bound = '1';
    }
    // (3) Esc, or (4) a click anywhere outside the picker except the toolbar (so
    //     Run/Lasso don't yank it shut). Bound once at the document level.
    if (document.body.dataset.drCustomDismiss !== '1') {
      document.addEventListener('mousedown', (e) => {
        if (!isCustomPickerOpen()) return;
        const panel = document.getElementById('drCustomPanel');
        if (panel.contains(e.target)) return;
        if (e.target.closest && e.target.closest('.dr-plot-toolbar')) return;
        setCustomPickerOpen(false);
      }, true);
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && isCustomPickerOpen()) setCustomPickerOpen(false);
      });
      document.body.dataset.drCustomDismiss = '1';
    }
    syncCustomPanelVisibility();  // reflect the initial feature-set value

    // Colour-by: re-tint the EXISTING projection immediately, WITHOUT re-running
    // UMAP (which would change the layout + wipe the selection). Changing the
    // colour only re-reads drPlot.rows through computeDRColors and redraws. If no
    // projection exists yet, it silently applies on the next Run.
    const colorSel = document.getElementById('drColorBy');
    if (colorSel && colorSel.dataset.bound !== '1') {
      colorSel.addEventListener('change', () => {
        if (typeof drPlot === 'undefined' || !drPlot || !drPlot.points
            || !Array.isArray(drPlot.rows) || !drPlot.rows.length
            || typeof computeDRColors !== 'function' || typeof redrawDR !== 'function') {
          return; // nothing plotted yet → colour applies on next Run
        }
        try {
          drPlot.colors = computeDRColors(drPlot.rows, colorSel.value, drPlot.mode);
          const sel = Array.isArray(drPlot.lastSelectionIdx) && drPlot.lastSelectionIdx.length
            ? drPlot.lastSelectionIdx : null;
          redrawDR(sel);
        } catch (e) { console.warn('DR recolour failed:', e); }
      });
      colorSel.dataset.bound = '1';
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