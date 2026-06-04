// Change-history log: snapshot/undo/redo for what-if scenarios.
// Tracks the per-building fairness state before/after each user action so the
// UI can replay the timeline. Extracted from main.js. Loaded as a classical
// script before main.js — top-level state is self-contained and the functions
// reach into main.js globals (baseCityFC, fairActive, ...) at call time.

// ======================= Change history log =======================

/**
 * Compute an RGBA snapshot color for a given Gini delta.
 * giniDelta < 0  → fairness improved → teal (positive)
 * giniDelta > 0  → fairness worsened → red  (negative)
 * Intensity scales with magnitude up to maxDelta.
 */
function getChangeSnapshotColor(giniDelta) {
  if (!Number.isFinite(giniDelta) || giniDelta === 0) return [128, 128, 128];
  const intensity = Math.min(1, Math.abs(giniDelta) / 0.1);
  if (giniDelta < 0) {
    // Improved: bright orange
    return [255, Math.round(100 + intensity * 80), 0];
  } else {
    // Worsened: crimson red
    return [Math.round(180 + intensity * 40), Math.round(20 * (1 - intensity)), Math.round(40 * (1 - intensity))];
  }
}

/** Capture current per-building fairness scores before a change. */
function captureScoreSnapshot() {
  const snapshot = new Map();
  (baseCityFC?.features || []).forEach((f, idx) => {
    const score = f?.properties?.fair?.score;
    if (Number.isFinite(score)) snapshot.set(idx, score);
  });
  return snapshot;
}

/**
 * After a recompute, diff each building's new score against the snapshot
 * and paint _changeColor based on per-building delta.
 */
// function applyDeltaColorsFromSnapshot(snapshot, changeId) {
//   // Returns {feature, color}[] pairs — does NOT write to features yet.
//   // Colors are applied only when user clicks the change entry.
//   const pairs = [];
//   (baseCityFC?.features || []).forEach((f, idx) => {
//     if (!f?.properties) return;
//     const score = f.properties.fair?.score;
//     if (!Number.isFinite(score)) return;
//     const before = snapshot.get(idx);
//     let delta = 0;
//     let useGlobal = false;
//     if (Number.isFinite(before)) {
//       delta = score - before; // positive = improved
//     } else {
//       // No before snapshot (first POI selection) — use score itself
//       useGlobal = true;
//       delta = (score - 0.5); // treat above-average as positive
//     }
//     if (!useGlobal && Math.abs(delta) < 0.002) {
//       pairs.push({ feature: f, color: [247, 247, 247], changeId });
//       return;
//     }
//     const maxDelta = useGlobal ? 0.4 : 0.06;
//     const intensity = Math.min(1, Math.max(0, Math.abs(delta) / maxDelta));
//     if (intensity < 0.05) {
//       pairs.push({ feature: f, color: [247, 247, 247], changeId });
//       return;
//     }
//     const alpha = Math.round(140 + intensity * 115);
//     // Diverging 9-color scale: red (worsened) ← neutral → blue (improved)
//     // #b2182b #d6604d #f4a582 #fddbc7 #f7f7f7 #d1e5f0 #92c5de #4393c3 #2166ac
//     const CHANGE_DIVERGING = [
//       [178,  24,  43], // #b2182b  (most worsened)
//       [214,  96,  77], // #d6604d
//       [244, 165, 130], // #f4a582
//       [253, 219, 199], // #fddbc7
//       [247, 247, 247], // #f7f7f7  (neutral center)
//       [209, 229, 240], // #d1e5f0
//       [146, 197, 222], // #92c5de
//       [ 67, 147, 195], // #4393c3
//       [ 33, 102, 172], // #2166ac  (most improved)

//       // [215,  48,  39], // #d73027 (most worsened)
//       // [244, 109,  67], // #f46d43
//       // [253, 174,  97], // #fdae61
//       // [254, 224, 139], // #fee08b
//       // [255, 255, 191], // #ffffbf (neutral center)
//       // [217, 239, 139], // #d9ef8b
//       // [166, 217, 106], // #a6d96a
//       // [102, 189,  99], // #66bd63
//       // [ 26, 152,  80], // #1a9850 (most improved)
//     ];
//     // Map intensity (0–1) + direction to a position in the 9-stop ramp
//     // delta > 0 → improved → blue half (indices 5–8)
//     // delta < 0 → worsened → red half  (indices 0–3)
//     // intensity 0 → near center, intensity 1 → most saturated end
//     // Skip the white center (#f7f7f7) — map directly to the colored halves
//     // so even low-intensity changes are visually distinct from gray "no change"
//     let t;
//     if (delta > 0) {
//       t = 5 + intensity * 3; // 5 → 8 (light blue to deep blue)
//     } else {
//       t = 3 - intensity * 3; // 3 → 0 (light pink to deep red)
//     }
//     t = Math.max(0, Math.min(8, t));
//     const lo = Math.floor(t);
//     const hi = Math.min(8, lo + 1);
//     const frac = t - lo;
//     const color = [
//       Math.round(CHANGE_DIVERGING[lo][0] + (CHANGE_DIVERGING[hi][0] - CHANGE_DIVERGING[lo][0]) * frac),
//       Math.round(CHANGE_DIVERGING[lo][1] + (CHANGE_DIVERGING[hi][1] - CHANGE_DIVERGING[lo][1]) * frac),
//       Math.round(CHANGE_DIVERGING[lo][2] + (CHANGE_DIVERGING[hi][2] - CHANGE_DIVERGING[lo][2]) * frac),
//       alpha
//     ];
//     pairs.push({ feature: f, color, changeId });
//   });
//   return pairs;
// }

function applyDeltaColorsFromSnapshot(snapshot, changeId) {
  const CHANGE_DIVERGING = [
    [178,  24,  43], // #b2182b  (most worsened)
    [214,  96,  77], // #d6604d
    [244, 165, 130], // #f4a582
    [253, 219, 199], // #fddbc7
    [247, 247, 247], // #f7f7f7  (neutral center)
    [209, 229, 240], // #d1e5f0
    [146, 197, 222], // #92c5de
    [ 67, 147, 195], // #4393c3
    [ 33, 102, 172], // #2166ac  (most improved)
  ];

  // --- Pass 1: compute deltas for every building ---
  const entries = [];
  (baseCityFC?.features || []).forEach((f, idx) => {
    if (!f?.properties) return;
    const score = f.properties.fair?.score;
    if (!Number.isFinite(score)) return;
    const before = snapshot.get(idx);
    let delta = 0;
    let useGlobal = false;
    if (Number.isFinite(before)) {
      delta = score - before;
    } else {
      useGlobal = true;
      delta = score - 0.5;
    }
    entries.push({ f, delta, useGlobal });
  });

  if (!entries.length) return [];

  // Find the actual min and max delta in this change
  const deltas = entries.map(e => e.delta);
  const minDelta = Math.min(...deltas);
  const maxDelta = Math.max(...deltas);

  // --- Pass 2: map each delta to the full 0–8 scale ---
  const pairs = [];
  for (const { f, delta, useGlobal } of entries) {
    // No-change → neutral gray
    if (!useGlobal && Math.abs(delta) < 0.002) {
      pairs.push({ feature: f, color: [247, 247, 247, 160], changeId });
      continue;
    }

    // Map delta to 0–8 using the actual range of this change
    // minDelta → 0 (deepest red), 0 → 4 (neutral), maxDelta → 8 (deepest blue)
    let t;
    if (delta < 0 && minDelta < 0) {
      t = 4 * (1 - delta / minDelta);  // minDelta → 0, zero → 4
    } else if (delta > 0 && maxDelta > 0) {
      t = 4 + 4 * (delta / maxDelta);  // zero → 4, maxDelta → 8
    } else {
      t = 4; // exactly zero or no range
    }

    t = Math.max(0, Math.min(8, t));
    const lo = Math.floor(t);
    const hi = Math.min(8, lo + 1);
    const frac = t - lo;
    const color = [
      Math.round(CHANGE_DIVERGING[lo][0] + (CHANGE_DIVERGING[hi][0] - CHANGE_DIVERGING[lo][0]) * frac),
      Math.round(CHANGE_DIVERGING[lo][1] + (CHANGE_DIVERGING[hi][1] - CHANGE_DIVERGING[lo][1]) * frac),
      Math.round(CHANGE_DIVERGING[lo][2] + (CHANGE_DIVERGING[hi][2] - CHANGE_DIVERGING[lo][2]) * frac),
      220
    ];
    pairs.push({ feature: f, color, changeId });
  }
  return pairs;
}

/**
 * Record a what-if change, paint affected buildings with a snapshot color,
 * and update the change log dropdown.
 */
function recordWhatIfChange({ action, description, category, beforeGini, afterGini, beforeOverall, afterOverall, affectedFeatures = [] }) {
  const city = lastCityName || '—';
  const source = sourceMode || 'osm';
  // Prefer category Gini delta; fall back to overall
  const catDelta = (Number.isFinite(beforeGini) && Number.isFinite(afterGini))
    ? afterGini - beforeGini : null;
  const overallDelta = (Number.isFinite(beforeOverall) && Number.isFinite(afterOverall))
    ? afterOverall - beforeOverall : null;
  const giniDelta = catDelta ?? overallDelta;

  const snapshotColor = getChangeSnapshotColor(giniDelta);

  // Colors are NOT applied here — only on-demand when user clicks the change entry

  // Compute centroids for map flyto + highlight
  // affectedFeatures may be raw features OR {feature, color, changeId}[] pairs
  const rawFeatures = affectedFeatures.map(f => f?.feature ?? f).filter(Boolean);
  const affectedCentroids = rawFeatures
    .map(f => { try { return turf.centroid(f).geometry.coordinates; } catch { return null; } })
    .filter(Boolean);
  const highlightCenter = affectedCentroids.length
    ? affectedCentroids.reduce((acc, c) => [acc[0] + c[0] / affectedCentroids.length, acc[1] + c[1] / affectedCentroids.length], [0, 0])
    : null;

  const record = {
    id: changeLogIdCounter++,
    timestamp: new Date(),
    city,
    source,
    action,
    description,
    category: category || null,
    beforeGini: Number.isFinite(beforeGini) ? beforeGini : null,
    afterGini: Number.isFinite(afterGini) ? afterGini : null,
    beforeOverall: Number.isFinite(beforeOverall) ? beforeOverall : null,
    afterOverall: Number.isFinite(afterOverall) ? afterOverall : null,
    giniDelta,
    overallDelta,
    snapshotColor,
    affectedCount: rawFeatures.length,
    affectedCentroids,
    highlightCenter,
    changeId: changeLogIdCounter - 1,
    colorPairs: affectedFeatures  // {feature, color, changeId}[] from applyDeltaColorsFromSnapshot
  };
  whatIfChangeLog.push(record);
  changeLogTick++;
  updateChangeLogUI();
  updateLayers();
  return record;
}

function deltaToColor(delta) {
  const CHANGE_DIVERGING = [
    [178,  24,  43],
    [214,  96,  77],
    [244, 165, 130],
    [253, 219, 199],
    [247, 247, 247],
    [209, 229, 240],
    [146, 197, 222],
    [ 67, 147, 195],
    [ 33, 102, 172],
  ];
  // Negative Gini delta = improvement = blue, positive = worsened = red
  const d = -delta;
  let t;
  if (d < 0) {
    t = Math.max(0, 4 * (1 - Math.min(1, Math.abs(d) / 0.1)));
  } else if (d > 0) {
    t = Math.min(8, 4 + 4 * Math.min(1, d / 0.1));
  } else {
    t = 4;
  }
  const lo = Math.floor(t);
  const hi = Math.min(8, lo + 1);
  const frac = t - lo;
  const r = Math.round(CHANGE_DIVERGING[lo][0] + (CHANGE_DIVERGING[hi][0] - CHANGE_DIVERGING[lo][0]) * frac);
  const g = Math.round(CHANGE_DIVERGING[lo][1] + (CHANGE_DIVERGING[hi][1] - CHANGE_DIVERGING[lo][1]) * frac);
  const b = Math.round(CHANGE_DIVERGING[lo][2] + (CHANGE_DIVERGING[hi][2] - CHANGE_DIVERGING[lo][2]) * frac);
  return `rgb(${r},${g},${b})`;
}

/** Rebuild the change log dropdown contents. */
function updateChangeLogUI() {
  const countBadge  = document.getElementById('changeLogCount');
  const list        = document.getElementById('changeLogList');
  const empty       = document.getElementById('changeLogEmpty');
  const footer      = document.getElementById('changeLogFooter');
  if (!list) return;

  const total = whatIfChangeLog.length;
  if (countBadge) {
    countBadge.textContent = total;
    countBadge.classList.toggle('d-none', total === 0);
  }
  if (total === 0) {
    list.innerHTML = '';
    empty?.classList.remove('d-none');
    footer?.classList.add('d-none');
    return;
  }
  empty?.classList.add('d-none');
  footer?.classList.remove('d-none');

  // Group by city + source (insertion order preserves chronology)
  const groups = new Map();
  whatIfChangeLog.forEach(rec => {
    const key = `${rec.city}||${rec.source}`;
    if (!groups.has(key)) groups.set(key, { city: rec.city, source: rec.source, entries: [] });
    groups.get(key).entries.push(rec);
  });

  let html = '';
  groups.forEach(group => {
    html += `<div class="change-group-header">🏙 ${group.city} · ${group.source.toUpperCase()}</div>`;
    // Newest first
    [...group.entries].reverse().forEach(rec => {
      const delta = rec.giniDelta;
      const dirClass = !Number.isFinite(delta) ? 'neutral'
        : delta < 0 ? 'positive' : delta > 0 ? 'negative' : 'neutral';
      const deltaText = Number.isFinite(delta)
        ? `${delta > 0 ? '+' : ''}${delta.toFixed(3)}`
        : '±—';
      const swatchBg = deltaToColor(Number.isFinite(delta) ? delta : 0);
      const textColor = Math.abs(Number.isFinite(delta) ? delta : 0) < 0.005 ? '#333' : '#fff';
      const timeStr = rec.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      const catLabel = rec.category ? `<span>📦 ${prettyPOIName(rec.category)}</span>` : '';
      const beforeAfter = (Number.isFinite(rec.beforeGini) && Number.isFinite(rec.afterGini))
        ? `<span>Gini ${rec.beforeGini.toFixed(3)} → ${rec.afterGini.toFixed(3)}</span>` : '';
      const hasFeatures = (rec.colorPairs?.length > 0) || rec.highlightCenter;
      const isPinned = pinnedChangeId === rec.id;
      const mapBtnHtml = hasFeatures
        ? `<button class="change-map-toggle btn btn-xs ms-2" data-change-id="${rec.id}"
            style="font-size:0.68rem;padding:1px 7px;border-radius:10px;border:1px solid ${isPinned ? '#0dcaf0' : '#aaa'};
            background:${isPinned ? '#0dcaf0' : 'transparent'};color:${isPinned ? '#000' : '#666'};cursor:pointer;white-space:nowrap;">
            ${isPinned ? '👁 Hide' : '👁 Show'}
           </button>`
        : '';
      html += `
        <div class="change-entry ${dirClass}" data-change-id="${rec.id}">
          <div class="d-flex justify-content-between align-items-start">
            <div class="fw-semibold" style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${rec.description}</div>
            <div class="d-flex align-items-center">
              ${mapBtnHtml}
              <span class="change-delta-badge ms-2" style="background:${swatchBg};color:${textColor};">${deltaText}</span>
            </div>
          </div>
          <div class="d-flex flex-wrap gap-2 mt-1" style="font-size:0.68rem;color:#555;">
            ${catLabel}${beforeAfter}
            <span class="ms-auto">${timeStr}</span>
          </div>
        </div>`;
    });
  });
  list.innerHTML = html;

  // Wire click handlers on entries that have affected features
  list.querySelectorAll('.change-map-toggle[data-change-id]').forEach(btn => {
    const id = Number(btn.dataset.changeId);
    const rec = whatIfChangeLog.find(r => r.id === id);
    if (rec) {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        highlightChangeOnMap(rec);
      });
    }
  });
}

let changeHighlightTimeout = null;
let activeHighlightPairs = [];

function clearChangeColorAggregates() {
  mezoHexData?.forEach(cell => { if (cell) delete cell._changeColor; });
  districtFC?.features?.forEach(f => { if (f?.properties) delete f.properties._changeColor; });
}

function highlightChangeOnMap(record) {
  if (!record) return;

  // If already pinned, unpin (hide)
  if (pinnedChangeId === record.id) {
    pinnedChangeId = null;
    activeHighlightPairs.forEach(({ feature }) => {
      if (feature?.properties) delete feature.properties._changeColor;
    });
    activeHighlightPairs = [];
    clearChangeColorAggregates();
    setSidePanelLegendMode((pinnedChangeId != null && !changeCompareBaseline) ? 'change' : 'fairness');
    changeLogTick++;
    updateLayers();
    updateChangeLogUI();
    return;
  }

  // Unpin previous
  activeHighlightPairs.forEach(({ feature }) => {
    if (feature?.properties) delete feature.properties._changeColor;
  });
  activeHighlightPairs = [];
  clearChangeColorAggregates();

  // Pin new
  pinnedChangeId = record.id;

  const pairs = record.colorPairs || [];
  // Only apply _changeColor for buildings with a meaningful color delta.
  // Neutral-gray pairs [247,247,247,*] indicate near-zero change — skip them
  // so those buildings fall through to the dim background ([90,90,90,120])
  // instead of being painted an almost-identical gray that makes the whole
  // map look washed out.
  const meaningfulPairs = pairs.filter(({ color }) => {
    if (!color) return false;
    return !(color[0] === 247 && color[1] === 247 && color[2] === 247);
  });
  meaningfulPairs.forEach(({ feature, color }) => {
    if (feature?.properties) feature.properties._changeColor = color;
  });
  activeHighlightPairs = meaningfulPairs;

  // Aggregate up to meso and district levels
  computeChangeColorAggregates(pairs);

  setSidePanelLegendMode('change');
  changeLogTick++;
  updateLayers();
  updateChangeLogUI();
}

function computeChangeColorAggregates(colorPairs) {
  // Use MAX intensity per cell (not average) so colors stay vivid.
  // Keep semantics aligned with building-level change map:
  // skip neutral no-change pairs ([247,247,247,*]).
  const meaningfulPairs = (colorPairs || []).filter(({ color }) => {
    if (!Array.isArray(color) || color.length < 3) return false;
    return !(color[0] === 247 && color[1] === 247 && color[2] === 247);
  });

  // ── Meso (H3 hex) aggregation ──
  const h3 = window.h3;
  const res = resolveMezoResolution();
  if (h3 && res != null && mezoHexData?.length) {
    const hexBest = new Map(); // hexId → {color, intensity}
    meaningfulPairs.forEach(({ feature, color }) => {
      if (!feature?.geometry) return;
      try {
        const [lng, lat] = turf.centroid(feature).geometry.coordinates;
        const cell = h3LatLngToCell(h3, lat, lng, res);
        if (!cell) return;
        const intensity = (color[3] ?? 200);
        const prev = hexBest.get(cell);
        if (!prev || intensity > prev.intensity) {
          hexBest.set(cell, { color, intensity });
        }
      } catch { /* ignore */ }
    });
    mezoHexData.forEach(cell => {
      if (!cell) return;
      const best = hexBest.get(cell.hex);
      if (best) {
        const c = best.color;
        // Boost alpha significantly for hex visibility
        cell._changeColor = [c[0], c[1], c[2], Math.min(255, (c[3] ?? 200) + 80)];
      } else {
        delete cell._changeColor;
      }
    });
  }

  // ── District aggregation ──
  if (districtFC?.features?.length) {
    // Build per-district: track dominant direction + max intensity
    const distBest = new Map(); // idx → {color, intensity}
    meaningfulPairs.forEach(({ feature, color }) => {
      if (!feature?.geometry) return;
      try {
        const pt = turf.centroid(feature);
        districtFC.features.forEach((dist, idx) => {
          if (!dist?.geometry) return;
          try {
            if (turf.booleanPointInPolygon(pt, dist)) {
              const intensity = (color[3] ?? 200);
              const prev = distBest.get(idx);
              if (!prev || intensity > prev.intensity) {
                distBest.set(idx, { color, intensity });
              }
            }
          } catch { /* ignore */ }
        });
      } catch { /* ignore */ }
    });
    districtFC.features.forEach((dist, idx) => {
      if (!dist?.properties) return;
      const best = distBest.get(idx);
      if (best) {
        const c = best.color;
        // Boost alpha significantly for district visibility
        dist.properties._changeColor = [c[0], c[1], c[2], Math.min(255, (c[3] ?? 200) + 100)];
      } else {
        delete dist.properties._changeColor;
      }
    });
  }
}

function formatWhatIfSuggestionSummary(suggestions) {
  if (!suggestions.length) return 'No suggestions available.';
  const byCat = suggestions.reduce((acc, s) => {
    const key = `${s.kind}:${s.cat}`;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const parts = Object.entries(byCat).map(([key, count]) => {
    const [kind, cat] = key.split(':');
    const verb = kind === 'change' ? 'Change' : 'Add';
    return `${verb} ${count} × ${prettyPOIName(cat)}`;
  });
  return `Suggestions ready: ${parts.join(', ')}.`;
}

function updateWhatIfSuggestionUI(text, { isError = false, isBusy = false, hasSuggestion = false } = {}) {
  if (whatIfSuggestionOut) {
    whatIfSuggestionOut.textContent = text || '';
    whatIfSuggestionOut.classList.toggle('text-danger', !!isError);
    whatIfSuggestionOut.classList.toggle('text-muted', !isError);
  }
  if (whatIfApplySuggestionBtn) whatIfApplySuggestionBtn.disabled = !hasSuggestion || isBusy;
  if (whatIfClearSuggestionBtn) whatIfClearSuggestionBtn.disabled = !hasSuggestion || isBusy;
  if (whatIfVerifySuggestionBtn) whatIfVerifySuggestionBtn.disabled = !hasSuggestion || isBusy;
  if (whatIfSuggestBtn) whatIfSuggestBtn.disabled = !!isBusy;
}

async function runWhatIfSuggestionFromChat(prompt, overrides = {}) {
  if (!baseCityFC) throw new Error('Load a city before requesting suggestions.');
  const fallbackKind = overrides.mode || whatIfModeSelect?.value || 'add';
  const fallbackCount = Number.isFinite(overrides.count)
    ? Math.max(1, Math.min(5, overrides.count))
    : Math.max(1, Math.min(5, parseInt(whatIfCountInput?.value || '1', 10) || 1));
  const selectedCategories = Array.isArray(overrides.categories) && overrides.categories.length
    ? overrides.categories
    : getSelectedWhatIfCategories();
  const fallbackCategories = selectedCategories.length
    ? selectedCategories
    : [whatIfType || ALL_CATEGORIES[0] || 'grocery'];
  const fallbackFairnessTarget = overrides.fairnessTarget || whatIfFairnessTargetSelect?.value || 'category';
  const fallbackAreaFocus = overrides.areaFocus || whatIfAreaFocusSelect?.value || 'any';

  const focusOverride = overrides.focus === 'viewport'
    ? 'viewport'
    : (overrides.focus === 'city'
      ? 'city'
      : (overrides.focus === 'lasso' ? 'lasso' : null));
  let lassoRequested = focusOverride === 'lasso';
  const useBounds = focusOverride === 'viewport'
    ? true
    : focusOverride === 'city'
      ? false
      : !!whatIfUseBoundsToggle?.checked;
  const lassoBbox = getWhatIfLassoBBox();
  const fallbackBbox = useBounds ? getMapBoundsBBox() : null;
  const radiusKm = Math.max(0, parseFloat(whatIfRadiusInput?.value || '0') || 0);
  const center = radiusKm > 0 ? getMapCenter() : null;

  updateWhatIfSuggestionUI('Thinking', { isBusy: true });
  try {
    let categories = fallbackCategories;
    let kind = fallbackKind;
    let count = fallbackCount;
    let bbox = fallbackBbox;
    let selectedRadiusKm = radiusKm;
    let selectedCenter = center;
    let fairnessTarget = fallbackFairnessTarget;
    let areaFocus = fallbackAreaFocus;
    let rationale = null;

    if (prompt) {
      const intent = await requestLLMWhatIfIntent(prompt, {
        available_categories: ALL_CATEGORIES,
        current_category: fallbackCategories[0],
        selected_categories: fallbackCategories,
        max_count: fallbackCount,
        radius_km: radiusKm,
        focus_default: useBounds ? 'viewport' : 'city',
        fairness_default: fallbackFairnessTarget,
        area_default: fallbackAreaFocus,
        lasso_available: !!whatIfLasso.selectionRing,
        lasso_bbox: lassoBbox
      });
      if (intent?.categories?.length) categories = intent.categories;
      if (intent?.mode) kind = intent.mode;
      if (intent?.count) count = intent.count;
      if (intent?.fairness_target) fairnessTarget = intent.fairness_target;
      if (intent?.area) areaFocus = intent.area;
      if (intent?.focus === 'city') {
        bbox = null;
        selectedRadiusKm = 0;
        selectedCenter = null;
      }
      if (intent?.focus === 'viewport') {
        bbox = getMapBoundsBBox();
      }
      if (intent?.focus === 'lasso') {
        bbox = lassoBbox;
        selectedRadiusKm = 0;
        selectedCenter = null;
        lassoRequested = true;
      }
      if (intent?.rationale) rationale = intent.rationale;
    }

    if (focusOverride === 'city') {
      bbox = null;
      selectedRadiusKm = 0;
      selectedCenter = null;
    }
    if (focusOverride === 'viewport') {
      bbox = getMapBoundsBBox();
    }
    if (focusOverride === 'lasso') {
      bbox = lassoBbox;
      selectedRadiusKm = 0;
      selectedCenter = null;
      lassoRequested = true;
    }
    if (Array.isArray(overrides.categories) && overrides.categories.length) {
      categories = overrides.categories;
    }

    if (lassoRequested && !bbox) {
      throw new Error('Draw a what-if lasso selection first.');
    }

    // Force city-wide exact scope for LLM suggestions: full city candidate space under current model.
    bbox = null;
    selectedRadiusKm = 0;
    selectedCenter = null;
    areaFocus = 'any';

    whatIfLastSuggestionConfig = {
      categories: [...categories],
      kind,
      count,
      bbox,
      center: selectedCenter,
      radiusKm: selectedRadiusKm,
      fairnessTarget,
      fairnessCategories: categories.length ? [...categories] : [...ALL_CATEGORIES],
      areaFocus
    };
    const suggestions = await computeWhatIfSuggestions({
      categories,
      kind,
      count,
      bbox,
      center: selectedCenter,
      radiusKm: selectedRadiusKm,
      fairnessTarget,
      fairnessCategories: categories.length ? categories : ALL_CATEGORIES,
      areaFocus
    });
    if (!suggestions.length) {
      throw new Error('No feasible city-wide suggestions found under current filters/model.');
    }
    setWhatIfSuggestions(suggestions);
    const summaryText = formatWhatIfSuggestionSummary(suggestions);
    const verifyHint = ' Verify optimality can run bounded search for multi-location requests.';
    updateWhatIfSuggestionUI(
      rationale ? `${summaryText} ${rationale}${verifyHint}` : `${summaryText}${verifyHint}`,
      { hasSuggestion: true }
    );
    return suggestions;
  } catch (err) {
    updateWhatIfSuggestionUI(err?.message || 'Unable to generate suggestions.', { isError: true });
    throw err;
  }
}

function setPOISymbolsVisibility(enabled) {
  showPOISymbols = !!enabled;
  const poiSymbolsToggle = document.getElementById('poiSymbolsToggle');
  if (poiSymbolsToggle) poiSymbolsToggle.checked = !!enabled;
  poiStyleTick++;
  updateLayers();
}

function createWhatIfSuggestionLayers() {
  if (!whatIfSuggestions.length) return [];
  const points = whatIfSuggestions.map((suggestion, idx) => turf.point(suggestion.location, {
    label: `${idx + 1}`,
    kind: suggestion.kind
  }));
  return [
    new deck.ScatterplotLayer({
      id: 'whatif-suggestion-points',
      data: points,
      pickable: false,
      getPosition: f => f.geometry.coordinates,
      getRadius: 14,
      radiusUnits: 'pixels',
      filled: true,
      getFillColor: WHATIF_SUGGESTION_COLOR,
      stroked: true,
      getLineColor: [30, 30, 30],
      getLineWidth: 2,
      parameters: { depthTest: false },
      updateTriggers: { data: [whatIfSuggestionTick] }
    }),
    new deck.TextLayer({
      id: 'whatif-suggestion-labels',
      data: points,
      getPosition: f => f.geometry.coordinates,
      getText: f => f.properties?.label || '',
      getSize: 12,
      getColor: [255, 255, 255],
      getTextAnchor: 'middle',
      getAlignmentBaseline: 'top',
      parameters: { depthTest: false },
      updateTriggers: { data: [whatIfSuggestionTick] }
    })
  ];
}

function createMockBuildingPolygon(lngLat, sizeMeters = 12, shape = 'random', variationPct = 50) {
  const center = turf.point(lngLat);
  const half = Math.max(4, sizeMeters / 2);
  const v = Math.max(0, Math.min(100, variationPct)) / 100; // 0..1

  // Helper: offset a point from center by (dx, dy) in meters using bearing + distance
  const offset = (dxM, dyM) => {
    const dist = Math.sqrt(dxM * dxM + dyM * dyM);
    if (dist < 0.01) return lngLat;
    const bearing = (Math.atan2(dxM, dyM) * 180) / Math.PI;
    return turf.destination(center, dist, bearing, { units: 'meters' }).geometry.coordinates;
  };

  // Pick a shape: at v=0 always 'rect', at v=1 fully random across all shapes
  const shapes = ['rect', 'rect_wide', 'L', 'T', 'U'];
  let pick;
  if (shape !== 'random') {
    pick = shapes.includes(shape) ? shape : 'rect';
  } else if (Math.random() >= v) {
    // Below the variation threshold → default rect
    pick = 'rect';
  } else {
    // Above threshold → pick from all shapes (including rect for natural mix)
    pick = shapes[Math.floor(Math.random() * shapes.length)];
  }

  // Rotation: at v=0 all buildings face north (angle=0), at v=1 fully random
  const angle = v * Math.random() * 2 * Math.PI;
  const rot = (dx, dy) => {
    const rx = dx * Math.cos(angle) - dy * Math.sin(angle);
    const ry = dx * Math.sin(angle) + dy * Math.cos(angle);
    return offset(rx, ry);
  };

  // Aspect ratio jitter: at v=0 all identical proportions, at v=1 up to ±30% variation
  const aspectJitter = 1 + (Math.random() - 0.5) * 0.6 * v;  // 0.7..1.3 at full variation

  let ring;
  const w = half * aspectJitter;                  // half-width (jittered)
  const d = half * (0.85 / aspectJitter);         // half-depth (inverse jitter keeps area ~constant)
  const arm = half * (0.35 + 0.1 * v * Math.random()); // arm thickness varies slightly

  switch (pick) {
    case 'rect_wide': {
      // wider rectangle
      const ww = half * 1.4;
      const dd = half * 0.6;
      ring = [rot(-ww,-dd), rot(ww,-dd), rot(ww,dd), rot(-ww,dd)];
      break;
    }
    case 'L': {
      ring = [
        rot(-w, -d), rot(w, -d), rot(w, -d + arm),
        rot(-w + arm, -d + arm), rot(-w + arm, d), rot(-w, d)
      ];
      break;
    }
    case 'T': {
      const hw = arm / 2;
      ring = [
        rot(-w, d), rot(-w, d - arm), rot(-hw, d - arm),
        rot(-hw, -d), rot(hw, -d), rot(hw, d - arm),
        rot(w, d - arm), rot(w, d)
      ];
      break;
    }
    case 'U': {
      ring = [
        rot(-w, -d), rot(w, -d), rot(w, d),
        rot(w - arm, d), rot(w - arm, -d + arm),
        rot(-w + arm, -d + arm), rot(-w + arm, d), rot(-w, d)
      ];
      break;
    }
    default: { // 'rect'
      ring = [rot(-w, -d), rot(w, -d), rot(w, d), rot(-w, d)];
    }
  }

  ring.push(ring[0]); // close the ring
  return { type: 'Polygon', coordinates: [ring] };
}

function getWhatIfAvoidLayerIds() {
  if (!map?.getStyle) return [];
  const style = map.getStyle();
  const layers = style?.layers || [];
  if (!layers.length) return [];
  const keywords = WHATIF_AVOID_LAYER_KEYWORDS;
  return layers
    .filter((layer) => {
      if (!layer || !layer.id) return false;
      if (!['fill', 'line', 'symbol'].includes(layer.type)) return false;
      const hay = `${layer.id} ${layer['source-layer'] || ''}`.toLowerCase();
      return keywords.some((k) => hay.includes(k));
    })
    .map((layer) => layer.id);
}

async function ensureForbiddenZones() {
  if (forbiddenZonesFC) return forbiddenZonesFC;
  const cityKey = normalizeCityKey(lastCityName);
  if (!cityKey) return null;
  try {
    const r = await fetch(`assets/data/cities/${cityKey}/forbidden_zones.geojson`, { cache: 'force-cache' });
    if (!r.ok) return null;
    forbiddenZonesFC = await r.json();
  } catch {
    forbiddenZonesFC = null;
  }
  return forbiddenZonesFC;
}

function isLngLatInForbiddenZone(lngLat) {
  if (!forbiddenZonesFC?.features?.length) return false;
  const pt = turf.point(lngLat);
  return forbiddenZonesFC.features.some(f => {
    try { return turf.booleanPointInPolygon(pt, f); } catch { return false; }
  });
}

function createWhatIfBuilding(lngLat, categoryOverride = null) {
  if (!baseCityFC?.features?.length) return null;
  const canonical = categoryOverride || whatIfType || ALL_CATEGORIES[0] || 'grocery';
  const point = turf.point(lngLat);
  const polygon = turf.buffer(point, 25, { units: 'meters' });
  polygon.properties = {
    name: `What-if ${prettyPOIName(canonical)}`,
    built_year: new Date().getFullYear(),
    __whatIf: true,
    __whatIfAdded: true
  };
  applyPOITags(polygon.properties, canonical);
  baseCityFC.features.push(polygon);
  if (newbuildsFC?.features) newbuildsFC.features.push(polygon);
  refreshBuildingTypeDropdown();
  return polygon;
}

function applyMockBuildingTags(props, type, { floors, floorHeight, footprint } = {}) {
  if (!props) return;
  const label = mockTypeLabel(type);
  props.__whatIf = true;
  props.__whatIfAdded = true;
  props.__whatIfMock = true;
  props.__whatIfMockType = type;
  props.name = `What-if ${label}`;
  props.built_year = new Date().getFullYear();

  // Height & floor metadata
  const nFloors = floors ?? whatIfMockFloors;
  const fHeight = floorHeight ?? whatIfMockFloorHeight;
  props.__mockFloors = nFloors;
  props.__mockFloorHeight = fHeight;
  props.__mockFootprint = footprint ?? null;
  props.height_m = nFloors * fHeight;

  // Population proxy: more floors + larger footprint → more residents/users
  // This feeds into equity weighting in the gravity model
  const fpArea = footprint ? (footprint * footprint) : 144; // fallback ~12m²
  props.__mockCapacity = Math.round(nFloors * (fpArea / 80)); // ~1 person per 80 m² per floor

  if (type === 'residential') {
    props.building = 'residential';
    props.category = 'residential';
    delete props.whatif_poi;
    return;
  }

  applyPOITags(props, type);
}

function getWhatIfMockBuildings() {
  return baseCityFC?.features?.filter((feat) => feat?.properties?.__whatIfMock) || [];
}

function createWhatIfMockBuildingsLayer() {
  const data = getWhatIfMockBuildings();
  if (!data.length) return null;
  return new deck.GeoJsonLayer({
    id: 'whatif-mock-outline',
    data,
    pickable: true,
    filled: true,
    stroked: false,
    extruded: true,
    opacity: 1,
    material: BUILDING_MATERIAL,
    getElevation: f => {
      const h = f.properties?.height_m;
      return (Number.isFinite(h) ? h : 10) * heightScale;
    },
    getFillColor: f => mockBuildingFillColor(f),
    updateTriggers: {
      getFillColor: [fairActive, fairCategory, fairRecolorTick, changeLogTick, drSelectionTick, drHasSelection, changeCompareBaseline, pinnedChangeId],
      getElevation: [heightScale]
    },
    onClick: handleClick
  });
}


function showWhatIfBlockedToast(msg) {
  const id = 'whatif-blocked-toast';
  let el = document.getElementById(id);
  if (!el) {
    el = document.createElement('div');
    el.id = id;
    el.className = 'toast align-items-center text-bg-warning border-0 position-fixed bottom-0 start-50 translate-middle-x mb-4';
    el.style.zIndex = '9999';
    el.setAttribute('role', 'alert');
    el.setAttribute('aria-live', 'assertive');
    el.innerHTML = '<div class="d-flex"><div class="toast-body fw-semibold"></div>' +
      '<button type="button" class="btn-close me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button></div>';
    document.body.appendChild(el);
  }
  el.querySelector('.toast-body').textContent = msg;
  bootstrap.Toast.getOrCreateInstance(el, { delay: 3000 }).show();
}

async function handleWhatIfMapClick(event) {
  if (whatIfMode !== 'add' || !event?.lngLat) return;
  if (mapLasso.active) return;
  if (whatIfLasso.active) return;
  if (Date.now() - lastFeatureClickAt < 260) return;

  const lngLat = [event.lngLat.lng, event.lngLat.lat];

  await ensureForbiddenZones();
  if (isLngLatInForbiddenZone(lngLat)) {
    showWhatIfBlockedToast("Can't place a building here — water or natural area.");
    return;
  }
  const categoryOverride = getActiveWhatIfCategory();

  const beforeCatGini = extractDisplayedGiniValue(giniOut?.textContent || '');
  const beforeOverallGini = Number.isFinite(overallGini) ? overallGini : null;
  const scoreSnapshot = captureScoreSnapshot();

  const feat = createWhatIfBuilding(lngLat, categoryOverride);
  if (!feat) return;
  showPopup(feat, lngLat);
  showGlobalSpinner('Adding building & recomputing…');
  await waitForSpinnerPaint();

  const recomputeRes = await recomputeFairnessAfterWhatIf();

  const afterCatGini = Number.isFinite(recomputeRes?.categoryGini)
    ? recomputeRes.categoryGini
    : extractDisplayedGiniValue(giniOut?.textContent || '');
  const afterOverallGini = Number.isFinite(recomputeRes?.overallGini)
    ? recomputeRes.overallGini
    : (Number.isFinite(overallGini) ? overallGini : null);

  const nextId = changeLogIdCounter;
  const colorPairs = applyDeltaColorsFromSnapshot(scoreSnapshot, nextId);

  // Keep the exact diverging change-map palette from applyDeltaColorsFromSnapshot.
  // Do NOT force a custom green color for new buildings, otherwise meso/macro
  // aggregation can inherit green and conflict with the red↔blue legend.

  // Filter out near-zero-delta buildings to avoid painting everything gray.
  // Keep only meaningful diverging colors for aggregation/highlight.
  const meaningfulPairs = colorPairs.filter(p =>
    !p.color || p.color[3] === undefined ||
    !(p.color[0] === 247 && p.color[1] === 247 && p.color[2] === 247)
  );

  recordWhatIfChange({
    action: 'add_building',
    description: `Added ${prettyPOIName(categoryOverride)} building`,
    category: categoryOverride || null,
    beforeGini: beforeCatGini,
    afterGini: afterCatGini,
    beforeOverall: beforeOverallGini,
    afterOverall: afterOverallGini,
    affectedFeatures: meaningfulPairs
  });

  hideGlobalSpinner();
  updateLayers();
}

function ensurePopupInView(popup, padding = 24) {
  const popupEl = popup?.getElement?.();
  const mapEl = map?.getContainer?.();
  if (!popupEl || !mapEl) return;

  const popupRect = popupEl.getBoundingClientRect();
  const mapRect = mapEl.getBoundingClientRect();

  const leftOverflow = (mapRect.left + padding) - popupRect.left;
  const rightOverflow = popupRect.right - (mapRect.right - padding);
  const topOverflow = (mapRect.top + padding) - popupRect.top;
  const bottomOverflow = popupRect.bottom - (mapRect.bottom - padding);

  let dx = 0;
  let dy = 0;

  if (rightOverflow > 0) dx = rightOverflow;
  else if (leftOverflow > 0) dx = -leftOverflow;

  if (bottomOverflow > 0) dy = bottomOverflow;
  else if (topOverflow > 0) dy = -topOverflow;

  if (dx !== 0 || dy !== 0) {
    map.panBy([dx, dy], { duration: 280 });
  }
}

function showPopup(payload, atLngLat) {
  closePopup();
  const feature = payload?.type === 'Feature' ? payload : null;
  const props = feature ? (feature.properties || {}) : (payload || {});
  const rows = [];
  // Skip rows when the value is empty/null/unknown so we don't show
  // "Built year: —" or "Name: —" — the user explicitly asked for those
  // blanks to disappear from the popup.
  const isEmpty = (v) =>
    v === null || v === undefined || v === '' ||
    (typeof v === 'string' && /^(?:—|-|n\/a|unknown|null)$/i.test(v.trim()));
  const add = (k, v) => {
    if (isEmpty(v)) return;
    rows.push(`<tr><th>${k}</th><td>${fmtValue(v)}</td></tr>`);
  };

  const isSingle = fairActive && fairCategory && fairCategory !== 'mix';
  const nearestDist = props?.fair?.nearest_dist_m;
  const isSelfPOI =
    isSingle &&
    (
      buildingMatchesPOI(props, fairCategory) ||
      (Number.isFinite(nearestDist) && nearestDist <= SELF_POI_EPS_M)
    );

  // Fairness shown as a coloured chip in the popup header (built below).
  // Per-category breakdown, distance and walk-time live in the right
  // inspector now, so they're no longer added as popup rows.
  const fairScore = (fairActive || props?.__whatIfMock) && Number.isFinite(props?.fair?.score)
    ? props.fair.score : null;
  const fairLabel = fairActive
    ? (fairCategory === 'mix' ? 'Custom mix' : prettyPOIName(fairCategory))
    : 'Overall';

  const popupCategoryRaw = (props?.category || props?.category_label || props?.objekttyp || '').toString().trim();
  const popupCategoryLabel = (!popupCategoryRaw || popupCategoryRaw.toLowerCase() === 'unknown')
    ? 'unknown/residential'
    : popupCategoryRaw;

  add('Name', props?.name || props?.byggnadsnamn1);
  add('Type', props?.andamal1 || props?.objekttyp);
  add('Category', popupCategoryLabel !== 'unknown' && popupCategoryLabel !== 'Other / unknown' ? popupCategoryLabel : null);
  add('Built year', getBuiltYear(props));
  if (props.__whatIfMock) {
    if (Number.isFinite(props.__mockFloors)) add('Floors', props.__mockFloors);
    if (Number.isFinite(props.height_m)) add('Height', `${props.height_m.toFixed(1)} m`);
    if (Number.isFinite(props.__mockCapacity)) add('Est. capacity', `${props.__mockCapacity} people`);
  }

  const title = props?.name || props?.byggnadsnamn1 || props?.andamal1?.split(';')[0] || popupCategoryLabel || props?.objekttyp || 'Building';
  const geomType = feature?.geometry?.type || '';
  const isBuilding = geomType === 'Polygon' || geomType === 'MultiPolygon';
  const showWhatIfEdit = whatIfMode === 'edit' && isBuilding;
  const editControls = showWhatIfEdit
     ? `
      <div class="mt-2 pt-2 border-top">
        <div class="small text-muted mb-1">What-if: change building type</div>
        <select class="form-select form-select-sm" data-whatif="type">
          ${buildWhatIfTypeOptions(detectPOICategoryFromProps(props) || whatIfType)}
        </select>
        <div class="d-flex gap-2 mt-2">
          <button class="btn btn-sm btn-warning" data-whatif="apply">Apply</button>
          <button class="btn btn-sm btn-outline-secondary" data-whatif="close">Close</button>
        </div>
      </div>`
    : '';
  // Fairness chip (header, right of the title) — colour-coded against the
  // same ramp the map uses. We render via colorFromScore so the popup tracks
  // the legend without us hard-coding bins.
  let fairChip = '';
  if (fairScore != null) {
    const c = (typeof colorFromScore === 'function') ? colorFromScore(fairScore) : null;
    const bg = Array.isArray(c) ? `rgb(${c[0]}, ${c[1]}, ${c[2]})` : '#3A6EA5';
    fairChip = `<span class="popup-fair-chip" title="${fairLabel} fairness" style="background:${bg};">${Math.round(fairScore * 100)}%</span>`;
  }
  const html = `
    <div class="building-popup">
      <div class="popup-header">
        <div class="popup-title">${title}</div>
        ${fairChip}
      </div>
      ${rows.length ? `<table class="popup-table">${rows.join('')}</table>` : ''}
      ${editControls}
    </div>`;
    currentPopup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: false,
    offset: [0, -8],
    anchor: 'bottom',
    maxWidth: '520px',
    // focusAfterOpen scrolls the focused popup into view, which the user
    // perceives as a zoom/pan jump. Keep the camera exactly where it was.
    focusAfterOpen: false,
    className: 'building-popup-pop'
  }).setLngLat(atLngLat).setHTML(html).addTo(map);

  if (showWhatIfEdit && currentPopup && feature) {
    const popupEl = currentPopup.getElement();
    const selectEl = popupEl?.querySelector?.('[data-whatif="type"]');
    const applyBtn = popupEl?.querySelector?.('[data-whatif="apply"]');
    const closeBtn = popupEl?.querySelector?.('[data-whatif="close"]');
    if (selectEl) selectEl.value = detectPOICategoryFromProps(props) || whatIfType;
    applyBtn?.addEventListener('click', () => {
      const nextType = selectEl?.value || buildingTypeOf(feature);
      applyBuildingTypeChange(feature, nextType);
    });
    closeBtn?.addEventListener('click', () => closePopup());
  }
}

