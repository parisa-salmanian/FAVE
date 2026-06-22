// Population-weighted per-group accessibility equity (all cities).
//
// The Växjö-only income-quintile analysis in views/equity.js answers "are
// low-income districts underserved?" but needs a bespoke gender-income JSON and
// only runs for one city. This module asks the same equity question for ANY
// city, weighted by the residents who actually live there, across the Phase-3
// baked DESO demographics — not just income, but social need and the elderly.
//
// For each residential building we take value = overall accessibility score and
// weight = estimated residents (epiBuildingPopMap); its group on a dimension is
// the population-weighted tercile of that building's DESO attribute. We then
// report each group's population-weighted mean accessibility + population share,
// the disadvantage gap between the extreme groups, and a population-weighted Gini
// over all residents. Pure runtime data (DESO props + pop map); no network.

const EQUITY_GROUP_DIMS = [
  // gapHigherIsConcern: when true, a positive "advantaged − attribute-high" gap
  // means the at-risk group (high need / many elderly / low income) is the worse
  // served, i.e. an equity concern.
  { key: 'needZ',      label: 'Social need',  bands: ['Low need', 'Medium', 'High need'],          atRisk: 'high' },
  { key: 'elder_frac', label: 'Elderly share', bands: ['Fewer elderly', 'Medium', 'More elderly'], atRisk: 'high' },
  { key: 'income',     label: 'Income',       bands: ['Lowest income', 'Middle', 'Highest income'], atRisk: 'low'  },
];

function _egAccessScore(props) {
  const s = (props?.fair_overall && Number.isFinite(props.fair_overall.score)) ? props.fair_overall.score
          : (props?.fair && Number.isFinite(props.fair.score)) ? props.fair.score
          : null;
  return Number.isFinite(s) ? Math.max(0, Math.min(1, s)) : null;
}

// Population-weighted quantile of {value, weight} samples (value-sorted).
function _egWeightedQuantile(samples, q) {
  if (!samples.length) return null;
  const sorted = samples.slice().sort((a, b) => a.value - b.value);
  const total = sorted.reduce((s, x) => s + x.weight, 0);
  if (total <= 0) return sorted[Math.floor((sorted.length - 1) * q)].value;
  const target = q * total;
  let cum = 0;
  for (const x of sorted) { cum += x.weight; if (cum >= target) return x.value; }
  return sorted[sorted.length - 1].value;
}

// Population-weighted Gini of a set of {value, weight} (value in 0..1). Operates
// on (1 − value) so that LOW accessibility reads as HIGH inequality, matching the
// fairness-Gini convention used everywhere else in the app.
function _egWeightedGini(samples) {
  const pts = samples.map(s => ({ x: 1 - s.value, w: s.weight }))
    .filter(p => p.w > 0).sort((a, b) => a.x - b.x);
  const W = pts.reduce((s, p) => s + p.w, 0);
  if (W <= 0) return null;
  const mean = pts.reduce((s, p) => s + p.x * p.w, 0) / W;
  if (mean <= 0) return 0;
  // Weighted Gini via cumulative population: G = 1 − Σ w_i (S_{i-1} + S_i) / S_n,
  // with S the weighted-value cumulative sum.
  let cumW = 0, cumXW = 0, prevXW = 0, area = 0;
  for (const p of pts) {
    prevXW = cumXW;
    cumXW += p.x * p.w;
    area += p.w * (prevXW + cumXW);
    cumW += p.w;
  }
  return Math.max(0, Math.min(1, 1 - area / (W * cumXW)));
}

// Build the per-group equity result for the current city, or null if the
// runtime demographics / fairness aren't ready.
function computeGroupEquity() {
  const feats = (typeof baseCityFC !== 'undefined' && baseCityFC) ? baseCityFC.features : null;
  if (!feats || !feats.length) return null;
  const propsMap = (typeof _demoLensPropsMap === 'function') ? _demoLensPropsMap() : null;
  if (!propsMap || !propsMap.size) return null;
  // Same population source as fairness demand: synthetic per-building residents
  // first (all cities), else the DESO floor-area proxy.
  const popMap = (typeof synthBuildingPopMap !== 'undefined' && synthBuildingPopMap && synthBuildingPopMap.size)
    ? synthBuildingPopMap
    : ((typeof epiBuildingPopMap !== 'undefined' && epiBuildingPopMap) ? epiBuildingPopMap : null);

  // Collect one record per residential building with an access score + DESO.
  const records = [];
  let totalPop = 0;
  for (let idx = 0; idx < feats.length; idx++) {
    const props = feats[idx].properties || {};
    const access = _egAccessScore(props);
    if (access == null) continue;
    const code = props.__deso;
    if (code == null) continue;
    const deso = propsMap.get(code);
    if (!deso) continue;
    const w = popMap ? (popMap.get(idx) || 0) : 1;
    if (!(w > 0)) continue;        // population-weighted ⇒ only residents count
    records.push({ access, deso, w });
    totalPop += w;
  }
  if (records.length < 10 || totalPop <= 0) return null;

  const overallAccess = records.reduce((s, r) => s + r.access * r.w, 0) / totalPop;
  const gini = _egWeightedGini(records.map(r => ({ value: r.access, weight: r.w })));

  const dims = EQUITY_GROUP_DIMS.map((dim) => {
    const samples = records
      .map(r => ({ value: Number(r.deso[dim.key]), weight: r.w, access: r.access }))
      .filter(s => Number.isFinite(s.value));
    if (samples.length < 6) return null;
    const t1 = _egWeightedQuantile(samples, 1 / 3);
    const t2 = _egWeightedQuantile(samples, 2 / 3);
    const buckets = [
      { label: dim.bands[0], pop: 0, sum: 0 },
      { label: dim.bands[1], pop: 0, sum: 0 },
      { label: dim.bands[2], pop: 0, sum: 0 },
    ];
    for (const s of samples) {
      const b = (s.value <= t1) ? 0 : (s.value <= t2) ? 1 : 2;
      buckets[b].pop += s.weight;
      buckets[b].sum += s.access * s.weight;
    }
    const groups = buckets.map(b => ({
      label: b.label,
      meanAccess: b.pop > 0 ? b.sum / b.pop : null,
      pop: b.pop,
      share: b.pop / totalPop,
    }));
    // Disadvantage gap: advantaged-group access − at-risk-group access. The
    // at-risk band is the top tercile (need/elderly) or bottom tercile (income).
    const atRisk = dim.atRisk === 'low' ? groups[0] : groups[2];
    const advantaged = dim.atRisk === 'low' ? groups[2] : groups[0];
    const gap = (Number.isFinite(advantaged.meanAccess) && Number.isFinite(atRisk.meanAccess))
      ? advantaged.meanAccess - atRisk.meanAccess : null;
    return { key: dim.key, label: dim.label, groups, gap, atRiskLabel: atRisk.label };
  }).filter(Boolean);

  return { city: (typeof EPI_DEMO !== 'undefined' && EPI_DEMO) ? EPI_DEMO.city : null,
           nBuildings: records.length, totalPop, overallAccess, gini, dims };
}

// Render the per-group equity block into #statsGroupEquity (inspector Stats tab).
function renderGroupEquity() {
  const host = document.getElementById('statsGroupEquity');
  if (!host) return;
  const res = computeGroupEquity();
  if (!res) {
    host.innerHTML = '<div class="empty">Run a fairness compute (with POIs) to see population-weighted equity across demographic groups.</div>';
    return;
  }
  const pct = v => Number.isFinite(v) ? `${Math.round(v * 100)}%` : '—';
  const giniTxt = Number.isFinite(res.gini) ? res.gini.toFixed(3) : '—';
  const head = `
    <div class="kv"><span class="kv-key">Residents weighted</span><span class="kv-val">${Math.round(res.totalPop).toLocaleString()}</span></div>
    <div class="kv"><span class="kv-key" title="Population-weighted accessibility inequality (1 = unequal). Weighted by residents, over 1 − accessibility.">Equity Gini (pop-weighted)</span><span class="kv-val">${giniTxt}</span></div>
    <div class="kv"><span class="kv-key">Mean access (pop-weighted)</span><span class="kv-val">${pct(res.overallAccess)}</span></div>`;

  const dimHtml = res.dims.map((d) => {
    const rows = d.groups.map((g) => {
      const w = Number.isFinite(g.meanAccess) ? Math.max(2, Math.min(100, g.meanAccess * 100)) : 0;
      const fill = (typeof colorFromScore === 'function' && Number.isFinite(g.meanAccess))
        ? (() => { const c = colorFromScore(g.meanAccess); return `rgb(${c[0]},${c[1]},${c[2]})`; })()
        : 'var(--insp-surface-2)';
      return `
        <div class="bar-row" title="${g.label}: ${pct(g.meanAccess)} mean access · ${pct(g.share)} of residents">
          <span class="label">${g.label} <span class="insp-tiny muted">${pct(g.share)}</span></span>
          <div class="bar-track"><div class="bar-fill" style="width:${w}%;background:${fill}"></div></div>
          <span class="num">${pct(g.meanAccess)}</span>
        </div>`;
    }).join('');
    // Positive gap = the at-risk group is worse served (equity concern, red);
    // negative = at-risk group is actually better served; near zero = balanced.
    const gapPts = Number.isFinite(d.gap) ? Math.abs(d.gap * 100).toFixed(0) : null;
    let gapNote;
    if (gapPts == null) {
      gapNote = '<span class="insp-tiny muted">—</span>';
    } else if (d.gap > 0.03) {
      gapNote = `<span class="insp-tiny" style="color:#d7191c;font-weight:700" title="The ${d.atRiskLabel.toLowerCase()} group has lower accessibility — an equity concern.">${d.atRiskLabel} underserved by ${gapPts} pts</span>`;
    } else if (d.gap < -0.03) {
      gapNote = `<span class="insp-tiny muted" title="The ${d.atRiskLabel.toLowerCase()} group actually has higher accessibility here.">${d.atRiskLabel} better served (+${gapPts} pts)</span>`;
    } else {
      gapNote = '<span class="insp-tiny muted">balanced</span>';
    }
    return `
      <div style="margin-top:8px">
        <div class="insp-tiny" style="display:flex;justify-content:space-between;align-items:baseline">
          <strong>${d.label}</strong>${gapNote}
        </div>
        <div class="bar-list">${rows}</div>
      </div>`;
  }).join('');

  host.innerHTML = head + dimHtml;
}
