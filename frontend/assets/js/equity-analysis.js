/* ==========================================================================
   Equity Analysis Module — Income-quintile × Fairness × Scenario comparison
   Depends on: main.js globals (baseCityFC, districtFC, fairnessTravelMode,
               ALL_CATEGORIES, computeOverallFairness, refreshDistrictScores,
               haversineMeters, scoreFromTimeSeconds, gini, generalizedEntropy,
               estimateTravelTimeSecondsFromMeters, districtNameOf,
               regsoCodeFromProps, fetchPOIs, collectPOICoordsForCategory,
               prettyPOIName, ensureDistrictData, turf)
   ========================================================================== */
 
/* ---------- constants & state ------------------------------------------- */
const DISTRICT_INCOME_URLS = [
  'assets/data/gender-district-income-vaxjo.json',
  './assets/data/gender-district-income-vaxjo.json',
  '../assets/data/gender-district-income-vaxjo.json'
];
const EQUITY_15MIN_SECONDS = 900;
const EQUITY_QUINTILE_COUNT = 5;
const EQUITY_SCENARIO_BUDGET = 3;
const EQUITY_ROBUSTNESS_JITTER_M = 200;
const EQUITY_ROBUSTNESS_STRICT_FACTOR = 0.8;
const EQUITY_DEFAULT_CHECKED = ['grocery', 'hospital', 'school_primary'];

let districtIncomeMap = null;
let districtIncomePromise = null;
let equityAnalysisResult = null;

/* ---------- income loader (tries multiple paths) ------------------------ */
async function ensureDistrictIncomeData() {
  if (districtIncomeMap) return districtIncomeMap;
  if (districtIncomePromise) return districtIncomePromise;
  districtIncomePromise = (async () => {
    let raw = null;
    for (const url of DISTRICT_INCOME_URLS) {
      try {
        const r = await fetch(url);
        if (r.ok) { raw = await r.json(); break; }
      } catch (_) { /* try next */ }
    }
    if (!raw) throw new Error('Income data not found. Place gender-district-income-vaxjo.json in assets/data/');
    districtIncomeMap = {};
    for (const row of raw.data) {
      const regso = row.key[0].split('_')[0];
      const gender = row.key[2];
      const mean_tkr = parseFloat(row.values[0]);
      const count = parseInt(row.values[1], 10);
      if (!districtIncomeMap[regso]) districtIncomeMap[regso] = {};
      districtIncomeMap[regso][gender] = { mean_tkr, count };
    }
    return districtIncomeMap;
  })();
  districtIncomePromise.catch(() => { districtIncomePromise = null; });
  return districtIncomePromise;
}
 
function lookupDistrictIncome(regsoCode) {
  if (!districtIncomeMap || !regsoCode) return null;
  const direct = districtIncomeMap[regsoCode];
  if (direct) return direct;
  const padded = regsoCode.length === 7 ? '0' + regsoCode : regsoCode;
  return districtIncomeMap[padded] || null;
}
 
/* ---------- quintile helpers -------------------------------------------- */
function assignQuintiles(arr, valueFn, propName) {
  const sorted = arr.slice().sort((a, b) => valueFn(a) - valueFn(b));
  const n = sorted.length;
  const qSize = Math.ceil(n / EQUITY_QUINTILE_COUNT);
  sorted.forEach((item, i) => {
    const q = Math.min(Math.floor(i / qSize) + 1, EQUITY_QUINTILE_COUNT);
    item[propName] = q;
  });
}
 
/* ---------- 15-min metrics per district --------------------------------- */
function compute15MinMetrics(districtRows, categories) {
  // Walking 5 km/h → 15 min = 1.25 km
  const WALK_15MIN_M = 1250;

  for (const d of districtRows) {
    const buildings = d.buildings || [];
    if (!buildings.length) {
      d.thresholdShare = 0;
      d.serviceCount = 0;
      d.catMetrics = {};
      continue;
    }
    const catMetrics = {};
    let totalServicesReachable = 0;

    for (const cat of categories) {
      let within = 0;
      let scoreSum = 0;

      for (const b of buildings) {
        const props = b.properties || {};
        const fm = props.fair_multi || {};
        const entry = fm[cat];

        // 1. Try explicit time_min from fairness computation
        const timeMin = entry?.time_min;
        if (Number.isFinite(timeMin)) {
          scoreSum += entry?.score ?? 0;
          if (timeMin <= 15) within++;
          continue;
        }

        // 2. Try explicit dist_m
        const distM = entry?.dist_m;
        if (Number.isFinite(distM)) {
          scoreSum += entry?.score ?? 0;
          if (distM <= WALK_15MIN_M) within++;
          continue;
        }

        // 3. Try per-category score (IF-City stores this differently)
        const catScore = entry?.score
          ?? entry?.access  // IF-City sometimes uses 'access'
          ?? null;
        if (Number.isFinite(catScore)) {
          scoreSum += catScore;
          if (catScore >= 0.4) within++;  // score ≥ 0.4 ≈ within 15-min reach
          continue;
        }

        // 4. Fall back to overall score as proxy
        const overall = props.fair_overall?.score ?? props.fair?.score ?? 0;
        scoreSum += overall;
        if (overall >= 0.4) within++;
      }

      const pct = buildings.length ? within / buildings.length : 0;
      catMetrics[cat] = {
        meanScore: buildings.length ? scoreSum / buildings.length : 0,
        within15minPct: pct
      };
      totalServicesReachable += pct;
    }

    const catsReachable = Object.values(catMetrics).filter(c => c.within15minPct >= 0.5).length;
    d.thresholdShare = catsReachable / Math.max(1, categories.length);
    d.serviceCount = categories.length ? totalServicesReachable / categories.length : 0;
    d.catMetrics = catMetrics;
  }
  return districtRows;
}
 
/* ---------- inequality metrics for a track ------------------------------ */
function computeTrackInequality(rows, quintileProp, valueFn, weightFn) {
  const q1Rows = rows.filter(r => r[quintileProp] === 1);
  const q5Rows = rows.filter(r => r[quintileProp] === EQUITY_QUINTILE_COUNT);
  const mean = arr => arr.length ? arr.reduce((s, r) => s + valueFn(r), 0) / arr.length : 0;

  const q1Mean = mean(q1Rows);
  const q5Mean = mean(q5Rows);
  const gap = q5Mean - q1Mean;
  const ratio = q5Mean > 0 ? q1Mean / q5Mean : null;

  const wmean = arr => {
    let ws = 0, wt = 0;
    arr.forEach(r => { const w = weightFn(r); ws += valueFn(r) * w; wt += w; });
    return wt > 0 ? ws / wt : 0;
  };
  const wQ1Mean = wmean(q1Rows);
  const wQ5Mean = wmean(q5Rows);
  const wGap = wQ5Mean - wQ1Mean;
  const wRatio = wQ5Mean > 0 ? wQ1Mean / wQ5Mean : null;

  const allValues = rows.map(valueFn);
  const giniVal = gini(allValues.map(v => 1 - v));

  return { q1Mean, q5Mean, gap, ratio, wQ1Mean, wQ5Mean, wGap, wRatio, gini: giniVal };
}
 
/* ---------- build district rows with income + fairness ------------------- */
function buildDistrictAnalysisRows(categories) {
  if (!districtFC?.features?.length || !baseCityFC?.features?.length) return [];
 
  const pts = baseCityFC.features.map(f => {
    const c = turf.centroid(f).geometry.coordinates;
    return turf.point(c, { __ref: f });
  });
  const ptsFC = turf.featureCollection(pts);
 
  return districtFC.features.map((feat, idx) => {
    const props = feat.properties || {};
    const name = props.__districtName || districtNameOf(props, idx);
    const regso = regsoCodeFromProps(props);
    const income = lookupDistrictIncome(regso);
 
    const within = turf.pointsWithinPolygon(ptsFC, feat);
    const buildings = within.features.map(p => p.properties.__ref);
 
    const overallScores = buildings
      .map(b => b.properties?.fair_overall?.score)
      .filter(Number.isFinite);
    const meanFairness = overallScores.length
      ? overallScores.reduce((a, b) => a + b, 0) / overallScores.length
      : 0;
 
    const incomeTotalTkr = income?.['1+2']?.mean_tkr ?? null;
    const incomeMenTkr = income?.['1']?.mean_tkr ?? null;
    const incomeWomenTkr = income?.['2']?.mean_tkr ?? null;
    const population = income?.['1+2']?.count ?? buildings.length;
 
    return {
      feature: feat,
      name,
      regso,
      buildings,
      buildingCount: buildings.length,
      meanFairness,
      incomeTotalTkr,
      incomeMenTkr,
      incomeWomenTkr,
      population,
      centroid: turf.centroid(feat).geometry.coordinates
    };
  }).filter(d => d.incomeTotalTkr != null && d.buildingCount > 0);
}
 
/* ---------- scenario POI placement -------------------------------------- */
function pickDistrictsByQuintile(rows, quintileProp, targetQ, budget) {
  const targets = rows.filter(r => r[quintileProp] === targetQ);
  if (!targets.length) return [];
  // Sort by worst fairness first (most need)
  targets.sort((a, b) => a.meanFairness - b.meanFairness);
  const placements = [];
  for (let i = 0; i < budget; i++) {
    const district = targets[i % targets.length];
    // Place near centroid with small jitter
    const [lng, lat] = district.centroid;
    const jitter = () => (Math.random() - 0.5) * 0.002;
    placements.push({
      coord: [lng + jitter(), lat + jitter()],
      districtName: district.name,
      quintile: targetQ
    });
  }
  return placements;
}
 
function pickNeutralDemandPlacements(rows, quintileProp, budget) {
  const placements = [];
  const sorted = rows.slice().sort((a, b) => b.population - a.population);
  for (let i = 0; i < budget; i++) {
    const district = sorted[i % sorted.length];
    const [lng, lat] = district.centroid;
    const jitter = () => (Math.random() - 0.5) * 0.002;
    placements.push({
      coord: [lng + jitter(), lat + jitter()],
      districtName: district.name,
      quintile: district[quintileProp]
    });
  }
  return placements;
}
 
/* ---------- recompute accessibility with injected POIs ------------------- */
function recomputeDistrictAccessibility(rows, categories, extraPOICoords, catToBaseCoords) {
  // For each building, recompute scores considering extra POI locations
  for (const d of rows) {
    for (const b of d.buildings) {
      const props = b.properties || (b.properties = {});
      const cB = turf.centroid(b).geometry.coordinates;
      if (!props.fair_multi) props.fair_multi = {};
 
      for (const cat of categories) {
        const baseCoords = catToBaseCoords[cat] || [];
        const allCoords = [...baseCoords, ...extraPOICoords.map(p => ({ c: p.coord }))];
        let bestD = Infinity;
        for (const poi of allCoords) {
          const d2 = haversineMeters(cB, poi.c);
          if (d2 < bestD) bestD = d2;
        }
        const timeSeconds = estimateTravelTimeSecondsFromMeters(bestD, fairnessTravelMode);
        const score = scoreFromTimeSeconds(cat, timeSeconds, fairnessTravelMode);
        props.fair_multi[cat] = {
          score,
          dist_m: bestD,
          time_min: timeSeconds / 60
        };
      }
 
      const scores = Object.values(props.fair_multi).map(o => o.score).filter(Number.isFinite);
      if (scores.length) {
        props.fair_overall = { score: scores.reduce((a, b) => a + b, 0) / scores.length };
      }
    }
 
    // Recompute district mean
    const oScores = d.buildings
      .map(b => b.properties?.fair_overall?.score)
      .filter(Number.isFinite);
    d.meanFairness = oScores.length ? oScores.reduce((a, b) => a + b, 0) / oScores.length : 0;
  }
}
 
function countResidentsImproved(rowsBefore, rowsAfter) {
  let improved = 0;
  for (let i = 0; i < rowsBefore.length; i++) {
    if (rowsAfter[i].meanFairness > rowsBefore[i].meanFairness + 0.005) {
      improved += rowsAfter[i].population;
    }
  }
  return improved;
}
 
/* ---------- deep clone rows for scenario simulation --------------------- */
function cloneDistrictRows(rows) {
  return rows.map(r => {
    const buildings = r.buildings.map(b => {
      const clone = { ...b, properties: { ...b.properties } };
      if (clone.properties.fair_multi) {
        clone.properties.fair_multi = JSON.parse(JSON.stringify(clone.properties.fair_multi));
      }
      if (clone.properties.fair_overall) {
        clone.properties.fair_overall = { ...clone.properties.fair_overall };
      }
      return clone;
    });
    // Spread copies all properties including quintileTotal, quintileMen, quintileWomen
    return { ...r, buildings };
  });
}
 
/* ---------- robustness check -------------------------------------------- */
function jitterPlacements(placements, jitterMeters) {
  return placements.map(p => {
    const dLat = (jitterMeters / 111320) * (Math.random() - 0.5) * 2;
    const dLng = (jitterMeters / (111320 * Math.cos(p.coord[1] * Math.PI / 180))) * (Math.random() - 0.5) * 2;
    return { ...p, coord: [p.coord[0] + dLng, p.coord[1] + dLat] };
  });
}
 
function runScenarioVariant(rows, categories, placements, catToBaseCoords, quintileProp) {
  const cloned = cloneDistrictRows(rows);
  recomputeDistrictAccessibility(cloned, categories, placements, catToBaseCoords);
  compute15MinMetrics(cloned, categories);
  const valueFn = r => r.meanFairness;
  const weightFn = r => r.population;
  return computeTrackInequality(cloned, quintileProp, valueFn, weightFn);
}
 
/* ---------- main orchestrator ------------------------------------------- */
async function runEquityAnalysis(opts = {}) {
  const categories = opts.categories || ['grocery', 'hospital', 'school_primary'];
  const budget = opts.budget ?? EQUITY_SCENARIO_BUDGET;
  const year = opts.year || '2024';
 
  const statusEl = document.getElementById('equityAnalysisStatus');
  const setStatus = msg => { if (statusEl) statusEl.textContent = msg; };
 
  setStatus('Loading income data…');
  await ensureDistrictIncomeData();
 
  setStatus('Loading districts…');
  await ensureDistrictData();
 
  setStatus('Computing baseline fairness…');
  await computeOverallFairness(categories);
  await refreshDistrictScores();
 
  setStatus('Building district analysis rows…');
  const baseRows = buildDistrictAnalysisRows(categories);
  if (!baseRows.length) throw new Error('No districts with income + building data.');
 
  // Step 2: quintiles for 3 tracks (mutates rows directly)
  assignQuintiles(baseRows, r => r.incomeTotalTkr, 'quintileTotal');
  assignQuintiles(baseRows, r => r.incomeMenTkr ?? r.incomeTotalTkr, 'quintileMen');
  assignQuintiles(baseRows, r => r.incomeWomenTkr ?? r.incomeTotalTkr, 'quintileWomen');

  // Step 3: baseline 15-min metrics (mutates in place)
  setStatus('Computing 15-min accessibility…');
  compute15MinMetrics(baseRows, categories);
  const baselineEnriched = baseRows;

  // Step 4: baseline inequality
  const valueFn = r => r.meanFairness;
  const weightFn = r => r.population;
  const baselineTotal = computeTrackInequality(baselineEnriched, 'quintileTotal', valueFn, weightFn);
  const baselineMen = computeTrackInequality(baselineEnriched, 'quintileMen', valueFn, weightFn);
  const baselineWomen = computeTrackInequality(baselineEnriched, 'quintileWomen', valueFn, weightFn);
 
  // Fetch base POI coords for recomputation
  setStatus('Fetching POI coordinates…');
  const catToBaseCoords = {};
  for (const cat of categories) {
    try {
      const pois = await fetchPOIs(cat, baseCityFC);
      catToBaseCoords[cat] = collectPOICoordsForCategory(cat, pois);
    } catch (_) {
      catToBaseCoords[cat] = [];
    }
  }
 
  // Step 5: 3 scenarios
  setStatus('Running equity scenario (Q1 targeted)…');
  const placementsQ1 = pickDistrictsByQuintile(baselineEnriched, 'quintileTotal', 1, budget);
  const placementsNeutral = pickNeutralDemandPlacements(baselineEnriched, 'quintileTotal', budget);
  const placementsQ5 = pickDistrictsByQuintile(baselineEnriched, 'quintileTotal', EQUITY_QUINTILE_COUNT, budget);
 
  // Step 6: recompute for each scenario
  const scenarios = [
    { name: 'Q1-targeted (equity)', placements: placementsQ1 },
    { name: 'Neutral-demand (efficiency)', placements: placementsNeutral },
    { name: 'Q5-targeted (control)', placements: placementsQ5 }
  ];
 
  const scenarioResults = [];
  for (const sc of scenarios) {
    setStatus(`Computing scenario: ${sc.name}…`);
    const cloned = cloneDistrictRows(baselineEnriched);
    const beforeMeans = cloned.map(r => r.meanFairness);
 
    recomputeDistrictAccessibility(cloned, categories, sc.placements, catToBaseCoords);
    compute15MinMetrics(cloned, categories);

    const afterTotal = computeTrackInequality(cloned, 'quintileTotal', valueFn, weightFn);
    const afterMen = computeTrackInequality(cloned, 'quintileMen', valueFn, weightFn);
    const afterWomen = computeTrackInequality(cloned, 'quintileWomen', valueFn, weightFn);
 
    const residentsImproved = countResidentsImproved(
      baselineEnriched.map((r, i) => ({ ...r, meanFairness: beforeMeans[i] })),
      cloned
    );

    scenarioResults.push({
      name: sc.name,
      placements: sc.placements,
      total: afterTotal,
      men: afterMen,
      women: afterWomen,
      residentsImproved,
      districtRows: cloned
    });
  }
 
  // Step 7: pick best scenario
  const ranked = scenarioResults.slice().sort((a, b) => {
    // Primary: which scenario brings |gap| closest to zero (most equitable)
    const absGapA = Math.abs(a.total.gap);
    const absGapB = Math.abs(b.total.gap);
    if (Math.abs(absGapA - absGapB) > 0.01) return absGapA - absGapB; // smaller |gap| = better
    // Secondary: more residents improved
    return b.residentsImproved - a.residentsImproved;
  });
  const recommended = ranked[0];
 
  // Step 8: robustness checks
  setStatus('Running robustness checks…');
  const robustness = [];
 
  // 8a. Jitter the recommended placements
  const recommendedGap = recommended.total.gap;
  for (let trial = 0; trial < 3; trial++) {
    const jittered = jitterPlacements(recommended.placements, EQUITY_ROBUSTNESS_JITTER_M);
    const res = runScenarioVariant(baselineEnriched, categories, jittered, catToBaseCoords, 'quintileTotal');
    // "Holds" = jittered result is closer to zero gap than baseline (same improvement direction)
    const baselineAbsGap = Math.abs(baselineTotal.gap);
    const jitteredAbsGap = Math.abs(res.gap);
    const holds = jitteredAbsGap <= baselineAbsGap * 1.1; // within 10% tolerance
    robustness.push({
      type: `jitter-${trial + 1}`,
      gap: res.gap,
      ratio: res.ratio,
      gini: res.gini,
      sameConclusion: holds
    });
  }
 
  // 8b. Strict vs moderate threshold
  const strictCategories = categories; // same cats, different scoring
  const strictCloned = cloneDistrictRows(baselineEnriched);
  recomputeDistrictAccessibility(strictCloned, strictCategories, recommended.placements, catToBaseCoords);
  // Apply stricter scoring: multiply scores by 0.8 (stricter definition)
  for (const d of strictCloned) {
    for (const b of d.buildings) {
      const fm = b.properties?.fair_multi || {};
      for (const cat of categories) {
        if (fm[cat]) fm[cat].score = (fm[cat].score || 0) * EQUITY_ROBUSTNESS_STRICT_FACTOR;
      }
      const scores = Object.values(fm).map(o => o.score).filter(Number.isFinite);
      if (scores.length) {
        b.properties.fair_overall = { score: scores.reduce((a, b2) => a + b2, 0) / scores.length };
      }
    }
    const oScores = d.buildings.map(b2 => b2.properties?.fair_overall?.score).filter(Number.isFinite);
    d.meanFairness = oScores.length ? oScores.reduce((a, b2) => a + b2, 0) / oScores.length : 0;
  }
  compute15MinMetrics(strictCloned, categories);
  const strictResult = computeTrackInequality(strictCloned, 'quintileTotal', valueFn, weightFn);
  robustness.push({
    type: 'strict-threshold',
    gap: strictResult.gap,
    ratio: strictResult.ratio,
    gini: strictResult.gini,
    sameConclusion: Math.abs(strictResult.gap) <= Math.abs(baselineTotal.gap) * 1.1
  });
 
  const allSame = robustness.every(r => r.sameConclusion);
 
  // Assemble result
  equityAnalysisResult = {
    year,
    categories,
    budget,
    districtCount: baseRows.length,
    districts: baselineEnriched,
    baseline: { total: baselineTotal, men: baselineMen, women: baselineWomen },
    scenarios: scenarioResults,
    recommended,
    robustness,
    robustnessHolds: allSame,
    quintileProps: ['quintileTotal', 'quintileMen', 'quintileWomen']
  };
 
  setStatus('Done.');
  renderEquityResults(equityAnalysisResult);
  return equityAnalysisResult;
}
 
/* ---------- rendering --------------------------------------------------- */
function fmt(v, d = 2) { return Number.isFinite(v) ? v.toFixed(d) : '—'; }
function fmtPct(v) { return Number.isFinite(v) ? (v * 100).toFixed(1) + '%' : '—'; }
 
function renderEquityResults(result) {
  const container = document.getElementById('equityAnalysisOutput');
  if (!container) return;
 
  const { baseline, scenarios, recommended, robustness, robustnessHolds, districts, budget, categories } = result;
 
  // Table 1: Baseline inequality
  let html = `
    <h6 class="mt-2">Baseline inequality (${result.year})</h6>
    <table class="table table-sm table-bordered" style="font-size:11px;">
      <thead><tr>
        <th>Track</th><th>Q1 mean</th><th>Q5 mean</th><th>Gap (Q5−Q1)</th><th>Ratio Q1/Q5</th>
        <th>wQ1</th><th>wQ5</th><th>wGap</th><th>wRatio</th><th>Gini</th>
      </tr></thead>
      <tbody>`;
 
  for (const [label, data] of [['Overall', baseline.total], ['Men', baseline.men], ['Women', baseline.women]]) {
    html += `<tr>
      <td><strong>${label}</strong></td>
      <td>${fmt(data.q1Mean)}</td><td>${fmt(data.q5Mean)}</td>
      <td>${fmt(data.gap)}</td><td>${fmt(data.ratio)}</td>
      <td>${fmt(data.wQ1Mean)}</td><td>${fmt(data.wQ5Mean)}</td>
      <td>${fmt(data.wGap)}</td><td>${fmt(data.wRatio)}</td>
      <td>${fmt(data.gini, 3)}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
 
  // Table 2: Scenario comparison
  html += `
    <h6 class="mt-3">Scenario comparison (budget = ${budget} POIs: ${categories.map(prettyPOIName).join(', ')})</h6>
    <table class="table table-sm table-bordered" style="font-size:11px;">
      <thead><tr>
        <th>Scenario</th><th>Gap before</th><th>Gap after</th><th>Δ Gap</th>
        <th>Ratio before</th><th>Ratio after</th><th>Δ Ratio</th>
        <th>Residents improved</th><th>Placements</th>
      </tr></thead>
      <tbody>`;
 
  for (const sc of scenarios) {
    const gapBefore = baseline.total.gap;
    const gapAfter = sc.total.gap;
    const dGap = gapBefore - gapAfter;
    const rBefore = baseline.total.ratio;
    const rAfter = sc.total.ratio;
    const dRatio = (rAfter ?? 0) - (rBefore ?? 0);
    const isRecommended = sc.name === recommended.name;
    const rowClass = isRecommended ? 'style="background:#d4edda;"' : '';
    html += `<tr ${rowClass}>
      <td>${isRecommended ? '★ ' : ''}${sc.name}</td>
      <td>${fmt(gapBefore)}</td><td>${fmt(gapAfter)}</td>
      <td><strong>${fmt(dGap)}</strong></td>
      <td>${fmt(rBefore)}</td><td>${fmt(rAfter)}</td>
      <td>${fmt(dRatio)}</td>
      <td>${sc.residentsImproved.toLocaleString()}</td>
      <td>${sc.placements.map(p => p.districtName + ' (Q' + p.quintile + ')').join(', ')}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
 
  // Table 3: Robustness
  html += `
    <h6 class="mt-3">Robustness checks</h6>
    <table class="table table-sm table-bordered" style="font-size:11px;">
      <thead><tr><th>Check</th><th>Gap</th><th>Ratio</th><th>Gini</th><th>Holds?</th></tr></thead>
      <tbody>`;
  for (const r of robustness) {
    const icon = r.sameConclusion ? '✅' : '⚠️';
    html += `<tr>
      <td>${r.type}</td><td>${fmt(r.gap)}</td><td>${fmt(r.ratio)}</td>
      <td>${fmt(r.gini, 3)}</td><td>${icon}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  html += `<div class="small ${robustnessHolds ? 'text-success' : 'text-warning'} fw-bold">
    ${robustnessHolds ? 'Conclusion is robust across checks.' : 'Some robustness checks diverge — interpret with caution.'}
  </div>`;
 
  // Recommendation
  html += `
    <h6 class="mt-3">Recommendation</h6>
    <div class="small">
      <strong>${recommended.name}</strong> reduces the income–fairness gap the most
      (Δ gap = ${fmt(baseline.total.gap - recommended.total.gap)})
      while improving access for <strong>${recommended.residentsImproved.toLocaleString()}</strong> residents.
    </div>`;
 
  // District detail table
  html += `
    <h6 class="mt-3">District income × fairness detail</h6>
    <div style="max-height:300px; overflow:auto;">
    <table class="table table-sm table-bordered" style="font-size:10px;">
      <thead><tr>
        <th>District</th><th>Quintile</th><th>Income (tkr)</th>
        <th>Men (tkr)</th><th>Women (tkr)</th><th>Gap M−W</th>
        <th>Population</th><th>Mean fairness</th><th>15-min share</th>
      </tr></thead>
      <tbody>`;
  const sortedDistricts = districts.slice().sort((a, b) => (a.quintileTotal || 0) - (b.quintileTotal || 0));
  for (const d of sortedDistricts) {
    const genderGap = (d.incomeMenTkr != null && d.incomeWomenTkr != null)
      ? (d.incomeMenTkr - d.incomeWomenTkr).toFixed(1)
      : '—';
    html += `<tr>
      <td>${d.name}</td><td>Q${d.quintileTotal || '?'}</td>
      <td>${fmt(d.incomeTotalTkr, 1)}</td>
      <td>${fmt(d.incomeMenTkr, 1)}</td><td>${fmt(d.incomeWomenTkr, 1)}</td>
      <td>${genderGap}</td>
      <td>${d.population}</td><td>${fmt(d.meanFairness)}</td>
      <td>${fmtPct(d.thresholdShare)}</td>
    </tr>`;
  }
  html += `</tbody></table></div>`;
 
  container.innerHTML = html;
}
 
/* ---------- UI wiring --------------------------------------------------- */
function populateEquityCategoryCheckboxes() {
  const container = document.getElementById('equityAnalysisCats');
  if (!container) return;
  const cats = (typeof ALL_CATEGORIES !== 'undefined' && Array.isArray(ALL_CATEGORIES))
    ? ALL_CATEGORIES
    : ['grocery','hospital','pharmacy','dentistry','healthcare_center',
       'veterinary','university','kindergarten','school_primary','school_high'];

  // Keep the label, clear old checkboxes
  const label = container.querySelector('label.small.fw-bold');
  container.innerHTML = '';
  if (label) container.appendChild(label);
  const br = document.createElement('br');
  container.appendChild(br);

  cats.forEach(cat => {
    const checked = EQUITY_DEFAULT_CHECKED.includes(cat) ? 'checked' : '';
    const lbl = (typeof prettyPOIName === 'function') ? prettyPOIName(cat) : cat;
    const wrapper = document.createElement('label');
    wrapper.className = 'form-check-label small me-2';
    wrapper.innerHTML = `<input type="checkbox" value="${cat}" ${checked} class="form-check-input form-check-input-sm"> ${lbl}`;
    container.appendChild(wrapper);
  });
}

function wireEquityAnalysisUI() {
  populateEquityCategoryCheckboxes();

  const btn = document.getElementById('equityAnalysisRunBtn');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    const catCheckboxes = document.querySelectorAll('#equityAnalysisCats input:checked');
    const cats = Array.from(catCheckboxes).map(cb => cb.value);
    const budgetInput = document.getElementById('equityAnalysisBudget');
    const budget = parseInt(budgetInput?.value, 10) || EQUITY_SCENARIO_BUDGET;
    try {
      await runEquityAnalysis({ categories: cats.length ? cats : undefined, budget });
    } catch (err) {
      const out = document.getElementById('equityAnalysisOutput');
      if (out) out.innerHTML = `<div class="text-danger small">${err.message}</div>`;
      console.error('Equity analysis error', err);
    } finally {
      btn.disabled = false;
    }
  });
}

// Expose globally
window.runEquityAnalysis = runEquityAnalysis;
window.ensureDistrictIncomeData = ensureDistrictIncomeData;
window.lookupDistrictIncome = lookupDistrictIncome;
window.equityAnalysisResult = () => equityAnalysisResult;

// Init UI on DOM ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', wireEquityAnalysisUI);
} else {
  wireEquityAnalysisUI();
}