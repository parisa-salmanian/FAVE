// City-specific file path tables and Swedish "andamal"/"byggnad" mappings,
// extracted from main.js. Loaded as a classical script before main.js.
// When adding a new city, update these maps + the corresponding files in
// frontend/assets/data/  (see CLAUDE.md → "Adding a new city").

const CITY_URL  = 'assets/data/lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson';
const STATS_URL = 'assets/data/bldg_web.geojson';
const OSM_GEOJSON_CITY_URLS = {
  malmo: 'assets/data/byggnad_malmo.geojson'
};
const DEFAULT_DISTRICT_URL = 'assets/data/Vaxjo_regso.geojson';
const DISTRICT_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/Vaxjo_regso.geojson',
  malmo: 'assets/data/malmo_regso.geojson',
  goteborg: 'assets/data/goteborg_regso.geojson',
  norrkoping: 'assets/data/norrkoping_regso.geojson',
  stockholm: 'assets/data/stockholm_regso.geojson',
  uppsala: 'assets/data/uppsala_regso.geojson',
  kalmar: 'assets/data/kalmar_regso.geojson'
};

const BUILDING_URL_BY_CITY_KEY = {
  // After running the Lantmäteriet pipeline (fetch + process) for all cities,
  // change the vaxjo entry to 'assets/data/byggnad_vaxjo.geojson' — the pipeline
  // will have replaced the old manual export with the full-coverage height data.
  vaxjo: 'assets/data/lantmateriat-byggnadsverk-buildings-wgs84-11-12-2024.geojson',
  malmo: 'assets/data/byggnad_malmo.geojson',
  goteborg: 'assets/data/byggnad_goteborg.geojson',
  norrkoping: 'assets/data/byggnad_norrkoping.geojson',
  stockholm: 'assets/data/byggnad_stockholm.geojson',
  uppsala: 'assets/data/byggnad_uppsala.geojson',
  kalmar: 'assets/data/byggnad_kalmar.geojson'
};

const LOCAL_CITY_NAMES = {
  vaxjo: 'Växjö', malmo: 'Malmö', goteborg: 'Göteborg',
  stockholm: 'Stockholm', kalmar: 'Kalmar',
  norrkoping: 'Norrköping', uppsala: 'Uppsala'
};

const ANDAMAL_TO_POI_CATEGORY = {
  'sjukhus':       'hospital',
  'vårdcentral':   'healthcare_center',
  'apotek':        'pharmacy',
  'tandvård':      'dentistry',
  'tandläkare':    'dentistry',
  'veterinär':     'veterinary',
  'djursjukhus':   'veterinary',
  'universitet':   'university',
  'högskola':      'university',
  'förskola':      'kindergarten',
  'skola':         'school_primary',
  'grundskola':    'school_primary',
  'gymnasium':     'school_high',
  'livsmedel':     'grocery',
  'dagligvaror':   'grocery',
  'handel':        'grocery',
  'butik':         'grocery'
};

const ANDAMAL_TO_BUILDING_TYPE = {
  'bostad':              'residential',
  'flerfamiljshus':      'residential',
  'småhus':              'residential',
  'radhus':              'residential',
  'parhus':              'residential',
  'fritidshus':          'residential',

  'industri':            'industrial',
  'lager':               'industrial',
  'fabrik':              'industrial',

  'verksamhet':          'commercial',
  'kontor':              'commercial',
  'affär':               'retail',
  'handel':              'retail',
  'köpcentrum':          'retail',
  'varuhus':             'retail',
  'butik':               'retail',

  'hotell':              'hotel',
  'vandrarhem':          'hotel',

  'kyrka':               'religious',
  'moské':               'religious',
  'samfund':             'religious',
  'kapell':              'religious',

  'skola':               'education',
  'grundskola':          'education',
  'gymnasium':           'education',
  'universitet':         'education',
  'högskola':            'education',
  'förskola':            'education',

  'sjukhus':             'public',
  'vårdcentral':         'public',
  'samhällsfunktion':    'public',
  'brandstation':        'public',
  'polisstation':        'public',
  'bibliotek':           'public',

  'garage':              'garage',
  'komplementbyggnad':   'garage',
  'förråd':              'garage',
  'parkering':           'garage',

  'ekonomibyggnad':      'farm',
  'jordbruk':            'farm',
  'ladugård':            'farm',
  'stall':               'farm',

  'station':             'transportation',
  'terminal':            'transportation',
  'flygplats':           'transportation',

  'övrig':               'Other / unknown',
  'ospecificerad':       'Other / unknown'
};

// Per-RegSO SCB demographics, baked OFFLINE by tools/bake_scb_demographics.py +
// tools/analysis/scb_parse.py. Keyed by regsokod. Read at runtime from the
// bundled JSON only — never from the SCB API (CLAUDE.md hard rule).
const DEMOGRAPHICS_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/demographics_vaxjo.json'
};

// Demographic features surfaced in the DR / EBM / contrastive / PCP views.
//   json  = field name in demographics_<city>.json
//   key   = metric key used inside drView (dr* matrix + metrics)
//   label = axis / feature label. The " (dem)" suffix is how drFeatureMode.js
//           tells demographic features apart from accessibility ones.
const DEMOGRAPHIC_FEATURES = [
  { json: 'employed_pct',             key: 'demEmployed',      label: 'Employment rate (dem)' },
  { json: 'unemployed_pct',           key: 'demUnemployed',    label: 'Unemployment (dem)' },
  { json: 'longterm_unemployed_pct',  key: 'demLongUnemp',     label: 'Long-term unemployed (dem)' },
  { json: 'managerial_pct',           key: 'demManagerial',    label: 'Managerial jobs (dem)' },
  { json: 'median_disp_income_pba',   key: 'demIncome',        label: 'Median income (dem)' },
  { json: 'income_support_share_pct', key: 'demIncomeSupport', label: 'Income support (dem)' },
  { json: 'eligible_higher_edu_pct',  key: 'demHigherEdu',     label: 'Higher-ed eligible (dem)' },
  { json: 'students_pct',             key: 'demStudents',      label: 'Students (dem)' }
];

// Rich per-building DR features (baked offline by tools/bake_dr_features.py).
const DR_FEATURES_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/dr_features_vaxjo.json'
};

// The "winning combo" extra dimensions added to the building-level DR matrix
// (ACCESS already enters via the fairness scores). Label suffixes (modal)/(morph)/
// (dem) let drFeatureMode.js group them as the non-accessibility feature set.
//   json = field in dr_features_<city>.json ; key = DR metric key ; label = axis label
const DR_RICH_FEATURES = [
  { json: 'modal_walk_drive',  key: 'richModalWD', label: 'Walk/drive access (modal)' },
  { json: 'modal_cycle_drive', key: 'richModalCD', label: 'Cycle/drive access (modal)' },
  { json: 'form_height',       key: 'richHeight',  label: 'Building height (morph)' },
  { json: 'form_logarea',      key: 'richLogArea', label: 'Footprint area (morph)' },
  { json: 'form_logdensity',   key: 'richDensity', label: 'Local density (morph)' },
  { json: 'form_multifamily',  key: 'richMulti',   label: 'Multi-family (morph)' },
  { json: 'demo_need',         key: 'richNeed',    label: 'Need index (dem)' },
  { json: 'demo_income',       key: 'richIncome',  label: 'Median income (dem)' }
];

// Synthetic per-building demographic shares (stamped by lib/synthpop.js for ALL
// cities, unlike the Växjö-only __drRich set above). Fed into the building-mode
// DR matrix so UMAP/PCP can find demographic structure (elderly-heavy,
// low-education, high-dependency neighbourhoods) instead of only built form.
// Keys are demSyn* so both the (dem) label filter AND the ^dem[A-Z] key filter
// in drFeatureMode.js treat them as "extra/demographic" features. NB these are a
// synthetic spread around the real DESO mean — defensible at the CLUSTER level,
// not as a per-building fact.
const DR_SYNTH_DEMO_FEATURES = [
  // `log: true` → log1p the value before z-score/color: these two are heavily
  // right-skewed (a few outlier buildings reach 30×/80× the median), which would
  // otherwise dominate the UMAP distance and crush the color ramp.
  { prop: '__synthElder',         key: 'demSynElder',     label: 'Elderly share (dem)' },
  { prop: '__synthChild',         key: 'demSynChild',     label: 'Child share (dem)' },
  { prop: '__synthDependency',    key: 'demSynDep',       label: 'Dependency ratio (dem)', log: true },
  { prop: '__synthHigherEd',      key: 'demSynHigherEd',  label: 'Higher education (dem)' },
  { prop: '__synthNeet',          key: 'demSynNeet',      label: 'NEET share (dem)' },
  { prop: '__synthIncomeSupport', key: 'demSynIncSup',    label: 'Income support (dem)', log: true },
  { prop: '__synthMale',          key: 'demSynMale',      label: 'Male share (dem)' }
];

// "All Data" explore set — the FULL SCB socio-demographic profile as DR EMBEDDING
// features, at EVERY scale + city. Real DESO values at meso/macro (aggregated per
// unit into the __*Real fields that districts.js stamps on cells/districts),
// synthetic per-building values at micro (__synth*). Same scb* keys + "(scb)"
// labels at all scales so the filter, contrastive, EBM and PCP treat them
// uniformly. Kept ONLY by the all_data feature set (label ends "(scb)") so every
// other set stays non-tautological (real demographics remain colour-only there).
// NB the 9 BASE entries are the DERIVED SCB summary baked into
// demographics/deso.geojson (each has an explicit __synth*/__*Real key). The
// FULL rich SCB profile (income/labour/transfers/education/students/age spectrum,
// ~130-160 fields, split by sex + Sweden-/foreign-born) lives in the separate
// demographics/deso_full.json (baked by tools/bake_scb_full.py) and is appended
// at runtime by applyDrScbFullFields() when a city loads. Those appended entries
// carry a `desoKey` instead of __synth*/__*Real: their value is resolved by DESO
// lookup (epiDesoFull) — a count-weighted DESO mean at meso/macro (via each unit's
// __desoMix) and the building's own DESO value at micro. So adding SCB fields is a
// bake-list change, never a code change here.
const DR_SCB_FEATURES_BASE = [
  { key: 'scbChild',    label: 'child share (scb)',      building: '__synthChild',         unit: '__childReal'    },
  { key: 'scbElder',    label: 'elderly share (scb)',    building: '__synthElder',         unit: '__elderReal'    },
  { key: 'scbDep',      label: 'dependency ratio (scb)', building: '__synthDependency',    unit: '__depReal'      },
  { key: 'scbHigherEd', label: 'higher education (scb)', building: '__synthHigherEd',      unit: '__higherEdReal' },
  { key: 'scbNeet',     label: 'NEET share (scb)',       building: '__synthNeet',          unit: '__neetReal'     },
  { key: 'scbIncSup',   label: 'income support (scb)',   building: '__synthIncomeSupport', unit: '__incSupReal'   },
  { key: 'scbMale',     label: 'male share (scb)',       building: '__synthMale',          unit: '__maleReal'     },
  { key: 'scbIncome',   label: 'median income (scb)',    building: '__synthIncome',        unit: '__incomeReal', log: true },
  { key: 'scbNeed',     label: 'need index (scb)',       building: '__synthNeed',          unit: '__needZ'        }
];
// Live SCB feature list = base 9 + the loaded city's deso_full.json manifest.
// Mutated IN PLACE (same binding) so every consumer that reads DR_SCB_FEATURES at
// call time (scbRowFields, scbLabelsIfPresent, the contrastive/EBM payload) stays
// current without a re-import. DR_FEATURE_CONFIG re-syncs its scb tail separately.
const DR_SCB_FEATURES = DR_SCB_FEATURES_BASE.slice();

// Rebuild DR_SCB_FEATURES = base 9 + `fields` (the deso_full.json manifest for the
// active city). Called on city load. `fields` = [{key,label,group,...}] or null.
function applyDrScbFullFields(fields) {
  DR_SCB_FEATURES.length = 0;
  for (const f of DR_SCB_FEATURES_BASE) DR_SCB_FEATURES.push(f);
  if (Array.isArray(fields)) {
    for (const fld of fields) {
      if (!fld || typeof fld.key !== 'string') continue;
      DR_SCB_FEATURES.push({
        key: 'scbF_' + fld.key,                 // scbF... → matches isScbFeature /^scb[A-Z]/
        label: (fld.label || fld.key) + ' (scb)',
        desoKey: fld.key,                        // resolved via epiDesoFull (DESO lookup)
        group: fld.group,
        log: /disposable income/i.test(fld.label || '')   // log-scale only the income *amounts*
      });
    }
  }
  // Keep the EBM/contrastive config's scb tail in step (drView owns it; runtime call).
  if (typeof syncDrFeatureConfigScb === 'function') syncDrFeatureConfigScb();
}

const GENDER_AGE_POP_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/gender-age-population.json',
  malmo: 'assets/data/malmo_age_gender.json',
  stockholm: 'assets/data/stockholm_age_gender.json'
};
const DEFAULT_GENDER_AGE_POP_URL = 'assets/data/gender-age-population.json';
const GENDER_AGE_POP_URL = 'assets/data/gender-age-population.json';
