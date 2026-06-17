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
  kalmar: 'assets/data/kalmar.geojson'
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

const GENDER_AGE_POP_URL_BY_CITY_KEY = {
  vaxjo: 'assets/data/gender-age-population.json',
  malmo: 'assets/data/malmo_age_gender.json',
  stockholm: 'assets/data/stockholm_age_gender.json'
};
const DEFAULT_GENDER_AGE_POP_URL = 'assets/data/gender-age-population.json';
const GENDER_AGE_POP_URL = 'assets/data/gender-age-population.json';
