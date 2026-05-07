// Numeric encoders for building categories/usage — used by the DR view to
// convert categorical building data into numeric features. Extracted from main.js.
// Loaded as classical script before drView.js so its functions are visible.

/* ---- DR helpers: encode categories/usage into numeric codes ---- */
const OSM_CAT_CODE = {
  residential: 0,
  house: 0,
  apartments: 0,
  terrace: 0,
  education: 1,
  school: 1,
  university: 1,
  health: 2,
  hospital: 2,
  clinic: 2,
  pharmacy: 2,
  retail: 3,
  supermarket: 3,
  commercial: 4,
  office: 4,
  public: 5,
  government: 5,
  religious: 6,
  industrial: 7
};

function osmCategoryCode(props = {}) {
  const raw = (props.category || props.objekttyp || props.building || '').toLowerCase();
  for (const key of Object.keys(OSM_CAT_CODE)) {
    if (raw.includes(key)) return OSM_CAT_CODE[key];
  }
  return 8; // other/unknown
}

function hybridCategoryCode(props = {}) {
  const osmCode = osmCategoryCode(props);
  if (osmCode !== 8) return osmCode;
  const localCode = localUsageCode(props);
  return Number.isFinite(localCode) ? localCode : 8;
}

const LOCAL_USAGE_CODE = {
  'Bostad': 0,
  'Samhällsfunktion': 1,
  'Verksamhet': 2,
  'Industri': 3,
  'Ekonomibyggnad': 4,
  'Komplementbyggnad': 5,
  'Övrig byggnad': 6
};

// Canonical building-type labels (used by both fairness charts + dropdown)
const BUILDING_TYPE_ORDER = [
  'farm',
  'religious',
  'hotel',
  'garage',
  'Other / unknown',
  'residential',
  'industrial',
  'education',
  'retail',
  'transportation',
  'public',
  'commercial'
];

// Synonyms → canonical building-type label
const BUILDING_TYPE_SYNONYMS = {
  farm: 'farm',
  barn: 'farm',
  agricultural: 'farm',
  agriculture: 'farm',
  stable: 'farm',

  religious: 'religious',
  church: 'religious',
  chapel: 'religious',
  mosque: 'religious',
  temple: 'religious',
  synagogue: 'religious',

  hotel: 'hotel',
  motel: 'hotel',
  hostel: 'hotel',
  guest_house: 'hotel',
  'guest house': 'hotel',
  resort: 'hotel',

  garage: 'garage',
  parking: 'garage',
  carport: 'garage',
  shed: 'garage',

  residential: 'residential',
  house: 'residential',
  apartments: 'residential',
  apartment: 'residential',
  terrace: 'residential',
  detached: 'residential',

  industrial: 'industrial',
  factory: 'industrial',
  manufactory: 'industrial',
  warehouse: 'industrial',

  education: 'education',
  school: 'education',
  university: 'education',
  college: 'education',
  kindergarten: 'education',

  retail: 'retail',
  supermarket: 'retail',
  shop: 'retail',
  mall: 'retail',
  store: 'retail',

  transportation: 'transportation',
  transport: 'transportation',
  station: 'transportation',
  railway: 'transportation',
  train: 'transportation',
  bus: 'transportation',
  terminal: 'transportation',
  aerodrome: 'transportation',

  public: 'public',
  civic: 'public',
  government: 'public',
  hospital: 'public',
  clinic: 'public',
  pharmacy: 'public',
  health: 'public',

  commercial: 'commercial',
  office: 'commercial',
  business: 'commercial',
};

// inverse lookup for category codes -> canonical building-type labels ---
const OSM_CODE_LABEL = {};
Object.entries(OSM_CAT_CODE).forEach(([name, code]) => {
  if (!(code in OSM_CODE_LABEL)) OSM_CODE_LABEL[code] = BUILDING_TYPE_SYNONYMS[name] || name;
});

const LOCAL_USAGE_CODE_LABEL = {};
Object.entries(LOCAL_USAGE_CODE).forEach(([name, code]) => {
  if (!(code in LOCAL_USAGE_CODE_LABEL)) {
    LOCAL_USAGE_CODE_LABEL[code] = name;
  }
});

const OSM_CODE_CANONICAL = {
  0: 'residential',
  1: 'education',
  2: 'public',
  3: 'retail',
  4: 'commercial',
  5: 'public',
  6: 'religious',
  7: 'industrial',
  8: 'Other / unknown'
};

const LOCAL_CODE_CANONICAL = {
  0: 'residential',          // Bostad
  1: 'public',               // Samhällsfunktion
  2: 'commercial',           // Verksamhet
  3: 'industrial',           // Industri
  4: 'farm',                 // Ekonomibyggnad (often agricultural)
  5: 'garage',               // Komplementbyggnad (sheds/garages)
  6: 'Other / unknown'
};

function canonicalBuildingType(label) {
  if (!label) return 'Other / unknown';
  const raw = String(label).trim();
  if (!raw) return 'Other / unknown';
  const low = raw.toLowerCase();

  // Exact match on known synonyms
  if (BUILDING_TYPE_SYNONYMS[low]) return BUILDING_TYPE_SYNONYMS[low];

  // Contains match (e.g., category strings with prefixes)
  for (const [key, val] of Object.entries(BUILDING_TYPE_SYNONYMS)) {
    if (low.includes(key)) return val;
  }

  // Preserve already canonical labels if user provided them
  if (BUILDING_TYPE_ORDER.includes(raw)) return raw;

  return 'Other / unknown';
}

/**
 * Map numeric category/usage code (metrics.categoryCode[i])
 * back to a canonical building-type label, depending on sourceMode.
 */
function categoryLabelFromCode(code) {
  if (!Number.isFinite(code)) return 'Other / unknown';

  if (sourceMode === 's1' || sourceMode === 'osm_s1') {
    // Lantmäteriet / local usage
    return LOCAL_CODE_CANONICAL[code] || 'Other / unknown';
  }
  // OSM-based categories
  return OSM_CODE_CANONICAL[code] || 'Other / unknown';
}



function localUsageCode(props = {}) {
  const o = props.objekttyp || '';
  if (Object.prototype.hasOwnProperty.call(LOCAL_USAGE_CODE, o)) {
    return LOCAL_USAGE_CODE[o];
  }
  return 0; // default bucket
}


