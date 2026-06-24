// POI selectors + tag classifiers + text-match helpers, extracted from main.js.
// POI_QUERIES is the canonical Overpass selector list — it must stay in sync
// with tools/bake_city_data.py (which holds an independent Python copy).

const POI_QUERIES = {
  grocery: [
    'nwr["shop"="supermarket"]',
    'nwr["shop"="convenience"]',
    'nwr["shop"="greengrocer"]',
    'nwr["amenity"="marketplace"]'
  ],
  hospital:  ['nwr["amenity"="hospital"]','nwr["healthcare"="hospital"]'],
  pharmacy:  ['nwr["amenity"="pharmacy"]'],
  dentistry: ['nwr["amenity"="dentist"]'],
  healthcare_center: ['nwr["amenity"="clinic"]','nwr["healthcare"~"clinic|centre|doctor",i]'],
  veterinary: ['nwr["amenity"="veterinary"]'],
  university: ['nwr["amenity"="university"]'],
  kindergarten:   ['nwr["amenity"="kindergarten"]','nwr["amenity"="childcare"]'],
  // Both school buckets pull the SAME amenity=school set (mirrors POI_QUERIES in
  // tools/bake_city_data.py); the grundskola/gymnasium split is done downstream.
  // Only the dead Overpass-fallback path uses these — supported cities load the
  // pre-split baked geojsons directly in fetchPOIs.
  school_primary: ['nwr["amenity"="school"]'],
  school_high:    ['nwr["amenity"="school"]']
};

function tagsText(...vals) {
  return vals.filter(Boolean).map(v => String(v).toLowerCase()).join(' ');
}

function parseIscedDigits(str) {
  const m = String(str || '').match(/[0-9]/g);
  return m ? m.map(d => parseInt(d, 10)) : [];
}

function normalizeForMatch(txt = '') {
  return String(txt)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '');
}

function textHasAny(text, needles = []) {
  const t = normalizeForMatch(text);
  return needles.some(n => t.includes(normalizeForMatch(n)));
}

function propsText(props = {}) {
  const keys = [
    'name','namn','objektnamn','byggnadsnamn','building','objekttyp','category','category_label',
    'amenity','shop','healthcare','school:level','education:level','level','isced:level',
    'verksamhet','verksamhetstyp','verksamhet_typ','anvandning','anvandningstyp','beskrivning'
  ];
  return keys
    .map(k => props?.[k])
    .filter(v => v != null && String(v).trim())
    .map(v => String(v).toLowerCase())
    .join(' ');
}

function isPrimarySchoolLike(tags = {}) {
  const t = `${tagsText(tags['school:level'], tags['education:level'], tags['level'])} ${propsText(tags)}`;
  if (/\bgrundskol/.test(t) || /\bprimary\b/.test(t) || /\blower\b/.test(t) || /\belementary\b/.test(t)) return true;
  const nums = parseIscedDigits(tags['isced:level']);
  return nums.includes(1) || nums.includes(2);
}

function isHighSchoolLike(tags = {}) {
  const t = `${tagsText(tags['school:level'], tags['education:level'], tags['level'])} ${propsText(tags)}`;
  if (/\bgymnas/.test(t) || /\bhigh\s*school\b/.test(t) || /\bupper\s*secondary\b/.test(t) || /\bsecondary\b/.test(t)) return true;
  const nums = parseIscedDigits(tags['isced:level']);
  return nums.includes(3);
}
