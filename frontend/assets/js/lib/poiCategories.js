// poiCategories.js — registry for the EpiCity-derived POI categories added to
// FAVE in Phase 2 (the original 10 stay hardcoded in their existing maps).
//
// This is the SINGLE source of truth for the new categories. It exposes
// window.POI_EXTRA_* maps that every consumer spreads into its own structure
// (ALL_CATEGORIES, POI_QUERIES, POI_LABEL/COLOR, POI_META, POI_SYMBOLS). Loaded
// right after config.js, before every other consumer so their spreads see it.
//
// The Overpass selectors mirror EpiCity epicity_engine/osm.py classify_zone()
// exactly (read-only reference — never modified); the matching Python copy lives
// in tools/bake_city_data.py POI_QUERIES. No FAVE-specific catchments / distance
// thresholds / weights are invented here — the new categories use FAVE's existing
// per-category DEFAULTS wherever a value isn't otherwise specified, exactly like
// any other category the fairness model doesn't have a bespoke entry for.
(function () {
  // key, label, group, color (hex — a valid CSS colour and the icon fill),
  // rgb (map-marker tint), q (Overpass nwr selectors, mirroring EpiCity).
  const EXTRA = [
    { key:'library',           label:'Library',           group:'education',  color:'#5b6ee1', rgb:[91,110,225],  q:['nwr["amenity"="library"]'] },
    { key:'place_of_worship',  label:'Place of worship',  group:'civic',      color:'#9b59b6', rgb:[155,89,182],  q:['nwr["amenity"="place_of_worship"]'] },
    { key:'restaurant',        label:'Restaurant / café', group:'food',       color:'#e67e22', rgb:[230,126,34],  q:['nwr["amenity"~"^(restaurant|cafe|fast_food|pub|bar|food_court|biergarten|ice_cream)$"]'] },
    { key:'sports_centre',     label:'Sports centre',     group:'recreation', color:'#27ae60', rgb:[39,174,96],   q:['nwr["leisure"~"^(sports_centre|sports_hall|fitness_centre|swimming_pool|ice_rink|fitness_station)$"]'] },
    { key:'hotel',             label:'Hotel',             group:'hospitality',color:'#c0651f', rgb:[192,101,31],  q:['nwr["tourism"~"^(hotel|hostel|guest_house|motel)$"]'] },
    { key:'community_centre',  label:'Community centre',  group:'civic',      color:'#16a085', rgb:[22,160,133],  q:['nwr["amenity"="community_centre"]'] },
    { key:'mall',              label:'Shopping mall',     group:'commerce',   color:'#d6336c', rgb:[214,51,108],  q:['nwr["shop"="mall"]'] },
    { key:'museum',            label:'Museum',            group:'culture',    color:'#8e44ad', rgb:[142,68,173],  q:['nwr["tourism"="museum"]','nwr["amenity"="museum"]'] },
    { key:'theatre',           label:'Theatre / cinema',  group:'culture',    color:'#c2185b', rgb:[194,24,91],   q:['nwr["amenity"~"^(theatre|concert_hall|arts_centre|cinema|opera_house|events_venue)$"]'] },
    { key:'stadium',           label:'Stadium',           group:'recreation', color:'#2980b9', rgb:[41,128,185],  q:['nwr["leisure"="stadium"]'] },
    { key:'nightclub',         label:'Nightclub',         group:'nightlife',  color:'#7b2ff7', rgb:[123,47,247],  q:['nwr["amenity"="nightclub"]'] },
    { key:'playground',        label:'Playground',        group:'recreation', color:'#7cb342', rgb:[124,179,66],  q:['nwr["leisure"="playground"]'] },
    { key:'park',              label:'Park / green space',group:'recreation', color:'#2e7d32', rgb:[46,125,50],   q:['nwr["leisure"="park"]'] },
    { key:'cemetery',          label:'Cemetery',          group:'civic',      color:'#607d8b', rgb:[96,125,139],  q:['nwr["landuse"="cemetery"]','nwr["amenity"="grave_yard"]'] },
    { key:'police',            label:'Police station',    group:'safety',     color:'#1565c0', rgb:[21,101,192],  q:['nwr["amenity"="police"]'] },
    { key:'fire_station',      label:'Fire station',      group:'safety',     color:'#d32f2f', rgb:[211,47,47],   q:['nwr["amenity"="fire_station"]'] },
    { key:'castle',            label:'Castle / fortress', group:'heritage',   color:'#795548', rgb:[121,85,72],   q:['nwr["historic"~"^(castle|fort|fortress)$"]'] },
    { key:'manor',             label:'Manor (herrgård)',  group:'heritage',   color:'#8d6e63', rgb:[141,110,99],  q:['nwr["historic"="manor"]'] },
    { key:'historic_landmark', label:'Historic landmark', group:'heritage',   color:'#b8860b', rgb:[184,134,11],  q:['nwr["historic"~"^(building|yes)$"]','nwr["tourism"="attraction"]'] },
    { key:'parking',           label:'Parking',           group:'infra',      color:'#455a64', rgb:[69,90,100],   q:['nwr["amenity"="parking"]'] },
  ];

  const keys = EXTRA.map(e => e.key);
  const labels = {}, colors = {}, markColors = {}, symbols = {}, queries = {}, meta = [];
  for (const e of EXTRA) {
    labels[e.key] = e.label;
    colors[e.key] = e.color;
    markColors[e.key] = e.rgb;
    symbols[e.key] = { icon: `assets/icons/poi-${e.key}.svg` };
    queries[e.key] = e.q;
    meta.push({ id: e.key, label: e.label, color: e.color });
  }

  window.POI_EXTRA = EXTRA;
  window.POI_EXTRA_KEYS = keys;
  window.POI_EXTRA_LABELS = labels;
  window.POI_EXTRA_COLORS = colors;
  window.POI_EXTRA_MARKCOLORS = markColors;
  window.POI_EXTRA_SYMBOLS = symbols;
  window.POI_EXTRA_QUERIES = queries;
  window.POI_EXTRA_META = meta;

  // Inject the legacy #poi_<id> legend rows (the source of truth that state.js
  // reads and the shell popover mirrors). New categories default UNCHECKED so
  // they don't bloat the initial fairness mix — the user opts into them.
  function injectLegacyRows() {
    const menu = document.querySelector('.poi-menu');
    if (!menu || menu.querySelector('.poi-check[data-cat="library"]')) return;
    const html = EXTRA.map(e => `
      <div class="poi-row mb-2 d-flex align-items-center justify-content-between gap-2" data-extra="1">
        <div class="form-check m-0">
          <input class="form-check-input poi-check" type="checkbox" id="poi_${e.key}" data-cat="${e.key}">
          <label class="form-check-label" for="poi_${e.key}"><img src="assets/icons/legend-${e.key}.svg" alt="" class="poi-legend-icon" width="22" height="22"> ${e.label}</label>
        </div>
        <div class="d-flex align-items-center gap-2">
          <input class="form-range poi-weight" data-cat="${e.key}" type="range" min="1" max="10" step="1" value="5">
          <span class="badge bg-dark-subtle text-dark poi-weight-val" data-cat="${e.key}">5</span>
        </div>
      </div>`).join('');
    const rows = menu.querySelectorAll('.poi-row');
    const anchor = rows.length ? rows[rows.length - 1] : null;
    if (anchor) anchor.insertAdjacentHTML('afterend', html);
    else menu.insertAdjacentHTML('beforeend', html);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectLegacyRows);
  } else {
    injectLegacyRows();
  }
})();
