// Visual constants — colors, materials, basemap styles, POI symbol icons.
// Extracted from main.js. Loaded after deck.gl/proj4 CDN bundles so that
// DISTRICT_DASH_EXT (which calls into deck.PathStyleExtension at parse time)
// resolves correctly.

const DR_SELECTION_COLOR_DEFAULT = [241, 105, 19];  // default selection highlight (#f16913)
const BACKDROP_COLOR = [118, 134, 156];   // muted slate-blue for context buildings
const DEFAULT_BUILDING_COLOR = [140, 170, 198]; // cool slate-blue (clean contrast on gray basemap)
const DR_UNSELECTED_COLOR = [180, 180, 180];
const DR_DOT_SIZE_BY_MODE = {
  building: { normal: 3, selected: 6 },
  mezo: { normal: 3, selected: 6 },
  district: { normal: 9, selected: 14 }
};
const WHATIF_SUGGESTION_COLOR = [255, 193, 7];
const DISTRICT_DASH_EXT = new deck.PathStyleExtension({ dash: true });
// Light = OSM raster fully desaturated and slightly dimmed so the fairness
// ramp / 3D buildings / POI markers pop against a neutral grey base. This
// matches the EpiCity project's basemap recipe (static/map.js initMap).
// Dark = same OSM source, just inverted via brightness mapping so labels
// render light-on-dark — keeps the tile cache in one place across modes
// and avoids needing a separate dark tile provider.
const LIGHT_BASEMAP_STYLE = {
  version: 8,
  sources: {
    'osm': {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxzoom: 19,
    },
  },
  layers: [{
    id:     'osm-tiles',
    type:   'raster',
    source: 'osm',
    paint:  {
      // Keep the OSM tiles desaturated so the fairness ramp + 3D buildings
      // pop, but lift the floor / drop the contrast so the base reads as
      // off-white rather than the EpiCity-style flat grey.
      'raster-saturation':     -1,
      'raster-brightness-min':  0.55,
      'raster-brightness-max':  1.0,
      'raster-contrast':       -0.15,
    },
  }],
};
const DARK_BASEMAP_STYLE = {
  version: 8,
  sources: {
    'osm': {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxzoom: 19,
    },
  },
  layers: [{
    id:     'osm-tiles',
    type:   'raster',
    source: 'osm',
    paint:  {
      // Invert + desaturate: brightness-min/max swap maps light pixels to
      // dark and vice-versa, so the same OSM tiles render as a dark base
      // with light labels/roads — paired with the dark UI shell.
      'raster-saturation':     -1,
      'raster-brightness-min':  0.85,
      'raster-brightness-max':  0.04,
      'raster-contrast':        0.2,
    },
  }],
};

const CATEGORY_COLORS = {
  supermarket:[255,204,0], residential:[52,152,219], commercial:[255,159,67], retail:[255,204,0],
  industrial:[128,139,150], education:[46,204,113], health:[231,76,60], public:[142,68,173],
  religious:[241,196,15], transportation:[0,177,106], garage:[149,165,166], hotel:[255,118,117],
  sports:[39,174,96], farm:[110,44,0], house:[52,152,219], detached:[52,152,219],
  apartments:[52,152,219], terrace:[52,152,219], yes:BACKDROP_COLOR, unknown:BACKDROP_COLOR
};

const SELECTED_BUILDING_TYPE_COLOR = [128, 0, 128];
const BUILDING_MATERIAL = {
  ambient: 0.25,
  diffuse: 0.65,
  shininess: 48,
  specularColor: [255, 250, 240]
};

const TYPE_COLORS = {
  'Komplementbyggnad':[141,211,199], 'Bostad':[255,255,179], 'Samhällsfunktion':[190,186,218],
  'Verksamhet':[251,128,114], 'Ekonomibyggnad':[128,177,211], 'Övrig byggnad':[253,180,98],
  'Industri':[179,222,105], default:BACKDROP_COLOR
};

const YEAR_COLORS = { '0':[190,190,190], '2021':[52,152,219], '2022':[46,204,113], '2023':[243,156,18], '2024':[231,76,60], 'null':[155,89,182] };

const POI_MARK_COLORS = {
  grocery:[0,180,255], hospital:[0,180,255], pharmacy:[0,180,255], dentistry:[0,180,255],
  healthcare_center:[0,180,255], veterinary:[0,180,255], university:[0,180,255],
  kindergarten:[0,180,255], school_primary:[0,180,255], school_high:[0,180,255],
};

const POI_SYMBOLS = {
  grocery:           { icon: 'assets/icons/poi-grocery.svg' },
  hospital:          { icon: 'assets/icons/poi-hospital.svg' },
  pharmacy:          { icon: 'assets/icons/poi-pharmacy.svg' },
  dentistry:         { icon: 'assets/icons/poi-dentistry.svg' },
  healthcare_center: { icon: 'assets/icons/poi-healthcare_center.svg' },
  veterinary:        { icon: 'assets/icons/poi-veterinary.svg' },
  university:        { icon: 'assets/icons/poi-university.svg' },
  kindergarten:      { icon: 'assets/icons/poi-kindergarten.svg' },
  school_primary:    { icon: 'assets/icons/poi-school_primary.svg' },
  school_high:       { icon: 'assets/icons/poi-school_high.svg' },
  default:           { icon: 'assets/icons/poi-university.svg' },
};

// District label font/charset (moved from main.js residual)
const DISTRICT_LABEL_FONT_FAMILY = '"Noto Sans","Noto Sans Symbols 2","Noto Sans Symbols","Segoe UI","Arial Unicode MS",sans-serif';
const DISTRICT_LABEL_CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzÅÄÖåäö0123456789 .,-/()';
