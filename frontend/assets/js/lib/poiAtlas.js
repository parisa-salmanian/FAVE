// POI icon atlas — extracted from main.js residual.
// Loaded after lib/visuals.js (POI_SYMBOLS, DARK_BASEMAP_STYLE).

// POI icon atlas builder + global atlas cache. Extracted from main.js.
let districtBoundaryFC = null;
let currentBasemapStyle = LIGHT_BASEMAP_STYLE;

function buildPOIIconAtlas(size = 64) {
  const cats = Object.keys(POI_SYMBOLS);
  const cols = Math.ceil(Math.sqrt(cats.length));
  const rows = Math.ceil(cats.length / cols);
  const canvas = document.createElement('canvas');
  canvas.width = cols * size;
  canvas.height = rows * size;
  const ctx = canvas.getContext('2d');
  const iconMapping = {};
  let loaded = 0;

  return new Promise((resolve) => {
    cats.forEach((cat, i) => {
      const col = i % cols;
      const row = Math.floor(i / cols);
      const img = new Image();
      img.onload = () => {
        ctx.drawImage(img, col * size, row * size, size, size);
        iconMapping[cat] = { x: col * size, y: row * size, width: size, height: size, mask: false };
        if (++loaded === cats.length) resolve({ iconAtlas: canvas, iconMapping });
      };
      img.onerror = () => {
        console.warn(`[POI Atlas] Failed to load icon for "${cat}":`, POI_SYMBOLS[cat].icon);
        if (++loaded === cats.length) resolve({ iconAtlas: canvas, iconMapping });
      };
      img.src = POI_SYMBOLS[cat].icon;
    });
  });
}

let _poiIconAtlas = null;
let _poiIconMapping = null;
let _poiAtlasReady = false;
let _poiAtlasPromise = null;
function ensurePOIIconAtlas() {
  if (_poiAtlasReady) return;
  if (_poiAtlasPromise) return;
  _poiAtlasPromise = buildPOIIconAtlas(64).then(result => {
    _poiIconAtlas = result.iconAtlas;
    _poiIconMapping = result.iconMapping;
    _poiAtlasReady = true;
    console.log('[POI Atlas] Ready');
    updateLayers();
  });
}
