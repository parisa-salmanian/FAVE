// POI markers — symbol/icon/text layers for POI overlays on the map.
// Extracted from main.js; loaded as classical script before main.js.

/* ======================= POI markers ======================= */
function isPOISelectedFeature(f) {
  const id = f?.properties?.id ?? f?.properties?.osm_id ?? f?.properties?.['@id'];
  if (selectedPOIId && id && String(id) === String(selectedPOIId)) return true;

  if (window.activePOICats instanceof Set && window.activePOICats.size) {
    return window.activePOICats.has(poiCategoryOf(f));
  }

  if (fairActive && fairCategory && fairCategory !== 'mix') {
    return poiCategoryOf(f) === String(fairCategory).toLowerCase();
  }
  return false;
}

function resolveActivePOICategories() {
  const cats = new Set();
  if (window.activePOICats instanceof Set && window.activePOICats.size) {
    window.activePOICats.forEach(cat => {
      if (cat != null && cat !== '') cats.add(String(cat).toLowerCase());
    });
  }
  if (!cats.size) {
    if (fairCategory === 'mix') {
      selectedPOIMix.forEach(({ cat }) => {
        if (cat != null && cat !== '') cats.add(String(cat).toLowerCase());
      });
    } else if (fairCategory) {
      cats.add(String(fairCategory).toLowerCase());
    }
  }
  return cats;
}

function activePOIFeaturesForDisplay() {
  if (!fairCategory || !currentPOIsFC || !currentPOIsFC.features?.length) return [];
  const activeCats = resolveActivePOICategories();
  let base = activeCats.size
    ? currentPOIsFC.features.filter(f => activeCats.has(poiCategoryOf(f)))
    : currentPOIsFC.features;

  // Filter out POIs outside district boundaries (sea, neighbouring municipalities)
  const mask = ensureMezoMaskPolygon();
  if (mask?.geometry) {
    base = base.filter(f => {
      const coord = poiFeatureCoord(f);
      if (!coord) return false;
      try { return turf.booleanPointInPolygon(turf.point(coord), mask); } catch (_) { return true; }
    });
  }

  return dedupePOIFeaturesForDisplay(base);
}

function withPOIDisplayOffsets(features = []) {
  // Spread symbols that share the same coordinate so all POIs are visible.
  const byCoord = new Map();
  for (const f of features) {
    const c = f?.geometry?.coordinates;
    if (!Array.isArray(c) || !Number.isFinite(c[0]) || !Number.isFinite(c[1])) continue;
    const k = `${c[0].toFixed(6)},${c[1].toFixed(6)}`;
    if (!byCoord.has(k)) byCoord.set(k, []);
    byCoord.get(k).push(f);
  }

  const out = [];
  for (const arr of byCoord.values()) {
    if (arr.length === 1) {
      out.push(arr[0]);
      continue;
    }
    arr.forEach((f, i) => {
      const c = f.geometry.coordinates;
      const ring = 0.00008; // ~8m lat/lon visual spread (small)
      const ang = (2 * Math.PI * i) / arr.length;
      const lon = c[0] + Math.cos(ang) * ring;
      const lat = c[1] + Math.sin(ang) * ring;
      out.push({ ...f, __displayCoord: [lon, lat] });
    });
  }
  return out;
}

function createPOIDotLayer() {
  if (!showPOISymbols) return null;
  const baseData = activePOIFeaturesForDisplay();
  if (!baseData.length) return null;
  const data = withPOIDisplayOffsets(baseData);

  return new deck.ScatterplotLayer({
    id: 'poi-dots',
    data,
    pickable: true,
    stroked: true,
    radiusUnits: 'pixels',
    getRadius: f => isPOISelectedFeature(f) ? 7 : 5,
    getFillColor: f => poiColor(poiCategoryOf(f)),
    getLineColor: [20, 20, 20, 220],
    lineWidthMinPixels: 1,
    getPosition: f => f.__displayCoord || f?.geometry?.coordinates || [0, 0],
    parameters: { depthTest: false },
    updateTriggers: {
      getRadius: [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick],
      getFillColor: [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick]
    }
  });
}

// function createPOIPointLayer() {
//   // Draw only when fairness is active AND we have POIs AND symbols are toggled ON
//   if (!showPOISymbols) return null;
//   const baseData = activePOIFeaturesForDisplay();
//   if (!baseData.length) return null;
//   const data = withPOIDisplayOffsets(baseData);

//   // NOTE: Removed halo/circle layer entirely
//   return new deck.TextLayer({
//     id: 'poi-symbols',
//     data,
//     pickable: true,
//     collisionEnabled: false,
//     characterSet: [...'●⬛▲▼▶◀◆⬢⬟⬭◼'],
//     fontFamily: '"Noto Sans Symbols 2","Noto Sans Symbols","Segoe UI Symbol","Arial Unicode MS",sans-serif',
//     sizeUnits: 'pixels',
//     getSize: f => isPOISelectedFeature(f) ? 30 : 22,
//     sizeMinPixels: 10,
//     sizeMaxPixels: 50,
//     getText:  f => poiGlyph(poiCategoryOf(f)),
//     getColor: f => {
//       const c = poiColor(poiCategoryOf(f)).slice();
//       if ((window.activePOICats && window.activePOICats.size) && !isPOISelectedFeature(f)) {
//         c[3] = Math.min(160, c[3] ?? 255);
//       }
//       return c;
//     },
//     getPosition: f => {
//      if (Array.isArray(f.__displayCoord)) return f.__displayCoord;
//       const g = f.geometry;
//       if (!g) return [0,0];
//       if (g.type === 'Point') return g.coordinates;
//       try { return turf.centroid(f).geometry.coordinates; } catch { return [0,0]; }
//     },
//     onClick: async info => {
//       noteFeatureClick();
//       if (fairActive) saveTransitionScores();
//       const id = info?.object?.properties?.id ?? info?.object?.properties?.osm_id ?? info?.object?.properties?.['@id'];
//       selectedPOIId = (id != null) ? String(id) : null;
//       selectedPOIFeature = info?.object || null;
//       if (info && info.coordinate) {
//         showPopup(info.object, info.coordinate);
//       }
//       if (districtView) {
//         await refreshDistrictScores();
//       }
//       if (mezoView) {
//         await refreshMezoScores();
//       }
//       updateLayers();
//       if (info && info.coordinate) {
//         try { map.flyTo({ center: info.coordinate, zoom: Math.max(map.getZoom(), 16), speed: 0.8 }); } catch {}
//       }
//     },
//     // onClick: info => {
//     //   const id = info?.object?.properties?.id ?? info?.object?.properties?.osm_id ?? info?.object?.properties?.['@id'];
//     //   selectedPOIId = (id != null) ? String(id) : null;
//     //   const props = info?.object?.properties || {};
//     //   if (info && info.coordinate) {
//     //     showPopup(props, info.coordinate);
//     //   }
//     //   updateLayers();
//     //   if (info && info.coordinate) {
//     //     try { map.flyTo({ center: info.coordinate, zoom: Math.max(map.getZoom(), 16), speed: 0.8 }); } catch {}
//     //   }
//     // },
//     //Change for tooltip width part2
//     parameters: { depthTest: false },
//     updateTriggers: {
//       getSize:  [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick],
//       getColor: [fairActive, fairCategory, fairRecolorTick, selectedPOIId, (window.activePOICats ? window.activePOICats.size : 0), poiStyleTick],
//       getText:  [poiStyleTick]
//     }
//   });
// }

function createPOIPointLayer() {
  if (!showPOISymbols) return null;
  if (!_poiAtlasReady) {
    ensurePOIIconAtlas();
    return null;
  }
  const baseData = activePOIFeaturesForDisplay();
  if (!baseData.length) return null;
  const data = withPOIDisplayOffsets(baseData);

  return new deck.IconLayer({
    id: 'poi-symbols',
    data,
    pickable: true,
    iconAtlas: _poiIconAtlas,
    iconMapping: _poiIconMapping,
    getIcon: f => {
      const cat = poiCategoryOf(f);
      return _poiIconMapping[cat] ? cat : 'default';
    },
    getSize: f => isPOISelectedFeature(f) ? 36 : 24,
    sizeUnits: 'pixels',
    sizeMinPixels: 12,
    sizeMaxPixels: 50,
    getPosition: f => {
      if (Array.isArray(f.__displayCoord)) return f.__displayCoord;
      const g = f.geometry;
      if (!g) return [0, 0];
      if (g.type === 'Point') return g.coordinates;
      try { return turf.centroid(f).geometry.coordinates; } catch { return [0, 0]; }
    },
    onClick: async info => {
      noteFeatureClick();
      if (fairActive) saveTransitionScores();
      const id = info?.object?.properties?.id ?? info?.object?.properties?.osm_id ?? info?.object?.properties?.['@id'];
      selectedPOIId = (id != null) ? String(id) : null;
      selectedPOIFeature = info?.object || null;
      if (info && info.coordinate) showPopup(info.object, info.coordinate);
      if (districtView) await refreshDistrictScores();
      if (mezoView) await refreshMezoScores();
      updateLayers();
      if (info && info.coordinate) {
        try { map.flyTo({ center: info.coordinate, zoom: Math.max(map.getZoom(), 16), speed: 0.8 }); } catch {}
      }
    },
    parameters: { depthTest: false },
    updateTriggers: {
      getSize: [selectedPOIId, poiStyleTick],
      getIcon: [poiStyleTick]
    }
  });
}

function buildingMatchesPOI(props, cat) {
  if (!props) return false;
  const amen = String(props.amenity || '').toLowerCase();
  const shop = String(props.shop || '').toLowerCase();
  const healthcare = String(props.healthcare || '').toLowerCase();
  const name = String(props.name || props.namn || '').toLowerCase();
  const building = String(props.building || props.objekttyp || '').toLowerCase();
  const t = propsText(props);

  switch (cat) {
    case 'grocery':
      return shop === 'supermarket' || shop === 'convenience' || shop === 'greengrocer' ||
             amen === 'marketplace' || name.includes('ica') || name.includes('coop') ||
             name.includes('lidl') || name.includes('willys') ||
             textHasAny(t, ['matbutik', 'livsmedel', 'supermarket', 'grocery', 'dagligvaru']);
    case 'hospital':          return amen === 'hospital' || healthcare === 'hospital' || textHasAny(t, ['sjukhus', 'hospital', 'lasarett', 'akutmottagning']);
    case 'pharmacy':          return amen === 'pharmacy' || textHasAny(t, ['apotek', 'pharmacy']);
    case 'dentistry':         return amen === 'dentist' || textHasAny(t, ['tandvard', 'tandvård', 'tandlakare', 'tandläkare', 'dentist', 'dental']);
    case 'healthcare_center': return amen === 'clinic' || healthcare.includes('clinic') || healthcare.includes('centre') ||
                                        healthcare.includes('center') || healthcare.includes('doctor') ||
                                        textHasAny(t, ['vårdcentral', 'vardcentral', 'hälsocentral', 'halsocentral', 'clinic', 'medical center', 'care center', 'husläkare']);
    case 'veterinary':        return amen === 'veterinary' || textHasAny(t, ['veterinär', 'veterinar', 'djurklinik', 'animal hospital', 'djursjukhus']);
    case 'university':        return amen === 'university' || building === 'university' ||
                                        name.includes('university') || name.includes('college') ||
                                        name.includes('campus') || /\buniversitet\b/.test(t);
    case 'kindergarten':      return amen === 'kindergarten' || amen === 'childcare' || /\bforskola\b|\bförskola\b/.test(t);
    case 'school_primary': {
      const schoolLike = amen === 'school' || textHasAny(t, ['skola', 'school']);
      const highLike = isHighSchoolLike(props);
      return schoolLike && (isPrimarySchoolLike(props) || !highLike);
    }
    case 'school_high': {
      const schoolLike = amen === 'school' || textHasAny(t, ['skola', 'school', 'gymnas']);
      return schoolLike && isHighSchoolLike(props);
    }
    default: return false;
  }
}

function buildPOIBuildingDots(catList) {
  if (!baseCityFC?.features?.length) return [];
  const cats = Array.isArray(catList) ? catList : [catList];
  const pts = [];
  for (const f of baseCityFC.features) {
    if (!f.properties) continue;
    for (const cat of cats) {
      if (buildingMatchesPOI(f.properties, cat)) {
        const c = turf.centroid(f).geometry.coordinates;
        pts.push({ position: c, name: f.properties.name || '', category: cat });
        break;
      }
    }
  }
  const asFeatures = pts.map((p, idx) => ({
    type: 'Feature',
    properties: {
      id: `fallback:${p.category}:${idx}`,
      name: p.name || '',
      category: p.category
    },
    geometry: { type: 'Point', coordinates: p.position }
  }));
  const deduped = dedupeLocalBuildingPOIs('fallback', asFeatures);
  return deduped.map((f) => ({
    position: f.geometry.coordinates,
    name: f.properties?.name || '',
    category: f.properties?.category || ''
  }));
}

function createPOIBuildingMarkerLayer() {
  // If we already have fetched POIs (TextLayer) or symbols are toggled off, skip the fallback layer
  if (!fairCategory || !showPOISymbols) return null;
  if (activePOIFeaturesForDisplay().length) return null;
  let cats = [];
  if (fairCategory === 'mix') {
    cats = selectedPOIMix.map(e => e.cat);
  } else {
    cats = [fairCategory];
  }
  const data = buildPOIBuildingDots(cats);
  if (!data.length) return null;

  return new deck.ScatterplotLayer({
    id: 'poi-building-dots',
    data,
    pickable: false,
    getPosition: d => d.position,
    getFillColor: [0,180,255],
    getRadius: 5,
    radiusUnits: 'pixels',
    stroked: true,
    getLineColor: [20,20,20],
    getLineWidth: 1.5,
    parameters: { depthTest: false },
    opacity: 0.95,
    updateTriggers: { data: [fairCategory, fairRecolorTick, JSON.stringify(selectedPOIMix)] }
  });
}

// Best/Least overlays live in assets/js/views/overlays.js
