// Map layer factory (deck.gl GeoJsonLayer / IconLayer / extrusion etc.).
// Extracted from main.js; loaded before main.js.

/* ======================= Layers ======================= */
function createCityLayer(grayBackdrop) {
  if (!baseCityFC) return null;


  // autoHighlight + the meso/macro grey tint give the user a consistent
  // hover preview at every scale. The inspector mirror inside
  // handleBuildingHover stays gated on inspector-open state (via
  // _scheduleInspectorUpdate), so when the inspector is closed the heavy
  // DOM work is still skipped — only deck.gl's picking pass runs, and the
  // outer rAF-throttle on handleBuildingHover keeps JS work bounded to
  // one dispatch per frame.
  const common = {
    id: 'city-buildings',
    data: baseCityFC,
    pickable: true,
    wireframe: false,
    autoHighlight: true,
    // Match the meso/macro hover tint so all three scales feel consistent.
    // (createMezoHexLayer uses [40, 50, 65, 110]; districts use a slightly
    // lighter [55, 65, 80, 90].)
    highlightColor: [40, 50, 65, 110],
    onClick: handleClick,
    onHover: typeof handleBuildingHover === 'function' ? handleBuildingHover : undefined,
  };

  const normalColor = (f) => baseBuildingColorForFeature(f);

  if (grayBackdrop) {
    return new deck.GeoJsonLayer({
      ...common,
      extruded:false,
      stroked:false,
      opacity:0.25,
      getFillColor: f => {
        if (f.properties && f.properties._drSelected) {
          const c = f.properties._drColor;
          if (Array.isArray(c) && c.length >= 3) {
            return [c[0], c[1], c[2]];
          }
          return DR_SELECTION_COLOR_DEFAULT;
        }
        if (selectedBuildingType) {
          return featureMatchesSelectedType(f)
            ? SELECTED_BUILDING_TYPE_COLOR
            : DR_UNSELECTED_COLOR;
        }
        return drHasSelection ? DR_UNSELECTED_COLOR : BACKDROP_COLOR;
      },
      updateTriggers: {
        getFillColor: [drSelectionTick, drHasSelection, buildingTypeTick, selectedBuildingType]
      }
    });
  }

  return new deck.GeoJsonLayer({
    ...common,
    extruded:true,
    stroked:false,
    opacity:0.95,
    material: BUILDING_MATERIAL,
    getElevation: f =>
      clampElev((f.properties?.height_m ?? f.properties?._mean ?? f.properties?.hojd ?? f.properties?.Hojd)) * heightScale,

    getFillColor: f => {
      // 1) DR selection highlight overrides everything
      if (f.properties && f.properties._drSelected) {
        const c = f.properties._drColor;
        if (Array.isArray(c) && c.length >= 3) {
          return [c[0], c[1], c[2]];      // color from UMAP (red/yellow/green)
        }
        // fallback if something went wrong
        return DR_SELECTION_COLOR_DEFAULT;
      }

      // 2) If there is an active DR selection, dim all other buildings
      if (drHasSelection && (pinnedChangeId == null || changeCompareBaseline)) {
        return DR_UNSELECTED_COLOR;
      }

      // 3) Building-type highlighting (purple for matches, dim non-matching)
      if (selectedBuildingType) {
        return featureMatchesSelectedType(f)
          ? SELECTED_BUILDING_TYPE_COLOR
          : DR_UNSELECTED_COLOR;
      }

      // 3b) Highlight pulse when user clicks a change entry
      if (f.properties?._changeHighlight) {
        return [255, 255, 255, 255]; // bright white flash
      }

      // 3c) What-if change highlight — colored for changed, white for unaffected
      if (pinnedChangeId != null && !changeCompareBaseline) {
        if (f.properties?._changeColor) {
          const c = f.properties._changeColor;
          return [c[0], c[1], c[2], c[3] ?? 220];
        }
        // No change for this building — white so changed buildings stand out
        return [255, 255, 255, 160];
      }

      // 4) Animated transition replay
      if (transitionAnimActive && transitionHasData) {
        return transitionBuildingColor(f.properties);
      }

      // 4a2) Priority zones — high need × poor access intervention overlay
      // (Phase-4). Highest-precedence lens so flagged pockets are never masked.
      if (typeof priorityZonesOn === 'function' && priorityZonesOn()) {
        return priorityColorForFeature(f);
      }

      // 4b) Demographic lens — color by who lives in the building's DESO
      // (Phase-4 alternate lens). Takes precedence over fairness coloring when
      // a field is selected, so planners can read demographics on the same map.
      if (typeof demoLensActive === 'function' && demoLensActive()) {
        return demoLensColorForFeature(f);
      }

      // 5) Map color variable = Mismatch: diverging color of proximity minus
      // available supply, lighting up where the Fairness and Supply views disagree
      // (see lib/supplyProvisionLens.js). Grey until both scores are stamped.
      if (typeof mismatchActive === 'function' && mismatchActive()) {
        return mismatchColorForFeature(f);
      }

      // 5b) Map color variable = Supply provision (2SFCA) instead of fairness.
      // A lens like the demographic one above: it paints whenever the Supply tab
      // is active, independent of the fairness analysis. Same green→purple ramp on
      // the pre-stamped _supplyScore (see lib/supplyProvisionLens.js); grey where
      // no baked 2SFCA row exists. The absolute adequacy mask is fairness-only, so
      // it's intentionally skipped here.
      if (typeof supplyActive === 'function' && supplyActive()) {
        return supplyColorForFeature(f);
      }

      // 5b) Fairness coloring if active (per-category mix selected by user)
      if (fairActive && f.properties?.fair) {
        // EXPERIMENTAL adequacy map mask (removable — see lib/absoluteColorScale.js):
        // grey out buildings below the chosen absolute provision bar.
        if (typeof window !== 'undefined' && typeof window.fairAdequacyRecolor === 'function') {
          const masked = window.fairAdequacyRecolor(f.properties);
          if (masked) return masked;
        }
        return colorFromScore(f.properties.fair.score);
      }

      // 6) Default category-based color (no POIs selected yet)
      return normalColor(f);
    },

    updateTriggers: {
      getElevation:[heightScale],
      getFillColor:[fairActive, fairCategory, fairRecolorTick, drSelectionTick, drHasSelection, buildingTypeTick, selectedBuildingType, transitionAnimTick, transitionAnimActive, changeLogTick, changeCompareBaseline, pinnedChangeId, (typeof demoLensField !== 'undefined' ? demoLensField : ''), (typeof demoLensTick !== 'undefined' ? demoLensTick : 0), (typeof priorityZonesActive !== 'undefined' ? priorityZonesActive : false), (typeof priorityZonesTick !== 'undefined' ? priorityZonesTick : 0), (typeof mapColorVar !== 'undefined' ? mapColorVar : 'fairness'), (typeof supplyTick !== 'undefined' ? supplyTick : 0)]
    }
  });
}


function createHighlightLayer(year) {
  if (viewMode !== 'new' || !year) return null;
  const feats = statsForYear(year);
  if (!feats.length) return null;
  const color = YEAR_COLORS[year] || [255,255,255];

  return new deck.GeoJsonLayer({
    id:'newbuilds-highlight', data:feats, pickable:true, extruded:true, wireframe:false, stroked:true, opacity:1,
    material: BUILDING_MATERIAL,
    getElevation: f => clampElev((f.properties?.height_m ?? f.properties?._mean ?? f.properties?.hojd ?? f.properties?.Hojd)) * heightScale,
    getFillColor: color, getLineColor:[255,255,255], getLineWidth:2, lineWidthUnits:'pixels',
    onClick: handleClick, updateTriggers:{ getElevation:[heightScale], data:[year] }
  });
}

function createRouteLayer() {
  if (!routeGeoJSON) return null;
  return new deck.GeoJsonLayer({
    id:'walking-route', data: routeGeoJSON, stroked:true, filled:false,
    getLineColor:[0,255,0], getLineWidth:4, lineWidthUnits:'pixels'
  });
}

function updateLayers() {
  if (!overlay) return;
  toggleLocalOnlyUI(sourceMode === 's1');

  const layers = [];
  const grayBackdrop = (viewMode === 'new' && !!selectedYear) || districtView || mezoView;

  const base = createCityLayer(grayBackdrop); if (base) layers.push(base);
  const hi = createHighlightLayer(selectedYear); if (hi) layers.push(hi);
  const mockOutline = createWhatIfMockBuildingsLayer(); if (mockOutline) layers.push(mockOutline);
  if (mezoView) {
    const mezoLayer = createMezoHexLayer();
    if (mezoLayer) layers.push(mezoLayer);
  }
  if (districtView) {
    const distPick = createDistrictPickLayer();
    const distFairness = createDistrictFairnessLayer();
    const districtBorder = createDistrictBoundaryLayer();
    if (distFairness) layers.push(distFairness);
    if (distPick) layers.push(distPick);
    if (districtBorder) layers.push(districtBorder);
  }

  const poiSymbols = createPOIPointLayer(); if (poiSymbols) layers.push(poiSymbols);
  const poiBldg = createPOIBuildingMarkerLayer(); if (poiBldg) layers.push(poiBldg);

  const route = createRouteLayer(); if (route) layers.push(route);

  createWhatIfSuggestionLayers().forEach(l => l && layers.push(l));

  overlay.setProps({ layers });
}

