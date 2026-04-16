(function bootstrapCysVis() {
  const DEFAULT_UNIPROT_ID = "P00533";
  const defaultProtein = createPlaceholderProtein(DEFAULT_UNIPROT_ID, "EGFR");
  let loadRequestId = 0;

  const state = {
    protein: defaultProtein,
    variantsData: createEmptyVariantsState(),
    hotspotData: createEmptyHotspotState(),
    renderState: {
      visibleDomains: defaultProtein.domains.reduce((accumulator, domain) => {
        accumulator[domain.key] = true;
        return accumulator;
      }, {}),
      showCysteines: true,
      showVariants: true,
      showSurface: false,
      encodingScheme: "size_split",
      variantGeometry: "sphere",
      annotationLayer: "uncharacterized",
      selectedCysteineResi: null,
      selectedVariantResidue: null,
    },
  };

  const ui = new CysVisUI({
    onProteinSubmit: handleProteinSubmit,
    onDomainToggle: handleDomainToggle,
    onLayerToggle: handleLayerToggle,
    onExportPng: handleExportPng,
    onAnnotationLayerChange: handleAnnotationLayerChange,
    onCysteineTableSelect: handleResidueSelected,
  });

  const viewer = new CysVisViewer({
    viewerElementId: "viewer",
    statusElementId: "viewer-status",
    onResidueSelected: handleResidueSelected,
    onVariantSelected: handleVariantSelected,
  });

  ui.init();
  viewer.init();
  syncUi();
  exposeTestApi();
  handleProteinSubmit(DEFAULT_UNIPROT_ID);

  function syncUi() {
    ui.renderProteinSummary(state.protein, state.variantsData);
    ui.renderDomainControls(state.protein, state.renderState.visibleDomains);
    ui.renderAnnotationLayerControl(state.renderState.annotationLayer);
    ui.renderLegend(state.renderState.encodingScheme, state.renderState.annotationLayer);
    ui.renderCysteineTable(state.protein, state.variantsData, state.hotspotData, state.renderState.annotationLayer);
    ui.showDefaultDetail(state.protein, state.variantsData, state.hotspotData);
  }

  async function loadActiveProtein() {
    const requestId = ++loadRequestId;
    state.variantsData = createEmptyVariantsState(true);
    state.hotspotData = createEmptyHotspotState(true);
    syncUi();

    try {
      await viewer.loadProtein(state.protein, state.renderState, state.variantsData.groups);
    } catch (_) {
      return;
    }

    try {
      const variantsData = await fetchClinvarVariants(state.protein);
      if (requestId !== loadRequestId) {
        return;
      }

      state.variantsData = variantsData;
      viewer.updateVariantGroups(variantsData.displayedGroups || variantsData.groups);
      state.hotspotData = createEmptyHotspotState(true);
      syncUi();
      computeHotspotStateAsync(requestId);
    } catch (error) {
      if (requestId !== loadRequestId) {
        return;
      }

      state.variantsData = createEmptyVariantsState(false, error.message);
      state.hotspotData = createEmptyHotspotState(false, "Hotspot analysis requires ClinVar variant data.");
      viewer.updateVariantGroups([]);
      syncUi();
    }
  }

  async function handleProteinSubmit(uniprotId) {
    if (!uniprotId) {
      return;
    }

    const requestedId = uniprotId.toUpperCase();
    const curatedProtein = CYSVIS_PROTEINS[requestedId];

    if (curatedProtein) {
      state.protein = curatedProtein;
      state.renderState.visibleDomains = curatedProtein.domains.reduce((accumulator, domain) => {
        accumulator[domain.key] = true;
        return accumulator;
      }, {});
      clearSelection();
      loadActiveProtein();
      return;
    }

    try {
      const dynamicProtein = await fetchDynamicProtein(requestedId);
      state.protein = dynamicProtein;
      state.renderState.visibleDomains = dynamicProtein.domains.reduce((accumulator, domain) => {
        accumulator[domain.key] = true;
        return accumulator;
      }, {});
      clearSelection();
      loadActiveProtein();
    } catch (error) {
      ui.showProteinLoadError(requestedId, error.message);
    }
  }

  function handleDomainToggle(domainKey, isVisible) {
    state.renderState.visibleDomains[domainKey] = isVisible;
    viewer.updateRenderState(state.renderState);
  }

  function handleLayerToggle(layerKey, isVisible) {
    state.renderState[layerKey] = isVisible;
    viewer.updateRenderState(state.renderState);
  }

  function handleAnnotationLayerChange(annotationLayer) {
    state.renderState.annotationLayer = annotationLayer;
    ui.renderAnnotationLayerControl(annotationLayer);
    ui.renderLegend(state.renderState.encodingScheme, state.renderState.annotationLayer);
    viewer.updateRenderState(state.renderState);
  }

  function handleResidueSelected(cysteine) {
    const previousSelection = {
      selectedCysteineResi: state.renderState.selectedCysteineResi,
      selectedVariantResidue: state.renderState.selectedVariantResidue,
    };
    state.renderState.selectedCysteineResi = cysteine.resi;
    state.renderState.selectedVariantResidue = null;
    viewer.updateSelection(previousSelection, {
      selectedCysteineResi: state.renderState.selectedCysteineResi,
      selectedVariantResidue: state.renderState.selectedVariantResidue,
    });
    ui.showResidueDetail(state.protein, cysteine, state.variantsData, state.hotspotData);
  }

  function handleVariantSelected(group) {
    const previousSelection = {
      selectedCysteineResi: state.renderState.selectedCysteineResi,
      selectedVariantResidue: state.renderState.selectedVariantResidue,
    };
    state.renderState.selectedCysteineResi = null;
    state.renderState.selectedVariantResidue = group.residue;
    viewer.updateSelection(previousSelection, {
      selectedCysteineResi: state.renderState.selectedCysteineResi,
      selectedVariantResidue: state.renderState.selectedVariantResidue,
    });
    ui.showVariantDetail(state.protein, group);
  }

  function clearSelection() {
    state.renderState.selectedCysteineResi = null;
    state.renderState.selectedVariantResidue = null;
  }

  function handleExportPng() {
    const pngUri = viewer.exportPngDataUri();
    if (!pngUri) {
      return;
    }

    const link = document.createElement("a");
    link.href = pngUri;
    link.download = `${state.protein.uniprotId.toLowerCase()}-cysvis.png`;
    link.click();
  }

  function createEmptyVariantsState(isLoading = false, error = "") {
    return {
      loading: isLoading,
      error,
      rawVariantCount: 0,
      groupedResidueCount: 0,
      groups: [],
      groupsByResidue: {},
      displayedGroups: [],
      displayedResidueCount: 0,
      displayPolicy: "all",
      displayPolicyNote: "",
    };
  }

  function createEmptyHotspotState(isLoading = false, error = "") {
    return {
      loading: isLoading,
      ready: false,
      error,
      byResidue: {},
      summary: {
        pathogenicResidueCount: 0,
        permutationCount: 0,
        eligibleResidueCount: 0,
      },
      cacheKey: "",
    };
  }

  function createPlaceholderProtein(uniprotId, name) {
    return {
      name,
      displayName: name,
      geneSymbol: name,
      uniprotId,
      length: 0,
      structureApiUrl: `https://alphafold.ebi.ac.uk/api/prediction/${uniprotId}`,
      fallbackPdbUrl: "",
      isCurated: false,
      note: "Loading live AlphaFold and ClinVar data for the default protein.",
      domains: [{ key: "Protein", label: "Protein", start: 1, end: 1, color: "#9bb1be" }],
      cysteines: [],
    };
  }

  async function computeHotspotStateAsync(requestId) {
    const residueCoordinates = viewer.getResidueCoordinates();
    const cacheKey = [
      state.protein.uniprotId,
      Object.keys(residueCoordinates).length,
      state.variantsData.groups
        .filter((group) => group.significanceBucket === "pathogenic")
        .map((group) => `${group.residue}:${group.significanceBucket}`)
        .join("|"),
    ].join("::");

    if (state.hotspotData.cacheKey === cacheKey && state.hotspotData.ready) {
      return;
    }

    try {
      await yieldToBrowser();
      const analysis = await analyzeCysteineHotspotsAsync({
        protein: state.protein,
        variantGroups: state.variantsData.groups,
        residueCoordinates,
      });

      if (requestId !== loadRequestId) {
        return;
      }

      state.hotspotData = {
        loading: false,
        ready: analysis.ready,
        error: analysis.error,
        byResidue: analysis.byResidue,
        summary: analysis.summary,
        cacheKey,
      };
      syncUi();
    } catch (error) {
      if (requestId !== loadRequestId) {
        return;
      }

      state.hotspotData = createEmptyHotspotState(false, error.message || "Hotspot analysis failed.");
      state.hotspotData.cacheKey = cacheKey;
      syncUi();
    }
  }

  function exposeTestApi() {
    window.__cysvisTestApi = {
      getState() {
        return {
          protein: {
            name: state.protein.name,
            displayName: state.protein.displayName,
            uniprotId: state.protein.uniprotId,
            length: state.protein.length,
            cysteineCount: state.protein.cysteines.length,
            isCurated: state.protein.isCurated,
          },
          renderState: structuredClone(state.renderState),
          variants: {
            loading: state.variantsData.loading,
            error: state.variantsData.error,
            rawVariantCount: state.variantsData.rawVariantCount,
            groupedResidueCount: state.variantsData.groupedResidueCount,
            displayedResidueCount: state.variantsData.displayedResidueCount,
            displayPolicy: state.variantsData.displayPolicy,
          },
          hotspots: {
            loading: state.hotspotData.loading,
            ready: state.hotspotData.ready,
            error: state.hotspotData.error,
            pathogenicResidueCount: state.hotspotData.summary.pathogenicResidueCount,
            permutationCount: state.hotspotData.summary.permutationCount,
          },
          statusHidden: document.getElementById("viewer-status").classList.contains("hidden"),
          detailText: document.getElementById("detail-content").textContent,
        };
      },
      selectCysteine(resi) {
        const cysteine = state.protein.cysteines.find((entry) => entry.resi === resi);
        if (!cysteine) {
          return false;
        }

        handleResidueSelected(cysteine);
        return true;
      },
      selectVariantResidue(resi) {
        const group = state.variantsData.groupsByResidue[resi];
        if (!group) {
          return false;
        }

        handleVariantSelected(group);
        return true;
      },
      hasSurface() {
        return Boolean(viewer.surfaceHandle);
      },
      hasExportButton() {
        return Boolean(document.getElementById("export-png"));
      },
    };
  }

  async function fetchDynamicProtein(uniprotId) {
    const response = await fetch(`https://alphafold.ebi.ac.uk/api/prediction/${uniprotId}`);
    if (!response.ok) {
      throw new Error(`AlphaFold returned ${response.status}. Try a reviewed UniProt accession with an available model.`);
    }

    const payload = await response.json();
    if (!Array.isArray(payload) || payload.length === 0) {
      throw new Error("AlphaFold did not return a model for this UniProt accession.");
    }

    const normalized = uniprotId.toUpperCase();
    const selectedEntry =
      payload.find((entry) => entry.uniprotAccession?.toUpperCase() === normalized) ||
      payload.find((entry) => entry.entryId?.toUpperCase().includes(normalized)) ||
      payload[0];

    return buildDynamicProteinFromAlphaFold(uniprotId, selectedEntry);
  }

  function yieldToBrowser() {
    return new Promise((resolve) => {
      if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
        window.requestAnimationFrame(() => resolve());
        return;
      }

      setTimeout(resolve, 0);
    });
  }
})();
