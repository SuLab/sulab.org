class CysVisViewer {
  constructor({ viewerElementId, statusElementId, onResidueSelected, onVariantSelected }) {
    this.viewerElement = document.getElementById(viewerElementId);
    this.statusElement = document.getElementById(statusElementId);
    this.onResidueSelected = onResidueSelected;
    this.onVariantSelected = onVariantSelected;
    this.viewer = null;
    this.model = null;
    this.hoverLabel = null;
    this.surfaceHandle = null;
    this.currentProtein = null;
    this.currentVariantGroups = [];
    this.markerShapes = [];
    this.selectionShapes = [];
    this.renderState = null;
    this.pdbText = null;
    this.resizeHandler = () => {
      if (this.viewer) {
        this.viewer.resize();
        this.viewer.render();
      }
    };
  }

  init() {
    this.viewer = $3Dmol.createViewer(this.viewerElement, {
      backgroundColor: "#10161d",
    });
    window.addEventListener("resize", this.resizeHandler);
  }

  setStatus(message, isError = false) {
    this.statusElement.textContent = message;
    this.statusElement.classList.remove("hidden");
    this.statusElement.style.background = isError
      ? "rgba(88, 17, 17, 0.88)"
      : "rgba(10, 15, 20, 0.8)";
  }

  clearStatus() {
    this.statusElement.classList.add("hidden");
  }

  async loadProtein(protein, renderState, variantGroups = []) {
    this.currentProtein = protein;
    this.renderState = renderState;
    this.currentVariantGroups = variantGroups;
    this.setStatus(`Loading ${protein.name} AlphaFold structure...`);

    try {
      this.pdbText = await this.fetchPdbText(protein);
      this.rebuildScene();
      this.viewer.render();
      this.clearStatus();
    } catch (error) {
      this.setStatus(`Unable to load structure: ${error.message}`, true);
      throw error;
    }
  }

  async fetchPdbText(protein) {
    try {
      const response = await fetch(protein.structureApiUrl);
      if (!response.ok) {
        throw new Error(`AlphaFold API returned ${response.status}`);
      }

      const payload = await response.json();
      const pdbUrl = payload?.[0]?.pdbUrl || protein.fallbackPdbUrl;
      return this.fetchText(pdbUrl);
    } catch (error) {
      return this.fetchText(protein.fallbackPdbUrl);
    }
  }

  async fetchText(url) {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Structure file returned ${response.status}`);
    }
    return response.text();
  }

  updateRenderState(renderState) {
    this.renderState = renderState;
    if (this.pdbText) {
      this.rebuildScene(false);
      this.viewer.render();
    }
  }

  updateVariantGroups(variantGroups) {
    this.currentVariantGroups = variantGroups;
    if (this.pdbText) {
      this.rebuildScene(false);
      this.viewer.render();
    }
  }

  updateSelection(previousSelection, nextSelection) {
    this.renderState = {
      ...this.renderState,
      selectedCysteineResi: nextSelection.selectedCysteineResi,
      selectedVariantResidue: nextSelection.selectedVariantResidue,
    };

    if (!this.viewer || !this.pdbText) {
      return;
    }

    this.syncSelectionOverlay();
    this.viewer.render();
  }

  exportPngDataUri() {
    if (!this.viewer) {
      return "";
    }

    return this.viewer.pngURI();
  }

  getResidueCoordinates() {
    if (!this.pdbText) {
      return {};
    }

    return parsePdbResidueCoordinates(this.pdbText);
  }

  rebuildScene(resetView = true) {
    this.clearHover();
    // `viewer.clear()` drops all models and surfaces, so any previously saved
    // surface handle becomes invalid and must not be removed afterward.
    this.surfaceHandle = null;
    this.markerShapes = [];
    this.selectionShapes = [];
    this.viewer.clear();
    this.model = this.viewer.addModel(this.pdbText, "pdb");
    this.applyScene();

    if (resetView) {
      this.viewer.zoomTo();
    }
  }

  applyScene() {
    const protein = this.currentProtein;
    const state = this.renderState;
    const scheme = CYSVIS_ENCODING_SCHEMES[state.encodingScheme];
    this.markerShapes = [];

    protein.domains.forEach((domain) => {
      if (!state.visibleDomains[domain.key]) {
        return;
      }

      this.viewer.setStyle(
        { chain: "A", resi: `${domain.start}-${domain.end}` },
        { cartoon: { color: domain.color, opacity: 0.92 } }
      );
    });

    if (state.showCysteines) {
      protein.cysteines.forEach((cysteine) => {
        if (!state.visibleDomains[cysteine.domain]) {
          return;
        }

        const color = cysteine.annotations.includes(state.annotationLayer)
          ? scheme.cysteineColors.annotated
          : scheme.cysteineColors.unannotated;
        this.viewer.addStyle(
          { chain: "A", resi: cysteine.resi, atom: "SG" },
          { sphere: { radius: scheme.cysteineSphereRadius, color } }
        );
        this.viewer.addStyle(
          { chain: "A", resi: cysteine.resi, resn: "CYS" },
          {
            stick: {
              radius: scheme.cysteineStickRadius,
              color,
            },
          }
        );
      });
    }

    this.viewer.setClickable({}, false);
    this.viewer.setHoverable({}, false);

    if (state.showCysteines) {
      protein.cysteines.forEach((cysteine) => {
        if (!state.visibleDomains[cysteine.domain]) {
          return;
        }

        this.viewer.setClickable({ chain: "A", resi: cysteine.resi }, true, () => {
          this.onResidueSelected(cysteine);
        });

        this.viewer.setHoverable(
          { chain: "A", resi: cysteine.resi },
          true,
          (atom) => this.handleHover(atom, cysteine),
          () => this.clearHover()
        );
      });
    }

    if (state.showVariants) {
      this.currentVariantGroups.forEach((group) => {
        if (!state.visibleDomains[group.domain]) {
          return;
        }

        this.renderVariantMarker(group, scheme, state.variantGeometry);

        this.viewer.setClickable({ chain: "A", resi: group.residue, atom: "CA" }, true, () => {
          this.onVariantSelected(group);
        });

        this.viewer.setHoverable(
          { chain: "A", resi: group.residue, atom: "CA" },
          true,
          (atom) => this.handleHover(atom, {
            label: `${group.count} variant${group.count === 1 ? "" : "s"} @ ${group.residue}`,
          }),
          () => this.clearHover()
        );
      });
    }

    this.syncSurface();
    this.syncSelectionOverlay();
  }

  renderVariantMarker(group, scheme, geometryKey) {
    const color = scheme.variantColors[group.significanceBucket];
    const geometry = geometryKey || "sphere";

    if (geometry === "sphere") {
      this.viewer.addStyle(
        { chain: "A", resi: group.residue, atom: "CA" },
        {
          sphere: {
            radius: scheme.variantSphereRadius,
            color,
            opacity: 0.95,
          },
        }
      );
      return;
    }

    const atom = this.model?.selectedAtoms({ chain: "A", resi: group.residue, atom: "CA" })?.[0];
    if (!atom) {
      return;
    }

    const arm = geometry === "star" ? 0.9 : 0.75;
    const radius = geometry === "star" ? 0.12 : 0.1;
    const segments =
      geometry === "star"
        ? [
            [{ x: -arm, y: 0, z: 0 }, { x: arm, y: 0, z: 0 }],
            [{ x: 0, y: -arm, z: 0 }, { x: 0, y: arm, z: 0 }],
            [{ x: 0, y: 0, z: -arm }, { x: 0, y: 0, z: arm }],
            [{ x: -arm * 0.72, y: -arm * 0.72, z: 0 }, { x: arm * 0.72, y: arm * 0.72, z: 0 }],
            [{ x: -arm * 0.72, y: arm * 0.72, z: 0 }, { x: arm * 0.72, y: -arm * 0.72, z: 0 }],
          ]
        : [
            [{ x: -arm, y: 0, z: 0 }, { x: arm, y: 0, z: 0 }],
            [{ x: 0, y: -arm, z: 0 }, { x: 0, y: arm, z: 0 }],
          ];

    segments.forEach(([start, end]) => {
      const shape = this.viewer.addLine({
        start: { x: atom.x + start.x, y: atom.y + start.y, z: atom.z + start.z },
        end: { x: atom.x + end.x, y: atom.y + end.y, z: atom.z + end.z },
        color,
        radius,
        dashed: false,
      });
      this.markerShapes.push(shape);
    });
  }

  syncSelectionOverlay() {
    this.selectionShapes.forEach((shape) => {
      this.viewer.removeShape(shape);
    });
    this.selectionShapes = [];

    const selectedCysteineResi = this.renderState?.selectedCysteineResi;
    if (selectedCysteineResi == null) {
      return;
    }

    const atom =
      this.model?.selectedAtoms({ chain: "A", resi: selectedCysteineResi, atom: "SG" })?.[0] ||
      this.model?.selectedAtoms({ chain: "A", resi: selectedCysteineResi, atom: "CA" })?.[0];
    if (!atom) {
      return;
    }

    this.selectionShapes.push(
      this.viewer.addSphere({
        center: { x: atom.x, y: atom.y, z: atom.z },
        radius: 1.28,
        color: "#ffffff",
        opacity: 0.78,
      })
    );
  }

  syncSurface() {
    if (this.surfaceHandle) {
      this.viewer.removeSurface(this.surfaceHandle);
      this.surfaceHandle = null;
    }

    if (!this.renderState.showSurface) {
      return;
    }

    this.surfaceHandle = this.viewer.addSurface(
      $3Dmol.SurfaceType.VDW,
      { color: "#d9e6ec", opacity: 0.18 },
      {}
    );
  }

  handleHover(atom, item) {
    this.clearHover();
    this.hoverLabel = this.viewer.addLabel(item.label || `C${item.resi}`, {
      position: { x: atom.x, y: atom.y, z: atom.z },
      backgroundColor: "rgba(9, 15, 20, 0.85)",
      fontColor: "#f7fbfd",
      fontSize: 12,
      padding: 6,
      borderThickness: 0,
      inFront: true,
    });
    this.viewer.render();
  }

  clearHover() {
    if (this.hoverLabel) {
      this.viewer.removeLabel(this.hoverLabel);
      this.hoverLabel = null;
      this.viewer.render();
    }
  }
}
