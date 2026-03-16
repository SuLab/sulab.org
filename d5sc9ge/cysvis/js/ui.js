class CysVisUI {
  constructor({ onProteinSubmit, onDomainToggle, onLayerToggle, onExportPng, onAnnotationLayerChange }) {
    this.onProteinSubmit = onProteinSubmit;
    this.onDomainToggle = onDomainToggle;
    this.onLayerToggle = onLayerToggle;
    this.onExportPng = onExportPng;
    this.onAnnotationLayerChange = onAnnotationLayerChange;
    this.domainControlsElement = document.getElementById("domain-controls");
    this.summaryElement = document.getElementById("protein-summary");
    this.proteinNoteElement = document.getElementById("protein-note");
    this.detailElement = document.getElementById("detail-content");
    this.legendElement = document.getElementById("legend");
    this.formElement = document.getElementById("protein-form");
    this.proteinInput = document.getElementById("protein-id");
    this.exportButton = document.getElementById("export-png");
    this.annotationLayerSelect = document.getElementById("annotation-layer-select");
    this.annotationLayerNote = document.getElementById("annotation-layer-note");
    this.cysteineToggle = document.getElementById("toggle-cysteines");
    this.variantToggle = document.getElementById("toggle-variants");
    this.surfaceToggle = document.getElementById("toggle-surface");
  }

  init() {
    Object.values(CYSVIS_ANNOTATION_LAYERS).forEach((layer) => {
      const option = document.createElement("option");
      option.value = layer.key;
      option.textContent = layer.label;
      this.annotationLayerSelect.appendChild(option);
    });

    this.formElement.addEventListener("submit", (event) => {
      event.preventDefault();
      this.onProteinSubmit(this.proteinInput.value.trim().toUpperCase());
    });

    this.cysteineToggle.addEventListener("change", () => {
      this.onLayerToggle("showCysteines", this.cysteineToggle.checked);
    });

    this.variantToggle.addEventListener("change", () => {
      this.onLayerToggle("showVariants", this.variantToggle.checked);
    });

    this.surfaceToggle.addEventListener("change", () => {
      this.onLayerToggle("showSurface", this.surfaceToggle.checked);
    });

    this.annotationLayerSelect.addEventListener("change", () => {
      this.onAnnotationLayerChange(this.annotationLayerSelect.value);
    });

    this.exportButton.addEventListener("click", () => {
      this.onExportPng();
    });
  }

  renderAnnotationLayerControl(selectedLayer) {
    this.annotationLayerSelect.value = selectedLayer;
    this.annotationLayerNote.textContent = CYSVIS_ANNOTATION_LAYERS[selectedLayer].description;
  }

  renderLegend(selectedKey, annotationLayer) {
    const scheme = CYSVIS_ENCODING_SCHEMES[selectedKey];
    const legendEntries = [
      { color: scheme.cysteineColors.annotated, label: `${CYSVIS_ANNOTATION_LAYERS[annotationLayer].label} cysteine` },
      { color: scheme.cysteineColors.unannotated, label: `Not annotated for ${CYSVIS_ANNOTATION_LAYERS[annotationLayer].label.toLowerCase()}` },
      { color: scheme.variantColors.pathogenic, label: "Pathogenic / likely pathogenic" },
      { color: scheme.variantColors.vus, label: "VUS / uncertain significance" },
      { color: scheme.variantColors.benign, label: "Benign / likely benign" },
    ];

    this.legendElement.innerHTML = legendEntries
      .map(
        (entry) => `
          <div class="legend-item">
            <span class="swatch" style="background:${entry.color}"></span>${entry.label}
          </div>
        `
      )
      .join("");
  }

  renderProteinSummary(protein, variantsData) {
    const cysteineCount = protein.cysteines.length;
    const variantValue = variantsData.loading
      ? "Loading..."
      : variantsData.error
        ? "Unavailable"
        : `${variantsData.rawVariantCount} across ${variantsData.groupedResidueCount} residues`;

    this.summaryElement.innerHTML = `
      <div class="summary-row"><span>Name</span><strong>${protein.name}</strong></div>
      <div class="summary-row"><span>UniProt</span><strong>${protein.uniprotId}</strong></div>
      <div class="summary-row"><span>Length</span><strong>${protein.length} aa</strong></div>
      <div class="summary-row"><span>Cysteines</span><strong>${cysteineCount}</strong></div>
      <div class="summary-row"><span>ClinVar</span><strong>${variantValue}</strong></div>
    `;
    const noteParts = [protein.note || "", variantsData.displayPolicyNote || ""].filter(Boolean);
    this.proteinNoteElement.textContent = noteParts.join(" ");
  }

  renderDomainControls(protein, visibleDomains) {
    this.domainControlsElement.innerHTML = "";

    protein.domains.forEach((domain) => {
      const label = document.createElement("label");
      label.className = "control-item";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = visibleDomains[domain.key];
      checkbox.addEventListener("change", () => {
        this.onDomainToggle(domain.key, checkbox.checked);
      });

      const text = document.createElement("span");
      text.textContent = `${domain.label} (${domain.start}-${domain.end})`;

      label.appendChild(checkbox);
      label.appendChild(text);
      this.domainControlsElement.appendChild(label);
    });
  }

  showDefaultDetail(protein, variantsData, hotspotData) {
    const variantLine = variantsData.loading
      ? "ClinVar variants are loading."
      : variantsData.error
        ? `ClinVar variants could not be fetched: ${variantsData.error}`
        : `ClinVar layer includes ${variantsData.rawVariantCount} parsed variants across ${variantsData.groupedResidueCount} residue positions.`;
    const hotspotLine = hotspotData.loading
      ? "3D hotspot analysis is running."
      : hotspotData.error
        ? `3D hotspot analysis is unavailable: ${hotspotData.error}`
        : hotspotData.summary.pathogenicResidueCount
          ? `3D hotspot analysis is ready for ${hotspotData.summary.pathogenicResidueCount} pathogenic residue position${hotspotData.summary.pathogenicResidueCount === 1 ? "" : "s"}.`
          : "3D hotspot analysis is waiting for pathogenic ClinVar residue positions.";

    this.detailElement.innerHTML = `
      <p class="detail-title">${protein.displayName} cysteine atlas</p>
      <p class="detail-meta">
        Select a cysteine or ClinVar marker in the structure to inspect functional annotations
        and residue-level variant context.
      </p>
      ${
        protein.isCurated
          ? ""
          : '<p class="detail-callout">This protein is loaded through the generic AlphaFold path. Cysteines are auto-detected from sequence and curated domain-level cysteine annotations are only available for KEAP1 in this prototype.</p>'
      }
      <p>
        ${variantLine}
      </p>
      <p>
        ${hotspotLine}
      </p>
    `;
  }

  showResidueDetail(protein, cysteine, variantsData, hotspotData) {
    const notes = cysteine.notes
      ? cysteine.notes
      : "No additional functional annotation is curated for this residue in the MVP dataset.";
    const exactGroup = variantsData.groupsByResidue[cysteine.resi];
    const nearbyGroups = variantsData.groups.filter(
      (group) => Math.abs(group.residue - cysteine.resi) > 0 && Math.abs(group.residue - cysteine.resi) <= 3
    );

    const exactSection = exactGroup
      ? `
        <p class="detail-section-title">ClinVar variants at this cysteine</p>
        <ul class="detail-list">
          ${exactGroup.variants
            .map(
              (variant) => `
                <li>
                  <strong>${variant.proteinHgvs}</strong> — ${variant.significance}
                  ${variant.accession ? `[${variant.accession}]` : ""}
                </li>
              `
            )
            .join("")}
        </ul>
      `
      : `
        <p class="detail-section-title">ClinVar variants at this cysteine</p>
        <p>No ClinVar records currently map directly to C${cysteine.resi}.</p>
      `;

    const nearbySection = nearbyGroups.length
      ? `
        <p class="detail-section-title">Nearby variants (±3 residues)</p>
        <ul class="detail-list">
          ${nearbyGroups
            .map(
              (group) => `
                <li>
                  <strong>Residue ${group.residue}</strong> — ${group.count} variant${group.count === 1 ? "" : "s"}
                  (${group.significanceLabel})
                </li>
              `
            )
            .join("")}
        </ul>
      `
      : "";

    const annotationSummary = cysteine.annotations.length
      ? cysteine.annotations.map((annotation) => CYSVIS_ANNOTATION_LAYERS[annotation]?.label || annotation).join(", ")
      : "None";
    const hotspot = hotspotData.byResidue[cysteine.resi];
    const hotspotSection = hotspotData.loading
      ? `
        <p class="detail-section-title">3D pathogenic hotspot</p>
        <p>Analyzing pathogenic variant proximity in 3D space...</p>
      `
      : hotspotData.error
        ? `
          <p class="detail-section-title">3D pathogenic hotspot</p>
          <p>${hotspotData.error}</p>
        `
        : hotspotData.summary.pathogenicResidueCount === 0
          ? `
            <p class="detail-section-title">3D pathogenic hotspot</p>
            <p>No pathogenic or likely pathogenic ClinVar residue positions are available for the currently loaded protein, so a hotspot score cannot be computed yet.</p>
          `
        : hotspot
          ? `
            <p class="detail-section-title">3D pathogenic hotspot</p>
            <ul class="detail-list">
              <li><strong>Nearest pathogenic residue:</strong> ${hotspot.nearestPathogenicDistance === null ? "None detected" : `${hotspot.nearestPathogenicDistance} A`}</li>
              <li><strong>Pathogenic residues within ${CYSVIS_HOTSPOT_CONFIG.nearDistanceAngstrom} A:</strong> ${hotspot.pathogenicWithinNearDistance}</li>
              <li><strong>Weighted local burden:</strong> ${hotspot.weightedBurden}</li>
              <li><strong>Hotspot percentile:</strong> ${hotspot.percentile === null ? "Unavailable" : `${hotspot.percentile.toFixed(1)}th percentile`}</li>
              <li><strong>Empirical p-value:</strong> ${hotspot.empiricalPValue === null ? "Unavailable" : hotspot.empiricalPValue.toFixed(3)}</li>
            </ul>
            ${
              hotspot.nearbyResidues.length
                ? `
                  <p class="detail-section-title">Nearby pathogenic residues (<= ${CYSVIS_HOTSPOT_CONFIG.maxDistanceAngstrom} A)</p>
                  <ul class="detail-list">
                    ${hotspot.nearbyResidues
                      .map(
                        (entry) => `
                          <li>
                            <strong>Residue ${entry.residue}</strong> — ${entry.distance} A
                            (${entry.significanceLabel})
                          </li>
                        `
                      )
                      .join("")}
                  </ul>
                `
                : "<p>No pathogenic residues fall within the local 3D neighborhood threshold.</p>"
            }
          `
          : `
            <p class="detail-section-title">3D pathogenic hotspot</p>
            <p>This residue could not be scored because a matching structural coordinate was not found.</p>
          `;

    this.detailElement.innerHTML = `
      <div class="detail-chip-row">
        <div class="detail-chip detail-chip-selected">Selected</div>
        <div class="detail-chip">Annotations: ${annotationSummary}</div>
      </div>
      <p class="detail-title">C${cysteine.resi} (${cysteine.domain} domain)</p>
      <p class="detail-meta">Residue ${cysteine.resi} of ${protein.displayName}</p>
      <p>${notes}</p>
      ${hotspotSection}
      ${exactSection}
      ${nearbySection}
    `;
  }

  showVariantDetail(protein, group) {
    const overlapCallout = group.createsCysteine
      ? `<p><strong>This variant set creates a cysteine at position ${group.residue}.</strong></p>`
      : group.destroysCysteine
        ? `<p><strong>This variant set disrupts the native cysteine at C${group.residue}.</strong></p>`
        : group.cysteineAtPosition
          ? `<p><strong>This variant set lands directly on annotated cysteine C${group.residue}.</strong></p>`
          : group.nearbyCysteines.length
            ? `<p><strong>Nearest annotated cysteine:</strong> C${group.nearbyCysteines[0].resi} (${group.nearbyCysteines[0].distance} residue${group.nearbyCysteines[0].distance === 1 ? "" : "s"} away).</p>`
            : "";

    this.detailElement.innerHTML = `
      <div class="detail-chip-row">
        <div class="detail-chip detail-chip-selected">Selected</div>
        <div class="detail-chip">${group.significanceLabel}</div>
      </div>
      <p class="detail-title">Residue ${group.residue} variant cluster (${group.domain} domain)</p>
      <p class="detail-meta">${group.count} ClinVar variant${group.count === 1 ? "" : "s"} in ${protein.displayName}</p>
      ${overlapCallout}
      <p class="detail-section-title">ClinVar entries</p>
      <ul class="detail-list">
        ${group.variants
          .map(
            (variant) => `
              <li>
                <strong>${variant.proteinHgvs}</strong>
                ${variant.codingHgvs ? `(${variant.codingHgvs})` : ""}
                — ${variant.significance}
                ${variant.conditions.length ? `; ${variant.conditions.join(", ")}` : ""}
              </li>
            `
          )
          .join("")}
      </ul>
    `;
  }

  showProteinLoadError(uniprotId, message) {
    this.detailElement.innerHTML = `
      <p class="detail-title">Unable to load protein</p>
      <p>
        ${uniprotId} could not be loaded from AlphaFold.
      </p>
      <p>${message}</p>
    `;
  }
}
