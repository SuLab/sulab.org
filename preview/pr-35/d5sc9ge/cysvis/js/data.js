const CYSVIS_DOMAIN_DEFINITIONS = [
  { key: "NTR", label: "NTR", start: 1, end: 49, color: "#cccccc" },
  { key: "BTB", label: "BTB", start: 50, end: 179, color: "#f4a7a0" },
  { key: "IVR", label: "IVR", start: 180, end: 314, color: "#f0d06e" },
  { key: "Kelch", label: "Kelch", start: 315, end: 598, color: "#7eb8da" },
  { key: "CTR", label: "CTR", start: 599, end: 624, color: "#cccccc" },
];

const CYSVIS_ANNOTATION_LAYERS = {
  hyperreactive: {
    key: "hyperreactive",
    label: "Hyperreactive",
    description: "Chemoproteomic intrinsic reactivity annotation. No curated prototype assignments are loaded yet.",
  },
  ligandable: {
    key: "ligandable",
    label: "Ligandable",
    description: "Competitive chemoproteomic fragment-engagement annotation. No curated prototype assignments are loaded yet.",
  },
  redox_modified: {
    key: "redox_modified",
    label: "Redox modified",
    description: "Oxidative PTM annotation layer. No curated prototype assignments are loaded yet.",
  },
  catalytic: {
    key: "catalytic",
    label: "Catalytic",
    description: "Active-site or mechanistically essential cysteine annotations. No curated prototype assignments are loaded yet.",
  },
  disulfide: {
    key: "disulfide",
    label: "Disulfide",
    description: "Disulfide-bond annotation layer. No curated prototype assignments are loaded yet.",
  },
  metal_binding: {
    key: "metal_binding",
    label: "Metal binding",
    description: "Metal-coordinating cysteine annotation layer. No curated prototype assignments are loaded yet.",
  },
  uncharacterized: {
    key: "uncharacterized",
    label: "Uncharacterized",
    description: "Default prototype layer showing cysteines without additional curated cross-protein annotation.",
  },
};

const CYSVIS_VARIANT_COLORS = {
  pathogenic: "#d1495b",
  vus: "#d6aa1a",
  benign: "#4f9d69",
  other: "#6c7a84",
};

const CYSVIS_VARIANT_LABELS = {
  pathogenic: "Pathogenic / likely pathogenic",
  vus: "VUS / uncertain significance",
  benign: "Benign / likely benign",
  other: "Other / unclassified",
};

const CYSVIS_ENCODING_SCHEMES = {
  separated: {
    key: "separated",
    label: "Cool Cys / Warm Variants",
    description:
      "Recommended. Cysteines use cool colors while ClinVar keeps the familiar pathogenic red, VUS amber, and benign green.",
    cysteineColors: {
      annotated: "#b44f5f",
      unannotated: "#87919a",
    },
    variantColors: {
      pathogenic: "#d1495b",
      vus: "#d6aa1a",
      benign: "#4f9d69",
      other: "#6c7a84",
    },
    cysteineSphereRadius: 0.82,
    cysteineStickRadius: 0.16,
    variantSphereRadius: 0.5,
  },
  monochrome: {
    key: "monochrome",
    label: "Muted Cys / Semantic Variants",
    description:
      "Pushes most of the semantic weight onto the variant layer. Cysteines stay visible but use a restrained steel-blue family.",
    cysteineColors: {
      annotated: "#a7c4d6",
      unannotated: "#546773",
    },
    variantColors: {
      pathogenic: "#c43c4e",
      vus: "#dda020",
      benign: "#3f9b5f",
      other: "#72808a",
    },
    cysteineSphereRadius: 0.78,
    cysteineStickRadius: 0.14,
    variantSphereRadius: 0.58,
  },
  size_split: {
    key: "size_split",
    label: "Large Cys / Small Variants",
    description:
      "Adds stronger size separation so native cysteines read as landmarks while variant markers feel like overlays.",
    cysteineColors: {
      annotated: "#b44f5f",
      unannotated: "#87919a",
    },
    variantColors: {
      pathogenic: "#e24a33",
      vus: "#d2b11f",
      benign: "#47a36f",
      other: "#6c7a84",
    },
    cysteineSphereRadius: 0.94,
    cysteineStickRadius: 0.18,
    variantSphereRadius: 0.42,
  },
};

const CYSVIS_VARIANT_GEOMETRIES = {
  sphere: {
    key: "sphere",
    label: "Small Spheres",
    description: "Compact and familiar. Best when you want the quietest overlay.",
  },
  cross: {
    key: "cross",
    label: "Cross Markers",
    description: "Reads more like a mutation/event marker and competes less with native residues.",
  },
  star: {
    key: "star",
    label: "Starburst Markers",
    description: "Most distinct from cysteines. Useful when variant overlays need to stand apart strongly.",
  },
};

const CYSVIS_PROTEINS = {
  Q14145: {
    name: "KEAP1",
    displayName: "KEAP1",
    geneSymbol: "KEAP1",
    uniprotId: "Q14145",
    length: 624,
    structureApiUrl: "https://alphafold.ebi.ac.uk/api/prediction/Q14145",
    fallbackPdbUrl: "https://alphafold.ebi.ac.uk/files/AF-Q14145-F1-model_v4.pdb",
    isCurated: true,
    note: "AlphaFold monomer view. KEAP1 functions biologically as a homodimer.",
    domains: CYSVIS_DOMAIN_DEFINITIONS,
    cysteines: [
      { resi: 13, domain: "NTR", annotations: ["uncharacterized"] },
      { resi: 14, domain: "NTR", annotations: ["uncharacterized"] },
      { resi: 23, domain: "NTR", annotations: ["uncharacterized"] },
      { resi: 38, domain: "NTR", annotations: ["uncharacterized"] },
      { resi: 77, domain: "BTB", annotations: ["uncharacterized"] },
      {
        resi: 151,
        domain: "BTB",
        annotations: ["uncharacterized"],
        notes:
          "KEAP1-specific note: primary electrophile sensor. Surrounded by basic residues (H129, K131, R135, K150, H154) that lower its pKa. Required for NRF2 stabilization by sulforaphane and oxidative stress. Most reactive cysteine in KEAP1.",
      },
      { resi: 171, domain: "BTB", annotations: ["uncharacterized"] },
      { resi: 196, domain: "IVR", annotations: ["uncharacterized"] },
      {
        resi: 226,
        domain: "IVR",
        annotations: ["uncharacterized"],
        notes: "Labeled by thiol-reactive reagents in vitro.",
      },
      { resi: 241, domain: "IVR", annotations: ["uncharacterized"] },
      { resi: 249, domain: "IVR", annotations: ["uncharacterized"] },
      {
        resi: 257,
        domain: "IVR",
        annotations: ["uncharacterized"],
        notes: "Preferred site of thiol labeling in vitro.",
      },
      {
        resi: 273,
        domain: "IVR",
        annotations: ["uncharacterized"],
        notes:
          "KEAP1-specific note: critical sensor cysteine for KEAP1-dependent ubiquitination of NRF2. Required for basal repression. May participate in zinc binding or direct ubiquitin transfer. Adjacent to R272 and H274.",
      },
      {
        resi: 288,
        domain: "IVR",
        annotations: ["uncharacterized"],
        notes:
          "KEAP1-specific note: critical sensor cysteine that works with C273 for basal NRF2 repression. Adjacent to K287.",
      },
      {
        resi: 297,
        domain: "IVR",
        annotations: ["uncharacterized"],
        notes: "Preferred site of thiol labeling in vitro.",
      },
      { resi: 319, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 368, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 395, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 406, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 434, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 489, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 513, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 518, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 583, domain: "Kelch", annotations: ["uncharacterized"] },
      { resi: 613, domain: "CTR", annotations: ["uncharacterized"] },
      { resi: 622, domain: "CTR", annotations: ["uncharacterized"] },
      { resi: 624, domain: "CTR", annotations: ["uncharacterized"] },
    ],
  },
};

function detectCysteinesFromSequence(sequence, domains) {
  const domainMap = domains || [];
  const cysteines = [];

  for (let index = 0; index < sequence.length; index += 1) {
    if (sequence[index] !== "C") {
      continue;
    }

    const residue = index + 1;
    const domain = domainMap.find((entry) => residue >= entry.start && residue <= entry.end);
    cysteines.push({
      resi: residue,
      domain: domain ? domain.key : "Protein",
      annotations: ["uncharacterized"],
      notes: "Auto-detected cysteine from the UniProt canonical sequence. No curated reactivity annotation is available for this protein in the prototype dataset.",
    });
  }

  return cysteines;
}

function buildDynamicProteinFromAlphaFold(uniprotId, alphaFoldEntry) {
  const length = alphaFoldEntry.sequence?.length || (alphaFoldEntry.sequenceEnd - alphaFoldEntry.sequenceStart + 1);
  const domains = [
    {
      key: "Protein",
      label: "Protein",
      start: 1,
      end: length,
      color: "#9bb1be",
    },
  ];

  return {
    name: alphaFoldEntry.gene || uniprotId,
    displayName: alphaFoldEntry.gene || alphaFoldEntry.uniprotId || uniprotId,
    geneSymbol: alphaFoldEntry.gene || "",
    uniprotId,
    length,
    structureApiUrl: `https://alphafold.ebi.ac.uk/api/prediction/${uniprotId}`,
    fallbackPdbUrl: alphaFoldEntry.pdbUrl,
    isCurated: false,
    note: "Generic AlphaFold view. Cysteines are auto-detected from sequence, and domain-specific annotations are only curated for KEAP1 in this prototype.",
    domains,
    cysteines: detectCysteinesFromSequence(alphaFoldEntry.sequence || "", domains),
  };
}
