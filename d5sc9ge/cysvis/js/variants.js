function parseHGVSProtein(hgvsProtein) {
  if (!hgvsProtein || typeof hgvsProtein !== "string") {
    return null;
  }

  const normalized = hgvsProtein.includes(":")
    ? hgvsProtein.split(":").pop()
    : hgvsProtein;
  const match = normalized.match(/^p\.([A-Z][a-z]{2})(\d+)([A-Z][a-z]{2}|Ter|=|fs.*|del.*|dup.*|ins.*|\?)/);

  if (!match) {
    return null;
  }

  return {
    proteinHgvs: normalized,
    refAa: match[1],
    residue: Number(match[2]),
    altAa: match[3],
  };
}

function extractProteinHgvsFromText(text) {
  if (!text || typeof text !== "string") {
    return null;
  }

  const match = text.match(/\(p\.([A-Z][a-z]{2}\d+(?:[A-Z][a-z]{2}|Ter|=|fs[^\)]*|del[^\)]*|dup[^\)]*|ins[^\)]*|\?))\)/);
  return match ? `p.${match[1]}` : null;
}

function getDomainForResidue(protein, residue) {
  return protein.domains.find((domain) => residue >= domain.start && residue <= domain.end) || null;
}

function normalizeToArray(value) {
  if (Array.isArray(value)) {
    return value;
  }

  if (value == null) {
    return [];
  }

  return [value];
}

function bucketClinicalSignificance(significance) {
  const normalized = (significance || "").toLowerCase();

  if (normalized.includes("pathogenic")) {
    return normalized.includes("benign") ? "other" : "pathogenic";
  }

  if (normalized.includes("uncertain") || normalized.includes("vus")) {
    return "vus";
  }

  if (normalized.includes("benign")) {
    return "benign";
  }

  return "other";
}

function collectClinicalAssertions(rcvEntries) {
  const assertions = normalizeToArray(rcvEntries)
    .filter(Boolean)
    .map((entry) => ({
      accession: entry.accession || "",
      significance: entry.clinical_significance || "Not provided",
      bucket: bucketClinicalSignificance(entry.clinical_significance || ""),
      conditions: normalizeToArray(entry.conditions?.name).filter(Boolean),
      preferredName: entry.preferred_name || "",
      origin: entry.origin || "",
      reviewStatus: entry.review_status || "",
    }));

  return assertions;
}

function scoreAssertionPriority(assertion) {
  const germlineBonus = assertion.origin.toLowerCase().includes("germline") ? 10 : 0;
  const reviewBonus = assertion.reviewStatus.toLowerCase().includes("criteria provided") ? 1 : 0;
  return bucketRank(assertion.bucket) * 100 + germlineBonus + reviewBonus;
}

function summarizeClinicalAssertions(assertions) {
  if (!assertions.length) {
    return {
      significance: "Not provided",
      significanceBucket: "other",
      accession: "",
      origin: "",
      preferredName: "",
      conditions: [],
      assertionSummary: [],
    };
  }

  const strongest = assertions
    .slice()
    .sort((left, right) => scoreAssertionPriority(right) - scoreAssertionPriority(left))[0];

  const summary = Array.from(
    new Set(
      assertions.map((assertion) =>
        `${assertion.significance}${assertion.origin ? ` (${assertion.origin})` : ""}`
      )
    )
  );

  return {
    significance: strongest.significance,
    significanceBucket: strongest.bucket,
    accession: strongest.accession,
    origin: strongest.origin,
    preferredName: strongest.preferredName,
    conditions: Array.from(new Set(assertions.flatMap((assertion) => assertion.conditions))),
    assertionSummary: summary,
  };
}

function bucketRank(bucket) {
  return {
    pathogenic: 3,
    vus: 2,
    benign: 1,
    other: 0,
  }[bucket] ?? 0;
}

function summarizeConditions(rcvEntries) {
  const names = new Set();
  rcvEntries.forEach((entry) => {
    const condition = entry?.conditions?.name;
    if (condition) {
      names.add(condition);
    }
  });

  return Array.from(names);
}

function findNearbyCysteines(protein, residue, windowSize = 3) {
  return protein.cysteines
    .map((cysteine) => ({ ...cysteine, distance: Math.abs(cysteine.resi - residue) }))
    .filter((cysteine) => cysteine.distance > 0 && cysteine.distance <= windowSize)
    .sort((left, right) => left.distance - right.distance);
}

function findNearestCysteine(protein, residue) {
  return protein.cysteines
    .map((cysteine) => ({ ...cysteine, distance: Math.abs(cysteine.resi - residue) }))
    .sort((left, right) => left.distance - right.distance)[0] || null;
}

function buildVariantEntry(hit, proteinHgvs, parsedHgvs, rcvEntries, protein) {
  const assertions = collectClinicalAssertions(rcvEntries);
  const clinicalSummary = summarizeClinicalAssertions(assertions);
  const codingHgvs = normalizeToArray(hit.clinvar?.hgvs?.coding)[0] || "";
  const residue = parsedHgvs.residue;
  const cysteineAtPosition = protein.cysteines.find((entry) => entry.resi === residue) || null;
  const nearbyCysteines = findNearbyCysteines(protein, residue);

  return {
    id: `${hit.clinvar?.variant_id || hit._id}:${proteinHgvs}`,
    variantId: hit.clinvar?.variant_id || hit._id,
    residue,
    domain: getDomainForResidue(protein, residue)?.key || "Unknown",
    significance: clinicalSummary.significance,
    significanceBucket: clinicalSummary.significanceBucket,
    proteinHgvs,
    codingHgvs,
    type: hit.clinvar?.type || "Variant",
    accession: clinicalSummary.accession,
    preferredName: clinicalSummary.preferredName || proteinHgvs,
    conditions: clinicalSummary.conditions,
    origin: clinicalSummary.origin,
    assertionSummary: clinicalSummary.assertionSummary,
    createsCysteine: parsedHgvs.altAa === "Cys" && parsedHgvs.refAa !== "Cys",
    destroysCysteine: parsedHgvs.refAa === "Cys" && parsedHgvs.altAa !== "Cys",
    cysteineAtPosition,
    nearbyCysteines,
  };
}

function groupVariantsByResidue(protein, parsedVariants) {
  const groups = {};

  parsedVariants.forEach((variant) => {
    const key = String(variant.residue);
    if (!groups[key]) {
      groups[key] = {
        residue: variant.residue,
        domain: variant.domain,
        count: 0,
        significanceBucket: "other",
        variants: [],
        cysteineAtPosition: variant.cysteineAtPosition,
        nearbyCysteines: [],
        nearestCysteine: findNearestCysteine(protein, variant.residue),
        createsCysteine: false,
        destroysCysteine: false,
      };
    }

    const group = groups[key];
    group.variants.push(variant);
    group.count += 1;
    group.createsCysteine = group.createsCysteine || variant.createsCysteine;
    group.destroysCysteine = group.destroysCysteine || variant.destroysCysteine;

    if (bucketRank(variant.significanceBucket) > bucketRank(group.significanceBucket)) {
      group.significanceBucket = variant.significanceBucket;
    }

    const nearbyMap = new Map(group.nearbyCysteines.map((entry) => [entry.resi, entry]));
    variant.nearbyCysteines.forEach((entry) => {
      nearbyMap.set(entry.resi, entry);
    });
    group.nearbyCysteines = Array.from(nearbyMap.values()).sort((left, right) => left.distance - right.distance);
  });

  return Object.values(groups)
    .sort((left, right) => left.residue - right.residue)
    .map((group) => ({
      ...group,
      color: CYSVIS_VARIANT_COLORS[group.significanceBucket],
      significanceLabel: CYSVIS_VARIANT_LABELS[group.significanceBucket],
    }));
}

function chooseDisplayedVariantGroups(protein, groups) {
  if (protein.isCurated || groups.length <= 200) {
    return {
      displayedGroups: groups,
      displayPolicy: "all",
      displayPolicyNote: "",
    };
  }

  const pathogenicGroups = groups.filter((group) => group.significanceBucket === "pathogenic");
  if (pathogenicGroups.length) {
    return {
      displayedGroups: pathogenicGroups,
      displayPolicy: "pathogenic_only",
      displayPolicyNote:
        "Showing pathogenic / likely pathogenic variant residues only in the 3D viewer to keep large proteins responsive.",
    };
  }

  return {
    displayedGroups: groups.slice(0, 200),
    displayPolicy: "capped",
    displayPolicyNote:
      "Showing a capped subset of variant residues in the 3D viewer to keep large proteins responsive.",
  };
}

async function fetchClinvarVariants(protein) {
  if (!protein.geneSymbol) {
    return {
      loading: false,
      error: "No gene symbol available for ClinVar lookup.",
      rawVariantCount: 0,
      groupedResidueCount: 0,
      groups: [],
      groupsByResidue: {},
    };
  }

  const params = new URLSearchParams({
    q: `clinvar.gene.symbol:${protein.geneSymbol}`,
    fields: "clinvar",
    size: "500",
  });

  const response = await fetch(`https://myvariant.info/v1/query?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`MyVariant returned ${response.status}`);
  }

  const payload = await response.json();
  const seen = new Set();
  const parsedVariants = [];

  (payload.hits || []).forEach((hit) => {
    const rcvEntries = normalizeToArray(hit.clinvar?.rcv);
    const proteinHgvsEntries = normalizeToArray(hit.clinvar?.hgvs?.protein);
    const fallbackProteinHgvsEntries = rcvEntries
      .map((entry) => extractProteinHgvsFromText(entry?.preferred_name || ""))
      .filter(Boolean);
    const resolvedProteinHgvsEntries = proteinHgvsEntries.length
      ? proteinHgvsEntries
      : fallbackProteinHgvsEntries;

    resolvedProteinHgvsEntries.forEach((proteinHgvs) => {
      const parsedHgvs = parseHGVSProtein(proteinHgvs);
      if (!parsedHgvs) {
        return;
      }

      const dedupeKey = `${hit.clinvar?.variant_id || hit._id}:${parsedHgvs.proteinHgvs}`;
      if (seen.has(dedupeKey)) {
        return;
      }

      seen.add(dedupeKey);
      parsedVariants.push(buildVariantEntry(hit, parsedHgvs.proteinHgvs, parsedHgvs, rcvEntries, protein));
    });
  });

  const groups = groupVariantsByResidue(protein, parsedVariants);
  const groupsByResidue = groups.reduce((accumulator, group) => {
    accumulator[group.residue] = group;
    return accumulator;
  }, {});
  const displayChoice = chooseDisplayedVariantGroups(protein, groups);

  return {
    loading: false,
    error: "",
    rawVariantCount: parsedVariants.length,
    groupedResidueCount: groups.length,
    groups,
    groupsByResidue,
    displayedGroups: displayChoice.displayedGroups,
    displayedResidueCount: displayChoice.displayedGroups.length,
    displayPolicy: displayChoice.displayPolicy,
    displayPolicyNote: displayChoice.displayPolicyNote,
  };
}
