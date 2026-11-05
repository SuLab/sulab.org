const CYSVIS_HOTSPOT_CONFIG = Object.freeze({
  sigmaAngstrom: 6,
  maxDistanceAngstrom: 12,
  nearDistanceAngstrom: 8,
  permutationCount: 750,
  batchSize: 25,
});

function parsePdbResidueCoordinates(pdbText) {
  const coordinates = {};
  const lines = pdbText.split(/\r?\n/);

  lines.forEach((line) => {
    if (!line.startsWith("ATOM")) {
      return;
    }

    const atomName = line.slice(12, 16).trim();
    const chainId = line.slice(21, 22).trim() || "A";
    if (chainId !== "A" || atomName !== "CA") {
      return;
    }

    const residueNumber = Number.parseInt(line.slice(22, 26).trim(), 10);
    if (Number.isNaN(residueNumber)) {
      return;
    }

    coordinates[residueNumber] = {
      x: Number.parseFloat(line.slice(30, 38).trim()),
      y: Number.parseFloat(line.slice(38, 46).trim()),
      z: Number.parseFloat(line.slice(46, 54).trim()),
    };
  });

  return coordinates;
}

function analyzeCysteineHotspots({ protein, variantGroups, residueCoordinates, config = CYSVIS_HOTSPOT_CONFIG }) {
  return analyzeCysteineHotspotsCore({
    protein,
    variantGroups,
    residueCoordinates,
    config,
    shouldYield: false,
  });
}

async function analyzeCysteineHotspotsAsync({
  protein,
  variantGroups,
  residueCoordinates,
  config = CYSVIS_HOTSPOT_CONFIG,
}) {
  return analyzeCysteineHotspotsCore({
    protein,
    variantGroups,
    residueCoordinates,
    config,
    shouldYield: true,
  });
}

async function analyzeCysteineHotspotsCore({
  protein,
  variantGroups,
  residueCoordinates,
  config,
  shouldYield,
}) {
  const pathogenicGroups = variantGroups.filter((group) => group.significanceBucket === "pathogenic");
  const eligibleResidues = Object.keys(residueCoordinates)
    .map((key) => Number.parseInt(key, 10))
    .filter((resi) => Number.isFinite(resi));

  if (!pathogenicGroups.length) {
    return {
      ready: true,
      error: "",
      config,
      byResidue: {},
      summary: {
        pathogenicResidueCount: 0,
        permutationCount: 0,
        eligibleResidueCount: eligibleResidues.length,
      },
    };
  }

  if (!eligibleResidues.length) {
    return {
      ready: false,
      error: "Residue coordinates are unavailable for hotspot analysis.",
      config,
      byResidue: {},
      summary: {
        pathogenicResidueCount: pathogenicGroups.length,
        permutationCount: 0,
        eligibleResidueCount: 0,
      },
    };
  }

  const domainCounts = pathogenicGroups.reduce((accumulator, group) => {
    accumulator[group.domain] = (accumulator[group.domain] || 0) + 1;
    return accumulator;
  }, {});

  const residueDomainMap = eligibleResidues.reduce((accumulator, residue) => {
    accumulator[residue] = getDomainForResidue(protein, residue)?.key || "Unknown";
    return accumulator;
  }, {});

  const actualScores = {};
  protein.cysteines.forEach((cysteine) => {
    const coordinate = residueCoordinates[cysteine.resi];
    if (!coordinate) {
      return;
    }

    actualScores[cysteine.resi] = scorePathogenicNeighborhood(
      coordinate,
      pathogenicGroups,
      residueCoordinates,
      config
    );
  });

  const cysteinePermutations = protein.cysteines.reduce((accumulator, cysteine) => {
    if (residueCoordinates[cysteine.resi]) {
      accumulator[cysteine.resi] = [];
    }
    return accumulator;
  }, {});

  const rng = createSeededRng(buildHotspotSeed(protein, pathogenicGroups, eligibleResidues.length, config));
  for (let index = 0; index < config.permutationCount; index += 1) {
    const permutedResidues = sampleResiduesByDomain(eligibleResidues, residueDomainMap, domainCounts, rng);
    if (!permutedResidues.length) {
      continue;
    }

    Object.keys(cysteinePermutations).forEach((resiKey) => {
      const residue = Number.parseInt(resiKey, 10);
      const coordinate = residueCoordinates[residue];
      const score = scoreResidueNeighborhood(coordinate, permutedResidues, residueCoordinates, config);
      cysteinePermutations[resiKey].push(score.weightedBurden);
    });

    if (shouldYield && (index + 1) % config.batchSize === 0) {
      await yieldToBrowser();
    }
  }

  const byResidue = {};
  Object.entries(actualScores).forEach(([resiKey, score]) => {
    const nullScores = cysteinePermutations[resiKey] || [];
    const mean = nullScores.length ? average(nullScores) : 0;
    const stdDev = nullScores.length ? standardDeviation(nullScores, mean) : 0;
    const nullAtOrAbove = nullScores.filter((value) => value >= score.weightedBurden).length;
    const nullAtOrBelow = nullScores.filter((value) => value <= score.weightedBurden).length;
    const percentile = nullScores.length ? (nullAtOrBelow / nullScores.length) * 100 : null;
    const empiricalPValue = nullScores.length ? (nullAtOrAbove + 1) / (nullScores.length + 1) : null;
    const zScore = stdDev > 0 ? (score.weightedBurden - mean) / stdDev : 0;

    byResidue[resiKey] = {
      ...score,
      nullMean: mean,
      nullStdDev: stdDev,
      percentile,
      empiricalPValue,
      zScore,
      permutationCount: nullScores.length,
    };
  });

  return {
    ready: true,
    error: "",
    config,
    byResidue,
    summary: {
      pathogenicResidueCount: pathogenicGroups.length,
      permutationCount: config.permutationCount,
      eligibleResidueCount: eligibleResidues.length,
    },
  };
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

function scorePathogenicNeighborhood(origin, pathogenicGroups, residueCoordinates, config) {
  const measurements = pathogenicGroups
    .map((group) => {
      const coordinate = residueCoordinates[group.residue];
      if (!coordinate) {
        return null;
      }

      return {
        residue: group.residue,
        significanceLabel: group.significanceLabel,
        distance: calculateDistance(origin, coordinate),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.distance - right.distance);

  return finalizeNeighborhoodScore(measurements, config);
}

function scoreResidueNeighborhood(origin, residues, residueCoordinates, config) {
  const measurements = residues
    .map((residue) => {
      const coordinate = residueCoordinates[residue];
      if (!coordinate) {
        return null;
      }

      return {
        residue,
        distance: calculateDistance(origin, coordinate),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.distance - right.distance);

  return finalizeNeighborhoodScore(measurements, config);
}

function finalizeNeighborhoodScore(measurements, config) {
  const nearbyResidues = measurements
    .filter((entry) => entry.distance <= config.maxDistanceAngstrom)
    .map((entry) => ({
      residue: entry.residue,
      significanceLabel: entry.significanceLabel || "Pathogenic / likely pathogenic",
      distance: roundTo(entry.distance, 1),
    }));

  const withinNearDistance = measurements.filter((entry) => entry.distance <= config.nearDistanceAngstrom).length;
  const nearestDistance = measurements.length ? roundTo(measurements[0].distance, 1) : null;
  const weightedBurden = measurements.reduce((sum, entry) => {
    if (entry.distance > config.maxDistanceAngstrom) {
      return sum;
    }

    const kernel = Math.exp(-((entry.distance * entry.distance) / (2 * config.sigmaAngstrom * config.sigmaAngstrom)));
    return sum + kernel;
  }, 0);

  return {
    weightedBurden: roundTo(weightedBurden, 3),
    nearestPathogenicDistance: nearestDistance,
    pathogenicWithinNearDistance: withinNearDistance,
    nearbyResidues,
  };
}

function sampleResiduesByDomain(eligibleResidues, residueDomainMap, domainCounts, rng) {
  const selected = [];

  Object.entries(domainCounts).forEach(([domainKey, count]) => {
    const pool = eligibleResidues.filter((residue) => residueDomainMap[residue] === domainKey);
    const sampleCount = Math.min(count, pool.length);
    if (!sampleCount) {
      return;
    }

    const shuffled = pool.slice();
    for (let index = shuffled.length - 1; index > 0; index -= 1) {
      const swapIndex = Math.floor(rng() * (index + 1));
      const temporary = shuffled[index];
      shuffled[index] = shuffled[swapIndex];
      shuffled[swapIndex] = temporary;
    }

    selected.push(...shuffled.slice(0, sampleCount));
  });

  return selected;
}

function calculateDistance(first, second) {
  const dx = first.x - second.x;
  const dy = first.y - second.y;
  const dz = first.z - second.z;
  return Math.sqrt(dx * dx + dy * dy + dz * dz);
}

function average(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function standardDeviation(values, mean) {
  const variance =
    values.reduce((sum, value) => sum + (value - mean) * (value - mean), 0) / values.length;
  return Math.sqrt(variance);
}

function roundTo(value, digits) {
  return Number.parseFloat(value.toFixed(digits));
}

function buildHotspotSeed(protein, pathogenicGroups, eligibleResidueCount, config) {
  return [
    protein.uniprotId,
    eligibleResidueCount,
    config.permutationCount,
    config.maxDistanceAngstrom,
    config.sigmaAngstrom,
    pathogenicGroups.map((group) => `${group.residue}:${group.domain}`).join("|"),
  ].join("::");
}

function createSeededRng(seedText) {
  let hash = 1779033703 ^ seedText.length;
  for (let index = 0; index < seedText.length; index += 1) {
    hash = Math.imul(hash ^ seedText.charCodeAt(index), 3432918353);
    hash = (hash << 13) | (hash >>> 19);
  }

  return function seededRandom() {
    hash = Math.imul(hash ^ (hash >>> 16), 2246822507);
    hash = Math.imul(hash ^ (hash >>> 13), 3266489909);
    const result = (hash ^= hash >>> 16) >>> 0;
    return result / 4294967296;
  };
}
