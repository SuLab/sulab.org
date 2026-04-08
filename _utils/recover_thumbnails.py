#!/usr/bin/env python3
"""
recover_thumbnails.py

Recovers publication thumbnail images for the Su Lab website using multiple
strategies:

  Phase 1: Re-download bioRxiv/medRxiv images from old URLs in git history
  Phase 2: Scrape PMC article pages for new CDN figure image URLs
  Phase 3: Convert PMIDs/DOIs to PMCIDs via NCBI API, then scrape PMC
  Phase 4: Scrape bioRxiv pages for DOI-only preprints

Usage:
    python _utils/recover_thumbnails.py [--dry-run] [--phase {1,2,3,4,all}]
                                        [--delay SECONDS] [--old-commit REF]

Dependencies: requests, pyyaml (standard library: subprocess, re, os, etc.)
"""

import argparse
import os
import re
import subprocess
import sys
import time
from urllib.parse import urlparse

import requests
import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SOURCES = os.path.join(REPO_ROOT, "_data", "sources.yaml")
DEFAULT_CITATIONS = os.path.join(REPO_ROOT, "_data", "citations.yaml")
DEFAULT_THUMBDIR = os.path.join(REPO_ROOT, "thumbnail")
DEFAULT_OLD_COMMIT = "c8b8df7~1"

USER_AGENT = "SuLab-thumbnail-recovery/1.0 (https://sulab.org; asu@scripps.edu)"
HEADERS = {"User-Agent": USER_AGENT}

# ---------------------------------------------------------------------------
# Utility functions (adapted from download_images.py)
# ---------------------------------------------------------------------------


def fallback_slug(rec):
    """Generate AuthorLastName_Year slug from a citation record."""
    authors = rec.get("authors", [])
    last = ""
    if authors and isinstance(authors, list) and len(authors) > 0:
        first = authors[0]
        if isinstance(first, str) and first.strip():
            name = first.strip()
            if "," in name:
                last = name.split(",")[0].strip()
            else:
                last = name.split()[-1]
            last = re.sub(r"[^A-Za-z0-9\-]+", "", last)

    year = ""
    date_val = rec.get("date", "")
    if isinstance(date_val, str) and date_val:
        if len(date_val) >= 4 and date_val[:4].isdigit():
            year = date_val[:4]
        else:
            m = re.search(r"\b(19|20)\d{2}\b", date_val)
            if m:
                year = m.group(0)

    if last and year:
        return f"{last}_{year}"
    return ""


def slug_for_record(rec):
    """Use explicit slug if present, else derive from authors+year."""
    slug = rec.get("slug", "")
    if isinstance(slug, str) and slug.strip():
        return slug.strip()
    return fallback_slug(rec)


# Track used slugs to handle duplicates (e.g., Tsueng_2016, Tsueng_2016b)
_used_slugs = set()


def unique_slug(base_slug):
    """Return a unique slug, appending b, c, d... if the base is already used."""
    if base_slug not in _used_slugs:
        _used_slugs.add(base_slug)
        return base_slug
    # Try suffixes b, c, d, ...
    for suffix in "bcdefghijklmnopqrstuvwxyz":
        candidate = f"{base_slug}{suffix}"
        if candidate not in _used_slugs:
            _used_slugs.add(candidate)
            return candidate
    return f"{base_slug}_dup"


def init_used_slugs(thumbdir):
    """Pre-populate used slugs from existing thumbnail files."""
    if os.path.isdir(thumbdir):
        for fname in os.listdir(thumbdir):
            name, _ = os.path.splitext(fname)
            _used_slugs.add(name)


def ext_from_content_type(ct):
    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/svg+xml": ".svg",
    }
    if not ct:
        return None
    ct_lower = ct.lower().split(";")[0].strip()
    return mapping.get(ct_lower)


def guess_ext(url, content_type=None):
    """Guess file extension from URL path or Content-Type."""
    path = urlparse(url).path
    _, ext = os.path.splitext(path)
    if ext and len(ext) <= 6:
        return ext
    if content_type:
        e = ext_from_content_type(content_type)
        if e:
            return e
    return ".jpg"


def download_image(url, dest_path, timeout=15.0):
    """Download an image URL to dest_path. Returns True on success."""
    try:
        r = requests.get(url, stream=True, timeout=timeout, headers=HEADERS)
        if r.status_code != 200:
            return False
        ct = r.headers.get("Content-Type", "").lower()
        if not ct.startswith("image/"):
            # Check URL extension as fallback
            ext = os.path.splitext(urlparse(url).path)[1].lower()
            if ext not in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}:
                return False
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        # Verify file is non-empty
        if os.path.getsize(dest_path) < 100:
            os.remove(dest_path)
            return False
        return True
    except Exception as e:
        sys.stderr.write(f"  Download error: {e}\n")
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return False


# ---------------------------------------------------------------------------
# YAML update helpers
#
# Instead of round-trip YAML parsing (which needs ruamel.yaml), we do
# targeted line-based replacements on the raw file text.  This preserves
# comments, formatting, and key order.
# ---------------------------------------------------------------------------


def update_yaml_image_field(filepath, citation_id, new_image_value):
    """
    In a YAML list-of-dicts file, find the record whose 'id' matches
    `citation_id` and replace or insert its `image:` value.
    Uses line-based editing to preserve formatting.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    in_target_record = False
    id_line_idx = None
    id_indent = None
    modified = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Detect start of a new list item (- id: ...) or (  id: ...)
        if stripped.startswith("- id:") or (stripped.startswith("id:") and "- " in line):
            # If we were in the target record but never found image:, insert it
            if in_target_record and not modified:
                # Insert image field right after the id line
                indent = "  " + " " * id_indent  # 2 extra spaces for field indent
                insert_line = f"{indent}image: {new_image_value}\n" if new_image_value else f"{indent}image: ''\n"
                lines.insert(id_line_idx + 1, insert_line)
                modified = True
                break  # lines shifted, stop processing

            id_match = re.search(r"id:\s*(.+)", stripped)
            if id_match:
                found_id = id_match.group(1).strip().strip("'\"")
                if found_id == citation_id:
                    in_target_record = True
                    id_line_idx = i
                    id_indent = len(line) - len(line.lstrip())
                    continue
                else:
                    in_target_record = False
                    continue

        if in_target_record:
            # Detect if we've left the record (new list item at same or lower indent)
            if stripped.startswith("- ") and not stripped.startswith("- type:"):
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= id_indent:
                    # Left record without finding image: field — insert it
                    indent = "  " + " " * id_indent
                    insert_line = f"{indent}image: {new_image_value}\n" if new_image_value else f"{indent}image: ''\n"
                    lines.insert(i, insert_line)
                    modified = True
                    in_target_record = False
                    break

            # Find the image field
            if stripped.startswith("image:"):
                leading = line[: len(line) - len(line.lstrip())]
                if new_image_value:
                    lines[i] = f"{leading}image: {new_image_value}\n"
                else:
                    lines[i] = f"{leading}image: ''\n"
                modified = True
                in_target_record = False

    # Handle case where target record is the last in the file
    if in_target_record and not modified:
        indent = "  " + " " * id_indent
        insert_line = f"{indent}image: {new_image_value}\n" if new_image_value else f"{indent}image: ''\n"
        lines.append(insert_line)
        modified = True

    if modified:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(lines)

    return modified


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_citations(path):
    """Load citations.yaml, return list of dicts."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or []


def load_old_citations(commit_ref, citations_relpath="_data/citations.yaml"):
    """Load citations.yaml from a git commit."""
    result = subprocess.run(
        ["git", "show", f"{commit_ref}:{citations_relpath}"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    if result.returncode != 0:
        sys.stderr.write(f"Error reading old commit: {result.stderr}\n")
        return []
    return yaml.safe_load(result.stdout) or []


def get_missing_image_records(citations):
    """Return records that have no image (empty string or missing)."""
    return [c for c in citations if not c.get("image")]


# ---------------------------------------------------------------------------
# Phase 1: bioRxiv/medRxiv re-download from git history
# ---------------------------------------------------------------------------


def phase1_biorxiv_redownload(citations, old_commit, thumbdir, delay, dry_run):
    """Re-download bioRxiv/medRxiv images using URLs from git history."""
    print("\n=== Phase 1: bioRxiv/medRxiv re-download ===")

    old_citations = load_old_citations(old_commit)
    old_urls = {}
    for c in old_citations:
        img = c.get("image", "")
        if img and isinstance(img, str) and img.startswith("http"):
            if "rxiv.org" in img:
                old_urls[c.get("id", "")] = img

    missing = get_missing_image_records(citations)
    targets = []
    for c in missing:
        cid = c.get("id", "")
        if cid in old_urls:
            targets.append((c, old_urls[cid]))

    print(f"  Found {len(targets)} bioRxiv/medRxiv entries to recover")

    results = []
    for rec, url in targets:
        cid = rec.get("id", "")
        base_slug = slug_for_record(rec)
        if not base_slug:
            print(f"  SKIP (no slug): {cid}")
            continue

        slug = unique_slug(base_slug)
        ext = guess_ext(url)
        filename = f"{slug}{ext}"
        dest = os.path.join(thumbdir, filename)
        rel_path = f"thumbnail/{filename}"

        if dry_run:
            print(f"  [DRY-RUN] Would download {url[:70]}... -> {rel_path}")
            results.append((cid, rel_path))
            continue

        print(f"  Downloading: {cid} -> {rel_path}")
        if download_image(url, dest):
            results.append((cid, rel_path))
            print(f"    OK ({os.path.getsize(dest)} bytes)")
        else:
            print(f"    FAILED")

        time.sleep(delay)

    return results


# ---------------------------------------------------------------------------
# Phase 2: PMC page scraping
# ---------------------------------------------------------------------------


def scrape_pmc_first_figure(pmcid, timeout=15.0):
    """
    Fetch a PMC article page and extract the first figure image URL
    from the new CDN (cdn.ncbi.nlm.nih.gov/pmc/blobs/...).
    """
    url = f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS)
        if r.status_code != 200:
            return None
        # Find CDN image URLs
        matches = re.findall(
            r"https://cdn\.ncbi\.nlm\.nih\.gov/pmc/blobs/[^\s\"'<>]+", r.text
        )
        if matches:
            return matches[0]
        return None
    except Exception as e:
        sys.stderr.write(f"  PMC scrape error for {pmcid}: {e}\n")
        return None


def phase2_pmc_scraping(citations, thumbdir, delay, dry_run):
    """Scrape PMC article pages for figure images."""
    print("\n=== Phase 2: PMC page scraping ===")

    missing = get_missing_image_records(citations)
    targets = [(c, c.get("pmcid", "")) for c in missing if c.get("pmcid")]

    print(f"  Found {len(targets)} entries with PMCIDs to scrape")

    results = []
    for i, (rec, pmcid) in enumerate(targets):
        cid = rec.get("id", "")
        base_slug = slug_for_record(rec)
        if not base_slug:
            print(f"  SKIP (no slug): {cid}")
            continue

        if dry_run:
            slug = unique_slug(base_slug)
            print(f"  [DRY-RUN] Would scrape {pmcid} for {cid}")
            results.append((cid, f"thumbnail/{slug}.jpg"))
            continue

        print(f"  [{i+1}/{len(targets)}] Scraping {pmcid} for {cid}...")
        fig_url = scrape_pmc_first_figure(pmcid)
        if not fig_url:
            print(f"    No figure found")
            time.sleep(delay)
            continue

        # Determine extension
        ct = None
        try:
            head = requests.head(fig_url, timeout=10, headers=HEADERS)
            ct = head.headers.get("Content-Type", "")
        except Exception:
            pass
        slug = unique_slug(base_slug)
        ext = guess_ext(fig_url, ct)
        filename = f"{slug}{ext}"
        dest = os.path.join(thumbdir, filename)
        rel_path = f"thumbnail/{filename}"

        if download_image(fig_url, dest):
            results.append((cid, rel_path))
            print(f"    OK: {rel_path} ({os.path.getsize(dest)} bytes)")
        else:
            print(f"    Download FAILED")

        time.sleep(delay)

    return results


# ---------------------------------------------------------------------------
# Phase 3: PMID/DOI -> PMCID conversion, then scrape
# ---------------------------------------------------------------------------


def convert_ids_to_pmcids(ids):
    """
    Use NCBI ID Converter API to batch-convert PMIDs or DOIs to PMCIDs.
    Returns dict: {input_id: pmcid}.
    """
    if not ids:
        return {}

    # NCBI API requires PMIDs and DOIs in separate requests
    pmids = [i for i in ids if i.isdigit()]
    dois = [i for i in ids if not i.isdigit()]

    result = {}
    for batch in [pmids, dois]:
        if not batch:
            continue
        id_str = ",".join(batch)
        url = (
            f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
            f"?ids={id_str}&format=json&tool=sulab-thumbs&email=asu@scripps.edu"
        )
        try:
            r = requests.get(url, timeout=30, headers=HEADERS)
            if r.status_code != 200:
                sys.stderr.write(f"  ID converter returned {r.status_code} for batch\n")
                continue
            data = r.json()
            for rec in data.get("records", []):
                pmcid = rec.get("pmcid", "")
                if pmcid:
                    if rec.get("pmid"):
                        result[str(rec["pmid"])] = pmcid
                    if rec.get("doi"):
                        result[str(rec["doi"])] = pmcid
        except Exception as e:
            sys.stderr.write(f"  ID conversion error: {e}\n")

    return result


def phase3_id_conversion(citations, thumbdir, delay, dry_run):
    """Convert PMIDs/DOIs to PMCIDs and scrape PMC for figures."""
    print("\n=== Phase 3: PMID/DOI -> PMCID conversion ===")

    missing = get_missing_image_records(citations)
    # Find entries without PMCID but with PMID or DOI
    targets = []
    lookup_ids = []
    for c in missing:
        if c.get("pmcid"):
            continue  # Already handled in Phase 2
        pmid = c.get("pmid", "")
        doi = ""
        cid = c.get("id", "")
        if cid.startswith("doi:"):
            doi = cid[4:]
        if pmid:
            lookup_ids.append(str(pmid))
            targets.append((c, str(pmid), "pmid"))
        elif doi:
            lookup_ids.append(doi)
            targets.append((c, doi, "doi"))

    print(f"  Found {len(targets)} entries to look up PMCIDs for")

    if not targets:
        return []

    # Batch convert
    if dry_run:
        print(f"  [DRY-RUN] Would query NCBI ID converter for {len(lookup_ids)} IDs")
        return []

    print(f"  Querying NCBI ID converter for {len(lookup_ids)} IDs...")
    pmcid_map = convert_ids_to_pmcids(lookup_ids)
    print(f"  Found {len(pmcid_map)} PMCIDs")

    results = []
    for rec, lookup_id, id_type in targets:
        pmcid = pmcid_map.get(lookup_id)
        if not pmcid:
            continue

        cid = rec.get("id", "")
        base_slug = slug_for_record(rec)
        if not base_slug:
            print(f"  SKIP (no slug): {cid}")
            continue

        print(f"  Scraping {pmcid} for {cid}...")
        fig_url = scrape_pmc_first_figure(pmcid)
        if not fig_url:
            print(f"    No figure found")
            time.sleep(delay)
            continue

        ct = None
        try:
            head = requests.head(fig_url, timeout=10, headers=HEADERS)
            ct = head.headers.get("Content-Type", "")
        except Exception:
            pass
        slug = unique_slug(base_slug)
        ext = guess_ext(fig_url, ct)
        filename = f"{slug}{ext}"
        dest = os.path.join(thumbdir, filename)
        rel_path = f"thumbnail/{filename}"

        if download_image(fig_url, dest):
            results.append((cid, rel_path))
            print(f"    OK: {rel_path} ({os.path.getsize(dest)} bytes)")
        else:
            print(f"    Download FAILED")

        time.sleep(delay)

    return results


# ---------------------------------------------------------------------------
# Phase 4: bioRxiv DOI scraping
# ---------------------------------------------------------------------------


def scrape_biorxiv_first_figure(doi):
    """Fetch a bioRxiv page by DOI and extract the first figure image URL."""
    url = f"https://www.biorxiv.org/content/{doi}"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code != 200:
            return None
        # Look for figure images - bioRxiv uses specific patterns
        matches = re.findall(
            r"https://www\.biorxiv\.org/content/biorxiv/early/[^\s\"'<>]+\.(?:jpg|gif|png)",
            r.text,
        )
        if matches:
            return matches[0]
        return None
    except Exception as e:
        sys.stderr.write(f"  bioRxiv scrape error for {doi}: {e}\n")
        return None


def phase4_biorxiv_doi_scraping(citations, thumbdir, delay, dry_run):
    """Scrape bioRxiv pages for DOI-only preprints without images."""
    print("\n=== Phase 4: bioRxiv DOI scraping ===")

    missing = get_missing_image_records(citations)
    targets = []
    for c in missing:
        cid = c.get("id", "")
        if cid.startswith("doi:10.1101/") and not c.get("pmcid"):
            targets.append(c)

    print(f"  Found {len(targets)} bioRxiv DOI-only entries")

    results = []
    for rec in targets:
        cid = rec.get("id", "")
        doi = cid[4:]  # strip "doi:" prefix
        base_slug = slug_for_record(rec)
        if not base_slug:
            print(f"  SKIP (no slug): {cid}")
            continue

        if dry_run:
            slug = unique_slug(base_slug)
            print(f"  [DRY-RUN] Would scrape bioRxiv for {doi}")
            results.append((cid, f"thumbnail/{slug}.jpg"))
            continue

        print(f"  Scraping bioRxiv for {doi}...")
        fig_url = scrape_biorxiv_first_figure(doi)
        if not fig_url:
            print(f"    No figure found")
            time.sleep(delay)
            continue

        slug = unique_slug(base_slug)
        ext = guess_ext(fig_url)
        filename = f"{slug}{ext}"
        dest = os.path.join(thumbdir, filename)
        rel_path = f"thumbnail/{filename}"

        if download_image(fig_url, dest):
            results.append((cid, rel_path))
            print(f"    OK: {rel_path} ({os.path.getsize(dest)} bytes)")
        else:
            print(f"    Download FAILED")

        time.sleep(delay)

    return results


# ---------------------------------------------------------------------------
# YAML update orchestration
# ---------------------------------------------------------------------------


def apply_updates(results, sources_path, citations_path, dry_run):
    """Update both sources.yaml and citations.yaml with recovered image paths."""
    if not results:
        print("\n  No updates to apply.")
        return

    print(f"\n=== Updating YAML files ({len(results)} entries) ===")

    for cid, rel_path in results:
        if dry_run:
            print(f"  [DRY-RUN] {cid} -> {rel_path}")
            continue

        # Update sources.yaml
        updated_src = update_yaml_image_field(sources_path, cid, rel_path)
        # Update citations.yaml
        updated_cit = update_yaml_image_field(citations_path, cid, rel_path)

        if updated_src or updated_cit:
            print(f"  Updated: {cid} -> {rel_path}")
        else:
            print(f"  WARNING: Could not find {cid} in YAML files")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description="Recover publication thumbnails")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without downloading",
    )
    ap.add_argument(
        "--phase",
        choices=["1", "2", "3", "4", "all"],
        default="all",
        help="Run specific phase or all (default: all)",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between HTTP requests in seconds (default: 1.0)",
    )
    ap.add_argument(
        "--old-commit",
        default=DEFAULT_OLD_COMMIT,
        help=f"Git commit ref for old citations (default: {DEFAULT_OLD_COMMIT})",
    )
    ap.add_argument(
        "--sources-yaml",
        default=DEFAULT_SOURCES,
        help="Path to sources.yaml",
    )
    ap.add_argument(
        "--citations-yaml",
        default=DEFAULT_CITATIONS,
        help="Path to citations.yaml",
    )
    ap.add_argument(
        "--thumbdir",
        default=DEFAULT_THUMBDIR,
        help="Thumbnail output directory",
    )
    args = ap.parse_args()

    os.makedirs(args.thumbdir, exist_ok=True)
    init_used_slugs(args.thumbdir)

    citations = load_citations(args.citations_yaml)
    missing_count = len(get_missing_image_records(citations))
    total = len(citations)
    print(f"Loaded {total} citations, {missing_count} missing thumbnails")

    all_results = []
    phase = args.phase

    if phase in ("1", "all"):
        results = phase1_biorxiv_redownload(
            citations, args.old_commit, args.thumbdir, args.delay * 0.5, args.dry_run
        )
        all_results.extend(results)
        # Update in-memory citations so later phases see the updates
        recovered_ids = {cid for cid, _ in results}
        for c in citations:
            if c.get("id", "") in recovered_ids:
                c["image"] = "recovered"  # Mark as handled

    if phase in ("2", "all"):
        results = phase2_pmc_scraping(
            citations, args.thumbdir, args.delay, args.dry_run
        )
        all_results.extend(results)
        recovered_ids = {cid for cid, _ in results}
        for c in citations:
            if c.get("id", "") in recovered_ids:
                c["image"] = "recovered"

    if phase in ("3", "all"):
        results = phase3_id_conversion(
            citations, args.thumbdir, args.delay, args.dry_run
        )
        all_results.extend(results)
        recovered_ids = {cid for cid, _ in results}
        for c in citations:
            if c.get("id", "") in recovered_ids:
                c["image"] = "recovered"

    if phase in ("4", "all"):
        results = phase4_biorxiv_doi_scraping(
            citations, args.thumbdir, args.delay, args.dry_run
        )
        all_results.extend(results)

    # Apply all YAML updates
    apply_updates(all_results, args.sources_yaml, args.citations_yaml, args.dry_run)

    # Summary
    print(f"\n=== Summary ===")
    print(f"  Total citations: {total}")
    print(f"  Previously missing: {missing_count}")
    print(f"  Recovered this run: {len(all_results)}")
    print(f"  Still missing: {missing_count - len(all_results)}")

    # List remaining missing entries
    if not args.dry_run:
        citations = load_citations(args.citations_yaml)
        still_missing = get_missing_image_records(citations)
        if still_missing:
            print(f"\n  Entries still without thumbnails:")
            for c in still_missing:
                print(f"    {c.get('id', '?')}  — {c.get('title', '')[:60]}")


if __name__ == "__main__":
    main()
