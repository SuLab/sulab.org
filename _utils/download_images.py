#!/usr/bin/env python3
"""
process_images.py  (round-trip YAML version)

Usage:
    python process_images.py INPUT.yaml OUTPUT.yaml [--thumbdir THUMBDIR] [--timeout SECONDS]

Purpose:
- Read YAML with FULL round-trip preservation (comments, formatting, key order, quotes).
- Validate/download 'image' URLs to a thumbnail directory.
- Update ONLY the 'image' field in-place.
- Write a revised YAML that is as close to the original as possible for clean diffs.

Notes:
- Requires: ruamel.yaml  (pip install ruamel.yaml)
- Also uses: requests  (pip install requests) for HTTP downloads.
- Duplicate slug check: warns to STDERR if the same slug appears more than once; continues.
- This script NEVER modifies 'slug' or other fields besides 'image'.
"""

import sys
import os
import re
import argparse
from urllib.parse import urlparse
from typing import Any, Dict, Iterable, Optional, Tuple

try:
    from ruamel.yaml import YAML
    from ruamel.yaml.comments import CommentedMap, CommentedSeq
except Exception as e:
    sys.stderr.write("Error: ruamel.yaml is required. Install with: pip install ruamel.yaml\n")
    raise

try:
    import requests
except Exception:
    requests = None

def _iter_record_refs(data: Any):
    """
    Yield (parent_container, key_or_index, record_dict) for each record IN ORIGINAL ORDER,
    so we can mutate in place and preserve order/comments on dump.
    Supports:
      - top-level list of records
      - top-level dict with a list of records under one of the common keys
      - top-level dict-of-dicts, treating each value as a record
    """
    # 1) list at top level
    if isinstance(data, CommentedSeq):
        for i in range(len(data)):
            rec = data[i]
            if isinstance(rec, CommentedMap):
                yield (data, i, rec)
        return

    # 2) dict at top level with a known list key
    if isinstance(data, CommentedMap):
        for k in ("items", "records", "citations", "entries", "data"):
            if k in data and isinstance(data[k], CommentedSeq):
                lst = data[k]
                for i in range(len(lst)):
                    rec = lst[i]
                    if isinstance(rec, CommentedMap):
                        yield (lst, i, rec)
                return

        # 3) dict-of-dicts
        for k in data.keys():
            v = data[k]
            if isinstance(v, CommentedMap):
                yield (data, k, v)
        return

    # No recognized structure; do nothing.

def _records_from_data(data: Any):
    for _, __, rec in _iter_record_refs(data):
        yield rec

def _pdf_basename_without_ext(rec: CommentedMap) -> str:
    btns = rec.get("buttons")
    if isinstance(btns, CommentedSeq):
        for btn in btns:
            if isinstance(btn, CommentedMap) and isinstance(btn.get("type"), str) and btn["type"].lower() == "pdf":
                link = btn.get("link")
                if isinstance(link, str) and link.strip():
                    base = os.path.basename(link.strip())
                    return ".".join(base.split(".")[:-1]) if "." in base else base
    return ""

def _fallback_slug(rec: CommentedMap) -> str:
    """first_author_lastname + '_' + YYYY (from 'date'), or '' if unavailable"""
    authors = rec.get("authors")
    last = ""
    if isinstance(authors, CommentedSeq) and len(authors) > 0:
        first = authors[0]
        if isinstance(first, str) and first.strip():
            name = first.strip()
            if "," in name:
                last = name.split(",")[0].strip()
            else:
                last = name.split()[-1]
            last = re.sub(r"[^A-Za-z0-9\-]+", "", last)

    year = ""
    date_val = rec.get("date")
    if isinstance(date_val, str):
        if len(date_val) >= 4 and date_val[:4].isdigit():
            year = date_val[:4]
        else:
            m = re.search(r"\b(19|20)\d{2}\b", date_val)
            if m:
                year = m.group(0)

    if last and year:
        return f"{last}_{year}"
    return ""

def _slug_for_check(rec: CommentedMap) -> str:
    """Use explicit slug if present; else derive (pdf basename, then firstauthor_year)."""
    slug = rec.get("slug")
    if isinstance(slug, str) and slug.strip():
        return slug.strip()
    slug = _pdf_basename_without_ext(rec)
    if slug:
        return slug
    return _fallback_slug(rec)

def _check_duplicate_slugs(data: Any) -> None:
    counts = {}
    idxs = {}
    for idx, (_, __, rec) in enumerate(_iter_record_refs(data)):
        s = _slug_for_check(rec)
        if not s:
            s = ""
        counts[s] = counts.get(s, 0) + 1
        idxs.setdefault(s, []).append(idx)

    conflicts = {s: idxs[s] for s, c in counts.items() if s and c > 1}
    if conflicts:
        sys.stderr.write("Warning: duplicate slug(s) detected in input:\n")
        for s, positions in conflicts.items():
            sys.stderr.write(f"  slug '{s}' occurs {len(positions)} times (records at indexes: {positions})\n")
        # Warning only; do not exit.

def _is_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False

def _ext_from_content_type(ct: str) -> Optional[str]:
    if not ct:
        return None
    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/svg+xml": ".svg",
        "image/tiff": ".tif",
        "image/bmp": ".bmp",
        "image/x-icon": ".ico",
        "image/heic": ".heic",
        "image/heif": ".heif",
    }
    ct_lower = ct.lower().split(";")[0].strip()
    return mapping.get(ct_lower)

def _safe_ext_from_url_or_ct(url: str, content_type: Optional[str]) -> str:
    path = urlparse(url).path
    _, ext = os.path.splitext(path)
    if ext and len(ext) <= 6:
        return ext
    if content_type:
        e = _ext_from_content_type(content_type)
        if e:
            return e
    return ".jpg"

def _download_image_if_valid(url: str, dest_fullpath: str, timeout: float = 15.0) -> bool:
    if requests is None:
        return False
    try:
        # Try HEAD (ignore failures)
        try:
            requests.head(url, allow_redirects=True, timeout=timeout)
        except Exception:
            pass
        r = requests.get(url, stream=True, timeout=timeout)
        if r.status_code != 200:
            return False
        ct = r.headers.get("Content-Type", "").lower()
        if not ct.startswith("image/"):
            ext = os.path.splitext(urlparse(url).path)[1].lower()
            if ext not in {".jpg",".jpeg",".png",".gif",".webp",".svg",".tif",".tiff",".bmp",".ico",".heic",".heif"}:
                return False
        with open(dest_fullpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except Exception:
        return False

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input_yaml", help="Path to input YAML")
    ap.add_argument("output_yaml", help="Path to revised YAML to write")
    ap.add_argument("--thumbdir", default="thumbnail", help="Thumbnail directory relative to OUTPUT.yaml directory")
    ap.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout in seconds")
    args = ap.parse_args()

    yaml = YAML(typ="rt")  # round-trip
    yaml.preserve_quotes = True
    # Avoid reflowing lines/indent where possible
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=2, offset=0)

    with open(args.input_yaml, "r", encoding="utf-8") as f:
        data = yaml.load(f)

    # 1) Warn on duplicate slugs
    _check_duplicate_slugs(data)

    out_dir = os.path.dirname(os.path.abspath(args.output_yaml))
    thumb_dir = os.path.join(out_dir, args.thumbdir)
    os.makedirs(thumb_dir, exist_ok=True)

    # 2) Process images IN-PLACE keeping order/comments
    for parent, key, rec in _iter_record_refs(data):
        img = rec.get("image")
        if not isinstance(img, str) or not img.strip():
            rec["image"] = ""
            continue

        url = img.strip()
        if not _is_url(url):
            rec["image"] = ""
            continue

        slug = _slug_for_check(rec)
        if not slug:
            rec["image"] = ""
            continue

        # Prefer content-type for extension when available
        ext = ".jpg"
        if requests is not None:
            try:
                h = requests.head(url, allow_redirects=True, timeout=args.timeout)
                if h.status_code == 200:
                    ct = h.headers.get("Content-Type", "")
                    ext = _safe_ext_from_url_or_ct(url, ct)
                else:
                    ext = _safe_ext_from_url_or_ct(url, None)
            except Exception:
                ext = _safe_ext_from_url_or_ct(url, None)
        else:
            ext = _safe_ext_from_url_or_ct(url, None)

        dest_filename = f"{slug}{ext}"
        dest_path = os.path.join(thumb_dir, dest_filename)

        if _download_image_if_valid(url, dest_path, timeout=args.timeout):
            rec["image"] = f"{args.thumbdir.rstrip('/')}/{dest_filename}"
        else:
            rec["image"] = ""

    # 3) Write revised YAML preserving formatting/comments
    with open(args.output_yaml, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

if __name__ == "__main__":
    main()
