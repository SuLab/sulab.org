#!/usr/bin/env python3
"""
standardize_yaml.py

Usage:
  python standardize_yaml.py INPUT.yaml OUTPUT.yaml
    [--indent-mapping N] [--indent-sequence N] [--indent-offset N]
    [--width N] [--no-preserve-quotes]
    [--strip-trailing-whitespace] [--normalize-newlines]
    [--ensure-eof-newline]

Goal:
- Round-trip your YAML with ruamel.yaml to *standardize indentation/width only*
  while preserving comments, key order, and (by default) quotes.
- Designed to be run BEFORE process_images.py, so diffs later only show 'image' changes.

Notes:
- Requires: ruamel.yaml  (pip install ruamel.yaml)
- This script does NOT change any data values.
- It writes a new file (OUTPUT.yaml). If you prefer in-place, set OUTPUT.yaml to the same path
  (consider using version control or make a backup first).

Defaults chosen to minimize churn:
- indent mapping=2, sequence=2, offset=0
- width 4096 (avoids re-wrapping long lines)
- preserve_quotes = True (can be disabled with --no-preserve-quotes)
"""

import sys
import argparse
from typing import Any

try:
    from ruamel.yaml import YAML
except Exception:
    sys.stderr.write("Error: ruamel.yaml is required. Install with: pip install ruamel.yaml\n")
    raise

def _strip_trailing_ws(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.splitlines())

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input_yaml", help="Path to input YAML")
    ap.add_argument("output_yaml", help="Path to output YAML")
    ap.add_argument("--indent-mapping", type=int, default=2, help="Spaces for mapping indentation (default 2)")
    ap.add_argument("--indent-sequence", type=int, default=2, help="Spaces for sequence indentation (default 2)")
    ap.add_argument("--indent-offset", type=int, default=0, help="Indentation offset for nested sequences (default 0)")
    ap.add_argument("--width", type=int, default=4096, help="Max line width before wrapping (default 4096 to avoid reflow)")
    ap.add_argument("--no-preserve-quotes", action="store_true", help="Disable preserving original quotes")
    ap.add_argument("--strip-trailing-whitespace", action="store_true", help="Strip trailing spaces after dump")
    ap.add_argument("--normalize-newlines", action="store_true", help="Convert CRLF to LF in output")
    ap.add_argument("--ensure-eof-newline", action="store_true", help="Ensure file ends with a single LF")
    args = ap.parse_args()

    yaml = YAML(typ="rt")  # round-trip to preserve comments/structure
    yaml.preserve_quotes = not args.no_preserve_quotes
    yaml.width = args.width
    yaml.indent(mapping=args.indent_mapping, sequence=args.indent_sequence, offset=args.indent_offset)

    # Read and support multi-document YAML
    with open(args.input_yaml, "r", encoding="utf-8", newline="") as f:
        docs = list(yaml.load_all(f))

    # Dump to a string buffer first
    from io import StringIO
    buf = StringIO()
    if len(docs) <= 1:
        yaml.dump(docs[0] if docs else None, buf)
    else:
        yaml.dump_all(docs, buf)

    out = buf.getvalue()

    if args.strip_trailing_whitespace:
        out = _strip_trailing_ws(out)
    if args.normalize_newlines:
        out = out.replace("\r\n", "\n").replace("\r", "\n")
    if args.ensure_eof_newline and not out.endswith("\n"):
        out += "\n"

    with open(args.output_yaml, "w", encoding="utf-8", newline="") as f:
        f.write(out)

if __name__ == "__main__":
    main()
