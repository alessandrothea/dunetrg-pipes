#!/usr/bin/env python3
"""
lar-pipe — lightweight JSON-based pipeline config parser and dry-run planner.

Usage:
  lar-pipe [-n|--dry-run] <config.json>

Example:
  lar-pipe --dry-run config.json
"""

import argparse
import json
import os
import sys

class JSONWithCommentsDecoder(json.JSONDecoder):
    def __init__(self, **kw):
        super().__init__(**kw)

    def decode(self, s: str):
        s = '\n'.join(l if not l.lstrip().startswith('//') else '' for l in s.split('\n'))
        return super().decode(s)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="lar-pipe",
        description="Parse JSON config for a LAr pipeline and print a dry-run plan."
    )
    parser.add_argument(
        "-n", "--dry-run",
        action="store_true",
        help="Do not execute anything; only print what would be done"
    )
    parser.add_argument(
        "config",
        help="Path to JSON config file"
    )
    return parser.parse_args()


def load_json(path):
    if not os.path.isfile(path):
        sys.stderr.write(f"Error: file '{path}' not found!\n")
        sys.exit(1)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f, cls=JSONWithCommentsDecoder)
    except json.JSONDecodeError as e:
        sys.stderr.write(f"Error parsing JSON: {e}\n")
        sys.exit(1)


def as_input_files(node):
    """Return list of input files:
       - string  -> [string]
       - list    -> list
       - missing -> []"""
    if node is None:
        return []
    if isinstance(node, str):
        return [node]
    if isinstance(node, list):
        return [str(x) for x in node]
    raise TypeError("input_files must be a string or a list")


def main():
    args = parse_args()
    cfg = load_json(args.config)

    # --- Scalars with defaults if missing
    pipeline_name = cfg.get("pipeline_name", "") or ""
    lar_area = cfg.get("lar_area", "") or ""
    n_ev = int(cfg.get("n_ev", 0) or 0)
    n_skip = int(cfg.get("n_skip", 0) or 0)
    skip_stages = int(cfg.get("skip_stages", 0) or 0)

    # --- input_files (optional)
    input_files = as_input_files(cfg.get("input_files", None))

    # Build ready-to-pass args: -s file1 -s file2 ...
    input_args = []
    for f in input_files:
        input_args.extend(["-s", f])

    # --- stages (dict) and sequence (list)
    stages = cfg.get("stages") or {}
    if not isinstance(stages, dict):
        sys.stderr.write("Error: 'stages' must be an object in the config.\n")
        sys.exit(1)
    stage_keys = list(stages.keys())  # preserves JSON order

    sequence = cfg.get("sequence") or []
    if not isinstance(sequence, list):
        sys.stderr.write("Error: 'sequence' must be an array in the config.\n")
        sys.exit(1)

    # --- Print info
    print("Standalone variables:")
    print(f"  pipeline_name = {pipeline_name}")
    print(f"  lar_area      = {lar_area}")
    if input_files:
        print(f"  input_files   = {' '.join(input_files)}")
    else:
        print("  input_files   = (none)")
    print(f"  n_ev          = {n_ev}")
    print(f"  n_skip        = {n_skip}")
    print(f"  skip_stages   = {skip_stages}")
    print()

    print("Stages map (in JSON order):")
    for k in stage_keys:
        print(f"  {k} → {stages[k]}")
    print()

    print(f"Sequence ({len(sequence)} steps):")
    for s in sequence:
        print(f"  -> {s}")
    print()

    print(f"Built input args: {len(input_args)//2} item(s)")
    print(f"  {' '.join(input_args) if input_args else '(none)'}")
    print()

    # if args.dry_run:
    #     print("DRY-RUN: planning to execute stages in the listed order.")
    for i, s in enumerate(sequence):
        print(f">>> Stage: '{s}' <<< ")
        # fcl = stages.get(s)
        # if fcl is None:
        #     print(f"  WARNING: sequence item '{s}' not found in stages")
        #     continue
        # print(
        #     f'  lar -c "{fcl}" -p "{pipeline_name}" {" ".join(input_args)}'
        # )
    # print("\nNo commands were executed (dry-run).")


if __name__ == "__main__":
    main()
