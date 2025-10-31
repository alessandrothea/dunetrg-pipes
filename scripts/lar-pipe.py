#!/usr/bin/env python3
"""
lar-pipe — JSON/YAML pipeline config parser and dry-run planner.

Usage:
  lar-pipe [-n|--dry-run] <config.(json|yaml|yml)>

Notes:
  - JSON requires no extra deps.
  - YAML requires PyYAML. If missing, you'll get a helpful error.
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import subprocess
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ----------------------------
# Parser abstraction
# ----------------------------

class ConfigParserBase:
    """Abstract interface for config parsers."""
    def load(self, path: str) -> Dict[str, Any]:
        raise NotImplementedError


class JSONWithCommentsDecoder(json.JSONDecoder):
    def __init__(self, **kw):
        super().__init__(**kw)

    def decode(self, s: str):
        s = '\n'.join(l if not l.lstrip().startswith('//') else '' for l in s.split('\n'))
        return super().decode(s)


class JsonConfigParser(ConfigParserBase):
    def load(self, path: str) -> Dict[str, Any]:
        if not os.path.isfile(path):
            sys.stderr.write(f"Error: file '{path}' not found!\n")
            sys.exit(1)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f, cls=JSONWithCommentsDecoder)
            if not isinstance(data, dict):
                raise ValueError("Top-level JSON must be an object.")
            return data
        except json.JSONDecodeError as e:
            sys.stderr.write(f"Error parsing JSON: {e}\n")
            sys.exit(1)


class YamlConfigParser(ConfigParserBase):
    def load(self, path: str) -> Dict[str, Any]:
        if not os.path.isfile(path):
            sys.stderr.write(f"Error: file '{path}' not found!\n")
            sys.exit(1)
        try:
            import yaml  # PyYAML
        except ImportError:
            sys.stderr.write(
                "Error: YAML requested but PyYAML is not installed.\n"
                "Install with:  pip install pyyaml\n"
                "Or convert your YAML to JSON and use that.\n"
            )
            sys.exit(1)

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise ValueError("Top-level YAML must be a mapping.")
            return data
        except yaml.YAMLError as e:
            sys.stderr.write(f"Error parsing YAML: {e}\n")
            sys.exit(1)


# ----------------------------
# Parser factory
# ----------------------------

def get_parser_for(path: str) -> ConfigParserBase:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".json", ".jsonc"):
        return JsonConfigParser()
    if ext in (".yaml", ".yml"):
        return YamlConfigParser()
    sys.stderr.write(
        "Error: unknown config extension. Please use one of: .json, .yaml, .yml\n"
    )
    sys.exit(1)


# ----------------------------
# Utilities
# ----------------------------

def as_input_files(node: Any) -> List[str]:
    """
    Normalize input_files:
      - string -> [string]
      - list   -> list of strings
      - missing/None/other -> []
    """
    if node is None:
        return []
    if isinstance(node, str):
        return [node]
    if isinstance(node, list):
        return [str(x) for x in node]
    # be forgiving: anything else -> []
    return []


def build_input_files_args(files: Sequence[str]) -> List[str]:
    """Build ready-to-pass args: -s file1 -s file2 ..."""
    args: List[str] = []
    for f in files:
        args.extend(["-s", f])
    return ' '.join(args)


# ----------------------------
# CLI & main
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="lar-pipe",
        description="Parse JSON/JSONC/YAML config for a LAr pipeline and print a dry-run plan."
    )
    p.add_argument(
        "-n", "--dry-run",
        action="store_true",
        help="Do not execute anything; only print what would be done"
    )
    p.add_argument(
        "config",
        help="Path to config file (.json | .jsonc | .yaml | .yml)"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    parser = get_parser_for(args.config)
    cfg = parser.load(args.config)

    # Scalars with defaults if missing
    pipeline_name = str(cfg.get("pipeline_name", "") or "")
    lar_area      = str(cfg.get("lar_area", "") or "")
    n_ev          = int(cfg.get("n_ev", 0) or 0)
    n_skip        = int(cfg.get("n_skip", 0) or 0)        # default 0 if missing
    skip_stages   = int(cfg.get("skip_stages", 0) or 0)

    # input_files: optional string or list
    input_files = as_input_files(cfg.get("input_files"))

    # stages (dict) and sequence (list). Python 3.7+ dict preserves insertion order.
    stages = cfg.get("stages") or {}
    if not isinstance(stages, dict):
        sys.stderr.write("Error: 'stages' must be a mapping/object in the config.\n")
        sys.exit(1)
    stage_keys = list(stages.keys())

    sequence = cfg.get("sequence") or []
    if not isinstance(sequence, list):
        sys.stderr.write("Error: 'sequence' must be a list/array in the config.\n")
        sys.exit(1)

    # Output
    print("Standalone variables:")
    print(f"  pipeline_name = {pipeline_name}")
    print(f"  lar_area      = {lar_area}")
    print(f"  input_files   = {' '.join(input_files) if input_files else '(none)'}")
    print(f"  n_ev          = {n_ev}")
    print(f"  n_skip        = {n_skip}")
    print(f"  skip_stages   = {skip_stages}")
    print()

    print("Stages map (in file order):")
    for k in stage_keys:
        print(f"  {k} → {stages[k]}")
    print()

    print(f"Sequence ({len(sequence)} steps):")
    for s in sequence:
        print(f"  -> {s}")
    print()

    # print(f"Built input args: {len(input_args)//2} item(s)")
    # print(f"  {' '.join(input_args) if input_args else '(none)'}")
    # print()

    # if args.dry_run:
    #     print("DRY-RUN: planning to execute stages in the listed order.")
    #     for s in sequence:
    #         fcl = stages.get(s)
    #         if fcl is None:
    #             print(f"  WARNING: sequence item '{s}' not found in stages")
    #             continue
    #         print(
    #             f'  your_command --stage "{s}" --fcl "{fcl}" '
    #             f'-p "{pipeline_name}" {" ".join(input_args)}'
    #         )
    #     print("\nNo commands were executed (dry-run).")

    base_dir=os.getcwd()

    for i,s in enumerate(sequence):
        print(f">>> Stage {i}: {s} <<< ")

        

        if ( i == 0 ):
            k=n_ev

            # No input files, generation pipeline
            if (len(input_files)) == 0:
                src_file_opt=''
                n_skip_opt=''
            else:
                src_file_opt = build_input_files_args(input_files)
                n_skip_opt=f"--n-skip {n_skip}"
        else:
            k='-1'
            src_file_opt=f"-s {out_root_file}"
            n_skip_opt=''

        cfg_file=stages[s]
        out_dir=f"{base_dir}/{s}" 
        out_root_file=f"{out_dir}/{s}_{pipeline_name}.root"
        out_tfs_file=f"{out_dir}/{s}_{pipeline_name}_hist.root"

        if ( i < skip_stages):
            print(f" <skipping {i}<skip_stages ({skip_stages})> ")
            continue

        os.makedirs(out_dir, exist_ok=True)
        os.chdir(out_dir)
        cmd_line=f"lar -c {cfg_file} {src_file_opt} -o {out_root_file} -T {out_tfs_file} -n {k} {n_skip_opt}"
        print(f"Command: '{cmd_line}'")
        print()

        if args.dry_run:
            continue

        with subprocess.Popen(
            # ["your_command", "--stage", stage, "--fcl", fcl, "-p", pipeline_name, *input_args],
            cmd_line.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered
        ) as proc:
            for line in proc.stdout:
                print(line, end="")  # stream to console
            rc = proc.wait()  # blocks until done
            if rc != 0:
                raise subprocess.CalledProcessError(rc, proc.args)


if __name__ == "__main__":
    main()
