#!/usr/bin/env python3
"""
lar-pipe — JSON/YAML pipeline config parser and dry-run planner.

Usage:
  lar-pipe [-n|--dry-run] <config.(json|yaml|yml)>

Notes:
  - JSON requires no extra deps.
  - YAML requires PyYAML. If missing, you'll get a helpful error.
  - Loop stages require FHICL_FILE_PATH to be set so templates can be found.
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
    return []


def build_input_files_args(files: Sequence[str]) -> List[str]:
    """Build ready-to-pass args: -s file1 -s file2 ..."""
    args: List[str] = []
    for f in files:
        args.extend(["-s", f])
    return ' '.join(args)


def find_in_FHICL_FILE_PATH(filename: str, dry_run: bool = False) -> str:
    """Search FHICL_FILE_PATH (colon-separated) for a template FCL file.
    In dry-run mode returns a placeholder string if the file is not found."""
    FHICL_FILE_PATH = os.environ.get("FHICL_FILE_PATH", "")
    for directory in FHICL_FILE_PATH.split(":"):
        if not directory:
            continue
        candidate = os.path.join(directory, filename)
        if os.path.isfile(candidate):
            return candidate
    if dry_run:
        sys.stderr.write(
            f"Warning: template '{filename}' not found in FHICL_FILE_PATH — "
            f"using placeholder in dry-run output.\n"
        )
        return f"<{filename}>"
    sys.stderr.write(
        f"Error: template '{filename}' not found in FHICL_FILE_PATH.\n"
        f"  FHICL_FILE_PATH={FHICL_FILE_PATH!r}\n"
    )
    sys.exit(1)


# ----------------------------
# Stage execution helpers
# ----------------------------

def run_lar_stage(
    cfg_file: str,
    src_file_opt: str,
    nev_opt: str,
    n_skip_opt: str,
    out_root_opt: str,
    out_tfs_opt: str,
    dry_run: bool,
) -> None:
    """Build and optionally execute a single `lar` command."""
    cmd_tokens = [
        'lar',
        f'-c {cfg_file}',
        src_file_opt,
        nev_opt,
        n_skip_opt,
        out_root_opt,
        out_tfs_opt,
    ]
    cmd_line = ' '.join(t for t in cmd_tokens if t)
    print(f"Command: '{cmd_line}'")
    print()

    if dry_run:
        return

    with subprocess.Popen(
        cmd_line.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        for line in proc.stdout:
            print(line, end="")
        rc = proc.wait()
        if rc != 0:
            raise subprocess.CalledProcessError(rc, proc.args)


# Marker string that FCL templates should contain when using the implicit
# (no generator_command) fallback.  Chosen to be safe in FHiCL: the language
# uses @word:: for references and @nil, but never @word@ with a trailing @,
# so this string cannot be mis-parsed as a FHiCL directive.
LOOP_INDEX_MARKER = "@loop_index@"


def run_loop_stage(
    stage_name: str,
    stage_def: Dict[str, Any],
    pipeline_name: str,
    out_dir: str,
    src_file_opt: str,
    nev_opt: str,
    is_last_stage: bool,
    keep_last_art_file: bool,
    keep_last_hist_file: bool,
    dry_run: bool,
) -> str:
    """
    Execute a loop stage: run `lar` n_iter times, each with a freshly
    generated FCL derived from a template.

    FCL generation per iteration (two modes):
      - generator_command present: run the external command with the template
        path appended and redirect stdout to the iteration FCL.
        The string {gen_idx} in generator_command is replaced with the
        current loop index before the command is executed.
      - generator_command absent: read the template, replace every occurrence
        of LOOP_INDEX_MARKER (@loop_index@) with the current loop index, and
        write the result directly.  FCL templates should contain a line like:
          process_apa_index: @loop_index@

    Files for each iteration are kept in a dedicated sub-directory
    {out_dir}/{i}/ to avoid name collisions and aid clean-up.

    Returns the path to the last iteration's ROOT output file.
    """
    template      = stage_def["template"]
    n_iter        = int(stage_def.get("n_iter", 1))
    gen_cmd_tmpl  = stage_def.get("generator_command")   # optional
    delete_inter  = bool(stage_def.get("delete_intermediate_products", False))

    template_path = find_in_FHICL_FILE_PATH(template, dry_run=dry_run)

    prev_root_file: Optional[str] = None
    iter_src_opt = src_file_opt

    for i in range(n_iter):
        iter_is_last = (i == n_iter - 1)

        # Per-iteration subdirectory; FCL always lives inside it.
        # ROOT output for the last iteration is written directly to out_dir so
        # it is immediately available at the stage level without a move step.
        iter_dir  = f"{out_dir}/{i}"
        iter_fcl  = f"{iter_dir}/{stage_name}.fcl"
        if iter_is_last:
            iter_root = f"{out_dir}/{stage_name}_{pipeline_name}.root"
            iter_hist = f"{out_dir}/{stage_name}_{pipeline_name}_hist.root"
        else:
            iter_root = f"{iter_dir}/{stage_name}_{pipeline_name}.root"
            iter_hist = f"{iter_dir}/{stage_name}_{pipeline_name}_hist.root"

        if not dry_run:
            os.makedirs(iter_dir, exist_ok=True)

        # --- FCL generation ---
        if gen_cmd_tmpl is not None:
            gen_cmd = gen_cmd_tmpl.format(gen_idx=i, loop_index=LOOP_INDEX_MARKER)
            full_gen_cmd = f"{gen_cmd} {template_path} > {iter_fcl}"
            print(f"  [iter {i}] Generator (cmd): {full_gen_cmd}")
            if not dry_run:
                result = subprocess.run(full_gen_cmd, shell=True)
                if result.returncode != 0:
                    sys.stderr.write(
                        f"Error: generator command failed at iteration {i}.\n"
                        f"  Command: {full_gen_cmd}\n"
                    )
                    sys.exit(result.returncode)
        else:
            print(
                f"  [iter {i}] Generator (replace): "
                f"'{LOOP_INDEX_MARKER}' → '{i}' in {template_path} → {iter_fcl}"
            )
            if not dry_run:
                with open(template_path, "r", encoding="utf-8") as fh:
                    content = fh.read()
                content = content.replace(LOOP_INDEX_MARKER, str(i))
                with open(iter_fcl, "w", encoding="utf-8") as fh:
                    fh.write(content)

        # --- lar invocation ---
        out_root_opt = (
            f"-o {iter_root}"
            if (not (is_last_stage and iter_is_last)) or keep_last_art_file
            else ''
        )
        out_tfs_opt = (
            f"-T {iter_hist}"
            if (not (is_last_stage and iter_is_last)) or keep_last_hist_file
            else ''
        )

        if not dry_run:
            os.chdir(iter_dir)

        print(f"  [iter {i}] ", end="")
        run_lar_stage(
            cfg_file=iter_fcl,
            src_file_opt=iter_src_opt,
            nev_opt=nev_opt,
            n_skip_opt='',
            out_root_opt=out_root_opt,
            out_tfs_opt=out_tfs_opt,
            dry_run=dry_run,
        )

        # Delete previous iteration's ROOT output once it has been consumed
        if delete_inter and prev_root_file is not None and not dry_run:
            if os.path.isfile(prev_root_file):
                os.remove(prev_root_file)
                print(f"  [iter {i}] Deleted intermediate: {prev_root_file}")

        prev_root_file = iter_root
        iter_src_opt = f"-s {iter_root}"

    return prev_root_file  # last iteration's output, for stage chaining


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
    pipeline_name        = str(cfg.get("pipeline_name", "") or "")
    n_ev                 = int(cfg.get("n_ev", 0) or 0)
    n_skip               = int(cfg.get("n_skip", 0) or 0)
    skip_stages          = int(cfg.get("skip_stages", 0) or 0)
    keep_last_hist_file  = bool(cfg.get("keep_last_hist_file", True))
    keep_last_art_file   = bool(cfg.get("keep_last_art_file", True))

    # input_files: optional string or list
    input_files = as_input_files(cfg.get("input_files"))

    # stages (dict) and sequence (list)
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
    print(f"  input_files   = {' '.join(input_files) if input_files else '(none)'}")
    print(f"  n_ev          = {n_ev}")
    print(f"  n_skip        = {n_skip}")
    print(f"  skip_stages   = {skip_stages}")
    print(f"  keep_last_hist_file = {keep_last_hist_file}")
    print(f"  keep_last_art_file  = {keep_last_art_file}")
    print()

    print("Stages map (in file order):")
    for k in stage_keys:
        v = stages[k]
        if isinstance(v, dict):
            print(f"  {k} → [loop, n_iter={v.get('n_iter')}, template={v.get('template')}]")
        else:
            print(f"  {k} → {v}")
    print()

    print(f"Sequence ({len(sequence)} steps):")
    for s in sequence:
        print(f"  -> {s}")
    print()

    base_dir = os.getcwd()
    last_stage_idx = len(sequence) - 1
    out_root_file: str = ""

    for i, s in enumerate(sequence):
        is_last_stage = (i == last_stage_idx)
        print(f">>> Stage {i}: {s} <<<")

        stage_def = stages.get(s)
        if stage_def is None:
            sys.stderr.write(f"Error: stage '{s}' listed in sequence but not defined in stages.\n")
            sys.exit(1)

        is_loop = isinstance(stage_def, dict)

        # Determine inputs
        if i == 0:
            nev_opt = f"-n {n_ev}"
            if not input_files:
                src_file_opt = ''
                n_skip_opt = ''
            else:
                src_file_opt = build_input_files_args(input_files)
                n_skip_opt = f"--n-skip {n_skip}"
        else:
            nev_opt = "-n -1"
            src_file_opt = f"-s {out_root_file}"
            n_skip_opt = ''

        out_dir = f"{base_dir}/{s}"

        # Compute the expected output path (needed even for skipped stages so
        # the next stage can compute its input correctly).
        if is_loop:
            out_root_file = f"{out_dir}/{s}_{pipeline_name}.root"
            out_tfs_file  = f"{out_dir}/{s}_{pipeline_name}_hist.root"
        else:
            out_root_file = f"{out_dir}/{s}_{pipeline_name}.root"
            out_tfs_file  = f"{out_dir}/{s}_{pipeline_name}_hist.root"

        if i < skip_stages:
            print(f"  <skipping: i={i} < skip_stages={skip_stages}>")
            continue

        os.makedirs(out_dir, exist_ok=True)
        os.chdir(out_dir)

        if is_loop:
            out_root_file = run_loop_stage(
                stage_name=s,
                stage_def=stage_def,
                pipeline_name=pipeline_name,
                out_dir=out_dir,
                src_file_opt=src_file_opt,
                nev_opt=nev_opt,
                is_last_stage=is_last_stage,
                keep_last_art_file=keep_last_art_file,
                keep_last_hist_file=keep_last_hist_file,
                dry_run=args.dry_run,
            )
        elif isinstance(stage_def, str):
            out_root_opt = f"-o {out_root_file}" if (not is_last_stage or keep_last_art_file) else ''
            out_tfs_opt  = f"-T {out_tfs_file}"  if (not is_last_stage or keep_last_hist_file) else ''
            run_lar_stage(
                cfg_file=stage_def,
                src_file_opt=src_file_opt,
                nev_opt=nev_opt,
                n_skip_opt=n_skip_opt,
                out_root_opt=out_root_opt,
                out_tfs_opt=out_tfs_opt,
                dry_run=args.dry_run,
            )
        else:
            sys.stderr.write(
                f"Error: stage '{s}' has unsupported definition type "
                f"({type(stage_def).__name__}). Expected str or dict.\n"
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
