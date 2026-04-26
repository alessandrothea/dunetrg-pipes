#!/usr/bin/env python3
"""
lar-richpipe — lar-pipe with rich terminal output.

Usage:
  lar-richpipe [-n|--dry-run] [-p KEY=VALUE ...] <config.(json|yaml|yml)>

Notes:
  - Requires `rich` for coloured output (pip install rich).
    Falls back to plain text if rich is not available.
  - YAML requires PyYAML (pip install pyyaml).
  - Loop stages require FHICL_FILE_PATH to be set.
  - Pipeline datacards are searched via LAR_PIPE_PATH.
"""

from __future__ import annotations
import argparse
import json
import os
import re
import shutil
import sys
import subprocess
from typing import Any, Dict, List, Optional, Sequence

# ----------------------------
# Rich setup (optional dep)
# ----------------------------

try:
    from rich.console import Console
    from rich.table import Table
    from rich.rule import Rule  # noqa: F401  (used implicitly via console.rule)
    _console     = Console(highlight=False)
    _err_console = Console(stderr=True, highlight=False)
    _RICH = True
except ImportError:
    _console = _err_console = None
    _RICH = False


def _strip_markup(s: str) -> str:
    """Remove rich markup tags for plain-text fallback."""
    return re.sub(r'\[/?[^\]]*\]', '', s)


def _print(msg: str = "", **kw) -> None:
    """lar-pipe stdout message."""
    if _RICH:
        _console.print(msg, **kw)
    else:
        print(_strip_markup(msg))


def _warn(msg: str) -> None:
    """Warning to stderr."""
    if _RICH:
        _err_console.print(f"[yellow]Warning:[/yellow] {msg}")
    else:
        sys.stderr.write(f"Warning: {_strip_markup(msg)}\n")


def _error(msg: str) -> None:
    """Error to stderr (caller must sys.exit)."""
    if _RICH:
        _err_console.print(f"[bold red]Error:[/bold red] {msg}")
    else:
        sys.stderr.write(f"Error: {_strip_markup(msg)}\n")


# ----------------------------
# Parser abstraction
# ----------------------------

class ConfigParserBase:
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
            _error(f"file '{path}' not found!")
            sys.exit(1)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f, cls=JSONWithCommentsDecoder)
            if not isinstance(data, dict):
                raise ValueError("Top-level JSON must be an object.")
            return data
        except json.JSONDecodeError as e:
            _error(f"parsing JSON: {e}")
            sys.exit(1)


class YamlConfigParser(ConfigParserBase):
    def load(self, path: str) -> Dict[str, Any]:
        if not os.path.isfile(path):
            _error(f"file '{path}' not found!")
            sys.exit(1)
        try:
            import yaml
        except ImportError:
            _error(
                "YAML requested but PyYAML is not installed.\n"
                "  Install with:  [bold]pip install pyyaml[/bold]\n"
                "  Or convert your YAML to JSON and use that."
            )
            sys.exit(1)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise ValueError("Top-level YAML must be a mapping.")
            return data
        except yaml.YAMLError as e:
            _error(f"parsing YAML: {e}")
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
    _error("unknown config extension. Please use one of: .json, .yaml, .yml")
    sys.exit(1)


# ----------------------------
# Utilities
# ----------------------------

def as_input_files(node: Any) -> List[str]:
    if node is None:
        return []
    if isinstance(node, str):
        return [node]
    if isinstance(node, list):
        return [str(x) for x in node]
    return []


def build_input_files_args(files: Sequence[str]) -> str:
    args: List[str] = []
    for f in files:
        args.extend(["-s", f])
    return ' '.join(args)


def resolve_config_path(name: str) -> str:
    """Resolve a pipeline datacard filename via LAR_PIPE_PATH."""
    if os.path.isabs(name) or os.path.isfile(name):
        return name
    lar_pipe_path = os.environ.get("LAR_PIPE_PATH", "")
    for directory in lar_pipe_path.split(":"):
        if not directory:
            continue
        candidate = os.path.join(directory, name)
        if os.path.isfile(candidate):
            return candidate
    searched = [d for d in lar_pipe_path.split(":") if d]
    locations = ["<CWD>"] + searched
    _error(
        f"config '[bold]{name}[/bold]' not found in: {', '.join(locations)}\n"
        f"  Set [bold]LAR_PIPE_PATH[/bold] to a colon-separated list of datacard directories."
    )
    sys.exit(1)


def _resolve_fcl(name: str) -> Optional[str]:
    """Return the resolved path of a FCL file, or None if not found.
    Checks: absolute path, CWD-relative path, then FHICL_FILE_PATH."""
    if os.path.isabs(name):
        return name if os.path.isfile(name) else None
    if os.path.isfile(name):
        return name
    fhicl_path = os.environ.get("FHICL_FILE_PATH", "")
    for directory in fhicl_path.split(":"):
        if not directory:
            continue
        candidate = os.path.join(directory, name)
        if os.path.isfile(candidate):
            return candidate
    return None


def preflight_check_fcls(
    stages: Dict[str, Any],
    sequence: List[str],
    first_stage: int,
    last_stage_run: int,
    dry_run: bool,
) -> None:
    """Check that all FCL files for stages that will run can be resolved.
    Collects all failures before reporting so the user sees everything at once."""
    # Gather results first so we know the outcome before printing.
    rows: List[tuple] = []   # (idx, stage_name, fcl_name, resolved_or_None)
    for idx, name in enumerate(sequence):
        if idx < first_stage or idx > last_stage_run:
            continue
        stage_def = stages.get(name)
        if isinstance(stage_def, str):
            fcl_name = stage_def
        elif isinstance(stage_def, dict):
            fcl_name = stage_def.get("template", "")
        else:
            continue
        rows.append((idx, name, fcl_name, _resolve_fcl(fcl_name)))

    missing = [r for r in rows if r[3] is None]

    if _RICH:
        _console.print()
        _console.rule("[bold]Pre-flight FCL check[/bold]", style="dim white", align="left")
        tbl = Table(show_header=True,
                    box=__import__('rich.box', fromlist=['SIMPLE']).SIMPLE)
        tbl.add_column("#",       style="dim", width=3)
        tbl.add_column("Stage",   style="bold")
        tbl.add_column("FCL",     style="cyan")
        tbl.add_column("Status")
        for idx, name, fcl_name, resolved in rows:
            if resolved:
                status = f"[green]\u2714[/green] [dim]{resolved}[/dim]"
            else:
                status = "[bold red]\u2718  not found[/bold red]"
            tbl.add_row(str(idx), name, fcl_name, status)
        _console.print(tbl)
    else:
        print("\nPre-flight FCL check")
        print("-" * 40)
        for idx, name, fcl_name, resolved in rows:
            mark   = "\u2714" if resolved else "\u2718"
            detail = resolved if resolved else "NOT FOUND"
            print(f"  {mark}  [{idx}] {name}  {fcl_name}  ->  {detail}")
        print()

    if missing:
        if dry_run:
            _warn(f"{len(missing)} FCL file(s) not found (dry-run: continuing anyway)")
        else:
            _error(
                f"{len(missing)} FCL file(s) could not be resolved. "
                f"Check FHICL_FILE_PATH and stage definitions."
            )
            sys.exit(1)


def find_in_FHICL_FILE_PATH(filename: str, dry_run: bool = False) -> str:
    """Search FHICL_FILE_PATH for a template FCL file."""
    fhicl_path = os.environ.get("FHICL_FILE_PATH", "")
    for directory in fhicl_path.split(":"):
        if not directory:
            continue
        candidate = os.path.join(directory, filename)
        if os.path.isfile(candidate):
            return candidate
    if dry_run:
        _warn(
            f"template '[bold]{filename}[/bold]' not found in FHICL_FILE_PATH — "
            f"using placeholder in dry-run output."
        )
        return f"<{filename}>"
    _error(
        f"template '[bold]{filename}[/bold]' not found in FHICL_FILE_PATH.\n"
        f"  FHICL_FILE_PATH={fhicl_path!r}"
    )
    sys.exit(1)


# ----------------------------
# Output helpers
# ----------------------------

def _check_input_files(src_opt: str, dry_run: bool) -> None:
    """Parse -s <file> tokens from src_opt and verify each file exists.
    In dry-run mode reports status without exiting."""
    if not src_opt:
        return
    parts = src_opt.split()
    files = [parts[j + 1] for j, p in enumerate(parts) if p == '-s' and j + 1 < len(parts)]
    if dry_run:
        for f in files:
            if f.startswith(("http://", "https://", "root://", "xroot://")):
                _print(f"  [blue]↗[/blue] input url:       [dim]{f}[/dim]")
            elif os.path.isfile(f):
                _print(f"  [green]✔[/green] input exists:    [dim]{f}[/dim]")
            else:
                _print(f"  [yellow]✘ input not found: {f}[/yellow]")
        return
    missing = [f for f in files if not f.startswith(("http://", "https://", "root://", "xroot://")) and not os.path.isfile(f)]
    if missing:
        for f in missing:
            _error(f"input file not found: [bold]{f}[/bold]")
        sys.exit(1)


def _stage_rule(i: int, total: int, name: str, stage_def: Any) -> None:
    """Print a stage separator using rich Rule or a plain bar."""
    tag = (
        f"[loop \u00d7{stage_def.get('n_step', '?')}]"
        if isinstance(stage_def, dict) else "[simple]"
    )
    title = f"Stage {i + 1}/{total}: {name}  {tag}"
    if _RICH:
        _console.print()
        _console.rule(f"[bold green]{title}[/bold green]", style="dim white", align="left")
    else:
        bar = "\u2500" * max(60, len(title) + 4)
        print(f"\n{bar}\n  {title}\n{bar}")


def _print_summary(
    pipeline_name: str,
    config_path: str,
    input_files: List[str],
    n_events: int,
    skip_events: int,
    first_event: Any,
    first_stage: int,
    last_stage_run: int,
    keep_last_hist_file: bool,
    keep_last_art_file: bool,
    stages: Dict[str, Any],
    sequence: List[str],
) -> None:
    """Print pipeline configuration as rich tables (or plain text)."""
    if _RICH:
        # --- config params table ---
        cfg_table = Table(show_header=False, box=None, padding=(0, 2))
        cfg_table.add_column(style="bold cyan", no_wrap=True)
        cfg_table.add_column()
        cfg_table.add_row("pipeline",    f"[bold]{pipeline_name}[/bold]")
        cfg_table.add_row("config",      config_path)
        cfg_table.add_row("input_files", ' '.join(input_files) if input_files else "[dim](none)[/dim]")
        cfg_table.add_row("n_events",     str(n_events))
        cfg_table.add_row("skip_events",  str(skip_events))
        cfg_table.add_row("first_event",
            f"{first_event['run']}:{first_event['subrun']}:{first_event['event']}"
            if isinstance(first_event, dict) else "[dim](none)[/dim]"
        )
        cfg_table.add_row("first_stage", str(first_stage))
        cfg_table.add_row("last_stage",  str(last_stage_run))
        cfg_table.add_row("keep_last_art_file",  str(keep_last_art_file))
        cfg_table.add_row("keep_last_hist_file", str(keep_last_hist_file))
        _console.print(cfg_table)

        # --- stages table ---
        stg_table = Table(show_header=True, box=__import__('rich.box', fromlist=['SIMPLE']).SIMPLE)
        stg_table.add_column("#",     style="dim", width=3)
        stg_table.add_column("Stage", style="bold")
        stg_table.add_column("Type",  style="cyan", no_wrap=True)
        stg_table.add_column("Configuration")
        for idx, name in enumerate(sequence):
            v = stages.get(name)
            if isinstance(v, dict):
                stype = f"loop \u00d7{v.get('n_step', '?')}"
                lsp   = v.get('last_step_products', 'symlink')
                sconf = f"template: {v.get('template', '?')}  last_step: {lsp}"
            elif isinstance(v, str):
                stype = "simple"
                sconf = v
            else:
                stype = "?"
                sconf = str(v)
            out_of_range = idx < first_stage or idx > last_stage_run
            row_style = "dim" if out_of_range else ""
            if idx < first_stage:
                skip_mark = " [dim](before first_stage)[/dim]"
            elif idx > last_stage_run:
                skip_mark = " [dim](after last_stage)[/dim]"
            else:
                skip_mark = ""
            stg_table.add_row(str(idx), name + skip_mark, stype, sconf, style=row_style)
        _console.print(stg_table)
    else:
        # Plain fallback
        print(f"pipeline      = {pipeline_name}")
        print(f"config        = {config_path}")
        print(f"input_files   = {' '.join(input_files) if input_files else '(none)'}")
        print(f"n_events      = {n_events}")
        print(f"skip_events   = {skip_events}")
        fe_str = (
            f"{first_event['run']}:{first_event['subrun']}:{first_event['event']}"
            if isinstance(first_event, dict) else "(none)"
        )
        print(f"first_event   = {fe_str}")
        print(f"first_stage   = {first_stage}")
        print(f"last_stage    = {last_stage_run}")
        print(f"keep_last_art_file  = {keep_last_art_file}")
        print(f"keep_last_hist_file = {keep_last_hist_file}")
        print()
        print("Stages (sequence order):")
        for idx, name in enumerate(sequence):
            v = stages.get(name)
            if idx < first_stage:
                marker = " [before first_stage]"
            elif idx > last_stage_run:
                marker = " [after last_stage]"
            else:
                marker = ""
            if isinstance(v, dict):
                lsp = v.get('last_step_products', 'symlink')
                print(f"  {idx}. {name}  [loop ×{v.get('n_step','?')}  last_step: {lsp}]{marker}")
            else:
                print(f"  {idx}. {name}  → {v}{marker}")
        print()


# ----------------------------
# Stage execution helpers
# ----------------------------

_GDB_PREFIX = 'gdb -q -ex "catch throw" -ex run --args'


def run_lar_stage(
    cfg_file: str,
    src_file_opt: str,
    nev_opt: str,
    skip_events_opt: str,
    out_root_opt: str,
    out_tfs_opt: str,
    dry_run: bool,
    prefix: str = "",
    first_event_opt: str = "",
    use_gdb: bool = False,
) -> None:
    """Build and optionally execute a single `lar` command."""
    cmd_tokens = [
        'lar',
        f'-c {cfg_file}',
        src_file_opt,
        nev_opt,
        skip_events_opt,
        first_event_opt,
        out_root_opt,
        out_tfs_opt,
    ]
    cmd_line = ' '.join(t for t in cmd_tokens if t)
    if use_gdb:
        cmd_line = f"{_GDB_PREFIX} {cmd_line}"
    _print(f"{prefix}[bold]$[/bold] [cyan]{cmd_line}[/cyan]")

    if dry_run:
        return

    with subprocess.Popen(
        cmd_line,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            print(line, end="")   # raw larsoft output — intentionally unstyled
        rc = proc.wait()
        if rc != 0:
            raise subprocess.CalledProcessError(rc, proc.args)


# Marker for the implicit (no generator_command) FCL template fallback.
# Safe in FHiCL: the language uses @word:: or @nil, never @word@ with trailing @.
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
    first_event_opt: str = "",
    use_gdb: bool = False,
) -> str:
    """
    Execute a loop stage: run `lar` n_step times with per-step FCLs.
    Returns the path to the last step's ROOT output file.
    """
    template       = stage_def["template"]
    n_step         = int(stage_def.get("n_step", 1))
    n_digits       = len(str(max(n_step - 1, 0)))
    skip_step      = int(stage_def.get("skip_step", 0) or 0)
    gen_cmd_tmpl   = stage_def.get("generator_command")
    delete_inter   = bool(stage_def.get("delete_intermediate_products", False))
    last_step_mode = stage_def.get("last_step_products", "symlink")
    if last_step_mode not in ("symlink", "move"):
        _error(f"stage '[bold]{stage_name}[/bold]': 'last_step_products' must be 'symlink' or 'move', got '{last_step_mode}'.")
        sys.exit(1)

    template_path = find_in_FHICL_FILE_PATH(template, dry_run=dry_run)

    if skip_step > 0:
        last_skipped_dir  = f"{out_dir}/step_{skip_step - 1:0{n_digits}d}"
        last_skipped_root = f"{last_skipped_dir}/{stage_name}_{pipeline_name}.root"
        step_src_opt = f"-s {last_skipped_root}"
        _check_input_files(step_src_opt, dry_run)
        _print(
            f"  [dim]\u23e9 skipping steps [bold]0..{skip_step - 1}[/bold]; "
            f"assuming input: {last_skipped_root}[/dim]"
        )
    else:
        step_src_opt = src_file_opt

    prev_root_file: Optional[str] = None
    prev_hist_file: Optional[str] = None

    for i in range(n_step):
        step_is_last = (i == n_step - 1)
        step_dir  = f"{out_dir}/step_{i:0{n_digits}d}"
        step_fcl  = f"{step_dir}/{stage_name}.fcl"
        step_root = f"{step_dir}/{stage_name}_{pipeline_name}.root"
        step_hist = f"{step_dir}/{stage_name}_{pipeline_name}_hist.root"

        if i < skip_step:
            prev_root_file = step_root
            prev_hist_file = step_hist
            continue

        if not dry_run:
            if not os.path.isdir(step_dir):
                os.makedirs(step_dir)
                _print(f"  [step {i}] [green]\u271a Created:[/green] {step_dir}")

        # --- FCL generation ---
        if gen_cmd_tmpl is not None:
            gen_cmd      = gen_cmd_tmpl.format(gen_idx=i, loop_index=LOOP_INDEX_MARKER)
            full_gen_cmd = f"{gen_cmd} {template_path} > {step_fcl}"
            _print(f"  [step {i}] [cyan]gen[/cyan] (cmd): {full_gen_cmd}")
            if not dry_run:
                proc = subprocess.Popen(
                    full_gen_cmd, shell=True,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True,
                )
                for line in proc.stdout:
                    print(line, end="")
                rc = proc.wait()
                if rc != 0:
                    _error(
                        f"generator command failed at step {i}.\n"
                        f"  Command: {full_gen_cmd}"
                    )
                    sys.exit(rc)
        else:
            _print(
                f"  [step {i}] [cyan]gen[/cyan] (replace): "
                f"[dim]'{LOOP_INDEX_MARKER}'[/dim] \u2192 [bold]{i}[/bold]"
                f"  {template_path} \u2192 {step_fcl}"
            )
            if not dry_run:
                with open(template_path, "r", encoding="utf-8") as fh:
                    content = fh.read()
                content = content.replace(LOOP_INDEX_MARKER, str(i))
                with open(step_fcl, "w", encoding="utf-8") as fh:
                    fh.write(content)

        # --- lar invocation ---
        out_root_opt = (
            f"-o {step_root}"
            if (not (is_last_stage and step_is_last)) or keep_last_art_file
            else ''
        )
        out_tfs_opt = (
            f"-T {step_hist}"
            if (not (is_last_stage and step_is_last)) or keep_last_hist_file
            else ''
        )

        if not dry_run:
            os.chdir(step_dir)

        run_lar_stage(
            cfg_file=step_fcl,
            src_file_opt=step_src_opt,
            nev_opt=nev_opt,
            skip_events_opt='',   # skip_events applies only to the first non-loop stage
            out_root_opt=out_root_opt,
            out_tfs_opt=out_tfs_opt,
            dry_run=dry_run,
            prefix=f"  [step {i}] ",
            first_event_opt=first_event_opt if i == skip_step else '',
            use_gdb=use_gdb,
        )

        if delete_inter and prev_root_file is not None and not dry_run:
            if os.path.isfile(prev_root_file):
                os.remove(prev_root_file)
                _print(f"  [step {i}] [dim red]\u2717 deleted:[/dim red] {prev_root_file}")

        prev_root_file = step_root
        prev_hist_file = step_hist
        step_src_opt   = f"-s {step_root}"

    if not dry_run:
        os.chdir(out_dir)

    # Place the last step's outputs in out_dir so downstream stages and the
    # user can reference them at the stage level without knowing the step index.
    link_root = f"{out_dir}/{stage_name}_{pipeline_name}.root"
    link_hist = f"{out_dir}/{stage_name}_{pipeline_name}_hist.root"

    if last_step_mode == "symlink":
        # Relative targets keep the directory structure portable.
        rel_root = os.path.relpath(prev_root_file, out_dir)
        rel_hist = os.path.relpath(prev_hist_file, out_dir)
        for link, rel_target in ((link_root, rel_root), (link_hist, rel_hist)):
            _print(f"  [dim]\u2192 symlink:[/dim] [cyan]{link}[/cyan] \u2192 [dim]{rel_target}[/dim]")
            if not dry_run:
                if os.path.islink(link):
                    os.unlink(link)
                os.symlink(rel_target, link)
    else:  # "move"
        for src, dst in ((prev_root_file, link_root), (prev_hist_file, link_hist)):
            _print(f"  [dim]\u2192 move:[/dim] [cyan]{src}[/cyan] \u2192 [dim]{dst}[/dim]")
            if not dry_run:
                shutil.move(src, dst)

    return link_root


# ----------------------------
# CLI helpers
# ----------------------------

def apply_overrides(cfg: Dict[str, Any], params: List[str]) -> None:
    """Apply KEY=VALUE overrides to cfg in-place. KEY may use dot notation."""
    try:
        import yaml as _yaml
        _loads = _yaml.safe_load
    except ImportError:
        import json as _json
        def _loads(s):
            try:
                return _json.loads(s)
            except _json.JSONDecodeError:
                return s

    for param in params:
        if "=" not in param:
            _error(f"--param '[bold]{param}[/bold]' must be in KEY=VALUE form.")
            sys.exit(1)
        key_path, _, raw_value = param.partition("=")
        keys  = key_path.split(".")
        value = _loads(raw_value)

        node = cfg
        for k in keys[:-1]:
            if not isinstance(node, dict):
                _error(f"--param '{param}': key path '[bold]{key_path}[/bold]' not found in config.")
                sys.exit(1)
            if k not in node:
                node[k] = {}   # auto-create missing intermediate dicts
            node = node[k]
        node[keys[-1]] = value
        _print(f"  [bold yellow]\u21ba[/bold yellow] override  [bold]{key_path}[/bold] = {value!r}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="lar-richpipe",
        description="LAr pipeline runner with rich terminal output.",
    )
    p.add_argument("-n", "--dry-run", action="store_true",
                   help="Print commands without executing")
    p.add_argument("-s", "--summary", action="store_true",
                   help="Print the pipeline summary table and exit")
    p.add_argument("-g", "--gdb", action="store_true",
                   help="Run lar inside gdb (catch throw + run)")
    p.add_argument("-p", "--param", metavar="KEY=VALUE", action="append",
                   default=[], dest="params",
                   help="Override a config parameter (dot notation, repeatable)")
    p.add_argument("config", help="Pipeline datacard (.json | .yaml | .yml)")
    return p.parse_args()


def build_first_event_opt(first_event: Any) -> str:
    """Validate and convert a first_event config value to a lar -e option string."""
    if not isinstance(first_event, dict):
        return ''
    missing = [k for k in ("run", "subrun", "event") if k not in first_event]
    if missing:
        _error(f"'first_event' is missing required keys: {missing}")
        sys.exit(1)
    return f"-e {first_event['run']}:{first_event['subrun']}:{first_event['event']}"


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    args = parse_args()

    config_path = resolve_config_path(args.config)
    parser      = get_parser_for(config_path)
    cfg         = parser.load(config_path)
    apply_overrides(cfg, args.params)

    pipeline_name       = str(cfg.get("pipeline_name", "") or "")
    n_events            = int(cfg.get("n_events", 0) or 0)
    skip_events         = int(cfg.get("skip_events", 0) or 0)
    first_stage         = int(cfg.get("first_stage", 0) or 0)
    last_stage          = cfg.get("last_stage")   # None → default to full sequence
    first_event         = cfg.get("first_event")  # None, or dict with run/subrun/event
    first_event_opt     = build_first_event_opt(first_event)
    keep_last_hist_file = bool(cfg.get("keep_last_hist_file", True))
    keep_last_art_file  = bool(cfg.get("keep_last_art_file", True))
    input_files         = as_input_files(cfg.get("input_files"))

    stages = cfg.get("stages") or {}
    if not isinstance(stages, dict):
        _error("'stages' must be a mapping/object in the config.")
        sys.exit(1)

    sequence = cfg.get("sequence") or []
    if not isinstance(sequence, list):
        _error("'sequence' must be a list/array in the config.")
        sys.exit(1)

    n_stages       = len(sequence)
    last_stage_run = (n_stages - 1) if last_stage is None else int(last_stage)

    _print_summary(
        pipeline_name, config_path, input_files,
        n_events, skip_events, first_event, first_stage, last_stage_run,
        keep_last_hist_file, keep_last_art_file,
        stages, sequence,
    )

    preflight_check_fcls(stages, sequence, first_stage, last_stage_run, args.dry_run)

    if args.summary:
        return

    base_dir      = os.getcwd()
    out_root_file: str = ""

    for i, s in enumerate(sequence):
        is_last_stage = (i == last_stage_run)
        # keep_last_* flags suppress output only at the true end of the full
        # sequence. If last_stage is explicitly set, all stages write their
        # output files unconditionally.
        apply_keep_flags = is_last_stage and last_stage is None

        stage_def = stages.get(s)
        if stage_def is None:
            _error(f"stage '[bold]{s}[/bold]' listed in sequence but not defined in stages.")
            sys.exit(1)

        _stage_rule(i, len(sequence), s, stage_def)

        is_loop = isinstance(stage_def, dict)

        if i == 0:
            nev_opt = f"-n {n_events}"
            if not input_files:
                src_file_opt = ''
                skip_events_opt   = ''
            else:
                src_file_opt = build_input_files_args(input_files)
                skip_events_opt   = f"--nskip {skip_events}"
        else:
            nev_opt      = "-n -1"
            src_file_opt = f"-s {out_root_file}"
            skip_events_opt   = ''

        out_dir       = f"{base_dir}/{s}"
        out_root_file = f"{out_dir}/{s}_{pipeline_name}.root"
        out_tfs_file  = f"{out_dir}/{s}_{pipeline_name}_hist.root"

        if i < first_stage:
            _print(f"  [dim]\u23e9 skipped (before first_stage) — output assumed at {out_root_file}[/dim]")
            continue
        if i > last_stage_run:
            _print(f"  [dim]\u23e9 skipped (after last_stage)[/dim]")
            continue

        _check_input_files(src_file_opt, args.dry_run)

        if not args.dry_run:
            if not os.path.isdir(out_dir):
                os.makedirs(out_dir)
                _print(f"  [green]\u271a Created:[/green] {out_dir}")
            os.chdir(out_dir)

        if is_loop:
            out_root_file = run_loop_stage(
                stage_name=s,
                stage_def=stage_def,
                pipeline_name=pipeline_name,
                out_dir=out_dir,
                src_file_opt=src_file_opt,
                nev_opt=nev_opt,
                is_last_stage=apply_keep_flags,
                keep_last_art_file=keep_last_art_file,
                keep_last_hist_file=keep_last_hist_file,
                dry_run=args.dry_run,
                first_event_opt=first_event_opt,
                use_gdb=args.gdb,
            )
        elif isinstance(stage_def, str):
            out_root_opt = f"-o {out_root_file}" if (not apply_keep_flags or keep_last_art_file) else ''
            out_tfs_opt  = f"-T {out_tfs_file}"  if (not apply_keep_flags or keep_last_hist_file) else ''
            run_lar_stage(
                cfg_file=stage_def,
                src_file_opt=src_file_opt,
                nev_opt=nev_opt,
                skip_events_opt=skip_events_opt,
                out_root_opt=out_root_opt,
                out_tfs_opt=out_tfs_opt,
                dry_run=args.dry_run,
                first_event_opt=first_event_opt,
                use_gdb=args.gdb,
            )
        else:
            _error(
                f"stage '[bold]{s}[/bold]' has unsupported definition type "
                f"({type(stage_def).__name__}). Expected str or dict."
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
