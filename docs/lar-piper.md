# lar-piper.py

A pipeline runner for LArSoft simulation chains. It reads a YAML (or JSON) datacard that describes a sequence of `lar` jobs, wires their inputs and outputs together automatically, and executes them in order. A dry-run mode prints every command without executing anything.

## Requirements

- Python 3.7+
- `pyyaml` (`pip install pyyaml`) for YAML datacards
- `lar` on `PATH`
- `LAR_PIPE_PATH` — colon-separated list of directories searched for pipeline datacard files (optional; CWD and absolute paths always work without it)
- `FHICL_FILE_PATH` — set correctly when using loop stages with templates

## Usage

```
lar-piper.py [-n] [-p KEY=VALUE ...] <config.yaml>
```

| Flag | Description |
|------|-------------|
| `-n`, `--dry-run` | Print commands without executing |
| `-p KEY=VALUE` | Override any config parameter (repeatable) |

## Datacard format

### Top-level fields

| Key | Type | Description |
|-----|------|-------------|
| `pipeline_name` | string | Used as a suffix in all output file names |
| `n_events` | int | Number of events for the first stage (`-n`) |
| `n_skip` | int | Events to skip in the first stage (`--n-skip`), only when `input_files` is set |
| `first_stage` | int | Index of the first stage to execute; earlier stages are skipped (default `0`) |
| `last_stage` | int | Index of the last stage to execute; later stages are not run (default last) |
| `keep_last_art_file` | bool | Write the ROOT art file for the last stage (default `True`) |
| `keep_last_hist_file` | bool | Write the histogram ROOT file for the last stage (default `True`) |
| `input_files` | string or list | External input file(s) for the first stage; omit for generation pipelines |
| `stages` | mapping | Stage definitions (see below) |
| `sequence` | list | Ordered list of stage names to execute |

### Simple stage

```yaml
stages:
  gen: "prodmarley_nue_cc_flat_dunevd10kt_1x8x14_3view_30deg.fcl"
  g4:  "supernova_g4_dunevd10kt_1x8x14_3view_30deg.fcl"
```

The value is a path to an FCL file. Relative paths are resolved from the working directory; absolute paths and paths starting with `./` work as usual. Files in `FHICL_FILE_PATH` can be referenced by name only.

### Loop stage

A stage whose value is a mapping is treated as a loop: `lar` is run `n_iter` times, each time with a freshly generated FCL derived from a template.

```yaml
stages:
  detsim_loop:
    template: "detsim_dunevd10kt_1x8x14_3view_30deg_tpc_only_template.fcl"
    n_iter: 112
    skip_iter: 0                       # optional, default 0
    generator_command: "sed 's/@loop_index@/process_apa_index: {gen_idx}/'"  # optional
    delete_intermediate_products: True # optional, default False
```

| Key | Required | Description |
|-----|----------|-------------|
| `template` | yes | FCL template filename; looked up in `FHICL_FILE_PATH` |
| `n_iter` | yes | Total number of iterations |
| `skip_iter` | no | Skip the first N iterations (assume their output already exists) |
| `generator_command` | no | Shell command to produce the per-iteration FCL (see below) |
| `delete_intermediate_products` | no | Delete each iteration's ROOT output once consumed by the next |

#### FCL generation — two modes

**With `generator_command`** (external command):

The command is run as a shell pipeline with the template path appended, and stdout is redirected to the iteration's FCL file:

```
<generator_command> <template_path> > <iter_dir>/<stage>.fcl
```

Two Python format placeholders are expanded in `generator_command` before it is executed:

| Placeholder | Expands to |
|-------------|-----------|
| `{gen_idx}` | Current loop index (integer, 0-based) |
| `{loop_index}` | The literal string `@loop_index@` (useful when the sed pattern and the marker are the same) |

Example: `"sed 's/@loop_index@/process_apa_index: {gen_idx}/'"` produces, for iteration 5:
```
sed 's/@loop_index@/process_apa_index: 5/' /path/to/template.fcl > detsim_loop/5/detsim_loop.fcl
```

**Without `generator_command`** (Python string replacement):

Every occurrence of `@loop_index@` in the template is replaced with the current loop index. The result is written directly as the iteration FCL. This requires the template to contain the marker:

```fhicl
process_apa_index: @loop_index@
```

`@loop_index@` is chosen as the marker because it is not a valid FHiCL directive (FHiCL uses `@word::` or `@nil`, never `@word@`), so it will not be mis-parsed by FHiCL tools.

#### Output layout for loop stages

Each iteration runs in its own subdirectory and writes its ROOT output there:

```
<stage>/
  <stage>_<pipeline_name>.root        ← final iteration output (stage-level input for next stage)
  <stage>_<pipeline_name>_hist.root
  0/
    <stage>.fcl                       ← generated FCL (kept)
    <stage>_<pipeline_name>.root      ← deleted if delete_intermediate_products: True
  1/
    <stage>.fcl
    <stage>_<pipeline_name>.root      ← deleted after iteration 2 consumes it
  ...
  N-1/
    <stage>.fcl
    (no ROOT here — written to parent)
```

### Full example datacard

```yaml
pipeline_name: "vd_marley_1x8x14"
n_events: 1
n_skip: 0
first_stage: 2
keep_last_hist_file: True
keep_last_art_file: True

stages:
  gen: "prodmarley_nue_cc_flat_dunevd10kt_1x8x14_3view_30deg.fcl"
  g4:  "supernova_g4_dunevd10kt_1x8x14_3view_30deg.fcl"

  detsim_loop:
    template: "detsim_dunevd10kt_1x8x14_3view_30deg_tpc_only_template.fcl"
    n_iter: 112
    delete_intermediate_products: True

  detsim_pds: "detsim_dunevd10kt_1x8x14_3view_30deg_pds_only.fcl"

sequence:
  - "gen"
  - "g4"
  - "detsim_loop"
  - "detsim_pds"
```

## CLI parameter overrides (`-p`)

Any config value can be overridden on the command line without editing the datacard. Keys use dot notation for nested fields. Values are parsed as YAML, so types are inferred automatically.

```bash
# Change event count
lar-piper.py -p n_events=10 pipeline.yaml

# Resume a loop from iteration 36
lar-piper.py -p stages.detsim_loop.skip_iter=36 pipeline.yaml

# Combine multiple overrides with dry-run
lar-piper.py -n -p skip_stages=2 -p stages.detsim_loop.n_iter=5 pipeline.yaml

# Override a boolean
lar-piper.py -p keep_last_art_file=False pipeline.yaml
```

## Output structure

For a pipeline with stages `gen → g4 → detsim_loop → detsim_pds`, run from directory `<run_dir>`:

```
<run_dir>/
  gen/
    gen_<pipeline_name>.root
    gen_<pipeline_name>_hist.root
  g4/
    g4_<pipeline_name>.root
    g4_<pipeline_name>_hist.root
  detsim_loop/
    detsim_loop_<pipeline_name>.root     ← output of last iteration
    detsim_loop_<pipeline_name>_hist.root
    0/  1/  2/  ...                      ← per-iteration subdirs with FCLs
  detsim_pds/
    detsim_pds_<pipeline_name>.root
    detsim_pds_<pipeline_name>_hist.root
```
