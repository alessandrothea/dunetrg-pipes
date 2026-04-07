# dunetrg-devtools

Developer tools for DUNE trigger simulation workflows. The package covers three areas:

- **Environment setup** — scripted creation of LArSoft/DUNE software development areas
- **Pipeline execution** — YAML/JSON-driven orchestration of multi-stage `lar` jobs with rich terminal output
- **Batch submission** — HTCondor job submission for large-scale simulation campaigns

---

## Repository layout

```
dunetrg-devtools/
├── setup_env.sh          # Source to add scripts/ to PATH
├── scripts/
│   ├── lar-piper.py      # Pipeline runner (rich terminal output)
│   ├── README-lar-pipe.md
│   ├── find-fhicl.sh     # Locate FCL files in FHICL_FILE_PATH
│   └── create_larsoft_area.sh
└── condor/
    ├── lar_condor.py     # HTCondor submission tool
    ├── pyproject.toml
    ├── run_larsoft_job.sh
    └── examples/
        ├── example.sub
        ├── runme.sh
        └── submit_example.py
```

---

## Quick start

Source `setup_env.sh` from the repository root to put the scripts on your `PATH`:

```bash
source setup_env.sh
```

---

## `create_larsoft_area.sh` — Development environment setup

Creates a complete DUNE software development area from scratch, including an MRB workspace, a Python virtual environment with common dependencies, and convenience wrapper scripts.

### Requirements

- CVMFS mounted (`/cvmfs/dune.opensciencegrid.org` reachable)
- `mrb` available (provided by the DUNE UPS stack)

### Usage

```bash
create_larsoft_area.sh -v <version> [-q <qualifiers>] <target_dir>
```

| Argument | Description |
|----------|-------------|
| `-v, --version VERSION` | DUNE software version, e.g. `v1_2_3` |
| `-q, --qualifiers QUALS` | UPS qualifiers (default: `e26:prof`) |
| `target_dir` | Directory to create (must not already exist) |

### What it does

1. Sources the DUNE setup scripts from CVMFS
2. Creates the target directory and initialises an MRB workspace inside it
3. Checks out the `dunetrigger` repository via MRB
4. Builds with `ninja`
5. Creates a Python virtual environment (`.venv`) and installs `pyyaml` and `rich`
6. Writes three wrapper scripts into the area root:
   - `setup_dunesw.sh` — sets all environment variables and activates the venv
   - `lar_wrap.sh` — thin wrapper around `lar`
   - `dunesw_wrap.sh` — generic wrapper for other DUNE executables

After creation, initialise any new shell session with:

```bash
source <target_dir>/setup_dunesw.sh
```

---

## `lar-piper.py` — Pipeline runner

Reads a YAML or JSON datacard describing a sequence of `lar` stages, wires their inputs and outputs together automatically, and runs them in order. Produces coloured terminal output via `rich` (plain text fallback if `rich` is not installed).

See [lar-piper.md](lar-piper.md) for the full reference including the datacard format, loop stage FCL generation, and output layout.

### Requirements

| Dependency | Notes |
|------------|-------|
| Python 3.7+ | |
| `rich` | `pip install rich` — optional, enables coloured output |
| `pyyaml` | `pip install pyyaml` — required for YAML datacards |
| `lar` on `PATH` | Provided by the DUNE software stack |
| `LAR_PIPE_PATH` | Colon-separated list of directories searched for datacard files (optional) |
| `FHICL_FILE_PATH` | Required when using loop stages with FCL templates |

### Usage

```bash
lar-piper.py [-n] [-s] [-p KEY=VALUE ...] <config.(yaml|json)>
```

| Flag | Description |
|------|-------------|
| `-n, --dry-run` | Print all commands without executing |
| `-s, --summary` | Print the pipeline summary table and exit |
| `-p KEY=VALUE` | Override any config key (dot notation, repeatable) |

### Minimal datacard

```yaml
pipeline_name: "my_pipeline"
n_events: 10

stages:
  gen: "prodmarley_nue_cc_flat_dunevd10kt_1x8x14_3view_30deg.fcl"
  g4:  "supernova_g4_dunevd10kt_1x8x14_3view_30deg.fcl"

sequence:
  - gen
  - g4
```

### Key datacard fields

| Key | Type | Description |
|-----|------|-------------|
| `pipeline_name` | string | Suffix used in all output file names |
| `n_events` | int | Events for the first stage (`-n`) |
| `skip_events` | int | Events to skip in the first stage (`--n-skip`), only when `input_files` is set |
| `first_stage` | int | 0-based index of the first stage to execute (earlier stages are skipped, default `0`) |
| `last_stage` | int | 0-based index of the last stage to execute (default last) |
| `keep_last_art_file` | bool | Write the art ROOT file for the last stage (default `True`) |
| `keep_last_hist_file` | bool | Write the histogram ROOT file for the last stage (default `True`) |
| `input_files` | string or list | External input files for the first stage; omit for generation pipelines |
| `stages` | mapping | Stage definitions (see below) |
| `sequence` | list | Ordered list of stage names to run |

### Stage types

**Simple stage** — a plain FCL file path:

```yaml
stages:
  reco: "reco_dunevd10kt.fcl"
```

**Loop stage** — runs `lar` `n_iter` times, each with a freshly generated FCL:

```yaml
stages:
  detsim_loop:
    template: "detsim_template.fcl"
    n_iter: 112
    skip_iter: 0                        # resume from iteration N (optional)
    generator_command: "sed 's/@loop_index@/apa_index: {gen_idx}/'"  # optional
    delete_intermediate_products: True  # discard each iteration's ROOT after use
```

Without `generator_command`, every `@loop_index@` token in the template is replaced with the current iteration number by Python string substitution.

### Overriding config values at the command line

```bash
# Change event count
lar-piper.py -p n_events=10 pipeline.yaml

# Resume a loop from iteration 36
lar-piper.py -p stages.detsim_loop.skip_iter=36 pipeline.yaml

# Preview a partial pipeline run
lar-piper.py -n -p first_stage=2 -p last_stage=3 pipeline.yaml
```

---

## `find-fhicl.sh` — FCL file locator

Searches `FHICL_FILE_PATH` (and the local `srcs/` directory if present) for a named FCL file.

```bash
find-fhicl.sh <filename.fcl>
```

Useful for quickly locating which version of a template FCL is active in the current environment.

---

## `condor/lar_condor.py` — HTCondor submission

Submits LArSoft jobs to an HTCondor cluster, reading job parameters from a YAML configuration card. Designed for use at CERN with EOS storage.

### Requirements

```bash
pip install click htcondor pydantic pyyaml rich
```

Valid Kerberos credentials are required for EOS access and job submission.

### Usage

```bash
lar_condor.py [-s] <card_file.yaml>
```

| Flag | Description |
|------|-------------|
| `-s, --submit` | Actually submit jobs (default is dry-run) |

### Job card format

```yaml
label: "mu_minus_detsim"
larsoft_runner: "/path/to/run_larsoft_job.sh"
config_fcl: "detsim_dunevd10kt.fcl"
n_eventsents: 1000
n_jobs_per_file: 10
output_file_prefix: "detsim_output"
eos_output_folder: "/eos/home-u/user/dune/output"
eos_input_files:
  - "/eos/home-u/user/dune/g4/g4_output_0.root"
  - "/eos/home-u/user/dune/g4/g4_output_1.root"
```

| Key | Required | Description |
|-----|----------|-------------|
| `label` | yes | Job label used in output paths |
| `larsoft_runner` | yes | Executable script for the batch job |
| `config_fcl` | yes | FCL configuration passed to `lar` |
| `n_eventsents` | yes (multi-job) | Total events per input file |
| `n_jobs_per_file` | yes | Number of jobs spawned per input file |
| `output_file_prefix` | yes | Base name for output ROOT files |
| `eos_output_folder` | yes | Destination directory on EOS (must be under `/eos`) |
| `eos_input_files` | no | Input ROOT files on EOS; omit for generation jobs |

Jobs run inside a Fermilab SL7 Singularity container. Each job automatically receives the correct input file and event-skip offset derived from its `n_jobs_per_file` split. Outputs land in `<eos_output_folder>/<ClusterId>/<ProcId>/`.

---

## Environment variables summary

| Variable | Used by | Description |
|----------|---------|-------------|
| `LAR_PIPE_PATH` | `lar-piper.py` | Colon-separated directories searched for pipeline datacards |
| `FHICL_FILE_PATH` | `lar-piper.py`, `find-fhicl.sh` | Colon-separated directories searched for FCL files |
| `DUNESW_VERSION` | `create_larsoft_area.sh` | DUNE software version (persisted in `setup_dunesw.sh`) |
| `DUNESW_QUALS` | `create_larsoft_area.sh` | UPS qualifiers (persisted in `setup_dunesw.sh`) |
