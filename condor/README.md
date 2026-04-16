# condor — HTCondor batch submission tools

Two complementary tools for submitting DUNE trigger simulation jobs to an HTCondor cluster at CERN. Both write outputs to EOS, run inside a Fermilab SL7 Singularity container, and use Kerberos credentials for storage access.

| Tool | Submits | Use when |
|------|---------|----------|
| `lar-condor.py` | A single `lar` command per job | Running one simulation stage across many files or event splits |
| `piper-condor.py` | A full `lar-piper.py` pipeline per job | Running the complete gen→g4→detsim→tpg chain as one batch job |

---

## Requirements

```bash
pip install click htcondor pydantic pyyaml rich
```

Valid Kerberos credentials are required (`kinit` before submitting). Both scripts add credentials to the HTCondor credd daemon automatically.

`piper-condor.py` additionally requires `lar-piper.py` on `PATH` (source `dunetrg-pipes/setup_env.sh`).

---

## Directory layout

```
condor/
├── lar-condor.py       # Single-stage HTCondor submission
├── piper-condor.py     # Full-pipeline HTCondor submission
├── run_larsoft_job.sh  # Prototype wrapper (reference only)
├── run_piper_job.sh    # Generic HTCondor executable for piper jobs
├── pyproject.toml
├── run/                # Job cards (YAML) — one file per submission
│   └── eminus_1x8x14_pipeline.yaml
└── examples/
    ├── example.sub
    ├── runme.sh
    └── submit_example.py
```

---

## `lar-condor.py` — single-stage submission

Submits one `lar` invocation per job. Each job calls:

```
<larsoft_runner> -c <config_fcl> [-s <input.root>] -n <N> [--nskip <K>] -o <output.root>
```

### Usage

```bash
# Dry-run (print submit definition, do not submit):
lar-condor.py <card_file.yaml>

# Submit:
lar-condor.py -s <card_file.yaml>
```

### Job card

```yaml
label: 'eminus_vd_g4'
larsoft_runner: '/afs/cern.ch/work/t/thea/dune/<area>/lar_wrap.sh'
config_fcl: '/path/to/supernova_g4_dunevd10kt_1x8x14_3view_30deg.fcl'

n_events: 10000          # total events in each input file
n_jobs_per_file: 100     # splits each file into 100 subjobs of 100 events each

output_file_prefix: 'eminus_vd_g4'   # optional; defaults to label
eos_output_folder: '/eos/home-t/thea/dune_trigger/eminus_vd/g4'

eos_input_files:          # omit for generation jobs (no input)
  - '/eos/home-t/thea/dune_trigger/eminus_vd/gen/<ClusterId>/job_0/eminus_vd_gen_0.root'
```

### Card fields

| Key | Required | Description |
|-----|----------|-------------|
| `label` | yes | Job label; used in output path `<folder>/<label>_<ClusterId>/` |
| `larsoft_runner` | yes | Executable invoked on the compute node (e.g. `lar_wrap.sh`) |
| `config_fcl` | yes | FHiCL configuration file passed to `lar -c` |
| `n_events` | yes (if `n_jobs_per_file > 1`) | Total events per input file; used to compute per-job event counts |
| `n_jobs_per_file` | no (default 1) | Number of subjobs per input file; events are split evenly |
| `output_file_prefix` | no | Base name for output ROOT files (defaults to `label`) |
| `eos_output_folder` | yes | Destination directory on EOS (must be under `/eos`) |
| `eos_input_files` | no | Input ROOT files on EOS; omit for generation stages |

### Output structure

```
<eos_output_folder>/<label>_<ClusterId>/
  job_00/  <output_prefix>_00.root
  job_01/  <output_prefix>_01.root
  ...
```

---

## `piper-condor.py` — full-pipeline submission

Submits one complete `lar-piper.py` pipeline run per job. The pipeline YAML and `lar-piper.py` are transferred to the compute node by HTCondor. The `setup_script` is sourced on the node to load the DUNE software environment. All stage output directories (`gen/`, `g4/`, `detsim/`, …) are transferred to EOS when the job completes.

The job card has two exclusive `source` modes selected by `source.type`.

### Usage

```bash
# Dry-run:
piper-condor.py <card_file.yaml>

# Submit:
piper-condor.py -s <card_file.yaml>
```

### Job card — generator source

For simulation pipelines that start from scratch (no input files). Number of jobs = `n_events // n_events_per_job`. Each job receives `-p n_events=<n_events_per_job>` and `-p first_event={"run":<run_number>,"subrun":<job_index>,"event":1}`.

```yaml
label: 'eminus_1x8x14_pipeline'
pipeline_config: '/home/thea/.../dunetrg-cards/pipelines/vd_single_eminus_1x8x14.yaml'
setup_script:    '/afs/cern.ch/work/t/thea/dune/trigsim_mark09_v10_20_03d00/setup_dunesw.sh'
eos_output_folder: '/eos/home-t/thea/dune_trigger/eminus_1x8x14'

source:
  type: generator
  n_events: 10000        # total events for the campaign
  n_events_per_job: 1000 # n_jobs = n_events // n_events_per_job = 10
  run_number: 1          # ART run; subrun = job index, event = 1
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | yes | Must be `generator` |
| `n_events` | yes | Total events for the campaign |
| `n_events_per_job` | yes | Events each job generates; determines number of jobs |
| `run_number` | yes | ART run number injected via `-e run:subrun:1` |

### Job card — file source

For downstream stages that consume existing EOS ROOT files. Each job receives `-p input_files=<basename>` (file transferred to job CWD by HTCondor).

```yaml
label: 'eminus_1x8x14_detsim'
pipeline_config: '/home/thea/.../dunetrg-cards/pipelines/vd_radiols_1x8x14_detsim_tpg.yaml'
setup_script:    '/afs/cern.ch/work/t/thea/dune/trigsim_mark09_v10_20_03d00/setup_dunesw.sh'
eos_output_folder: '/eos/home-t/thea/dune_trigger/eminus_1x8x14_detsim'

source:
  type: file
  eos_input_files:
    - '/eos/.../job_00/g4_vd_eminus_1x8x14.root'
    - '/eos/.../job_01/g4_vd_eminus_1x8x14.root'
  n_jobs_per_input_file: 1   # set >1 to split events within each file
  # n_events_per_job: 100    # required when n_jobs_per_input_file > 1
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | yes | Must be `file` |
| `eos_input_files` | yes | Input ROOT files on EOS |
| `n_jobs_per_input_file` | no (default 1) | Subjobs per file; implies event splitting when > 1 |
| `n_events_per_job` | yes (if `n_jobs_per_input_file > 1`) | Events per subjob; used to compute `-p n_events` and `-p skip_events` |

### Common top-level fields

| Key | Required | Description |
|-----|----------|-------------|
| `label` | yes | Job label; used in output path `<folder>/<label>_<ClusterId>/` |
| `pipeline_config` | yes | Absolute path to a `lar-piper.py` pipeline YAML datacard |
| `setup_script` | yes | Absolute path to the DUNE software setup script; sourced on the compute node |
| `eos_output_folder` | yes | Destination directory on EOS (must be under `/eos`) |
| `copy_to_eos` | no | List of local paths (relative to job CWD) to copy to EOS after the pipeline completes; directories are copied recursively (`xrdcp -r`), files directly (`xrdcp`) |

Example with copy-back:

```yaml
copy_to_eos:
  - 'logs/'           # directory — copied with xrdcp -r
  - 'debug_dump.root' # file — copied with xrdcp
```

### Output structure

Each job produces the full pipeline directory tree:

```
<eos_output_folder>/<label>_<ClusterId>/
  job_00/
    gen/   gen_<pipeline_name>.root   gen_<pipeline_name>_hist.root
    g4/    g4_<pipeline_name>.root    g4_<pipeline_name>_hist.root
    detsim_pds/ ...
    detsim_tpc/ ...
    tpg/   ...
    anatree/ ...
  job_01/
    ...
```

Output naming within each stage follows the `pipeline_name` field in the pipeline card.

### `run_piper_job.sh`

The HTCondor executable used by `piper-condor.py`. It is found automatically at `condor/run_piper_job.sh` relative to `piper-condor.py`. No path needs to be specified in the job card.

The script sources `$SETUP_SCRIPT` (injected by HTCondor) and then runs `python3 lar-piper.py "$@"`, forwarding all job arguments. `lar-piper.py` is transferred to the job working directory alongside the pipeline YAML.

---

## Differences at a glance

| | `lar-condor.py` | `piper-condor.py` |
|---|---|---|
| Executable on node | user-supplied `larsoft_runner` | `run_piper_job.sh` (built-in) |
| Config | `config_fcl` (single FCL) | `pipeline_config` (pipeline YAML) |
| Setup | embedded in `larsoft_runner` | `setup_script` field |
| Job args | `-c fcl -n N --nskip K -o out.root` | `-p n_events=N -p skip_events=K pipeline.yaml` |
| Output per job | single ROOT file | full stage directory tree |
| Transferred inputs | input ROOT file | pipeline YAML + `lar-piper.py` + optional input ROOT |

---

## Common HTCondor settings (both tools)

| Setting | Value |
|---------|-------|
| Container | Fermilab SL7 (`/cvmfs/unpacked.cern.ch/…/fermilab/fnal-dev-sl7:latest`) |
| Job flavour | `tomorrow` |
| Output storage | EOS via XRootD (`root://eosuser.cern.ch/`) |
| Credentials | Kerberos (added to credd daemon at submit time) |
| Directory creation | `MY.XRDCP_CREATE_DIR: True` |
