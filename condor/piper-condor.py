#!/usr/bin/env python3

from rich import print
import click
import yaml
from pathlib import Path
import os.path

from pydantic import BaseModel, FilePath, DirectoryPath, Field, field_validator, model_validator, ValidationError
from typing import Annotated, List, Literal, Optional, Union
import htcondor2 as htcondor

#------------------------------------------------------------------------------

_SCRIPT_DIR      = Path(__file__).resolve().parent
_RUN_PIPER       = _SCRIPT_DIR / 'run_piper_job.sh'
_LAR_PIPER_SCRIPT = _SCRIPT_DIR.parent / 'scripts' / 'lar-piper.py'

_DEFAULT_JOB_FLAVOUR      = "tomorrow"
_DEFAULT_SINGULARITY_IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/fermilab/fnal-dev-sl7:latest"
_DEFAULT_REQUEST_MEMORY    = "3 GB"

#------------------------------------------------------------------------------

class GeneratorSource(BaseModel):
    """
    Simulation/generation jobs — no input ROOT files.
    Each job runs the full pipeline from scratch, generating n_events_per_job events.
    Total number of jobs: n_events // n_events_per_job.
    """
    type:             Literal['generator']
    n_events:         int   # total events for the campaign
    n_events_per_job: int   # events each job generates
    run_number:       int   # ART run number; subrun = job index, event = 1


class FileSource(BaseModel):
    """
    File-based jobs — pipeline starts from existing EOS ROOT files.
    One or more jobs are created per input file (event splitting with n_jobs_per_input_file > 1).
    """
    type:                  Literal['file']
    eos_input_files:       List[FilePath]
    n_jobs_per_input_file: int           = 1
    n_events_per_job:      Optional[int] = None  # required when n_jobs_per_input_file > 1

    @field_validator('eos_input_files', mode='before')
    def expand_eos_paths(cls, value_list):
        return [Path(os.path.expandvars(v)).expanduser() for v in value_list]

    @field_validator('eos_input_files', mode='after')
    def check_eos_paths(cls, value_list):
        for value in value_list:
            if not value.parts[1] == 'eos':
                raise ValueError(f"'{value}' is not an eos folder")
        return value_list

    @model_validator(mode='after')
    def check_splitting(self):
        if self.n_jobs_per_input_file > 1 and self.n_events_per_job is None:
            raise ValueError('n_events_per_job is required when n_jobs_per_input_file > 1')
        return self


Source = Annotated[Union[GeneratorSource, FileSource], Field(discriminator='type')]

#------------------------------------------------------------------------------

class PiperJobConfig(BaseModel):
    """
    Model for lar-piper.py pipeline jobs submitted via HTCondor.
    """
    label:             str
    pipeline_config:   FilePath      # lar-piper.py pipeline YAML datacard
    setup_script:      FilePath      # DUNEsw setup script (sourced on compute node)
    eos_output_folder: DirectoryPath
    source:            Source
    copy_to_eos:       Optional[List[str]] = None  # local paths (relative to job CWD) to transfer to EOS via transfer_output_files
    job_flavour:       str = _DEFAULT_JOB_FLAVOUR
    singularity_image: str = _DEFAULT_SINGULARITY_IMAGE
    request_memory:    str = _DEFAULT_REQUEST_MEMORY

    @field_validator('pipeline_config', 'setup_script', 'eos_output_folder', mode='before')
    def expand_and_validate_path(cls, value):
        return Path(os.path.expandvars(value)).expanduser()

    @field_validator('eos_output_folder', mode='after')
    def check_eos_path(cls, value):
        if not value.parts[1] == 'eos':
            raise ValueError(f"'{value}' is not an eos folder")
        return value

#------------------------------------------------------------------------------

def to_eos(p):
    return f"root://eosuser.cern.ch/{p}"


def _print_summary(cfg: PiperJobConfig, n_jobs: int, submit: bool) -> None:
    """Print a human-readable job submission summary."""
    src = cfg.source
    if isinstance(src, GeneratorSource):
        src_desc = (
            f'generator  |  {src.n_events} events total'
            f'  |  {src.n_events_per_job} per job'
            f'  |  run {src.run_number}'
        )
    else:
        n_files = len(src.eos_input_files)
        src_desc = f'file  |  {n_files} file(s) × {src.n_jobs_per_input_file} subjob(s)'
        if src.n_events_per_job is not None:
            src_desc += f'  |  {src.n_events_per_job} events/job'

    mode = '[green]SUBMIT[/green]' if submit else '[yellow]DRY RUN[/yellow] (use -s to submit)'

    W = 14  # label column width
    print()
    print('[bold]=== piper-condor ===[/bold]')
    print(f'  {"label:":{W}} {cfg.label}')
    print(f'  {"pipeline:":{W}} {cfg.pipeline_config.name}')
    print(f'  {"setup:":{W}} {cfg.setup_script}')
    print(f'  {"output:":{W}} {cfg.eos_output_folder}')
    print(f'  {"source:":{W}} {src_desc}')
    print(f'  {"job_flavour:":{W}} {cfg.job_flavour}')
    print(f'  {"singularity:":{W}} {cfg.singularity_image}')
    print(f'  {"memory:":{W}} {cfg.request_memory}')
    print(f'  {"jobs:":{W}} {n_jobs}')
    if cfg.copy_to_eos:
        print(f'  {"copy to EOS:":{W}} {", ".join(cfg.copy_to_eos)}')
    print(f'  {"mode:":{W}} {mode}')
    print()


def _expand(template: str, item: dict) -> str:
    """Expand HTCondor itemdata macros in *template*.

    Replaces $(key) with item[key] for each key in *item*.
    $(ClusterId) and $(ProcId) are replaced with the literal strings
    <ClusterId> and <ProcId> since those are only known after submission.
    """
    s = template.replace('$(ClusterId)', '<ClusterId>').replace('$(ProcId)', '<ProcId>')
    for k, v in item.items():
        s = s.replace(f'$({k})', str(v))
    return s


def _print_job_cards(sub: htcondor.Submit, itemdata: list) -> None:
    """Print the submit description template and each job's expanded key fields."""
    print('[bold]--- Submit description ---[/bold]')
    print(str(sub))

    tmpl_dst = sub['output_destination']
    tmpl_env = sub['environment']

    print(f'[bold]--- Per-job expansion ({len(itemdata)} job(s)) ---[/bold]')
    for item in itemdata:
        print(f'  [cyan]job {item["job_index"]}[/cyan]')
        print(f'    arguments:  {item["job_args"]}')
        if 'input_file' in item:
            print(f'    input:      {item["input_file"]}')
        print(f'    output:     {_expand(tmpl_dst, item)}')
        print(f'    env:        {_expand(tmpl_env, item)}')
    print()


@click.command()
@click.argument('card_file', type=click.Path(exists=True, dir_okay=False))
@click.option('-s', '--submit', default=False, is_flag=True,
              help='Actually submit jobs to HTCondor (default: dry-run)')
@click.option('-p', '--print-cards', 'print_cards', default=False, is_flag=True,
              help='Print the HTCondor submit description and per-job itemdata')
def cli(card_file, submit, print_cards):
    with open(card_file, 'r') as stream:
        data_loaded = yaml.safe_load(stream)

    try:
        cfg = PiperJobConfig(**data_loaded)
    except ValidationError as e:
        print('[red]ERROR Validating the job card[/red]')
        print()
        print(e)
        print()
        raise SystemExit(-1)

    print("[CREDD] Adding user credentials to credd daemon")
    try:
        credd = htcondor.Credd()
        credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
        print("[CREDD] OK")
    except Exception as e:
        print(f"[yellow][CREDD] Warning: {e}[/yellow]")
        print("[yellow][CREDD] Proceeding — HTCondor may handle Kerberos delegation automatically.[/yellow]")

    # transfer_input_files: pipeline config always; $(input_file) for file-source jobs.
    # lar-piper.py and run_piper_job.sh live in the toolbox on AFS — no transfer needed.
    transfer_files = str(cfg.pipeline_config)
    if isinstance(cfg.source, FileSource):
        transfer_files += ', $(input_file)'

    sub_dict = {
        'executable':            str(_RUN_PIPER),
        'arguments':             '$(job_args)',
        'error':                 'piper.$(ClusterId).$(ProcId).err.txt',
        'output':                'piper.$(ClusterId).$(ProcId).out.txt',
        'log':                   'piper.$(ClusterId).log',
        'transfer_executable':   'false',
        'should_transfer_files': 'YES',
        'transfer_input_files':  transfer_files,
        'output_destination':    to_eos(cfg.eos_output_folder) + f'/{cfg.label}_$(ClusterId)/job_$(job_index)/',
        'environment':           f'"SETUP_SCRIPT={cfg.setup_script} LAR_PIPER_SCRIPT={_LAR_PIPER_SCRIPT}"',
        '+JobFlavour':           f'"{cfg.job_flavour}"',
        'MY.SendCredential':     'True',
        'MY.XRDCP_CREATE_DIR':   'True',
        'MY.SingularityImage':   f'"{cfg.singularity_image}"',
        'request_memory':        cfg.request_memory,
    }
    if cfg.copy_to_eos:
        sub_dict['transfer_output_files'] = ', '.join(cfg.copy_to_eos)
    sub = htcondor.Submit(sub_dict)

    # Build per-job itemdata
    itemdata = []
    src = cfg.source

    if isinstance(src, GeneratorSource):
        n_jobs     = src.n_events // src.n_events_per_job
        digits_job = len(str(n_jobs - 1))

        for j in range(n_jobs):
            job_index = f'{j:0{digits_job}}'

            overrides = [
                f'-p n_events={src.n_events_per_job}',
                f'-p first_event.run={src.run_number}',
                f'-p first_event.subrun={j}',
                f'-p first_event.event=1',
            ]

            itemdata.append({
                'job_index': job_index,
                'job_args':  ' '.join(overrides) + f' {cfg.pipeline_config.name}',
            })

    else:  # FileSource
        n_total    = src.n_jobs_per_input_file * len(src.eos_input_files)
        digits_job  = len(str(n_total - 1))

        for i, f in enumerate(src.eos_input_files):
            for k in range(src.n_jobs_per_input_file):
                job_idx_int = i * src.n_jobs_per_input_file + k
                job_index   = f'{job_idx_int:0{digits_job}}'

                overrides = []
                if src.n_events_per_job is not None:
                    overrides.append(f'-p n_events={src.n_events_per_job}')
                    if src.n_jobs_per_input_file > 1:
                        overrides.append(f'-p skip_events={k * src.n_events_per_job}')
                overrides.append(f'-p input_files={f.name}')

                itemdata.append({
                    'job_index':  job_index,
                    'job_args':   ' '.join(overrides) + f' {cfg.pipeline_config.name}',
                    'input_file': to_eos(f),
                })

    _print_summary(cfg, len(itemdata), submit)

    if print_cards:
        _print_job_cards(sub, itemdata)

    if submit:
        schedd = htcondor.Schedd()
        res = schedd.submit(sub, itemdata=iter(itemdata))
        cluster_id = res.cluster()
        print(f"Job cluster: {cluster_id}")


if __name__ == '__main__':
    cli()
