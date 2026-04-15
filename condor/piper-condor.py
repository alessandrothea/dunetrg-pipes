#!/usr/bin/env python3

from rich import print
import click
import shutil
import yaml
from pathlib import Path
import os.path

from pydantic import BaseModel, FilePath, DirectoryPath, field_validator, model_validator, ValidationError
from typing import Optional, List
import htcondor2 as htcondor

#------------------------------------------------------------------------------

_SCRIPT_DIR   = Path(__file__).resolve().parent
_RUN_PIPER    = _SCRIPT_DIR / 'run_piper_job.sh'

#------------------------------------------------------------------------------

class PiperJobConfig(BaseModel):
    """
    Model for lar-piper.py pipeline jobs submitted via HTCondor.
    """
    label:             str
    pipeline_config:   FilePath       # lar-piper.py pipeline YAML datacard
    setup_script:      FilePath       # DUNEsw setup script (sourced on compute node)

    n_events:          Optional[int] = -1
    n_jobs_per_file:   Optional[int] = 1
    eos_output_folder: DirectoryPath
    eos_input_files:   Optional[List[FilePath]] = None


    @field_validator('pipeline_config', 'setup_script', 'eos_output_folder', mode='before')
    def expand_and_validate_path(cls, value):
        p = Path(os.path.expandvars(value)).expanduser()
        return p

    @field_validator('eos_output_folder', mode='after')
    def check_eos_path(cls, value):
        if not value.parts[1] == 'eos':
            raise ValueError(f"'{value}' is not an eos folder")
        return value

    @field_validator('eos_input_files', mode='after')
    def check_eos_path_list(cls, value_list):
        path_list = []
        for value in value_list:
            if not value.parts[1] == 'eos':
                raise ValueError(f"'{value}' is not an eos folder")
            path_list.append(value)
        return path_list

    @model_validator(mode='after')
    def check_events_and_jobs(self):
        if self.n_jobs_per_file != 1 and self.n_events == -1:
            raise ValueError('Multiple jobs per file require the number of events per file to be defined')
        return self


#------------------------------------------------------------------------------

def to_eos(p):
    return f"root://eosuser.cern.ch/{p}"


@click.command()
@click.argument('card_file', type=click.Path(exists=True, dir_okay=False))
@click.option('-s', '--submit', default=False, is_flag=True,
              help='Actually submit jobs to HTCondor (default: dry-run)')
def cli(card_file, submit):
    with open(card_file, 'r') as stream:
        data_loaded = yaml.safe_load(stream)

    cfg = None
    try:
        cfg = PiperJobConfig(**data_loaded)
    except ValidationError as e:
        print('[red]ERROR Validating the job card[/red]')
        print()
        print(e)
        print()
        raise SystemExit(-1)

    # Locate lar-piper.py on PATH (put there by setup_env.sh)
    lar_piper_path = shutil.which('lar-piper.py')
    if lar_piper_path is None:
        print('[red]ERROR[/red] lar-piper.py not found on PATH.')
        print('Source dunetrg-pipes/setup_env.sh before running piper_condor.py.')
        raise SystemExit(-1)

    job_files = [(0, None)] if cfg.eos_input_files is None \
                else [(i, f) for i, f in enumerate(cfg.eos_input_files)]

    print("[CREDD] Adding user credentials to credd daemon")
    try:
        credd = htcondor.Credd()
        credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
        print("[CREDD] OK")
    except Exception as e:
        print(f"[yellow][CREDD] Warning: {e}[/yellow]")
        print("[yellow][CREDD] Proceeding — HTCondor may handle Kerberos delegation automatically.[/yellow]")

    # transfer_input_files: always include pipeline config + lar-piper.py;
    # append $(input_file) per-job when EOS input files are provided.
    transfer_files = f'{cfg.pipeline_config}, {lar_piper_path}'

    sub = htcondor.Submit({
        'executable':            str(_RUN_PIPER),
        'arguments':             '$(job_args)',
        'error':                 'piper.$(ClusterId).$(ProcId).err',
        'output':                'piper.$(ClusterId).$(ProcId).out',
        'log':                   'piper.$(ClusterId).log',
        'transfer_executable':   'false',
        'should_transfer_files': 'YES',
        'transfer_input_files':  transfer_files,
        'output_destination':    to_eos(cfg.eos_output_folder) + f'/{cfg.label}_$(ClusterId)/job_$(job_index)/',
        'environment':           f'"SETUP_SCRIPT={cfg.setup_script}"',
        '+JobFlavour':           '"tomorrow"',
        'MY.SendCredential':     'True',
        'MY.XRDCP_CREATE_DIR':   'True',
        'MY.SingularityImage':   '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/fermilab/fnal-dev-sl7:latest"',
    })

    if cfg.eos_input_files is not None:
        sub['transfer_input_files'] += ', $(input_file)'

    # Build per-job itemdata
    itemdata = []

    n_events_per_job = cfg.n_events // cfg.n_jobs_per_file

    digits_file = len(str(len(job_files) - 1))
    digits_job  = len(str((cfg.n_jobs_per_file * len(job_files)) - 1))

    for i, f in job_files:
        for k in range(cfg.n_jobs_per_file):

            print(f"[cyan]Creating job: file {i} subjob {k}[/cyan]")

            file_index = '{num:0{width}}'.format(num=i, width=digits_file)
            job_index  = '{num:0{width}}'.format(num=i * cfg.n_jobs_per_file + k, width=digits_job)

            # Build -p KEY=VALUE override string for lar-piper.py
            overrides = [f'-p n_events={n_events_per_job}']

            if cfg.n_jobs_per_file != 1:
                overrides.append(f'-p skip_events={k * n_events_per_job}')

            if f is not None:
                # Transfer delivers the file to job CWD; pass basename only
                overrides.append(f'-p input_files={f.name}')

            # Pipeline YAML is positional arg (last); it arrives in job CWD via transfer
            job_args = ' '.join(overrides) + f' {cfg.pipeline_config.name}'

            item = {
                'job_index': job_index,
                'job_args':  job_args,
            }

            if f is not None:
                item['input_file'] = to_eos(f)

            itemdata.append(item)

    # Print cluster definition
    print(sub)
    print(itemdata)

    if submit:
        schedd = htcondor.Schedd()
        res = schedd.submit(sub, itemdata=iter(itemdata))
        cluster_id = res.cluster()
        print(f"Job cluster: {cluster_id}")


if __name__ == '__main__':
    cli()
