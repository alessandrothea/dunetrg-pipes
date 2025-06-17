#!/usr/bin/env python

from rich import print
import click

import yaml
from pathlib import Path
import os.path

from pydantic import BaseModel, FilePath, DirectoryPath, field_validator
from typing import Optional, List
import htcondor

#------------------------------------------------------------------------------

class JobConfig(BaseModel):
    """
    Modle for 
    """
    larsoft_runner: FilePath
    # work_area: DirectoryPath
    config_fcl: FilePath

    n_events: int
    output_file_prefix: Optional[str]
    eos_output_folder : DirectoryPath
    eos_input_files: Optional[List[FilePath]] = None


    # @field_validator('larsoft_runner', 'config_fcl', 'work_area', 'eos_output_folder', mode='before')
    @field_validator('larsoft_runner', 'config_fcl', 'eos_output_folder', mode='before')
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
                raise ValueError(f"'{p}' is not an eos folder")
            path_list.append(value)
        return path_list

#------------------------------------------------------------------------------

def to_eos(p):
    return  f"root://eosuser.cern.ch/{p}"

@click.command()
@click.argument('card_file',default="CMakeLists.txt", type=click.Path(exists=True, dir_okay=False))
@click.option('-s', '--submit',default=False, is_flag=True)
def cli(card_file, submit):
    with open(card_file, 'r') as stream:
        data_loaded = yaml.safe_load(stream)

    cfg = JobConfig(**data_loaded)

    # args = [cfg.work_area, '-c', cfg.config_fcl, ]
    args = ['-c', cfg.config_fcl, ]
    if not cfg.n_events is None:
        args += ['-n', cfg.n_events]

    job_files = {}
    if cfg.eos_input_files is None:
        job_files = [(0, None)]
    else:
        job_files = [(i, f) for i,f in enumerate(cfg.eos_input_files)]


    print ("[CREDD] Adding user credentials to credd daemon")
    # col = htcondor.Collector()
    credd = htcondor.Credd()
    credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
    
    
    sub = htcondor.Submit({
        'executable': str(cfg.larsoft_runner),
        'arguments' : "$(job_args)",
        'error'     : 'larsoft.$(ClusterId).$(ProcId).err',
        'output'    : 'larsoft.$(ClusterId).$(ProcId).out',
        'log'       : 'larsoft.$(ClusterId).log',
        'transfer_executable' : 'false',
        'should_transfer_files': 'YES',
        'output_destination':  to_eos(cfg.eos_output_folder) + '/$(ClusterId)/job_$(ProcId)/',
        # '+JobFlavour': '"longlunch"'.
        'MY.SendCredential': 'True',
        'MY.XRDCP_CREATE_DIR':  'True',
        'MY.SingularityImage':  '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/fermilab/fnal-dev-sl7:latest"',
    })

    if not cfg.eos_input_files is None:
        sub['transfer_input_files'] = '$(input_file)'

    print(sub)

    itemdata = []
    for i,f in job_files:

        print(f"[red]Creating job {i}[/red]")
        job_args = args + (['-s', f.name] if not f is None else [])

        if not cfg.output_file_prefix is None:
            job_args += ['-o', f'{cfg.output_file_prefix}_{i}.root']   

        
        item_extra = {
                'job_args': ' '.join([ str(a) for a in job_args])
            }

        if not f is None:
            item_extra['input_file'] = to_eos(f)

        itemdata.append(item_extra)

        # sub = htcondor.Submit({
        #     'executable': str(cfg.larsoft_runner),
        #     'arguments' : ' '.join([ str(a) for a in job_args]),
        #     'error'     : 'larsoft.$(ClusterId).$(ProcId).err',
        #     'output'    : 'larsoft.$(ClusterId).$(ProcId).out',
        #     'log'       : 'larsoft.$(ClusterId).log',
        #     'should_transfer_files': 'YES',
        #     'output_destination':  to_eos(cfg.eos_output_folder) + '/$(ClusterId)/',
        #     # '+JobFlavour': '"longlunch"'.
        #     'MY.SendCredential': 'True',
        #     'MY.XRDCP_CREATE_DIR':  'True',
        #     'MY.SingularityImage':  '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/fermilab/fnal-dev-sl7:latest"',
        
        # })
    print(itemdata)

    if submit:
        schedd = htcondor.Schedd()
    
        res = schedd.submit(sub, itemdata = iter(itemdata))
        cluster_id = res.cluster()

        print(f"Job cluster: {cluster_id}")

if __name__ == '__main__':
    cli()