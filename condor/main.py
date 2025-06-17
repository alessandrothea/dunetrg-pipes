#!/usr/bin/env python

import htcondor
from rich import print
import click

@click.command()
def main():
    print("[red]Hello from condor![/red]")

    col = htcondor.Collector()
    credd = htcondor.Credd()
    credd.add_user_cred(htcondor.CredTypes.Kerberos, None)

    script_path = "path_to_the_run_script"
    job_dir = "input_or_output?"
    job_name = "a_base_name"
    executable = "my_runme_script.sh"
    common_arguments = "" # include LArsoft area for the setup script and the path to the fhicl file?
    output_destination = "somewhere_on_eos" # How do I specify? syntax: root://eosuser.cern.ch//eos/home-t/thea/
    transfer_output_files = [""] # Comma separated, list of files in local directory
    transfer_input_files = [""] # Comma separated path list, syntax: root://eosuser.cern.ch//eos/home-t/thea/...

    # sub = htcondor.Submit()
    # sub['Executable'] = executable
    # sub['Error'] = f"${job_dir}/error/${job_name}-$(ClusterId).$(ProcId).err"
    # sub['Output'] = f"${job_dir}/output/${job_name}-$(ClusterId).$(ProcId).out"
    # sub['Log'] = f"${job_dir}/log/${job_name}-$(ClusterId).log"
    # sub['MY.SendCredential'] = True
    # sub['+JobFlavour'] = '"tomorrow"'
    # sub['request_cpus'] = '1'
    
    # files=[{'Arguments':f} for f in os.listdir('.') if os.path.isfile(f)]
    # with schedd.transaction() as txn: 
    #     sub.queue_with_itemdata(txn,1,iter(files))

    # schedd = htcondor.Schedd()
    # res = schedd.submit(sub)
    # cluster_id = res.cluster()

    # print(cluster_id)


if __name__ == "__main__":
    main()
