#!/usr/bin/env python
from rich import print
import htcondor

col = htcondor.Collector()
credd = htcondor.Credd()
credd.add_user_cred(htcondor.CredTypes.Kerberos, None)


mydict = {
    'executable'            : 'runme.sh',
    'arguments'             : '-s one o two',
    'log'                   : 'singularity.$(ClusterId).log',
    'error'                 : 'singularity.$(ClusterId).$(ProcId).err',
    'output'                : 'singularity.$(ClusterId).$(ProcId).out',
    'should_transfer_files' : 'YES',
    # 'MY.JobFlavour'         : '"longlunch"',
    'transfer_input_files'  : 'root://eosuser.cern.ch//eos/home-t/thea/dune_trigger/eminus_snbbkg_hd/eminus_snbbkg_detsim_09.root ',
    'output_destination'    : 'root://eosuser.cern.ch//eos/home-t/thea/dune_trigger/test/$(ClusterId)/',
    'MY.XRDCP_CREATE_DIR'   : 'True',
    'MY.SingularityImage'   : '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/fermilab/fnal-dev-sl7:latest"',
    '+SendCredential': 'True',


}

schedd = htcondor.Schedd()
sub = htcondor.Submit(mydict)
print(sub)
# print(os.environ)
res = schedd.submit(sub)
print(res)