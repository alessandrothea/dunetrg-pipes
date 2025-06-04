#!/bin/bash


echo "Hello Condor"
echo "This is a test file" > test_file.log

echo "=========" >> test_file.log
pwd >>  test_file.log
echo "=========" >> test_file.log
ls -la >> test_file.log
echo "=========" >> test_file.log
uname -a >> test_file.log
echo "=========" >> test_file.log
cat /etc/redhat-release >> test_file.log
echo "=========" >> test_file.log
ls -la /afs/cern.ch/user/t/thea/work/dune/ >> test_file.log


# Load LArSoft Environment
source /afs/cern.ch/work/t/thea/dune/trigsim_mark01/setup_dunesw.sh


echo "=========" >> test_file.log
env >> test_file.log
