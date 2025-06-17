#!/bin/bash


echo "Hello Condor"
echo "This is a test script" > test_file.log

echo "=== Arguments ===" >> test_file.log
echo $@ >> test_file.log

echo "=== Current directory ===" >> test_file.log
pwd >>  test_file.log
echo "=== Content ===" >> test_file.log
ls -la >> test_file.log
echo "=== Arch ====" >> test_file.log
uname -a >> test_file.log
echo "==== Distribution =====" >> test_file.log
cat /etc/redhat-release >> test_file.log
echo "=== Afs access ======" >> test_file.log
ls -la /afs/cern.ch/user/t/thea/work/dune/ >> test_file.log

echo "=== Larsoft loading ======" >> test_file.log
# Load LArSoft Environment
source /afs/cern.ch/work/t/thea/dune/trigsim_mark01/setup_dunesw.sh


echo "=== Creating a ramdom data file ===="
dd if=/dev/zero of=data_file_200M.dat bs=1 count=0 seek=200M


echo "=========" >> test_file.log
env >> test_file.log
