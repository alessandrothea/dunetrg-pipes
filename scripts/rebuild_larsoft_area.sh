#!/bin/bash
echo $MRB_TOP
source ${MRB_TOP}/setup_dunesw.sh
mrb uc
mrb z
mrb zd
mrbsetenv
mrb i --generator=ninja 
mrbslp