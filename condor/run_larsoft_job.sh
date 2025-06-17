

DUNESW_PATH="/afs/cern.ch/work/t/thea/dune/trigsim_mark01"

NEVENTS=1000
JOB_SIZE=100
CONFIG_FILE="detsim_dunevd10kt_1x8x6_3view_30deg_notpcsigproc.fcl"
FILE_LIST="eminus_files.cfl"
JOB_ID=3
OUTPUT_FILE_BASE=aaaa


#---- script starts here ----

OUTPUT_FILE="${OUTPUT_FILE_BASE}_${JOB_ID}.root"

source ${DUNESW_PATH}/setup_dunesw.sh

lar -c ${CONFIG_FILE} -n ${JOB_SIZE} -nskip ${JOB_SIZE}*${JOB_ID} -S ${FILE_LIST} -o ${OUTPUT_FILE_BASE}_${JOB_ID}.root