HERE=$(cd $(dirname $(readlink -f ${BASH_SOURCE})) && pwd)

export FHICL_FILE_PATH="${HERE}/fcl/vd:${FHICL_FILE_PATH}"
export FHICL_FILE_PATH="${HERE}/fcl/vd:${FHICL_FILE_PATH}"
export FHICL_FILE_PATH="${HERE}/fcl/utils:${FHICL_FILE_PATH}"
export PATH="${HERE}/scripts:${PATH}"

unset HERE