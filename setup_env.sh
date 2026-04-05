HERE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)

export FHICL_FILE_PATH="${HERE}/fcl/vd${FHICL_FILE_PATH:+:${FHICL_FILE_PATH}}"
export FHICL_FILE_PATH="${HERE}/fcl/hd${FHICL_FILE_PATH:+:${FHICL_FILE_PATH}}"
export FHICL_FILE_PATH="${HERE}/fcl/utils${FHICL_FILE_PATH:+:${FHICL_FILE_PATH}}"
export LAR_PIPE_PATH="${HERE}/pipelines${LAR_PIPE_PATH:+:${LAR_PIPE_PATH}}"
export PATH="${HERE}/scripts${PATH:+:${PATH}}"

unset HERE

echo "--- dntrg toolbox setup complete ---"