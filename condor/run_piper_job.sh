#!/bin/bash

# HTCondor batch wrapper for lar-piper.py pipeline jobs.
#
# All positional arguments are forwarded verbatim to lar-piper.py:
#   [-p KEY=VALUE ...] <pipeline_config_basename.yaml>
#
# Required environment variable (set via HTCondor 'environment' key):
#   SETUP_SCRIPT — absolute path to the DUNEsw setup script to source
#                  (e.g. /afs/cern.ch/work/t/thea/dune/<area>/setup_dunesw.sh)
#
# lar-piper.py and the pipeline YAML are transferred to the job working
# directory by HTCondor via transfer_input_files.  All pipeline stage output
# directories (gen/, g4/, detsim/, ...) are created in that working directory
# and transferred to EOS by HTCondor's output_destination setting.

echo "=== piper job ==="
echo "  date:         $(date)"
echo "  host:         $(hostname)"
echo "  workdir:      $(pwd)"
echo "  SETUP_SCRIPT: ${SETUP_SCRIPT}"
echo "  args:         $*"

source "${SETUP_SCRIPT}"

python3 "${LAR_PIPER_SCRIPT}" "$@"

# Optional copy-back to EOS (set via copy_to_eos in the piper-condor job card)
# TODO: move to a separate script?
if [ -n "${XRDCP_SOURCES:-}" ]; then
    echo "=== xrdcp copy-back ==="
    echo "  destination: ${EOS_JOB_OUTPUT}"

    # Create destination directory before copying.
    # EOS_JOB_OUTPUT has the form root://<server>/<path>; strip the URL prefix
    # to get the bare path that xrdfs expects.
    _EOS_SERVER="${EOS_JOB_OUTPUT%%/eos/*}"       # e.g. root://eosuser.cern.ch
    _EOS_PATH="${EOS_JOB_OUTPUT#${_EOS_SERVER}}"  # e.g. /eos/home-t/thea/...
    xrdfs "${_EOS_SERVER}" mkdir -p "${_EOS_PATH}"

    IFS=':' read -ra _SOURCES <<< "${XRDCP_SOURCES}"
    for _src in "${_SOURCES[@]}"; do
        echo "  copying: ${_src}"
        if [ -d "${_src}" ]; then
            xrdcp -r "${_src}" "${EOS_JOB_OUTPUT}"
        elif [ -f "${_src}" ]; then
            xrdcp "${_src}" "${EOS_JOB_OUTPUT}"
        else
            echo "  WARNING: '${_src}' not found, skipping"
        fi
    done
fi
