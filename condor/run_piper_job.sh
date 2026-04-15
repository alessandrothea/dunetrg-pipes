#!/bin/bash
set -euo pipefail

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

set -x
source "${SETUP_SCRIPT}"

python3 "${LAR_PIPER_SCRIPT}" "$@"
