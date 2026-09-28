#!/usr/bin/env bash

usage() {
    echo "Usage: $(basename "$0") <fhicl_file> <output_file>"
    echo "  fhicl_file   Input FHiCL configuration file"
    echo "  output_file  Output file path for --debug-config dump"
    exit 1
}

[[ $# -ne 2 ]] && usage

FHICL_FILE="$1"
FHICL_OUTPUT="$2"

lar -c "${FHICL_FILE}" --debug-config "${FHICL_OUTPUT}"
