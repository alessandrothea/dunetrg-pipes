#!/usr/bin/bash

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <target_dir>" >&2
    exit 1
fi

TARGET_DIR="$1"

if [[ ! -e "${TARGET_DIR}" ]]; then
    echo "Error: path does not exist: ${TARGET_DIR}" >&2
    exit 1
fi

if [[ ! -d "${TARGET_DIR}" ]]; then
    echo "Error: path is not a directory: ${TARGET_DIR}" >&2
    exit 1
fi

find "${TARGET_DIR}" \( -name "piper*.out.txt" -o -name "piper*.err.txt" \) -exec sh -c 'echo "Deleting: $1" && rm "$1"' _ {} \;
