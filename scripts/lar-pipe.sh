#!/bin/bash
set -euo pipefail
# set -x
# --- Check arguments ---

usage() {
    cat <<'USAGE'
Usage: lar-pipe.sh [options] <config.yaml>

Options:
  -n, --dry-run   Do not execute anything; only print what would be done
  -h, --help      Show this help
USAGE
}

# ----------------------------
# Parse CLI with getopt
# ----------------------------
TEMP=$(getopt -o nh --long dry-run,help -n 'cfg_parser.sh' -- "$@")
if [[ $? -ne 0 ]]; then
    usage
    exit 1
fi
eval set -- "$TEMP"

DRY_RUN=0

while true; do
    case "$1" in
        -n|--dry-run)
            DRY_RUN=1; shift ;;
        -h|--help)
            usage; exit 0 ;;
        --)
            shift; break ;;
        *)
            echo "Internal error: $1"; exit 1 ;;
    esac
done

if [[ $# -lt 1 ]]; then
    echo "Error: missing <config.yaml>"
    echo
    usage
    exit 1
fi

config="$1"

if [[ ! -f "$config" ]]; then
    echo "Error: file '$config' not found!"
    exit 1
fi

# --- Check yq availability ---
if ! command -v yq &>/dev/null; then
    echo "Error: 'yq' command not found."
    echo
    echo "You can install it locally with:"
    echo "  mkdir -p ~/.local/bin"
    echo "  wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O ~/.local/bin/yq"
    echo "  chmod +x ~/.local/bin/yq"
    echo
    echo "Then add ~/.local/bin to your PATH if not already:"
    echo '  export PATH="$HOME/.local/bin:$PATH"'
    exit 1
fi

# --- Parse standalone variables (with defaults if missing) ---
pipeline_name=$(yq -r '.pipeline_name // ""' "$config")
lar_area=$(yq -r '.lar_area // ""' "$config")
n_ev=$(yq -r '.n_ev // 0' "$config")
n_skip=$(yq -r '.n_skip // 0' "$config")
skip_stages=$(yq -r '.skip_stages // 0' "$config")

# --- input_files as optional array, robust across yq variants ---
declare -a input_files=()
# Get the type of .input_files (mikefarah: "!!seq"/"!!str"/"null"; jq-yq: "array"/"string"/"null")
input_type=$(yq -r '.input_files | (type // "null")' "$config" 2>/dev/null || echo "null")

case "$input_type" in
  "!!seq"|"array")
      mapfile -t input_files < <(yq -r '.input_files[]' "$config")
      ;;
  "!!str"|"string")
      val=$(yq -r '.input_files' "$config")
      [[ "$val" != "null" && -n "$val" ]] && input_files=("$val")
      ;;
  "null"|*)
      # leave as empty array
      ;;
esac

echo "Standalone variables:"
echo "  pipeline_name = $pipeline_name"
echo "  lar_area      = $lar_area"
if ((${#input_files[@]} > 0)); then
    echo "  input_files   = ${input_files[*]}"
else
    echo "  input_files   = (none)"
fi
echo "  n_ev          = $n_ev"
echo "  n_skip        = $n_skip"
echo "  skip_stages   = $skip_stages"
echo

# --- Stages into associative array (robust tab separation) ---
declare -A stages
declare -a stage_keys=()

while IFS=$'\t' read -r key val || [[ -n "$key" ]]; do
  # strip possible Windows CRs (just in case)
  key=${key%$'\r'}
  val=${val%$'\r'}

  stages["$key"]="$val"
  stage_keys+=("$key")
done < <(yq -r '.stages | to_entries[] | [.key, .value] | @tsv' "$config")

echo "Stages map:"
for key in "${stage_keys[@]}"; do
    echo "  $key -> ${stages[$key]}"
done
echo

# --- Sequence into indexed array ---
mapfile -t sequence < <(yq -r '.sequence[]' "$config")

echo "Sequence (${#sequence[@]} steps):"
for s in "${sequence[@]}"; do
    echo "  -> $s"
done

### Execute pipeline

base_dir=$(pwd)
for i in "${!sequence[@]}"; do
    echo -e "\n"

    cd ${base_dir}
    stage_key=${sequence[$i]}
    echo ">>> Stage: '$stage_key' <<< "

    if [[ $i -eq 0 ]]; then
        k=${n_ev}
        if ((${#input_files[@]} == 0)); then
            src_file_opt=''
            n_skip_opt=''
        else
            # Expand into "-s file1 -s file2 ..."
            src_args=()
            for f in "${input_files[@]}"; do
                echo Adding "$f"
                src_args+=("-s" "$f")
            done
            src_file_opt="${src_args[@]}"
            n_skip_opt="--n-skip ${n_skip}"
        fi
    else
        k='-1'
        src_file_opt="-s ${out_file}"
        n_skip_opt=''
    fi
    
    cfg_file=${stages[$stage_key]}
    out_dir=${base_dir}/${stage_key} 
    out_file=${out_dir}/${stage_key}_${pipeline_name}.root

    if [[ $i -lt $skip_stages ]]; then
        echo "   <skipping>"
        continue
    fi

    mkdir -p ${out_dir}    
    cd ${out_dir}

    cmd_line="lar -c ${cfg_file} ${src_file_opt} -o ${out_file} -n ${k} ${n_skip_opt}"
    echo -e "Command '${cmd_line}'"

    if ((${DRY_RUN} == 1)); then
        continue
    fi

    echo "Executing command!"

    ${cmd_line}

done