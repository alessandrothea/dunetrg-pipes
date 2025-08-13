#!/bin/bash

# set -euo pipefail

# Help message function
usage() {
    cat <<EOF
Usage: $0 [OPTIONS] <directory>

Create a new environment directory for building software.

Required arguments:
  <directory>            Path to the directory to create (must not already exist)

Options:
  -v, --version VERSION      Software version (e.g., v1_2_3)
  -q, --qualifiers QUALS     Qualifiers string (e.g., e26:prof)
  -h, --help                 Show this help message and exit

Example:
  $0 --version v1_2_3 --qualifiers e20:prof my_build_dir
EOF
    exit 1
}

# Parse options using getopt
TEMP=$(getopt -o v:q:h --long version:,qualifiers:,help -n "$0" -- "$@")
if [ $? != 0 ]; then usage; fi
eval set -- "$TEMP"

# Initialize variables
VERSION=""
QUALIFIERS="e26:prof"

# Extract options
while true; do
    case "$1" in
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -q|--qualifiers)
            QUALIFIERS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error!"
            exit 1
            ;;
    esac
done

# Check for remaining positional argument
if [ $# -ne 1 ]; then
    echo "Error: missing target directory."
    usage
fi

TARGET_DIR="$1"

# Validate inputs
if [[ -z "$VERSION" || -z "$QUALIFIERS" ]]; then
    echo "Error: --version and --qualifiers are required."
    usage
    exit 1
fi

# Directory handling
if [[ -e "$TARGET_DIR" ]]; then
    echo "Error: directory '$TARGET_DIR' already exists."
    exit 1
else
    echo "Creating directory: $TARGET_DIR"
    mkdir -p "$TARGET_DIR"
fi

# Final output
echo "Software version: $VERSION"
echo "Qualifiers: $QUALIFIERS"
echo "Environment setup complete in: $TARGET_DIR"

#-------------------------------------------


export UPS_OVERRIDE="-H Linux64bit+3.10-2.17"
source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh

mkdir -p ${TARGET_DIR}
cd ${TARGET_DIR}


export DUNESW_VERSION=${VERSION}
export DUNESW_QUALS=${QUALIFIERS}
setup dunesw ${DUNESW_VERSION} -q ${DUNESW_QUALS}

mrb newDev -v ${DUNESW_VERSION} -q ${DUNESW_QUALS}


cat > setup_dunesw.sh << EOF
#!/bin/bash

HERE=\$(cd \$(dirname \$(readlink -f \${BASH_SOURCE})) && pwd)

export DUNESW_VERSION=${DUNESW_VERSION}  # Version of the software to be used
export DUNESW_QUALS=${DUNESW_QUALS}  # Qualifiers for the software packages
export UPS_OVERRIDE="-H Linux64bit+3.10-2.17"

# Source the setup script for the DUNE software
source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh

# Setup the specific version of the DUNE software
setup dunesw ${DUNESW_VERSION} -q ${DUNESW_QUALS}

# Source the setup script for the local products associated to the development area
source \${HERE}/localProducts_larsoft_*/setup

# Set up the MRB source local products
mrbslp

# Cleanup
unset HERE

#
alias dunesw-build='ninja -C \${MRB_BUILDDIR} -k 0 install | grep -v "Up-to-date" '

EOF


cat > lar_wrap.sh << EOF
#!/bin/bash

HERE=\$(cd \$(dirname \$(readlink -f \${BASH_SOURCE})) && pwd)

source \${HERE}/setup_dunesw.sh
exec lar "\$@"
EOF

chmod a+x lar_wrap.sh


echo "I am here : $PWD"
source localProducts_larsoft_${DUNESW_VERSION}_${DUNESW_QUALS/:/_}/setup

# Check out dune trigger
mrb g -t ${DUNESW_VERSION} dunesw
mrb g -t ${DUNESW_VERSION} dunecore
mrb g -t ${DUNESW_VERSION} duneprototypes
mrb g -t ${DUNESW_VERSION} duneopdet

mrb g -t ${DUNESW_VERSION} dunetrigger

mrbsetenv #set up environment variables for build
mrb i --generator=ninja  #compile

