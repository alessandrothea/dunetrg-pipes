HERE=$(cd $(dirname $(readlink -f ${BASH_SOURCE})) && pwd)


DUNESW_ENV=$HERE/setup_dunesw.sh
TOOLS_ENV=$HERE/../toolbox/setup_env.sh
source $DUNESW_ENV
source $TOOLS_ENV

