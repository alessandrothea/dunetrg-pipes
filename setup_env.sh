HERE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)

export PATH="${HERE}/scripts${PATH:+:${PATH}}"

unset HERE

echo "--- dunetrg-pipes setup complete ---"