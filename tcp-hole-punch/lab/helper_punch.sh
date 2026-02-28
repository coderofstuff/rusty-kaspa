#!/usr/bin/env bash
# Trigger a libp2p dial via the helper API.
# Usage:
#   export HELPER_HOST=... (default 127.0.0.1)
#   export HELPER_PORT=... (default 38080)
#   export RELAY_PEER_ID=...
#   export TARGET_PEER_ID=...
#   ./helper_punch.sh

set -e

HELPER_HOST=${HELPER_HOST:-127.0.0.1}
HELPER_PORT=${HELPER_PORT:-38080}

if [[ -z "${RELAY_PEER_ID}" || -z "${TARGET_PEER_ID}" ]]; then
    echo "Error: RELAY_PEER_ID and TARGET_PEER_ID must be set."
    exit 1
fi

# DCUtR dial string: /p2p/<relay>/p2p-circuit/p2p/<target>
MULTIADDR="/p2p/${RELAY_PEER_ID}/p2p-circuit/p2p/${TARGET_PEER_ID}"

echo "Triggering dial to ${MULTIADDR} on helper ${HELPER_HOST}:${HELPER_PORT}"

# JSON request
# {"action": "dial", "multiaddr": "..."}
JSON=$(printf '{"action":"dial","multiaddr":"%s"}' "${MULTIADDR}")

echo "${JSON}" | nc -w 5 "${HELPER_HOST}" "${HELPER_PORT}"
echo ""

