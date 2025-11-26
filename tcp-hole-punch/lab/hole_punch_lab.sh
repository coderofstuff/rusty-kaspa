#!/usr/bin/env bash
set -euo pipefail

# Minimal lab harness for the Proxmox NAT setup.
# It starts/stops the three kaspad nodes with libp2p full mode enabled.
# Note: current branch logs "libp2p mode enabled; transport is currently unimplemented..."
# so reservations/dcutr are not active yet. This script simply codifies the commands used.

SSH_JUMP=${SSH_JUMP:-root@10.0.3.11}
SSH_OPTS="-o StrictHostKeyChecking=no"
if [[ -n "${SSH_JUMP:-}" ]]; then
  SSH_OPTS+=" -J ${SSH_JUMP}"
fi

NODE_A=${NODE_A:-ubuntu@192.168.1.10}
NODE_B=${NODE_B:-ubuntu@192.168.2.10}
NODE_C=${NODE_C:-ubuntu@10.0.3.50}

REPO_PATH=${REPO_PATH:-~/rusty-kaspa}
BIN_PATH=${BIN_PATH:-${REPO_PATH}/target/release/kaspad}
P2P_PORT=${P2P_PORT:-16111}
RPC_PORT=${RPC_PORT:-17110}
HELPER_PORT=${HELPER_PORT:-38080}
LOGLEVEL=${LOGLEVEL:-debug}
RUST_LOG=${RUST_LOG:-info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info}

APPDIR_A=${APPDIR_A:-~/kaspa/node-a}
APPDIR_B=${APPDIR_B:-~/kaspa/node-b}
APPDIR_C=${APPDIR_C:-~/kaspa/node-c}

C_PUBLIC_IP=${C_PUBLIC_IP:-10.0.3.50}
A_EXTERNAL_IP=${A_EXTERNAL_IP:-10.0.3.61}
B_EXTERNAL_IP=${B_EXTERNAL_IP:-10.0.3.62}

# Required for starting nodes A/B.
C_PEER_ID=${C_PEER_ID:-}

ssh_run() {
  local host=$1
  local script=$2
  ssh ${SSH_OPTS} "${host}" "bash -lc $(printf '%q' "${script}")"
}

start_node_c() {
  local script
  script=$(cat <<EOF
set -e
source ~/.cargo/env
mkdir -p ${APPDIR_C}
LOG=${APPDIR_C}/kaspad.log
: > "\${LOG}"
RUST_LOG=${RUST_LOG} \\
nohup ${BIN_PATH} \\
  --simnet \\
  --appdir=${APPDIR_C} \\
  --loglevel=${LOGLEVEL} \\
  --nodnsseed \\
  --disable-upnp \\
  --listen=0.0.0.0:${P2P_PORT} \\
  --rpclisten=0.0.0.0:${RPC_PORT} \\
  --libp2p-mode=full \\
  --libp2p-helper-listen=0.0.0.0:${HELPER_PORT} \\
  --libp2p-identity-path=${APPDIR_C}/libp2p.id \\
  >>"\${LOG}" 2>&1 < /dev/null & echo \$! > ${APPDIR_C}/kaspad.pid
EOF
)
  ssh_run "${NODE_C}" "${script}"
}

start_private_node() {
  local host=$1
  local appdir=$2
  local external_ip=$3
  local helper_listen=$4
  if [[ -z "${C_PEER_ID}" ]]; then
    echo "C_PEER_ID is required to start ${host}. Set it from Node C getLibp2pStatus." >&2
    exit 1
  fi
  local reservations="/ip4/${C_PUBLIC_IP}/tcp/${P2P_PORT}/p2p/${C_PEER_ID}"
  local script
  script=$(cat <<EOF
set -e
source ~/.cargo/env
mkdir -p ${appdir}
LOG=${appdir}/kaspad.log
: > "\${LOG}"
KASPAD_LIBP2P_RESERVATIONS=${reservations} \\
KASPAD_LIBP2P_EXTERNAL_MULTIADDRS=/ip4/${external_ip}/tcp/${P2P_PORT} \\
RUST_LOG=${RUST_LOG} \\
nohup ${BIN_PATH} \\
  --simnet \\
  --appdir=${appdir} \\
  --loglevel=${LOGLEVEL} \\
  --nodnsseed \\
  --disable-upnp \\
  --listen=0.0.0.0:${P2P_PORT} \\
  --rpclisten=0.0.0.0:${RPC_PORT} \\
  --libp2p-mode=full \\
  --libp2p-helper-listen=${helper_listen} \\
  --libp2p-identity-path=${appdir}/libp2p.id \\
  >>"\${LOG}" 2>&1 < /dev/null & echo \$! > ${appdir}/kaspad.pid
EOF
)
  ssh_run "${host}" "${script}"
}

stop_node() {
  local host=$1
  ssh_run "${host}" "pkill -x kaspad || true"
}

action=${1:-start}
case "${action}" in
  start)
    start_node_c
    echo "Started node C on ${NODE_C}; fetch peer id via getLibp2pStatus before starting A/B."
    start_private_node "${NODE_A}" "${APPDIR_A}" "${A_EXTERNAL_IP}" "127.0.0.1:${HELPER_PORT}"
    start_private_node "${NODE_B}" "${APPDIR_B}" "${B_EXTERNAL_IP}" "127.0.0.1:${HELPER_PORT}"
    ;;
  stop)
    stop_node "${NODE_A}"
    stop_node "${NODE_B}"
    stop_node "${NODE_C}"
    ;;
  *)
    echo "Usage: $0 [start|stop]" >&2
    exit 1
    ;;
esac
