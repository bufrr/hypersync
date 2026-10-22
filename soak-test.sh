#!/bin/bash
# hypersync stability soak monitor: samples the node's applied-block rate, gateway-only sync
# source, real error count, and container health at a fixed interval. Companion to peerd.sh.
#
# "Real" errors exclude internet-scanner noise by SIGNATURE, not by source IP: the node never
# receives inbound peer connections (it only dials out), so any "tcp greeting ... gossip" /
# "gossip rpc request ..." over-limit error is always scanner noise hitting the node's public
# ports, regardless of which IP sent it. A real sync-path error has a different desc (e.g.
# "abci_stream recv greeting", "process_client_block error").
# A cache replay can log "receiving evm kvs for checkpoint that already exists" when the node has
# a leftover EVM checkpoint but no linked ABCI state during restart; the node recovers by relinking
# state, so the monitor does not treat that specific line as fatal.
# Samples use non-overlapping docker-log windows so a failed/aborted previous run cannot pollute the
# first sample of a restarted soak.
# Applied height is sampled only from the current window; stale docker-log tail fallback would make a
# restarted node appear to move backwards.
#
# Tier/mempool columns (QuickNode upgrade plan): tier = the node's current upstream tier from the
# gateway's latest active_peer= log field (prio = Tier P, pool = Tier O); starts = session starts per tier
# in the window; mp_mb = mempool_txs bytes the node wrote in the window; drops/alert = --push
# round-gate drops (sampled) and the wire-format alarm.
#
# Usage: soak-test.sh [restart-node-at-start:0|1] [samples] [interval-seconds] [log-file]
NODE="${HL_NODE:-hyperliquid-node-1}"
GW="${HL_GW:-hypersync-gw}"
# the node's address as the gateway sees it (its log tag); default: the node container's IP on the
# gateway network. Set HL_NODE_IP for a node on another host (e.g. its VPN address).
NODE_IP="${HL_NODE_IP:-$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "$NODE" 2>/dev/null | tr ' ' '\n' | grep -m1 '^172\.28\.')}"
RESTART="${1:-0}"
N="${2:-20}"
INTERVAL="${3:-180}"
LOG="${4:-/dev/stdout}"

if [ "$RESTART" = "1" ]; then
  echo "$(date '+%F %T') restarting $NODE at t=0" >> "$LOG"
  sudo docker restart "$NODE" >/dev/null 2>&1
fi

GWIP=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW" 2>/dev/null)
echo "=== soak start $(date '+%F %T') node=$NODE gw=$GW($GWIP) samples=$N interval=${INTERVAL}s ===" >> "$LOG"
mp_bytes() { sudo docker exec "$NODE" sh -c 'du -sb /home/hluser/hl/data/mempool_txs 2>/dev/null | cut -f1' 2>/dev/null; }
prev=
mp_prev=$(mp_bytes)
SINCE_TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
for i in $(seq 1 "$N"); do
  UNTIL_TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  NLOG=$(sudo docker logs --since "$SINCE_TS" --until "$UNTIL_TS" "$NODE" 2>&1)
  GLOG=$(sudo docker logs --since "$SINCE_TS" --until "$UNTIL_TS" "$GW" 2>&1)
  A=$(printf '%s\n' "$NLOG" | grep -a 'applied block' | tail -1 | grep -oE '[0-9]{9,}')
  PID=$(sudo docker inspect -f '{{.State.Pid}}' "$NODE" 2>/dev/null)
  SRC=$(sudo nsenter -t "$PID" -n ss -tn 2>/dev/null | grep ESTAB | awk -v ip="$GWIP" '$5 ~ "^"ip":"{print $5}' | sort -u | tr '\n' ',')
  BOOTTO=$(printf '%s\n' "$NLOG" | grep -ac 'bootstrap timeout: lu::timeout node_bootstrap')
  REAL=$(printf '%s\n' "$NLOG" | grep -aE 'ERROR|panic|panicked' | grep -aE 'panic|panicked|over limit|recv greeting|process_client_block|failed to verify client block batch|Unexpected proposer|Bad block proposer|unexpected client block round|invalid parent|Peer-only request|receiving evm kvs|visor child in bad state|child_low_memory' | grep -avE 'desc: "(tcp greeting|gossip rpc request)' | grep -av 'receiving evm kvs for checkpoint that already exists' | grep -av 'bootstrap timeout: lu::timeout node_bootstrap' | wc -l)
  BEHIND=$(printf '%s\n' "$NLOG" | grep -ac 'Querying jailed validators for high round')
  CHILD=$(printf '%s\n' "$NLOG" | grep -acE 'visor child in bad state|child_low_memory')
  GWERR=$(printf '%s\n' "$GLOG" | grep -aEi 'panic|error|Peer-only|no peer serving|oversized frame' | wc -l)
  CATCHUP=$(printf '%s\n' "$NLOG" | grep -acE 'client block batch during bootstrap|reading bytes for gossip')
  GWRC=$(sudo docker inspect -f '{{.RestartCount}}' "$GW" 2>/dev/null)
  GWST=$(sudo docker inspect -f '{{.State.Status}}' "$GW" 2>/dev/null)
  GWOOM=$(sudo docker inspect -f '{{.State.OOMKilled}}' "$GW" 2>/dev/null)
  NRC=$(sudo docker inspect -f '{{.RestartCount}}' "$NODE" 2>/dev/null)
  GWMEM=$(sudo docker stats --no-stream --format '{{.MemUsage}}' "$GW" 2>/dev/null | awk '{print $1}')
  NMEM=$(sudo docker stats --no-stream --format '{{.MemUsage}}' "$NODE" 2>/dev/null | awk '{print $1}')
  # the node's current upstream: the newest session start or failover switch the gateway logged
  # for THIS node (lines tagged "[gw] [<node-ip>]"); a switch moves the active without a new
  # session start. Without a known node IP, fall back to the newest active_peer= of any node.
  if [ -n "$NODE_IP" ]; then
    LAST_ACTIVE=$(sudo docker logs "$GW" 2>&1 | grep -aF "[gw] [$NODE_IP] " \
      | grep -aoE 'session start active_peer=[^ ]+|failover switch [^ ]+ -> [^ ]+' | tail -1 \
      | sed -E 's/.*(active_peer=|-> )//')
  else
    LAST_ACTIVE=$(sudo docker logs "$GW" 2>&1 | grep -aoE 'active_peer=[^ ]+' | tail -1 | sed 's/^active_peer=//')
  fi
  case "$LAST_ACTIVE" in
    prio-*) TIER=prio ;;
    "") TIER=none ;;
    *) TIER=pool ;;
  esac
  ST_PRIO=$(printf '%s\n' "$GLOG" | grep -ac 'session start.*tier=prio')
  ST_POOL=$(printf '%s\n' "$GLOG" | grep -ac 'session start.*tier=pool')
  # DROPS / ALERT come from serve_push's stats lines; with hot standbys (serve_failover) those
  # lines aren't written, so both stay 0 there — check the failover switch/status lines instead
  DROPS=$(printf '%s\n' "$GLOG" | grep -ac 'push drop sample')
  ALERT=$(printf '%s\n' "$GLOG" | grep -ac 'ALERT=')
  PRIOFAIL=$(printf '%s\n' "$GLOG" | grep -ac 'live attempt failed')
  mp_now=$(mp_bytes)
  if [ -n "$mp_now" ] && [ -n "$mp_prev" ]; then
    MPMB=$(( (mp_now - mp_prev) / 1000000 ))
  else
    MPMB=NA
  fi
  mp_prev="$mp_now"
  if [ -n "$A" ] && [ -n "$prev" ]; then
    delta=$(( A - prev ))
  else
    delta=NA
  fi
  printf '%s s%02d/%d applied=%s d/%ds=%s src=[%s] realerr=%s boot_timeout=%s behind=%s child=%s catchup=%s gwerr=%s tier=%s starts{prio=%s pool=%s} prio_fail=%s mp_mb=%s drops=%s alert=%s gw{rc=%s st=%s oom=%s mem=%s} node{rc=%s mem=%s}\n' \
    "$(date '+%T')" "$i" "$N" "${A:-?}" "$INTERVAL" "$delta" "$SRC" "$REAL" "$BOOTTO" "$BEHIND" "$CHILD" "$CATCHUP" "$GWERR" "$TIER" "$ST_PRIO" "$ST_POOL" "$PRIOFAIL" "$MPMB" "$DROPS" "$ALERT" "${GWRC:-?}" "${GWST:-?}" "${GWOOM:-?}" "${GWMEM:-?}" "${NRC:-?}" "${NMEM:-?}" >> "$LOG"
  prev="$A"
  SINCE_TS="$UNTIL_TS"
  [ "$i" -lt "$N" ] && sleep "$INTERVAL"
done
echo "=== soak end $(date '+%F %T') ===" >> "$LOG"
