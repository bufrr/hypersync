#!/bin/bash
# hypersync stability soak monitor: samples the node's applied-block rate, gateway-only sync
# source, real error count, and container health at a fixed interval. Companion to peerd.sh.
#
# "Real" errors exclude internet-scanner noise by SIGNATURE, not by source IP: the node never
# receives inbound peer connections (it only dials out), so any "tcp greeting ... gossip" /
# "gossip rpc request ..." over-limit error is always scanner noise hitting the node's public
# ports, regardless of which IP sent it. A real sync-path error has a different desc (e.g.
# "abci_stream recv greeting", "process_client_block error").
#
# Usage: soak-test.sh [restart-node-at-start:0|1] [samples] [interval-seconds] [log-file]
NODE="${HL_NODE:-hyperliquid-node-1}"
GW="${HL_GW:-hl-gw}"
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
prev=0
for i in $(seq 1 "$N"); do
  A=$(sudo docker logs --tail 40 "$NODE" 2>&1 | grep -a 'applied block' | tail -1 | grep -oE '[0-9]{9,}')
  PID=$(sudo docker inspect -f '{{.State.Pid}}' "$NODE" 2>/dev/null)
  SRC=$(sudo nsenter -t "$PID" -n ss -tn 2>/dev/null | grep ESTAB | awk -v ip="$GWIP" '$5 ~ "^"ip":"{print $5}' | sort -u | tr '\n' ',')
  REAL=$(sudo docker logs --tail 1500 "$NODE" 2>&1 | grep -aE 'panic|over limit|recv greeting|process_client_block' | grep -avE 'desc: "(tcp greeting|gossip rpc request)' | wc -l)
  CATCHUP=$(sudo docker logs --tail 25 "$NODE" 2>&1 | grep -acE 'client block batch during bootstrap|reading bytes for gossip')
  GWRC=$(sudo docker inspect -f '{{.RestartCount}}' "$GW" 2>/dev/null)
  GWST=$(sudo docker inspect -f '{{.State.Status}}' "$GW" 2>/dev/null)
  GWOOM=$(sudo docker inspect -f '{{.State.OOMKilled}}' "$GW" 2>/dev/null)
  NRC=$(sudo docker inspect -f '{{.RestartCount}}' "$NODE" 2>/dev/null)
  delta=$(( ${A:-0} - prev ))
  [ "$prev" = 0 ] && delta=NA
  printf '%s s%02d/%d applied=%s d/%ds=%s src=[%s] realerr=%s catchup=%s gw{rc=%s st=%s oom=%s} noderc=%s\n' \
    "$(date '+%T')" "$i" "$N" "${A:-?}" "$INTERVAL" "$delta" "$SRC" "$REAL" "$CATCHUP" "${GWRC:-?}" "${GWST:-?}" "${GWOOM:-?}" "${NRC:-?}" >> "$LOG"
  prev=${A:-0}
  [ "$i" -lt "$N" ] && sleep "$INTERVAL"
done
echo "=== soak end $(date '+%F %T') ===" >> "$LOG"
