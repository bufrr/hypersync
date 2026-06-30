#!/bin/bash
# hypersync peer daemon: continuously discover peers and probe LIVE-block serving (send_abci:false),
# maintaining a ranked pool of healthy live-servers in peers.json. It deliberately does NOT pull the
# abci_state (send_abci:true) — that is rate-limited per IP, so probing it would exhaust our quota.
# Live blocks are cheap/unlimited, so this probe is safe to run continuously. The gateway reads
# peers.json: it merge-streams live blocks from many of these peers, and pulls+caches the abci_state
# once from whichever of them also serves it.
SP="${HYPERSYNC_DATA:-$(cd "$(dirname "$0")" && pwd)/data}"; mkdir -p "$SP"
NODE="${HL_NODE:-hyperliquid-node-1}"   # a running HL node whose discovered peers we harvest
CAND="$SP/peer_candidates.txt"; OUT="$SP/peers.json"; LOG="$SP/peerd.log"
INTERVAL=${1:-300}
touch "$CAND"
while true; do
  ROOTS=$(curl -s -X POST -H 'Content-Type: application/json' --data '{"type":"gossipRootIps"}' https://api.hyperliquid.xyz/info 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
  HARV=$(sudo docker logs --tail 3000 "$NODE" 2>&1 | grep -aoE 'Ip\(([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\)' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
  printf '%s\n%s\n%s\n' "$(cat "$CAND")" "$ROOTS" "$HARV" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | grep -vE '^172\.18|^127\.|152\.53\.128\.11' | sort -u > "$CAND.tmp" && mv "$CAND.tmp" "$CAND"
  python3 - "$CAND" "$OUT" <<'PY'
import socket,struct,sys,threading,json,time
ips=[l.strip() for l in open(sys.argv[1]) if l.strip()]
live={}; lock=threading.Lock()
def probe(ip):
    try:
        s=socket.create_connection((ip,4001),timeout=4); s.sendall(bytes.fromhex('0000000300000000')); s.settimeout(5)  # send_abci:false
        t0=time.time(); blocks=0; buf=b''
        while time.time()-t0<4:
            try: d=s.recv(65536)
            except: break
            if not d: break
            buf+=d
            while len(buf)>=5:
                L=struct.unpack('>I',buf[:4])[0]; typ=buf[4]
                if len(buf)<5+L: break
                if typ==1 and L>1: blocks+=1
                buf=buf[5+L:]
        s.close()
        if blocks>=2:
            with lock: live[ip]=blocks
    except: pass
ts=[threading.Thread(target=probe,args=(ip,)) for ip in ips]
for t in ts: t.start()
for t in ts: t.join()
ranked=sorted(live, key=lambda ip:-live[ip])
json.dump({"live_servers":ranked,"n_candidates":len(ips)}, open(sys.argv[2],'w'))
print("candidates=%d live_servers=%d top=%s"%(len(ips),len(ranked),ranked[:6]))
PY
  echo "$(date '+%H:%M:%S') $(python3 -c "import json;d=json.load(open('$OUT'));print('candidates=%d live=%d top=%s'%(d['n_candidates'],len(d['live_servers']),d['live_servers'][:6]))" 2>/dev/null)" >> "$LOG"
  sleep "$INTERVAL"
done
