#!/usr/bin/env bash
# Five real Quorum servers in Docker, broken on purpose while quorum-check hammers them.
#
#   scripts/chaos.sh [seconds]        (needs docker, docker compose, go; tc + nsenter + sudo for netem)
#
# Faults, picked at random every few seconds:
#   kill        kill -9 a node, start it again from its disk
#   kill-leader the same, aimed at the leader
#   pause       freeze a node (SIGSTOP): it is alive but says nothing, like a long GC pause
#   partition   cut a node off the network, then reconnect it
#   isolate-leader  the same, aimed at the leader: the rest must elect a new one, and the old
#               one must not answer anything it cannot prove is current
#   netem       every link gets slow and lossy (60±40 ms, 10% loss) for a while
#   majority    kill 3 of 5: nothing can commit (and nothing wrong may be answered), then recovery
#   blackout    a power cut: kill all five at once, start them again; nothing acknowledged may be lost
#
# Everything is logged to chaos/events.log; the checker's verdict and the full history go to
# chaos/report.json. Exit status is the checker's: 0 linearizable, 1 not (or no progress).
set -uo pipefail
cd "$(dirname "$0")/.."
DURATION=${1:-90}
SEED=${SEED:-$RANDOM}
RANDOM=$SEED
OUT=chaos
NODES="1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003,4=http://127.0.0.1:7004,5=http://127.0.0.1:7005"
mkdir -p $OUT bin
: > $OUT/events.log
log() { echo "$(date +%H:%M:%S.%3N) $*" | tee -a $OUT/events.log; }

CGO_ENABLED=0 go build -o bin/quorum-server ./cmd/quorum-server || exit 1
go build -o bin/quorum-check ./cmd/quorum-check || exit 1
docker build -q -t quorum . >/dev/null || exit 1
docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1 || exit 1
trap 'docker compose logs --no-color > $OUT/servers.log 2>&1; docker compose down -v >/dev/null 2>&1' EXIT

leader() {
  for i in 1 2 3 4 5; do
    l=$(curl -s -m 1 127.0.0.1:700$i/status | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["id"] if s["role"]=="leader" else "")' 2>/dev/null)
    [ -n "$l" ] && { echo "$l"; return; }
  done
  echo $(( RANDOM % 5 + 1 ))
}
pick() { echo $(( RANDOM % 5 + 1 )); }
between() { local x=$(( $1 * 10 + RANDOM % (($2 - $1) * 10 + 1) )); echo "$(( x / 10 )).$(( x % 10 ))"; }
NETEM=0
if command -v tc >/dev/null && command -v nsenter >/dev/null && sudo -n true 2>/dev/null; then NETEM=1; fi
ns() { sudo nsenter -t "$(docker inspect -f '{{.State.Pid}}' n$1)" -n "${@:2}"; }

for t in $(seq 50); do curl -s -m 1 127.0.0.1:7001/status | grep -q '"leader":[1-5]' && break; sleep 0.2; done
log "cluster up, seed $SEED, netem $([ $NETEM = 1 ] && echo on || echo 'off (needs tc, nsenter, sudo)')"

bin/quorum-check -nodes "$NODES" -clients 10 -keys 5 -duration "${DURATION}s" -seed "$SEED" -min-ok 1000 -out $OUT/report.json > $OUT/check.txt &
CHECK=$!
sleep 3
FAULTS=(kill kill-leader pause partition isolate-leader majority blackout)
[ $NETEM = 1 ] && FAULTS+=(netem netem)
while kill -0 $CHECK 2>/dev/null; do
  f=${FAULTS[$(( RANDOM % ${#FAULTS[@]} ))]}
  case $f in
    kill|kill-leader)
      n=$([ $f = kill ] && pick || leader); d=$(between 1 4)
      log "$f: kill -9 n$n for ${d}s"; docker kill -s KILL n$n >/dev/null; sleep $d
      docker start n$n >/dev/null; log "  n$n restarted" ;;
    pause)
      n=$(pick); d=$(between 2 5)
      log "pause: freeze n$n for ${d}s"; docker pause n$n >/dev/null; sleep $d; docker unpause n$n >/dev/null ;;
    partition|isolate-leader)
      n=$([ $f = partition ] && pick || leader); d=$(between 3 6)
      log "$f: cut n$n off the network for ${d}s"; docker network disconnect -f quorum_quorum n$n >/dev/null; sleep $d
      docker network connect quorum_quorum n$n >/dev/null; log "  n$n reconnected" ;;
    netem)
      d=$(between 4 8)
      log "netem: every node 60±40 ms, 10% loss for ${d}s"
      for i in 1 2 3 4 5; do ns $i tc qdisc add dev eth0 root netem delay 60ms 40ms loss 10% 2>/dev/null; done
      sleep $d
      for i in 1 2 3 4 5; do ns $i tc qdisc del dev eth0 root 2>/dev/null; done ;;
    majority)
      v=(1 2 3 4 5); for i in 4 3 2 1; do j=$(( RANDOM % (i + 1) )); x=${v[i]}; v[i]=${v[j]}; v[j]=$x; done
      set -- ${v[0]} ${v[1]} ${v[2]}; d=$(between 2 4)
      log "majority: kill -9 n$1 n$2 n$3 for ${d}s (no quorum: nothing may commit)"
      docker kill -s KILL n$1 n$2 n$3 >/dev/null; sleep $d; docker start n$1 n$2 n$3 >/dev/null; log "  restarted" ;;
    blackout)
      d=$(between 1 3)
      log "blackout: power cut, all five killed for ${d}s"
      docker kill -s KILL n1 n2 n3 n4 n5 >/dev/null; sleep $d; docker start n1 n2 n3 n4 n5 >/dev/null; log "  power back" ;;
  esac
  sleep "$(between 2 5)"
done
wait $CHECK; STATUS=$?
cat $OUT/check.txt | tee -a $OUT/events.log
log "checker exit status $STATUS"
exit $STATUS
