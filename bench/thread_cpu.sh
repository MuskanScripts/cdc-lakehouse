#!/usr/bin/env bash
# Per-thread CPU of the main java process in a container, sampled over N seconds, grouped by
# thread name (digits stripped). A poor man's profiler that needs nothing in the image but /proc.
#   ./bench/thread_cpu.sh streaming 30
set -u
C=${1:?container}; S=${2:-30}
docker exec -i "$C" sh -s "$S" <<'EOF'
S=$1
pid=$(for p in /proc/[0-9]*; do tr '\0' ' ' < $p/cmdline 2>/dev/null | grep -q java && { echo ${p#/proc/}; break; }; done)
snap() { for t in /proc/$pid/task/*; do n=$(cat $t/comm 2>/dev/null); s=$(cut -d')' -f2 $t/stat 2>/dev/null); echo "$(basename $t)|$n|$(echo $s | awk '{print $12+$13}')"; done; }
snap > /tmp/a; sleep $S; snap > /tmp/b
hz=$(getconf CLK_TCK 2>/dev/null || echo 100)
awk -F'|' -v S=$S -v hz=$hz 'NR==FNR{a[$1]=$3; next} {d=$3-a[$1]; n=$2; gsub(/[0-9]+/,"#",n); c[n]+=d; tot+=d}
  END{printf "total %.0f%% of one core over %ss\n", 100*tot/hz/S, S; for(k in c) if(c[k]>0) printf "%7.1f%%  %s\n", 100*c[k]/hz/S, k}' /tmp/a /tmp/b | sort -rn | head -16
EOF
