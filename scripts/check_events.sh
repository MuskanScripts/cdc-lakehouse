#!/usr/bin/env bash
# Week 1 "done when" check: reads each CDC topic and counts Debezium ops.
#   r = snapshot read (initial load)   c = insert   u = update   d = delete
# Pass = every table shows c, u and d.
set -uo pipefail

MAX="${MAX_MESSAGES:-3000}"
TABLES=(users products orders order_items payments)
fail=0

for t in "${TABLES[@]}"; do
  topic="ecom.public.$t"
  out=$(docker compose exec -T schema-registry kafka-avro-console-consumer \
          --bootstrap-server kafka:29092 --topic "$topic" --from-beginning \
          --max-messages "$MAX" --timeout-ms 15000 \
          --property schema.registry.url=http://localhost:8081 2>/dev/null)
  counts=$(printf '%s\n' "$out" | grep -oE '"op":"[rcud]"' | cut -d'"' -f4 | sort | uniq -c \
           | awk '{printf "%s=%s ", $2, $1}')
  missing=""
  for op in c u d; do
    grep -q "\"op\":\"$op\"" <<<"$out" || missing+="$op "
  done
  if [[ -z "$missing" ]]; then
    printf '  PASS  %-28s %s\n' "$topic" "$counts"
  else
    printf '  FAIL  %-28s %s(missing: %s)\n' "$topic" "$counts" "$missing"
    fail=1
  fi
done

if [[ $fail -eq 0 ]]; then
  echo "Week 1 done: c/u/d events flowing for every table."
else
  echo "Some ops missing. Let the simulator run longer (deletes are rare), or raise MAX_MESSAGES." >&2
fi
exit $fail
