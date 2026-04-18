#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8082}"
PROFILE="${PROFILE:-mixed}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
CONCURRENCY="${CONCURRENCY:-20}"
REQUEST_DELAY_MS="${REQUEST_DELAY_MS:-0}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || ! [[ "$CONCURRENCY" =~ ^[0-9]+$ ]]; then
  echo "DURATION_SECONDS and CONCURRENCY must be integers" >&2
  exit 1
fi

case "$PROFILE" in
  happy|failure|slow|mixed)
    ;;
  *)
    echo "Unsupported PROFILE=$PROFILE. Use one of: happy,failure,slow,mixed" >&2
    exit 1
    ;;
esac

end_ts=$(( $(date +%s) + DURATION_SECONDS ))

declare -i total=0
declare -i ok=0
declare -i failed=0

echo "Starting load test"
echo "  BASE_URL=$BASE_URL"
echo "  PROFILE=$PROFILE"
echo "  DURATION_SECONDS=$DURATION_SECONDS"
echo "  CONCURRENCY=$CONCURRENCY"
echo "  REQUEST_DELAY_MS=$REQUEST_DELAY_MS"

hit_endpoint() {
  local url="$1"
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" "$url" || echo "000")
  echo "$code"
}

pick_path() {
  local roll=$((RANDOM % 100))
  case "$PROFILE" in
    happy)
      echo "/demo"
      ;;
    failure)
      if (( roll < 80 )); then
        echo "/demo?fail=1"
      else
        echo "/demo"
      fi
      ;;
    slow)
      if (( roll < 80 )); then
        echo "/demo?slow=1"
      else
        echo "/demo"
      fi
      ;;
    mixed)
      if (( roll < 45 )); then
        echo "/demo"
      elif (( roll < 75 )); then
        echo "/demo?slow=1"
      else
        echo "/demo?fail=1"
      fi
      ;;
  esac
}

worker() {
  while (( $(date +%s) < end_ts )); do
    local path
    local status
    path=$(pick_path)
    status=$(hit_endpoint "$BASE_URL$path")

    printf "%s\n" "$status"

    if (( REQUEST_DELAY_MS > 0 )); then
      sleep "$(awk "BEGIN {printf \"%.3f\", ${REQUEST_DELAY_MS}/1000}")"
    fi
  done
}

pids=()
tmpfile=$(mktemp)
trap 'rm -f "$tmpfile"' EXIT

for _ in $(seq 1 "$CONCURRENCY"); do
  (
    worker >> "$tmpfile"
  ) &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

while IFS= read -r line; do
  ((total += 1))
  if [[ "$line" =~ ^2[0-9][0-9]$ ]]; then
    ((ok += 1))
  else
    ((failed += 1))
  fi
done < "$tmpfile"

if (( total == 0 )); then
  success_rate="0.00"
else
  success_rate=$(awk "BEGIN {printf \"%.2f\", ($ok/$total)*100}")
fi

echo "Load complete"
echo "  total_requests=$total"
echo "  success_2xx=$ok"
echo "  failed_non2xx_or_transport=$failed"
echo "  success_rate_percent=$success_rate"
