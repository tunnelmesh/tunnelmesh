%{ if coord_url != "" ~}
# === WAIT FOR COORDINATOR ===
# Polls the primary coordinator's health endpoint before starting tunnelmesh.
# Prevents join failures when this node starts before the lead coordinator is ready.

COORD_URL="${coord_url}"
TIMEOUT=900
INTERVAL=10
ELAPSED=0

echo "=== Waiting for coordinator at $$COORD_URL/health (up to $${TIMEOUT}s) ==="
until curl -sk --connect-timeout 5 --max-time 10 "$$COORD_URL/health" -o /dev/null 2>&1; do
  if [ $$ELAPSED -ge $$TIMEOUT ]; then
    echo "WARNING: Coordinator not reachable after $${TIMEOUT}s, proceeding anyway"
    break
  fi
  echo "Coordinator not ready ($${ELAPSED}s elapsed), retrying in $${INTERVAL}s..."
  sleep $$INTERVAL
  ELAPSED=$$(($$ELAPSED + $$INTERVAL))
done

if [ $$ELAPSED -lt $$TIMEOUT ]; then
  echo "Coordinator ready after $${ELAPSED}s"
fi
%{ endif ~}
