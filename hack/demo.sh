#!/usr/bin/env bash
# Walks a WorkspaceSession through its whole lifecycle against a running
# controller. Assumes `make install` has run and `make run` is active.
#
# Waits on conditions rather than fixed sleeps: the exact moment a session
# extends or expires depends on when the controller's requeue fires, and a
# demo that prints the wrong state because a sleep was 3 seconds short is
# worse than one that takes a little longer.
set -euo pipefail

NS=${NS:-default}
NAME=${NAME:-demo}
SAMPLE=${SAMPLE:-config/samples/workspace_v1alpha1_workspacesession.yaml}

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }

# wait_for <timeout-seconds> <description> <command...>
wait_for() {
  local timeout=$1 desc=$2; shift 2
  local i
  for ((i = 1; i <= timeout; i++)); do
    if "$@" >/dev/null 2>&1; then
      printf '    %s after %ds\n' "$desc" "$i"
      return 0
    fi
    sleep 1
  done
  printf '    TIMED OUT after %ds waiting for %s\n' "$timeout" "$desc" >&2
  return 1
}

phase_is()      { [ "$(kubectl get ws "$NAME" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)" = "$1" ]; }
extensions_is() { [ "$(kubectl get ws "$NAME" -n "$NS" -o jsonpath='{.status.extensionsUsed}' 2>/dev/null)" = "$1" ]; }
children_gone() { [ -z "$(kubectl get pod,svc -n "$NS" -l "workspace.smoothy.dev/session=$NAME" -o name 2>/dev/null)" ]; }

say "Creating the session"
kubectl apply -f "$SAMPLE"
wait_for 90 "pod ready" phase_is Running
kubectl get ws -n "$NS"

say "Children created by the operator"
kubectl get pod,svc -n "$NS" -l "workspace.smoothy.dev/session=$NAME"

say "Each child is owned by the session, so Kubernetes can garbage-collect it"
kubectl get pod "$NAME" -n "$NS" \
  -o jsonpath='{range .metadata.ownerReferences[*]}{.kind}/{.name} controller={.controller}{"\n"}{end}'

say "Recording activity (what a session gateway would do)"
kubectl annotate ws "$NAME" -n "$NS" \
  "workspace.smoothy.dev/last-activity=$(date -u +%FT%TZ)" --overwrite

say "Waiting out the idle window to watch the session extend"
wait_for 200 "extended" extensions_is 1
kubectl get ws -n "$NS"

say "Going quiet so the session expires"
wait_for 300 "expired" phase_is Expired
kubectl get ws -n "$NS"

say "Children are gone, the tombstone remains with the reason"
wait_for 60 "children removed" children_gone
kubectl get pod,svc -n "$NS" -l "workspace.smoothy.dev/session=$NAME" || true
kubectl describe ws "$NAME" -n "$NS" | sed -n '/Conditions/,$p'

say "Cleaning up"
kubectl delete ws "$NAME" -n "$NS"
