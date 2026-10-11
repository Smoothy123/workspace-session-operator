# WorkspaceSession Operator — Design

**Date:** 2026-10-11
**Status:** Approved
**Repo:** `github.com/Smoothy123/workspace-session-operator`

## 1. Purpose

A Kubernetes operator that manages the lifecycle of **ephemeral developer
workspaces** — short-lived pods that back a cloud IDE, a preview environment, or
a review app.

The problem it solves is cost. Dev environments are created constantly and
abandoned silently; nobody remembers to delete them. The operator makes the
environment responsible for its own death: a session declares how long it may
idle, how much it may be extended, and the absolute ceiling on its lifetime.
When the clock runs out, it tears itself down.

### Audience and success criteria

This is a portfolio project. An engineer evaluating it should be able to clone
the repo, run it against a local cluster in under five minutes, and come away
convinced the author understands CRDs, controller-runtime, reconciliation,
owner references, finalizers, and status conditions.

That sets a bar beyond "the code looks plausible":

- It must compile and the tests must pass.
- The README walkthrough must reflect output actually observed, not invented.
- Design tradeoffs must be stated honestly, including where a simpler mechanism
  would have sufficed.

Code that only looks correct is worth less than less code that provably works.

### Non-goals

- Detecting real network traffic to a workspace. Activity enters through a
  documented seam; the operator does not sniff connections.
- Multi-tenancy, quotas, authentication, or billing.
- Persistent volumes, workspace snapshotting, or hibernate/resume.
- High availability of the controller itself (single replica, leader election
  left at the kubebuilder default).

## 2. API

Group `workspace.smoothy.dev`, version `v1alpha1`, kind `WorkspaceSession`,
scope Namespaced, short name `ws`.

### Spec

| Field | Type | Default | Meaning |
|---|---|---|---|
| `image` | string | — (required) | Container image backing the workspace |
| `port` | int32 | `8080` | Port the workspace listens on |
| `idleTimeout` | duration | `30m` | Time without activity before a session is eligible to expire |
| `extensionDuration` | duration | `30m` | Lifetime granted by each extension |
| `maxExtensions` | int32 | `3` | Cap on how many times a session may be extended |
| `hardTTL` | duration | `4h` | Absolute lifetime ceiling, measured from `creationTimestamp` |

Durations are `metav1.Duration` so they serialize as `"30m"` rather than
nanoseconds. `image` has `MinLength=1`; `maxExtensions` has `Minimum=0`
(zero is meaningful: a session that may never be extended).

### Status

| Field | Type | Meaning |
|---|---|---|
| `phase` | enum | `Pending` \| `Running` \| `Expired` \| `Terminating` |
| `expiresAt` | *Time | When the controller will next wake and decide |
| `lastActivityAt` | *Time | Most recent activity, mirrored from the annotation |
| `extensionsUsed` | int32 | Extensions consumed so far |
| `endpoint` | string | Service DNS name and port |
| `conditions` | []metav1.Condition | `Ready`, `Expired` |

Status is a subresource. Printer columns expose `PHASE`, `ENDPOINT`, `EXPIRES`,
`EXTENSIONS`, and `AGE` so `kubectl get ws` is readable without `-o yaml`.

### Phase semantics

- **Pending** — children created, Pod not yet Ready.
- **Running** — Pod Ready; the session is live.
- **Expired** — terminal. Children deleted, object retained as a tombstone.
- **Terminating** — `deletionTimestamp` set, finalizer running.

`Expired` is terminal and absorbing: once set, reconcile returns immediately
without requeueing. A session is never resurrected by late activity.

## 3. Activity tracking

Activity arrives as an RFC3339 timestamp in an annotation on the
WorkspaceSession:

```
workspace.smoothy.dev/last-activity: 2026-10-11T09:30:00Z
```

The controller parses it and mirrors it into `status.lastActivityAt`, keeping
whichever timestamp is later so a stale write cannot move activity backwards. A
malformed value is ignored, logged, and surfaced as a Warning event rather than
failing the reconcile — a bad annotation must not wedge the lifecycle.

**Why an annotation.** The operator owns *lifecycle*, not *traffic detection*.
In a real deployment the writer would be an ingress proxy or session gateway
that already terminates workspace connections. Keeping that out of the operator
leaves one clear seam, and makes the keep-alive demonstrable in a single
command:

```bash
kubectl annotate ws demo \
  workspace.smoothy.dev/last-activity=$(date -u +%FT%TZ) --overwrite
```

The alternatives were considered and rejected: an HTTP heartbeat endpoint on the
manager adds an unconvincing auth story and forces a port-forward into every
demo; deriving activity from pod metrics requires metrics-server and produces
fragile code for little signal.

## 4. The decision core

The expiry logic lives in `internal/controller/lifecycle.go` as a pure function.
It takes no client, no context, and no wall clock:

```go
type Action string

const (
    ActionInit   Action = "Init"
    ActionWait   Action = "Wait"
    ActionExtend Action = "Extend"
    ActionExpire Action = "Expire"
)

type Decision struct {
    Action       Action
    NewExpiry    time.Time
    RequeueAfter time.Duration
    Reason       string // HardTTLExceeded | IdleTimeout | ExtensionsExhausted
}

func Decide(now, createdAt time.Time, spec DecisionSpec, st DecisionState) Decision
```

### Algorithm

```
hardDeadline := createdAt.Add(spec.HardTTL)

1. now >= hardDeadline              -> Expire{HardTTLExceeded}
2. st.ExpiresAt == nil              -> Init{min(now + IdleTimeout, hardDeadline)}
3. now < *st.ExpiresAt              -> Wait{requeue: *st.ExpiresAt - now}
   --- at or past expiry ---
4. activity stale or absent         -> Expire{IdleTimeout}
5. ExtensionsUsed >= MaxExtensions  -> Expire{ExtensionsExhausted}
6. otherwise                        -> Extend{min(now + ExtensionDuration, hardDeadline)}
```

Activity is *stale* when `st.LastActivityAt` is nil, or when
`now - *st.LastActivityAt >= spec.IdleTimeout`.

Three properties this ordering buys:

- **Hard TTL is strictly dominant.** Checked first, before any extension path
  can run.
- **Hard TTL clamps every extension** via `min(..., hardDeadline)`, rather than
  being a second check that a long `extensionDuration` could overshoot. The
  controller therefore never schedules a wake-up past the ceiling.
- **`Init` does not consume an extension.** Establishing the first expiry window
  is not an extension, and conflating the two would silently cost the user one.

`RequeueAfter` is floored at one second. A clamped expiry can land exactly on
`now`, and a zero-duration requeue would spin the work queue; the floor costs at
most one second of precision on teardown.

### Why it is a pure function

`Reconcile` becomes: read state, call `Decide`, apply the result. Every
interesting rule — precedence, clamping, the off-by-one on `maxExtensions` — is
then exercised by table-driven unit tests that need no API server and run in
milliseconds. This is the single most important structural choice in the
project.

## 5. Reconciliation

```
1. Get object; NotFound -> done.
2. deletionTimestamp set?
     finalizer present -> set phase=Terminating; teardown(Service, then Pod);
                          once both gone, remove finalizer. Children still
                          terminating -> requeue.
     -> return
3. Ensure finalizer present.
4. phase == Expired -> return, no requeue.   (terminal, absorbing)
5. Sync annotation -> status.lastActivityAt (monotonic: keep the later value).
6. Ensure Pod exists, then Service, both with SetControllerReference.
7. d := Decide(clock.Now(), obj.CreationTimestamp.Time, spec, state)
8. Apply:
     Init / Extend -> set expiresAt; Extend also increments extensionsUsed and
                      emits a SessionExtended event; RequeueAfter d.RequeueAfter
     Wait          -> phase from Pod readiness; RequeueAfter d.RequeueAfter
     Expire        -> delete Service and Pod; phase=Expired; Expired condition
                      with d.Reason; SessionExpired event; no requeue
9. Update status.
```

Ordering note: the finalizer is added (step 3) before any child is created
(step 6). A crash between the two leaves an object with a finalizer and no
children, which teardown handles idempotently via `IgnoreNotFound`. The reverse
order could orphan children.

### Watches

```go
For(&WorkspaceSession{}).Owns(&corev1.Pod{}).Owns(&corev1.Service{})
```

Owning the Pod means readiness changes trigger a reconcile immediately, so
`Pending -> Running` is observed at once rather than at the next expiry wake-up.

### Clock injection

The reconciler holds a `k8s.io/utils/clock.PassiveClock`. Production uses
`clock.RealClock{}`; tests substitute a fake clock and advance it, so expiry and
extension paths are exercised without waiting real time.

### Conditions

- `Ready` — `True` when the Pod is Ready; reason `PodReady` / `PodNotReady`.
- `Expired` — `True` once terminal; reason carries `d.Reason`, so
  `kubectl describe` states *why* a session ended.

### Events

`SessionCreated`, `SessionExtended`, `SessionExpired`, and a Warning
`InvalidActivityAnnotation`.

## 6. Finalizer

Name: `workspace.smoothy.dev/teardown`.

Teardown deletes the **Service first, then the Pod**, each with
`client.IgnoreNotFound`, and only removes the finalizer once both are confirmed
gone.

**Honest accounting of what this buys.** OwnerReferences already garbage-collect
the Pod and Service when the WorkspaceSession is deleted. The finalizer is not
load-bearing for cleanup. It provides two things GC does not: deterministic
ordering, so the Service stops routing before the Pod begins terminating and no
request lands on a dying backend; and a guaranteed window to write terminal
status and emit a final event before the object disappears.

The README will state this plainly. Presenting the finalizer as the mechanism
that makes cleanup work would be inaccurate, and is exactly the claim a reviewer
probes.

## 7. Repository layout

```
api/v1alpha1/
  workspacesession_types.go        Spec, Status, Phase, conditions, markers
internal/controller/
  lifecycle.go                     Decide() — pure, no deps
  lifecycle_test.go                table-driven unit tests
  workspacesession_controller.go   Reconcile, children, teardown
  workspacesession_controller_test.go  envtest / Ginkgo
  suite_test.go                    envtest bootstrap
config/                            CRD, RBAC, manager, samples (kustomize)
.github/workflows/ci.yml           build, vet, lint, test
Dockerfile  Makefile  PROJECT
README.md
docs/superpowers/specs/            this document
```

## 8. RBAC

Generated from kubebuilder markers, least-privilege:

- `workspacesessions` — get, list, watch, create, update, patch, delete
- `workspacesessions/status` — get, update, patch
- `workspacesessions/finalizers` — update
- `pods`, `services` — get, list, watch, create, update, patch, delete
- `events` — create, patch

## 9. Testing

### Unit — `Decide`

Table-driven, no cluster. Cases:

1. Fresh object, no expiry -> `Init`, expiry at `now + idleTimeout`
2. `Init` clamped when `idleTimeout` exceeds remaining hard TTL
3. Before expiry -> `Wait` with correct requeue
4. At expiry, recent activity, extensions remain -> `Extend`, counter increments
5. `Extend` clamped by `hardDeadline`
6. At expiry, stale activity -> `Expire{IdleTimeout}`
7. At expiry, nil activity -> `Expire{IdleTimeout}`
8. At expiry, recent activity, `extensionsUsed == maxExtensions` -> `Expire{ExtensionsExhausted}`
9. `maxExtensions: 0` -> first expiry terminates
10. Past hard deadline with recent activity and extensions left -> `Expire{HardTTLExceeded}` (precedence)
11. Activity exactly `idleTimeout` old -> stale (boundary is `>=`)
12. Requeue floor: clamped expiry equal to `now` -> `RequeueAfter >= 1s`

### Integration — envtest / Ginkgo

1. Creation yields Pod and Service with correct controller references, and the
   finalizer is present.
2. Pod marked Ready drives `Pending -> Running` and `Ready` condition `True`.
3. Annotation bump plus fake-clock advance past expiry extends the session and
   increments `extensionsUsed`.
4. Fake-clock advance with no activity expires the session: children deleted,
   object retained with `phase: Expired` and an `Expired` condition.
5. Deleting the object removes children and the finalizer; the object goes away.

**Triggering clock-dependent tests.** Advancing a fake clock does not by itself
wake the controller: the pending `RequeueAfter` was scheduled against real time.
Tests whose assertions depend on the clock moving must advance it and then
enqueue a reconcile explicitly, by writing a no-op label to the object. Test 3
gets this for free from its annotation write; test 4 does it deliberately.

`setup-envtest` fetches control-plane binaries; the Makefile wires this up.

## 10. CI

`.github/workflows/ci.yml`, on push to `main` and on pull requests:

- Go 1.27, module cache keyed on `go.sum`
- `go vet`
- `gofmt -l` with a non-empty result failing the build
- `make test` — runs `setup-envtest` then the full suite
- `make build` — compiles the manager binary
- `golangci-lint` via the official action

A status badge goes at the top of the README. The build must be green before the
repo is presented.

## 11. README

1. One-paragraph problem statement
2. Mermaid state diagram of the lifecycle
3. CRD field reference
4. Quickstart: `kind create cluster` through a live session, with **real pasted
   terminal output**
5. Keep-alive and expiry demonstration
6. How the reconcile loop works, including the `Decide` precedence rules
7. Design notes: the annotation seam, the finalizer's actual role, why the
   decision core is pure
8. Testing instructions
9. Limitations and possible next steps

## 12. Risks

| Risk | Handling |
|---|---|
| envtest binaries unavailable offline | `make test` fetches via `setup-envtest`; cached after first run |
| Status update conflicts under rapid annotation writes | Retry on conflict; status writes are idempotent |
| Long `hardTTL` makes manual demos slow | Sample manifests use short durations (`2m` idle, `10m` TTL) |
| Clock skew between controller and API server | All decisions use the controller's clock; `creationTimestamp` is the only server value read |
