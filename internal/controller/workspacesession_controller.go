/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

// WorkspaceSessionReconciler drives a WorkspaceSession toward its declared
// lifetime policy.
type WorkspaceSessionReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Clock is injectable so tests can fast-forward instead of sleeping.
	Clock clock.PassiveClock
}

// +kubebuilder:rbac:groups=workspace.smoothy.dev,resources=workspacesessions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workspace.smoothy.dev,resources=workspacesessions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workspace.smoothy.dev,resources=workspacesessions/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *WorkspaceSessionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var ws workspacev1alpha1.WorkspaceSession
	if err := r.Get(ctx, req.NamespacedName, &ws); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !ws.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &ws)
	}

	// The finalizer goes on before any child is created. A crash between the two
	// leaves an object with a finalizer and no children, which teardown handles
	// idempotently. The reverse order could orphan a Pod.
	if !controllerutil.ContainsFinalizer(&ws, workspacev1alpha1.TeardownFinalizer) {
		controllerutil.AddFinalizer(&ws, workspacev1alpha1.TeardownFinalizer)
		if err := r.Update(ctx, &ws); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
	}

	// Expired is terminal and absorbing. Late activity never resurrects a session.
	if ws.Status.Phase == workspacev1alpha1.PhaseExpired {
		return ctrl.Result{}, nil
	}

	r.syncActivity(ctx, &ws)

	// Decide before touching children. Ensuring them first means a session that
	// is already past its ceiling still gets a Pod scheduled and an image pull
	// started, only to be deleted microseconds later on the same reconcile.
	d := Decide(r.Clock.Now(), ws.CreationTimestamp.Time, decisionSpec(&ws), decisionState(&ws))
	log.V(1).Info("decided", "action", d.Action, "reason", d.Reason, "expiry", d.NewExpiry)

	if d.Action == ActionExpire {
		return ctrl.Result{}, r.expire(ctx, &ws, d.Reason)
	}

	pod, err := r.ensurePod(ctx, &ws)
	if err == nil {
		err = r.ensureService(ctx, &ws)
	}
	if err != nil {
		if errors.Is(err, errForeignChild) {
			return r.reportForeignChild(ctx, &ws, err)
		}
		return ctrl.Result{}, fmt.Errorf("ensuring children: %w", err)
	}
	ws.Status.Endpoint = endpointFor(&ws)

	// An event is a claim that something happened, so it is held until the write
	// that makes it true succeeds. Emitting first means a status update lost to a
	// conflict still announces a start or an extension that was then discarded —
	// and announces it again on every retry.
	var announce func()

	switch d.Action {
	case ActionInit:
		ws.Status.ExpiresAt = &metav1.Time{Time: d.NewExpiry}
		announce = func() {
			r.Recorder.Eventf(&ws, corev1.EventTypeNormal, "SessionCreated",
				"Workspace session started, expires at %s", d.NewExpiry.Format(time.RFC3339))
		}

	case ActionExtend:
		ws.Status.ExpiresAt = &metav1.Time{Time: d.NewExpiry}
		ws.Status.ExtensionsUsed++
		used, max := ws.Status.ExtensionsUsed, ws.Spec.MaxExtensions
		announce = func() {
			r.Recorder.Eventf(&ws, corev1.EventTypeNormal, "SessionExtended",
				"Extended to %s (%d of %d extensions used)",
				d.NewExpiry.Format(time.RFC3339), used, max)
		}

	case ActionWait:
		// Nothing to write beyond the readiness-derived phase below.

	case ActionExpire:
		// Handled above, before any child is touched.
	}

	r.applyReadiness(&ws, pod)

	if err := r.Status().Update(ctx, &ws); err != nil {
		// Deliberately no in-place conflict retry here. Returning the error
		// requeues the whole reconcile, which re-reads the object and re-runs
		// Decide against fresh state. Re-applying a decision computed from the
		// stale read could double-count an extension that another writer had
		// already recorded.
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}
	if announce != nil {
		announce()
	}
	return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
}

// applyReadiness derives phase and the Ready condition from the Pod.
func (r *WorkspaceSessionReconciler) applyReadiness(ws *workspacev1alpha1.WorkspaceSession, pod *corev1.Pod) {
	ready := podIsReady(pod)
	if ready {
		ws.Status.Phase = workspacev1alpha1.PhaseRunning
	} else {
		ws.Status.Phase = workspacev1alpha1.PhasePending
	}

	cond := metav1.Condition{
		Type:    workspacev1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  workspacev1alpha1.ReasonPodNotReady,
		Message: "Workspace pod is not ready",
	}
	if ready {
		cond.Status = metav1.ConditionTrue
		cond.Reason = workspacev1alpha1.ReasonPodReady
		cond.Message = "Workspace pod is ready"
	}
	meta.SetStatusCondition(&ws.Status.Conditions, cond)
}

// errForeignChild means a Pod or Service with this session's name already
// exists and is controlled by something else.
//
// Adopting it would make the session report healthy while backed by a workload
// it never created, and would then destroy that workload on expiry. Refusing is
// the only safe answer: a controller that can be made to delete resources it
// does not own by choosing a colliding name is destructive by default.
var errForeignChild = errors.New("a resource with this name exists and is not owned by this session")

// ensurePod creates the workspace Pod if absent and returns the live object.
// It is idempotent: a Pod deleted out from under a live session is recreated.
func (r *WorkspaceSessionReconciler) ensurePod(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) (*corev1.Pod, error) {
	key := types.NamespacedName{Name: ws.Name, Namespace: ws.Namespace}

	var pod corev1.Pod
	err := r.Get(ctx, key, &pod)
	if err == nil {
		if !metav1.IsControlledBy(&pod, ws) {
			return nil, fmt.Errorf("pod %s: %w", key, errForeignChild)
		}
		return &pod, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	desired := buildPod(ws)
	if err := controllerutil.SetControllerReference(ws, desired, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		// The cache had not caught up. Re-read rather than returning the
		// zero-valued Pod from the failed Get above, which would be reported as
		// not-ready and flap the session's phase back to Pending.
		if err := r.Get(ctx, key, &pod); err != nil {
			return nil, err
		}
		if !metav1.IsControlledBy(&pod, ws) {
			return nil, fmt.Errorf("pod %s: %w", key, errForeignChild)
		}
		return &pod, nil
	}
	return desired, nil
}

// reportForeignChild records the collision and stops, rather than failing the
// reconcile in a hot retry loop. The condition tells the operator exactly what
// is wrong; only a human renaming or removing the conflicting object fixes it.
func (r *WorkspaceSessionReconciler) reportForeignChild(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession, cause error) (ctrl.Result, error) {
	ws.Status.Phase = workspacev1alpha1.PhasePending
	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:    workspacev1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  workspacev1alpha1.ReasonChildNotOwned,
		Message: cause.Error(),
	})
	r.Recorder.Event(ws, corev1.EventTypeWarning, workspacev1alpha1.ReasonChildNotOwned, cause.Error())

	if err := r.Status().Update(ctx, ws); err != nil {
		return ctrl.Result{}, fmt.Errorf("reporting foreign child: %w", err)
	}
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// ensureService creates the workspace Service if absent.
func (r *WorkspaceSessionReconciler) ensureService(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) error {
	key := types.NamespacedName{Name: ws.Name, Namespace: ws.Namespace}

	var svc corev1.Service
	err := r.Get(ctx, key, &svc)
	if err == nil {
		if !metav1.IsControlledBy(&svc, ws) {
			return fmt.Errorf("service %s: %w", key, errForeignChild)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	desired := buildService(ws)
	if err := controllerutil.SetControllerReference(ws, desired, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func decisionSpec(ws *workspacev1alpha1.WorkspaceSession) DecisionSpec {
	return DecisionSpec{
		IdleTimeout:       ws.Spec.IdleTimeout.Duration,
		ExtensionDuration: ws.Spec.ExtensionDuration.Duration,
		MaxExtensions:     ws.Spec.MaxExtensions,
		HardTTL:           ws.Spec.HardTTL.Duration,
	}
}

func decisionState(ws *workspacev1alpha1.WorkspaceSession) DecisionState {
	st := DecisionState{ExtensionsUsed: ws.Status.ExtensionsUsed}
	if ws.Status.ExpiresAt != nil {
		t := ws.Status.ExpiresAt.Time
		st.ExpiresAt = &t
	}
	if ws.Status.LastActivityAt != nil {
		t := ws.Status.LastActivityAt.Time
		st.LastActivityAt = &t
	}
	return st
}

func (r *WorkspaceSessionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.WorkspaceSession{}).
		// Owning the Pod means a readiness flip triggers a reconcile at once,
		// so Pending -> Running is observed immediately rather than at the next
		// expiry wake-up.
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Named("workspacesession").
		Complete(r)
}

// expire tears the session down and records why.
//
// The WorkspaceSession object itself survives as a tombstone: `kubectl get ws`
// then shows which sessions ended and for what reason, which is worth far more
// when debugging than a resource that silently vanished.
func (r *WorkspaceSessionReconciler) expire(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession, reason string) error {
	if err := r.deleteChildren(ctx, ws); err != nil {
		return fmt.Errorf("deleting children: %w", err)
	}

	ws.Status.Phase = workspacev1alpha1.PhaseExpired
	ws.Status.ExpiresAt = &metav1.Time{Time: r.Clock.Now()}
	// The Service backing this endpoint has just been deleted, so continuing to
	// advertise the address would point callers at nothing.
	ws.Status.Endpoint = ""

	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:    workspacev1alpha1.ConditionExpired,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: expiryMessage(reason),
	})
	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:    workspacev1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: "Session has expired",
	})

	if err := r.Status().Update(ctx, ws); err != nil {
		return err
	}
	r.Recorder.Eventf(ws, corev1.EventTypeNormal, "SessionExpired",
		"Session expired: %s", expiryMessage(reason))
	return nil
}

func expiryMessage(reason string) string {
	switch reason {
	case ReasonIdleTimeout:
		return "No activity within the idle timeout"
	case ReasonExtensionsExhausted:
		return "All permitted extensions have been used"
	case ReasonHardTTLExceeded:
		return "Reached the hard TTL ceiling"
	default:
		return reason
	}
}

// deleteChildren issues the Service delete before the Pod delete.
//
// That ordering narrows, but does not close, the window in which a request
// reaches a terminating backend: EndpointSlice removal still has to propagate
// through the endpoint controller and every kube-proxy, which usually takes
// longer than the two API calls are apart. Closing it properly would need a
// drain wait between the deletes. Both tolerate NotFound so teardown is
// idempotent.
//
// Each delete is guarded twice: the object must be controlled by this session,
// and the delete carries a UID precondition so it cannot race into removing a
// replacement that appeared under the same name in between.
func (r *WorkspaceSessionReconciler) deleteChildren(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) error {
	key := types.NamespacedName{Name: ws.Name, Namespace: ws.Namespace}

	var svc corev1.Service
	switch err := r.Get(ctx, key, &svc); {
	case err == nil:
		if metav1.IsControlledBy(&svc, ws) {
			if err := r.Delete(ctx, &svc, client.Preconditions{UID: &svc.UID}); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	case !apierrors.IsNotFound(err):
		return err
	}

	var pod corev1.Pod
	switch err := r.Get(ctx, key, &pod); {
	case err == nil:
		if metav1.IsControlledBy(&pod, ws) {
			if err := r.Delete(ctx, &pod, client.Preconditions{UID: &pod.UID}); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	case !apierrors.IsNotFound(err):
		return err
	}
	return nil
}

// reconcileDelete runs ordered teardown behind the finalizer.
//
// OwnerReferences would already garbage-collect the Pod and Service, so this
// finalizer is not what makes cleanup happen. It buys two things GC does not:
// the Service delete is issued first, which narrows the window in which a
// request reaches a terminating backend; and there is a guaranteed window to
// mark the object Terminating and emit a final event before it disappears.
func (r *WorkspaceSessionReconciler) reconcileDelete(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(ws, workspacev1alpha1.TeardownFinalizer) {
		return ctrl.Result{}, nil
	}

	if ws.Status.Phase != workspacev1alpha1.PhaseTerminating {
		ws.Status.Phase = workspacev1alpha1.PhaseTerminating
		if err := r.Status().Update(ctx, ws); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{}, fmt.Errorf("marking terminating: %w", err)
		}
	}

	if err := r.deleteChildren(ctx, ws); err != nil {
		return ctrl.Result{}, fmt.Errorf("tearing down children: %w", err)
	}

	gone, err := r.childrenGone(ctx, ws)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !gone {
		// Children are still terminating. Come back rather than dropping the
		// finalizer early, which would let the object vanish mid-teardown.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	controllerutil.RemoveFinalizer(ws, workspacev1alpha1.TeardownFinalizer)
	if err := r.Update(ctx, ws); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	r.Recorder.Event(ws, corev1.EventTypeNormal, "SessionTornDown",
		"Workspace service and pod removed")
	return ctrl.Result{}, nil
}

// childrenGone reports whether both children have been released, meaning each
// is either absent or already marked for deletion.
//
// Accepting "marked for deletion" rather than waiting for the object to vanish
// is deliberate. Once the API server has accepted the delete, the Service has
// stopped routing and the Pod is terminating — which is everything the
// ordering guarantee needs. Waiting for final removal would make the finalizer
// depend on a kubelet confirming termination, so a session on an unreachable
// node would strand its WorkspaceSession indefinitely. A finalizer that can
// deadlock on an unhealthy node is worse than one that releases a moment early.
//
// It also means these specs run under envtest, which has no kubelet and no
// garbage collector: a pod deleted with the default grace period would keep its
// deletionTimestamp forever.
func (r *WorkspaceSessionReconciler) childrenGone(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) (bool, error) {
	key := types.NamespacedName{Name: ws.Name, Namespace: ws.Namespace}

	var svc corev1.Service
	switch err := r.Get(ctx, key, &svc); {
	case err == nil:
		// An object we never owned is not ours to wait on; blocking on it would
		// strand this session behind an unrelated workload's lifetime.
		if metav1.IsControlledBy(&svc, ws) && svc.DeletionTimestamp.IsZero() {
			return false, nil
		}
	case !apierrors.IsNotFound(err):
		return false, err
	}

	var pod corev1.Pod
	switch err := r.Get(ctx, key, &pod); {
	case err == nil:
		if metav1.IsControlledBy(&pod, ws) && pod.DeletionTimestamp.IsZero() {
			return false, nil
		}
	case !apierrors.IsNotFound(err):
		return false, err
	}

	return true, nil
}
