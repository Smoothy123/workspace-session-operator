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

	pod, err := r.ensurePod(ctx, &ws)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring pod: %w", err)
	}
	if err := r.ensureService(ctx, &ws); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring service: %w", err)
	}
	ws.Status.Endpoint = endpointFor(&ws)

	d := Decide(r.Clock.Now(), ws.CreationTimestamp.Time, decisionSpec(&ws), decisionState(&ws))
	log.V(1).Info("decided", "action", d.Action, "reason", d.Reason, "expiry", d.NewExpiry)

	switch d.Action {
	case ActionExpire:
		return ctrl.Result{}, r.expire(ctx, &ws, d.Reason)

	case ActionInit:
		ws.Status.ExpiresAt = &metav1.Time{Time: d.NewExpiry}
		r.Recorder.Eventf(&ws, corev1.EventTypeNormal, "SessionCreated",
			"Workspace session started, expires at %s", d.NewExpiry.Format(time.RFC3339))

	case ActionExtend:
		ws.Status.ExpiresAt = &metav1.Time{Time: d.NewExpiry}
		ws.Status.ExtensionsUsed++
		r.Recorder.Eventf(&ws, corev1.EventTypeNormal, "SessionExtended",
			"Extended to %s (%d of %d extensions used)",
			d.NewExpiry.Format(time.RFC3339), ws.Status.ExtensionsUsed, ws.Spec.MaxExtensions)

	case ActionWait:
		// Nothing to write beyond the readiness-derived phase below.
	}

	r.applyReadiness(&ws, pod)

	if err := r.Status().Update(ctx, &ws); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
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

// ensurePod creates the workspace Pod if absent and returns the live object.
// It is idempotent: a Pod deleted out from under a live session is recreated.
func (r *WorkspaceSessionReconciler) ensurePod(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) (*corev1.Pod, error) {
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Name: ws.Name, Namespace: ws.Namespace}, &pod)
	if err == nil {
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
		if apierrors.IsAlreadyExists(err) {
			return &pod, nil
		}
		return nil, err
	}
	return desired, nil
}

// ensureService creates the workspace Service if absent.
func (r *WorkspaceSessionReconciler) ensureService(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) error {
	var svc corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: ws.Name, Namespace: ws.Namespace}, &svc)
	if err == nil {
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

	r.Recorder.Eventf(ws, corev1.EventTypeNormal, "SessionExpired",
		"Session expired: %s", expiryMessage(reason))

	return r.Status().Update(ctx, ws)
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

// deleteChildren removes the Service before the Pod, so the Service stops
// routing before the backend begins terminating and no request lands on a
// dying pod. Both deletes tolerate NotFound so teardown is idempotent.
func (r *WorkspaceSessionReconciler) deleteChildren(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: ws.Name, Namespace: ws.Namespace}}
	if err := r.Delete(ctx, svc); client.IgnoreNotFound(err) != nil {
		return err
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: ws.Name, Namespace: ws.Namespace}}
	if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
		return err
	}
	return nil
}

// reconcileDelete runs ordered teardown behind the finalizer.
//
// OwnerReferences would already garbage-collect the Pod and Service, so this
// finalizer is not what makes cleanup happen. It buys two things GC does not:
// the Service is withdrawn before the Pod begins terminating, so no request
// lands on a dying backend; and there is a guaranteed window to mark the object
// Terminating and emit a final event before it disappears.
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

	r.Recorder.Event(ws, corev1.EventTypeNormal, "SessionTornDown",
		"Workspace service and pod removed")

	controllerutil.RemoveFinalizer(ws, workspacev1alpha1.TeardownFinalizer)
	if err := r.Update(ctx, ws); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
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
		if svc.DeletionTimestamp.IsZero() {
			return false, nil
		}
	case !apierrors.IsNotFound(err):
		return false, err
	}

	var pod corev1.Pod
	switch err := r.Get(ctx, key, &pod); {
	case err == nil:
		if pod.DeletionTimestamp.IsZero() {
			return false, nil
		}
	case !apierrors.IsNotFound(err):
		return false, err
	}

	return true, nil
}
