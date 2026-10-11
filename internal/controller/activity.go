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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

// parseActivity reads the activity annotation. It returns the parsed time, a
// flag for whether the annotation was present at all, and a parse error.
//
// Present-but-unparseable and absent are deliberately distinct: the former
// deserves a Warning event telling the operator their gateway is writing
// garbage, the latter is the ordinary state of a session nobody has touched.
func parseActivity(ws *workspacev1alpha1.WorkspaceSession) (time.Time, bool, error) {
	raw, ok := ws.Annotations[workspacev1alpha1.LastActivityAnnotation]
	if !ok || raw == "" {
		return time.Time{}, false, nil
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, true, fmt.Errorf("parsing %q as RFC3339: %w", raw, err)
	}
	return t, true, nil
}

// applyActivity moves status.lastActivityAt forward to t, and reports whether
// it changed anything.
//
// The comparison is what makes activity monotonic. A gateway retrying an old
// request, or two gateways racing, can write an older timestamp than one
// already recorded; letting that win would shorten a session the user is
// actively using.
func applyActivity(ws *workspacev1alpha1.WorkspaceSession, t time.Time) bool {
	if ws.Status.LastActivityAt != nil && !t.After(ws.Status.LastActivityAt.Time) {
		return false
	}
	ws.Status.LastActivityAt = &metav1.Time{Time: t}
	return true
}

// maxActivitySkew is how far ahead of the controller's clock a gateway-supplied
// timestamp may sit before it is treated as bogus rather than as clock jitter.
// A minute absorbs ordinary NTP drift between two machines without believing a
// writer whose clock is hours out.
const maxActivitySkew = time.Minute

// activityIsPlausible reports whether a supplied timestamp can be believed.
//
// Activity arrives from an external writer, so its clock is not ours to trust.
// A timestamp hours in the future makes a session look permanently active:
// every idle check sees activity "moments ago", so it extends all the way to
// maxExtensions having never been used — exactly what the idle timeout exists
// to prevent.
//
// Implausible timestamps are REJECTED rather than clamped to now. Clamping
// looks gentler but is worse: the annotation is a level, not an edge, so the
// controller re-reads the same value on every reconcile. A clamped value would
// be refreshed to "now" each time, and a single bogus write would keep the
// session alive forever — the very failure being guarded against. Refusing to
// believe the timestamp leaves lastActivityAt where it was, and the session
// expires on schedule.
func activityIsPlausible(t, now time.Time) bool {
	return !t.After(now.Add(maxActivitySkew))
}

// syncActivity mirrors the annotation into status.
//
// A malformed annotation is logged and surfaced as a Warning event, then
// ignored. It must never fail the reconcile: a buggy gateway writing garbage
// would otherwise strand every workspace it touches, which is a far worse
// failure than an un-extended session.
func (r *WorkspaceSessionReconciler) syncActivity(ctx context.Context, ws *workspacev1alpha1.WorkspaceSession) {
	t, found, err := parseActivity(ws)
	if !found {
		return
	}
	if err != nil {
		logf.FromContext(ctx).Error(err, "ignoring malformed activity annotation")
		r.Recorder.Eventf(ws, nil, corev1.EventTypeWarning, "InvalidActivityAnnotation", "Ignoring",
			"Ignoring %s: %v", workspacev1alpha1.LastActivityAnnotation, err)
		return
	}

	if now := r.Clock.Now(); !activityIsPlausible(t, now) {
		logf.FromContext(ctx).Info("ignoring implausible future-dated activity",
			"supplied", t, "controllerClock", now)
		r.Recorder.Eventf(ws, nil, corev1.EventTypeWarning, "ActivityClockSkew", "Ignoring",
			"Ignoring activity timestamp %s: it is ahead of the controller clock (%s)",
			t.Format(time.RFC3339), now.Format(time.RFC3339))
		return
	}

	applyActivity(ws, t)
}
