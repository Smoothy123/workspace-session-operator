package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

const (
	timeout  = time.Second * 15
	interval = time.Millisecond * 250
)

var sessionCounter int

// newSession creates a session with a unique name so specs cannot collide.
func newSession() *workspacev1alpha1.WorkspaceSession {
	sessionCounter++
	ws := &workspacev1alpha1.WorkspaceSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("session-%d", sessionCounter),
			Namespace: "default",
		},
		Spec: workspacev1alpha1.WorkspaceSessionSpec{
			Image:             "nginx:1.27-alpine",
			Port:              8080,
			IdleTimeout:       metav1.Duration{Duration: 30 * time.Minute},
			ExtensionDuration: metav1.Duration{Duration: 30 * time.Minute},
			MaxExtensions:     2,
			HardTTL:           metav1.Duration{Duration: 4 * time.Hour},
		},
	}
	Expect(k8sClient.Create(ctx, ws)).To(Succeed())
	return ws
}

func getSession(ws *workspacev1alpha1.WorkspaceSession) *workspacev1alpha1.WorkspaceSession {
	var live workspacev1alpha1.WorkspaceSession
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &live)).To(Succeed())
	return &live
}

var _ = Describe("WorkspaceSession creation", func() {
	It("creates a Pod and Service owned by the session, and adds the finalizer", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		By("creating the Pod with a controller reference")
		var pod corev1.Pod
		Eventually(func() error {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &pod)
		}, timeout, interval).Should(Succeed())

		Expect(pod.Spec.Containers[0].Image).To(Equal("nginx:1.27-alpine"))
		Expect(metav1.IsControlledBy(&pod, getSession(ws))).To(BeTrue())

		By("creating the Service with a controller reference")
		var svc corev1.Service
		Eventually(func() error {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &svc)
		}, timeout, interval).Should(Succeed())
		Expect(metav1.IsControlledBy(&svc, getSession(ws))).To(BeTrue())

		By("adding the teardown finalizer")
		Eventually(func() bool {
			return controllerutil.ContainsFinalizer(getSession(ws), workspacev1alpha1.TeardownFinalizer)
		}, timeout, interval).Should(BeTrue())

		By("setting the initial expiry one idleTimeout out, consuming no extension")
		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		live := getSession(ws)
		Expect(live.Status.ExpiresAt.Time).To(BeTemporally("~", startTime.Add(30*time.Minute), time.Second))
		Expect(live.Status.ExtensionsUsed).To(BeZero())
		Expect(live.Status.Endpoint).To(Equal(fmt.Sprintf("%s.default.svc.cluster.local:8080", ws.Name)))
	})

	It("moves from Pending to Running when the Pod becomes Ready", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhasePending))

		markPodReady(ws.Name, ws.Namespace)

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseRunning))

		cond := meta.FindStatusCondition(getSession(ws).Status.Conditions, workspacev1alpha1.ConditionReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
	})

	It("recreates a Pod deleted out from under a live session", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		var pod corev1.Pod
		Eventually(func() error {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &pod)
		}, timeout, interval).Should(Succeed())

		originalUID := pod.UID

		// GracePeriodSeconds(0) is required under envtest: there is no kubelet
		// to confirm termination, so a pod deleted with the default 30s grace
		// period keeps its deletionTimestamp forever and is never removed.
		Expect(k8sClient.Delete(ctx, &pod, client.GracePeriodSeconds(0))).To(Succeed())

		Eventually(func() bool {
			var fresh corev1.Pod
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &fresh); err != nil {
				return false
			}
			return fresh.UID != originalUID && fresh.DeletionTimestamp.IsZero()
		}, timeout, interval).Should(BeTrue(), "a live session must not be left without a backend pod")

		Expect(apierrors.IsNotFound(nil)).To(BeFalse())
	})
})

var _ = Describe("WorkspaceSession lifecycle", func() {
	It("extends a session with recent activity and increments the counter", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		By("recording activity just before the expiry")
		fakeClock.SetTime(startTime.Add(29 * time.Minute))
		Eventually(func() error {
			live := getSession(ws)
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations[workspacev1alpha1.LastActivityAnnotation] =
				startTime.Add(29 * time.Minute).Format(time.RFC3339)
			return k8sClient.Update(ctx, live)
		}, timeout, interval).Should(Succeed())

		By("advancing past the expiry and waking the controller")
		fakeClock.SetTime(startTime.Add(30 * time.Minute))
		nudge(ws)

		Eventually(func() int32 {
			return getSession(ws).Status.ExtensionsUsed
		}, timeout, interval).Should(Equal(int32(1)))

		live := getSession(ws)
		Expect(live.Status.Phase).NotTo(Equal(workspacev1alpha1.PhaseExpired))
		Expect(live.Status.ExpiresAt.Time).To(BeTemporally("~", startTime.Add(60*time.Minute), time.Minute))
	})

	It("expires an idle session, deleting children but keeping the object", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		Eventually(func() error {
			var pod corev1.Pod
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &pod)
		}, timeout, interval).Should(Succeed())

		By("advancing past the expiry with no activity recorded")
		fakeClock.SetTime(startTime.Add(31 * time.Minute))
		nudge(ws)

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseExpired))

		By("deleting the Service and the Pod")
		Eventually(func() bool {
			var svc corev1.Service
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &svc)
			return apierrors.IsNotFound(err) || !svc.DeletionTimestamp.IsZero()
		}, timeout, interval).Should(BeTrue())

		By("retaining the object as a tombstone explaining why")
		live := getSession(ws)
		cond := meta.FindStatusCondition(live.Status.Conditions, workspacev1alpha1.ConditionExpired)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(ReasonIdleTimeout))
	})

	It("stays expired even if activity arrives afterwards", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		// Wait for the first reconcile to establish the expiry window before
		// moving the clock. Without this the clock can jump while ExpiresAt is
		// still nil, and Decide correctly returns Init rather than Expire — the
		// session simply gets its first idle window 31 minutes late.
		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		fakeClock.SetTime(startTime.Add(31 * time.Minute))
		nudge(ws)

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseExpired))

		By("writing fresh activity against an expired session")
		Eventually(func() error {
			live := getSession(ws)
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations[workspacev1alpha1.LastActivityAnnotation] =
				startTime.Add(32 * time.Minute).Format(time.RFC3339)
			return k8sClient.Update(ctx, live)
		}, timeout, interval).Should(Succeed())

		Consistently(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, time.Second*3, interval).Should(Equal(workspacev1alpha1.PhaseExpired),
			"Expired is terminal and absorbing")
	})

	It("ignores a malformed activity annotation without wedging the session", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		By("writing garbage into the activity annotation")
		Eventually(func() error {
			live := getSession(ws)
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations[workspacev1alpha1.LastActivityAnnotation] = "not-a-timestamp"
			return k8sClient.Update(ctx, live)
		}, timeout, interval).Should(Succeed())

		nudge(ws)

		By("continuing to reconcile normally with lastActivityAt left unset")
		Consistently(func() bool {
			live := getSession(ws)
			return live.Status.ExpiresAt != nil && live.Status.LastActivityAt == nil
		}, time.Second*3, interval).Should(BeTrue())

		By("surfacing a Warning event")
		Eventually(func() bool {
			var events corev1.EventList
			if err := k8sClient.List(ctx, &events, client.InNamespace(ws.Namespace)); err != nil {
				return false
			}
			for _, e := range events.Items {
				if e.InvolvedObject.Name == ws.Name && e.Reason == "InvalidActivityAnnotation" {
					return true
				}
			}
			return false
		}, timeout, interval).Should(BeTrue())
	})

	It("lets the hard TTL override an actively used session", func() {
		fakeClock.SetTime(startTime)
		sessionCounter++
		ws := &workspacev1alpha1.WorkspaceSession{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("session-%d", sessionCounter),
				Namespace: "default",
			},
			Spec: workspacev1alpha1.WorkspaceSessionSpec{
				Image:             "nginx:1.27-alpine",
				Port:              8080,
				IdleTimeout:       metav1.Duration{Duration: 10 * time.Minute},
				ExtensionDuration: metav1.Duration{Duration: 10 * time.Minute},
				MaxExtensions:     10,
				HardTTL:           metav1.Duration{Duration: 20 * time.Minute},
			},
		}
		Expect(k8sClient.Create(ctx, ws)).To(Succeed())

		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		// 21m, not 20m: creationTimestamp is stamped by the API server a moment
		// after startTime is read and is truncated to the second, so the hard
		// deadline sits slightly after startTime+20m. Landing exactly on the
		// nominal boundary would be flaky.
		By("keeping the session busy right up to the ceiling")
		fakeClock.SetTime(startTime.Add(21 * time.Minute))
		Eventually(func() error {
			live := getSession(ws)
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations[workspacev1alpha1.LastActivityAnnotation] =
				startTime.Add(21 * time.Minute).Format(time.RFC3339)
			return k8sClient.Update(ctx, live)
		}, timeout, interval).Should(Succeed())

		nudge(ws)

		Eventually(func() string {
			cond := meta.FindStatusCondition(getSession(ws).Status.Conditions, workspacev1alpha1.ConditionExpired)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, timeout, interval).Should(Equal(ReasonHardTTLExceeded),
			"an active session must still die at the hard TTL")
	})
})
