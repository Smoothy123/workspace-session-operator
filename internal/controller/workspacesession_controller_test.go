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
