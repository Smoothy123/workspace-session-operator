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
	"fmt"
	"sync"
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

		By("no longer advertising an endpoint that has been torn down")
		Expect(live.Status.Endpoint).To(BeEmpty(),
			"an expired session still advertises a Service that no longer exists")
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

var _ = Describe("WorkspaceSession deletion", func() {
	It("tears down children and releases the finalizer", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		Eventually(func() error {
			var pod corev1.Pod
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &pod)
		}, timeout, interval).Should(Succeed())

		Eventually(func() bool {
			return controllerutil.ContainsFinalizer(getSession(ws), workspacev1alpha1.TeardownFinalizer)
		}, timeout, interval).Should(BeTrue())

		By("deleting the session")
		Expect(k8sClient.Delete(ctx, getSession(ws))).To(Succeed())

		By("removing the object entirely once teardown completes")
		Eventually(func() bool {
			var live workspacev1alpha1.WorkspaceSession
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &live)
			return apierrors.IsNotFound(err)
		}, timeout, interval).Should(BeTrue(), "the finalizer must not strand the object")

		By("leaving no Service behind")
		Eventually(func() bool {
			var svc corev1.Service
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &svc)
			return apierrors.IsNotFound(err) || !svc.DeletionTimestamp.IsZero()
		}, timeout, interval).Should(BeTrue())
	})

	It("deletes an already-expired session without stranding it", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		// Same gate as the lifecycle specs: let the first reconcile establish
		// the expiry window before moving the clock, otherwise Decide sees a nil
		// ExpiresAt and returns Init rather than Expire.
		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		fakeClock.SetTime(startTime.Add(31 * time.Minute))
		nudge(ws)

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseExpired))

		By("deleting a tombstone whose children are already gone")
		Expect(k8sClient.Delete(ctx, getSession(ws))).To(Succeed())

		Eventually(func() bool {
			var live workspacev1alpha1.WorkspaceSession
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &live)
			return apierrors.IsNotFound(err)
		}, timeout, interval).Should(BeTrue(), "teardown must be idempotent")
	})
})

var _ = Describe("WorkspaceSession name collisions", func() {
	// A WorkspaceSession must never adopt — and must never delete — a Pod or
	// Service that merely shares its name. Adopting one makes the session
	// report healthy while backed by a workload it did not create; deleting one
	// destroys an unrelated user's workload on expiry.
	It("refuses to adopt or delete a Pod and Service it does not own", func() {
		fakeClock.SetTime(startTime)
		const name = "collide"

		By("planting unrelated objects that happen to share the session's name")
		foreignSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "production"},
				Ports:    []corev1.ServicePort{{Name: "pg", Port: 9999}},
			},
		}
		Expect(k8sClient.Create(ctx, foreignSvc)).To(Succeed())

		foreignPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "db", Image: "postgres:16"}},
			},
		}
		Expect(k8sClient.Create(ctx, foreignPod)).To(Succeed())

		svcUID, podUID := foreignSvc.UID, foreignPod.UID

		By("creating a session with the colliding name")
		ws := &workspacev1alpha1.WorkspaceSession{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
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

		By("reporting the collision instead of adopting")
		Eventually(func() string {
			c := meta.FindStatusCondition(getSession(ws).Status.Conditions, workspacev1alpha1.ConditionReady)
			if c == nil {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(Equal(workspacev1alpha1.ReasonChildNotOwned))

		By("leaving the foreign objects untouched")
		var svc corev1.Service
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignSvc), &svc)).To(Succeed())
		Expect(svc.UID).To(Equal(svcUID))
		Expect(svc.Spec.Ports[0].Port).To(Equal(int32(9999)), "the foreign Service was mutated")
		Expect(svc.OwnerReferences).To(BeEmpty(), "the foreign Service was adopted")

		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignPod), &pod)).To(Succeed())
		Expect(pod.UID).To(Equal(podUID))
		Expect(pod.Spec.Containers[0].Image).To(Equal("postgres:16"))
		Expect(pod.OwnerReferences).To(BeEmpty(), "the foreign Pod was adopted")

		By("expiring the session without destroying them")
		fakeClock.SetTime(startTime.Add(5 * time.Hour))
		nudge(ws)
		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseExpired))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignSvc), &svc)).To(Succeed())
		Expect(svc.DeletionTimestamp.IsZero()).To(BeTrue(), "expiry deleted a foreign Service")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignPod), &pod)).To(Succeed())
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue(), "expiry deleted a foreign Pod")

		By("deleting the session without destroying them either")
		Expect(k8sClient.Delete(ctx, getSession(ws))).To(Succeed())
		Eventually(func() bool {
			var live workspacev1alpha1.WorkspaceSession
			return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &live))
		}, timeout, interval).Should(BeTrue())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignSvc), &svc)).To(Succeed())
		Expect(svc.DeletionTimestamp.IsZero()).To(BeTrue(), "teardown deleted a foreign Service")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignPod), &pod)).To(Succeed())
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue(), "teardown deleted a foreign Pod")

		Expect(k8sClient.Delete(ctx, &svc, client.GracePeriodSeconds(0))).To(Succeed())
		Expect(k8sClient.Delete(ctx, &pod, client.GracePeriodSeconds(0))).To(Succeed())
	})
})

var _ = Describe("WorkspaceSession already past its ceiling", func() {
	// Ensuring children before deciding means a session that is already dead on
	// arrival still gets a Pod scheduled and an image pull started, only to be
	// deleted microseconds later on the same reconcile.
	It("creates no children for a session already past its hard TTL", func() {
		// Clock ahead BEFORE the session exists, so its very first reconcile
		// already sees it as past the ceiling.
		fakeClock.SetTime(startTime.Add(5 * time.Hour))

		sessionCounter++
		name := fmt.Sprintf("session-%d", sessionCounter)
		key := client.ObjectKey{Name: name, Namespace: "default"}

		// The Pod would exist for only milliseconds, so watch for it from before
		// the session is created. Polling after the fact cannot see the window.
		sawPod := make(chan struct{}, 1)
		stop := make(chan struct{})
		watching := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			close(watching)
			for {
				select {
				case <-stop:
					return
				default:
				}
				var pod corev1.Pod
				if err := k8sClient.Get(ctx, key, &pod); err == nil {
					select {
					case sawPod <- struct{}{}:
					default:
					}
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
		<-watching

		ws := &workspacev1alpha1.WorkspaceSession{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
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

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseExpired))

		close(stop)
		Expect(sawPod).NotTo(Receive(),
			"a session that is dead on arrival must not schedule a workspace pod")

		var svc corev1.Service
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &svc))).To(BeTrue())
	})
})

var _ = Describe("WorkspaceSession event honesty", func() {
	// An event is a claim about something that happened. Emitting SessionCreated
	// before the status write that records the expiry means the controller can
	// announce a start whose record was then discarded by a conflict — and
	// announce it again on every retry.
	It("announces a session start exactly once, even under write contention", func() {
		fakeClock.SetTime(startTime)

		sessionCounter++
		name := fmt.Sprintf("session-%d", sessionCounter)
		ws := &workspacev1alpha1.WorkspaceSession{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
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

		By("racing the first reconcile with concurrent metadata writes")
		var wg sync.WaitGroup
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func(n int) {
				defer GinkgoRecover()
				defer wg.Done()
				var live workspacev1alpha1.WorkspaceSession
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &live); err != nil {
					return
				}
				if live.Labels == nil {
					live.Labels = map[string]string{}
				}
				live.Labels["contend"] = fmt.Sprintf("%d", n)
				_ = k8sClient.Update(ctx, &live)
			}(i)
		}
		wg.Wait()

		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		By("counting SessionCreated announcements, including event aggregation")
		Consistently(func() int32 {
			var events corev1.EventList
			if err := k8sClient.List(ctx, &events, client.InNamespace("default")); err != nil {
				return -1
			}
			var total int32
			for _, e := range events.Items {
				if e.InvolvedObject.Name == name && e.Reason == "SessionCreated" {
					if e.Count == 0 {
						total++
					} else {
						total += e.Count
					}
				}
			}
			return total
		}, time.Second*5, time.Millisecond*500).Should(Equal(int32(1)),
			"the controller announced a session start more than once")
	})
})

var _ = Describe("WorkspaceSession gateway clock skew", func() {
	// Activity comes from an external writer whose clock is not ours to trust.
	// An unclamped future timestamp makes the session look permanently active,
	// so it extends to its cap having never been used.
	It("does not let a clock-skewed gateway keep a session alive", func() {
		fakeClock.SetTime(startTime)
		ws := newSession()

		Eventually(func() *metav1.Time {
			return getSession(ws).Status.ExpiresAt
		}, timeout, interval).ShouldNot(BeNil())

		By("a gateway 72 hours ahead writing a single activity timestamp")
		Eventually(func() error {
			live := getSession(ws)
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations[workspacev1alpha1.LastActivityAnnotation] =
				startTime.Add(72 * time.Hour).Format(time.RFC3339)
			return k8sClient.Update(ctx, live)
		}, timeout, interval).Should(Succeed())

		By("advancing past the idle window with no further activity")
		fakeClock.SetTime(startTime.Add(31 * time.Minute))
		nudge(ws)

		Eventually(func() workspacev1alpha1.WorkspaceSessionPhase {
			return getSession(ws).Status.Phase
		}, timeout, interval).Should(Equal(workspacev1alpha1.PhaseExpired),
			"a future-dated timestamp kept an unused session alive")

		live := getSession(ws)
		Expect(live.Status.ExtensionsUsed).To(BeZero(),
			"the session spent an extension it never earned")
		cond := meta.FindStatusCondition(live.Status.Conditions, workspacev1alpha1.ConditionExpired)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(ReasonIdleTimeout))
	})
})
