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
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	testclock "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

var (
	cfg       *rest.Config
	k8sClient client.Client
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc

	// fakeClock drives every expiry decision in these tests. Advancing it is how
	// a test reaches an expiry without waiting real time.
	fakeClock *testclock.FakePassiveClock

	// startTime anchors the fake clock. It MUST be close to real wall time.
	//
	// Decide() mixes two time sources: now comes from the injected clock, but
	// createdAt comes from ws.CreationTimestamp, which the API server stamps
	// with real time and which no fake clock can influence. Anchoring the fake
	// clock far from real time (say 2026-01-01) would put every hard deadline
	// months away from every now, and no hard-TTL assertion would ever fire.
	// Assigned in BeforeSuite.
	startTime time.Time
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: os.Getenv("KUBEBUILDER_ASSETS"),
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	Expect(workspacev1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	// Truncated to the second because creationTimestamp has second granularity.
	startTime = time.Now().UTC().Truncate(time.Second)
	fakeClock = testclock.NewFakePassiveClock(startTime)

	Expect((&WorkspaceSessionReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("test"),
		Clock:    fakeClock,
	}).SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()
})

var _ = AfterSuite(func() {
	cancel()
	Expect(testEnv.Stop()).To(Succeed())
})

// nudge forces a reconcile. Advancing the fake clock does not wake the
// controller on its own: the pending RequeueAfter was scheduled against real
// time. Tests that depend on the clock moving advance it, then call nudge.
func nudge(ws *workspacev1alpha1.WorkspaceSession) {
	Eventually(func() error {
		var live workspacev1alpha1.WorkspaceSession
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), &live); err != nil {
			return err
		}
		if live.Labels == nil {
			live.Labels = map[string]string{}
		}
		live.Labels["test.nudge"] = time.Now().Format("150405.000000000")
		return k8sClient.Update(ctx, &live)
	}, time.Second*10, time.Millisecond*200).Should(Succeed())
}

// markPodReady sets the Pod's Ready condition, which envtest will not do for us
// because there is no kubelet.
func markPodReady(name, namespace string) {
	Eventually(func() error {
		var pod corev1.Pod
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &pod); err != nil {
			return err
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}
		return k8sClient.Status().Update(ctx, &pod)
	}, time.Second*10, time.Millisecond*200).Should(Succeed())
}
