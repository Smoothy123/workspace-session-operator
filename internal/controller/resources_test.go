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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

func sampleSession() *workspacev1alpha1.WorkspaceSession {
	return &workspacev1alpha1.WorkspaceSession{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "dev"},
		Spec: workspacev1alpha1.WorkspaceSessionSpec{
			Image: "nginx:1.27-alpine",
			Port:  8080,
		},
	}
}

func TestBuildPodUsesSpecImageAndPort(t *testing.T) {
	pod := buildPod(sampleSession())

	if pod.Name != "demo" || pod.Namespace != "dev" {
		t.Fatalf("got %s/%s, want dev/demo", pod.Namespace, pod.Name)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("got %d containers, want 1", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if c.Image != "nginx:1.27-alpine" {
		t.Errorf("image: got %q", c.Image)
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 8080 {
		t.Errorf("ports: got %+v, want one port 8080", c.Ports)
	}
}

func TestBuildPodCarriesSelectorLabels(t *testing.T) {
	pod := buildPod(sampleSession())
	if pod.Labels["workspace.smoothy.dev/session"] != "demo" {
		t.Errorf("labels: got %+v", pod.Labels)
	}
}

func TestBuildServiceSelectsThePod(t *testing.T) {
	ws := sampleSession()
	svc := buildService(ws)
	pod := buildPod(ws)

	for k, v := range svc.Spec.Selector {
		if pod.Labels[k] != v {
			t.Fatalf("selector %s=%s does not match pod labels %+v", k, v, pod.Labels)
		}
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("ports: got %+v", svc.Spec.Ports)
	}
}

func TestEndpointForIsClusterDNS(t *testing.T) {
	got := endpointFor(sampleSession())
	want := "demo.dev.svc.cluster.local:8080"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPodIsReady(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"nil pod", nil, false},
		{"no conditions", &corev1.Pod{}, false},
		{
			"ready true",
			&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}}},
			true,
		},
		{
			"ready false",
			&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			}}},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podIsReady(tc.pod); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
