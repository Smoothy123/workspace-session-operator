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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

// sessionLabel ties a Pod and Service back to the session that owns them, and
// is what the Service selector matches on.
const sessionLabel = "workspace.smoothy.dev/session"

func childLabels(ws *workspacev1alpha1.WorkspaceSession) map[string]string {
	return map[string]string{
		sessionLabel:                   ws.Name,
		"app.kubernetes.io/managed-by": "workspace-session-operator",
		"app.kubernetes.io/name":       "workspace",
	}
}

// buildPod renders the workspace Pod. The Pod is never updated after creation —
// a workspace's image is fixed for the life of the session — so this is a pure
// constructor with no merge semantics to get wrong.
func buildPod(ws *workspacev1alpha1.WorkspaceSession) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ws.Name,
			Namespace: ws.Namespace,
			Labels:    childLabels(ws),
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{{
				Name:  "workspace",
				Image: ws.Spec.Image,
				Ports: []corev1.ContainerPort{{
					Name:          "http",
					ContainerPort: ws.Spec.Port,
				}},
			}},
		},
	}
}

// buildService renders the ClusterIP Service fronting the workspace Pod.
func buildService(ws *workspacev1alpha1.WorkspaceSession) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ws.Name,
			Namespace: ws.Namespace,
			Labels:    childLabels(ws),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{sessionLabel: ws.Name},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       ws.Spec.Port,
				TargetPort: intstr.FromInt32(ws.Spec.Port),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

// endpointFor is the in-cluster address reported in status.
func endpointFor(ws *workspacev1alpha1.WorkspaceSession) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", ws.Name, ws.Namespace, ws.Spec.Port)
}

// podIsReady reports whether the Pod carries a Ready condition set to True.
func podIsReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
