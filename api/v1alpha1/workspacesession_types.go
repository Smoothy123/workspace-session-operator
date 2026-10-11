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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// LastActivityAnnotation carries an RFC3339 timestamp of the most recent
	// activity against the workspace. An external session gateway writes it;
	// the controller only reads it. This annotation is the single seam between
	// traffic observation and lifecycle management.
	LastActivityAnnotation = "workspace.smoothy.dev/last-activity"

	// TeardownFinalizer guarantees the Service is withdrawn before the Pod is
	// deleted, and that terminal status is written before the object vanishes.
	// Note that ownerReferences, not this finalizer, perform garbage collection.
	TeardownFinalizer = "workspace.smoothy.dev/teardown"
)

// Condition types reported in status.conditions.
const (
	ConditionReady   = "Ready"
	ConditionExpired = "Expired"
)

// Condition reasons owned by this package. The expiry reasons
// (IdleTimeout, ExtensionsExhausted, HardTTLExceeded) deliberately live in
// internal/controller alongside the logic that produces them, so there is
// exactly one definition of each string.
const (
	ReasonPodReady    = "PodReady"
	ReasonPodNotReady = "PodNotReady"
)

// WorkspaceSessionPhase summarises where a session sits in its lifecycle.
// +kubebuilder:validation:Enum=Pending;Running;Expired;Terminating
type WorkspaceSessionPhase string

const (
	// PhasePending means the children exist but the Pod is not yet Ready.
	PhasePending WorkspaceSessionPhase = "Pending"
	// PhaseRunning means the Pod is Ready and the session is live.
	PhaseRunning WorkspaceSessionPhase = "Running"
	// PhaseExpired is terminal and absorbing: children have been deleted and
	// the object is retained as a tombstone explaining why the session ended.
	PhaseExpired WorkspaceSessionPhase = "Expired"
	// PhaseTerminating means deletion was requested and the finalizer is running.
	PhaseTerminating WorkspaceSessionPhase = "Terminating"
)

// WorkspaceSessionSpec defines the desired lifetime policy of a workspace.
type WorkspaceSessionSpec struct {
	// Image is the container image backing the workspace.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Port is the port the workspace container listens on.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=8080
	// +optional
	Port int32 `json:"port,omitempty"`

	// IdleTimeout is how long a session may go without activity before it
	// becomes eligible to expire.
	// +kubebuilder:default="30m"
	// +optional
	IdleTimeout metav1.Duration `json:"idleTimeout,omitempty"`

	// ExtensionDuration is the additional lifetime granted by each extension.
	// +kubebuilder:default="30m"
	// +optional
	ExtensionDuration metav1.Duration `json:"extensionDuration,omitempty"`

	// MaxExtensions caps how many times a session may be extended. Zero means
	// the session expires at its first idle check and is never extended.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	// +optional
	MaxExtensions int32 `json:"maxExtensions,omitempty"`

	// HardTTL is the absolute lifetime ceiling measured from creationTimestamp.
	// No extension can carry a session past it.
	// +kubebuilder:default="4h"
	// +optional
	HardTTL metav1.Duration `json:"hardTTL,omitempty"`
}

// WorkspaceSessionStatus reports observed lifecycle state.
type WorkspaceSessionStatus struct {
	// Phase is a coarse summary of lifecycle state.
	// +optional
	Phase WorkspaceSessionPhase `json:"phase,omitempty"`

	// ExpiresAt is when the controller will next wake and decide this session's
	// fate. It is always clamped to creationTimestamp + hardTTL.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// LastActivityAt mirrors the most recent valid activity annotation. It only
	// ever moves forward.
	// +optional
	LastActivityAt *metav1.Time `json:"lastActivityAt,omitempty"`

	// ExtensionsUsed counts extensions granted so far. Establishing the initial
	// expiry window does not count as an extension.
	//
	// Deliberately not omitempty: "extended zero times" is real information and
	// differs from "unknown". With omitempty the value is dropped from JSON and
	// the EXT printer column renders blank instead of 0.
	// +optional
	ExtensionsUsed int32 `json:"extensionsUsed"`

	// Endpoint is the in-cluster address of the workspace Service.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Conditions holds Ready and Expired.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ws
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`
// +kubebuilder:printcolumn:name="Expires",type=string,JSONPath=`.status.expiresAt`
// +kubebuilder:printcolumn:name="Ext",type=integer,JSONPath=`.status.extensionsUsed`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// WorkspaceSession is an ephemeral developer workspace with a self-enforcing
// lifetime.
type WorkspaceSession struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkspaceSessionSpec   `json:"spec,omitempty"`
	Status WorkspaceSessionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkspaceSessionList contains a list of WorkspaceSession.
type WorkspaceSessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkspaceSession `json:"items"`
}

func init() {
	// kubebuilder v4.16 scaffolds SchemeBuilder as a runtime.SchemeBuilder,
	// whose Register takes registration functions rather than objects.
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &WorkspaceSession{}, &WorkspaceSessionList{})
		return nil
	})
}
