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

import "time"

// Action is what the controller should do with a session on this reconcile.
type Action string

const (
	// ActionInit establishes the first expiry window. It is deliberately not an
	// extension: conflating the two would silently cost the user one extension.
	ActionInit Action = "Init"
	// ActionWait means the session is live and the expiry is still in the future.
	ActionWait Action = "Wait"
	// ActionExtend grants more time and consumes one extension.
	ActionExtend Action = "Extend"
	// ActionExpire tears the session down. Terminal.
	ActionExpire Action = "Expire"
)

// Reasons attached to an ActionExpire decision, surfaced on the Expired
// condition so `kubectl describe` says why a session ended.
const (
	ReasonIdleTimeout         = "IdleTimeout"
	ReasonExtensionsExhausted = "ExtensionsExhausted"
	ReasonHardTTLExceeded     = "HardTTLExceeded"
)

// MinRequeue floors every requeue. A hard-TTL-clamped expiry can land exactly on
// now, and a zero-duration requeue would spin the work queue hot. One second of
// imprecision on teardown is a good trade.
const MinRequeue = time.Second

// DecisionSpec is the lifetime policy, lifted out of the CRD into plain Go
// types so this package needs no Kubernetes imports.
type DecisionSpec struct {
	IdleTimeout       time.Duration
	ExtensionDuration time.Duration
	MaxExtensions     int32
	HardTTL           time.Duration
}

// DecisionState is the observed state the decision depends on.
type DecisionState struct {
	ExpiresAt      *time.Time
	LastActivityAt *time.Time
	ExtensionsUsed int32
}

// Decision is the verdict. NewExpiry and RequeueAfter are meaningful only for
// Init, Wait and Extend; Reason only for Expire.
type Decision struct {
	Action       Action
	NewExpiry    time.Time
	RequeueAfter time.Duration
	Reason       string
}

// Decide resolves a session's fate from time and state alone.
//
// It takes no client, no context and no wall clock, which is what makes every
// rule below exercisable by a table test in microseconds. The ordering matters:
//
//  1. The hard TTL is checked first, so it strictly dominates every other rule.
//  2. Both Init and Extend clamp to the hard deadline via earlier(), so the
//     controller never schedules a wake-up beyond the ceiling. Clamping rather
//     than re-checking is what makes rule 1 unbeatable by a long
//     extensionDuration.
//  3. Staleness uses >=, so activity exactly idleTimeout old is already stale.
func Decide(now, createdAt time.Time, spec DecisionSpec, st DecisionState) Decision {
	hardDeadline := createdAt.Add(spec.HardTTL)

	if !now.Before(hardDeadline) {
		return Decision{Action: ActionExpire, Reason: ReasonHardTTLExceeded}
	}

	if st.ExpiresAt == nil {
		expiry := earlier(now.Add(spec.IdleTimeout), hardDeadline)
		return Decision{
			Action:       ActionInit,
			NewExpiry:    expiry,
			RequeueAfter: requeueFor(now, expiry),
		}
	}

	if now.Before(*st.ExpiresAt) {
		return Decision{
			Action:       ActionWait,
			NewExpiry:    *st.ExpiresAt,
			RequeueAfter: requeueFor(now, *st.ExpiresAt),
		}
	}

	// At or past expiry: decide whether this session has earned more time.
	if st.LastActivityAt == nil || now.Sub(*st.LastActivityAt) >= spec.IdleTimeout {
		return Decision{Action: ActionExpire, Reason: ReasonIdleTimeout}
	}

	if st.ExtensionsUsed >= spec.MaxExtensions {
		return Decision{Action: ActionExpire, Reason: ReasonExtensionsExhausted}
	}

	expiry := earlier(now.Add(spec.ExtensionDuration), hardDeadline)
	return Decision{
		Action:       ActionExtend,
		NewExpiry:    expiry,
		RequeueAfter: requeueFor(now, expiry),
	}
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func requeueFor(now, target time.Time) time.Duration {
	if d := target.Sub(now); d > MinRequeue {
		return d
	}
	return MinRequeue
}
