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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workspacev1alpha1 "github.com/Smoothy123/workspace-session-operator/api/v1alpha1"
)

func withAnnotation(v string) *workspacev1alpha1.WorkspaceSession {
	ws := &workspacev1alpha1.WorkspaceSession{}
	if v != "" {
		ws.Annotations = map[string]string{workspacev1alpha1.LastActivityAnnotation: v}
	}
	return ws
}

func TestParseActivity(t *testing.T) {
	cases := []struct {
		name      string
		value     string
		wantFound bool
		wantErr   bool
		wantTime  time.Time
	}{
		{"absent annotation", "", false, false, time.Time{}},
		{
			"valid RFC3339",
			"2026-01-01T12:00:00Z",
			true, false,
			time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{"malformed value", "not-a-timestamp", true, true, time.Time{}},
		{"empty value", "", false, false, time.Time{}},
		{"unix seconds instead of RFC3339", "1767268800", true, true, time.Time{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, found, err := parseActivity(withAnnotation(tc.value))

			if found != tc.wantFound {
				t.Fatalf("found: got %v, want %v", found, tc.wantFound)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && tc.wantFound && !got.Equal(tc.wantTime) {
				t.Errorf("time: got %v, want %v", got, tc.wantTime)
			}
		})
	}
}

// TestSyncActivityIsMonotonic pins Review Focus item 2: a stale gateway write
// carrying an older timestamp must not move lastActivityAt backwards.
func TestSyncActivityIsMonotonic(t *testing.T) {
	newer := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)
	older := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	ws := withAnnotation(older.Format(time.RFC3339))
	ws.Status.LastActivityAt = &metav1.Time{Time: newer}

	changed := applyActivity(ws, older)

	if changed {
		t.Error("applyActivity reported a change for an older timestamp")
	}
	if !ws.Status.LastActivityAt.Time.Equal(newer) {
		t.Errorf("lastActivityAt moved backwards: got %v, want %v",
			ws.Status.LastActivityAt.Time, newer)
	}
}

func TestSyncActivityAdvances(t *testing.T) {
	older := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)

	ws := withAnnotation(newer.Format(time.RFC3339))
	ws.Status.LastActivityAt = &metav1.Time{Time: older}

	if changed := applyActivity(ws, newer); !changed {
		t.Error("applyActivity did not report a change for a newer timestamp")
	}
	if !ws.Status.LastActivityAt.Time.Equal(newer) {
		t.Errorf("got %v, want %v", ws.Status.LastActivityAt.Time, newer)
	}
}

// TestActivityIsPlausible pins the guard against a gateway whose clock runs
// ahead. Activity is supplied by an external writer, so its clock is not ours
// to trust; a timestamp hours in the future makes a session look permanently
// busy. A minute of slack absorbs ordinary NTP drift.
func TestActivityIsPlausible(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		in   time.Time
		want bool
	}{
		{"hours ahead is not believed", now.Add(72 * time.Hour), false},
		{"well past the skew allowance is not believed", now.Add(2 * time.Minute), false},
		{"ordinary NTP drift is believed", now.Add(30 * time.Second), true},
		{"exactly now is believed", now, true},
		{"the past is always believed", now.Add(-5 * time.Minute), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := activityIsPlausible(tc.in, now); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
