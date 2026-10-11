package controller

import (
	"testing"
	"time"
)

// base is an arbitrary fixed instant. Using a constant rather than time.Now()
// keeps every case deterministic.
var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func ptr(t time.Time) *time.Time { return &t }

func defaultSpec() DecisionSpec {
	return DecisionSpec{
		IdleTimeout:       30 * time.Minute,
		ExtensionDuration: 30 * time.Minute,
		MaxExtensions:     3,
		HardTTL:           4 * time.Hour,
	}
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name        string
		now         time.Time
		createdAt   time.Time
		spec        DecisionSpec
		state       DecisionState
		wantAction  Action
		wantReason  string
		wantExpiry  time.Time // zero means "do not check"
		wantRequeue time.Duration
	}{
		{
			name:        "fresh object initialises its expiry window",
			now:         base,
			createdAt:   base,
			spec:        defaultSpec(),
			state:       DecisionState{},
			wantAction:  ActionInit,
			wantExpiry:  base.Add(30 * time.Minute),
			wantRequeue: 30 * time.Minute,
		},
		{
			name:      "init is clamped when idleTimeout overruns the hard TTL",
			now:       base,
			createdAt: base,
			spec: DecisionSpec{
				IdleTimeout:       30 * time.Minute,
				ExtensionDuration: 30 * time.Minute,
				MaxExtensions:     3,
				HardTTL:           10 * time.Minute,
			},
			state:       DecisionState{},
			wantAction:  ActionInit,
			wantExpiry:  base.Add(10 * time.Minute),
			wantRequeue: 10 * time.Minute,
		},
		{
			name:        "before expiry it waits",
			now:         base.Add(10 * time.Minute),
			createdAt:   base,
			spec:        defaultSpec(),
			state:       DecisionState{ExpiresAt: ptr(base.Add(30 * time.Minute))},
			wantAction:  ActionWait,
			wantRequeue: 20 * time.Minute,
		},
		{
			name:      "at expiry with recent activity and extensions left, it extends",
			now:       base.Add(30 * time.Minute),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(30 * time.Minute)),
				LastActivityAt: ptr(base.Add(25 * time.Minute)),
				ExtensionsUsed: 1,
			},
			wantAction:  ActionExtend,
			wantExpiry:  base.Add(60 * time.Minute),
			wantRequeue: 30 * time.Minute,
		},
		{
			name:      "extension is clamped by the hard deadline",
			now:       base.Add(50 * time.Minute),
			createdAt: base,
			spec: DecisionSpec{
				IdleTimeout:       30 * time.Minute,
				ExtensionDuration: 30 * time.Minute,
				MaxExtensions:     3,
				HardTTL:           time.Hour,
			},
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(50 * time.Minute)),
				LastActivityAt: ptr(base.Add(45 * time.Minute)),
			},
			wantAction:  ActionExtend,
			wantExpiry:  base.Add(60 * time.Minute),
			wantRequeue: 10 * time.Minute,
		},
		{
			name:      "stale activity expires the session",
			now:       base.Add(45 * time.Minute),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(30 * time.Minute)),
				LastActivityAt: ptr(base.Add(5 * time.Minute)),
			},
			wantAction: ActionExpire,
			wantReason: ReasonIdleTimeout,
		},
		{
			// Boundary: paired with the case below, these two pin staleness to
			// >= rather than >. Exactly one of them must expire.
			name:      "activity exactly idleTimeout old counts as stale",
			now:       base.Add(30 * time.Minute),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(30 * time.Minute)),
				LastActivityAt: ptr(base),
			},
			wantAction: ActionExpire,
			wantReason: ReasonIdleTimeout,
		},
		{
			name:      "activity one second under idleTimeout still extends",
			now:       base.Add(30 * time.Minute),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(30 * time.Minute)),
				LastActivityAt: ptr(base.Add(time.Second)),
			},
			wantAction:  ActionExtend,
			wantExpiry:  base.Add(60 * time.Minute),
			wantRequeue: 30 * time.Minute,
		},
		{
			name:       "no activity ever recorded expires the session",
			now:        base.Add(30 * time.Minute),
			createdAt:  base,
			spec:       defaultSpec(),
			state:      DecisionState{ExpiresAt: ptr(base.Add(30 * time.Minute))},
			wantAction: ActionExpire,
			wantReason: ReasonIdleTimeout,
		},
		{
			name:      "exhausted extensions expire the session despite activity",
			now:       base.Add(30 * time.Minute),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(30 * time.Minute)),
				LastActivityAt: ptr(base.Add(29 * time.Minute)),
				ExtensionsUsed: 3,
			},
			wantAction: ActionExpire,
			wantReason: ReasonExtensionsExhausted,
		},
		{
			name:      "maxExtensions zero means the first idle check is terminal",
			now:       base.Add(30 * time.Minute),
			createdAt: base,
			spec: DecisionSpec{
				IdleTimeout:       30 * time.Minute,
				ExtensionDuration: 30 * time.Minute,
				MaxExtensions:     0,
				HardTTL:           4 * time.Hour,
			},
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(30 * time.Minute)),
				LastActivityAt: ptr(base.Add(29 * time.Minute)),
			},
			wantAction: ActionExpire,
			wantReason: ReasonExtensionsExhausted,
		},
		{
			name:      "hard TTL dominates active sessions with extensions remaining",
			now:       base.Add(4 * time.Hour),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(4 * time.Hour)),
				LastActivityAt: ptr(base.Add(4*time.Hour - time.Minute)),
				ExtensionsUsed: 0,
			},
			wantAction: ActionExpire,
			wantReason: ReasonHardTTLExceeded,
		},
		{
			name:      "future-dated activity cannot outlive the hard TTL",
			now:       base.Add(4 * time.Hour),
			createdAt: base,
			spec:      defaultSpec(),
			state: DecisionState{
				ExpiresAt:      ptr(base.Add(4 * time.Hour)),
				LastActivityAt: ptr(base.Add(99 * time.Hour)),
			},
			wantAction: ActionExpire,
			wantReason: ReasonHardTTLExceeded,
		},
		{
			// Boundary: now exactly on the hard deadline must already be expired,
			// because the check is !now.Before(hardDeadline).
			name:      "now exactly at the hard deadline expires",
			now:       base.Add(59 * time.Minute),
			createdAt: base,
			spec: DecisionSpec{
				IdleTimeout:       30 * time.Minute,
				ExtensionDuration: 30 * time.Minute,
				MaxExtensions:     3,
				HardTTL:           59 * time.Minute,
			},
			state:      DecisionState{},
			wantAction: ActionExpire,
			wantReason: ReasonHardTTLExceeded,
		},
		{
			name:      "init requeue is floored when the clamped window is zero-length",
			now:       base.Add(30 * time.Minute),
			createdAt: base,
			spec: DecisionSpec{
				IdleTimeout:       30 * time.Minute,
				ExtensionDuration: 30 * time.Minute,
				MaxExtensions:     3,
				HardTTL:           30*time.Minute + time.Millisecond,
			},
			state:       DecisionState{},
			wantAction:  ActionInit,
			wantExpiry:  base.Add(30*time.Minute + time.Millisecond),
			wantRequeue: MinRequeue,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Decide(tc.now, tc.createdAt, tc.spec, tc.state)

			if got.Action != tc.wantAction {
				t.Fatalf("action: got %q, want %q", got.Action, tc.wantAction)
			}
			if tc.wantReason != "" && got.Reason != tc.wantReason {
				t.Errorf("reason: got %q, want %q", got.Reason, tc.wantReason)
			}
			if !tc.wantExpiry.IsZero() && !got.NewExpiry.Equal(tc.wantExpiry) {
				t.Errorf("expiry: got %v, want %v", got.NewExpiry, tc.wantExpiry)
			}
			if tc.wantRequeue != 0 && got.RequeueAfter != tc.wantRequeue {
				t.Errorf("requeue: got %v, want %v", got.RequeueAfter, tc.wantRequeue)
			}
		})
	}
}

// TestDecideExpireCarriesNoRequeue pins the contract that an expired session is
// terminal: the controller must not schedule another wake-up for it.
func TestDecideExpireCarriesNoRequeue(t *testing.T) {
	got := Decide(base.Add(5*time.Hour), base, defaultSpec(), DecisionState{
		ExpiresAt: ptr(base.Add(30 * time.Minute)),
	})
	if got.Action != ActionExpire {
		t.Fatalf("action: got %q, want %q", got.Action, ActionExpire)
	}
	if got.RequeueAfter != 0 {
		t.Errorf("requeue: got %v, want 0 — expiry is terminal", got.RequeueAfter)
	}
}
