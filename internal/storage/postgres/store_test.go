package postgres

import "testing"

func TestValidStateTransitionMatrix(t *testing.T) {
	valid := [][2]string{{"PENDING", "PAUSED"}, {"PENDING", "CANCELED"}, {"RUNNING", "SUCCEEDED"}, {"RUNNING", "RETRY_WAIT"}, {"PAUSED", "PENDING"}, {"FAILED", "PENDING"}}
	for _, pair := range valid {
		if !validStateTransition(pair[0], pair[1]) {
			t.Errorf("expected valid %s -> %s", pair[0], pair[1])
		}
	}
	invalid := [][2]string{{"SUCCEEDED", "CANCELED"}, {"CANCELED", "PENDING"}, {"PAUSED", "SUCCEEDED"}, {"UNKNOWN", "PENDING"}}
	for _, pair := range invalid {
		if validStateTransition(pair[0], pair[1]) {
			t.Errorf("expected invalid %s -> %s", pair[0], pair[1])
		}
	}
}
