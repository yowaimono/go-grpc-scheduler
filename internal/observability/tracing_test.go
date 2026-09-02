package observability

import (
	"context"
	"os"
	"testing"
)

func TestSetupTracingDefaultsToNoop(t *testing.T) {
	_ = os.Unsetenv("SCHEDULER_TRACE_STDOUT")
	_ = os.Unsetenv("SCHEDULER_OTEL_ENDPOINT")
	shutdown, err := SetupTracing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupTracingStdoutModeCanStartAndShutdown(t *testing.T) {
	t.Setenv("SCHEDULER_TRACE_STDOUT", "1")
	t.Setenv("SCHEDULER_OTEL_ENDPOINT", "")
	shutdown, err := SetupTracing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
