package observability

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
)

func SetupTracing(ctx context.Context) (func(context.Context) error, error) {
	if endpoint := strings.TrimSpace(os.Getenv("SCHEDULER_OTEL_ENDPOINT")); endpoint != "" {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
		if err != nil {
			return nil, err
		}
		provider := trace.NewTracerProvider(trace.WithBatcher(exporter))
		otel.SetTracerProvider(provider)
		return provider.Shutdown, nil
	}
	if os.Getenv("SCHEDULER_TRACE_STDOUT") != "1" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}
	provider := trace.NewTracerProvider(trace.WithBatcher(exporter))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
