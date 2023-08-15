package otelkafkago

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestWithTracerProvider(t *testing.T) {
	mtp := mockTracerProvider(nil)
	// Default is to use the global TracerProvider. This will override that.
	c := NewConfig(instrumentationName, WithTracerProvider(mtp))
	expected := mtp.Tracer(instrumentationName)
	assert.Same(t, expected, c.Tracer)
}

func TestWithAttributes(t *testing.T) {
	attr := attribute.String("key", "value")
	c := NewConfig(instrumentationName, WithAttributes([]attribute.KeyValue{attr}))
	ssc := trace.NewSpanStartConfig(c.DefaultStartOpts...)
	assert.Contains(t, ssc.Attributes(), attr)
}

func TestWithPropagator(t *testing.T) {
	p := propagation.NewCompositeTextMapPropagator()
	// Use a non-nil value.
	p = propagation.NewCompositeTextMapPropagator(p)
	assert.Equal(t, p, NewConfig(instrumentationName, WithPropagator(p)).Propagator)
}

var mockTracerProvider = func(spanRecorder map[string]*mockSpan) trace.TracerProvider {
	recordSpan := func(s *mockSpan) {
		if spanRecorder != nil {
			spanRecorder[s.Name] = s
		}
	}

	return &fnTracerProvider{
		tracer: func() func(string, ...trace.TracerOption) trace.Tracer {
			registry := make(map[string]trace.Tracer)
			return func(name string, opts ...trace.TracerOption) trace.Tracer {
				t, ok := registry[name]
				if !ok {
					t = &fnTracer{
						start: func(ctx context.Context, n string, o ...trace.SpanStartOption) (context.Context, trace.Span) {
							span := &mockSpan{Name: n, StartOpts: o}
							recordSpan(span)
							ctx = trace.ContextWithSpan(ctx, span)
							return ctx, span
						},
					}
					registry[name] = t
				}
				return t
			}
		}(),
	}
}

type mockSpan struct {
	trace.Span

	Name      string
	StartOpts []trace.SpanStartOption

	RecordedErrs []error
	Statuses     []status
	Ended        bool
}

func (s *mockSpan) RecordError(err error, _ ...trace.EventOption) {
	s.RecordedErrs = append(s.RecordedErrs, err)
}

func (s *mockSpan) SetStatus(c codes.Code, desc string) {
	s.Statuses = append(s.Statuses, status{Code: c, Description: desc})
}

func (s *mockSpan) End(...trace.SpanEndOption) {
	s.Ended = true
}

type status struct {
	Code        codes.Code
	Description string
}
