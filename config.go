package otelkafkago

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.13.0"
	"go.opentelemetry.io/otel/trace"
)

// version to attach in the tracer
const version = "0.0.1"

// instrumentationName is the instrumentation library identifier for a Tracer.
const instrumentationName = "github.com/Trendyol/kafka-konsumer"

func localToInternal(opts []Option) []Option {
	out := make([]Option, len(opts))
	for i, o := range opts {
		out[i] = Option(o)
	}
	return out
}

// Config contains configuration options.
type Config struct {
	defaultTracerName string

	Tracer         trace.Tracer
	TracerProvider trace.TracerProvider
	Propagator     propagation.TextMapPropagator

	DefaultStartOpts []trace.SpanStartOption
}

func NewConfig(instrumentationName string, options ...Option) *Config {
	c := Config{defaultTracerName: instrumentationName}

	for _, o := range options {
		if o != nil {
			o.Apply(&c)
		}
	}

	c.Tracer = otel.Tracer(
		c.defaultTracerName,
		trace.WithInstrumentationVersion(version),
		trace.WithSchemaURL(semconv.SchemaURL),
	)

	return &c
}

// ResolveTracer returns an OpenTelemetry tracer from the appropriate
// TracerProvider.
//
// If the passed context contains a span, the TracerProvider that created the
// tracer that created that span will be used. Otherwise, the TracerProvider
// from c is used.
func (c *Config) ResolveTracer(ctx context.Context) trace.Tracer {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		return span.TracerProvider().Tracer(
			c.defaultTracerName,
			trace.WithInstrumentationVersion(version),
			trace.WithSchemaURL(semconv.SchemaURL),
		)
	}
	return c.Tracer
}

// MergedSpanStartOptions returns a copy of opts with any DefaultStartOpts
// that c is configured with prepended.
func (c *Config) MergedSpanStartOptions(opts ...trace.SpanStartOption) []trace.SpanStartOption {
	if c == nil || len(c.DefaultStartOpts) == 0 {
		if len(opts) == 0 {
			return nil
		}
		cp := make([]trace.SpanStartOption, len(opts))
		copy(cp, opts)
		return cp
	}

	merged := make([]trace.SpanStartOption, len(c.DefaultStartOpts)+len(opts))
	copy(merged, c.DefaultStartOpts)
	copy(merged[len(c.DefaultStartOpts):], opts)
	return merged
}

// WithSpan wraps the function f with a span named name.
func (c *Config) WithSpan(ctx context.Context, name string, f func(context.Context) error, opts ...trace.SpanStartOption) error {
	sso := c.MergedSpanStartOptions(opts...)
	ctx, span := c.ResolveTracer(ctx).Start(ctx, name, sso...)
	err := f(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()

	return err
}
