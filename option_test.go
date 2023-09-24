package otelkafkakonsumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

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
