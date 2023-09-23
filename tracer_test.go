package otelkafkakonsumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func TestFnTracerProvider_Tracer(t *testing.T) {
	// Given
	var name string = "test_tracer"
	var opts []trace.TracerOption = []trace.TracerOption{trace.WithInstrumentationVersion("v1.0.0")}
	var expectedTracer trace.Tracer = &fnTracer{}
	fn := &fnTracerProvider{
		tracer: func(n string, o ...trace.TracerOption) trace.Tracer {
			assert.Equal(t, name, n)
			assert.Equal(t, opts, o)
			return expectedTracer
		},
	}

	// When
	tracer := fn.Tracer(name, opts...)

	// Then
	assert.Equal(t, expectedTracer, tracer)
}
