package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"io"
	"log"
	"time"
)

func newstdoutTraceProvider(w io.Writer) *trace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("fib"),
			semconv.ServiceVersion("v0.1.0"),
			attribute.String("environment", "demo"),
		),
	)
	if err != nil {
		log.Fatalf("err creating resource %v", err)
	}

	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(w),
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithoutTimestamps(),
	)
	if err != nil {
		log.Fatalf("err initializing exporter %v", err)
	}

	return trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithBatcher(exporter),
		trace.WithResource(r),
	)
}

func initJaegerTracer(url string) *trace.TracerProvider {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		log.Fatalf("Err initializing jaeger instance %v", err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("trace-demo"),
			attribute.String("environment", "prod"),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp
}

// docker run -d --name jaeger -e COLLECTOR_OTLP_ENABLED=true -p 14268:14268 -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one:latest
func main() {
	/*
		l := log.New(os.Stdout, "", 0)

		f, err := os.Create("traces.txt")
		if err != nil {
			l.Fatal(err)
		}
		defer f.Close()

		tp := newstdoutTraceProvider(f)
	*/

	tp := initJaegerTracer("http://localhost:14268/api/traces")
	defer tp.Shutdown(context.Background())

	segmentioReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:29092"},
		GroupTopics: []string{"opentel"},
		GroupID:     "opentel-cg",
	})

	reader, err := otelkafkago.NewReader(
		segmentioReader,
		otelkafkago.WithTracerProvider(tp),
		otelkafkago.WithPropagator(propagation.TraceContext{}),
		otelkafkago.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-cg"),
			},
		),
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Println(message)

		// Extract tracing info from message
		ctx := reader.TraceConfig.Propagator.Extract(context.Background(), otelkafkago.NewMessageCarrier(message))

		tr := otel.Tracer("consumer")
		parentCtx, span := tr.Start(ctx, "work")
		time.Sleep(100 * time.Millisecond)
		span.End()

		_, span = tr.Start(parentCtx, "another work")
		time.Sleep(50 * time.Millisecond)
		span.End()
	}
}
