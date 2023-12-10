package main

import (
	"context"
	"fmt"
	"log"
	"time"

	otelkafkakonsumer "github.com/Trendyol/otel-kafka-konsumer"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func main() {
	tp := initJaegerTracer("http://localhost:14268/api/traces")
	defer tp.Shutdown(context.Background())

	reader, _ := otelkafkakonsumer.NewReader(
		kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{"localhost:29092"},
			GroupTopics: []string{"opentel"},
			GroupID:     "opentel-manualcommit-cg",
		}),
		otelkafkakonsumer.WithTracerProvider(tp),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-manualcommit-cg"),
			},
		),
	)

	for {
		// Consume message
		m := &kafka.Message{}
		reader.FetchMessage(context.Background(), m)
		fmt.Println("incoming message", *m)

		// Extract tracing info from message
		ctx := reader.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(m))

		tr := otel.Tracer("consumer")
		parentCtx, span := tr.Start(ctx, "work")
		time.Sleep(100 * time.Millisecond) // simulate some work
		span.End()

		_, span = tr.Start(parentCtx, "another work")
		time.Sleep(50 * time.Millisecond) // simulate some work
		span.End()

		// Commit message
		reader.CommitMessages(context.Background(), *m)
	}
}

func initJaegerTracer(url string) *trace.TracerProvider {
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
