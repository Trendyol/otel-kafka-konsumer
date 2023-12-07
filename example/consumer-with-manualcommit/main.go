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

func main() {
	tp := initJaegerTracer("http://localhost:14268/api/traces")
	defer func(tp *trace.TracerProvider, ctx context.Context) {
		err := tp.Shutdown(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}(tp, context.Background())

	segmentioReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:29092"},
		GroupTopics: []string{"opentel"},
		GroupID:     "opentel-manualcommit-cg",
	})

	reader, err := otelkafkakonsumer.NewReader(
		segmentioReader,
		otelkafkakonsumer.WithTracerProvider(tp),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-manualcommit-cg"),
			},
		),
	)
	if err != nil {
		log.Fatal(err.Error()) //nolint:gocritic
	}

	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Println(message)

		// Extract tracing info from message
		ctx := reader.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(message))

		tr := otel.Tracer("consumer")
		parentCtx, span := tr.Start(ctx, "work")
		time.Sleep(100 * time.Millisecond)
		span.End()

		_, span = tr.Start(parentCtx, "another work")
		time.Sleep(50 * time.Millisecond)
		span.End()

		reader.CommitMessages(context.Background(), *message)
	}
}
