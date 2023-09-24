package main

import (
	"context"
	otelkafkakonsumer "github.com/Abdulsametileri/otel-kafka-konsumer"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"log"
	"time"
)

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

func main() {
	tp := initJaegerTracer("http://localhost:14268/api/traces")
	defer tp.Shutdown(context.Background())

	segmentioProducer := &kafka.Writer{
		Addr: kafka.TCP("localhost:29092"),
	}

	writer, err := otelkafkakonsumer.NewWriter(segmentioProducer,
		otelkafkakonsumer.WithTracerProvider(tp),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-cg"),
			},
		))
	if err != nil {
		log.Fatal(err.Error())
	}
	defer writer.Close()

	message := kafka.Message{Topic: "opentel", Value: []byte("1")}

	// Extract tracing info from message
	ctx := writer.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(&message))

	tr := otel.Tracer("after producing")
	parentCtx, span := tr.Start(ctx, "work")
	time.Sleep(100 * time.Millisecond)
	span.End()

	writer.WriteMessages(parentCtx, message)
}
