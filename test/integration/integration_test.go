package integration

import (
	"context"
	"fmt"
	otelkafkakonsumer "github.com/Trendyol/otel-kafka-konsumer"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"log"
	"strings"
	"testing"
	"time"
)

func getTracer() *trace.TracerProvider {

	tp := trace.NewTracerProvider(
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

func Test_Producer_And_Consumer_Message_Have_Same_Trace_Id(t *testing.T) {
	tracer := getTracer()

	segmentioProducer := &kafka.Writer{
		Addr: kafka.TCP("localhost:29092"),
	}

	writer, err := otelkafkakonsumer.NewWriter(segmentioProducer,
		otelkafkakonsumer.WithTracerProvider(tracer),
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

	tr := tracer.Tracer("otel-kafka-konsumer")
	parentCtx, producerSpan := tr.Start(ctx, "before produce operation")
	time.Sleep(100 * time.Millisecond)
	producerSpan.End()

	writer.WriteMessage(parentCtx, message)

	segmentioReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:29092"},
		GroupTopics: []string{"opentel"},
		GroupID:     "opentel-cg",
	})

	reader, err := otelkafkakonsumer.NewReader(
		segmentioReader,
		otelkafkakonsumer.WithTracerProvider(tracer),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-cg"),
			},
		),
	)
	if err != nil {
		log.Fatal(err.Error())
	}
	consumerMessage, err := reader.ReadMessage(context.Background())
	if err != nil {
		fmt.Println(err.Error())
	}

	//assert
	traceParent := strings.Split(string(consumerMessage.Headers[0].Value), "-")
	println(producerSpan.SpanContext().TraceID().String())
	println(traceParent[1])
	assert.Equal(t, producerSpan.SpanContext().HasTraceID(), true)
	assert.Equal(t, producerSpan.SpanContext().IsValid(), true)
	assert.Equal(t, producerSpan.SpanContext().HasSpanID(), true)
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), traceParent[1])
}

func Test_Producer_And_Consumer_Spans_Have_Same_Trace_Id(t *testing.T) {
	tracer := getTracer()
	segmentioProducer := &kafka.Writer{
		Addr: kafka.TCP("localhost:29092"),
	}

	writer, err := otelkafkakonsumer.NewWriter(segmentioProducer,
		otelkafkakonsumer.WithTracerProvider(tracer),
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

	message := kafka.Message{Topic: "opentel-consumer-test", Value: []byte("1")}

	// Extract tracing info from message
	ctx := writer.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(&message))

	tr := tracer.Tracer("otel-kafka-konsumer")
	parentCtx, producerSpan := tr.Start(ctx, "before produce operation")
	time.Sleep(100 * time.Millisecond)
	producerSpan.End()

	writer.WriteMessage(parentCtx, message)

	segmentioReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:29092"},
		GroupTopics: []string{"opentel-consumer-test"},
		GroupID:     "opentel-cg",
	})

	reader, err := otelkafkakonsumer.NewReader(
		segmentioReader,
		otelkafkakonsumer.WithTracerProvider(tracer),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-cg"),
			},
		),
	)
	if err != nil {
		log.Fatal(err.Error())
	}
	consumerMessage, err := reader.ReadMessage(context.Background())

	// Extract tracing info from message
	consumerCtx := reader.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(consumerMessage))
	consumerCtx, workSpan := tr.Start(consumerCtx, "work")

	if err != nil {
		fmt.Println(err.Error())
	}

	time.Sleep(100 * time.Millisecond)
	workSpan.End()

	_, anotherWorkSpan := tr.Start(consumerCtx, "another work")
	time.Sleep(50 * time.Millisecond)
	anotherWorkSpan.End()

	//assert
	println(producerSpan.SpanContext().TraceID().String())
	println(anotherWorkSpan.SpanContext().TraceID().String())
	println(workSpan.SpanContext().TraceID().String())

	assert.Equal(t, producerSpan.SpanContext().HasTraceID(), true)
	assert.Equal(t, producerSpan.SpanContext().IsValid(), true)
	assert.Equal(t, producerSpan.SpanContext().HasSpanID(), true)
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), anotherWorkSpan.SpanContext().TraceID().String())
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), workSpan.SpanContext().TraceID().String())
}
