package integration

import (
	"context"
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
			semconv.ServiceName("integration-test"),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp
}

func Test_Producer_And_Consumer_Spans_Have_Same_Trace_Id_In_AutoCommit_Mode(t *testing.T) {
	tracer := getTracer()
	segmentioProducer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		AllowAutoTopicCreation: true,
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
		Brokers:     []string{"localhost:9092"},
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

	time.Sleep(100 * time.Millisecond)
	workSpan.End()

	_, anotherWorkSpan := tr.Start(consumerCtx, "another work")
	time.Sleep(50 * time.Millisecond)
	anotherWorkSpan.End()

	//assert
	traceParent := strings.Split(string(consumerMessage.Headers[0].Value), "-")
	assert.Equal(t, producerSpan.SpanContext().HasTraceID(), true)
	assert.Equal(t, producerSpan.SpanContext().IsValid(), true)
	assert.Equal(t, producerSpan.SpanContext().HasSpanID(), true)
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), anotherWorkSpan.SpanContext().TraceID().String())
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), workSpan.SpanContext().TraceID().String())
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), traceParent[1])
}

func Test_Producer_And_Consumer_Spans_Have_Same_Trace_Id_In_ManualCommit_Mode(t *testing.T) {
	tracer := getTracer()
	segmentioProducer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		AllowAutoTopicCreation: true,
	}
	writer, err := otelkafkakonsumer.NewWriter(segmentioProducer,
		otelkafkakonsumer.WithTracerProvider(tracer),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-cg-manual-commit"),
			},
		))
	if err != nil {
		log.Fatal(err.Error())
	}
	defer writer.Close()

	message := kafka.Message{Topic: "opentel-consumer-test-manual-commit", Value: []byte("1")}

	// Extract tracing info from message
	ctx := writer.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(&message))

	tr := tracer.Tracer("otel-kafka-konsumer")
	parentCtx, producerSpan := tr.Start(ctx, "before produce operation")
	time.Sleep(100 * time.Millisecond)
	producerSpan.End()

	writer.WriteMessage(parentCtx, message)

	segmentioReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupTopics: []string{"opentel-consumer-test-manual-commit"},
		GroupID:     "opentel-cg-manual-commit",
	})

	reader, err := otelkafkakonsumer.NewReader(
		segmentioReader,
		otelkafkakonsumer.WithTracerProvider(tracer),
		otelkafkakonsumer.WithPropagator(propagation.TraceContext{}),
		otelkafkakonsumer.WithAttributes(
			[]attribute.KeyValue{
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingKafkaClientIDKey.String("opentel-cg-manual-commit"),
			},
		),
	)
	if err != nil {
		log.Fatal(err.Error())
	}
	m := &kafka.Message{}
	err = reader.FetchMessage(context.Background(), m)
	if err != nil {
		log.Fatal(err.Error())
	}
	// Extract tracing info from message
	consumerCtx := reader.TraceConfig.Propagator.Extract(context.Background(), otelkafkakonsumer.NewMessageCarrier(m))
	consumerCtx, workSpan := tr.Start(consumerCtx, "work")

	time.Sleep(100 * time.Millisecond)
	workSpan.End()

	_, anotherWorkSpan := tr.Start(consumerCtx, "another work")
	time.Sleep(50 * time.Millisecond)
	anotherWorkSpan.End()

	reader.CommitMessages(context.Background(), *m)

	//assert
	traceParent := strings.Split(string(m.Headers[0].Value), "-")
	assert.Equal(t, producerSpan.SpanContext().HasTraceID(), true)
	assert.Equal(t, producerSpan.SpanContext().IsValid(), true)
	assert.Equal(t, producerSpan.SpanContext().HasSpanID(), true)
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), anotherWorkSpan.SpanContext().TraceID().String())
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), workSpan.SpanContext().TraceID().String())
	assert.Equal(t, producerSpan.SpanContext().TraceID().String(), traceParent[1])
}
