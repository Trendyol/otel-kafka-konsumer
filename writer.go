package otelkafkago

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.16.0"
	"go.opentelemetry.io/otel/trace"
	"strconv"
)

type Writer struct {
	W   *kafka.Writer
	cfg *Config
}

// NewWriter wraps the resulting Writer with OpenTelemetry instrumentation
func NewWriter(w *kafka.Writer, opts ...Option) (*Writer, error) {
	cfg := NewConfig(instrumentationName, opts...)

	// Common attributes for all spans this producer will produce.
	cfg.DefaultStartOpts = append(
		cfg.DefaultStartOpts,
		trace.WithAttributes(
			semconv.MessagingDestinationKindTopic,
		),
	)

	return &Writer{
		W:   w,
		cfg: cfg,
	}, nil
}

func (w *Writer) Close() {
	w.W.Close()
}

func (w *Writer) WriteMessages(ctx context.Context, msg *kafka.Message) error {
	span := w.startSpan(msg)
	err := w.W.WriteMessages(ctx, *msg)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
	return err
}

func (w *Writer) startSpan(msg *kafka.Message) trace.Span {
	carrier := NewMessageCarrier(msg)
	psc := w.cfg.Propagator.Extract(context.Background(), carrier)

	opts := w.cfg.MergedSpanStartOptions(
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(msg.Topic),
			semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)

	name := fmt.Sprintf("%s send", msg.Topic)
	ctx, span := w.cfg.Tracer.Start(psc, name, opts...)

	w.cfg.Propagator.Inject(ctx, carrier)
	return span
}
