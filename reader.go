package otelkafkago

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/segmentio/kafka-go"
	semconv "go.opentelemetry.io/otel/semconv/v1.13.0"
	"go.opentelemetry.io/otel/trace"
)

type Reader struct {
	R           *kafka.Reader
	TraceConfig *Config
	activeSpan  unsafe.Pointer
}

type readerSpan struct {
	otelSpan trace.Span
}

func NewReader(r *kafka.Reader, opts ...Option) (*Reader, error) {
	cfg := NewConfig(instrumentationName, opts...)

	// Common attributes for all spans this consumer will produce.
	cfg.DefaultStartOpts = append(
		cfg.DefaultStartOpts,
		trace.WithAttributes(
			semconv.MessagingDestinationKindTopic,
			semconv.MessagingOperationReceive,
		),
	)
	return &Reader{
		R:           r,
		TraceConfig: cfg,
		activeSpan:  unsafe.Pointer(&readerSpan{}),
	}, nil
}

func (r *Reader) startSpan(msg *kafka.Message) readerSpan {
	carrier := NewMessageCarrier(msg)
	psc := r.TraceConfig.Propagator.Extract(context.Background(), carrier)

	name := fmt.Sprintf("received from %s", msg.Topic)
	opts := r.TraceConfig.MergedSpanStartOptions(
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(msg.Topic),
			semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	ctx, otelSpan := r.TraceConfig.Tracer.Start(psc, name, opts...)

	// Inject the current span into the original message, so it can be used to
	// propagate the span.
	r.TraceConfig.Propagator.Inject(ctx, carrier)

	return readerSpan{otelSpan: otelSpan}
}

func (r *Reader) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	endTime := time.Now()
	msg, err := r.R.ReadMessage(ctx)
	if err == nil {
		s := r.startSpan(&msg)
		active := atomic.SwapPointer(&r.activeSpan, unsafe.Pointer(&s))
		(*readerSpan)(active).End(trace.WithTimestamp(endTime))
		s.End()
	}
	return &msg, err
}

func (s readerSpan) End(options ...trace.SpanEndOption) {
	if s.otelSpan != nil {
		s.otelSpan.End(options...)
	}
}

// Close calls the underlying Consumer.Close and if polling is enabled, ends
// any remaining span.
func (r *Reader) Close() error {
	err := r.R.Close()
	(*readerSpan)(atomic.LoadPointer(&r.activeSpan)).End()
	return err
}
