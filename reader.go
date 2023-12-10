package otelkafkakonsumer

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
	R                *kafka.Reader
	TraceConfig      *Config
	activeFetchSpan  unsafe.Pointer
	activeCommitSpan unsafe.Pointer
}

type spanWrapper struct {
	otelSpan trace.Span
}

// NewReader calls kafka.NewReader and wraps the resulting Consumer with
// tracing instrumentation.
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
		R:                r,
		TraceConfig:      cfg,
		activeFetchSpan:  unsafe.Pointer(&spanWrapper{}),
		activeCommitSpan: unsafe.Pointer(&spanWrapper{}),
	}, nil
}

func (r *Reader) startSpan(spanName string, msg *kafka.Message) spanWrapper {
	carrier := NewMessageCarrier(msg)
	psc := r.TraceConfig.Propagator.Extract(context.Background(), carrier)

	opts := r.TraceConfig.MergedSpanStartOptions(
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(msg.Topic),
			semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	ctx, otelSpan := r.TraceConfig.Tracer.Start(psc, spanName, opts...)

	// Inject the current span into the original message, so it can be used to
	// propagate the span.
	r.TraceConfig.Propagator.Inject(ctx, carrier)

	return spanWrapper{otelSpan: otelSpan}
}

func (r *Reader) FetchMessage(ctx context.Context, message *kafka.Message) error {
	startTime := time.Now()
	m, err := r.R.FetchMessage(ctx)
	if err != nil {
		return err
	}
	*message = m

	s := r.startSpan(fmt.Sprintf("fetched from %s", message.Topic), message)
	active := atomic.SwapPointer(&r.activeFetchSpan, unsafe.Pointer(&s))
	(*spanWrapper)(active).End(trace.WithTimestamp(startTime))
	s.End()

	return nil
}

func (r *Reader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	// open span
	startTime := time.Now()
	s := r.startSpan(fmt.Sprintf("committed to %s", msgs[0].Topic), &msgs[0])
	active := atomic.SwapPointer(&r.activeCommitSpan, unsafe.Pointer(&s))

	err := r.R.CommitMessages(ctx, msgs...)

	// end span
	(*spanWrapper)(active).End(trace.WithTimestamp(startTime))
	s.End()

	return err
}

func (r *Reader) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	endTime := time.Now()
	msg, err := r.R.ReadMessage(ctx)
	if err == nil {
		s := r.startSpan(fmt.Sprintf("received from %s", msg.Topic), &msg)
		active := atomic.SwapPointer(&r.activeFetchSpan, unsafe.Pointer(&s))
		(*spanWrapper)(active).End(trace.WithTimestamp(endTime))
		s.End()
	}
	return &msg, err
}

func (s spanWrapper) End(options ...trace.SpanEndOption) {
	if s.otelSpan != nil {
		s.otelSpan.End(options...)
	}
}

// Close calls the underlying Consumer.Close and if polling is enabled, ends
// any remaining span.
func (r *Reader) Close() error {
	err := r.R.Close()
	(*spanWrapper)(atomic.LoadPointer(&r.activeFetchSpan)).End()
	(*spanWrapper)(atomic.LoadPointer(&r.activeCommitSpan)).End()
	return err
}
