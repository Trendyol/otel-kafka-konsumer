package otelkafkago

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const (
	k, v = "key", "value"
)

func TestCarrierGet(t *testing.T) {
	msg := &kafka.Message{
		Headers: []kafka.Header{
			{Key: k, Value: []byte(v)},
		},
	}
	carrier := NewMessageCarrier(msg)
	assert.Equal(t, v, carrier.Get(k))
}

func TestCarrierGetEmpty(t *testing.T) {
	msg := &kafka.Message{}
	carrier := NewMessageCarrier(msg)
	assert.Equal(t, "", carrier.Get("key"))
}

func TestCarrierSet(t *testing.T) {
	msg := &kafka.Message{}
	carrier := NewMessageCarrier(msg)
	carrier.Set(k, v)
	var got string
	for _, h := range msg.Headers {
		if h.Key == k {
			got = string(h.Value)
		}
	}
	assert.Equal(t, v, got)
}

func TestCarrierSetOverwrites(t *testing.T) {
	msg := &kafka.Message{
		Headers: []kafka.Header{
			{Key: k, Value: []byte("not value")},
			{Key: k, Value: []byte("also not value")},
		},
	}
	carrier := NewMessageCarrier(msg)
	carrier.Set(k, v)
	var got string
	for _, h := range msg.Headers {
		if h.Key == k {
			got = string(h.Value)
		}
	}
	assert.Equal(t, v, got)
}

func TestCarrierKeys(t *testing.T) {
	keys := []string{"one", "two", "three"}
	msg := &kafka.Message{
		Headers: []kafka.Header{
			{Key: keys[0], Value: []byte("")},
			{Key: keys[1], Value: []byte("")},
			{Key: keys[2], Value: []byte("")},
		},
	}
	carrier := NewMessageCarrier(msg)
	assert.Equal(t, keys, carrier.Keys())
}
