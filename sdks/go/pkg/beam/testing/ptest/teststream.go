package ptest

import (
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

type testStream struct {
	coder  coder.Coder
	events []Event
}

func (t testStream) something(c beam.PCollection) beam.PCollection {
	return beam.PCollection{}
}

func create(coder coder.Coder) *builder {
	return &builder{coder: coder}
}

type builder struct {
	coder            coder.Coder
	events           []Event
	currentWatermark time.Time
}

func (b builder) addElements(element interface{}, elements []interface{}) {
	firstElement := beam.Of(element, b.currentWatermark)
	var remainingElements = make([]beam.TimestampedValue, len(elements))
}

type Event interface {
	getType() EventType
}

type EventType string

type ElementEvent struct {
}

func (ee ElementEvent) getType() EventType {
	return ""
}
