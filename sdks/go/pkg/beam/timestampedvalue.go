package beam

import "time"

type TimestampedValue struct {
	Value     interface{}
	Timestamp time.Time
}

func Of(value interface{}, timestamp time.Time) TimestampedValue {
	return TimestampedValue{Value: value, Timestamp: timestamp}
}
