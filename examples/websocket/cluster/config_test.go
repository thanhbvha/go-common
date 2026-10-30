package main

import "testing"

func TestIndependentDriverConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name               string
		input              options
		eventbus, delivery string
		invalid            bool
	}{
		{name: "default", eventbus: "redis", delivery: "redis"},
		{name: "legacy nats", input: options{broker: "nats"}, eventbus: "nats", delivery: "nats"},
		{name: "Kafka Redis", input: options{eventbusDriver: "kafka", deliveryDriver: "redis"}, eventbus: "kafka", delivery: "redis"},
		{name: "Kafka NATS", input: options{eventbusDriver: "kafka", deliveryDriver: "nats"}, eventbus: "kafka", delivery: "nats"},
		{name: "NATS Redis", input: options{eventbusDriver: "nats", deliveryDriver: "redis"}, eventbus: "nats", delivery: "redis"},
		{name: "Redis NATS", input: options{eventbusDriver: "redis", deliveryDriver: "nats"}, eventbus: "redis", delivery: "nats"},
		{name: "Kafka legacy shorthand rejected", input: options{broker: "kafka"}, invalid: true},
		{name: "Kafka delivery rejected", input: options{eventbusDriver: "kafka", deliveryDriver: "kafka"}, invalid: true},
		{name: "unknown EventBus", input: options{eventbusDriver: "unknown"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := tc.input.normalized()
			if tc.invalid {
				if err == nil {
					t.Fatal("expected invalid configuration")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if o.eventbusDriver != tc.eventbus || o.deliveryDriver != tc.delivery {
				t.Fatalf("drivers=%s/%s", o.eventbusDriver, o.deliveryDriver)
			}
		})
	}
}
