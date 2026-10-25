package pubsub

import (
	"testing"
	"time"

	"github.com/goccy/go-json"
)

func BenchmarkPubSubDecode(b *testing.B) {
	payload, err := json.Marshal(CrossNodeMessage{ID: "message", Version: 1, Type: MessageTypeNotification, NodeID: "node", ShardID: "shard", UserID: "user", Data: map[string]interface{}{"message": "benchmark"}, Timestamp: time.Now()})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var message CrossNodeMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			b.Fatal(err)
		}
	}
}
