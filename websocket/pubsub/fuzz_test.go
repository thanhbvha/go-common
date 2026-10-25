package pubsub

import (
	stdjson "encoding/json"
	"testing"

	"github.com/goccy/go-json"
)

func FuzzCrossNodeMessageDecode(f *testing.F) {
	f.Add([]byte(`{"id":"1","version":1,"type":"notification","node_id":"node"}`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		if !stdjson.Valid(payload) {
			return
		}
		var message CrossNodeMessage
		_ = json.Unmarshal(payload, &message)
	})
}
