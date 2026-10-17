package redis

import (
	"encoding/base64"
	"fmt"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Raw URL base64 contains neither ':' nor braces. Encoding each tuple field
// separately prevents delimiter ambiguity and caller-controlled hash tags.
func topologyPrefix(topo transport.Topology) string {
	encode := base64.RawURLEncoding.EncodeToString
	return fmt.Sprintf("eventbus:v1:{%s:%d:%s}", encode([]byte(topo.Namespace)), topo.Kind, encode([]byte(topo.JobType)))
}

func (*redisTransport) streamKey(topo transport.Topology) string {
	return topologyPrefix(topo) + ":stream"
}
func (*redisTransport) delayedKey(topo transport.Topology) string {
	return topologyPrefix(topo) + ":delayed"
}
func (*redisTransport) dlqKey(topo transport.Topology) string { return topologyPrefix(topo) + ":dlq" }
func (*redisTransport) metadataKey(topo transport.Topology) string {
	return topologyPrefix(topo) + ":metadata"
}
func (*redisTransport) dedupeKey(topo transport.Topology, key string) string {
	return topologyPrefix(topo) + ":dedupe:" + base64.RawURLEncoding.EncodeToString([]byte(key))
}
