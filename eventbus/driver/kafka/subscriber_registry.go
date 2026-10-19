package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	kafkaEventLayoutOption = "kafka.event_layout"
	kafkaTargetedLayoutV1  = "targeted-v1"
	subscriberMarkerSuffix = ".subscriber-v1"
)

func requireTargetedEventLayout(topology transport.Topology) error {
	if topology.Kind != transport.EventTopology {
		return nil
	}
	if layout, _ := topology.Options[kafkaEventLayoutOption].(string); layout != kafkaTargetedLayoutV1 {
		return fmt.Errorf("kafka: event topology requires %s=%q to exclude older broadcast-retry instances", kafkaEventLayoutOption, kafkaTargetedLayoutV1)
	}
	return nil
}

func (t *kafkaTransport) eventLayoutTopic(topology transport.Topology) string {
	return topology.Name() + ".layout-" + kafkaTargetedLayoutV1
}

func (t *kafkaTransport) subscriberMarkerTopic(topology transport.Topology, group string) string {
	return t.privateReadyTopic(topology, group) + subscriberMarkerSuffix
}

func subscriberMarkerConfigs() map[string]string {
	return map[string]string{"cleanup.policy": "delete", "retention.ms": "-1"}
}

func (t *kafkaTransport) validateEventLayout(ctx context.Context, topology transport.Topology) error {
	if topology.Kind != transport.EventTopology {
		return nil
	}
	ready, err := t.relatedTopicPartitionsReady(ctx, []string{t.eventLayoutTopic(topology)}, 1)
	if err != nil {
		return fmt.Errorf("kafka: validate targeted event layout: %w", err)
	}
	if !ready {
		return fmt.Errorf("kafka: targeted event layout is not provisioned")
	}
	return nil
}

func (t *kafkaTransport) validateProvisionedSubscriber(ctx context.Context, topology transport.Topology, group string) error {
	if topology.Kind != transport.EventTopology || group == "" {
		return fmt.Errorf("kafka: provisioned event subscriber required")
	}
	if err := t.validateEventLayout(ctx, topology); err != nil {
		return err
	}
	partitions := int32(1)
	if value, ok := topology.Options["kafka.partitions"].(int32); ok {
		partitions = value
	}
	privateTopic := t.privateReadyTopic(topology, group)
	marker := t.subscriberMarkerTopic(topology, group)
	if len(privateTopic) > 249 || len(marker) > 249 || !kafkaTopicName.MatchString(privateTopic) || !kafkaTopicName.MatchString(marker) {
		return fmt.Errorf("kafka: subscriber produces invalid private topic layout")
	}
	return t.validateSubscriberTopics(ctx, group, privateTopic, marker, partitions)
}

func (t *kafkaTransport) validateSubscriberTopics(ctx context.Context, group, privateTopic, marker string, partitions int32) error {
	ready, err := t.relatedTopicPartitionsReady(ctx, []string{privateTopic}, partitions)
	if err != nil {
		return fmt.Errorf("kafka: validate subscriber private topic: %w", err)
	}
	if !ready {
		return fmt.Errorf("kafka: subscriber %q is not provisioned", group)
	}
	ready, err = t.relatedTopicPartitionsReady(ctx, []string{marker}, 1)
	if err != nil {
		return fmt.Errorf("kafka: validate subscriber registry marker: %w", err)
	}
	if !ready {
		return fmt.Errorf("kafka: subscriber %q is not registered", group)
	}
	configs, err := t.adm.DescribeTopicConfigs(ctx, privateTopic, marker)
	if err != nil {
		return fmt.Errorf("kafka: inspect subscriber registry marker: %w", err)
	}
	byTopic := make(map[string]map[string]string, len(configs))
	for _, resource := range configs {
		if resource.Err != nil {
			return fmt.Errorf("kafka: subscriber %q has invalid topic config: %w", group, resource.Err)
		}
		values := make(map[string]string, len(resource.Configs))
		for _, config := range resource.Configs {
			if config.Value != nil {
				values[config.Key] = *config.Value
			}
		}
		byTopic[resource.Name] = values
	}
	privateValues, privateOK := byTopic[privateTopic]
	markerValues, markerOK := byTopic[marker]
	if !privateOK || !markerOK || !cleanupPolicyDeleteOnly(privateValues["cleanup.policy"]) {
		return fmt.Errorf("kafka: subscriber %q has invalid private topic configuration", group)
	}
	for key, want := range subscriberMarkerConfigs() {
		if markerValues[key] != want {
			return fmt.Errorf("kafka: subscriber %q registry marker has %s=%q, want %q", group, key, markerValues[key], want)
		}
	}
	return nil
}

func (t *kafkaTransport) registeredPrivateTopics(ctx context.Context, topology transport.Topology) ([]string, error) {
	all, err := t.registeredSubscriberTopics(ctx, topology)
	if err != nil {
		return nil, err
	}
	private := all[:0]
	for _, topic := range all {
		if !strings.HasSuffix(topic, subscriberMarkerSuffix) {
			private = append(private, topic)
		}
	}
	return private, nil
}

func (t *kafkaTransport) registeredSubscriberTopics(ctx context.Context, topology transport.Topology) ([]string, error) {
	metadata, err := t.adm.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: discover subscriber registry: %w", err)
	}
	prefix := topology.Name() + ".c."
	if version, _ := topology.Options[transport.ResourceNameVersionOption].(string); version == transport.ResourceNameVersionLegacy {
		prefix = topology.Name() + "-"
	}
	var topics []string
	for topic, detail := range metadata.Topics {
		if detail.Err != nil || !strings.HasPrefix(topic, prefix) || !strings.HasSuffix(topic, ".retry-ready"+subscriberMarkerSuffix) {
			continue
		}
		privateTopic := strings.TrimSuffix(topic, subscriberMarkerSuffix)
		encodedGroup := strings.TrimSuffix(strings.TrimPrefix(privateTopic, prefix), ".retry-ready")
		if encodedGroup == "" {
			continue
		}
		partitions := int32(1)
		if value, ok := topology.Options["kafka.partitions"].(int32); ok {
			partitions = value
		}
		if err := t.validateSubscriberTopics(ctx, encodedGroup, privateTopic, topic, partitions); err != nil {
			return nil, err
		}
		topics = append(topics, privateTopic, topic)
	}
	sort.Strings(topics)
	return topics, nil
}
