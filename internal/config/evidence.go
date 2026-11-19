package config

import (
	"errors"
	"os"
	"regexp"
	"strings"

	"github.com/overmindv/content/pkg/api/activity"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Evidence struct {
	Enabled      bool
	ContentTopic string
	KafkaOptions []kgo.Opt
}

func loadEvidence(resources Resources) (Evidence, error) {
	enabled, err := envSwitch("RECOMMENDATIONS_LEARNING_EVIDENCE_ENABLED")
	if err != nil || !enabled {
		return Evidence{}, err
	}
	const outcomeTopic = "learning.outcome.v1"
	topic := strings.TrimSpace(os.Getenv("KAFKA_CONTENT_LEARNING_TOPIC"))
	if topic == "" {
		topic = activity.DefaultLearningTopic
	}
	if !validKafkaTopic(topic) || topic == outcomeTopic || topic == activity.DefaultMaterialTopic ||
		topic == activity.DefaultContributionTopic || (resources.Enabled && (topic == resources.MaterialTopic || resources.MaterialTopic == outcomeTopic)) {
		return Evidence{}, errors.New("learning evidence topics must be valid and separate from resource and activity topics")
	}
	production := strings.EqualFold(strings.TrimSpace(os.Getenv("ENV")), "production")
	if production && strings.TrimSpace(os.Getenv("KAFKA_SASL_MECHANISM")) == "" && os.Getenv("KAFKA_TLS_CERT_FILE") == "" {
		return Evidence{}, errors.New("learning evidence requires Kafka client credentials in production")
	}
	options, err := kafkaOptions(production)
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{Enabled: true, ContentTopic: topic, KafkaOptions: options}, nil
}

var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

func validKafkaTopic(topic string) bool {
	return topic != "." && topic != ".." && kafkaTopicName.MatchString(topic)
}
