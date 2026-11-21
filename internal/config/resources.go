package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const defaultMaterialTopic = "content.material.v1"

type Resources struct {
	Enabled        bool
	TasksURL       string
	TasksToken     string
	ContentAddress string
	ContentToken   string
	MaterialTopic  string
	KafkaOptions   []kgo.Opt
}

func loadResources(cfg Config) (Resources, error) {
	enabled, err := envSwitch("RECOMMENDATIONS_RESOURCE_PROJECTION_ENABLED")
	if err != nil || !enabled {
		return Resources{}, err
	}
	r := Resources{
		Enabled: true, TasksURL: strings.TrimSpace(os.Getenv("TASKS_RESOURCE_URL")),
		TasksToken: os.Getenv("TASKS_RESOURCE_TOKEN"), ContentAddress: strings.TrimSpace(os.Getenv("CONTENT_CATALOG_ADDRESS")),
		ContentToken: os.Getenv("CONTENT_GRPC_CATALOG_TOKEN"), MaterialTopic: strings.TrimSpace(os.Getenv("KAFKA_CONTENT_MATERIAL_TOPIC")),
	}
	if r.MaterialTopic == "" {
		r.MaterialTopic = defaultMaterialTopic
	}
	if cfg.EntitiesURL == "" || cfg.CatalogSnapshotToken == "" || r.TasksURL == "" || r.TasksToken == "" || r.ContentAddress == "" || r.ContentToken == "" {
		return Resources{}, errors.New("resource projection requires Entities, Tasks and Content catalog endpoints and credentials")
	}
	seen := make(map[string]bool, 4)
	for _, token := range []string{cfg.ServiceToken, cfg.CatalogSnapshotToken, r.TasksToken, r.ContentToken} {
		if seen[token] {
			return Resources{}, errors.New("catalog and service credentials must be distinct")
		}
		seen[token] = true
	}
	production := strings.TrimSpace(os.Getenv("ENV")) == "production"
	if production && (!strings.HasPrefix(r.TasksURL, "https://") || !strings.HasPrefix(r.ContentAddress, "grpcs://") ||
		!strings.HasPrefix(cfg.EntitiesURL, "https://")) {
		return Resources{}, errors.New("resource projection requires encrypted catalog connections in production")
	}
	r.KafkaOptions, err = kafkaOptions(production)
	if err != nil {
		return Resources{}, err
	}

	return r, nil
}

func kafkaOptions(production bool) ([]kgo.Opt, error) {
	encrypted, err := envSwitch("KAFKA_TLS_ENABLED")
	if err != nil {
		return nil, err
	}
	ca := os.Getenv("KAFKA_TLS_CA_FILE")
	cert := os.Getenv("KAFKA_TLS_CERT_FILE")
	key := os.Getenv("KAFKA_TLS_KEY_FILE")
	serverName := os.Getenv("KAFKA_TLS_SERVER_NAME")
	mechanism := strings.ToUpper(strings.TrimSpace(os.Getenv("KAFKA_SASL_MECHANISM")))
	user, password := os.Getenv("KAFKA_SASL_USERNAME"), os.Getenv("KAFKA_SASL_PASSWORD")
	if (cert == "") != (key == "") || (!encrypted && (production || ca != "" || cert != "" || serverName != "" || mechanism != "")) {
		return nil, errors.New("invalid Kafka TLS settings")
	}
	if (mechanism == "" && (user != "" || password != "")) || (mechanism != "" && (user == "" || password == "")) {
		return nil, errors.New("invalid Kafka SASL settings")
	}
	opts := make([]kgo.Opt, 0, 2)
	if encrypted {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
		if ca != "" {
			pem, err := os.ReadFile(ca) //nolint:gosec // Путь к CA задает оператор при запуске.
			if err != nil {
				return nil, errors.New("could not read Kafka CA")
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("invalid Kafka CA")
			}
			tlsConfig.RootCAs = roots
		}
		if cert != "" {
			pair, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				return nil, errors.New("invalid Kafka client certificate")
			}
			tlsConfig.Certificates = []tls.Certificate{pair}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	switch mechanism {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: password}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()))
	default:
		return nil, errors.New("unsupported Kafka SASL mechanism")
	}

	return opts, nil
}

func envSwitch(key string) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(value)
	if err != nil {
		return false, errors.New("invalid " + key)
	}

	return on, nil
}
