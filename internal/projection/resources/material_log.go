package resources

import (
	"context"
	"encoding/hex"
	"errors"
	"maps"
	"math"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	errResnapshot       = errors.New("material log needs a new snapshot")
	errKafkaUnavailable = errors.New("material log unavailable")
)

const maxMaterialPartitions = 128

type materialBounds struct {
	id    string
	start map[int32]int64
	end   map[int32]int64
}

type MaterialLog struct {
	topic    string
	opts     []kgo.Opt
	info     *kgo.Client
	reader   *kgo.Client
	readPos  KafkaPosition
	boundsAt time.Time
}

// Один экземпляр читает один поток. При смене checkpoint буфер старого чтения больше не используется.
func NewMaterialLog(topic string, opts ...kgo.Opt) (*MaterialLog, error) {
	if topic == "" || len(topic) > 249 || topic == "." || topic == ".." ||
		strings.ContainsFunc(topic, func(r rune) bool {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
				return false
			default:
				return true
			}
		}) {
		return nil, errors.New("invalid material Kafka topic")
	}
	info, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errKafkaUnavailable
	}
	return &MaterialLog{topic: topic, opts: append([]kgo.Opt(nil), opts...), info: info}, nil
}

func (l *MaterialLog) Close() {
	l.resetReader()
	l.info.Close()
}

func (l *MaterialLog) Start(ctx context.Context) (KafkaPosition, error) {
	bounds, err := l.loadBounds(ctx)
	if err != nil {
		return KafkaPosition{}, err
	}
	l.resetReader()
	// Следующий poll перепроверит, не изменился ли топик за время загрузки снимка.
	l.boundsAt = time.Time{}
	return KafkaPosition{TopicID: bounds.id, Offsets: bounds.end}, nil
}

func (l *MaterialLog) Read(ctx context.Context, pos KafkaPosition) (KafkaPosition, []Event, error) {
	same := l.reader != nil && l.readPos.TopicID == pos.TopicID && maps.Equal(l.readPos.Offsets, pos.Offsets)
	if !same || time.Since(l.boundsAt) > 30*time.Second {
		bounds, err := l.loadBounds(ctx)
		if err != nil {
			return KafkaPosition{}, nil, err
		}
		if !bounds.contains(pos) {
			l.resetReader()
			return KafkaPosition{}, nil, errResnapshot
		}
		l.boundsAt = time.Now()
	}
	if !same {
		if err := l.openReader(pos); err != nil {
			return KafkaPosition{}, nil, err
		}
	}
	pollCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	fetches := l.reader.PollRecords(pollCtx, pageLimit)
	if problem := materialFetchError(fetches); problem != nil {
		if errors.Is(problem, context.DeadlineExceeded) && ctx.Err() == nil {
			return pos, nil, nil
		}
		l.resetReader()
		return KafkaPosition{}, nil, problem
	}
	next := KafkaPosition{TopicID: pos.TopicID, Offsets: maps.Clone(pos.Offsets)}
	var events []Event
	for record := range fetches.RecordsAll() {
		prior, known := next.Offsets[record.Partition]
		if record.Topic != l.topic || !known || record.Offset < prior || record.Offset == math.MaxInt64 {
			l.resetReader()
			return KafkaPosition{}, nil, errResnapshot
		}
		event, err := materialEvent(record.Key, record.Value)
		if err != nil {
			l.resetReader()
			return KafkaPosition{}, nil, err
		}
		events = append(events, event)
		next.Offsets[record.Partition] = record.Offset + 1
	}
	l.readPos = next
	return next, events, nil
}

func (l *MaterialLog) openReader(pos KafkaPosition) error {
	l.resetReader()
	partitions := make(map[int32]kgo.Offset, len(pos.Offsets))
	for p, offset := range pos.Offsets {
		partitions[p] = kgo.NewOffset().At(offset)
	}
	opts := append(append([]kgo.Opt(nil), l.opts...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{l.topic: partitions}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.FetchMaxBytes(2<<20), kgo.FetchMaxPartitionBytes(1<<20),
		kgo.MaxConcurrentFetches(2), kgo.FetchMaxWait(200*time.Millisecond))
	reader, err := kgo.NewClient(opts...)
	if err != nil {
		return errKafkaUnavailable
	}
	l.reader, l.readPos = reader, pos
	return nil
}

func (l *MaterialLog) resetReader() {
	if l.reader != nil {
		l.reader.Close()
		l.reader = nil
	}
}

func materialFetchError(fetches kgo.Fetches) error {
	errs := fetches.Errors()
	if len(errs) == 0 {
		return nil
	}
	timedOut := true
	for _, fetchErr := range errs {
		if errors.Is(fetchErr.Err, kerr.OffsetOutOfRange) {
			return errResnapshot
		}
		timedOut = timedOut && errors.Is(fetchErr.Err, context.DeadlineExceeded)
	}
	if timedOut && fetches.NumRecords() == 0 {
		return context.DeadlineExceeded
	}
	return errKafkaUnavailable
}

func (b materialBounds) contains(pos KafkaPosition) bool {
	if pos.TopicID != b.id || len(pos.Offsets) != len(b.start) {
		return false
	}
	for partition, start := range b.start {
		offset, ok := pos.Offsets[partition]
		if !ok || offset < start || offset > b.end[partition] {
			return false
		}
	}
	return true
}

func (l *MaterialLog) loadBounds(ctx context.Context) (materialBounds, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &l.topic}}
	response, err := request.RequestWith(ctx, l.info)
	if err != nil || response == nil || len(response.Topics) != 1 {
		return materialBounds{}, errKafkaUnavailable
	}
	topic := response.Topics[0]
	if topic.Topic == nil || *topic.Topic != l.topic || topic.TopicID == [16]byte{} || topic.ErrorCode != 0 ||
		len(topic.Partitions) == 0 || len(topic.Partitions) > maxMaterialPartitions {
		return materialBounds{}, errKafkaUnavailable
	}
	partitions := make([]int32, 0, len(topic.Partitions))
	for _, p := range topic.Partitions {
		if p.ErrorCode != 0 {
			return materialBounds{}, errKafkaUnavailable
		}
		partitions = append(partitions, p.Partition)
	}
	end, err := l.offsets(ctx, partitions, -1)
	if err != nil {
		return materialBounds{}, err
	}
	start, err := l.offsets(ctx, partitions, -2)
	if err != nil {
		return materialBounds{}, err
	}
	for _, p := range partitions {
		if start[p] > end[p] {
			return materialBounds{}, errKafkaUnavailable
		}
	}
	return materialBounds{id: hex.EncodeToString(topic.TopicID[:]), start: start, end: end}, nil
}

func (l *MaterialLog) offsets(ctx context.Context, partitions []int32, timestamp int64) (map[int32]int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = l.topic
	out := make(map[int32]int64, len(partitions))
	for _, id := range partitions {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition, part.Timestamp = id, timestamp
		topic.Partitions = append(topic.Partitions, part)
		out[id] = -1
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	for _, shard := range l.info.RequestSharded(ctx, request) {
		response, ok := shard.Resp.(*kmsg.ListOffsetsResponse)
		if shard.Err != nil || !ok {
			return nil, errKafkaUnavailable
		}
		for _, result := range response.Topics {
			if result.Topic != l.topic {
				return nil, errKafkaUnavailable
			}
			for _, part := range result.Partitions {
				if prior, expected := out[part.Partition]; !expected || prior != -1 || part.ErrorCode != 0 || part.Offset < 0 {
					return nil, errKafkaUnavailable
				}
				out[part.Partition] = part.Offset
			}
		}
	}
	for _, value := range out {
		if value < 0 {
			return nil, errKafkaUnavailable
		}
	}
	return out, nil
}
