package resources

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/overmindv/content/pkg/api/activity"
	contentpb "github.com/overmindv/content/pkg/api/content"
	"github.com/overmindv/recommendations/internal/planner"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const maxMaterialEventBytes = 32 << 10

type ContentClient struct {
	conn         *grpc.ClientConn
	snapshots    contentpb.ContentServiceClient
	availability contentpb.MaterialCatalogServiceClient
	bearer       string
}

func NewContentClient(address, token string) (*ContentClient, error) {
	if !validToken(token, 256) {
		return nil, errors.New("invalid CONTENT_GRPC_CATALOG_TOKEN")
	}
	security := insecure.NewCredentials()
	if target, encrypted := strings.CutPrefix(address, "grpcs://"); encrypted {
		address = target
		security = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 1 || number > 65535 || host == "" ||
		strings.ContainsAny(address, "/\\@?#% \t\r\n") {
		return nil, errors.New("invalid CONTENT_CATALOG_ADDRESS")
	}
	conn, err := grpc.NewClient("dns:///"+address, grpc.WithTransportCredentials(security), grpc.WithNoProxy(),
		grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxPageBytes), grpc.MaxCallSendMsgSize(maxMaterialEventBytes)))
	if err != nil {
		return nil, errors.New("could not initialize content catalog connection")
	}
	return contentClient(conn, token), nil
}

func contentClient(conn *grpc.ClientConn, token string) *ContentClient {
	return &ContentClient{
		conn: conn, snapshots: contentpb.NewContentServiceClient(conn),
		availability: contentpb.NewMaterialCatalogServiceClient(conn), bearer: "Bearer " + token,
	}
}

func (c *ContentClient) Close() error { return c.conn.Close() }

func (c *ContentClient) Snapshot(ctx context.Context, watermark *int64, cursor string) (SnapshotPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", c.bearer))
	out, err := c.snapshots.SnapshotMaterials(ctx, &contentpb.SnapshotMaterialsRequest{
		Watermark: watermark, AfterId: cursor, PageSize: pageLimit,
	})
	if err != nil {
		return SnapshotPage{}, errors.New("content resource snapshot failed")
	}
	if out == nil || len(out.Entries) > pageLimit {
		return SnapshotPage{}, errInvalidPage
	}
	page := SnapshotPage{Watermark: out.Watermark, NextCursor: out.NextAfterId, Records: make([]Record, 0, len(out.Entries))}
	for _, entry := range out.Entries {
		if entry == nil || (entry.Tombstone && entry.Metadata != nil) {
			return SnapshotPage{}, errInvalidPage
		}
		record := Record{ID: entry.MaterialId, Version: entry.Version}
		if m := entry.Metadata; m != nil {
			record.Metadata = materialMetadata(entry.RevisionId, m.Title, m.Type, m.Format, m.EstimatedMinutes, m.TopicIds)
		}
		record, err := record.normalized(Content)
		if err != nil {
			return SnapshotPage{}, err
		}
		page.Records = append(page.Records, record)
	}
	if err := validateSnapshot(page, watermark, cursor); err != nil {
		return SnapshotPage{}, err
	}
	return page, nil
}

func materialMetadata(revision, title, kind, format string, minutes int32, topicIDs []string) *Metadata {
	m := &Metadata{RevisionID: revision, Title: title, Type: kind, Format: format}
	if minutes != 0 {
		value := int(minutes)
		m.EstimatedMinutes = &value
	}
	if len(topicIDs) <= 8 {
		for _, id := range topicIDs {
			m.Topics = append(m.Topics, TopicWeight{ID: id, Weight: 1})
		}
	}
	return m
}

func (c *ContentClient) Available(ctx context.Context, keys []VersionKey) (map[VersionKey]bool, error) {
	requested, err := validateVersionKeys(keys)
	if err != nil {
		return nil, err
	}
	input := &contentpb.CheckMaterialAvailabilityRequest{Resources: make([]*contentpb.MaterialVersionKey, 0, len(keys))}
	for _, key := range keys {
		input.Resources = append(input.Resources, &contentpb.MaterialVersionKey{MaterialId: key.ID, RevisionId: key.RevisionID})
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", c.bearer))
	out, err := c.availability.CheckMaterialAvailability(ctx, input)
	if err != nil {
		return nil, errors.New("content resource availability failed")
	}
	if out == nil || len(out.Resources) > len(keys) {
		return nil, errInvalidPage
	}
	for _, key := range out.Resources {
		if err := confirmedVersion(requested, VersionKey{ID: key.GetMaterialId(), RevisionID: key.GetRevisionId()}); err != nil {
			return nil, err
		}
	}
	return requested, nil
}

func materialEvent(key, raw []byte) (Event, error) {
	var wire activity.Event[activity.MaterialState]
	if err := decodeResourceJSON(bytes.NewReader(raw), &wire, maxMaterialEventBytes); err != nil {
		return Event{}, errInvalidEvent
	}
	if wire.SchemaVersion != activity.SchemaVersion || wire.OccurredAt.IsZero() ||
		!planner.ValidID(wire.EventID) || wire.AggregateVersion < 1 ||
		!bytes.Equal(key, []byte(wire.AggregateID)) {
		return Event{}, errInvalidEvent
	}
	record := Record{ID: wire.AggregateID, Version: wire.AggregateVersion}
	switch wire.EventType {
	case activity.MaterialWithdrawn, activity.MaterialArchived:
		if !wire.Payload.Tombstone || wire.Payload.Metadata != nil {
			return Event{}, errInvalidEvent
		}
	case activity.MaterialPublished, activity.MaterialRestored, activity.MaterialCorrected:
		if wire.Payload.Tombstone {
			return Event{}, errInvalidEvent
		}
		if m := wire.Payload.Metadata; m != nil {
			record.Metadata = materialMetadata(wire.RevisionID, m.Title, m.Type, m.Format, m.EstimatedMinutes, m.TopicIDs)
		}
	default:
		return Event{}, errInvalidEvent
	}
	record, err := record.normalized(Content)
	if err != nil {
		return Event{}, err
	}
	return Event{ID: wire.EventID, Fingerprint: sha256.Sum256(raw), Record: record}, nil
}
