package resources

import (
	"context"
	"net"
	"strings"
	"testing"

	contentpb "github.com/overmindv/content/pkg/api/content"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type materialRPCFixture struct {
	contentpb.UnimplementedContentServiceServer
	contentpb.UnimplementedMaterialCatalogServiceServer
	token string
}

func (s *materialRPCFixture) authorized(ctx context.Context) bool {
	auth := metadata.ValueFromIncomingContext(ctx, "authorization")

	return len(auth) == 1 && auth[0] == "Bearer "+s.token && len(metadata.ValueFromIncomingContext(ctx, "x-user-id")) == 0
}

func (s *materialRPCFixture) SnapshotMaterials(ctx context.Context, request *contentpb.SnapshotMaterialsRequest) (*contentpb.SnapshotMaterialsResponse, error) {
	if !s.authorized(ctx) || request.GetPageSize() != pageLimit || request.GetWatermark() != 3 {
		return nil, status.Error(codes.PermissionDenied, "wrong service metadata or page bounds")
	}

	return &contentpb.SnapshotMaterialsResponse{Watermark: 3, Entries: []*contentpb.MaterialSnapshotEntry{
		{MaterialId: resourceID, RevisionId: revisionID, Version: 2, Metadata: &contentpb.MaterialSnapshotMetadata{
			Title: "A published note", Type: "note", Format: "typst", TopicIds: []string{topicID}, EstimatedMinutes: 12,
		}},
	}}, nil
}

func (s *materialRPCFixture) CheckMaterialAvailability(ctx context.Context, request *contentpb.CheckMaterialAvailabilityRequest) (*contentpb.CheckMaterialAvailabilityResponse, error) {
	if !s.authorized(ctx) || len(request.Resources) != 1 {
		return nil, status.Error(codes.PermissionDenied, "wrong service metadata or batch")
	}

	return &contentpb.CheckMaterialAvailabilityResponse{Resources: []*contentpb.MaterialVersionKey{
		{MaterialId: resourceID, RevisionId: revisionID},
	}}, nil
}

func TestContentClientUsesCatalogScopeAndExactVersions(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("c", 32)
	server := grpc.NewServer()
	fixture := &materialRPCFixture{token: token}
	contentpb.RegisterContentServiceServer(server, fixture)
	contentpb.RegisterMaterialCatalogServiceServer(server, fixture)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := NewContentClient(listener.Addr().String(), token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	watermark := int64(3)
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", resourceID, "authorization", "another credential"))
	got, err := client.Snapshot(ctx, &watermark, "")
	if err != nil || len(got.Records) != 1 || got.Records[0].Metadata == nil ||
		got.Records[0].Metadata.EstimatedMinutes == nil || *got.Records[0].Metadata.EstimatedMinutes != 12 {
		t.Fatal(got, err)
	}
	key := VersionKey{resourceID, revisionID}
	available, err := client.Available(ctx, []VersionKey{key})
	if err != nil || !available[key] {
		t.Fatal(available, err)
	}
	if _, err := client.Available(ctx, []VersionKey{{resourceID, topicID}}); err == nil {
		t.Fatal("owner confirmed another version than requested")
	}
}
