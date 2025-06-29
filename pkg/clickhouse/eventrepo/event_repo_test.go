//go:generate go tool mockgen -source=./eventrepo.go -destination=eventrepo_mock_test.go -package=eventrepo_test
package eventrepo_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chconfig "github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
	"github.com/DIMO-Network/clickhouse-infra/pkg/container"
	"github.com/DIMO-Network/cloudevent"
	chindexer "github.com/DIMO-Network/cloudevent/pkg/clickhouse"
	"github.com/DIMO-Network/cloudevent/pkg/clickhouse/eventrepo"
	"github.com/DIMO-Network/cloudevent/pkg/clickhouse/migrations"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var dataType = "small"

// setupClickHouseContainer starts a ClickHouse container for testing and returns the connection.
func setupClickHouseContainer(t *testing.T) *container.Container {
	ctx := context.Background()
	settings := chconfig.Settings{
		User:     "default",
		Database: "dimo",
	}

	chContainer, err := container.CreateClickHouseContainer(ctx, settings)
	require.NoError(t, err)

	chDB, err := chContainer.GetClickhouseAsDB()
	require.NoError(t, err)

	// Ensure we terminate the container at the end
	t.Cleanup(func() {
		chContainer.Terminate(ctx)
	})

	err = migrations.RunGoose(ctx, []string{"up"}, chDB)
	require.NoError(t, err)

	return chContainer
}

// insertTestData inserts test data into ClickHouse.
func insertTestData(t *testing.T, ctx context.Context, conn clickhouse.Conn, index *cloudevent.CloudEventHeader) string {
	values := chindexer.CloudEventToSlice(index)

	err := conn.Exec(ctx, chindexer.InsertStmt, values...)
	require.NoError(t, err)
	return values[len(values)-1].(string)
}

// TestGetLatestIndexKey tests the GetLatestIndexKey function.
func TestGetLatestIndexKey(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	contractAddr := randAddress()
	device1TokenID := big.NewInt(1234567890)
	device2TokenID := big.NewInt(976543210)
	ctx := context.Background()
	now := time.Now()

	// Create test indices
	eventIdx1 := &cloudevent.CloudEventHeader{
		Subject: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		DataVersion: dataType,
		Time:        now.Add(-1 * time.Hour),
	}

	eventIdx2 := &cloudevent.CloudEventHeader{
		Subject: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		DataVersion: dataType,
		Time:        now,
	}

	// Insert test data
	_ = insertTestData(t, ctx, conn, eventIdx1)
	indexKey2 := insertTestData(t, ctx, conn, eventIdx2)

	tests := []struct {
		name          string
		subject       cloudevent.ERC721DID
		expectedKey   string
		expectedError bool
	}{
		{
			name: "valid latest object",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device1TokenID,
			},
			expectedKey: indexKey2,
		},
		{
			name: "no records",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device2TokenID,
			},
			expectedError: true,
		},
	}

	indexService := eventrepo.New(conn, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &eventrepo.SearchOptions{
				DataVersion: &dataType,
				Subject:     ref(tt.subject.String()),
			}
			metadata, err := indexService.GetLatestIndex(context.Background(), opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedKey, metadata.Data.Key)
			}
		})
	}
}

// TestGetDataFromIndex tests the GetDataFromIndex function.
func TestGetDataFromIndex(t *testing.T) {
	chContainer := setupClickHouseContainer(t)
	contractAddr := randAddress()
	device1TokenID := big.NewInt(1234567890)
	device2TokenID := big.NewInt(976543210)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	eventIdx := &cloudevent.CloudEventHeader{
		Subject: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		DataVersion: dataType,
		Time:        time.Now().Add(-1 * time.Hour),
	}

	_ = insertTestData(t, ctx, conn, eventIdx)

	tests := []struct {
		name            string
		subject         cloudevent.ERC721DID
		expectedContent []byte
		expectedError   bool
	}{
		{
			name: "valid object content",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device1TokenID,
			},
			expectedContent: []byte(`{"vin": "1HGCM82633A123456"}`),
		},
		{
			name: "no records",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device2TokenID,
			},
			expectedError: true,
		},
	}

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(content)),
		ContentLength: ref(int64(len(content))),
	}, nil).AnyTimes()

	indexService := eventrepo.New(conn, mockS3Client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &eventrepo.SearchOptions{
				DataVersion: &dataType,
				Subject:     ref(tt.subject.String()),
			}
			content, err := indexService.GetLatestCloudEvent(context.Background(), "test-bucket", opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, tt.expectedContent, []byte(content.Data))
			}
		})
	}
}

func TestStoreObject(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	mockS3Client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.PutObjectOutput{}, nil).AnyTimes()

	indexService := eventrepo.New(conn, mockS3Client)

	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	did := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: randAddress(),
		TokenID:         big.NewInt(123456),
	}

	event := cloudevent.CloudEvent[json.RawMessage]{
		CloudEventHeader: cloudevent.CloudEventHeader{
			Subject:     did.String(),
			Time:        time.Now(),
			DataVersion: dataType,
		},
		Data: content,
	}
	err = indexService.StoreObject(ctx, "test-bucket", &event.CloudEventHeader, event.Data)
	require.NoError(t, err)

	// Verify the data is stored in ClickHouse
	opts := &eventrepo.SearchOptions{
		DataVersion: &dataType,
		Subject:     ref(did.String()),
	}
	metadata, err := indexService.GetLatestIndex(ctx, opts)
	require.NoError(t, err)
	expectedIndexKey := chindexer.CloudEventToObjectKey(&event.CloudEventHeader)
	require.NoError(t, err)
	require.Equal(t, expectedIndexKey, metadata.Data.Key)
}

// TestGetData tests the GetData function with different SearchOptions combinations.
func TestGetData(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	source1 := randAddress()
	contractAddr := randAddress()
	device1TokenID := big.NewInt(123456)
	device2TokenID := big.NewInt(654321)
	ctx := context.Background()
	now := time.Now()
	eventDID := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         device1TokenID,
	}
	eventIdx := cloudevent.CloudEventHeader{
		Subject: eventDID.String(),
		Time:    now.Add(-4 * time.Hour),
		Producer: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		Type:        cloudevent.TypeStatus,
		Source:      source1.Hex(),
		DataVersion: dataType,
	}
	indexKey1 := insertTestData(t, ctx, conn, &eventIdx)
	eventIdx2 := eventIdx
	eventIdx2.Time = now.Add(-3 * time.Hour)
	indexKey2 := insertTestData(t, ctx, conn, &eventIdx2)
	eventIdx3 := eventIdx
	eventIdx3.Time = now.Add(-2 * time.Hour)
	eventIdx3.Type = cloudevent.TypeFingerprint
	indexKey3 := insertTestData(t, ctx, conn, &eventIdx3)
	eventIdx4 := eventIdx
	eventIdx4.Time = now.Add(-1 * time.Hour)
	eventIdx4.DataContentType = "utf-8"
	indexKey4 := insertTestData(t, ctx, conn, &eventIdx4)

	tests := []struct {
		name              string
		opts              *eventrepo.SearchOptions
		expectedIndexKeys []string
		expectedError     bool
	}{
		{
			name: "valid data with address",
			opts: &eventrepo.SearchOptions{
				DataVersion: &dataType,
				Subject:     ref(eventDID.String()),
			},
			expectedIndexKeys: []string{indexKey4, indexKey3, indexKey2, indexKey1},
		},
		{
			name: "no records with address",
			opts: &eventrepo.SearchOptions{
				DataVersion: &dataType,
				Subject: ref(cloudevent.ERC721DID{
					ChainID:         153,
					ContractAddress: contractAddr,
					TokenID:         device2TokenID,
				}.String()),
			},
			expectedIndexKeys: nil,
			expectedError:     true,
		},
		{
			name: "data within time range",
			opts: &eventrepo.SearchOptions{
				DataVersion: &dataType,
				After:       now.Add(-3 * time.Hour),
				Before:      now.Add(-1 * time.Minute),
			},
			expectedIndexKeys: []string{indexKey4, indexKey3},
		},
		{
			name: "data with type filter",
			opts: &eventrepo.SearchOptions{
				DataVersion: &dataType,
				Type:        ref(cloudevent.TypeStatus),
			},
			expectedIndexKeys: []string{indexKey4, indexKey2, indexKey1},
		},
		{
			name:              "data with nil options",
			opts:              nil,
			expectedIndexKeys: []string{indexKey4, indexKey3, indexKey2, indexKey1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockS3Client := NewMockObjectGetter(ctrl)

			indexService := eventrepo.New(conn, mockS3Client)
			var expectedContent [][]byte
			for _, indexKey := range tt.expectedIndexKeys {
				mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					require.Equal(t, *params.Key, indexKey)
					quotedKey := `"` + indexKey + `"`
					// content := []byte(`{"data":` + quotedKey + `}`)
					expectedContent = append(expectedContent, []byte(quotedKey))
					return &s3.GetObjectOutput{
						Body:          io.NopCloser(bytes.NewReader([]byte(quotedKey))),
						ContentLength: ref(int64(len(quotedKey))),
					}, nil
				})
			}
			events, err := indexService.ListCloudEvents(context.Background(), "test-bucket", 10, tt.opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, events, len(expectedContent))
				for i, content := range expectedContent {
					require.Equal(t, string(content), string(events[i].Data))
				}
			}
		})
	}
}

// TestGetEventWithAllHeaderFields tests retrieving events with all header fields properly populated
func TestGetEventWithAllHeaderFields(t *testing.T) {
	chContainer := setupClickHouseContainer(t)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Create a DID for the test
	contractAddr := randAddress()
	deviceTokenID := big.NewInt(1234567890)
	eventDID1 := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         deviceTokenID,
	}
	eventDID2 := cloudevent.ERC721DID{
		ChainID:         151,
		ContractAddress: contractAddr,
		TokenID:         deviceTokenID,
	}

	// Create event with all header fields populated
	fullHeaderEvent := cloudevent.CloudEventHeader{
		ID:              "test-id-123456",
		Source:          "test-source",
		Producer:        "test-producer",
		Subject:         eventDID1.String(),
		Time:            now,
		Type:            cloudevent.TypeStatus,
		DataContentType: "application/json",
		DataSchema:      "https://example.com/schemas/status.json",
		DataVersion:     dataType,
		SpecVersion:     cloudevent.SpecVersion,
		Signature:       "0x1234567890",
		Extras: map[string]any{
			"extraField": "extra-value",
		},
	}

	fullHeaderEvent2 := fullHeaderEvent
	fullHeaderEvent2.Subject = eventDID2.String()
	fullHeaderEvent2.Signature = ""
	fullHeaderEvent2.Extras = map[string]any{
		"signature": "0x09876543210",
	}

	// Insert the event
	indexKey := insertTestData(t, ctx, conn, &fullHeaderEvent)
	indexKey2 := insertTestData(t, ctx, conn, &fullHeaderEvent2)

	// Setup mock S3 client for retrieving the event data
	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	eventData := []byte(`{"status": "online", "lastSeen": "2023-01-01T12:00:00Z"}`)

	// Create service
	indexService := eventrepo.New(conn, mockS3Client)

	// Test retrieving the event
	t.Run("retrieve event with full headers", func(t *testing.T) {
		mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				// Verify the correct key was requested
				require.Equal(t, indexKey, *params.Key)
				return &s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(eventData)),
					ContentLength: ref(int64(len(eventData))),
				}, nil
			},
		)
		opts := &eventrepo.SearchOptions{
			DataVersion: &dataType,
			Subject:     ref(eventDID1.String()),
		}

		retrievedEvent, err := indexService.GetLatestCloudEvent(ctx, "test-bucket", opts)
		require.NoError(t, err)

		// Verify all header fields
		assert.Equal(t, fullHeaderEvent.ID, retrievedEvent.ID, "ID mismatch")
		assert.Equal(t, fullHeaderEvent.Source, retrievedEvent.Source, "Source mismatch")
		assert.Equal(t, fullHeaderEvent.Producer, retrievedEvent.Producer, "Producer mismatch")
		assert.Equal(t, fullHeaderEvent.Subject, retrievedEvent.Subject, "Subject mismatch")
		assert.Equal(t, fullHeaderEvent.Time.UTC().Truncate(time.Second), retrievedEvent.Time.UTC().Truncate(time.Second), "Time mismatch")
		assert.Equal(t, fullHeaderEvent.Type, retrievedEvent.Type, "Type mismatch")
		assert.Equal(t, fullHeaderEvent.DataContentType, retrievedEvent.DataContentType, "DataContentType mismatch")
		assert.Equal(t, fullHeaderEvent.DataSchema, retrievedEvent.DataSchema, "DataSchema mismatch")
		assert.Equal(t, fullHeaderEvent.DataVersion, retrievedEvent.DataVersion, "DataVersion mismatch")
		assert.Equal(t, cloudevent.SpecVersion, retrievedEvent.SpecVersion, "SpecVersion mismatch")
		assert.Equal(t, fullHeaderEvent.Signature, retrievedEvent.Signature, "Signature mismatch")

		// Verify extras
		require.NotNil(t, retrievedEvent.Extras)
		require.Equal(t, 2, len(retrievedEvent.Extras))
		require.Equal(t, "extra-value", retrievedEvent.Extras["extraField"])
		require.Equal(t, fullHeaderEvent.Signature, retrievedEvent.Extras["signature"].(string))

		// Verify data content
		require.Equal(t, string(eventData), string(retrievedEvent.Data))
	})

	t.Run("retrieve event with signature that is originally in extras", func(t *testing.T) {
		mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				// Verify the correct key was requested
				require.Equal(t, indexKey2, *params.Key)
				return &s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(eventData)),
					ContentLength: ref(int64(len(eventData))),
				}, nil
			},
		)
		opts := &eventrepo.SearchOptions{
			DataVersion: &dataType,
			Subject:     ref(eventDID2.String()),
		}
		retrievedEvent, err := indexService.GetLatestCloudEvent(ctx, "test-bucket", opts)
		require.NoError(t, err)

		// Verify all header fields
		assert.Equal(t, fullHeaderEvent2.ID, retrievedEvent.ID, "ID mismatch")
		assert.Equal(t, fullHeaderEvent2.Extras["signature"], retrievedEvent.Signature, "Signature field not set correctly")
		assert.Equal(t, fullHeaderEvent2.Extras["signature"], retrievedEvent.Extras["signature"], "Signature field not set correctly in extras")
	})
}

func ref[T any](x T) *T {
	return &x
}

func randAddress() common.Address {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		log.Fatalf("Failed to generate private key: %v", err)
	}
	return crypto.PubkeyToAddress(privateKey.PublicKey)
}
