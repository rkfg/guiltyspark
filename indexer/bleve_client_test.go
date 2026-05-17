//go:build vectors

package indexer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldsToDocument_table(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		wantID string
		want   IndexedDocument
	}{
		{
			name:   "full set of fields",
			fields: map[string]any{"event_id": "evt1", "room_id": "#room:server.org", "user_id": "@bob:matrix.org", "text": "hello", "image_desc": "a cat", "raw_url": "https://example.com/img.jpg", "file_name": "img.jpg", "mime_type": "image/jpeg", "event_type": "m.room.message", "timestamp": float64(1609459200000), "vector": []float32{0.1, 0.2, 0.3}},
			wantID: "doc1",
			want: IndexedDocument{
				ID: "doc1", EventID: "evt1", RoomID: "#room:server.org", UserID: "@bob:matrix.org",
				Text: "hello", ImageDesc: "a cat", RawURL: "https://example.com/img.jpg",
				FileName: "img.jpg", MimeType: "image/jpeg", EventType: "m.room.message",
				Timestamp: 1609459200000, Vector: []float32{0.1, 0.2, 0.3},
			},
		},
		{
			name:   "timestamp as string",
			fields: map[string]any{"event_id": "evt1", "timestamp": "1609459200000"},
			wantID: "doc2",
			want: IndexedDocument{ID: "doc2", EventID: "evt1", Timestamp: 1609459200000},
		},
		{
			name:   "empty fields",
			fields: map[string]any{},
			wantID: "doc3",
			want:   IndexedDocument{ID: "doc3"},
		},
		{
			name:   "vector as []any",
			fields: map[string]any{"vector": []any{float32(0.1), float32(0.2), float32(0.3)}},
			wantID: "doc4",
			want:   IndexedDocument{ID: "doc4", Vector: []float32{0.1, 0.2, 0.3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FieldsToDocument(tt.wantID, tt.fields)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBleveClient_integration(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := tmpDir + "/test.bleve"

	// Need to set the analyzer for the test - use a simple one that won't panic
	// The default analyzer should work for basic text search
	bc, err := NewBleveClient(indexPath, 4096, "standard")
	require.NoError(t, err)
	defer bc.Close()

	// Add document
	doc := IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@bob:matrix.org",
		Text:      "hello world",
		Timestamp: 1609459200000,
		EventType: "m.room.message",
	}
	err = bc.IndexDocumentStruct(doc)
	require.NoError(t, err)
	err = bc.AddEventID("evt1")
	require.NoError(t, err)
	err = bc.Flush()
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)

	// CountDocuments
	count, err := bc.CountDocuments()
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// SearchExact
	result, err := bc.SearchExact("hello", "room1")
	require.NoError(t, err)
	assert.Equal(t, 1, int(result.Total))

	// ScanAllDocuments
	count2 := 0
	err = bc.ScanAllDocuments(func(d IndexedDocument) bool {
		count2++
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count2)
}

func TestBleveClient_dedup(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := tmpDir + "/test.bleve"

	bc, err := NewBleveClient(indexPath, 4096, "standard")
	require.NoError(t, err)
	defer bc.Close()

	// Add event ID
	err = bc.AddEventID("evt1")
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)

	// Check exists
	exists, err := bc.IsEventIDExists("evt1")
	require.NoError(t, err)
	assert.True(t, exists)

	// Try to index same document again (should still succeed - dedup is done in batch_indexer)
	doc := IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@bob:matrix.org",
		Text:      "hello world",
		Timestamp: 1609459200000,
		EventType: "m.room.message",
	}
	err = bc.IndexDocumentStruct(doc)
	require.NoError(t, err)
	err = bc.Flush()
	require.NoError(t, err)

	count, err := bc.CountDocuments()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestBleveClient_flush(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := tmpDir + "/test.bleve"

	bc, err := NewBleveClient(indexPath, 4096, "standard")
	require.NoError(t, err)
	defer bc.Close()

	// Add 200 documents (should trigger batch flush at 100)
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("%d", i)
		doc := IndexedDocument{
			ID:        "room1:evt" + id,
			EventID:   "evt" + id,
			RoomID:    "room1",
			UserID:    "@bob:matrix.org",
			Text:      "test document " + id,
			Timestamp: 1609459200000 + int64(i),
			EventType: "m.room.message",
		}
		err = bc.IndexDocumentStruct(doc)
		require.NoError(t, err)
	}
	err = bc.Flush()
	require.NoError(t, err)

	count, err := bc.CountDocuments()
	require.NoError(t, err)
	assert.Equal(t, 200, count)
}
