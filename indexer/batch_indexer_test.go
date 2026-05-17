//go:build vectors

package indexer

import (
	"sync"
	"testing"

	"github.com/rkfg/guiltyspark/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBleveClient implements a minimal BleveClientInterface for testing BatchIndexer
type mockBleveClient struct {
	mu               sync.Mutex
	Documents        map[string]IndexedDocument
	Events           map[string]bool // event IDs
	EventsExist      map[string]bool // cached event IDs
	FlushCalled      bool
	EventFlushCalled bool
}

func newMockBleveClient() *mockBleveClient {
	return &mockBleveClient{
		Documents:   make(map[string]IndexedDocument),
		Events:      make(map[string]bool),
		EventsExist: make(map[string]bool),
	}
}

func (m *mockBleveClient) IsEventIDExists(eventID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.EventsExist[eventID] {
		return true, nil
	}
	if m.Events[eventID] {
		return true, nil
	}
	return false, nil
}

func (m *mockBleveClient) IndexDocumentStruct(doc IndexedDocument) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Documents[doc.ID] = doc
	return nil
}

func (m *mockBleveClient) AddEventID(eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Events[eventID] = true
	m.EventsExist[eventID] = true
	return nil
}

func (m *mockBleveClient) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FlushCalled = true
	return nil
}

func (m *mockBleveClient) FlushEventID() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.EventFlushCalled = true
	return nil
}

func (m *mockBleveClient) CountDocuments() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Documents), nil
}

// mockEmbedClient implements a minimal EmbedClientInterface for testing
type mockEmbedClient struct {
	CreateEmbeddingCalls int
}

func newMockEmbedClient() *mockEmbedClient {
	return &mockEmbedClient{}
}

func (m *mockEmbedClient) CreateEmbedding(text, prefix string) ([]float32, error) {
	m.CreateEmbeddingCalls++
	return []float32{0.1, 0.2, 0.3}, nil
}

func TestSaveLoadDeferred_table(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial deferred data
	deferredImages := []PendingImage{
		{EventID: "img1", RoomID: "room1", UserID: "@bob:matrix.org", Timestamp: 1609459200000, RawURL: "mxc://server.org/img1", FileName: "img1.jpg", MimeType: "image/jpeg"},
		{EventID: "img2", RoomID: "room1", UserID: "@alice:matrix.org", Timestamp: 1609459201000, RawURL: "mxc://server.org/img2", FileName: "img2.jpg", MimeType: "image/jpeg"},
	}
	deferredTextEmbed := []PendingMessage{
		{EventID: "text1", RoomID: "room1", UserID: "@bob:matrix.org", Timestamp: 1609459200000, EventType: "m.room.message", Text: "first message"},
		{EventID: "text2", RoomID: "room1", UserID: "@alice:matrix.org", Timestamp: 1609459201000, EventType: "m.room.message", Text: "second message"},
	}

	// Create BatchIndexer with mock clients
	bc := newMockBleveClient()
	ec := newMockEmbedClient()
	ip := &ImageProcessor{
		cfg: &config.ImageProcConfig{
			CacheDir: tmpDir,
		},
	}

	bi := NewBatchIndexer(5, 0, tmpDir, bc, ec, ip)
	defer bi.Stop()

	// Set up deferred state directly (since loadDeferred is called in NewBatchIndexer)
	bi.deferredImages = append(bi.deferredImages, deferredImages...)
	bi.deferredTextEmbed = append(bi.deferredTextEmbed, deferredTextEmbed...)

	// Save deferred data
	bi.saveDeferred()

	// Create a new BatchIndexer with the same temp dir — it should load the deferred data
	bc2 := newMockBleveClient()
	ec2 := newMockEmbedClient()
	ip2 := &ImageProcessor{
		cfg: &config.ImageProcConfig{
			CacheDir: tmpDir,
		},
	}

	bi2 := NewBatchIndexer(5, 0, tmpDir, bc2, ec2, ip2)
	defer bi2.Stop()

	// Verify loaded deferred data
	require.Equal(t, len(deferredImages), len(bi2.deferredImages), "deferred images count mismatch")
	require.Equal(t, len(deferredTextEmbed), len(bi2.deferredTextEmbed), "deferred texts count mismatch")

	// Check individual items
	for i, expected := range deferredImages {
		assert.Equal(t, expected.EventID, bi2.deferredImages[i].EventID)
		assert.Equal(t, expected.RoomID, bi2.deferredImages[i].RoomID)
	}
	for i, expected := range deferredTextEmbed {
		assert.Equal(t, expected.EventID, bi2.deferredTextEmbed[i].EventID)
		assert.Equal(t, expected.Text, bi2.deferredTextEmbed[i].Text)
	}
}
