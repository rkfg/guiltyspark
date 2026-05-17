//go:build vectors

package search

import (
	"fmt"
	"time"
	"testing"

	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/indexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEmbedClientForSearch struct{}

func newMockEmbedClientForSearch() *mockEmbedClientForSearch {
	return &mockEmbedClientForSearch{}
}

func (m *mockEmbedClientForSearch) CreateEmbedding(text, prefix string) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

func createTestBleveClient(t *testing.T) (*indexer.BleveClient, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	bc, err := indexer.NewBleveClient(tmpDir+"/test.bleve", 4096, "standard")
	require.NoError(t, err)
	return bc, func() {
		bc.Close()
	}
}

func addDoc(t *testing.T, bc *indexer.BleveClient, doc indexer.IndexedDocument) {
	t.Helper()
	err := bc.IndexDocumentStruct(doc)
	require.NoError(t, err)
	err = bc.AddEventID(doc.EventID)
	require.NoError(t, err)
	err = bc.Flush()
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)
}

func TestExactSearch_finds_matching_text(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello world",
		Timestamp: ts,
	})
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room2:evt2",
		EventID:   "evt2",
		RoomID:    "room2",
		UserID:    "@bob:example.org",
		Text:      "hello world",
		Timestamp: ts,
	})

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Find in specific room
	result, err := eng.ExactSearch("hello", "room1", "")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Exact))
	assert.Equal(t, "evt1", result.Exact[0].EventID)
	assert.Equal(t, "room1", result.Exact[0].RoomID)

	// Find across all rooms
	result, err = eng.ExactSearch("hello", "", "")
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Exact))
	// Both should be found
	eventIDs := make(map[string]bool)
	for _, r := range result.Exact {
		eventIDs[r.EventID] = true
	}
	assert.True(t, eventIDs["evt1"])
	assert.True(t, eventIDs["evt2"])
}

func TestExactSearch_does_not_find_non_matching_text(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello world",
		Timestamp: ts,
	})

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	result, err := eng.ExactSearch("goodbye", "room1", "")
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Exact))
}

func TestExactSearch_filters_by_room(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello",
		Timestamp: ts,
	})
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room2:evt2",
		EventID:   "evt2",
		RoomID:    "room2",
		UserID:    "@bob:example.org",
		Text:      "hello",
		Timestamp: ts,
	})

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Only room1 should be found
	result, err := eng.ExactSearch("hello", "room1", "")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Exact))
	assert.Equal(t, "room1", result.Exact[0].RoomID)
}

func TestExactSearch_filters_by_user(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello",
		Timestamp: ts,
	})
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room1:evt2",
		EventID:   "evt2",
		RoomID:    "room1",
		UserID:    "@bob:example.org",
		Text:      "hello",
		Timestamp: ts,
	})

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Only @alice's messages
	result, err := eng.ExactSearch("hello", "", "@alice:example.org")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Exact))
	assert.Equal(t, "@alice:example.org", result.Exact[0].UserID)

	// Only @bob's messages
	result, err = eng.ExactSearch("hello", "", "@bob:example.org")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Exact))
	assert.Equal(t, "@bob:example.org", result.Exact[0].UserID)
}

func TestExactSearch_all_stop_words_returns_empty(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Only stop words — should return empty search result (no Bleve query issued)
	result, err := eng.ExactSearch("и в на что это", "", "")
	require.NoError(t, err)
	// Query should be empty since all words are stop words
	assert.Equal(t, 0, len(result.Exact))
}

func TestExactSearch_image_desc_found(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	addDoc(t, bc, indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "some other text",
		ImageDesc: "a cat sitting on a sofa",
		Timestamp: ts,
	})

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	result, err := eng.ExactSearch("cat", "", "")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Exact))
	assert.Equal(t, "a cat sitting on a sofa", result.Exact[0].ImageDesc)
	assert.Equal(t, "some other text", result.Exact[0].Text)
}

func TestExactSearch_limit_results(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	for i := 1; i <= 5; i++ {
		doc := indexer.IndexedDocument{
			ID:        fmt.Sprintf("room1:evt%d", i),
			EventID:   fmt.Sprintf("evt%d", i),
			RoomID:    "room1",
			UserID:    "@alice:example.org",
			Text:      fmt.Sprintf("hello %d", i),
			Timestamp: ts + int64(i)*1000,
		}
		addDoc(t, bc, doc)
	}

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 3})

	result, err := eng.ExactSearch("hello", "", "")
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Exact))
	// Should be limited to 3 results (top scoring)
	eventIDs := make(map[string]bool)
	for _, r := range result.Exact {
		eventIDs[r.EventID] = true
	}
	// All 5 documents match "hello", but only 3 are returned due to limit
	assert.True(t, len(eventIDs) == 3)
}

func TestSemanticSearch_finds_matching_results(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	// Document with vector
	doc := indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello world",
		Timestamp: ts,
		Vector:    []float32{0.1, 0.2, 0.3},
	}
	err := bc.IndexDocumentStruct(doc)
	require.NoError(t, err)
	err = bc.AddEventID("evt1")
	require.NoError(t, err)
	err = bc.Flush()
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Should find the document with matching vector
	result, err := eng.SemanticSearch("hello world", "room1", "")
	require.NoError(t, err)
	assert.Equal(t, "hello world", result.Query)
	assert.Equal(t, 1, len(result.Semantic))
	assert.Equal(t, "evt1", result.Semantic[0].EventID)
	assert.Equal(t, "room1", result.Semantic[0].RoomID)
}

func TestSemanticSearch_does_not_find_non_matching_results(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	// No documents with vectors
	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	result, err := eng.SemanticSearch("hello world", "room1", "")
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Semantic))
}

func TestSemanticSearch_filters_by_room(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	// Document in room1 with vector
	doc1 := indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello",
		Timestamp: ts,
		Vector:    []float32{0.1, 0.2, 0.3},
	}
	err := bc.IndexDocumentStruct(doc1)
	require.NoError(t, err)
	err = bc.AddEventID("evt1")
	require.NoError(t, err)

	// Document in room2 with vector
	doc2 := indexer.IndexedDocument{
		ID:        "room2:evt2",
		EventID:   "evt2",
		RoomID:    "room2",
		UserID:    "@bob:example.org",
		Text:      "hello",
		Timestamp: ts,
		Vector:    []float32{0.1, 0.2, 0.3},
	}
	err = bc.IndexDocumentStruct(doc2)
	require.NoError(t, err)
	err = bc.AddEventID("evt2")
	require.NoError(t, err)
	err = bc.Flush()
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Only room1 should be found
	result, err := eng.SemanticSearch("hello", "room1", "")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Semantic))
	assert.Equal(t, "room1", result.Semantic[0].RoomID)
}

func TestSemanticSearch_filters_by_user(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	doc1 := indexer.IndexedDocument{
		ID:        "room1:evt1",
		EventID:   "evt1",
		RoomID:    "room1",
		UserID:    "@alice:example.org",
		Text:      "hello",
		Timestamp: ts,
		Vector:    []float32{0.1, 0.2, 0.3},
	}
	err := bc.IndexDocumentStruct(doc1)
	require.NoError(t, err)
	err = bc.AddEventID("evt1")
	require.NoError(t, err)

	doc2 := indexer.IndexedDocument{
		ID:        "room1:evt2",
		EventID:   "evt2",
		RoomID:    "room1",
		UserID:    "@bob:example.org",
		Text:      "hello",
		Timestamp: ts,
		Vector:    []float32{0.1, 0.2, 0.3},
	}
	err = bc.IndexDocumentStruct(doc2)
	require.NoError(t, err)
	err = bc.AddEventID("evt2")
	require.NoError(t, err)
	err = bc.Flush()
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	// Only @alice's results
	result, err := eng.SemanticSearch("hello", "", "@alice:example.org")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Semantic))
	assert.Equal(t, "@alice:example.org", result.Semantic[0].UserID)

	// Only @bob's results
	result, err = eng.SemanticSearch("hello", "", "@bob:example.org")
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Semantic))
	assert.Equal(t, "@bob:example.org", result.Semantic[0].UserID)
}

func TestSemanticSearch_all_stop_words_returns_empty(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 10})

	result, err := eng.SemanticSearch("и в на что это", "", "")
	require.NoError(t, err)
	assert.Equal(t, "и в на что это", result.Query)
	assert.Equal(t, 0, len(result.Semantic))
}

func TestSemanticSearch_limit_results(t *testing.T) {
	bc, cleanup := createTestBleveClient(t)
	defer cleanup()

	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	for i := 1; i <= 5; i++ {
		doc := indexer.IndexedDocument{
			ID:        fmt.Sprintf("room1:evt%d", i),
			EventID:   fmt.Sprintf("evt%d", i),
			RoomID:    "room1",
			UserID:    "@alice:example.org",
			Text:      fmt.Sprintf("hello %d", i),
			Timestamp: ts + int64(i)*1000,
			Vector:    []float32{0.1, 0.2, float32(i) * 0.1},
		}
		err := bc.IndexDocumentStruct(doc)
		require.NoError(t, err)
		err = bc.AddEventID(doc.EventID)
		require.NoError(t, err)
	}
	err := bc.Flush()
	require.NoError(t, err)
	err = bc.FlushEventID()
	require.NoError(t, err)

	eng := NewEngine(bc, newMockEmbedClientForSearch(), &config.SearchConfig{ResultLimit: 2})

	result, err := eng.SemanticSearch("hello", "", "")
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Semantic))
	// Just check that results are returned and limited to 2,
	// FAISS vector similarity doesn't guarantee a specific order
	eventIDs := make(map[string]bool)
	for _, r := range result.Semantic {
		eventIDs[r.EventID] = true
	}
	assert.True(t, len(eventIDs) == 2)
}

func TestFormatResults_both_exact_and_semantic(t *testing.T) {
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	result := &SearchResult{
		Query: "hello",
		Exact: []Result{
			{EventID: "evt1", RoomID: "#room:server.org", UserID: "@alice:example.org", Timestamp: ts, Text: "hello exact", Score: 0.9},
		},
		Semantic: []Result{
			{EventID: "evt2", RoomID: "#room:server.org", UserID: "@bob:example.org", Timestamp: ts, Text: "hello semantic", Score: 0.8},
		},
	}

	eng := NewEngine(nil, nil, &config.SearchConfig{ResultLimit: 10})
	text, _ := eng.FormatResults(result)

	assert.Contains(t, text, "Search results for:")
	assert.Contains(t, text, "Exact matches:")
	assert.Contains(t, text, "Similar (semantic):")
	assert.Contains(t, text, "hello exact")
	assert.Contains(t, text, "hello semantic")
}

func TestFormatResults_only_semantic(t *testing.T) {
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	result := &SearchResult{
		Query: "hello",
		Exact:  []Result{},
		Semantic: []Result{
			{EventID: "evt1", RoomID: "#room:server.org", UserID: "@alice:example.org", Timestamp: ts, Text: "hello semantic", Score: 0.8},
		},
	}

	eng := NewEngine(nil, nil, &config.SearchConfig{ResultLimit: 10})
	text, _ := eng.FormatResults(result)

	assert.NotContains(t, text, "Exact matches:")
	assert.Contains(t, text, "Similar (semantic):")
}

func TestFormatSemanticResults_only_semantic(t *testing.T) {
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	result := &SearchResult{
		Query: "hello",
		Semantic: []Result{
			{EventID: "evt1", RoomID: "#room:server.org", UserID: "@alice:example.org", Timestamp: ts, Text: "hello semantic", Score: 0.8},
		},
	}

	eng := NewEngine(nil, nil, &config.SearchConfig{ResultLimit: 10})
	text, _ := eng.FormatSemanticResults(result)

	assert.Contains(t, text, "Semantic search results for:")
	assert.Contains(t, text, "Similar (semantic):")
	assert.Contains(t, text, "hello semantic")
	assert.NotContains(t, text, "Exact matches:")
}
