package indexer

import (
	"fmt"
	"log"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

type BleveClient struct {
	index        bleve.Index
	eventIDIndex bleve.Index
	mu           sync.Mutex

	// batchBuf accumulates docs for batched indexing to reduce disk I/O.
	batchBuf    []IndexedDocument
	batchBufLen int

	// eventIDBuf accumulates event IDs for batched storage.
	eventIDBuf    []string
	eventIDBufLen int

	// processedEventIDs is a local cache of event IDs already added to the index.
	// Used for O(1) dedup checks without hitting Bleve.
	// Cleared after flushEventIDBatchLocked to allow Bleve to be the source of truth.
	processedEventIDs map[string]bool
}

func NewBleveClient(indexPath string, vectorDims int) (*BleveClient, error) {
	// Increase persister nap time to allow segments to accumulate
	// before persisting, reducing disk I/O during bulk indexing.
	scorch.DefaultPersisterNapTimeMSec = 500

	indexMapping := bleve.NewIndexMapping()

	textMapping := bleve.NewTextFieldMapping()
	textMapping.Analyzer = "standard"

	keywordMapping := bleve.NewKeywordFieldMapping()

	// Vector field mapping for kNN search with FAISS (requires -tags vectors)
	vectorMapping := bleve.NewVectorFieldMapping()
	vectorMapping.Dims = vectorDims
	vectorMapping.Similarity = index.CosineSimilarity
	vectorMapping.VectorIndexOptimizedFor = index.IndexOptimizedForRecall

	// Use DefaultMapping so all fields are indexed by default
	indexMapping.DefaultMapping.AddFieldMappingsAt("text", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("image_desc", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("room_id", keywordMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("user_id", keywordMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("event_id", keywordMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("timestamp", bleve.NewNumericFieldMapping())
	indexMapping.DefaultMapping.AddFieldMappingsAt("event_type", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("vector", vectorMapping)

	// Try to open existing index first, then create new
	index, err := bleve.Open(indexPath)
	if err != nil {
		// Index doesn't exist, create it
		index, err = bleve.New(indexPath, indexMapping)
		if err != nil {
			return nil, fmt.Errorf("create bleve index: %w", err)
		}
		log.Printf("INFO bleve: Created new index at %s", indexPath)
	} else {
		log.Printf("INFO bleve: Opened existing index at %s", indexPath)
	}

	// Create processedEvents index — minimal mapping, only event_id field
	eventIDMapping := bleve.NewIndexMapping()
	eventIDMapping.DefaultMapping = bleve.NewDocumentMapping()
	eventIDMapping.DefaultMapping.AddFieldMappingsAt("event_id", bleve.NewKeywordFieldMapping())
	eventIDIndex, err := bleve.Open(indexPath + ".eventid")
	if err != nil {
		eventIDIndex, err = bleve.New(indexPath+".eventid", eventIDMapping)
		if err != nil {
			return nil, fmt.Errorf("create eventID index: %w", err)
		}
		log.Printf("INFO bleve: Created new processedEvents index at %s.eventid", indexPath)
	} else {
		log.Printf("INFO bleve: Opened existing processedEvents index at %s.eventid", indexPath)
	}

	return &BleveClient{index: index, eventIDIndex: eventIDIndex, processedEventIDs: make(map[string]bool)}, nil
}

func (b *BleveClient) Close() error {
	b.mu.Lock()
	if err := b.flushBatchLocked(); err != nil {
		log.Printf("ERROR bleve: failed to flush batch on close: %v", err)
	}
	if err := b.flushEventIDBatchLocked(); err != nil {
		log.Printf("ERROR bleve: failed to flush eventID batch on close: %v", err)
	}
	b.mu.Unlock()
	err := b.index.Close()
	if err2 := b.eventIDIndex.Close(); err2 != nil && err == nil {
		err = err2
	}
	return err
}

// IndexDocumentStruct uses struct-based indexing which preserves []float32 type.
// Accumulates documents in a buffer and flushes in batches of 100 to reduce
// disk I/O (segment creation + bolt sync) during bulk indexing.
func (b *BleveClient) IndexDocumentStruct(doc IndexedDocument) error {
	b.mu.Lock()
	b.batchBufLen++
	if b.batchBufLen > cap(b.batchBuf) {
		newBuf := make([]IndexedDocument, len(b.batchBuf)+100)
		copy(newBuf, b.batchBuf)
		b.batchBuf = newBuf
	}
	b.batchBuf[b.batchBufLen-1] = doc

	const batchSize = 100
	var flushErr error
	if b.batchBufLen >= batchSize {
		flushErr = b.flushBatchLocked()
	}
	b.mu.Unlock()
	return flushErr
}

func (b *BleveClient) flushBatchLocked() error {
	if b.batchBufLen == 0 {
		return nil
	}
	batch := b.index.NewBatch()
	for i := 0; i < b.batchBufLen; i++ {
		if err := batch.Index(b.batchBuf[i].ID, b.batchBuf[i]); err != nil {
			log.Printf("ERROR bleve: batch index error docID=%s ERROR=%v", b.batchBuf[i].ID, err)
		}
	}
	b.batchBufLen = 0
	return b.index.Batch(batch)
}

func (b *BleveClient) SearchExact(queryText string, roomID string) (*bleve.SearchResult, error) {
	// Use MatchQuery — it searches only in the specified field
	textQ := bleve.NewMatchQuery(queryText)
	textQ.SetField("text")

	imageQ := bleve.NewMatchQuery(queryText)
	imageQ.SetField("image_desc")

	// Use DisjunctionQuery for OR search across text and image_desc
	disjQ := bleve.NewDisjunctionQuery(textQ, imageQ)

	var q query.Query
	if roomID != "" {
		filterQ := bleve.NewTermQuery(roomID)
		filterQ.SetField("room_id")
		q = bleve.NewConjunctionQuery(disjQ, filterQ)
	} else {
		q = disjQ
	}

	searchReq := bleve.NewSearchRequest(q)
	searchReq.Size = 50
	searchReq.Fields = []string{"text", "image_desc", "user_id", "room_id", "timestamp", "event_id", "raw_url", "file_name", "mime_type"}

	result, err := b.index.Search(searchReq)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (b *BleveClient) SearchSemantic(queryVector []float32, roomID string) (*bleve.SearchResult, error) {
	searchReq := bleve.NewSearchRequest(bleve.NewMatchAllQuery())
	searchReq.Size = 5
	searchReq.Fields = []string{"text", "image_desc", "user_id", "room_id", "timestamp", "event_id", "raw_url", "file_name", "mime_type"}

	// Use plain kNN without pre-filter to preserve original search behavior.
	// Room filtering is applied post-search (in search.Engine.Search).
	searchReq.AddKNN("vector", queryVector, 5, 1.0)

	result, err := b.index.Search(searchReq)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// AddEventID stores an event ID in the processedEvents index for deduplication.
// Buffers event IDs and flushes in batches to reduce disk I/O.
// Also adds to local cache for O(1) dedup checks.
func (b *BleveClient) AddEventID(eventID string) error {
	b.mu.Lock()
	b.processedEventIDs[eventID] = true
	b.eventIDBufLen++
	if b.eventIDBufLen > cap(b.eventIDBuf) {
		newBuf := make([]string, len(b.eventIDBuf)+50)
		copy(newBuf, b.eventIDBuf)
		b.eventIDBuf = newBuf
	}
	b.eventIDBuf[b.eventIDBufLen-1] = eventID

	const eventIDBatchSize = 50
	var flushErr error
	if b.eventIDBufLen >= eventIDBatchSize {
		flushErr = b.flushEventIDBatchLocked()
	}
	b.mu.Unlock()
	return flushErr
}

func (b *BleveClient) flushEventIDBatchLocked() error {
	if b.eventIDBufLen == 0 {
		return nil
	}
	batch := b.eventIDIndex.NewBatch()
	for i := 0; i < b.eventIDBufLen; i++ {
		if err := batch.Index(b.eventIDBuf[i], map[string]any{"event_id": b.eventIDBuf[i]}); err != nil {
			log.Printf("ERROR bleve: batch eventID index error eventID=%s ERROR=%v", b.eventIDBuf[i], err)
		}
	}
	b.eventIDBufLen = 0
	clear(b.processedEventIDs)
	return b.eventIDIndex.Batch(batch)
}

// IsEventIDExists checks if an event ID exists in the processedEvents index.
// Checks local cache first (O(1)), falls back to Bleve query.
func (b *BleveClient) IsEventIDExists(eventID string) (bool, error) {
	if b.processedEventIDs[eventID] {
		return true, nil
	}
	q := bleve.NewTermQuery(eventID)
	q.SetField("event_id")
	searchReq := bleve.NewSearchRequest(q)
	searchReq.Size = 1
	result, err := b.eventIDIndex.Search(searchReq)
	if err != nil {
		return false, err
	}
	return result.Total > 0, nil
}

func (b *BleveClient) CountDocuments() (int, error) {
	countReq := bleve.NewMatchAllQuery()

	countReq2 := bleve.NewSearchRequest(countReq)
	countReq2.Size = 0

	result, err := b.index.Search(countReq2)
	if err != nil {
		return 0, err
	}

	return int(result.Total), nil
}

func (b *BleveClient) NewBatch() *bleve.Batch {
	return b.index.NewBatch()
}

func (b *BleveClient) BatchIndex(batch *bleve.Batch, doc IndexedDocument) error {
	return batch.Index(doc.ID, doc)
}

func (b *BleveClient) ExecBatch(batch *bleve.Batch) error {
	return b.index.Batch(batch)
}
