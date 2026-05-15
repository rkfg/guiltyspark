package indexer

import (
	"fmt"
	"log"
	"sync"

	"github.com/blevesearch/bleve/v2"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ru"
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

func NewBleveClient(indexPath string, vectorDims int, analyzer string) (*BleveClient, error) {
	// Increase persister nap time to allow segments to accumulate
	// before persisting, reducing disk I/O during bulk indexing.
	scorch.DefaultPersisterNapTimeMSec = 500

	indexMapping := bleve.NewIndexMapping()

	textMapping := bleve.NewTextFieldMapping()
	textMapping.Analyzer = analyzer

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
// flushBatchLocked must be called with b.mu held.
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

// IndexDocumentStruct uses struct-based indexing which preserves []float32 type.
// Accumulates documents in a buffer and flushes in batches of 100 to reduce
// disk I/O (segment creation + bolt sync) during bulk indexing.
//
// CRITICAL: b.index.Batch(batch) is called OUTSIDE b.mu to avoid deadlocking.
// If b.index.Batch() blocks on bolt write locks (scorch persister), holding b.mu
// would prevent all concurrent IndexDocumentStruct calls.
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
	var batchDocs []IndexedDocument
	if b.batchBufLen >= batchSize {
		// Snapshot batch contents and reset buffer while holding the lock
		batchDocs = make([]IndexedDocument, b.batchBufLen)
		copy(batchDocs, b.batchBuf)
		b.batchBufLen = 0
	}
	b.mu.Unlock()

	// Execute batch WITHOUT holding b.mu
	// This prevents deadlock when b.index.Batch() blocks on bolt locks
	if len(batchDocs) > 0 {
		batch := b.index.NewBatch()
		for i := 0; i < len(batchDocs); i++ {
			if err := batch.Index(batchDocs[i].ID, batchDocs[i]); err != nil {
				log.Printf("ERROR bleve: batch index error docID=%s ERROR=%v", batchDocs[i].ID, err)
			}
		}
		flushErr = b.index.Batch(batch)
	}

	return flushErr
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
	searchReq.Size = 50
	searchReq.Fields = []string{"text", "image_desc", "user_id", "room_id", "timestamp", "event_id", "raw_url", "file_name", "mime_type"}

	// Use plain kNN without pre-filter to preserve original search behavior.
	// Room filtering is applied post-search (in search.Engine.Search).
	searchReq.AddKNN("vector", queryVector, 50, 1.0)

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

// ScanAllDocuments iterates over all documents in the index, calling fn for each.
// fn receives a copy of the document and should return true to continue, false to stop.
func (b *BleveClient) ScanAllDocuments(fn func(doc IndexedDocument) bool) error {
	var lastID string
	hasMore := true

	for hasMore {
		q := bleve.NewMatchAllQuery()
		searchReq := bleve.NewSearchRequest(q)
		searchReq.Size = 1000
		searchReq.Fields = []string{
			"text", "image_desc", "user_id", "room_id",
			"timestamp", "event_id", "raw_url", "file_name",
			"mime_type", "event_type", "vector",
		}
		searchReq.SortBy([]string{"_id"})

		if lastID != "" {
			searchReq.SetSearchAfter([]string{lastID})
		}

		result, err := b.index.Search(searchReq)
		if err != nil {
			return err
		}

		if len(result.Hits) == 0 {
			hasMore = false
			break
		}

		for _, hit := range result.Hits {
			doc := FieldsToDocument(hit.ID, hit.Fields)
			lastID = hit.ID
			if !fn(doc) {
				return nil
			}
		}

		if len(result.Hits) < 1000 {
			hasMore = false
		}
	}

	return nil
}

func FieldsToDocument(docID string, fields map[string]any) IndexedDocument {
	var doc IndexedDocument
	doc.ID = docID

	if v, ok := fields["event_id"]; ok {
		if s, ok := v.(string); ok {
			doc.EventID = s
		}
	}
	if v, ok := fields["room_id"]; ok {
		if s, ok := v.(string); ok {
			doc.RoomID = s
		}
	}
	if v, ok := fields["user_id"]; ok {
		if s, ok := v.(string); ok {
			doc.UserID = s
		}
	}
	if v, ok := fields["text"]; ok {
		if s, ok := v.(string); ok {
			doc.Text = s
		}
	}
	if v, ok := fields["image_desc"]; ok {
		if s, ok := v.(string); ok {
			doc.ImageDesc = s
		}
	}
	if v, ok := fields["raw_url"]; ok {
		if s, ok := v.(string); ok {
			doc.RawURL = s
		}
	}
	if v, ok := fields["file_name"]; ok {
		if s, ok := v.(string); ok {
			doc.FileName = s
		}
	}
	if v, ok := fields["mime_type"]; ok {
		if s, ok := v.(string); ok {
			doc.MimeType = s
		}
	}
	if v, ok := fields["event_type"]; ok {
		if s, ok := v.(string); ok {
			doc.EventType = s
		}
	}
	if v, ok := fields["timestamp"]; ok {
		switch tv := v.(type) {
		case float64:
			doc.Timestamp = int64(tv)
		case string:
			fmt.Sscanf(tv, "%d", &doc.Timestamp)
		}
	}

	if v, ok := fields["vector"]; ok {
		switch tv := v.(type) {
		case []float32:
			doc.Vector = tv
		case []any:
			doc.Vector = make([]float32, 0, len(tv))
			for _, item := range tv {
				if f, ok := item.(float32); ok {
					doc.Vector = append(doc.Vector, f)
				}
			}
		}
	}

	return doc
}

// ScanIndex iterates over all documents in the given index, calling fn for each.
// fn receives a copy of the document and should return true to continue, false to stop.
func (b *BleveClient) ScanIndex(srcIdx bleve.Index, fn func(doc IndexedDocument) bool) error {
	var lastID string
	hasMore := true

	for hasMore {
		q := bleve.NewMatchAllQuery()
		searchReq := bleve.NewSearchRequest(q)
		searchReq.Size = 1000
		searchReq.Fields = []string{
			"text", "image_desc", "user_id", "room_id",
			"timestamp", "event_id", "raw_url", "file_name",
			"mime_type", "event_type", "vector",
		}
		searchReq.SortBy([]string{"_id"})

		if lastID != "" {
			searchReq.SetSearchAfter([]string{lastID})
		}

		result, err := srcIdx.Search(searchReq)
		if err != nil {
			return err
		}

		if len(result.Hits) == 0 {
			hasMore = false
			break
		}

		for _, hit := range result.Hits {
			doc := FieldsToDocument(hit.ID, hit.Fields)
			lastID = hit.ID
			if !fn(doc) {
				return nil
			}
		}

		if len(result.Hits) < 1000 {
			hasMore = false
		}
	}

	return nil
}

// Flush persists any pending documents in the batch buffer to the Bleve index.
// Called after live message indexing to make documents immediately searchable.
func (b *BleveClient) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushBatchLocked()
}

// FlushEventID persists any pending event IDs in the batch buffer to the eventID index.
// Called after live message indexing for dedup correctness and after batch operations.
func (b *BleveClient) FlushEventID() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushEventIDBatchLocked()
}
