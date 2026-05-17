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

type bleveTaskType int

const (
	taskIndexDoc bleveTaskType = iota
	taskAddEventID
	taskFlush
	taskFlushEventID
	taskIsEventIDExists
	taskCountDocuments
	taskClose
	taskExecBatch
)

type bleveTask struct {
	Type    bleveTaskType
	Doc     IndexedDocument
	EventID string
	Batch   *bleve.Batch
	RespCh  chan bleveTaskResponse
}

type bleveTaskResponse struct {
	Exists bool
	Count  int
	Err    error
}

type BleveClient struct {
	index        bleve.Index
	eventIDIndex bleve.Index

	// batchBuf accumulates docs for batched indexing to reduce disk I/O.
	batchBuf    []IndexedDocument
	batchBufLen int

	// eventIDBuf accumulates event IDs for batched storage.
	eventIDBuf    []string
	eventIDBufLen int

	// processedEventIDs is a local cache of event IDs already added to the index.
	// Cleared after flushEventIDBatch to allow Bleve to be the source of truth.
	processedEventIDs map[string]bool

	// CSP: воркер — единственный, кто модифицирует local state
	taskCh  chan bleveTask
	stopCh  chan struct{}
	wg      sync.WaitGroup
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

	b := &BleveClient{
		index:             index,
		eventIDIndex:      eventIDIndex,
		processedEventIDs: make(map[string]bool),

		// CSP: воркер — единственный, кто модифицирует local state
		taskCh:  make(chan bleveTask, 1000),
		stopCh:  make(chan struct{}),
	}

	b.wg.Add(1)
	go b.bleveWorker()
	return b, nil
}

func (b *BleveClient) Close() error {
	close(b.stopCh)
	b.wg.Wait()
	err := b.index.Close()
	if err2 := b.eventIDIndex.Close(); err2 != nil && err == nil {
		err = err2
	}
	return err
}

// flushBatch persists pending documents in the batch buffer to the Bleve index.
// Called only by the worker goroutine — no mutex needed.
func (b *BleveClient) flushBatch() error {
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

// addDocToBatch accumulates a document in batchBuf, flushing when the batch is full.
// Called only by the worker goroutine — no mutex needed.
func (b *BleveClient) addDocToBatch(doc IndexedDocument) error {
	b.batchBufLen++
	if b.batchBufLen > cap(b.batchBuf) {
		newBuf := make([]IndexedDocument, len(b.batchBuf)+100)
		copy(newBuf, b.batchBuf)
		b.batchBuf = newBuf
	}
	b.batchBuf[b.batchBufLen-1] = doc

	const batchSize = 100
	if b.batchBufLen >= batchSize {
		return b.flushBatch()
	}
	return nil
}

// IndexDocumentStruct uses struct-based indexing which preserves []float32 type.
// Accumulates documents in a buffer and flushes in batches of 100 to reduce
// disk I/O (segment creation + bolt sync) during bulk indexing.
func (b *BleveClient) IndexDocumentStruct(doc IndexedDocument) error {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskIndexDoc, Doc: doc, RespCh: respCh}
	resp := <-respCh
	return resp.Err
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

// addEventIDToBuffer accumulates an event ID in eventIDBuf, flushing when the batch is full.
// Called only by the worker goroutine — no mutex needed.
func (b *BleveClient) addEventIDToBuffer(eventID string) error {
	b.processedEventIDs[eventID] = true
	b.eventIDBufLen++
	if b.eventIDBufLen > cap(b.eventIDBuf) {
		newBuf := make([]string, len(b.eventIDBuf)+50)
		copy(newBuf, b.eventIDBuf)
		b.eventIDBuf = newBuf
	}
	b.eventIDBuf[b.eventIDBufLen-1] = eventID

	const eventIDBatchSize = 50
	if b.eventIDBufLen >= eventIDBatchSize {
		return b.flushEventIDBatch()
	}
	return nil
}

// AddEventID stores an event ID in the processedEvents index for deduplication.
// Buffers event IDs and flushes in batches to reduce disk I/O.
// Also adds to local cache for O(1) dedup checks.
func (b *BleveClient) AddEventID(eventID string) error {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskAddEventID, EventID: eventID, RespCh: respCh}
	resp := <-respCh
	return resp.Err
}

func (b *BleveClient) flushEventIDBatch() error {
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

// isEventIDExists checks if an event ID exists in the processedEvents index.
// Called only by the worker goroutine — no mutex needed.
// Checks local cache first (O(1)), falls back to Bleve query.
func (b *BleveClient) isEventIDExists(eventID string) (bool, error) {
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

// IsEventIDExists checks if an event ID exists in the processedEvents index.
// Checks local cache first (O(1)), falls back to Bleve query.
func (b *BleveClient) IsEventIDExists(eventID string) (bool, error) {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskIsEventIDExists, EventID: eventID, RespCh: respCh}
	resp := <-respCh
	return resp.Exists, resp.Err
}

func (b *BleveClient) CountDocuments() (int, error) {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskCountDocuments, RespCh: respCh}
	resp := <-respCh
	return resp.Count, resp.Err
}

// countDocuments returns the document count using the Bleve index.
// Called only by the worker goroutine — no mutex needed.
func (b *BleveClient) countDocuments() (int, error) {
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

// ExecBatch executes a batch through the worker for bulk indexing.
// Used in reembed for bulk indexing of migrated documents.
func (b *BleveClient) ExecBatch(batch *bleve.Batch) error {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskExecBatch, Batch: batch, RespCh: respCh}
	resp := <-respCh
	return resp.Err
}

// bleveWorker is the single goroutine that processes all tasks.
// Since it's the only goroutine, no mutexes are needed for local state.
func (b *BleveClient) bleveWorker() {
	defer b.wg.Done()
	for {
		select {
		case task := <-b.taskCh:
			switch task.Type {
			case taskIndexDoc:
				if err := b.addDocToBatch(task.Doc); err != nil {
					task.RespCh <- bleveTaskResponse{Err: err}
				} else {
					task.RespCh <- bleveTaskResponse{}
				}
			case taskAddEventID:
				if err := b.addEventIDToBuffer(task.EventID); err != nil {
					task.RespCh <- bleveTaskResponse{Err: err}
				} else {
					task.RespCh <- bleveTaskResponse{}
				}
			case taskFlush:
				if err := b.flushBatch(); err != nil {
					task.RespCh <- bleveTaskResponse{Err: err}
				} else {
					task.RespCh <- bleveTaskResponse{}
				}
			case taskFlushEventID:
				if err := b.flushEventIDBatch(); err != nil {
					task.RespCh <- bleveTaskResponse{Err: err}
				} else {
					task.RespCh <- bleveTaskResponse{}
				}
			case taskIsEventIDExists:
				exists, err := b.isEventIDExists(task.EventID)
				task.RespCh <- bleveTaskResponse{Exists: exists, Err: err}
			case taskCountDocuments:
				count, err := b.countDocuments()
				task.RespCh <- bleveTaskResponse{Count: count, Err: err}
			case taskExecBatch:
				if task.Batch != nil {
					if err := b.index.Batch(task.Batch); err != nil {
						task.RespCh <- bleveTaskResponse{Err: err}
					}
				}
				task.RespCh <- bleveTaskResponse{}
			case taskClose:
				b.flushBatch()
				b.flushEventIDBatch()
				task.RespCh <- bleveTaskResponse{}
			}
		case <-b.stopCh:
			b.flushBatch()
			b.flushEventIDBatch()
			close(b.taskCh)
			return
		}
	}
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
func (b *BleveClient) Flush() error {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskFlush, RespCh: respCh}
	<-respCh
	return nil
}

// FlushEventID persists any pending event IDs in the batch buffer to the eventID index.
func (b *BleveClient) FlushEventID() error {
	respCh := make(chan bleveTaskResponse, 1)
	b.taskCh <- bleveTask{Type: taskFlushEventID, RespCh: respCh}
	<-respCh
	return nil
}
