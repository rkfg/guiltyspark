package indexer

import (
	"fmt"
	"log"
	"strconv"

	"github.com/blevesearch/bleve/v2"
	index "github.com/blevesearch/bleve_index_api"
)

type BleveClient struct {
	index bleve.Index
}

func NewBleveClient(indexPath string, vectorDims int) (*BleveClient, error) {
	indexMapping := bleve.NewIndexMapping()

	textMapping := bleve.NewTextFieldMapping()
	textMapping.Analyzer = "standard"

	// Vector field mapping for kNN search with FAISS
	vectorMapping := bleve.NewVectorFieldMapping()
	vectorMapping.Dims = vectorDims
	vectorMapping.Similarity = index.CosineSimilarity
	vectorMapping.VectorIndexOptimizedFor = index.IndexOptimizedForRecall

	// Use DefaultMapping so all fields are indexed by default
	indexMapping.DefaultMapping.AddFieldMappingsAt("text", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("image_desc", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("room_id", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("user_id", textMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("event_id", textMapping)
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

	return &BleveClient{index: index}, nil
}

func (b *BleveClient) Close() error {
	return b.index.Close()
}

func (b *BleveClient) IndexDocument(doc IndexedDocument) error {
	// Use map[string]interface{} to preserve vector type as []float32
	// When using struct, Bleve serializes via JSON which converts []float32 to []interface{}{float64, ...}
	// FAISS vector index requires []float32
	docMap := map[string]interface{}{
		"id":          doc.ID,
		"event_id":    doc.EventID,
		"room_id":     doc.RoomID,
		"user_id":     doc.UserID,
		"timestamp":   doc.Timestamp,
		"event_type":  doc.EventType,
		"text":        doc.Text,
		"image_desc":  doc.ImageDesc,
		"vector":      doc.Vector, // Keep as []float32
		"raw_url":     doc.RawURL,
		"file_name":   doc.FileName,
		"mime_type":   doc.MimeType,
	}
	
	// Log vector info for debugging
	if len(doc.Vector) > 0 {
		log.Printf("INFO bleve: IndexDocument docID=%s vector dims=%d first3=%v", doc.ID, len(doc.Vector), doc.Vector[:min(3, len(doc.Vector))])
	} else {
		log.Printf("WARN bleve: IndexDocument docID=%s vector is EMPTY", doc.ID)
	}
	
	err := b.index.Index(doc.ID, docMap)
	if err != nil {
		log.Printf("ERROR bleve: IndexDocument docID=%s ERROR=%v", doc.ID, err)
	}
	return err
}

// IndexDocumentStruct uses struct-based indexing which preserves []float32 type
func (b *BleveClient) IndexDocumentStruct(doc IndexedDocument) error {
	err := b.index.Index(doc.ID, doc)
	if err != nil {
		log.Printf("ERROR bleve: IndexDocumentStruct docID=%s ERROR=%v", doc.ID, err)
	}
	return err
}

func (b *BleveClient) SearchExact(queryText string, roomID string) (*bleve.SearchResult, error) {
	textQ := bleve.NewMatchQuery(queryText)
	textQ.SetField("text")

	imageQ := bleve.NewMatchQuery(queryText)
	imageQ.SetField("image_desc")

	booleanQ := bleve.NewBooleanQuery()
	booleanQ.AddShould(textQ)
	booleanQ.AddShould(imageQ)

	if roomID != "" {
		filterQ := bleve.NewTermQuery(roomID)
		filterQ.SetField("room_id")
		booleanQ.AddMust(filterQ)
	}

	searchReq := bleve.NewSearchRequest(booleanQ)
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

	// Log query vector for debugging
	log.Printf("INFO bleve: SearchSemantic queryVector dims=%d first5=%v", len(queryVector), queryVector[:min(5, len(queryVector))])

	// Use plain kNN without pre-filter to preserve original search behavior.
	// Room filtering is applied post-search (in search.Engine.Search).
	searchReq.AddKNN("vector", queryVector, 50, 1.0)

	result, err := b.index.Search(searchReq)
	if err != nil {
		return nil, err
	}

	log.Printf("INFO bleve: SearchSemantic result total=%d hits=%d", result.Total, len(result.Hits))
	for i, hit := range result.Hits {
		log.Printf("INFO bleve:   hit[%d] id=%s score=%.6f", i, hit.ID, hit.Score)
	}

	return result, nil
}

func (b *BleveClient) IsEventIndexed(eventID string) (bool, error) {
	// Use MatchQuery on event_id field
	// MatchQuery tokenizes input with the same analyzer used during indexing,
	// so if the event_id was indexed, it will be found
	q := bleve.NewMatchQuery(eventID)
	q.SetField("event_id")

	searchReq := bleve.NewSearchRequest(q)
	searchReq.Size = 1

	result, err := b.index.Search(searchReq)
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

func (b *BleveClient) CountByRoom(roomID string) (int, error) {
	filterQ := bleve.NewTermQuery(roomID)
	filterQ.SetField("room_id")

	countReq := bleve.NewSearchRequest(filterQ)
	countReq.Size = 0

	result, err := b.index.Search(countReq)
	if err != nil {
		return 0, err
	}

	return int(result.Total), nil
}

// SearchByRoom returns all documents in a specific room.
func (b *BleveClient) SearchByRoom(roomID string, size int) (*bleve.SearchResult, error) {
	filterQ := bleve.NewTermQuery(roomID)
	filterQ.SetField("room_id")

	searchReq := bleve.NewSearchRequest(filterQ)
	searchReq.Size = size
	searchReq.Fields = []string{"text", "image_desc", "user_id", "room_id", "timestamp", "event_id", "raw_url", "file_name", "mime_type", "vector"}

	return b.index.Search(searchReq)
}

// SearchByID returns a single document by event_id.
func (b *BleveClient) SearchByID(eventID string) (*bleve.SearchResult, error) {
	queryQ := bleve.NewMatchQuery(eventID)
	queryQ.SetField("event_id")

	searchReq := bleve.NewSearchRequest(queryQ)
	searchReq.Size = 1
	searchReq.Fields = []string{"text", "image_desc", "user_id", "room_id", "timestamp", "event_id", "raw_url", "file_name", "mime_type"}

	return b.index.Search(searchReq)
}

func ParseTimestamp(val string) (int64, error) {
	return strconv.ParseInt(val, 10, 64)
}
