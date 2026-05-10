package indexer

import (
	"fmt"
	"log"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

type BleveClient struct {
	index        bleve.Index
	eventIDIndex bleve.Index
}

func NewBleveClient(indexPath string, vectorDims int) (*BleveClient, error) {
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

	return &BleveClient{index: index, eventIDIndex: eventIDIndex}, nil
}

func (b *BleveClient) Close() error {
	err := b.index.Close()
	if err2 := b.eventIDIndex.Close(); err2 != nil && err == nil {
		err = err2
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

	// Log query vector for debugging
	log.Printf("INFO bleve: SearchSemantic queryVector dims=%d first5=%v", len(queryVector), queryVector[:min(5, len(queryVector))])

	// Use plain kNN without pre-filter to preserve original search behavior.
	// Room filtering is applied post-search (in search.Engine.Search).
	searchReq.AddKNN("vector", queryVector, 5, 1.0)

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
	q := bleve.NewTermQuery(eventID)
	q.SetField("event_id")
	searchReq := bleve.NewSearchRequest(q)
	searchReq.Size = 1
	result, err := b.index.Search(searchReq)
	if err != nil {
		return false, err
	}
	return result.Total > 0, nil
}

// AddEventID stores an event ID in the processedEvents index for deduplication.
func (b *BleveClient) AddEventID(eventID string) error {
	return b.eventIDIndex.Index(eventID, map[string]any{"event_id": eventID})
}

// IsEventIDExists checks if an event ID exists in the processedEvents index.
func (b *BleveClient) IsEventIDExists(eventID string) (bool, error) {
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
