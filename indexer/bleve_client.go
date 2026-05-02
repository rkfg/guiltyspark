package indexer

import (
	"fmt"
	"log"

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

	// Vector field mapping for kNN search with FAISS (requires -tags vectors)
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
