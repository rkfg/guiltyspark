package indexer

// BleveClient interface defines the methods BatchIndexer needs from BleveClient.
// This allows mocking for tests.
type BleveClientInterface interface {
	IsEventIDExists(eventID string) (bool, error)
	IndexDocumentStruct(doc IndexedDocument) error
	AddEventID(eventID string) error
	Flush() error
	FlushEventID() error
	CountDocuments() (int, error)
}

// EmbedClient interface defines the methods BatchIndexer needs for embedding.
// This is the same interface defined in search/search.go.
type EmbedClientInterface interface {
	CreateEmbedding(text, prefix string) ([]float32, error)
}
