---
name: bleve
description: Research notes for Bleve search engine (v2.6.0) — indexing, searching, keyword fields, FAISS vector search. Use when working with Bleve index, document mapping, search queries, or any Bleve operations.
---

# Bleve Search Engine Research Notes

## Version
`v2.6.0` — imported as `github.com/blevesearch/bleve/v2`

## Module Cache Location
`$(go env GOMODCACHE)/github.com/blevesearch/bleve/v2@v2.6.0/`

---

## Indexing

### `IndexDocumentStruct` — always use struct-based indexing
```go
func (b *BleveClient) IndexDocumentStruct(doc IndexedDocument) error {
    err := b.index.Index(doc.ID, doc)
    return err
}
```
**Use `IndexDocumentStruct` (struct), NOT `IndexDocument` (map).**
Map-based indexing loses `[]float32` type (becomes `[]any`).
FAISS vector search requires raw `[]float32` in the document.

---

## Field Mapping

### Keyword fields require `NewKeywordFieldMapping()`
```go
keywordMapping := bleve.NewKeywordFieldMapping()
indexMapping.DefaultMapping.AddFieldMappingsAt("room_id", keywordMapping)
indexMapping.DefaultMapping.AddFieldMappingsAt("user_id", keywordMapping)
indexMapping.DefaultMapping.AddFieldMappingsAt("event_id", keywordMapping)
```
**Keyword mapping stores the value as a single token (no analysis/tokenization).**
This is critical for exact-match fields like `event_id`, `room_id`, `user_id`.

### Default mapping includes all fields
```go
indexMapping.DefaultMapping.AddFieldMappingsAt("text", textMapping)
indexMapping.DefaultMapping.AddFieldMappingsAt("timestamp", bleve.NewNumericFieldMapping())
// ... etc
```
Use `DefaultMapping` + explicit `AddFieldMappingsAt` overrides. All fields are indexed by default.

---

## Searching

### `IsEventIDExists` — check processedEvents index (dedup)
```go
func (b *BleveClient) IsEventIDExists(eventID string) (bool, error) {
    q := bleve.NewTermQuery(eventID)
    q.SetField("event_id")
    searchReq := bleve.NewSearchRequest(q)
    searchReq.Size = 1
    result, err := b.eventIDIndex.Search(searchReq)
    if err != nil { return false, err }
    return result.Total > 0, nil
}
```
**Uses `b.eventIDIndex` (`.eventid` directory), NOT `b.index`.** This is the `processedEvents` index where `AddEventID` stores event IDs for deduplication. Do NOT use `b.index.Search()` for this check.

### `SearchExact` — text search with optional room filter
```go
func (b *BleveClient) SearchExact(queryText, roomID string) (*bleve.SearchResult, error) {
    textQ := bleve.NewMatchQuery(queryText)
    textQ.SetField("text")
    imageQ := bleve.NewMatchQuery(queryText)
    imageQ.SetField("image_desc")
    disjQ := bleve.NewDisjunctionQuery(textQ, imageQ) // OR search
    var q query.Query
    if roomID != "" {
        filterQ := bleve.NewTermQuery(roomID)
        filterQ.SetField("room_id")
        q = bleve.NewConjunctionQuery(disjQ, filterQ) // AND room filter
    } else {
        q = disjQ
    }
    searchReq := bleve.NewSearchRequest(q)
    searchReq.Size = 50
    searchReq.Fields = []string{"text", "image_desc", "user_id", "room_id", ...}
    return b.index.Search(searchReq)
}
```

### `SearchSemantic` — FAISS vector kNN search
```go
func (b *BleveClient) SearchSemantic(queryVector []float32, roomID string) (*bleve.SearchResult, error) {
    searchReq := bleve.NewSearchRequest(bleve.NewMatchAllQuery())
    searchReq.Size = 50
    searchReq.AddKNN("vector", queryVector, 50, 1.0)
    return b.index.Search(searchReq)
}
```
**Post-filter room_id in search engine code** (not in Bleve query).

---

## Known quirks

- `MatchQuery` always calls `analyzer.Analyze()`. For keyword fields, the keyword analyzer returns exactly one token (the full value), so it works for exact matches.
- `MatchQuery` with default `MatchQueryOperatorOr` uses `DisjunctionQuery` (OR) over analyzed tokens. If the query text is tokenized into multiple terms, it matches documents containing ANY of the terms.
- For boolean `AND` behavior, use `SetOperator(MatchQueryOperatorAnd)` which uses `ConjunctionQuery`.
- `index.Index(docID, doc)` — if the docID already exists, it **overwrites** the document (upsert).
- `TermQuery` works correctly for keyword fields — no tokenization, pure exact match.
- `CountDocuments()` uses `NewMatchAllQuery()` + `NewSearchRequest` to count hits.

## Vector search (FAISS)

Requires `-tags vectors` build flag.

```go
vectorMapping := bleve.NewVectorFieldMapping()
vectorMapping.Dims = 4096
vectorMapping.Similarity = index.CosineSimilarity
vectorMapping.VectorIndexOptimizedFor = index.IndexOptimizedForRecall
```

`AddKNN("vector", queryVector, topK, threshold)` in search request.
Vector stored as `[]float32` in struct.

---

## References

- Module source: `https://github.com/blevesearch/bleve`
- `mapping/field.go` — field mapping types (`NewKeywordFieldMapping`, etc.)
- `mapping/index_mapping.go` — index mapping configuration
- `search/query/match.go` — `MatchQuery` implementation
- `search/query/term.go` — `TermQuery` implementation
- `analysis/analyzer/keyword/keyword.go` — keyword analyzer (single token)
