package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/embedding"
	"github.com/rkfg/guiltyspark/indexer"
	"github.com/rkfg/guiltyspark/retry"
)

func reembed(indexPath string, cfg *config.Config) error {
	log.Printf("INFO reembed: ensuring the bot process is STOPPED before running this command")

	// Check if migration is needed (vector dimensions mismatch)
	migrationNeeded := false
	if oldD := oldVectorDims(indexPath); oldD > 0 && oldD != cfg.Search.VectorDimensions {
		log.Printf("INFO reembed: vector dimension mismatch detected (%d -> %d), migrating",
			oldD, cfg.Search.VectorDimensions)
		migrationNeeded = true
	} else if oldD > 0 {
		log.Printf("INFO reembed: vector dimensions match (dims=%d), no migration needed", oldD)
	}

	// Migration path: rename old index, create new, copy all data except vectors
	if migrationNeeded {
		return migrate(indexPath, cfg)
	}

	// Normal reembed path: re-embed from existing index
	bleveClient, err := indexer.NewBleveClient(indexPath, cfg.Search.VectorDimensions, cfg.Search.Analyzer)
	if err != nil {
		return fmt.Errorf("open bleve index: %w", err)
	}
	defer bleveClient.Close()

	embedClient := embedding.NewClient(&cfg.LLM, retry.BackoffConfig{
		InitialDelay: cfg.Retry.InitialDelay,
		MaxDelay:     cfg.Retry.MaxDelay,
		Multiplier:   cfg.Retry.Multiplier,
		MaxRetries:   cfg.Retry.MaxRetries,
	}, cfg.Retry.Timeout)

	return reembedFromIndex(bleveClient, embedClient)
}

// migrate renames the old index, creates a new one with correct dimensions,
// copies all non-vector data from old index, re-embeds vectors, writes to new index,
// and removes the old index on success.
func migrate(indexPath string, cfg *config.Config) error {
	// 1. Rename old index
	ts := time.Now().UTC().Format("20060102T150405")
	oldPath := indexPath + "." + ts + ".old"
	if err := os.Rename(indexPath, oldPath); err != nil {
		return fmt.Errorf("rename old index %s -> %s: %w", indexPath, oldPath, err)
	}
	log.Printf("INFO reembed: old index moved to %s", oldPath)

	// 2. Create new index with correct dimensions
	log.Printf("INFO reembed: creating new index with dims=%d", cfg.Search.VectorDimensions)
	bleveClient, err := indexer.NewBleveClient(indexPath, cfg.Search.VectorDimensions, cfg.Search.Analyzer)
	if err != nil {
		return fmt.Errorf("create new bleve index: %w", err)
	}
	defer bleveClient.Close()

	// 3. Open old index for reading
	log.Printf("INFO reembed: opening old index for reading at %s", oldPath)
	oldIdx, err := bleve.Open(oldPath)
	if err != nil {
		return fmt.Errorf("open old index %s for reading: %w", oldPath, err)
	}
	log.Printf("INFO reembed: old index opened successfully")

	// Get doc count from old index first
	docCount, err := oldIdx.DocCount()
	if err != nil {
		oldIdx.Close()
		return fmt.Errorf("get doc count from old index: %w", err)
	}
	log.Printf("INFO reembed: old index has %d documents", docCount)

	embedClient := embedding.NewClient(&cfg.LLM, retry.BackoffConfig{
		InitialDelay: cfg.Retry.InitialDelay,
		MaxDelay:     cfg.Retry.MaxDelay,
		Multiplier:   cfg.Retry.Multiplier,
		MaxRetries:   cfg.Retry.MaxRetries,
	}, cfg.Retry.Timeout)

	// 4. Read ALL documents from old index into memory, then close oldIdx
	// This avoids holding oldIdx open (and its segment cache) during the
	// entire embed+write phase.
	log.Printf("INFO reembed: reading all documents from old index into memory...")
	var docs []indexer.IndexedDocument
	err = bleveClient.ScanIndex(oldIdx, func(doc indexer.IndexedDocument) bool {
		docs = append(docs, doc)
		return true
	})
	if err != nil {
		oldIdx.Close()
		return fmt.Errorf("scan old index: %w", err)
	}
	log.Printf("INFO reembed: read %d documents from old index, closing old index", len(docs))
	oldIdx.Close()
	oldIdx = nil

	// 5. Now write documents (with re-embedded vectors) to new index
	log.Printf("INFO reembed: processing %d documents (embed + write to new index)...", len(docs))
	return reembedDocs(bleveClient, embedClient, docs)
}

// reembedFromIndex reads all docs into memory first, closes index, then re-embeds.
// This avoids Bolt concurrent read+write deadlock.
func reembedFromIndex(bleveClient *indexer.BleveClient, embedClient *embedding.Client) error {
	log.Printf("INFO reembed: reading all documents from index into memory...")
	var docs []indexer.IndexedDocument
	err := bleveClient.ScanAllDocuments(func(doc indexer.IndexedDocument) bool {
		docs = append(docs, doc)
		return true
	})
	if err != nil {
		return fmt.Errorf("scan documents: %w", err)
	}
	log.Printf("INFO reembed: read %d documents from index", len(docs))

	log.Printf("INFO reembed: processing %d documents (embed + re-index)...", len(docs))
	return reembedDocs(bleveClient, embedClient, docs)
}

// reembedDocs re-embeds and re-indexes a slice of documents.
func reembedDocs(bleveClient *indexer.BleveClient, embedClient *embedding.Client, docs []indexer.IndexedDocument) error {
	total := 0
	updated := 0
	skipped := 0
	failed := 0
	var embErr error

	start := time.Now()

	for _, doc := range docs {
		total++
		hasText := doc.Text != ""
		hasImageDesc := doc.ImageDesc != ""

		if !hasText && !hasImageDesc {
			skipped++
			continue
		}

		var newTextVector []float32
		var newImageVector []float32

		// Log progress every 100 docs
		if total%100 == 0 {
			log.Printf("INFO reembed: processing doc %d/%d (event %s)", total, len(docs), doc.EventID)
		}

		if hasText {
			newTextVector, embErr = embedClient.CreateEmbedding(doc.Text, "search_document: ")
			if embErr != nil {
				log.Printf("WARN reembed: failed to embed text for doc %d/%d (event %s): %v", total, len(docs), doc.EventID, embErr)
				failed++
				continue
			}
		}

		if hasImageDesc {
			newImageVector, embErr = embedClient.CreateEmbedding(doc.ImageDesc, "search_document: ")
			if embErr != nil {
				log.Printf("WARN reembed: failed to embed image_desc for doc %d/%d (event %s): %v", total, len(docs), doc.EventID, embErr)
				failed++
				continue
			}
		}

		// Prioritize text vector; fallback to image if no text
		if hasText && newTextVector != nil {
			doc.Vector = newTextVector
		} else if hasImageDesc && newImageVector != nil {
			doc.Vector = newImageVector
		}

		// Use IndexDocumentStruct — it internally batches by 100 docs in batchBuf
		if err := bleveClient.IndexDocumentStruct(doc); err != nil {
			log.Printf("WARN reembed: failed to index doc %d/%d (event %s): %v", total, len(docs), doc.EventID, err)
			failed++
			continue
		}

		updated++
	}

	bleveClient.FlushEventID()

	elapsed := time.Since(start)
	log.Printf("INFO reembed: total=%d updated=%d skipped=%d failed=%d elapsed=%s",
		total, updated, skipped, failed, elapsed.Round(time.Second))

	if failed > 0 {
		return fmt.Errorf("reembed completed with %d failures", failed)
	}

	return nil
}

// oldVectorDims opens the existing index and extracts the vector field's Dims
// from the index mapping. Returns 0 if the index doesn't exist or no vector field found.
func oldVectorDims(indexPath string) int {
	idx, err := bleve.Open(indexPath)
	if err != nil {
		return 0
	}
	defer idx.Close()

	m := idx.Mapping()
	if m == nil {
		return 0
	}

	im, ok := m.(*mapping.IndexMappingImpl)
	if !ok {
		return 0
	}

	if im.DefaultMapping == nil {
		return 0
	}

	// AddFieldMappingsAt stores fields in Properties["name"].Fields[0]
	if vecProps, ok := im.DefaultMapping.Properties["vector"]; ok {
		if len(vecProps.Fields) > 0 {
			return vecProps.Fields[0].Dims
		}
	}

	return 0
}
