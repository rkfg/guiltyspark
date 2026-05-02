package indexer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type PendingMessage struct {
	EventID   string
	RoomID    string
	UserID    string
	Timestamp int64
	EventType string
	Text      string
}

type BatchIndexer struct {
	// deferredImages are images whose LLM processing (VLM + embedding) is deferred
	// until the daily scheduled time (configurable via delayed_embed_hour/minute).
	deferredImages []PendingImage
	// deferredTextEmbed are text messages whose embedding is deferred
	// until the daily scheduled time.
	deferredTextEmbed []PendingMessage
	mu                sync.Mutex
	saveMu            sync.Mutex
	stopCh            chan struct{}
	doneCh            chan struct{}

	// Callbacks
	IndexTextFn func(doc IndexedDocument) error
	IsIndexedFn func(eventID string) (bool, error)

	// Deferred processing
	ProcessDeferredFn     func(images []PendingImage) error
	ProcessDeferredTextFn func(texts []PendingMessage) error

	// Channels for non-blocking ingestion
	imageCh chan PendingImage

	// Deferred processing scheduler
	embedHour   int
	embedMinute int
	persistDir string
}

type PendingImage struct {
	EventID   string
	RoomID    string
	UserID    string
	Timestamp int64
	RawURL    string
	FileName  string
	MimeType  string
}

const persistFile = "deferred.json"

func NewBatchIndexer(embedHour, embedMinute int) *BatchIndexer {
	b := &BatchIndexer{
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		imageCh:     make(chan PendingImage, 1000),
		embedHour:   embedHour,
		embedMinute: embedMinute,
		persistDir:  "./bot-data",
	}

	// Load persisted deferred data
	b.loadDeferred()

	// Start ingestion goroutine
	go b.ingestLoop()
	// Start deferred processing scheduler
	go b.deferredProcessingLoop()
	return b
}

type persistData struct {
	DeferredImages    []PendingImage   `json:"deferred_images"`
	DeferredTextEmbed []PendingMessage `json:"deferred_text"`
}

func (b *BatchIndexer) saveDeferred() {
	b.saveMu.Lock()
	defer b.saveMu.Unlock()
	b.mu.Lock()
	data := persistData{
		DeferredImages:    make([]PendingImage, len(b.deferredImages)),
		DeferredTextEmbed: make([]PendingMessage, len(b.deferredTextEmbed)),
	}
	copy(data.DeferredImages, b.deferredImages)
	copy(data.DeferredTextEmbed, b.deferredTextEmbed)
	b.mu.Unlock()

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("ERROR batch_indexer: failed to marshal deferred data: %v", err)
		return
	}

	file := filepath.Join(b.persistDir, persistFile)
	tmpFile := file + ".tmp"
	if err := os.WriteFile(tmpFile, jsonData, 0644); err != nil {
		log.Printf("ERROR batch_indexer: failed to save deferred data: %v", err)
		return
	}
	if err := os.Rename(tmpFile, file); err != nil {
		log.Printf("ERROR batch_indexer: failed to rename deferred data: %v", err)
	}
}

func (b *BatchIndexer) loadDeferred() {
	file := filepath.Join(b.persistDir, persistFile)
	jsonData, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return // No persisted data
		}
		log.Printf("ERROR batch_indexer: failed to load deferred data: %v", err)
		return
	}

	var data persistData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		log.Printf("ERROR batch_indexer: failed to unmarshal deferred data: %v", err)
		return
	}

	b.mu.Lock()
	b.deferredImages = append(b.deferredImages, data.DeferredImages...)
	b.deferredTextEmbed = append(b.deferredTextEmbed, data.DeferredTextEmbed...)
	b.mu.Unlock()

	log.Printf("INFO batch_indexer: loaded %d deferred images and %d deferred text embeddings", len(data.DeferredImages), len(data.DeferredTextEmbed))
}

// OnTextMessage indexes text immediately (no batching).
func (b *BatchIndexer) OnTextMessage(msg PendingMessage) {
	b.indexTextMessage(msg)

	// Queue for deferred embedding (with dedup)
	b.mu.Lock()
	if !b.hasDeferredText(msg.EventID) {
		b.deferredTextEmbed = append(b.deferredTextEmbed, msg)
	}
	b.mu.Unlock()
	b.saveDeferred()
}

// OnImageMessage is non-blocking — enqueues image via channel (with dedup).
func (b *BatchIndexer) OnImageMessage(img PendingImage) {
	select {
	case b.imageCh <- img:
	default:
	}
}

func (b *BatchIndexer) ingestLoop() {
	for {
		select {
		case img := <-b.imageCh:
			b.mu.Lock()
			if !b.hasDeferredImage(img.EventID) {
				b.deferredImages = append(b.deferredImages, img)
			}
			b.mu.Unlock()
			b.saveDeferred()

		case <-b.stopCh:
			close(b.doneCh)
			return
		}
	}
}

// hasDeferredImage checks if an image with the given EventID is already in deferredImages.
func (b *BatchIndexer) hasDeferredImage(eventID string) bool {
	for _, img := range b.deferredImages {
		if img.EventID == eventID {
			return true
		}
	}
	return false
}

// hasDeferredText checks if a text message with the given EventID is already in deferredTextEmbed.
func (b *BatchIndexer) hasDeferredText(eventID string) bool {
	for _, msg := range b.deferredTextEmbed {
		if msg.EventID == eventID {
			return true
		}
	}
	return false
}

// deferredProcessingLoop waits until the configured daily time and processes
// all deferred images (VLM description + embedding) in one batch.
func (b *BatchIndexer) deferredProcessingLoop() {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(),
			b.embedHour, b.embedMinute, 0, 0, now.Location())
		if next.Before(now) || next.Equal(now) {
			next = next.Add(24 * time.Hour)
		}

		select {
		case <-time.After(time.Until(next)):
			b.mu.Lock()
			var deferredImages []PendingImage
			if len(b.deferredImages) > 0 {
				deferredImages = make([]PendingImage, len(b.deferredImages))
				copy(deferredImages, b.deferredImages)
				b.deferredImages = b.deferredImages[:0]
			}
			var deferredText []PendingMessage
			if len(b.deferredTextEmbed) > 0 {
				deferredText = make([]PendingMessage, len(b.deferredTextEmbed))
				copy(deferredText, b.deferredTextEmbed)
				b.deferredTextEmbed = b.deferredTextEmbed[:0]
			}
			b.mu.Unlock()

			if len(deferredImages) > 0 {
				log.Printf("INFO batch_indexer: processing %d deferred images", len(deferredImages))
				if b.ProcessDeferredFn != nil {
					if err := b.ProcessDeferredFn(deferredImages); err != nil {
						log.Printf("ERROR batch_indexer: deferred processing error: %v", err)
					}
				}
			}
			if len(deferredText) > 0 {
				log.Printf("INFO batch_indexer: processing %d deferred text embeddings", len(deferredText))
				if b.ProcessDeferredTextFn != nil {
					if err := b.ProcessDeferredTextFn(deferredText); err != nil {
						log.Printf("ERROR batch_indexer: deferred text embedding error: %v", err)
					}
				}
			}

			// Clear persisted deferred data after processing
			b.saveDeferred()
		case <-b.stopCh:
			return
		}
	}
}

func (b *BatchIndexer) Stop() {
	close(b.stopCh)
	<-b.doneCh
}


func (b *BatchIndexer) indexTextMessage(msg PendingMessage) {
	// Check dedup via Bleve
	if b.IsIndexedFn != nil {
		indexed, err := b.IsIndexedFn(msg.EventID)
		if err != nil {
			log.Printf("ERROR batch_indexer: IsIndexedFn error for event %s: %v", msg.EventID, err)
			return
		}
		if indexed {
			log.Printf("INFO batch_indexer: skipping already indexed event %s", msg.EventID)
			return
		}
	}

	// Immediate Bleve indexing (no embedding — done later)
	doc := IndexedDocument{
		ID:        fmt.Sprintf("%s:%s", msg.RoomID, msg.EventID),
		EventID:   msg.EventID,
		RoomID:    msg.RoomID,
		UserID:    msg.UserID,
		Timestamp: msg.Timestamp,
		EventType: msg.EventType,
		Text:      msg.Text,
	}

	if err := b.IndexTextFn(doc); err != nil {
		log.Printf("ERROR batch_indexer: IndexTextFn error for event %s: %v", msg.EventID, err)
	} else {
		log.Printf("INFO batch_indexer: indexed text event %s", msg.EventID)
	}
}
